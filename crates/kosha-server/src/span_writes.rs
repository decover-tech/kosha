//! Background writes of fetched spans into the sparse span cache.
//!
//! A ranged fetch (kNN vector postings, term postings, page documents,
//! `doc_vectors.bin` headers) used to write each fetched range to disk on
//! the thread that fetched it, before that thread read it back. The kNN
//! fetch runs inside a segment's scoring task on the process-wide rayon
//! pool (4 threads on a 4-CPU pod). Once dirty memory passed the kernel's
//! throttle point, each of those writes slept in `balance_dirty_pages`,
//! paced to the 125 MiB/s cache disk. Measured on staging (run8): one
//! filtered kNN request (rid 15741) wrote 3.55 GB of spans and spent 34.8 s
//! scoring. Its writes held the rayon threads, so every other search's
//! parallel work queued behind them: seven requests finished scoring within
//! 0.7 ms of each other, and a keyword request with no IO at all spent
//! 37.8 s in planning. Under traffic on run9 pods, 3–4 `kosha-http` threads
//! sat in `balance_dirty_pages` 69–90 s after ready.
//!
//! Now a fetch hands its bytes to [`SpanWriteQueue::persist`], which never
//! touches the disk beyond one `stat` while there is room. The bytes become
//! a [`PendingSpan`], readable by every span reader at once, and the write
//! is queued for one background thread, which writes each span through a
//! temp file and a rename (so a reader never sees a partial span), starts
//! its writeback ([`crate::writeback::start`]), accounts it in the disk
//! cache, and only then releases the in-memory copy.
//!
//! Bounds, in order:
//! 1. **Queue** ([`SPAN_WRITE_QUEUE_BYTES`]): bytes waiting to be written.
//!    A queued span counts as cached at once (a query confirms its terms
//!    present); a write that then fails bumps [`SpanWriteQueue::failures`],
//!    which invalidates those confirmations.
//! 2. **Held** ([`SPAN_HELD_BYTES`]): once the queue is full, a further
//!    span's write is dropped and counted, and the span stays readable only
//!    while the fetching request holds it (the returned hold). It never
//!    counts as cached, so no other request skips a fetch because of it; a
//!    later request fetches it again.
//! 3. **Inline**: with both full the span is written on the calling thread,
//!    as before this module. That is backpressure at a point where the disk
//!    is far behind; it keeps memory bounded by the two caps.
//!
//! A span another request's correctness depends on beyond the fetcher's
//! own lifetime ([`SpanWriteQueue::persist_required`]: `doc_vectors.bin`
//! headers, which shared completeness checks and single-flight waiters
//! read) is never dropped: it is queued past the cap.

#![cfg_attr(not(feature = "s3"), allow(dead_code))]

use std::collections::VecDeque;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use kosha_cache::Cache;
use kosha_segment::span_cache::{self, PendingSpan};

/// Most span bytes queued for writing (including the one being written).
/// They live in memory until written. 256 MiB is about two seconds of the
/// cache disk's 125 MiB/s, so a queued span is on disk within seconds of
/// its fetch, and ~1% of the pod's 24 GiB live-memory watermark.
pub const SPAN_WRITE_QUEUE_BYTES: u64 = 256 << 20;

/// Most bytes of dropped writes held in memory by the requests that fetched
/// them (~4% of the 24 GiB watermark). Past it a span is written inline.
pub const SPAN_HELD_BYTES: u64 = 1 << 30;

/// Least time between two "queue full" log lines.
const DROP_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// One queued job: write a span, or account a span written inline.
pub struct SpanWrite {
    /// `None`: already written inline; only the cache accounting is left.
    span: Option<Arc<PendingSpan>>,
    /// The span's disk-cache key: `{rel}/{file}.spans/{name}`.
    key: String,
    /// Bytes counted in the queue for this job.
    queued: u64,
}

#[derive(Default)]
struct Queue {
    writes: VecDeque<SpanWrite>,
    /// Bytes queued plus the bytes of the write in progress.
    bytes: u64,
}

#[derive(Default)]
struct DropLog {
    writes: u64,
    bytes: u64,
    inline: u64,
    at: Option<Instant>,
}

/// See the module docs. One per process.
pub struct SpanWriteQueue {
    cap: u64,
    held_cap: u64,
    queue: Mutex<Queue>,
    cv: Condvar,
    held: Arc<AtomicU64>,
    written: AtomicU64,
    written_bytes: AtomicU64,
    skipped: AtomicU64,
    dropped: AtomicU64,
    dropped_bytes: AtomicU64,
    inline: AtomicU64,
    failed: AtomicU64,
    drop_log: Mutex<DropLog>,
}

/// Point-in-time totals for `/stats` and `/metrics`.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpanWriteStats {
    pub queued_bytes: u64,
    pub held_bytes: u64,
    pub written: u64,
    pub written_bytes: u64,
    pub dropped: u64,
    pub dropped_bytes: u64,
    pub inline: u64,
    pub failed: u64,
}

impl SpanWriteQueue {
    pub fn new(cap: u64, held_cap: u64) -> Self {
        Self {
            cap,
            held_cap,
            queue: Mutex::new(Queue::default()),
            cv: Condvar::new(),
            held: Arc::new(AtomicU64::new(0)),
            written: AtomicU64::new(0),
            written_bytes: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            dropped_bytes: AtomicU64::new(0),
            inline: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            drop_log: Mutex::new(DropLog::default()),
        }
    }

    /// Make the fetched range `[start, start + bytes.len())` of
    /// `{segment_dir}/{file}` readable now, and queue its write into the
    /// span cache under `{rel}/{file}.spans/…`.
    ///
    /// `Some(hold)` when the write was dropped because the queue is full:
    /// the span is readable only while the caller keeps the hold, which it
    /// must do until it has read the range. `None` when the write was
    /// queued (readable until it lands, then from disk), written inline
    /// (both caps full), or the span is already on disk.
    #[must_use = "a dropped span is readable only while its hold lives"]
    pub fn persist(
        &self,
        segment_dir: &Path,
        rel: &str,
        file: &str,
        start: u64,
        bytes: Vec<u8>,
    ) -> Option<Arc<PendingSpan>> {
        self.persist_with(segment_dir, rel, file, start, bytes, false)
    }

    /// [`Self::persist`] for a span that is never dropped: queued even past
    /// the cap. For spans read beyond the fetching request (headers).
    pub fn persist_required(
        &self,
        segment_dir: &Path,
        rel: &str,
        file: &str,
        start: u64,
        bytes: Vec<u8>,
    ) {
        let none = self.persist_with(segment_dir, rel, file, start, bytes, true);
        debug_assert!(none.is_none());
    }

    fn persist_with(
        &self,
        segment_dir: &Path,
        rel: &str,
        file: &str,
        start: u64,
        bytes: Vec<u8>,
        required: bool,
    ) -> Option<Arc<PendingSpan>> {
        let len = bytes.len() as u64;
        if len == 0 {
            return None;
        }
        let name = span_cache::span_file_name(start, start + len);
        let dest = span_cache::spans_dir(segment_dir, file).join(&name);
        if fs::metadata(&dest).is_ok_and(|m| m.len() == len) {
            return None;
        }
        let key = format!("{rel}/{file}{}/{name}", span_cache::SPANS_SUFFIX);
        let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        if required || queue.bytes.saturating_add(len) <= self.cap {
            let span = PendingSpan::register(segment_dir, file, start, bytes, true);
            queue.bytes += len;
            queue.writes.push_back(SpanWrite {
                span: Some(span),
                key,
                queued: len,
            });
            drop(queue);
            self.cv.notify_one();
            return None;
        }
        drop(queue);
        if self.held.load(Ordering::Acquire).saturating_add(len) <= self.held_cap {
            self.note_drop(len, false);
            return Some(PendingSpan::register_held(
                segment_dir,
                file,
                start,
                bytes,
                Arc::clone(&self.held),
            ));
        }
        // Both full: write here, as before this queue existed; the writer
        // thread accounts it (it holds the cache).
        self.note_drop(len, true);
        let span = PendingSpan::register(segment_dir, file, start, bytes, true);
        if self.write_file(&span) {
            let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
            queue.writes.push_back(SpanWrite {
                span: None,
                key,
                queued: 0,
            });
            drop(queue);
            self.cv.notify_one();
        }
        None
    }

    /// The next queued job, waiting up to `timeout` for one.
    pub fn pop_timeout(&self, timeout: Duration) -> Option<SpanWrite> {
        let queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        let (mut queue, _) = self
            .cv
            .wait_timeout_while(queue, timeout, |q| q.writes.is_empty())
            .unwrap_or_else(|e| e.into_inner());
        queue.writes.pop_front()
    }

    /// Run one job: write its span into the span cache, account it in
    /// `cache`, then release its in-memory copy (the rename lands first, so
    /// the range is readable throughout). The queue's bytes are given back
    /// and a write that did not complete — an error or a panic — is counted
    /// in [`Self::failures`] whatever happens inside.
    pub fn write(&self, cache: &Cache, job: SpanWrite) {
        struct Done<'a> {
            q: &'a SpanWriteQueue,
            bytes: u64,
            ok: bool,
        }
        impl Drop for Done<'_> {
            fn drop(&mut self) {
                if !self.ok {
                    self.q.failed.fetch_add(1, Ordering::AcqRel);
                }
                let mut queue = self.q.queue.lock().unwrap_or_else(|e| e.into_inner());
                queue.bytes = queue.bytes.saturating_sub(self.bytes);
            }
        }
        let mut done = Done {
            q: self,
            bytes: job.queued,
            ok: false,
        };
        done.ok = match &job.span {
            Some(span) => self.write_file(span),
            None => true,
        };
        if done.ok {
            cache.note_external_write(&job.key);
        }
        drop(job);
    }

    /// Write `span` to its span file unless it (or its whole file) is
    /// already there; `false` on failure (logged and counted by the
    /// caller's accounting).
    fn write_file(&self, span: &PendingSpan) -> bool {
        static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
        let len = span.bytes().len() as u64;
        let dest = span.dest();
        if fs::metadata(&dest).is_ok_and(|m| m.len() == len)
            || fs::metadata(span.whole_path()).is_ok_and(|m| m.len() >= span.end())
        {
            // A racing fetch wrote it, or a promotion landed the whole file
            // (and dropped the span directory): not recreated.
            self.skipped.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        let tmp = span.dir().join(format!(
            ".tmp.{}.{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        // Only the span directory is created: a segment directory that is
        // gone (the segment was deleted or collected since the fetch) is
        // not brought back for one span.
        let written = create_span_dir(span.dir())
            .and_then(|_| {
                let mut f = fs::File::create(&tmp)?;
                f.write_all(span.bytes())?;
                crate::writeback::start(&f, 0, 0);
                Ok(())
            })
            .and_then(|_| fs::rename(&tmp, &dest));
        match written {
            Ok(()) => {
                self.written.fetch_add(1, Ordering::Relaxed);
                self.written_bytes.fetch_add(len, Ordering::Relaxed);
                true
            }
            Err(e) => {
                if let Err(rm) = fs::remove_file(&tmp) {
                    if rm.kind() != std::io::ErrorKind::NotFound {
                        eprintln!("WARN: failed to remove {}: {rm}", tmp.display());
                    }
                }
                eprintln!(
                    "WARN: failed to persist span {}: {e} (a later read fetches it again)",
                    dest.display()
                );
                false
            }
        }
    }

    /// Run every queued job now, on this thread.
    #[cfg(test)]
    pub fn drain(&self, cache: &Cache) {
        while let Some(job) = self.pop_timeout(Duration::ZERO) {
            self.write(cache, job);
        }
    }

    /// Bytes queued or being written.
    pub fn queued_bytes(&self) -> u64 {
        self.queue.lock().unwrap_or_else(|e| e.into_inner()).bytes
    }

    /// Span writes that did not complete since the process started (queued
    /// or inline). Every query that confirmed a term present on the strength
    /// of a queued span did so before this moved, so presence entries are
    /// stamped with it (see `AppState::presence_generation`).
    pub fn failures(&self) -> u64 {
        self.failed.load(Ordering::Acquire)
    }

    fn note_drop(&self, len: u64, inline: bool) {
        if inline {
            self.inline.fetch_add(1, Ordering::Relaxed);
        } else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            self.dropped_bytes.fetch_add(len, Ordering::Relaxed);
        }
        let mut log = self.drop_log.lock().unwrap_or_else(|e| e.into_inner());
        if inline {
            log.inline += 1;
        } else {
            log.writes += 1;
            log.bytes += len;
        }
        if log.at.is_some_and(|at| at.elapsed() < DROP_LOG_INTERVAL) {
            return;
        }
        eprintln!(
            "WARN: span write queue full ({} MiB queued for the cache disk): since the last \
             report dropped {} span write(s), {:.1} MiB (held by the requests that fetched \
             them, {} MiB held now; later requests fetch them again), and wrote {} inline \
             because the held cap ({} MiB) was reached",
            self.cap >> 20,
            log.writes,
            log.bytes as f64 / (1 << 20) as f64,
            self.held.load(Ordering::Relaxed) >> 20,
            log.inline,
            self.held_cap >> 20,
        );
        *log = DropLog {
            at: Some(Instant::now()),
            ..DropLog::default()
        };
    }

    pub fn stats(&self) -> SpanWriteStats {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        SpanWriteStats {
            queued_bytes: self.queued_bytes(),
            held_bytes: get(&self.held),
            written: get(&self.written),
            written_bytes: get(&self.written_bytes),
            dropped: get(&self.dropped),
            dropped_bytes: get(&self.dropped_bytes),
            inline: get(&self.inline),
            failed: get(&self.failed),
        }
    }

    /// Process-lifetime totals for `GET /stats`.
    pub fn to_json(&self) -> serde_json::Value {
        let s = self.stats();
        serde_json::json!({
            "queue_cap_bytes": self.cap,
            "held_cap_bytes": self.held_cap,
            "queued_bytes": s.queued_bytes,
            "held_bytes": s.held_bytes,
            "written": s.written,
            "written_bytes": s.written_bytes,
            "skipped": self.skipped.load(Ordering::Relaxed),
            "dropped": s.dropped,
            "dropped_bytes": s.dropped_bytes,
            "inline": s.inline,
            "failed": s.failed,
            "writeback_advice_skipped": crate::writeback::skipped(),
        })
    }
}

fn create_span_dir(dir: &Path) -> std::io::Result<()> {
    match fs::create_dir(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_segment::span_cache::{LocalRangeReader, SpanIndex};

    fn fixture(name: &str) -> (std::path::PathBuf, Cache) {
        let root =
            std::env::temp_dir().join(format!("kosha-span-writes-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("ns/seg")).unwrap();
        let cache = Cache::new(root.clone());
        (root, cache)
    }

    /// The calling thread never writes: the span is readable at once from
    /// memory, the file appears only when the writer runs, and the writer
    /// releases the memory copy only once the file is in place.
    #[test]
    fn persist_queues_the_write_and_the_span_reads_from_memory_until_it_lands() {
        let (root, cache) = fixture("queued");
        let seg = root.join("ns/seg");
        let writes = SpanWriteQueue::new(1 << 20, 1 << 20);
        let bytes: Vec<u8> = (0..100u8).collect();
        assert!(writes
            .persist(&seg, "ns/seg", "vector.idx", 1000, bytes.clone())
            .is_none());
        let dest =
            span_cache::spans_dir(&seg, "vector.idx").join(span_cache::span_file_name(1000, 1100));
        assert!(!dest.exists(), "nothing written on the calling thread");
        assert_eq!(writes.queued_bytes(), 100);
        let idx = SpanIndex::load(&seg, "vector.idx");
        assert!(idx.covers(1010, 1020), "a queued span counts as cached");
        assert_eq!(idx.read(1010, 1020).as_deref(), Some(&bytes[10..20]));
        let mut reader = LocalRangeReader::new(&seg, "vector.idx");
        assert_eq!(reader.read(u64::MAX, 1050, 5), Some(&bytes[50..55]));
        drop((idx, reader));

        writes.drain(&cache);
        assert_eq!(fs::read(&dest).unwrap(), bytes);
        assert_eq!(writes.queued_bytes(), 0);
        let idx = SpanIndex::load(&seg, "vector.idx");
        assert!(idx.covers(1000, 1100));
        assert_eq!(idx.read(1090, 1100).as_deref(), Some(&bytes[90..]));
        assert_eq!(writes.to_json()["written"], 1);
        assert!(cache.total_size() >= 100, "accounted in the disk cache");
        // Already on disk: nothing queued again.
        assert!(writes
            .persist(&seg, "ns/seg", "vector.idx", 1000, bytes)
            .is_none());
        assert_eq!(writes.queued_bytes(), 0);
        let _ = fs::remove_dir_all(&root);
    }

    /// A segment deleted between the fetch and the write is not recreated
    /// for its span: the write fails, is counted, and the memory goes.
    #[test]
    fn a_deleted_segment_is_not_recreated_by_its_queued_span() {
        let (root, cache) = fixture("deleted");
        let seg = root.join("ns/seg");
        let writes = SpanWriteQueue::new(1 << 20, 1 << 20);
        assert!(writes
            .persist(&seg, "ns/seg", "doc_store.bin", 0, vec![1u8; 10])
            .is_none());
        fs::remove_dir_all(&seg).unwrap();
        writes.drain(&cache);
        assert!(!seg.exists());
        assert_eq!(writes.to_json()["failed"], 1);
        assert_eq!(writes.queued_bytes(), 0);
        assert!(SpanIndex::load(&seg, "doc_store.bin").is_empty());
        let _ = fs::remove_dir_all(&root);
    }

    /// A full queue drops the write and counts it; the fetching request
    /// still reads the span through its hold, nobody counts it as cached,
    /// and it is gone (memory and all) once the hold drops.
    #[test]
    fn a_full_queue_drops_and_counts_and_the_holder_still_reads() {
        let (root, cache) = fixture("full");
        let seg = root.join("ns/seg");
        let writes = SpanWriteQueue::new(150, 1 << 20);
        let a = vec![1u8; 100];
        let b = vec![2u8; 100];
        assert!(writes
            .persist(&seg, "ns/seg", "doc_store.bin", 0, a.clone())
            .is_none());
        let hold = writes
            .persist(&seg, "ns/seg", "doc_store.bin", 500, b.clone())
            .expect("over the cap: dropped, and held by the caller");
        let stats = writes.to_json();
        assert_eq!(
            (stats["dropped"].as_u64(), stats["dropped_bytes"].as_u64()),
            (Some(1), Some(100))
        );
        assert_eq!(writes.queued_bytes(), 100, "the dropped span is not queued");
        assert_eq!(writes.stats().held_bytes, 100, "held bytes are counted");

        let idx = SpanIndex::load(&seg, "doc_store.bin");
        assert!(
            !idx.covers(500, 600),
            "a dropped span never counts as cached"
        );
        assert_eq!(idx.read(510, 520).as_deref(), Some(&b[10..20]), "but reads");
        assert_eq!(
            kosha_segment::span_cache::read_local_range(&seg, "doc_store.bin", u64::MAX, 550, 10)
                .as_deref(),
            Some(&b[50..60])
        );
        drop(idx);

        writes.drain(&cache);
        drop(hold);
        assert_eq!(writes.stats().held_bytes, 0);
        let idx = SpanIndex::load(&seg, "doc_store.bin");
        assert_eq!(
            idx.read(0, 100).as_deref(),
            Some(&a[..]),
            "the queued one landed"
        );
        assert_eq!(idx.read(500, 600), None, "the dropped one is gone");
        assert!(!span_cache::spans_dir(&seg, "doc_store.bin")
            .join(span_cache::span_file_name(500, 600))
            .exists());
        // Room again once the queue drained.
        assert!(writes
            .persist(&seg, "ns/seg", "doc_store.bin", 500, b)
            .is_none());
        let _ = fs::remove_dir_all(&root);
    }

    /// With the queue and the held cap both full a span is written on the
    /// calling thread (bounded memory), and accounted by the writer; a
    /// required span is queued past the cap and never dropped.
    #[test]
    fn past_both_caps_writes_inline_and_required_spans_are_never_dropped() {
        let (root, cache) = fixture("inline");
        let seg = root.join("ns/seg");
        let writes = SpanWriteQueue::new(100, 100);
        assert!(writes
            .persist(&seg, "ns/seg", "f.bin", 0, vec![1; 100])
            .is_none());
        let hold = writes.persist(&seg, "ns/seg", "f.bin", 100, vec![2; 100]);
        assert!(hold.is_some(), "queue full: held");
        assert!(writes
            .persist(&seg, "ns/seg", "f.bin", 200, vec![3; 100])
            .is_none());
        let inline =
            span_cache::spans_dir(&seg, "f.bin").join(span_cache::span_file_name(200, 300));
        assert_eq!(fs::read(&inline).unwrap(), vec![3; 100], "written inline");
        assert_eq!(writes.stats().inline, 1);
        assert_eq!(writes.stats().held_bytes, 100, "inline bytes are not held");

        writes.persist_required(&seg, "ns/seg", "doc_vectors.bin", 0, vec![4; 64]);
        assert_eq!(writes.queued_bytes(), 164, "queued past the cap");
        assert!(SpanIndex::load(&seg, "doc_vectors.bin").covers(0, 64));

        let before = cache.total_size();
        writes.drain(&cache);
        assert!(
            cache.total_size() >= before + 100 + 64,
            "inline + required accounted"
        );
        assert_eq!(writes.stats().failed, 0);
        drop(hold);
        let _ = fs::remove_dir_all(&root);
    }

    /// A queued span whose whole file landed meanwhile (a promotion that
    /// dropped the span directory) is not written: the directory stays gone.
    #[test]
    fn a_promoted_file_is_not_given_its_span_directory_back() {
        let (root, cache) = fixture("promoted");
        let seg = root.join("ns/seg");
        let writes = SpanWriteQueue::new(1 << 20, 1 << 20);
        assert!(writes
            .persist(&seg, "ns/seg", "doc_store.bin", 10, vec![1; 10])
            .is_none());
        fs::write(seg.join("doc_store.bin"), [0u8; 100]).unwrap();
        writes.drain(&cache);
        assert!(!span_cache::spans_dir(&seg, "doc_store.bin").exists());
        assert_eq!(writes.stats().failed, 0);
        let _ = fs::remove_dir_all(&root);
    }
}
