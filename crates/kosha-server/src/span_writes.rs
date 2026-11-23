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
//!    is far behind; it keeps memory bounded by the two caps, and avoids a
//!    429 the client would not retry. Expected 0 at target load:
//!    `kosha_span_writes_total{outcome="inline"}` is alert-worthy.
//!
//! A span another request's correctness depends on beyond the fetcher's
//! own lifetime ([`SpanWriteQueue::persist_required`]: `doc_vectors.bin`
//! headers, which shared completeness checks and single-flight waiters
//! read) is never dropped: it is queued past the cap.

#![cfg_attr(not(feature = "s3"), allow(dead_code))]

use std::collections::VecDeque;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard};
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
    /// Exclusion between span writes and the drop of a superseded span
    /// directory ([`Self::drop_superseded`]), striped by the directory's
    /// cache key. A write holds its stripe shared across its whole-file
    /// check, its rename into the span directory and its cache accounting;
    /// a drop holds it exclusive, so a drop never walks a directory a write
    /// is adding to (finding 33). Writes never wait for each other.
    /// Lock order: a stripe, then the cache's locks.
    dir_gates: [RwLock<()>; DIR_GATE_STRIPES],
}

/// Stripes of [`SpanWriteQueue::dir_gates`]. A drop waits only for the
/// writes in flight in its stripe: 1 background writer plus the inline
/// writers (0 at target load, 44 in a whole run10), so with 64 stripes a
/// drop waits for an unrelated directory's write with probability about
/// (1 + inline) / 64.
const DIR_GATE_STRIPES: usize = 64;

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
            dir_gates: std::array::from_fn(|_| RwLock::new(())),
        }
    }

    /// The gate of the span directory `dir_key` (`{rel}/{file}.spans`).
    fn dir_gate(&self, dir_key: &str) -> &RwLock<()> {
        let mut h = std::hash::DefaultHasher::new();
        dir_key.trim_matches('/').hash(&mut h);
        &self.dir_gates[h.finish() as usize % DIR_GATE_STRIPES]
    }

    /// Shared hold on the gate of the span directory holding the span file
    /// `key` (`{rel}/{file}.spans/{name}`).
    fn write_gate(&self, key: &str) -> RwLockReadGuard<'_, ()> {
        let dir_key = key.rsplit_once('/').map_or(key, |(dir, _)| dir);
        self.dir_gate(dir_key)
            .read()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Drop the span directory `dir_key` (`{rel}/{file}.spans`) of a file a
    /// promotion just landed whole
    /// ([`Cache::drop_superseded_dir_unless_pinned`]), never interleaved
    /// with a span write into it. Without the gate a write that checked for
    /// the whole file before it landed renamed its span in after the drop's
    /// walk: the drop failed with ENOTEMPTY and left the directory behind,
    /// and the cache kept counting the spans it had already unlinked, or a
    /// write accounted mid-drop stayed counted with its file gone. With it
    /// the write either runs first (and its span is dropped) or sees the
    /// whole file and writes nothing.
    pub fn drop_superseded(
        &self,
        cache: &Cache,
        dir_key: &str,
    ) -> Result<Option<u64>, kosha_core::KoshaError> {
        let _gate = self
            .dir_gate(dir_key)
            .write()
            .unwrap_or_else(|e| e.into_inner());
        cache.drop_superseded_dir_unless_pinned(dir_key)
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
        // Its accounting job takes the gate again: a drop in between
        // removes the untracked file, and the job then accounts 0 bytes.
        let gate = self.write_gate(&key);
        let ok = self.write_file(&span);
        drop(gate);
        if !ok {
            // It counted as cached while registered (a query may have
            // confirmed its term): invalidate that, as for a queued write.
            self.failed.fetch_add(1, Ordering::AcqRel);
        } else {
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
        // The whole-file check, the rename and the accounting are one step
        // relative to a drop of this span directory.
        let gate = self.write_gate(&job.key);
        done.ok = match &job.span {
            Some(span) => self.write_file(span),
            None => true,
        };
        if done.ok {
            cache.note_external_write(&job.key);
        }
        drop(gate);
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

    /// An inline write that fails counts as a failed write, so a term a
    /// query confirmed on the strength of it is invalidated.
    #[test]
    fn a_failed_inline_write_counts_as_failed() {
        let (root, _cache) = fixture("inline-fail");
        let writes = SpanWriteQueue::new(0, 0);
        let gone = root.join("ns/gone");
        assert!(writes
            .persist(&gone, "ns/gone", "f.bin", 0, vec![1; 10])
            .is_none());
        assert_eq!(writes.stats().inline, 1);
        assert_eq!(writes.failures(), 1);
        assert!(!gone.exists());
        let _ = fs::remove_dir_all(&root);
    }

    /// Bytes under `dir`, as the disk has them.
    fn bytes_on_disk(dir: &Path) -> u64 {
        fs::read_dir(dir)
            .map(|d| {
                d.flatten()
                    .filter_map(|e| e.metadata().ok())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0)
    }

    /// Finding 33's failure shape, made deterministic: the drop unlinks
    /// every span, then the final rmdir of the span directory fails (here
    /// EACCES from a read-only segment directory; on staging ENOTEMPTY,
    /// because a write renamed a span in after the walk). The cache kept
    /// counting every span the drop had already unlinked.
    #[cfg(unix)]
    #[test]
    fn a_failed_superseded_drop_stops_counting_the_spans_it_removed() {
        use std::os::unix::fs::PermissionsExt;
        let (root, cache) = fixture("failed-drop");
        let seg = root.join("ns/seg");
        let writes = SpanWriteQueue::new(1 << 20, 1 << 20);
        for i in 0..8u64 {
            assert!(writes
                .persist(&seg, "ns/seg", "doc_store.bin", i * 1000, vec![7; 1000])
                .is_none());
        }
        writes.drain(&cache);
        assert_eq!(cache.total_size(), 8000);
        // The promotion landed the whole file (written behind the cache).
        fs::write(seg.join("doc_store.bin"), vec![0u8; 8000]).unwrap();
        fs::set_permissions(&seg, fs::Permissions::from_mode(0o555)).unwrap();
        if fs::write(seg.join("probe"), b"").is_ok() {
            println!("skipped: running as root, a read-only directory cannot block removal");
            fs::set_permissions(&seg, fs::Permissions::from_mode(0o755)).unwrap();
            let _ = fs::remove_dir_all(&root);
            return;
        }
        let res = writes.drop_superseded(&cache, "ns/seg/doc_store.bin.spans");
        fs::set_permissions(&seg, fs::Permissions::from_mode(0o755)).unwrap();
        let spans = span_cache::spans_dir(&seg, "doc_store.bin");
        assert!(res.is_err(), "the rmdir after the walk failed");
        assert!(spans.exists());
        assert_eq!(bytes_on_disk(&spans), 0, "every span was unlinked");
        assert_eq!(
            cache.total_size(),
            0,
            "spans the failed drop removed are no longer counted"
        );
        let _ = fs::remove_dir_all(&root);
    }

    /// The race itself, on the real writer, the real inline path and the
    /// real promotion drop (`SpanHydrator::finish_promotion`): span writes
    /// into `doc_store.bin.spans` are in flight when the promotion lands
    /// the whole file and drops the span directory. Even iterations write
    /// from the background writer thread, odd ones inline from two request
    /// threads (both caps full). Afterwards the span directory must be gone
    /// (a failed drop leaves it, and is logged), no write may have failed,
    /// and the cache must count exactly the span bytes on disk. On main 13–
    /// 20% of iterations broke one of these (finding 33).
    /// `SPAN_RACE_ITERS` sets the iteration count (default 200).
    #[test]
    fn span_writes_never_interleave_with_a_superseded_drop() {
        use crate::doc_spans::{SpanConfig, SpanHydrator};
        let iters: usize = std::env::var("SPAN_RACE_ITERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200);
        const SPAN: usize = 1 << 20;
        const SPANS: u64 = 24;
        let (mut orphans, mut failed, mut miscounted) = (0, 0, 0);
        let mut drop_ms = Vec::with_capacity(iters);
        for it in 0..iters {
            let (root, cache) = fixture(&format!("race-{it}"));
            let cache = Arc::new(cache);
            let seg = root.join("ns/seg");
            let inline = it % 2 == 1;
            let (cap, held) = if inline { (0, 0) } else { (1 << 30, 1 << 30) };
            let writes = Arc::new(SpanWriteQueue::new(cap, held));
            fn persist(writes: &SpanWriteQueue, seg: &Path, i: u64) {
                let bytes = vec![i as u8; SPAN];
                assert!(writes
                    .persist(seg, "ns/seg", "doc_store.bin", i * SPAN as u64, bytes)
                    .is_none());
            }
            let workers: Vec<_> = if inline {
                (0..2u64)
                    .map(|t| {
                        let (writes, seg) = (writes.clone(), seg.clone());
                        std::thread::spawn(move || {
                            for i in (t..SPANS).step_by(2) {
                                persist(&writes, &seg, i);
                            }
                        })
                    })
                    .collect()
            } else {
                for i in 0..SPANS {
                    persist(&writes, &seg, i);
                }
                let (writes, cache) = (writes.clone(), cache.clone());
                vec![std::thread::spawn(move || writes.drain(&cache))]
            };
            // Land the whole file part-way through the writes, as
            // `promote_doc_store` does, then drop as it does.
            let spans = span_cache::spans_dir(&seg, "doc_store.bin");
            let want = (it as u64 / 2 % SPANS).max(1);
            while fs::read_dir(&spans).map_or(0, |d| d.count() as u64) < want {
                std::hint::spin_loop();
            }
            let tmp = seg.join(".doc_store.tmp");
            fs::write(&tmp, vec![0u8; SPAN * SPANS as usize]).unwrap();
            fs::rename(&tmp, seg.join("doc_store.bin")).unwrap();
            let t = Instant::now();
            SpanHydrator::new(root.clone(), SpanConfig::default())
                .finish_promotion(&cache, &writes, &seg, true);
            drop_ms.push(t.elapsed().as_secs_f64() * 1e3);
            for w in workers {
                w.join().unwrap();
            }
            writes.drain(&cache); // inline writes' accounting jobs
            orphans += usize::from(spans.exists());
            failed += writes.stats().failed;
            if cache.total_size() != bytes_on_disk(&spans) {
                miscounted += 1;
                eprintln!(
                    "it={it} inline={inline} counted={} on_disk={}",
                    cache.total_size(),
                    bytes_on_disk(&spans)
                );
            }
            let _ = fs::remove_dir_all(&root);
        }
        drop_ms.sort_by(f64::total_cmp);
        eprintln!(
            "span drop race: iters={iters} orphan_dirs={orphans} failed_writes={failed} \
             miscounted={miscounted} drop_ms p50={:.2} max={:.2}",
            drop_ms[iters / 2],
            drop_ms[iters - 1]
        );
        assert_eq!(
            (orphans, failed, miscounted),
            (0, 0, 0),
            "a span write interleaved with the promotion drop"
        );
    }

    /// The gate is per span directory: while a drop holds one directory's
    /// stripe, a write into a directory on another stripe goes ahead, and a
    /// write into the dropped directory waits for the drop.
    #[test]
    fn a_drop_blocks_only_writes_into_its_own_span_directory() {
        let (root, cache) = fixture("stripes");
        let writes = Arc::new(SpanWriteQueue::new(0, 0));
        let dropped = "ns/seg/doc_store.bin.spans";
        let other = (0..)
            .map(|i| format!("ns/seg{i}"))
            .find(|rel| {
                let key = format!("{rel}/doc_store.bin.spans");
                !std::ptr::eq(writes.dir_gate(&key), writes.dir_gate(dropped))
            })
            .unwrap();
        fs::create_dir_all(root.join(&other)).unwrap();
        let gate = writes.dir_gate(dropped).write().unwrap();
        let inline_write = |rel: String| {
            let (writes, seg) = (writes.clone(), root.join(&rel));
            std::thread::spawn(move || {
                assert!(writes
                    .persist(&seg, &rel, "doc_store.bin", 0, vec![1; 10])
                    .is_none());
            })
        };
        let free = inline_write(other.clone());
        let blocked = inline_write("ns/seg".into());
        free.join().unwrap();
        assert!(span_cache::spans_dir(&root.join(&other), "doc_store.bin").exists());
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !blocked.is_finished(),
            "a write into the directory being dropped waits for the drop"
        );
        drop(gate);
        blocked.join().unwrap();
        writes.drain(&cache);
        assert_eq!(cache.total_size(), 20);
        let _ = fs::remove_dir_all(&root);
    }
}
