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
//! 37.8 s in planning.
//!
//! Now a fetch hands its bytes to [`SpanWriteQueue::persist`], which never
//! touches the disk beyond one `stat`. The bytes become a
//! [`PendingSpan`], readable by every span reader at once, and the write is
//! queued for one background thread, which writes each span through a temp
//! file and a rename (so a reader never sees a partial span), starts its
//! writeback ([`crate::writeback::start`]), accounts it in the disk cache,
//! and only then releases the in-memory copy.
//!
//! The queue is bounded in bytes ([`SPAN_WRITE_QUEUE_BYTES`]). The disk
//! cannot absorb writes faster than it writes, so once the queue is full a
//! further span is not queued: the write is dropped and counted, and the
//! span stays in memory for as long as the fetching request holds it (the
//! returned hold), so that request still reads it. A later request that
//! needs the range fetches it again. A dropped span never counts as cached,
//! so no other reader skips a fetch because of it.

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
/// They live in memory until written, so this is also the most memory the
/// queue holds. 256 MiB is about two seconds of the cache disk's 125 MiB/s,
/// so a span is on disk within a few seconds of its fetch, and it is
/// ~1% of the pod's 24 GiB live-memory watermark.
pub const SPAN_WRITE_QUEUE_BYTES: u64 = 256 << 20;

/// Least time between two "queue full" log lines.
const DROP_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// One queued span write.
pub struct SpanWrite {
    span: Arc<PendingSpan>,
    /// The span's disk-cache key: `{rel}/{file}.spans/{name}`.
    key: String,
}

#[derive(Default)]
struct Queue {
    writes: VecDeque<SpanWrite>,
    /// Bytes queued plus the bytes of the write in progress.
    bytes: u64,
}

/// See the module docs. One per process.
pub struct SpanWriteQueue {
    cap: u64,
    queue: Mutex<Queue>,
    cv: Condvar,
    written: AtomicU64,
    written_bytes: AtomicU64,
    dropped: AtomicU64,
    dropped_bytes: AtomicU64,
    failed: AtomicU64,
    /// Drops not yet reported, and when the last report was made.
    drop_log: Mutex<(u64, u64, Option<Instant>)>,
}

impl SpanWriteQueue {
    pub fn new(cap: u64) -> Self {
        Self {
            cap,
            queue: Mutex::new(Queue::default()),
            cv: Condvar::new(),
            written: AtomicU64::new(0),
            written_bytes: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            dropped_bytes: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            drop_log: Mutex::new((0, 0, None)),
        }
    }

    /// Make the fetched range `[start, start + bytes.len())` of
    /// `{segment_dir}/{file}` readable now, and queue its write into the
    /// span cache under `{rel}/{file}.spans/…`. Never writes on the calling
    /// thread.
    ///
    /// `Some(hold)` when the write was dropped because the queue is full:
    /// the span is readable only while the caller keeps the hold, which it
    /// must do until it has read the range. `None` when the write was
    /// queued (readable until it lands, then from disk) or the span is
    /// already on disk.
    #[must_use = "a dropped span is readable only while its hold lives"]
    pub fn persist(
        &self,
        segment_dir: &Path,
        rel: &str,
        file: &str,
        start: u64,
        bytes: Vec<u8>,
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
        let mut queue = self.queue.lock().unwrap();
        let queued = queue.bytes.saturating_add(len) <= self.cap;
        let span = PendingSpan::register(segment_dir, file, start, bytes, queued);
        if queued {
            queue.bytes += len;
            queue.writes.push_back(SpanWrite {
                span,
                key: format!("{rel}/{file}{}/{name}", span_cache::SPANS_SUFFIX),
            });
            drop(queue);
            self.cv.notify_one();
            return None;
        }
        drop(queue);
        self.note_drop(len);
        Some(span)
    }

    /// The next queued write, waiting up to `timeout` for one.
    pub fn pop_timeout(&self, timeout: Duration) -> Option<SpanWrite> {
        let queue = self.queue.lock().unwrap();
        let (mut queue, _) = self
            .cv
            .wait_timeout_while(queue, timeout, |q| q.writes.is_empty())
            .unwrap();
        queue.writes.pop_front()
    }

    /// Write `job`'s span into the span cache and account it in `cache`,
    /// then release its in-memory copy. Through a temp file and a rename,
    /// so a reader never sees a partial span; the rename lands before the
    /// copy goes, so the range is readable throughout.
    pub fn write(&self, cache: &Cache, job: SpanWrite) {
        static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
        let span = &job.span;
        let len = span.bytes().len() as u64;
        let dest = span.dest();
        let tmp = span.dir().join(format!(
            ".tmp.{}.{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let already = fs::metadata(&dest).is_ok_and(|m| m.len() == len);
        let written = if already {
            Ok(())
        } else {
            // Only the span directory is created: a segment directory that
            // is gone (the segment was deleted or collected since the
            // fetch) is not brought back for one span.
            create_span_dir(span.dir())
                .and_then(|_| {
                    let mut f = fs::File::create(&tmp)?;
                    f.write_all(span.bytes())?;
                    crate::writeback::start(&f, 0, 0);
                    Ok(())
                })
                .and_then(|_| fs::rename(&tmp, &dest))
        };
        match written {
            Ok(()) => {
                if !already {
                    cache.note_external_write(&job.key);
                }
                self.written.fetch_add(1, Ordering::Relaxed);
                self.written_bytes.fetch_add(len, Ordering::Relaxed);
            }
            Err(e) => {
                if let Err(rm) = fs::remove_file(&tmp) {
                    if rm.kind() != std::io::ErrorKind::NotFound {
                        eprintln!("WARN: failed to remove {}: {rm}", tmp.display());
                    }
                }
                self.failed.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "WARN: failed to persist span {}: {e} (a later read fetches it again)",
                    dest.display()
                );
            }
        }
        drop(job);
        let mut queue = self.queue.lock().unwrap();
        queue.bytes = queue.bytes.saturating_sub(len);
    }

    /// Write everything queued now, on this thread.
    #[cfg(test)]
    pub fn drain(&self, cache: &Cache) {
        while let Some(job) = self.pop_timeout(Duration::ZERO) {
            self.write(cache, job);
        }
    }

    /// Bytes queued or being written.
    pub fn queued_bytes(&self) -> u64 {
        self.queue.lock().unwrap().bytes
    }

    fn note_drop(&self, len: u64) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        self.dropped_bytes.fetch_add(len, Ordering::Relaxed);
        let mut log = self.drop_log.lock().unwrap();
        log.0 += 1;
        log.1 += len;
        if log.2.is_some_and(|at| at.elapsed() < DROP_LOG_INTERVAL) {
            return;
        }
        eprintln!(
            "WARN: span write queue full ({} MiB queued for the cache disk): dropped {} span \
             write(s), {:.1} MiB, since the last report ({} total); the requests that \
             fetched them still read them, later ones fetch them again",
            self.cap >> 20,
            log.0,
            log.1 as f64 / (1 << 20) as f64,
            self.dropped.load(Ordering::Relaxed)
        );
        *log = (0, 0, Some(Instant::now()));
    }

    /// Process-lifetime totals for `GET /stats`.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "queue_cap_bytes": self.cap,
            "queued_bytes": self.queued_bytes(),
            "written": self.written.load(Ordering::Relaxed),
            "written_bytes": self.written_bytes.load(Ordering::Relaxed),
            "dropped": self.dropped.load(Ordering::Relaxed),
            "dropped_bytes": self.dropped_bytes.load(Ordering::Relaxed),
            "failed": self.failed.load(Ordering::Relaxed),
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
        let writes = SpanWriteQueue::new(1 << 20);
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
        assert!(!idx.covers_on_disk(1010, 1020));
        assert_eq!(idx.read(1010, 1020).as_deref(), Some(&bytes[10..20]));
        let mut reader = LocalRangeReader::new(&seg, "vector.idx");
        assert_eq!(reader.read(u64::MAX, 1050, 5), Some(&bytes[50..55]));
        drop((idx, reader));

        writes.drain(&cache);
        assert_eq!(fs::read(&dest).unwrap(), bytes);
        assert_eq!(writes.queued_bytes(), 0);
        let idx = SpanIndex::load(&seg, "vector.idx");
        assert!(idx.covers_on_disk(1000, 1100));
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
        let writes = SpanWriteQueue::new(1 << 20);
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
        let writes = SpanWriteQueue::new(150);
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
}
