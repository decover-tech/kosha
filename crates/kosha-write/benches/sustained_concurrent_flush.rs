//! Sustained-concurrent-write throughput harness for one namespace —
//! reproduces the exact production failure mode issue #176's follow-up
//! found live: a real, sustained (multi-minute, not bursty) ingest from
//! multiple concurrent backend workers, each calling `index_documents` then
//! immediately `flush_namespace` (matching `kosha_client.bulk()`, which
//! calls `POST /flush` after *every* `bulk()` call), against ONE shared
//! namespace. PRs #177/#178 fixed lock contention and coalesced concurrent
//! flush *calls*, but every namespace still only ever ran ONE segment write
//! at a time — a sustained-enough arrival rate outpaces that no matter how
//! well the arrivals are coalesced, and every caller queued behind the
//! backlog times out indefinitely, not just during a brief burst (confirmed
//! live: 755-document real ingest, 100% `/flush` timeout for the full
//! multi-minute duration).
//!
//! Same shape as `kosha-query`'s `knn_open_search` bench (plain binary,
//! `KOSHA_BENCH_*` env knobs, `KOSHA_BENCH_JSON` for machine-readable
//! output) — see its module doc for the convention this follows.
//!
//! Run it locally:
//! ```text
//! cargo bench -p kosha-write --bench sustained_concurrent_flush
//! # heavier / longer:
//! KOSHA_BENCH_THREADS=16 KOSHA_BENCH_DURATION_SECS=30 \
//!   cargo bench -p kosha-write --bench sustained_concurrent_flush
//! ```
//!
//! **Before/after comparison, same binary, no git-stash needed**: set
//! `KOSHA_BENCH_MAX_CONCURRENT_FLUSH_WRITERS=1` to reproduce the pre-change,
//! single-writer-per-namespace behavior exactly — `take_flush_chunks_in`
//! with `max_writers=1` always computes exactly one chunk per round
//! regardless of backlog size, which is the *same* code path
//! (`flush_coord`/`flush_done` singleflight, one `SegmentWriter` per round)
//! the pre-parallel `flush_io: Mutex<()>` design ran, just with the lock
//! renamed to `writer_slots` and capacity 1. Compare against the default
//! (`max_concurrent_flush_writers=4`):
//! ```text
//! KOSHA_BENCH_MAX_CONCURRENT_FLUSH_WRITERS=1 KOSHA_BENCH_JSON=/tmp/before.json \
//!   cargo bench -p kosha-write --bench sustained_concurrent_flush
//! KOSHA_BENCH_JSON=/tmp/after.json \
//!   cargo bench -p kosha-write --bench sustained_concurrent_flush
//! ```

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use kosha_core::{Document, DocumentId, Field, NamespaceId};
use kosha_write::Indexer;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .and_then(|v| match v.as_str() {
            "1" | "true" | "TRUE" => Some(true),
            "0" | "false" | "FALSE" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

/// Latency distribution summary over one metric's samples (nearest-rank
/// percentiles) — same shape as kosha-query's benches' `Dist`.
struct Dist {
    n: usize,
    mean: f64,
    p50: f64,
    p90: f64,
    p99: f64,
    max: f64,
}

impl Dist {
    fn from_samples(mut samples: Vec<f64>) -> Dist {
        if samples.is_empty() {
            return Dist {
                n: 0,
                mean: 0.0,
                p50: 0.0,
                p90: 0.0,
                p99: 0.0,
                max: 0.0,
            };
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pct = |p: f64| {
            let rank = ((p / 100.0) * samples.len() as f64).ceil() as usize;
            samples[rank.saturating_sub(1).min(samples.len() - 1)]
        };
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        Dist {
            n: samples.len(),
            mean,
            p50: pct(50.0),
            p90: pct(90.0),
            p99: pct(99.0),
            max: *samples.last().unwrap(),
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "n": self.n, "mean_ms": self.mean, "p50_ms": self.p50,
            "p90_ms": self.p90, "p99_ms": self.p99, "max_ms": self.max,
        })
    }
}

/// Fixed vocabulary repeated to build a `body` field of `body_words` words —
/// real ingested documents (e.g. mirrored OpenSearch documents in the
/// shadow-write path this bench reproduces) carry non-trivial text content,
/// not a handful of words; `KOSHA_BENCH_BODY_WORDS` lets a comparison run
/// dial that up to make each segment write's real CPU + I/O cost closer to
/// production-sized documents instead of this bench's lightweight default.
const VOCAB: &[&str] = &[
    "lorem",
    "ipsum",
    "dolor",
    "sit",
    "amet",
    "consectetur",
    "adipiscing",
    "elit",
    "sed",
    "do",
    "eiusmod",
    "tempor",
    "incididunt",
    "ut",
    "labore",
    "et",
    "dolore",
    "magna",
    "aliqua",
];

fn mk_doc(thread_id: usize, seq: u64, body_words: usize) -> Document {
    let body = (0..body_words)
        .map(|i| VOCAB[(thread_id + seq as usize + i) % VOCAB.len()])
        .collect::<Vec<_>>()
        .join(" ");
    Document {
        id: DocumentId(format!("bench-t{thread_id}-d{seq}")),
        fields: vec![
            Field::text(
                "title",
                format!("shadow-write doc {thread_id}-{seq} sustained ingest bench"),
            ),
            Field::text("body", body),
            Field::keyword("status", "active"),
            Field::keyword("source", "bench"),
        ],
    }
}

fn main() {
    let threads = env_usize("KOSHA_BENCH_THREADS", 8);
    let duration_secs = env_usize("KOSHA_BENCH_DURATION_SECS", 20);
    let batch_size = env_usize("KOSHA_BENCH_BATCH_SIZE", 2);
    // Same defaults Indexer::new ships -- override to KOSHA_BENCH_MAX_CONCURRENT_FLUSH_WRITERS=1
    // to reproduce pre-change single-writer-per-namespace behavior, see module doc.
    let max_writers = env_usize("KOSHA_BENCH_MAX_CONCURRENT_FLUSH_WRITERS", 4);
    let min_docs_per_writer = env_usize("KOSHA_BENCH_MIN_DOCS_PER_FLUSH_WRITER", 250);
    // High enough that index_documents's own auto-flush threshold doesn't
    // fire and add a second, uncontrolled flush trigger on top of the
    // explicit per-bulk-call flush this bench is measuring -- matches how
    // kosha_client.bulk() drives real traffic (it calls /flush itself after
    // every bulk() call; auto-flush is a safety net, not the common path).
    let flush_threshold = env_usize("KOSHA_BENCH_FLUSH_THRESHOLD", 1_000_000);
    let wal_enabled = env_bool("KOSHA_BENCH_WAL_ENABLED", true);
    let timeout_ms = env_usize("KOSHA_BENCH_TIMEOUT_MS", 10_000) as f64;
    let body_words = env_usize("KOSHA_BENCH_BODY_WORDS", 12);
    let backlog_docs = env_usize("KOSHA_BENCH_BACKLOG_DOCS", 4000);

    let root: PathBuf = std::env::temp_dir().join(format!(
        "kosha-bench-sustained-flush-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);

    let idx = Arc::new(
        Indexer::new(root.clone())
            .with_wal(wal_enabled)
            .with_flush_threshold(flush_threshold)
            .with_max_concurrent_flush_writers(max_writers)
            .with_min_docs_per_flush_writer(min_docs_per_writer),
    );
    let ns = NamespaceId("bench".into());

    eprintln!(
        "sustained concurrent flush: {threads} threads x {duration_secs}s, batch_size={batch_size}, \
         body_words={body_words}, max_concurrent_flush_writers={max_writers}, \
         min_docs_per_flush_writer={min_docs_per_writer}, wal_enabled={wal_enabled}"
    );

    // ── Phase B: single-round backlog latency ───────────────────────────
    //
    // The sustained closed-loop measurement below (Phase A) is inherently
    // self-throttling: each thread blocks on its own flush_namespace call
    // before submitting more work, so on fast local disk where one round
    // finishes in low-single-digit milliseconds, backlogs never grow large
    // enough for a difference in writer count to show up much in aggregate
    // throughput. That's *not* what actually broke in production: the
    // reported incident is closed-loop too (kosha_client.bulk() blocks on
    // its own /flush call), but production segment writes go to
    // network-backed storage where a single round's I/O latency alone can
    // approach or exceed the 10s client timeout -- at that point every
    // caller times out no matter how well coalesced the arrivals are,
    // purely because of how long ONE round takes. This phase isolates that
    // number directly: pre-load a large backlog (matching a namespace that
    // fell behind under sustained overload) on its own namespace, then time
    // exactly one `flush_namespace` call's wall-clock latency -- the number
    // that determines whether a caller waiting on it times out.
    let (backlog_round_ms, backlog_round_segments): (f64, usize) = {
        let backlog_ns = NamespaceId("backlog".into());
        let docs: Vec<Document> = (0..backlog_docs)
            .map(|i| mk_doc(0, i as u64, body_words))
            .collect();
        idx.index_documents(backlog_ns.clone(), docs).unwrap();
        let t = Instant::now();
        idx.flush_namespace(&backlog_ns).unwrap();
        let elapsed_ms = t.elapsed().as_secs_f64() * 1e3;
        let seg_count = idx
            .manifest(&backlog_ns)
            .map(|m| m.segments.len())
            .unwrap_or(0);
        println!();
        println!(
            "backlog round latency: {backlog_docs} docs buffered, one flush_namespace() call \
             -> {elapsed_ms:.1}ms ({seg_count} segment(s), max_concurrent_flush_writers={max_writers})"
        );
        (elapsed_ms, seg_count)
    };

    let start_barrier = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let total_docs = Arc::new(AtomicU64::new(0));
    let total_flushes = Arc::new(AtomicU64::new(0));
    let total_flush_errors = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..threads)
        .map(|tid| {
            let idx = idx.clone();
            let ns = ns.clone();
            let start_barrier = start_barrier.clone();
            let stop = stop.clone();
            let total_docs = total_docs.clone();
            let total_flushes = total_flushes.clone();
            let total_flush_errors = total_flush_errors.clone();
            std::thread::spawn(move || {
                let mut flush_samples_ms: Vec<f64> = Vec::new();
                let mut seq: u64 = 0;
                start_barrier.wait();
                while !stop.load(Ordering::Relaxed) {
                    let docs: Vec<Document> = (0..batch_size)
                        .map(|_| {
                            let d = mk_doc(tid, seq, body_words);
                            seq += 1;
                            d
                        })
                        .collect();
                    let n = docs.len() as u64;
                    if idx.index_documents(ns.clone(), docs).is_err() {
                        continue;
                    }
                    let t = Instant::now();
                    let result = idx.flush_namespace(&ns);
                    let elapsed_ms = t.elapsed().as_secs_f64() * 1e3;
                    flush_samples_ms.push(elapsed_ms);
                    total_flushes.fetch_add(1, Ordering::Relaxed);
                    match result {
                        Ok(()) => {
                            total_docs.fetch_add(n, Ordering::Relaxed);
                        }
                        Err(_) => {
                            total_flush_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                flush_samples_ms
            })
        })
        .collect();

    start_barrier.wait();
    let t_start = Instant::now();
    std::thread::sleep(Duration::from_secs(duration_secs as u64));
    stop.store(true, Ordering::Relaxed);

    let mut all_samples: Vec<f64> = Vec::new();
    for h in handles {
        all_samples.extend(h.join().unwrap());
    }
    let wall_elapsed = t_start.elapsed().as_secs_f64();

    // Drain whatever's still buffered so the final doc count reflects
    // everything actually indexed during the run, not just what each
    // worker's own flush call happened to observe as durable before the
    // deadline hit mid-flush.
    idx.flush_namespace(&ns).ok();

    let docs_indexed = total_docs.load(Ordering::Relaxed);
    let flushes = total_flushes.load(Ordering::Relaxed);
    let flush_errors = total_flush_errors.load(Ordering::Relaxed);
    let dist = Dist::from_samples(all_samples.clone());
    let over_timeout = all_samples.iter().filter(|&&ms| ms > timeout_ms).count();
    let throughput = docs_indexed as f64 / wall_elapsed;

    let manifest_docs: u32 = idx
        .manifest(&ns)
        .map(|m| m.segments.iter().map(|e| e.doc_count).sum())
        .unwrap_or(0);

    println!();
    println!(
        "config: threads={threads} duration={duration_secs}s batch_size={batch_size} \
         max_concurrent_flush_writers={max_writers} min_docs_per_flush_writer={min_docs_per_writer} \
         wal_enabled={wal_enabled}"
    );
    println!();
    println!("throughput:        {throughput:>10.1} docs/sec  ({docs_indexed} docs / {wall_elapsed:.2}s)");
    println!("flush_namespace calls: {flushes:>7}   errors: {flush_errors}   >{timeout_ms:.0}ms: {over_timeout}");
    println!("manifest doc count (post-drain): {manifest_docs}");
    println!();
    println!(
        "{:<20} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "flush_namespace", "n", "mean", "p50", "p90", "p99"
    );
    println!(
        "{:<20} {:>10} {:>9.2}ms {:>9.2}ms {:>9.2}ms {:>9.2}ms  max={:.2}ms",
        "latency", dist.n, dist.mean, dist.p50, dist.p90, dist.p99, dist.max
    );

    if let Ok(path) = std::env::var("KOSHA_BENCH_JSON") {
        let report = serde_json::json!({
            "schema": 1,
            "config": {
                "threads": threads,
                "duration_secs": duration_secs,
                "batch_size": batch_size,
                "body_words": body_words,
                "max_concurrent_flush_writers": max_writers,
                "min_docs_per_flush_writer": min_docs_per_writer,
                "wal_enabled": wal_enabled,
                "backlog_docs": backlog_docs,
            },
            "backlog_round_latency_ms": backlog_round_ms,
            "backlog_round_segments": backlog_round_segments,
            "docs_indexed": docs_indexed,
            "wall_elapsed_secs": wall_elapsed,
            "throughput_docs_per_sec": throughput,
            "flush_calls": flushes,
            "flush_errors": flush_errors,
            "flushes_over_timeout": over_timeout,
            "timeout_ms": timeout_ms,
            "manifest_doc_count": manifest_docs,
            "flush_latency_ms": dist.json(),
        });
        std::fs::write(&path, serde_json::to_string_pretty(&report).unwrap())
            .unwrap_or_else(|e| panic!("write KOSHA_BENCH_JSON={path}: {e}"));
        eprintln!("wrote JSON report to {path}");
    }

    let _ = std::fs::remove_dir_all(&root);
}
