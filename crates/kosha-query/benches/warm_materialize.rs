//! Warm page-materialize microbenchmark — measures the Option A fix
//! (persist `doc_store.bin` per page-segment, then local seeks) against the
//! per-doc ranged-GET behavior it replaces.
//!
//! Local Rust benches can't reproduce the *real* cost this changes (S3
//! round-trip latency — ~35 ms/GET in-region), so this harness simulates it
//! with a sleep-injecting hydrator and reports:
//!   - hydrator call count per warm query (the structural change: N per-doc
//!     → 1 per-segment on first warm, 0 on subsequent warm), and
//!   - end-to-end materialize wall-clock with that simulated S3 latency
//!     injected per hydrator call.
//!
//! Run:
//! ```text
//! cargo bench -p kosha-query --bench warm_materialize
//! # bigger corpus / slower simulated S3:
//! KOSHA_BENCH_SEGS=8 KOSHA_BENCH_DOCS=2000 KOSHA_BENCH_S3_MS=35 \
//!   cargo bench -p kosha-query --bench warm_materialize
//! ```
//!
//! What the numbers mean:
//!   - **Option A** (this PR): `ensure_doc_store([PathBuf])` — one call per
//!     distinct page-hit segment on the first warm query, zero thereafter
//!     (the file is on disk → `has_local_doc_store` short-circuits). A
//!     10-hit page over ≤10 segments costs ≤10 simulated GETs once, then 0.
//!   - **Before (per-doc spans)**: simulates the previous hydrator shape
//!     (`Fn(&[DocSpan]) -> ...`) — one call per page *document*, every warm
//!     query, because span bytes are never persisted. A 10-hit page costs 10
//!     simulated GETs on every warm query.
//!
//! The structural delta is the cargo of this PR: warm p50 ~350 ms → ~ms,
//! matching the observed 1M-doc / 8 QPS / topk=10 benchmark where warm p50
//! was dominated by per-doc ranged GETs against S3.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kosha_core::{
    Bm25Params, DocumentId, Field, Manifest, ManifestEntry, NamespaceId, SearchQuery, SegmentId,
};
use kosha_query::Searcher;
use kosha_segment::SegmentWriter;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ─── Deterministic synthetic corpus (same shape as segment_memory bench) ────

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn zipfish(&mut self, n: usize) -> usize {
        let u = self.next() as f64 / u64::MAX as f64;
        ((u * u) * n as f64) as usize
    }
}

fn vocab_word(rank: usize) -> String {
    match rank {
        0 => "the".into(),
        1 => "contract".into(),
        2 => "dispute".into(),
        _ => format!("w{rank}"),
    }
}

const WORDS_PER_DOC: usize = 120;

fn build_corpus(
    root: &std::path::Path,
    ns: &NamespaceId,
    segs: usize,
    docs: usize,
    vocab: usize,
) -> Manifest {
    let _ = std::fs::remove_dir_all(root);
    let mut entries = Vec::with_capacity(segs);
    for s in 0..segs {
        let seg_id = SegmentId(format!("s{s}"));
        let seg_dir = root.join(&ns.0).join(&seg_id.0);
        let mut w = SegmentWriter::new(seg_id.clone(), seg_dir);
        let mut rng = Lcg(0x5EED + s as u64);
        for d in 0..docs {
            let mut words = Vec::with_capacity(WORDS_PER_DOC);
            for _ in 0..WORDS_PER_DOC {
                words.push(vocab_word(rng.zipfish(vocab)));
            }
            w.add_document(
                DocumentId(format!("s{s}-d{d}")),
                vec![Field::text("t", words.join(" "))],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();
        entries.push(ManifestEntry {
            segment_id: seg_id,
            doc_count: docs as u32,
        });
    }
    Manifest {
        version: 1,
        segments: entries,
        segment_footers: Default::default(),
    }
}

fn mk_query(text: &str, topk: usize) -> SearchQuery {
    SearchQuery {
        query_text: text.into(),
        max_results: topk,
        from: 0,
        bm25_params: Bm25Params::default(),
        filter: None,
        sort: vec![],
        search_after: None,
        highlight: None,
        aggs: std::collections::HashMap::new(),
        wildcard: None,
        match_phrase: None,
        knn: None,
    }
}

/// One test row: how many hydrator calls a warm query made + its wall-clock.
struct Row {
    label: &'static str,
    calls: usize,
    wall_ms: f64,
}

fn main() {
    let segs = env_usize("KOSHA_BENCH_SEGS", 8);
    let docs = env_usize("KOSHA_BENCH_DOCS", 2000);
    let vocab = env_usize("KOSHA_BENCH_VOCAB", 20000);
    let topk = env_usize("KOSHA_BENCH_TOPK", 10);
    // Simulated in-region S3 GET latency per hydrator invocation. 35 ms is
    // typical; tune to match the environment you're projecting for.
    let s3_ms = env_usize("KOSHA_BENCH_S3_MS", 35);
    let s3_latency = Duration::from_millis(s3_ms as u64);

    let work = std::env::temp_dir().join("kosha-bench-warm-materialize");
    let ns = NamespaceId("bench".into());
    let manifest = build_corpus(&work, &ns, segs, docs, vocab);
    // Back up each segment's `doc_store.bin`, then remove it to model
    // scoring-set-only hydration: offsets sidecar present, doc_store.bin
    // absent (the cold state for page-materialize purposes). The bench's
    // hydrator restores the backup so warm #1 actually materializes and
    // warm #2+ is a true local read.
    for entry in &manifest.segments {
        let seg_dir = work.join(&ns.0).join(&entry.segment_id.0);
        let bin = seg_dir.join("doc_store.bin");
        std::fs::copy(&bin, seg_dir.join("doc_store.bin.bak")).unwrap();
        let _ = std::fs::remove_file(&bin);
    }

    let query = mk_query("the", topk);
    let warm_iters = 3;
    let samples = 5;

    println!(
        "\n  corpus: {segs} segs × {docs} docs (vocab={vocab}), topk={topk}, \
         simulated S3 latency = {s3_ms} ms/GET, warm iters = {warm_iters}, samples = {samples}\n"
    );

    // ── After (this PR: Option A — per-segment ensure, persisted) ─────────
    let mut after_rows: Vec<Row> = Vec::new();
    for _ in 0..samples {
        // Fresh cold state for each sample: drop doc_store.bin again so
        // the first warm query of the sample actually pays for hydration.
        for entry in &manifest.segments {
            let seg_dir = work.join(&ns.0).join(&entry.segment_id.0);
            let _ = std::fs::remove_file(seg_dir.join("doc_store.bin"));
        }
        let searcher = Searcher::new(work.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let s3 = s3_latency;
        let calls_clone = Arc::clone(&calls);
        let ensure = move |segs: &[PathBuf]| {
            for seg in segs {
                calls_clone.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(s3);
                // Stand-in for the real S3 fetch+persist: restore the backed-
                // up `doc_store.bin` so the segment's local seek+read
                // succeeds on this query and every subsequent warm query
                // (which is the whole point — `has_local_doc_store` then
                // short-circuits the hydrator on warm #2+).
                let _ = std::fs::copy(seg.join("doc_store.bin.bak"), seg.join("doc_store.bin"));
            }
        };
        for iter in 0..warm_iters {
            let calls_before = calls.load(Ordering::SeqCst);
            let t = Instant::now();
            let _ = searcher.search_with_doc_store_hydrator(
                &ns,
                &manifest,
                &query,
                None,
                Some(&ensure),
            );
            let wall_ms = t.elapsed().as_secs_f64() * 1e3;
            let delta_calls = calls.load(Ordering::SeqCst) - calls_before;
            after_rows.push(Row {
                label: if iter == 0 {
                    "warm #1 (post-cold)"
                } else {
                    "warm #2+"
                },
                calls: delta_calls,
                wall_ms,
            });
        }
    }

    // ── Before (per-doc ranged GET every warm query, never persisted) ─────
    // Simulated by a hydrator that sleeps once per *page document* — the
    // shape of the previous `Fn(&[DocSpan])` callback — and never persists
    // anything. Same N=simulated-GETs cost on every warm query.
    let mut before_rows: Vec<Row> = Vec::new();
    for _ in 0..samples {
        for entry in &manifest.segments {
            let bin = work
                .join(&ns.0)
                .join(&entry.segment_id.0)
                .join("doc_store.bin");
            let _ = std::fs::remove_file(&bin);
        }
        let searcher = Searcher::new(work.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let s3 = s3_latency;
        // We can't pass the old `Fn(&[DocSpan])` shape to the *new* API —
        // the type changed. Instead we simulate the previous behavior by
        // calling the hydrator topk times from the outside, once per page
        // document, for every warm query (since the old code never
        // persisted). The materialize path itself is identical; only the
        // hydrator's per-doc cost differs.
        for iter in 0..warm_iters {
            let calls_before = calls.load(Ordering::SeqCst);
            let t = Instant::now();
            // Force a no-op hydrator (we're accounting the per-doc sleeps
            // separately below to simulate the old behavior faithfully),
            // then add topk simulated GETs on top.
            let no_op = |_: &[PathBuf]| {};
            let _ =
                searcher.search_with_doc_store_hydrator(&ns, &manifest, &query, None, Some(&no_op));
            for _ in 0..topk {
                calls.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(s3);
            }
            let wall_ms = t.elapsed().as_secs_f64() * 1e3;
            let delta_calls = calls.load(Ordering::SeqCst) - calls_before;
            before_rows.push(Row {
                label: if iter == 0 {
                    "warm #1 (post-cold)"
                } else {
                    "warm #2+"
                },
                calls: delta_calls,
                wall_ms,
            });
        }
    }

    // ── Report ───────────────────────────────────────────────────────────
    fn report(label: &str, rows: &[Row]) {
        let by_phase: std::collections::HashMap<&str, Vec<&Row>> =
            rows.iter().fold(Default::default(), |mut acc, r| {
                acc.entry(r.label).or_default().push(r);
                acc
            });
        println!("  {label}");
        for phase in ["warm #1 (post-cold)", "warm #2+"] {
            if let Some(rs) = by_phase.get(phase) {
                let med_calls = {
                    let mut v: Vec<usize> = rs.iter().map(|r| r.calls).collect();
                    v.sort();
                    v[v.len() / 2]
                };
                let med_ms = {
                    let mut v: Vec<f64> = rs.iter().map(|r| r.wall_ms).collect();
                    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    v[v.len() / 2]
                };
                println!(
                    "    {:<22} calls/query = {:>3} | wall = {:>8.1} ms",
                    phase, med_calls, med_ms
                );
            }
        }
    }

    report(
        "Before (per-doc ranged GET, never persisted):",
        &before_rows,
    );
    println!();
    report(
        "After  (Option A: per-segment ensure, persisted):",
        &after_rows,
    );

    let _ = std::fs::remove_dir_all(&work);
}
