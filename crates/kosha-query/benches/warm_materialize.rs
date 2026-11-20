//! Warm page-materialize microbenchmark — the persisted page-hydration
//! paths against the per-doc ranged-GET behavior PR #84 replaced.
//!
//! Local Rust benches can't reproduce the *real* cost this changes (S3
//! round-trip latency — ~35 ms/GET in-region), so this harness simulates it
//! with a sleep-injecting hydrator and reports, per query:
//!   - simulated GETs (the structural number) and bytes fetched, and
//!   - end-to-end wall-clock with one simulated round trip per hydrator
//!     call (the server issues a call's GETs concurrently). The latency
//!     does not model transfer time, which is what Option A's whole-file
//!     bytes cost on a real network.
//!
//! Run:
//! ```text
//! cargo bench -p kosha-query --bench warm_materialize
//! # bigger corpus / slower simulated S3:
//! KOSHA_BENCH_SEGS=8 KOSHA_BENCH_DOCS=2000 KOSHA_BENCH_S3_MS=35 \
//!   cargo bench -p kosha-query --bench warm_materialize
//! ```
//!
//! What the rows mean:
//!   - **Before (per-doc spans, never persisted)**: one GET per page
//!     document on every query — the pre-#84 shape.
//!   - **Option A (whole file, persisted)**: one whole `doc_store.bin` GET
//!     per page segment on the cold query, zero after
//!     (`KOSHA_DOC_STORE_READ_MODE=whole`).
//!   - **Option B (ranged spans, persisted — #245 default)**: the page's
//!     document spans, coalesced per segment, fetched as ranged GETs and
//!     persisted into the sparse span cache; zero GETs after. Same round
//!     trips as Option A, a small fraction of the bytes.
//!
//! The regression guard for #84: every persisted mode must show `gets = 0`
//! on warm #2+.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kosha_core::{
    Bm25Params, DocumentId, Field, Manifest, ManifestEntry, NamespaceId, SearchQuery, SegmentId,
};
use kosha_query::{PageSegment, Searcher};
use kosha_segment::{span_cache, SegmentWriter};

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
        footer_crc32: Default::default(),
        wal_checkpoint: 0,
        wal_origin: None,
        tombstones: Default::default(),
        deletion_vectors: Default::default(),
        branch: None,
        schema: None,
        retired: Default::default(),
        shards: None,
    }
}

fn mk_query(text: &str, topk: usize) -> SearchQuery {
    SearchQuery {
        query_text: text.into(),
        max_results: topk,
        from: 0,
        bm25_params: Default::default(),
        filter: None,
        sort: vec![],
        search_after: None,
        highlight: None,
        aggs: std::collections::HashMap::new(),
        wildcard: None,
        fuzzy: None,
        match_phrase: None,
        knn: None,
        exact_total_hits: None,
        total_hits_cap: None,
        operator: None,
        no_cache: None,
        collapse: None,
        limit_per: None,
        rank_by: None,
        source_includes: None,
        source_excludes: None,
        rank: None,
        mode: None,
        fields: Vec::new(),
        rerank: None,
    }
}

/// One test row: simulated GETs and bytes a query made + its wall-clock.
struct Row {
    label: &'static str,
    calls: usize,
    bytes: usize,
    wall_ms: f64,
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Whole,
    Ranged,
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

    let run_persisted = |mode: Mode| -> Vec<Row> {
        let mut rows: Vec<Row> = Vec::new();
        for _ in 0..samples {
            // Fresh cold state for each sample: drop doc_store.bin and any
            // cached spans so the first query of the sample pays for them.
            for entry in &manifest.segments {
                let seg_dir = work.join(&ns.0).join(&entry.segment_id.0);
                let _ = std::fs::remove_file(seg_dir.join("doc_store.bin"));
                let _ = std::fs::remove_dir_all(span_cache::spans_dir(&seg_dir, "doc_store.bin"));
            }
            let searcher = Searcher::new(work.clone());
            let calls = Arc::new(AtomicUsize::new(0));
            let bytes = Arc::new(AtomicUsize::new(0));
            let (calls_in, bytes_in) = (Arc::clone(&calls), Arc::clone(&bytes));
            let ensure = move |segs: &[PageSegment<'_>]| {
                // The server issues one call's GETs concurrently: one round
                // trip of simulated latency per call, not per GET.
                std::thread::sleep(s3_latency);
                for seg in segs {
                    let dir = seg.segment_dir;
                    let full = std::fs::read(dir.join("doc_store.bin.bak")).unwrap();
                    match mode {
                        // Stand-in for the real S3 fetch+persist of the
                        // whole file.
                        Mode::Whole => {
                            calls_in.fetch_add(1, Ordering::SeqCst);
                            bytes_in.fetch_add(full.len(), Ordering::SeqCst);
                            std::fs::write(dir.join("doc_store.bin"), &full).unwrap();
                        }
                        // Coalesced ranged GETs persisted as spans.
                        Mode::Ranged => {
                            let spans: Vec<(u64, u64)> = seg
                                .doc_seqs
                                .iter()
                                .filter_map(|&d| seg.reader.doc_span(d))
                                .map(|(o, l)| (o, o + l as u64))
                                .collect();
                            let out = span_cache::spans_dir(dir, "doc_store.bin");
                            std::fs::create_dir_all(&out).unwrap();
                            for (s, e) in span_cache::coalesce(spans, 64 * 1024) {
                                calls_in.fetch_add(1, Ordering::SeqCst);
                                bytes_in.fetch_add((e - s) as usize, Ordering::SeqCst);
                                std::fs::write(
                                    out.join(span_cache::span_file_name(s, e)),
                                    &full[s as usize..e as usize],
                                )
                                .unwrap();
                            }
                        }
                    }
                }
            };
            for iter in 0..warm_iters {
                let calls_before = calls.load(Ordering::SeqCst);
                let bytes_before = bytes.load(Ordering::SeqCst);
                let t = Instant::now();
                let (result, _) = searcher
                    .search_with_doc_store_hydrator(&ns, &manifest, &query, None, Some(&ensure))
                    .expect("page must materialize");
                assert_eq!(result.results.len(), topk.min(result.total_hits));
                let wall_ms = t.elapsed().as_secs_f64() * 1e3;
                let delta_calls = calls.load(Ordering::SeqCst) - calls_before;
                if iter > 0 {
                    assert_eq!(delta_calls, 0, "warm repeat must make zero GETs (#84)");
                }
                rows.push(Row {
                    label: if iter == 0 {
                        "cold page"
                    } else {
                        "warm repeat"
                    },
                    calls: delta_calls,
                    bytes: bytes.load(Ordering::SeqCst) - bytes_before,
                    wall_ms,
                });
            }
        }
        rows
    };
    let whole_rows = run_persisted(Mode::Whole);
    let ranged_rows = run_persisted(Mode::Ranged);

    // ── Before (per-doc ranged GET every query, never persisted) ─────────
    // A hydrator that pays one simulated GET per page *document* and keeps
    // nothing, so every query is cold again. Materialization then fails
    // for want of bytes (ignored): only the fetch cost is being modelled.
    let mut before_rows: Vec<Row> = Vec::new();
    for _ in 0..samples {
        for entry in &manifest.segments {
            let seg_dir = work.join(&ns.0).join(&entry.segment_id.0);
            let _ = std::fs::remove_file(seg_dir.join("doc_store.bin"));
            let _ = std::fs::remove_dir_all(span_cache::spans_dir(&seg_dir, "doc_store.bin"));
        }
        let searcher = Searcher::new(work.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let bytes = Arc::new(AtomicUsize::new(0));
        let (calls_in, bytes_in) = (Arc::clone(&calls), Arc::clone(&bytes));
        let per_doc = move |segs: &[PageSegment<'_>]| {
            std::thread::sleep(s3_latency);
            for seg in segs {
                for &d in &seg.doc_seqs {
                    calls_in.fetch_add(1, Ordering::SeqCst);
                    let len = seg.reader.doc_span(d).map_or(0, |(_, l)| l as usize);
                    bytes_in.fetch_add(len, Ordering::SeqCst);
                }
            }
        };
        for iter in 0..warm_iters {
            let calls_before = calls.load(Ordering::SeqCst);
            let bytes_before = bytes.load(Ordering::SeqCst);
            let t = Instant::now();
            let _ = searcher.search_with_doc_store_hydrator(
                &ns,
                &manifest,
                &query,
                None,
                Some(&per_doc),
            );
            let wall_ms = t.elapsed().as_secs_f64() * 1e3;
            before_rows.push(Row {
                label: if iter == 0 {
                    "cold page"
                } else {
                    "warm repeat"
                },
                calls: calls.load(Ordering::SeqCst) - calls_before,
                bytes: bytes.load(Ordering::SeqCst) - bytes_before,
                wall_ms,
            });
        }
    }

    // ── Report ───────────────────────────────────────────────────────────
    fn report(label: &str, rows: &[Row]) {
        println!("  {label}");
        for phase in ["cold page", "warm repeat"] {
            let rs: Vec<&Row> = rows.iter().filter(|r| r.label == phase).collect();
            if rs.is_empty() {
                continue;
            }
            let median = |mut v: Vec<f64>| {
                v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                v[v.len() / 2]
            };
            println!(
                "    {:<12} gets/query = {:>3} | bytes/query = {:>10} | wall = {:>8.1} ms",
                phase,
                median(rs.iter().map(|r| r.calls as f64).collect()) as usize,
                median(rs.iter().map(|r| r.bytes as f64).collect()) as usize,
                median(rs.iter().map(|r| r.wall_ms).collect()),
            );
        }
    }

    report(
        "Before   (per-doc ranged GET, never persisted):",
        &before_rows,
    );
    println!();
    report("Option A (whole doc_store.bin, persisted):", &whole_rows);
    println!();
    report("Option B (coalesced spans, persisted; #245):", &ranged_rows);

    let _ = std::fs::remove_dir_all(&work);
}
