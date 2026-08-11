//! Fast local iteration harness for kNN/vector-path optimizations.
//!
//! Same shape as `segment_memory` (plain binary, deterministic corpus, one
//! p50 table), but for the vector path. Isolates the metrics the kNN
//! optimization backlog targets, so each change moves a named number:
//!
//!   * **open all segments (vectors)** — `SegmentReader::open_with_options
//!     (…, true)` across the manifest: vector-index load (v2: centroid
//!     sidecar parse; legacy: full `vector.idx` read). The cold-open cost.
//!   * **resident while open** — exact allocated bytes (counting global
//!     allocator) while all vector-bearing segments are held open; the
//!     currency quantization would move.
//!   * **cold / warm kNN end-to-end** and **warm hybrid** through
//!     `Searcher::search` — what distance-function and fan-out work moves.
//!
//! Run it locally (seconds, no cluster):
//! ```text
//! cargo bench -p kosha-query --bench knn_open_search
//! # bigger corpus / real-embedding dimensionality:
//! KOSHA_BENCH_SEGS=16 KOSHA_BENCH_DOCS=4000 KOSHA_BENCH_DIM=384 \
//!   cargo bench -p kosha-query --bench knn_open_search
//! # CI-grade percentiles:
//! KOSHA_BENCH_COLD_ITERS=25 KOSHA_BENCH_WARM_ITERS=200 \
//!   KOSHA_BENCH_JSON=/tmp/knn_bench.json cargo bench -p kosha-query --bench knn_open_search
//! ```
//!
//! The corpus is deterministic (fixed-seed LCG for text and vectors), so
//! numbers are comparable run to run on the same machine.

use std::alloc::System;
use std::collections::HashMap;
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

use cap::Cap;
use kosha_core::{
    Bm25Params, DocumentId, Field, KnnQuery, Manifest, ManifestEntry, NamespaceId, SearchQuery,
    SegmentId,
};
use kosha_query::Searcher;
use kosha_segment::{SegmentReader, SegmentWriter};

#[global_allocator]
static ALLOC: Cap<System> = Cap::new(System, usize::MAX);

fn live_bytes() -> usize {
    ALLOC.allocated()
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ─── Deterministic synthetic corpus ─────────────────────────────────────────

/// Fixed-seed LCG so corpus content (and therefore numbers) are stable
/// across runs without pulling in a rand dependency.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }

    /// Uniform f32 in [-1, 1) — embedding-ish coordinates. Realistic
    /// direction diversity is what matters for HNSW build/search cost, not
    /// the marginal distribution.
    fn coord(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 23) as f32 * 2.0 - 1.0
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
        _ => format!("w{rank}"),
    }
}

/// Short prose (kNN benches shouldn't be dominated by postings volume) plus
/// one embedding per doc.
const WORDS_PER_DOC: usize = 24;

/// Deterministic cluster centers shared by every segment — real embedding
/// corpora are strongly clustered (topics), and cluster structure is what
/// makes centroid bounds tight. `clusters == 0` degenerates to the old
/// uniform-random corpus (worst case for any pruning).
fn cluster_center(cluster: usize, dim: usize) -> Vec<f32> {
    let mut rng = Lcg(0xC1_0000 + cluster as u64);
    (0..dim).map(|_| rng.coord()).collect()
}

fn clustered_vector(rng: &mut Lcg, clusters: usize, dim: usize) -> Vec<f32> {
    if clusters == 0 {
        return (0..dim).map(|_| rng.coord()).collect();
    }
    let c = (rng.next() % clusters as u64) as usize;
    cluster_center(c, dim)
        .into_iter()
        .map(|x| x + rng.coord() * 0.2)
        .collect()
}

fn build_corpus(
    root: &Path,
    ns: &NamespaceId,
    segs: usize,
    docs: usize,
    dim: usize,
    clusters: usize,
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
                words.push(vocab_word(rng.zipfish(2000)));
            }
            let vector = clustered_vector(&mut rng, clusters, dim);
            w.add_document(
                DocumentId(format!("s{s}-d{d}")),
                vec![
                    Field::text("t", words.join(" ")),
                    Field::vector("embedding", vector),
                ],
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

// ─── Measurement ────────────────────────────────────────────────────────────

fn mk_query(text: &str, knn: Option<KnnQuery>) -> SearchQuery {
    SearchQuery {
        query_text: text.into(),
        max_results: 10,
        from: 0,
        bm25_params: Bm25Params::default(),
        filter: None,
        sort: vec![],
        search_after: None,
        highlight: None,
        aggs: HashMap::new(),
        wildcard: None,
        match_phrase: None,
        knn,
        exact_total_hits: None,
        total_hits_cap: None,
        operator: None,
        no_cache: None,
    }
}

fn mk_knn(dim: usize, k: usize, clusters: usize) -> KnnQuery {
    // Deterministic query vector from its own seed — not a corpus vector,
    // so the search can't degenerate into an exact-match shortcut. In
    // clustered mode it sits near (not on) a cluster center, like a real
    // query embedding landing in a topic's neighborhood.
    let mut rng = Lcg(0xC0FFEE);
    let vector = if clusters == 0 {
        (0..dim).map(|_| rng.coord()).collect()
    } else {
        cluster_center(1 % clusters, dim)
            .into_iter()
            .map(|x| x + rng.coord() * 0.1)
            .collect()
    };
    KnnQuery {
        field: "embedding".into(),
        vector,
        k,
        num_candidates: 10 * k,
        filter: None,
    }
}

/// Latency distribution summary over one metric's samples (nearest-rank
/// percentiles). With small sample counts p90/p99 degrade toward the max —
/// `n` is carried so downstream consumers can tell.
#[derive(Clone, Copy)]
struct Dist {
    n: usize,
    p50: f64,
    p90: f64,
    p99: f64,
}

impl Dist {
    fn from_samples(mut samples: Vec<f64>) -> Dist {
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pct = |p: f64| {
            let rank = ((p / 100.0) * samples.len() as f64).ceil() as usize;
            samples[rank.saturating_sub(1).min(samples.len() - 1)]
        };
        Dist {
            n: samples.len(),
            p50: pct(50.0),
            p90: pct(90.0),
            p99: pct(99.0),
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({ "n": self.n, "p50": self.p50, "p90": self.p90, "p99": self.p99 })
    }
}

fn dir_file_bytes(root: &Path, ns: &NamespaceId, manifest: &Manifest, file: &str) -> u64 {
    manifest
        .segments
        .iter()
        .map(|e| {
            std::fs::metadata(root.join(&ns.0).join(&e.segment_id.0).join(file))
                .map(|m| m.len())
                .unwrap_or(0)
        })
        .sum()
}

fn mb(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() {
    let segs = env_usize("KOSHA_BENCH_SEGS", 8);
    let docs = env_usize("KOSHA_BENCH_DOCS", 2000);
    let dim = env_usize("KOSHA_BENCH_DIM", 128);
    let k = env_usize("KOSHA_BENCH_K", 10);
    let clusters = env_usize("KOSHA_BENCH_CLUSTERS", 64);

    let root = std::env::temp_dir().join("kosha-bench-knn-open-search");
    let ns = NamespaceId("bench".into());

    eprintln!("building corpus: {segs} segs × {docs} docs, dim {dim} …");
    let t_build = Instant::now();
    let manifest = build_corpus(&root, &ns, segs, docs, dim, clusters);
    let build_ms = t_build.elapsed().as_secs_f64() * 1e3;

    // Cold open (vectors + HNSW build-or-load) + exact resident bytes while
    // every segment is held open. Fresh readers each sample.
    let cold_iters = env_usize("KOSHA_BENCH_COLD_ITERS", 3);
    let mut open_samples = Vec::new();
    let mut open_bytes = 0usize;
    for _ in 0..cold_iters {
        let before = live_bytes();
        let t = Instant::now();
        let readers: Vec<SegmentReader> = manifest
            .segments
            .iter()
            .map(|e| {
                SegmentReader::open_with_options(root.join(&ns.0).join(&e.segment_id.0), true)
                    .unwrap()
            })
            .collect();
        open_samples.push(t.elapsed().as_secs_f64() * 1e3);
        open_bytes = live_bytes().saturating_sub(before);
        for r in &readers {
            // v2 segments carry the lazy posting index; legacy/flat ones
            // carry a populated vector_store (served by exact flat_knn).
            assert!(
                r.has_lazy_vector_index()
                    || r.hnsw_map.is_some()
                    || !r.vector_store.vectors.is_empty(),
                "vector segment opened without any vector index"
            );
        }
        black_box(&readers);
        drop(readers);
    }
    let open = Dist::from_samples(open_samples);

    // Cold end-to-end kNN: fresh Searcher (empty cache) per sample, every
    // segment opened (with vectors) inside the search itself.
    let mut cold_samples = Vec::new();
    for _ in 0..cold_iters {
        let searcher = Searcher::new(root.to_path_buf());
        let q = mk_query("", Some(mk_knn(dim, k, clusters)));
        let t = Instant::now();
        let r = searcher.search(&ns, &manifest, &q, None).unwrap();
        cold_samples.push(t.elapsed().as_secs_f64() * 1e3);
        // A zero-hit kNN query means the vector read path is broken, and a
        // broken read path benches *faster* — fail loudly instead (PR #83).
        assert!(r.total_hits > 0, "cold kNN query returned 0 hits");
        black_box(r.total_hits);
    }
    let cold_knn = Dist::from_samples(cold_samples);

    // Warm queries: one shared Searcher, cache primed by a warmup pass.
    let searcher = Searcher::new(root.to_path_buf());
    let warm_iters = env_usize("KOSHA_BENCH_WARM_ITERS", 20);
    let shapes: Vec<(&'static str, &'static str, SearchQuery)> = vec![
        (
            "warm kNN",
            "knn",
            mk_query("", Some(mk_knn(dim, k, clusters))),
        ),
        (
            "warm hybrid (\"the\" + kNN)",
            "hybrid_broad",
            mk_query("the", Some(mk_knn(dim, k, clusters))),
        ),
    ];
    let mut warm = Vec::new();
    for (label, key, query) in shapes {
        searcher.search(&ns, &manifest, &query, None).unwrap(); // warmup
        let mut samples = Vec::new();
        for _ in 0..warm_iters {
            let t = Instant::now();
            let r = searcher.search(&ns, &manifest, &query, None).unwrap();
            samples.push(t.elapsed().as_secs_f64() * 1e3);
            assert!(r.total_hits > 0, "{label} returned 0 hits");
            black_box(r.total_hits);
        }
        warm.push((label, key, Dist::from_samples(samples)));
    }

    let idx_bytes = dir_file_bytes(&root, &ns, &manifest, "vector.idx");

    println!();
    println!(
        "corpus: {segs} segments × {docs} docs ({} total), dim {dim}, k {k}, clusters {clusters}",
        segs * docs
    );
    println!(
        "write (all segments, incl. ANN build): {build_ms:.0} ms | vector.idx {:.1} MiB",
        idx_bytes as f64 / (1024.0 * 1024.0),
    );
    println!();
    println!("{:<28} {:>12} {:>12} {:>12}", "metric", "p50", "p90", "p99");
    let row = |label: &str, d: &Dist| {
        println!(
            "{:<28} {:>10.2}ms {:>10.2}ms {:>10.2}ms",
            label, d.p50, d.p90, d.p99
        );
    };
    row("open all segments (vectors)", &open);
    println!("{:<28} {:>11.1}MiB", "resident while open", mb(open_bytes));
    row("cold kNN end-to-end", &cold_knn);
    for (label, _, d) in &warm {
        row(label, d);
    }

    if let Ok(path) = std::env::var("KOSHA_BENCH_JSON") {
        let warm_json: serde_json::Map<String, serde_json::Value> = warm
            .iter()
            .map(|(_, key, d)| (key.to_string(), d.json()))
            .collect();
        let report = serde_json::json!({
            "schema": 1,
            "corpus": { "segs": segs, "docs": docs, "dim": dim, "k": k, "clusters": clusters },
            "write_ms": build_ms,
            "vector_idx_bytes": idx_bytes,
            "open_ms": open.json(),
            "open_bytes": open_bytes,
            "cold_knn_ms": cold_knn.json(),
            "warm_ms": warm_json,
        });
        std::fs::write(&path, serde_json::to_string_pretty(&report).unwrap())
            .unwrap_or_else(|e| panic!("write KOSHA_BENCH_JSON={path}: {e}"));
        eprintln!("wrote JSON report to {path}");
    }
}
