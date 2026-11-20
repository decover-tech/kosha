//! Filtered kNN latency and recall per selectivity bucket (#252).
//!
//! Builds a deterministic corpus of random vectors with keyword attributes
//! passing 50%, 5%, 0.5% and 0.05% of documents, then times warm
//! `Searcher::search` for each bucket under two policies:
//!
//!   * **probe** — exact filtered kNN off (`KOSHA_KNN_EXACT_THRESHOLD=0`):
//!     the filter-aware probe with its selectivity-widened budget;
//!   * **planned** — the default policy: exact over the passing documents
//!     at or below 50,000 namespace-wide (or 1% of a segment), else probe.
//!
//! Recall@k is measured against brute force over the same vectors. The
//! `100%` row is the unfiltered baseline: the probe's own recall at this
//! `num_candidates`, which a filtered probe cannot be expected to beat.
//!
//! Vectors are drawn around `KOSHA_BENCH_CLUSTERS` random centres (default
//! 256; `0` = uniform random, the worst case for any cluster index).
//! `KOSHA_BENCH_NUM_CANDIDATES` (default 100) sets the probe budget.
//!
//! ```text
//! cargo bench -p kosha-query --bench filtered_knn
//! KOSHA_BENCH_CLUSTERS=0 cargo bench -p kosha-query --bench filtered_knn
//! KOSHA_BENCH_SEGS=16 KOSHA_BENCH_DOCS=15000 KOSHA_BENCH_DIM=384 \
//!   cargo bench -p kosha-query --bench filtered_knn
//! ```

use std::collections::HashMap;
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

use kosha_core::{
    Bm25Params, DocumentId, Field, FilterClause, KnnQuery, Manifest, ManifestEntry, NamespaceId,
    SearchQuery, SegmentId,
};
use kosha_query::{Searcher, DEFAULT_KNN_EXACT_FRACTION, DEFAULT_KNN_EXACT_THRESHOLD};
use kosha_segment::SegmentWriter;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn coord(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 23) as f32 * 2.0 - 1.0
    }
    fn vector(&mut self, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| self.coord()).collect()
    }
    /// A point near one of `centres` (or uniform when there are none).
    fn clustered(&mut self, centres: &[Vec<f32>], dim: usize) -> Vec<f32> {
        if centres.is_empty() {
            return self.vector(dim);
        }
        let centre = &centres[(self.next() >> 33) as usize % centres.len()];
        centre.iter().map(|c| c + 0.35 * self.coord()).collect()
    }
}

/// `(label, field, period)`: every `period`-th document passes. The
/// empty field is the unfiltered baseline.
const BUCKETS: [(&str, &str, usize); 5] = [
    ("100%", "", 1),
    ("50%", "s50", 2),
    ("5%", "s5", 20),
    ("0.5%", "s05", 200),
    ("0.05%", "s005", 2000),
];

fn build_corpus(
    root: &Path,
    ns: &NamespaceId,
    segs: usize,
    docs: usize,
    dim: usize,
    clusters: usize,
) -> (Manifest, Vec<Vec<f32>>) {
    let _ = std::fs::remove_dir_all(root);
    let mut rng = Lcg(0x5EED_0252);
    let centres: Vec<Vec<f32>> = (0..clusters).map(|_| rng.vector(dim)).collect();
    let mut entries = Vec::with_capacity(segs);
    let mut vectors = Vec::with_capacity(segs * docs);
    for s in 0..segs {
        let seg_id = SegmentId(format!("s{s}"));
        let mut w = SegmentWriter::new(seg_id.clone(), root.join(&ns.0).join(&seg_id.0));
        for d in 0..docs {
            let i = s * docs + d;
            let vector = rng.clustered(&centres, dim);
            let mut fields = vec![Field::vector("embedding", vector.clone())];
            for (_, field, period) in BUCKETS.into_iter().filter(|b| !b.1.is_empty()) {
                fields.push(Field::keyword(
                    field,
                    if i.is_multiple_of(period) { "y" } else { "n" },
                ));
            }
            w.add_document(DocumentId(format!("d{i}")), fields);
            vectors.push(vector);
        }
        w.finalize(Bm25Params::default()).unwrap();
        entries.push(ManifestEntry {
            segment_id: seg_id,
            doc_count: docs as u32,
        });
    }
    let manifest = Manifest {
        version: 1,
        segments: entries,
        segment_footers: Default::default(),
        footer_crc32: Default::default(),
        wal_checkpoint: 0,
        wal_origin: None,
        tombstones: Default::default(),
        deletion_vectors: Default::default(),
        schema: None,
        retired: Default::default(),
        shards: None,
        branch: None,
    };
    (manifest, vectors)
}

fn query(vector: Vec<f32>, k: usize, num_candidates: usize, field: &str) -> SearchQuery {
    let mut q: SearchQuery = serde_json::from_value(serde_json::json!({
        "query_text": "",
        "max_results": k,
    }))
    .unwrap();
    q.knn = Some(KnnQuery {
        field: "embedding".into(),
        vector,
        k,
        num_candidates,
        filter: (!field.is_empty()).then(|| FilterClause::Term {
            term: HashMap::from([(field.to_string(), "y".to_string())]),
        }),
        scoring: None,
        vectors: None,
    });
    q
}

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    (dot / (na * nb)) as f64
}

fn brute_force(vectors: &[Vec<f32>], q: &[f32], k: usize, period: usize) -> Vec<String> {
    let mut scored: Vec<(usize, f64)> = vectors
        .iter()
        .enumerate()
        .filter(|(i, _)| i.is_multiple_of(period))
        .map(|(i, v)| (i, cosine(q, v)))
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    scored
        .into_iter()
        .take(k)
        .map(|(i, _)| format!("d{i}"))
        .collect()
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn main() {
    let segs = env_usize("KOSHA_BENCH_SEGS", 8);
    let docs = env_usize("KOSHA_BENCH_DOCS", 15_000);
    let dim = env_usize("KOSHA_BENCH_DIM", 128);
    let queries = env_usize("KOSHA_BENCH_QUERIES", 50);
    let clusters = env_usize("KOSHA_BENCH_CLUSTERS", 256);
    let num_candidates = env_usize("KOSHA_BENCH_NUM_CANDIDATES", 100);
    let k = 10;
    let root = std::env::temp_dir().join("kosha-bench-filtered-knn");
    let ns = NamespaceId("bench".into());
    let t = Instant::now();
    let (manifest, vectors) = build_corpus(&root, &ns, segs, docs, dim, clusters);
    eprintln!(
        "corpus: {segs} segments x {docs} docs, {dim} dims, {clusters} clusters ({} docs) \
         built in {:.1}s; num_candidates {num_candidates}",
        segs * docs,
        t.elapsed().as_secs_f64()
    );
    let mut rng = Lcg(0xC0FFEE);
    // Queries are drawn like the corpus (not copies of corpus vectors).
    let centres: Vec<Vec<f32>> = {
        let mut seed = Lcg(0x5EED_0252);
        (0..clusters).map(|_| seed.vector(dim)).collect()
    };
    let query_vectors: Vec<Vec<f32>> = (0..queries).map(|_| rng.clustered(&centres, dim)).collect();

    println!(
        "| bucket | passing | policy | knn_mode | p50 ms | p95 ms | recall@{k} | postings | exact vectors |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for (label, field, period) in BUCKETS {
        let passing = vectors.len().div_ceil(period);
        let truths: Vec<Vec<String>> = query_vectors
            .iter()
            .map(|q| brute_force(&vectors, q, k, period))
            .collect();
        let policies: &[(&str, usize)] = if field.is_empty() {
            &[("unfiltered", DEFAULT_KNN_EXACT_THRESHOLD)]
        } else {
            &[("probe", 0), ("planned", DEFAULT_KNN_EXACT_THRESHOLD)]
        };
        for &(policy, threshold) in policies {
            let searcher = Searcher::new(root.clone())
                .with_knn_exact_policy(threshold, DEFAULT_KNN_EXACT_FRACTION);
            // Warm: segments open, filter sets cached, postings admitted.
            for q in &query_vectors {
                for _ in 0..2 {
                    black_box(
                        searcher
                            .search(
                                &ns,
                                &manifest,
                                &query(q.clone(), k, num_candidates, field),
                                None,
                            )
                            .unwrap(),
                    );
                }
            }
            let mut samples = Vec::with_capacity(queries);
            let mut recall = 0.0;
            let mut postings = 0usize;
            let mut exact_vectors = 0usize;
            let mut mode = String::new();
            for (q, truth) in query_vectors.iter().zip(&truths) {
                let sq = query(q.clone(), k, num_candidates, field);
                let t = Instant::now();
                let (r, stats) = searcher
                    .search_with_stats(&ns, &manifest, &sq, None)
                    .unwrap();
                samples.push(t.elapsed().as_secs_f64() * 1e3);
                assert_eq!(r.results.len(), k.min(passing));
                let hits = r
                    .results
                    .iter()
                    .filter(|d| truth.contains(&d.doc_id.0))
                    .count();
                recall += hits as f64 / truth.len() as f64;
                postings += stats.knn_postings_probed;
                exact_vectors += stats.knn_exact_vectors;
                mode = r.knn_mode.unwrap_or_default();
            }
            samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let n = query_vectors.len();
            println!(
                "| {label} | {passing} | {policy} | {mode} | {:.2} | {:.2} | {:.3} | {} | {} |",
                pct(&samples, 50.0),
                pct(&samples, 95.0),
                recall / n as f64,
                postings / n,
                exact_vectors / n,
            );
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}
