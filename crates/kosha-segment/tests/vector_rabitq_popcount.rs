//! RaBitQ postings scored with `KOSHA_VECTOR_RABITQ_ESTIMATOR=popcount`
//! (#382): the 4-bit-query AND + popcount estimator must give the same
//! recall and exact rescored scores as the default lookup table does in
//! `vector_rabitq.rs`.
//!
//! The estimator is read once per process, so this is its own test
//! binary, and every test sets the variable before its first query: no
//! test here can observe the default.

use std::path::{Path, PathBuf};

use kosha_core::{Bm25Params, DocumentId, Field, SegmentId, VectorQuantization};
use kosha_segment::{SegmentReader, SegmentWriter, VectorQuantizationConfig};

fn use_popcount() {
    std::env::set_var("KOSHA_VECTOR_RABITQ_ESTIMATOR", "popcount");
}

fn temp_dir(name: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-vector-rabitq-popcount-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Deterministic pseudo-random vector in `[-1, 1)^dim` (as in `vector_rabitq.rs`).
fn vector(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    (0..dim)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
        })
        .collect()
}

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let dot: f64 = a.iter().zip(b).map(|(x, y)| *x as f64 * *y as f64).sum();
    let na: f64 = a.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    let nb: f64 = b.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    dot / (na * nb)
}

fn knn(reader: &SegmentReader, query: &[f32], k: usize) -> Vec<(u32, f64)> {
    reader
        .knn_search_filtered("emb", query, k, 10_000, None)
        .expect("column has a lazy index")
        .unwrap()
}

fn knn_within(reader: &SegmentReader, query: &[f32], k: usize) -> Vec<(u32, f64)> {
    reader
        .knn_search_within_filtered("emb", query, k, f32::MAX, usize::MAX, None)
        .expect("column has a lazy index")
        .unwrap()
        .0
}

fn recall(got: &[(u32, f64)], want: &[(u32, f64)]) -> f64 {
    let want: Vec<u32> = want.iter().map(|(id, _)| *id).collect();
    got.iter().filter(|(id, _)| want.contains(id)).count() as f64 / want.len() as f64
}

/// Clustered, embedding-like data (as in `vector_rabitq.rs`).
struct Clustered {
    dim: usize,
    noise: f32,
    centres: Vec<Vec<f32>>,
    data: Vec<Vec<f32>>,
}

impl Clustered {
    fn new(n: usize, dim: usize, clusters: u64, noise: f32) -> Self {
        let centres: Vec<Vec<f32>> = (0..clusters).map(|c| vector(9_000_000 + c, dim)).collect();
        let data = (0..n)
            .map(|i| {
                Self::around(
                    &centres[i % clusters as usize],
                    &vector(i as u64, dim),
                    noise,
                )
            })
            .collect();
        Self {
            dim,
            noise,
            centres,
            data,
        }
    }

    fn around(centre: &[f32], e: &[f32], noise: f32) -> Vec<f32> {
        centre.iter().zip(e).map(|(c, x)| c + noise * x).collect()
    }

    fn write(&self, dir: &Path) {
        let mut w = SegmentWriter::new(SegmentId("clustered".into()), dir.to_path_buf())
            .with_vector_quantization(
                VectorQuantizationConfig::default().with_field("emb", VectorQuantization::RaBitQ),
            );
        for (i, v) in self.data.iter().enumerate() {
            w.add_document(
                DocumentId(format!("doc-{i:05}")),
                vec![Field::vector("emb", v.clone())],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();
    }

    fn query(&self, q: u64) -> Vec<f32> {
        let centre = &self.centres[(q * 7) as usize % self.centres.len()];
        Self::around(centre, &vector(777_000 + q, self.dim), self.noise)
    }

    fn recall(&self, reader: &SegmentReader, queries: u64, check: impl Fn(f64, f64)) -> f64 {
        let mut total = 0.0;
        for q in 0..queries {
            let query = self.query(q);
            let mut want: Vec<(u32, f64)> = (0..self.data.len() as u32)
                .map(|id| (id, cosine(&query, &self.data[id as usize])))
                .collect();
            want.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            want.truncate(10);
            for probe in [knn, knn_within] {
                let got = probe(reader, &query, 10);
                assert_eq!(got.len(), 10);
                for (id, score) in &got {
                    check(*score, cosine(&query, &self.data[*id as usize]));
                }
                total += recall(&got, &want) / 2.0;
            }
        }
        total / queries as f64
    }
}

#[test]
fn popcount_rescoring_restores_exact_scores_and_recall() {
    use_popcount();
    let set = Clustered::new(3000, 256, 30, 0.6);
    let dir = temp_dir("recall");
    set.write(&dir);
    let rescored = SegmentReader::open(dir.clone()).unwrap();
    let recall_rescored = set.recall(&rescored, 10, |score, exact| {
        assert!((score - exact).abs() < 1e-5, "{score} vs {exact}");
    });
    assert!(
        recall_rescored >= 0.99,
        "rescored recall@10 {recall_rescored}"
    );
    let raw = SegmentReader::open(dir.clone())
        .unwrap()
        .with_vector_rescore_factor(0);
    set.recall(&raw, 10, |score, exact| {
        assert!((score - exact).abs() < 0.1, "estimate {score} vs {exact}");
    });
}

/// As `error_bounds_widen_a_short_rescoring_list` in `vector_rabitq.rs`:
/// the popcount estimator's bounds (RaBitQ's plus the query rounding)
/// still keep every true neighbour in a factor-2 shortlist.
#[test]
fn popcount_error_bounds_widen_a_short_rescoring_list() {
    use_popcount();
    let set = Clustered::new(3000, 256, 30, 0.6);
    let dir = temp_dir("bounds");
    set.write(&dir);
    let reader = SegmentReader::open(dir.clone())
        .unwrap()
        .with_vector_rescore_factor(2);
    let recall = set.recall(&reader, 10, |score, exact| {
        assert!((score - exact).abs() < 1e-5);
    });
    assert!(recall >= 0.99, "factor-2 recall@10 {recall}");
}
