//! Cost accounting for late-interaction (ColBERT-style) kNN (#205):
//! storage per document, write time, query latency through
//! `Searcher::search`, and recall@k against brute-force MaxSim — next to a
//! single-vector kNN query over the same documents.
//!
//! ```text
//! cargo bench -p kosha-query --bench late_interaction
//! # ColBERTv2-like shape (128-dim tokens, ~64 tokens per passage):
//! KOSHA_BENCH_DIM=128 KOSHA_BENCH_TOKENS=64 KOSHA_BENCH_DOCS=2000 \
//!   cargo bench -p kosha-query --bench late_interaction
//! ```
//!
//! The corpus is synthetic and clustered (tokens drawn around topic
//! centres, documents mixing a few topics) and deterministic, so numbers
//! are comparable run to run. Its recall says how well candidate
//! generation finds the true MaxSim top-k; it says nothing about relevance
//! on real text, which needs real ColBERT embeddings.

use std::path::Path;
use std::time::{Duration, Instant};

use kosha_core::{
    Bm25Params, DocumentId, Field, FieldType, Manifest, ManifestEntry, NamespaceId, SearchQuery,
    SegmentId,
};
use kosha_query::Searcher;
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
    fn below(&mut self, n: usize) -> usize {
        (self.next() >> 33) as usize % n
    }
}

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let dot: f64 = a
        .iter()
        .zip(b)
        .map(|(x, y)| (*x as f64) * (*y as f64))
        .sum();
    let na: f64 = a.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    let nb: f64 = b.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}

fn maxsim(query: &[Vec<f32>], doc: &[Vec<f32>]) -> f64 {
    query
        .iter()
        .map(|q| doc.iter().map(|d| cosine(q, d)).fold(f64::MIN, f64::max))
        .sum()
}

fn mean(bag: &[Vec<f32>]) -> Vec<f32> {
    let mut m = vec![0.0; bag[0].len()];
    for t in bag {
        for (a, b) in m.iter_mut().zip(t) {
            *a += b / bag.len() as f32;
        }
    }
    m
}

fn p50(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

fn dir_bytes(dir: &Path, pred: impl Fn(&str) -> bool) -> u64 {
    let mut total = 0;
    for seg in std::fs::read_dir(dir).unwrap() {
        for f in std::fs::read_dir(seg.unwrap().path()).unwrap() {
            let f = f.unwrap();
            if pred(&f.file_name().to_string_lossy()) {
                total += f.metadata().unwrap().len();
            }
        }
    }
    total
}

fn main() {
    let segs = env_usize("KOSHA_BENCH_SEGS", 4);
    let docs = env_usize("KOSHA_BENCH_DOCS", 1000);
    let dim = env_usize("KOSHA_BENCH_DIM", 64);
    let tokens = env_usize("KOSHA_BENCH_TOKENS", 32);
    let q_tokens = env_usize("KOSHA_BENCH_QUERY_TOKENS", 16);
    let queries = env_usize("KOSHA_BENCH_QUERIES", 20);
    let k = 10;
    let topics = (segs * docs / 20).max(8);

    let root = std::env::temp_dir().join("kosha-bench-late-interaction");
    let _ = std::fs::remove_dir_all(&root);
    let ns = NamespaceId("li".into());
    let mut rng = Lcg(0x5EED);
    let centres: Vec<Vec<f32>> = (0..topics)
        .map(|_| (0..dim).map(|_| rng.coord()).collect())
        .collect();
    let token_near = |rng: &mut Lcg, c: &[f32]| -> Vec<f32> {
        c.iter()
            .map(|x| x + 0.6 * rng.coord())
            .collect::<Vec<f32>>()
    };
    println!(
        "corpus: {segs} segments x {docs} docs, {tokens} tokens/doc, dim {dim}, \
         {topics} topics; queries: {queries} x {q_tokens} tokens, k {k}"
    );

    let mut bags: Vec<Vec<Vec<f32>>> = Vec::with_capacity(segs * docs);
    let mut entries = Vec::new();
    let mut json_bytes = 0u64;
    let t_write = Instant::now();
    for s in 0..segs {
        let seg_id = SegmentId(format!("s{s}"));
        let mut w = SegmentWriter::new(seg_id.clone(), root.join(&ns.0).join(&seg_id.0));
        for d in 0..docs {
            let picks: Vec<usize> = (0..3).map(|_| rng.below(topics)).collect();
            let bag: Vec<Vec<f32>> = (0..tokens)
                .map(|t| token_near(&mut rng, &centres[picks[t % 3]]))
                .collect();
            let value = serde_json::to_string(&bag).unwrap();
            json_bytes += value.len() as u64;
            w.add_document(
                DocumentId(format!("s{s}-d{d}")),
                vec![
                    Field::vector("pooled", mean(&bag)),
                    Field {
                        name: "tokens".into(),
                        field_type: FieldType::Vector,
                        value,
                    },
                ],
            );
            bags.push(bag);
        }
        w.finalize(Bm25Params::default()).unwrap();
        entries.push(ManifestEntry {
            segment_id: seg_id,
            doc_count: docs as u32,
        });
    }
    let write = t_write.elapsed();
    let manifest: Manifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "segments": entries,
    }))
    .unwrap();

    let n = (segs * docs) as f64;
    let ns_dir = root.join(&ns.0);
    let bags_b = dir_bytes(&ns_dir, |f| f.ends_with(".bags")) as f64 / n;
    let ivf_b = dir_bytes(&ns_dir, |f| f.ends_with(".ivf")) as f64 / n;
    let single_b = dir_bytes(&ns_dir, |f| f.starts_with("vector.")) as f64 / n;
    let f32_b = (tokens * dim * 4) as f64;
    println!(
        "write (all segments, both indexes): {} ms",
        write.as_millis()
    );
    println!(
        "storage per doc: raw f32 tokens {f32_b:.0} B | .bags {bags_b:.0} B ({:.2}x) | \
         .ivf {ivf_b:.0} B ({:.2}x) | source JSON in doc store {:.0} B ({:.2}x) | \
         single-vector index {single_b:.0} B",
        bags_b / f32_b,
        ivf_b / f32_b,
        json_bytes as f64 / n,
        json_bytes as f64 / n / f32_b,
    );

    let searcher = Searcher::new(root.clone());
    let all_ids: Vec<String> = (0..segs)
        .flat_map(|s| (0..docs).map(move |d| format!("s{s}-d{d}")))
        .collect();
    let mut li_lat = Vec::new();
    let mut sv_lat = Vec::new();
    let (mut li_found, mut sv_found, mut probed) = (0usize, 0usize, 0usize);
    for qi in 0..queries {
        let picks: Vec<usize> = (0..2).map(|_| rng.below(topics)).collect();
        let query: Vec<Vec<f32>> = (0..q_tokens)
            .map(|t| token_near(&mut rng, &centres[picks[t % 2]]))
            .collect();
        let mut truth: Vec<(usize, f64)> = bags
            .iter()
            .enumerate()
            .map(|(i, b)| (i, maxsim(&query, b)))
            .collect();
        truth.sort_by(|a, b| b.1.total_cmp(&a.1));
        let truth: Vec<&str> = truth[..k]
            .iter()
            .map(|(i, _)| all_ids[*i].as_str())
            .collect();

        let li: SearchQuery = serde_json::from_value(serde_json::json!({
            "query_text": "", "max_results": k, "no_cache": true,
            "knn": {"field": "tokens", "vectors": query, "k": k}
        }))
        .unwrap();
        let sv: SearchQuery = serde_json::from_value(serde_json::json!({
            "query_text": "", "max_results": k, "no_cache": true,
            "knn": {"field": "pooled", "vector": mean(&query), "k": k}
        }))
        .unwrap();
        for (q, lat, found) in [
            (&li, &mut li_lat, &mut li_found),
            (&sv, &mut sv_lat, &mut sv_found),
        ] {
            let t = Instant::now();
            let (r, stats) = searcher.search_with_stats(&ns, &manifest, q, None).unwrap();
            if qi > 0 {
                lat.push(t.elapsed());
            }
            if std::ptr::eq(q, &li) {
                probed += stats.knn_postings_probed;
            }
            *found += r
                .results
                .iter()
                .filter(|d| truth.contains(&d.doc_id.0.as_str()))
                .count();
        }
    }
    let total = (queries * k) as f64;
    println!(
        "late interaction: warm p50 {:.2} ms, recall@{k} vs brute-force MaxSim {:.3}, \
         {:.0} postings probed/query",
        p50(li_lat).as_secs_f64() * 1e3,
        li_found as f64 / total,
        probed as f64 / queries as f64,
    );
    println!(
        "single vector (mean-pooled tokens): warm p50 {:.2} ms, recall@{k} of the MaxSim top-{k} {:.3}",
        p50(sv_lat).as_secs_f64() * 1e3,
        sv_found as f64 / total,
    );
    let _ = std::fs::remove_dir_all(&root);
}
