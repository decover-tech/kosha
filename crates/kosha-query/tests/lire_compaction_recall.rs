//! End-to-end recall of LIRE compaction (#253): write a namespace through
//! the real `Indexer` (one big segment, then flush-sized segments of
//! updated documents), compact it with the merged vector index built
//! either from scratch (`rebuild`) or seeded from the inputs' postings and
//! repaired by LIRE (`lire`), and measure kNN recall@10 through the real
//! `Searcher` against brute force over the live documents.

use std::collections::{HashMap, HashSet};

use kosha_core::{Document, DocumentId, Field, KnnQuery, NamespaceId, SearchQuery};
use kosha_query::Searcher;
use kosha_segment::VectorCompactionMode;
use kosha_write::{CompactOptions, Indexer};

const DIM: usize = 32;
const BIG: u64 = 6_000;
const FLUSHES: u64 = 4;
const UPDATES_PER_FLUSH: u64 = 75;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> f32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 40) as f32 / (1u64 << 24) as f32
    }
    fn range(&mut self, lo: f32, hi: f32) -> f32 {
        lo + (hi - lo) * self.next()
    }
}

/// A mixture of 40 random directions, jittered — clusters overlap in every
/// dimension, like embeddings.
fn centres() -> Vec<Vec<f32>> {
    let mut rng = Rng(0xC0FF_EE11);
    (0..40)
        .map(|_| (0..DIM).map(|_| rng.range(-1.0, 1.0)).collect())
        .collect()
}

fn point(centres: &[Vec<f32>], rng: &mut Rng) -> Vec<f32> {
    let c = &centres[(rng.next() * centres.len() as f32) as usize % centres.len()];
    c.iter().map(|x| x + rng.range(-0.6, 0.6)).collect()
}

fn doc(id: u64, v: &[f32]) -> Document {
    Document {
        id: DocumentId(format!("d{id}")),
        fields: vec![
            Field::text("body", format!("doc {id}")),
            Field::vector("emb", v.to_vec()),
        ],
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
    dot / (na * nb)
}

/// Builds the namespace, compacts it with `mode`, and returns mean
/// recall@10 over `queries`.
fn compacted_recall(mode: VectorCompactionMode, data_seed: u64, queries: &[Vec<f32>]) -> f64 {
    let dir = std::env::temp_dir().join(format!(
        "kosha-lire-recall-{mode:?}-{data_seed}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let ns = NamespaceId("lire".into());
    let idx = Indexer::new(dir.clone());
    let cs = centres();
    let mut rng = Rng(0x1234_5678 ^ data_seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut live: HashMap<u64, Vec<f32>> = HashMap::new();

    let batch: Vec<Document> = (0..BIG)
        .map(|id| {
            let v = point(&cs, &mut rng);
            let d = doc(id, &v);
            live.insert(id, v);
            d
        })
        .collect();
    idx.index_documents(ns.clone(), batch).unwrap();
    idx.flush_namespace(&ns).unwrap();

    // Updates: new vectors for existing ids, one flush-sized segment each
    // (their old versions become tombstones in the big segment).
    let mut updated = HashSet::new();
    for _ in 0..FLUSHES {
        let mut batch = Vec::new();
        while batch.len() < UPDATES_PER_FLUSH as usize {
            let id = (rng.next() * BIG as f32) as u64 % BIG;
            if !updated.insert(id) {
                continue;
            }
            let v = point(&cs, &mut rng);
            batch.push(doc(id, &v));
            live.insert(id, v);
        }
        idx.index_documents(ns.clone(), batch).unwrap();
        idx.flush_namespace(&ns).unwrap();
    }
    // The big batch may itself flush as several segments.
    assert!(idx.manifest(&ns).unwrap().segments.len() > FLUSHES as usize);

    let result = idx
        .compact_namespace_with_options(&ns, CompactOptions::full().with_vector_index(mode))
        .unwrap();
    assert!(result.merged);
    let manifest = idx.manifest(&ns).unwrap();
    assert_eq!(manifest.segments.len(), 1);

    let live: Vec<(String, Vec<f32>)> = live
        .into_iter()
        .map(|(id, v)| (format!("d{id}"), v))
        .collect();
    // Every merged vector sits in exactly one posting.
    {
        let seg = dir.join(&ns.0).join(&manifest.segments[0].segment_id.0);
        let reader = kosha_segment::SegmentReader::open(seg).unwrap();
        let layout = reader.vector_posting_layout("emb").unwrap().unwrap();
        let mut seqs: Vec<u32> = layout
            .postings
            .iter()
            .flat_map(|p| p.doc_seqs.iter().copied())
            .collect();
        seqs.sort_unstable();
        assert_eq!(seqs, (0..live.len() as u32).collect::<Vec<_>>());
    }
    let searcher = Searcher::new(dir.clone());
    let mut total = 0.0;
    for q in queries {
        let mut truth: Vec<(&str, f64)> = live
            .iter()
            .map(|(id, v)| (id.as_str(), cosine(q, v)))
            .collect();
        truth.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let truth: HashSet<&str> = truth[..10].iter().map(|(id, _)| *id).collect();

        let mut query: SearchQuery = serde_json::from_value(serde_json::json!({
            "query_text": "",
            "max_results": 10,
        }))
        .unwrap();
        // num_candidates 1: the probe budget is the namespace floor,
        // `KOSHA_KNN_BUDGET_FRACTION` (5%) of all centroids — the same
        // relative cost for both indexes.
        query.knn = Some(KnnQuery {
            field: "emb".into(),
            vector: q.clone(),
            k: 10,
            num_candidates: 1,
            filter: None,
            scoring: None,
            vectors: None,
        });
        let r = searcher
            .search(&ns, &manifest, &query, Some(&manifest.tombstones))
            .unwrap();
        assert_eq!(r.knn_degraded_segments, Some(0));
        let hits = r
            .results
            .iter()
            .filter(|d| truth.contains(d.doc_id.0.as_str()))
            .count();
        total += hits as f64 / 10.0;
    }
    let _ = std::fs::remove_dir_all(&dir);
    total / queries.len() as f64
}

#[test]
fn lire_compaction_recall_is_within_half_a_point_of_rebuild() {
    let cs = centres();
    let mut rng = Rng(0xABCD_EF01);
    let queries: Vec<Vec<f32>> = (0..200).map(|_| point(&cs, &mut rng)).collect();
    // Averaged over datasets: two from-scratch clusterings of the same
    // data already differ by about a point of recall@10 here.
    let (mut rebuild, mut lire) = (0.0, 0.0);
    let datasets = 3;
    for seed in 0..datasets {
        rebuild += compacted_recall(VectorCompactionMode::Rebuild, seed, &queries);
        lire += compacted_recall(VectorCompactionMode::Lire, seed, &queries);
    }
    let (rebuild, lire) = (rebuild / datasets as f64, lire / datasets as f64);
    eprintln!("recall@10 after compaction: rebuild {rebuild:.4}, lire {lire:.4}");
    assert!(
        lire >= rebuild - 0.005,
        "lire recall@10 {lire:.4} is more than 0.5pt below rebuild {rebuild:.4}"
    );
}
