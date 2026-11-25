//! Research #464 measurement: does a tombstone-ratio compaction trigger
//! (`CompactionPolicy::tombstone_ratio_bp`, `KOSHA_COMPACT_TOMBSTONE_RATIO`)
//! keep kNN recall and probe work steady under update churn?
//!
//! One large segment, too big to be a tiered merge input, then cycles of
//! "update 10% of the documents with new vectors, flush, tiered compaction".
//! Each update tombstones the old version in whichever segment held it. With
//! the trigger off, the large segment never merges, so it keeps every dead
//! vector; with it on, it is rewritten once its tombstoned share crosses the
//! ratio. Recall@10 is against brute force over the live documents, at the
//! namespace floor budget (`num_candidates` 1).
//!
//! Measurement only (prints a table), so it is ignored by default:
//!
//! ```text
//! cargo test --release -p kosha-query --test tombstone_compaction_churn -- --ignored --nocapture
//! ```

use std::collections::{HashMap, HashSet};

use kosha_core::{Document, DocumentId, Field, KnnQuery, NamespaceId, SearchQuery};
use kosha_query::Searcher;
use kosha_write::{CompactOptions, CompactionPolicy, Indexer};

const DIM: usize = 32;
const BIG: u64 = 6_000;
const CYCLES: usize = 8;
const UPDATES_PER_CYCLE: u64 = BIG / 10;

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

struct Cycle {
    segments: usize,
    indexed: u64,
    rewrites: usize,
    recall: f64,
    postings: f64,
    centroids: f64,
}

fn run(trigger_bp: Option<u32>, queries: &[Vec<f32>]) -> Vec<Cycle> {
    let dir = std::env::temp_dir().join(format!(
        "kosha-tombstone-churn-{}-{}",
        trigger_bp.unwrap_or(0),
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let ns = NamespaceId("churn".into());
    let policy = CompactionPolicy {
        // The initial segment(s) are never tiered inputs on size alone.
        max_mergeable_docs: 1_000,
        trigger_mergeable_segments: 2,
        tombstone_ratio_bp: trigger_bp,
        ..CompactionPolicy::default()
    };
    let idx = Indexer::new(dir.clone()).with_compaction_policy(policy.clone());
    let cs = centres();
    let mut rng = Rng(0x1234_5678);
    let mut live: HashMap<u64, Vec<f32>> = HashMap::new();
    let batch: Vec<Document> = (0..BIG)
        .map(|id| {
            let v = point(&cs, &mut rng);
            live.insert(id, v.clone());
            doc(id, &v)
        })
        .collect();
    idx.index_documents(ns.clone(), batch).unwrap();
    idx.flush_namespace(&ns).unwrap();

    let mut out = Vec::new();
    for _ in 0..CYCLES {
        let mut picked = HashSet::new();
        let mut batch = Vec::new();
        while batch.len() < UPDATES_PER_CYCLE as usize {
            let id = (rng.next() * BIG as f32) as u64 % BIG;
            if !picked.insert(id) {
                continue;
            }
            let v = point(&cs, &mut rng);
            live.insert(id, v.clone());
            batch.push(doc(id, &v));
        }
        idx.index_documents(ns.clone(), batch).unwrap();
        idx.flush_namespace(&ns).unwrap();
        // The scheduler's loop: tiered passes while one is called for.
        let mut rewrites = 0;
        for _ in 0..8 {
            if !idx.needs_compaction(&ns) {
                break;
            }
            let r = idx
                .compact_namespace_with_options(&ns, CompactOptions::tiered(policy.clone()))
                .unwrap();
            if !r.merged {
                break;
            }
            rewrites += 1;
        }

        let manifest = idx.manifest(&ns).unwrap();
        let live_ids: Vec<(String, &Vec<f32>)> =
            live.iter().map(|(id, v)| (format!("d{id}"), v)).collect();
        let searcher = Searcher::new(dir.clone());
        let (mut recall, mut postings, mut centroids) = (0.0, 0.0, 0.0);
        for q in queries {
            let mut truth: Vec<(&str, f64)> = live_ids
                .iter()
                .map(|(id, v)| (id.as_str(), cosine(q, v)))
                .collect();
            truth.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            let truth: HashSet<&str> = truth[..10].iter().map(|(id, _)| *id).collect();
            let mut query: SearchQuery =
                serde_json::from_value(serde_json::json!({"query_text": "", "max_results": 10}))
                    .unwrap();
            query.knn = Some(KnnQuery {
                query_text: None,
                embedding_model: None,
                field: "emb".into(),
                vector: q.clone(),
                k: 10,
                num_candidates: 1,
                filter: None,
                scoring: None,
                vectors: None,
            });
            let (r, stats) = searcher
                .search_with_stats(&ns, &manifest, &query, Some(&manifest.tombstones))
                .unwrap();
            recall += r
                .results
                .iter()
                .filter(|d| truth.contains(d.doc_id.0.as_str()))
                .count() as f64
                / 10.0;
            postings += stats.knn_postings_probed as f64;
            centroids += stats.knn_centroids_scanned as f64;
        }
        let n = queries.len() as f64;
        out.push(Cycle {
            segments: manifest.segments.len(),
            indexed: manifest.segments.iter().map(|e| e.doc_count as u64).sum(),
            rewrites,
            recall: recall / n,
            postings: postings / n,
            centroids: centroids / n,
        });
    }
    let _ = std::fs::remove_dir_all(&dir);
    out
}

#[test]
#[ignore = "measurement for research #464; run with --ignored --nocapture"]
fn tombstone_ratio_trigger_under_update_churn() {
    let cs = centres();
    let mut rng = Rng(0xABCD_EF01);
    let queries: Vec<Vec<f32>> = (0..200).map(|_| point(&cs, &mut rng)).collect();
    for trigger in [None, Some(2_000), Some(4_000)] {
        let label = trigger.map_or("off".to_string(), |bp| format!("{:.2}", bp as f64 / 1e4));
        eprintln!("\ntrigger {label}: {BIG} live docs, {UPDATES_PER_CYCLE} updates per cycle");
        eprintln!("cycle | segments | indexed docs (live {BIG}) | merges | recall@10 | postings/q | centroids/q");
        for (i, c) in run(trigger, &queries).iter().enumerate() {
            eprintln!(
                "{:>5} | {:>8} | {:>12} | {:>6} | {:>9.4} | {:>10.1} | {:>11.1}",
                i + 1,
                c.segments,
                c.indexed,
                c.rewrites,
                c.recall,
                c.postings,
                c.centroids
            );
        }
    }
}
