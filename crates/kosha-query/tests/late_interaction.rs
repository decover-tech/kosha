//! Late-interaction (ColBERT-style) kNN through the searcher (#205):
//! namespace-wide MaxSim ranking across segments, tombstones, `knn.filter`,
//! hybrid fusion, and request validation.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use kosha_core::{
    Bm25Params, DocumentId, Field, FieldType, KoshaError, Manifest, NamespaceId, SearchQuery,
    SegmentId,
};
use kosha_query::Searcher;
use kosha_segment::SegmentWriter;

const DIM: usize = 8;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> f32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        ((self.0 >> 11) as f64 / (1u64 << 53) as f64) as f32 * 2.0 - 1.0
    }
    fn vector(&mut self) -> Vec<f32> {
        (0..DIM).map(|_| self.next()).collect()
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

fn maxsim(query: &[Vec<f32>], doc: &[Vec<f32>]) -> f64 {
    query
        .iter()
        .map(|q| doc.iter().map(|d| cosine(q, d)).fold(f64::MIN, f64::max))
        .sum()
}

struct Corpus {
    dir: PathBuf,
    ns: NamespaceId,
    manifest: Manifest,
    bags: Vec<Vec<Vec<f32>>>,
    tombstones: HashMap<SegmentId, HashSet<u32>>,
}

/// 80 documents over two segments; `d3` is tombstoned.
fn corpus(name: &str) -> Corpus {
    let dir = std::env::temp_dir().join(format!("kosha-q-li-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let ns = NamespaceId("li".into());
    let mut rng = Rng(42);
    let bags: Vec<Vec<Vec<f32>>> = (0..80)
        .map(|i| (0..3 + i % 5).map(|_| rng.vector()).collect())
        .collect();
    let mut segments = Vec::new();
    for (s, range) in [("s1", 0..40), ("s2", 40..80)] {
        let mut w = SegmentWriter::new(SegmentId(s.into()), dir.join(&ns.0).join(s));
        for i in range.clone() {
            w.add_document(
                DocumentId(format!("d{i}")),
                vec![
                    Field::text("body", if i % 2 == 0 { "alpha" } else { "beta" }),
                    Field::keyword("parity", if i % 2 == 0 { "even" } else { "odd" }),
                    Field::vector("emb", bags[i][0].clone()),
                    Field {
                        name: "tokens".into(),
                        field_type: FieldType::Vector,
                        value: serde_json::to_string(&bags[i]).unwrap(),
                    },
                ],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();
        segments.push(serde_json::json!({"segment_id": s, "doc_count": range.len()}));
    }
    let manifest: Manifest =
        serde_json::from_value(serde_json::json!({"version": 1, "segments": segments})).unwrap();
    let tombstones = HashMap::from([(SegmentId("s1".into()), HashSet::from([3u32]))]);
    Corpus {
        dir,
        ns,
        manifest,
        bags,
        tombstones,
    }
}

fn query(json: serde_json::Value) -> SearchQuery {
    let q: SearchQuery = serde_json::from_value(json).unwrap();
    q.validate().unwrap();
    q
}

fn ids(r: &kosha_core::SearchResult) -> Vec<String> {
    r.results.iter().map(|d| d.doc_id.0.clone()).collect()
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn late_interaction_ranks_the_namespace_by_maxsim() {
    let c = corpus("rank");
    let searcher = Searcher::new(c.dir.clone());
    let q_tokens = vec![
        c.bags[17][1].clone(),
        c.bags[55][0].clone(),
        c.bags[3][2].clone(),
    ];
    let q = query(serde_json::json!({
        "query_text": "", "max_results": 6,
        "knn": {"field": "tokens", "vectors": q_tokens, "k": 6, "num_candidates": 1000}
    }));
    assert!(q.knn.as_ref().unwrap().is_late_interaction());
    let r = searcher
        .search(&c.ns, &c.manifest, &q, Some(&c.tombstones))
        .unwrap();
    let mut expected: Vec<(usize, f64)> = (0..80)
        .filter(|&i| i != 3)
        .map(|i| (i, maxsim(&q_tokens, &c.bags[i])))
        .collect();
    expected.sort_by(|a, b| b.1.total_cmp(&a.1));
    let expected: Vec<String> = expected[..6].iter().map(|(i, _)| format!("d{i}")).collect();
    assert_eq!(ids(&r), expected);
    assert!(!ids(&r).contains(&"d3".to_string()), "tombstoned");
    assert_eq!(r.knn_degraded_segments, Some(0));
    // Stored bags come back as sent.
    let top = &r.results[0];
    let i: usize = top.doc_id.0[1..].parse().unwrap();
    let tokens = top.fields.iter().find(|f| f.name == "tokens").unwrap();
    assert_eq!(tokens.value, serde_json::to_string(&c.bags[i]).unwrap());

    // knn.filter restricts candidates inside the probe.
    let q = query(serde_json::json!({
        "query_text": "", "max_results": 5,
        "knn": {"field": "tokens", "vectors": q_tokens, "k": 5,
                "filter": {"term": {"parity": "odd"}}}
    }));
    let r = searcher.search(&c.ns, &c.manifest, &q, None).unwrap();
    assert_eq!(r.results.len(), 5);
    assert!(ids(&r)
        .iter()
        .all(|id| id[1..].parse::<usize>().unwrap() % 2 == 1));
    cleanup(&c.dir);
}

#[test]
fn hybrid_and_single_vector_queries_coexist() {
    let c = corpus("hybrid");
    let searcher = Searcher::new(c.dir.clone());
    let q_tokens = vec![c.bags[20][0].clone(), c.bags[21][1].clone()];
    let q = query(serde_json::json!({
        "query_text": "alpha", "max_results": 5,
        "knn": {"field": "tokens", "vectors": q_tokens, "k": 5}
    }));
    let r = searcher.search(&c.ns, &c.manifest, &q, None).unwrap();
    assert_eq!(r.results.len(), 5);
    assert!(r.results.iter().any(|d| d
        .rank_details
        .as_ref()
        .is_some_and(|rd| rd.knn_rank.is_some())));

    // The single-vector index on the same documents is untouched.
    let q = query(serde_json::json!({
        "query_text": "", "max_results": 1,
        "knn": {"field": "emb", "vector": c.bags[44][0], "k": 1}
    }));
    let r = searcher.search(&c.ns, &c.manifest, &q, None).unwrap();
    assert_eq!(ids(&r), vec!["d44".to_string()]);

    // `scoring: late_interaction` with one `vector` is a one-token bag.
    let q = query(serde_json::json!({
        "query_text": "", "max_results": 1,
        "knn": {"field": "tokens", "vector": c.bags[61][2], "k": 1,
                "scoring": "late_interaction"}
    }));
    let r = searcher.search(&c.ns, &c.manifest, &q, None).unwrap();
    assert_eq!(ids(&r), vec!["d61".to_string()]);
    cleanup(&c.dir);
}

#[test]
fn late_interaction_request_errors() {
    let bad = |json: serde_json::Value| {
        let q: SearchQuery = serde_json::from_value(json).unwrap();
        q.validate().unwrap_err()
    };
    bad(serde_json::json!({"query_text": "",
        "knn": {"field": "t", "vector": [1.0], "vectors": [[1.0]]}}));
    bad(serde_json::json!({"query_text": "", "knn": {"field": "t", "vectors": []}}));
    bad(serde_json::json!({"query_text": "", "knn": {"field": "t", "k": 3}}));
    bad(serde_json::json!({"query_text": "",
        "knn": {"field": "t", "vectors": [[1.0, 0.0], [1.0]]}}));
    bad(serde_json::json!({"query_text": "",
        "knn": {"field": "t", "vectors": [[1.0]], "scoring": "cosine"}}));
    bad(serde_json::json!({"query_text": "",
        "knn": {"field": "t", "scoring": "late_interaction"}}));

    // A dimension the field does not have is the client's error.
    let c = corpus("dim");
    let searcher = Searcher::new(c.dir.clone());
    let q = query(serde_json::json!({"query_text": "",
        "knn": {"field": "tokens", "vectors": [[1.0, 0.0, 0.0]]}}));
    let err = searcher.search(&c.ns, &c.manifest, &q, None).unwrap_err();
    assert!(matches!(err, KoshaError::InvalidQuery(_)), "{err:?}");
    cleanup(&c.dir);
}
