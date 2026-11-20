//! Unit tests for the `relevance` bench's metrics (#258). The bench itself
//! is `harness = false`, so its pure metric module is compiled in here too.

#[allow(dead_code)]
#[path = "../benches/relevance/metrics.rs"]
mod metrics;

use std::collections::HashMap;

use metrics::{brute_force_top_k, cosine, ndcg_at, parse_qrels, recall_at, set_recall, Mean};

fn ids(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

fn judged(v: &[(&str, u32)]) -> HashMap<String, u32> {
    v.iter().map(|(d, g)| (d.to_string(), *g)).collect()
}

#[test]
fn parses_trec_qrels_keeping_the_highest_grade() {
    let q =
        parse_qrels("# comment\nq1 0 a 1\nq1 0 b 2\n\nq1 0 a 2\nq2 0 c 0\nq3 Q0 d 1\n").unwrap();
    assert_eq!(q.len(), 2, "q2 has no relevant document");
    assert_eq!(q["q1"], judged(&[("a", 2), ("b", 2)]));
    assert_eq!(q["q3"], judged(&[("d", 1)]));
    assert!(parse_qrels("q1 0 a").is_err());
    assert!(parse_qrels("q1 0 a high").is_err());
}

#[test]
fn ndcg_is_one_for_the_ideal_ranking_and_matches_a_hand_computation() {
    let j = judged(&[("a", 2), ("b", 1), ("c", 1)]);
    let ideal = ndcg_at(&ids(&["a", "b", "c", "x"]), &j, 10).unwrap();
    assert!((ideal - 1.0).abs() < 1e-12);
    // Reversed: dcg = 1/1 + 1/log2(3) + 3/2; idcg = 3/1 + 1/log2(3) + 1/2.
    let got = ndcg_at(&ids(&["c", "b", "a"]), &j, 10).unwrap();
    let l3 = 3f64.log2();
    let want = (1.0 + 1.0 / l3 + 1.5) / (3.0 + 1.0 / l3 + 0.5);
    assert!((got - want).abs() < 1e-12, "{got} vs {want}");
    // Only the first k hits count.
    assert_eq!(ndcg_at(&ids(&["x", "a"]), &j, 1), Some(0.0));
    // A duplicated hit scores once.
    let dup = ndcg_at(&ids(&["a", "a"]), &judged(&[("a", 1), ("b", 1)]), 10).unwrap();
    let single = ndcg_at(&ids(&["a"]), &judged(&[("a", 1), ("b", 1)]), 10).unwrap();
    assert!((dup - single).abs() < 1e-12);
    assert_eq!(ndcg_at(&ids(&["a"]), &HashMap::new(), 10), None);
}

#[test]
fn recall_counts_relevant_hits_within_k() {
    let j = judged(&[("a", 1), ("b", 2), ("c", 1), ("d", 1)]);
    assert_eq!(recall_at(&ids(&["a", "x", "b"]), &j, 100), Some(0.5));
    assert_eq!(recall_at(&ids(&["a", "x", "b"]), &j, 2), Some(0.25));
    assert_eq!(recall_at(&ids(&["a", "a"]), &j, 2), Some(0.25));
    assert_eq!(recall_at(&ids(&["a"]), &HashMap::new(), 2), None);
}

#[test]
fn set_recall_and_brute_force() {
    assert_eq!(set_recall(&ids(&["a", "b"]), &ids(&["b", "c"])), Some(0.5));
    assert_eq!(set_recall(&ids(&["a"]), &[]), None);
    let vs: Vec<(&str, Vec<f32>)> = vec![
        ("far", vec![-1.0, 0.0]),
        ("near", vec![1.0, 0.1]),
        ("tie_b", vec![0.0, 1.0]),
        ("tie_a", vec![0.0, 2.0]),
    ];
    let top = brute_force_top_k(&[1.0, 0.0], vs.iter().map(|(id, v)| (*id, v.as_slice())), 3);
    // Equal cosines are ordered by id.
    assert_eq!(top, ids(&["near", "tie_a", "tie_b"]));
    assert_eq!(cosine(&[0.0, 0.0], &[1.0, 0.0]), 0.0);
}

#[test]
fn mean_skips_unscorable_queries() {
    let mut m = Mean::default();
    assert_eq!(m.value(), None);
    m.add(Some(1.0));
    m.add(None);
    m.add(Some(0.0));
    assert_eq!((m.value(), m.n), (Some(0.5), 2));
}
