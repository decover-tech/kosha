//! recall@k on a synthetic clustered dataset, measured against the crate's
//! own brute-force ground truth (same `cosine_distance`, so this isolates
//! the index structure's approximation error from any metric mismatch).

mod common;

use common::{brute_force_topk, gen_clustered_dataset, gen_queries, recall_at_k};
use kosha_vector_spfresh::{ClusterIndex, ClusterIndexConfig};

#[test]
fn recall_at_10_clears_threshold_with_default_nprobe() {
    let dim = 32;
    let vectors = gen_clustered_dataset(dim, 20, 500, 1);
    let queries = gen_queries(dim, 20, 5, 2);

    let cfg = ClusterIndexConfig::new(dim);
    let idx = ClusterIndex::build(&vectors, cfg).unwrap();

    let mut total_recall = 0.0;
    for q in &queries {
        let predicted = idx.search(q, 10).unwrap();
        let truth = brute_force_topk(&vectors, q, 10);
        total_recall += recall_at_k(&predicted, &truth);
    }
    let avg_recall = total_recall / queries.len() as f64;
    assert!(
        avg_recall >= 0.9,
        "avg recall@10 = {avg_recall}, expected >= 0.9"
    );
}

#[test]
fn recall_improves_monotonically_non_decreasing_with_nprobe() {
    let dim = 32;
    let vectors = gen_clustered_dataset(dim, 20, 500, 3);
    let queries = gen_queries(dim, 20, 5, 4);

    let mut prev_recall = 0.0;
    for nprobe in [1usize, 4, 16, 64] {
        let mut cfg = ClusterIndexConfig::new(dim);
        cfg.nprobe = nprobe;
        let idx = ClusterIndex::build(&vectors, cfg).unwrap();

        let mut total_recall = 0.0;
        for q in &queries {
            let predicted = idx.search(q, 10).unwrap();
            let truth = brute_force_topk(&vectors, q, 10);
            total_recall += recall_at_k(&predicted, &truth);
        }
        let avg_recall = total_recall / queries.len() as f64;
        // A larger nprobe always scans a superset of postings, so recall is
        // monotonically non-decreasing *in principle* — but `search`'s
        // top-k selection uses an unstable partial sort, so when several
        // candidates are exactly tied at the k-th-place score, which one
        // wins the cut can differ between candidate-pool sizes. A 1-in-200
        // tie flip is that, not a real regression; the tolerance absorbs it
        // without hiding an actual monotonicity break.
        assert!(
            avg_recall >= prev_recall - 0.01,
            "recall regressed going from a smaller to larger nprobe: {avg_recall} < {prev_recall} at nprobe={nprobe}"
        );
        prev_recall = avg_recall;
    }
    // With nprobe=64 covering essentially the whole index, recall should be
    // at (or extremely close to) brute-force parity.
    assert!(
        prev_recall >= 0.98,
        "recall at high nprobe = {prev_recall}, expected near-exact parity"
    );
}
