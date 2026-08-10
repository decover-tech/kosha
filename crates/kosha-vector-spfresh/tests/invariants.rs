//! Structural invariants + termination, checked after every single op across
//! thousands of randomized insert/delete cycles. This is the empirical
//! stand-in for the paper's convergence proof: `Split`/`Merge`/`Reassign`
//! run synchronously inline (see `lib.rs`'s doc comment on `ClusterIndex`),
//! so if the paper's argument (posting count is monotonically bounded above
//! by total vector count, so split→reassign cascades terminate) didn't
//! hold, this test would either panic (`MAX_CASCADE_DEPTH` in
//! `ops/split.rs`) or time out.

mod common;

use std::collections::HashMap;

use common::{brute_force_topk, recall_at_k};
use kosha_vector_spfresh::{ClusterIndex, ClusterIndexConfig, DeterministicRng};

fn assert_structural_invariants(idx: &ClusterIndex, ground_truth: &HashMap<u32, Vec<f32>>) {
    assert_eq!(
        idx.len(),
        ground_truth.len(),
        "index live count diverged from expected"
    );

    let mut idx_ids = idx.ids();
    idx_ids.sort_unstable();
    let mut truth_ids: Vec<u32> = ground_truth.keys().copied().collect();
    truth_ids.sort_unstable();
    assert_eq!(idx_ids, truth_ids, "index id set diverged from expected");

    let sizes = idx.posting_sizes();
    let active = idx.active_posting_count();
    assert_eq!(
        sizes.len(),
        active,
        "posting_sizes() length disagrees with active_posting_count()"
    );
    let sum: usize = sizes.iter().sum();
    assert_eq!(
        sum,
        idx.len(),
        "posting sizes don't sum to total live count (double-owned or lost vector)"
    );

    for &size in &sizes {
        assert!(
            size <= idx.config().max_posting_size,
            "posting size {size} exceeds max_posting_size"
        );
        if active > 1 {
            assert!(
                size >= idx.config().min_posting_size,
                "posting size {size} below min_posting_size with {active} active postings"
            );
        }
    }

    // Every posting-count increase happens one-at-a-time via Split (the
    // paper's convergence argument); it should never exceed the number of
    // vectors ever inserted, let alone the current live count by a wide
    // margin.
    assert!(
        active <= ground_truth.len().max(1),
        "active posting count {active} exceeds live vector count"
    );
}

#[test]
fn invariants_hold_across_thousands_of_randomized_ops() {
    let dim = 8;
    let mut cfg = ClusterIndexConfig::new(dim);
    cfg.target_posting_size = 16;
    cfg.max_posting_size = 32;
    cfg.min_posting_size = 4;

    let mut idx = ClusterIndex::build(&[], cfg).unwrap();
    let mut ground_truth: HashMap<u32, Vec<f32>> = HashMap::new();
    let mut rng = DeterministicRng::new(99);
    let mut next_id = 0u32;

    const N_OPS: usize = 4000;
    for i in 0..N_OPS {
        // Mostly insert (so the index actually grows enough to exercise
        // splits), with deletes once there's something to delete — biased
        // toward insert early, more balanced later so merges get exercised
        // too.
        let insert_prob = if i < 500 { 0.95 } else { 0.6 };
        let do_insert = ground_truth.is_empty() || rng.next_f64() < insert_prob;

        if do_insert {
            let id = next_id;
            next_id += 1;
            // Cluster centers drift over the run — this is what exercises
            // Reassign (new-region growth pulling boundary vectors) and
            // Merge (old-region postings shrinking as their neighborhood
            // goes cold), not just Split.
            let cluster = (i / 200) % 5;
            let angle = (cluster as f32) * std::f32::consts::TAU / 5.0;
            let mut v = vec![0.0f32; dim];
            v[0] = angle.cos() * 10.0 + rng.next_f32_range(-0.5, 0.5);
            v[1] = angle.sin() * 10.0 + rng.next_f32_range(-0.5, 0.5);
            for x in v.iter_mut().skip(2) {
                *x = rng.next_f32_range(-0.1, 0.1);
            }
            idx.insert(id, v.clone()).unwrap();
            ground_truth.insert(id, v);
        } else {
            // Sorted, not raw HashMap iteration order — see the identical
            // comment in churn_ablation.rs's generate_op_sequence.
            let mut ids: Vec<u32> = ground_truth.keys().copied().collect();
            ids.sort_unstable();
            let victim = ids[rng.next_usize(ids.len())];
            assert!(idx.delete(victim), "delete of a known-live id must succeed");
            ground_truth.remove(&victim);
        }

        assert_structural_invariants(&idx, &ground_truth);
    }

    // Ties the structural invariants back to actual search quality: passing
    // bookkeeping invariants alone wouldn't catch a bug that moved a
    // vector's *value* into the wrong posting while still keeping the
    // id-accounting consistent.
    let vectors = common::sorted_vectors(&ground_truth);
    assert!(
        vectors.len() > 100,
        "sanity: churn run should have left a substantial live set"
    );
    let mut total_recall = 0.0;
    let sample: Vec<&(u32, Vec<f32>)> = vectors
        .iter()
        .step_by((vectors.len() / 20).max(1))
        .collect();
    for (_, v) in &sample {
        let predicted = idx.search(v, 10).unwrap();
        let truth = brute_force_topk(&vectors, v, 10);
        total_recall += recall_at_k(&predicted, &truth);
    }
    let avg_recall = total_recall / sample.len() as f64;
    assert!(
        avg_recall >= 0.7,
        "post-churn recall@10 = {avg_recall}, expected >= 0.7"
    );
}
