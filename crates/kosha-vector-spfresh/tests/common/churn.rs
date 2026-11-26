use std::collections::HashMap;

use kosha_vector_spfresh::DeterministicRng;

#[derive(Clone)]
pub enum Op {
    Insert(u32, Vec<f32>),
    Delete(u32),
}

pub type CycleOps = Vec<Op>;
pub type LiveSnapshot = HashMap<u32, Vec<f32>>;

/// Generates `seed_count` vectors spread across several regions around the
/// same circle the churn sequence later drifts along — a stand-in for "the
/// already well-partitioned index the paper's ablation starts from" (Figure
/// 10 begins from Static, not from empty). Without this, a config with
/// splitting disabled can never grow past a single posting (an empty index
/// only allocates a new posting when it has zero active postings),
/// degenerating `search` into brute force over everything and making
/// "in-place-only" trivially *perfect* — the opposite of the paper's point,
/// and purely an artifact of starting from nothing rather than the
/// mechanism under test.
pub fn generate_seed(
    dim: usize,
    seed_count: usize,
    num_regions: usize,
    seed: u64,
) -> Vec<(u32, Vec<f32>)> {
    let mut rng = DeterministicRng::new(seed);
    let mut out = Vec::with_capacity(seed_count);
    for id in 0..seed_count as u32 {
        let region = id as usize % num_regions;
        let angle = (region as f32) * std::f32::consts::TAU / (num_regions as f32);
        let mut v = vec![0.0f32; dim];
        v[0] = angle.cos() * 10.0 + rng.next_f32_range(-0.4, 0.4);
        v[1] = angle.sin() * 10.0 + rng.next_f32_range(-0.4, 0.4);
        for x in v.iter_mut().skip(2) {
            *x = rng.next_f32_range(-0.1, 0.1);
        }
        out.push((id, v));
    }
    out
}

/// Builds `num_cycles` batches of ops with a cluster center that sweeps
/// (drifts) around a circle across cycles — old regions cool down as their
/// vectors get deleted (exercising Merge), a new region heats up as vectors
/// land near it (exercising Reassign of nearby boundary vectors), mirroring
/// the paper's own churn simulation (§5.2: daily delete-1%/insert-1% against
/// a shifting sample pool). `next_id`/`ground_truth` continue on from a
/// prior `generate_seed` call. Returns the per-cycle op batches plus a
/// snapshot of the live `{id: vector}` set after each cycle (used as recall
/// ground truth — identical for every config, since all configs replay the
/// exact same op sequence on top of the exact same seed).
pub fn generate_op_sequence(
    dim: usize,
    mut next_id: u32,
    mut ground_truth: HashMap<u32, Vec<f32>>,
    num_cycles: usize,
    ops_per_cycle: usize,
    seed: u64,
) -> (Vec<CycleOps>, Vec<LiveSnapshot>) {
    let mut rng = DeterministicRng::new(seed);
    let mut cycles = Vec::with_capacity(num_cycles);
    let mut snapshots = Vec::with_capacity(num_cycles);

    for cycle in 0..num_cycles {
        let mut ops = Vec::with_capacity(ops_per_cycle);
        let angle = (cycle as f32) * 0.6;
        let cx = angle.cos() * 10.0;
        let cy = angle.sin() * 10.0;

        for _ in 0..ops_per_cycle {
            let insert_prob = if ground_truth.len() < 200 { 0.9 } else { 0.55 };
            if ground_truth.is_empty() || rng.next_f64() < insert_prob {
                let id = next_id;
                next_id += 1;
                let mut v = vec![0.0f32; dim];
                v[0] = cx + rng.next_f32_range(-0.4, 0.4);
                v[1] = cy + rng.next_f32_range(-0.4, 0.4);
                for x in v.iter_mut().skip(2) {
                    *x = rng.next_f32_range(-0.1, 0.1);
                }
                ops.push(Op::Insert(id, v.clone()));
                ground_truth.insert(id, v);
            } else {
                // Sorted, not raw HashMap iteration order: std's hasher is
                // randomly seeded per-process, so an unsorted collect here
                // would pick a different victim on every run despite the
                // fixed rng seed, silently breaking reproducibility.
                let mut ids: Vec<u32> = ground_truth.keys().copied().collect();
                ids.sort_unstable();
                let victim = ids[rng.next_usize(ids.len())];
                ops.push(Op::Delete(victim));
                ground_truth.remove(&victim);
            }
        }
        cycles.push(ops);
        snapshots.push(ground_truth.clone());
    }
    (cycles, snapshots)
}
