//! Shared synthetic-data + ground-truth helpers for the integration tests.
//! Not part of the crate's public API surface — test-only.

#![allow(dead_code)]

use std::collections::HashMap;

use kosha_vector_spfresh::{cosine_distance, DeterministicRng};

/// Converts a `{id: vector}` ground-truth map into an id-sorted `Vec`.
///
/// Not cosmetic: std's `HashMap` hasher is randomly seeded per process, so
/// `map.iter().collect()` yields a different order on every run. Anything
/// downstream that uses that order for more than an unordered scan — most
/// importantly, `ClusterIndex::build`'s bisection, which seeds via
/// `rng.next_usize(members.len())` indexing into input order — would then
/// silently produce a different index on every run despite a fixed RNG
/// seed. Always go through this helper rather than collecting a HashMap
/// straight into a `Vec` for anything fed to `build`.
pub fn sorted_vectors(map: &HashMap<u32, Vec<f32>>) -> Vec<(u32, Vec<f32>)> {
    let mut v: Vec<(u32, Vec<f32>)> = map.iter().map(|(id, vec)| (*id, vec.clone())).collect();
    v.sort_unstable_by_key(|(id, _)| *id);
    v
}

/// Generates `num_clusters` well-separated Gaussian-ish blobs of
/// `per_cluster` points each in `dim` dimensions (first two dims carry the
/// cluster structure via a coarse grid + jitter; remaining dims are jitter
/// only) — deterministic given `seed`.
pub fn gen_clustered_dataset(
    dim: usize,
    num_clusters: usize,
    per_cluster: usize,
    seed: u64,
) -> Vec<(u32, Vec<f32>)> {
    let mut rng = DeterministicRng::new(seed);
    let mut out = Vec::with_capacity(num_clusters * per_cluster);
    let mut id = 0u32;
    for c in 0..num_clusters {
        // Spread cluster centers around a circle so they're pairwise
        // well-separated by cosine distance regardless of num_clusters.
        let angle = (c as f32) * std::f32::consts::TAU / (num_clusters as f32);
        let cx = angle.cos() * 10.0;
        let cy = angle.sin() * 10.0;
        for _ in 0..per_cluster {
            let mut v = vec![0.0f32; dim];
            v[0] = cx + rng.next_f32_range(-0.5, 0.5);
            if dim > 1 {
                v[1] = cy + rng.next_f32_range(-0.5, 0.5);
            }
            for x in v.iter_mut().skip(2) {
                *x = rng.next_f32_range(-0.1, 0.1);
            }
            out.push((id, v));
            id += 1;
        }
    }
    out
}

/// Exact brute-force top-k by cosine similarity, using the crate's own
/// `cosine_distance` so this is a fair ground truth (same metric, not an
/// independent implementation that could disagree on tie-breaking/edge
/// cases).
pub fn brute_force_topk(vectors: &[(u32, Vec<f32>)], query: &[f32], k: usize) -> Vec<(u32, f32)> {
    let mut scored: Vec<(u32, f32)> = vectors
        .iter()
        .map(|(id, v)| (*id, 1.0 - cosine_distance(query, v)))
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    scored.truncate(k);
    scored
}

/// Fraction of `ground_truth`'s ids present in `predicted` (standard
/// recall@k over ids, order-independent).
pub fn recall_at_k(predicted: &[(u32, f32)], ground_truth: &[(u32, f32)]) -> f64 {
    if ground_truth.is_empty() {
        return 1.0;
    }
    let predicted_ids: std::collections::HashSet<u32> =
        predicted.iter().map(|(id, _)| *id).collect();
    let hits = ground_truth
        .iter()
        .filter(|(id, _)| predicted_ids.contains(id))
        .count();
    hits as f64 / ground_truth.len() as f64
}

/// A handful of deterministic query vectors near (but not exactly at) the
/// generator's cluster centers — realistic "search for something like
/// cluster c" queries rather than only exact-match lookups.
pub fn gen_queries(
    dim: usize,
    num_clusters: usize,
    per_cluster_queries: usize,
    seed: u64,
) -> Vec<Vec<f32>> {
    let mut rng = DeterministicRng::new(seed);
    let mut out = Vec::with_capacity(num_clusters * per_cluster_queries);
    for c in 0..num_clusters {
        let angle = (c as f32) * std::f32::consts::TAU / (num_clusters as f32);
        let cx = angle.cos() * 10.0;
        let cy = angle.sin() * 10.0;
        for _ in 0..per_cluster_queries {
            let mut v = vec![0.0f32; dim];
            v[0] = cx + rng.next_f32_range(-0.5, 0.5);
            if dim > 1 {
                v[1] = cy + rng.next_f32_range(-0.5, 0.5);
            }
            for x in v.iter_mut().skip(2) {
                *x = rng.next_f32_range(-0.1, 0.1);
            }
            out.push(v);
        }
    }
    out
}
