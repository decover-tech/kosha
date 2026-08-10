//! Balanced 2-way k-means bisection — the single primitive shared by initial
//! index construction (`build.rs`) and the runtime `Split` operation
//! (`ops/split.rs`). The SPFresh/SPANN papers cite a "multi-constraint
//! balanced clustering algorithm" for this step without giving its detail;
//! this is a concrete, from-scratch stand-in — see README.md.

use crate::point::{cosine_distance, mean_vector};
use crate::rng::DeterministicRng;
use crate::ClusterIndexConfig;

pub(crate) struct BisectResult {
    pub(crate) left_ids: Vec<u32>,
    pub(crate) right_ids: Vec<u32>,
    pub(crate) left_centroid: Vec<f32>,
    pub(crate) right_centroid: Vec<f32>,
}

/// Splits `members` into two roughly-balanced groups by cosine distance.
///
/// `members` must be non-empty. A degenerate input (e.g. every vector
/// numerically identical) falls back to a deterministic index-order split
/// rather than looping or producing an empty half — callers (`build.rs`,
/// `ops/split.rs`) must still check for an empty half themselves, since a
/// group of size 1 legitimately produces one.
pub(crate) fn balanced_bisect(
    members: &[(u32, &[f32])],
    dim: usize,
    cfg: &ClusterIndexConfig,
    rng: &mut DeterministicRng,
) -> BisectResult {
    let n = members.len();
    assert!(n > 0, "balanced_bisect: members must be non-empty");

    if n == 1 {
        let (id, v) = members[0];
        return BisectResult {
            left_ids: vec![id],
            right_ids: Vec::new(),
            left_centroid: v.to_vec(),
            right_centroid: v.to_vec(),
        };
    }

    // 1. Seed: a random point, then its farthest point (deterministic
    // lowest-id tie-break so results are reproducible across runs).
    let idx_a = rng.next_usize(n);
    let mut idx_b = if idx_a == 0 { 1 } else { 0 };
    let mut best_dist = cosine_distance(members[idx_a].1, members[idx_b].1);
    for i in 0..n {
        if i == idx_a {
            continue;
        }
        let d = cosine_distance(members[idx_a].1, members[i].1);
        if d > best_dist || (d == best_dist && members[i].0 < members[idx_b].0) {
            best_dist = d;
            idx_b = i;
        }
    }

    if best_dist <= 0.0 {
        // Degenerate: every vector is (numerically) identical in direction —
        // k-means has nothing to separate on. Split by index order instead
        // of looping forever trying to find a real separation.
        return index_order_split(members, dim);
    }

    let mut centroid_a = members[idx_a].1.to_vec();
    let mut centroid_b = members[idx_b].1.to_vec();
    let mut assign = vec![false; n]; // false = a, true = b

    // 2. Lloyd iterations to convergence (or the iteration cap).
    for iter in 0..cfg.max_kmeans_iters {
        let mut new_assign = vec![false; n];
        for (i, (_, v)) in members.iter().enumerate() {
            let da = cosine_distance(&centroid_a, v);
            let db = cosine_distance(&centroid_b, v);
            new_assign[i] = db < da;
        }

        let count_b = new_assign.iter().filter(|&&b| b).count();
        if count_b == 0 || count_b == n {
            // One side went empty — reseed it with the point farthest from
            // the surviving centroid instead of letting k-means collapse.
            let (surviving_centroid, reseed_to_b) = if count_b == 0 {
                (&centroid_a, true)
            } else {
                (&centroid_b, false)
            };
            let far = (0..n)
                .max_by(|&i, &j| {
                    let di = cosine_distance(surviving_centroid, members[i].1);
                    let dj = cosine_distance(surviving_centroid, members[j].1);
                    di.partial_cmp(&dj).unwrap()
                })
                .expect("n > 1 here");
            new_assign[far] = reseed_to_b;
        }

        let converged = iter > 0 && new_assign == assign;
        assign = new_assign;

        centroid_a = mean_vector(
            members
                .iter()
                .zip(assign.iter())
                .filter(|(_, &b)| !b)
                .map(|((_, v), _)| *v),
            dim,
        );
        centroid_b = mean_vector(
            members
                .iter()
                .zip(assign.iter())
                .filter(|(_, &b)| b)
                .map(|((_, v), _)| *v),
            dim,
        );

        if converged {
            break;
        }
    }

    // 3. Rebalance pass: move the larger cluster's furthest-from-centroid
    // members into the smaller cluster until within the configured ratio.
    let mut group_a: Vec<usize> = (0..n).filter(|&i| !assign[i]).collect();
    let mut group_b: Vec<usize> = (0..n).filter(|&i| assign[i]).collect();

    for _ in 0..cfg.max_rebalance_iters {
        let (len_a, len_b) = (group_a.len(), group_b.len());
        if len_a == 0 || len_b == 0 {
            break;
        }
        let ratio = len_a.max(len_b) as f32 / len_a.min(len_b) as f32;
        if ratio <= cfg.max_cluster_size_ratio {
            break;
        }

        let a_is_larger = len_a >= len_b;
        let larger_centroid = if a_is_larger {
            centroid_a.clone()
        } else {
            centroid_b.clone()
        };
        {
            let (larger, smaller) = if a_is_larger {
                (&mut group_a, &mut group_b)
            } else {
                (&mut group_b, &mut group_a)
            };
            // Furthest-from-own-centroid first.
            larger.sort_by(|&i, &j| {
                let di = cosine_distance(&larger_centroid, members[i].1);
                let dj = cosine_distance(&larger_centroid, members[j].1);
                dj.partial_cmp(&di).unwrap()
            });
            let diff = larger.len() as isize - smaller.len() as isize;
            // len_a/len_b > 0 and ratio > max_cluster_size_ratio >= 1.0 imply
            // larger.len() >= 2 here, so `larger.len() - 1` never underflows.
            let move_count = ((diff / 2).max(1) as usize).min(larger.len() - 1);
            let moved: Vec<usize> = larger.drain(0..move_count).collect();
            smaller.extend(moved);
        }

        centroid_a = mean_vector(group_a.iter().map(|&i| members[i].1), dim);
        centroid_b = mean_vector(group_b.iter().map(|&i| members[i].1), dim);
    }

    if group_a.is_empty() || group_b.is_empty() {
        // Should not happen given the invariants above, but never return an
        // empty half — fall back rather than risk a caller mishandling it.
        return index_order_split(members, dim);
    }

    BisectResult {
        left_ids: group_a.iter().map(|&i| members[i].0).collect(),
        right_ids: group_b.iter().map(|&i| members[i].0).collect(),
        left_centroid: centroid_a,
        right_centroid: centroid_b,
    }
}

fn index_order_split(members: &[(u32, &[f32])], dim: usize) -> BisectResult {
    let n = members.len();
    let mid = n / 2;
    BisectResult {
        left_ids: members[..mid].iter().map(|(id, _)| *id).collect(),
        right_ids: members[mid..].iter().map(|(id, _)| *id).collect(),
        left_centroid: mean_vector(members[..mid].iter().map(|(_, v)| *v), dim),
        right_centroid: mean_vector(members[mid..].iter().map(|(_, v)| *v), dim),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ClusterIndexConfig {
        ClusterIndexConfig::new(2)
    }

    #[test]
    fn splits_two_well_separated_clusters_cleanly() {
        let left_pts: Vec<Vec<f32>> = vec![vec![1.0, 0.0], vec![0.99, 0.01], vec![0.98, -0.02]];
        let right_pts: Vec<Vec<f32>> = vec![vec![-1.0, 0.0], vec![-0.99, 0.02], vec![-0.97, -0.01]];
        let members: Vec<(u32, &[f32])> = left_pts
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u32, v.as_slice()))
            .chain(
                right_pts
                    .iter()
                    .enumerate()
                    .map(|(i, v)| ((i + 10) as u32, v.as_slice())),
            )
            .collect();
        let mut rng = DeterministicRng::new(42);
        let result = balanced_bisect(&members, 2, &cfg(), &mut rng);

        assert_eq!(result.left_ids.len() + result.right_ids.len(), 6);
        // Each original group must land entirely on one side (well-separated
        // clusters should never get split across the bisection).
        let ids_below_10: Vec<u32> = (0..3).collect();
        let all_left_or_all_right = ids_below_10.iter().all(|id| result.left_ids.contains(id))
            || ids_below_10.iter().all(|id| result.right_ids.contains(id));
        assert!(
            all_left_or_all_right,
            "left cluster got split across the bisection"
        );
    }

    #[test]
    fn degenerate_identical_vectors_still_splits_without_empty_half() {
        let pts: Vec<Vec<f32>> = (0..6).map(|_| vec![1.0, 1.0]).collect();
        let members: Vec<(u32, &[f32])> = pts
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u32, v.as_slice()))
            .collect();
        let mut rng = DeterministicRng::new(7);
        let result = balanced_bisect(&members, 2, &cfg(), &mut rng);
        assert!(!result.left_ids.is_empty());
        assert!(!result.right_ids.is_empty());
        assert_eq!(result.left_ids.len() + result.right_ids.len(), 6);
    }

    #[test]
    fn single_member_returns_one_sided_result() {
        let v = vec![1.0, 2.0];
        let members: Vec<(u32, &[f32])> = vec![(0, v.as_slice())];
        let mut rng = DeterministicRng::new(1);
        let result = balanced_bisect(&members, 2, &cfg(), &mut rng);
        assert_eq!(result.left_ids, vec![0]);
        assert!(result.right_ids.is_empty());
    }

    #[test]
    fn rebalance_keeps_skewed_split_within_ratio() {
        // 1 far outlier vs. 19 tightly clustered points: an unconstrained
        // 2-means would likely produce a 1-vs-19 split; the rebalance pass
        // must pull it back toward the configured ratio.
        let mut pts: Vec<Vec<f32>> = vec![vec![-1.0, 0.0]];
        for i in 0..19 {
            let jitter = i as f32 * 0.001;
            pts.push(vec![1.0, jitter]);
        }
        let members: Vec<(u32, &[f32])> = pts
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u32, v.as_slice()))
            .collect();
        let mut c = cfg();
        c.max_cluster_size_ratio = 2.0;
        let mut rng = DeterministicRng::new(3);
        let result = balanced_bisect(&members, 2, &c, &mut rng);
        let (a, b) = (result.left_ids.len(), result.right_ids.len());
        let ratio = a.max(b) as f32 / a.min(b).max(1) as f32;
        assert!(
            ratio <= c.max_cluster_size_ratio + 1e-3,
            "ratio {ratio} exceeds configured bound"
        );
    }
}
