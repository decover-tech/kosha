//! LIRE's Reassign operation — the paper's central mechanism (§3.3, Eq. 1 /
//! Eq. 2). After a Split or Merge, only a *bounded* set of vectors can
//! possibly now violate NPA (nearest partition assignment): the members of
//! the touched posting(s) themselves, and the members of a small number of
//! neighboring postings. This is what makes rebalancing local and cheap
//! instead of a full rescan.
//!
//! One routine serves both callers: `ops::split` passes one old centroid and
//! two touched postings; `ops::merge` passes two old centroids and one.

use std::collections::HashSet;

use crate::centroid_probe::nearest_active_postings;
use crate::ops::split::split_posting;
use crate::point::{cosine_distance, dot, elkan_cutoff};
use crate::posting::PostingId;
use crate::ClusterIndex;

pub(crate) fn run_reassign(
    index: &mut ClusterIndex,
    old_centroids: &[Vec<f32>],
    touched: &[PostingId],
    depth: usize,
) {
    if !index.config.enable_reassign {
        return;
    }

    let active_count = index.active_posting_count();
    let radius = index
        .config
        .reassign_radius
        .min(active_count.saturating_sub(touched.len()));
    let touched_centroids: Vec<Vec<f32>> = touched
        .iter()
        .map(|&pid| {
            index.postings[pid]
                .as_ref()
                .expect("touched posting must be active")
                .centroid
                .clone()
        })
        .collect();
    let neighbors = nearest_active_postings(&index.postings, &touched_centroids, touched, radius);

    // The scope any candidate is allowed to move within. Re-verifying
    // within this scope, never a full index scan, keeps reassignment local.
    let mut scope: Vec<PostingId> = touched.to_vec();
    scope.extend(neighbors.iter().copied());

    // --- Gather candidates (read-only pass over the pre-move state) ---
    let mut candidates: Vec<(PostingId, u32)> = Vec::new();

    // Condition 1 (Eq. 1): members of a touched posting whose *old* centroid
    // is no worse than the new one they just landed under — a nearby
    // posting might now be strictly closer than either.
    for &pid in touched {
        let posting = index.postings[pid].as_ref().unwrap();
        let cutoff = reassign_cutoff(index, pid, &scope);
        for e in posting.live_entries() {
            let d_self = cosine_distance(&posting.centroid, &e.vector);
            if d_self < cutoff && dot(&e.vector, &e.vector).is_normal() {
                continue;
            }
            let d_old = old_centroids
                .iter()
                .map(|c| cosine_distance(c, &e.vector))
                .fold(f32::INFINITY, f32::min);
            if d_old <= d_self {
                candidates.push((pid, e.id));
            }
        }
    }

    // Condition 2 (Eq. 2): members of a neighboring, untouched posting for
    // whom a new/touched centroid is now closer than their own posting's.
    for &pid in &neighbors {
        let posting = index.postings[pid].as_ref().unwrap();
        // If no touched centroid can win, Condition 2 cannot nominate
        // this entry, regardless of the other neighbours in the scope.
        let cutoff = reassign_cutoff(index, pid, touched);
        for e in posting.live_entries() {
            let d_self = cosine_distance(&posting.centroid, &e.vector);
            if d_self < cutoff && dot(&e.vector, &e.vector).is_normal() {
                continue;
            }
            let d_touched = touched_centroids
                .iter()
                .map(|c| cosine_distance(c, &e.vector))
                .fold(f32::INFINITY, f32::min);
            if d_touched <= d_self {
                candidates.push((pid, e.id));
            }
        }
    }

    // --- Re-verify + move ---
    let mut grown: HashSet<PostingId> = HashSet::new();
    for (cur_pid, entry_id) in candidates {
        let Some(posting) = index.postings[cur_pid].as_ref() else {
            continue;
        };
        // Look up by id, not by a stashed index/reference: an earlier
        // iteration of this very loop may have mutated cur_pid's entries.
        let Some(entry) = posting
            .entries
            .iter()
            .find(|e| e.id == entry_id && !e.deleted)
        else {
            continue; // already moved (or a duplicate candidate) — no-op
        };
        let vector = entry.vector.clone();

        let mut best_pid = cur_pid;
        let mut best_dist = cosine_distance(&posting.centroid, &vector);
        for &pid in &scope {
            if pid == cur_pid {
                continue;
            }
            if let Some(p) = index.postings[pid].as_ref() {
                let d = cosine_distance(&p.centroid, &vector);
                if d < best_dist {
                    best_dist = d;
                    best_pid = pid;
                }
            }
        }

        if best_pid != cur_pid {
            if let Some(p) = index.postings[cur_pid].as_mut() {
                if let Some(e) = p
                    .entries
                    .iter_mut()
                    .find(|e| e.id == entry_id && !e.deleted)
                {
                    e.deleted = true;
                    e.version = e.version.wrapping_add(1);
                }
            }
            index.postings[best_pid]
                .as_mut()
                .unwrap()
                .entries
                .push(crate::posting::PostingEntry {
                    id: entry_id,
                    vector,
                    deleted: false,
                    version: 0,
                });
            index.owner_map.insert(entry_id, best_pid);
            grown.insert(best_pid);
        }
    }

    // --- Cascade: a posting that grew past max_posting_size from a
    // reassignment needs its own split. Bounded by MAX_CASCADE_DEPTH inside
    // split_posting (belt-and-suspenders on top of the paper's convergence
    // proof).
    // Inside `ClusterIndex::rebalance` its own loop does these splits.
    if index.config.enable_split && !index.defer_cascade_splits {
        // `grown` is a HashSet — std's default hasher is randomly seeded
        // per-process, so its iteration order (and hence the order splits
        // fire in, and hence how many `index.rng` draws precede each one)
        // would otherwise differ between runs on identical input. Sort for
        // the same-seed-same-index determinism `ClusterIndexConfig::seed`
        // promises.
        let mut grown: Vec<PostingId> = grown.into_iter().collect();
        grown.sort_unstable();
        for pid in grown {
            let oversized = index.postings[pid]
                .as_ref()
                .map(|p| p.live_count() > index.config.max_posting_size)
                .unwrap_or(false);
            if oversized {
                let _ = split_posting(index, pid, depth + 1);
            }
        }
    }
}

fn reassign_cutoff(index: &ClusterIndex, pid: PostingId, others: &[PostingId]) -> f32 {
    if !index.config.enable_elkan_pruning {
        return 0.0;
    }
    let own = &index.postings[pid].as_ref().unwrap().centroid;
    if !dot(own, own).is_normal() {
        return 0.0;
    }
    let mut nearest = 2.0f32;
    for &q in others {
        if q != pid {
            let other = &index.postings[q].as_ref().unwrap().centroid;
            if !dot(other, other).is_normal() {
                return 0.0;
            }
            nearest = nearest.min(cosine_distance(own, other));
        }
    }
    elkan_cutoff(nearest, index.config.dim)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterIndexConfig, PostingSnapshot};

    #[test]
    fn elkan_preserves_touched_and_neighbour_moves() {
        let fixture = || {
            vec![
                PostingSnapshot {
                    centroid: vec![1.0, 0.0],
                    entries: vec![(0, vec![1.0, 0.0]), (1, vec![0.0, 1.0])],
                    radius: 0.0,
                },
                PostingSnapshot {
                    centroid: vec![0.0, 1.0],
                    entries: vec![(2, vec![0.99, 0.01]), (3, vec![0.0, 1.0])],
                    radius: 0.0,
                },
            ]
        };
        for prune in [false, true] {
            let mut cfg = ClusterIndexConfig::new(2);
            cfg.enable_elkan_pruning = prune;
            let mut index = ClusterIndex::from_postings(fixture(), cfg).unwrap();
            if prune {
                assert!(reassign_cutoff(&index, 0, &[0, 1]) > 0.0);
                assert!(reassign_cutoff(&index, 1, &[0]) > 0.0);
            }
            run_reassign(&mut index, &[vec![0.0, 1.0]], &[0], 0);
            assert_eq!([0, 1, 2, 3].map(|id| index.owner_map[&id]), [0, 1, 0, 1]);
        }
    }
}
