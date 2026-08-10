//! Initial index construction: recursive balanced bisection until every leaf
//! posting is within `max_posting_size`. Reuses the exact same
//! `balanced_bisect` primitive the runtime `Split` operation uses — the
//! paper treats "split cascades during a rebuild" and "a runtime split" as
//! the same mechanism, and this implementation makes that literal.

use std::collections::{HashMap, HashSet};

use crate::error::VectorIndexError;
use crate::kmeans::balanced_bisect;
use crate::point::mean_vector;
use crate::posting::{Posting, PostingEntry};
use crate::rng::DeterministicRng;
use crate::{validate_config, ClusterIndex, ClusterIndexConfig};

pub(crate) fn build(
    vectors: &[(u32, Vec<f32>)],
    config: ClusterIndexConfig,
) -> Result<ClusterIndex, VectorIndexError> {
    validate_config(&config)?;

    if vectors.is_empty() {
        return Ok(ClusterIndex::empty(config));
    }

    for (_, v) in vectors {
        if v.len() != config.dim {
            return Err(VectorIndexError::DimensionMismatch {
                expected: config.dim,
                got: v.len(),
            });
        }
    }
    let mut seen = HashSet::with_capacity(vectors.len());
    for (id, _) in vectors {
        if !seen.insert(*id) {
            return Err(VectorIndexError::DuplicateId(*id));
        }
    }

    let mut rng = DeterministicRng::new(config.seed);
    let by_id: HashMap<u32, &Vec<f32>> = vectors.iter().map(|(id, v)| (*id, v)).collect();
    let mut postings: Vec<Posting> = Vec::new();

    let mut stack: Vec<Vec<u32>> = vec![vectors.iter().map(|(id, _)| *id).collect()];
    while let Some(group) = stack.pop() {
        if group.len() <= config.max_posting_size {
            postings.push(leaf_posting(&group, &by_id, config.dim));
            continue;
        }

        let members: Vec<(u32, &[f32])> =
            group.iter().map(|id| (*id, by_id[id].as_slice())).collect();
        let result = balanced_bisect(&members, config.dim, &config, &mut rng);
        if result.left_ids.is_empty() || result.right_ids.is_empty() {
            // Degenerate group (e.g. every member numerically identical) —
            // emit as one oversized leaf rather than looping forever trying
            // to bisect something that won't separate.
            postings.push(leaf_posting(&group, &by_id, config.dim));
            continue;
        }
        stack.push(result.left_ids);
        stack.push(result.right_ids);
    }

    let owner_map = postings
        .iter()
        .enumerate()
        .flat_map(|(pid, p)| p.entries.iter().map(move |e| (e.id, pid)))
        .collect();

    let mut index = ClusterIndex {
        config,
        postings: postings.into_iter().map(Some).collect(),
        free_slots: Vec::new(),
        owner_map,
        rng,
    };

    // The recursive bisection above only enforces the *upper* bound
    // (max_posting_size); it has no reason to keep every leaf above
    // min_posting_size (a group that was never split, or the smaller side
    // of a split accepted past its balance-ratio cap, can legitimately land
    // below it). Sweep those away here so a freshly built index already
    // satisfies its own configured invariants rather than relying on a
    // later delete() to happen to trigger the merge.
    if index.config.enable_merge {
        for pid in index.active_posting_ids().collect::<Vec<_>>() {
            let Some(p) = index.postings[pid].as_ref() else {
                continue;
            }; // merged away already
            if p.live_count() < index.config.min_posting_size {
                crate::ops::merge::merge_posting(&mut index, pid);
            }
        }
    }

    Ok(index)
}

fn leaf_posting(group: &[u32], by_id: &HashMap<u32, &Vec<f32>>, dim: usize) -> Posting {
    let centroid = mean_vector(group.iter().map(|id| by_id[id].as_slice()), dim);
    let entries = group
        .iter()
        .map(|id| PostingEntry {
            id: *id,
            vector: by_id[id].clone(),
            deleted: false,
            version: 0,
        })
        .collect();
    Posting { centroid, entries }
}
