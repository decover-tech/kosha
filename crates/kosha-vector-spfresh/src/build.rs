//! Initial index construction: recursive balanced bisection until every leaf
//! posting is within `max_posting_size`. Reuses the exact same
//! `balanced_bisect` primitive the runtime `Split` operation uses — the
//! paper treats "split cascades during a rebuild" and "a runtime split" as
//! the same mechanism, and this implementation makes that literal.
//!
//! Bisection's balance pass moves the larger half's *farthest* members into
//! the smaller half to meet `max_cluster_size_ratio`, so a leaf can hold
//! members far from its centroid: loose postings, large radii, and a
//! radius-aware probe (kosha-query ranks postings by `max(0, d - r)`) that
//! cannot rule them out. Unless `build_refine_rounds` is 0, the leaves are
//! then refined: each round moves every vector to the nearest centroid among
//! its posting's `build_refine_candidates` nearest postings (found with a
//! `CentroidIndex`, not a scan of all of them) and recomputes centroids;
//! in the first round a stray the candidates may not cover looks up its
//! own nearest posting,
//! and a final `ClusterIndex::rebalance` restores the size bounds with
//! LIRE's own Split/Merge/Reassign.

use std::collections::{HashMap, HashSet};

use crate::centroid_index::CentroidIndex;
use crate::error::VectorIndexError;
use crate::kmeans::balanced_bisect;
use crate::point::{dot, mean_vector, normalize_in_place};
use crate::posting::{Posting, PostingEntry, PostingId};
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

    let refine = config.build_refine_rounds > 0;
    if refine {
        refine_postings(&mut postings, &config);
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
        defer_cascade_splits: false,
    };

    if refine {
        // Refinement moved vectors without regard to size: Split what grew
        // past `max_posting_size`, Merge what fell under
        // `min_posting_size`, each with LIRE's bounded Reassign.
        index.rebalance();
        return Ok(index);
    }

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

/// How far beyond its posting's median best-candidate angle a vector must
/// be before refinement treats it as a possible stray and looks up its own
/// nearest posting. Lower catches more strays at more lookups: on dense,
/// tightly clustered data 1.0 adds ~30% build time at 100k vectors, 1.25
/// about 3%; on overlapping mixtures 1.25 keeps nearly all of the recall
/// gain of looking up every uncovered vector (see `compaction_cost_report`).
const STRAY_FACTOR: f32 = 1.25;

/// Bounded nearest-centroid refinement of freshly bisected leaves: up to
/// `build_refine_rounds` rounds, each moving every vector to the nearest
/// centroid among its posting's `build_refine_candidates` nearest postings
/// (its own included; ties stay put), then recomputing every centroid. In
/// the first round, a stray — far from every candidate, which the
/// candidates cannot be shown to cover (see the triangle-inequality test
/// below) — also looks up its own nearest posting through the
/// `CentroidIndex`.
/// Postings that end a round empty are dropped. Stops early once a round
/// moves nothing. Sizes are not bounded here; the caller rebalances.
fn refine_postings(postings: &mut Vec<Posting>, config: &ClusterIndexConfig) {
    let candidates = config.build_refine_candidates.max(1);
    for round in 0..config.build_refine_rounds {
        if postings.len() < 2 {
            return;
        }
        let units: Vec<Vec<f32>> = postings
            .iter()
            .map(|p| {
                let mut u = p.centroid.clone();
                normalize_in_place(&mut u);
                u
            })
            .collect();
        let centroids = CentroidIndex::build(
            postings
                .iter()
                .enumerate()
                .map(|(pid, p)| (pid, p.centroid.as_slice())),
        );
        let mut next: Vec<Vec<PostingEntry>> = (0..postings.len()).map(|_| Vec::new()).collect();
        let mut moved = 0usize;
        for (pid, posting) in postings.iter_mut().enumerate() {
            let near = centroids.nearest(&units[pid], candidates);
            // Every posting outside `near` is at least `reach` (an angle)
            // from this posting's centroid.
            let reach = match near.last() {
                Some(&far) if near.len() < units.len() => angle(dot(&units[pid], &units[far])),
                _ => f32::INFINITY,
            };
            // Pass 1: the best of the candidates, and its angle to `v`.
            let picks: Vec<(PostingId, f32, f32)> = posting
                .entries
                .iter()
                .map(|entry| {
                    let v = &entry.vector;
                    // For a fixed vector, `dot(v, unit centroid)` orders
                    // centroids exactly as cosine distance does.
                    let own_score = dot(v, &units[pid]);
                    let mut best = pid;
                    let mut best_score = own_score;
                    for &q in &near {
                        if q != pid {
                            let s = dot(v, &units[q]);
                            if s > best_score {
                                best_score = s;
                                best = q;
                            }
                        }
                    }
                    let norm = dot(v, v).sqrt().max(f32::MIN_POSITIVE);
                    (best, angle(best_score / norm), angle(own_score / norm))
                })
                .collect();
            // A stray (bisection's balance pass can push a member far from
            // its centroid, into an unrelated region) may belong to a
            // posting outside `near`. By the triangle inequality on
            // angles, any such posting is at least `reach - angle(v, own)`
            // from `v`, so only a vector whose best candidate is farther
            // than that can be misplaced. In a dense region that bound is
            // loose for many ordinary members; a stray is also far from
            // every candidate, so only vectors beyond `STRAY_FACTOR` times
            // the posting's median best-candidate angle look up their own
            // nearest posting. Strays come from bisection, so the first
            // round catches them; later rounds only move vectors between
            // neighbours.
            let stray_floor = if round == 0 {
                let mut a: Vec<f32> = picks.iter().map(|p| p.1).collect();
                let mid = a.len() / 2;
                a.select_nth_unstable_by(mid, f32::total_cmp);
                STRAY_FACTOR * a[mid]
            } else {
                f32::INFINITY
            };
            for (entry, (mut best, best_angle, own_angle)) in posting.entries.drain(..).zip(picks) {
                if best_angle > stray_floor && best_angle > reach - own_angle {
                    if let Some(&q) = centroids.nearest(&entry.vector, 1).first() {
                        if dot(&entry.vector, &units[q]) > dot(&entry.vector, &units[best]) {
                            best = q;
                        }
                    }
                }
                moved += usize::from(best != pid);
                next[best].push(entry);
            }
        }
        *postings = next
            .into_iter()
            .filter(|entries| !entries.is_empty())
            .map(|entries| Posting {
                centroid: mean_vector(entries.iter().map(|e| e.vector.as_slice()), config.dim),
                entries,
            })
            .collect();
        if moved == 0 {
            return;
        }
    }
}

/// The angle (radians) whose cosine is `cos`, clamped into range.
fn angle(cos: f32) -> f32 {
    cos.clamp(-1.0, 1.0).acos()
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
