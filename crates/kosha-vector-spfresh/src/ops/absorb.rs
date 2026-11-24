//! Bulk absorption of another partition's postings (#253): how compaction
//! folds the postings of every input but the largest into the largest
//! one's, without re-clustering. Not one of the paper's five operations —
//! it is LIRE's Insert applied a posting at a time, with the Split it
//! would trigger deferred to one `ClusterIndex::rebalance` at the end.
//!
//! Two partitions of the same space overlap: posting `p` of the absorbed
//! partition covers roughly the region of a few of this index's postings.
//! Keeping both (concatenation) would split every query's neighbours
//! across both partitions' postings, and a fixed probe budget would then
//! reach fewer of them. So `p`'s members move into this index's postings:
//! `p`'s centroid picks `candidates` nearby postings (one lookup per
//! absorbed *posting*, not per vector), and each member goes to the
//! nearest of those.
//!
//! The lookup goes through a `CentroidIndex` built once per call over this
//! index's centroids, which do not change until the end of the call: a
//! coarse index (about `sqrt(P)` cells, a quarter of them probed) once
//! there are 256 postings or more, so absorbing many loose vectors costs
//! about `changed * P / 4` centroid comparisons instead of `changed * P`,
//! each a single dot product against a pre-normalized centroid rather than
//! a full cosine distance.

use std::collections::BTreeSet;

use crate::centroid_index::CentroidIndex;
use crate::error::VectorIndexError;
use crate::point::{cosine_distance, mean_vector};
use crate::posting::{Posting, PostingEntry, PostingId};
use crate::{ClusterIndex, PostingSnapshot};

pub(crate) fn absorb(
    index: &mut ClusterIndex,
    postings: Vec<PostingSnapshot>,
    candidates: usize,
) -> Result<usize, VectorIndexError> {
    let dim = index.config.dim;
    // Validate everything first: a failed call leaves the index untouched.
    for p in &postings {
        for (id, v) in &p.entries {
            if v.len() != dim {
                return Err(VectorIndexError::DimensionMismatch {
                    expected: dim,
                    got: v.len(),
                });
            }
            if index.owner_map.contains_key(id) {
                return Err(VectorIndexError::DuplicateId(*id));
            }
        }
        if !p.entries.is_empty() && p.centroid.len() != dim {
            return Err(VectorIndexError::DimensionMismatch {
                expected: dim,
                got: p.centroid.len(),
            });
        }
    }
    let mut seen = std::collections::HashSet::new();
    for p in &postings {
        for (id, _) in &p.entries {
            if !seen.insert(*id) {
                return Err(VectorIndexError::DuplicateId(*id));
            }
        }
    }

    let mut grown: BTreeSet<PostingId> = BTreeSet::new();
    let mut absorbed = 0;
    // Built on first use. An empty index takes its first posting whole
    // first, so the index then covers that posting.
    let mut centroids: Option<CentroidIndex> = None;
    for p in postings {
        if p.entries.is_empty() {
            continue;
        }
        if centroids.is_none() && index.active_posting_count() == 0 {
            // Nothing to absorb into: the posting comes over whole.
            let pid = index.alloc_slot(Posting {
                centroid: p.centroid,
                entries: Vec::new(),
            });
            for (id, vector) in p.entries {
                push(index, pid, id, vector);
                absorbed += 1;
            }
            continue;
        }
        let near =
            centroids
                .get_or_insert_with(|| {
                    CentroidIndex::build(
                        index.postings.iter().enumerate().filter_map(|(pid, p)| {
                            p.as_ref().map(|p| (pid, p.centroid.as_slice()))
                        }),
                    )
                })
                .nearest(&p.centroid, candidates.max(1));
        for (id, vector) in p.entries {
            let target = near
                .iter()
                .copied()
                .map(|pid| {
                    let c = &index.postings[pid].as_ref().unwrap().centroid;
                    (pid, cosine_distance(c, &vector))
                })
                .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
                .map(|(pid, _)| pid)
                .expect("a lookup over a non-empty index returns a posting");
            push(index, target, id, vector);
            grown.insert(target);
            absorbed += 1;
        }
    }

    // A posting that took members has a new mean. Recomputed once here,
    // after every absorbed posting picked its targets against the
    // centroids it was built against.
    for pid in grown {
        let p = index.postings[pid].as_mut().unwrap();
        p.centroid = mean_vector(p.live_entries().map(|e| e.vector.as_slice()), dim);
    }
    Ok(absorbed)
}

fn push(index: &mut ClusterIndex, pid: PostingId, id: u32, vector: Vec<f32>) {
    index.postings[pid]
        .as_mut()
        .unwrap()
        .entries
        .push(PostingEntry {
            id,
            vector,
            deleted: false,
            version: 0,
        });
    index.owner_map.insert(id, pid);
}
