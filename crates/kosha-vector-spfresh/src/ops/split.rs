//! LIRE's Split operation (paper §3.3): garbage-collect a posting's
//! tombstones, and if it's still oversized, balanced-bisect it into two new
//! postings — then Reassign to fix any NPA violations the new centroids
//! created in the neighborhood (Figure 4 in the paper).

use crate::kmeans::balanced_bisect;
use crate::ops::reassign::run_reassign;
use crate::posting::{Posting, PostingEntry, PostingId};
use crate::ClusterIndex;

/// Guards against a genuine implementation bug turning a bounded cascade
/// (the paper proves split→reassign chains terminate in finite steps, since
/// posting count is monotonically bounded above by total vector count) into
/// an infinite loop. In practice the paper observes cascades of depth ~2;
/// 64 is a defensive margin, not a realistic depth.
const MAX_CASCADE_DEPTH: usize = 64;

pub(crate) fn split_posting(index: &mut ClusterIndex, posting_id: PostingId, depth: usize) {
    assert!(
        depth < MAX_CASCADE_DEPTH,
        "LIRE split cascade exceeded MAX_CASCADE_DEPTH ({MAX_CASCADE_DEPTH}) — this should be \
         impossible per the paper's convergence proof (posting count is bounded above by total \
         vector count), so this indicates an implementation bug rather than pathological input"
    );

    let (old_centroid, live_count) = {
        let posting = index.postings[posting_id]
            .as_mut()
            .expect("posting_id must be active");
        posting.gc();
        (posting.centroid.clone(), posting.entries.len())
    };

    if live_count <= index.config.max_posting_size {
        // GC alone resolved the overflow — no split, no reassign needed.
        return;
    }

    let members: Vec<(u32, &[f32])> = {
        let posting = index.postings[posting_id].as_ref().unwrap();
        posting
            .entries
            .iter()
            .map(|e| (e.id, e.vector.as_slice()))
            .collect()
    };
    let result = balanced_bisect(&members, index.config.dim, &index.config, &mut index.rng);
    if result.left_ids.is_empty() || result.right_ids.is_empty() {
        // Degenerate (e.g. every remaining vector numerically identical) —
        // leave the posting oversized rather than create an empty one.
        return;
    }

    let entries_by_id = |posting: &Posting, ids: &[u32]| -> Vec<PostingEntry> {
        ids.iter()
            .map(|id| {
                posting
                    .entries
                    .iter()
                    .find(|e| e.id == *id)
                    .expect("id came from this posting")
                    .clone()
            })
            .collect()
    };

    let (left_entries, right_entries) = {
        let posting = index.postings[posting_id].as_ref().unwrap();
        (
            entries_by_id(posting, &result.left_ids),
            entries_by_id(posting, &result.right_ids),
        )
    };

    let right_id = index.alloc_slot(Posting {
        centroid: result.right_centroid.clone(),
        entries: right_entries,
    });
    for id in &result.right_ids {
        index.owner_map.insert(*id, right_id);
    }

    // Reuse posting_id for the left half — keeps the paper's "split deletes
    // one centroid, adds two" bookkeeping literal (right_id is the +1 slot).
    index.postings[posting_id] = Some(Posting {
        centroid: result.left_centroid.clone(),
        entries: left_entries,
    });
    for id in &result.left_ids {
        index.owner_map.insert(*id, posting_id);
    }

    run_reassign(index, &[old_centroid], &[posting_id, right_id], depth);
}
