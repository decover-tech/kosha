//! LIRE's Merge operation (paper §3.2): fold an undersized posting into its
//! nearest active neighbor, then Reassign to fix any NPA violations the
//! deleted centroid left behind.

use crate::centroid_probe::nearest_active_posting;
use crate::ops::reassign::run_reassign;
use crate::point::mean_vector;
use crate::posting::PostingId;
use crate::ClusterIndex;

pub(crate) fn merge_posting(index: &mut ClusterIndex, posting_id: PostingId) {
    if index.active_posting_count() <= 1 {
        return;
    }

    let mut this = index.postings[posting_id]
        .take()
        .expect("posting_id must be active");
    this.gc();
    let this_centroid = this.centroid;
    let this_entries = this.entries;

    // `posting_id`'s slot is now `None`, so it's naturally excluded from the
    // neighbor search without needing an explicit exclude list.
    let target_id = nearest_active_posting(&index.postings, &this_centroid)
        .expect("active_posting_count() > 1 was checked above");
    let old_target_centroid = index.postings[target_id].as_ref().unwrap().centroid.clone();

    for e in this_entries {
        index.owner_map.insert(e.id, target_id);
        index.postings[target_id].as_mut().unwrap().entries.push(e);
    }

    // Recompute the target's centroid as a fresh mean over its current live
    // members — not a weighted-old-centroid shortcut, since deletions since
    // the target's last rebuild may have already drifted its stored
    // centroid away from the true member mean.
    let mean = mean_vector(
        index.postings[target_id]
            .as_ref()
            .unwrap()
            .live_entries()
            .map(|e| e.vector.as_slice()),
        index.config.dim,
    );
    index.postings[target_id].as_mut().unwrap().centroid = mean;

    index.free_slot(posting_id);

    // A merge is always a fresh top-level trigger (like an insert
    // overflowing a posting), not itself a cascade step — depth 0.
    run_reassign(
        index,
        &[this_centroid, old_target_centroid],
        &[target_id],
        0,
    );
}
