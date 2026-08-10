//! LIRE's Delete operation: tombstone in place (version bump), don't
//! physically remove — physical removal happens lazily at the next GC point
//! (a Split, or an explicit `ClusterIndex::rebalance`). Trigger a Merge if
//! this drops the posting below `min_posting_size`.

use crate::ops::merge::merge_posting;
use crate::ClusterIndex;

pub(crate) fn delete(index: &mut ClusterIndex, id: u32) -> bool {
    let Some(pid) = index.owner_map.remove(&id) else {
        return false;
    };
    let Some(posting) = index.postings[pid].as_mut() else {
        // Should not happen given the owner_map invariant, but don't panic
        // on a bookkeeping bug — just report "not found".
        return false;
    };
    let Some(entry) = posting
        .entries
        .iter_mut()
        .find(|e| e.id == id && !e.deleted)
    else {
        return false;
    };
    entry.deleted = true;
    entry.version = entry.version.wrapping_add(1);

    if index.config.enable_merge {
        let live = index.postings[pid].as_ref().unwrap().live_count();
        if live < index.config.min_posting_size && index.active_posting_count() > 1 {
            merge_posting(index, pid);
        }
    }

    true
}
