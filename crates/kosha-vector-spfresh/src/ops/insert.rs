//! LIRE's Insert operation: append to the nearest posting, following NPA;
//! trigger a Split if that pushes the posting over `max_posting_size`.

use crate::centroid_probe::probe;
use crate::error::VectorIndexError;
use crate::ops::split::split_posting;
use crate::posting::{Posting, PostingEntry};
use crate::ClusterIndex;

pub(crate) fn insert(
    index: &mut ClusterIndex,
    id: u32,
    vector: Vec<f32>,
) -> Result<(), VectorIndexError> {
    if vector.len() != index.config.dim {
        return Err(VectorIndexError::DimensionMismatch {
            expected: index.config.dim,
            got: vector.len(),
        });
    }
    if index.owner_map.contains_key(&id) {
        return Err(VectorIndexError::DuplicateId(id));
    }

    let target = if index.active_posting_count() == 0 {
        // First vector in an empty index: seed a posting with it as its own
        // centroid.
        index.alloc_slot(Posting {
            centroid: vector.clone(),
            entries: Vec::new(),
        })
    } else {
        probe(&index.postings, &vector, 1)[0]
    };

    index.postings[target]
        .as_mut()
        .expect("target came from probe/alloc_slot, must be active")
        .entries
        .push(PostingEntry {
            id,
            vector,
            deleted: false,
            version: 0,
        });
    index.owner_map.insert(id, target);

    if index.config.enable_split {
        let live = index.postings[target].as_ref().unwrap().live_count();
        if live > index.config.max_posting_size {
            split_posting(index, target, 0);
        }
    }

    Ok(())
}
