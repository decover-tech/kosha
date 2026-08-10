//! Query path: probe the `nprobe` nearest posting centroids, scan their live
//! entries, return the top-`k`.

use crate::centroid_probe::probe;
use crate::error::VectorIndexError;
use crate::point::cosine_distance;
use crate::ClusterIndex;

pub(crate) fn search(
    index: &ClusterIndex,
    query: &[f32],
    k: usize,
) -> Result<Vec<(u32, f32)>, VectorIndexError> {
    if query.len() != index.config.dim {
        return Err(VectorIndexError::DimensionMismatch {
            expected: index.config.dim,
            got: query.len(),
        });
    }
    if k == 0 {
        return Ok(Vec::new());
    }

    let candidate_postings = probe(&index.postings, query, index.config.nprobe);
    let mut scored: Vec<(u32, f32)> = Vec::new();
    for pid in candidate_postings {
        let posting = index.postings[pid]
            .as_ref()
            .expect("probe only returns active postings");
        for e in posting.live_entries() {
            scored.push((e.id, 1.0 - cosine_distance(query, &e.vector)));
        }
    }

    let n = k.min(scored.len());
    if n == 0 {
        return Ok(Vec::new());
    }
    // Descending by similarity.
    scored.select_nth_unstable_by(n - 1, |a, b| b.1.partial_cmp(&a.1).unwrap());
    scored.truncate(n);
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    Ok(scored)
}
