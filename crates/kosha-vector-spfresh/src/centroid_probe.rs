//! Centroid probing: given a query vector (or a set of reference centroids,
//! for LIRE's reassignment radius), find the nearest active postings.
//!
//! A flat linear scan over centroids is deliberately not a graph-of-centroids
//! (SPANN uses SPTAG for this at billion-scale) — realistic posting counts
//! at kosha's segment scale top out around ~800 (50,000 vectors / a target
//! posting size of 64), so a linear scan + partial sort is cheap and simpler
//! to reason about. See README.md.

use crate::point::cosine_distance;
use crate::posting::{Posting, PostingId};

/// Returns up to `nprobe` active postings nearest to `query`, ascending by
/// distance.
pub(crate) fn probe(postings: &[Option<Posting>], query: &[f32], nprobe: usize) -> Vec<PostingId> {
    if nprobe == 0 {
        return Vec::new();
    }
    let mut scored: Vec<(PostingId, f32)> = postings
        .iter()
        .enumerate()
        .filter_map(|(id, p)| {
            p.as_ref()
                .map(|p| (id, cosine_distance(query, &p.centroid)))
        })
        .collect();
    let n = nprobe.min(scored.len());
    if n == 0 {
        return Vec::new();
    }
    scored.select_nth_unstable_by(n - 1, |a, b| a.1.partial_cmp(&b.1).unwrap());
    scored.truncate(n);
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    scored.into_iter().map(|(id, _)| id).collect()
}

/// The single nearest active posting to `centroid`, or `None` if there are
/// no active postings. Used by `ops::merge` to pick a merge target.
pub(crate) fn nearest_active_posting(
    postings: &[Option<Posting>],
    centroid: &[f32],
) -> Option<PostingId> {
    postings
        .iter()
        .enumerate()
        .filter_map(|(id, p)| {
            p.as_ref()
                .map(|p| (id, cosine_distance(centroid, &p.centroid)))
        })
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .map(|(id, _)| id)
}

/// The `limit` active postings (excluding `exclude`) nearest to *any* of
/// `reference_centroids` — LIRE's reassignment radius `R`: the bounded
/// neighborhood a split/merge event needs to re-check (Eq. 1/Eq. 2 in the
/// paper; see `ops/reassign.rs`).
pub(crate) fn nearest_active_postings(
    postings: &[Option<Posting>],
    reference_centroids: &[Vec<f32>],
    exclude: &[PostingId],
    limit: usize,
) -> Vec<PostingId> {
    if limit == 0 {
        return Vec::new();
    }
    let mut scored: Vec<(PostingId, f32)> = postings
        .iter()
        .enumerate()
        .filter(|(id, p)| p.is_some() && !exclude.contains(id))
        .map(|(id, p)| {
            let p = p.as_ref().unwrap();
            let d = reference_centroids
                .iter()
                .map(|c| cosine_distance(c, &p.centroid))
                .fold(f32::INFINITY, f32::min);
            (id, d)
        })
        .collect();
    let n = limit.min(scored.len());
    if n == 0 {
        return Vec::new();
    }
    scored.select_nth_unstable_by(n - 1, |a, b| a.1.partial_cmp(&b.1).unwrap());
    scored.truncate(n);
    scored.into_iter().map(|(id, _)| id).collect()
}
