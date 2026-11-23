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

/// Shared ranking core for both `probe` (internal, over live postings) and
/// `nearest_centroids` (external, over bare centroid vectors) — takes an
/// iterator so neither caller needs to allocate an intermediate copy of the
/// centroids just to share this logic.
fn rank_by_distance<'a>(
    candidates: impl Iterator<Item = (usize, &'a [f32])>,
    query: &[f32],
    limit: usize,
) -> Vec<usize> {
    rank_by(candidates, limit, |c| cosine_distance(query, c))
}

/// Ranking core parameterized on the distance function (ascending — smaller
/// is nearer), so the unit-norm dot path shares the same select/sort logic.
fn rank_by<'a>(
    candidates: impl Iterator<Item = (usize, &'a [f32])>,
    limit: usize,
    distance: impl Fn(&[f32]) -> f32,
) -> Vec<usize> {
    if limit == 0 {
        return Vec::new();
    }
    let mut scored: Vec<(usize, f32)> = candidates.map(|(id, c)| (id, distance(c))).collect();
    let n = limit.min(scored.len());
    if n == 0 {
        return Vec::new();
    }
    scored.select_nth_unstable_by(n - 1, |a, b| a.1.partial_cmp(&b.1).unwrap());
    scored.truncate(n);
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    scored.into_iter().map(|(id, _)| id).collect()
}

/// Returns up to `nprobe` active postings nearest to `query`, ascending by
/// distance.
pub(crate) fn probe(postings: &[Option<Posting>], query: &[f32], nprobe: usize) -> Vec<PostingId> {
    rank_by_distance(
        postings
            .iter()
            .enumerate()
            .filter_map(|(id, p)| p.as_ref().map(|p| (id, p.centroid.as_slice()))),
        query,
        nprobe,
    )
}

/// Returns up to `nprobe` indices into `centroids`, ascending by distance to
/// `query`. Standalone and decoupled from `ClusterIndex`'s internal posting
/// slab (`Posting`/`PostingId` are crate-private) — for external callers
/// that only have plain centroid vectors and never hold a real
/// `ClusterIndex`, e.g. `kosha-segment`'s lazy on-disk reader, which loads
/// posting centroids from a sidecar file without reconstructing the full
/// index. Shares its ranking logic with `probe` via `rank_by_distance`
/// rather than duplicating it.
pub fn nearest_centroids(centroids: &[Vec<f32>], query: &[f32], nprobe: usize) -> Vec<usize> {
    rank_by_distance(
        centroids.iter().enumerate().map(|(i, c)| (i, c.as_slice())),
        query,
        nprobe,
    )
}

/// [`nearest_centroids`] for the unit-norm fast path: when `query` and every
/// centroid are unit-L2, `1 - dot` *is* the cosine distance, so ranking by
/// negated dot gives the identical order without recomputing norms per
/// comparison. Callers are responsible for the unit-norm precondition (in
/// kosha-segment it's carried by the vector-index format version).
pub fn nearest_centroids_dot(centroids: &[Vec<f32>], query: &[f32], nprobe: usize) -> Vec<usize> {
    rank_by(
        centroids.iter().enumerate().map(|(i, c)| (i, c.as_slice())),
        nprobe,
        |c| -crate::point::dot(query, c),
    )
}

/// The rows of a row-major centroid matrix: `rows.len() / dim` slices of
/// `dim` floats each. `dim == 0` has no rows.
fn matrix_rows(rows: &[f32], dim: usize) -> impl Iterator<Item = (usize, &[f32])> {
    // `chunks_exact(0)` panics; a zero-dimension matrix holds nothing.
    rows.chunks_exact(dim.max(1))
        .take(if dim == 0 { 0 } else { usize::MAX })
        .enumerate()
}

/// [`nearest_centroids`] over one flat row-major matrix (`dim` floats per
/// centroid) instead of a `Vec` per centroid, so the scan reads memory
/// sequentially and a caller that holds its centroids flat does not
/// rebuild a `Vec<Vec<f32>>` per query. Same order, same ties.
pub fn nearest_centroids_rows(
    rows: &[f32],
    dim: usize,
    query: &[f32],
    nprobe: usize,
) -> Vec<usize> {
    rank_by_distance(matrix_rows(rows, dim), query, nprobe)
}

/// [`nearest_centroids_dot`] over one flat row-major matrix; see
/// [`nearest_centroids_rows`].
pub fn nearest_centroids_dot_rows(
    rows: &[f32],
    dim: usize,
    query: &[f32],
    nprobe: usize,
) -> Vec<usize> {
    rank_by(matrix_rows(rows, dim), nprobe, |c| {
        -crate::point::dot(query, c)
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_nearest_first_ascending_by_distance() {
        let centroids = vec![
            vec![1.0, 0.0],  // index 0: far from query
            vec![0.0, 1.0],  // index 1: exact match
            vec![-1.0, 0.0], // index 2: farthest
        ];
        let query = vec![0.0, 1.0];
        let result = nearest_centroids(&centroids, &query, 3);
        assert_eq!(result[0], 1, "exact match must rank first");
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn nprobe_caps_result_length() {
        let centroids = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![-1.0, 0.0]];
        let result = nearest_centroids(&centroids, &[0.0, 1.0], 2);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn row_matrix_variants_rank_exactly_as_the_vec_of_vecs_ones() {
        // Includes exact ties (duplicated rows), which must break the
        // same way in both layouts.
        let mut rng = crate::rng::DeterministicRng::new(7);
        let dim = 12;
        let mut centroids: Vec<Vec<f32>> = (0..97)
            .map(|_| {
                let mut v: Vec<f32> = (0..dim).map(|_| rng.next_f32_range(-0.5, 0.5)).collect();
                crate::point::normalize_in_place(&mut v);
                v
            })
            .collect();
        centroids.push(centroids[3].clone());
        centroids.push(centroids[40].clone());
        let flat: Vec<f32> = centroids.iter().flatten().copied().collect();
        for q in 0..20 {
            let mut query: Vec<f32> = (0..dim).map(|_| rng.next_f32_range(-0.5, 0.5)).collect();
            crate::point::normalize_in_place(&mut query);
            for nprobe in [0, 1, 5, 50, centroids.len(), centroids.len() + 9] {
                assert_eq!(
                    nearest_centroids_rows(&flat, dim, &query, nprobe),
                    nearest_centroids(&centroids, &query, nprobe),
                    "cosine q{q} nprobe {nprobe}"
                );
                assert_eq!(
                    nearest_centroids_dot_rows(&flat, dim, &query, nprobe),
                    nearest_centroids_dot(&centroids, &query, nprobe),
                    "dot q{q} nprobe {nprobe}"
                );
            }
        }
        assert!(nearest_centroids_rows(&[], 0, &[], 3).is_empty());
        assert!(nearest_centroids_dot_rows(&[], 4, &[0.0; 4], 3).is_empty());
    }

    #[test]
    fn empty_centroids_returns_empty() {
        let centroids: Vec<Vec<f32>> = Vec::new();
        assert!(nearest_centroids(&centroids, &[0.0, 1.0], 5).is_empty());
    }

    #[test]
    fn zero_nprobe_returns_empty() {
        let centroids = vec![vec![1.0, 0.0]];
        assert!(nearest_centroids(&centroids, &[1.0, 0.0], 0).is_empty());
    }
}
