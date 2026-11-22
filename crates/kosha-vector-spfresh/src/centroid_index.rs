//! A small coarse index over posting centroids, for "which postings are
//! nearest to this vector" lookups that would otherwise scan every
//! centroid (#253).
//!
//! The postings' centroids are grouped into about `sqrt(P)` coarse cells
//! by a few Lloyd rounds over the (unit-normalized) centroids themselves.
//! A lookup ranks the cells, gathers the postings of the nearest
//! [`probe_cells`] cells and ranks those exactly, so it costs about
//! `sqrt(P) + probe_cells * sqrt(P)` centroid comparisons instead of `P`.
//! Below [`EXACT_BELOW`] postings there is no coarse level and a lookup is
//! an exact scan.
//!
//! The index is a snapshot: it is built for one batch of lookups against
//! centroids that do not change during the batch (absorbing postings,
//! one refinement round of a build) and then dropped. LIRE's per-event
//! lookups (Insert, Split/Merge/Reassign) still scan, because every event
//! moves centroids.
//!
//! Centroids are stored unit-normalized, so ranking is a single dot
//! product per centroid: for a fixed query `q`, `dot(q, c / |c|)` orders
//! centroids exactly as cosine distance does (a zero centroid scores 0,
//! cosine distance 1 — "maximally distant" in both).

use crate::point::{dot, normalize_in_place};
use crate::posting::PostingId;

/// Below this many postings a lookup scans every centroid.
pub(crate) const EXACT_BELOW: usize = 256;

/// Lloyd rounds over the centroids when building the coarse level.
const COARSE_ITERS: usize = 4;

/// How many coarse cells a lookup probes for `cells` cells: a quarter of
/// them, at least 6. At P = 2,000 postings (about 200k vectors) that is 12
/// of 45 cells, ~600 centroids compared instead of 2,000.
fn probe_cells(cells: usize) -> usize {
    (cells / 4).max(6).min(cells)
}

#[derive(Debug)]
pub(crate) struct CentroidIndex {
    /// Posting id of each indexed centroid, by position.
    ids: Vec<PostingId>,
    /// Unit-normalized centroid of each position.
    units: Vec<Vec<f32>>,
    /// Positions in each coarse cell; empty when the index is exact.
    cells: Vec<Vec<u32>>,
    /// Unit-normalized centroid of each coarse cell.
    cell_units: Vec<Vec<f32>>,
}

impl CentroidIndex {
    /// Indexes `(posting id, centroid)` pairs. Deterministic: same input
    /// order, same index.
    pub(crate) fn build<'a>(centroids: impl Iterator<Item = (PostingId, &'a [f32])>) -> Self {
        let mut ids = Vec::new();
        let mut units = Vec::new();
        for (pid, c) in centroids {
            let mut u = c.to_vec();
            normalize_in_place(&mut u);
            ids.push(pid);
            units.push(u);
        }
        let mut index = Self {
            ids,
            units,
            cells: Vec::new(),
            cell_units: Vec::new(),
        };
        if index.units.len() >= EXACT_BELOW {
            index.build_coarse();
        }
        index
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.ids.len()
    }

    /// Whether lookups scan every centroid (no coarse level).
    pub(crate) fn is_exact(&self) -> bool {
        self.cells.is_empty()
    }

    fn build_coarse(&mut self) {
        let p = self.units.len();
        let k = (p as f64).sqrt().ceil() as usize;
        // Seeds at an even stride through the input order; build and
        // `ClusterIndex` slot order both follow the bisection tree, so a
        // stride is spread across the space.
        let mut cell_units: Vec<Vec<f32>> = (0..k).map(|i| self.units[i * p / k].clone()).collect();
        let dim = self.units[0].len();
        let mut assign = vec![0u32; p];
        for _ in 0..COARSE_ITERS {
            for (i, u) in self.units.iter().enumerate() {
                assign[i] = nearest_of(&cell_units, u) as u32;
            }
            let mut sums = vec![vec![0.0f32; dim]; k];
            let mut counts = vec![0usize; k];
            for (i, u) in self.units.iter().enumerate() {
                let c = assign[i] as usize;
                counts[c] += 1;
                for (s, x) in sums[c].iter_mut().zip(u) {
                    *s += x;
                }
            }
            for (c, mut sum) in sums.into_iter().enumerate() {
                // An empty cell keeps its previous centre.
                if counts[c] > 0 {
                    normalize_in_place(&mut sum);
                    cell_units[c] = sum;
                }
            }
        }
        for (i, u) in self.units.iter().enumerate() {
            assign[i] = nearest_of(&cell_units, u) as u32;
        }
        let mut cells = vec![Vec::new(); k];
        for (i, &c) in assign.iter().enumerate() {
            cells[c as usize].push(i as u32);
        }
        // Drop empty cells so a probe never spends a slot on one.
        let (cells, cell_units): (Vec<_>, Vec<_>) = cells
            .into_iter()
            .zip(cell_units)
            .filter(|(members, _)| !members.is_empty())
            .unzip();
        self.cells = cells;
        self.cell_units = cell_units;
    }

    /// Up to `limit` posting ids nearest to `query` by cosine distance,
    /// nearest first. Exact below [`EXACT_BELOW`] postings; otherwise the
    /// postings of the nearest coarse cells (at least [`probe_cells`], and
    /// more until `limit` candidates are found).
    pub(crate) fn nearest(&self, query: &[f32], limit: usize) -> Vec<PostingId> {
        if limit == 0 || self.ids.is_empty() {
            return Vec::new();
        }
        let scored: Vec<(u32, f32)> = if self.is_exact() {
            (0..self.units.len() as u32)
                .map(|i| (i, dot(query, &self.units[i as usize])))
                .collect()
        } else {
            let mut cells: Vec<(usize, f32)> = self
                .cell_units
                .iter()
                .enumerate()
                .map(|(c, u)| (c, dot(query, u)))
                .collect();
            cells.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            let min_cells = probe_cells(self.cells.len());
            let mut scored = Vec::new();
            for (probed, (c, _)) in cells.into_iter().enumerate() {
                if probed >= min_cells && scored.len() >= limit {
                    break;
                }
                for &i in &self.cells[c] {
                    scored.push((i, dot(query, &self.units[i as usize])));
                }
            }
            scored
        };
        top_by_score(scored, limit)
            .into_iter()
            .map(|i| self.ids[i as usize])
            .collect()
    }
}

/// Position of the highest-scoring (nearest) centre; lowest index on ties.
fn nearest_of(centres: &[Vec<f32>], u: &[f32]) -> usize {
    let mut best = 0;
    let mut best_score = f32::NEG_INFINITY;
    for (c, cu) in centres.iter().enumerate() {
        let s = dot(u, cu);
        if s > best_score {
            best_score = s;
            best = c;
        }
    }
    best
}

/// The `limit` highest scores, descending; ties broken by position so the
/// result does not depend on the scan order.
fn top_by_score(mut scored: Vec<(u32, f32)>, limit: usize) -> Vec<u32> {
    let n = limit.min(scored.len());
    if n == 0 {
        return Vec::new();
    }
    let cmp = |a: &(u32, f32), b: &(u32, f32)| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0));
    if n < scored.len() {
        scored.select_nth_unstable_by(n - 1, cmp);
        scored.truncate(n);
    }
    scored.sort_by(cmp);
    scored.into_iter().map(|(i, _)| i).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::point::cosine_distance;
    use crate::rng::DeterministicRng;

    fn random_centroids(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = DeterministicRng::new(seed);
        // A mixture, so centroids cluster the way real posting centroids do.
        let centres: Vec<Vec<f32>> = (0..40)
            .map(|_| (0..dim).map(|_| rng.next_f32_range(-1.0, 1.0)).collect())
            .collect();
        (0..n)
            .map(|_| {
                let c = &centres[rng.next_usize(centres.len())];
                c.iter()
                    .map(|x| x + rng.next_f32_range(-0.5, 0.5))
                    .collect()
            })
            .collect()
    }

    fn exact(centroids: &[Vec<f32>], q: &[f32], limit: usize) -> Vec<usize> {
        let mut d: Vec<(usize, f32)> = centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, cosine_distance(q, c)))
            .collect();
        d.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        d.into_iter().take(limit).map(|(i, _)| i).collect()
    }

    #[test]
    fn small_index_is_an_exact_scan() {
        let cs = random_centroids(EXACT_BELOW - 1, 16, 1);
        let idx = CentroidIndex::build(cs.iter().enumerate().map(|(i, c)| (i, c.as_slice())));
        assert!(idx.is_exact());
        let qs = random_centroids(50, 16, 2);
        for q in &qs {
            let got = idx.nearest(q, 8);
            let want = exact(&cs, q, 8);
            // Same set; order may differ only on float ties.
            let mut g = got.clone();
            let mut w = want.clone();
            g.sort_unstable();
            w.sort_unstable();
            assert_eq!(g, w);
            assert_eq!(got[0], want[0]);
        }
    }

    #[test]
    fn coarse_lookup_finds_the_exact_nearest_almost_always() {
        let cs = random_centroids(2_000, 32, 3);
        let idx = CentroidIndex::build(cs.iter().enumerate().map(|(i, c)| (i, c.as_slice())));
        assert!(!idx.is_exact());
        assert_eq!(idx.len(), 2_000);
        let qs = random_centroids(500, 32, 4);
        let (mut top1, mut top8) = (0usize, 0usize);
        for q in &qs {
            let got = idx.nearest(q, 8);
            assert_eq!(got.len(), 8);
            let want = exact(&cs, q, 8);
            top1 += usize::from(got[0] == want[0]);
            top8 += got.iter().filter(|g| want.contains(g)).count();
        }
        let top1 = top1 as f64 / qs.len() as f64;
        let top8 = top8 as f64 / (qs.len() * 8) as f64;
        assert!(top1 >= 0.97, "nearest-posting agreement {top1:.3}");
        assert!(top8 >= 0.95, "top-8 overlap {top8:.3}");
    }

    #[test]
    fn returns_at_most_the_indexed_count_and_skips_ids_correctly() {
        let cs = random_centroids(300, 8, 5);
        // Sparse posting ids (slab slots with holes).
        let idx = CentroidIndex::build(
            cs.iter()
                .enumerate()
                .map(|(i, c)| (i * 3 + 1, c.as_slice())),
        );
        let got = idx.nearest(&cs[7], 400);
        assert_eq!(got.len(), 300);
        assert_eq!(got[0], 7 * 3 + 1);
        assert!(idx.nearest(&cs[0], 0).is_empty());
        let empty = CentroidIndex::build(std::iter::empty());
        assert!(empty.nearest(&cs[0], 3).is_empty());
    }
}
