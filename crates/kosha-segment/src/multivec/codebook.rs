//! The coarse codebook behind `multivec.{i}.ivf` (#205): ColBERTv2's
//! centroid layout (Santhanam et al., 2022, §3.3-3.4) instead of the
//! 16..128-vector postings the single-vector index maintains.
//!
//! A segment's tokens are clustered into about `16 * sqrt(n)` centroids,
//! rounded down to a power of two (2^18 for ~600M MS MARCO token vectors,
//! ~2,300 tokens per centroid), but never more than one per
//! [`MIN_MEAN_POSTING`] tokens, so a small segment keeps postings of about
//! that size. Posting length then grows as `sqrt(n) / 16` and the centroid
//! scan a query token pays as `16 * sqrt(n)`, instead of the centroid count
//! growing linearly with the token count (millions of centroids per
//! segment at ColBERT scale) as fixed-size postings require.
//!
//! The codebook is trained once, on a sample of [`SAMPLE_PER_CENTROID`]
//! tokens per centroid, and then frozen: every token is assigned to its
//! nearest centroid and nothing is split, merged or reassigned afterwards.
//! Training is two-level so it never compares every sampled token with
//! every centroid: spherical k-means into `sqrt(k)` coarse cells, then
//! within each cell k-means into a share of the `k` centroids proportional
//! to the cell's size. Assignment ranks the coarse cells and scans only the
//! centroids of the nearest [`PROBE_CELLS`], so it costs about
//! `(1 + PROBE_CELLS) * sqrt(k)` comparisons per token instead of `k`.

use kosha_vector_spfresh::{dot, normalize_in_place, DeterministicRng};
use rayon::prelude::*;

/// Centroids per `sqrt(token count)` (ColBERTv2's 16).
const CENTROIDS_PER_SQRT: f64 = 16.0;
/// A segment never gets more than one centroid per this many tokens.
pub(crate) const MIN_MEAN_POSTING: usize = 64;
/// Training tokens sampled per centroid (Faiss warns below 39).
const SAMPLE_PER_CENTROID: usize = 64;
/// Below this many centroids training and assignment are a single level.
const TWO_LEVEL_FROM: usize = 256;
/// Coarse cells whose centroids a token is compared with on assignment.
const PROBE_CELLS: usize = 8;
/// Lloyd iterations per k-means (stops early once assignments settle).
const LLOYD_ITERS: usize = 10;
const SEED: u64 = 0xC01B_E127_C0DE_B00C;

/// Centroid count for `n` tokens.
pub(crate) fn codebook_size(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let sqrt_rule = (CENTROIDS_PER_SQRT * (n as f64).sqrt()) as usize;
    let pow2 = 1usize << (usize::BITS - 1 - sqrt_rule.max(1).leading_zeros());
    pow2.min(n / MIN_MEAN_POSTING).max(1)
}

/// A frozen set of unit centroids.
pub(crate) struct Codebook {
    dim: usize,
    /// Unit centroids, flat (`dim` floats each).
    centroids: Vec<f32>,
    /// Unit coarse-cell centroids, flat; empty for a single-level codebook.
    cell_centroids: Vec<f32>,
    /// Positions in `centroids` of each coarse cell's centroids.
    cells: Vec<Vec<u32>>,
}

fn token(tokens: &[f32], dim: usize, id: u32) -> &[f32] {
    &tokens[id as usize * dim..(id as usize + 1) * dim]
}

/// Position of the centroid in `centroids` (flat, unit) nearest `t`.
fn nearest(centroids: &[f32], dim: usize, t: &[f32]) -> usize {
    let mut best = (0, f32::NEG_INFINITY);
    for (i, c) in centroids.chunks_exact(dim).enumerate() {
        let s = dot(t, c);
        if s > best.1 {
            best = (i, s);
        }
    }
    best.0
}

/// `k` distinct indices in `0..n` (`k <= n`), one from each of `k` equal
/// strata, so a sample spreads over the whole segment.
fn spread(n: usize, k: usize, rng: &mut DeterministicRng) -> Vec<usize> {
    (0..k)
        .map(|j| {
            let lo = j * n / k;
            let hi = (j + 1) * n / k;
            lo + rng.next_usize(hi - lo)
        })
        .collect()
}

/// Spherical k-means over `tokens[ids]` into at most `k` unit centroids
/// (flat). An emptied cluster is re-seeded with a random member; centroids
/// that own no token at the end (duplicates, when the tokens repeat) are
/// dropped.
fn spherical_kmeans(
    tokens: &[f32],
    dim: usize,
    ids: &[u32],
    k: usize,
    rng: &mut DeterministicRng,
    parallel: bool,
) -> Vec<f32> {
    let k = k.clamp(1, ids.len());
    let mut centroids = Vec::with_capacity(k * dim);
    for i in spread(ids.len(), k, rng) {
        centroids.extend_from_slice(token(tokens, dim, ids[i]));
    }
    let mut assign: Vec<u32> = Vec::new();
    for _ in 0..LLOYD_ITERS {
        let nearest_of = |&id: &u32| nearest(&centroids, dim, token(tokens, dim, id)) as u32;
        let next: Vec<u32> = if parallel {
            ids.par_iter().map(nearest_of).collect()
        } else {
            ids.iter().map(nearest_of).collect()
        };
        if next == assign {
            break;
        }
        assign = next;
        let mut sums = vec![0.0f32; k * dim];
        let mut counts = vec![0usize; k];
        for (&id, &c) in ids.iter().zip(&assign) {
            counts[c as usize] += 1;
            let sum = &mut sums[c as usize * dim..(c as usize + 1) * dim];
            for (s, x) in sum.iter_mut().zip(token(tokens, dim, id)) {
                *s += x;
            }
        }
        for (c, &count) in counts.iter().enumerate() {
            let sum = &mut sums[c * dim..(c + 1) * dim];
            if count == 0 {
                let id = ids[rng.next_usize(ids.len())];
                sum.copy_from_slice(token(tokens, dim, id));
            } else {
                normalize_in_place(sum);
            }
        }
        centroids = sums;
    }
    let nearest_of = |&id: &u32| nearest(&centroids, dim, token(tokens, dim, id));
    let owners: Vec<usize> = if parallel {
        ids.par_iter().map(nearest_of).collect()
    } else {
        ids.iter().map(nearest_of).collect()
    };
    let mut owned = vec![false; k];
    for c in owners {
        owned[c] = true;
    }
    centroids
        .chunks_exact(dim)
        .zip(owned)
        .filter(|(_, owned)| *owned)
        .flat_map(|(c, _)| c.iter().copied())
        .collect()
}

impl Codebook {
    /// Trains a codebook for `tokens` (unit, flat, `dim` floats each).
    /// Deterministic: the same tokens give the same codebook.
    pub(crate) fn train(tokens: &[f32], dim: usize) -> Self {
        let n = tokens.len() / dim;
        let k = codebook_size(n);
        let mut rng = DeterministicRng::new(SEED);
        let sample: Vec<u32> = if n <= k * SAMPLE_PER_CENTROID {
            (0..n as u32).collect()
        } else {
            spread(n, k * SAMPLE_PER_CENTROID, &mut rng)
                .into_iter()
                .map(|i| i as u32)
                .collect()
        };
        if k < TWO_LEVEL_FROM {
            let centroids = spherical_kmeans(tokens, dim, &sample, k, &mut rng, true);
            return Self {
                dim,
                centroids,
                cell_centroids: Vec::new(),
                cells: Vec::new(),
            };
        }

        let coarse = (k as f64).sqrt().ceil() as usize;
        let coarse = spherical_kmeans(tokens, dim, &sample, coarse, &mut rng, true);
        let owner: Vec<u32> = sample
            .par_iter()
            .map(|&id| nearest(&coarse, dim, token(tokens, dim, id)) as u32)
            .collect();
        let mut members: Vec<Vec<u32>> = vec![Vec::new(); coarse.len() / dim];
        for (&id, &cell) in sample.iter().zip(&owner) {
            members[cell as usize].push(id);
        }
        let seeds: Vec<u64> = members.iter().map(|_| rng.next_u64()).collect();
        let per_cell: Vec<Vec<f32>> = members
            .par_iter()
            .zip(&seeds)
            .map(|(ids, &seed)| {
                if ids.is_empty() {
                    return Vec::new();
                }
                let share = (k as f64 * ids.len() as f64 / sample.len() as f64).round() as usize;
                let mut rng = DeterministicRng::new(seed);
                spherical_kmeans(tokens, dim, ids, share, &mut rng, false)
            })
            .collect();

        let mut centroids = Vec::with_capacity(k * dim);
        let mut cell_centroids = Vec::with_capacity(coarse.len());
        let mut cells = Vec::new();
        for (cell, leaves) in per_cell.iter().enumerate() {
            if leaves.is_empty() {
                continue;
            }
            let first = (centroids.len() / dim) as u32;
            centroids.extend_from_slice(leaves);
            cells.push((first..(centroids.len() / dim) as u32).collect());
            cell_centroids.extend_from_slice(&coarse[cell * dim..(cell + 1) * dim]);
        }
        Self {
            dim,
            centroids,
            cell_centroids,
            cells,
        }
    }

    /// Unit centroids, flat.
    pub(crate) fn centroids(&self) -> &[f32] {
        &self.centroids
    }

    /// The (approximately) nearest centroid of every token in `tokens`.
    pub(crate) fn assign(&self, tokens: &[f32]) -> Vec<u32> {
        let dim = self.dim;
        if self.cells.is_empty() {
            return tokens
                .par_chunks_exact(dim)
                .map(|t| nearest(&self.centroids, dim, t) as u32)
                .collect();
        }
        let probe = PROBE_CELLS.min(self.cells.len());
        tokens
            .par_chunks_exact(dim)
            .map_init(Vec::new, |scored: &mut Vec<(usize, f32)>, t| {
                scored.clear();
                scored.extend(
                    self.cell_centroids
                        .chunks_exact(dim)
                        .map(|c| dot(t, c))
                        .enumerate(),
                );
                if probe < scored.len() {
                    scored.select_nth_unstable_by(probe - 1, |a, b| b.1.total_cmp(&a.1));
                }
                let mut best = (0u32, f32::NEG_INFINITY);
                for &(cell, _) in &scored[..probe] {
                    for &c in &self.cells[cell] {
                        let s = dot(t, &self.centroids[c as usize * dim..(c as usize + 1) * dim]);
                        if s > best.1 {
                            best = (c, s);
                        }
                    }
                }
                best.0
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_tokens_collapse_to_one_centroid() {
        let dim = 8;
        let n = TWO_LEVEL_FROM * MIN_MEAN_POSTING * 4;
        assert!(codebook_size(n) >= TWO_LEVEL_FROM);
        let mut t = vec![1.0f32; dim];
        normalize_in_place(&mut t);
        let tokens: Vec<f32> = t.iter().copied().cycle().take(n * dim).collect();
        let book = Codebook::train(&tokens, dim);
        assert_eq!(book.centroids().len(), dim);
        assert!(book.assign(&tokens).iter().all(|&c| c == 0));
    }

    #[test]
    fn size_follows_sixteen_sqrt_n_capped_by_posting_size() {
        assert_eq!(codebook_size(0), 0);
        assert_eq!(codebook_size(10), 1);
        assert_eq!(codebook_size(6_400), 100);
        // 16 * sqrt(1M) = 16,000 -> 2^13; 1M / 64 = 15,625.
        assert_eq!(codebook_size(1_000_000), 8_192);
        // The paper's MS MARCO figure: ~600M token vectors -> 2^18.
        assert_eq!(codebook_size(600_000_000), 1 << 18);
    }

    /// `n` unit tokens of `dim` around `topics` random centres.
    fn clustered(n: usize, dim: usize, topics: usize) -> Vec<f32> {
        let mut rng = DeterministicRng::new(7);
        let centres: Vec<Vec<f32>> = (0..topics)
            .map(|_| (0..dim).map(|_| rng.next_f32_range(-1.0, 1.0)).collect())
            .collect();
        let mut out = Vec::with_capacity(n * dim);
        for _ in 0..n {
            let c = &centres[rng.next_usize(topics)];
            let at = out.len();
            out.extend(c.iter().map(|x| x + rng.next_f32_range(-0.4, 0.4)));
            normalize_in_place(&mut out[at..]);
        }
        out
    }

    #[test]
    fn two_level_assignment_is_almost_always_the_exact_nearest() {
        let dim = 16;
        let tokens = clustered(40_000, dim, 50);
        let book = Codebook::train(&tokens, dim);
        assert!(!book.cells.is_empty(), "40k tokens train two levels");
        let k = book.centroids().len() / dim;
        assert!(k.abs_diff(codebook_size(40_000)) * 10 <= k, "k {k}");
        let got = book.assign(&tokens);
        let exact = tokens
            .chunks_exact(dim)
            .zip(&got)
            .filter(|(t, &c)| {
                let best = nearest(book.centroids(), dim, t);
                let sim = |i: usize| dot(t, &book.centroids()[i * dim..(i + 1) * dim]);
                best == c as usize || sim(best) - sim(c as usize) < 1e-6
            })
            .count();
        let rate = exact as f64 / got.len() as f64;
        assert!(rate >= 0.95, "exact-nearest rate {rate}");
    }

    #[test]
    fn training_is_deterministic_and_covers_every_token() {
        let dim = 8;
        let tokens = clustered(3_000, dim, 6);
        let a = Codebook::train(&tokens, dim);
        let b = Codebook::train(&tokens, dim);
        assert_eq!(a.centroids(), b.centroids());
        let assign = a.assign(&tokens);
        assert_eq!(assign.len(), 3_000);
        let k = a.centroids().len() / dim;
        assert_eq!(k, codebook_size(3_000));
        assert!(assign.iter().all(|&c| (c as usize) < k));
    }

    #[test]
    fn a_tiny_segment_gets_one_centroid() {
        let tokens = clustered(5, 4, 2);
        let book = Codebook::train(&tokens, 4);
        assert_eq!(book.centroids().len(), 4);
        assert_eq!(book.assign(&tokens), vec![0; 5]);
    }
}
