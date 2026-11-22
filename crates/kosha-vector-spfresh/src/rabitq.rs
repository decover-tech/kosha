//! RaBitQ 1-bit vector codes (Gao & Long, SIGMOD 2024).
//!
//! A posting entry `o` (unit-norm, like every stored vector) is coded
//! relative to its posting's centroid `c`:
//!
//! * residual `r = o - c`, its norm `‖r‖`, and its direction `o' = r / ‖r‖`;
//! * a fixed random rotation `R` (see [`RaBitQRotation`]) maps `o'` to `y`;
//!   the code is one sign bit per rotated coordinate, `x̄_i = sign(y_i)`;
//! * the quantized unit vector is `ō = x̄ / √D` (in rotated space), and the
//!   entry also stores `⟨ō, o'⟩ = Σ|y_i| / √D`.
//!
//! Scoring a unit query `q` uses `⟨o, q⟩ = ⟨c, q⟩ + ‖r‖·⟨o', q⟩`, where the
//! centroid term is exact and RaBitQ's estimator gives
//! `⟨o', q⟩ ≈ ⟨ō, Rq⟩ / ⟨ō, o'⟩`, whose error is close to Gaussian with
//! standard deviation `√((1 - ⟨ō,o'⟩²) / ⟨ō,o'⟩²) / √(D - 1)`. The error
//! bound is that times `ε0` ([`RABITQ_EPSILON0`]) — what the probe uses to
//! decide how many candidates to rescore at full precision.
//!
//! The paper residualizes the query too, because it estimates Euclidean
//! distance. Kosha scores cosine (a dot product of unit vectors), so only
//! the data side needs the centroid; the estimator holds for any unit `q`.
//!
//! `R` is a randomized Hadamard transform rather than a dense random
//! orthogonal matrix: `O(D log D)` to apply, and nothing to store, since
//! it is derived from a fixed seed. `D` is the vector dimension rounded
//! up to a power of two (vectors are zero-padded).

use crate::DeterministicRng;

/// Confidence parameter of the estimator's error bound, in units of the
/// estimator's standard deviation `√((1 - ⟨ō,o'⟩²) / ⟨ō,o'⟩²) / √(D - 1)`.
/// The error is close to Gaussian, so `1.9` covers ~94% of estimates; the
/// paper finds that already gives near-perfect recall once the candidates
/// the bound cannot rule out are rescored at full precision.
pub const RABITQ_EPSILON0: f32 = 1.9;

/// Seed of the rotation every RaBitQ code is written with. Part of the
/// on-disk format: changing it makes existing codes unreadable.
pub const RABITQ_ROTATION_SEED: u64 = 0x5241_4249_5451_0001; // "RABITQ" v1

/// Rounds of sign-flip + Hadamard. Three is the usual choice for making a
/// structured transform behave like a random rotation.
const ROTATION_ROUNDS: usize = 3;

/// Smallest `⟨ō, o'⟩` the estimator divides by. The real value concentrates
/// around `0.8` and is positive for any nonzero residual; this only guards
/// against division by (near) zero on degenerate input.
const MIN_IP: f32 = 1e-6;

/// The randomized Hadamard rotation for one vector dimension.
#[derive(Debug, Clone, PartialEq)]
pub struct RaBitQRotation {
    dim: usize,
    padded: usize,
    /// `ROTATION_ROUNDS` diagonal ±1 matrices, `padded` entries each.
    signs: Vec<f32>,
}

impl RaBitQRotation {
    /// The rotation for `dim`-dimensional vectors, derived from
    /// [`RABITQ_ROTATION_SEED`] — every writer and reader gets the same one.
    pub fn new(dim: usize) -> Self {
        let padded = rabitq_padded_dim(dim);
        let mut rng = DeterministicRng::new(RABITQ_ROTATION_SEED ^ padded as u64);
        let signs = (0..ROTATION_ROUNDS * padded)
            .map(|_| if rng.next_u64() & 1 == 0 { 1.0 } else { -1.0 })
            .collect();
        Self { dim, padded, signs }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Rotated (and padded) dimension: one code bit per coordinate.
    pub fn padded_dim(&self) -> usize {
        self.padded
    }

    /// Code bytes per vector.
    pub fn code_bytes(&self) -> usize {
        rabitq_code_bytes(self.dim)
    }

    /// `R v`: `v` zero-padded to [`Self::padded_dim`], then rotated. Norm
    /// preserving (each round is a sign flip and a normalized Hadamard).
    pub fn apply(&self, v: &[f32]) -> Vec<f32> {
        debug_assert_eq!(v.len(), self.dim, "RaBitQRotation: dimension mismatch");
        let mut out = vec![0.0f32; self.padded];
        out[..v.len().min(self.padded)].copy_from_slice(&v[..v.len().min(self.padded)]);
        let scale = 1.0 / (self.padded as f32).sqrt();
        for round in self.signs.chunks_exact(self.padded) {
            for (x, s) in out.iter_mut().zip(round) {
                *x *= s;
            }
            fwht(&mut out);
            for x in &mut out {
                *x *= scale;
            }
        }
        out
    }
}

/// `dim` rounded up to a power of two (at least 8, so a code is whole bytes).
pub fn rabitq_padded_dim(dim: usize) -> usize {
    dim.max(8).next_power_of_two()
}

/// Bytes of one RaBitQ code for `dim`-dimensional vectors.
pub fn rabitq_code_bytes(dim: usize) -> usize {
    rabitq_padded_dim(dim) / 8
}

/// In-place unnormalized fast Walsh–Hadamard transform. `v.len()` must be
/// a power of two.
fn fwht(v: &mut [f32]) {
    let n = v.len();
    let mut h = 1;
    while h < n {
        for block in v.chunks_exact_mut(2 * h) {
            let (lo, hi) = block.split_at_mut(h);
            for (a, b) in lo.iter_mut().zip(hi.iter_mut()) {
                let (x, y) = (*a, *b);
                *a = x + y;
                *b = x - y;
            }
        }
        h *= 2;
    }
}

/// One coded vector: what an index entry stores besides its id.
#[derive(Debug, Clone, PartialEq)]
pub struct RaBitQCode {
    /// `‖o - c‖`.
    pub residual_norm: f32,
    /// `⟨ō, o'⟩`, in `(0, 1]`.
    pub ip: f32,
}

/// Codes `v` against `centroid`, writing [`RaBitQRotation::code_bytes`]
/// bytes into `bits` (cleared first; bit `i` is byte `i / 8`, bit `i % 8`).
/// A vector equal to its centroid has no residual: it codes as all-zero
/// bits with `residual_norm = 0`, and scores exactly `⟨c, q⟩`.
pub fn encode_rabitq(
    rotation: &RaBitQRotation,
    v: &[f32],
    centroid: &[f32],
    bits: &mut Vec<u8>,
) -> RaBitQCode {
    bits.clear();
    bits.resize(rotation.code_bytes(), 0);
    let residual: Vec<f32> = v
        .iter()
        .zip(centroid)
        .map(|(x, c)| if x.is_finite() { x - c } else { -c })
        .collect();
    let norm = residual.iter().map(|x| x * x).sum::<f32>().sqrt();
    if !norm.is_finite() || norm <= f32::EPSILON {
        return RaBitQCode {
            residual_norm: 0.0,
            ip: 1.0,
        };
    }
    let unit: Vec<f32> = residual.iter().map(|x| x / norm).collect();
    let y = rotation.apply(&unit);
    let mut abs_sum = 0.0f32;
    for (i, &yi) in y.iter().enumerate() {
        if yi > 0.0 {
            bits[i / 8] |= 1 << (i % 8);
        }
        abs_sum += yi.abs();
    }
    let ip = (abs_sum / (rotation.padded_dim() as f32).sqrt()).clamp(MIN_IP, 1.0);
    RaBitQCode {
        residual_norm: norm,
        ip,
    }
}

/// A unit query prepared for scoring RaBitQ codes of one dimension: the
/// rotated query as a per-byte lookup table, so `⟨x̄, Rq⟩` costs one table
/// read per code byte instead of one add per bit.
#[derive(Debug, Clone)]
pub struct RaBitQQuery {
    /// `lut[j * 256 + m]` is the sum of `(Rq)_{8j+t}` over the set bits
    /// `t` of `m`.
    lut: Vec<f32>,
    /// `Σ (Rq)_i`.
    total: f32,
    inv_sqrt_padded: f32,
    /// `ε0 / √(D - 1)`.
    bound_scale: f32,
}

impl RaBitQQuery {
    /// Prepares `unit_query` (already unit-normalized) for `rotation`.
    pub fn new(rotation: &RaBitQRotation, unit_query: &[f32]) -> Self {
        let y = rotation.apply(unit_query);
        let bytes = rotation.code_bytes();
        let mut lut = vec![0.0f32; bytes * 256];
        for (j, table) in lut.chunks_exact_mut(256).enumerate() {
            let lane = &y[j * 8..j * 8 + 8];
            for m in 1..256usize {
                let low = m.trailing_zeros() as usize;
                table[m] = table[m & (m - 1)] + lane[low];
            }
        }
        let padded = rotation.padded_dim() as f32;
        Self {
            lut,
            total: y.iter().sum(),
            inv_sqrt_padded: 1.0 / padded.sqrt(),
            bound_scale: RABITQ_EPSILON0 / (padded - 1.0).max(1.0).sqrt(),
        }
    }

    /// `⟨ō, Rq⟩` for a code: `(2·Σ_{set} y_i - Σ y_i) / √D`.
    fn quantized_ip(&self, bits: &[u8]) -> f32 {
        let mut set = 0.0f32;
        for (table, &b) in self.lut.chunks_exact(256).zip(bits) {
            set += table[b as usize];
        }
        (2.0 * set - self.total) * self.inv_sqrt_padded
    }

    /// Estimated `⟨o, q⟩` of the coded vector, and the estimator's error
    /// bound (see the module docs). `centroid_dot` is `⟨c, q⟩`, exact.
    pub fn estimate(&self, code: &RaBitQCode, bits: &[u8], centroid_dot: f32) -> (f32, f32) {
        if code.residual_norm == 0.0 {
            return (centroid_dot, 0.0);
        }
        let ip = code.ip.max(MIN_IP);
        let est = centroid_dot + code.residual_norm * self.quantized_ip(bits) / ip;
        let err =
            code.residual_norm * ((1.0 - ip * ip).max(0.0) / (ip * ip)).sqrt() * self.bound_scale;
        (est, err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{dot, normalize_in_place};

    fn unit(rng: &mut DeterministicRng, dim: usize) -> Vec<f32> {
        let mut v: Vec<f32> = (0..dim).map(|_| rng.next_f32_range(-1.0, 1.0)).collect();
        normalize_in_place(&mut v);
        v
    }

    #[test]
    fn rotation_preserves_norms_and_inner_products() {
        let mut rng = DeterministicRng::new(1);
        for dim in [3, 8, 100, 384, 768, 1024] {
            let rot = RaBitQRotation::new(dim);
            assert!(rot.padded_dim().is_power_of_two() && rot.padded_dim() >= dim);
            let (a, b) = (unit(&mut rng, dim), unit(&mut rng, dim));
            let (ra, rb) = (rot.apply(&a), rot.apply(&b));
            assert!((dot(&ra, &ra) - 1.0).abs() < 1e-4, "dim {dim}");
            assert!((dot(&ra, &rb) - dot(&a, &b)).abs() < 1e-4, "dim {dim}");
        }
    }

    #[test]
    fn rotation_is_deterministic() {
        assert_eq!(RaBitQRotation::new(384), RaBitQRotation::new(384));
        let mut rng = DeterministicRng::new(2);
        let v = unit(&mut rng, 384);
        assert_eq!(
            RaBitQRotation::new(384).apply(&v),
            RaBitQRotation::new(384).apply(&v)
        );
    }

    #[test]
    fn code_size_is_one_bit_per_padded_dimension() {
        assert_eq!(rabitq_code_bytes(1024), 128);
        assert_eq!(rabitq_code_bytes(768), 128);
        assert_eq!(rabitq_code_bytes(1536), 256);
        assert_eq!(rabitq_code_bytes(2), 1);
    }

    /// The estimator's error behaves like the Gaussian the bound assumes:
    /// ~94% of estimates inside `ε0 = 1.9` deviations, nearly all inside
    /// 1.5x that, and a small mean error — the properties the probe's
    /// rescoring shortlist relies on.
    #[test]
    fn estimates_stay_within_the_error_bound() {
        let mut rng = DeterministicRng::new(3);
        for dim in [64, 384, 1024] {
            let rot = RaBitQRotation::new(dim);
            let mut bits = Vec::new();
            let (mut outside, mut outside_wide, mut total, mut abs_err) =
                (0usize, 0usize, 0usize, 0.0f64);
            for _ in 0..20 {
                // A cluster: centroid plus members near it, like a posting.
                let centroid = unit(&mut rng, dim);
                let q = unit(&mut rng, dim);
                let rq = RaBitQQuery::new(&rot, &q);
                let cq = dot(&centroid, &q);
                for _ in 0..50 {
                    let noise = unit(&mut rng, dim);
                    let mut o: Vec<f32> = centroid
                        .iter()
                        .zip(&noise)
                        .map(|(c, n)| c + 0.6 * n)
                        .collect();
                    normalize_in_place(&mut o);
                    let code = encode_rabitq(&rot, &o, &centroid, &mut bits);
                    let (est, err) = rq.estimate(&code, &bits, cq);
                    let exact = dot(&o, &q);
                    total += 1;
                    abs_err += (est - exact).abs() as f64;
                    if (est - exact).abs() > err {
                        outside += 1;
                    }
                    if (est - exact).abs() > 1.5 * err {
                        outside_wide += 1;
                    }
                }
            }
            assert!(
                outside * 100 <= 9 * total,
                "dim {dim}: {outside}/{total} estimates outside the bound"
            );
            assert!(
                outside_wide * 100 <= total,
                "dim {dim}: {outside_wide}/{total} estimates outside 1.5x the bound"
            );
            let mean = abs_err / total as f64;
            let limit = 0.6 / (dim as f64).sqrt();
            assert!(mean < limit, "dim {dim}: mean abs error {mean} >= {limit}");
        }
    }

    #[test]
    fn a_vector_equal_to_its_centroid_scores_the_centroid() {
        let mut rng = DeterministicRng::new(4);
        let rot = RaBitQRotation::new(16);
        let c = unit(&mut rng, 16);
        let mut bits = Vec::new();
        let code = encode_rabitq(&rot, &c, &c, &mut bits);
        assert_eq!(code.residual_norm, 0.0);
        assert!(bits.iter().all(|&b| b == 0));
        let q = unit(&mut rng, 16);
        let (est, err) = RaBitQQuery::new(&rot, &q).estimate(&code, &bits, dot(&c, &q));
        assert_eq!((est, err), (dot(&c, &q), 0.0));
    }

    #[test]
    fn non_finite_components_do_not_poison_the_code() {
        let rot = RaBitQRotation::new(4);
        let mut bits = Vec::new();
        let code = encode_rabitq(
            &rot,
            &[f32::NAN, 1.0, f32::INFINITY, 0.0],
            &[0.0; 4],
            &mut bits,
        );
        assert!(code.residual_norm.is_finite() && code.ip.is_finite());
        let (est, err) = RaBitQQuery::new(&rot, &[0.0, 1.0, 0.0, 0.0]).estimate(&code, &bits, 0.0);
        assert!(est.is_finite() && err.is_finite());
    }
}
