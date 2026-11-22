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

/// How a [`RaBitQQuery`] computes `⟨x̄, Rq⟩` (#382).
///
/// * [`Lut`](Self::Lut): the default, a 256-entry f32 table per code byte
///   (`D/8 · 1 KiB`, 128 KiB at `D = 1024`), one table read per code byte.
/// * [`Popcount4`](Self::Popcount4): RaBitQ §3.3.2. The rotated query is
///   scalar-quantized to [`RABITQ_QUERY_BITS`] bits per coordinate, stored
///   as that many bit planes (`D/64` words each, 512 B at `D = 1024`), and
///   a code is scored with AND + popcount per 64-bit word. Its estimate
///   equals [`Lut`](Self::Lut) run on the dequantized query up to float
///   rounding, so it differs from [`Lut`](Self::Lut) only by the query's
///   rounding error. That error is small but not free: on synthetic
///   clusters at `D = 1024` it averages ~9% of the RaBitQ error bound, so
///   this estimator's bound adds the rounding term (`ε0 · delta/√12`, in
///   quadrature) to keep rescoring as safe as with the LUT.
///
/// The choice touches only query-time arithmetic, never the on-disk codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RaBitQEstimator {
    #[default]
    Lut,
    Popcount4,
}

impl RaBitQEstimator {
    /// `lut` or `popcount` (also `popcount4`), case-insensitive.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "lut" => Some(Self::Lut),
            "popcount" | "popcount4" => Some(Self::Popcount4),
            _ => None,
        }
    }
}

/// Bits per rotated query coordinate for [`RaBitQEstimator::Popcount4`]
/// (`B_q` in the paper; 4 is its recommended value).
pub const RABITQ_QUERY_BITS: usize = 4;

/// The query side of `⟨x̄, Rq⟩`, per [`RaBitQEstimator`].
#[derive(Debug, Clone)]
enum Kernel {
    /// `lut[j * 256 + m]` is the sum of `(Rq)_{8j+t}` over the set bits
    /// `t` of `m`.
    Lut(Vec<f32>),
    /// `(Rq)_i ≈ low + delta · q_i` with `q_i ∈ [0, 2^B_q)`. `planes[w][b]`
    /// holds bit `b` of `q_i` for the 64 coordinates `i` of word `w`, in
    /// the codes' bit order (coordinate `i` is bit `i % 64` of the
    /// little-endian word `i / 64`). `quantized_ip` is
    /// `scale_q · Σ_b 2^b·popcount(x & plane_b) + scale_x · popcount(x) - offset`.
    Popcount {
        planes: Vec<[u64; RABITQ_QUERY_BITS]>,
        scale_q: f32,
        scale_x: f32,
        offset: f32,
        /// `ε0 · delta / √12`: the bound on `⟨x̄, Rq⟩`'s error from
        /// rounding the query (each coordinate's error is uniform on
        /// `±delta/2`, so `Σ x̄_i e_i` has deviation `delta / √12`).
        rounding_bound: f32,
    },
}

/// A unit query prepared for scoring RaBitQ codes of one dimension: the
/// rotated query as a per-byte lookup table (or, with
/// [`RaBitQEstimator::Popcount4`], as quantized bit planes), so `⟨x̄, Rq⟩`
/// costs one table read per code byte (or a few popcounts per 64 bits)
/// instead of one add per bit.
#[derive(Debug, Clone)]
pub struct RaBitQQuery {
    kernel: Kernel,
    /// `Σ (Rq)_i` (used by the LUT kernel).
    total: f32,
    inv_sqrt_padded: f32,
    /// `ε0 / √(D - 1)`.
    bound_scale: f32,
}

impl RaBitQQuery {
    /// Prepares `unit_query` (already unit-normalized) for `rotation`, with
    /// the default [`RaBitQEstimator::Lut`].
    pub fn new(rotation: &RaBitQRotation, unit_query: &[f32]) -> Self {
        Self::with_estimator(rotation, unit_query, RaBitQEstimator::Lut)
    }

    /// Prepares `unit_query` (already unit-normalized) for `rotation`,
    /// scoring codes with `estimator`.
    pub fn with_estimator(
        rotation: &RaBitQRotation,
        unit_query: &[f32],
        estimator: RaBitQEstimator,
    ) -> Self {
        Self::from_rotated(&rotation.apply(unit_query), estimator)
    }

    /// Prepares an already rotated (and padded) query `y`.
    fn from_rotated(y: &[f32], estimator: RaBitQEstimator) -> Self {
        let padded = y.len() as f32;
        let inv_sqrt_padded = 1.0 / padded.sqrt();
        let kernel = match estimator {
            RaBitQEstimator::Lut => Kernel::Lut(build_lut(y)),
            RaBitQEstimator::Popcount4 => quantize_query(y, inv_sqrt_padded),
        };
        Self {
            kernel,
            total: y.iter().sum(),
            inv_sqrt_padded,
            bound_scale: RABITQ_EPSILON0 / (padded - 1.0).max(1.0).sqrt(),
        }
    }

    /// Which estimator this query scores with.
    pub fn estimator(&self) -> RaBitQEstimator {
        match self.kernel {
            Kernel::Lut(_) => RaBitQEstimator::Lut,
            Kernel::Popcount { .. } => RaBitQEstimator::Popcount4,
        }
    }

    /// `⟨ō, Rq⟩` for a code: `(2·Σ_{set} y_i - Σ y_i) / √D`.
    fn quantized_ip(&self, bits: &[u8]) -> f32 {
        match &self.kernel {
            Kernel::Lut(lut) => {
                let mut set = 0.0f32;
                for (table, &b) in lut.as_chunks::<256>().0.iter().zip(bits) {
                    set += table[b as usize];
                }
                (2.0 * set - self.total) * self.inv_sqrt_padded
            }
            Kernel::Popcount {
                planes,
                scale_q,
                scale_x,
                offset,
                ..
            } => {
                let (words, rest) = bits.as_chunks::<8>();
                let mut acc = [0u32; RABITQ_QUERY_BITS];
                let mut ones = 0u32;
                let mut score = |x: u64, plane: &[u64; RABITQ_QUERY_BITS]| {
                    ones += x.count_ones();
                    for (a, p) in acc.iter_mut().zip(plane) {
                        *a += (x & p).count_ones();
                    }
                };
                for (word, plane) in words.iter().zip(planes) {
                    score(u64::from_le_bytes(*word), plane);
                }
                if !rest.is_empty() {
                    // Codes shorter than a word (`D < 64`).
                    let mut last = [0u8; 8];
                    last[..rest.len()].copy_from_slice(rest);
                    score(u64::from_le_bytes(last), &planes[words.len()]);
                }
                let weighted: u32 = acc.iter().enumerate().map(|(b, &a)| a << b).sum();
                scale_q * weighted as f32 + scale_x * ones as f32 - offset
            }
        }
    }

    /// Estimated `⟨o, q⟩` of the coded vector, and the estimator's error
    /// bound (see the module docs). `centroid_dot` is `⟨c, q⟩`, exact.
    pub fn estimate(&self, code: &RaBitQCode, bits: &[u8], centroid_dot: f32) -> (f32, f32) {
        if code.residual_norm == 0.0 {
            return (centroid_dot, 0.0);
        }
        let ip = code.ip.max(MIN_IP);
        let est = centroid_dot + code.residual_norm * self.quantized_ip(bits) / ip;
        let mut err =
            code.residual_norm * ((1.0 - ip * ip).max(0.0) / (ip * ip)).sqrt() * self.bound_scale;
        if let Kernel::Popcount { rounding_bound, .. } = self.kernel {
            if rounding_bound > 0.0 {
                // The query's rounding error is independent of the code's,
                // so the two deviations add in quadrature.
                err = err.hypot(code.residual_norm * rounding_bound / ip);
            }
        }
        (est, err)
    }
}

/// The per-byte lookup table of [`Kernel::Lut`] for the rotated query `y`.
fn build_lut(y: &[f32]) -> Vec<f32> {
    let mut lut = vec![0.0f32; y.len() / 8 * 256];
    for (j, table) in lut.as_chunks_mut::<256>().0.iter_mut().enumerate() {
        let lane = &y[j * 8..j * 8 + 8];
        for m in 1..256usize {
            let low = m.trailing_zeros() as usize;
            table[m] = table[m & (m - 1)] + lane[low];
        }
    }
    lut
}

/// `y` scalar-quantized to [`RABITQ_QUERY_BITS`] bits per coordinate
/// (RaBitQ §3.3.2): `y_i ≈ low + delta · q_i` over `[min y, max y]`. The
/// paper rounds stochastically for an unbiased estimator; this rounds to
/// nearest, which is deterministic and has a smaller per-coordinate error
/// (`≤ delta / 2`). A degenerate (constant or non-finite) query quantizes
/// to `q = 0`.
fn quantize_query(y: &[f32], inv_sqrt_padded: f32) -> Kernel {
    const LEVELS: f32 = ((1u32 << RABITQ_QUERY_BITS) - 1) as f32;
    let (mut low, mut high) = (f32::INFINITY, f32::NEG_INFINITY);
    for &v in y.iter().filter(|v| v.is_finite()) {
        low = low.min(v);
        high = high.max(v);
    }
    if !low.is_finite() {
        (low, high) = (0.0, 0.0);
    }
    let mut delta = (high - low) / LEVELS;
    if !delta.is_finite() {
        delta = 0.0;
    }
    let mut planes = vec![[0u64; RABITQ_QUERY_BITS]; y.len().div_ceil(64)];
    let mut sum_q = 0u32;
    if delta > 0.0 {
        for (i, &v) in y.iter().enumerate() {
            let q = if v.is_finite() {
                ((v - low) / delta).round().clamp(0.0, LEVELS) as u32
            } else {
                0
            };
            sum_q += q;
            for (b, plane) in planes[i / 64].iter_mut().enumerate() {
                *plane |= u64::from((q >> b) & 1) << (i % 64);
            }
        }
    }
    // (2·Σ_set (low + delta·q_i) - Σ_all (low + delta·q_i)) / √D
    Kernel::Popcount {
        planes,
        scale_q: 2.0 * delta * inv_sqrt_padded,
        scale_x: 2.0 * low * inv_sqrt_padded,
        offset: (delta * sum_q as f32 + low * y.len() as f32) * inv_sqrt_padded,
        rounding_bound: RABITQ_EPSILON0 * delta / 12f32.sqrt(),
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
    /// rescoring shortlist relies on. Holds for both estimators, on the
    /// same data (the popcount one adds only the query's 4-bit error).
    #[test]
    fn estimates_stay_within_the_error_bound() {
        for estimator in [RaBitQEstimator::Lut, RaBitQEstimator::Popcount4] {
            estimates_stay_within_the_error_bound_with(estimator);
        }
    }

    fn estimates_stay_within_the_error_bound_with(estimator: RaBitQEstimator) {
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
                let rq = RaBitQQuery::with_estimator(&rot, &q, estimator);
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
                "{estimator:?} dim {dim}: {outside}/{total} estimates outside the bound"
            );
            assert!(
                outside_wide * 100 <= total,
                "{estimator:?} dim {dim}: {outside_wide}/{total} estimates outside 1.5x the bound"
            );
            let mean = abs_err / total as f64;
            let limit = 0.6 / (dim as f64).sqrt();
            assert!(
                mean < limit,
                "{estimator:?} dim {dim}: mean abs error {mean} >= {limit}"
            );
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
        let popcount =
            RaBitQQuery::with_estimator(&rot, &[0.0, 1.0, 0.0, 0.0], RaBitQEstimator::Popcount4);
        let (est, err) = popcount.estimate(&code, &bits, 0.0);
        assert!(est.is_finite() && err.is_finite());
        // A degenerate (all-equal) rotated query quantizes to q = 0 and
        // still scores like the LUT does on it.
        let flat = vec![0.25f32; 16];
        let (p, l) = (
            RaBitQQuery::from_rotated(&flat, RaBitQEstimator::Popcount4),
            RaBitQQuery::from_rotated(&flat, RaBitQEstimator::Lut),
        );
        for bits in [[0u8, 0], [0xff, 0x0f], [0x81, 0xff]] {
            assert!((p.quantized_ip(&bits) - l.quantized_ip(&bits)).abs() < 1e-6);
        }
    }

    #[test]
    fn estimator_parses_and_defaults_to_lut() {
        assert_eq!(RaBitQEstimator::default(), RaBitQEstimator::Lut);
        assert_eq!(RaBitQEstimator::parse(" LUT "), Some(RaBitQEstimator::Lut));
        assert_eq!(
            RaBitQEstimator::parse("popcount"),
            Some(RaBitQEstimator::Popcount4)
        );
        assert_eq!(
            RaBitQEstimator::parse("Popcount4"),
            Some(RaBitQEstimator::Popcount4)
        );
        assert_eq!(RaBitQEstimator::parse("fastscan"), None);
        let rot = RaBitQRotation::new(64);
        let q = unit(&mut DeterministicRng::new(5), 64);
        assert_eq!(RaBitQQuery::new(&rot, &q).estimator(), RaBitQEstimator::Lut);
        assert_eq!(
            RaBitQQuery::with_estimator(&rot, &q, RaBitQEstimator::Popcount4).estimator(),
            RaBitQEstimator::Popcount4
        );
    }

    /// Rounds `y` to the 4-bit grid the popcount kernel quantizes to,
    /// written out independently of `quantize_query`.
    fn dequantized(y: &[f32]) -> Vec<f32> {
        let low = y.iter().copied().fold(f32::INFINITY, f32::min);
        let high = y.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let delta = (high - low) / 15.0;
        y.iter()
            .map(|&v| low + delta * ((v - low) / delta).round())
            .collect()
    }

    fn random_bits(rng: &mut DeterministicRng, bytes: usize) -> Vec<u8> {
        (0..bytes).map(|_| rng.next_u64() as u8).collect()
    }

    /// The popcount kernel is exact arithmetic on the 4-bit query: it
    /// matches the LUT kernel run on the dequantized query up to float
    /// rounding, for every code, including codes shorter than a word.
    #[test]
    fn popcount_equals_lut_on_the_dequantized_query() {
        let mut rng = DeterministicRng::new(6);
        for dim in [3, 8, 16, 40, 64, 100, 384, 768, 1024, 1536] {
            let rot = RaBitQRotation::new(dim);
            for _ in 0..10 {
                let y = rot.apply(&unit(&mut rng, dim));
                let popcount = RaBitQQuery::from_rotated(&y, RaBitQEstimator::Popcount4);
                let lut = RaBitQQuery::from_rotated(&dequantized(&y), RaBitQEstimator::Lut);
                let mut codes = vec![vec![0u8; rot.code_bytes()], vec![0xffu8; rot.code_bytes()]];
                codes.extend((0..50).map(|_| random_bits(&mut rng, rot.code_bytes())));
                let code = RaBitQCode {
                    residual_norm: 0.7,
                    ip: 0.8,
                };
                for bits in &codes {
                    let (a, b) = (popcount.quantized_ip(bits), lut.quantized_ip(bits));
                    assert!(
                        (a - b).abs() <= 1e-5 * (1.0 + b.abs()),
                        "dim {dim}: popcount {a} vs lut-on-dequantized {b}"
                    );
                    let (pa, pe) = popcount.estimate(&code, bits, 0.1);
                    let (la, le) = lut.estimate(&code, bits, 0.1);
                    assert!((pa - la).abs() <= 2e-5, "dim {dim}: {pa} vs {la}");
                    // The same RaBitQ bound, plus the query-rounding term.
                    assert!(pe >= le, "dim {dim}: bound {pe} < lut bound {le}");
                }
            }
        }
    }

    /// Against the full-precision LUT, the popcount estimate moves by at
    /// most the 4-bit query rounding (`|Σ ±e_i| / √D ≤ √D·delta/2`, times
    /// `‖r‖ / ⟨ō,o'⟩`), and on posting-like data by a small fraction of
    /// RaBitQ's own error bound.
    #[test]
    fn popcount_tracks_the_lut_within_query_rounding() {
        let mut rng = DeterministicRng::new(7);
        for dim in [64, 384, 1024] {
            let rot = RaBitQRotation::new(dim);
            let sqrt_d = (rot.padded_dim() as f32).sqrt();
            let mut bits = Vec::new();
            let (mut diff_sum, mut bound_sum, mut widened_sum, mut n) =
                (0.0f64, 0.0f64, 0.0f64, 0usize);
            for _ in 0..20 {
                let centroid = unit(&mut rng, dim);
                let q = unit(&mut rng, dim);
                let y = rot.apply(&q);
                let low = y.iter().copied().fold(f32::INFINITY, f32::min);
                let high = y.iter().copied().fold(f32::NEG_INFINITY, f32::max);
                let delta = (high - low) / 15.0;
                let lut = RaBitQQuery::with_estimator(&rot, &q, RaBitQEstimator::Lut);
                let popcount = RaBitQQuery::with_estimator(&rot, &q, RaBitQEstimator::Popcount4);
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
                    let (l, err) = lut.estimate(&code, &bits, cq);
                    let (p, perr) = popcount.estimate(&code, &bits, cq);
                    assert!(perr >= err, "dim {dim}: bound {perr} < lut bound {err}");
                    widened_sum += (perr / err) as f64;
                    let hard = code.residual_norm / code.ip * sqrt_d * delta / 2.0;
                    assert!(
                        (p - l).abs() <= hard * 1.001 + 1e-6,
                        "dim {dim}: |{p} - {l}| > rounding bound {hard}"
                    );
                    diff_sum += (p - l).abs() as f64;
                    bound_sum += err as f64;
                    n += 1;
                }
            }
            let (mean_diff, mean_bound) = (diff_sum / n as f64, bound_sum / n as f64);
            let widened = widened_sum / n as f64;
            eprintln!(
                "dim {dim}: mean |popcount - lut| {mean_diff:.5}, mean lut bound {mean_bound:.5}, \
                 popcount bound / lut bound {widened:.4}"
            );
            assert!(
                mean_diff <= 0.25 * mean_bound,
                "dim {dim}: mean |popcount - lut| {mean_diff} vs mean error bound {mean_bound}"
            );
            // The rounding term widens the bound (and so the rescoring
            // shortlist) only slightly.
            assert!(widened <= 1.1, "dim {dim}: bound widened {widened}x");
        }
    }
}
