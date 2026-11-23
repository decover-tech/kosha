//! i8 scalar quantization (#210): one symmetric scale per vector,
//! `scale = max|x_i| / 127`, codes `round(x_i / scale)` in `-127..=127`.
//!
//! Symmetric per-vector scaling suits the unit-norm vectors the posting
//! index stores: every component already lies in `[-1, 1]`, there is no
//! offset to carry, and a vector's scale folds out of the dot product, so
//! scoring is `scale * sum(q_i * code_i)` with no dequantize step.

/// Largest code magnitude. `-128` is never produced, so the code range is
/// symmetric and negating a vector negates its codes exactly.
pub const I8_MAX_CODE: f32 = 127.0;

/// Quantizes `v` into `codes` (cleared first) and returns the vector's
/// scale. A zero vector (or one with no finite component) gets scale `0.0`
/// and all-zero codes, so it scores `0` against everything, like the zero
/// vector it stands for. Non-finite components quantize to `0`.
pub fn quantize_i8(v: &[f32], codes: &mut Vec<i8>) -> f32 {
    codes.clear();
    let max_abs = v
        .iter()
        .filter(|x| x.is_finite())
        .fold(0.0f32, |m, x| m.max(x.abs()));
    if max_abs == 0.0 {
        codes.resize(v.len(), 0);
        return 0.0;
    }
    let scale = max_abs / I8_MAX_CODE;
    let inv = 1.0 / scale;
    codes.extend(v.iter().map(|&x| {
        if x.is_finite() {
            (x * inv).round().clamp(-I8_MAX_CODE, I8_MAX_CODE) as i8
        } else {
            0
        }
    }));
    scale
}

/// The `f32` vector `codes` stands for. Only for paths that need a plain
/// vector (the eager fallback that serves a column without its offsets
/// sidecar); scoring uses [`dot_i8`] instead.
pub fn dequantize_i8(codes: &[i8], scale: f32) -> Vec<f32> {
    codes.iter().map(|&c| c as f32 * scale).collect()
}

/// `scale * sum(q_i * codes_i)`: the dot product of an `f32` query with a
/// quantized vector, without materializing the vector. Same 8-lane
/// accumulation as [`crate::dot`] so it vectorizes the same way.
pub fn dot_i8(q: &[f32], codes: &[i8], scale: f32) -> f32 {
    debug_assert_eq!(q.len(), codes.len(), "dot_i8: dimension mismatch");
    let mut acc = [0.0f32; 8];
    let (cq, rq) = q.as_chunks::<8>();
    let (cc, rc) = codes.as_chunks::<8>();
    for (xq, xc) in cq.iter().zip(cc.iter()) {
        for l in 0..8 {
            acc[l] += xq[l] * xc[l] as f32;
        }
    }
    let mut s = ((acc[0] + acc[1]) + (acc[2] + acc[3])) + ((acc[4] + acc[5]) + (acc[6] + acc[7]));
    for (x, &c) in rq.iter().zip(rc.iter()) {
        s += x * c as f32;
    }
    s * scale
}

/// Quantizes a query to i16 codes in `-max_code..=max_code`, one symmetric
/// scale for the vector (`scale = max|x_i| / max_code`), for
/// [`dot_i16_i8`]. A zero or non-finite query gets scale `0.0` and
/// all-zero codes. `max_code` is clamped to `1..=i16::MAX`.
pub fn quantize_i16(v: &[f32], max_code: i16) -> (Vec<i16>, f32) {
    let max_code = f32::from(max_code.max(1));
    let max_abs = v.iter().fold(0.0f32, |m, x| m.max(x.abs()));
    if max_abs == 0.0 || !max_abs.is_finite() || v.iter().any(|x| !x.is_finite()) {
        return (vec![0; v.len()], 0.0);
    }
    let scale = max_abs / max_code;
    let inv = 1.0 / scale;
    let codes = v
        .iter()
        .map(|&x| (x * inv).round().clamp(-max_code, max_code) as i16)
        .collect();
    (codes, scale)
}

/// Largest `max_code` for [`quantize_i16`] at which [`dot_i16_i8`] over
/// `dim` components cannot overflow its `i32` sum:
/// `max_code * 127 * dim <= i32::MAX`.
pub fn max_i16_query_code(dim: usize) -> i16 {
    let limit = i32::MAX as usize / (I8_MAX_CODE as usize * dim.max(1));
    limit.min(i16::MAX as usize) as i16
}

/// `sum(q_i * codes_i)` in integers: the dot product of a quantized query
/// with a quantized vector, exactly (multiply by both scales for the
/// estimate). Integer addition is associative, so unlike [`dot_i8`] the
/// compiler is free to widen and unroll this as far as the target allows.
/// The caller keeps `|q_i| <= max_i16_query_code(dim)` so the sum fits.
pub fn dot_i16_i8(q: &[i16], codes: &[i8]) -> i32 {
    debug_assert_eq!(q.len(), codes.len(), "dot_i16_i8: dimension mismatch");
    q.iter()
        .zip(codes)
        .map(|(&x, &c)| i32::from(x) * i32::from(c))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{dot, normalize_in_place};

    fn unit(seed: u64, dim: usize) -> Vec<f32> {
        let mut rng = crate::DeterministicRng::new(seed);
        let mut v: Vec<f32> = (0..dim).map(|_| rng.next_f32_range(-1.0, 1.0)).collect();
        normalize_in_place(&mut v);
        v
    }

    #[test]
    fn round_trip_error_is_bounded_by_half_a_step() {
        let v = unit(7, 384);
        let mut codes = Vec::new();
        let scale = quantize_i8(&v, &mut codes);
        assert_eq!(codes.len(), v.len());
        let back = dequantize_i8(&codes, scale);
        for (a, b) in v.iter().zip(&back) {
            assert!((a - b).abs() <= scale * 0.5 + 1e-7, "{a} vs {b}");
        }
        // The largest component maps to the extreme code exactly.
        assert!(codes.iter().any(|&c| c == 127 || c == -127));
        assert!(codes.iter().all(|&c| c != -128));
    }

    #[test]
    fn dot_i8_matches_dot_of_the_dequantized_vector() {
        for dim in [1, 7, 8, 9, 384, 1023] {
            let q = unit(1, dim);
            let v = unit(2, dim);
            let mut codes = Vec::new();
            let scale = quantize_i8(&v, &mut codes);
            let expected = dot(&q, &dequantize_i8(&codes, scale));
            let got = dot_i8(&q, &codes, scale);
            assert!(
                (expected - got).abs() < 1e-5,
                "dim {dim}: {expected} vs {got}"
            );
            // ... and is close to the exact similarity.
            assert!((dot(&q, &v) - got).abs() < 0.02, "dim {dim}");
        }
    }

    #[test]
    fn dot_i16_i8_is_the_integer_dot_product_and_cannot_overflow() {
        for dim in [1usize, 7, 16, 17, 384, 1024, 4096, 70_000] {
            let max_code = max_i16_query_code(dim);
            assert!(max_code >= 1);
            assert!(i64::from(max_code) * 127 * dim as i64 <= i64::from(i32::MAX));
            // The extreme: every product at its largest.
            let q = vec![max_code; dim];
            let c = vec![127i8; dim];
            assert_eq!(
                i64::from(dot_i16_i8(&q, &c)),
                i64::from(max_code) * 127 * dim as i64
            );
            let c = vec![-127i8; dim];
            assert_eq!(
                i64::from(dot_i16_i8(&q, &c)),
                -i64::from(max_code) * 127 * dim as i64
            );
            // And against the plain f32 route.
            let v = unit(3, dim);
            let x = unit(4, dim);
            let (qc, qs) = quantize_i16(&v, max_code);
            let mut codes = Vec::new();
            let scale = quantize_i8(&x, &mut codes);
            let wide: i64 = qc
                .iter()
                .zip(&codes)
                .map(|(&a, &b)| i64::from(a) * i64::from(b))
                .sum();
            assert_eq!(i64::from(dot_i16_i8(&qc, &codes)), wide);
            assert!(qc.iter().all(|c| c.abs() <= max_code));
            if dim <= 4096 {
                let estimate = dot_i16_i8(&qc, &codes) as f32 * qs * scale;
                assert!((estimate - dot(&v, &x)).abs() < 0.03, "dim {dim}");
            }
        }
        assert_eq!(quantize_i16(&[0.0; 3], 100), (vec![0; 3], 0.0));
        assert_eq!(quantize_i16(&[1.0, f32::NAN], 100), (vec![0; 2], 0.0));
        assert_eq!(quantize_i16(&[0.5, -1.0], 100), (vec![50, -100], 0.01));
    }

    #[test]
    fn zero_and_non_finite_vectors_quantize_to_zero() {
        let mut codes = Vec::new();
        assert_eq!(quantize_i8(&[0.0; 4], &mut codes), 0.0);
        assert_eq!(codes, vec![0; 4]);
        assert_eq!(dot_i8(&[1.0; 4], &codes, 0.0), 0.0);
        let scale = quantize_i8(&[f32::NAN, 0.5, f32::INFINITY, -0.25], &mut codes);
        assert_eq!(codes, vec![0, 127, 0, -64]);
        assert!((scale - 0.5 / 127.0).abs() < 1e-9);
    }
}
