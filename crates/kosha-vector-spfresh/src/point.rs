//! Cosine distance and the dot-product primitives behind it. Accumulation
//! is chunked into independent lanes (see [`dot`]) rather than one serial
//! `.sum()` chain, so results differ from the historical
//! `kosha_segment::CosinePoint::distance` in the last float bits — nothing
//! persisted or compared across versions depends on bit-exact scores, and
//! the HNSW path this was once kept bit-compatible with is gone (#126).

/// Dot product with 8 independent accumulator lanes. A plain
/// `.zip().map().sum()` compiles to a serial dependency chain (rustc won't
/// reassociate f32 addition), leaving SIMD units idle in the hottest loop
/// of both kNN scoring and k-means; independent lanes let LLVM vectorize.
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "dot: dimension mismatch");
    let mut acc = [0.0f32; 8];
    let ca = a.chunks_exact(8);
    let cb = b.chunks_exact(8);
    let (ra, rb) = (ca.remainder(), cb.remainder());
    for (xa, xb) in ca.zip(cb) {
        for l in 0..8 {
            acc[l] += xa[l] * xb[l];
        }
    }
    let mut s = ((acc[0] + acc[1]) + (acc[2] + acc[3])) + ((acc[4] + acc[5]) + (acc[6] + acc[7]));
    for (x, y) in ra.iter().zip(rb.iter()) {
        s += x * y;
    }
    s
}

/// Scale `v` to unit L2 norm in place. A zero vector is left untouched —
/// downstream dot scoring then yields similarity `0`, matching
/// `cosine_distance`'s "zero-norm is maximally distant" convention.
pub fn normalize_in_place(v: &mut [f32]) {
    let n = dot(v, v).sqrt();
    if n > 0.0 {
        let inv = 1.0 / n;
        for x in v.iter_mut() {
            *x *= inv;
        }
    }
}

/// `1.0 - clamp(dot(a,b) / (||a|| * ||b||), -1, 1)`; a zero-norm vector is
/// defined to be maximally distant (`1.0`) from everything.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "cosine_distance: dimension mismatch");
    let d = dot(a, b);
    let na = dot(a, a).sqrt();
    let nb = dot(b, b).sqrt();
    if na == 0.0 || nb == 0.0 {
        return 1.0;
    }
    1.0 - (d / (na * nb)).clamp(-1.0, 1.0)
}

/// `1.0 - cosine_distance(a, b)`.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    1.0 - cosine_distance(a, b)
}

/// Component-wise arithmetic mean of a set of vectors. Used for centroid
/// (re)computation everywhere in this crate. `cosine_distance` normalizes
/// both operands at compare time, so the centroid's raw magnitude doesn't
/// bias results — a plain mean is an adequate stand-in for a true "cosine
/// centroid" here (see README.md).
pub(crate) fn mean_vector<'a>(vectors: impl Iterator<Item = &'a [f32]>, dim: usize) -> Vec<f32> {
    let mut sum = vec![0.0f32; dim];
    let mut count = 0usize;
    for v in vectors {
        for (s, x) in sum.iter_mut().zip(v.iter()) {
            *s += x;
        }
        count += 1;
    }
    if count > 0 {
        let inv = 1.0 / count as f32;
        for s in sum.iter_mut() {
            *s *= inv;
        }
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_vectors_have_zero_distance() {
        let a = [1.0, 2.0, 3.0];
        assert!(cosine_distance(&a, &a) < 1e-6);
    }

    #[test]
    fn orthogonal_vectors_have_unit_distance() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        assert!((cosine_distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn zero_norm_vector_is_maximally_distant() {
        let a = [0.0, 0.0];
        let b = [1.0, 1.0];
        assert_eq!(cosine_distance(&a, &b), 1.0);
    }

    #[test]
    fn mean_vector_computes_componentwise_average() {
        let vs: Vec<Vec<f32>> = vec![vec![1.0, 2.0], vec![3.0, 4.0]];
        let refs: Vec<&[f32]> = vs.iter().map(|v| v.as_slice()).collect();
        let m = mean_vector(refs.into_iter(), 2);
        assert_eq!(m, vec![2.0, 3.0]);
    }
}
