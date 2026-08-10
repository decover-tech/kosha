//! Cosine distance — deliberately matches `kosha_segment::CosinePoint::distance`
//! bit-for-bit so this crate's behavior is directly comparable to kosha's
//! current HNSW path (see the benches).

/// `1.0 - clamp(dot(a,b) / (||a|| * ||b||), -1, 1)`; a zero-norm vector is
/// defined to be maximally distant (`1.0`) from everything, matching
/// kosha-segment's `CosinePoint::distance`.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "cosine_distance: dimension mismatch");
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 {
        return 1.0;
    }
    1.0 - (dot / (na * nb)).clamp(-1.0, 1.0)
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
