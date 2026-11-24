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
    let (ca, ra) = a.as_chunks::<8>();
    let (cb, rb) = b.as_chunks::<8>();
    for (xa, xb) in ca.iter().zip(cb.iter()) {
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

/// [`dot`] with `L` independent accumulator lanes (`L` a power of two),
/// reduced pairwise. Experimental (#382): used only when
/// `KOSHA_VECTOR_DOT_LANES` is set to 16/32/64 (kosha-segment's posting
/// scans), and by `benches/distance_kernels.rs`, to measure whether a
/// wider accumulator helps. With AVX2 + FMA (`-C target-cpu=x86-64-v3`) one
/// 8-lane accumulator is a single register, so every iteration waits on
/// the previous FMA (~4 cycles latency, 2 per cycle throughput); 32-64
/// lanes give the core enough independent chains. Summation order differs
/// from [`dot`], so results differ in the last float bits.
pub fn dot_lanes<const L: usize>(a: &[f32], b: &[f32]) -> f32 {
    const { assert!(L.is_power_of_two(), "dot_lanes: L must be a power of two") };
    debug_assert_eq!(a.len(), b.len(), "dot_lanes: dimension mismatch");
    let mut acc = [0.0f32; L];
    let (ca, ra) = a.as_chunks::<L>();
    let (cb, rb) = b.as_chunks::<L>();
    for (xa, xb) in ca.iter().zip(cb.iter()) {
        for l in 0..L {
            acc[l] += xa[l] * xb[l];
        }
    }
    let mut width = L;
    while width > 1 {
        width /= 2;
        for l in 0..width {
            acc[l] += acc[l + width];
        }
    }
    let mut s = acc[0];
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

/// Convert half the angular separation into cosine-distance units. Stay
/// away from the boundary by a margin for f32 dot products and normalization.
pub(crate) fn elkan_cutoff(nearest_distance: f32, dim: usize) -> f32 {
    1.0 - (1.0 - 0.5 * nearest_distance).max(0.0).sqrt() - (dim as f32 + 8.0) * (4.0 * f32::EPSILON)
}

/// Component-wise arithmetic mean of a set of vectors. Used for centroid
/// (re)computation everywhere in this crate. `cosine_distance` normalizes
/// both operands at compare time, so the centroid's raw magnitude doesn't
/// bias results — a plain mean is an adequate stand-in for a true "cosine
/// centroid" here (see README.md).
pub fn mean_vector<'a>(vectors: impl Iterator<Item = &'a [f32]>, dim: usize) -> Vec<f32> {
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

/// Brute-force late-interaction score (ColBERT MaxSim): for each query
/// token, the best [`cosine_similarity`] against any document token,
/// summed. The reference the segment index is tested against (#205).
pub fn maxsim<Q: AsRef<[f32]>, D: AsRef<[f32]>>(query: &[Q], doc: &[D]) -> f32 {
    query
        .iter()
        .map(|q| {
            doc.iter()
                .map(|d| cosine_similarity(q.as_ref(), d.as_ref()))
                .fold(f32::NEG_INFINITY, f32::max)
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dot_lanes_matches_dot_up_to_rounding() {
        let mut rng = crate::DeterministicRng::new(11);
        for dim in [0, 1, 7, 8, 15, 33, 100, 384, 1024, 1027] {
            let a: Vec<f32> = (0..dim).map(|_| rng.next_f32_range(-1.0, 1.0)).collect();
            let b: Vec<f32> = (0..dim).map(|_| rng.next_f32_range(-1.0, 1.0)).collect();
            let exact: f64 = a.iter().zip(&b).map(|(x, y)| *x as f64 * *y as f64).sum();
            let tol = 1e-5 * (1.0 + dim as f64).sqrt();
            for got in [
                dot(&a, &b),
                dot_lanes::<1>(&a, &b),
                dot_lanes::<8>(&a, &b),
                dot_lanes::<16>(&a, &b),
                dot_lanes::<32>(&a, &b),
                dot_lanes::<64>(&a, &b),
            ] {
                assert!(
                    (got as f64 - exact).abs() <= tol,
                    "dim {dim}: {got} vs {exact}"
                );
            }
        }
    }

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
