//! i8 centroid codes for two-stage kNN planning: the score bounds they
//! give must hold for every posting, whatever the centroid, the query, the
//! radius or the radius weight.
//!
//! The bounds are compared with [`SegmentReader::centroid_scores`], the
//! f32 scores the plan ranks by. A violation here would let the plan drop
//! a posting that belongs inside the probe budget.

use super::*;

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }

    /// Uniform in `[-1, 1)`.
    fn coord(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 23) as f32 - 1.0
    }

    fn below(&mut self, n: usize) -> usize {
        ((self.next() >> 33) as usize) % n
    }
}

fn unit(mut v: Vec<f32>) -> Vec<f32> {
    kosha_vector_spfresh::normalize_in_place(&mut v);
    v
}

/// A unit-norm lazy index over `centroids` with the given radii; nothing
/// is read from disk by the centroid scoring under test.
fn index(dim: usize, centroids: Vec<Vec<f32>>, radii: &[f32]) -> LazyVectorIndex {
    let parsed = centroids
        .into_iter()
        .zip(radii)
        .map(|(centroid, &radius)| VectorPostingIndex {
            centroid,
            offset: 0,
            length: 0,
            radius,
            radius_quantiles: None,
        })
        .collect();
    LazyVectorIndex::from_parsed(
        Path::new("vector.idx"),
        0,
        dim as u32,
        RADIUS_AWARE_VECTOR_INDEX_VERSION,
        parsed,
        Some("emb".into()),
    )
}

/// Centroids chosen to stress the quantizer: one dominant component (the
/// scale is set by it and every other component rounds coarsely), equal
/// components, a single non-zero, the zero vector, components straddling
/// half a quantization step, and plain random directions.
fn adversarial_centroids(dim: usize, rng: &mut Lcg) -> Vec<Vec<f32>> {
    let mut out: Vec<Vec<f32>> = Vec::new();
    // One large component, the rest small.
    for small in [1e-1f32, 1e-2, 3.9e-3, 4.0e-3, 1e-4] {
        let mut v: Vec<f32> = (0..dim).map(|_| rng.coord() * small).collect();
        let at = rng.below(dim);
        v[at] = 1.0;
        out.push(unit(v));
    }
    // All components equal, both signs, and alternating.
    out.push(unit(vec![1.0; dim]));
    out.push(unit(vec![-1.0; dim]));
    out.push(unit(
        (0..dim)
            .map(|i| if i % 2 == 0 { 1.0 } else { -1.0 })
            .collect(),
    ));
    // One-hot, each sign.
    for sign in [1.0f32, -1.0] {
        let mut v = vec![0.0; dim];
        v[rng.below(dim)] = sign;
        out.push(v);
    }
    // The zero vector (scale 0, all-zero codes).
    out.push(vec![0.0; dim]);
    // Every component exactly half a step from two codes.
    let mut half: Vec<f32> = (0..dim).map(|i| (i % 5) as f32 + 0.5).collect();
    half[0] = 127.0;
    out.push(unit(half));
    // Not quite unit (the writer renormalizes, but nothing depends on it).
    out.push(unit(vec![1.0; dim]).iter().map(|x| x * 1.3).collect());
    out.push(
        unit((0..dim).map(|_| rng.coord()).collect())
            .iter()
            .map(|x| x * 0.4)
            .collect(),
    );
    // Tiny components next to denormals.
    let mut tiny: Vec<f32> = (0..dim).map(|_| rng.coord() * 1e-30).collect();
    tiny[0] = 1.0;
    out.push(tiny);
    for _ in 0..40 {
        out.push(unit((0..dim).map(|_| rng.coord()).collect()));
    }
    // Sparse: a few non-zeros.
    for _ in 0..6 {
        let mut v = vec![0.0; dim];
        for _ in 0..3 {
            v[rng.below(dim)] = rng.coord();
        }
        out.push(unit(v));
    }
    out
}

fn adversarial_queries(dim: usize, centroids: &[Vec<f32>], rng: &mut Lcg) -> Vec<Vec<f32>> {
    let mut out: Vec<Vec<f32>> = Vec::new();
    out.push(vec![0.0; dim]);
    out.push(vec![1.0; dim]);
    out.push(vec![-3.5; dim]);
    for sign in [1.0f32, -1e6, 1e-20] {
        let mut v = vec![0.0; dim];
        v[rng.below(dim)] = sign;
        out.push(v);
    }
    let mut spike: Vec<f32> = (0..dim).map(|_| rng.coord() * 1e-3).collect();
    spike[0] = 5.0;
    out.push(spike);
    // A query equal to, and opposite to, stored centroids: distances at
    // the clamp.
    for c in centroids.iter().step_by(7) {
        out.push(c.clone());
        out.push(c.iter().map(|x| -x).collect());
    }
    for _ in 0..12 {
        out.push((0..dim).map(|_| rng.coord() * 10.0).collect());
    }
    out
}

const ALPHAS: [f32; 5] = [0.0, 0.5, 1.0, 2.5, 1e9];

#[test]
fn the_bounds_hold_on_adversarial_centroids_and_queries() {
    let mut rng = Lcg(0x18);
    let (mut checked, mut width, mut resolved) = (0usize, 0f64, 0usize);
    for dim in [1usize, 2, 7, 8, 9, 31, 64, 257, 1024] {
        let centroids = adversarial_centroids(dim, &mut rng);
        // Radii from none to larger than any distance, and a couple the
        // score function has to survive rather than make sense of.
        let radii: Vec<f32> = (0..centroids.len())
            .map(|i| match i % 9 {
                0 => 0.0,
                1 => 2.5,
                2 => -0.25,
                3 => f32::INFINITY,
                4 => f32::NAN,
                _ => (rng.coord() + 1.0) * 0.5,
            })
            .collect();
        let vi = index(dim, centroids.clone(), &radii);
        assert!(vi.centroid_codes.is_some(), "dim {dim}: codes are built");
        for (q, query) in adversarial_queries(dim, &centroids, &mut rng)
            .iter()
            .enumerate()
        {
            for alpha in ALPHAS {
                let exact = SegmentReader::centroid_scores(&vi, query, alpha);
                let bounds = SegmentReader::centroid_score_bounds(&vi, query, alpha)
                    .unwrap_or_else(|| panic!("dim {dim}, query {q}: finite query has bounds"));
                assert_eq!(bounds.lower.len(), exact.len());
                assert_eq!(bounds.upper.len(), exact.len());
                for (i, &s) in exact.iter().enumerate() {
                    let (lo, hi) = (bounds.lower[i], bounds.upper[i]);
                    assert!(
                        lo <= s && s <= hi,
                        "dim {dim}, query {q}, alpha {alpha}, posting {i} (radius {}): \
                         {lo} <= {s} <= {hi} does not hold",
                        radii[i]
                    );
                    // What stage two relies on to skip the f32 score.
                    if lo == 0.0 && hi == 0.0 {
                        assert_eq!(s.to_bits(), 0f32.to_bits(), "a resolved zero is +0.0");
                        resolved += 1;
                    }
                    checked += 1;
                    if hi.is_finite() && alpha == 0.0 {
                        width += f64::from(hi - lo);
                    }
                }
                // The scores of a few postings are the full scan's, bit
                // for bit.
                let some: Vec<u32> = (0..exact.len() as u32).rev().step_by(3).collect();
                let scores = SegmentReader::centroid_scores_at(&vi, query, &some, alpha);
                assert_eq!(scores.len(), some.len());
                for (&i, score) in some.iter().zip(scores) {
                    assert_eq!(score.to_bits(), exact[i as usize].to_bits());
                }
            }
        }
    }
    assert!(checked > 50_000, "{checked} bounds checked");
    assert!(resolved > 0, "some postings were certain to clamp to zero");
    assert!(width > 0.0);
}

#[test]
fn the_bounds_are_tight_enough_to_be_useful_at_1024_dimensions() {
    // The error of a dense unit query against a dense unit centroid is
    // about `scale * sqrt(dim / 12)`: under 0.02 at 1024 dimensions, where
    // centroid distances spread over tenths.
    let mut rng = Lcg(0x1024);
    let dim = 1024;
    let centroids: Vec<Vec<f32>> = (0..64)
        .map(|_| unit((0..dim).map(|_| rng.coord()).collect()))
        .collect();
    let vi = index(dim, centroids, &[0.0; 64]);
    let query: Vec<f32> = (0..dim).map(|_| rng.coord()).collect();
    let bounds = SegmentReader::centroid_score_bounds(&vi, &query, 0.0).unwrap();
    let widest = bounds
        .lower
        .iter()
        .zip(&bounds.upper)
        .map(|(lo, hi)| hi - lo)
        .fold(0f32, f32::max);
    assert!(widest < 0.04, "widest interval {widest}");
}

#[test]
fn no_bounds_without_a_guarantee() {
    let dim = 8;
    let good = unit(vec![1.0; dim]);
    // A non-finite or far-from-unit centroid: no codes, so no bounds.
    for bad in [
        vec![f32::NAN; dim],
        vec![f32::INFINITY; dim],
        vec![1e6; dim],
        vec![3.0; dim],
    ] {
        let vi = index(dim, vec![good.clone(), bad], &[0.1, 0.1]);
        assert!(vi.centroid_codes.is_none());
        assert!(SegmentReader::centroid_score_bounds(&vi, &good, 1.0).is_none());
    }
    let vi = index(dim, vec![good.clone(), good.clone()], &[0.1, 0.1]);
    assert!(SegmentReader::centroid_score_bounds(&vi, &good, 1.0).is_some());
    // A query that is not finite, or a weight the score is not defined
    // for.
    for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        let mut query = good.clone();
        query[3] = bad;
        assert!(SegmentReader::centroid_score_bounds(&vi, &query, 1.0).is_none());
    }
    for alpha in [f32::NAN, f32::INFINITY, -1.0] {
        assert!(SegmentReader::centroid_score_bounds(&vi, &good, alpha).is_none());
    }
    // A column that is not unit-norm is never coded.
    let parsed = vec![VectorPostingIndex {
        centroid: good.clone(),
        offset: 0,
        length: 0,
        radius: 0.0,
        radius_quantiles: None,
    }];
    let legacy = LazyVectorIndex::from_parsed(
        Path::new("vector.idx"),
        0,
        dim as u32,
        VECTOR_INDEX_VERSION,
        parsed,
        None,
    );
    assert!(!legacy.unit_norm && legacy.centroid_codes.is_none());
    assert!(SegmentReader::centroid_score_bounds(&legacy, &good, 1.0).is_none());
}

#[test]
fn a_ranking_from_candidates_is_the_ranking_from_every_score() {
    let mut rng = Lcg(0x77);
    for round in 0..400 {
        let n = 1 + rng.below(60);
        // Few distinct values, so ties (and ties at the cutoff) are the
        // rule.
        let scores: Vec<f32> = (0..n)
            .map(|_| match rng.below(4) {
                0 => 0.0,
                1 => 0.25,
                _ => (rng.below(8) as f32) / 8.0,
            })
            .collect();
        let cutoff = match rng.below(5) {
            0 => f32::INFINITY,
            1 => f32::NEG_INFINITY,
            _ => scores[rng.below(n)],
        };
        let max_probes = match rng.below(4) {
            0 => 0,
            1 => usize::MAX,
            _ => rng.below(n + 2),
        };
        let full = KnnSegmentRanking::new(scores.clone(), cutoff, max_probes);
        // Every posting at or under the cutoff, plus some that are not.
        let candidates: Vec<(u32, f32)> = scores
            .iter()
            .enumerate()
            .filter(|(_, &s)| s <= cutoff || rng.below(3) == 0)
            .map(|(i, &s)| (i as u32, s))
            .collect();
        let partial = KnnSegmentRanking::from_candidates(n, &candidates, cutoff, max_probes);
        assert_eq!(partial.budgeted(), full.budgeted(), "round {round}");
        assert_eq!(partial.postings(), full.postings());
        assert_eq!(partial.max_probes(), full.max_probes());
        assert_eq!(
            partial.max_distance().to_bits(),
            full.max_distance().to_bits()
        );
        assert!(partial.bounds().is_none() && full.bounds().is_some());
    }
}

#[test]
fn code_bytes_are_what_the_codes_hold() {
    // The accounting helper reads only the sidecar header; it must agree
    // with what a load actually allocates.
    let dir = std::env::temp_dir().join(format!("kosha-centroid-codes-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let mut writer = SegmentWriter::new(kosha_core::SegmentId("codes".into()), dir.clone());
    let mut rng = Lcg(5);
    for i in 0..600 {
        let v: Vec<f32> = (0..24).map(|_| rng.coord()).collect();
        writer.add_document(
            kosha_core::DocumentId(format!("d{i}")),
            vec![kosha_core::Field::vector("emb", v)],
        );
    }
    writer.finalize(kosha_core::Bm25Params::default()).unwrap();
    let reader = SegmentReader::open_with_options(dir.clone(), true).unwrap();
    let index = reader.vector_field("emb").unwrap().unwrap();
    let vi = index.lazy.as_ref().unwrap();
    let codes = vi
        .centroid_codes
        .as_ref()
        .expect("a unit-norm column is coded");
    let held =
        codes.codes.len() + 4 * (codes.scales.len() + codes.err_l2.len() + codes.err_linf.len());
    assert_eq!(codes.codes.len(), vi.centroids.len());
    assert_eq!(
        vector_centroid_code_bytes(&dir.join(kosha_core::vector_offsets_file(0))),
        held as u64
    );
    assert_eq!(vector_centroid_code_bytes(&dir.join("missing.offsets")), 0);
    assert_eq!(reader.knn_centroid_count("emb", 24), Some(vi.ranges.len()));
    assert_eq!(reader.knn_centroid_count("emb", 25), None);
    assert_eq!(reader.knn_centroid_count("nope", 24), None);
    let _ = fs::remove_dir_all(&dir);
}
