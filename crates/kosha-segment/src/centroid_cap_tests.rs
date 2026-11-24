//! The centroid cap's score floor (#253): it must never be above any
//! posting's rank score, whatever the centroids, the query, the radii or
//! the radius weight. A violation would let the kNN plan leave out a
//! segment that holds a posting inside the probe budget.

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
}

fn unit(mut v: Vec<f32>) -> Vec<f32> {
    kosha_vector_spfresh::normalize_in_place(&mut v);
    v
}

fn index(dim: usize, centroids: Vec<Vec<f32>>, radii: &[f32]) -> LazyVectorIndex {
    let parsed = centroids
        .into_iter()
        .zip(radii)
        .map(|(centroid, &radius)| VectorPostingIndex {
            centroid,
            offset: 0,
            length: 0,
            radius,
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

/// `n` unit centroids within roughly `spread` of `center` (per coordinate,
/// before renormalizing).
fn cluster(center: &[f32], spread: f32, n: usize, rng: &mut Lcg) -> Vec<Vec<f32>> {
    (0..n)
        .map(|_| unit(center.iter().map(|&c| c + rng.coord() * spread).collect()))
        .collect()
}

const ALPHAS: [f32; 5] = [0.0, 0.5, 1.0, 2.5, 1e9];

#[test]
fn the_floor_is_never_above_a_score() {
    let mut rng = Lcg(0x253);
    let (mut checked, mut positive) = (0usize, 0usize);
    for dim in [1usize, 2, 3, 8, 9, 64, 257, 1024] {
        for spread in [0.0f32, 1e-4, 0.05, 0.3, 1.0, 10.0] {
            let center = unit((0..dim).map(|_| rng.coord()).collect());
            let mut centroids = cluster(&center, spread, 24, &mut rng);
            // Not quite unit (the writer renormalizes, but nothing depends
            // on it).
            centroids.push(centroids[0].iter().map(|x| x * 1.3).collect());
            centroids.push(centroids[1].iter().map(|x| x * 0.6).collect());
            let radii: Vec<f32> = (0..centroids.len())
                .map(|i| match i % 7 {
                    0 => 0.0,
                    1 => 2.5,
                    2 => -0.25,
                    _ => (rng.coord() + 1.0) * 0.5,
                })
                .collect();
            let vi = index(dim, centroids.clone(), &radii);
            let Some(cap) = vi.centroid_cap.as_ref() else {
                panic!("dim {dim}, spread {spread}: sane centroids get a cap");
            };
            assert!(cap.theta >= 0.0);
            let mut queries: Vec<Vec<f32>> = vec![
                vec![0.0; dim],
                vec![1.0; dim],
                center.clone(),
                center.iter().map(|x| -x).collect(),
                center.iter().map(|x| -x * 1e6).collect(),
                center.iter().map(|x| x * 1e-20).collect(),
            ];
            for c in centroids.iter().step_by(5) {
                queries.push(c.clone());
                queries.push(c.iter().map(|x| -x).collect());
            }
            for _ in 0..10 {
                queries.push((0..dim).map(|_| rng.coord() * 10.0).collect());
            }
            for (q, query) in queries.iter().enumerate() {
                for alpha in ALPHAS {
                    let floor = SegmentReader::centroid_score_floor(&vi, query, alpha)
                        .unwrap_or_else(|| {
                            panic!("dim {dim}, query {q}: finite query has a floor")
                        });
                    assert!(floor >= 0.0);
                    let exact = SegmentReader::centroid_scores(&vi, query, alpha);
                    for (i, &s) in exact.iter().enumerate() {
                        assert!(
                            floor <= s,
                            "dim {dim}, spread {spread}, query {q}, alpha {alpha}, posting {i}: \
                             floor {floor} > score {s}"
                        );
                        checked += 1;
                    }
                    if floor > 0.0 {
                        positive += 1;
                    }
                }
            }
        }
    }
    assert!(checked > 50_000, "{checked} scores checked");
    assert!(positive > 0, "some floors rule a segment out");
}

#[test]
fn a_tight_cluster_far_from_the_query_has_a_useful_floor() {
    let mut rng = Lcg(0x1024);
    let dim = 1024;
    let center = unit((0..dim).map(|_| rng.coord()).collect());
    let centroids = cluster(&center, 0.01, 64, &mut rng);
    let vi = index(dim, centroids, &[0.2; 64]);
    let far: Vec<f32> = center.iter().map(|x| -x).collect();
    let exact = SegmentReader::centroid_scores(&vi, &far, 1.0);
    let best = exact.iter().copied().fold(f32::INFINITY, f32::min);
    let floor = SegmentReader::centroid_score_floor(&vi, &far, 1.0).unwrap();
    assert!(floor <= best);
    assert!(floor > best - 0.05, "floor {floor}, best score {best}");
    // A query inside the cap gets no useful floor.
    assert_eq!(
        SegmentReader::centroid_score_floor(&vi, &center, 1.0),
        Some(0.0)
    );
}

#[test]
fn no_floor_without_a_guarantee() {
    let dim = 8;
    let good = unit(vec![1.0; dim]);
    for bad in [
        vec![f32::NAN; dim],
        vec![f32::INFINITY; dim],
        vec![0.0; dim],
        vec![3.0; dim],
    ] {
        let vi = index(dim, vec![good.clone(), bad], &[0.1, 0.1]);
        assert!(vi.centroid_cap.is_none());
        assert!(SegmentReader::centroid_score_floor(&vi, &good, 1.0).is_none());
    }
    for radius in [f32::NAN, f32::INFINITY] {
        let vi = index(dim, vec![good.clone()], &[radius]);
        assert!(vi.centroid_cap.is_none());
    }
    // Centroids that cancel out have no centre.
    let opposite: Vec<f32> = good.iter().map(|x| -x).collect();
    let vi = index(dim, vec![good.clone(), opposite], &[0.1, 0.1]);
    assert!(vi.centroid_cap.is_none());

    let vi = index(dim, vec![good.clone(), good.clone()], &[0.1, 0.1]);
    assert!(SegmentReader::centroid_score_floor(&vi, &good, 1.0).is_some());
    for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        let mut query = good.clone();
        query[3] = bad;
        assert!(SegmentReader::centroid_score_floor(&vi, &query, 1.0).is_none());
    }
    for alpha in [f32::NAN, f32::INFINITY, -1.0] {
        assert!(SegmentReader::centroid_score_floor(&vi, &good, alpha).is_none());
    }
    // A column that is not unit-norm is scored with the full cosine and
    // gets no cap.
    let parsed = vec![VectorPostingIndex {
        centroid: good.clone(),
        offset: 0,
        length: 0,
        radius: 0.0,
    }];
    let legacy = LazyVectorIndex::from_parsed(
        Path::new("vector.idx"),
        0,
        dim as u32,
        VECTOR_INDEX_VERSION,
        parsed,
        None,
    );
    assert!(!legacy.unit_norm && legacy.centroid_cap.is_none());
    assert!(SegmentReader::centroid_score_floor(&legacy, &good, 1.0).is_none());
}
