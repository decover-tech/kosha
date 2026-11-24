//! Offline centroid shortlist experiment (#461), not a search mode.

use super::*;
use kosha_segment::{centroid_probe_score, KnnCentroidMatrix};
use kosha_vector_spfresh::{
    encode_rabitq, RaBitQCode, RaBitQEstimator, RaBitQQuery, RaBitQRotation,
};

pub(super) struct CentroidCodes {
    bits: Vec<u8>,
    metadata: Vec<RaBitQCode>,
    radii: Vec<f32>,
}

impl CentroidCodes {
    pub(super) fn new(rotation: &RaBitQRotation, centroids: &[f32], radii: Vec<f32>) -> Self {
        assert_eq!(centroids.len(), rotation.dim() * radii.len());
        let origin = vec![0.0; rotation.dim()];
        let mut bits = Vec::with_capacity(rotation.code_bytes() * radii.len());
        let mut scratch = Vec::new();
        let metadata = centroids
            .chunks_exact(rotation.dim())
            .map(|row| {
                let code = encode_rabitq(rotation, row, &origin, &mut scratch);
                bits.extend_from_slice(&scratch);
                code
            })
            .collect();
        Self {
            bits,
            metadata,
            radii,
        }
    }

    fn scores<'a>(
        &'a self,
        query: &'a RaBitQQuery,
        stride: usize,
        alpha: f32,
    ) -> impl Iterator<Item = f32> + 'a {
        self.bits
            .chunks_exact(stride)
            .zip(self.metadata.iter().zip(&self.radii))
            .map(move |(bits, (code, radius))| {
                let (dot, margin) = query.estimate(code, bits, 0.0);
                // Favor postings that could have a small radius-adjusted score.
                // This is a confidence margin, not a guaranteed bound.
                centroid_probe_score(1.0 - (dot + margin).clamp(-1.0, 1.0), *radius, alpha)
            })
    }
}

fn rank(
    sources: &[MatrixSource<'_>],
    codes: &[CentroidCodes],
    rotation: &RaBitQRotation,
    budget: usize,
    shortlist: usize,
) -> KnnRankedPostings {
    let first = &sources[0];
    let mut unit = first.query.to_vec();
    kosha_vector_spfresh::normalize_in_place(&mut unit);
    let query = RaBitQQuery::with_estimator(rotation, &unit, RaBitQEstimator::Popcount4);
    let stride = rotation.code_bytes();
    let mut candidates: Vec<_> = codes
        .par_iter()
        .enumerate()
        .flat_map_iter(|(s, codes)| {
            codes
                .scores(&query, stride, first.alpha)
                .enumerate()
                .map(move |(i, score)| (score, s, i as u32))
        })
        .collect();
    if shortlist < candidates.len() {
        candidates.select_nth_unstable_by(shortlist, |a, b| {
            a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2))
        });
        candidates.truncate(shortlist);
    }

    let mut by_source = vec![Vec::new(); sources.len()];
    for (_, s, posting) in candidates {
        by_source[s].push(posting);
    }
    let scored: Vec<Vec<_>> = sources
        .par_iter()
        .zip(by_source)
        .map(|(source, mut postings)| {
            postings.sort_unstable(); // walk the f32 rows in posting order
            let scores = source
                .matrix
                .scores_for(source.query, &postings, source.alpha);
            postings.into_iter().zip(scores).collect()
        })
        .collect();
    let mut all: Vec<f32> = scored.iter().flatten().map(|&(_, score)| score).collect();
    let exact_centroids = all.len();
    let total = sources.iter().map(KnnPlanSource::postings).sum();
    let (cutoff, rule) = knn_budget_cutoff_of(&mut all, total, budget, None);
    let rankings = sources
        .par_iter()
        .zip(scored)
        .map(|(source, scores)| {
            Some(KnnSegmentRanking::from_candidates(
                source.postings(),
                &scores,
                cutoff,
                budget,
            ))
        })
        .collect();
    KnnRankedPostings {
        cutoff,
        rule,
        rankings,
        exact_centroids,
    }
}

fn overlap(reference: &KnnRankedPostings, trial: &KnnRankedPostings) -> f64 {
    let (mut matched, mut expected) = (0, 0);
    for (a, b) in reference.rankings.iter().zip(&trial.rankings) {
        let a = a.as_ref().unwrap().budgeted();
        let b: HashSet<_> = b.as_ref().unwrap().budgeted().iter().copied().collect();
        expected += a.len();
        matched += a.iter().filter(|posting| b.contains(posting)).count();
    }
    100.0 * matched as f64 / expected as f64
}

pub(super) fn measure(
    rotation: &RaBitQRotation,
    matrices: &[KnnCentroidMatrix],
    codes: &[CentroidCodes],
    queries: &[Vec<f32>],
    alpha: f32,
    budget: usize,
    iters: usize,
) {
    let variants = [
        "f32",
        "i8 + exact",
        "1-bit m=1000",
        "1-bit m=5000",
        "1-bit m=20000",
    ];
    let code_bytes: usize = codes
        .iter()
        .map(|c| c.bits.len() + std::mem::size_of_val(c.metadata.as_slice()))
        .sum();
    println!("RaBitQ codes + metadata: {:.2} MB", code_bytes as f64 / 1e6);
    let mut samples = variants.map(|_| Vec::new());
    let mut overlaps = variants.map(|_| Vec::new());
    for iter in 0..=iters {
        for (q, query) in queries.iter().enumerate() {
            let sources: Vec<_> = matrices
                .iter()
                .map(|matrix| MatrixSource {
                    matrix,
                    query,
                    alpha,
                })
                .collect();
            let mut plans = variants.map(|_| None);
            for offset in 0..variants.len() {
                let slot = (offset + iter + q) % variants.len();
                let start = Instant::now();
                let plan = match slot {
                    0 | 1 => knn_rank_postings(&sources, budget, None, slot == 1),
                    _ => rank(
                        &sources,
                        codes,
                        rotation,
                        budget,
                        [1000, 5000, 20000][slot - 2],
                    ),
                };
                if iter > 0 {
                    samples[slot].push(start.elapsed().as_secs_f64() * 1e3);
                }
                plans[slot] = Some(plan);
            }
            let plans = plans.map(Option::unwrap);
            assert_eq!(view(&plans[0]), view(&plans[1]), "i8 plan differs from f32");
            if iter == 0 {
                for (slot, plan) in plans.iter().enumerate() {
                    overlaps[slot].push(overlap(&plans[0], plan));
                }
            }
        }
    }
    for (slot, name) in variants.iter().enumerate() {
        let times = &mut samples[slot];
        times.sort_unstable_by(f64::total_cmp);
        let p50 = times[times.len().div_ceil(2) - 1];
        let p99 = times[(99 * times.len()).div_ceil(100) - 1];
        let min_overlap = overlaps[slot].iter().copied().fold(100.0, f64::min);
        let mean_overlap = overlaps[slot].iter().sum::<f64>() / overlaps[slot].len() as f64;
        println!("{name:<14} p50 {p50:6.2} ms  p99 {p99:6.2} ms  overlap mean {mean_overlap:6.2}%  min {min_overlap:6.2}%");
    }
}

#[test]
fn full_shortlist_preserves_exact_planning_with_ties() {
    let centroids = [1.0, 0.0, 0.0, 1.0, 0.0, 1.0];
    let radii = vec![0.0, 1.0, 1.0];
    let rotation = RaBitQRotation::new(2);
    let matrix = KnnCentroidMatrix::new(2, &centroids, &radii);
    let sources = [MatrixSource {
        matrix: &matrix,
        query: &[1.0, 0.0],
        alpha: 1.0,
    }];
    let codes = [CentroidCodes::new(&rotation, &centroids, radii)];
    let exact = knn_rank_postings(&sources, 2, None, false);
    let full = rank(&sources, &codes, &rotation, 2, 3);
    assert_eq!(view(&exact), view(&full));
    let short = rank(&sources, &codes, &rotation, 2, 1);
    assert_eq!(short.exact_centroids, 1);
    assert_eq!(overlap(&exact, &short), 50.0);
}

/// Same synthetic corpus, sizing knobs and planning phase as the i8
/// microbench; also measures posting-set overlap for the three shortlists.
#[test]
#[ignore = "a timing harness, run by hand in release mode"]
fn knn_plan_rabitq_microbench() {
    assert!(
        kosha_segment::knn_plan_i8_from_env(),
        "enable i8 for the baseline"
    );
    run_plan_microbench(true);
}
