//! Segment pruning in kNN planning (`KOSHA_KNN_SEGMENT_PRUNE`): leaving
//! out, unscored, the segments whose score floor shows none of their
//! postings can be inside the probe budget must give the plan that
//! planning every segment gives: the same cutoff, the same rule, the same
//! postings in the same order in every segment.

use super::*;
use kosha_segment::{KnnCentroidScoreBounds, KnnSegmentRanking};
use std::sync::atomic::AtomicUsize;

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

type PlanView = (
    u32,
    Option<KnnCutoffRule>,
    Vec<Option<(Vec<u32>, u32, usize, usize)>>,
);

fn view(plan: &KnnRankedPostings) -> PlanView {
    (
        plan.cutoff.to_bits(),
        plan.rule,
        plan.rankings
            .iter()
            .map(|r: &Option<KnnSegmentRanking>| {
                r.as_ref().map(|r| {
                    (
                        r.budgeted().to_vec(),
                        r.max_distance().to_bits(),
                        r.max_probes(),
                        r.postings(),
                    )
                })
            })
            .collect(),
    )
}

fn budgets(total: usize) -> Vec<usize> {
    let mut out = vec![
        1,
        2,
        3,
        7,
        total / 4,
        total / 2,
        total.saturating_sub(1),
        total,
        total + 5,
    ];
    out.retain(|&b| b >= 1);
    out.sort_unstable();
    out.dedup();
    out
}

fn relatives(budget: usize) -> Vec<Option<RelativeCutoff>> {
    let mut out = vec![None];
    for eps in [0.0f32, 0.05, 0.5] {
        for min_probes in [0, 1, budget / 2, budget, budget + 3, usize::MAX] {
            out.push(Some(RelativeCutoff { eps, min_probes }));
        }
    }
    out
}

/// A source with arbitrary scores, optional (valid) bounds and a floor
/// that is valid, useless or garbage, counting the exact scores asked of
/// it.
struct Floored {
    scores: Vec<f32>,
    bounds: Option<KnnCentroidScoreBounds>,
    floor: Option<f32>,
    unscorable: bool,
    scored: AtomicUsize,
}

impl KnnPlanSource for Floored {
    fn postings(&self) -> usize {
        self.scores.len()
    }

    fn scores(&self) -> Option<Vec<f32>> {
        self.scored.fetch_add(self.scores.len(), Ordering::Relaxed);
        (!self.unscorable).then(|| self.scores.clone())
    }

    fn scores_for(&self, postings: &[u32]) -> Option<Vec<f32>> {
        self.scored.fetch_add(postings.len(), Ordering::Relaxed);
        (!self.unscorable).then(|| postings.iter().map(|&i| self.scores[i as usize]).collect())
    }

    fn score_bounds(&self) -> Option<KnnCentroidScoreBounds> {
        self.bounds.clone().filter(|_| !self.unscorable)
    }

    fn score_floor(&self) -> Option<f32> {
        self.floor
    }
}

/// A segment whose scores sit around `center`, on a coarse grid when
/// `grid` so ties (at the cutoff too) are common.
fn floored(rng: &mut Lcg, center: f32, grid: bool) -> Floored {
    let n = rng.below(30);
    let scores: Vec<f32> = (0..n)
        .map(|_| {
            let s = (center + rng.coord() * 0.3).max(0.0);
            if grid {
                (s * 8.0).round() / 8.0
            } else {
                s
            }
        })
        .collect();
    let min = scores.iter().copied().fold(f32::INFINITY, f32::min);
    let floor = match rng.below(8) {
        0 => None,
        1 => Some(f32::NAN),
        2 => Some(-1.0),
        3 => Some(0.0),
        // Exactly the smallest score: the tightest valid floor.
        4 if n > 0 => Some(min),
        _ if n > 0 => Some(min * (rng.coord() + 1.0) * 0.5),
        _ => Some(rng.coord().abs() * 10.0),
    };
    let bounds = (rng.below(3) != 0).then(|| {
        let mut lower = Vec::new();
        let mut upper = Vec::new();
        for &s in &scores {
            lower.push((s - rng.below(3) as f32 / 16.0).max(0.0));
            upper.push(s + rng.below(3) as f32 / 16.0);
        }
        KnnCentroidScoreBounds { lower, upper }
    });
    Floored {
        scores,
        bounds,
        floor,
        unscorable: false,
        scored: AtomicUsize::new(0),
    }
}

fn reset(sources: &[Floored]) {
    for s in sources {
        s.scored.store(0, Ordering::Relaxed);
    }
}

#[test]
fn pruned_plan_is_the_full_plan() {
    let mut rng = Lcg(0x9_12E5);
    let (mut plans, mut pruned, mut untouched) = (0usize, 0usize, 0usize);
    for round in 0..400 {
        let grid = round % 3 != 0;
        let sources: Vec<Floored> = (0..1 + rng.below(12))
            .map(|_| {
                let center = rng.below(6) as f32 * 0.5;
                floored(&mut rng, center, grid)
            })
            .collect();
        let total: usize = sources.iter().map(|s| s.postings()).sum();
        for budget in budgets(total) {
            for relative in relatives(budget) {
                for two_stage in [false, true] {
                    let full = knn_rank_postings(&sources, budget, relative, two_stage);
                    reset(&sources);
                    let plan = knn_rank_postings_with(&sources, budget, relative, two_stage, true);
                    assert_eq!(
                        view(&plan),
                        view(&full),
                        "round {round}, budget {budget}, {relative:?}, two_stage {two_stage}"
                    );
                    plans += 1;
                    if knn_rank_postings_pruned(&sources, budget, relative, two_stage).is_some() {
                        pruned += 1;
                    }
                    untouched += sources
                        .iter()
                        .filter(|s| s.postings() > 0 && s.scored.load(Ordering::Relaxed) == 0)
                        .count();
                    reset(&sources);
                }
            }
        }
    }
    assert!(plans > 10_000, "{plans} plans");
    assert!(
        pruned > 1_000,
        "pruning engaged in only {pruned} of {plans} plans"
    );
    assert!(untouched > 1_000, "only {untouched} segments went unscored");
}

#[test]
fn a_pruned_segment_is_never_scored() {
    let near = |scores: Vec<f32>| Floored {
        floor: Some(scores.iter().copied().fold(f32::INFINITY, f32::min)),
        scores,
        bounds: None,
        unscorable: false,
        scored: AtomicUsize::new(0),
    };
    let sources = vec![
        near(vec![0.1, 0.2, 0.3, 0.4]),
        near(vec![5.0, 5.5, 6.0]),
        near(vec![0.15, 0.25]),
        near(vec![9.0; 8]),
    ];
    for two_stage in [false, true] {
        let full = knn_rank_postings(&sources, 4, None, two_stage);
        reset(&sources);
        let plan = knn_rank_postings_with(&sources, 4, None, two_stage, true);
        assert_eq!(view(&plan), view(&full));
        assert_eq!(plan.exact_centroids, 6, "only the near segments are scored");
        assert_eq!(sources[1].scored.load(Ordering::Relaxed), 0);
        assert_eq!(sources[3].scored.load(Ordering::Relaxed), 0);
        assert_eq!(plan.rankings[1].as_ref().unwrap().budgeted(), &[] as &[u32]);
        assert_eq!(plan.rankings[0].as_ref().unwrap().budgeted(), &[0, 1]);
        reset(&sources);
    }
}

/// A segment that cannot be scored leaves the plan exactly as it does
/// without pruning, whether it is a seed, kept or left out.
#[test]
fn an_unscorable_segment_plans_as_without_pruning() {
    let mut rng = Lcg(0xBAD_5E6);
    let mut checked = 0usize;
    for round in 0..300 {
        let mut sources: Vec<Floored> = (0..2 + rng.below(8))
            .map(|_| {
                let center = rng.below(6) as f32 * 0.5;
                floored(&mut rng, center, round % 2 == 0)
            })
            .collect();
        let bad = rng.below(sources.len());
        sources[bad].unscorable = true;
        let total: usize = sources.iter().map(|s| s.postings()).sum();
        for budget in budgets(total) {
            for two_stage in [false, true] {
                let full = knn_rank_postings(&sources, budget, None, two_stage);
                let plan = knn_rank_postings_with(&sources, budget, None, two_stage, true);
                // A left-out segment gets an empty ranking where the full
                // plan, which could not score it, gives none: both probe
                // nothing of it under the cutoff.
                let (mut a, mut b) = (view(&plan), view(&full));
                if a.2[bad].is_some() && b.2[bad].is_none() {
                    assert!(a.2[bad].as_ref().unwrap().0.is_empty());
                    a.2[bad] = None;
                    b.2[bad] = None;
                    // The full plan counted only scorable postings.
                    if total - sources[bad].postings() <= budget {
                        continue;
                    }
                }
                assert_eq!(
                    a, b,
                    "round {round}, budget {budget}, two_stage {two_stage}"
                );
                checked += 1;
            }
        }
    }
    assert!(checked > 1_000);
}

fn unit(v: Vec<f32>) -> Vec<f32> {
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    v.into_iter().map(|x| x / n).collect()
}

/// Real centroid columns, one tight cluster of directions per segment (as
/// cluster-aligned segments are): the plan is unchanged and most segments
/// are never scored.
#[test]
fn clustered_segments_are_pruned_without_changing_the_plan() {
    let mut rng = Lcg(0xC1_A5);
    let dim = 32;
    let (segs, per_seg) = (24, 40);
    let matrices: Vec<kosha_segment::KnnCentroidMatrix> = (0..segs)
        .map(|_| {
            let topic: Vec<f32> = (0..dim).map(|_| rng.coord()).collect();
            let mut flat = Vec::new();
            let mut radii = Vec::new();
            for _ in 0..per_seg {
                let c: Vec<f32> = topic.iter().map(|&t| t + rng.coord() * 0.05).collect();
                flat.extend(unit(c));
                radii.push(0.02 + (rng.coord() + 1.0) * 0.02);
            }
            kosha_segment::KnnCentroidMatrix::new(dim, &flat, &radii)
        })
        .collect();
    let (mut exact_full, mut exact_pruned) = (0usize, 0usize);
    for q in 0..40 {
        let query = unit((0..dim).map(|_| rng.coord()).collect());
        for alpha in [0.0f32, 1.0] {
            let sources: Vec<_> = matrices
                .iter()
                .map(|matrix| super::knn_plan_two_stage::MatrixSource {
                    matrix,
                    query: &query,
                    alpha,
                })
                .collect();
            for budget in [1, 10, 40, 100] {
                for two_stage in [false, true] {
                    let full = knn_rank_postings(&sources, budget, None, two_stage);
                    let plan = knn_rank_postings_with(&sources, budget, None, two_stage, true);
                    assert_eq!(view(&plan), view(&full), "query {q}, budget {budget}");
                    exact_full += full.exact_centroids;
                    exact_pruned += plan.exact_centroids;
                }
            }
        }
    }
    assert!(
        exact_pruned * 2 < exact_full,
        "pruning scored {exact_pruned} of {exact_full} centroids"
    );
}
