//! Two-stage kNN planning (`KOSHA_KNN_PLAN_I8`): bounding every centroid's
//! score from its i8 code and scoring exactly only the centroids the
//! bounds leave open must give the plan that scoring every centroid gives:
//! the same cutoff, the same rule, the same postings in the same order in
//! every segment, and so the same hits.

use super::*;
use kosha_segment::{KnnCentroidScoreBounds, KnnSegmentRanking};
use std::sync::atomic::AtomicUsize;

mod rabitq;

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

    fn vector(&mut self, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| self.coord()).collect()
    }
}

/// What a plan decided, down to the bits, without the scores a
/// single-stage ranking additionally carries.
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

/// Budgets around every interesting point of a plan over `total` postings.
fn budgets(total: usize) -> Vec<usize> {
    let mut out = vec![
        1,
        2,
        3,
        7,
        total / 20,
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
    for eps in [0.0f32, 0.05, 0.5, 10.0] {
        for min_probes in [0, 1, budget / 2, budget, budget + 3, usize::MAX] {
            out.push(Some(RelativeCutoff { eps, min_probes }));
        }
    }
    out
}

// ── the selection logic, on made-up scores and bounds ──────────────────

/// A source with arbitrary scores and arbitrary (valid) bounds on them.
struct Synthetic {
    scores: Vec<f32>,
    bounds: Option<KnnCentroidScoreBounds>,
    /// Postings asked for through `scores_for`.
    scored: AtomicUsize,
    /// Which methods fail, as for a segment whose column went away after
    /// it was counted (then the bounds are gone too).
    fail: Fail,
}

#[derive(Clone, Copy, Default, PartialEq)]
enum Fail {
    #[default]
    Never,
    /// `scores_for` only.
    Subset,
    Always,
}

impl KnnPlanSource for Synthetic {
    fn postings(&self) -> usize {
        self.scores.len()
    }

    fn scores(&self) -> Option<Vec<f32>> {
        (self.fail != Fail::Always).then(|| self.scores.clone())
    }

    fn scores_for(&self, postings: &[u32]) -> Option<Vec<f32>> {
        self.scored.fetch_add(postings.len(), Ordering::Relaxed);
        (self.fail == Fail::Never)
            .then(|| postings.iter().map(|&i| self.scores[i as usize]).collect())
    }

    fn score_bounds(&self) -> Option<KnnCentroidScoreBounds> {
        self.bounds.clone().filter(|_| self.fail != Fail::Always)
    }
}

/// `tight`: the bounds are narrow (as real ones are); otherwise anything
/// from exact to useless. Scores sit on a coarse grid when `grid`, so
/// ties, zero scores and ties at the cutoff are common.
fn synthetic(rng: &mut Lcg, tight: bool, grid: bool) -> Synthetic {
    let n = rng.below(40);
    let scores: Vec<f32> = (0..n)
        .map(|_| {
            if grid {
                match rng.below(5) {
                    0 | 1 => 0.0,
                    _ => rng.below(9) as f32 / 8.0,
                }
            } else {
                (rng.coord() + 1.0) * 0.5
            }
        })
        .collect();
    let bounds = (rng.below(5) != 0).then(|| {
        let width = |rng: &mut Lcg| -> f32 {
            match rng.below(if tight { 3 } else { 6 }) {
                0 => 0.0,
                1 | 2 => rng.below(3) as f32 / 16.0,
                3 => rng.below(9) as f32 / 8.0,
                4 => 2.0,
                _ => f32::INFINITY,
            }
        };
        let (mut lower, mut upper) = (Vec::new(), Vec::new());
        for &s in &scores {
            // Real lower bounds are never negative (the score is clamped
            // at zero), but nothing requires that.
            let lo = s - width(rng);
            lower.push(if rng.below(2) == 0 { lo.max(0.0) } else { lo });
            upper.push(s + width(rng));
        }
        KnnCentroidScoreBounds { lower, upper }
    });
    Synthetic {
        scores,
        bounds,
        scored: AtomicUsize::new(0),
        fail: Fail::Never,
    }
}

#[test]
fn two_stage_plan_is_the_single_stage_plan_on_arbitrary_bounds() {
    let mut rng = Lcg(0x2_57A6E);
    let (mut plans, mut engaged, mut saved) = (0usize, 0usize, 0usize);
    for round in 0..600 {
        let (tight, grid) = (round % 2 == 0, round % 3 != 0);
        let sources: Vec<Synthetic> = (0..1 + rng.below(9))
            .map(|_| synthetic(&mut rng, tight, grid))
            .collect();
        let total: usize = sources.iter().map(|s| s.postings()).sum();
        if total == 0 {
            continue;
        }
        for budget in budgets(total) {
            for relative in relatives(budget) {
                let single = knn_rank_postings(&sources, budget, relative, false);
                assert_eq!(single.exact_centroids, total);
                for s in &sources {
                    s.scored.store(0, Ordering::Relaxed);
                }
                let two = knn_rank_postings(&sources, budget, relative, true);
                assert_eq!(
                    view(&two),
                    view(&single),
                    "round {round}, budget {budget} of {total}, {relative:?}"
                );
                assert!(two.exact_centroids <= total);
                plans += 1;
                if sources.iter().any(|s| s.scored.load(Ordering::Relaxed) > 0) {
                    engaged += 1;
                }
                saved += total - two.exact_centroids;
            }
        }
    }
    assert!(plans > 50_000, "{plans} plans compared");
    assert!(engaged > plans / 4, "stage two ran in {engaged} of {plans}");
    assert!(saved > 0, "the bounds never ruled a posting out");
}

#[test]
fn a_nan_anywhere_falls_back_to_the_single_stage_plan() {
    let source = |scores: Vec<f32>, lower: Vec<f32>, upper: Vec<f32>| Synthetic {
        scores,
        bounds: Some(KnnCentroidScoreBounds { lower, upper }),
        scored: AtomicUsize::new(0),
        fail: Fail::Never,
    };
    let scores = vec![0.1f32, 0.2, 0.3, 0.4];
    let exact = scores.clone();
    let cases = [
        // NaN in a bound.
        source(scores.clone(), vec![0.1, f32::NAN, 0.3, 0.4], exact.clone()),
        source(scores.clone(), exact.clone(), vec![0.1, 0.2, f32::NAN, 0.4]),
        // NaN in an exact score.
        source(vec![0.1, f32::NAN, 0.3, 0.4], vec![0.0; 4], vec![1.0; 4]),
        // Bounds of the wrong length are not bounds.
        source(scores.clone(), vec![0.0; 3], vec![1.0; 3]),
    ];
    for (i, case) in cases.iter().enumerate() {
        for budget in 1..=5 {
            let sources = std::slice::from_ref(case);
            let single = knn_rank_postings(sources, budget, None, false);
            let two = knn_rank_postings(sources, budget, None, true);
            assert_eq!(view(&two), view(&single), "case {i}, budget {budget}");
        }
    }
}

#[test]
fn a_segment_that_cannot_be_scored_leaves_the_plan_instead_of_taking_no_postings() {
    // A source whose column went away after it was counted used to score
    // every posting INFINITY: it got a ranking with no postings, so the
    // segment was never probed and the search still answered 200. It must
    // instead get no ranking (the segment then ranks its own postings
    // under the cutoff, as before #469), and the cutoff must be the one
    // the other sources alone give.
    let mut rng = Lcg(0xFA11);
    let mut checked = 0usize;
    for round in 0..300 {
        let mut sources: Vec<Synthetic> = (0..2 + rng.below(6))
            .map(|_| synthetic(&mut rng, round % 2 == 0, round % 3 != 0))
            .collect();
        let broken = rng.below(sources.len());
        let fail = if round % 2 == 0 {
            Fail::Always
        } else {
            Fail::Subset
        };
        for budget in budgets(sources.iter().map(|s| s.postings()).sum()) {
            for two_stage in [false, true] {
                sources[broken].fail = Fail::Never;
                let healthy: Vec<Synthetic> = sources
                    .iter()
                    .enumerate()
                    .filter(|&(i, _)| i != broken)
                    .map(|(_, s)| Synthetic {
                        scores: s.scores.clone(),
                        bounds: s.bounds.clone(),
                        scored: AtomicUsize::new(0),
                        fail: Fail::Never,
                    })
                    .collect();
                let without = knn_rank_postings(&healthy, budget, None, two_stage);
                let whole = knn_rank_postings(&sources, budget, None, two_stage);
                sources[broken].fail = fail;
                let plan = knn_rank_postings(&sources, budget, None, two_stage);
                let mut view_plan = view(&plan);
                match fail {
                    // `scores_for` alone failing redoes the plan
                    // single-stage, which needs only `scores`: same plan.
                    Fail::Subset => assert_eq!(view_plan, view(&whole), "round {round}"),
                    _ => {
                        assert!(view_plan.2[broken].is_none(), "round {round}: no ranking");
                        view_plan.2.remove(broken);
                        assert_eq!(view_plan, view(&without), "round {round}, budget {budget}");
                        // Redone single-stage: every healthy posting scored.
                        let healthy_total: usize = healthy.iter().map(|s| s.postings()).sum();
                        assert_eq!(plan.exact_centroids, healthy_total);
                    }
                }
                checked += 1;
            }
        }
    }
    assert!(checked > 1_000, "{checked}");
}

#[test]
fn the_unscorable_segment_warning_is_rate_limited_and_counts_what_it_held_back() {
    let limit = RateLimitedWarn::new(3600);
    let mut held_back = None;
    assert!(limit.warn(|n| {
        held_back = Some(n);
        String::from("first")
    }));
    assert_eq!(held_back, Some(0));
    for _ in 0..5 {
        assert!(!limit.warn(|_| unreachable!("inside the interval")));
    }
    // Past the interval the next line reports the five held back.
    limit.next_secs.store(0, Ordering::Relaxed);
    assert!(limit.warn(|n| {
        held_back = Some(n);
        String::from("second")
    }));
    assert_eq!(held_back, Some(5));
}

// ── real segments ─────────────────────────────────────────────────────

const FIELD: &str = "emb";

struct Corpus {
    name: &'static str,
    dir: PathBuf,
    ns: NamespaceId,
    manifest: Manifest,
    dim: usize,
    /// Stored vectors, to draw queries from.
    vectors: Vec<Vec<f32>>,
}

impl Drop for Corpus {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Writes `segments` (each a list of vectors) as one namespace. Every
/// document also carries a keyword `tag`: `half` on every second document,
/// `tenth` and `rare` on every tenth and hundredth, for filtered queries
/// of three selectivities.
fn corpus(name: &'static str, dim: usize, segments: Vec<Vec<Vec<f32>>>) -> Corpus {
    // Tests run in parallel and each builds its own copy.
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-test-knn-plan-i8-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let mut entries = Vec::new();
    let mut vectors = Vec::new();
    let mut n = 0usize;
    for (s, segment) in segments.into_iter().enumerate() {
        let seg = format!("s{s:03}");
        let mut w = SegmentWriter::new(SegmentId(seg.clone()), dir.join("test").join(&seg));
        let doc_count = segment.len() as u32;
        for (i, v) in segment.into_iter().enumerate() {
            let tag = if n % 100 == 37 {
                "rare"
            } else if n % 10 == 3 {
                "tenth"
            } else if n.is_multiple_of(2) {
                "half"
            } else {
                "rest"
            };
            w.add_document(
                DocumentId(format!("{seg}-d{i}")),
                vec![Field::vector(FIELD, v.clone()), Field::keyword("tag", tag)],
            );
            vectors.push(v);
            n += 1;
        }
        w.finalize(Bm25Params::default()).unwrap();
        entries.push(ManifestEntry {
            segment_id: SegmentId(seg),
            doc_count,
        });
    }
    Corpus {
        name,
        dir,
        ns: NamespaceId("test".into()),
        manifest: Manifest {
            version: 1,
            segments: entries,
            segment_footers: Default::default(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            branch: None,
            schema: None,
            retired: Default::default(),
            shards: None,
        },
        dim,
        vectors,
    }
}

/// Tight clusters: most centroids are far from a query and a few are
/// near, the shape the stage is meant for.
fn clustered() -> Corpus {
    let mut rng = Lcg(0xC1);
    let dim = 32;
    let centers: Vec<Vec<f32>> = (0..24).map(|_| rng.vector(dim)).collect();
    let segments = (0..36)
        .map(|_| {
            (0..300)
                .map(|_| {
                    let c = &centers[rng.below(centers.len())];
                    c.iter().map(|x| x + rng.coord() * 0.2).collect()
                })
                .collect()
        })
        .collect();
    corpus("clustered", dim, segments)
}

/// No structure: every centroid is about as far as every other, so the
/// bounds overlap the cutoff for many of them.
fn uniform() -> Corpus {
    let mut rng = Lcg(0x0F);
    let dim = 16;
    let segments = (0..24)
        .map(|_| (0..300).map(|_| rng.vector(dim)).collect())
        .collect();
    corpus("uniform", dim, segments)
}

/// The same vectors in every segment, many of them repeated: identical
/// centroids across segments, so scores tie across segments and at the
/// cutoff, and whole postings have radius zero.
fn tied() -> Corpus {
    let mut rng = Lcg(0x71ED);
    let dim = 8;
    let distinct: Vec<Vec<f32>> = (0..40).map(|_| rng.vector(dim)).collect();
    let segment: Vec<Vec<f32>> = (0..360)
        .map(|i| distinct[i % distinct.len()].clone())
        .collect();
    corpus("tied", dim, vec![segment; 12])
}

/// Wide spread around few centers: large radii, so at the default radius
/// weight most scores clamp to zero.
fn diffuse() -> Corpus {
    let mut rng = Lcg(0xD1F);
    let dim = 24;
    let centers: Vec<Vec<f32>> = (0..3).map(|_| rng.vector(dim)).collect();
    let segments = (0..16)
        .map(|_| {
            (0..300)
                .map(|_| {
                    let c = &centers[rng.below(centers.len())];
                    c.iter().map(|x| x + rng.coord() * 0.9).collect()
                })
                .collect()
        })
        .collect();
    corpus("diffuse", dim, segments)
}

/// Fewer centroids than any budget.
fn tiny() -> Corpus {
    let mut rng = Lcg(0x7);
    let dim = 8;
    let segments = (0..3)
        .map(|_| (0..6).map(|_| rng.vector(dim)).collect())
        .collect();
    corpus("tiny", dim, segments)
}

fn corpora() -> Vec<Corpus> {
    vec![clustered(), uniform(), tied(), diffuse(), tiny()]
}

fn queries(corpus: &Corpus, rng: &mut Lcg, n: usize) -> Vec<Vec<f32>> {
    let mut out: Vec<Vec<f32>> = Vec::new();
    // A stored vector (distance zero to something), its negation, the
    // zero vector, a one-hot, then random directions and perturbed stored
    // vectors.
    out.push(corpus.vectors[rng.below(corpus.vectors.len())].clone());
    out.push(out[0].iter().map(|x| -x).collect());
    out.push(vec![0.0; corpus.dim]);
    let mut one_hot = vec![0.0; corpus.dim];
    one_hot[rng.below(corpus.dim)] = 1.0;
    out.push(one_hot);
    while out.len() < n {
        if rng.below(2) == 0 {
            out.push(rng.vector(corpus.dim));
        } else {
            let v = &corpus.vectors[rng.below(corpus.vectors.len())];
            out.push(v.iter().map(|x| x + rng.coord() * 0.1).collect());
        }
    }
    out
}

#[test]
fn two_stage_plan_is_the_single_stage_plan_on_real_segments() {
    let mut rng = Lcg(0x5E6);
    let (mut plans, mut engaged) = (0usize, 0usize);
    let coded = kosha_segment::knn_plan_i8_from_env();
    for corpus in corpora() {
        let readers: Vec<SegmentReader> = corpus
            .manifest
            .segments
            .iter()
            .map(|entry| {
                SegmentReader::open_with_options(
                    corpus.dir.join("test").join(&entry.segment_id.0),
                    true,
                )
                .unwrap()
            })
            .collect();
        let (mut scored_two, mut scored_single) = (0usize, 0usize);
        for (q, query) in queries(&corpus, &mut rng, 40).iter().enumerate() {
            for alpha in [0.0f32, 0.5, 1.0] {
                let sources: Vec<ColumnPlanSource<'_>> = readers
                    .iter()
                    .map(|reader| ColumnPlanSource {
                        reader,
                        field: FIELD,
                        query,
                        alpha,
                        postings: reader.knn_centroid_count(FIELD, corpus.dim).unwrap(),
                    })
                    .collect();
                // With KOSHA_KNN_PLAN_I8=0 no column is coded and both
                // plans below are the single-stage one.
                assert!(
                    sources.iter().all(|s| s.score_bounds().is_some() == coded),
                    "{}: every column is coded",
                    corpus.name
                );
                let total: usize = sources.iter().map(|s| s.postings).sum();
                for budget in budgets(total) {
                    for relative in relatives(budget)
                        .into_iter()
                        .step_by(if q < 6 { 1 } else { 5 })
                    {
                        let single = knn_rank_postings(&sources, budget, relative, false);
                        let two = knn_rank_postings(&sources, budget, relative, true);
                        assert_eq!(
                            view(&two),
                            view(&single),
                            "{}: query {q}, alpha {alpha}, budget {budget} of {total}, \
                             {relative:?}",
                            corpus.name
                        );
                        plans += 1;
                        if budget < total {
                            scored_two += two.exact_centroids;
                            scored_single += single.exact_centroids;
                            engaged += 1;
                        }
                    }
                }
            }
        }
        // A budget that admits everything never runs the stage; anywhere
        // else it has to save some exact scores, or it is pure overhead.
        if coded && corpus.name != "tiny" {
            assert!(
                scored_two < scored_single,
                "{}: {scored_two} of {scored_single} centroids scored exactly",
                corpus.name
            );
        }
        eprintln!(
            "{}: {} centroids, two-stage scored {scored_two} of {scored_single} exactly ({:.0}%)",
            corpus.name,
            readers
                .iter()
                .map(|r| r.knn_centroid_count(FIELD, corpus.dim).unwrap())
                .sum::<usize>(),
            100.0 * scored_two as f64 / scored_single.max(1) as f64
        );
    }
    assert!(
        plans > 20_000 && engaged > plans / 2,
        "{engaged} of {plans}"
    );
}

fn tag_filter(tag: &str) -> kosha_core::FilterClause {
    kosha_core::FilterClause::Term {
        term: HashMap::from([("tag".to_string(), tag.to_string())]),
    }
}

/// Everything a client or an operator can see of one search.
#[derive(Debug, PartialEq)]
struct Outcome {
    hits: Vec<(String, u64)>,
    total_hits: usize,
    degraded: Option<u32>,
    postings: usize,
    segments: usize,
    centroids: usize,
    mode: Option<KnnMode>,
    filtered: Option<u64>,
    rule: Option<KnnCutoffRule>,
}

fn run(
    searcher: &Searcher,
    corpus: &Corpus,
    query: &SearchQuery,
    tombstones: Option<&HashMap<SegmentId, HashSet<u32>>>,
) -> (Outcome, usize) {
    let (r, stats) = searcher
        .search_with_stats(&corpus.ns, &corpus.manifest, query, tombstones)
        .unwrap();
    (
        Outcome {
            hits: r
                .results
                .iter()
                .map(|d| (d.doc_id.0.clone(), d.score.to_bits()))
                .collect(),
            total_hits: r.total_hits,
            degraded: r.knn_degraded_segments,
            postings: stats.knn_postings_probed,
            segments: stats.knn_segments_probed,
            centroids: stats.knn_centroids_scanned,
            mode: stats.knn_mode,
            filtered: stats.knn_filtered_candidates,
            rule: stats.knn_cutoff_rule,
        },
        stats.knn_plan_exact_centroids,
    )
}

#[test]
fn searches_are_identical_with_the_i8_stage_on_and_off() {
    // End to end, in whatever mode the process's KOSHA_KNN_* variables
    // select. They are read once per process, so covering a mode means
    // running this test under it: KOSHA_KNN_PROBE_RADIUS_ALPHA,
    // KOSHA_KNN_RELATIVE_CUTOFF_EPS, KOSHA_KNN_GLOBAL_BUDGET=0,
    // KOSHA_KNN_GLOBAL_RESCORE=1, KOSHA_VECTOR_QUANTIZATION. A plain
    // `cargo test` covers the defaults only.
    let mut rng = Lcg(0xE2E);
    let (mut compared, mut refilled, mut saved) = (0usize, 0usize, 0usize);
    for corpus in corpora() {
        // Two exact-filtered-kNN policies: the default (a selective filter
        // is answered exactly, without the plan's rankings) and off (every
        // filtered query probes, and a selective one refills far past its
        // budget, which needs the full ranking of its segments).
        for exact_threshold in [DEFAULT_KNN_EXACT_THRESHOLD, 0] {
            let searcher = |i8_stage: bool| {
                Searcher::new(corpus.dir.clone())
                    .with_knn_plan_i8(i8_stage)
                    .with_knn_exact_policy(exact_threshold, DEFAULT_KNN_EXACT_FRACTION)
            };
            let (on, off) = (searcher(true), searcher(false));
            // A few deleted documents in two segments.
            let tombstones: HashMap<SegmentId, HashSet<u32>> = corpus
                .manifest
                .segments
                .iter()
                .take(2)
                .map(|entry| {
                    (
                        entry.segment_id.clone(),
                        (0..entry.doc_count).filter(|seq| seq % 5 == 1).collect(),
                    )
                })
                .collect();
            for (q, vector) in queries(&corpus, &mut rng, 14).into_iter().enumerate() {
                for (num_candidates, k) in [(1usize, 1usize), (3, 10), (20, 10), (100_000, 25)] {
                    for shape in 0..7 {
                        let mut query = mk_query("", k);
                        let mut knn = kosha_core::KnnQuery {
                            query_text: None,
                            embedding_model: None,
                            field: FIELD.into(),
                            vector: vector.clone(),
                            k,
                            num_candidates,
                            filter: None,
                            vectors: None,
                            scoring: None,
                        };
                        let mut deleted = None;
                        match shape {
                            0 => {}
                            1 => query.filter = Some(tag_filter("half")),
                            2 => query.filter = Some(tag_filter("tenth")),
                            3 => knn.filter = Some(tag_filter("rare")),
                            4 => {
                                query.filter = Some(tag_filter("half"));
                                knn.filter = Some(tag_filter("nobody"));
                            }
                            5 => deleted = Some(&tombstones),
                            _ => {
                                knn.filter = Some(tag_filter("tenth"));
                                deleted = Some(&tombstones);
                            }
                        }
                        query.knn = Some(knn);
                        let (with, scored_with) = run(&on, &corpus, &query, deleted);
                        let (without, scored_without) = run(&off, &corpus, &query, deleted);
                        assert_eq!(
                            with, without,
                            "{}: query {q}, num_candidates {num_candidates}, k {k}, shape \
                             {shape}, exact threshold {exact_threshold}",
                            corpus.name
                        );
                        assert!(scored_with <= scored_without);
                        compared += 1;
                        saved += scored_without - scored_with;
                        if shape != 0 && with.postings > num_candidates.max(k) * 2 {
                            refilled += 1;
                        }
                    }
                }
            }
        }
    }
    assert!(compared > 3_000, "{compared} searches compared");
    if knn_global_budget_enabled() {
        if kosha_segment::knn_plan_i8_from_env() {
            assert!(saved > 0, "the stage never saved an exact centroid score");
        }
        assert!(refilled > 0, "no restricted probe ran past its budget");
    }
}

// ── the plan at the 10M bench's size ──────────────────────────────────

/// [`KnnPlanSource`] over centroids held in memory.
pub(super) struct MatrixSource<'a> {
    pub(super) matrix: &'a kosha_segment::KnnCentroidMatrix,
    pub(super) query: &'a [f32],
    pub(super) alpha: f32,
}

impl KnnPlanSource for MatrixSource<'_> {
    fn postings(&self) -> usize {
        self.matrix.postings()
    }

    fn scores(&self) -> Option<Vec<f32>> {
        Some(self.matrix.scores(self.query, self.alpha))
    }

    fn scores_for(&self, postings: &[u32]) -> Option<Vec<f32>> {
        Some(self.matrix.scores_for(self.query, postings, self.alpha))
    }

    fn score_bounds(&self) -> Option<KnnCentroidScoreBounds> {
        self.matrix.score_bounds(self.query, self.alpha)
    }
    fn score_floor(&self) -> Option<f32> {
        self.matrix.score_floor(self.query, self.alpha)
    }
}

/// Not a test of anything: times [`knn_rank_postings`], single-stage and
/// two-stage, over centroid matrices the size of the 10M vector bench's
/// (by default 196 segments x 582 centroids x 1024 dimensions, 467 MB of
/// f32), which no on-disk test corpus reaches. It covers the plan's
/// scoring, cutoff and ranking and nothing else of `knn_plan` (no segment
/// opens, no filter counting). The centroids are synthetic, so the share
/// of them the first stage leaves for an exact score is this data's, not
/// MS MARCO's.
///
/// ```text
/// cargo test --release -p kosha-query --lib -- --ignored --nocapture knn_plan_microbench
/// ```
///
/// Knobs: `KOSHA_PLAN_BENCH_SEGS`, `KOSHA_PLAN_BENCH_CENTROIDS` (per
/// segment), `KOSHA_PLAN_BENCH_DIM`, `KOSHA_PLAN_BENCH_QUERIES`,
/// `KOSHA_PLAN_BENCH_ITERS`, `KOSHA_PLAN_BENCH_NUM_CANDIDATES`.
#[test]
#[ignore = "a timing harness, run by hand in release mode"]
fn knn_plan_microbench() {
    run_plan_microbench(false);
}

fn run_plan_microbench(rabitq: bool) {
    let knob = |name: &str, default: usize| -> usize {
        std::env::var(format!("KOSHA_PLAN_BENCH_{name}"))
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let normalize = |v: &mut Vec<f32>| {
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        v.iter_mut().for_each(|x| *x /= norm);
    };
    let (segs, per_seg, dim) = (knob("SEGS", 196), knob("CENTROIDS", 582), knob("DIM", 1024));
    let (n_queries, iters) = (knob("QUERIES", 16), knob("ITERS", 20));
    let requested = knob("NUM_CANDIDATES", 100);
    assert!(segs > 0 && per_seg > 0 && dim > 0 && n_queries > 0 && iters > 0);
    let mut rng = Lcg(0xBE7C);
    let rotation = rabitq.then(|| kosha_vector_spfresh::RaBitQRotation::new(dim));
    // Roughly normal coordinates: a sum of four uniforms.
    let noise = |rng: &mut Lcg, dim: usize| -> Vec<f32> {
        let mut v: Vec<f32> = (0..dim)
            .map(|_| rng.coord() + rng.coord() + rng.coord() + rng.coord())
            .collect();
        normalize(&mut v);
        v
    };
    let mix = |a: &[f32], b: &[f32], weight: f32| -> Vec<f32> {
        let mut v: Vec<f32> = a.iter().zip(b).map(|(x, y)| x + weight * y).collect();
        normalize(&mut v);
        v
    };
    let p50 = |mut samples: Vec<f64>| -> f64 {
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        samples[samples.len() / 2]
    };

    // Two layouts of the centroids. "topics": every centroid is a topic
    // direction plus noise (cosine about 0.6 within a topic, about 0
    // across), and queries sit near a centroid. "iid": independent random
    // directions, where every distance is within a few hundredths of 1 —
    // the hardest case for a bound of about a hundredth.
    for layout in ["topics", "iid"] {
        let topics: Vec<Vec<f32>> = (0..256).map(|_| noise(&mut rng, dim)).collect();
        let centroid_bytes = segs * per_seg * dim * 4;
        for (radii_name, radius_of) in [
            ("no radii", (|_: &mut Lcg| 0.0) as fn(&mut Lcg) -> f32),
            ("radii 0.2..0.6", |rng: &mut Lcg| 0.4 + rng.coord() * 0.2),
        ] {
            let mut sample: Vec<Vec<f32>> = Vec::new();
            let (matrices, codes): (Vec<_>, Vec<_>) = (0..segs)
                .map(|_| {
                    let mut flat = Vec::with_capacity(per_seg * dim);
                    let mut radii = Vec::with_capacity(per_seg);
                    for _ in 0..per_seg {
                        let g = noise(&mut rng, dim);
                        let c = if layout == "topics" {
                            mix(&topics[rng.below(topics.len())], &g, 0.8)
                        } else {
                            g
                        };
                        if sample.len() < n_queries {
                            sample.push(c.clone());
                        }
                        flat.extend_from_slice(&c);
                        radii.push(radius_of(&mut rng));
                    }
                    let matrix = kosha_segment::KnnCentroidMatrix::new(dim, &flat, &radii);
                    let codes = rotation
                        .as_ref()
                        .map(|r| rabitq::CentroidCodes::new(r, &flat, radii));
                    (matrix, codes)
                })
                .unzip();
            let codes: Option<Vec<_>> = codes.into_iter().collect();
            let queries: Vec<Vec<f32>> = sample
                .iter()
                .map(|c| mix(c, &noise(&mut rng, dim), 0.5))
                .collect();
            let total = segs * per_seg;
            let budget = effective_knn_budget(requested, total);
            for alpha in [1.0f32, 0.0] {
                if radii_name == "no radii" && alpha == 0.0 {
                    continue; // the same scores as alpha 1
                }
                if let (Some(rotation), Some(codes)) = (&rotation, &codes) {
                    println!("{layout}, {radii_name}, alpha {alpha}: {total} x {dim} centroids, budget {budget}");
                    rabitq::measure(rotation, &matrices, codes, &queries, alpha, budget, iters);
                    continue;
                }
                let mut ms = [Vec::new(), Vec::new()];
                let (mut exact, mut plans) = (0usize, 0usize);
                for iter in 0..iters + 1 {
                    for query in &queries {
                        let sources: Vec<MatrixSource<'_>> = matrices
                            .iter()
                            .map(|matrix| MatrixSource {
                                matrix,
                                query,
                                alpha,
                            })
                            .collect();
                        let mut views = Vec::new();
                        for (slot, two_stage) in [(0, false), (1, true)] {
                            let t = Instant::now();
                            let plan = knn_rank_postings(&sources, budget, None, two_stage);
                            let elapsed = t.elapsed().as_secs_f64() * 1e3;
                            if iter > 0 {
                                ms[slot].push(elapsed);
                                if two_stage {
                                    exact += plan.exact_centroids;
                                    plans += 1;
                                }
                            }
                            views.push(view(&plan));
                        }
                        assert_eq!(views[0], views[1], "the two plans differ");
                    }
                }
                let [single, two] = ms;
                let (single, two) = (p50(single), p50(two));
                println!(
                    "{layout}, {radii_name}, alpha {alpha}: {segs} x {per_seg} = {total} centroids \
                     x {dim} dims ({:.0} MB f32), budget {budget}: single-stage p50 {single:.2} ms, \
                     two-stage p50 {two:.2} ms ({:.1}x), exact f32 scores per plan {:.0} ({:.1}% \
                     of centroids)",
                    centroid_bytes as f64 / 1e6,
                    single / two,
                    exact as f64 / plans.max(1) as f64,
                    100.0 * exact as f64 / (plans.max(1) * total) as f64,
                );
            }
        }
    }
}
