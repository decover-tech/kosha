//! Compaction-style merge (#253): seed a `ClusterIndex` from existing
//! postings with `from_postings`, drop deleted members, insert the vectors
//! of inputs too small to have useful postings, `rebalance` — and compare
//! recall@10 with a from-scratch `build` over the same live set, both for
//! one compaction and across repeated compaction cycles under churn.
//!
//! Recall is averaged over several RNG seeds: two from-scratch builds of
//! the same data with different seeds already differ by about a point of
//! recall@10 on these datasets, so a single-seed comparison measures
//! clusterer noise, not the merge.

mod common;
#[path = "common/compaction.rs"]
mod compaction;

use std::collections::HashSet;

use common::{brute_force_topk, recall_at_k};
use compaction::{input_of, lire_merge, Input, ABSORB_CANDIDATES};
use kosha_vector_spfresh::{
    cosine_distance, cosine_similarity, mean_vector, nearest_centroids, normalize_in_place,
    ClusterIndex, ClusterIndexConfig, DeterministicRng, PostingSnapshot, VectorIndexError,
};

const SEEDS: u64 = 5;

/// `n` points of a mixture of `centers` random directions in `dim`
/// dimensions, jittered. `drift` rotates every center a little (churn
/// cycles move the distribution). Clusters overlap in every dimension —
/// harder than the circle datasets, closer to embedding data.
fn mixture(
    n: usize,
    dim: usize,
    centers: usize,
    first_id: u32,
    drift: f32,
    seed: u64,
) -> Vec<(u32, Vec<f32>)> {
    let mut crng = DeterministicRng::new(0xC0FFEE);
    let cs: Vec<Vec<f32>> = (0..centers)
        .map(|_| {
            (0..dim)
                .map(|d| crng.next_f32_range(-1.0, 1.0) + drift * ((d % 3) as f32 - 1.0))
                .collect()
        })
        .collect();
    let mut rng = DeterministicRng::new(seed);
    (0..n)
        .map(|i| {
            let c = &cs[rng.next_usize(centers)];
            let v = c
                .iter()
                .map(|x| x + rng.next_f32_range(-0.6, 0.6))
                .collect();
            (first_id + i as u32, v)
        })
        .collect()
}

fn queries(dim: usize, drift: f32) -> Vec<Vec<f32>> {
    mixture(400, dim, 40, 0, drift, 999)
        .into_iter()
        .map(|(_, v)| v)
        .collect()
}

fn config(dim: usize, seed: u64) -> ClusterIndexConfig {
    let mut cfg = ClusterIndexConfig::new(dim);
    cfg.seed = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    cfg
}

/// Mean recall@10 probing `fraction` of the index's postings — how
/// kosha-query sizes its global budget (`KOSHA_KNN_BUDGET_FRACTION`), so
/// indexes with slightly different posting counts compare at equal
/// relative cost.
fn recall(idx: &ClusterIndex, live: &[(u32, Vec<f32>)], qs: &[Vec<f32>], fraction: f64) -> f64 {
    let snap = idx.snapshot();
    let nprobe = ((snap.len() as f64 * fraction).ceil() as usize).max(1);
    let centroids: Vec<Vec<f32>> = snap.iter().map(|p| p.centroid.clone()).collect();
    let mut total = 0.0;
    for q in qs {
        let truth = brute_force_topk(live, q, 10);
        let mut hits: Vec<(u32, f32)> = nearest_centroids(&centroids, q, nprobe)
            .into_iter()
            .flat_map(|i| snap[i].entries.iter())
            .map(|(id, v)| (*id, cosine_similarity(q, v)))
            .collect();
        hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        hits.truncate(10);
        total += recall_at_k(&hits, &truth);
    }
    total / qs.len() as f64
}

/// [`recall`] with kosha-query's radius-aware probe order: postings
/// ranked by `max(0, d(q, centroid) - radius)`, a lower bound on the
/// distance to any member, so loose postings (large radii) cost probes.
fn radius_recall(
    snap: &[PostingSnapshot],
    live: &[(u32, Vec<f32>)],
    qs: &[Vec<f32>],
    fraction: f64,
) -> f64 {
    let nprobe = ((snap.len() as f64 * fraction).ceil() as usize).max(1);
    let mut total = 0.0;
    for q in qs {
        let truth = brute_force_topk(live, q, 10);
        let mut order: Vec<(usize, f32)> = snap
            .iter()
            .enumerate()
            .map(|(i, p)| (i, (cosine_distance(q, &p.centroid) - p.radius).max(0.0)))
            .collect();
        order.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        let mut hits: Vec<(u32, f32)> = order[..nprobe]
            .iter()
            .flat_map(|(i, _)| snap[*i].entries.iter())
            .map(|(id, v)| (*id, cosine_similarity(q, v)))
            .collect();
        hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        hits.truncate(10);
        total += recall_at_k(&hits, &truth);
    }
    total / qs.len() as f64
}

fn radius_quantile(snap: &[PostingSnapshot], q: f64) -> f32 {
    let mut r: Vec<f32> = snap.iter().map(|p| p.radius).collect();
    r.sort_by(f32::total_cmp);
    r.get(((r.len() as f64 * q) as usize).min(r.len().saturating_sub(1)))
        .copied()
        .unwrap_or(0.0)
}

fn median_radius(snap: &[PostingSnapshot]) -> f32 {
    radius_quantile(snap, 0.5)
}

fn refine(cfg: &ClusterIndexConfig, on: bool) -> ClusterIndexConfig {
    let mut cfg = cfg.clone();
    if !on {
        cfg.build_refine_rounds = 0;
    }
    cfg
}

fn assert_same_ids(idx: &ClusterIndex, live: &[(u32, Vec<f32>)]) {
    let got: HashSet<u32> = idx.ids().into_iter().collect();
    let want: HashSet<u32> = live.iter().map(|(id, _)| *id).collect();
    assert_eq!(got, want, "merged index must hold exactly the live set");
    let snap_ids: usize = idx.snapshot().iter().map(|p| p.entries.len()).sum();
    assert_eq!(snap_ids, live.len(), "every live id in exactly one posting");
}

#[test]
fn one_compaction_recall_matches_rebuild_within_half_a_point() {
    let dim = 32;
    let big = mixture(8_000, dim, 40, 0, 0.0, 1);
    // 400 of the big segment's vectors updated: deleted there, new
    // versions spread over 8 flush segments.
    let mut rng = DeterministicRng::new(7);
    let mut deleted = HashSet::new();
    while deleted.len() < 400 {
        deleted.insert(rng.next_usize(big.len()) as u32);
    }
    let small: Vec<Vec<(u32, Vec<f32>)>> = (0..8)
        .map(|s| mixture(50, dim, 40, 8_000 + s * 50, 0.0, 100 + s as u64))
        .collect();
    let live: Vec<(u32, Vec<f32>)> = big
        .iter()
        .filter(|(id, _)| !deleted.contains(id))
        .chain(small.iter().flatten())
        .cloned()
        .collect();
    let qs = queries(dim, 0.0);

    for fraction in [0.05, 0.1] {
        let (mut r_sum, mut l_sum) = (0.0, 0.0);
        for seed in 0..SEEDS {
            let cfg = config(dim, seed);
            let rebuilt = ClusterIndex::build(&live, cfg.clone()).unwrap();
            let mut inputs = vec![input_of(big.clone(), &cfg)];
            inputs.extend(small.iter().cloned().map(Input::Vectors));
            let merged = lire_merge(inputs, &deleted, &cfg);
            assert_same_ids(&merged, &live);
            r_sum += recall(&rebuilt, &live, &qs, fraction);
            l_sum += recall(&merged, &live, &qs, fraction);
        }
        let (r, l) = (r_sum / SEEDS as f64, l_sum / SEEDS as f64);
        eprintln!("fraction {fraction}: rebuild recall@10 {r:.4}, lire {l:.4}");
        assert!(
            l >= r - 0.005,
            "fraction {fraction}: lire recall {l:.4} is more than 0.5pt below rebuild {r:.4}"
        );
    }
}

/// Build refinement (#253): bisection leaves far members in postings, so
/// their radii are loose and a radius-aware probe at the 5% floor budget
/// misses neighbours. Refinement must tighten the median radius, raise
/// that recall, and keep the index well-formed: every vector in exactly
/// one posting, sizes within bounds.
#[test]
fn build_refinement_tightens_radii_and_raises_radius_aware_recall() {
    let dim = 32;
    // Unit-normalized, as kosha-segment builds them.
    let unit = |mut v: Vec<f32>| {
        normalize_in_place(&mut v);
        v
    };
    let live: Vec<(u32, Vec<f32>)> = mixture(8_000, dim, 40, 0, 0.0, 1)
        .into_iter()
        .map(|(id, v)| (id, unit(v)))
        .collect();
    let qs: Vec<Vec<f32>> = queries(dim, 0.0).into_iter().map(unit).collect();
    let (mut off_r, mut on_r, mut off_rad, mut on_rad) = (0.0, 0.0, 0.0, 0.0);
    for seed in 0..SEEDS {
        let cfg = config(dim, seed);
        let plain = ClusterIndex::build(&live, refine(&cfg, false)).unwrap();
        let refined = ClusterIndex::build(&live, refine(&cfg, true)).unwrap();
        assert_same_ids(&refined, &live);
        for size in refined.posting_sizes() {
            assert!(
                (cfg.min_posting_size..=cfg.max_posting_size).contains(&size),
                "posting size {size} outside [{}, {}]",
                cfg.min_posting_size,
                cfg.max_posting_size
            );
        }
        let (ps, rs) = (plain.snapshot(), refined.snapshot());
        off_r += radius_recall(&ps, &live, &qs, 0.05);
        on_r += radius_recall(&rs, &live, &qs, 0.05);
        off_rad += median_radius(&ps) as f64;
        on_rad += median_radius(&rs) as f64;
    }
    let n = SEEDS as f64;
    let (off_r, on_r, off_rad, on_rad) = (off_r / n, on_r / n, off_rad / n, on_rad / n);
    eprintln!(
        "radius-aware recall@10 at 5%: off {off_r:.4}, on {on_r:.4}; median radius off {off_rad:.3}, on {on_rad:.3}"
    );
    assert!(
        on_rad < off_rad * 0.95,
        "median radius {on_rad:.3} vs {off_rad:.3}"
    );
    assert!(
        on_r >= 0.95,
        "recall with refinement {on_r:.4} (off {off_r:.4})"
    );
    assert!(on_r >= off_r + 0.2, "recall {on_r:.4} vs {off_r:.4}");
}

/// `build_refine_rounds = 0` is the plain bisection build: deterministic,
/// and no rebalance after it (the refinement is the only difference).
#[test]
fn build_refinement_off_is_deterministic_and_on_is_too() {
    let dim = 16;
    let live = mixture(3_000, dim, 20, 0, 0.0, 9);
    for on in [false, true] {
        let cfg = refine(&config(dim, 4), on);
        let a = ClusterIndex::build(&live, cfg.clone()).unwrap().snapshot();
        let b = ClusterIndex::build(&live, cfg).unwrap().snapshot();
        assert_eq!(a, b, "refine {on}: same input and seed, same index");
    }
}

/// Absorbing loose vectors goes through the coarse centroid index once the
/// index has 256+ postings: each vector must still land in its exact
/// nearest posting almost always, and in exactly one posting.
#[test]
fn absorb_through_the_centroid_index_matches_exact_nearest_posting() {
    let dim = 32;
    let big = mixture(40_000, dim, 40, 0, 0.0, 1);
    let cfg = config(dim, 0);
    let seeds = ClusterIndex::build(&big, cfg.clone()).unwrap().snapshot();
    assert!(seeds.len() >= 256, "{} postings", seeds.len());
    let centroids: Vec<Vec<f32>> = seeds.iter().map(|p| p.centroid.clone()).collect();
    let mut idx = ClusterIndex::from_postings(seeds, cfg).unwrap();
    let fresh = mixture(2_000, dim, 40, 40_000, 0.0, 2);
    let loose: Vec<PostingSnapshot> = fresh
        .iter()
        .map(|(id, v)| PostingSnapshot {
            centroid: v.clone(),
            entries: vec![(*id, v.clone())],
            radius: 0.0,
        })
        .collect();
    assert_eq!(
        idx.absorb_postings(loose, ABSORB_CANDIDATES).unwrap(),
        2_000
    );
    let live: Vec<(u32, Vec<f32>)> = big.iter().chain(fresh.iter()).cloned().collect();
    assert_same_ids(&idx, &live);
    // `absorb_postings` creates no postings, so snapshot order is the
    // seeds' order.
    let snap = idx.snapshot();
    assert_eq!(snap.len(), centroids.len());
    let mut owner = std::collections::HashMap::new();
    for (i, p) in snap.iter().enumerate() {
        for (id, _) in &p.entries {
            owner.insert(*id, i);
        }
    }
    let agree = fresh
        .iter()
        .filter(|(id, v)| nearest_centroids(&centroids, v, 1)[0] == owner[id])
        .count();
    let agree = agree as f64 / fresh.len() as f64;
    eprintln!("absorbed into the exact nearest posting: {agree:.4}");
    assert!(
        agree >= 0.97,
        "only {agree:.4} absorbed into the exact nearest posting"
    );
}

/// A tiered merge: four inputs of equal size, each clustered on its own
/// (overlapping partitions of one space), plus a little churn.
#[test]
fn equal_size_inputs_merge_recall_matches_rebuild_within_half_a_point() {
    let dim = 32;
    let parts: Vec<Vec<(u32, Vec<f32>)>> = (0..4)
        .map(|s| mixture(2_000, dim, 40, s * 2_000, 0.0, 20 + s as u64))
        .collect();
    let mut rng = DeterministicRng::new(9);
    let mut deleted = HashSet::new();
    while deleted.len() < 200 {
        deleted.insert(rng.next_usize(8_000) as u32);
    }
    let live: Vec<(u32, Vec<f32>)> = parts
        .iter()
        .flatten()
        .filter(|(id, _)| !deleted.contains(id))
        .cloned()
        .collect();
    let qs = queries(dim, 0.0);
    let (mut r_sum, mut l_sum) = (0.0, 0.0);
    for seed in 0..SEEDS {
        let cfg = config(dim, seed);
        let rebuilt = ClusterIndex::build(&live, cfg.clone()).unwrap();
        let inputs = parts.iter().map(|p| input_of(p.clone(), &cfg)).collect();
        let merged = lire_merge(inputs, &deleted, &cfg);
        assert_same_ids(&merged, &live);
        r_sum += recall(&rebuilt, &live, &qs, 0.05);
        l_sum += recall(&merged, &live, &qs, 0.05);
    }
    let (r, l) = (r_sum / SEEDS as f64, l_sum / SEEDS as f64);
    eprintln!("4 equal inputs: rebuild recall@10 {r:.4}, lire {l:.4}");
    assert!(
        l >= r - 0.005,
        "lire recall {l:.4} is more than 0.5pt below rebuild {r:.4}"
    );
}

/// The churn benchmark, compaction-shaped: every cycle deletes 5% of the
/// live set, writes 5% new vectors from a drifting distribution across
/// four flush segments, then compacts everything into one segment —
/// either rebuilt from scratch, or LIRE-merged from the previous cycle's
/// merged postings. Errors would compound across cycles if the merged
/// index degraded.
#[test]
fn churn_cycles_recall_matches_rebuild_within_half_a_point() {
    let dim = 32;
    let cycles = 6;
    let (mut r_sum, mut l_sum) = (0.0, 0.0);
    let mut final_qs = Vec::new();
    for seed in 0..SEEDS {
        let cfg = config(dim, seed);
        let mut live = mixture(6_000, dim, 40, 0, 0.0, 1);
        let mut next_id = live.len() as u32;
        let mut merged = ClusterIndex::build(&live, cfg.clone()).unwrap();
        let mut rng = DeterministicRng::new(11 + seed);
        for cycle in 1..=cycles {
            let drift = 0.05 * cycle as f32;
            let churn = live.len() / 20;
            let mut deleted = HashSet::new();
            while deleted.len() < churn {
                deleted.insert(live[rng.next_usize(live.len())].0);
            }
            live.retain(|(id, _)| !deleted.contains(id));
            let mut inputs = vec![Input::Postings(merged.snapshot())];
            for s in 0..4 {
                let seg = mixture(churn / 4, dim, 40, next_id, drift, 1_000 * cycle as u64 + s);
                next_id += seg.len() as u32;
                live.extend(seg.iter().cloned());
                inputs.push(Input::Vectors(seg));
            }
            merged = lire_merge(inputs, &deleted, &cfg);
            assert_same_ids(&merged, &live);
        }
        let qs = queries(dim, 0.05 * cycles as f32);
        let rebuilt = ClusterIndex::build(&live, cfg.clone()).unwrap();
        r_sum += recall(&rebuilt, &live, &qs, 0.05);
        l_sum += recall(&merged, &live, &qs, 0.05);
        final_qs = qs;
    }
    assert!(!final_qs.is_empty());
    let (r, l) = (r_sum / SEEDS as f64, l_sum / SEEDS as f64);
    eprintln!("after {cycles} churn cycles: rebuild recall@10 {r:.4}, lire {l:.4}");
    // Merged postings come out *better* than a rebuild here (LIRE only
    // re-clusters what churn touched; a rebuild re-applies the bisection's
    // approximation noise everywhere, see README.md) — the criterion is
    // that they are no worse.
    assert!(
        l >= r - 0.005,
        "lire recall {l:.4} is more than 0.5pt below rebuild {r:.4} after {cycles} cycles"
    );
}

#[test]
fn from_postings_keeps_postings_and_centroids_verbatim() {
    let dim = 4;
    let vs = mixture(300, dim, 5, 0, 0.0, 3);
    let cfg = config(dim, 0);
    let built = ClusterIndex::build(&vs, cfg.clone()).unwrap();
    let snap = built.snapshot();
    let seeded = ClusterIndex::from_postings(snap.clone(), cfg).unwrap();
    assert_eq!(seeded.snapshot(), snap);
    assert_eq!(seeded.len(), vs.len());
}

#[test]
fn from_postings_skips_empty_and_rejects_bad_input() {
    let cfg = config(2, 0);
    let p = |c: Vec<f32>, e: Vec<(u32, Vec<f32>)>| PostingSnapshot {
        centroid: c,
        entries: e,
        radius: 0.0,
    };
    let idx = ClusterIndex::from_postings(
        vec![
            p(vec![1.0, 0.0], vec![]),
            p(vec![0.0, 1.0], vec![(1, vec![0.0, 1.0])]),
        ],
        cfg.clone(),
    )
    .unwrap();
    assert_eq!(idx.active_posting_count(), 1);

    let dup = ClusterIndex::from_postings(
        vec![
            p(vec![1.0, 0.0], vec![(1, vec![1.0, 0.0])]),
            p(vec![0.0, 1.0], vec![(1, vec![0.0, 1.0])]),
        ],
        cfg.clone(),
    );
    assert_eq!(dup.unwrap_err(), VectorIndexError::DuplicateId(1));

    let bad_dim = ClusterIndex::from_postings(
        vec![p(vec![1.0, 0.0], vec![(1, vec![1.0, 0.0, 0.0])])],
        cfg.clone(),
    );
    assert!(matches!(
        bad_dim.unwrap_err(),
        VectorIndexError::DimensionMismatch {
            expected: 2,
            got: 3
        }
    ));
    let bad_centroid =
        ClusterIndex::from_postings(vec![p(vec![1.0], vec![(1, vec![1.0, 0.0])])], cfg);
    assert!(matches!(
        bad_centroid.unwrap_err(),
        VectorIndexError::DimensionMismatch {
            expected: 2,
            got: 1
        }
    ));
}

#[test]
fn rebalance_splits_far_oversized_postings_down_to_the_cap_and_merges_runts() {
    let dim = 8;
    let cfg = config(dim, 0);
    let vs = mixture(1_000, dim, 10, 0, 0.0, 5);
    // One posting ~8x the cap, plus a runt.
    let centroid = mean_vector(vs[..990].iter().map(|(_, v)| v.as_slice()), dim);
    let runt_centroid = mean_vector(vs[990..].iter().map(|(_, v)| v.as_slice()), dim);
    let mut idx = ClusterIndex::from_postings(
        vec![
            PostingSnapshot {
                centroid,
                entries: vs[..990].to_vec(),
                radius: 0.0,
            },
            PostingSnapshot {
                centroid: runt_centroid,
                entries: vs[990..].to_vec(),
                radius: 0.0,
            },
        ],
        cfg.clone(),
    )
    .unwrap();
    let stats = idx.rebalance();
    // Top-level splits only; the rest happen in Reassign's split cascade,
    // which can also absorb the runt before the merge pass sees it.
    assert!(stats.postings_split >= 1, "{stats:?}");
    assert!(idx.active_posting_count() >= 8, "{:?}", idx.posting_sizes());
    for size in idx.posting_sizes() {
        assert!(
            (cfg.min_posting_size..=cfg.max_posting_size).contains(&size),
            "posting of {size} outside [{}, {}]",
            cfg.min_posting_size,
            cfg.max_posting_size
        );
    }
    assert_same_ids(&idx, &vs);
}

/// The cost claim: a merge that disturbs nothing does no LIRE work.
#[test]
fn rebalance_of_untouched_postings_does_nothing() {
    let dim = 16;
    let cfg = config(dim, 0);
    let a = ClusterIndex::build(&mixture(6_000, dim, 20, 0, 0.0, 1), cfg.clone()).unwrap();
    let seeds = a.snapshot();
    let before = seeds.clone();
    let mut idx = ClusterIndex::from_postings(seeds, cfg).unwrap();
    let stats = idx.rebalance();
    assert_eq!(stats, Default::default());
    assert_eq!(idx.snapshot(), before);
}

/// Wall-clock numbers for the PR / README (`cargo test --release -p
/// kosha-vector-spfresh --test compaction_merge -- --ignored --nocapture`).
///
/// 1. From-scratch build with refinement off and on (#253): build time,
///    median posting radius, radius-aware recall@10 at the 5% floor.
///    Vectors are unit-normalized, as kosha-segment builds them.
/// 2. Compaction's index step, rebuild vs lire, one big input plus `pct`%
///    changed vectors, with the big input built with refinement off and
///    on. Absorption goes through the coarse centroid index either way.
///
/// The criterion version is `benches/build_query_cost.rs`
/// (`build_cost`, `compaction_vector_index`).
#[test]
#[ignore]
fn compaction_cost_report() {
    let dim = 128;
    let unit = |vs: Vec<(u32, Vec<f32>)>| -> Vec<(u32, Vec<f32>)> {
        vs.into_iter()
            .map(|(id, mut v)| {
                normalize_in_place(&mut v);
                (id, v)
            })
            .collect()
    };
    let qs: Vec<Vec<f32>> = unit(mixture(200, dim, 200, 0, 0.0, 999))
        .into_iter()
        .map(|(_, v)| v)
        .collect();
    for n in [1_000usize, 2_000, 5_000, 10_000, 100_000, 200_000] {
        let live = unit(mixture(n, dim, 200, 0, 0.0, 1));
        let reps = if n <= 10_000 { 5 } else { 1 };
        let mut line = format!("build n={n} dim={dim}:");
        let mut times = [0.0f64; 2];
        for (i, on) in [false, true].into_iter().enumerate() {
            let cfg = refine(&config(dim, 0), on);
            // Best of `reps`: these are milliseconds at flush sizes.
            let mut best = f64::INFINITY;
            let mut idx = None;
            for _ in 0..reps {
                let t = std::time::Instant::now();
                let built = ClusterIndex::build(&live, cfg.clone()).unwrap();
                best = best.min(t.elapsed().as_secs_f64() * 1e3);
                idx = Some(built);
            }
            times[i] = best;
            let snap = idx.unwrap().snapshot();
            let r = radius_recall(&snap, &live, &qs, 0.05);
            line += &format!(
                " refine {} {best:.1} ms, {} postings, radius p50 {:.3} p90 {:.3}, recall@10@5% {r:.3};",
                if on { "on" } else { "off" },
                snap.len(),
                median_radius(&snap),
                radius_quantile(&snap, 0.9)
            );
        }
        eprintln!(
            "{line} overhead {:+.0}%",
            (times[1] / times[0] - 1.0) * 100.0
        );
    }
    for n in [10_000usize, 50_000, 100_000, 200_000] {
        for pct in [1usize, 5] {
            for on in [false, true] {
                let cfg = refine(&config(dim, 0), on);
                let big = unit(mixture(n, dim, 200, 0, 0.0, 1));
                let changed = n * pct / 100;
                let mut rng = DeterministicRng::new(3);
                let mut deleted = HashSet::new();
                while deleted.len() < changed {
                    deleted.insert(rng.next_usize(n) as u32);
                }
                let fresh = unit(mixture(changed, dim, 200, n as u32, 0.0, 2));
                let live: Vec<(u32, Vec<f32>)> = big
                    .iter()
                    .filter(|(id, _)| !deleted.contains(id))
                    .chain(fresh.iter())
                    .cloned()
                    .collect();
                let snapshot = ClusterIndex::build(&big, cfg.clone()).unwrap().snapshot();

                let t = std::time::Instant::now();
                let rebuilt = ClusterIndex::build(&live, cfg.clone()).unwrap();
                let rebuild = t.elapsed();
                let t = std::time::Instant::now();
                let merged = lire_merge(
                    vec![Input::Postings(snapshot), Input::Vectors(fresh)],
                    &deleted,
                    &cfg,
                );
                let lire = t.elapsed();
                eprintln!(
                    "n={n} dim={dim} changed={pct}% refine {}: rebuild {:.0} ms ({} postings, median radius {:.3}), lire {:.0} ms ({} postings, median radius {:.3}), {:.1}x",
                    if on { "on" } else { "off" },
                    rebuild.as_secs_f64() * 1e3,
                    rebuilt.active_posting_count(),
                    median_radius(&rebuilt.snapshot()),
                    lire.as_secs_f64() * 1e3,
                    merged.active_posting_count(),
                    median_radius(&merged.snapshot()),
                    rebuild.as_secs_f64() / lire.as_secs_f64()
                );
            }
        }
    }
    // Worst case for LIRE: a tiered merge of four equal inputs, where
    // every adopted posting absorbs ~3x its size and is split.
    for n in [50_000usize, 200_000] {
        let cfg = config(dim, 0);
        let parts: Vec<Vec<(u32, Vec<f32>)>> = (0..4)
            .map(|p| mixture(n / 4, dim, 200, (p * n / 4) as u32, 0.0, 30 + p as u64))
            .collect();
        let live: Vec<(u32, Vec<f32>)> = parts.iter().flatten().cloned().collect();
        let inputs: Vec<Input> = parts
            .iter()
            .map(|p| Input::Postings(ClusterIndex::build(p, cfg.clone()).unwrap().snapshot()))
            .collect();
        let t = std::time::Instant::now();
        ClusterIndex::build(&live, cfg.clone()).unwrap();
        let rebuild = t.elapsed();
        let t = std::time::Instant::now();
        lire_merge(inputs, &HashSet::new(), &cfg);
        let lire = t.elapsed();
        eprintln!(
            "n={n} dim={dim} 4 equal inputs: rebuild {:.0} ms, lire {:.0} ms",
            rebuild.as_secs_f64() * 1e3,
            lire.as_secs_f64() * 1e3
        );
    }
}
