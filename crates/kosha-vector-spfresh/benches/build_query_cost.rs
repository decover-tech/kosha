//! Benchmarks this crate's `ClusterIndex` against kosha's real,
//! currently-shipping vector-search primitives
//! (`kosha_segment::build_hnsw`/`CosinePoint`, `kosha_query::flat_knn`) —
//! the concrete, measured version of the "dominant cost of opening a
//! segment" problem noted in `kosha-segment`'s own code comments (every
//! segment open rebuilds its HNSW graph from scratch; nothing is
//! persisted).
//!
//! Run: `cargo bench -p kosha-vector-spfresh --bench build_query_cost`
//! (`-- compaction_vector_index` for just the #253 compaction comparison,
//! `-- build_refine` for the post-build refinement's cost).
//! Not wired into CI's `bench-compare.yml` gate (that only runs
//! `kosha-query`'s `segment_memory` today) — this is a local-run comparison,
//! per README.md.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use kosha_segment::{build_hnsw, CosinePoint};
use kosha_vector_spfresh::{
    mean_vector, ClusterIndex, ClusterIndexConfig, DeterministicRng, PostingSnapshot,
};

const SIZES: &[usize] = &[1_000, 10_000, 50_000];
const DIMS: &[usize] = &[128, 1536];

/// A handful of loose clusters rather than pure uniform noise — closer to
/// real embedding data (which clusters by semantic content) than an
/// adversarial uniform-random point cloud would be, and closer to what
/// `ClusterIndex::build`'s balanced bisection is actually designed for.
fn gen_vectors(n: usize, dim: usize, seed: u64) -> Vec<(u32, Vec<f32>)> {
    let mut rng = DeterministicRng::new(seed);
    (0..n as u32)
        .map(|id| {
            let cluster = (id as usize) % 20;
            let angle = (cluster as f32) * std::f32::consts::TAU / 20.0;
            let mut v = vec![0.0f32; dim];
            v[0] = angle.cos() * 10.0 + rng.next_f32_range(-0.5, 0.5);
            if dim > 1 {
                v[1] = angle.sin() * 10.0 + rng.next_f32_range(-0.5, 0.5);
            }
            for x in v.iter_mut().skip(2) {
                *x = rng.next_f32_range(-0.1, 0.1);
            }
            (id, v)
        })
        .collect()
}

/// Criterion's default `sample_size` (100) is fine for the microsecond-scale
/// `spfresh_*` functions but not for `build_hnsw`/`flat_knn`, which are
/// linear-to-superlinear in `n` with a heavy constant factor — at
/// `n=10_000` a single `build_hnsw` call already takes ~1s, so 100 samples
/// means minutes for one benchmark line. Reduce samples once `n*dim`
/// crosses a size where that stops being a rounding error.
fn reduced_sample_size(n: usize, dim: usize) -> Option<usize> {
    let cost = n * dim;
    if cost > 10_000 * 1536 {
        Some(10)
    } else if cost > 1_000 * 128 {
        Some(20)
    } else {
        None
    }
}

fn bench_build_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_cost");
    for &dim in DIMS {
        for &n in SIZES {
            let vectors = gen_vectors(n, dim, 1);
            group.throughput(Throughput::Elements(n as u64));
            if let Some(s) = reduced_sample_size(n, dim) {
                group.sample_size(s);
            }

            group.bench_function(format!("spfresh_build/{n}x{dim}"), |b| {
                b.iter(|| {
                    let cfg = ClusterIndexConfig::new(dim);
                    black_box(ClusterIndex::build(black_box(&vectors), cfg).unwrap())
                })
            });
            group.bench_function(format!("hnsw_rebuild/{n}x{dim}"), |b| {
                b.iter(|| black_box(build_hnsw(black_box(&vectors))))
            });
        }
    }
    group.finish();
}

/// Post-build refinement (#253): `ClusterIndex::build` with
/// `build_refine_rounds` at its default and at 0 (plain bisection, what
/// `KOSHA_VECTOR_BUILD_REFINE=off` builds), from flush sizes up to a large
/// compaction rebuild.
fn bench_build_refine(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_refine");
    group.sample_size(10);
    let dim = 128;
    for &n in &[1_000usize, 10_000, 100_000] {
        let vectors = gen_vectors(n, dim, 1);
        group.throughput(Throughput::Elements(n as u64));
        for on in [false, true] {
            let mut cfg = ClusterIndexConfig::new(dim);
            if !on {
                cfg.build_refine_rounds = 0;
            }
            let name = if on { "refine_on" } else { "refine_off" };
            group.bench_function(format!("{name}/{n}x{dim}"), |b| {
                b.iter(|| black_box(ClusterIndex::build(black_box(&vectors), cfg.clone()).unwrap()))
            });
        }
    }
    group.finish();
}

/// What kosha pays today on *any* segment update: read the whole
/// `vector.idx`, rebuild the HNSW graph from scratch. Compared against this
/// crate's actual incremental `insert` — the whole point of LIRE.
fn bench_incremental_update_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("incremental_update_cost");
    for &dim in DIMS {
        for &n in SIZES {
            let base_vectors = gen_vectors(n, dim, 2);
            let extra: Vec<(u32, Vec<f32>)> = gen_vectors(n / 100, dim, 3)
                .into_iter()
                .map(|(id, v)| (id + n as u32, v)) // shift ids past base_vectors' range
                .collect();

            group.throughput(Throughput::Elements(extra.len() as u64));
            if let Some(s) = reduced_sample_size(n, dim) {
                group.sample_size(s);
            }

            group.bench_function(format!("spfresh_insert_1pct/{n}x{dim}"), |b| {
                b.iter_batched(
                    || ClusterIndex::build(&base_vectors, ClusterIndexConfig::new(dim)).unwrap(),
                    |mut idx| {
                        for (id, v) in &extra {
                            idx.insert(*id, v.clone()).unwrap();
                        }
                        black_box(idx)
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function(
                format!("hnsw_full_rebuild_after_1pct_change/{n}x{dim}"),
                |b| {
                    b.iter_batched(
                        || {
                            let mut all = base_vectors.clone();
                            all.extend(extra.iter().cloned());
                            all
                        },
                        |all| black_box(build_hnsw(black_box(&all))),
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }
    group.finish();
}

fn bench_query_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_latency");
    for &dim in DIMS {
        for &n in SIZES {
            let vectors = gen_vectors(n, dim, 4);
            let spfresh_idx = ClusterIndex::build(&vectors, ClusterIndexConfig::new(dim)).unwrap();
            let (hnsw_map, _) = build_hnsw(&vectors).expect("non-empty vector set");
            let query = vectors[0].1.clone();

            if let Some(s) = reduced_sample_size(n, dim) {
                group.sample_size(s);
            }

            group.bench_function(format!("spfresh_search_k10/{n}x{dim}"), |b| {
                b.iter(|| black_box(spfresh_idx.search(black_box(&query), 10).unwrap()))
            });

            group.bench_function(format!("hnsw_search_k10/{n}x{dim}"), |b| {
                b.iter(|| {
                    let point = CosinePoint(query.clone());
                    let mut search = instant_distance::Search::default();
                    let hits: Vec<u32> = hnsw_map
                        .search(&point, &mut search)
                        .take(10)
                        .map(|item| *item.value)
                        .collect();
                    black_box(hits)
                })
            });

            // flat_knn only at the smaller sizes: it's O(n) per query with
            // no approximation at all, and at n=50,000 x dim=1536 the
            // wall-clock cost swamps criterion's iteration budget for no
            // additional signal (kosha itself only takes this path for
            // small segments — see kosha_query::flat_knn's doc comment).
            if n <= 10_000 {
                group.bench_function(format!("flat_knn/{n}x{dim}"), |b| {
                    b.iter(|| {
                        black_box(kosha_query::flat_knn(
                            black_box(&query),
                            black_box(&vectors),
                            10,
                        ))
                    })
                });
            }
        }
    }
    group.finish();
}

/// Compaction's vector-index step (#253): merge N input segments into one
/// index, either clustering every live vector from scratch (`rebuild`,
/// what compaction did before) or seeding from the largest input's
/// postings, absorbing the other inputs' postings and loose vectors, and
/// letting LIRE repair what changed (`lire`, what kosha-segment's
/// `seed_cluster_index` does). Two shapes:
///
/// * `big+flushes`: one large, previously compacted segment of `n`
///   vectors, `pct`% of them updated — deleted there, new versions in 4
///   flush-sized segments (too small to adopt, absorbed vector by vector).
///   The common steady state: LIRE's cost should track `pct`, not `n`.
/// * `4_equal`: a tiered merge of four equal inputs (`n/4` each, 1% of
///   them deleted): every input's postings overlap the base's, so most of
///   the index changes — the worst case for LIRE.
///
/// Input snapshots are built outside the timed region: compaction reads
/// them from the inputs' `vector.offsets`, it does not re-cluster them.
fn bench_compaction_vector_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction_vector_index");
    group.sample_size(10);
    for &dim in &[128usize] {
        for &n in &[10_000usize, 50_000] {
            for &pct in &[1usize, 5] {
                let cfg = ClusterIndexConfig::new(dim);
                let base = gen_vectors(n, dim, 5);
                let changed = n * pct / 100;
                let mut rng = DeterministicRng::new(6);
                let mut deleted = std::collections::HashSet::new();
                while deleted.len() < changed {
                    deleted.insert(rng.next_usize(n) as u32);
                }
                let fresh: Vec<(u32, Vec<f32>)> = gen_vectors(changed, dim, 7)
                    .into_iter()
                    .map(|(id, v)| (id + n as u32, v))
                    .collect();
                let base_postings = ClusterIndex::build(&base, cfg.clone()).unwrap().snapshot();
                let live: Vec<(u32, Vec<f32>)> = base
                    .iter()
                    .filter(|(id, _)| !deleted.contains(id))
                    .chain(fresh.iter())
                    .cloned()
                    .collect();
                group.throughput(Throughput::Elements(live.len() as u64));
                group.bench_function(format!("rebuild/big+flushes/{n}x{dim}/{pct}pct"), |b| {
                    b.iter(|| {
                        black_box(ClusterIndex::build(black_box(&live), cfg.clone()).unwrap())
                    })
                });
                group.bench_function(format!("lire/big+flushes/{n}x{dim}/{pct}pct"), |b| {
                    b.iter_batched(
                        || (base_postings.clone(), fresh.clone()),
                        |(postings, fresh)| {
                            black_box(lire_merge(vec![postings], fresh, &deleted, &cfg))
                        },
                        BatchSize::LargeInput,
                    )
                });
            }

            // Tiered merge of four equal inputs.
            let cfg = ClusterIndexConfig::new(dim);
            let parts: Vec<Vec<(u32, Vec<f32>)>> = (0..4u64)
                .map(|p| {
                    gen_vectors(n / 4, dim, 10 + p)
                        .into_iter()
                        .map(|(id, v)| (id + (p as usize * n / 4) as u32, v))
                        .collect()
                })
                .collect();
            let mut rng = DeterministicRng::new(8);
            let mut deleted = std::collections::HashSet::new();
            while deleted.len() < n / 100 {
                deleted.insert(rng.next_usize(n) as u32);
            }
            let snapshots: Vec<Vec<PostingSnapshot>> = parts
                .iter()
                .map(|p| ClusterIndex::build(p, cfg.clone()).unwrap().snapshot())
                .collect();
            let live: Vec<(u32, Vec<f32>)> = parts
                .iter()
                .flatten()
                .filter(|(id, _)| !deleted.contains(id))
                .cloned()
                .collect();
            group.throughput(Throughput::Elements(live.len() as u64));
            group.bench_function(format!("rebuild/4_equal/{n}x{dim}"), |b| {
                b.iter(|| black_box(ClusterIndex::build(black_box(&live), cfg.clone()).unwrap()))
            });
            group.bench_function(format!("lire/4_equal/{n}x{dim}"), |b| {
                b.iter_batched(
                    || snapshots.clone(),
                    |snapshots| black_box(lire_merge(snapshots, Vec::new(), &deleted, &cfg)),
                    BatchSize::LargeInput,
                )
            });
        }
    }
    group.finish();
}

/// kosha-segment's `seed_cluster_index`, on in-memory snapshots: the
/// largest input's postings (minus `deleted`, recentred where something
/// was dropped) seed the index; the other inputs and `loose` vectors are
/// absorbed; LIRE rebalances.
fn lire_merge(
    inputs: Vec<Vec<PostingSnapshot>>,
    loose: Vec<(u32, Vec<f32>)>,
    deleted: &std::collections::HashSet<u32>,
    cfg: &ClusterIndexConfig,
) -> ClusterIndex {
    let mut groups: Vec<Vec<PostingSnapshot>> = inputs
        .into_iter()
        .map(|g| {
            g.into_iter()
                .map(|p| {
                    let before = p.entries.len();
                    let entries: Vec<(u32, Vec<f32>)> = p
                        .entries
                        .into_iter()
                        .filter(|(id, _)| !deleted.contains(id))
                        .collect();
                    let centroid = if entries.len() == before {
                        p.centroid
                    } else {
                        mean_vector(entries.iter().map(|(_, v)| v.as_slice()), cfg.dim)
                    };
                    PostingSnapshot {
                        centroid,
                        entries,
                        radius: 0.0,
                    }
                })
                .collect()
        })
        .collect();
    let size = |g: &Vec<PostingSnapshot>| g.iter().map(|p| p.entries.len()).sum::<usize>();
    let base = (0..groups.len()).max_by_key(|&i| size(&groups[i])).unwrap();
    let mut idx = ClusterIndex::from_postings(groups.swap_remove(base), cfg.clone()).unwrap();
    for g in groups {
        idx.absorb_postings(g, 8).unwrap();
    }
    let loose = loose
        .into_iter()
        .map(|(id, v)| PostingSnapshot {
            centroid: v.clone(),
            entries: vec![(id, v)],
            radius: 0.0,
        })
        .collect();
    idx.absorb_postings(loose, 8).unwrap();
    idx.rebalance();
    idx
}

criterion_group!(
    benches,
    bench_build_cost,
    bench_build_refine,
    bench_incremental_update_cost,
    bench_query_latency,
    bench_compaction_vector_index
);
criterion_main!(benches);
