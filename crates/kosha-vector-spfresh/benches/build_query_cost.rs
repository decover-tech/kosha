//! Benchmarks this crate's `ClusterIndex` against kosha's real,
//! currently-shipping vector-search primitives
//! (`kosha_segment::build_hnsw`/`CosinePoint`, `kosha_query::flat_knn`) —
//! the concrete, measured version of the "dominant cost of opening a
//! segment" problem noted in `kosha-segment`'s own code comments (every
//! segment open rebuilds its HNSW graph from scratch; nothing is
//! persisted).
//!
//! Run: `cargo bench -p kosha-vector-spfresh --bench build_query_cost`.
//! Not wired into CI's `bench-compare.yml` gate (that only runs
//! `kosha-query`'s `segment_memory` today) — this is a local-run comparison,
//! per README.md.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use kosha_segment::{build_hnsw, CosinePoint};
use kosha_vector_spfresh::{ClusterIndex, ClusterIndexConfig, DeterministicRng};

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

criterion_group!(
    benches,
    bench_build_cost,
    bench_incremental_update_cost,
    bench_query_latency
);
criterion_main!(benches);
