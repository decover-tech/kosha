//! Distance-kernel microbenchmarks for #382: the f32 dot (a serial sum,
//! the current 8-lane [`dot`], and wider accumulators) and the RaBitQ
//! estimator (the per-byte lookup table vs 4-bit-query AND + popcount),
//! at the 1024-dim shape of the MSMarco bench corpus.
//!
//! Each case scores one query against a posting-sized batch of
//! [`POSTING`] entries, the unit of work in `for_each_similarity`, so the
//! numbers include streaming the entries (and, for the LUT, its 128 KiB
//! table) through the cache. Build flags matter: compare a default build
//! with `RUSTFLAGS="-C target-cpu=x86-64-v3"` (or `native`) on the bench
//! host. Numbers from an arm64 laptop say nothing about AVX2/AVX-512.
//!
//! ```text
//! cargo bench -p kosha-vector-spfresh --bench distance_kernels
//! RUSTFLAGS="-C target-cpu=x86-64-v3" cargo bench -p kosha-vector-spfresh --bench distance_kernels
//! ```

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kosha_vector_spfresh::{
    dot, dot_lanes, encode_rabitq, normalize_in_place, DeterministicRng, RaBitQCode,
    RaBitQEstimator, RaBitQQuery, RaBitQRotation,
};

const DIM: usize = 1024;
/// Entries per posting: the order of a 10M-vector SPFresh posting.
const POSTING: usize = 256;

fn unit(rng: &mut DeterministicRng, dim: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.next_f32_range(-1.0, 1.0)).collect();
    normalize_in_place(&mut v);
    v
}

fn dot_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn bench_dot(c: &mut Criterion) {
    let mut rng = DeterministicRng::new(1);
    let q = unit(&mut rng, DIM);
    let entries: Vec<f32> = (0..POSTING).flat_map(|_| unit(&mut rng, DIM)).collect();
    let mut group = c.benchmark_group("dot_f32_1024x256");
    group.throughput(Throughput::Elements(POSTING as u64));
    macro_rules! case {
        ($name:expr, $f:expr) => {
            group.bench_function($name, |b| {
                b.iter(|| {
                    let mut s = 0.0f32;
                    for v in entries.chunks_exact(DIM) {
                        s += $f(black_box(&q), v);
                    }
                    s
                })
            });
        };
    }
    case!("dot_scalar", dot_scalar);
    case!("dot_current_8_lanes", dot);
    case!("dot_lanes_8", dot_lanes::<8>);
    case!("dot_lanes_16", dot_lanes::<16>);
    case!("dot_lanes_32", dot_lanes::<32>);
    case!("dot_lanes_64", dot_lanes::<64>);
    group.finish();
}

fn bench_rabitq(c: &mut Criterion) {
    let mut rng = DeterministicRng::new(2);
    let rot = RaBitQRotation::new(DIM);
    let centroid = unit(&mut rng, DIM);
    let q = unit(&mut rng, DIM);
    let mut codes: Vec<RaBitQCode> = Vec::with_capacity(POSTING);
    let mut bits: Vec<u8> = Vec::with_capacity(POSTING * rot.code_bytes());
    let mut scratch = Vec::new();
    for _ in 0..POSTING {
        let noise = unit(&mut rng, DIM);
        let mut o: Vec<f32> = centroid
            .iter()
            .zip(&noise)
            .map(|(c, n)| c + 0.6 * n)
            .collect();
        normalize_in_place(&mut o);
        codes.push(encode_rabitq(&rot, &o, &centroid, &mut scratch));
        bits.extend_from_slice(&scratch);
    }
    let cq = dot(&q, &centroid);
    let estimators = [
        ("lut", RaBitQEstimator::Lut),
        ("popcount4", RaBitQEstimator::Popcount4),
    ];

    let mut group = c.benchmark_group("rabitq_score_1024x256");
    group.throughput(Throughput::Elements(POSTING as u64));
    for (name, estimator) in estimators {
        let query = RaBitQQuery::with_estimator(&rot, &q, estimator);
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| {
                let mut s = 0.0f32;
                for (code, entry) in codes.iter().zip(bits.chunks_exact(rot.code_bytes())) {
                    let (est, err) = black_box(&query).estimate(code, entry, cq);
                    s += est + err;
                }
                s
            })
        });
    }
    group.finish();

    // Per-query setup: the rotation plus the LUT (128 KiB) or the bit
    // planes (512 B). Paid once per query per scoring thread.
    let mut group = c.benchmark_group("rabitq_query_prep_1024");
    for (name, estimator) in estimators {
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| RaBitQQuery::with_estimator(&rot, black_box(&q), estimator))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_dot, bench_rabitq);
criterion_main!(benches);
