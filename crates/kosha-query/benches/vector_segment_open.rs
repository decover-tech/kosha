//! Benchmarks the actual claim the whole `vector.idx` v2 effort rests on:
//! `SegmentReader::open` cost for a v2 (posting-based, lazy-centroid) segment
//! vs. a v1 (flat, eager `build_hnsw`-on-every-open) segment, at the same
//! vector count/dim. This is the concrete, measured version of the
//! "vector.idx reads and build_hnsw are the dominant cost of opening a
//! segment" problem the v1 path's own doc comment names.
//!
//! Run: `cargo bench -p kosha-query --bench vector_segment_open`. Not wired
//! into CI's `bench-compare.yml` gate (only `segment_memory` is gated
//! there) — local-run comparison, same convention as
//! `kosha-vector-spfresh/benches/build_query_cost.rs`.

use std::hint::black_box;
use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};
use kosha_segment::{SegmentReader, SegmentWriter};

/// A handful of loose clusters rather than pure uniform noise — matches the
/// generator shape used in `kosha-vector-spfresh`'s own bench, so numbers
/// from the two are comparable.
fn gen_vectors(n: usize, dim: usize) -> Vec<Vec<f32>> {
    (0..n)
        .map(|i| {
            let cluster = i % 20;
            let angle = (cluster as f32) * std::f32::consts::TAU / 20.0;
            let mut v = vec![0.0f32; dim];
            v[0] = angle.cos();
            if dim > 1 {
                v[1] = angle.sin();
            }
            for (j, x) in v.iter_mut().enumerate().skip(2) {
                *x = (((i * 7 + j * 3 + 3) % 100) as f32) / 100.0 - 0.5;
            }
            v
        })
        .collect()
}

/// Builds a v2 segment (today's `SegmentWriter` default).
fn build_v2_segment(dir: &PathBuf, n: usize, dim: usize) {
    let _ = std::fs::remove_dir_all(dir);
    let mut w = SegmentWriter::new(SegmentId("s1".into()), dir.clone());
    for (i, v) in gen_vectors(n, dim).into_iter().enumerate() {
        w.add_document(
            DocumentId(format!("d{i}")),
            vec![Field::vector("embedding", v)],
        );
    }
    w.finalize(Bm25Params::default()).unwrap();
}

/// Builds a v1 (legacy flat) segment: write a normal (v2) segment for
/// everything else, then replace `vector.idx`/`vector.offsets` with the
/// legacy byte layout by hand — `[dim:u32][count:u32]` then `count ×
/// [doc_seq:u32][f32×dim]` (see `kosha_segment`'s `read_vectors`). Pure
/// fixture construction, not a code path this benchmark measures — the
/// legacy *writer* isn't reachable cross-crate from a bench (it's
/// `KOSHA_VECTOR_WRITE_VERSION`-gated behind a process-wide `OnceLock`, and
/// this bench needs both formats side by side in one process).
fn build_v1_segment(dir: &PathBuf, n: usize, dim: usize) {
    build_v2_segment(dir, n, dim);
    let _ = std::fs::remove_file(dir.join("vector.offsets"));
    let vectors = gen_vectors(n, dim);
    let mut buf = Vec::new();
    buf.extend_from_slice(&(dim as u32).to_le_bytes());
    buf.extend_from_slice(&(n as u32).to_le_bytes());
    for (i, v) in vectors.iter().enumerate() {
        buf.extend_from_slice(&(i as u32).to_le_bytes());
        for &x in v {
            buf.extend_from_slice(&x.to_le_bytes());
        }
    }
    std::fs::write(dir.join("vector.idx"), &buf).unwrap();
}

fn bench_segment_open(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_segment_open");
    for &dim in &[128usize, 1536] {
        for &n in &[1_000usize, 10_000] {
            let v1_dir = std::env::temp_dir().join(format!("kosha-bench-v1-open-{n}x{dim}"));
            let v2_dir = std::env::temp_dir().join(format!("kosha-bench-v2-open-{n}x{dim}"));
            build_v1_segment(&v1_dir, n, dim);
            build_v2_segment(&v2_dir, n, dim);

            group.throughput(Throughput::Elements(n as u64));
            // The v1 (build_hnsw) side gets very slow at the larger cells
            // (matches kosha-vector-spfresh's own bench finding: ~5s/call
            // at n=10,000, dim=128) — reduce samples there so this
            // benchmark finishes in a reasonable time, same reasoning as
            // that bench's `reduced_sample_size`.
            if n * dim > 1_000 * 128 {
                group.sample_size(10);
            }

            group.bench_function(format!("v1_flat/{n}x{dim}"), |b| {
                b.iter(|| black_box(SegmentReader::open(black_box(v1_dir.clone())).unwrap()))
            });
            group.bench_function(format!("v2_lazy/{n}x{dim}"), |b| {
                b.iter(|| black_box(SegmentReader::open(black_box(v2_dir.clone())).unwrap()))
            });

            let _ = std::fs::remove_dir_all(&v1_dir);
            let _ = std::fs::remove_dir_all(&v2_dir);
        }
    }
    group.finish();
}

criterion_group!(benches, bench_segment_open);
criterion_main!(benches);
