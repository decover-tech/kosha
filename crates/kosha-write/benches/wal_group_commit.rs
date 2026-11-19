//! WAL append throughput and ack latency with group commit (#233).
//!
//!     cargo bench -p kosha-write --bench wal_group_commit
//!
//! Before the criterion samples, prints one table row per configuration:
//! appends/s, p50/p99 ack latency and fsync'd files per append, for 1 and
//! 16 concurrent writers (spread over 16 namespaces, as concurrent
//! appends to one namespace serialize on its write lock) and for
//! `KOSHA_WAL_GROUP_COMMIT_MS` 0 and 2. `KOSHA_BENCH_WAL_APPENDS` sets the
//! appends per configuration (default 800).
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion};
use kosha_core::{Document, DocumentId, Field, NamespaceId};
use kosha_write::wal::{GroupCommitConfig, WalWriter};

const WRITERS: [usize; 2] = [1, 16];
const DELAYS_MS: [u64; 2] = [0, 2];

fn document(n: usize) -> Document {
    Document {
        id: DocumentId(format!("doc-{n}")),
        fields: vec![
            Field::text(
                "body",
                "the quick brown fox jumps over the lazy dog ".repeat(8),
            ),
            Field::keyword("status", "active"),
            Field::vector(
                "embedding",
                (0..128).map(|d| (d * n) as f32 * 1e-3).collect(),
            ),
        ],
    }
}

fn open(delay_ms: u64) -> (WalWriter, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("kosha-wal-bench-{}", uuid::Uuid::new_v4()));
    let wal = WalWriter::with_config(
        dir.clone(),
        GroupCommitConfig {
            max_delay: Duration::from_millis(delay_ms),
            ..GroupCommitConfig::default()
        },
    );
    (wal, dir)
}

/// Runs `total` appends over `writers` threads; returns per-append ack
/// latencies and the wall time.
fn run(wal: &Arc<WalWriter>, writers: usize, total: usize) -> (Vec<Duration>, Duration) {
    let per_writer = total.div_ceil(writers);
    let barrier = Arc::new(Barrier::new(writers + 1));
    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let wal = wal.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let ns = NamespaceId(format!("ns-{w}"));
                let docs: Vec<_> = (0..per_writer).map(document).collect();
                barrier.wait();
                docs.iter()
                    .map(|doc| {
                        let start = Instant::now();
                        wal.append(&ns, std::slice::from_ref(doc)).unwrap();
                        start.elapsed()
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    barrier.wait();
    let start = Instant::now();
    let latencies = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    (latencies, start.elapsed())
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1]
}

fn report() {
    let total: usize = std::env::var("KOSHA_BENCH_WAL_APPENDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(800);
    println!("writers delay_ms appends appends/s p50_ms p99_ms files/append");
    for writers in WRITERS {
        for delay in DELAYS_MS {
            let (wal, dir) = open(delay);
            let wal = Arc::new(wal);
            let (mut latencies, wall) = run(&wal, writers, total);
            latencies.sort();
            let appends = latencies.len();
            println!(
                "{writers:>7} {delay:>8} {appends:>7} {:>9.0} {:>6.2} {:>6.2} {:>12.3}",
                appends as f64 / wall.as_secs_f64(),
                percentile(&latencies, 0.50).as_secs_f64() * 1e3,
                percentile(&latencies, 0.99).as_secs_f64() * 1e3,
                wal.files_written() as f64 / appends as f64,
            );
            drop(wal);
            std::fs::remove_dir_all(dir).ok();
        }
    }
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_append");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    for writers in WRITERS {
        for delay in DELAYS_MS {
            group.bench_with_input(
                BenchmarkId::new(format!("{writers}_writers"), format!("{delay}ms")),
                &(writers, delay),
                |b, &(writers, delay)| {
                    let (wal, dir) = open(delay);
                    let wal = Arc::new(wal);
                    // One iteration is one acknowledged append.
                    b.iter_custom(|iters| run(&wal, writers, iters.max(1) as usize).1);
                    drop(wal);
                    std::fs::remove_dir_all(dir).ok();
                },
            );
        }
    }
    group.finish();
}

fn main() {
    report();
    let mut criterion = Criterion::default().configure_from_args();
    bench(&mut criterion);
    criterion.final_summary();
}
