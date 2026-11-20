//! Ack latency and throughput of the object-store WAL (#236).
//!
//!     cargo bench -p kosha-write --bench s3_wal_ack
//!
//! Each write is one ~500 kB batch (100 documents of 1 kB text plus a
//! 1024-dimension vector), the size turbopuffer quotes its p50 for. The
//! WAL runs on the in-memory object store with a simulated S3 PUT cost of
//! `KOSHA_BENCH_S3_PUT_MS` (default 25) plus the body at
//! `KOSHA_BENCH_S3_MBPS` (default 100 MB/s); real S3 adds its own variance,
//! so these numbers isolate what group commit does with a given PUT cost.
//! Prints one row per writer count (1, 4, 16; one namespace per writer,
//! since writes to one namespace serialize) and group-commit window
//! (`KOSHA_WAL_GROUP_COMMIT_MS` 0 and 2): writes/s, MB/s, p50/p99 ack
//! latency and PUTs per write. `KOSHA_BENCH_S3_WRITES` sets writes per row
//! (default 160).
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use kosha_core::{Document, DocumentId, Field, NamespaceId};
use kosha_write::object_wal::{MemoryObjectBackend, ObjectWalStore};
use kosha_write::wal::{GroupCommitConfig, WalWriter};

const WRITERS: [usize; 3] = [1, 4, 16];
const DELAYS_MS: [u64; 2] = [0, 2];
const DOCS_PER_WRITE: usize = 100;

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn batch(writer: usize, n: usize) -> Vec<Document> {
    (0..DOCS_PER_WRITE)
        .map(|d| Document {
            id: DocumentId(format!("w{writer}-{n}-{d}")),
            fields: vec![
                Field::text("body", "lorem ipsum dolor sit amet ".repeat(37)),
                Field::vector(
                    "embedding",
                    (0..1024)
                        .map(|i| ((i + d + n) % 97) as f32 * 0.01)
                        .collect(),
                ),
            ],
        })
        .collect()
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1]
}

fn main() {
    let put_ms: u64 = env("KOSHA_BENCH_S3_PUT_MS", 25);
    let mbps: u64 = env("KOSHA_BENCH_S3_MBPS", 100);
    let total: usize = env("KOSHA_BENCH_S3_WRITES", 160);
    let write_bytes = {
        let backend = MemoryObjectBackend::new();
        let store = ObjectWalStore::new(Arc::new(backend.clone()), "size").unwrap();
        let (wal, _) = WalWriter::open(Arc::new(store), GroupCommitConfig::default()).unwrap();
        wal.append(&NamespaceId("ns".into()), &batch(0, 0)).unwrap();
        let key = backend
            .keys()
            .into_iter()
            .find(|k| k.ends_with(".bin"))
            .unwrap();
        kosha_write::object_wal::WalObjectBackend::get(&backend, &key)
            .unwrap()
            .unwrap()
            .len()
    };
    println!(
        "simulated PUT: {put_ms} ms + body at {mbps} MB/s; {} kB per write ({DOCS_PER_WRITE} docs)",
        write_bytes / 1000
    );
    println!("writers window_ms writes writes/s   MB/s p50_ms p99_ms puts/write");
    for writers in WRITERS {
        for delay in DELAYS_MS {
            let backend = MemoryObjectBackend::new();
            backend.set_put_latency(Duration::from_millis(put_ms), 1000 / mbps.max(1));
            let store = ObjectWalStore::new(Arc::new(backend.clone()), "bench").unwrap();
            let (wal, _) = WalWriter::open(
                Arc::new(store),
                GroupCommitConfig {
                    max_delay: Duration::from_millis(delay),
                    ..GroupCommitConfig::default()
                },
            )
            .unwrap();
            let wal = Arc::new(wal);
            let per_writer = total.div_ceil(writers);
            let barrier = Arc::new(Barrier::new(writers + 1));
            let handles: Vec<_> = (0..writers)
                .map(|w| {
                    let wal = wal.clone();
                    let barrier = barrier.clone();
                    std::thread::spawn(move || {
                        let ns = NamespaceId(format!("ns-{w}"));
                        let batches: Vec<_> = (0..per_writer).map(|n| batch(w, n)).collect();
                        barrier.wait();
                        batches
                            .iter()
                            .map(|docs| {
                                let start = Instant::now();
                                wal.append(&ns, docs).unwrap();
                                start.elapsed()
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            barrier.wait();
            let start = Instant::now();
            let mut latencies: Vec<Duration> = handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect();
            let wall = start.elapsed().as_secs_f64();
            latencies.sort();
            let writes = latencies.len();
            println!(
                "{writers:>7} {delay:>9} {writes:>6} {:>8.1} {:>6.1} {:>6.1} {:>6.1} {:>10.3}",
                writes as f64 / wall,
                (writes * write_bytes) as f64 / wall / 1e6,
                percentile(&latencies, 0.50).as_secs_f64() * 1e3,
                percentile(&latencies, 0.99).as_secs_f64() * 1e3,
                backend.puts() as f64 / writes as f64,
            );
        }
    }
}
