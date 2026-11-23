//! Regression tests for #452: an index build (and a runtime insert) over
//! clumps of near-identical vectors must terminate.
//!
//! With such a clump in one oversized posting, a split "succeeds" but
//! Reassign moves both halves back together (every member is nearest the
//! same centroid by a rounding error). `ClusterIndex::rebalance` then split
//! the same posting forever, adding an empty posting per pass; on the 10M
//! MS MARCO build one segment never sealed.

use kosha_vector_spfresh::{ClusterIndex, ClusterIndexConfig};
use std::sync::mpsc;
use std::time::Duration;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn f(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 24) as f32 - 0.5
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn unit(mut v: Vec<f32>) -> Vec<f32> {
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-12);
    for x in &mut v {
        *x /= n;
    }
    v
}

/// Clumps of up to 700 vectors around random centres, each clump with its
/// own spread from exact duplicates (0) to loose (0.1), shuffled.
fn clumped_corpus(seed: u64, dim: usize, n: usize) -> Vec<(u32, Vec<f32>)> {
    let mut r = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut out: Vec<Vec<f32>> = Vec::new();
    let kind = seed % 4;
    while out.len() < n {
        let c: Vec<f32> = unit((0..dim).map(|_| r.f()).collect());
        let size = match kind {
            0 => 1,
            _ => 1 + r.below(700) as usize,
        };
        let sigma = match r.below(5) {
            0 => 0.0,
            1 => 1e-7,
            2 => 1e-5,
            3 => 1e-3,
            _ => 1e-1,
        };
        for _ in 0..size {
            if out.len() >= n {
                break;
            }
            let v: Vec<f32> = c.iter().map(|x| x + sigma * r.f()).collect();
            out.push(if kind == 3 { v } else { unit(v) });
        }
    }
    for i in (1..out.len()).rev() {
        let j = r.below(i as u64 + 1) as usize;
        out.swap(i, j);
    }
    out.into_iter()
        .enumerate()
        .map(|(i, v)| (i as u32, v))
        .collect()
}

/// Runs `work` on its own thread and fails if it has not returned in
/// `secs` seconds (a hang cannot be asserted on from inside it).
fn within<T: Send + 'static>(
    secs: u64,
    what: &str,
    work: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(work());
    });
    rx.recv_timeout(Duration::from_secs(secs))
        .unwrap_or_else(|_| panic!("{what} did not finish in {secs}s"))
}

#[test]
fn build_over_near_duplicate_clumps_terminates_and_keeps_every_vector() {
    // Seeds 31, 81 and 86 never finished before the fix; the rest cover the
    // other corpus kinds.
    let (dim, n) = (24, 6000);
    for seed in [31u64, 81, 86, 0, 1, 2, 3] {
        let vectors = clumped_corpus(seed, dim, n);
        let index = within(120, &format!("build of seed {seed}"), move || {
            ClusterIndex::build(&vectors, ClusterIndexConfig::new(dim)).unwrap()
        });
        let snapshot = index.snapshot();
        let mut ids: Vec<u32> = snapshot
            .iter()
            .flat_map(|p| p.entries.iter().map(|(id, _)| *id))
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..n as u32).collect::<Vec<_>>(), "seed {seed}");
        // The split loop used to add one empty posting per pass.
        assert!(
            snapshot.iter().all(|p| !p.entries.is_empty()),
            "seed {seed}: an empty posting survived the build"
        );
        assert!(
            snapshot.len() <= n,
            "seed {seed}: {} postings for {n} vectors",
            snapshot.len()
        );
    }
}

#[test]
fn inserting_a_near_duplicate_clump_terminates() {
    // The runtime path splits through Reassign's cascade, not through
    // `rebalance`. It was not affected by #452; this pins that a clump of
    // near-identical inserts neither hangs nor trips `MAX_CASCADE_DEPTH`.
    let dim = 24;
    let index = within(120, "inserts", move || {
        let mut r = Rng(0xC0FF_EE11);
        let seedlings: Vec<(u32, Vec<f32>)> = (0..400u32)
            .map(|i| (i, unit((0..dim).map(|_| r.f()).collect())))
            .collect();
        let mut index = ClusterIndex::build(&seedlings, ClusterIndexConfig::new(dim)).unwrap();
        let centre: Vec<f32> = unit((0..dim).map(|_| r.f()).collect());
        for (i, sigma) in [0.0f32, 1e-7, 1e-5].into_iter().enumerate() {
            for j in 0..600u32 {
                let v: Vec<f32> = centre.iter().map(|x| x + sigma * r.f()).collect();
                index.insert(1000 + i as u32 * 1000 + j, unit(v)).unwrap();
            }
        }
        index
    });
    let live: usize = index.snapshot().iter().map(|p| p.entries.len()).sum();
    assert_eq!(live, 400 + 3 * 600);
}

/// Wider sweep of the same generator, for hunting further cases:
/// `SEEDS=400 cargo test --release -p kosha-vector-spfresh --test
/// near_duplicate_build -- --ignored`.
#[test]
#[ignore = "fuzz sweep; minutes in debug"]
fn build_terminates_across_a_seed_sweep() {
    let seeds: u64 = std::env::var("SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let (dim, n) = (24, 6000);
    for seed in 0..seeds {
        let vectors = clumped_corpus(seed, dim, n);
        let index = within(60, &format!("build of seed {seed}"), move || {
            ClusterIndex::build(&vectors, ClusterIndexConfig::new(dim)).unwrap()
        });
        let live: usize = index.snapshot().iter().map(|p| p.entries.len()).sum();
        assert_eq!(live, n, "seed {seed}");
    }
}
