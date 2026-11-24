//! FreshDiskANN §4.3-style stability gate (#466), using the in-memory compaction model.

#[path = "common/compaction.rs"]
mod compaction;

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::PathBuf;

use compaction::{input_of, lire_merge, Input};
use kosha_vector_spfresh::{
    cosine_distance, dot, normalize_in_place, ClusterIndex, ClusterIndexConfig, DeterministicRng,
    PostingSnapshot,
};

const K: usize = 10;
const TRUTH_PREFIX: usize = 128;
const MSMARCO_DIM: usize = 1024;

fn setting(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("invalid {name}: {value}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("invalid {name}: {error}"),
    }
}

fn read_embeddings(paths: Vec<PathBuf>, limit: usize) -> Vec<Vec<f32>> {
    let mut vectors = Vec::with_capacity(limit);
    let mut record = vec![0; MSMARCO_DIM * size_of::<f32>()];
    for path in paths {
        let file = File::open(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        let bytes = file.metadata().unwrap().len();
        assert_eq!(
            bytes % record.len() as u64,
            0,
            "partial record in {}",
            path.display()
        );
        let mut reader = BufReader::new(file);
        for _ in 0..(bytes / record.len() as u64).min((limit - vectors.len()) as u64) {
            reader.read_exact(&mut record).unwrap();
            vectors.push(
                record
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|&bytes| f32::from_le_bytes(bytes))
                    .collect(),
            );
        }
        if vectors.len() == limit {
            break;
        }
    }
    assert_eq!(vectors.len(), limit, "not enough embedding records");
    vectors
}

fn dataset(count: usize, query_count: usize) -> (Vec<Vec<f32>>, Vec<Vec<f32>>) {
    if let Some(dir) = std::env::var_os("KOSHA_BENCH_CHURN_EMBEDDINGS").map(PathBuf::from) {
        let mut shards: Vec<_> = fs::read_dir(&dir)
            .unwrap_or_else(|error| panic!("{}: {error}", dir.display()))
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                let name = path.file_name().unwrap().to_string_lossy();
                name.starts_with("emb-") && name.ends_with(".f32")
            })
            .collect();
        shards.sort_unstable();
        return (
            read_embeddings(shards, count),
            read_embeddings(vec![dir.join("queries_emb.f32")], query_count),
        );
    }

    let mut rng = DeterministicRng::new(7);
    let centers: Vec<Vec<f32>> = (0..40)
        .map(|_| (0..32).map(|_| rng.next_f32_range(-1.0, 1.0)).collect())
        .collect();
    let mut sample = |n| {
        (0..n)
            .map(|_| {
                centers[rng.next_usize(centers.len())]
                    .iter()
                    .map(|x| x + rng.next_f32_range(-0.6, 0.6))
                    .collect()
            })
            .collect()
    };
    (sample(count), sample(query_count))
}

fn take_random(pool: &mut Vec<u32>, count: usize, rng: &mut DeterministicRng) -> Vec<u32> {
    (0..count)
        .map(|_| pool.swap_remove(rng.next_usize(pool.len())))
        .collect()
}

fn entries(data: &[Vec<f32>], ids: &[u32]) -> Vec<(u32, Vec<f32>)> {
    let mut ordered = ids.to_vec();
    ordered.sort_unstable();
    ordered
        .into_iter()
        .map(|id| (id, data[id as usize].clone()))
        .collect()
}

fn exact_top(
    data: &[Vec<f32>],
    query: &[f32],
    ids: impl Iterator<Item = u32>,
    limit: usize,
) -> Vec<u32> {
    let mut scores: Vec<_> = ids.map(|id| (id, dot(query, &data[id as usize]))).collect();
    let better = |a: &(u32, f32), b: &(u32, f32)| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0));
    if scores.len() > limit {
        scores.select_nth_unstable_by(limit, better);
        scores.truncate(limit);
    }
    scores.sort_unstable_by(better);
    scores.into_iter().map(|(id, _)| id).collect()
}

fn truth(
    data: &[Vec<f32>],
    queries: &[Vec<f32>],
    prefixes: &[Vec<u32>],
    live: &[u32],
) -> Vec<Vec<u32>> {
    let mut active = vec![false; data.len()];
    for &id in live {
        active[id as usize] = true;
    }
    queries
        .iter()
        .zip(prefixes)
        .map(|(query, prefix)| {
            // If the exact corpus-wide prefix has ten live neighbours, the prefix
            // suffices. Otherwise recompute against the full live set.
            let cached: Vec<_> = prefix
                .iter()
                .copied()
                .filter(|&id| active[id as usize])
                .take(K)
                .collect();
            if cached.len() == K {
                cached
            } else {
                exact_top(data, query, live.iter().copied(), K)
            }
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Default)]
struct ProbeTotals {
    hits: usize,
    scanned: usize,
}

impl ProbeTotals {
    fn within_bar(self, rebuilt: Self, query_count: usize) -> bool {
        self.hits.abs_diff(rebuilt.hits) * 200 <= query_count * K
            && self.scanned.abs_diff(rebuilt.scanned) * 10 <= rebuilt.scanned
    }
}

fn probe_curve(
    postings: &[PostingSnapshot],
    queries: &[Vec<f32>],
    truth: &[Vec<u32>],
    live: &[u32],
    capacity: usize,
) -> Vec<ProbeTotals> {
    let mut owners = vec![usize::MAX; capacity];
    let mut sizes = Vec::with_capacity(postings.len());
    for (posting, p) in postings.iter().enumerate() {
        sizes.push(p.entries.len());
        for (id, _) in &p.entries {
            assert_eq!(owners[*id as usize], usize::MAX, "duplicate id {id}");
            owners[*id as usize] = posting;
        }
    }
    assert_eq!(sizes.iter().sum::<usize>(), live.len(), "wrong live count");
    assert!(
        live.iter().all(|&id| owners[id as usize] != usize::MAX),
        "missing live id"
    );

    let mut curve = vec![ProbeTotals::default(); postings.len()];
    let mut ranks = vec![0; postings.len()];
    let mut order = Vec::with_capacity(postings.len());
    for (query, neighbours) in queries.iter().zip(truth) {
        order.clear();
        order.extend(
            postings
                .iter()
                .enumerate()
                .map(|(i, p)| ((cosine_distance(query, &p.centroid) - p.radius).max(0.0), i)),
        );
        order.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        for (rank, &(_, i)) in order.iter().enumerate() {
            ranks[i] = rank;
            curve[rank].scanned += sizes[i];
        }
        // Exact reranking of probed vectors recovers an exact top-k ID iff
        // its posting is probed (using the same score/ID tie break).
        for &id in neighbours {
            curve[ranks[owners[id as usize]]].hits += 1;
        }
    }
    let mut prefix = ProbeTotals::default();
    for point in &mut curve {
        prefix.hits += point.hits;
        prefix.scanned += point.scanned;
        *point = prefix;
    }
    curve
}

fn tune(curve: &[ProbeTotals], query_count: usize) -> usize {
    1 + curve
        .iter()
        .position(|p| p.hits * 100 >= 95 * K * query_count)
        .expect("full probing must recover all exact neighbours")
}

fn measure(curve: &[ProbeTotals], nprobe: usize) -> ProbeTotals {
    curve[nprobe.min(curve.len()) - 1]
}

fn report(
    label: (&str, usize, usize, &str),
    nprobe: usize,
    query_count: usize,
    postings: &[PostingSnapshot],
    metrics: ProbeTotals,
) {
    let (scenario, rate, cycle, method) = label;
    let mut radii: Vec<_> = postings.iter().map(|p| p.radius).collect();
    radii.sort_unstable_by(f32::total_cmp);
    let median = (radii[(radii.len() - 1) / 2] + radii[radii.len() / 2]) / 2.0;
    let p90 = radii[(9 * radii.len()).div_ceil(10) - 1];
    let recall = metrics.hits as f64 / (query_count * K) as f64;
    let scanned = metrics.scanned as f64 / query_count as f64;
    let count = postings.len();
    println!("{scenario},{rate},{cycle},{method},{nprobe},{recall:.6},{scanned:.3},{median:.6},{p90:.6},{count}");
}

#[test]
#[ignore = "offline stability experiment; see the crate README"]
fn churn_cycle_recall_stability() {
    let size = setting("KOSHA_BENCH_CHURN_SIZE", 8_000);
    let cycles = setting("KOSHA_BENCH_CHURN_CYCLES", 50);
    let query_count = setting("KOSHA_BENCH_CHURN_QUERIES", 100);
    assert!((20..=u32::MAX as usize / 2).contains(&size) && cycles > 0 && query_count > 0);
    let capacity = size * 2;
    let (mut data, mut queries) = dataset(capacity, query_count);
    for v in data.iter_mut().chain(&mut queries) {
        assert!(v.iter().all(|x| x.is_finite()), "non-finite embedding");
        normalize_in_place(v);
    }
    // IDs are ranks along a fixed projection; a moving window of the
    // inactive IDs thus inserts from a shifting region of embedding space.
    data.sort_unstable_by(|a, b| a[0].total_cmp(&b[0]));
    let all_ids: Vec<u32> = (0..capacity as u32).collect();
    let prefixes: Vec<_> = queries
        .iter()
        .map(|q| exact_top(&data, q, all_ids.iter().copied(), TRUTH_PREFIX))
        .collect();
    let mut rng = DeterministicRng::new(42);
    let mut reserve = all_ids;
    let live = take_random(&mut reserve, size, &mut rng);
    let cfg = ClusterIndexConfig::new(data[0].len());
    let initial = ClusterIndex::build(&entries(&data, &live), cfg.clone())
        .unwrap()
        .snapshot();
    let exact = truth(&data, &queries, &prefixes, &live);
    let curve = probe_curve(&initial, &queries, &exact, &live, capacity);
    let nprobe = tune(&curve, query_count);
    println!("\nscenario,churn_pct,cycle,method,nprobe,recall_at_10,mean_vectors_scanned,median_radius,p90_radius,postings");
    report(
        ("initial", 0, 0, "build"),
        nprobe,
        query_count,
        &initial,
        measure(&curve, nprobe),
    );

    let mut checked = 0;
    let mut failed = 0;
    for drifting in [false, true] {
        let scenario = if drifting { "drifting" } else { "stationary" };
        for rate in [5, 10, 50] {
            let mut rng = rng.clone();
            let mut lire = initial.clone();
            let (mut live, mut reserve) = (live.clone(), reserve.clone());
            let churn = size * rate / 100;
            for cycle in 1..=cycles {
                let deleted = take_random(&mut live, churn, &mut rng);
                let inserted = if drifting {
                    reserve.sort_unstable();
                    let start = (reserve.len() - churn) * (cycle - 1) / (cycles - 1).max(1);
                    reserve.drain(start..start + churn).collect()
                } else {
                    take_random(&mut reserve, churn, &mut rng)
                };
                live.extend(&inserted);
                reserve.extend(&deleted);
                // Force seed adoption even on tiny smoke runs; this is a
                // LIRE-versus-rebuild experiment, not a fallback test.
                lire = lire_merge(
                    vec![
                        Input::Postings(lire),
                        input_of(entries(&data, &inserted), &cfg),
                    ],
                    &deleted.into_iter().collect::<HashSet<_>>(),
                    &cfg,
                )
                .snapshot();
                let rebuilt = ClusterIndex::build(&entries(&data, &live), cfg.clone())
                    .unwrap()
                    .snapshot();
                let exact = truth(&data, &queries, &prefixes, &live);
                let measured = [&lire, &rebuilt].map(|postings| {
                    measure(
                        &probe_curve(postings, &queries, &exact, &live, capacity),
                        nprobe,
                    )
                });
                for ((method, postings), metrics) in [("lire", &lire), ("rebuild", &rebuilt)]
                    .into_iter()
                    .zip(measured)
                {
                    report(
                        (scenario, rate, cycle, method),
                        nprobe,
                        query_count,
                        postings,
                        metrics,
                    );
                }
                checked += 1;
                if !measured[0].within_bar(measured[1], query_count) {
                    failed += 1;
                    eprintln!("outside stability bar: {scenario} {rate}% cycle {cycle}");
                }
            }
        }
    }
    assert_eq!(
        failed, 0,
        "{failed}/{checked} cycles outside the stability bar; see paired CSV rows above"
    );
}

#[test]
fn probe_budget_and_gate_use_the_stated_thresholds() {
    let before = ProbeTotals {
        hits: 1899,
        scanned: 1000,
    };
    let at = ProbeTotals {
        hits: 1900,
        scanned: 1100,
    };
    assert_eq!(tune(&[before, at], 200), 2);
    assert!(ProbeTotals {
        hits: 1889,
        scanned: 900
    }
    .within_bar(before, 200));
    assert!(!ProbeTotals {
        hits: 1888,
        scanned: 1000
    }
    .within_bar(before, 200));
    assert!(!ProbeTotals {
        hits: 1899,
        scanned: 1101
    }
    .within_bar(before, 200));
}
