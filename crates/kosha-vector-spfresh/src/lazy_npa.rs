#[path = "../tests/common/churn.rs"]
mod churn;
#[path = "../tests/common/mod.rs"]
mod common;

use std::collections::{HashMap, HashSet};

use crate::posting::PostingId;
use crate::{
    cosine_distance, mean_vector, ClusterIndex, ClusterIndexConfig, DeterministicRng,
    PostingSnapshot,
};
use churn::{CycleOps, LiveSnapshot, Op};

#[derive(Debug)]
pub(super) struct DeferredReassign {
    percent: usize,
    rng: DeterministicRng,
    eligible: usize,
    skipped: usize,
    intents: HashMap<u32, Intent>,
}

#[derive(Debug)]
struct Intent {
    source: PostingId,
    source_centroid: Vec<f32>,
    dest: PostingId,
    dest_centroid: Vec<f32>,
}

impl DeferredReassign {
    pub(super) fn should_skip(&mut self, id: u32) -> bool {
        self.eligible += 1;
        let skip = self.rng.next_usize(100) < self.percent;
        if skip {
            self.skipped += 1;
        } else {
            self.intents.remove(&id);
        }
        skip
    }

    pub(super) fn record(
        &mut self,
        id: u32,
        source: PostingId,
        source_centroid: &[f32],
        dest: PostingId,
        dest_centroid: &[f32],
    ) {
        self.intents.insert(
            id,
            Intent {
                source,
                source_centroid: source_centroid.to_vec(),
                dest,
                dest_centroid: dest_centroid.to_vec(),
            },
        );
    }

    fn pending(&self, index: &ClusterIndex) -> (Moves, usize) {
        let mut pending = Moves::new();
        let mut touched = HashSet::new();
        for (&id, intent) in &self.intents {
            let unchanged = |slot: PostingId, centroid: &[f32]| {
                index.postings[slot]
                    .as_ref()
                    .is_some_and(|posting| posting.centroid == centroid)
            };
            // Splits and merges may recenter or recycle a posting slot.
            if index.owner_map.get(&id) == Some(&intent.source)
                && unchanged(intent.source, &intent.source_centroid)
                && unchanged(intent.dest, &intent.dest_centroid)
            {
                pending.insert(id, intent.dest);
                touched.extend([intent.source, intent.dest]);
            }
        }
        (pending, touched.len())
    }
}

type Moves = HashMap<u32, PostingId>;

fn count_violations(index: &ClusterIndex) -> usize {
    let postings: Vec<_> = index
        .active_posting_ids()
        .map(|id| index.postings[id].as_ref().unwrap())
        .collect();
    let mut violations = 0;
    for posting in &postings {
        for entry in posting.live_entries() {
            let distance = cosine_distance(&posting.centroid, &entry.vector);
            if postings
                .iter()
                .any(|p| cosine_distance(&p.centroid, &entry.vector) < distance)
            {
                violations += 1;
            }
        }
    }
    violations
}

fn next_compaction(
    previous: &ClusterIndex,
    pending: &Moves,
    ops: &CycleOps,
    live: &LiveSnapshot,
    config: &ClusterIndexConfig,
) -> ClusterIndex {
    let mut writes = HashMap::<PostingId, Vec<(u32, Vec<f32>)>>::new();
    let mut deletions = HashSet::new();
    for source in previous.active_posting_ids() {
        for entry in previous.postings[source].as_ref().unwrap().live_entries() {
            if live.contains_key(&entry.id) {
                let dest = pending.get(&entry.id).copied().unwrap_or(source);
                writes
                    .entry(dest)
                    .or_default()
                    .push((entry.id, entry.vector.clone()));
            } else {
                deletions.insert(source);
            }
        }
    }
    let seeds = previous
        .active_posting_ids()
        .filter_map(|id| {
            let entries = writes.remove(&id)?;
            let centroid = if deletions.contains(&id) {
                mean_vector(
                    entries.iter().map(|(_, vector)| vector.as_slice()),
                    config.dim,
                )
            } else {
                previous.postings[id].as_ref().unwrap().centroid.clone()
            };
            Some(PostingSnapshot {
                centroid,
                entries,
                radius: 0.0,
            })
        })
        .collect();
    assert!(writes.is_empty());
    let loose = ops
        .iter()
        .filter_map(|op| match op {
            Op::Insert(id, vector) if live.contains_key(id) => Some(PostingSnapshot {
                centroid: vector.clone(),
                entries: vec![(*id, vector.clone())],
                radius: 0.0,
            }),
            Op::Insert(..) => None,
            Op::Delete(id) => {
                assert!(!live.contains_key(id));
                None
            }
        })
        .collect();
    let mut index = ClusterIndex::from_postings(seeds, config.clone()).unwrap();
    index.absorb_postings(loose, 8).unwrap();
    index
}

#[test]
fn deferred_reassign_recall_curve() {
    const K: usize = 10;
    const CYCLES: usize = 20;
    let dim = 16;
    let seed = churn::generate_seed(dim, 1200, 8, 111);
    let (cycles, snapshots) = churn::generate_op_sequence(
        dim,
        1200,
        seed.iter().cloned().collect(),
        CYCLES,
        250,
        12345,
    );
    let mut rng = DeterministicRng::new(2024);
    let queries: Vec<_> = (0..120)
        .map(|_| {
            let angle = rng.next_f32_range(0.0, std::f32::consts::TAU);
            let mut v = vec![0.0; dim];
            v[0] = angle.cos() * 10.0;
            v[1] = angle.sin() * 10.0;
            v
        })
        .collect();
    let exact: Vec<_> = snapshots
        .iter()
        .map(|live| {
            let vectors = common::sorted_vectors(live);
            queries
                .iter()
                .map(|query| common::brute_force_topk(&vectors, query, K))
                .collect::<Vec<_>>()
        })
        .collect();
    let mut config = ClusterIndexConfig::new(dim);
    config.target_posting_size = 16;
    config.max_posting_size = 32;
    config.min_posting_size = 4;
    config.nprobe = 5;
    let mut eager_recall = 0.0;
    eprintln!(
        "skip_pct,cycle,eligible,skipped,live,npa_violations,pending,postings,touched,recall_at_10"
    );
    for percent in [0, 1, 5, 10, 25, 50, 75, 100] {
        let mut index = ClusterIndex::build(&seed, config.clone()).unwrap();
        let mut pending = Moves::new();
        let mut recall_sum = 0.0;
        let mut totals = [0; 6];
        let mut max_touched = 0;
        let mut rng = DeterministicRng::new(462);
        for (cycle, ((ops, live), truth)) in cycles.iter().zip(&snapshots).zip(&exact).enumerate() {
            index = next_compaction(&index, &pending, ops, live, &config);
            index.deferred_reassign = Some(DeferredReassign {
                percent,
                rng,
                eligible: 0,
                skipped: 0,
                intents: HashMap::new(),
            });
            index.rebalance();
            let deferred = index.deferred_reassign.take().unwrap();
            let (moves, touched) = deferred.pending(&index);
            let violations = count_violations(&index);
            rng = deferred.rng;
            pending = moves;
            let recall = queries
                .iter()
                .zip(truth)
                .map(|(q, exact)| common::recall_at_k(&index.search(q, K).unwrap(), exact))
                .sum::<f64>()
                / queries.len() as f64;
            let mut live_ids: Vec<_> = live.keys().copied().collect();
            live_ids.sort_unstable();
            assert_eq!(index.ids(), live_ids);
            assert_eq!(
                index
                    .snapshot()
                    .iter()
                    .map(|p| p.entries.len())
                    .sum::<usize>(),
                live.len()
            );
            assert!(pending.len() <= deferred.skipped && pending.len() <= violations);
            let postings = index.active_posting_count();
            assert!(touched <= postings);
            recall_sum += recall;
            for (total, count) in totals.iter_mut().zip([
                deferred.eligible,
                deferred.skipped,
                live.len(),
                violations,
                pending.len(),
                touched,
            ]) {
                *total += count;
            }
            max_touched = max_touched.max(touched);
            eprintln!(
                "{percent},{cycle},{},{},{},{violations},{},{postings},{touched},{recall:.6}",
                deferred.eligible,
                deferred.skipped,
                live.len(),
                pending.len()
            );
        }
        let [eligible, skipped, live, violations, pending, touched] = totals;
        if percent == 0 {
            assert_eq!((skipped, pending, touched), (0, 0, 0));
            eager_recall = recall_sum / CYCLES as f64;
        } else if percent == 100 {
            assert_eq!(eligible, skipped);
            assert!(pending > 0 && touched > 0);
        }
        let recall = recall_sum / CYCLES as f64;
        eprintln!(
            "summary skip={percent:3}% events={skipped}/{eligible} npa={:.3}% pending={:.3}% \
             recall={recall:.6} drop={:+.3}pp touched_mean={:.2} touched_max={max_touched}",
            100.0 * violations as f64 / live as f64,
            100.0 * pending as f64 / live as f64,
            100.0 * (eager_recall - recall),
            touched as f64 / CYCLES as f64,
        );
    }
}
