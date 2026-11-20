//! Randomized interleaving tests for the write path (#257): flush
//! coalescing (leader / piggyback), compaction vs delete, compaction vs
//! upsert, and all three at once.
//!
//! # Approach
//!
//! `shuttle` and `loom` only explore code written against their own
//! `Mutex`/`Condvar`/thread shims. `Indexer` uses `std::sync` directly and
//! does real file I/O in its critical paths, so swapping its primitives
//! would mean making the production type generic over a sync backend for
//! tests alone. Instead this uses a deterministic scheduler seam over the
//! failpoints the `failpoints` feature compiles in (`flush.leader.pass`,
//! `flush.piggyback.registered`, `flush.snapshot_taken`,
//! `flush.segment_written`, `compaction.planned`,
//! `compaction.output_written`, `upsert.before_io`,
//! `upsert.segment_written`, `delete.persist.before`, and the WAL ones):
//! the generalization of the hand-placed `after_plan` race seam in
//! `compact_namespace_with_options_impl`.
//!
//! Each worker thread runs one operation. At every seam point it parks and
//! a seeded scheduler picks which parked worker runs next, so only one
//! worker runs between points and the order is the schedule. A worker that
//! blocks on a lock held by a parked one (not at a point) is detected by a
//! short quiescence timeout, and the scheduler then lets another parked
//! worker run. Schedules are reproducible from their seed except where
//! such a timeout fires; every failure prints its seed and decision trace.
//!
//! 1,000 schedules run by default (250 per scenario);
//! `KOSHA_INTERLEAVING_SCHEDULES` sets the per-scenario count.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use kosha_core::{Document, DocumentId, Field, FilterClause, NamespaceId};
use kosha_segment::SegmentReader;
use kosha_write::{CompactOptions, Indexer};

/// Every scheduling seam the write path exposes.
const SEAMS: &[&str] = &[
    "flush.leader.pass",
    "flush.piggyback.registered",
    "flush.snapshot_taken",
    "flush.segment_written",
    "compaction.planned",
    "compaction.output_written",
    "upsert.before_io",
    "upsert.segment_written",
    "delete.persist.before",
    "wal.append.before_fsync",
    "wal.append.after_fsync",
];

// ─── Scheduler ────────────────────────────────────────────────────────────

struct Sched {
    rng: u64,
    /// The worker allowed to run, if any.
    token: Option<usize>,
    /// Workers waiting at a seam (or at their start).
    parked: BTreeSet<usize>,
    done: usize,
    workers: usize,
    /// Bumped on every scheduling event; the watchdog's progress signal.
    events: u64,
    /// `(worker, seam)` in the order workers reached their seams.
    trace: Vec<(usize, &'static str)>,
}

impl Sched {
    fn next_random(&mut self) -> u64 {
        // xorshift64*
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        self.rng.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn hand_off(&mut self) {
        if self.parked.is_empty() {
            self.token = None;
            return;
        }
        let pick = (self.next_random() % self.parked.len() as u64) as usize;
        self.token = self.parked.iter().nth(pick).copied();
    }
}

struct Scheduler {
    state: Mutex<Sched>,
    wake: Condvar,
}

thread_local! {
    static WORKER: std::cell::RefCell<Option<(Arc<Scheduler>, usize)>> =
        const { std::cell::RefCell::new(None) };
}

impl Scheduler {
    fn new(seed: u64, workers: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Sched {
                rng: seed.max(1),
                token: None,
                parked: BTreeSet::new(),
                done: 0,
                workers,
                events: 0,
                trace: Vec::new(),
            }),
            wake: Condvar::new(),
        })
    }

    /// Park `me` at `seam` until the scheduler hands it the token.
    fn park(&self, me: usize, seam: &'static str) {
        let mut st = self.state.lock().unwrap();
        st.trace.push((me, seam));
        st.parked.insert(me);
        st.events += 1;
        if st.token.is_none() || st.token == Some(me) {
            st.hand_off();
        }
        self.wake.notify_all();
        while st.token != Some(me) {
            st = self.wake.wait(st).unwrap();
        }
        st.parked.remove(&me);
        st.events += 1;
    }

    fn finish(&self, me: usize) {
        let mut st = self.state.lock().unwrap();
        st.done += 1;
        st.events += 1;
        if st.token == Some(me) {
            st.hand_off();
        }
        self.wake.notify_all();
    }

    /// Run until every worker finished. When nothing happens for a
    /// quiescence period, the running worker is blocked outside a seam
    /// (on a lock a parked worker holds), so another parked worker runs.
    fn drive(&self, seed: u64) {
        const QUIET: Duration = Duration::from_millis(3);
        let started = Instant::now();
        let mut st = self.state.lock().unwrap();
        let mut seen = st.events;
        while st.done < st.workers {
            let (guard, timeout) = self.wake.wait_timeout(st, QUIET).unwrap();
            st = guard;
            if !timeout.timed_out() || st.events != seen {
                seen = st.events;
                continue;
            }
            let holder_parked = st.token.is_some_and(|t| st.parked.contains(&t));
            if !holder_parked && !st.parked.is_empty() {
                st.hand_off();
                st.events += 1;
                seen = st.events;
                self.wake.notify_all();
            }
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "schedule {seed:#x} made no progress: {:?}",
                st.trace
            );
        }
    }
}

fn install_seams() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        for &seam in SEAMS {
            fail::cfg_callback(seam, move || {
                let worker = WORKER.with(|w| w.borrow().clone());
                if let Some((scheduler, me)) = worker {
                    scheduler.park(me, seam);
                }
            })
            .unwrap();
        }
    });
}

type Op = Box<dyn FnOnce() + Send>;

/// Run `ops` on worker threads under the schedule `seed` picks. Returns
/// the decision trace; panics (with the seed and trace) if a worker did.
fn run_schedule(seed: u64, ops: Vec<Op>) -> Vec<(usize, &'static str)> {
    install_seams();
    let scheduler = Scheduler::new(seed, ops.len());
    let handles: Vec<_> = ops
        .into_iter()
        .enumerate()
        .map(|(me, op)| {
            let scheduler = Arc::clone(&scheduler);
            std::thread::spawn(move || {
                WORKER.with(|w| *w.borrow_mut() = Some((Arc::clone(&scheduler), me)));
                scheduler.park(me, "start");
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(op));
                WORKER.with(|w| *w.borrow_mut() = None);
                scheduler.finish(me);
                result
            })
        })
        .collect();
    scheduler.drive(seed);
    let trace = scheduler.state.lock().unwrap().trace.clone();
    for handle in handles {
        if let Err(panic) = handle.join().unwrap() {
            let message = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            panic!("schedule {seed:#x} failed: {message}\ntrace: {trace:?}");
        }
    }
    trace
}

fn schedules() -> u64 {
    std::env::var("KOSHA_INTERLEAVING_SCHEDULES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(250)
}

// ─── Fixtures ─────────────────────────────────────────────────────────────

fn temp_dir(tag: &str, seed: u64) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "kosha-interleave-{tag}-{}-{seed:x}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn doc(id: &str, grp: &str, v: u32) -> Document {
    Document {
        id: DocumentId(id.into()),
        fields: vec![
            Field::text("body", format!("document {id}")),
            Field::keyword("grp", grp),
            Field::keyword("v", v.to_string()),
        ],
    }
}

/// Live documents in committed segments: id → every live copy's `v`.
fn live(idx: &Indexer, dir: &Path, ns: &NamespaceId) -> BTreeMap<String, Vec<u32>> {
    let mut out: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    let Some(manifest) = idx.manifest_cloned(ns) else {
        return out;
    };
    let tombstones = idx.get_tombstones(ns).unwrap_or_default();
    for entry in &manifest.segments {
        let reader = SegmentReader::open(manifest.segment_dir(dir, ns, &entry.segment_id)).unwrap();
        let dead = tombstones.get(&entry.segment_id);
        for seq in 0..reader.doc_count() {
            if dead.is_some_and(|d| d.contains(&seq)) {
                continue;
            }
            let record = reader.doc_record_full(seq).unwrap().unwrap();
            let v = record
                .fields
                .iter()
                .find(|f| f.name == "v")
                .map(|f| f.value.parse().unwrap())
                .unwrap();
            out.entry(record.doc_id.0).or_default().push(v);
        }
    }
    out
}

/// An indexer holding three flushed segments: `a0..a2` (group `a`) and
/// `b0..b2` (group `b`), interleaved across segments.
fn three_segments(dir: &Path, ns: &NamespaceId) -> Arc<Indexer> {
    // No WAL: these races are about segments and tombstones, and fsyncs
    // would dominate the run time.
    let idx = Arc::new(Indexer::new(dir.to_path_buf()).with_wal(false));
    for i in 0..3 {
        idx.index_documents(
            ns.clone(),
            vec![doc(&format!("a{i}"), "a", 1), doc(&format!("b{i}"), "b", 1)],
        )
        .unwrap();
        idx.flush_namespace(ns).unwrap();
    }
    assert_eq!(idx.manifest_cloned(ns).unwrap().segments.len(), 3);
    idx
}

fn group_b() -> FilterClause {
    serde_json::from_str(r#"{"term": {"grp": "b"}}"#).unwrap()
}

/// Runs `schedules()` seeds of one scenario and checks the scheduler
/// really explored: many distinct orderings, every expected seam reached.
fn explore(
    name: &str,
    expected_seams: &[&str],
    mut scenario: impl FnMut(u64) -> Vec<(usize, &'static str)>,
) {
    let mut orderings = HashSet::new();
    let mut reached = BTreeSet::new();
    let n = schedules();
    for i in 0..n {
        let seed = 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(i + 1) ^ name.len() as u64;
        let trace = scenario(seed);
        reached.extend(trace.iter().map(|(_, seam)| *seam));
        orderings.insert(trace);
    }
    for seam in expected_seams {
        assert!(reached.contains(seam), "{name}: seam {seam} never reached");
    }
    // Operations that serialize on a lock (an upsert waits out a whole
    // compaction) leave few orderings; a stuck scheduler leaves one.
    let distinct = orderings.len() as u64;
    assert!(
        distinct >= n.min(4),
        "{name}: only {distinct} distinct orderings in {n} schedules"
    );
    eprintln!("{name}: {n} schedules, {distinct} distinct orderings");
}

// ─── Scenarios ────────────────────────────────────────────────────────────

/// Three writers each index a document and flush. Whichever leads, and
/// however the others piggyback, a flush that returns `Ok` has made its
/// caller's document durable in a committed segment, and every document
/// ends up live exactly once.
#[test]
fn flush_coalescing_leader_and_piggybacks() {
    explore(
        "flush coalescing",
        &[
            "flush.leader.pass",
            "flush.piggyback.registered",
            "flush.segment_written",
        ],
        |seed| {
            let dir = temp_dir("flush", seed);
            let ns = NamespaceId("t/flush".into());
            let idx = Arc::new(Indexer::new(dir.clone()).with_wal(false));
            let ops: Vec<Op> = (0..3)
                .map(|w| {
                    let (idx, ns, dir) = (Arc::clone(&idx), ns.clone(), dir.clone());
                    Box::new(move || {
                        let id = format!("w{w}");
                        idx.index_documents(ns.clone(), vec![doc(&id, "a", 1)])
                            .unwrap();
                        idx.flush_namespace(&ns).unwrap();
                        let live = live(&idx, &dir, &ns);
                        assert_eq!(
                            live.get(&id),
                            Some(&vec![1]),
                            "{id} not flushed by its own flush"
                        );
                    }) as Op
                })
                .collect();
            let trace = run_schedule(seed, ops);
            let live = live(&idx, &dir, &ns);
            assert_eq!(idx.pending_doc_count(&ns), 0, "seed {seed:#x}");
            assert_eq!(
                live.keys().map(String::as_str).collect::<Vec<_>>(),
                ["w0", "w1", "w2"],
                "seed {seed:#x}"
            );
            assert!(
                live.values().all(|v| v.len() == 1),
                "seed {seed:#x}: {live:?}"
            );
            drop(idx);
            std::fs::remove_dir_all(&dir).ok();
            trace
        },
    );
}

/// A full compaction races a delete of group `b` and an unrelated write.
/// Every acknowledged delete stays deleted (the merge must carry a
/// tombstone that landed mid-merge), nothing else is lost or duplicated.
#[test]
fn compaction_vs_delete() {
    explore(
        "compaction vs delete",
        &[
            "compaction.planned",
            "compaction.output_written",
            "delete.persist.before",
        ],
        |seed| {
            let dir = temp_dir("cvd", seed);
            let ns = NamespaceId("t/cvd".into());
            let idx = three_segments(&dir, &ns);
            let ops: Vec<Op> = vec![
                {
                    let (idx, ns) = (Arc::clone(&idx), ns.clone());
                    Box::new(move || {
                        idx.compact_namespace_with_options(&ns, CompactOptions::full())
                            .unwrap();
                    })
                },
                {
                    let (idx, ns) = (Arc::clone(&idx), ns.clone());
                    Box::new(move || {
                        let manifest = idx.manifest_cloned(&ns).unwrap();
                        assert_eq!(idx.delete_by_query(&ns, &manifest, &group_b()).unwrap(), 3);
                    })
                },
                {
                    let (idx, ns) = (Arc::clone(&idx), ns.clone());
                    Box::new(move || {
                        idx.index_documents(ns.clone(), vec![doc("c0", "c", 1)])
                            .unwrap();
                        idx.flush_namespace(&ns).unwrap();
                    })
                },
            ];
            let trace = run_schedule(seed, ops);
            idx.flush_namespace(&ns).unwrap();
            let live = live(&idx, &dir, &ns);
            let expected: BTreeMap<String, Vec<u32>> = ["a0", "a1", "a2", "c0"]
                .iter()
                .map(|id| (id.to_string(), vec![1]))
                .collect();
            assert_eq!(live, expected, "seed {seed:#x}: {trace:?}");
            drop(idx);
            std::fs::remove_dir_all(&dir).ok();
            trace
        },
    );
}

/// A full compaction races an upsert (`/index` of an existing id) and a
/// flush. The upserted document is live exactly once, with its new value.
#[test]
fn compaction_vs_upsert() {
    explore(
        "compaction vs upsert",
        &[
            "compaction.planned",
            "upsert.before_io",
            "upsert.segment_written",
        ],
        |seed| {
            let dir = temp_dir("cvu", seed);
            let ns = NamespaceId("t/cvu".into());
            let idx = three_segments(&dir, &ns);
            let ops: Vec<Op> = vec![
                {
                    let (idx, ns) = (Arc::clone(&idx), ns.clone());
                    Box::new(move || {
                        idx.compact_namespace_with_options(&ns, CompactOptions::full())
                            .unwrap();
                    })
                },
                {
                    let (idx, ns) = (Arc::clone(&idx), ns.clone());
                    Box::new(move || {
                        idx.index_documents(ns.clone(), vec![doc("a1", "a", 2)])
                            .unwrap();
                    })
                },
                {
                    let (idx, ns) = (Arc::clone(&idx), ns.clone());
                    Box::new(move || {
                        idx.index_documents(ns.clone(), vec![doc("c0", "c", 1)])
                            .unwrap();
                        idx.flush_namespace(&ns).unwrap();
                    })
                },
            ];
            let trace = run_schedule(seed, ops);
            idx.flush_namespace(&ns).unwrap();
            let live = live(&idx, &dir, &ns);
            let mut expected: BTreeMap<String, Vec<u32>> = ["a0", "a2", "b0", "b1", "b2", "c0"]
                .iter()
                .map(|id| (id.to_string(), vec![1]))
                .collect();
            expected.insert("a1".into(), vec![2]);
            assert_eq!(live, expected, "seed {seed:#x}: {trace:?}");
            drop(idx);
            std::fs::remove_dir_all(&dir).ok();
            trace
        },
    );
}

/// Compaction, a delete of group `b`, and an upsert of a group `a`
/// document, all at once.
#[test]
fn compaction_vs_delete_vs_upsert() {
    explore(
        "compaction vs delete vs upsert",
        &[
            "compaction.planned",
            "delete.persist.before",
            "upsert.before_io",
        ],
        |seed| {
            let dir = temp_dir("cdu", seed);
            let ns = NamespaceId("t/cdu".into());
            let idx = three_segments(&dir, &ns);
            let ops: Vec<Op> = vec![
                {
                    let (idx, ns) = (Arc::clone(&idx), ns.clone());
                    Box::new(move || {
                        idx.compact_namespace_with_options(&ns, CompactOptions::full())
                            .unwrap();
                    })
                },
                {
                    let (idx, ns) = (Arc::clone(&idx), ns.clone());
                    Box::new(move || {
                        let manifest = idx.manifest_cloned(&ns).unwrap();
                        assert_eq!(idx.delete_by_query(&ns, &manifest, &group_b()).unwrap(), 3);
                    })
                },
                {
                    let (idx, ns) = (Arc::clone(&idx), ns.clone());
                    Box::new(move || {
                        idx.index_documents(ns.clone(), vec![doc("a2", "a", 2)])
                            .unwrap();
                    })
                },
            ];
            let trace = run_schedule(seed, ops);
            idx.flush_namespace(&ns).unwrap();
            let live = live(&idx, &dir, &ns);
            let mut expected: BTreeMap<String, Vec<u32>> = ["a0", "a1"]
                .iter()
                .map(|id| (id.to_string(), vec![1]))
                .collect();
            expected.insert("a2".into(), vec![2]);
            assert_eq!(live, expected, "seed {seed:#x}: {trace:?}");
            drop(idx);
            std::fs::remove_dir_all(&dir).ok();
            trace
        },
    );
}
