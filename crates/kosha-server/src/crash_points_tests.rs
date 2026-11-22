//! Crash-point harness for the write path (#257). Built only with
//! `--features failpoints` (CI's `--all-features` includes it).
//!
//! One test per failpoint. Each runs a fixed workload (index, flush,
//! delete, upsert, buffered delete, compaction) through the real request
//! handlers against a temp data dir, an in-memory control plane and an
//! in-memory object bucket, and panics the moment the point is reached.
//! The node's state is then dropped without running a destructor
//! (`mem::forget`, a process death: no shutdown flush), a new node is
//! opened on the same dir, control plane and bucket, it replays its WAL
//! and flushes, and the harness asserts:
//!
//! - every acknowledged write is present with its latest acknowledged
//!   value, and every acknowledged delete is absent (the operation the
//!   crash interrupted may have landed or not, but nothing else);
//! - no document is live twice;
//! - every segment of the published manifest has a `_COMMIT` in the
//!   bucket, and every file that marker lists is there at its size.
//!
//! A dry run first counts how often the workload reaches the point (so a
//! point that is never reached fails its test); the crash is then injected
//! at the first, a middle and the last hit.
//!
//! Lives in the binary crate (not `tests/`) because `AppState` and the
//! handlers are private to it. Failpoints are process-global and the rest
//! of this binary's tests run in parallel, so every point is armed only
//! for the harness thread: on any other thread the callback is a no-op.

use super::tests::{shared_store, test_dir, SharedManifestStore};
use super::*;
use kosha_core::{Document, DocumentId, Field};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

const NS: &str = "t/crash";

// ─── In-memory bucket ─────────────────────────────────────────────────────

/// Object store the publish path uploads segments into while installed on
/// the current thread (see `upload_hook`).
#[derive(Clone, Default)]
struct Bucket(Arc<Mutex<BTreeMap<String, Vec<u8>>>>);

impl Bucket {
    fn put(&self, key: String, bytes: Vec<u8>) {
        self.0.lock().unwrap().insert(key, bytes);
    }
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.0.lock().unwrap().get(key).cloned()
    }
}

thread_local! {
    static BUCKET: RefCell<Option<Bucket>> = const { RefCell::new(None) };
}

/// `publish_namespace`'s upload step under test: upload every segment of
/// `manifest` not yet committed in the thread's bucket, data files first
/// and `_COMMIT` last (the order `S3Storage::sync_segment_dir` uses, through
/// the same `commit_uploaded_files`), then its deletion vectors. A no-op
/// on threads without a bucket, i.e. every other test.
pub(crate) fn upload_hook(
    data_dir: &Path,
    ns: &NamespaceId,
    manifest: &kosha_core::Manifest,
) -> Result<(), KoshaError> {
    let Some(bucket) = BUCKET.with(|b| b.borrow().clone()) else {
        return Ok(());
    };
    for entry in &manifest.segments {
        let dir = manifest.segment_dir(data_dir, ns, &entry.segment_id);
        let rel = dir
            .strip_prefix(data_dir)
            .unwrap_or(&dir)
            .to_string_lossy()
            .into_owned();
        let commit_key = format!("{rel}/{}", segment_commit::COMMIT_FILE);
        if bucket.get(&commit_key).is_some() {
            continue;
        }
        let mut names: Vec<String> = fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .filter_map(|e| e.file_name().to_str().map(str::to_owned))
            .filter(|name| {
                name != segment_commit::COMMIT_FILE
                    && !name.starts_with('.')
                    && !name.contains(".tmp.")
                    && kosha_core::gc::deletion_generation_of(name).is_none()
            })
            .collect();
        names.sort();
        let mut files = Vec::new();
        for name in names {
            let data = fs::read(dir.join(&name))?;
            files.push(segment_commit::SegmentCommit::entry(&name, &data));
            bucket.put(format!("{rel}/{name}"), data);
        }
        segment_commit::commit_uploaded_files(files, |bytes| {
            bucket.put(commit_key, bytes);
            Ok::<_, KoshaError>(())
        })?;
    }
    for (segment, vector) in &manifest.deletion_vectors {
        if vector.is_inherited(ns) {
            continue;
        }
        let path = vector.path(ns, segment)?;
        bucket.put(path.clone(), fs::read(data_dir.join(&path))?);
    }
    Ok(())
}

// ─── Arming ───────────────────────────────────────────────────────────────

const CRASH: &str = "crash-point harness: injected crash";

/// A failpoint armed for the current thread only: counts its hits there
/// and panics on hit number `crash_at` (never, when `None`). Removed on
/// drop.
struct Armed {
    name: &'static str,
    hits: Arc<AtomicUsize>,
}

impl Armed {
    fn new(name: &'static str, crash_at: Option<usize>) -> Self {
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        let owner = std::thread::current().id();
        fail::cfg_callback(name, move || {
            if std::thread::current().id() != owner {
                return;
            }
            let hit = counter.fetch_add(1, Ordering::SeqCst) + 1;
            if Some(hit) == crash_at {
                panic!("{CRASH} at {name} (hit {hit})");
            }
        })
        .unwrap();
        Self { name, hits }
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}

impl Drop for Armed {
    fn drop(&mut self) {
        fail::remove(self.name);
    }
}

// ─── Workload and model ───────────────────────────────────────────────────

#[derive(Clone, Copy, Debug)]
enum Op {
    /// `(id, group, version)` documents in one `/index` request.
    Index(&'static [(&'static str, &'static str, u32)]),
    Flush,
    /// Delete every document whose `grp` is this.
    Delete(&'static str),
    Compact,
}

/// Covers every durability step: WAL appends, flushes and their
/// publications, a delete of flushed documents, an upsert of a flushed
/// document, a delete of buffered documents (#238), and a compaction.
const WORKLOAD: &[Op] = &[
    Op::Index(&[("a", "keep", 1), ("b", "drop", 1), ("c", "keep", 1)]),
    Op::Flush,
    Op::Index(&[("d", "drop", 1), ("e", "keep", 1)]),
    Op::Flush,
    Op::Delete("drop"),
    Op::Index(&[("a", "keep", 2)]),
    Op::Index(&[("f", "keep", 1), ("g", "drop", 1)]),
    Op::Delete("drop"),
    Op::Flush,
    Op::Compact,
    Op::Index(&[("h", "keep", 1)]),
    Op::Flush,
];

/// Expected live documents: id → `(group, version)`.
type Model = BTreeMap<String, (String, u32)>;

fn apply(model: &mut Model, op: Op) {
    match op {
        Op::Index(docs) => {
            for (id, grp, v) in docs {
                model.insert((*id).into(), ((*grp).into(), *v));
            }
        }
        Op::Delete(grp) => model.retain(|_, (g, _)| g != grp),
        Op::Flush | Op::Compact => {}
    }
}

fn run(state: &AppState, op: Op) {
    let ns = NamespaceId(NS.into());
    let response = match op {
        Op::Index(docs) => {
            let body = serde_json::to_vec(&IndexRequest {
                namespace: ns,
                documents: docs
                    .iter()
                    .map(|(id, grp, v)| Document {
                        id: DocumentId((*id).into()),
                        fields: vec![
                            Field::text("body", format!("document {id}")),
                            Field::keyword("grp", *grp),
                            Field::keyword("v", v.to_string()),
                        ],
                    })
                    .collect(),
            })
            .unwrap();
            handle_index(&body, state)
        }
        Op::Flush => handle_flush(format!(r#"{{"namespace":"{NS}"}}"#).as_bytes(), state),
        Op::Delete(grp) => handle_delete(
            format!(r#"{{"namespace":"{NS}","filter":{{"term":{{"grp":"{grp}"}}}}}}"#).as_bytes(),
            state,
        ),
        Op::Compact => {
            match compaction_pass_with(
                state,
                state,
                &ns,
                kosha_write::CompactOptions::full(),
                |_| {},
            ) {
                Ok(_) => "HTTP/1.1 200 OK".into(),
                Err(e) => format!("HTTP/1.1 500 {e}"),
            }
        }
    };
    assert!(response.starts_with("HTTP/1.1 200"), "{op:?}: {response}");
}

struct Node {
    dir: PathBuf,
    control: SharedManifestStore,
    bucket: Bucket,
}

impl Node {
    fn new() -> Self {
        Self {
            dir: test_dir(),
            control: shared_store(),
            bucket: Bucket::default(),
        }
    }

    /// A node over this dir, control plane and bucket, as after a restart.
    fn open(&self) -> AppState {
        BUCKET.with(|b| *b.borrow_mut() = Some(self.bucket.clone()));
        AppState::with_options(
            self.dir.clone(),
            StateOptions {
                wal_store: None,
                control_store: Some(Box::new(self.control.clone())),
                role: None,
                writer_lease: None,
                writer_lease_ttl: None,
                usage: None,
                billing: None,
            },
        )
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        BUCKET.with(|b| *b.borrow_mut() = None);
        fs::remove_dir_all(&self.dir).ok();
    }
}

/// Run the workload on a fresh node, crashing at hit `crash_at` of `point`.
/// Returns how often the point was hit, and — after a crash — the models
/// before and after the interrupted operation.
fn crash_run(node: &Node, point: &'static str, crash_at: Option<usize>) -> (usize, Model, Model) {
    let armed = Armed::new(point, crash_at);
    let state = node.open();
    let mut acked = Model::new();
    let mut in_flight = Model::new();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for &op in WORKLOAD {
            in_flight = acked.clone();
            apply(&mut in_flight, op);
            run(&state, op);
            acked = in_flight.clone();
        }
    }));
    let hits = armed.hits();
    drop(armed);
    match outcome {
        Ok(()) => {
            assert!(crash_at.is_none(), "{point}: armed crash never fired");
            drop(state);
            (hits, acked.clone(), acked)
        }
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            assert!(
                message.starts_with(CRASH),
                "{point}: workload failed instead of crashing: {message}"
            );
            // Process death: no destructor, no shutdown flush, locks
            // possibly poisoned mid-operation.
            std::mem::forget(state);
            (hits, acked, in_flight)
        }
    }
}

/// Recover a node after a crash and check the invariants.
fn verify_recovery(node: &Node, point: &str, crash_at: usize, before: &Model, after: &Model) {
    let context = format!("crash at {point} (hit {crash_at})");
    let state = node.open();
    let ns = NamespaceId(NS.into());
    let flush = handle_flush(format!(r#"{{"namespace":"{NS}"}}"#).as_bytes(), &state);
    assert!(
        flush.starts_with("HTTP/1.1 200"),
        "{context}: replay flush: {flush}"
    );
    assert_eq!(state.indexer.pending_doc_count(&ns), 0, "{context}");

    // Live documents of the recovered node.
    // A crash before the first write was acknowledged leaves no namespace.
    let manifest = state.indexer.manifest_cloned(&ns);
    let tombstones = state.indexer.get_tombstones(&ns).unwrap_or_default();
    let mut live: BTreeMap<String, Vec<(String, u32)>> = BTreeMap::new();
    for entry in manifest.iter().flat_map(|m| &m.segments) {
        let manifest = manifest.as_ref().unwrap();
        let reader = kosha_segment::SegmentReader::open(manifest.segment_dir(
            &state.data_dir,
            &ns,
            &entry.segment_id,
        ))
        .unwrap_or_else(|e| panic!("{context}: segment {}: {e}", entry.segment_id.0));
        let dead = tombstones.get(&entry.segment_id);
        for seq in 0..reader.doc_count() {
            if dead.is_some_and(|d| d.contains(&seq)) {
                continue;
            }
            let record = reader.doc_record_full(seq).unwrap().unwrap();
            let field = |name: &str| {
                record
                    .fields
                    .iter()
                    .find(|f| f.name == name)
                    .map(|f| f.value.clone())
                    .unwrap_or_default()
            };
            live.entry(record.doc_id.0.clone())
                .or_default()
                .push((field("grp"), field("v").parse().unwrap()));
        }
    }
    for (id, copies) in &live {
        assert_eq!(copies.len(), 1, "{context}: {id} is live {copies:?}");
    }
    let ids: std::collections::BTreeSet<&String> = before
        .keys()
        .chain(after.keys())
        .chain(live.keys())
        .collect();
    for id in ids {
        let got = live.get(id).map(|c| &c[0]);
        let allowed = [before.get(id), after.get(id)];
        assert!(
            allowed.contains(&got),
            "{context}: {id} is {got:?}; acknowledged {:?}, in flight {:?}",
            before.get(id),
            after.get(id)
        );
    }

    // The published manifest only references committed segments.
    let Some(published) = node.control.manifest_cloned(&ns) else {
        assert!(
            live.is_empty(),
            "{context}: live documents, nothing published"
        );
        return;
    };
    assert_eq!(
        state.indexer.manifest_version(&ns),
        Some(published.version),
        "{context}: replay published"
    );
    for entry in &published.segments {
        let dir = published.segment_dir(&state.data_dir, &ns, &entry.segment_id);
        let rel = dir.strip_prefix(&state.data_dir).unwrap().to_string_lossy();
        let commit = node
            .bucket
            .get(&format!("{rel}/{}", segment_commit::COMMIT_FILE))
            .unwrap_or_else(|| panic!("{context}: published {rel} has no _COMMIT"));
        let commit = segment_commit::SegmentCommit::from_bytes(&commit).unwrap();
        for file in &commit.files {
            let data = node
                .bucket
                .get(&format!("{rel}/{}", file.name))
                .unwrap_or_else(|| panic!("{context}: {rel}/{} missing", file.name));
            assert_eq!(
                data.len() as u64,
                file.size,
                "{context}: {rel}/{}",
                file.name
            );
        }
        for required in segment_commit::REQUIRED_SEGMENT_FILES {
            assert!(
                commit.get(required).is_some(),
                "{context}: {rel} {required}"
            );
        }
    }
    for (segment, vector) in &published.deletion_vectors {
        let path = vector.path(&ns, segment).unwrap();
        assert!(
            node.bucket.get(&path).is_some(),
            "{context}: deletion vector {path} not uploaded"
        );
    }
    drop(state);
}

/// The matrix cell for `point`: prove it is reached, then crash at its
/// first, middle and last hit and verify recovery each time.
fn crash_matrix(point: &'static str) {
    let node = Node::new();
    let (hits, model, _) = crash_run(&node, point, None);
    assert!(
        hits > 0,
        "failpoint {point} was never reached by the workload"
    );
    drop(node);
    // The uninterrupted run must itself satisfy the invariants.
    let node = Node::new();
    crash_run(&node, point, None);
    verify_recovery(&node, point, 0, &model, &model);
    drop(node);

    let mut targets = vec![1, hits.div_ceil(2), hits];
    targets.dedup();
    for crash_at in targets {
        let node = Node::new();
        let (reached, before, after) = crash_run(&node, point, Some(crash_at));
        assert_eq!(reached, crash_at, "{point}: crash fired at the wrong hit");
        verify_recovery(&node, point, crash_at, &before, &after);
    }
}

macro_rules! crash_point_tests {
    ($($test:ident => $point:literal,)*) => {
        $(
            #[test]
            fn $test() {
                crash_matrix($point);
            }
        )*
        /// Every point the matrix covers, for the reachability test.
        const POINTS: &[&str] = &[$($point),*];
    };
}

crash_point_tests! {
    crash_at_wal_append_before_fsync => "wal.append.before_fsync",
    crash_at_wal_append_after_fsync => "wal.append.after_fsync",
    crash_at_flush_segment_written => "flush.segment_written",
    crash_at_flush_before_upload => "flush.before_upload",
    crash_at_flush_after_upload_before_commit_marker => "flush.after_upload_before_commit_marker",
    crash_at_publish_before_cas => "publish.before_cas",
    crash_at_publish_after_cas_before_ack => "publish.after_cas_before_ack",
    crash_at_wal_truncate_before => "wal.truncate.before",
    crash_at_compaction_output_written => "compaction.output_written",
    crash_at_compaction_before_publish => "compaction.before_publish",
    crash_at_delete_persist_before => "delete.persist.before",
    crash_at_upsert_segment_written => "upsert.segment_written",
}

/// The failpoint names the harness covers are exactly the durability
/// points named in #257 (plus the upsert segment write); a renamed point
/// would otherwise silently drop out of the matrix.
#[test]
fn matrix_covers_every_durability_point() {
    for required in [
        "wal.append.before_fsync",
        "wal.append.after_fsync",
        "flush.segment_written",
        "flush.before_upload",
        "flush.after_upload_before_commit_marker",
        "publish.before_cas",
        "publish.after_cas_before_ack",
        "wal.truncate.before",
        "compaction.output_written",
        "compaction.before_publish",
        "delete.persist.before",
    ] {
        assert!(POINTS.contains(&required), "{required} not in the matrix");
    }
}
