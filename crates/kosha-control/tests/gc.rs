//! Segment GC (#239) against the in-memory `Controller` and a local-disk
//! object store: what a dry run lists, what a real run deletes, and what it
//! must never touch.

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kosha_control::Controller;
use kosha_core::deletes::DeletionVector;
use kosha_core::gc::{self, CandidateKind, CandidateReason, GcConfig, RetiredObject};
use kosha_core::{
    ControlStore, KoshaError, LocalStorage, Manifest, ManifestEntry, NamespaceId, ObjectMeta,
    SegmentId, StorageBackend,
};

const HOUR_MS: u64 = 3_600_000;

/// Local storage that records the order of deletes.
#[derive(Debug)]
struct Recording {
    inner: LocalStorage,
    deletes: Mutex<Vec<String>>,
}

impl StorageBackend for Recording {
    fn read(&self, path: &str) -> Result<Vec<u8>, KoshaError> {
        self.inner.read(path)
    }
    fn write(&self, path: &str, data: &[u8]) -> Result<(), KoshaError> {
        self.inner.write(path, data)
    }
    fn exists(&self, path: &str) -> bool {
        self.inner.exists(path)
    }
    fn delete(&self, path: &str) -> Result<(), KoshaError> {
        self.deletes.lock().unwrap().push(path.to_string());
        self.inner.delete(path)
    }
    fn list(&self, path: &str) -> Result<Vec<String>, KoshaError> {
        self.inner.list(path)
    }
    fn create_dir_all(&self, path: &str) -> Result<(), KoshaError> {
        self.inner.create_dir_all(path)
    }
    fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectMeta>, KoshaError> {
        self.inner.list_objects(prefix)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn put(root: &Path, rel: &str, bytes: &[u8]) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn age(root: &Path, rel: &str, by: Duration) {
    let file = std::fs::File::options()
        .write(true)
        .open(root.join(rel))
        .unwrap();
    file.set_modified(SystemTime::now() - by).unwrap();
}

fn generation(c: char) -> String {
    c.to_string().repeat(64)
}

struct Fixture {
    root: PathBuf,
    ns: NamespaceId,
    storage: Recording,
    controller: Controller,
}

const LIVE: &str = "tenant_idx-w1-000002";
const COMPACTED: &str = "tenant_idx-w1-000001";
const YOUNG_ORPHAN: &str = "tenant_idx-w2-000007";
const OLD_ORPHAN: &str = "tenant_idx-w3-000003";

/// Namespace `tenant/idx` at manifest version 5:
/// - `LIVE` referenced, current deletion generation `c`, superseded `a`
///   still on storage with a retirement record;
/// - `COMPACTED` retired by a compaction (record), fully uploaded;
/// - `YOUNG_ORPHAN` untracked, no `_COMMIT`, written just now;
/// - `OLD_ORPHAN` untracked, committed, three days old;
/// - `tenant/idx/nested`, another namespace living under this prefix.
fn fixture(name: &str) -> Fixture {
    let root = std::env::temp_dir().join(format!("kosha-gc-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let ns = NamespaceId("tenant/idx".into());
    let current = generation('c');
    let superseded = generation('a');
    for file in ["footer.json", "doc_store.bin", "_COMMIT"] {
        put(&root, &format!("tenant/idx/{LIVE}/{file}"), b"live");
        put(
            &root,
            &format!("tenant/idx/{COMPACTED}/{file}"),
            b"old-data",
        );
    }
    put(
        &root,
        &format!("tenant/idx/{LIVE}/deletes-{current}.roaring"),
        b"cur",
    );
    put(
        &root,
        &format!("tenant/idx/{LIVE}/deletes-{superseded}.roaring"),
        b"older",
    );
    put(
        &root,
        &format!("tenant/idx/{YOUNG_ORPHAN}/doc_store.bin"),
        b"x",
    );
    for file in ["footer.json", "_COMMIT"] {
        let rel = format!("tenant/idx/{OLD_ORPHAN}/{file}");
        put(&root, &rel, b"orphan");
        age(&root, &rel, Duration::from_secs(3 * 24 * 3600));
    }
    put(
        &root,
        "tenant/idx/nested/tenant_idx_nested-x-000001/doc_store.bin",
        b"n",
    );

    let retired_at = now_ms() - 2 * HOUR_MS;
    let mut manifest = Manifest {
        version: 5,
        segments: vec![ManifestEntry {
            segment_id: SegmentId(LIVE.into()),
            doc_count: 4,
        }],
        segment_footers: Default::default(),
        footer_crc32: Default::default(),
        wal_checkpoint: 0,
        wal_origin: None,
        tombstones: Default::default(),
        deletion_vectors: Default::default(),
        retired: vec![
            RetiredObject {
                segment_id: SegmentId(COMPACTED.into()),
                deletion_generation: None,
                retired_at_ms: retired_at,
                retired_in_version: 4,
            },
            RetiredObject {
                segment_id: SegmentId(LIVE.into()),
                deletion_generation: Some(superseded),
                retired_at_ms: retired_at,
                retired_in_version: 5,
            },
        ],
        schema: Default::default(),
    };
    manifest.deletion_vectors.insert(
        SegmentId(LIVE.into()),
        DeletionVector {
            generation: current,
            count: 1,
        },
    );
    let mut controller = Controller::new();
    controller.save_manifest(&ns, &manifest).unwrap();
    controller.ensure_namespace(NamespaceId("tenant/idx/nested".into()));
    Fixture {
        storage: Recording {
            inner: LocalStorage::new(root.clone()),
            deletes: Mutex::new(Vec::new()),
        },
        root,
        ns,
        controller,
    }
}

fn config(dry_run: bool) -> GcConfig {
    GcConfig {
        dry_run,
        grace_ms: HOUR_MS,
        orphan_grace_ms: 24 * HOUR_MS,
        now_ms: now_ms(),
    }
}

fn run(
    fx: &mut Fixture,
    config: &GcConfig,
    mut on_reload: impl FnMut(&mut Controller, usize),
) -> gc::NamespaceGcReport {
    let others: Vec<NamespaceId> = fx
        .controller
        .list_namespaces()
        .iter()
        .filter(|n| **n != fx.ns)
        .cloned()
        .collect();
    let calls = Cell::new(0usize);
    let controller = std::cell::RefCell::new(&mut fx.controller);
    let ns = fx.ns.clone();
    let mut load = || {
        let n = calls.get();
        calls.set(n + 1);
        on_reload(&mut controller.borrow_mut(), n);
        Ok(controller.borrow().manifest_cloned(&ns))
    };
    let ns2 = fx.ns.clone();
    let controller_ref = &controller;
    let mut commit = |expected: u64, manifest: Manifest| {
        controller_ref
            .borrow_mut()
            .compare_and_swap_manifest(&ns2, expected, manifest)
    };
    gc::collect_namespace(&fx.storage, &fx.ns, &others, &mut load, &mut commit, config).unwrap()
}

fn exists(fx: &Fixture, rel: &str) -> bool {
    fx.root.join(rel).exists()
}

#[test]
fn dry_run_lists_exactly_the_unreferenced_objects_and_deletes_nothing() {
    let mut fx = fixture("dry");
    let report = run(&mut fx, &config(true), |_, _| {});
    let mut listed: Vec<(CandidateKind, CandidateReason, String)> = report
        .candidates
        .iter()
        .map(|c| (c.kind, c.reason, c.segment_id.clone()))
        .collect();
    listed.sort_by(|a, b| a.2.cmp(&b.2));
    assert_eq!(
        listed,
        vec![
            (
                CandidateKind::Segment,
                CandidateReason::Retired,
                COMPACTED.to_string()
            ),
            (
                CandidateKind::DeletionVector,
                CandidateReason::Retired,
                LIVE.to_string()
            ),
            (
                CandidateKind::Segment,
                CandidateReason::Untracked,
                OLD_ORPHAN.to_string()
            ),
        ]
    );
    let dv = report
        .candidates
        .iter()
        .find(|c| c.kind == CandidateKind::DeletionVector)
        .unwrap();
    assert_eq!(dv.deletion_generation.as_deref(), Some(&*generation('a')));
    // 3 × "old-data" + "older" + 2 × "orphan"
    assert_eq!(report.reclaimable_bytes, 3 * 8 + 5 + 2 * 6);
    assert!(report
        .skipped
        .iter()
        .any(|s| s.path.ends_with(YOUNG_ORPHAN) && s.reason.contains("no _COMMIT")));
    assert!(fx.storage.deletes.lock().unwrap().is_empty());
    assert_eq!(fx.controller.manifest(&fx.ns).unwrap().version, 5);
    assert!(exists(&fx, &format!("tenant/idx/{COMPACTED}/_COMMIT")));
    let _ = std::fs::remove_dir_all(&fx.root);
}

#[test]
fn real_run_deletes_commit_first_never_touches_referenced_and_prunes_records() {
    let mut fx = fixture("real");
    let report = run(&mut fx, &config(false), |_, _| {});
    assert_eq!(report.aborted, None);
    assert_eq!(report.segments_deleted, 2);
    assert_eq!(report.deletion_vectors_deleted, 1);
    assert_eq!(report.bytes_reclaimed, 3 * 8 + 5 + 2 * 6);
    assert_eq!(report.records_pruned, 2);

    // `_COMMIT` goes before any data file of the same segment.
    let deletes = fx.storage.deletes.lock().unwrap().clone();
    for seg in [COMPACTED, OLD_ORPHAN] {
        let mine: Vec<&String> = deletes.iter().filter(|p| p.contains(seg)).collect();
        assert!(mine[0].ends_with("/_COMMIT"), "{mine:?}");
        assert!(mine.len() > 1);
    }

    // Gone.
    assert!(!exists(
        &fx,
        &format!("tenant/idx/{COMPACTED}/doc_store.bin")
    ));
    assert!(!exists(
        &fx,
        &format!("tenant/idx/{OLD_ORPHAN}/footer.json")
    ));
    assert!(!exists(
        &fx,
        &format!("tenant/idx/{LIVE}/deletes-{}.roaring", generation('a'))
    ));
    // Kept: referenced segment and its current generation, the in-progress
    // upload, and the nested namespace.
    for file in ["footer.json", "doc_store.bin", "_COMMIT"] {
        assert!(exists(&fx, &format!("tenant/idx/{LIVE}/{file}")));
    }
    assert!(exists(
        &fx,
        &format!("tenant/idx/{LIVE}/deletes-{}.roaring", generation('c'))
    ));
    assert!(exists(
        &fx,
        &format!("tenant/idx/{YOUNG_ORPHAN}/doc_store.bin")
    ));
    assert!(exists(
        &fx,
        "tenant/idx/nested/tenant_idx_nested-x-000001/doc_store.bin"
    ));
    assert!(
        deletes
            .iter()
            .all(|p| !p.contains(LIVE)
                || p.ends_with(&format!("deletes-{}.roaring", generation('a'))))
    );

    // The retirement records were dropped in a new manifest version that
    // still references exactly what it did.
    let after = fx.controller.manifest(&fx.ns).unwrap();
    assert_eq!(after.version, 6);
    assert!(after.retired.is_empty());
    assert_eq!(after.segments.len(), 1);
    assert_eq!(
        after.deletion_vectors[&SegmentId(LIVE.into())].generation,
        generation('c')
    );

    // A second run has nothing left to do but the young orphan it keeps.
    let again = run(&mut fx, &config(false), |_, _| {});
    assert!(again.candidates.is_empty());
    assert_eq!(again.records_pruned, 0);
    let _ = std::fs::remove_dir_all(&fx.root);
}

#[test]
fn a_manifest_change_mid_run_aborts_the_namespace() {
    let mut fx = fixture("abort");
    let ns = fx.ns.clone();
    let report = run(&mut fx, &config(false), |controller, call| {
        if call == 1 {
            // A publish lands between planning and deleting.
            let mut next = controller.manifest_cloned(&ns).unwrap();
            next.version += 1;
            controller.compare_and_swap_manifest(&ns, 5, next).unwrap();
        }
    });
    assert!(report.aborted.as_deref().unwrap().contains("changed"));
    assert_eq!(report.objects_deleted, 0);
    assert!(fx.storage.deletes.lock().unwrap().is_empty());
    assert!(exists(&fx, &format!("tenant/idx/{COMPACTED}/_COMMIT")));
    assert_eq!(fx.controller.manifest(&fx.ns).unwrap().version, 6);
    assert_eq!(fx.controller.manifest(&fx.ns).unwrap().retired.len(), 2);
    let _ = std::fs::remove_dir_all(&fx.root);
}

#[test]
fn retired_objects_inside_the_grace_window_are_kept() {
    let mut fx = fixture("grace");
    let mut cfg = config(false);
    cfg.grace_ms = 3 * HOUR_MS;
    let report = run(&mut fx, &cfg, |_, _| {});
    assert_eq!(
        report
            .candidates
            .iter()
            .map(|c| c.segment_id.as_str())
            .collect::<Vec<_>>(),
        vec![OLD_ORPHAN]
    );
    assert!(exists(&fx, &format!("tenant/idx/{COMPACTED}/_COMMIT")));
    assert!(exists(
        &fx,
        &format!("tenant/idx/{LIVE}/deletes-{}.roaring", generation('a'))
    ));
    assert_eq!(fx.controller.manifest(&fx.ns).unwrap().retired.len(), 2);
    let _ = std::fs::remove_dir_all(&fx.root);
}

#[test]
fn stale_retirement_records_are_pruned() {
    let mut fx = fixture("stale");
    // The compacted segment's objects are already gone (an earlier run
    // deleted them but lost the manifest commit).
    std::fs::remove_dir_all(fx.root.join(format!("tenant/idx/{COMPACTED}"))).unwrap();
    let mut cfg = config(false);
    cfg.grace_ms = 3 * HOUR_MS; // keep the DV generation this time
    let report = run(&mut fx, &cfg, |_, _| {});
    assert_eq!(report.records_pruned, 1);
    let after = fx.controller.manifest(&fx.ns).unwrap();
    assert_eq!(after.retired.len(), 1);
    assert_eq!(after.retired[0].segment_id.0, LIVE);
    let _ = std::fs::remove_dir_all(&fx.root);
}
