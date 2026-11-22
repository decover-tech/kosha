//! #348 tests: the stored-bytes reconciliation, over an in-memory object
//! inventory with hand-built manifests (every class of object, branches,
//! shards, deleted and unknown namespaces) and end to end over a real
//! server's local data directory (flushes, the hourly snapshot, deletion).

use super::*;
use kosha_control::usage::{meter, MemoryUsageStore, UsageBucket, UsageEvent, UsageStore, HOUR_MS};
use kosha_core::deletes::DeletionVector;
use kosha_core::deletion::NewDeletion;
use kosha_core::gc::RetiredObject;
use kosha_core::{Manifest, ManifestEntry, SegmentId};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use usage::{UsageConfig, UsageMeter};
use usage_reconcile::{
    reconcile, Child, ControlPlane, ControlView, ObjectInventory, Options, Report, Status,
};

/// Any time well inside an hour, so `NOW` and its snapshot share the hour.
/// In the past (2015), so a deletion record these tests create, stamped by
/// the clock, is later than every snapshot taken at `NOW`.
const NOW: u64 = 400_000 * HOUR_MS + 20 * 60_000;

// ─── In-memory fixtures ──────────────────────────────────────────────────────

/// Object storage as a sorted map, listed through the same callbacks as S3.
#[derive(Default)]
struct MemoryInventory {
    objects: BTreeMap<String, u64>,
    /// Every recursive listing asked for, in order.
    walks: RefCell<Vec<String>>,
    fail_walk_of: Option<String>,
}

impl MemoryInventory {
    fn with(objects: &[(&str, u64)]) -> Self {
        Self {
            objects: objects
                .iter()
                .map(|(path, size)| (path.to_string(), *size))
                .collect(),
            ..Self::default()
        }
    }

    fn under(dir: &str) -> String {
        if dir.is_empty() {
            String::new()
        } else {
            format!("{dir}/")
        }
    }
}

impl ObjectInventory for MemoryInventory {
    fn backend(&self) -> &'static str {
        "memory"
    }

    fn walk(&self, dir: &str, visit: &mut dyn FnMut(&str, u64)) -> Result<(), KoshaError> {
        self.walks.borrow_mut().push(dir.to_string());
        if self.fail_walk_of.as_deref() == Some(dir) {
            return Err(KoshaError::NotFound("injected: listing failed".into()));
        }
        let prefix = Self::under(dir);
        for (path, size) in &self.objects {
            if path.starts_with(&prefix) {
                visit(path, *size);
            }
        }
        Ok(())
    }

    fn children(&self, dir: &str, visit: &mut dyn FnMut(Child<'_>)) -> Result<(), KoshaError> {
        let prefix = Self::under(dir);
        let mut last_dir = String::new();
        for (path, size) in &self.objects {
            let Some(rest) = path.strip_prefix(&prefix) else {
                continue;
            };
            match rest.split_once('/') {
                Some((name, _)) => {
                    if name != last_dir {
                        last_dir = name.to_string();
                        visit(Child::Dir(name));
                    }
                }
                None => visit(Child::File(rest, *size)),
            }
        }
        Ok(())
    }
}

fn ns(id: &str) -> NamespaceId {
    NamespaceId(id.into())
}

fn manifest(segments: &[&str]) -> Manifest {
    let mut manifest = Manifest::empty(1);
    manifest.segments = segments
        .iter()
        .map(|segment| ManifestEntry {
            segment_id: SegmentId(segment.to_string()),
            doc_count: 1,
        })
        .collect();
    manifest
}

fn generation(c: char) -> String {
    std::iter::repeat_n(c, 64).collect()
}

fn deletes(c: char) -> String {
    format!("deletes-{}.roaring", generation(c))
}

fn retire(manifest: &mut Manifest, segment: &str) {
    manifest.retired.push(RetiredObject {
        segment_id: SegmentId(segment.into()),
        deletion_generation: None,
        retired_at_ms: NOW,
        retired_in_version: manifest.version,
    });
}

type Control = Mutex<Box<dyn ControlStore>>;

fn control_with(manifests: Vec<(&str, Manifest)>) -> Control {
    let mut controller = kosha_control::Controller::new();
    for (id, manifest) in manifests {
        controller.ensure_namespace(ns(id));
        controller.save_manifest(&ns(id), &manifest).unwrap();
    }
    Mutex::new(Box::new(controller))
}

/// Record the `stored_bytes` snapshot of the hour `at_ms` falls in.
fn snapshot(store: &dyn UsageStore, tenant: &str, namespace: &str, at_ms: u64, bytes: u64) {
    let hour = at_ms / HOUR_MS;
    store
        .record(&[UsageEvent {
            id: format!("stored/{tenant}/{namespace}/{hour}"),
            tenant: tenant.into(),
            namespace: namespace.into(),
            at_ms: hour * HOUR_MS,
            meters: BTreeMap::from([(meter::STORED_BYTES.to_string(), bytes)]),
        }])
        .unwrap();
}

/// Zero tolerance: only an exact match passes.
fn exact() -> Options {
    Options {
        tenant: None,
        tolerance: 0.0,
        tolerance_bytes: 0,
        max_snapshot_age_ms: 3 * HOUR_MS,
        now_ms: NOW,
    }
}

fn row<'a>(
    report: &'a Report,
    tenant: &str,
    namespace: &str,
) -> &'a usage_reconcile::NamespaceReport {
    report
        .namespaces
        .iter()
        .find(|row| row.tenant == tenant && row.namespace == namespace)
        .unwrap_or_else(|| panic!("no row for {tenant}/{namespace}: {report:#?}"))
}

// ─── Classes of objects ──────────────────────────────────────────────────────

/// The snapshot counts every object under a segment the manifest lists,
/// and nothing else. Reconciliation compares that same number, and reports
/// what GC has not removed yet apart from it.
#[test]
fn only_referenced_bytes_are_compared_and_orphans_are_reported_apart() {
    let mut docs = manifest(&["seg-a", "seg-b"]);
    docs.deletion_vectors.insert(
        SegmentId("seg-a".into()),
        DeletionVector {
            generation: generation('1'),
            count: 1,
            namespace: None,
        },
    );
    retire(&mut docs, "seg-old");
    let control = control_with(vec![("acme/docs", docs)]);
    let inventory = MemoryInventory::with(&[
        ("acme/docs/seg-a/postings.bin", 100),
        ("acme/docs/seg-a/_COMMIT", 10),
        (&format!("acme/docs/seg-a/{}", deletes('1')), 7),
        // Superseded, still inside a listed segment: the snapshot counts it.
        (&format!("acme/docs/seg-a/{}", deletes('0')), 5),
        ("acme/docs/seg-b/doc_store.bin", 200),
        // Compacted away, waiting for GC.
        ("acme/docs/seg-old/postings.bin", 1_000),
        // A flush that crashed before it published.
        ("acme/docs/seg-crashed/postings.bin", 400),
        ("acme/docs/NOTES", 3),
        // A sibling whose name starts the same is another prefix.
        ("acme/docs2/seg-z/postings.bin", 50_000),
    ]);
    let store = MemoryUsageStore::new();
    snapshot(&store, "acme", "docs", NOW, 322);

    let mut options = exact();
    options.tenant = Some("acme".into());
    let report = reconcile(&control, &store, &inventory, &options).unwrap();
    let docs = row(&report, "acme", "docs");
    assert_eq!(docs.status, Status::Match, "{docs:#?}");
    assert_eq!(docs.control_plane, ControlPlane::Live);
    assert_eq!(docs.snapshot_bytes, Some(322));
    assert_eq!(docs.snapshot_at_ms, Some(NOW / HOUR_MS * HOUR_MS));
    assert_eq!(docs.difference_bytes, Some(0));
    let storage = &docs.storage;
    assert_eq!(
        (storage.referenced_objects, storage.referenced_bytes),
        (5, 322)
    );
    assert_eq!(storage.superseded_deletion_bytes, 5);
    assert_eq!(
        (storage.orphaned_objects, storage.orphaned_bytes),
        (3, 1_403)
    );
    assert_eq!(
        (
            storage.retired_bytes,
            storage.untracked_bytes,
            storage.stray_bytes
        ),
        (1_000, 400, 3)
    );
    assert_eq!(storage.referenced_unmetered_bytes, 0);
    assert_eq!((storage.objects, storage.bytes), (8, 1_725));
    // The namespace's prefix was listed exactly once.
    assert_eq!(
        inventory
            .walks
            .borrow()
            .iter()
            .filter(|dir| *dir == "acme/docs")
            .count(),
        1
    );

    // The sibling prefix has objects and no namespace: its own row.
    let stranger = row(&report, "acme", "docs2");
    assert_eq!(stranger.control_plane, ControlPlane::Unknown);
    assert_eq!(stranger.status, Status::StorageWithoutSnapshot);
    assert_eq!(stranger.storage.orphaned_bytes, 50_000);
    assert!(!report.within_tolerance);
    assert_eq!(report.namespaces_out_of_tolerance, 1);
    assert_eq!(report.totals.snapshot_bytes, 322);
    assert_eq!(report.totals.storage.referenced_bytes, 322);
    assert_eq!(report.totals.storage.orphaned_bytes, 51_403);
}

/// A branch lists its parent's segments where they are (#208). The
/// snapshot leaves them to the parent, and once the parent drops one that
/// the branch still lists nobody's snapshot counts it: it is referenced,
/// not orphaned, and not part of the comparison.
#[test]
fn segments_and_bitmaps_held_by_a_branch_are_neither_metered_nor_orphaned() {
    let parent_then = manifest(&["seg-a", "seg-shared"]);
    let mut branch = parent_then.branch_from(&ns("acme/docs"), 1);
    branch.segments.push(ManifestEntry {
        segment_id: SegmentId("seg-x".into()),
        doc_count: 1,
    });
    // The branch deleted a document of an inherited segment: the bitmap
    // lands under the branch's own prefix.
    branch.deletion_vectors.insert(
        SegmentId("seg-a".into()),
        DeletionVector {
            generation: generation('2'),
            count: 1,
            namespace: None,
        },
    );
    // Since then the parent compacted `seg-shared` away.
    let mut parent_now = manifest(&["seg-a", "seg-new"]);
    retire(&mut parent_now, "seg-shared");
    let control = control_with(vec![("acme/docs", parent_now), ("acme/exp", branch)]);
    let inventory = MemoryInventory::with(&[
        ("acme/docs/seg-a/postings.bin", 100),
        ("acme/docs/seg-new/postings.bin", 50),
        ("acme/docs/seg-shared/postings.bin", 300),
        ("acme/exp/seg-x/postings.bin", 40),
        (&format!("acme/exp/seg-a/{}", deletes('2')), 9),
        (&format!("acme/exp/seg-a/{}", deletes('3')), 4),
    ]);
    let store = MemoryUsageStore::new();
    // What `usage::run_snapshot_tick` records: owned segments only.
    snapshot(&store, "acme", "docs", NOW, 150);
    snapshot(&store, "acme", "exp", NOW, 40);

    let report = reconcile(&control, &store, &inventory, &exact()).unwrap();
    assert!(report.within_tolerance, "{report:#?}");
    let docs = &row(&report, "acme", "docs").storage;
    assert_eq!(docs.referenced_bytes, 150);
    assert_eq!(
        (
            docs.referenced_unmetered_objects,
            docs.referenced_unmetered_bytes
        ),
        (1, 300)
    );
    assert_eq!(docs.orphaned_bytes, 0);
    let exp = &row(&report, "acme", "exp").storage;
    assert_eq!(exp.referenced_bytes, 40);
    assert_eq!(exp.referenced_unmetered_bytes, 9);
    // The bitmap the branch superseded is garbage.
    assert_eq!((exp.orphaned_bytes, exp.untracked_bytes), (4, 4));
}

/// Shards are metered under their root, a bare (pre-#262) namespace under
/// the empty tenant, and a tenant's namespace that sits below a bare
/// namespace of the tenant's name is not that namespace's garbage.
#[test]
fn shards_roll_up_and_a_bare_namespace_does_not_swallow_a_tenants() {
    let mut root = Manifest::empty(1);
    root.shards = Some(2);
    let control = control_with(vec![
        ("corpus", root),
        ("corpus/_shard-0000", manifest(&["seg-1"])),
        ("corpus/_shard-0001", manifest(&["seg-2"])),
        ("corpus/docs", manifest(&["seg-9"])),
    ]);
    let inventory = MemoryInventory::with(&[
        ("corpus/_shard-0000/seg-1/postings.bin", 11),
        ("corpus/_shard-0001/seg-2/postings.bin", 22),
        ("corpus/_shard-0001/seg-lost/postings.bin", 5),
        ("corpus/docs/seg-9/postings.bin", 700),
    ]);
    let store = MemoryUsageStore::new();
    snapshot(&store, "", "corpus", NOW, 33);
    snapshot(&store, "corpus", "docs", NOW, 700);

    let report = reconcile(&control, &store, &inventory, &exact()).unwrap();
    assert!(report.within_tolerance, "{report:#?}");
    assert_eq!(report.namespaces.len(), 2, "{report:#?}");
    let bare = row(&report, "", "corpus");
    assert_eq!(bare.storage.referenced_bytes, 33);
    assert_eq!(bare.storage.untracked_bytes, 5);
    assert_eq!(bare.storage.bytes, 38, "corpus/docs is not corpus's");
    assert_eq!(row(&report, "corpus", "docs").storage.referenced_bytes, 700);
}

// ─── Namespaces only one side knows ──────────────────────────────────────────

#[test]
fn storage_and_snapshots_without_their_counterpart_are_flagged() {
    let mut controller = kosha_control::Controller::new();
    controller.ensure_namespace(ns("acme/docs"));
    controller
        .save_manifest(&ns("acme/docs"), &manifest(&["seg-a"]))
        .unwrap();
    // A live namespace nobody snapshots, and one with nothing at all.
    controller.ensure_namespace(ns("acme/unmetered"));
    controller
        .save_manifest(&ns("acme/unmetered"), &manifest(&["seg-u"]))
        .unwrap();
    controller.ensure_namespace(ns("acme/fresh"));
    // Deleted namespaces: one whose storage was reclaimed, one whose
    // record says so while objects remain, and a whole deleted tenant.
    let complete = |controller: &mut kosha_control::Controller, deletion: NewDeletion| {
        let record = controller.begin_deletion(&deletion, 3);
        controller
            .record_deletion_attempt(&record.id, 1, None)
            .unwrap()
    };
    complete(
        &mut controller,
        NewDeletion::namespace("acme/reclaimed", Some("acme"), None),
    );
    let leaked = complete(
        &mut controller,
        NewDeletion::namespace("acme/gone", Some("acme"), None),
    );
    let oldco = controller.begin_deletion(&NewDeletion::tenant("oldco"), 0);
    let control: Control = Mutex::new(Box::new(controller));

    let inventory = MemoryInventory::with(&[
        ("acme/docs/seg-a/postings.bin", 100),
        ("acme/unmetered/seg-u/postings.bin", 60),
        ("acme/gone/seg-1/postings.bin", 70),
        ("oldco/a/seg-1/postings.bin", 30),
        ("ghost/ns/seg-1/postings.bin", 20),
        ("_wal/writer-1/000001.wal", 999),
        ("acme/loose.txt", 2),
        ("top.txt", 1),
    ]);
    let store = MemoryUsageStore::new();
    snapshot(&store, "acme", "docs", NOW, 100);
    snapshot(&store, "acme", "reclaimed", NOW - HOUR_MS, 4_000);
    snapshot(&store, "acme", "phantom", NOW, 500);

    let report = reconcile(&control, &store, &inventory, &exact()).unwrap();
    let status = |tenant: &str, namespace: &str| {
        let row = row(&report, tenant, namespace);
        (row.control_plane, row.status, row.within_tolerance)
    };
    assert_eq!(
        status("acme", "docs"),
        (ControlPlane::Live, Status::Match, true)
    );
    assert_eq!(
        status("acme", "unmetered"),
        (ControlPlane::Live, Status::StorageWithoutSnapshot, false)
    );
    assert_eq!(
        status("acme", "fresh"),
        (ControlPlane::Live, Status::Empty, true)
    );
    // Deleted and gone: its last snapshots are still in the window, and
    // that is as it should be.
    assert_eq!(
        status("acme", "reclaimed"),
        (ControlPlane::Deleted, Status::Deleted, true)
    );
    // Deleted, the record even completed, and objects are back or left.
    assert_eq!(
        status("acme", "gone"),
        (ControlPlane::Deleted, Status::DeletedWithStorage, false)
    );
    let gone = row(&report, "acme", "gone");
    assert_eq!(gone.deletion.as_ref().unwrap().id, leaked.id);
    assert_eq!(gone.storage.orphaned_bytes, 70);
    assert_eq!(gone.storage.referenced_bytes, 0);
    // A tenant deletion covers every namespace under the tenant.
    assert_eq!(
        status("oldco", "a"),
        (ControlPlane::Deleted, Status::DeletedWithStorage, false)
    );
    assert_eq!(
        row(&report, "oldco", "a").deletion.as_ref().unwrap().id,
        oldco.id
    );
    assert_eq!(
        status("ghost", "ns"),
        (ControlPlane::Unknown, Status::StorageWithoutSnapshot, false)
    );
    assert_eq!(
        status("acme", "phantom"),
        (ControlPlane::Unknown, Status::SnapshotWithoutStorage, false)
    );
    let phantom = row(&report, "acme", "phantom");
    assert_eq!(phantom.difference_bytes, Some(-500));
    assert_eq!(report.namespaces.len(), 8, "{report:#?}");
    assert_eq!(report.namespaces_out_of_tolerance, 5);
    assert!(!report.within_tolerance);

    // Internal prefixes are named, never listed; loose files are counted.
    assert_eq!(report.skipped_prefixes, vec!["_wal".to_string()]);
    assert!(!inventory
        .walks
        .borrow()
        .iter()
        .any(|dir| dir.starts_with("_wal")));
    let loose: Vec<(&str, u64, u64)> = report
        .unattributed
        .iter()
        .map(|u| (u.prefix.as_str(), u.objects, u.bytes))
        .collect();
    assert_eq!(loose, vec![("", 1, 1), ("acme/", 1, 2)]);

    // One tenant: only its namespaces, its directory and its snapshots.
    let mut options = exact();
    options.tenant = Some("acme".into());
    let acme = reconcile(&control, &store, &inventory, &options).unwrap();
    assert_eq!(acme.tenant.as_deref(), Some("acme"));
    assert!(acme.namespaces.iter().all(|row| row.tenant == "acme"));
    assert_eq!(acme.namespaces.len(), 6, "{acme:#?}");
    assert_eq!(acme.unattributed.len(), 1);
    assert!(acme.skipped_prefixes.is_empty());
    options.tenant = Some("globex".into());
    let globex = reconcile(&control, &store, &inventory, &options).unwrap();
    assert!(globex.namespaces.is_empty() && globex.within_tolerance);
}

/// Deleting a middle branch keeps, under the deleted branch's own prefix,
/// the segments a descendant branch inherited from it, and writes no
/// deletion record (`AppState::delete_branch`). That prefix is unknown to
/// the control plane but a live manifest lists what it holds: referenced,
/// not orphaned, and no reason to fail the run.
#[test]
fn what_a_deleted_branch_left_for_its_descendant_is_referenced_not_orphaned() {
    let docs = manifest(&["seg-a"]);
    let mut b1 = docs.branch_from(&ns("acme/docs"), 1);
    b1.segments.push(ManifestEntry {
        segment_id: SegmentId("seg-x".into()),
        doc_count: 1,
    });
    // `b1` also deleted a document of the segment it inherited from docs.
    b1.deletion_vectors.insert(
        SegmentId("seg-a".into()),
        DeletionVector {
            generation: generation('2'),
            count: 1,
            namespace: None,
        },
    );
    let mut b2 = b1.branch_from(&ns("acme/b1"), 1);
    b2.segments.push(ManifestEntry {
        segment_id: SegmentId("seg-y".into()),
        doc_count: 1,
    });
    // `b1` has been deleted since: only docs and b2 are left.
    let control = control_with(vec![("acme/docs", docs), ("acme/b2", b2)]);
    let held = [
        ("acme/docs/seg-a/postings.bin".to_string(), 100),
        ("acme/b1/seg-x/postings.bin".to_string(), 300),
        (format!("acme/b1/seg-a/{}", deletes('2')), 9),
        ("acme/b2/seg-y/postings.bin".to_string(), 40),
    ];
    let inventory_of = |extra: &[(&str, u64)]| {
        let mut objects: Vec<(&str, u64)> = held
            .iter()
            .map(|(path, size)| (path.as_str(), *size))
            .collect();
        objects.extend_from_slice(extra);
        MemoryInventory::with(&objects)
    };
    let store = MemoryUsageStore::new();
    snapshot(&store, "acme", "docs", NOW, 100);
    snapshot(&store, "acme", "b2", NOW, 40);

    // No snapshot of b1 left in the window.
    let report = reconcile(&control, &store, &inventory_of(&[]), &exact()).unwrap();
    let b1_row = row(&report, "acme", "b1");
    assert_eq!(
        (b1_row.control_plane, b1_row.status, b1_row.within_tolerance),
        (ControlPlane::Unknown, Status::HeldByBranch, true)
    );
    assert_eq!(
        (
            b1_row.storage.referenced_unmetered_objects,
            b1_row.storage.referenced_unmetered_bytes,
            b1_row.storage.orphaned_bytes,
        ),
        (2, 309, 0)
    );
    assert!(report.within_tolerance, "{report:#?}");

    // b1's last snapshot, from the hour before, is still in the window.
    snapshot(&store, "acme", "b1", NOW - HOUR_MS, 300);
    let report = reconcile(&control, &store, &inventory_of(&[]), &exact()).unwrap();
    assert_eq!(row(&report, "acme", "b1").status, Status::HeldByBranch);
    assert!(report.within_tolerance, "{report:#?}");

    // Anything no manifest lists under that prefix is still orphaned, and
    // still fails the run.
    let superseded = format!("acme/b1/seg-a/{}", deletes('3'));
    let dirty = inventory_of(&[("acme/b1/seg-dead/postings.bin", 7), (&superseded, 4)]);
    let report = reconcile(&control, &store, &dirty, &exact()).unwrap();
    let b1_row = row(&report, "acme", "b1");
    assert_eq!(b1_row.storage.referenced_unmetered_bytes, 309);
    assert_eq!(
        (
            b1_row.storage.orphaned_objects,
            b1_row.storage.untracked_bytes
        ),
        (2, 11)
    );
    assert_eq!(b1_row.status, Status::Mismatch);
    assert!(!report.within_tolerance);

    // A snapshot of b1 stamped with the current hour: something still
    // meters it, so the held objects do not excuse it.
    snapshot(&store, "acme", "b1", NOW, 300);
    let report = reconcile(&control, &store, &inventory_of(&[]), &exact()).unwrap();
    assert_eq!(row(&report, "acme", "b1").status, Status::Mismatch);
    assert!(!report.within_tolerance);
}

/// Branch deletion and `migrate-namespace` write no deletion record. The
/// namespace's last snapshots are inside the window for hours afterwards;
/// with an empty prefix that is a namespace that is gone, not a fault.
#[test]
fn a_namespace_removed_without_a_deletion_record_passes_once_its_hour_is_over() {
    let control = control_with(vec![("acme/docs", manifest(&["seg-a"]))]);
    let inventory = MemoryInventory::with(&[("acme/docs/seg-a/postings.bin", 100)]);
    let store = MemoryUsageStore::new();
    snapshot(&store, "acme", "docs", NOW, 100);
    // The deleted branch held 5 MiB: far above any tolerance floor.
    snapshot(&store, "acme", "exp", NOW - 2 * HOUR_MS, 5 << 20);
    snapshot(&store, "acme", "exp", NOW - HOUR_MS, 5 << 20);

    for options in [exact(), Options::from_lookup(|_| None, NOW).unwrap()] {
        let report = reconcile(&control, &store, &inventory, &options).unwrap();
        let exp = row(&report, "acme", "exp");
        assert_eq!(
            (exp.control_plane, exp.status, exp.within_tolerance),
            (ControlPlane::Unknown, Status::Removed, true)
        );
        assert_eq!(
            exp.snapshot_at_ms,
            Some((NOW - HOUR_MS) / HOUR_MS * HOUR_MS)
        );
        assert!(report.within_tolerance, "{report:#?}");
    }

    // Snapshotted again in the current hour: a writer still meters a
    // namespace that is stored nowhere.
    snapshot(&store, "acme", "exp", NOW, 5 << 20);
    let report = reconcile(&control, &store, &inventory, &exact()).unwrap();
    let exp = row(&report, "acme", "exp");
    assert_eq!(
        (exp.status, exp.within_tolerance),
        (Status::SnapshotWithoutStorage, false)
    );
    assert!(!report.within_tolerance);
}

/// A deleted pre-#262 namespace has no tenant directory: its record names
/// the top-level prefix itself.
#[test]
fn a_deleted_bare_namespace_is_one_row_not_one_per_segment() {
    let mut controller = kosha_control::Controller::new();
    controller.begin_deletion(&NewDeletion::namespace("legacy", None, None), 7);
    let control: Control = Mutex::new(Box::new(controller));
    let inventory = MemoryInventory::with(&[
        ("legacy/seg-1/postings.bin", 10),
        ("legacy/seg-2/postings.bin", 20),
    ]);
    let report = reconcile(&control, &MemoryUsageStore::new(), &inventory, &exact()).unwrap();
    assert_eq!(report.namespaces.len(), 1, "{report:#?}");
    let legacy = row(&report, "", "legacy");
    assert_eq!(legacy.status, Status::DeletedWithStorage);
    assert_eq!(legacy.storage.bytes, 30);
}

/// The snapshots a deleted namespace had before it was deleted are its
/// history. One stamped with an hour that began after the deletion means a
/// writer is still metering (and billing) it.
#[test]
fn a_snapshot_taken_after_the_deletion_is_flagged() {
    let mut controller = kosha_control::Controller::new();
    let record = controller.begin_deletion(
        &NewDeletion::namespace("acme/zombie", Some("acme"), None),
        3,
    );
    controller.begin_deletion(&NewDeletion::namespace("acme/gone", Some("acme"), None), 3);
    let control: Control = Mutex::new(Box::new(controller));
    let deleted_at = record.requested_at_ms;
    let store = MemoryUsageStore::new();
    // The hour of the deletion itself is stamped at its start: before it.
    snapshot(&store, "acme", "gone", deleted_at, 10);
    snapshot(&store, "acme", "zombie", deleted_at, 10);
    snapshot(&store, "acme", "zombie", deleted_at + HOUR_MS, 10);
    let options = Options {
        now_ms: deleted_at + HOUR_MS + 60_000,
        ..exact()
    };
    let report = reconcile(&control, &store, &MemoryInventory::default(), &options).unwrap();
    assert_eq!(row(&report, "acme", "gone").status, Status::Deleted);
    let zombie = row(&report, "acme", "zombie");
    assert_eq!(zombie.status, Status::SnapshotAfterDeletion);
    assert!(!zombie.within_tolerance && !report.within_tolerance);
}

#[test]
fn unknown_prefixes_beyond_the_cap_are_counted_and_fail_the_run() {
    let objects: Vec<(String, u64)> = (0..1_003)
        .map(|i| (format!("acme/stray-{i:04}/seg/postings.bin"), 1))
        .collect();
    let objects: Vec<(&str, u64)> = objects.iter().map(|(p, s)| (p.as_str(), *s)).collect();
    let inventory = MemoryInventory::with(&objects);
    let control = control_with(vec![("acme/docs", Manifest::empty(1))]);
    let report = reconcile(&control, &MemoryUsageStore::new(), &inventory, &exact()).unwrap();
    assert_eq!(report.unknown_prefixes_not_listed, 3);
    // 1,000 unknown prefixes and the (empty) live namespace.
    assert_eq!(report.namespaces.len(), 1_001);
    assert!(!report.within_tolerance);
    assert_eq!(report.warnings.len(), 1, "{:?}", report.warnings);
}

// ─── Tolerance and snapshot age ──────────────────────────────────────────────

#[test]
fn the_tolerance_is_the_larger_of_the_fraction_and_the_byte_floor() {
    let control = control_with(vec![("acme/docs", manifest(&["seg-a"]))]);
    let inventory = MemoryInventory::with(&[("acme/docs/seg-a/postings.bin", 1_040)]);
    let store = MemoryUsageStore::new();
    snapshot(&store, "acme", "docs", NOW, 1_000);
    let run = |tolerance: f64, tolerance_bytes: u64| {
        let options = Options {
            tolerance,
            tolerance_bytes,
            ..exact()
        };
        let report = reconcile(&control, &store, &inventory, &options).unwrap();
        let docs = row(&report, "acme", "docs").clone();
        assert_eq!(docs.difference_bytes, Some(40));
        assert_eq!(report.within_tolerance, docs.within_tolerance);
        (docs.status, docs.allowed_difference_bytes)
    };
    assert_eq!(run(0.05, 0), (Status::Match, Some(52)));
    assert_eq!(run(0.01, 0), (Status::Mismatch, Some(11)));
    assert_eq!(run(0.0, 40), (Status::Match, Some(40)));
    assert_eq!(run(0.0, 39), (Status::Mismatch, Some(39)));
    assert_eq!(run(0.01, 1_024), (Status::Match, Some(1_024)));
}

/// Only a recent snapshot counts: a namespace whose writer stopped
/// snapshotting is storage nobody meters, whatever its old snapshot said.
#[test]
fn a_snapshot_older_than_the_window_is_no_snapshot() {
    let control = control_with(vec![("acme/docs", manifest(&["seg-a"]))]);
    let inventory = MemoryInventory::with(&[("acme/docs/seg-a/postings.bin", 100)]);
    let store = MemoryUsageStore::new();
    snapshot(&store, "acme", "docs", NOW - 2 * HOUR_MS, 100);
    // An older snapshot does not win over a newer one.
    snapshot(&store, "acme", "docs", NOW - 3 * HOUR_MS, 1);
    let at = |now_ms: u64| {
        let options = Options { now_ms, ..exact() };
        let report = reconcile(&control, &store, &inventory, &options).unwrap();
        row(&report, "acme", "docs").status
    };
    assert_eq!(at(NOW), Status::Match);
    assert_eq!(at(NOW + 2 * HOUR_MS), Status::StorageWithoutSnapshot);
}

// ─── Failures ────────────────────────────────────────────────────────────────

struct DownStore;

impl UsageStore for DownStore {
    fn record(&self, _: &[UsageEvent]) -> Result<u64, KoshaError> {
        Err(KoshaError::ControlPlane("injected: store down".into()))
    }
    fn hourly(&self, _: Option<&str>, _: u64, _: u64) -> Result<Vec<UsageBucket>, KoshaError> {
        Err(KoshaError::ControlPlane("injected: store down".into()))
    }
    fn latest_between(
        &self,
        _: Option<&str>,
        _: &str,
        _: u64,
        _: u64,
    ) -> Result<Vec<kosha_control::usage::UsageGauge>, KoshaError> {
        Err(KoshaError::ControlPlane("injected: store down".into()))
    }
}

/// A control store that cannot be read, or one without deletion records.
struct BrokenControl {
    inner: Control,
    namespaces_down: bool,
}

impl ControlView for BrokenControl {
    fn namespaces(&self) -> Result<Vec<NamespaceId>, KoshaError> {
        if self.namespaces_down {
            return Err(KoshaError::ControlPlane("injected: postgres down".into()));
        }
        self.inner.namespaces()
    }
    fn manifest(&self, ns: &NamespaceId) -> Result<Option<Manifest>, KoshaError> {
        self.inner.manifest(ns)
    }
    fn deletions(&self, _: u32) -> Result<Vec<kosha_core::deletion::DeletionRecord>, KoshaError> {
        Err(KoshaError::ControlPlane("injected: no records".into()))
    }
}

/// A read that fails stops the run: a report built on a partial listing
/// or an empty namespace list would clear or flag the wrong namespaces.
#[test]
fn a_failed_read_fails_the_run_instead_of_reporting_half_of_it() {
    let control = control_with(vec![("acme/docs", manifest(&["seg-a"]))]);
    let store = MemoryUsageStore::new();
    let mut inventory = MemoryInventory::with(&[("acme/docs/seg-a/postings.bin", 100)]);

    let error = reconcile(&control, &DownStore, &inventory, &exact()).unwrap_err();
    assert!(error.contains("usage store"), "{error}");

    let mut broken = BrokenControl {
        inner: control_with(vec![("acme/docs", manifest(&["seg-a"]))]),
        namespaces_down: true,
    };
    let error = reconcile(&broken, &store, &inventory, &exact()).unwrap_err();
    assert!(error.contains("control store"), "{error}");
    // Missing deletion records only cost the explanation of what is gone.
    broken.namespaces_down = false;
    let report = reconcile(&broken, &store, &inventory, &exact()).unwrap();
    assert_eq!(report.warnings.len(), 1, "{:?}", report.warnings);
    assert_eq!(report.namespaces.len(), 1);

    inventory.fail_walk_of = Some("acme/docs".into());
    let error = reconcile(&control, &store, &inventory, &exact()).unwrap_err();
    assert!(error.contains("listing acme/docs/"), "{error}");
}

// ─── Settings ────────────────────────────────────────────────────────────────

#[test]
fn settings_default_and_an_invalid_one_is_refused() {
    let none = |_: &str| None;
    let defaults = Options::from_lookup(none, NOW).unwrap();
    assert_eq!(
        (
            defaults.tolerance,
            defaults.tolerance_bytes,
            defaults.max_snapshot_age_ms
        ),
        (0.05, 1024 * 1024, 3 * HOUR_MS)
    );
    assert_eq!(defaults.tenant, None);

    let env = |pairs: &'static [(&'static str, &'static str)]| {
        move |name: &str| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        }
    };
    let set = Options::from_lookup(
        env(&[
            ("KOSHA_USAGE_RECONCILE_TOLERANCE", "0.2"),
            ("KOSHA_USAGE_RECONCILE_TOLERANCE_BYTES", "4096"),
            ("KOSHA_USAGE_RECONCILE_MAX_SNAPSHOT_AGE", "6h"),
        ]),
        NOW,
    )
    .unwrap();
    assert_eq!(
        (set.tolerance, set.tolerance_bytes, set.max_snapshot_age_ms),
        (0.2, 4096, 6 * HOUR_MS)
    );
    // Set but empty is unset.
    assert_eq!(
        Options::from_lookup(env(&[("KOSHA_USAGE_RECONCILE_TOLERANCE", " ")]), NOW).unwrap(),
        defaults
    );
    for (name, value) in [
        ("KOSHA_USAGE_RECONCILE_TOLERANCE", "5%"),
        ("KOSHA_USAGE_RECONCILE_TOLERANCE", "1.5"),
        ("KOSHA_USAGE_RECONCILE_TOLERANCE", "-0.1"),
        ("KOSHA_USAGE_RECONCILE_TOLERANCE", "NaN"),
        ("KOSHA_USAGE_RECONCILE_TOLERANCE_BYTES", "1MiB"),
        ("KOSHA_USAGE_RECONCILE_MAX_SNAPSHOT_AGE", "soon"),
        // Shorter than the snapshot interval: everything would be flagged.
        ("KOSHA_USAGE_RECONCILE_MAX_SNAPSHOT_AGE", "10m"),
    ] {
        let error = Options::from_lookup(
            move |asked: &str| (asked == name).then(|| value.to_string()),
            NOW,
        )
        .unwrap_err();
        assert!(error.contains(name), "{name}={value}: {error}");
    }
}

// ─── End to end over a server's data directory ───────────────────────────────

fn test_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "kosha-test-reconcile-{}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
        unix_millis()
    ))
}

/// A standalone server with metering on, and its usage store.
fn metered_state() -> (AppState, Arc<MemoryUsageStore>) {
    let dir = test_dir();
    let store = Arc::new(MemoryUsageStore::new());
    let state = AppState::with_options(
        dir.clone(),
        StateOptions {
            usage: Some(UsageMeter::new(store.clone(), UsageConfig::under(&dir))),
            ..Default::default()
        },
    );
    (state, store)
}

fn status_of(response: &str) -> u16 {
    response
        .get(9..12)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0)
}

fn body_of(response: &str) -> serde_json::Value {
    let body = response.split_once("\r\n\r\n").expect("no body").1;
    serde_json::from_str(body).unwrap_or(serde_json::Value::Null)
}

fn call(state: &AppState, tenant: &str, line: &str, body: serde_json::Value) -> String {
    let bytes = if body.is_null() {
        Vec::new()
    } else {
        serde_json::to_vec(&body).unwrap()
    };
    let response = route(
        &format!("{line} HTTP/1.1\r\n"),
        &HashMap::new(),
        &bytes,
        tenant,
        state,
    );
    assert_eq!(status_of(&response), 200, "{line}: {response}");
    response
}

fn write_and_flush(state: &AppState, tenant: &str, namespace: &str, ids: std::ops::Range<u32>) {
    let documents: Vec<_> = ids
        .map(|i| {
            serde_json::json!({"id": format!("d{i}"), "fields": [
                {"name": "title", "field_type": "Text", "value": format!("quick fox {i}")}
            ]})
        })
        .collect();
    call(
        state,
        tenant,
        &format!("POST /v1/namespaces/{namespace}/documents"),
        serde_json::json!({ "documents": documents }),
    );
    call(
        state,
        tenant,
        &format!("POST /v1/namespaces/{namespace}/flush"),
        serde_json::json!({}),
    );
}

/// The admin route as `route` dispatches it (the admin credential check
/// sits in front and is tested separately).
fn reconcile_route(state: &AppState, query: &str) -> (u16, serde_json::Value) {
    let response = usage_reconcile::handle_admin_reconcile(
        &format!("GET /v1/admin/usage/reconcile{query} HTTP/1.1\r\n"),
        state,
    );
    (status_of(&response), body_of(&response))
}

fn plant(state: &AppState, path: &str, bytes: usize) {
    let path = state.data_dir.join(path);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, vec![0u8; bytes]).unwrap();
}

fn json_row<'a>(
    report: &'a serde_json::Value,
    tenant: &str,
    namespace: &str,
) -> &'a serde_json::Value {
    report["namespaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["tenant"] == tenant && row["namespace"] == namespace)
        .unwrap_or_else(|| panic!("no row for {tenant}/{namespace}: {report:#}"))
}

/// What the hourly snapshot recorded is, byte for byte, what the listing
/// finds under the manifest's segments: zero tolerance passes, for a plain
/// and for a sharded namespace, with garbage lying next to the segments.
#[test]
fn a_real_snapshot_reconciles_exactly_with_the_data_directory() {
    let (state, _store) = metered_state();
    write_and_flush(&state, "acme", "docs", 0..3);
    call(
        &state,
        "acme",
        "POST /v1/namespaces/big/shards",
        serde_json::json!({"shards": 3}),
    );
    write_and_flush(&state, "acme", "big", 0..40);
    write_and_flush(&state, "globex", "docs", 0..2);
    assert_eq!(usage::run_snapshot_tick(&state, unix_millis()), 3);

    // Not referenced by any manifest: must not disturb the comparison.
    plant(&state, "acme/docs/seg-crashed/postings.bin", 5_000);
    plant(&state, "acme/docs/NOTES", 7);
    plant(&state, "acme/big/_shard-0001/seg-crashed/postings.bin", 300);

    let (status, report) = reconcile_route(&state, "?tolerance=0&tolerance_bytes=0");
    assert_eq!(status, 200, "{report}");
    assert_eq!(report["within_tolerance"], true, "{report:#}");
    assert_eq!(report["storage_backend"], "local");
    assert_eq!(report["namespaces_checked"], 3);
    let docs = json_row(&report, "acme", "docs");
    assert_eq!(docs["status"], "match");
    assert_eq!(docs["control_plane"], "live");
    assert!(docs["snapshot_bytes"].as_u64().unwrap() > 0);
    assert_eq!(docs["storage"]["referenced_bytes"], docs["snapshot_bytes"]);
    assert_eq!(docs["difference_bytes"], 0);
    assert_eq!(docs["storage"]["orphaned_bytes"], 5_007);
    assert_eq!(docs["storage"]["untracked_bytes"], 5_000);
    assert_eq!(docs["storage"]["stray_bytes"], 7);
    let big = json_row(&report, "acme", "big");
    assert_eq!(big["status"], "match");
    assert!(big["snapshot_bytes"].as_u64().unwrap() > 0);
    assert_eq!(big["storage"]["referenced_bytes"], big["snapshot_bytes"]);
    assert_eq!(big["storage"]["untracked_bytes"], 300);
    // The spool and the WAL are not namespaces.
    let skipped = report["skipped_prefixes"].as_array().unwrap();
    assert!(
        skipped.contains(&serde_json::json!("_usage")),
        "{skipped:?}"
    );

    // The namespace grew after its snapshot: out of tolerance at zero, and
    // by exactly the new segment; within a tolerance that allows it.
    write_and_flush(&state, "acme", "docs", 3..6);
    let (_, grown) = reconcile_route(&state, "?tenant=acme&tolerance=0&tolerance_bytes=0");
    assert_eq!(grown["within_tolerance"], false);
    assert_eq!(grown["namespaces_out_of_tolerance"], 1);
    assert_eq!(grown["namespaces_checked"], 2, "globex is out of scope");
    let docs = json_row(&grown, "acme", "docs");
    assert_eq!(docs["status"], "mismatch");
    let difference = docs["difference_bytes"].as_i64().unwrap();
    assert!(difference > 0);
    assert_eq!(
        docs["storage"]["referenced_bytes"].as_i64().unwrap(),
        docs["snapshot_bytes"].as_i64().unwrap() + difference
    );
    let (_, allowed) = reconcile_route(&state, "?tenant=acme&tolerance=1");
    assert_eq!(allowed["within_tolerance"], true, "{allowed:#}");
}

/// Deleting a namespace (#347) leaves its last snapshots in the usage
/// store. With its storage gone that is expected; with objects still under
/// its prefix the run fails and names the deletion record.
#[test]
fn a_deleted_namespace_is_explained_by_its_deletion_record() {
    let (state, _store) = metered_state();
    write_and_flush(&state, "acme", "docs", 0..3);
    write_and_flush(&state, "acme", "keep", 0..3);
    assert_eq!(usage::run_snapshot_tick(&state, unix_millis()), 2);
    call(
        &state,
        "acme",
        "DELETE /v1/namespaces/docs",
        serde_json::Value::Null,
    );

    let (_, report) = reconcile_route(&state, "?tolerance=0&tolerance_bytes=0");
    assert_eq!(report["within_tolerance"], true, "{report:#}");
    let docs = json_row(&report, "acme", "docs");
    assert_eq!(docs["control_plane"], "deleted");
    assert_eq!(docs["status"], "deleted");
    assert!(docs["snapshot_bytes"].as_u64().unwrap() > 0);
    assert_eq!(docs["storage"]["objects"], 0);
    assert_eq!(docs["deletion"]["state"], "completed");
    assert_eq!(docs["deletion"]["scope"], "namespace");

    // An object reappears under the deleted prefix (a late upload, a
    // reclaim that missed it).
    plant(&state, "acme/docs/seg-late/postings.bin", 64);
    let (_, report) = reconcile_route(&state, "");
    assert_eq!(report["within_tolerance"], false);
    let docs = json_row(&report, "acme", "docs");
    assert_eq!(docs["status"], "deleted_with_storage");
    assert_eq!(docs["storage"]["orphaned_bytes"], 64);
    assert_eq!(json_row(&report, "acme", "keep")["status"], "match");
}

#[test]
fn the_route_is_admin_only_off_without_metering_and_refuses_bad_parameters() {
    let (state, _store) = metered_state();
    // No admin credential on this request.
    let refused = route(
        "GET /v1/admin/usage/reconcile HTTP/1.1\r\n",
        &HashMap::new(),
        &[],
        "acme",
        &state,
    );
    assert_eq!(status_of(&refused), 403, "{refused}");
    // The usage route next to it still answers its own path only.
    let usage = usage::handle_admin_usage("GET /v1/admin/usage HTTP/1.1\r\n", &state);
    assert!(body_of(&usage).get("pipeline").is_some(), "{usage}");

    for bad in [
        "?tolerance=lots",
        "?tolerance=2",
        "?tolerance_bytes=-1",
        "?max_snapshot_age=5m",
        "?tenant=a/b",
        "?tolerence=0.1",
    ] {
        let (status, body) = reconcile_route(&state, bad);
        assert_eq!(status, 400, "{bad}: {body}");
    }
    let (status, body) = reconcile_route(&state, "?tolerance=0.5&max_snapshot_age=2h&tenant=acme");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["tolerance"], 0.5);
    assert_eq!(body["max_snapshot_age_seconds"], 7_200);
    assert_eq!(body["tenant"], "acme");

    let off = AppState::new(test_dir());
    let (status, _) = reconcile_route(&off, "");
    assert_eq!(status, 404);
}

// ─── Command line ────────────────────────────────────────────────────────────

#[test]
fn the_command_prints_one_json_report_and_exits_by_the_verdict() {
    use usage_reconcile::{execute as execute_for_test, exit_code, parse_args};
    let parse = |flags: &[&str], options: Options| {
        parse_args(flags.iter().map(|flag| flag.to_string()), options)
    };

    let control = control_with(vec![("acme/docs", manifest(&["seg-a"]))]);
    let inventory = MemoryInventory::with(&[("acme/docs/seg-a/postings.bin", 1_040)]);
    let store = MemoryUsageStore::new();
    snapshot(&store, "acme", "docs", NOW, 1_000);

    let run = |flags: &[&str]| {
        let mut out = Vec::new();
        let args = parse(flags, exact()).unwrap().expect("not --help");
        let result = execute_for_test(&control, &store, &inventory, &args, &mut out);
        (result, String::from_utf8(out).unwrap())
    };
    let (result, out) = run(&[]);
    assert_eq!(result, Ok(false));
    assert_eq!(out.lines().count(), 1, "one JSON document: {out}");
    let report: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(report["within_tolerance"], false);
    assert_eq!(report["namespaces"][0]["status"], "mismatch");
    assert_eq!(exit_code(result), 1);

    let (result, out) = run(&["--tolerance", "0.05", "--pretty"]);
    assert_eq!(result, Ok(true));
    assert!(out.lines().count() > 1);
    let report: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(report["within_tolerance"], true);
    assert_eq!(exit_code(result), 0);
    let (result, _) = run(&[
        "--tolerance-bytes=40",
        "--tenant=acme",
        "--max-snapshot-age=2h",
    ]);
    assert_eq!(result, Ok(true));

    // A run that could not compare anything is neither verdict.
    let mut out = Vec::new();
    let args = parse(&[], exact()).unwrap().unwrap();
    let failed = execute_for_test(&control, &DownStore, &inventory, &args, &mut out);
    assert!(failed.is_err() && out.is_empty());
    assert_eq!(exit_code(failed), 2);

    assert_eq!(parse(&["--help"], exact()), Ok(None));
    for bad in [
        &["--tolerance"][..],
        &["--tolerance", "high"],
        &["--max-snapshot-age", "1m"],
        &["--dry-run"],
        &["acme"],
        &["--tenant", ""],
    ] {
        assert!(parse(bad, exact()).is_err(), "{bad:?}");
    }
}
