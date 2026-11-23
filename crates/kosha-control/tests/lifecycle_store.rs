//! #347 part 2: deletion scopes, per-pod acknowledgements and export jobs,
//! run against both control stores through the `ControlStore` trait.
//!
//! The Postgres half needs a disposable database:
//! KOSHA_TEST_DATABASE_URL=... cargo test -p kosha-control --features postgres --test lifecycle_store -- --ignored

use kosha_core::deletion::{DeletionScope, DeletionState, NewDeletion, PodHeartbeat};
use kosha_core::export::{ExportFilter, ExportState, ExportUpdate, NewExport};
use kosha_core::{ControlStore, Manifest, NamespaceId};

fn pod(id: &str) -> PodHeartbeat {
    PodHeartbeat {
        pod_id: id.into(),
        role: "query".into(),
        purge_enabled: true,
    }
}

fn exercise(store: &mut dyn ControlStore, tag: &str) {
    // ── Branch and migrated scopes round-trip ────────────────────────────
    let branch = store
        .begin_deletion(
            &NewDeletion::branch(&format!("{tag}/b"), Some(tag), None),
            4,
        )
        .unwrap();
    assert_eq!(branch.scope, DeletionScope::Branch);
    let migrated = store
        .begin_deletion(&NewDeletion::migrated(&format!("{tag}-bare"), tag), 0)
        .unwrap();
    assert_eq!(migrated.scope, DeletionScope::Migrated);
    assert_eq!(migrated.tenant_id.as_deref(), Some(tag));
    assert_eq!(
        store.deletion(&migrated.id).unwrap().unwrap().scope,
        DeletionScope::Migrated
    );

    // ── Acknowledgements ─────────────────────────────────────────────────
    let pod_a = format!("{tag}-pod-a");
    let pod_b = format!("{tag}-pod-b");
    store.heartbeat_pod(&pod(&pod_a)).unwrap();
    store.heartbeat_pod(&pod(&pod_b)).unwrap();
    let record = store
        .begin_deletion(
            &NewDeletion::namespace(&format!("{tag}/docs"), Some(tag), None).with_acks(true),
            7,
        )
        .unwrap();
    assert!(record.acks_required);
    // Not before storage is verified, whoever acknowledged.
    store.acknowledge_deletion(&record.id, &pod_a).unwrap();
    store.acknowledge_deletion(&record.id, &pod_b).unwrap();
    let early = store.finalize_deletion(&record.id, 60_000).unwrap();
    assert_eq!(early.state, DeletionState::Pending);
    // Storage verified: pending until every live pod acknowledged.
    let verified = store.record_deletion_attempt(&record.id, 3, None).unwrap();
    assert!(verified.storage_verified && verified.awaiting_acks());
    assert_eq!(verified.state, DeletionState::Pending);
    let pod_c = format!("{tag}-pod-c");
    store.heartbeat_pod(&pod(&pod_c)).unwrap();
    let status = store.deletion_ack_status(&record.id, 60_000).unwrap();
    assert!(status.missing.contains(&pod_c), "{status:?}");
    assert!(!status.complete());
    assert_eq!(
        store.finalize_deletion(&record.id, 60_000).unwrap().state,
        DeletionState::Pending
    );
    // Acknowledging twice is fine.
    store.acknowledge_deletion(&record.id, &pod_c).unwrap();
    store.acknowledge_deletion(&record.id, &pod_c).unwrap();
    let done = store.finalize_deletion(&record.id, 60_000).unwrap();
    assert_eq!(done.state, DeletionState::Completed);
    assert!(done.completed_at_ms.is_some());

    // A record without acks completes on a clean attempt, as before.
    let plain = store
        .begin_deletion(
            &NewDeletion::namespace(&format!("{tag}/plain"), Some(tag), None),
            1,
        )
        .unwrap();
    let plain = store.record_deletion_attempt(&plain.id, 0, None).unwrap();
    assert!(plain.is_completed() && plain.storage_verified);
    store.note_deletion_exports(&plain.id, 2).unwrap();
    assert_eq!(
        store.deletion(&plain.id).unwrap().unwrap().exports_deleted,
        2
    );

    // ── Exports ──────────────────────────────────────────────────────────
    let ns = NamespaceId(format!("{tag}/docs"));
    let export = store
        .create_export(&NewExport {
            tenant_id: tag.into(),
            namespace: ns.0.clone(),
            include_vectors: true,
            pins: vec![(ns.clone(), Manifest::empty(5))],
            upload_url: Some("https://bucket.example.com/put?sig=secret".into()),
            ttl_ms: 1000,
        })
        .unwrap();
    assert_eq!(export.state, ExportState::Pending);
    assert_eq!(export.versions, vec![(ns.0.clone(), 5)]);
    assert_eq!(export.delivery_host.as_deref(), Some("bucket.example.com"));
    let json = serde_json::to_string(&export).unwrap();
    assert!(!json.contains("secret"), "the upload URL never serializes");
    let mine = format!("{tag}-writer");
    let claimed = store
        .claim_export(&export.id, &mine, 60_000)
        .unwrap()
        .unwrap();
    assert_eq!((claimed.state, claimed.attempts), (ExportState::Running, 1));
    assert_eq!(claimed.pins.len(), 1);
    // Held by a live claim: not another pod's.
    assert!(store
        .claim_export(&export.id, "other", 60_000)
        .unwrap()
        .is_none());
    // A holder-guarded update by another pod changes nothing.
    let foreign = ExportUpdate {
        holder: Some("other".into()),
        state: Some(ExportState::Failed),
        ..ExportUpdate::default()
    };
    assert!(store.update_export(&export.id, &foreign).unwrap().is_none());
    let active = store.exports(&ExportFilter::active()).unwrap();
    assert!(active
        .iter()
        .any(|e| e.id == export.id && !e.pins.is_empty()));
    let finished = store
        .update_export(
            &export.id,
            &ExportUpdate {
                holder: Some(mine.clone()),
                state: Some(ExportState::Completed),
                documents: Some(3),
                completed_at_ms: Some(1_700_000_000_000),
                expires_at_ms: Some(1_700_000_001_000),
                clear_pins: true,
                clear_upload_url: true,
                ..ExportUpdate::default()
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(finished.state, ExportState::Completed);
    assert!(finished.pins.is_empty() && finished.upload_url.is_none());
    assert_eq!(finished.expires_at_ms, Some(1_700_000_001_000));
    assert!(!store
        .exports(&ExportFilter::active())
        .unwrap()
        .iter()
        .any(|e| e.id == export.id));
    let of_ns = ExportFilter {
        tenant_id: Some(tag.into()),
        namespace: Some(ns.0.clone()),
        ..ExportFilter::default()
    };
    assert_eq!(store.exports(&of_ns).unwrap().len(), 1);
    assert_eq!(
        store
            .delete_exports(std::slice::from_ref(&export.id))
            .unwrap(),
        1
    );
    assert!(store.export(&export.id).unwrap().is_none());
}

#[test]
fn in_memory_store_keeps_scopes_acks_and_exports() {
    let mut store = kosha_control::Controller::new();
    exercise(&mut store, "mem");
}

#[cfg(feature = "postgres")]
#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn postgres_store_keeps_scopes_acks_and_exports() {
    let url = std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
    let mut store = kosha_control::PgStore::new(&url).unwrap();
    // A second constructor re-runs every migration (010 included).
    let _again = kosha_control::PgStore::new(&url).unwrap();
    let tag = format!("lc-{}", uuid::Uuid::new_v4().simple());
    exercise(&mut store, &tag);
    // Paging by position uses the new index expression and stays exact.
    let first = store.deletions_after(None, 1).unwrap();
    if let Some(record) = first.first() {
        let next = store.deletions_after(Some(&record.position()), 1).unwrap();
        assert!(next.iter().all(|r| r.position() > record.position()));
    }
}
