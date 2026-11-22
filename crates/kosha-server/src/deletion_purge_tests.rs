//! Tests for the per-pod purge of deleted namespaces (#347).
//!
//! Pods are separate `AppState`s with their own data directories over one
//! shared control store. A follower's disk cache is filled by copying the
//! writer's segment directories into it, which is what hydration from
//! object storage leaves behind.

use super::*;
use crate::tests::{test_dir, SharedManifestStore};
use kosha_core::deletion::NewDeletion;
use kosha_core::{Document, DocumentId, Field};
use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;

/// No waiting: sweeps repeat on the next pass and nothing counts as
/// recently written.
fn tuning() -> PurgeTuning {
    PurgeTuning {
        settle: Duration::ZERO,
        write_margin: Duration::ZERO,
        page: 2,
    }
}

type SharedControl = Arc<Mutex<kosha_control::Controller>>;

fn writer_on(control: &SharedControl) -> AppState {
    let state = AppState::new(test_dir());
    *state.controller.lock().unwrap() = Box::new(SharedManifestStore::new(control.clone()));
    state
}

fn follower_on(control: &SharedControl, data_dir: PathBuf) -> AppState {
    let mut state = AppState::new(data_dir);
    state.role = role::Role::Query;
    state.ingest_host = Some("http://ingest.invalid".into());
    state.manifest_follow = manifest_follow::ManifestFollow::new(None);
    *state.controller.lock().unwrap() = Box::new(SharedManifestStore::new(control.clone()));
    state
}

/// A writer pod and a query pod over one control store.
fn pods() -> (AppState, AppState, SharedControl) {
    let control: SharedControl = Arc::new(Mutex::new(kosha_control::Controller::new()));
    let writer = writer_on(&control);
    let follower = follower_on(&control, test_dir());
    (writer, follower, control)
}

fn parse(response: &str) -> (u16, serde_json::Value) {
    let status: u16 = response[9..12].parse().unwrap();
    let body = &response[response.find("\r\n\r\n").unwrap() + 4..];
    (status, serde_json::from_str(body).unwrap())
}

fn call(
    state: &AppState,
    tenant: &str,
    line: &str,
    body: serde_json::Value,
) -> (u16, serde_json::Value) {
    parse(&route(
        &format!("{line} HTTP/1.1\r\n"),
        &HashMap::new(),
        &serde_json::to_vec(&body).unwrap(),
        tenant,
        state,
    ))
}

fn index(state: &AppState, tenant: &str, ns: &str, ids: &[&str]) {
    let documents: Vec<_> = ids
        .iter()
        .map(|id| {
            serde_json::json!({"id": id, "fields": [
                {"name": "body", "field_type": "Text", "value": format!("common {id}")}]})
        })
        .collect();
    let (status, body) = call(
        state,
        tenant,
        &format!("POST /v1/namespaces/{ns}/documents"),
        serde_json::json!({ "documents": documents }),
    );
    assert_eq!(status, 200, "{body}");
    let (status, body) = call(
        state,
        tenant,
        &format!("POST /v1/namespaces/{ns}/flush"),
        serde_json::json!({}),
    );
    assert_eq!(status, 200, "{body}");
}

/// Search status and the ids found.
fn search(state: &AppState, tenant: &str, ns: &str) -> (u16, Vec<String>) {
    let (status, body) = call(
        state,
        tenant,
        &format!("POST /v1/namespaces/{ns}/search"),
        serde_json::json!({"query_text": "common", "top_k": 50}),
    );
    let mut ids: Vec<String> = body["results"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|hit| hit["doc_id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    (status, ids)
}

fn delete_ns(state: &AppState, tenant: &str, ns: &str) {
    let (status, body) = call(
        state,
        tenant,
        &format!("DELETE /v1/namespaces/{ns}"),
        serde_json::json!({}),
    );
    assert_eq!(status, 200, "{body}");
}

fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap().flatten() {
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_tree(&entry.path(), &target);
        } else if !target.exists() {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// Copy what `from` holds on disk for `subject` (a namespace id or a
/// tenant) into `to`'s data directory: a follower that hydrated all of it.
fn hydrate(from: &AppState, to: &AppState, subject: &str) {
    copy_tree(&from.data_dir.join(subject), &to.data_dir.join(subject));
}

/// The directory names under `{data_dir}/{ns}`.
fn dirs(state: &AppState, ns: &str) -> BTreeSet<String> {
    std::fs::read_dir(state.data_dir.join(ns))
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| entry.path().is_dir())
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

/// The segments the control store lists for `ns` right now.
fn stored_segments(state: &AppState, ns: &str) -> BTreeSet<String> {
    state
        .controller
        .lock()
        .unwrap()
        .try_manifest_cloned(&NamespaceId(ns.into()))
        .unwrap()
        .map(|manifest| {
            manifest
                .segments
                .iter()
                .map(|entry| entry.segment_id.0.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// Let "now" move past the modification time of what was just written.
fn settle_clock() {
    std::thread::sleep(Duration::from_millis(20));
}

fn pass(state: &AppState) -> PassReport {
    settle_clock();
    state.run_deletion_purge_pass(&tuning())
}

#[test]
fn intervals_are_opt_in_and_a_typo_is_refused() {
    assert_eq!(
        PurgeSettings::parse(None, None).unwrap(),
        PurgeSettings::default()
    );
    assert_eq!(PurgeSettings::default().interval, None);
    assert_eq!(
        PurgeSettings::parse(Some(" 0 "), Some("")).unwrap(),
        PurgeSettings::default()
    );
    let set = PurgeSettings::parse(Some("250"), Some("60000")).unwrap();
    assert_eq!(set.interval, Some(Duration::from_millis(250)));
    assert_eq!(set.resume_interval, Some(Duration::from_secs(60)));
    for bad in ["30s", "-1", "1.5", "on"] {
        let refused = PurgeSettings::parse(Some(bad), None).unwrap_err();
        assert!(
            refused.contains("KOSHA_DELETION_PURGE_INTERVAL_MS"),
            "{refused}"
        );
        let refused = PurgeSettings::parse(None, Some(bad)).unwrap_err();
        assert!(
            refused.contains("KOSHA_DELETION_RESUME_INTERVAL_MS"),
            "{refused}"
        );
    }
}

/// The deferred item itself: a namespace deleted on the writer pod leaves
/// a follower's memory and disk on the follower's next pass, and nothing
/// else does.
#[test]
fn a_follower_drops_a_deleted_namespace_from_memory_and_disk() {
    let (writer, follower, _) = pods();
    index(&writer, "acme", "docs", &["a", "b"]);
    index(&writer, "acme", "docs2", &["n"]);
    index(&writer, "acme-eu", "docs", &["e"]);
    hydrate(&writer, &follower, "acme");
    hydrate(&writer, &follower, "acme-eu");
    assert_eq!(
        search(&follower, "acme", "docs"),
        (200, vec!["a".into(), "b".into()])
    );
    assert_eq!(search(&follower, "acme", "docs2").1, vec!["n".to_string()]);

    delete_ns(&writer, "acme", "docs");
    // Without a pass the follower keeps its copy: the documents are still
    // on its disk.
    assert!(!dirs(&follower, "acme/docs").is_empty());
    assert_eq!(follower.deletion_purge.to_json()["enabled"], false);

    let report = pass(&follower);
    assert_eq!(report.records, 1, "{report:?}");
    assert!(report.segment_dirs_removed >= 1, "{report:?}");
    assert!(report.errors.is_empty(), "{report:?}");
    assert!(
        !follower.data_dir.join("acme/docs").exists(),
        "the deleted namespace's directory is gone"
    );
    let stats = follower.deletion_purge.to_json();
    assert_eq!(stats["caught_up"], true, "{stats}");
    assert!(
        stats["kosha_deletion_purge_parsed_segments_dropped_total"]
            .as_u64()
            .unwrap()
            >= 1,
        "the parsed segment the search cached was dropped: {stats}"
    );
    assert!(follower
        .result_cache
        .lock()
        .unwrap()
        .entries
        .keys()
        .all(|(ns, _, _)| ns != "acme/docs"));
    assert!(follower
        .completed_hydrations
        .lock()
        .unwrap()
        .keys()
        .all(|(ns, _)| ns != "acme/docs"));
    assert_eq!(search(&follower, "acme", "docs").0, 404);
    // A namespace sharing the name as a prefix, and another tenant's
    // namespace of the same name, are untouched and still answer.
    assert_eq!(search(&follower, "acme", "docs2"), (200, vec!["n".into()]));
    assert_eq!(
        search(&follower, "acme-eu", "docs"),
        (200, vec!["e".into()])
    );

    // The record is not handled twice. The subject is swept once more
    // (the settle sweep) and then left alone.
    let again = pass(&follower);
    assert_eq!((again.records, again.sweeps), (0, 1), "{again:?}");
    let idle = pass(&follower);
    assert_eq!((idle.records, idle.sweeps), (0, 0), "{idle:?}");
    // A later deletion is picked up.
    delete_ns(&writer, "acme", "docs2");
    let next = pass(&follower);
    assert_eq!(next.records, 1, "{next:?}");
    assert!(!follower.data_dir.join("acme/docs2").exists());
    assert_eq!(
        search(&follower, "acme-eu", "docs"),
        (200, vec!["e".into()])
    );
}

/// PR #420's risk: the namespace exists again, under the same name and in
/// the same directory, when a pod gets round to purging. Only the deleted
/// incarnation's segments go.
#[test]
fn a_recreated_namespace_keeps_its_files_while_the_old_incarnation_goes() {
    let (writer, follower, _) = pods();
    index(&writer, "acme", "docs", &["old1", "old2"]);
    hydrate(&writer, &follower, "acme/docs");
    assert_eq!(search(&follower, "acme", "docs").1.len(), 2);
    let old = stored_segments(&writer, "acme/docs");
    assert!(!old.is_empty());

    delete_ns(&writer, "acme", "docs");
    index(&writer, "acme", "docs", &["new"]);
    let new = stored_segments(&writer, "acme/docs");
    assert!(!new.is_empty() && new.is_disjoint(&old));
    // The follower has both incarnations on disk now.
    hydrate(&writer, &follower, "acme/docs");
    let on_disk = dirs(&follower, "acme/docs");
    assert!(on_disk.is_superset(&old) && on_disk.is_superset(&new));

    let report = pass(&follower);
    assert_eq!(report.segment_dirs_removed, old.len() as u64, "{report:?}");
    assert_eq!(dirs(&follower, "acme/docs"), new);
    assert_eq!(search(&follower, "acme", "docs"), (200, vec!["new".into()]));

    // The writer already removed the old files when it ran the delete;
    // its own pass must leave the new incarnation alone as well.
    let report = pass(&writer);
    assert_eq!(report.segment_dirs_removed, 0, "{report:?}");
    assert_eq!(dirs(&writer, "acme/docs"), new);
    assert_eq!(search(&writer, "acme", "docs"), (200, vec!["new".into()]));

    // Deleted again: now everything goes.
    delete_ns(&writer, "acme", "docs");
    pass(&follower);
    assert!(!follower.data_dir.join("acme/docs").exists());
}

/// A follower that still holds the deleted incarnation's manifest in
/// memory (its refresher has not run) does not mistake it for a current
/// one: the retired version on the record says which it is.
#[test]
fn a_stale_manifest_in_memory_does_not_protect_the_deleted_incarnation() {
    let (writer, follower, _) = pods();
    index(&writer, "acme", "docs", &["old"]);
    hydrate(&writer, &follower, "acme/docs");
    assert_eq!(search(&follower, "acme", "docs").1, vec!["old".to_string()]);
    let ns = NamespaceId("acme/docs".into());
    let loaded = follower.indexer.manifest_version(&ns).unwrap();
    delete_ns(&writer, "acme", "docs");
    index(&writer, "acme", "docs", &["new"]);
    hydrate(&writer, &follower, "acme/docs");
    assert_eq!(
        follower.indexer.manifest_version(&ns),
        Some(loaded),
        "still the deleted incarnation's manifest"
    );

    pass(&follower);
    assert_eq!(
        dirs(&follower, "acme/docs"),
        stored_segments(&writer, "acme/docs")
    );
    assert_eq!(search(&follower, "acme", "docs"), (200, vec!["new".into()]));
}

/// On a writer, segments of the new incarnation that were flushed but not
/// published are in no stored manifest. They are the only copy and must
/// survive; the sweep comes back once they are published.
#[test]
fn a_writer_keeps_unpublished_segments_of_the_new_incarnation() {
    let control: SharedControl = Arc::new(Mutex::new(kosha_control::Controller::new()));
    let writer = writer_on(&control);
    index(&writer, "acme", "docs", &["old"]);
    delete_ns(&writer, "acme", "docs");

    let ns = NamespaceId("acme/docs".into());
    writer
        .indexer
        .index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("fresh".into()),
                fields: vec![Field::text("body", "common fresh")],
            }],
        )
        .unwrap();
    writer.indexer.flush_namespace(&ns).unwrap();
    assert!(
        stored_segments(&writer, "acme/docs").is_empty(),
        "unpublished"
    );
    let unpublished = dirs(&writer, "acme/docs");
    assert_eq!(unpublished.len(), 1);
    // A leftover of the deleted incarnation next to it.
    let leftover = writer.data_dir.join("acme/docs/acme_docs-leftover-000000");
    std::fs::create_dir_all(&leftover).unwrap();
    std::fs::write(leftover.join("doc_store.bin"), b"deleted documents").unwrap();

    let report = pass(&writer);
    assert_eq!(report.segment_dirs_removed, 1, "{report:?}");
    assert!(!leftover.exists());
    assert_eq!(dirs(&writer, "acme/docs"), unpublished);
    assert!(report.deferred >= 1, "it comes back: {report:?}");
    assert_eq!(writer.deletion_purge.to_json()["pending_sweeps"], 1);

    writer.publish_namespace(&ns).unwrap();
    let report = pass(&writer);
    assert_eq!((report.sweeps, report.deferred), (1, 0), "{report:?}");
    assert_eq!(dirs(&writer, "acme/docs"), unpublished);
    assert_eq!(search(&writer, "acme", "docs"), (200, vec!["fresh".into()]));
    assert_eq!(writer.deletion_purge.to_json()["pending_sweeps"], 0);

    // The namespace is registered again and the writer is ahead of the
    // store by one flushed segment: only its own manifest lists it.
    writer
        .indexer
        .index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("ahead".into()),
                fields: vec![Field::text("body", "common ahead")],
            }],
        )
        .unwrap();
    writer.indexer.flush_namespace(&ns).unwrap();
    let local = dirs(&writer, "acme/docs");
    assert_eq!(local.len(), 2);
    assert_eq!(stored_segments(&writer, "acme/docs").len(), 1);
    std::fs::create_dir_all(&leftover).unwrap();
    let floor = writer
        .controller
        .lock()
        .unwrap()
        .deletions(&DeletionFilter::default())
        .unwrap()[0]
        .last_version;
    settle_clock();
    let outcome = writer.sweep_deleted_subject(
        DeletionScope::Namespace,
        "acme/docs",
        Some(floor),
        &tuning(),
    );
    assert_eq!(
        (
            outcome.segment_dirs_removed,
            outcome.deferred,
            &outcome.error
        ),
        (1, 0, &None)
    );
    assert_eq!(dirs(&writer, "acme/docs"), local);
    writer.publish_namespace(&ns).unwrap();
    assert_eq!(
        search(&writer, "acme", "docs"),
        (200, vec!["ahead".into(), "fresh".into()])
    );
}

/// A directory written to after the sweep started could belong to a
/// segment published after the control store was read. It waits for the
/// next sweep, as does one a fetch has pinned.
#[test]
fn recently_written_and_pinned_directories_wait_for_the_next_sweep() {
    let (writer, follower, _) = pods();
    index(&writer, "acme", "docs", &["a"]);
    hydrate(&writer, &follower, "acme/docs");
    let segment = dirs(&follower, "acme/docs").into_iter().next().unwrap();
    delete_ns(&writer, "acme", "docs");

    let cautious = PurgeTuning {
        write_margin: Duration::from_secs(3600),
        ..tuning()
    };
    let report = follower.run_deletion_purge_pass(&cautious);
    assert_eq!(
        (report.segment_dirs_removed, report.deferred),
        (0, 1),
        "{report:?}"
    );
    assert!(follower.data_dir.join("acme/docs").join(&segment).exists());

    let pinned = format!("acme/docs/{segment}/footer.json");
    follower.cache.pin(&pinned);
    let report = pass(&follower);
    assert_eq!(
        (report.segment_dirs_removed, report.deferred),
        (0, 1),
        "{report:?}"
    );
    follower.cache.unpin(&pinned);

    let report = pass(&follower);
    assert_eq!(
        (report.records, report.segment_dirs_removed),
        (0, 1),
        "{report:?}"
    );
    assert!(!follower.data_dir.join("acme/docs").exists());
    assert_eq!(pass(&follower).sweeps, 0);
}

/// A tenant deletion: everything under the tenant's directory goes on a
/// follower, branches included, and a tenant whose id merely starts the
/// same way keeps its files.
#[test]
fn a_deleted_tenant_leaves_a_follower_and_its_neighbour_stays() {
    let (writer, follower, _) = pods();
    index(&writer, "acme", "a", &["a1"]);
    index(&writer, "acme", "b", &["b1"]);
    index(&writer, "acme-eu", "a", &["e1"]);
    hydrate(&writer, &follower, "acme");
    hydrate(&writer, &follower, "acme-eu");
    // Something no namespace record covers.
    let stray = follower.data_dir.join("acme/never-registered/seg-0");
    std::fs::create_dir_all(&stray).unwrap();
    std::fs::write(stray.join("doc_store.bin"), b"stray").unwrap();
    assert_eq!(search(&follower, "acme", "a").1, vec!["a1".to_string()]);

    let no_keys = auth::Auth::Keys(auth::KeyStore::with_keys(HashMap::new()));
    let response = namespace_deletion::handle_delete_tenant(
        "acme",
        br#"{"confirm":"acme"}"#,
        &writer,
        &no_keys,
    );
    let (status, body) = parse(&response);
    assert_eq!(status, 200, "{body}");

    let report = pass(&follower);
    assert_eq!(
        report.records, 3,
        "the tenant and its two namespaces: {report:?}"
    );
    assert!(report.errors.is_empty(), "{report:?}");
    assert!(!follower.data_dir.join("acme").exists());
    assert_eq!(search(&follower, "acme", "a").0, 404);
    assert_eq!(search(&follower, "acme-eu", "a"), (200, vec!["e1".into()]));
}

/// A pod that was down during the deletes has no memory of them. Its
/// first pass reads every record, a page at a time, and removes what its
/// disk still holds.
#[test]
fn a_pod_started_after_the_deletes_catches_up_on_its_first_pass() {
    let (writer, follower, control) = pods();
    let names = ["n0", "n1", "n2", "n3", "n4"];
    for name in names {
        index(&writer, "acme", name, &["d"]);
    }
    index(&writer, "acme", "kept", &["k"]);
    hydrate(&writer, &follower, "acme");
    let disk = follower.data_dir.clone();
    drop(follower);
    for name in names {
        delete_ns(&writer, "acme", name);
    }

    let restarted = follower_on(&control, disk);
    assert!(!dirs(&restarted, "acme/n3").is_empty());
    let report = pass(&restarted);
    assert_eq!(report.records, names.len() as u64, "{report:?}");
    assert!(report.errors.is_empty(), "{report:?}");
    for name in names {
        assert!(
            !restarted.data_dir.join("acme").join(name).exists(),
            "{name}"
        );
    }
    assert_eq!(restarted.deletion_purge.to_json()["caught_up"], true);
    assert_eq!(search(&restarted, "acme", "kept"), (200, vec!["k".into()]));
}

/// A control store that answers nothing.
struct Unreachable;

impl ControlStore for Unreachable {
    fn create_namespace(&mut self, _: NamespaceId) -> Result<(), KoshaError> {
        Err(KoshaError::ControlPlane("unreachable".into()))
    }
    fn ensure_namespace(&mut self, _: NamespaceId) {}
    fn has_namespace(&self, _: &NamespaceId) -> bool {
        false
    }
    fn manifest(&self, _: &NamespaceId) -> Option<&kosha_core::Manifest> {
        None
    }
    fn manifest_mut(&mut self, _: &NamespaceId) -> Option<&mut kosha_core::Manifest> {
        None
    }
    fn save_manifest(
        &mut self,
        _: &NamespaceId,
        _: &kosha_core::Manifest,
    ) -> Result<(), KoshaError> {
        Err(KoshaError::ControlPlane("unreachable".into()))
    }
    fn compare_and_swap_manifest(
        &mut self,
        _: &NamespaceId,
        _: u64,
        _: kosha_core::Manifest,
    ) -> Result<(), KoshaError> {
        Err(KoshaError::ControlPlane("unreachable".into()))
    }
    fn list_namespaces(&self) -> Vec<NamespaceId> {
        Vec::new()
    }
    fn namespace_count(&self) -> usize {
        0
    }
}

/// A control store that cannot be read loses nothing: the pass reports
/// it, removes nothing, and the next one handles the record.
#[test]
fn a_control_store_outage_is_reported_and_the_record_is_handled_later() {
    let (writer, follower, control) = pods();
    index(&writer, "acme", "docs", &["a"]);
    hydrate(&writer, &follower, "acme/docs");
    delete_ns(&writer, "acme", "docs");

    *follower.controller.lock().unwrap() = Box::new(Unreachable);
    let report = pass(&follower);
    assert_eq!(report.records, 0);
    assert_eq!(report.errors.len(), 1, "{report:?}");
    assert!(!dirs(&follower, "acme/docs").is_empty());
    let stats = follower.deletion_purge.to_json();
    assert_eq!(stats["caught_up"], false, "{stats}");
    assert!(stats["last_error"].is_string(), "{stats}");
    assert_eq!(stats["kosha_deletion_purge_errors_total"], 1, "{stats}");

    *follower.controller.lock().unwrap() = Box::new(SharedManifestStore::new(control));
    let report = pass(&follower);
    assert_eq!(report.records, 1, "{report:?}");
    assert!(!follower.data_dir.join("acme/docs").exists());
    assert!(follower.deletion_purge.to_json()["last_error"].is_null());
}

/// Object storage that refuses deletes until told otherwise.
#[derive(Default)]
struct FlakyRemote {
    objects: Mutex<BTreeSet<String>>,
    failing: AtomicBool,
}

impl namespace_deletion::RemotePrefixStore for FlakyRemote {
    fn delete_prefix(&self, prefix: &str) -> Result<usize, KoshaError> {
        if self.failing.load(Ordering::Relaxed) {
            return Err(KoshaError::ControlPlane(
                "S3 delete_objects: throttled".into(),
            ));
        }
        let below = format!("{prefix}/");
        let mut objects = self.objects.lock().unwrap();
        let before = objects.len();
        objects.retain(|key| !key.starts_with(&below));
        Ok(before - objects.len())
    }

    fn count_prefix(&self, prefix: &str) -> Result<usize, KoshaError> {
        let below = format!("{prefix}/");
        let objects = self.objects.lock().unwrap();
        Ok(objects.iter().filter(|key| key.starts_with(&below)).count())
    }
}

/// The automatic retry: a pending namespace record completes on a later
/// pass without an admin call, a record that keeps failing is not retried
/// on every tick, and a tenant record is never retried here.
#[test]
fn pending_namespace_deletions_are_retried_with_a_growing_pause() {
    let mut writer = AppState::new(test_dir());
    let remote = Arc::new(FlakyRemote::default());
    writer.remote_prefix_override = Some(remote.clone());
    index(&writer, "acme", "docs", &["a"]);
    remote
        .objects
        .lock()
        .unwrap()
        .insert("acme/docs/seg/footer.json".into());
    remote.failing.store(true, Ordering::Relaxed);
    let (status, body) = call(
        &writer,
        "acme",
        "DELETE /v1/namespaces/docs",
        serde_json::json!({}),
    );
    assert_eq!(status, 200, "{body}");
    let id = body["deletion"]["id"].as_str().unwrap().to_string();
    assert_eq!(body["deletion"]["state"], "pending");
    let tenant_record = writer
        .controller
        .lock()
        .unwrap()
        .begin_deletion(&NewDeletion::tenant("globex"), 0)
        .unwrap();
    let record = |id: &str| {
        writer
            .controller
            .lock()
            .unwrap()
            .deletion(id)
            .unwrap()
            .unwrap()
    };

    let interval = Duration::from_secs(3600);
    assert_eq!(writer.run_deletion_resume_pass(interval), 0);
    assert_eq!(
        record(&id).attempts,
        2,
        "the delete's own attempt plus one retry"
    );
    // Storage works again, but the record waits out its pause: a pass
    // straight after does not touch it.
    remote.failing.store(false, Ordering::Relaxed);
    assert_eq!(writer.run_deletion_resume_pass(interval), 0);
    assert_eq!(record(&id).attempts, 2);

    // Once the pause is over it is retried, and completes.
    writer
        .deletion_purge
        .progress
        .lock()
        .unwrap()
        .resume_backoff
        .clear();
    assert_eq!(writer.run_deletion_resume_pass(interval), 1);
    assert!(record(&id).is_completed());
    assert!(remote.objects.lock().unwrap().is_empty());
    let untouched = record(&tenant_record.id);
    assert_eq!((untouched.attempts, untouched.is_completed()), (0, false));
    let stats = writer.deletion_purge.to_json();
    assert_eq!(stats["kosha_deletion_resume_attempts_total"], 2, "{stats}");
    assert_eq!(stats["kosha_deletion_resume_completed_total"], 1, "{stats}");
}
