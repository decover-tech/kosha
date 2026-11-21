//! #254 acceptance tests: writer leases and fencing between two writers in
//! one process, and an `indexer` compacting next to an `ingest` writer, all
//! over one shared in-memory control store.

use super::*;

/// The in-memory controller shared by several `AppState`s, forwarding the
/// writer-lease methods too. `takeover_before_next_cas` hands the lease to
/// another holder right before the next fenced CAS: a writer that paused
/// between taking its token and committing.
#[derive(Clone)]
struct LeasedStore {
    inner: Arc<Mutex<kosha_control::Controller>>,
    takeover_before_next_cas: Arc<Mutex<Option<(String, String)>>>,
}

impl LeasedStore {
    fn new(inner: Arc<Mutex<kosha_control::Controller>>) -> Self {
        Self {
            inner,
            takeover_before_next_cas: Default::default(),
        }
    }
}

impl ControlStore for LeasedStore {
    fn create_namespace(&mut self, id: NamespaceId) -> Result<(), KoshaError> {
        self.inner.lock().unwrap().create_namespace(id)
    }
    fn ensure_namespace(&mut self, id: NamespaceId) {
        self.inner.lock().unwrap().ensure_namespace(id);
    }
    fn has_namespace(&self, id: &NamespaceId) -> bool {
        self.inner.lock().unwrap().has_namespace(id)
    }
    fn manifest(&self, _: &NamespaceId) -> Option<&kosha_core::Manifest> {
        None
    }
    fn manifest_mut(&mut self, _: &NamespaceId) -> Option<&mut kosha_core::Manifest> {
        None
    }
    fn manifest_cloned(&self, id: &NamespaceId) -> Option<kosha_core::Manifest> {
        self.inner.lock().unwrap().manifest(id).cloned()
    }
    fn manifest_version(&self, id: &NamespaceId) -> Result<Option<u64>, KoshaError> {
        Ok(self.inner.lock().unwrap().manifest(id).map(|m| m.version))
    }
    fn save_manifest(
        &mut self,
        id: &NamespaceId,
        manifest: &kosha_core::Manifest,
    ) -> Result<(), KoshaError> {
        self.inner.lock().unwrap().save_manifest(id, manifest)
    }
    fn compare_and_swap_manifest(
        &mut self,
        id: &NamespaceId,
        expected: u64,
        manifest: kosha_core::Manifest,
    ) -> Result<(), KoshaError> {
        self.inner
            .lock()
            .unwrap()
            .compare_and_swap_manifest(id, expected, manifest)
    }
    fn list_namespaces(&self) -> Vec<NamespaceId> {
        self.inner.lock().unwrap().list_namespaces().to_vec()
    }
    fn namespace_count(&self) -> usize {
        self.inner.lock().unwrap().namespace_count()
    }
    fn delete_namespace(
        &mut self,
        id: &NamespaceId,
        retire_at_least: u64,
    ) -> Result<(kosha_core::Manifest, u64), KoshaError> {
        self.inner
            .lock()
            .unwrap()
            .delete_namespace(id, retire_at_least)
    }
    fn remove_namespace(
        &mut self,
        id: &NamespaceId,
    ) -> Result<Option<kosha_core::Manifest>, KoshaError> {
        Ok(self.inner.lock().unwrap().remove_namespace(id))
    }
    fn acquire_writer_lease(
        &mut self,
        id: &NamespaceId,
        holder: &str,
        ttl: Duration,
    ) -> Result<Option<u64>, KoshaError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .acquire_writer_lease(id, holder, ttl))
    }
    fn renew_writer_lease(
        &mut self,
        id: &NamespaceId,
        holder: &str,
        token: u64,
        ttl: Duration,
    ) -> Result<bool, KoshaError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .renew_writer_lease(id, holder, token, ttl))
    }
    fn release_writer_lease(
        &mut self,
        id: &NamespaceId,
        holder: &str,
        token: u64,
    ) -> Result<(), KoshaError> {
        self.inner
            .lock()
            .unwrap()
            .release_writer_lease(id, holder, token);
        Ok(())
    }
    fn compare_and_swap_manifest_fenced(
        &mut self,
        id: &NamespaceId,
        expected: u64,
        manifest: kosha_core::Manifest,
        token: Option<u64>,
    ) -> Result<(), KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        if let Some((old, new)) = self.takeover_before_next_cas.lock().unwrap().take() {
            let current = inner.writer_lease_token(id).unwrap();
            inner.release_writer_lease(id, &old, current);
            inner
                .acquire_writer_lease(id, &new, Duration::from_secs(60))
                .unwrap();
        }
        inner.compare_and_swap_manifest_fenced(id, expected, manifest, token)
    }
}

fn state(
    shared: &Arc<Mutex<kosha_control::Controller>>,
    role: role::Role,
    lease: bool,
) -> (AppState, LeasedStore) {
    // The clock alone collides between tests running in parallel (macOS
    // reports microseconds), and a test deleting its dir would take another
    // test's WAL with it.
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let store = LeasedStore::new(shared.clone());
    let dir = std::env::temp_dir().join(format!(
        "kosha-lease-test-{}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let state = AppState::with_options(
        dir,
        StateOptions {
            control_store: Some(Box::new(store.clone())),
            role: Some(role),
            writer_lease_ttl: Some(Duration::from_secs(60)),
            writer_lease: Some(lease),
            ..Default::default()
        },
    );
    (state, store)
}

fn call(state: &AppState, line: &str, body: serde_json::Value) -> String {
    let bytes = if body.is_null() {
        Vec::new()
    } else {
        serde_json::to_vec(&body).unwrap()
    };
    route(
        &format!("{line} HTTP/1.1\r\n"),
        &HashMap::new(),
        &bytes,
        "acme",
        state,
    )
}

fn status(response: &str) -> u16 {
    response
        .get(9..12)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0)
}

fn write(state: &AppState, ids: &[&str]) -> String {
    let documents: Vec<serde_json::Value> = ids
        .iter()
        .map(|id| {
            serde_json::json!({"id": id, "fields": [
                {"name": "title", "field_type": "Text", "value": format!("doc {id}")}
            ]})
        })
        .collect();
    call(
        state,
        "POST /v1/namespaces/docs/documents",
        serde_json::json!({ "documents": documents }),
    )
}

fn flush(state: &AppState) -> String {
    call(
        state,
        "POST /v1/namespaces/docs/flush",
        serde_json::Value::Null,
    )
}

fn ns() -> NamespaceId {
    NamespaceId("acme/docs".into())
}

fn committed(shared: &Arc<Mutex<kosha_control::Controller>>) -> kosha_core::Manifest {
    shared.lock().unwrap().manifest(&ns()).cloned().unwrap()
}

fn committed_docs(shared: &Arc<Mutex<kosha_control::Controller>>) -> u32 {
    committed(shared).segments.iter().map(|e| e.doc_count).sum()
}

/// Acceptance: two writers on one namespace. The first write takes the
/// lease; the second writer is refused (503 + Retry-After) and keeps being
/// refused, while the first commits.
#[test]
fn second_writer_is_refused_while_the_first_holds_the_lease() {
    let shared = Arc::new(Mutex::new(kosha_control::Controller::new()));
    let (a, _) = state(&shared, role::Role::Standalone, true);
    let (b, _) = state(&shared, role::Role::Ingest, true);

    assert_eq!(status(&write(&a, &["a1"])), 200);
    let refused = write(&b, &["b1"]);
    assert_eq!(status(&refused), 503, "{refused}");
    assert!(refused.contains("retry-after: 60"), "{refused}");
    assert!(refused.contains("writer fenced"), "{refused}");
    assert_eq!(status(&flush(&a)), 200);
    assert_eq!(status(&write(&b, &["b2"])), 503);
    // Nothing of B's reached the namespace, and B buffered nothing.
    assert_eq!(b.indexer.pending_doc_count(&ns()), 0);
    assert_eq!(committed_docs(&shared), 1);
    assert_eq!(status(&write(&a, &["a2"])), 200);
    assert_eq!(status(&flush(&a)), 200);
    assert_eq!(committed_docs(&shared), 2);
}

/// Acceptance: both writers get as far as a commit. A takes its token,
/// pauses, and B takes the lease over before A's CAS. Exactly one commits:
/// A's CAS is fenced (not rebased), A drops its unpublished state and
/// accepts no further writes for the namespace, and B's writes commit.
#[test]
fn a_writer_fenced_at_commit_stops_writing_and_drops_unpublished_state() {
    let shared = Arc::new(Mutex::new(kosha_control::Controller::new()));
    let (a, a_store) = state(&shared, role::Role::Ingest, true);
    let (b, _) = state(&shared, role::Role::Standalone, true);

    assert_eq!(status(&write(&a, &["a1"])), 200);
    assert_eq!(status(&flush(&a)), 200);
    let before = committed(&shared);
    assert_eq!(status(&write(&a, &["a2"])), 200);
    *a_store.takeover_before_next_cas.lock().unwrap() = Some((
        a.writer_leases.holder().to_owned(),
        b.writer_leases.holder().to_owned(),
    ));
    let fenced = flush(&a);
    assert_eq!(status(&fenced), 503, "{fenced}");
    assert!(fenced.contains("retry-after:"), "{fenced}");
    assert!(a.writer_leases.is_fenced(&ns()));
    assert_eq!(a.metrics.writer_fenced_count(), 1);
    assert_eq!(b.metrics.writer_fenced_count(), 0);
    // Nothing changed in the control store; A rolled back to it.
    assert!(committed(&shared).same_contents(&before));
    assert_eq!(a.indexer.pending_doc_count(&ns()), 0);
    assert_eq!(
        a.indexer.manifest_version(&ns()),
        Some(before.version),
        "unpublished local state dropped"
    );
    // A accepts no further writes for the namespace...
    assert_eq!(status(&write(&a, &["a3"])), 503);
    assert_eq!(a.indexer.pending_doc_count(&ns()), 0);
    // ...and B, the new owner, commits.
    assert_eq!(status(&write(&b, &["b1"])), 200);
    assert_eq!(status(&flush(&b)), 200);
    assert_eq!(committed_docs(&shared), 2);
    assert_eq!(status(&write(&a, &["a4"])), 503);
    assert_eq!(status(&flush(&a)), 503);
    assert_eq!(shared.lock().unwrap().writer_lease_token(&ns()), Some(2));
    // One takeover is one fence, however many writes it refuses.
    assert_eq!(a.metrics.writer_fenced_count(), 1);
    assert_eq!(b.metrics.writer_fenced_count(), 0);
}

/// A renewal that finds the lease taken over fences the namespace too.
#[test]
fn a_lost_renewal_fences_the_namespace() {
    let shared = Arc::new(Mutex::new(kosha_control::Controller::new()));
    let (a, _) = state(&shared, role::Role::Ingest, true);
    assert_eq!(status(&write(&a, &["a1"])), 200);
    {
        let mut inner = shared.lock().unwrap();
        inner.release_writer_lease(&ns(), a.writer_leases.holder(), 1);
        assert_eq!(
            inner.acquire_writer_lease(&ns(), "someone-else", Duration::from_secs(60)),
            Some(2)
        );
    }
    assert_eq!(a.metrics.writer_fenced_count(), 0);
    a.renew_writer_leases();
    assert!(a.writer_leases.is_fenced(&ns()));
    assert_eq!(a.metrics.writer_fenced_count(), 1);
    assert_eq!(a.indexer.pending_doc_count(&ns()), 0);
    assert_eq!(status(&write(&a, &["a2"])), 503);
    a.renew_writer_leases();
    assert_eq!(a.metrics.writer_fenced_count(), 1);
}

/// `KOSHA_WRITER_LEASE=off`: two writers behave exactly as before #254,
/// rebasing onto each other's commits.
#[test]
fn leases_off_keeps_the_rebasing_multi_writer_behaviour() {
    let shared = Arc::new(Mutex::new(kosha_control::Controller::new()));
    let (a, _) = state(&shared, role::Role::Standalone, false);
    let (b, _) = state(&shared, role::Role::Standalone, false);
    assert_eq!(status(&write(&a, &["a1"])), 200);
    assert_eq!(status(&flush(&a)), 200);
    assert_eq!(status(&write(&b, &["b1"])), 200);
    assert_eq!(status(&flush(&b)), 200);
    assert_eq!(committed_docs(&shared), 2);
    assert_eq!(shared.lock().unwrap().writer_lease_token(&ns()), None);
}

/// Object storage stand-in for the indexer: its segments come from the
/// ingest writer's data dir. `on_hydrate` runs once, between the indexer's
/// plan and its merge (an ingest flush racing the compaction).
struct IngestDirStorage<'a> {
    remote: PathBuf,
    local: PathBuf,
    on_hydrate: Mutex<Option<Box<dyn FnOnce() + 'a>>>,
}

impl CompactionStorage for IngestDirStorage<'_> {
    fn segment_sizes(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
    ) -> HashMap<kosha_core::SegmentId, u64> {
        manifest
            .segments
            .iter()
            .filter_map(|e| {
                let rel = std::path::Path::new(&ns.0).join(&e.segment_id.0);
                Some((
                    e.segment_id.clone(),
                    local_dir_bytes(&self.remote.join(rel))?,
                ))
            })
            .collect()
    }
    fn hydrate(&self, seg_paths: &[PathBuf]) {
        fn copy(from: &std::path::Path, to: &std::path::Path) {
            std::fs::create_dir_all(to).unwrap();
            for entry in std::fs::read_dir(from).unwrap().flatten() {
                if entry.path().is_dir() {
                    copy(&entry.path(), &to.join(entry.file_name()));
                } else {
                    std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
                }
            }
        }
        for path in seg_paths {
            let rel = path.strip_prefix(&self.local).unwrap();
            if !path.exists() {
                copy(&self.remote.join(rel), path);
            }
        }
        if let Some(hook) = self.on_hydrate.lock().unwrap().take() {
            hook();
        }
    }
    fn evicts_superseded(&self) -> bool {
        false
    }
}

/// Acceptance: an indexer compaction concurrent with ingest flushes loses
/// no acknowledged document and keeps the ingest writer's WAL checkpoint
/// and origin. The indexer publishes with plain CAS + rebase (no lease,
/// no WAL); the ingest writer rebases onto the compaction.
#[test]
fn indexer_compaction_racing_ingest_flushes_loses_nothing_and_keeps_the_wal_checkpoint() {
    let shared = Arc::new(Mutex::new(kosha_control::Controller::new()));
    let (ingest, _) = state(&shared, role::Role::Ingest, true);
    for batch in [["a", "b"], ["c", "d"], ["e", "f"], ["g", "h"]] {
        assert_eq!(status(&write(&ingest, &batch)), 200);
        assert_eq!(status(&flush(&ingest)), 200);
    }
    let origin = committed(&shared).wal_origin.clone();
    assert!(origin.is_some());

    let (indexer, _) = state(&shared, role::Role::Indexer, true);
    assert!(
        !indexer.writer_leases.enabled(),
        "an indexer takes no lease"
    );
    // The indexer serves only the compaction admin API.
    assert!(!role::Role::indexer_serves(
        "POST /v1/namespaces/docs/documents"
    ));

    let storage = IngestDirStorage {
        remote: ingest.data_dir.clone(),
        local: indexer.data_dir.clone(),
        on_hydrate: Mutex::new(Some(Box::new(|| {
            assert_eq!(status(&write(&ingest, &["i", "j"])), 200);
            assert_eq!(status(&flush(&ingest)), 200);
        }))),
    };
    let opts = compact_options(&indexer, kosha_write::CompactMode::Full, None);
    let outcome = compaction_pass_with(&indexer, &storage, &ns(), opts, |_| {}).unwrap();
    assert!(outcome.merged, "{outcome:?}");

    // The compaction committed over the racing flush: its inputs are gone,
    // the flushed segment is kept, and the WAL fields are the ingest's.
    let after_compaction = committed(&shared);
    assert_eq!(
        after_compaction.segments.len(),
        2,
        "{:?}",
        after_compaction.segments
    );
    assert_eq!(after_compaction.wal_origin, origin);
    assert_eq!(
        after_compaction.wal_checkpoint,
        ingest.indexer.manifest(&ns()).unwrap().wal_checkpoint
    );
    assert_eq!(committed_docs(&shared), 10);

    // The ingest writer's next flush rebases onto the compaction.
    assert_eq!(status(&write(&ingest, &["k"])), 200);
    assert_eq!(status(&flush(&ingest)), 200);
    let last = committed(&shared);
    assert_eq!(committed_docs(&shared), 11);
    assert_eq!(last.wal_origin, origin);
    assert_eq!(
        last.wal_checkpoint,
        ingest.indexer.manifest(&ns()).unwrap().wal_checkpoint
    );
    assert!(last
        .segments
        .iter()
        .any(|e| e.segment_id.0.contains("-compact-")));
}

/// Acceptance: an indexer killed mid-compaction (merged locally, dropped
/// before publishing) leaves the published manifest intact and loses no
/// write.
#[test]
fn indexer_dropped_before_publishing_leaves_the_manifest_intact() {
    let shared = Arc::new(Mutex::new(kosha_control::Controller::new()));
    let (ingest, _) = state(&shared, role::Role::Ingest, true);
    for batch in [["a", "b"], ["c", "d"], ["e", "f"]] {
        assert_eq!(status(&write(&ingest, &batch)), 200);
        assert_eq!(status(&flush(&ingest)), 200);
    }
    let before = committed(&shared);
    {
        let (indexer, _) = state(&shared, role::Role::Indexer, true);
        let storage = IngestDirStorage {
            remote: ingest.data_dir.clone(),
            local: indexer.data_dir.clone(),
            on_hydrate: Mutex::new(None),
        };
        let paths: Vec<PathBuf> = before
            .segments
            .iter()
            .map(|e| before.segment_dir(&indexer.data_dir, &ns(), &e.segment_id))
            .collect();
        storage.hydrate(&paths);
        let result = indexer
            .indexer
            .compact_namespace_with_options(&ns(), kosha_write::CompactOptions::full())
            .unwrap();
        assert!(result.merged);
        // Killed here: dropped without publishing.
    }
    assert!(committed(&shared).same_contents(&before));
    assert_eq!(status(&write(&ingest, &["g"])), 200);
    assert_eq!(status(&flush(&ingest)), 200);
    assert_eq!(committed_docs(&shared), 7);
    assert_eq!(committed(&shared).wal_origin, before.wal_origin);
}

/// Acceptance: with `KOSHA_COMPACTION_ON_INGEST=false` the ingest pod runs
/// no compaction scheduler; standalone and indexer do.
#[test]
fn compaction_scheduler_placement_follows_the_role() {
    let shared = Arc::new(Mutex::new(kosha_control::Controller::new()));
    let (mut ingest, _) = state(&shared, role::Role::Ingest, true);
    assert!(
        ingest.runs_compaction_scheduler(),
        "default: today's behaviour"
    );
    ingest.compaction_on_ingest = false;
    assert!(!ingest.runs_compaction_scheduler());
    let status_body = handle_compaction_status(&ingest);
    assert!(status_body.contains("\"enabled\":false"), "{status_body}");
    let (indexer, _) = state(&shared, role::Role::Indexer, true);
    assert!(indexer.runs_compaction_scheduler());
}

/// The indexer answers 404 to everything but probes and the compaction
/// admin API, over a real socket.
#[test]
fn indexer_serves_only_probes_and_compaction_admin() {
    use std::io::Write;
    use std::net::TcpStream;
    let shared = Arc::new(Mutex::new(kosha_control::Controller::new()));
    let (indexer, _) = state(&shared, role::Role::Indexer, true);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || serve(listener, Arc::new(indexer), Duration::from_secs(30)));
    let get = |method: &str, path: &str| -> u16 {
        let mut s = TcpStream::connect(addr).unwrap();
        s.write_all(
            format!("{method} {path} HTTP/1.1\r\nhost: i\r\ncontent-length: 2\r\n\r\n{{}}")
                .as_bytes(),
        )
        .unwrap();
        let resp = crate::http::read_response_string(&mut s).unwrap();
        status(&resp)
    };
    assert_eq!(get("GET", "/healthz"), 200);
    assert_eq!(get("GET", "/readyz"), 200);
    // Routed to the admin handler, which wants the admin key (403), not
    // refused by role (404).
    assert_eq!(get("GET", "/v1/admin/compaction-status"), 403);
    assert_eq!(get("POST", "/v1/admin/compact-namespace"), 403);
    assert_eq!(get("POST", "/v1/namespaces/docs/documents"), 404);
    assert_eq!(get("POST", "/v1/namespaces/docs/search"), 404);
    assert_eq!(get("POST", "/index"), 404);
    assert_eq!(get("GET", "/stats"), 404);
}
