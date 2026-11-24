//! #348 tests: usage events from the write and search handlers, their
//! delivery through the spool, the hourly stored-bytes snapshot and the
//! read API, over an in-memory usage store.

use super::*;
use kosha_control::usage::{usage_summary, MemoryUsageStore, UsageEvent, UsageStore, HOUR_MS};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use usage::{FlushReport, UsageConfig, UsageMeter};

/// A [`MemoryUsageStore`] that can be made unreachable, or made to lose the
/// reply of a `record` that did commit.
#[derive(Default)]
struct FlakyStore {
    inner: MemoryUsageStore,
    down: AtomicBool,
    lose_next_reply: AtomicBool,
    reads_down: AtomicBool,
    record_calls: AtomicU64,
}

impl UsageStore for FlakyStore {
    fn record(&self, events: &[UsageEvent]) -> Result<u64, KoshaError> {
        self.record_calls.fetch_add(1, Ordering::SeqCst);
        if self.down.load(Ordering::SeqCst) {
            return Err(KoshaError::ControlPlane("injected: store down".into()));
        }
        let stored = self.inner.record(events)?;
        if self.lose_next_reply.swap(false, Ordering::SeqCst) {
            return Err(KoshaError::ControlPlane("injected: lost reply".into()));
        }
        Ok(stored)
    }
    fn hourly(
        &self,
        tenant: Option<&str>,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<kosha_control::usage::UsageBucket>, KoshaError> {
        if self.reads_down.load(Ordering::SeqCst) {
            return Err(KoshaError::ControlPlane(
                "injected: relation kosha.usage_events".into(),
            ));
        }
        self.inner.hourly(tenant, from_ms, to_ms)
    }
    fn latest_between(
        &self,
        tenant: Option<&str>,
        meter: &str,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<kosha_control::usage::UsageGauge>, KoshaError> {
        self.inner.latest_between(tenant, meter, from_ms, to_ms)
    }
}

fn test_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "kosha-test-usage-{}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
        unix_millis()
    ))
}

fn meter_in(dir: &std::path::Path, store: Arc<dyn UsageStore>) -> UsageMeter {
    UsageMeter::new(store, UsageConfig::under(dir))
}

/// A standalone server with metering on, and its usage store.
fn metered_state() -> (AppState, Arc<FlakyStore>) {
    let dir = test_dir();
    let store = Arc::new(FlakyStore::default());
    let state = AppState::with_options(
        dir.clone(),
        StateOptions {
            usage: Some(meter_in(&dir, store.clone())),
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

fn body_text(response: &str) -> &str {
    response.split_once("\r\n\r\n").expect("no body").1
}

fn body_of(response: &str) -> serde_json::Value {
    serde_json::from_str(body_text(response)).unwrap_or(serde_json::Value::Null)
}

fn call(state: &AppState, tenant: &str, line: &str, body: serde_json::Value) -> String {
    let bytes = if body.is_null() {
        Vec::new()
    } else {
        serde_json::to_vec(&body).unwrap()
    };
    route(
        &format!("{line} HTTP/1.1\r\n"),
        &HashMap::new(),
        &bytes,
        tenant,
        state,
    )
}

fn ok(response: String) -> String {
    assert_eq!(status_of(&response), 200, "{response}");
    response
}

fn doc(id: &str, text: &str) -> serde_json::Value {
    serde_json::json!({"id": id, "fields": [{"name": "title", "field_type": "Text", "value": text}]})
}

/// The usage the store holds for `tenant` over all time.
fn stored_usage(store: &FlakyStore, tenant: &str) -> Vec<kosha_control::usage::NamespaceUsage> {
    usage_summary(store, Some(tenant), 0, u64::MAX).unwrap()
}

fn flush(state: &AppState) -> FlushReport {
    state.usage.as_ref().unwrap().flush().unwrap()
}

#[test]
fn writes_and_searches_are_metered_per_tenant_and_namespace() {
    let (state, store) = metered_state();
    let docs = serde_json::json!({"documents": [doc("d1", "quick fox"), doc("d22", "lazy dog")]});
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/documents",
        docs,
    ));
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/flush",
        serde_json::json!({}),
    ));
    ok(call(
        &state,
        "globex",
        "POST /v1/namespaces/docs/documents",
        serde_json::json!({"documents": [doc("x", "fox")]}),
    ));
    let search = |tenant: &str, body: serde_json::Value| {
        call(&state, tenant, "POST /v1/namespaces/docs/search", body)
    };
    let first = ok(search("acme", serde_json::json!({"query_text": "fox"})));
    // The same request again is answered by the result cache.
    let cached = ok(search("acme", serde_json::json!({"query_text": "fox"})));
    assert_eq!(body_text(&first), body_text(&cached));
    let uncached = ok(search(
        "acme",
        serde_json::json!({"query_text": "dog", "no_cache": true}),
    ));
    // Searches that do not answer are not metered: an unknown namespace
    // and a malformed body.
    assert_eq!(
        status_of(&call(
            &state,
            "acme",
            "POST /v1/namespaces/missing/search",
            serde_json::json!({"query_text": "fox"}),
        )),
        404
    );
    assert_eq!(
        status_of(&search("acme", serde_json::json!({"max_results": "many"}))),
        400
    );

    flush(&state);
    let acme = stored_usage(&store, "acme");
    assert_eq!(acme.len(), 1, "{acme:?}");
    let usage = &acme[0];
    assert_eq!(usage.namespace, "docs");
    // id + field name + field value, per document.
    let logical = ("d1".len() + "title".len() + "quick fox".len())
        + ("d22".len() + "title".len() + "lazy dog".len());
    assert_eq!(usage.bytes_written, logical as u64);
    assert_eq!(usage.documents_written, 2);
    assert_eq!(usage.queries, 3);
    assert_eq!(
        usage.bytes_returned,
        (body_text(&first).len() + body_text(&cached).len() + body_text(&uncached).len()) as u64
    );
    // Nothing is in object storage in this test, so nothing was fetched.
    assert_eq!(usage.bytes_fetched, 0);

    let globex = stored_usage(&store, "globex");
    assert_eq!(globex.len(), 1);
    assert_eq!(
        (globex[0].documents_written, globex[0].queries),
        (1, 0),
        "tenants are metered apart"
    );
}

#[test]
fn legacy_routes_and_replace_are_metered_too() {
    let _legacy = auth::override_legacy_policy(true, "");
    let (state, store) = metered_state();
    ok(call(
        &state,
        "acme",
        "POST /index",
        serde_json::json!({"namespace": "docs", "documents": [doc("d1", "quick fox")]}),
    ));
    ok(call(
        &state,
        "acme",
        "POST /replace",
        serde_json::json!({"namespace": "docs", "documents": [doc("d1", "slow fox")]}),
    ));
    ok(call(
        &state,
        "acme",
        "POST /flush",
        serde_json::json!({"namespace": "docs"}),
    ));
    ok(call(
        &state,
        "acme",
        "POST /search",
        serde_json::json!({"namespace": "docs", "query_text": "fox"}),
    ));
    ok(call(
        &state,
        "acme",
        "GET /search?ns=docs&q=fox",
        serde_json::Value::Null,
    ));
    flush(&state);
    let usage = &stored_usage(&store, "acme")[0];
    assert_eq!(usage.namespace, "docs");
    assert_eq!(usage.documents_written, 2);
    assert_eq!(
        usage.bytes_written,
        (2 * ("d1".len() + "title".len()) + "quick fox".len() + "slow fox".len()) as u64
    );
    assert_eq!(usage.queries, 2);
}

/// A rejected write is not metered.
#[test]
fn a_refused_write_emits_nothing() {
    let (state, store) = metered_state();
    let refused = call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/documents",
        serde_json::json!({"documents": [
            {"id": "d1", "fields": [{"name": "at", "field_type": "GeoPoint", "value": "nowhere"}]}
        ]}),
    );
    assert_eq!(status_of(&refused), 400, "{refused}");
    assert_eq!(flush(&state), FlushReport::default());
    assert!(stored_usage(&store, "acme").is_empty());
}

/// The store committed a batch but the pod never saw the reply (or crashed
/// before deleting the spool file): the batch is sent again and counts once.
#[test]
fn a_batch_resent_after_a_lost_reply_counts_once() {
    let (state, store) = metered_state();
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/documents",
        serde_json::json!({"documents": [doc("d1", "quick fox")]}),
    ));
    let meter = state.usage.as_ref().unwrap();
    store.lose_next_reply.store(true, Ordering::SeqCst);
    assert!(meter.flush().is_err());
    assert_eq!(store.inner.row_count(), 2, "the store did commit the batch");
    // The file is still spooled, so the next flush sends it again.
    let report = meter.flush().unwrap();
    assert_eq!((report.files, report.events, report.stored_rows), (1, 1, 0));
    assert_eq!(store.inner.row_count(), 2);
    assert_eq!(stored_usage(&store, "acme")[0].documents_written, 1);
    // And nothing is left to send.
    assert_eq!(meter.flush().unwrap(), FlushReport::default());
}

/// A process that dies with events in its spool: the next process on the
/// same disk delivers them, including the file that was still open.
#[test]
fn events_spooled_by_a_dead_process_are_delivered_by_the_next() {
    let dir = test_dir();
    let store = Arc::new(FlakyStore::default());
    let ns = NamespaceId("acme/docs".into());
    {
        let dead = meter_in(&dir, store.clone());
        store.down.store(true, Ordering::SeqCst);
        dead.record_query(&ns, 10, 0);
        // A failed flush leaves one sealed file behind...
        assert!(dead.flush().is_err());
        // ...and these two are still in the active file at the "crash".
        dead.record_query(&ns, 20, 0);
        dead.record_write(&ns, 300, 3);
    }
    assert_eq!(store.inner.row_count(), 0);
    store.down.store(false, Ordering::SeqCst);
    let next = meter_in(&dir, store.clone());
    let report = next.flush().unwrap();
    assert_eq!((report.files, report.events), (2, 3));
    let usage = &stored_usage(&store, "acme")[0];
    assert_eq!(
        (usage.queries, usage.bytes_returned, usage.bytes_written),
        (2, 30, 300)
    );
    // Delivered files are gone: a third process finds nothing to resend.
    let calls = store.record_calls.load(Ordering::SeqCst);
    assert_eq!(
        meter_in(&dir, store.clone()).flush().unwrap(),
        FlushReport::default()
    );
    assert_eq!(store.record_calls.load(Ordering::SeqCst), calls);
}

/// While the store is unreachable events wait on disk, up to the spool's
/// cap; past it they are dropped and counted, and requests still succeed.
#[test]
fn a_store_outage_spools_up_to_the_cap_then_drops_and_counts() {
    let dir = test_dir();
    let store = Arc::new(FlakyStore::default());
    store.down.store(true, Ordering::SeqCst);
    let ns = NamespaceId("acme/docs".into());
    // Room for exactly three events of this shape.
    let event_len = {
        let probe = meter_in(&test_dir(), store.clone());
        probe.record_query(&ns, 1, 0);
        probe.spool_bytes()
    };
    let state = AppState::with_options(
        dir.clone(),
        StateOptions {
            usage: Some(UsageMeter::new(
                store.clone(),
                UsageConfig {
                    spool_max_bytes: 3 * event_len + event_len / 2,
                    ..UsageConfig::under(&dir)
                },
            )),
            ..Default::default()
        },
    );
    let meter = state.usage.as_ref().unwrap();
    for _ in 0..2 {
        meter.record_query(&ns, 1, 0);
    }
    // Sealed-but-undelivered bytes still count against the cap.
    assert!(meter.flush().is_err());
    for _ in 0..3 {
        meter.record_query(&ns, 1, 0);
    }
    let pipeline = |state: &AppState| {
        let response = ok(usage::handle_admin_usage(
            "GET /v1/admin/usage HTTP/1.1\r\n",
            state,
        ));
        body_of(&response)["pipeline"].clone()
    };
    let during = pipeline(&state);
    assert_eq!(during["spooled_events"], 3);
    assert_eq!(during["dropped_events"], 2);
    assert_eq!(during["delivered_events"], 0);
    assert!(during["last_flush_error"]
        .as_str()
        .is_some_and(|e| e.contains("store down")));

    store.down.store(false, Ordering::SeqCst);
    let after = pipeline(&state);
    assert_eq!(after["delivered_events"], 3);
    assert_eq!(after["spool_bytes"], 0);
    assert_eq!(after["last_flush_error"], serde_json::Value::Null);
    assert_eq!(stored_usage(&store, "acme")[0].queries, 3);
    // With the spool drained there is room again.
    meter.record_query(&ns, 1, 0);
    assert_eq!(pipeline(&state)["delivered_events"], 4);
}

/// A line cut short by a crash is skipped; the events around it are kept.
#[test]
fn a_torn_spool_line_is_skipped() {
    let dir = test_dir();
    let store = Arc::new(FlakyStore::default());
    let ns = NamespaceId("acme/docs".into());
    {
        let dead = meter_in(&dir, store.clone());
        dead.record_query(&ns, 10, 0);
        let active = std::fs::read_dir(dead.spool_dir())
            .unwrap()
            .flatten()
            .next()
            .unwrap()
            .path();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(active)
            .unwrap();
        std::io::Write::write_all(&mut file, b"{\"id\":\"torn\",\"tenant\":\"ac").unwrap();
    }
    let report = meter_in(&dir, store.clone()).flush().unwrap();
    assert_eq!((report.events, report.malformed_lines), (1, 1));
    assert_eq!(stored_usage(&store, "acme")[0].queries, 1);
}

/// The hourly snapshot records what the namespace's segments occupy, once
/// per hour, and searches are priced against it whatever they read.
#[test]
fn stored_bytes_are_snapshotted_hourly_and_price_every_search_alike() {
    let (state, store) = metered_state();
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/documents",
        serde_json::json!({"documents": [doc("d1", "quick fox"), doc("d2", "lazy dog")]}),
    ));
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/flush",
        serde_json::json!({}),
    ));
    let on_disk = local_dir_bytes(&state.data_dir.join("acme/docs")).unwrap();
    assert!(on_disk > 0);

    let now = unix_millis();
    let hour = now / HOUR_MS;
    assert_eq!(usage::run_snapshot_tick(&state, now), 1);
    // Later ticks in the same hour add nothing, even after the namespace
    // grew: the hour's snapshot is taken once.
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/documents",
        serde_json::json!({"documents": [doc("d3", "another fox entirely")]}),
    ));
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/flush",
        serde_json::json!({}),
    ));
    assert_eq!(usage::run_snapshot_tick(&state, now + 1), 0);

    // Three searches on three paths: scored, served from the result cache,
    // and scored again with the cache bypassed.
    for body in [
        serde_json::json!({"query_text": "fox"}),
        serde_json::json!({"query_text": "fox"}),
        serde_json::json!({"query_text": "fox", "no_cache": true}),
    ] {
        ok(call(
            &state,
            "acme",
            "POST /v1/namespaces/docs/search",
            body,
        ));
    }
    flush(&state);
    let usage = usage_summary(
        store.as_ref(),
        Some("acme"),
        hour * HOUR_MS,
        (hour + 1) * HOUR_MS,
    )
    .unwrap();
    assert_eq!(usage[0].stored_snapshots, 1);
    assert_eq!(usage[0].stored_bytes_latest, Some(on_disk));
    assert_eq!(usage[0].stored_byte_hours, on_disk);
    assert_eq!(usage[0].queries, 3);
    assert_eq!(usage[0].bytes_queried, 3 * on_disk);

    // The next hour snapshots the grown namespace.
    let grown = local_dir_bytes(&state.data_dir.join("acme/docs")).unwrap();
    assert!(grown > on_disk);
    assert_eq!(usage::run_snapshot_tick(&state, (hour + 1) * HOUR_MS), 1);
    flush(&state);
    let next = usage_summary(
        store.as_ref(),
        Some("acme"),
        (hour + 1) * HOUR_MS,
        (hour + 2) * HOUR_MS,
    )
    .unwrap();
    assert_eq!(next[0].stored_bytes_latest, Some(grown));
}

/// A restarted writer (or a second one) snapshotting an hour that is
/// already recorded changes nothing.
#[test]
fn a_repeated_snapshot_of_the_same_hour_is_idempotent_across_processes() {
    let dir = test_dir();
    let store = Arc::new(FlakyStore::default());
    // One control store for both processes, as with Postgres.
    let control = Arc::new(Mutex::new(kosha_control::Controller::new()));
    let boot = || {
        AppState::with_options(
            dir.clone(),
            StateOptions {
                usage: Some(meter_in(&dir, store.clone())),
                control_store: Some(Box::new(tests::SharedManifestStore::new(control.clone()))),
                ..Default::default()
            },
        )
    };
    let now = unix_millis();
    {
        let state = boot();
        ok(call(
            &state,
            "acme",
            "POST /v1/namespaces/docs/documents",
            serde_json::json!({"documents": [doc("d1", "quick fox")]}),
        ));
        ok(call(
            &state,
            "acme",
            "POST /v1/namespaces/docs/flush",
            serde_json::json!({}),
        ));
        assert_eq!(usage::run_snapshot_tick(&state, now), 1);
        flush(&state);
    }
    let rows = store.inner.row_count();
    let restarted = boot();
    // The new process does not know the hour was snapshotted; the store does.
    // Two events: `stored_bytes` and `vector_dimensions`.
    assert_eq!(usage::run_snapshot_tick(&restarted, now), 1);
    let report = flush(&restarted);
    assert_eq!((report.events, report.stored_rows), (2, 0));
    assert_eq!(store.inner.row_count(), rows);
}

/// A delete by query is metered with the documents it removed and their
/// doc-store bytes; one that matches nothing emits nothing. The hourly
/// snapshot carries `vector_dimensions` from the segments' footers, read
/// once per segment.
#[test]
fn deletes_and_vector_dimensions_are_metered() {
    let (state, store) = metered_state();
    let vector = |id: &str, dims: usize| {
        let v: Vec<f32> = (0..dims).map(|i| i as f32 + 0.5).collect();
        serde_json::json!({"id": id, "fields": [
            {"name": "title", "field_type": "Text", "value": format!("doc {id}")},
            {"name": "emb", "field_type": "Vector", "value": serde_json::to_string(&v).unwrap()}
        ]})
    };
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/documents",
        serde_json::json!({"documents": [vector("a", 8), vector("b", 8), vector("c", 8), doc("d", "no vector")]}),
    ));
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/flush",
        serde_json::json!({}),
    ));
    let now = unix_millis();
    assert_eq!(usage::run_snapshot_tick(&state, now), 1);
    let stats = state.usage.as_ref().unwrap().stats();
    assert_eq!(stats.footer_reads.load(Ordering::Relaxed), 1);
    assert_eq!(stats.vector_snapshots.load(Ordering::Relaxed), 1);

    let deleted = ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/delete",
        serde_json::json!({"filter": {"match_all": {}}}),
    ));
    assert_eq!(body_of(&deleted)["deleted"], 4);
    // Nothing matched: nothing metered.
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/delete",
        serde_json::json!({"filter": {"term": {"nope": "zzz"}}}),
    ));
    flush(&state);
    let usage = stored_usage(&store, "acme");
    let docs = usage.iter().find(|u| u.namespace == "docs").unwrap();
    assert_eq!(docs.documents_deleted, 4);
    assert!(docs.bytes_deleted > 0, "{docs:?}");
    // Three 8-dimension vectors in one segment.
    assert_eq!(docs.vector_dimensions_latest, Some(3 * 8));
    assert_eq!(docs.vector_dimension_hours, 3 * 8);

    // The next hour: the same segment, its footer not read again.
    assert_eq!(usage::run_snapshot_tick(&state, now + HOUR_MS), 1);
    assert_eq!(stats.footer_reads.load(Ordering::Relaxed), 1);
    assert_eq!(stats.vector_snapshots.load(Ordering::Relaxed), 2);
    flush(&state);
    let usage = stored_usage(&store, "acme");
    let docs = usage.iter().find(|u| u.namespace == "docs").unwrap();
    assert_eq!(docs.vector_dimension_hours, 2 * 3 * 8);
}

/// Deletes are metered once applied, even when the publish after them
/// fails: the documents are gone from the writer either way (#348 review).
#[test]
fn applied_deletes_are_metered_even_when_the_publish_fails() {
    let (state, store) = metered_state();
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/documents",
        serde_json::json!({"documents": [doc("a", "x"), doc("b", "y")]}),
    ));
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/flush",
        serde_json::json!({}),
    ));
    *state.controller.lock().unwrap() = Box::new(crate::tests::FailingManifestStore(
        kosha_control::Controller::new(),
    ));
    let response = call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/delete",
        serde_json::json!({"filter": {"match_all": {}}}),
    );
    assert_eq!(status_of(&response), 500, "{response}");
    flush(&state);
    let usage = stored_usage(&store, "acme");
    let docs = usage.iter().find(|u| u.namespace == "docs").unwrap();
    assert_eq!(docs.documents_deleted, 2, "{docs:?}");
}

#[test]
fn footer_vector_dimensions_counts_columns_and_bags() {
    let mut footer: kosha_core::Footer = serde_json::from_value(serde_json::json!({
        "segment_id": "s", "doc_count": 3, "total_field_length": 0, "avg_field_length": 0.0,
        "bm25_params": {"k1": 1.2, "b": 0.75}, "created_at": "now"
    }))
    .unwrap();
    // A legacy footer (no column inventory) counts nothing.
    assert_eq!(usage::footer_vector_dimensions(&footer), 0);
    footer.vector_fields = Some(vec![
        serde_json::from_value(
            serde_json::json!({"name": "a", "dim": 768, "count": 10, "slot": 0}),
        )
        .unwrap(),
        serde_json::from_value(serde_json::json!({"name": "b", "dim": 4, "count": 2, "slot": 1}))
            .unwrap(),
    ]);
    footer.multi_vectors = vec![kosha_core::MultiVectorField {
        name: "m".into(),
        dim: 128,
        docs: 2,
        tokens: 50,
    }];
    assert_eq!(
        usage::footer_vector_dimensions(&footer),
        768 * 10 + 4 * 2 + 128 * 50
    );
}

/// A sharded namespace is metered as one namespace: its shards' segments
/// add up under the root, and no shard appears on its own.
#[test]
fn a_sharded_namespace_is_metered_under_its_root() {
    let (state, store) = metered_state();
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/big/shards",
        serde_json::json!({"shards": 3}),
    ));
    let documents: Vec<_> = (0..40)
        .map(|i| doc(&format!("d{i}"), "quick fox"))
        .collect();
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/big/documents",
        serde_json::json!({ "documents": documents }),
    ));
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/big/flush",
        serde_json::json!({}),
    ));
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/big/search",
        serde_json::json!({"query_text": "fox"}),
    ));
    let now = unix_millis();
    assert_eq!(usage::run_snapshot_tick(&state, now), 1);
    flush(&state);
    let usage = stored_usage(&store, "acme");
    assert_eq!(usage.len(), 1, "{usage:?}");
    assert_eq!(usage[0].namespace, "big");
    assert_eq!(usage[0].documents_written, 40);
    assert_eq!(usage[0].queries, 1);
    let shard_bytes: u64 =
        kosha_core::sharding::shard_namespaces(&NamespaceId("acme/big".into()), 3)
            .iter()
            .filter_map(|shard| local_dir_bytes(&state.data_dir.join(&shard.0)))
            .sum();
    assert!(shard_bytes > 0);
    assert_eq!(usage[0].stored_bytes_latest, Some(shard_bytes));

    assert_eq!(
        usage::usage_identity(&NamespaceId("acme/big/_shard-0002".into())),
        ("acme".to_string(), "big".to_string())
    );
    assert_eq!(
        usage::usage_identity(&NamespaceId("legacy_bare".into())),
        (String::new(), "legacy_bare".to_string())
    );
}

#[test]
fn the_read_api_is_tenant_scoped_and_the_admin_view_is_not() {
    let (state, _store) = metered_state();
    for tenant in ["acme", "globex"] {
        ok(call(
            &state,
            tenant,
            "POST /v1/namespaces/docs/documents",
            serde_json::json!({"documents": [doc("d1", "quick fox")]}),
        ));
        ok(call(
            &state,
            tenant,
            "POST /v1/namespaces/docs/flush",
            serde_json::json!({}),
        ));
    }
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/search",
        serde_json::json!({"query_text": "fox"}),
    ));

    // A tenant reads its own usage, and only that; the handler delivers
    // this pod's spool first, so no explicit flush is needed.
    let acme = body_of(&ok(call(
        &state,
        "acme",
        "GET /v1/usage",
        serde_json::Value::Null,
    )));
    assert_eq!(acme["tenant"], "acme");
    assert_eq!(acme["namespaces"].as_array().unwrap().len(), 1);
    assert_eq!(acme["namespaces"][0]["namespace"], "docs");
    assert_eq!(acme["namespaces"][0]["queries"], 1);
    assert_eq!(acme["total"]["queries"], 1);
    assert_eq!(acme["total"]["documents_written"], 1);
    assert!(
        acme.get("pipeline").is_none(),
        "no pod internals for a tenant"
    );
    assert!(acme["to"].as_u64().unwrap() > acme["from"].as_u64().unwrap());
    // `?tenant=` is an admin parameter: a tenant cannot read another's.
    let sneaky = body_of(&ok(call(
        &state,
        "globex",
        "GET /v1/usage?tenant=acme",
        serde_json::Value::Null,
    )));
    assert_eq!(sneaky["tenant"], "globex");
    assert_eq!(sneaky["total"]["queries"], 0);

    // A window that ended before anything happened is empty.
    let past = body_of(&ok(call(
        &state,
        "acme",
        "GET /v1/usage?from=1000&to=2000",
        serde_json::Value::Null,
    )));
    assert_eq!(past["namespaces"].as_array().unwrap().len(), 0);
    assert_eq!(
        (past["from"].as_u64(), past["to"].as_u64()),
        (Some(1000), Some(2000))
    );
    for bad in [
        "GET /v1/usage?from=2000&to=1000",
        "GET /v1/usage?from=yesterday",
        "GET /v1/usage?from=0&to=99999999999",
    ] {
        let response = call(&state, "acme", bad, serde_json::Value::Null);
        assert_eq!(status_of(&response), 400, "{bad}: {response}");
    }

    // The admin route needs the admin credential...
    let refused = call(
        &state,
        "acme",
        "GET /v1/admin/usage",
        serde_json::Value::Null,
    );
    assert_eq!(status_of(&refused), 403, "{refused}");
    // ...which this state does not configure, so call the handler the
    // route dispatches to.
    let all = body_of(&ok(usage::handle_admin_usage(
        "GET /v1/admin/usage HTTP/1.1\r\n",
        &state,
    )));
    assert_eq!(all["namespaces"].as_array().unwrap().len(), 2);
    assert_eq!(all["total"]["documents_written"], 2);
    assert_eq!(all["pipeline"]["dropped_events"], 0);
    let one = body_of(&ok(usage::handle_admin_usage(
        "GET /v1/admin/usage?tenant=globex HTTP/1.1\r\n",
        &state,
    )));
    assert_eq!(one["tenant"], "globex");
    assert_eq!(one["total"]["queries"], 0);
    assert_eq!(one["total"]["documents_written"], 1);
}

/// A failed read answers 503; the store's own error text reaches the admin
/// route only, never a tenant.
#[test]
fn a_store_read_failure_does_not_show_a_tenant_the_store_error() {
    let (state, store) = metered_state();
    store.reads_down.store(true, Ordering::SeqCst);
    let tenant = call(&state, "acme", "GET /v1/usage", serde_json::Value::Null);
    assert_eq!(status_of(&tenant), 503, "{tenant}");
    assert!(!tenant.contains("injected"), "{tenant}");
    let admin = usage::handle_admin_usage("GET /v1/admin/usage HTTP/1.1\r\n", &state);
    assert_eq!(status_of(&admin), 503, "{admin}");
    assert!(admin.contains("injected"), "{admin}");
}

/// Metering is opt-in: a default server emits nothing, creates no spool
/// and answers the usage routes with 404.
#[test]
fn metering_is_off_by_default() {
    let dir = test_dir();
    let state = AppState::new(dir.clone());
    assert!(state.usage.is_none());
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/documents",
        serde_json::json!({"documents": [doc("d1", "quick fox")]}),
    ));
    ok(call(
        &state,
        "acme",
        "POST /v1/namespaces/docs/search",
        serde_json::json!({"query_text": "fox"}),
    ));
    assert!(!dir.join(usage::SPOOL_DIR_NAME).exists());
    assert_eq!(usage::run_snapshot_tick(&state, unix_millis()), 0);
    let response = call(&state, "acme", "GET /v1/usage", serde_json::Value::Null);
    assert_eq!(status_of(&response), 404, "{response}");
    state.flush_usage_on_shutdown();
}
