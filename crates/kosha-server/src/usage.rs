//! Usage metering per tenant and namespace (#348): local counters, sealed
//! windows, hourly storage snapshots, the usage API and reconciliation.
//!
//! # Counting
//!
//! [`UsageRecorder`] keeps one row of counters per `(window, namespace)`
//! for the current window (`KOSHA_USAGE_FLUSH_SECS`, default 60 s, aligned
//! to multiples of the window on the wall clock). Recording is lock-light:
//! a shared read lock and atomic adds when the row exists, a write lock
//! only for the first record of a namespace in a window.
//!
//! # Sealing and flushing
//!
//! A flusher thread wakes after each window boundary, seals every window
//! that has ended into [`UsageEvent`]s and appends them to the
//! [`UsageStore`]. An event's id is a hash of this process's source id,
//! the window, tenant, namespace and meter, so a flush retried after a
//! store error or a timeout that did commit never double-counts: the store
//! ignores ids it holds. Sealed events that failed to append stay pending
//! and are retried on the next tick (bounded by [`MAX_PENDING_EVENTS`]).
//! A record that arrives for a window that was already sealed (a request
//! that started before the boundary) is counted in the open window
//! instead, so a sealed window's id is never reused for a second event.
//!
//! The source id is the pod name plus a per-process nonce: a restarted pod
//! (same name) cannot collide with the ids its previous incarnation wrote.
//!
//! # Loss bound
//!
//! A graceful shutdown (SIGTERM) seals the open window and flushes. Hard
//! pod loss (OOM kill, node loss) loses what was not yet appended: at most
//! the one open window, plus any sealed windows still pending because the
//! store was unreachable at the time. With a healthy store that is at most
//! `KOSHA_USAGE_FLUSH_SECS` of counts on that pod.
//!
//! # Snapshots
//!
//! Once an hour (and at start-up) a writer pod emits `storage_bytes` and
//! `vector_dim_docs` for each namespace: the S3 prefix size when S3 is
//! enabled, else the local segment directory size; and the sum over
//! segment footers of vector dimension × vectors. A snapshot's id does not
//! include the pod, so a re-emitted snapshot for the same hour (a restart)
//! is stored once. A deleted namespace gets a final zero snapshot.
//!
//! # Bytes scanned
//!
//! The query path does not expose the bytes a search read cheaply, so
//! `query_bytes_scanned` is approximated by the namespace's stored bytes
//! as of its latest snapshot on this pod (a search may read all of it).
//! Exists requests are id lookups and scan nothing for metering.
//!
//! Without a configured store (`KOSHA_USAGE=off`, or no `DATABASE_URL`)
//! the recorder is inert: every hook is a no-op.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kosha_control::{MemoryUsageStore, Meter, UsageEvent, UsageRow, UsageStore};
use kosha_core::tenancy::{tenant_of_namespace, LEGACY_TENANT};
use kosha_core::NamespaceId;

use crate::{json_error, json_ok, url_decode, AppState};

/// Default for `KOSHA_USAGE_FLUSH_SECS`.
pub(crate) const DEFAULT_FLUSH_SECS: i64 = 60;
/// Snapshot period (`KOSHA_USAGE_SNAPSHOT_SECS`, default an hour).
pub(crate) const DEFAULT_SNAPSHOT_SECS: i64 = 3600;
/// Default for `KOSHA_USAGE_RECONCILE_TOLERANCE`: 2%.
pub(crate) const DEFAULT_RECONCILE_TOLERANCE: f64 = 0.02;
/// Sealed events kept for retry while the store is unreachable. Beyond
/// this the oldest are dropped (and logged): memory is bounded before
/// completeness.
pub(crate) const MAX_PENDING_EVENTS: usize = 500_000;
/// The `source` hashed into snapshot ids, so every pod's snapshot of the
/// same namespace and period is one event.
const SNAPSHOT_SOURCE: &str = "snapshot";

/// Counter meters, in counter-array order.
const COUNTERS: [Meter; 5] = [
    Meter::WriteBytes,
    Meter::WriteDocs,
    Meter::Queries,
    Meter::QueryBytesReturned,
    Meter::QueryBytesScanned,
];

fn counter_slot(meter: Meter) -> usize {
    COUNTERS
        .iter()
        .position(|m| *m == meter)
        .expect("snapshot meters are not counted")
}

pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn floor_to(t: i64, period: i64) -> i64 {
    t.div_euclid(period) * period
}

/// `(tenant, namespace-within-tenant)` of an internal namespace id.
pub(crate) fn split_namespace(ns: &str) -> (String, String) {
    let tenant = tenant_of_namespace(ns);
    let name = if tenant == LEGACY_TENANT {
        ns
    } else {
        ns.strip_prefix(tenant)
            .and_then(|r| r.strip_prefix('/'))
            .unwrap_or(ns)
    };
    (tenant.to_string(), name.to_string())
}

/// The internal namespace id of `(tenant, namespace)`; the inverse of
/// [`split_namespace`].
pub(crate) fn join_namespace(tenant: &str, namespace: &str) -> String {
    if tenant == LEGACY_TENANT {
        namespace.to_string()
    } else {
        format!("{tenant}/{namespace}")
    }
}

/// Settings read from the environment.
#[derive(Debug, Clone)]
pub(crate) struct UsageConfig {
    pub window_secs: i64,
    pub snapshot_secs: i64,
    pub reconcile_tolerance: f64,
}

impl Default for UsageConfig {
    fn default() -> Self {
        Self {
            window_secs: DEFAULT_FLUSH_SECS,
            snapshot_secs: DEFAULT_SNAPSHOT_SECS,
            reconcile_tolerance: DEFAULT_RECONCILE_TOLERANCE,
        }
    }
}

impl UsageConfig {
    pub(crate) fn from_env() -> Self {
        let secs = |name: &str, default: i64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|n| *n > 0)
                .unwrap_or(default)
        };
        Self {
            window_secs: secs("KOSHA_USAGE_FLUSH_SECS", DEFAULT_FLUSH_SECS),
            snapshot_secs: secs("KOSHA_USAGE_SNAPSHOT_SECS", DEFAULT_SNAPSHOT_SECS),
            reconcile_tolerance: parse_tolerance(
                std::env::var("KOSHA_USAGE_RECONCILE_TOLERANCE")
                    .ok()
                    .as_deref(),
            ),
        }
    }
}

/// `KOSHA_USAGE_RECONCILE_TOLERANCE`: a fraction (`0.02`) or a percentage
/// (`2%`). Anything else is the default.
pub(crate) fn parse_tolerance(raw: Option<&str>) -> f64 {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return DEFAULT_RECONCILE_TOLERANCE;
    };
    let parsed = match raw.strip_suffix('%') {
        Some(pct) => pct.trim().parse::<f64>().ok().map(|p| p / 100.0),
        None => raw.parse::<f64>().ok(),
    };
    match parsed.filter(|t| t.is_finite() && *t >= 0.0) {
        Some(t) => t,
        None => {
            eprintln!(
                "WARN: KOSHA_USAGE_RECONCILE_TOLERANCE={raw} is not a fraction or percentage; \
                 using {DEFAULT_RECONCILE_TOLERANCE}"
            );
            DEFAULT_RECONCILE_TOLERANCE
        }
    }
}

/// Drop the oldest pending events beyond [`MAX_PENDING_EVENTS`].
fn cap_pending(pending: &mut Vec<UsageEvent>) {
    if pending.len() > MAX_PENDING_EVENTS {
        let excess = pending.len() - MAX_PENDING_EVENTS;
        eprintln!(
            "WARN: usage: {excess} unflushed usage event(s) dropped; the usage store has \
             been unreachable for too long"
        );
        pending.drain(..excess);
    }
}

/// One namespace's counters in one window.
#[derive(Default)]
struct Cell([AtomicU64; COUNTERS.len()]);

struct Counters {
    /// Windows starting before this are sealed; a late record lands here.
    open_from: i64,
    windows: BTreeMap<i64, HashMap<String, Cell>>,
}

/// The live recorder behind an enabled [`UsageRecorder`].
pub(crate) struct Recorder {
    store: Arc<dyn UsageStore>,
    cfg: UsageConfig,
    /// Pod name + process nonce: the `source` of counter event ids.
    source: String,
    /// The pod name alone, for snapshot events' `source_pod`.
    pod: String,
    counters: RwLock<Counters>,
    pending: Mutex<Vec<UsageEvent>>,
    /// Namespace id → stored bytes as of its latest snapshot on this pod,
    /// the `query_bytes_scanned` approximation.
    namespace_bytes: RwLock<HashMap<String, u64>>,
    /// Serializes flushes (the flusher thread, shutdown, tests).
    flush_lock: Mutex<()>,
    stop: (Mutex<bool>, Condvar),
}

impl Recorder {
    pub(crate) fn new(store: Arc<dyn UsageStore>, cfg: UsageConfig, pod: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        Self::with_source(store, cfg, pod, &format!("{pod}#{nonce:x}"))
    }

    pub(crate) fn with_source(
        store: Arc<dyn UsageStore>,
        cfg: UsageConfig,
        pod: &str,
        source: &str,
    ) -> Self {
        Self {
            store,
            cfg,
            source: source.to_string(),
            pod: pod.to_string(),
            counters: RwLock::new(Counters {
                open_from: i64::MIN,
                windows: BTreeMap::new(),
            }),
            pending: Mutex::new(Vec::new()),
            namespace_bytes: RwLock::new(HashMap::new()),
            flush_lock: Mutex::new(()),
            stop: (Mutex::new(false), Condvar::new()),
        }
    }

    /// Add `amounts` to namespace `ns`'s counters for the window holding
    /// `now` (or the open window, if that one is sealed already).
    pub(crate) fn record_at(&self, now: i64, ns: &str, amounts: &[(Meter, u64)]) {
        if amounts.iter().all(|(_, q)| *q == 0) {
            return;
        }
        let add = |cell: &Cell| {
            for (meter, quantity) in amounts {
                if *quantity > 0 {
                    cell.0[counter_slot(*meter)].fetch_add(*quantity, Ordering::Relaxed);
                }
            }
        };
        let window = floor_to(now, self.cfg.window_secs);
        {
            let counters = self.counters.read().unwrap_or_else(|e| e.into_inner());
            let window = window.max(counters.open_from);
            if let Some(cell) = counters.windows.get(&window).and_then(|w| w.get(ns)) {
                add(cell);
                return;
            }
        }
        let mut counters = self.counters.write().unwrap_or_else(|e| e.into_inner());
        let window = window.max(counters.open_from);
        let cell = counters
            .windows
            .entry(window)
            .or_default()
            .entry(ns.to_string())
            .or_default();
        add(cell);
    }

    /// Seal every window that ended by `now` (or, with `all`, every
    /// window) into events. Later records for those windows go to the
    /// open window.
    pub(crate) fn seal_at(&self, now: i64, all: bool) -> Vec<UsageEvent> {
        let w = self.cfg.window_secs;
        let cutoff = if all {
            floor_to(now, w) + w
        } else {
            floor_to(now, w)
        };
        let sealed = {
            let mut counters = self.counters.write().unwrap_or_else(|e| e.into_inner());
            counters.open_from = counters.open_from.max(cutoff);
            let open = counters.windows.split_off(&cutoff);
            std::mem::replace(&mut counters.windows, open)
        };
        let mut events = Vec::new();
        for (start, namespaces) in sealed {
            for (ns, cell) in namespaces {
                let (tenant, namespace) = split_namespace(&ns);
                for (slot, meter) in COUNTERS.iter().enumerate() {
                    let quantity = cell.0[slot].load(Ordering::Relaxed);
                    if quantity == 0 {
                        continue;
                    }
                    events.push(UsageEvent {
                        event_id: UsageEvent::derive_id(
                            &self.source,
                            start,
                            start + w,
                            &tenant,
                            &namespace,
                            *meter,
                        ),
                        tenant: tenant.clone(),
                        namespace: namespace.clone(),
                        meter: *meter,
                        quantity,
                        window_start: start,
                        window_end: start + w,
                        source_pod: self.source.clone(),
                    });
                }
            }
        }
        events
    }

    /// Queue `events` for the store.
    fn enqueue(&self, events: Vec<UsageEvent>) {
        if events.is_empty() {
            return;
        }
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.extend(events);
        cap_pending(&mut pending);
    }

    /// Seal what ended by `now` and append everything pending. On a store
    /// error the events stay pending for the next attempt.
    pub(crate) fn flush_at(&self, now: i64, all: bool) -> Result<usize, String> {
        let _guard = self.flush_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.enqueue(self.seal_at(now, all));
        let batch = std::mem::take(&mut *self.pending.lock().unwrap_or_else(|e| e.into_inner()));
        if batch.is_empty() {
            return Ok(0);
        }
        match self.store.append(&batch) {
            Ok(added) => Ok(added),
            Err(e) => {
                // Back in front of anything queued meanwhile, in order.
                let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
                let newer = std::mem::replace(&mut *pending, batch);
                pending.extend(newer);
                cap_pending(&mut pending);
                Err(e.to_string())
            }
        }
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.pending.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Snapshot events for the period holding `now`: one `storage_bytes`
    /// and one `vector_dim_docs` per `(namespace, bytes, dim_docs)`.
    /// `emit` false only refreshes the bytes-scanned approximation.
    pub(crate) fn snapshot_at(&self, now: i64, entries: &[(String, u64, u64)], emit: bool) {
        {
            let mut bytes = self
                .namespace_bytes
                .write()
                .unwrap_or_else(|e| e.into_inner());
            for (ns, storage, _) in entries {
                bytes.insert(ns.clone(), *storage);
            }
        }
        if !emit {
            return;
        }
        let period = self.cfg.snapshot_secs;
        let start = floor_to(now, period);
        let events = entries
            .iter()
            .flat_map(|(ns, storage, dim_docs)| {
                [
                    self.snapshot_event(ns, Meter::StorageBytes, *storage, start, start + period),
                    self.snapshot_event(ns, Meter::VectorDimDocs, *dim_docs, start, start + period),
                ]
            })
            .collect();
        self.enqueue(events);
    }

    fn snapshot_event(
        &self,
        ns: &str,
        meter: Meter,
        quantity: u64,
        start: i64,
        end: i64,
    ) -> UsageEvent {
        let (tenant, namespace) = split_namespace(ns);
        UsageEvent {
            event_id: UsageEvent::derive_id(
                SNAPSHOT_SOURCE,
                start,
                end,
                &tenant,
                &namespace,
                meter,
            ),
            tenant,
            namespace,
            meter,
            quantity,
            window_start: start,
            window_end: end,
            source_pod: self.pod.clone(),
        }
    }

    /// A deleted namespace's final zero snapshot, stamped at `now` so it is
    /// the namespace's latest.
    pub(crate) fn namespace_deleted_at(&self, now: i64, ns: &str) {
        self.namespace_bytes
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(ns);
        self.enqueue(vec![
            self.snapshot_event(ns, Meter::StorageBytes, 0, now, now + 1),
            self.snapshot_event(ns, Meter::VectorDimDocs, 0, now, now + 1),
        ]);
    }

    fn namespace_bytes(&self, ns: &str) -> u64 {
        self.namespace_bytes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(ns)
            .copied()
            .unwrap_or(0)
    }

    /// Sleep up to `d`, waking early on [`UsageRecorder::shutdown`].
    /// Returns whether the recorder was stopped.
    fn sleep(&self, d: Duration) -> bool {
        let guard = self.stop.0.lock().unwrap_or_else(|e| e.into_inner());
        let (guard, _) = self
            .stop
            .1
            .wait_timeout_while(guard, d, |stopped| !*stopped)
            .unwrap_or_else(|e| e.into_inner());
        *guard
    }
}

/// The handle on `AppState`. Inert (every call a no-op) without a store.
#[derive(Clone, Default)]
pub(crate) struct UsageRecorder {
    inner: Option<Arc<Recorder>>,
}

impl UsageRecorder {
    pub(crate) fn inert() -> Self {
        Self { inner: None }
    }

    pub(crate) fn new(recorder: Recorder) -> Self {
        Self {
            inner: Some(Arc::new(recorder)),
        }
    }

    /// `KOSHA_USAGE`: `auto` (the default: Postgres when `DATABASE_URL` is
    /// set and the binary has Postgres, else off), `postgres`, `memory`
    /// (single-node dev; lost on restart) or `off`.
    pub(crate) fn from_env() -> Self {
        let mode = std::env::var("KOSHA_USAGE")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        let database_url = std::env::var("DATABASE_URL").ok().filter(|u| !u.is_empty());
        let store: Option<Arc<dyn UsageStore>> = match mode.as_str() {
            "off" | "false" | "0" => None,
            "memory" => Some(Arc::new(MemoryUsageStore::new())),
            "" | "auto" | "postgres" | "on" => match database_url {
                Some(url) => pg_store(&url),
                None => {
                    if mode == "postgres" {
                        eprintln!("WARN: KOSHA_USAGE=postgres but DATABASE_URL is not set");
                    }
                    None
                }
            },
            other => {
                eprintln!("WARN: unknown KOSHA_USAGE={other:?}: use auto, postgres, memory or off");
                None
            }
        };
        let Some(store) = store else {
            println!("usage metering: off");
            return Self::inert();
        };
        let cfg = UsageConfig::from_env();
        let pod = std::env::var("POD_NAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .ok()
            .filter(|p| !p.is_empty())
            .unwrap_or_else(|| "local".into());
        println!(
            "usage metering: on (window {} s, snapshot every {} s, reconcile tolerance {})",
            cfg.window_secs, cfg.snapshot_secs, cfg.reconcile_tolerance
        );
        Self::new(Recorder::new(store, cfg, &pod))
    }

    pub(crate) fn recorder(&self) -> Option<&Arc<Recorder>> {
        self.inner.as_ref()
    }

    /// An accepted index/replace request into namespace `ns`.
    pub(crate) fn record_write(&self, ns: &str, bytes: u64, docs: u64) {
        if let Some(r) = &self.inner {
            r.record_at(
                unix_now(),
                ns,
                &[(Meter::WriteBytes, bytes), (Meter::WriteDocs, docs)],
            );
        }
    }

    /// A search (`scans`) or exists request against `ns`, metered when it
    /// answered 200. Returns `response`, so a handler can wrap its result.
    pub(crate) fn query(&self, ns: &str, response: String, scans: bool) -> String {
        if let Some(r) = &self.inner {
            if !ns.is_empty() && response.starts_with("HTTP/1.1 200") {
                let body = response
                    .split_once("\r\n\r\n")
                    .map_or(0, |(_, body)| body.len() as u64);
                let scanned = if scans { r.namespace_bytes(ns) } else { 0 };
                r.record_at(
                    unix_now(),
                    ns,
                    &[
                        (Meter::Queries, 1),
                        (Meter::QueryBytesReturned, body),
                        (Meter::QueryBytesScanned, scanned),
                    ],
                );
            }
        }
        response
    }

    /// Namespace `ns` was deleted: a final zero storage snapshot.
    pub(crate) fn record_namespace_deleted(&self, ns: &str) {
        if let Some(r) = &self.inner {
            r.namespace_deleted_at(unix_now(), ns);
        }
    }

    /// Start the flusher thread: seal and append after every window
    /// boundary, retrying pending events after a store error.
    pub(crate) fn spawn_flusher(&self) {
        let Some(r) = self.inner.clone() else {
            return;
        };
        std::thread::Builder::new()
            .name("kosha-usage-flush".into())
            .spawn(move || loop {
                let now = unix_now();
                let next = floor_to(now, r.cfg.window_secs) + r.cfg.window_secs;
                // A second past the boundary, so the window has ended.
                let wait = Duration::from_secs((next - now).max(0) as u64 + 1);
                if r.sleep(wait) {
                    return;
                }
                if let Err(e) = r.flush_at(unix_now(), false) {
                    eprintln!(
                        "WARN: usage flush failed ({} event(s) kept for retry): {e}",
                        r.pending_len()
                    );
                }
            })
            .expect("failed to spawn usage flusher thread");
    }

    /// Graceful shutdown: stop the background threads, seal the open
    /// window and flush, retrying a few times.
    pub(crate) fn shutdown(&self) {
        let Some(r) = &self.inner else {
            return;
        };
        *r.stop.0.lock().unwrap_or_else(|e| e.into_inner()) = true;
        r.stop.1.notify_all();
        for attempt in 1..=3 {
            match r.flush_at(unix_now(), true) {
                Ok(added) => {
                    println!("usage: flushed {added} event(s) at shutdown");
                    return;
                }
                Err(e) => {
                    eprintln!("WARN: usage flush at shutdown failed (attempt {attempt}/3): {e}");
                    std::thread::sleep(Duration::from_millis(500 * attempt));
                }
            }
        }
        eprintln!("WARN: usage: {} event(s) lost at shutdown", r.pending_len());
    }
}

#[cfg(feature = "postgres")]
fn pg_store(url: &str) -> Option<Arc<dyn UsageStore>> {
    match kosha_control::PgUsageStore::new(url) {
        Ok(store) => Some(Arc::new(store)),
        Err(e) => {
            // Metering must not take the data path down with it.
            eprintln!("WARN: usage metering disabled: usage store: {e}");
            None
        }
    }
}

#[cfg(not(feature = "postgres"))]
fn pg_store(_url: &str) -> Option<Arc<dyn UsageStore>> {
    eprintln!("WARN: usage metering needs the postgres feature for DATABASE_URL; off");
    None
}

// ─── Snapshots ─────────────────────────────────────────────────────────────

/// Recursive size of the regular files under `dir` (0 if absent).
fn dir_size(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| match entry.file_type() {
            Ok(t) if t.is_dir() => dir_size(&entry.path()),
            Ok(t) if t.is_file() => entry.metadata().map_or(0, |m| m.len()),
            _ => 0,
        })
        .sum()
}

/// Σ vector dimension × vectors over the segments whose footers `manifest`
/// carries (a segment without a cached footer is not counted).
fn manifest_vector_dim_docs(manifest: &kosha_core::Manifest) -> u64 {
    manifest
        .segments
        .iter()
        .filter_map(|entry| manifest.segment_footer(&entry.segment_id))
        .flat_map(|footer| footer.vector_fields.iter().flatten())
        .map(|field| u64::from(field.dim) * u64::from(field.count))
        .sum()
}

impl AppState {
    /// Start the usage threads: the flusher, and the snapshot task. Writer
    /// pods emit snapshots; other pods with S3 only refresh the
    /// bytes-scanned approximation.
    pub(crate) fn spawn_usage(self: &Arc<Self>) {
        let Some(recorder) = self.usage.recorder().cloned() else {
            return;
        };
        self.usage.spawn_flusher();
        let emit = self.role.accepts_writes();
        #[cfg(feature = "s3")]
        let measure = emit || self.s3_storage.is_some();
        #[cfg(not(feature = "s3"))]
        let measure = emit;
        if !measure {
            return;
        }
        let state = Arc::clone(self);
        std::thread::Builder::new()
            .name("kosha-usage-snapshot".into())
            .spawn(move || loop {
                let now = unix_now();
                let entries = state.usage_snapshot_entries();
                recorder.snapshot_at(now, &entries, emit);
                let period = recorder.cfg.snapshot_secs;
                let next = floor_to(now, period) + period;
                let wait = Duration::from_secs((next - unix_now()).max(0) as u64 + 1);
                if recorder.sleep(wait) {
                    return;
                }
            })
            .expect("failed to spawn usage snapshot thread");
    }

    /// `(namespace, storage bytes, vector dim × docs)` for every namespace
    /// in the control store. Shards are counted under their root: the
    /// root's prefix holds their files.
    pub(crate) fn usage_snapshot_entries(&self) -> Vec<(String, u64, u64)> {
        let namespaces = self.controller.lock().unwrap().list_namespaces();
        namespaces
            .into_iter()
            .filter(|ns| kosha_core::sharding::shard_index_of(ns).is_none())
            .filter_map(|ns| {
                let bytes = self.namespace_storage_bytes(&ns.0)?;
                let mut ids = vec![ns.clone()];
                if let Some(shards) = self.indexer.shard_count(&ns) {
                    ids.extend(kosha_core::sharding::shard_namespaces(&ns, shards));
                }
                let dim_docs = ids
                    .iter()
                    .filter_map(|id| self.indexer.manifest_shared(id))
                    .map(|m| manifest_vector_dim_docs(&m))
                    .sum();
                Some((ns.0, bytes, dim_docs))
            })
            .collect()
    }

    /// A namespace's stored bytes: its S3 prefix with S3, else its local
    /// segment directory (authoritative only on a writer pod).
    fn namespace_storage_bytes(&self, ns: &str) -> Option<u64> {
        #[cfg(feature = "s3")]
        if let Some(ref s3) = self.s3_storage {
            return match s3.list_namespace_objects(ns) {
                Ok(objects) => Some(objects.iter().map(|(_, size)| size).sum()),
                Err(e) => {
                    eprintln!("WARN: usage snapshot: listing {ns} failed: {e}");
                    None
                }
            };
        }
        self.role
            .accepts_writes()
            .then(|| dir_size(&self.data_dir.join(ns)))
    }

    /// Live bytes under a namespace's S3 prefix, for reconciliation.
    #[cfg(feature = "s3")]
    fn live_prefix_bytes(&self, ns: &str) -> Option<Result<u64, String>> {
        let s3 = self.s3_storage.as_ref()?;
        Some(
            s3.list_namespace_objects(ns)
                .map(|objects| objects.iter().map(|(_, size)| size).sum())
                .map_err(|e| e.to_string()),
        )
    }

    #[cfg(not(feature = "s3"))]
    fn live_prefix_bytes(&self, _ns: &str) -> Option<Result<u64, String>> {
        None
    }
}

// ─── Reconciliation ────────────────────────────────────────────────────────

/// One namespace's metered storage against the bucket.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(crate) struct Drift {
    pub tenant: String,
    pub namespace: String,
    /// Latest `storage_bytes` snapshot (0 when none was ever taken).
    pub metered_bytes: u64,
    /// Its `window_start` (Unix seconds), `None` when never metered.
    pub metered_at: Option<i64>,
    pub live_bytes: u64,
    /// `metered - live`.
    pub drift_bytes: i64,
    /// `drift_bytes / live`; ±1.0 when one side is zero and the other not.
    pub drift_ratio: f64,
    pub within_tolerance: bool,
}

/// Compare each namespace's latest snapshot with `live(namespace id)`.
/// `also` lists namespaces that exist but may never have been metered.
/// Returns the drifts (sorted by tenant, namespace) and per-namespace
/// listing errors.
pub(crate) fn reconcile(
    snapshots: &[UsageEvent],
    also: &[(String, String)],
    mut live: impl FnMut(&str) -> Result<u64, String>,
    tolerance: f64,
) -> (Vec<Drift>, Vec<(String, String)>) {
    let mut metered: BTreeMap<(String, String), (u64, Option<i64>)> =
        also.iter().map(|key| (key.clone(), (0, None))).collect();
    for e in snapshots {
        metered.insert(
            (e.tenant.clone(), e.namespace.clone()),
            (e.quantity, Some(e.window_start)),
        );
    }
    let mut drifts = Vec::new();
    let mut errors = Vec::new();
    for ((tenant, namespace), (metered_bytes, metered_at)) in metered {
        let id = join_namespace(&tenant, &namespace);
        let live_bytes = match live(&id) {
            Ok(bytes) => bytes,
            Err(e) => {
                errors.push((id, e));
                continue;
            }
        };
        let drift_bytes = metered_bytes as i64 - live_bytes as i64;
        let drift_ratio = match (metered_bytes, live_bytes) {
            (0, 0) => 0.0,
            (_, 0) => 1.0,
            _ => drift_bytes as f64 / live_bytes as f64,
        };
        drifts.push(Drift {
            tenant,
            namespace,
            metered_bytes,
            metered_at,
            live_bytes,
            drift_bytes,
            drift_ratio,
            within_tolerance: drift_ratio.abs() <= tolerance,
        });
    }
    (drifts, errors)
}

// ─── API ───────────────────────────────────────────────────────────────────

/// Query-string pairs of a request line's target, URL-decoded.
fn query_params(request_line: &str) -> HashMap<String, String> {
    request_line
        .split(' ')
        .nth(1)
        .and_then(|target| target.split_once('?'))
        .map(|(_, q)| q)
        .unwrap_or("")
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| (k.to_string(), url_decode(v)))
        .collect()
}

/// The bare path of a request line, when its method is `method`.
fn bare_path<'a>(request_line: &'a str, method: &str) -> Option<&'a str> {
    let mut parts = request_line.split(' ');
    if parts.next()? != method {
        return None;
    }
    let target = parts.next()?;
    Some(target.split('?').next().unwrap_or(target))
}

/// A time parameter: Unix seconds, or a date / date-time (`2026-11-01`,
/// `2026-11-01T00:00:00Z`).
pub(crate) fn parse_time(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    if let Ok(secs) = raw.parse::<i64>() {
        return Some(secs);
    }
    kosha_core::datetime::parse_epoch_millis(raw).map(|ms| ms.div_euclid(1000))
}

/// Start of the UTC calendar month holding `now`.
pub(crate) fn month_start(now: i64) -> i64 {
    let date = kosha_core::datetime::format_epoch_millis(now * 1000);
    date.get(..7)
        .and_then(|ym| parse_time(&format!("{ym}-01")))
        .unwrap_or_else(|| floor_to(now, 86_400))
}

/// `from`/`to` of a usage query: default from the start of this month to
/// now. `Err` carries the 400.
fn time_range(params: &HashMap<String, String>) -> Result<(i64, i64), String> {
    let now = unix_now();
    let get = |key: &str, default: i64| match params.get(key) {
        None => Ok(default),
        Some(raw) => parse_time(raw).ok_or_else(|| {
            json_error(
                400,
                &format!("invalid '{key}': use Unix seconds or an ISO-8601 date"),
            )
        }),
    };
    let to = get("to", now + 1)?;
    let from = get("from", month_start(now))?;
    if from >= to {
        return Err(json_error(400, "'from' must be before 'to'"));
    }
    Ok((from, to))
}

fn disabled() -> String {
    json_error(
        404,
        "usage metering is not enabled on this deployment (KOSHA_USAGE)",
    )
}

/// `{"namespaces": [{"namespace", "meters": {..}}], "totals": {..}}` from
/// per-namespace rows.
fn usage_body(tenant: &str, from: i64, to: i64, rows: &[UsageRow]) -> serde_json::Value {
    let mut namespaces: BTreeMap<&str, serde_json::Map<String, serde_json::Value>> =
        BTreeMap::new();
    let mut totals: BTreeMap<&str, u64> = BTreeMap::new();
    for row in rows {
        let ns = row.namespace.as_deref().unwrap_or("");
        namespaces
            .entry(ns)
            .or_default()
            .insert(row.meter.as_str().into(), row.quantity.into());
        *totals.entry(row.meter.as_str()).or_default() += row.quantity;
    }
    serde_json::json!({
        "tenant": tenant,
        "from": from,
        "to": to,
        "namespaces": namespaces
            .into_iter()
            .map(|(ns, meters)| serde_json::json!({"namespace": ns, "meters": meters}))
            .collect::<Vec<_>>(),
        "totals": totals,
    })
}

/// `GET /v1/usage?from=&to=&namespace=`: the caller's own usage, totals per
/// namespace and meter. Answered before `route` (which 404s paths outside
/// `/v1/namespaces/`); `None` for any other request.
pub(crate) fn route_tenant(request_line: &str, tenant: &str, state: &AppState) -> Option<String> {
    if bare_path(request_line, "GET")? != "/v1/usage" {
        return None;
    }
    if !crate::valid_name_component(tenant) {
        return Some(json_error(403, "invalid tenant"));
    }
    let Some(recorder) = state.usage.recorder() else {
        return Some(disabled());
    };
    let params = query_params(request_line);
    let (from, to) = match time_range(&params) {
        Ok(range) => range,
        Err(response) => return Some(response),
    };
    let mut rows = match recorder.store.aggregate(tenant, from, to, true) {
        Ok(rows) => rows,
        Err(e) => return Some(json_error(503, &format!("usage store: {e}"))),
    };
    if let Some(ns) = params.get("namespace") {
        rows.retain(|r| r.namespace.as_deref() == Some(ns.as_str()));
    }
    Some(json_ok(&usage_body(tenant, from, to, &rows)))
}

/// The admin usage routes (the admin credential was checked by `route`):
/// `GET /v1/admin/usage?tenant_id=&from=&to=&raw=true` and
/// `POST /v1/admin/usage/reconcile {"tenant_id"?}`. `None` for any other
/// request.
pub(crate) fn route_admin(request_line: &str, body: &[u8], state: &AppState) -> Option<String> {
    if bare_path(request_line, "GET") == Some("/v1/admin/usage") {
        return Some(admin_usage(request_line, state));
    }
    if bare_path(request_line, "POST") == Some("/v1/admin/usage/reconcile") {
        return Some(admin_reconcile(body, state));
    }
    None
}

fn admin_usage(request_line: &str, state: &AppState) -> String {
    let Some(recorder) = state.usage.recorder() else {
        return disabled();
    };
    let params = query_params(request_line);
    let Some(tenant) = params.get("tenant_id").filter(|t| !t.is_empty()) else {
        return json_error(400, "missing 'tenant_id'");
    };
    let (from, to) = match time_range(&params) {
        Ok(range) => range,
        Err(response) => return response,
    };
    if params.get("raw").map(String::as_str) == Some("true") {
        return match recorder.store.raw(tenant, from, to) {
            Ok(events) => json_ok(&serde_json::json!({
                "tenant": tenant,
                "from": from,
                "to": to,
                "events": events,
            })),
            Err(e) => json_error(503, &format!("usage store: {e}")),
        };
    }
    match recorder.store.aggregate(tenant, from, to, true) {
        Ok(rows) => {
            let mut body = usage_body(tenant, from, to, &rows);
            body["pending_events"] = recorder.pending_len().into();
            json_ok(&body)
        }
        Err(e) => json_error(503, &format!("usage store: {e}")),
    }
}

fn admin_reconcile(body: &[u8], state: &AppState) -> String {
    let Some(recorder) = state.usage.recorder() else {
        return disabled();
    };
    let request: serde_json::Value = if body.iter().all(u8::is_ascii_whitespace) {
        serde_json::json!({})
    } else {
        match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
        }
    };
    let tenant = request.get("tenant_id").and_then(|v| v.as_str());
    if state.live_prefix_bytes("").is_none() {
        return json_error(
            409,
            "reconciliation compares against the S3 bucket; S3 storage is not enabled",
        );
    }
    let snapshots = match recorder.store.latest(tenant, Meter::StorageBytes) {
        Ok(s) => s,
        Err(e) => return json_error(503, &format!("usage store: {e}")),
    };
    // Namespaces that exist now but may never have been metered.
    let also: Vec<(String, String)> = state
        .controller
        .lock()
        .unwrap()
        .list_namespaces()
        .into_iter()
        .filter(|ns| kosha_core::sharding::shard_index_of(ns).is_none())
        .map(|ns: NamespaceId| split_namespace(&ns.0))
        .filter(|(t, _)| tenant.is_none_or(|want| want == t))
        .collect();
    let tolerance = recorder.cfg.reconcile_tolerance;
    let (drifts, errors) = reconcile(
        &snapshots,
        &also,
        |ns| state.live_prefix_bytes(ns).unwrap_or(Ok(0)),
        tolerance,
    );
    for d in &drifts {
        state
            .metrics
            .set_usage_reconcile_drift(&d.tenant, &d.namespace, d.drift_ratio);
    }
    let outside: Vec<&Drift> = drifts.iter().filter(|d| !d.within_tolerance).collect();
    json_ok(&serde_json::json!({
        "tolerance": tolerance,
        "checked": drifts.len(),
        "outside_tolerance": outside,
        "namespaces": drifts,
        "errors": errors
            .iter()
            .map(|(ns, e)| serde_json::json!({"namespace": ns, "error": e}))
            .collect::<Vec<_>>(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const W: i64 = 60;

    fn recorder(store: Arc<dyn UsageStore>) -> Recorder {
        Recorder::with_source(
            store,
            UsageConfig {
                window_secs: W,
                snapshot_secs: 3600,
                reconcile_tolerance: 0.02,
            },
            "pod-a",
            "pod-a#1",
        )
    }

    /// A store that fails while `failing` is set.
    #[derive(Default)]
    struct FlakyStore {
        inner: MemoryUsageStore,
        failing: std::sync::atomic::AtomicBool,
        calls: AtomicU64,
    }

    impl UsageStore for FlakyStore {
        fn append(&self, events: &[UsageEvent]) -> Result<usize, kosha_core::KoshaError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if self.failing.load(Ordering::Relaxed) {
                return Err(kosha_core::KoshaError::ControlPlane("down".into()));
            }
            self.inner.append(events)
        }
        fn aggregate(
            &self,
            tenant: &str,
            from: i64,
            to: i64,
            group: bool,
        ) -> Result<Vec<UsageRow>, kosha_core::KoshaError> {
            self.inner.aggregate(tenant, from, to, group)
        }
        fn raw(
            &self,
            tenant: &str,
            from: i64,
            to: i64,
        ) -> Result<Vec<UsageEvent>, kosha_core::KoshaError> {
            self.inner.raw(tenant, from, to)
        }
        fn latest(
            &self,
            tenant: Option<&str>,
            meter: Meter,
        ) -> Result<Vec<UsageEvent>, kosha_core::KoshaError> {
            self.inner.latest(tenant, meter)
        }
    }

    fn total(store: &dyn UsageStore, tenant: &str, meter: Meter) -> u64 {
        store
            .aggregate(tenant, i64::MIN / 2, i64::MAX / 2, false)
            .unwrap()
            .iter()
            .filter(|r| r.meter == meter)
            .map(|r| r.quantity)
            .sum()
    }

    #[test]
    fn namespaces_split_by_tenant() {
        assert_eq!(split_namespace("acme/docs"), ("acme".into(), "docs".into()));
        assert_eq!(
            split_namespace("bare"),
            (LEGACY_TENANT.into(), "bare".into())
        );
        for ns in ["acme/docs", "bare"] {
            let (t, n) = split_namespace(ns);
            assert_eq!(join_namespace(&t, &n), ns);
        }
    }

    #[test]
    fn windows_seal_only_once_they_end() {
        let store = Arc::new(MemoryUsageStore::new());
        let r = recorder(store.clone());
        r.record_at(
            120,
            "acme/docs",
            &[(Meter::WriteBytes, 10), (Meter::WriteDocs, 1)],
        );
        r.record_at(
            150,
            "acme/docs",
            &[(Meter::WriteBytes, 5), (Meter::WriteDocs, 2)],
        );
        r.record_at(185, "acme/docs", &[(Meter::Queries, 1)]);
        r.record_at(185, "globex/x", &[(Meter::Queries, 4)]);
        // Window [120, 180) has not ended at 179.
        assert!(r.seal_at(179, false).is_empty());
        let sealed = r.seal_at(181, false);
        let mut got: Vec<(Meter, u64, i64, i64)> = sealed
            .iter()
            .map(|e| (e.meter, e.quantity, e.window_start, e.window_end))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                (Meter::WriteBytes, 15, 120, 180),
                (Meter::WriteDocs, 3, 120, 180)
            ]
        );
        assert!(sealed
            .iter()
            .all(|e| e.tenant == "acme" && e.namespace == "docs" && e.source_pod == "pod-a#1"));
        // Zero meters are not emitted; the open window is untouched.
        assert!(r.seal_at(181, false).is_empty());
        let rest = r.seal_at(250, false);
        assert_eq!(rest.len(), 2);
        assert!(rest.iter().all(|e| e.meter == Meter::Queries));
    }

    #[test]
    fn a_late_record_for_a_sealed_window_lands_in_the_open_one() {
        let store = Arc::new(MemoryUsageStore::new());
        let r = recorder(store.clone());
        r.record_at(10, "acme/docs", &[(Meter::Queries, 1)]);
        let first = r.seal_at(61, false);
        assert_eq!(first.len(), 1);
        // A request that started at t=59 finishes after the seal.
        r.record_at(59, "acme/docs", &[(Meter::Queries, 1)]);
        let second = r.seal_at(121, false);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].window_start, 60);
        assert_ne!(first[0].event_id, second[0].event_id);
        store.append(&first).unwrap();
        store.append(&second).unwrap();
        assert_eq!(total(store.as_ref(), "acme", Meter::Queries), 2);
    }

    #[test]
    fn event_ids_are_deterministic_per_source_and_window() {
        let a = recorder(Arc::new(MemoryUsageStore::new()));
        let b = recorder(Arc::new(MemoryUsageStore::new()));
        for r in [&a, &b] {
            r.record_at(5, "acme/docs", &[(Meter::Queries, 1)]);
        }
        assert_eq!(a.seal_at(60, false), b.seal_at(60, false));
    }

    #[test]
    fn a_failed_flush_is_retried_without_double_counting() {
        let store = Arc::new(FlakyStore::default());
        let r = recorder(store.clone());
        r.record_at(0, "acme/docs", &[(Meter::WriteDocs, 7)]);
        store.failing.store(true, Ordering::Relaxed);
        assert!(r.flush_at(61, false).is_err());
        assert_eq!(r.pending_len(), 1);
        r.record_at(70, "acme/docs", &[(Meter::WriteDocs, 3)]);
        store.failing.store(false, Ordering::Relaxed);
        assert_eq!(r.flush_at(121, false).unwrap(), 2);
        assert_eq!(r.pending_len(), 0);
        assert_eq!(total(store.as_ref(), "acme", Meter::WriteDocs), 10);
        // A timeout that did commit, then a retry of the same events.
        let replay = store.inner.all();
        assert_eq!(store.append(&replay).unwrap(), 0);
        assert_eq!(total(store.as_ref(), "acme", Meter::WriteDocs), 10);
    }

    #[test]
    fn shutdown_flush_seals_the_open_window() {
        let store = Arc::new(MemoryUsageStore::new());
        let r = recorder(store.clone());
        r.record_at(30, "acme/docs", &[(Meter::Queries, 2)]);
        assert_eq!(r.flush_at(31, false).unwrap(), 0);
        assert_eq!(r.flush_at(31, true).unwrap(), 1);
        assert_eq!(total(store.as_ref(), "acme", Meter::Queries), 2);
        // Anything after shutdown's seal goes to a later window.
        r.record_at(40, "acme/docs", &[(Meter::Queries, 1)]);
        assert_eq!(r.flush_at(200, false).unwrap(), 1);
        assert_eq!(total(store.as_ref(), "acme", Meter::Queries), 3);
    }

    #[test]
    fn concurrent_records_are_all_counted() {
        let store = Arc::new(MemoryUsageStore::new());
        let r = Arc::new(recorder(store.clone()));
        std::thread::scope(|s| {
            for t in 0..8 {
                let r = Arc::clone(&r);
                s.spawn(move || {
                    for i in 0..1000 {
                        let ns = if i % 2 == 0 { "acme/a" } else { "acme/b" };
                        r.record_at(t * 7 + i % 200, ns, &[(Meter::Queries, 1)]);
                    }
                });
            }
            // Flushing while recording loses nothing.
            let r = Arc::clone(&r);
            s.spawn(move || {
                for now in [60, 120, 180] {
                    r.flush_at(now, false).unwrap();
                }
            });
        });
        r.flush_at(10_000, true).unwrap();
        assert_eq!(total(store.as_ref(), "acme", Meter::Queries), 8000);
    }

    #[test]
    fn snapshots_are_hourly_and_pod_independent() {
        let store = Arc::new(MemoryUsageStore::new());
        let a = recorder(store.clone());
        let b = Recorder::with_source(store.clone(), a.cfg.clone(), "pod-b", "pod-b#9");
        let entries = vec![("acme/docs".to_string(), 1000, 64 * 10)];
        a.snapshot_at(3600 + 5, &entries, true);
        b.snapshot_at(3600 + 900, &entries, true);
        a.flush_at(3700, false).unwrap();
        b.flush_at(3700, false).unwrap();
        let latest = store.latest(Some("acme"), Meter::StorageBytes).unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!((latest[0].quantity, latest[0].window_start), (1000, 3600));
        assert_eq!(total(store.as_ref(), "acme", Meter::VectorDimDocs), 640);
        // The scanned approximation follows the snapshot.
        assert_eq!(a.namespace_bytes("acme/docs"), 1000);
        // Deletion: a final zero becomes the latest.
        a.namespace_deleted_at(3800, "acme/docs");
        a.flush_at(3801, false).unwrap();
        let latest = store.latest(Some("acme"), Meter::StorageBytes).unwrap();
        assert_eq!(latest[0].quantity, 0);
        assert_eq!(a.namespace_bytes("acme/docs"), 0);
        // A non-emitting pod only refreshes its approximation.
        let q = recorder(Arc::new(MemoryUsageStore::new()));
        q.snapshot_at(0, &entries, false);
        assert_eq!(q.pending_len(), 0);
        assert_eq!(q.namespace_bytes("acme/docs"), 1000);
    }

    #[test]
    fn query_meters_only_successful_responses() {
        let store = Arc::new(MemoryUsageStore::new());
        let rec = UsageRecorder::new(recorder(store.clone()));
        rec.recorder()
            .unwrap()
            .snapshot_at(0, &[("acme/docs".into(), 500, 0)], false);
        let ok = crate::json_ok(&serde_json::json!({"results": []}));
        let body_len = ok.split_once("\r\n\r\n").unwrap().1.len() as u64;
        assert_eq!(rec.query("acme/docs", ok.clone(), true), ok);
        rec.query("acme/docs", ok, false);
        rec.query("acme/docs", crate::json_error(400, "bad"), true);
        rec.recorder()
            .unwrap()
            .flush_at(i64::MAX / 4, true)
            .unwrap();
        assert_eq!(total(store.as_ref(), "acme", Meter::Queries), 2);
        assert_eq!(
            total(store.as_ref(), "acme", Meter::QueryBytesReturned),
            2 * body_len
        );
        assert_eq!(total(store.as_ref(), "acme", Meter::QueryBytesScanned), 500);
        // Inert: nothing happens, the response passes through.
        let inert = UsageRecorder::inert();
        assert_eq!(inert.query("acme/docs", "x".into(), true), "x");
        inert.record_write("acme/docs", 1, 1);
        inert.record_namespace_deleted("acme/docs");
        inert.shutdown();
    }

    #[test]
    fn reconcile_reports_drift_against_a_fake_listing() {
        let snap = |tenant: &str, ns: &str, q: u64| UsageEvent {
            event_id: format!("{tenant}/{ns}"),
            tenant: tenant.into(),
            namespace: ns.into(),
            meter: Meter::StorageBytes,
            quantity: q,
            window_start: 3600,
            window_end: 7200,
            source_pod: "p".into(),
        };
        let snapshots = vec![
            snap("acme", "exact", 1000),
            snap("acme", "close", 1015), // +1.5%
            snap("acme", "over", 1100),  // +10%
            snap("acme", "under", 900),  // -10%
            snap("acme", "gone", 0),     // deleted, nothing left
            snap("acme", "leaked", 0),   // metered zero, bytes remain
            snap(LEGACY_TENANT, "bare", 50),
            snap("acme", "broken", 10),
        ];
        let live: HashMap<&str, u64> = [
            ("acme/exact", 1000),
            ("acme/close", 1000),
            ("acme/over", 1000),
            ("acme/under", 1000),
            ("acme/gone", 0),
            ("acme/leaked", 400),
            ("bare", 50),
            ("acme/new", 300),
        ]
        .into_iter()
        .collect();
        let also = vec![("acme".to_string(), "new".to_string())];
        let (drifts, errors) = reconcile(
            &snapshots,
            &also,
            |ns| {
                live.get(ns)
                    .copied()
                    .ok_or_else(|| "list failed".to_string())
            },
            0.02,
        );
        assert_eq!(
            errors,
            vec![("acme/broken".to_string(), "list failed".to_string())]
        );
        let by: HashMap<String, &Drift> = drifts
            .iter()
            .map(|d| (join_namespace(&d.tenant, &d.namespace), d))
            .collect();
        let check = |ns: &str, ratio: f64, ok: bool| {
            let d = by[ns];
            assert!((d.drift_ratio - ratio).abs() < 1e-9, "{ns}: {d:?}");
            assert_eq!(d.within_tolerance, ok, "{ns}: {d:?}");
        };
        check("acme/exact", 0.0, true);
        check("acme/close", 0.015, true);
        check("acme/over", 0.1, false);
        check("acme/under", -0.1, false);
        check("acme/gone", 0.0, true);
        check("acme/leaked", -1.0, false);
        check("bare", 0.0, true);
        // Never metered: everything live is drift.
        check("acme/new", -1.0, false);
        assert_eq!(by["acme/new"].metered_at, None);
        assert_eq!(by["acme/under"].drift_bytes, -100);
        assert_eq!(drifts.len(), 8);
    }

    #[test]
    fn tolerance_and_time_parsing() {
        assert_eq!(parse_tolerance(None), 0.02);
        assert_eq!(parse_tolerance(Some("0.05")), 0.05);
        assert!((parse_tolerance(Some("5%")) - 0.05).abs() < 1e-12);
        assert_eq!(parse_tolerance(Some("lots")), 0.02);
        assert_eq!(parse_tolerance(Some("-1")), 0.02);
        assert_eq!(parse_time("1767225600"), Some(1_767_225_600));
        assert_eq!(parse_time("2026-01-01"), Some(1_767_225_600));
        assert_eq!(parse_time("2026-01-01T00:00:00Z"), Some(1_767_225_600));
        assert_eq!(parse_time("soon"), None);
        assert_eq!(month_start(1_767_225_600 + 20 * 86_400 + 5), 1_767_225_600);
    }

    #[test]
    fn usage_body_groups_per_namespace() {
        let rows = vec![
            UsageRow {
                namespace: Some("a".into()),
                meter: Meter::Queries,
                quantity: 3,
                events: 1,
            },
            UsageRow {
                namespace: Some("b".into()),
                meter: Meter::Queries,
                quantity: 4,
                events: 2,
            },
            UsageRow {
                namespace: Some("b".into()),
                meter: Meter::WriteBytes,
                quantity: 10,
                events: 1,
            },
        ];
        let body = usage_body("acme", 0, 10, &rows);
        assert_eq!(body["totals"]["queries"], 7);
        assert_eq!(body["namespaces"][1]["namespace"], "b");
        assert_eq!(body["namespaces"][1]["meters"]["write_bytes"], 10);
    }

    #[test]
    fn hooks_meter_writes_queries_and_deletes_end_to_end() {
        let mut state = AppState::new(crate::tests::test_dir());
        let store = Arc::new(MemoryUsageStore::new());
        state.usage = UsageRecorder::new(recorder(store.clone()));
        state.admin_auth_key = Some("admin-key".into());
        let state = state;
        let none = HashMap::new();
        let admin: HashMap<String, String> =
            [("authorization".to_string(), "Bearer admin-key".to_string())]
                .into_iter()
                .collect();
        let call = |line: &str, body: &[u8], tenant: &str, headers: &HashMap<String, String>| {
            crate::serve_tenant_request(line, headers, body, tenant, &state)
        };
        let docs = serde_json::to_vec(&serde_json::json!({
            "documents": [
                {"id": "d1", "fields": [{"name": "title", "field_type": "Text", "value": "quick brown fox"}]},
                {"id": "d2", "fields": [{"name": "title", "field_type": "Text", "value": "lazy dog"}]}
            ]
        }))
        .unwrap();
        let r = call(
            "POST /v1/namespaces/books/documents HTTP/1.1",
            &docs,
            "acme",
            &none,
        );
        assert!(r.starts_with("HTTP/1.1 200"), "{r}");
        state
            .indexer
            .flush_namespace(&NamespaceId("acme/books".into()))
            .unwrap();
        let search = br#"{"query_text": "quick", "max_results": 5}"#;
        let r = call(
            "POST /v1/namespaces/books/search HTTP/1.1",
            search,
            "acme",
            &none,
        );
        assert!(r.starts_with("HTTP/1.1 200"), "{r}");
        let search_bytes = r.split_once("\r\n\r\n").unwrap().1.len() as u64;
        let r = call(
            "POST /v1/namespaces/books/exists HTTP/1.1",
            br#"{"ids": ["d1", "zz"]}"#,
            "acme",
            &none,
        );
        assert!(r.starts_with("HTTP/1.1 200"), "{r}");
        // A failed search is not metered.
        let r = call(
            "POST /v1/namespaces/books/search HTTP/1.1",
            b"not json",
            "acme",
            &none,
        );
        assert!(!r.starts_with("HTTP/1.1 200"), "{r}");

        let recorder = state.usage.recorder().unwrap();
        recorder.flush_at(unix_now(), true).unwrap();
        let q = |meter| total(store.as_ref(), "acme", meter);
        assert_eq!(q(Meter::WriteDocs), 2);
        assert!(q(Meter::WriteBytes) >= docs.len() as u64);
        assert_eq!(q(Meter::Queries), 2);
        assert!(q(Meter::QueryBytesReturned) > search_bytes);

        // Writer-pod snapshot: local segment bytes for the namespace.
        let entries = state.usage_snapshot_entries();
        let books = entries
            .iter()
            .find(|(ns, _, _)| ns == "acme/books")
            .unwrap();
        assert!(books.1 > 0, "{entries:?}");

        // The tenant API, scoped to the caller.
        let r = call("GET /v1/usage?from=0 HTTP/1.1", b"", "acme", &none);
        assert!(r.starts_with("HTTP/1.1 200"), "{r}");
        let body = crate::response_body_json(&r);
        assert_eq!(body["tenant"], "acme");
        assert_eq!(body["namespaces"][0]["namespace"], "books");
        assert_eq!(body["namespaces"][0]["meters"]["write_docs"], 2);
        assert_eq!(body["totals"]["queries"], 2);
        let r = call("GET /v1/usage?from=0 HTTP/1.1", b"", "globex", &none);
        let body = crate::response_body_json(&r);
        assert_eq!(body["namespaces"].as_array().unwrap().len(), 0, "{body}");
        let r = call(
            "GET /v1/usage?from=0&namespace=other HTTP/1.1",
            b"",
            "acme",
            &none,
        );
        let body = crate::response_body_json(&r);
        assert_eq!(body["namespaces"].as_array().unwrap().len(), 0, "{body}");
        let r = call("GET /v1/usage?from=soon HTTP/1.1", b"", "acme", &none);
        assert!(r.starts_with("HTTP/1.1 400"), "{r}");

        // Admin: aggregates and raw events; the admin credential is required.
        let r = call(
            "GET /v1/admin/usage?tenant_id=acme&from=0 HTTP/1.1",
            b"",
            "acme",
            &none,
        );
        assert!(r.starts_with("HTTP/1.1 403"), "{r}");
        let r = call(
            "GET /v1/admin/usage?tenant_id=acme&from=0 HTTP/1.1",
            b"",
            crate::SERVICE_TENANT,
            &admin,
        );
        assert!(r.starts_with("HTTP/1.1 200"), "{r}");
        assert_eq!(crate::response_body_json(&r)["totals"]["write_docs"], 2);
        let r = call(
            "GET /v1/admin/usage?tenant_id=acme&from=0&raw=true HTTP/1.1",
            b"",
            crate::SERVICE_TENANT,
            &admin,
        );
        let raw = crate::response_body_json(&r);
        let events: Vec<UsageEvent> = serde_json::from_value(raw["events"].clone()).unwrap();
        let rows = kosha_control::usage::aggregate_events(&events, true);
        assert_eq!(
            rows.iter()
                .filter(|r| r.meter == Meter::Queries)
                .map(|r| r.quantity)
                .sum::<u64>(),
            2
        );
        // No S3 in this build of the state: reconciliation says so.
        let r = call(
            "POST /v1/admin/usage/reconcile HTTP/1.1",
            b"{}",
            crate::SERVICE_TENANT,
            &admin,
        );
        assert!(r.starts_with("HTTP/1.1 409"), "{r}");

        // Deleting the namespace leaves a zero storage snapshot as latest.
        let r = call("DELETE /v1/namespaces/books HTTP/1.1", b"", "acme", &none);
        assert!(r.starts_with("HTTP/1.1 200"), "{r}");
        recorder.flush_at(unix_now(), false).unwrap();
        let latest = store.latest(Some("acme"), Meter::StorageBytes).unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(
            (latest[0].namespace.as_str(), latest[0].quantity),
            ("books", 0)
        );
    }

    #[test]
    fn inert_recorder_answers_404_on_the_usage_api() {
        let state = AppState::new(crate::tests::test_dir());
        let r = route_tenant("GET /v1/usage HTTP/1.1", "acme", &state).unwrap();
        assert!(r.starts_with("HTTP/1.1 404"), "{r}");
        assert!(route_tenant("GET /v1/namespaces HTTP/1.1", "acme", &state).is_none());
        assert!(route_admin("GET /v1/admin/gc HTTP/1.1", b"", &state).is_none());
    }
}
