//! Usage metering on a pod (#348): what this process emits, how it gets to
//! the usage store, and the read API. Off unless `KOSHA_USAGE_METERING=on`.
//!
//! # Meters
//!
//! - A successful search (`observe_search`) emits `queries = 1`,
//!   `bytes_returned` (the response body) and `bytes_fetched` (what it
//!   pulled from object storage, whole files and ranges alike). A response
//!   served from the result cache emits the same event with
//!   `bytes_fetched = 0`.
//! - An accepted document write (`/documents`, legacy `/index` and
//!   `/replace`) emits `bytes_written` and `documents_written`, on the pod
//!   that executed it (a query pod forwards writes and emits nothing).
//! - A pod that accepts writes snapshots each namespace's `stored_bytes`
//!   once an hour ([`run_snapshot_tick`]).
//!
//! The billable "bytes scanned" meter is not emitted per search. Bytes
//! fetched depend on what happened to be cached, and nothing in the engine
//! counts the logical bytes a query touched, so a per-search number would
//! be either cache-dependent or made up. The summary derives
//! `bytes_queried` instead: each search is priced at its namespace's stored
//! bytes at the time (`kosha_control::usage::summarize`), which is the same
//! on the whole-file, ranged-read and fully cached paths.
//!
//! # Delivery
//!
//! A request appends its event to a spool file under `KOSHA_USAGE_SPOOL_DIR`
//! (one `write` of one JSON line, no fsync) and returns. A background thread
//! seals the file every `KOSHA_USAGE_FLUSH_MS` and sends sealed files to the
//! store, deleting each one only after the store took it. The store ignores
//! `(event id, meter)` pairs it already has, so a file sent twice (a crash
//! between the insert and the delete) counts once.
//!
//! What survives what:
//! - Store unreachable: events wait in the spool, up to
//!   `KOSHA_USAGE_SPOOL_MAX_BYTES`; beyond that new events are dropped and
//!   counted (`dropped_events` in `GET /v1/admin/usage`).
//! - Process crash or restart on the same disk: files left by the old
//!   process are adopted and sent at startup.
//! - Host crash, or a pod replaced with an empty disk: events appended
//!   since the last flush are lost (at most one flush interval, plus
//!   whatever was waiting on an unreachable store).
//!
//! The spool directory belongs to one running process. A process adopts
//! every `active-*` file it finds at startup as a dead predecessor's, so two
//! live processes on one directory would each send and delete the file the
//! other is still appending to, losing those appends.

use super::*;
use kosha_control::usage::{
    meter, usage_summary, MemoryUsageStore, NamespaceUsage, UsageEvent, UsageStore, HOUR_MS,
};
use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_FLUSH_MS: u64 = 5_000;
const DEFAULT_SNAPSHOT_TICK_MS: u64 = 300_000;
const DEFAULT_SPOOL_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// Default window of the read API, and its upper bound.
const DEFAULT_WINDOW_MS: u64 = 30 * 24 * HOUR_MS;
const MAX_WINDOW_MS: u64 = 366 * 24 * HOUR_MS;

/// Directory under the data dir the spool defaults to. `kosha_cache`
/// leaves it alone (it is not cache content), like `_wal`.
pub(crate) const SPOOL_DIR_NAME: &str = "_usage";

/// `KOSHA_USAGE_METERING`: on only for `on`, `true` or `1`.
fn metering_enabled(raw: Option<&str>) -> bool {
    matches!(
        raw.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("on" | "true" | "1")
    )
}

/// A millisecond or byte setting: unset, empty or unparsable means the
/// default.
fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// Append-only local buffer of events not yet in the store.
///
/// One `active-<nonce>.jsonl` receives appends. Sealing renames it to
/// `sealed-<nonce>-<seq>.jsonl`, which is never written again, so a sealed
/// file can be read and sent without holding the append lock.
struct Spool {
    dir: PathBuf,
    max_bytes: u64,
    nonce: String,
    inner: Mutex<SpoolInner>,
}

#[derive(Default)]
struct SpoolInner {
    active: Option<std::fs::File>,
    active_bytes: u64,
    sealed_bytes: u64,
    next_seal: u64,
}

impl Spool {
    /// Open the spool, adopting whatever an earlier process left: its
    /// sealed files as they are, and its active file sealed as it stands.
    fn open(dir: PathBuf, max_bytes: u64, nonce: String) -> Self {
        if let Err(e) = std::fs::create_dir_all(&dir) {
            eprintln!(
                "WARN: usage metering: cannot create spool dir {}: {e}",
                dir.display()
            );
        }
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                let Some(stem) = name
                    .strip_prefix("active-")
                    .and_then(|rest| rest.strip_suffix(".jsonl"))
                else {
                    continue;
                };
                let sealed = dir.join(format!("sealed-{stem}-final.jsonl"));
                if let Err(e) = std::fs::rename(entry.path(), &sealed) {
                    eprintln!("WARN: usage metering: cannot adopt spool file {name}: {e}");
                }
            }
        }
        let spool = Self {
            dir,
            max_bytes,
            nonce,
            inner: Mutex::new(SpoolInner::default()),
        };
        let adopted: u64 = spool
            .sealed_files()
            .iter()
            .filter_map(|path| std::fs::metadata(path).ok())
            .map(|meta| meta.len())
            .sum();
        spool.inner.lock().unwrap().sealed_bytes = adopted;
        spool
    }

    fn active_path(&self) -> PathBuf {
        self.dir.join(format!("active-{}.jsonl", self.nonce))
    }

    /// Append one line. `false` when the spool is full or the disk refused
    /// the write: the event is dropped.
    fn append(&self, line: &[u8]) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let len = line.len() as u64;
        if inner.active_bytes + inner.sealed_bytes + len > self.max_bytes {
            return false;
        }
        if inner.active.is_none() {
            let opened = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.active_path());
            match opened {
                Ok(file) => inner.active = Some(file),
                Err(_) => return false,
            }
        }
        let written = inner
            .active
            .as_mut()
            .is_some_and(|file| file.write_all(line).is_ok());
        if !written {
            // A partial line may be on disk; the reader skips a line it
            // cannot parse. Seal what is there and start a new file.
            self.seal_locked(&mut inner);
            return false;
        }
        inner.active_bytes += len;
        true
    }

    fn seal_locked(&self, inner: &mut SpoolInner) {
        let Some(file) = inner.active.take() else {
            return;
        };
        drop(file);
        let sealed = self.dir.join(format!(
            "sealed-{}-{:08}.jsonl",
            self.nonce, inner.next_seal
        ));
        inner.next_seal += 1;
        match std::fs::rename(self.active_path(), &sealed) {
            Ok(()) => {
                inner.sealed_bytes += inner.active_bytes;
                inner.active_bytes = 0;
            }
            // Left in place: appends continue into it, and the next seal
            // (or the next process) picks it up.
            Err(e) => eprintln!("WARN: usage metering: cannot seal spool file: {e}"),
        }
    }

    /// Close the active file so the flusher can send it.
    fn seal(&self) {
        self.seal_locked(&mut self.inner.lock().unwrap());
    }

    /// Sealed files, oldest name first.
    fn sealed_files(&self) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(&self.dir)
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("sealed-") && name.ends_with(".jsonl"))
            })
            .collect();
        files.sort();
        files
    }

    /// A sealed file of `len` bytes was delivered and removed.
    fn released(&self, len: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.sealed_bytes = inner.sealed_bytes.saturating_sub(len);
    }

    fn bytes(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.active_bytes + inner.sealed_bytes
    }
}

/// What one [`UsageMeter::flush`] delivered.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct FlushReport {
    pub files: usize,
    pub events: usize,
    /// Meter rows the store did not already have.
    pub stored_rows: u64,
    /// Lines that did not parse (a write cut short by a crash).
    pub malformed_lines: usize,
}

/// Settings of a [`UsageMeter`].
pub(crate) struct UsageConfig {
    pub spool_dir: PathBuf,
    pub spool_max_bytes: u64,
    pub flush_interval: Duration,
    pub snapshot_tick: Duration,
}

impl UsageConfig {
    /// Defaults with the spool under `data_dir`.
    pub(crate) fn under(data_dir: &Path) -> Self {
        Self {
            spool_dir: data_dir.join(SPOOL_DIR_NAME),
            spool_max_bytes: DEFAULT_SPOOL_MAX_BYTES,
            flush_interval: Duration::from_millis(DEFAULT_FLUSH_MS),
            snapshot_tick: Duration::from_millis(DEFAULT_SNAPSHOT_TICK_MS),
        }
    }

    fn from_env(data_dir: &Path) -> Self {
        let mut config = Self::under(data_dir);
        if let Some(dir) = std::env::var("KOSHA_USAGE_SPOOL_DIR")
            .ok()
            .filter(|dir| !dir.trim().is_empty())
        {
            config.spool_dir = PathBuf::from(dir);
        }
        config.spool_max_bytes = env_u64("KOSHA_USAGE_SPOOL_MAX_BYTES", DEFAULT_SPOOL_MAX_BYTES);
        config.flush_interval =
            Duration::from_millis(env_u64("KOSHA_USAGE_FLUSH_MS", DEFAULT_FLUSH_MS).max(100));
        config.snapshot_tick = Duration::from_millis(
            env_u64("KOSHA_USAGE_SNAPSHOT_TICK_MS", DEFAULT_SNAPSHOT_TICK_MS).max(1_000),
        );
        config
    }
}

/// This pod's usage emitter: the spool, the store it drains into, and the
/// counters the admin read reports.
pub(crate) struct UsageMeter {
    store: Arc<dyn UsageStore>,
    spool: Spool,
    /// One flush at a time: the flusher thread, a read, and shutdown.
    flush_lock: Mutex<()>,
    /// Unique per process; event ids are `<prefix>-<n>`.
    id_prefix: String,
    next_id: AtomicU64,
    flush_interval: Duration,
    snapshot_tick: Duration,
    /// Root namespace -> the hour this process last snapshotted it.
    snapshotted: Mutex<HashMap<String, u64>>,
    spooled_events: AtomicU64,
    dropped_events: AtomicU64,
    delivered_events: AtomicU64,
    malformed_lines: AtomicU64,
    last_flush_error: Mutex<Option<String>>,
}

impl UsageMeter {
    pub(crate) fn new(store: Arc<dyn UsageStore>, config: UsageConfig) -> Self {
        static PROCESS_SEQ: AtomicU64 = AtomicU64::new(0);
        // Time, pid and a per-process counter: unique across restarts,
        // across pods sharing a store, and across meters in one process.
        let nonce = format!(
            "{:x}-{:x}-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
            std::process::id(),
            PROCESS_SEQ.fetch_add(1, Ordering::Relaxed),
        );
        let host = std::env::var("HOSTNAME").unwrap_or_default();
        let id_prefix = if host.is_empty() {
            nonce.clone()
        } else {
            format!("{host}-{nonce}")
        };
        Self {
            store,
            spool: Spool::open(config.spool_dir, config.spool_max_bytes, nonce),
            flush_lock: Mutex::new(()),
            id_prefix,
            next_id: AtomicU64::new(0),
            flush_interval: config.flush_interval,
            snapshot_tick: config.snapshot_tick,
            snapshotted: Mutex::new(HashMap::new()),
            spooled_events: AtomicU64::new(0),
            dropped_events: AtomicU64::new(0),
            delivered_events: AtomicU64::new(0),
            malformed_lines: AtomicU64::new(0),
            last_flush_error: Mutex::new(None),
        }
    }

    /// The meter the environment asks for, or `None` (the default) when
    /// `KOSHA_USAGE_METERING` is not on. The store is Postgres when
    /// `DATABASE_URL` is set, in-memory otherwise.
    pub(crate) fn from_env(data_dir: &Path) -> Option<Self> {
        if !metering_enabled(std::env::var("KOSHA_USAGE_METERING").ok().as_deref()) {
            return None;
        }
        let config = UsageConfig::from_env(data_dir);
        let store = store_from_env();
        println!(
            "usage metering: on (spool {}, flush every {} ms)",
            config.spool_dir.display(),
            config.flush_interval.as_millis()
        );
        Some(Self::new(store, config))
    }

    fn next_event_id(&self) -> String {
        format!(
            "{}-{}",
            self.id_prefix,
            self.next_id.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Spool one event. `false` when it was dropped (spool full or disk
    /// error); the request it describes is unaffected either way.
    fn emit(&self, event: &UsageEvent) -> bool {
        let mut line = serde_json::to_vec(event).expect("UsageEvent serializes");
        line.push(b'\n');
        if self.spool.append(&line) {
            self.spooled_events.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            if self.dropped_events.fetch_add(1, Ordering::Relaxed) == 0 {
                eprintln!(
                    "ERROR: usage metering: dropping events (spool {} full or unwritable); \
                     see dropped_events in GET /v1/admin/usage",
                    self.spool.dir.display()
                );
            }
            false
        }
    }

    /// A successful search of `ns`.
    pub(crate) fn record_query(&self, ns: &NamespaceId, bytes_returned: u64, bytes_fetched: u64) {
        let (tenant, namespace) = usage_identity(ns);
        self.emit(&UsageEvent {
            id: self.next_event_id(),
            tenant,
            namespace,
            at_ms: unix_millis(),
            meters: BTreeMap::from([
                (meter::QUERIES.to_string(), 1),
                (meter::BYTES_RETURNED.to_string(), bytes_returned),
                (meter::BYTES_FETCHED.to_string(), bytes_fetched),
            ]),
        });
    }

    /// An accepted write of `documents` documents to `ns`.
    pub(crate) fn record_write(&self, ns: &NamespaceId, bytes_written: u64, documents: u64) {
        let (tenant, namespace) = usage_identity(ns);
        self.emit(&UsageEvent {
            id: self.next_event_id(),
            tenant,
            namespace,
            at_ms: unix_millis(),
            meters: BTreeMap::from([
                (meter::BYTES_WRITTEN.to_string(), bytes_written),
                (meter::DOCUMENTS_WRITTEN.to_string(), documents),
            ]),
        });
    }

    /// Seal the spool and send every sealed file to the store, oldest
    /// first. Stops at the first file the store refuses; that file and the
    /// ones after it stay for the next flush.
    pub(crate) fn flush(&self) -> Result<FlushReport, String> {
        let _one_at_a_time = self.flush_lock.lock().unwrap();
        let result = self.flush_locked();
        let mut last = self.last_flush_error.lock().unwrap();
        match &result {
            Ok(_) => {
                if last.take().is_some() {
                    println!("usage metering: delivery recovered");
                }
            }
            Err(e) => {
                if last.is_none() {
                    eprintln!("WARN: usage metering: delivery failed, events stay spooled: {e}");
                }
                *last = Some(e.clone());
            }
        }
        result
    }

    fn flush_locked(&self) -> Result<FlushReport, String> {
        self.spool.seal();
        let mut report = FlushReport::default();
        for path in self.spool.sealed_files() {
            let data = match std::fs::read(&path) {
                Ok(data) => data,
                Err(e) => return Err(format!("read {}: {e}", path.display())),
            };
            let mut events = Vec::new();
            let mut malformed = 0;
            for line in data.split(|b| *b == b'\n').filter(|line| !line.is_empty()) {
                match serde_json::from_slice::<UsageEvent>(line) {
                    Ok(event) => events.push(event),
                    Err(_) => malformed += 1,
                }
            }
            if !events.is_empty() {
                report.stored_rows += self.store.record(&events).map_err(|e| e.to_string())?;
            }
            // Only now: a crash before this line resends the file, and the
            // store ignores what it already has.
            if let Err(e) = std::fs::remove_file(&path) {
                return Err(format!("remove {}: {e}", path.display()));
            }
            self.spool.released(data.len() as u64);
            self.delivered_events
                .fetch_add(events.len() as u64, Ordering::Relaxed);
            self.malformed_lines
                .fetch_add(malformed as u64, Ordering::Relaxed);
            report.files += 1;
            report.events += events.len();
            report.malformed_lines += malformed;
        }
        Ok(report)
    }

    #[cfg(test)]
    pub(crate) fn spool_dir(&self) -> &Path {
        &self.spool.dir
    }

    /// Bytes waiting in the spool.
    #[cfg(test)]
    pub(crate) fn spool_bytes(&self) -> u64 {
        self.spool.bytes()
    }

    /// Delivery counters of this pod, for the admin read.
    fn pipeline_json(&self) -> serde_json::Value {
        serde_json::json!({
            "spooled_events": self.spooled_events.load(Ordering::Relaxed),
            "delivered_events": self.delivered_events.load(Ordering::Relaxed),
            "dropped_events": self.dropped_events.load(Ordering::Relaxed),
            "malformed_lines": self.malformed_lines.load(Ordering::Relaxed),
            "spool_bytes": self.spool.bytes(),
            "last_flush_error": *self.last_flush_error.lock().unwrap(),
        })
    }
}

fn store_from_env() -> Arc<dyn UsageStore> {
    #[cfg(feature = "postgres")]
    if let Some(url) = std::env::var("DATABASE_URL").ok().filter(|u| !u.is_empty()) {
        let store = kosha_control::usage::PgUsageStore::new(&url)
            .unwrap_or_else(|e| fatal(&format!("usage metering: {e}")));
        println!("usage metering: store is postgres (kosha.usage_events)");
        return Arc::new(store);
    }
    println!(
        "usage metering: store is in-memory (no DATABASE_URL): usage is per pod and lost on restart"
    );
    Arc::new(MemoryUsageStore::new())
}

/// `(tenant, namespace)` an event for `ns` is recorded under: the tenant
/// prefix and the tenant-relative name, with a shard rolled up to its root.
/// A pre-#262 bare namespace has no tenant and is recorded under `""`.
pub(crate) fn usage_identity(ns: &NamespaceId) -> (String, String) {
    let root = shard_root(ns);
    match root.split_once('/') {
        Some((tenant, namespace)) => (tenant.to_string(), namespace.to_string()),
        None => (String::new(), root.to_string()),
    }
}

/// `ns` without its `/_shard-NNNN` component, if it has one.
fn shard_root(ns: &NamespaceId) -> &str {
    match kosha_core::sharding::shard_index_of(ns) {
        Some(_) => {
            ns.0.rsplit_once('/')
                .map_or(ns.0.as_str(), |(root, _)| root)
        }
        None => &ns.0,
    }
}

/// Logical bytes of a write as the client submitted it: each document's id
/// plus every field's name and value.
pub(crate) fn documents_logical_bytes(documents: &[kosha_core::Document]) -> u64 {
    documents
        .iter()
        .map(|doc| {
            doc.id.0.len() as u64
                + doc
                    .fields
                    .iter()
                    .map(|field| (field.name.len() + field.value.len()) as u64)
                    .sum::<u64>()
        })
        .sum()
}

/// Bytes of the objects of every segment `ns`'s manifest references and
/// `ns` owns (a segment a branch inherits belongs to its parent). Object
/// storage is authoritative; local size only for a segment it does not
/// list (local-only mode, or a flush not uploaded yet). `None` when the
/// manifest is not loaded or the listing failed: no snapshot is better
/// than a wrong one.
fn namespace_stored_bytes(state: &AppState, ns: &NamespaceId) -> Option<u64> {
    let manifest = state.indexer.manifest_shared(ns)?;
    #[cfg_attr(not(feature = "s3"), allow(unused_mut))]
    let mut remote: HashMap<String, u64> = HashMap::new();
    #[cfg(feature = "s3")]
    if let Some(ref s3) = state.s3_storage {
        match s3.segment_sizes(&ns.0, |_| true) {
            Ok(sizes) => remote = sizes,
            Err(e) => {
                eprintln!(
                    "WARN: usage metering: size listing failed for {}: {e}",
                    ns.0
                );
                return None;
            }
        }
    }
    Some(
        manifest
            .segments
            .iter()
            .filter(|entry| !manifest.is_inherited_segment(ns, &entry.segment_id))
            .map(|entry| {
                remote
                    .get(&entry.segment_id.0)
                    .copied()
                    .filter(|bytes| *bytes > 0)
                    .or_else(|| {
                        local_dir_bytes(&manifest.segment_dir(
                            &state.data_dir,
                            ns,
                            &entry.segment_id,
                        ))
                    })
                    .unwrap_or(0)
            })
            .sum(),
    )
}

/// Emit the `stored_bytes` snapshot of hour `now_ms / HOUR_MS` for every
/// namespace this process has not snapshotted in that hour yet. Returns
/// how many it emitted.
///
/// The event id is `stored/<tenant>/<namespace>/<hour>` and its time is the
/// start of the hour, so a second snapshot of the same hour (this pod after
/// a restart, or another writer) is ignored by the store. A namespace
/// created mid-hour gets its first snapshot at the next tick. One object
/// listing per namespace per hour.
pub(crate) fn run_snapshot_tick(state: &AppState, now_ms: u64) -> usize {
    let Some(meter) = state.usage.as_ref() else {
        return 0;
    };
    let hour = now_ms / HOUR_MS;
    let mut roots: BTreeMap<String, Vec<NamespaceId>> = BTreeMap::new();
    for ns in state.indexer.namespaces() {
        roots
            .entry(shard_root(&ns).to_string())
            .or_default()
            .push(ns);
    }
    meter
        .snapshotted
        .lock()
        .unwrap()
        .retain(|root, at| *at == hour && roots.contains_key(root));
    let mut emitted = 0;
    for (root, members) in roots {
        if meter.snapshotted.lock().unwrap().contains_key(&root) {
            continue;
        }
        let Some(bytes) = members
            .iter()
            .map(|ns| namespace_stored_bytes(state, ns))
            .sum::<Option<u64>>()
        else {
            continue; // retried next tick
        };
        let (tenant, namespace) = usage_identity(&NamespaceId(root.clone()));
        let spooled = meter.emit(&UsageEvent {
            id: format!("stored/{root}/{hour}"),
            tenant,
            namespace,
            at_ms: hour * HOUR_MS,
            meters: BTreeMap::from([(meter::STORED_BYTES.to_string(), bytes)]),
        });
        if spooled {
            meter.snapshotted.lock().unwrap().insert(root, hour);
            emitted += 1;
        }
    }
    emitted
}

impl AppState {
    /// Start the thread that drains the usage spool into the store.
    pub(crate) fn spawn_usage_flusher(self: &Arc<Self>) {
        let Some(interval) = self.usage.as_ref().map(|meter| meter.flush_interval) else {
            return;
        };
        let state = Arc::clone(self);
        std::thread::Builder::new()
            .name("kosha-usage-flush".into())
            .spawn(move || loop {
                if let Some(meter) = state.usage.as_ref() {
                    // Failures are logged by `flush` and retried next tick.
                    let _ = meter.flush();
                }
                std::thread::sleep(interval);
            })
            .expect("failed to spawn usage flush thread");
    }

    /// Start the hourly stored-bytes snapshots, on a pod that accepts
    /// writes: it holds every namespace's current manifest.
    pub(crate) fn spawn_usage_snapshotter(self: &Arc<Self>) {
        let Some(tick) = self.usage.as_ref().map(|meter| meter.snapshot_tick) else {
            return;
        };
        if !self.role.accepts_writes() {
            return;
        }
        let state = Arc::clone(self);
        std::thread::Builder::new()
            .name("kosha-usage-snapshot".into())
            .spawn(move || loop {
                run_snapshot_tick(&state, unix_millis());
                std::thread::sleep(tick);
            })
            .expect("failed to spawn usage snapshot thread");
    }

    /// Deliver what is spooled before the process exits.
    pub(crate) fn flush_usage_on_shutdown(&self) {
        if let Some(meter) = self.usage.as_ref() {
            match meter.flush() {
                Ok(report) => println!(
                    "usage metering: delivered {} event(s) at shutdown",
                    report.events
                ),
                Err(e) => eprintln!(
                    "WARN: usage metering: events left in the spool at shutdown, \
                     sent on next start: {e}"
                ),
            }
        }
    }
}

/// A query-string parameter of a request line's target, URL-decoded.
fn query_param(request_line: &str, name: &str) -> Option<String> {
    let target = request_line.split_whitespace().nth(1)?;
    let (_, query) = target.split_once('?')?;
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| url_decode(value))
    })
}

/// `[from_ms, to_ms)` from `from` and `to` (Unix seconds). `to` defaults to
/// just after now and `from` to 30 days before `to`.
fn usage_window(request_line: &str, now_ms: u64) -> Result<(u64, u64), String> {
    let seconds = |name: &str| -> Result<Option<u64>, String> {
        match query_param(request_line, name) {
            None => Ok(None),
            Some(raw) => raw
                .parse::<u64>()
                .ok()
                .and_then(|s| s.checked_mul(1_000))
                .map(Some)
                .ok_or_else(|| format!("invalid '{name}': expected Unix seconds")),
        }
    };
    // The default end is the start of the next second, so the window
    // includes what happened a moment ago.
    let to_ms = seconds("to")?.unwrap_or((now_ms / 1_000 + 1) * 1_000);
    let from_ms = seconds("from")?.unwrap_or_else(|| to_ms.saturating_sub(DEFAULT_WINDOW_MS));
    if from_ms >= to_ms {
        return Err("'from' must be before 'to'".into());
    }
    if to_ms - from_ms > MAX_WINDOW_MS {
        return Err("window too long: at most 366 days".into());
    }
    Ok((from_ms, to_ms))
}

/// `GET /v1/usage` (the calling tenant) and `GET /v1/admin/usage` (one
/// tenant, or all of them): usage per namespace over `?from=&to=` (Unix
/// seconds, `to` exclusive), summarized from the raw events in the store.
/// An admin answer also carries this pod's delivery counters.
///
/// Events this pod has spooled are delivered first, so a single pod reads
/// its own writes; events still spooled on other pods appear after their
/// next flush.
pub(crate) fn handle_usage(
    request_line: &str,
    tenant: Option<&str>,
    admin: bool,
    state: &AppState,
) -> String {
    let Some(meter) = state.usage.as_ref() else {
        return json_error(
            404,
            "usage metering is off on this pod (KOSHA_USAGE_METERING=on enables it)",
        );
    };
    let (from_ms, to_ms) = match usage_window(request_line, unix_millis()) {
        Ok(window) => window,
        Err(e) => return json_error(400, &e),
    };
    // Best effort: a failed delivery is reported in `pipeline`, and the
    // read still answers from what the store has.
    let _ = meter.flush();
    let namespaces: Vec<NamespaceUsage> =
        match usage_summary(meter.store.as_ref(), tenant, from_ms, to_ms) {
            Ok(usage) => usage,
            Err(e) => {
                // The store's error text (driver and SQL detail) is for the
                // operator: logged, and shown on the admin route only.
                eprintln!("WARN: usage metering: read failed: {e}");
                return if admin {
                    json_error(503, &format!("usage store unavailable: {e}"))
                } else {
                    json_error(503, "usage store unavailable")
                };
            }
        };
    let mut total = NamespaceUsage::default();
    for usage in &namespaces {
        total.add(usage);
    }
    let mut body = serde_json::json!({
        "from": from_ms / 1_000,
        "to": to_ms / 1_000,
        "namespaces": namespaces,
        "total": total,
    });
    if let Some(total) = body["total"].as_object_mut() {
        total.remove("tenant");
        total.remove("namespace");
    }
    if let Some(tenant) = tenant {
        body["tenant"] = serde_json::json!(tenant);
    }
    if admin {
        body["pipeline"] = meter.pipeline_json();
    }
    json_ok(&body)
}

/// `GET /v1/admin/usage`: `?tenant=` narrows it to one tenant.
pub(crate) fn handle_admin_usage(request_line: &str, state: &AppState) -> String {
    let tenant = query_param(request_line, "tenant").filter(|t| !t.is_empty());
    handle_usage(request_line, tenant.as_deref(), true, state)
}
