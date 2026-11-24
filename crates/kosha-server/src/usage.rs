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
//! - An applied delete by query emits `documents_deleted` and
//!   `bytes_deleted` (the deleted records' bytes in the doc store), on the
//!   pod that executed it.
//! - A pod that accepts writes snapshots each namespace's `stored_bytes`
//!   and `vector_dimensions` once an hour ([`run_snapshot_tick`]).
//!
//! # Vector dimensions
//!
//! `vector_dimensions` is `dim x vectors` summed over the vector columns
//! (and `dim x tokens` over the late-interaction fields) of every segment
//! the manifest references and owns. The numbers are in each segment's
//! footer (`Footer::vector_fields`, `Footer::multi_vectors`), not in the
//! manifest, and a segment never changes once written, so each segment's
//! footer is read once (from the manifest's in-memory footer cache, the
//! local `footer.json`, or object storage, in that order) and its number
//! kept for as long as a manifest lists it. An hourly snapshot then costs
//! one map lookup per segment. The manifest and segment formats are
//! unchanged. A segment written before vector columns existed (#204) has
//! no per-column inventory in its footer and counts 0 until compaction
//! rewrites it. A footer that cannot be read skips the namespace's
//! vector snapshot for that tick (retried at the next one) without holding
//! back its `stored_bytes` snapshot: the two are separate events.
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
//!   counted (`dropped_events` in `GET /v1/admin/usage`, and
//!   `kosha_usage_events_dropped_total` on `/metrics`).
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
    /// Bytes waiting, mirrored for `/metrics` after every change.
    depth: Arc<PipelineStats>,
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
    fn open(dir: PathBuf, max_bytes: u64, nonce: String, depth: Arc<PipelineStats>) -> Self {
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
            depth,
        };
        let adopted: u64 = spool
            .sealed_files()
            .iter()
            .filter_map(|path| std::fs::metadata(path).ok())
            .map(|meta| meta.len())
            .sum();
        {
            let mut inner = spool.inner.lock().unwrap();
            inner.sealed_bytes = adopted;
            spool.mirror(&inner);
        }
        spool
    }

    fn mirror(&self, inner: &SpoolInner) {
        self.depth
            .spool_bytes
            .store(inner.active_bytes + inner.sealed_bytes, Ordering::Relaxed);
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
        self.mirror(&inner);
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
        self.mirror(&inner);
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
    /// Root namespace -> the hour this process last snapshotted its
    /// `stored_bytes`.
    snapshotted: Mutex<HashMap<String, u64>>,
    /// The same for `vector_dimensions`.
    vector_snapshotted: Mutex<HashMap<String, u64>>,
    /// Segment (logical dir) -> its vector dimensions, read once from its
    /// footer. Kept for the segments the last tick saw.
    segment_vector_dims: Mutex<HashMap<String, u64>>,
    stats: Arc<PipelineStats>,
    last_flush_error: Mutex<Option<String>>,
}

/// This pod's usage pipeline counters, shared with the `/metrics`
/// collector (`pipeline_metrics.rs`).
#[derive(Debug, Default)]
pub(crate) struct PipelineStats {
    /// Events the pod tried to spool (spooled + dropped).
    pub emitted: AtomicU64,
    pub spooled: AtomicU64,
    /// Events delivered to the store (a resent row it already had
    /// included).
    pub delivered: AtomicU64,
    pub dropped: AtomicU64,
    pub malformed_lines: AtomicU64,
    /// Flushes that failed (store or spool error).
    pub sink_errors: AtomicU64,
    /// Bytes waiting in the spool.
    pub spool_bytes: AtomicU64,
    /// Unix ms of the last flush that delivered everything; 0 never.
    pub last_flush_ok_ms: AtomicU64,
    pub snapshot_runs: AtomicU64,
    pub stored_snapshots: AtomicU64,
    pub vector_snapshots: AtomicU64,
    /// Namespace snapshots a tick could not take (listing or footer
    /// failure); retried at the next tick.
    pub snapshot_failures: AtomicU64,
    /// Segment footers read for `vector_dimensions` (cache misses).
    pub footer_reads: AtomicU64,
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
        let stats = Arc::new(PipelineStats::default());
        Self {
            store,
            spool: Spool::open(
                config.spool_dir,
                config.spool_max_bytes,
                nonce,
                Arc::clone(&stats),
            ),
            flush_lock: Mutex::new(()),
            id_prefix,
            next_id: AtomicU64::new(0),
            flush_interval: config.flush_interval,
            snapshot_tick: config.snapshot_tick,
            snapshotted: Mutex::new(HashMap::new()),
            vector_snapshotted: Mutex::new(HashMap::new()),
            segment_vector_dims: Mutex::new(HashMap::new()),
            stats,
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
        self.stats.emitted.fetch_add(1, Ordering::Relaxed);
        if self.spool.append(&line) {
            self.stats.spooled.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            if self.stats.dropped.fetch_add(1, Ordering::Relaxed) == 0 {
                eprintln!(
                    "ERROR: usage metering: dropping events (spool {} full or unwritable); \
                     see dropped_events in GET /v1/admin/usage",
                    self.spool.dir.display()
                );
            }
            false
        }
    }

    /// The pipeline counters, for `/metrics`.
    pub(crate) fn stats(&self) -> Arc<PipelineStats> {
        Arc::clone(&self.stats)
    }

    /// The store this meter delivers to (billing reads usage from it).
    pub(crate) fn store(&self) -> Arc<dyn UsageStore> {
        Arc::clone(&self.store)
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

    /// An applied delete by query of `documents` documents from `ns`,
    /// whose records held `bytes` in the doc store. Nothing is emitted for
    /// a delete that matched nothing.
    pub(crate) fn record_delete(&self, ns: &NamespaceId, documents: u64, bytes: u64) {
        if documents == 0 {
            return;
        }
        let (tenant, namespace) = usage_identity(ns);
        self.emit(&UsageEvent {
            id: self.next_event_id(),
            tenant,
            namespace,
            at_ms: unix_millis(),
            meters: BTreeMap::from([
                (meter::DOCUMENTS_DELETED.to_string(), documents),
                (meter::BYTES_DELETED.to_string(), bytes),
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
                self.stats
                    .last_flush_ok_ms
                    .store(unix_millis(), Ordering::Relaxed);
                if last.take().is_some() {
                    println!("usage metering: delivery recovered");
                }
            }
            Err(e) => {
                self.stats.sink_errors.fetch_add(1, Ordering::Relaxed);
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
            self.stats
                .delivered
                .fetch_add(events.len() as u64, Ordering::Relaxed);
            self.stats
                .malformed_lines
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
            "spooled_events": self.stats.spooled.load(Ordering::Relaxed),
            "delivered_events": self.stats.delivered.load(Ordering::Relaxed),
            "dropped_events": self.stats.dropped.load(Ordering::Relaxed),
            "malformed_lines": self.stats.malformed_lines.load(Ordering::Relaxed),
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
pub(crate) fn namespace_stored_bytes(state: &AppState, ns: &NamespaceId) -> Option<u64> {
    let manifest = state.indexer.manifest_shared(ns)?;
    owned_segment_bytes(state, ns, &manifest, false)
}

pub(crate) fn owned_segment_bytes(
    state: &AppState,
    ns: &NamespaceId,
    manifest: &kosha_core::Manifest,
    require_size: bool,
) -> Option<u64> {
    let owned = || {
        manifest
            .segments
            .iter()
            .filter(|entry| !manifest.is_inherited_segment(ns, &entry.segment_id))
    };
    if owned().next().is_none() {
        return Some(0);
    }
    #[cfg_attr(not(feature = "s3"), allow(unused_mut))]
    let mut remote: HashMap<String, u64> = HashMap::new();
    #[cfg(feature = "s3")]
    if let Some(ref s3) = state.s3_storage {
        match s3.segment_sizes(&ns.0, |_| true) {
            Ok(sizes) => remote = sizes,
            Err(e) => {
                eprintln!("WARN: segment size listing failed for {}: {e}", ns.0);
                return None;
            }
        }
    }
    owned().try_fold(0u64, |total, entry| {
        let bytes = remote
            .get(&entry.segment_id.0)
            .copied()
            .filter(|bytes| *bytes > 0)
            .or_else(|| {
                local_dir_bytes(&manifest.segment_dir(&state.data_dir, ns, &entry.segment_id))
            });
        match bytes {
            Some(bytes) => Some(total.saturating_add(bytes)),
            None if require_size => None,
            None => Some(total),
        }
    })
}

/// Vector dimensions stored by the segment `footer` describes: `dim x
/// count` per vector column plus `dim x tokens` per late-interaction field.
pub(crate) fn footer_vector_dimensions(footer: &kosha_core::Footer) -> u64 {
    let columns: u64 = footer
        .vector_fields
        .iter()
        .flatten()
        .map(|field| u64::from(field.dim).saturating_mul(u64::from(field.count)))
        .fold(0, u64::saturating_add);
    let bags: u64 = footer
        .multi_vectors
        .iter()
        .map(|field| u64::from(field.dim).saturating_mul(field.tokens))
        .fold(0, u64::saturating_add);
    columns.saturating_add(bags)
}

/// Read a segment's footer for its vector dimensions: from the manifest's
/// footer cache, the local `footer.json`, or object storage.
fn read_segment_vector_dimensions(
    state: &AppState,
    manifest: &kosha_core::Manifest,
    ns: &NamespaceId,
    segment: &kosha_core::SegmentId,
) -> Option<u64> {
    if let Some(footer) = manifest.segment_footers.get(segment) {
        return Some(footer_vector_dimensions(footer));
    }
    let local = manifest.segment_dir(&state.data_dir, ns, segment);
    if let Ok(footer) = kosha_segment::SegmentReader::read_footer(&local) {
        return Some(footer_vector_dimensions(&footer));
    }
    #[cfg(feature = "s3")]
    if let Some(ref s3) = state.s3_storage {
        let path = format!("{}/footer.json", manifest.segment_rel_dir(ns, segment));
        let footer = s3.read_remote(&path).and_then(|bytes| {
            serde_json::from_slice::<kosha_core::Footer>(&bytes).map_err(KoshaError::from)
        });
        return match footer {
            Ok(footer) => Some(footer_vector_dimensions(&footer)),
            Err(e) => {
                eprintln!("WARN: usage metering: cannot read footer {path}: {e}");
                None
            }
        };
    }
    eprintln!(
        "WARN: usage metering: cannot read the footer of {}/{}",
        ns.0, segment.0
    );
    None
}

/// `vector_dimensions` of the segments `ns`'s manifest references and
/// owns, from `cache` (segment logical dir -> dimensions, earlier ticks)
/// or the segment's footer, recording every segment it used in `seen`.
/// `None` when the manifest is not loaded or a footer could not be read.
fn namespace_vector_dimensions(
    state: &AppState,
    meter: &UsageMeter,
    ns: &NamespaceId,
    cache: &HashMap<String, u64>,
    seen: &mut HashMap<String, u64>,
) -> Option<u64> {
    let manifest = state.indexer.manifest_shared(ns)?;
    let mut total = 0u64;
    for entry in &manifest.segments {
        if manifest.is_inherited_segment(ns, &entry.segment_id) {
            continue;
        }
        let key = manifest.segment_rel_dir(ns, &entry.segment_id);
        let dims = match cache.get(&key).or_else(|| seen.get(&key)) {
            Some(dims) => *dims,
            None => {
                meter.stats.footer_reads.fetch_add(1, Ordering::Relaxed);
                read_segment_vector_dimensions(state, &manifest, ns, &entry.segment_id)?
            }
        };
        seen.insert(key, dims);
        total = total.saturating_add(dims);
    }
    Some(total)
}

/// Emit the `stored_bytes` and `vector_dimensions` snapshots of hour
/// `now_ms / HOUR_MS` for every namespace this process has not snapshotted
/// in that hour yet. Returns how many `stored_bytes` snapshots it emitted.
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
    meter.stats.snapshot_runs.fetch_add(1, Ordering::Relaxed);
    let hour = now_ms / HOUR_MS;
    let mut roots: BTreeMap<String, Vec<NamespaceId>> = BTreeMap::new();
    for ns in state.indexer.namespaces() {
        roots
            .entry(shard_root(&ns).to_string())
            .or_default()
            .push(ns);
    }
    for done in [&meter.snapshotted, &meter.vector_snapshotted] {
        done.lock()
            .unwrap()
            .retain(|root, at| *at == hour && roots.contains_key(root));
    }
    // Footers read in earlier ticks. Only the segments this tick sees are
    // kept, so a segment no manifest lists any more is forgotten.
    let cache = std::mem::take(&mut *meter.segment_vector_dims.lock().unwrap());
    let mut seen: HashMap<String, u64> = HashMap::new();
    let mut emitted = 0;
    for (root, members) in roots {
        let (tenant, namespace) = usage_identity(&NamespaceId(root.clone()));
        if !meter.snapshotted.lock().unwrap().contains_key(&root) {
            match members
                .iter()
                .map(|ns| namespace_stored_bytes(state, ns))
                .sum::<Option<u64>>()
            {
                Some(bytes) => {
                    let spooled = meter.emit(&UsageEvent {
                        id: format!("stored/{root}/{hour}"),
                        tenant: tenant.clone(),
                        namespace: namespace.clone(),
                        at_ms: hour * HOUR_MS,
                        meters: BTreeMap::from([(meter::STORED_BYTES.to_string(), bytes)]),
                    });
                    if spooled {
                        meter.snapshotted.lock().unwrap().insert(root.clone(), hour);
                        meter.stats.stored_snapshots.fetch_add(1, Ordering::Relaxed);
                        emitted += 1;
                    }
                }
                // Retried next tick.
                None => {
                    meter
                        .stats
                        .snapshot_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        // Walked even when this hour's vector snapshot is taken, so the
        // namespace's segments stay in the cache (map lookups only).
        let vectors = members
            .iter()
            .map(|ns| namespace_vector_dimensions(state, meter, ns, &cache, &mut seen))
            .sum::<Option<u64>>();
        if meter.vector_snapshotted.lock().unwrap().contains_key(&root) {
            continue;
        }
        let Some(dims) = vectors else {
            meter
                .stats
                .snapshot_failures
                .fetch_add(1, Ordering::Relaxed);
            continue; // retried next tick
        };
        let spooled = meter.emit(&UsageEvent {
            id: format!("vectors/{root}/{hour}"),
            tenant,
            namespace,
            at_ms: hour * HOUR_MS,
            meters: BTreeMap::from([(meter::VECTOR_DIMENSIONS.to_string(), dims)]),
        });
        if spooled {
            meter.vector_snapshotted.lock().unwrap().insert(root, hour);
            meter.stats.vector_snapshots.fetch_add(1, Ordering::Relaxed);
        }
    }
    *meter.segment_vector_dims.lock().unwrap() = seen;
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
pub(crate) fn query_param(request_line: &str, name: &str) -> Option<String> {
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
        return api_error(
            ErrorCode::NotFound,
            "usage metering is off on this pod (KOSHA_USAGE_METERING=on enables it)",
        );
    };
    let (from_ms, to_ms) = match usage_window(request_line, unix_millis()) {
        Ok(window) => window,
        Err(e) => return api_error(ErrorCode::InvalidArgument, &e),
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
                    api_error(
                        ErrorCode::Unavailable,
                        &format!("usage store unavailable: {e}"),
                    )
                } else {
                    api_error(ErrorCode::Unavailable, "usage store unavailable")
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
