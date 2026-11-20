//! Write-ahead log for buffered documents and deletes (DESIGN.md §7).
//!
//! Every record gets its own log sequence number (LSN). Concurrent appends
//! are group-committed: one leader writes every queued record into a single
//! WAL file, fsyncs it once and then acknowledges every caller the file
//! covers. A namespace manifest's `wal_checkpoint` is the highest LSN its
//! segments contain; a file is reclaimed only when every namespace with a
//! record in it has a published checkpoint at or past that record.
//!
//! On-disk formats, all little-endian:
//! - v3 (written now): a file holds one or more frames, each
//!   `magic u32 | version=3 u32 | lsn u64 | len u64 | crc32c u32 | payload`.
//!   The CRC covers the LSN bytes and the payload. The payload is a binary
//!   record; vector fields are stored as raw f32 (see [`encode_record`]).
//! - v2: one frame per file, `magic | version=2 | len u64 | crc32c | JSON`.
//! - v1: unchecksummed binary records.
//!
//! v1 and v2 files remain readable; their records take the LSN in the file
//! name. A torn final file keeps every complete frame and drops only the
//! incomplete tail; a corrupt complete frame fails recovery closed.
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use kosha_core::{Document, DocumentId, Field, FieldType, KoshaError, NamespaceId, WalRecord};

pub(crate) fn durable_write(path: &std::path::Path, bytes: &[u8]) -> Result<(), KoshaError> {
    use std::io::Write;
    let temporary = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    std::fs::File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}

const MAGIC: u32 = 0x4B57414C;
const VERSION: u32 = 3;
/// magic + version + lsn + len + crc.
const FRAME_HEADER: usize = 4 + 4 + 8 + 8 + 4;

/// Durable storage for WAL files. A file is keyed by an LSN no greater than
/// its first record's ([`LocalWalStore`] uses exactly the first record's
/// LSN). [`WalWriter`] decides which files a checkpoint covers; the store
/// only persists, lists, reads and deletes them. Implementations:
/// [`LocalWalStore`] (local disk) and
/// [`ObjectWalStore`](crate::object_wal::ObjectWalStore) (S3, #236).
pub trait WalStore: Send + Sync + std::fmt::Debug {
    /// Atomically and durably persist one group-commit file. Returns only
    /// after the bytes survive a crash; a failure leaves no visible file.
    /// A [`conditional`](Self::conditional) store never replaces an
    /// existing key: it fails with [`std::io::ErrorKind::AlreadyExists`].
    fn append_batch(&self, first_lsn: u64, bytes: &[u8]) -> Result<(), KoshaError>;
    /// `true` when `append_batch` is a create-if-absent (S3
    /// `If-None-Match: *`) on a store other writers can reach. The writer
    /// then keys each group by the first LSN after the previous group, so
    /// two writers on one stream race for the same key and exactly one
    /// wins (see [`WalWriter`]).
    fn conditional(&self) -> bool {
        false
    }
    /// Write-once metadata (`origin`): create `name` unless it exists; an
    /// existing value is kept and is not an error.
    fn create_meta(&self, name: &str, bytes: &[u8]) -> Result<(), KoshaError> {
        match self.read_meta(name)? {
            Some(_) => Ok(()),
            None => self.write_meta(name, bytes),
        }
    }
    /// First LSNs of every WAL file, ascending.
    fn list(&self) -> Result<Vec<u64>, KoshaError>;
    fn read(&self, first_lsn: u64) -> Result<Vec<u8>, KoshaError>;
    /// Delete files fully covered by published checkpoints. Missing files
    /// are not an error.
    fn delete(&self, first_lsns: &[u64]) -> Result<(), KoshaError>;
    /// Keep only the first `complete_len` bytes (whole frames) of a final
    /// file that ends in a torn frame, preserving the original bytes out of
    /// the replay set, so later appends cannot turn the torn frame into a
    /// corrupt middle record.
    fn truncate_torn_tail(&self, first_lsn: u64, complete_len: usize) -> Result<(), KoshaError>;
    /// Small metadata blobs (`origin`, `sequence`).
    fn read_meta(&self, name: &str) -> Result<Option<Vec<u8>>, KoshaError>;
    fn write_meta(&self, name: &str, bytes: &[u8]) -> Result<(), KoshaError>;
}

/// WAL files in one local directory: `wal-{first_lsn:020x}.wal`, written
/// to a temporary file, fsynced, renamed, and the directory fsynced.
#[derive(Debug, Clone)]
pub struct LocalWalStore {
    dir: PathBuf,
}

impl LocalWalStore {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn path(&self, first_lsn: u64) -> PathBuf {
        self.dir.join(format!("wal-{first_lsn:020x}.wal"))
    }
}

impl WalStore for LocalWalStore {
    fn append_batch(&self, first_lsn: u64, bytes: &[u8]) -> Result<(), KoshaError> {
        std::fs::create_dir_all(&self.dir).ok();
        durable_write(&self.path(first_lsn), bytes)
    }

    fn list(&self) -> Result<Vec<u64>, KoshaError> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut files = Vec::new();
        for entry in entries {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "wal") {
                let lsn = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.strip_prefix("wal-"))
                    .and_then(|s| u64::from_str_radix(s, 16).ok())
                    .ok_or_else(|| KoshaError::NotFound("invalid WAL filename".into()))?;
                files.push(lsn);
            }
        }
        files.sort_unstable();
        Ok(files)
    }

    fn read(&self, first_lsn: u64) -> Result<Vec<u8>, KoshaError> {
        Ok(std::fs::read(self.path(first_lsn))?)
    }

    fn delete(&self, first_lsns: &[u64]) -> Result<(), KoshaError> {
        for lsn in first_lsns {
            match std::fs::remove_file(self.path(*lsn)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn truncate_torn_tail(&self, first_lsn: u64, complete_len: usize) -> Result<(), KoshaError> {
        let path = self.path(first_lsn);
        let torn = path.with_extension("torn");
        if complete_len == 0 {
            std::fs::rename(&path, &torn)?;
            std::fs::File::open(&self.dir)?.sync_all()?;
        } else {
            let bytes = std::fs::read(&path)?;
            durable_write(&torn, &bytes)?;
            durable_write(&path, &bytes[..complete_len])?;
        }
        Ok(())
    }

    fn read_meta(&self, name: &str) -> Result<Option<Vec<u8>>, KoshaError> {
        match std::fs::read(self.dir.join(name)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn write_meta(&self, name: &str, bytes: &[u8]) -> Result<(), KoshaError> {
        durable_write(&self.dir.join(name), bytes)
    }
}

/// Group-commit tuning. `max_delay` is how long a commit leader waits for
/// more records once concurrent appends have been observed (the previous
/// group held more than one record); a lone writer never waits. A group is
/// cut early once it holds `max_bytes` of frames.
#[derive(Debug, Clone)]
pub struct GroupCommitConfig {
    pub max_delay: Duration,
    pub max_bytes: usize,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_delay: Duration::from_millis(2),
            max_bytes: 4 * 1024 * 1024,
        }
    }
}

impl GroupCommitConfig {
    /// `KOSHA_WAL_GROUP_COMMIT_MS` (default 2, `0` commits as soon as the
    /// previous fsync finishes) and `KOSHA_WAL_GROUP_COMMIT_BYTES`
    /// (default 4 MiB).
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Some(ms) = std::env::var("KOSHA_WAL_GROUP_COMMIT_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
        {
            config.max_delay = Duration::from_millis(ms);
        }
        if let Some(bytes) = std::env::var("KOSHA_WAL_GROUP_COMMIT_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
        {
            config.max_bytes = bytes.max(1);
        }
        config
    }
}

struct PendingRecord {
    lsn: u64,
    namespace: NamespaceId,
    payload: Vec<u8>,
}

struct WriterState {
    /// Next LSN to assign. Never moves backwards.
    next_lsn: u64,
    /// Highest LSN durably recorded in the `sequence` meta file.
    persisted_high: u64,
    pending: Vec<PendingRecord>,
    pending_bytes: usize,
    leader_active: bool,
    /// Every assigned LSN at or below this has a final outcome. Groups are
    /// committed one at a time in LSN order, so this only grows.
    resolved_through: u64,
    /// LSNs whose group failed to persist; removed by their waiter.
    failed: HashMap<u64, (std::io::ErrorKind, String)>,
    last_group_len: usize,
    /// File first LSN -> highest LSN each namespace has in that file.
    files: BTreeMap<u64, HashMap<NamespaceId, u64>>,
    /// Highest published checkpoint per namespace (for this origin).
    checkpoints: HashMap<NamespaceId, u64>,
    files_written: u64,
    /// Conditional stores only: key of the next group, one past the last
    /// LSN of the newest group this writer persisted or recovered. It
    /// does not move when a group fails, so the next group retries the
    /// same key and a writer that lost a race for it learns so.
    next_key: u64,
    /// Bytes of failed conditional puts to `next_key` whose outcome is
    /// unknown (a timeout may still have landed). A later conflict on that
    /// key is this writer's own write when the stored bytes match one.
    ambiguous: Vec<Arc<Vec<u8>>>,
    /// Set when another writer won a key this writer needed: every later
    /// append fails until the process restarts and re-reads the stream.
    fenced: Option<String>,
}

/// Failed puts remembered for `next_key`; older ones are forgotten, which
/// can only turn a later self-conflict into a (safe) fence.
const MAX_AMBIGUOUS_PUTS: usize = 8;

/// Write-Ahead Log for durability of buffered documents. Thread-safe:
/// appends from many threads are group-committed into shared files.
///
/// LSNs are allocated per writer stream (one `origin`), not per namespace:
/// one group file holds every namespace's records that arrived together,
/// and a namespace manifest's `(wal_origin, wal_checkpoint)` names the
/// highest LSN of that origin its segments contain.
///
/// On a [`conditional`](WalStore::conditional) store the writer keys group
/// `n + 1` by the LSN after group `n`'s last record, and persists it with a
/// create-if-absent. Two processes appending to one stream therefore race
/// for the same key: one wins, and the loser fails that group, is fenced
/// (every later append fails) and must restart, which re-reads the stream.
/// Neither acknowledges a record the other cannot recover.
pub struct WalWriter {
    store: Arc<dyn WalStore>,
    origin: String,
    config: GroupCommitConfig,
    state: Mutex<WriterState>,
    wake: Condvar,
    /// Serializes reclamation (`clear_through`, `clear`).
    reclaim: Mutex<()>,
}

/// Records recovered when a [`WalWriter`] opens, with their LSNs, in order.
pub type RecoveredRecords = Vec<(u64, WalRecord)>;

impl WalWriter {
    /// Local-disk WAL in `wal_dir`, group commit configured from the
    /// environment.
    pub fn new(wal_dir: PathBuf) -> Self {
        Self::with_config(wal_dir, GroupCommitConfig::from_env())
    }

    pub fn with_config(wal_dir: PathBuf, config: GroupCommitConfig) -> Self {
        std::fs::create_dir_all(&wal_dir).expect("cannot create WAL directory");
        Self::with_store(Arc::new(LocalWalStore::new(wal_dir)), config)
    }

    pub fn with_store(store: Arc<dyn WalStore>, config: GroupCommitConfig) -> Self {
        Self::open(store, config).expect("WAL recovery failed").0
    }

    /// Open a writer on `store` and return the records it recovered (every
    /// complete record of every file, in LSN order). Fails instead of
    /// panicking, for stores that can be briefly unavailable.
    pub fn open(
        store: Arc<dyn WalStore>,
        config: GroupCommitConfig,
    ) -> Result<(Self, RecoveredRecords), KoshaError> {
        if store.read_meta("origin")?.is_none() {
            store.create_meta("origin", uuid::Uuid::new_v4().to_string().as_bytes())?;
        }
        // Read back: a concurrent opener of a shared stream may have
        // created it first.
        let origin = store
            .read_meta("origin")?
            .ok_or_else(|| invalid("WAL origin missing after creation"))?;
        let origin = String::from_utf8(origin).map_err(|_| invalid("invalid WAL origin"))?;
        uuid::Uuid::parse_str(&origin).map_err(|_| invalid("invalid WAL origin"))?;
        let recovered = recover_from(store.as_ref())?;
        if let Some((file, complete_len)) = recovered.torn_tail {
            // A discarded tail must not become a corrupt middle record after
            // subsequent writes.
            store.truncate_torn_tail(file, complete_len)?;
        }
        let persisted = store
            .read_meta("sequence")?
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);
        let high = persisted.max(recovered.high_lsn);
        let mut files: BTreeMap<u64, HashMap<NamespaceId, u64>> = BTreeMap::new();
        for (file, lsn, namespace) in &recovered.locations {
            let max = files
                .entry(*file)
                .or_default()
                .entry(namespace.clone())
                .or_insert(*lsn);
            *max = (*max).max(*lsn);
        }
        let writer = Self {
            store,
            origin,
            config,
            state: Mutex::new(WriterState {
                next_lsn: high + 1,
                persisted_high: persisted,
                pending: Vec::new(),
                pending_bytes: 0,
                leader_active: false,
                resolved_through: high,
                failed: HashMap::new(),
                last_group_len: 0,
                files,
                checkpoints: HashMap::new(),
                files_written: 0,
                next_key: high + 1,
                ambiguous: Vec::new(),
                fenced: None,
            }),
            wake: Condvar::new(),
            reclaim: Mutex::new(()),
        };
        Ok((writer, recovered.records))
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// Highest LSN assigned so far. Under concurrent appends this is not
    /// the caller's own record: use the LSN `append` returns.
    pub fn last_sequence(&self) -> u64 {
        self.state.lock().unwrap().next_lsn - 1
    }

    /// Number of group-commit files this writer has persisted (each one
    /// file fsync plus one directory fsync).
    pub fn files_written(&self) -> u64 {
        self.state.lock().unwrap().files_written
    }

    /// A fresh ingest disk must not reuse sequence numbers already covered
    /// by the remote manifests restored on startup.
    pub fn advance_past(&self, checkpoint: u64) {
        let mut state = self.state.lock().unwrap();
        state.next_lsn = state.next_lsn.max(checkpoint.saturating_add(1));
    }

    /// Record a namespace's published checkpoint without reclaiming files
    /// (startup restore). Lets a later [`clear_through`] of another
    /// namespace reclaim files the two share.
    ///
    /// [`clear_through`]: Self::clear_through
    pub fn note_checkpoint(&self, namespace: &NamespaceId, checkpoint: u64) {
        let mut state = self.state.lock().unwrap();
        let known = state.checkpoints.entry(namespace.clone()).or_insert(0);
        *known = (*known).max(checkpoint);
    }

    /// Reclaim only records covered by a successfully published namespace
    /// manifest. A file shared with other namespaces is deleted once every
    /// namespace in it is checkpointed past its records there; later writes
    /// and every other namespace remain recoverable.
    pub fn clear_through(
        &self,
        namespace: &NamespaceId,
        checkpoint: u64,
    ) -> Result<(), KoshaError> {
        // Reclamations run one at a time so the `sequence` high-water mark
        // is written in order (it never regresses) and concurrent meta
        // writes never share a temporary file.
        let _reclaim = self.reclaim.lock().unwrap();
        let (eligible, high) = {
            let mut state = self.state.lock().unwrap();
            let known = state.checkpoints.entry(namespace.clone()).or_insert(0);
            *known = (*known).max(checkpoint);
            let eligible: Vec<u64> = state
                .files
                .range(..=checkpoint)
                .filter(|(_, namespaces)| {
                    namespaces.contains_key(namespace)
                        && namespaces
                            .iter()
                            .all(|(ns, max)| state.checkpoints.get(ns).is_some_and(|cp| cp >= max))
                })
                .map(|(file, _)| *file)
                .collect();
            if eligible.is_empty() {
                return Ok(());
            }
            (eligible, state.next_lsn - 1)
        };
        // Deleted files no longer witness their LSNs: persist the high
        // water mark first so a restart never reuses a checkpointed LSN.
        self.persist_high(high)?;
        self.store.delete(&eligible)?;
        let mut state = self.state.lock().unwrap();
        for file in &eligible {
            state.files.remove(file);
        }
        Ok(())
    }

    /// Durably record `high` in the `sequence` meta file unless an equal or
    /// higher value is already there. Callers hold `reclaim`.
    fn persist_high(&self, high: u64) -> Result<(), KoshaError> {
        if high <= self.state.lock().unwrap().persisted_high {
            return Ok(());
        }
        self.store
            .write_meta("sequence", high.to_string().as_bytes())?;
        let mut state = self.state.lock().unwrap();
        state.persisted_high = state.persisted_high.max(high);
        Ok(())
    }

    /// Discard every WAL record of a deleted namespace, whatever its LSN,
    /// so a restart cannot replay them and resurrect the namespace. A
    /// group-commit file that holds only this namespace's records is
    /// deleted now. A file shared with other namespaces' records is kept
    /// (those records are still needed) and counted in the returned number;
    /// the namespace is marked checkpointed through its records there, so
    /// the file is reclaimed as soon as the other namespaces in it are
    /// checkpointed. Until then a restart would replay the deleted
    /// namespace's records from it.
    pub fn discard_namespace(&self, namespace: &NamespaceId) -> Result<usize, KoshaError> {
        let _reclaim = self.reclaim.lock().unwrap();
        let (eligible, shared, high) = {
            let mut state = self.state.lock().unwrap();
            let mut eligible = Vec::new();
            let mut shared = 0usize;
            let mut through = 0u64;
            for (file, namespaces) in &state.files {
                let Some(max) = namespaces.get(namespace) else {
                    continue;
                };
                through = through.max(*max);
                if namespaces.len() == 1 {
                    eligible.push(*file);
                } else {
                    shared += 1;
                }
            }
            if through > 0 {
                let known = state.checkpoints.entry(namespace.clone()).or_insert(0);
                *known = (*known).max(through);
            }
            (eligible, shared, state.next_lsn - 1)
        };
        if !eligible.is_empty() {
            // Deleted files no longer witness their LSNs (as in
            // `clear_through`).
            self.persist_high(high)?;
            self.store.delete(&eligible)?;
            let mut state = self.state.lock().unwrap();
            for file in &eligible {
                state.files.remove(file);
            }
        }
        Ok(shared)
    }

    /// Append a batch of documents; returns the record's LSN once it is
    /// durable.
    pub fn append(
        &self,
        namespace: &NamespaceId,
        documents: &[Document],
    ) -> Result<u64, KoshaError> {
        let record = WalRecord::new(namespace.clone(), documents.to_vec());
        self.append_record(&record)
    }

    pub fn append_delete(
        &self,
        namespace: &NamespaceId,
        ids: Vec<DocumentId>,
    ) -> Result<u64, KoshaError> {
        let mut record = WalRecord::new(namespace.clone(), Vec::new());
        record.deleted_ids = ids;
        self.append_record(&record)
    }

    pub fn append_record(&self, record: &WalRecord) -> Result<u64, KoshaError> {
        let payload = encode_record(record);
        let state = self.state.lock().unwrap();
        let (state, lsn) = self.enqueue(state, record.namespace.clone(), payload);
        self.await_commit(state, lsn)
    }

    /// Append several records into one commit (they land in the same file
    /// unless the group is cut by `max_bytes`). Returns their LSNs in order.
    pub fn append_records(&self, records: &[WalRecord]) -> Result<Vec<u64>, KoshaError> {
        let payloads: Vec<_> = records.iter().map(encode_record).collect();
        let mut state = self.state.lock().unwrap();
        let mut lsns = Vec::with_capacity(records.len());
        for (record, payload) in records.iter().zip(payloads) {
            let (next, lsn) = self.enqueue(state, record.namespace.clone(), payload);
            state = next;
            lsns.push(lsn);
        }
        let mut first_error = None;
        for lsn in &lsns {
            match self.await_commit(state, *lsn) {
                Ok(_) => {}
                Err(e) => first_error = first_error.or(Some(e)),
            }
            state = self.state.lock().unwrap();
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(lsns),
        }
    }

    fn enqueue<'a>(
        &'a self,
        mut state: MutexGuard<'a, WriterState>,
        namespace: NamespaceId,
        payload: Vec<u8>,
    ) -> (MutexGuard<'a, WriterState>, u64) {
        let lsn = state.next_lsn;
        state.next_lsn += 1;
        state.pending_bytes += FRAME_HEADER + payload.len();
        state.pending.push(PendingRecord {
            lsn,
            namespace,
            payload,
        });
        if state.leader_active && state.pending_bytes >= self.config.max_bytes {
            self.wake.notify_all();
        }
        (state, lsn)
    }

    fn await_commit<'a>(
        &'a self,
        mut state: MutexGuard<'a, WriterState>,
        lsn: u64,
    ) -> Result<u64, KoshaError> {
        loop {
            if lsn <= state.resolved_through {
                return match state.failed.remove(&lsn) {
                    Some((kind, message)) => Err(std::io::Error::new(kind, message).into()),
                    None => Ok(lsn),
                };
            }
            if !state.leader_active {
                state.leader_active = true;
                state = self.commit_group(state);
                continue;
            }
            state = self.wake.wait(state).unwrap();
        }
    }

    /// Leader: gather a group, persist it with one durable write, resolve
    /// every record in it and wake the followers.
    fn commit_group<'a>(
        &'a self,
        mut state: MutexGuard<'a, WriterState>,
    ) -> MutexGuard<'a, WriterState> {
        if state.last_group_len > 1 && !self.config.max_delay.is_zero() {
            let deadline = Instant::now() + self.config.max_delay;
            while state.pending_bytes < self.config.max_bytes {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                state = self.wake.wait_timeout(state, deadline - now).unwrap().0;
            }
        }
        let mut taken_bytes = 0;
        let mut cut = 0;
        for record in &state.pending {
            if cut > 0 && taken_bytes >= self.config.max_bytes {
                break;
            }
            taken_bytes += FRAME_HEADER + record.payload.len();
            cut += 1;
        }
        let batch: Vec<PendingRecord> = state.pending.drain(..cut).collect();
        state.pending_bytes -= taken_bytes;
        let conditional = self.store.conditional();
        let key = if conditional {
            state.next_key
        } else {
            batch[0].lsn
        };
        let fenced = state.fenced.clone();
        let ambiguous = state.ambiguous.clone();
        drop(state);

        let mut bytes = Vec::with_capacity(taken_bytes);
        for record in &batch {
            encode_frame(&mut bytes, record.lsn, &record.payload);
        }
        let last = batch[batch.len() - 1].lsn;
        let outcome = match fenced {
            Some(reason) => GroupOutcome {
                key,
                adopted: None,
                result: Err(GroupFailure::Fenced(reason)),
            },
            None if conditional => self.persist_conditional(key, &bytes, &ambiguous),
            None => GroupOutcome {
                key,
                adopted: None,
                result: self
                    .store
                    .append_batch(key, &bytes)
                    .map_err(GroupFailure::Failed),
            },
        };

        let mut state = self.state.lock().unwrap();
        if let Some((file, high, locations)) = outcome.adopted {
            let entry = state.files.entry(file).or_default();
            for (lsn, namespace) in locations {
                let max = entry.entry(namespace).or_insert(lsn);
                *max = (*max).max(lsn);
            }
            state.next_key = state.next_key.max(high + 1);
            state.ambiguous.clear();
            state.files_written += 1;
        }
        let failure = match outcome.result {
            Ok(()) => {
                let file = state.files.entry(outcome.key).or_default();
                for record in &batch {
                    let max = file.entry(record.namespace.clone()).or_insert(record.lsn);
                    *max = (*max).max(record.lsn);
                }
                state.files_written += 1;
                if conditional {
                    state.next_key = state.next_key.max(last + 1);
                    state.ambiguous.clear();
                }
                None
            }
            Err(GroupFailure::Fenced(reason)) => Some(fenced_error(&reason)),
            Err(GroupFailure::Lost(error)) => {
                let reason = format!(
                    "another writer owns WAL key {} of origin {} ({error})",
                    outcome.key, self.origin
                );
                state.fenced.get_or_insert(reason.clone());
                Some(fenced_error(&reason))
            }
            Err(GroupFailure::Failed(error)) => {
                if conditional && outcome.key == state.next_key {
                    if state.ambiguous.len() == MAX_AMBIGUOUS_PUTS {
                        state.ambiguous.remove(0);
                    }
                    state.ambiguous.push(Arc::new(bytes));
                }
                Some(error)
            }
        };
        if let Some(e) = failure {
            let (kind, message) = match &e {
                KoshaError::Io(io) => (io.kind(), io.to_string()),
                other => (std::io::ErrorKind::Other, other.to_string()),
            };
            for record in &batch {
                state.failed.insert(record.lsn, (kind, message.clone()));
            }
        }
        state.resolved_through = state.resolved_through.max(last);
        state.last_group_len = batch.len();
        state.leader_active = false;
        self.wake.notify_all();
        state
    }

    /// Create the group at `key`. A conflict is this writer's own earlier
    /// put whose reply was lost when the stored bytes equal a remembered
    /// failed attempt: that file is adopted and the group retries at the
    /// key after it. Any other conflict means another writer owns the key.
    fn persist_conditional(
        &self,
        key: u64,
        bytes: &[u8],
        ambiguous: &[Arc<Vec<u8>>],
    ) -> GroupOutcome {
        let error = match self.store.append_batch(key, bytes) {
            Ok(()) => {
                return GroupOutcome {
                    key,
                    adopted: None,
                    result: Ok(()),
                }
            }
            Err(error) if is_conflict(&error) => error,
            Err(error) => {
                return GroupOutcome {
                    key,
                    adopted: None,
                    result: Err(GroupFailure::Failed(error)),
                }
            }
        };
        let stored = if ambiguous.is_empty() {
            None
        } else {
            self.store.read(key).ok()
        };
        let decoded = stored
            .filter(|stored| ambiguous.iter().any(|attempt| **attempt == *stored))
            .and_then(|stored| decode_frames(&stored, key).ok())
            .filter(|decoded| !decoded.torn);
        let Some(decoded) = decoded else {
            return GroupOutcome {
                key,
                adopted: None,
                result: Err(GroupFailure::Lost(error)),
            };
        };
        let high = decoded.records.last().map_or(key, |(lsn, _)| *lsn);
        let locations = decoded
            .records
            .into_iter()
            .map(|(lsn, record)| (lsn, record.namespace))
            .collect();
        let retry = high + 1;
        let result = match self.store.append_batch(retry, bytes) {
            Ok(()) => Ok(()),
            Err(error) if is_conflict(&error) => Err(GroupFailure::Lost(error)),
            Err(error) => Err(GroupFailure::Failed(error)),
        };
        GroupOutcome {
            key: retry,
            adopted: Some((key, high, locations)),
            result,
        }
    }

    /// Explicit administrative reset. Normal flushes use `clear_through`
    /// only after durable publication.
    pub fn clear(&self) -> Result<(), KoshaError> {
        let _reclaim = self.reclaim.lock().unwrap();
        let high = self.state.lock().unwrap().next_lsn - 1;
        self.persist_high(high)?;
        let files = self.store.list()?;
        self.store.delete(&files)?;
        let mut state = self.state.lock().unwrap();
        for file in &files {
            state.files.remove(file);
        }
        state.checkpoints.clear();
        Ok(())
    }

    /// Read all records from all WAL files for recovery.
    pub fn recover(wal_dir: &Path) -> Result<Vec<WalRecord>, KoshaError> {
        Ok(Self::recover_sequenced(wal_dir)?
            .into_iter()
            .map(|(_, r)| r)
            .collect())
    }

    /// Every recoverable record with its LSN, in LSN order.
    pub fn recover_sequenced(wal_dir: &Path) -> Result<Vec<(u64, WalRecord)>, KoshaError> {
        Ok(recover_from(&LocalWalStore::new(wal_dir.to_path_buf()))?.records)
    }

    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.state.lock().unwrap().pending.len()
    }
}

/// An earlier put of this writer that landed although it reported a
/// failure: `(key, highest LSN, (lsn, namespace) of each record)`.
type AdoptedFile = (u64, u64, Vec<(u64, NamespaceId)>);

/// Result of persisting one group: the key it was written (or tried) at,
/// and an [`AdoptedFile`] found on the way.
struct GroupOutcome {
    key: u64,
    adopted: Option<AdoptedFile>,
    result: Result<(), GroupFailure>,
}

enum GroupFailure {
    /// The writer was already fenced.
    Fenced(String),
    /// Another writer owns the key: fence this one.
    Lost(KoshaError),
    /// The put failed (or its outcome is unknown); the key is retried.
    Failed(KoshaError),
}

fn is_conflict(error: &KoshaError) -> bool {
    matches!(error, KoshaError::Io(io) if io.kind() == std::io::ErrorKind::AlreadyExists)
}

fn fenced_error(reason: &str) -> KoshaError {
    std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        format!("WAL writer fenced, restart to re-read the stream: {reason}"),
    )
    .into()
}

struct Recovered {
    records: Vec<(u64, WalRecord)>,
    /// (file, lsn, namespace) for every recovered record.
    locations: Vec<(u64, u64, NamespaceId)>,
    /// Highest LSN or file name seen, including a torn file's.
    high_lsn: u64,
    /// Final file ending in an incomplete frame: (file, complete prefix).
    torn_tail: Option<(u64, usize)>,
}

fn recover_from(store: &dyn WalStore) -> Result<Recovered, KoshaError> {
    let files = store.list()?;
    let mut recovered = Recovered {
        records: Vec::new(),
        locations: Vec::new(),
        high_lsn: files.last().copied().unwrap_or(0),
        torn_tail: None,
    };
    let mut previous = 0u64;
    for (n, file) in files.iter().enumerate() {
        let data = store.read(*file)?;
        let decoded = decode_frames(&data, *file)?;
        if decoded.torn {
            if n + 1 != files.len() {
                return Err(torn());
            }
            recovered.torn_tail = Some((*file, decoded.complete_len));
        }
        for (lsn, record) in decoded.records {
            if lsn < previous {
                return Err(invalid("WAL LSNs out of order"));
            }
            previous = lsn;
            recovered.high_lsn = recovered.high_lsn.max(lsn);
            recovered
                .locations
                .push((*file, lsn, record.namespace.clone()));
            recovered.records.push((lsn, record));
        }
    }
    Ok(recovered)
}

fn encode_frame(out: &mut Vec<u8>, lsn: u64, payload: &[u8]) {
    let lsn_bytes = lsn.to_le_bytes();
    let crc = crc32c::crc32c_append(crc32c::crc32c(&lsn_bytes), payload);
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&lsn_bytes);
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
}

const VALUE_STRING: u8 = 0;
const VALUE_F32: u8 = 1;

fn field_type_code(field_type: FieldType) -> u8 {
    match field_type {
        FieldType::Text => 0,
        FieldType::Keyword => 1,
        FieldType::Integer => 2,
        FieldType::Float => 3,
        FieldType::Date => 4,
        FieldType::Boolean => 5,
        FieldType::Vector => 6,
        FieldType::GeoPoint => 7,
    }
}

fn field_type_from_code(code: u8) -> Result<FieldType, KoshaError> {
    Ok(match code {
        0 => FieldType::Text,
        1 => FieldType::Keyword,
        2 => FieldType::Integer,
        3 => FieldType::Float,
        4 => FieldType::Date,
        5 => FieldType::Boolean,
        6 => FieldType::Vector,
        7 => FieldType::GeoPoint,
        _ => return Err(invalid("unknown WAL field type")),
    })
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// A vector value whose JSON text is exactly what serde_json prints for its
/// f32s (every value built by `Field::vector`) is stored as raw f32 and
/// re-serialized on replay to the same text. Anything else stays a string,
/// so replay never changes a stored value.
fn binary_vector(field: &Field) -> Option<Vec<f32>> {
    if field.field_type != FieldType::Vector {
        return None;
    }
    let vector: Vec<f32> = serde_json::from_str(&field.value).ok()?;
    (serde_json::to_string(&vector).ok()? == field.value).then_some(vector)
}

/// v3 record payload: namespace, timestamp, documents (id, fields with a
/// type code and a string or f32 value), deleted IDs.
pub(crate) fn encode_record(record: &WalRecord) -> Vec<u8> {
    let mut out = Vec::new();
    put_str(&mut out, &record.namespace.0);
    out.extend_from_slice(&record.timestamp.to_le_bytes());
    out.extend_from_slice(&(record.documents.len() as u32).to_le_bytes());
    for doc in &record.documents {
        put_str(&mut out, &doc.id.0);
        out.extend_from_slice(&(doc.fields.len() as u32).to_le_bytes());
        for field in &doc.fields {
            put_str(&mut out, &field.name);
            out.push(field_type_code(field.field_type));
            match binary_vector(field) {
                Some(vector) => {
                    out.push(VALUE_F32);
                    out.extend_from_slice(&(vector.len() as u32).to_le_bytes());
                    for value in vector {
                        out.extend_from_slice(&value.to_le_bytes());
                    }
                }
                None => {
                    out.push(VALUE_STRING);
                    out.extend_from_slice(&(field.value.len() as u64).to_le_bytes());
                    out.extend_from_slice(field.value.as_bytes());
                }
            }
        }
    }
    out.extend_from_slice(&(record.deleted_ids.len() as u32).to_le_bytes());
    for id in &record.deleted_ids {
        put_str(&mut out, &id.0);
    }
    out
}

fn decode_record(payload: &[u8]) -> Result<WalRecord, KoshaError> {
    let mut input = payload;
    let input = &mut input;
    let len = u32_value(input)? as usize;
    let namespace = NamespaceId(string(input, len)?);
    let timestamp = u64_value(input)?;
    let count = u32_value(input)?;
    let mut documents = Vec::new();
    for _ in 0..count {
        let len = u32_value(input)? as usize;
        let id = DocumentId(string(input, len)?);
        let field_count = u32_value(input)?;
        let mut fields = Vec::new();
        for _ in 0..field_count {
            let len = u32_value(input)? as usize;
            let name = string(input, len)?;
            let field_type = field_type_from_code(take(input, 1)?[0])?;
            let value = match take(input, 1)?[0] {
                VALUE_STRING => {
                    let len = usize::try_from(u64_value(input)?)
                        .map_err(|_| invalid("WAL field too large"))?;
                    string(input, len)?
                }
                VALUE_F32 if field_type == FieldType::Vector => {
                    let dims = u32_value(input)? as usize;
                    let raw = take(input, dims.checked_mul(4).ok_or_else(torn)?)?;
                    let vector: Vec<f32> = raw
                        .as_chunks::<4>()
                        .0
                        .iter()
                        .map(|b| f32::from_le_bytes(*b))
                        .collect();
                    serde_json::to_string(&vector)?
                }
                _ => return Err(invalid("unknown WAL value encoding")),
            };
            fields.push(Field {
                name,
                field_type,
                value,
            });
        }
        documents.push(Document { id, fields });
    }
    let deleted = u32_value(input)?;
    let mut deleted_ids = Vec::new();
    for _ in 0..deleted {
        let len = u32_value(input)? as usize;
        deleted_ids.push(DocumentId(string(input, len)?));
    }
    if !input.is_empty() {
        return Err(invalid("trailing WAL record bytes"));
    }
    Ok(WalRecord {
        namespace,
        documents,
        deleted_ids,
        timestamp,
    })
}

struct DecodedFile {
    records: Vec<(u64, WalRecord)>,
    /// Bytes of whole frames (the whole file unless `torn`).
    complete_len: usize,
    /// The file ends in an incomplete frame.
    torn: bool,
}

/// Decode one WAL file of any version. An incomplete final frame is
/// reported as `torn` with every complete frame before it; malformed or
/// checksum-failed data is an error.
fn decode_frames(data: &[u8], file_lsn: u64) -> Result<DecodedFile, KoshaError> {
    let torn_empty = DecodedFile {
        records: Vec::new(),
        complete_len: 0,
        torn: true,
    };
    if data.len() < 8 {
        return Ok(torn_empty);
    }
    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    if u32::from_le_bytes(data[..4].try_into().unwrap()) != MAGIC {
        return Err(invalid("invalid WAL magic"));
    }
    if version != VERSION {
        // v1/v2: a whole file is one unit and takes the file's LSN.
        return match decode_file(data) {
            Ok(records) => Ok(DecodedFile {
                records: records.into_iter().map(|r| (file_lsn, r)).collect(),
                complete_len: data.len(),
                torn: false,
            }),
            Err(KoshaError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                Ok(torn_empty)
            }
            Err(e) => Err(e),
        };
    }
    let mut records = Vec::new();
    let mut offset = 0;
    let mut previous: Option<u64> = None;
    while offset < data.len() {
        let rest = &data[offset..];
        if rest.len() < FRAME_HEADER {
            return Ok(DecodedFile {
                records,
                complete_len: offset,
                torn: true,
            });
        }
        let mut header = &rest[..FRAME_HEADER];
        if u32_value(&mut header)? != MAGIC {
            return Err(invalid("invalid WAL magic"));
        }
        if u32_value(&mut header)? != VERSION {
            return Err(invalid("mixed WAL frame versions"));
        }
        let lsn_bytes: [u8; 8] = header[..8].try_into().unwrap();
        let lsn = u64_value(&mut header)?;
        let len = u64_value(&mut header)?;
        let crc = u32_value(&mut header)?;
        let available = (rest.len() - FRAME_HEADER) as u64;
        if len > available {
            return Ok(DecodedFile {
                records,
                complete_len: offset,
                torn: true,
            });
        }
        let payload = &rest[FRAME_HEADER..FRAME_HEADER + len as usize];
        if crc32c::crc32c_append(crc32c::crc32c(&lsn_bytes), payload) != crc {
            return Err(invalid("WAL checksum mismatch"));
        }
        if lsn < file_lsn || previous.is_some_and(|p| lsn <= p) {
            return Err(invalid("WAL LSN out of order"));
        }
        previous = Some(lsn);
        // The payload is complete and checksummed: a short read inside it
        // is corruption, not a torn tail.
        let record = decode_record(payload).map_err(|e| match e {
            KoshaError::Io(io) if io.kind() == std::io::ErrorKind::UnexpectedEof => {
                invalid("truncated WAL record payload")
            }
            other => other,
        })?;
        records.push((lsn, record));
        offset += FRAME_HEADER + len as usize;
    }
    Ok(DecodedFile {
        records,
        complete_len: data.len(),
        torn: false,
    })
}

fn invalid(message: &str) -> KoshaError {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message).into()
}
fn torn() -> KoshaError {
    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "torn WAL tail").into()
}
fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8], KoshaError> {
    if input.len() < n {
        return Err(torn());
    }
    let (value, rest) = input.split_at(n);
    *input = rest;
    Ok(value)
}
fn u32_value(input: &mut &[u8]) -> Result<u32, KoshaError> {
    Ok(u32::from_le_bytes(take(input, 4)?.try_into().unwrap()))
}
fn u64_value(input: &mut &[u8]) -> Result<u64, KoshaError> {
    Ok(u64::from_le_bytes(take(input, 8)?.try_into().unwrap()))
}
fn string(input: &mut &[u8], n: usize) -> Result<String, KoshaError> {
    String::from_utf8(take(input, n)?.to_vec()).map_err(|_| invalid("invalid UTF-8 in WAL"))
}

/// Decode a legacy (v1 or v2) WAL file.
fn decode_file(data: &[u8]) -> Result<Vec<WalRecord>, KoshaError> {
    let mut input = data;
    if u32_value(&mut input)? != MAGIC {
        return Err(invalid("invalid WAL magic"));
    }
    match u32_value(&mut input)? {
        2 => {
            let len = usize::try_from(u64_value(&mut input)?)
                .map_err(|_| invalid("WAL record too large"))?;
            let crc = u32_value(&mut input)?;
            let payload = take(&mut input, len)?;
            if !input.is_empty() {
                return Err(invalid("trailing WAL bytes"));
            }
            if crc32c::crc32c(payload) != crc {
                return Err(invalid("WAL checksum mismatch"));
            }
            Ok(vec![serde_json::from_slice(payload)?])
        }
        1 => {
            // Legacy files remain readable, but incomplete records are never
            // partially replayed and unknown field types fail closed.
            let count = u32_value(&mut input)?;
            let mut records = Vec::new();
            for _ in 0..count {
                let len = u32_value(&mut input)? as usize;
                let namespace = NamespaceId(string(&mut input, len)?);
                let timestamp = u64_value(&mut input)?;
                let docs = u32_value(&mut input)?;
                let mut documents = Vec::new();
                for _ in 0..docs {
                    let len = u32_value(&mut input)? as usize;
                    let id = DocumentId(string(&mut input, len)?);
                    let count = u32_value(&mut input)?;
                    let mut fields = Vec::new();
                    for _ in 0..count {
                        let len = u32_value(&mut input)? as usize;
                        let name = string(&mut input, len)?;
                        let field_type = field_type_from_code(take(&mut input, 1)?[0])?;
                        let len = usize::try_from(u64_value(&mut input)?)
                            .map_err(|_| invalid("WAL field too large"))?;
                        let value = string(&mut input, len)?;
                        fields.push(Field {
                            name,
                            field_type,
                            value,
                        });
                    }
                    documents.push(Document { id, fields });
                }
                records.push(WalRecord {
                    namespace,
                    timestamp,
                    documents,
                    deleted_ids: Vec::new(),
                });
            }
            if !input.is_empty() {
                return Err(invalid("trailing legacy WAL bytes"));
            }
            Ok(records)
        }
        _ => Err(invalid("unsupported WAL version")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("kosha-wal-{name}-{}", uuid::Uuid::new_v4()))
    }

    fn doc(id: &str) -> Document {
        Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("body", id)],
        }
    }

    fn record(ns: &str, id: &str) -> WalRecord {
        WalRecord::new(NamespaceId(ns.into()), vec![doc(id)])
    }

    fn wal_files(dir: &Path) -> Vec<u64> {
        LocalWalStore::new(dir.to_path_buf()).list().unwrap()
    }

    fn file_path(dir: &Path, first_lsn: u64) -> PathBuf {
        dir.join(format!("wal-{first_lsn:020x}.wal"))
    }

    fn ids(records: &[WalRecord]) -> Vec<String> {
        records
            .iter()
            .flat_map(|r| r.documents.iter().map(|d| d.id.0.clone()))
            .collect()
    }

    /// The v2 encoding written by releases before group commit.
    fn v2_file(record: &WalRecord) -> Vec<u8> {
        let payload = serde_json::to_vec(record).unwrap();
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC.to_le_bytes());
        out.extend_from_slice(&2u32.to_le_bytes());
        out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        out.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }

    #[test]
    fn framed_wal_torn_tail_never_replays_partial_documents_or_deletes() {
        let dir = temp_dir("tail");
        let wal = WalWriter::new(dir.clone());
        let ns = NamespaceId("ns".into());
        wal.append(&ns, &[doc("a")]).unwrap();
        let delete_lsn = wal
            .append_delete(&ns, vec![DocumentId("a".into())])
            .unwrap();
        let tail = file_path(&dir, delete_lsn);
        let bytes = std::fs::read(&tail).unwrap();
        assert_eq!(
            WalWriter::recover(&dir).unwrap()[1].deleted_ids,
            vec![DocumentId("a".into())]
        );
        for end in 0..bytes.len() {
            std::fs::write(&tail, &bytes[..end]).unwrap();
            let recovered = WalWriter::recover(&dir).unwrap();
            assert_eq!(recovered.len(), 1, "tail length {end}");
            assert_eq!(recovered[0].documents[0].id.0, "a");
        }
        let mut corrupt = bytes;
        *corrupt.last_mut().unwrap() ^= 1;
        std::fs::write(&tail, corrupt).unwrap();
        assert!(WalWriter::recover(&dir).is_err());
        std::fs::write(&tail, [0u8; 3]).unwrap();
        drop(wal);
        let restarted = WalWriter::new(dir.clone());
        restarted.append(&ns, &[doc("b")]).unwrap();
        drop(restarted);
        let _second_restart = WalWriter::new(dir.clone());
        assert_eq!(
            WalWriter::recover(&dir).unwrap().len(),
            2,
            "discarded tail must not block a later restart"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn legacy_wal_rejects_unknown_field_types_and_partial_records() {
        let mut data = Vec::new();
        for n in [MAGIC, 1, 1, 2] {
            data.extend_from_slice(&n.to_le_bytes());
        }
        data.extend_from_slice(b"ns");
        data.extend_from_slice(&1u64.to_le_bytes());
        for n in [1u32, 1] {
            data.extend_from_slice(&n.to_le_bytes());
        }
        data.push(b'a');
        for n in [1u32, 1] {
            data.extend_from_slice(&n.to_le_bytes());
        }
        data.push(b'f');
        let type_offset = data.len();
        data.push(0);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.push(b'v');
        assert_eq!(decode_file(&data).unwrap()[0].documents[0].id.0, "a");
        for end in 0..data.len() {
            assert!(decode_file(&data[..end]).is_err());
        }
        data[type_offset] = 255;
        assert!(decode_file(&data).is_err());
    }

    #[test]
    fn v3_record_rejects_unknown_field_types_and_value_encodings() {
        let mut record = record("ns", "a");
        record.documents[0].fields = vec![Field::keyword("k", "v")];
        let payload = encode_record(&record);
        // namespace (4 + 2), timestamp, doc count, id (4 + 1), field count,
        // field name (4 + 1), then the type code and value encoding.
        let type_offset = 6 + 8 + 4 + 5 + 4 + 5;
        assert_eq!(payload[type_offset], 1);
        let mut bad_type = payload.clone();
        bad_type[type_offset] = 255;
        assert!(decode_record(&bad_type).is_err());
        let mut bad_encoding = payload.clone();
        bad_encoding[type_offset + 1] = 9;
        assert!(decode_record(&bad_encoding).is_err());
        // f32 encoding is only valid for vector fields.
        let mut f32_keyword = payload;
        f32_keyword[type_offset + 1] = VALUE_F32;
        assert!(decode_record(&f32_keyword).is_err());
    }

    #[test]
    fn wal_write_and_recover() {
        let dir = temp_dir("recover");
        let wal = WalWriter::new(dir.clone());
        let ns = NamespaceId("test-ns".into());
        let docs = vec![
            Document {
                id: DocumentId("d1".into()),
                fields: vec![Field::text("title", "hello world")],
            },
            Document {
                id: DocumentId("d2".into()),
                fields: vec![Field::keyword("status", "active")],
            },
        ];
        wal.append(&ns, &docs).unwrap();

        let recovered = WalWriter::recover(&dir).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].namespace.0, "test-ns");
        assert_eq!(recovered[0].documents.len(), 2);
        assert_eq!(recovered[0].documents[0].id.0, "d1");

        wal.clear().unwrap();
        assert!(WalWriter::recover(&dir).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_multiple_batches() {
        let dir = temp_dir("multi");
        let wal = WalWriter::new(dir.clone());
        let ns = NamespaceId("ns".into());
        let first = wal.append(&ns, &[doc("d1")]).unwrap();
        let second = wal.append(&ns, &[doc("d2")]).unwrap();
        assert!(second > first);
        let recovered = WalWriter::recover_sequenced(&dir).unwrap();
        assert_eq!(
            recovered.iter().map(|(lsn, _)| *lsn).collect::<Vec<_>>(),
            vec![first, second]
        );
        assert_eq!(recovered[0].1.documents[0].id.0, "d1");
        assert_eq!(recovered[1].1.documents[0].id.0, "d2");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn vectors_are_binary_f32_and_replay_to_the_same_value() {
        let mut record = record("ns", "v");
        let values: Vec<f32> = (0..256).map(|n| n as f32 * 0.123_456_7).collect();
        record.documents[0].fields = vec![
            Field::vector("embedding", values.clone()),
            // Not serde_json's canonical text: kept verbatim.
            Field {
                name: "raw".into(),
                field_type: FieldType::Vector,
                value: "[1, 2.50]".into(),
            },
        ];
        let payload = encode_record(&record);
        let json = serde_json::to_vec(&record).unwrap();
        assert!(
            payload.len() < json.len() / 2,
            "binary {} vs json {}",
            payload.len(),
            json.len()
        );
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        assert!(payload.windows(raw.len()).any(|w| w == raw.as_slice()));
        let decoded = decode_record(&payload).unwrap();
        assert_eq!(
            decoded.documents[0].fields[0].value,
            record.documents[0].fields[0].value
        );
        assert_eq!(decoded.documents[0].fields[1].value, "[1, 2.50]");
        assert_eq!(decoded.timestamp, record.timestamp);
    }

    #[test]
    fn geo_points_replay_with_their_type() {
        // #212 GeoPoint fields go through the v3 binary record as strings.
        let mut record = record("ns", "g");
        record.documents[0].fields = vec![Field {
            name: "loc".into(),
            field_type: FieldType::GeoPoint,
            value: "52.52,13.405".into(),
        }];
        let decoded = decode_record(&encode_record(&record)).unwrap();
        let field = &decoded.documents[0].fields[0];
        assert_eq!(field.field_type, FieldType::GeoPoint);
        assert_eq!(field.value, "52.52,13.405");
    }

    #[test]
    fn concurrent_appends_share_files_and_recover_in_order() {
        const THREADS: usize = 16;
        const PER_THREAD: usize = 25;
        let dir = temp_dir("group");
        let wal = Arc::new(WalWriter::with_config(
            dir.clone(),
            GroupCommitConfig {
                max_delay: Duration::from_millis(5),
                max_bytes: 4 * 1024 * 1024,
            },
        ));
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let wal = wal.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let ns = NamespaceId(if t % 2 == 0 { "a" } else { "b" }.into());
                    (0..PER_THREAD)
                        .map(|i| {
                            let id = format!("t{t}-{i}");
                            (wal.append(&ns, &[doc(&id)]).unwrap(), id)
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut acked: HashMap<String, u64> = HashMap::new();
        for handle in handles {
            let lsns = handle.join().unwrap();
            assert!(lsns.windows(2).all(|w| w[0].0 < w[1].0));
            acked.extend(lsns.into_iter().map(|(lsn, id)| (id, lsn)));
        }
        let appends = (THREADS * PER_THREAD) as u64;
        let files = wal.files_written();
        assert!(files < appends, "{files} files for {appends} appends");
        assert_eq!(wal_files(&dir).len() as u64, files);
        let recovered = WalWriter::recover_sequenced(&dir).unwrap();
        assert_eq!(recovered.len() as u64, appends);
        assert!(recovered.windows(2).all(|w| w[0].0 < w[1].0));
        for (lsn, record) in &recovered {
            let id = &record.documents[0].id.0;
            assert_eq!(acked[id], *lsn, "{id} recovered under its own LSN");
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Blocks the first `append_batch` until released, so the next group
    /// deterministically collects the records queued meanwhile.
    #[derive(Debug)]
    struct GateStore {
        inner: LocalWalStore,
        /// (entered, released)
        gate: Mutex<(bool, bool)>,
        signal: Condvar,
    }

    impl GateStore {
        fn wait_entered(&self) {
            let mut gate = self.gate.lock().unwrap();
            while !gate.0 {
                gate = self.signal.wait(gate).unwrap();
            }
        }
        fn release(&self) {
            self.gate.lock().unwrap().1 = true;
            self.signal.notify_all();
        }
    }

    impl WalStore for GateStore {
        fn append_batch(&self, first_lsn: u64, bytes: &[u8]) -> Result<(), KoshaError> {
            let mut gate = self.gate.lock().unwrap();
            gate.0 = true;
            self.signal.notify_all();
            while !gate.1 {
                gate = self.signal.wait(gate).unwrap();
            }
            drop(gate);
            self.inner.append_batch(first_lsn, bytes)
        }
        fn list(&self) -> Result<Vec<u64>, KoshaError> {
            self.inner.list()
        }
        fn read(&self, first_lsn: u64) -> Result<Vec<u8>, KoshaError> {
            self.inner.read(first_lsn)
        }
        fn delete(&self, first_lsns: &[u64]) -> Result<(), KoshaError> {
            self.inner.delete(first_lsns)
        }
        fn truncate_torn_tail(&self, first_lsn: u64, len: usize) -> Result<(), KoshaError> {
            self.inner.truncate_torn_tail(first_lsn, len)
        }
        fn read_meta(&self, name: &str) -> Result<Option<Vec<u8>>, KoshaError> {
            self.inner.read_meta(name)
        }
        fn write_meta(&self, name: &str, bytes: &[u8]) -> Result<(), KoshaError> {
            self.inner.write_meta(name, bytes)
        }
    }

    #[test]
    fn shared_group_file_reclaimed_only_after_every_namespace_checkpoints() {
        let dir = temp_dir("shared");
        std::fs::create_dir_all(&dir).unwrap();
        let store = Arc::new(GateStore {
            inner: LocalWalStore::new(dir.clone()),
            gate: Mutex::new((false, false)),
            signal: Condvar::new(),
        });
        let wal = Arc::new(WalWriter::with_store(
            store.clone(),
            GroupCommitConfig::default(),
        ));
        let (a, b) = (NamespaceId("a".into()), NamespaceId("b".into()));
        let append = |ns: &NamespaceId, id: &'static str| {
            let wal = wal.clone();
            let ns = ns.clone();
            std::thread::spawn(move || wal.append(&ns, &[doc(id)]).unwrap())
        };
        let a1 = append(&a, "a1");
        store.wait_entered();
        let b1 = append(&b, "b1");
        let a2 = append(&a, "a2");
        while wal.pending_len() < 2 {
            std::thread::yield_now();
        }
        store.release();
        let (a1, b1, a2) = (a1.join().unwrap(), b1.join().unwrap(), a2.join().unwrap());
        assert_eq!(wal.files_written(), 2);
        let files = wal_files(&dir);
        assert_eq!(files, vec![a1, b1.min(a2)], "b1 and a2 share one file");

        // Flush A: its own file goes, the shared one keeps B's record.
        wal.clear_through(&a, a1.max(a2)).unwrap();
        assert_eq!(wal_files(&dir), vec![b1.min(a2)]);
        drop(wal);
        let restarted = WalWriter::new(dir.clone());
        let recovered = WalWriter::recover(&dir).unwrap();
        assert!(ids(&recovered).contains(&"b1".to_string()));
        // After a restart the checkpoint of A comes from its manifest.
        restarted.note_checkpoint(&a, a1.max(a2));
        restarted.clear_through(&b, b1).unwrap();
        assert!(wal_files(&dir).is_empty());
        // Reclaimed LSNs are never reused.
        drop(restarted);
        let reopened = WalWriter::new(dir.clone());
        assert!(reopened.append(&a, &[doc("a3")]).unwrap() > a1.max(a2).max(b1));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn torn_multi_frame_file_keeps_every_complete_frame() {
        let dir = temp_dir("torn-group");
        let wal = WalWriter::new(dir.clone());
        let lsns = wal
            .append_records(&[record("a", "x"), record("b", "y"), record("a", "z")])
            .unwrap();
        drop(wal);
        let files = wal_files(&dir);
        assert_eq!(files, vec![lsns[0]]);
        let path = file_path(&dir, lsns[0]);
        let bytes = std::fs::read(&path).unwrap();
        // Frame boundaries.
        let mut ends = Vec::new();
        let mut offset = 0;
        while offset < bytes.len() {
            let len = u64::from_le_bytes(bytes[offset + 16..offset + 24].try_into().unwrap());
            offset += FRAME_HEADER + len as usize;
            ends.push(offset);
        }
        assert_eq!(ends.len(), 3);
        let expected = ["x", "y", "z"];
        for end in 0..bytes.len() {
            std::fs::write(&path, &bytes[..end]).unwrap();
            let complete = ends.iter().filter(|e| **e <= end).count();
            let recovered = WalWriter::recover_sequenced(&dir).unwrap();
            assert_eq!(
                ids(&recovered.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>()),
                expected[..complete].to_vec(),
                "tail length {end}"
            );
            assert_eq!(
                recovered.iter().map(|(lsn, _)| *lsn).collect::<Vec<_>>(),
                lsns[..complete].to_vec()
            );
        }
        // A corrupt complete frame before the tail fails closed.
        let mut corrupt = bytes.clone();
        corrupt[ends[0] + FRAME_HEADER] ^= 1;
        std::fs::write(&path, &corrupt).unwrap();
        assert!(WalWriter::recover(&dir).is_err());

        // Restart repairs the tail to its complete frames; appends after it
        // and a second restart still recover them.
        std::fs::write(&path, &bytes[..ends[1] + 5]).unwrap();
        let restarted = WalWriter::new(dir.clone());
        assert_eq!(std::fs::read(&path).unwrap(), bytes[..ends[1]].to_vec());
        assert!(path.with_extension("torn").exists());
        let next = restarted
            .append(&NamespaceId("a".into()), &[doc("w")])
            .unwrap();
        assert!(next > lsns[1]);
        drop(restarted);
        let _again = WalWriter::new(dir.clone());
        assert_eq!(ids(&WalWriter::recover(&dir).unwrap()), ["x", "y", "w"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn v2_files_from_older_writers_still_replay() {
        let dir = temp_dir("v2");
        std::fs::create_dir_all(&dir).unwrap();
        let origin = uuid::Uuid::new_v4().to_string();
        std::fs::write(dir.join("origin"), &origin).unwrap();
        std::fs::write(dir.join("sequence"), "8").unwrap();
        let mut deleted = record("a", "gone");
        deleted.documents.clear();
        deleted.deleted_ids = vec![DocumentId("old".into())];
        std::fs::write(file_path(&dir, 7), v2_file(&record("a", "old"))).unwrap();
        std::fs::write(file_path(&dir, 8), v2_file(&deleted)).unwrap();

        let wal = WalWriter::new(dir.clone());
        assert_eq!(wal.origin(), origin);
        let lsn = wal.append(&NamespaceId("b".into()), &[doc("new")]).unwrap();
        assert!(lsn > 8, "sequence {lsn} must not reuse a v2 sequence");
        let recovered = WalWriter::recover_sequenced(&dir).unwrap();
        assert_eq!(
            recovered.iter().map(|(lsn, _)| *lsn).collect::<Vec<_>>(),
            vec![7, 8, lsn]
        );
        assert_eq!(recovered[0].1.documents[0].id.0, "old");
        assert_eq!(recovered[1].1.deleted_ids, vec![DocumentId("old".into())]);
        assert_eq!(recovered[2].1.documents[0].id.0, "new");

        // v2 files are reclaimed by checkpoint like v3 ones.
        wal.clear_through(&NamespaceId("a".into()), 8).unwrap();
        assert_eq!(wal_files(&dir), vec![lsn]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn failed_group_reports_error_to_every_caller() {
        let dir = temp_dir("fail");
        let wal = WalWriter::new(dir.clone());
        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::write(&dir, b"blocked").unwrap();
        assert!(wal
            .append_records(&[record("a", "x"), record("b", "y")])
            .is_err());
        assert!(wal.append(&NamespaceId("a".into()), &[doc("z")]).is_err());
        std::fs::remove_file(&dir).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        let lsn = wal.append(&NamespaceId("a".into()), &[doc("ok")]).unwrap();
        assert_eq!(
            WalWriter::recover_sequenced(&dir).unwrap()[0].0,
            lsn,
            "a later group still commits"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Fails `write_meta("sequence")` while `fail_meta` is set.
    #[derive(Debug)]
    struct MetaFailStore {
        inner: LocalWalStore,
        fail_meta: std::sync::atomic::AtomicBool,
    }

    impl WalStore for MetaFailStore {
        fn append_batch(&self, first_lsn: u64, bytes: &[u8]) -> Result<(), KoshaError> {
            self.inner.append_batch(first_lsn, bytes)
        }
        fn list(&self) -> Result<Vec<u64>, KoshaError> {
            self.inner.list()
        }
        fn read(&self, first_lsn: u64) -> Result<Vec<u8>, KoshaError> {
            self.inner.read(first_lsn)
        }
        fn delete(&self, first_lsns: &[u64]) -> Result<(), KoshaError> {
            self.inner.delete(first_lsns)
        }
        fn truncate_torn_tail(&self, first_lsn: u64, len: usize) -> Result<(), KoshaError> {
            self.inner.truncate_torn_tail(first_lsn, len)
        }
        fn read_meta(&self, name: &str) -> Result<Option<Vec<u8>>, KoshaError> {
            self.inner.read_meta(name)
        }
        fn write_meta(&self, name: &str, bytes: &[u8]) -> Result<(), KoshaError> {
            if name == "sequence" && self.fail_meta.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(std::io::Error::other("sequence write failed").into());
            }
            self.inner.write_meta(name, bytes)
        }
    }

    fn persisted_sequence(dir: &Path) -> u64 {
        std::fs::read_to_string(dir.join("sequence"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }

    #[test]
    fn failed_sequence_write_keeps_files_and_a_retry_reclaims_them() {
        let dir = temp_dir("meta-fail");
        std::fs::create_dir_all(&dir).unwrap();
        let store = Arc::new(MetaFailStore {
            inner: LocalWalStore::new(dir.clone()),
            fail_meta: std::sync::atomic::AtomicBool::new(true),
        });
        let wal = WalWriter::with_store(store.clone(), GroupCommitConfig::default());
        let ns = NamespaceId("a".into());
        let lsn = wal.append(&ns, &[doc("x")]).unwrap();
        assert!(wal.clear_through(&ns, lsn).is_err());
        assert_eq!(wal_files(&dir), vec![lsn], "nothing deleted unpersisted");
        store
            .fail_meta
            .store(false, std::sync::atomic::Ordering::SeqCst);
        // No new appends: the retry must still persist the high-water mark
        // before deleting the file that witnessed it.
        wal.clear_through(&ns, lsn).unwrap();
        assert!(wal_files(&dir).is_empty());
        assert_eq!(persisted_sequence(&dir), lsn);
        drop(wal);
        assert!(
            WalWriter::new(dir.clone())
                .append(&ns, &[doc("y")])
                .unwrap()
                > lsn
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn concurrent_reclamation_never_regresses_the_sequence() {
        const THREADS: usize = 8;
        const ROUNDS: usize = 25;
        let dir = temp_dir("reclaim-race");
        let wal = Arc::new(WalWriter::with_config(
            dir.clone(),
            GroupCommitConfig {
                max_delay: Duration::ZERO,
                ..GroupCommitConfig::default()
            },
        ));
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let wal = wal.clone();
                std::thread::spawn(move || {
                    let ns = NamespaceId(format!("ns{t}"));
                    for i in 0..ROUNDS {
                        let lsn = wal.append(&ns, &[doc(&format!("{t}-{i}"))]).unwrap();
                        wal.clear_through(&ns, lsn).unwrap();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        let high = wal.last_sequence();
        assert!(wal_files(&dir).is_empty());
        assert_eq!(persisted_sequence(&dir), high);
        drop(wal);
        let ns = NamespaceId("ns0".into());
        assert!(
            WalWriter::new(dir.clone())
                .append(&ns, &[doc("z")])
                .unwrap()
                > high
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
