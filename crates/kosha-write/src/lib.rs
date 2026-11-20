pub mod compaction;
pub mod object_wal;
pub mod wal;

pub use compaction::{
    needs_compaction, plan_merge_group, CompactMode, CompactOptions, CompactResult,
    CompactionPolicy, MergePlan,
};

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use kosha_core::gc::CommittedRefs;
use kosha_core::{
    Bm25Params, Document, DocumentId, FilterClause, FilterStore, KoshaError, Manifest,
    ManifestEntry, NamespaceId, SegmentId,
};
use kosha_segment::SegmentWriter;

use compaction::select_merge_inputs;

#[derive(Clone)]
struct NamespaceBuffer {
    documents: Vec<Document>,
    segment_counter: u64,
    segment_prefix: String,
    bm25_params: Bm25Params,
}

/// Mutable per-namespace write state (DESIGN.md §7.1).
#[derive(Clone)]
struct NamespaceState {
    buffer: Option<NamespaceBuffer>,
    manifest: Manifest,
    published_version: u64,
    /// What the control store held at `published_version`, as far as this
    /// writer knows: the base [`Indexer::rebase_onto`] diffs the local
    /// manifest against to find this writer's unpublished delta.
    published_base: PublishedBase,
    /// The bases the snapshots most recently handed out by
    /// [`Indexer::prepare_publication`] would become once committed, keyed
    /// by snapshot version (newest last, a few kept). Promoted by
    /// `acknowledge_manifest`; also how a rebase recognizes a commit of its
    /// own whose reply was lost.
    pending_bases: Vec<(u64, PublishedBase)>,
    /// Tombstoned docs: segment_id → set of doc_seqs
    tombstones: HashMap<SegmentId, HashSet<u32>>,
    /// Lazy map of doc_id → flushed (segment, seq) locations.
    /// Built on first `/exists` call; invalidated across replace/compact/restore.
    id_index: Option<HashMap<DocumentId, Vec<(SegmentId, u32)>>>,
    /// True once a manifest has been created or restored for this namespace.
    /// Distinguishes "unknown namespace" from "empty namespace".
    known: bool,
    wal_sequence: u64,
    flushing_ids: HashSet<DocumentId>,
    recovered: Vec<(u64, kosha_core::WalRecord)>,
    recovered_deletes: HashSet<DocumentId>,
    replay_required: bool,
    /// The manifest as last handed to a search, with what it was built
    /// from — see [`Indexer::manifest_shared`].
    shared_manifest: Option<(ManifestStamp, Arc<Manifest>)>,
    /// Every snapshot [`Indexer::manifest_shared`] handed out that may still
    /// be alive. A segment retired while one of these is held stays on
    /// local disk until it is dropped (#239).
    snapshots: Vec<Weak<Manifest>>,
    /// What the control store held at `published_version`: the base
    /// retirement records are computed against (#239).
    committed: CommittedRefs,
    /// `retired_at_ms` for each object a prepared publication retired, so
    /// rebuilding the same candidate (a retried publish) yields the same
    /// bytes for the compare-and-swap.
    retire_stamps: HashMap<(SegmentId, Option<String>), u64>,
}

/// A segment (or one deletion generation of a segment) this node's
/// manifest stopped referencing, waiting until no reader can still use its
/// local copy (#239). See [`Indexer::reclaim_local`].
#[derive(Clone)]
struct LocalRetirement {
    namespace: NamespaceId,
    segment_id: SegmentId,
    generation: Option<String>,
    since: Instant,
    /// The manifest version that dropped it. Its files stay until this
    /// node has committed that version: until then the control store still
    /// references the segment, and without S3 this disk is its only copy.
    after_version: u64,
    /// Search snapshots that were alive when it was retired. They may
    /// reference the segment, so it stays until every one is dropped.
    pins: Vec<Weak<Manifest>>,
}

impl LocalRetirement {
    fn same_object(&self, other: &LocalRetirement) -> bool {
        self.namespace == other.namespace
            && self.segment_id == other.segment_id
            && self.generation == other.generation
    }

    fn relative_path(&self) -> String {
        match &self.generation {
            None => format!("{}/{}", self.namespace.0, self.segment_id.0),
            Some(g) => format!(
                "{}/{}/deletes-{g}.roaring",
                self.namespace.0, self.segment_id.0
            ),
        }
    }
}

/// What one [`Indexer::reclaim_local`] pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LocalReclaimReport {
    pub segments_removed: usize,
    pub deletion_vectors_removed: usize,
    pub bytes_removed: u64,
    /// Retired objects still waiting: inside the grace window, pinned by a
    /// live snapshot or reader, or a removal failed.
    pub pending: usize,
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// How many prepared-but-unacknowledged publication bases a namespace keeps.
const MAX_PENDING_BASES: usize = 4;

/// Segment set and deletes of a published manifest version.
#[derive(Clone, Default)]
struct PublishedBase {
    segments: HashSet<SegmentId>,
    tombstones: HashMap<SegmentId, HashSet<u32>>,
    /// The namespace schema as published (#220), so a rebase can tell a
    /// local, unpublished schema change from the remote's.
    schema: Option<kosha_core::NamespaceSchema>,
}

impl PublishedBase {
    fn of(manifest: &Manifest, tombstones: &HashMap<SegmentId, HashSet<u32>>) -> Self {
        let segments: HashSet<SegmentId> = manifest
            .segments
            .iter()
            .map(|entry| entry.segment_id.clone())
            .collect();
        let tombstones = tombstones
            .iter()
            .filter(|(segment, ids)| segments.contains(*segment) && !ids.is_empty())
            .map(|(segment, ids)| (segment.clone(), ids.clone()))
            .collect();
        Self {
            segments,
            tombstones,
            schema: manifest.schema.clone(),
        }
    }

    /// Deletes in `current` on `segment` that this base does not have.
    fn new_deletes(
        &self,
        segment: &SegmentId,
        current: &HashMap<SegmentId, HashSet<u32>>,
    ) -> usize {
        let base = self.tombstones.get(segment);
        current.get(segment).map_or(0, |ids| {
            ids.iter()
                .filter(|seq| !base.is_some_and(|base| base.contains(seq)))
                .count()
        })
    }
}

/// What [`Indexer::rebase_onto`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebaseOutcome {
    /// This writer had nothing unpublished: the remote manifest was
    /// installed as is, and there is nothing left to publish.
    Adopted,
    /// The writer's unpublished delta was replayed on top of the remote
    /// manifest; publish the new snapshot against the remote version.
    Rebased,
}

/// What a shared manifest snapshot was built from. Two equal stamps mean
/// the snapshot is still what `publication_snapshot` would build.
///
/// The version covers every publish (flush, rewrite, compaction, restore
/// of a newer manifest). Deletes add tombstones between publishes, which
/// the tombstone counts cover: within one version tombstones only grow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ManifestStamp {
    version: u64,
    segments: usize,
    footers: usize,
    tombstoned_segments: usize,
    tombstones: usize,
    deletion_vectors: usize,
}

impl ManifestStamp {
    fn of(state: &NamespaceState) -> Self {
        Self {
            version: state.manifest.version,
            segments: state.manifest.segments.len(),
            footers: state.manifest.segment_footers.len(),
            tombstoned_segments: state.tombstones.len(),
            tombstones: state.tombstones.values().map(HashSet::len).sum(),
            deletion_vectors: state.manifest.deletion_vectors.len(),
        }
    }
}

impl NamespaceState {
    fn new() -> Self {
        Self {
            buffer: None,
            manifest: Manifest {
                version: 0,
                segments: Vec::new(),
                segment_footers: Default::default(),
                footer_crc32: Default::default(),
                wal_checkpoint: 0,
                wal_origin: None,
                tombstones: Default::default(),
                deletion_vectors: Default::default(),
                schema: None,
                retired: Default::default(),
            },
            published_version: 0,
            published_base: PublishedBase::default(),
            pending_bases: Vec::new(),
            tombstones: HashMap::new(),
            id_index: None,
            known: false,
            wal_sequence: 0,
            flushing_ids: HashSet::new(),
            recovered: Vec::new(),
            recovered_deletes: HashSet::new(),
            replay_required: false,
            shared_manifest: None,
            snapshots: Vec::new(),
            committed: CommittedRefs::default(),
            retire_stamps: HashMap::new(),
        }
    }
}

/// Singleflight coordinator for `flush_namespace` — see its doc comment.
/// `generation` counts completed rounds (a "round" is one leader's full
/// snapshot -> I/O -> commit cycle, possibly repeated in place if `pending`
/// was set partway through); waiters block on `NamespaceHandle::flush_done`
/// until it advances past the value they observed.
struct FlushCoord {
    in_progress: bool,
    pending: bool,
    generation: u64,
    /// Set (alongside bumping `generation`) if the round that just finished
    /// failed, so waiters who piggybacked on it know not to treat it as a
    /// silent success. Cleared at the start of the next round.
    last_round_error: Option<String>,
}

struct NamespaceHandle {
    mutation: Mutex<()>,
    writes: Mutex<()>,
    state: Mutex<NamespaceState>,
    /// At most one compaction in flight per namespace. Held across merge I/O
    /// while `state` is released (DESIGN.md §7.1).
    compact: Mutex<()>,
    /// Bounded pool of concurrent segment-write "slots" for this namespace —
    /// see `run_one_flush_pass`'s doc comment for the full parallel-flush
    /// design this backs.
    ///
    /// This used to be a plain `Mutex<()>` (`flush_io`) held across the
    /// *entire* `SegmentWriter` I/O for a flush pass while `state` was
    /// released — mirroring `compact` above. That fixed `flush_namespace`
    /// blocking `index_documents`'s cheap bookkeeping behind disk I/O
    /// (issue #176 / PR #177), and PR #178 added singleflight coalescing on
    /// top so N concurrent small `/flush` calls collapse into far fewer
    /// segment writes. Both were validated with synthetic bursts. **Live
    /// production traffic then found the remaining gap**: a real, sustained
    /// (multi-minute, not bursty) ingest from multiple concurrent backend
    /// workers can arrive faster than one writer can drain it to disk, no
    /// matter how well the arrivals are coalesced — coalescing reduces the
    /// *number* of segment-write cycles needed, but a strict single-writer
    /// namespace can still only ever run one of them at a time, so a
    /// sustained-enough arrival rate builds an ever-growing backlog and
    /// every caller queued behind it times out, indefinitely, not just
    /// during a brief burst (755-document real ingest via
    /// `decover-tech/backend`'s shadow-write path: 100% `/flush` timeout for
    /// the full multi-minute duration).
    ///
    /// Widened from exactly-one to a configurable bound
    /// (`Indexer::max_concurrent_flush_writers`) so a flush round with
    /// enough buffered backlog to justify it can drain the namespace's
    /// buffer with multiple `SegmentWriter`s running concurrently, each
    /// writing a distinct segment. Acquiring a permit here is the actual
    /// backstop on how many segment writes run at once; `run_one_flush_pass`
    /// already caps the number of chunks it spawns at this pool's capacity,
    /// so in normal operation a chunk never has to wait on a permit here —
    /// this exists so that bound is enforced structurally, not just by
    /// chunk-count arithmetic that could have a bug.
    writer_slots: WriterSlots,
    /// Coalescing on top of `writer_slots`: even with parallel writers,
    /// still no reason to spin up a brand new round for every tiny
    /// concurrent `/flush` call when one is already running.
    /// `kosha_client.bulk()` calls `/flush` after *every* single `bulk()`
    /// call, so bursty small-batch writes (the shadow-write mirror under
    /// real document-upload traffic, or any real-time ingestion workload)
    /// turn into exactly that pattern. `flush_coord`/`flush_done` implement
    /// a singleflight pattern: while one round is running, later callers
    /// don't start their own — they mark `pending` and wait for the current
    /// leader's guaranteed extra pass (see `flush_namespace`) instead of
    /// each doing a dedicated `SegmentWriter` cycle for their own tiny
    /// batch. A round that picks up a large-enough backlog (because
    /// multiple callers piled up pending work, or because a burst simply
    /// arrived faster than one round could drain it) is exactly where
    /// `writer_slots` kicks in and that one round uses more than one
    /// concurrent writer to drain it.
    flush_coord: Mutex<FlushCoord>,
    flush_done: Condvar,
}

/// Bounded counting semaphore backing `NamespaceHandle::writer_slots`. Not a
/// general-purpose primitive — small and specific to bounding concurrent
/// `SegmentWriter`s for one namespace's flush round.
struct WriterSlots {
    available: Mutex<usize>,
    released: Condvar,
}

impl WriterSlots {
    fn new(capacity: usize) -> Self {
        Self {
            available: Mutex::new(capacity.max(1)),
            released: Condvar::new(),
        }
    }

    fn acquire(&self) -> WriterSlotGuard<'_> {
        let mut avail = self.available.lock().unwrap();
        while *avail == 0 {
            avail = self.released.wait(avail).unwrap();
        }
        *avail -= 1;
        WriterSlotGuard { slots: self }
    }
}

struct WriterSlotGuard<'a> {
    slots: &'a WriterSlots,
}

impl Drop for WriterSlotGuard<'_> {
    fn drop(&mut self) {
        let mut avail = self.slots.available.lock().unwrap();
        *avail += 1;
        self.slots.released.notify_one();
    }
}

/// Thread-safe indexer with per-namespace isolation (DESIGN.md §7.1).
///
/// Request handlers share one `Indexer` without a process-wide mutex: each
/// namespace serializes its own mutations via [`NamespaceHandle::state`].
pub struct Indexer {
    data_dir: PathBuf,
    flush_threshold: usize,
    bm25_params: Bm25Params,
    /// Write-Ahead Log for durability
    wal: crate::wal::WalWriter,
    /// Whether WAL is enabled
    wal_enabled: bool,
    namespaces: Mutex<HashMap<NamespaceId, Arc<NamespaceHandle>>>,
    compaction_policy: CompactionPolicy,
    /// Upper bound on concurrent `SegmentWriter`s for one namespace's flush
    /// round (see `NamespaceHandle::writer_slots` / `run_one_flush_pass`).
    /// 1 reproduces the pre-parallel single-writer-per-namespace behavior.
    max_concurrent_flush_writers: usize,
    /// A flush round only splits its buffered snapshot across more than one
    /// concurrent writer once there's at least this many documents *per*
    /// writer to hand out — keeps light bursts (a handful of buffered docs,
    /// the common case) on the cheap single-writer path instead of paying
    /// thread-spawn overhead for work too small to benefit from it.
    min_docs_per_flush_writer: usize,
    /// Segments and deletion generations retired from this node's
    /// manifests whose local files are not yet removed (#239).
    ///
    /// Lock order: may be taken while a namespace `state` lock is held,
    /// never the other way round.
    local_retired: Mutex<Vec<LocalRetirement>>,
}

impl Indexer {
    pub fn new(data_dir: PathBuf) -> Self {
        let wal_dir = data_dir.join("_wal");
        std::fs::create_dir_all(&wal_dir).ok();
        let (wal, records) = crate::wal::WalWriter::open(
            Arc::new(crate::wal::LocalWalStore::new(wal_dir)),
            crate::wal::GroupCommitConfig::from_env(),
        )
        .expect("WAL recovery failed");
        Self::with_wal_writer(data_dir, wal, records)
    }

    /// An indexer whose WAL lives in `store` instead of `data_dir/_wal`
    /// (`KOSHA_WAL_MODE=s3`, #236). A local WAL left in `data_dir/_wal` is
    /// first copied into `store` (see
    /// [`migrate_local_wal`](crate::object_wal::migrate_local_wal)); then
    /// every record in `store` is recovered into the write buffers, exactly
    /// like a local restart, so a fresh disk loses no acknowledged write.
    pub fn with_wal_store(
        data_dir: PathBuf,
        store: Arc<dyn crate::wal::WalStore>,
    ) -> Result<Self, KoshaError> {
        std::fs::create_dir_all(&data_dir)?;
        crate::object_wal::migrate_local_wal(&data_dir.join("_wal"), store.as_ref())?;
        let (wal, records) =
            crate::wal::WalWriter::open(store, crate::wal::GroupCommitConfig::from_env())?;
        Ok(Self::with_wal_writer(data_dir, wal, records))
    }

    fn with_wal_writer(
        data_dir: PathBuf,
        wal: crate::wal::WalWriter,
        records: crate::wal::RecoveredRecords,
    ) -> Self {
        let idx = Self {
            data_dir,
            flush_threshold: 1000,
            bm25_params: Bm25Params::default(),
            wal,
            wal_enabled: true,
            namespaces: Mutex::new(HashMap::new()),
            compaction_policy: CompactionPolicy::default(),
            max_concurrent_flush_writers: 4,
            // Matches `flush_threshold`'s default: a threshold flush is one
            // segment; only a >= 2x backlog splits (#241).
            min_docs_per_flush_writer: 1000,
            local_retired: Mutex::new(Vec::new()),
        };

        // Recover un-flushed documents from WAL on startup.
        for (seq, record) in &records {
            let handle = idx.ns_handle(&record.namespace);
            let mut state = handle.state.lock().unwrap();
            idx.apply_recovered_record(&mut state, record);
            state.wal_sequence = state.wal_sequence.max(*seq);
            state.recovered.push((*seq, record.clone()));
            state.known = true;
        }

        idx
    }

    fn apply_recovered_record(&self, state: &mut NamespaceState, record: &kosha_core::WalRecord) {
        let deleted: HashSet<_> = record.deleted_ids.iter().cloned().collect();
        state.recovered_deletes.extend(deleted.iter().cloned());
        for doc in &record.documents {
            state.recovered_deletes.remove(&doc.id);
        }
        let buf = Self::buffer_mut_in(
            state,
            record.namespace.clone(),
            &self.data_dir,
            &self.bm25_params,
        );
        buf.documents.retain(|doc| !deleted.contains(&doc.id));
        buf.documents.extend(record.documents.clone());
        buf.documents = dedupe_documents_last_wins(std::mem::take(&mut buf.documents));
    }

    /// Enable or disable the WAL.
    pub fn with_wal(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    pub fn with_flush_threshold(mut self, threshold: usize) -> Self {
        self.flush_threshold = threshold;
        self
    }

    pub fn with_bm25_params(mut self, params: Bm25Params) -> Self {
        self.bm25_params = params;
        self
    }

    pub fn with_compaction_policy(mut self, policy: CompactionPolicy) -> Self {
        self.compaction_policy = policy;
        self
    }

    pub fn compaction_policy(&self) -> &CompactionPolicy {
        &self.compaction_policy
    }

    /// See `Indexer::max_concurrent_flush_writers`. `n` is floored at 1.
    pub fn with_max_concurrent_flush_writers(mut self, n: usize) -> Self {
        self.max_concurrent_flush_writers = n.max(1);
        self
    }

    /// See `Indexer::min_docs_per_flush_writer`. `n` is floored at 1.
    pub fn with_min_docs_per_flush_writer(mut self, n: usize) -> Self {
        self.min_docs_per_flush_writer = n.max(1);
        self
    }

    fn ns_handle(&self, namespace: &NamespaceId) -> Arc<NamespaceHandle> {
        let mut map = self.namespaces.lock().unwrap();
        map.entry(namespace.clone())
            .or_insert_with(|| {
                Arc::new(NamespaceHandle {
                    mutation: Mutex::new(()),
                    writes: Mutex::new(()),
                    state: Mutex::new(NamespaceState::new()),
                    compact: Mutex::new(()),
                    writer_slots: WriterSlots::new(self.max_concurrent_flush_writers),
                    flush_coord: Mutex::new(FlushCoord {
                        in_progress: false,
                        pending: false,
                        generation: 0,
                        last_round_error: None,
                    }),
                    flush_done: Condvar::new(),
                })
            })
            .clone()
    }

    fn buffer_mut_in<'a>(
        state: &'a mut NamespaceState,
        namespace: NamespaceId,
        data_dir: &Path,
        bm25_params: &Bm25Params,
    ) -> &'a mut NamespaceBuffer {
        if state.buffer.is_none() {
            let counter = if data_dir.join(&namespace.0).exists() {
                std::fs::read_dir(data_dir.join(&namespace.0))
                    .map(|e| e.filter_map(|e| e.ok()).count() as u64)
                    .unwrap_or(0)
            } else {
                0
            };
            state.buffer = Some(NamespaceBuffer {
                documents: Vec::new(),
                segment_counter: counter,
                segment_prefix: format!(
                    "{}-{}",
                    namespace.0.replace('/', "_"),
                    uuid::Uuid::new_v4().simple()
                ),
                bm25_params: bm25_params.clone(),
            });
        }
        state.buffer.as_mut().unwrap()
    }

    /// Delete documents matching a filter clause.
    /// Records tombstones so subsequent searches exclude them.
    pub fn delete_by_query(
        &self,
        namespace: &NamespaceId,
        _manifest: &Manifest,
        filter: &FilterClause,
    ) -> Result<usize, KoshaError> {
        let handle = self.ns_handle(namespace);
        let _writes = handle.writes.lock().unwrap();
        if self.needs_recovery_hydration(namespace) {
            self.flush_namespace(namespace)?;
        }
        let _mutation = handle.mutation.lock().unwrap();
        let mut state = handle.state.lock().unwrap();
        // Include buffered documents in the deletion's snapshot.
        Self::flush_namespace_in(
            &mut state,
            namespace,
            &self.data_dir,
            self.wal_enabled,
            &self.wal,
        )?;
        // First compute every match; read failure must not leave a partial
        // deletion in memory that the caller was told failed.
        let mut changes = Vec::new();
        let mut deleted_ids = HashSet::new();
        for entry in &state.manifest.segments {
            let seg_dir = self.data_dir.join(&namespace.0).join(&entry.segment_id.0);
            let reader = kosha_segment::SegmentReader::open(seg_dir)?;
            // The search path's evaluator (columnar or `filters.bin`), so a
            // delete matches exactly the documents a search would.
            let mut matching_set = reader.evaluate_filter(filter)?;
            if let Some(dead) = state.tombstones.get(&entry.segment_id) {
                matching_set.remove_all(dead);
            }
            let matching: Vec<u32> = matching_set.iter().collect();
            for seq in &matching {
                let meta = reader
                    .doc_meta(*seq)
                    .ok_or_else(|| KoshaError::NotFound("delete doc metadata missing".into()))?;
                deleted_ids.insert(DocumentId(meta.doc_id.to_owned()));
            }
            changes.push((entry.segment_id.clone(), matching));
        }
        let total = changes.iter().map(|(_, ids)| ids.len()).sum();
        if total > 0 && self.wal_enabled {
            let lsn = self
                .wal
                .append_delete(namespace, deleted_ids.into_iter().collect())?;
            state.wal_sequence = state.wal_sequence.max(lsn);
            state.manifest.wal_checkpoint = state.wal_sequence;
            state.manifest.wal_origin = Some(self.wal.origin().to_owned());
        }
        for (segment, matching) in changes {
            if !matching.is_empty() {
                state
                    .tombstones
                    .entry(segment)
                    .or_default()
                    .extend(matching);
            }
        }
        if total > 0 {
            state.manifest.version += 1;
        }
        state.manifest.tombstones = state.tombstones.clone();
        Ok(total)
    }

    /// Get tombstoned doc_seqs for a namespace, keyed by segment.
    pub fn get_tombstones(
        &self,
        namespace: &NamespaceId,
    ) -> Option<HashMap<SegmentId, HashSet<u32>>> {
        let map = self.namespaces.lock().unwrap();
        let handle = map.get(namespace)?;
        let state = handle.state.lock().unwrap();
        if !state.known && state.tombstones.is_empty() {
            return None;
        }
        if state.tombstones.is_empty() {
            return None;
        }
        Some(state.tombstones.clone())
    }

    /// Which of `ids` already exist in the namespace (buffer or flushed segments)?
    ///
    /// Flushed lookups require local segment files (hydrated cache). Missing
    /// local files fail the lookup rather than treating live IDs as absent.
    pub fn existing_ids(
        &self,
        namespace: &NamespaceId,
        ids: &[DocumentId],
    ) -> Result<HashSet<DocumentId>, KoshaError> {
        let mut found = HashSet::new();
        if ids.is_empty() {
            return Ok(found);
        }
        let wanted: HashSet<&DocumentId> = ids.iter().collect();
        let handle = self.ns_handle(namespace);
        let mut state = handle.state.lock().unwrap();

        if let Some(buf) = state.buffer.as_ref() {
            for doc in &buf.documents {
                if wanted.contains(&doc.id) {
                    found.insert(doc.id.clone());
                }
            }
        }

        if found.len() == wanted.len() {
            return Ok(found);
        }

        Self::ensure_id_index_in(&mut state, namespace, &self.data_dir)?;
        if let Some(index) = state.id_index.as_ref() {
            for id in ids {
                if found.contains(id) {
                    continue;
                }
                let Some(locs) = index.get(id) else {
                    continue;
                };
                let alive = locs.iter().any(|(seg, seq)| {
                    !state
                        .tombstones
                        .get(seg)
                        .is_some_and(|set| set.contains(seq))
                });
                if alive {
                    found.insert(id.clone());
                }
            }
        }
        Ok(found)
    }

    fn ensure_id_index_in(
        state: &mut NamespaceState,
        namespace: &NamespaceId,
        data_dir: &Path,
    ) -> Result<(), KoshaError> {
        if state.id_index.is_some() {
            return Ok(());
        }
        let mut index: HashMap<DocumentId, Vec<(SegmentId, u32)>> = HashMap::new();
        if state.known {
            for entry in &state.manifest.segments {
                let seg_dir = data_dir.join(&namespace.0).join(&entry.segment_id.0);
                if !seg_dir.exists() {
                    return Err(KoshaError::SegmentNotFound(entry.segment_id.clone()));
                }
                let reader = kosha_segment::SegmentReader::open_with_options(seg_dir, false)?;
                for meta in reader.iter_doc_meta() {
                    index
                        .entry(DocumentId(meta.doc_id.to_owned()))
                        .or_default()
                        .push((entry.segment_id.clone(), meta.doc_seq));
                }
            }
        }
        state.id_index = Some(index);
        Ok(())
    }

    fn ensure_id_index(
        &self,
        namespace: &NamespaceId,
        handle: &NamespaceHandle,
    ) -> Result<(), KoshaError> {
        loop {
            let mut snapshot = {
                let state = handle.state.lock().unwrap();
                if state.id_index.is_some() {
                    return Ok(());
                }
                state.clone()
            };
            Self::ensure_id_index_in(&mut snapshot, namespace, &self.data_dir)?;
            let mut state = handle.state.lock().unwrap();
            if state.manifest.version == snapshot.manifest.version {
                state.id_index = snapshot.id_index;
                return Ok(());
            }
        }
    }

    fn remember_flushed_ids_in(state: &mut NamespaceState, seg_id: &SegmentId, docs: &[Document]) {
        let Some(index) = state.id_index.as_mut() else {
            return;
        };
        for (seq, doc) in docs.iter().enumerate() {
            index
                .entry(doc.id.clone())
                .or_default()
                .push((seg_id.clone(), seq as u32));
        }
    }

    /// Index documents into a namespace (OpenSearch `index` semantics).
    ///
    /// Same `doc_id` overwrites any prior live copy: buffered versions are
    /// dropped in place; flushed versions are marked deleted while a new
    /// immutable segment contains only the replacement documents. Incoming batches are last-write-wins
    /// on `doc_id`. New ids take the fast append path.
    pub fn index_documents(
        &self,
        namespace: NamespaceId,
        documents: Vec<Document>,
    ) -> Result<usize, KoshaError> {
        let documents = dedupe_documents_last_wins(documents);
        let count = documents.len();
        if count == 0 {
            return Ok(0);
        }

        let ids: Vec<DocumentId> = documents.iter().map(|doc| doc.id.clone()).collect();
        let id_set: HashSet<&DocumentId> = ids.iter().collect();
        let handle = self.ns_handle(&namespace);

        let writes = handle.writes.lock().unwrap();
        if self.needs_recovery_hydration(&namespace) {
            self.flush_namespace(&namespace)?;
        }
        self.ensure_id_index(&namespace, &handle)?;

        // `needs_flush` is decided under `state` but acted on after it's
        // released — flush_namespace re-acquires state itself for its own
        // (much shorter) phase 1. Calling the old synchronous
        // flush_namespace_in from here would hold this lock across the
        // segment-write I/O, exactly the contention issue #176 fixes.
        let needs_flush = loop {
            self.ensure_id_index(&namespace, &handle)?;
            let mut state = handle.state.lock().unwrap();
            if state.id_index.is_none() {
                continue;
            }
            if Self::has_flushed_ids_in(&mut state, &namespace, &self.data_dir, &ids)? {
                drop(state);
                drop(writes);
                return self.upsert_documents(namespace, documents, false);
            }

            if self.wal_enabled {
                // The append (an fsync, or an S3 PUT in s3 mode) runs with
                // the state lock released, so searches and flushes of this
                // namespace never wait on it; `writes` still orders it
                // against other writes here.
                drop(state);
                let lsn = self.wal.append(&namespace, &documents)?;
                state = handle.state.lock().unwrap();
                state.wal_sequence = state.wal_sequence.max(lsn);
                // A flush may have taken a buffered copy of one of these
                // ids meanwhile. The record is durable (replay is
                // last-write-wins), so publish through the upsert path.
                if Self::has_flushed_ids_in(&mut state, &namespace, &self.data_dir, &ids)? {
                    drop(state);
                    drop(writes);
                    return self.upsert_documents(namespace, documents, false);
                }
            }

            if let Some(buf) = state.buffer.as_mut() {
                buf.documents.retain(|doc| !id_set.contains(&doc.id));
            }
            state.known = true;
            let flush_threshold = self.flush_threshold;
            let buf = Self::buffer_mut_in(
                &mut state,
                namespace.clone(),
                &self.data_dir,
                &self.bm25_params,
            );
            buf.documents.extend(documents);
            break buf.documents.len() >= flush_threshold;
        };
        drop(writes);
        if needs_flush {
            self.flush_namespace(&namespace)?;
        }
        Ok(count)
    }

    /// Replace documents by id with atomic mark-deleted + append publication.
    ///
    /// Immutable segments keep their other live documents unchanged. Partial
    /// replacements merge fields onto the latest copy (OpenSearch `_update`
    /// semantics). The replacement segment and old-copy deletions become
    /// visible in one manifest update and survive restart together.
    pub fn replace_documents(
        &self,
        namespace: NamespaceId,
        documents: Vec<Document>,
    ) -> Result<usize, KoshaError> {
        self.upsert_documents(namespace, documents, true)
    }

    /// Mark prior copies deleted and write only the incoming documents. When `merge` is true, patch fields onto the latest existing
    /// copy (`/replace`); when false, the incoming document fully replaces it
    /// (`/index` upsert).
    fn upsert_documents(
        &self,
        namespace: NamespaceId,
        documents: Vec<Document>,
        merge: bool,
    ) -> Result<usize, KoshaError> {
        self.upsert_documents_with_hook(namespace, documents, merge, || {})
    }

    fn upsert_documents_with_hook(
        &self,
        namespace: NamespaceId,
        documents: Vec<Document>,
        merge: bool,
        before_io: impl FnOnce(),
    ) -> Result<usize, KoshaError> {
        if documents.is_empty() {
            return Ok(0);
        }

        let documents = dedupe_documents_last_wins(documents);
        let count = documents.len();
        let handle = self.ns_handle(&namespace);
        let _writes = handle.writes.lock().unwrap();
        // Recovery must be resolved before assigning a new operation order.
        if self.needs_recovery_hydration(&namespace) {
            self.flush_namespace(&namespace)?;
        }
        let _compact = handle.compact.lock().unwrap();
        let _mutation = handle.mutation.lock().unwrap();
        self.ensure_id_index(&namespace, &handle)?;
        let snapshot = handle.state.lock().unwrap().clone();
        before_io();
        let index = snapshot.id_index.as_ref().unwrap();
        let mut locations = Vec::new();
        let mut replacements = Vec::with_capacity(count);
        for mut doc in documents {
            let live: Vec<_> = index
                .get(&doc.id)
                .into_iter()
                .flatten()
                .filter(|(seg, seq)| {
                    !snapshot
                        .tombstones
                        .get(seg)
                        .is_some_and(|ids| ids.contains(seq))
                })
                .cloned()
                .collect();
            if merge {
                let buffered = snapshot
                    .buffer
                    .as_ref()
                    .and_then(|b| b.documents.iter().rev().find(|d| d.id == doc.id));
                let fields = if let Some(buffered) = buffered {
                    Some(buffered.fields.clone())
                } else if let Some((seg, seq)) = live.last() {
                    let reader = kosha_segment::SegmentReader::open_with_options(
                        self.data_dir.join(&namespace.0).join(&seg.0),
                        false,
                    )?;
                    Some(
                        reader
                            .doc_record_full(*seq)?
                            .ok_or_else(|| {
                                KoshaError::NotFound("replacement document missing".into())
                            })?
                            .fields,
                    )
                } else {
                    None
                };
                if let Some(fields) = fields {
                    doc.fields = merge_fields(fields, doc.fields);
                }
            }
            locations.extend(live);
            replacements.push(doc);
        }
        // WAL fsync and segment I/O happen with state unlocked. The operation
        // lock orders writes; compaction cannot move the old doc locations.
        let sequence = if self.wal_enabled {
            self.wal.append(&namespace, &replacements)?
        } else {
            snapshot.wal_sequence
        };
        let seg_id = SegmentId(format!(
            "{}-upsert-{}",
            namespace.0.replace('/', "_"),
            uuid::Uuid::new_v4().simple()
        ));
        let mut writer = SegmentWriter::new(
            seg_id.clone(),
            self.data_dir.join(&namespace.0).join(&seg_id.0),
        )
        .with_analysis(&snapshot.manifest.analysis_config());
        for doc in &replacements {
            writer.add_document(doc.id.clone(), doc.fields.clone());
        }
        let params = snapshot
            .buffer
            .as_ref()
            .map(|b| b.bm25_params.clone())
            .unwrap_or_else(|| self.bm25_params.clone());
        let footer = writer.finalize(params)?;
        let origin = self.wal.origin().to_owned();
        let mut state = handle.state.lock().unwrap();
        let ids: HashSet<_> = replacements.iter().map(|doc| &doc.id).collect();
        if let Some(buffer) = state.buffer.as_mut() {
            buffer.documents.retain(|doc| !ids.contains(&doc.id));
        }
        for (segment, seq) in locations {
            state.tombstones.entry(segment).or_default().insert(seq);
        }
        Self::remember_flushed_ids_in(&mut state, &seg_id, &replacements);
        state.manifest.segments.push(ManifestEntry {
            segment_id: seg_id,
            doc_count: footer.doc_count,
        });
        state.manifest.remember_segment_footer(footer);
        state.manifest.version += 1;
        state.manifest.tombstones = state.tombstones.clone();
        state.wal_sequence = state.wal_sequence.max(sequence);
        // Other buffered records must remain recoverable after publication.
        if state.buffer.as_ref().is_none_or(|b| b.documents.is_empty()) {
            state.manifest.wal_checkpoint = sequence;
            state.manifest.wal_origin = Some(origin);
        }
        state.known = true;
        Ok(count)
    }

    fn prepare_recovery(
        &self,
        namespace: &NamespaceId,
        handle: &NamespaceHandle,
    ) -> Result<Option<HashMap<SegmentId, HashSet<u32>>>, KoshaError> {
        let mut snapshot = {
            let state = handle.state.lock().unwrap();
            if !state.replay_required && state.recovered_deletes.is_empty() {
                return Ok(None);
            }
            state.clone()
        };
        Self::ensure_id_index_in(&mut snapshot, namespace, &self.data_dir)?;
        let mut ids = snapshot.recovered_deletes;
        if snapshot.replay_required {
            if let Some(buffer) = snapshot.buffer {
                ids.extend(buffer.documents.into_iter().map(|d| d.id));
            }
        }
        let mut changes: HashMap<SegmentId, HashSet<u32>> = HashMap::new();
        for id in ids {
            if let Some(locs) = snapshot.id_index.as_ref().and_then(|index| index.get(&id)) {
                for (segment, seq) in locs {
                    changes.entry(segment.clone()).or_default().insert(*seq);
                }
            }
        }
        Ok(Some(changes))
    }

    fn commit_recovery(
        state: &mut NamespaceState,
        changes: Option<HashMap<SegmentId, HashSet<u32>>>,
    ) {
        if let Some(changes) = changes {
            for (segment, ids) in changes {
                state.tombstones.entry(segment).or_default().extend(ids);
            }
            state.manifest.tombstones = state.tombstones.clone();
            state.manifest.version += 1;
            state.recovered_deletes.clear();
            state.replay_required = false;
        }
    }

    fn has_flushed_ids_in(
        state: &mut NamespaceState,
        namespace: &NamespaceId,
        data_dir: &Path,
        ids: &[DocumentId],
    ) -> Result<bool, KoshaError> {
        if ids.iter().any(|id| state.flushing_ids.contains(id)) {
            return Ok(true);
        }
        if ids.is_empty() {
            return Ok(false);
        }
        Self::ensure_id_index_in(state, namespace, data_dir)?;
        let Some(index) = state.id_index.as_ref() else {
            return Ok(false);
        };
        Ok(ids.iter().any(|id| {
            index.get(id).is_some_and(|locs| {
                locs.iter().any(|(seg, seq)| {
                    !state
                        .tombstones
                        .get(seg)
                        .is_some_and(|set| set.contains(seq))
                })
            })
        }))
    }

    /// Flush whatever's buffered for `namespace` into a new segment(s).
    ///
    /// Singleflight-coordinated (`NamespaceHandle::flush_coord`/`flush_done`
    /// — see their doc comments for why this exists on top of
    /// `writer_slots`). Only one caller at a time actually runs a flush pass
    /// (`run_one_flush_pass`); everyone else who calls this while a pass is
    /// running marks `pending` and waits — the running leader, before
    /// declaring itself done, checks `pending` and runs one more pass in
    /// place if it's set. Since each pass's own snapshot picks up
    /// *everything* currently buffered, a `pending` flag set at any point
    /// during a pass is guaranteed to be covered by that pass's own
    /// follow-up, without the waiter needing a dedicated `SegmentWriter`
    /// cycle for what might be a handful of documents.
    ///
    /// A pass itself is no longer strictly single-writer: `run_one_flush_pass`
    /// splits a large-enough snapshot into multiple chunks and writes their
    /// segments concurrently (bounded by `NamespaceHandle::writer_slots`).
    /// This is what raises the sustained-throughput ceiling beyond what
    /// singleflight coalescing alone could — see `writer_slots`'s doc
    /// comment for the live incident (100% `/flush` timeout under sustained
    /// concurrent ingest) that motivated it.
    ///
    /// This is the public entry point (the `/flush` handler and
    /// `index_documents`'s auto-flush-on-threshold both call it).
    pub fn flush_namespace(&self, namespace: &NamespaceId) -> Result<(), KoshaError> {
        let handle = self.ns_handle(namespace);

        // Become the leader for this round, or piggyback on whoever's
        // already running one.
        {
            let mut coord = handle.flush_coord.lock().unwrap();
            if coord.in_progress {
                coord.pending = true;
                let target_gen = coord.generation;
                let coord = handle
                    .flush_done
                    .wait_while(coord, |c| c.generation <= target_gen)
                    .unwrap();
                // The round we piggybacked on might have failed -- we can't
                // just claim success on its behalf. KoshaError isn't Clone,
                // so reconstruct an equivalent I/O error from its message
                // rather than silently swallowing the failure.
                return match &coord.last_round_error {
                    None => Ok(()),
                    Some(msg) => Err(KoshaError::Io(std::io::Error::other(format!(
                        "flush failed for a coalesced round: {msg}"
                    )))),
                };
            }
            coord.in_progress = true;
        }

        // Leader: keep running passes until nobody asked for another one
        // while we worked.
        loop {
            let result = self.run_one_flush_pass(namespace, &handle);

            let mut coord = handle.flush_coord.lock().unwrap();
            if result.is_ok() && coord.pending {
                coord.pending = false;
                drop(coord);
                continue;
            }
            coord.in_progress = false;
            coord.pending = false;
            coord.generation += 1;
            coord.last_round_error = result.as_ref().err().map(|e| e.to_string());
            drop(coord);
            handle.flush_done.notify_all();
            return result;
        }
    }

    /// One leader's snapshot -> segment-write I/O -> manifest-commit cycle
    /// -- the unit of work `flush_namespace`'s singleflight loop repeats as
    /// needed. Only ever called by the current `flush_coord` leader; not
    /// meant to run concurrently with itself for one namespace.
    ///
    /// Phase 2 (the actual segment write) is where this differs from the
    /// pre-parallel version: the buffered snapshot is split into 1..=
    /// `max_concurrent_flush_writers` chunks (see `take_flush_chunks_in`),
    /// each with its own `SegmentId`, and — when there's more than one --
    /// they're written concurrently instead of one at a time. This is what
    /// actually raises the namespace's write throughput ceiling: coalescing
    /// (#178) reduces how many *rounds* a burst of small flush calls needs,
    /// but every round still only ran one `SegmentWriter` at a time, so a
    /// sustained arrival rate that outpaces one writer thread built an
    /// unbounded backlog no matter how well the arrivals were coalesced --
    /// see `NamespaceHandle::writer_slots`'s doc comment for the live
    /// incident this fixes.
    fn run_one_flush_pass(
        &self,
        namespace: &NamespaceId,
        handle: &NamespaceHandle,
    ) -> Result<(), KoshaError> {
        let _recovery_compact = if self.needs_recovery_hydration(namespace) {
            Some(handle.compact.lock().unwrap())
        } else {
            None
        };
        let _mutation = handle.mutation.lock().unwrap();
        let recovery = self.prepare_recovery(namespace, handle)?;
        let origin = self.wal.origin().to_owned();
        // Phase 1 (state locked, cheap): snapshot whatever's buffered and
        // split it into chunks, each with its own reserved SegmentId.
        // Released immediately after — concurrent index_documents calls for
        // this namespace stay unblocked while phase 2 runs. Reserving every
        // chunk's segment_counter value in this one state-locked pass is
        // what keeps concurrent segment IDs collision-free: by the time
        // phase 2 spawns writer threads, every chunk already owns a
        // distinct, already-incremented counter value.
        let (chunks, checkpoint, analysis) = {
            let mut state = handle.state.lock().unwrap();
            let checkpoint = state.wal_sequence;
            let analysis = state.manifest.analysis_config();
            state.flushing_ids = state
                .buffer
                .as_ref()
                .map(|b| b.documents.iter().map(|doc| doc.id.clone()).collect())
                .unwrap_or_default();
            let chunks = Self::take_flush_chunks_in(
                &mut state,
                self.max_concurrent_flush_writers,
                self.min_docs_per_flush_writer,
            );
            (chunks, checkpoint, analysis)
        };
        if chunks.is_empty() {
            if recovery.is_some() {
                let mut state = handle.state.lock().unwrap();
                Self::commit_recovery(&mut state, recovery);
                state.manifest.wal_checkpoint = checkpoint;
                state.manifest.wal_origin = Some(origin);
            }
            return Ok(());
        }

        // Phase 2 (state free): write every chunk's segment.
        //
        // A single chunk (the common case under light load -- see
        // take_flush_chunks_in) runs directly on this thread: identical cost
        // to the pre-parallel version, no thread-spawn overhead for work
        // that wouldn't benefit from it. More than one chunk means the
        // backlog crossed min_docs_per_flush_writer, so it's worth paying
        // for real concurrency. `writer_slots` bounds how many
        // `SegmentWriter`s actually run at once -- `chunks.len()` is already
        // <= its capacity by construction, so this is the structural
        // backstop, not the primary bound.
        let ns_dir = self.data_dir.join(&namespace.0);
        let results: Vec<(
            SegmentId,
            Vec<Document>,
            Result<kosha_core::Footer, KoshaError>,
        )> = if chunks.len() == 1 {
            let (docs, seg_id, bm25_params) = chunks.into_iter().next().unwrap();
            vec![Self::write_flush_chunk(
                &ns_dir,
                handle,
                docs,
                seg_id,
                bm25_params,
                &analysis,
            )]
        } else {
            std::thread::scope(|scope| {
                let handles: Vec<_> = chunks
                    .into_iter()
                    .map(|(docs, seg_id, bm25_params)| {
                        let ns_dir = &ns_dir;
                        let analysis = &analysis;
                        scope.spawn(move || {
                            Self::write_flush_chunk(
                                ns_dir,
                                handle,
                                docs,
                                seg_id,
                                bm25_params,
                                analysis,
                            )
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().expect("segment-writer thread panicked"))
                    .collect()
            })
        };

        // Every chunk's finalize() outcome is now known. A chunk that failed
        // must have its documents go back into the buffer -- not be
        // silently dropped -- so "every buffered document ends up in
        // exactly one segment" still holds on partial failure, same as it
        // held (trivially, with only one chunk) before this change.
        let mut failed_docs: Vec<Document> = Vec::new();
        let mut first_err: Option<KoshaError> = None;
        let mut succeeded: Vec<(SegmentId, Vec<Document>, kosha_core::Footer)> = Vec::new();
        for (seg_id, docs, result) in results {
            match result {
                Ok(footer) => succeeded.push((seg_id, docs, footer)),
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                    failed_docs.extend(docs);
                }
            }
        }

        // A failed round publishes no partial snapshot. Keep every input
        // in the WAL and restore it ahead of newer buffered writes.
        if let Some(error) = first_err {
            for (_, docs, _) in succeeded {
                failed_docs.extend(docs);
            }
            let mut state = handle.state.lock().unwrap();
            let buf = Self::buffer_mut_in(
                &mut state,
                namespace.clone(),
                &self.data_dir,
                &self.bm25_params,
            );
            failed_docs.append(&mut buf.documents);
            buf.documents = dedupe_documents_last_wins(failed_docs);
            state.flushing_ids.clear();
            return Err(error);
        }

        // Phase 3 (state locked, cheap): commit every succeeded chunk's
        // segment and restore any failed chunk's documents to the buffer.
        // Order among succeeded chunks doesn't matter for correctness --
        // manifest.segments is an unordered set of live segments -- so
        // committing them all under one lock acquisition (rather than one
        // per chunk) is simpler and no less correct, and still keeps state
        // held only for cheap bookkeeping, never across I/O.
        {
            let mut state = handle.state.lock().unwrap();
            Self::commit_recovery(&mut state, recovery);
            state.flushing_ids.clear();
            state.manifest.wal_checkpoint = state.manifest.wal_checkpoint.max(checkpoint);
            state.manifest.wal_origin = Some(origin);
            for (seg_id, docs, footer) in succeeded {
                Self::remember_flushed_ids_in(&mut state, &seg_id, &docs);
                state.known = true;
                state.manifest.version += 1;
                state.manifest.segments.push(ManifestEntry {
                    segment_id: seg_id,
                    doc_count: footer.doc_count,
                });
                state.manifest.remember_segment_footer(footer);
            }
            if !failed_docs.is_empty() {
                let buf = Self::buffer_mut_in(
                    &mut state,
                    namespace.clone(),
                    &self.data_dir,
                    &self.bm25_params,
                );
                buf.documents.extend(failed_docs);
            }
        }

        Ok(())
    }

    /// Write one flush chunk's segment to disk. Bounded by `writer_slots` so
    /// at most `max_concurrent_flush_writers` of these run at once for a
    /// namespace, whether called directly (single-chunk fast path) or from
    /// a `thread::scope`-spawned thread (multi-chunk path) in
    /// `run_one_flush_pass`. Returns the outcome rather than propagating an
    /// error so a sibling chunk's failure can't take down chunks that
    /// already succeeded -- the caller decides how to handle a mix of
    /// `Ok`/`Err` results across chunks.
    fn write_flush_chunk(
        ns_dir: &Path,
        handle: &NamespaceHandle,
        docs: Vec<Document>,
        seg_id: SegmentId,
        bm25_params: Bm25Params,
        analysis: &kosha_core::AnalysisConfig,
    ) -> (
        SegmentId,
        Vec<Document>,
        Result<kosha_core::Footer, KoshaError>,
    ) {
        let _permit = handle.writer_slots.acquire();
        let seg_dir = ns_dir.join(seg_id.0.as_str());
        let mut writer = SegmentWriter::new(seg_id.clone(), seg_dir).with_analysis(analysis);
        for doc in &docs {
            writer.add_document(doc.id.clone(), doc.fields.clone());
        }
        let result = writer.finalize(bm25_params);
        (seg_id, docs, result)
    }

    /// Take whatever's currently buffered for flushing and split it into up
    /// to `max_writers` chunks, each with its own freshly-reserved
    /// `SegmentId` -- the state-locked "phase 1" half of a flush round, kept
    /// separate so phase 2's disk I/O never runs while `state` is held.
    /// Empty (not `Vec` of one empty chunk) if there's nothing buffered,
    /// matching the prior single-function early return.
    ///
    /// Splits into more than one chunk only once there's at least
    /// `min_docs_per_writer` documents *per* writer to give it -- a handful
    /// of buffered docs (the common case under light load) always yields
    /// exactly one chunk, matching pre-parallel behavior exactly.
    fn take_flush_chunks_in(
        state: &mut NamespaceState,
        max_writers: usize,
        min_docs_per_writer: usize,
    ) -> Vec<(Vec<Document>, SegmentId, Bm25Params)> {
        let Some(buf) = state.buffer.as_mut() else {
            return Vec::new();
        };
        if buf.documents.is_empty() {
            return Vec::new();
        }
        let total = buf.documents.len();
        let max_writers = max_writers.max(1);
        let min_docs_per_writer = min_docs_per_writer.max(1);
        let n_chunks = (total / min_docs_per_writer).clamp(1, max_writers);

        let docs = dedupe_documents_last_wins(std::mem::take(&mut buf.documents));
        let bm25_params = buf.bm25_params.clone();
        let ns_prefix = &buf.segment_prefix;

        let chunk_size = total.div_ceil(n_chunks);
        let mut chunks = Vec::with_capacity(n_chunks);
        let mut iter = docs.into_iter().peekable();
        while iter.peek().is_some() {
            let piece: Vec<Document> = (&mut iter).take(chunk_size).collect();
            let seg_id = SegmentId(format!("{}-{:06x}", ns_prefix, buf.segment_counter));
            buf.segment_counter += 1;
            chunks.push((piece, seg_id, bm25_params.clone()));
        }
        chunks
    }

    /// Synchronous flush used only by callers already holding `state` for
    /// buffered-delete bookkeeping in the same critical section. Unlike
    /// `flush_namespace`, this holds `state` across the segment-write I/O;
    /// not fixed here, tracked as a known follow-up in issue #176.
    fn flush_namespace_in(
        state: &mut NamespaceState,
        namespace: &NamespaceId,
        data_dir: &Path,
        wal_enabled: bool,
        wal: &crate::wal::WalWriter,
    ) -> Result<(), KoshaError> {
        let (docs, seg_id, bm25_params) = {
            let Some(buf) = state.buffer.as_mut() else {
                return Ok(());
            };
            if buf.documents.is_empty() {
                return Ok(());
            }
            let docs = dedupe_documents_last_wins(std::mem::take(&mut buf.documents));
            let seg_id = SegmentId(format!(
                "{}-{:06x}",
                buf.segment_prefix, buf.segment_counter
            ));
            buf.segment_counter += 1;
            let bm25_params = buf.bm25_params.clone();
            (docs, seg_id, bm25_params)
        };

        let seg_dir = data_dir.join(&namespace.0).join(seg_id.0.as_str());
        let mut writer = SegmentWriter::new(seg_id.clone(), seg_dir)
            .with_analysis(&state.manifest.analysis_config());
        for doc in &docs {
            writer.add_document(doc.id.clone(), doc.fields.clone());
        }
        let footer = match writer.finalize(bm25_params) {
            Ok(footer) => footer,
            Err(e) => {
                state.buffer.as_mut().unwrap().documents = docs;
                return Err(e);
            }
        };

        // WAL reclamation waits for durable manifest publication.
        let _ = (wal_enabled, wal);
        state.manifest.wal_checkpoint = state.wal_sequence;
        state.manifest.wal_origin = Some(wal.origin().to_owned());

        Self::remember_flushed_ids_in(state, &seg_id, &docs);

        state.known = true;
        state.manifest.version += 1;
        state.manifest.segments.push(ManifestEntry {
            segment_id: seg_id,
            doc_count: footer.doc_count,
        });
        state.manifest.remember_segment_footer(footer);
        Ok(())
    }

    /// Compact a namespace using the indexer's default tiered policy.
    pub fn compact_namespace(&self, namespace: &NamespaceId) -> Result<CompactResult, KoshaError> {
        self.compact_namespace_with_options(
            namespace,
            CompactOptions::tiered(self.compaction_policy.clone()),
        )
    }

    /// Compact a namespace under explicit options (tiered vs emergency full).
    ///
    /// Holds the per-namespace `compact` lock for the whole operation, but
    /// releases `state` during merge I/O so other work on this namespace (and
    /// every other namespace) is not blocked on disk reads/writes.
    pub fn compact_namespace_with_options(
        &self,
        namespace: &NamespaceId,
        opts: CompactOptions,
    ) -> Result<CompactResult, KoshaError> {
        self.compact_namespace_with_options_impl(namespace, opts, || {})
    }

    /// Same as `compact_namespace_with_options`, but calls `after_plan` once
    /// the tombstone snapshot below has been taken and `state` released,
    /// immediately before merge I/O starts — the exact window in which a
    /// concurrent `delete_by_query` can land on an input segment. Always
    /// `|| {}` in production; the test-only seam a race regression needs to
    /// land a delete deterministically instead of racing real threads
    /// against merge I/O timing. See
    /// `compact_carries_forward_a_delete_that_lands_mid_merge`.
    fn compact_namespace_with_options_impl(
        &self,
        namespace: &NamespaceId,
        opts: CompactOptions,
        after_plan: impl FnOnce(),
    ) -> Result<CompactResult, KoshaError> {
        self.compact_with_selector(
            namespace,
            |manifest, ns_dir| {
                select_merge_inputs(
                    manifest,
                    &opts,
                    |seg_id| ns_dir.join(&seg_id.0).exists(),
                    |seg_id| segment_dir_bytes(&ns_dir.join(&seg_id.0)),
                )
            },
            after_plan,
        )
    }

    /// Merge exactly the segments of a plan made beforehand with
    /// [`plan_merge_group`] (#242): the caller planned from remote sizes,
    /// hydrated only these inputs, and now merges them. Re-planning here
    /// from whatever happens to be on local disk could pick a different
    /// group (fresh flushes are local too), so the plan is honored as
    /// given, minus any input that has since left the manifest or is not
    /// on local disk (hydration failed). Fewer than two left is a no-op.
    pub fn compact_planned(
        &self,
        namespace: &NamespaceId,
        plan: &MergePlan,
    ) -> Result<CompactResult, KoshaError> {
        self.compact_with_selector(
            namespace,
            |manifest, ns_dir| {
                let inputs: Vec<ManifestEntry> = plan
                    .inputs
                    .iter()
                    .filter_map(|planned| {
                        manifest
                            .segments
                            .iter()
                            .find(|e| e.segment_id == planned.segment_id)
                    })
                    .filter(|e| ns_dir.join(&e.segment_id.0).exists())
                    .cloned()
                    .collect();
                (inputs.len() >= 2).then_some(MergePlan { inputs })
            },
            || {},
        )
    }

    fn compact_with_selector(
        &self,
        namespace: &NamespaceId,
        select: impl FnOnce(&Manifest, &Path) -> Option<MergePlan>,
        after_plan: impl FnOnce(),
    ) -> Result<CompactResult, KoshaError> {
        let handle = self.ns_handle(namespace);
        let _compact_guard = handle.compact.lock().unwrap();

        let (plan, tombstones, bm25_params, segments_before, analysis) = {
            let mut state = handle.state.lock().unwrap();
            if !state.known {
                return Ok(CompactResult::unchanged(0));
            }
            let segments_before = state.manifest.segments.len();
            let ns_dir = self.data_dir.join(&namespace.0);
            let plan = match select(&state.manifest, &ns_dir) {
                Some(p) => p,
                None => return Ok(CompactResult::unchanged(segments_before)),
            };
            let mut tombstones = HashMap::new();
            for entry in &plan.inputs {
                if let Some(ts) = state.tombstones.get(&entry.segment_id) {
                    tombstones.insert(entry.segment_id.clone(), ts.clone());
                }
            }
            let bm25_params = state
                .buffer
                .as_ref()
                .map(|b| b.bm25_params.clone())
                .unwrap_or_else(|| self.bm25_params.clone());
            // Ensure buffer exists so future flushes keep stable params.
            let _ = Self::buffer_mut_in(
                &mut state,
                namespace.clone(),
                &self.data_dir,
                &self.bm25_params,
            );
            // Merged documents are re-analyzed under the namespace's
            // *current* schema (#220): compaction is how segments written
            // with an older analyzer converge on the new one.
            let analysis = state.manifest.analysis_config();
            (plan, tombstones, bm25_params, segments_before, analysis)
        };

        after_plan();

        // ── Merge I/O with state lock released (DESIGN.md §7.1) ──────────
        let ns_dir = self.data_dir.join(&namespace.0);
        let seg_id = SegmentId(format!(
            "{}-compact-{}",
            namespace.0.replace('/', "_"),
            uuid::Uuid::new_v4().simple()
        ));
        let seg_dir = ns_dir.join(seg_id.0.as_str());
        let mut writer =
            kosha_segment::SegmentWriter::new(seg_id.clone(), seg_dir).with_analysis(&analysis);
        let mut any_docs = false;
        let old_segment_ids = plan.input_ids();
        // (old segment_id, old doc_seq) → the doc's doc_seq in the merged
        // output, for every doc actually copied. `SegmentWriter::add_document`
        // assigns doc_seq as `self.doc_records.len()` at call time (strictly
        // in call order), so `next_new_seq` tracks it exactly. Only needed to
        // carry a tombstone forward (see the publish block below) — a doc
        // excluded here by the snapshot below never needs an entry.
        let mut seq_remap: HashMap<(SegmentId, u32), u32> = HashMap::new();
        let mut next_new_seq: u32 = 0;

        for entry in &plan.inputs {
            let reader = kosha_segment::SegmentReader::open(ns_dir.join(&entry.segment_id.0))?;
            let ts = tombstones.get(&entry.segment_id);
            // `?` here is deliberate: a doc that fails to read must abort
            // the whole compaction, not just be skipped from the merge
            // output — see `iter_doc_records`'s doc comment for the
            // incident this silent-drop used to cause. `add_document`
            // streams into `seg_dir/.build` (#242), but aborting here drops
            // the writer, and `SegmentWriter`'s `Drop` removes that scratch
            // (and `seg_dir` itself, still empty), so no partial output is
            // left on disk.
            //
            // `iter_doc_records_raw` hands sidecar vectors over as f32, so
            // they pass straight into the output without a JSON round trip
            // (#242); inline-stored vectors stay JSON in their fields.
            for raw in reader.iter_doc_records_raw() {
                let kosha_segment::RawDocRecord {
                    record: doc_rec,
                    vectors,
                } = raw?;
                if ts.is_some_and(|set| set.contains(&doc_rec.doc_seq)) {
                    continue;
                }
                seq_remap.insert((entry.segment_id.clone(), doc_rec.doc_seq), next_new_seq);
                next_new_seq += 1;
                writer.add_document_with_vectors(doc_rec.doc_id, doc_rec.fields, vectors);
                any_docs = true;
            }
        }

        if !any_docs {
            let _ = std::fs::remove_dir_all(ns_dir.join(&seg_id.0));
            return Ok(CompactResult::unchanged(segments_before));
        }

        let footer = writer.finalize(bm25_params)?;
        let output_segment_id = seg_id.clone();

        // ── CAS publish under state lock ─────────────────────────────────
        {
            let mut state = handle.state.lock().unwrap();
            let still_present = old_segment_ids
                .iter()
                .all(|id| state.manifest.segments.iter().any(|e| e.segment_id == *id));
            if !still_present {
                // A concurrent manifest change removed an input — drop the orphan
                // merge output rather than publishing a conflicting manifest.
                let _ = std::fs::remove_dir_all(ns_dir.join(&seg_id.0));
                return Ok(CompactResult {
                    segments_before,
                    ..CompactResult::unchanged(state.manifest.segments.len())
                });
            }

            // Carry forward any tombstone added to an input segment *after*
            // the plan snapshot above but before this publish — a
            // `delete_by_query` landing mid-merge. Without this, the tombstone
            // below is unconditionally dropped with the rest of the input
            // segment's state, and the doc it targeted (already copied into
            // the merge output under the stale snapshot) comes back to life
            // in the merged segment. `seq_remap` has no entry for a doc_seq
            // that was already tombstoned at snapshot time — it was excluded
            // from the merge output entirely, so there's nothing to carry.
            let mut carried_tombstones: HashSet<u32> = HashSet::new();
            for old_id in &old_segment_ids {
                let snapshot = tombstones.get(old_id);
                let Some(live) = state.tombstones.get(old_id) else {
                    continue;
                };
                for &old_seq in live {
                    if snapshot.is_some_and(|s| s.contains(&old_seq)) {
                        continue;
                    }
                    if let Some(&new_seq) = seq_remap.get(&(old_id.clone(), old_seq)) {
                        carried_tombstones.insert(new_seq);
                    }
                }
            }

            state
                .manifest
                .segments
                .retain(|e| !old_segment_ids.contains(&e.segment_id));
            state.manifest.version += 1;
            let merged_segment_id = seg_id.clone();
            state.manifest.segments.push(ManifestEntry {
                segment_id: seg_id,
                doc_count: footer.doc_count,
            });
            state.manifest.remember_segment_footer(footer);
            state.known = true;
            for seg_id in &old_segment_ids {
                state.tombstones.remove(seg_id);
                state.manifest.segment_footers.remove(seg_id);
            }
            // The inputs stay on disk until no search can still read them
            // (#239): searches holding an older snapshot keep running.
            let version = state.manifest.version;
            self.retire_locally(
                &mut state,
                namespace,
                version,
                old_segment_ids
                    .iter()
                    .map(|id| (id.clone(), None))
                    .collect(),
            );
            if !carried_tombstones.is_empty() {
                state
                    .tombstones
                    .entry(merged_segment_id)
                    .or_default()
                    .extend(carried_tombstones);
            }
            state.id_index = None;
        }

        let segments_after = self
            .manifest_cloned(namespace)
            .map(|m| m.segments.len())
            .unwrap_or(0);

        Ok(CompactResult {
            merged: true,
            segments_before,
            segments_after,
            segments_merged: old_segment_ids.len(),
            merged_inputs: old_segment_ids,
            output: Some(output_segment_id),
        })
    }

    /// Whether a scheduler should run a tiered compaction pass on `namespace`.
    pub fn needs_compaction(&self, namespace: &NamespaceId) -> bool {
        match self.manifest_cloned(namespace) {
            Some(m) => needs_compaction(&m, &self.compaction_policy),
            None => false,
        }
    }

    pub fn flush_all(&self) -> Result<(), KoshaError> {
        let namespaces: Vec<NamespaceId> = {
            let map = self.namespaces.lock().unwrap();
            map.keys().cloned().collect()
        };
        for ns in namespaces {
            self.flush_namespace(&ns)?;
        }
        Ok(())
    }

    /// Restore a persisted manifest (from the control store) on startup, so a
    /// previously-indexed namespace is searchable again after a restart.
    ///
    /// Also advances the namespace's segment counter past any restored
    /// segment IDs, so future flushes can't collide with segments that exist
    /// in durable storage (e.g. S3) but not on local disk.
    pub fn restore_manifest(&self, namespace: NamespaceId, mut manifest: Manifest) {
        let handle = self.ns_handle(&namespace);
        let mut state = handle.state.lock().unwrap();
        if state.known && state.manifest.version > manifest.version {
            return;
        }
        // Persisted manifests no longer carry footers (#241). Keep every
        // footer this process already parsed for a segment the new version
        // still lists, so a manifest refresh costs no footer I/O for
        // unchanged segments.
        manifest.carry_segment_footers_from(&state.manifest);
        let checkpoint_applies = manifest
            .wal_origin
            .as_deref()
            .is_none_or(|origin| origin == self.wal.origin());
        self.wal.advance_past(manifest.wal_checkpoint);
        if checkpoint_applies {
            // Lets a later reclamation of another namespace delete group
            // files this namespace shares with it.
            self.wal
                .note_checkpoint(&namespace, manifest.wal_checkpoint);
        }
        state.wal_sequence = state.wal_sequence.max(manifest.wal_checkpoint);
        let max_counter = manifest
            .segments
            .iter()
            .filter_map(|e| segment_flush_counter(&e.segment_id))
            .max();
        if let Some(max) = max_counter {
            let buf = Self::buffer_mut_in(
                &mut state,
                namespace.clone(),
                &self.data_dir,
                &self.bm25_params,
            );
            buf.segment_counter = buf.segment_counter.max(max + 1);
        }
        state.id_index = None;
        if !state.recovered.is_empty() {
            let replay: Vec<_> = std::mem::take(&mut state.recovered)
                .into_iter()
                .filter(|(seq, _)| !checkpoint_applies || *seq > manifest.wal_checkpoint)
                .collect();
            Self::buffer_mut_in(
                &mut state,
                namespace.clone(),
                &self.data_dir,
                &self.bm25_params,
            )
            .documents
            .clear();
            state.recovered_deletes.clear();
            for (_, record) in replay {
                self.apply_recovered_record(&mut state, &record);
            }
            let buf = state.buffer.as_ref().unwrap();
            state.replay_required = !buf.documents.is_empty() && !manifest.segments.is_empty();
        }
        state.tombstones = manifest.tombstones.clone();
        state.published_version = manifest.version;
        state.published_base = PublishedBase::of(&manifest, &manifest.tombstones);
        state.pending_bases.clear();
        if state.known {
            let mut retired = dropped_segments(&state.manifest, &manifest);
            retired.extend(superseded_generations(&state.committed, &manifest));
            self.retire_locally(&mut state, &namespace, manifest.version, retired);
        }
        state.committed = CommittedRefs::of(&manifest);
        state.manifest = manifest;
        state.shared_manifest = None;
        state.known = true;
    }

    /// Set the namespace schema (#220), creating the namespace if needed.
    /// Every analyzer is resolved first, so an invalid schema is rejected
    /// with nothing changed. Only segments written from now on (flushes,
    /// and compaction outputs) use it; existing segments keep the chains
    /// recorded in their footers and are searched with those. The caller
    /// publishes the manifest.
    ///
    /// Returns whether anything changed (setting the same schema again is
    /// a no-op that does not bump the manifest version).
    pub fn set_schema(
        &self,
        namespace: &NamespaceId,
        schema: kosha_core::NamespaceSchema,
    ) -> Result<bool, KoshaError> {
        schema
            .resolve()
            .map_err(|e| KoshaError::InvalidQuery(format!("invalid schema: {e}")))?;
        let handle = self.ns_handle(namespace);
        let _mutation = handle.mutation.lock().unwrap();
        let mut state = handle.state.lock().unwrap();
        if state.known && state.manifest.schema.as_ref() == Some(&schema) {
            return Ok(false);
        }
        state.manifest.schema = Some(schema);
        state.manifest.version += 1;
        state.known = true;
        Ok(true)
    }

    /// Forget everything this writer holds for a deleted namespace: its
    /// manifest, write buffer, deletes, WAL-recovered records and every WAL
    /// record of it on disk, so neither a later flush nor a restart can
    /// bring the deleted documents back.
    ///
    /// Waits for in-flight writes, flushes, upserts and compactions on the
    /// namespace to finish. A write that arrives afterwards starts a new,
    /// empty incarnation (namespaces auto-create on first write) whose
    /// first manifest version is `next_version`, the version the control
    /// store will accept for a re-created namespace (see
    /// `ControlStore::delete_namespace`). The namespace reads as unknown
    /// (`manifest` is `None`) until something is written to it again.
    ///
    /// `while_locked` runs after the state is reset, before the
    /// namespace's locks are released: the place to remove its local
    /// segment files without racing a write to the new incarnation.
    ///
    /// Returns how many WAL files were kept because they also hold another
    /// namespace's records (legacy multi-record files only).
    pub fn drop_namespace(
        &self,
        namespace: &NamespaceId,
        next_version: u64,
        while_locked: impl FnOnce(),
    ) -> Result<usize, KoshaError> {
        let handle = self.ns_handle(namespace);
        // Same order every writer takes them: writes -> compact -> mutation
        // -> state.
        let _writes = handle.writes.lock().unwrap();
        let _compact = handle.compact.lock().unwrap();
        let _mutation = handle.mutation.lock().unwrap();
        let mut state = handle.state.lock().unwrap();
        let mut fresh = NamespaceState::new();
        fresh.manifest.version = next_version;
        fresh.published_version = next_version;
        // WAL sequences are process-wide; never hand one out twice.
        fresh.wal_sequence = state.wal_sequence;
        *state = fresh;
        drop(state);
        // Still under the namespace's locks: no new record can be appended
        // for it until the old ones are gone.
        let shared = self.wal.discard_namespace(namespace);
        while_locked();
        shared
    }

    pub fn manifest(&self, namespace: &NamespaceId) -> Option<Manifest> {
        self.manifest_cloned(namespace)
    }

    /// Cache parsed footers on the in-memory manifest for segments it lists
    /// and has no footer for yet (e.g. a query pod that just hydrated
    /// `footer.json` for segments published by another node). Later
    /// snapshots then skip the footer phase for those segments. Returns how
    /// many were cached. Never changes the manifest version or what gets
    /// published: footers are not persisted (#241).
    pub fn remember_segment_footers(
        &self,
        namespace: &NamespaceId,
        footers: impl IntoIterator<Item = std::sync::Arc<kosha_core::Footer>>,
    ) -> usize {
        let map = self.namespaces.lock().unwrap();
        let Some(handle) = map.get(namespace) else {
            return 0;
        };
        let mut state = handle.state.lock().unwrap();
        if !state.known {
            return 0;
        }
        footers
            .into_iter()
            .filter(|footer| {
                state
                    .manifest
                    .remember_listed_segment_footer(footer.clone())
            })
            .count()
    }

    pub fn manifest_cloned(&self, namespace: &NamespaceId) -> Option<Manifest> {
        self.publication_snapshot(namespace)
            .map(|(_, manifest)| manifest)
    }

    /// The namespace's manifest version, without cloning the manifest.
    pub fn manifest_version(&self, namespace: &NamespaceId) -> Option<u64> {
        let map = self.namespaces.lock().unwrap();
        let handle = map.get(namespace)?;
        let state = handle.state.lock().unwrap();
        state.known.then_some(state.manifest.version)
    }

    /// Replace the footers the manifest carries for segments it still
    /// references, and bump the version if any were replaced.
    ///
    /// The manifest's copy of a footer is the one searches prune with, so a
    /// footer rewritten on disk (new blooms) changes nothing until it is
    /// installed here. Each footer comes with the CRC32 of the rewritten
    /// `footer.json`; it is recorded in [`Manifest::footer_crc32`], which is
    /// persisted (footers are not, #241) and tells query replicas to drop
    /// their cached copy and local file. Returns how many were installed.
    pub fn replace_segment_footers(
        &self,
        namespace: &NamespaceId,
        footers: Vec<(kosha_core::Footer, u32)>,
    ) -> usize {
        let handle = self.ns_handle(namespace);
        let mut state = handle.state.lock().unwrap();
        let mut replaced = 0;
        for (footer, crc32) in footers {
            let referenced = state
                .manifest
                .segments
                .iter()
                .any(|entry| entry.segment_id == footer.segment_id);
            if referenced {
                state
                    .manifest
                    .footer_crc32
                    .insert(footer.segment_id.clone(), crc32);
                state.manifest.remember_segment_footer(footer);
                replaced += 1;
            }
        }
        if replaced > 0 {
            state.manifest.version += 1;
        }
        replaced
    }

    /// The manifest for a search (tombstones included), shared rather than
    /// copied: the same value [`Indexer::manifest_cloned`] returns.
    ///
    /// A manifest carries a footer for every segment, each with its term
    /// and filter blooms, so copying it is megabytes of allocation for a
    /// namespace of a few hundred segments, and every search made that copy
    /// before doing any work. This hands out one shared copy and rebuilds
    /// it only when the namespace changes (see [`ManifestStamp`]).
    pub fn manifest_shared(&self, namespace: &NamespaceId) -> Option<Arc<Manifest>> {
        let map = self.namespaces.lock().unwrap();
        let handle = map.get(namespace)?;
        let mut state = handle.state.lock().unwrap();
        if !state.known {
            return None;
        }
        let stamp = ManifestStamp::of(&state);
        if let Some((cached, manifest)) = state.shared_manifest.as_ref() {
            if *cached == stamp {
                return Some(Arc::clone(manifest));
            }
        }
        let mut manifest = state.manifest.clone();
        manifest.tombstones = state.tombstones.clone();
        manifest.deletion_vectors.retain(|segment, _| {
            manifest
                .segments
                .iter()
                .any(|entry| &entry.segment_id == segment)
        });
        let manifest = Arc::new(manifest);
        state.shared_manifest = Some((stamp, Arc::clone(&manifest)));
        state.snapshots.retain(|s| s.strong_count() > 0);
        state.snapshots.push(Arc::downgrade(&manifest));
        Some(manifest)
    }

    /// Capture the CAS anchor and candidate under one namespace lock. The
    /// anchor advances only after a successful control-store commit.
    pub fn publication_snapshot(&self, namespace: &NamespaceId) -> Option<(u64, Manifest)> {
        let map = self.namespaces.lock().unwrap();
        let handle = map.get(namespace)?;
        let state = handle.state.lock().unwrap();
        if !state.known {
            return None;
        }
        let mut manifest = state.manifest.clone();
        manifest.tombstones = state.tombstones.clone();
        let listed: std::collections::HashSet<SegmentId> = manifest
            .segments
            .iter()
            .map(|entry| entry.segment_id.clone())
            .collect();
        manifest
            .deletion_vectors
            .retain(|segment, _| listed.contains(segment));
        manifest
            .footer_crc32
            .retain(|segment, _| listed.contains(segment));
        Some((state.published_version, manifest))
    }

    /// Writes accepted for `namespace` but not yet visible to readers: the
    /// write buffer's documents plus every WAL-recovered record awaiting
    /// replay — its documents *and* its deletes, since a delete-only record
    /// also needs a flush + publish to take effect. `0` for an unknown
    /// namespace (never creates a handle). Used by the server's idle-buffer
    /// flush timer (`KOSHA_FLUSH_MAX_AGE_MS`) to tell which namespaces still
    /// have invisible writes.
    pub fn pending_doc_count(&self, namespace: &NamespaceId) -> usize {
        let map = self.namespaces.lock().unwrap();
        let Some(handle) = map.get(namespace) else {
            return 0;
        };
        let state = handle.state.lock().unwrap();
        let buffered = state.buffer.as_ref().map_or(0, |b| b.documents.len());
        let recovered: usize = state
            .recovered
            .iter()
            .map(|(_, record)| record.documents.len() + record.deleted_ids.len())
            .sum();
        buffered + recovered
    }

    /// Materialize immutable sidecars for precisely the snapshot being
    /// published. Sidecar I/O does not hold the namespace state lock.
    pub fn prepare_publication(
        &self,
        namespace: &NamespaceId,
    ) -> Result<Option<(u64, Manifest)>, KoshaError> {
        let Some((expected, mut manifest)) = self.publication_snapshot(namespace) else {
            return Ok(None);
        };
        {
            let handle = self.ns_handle(namespace);
            let mut state = handle.state.lock().unwrap();
            let base = PublishedBase::of(&manifest, &manifest.tombstones);
            state
                .pending_bases
                .retain(|(version, _)| *version != manifest.version);
            if state.pending_bases.len() >= MAX_PENDING_BASES {
                state.pending_bases.remove(0);
            }
            state.pending_bases.push((manifest.version, base));
        }
        // A no-op retry must keep the exact published representation,
        // including legacy inline tombstones, without a same-version migration.
        if expected == manifest.version && manifest.deletion_vectors.is_empty() {
            manifest.retired = self
                .ns_handle(namespace)
                .state
                .lock()
                .unwrap()
                .committed
                .retired
                .clone();
            return Ok(Some((expected, manifest)));
        }
        manifest.deletion_vectors.clear();
        for entry in &manifest.segments {
            if let Some(ids) = manifest
                .tombstones
                .get(&entry.segment_id)
                .filter(|ids| !ids.is_empty())
            {
                let (vector, bytes) = kosha_core::deletes::DeletionVector::encode(ids)?;
                let path = self
                    .data_dir
                    .join(vector.path(namespace, &entry.segment_id)?);
                std::fs::create_dir_all(path.parent().unwrap())?;
                crate::wal::durable_write(&path, &bytes)?;
                manifest
                    .deletion_vectors
                    .insert(entry.segment_id.clone(), vector);
            }
        }
        // Published manifests carry compact references; readers hydrate before
        // installing a snapshot. Legacy inline tombstones remain readable.
        manifest.tombstones.clear();
        {
            // Record what this publish stops referencing (#239), in the same
            // manifest — and so the same compare-and-swap — as the change.
            let handle = self.ns_handle(namespace);
            let mut state = handle.state.lock().unwrap();
            if expected == manifest.version {
                manifest.retired = state.committed.retired.clone();
            } else {
                let now = unix_now_ms();
                let state = &mut *state;
                let stamps = &mut state.retire_stamps;
                kosha_core::gc::record_retirements(&state.committed, &mut manifest, |seg, g| {
                    *stamps
                        .entry((seg.clone(), g.map(str::to_owned)))
                        .or_insert(now)
                });
                let keep: HashSet<(SegmentId, Option<String>)> = manifest
                    .retired
                    .iter()
                    .map(|o| (o.segment_id.clone(), o.deletion_generation.clone()))
                    .collect();
                stamps.retain(|key, _| keep.contains(key));
            }
        }
        Ok(Some((expected, manifest)))
    }

    pub fn needs_recovery_hydration(&self, namespace: &NamespaceId) -> bool {
        let handle = self.ns_handle(namespace);
        let state = handle.state.lock().unwrap();
        state.replay_required || !state.recovered_deletes.is_empty()
    }

    pub fn acknowledge_manifest(
        &self,
        namespace: &NamespaceId,
        manifest: &Manifest,
    ) -> Result<(), KoshaError> {
        // Advance even if WAL reclamation fails: the remote commit already
        // happened. A retry can then verify the same snapshot idempotently.
        let handle = self.ns_handle(namespace);
        let mut state = handle.state.lock().unwrap();
        if manifest.version >= state.published_version {
            // Without the snapshot's own deletes (the committed manifest
            // only carries sidecar references), an empty delete set is the
            // safe under-estimate: a later rebase then treats more deletes
            // as unpublished, which can only make it abort, never drop one.
            let pending = state
                .pending_bases
                .iter()
                .position(|(version, _)| *version == manifest.version);
            state.published_base = match pending {
                Some(at) => state.pending_bases.remove(at).1,
                None => PublishedBase::of(manifest, &HashMap::new()),
            };
            state
                .pending_bases
                .retain(|(version, _)| *version > manifest.version);
            let superseded = superseded_generations(&state.committed, manifest);
            self.retire_locally(&mut state, namespace, manifest.version, superseded);
            state.committed = CommittedRefs::of(manifest);
        }
        state.published_version = state.published_version.max(manifest.version);
        if state.manifest.version == manifest.version {
            state.manifest.deletion_vectors = manifest.deletion_vectors.clone();
            state.manifest.retired = manifest.retired.clone();
        }
        drop(state);
        if self.wal_enabled
            && manifest
                .wal_origin
                .as_deref()
                .is_none_or(|origin| origin == self.wal.origin())
        {
            self.wal.clear_through(namespace, manifest.wal_checkpoint)?;
        }
        Ok(())
    }

    /// Replay this writer's unpublished delta on top of `remote`, the
    /// manifest another writer committed after this writer's
    /// `published_version` (a `ManifestConflict` on publish).
    ///
    /// `remote` must have its deletion vectors hydrated into `tombstones`.
    /// The delta is measured against the last version this writer published
    /// or restored: segments it added (flushes, compaction outputs),
    /// segments it removed (compaction inputs), and deletes. The result is
    /// the remote segment list minus what this writer compacted away, plus
    /// what it added, with deletes merged per segment (segments are
    /// immutable, so a segment's deletes only ever grow and the union is
    /// exact). The WAL checkpoint stays this writer's own when it has one,
    /// so publishing the result clears exactly the WAL records it covers.
    ///
    /// Aborts with `ManifestConflict`, leaving local state untouched, when
    /// the two histories cannot be merged without losing data:
    /// - the remote removed a segment this writer compacted away (two
    ///   compactions of one input would publish its documents twice);
    /// - the remote added deletes to a segment this writer compacted away
    ///   (its compaction output would resurrect those documents);
    /// - the remote removed a segment that carries unpublished local
    ///   deletes (they could no longer be applied).
    ///
    /// The caller then retries the CAS against `remote.version`. Waits for
    /// an in-flight compaction or upsert on the namespace to finish.
    pub fn rebase_onto(
        &self,
        namespace: &NamespaceId,
        mut remote: Manifest,
    ) -> Result<RebaseOutcome, KoshaError> {
        let own_origin = self.wal.origin().to_owned();
        let handle = self.ns_handle(namespace);
        // Upserts and compactions hold `compact` across their I/O and
        // commit doc locations / merge inputs read before it; swapping the
        // segment list underneath them could strand their tombstones on a
        // segment the rebase just dropped.
        let _compact = handle.compact.lock().unwrap();
        let mut state = handle.state.lock().unwrap();
        let conflict = |state: &NamespaceState, reason: String| {
            eprintln!(
                "WARN: manifest rebase of namespace {} onto version {} aborted: {reason}",
                namespace.0, remote.version
            );
            KoshaError::ManifestConflict {
                namespace: namespace.clone(),
                expected: state.published_version,
                actual: Some(remote.version),
            }
        };
        if state.known && remote.version < state.published_version {
            let reason = format!(
                "remote version is older than this writer's published version {}",
                state.published_version
            );
            return Err(conflict(&state, reason));
        }
        let remote_tombstones = std::mem::take(&mut remote.tombstones);
        let remote_ids: HashSet<SegmentId> = remote
            .segments
            .iter()
            .map(|entry| entry.segment_id.clone())
            .collect();
        let local_ids: HashSet<SegmentId> = state
            .manifest
            .segments
            .iter()
            .map(|entry| entry.segment_id.clone())
            .collect();
        // A commit of this writer's whose reply was lost (the CAS landed,
        // the caller saw an error) is this writer's own history: take it as
        // the base rather than as a competing writer's change.
        if remote.wal_origin.as_deref() == Some(own_origin.as_str()) {
            let own = state.pending_bases.iter().position(|(version, base)| {
                *version == remote.version && base.segments == remote_ids
            });
            if let Some(at) = own {
                let (version, base) = state.pending_bases.remove(at);
                state.published_version = version;
                state.published_base = base;
            }
        }
        let local_changed = state.known && state.manifest.version != state.published_version;

        let (manifest, tombstones, outcome) = if local_changed {
            let base = &state.published_base;
            let mut removed = HashSet::new();
            for id in base.segments.difference(&local_ids) {
                if !remote_ids.contains(id) {
                    let reason = format!(
                        "segment {} was compacted away both locally and remotely",
                        id.0
                    );
                    return Err(conflict(&state, reason));
                }
                if base.new_deletes(id, &remote_tombstones) > 0 {
                    let reason = format!(
                        "remote deleted documents in segment {}, which this writer compacted away",
                        id.0
                    );
                    return Err(conflict(&state, reason));
                }
                removed.insert(id.clone());
            }
            for id in base.segments.intersection(&local_ids) {
                if !remote_ids.contains(id) && base.new_deletes(id, &state.tombstones) > 0 {
                    let reason = format!(
                        "remote removed segment {}, which has unpublished local deletes",
                        id.0
                    );
                    return Err(conflict(&state, reason));
                }
            }

            let mut segments: Vec<ManifestEntry> = remote
                .segments
                .iter()
                .filter(|entry| !removed.contains(&entry.segment_id))
                .cloned()
                .collect();
            let added: Vec<ManifestEntry> = state
                .manifest
                .segments
                .iter()
                .filter(|entry| {
                    !base.segments.contains(&entry.segment_id)
                        && !remote_ids.contains(&entry.segment_id)
                })
                .cloned()
                .collect();
            segments.extend(added);

            let mut tombstones: HashMap<SegmentId, HashSet<u32>> = HashMap::new();
            let mut footer_crc32 = HashMap::new();
            for entry in &segments {
                let id = &entry.segment_id;
                let mut ids = remote_tombstones.get(id).cloned().unwrap_or_default();
                if let Some(local) = state.tombstones.get(id) {
                    ids.extend(local.iter().copied());
                }
                if !ids.is_empty() {
                    tombstones.insert(id.clone(), ids);
                }
                // A footer rewrite is an admin action on one writer; prefer
                // the remote record for a segment both sides list.
                if let Some(crc) = remote
                    .footer_crc32
                    .get(id)
                    .or_else(|| state.manifest.footer_crc32.get(id))
                {
                    footer_crc32.insert(id.clone(), *crc);
                }
            }

            let (wal_checkpoint, wal_origin) =
                if state.manifest.wal_origin.as_deref() == Some(own_origin.as_str()) {
                    let mut checkpoint = state.manifest.wal_checkpoint;
                    if remote.wal_origin.as_deref() == Some(own_origin.as_str()) {
                        checkpoint = checkpoint.max(remote.wal_checkpoint);
                    }
                    (checkpoint, Some(own_origin.clone()))
                } else {
                    (remote.wal_checkpoint, remote.wal_origin.clone())
                };

            let manifest = Manifest {
                version: state.manifest.version.max(remote.version) + 1,
                segments,
                segment_footers: Default::default(),
                footer_crc32,
                wal_checkpoint,
                wal_origin,
                tombstones: tombstones.clone(),
                // Rebuilt from `tombstones` by `prepare_publication`.
                deletion_vectors: Default::default(),
                // A schema set on this writer since its last publication
                // wins (it is the newer admin action); otherwise keep the
                // remote's, which may carry another writer's change.
                schema: if state.manifest.schema != base.schema {
                    state.manifest.schema.clone()
                } else {
                    remote.schema.clone()
                },
                retired: Default::default(),
            };
            (manifest, tombstones, RebaseOutcome::Rebased)
        } else {
            let mut manifest = remote.clone();
            manifest.tombstones = remote_tombstones.clone();
            (manifest, remote_tombstones.clone(), RebaseOutcome::Adopted)
        };

        let mut manifest = manifest;
        manifest.carry_segment_footers_from(&state.manifest);
        manifest.carry_segment_footers_from(&remote);
        if remote.wal_origin.as_deref() == Some(own_origin.as_str()) {
            self.wal.advance_past(remote.wal_checkpoint);
            state.wal_sequence = state.wal_sequence.max(remote.wal_checkpoint);
        }
        let max_counter = manifest
            .segments
            .iter()
            .filter_map(|e| segment_flush_counter(&e.segment_id))
            .max();
        if let Some(max) = max_counter {
            let buf = Self::buffer_mut_in(
                &mut state,
                namespace.clone(),
                &self.data_dir,
                &self.bm25_params,
            );
            buf.segment_counter = buf.segment_counter.max(max + 1);
        }
        remote.tombstones = remote_tombstones;
        state.published_base = PublishedBase::of(&remote, &remote.tombstones);
        state.pending_bases.clear();
        state.published_version = remote.version;
        state.tombstones = tombstones;
        let mut retired = dropped_segments(&state.manifest, &manifest);
        retired.extend(superseded_generations(&state.committed, &remote));
        self.retire_locally(&mut state, namespace, manifest.version, retired);
        state.committed = CommittedRefs::of(&remote);
        state.manifest = manifest;
        state.id_index = None;
        state.shared_manifest = None;
        state.known = true;
        Ok(outcome)
    }

    /// Queue `objects` (segment, optional deletion generation) for local
    /// removal once nothing can read them (#239). Every search snapshot
    /// alive right now pins them.
    fn retire_locally(
        &self,
        state: &mut NamespaceState,
        namespace: &NamespaceId,
        after_version: u64,
        objects: Vec<(SegmentId, Option<String>)>,
    ) {
        if objects.is_empty() {
            return;
        }
        // The cached snapshot references the retired segments too; drop it
        // so it does not pin them forever. It is rebuilt on the next search.
        state.shared_manifest = None;
        state.snapshots.retain(|s| s.strong_count() > 0);
        let pins = state.snapshots.clone();
        let now = Instant::now();
        let mut queue = self.local_retired.lock().unwrap();
        for (segment_id, generation) in objects {
            let entry = LocalRetirement {
                namespace: namespace.clone(),
                segment_id,
                generation,
                since: now,
                after_version,
                pins: pins.clone(),
            };
            if !queue.iter().any(|e| e.same_object(&entry)) {
                queue.push(entry);
            }
        }
    }

    /// Remove the local files of retired segments and deletion generations
    /// that no reader can still use (#239).
    ///
    /// An object goes only when all of these hold:
    /// - it was retired at least `grace` ago;
    /// - this node has committed the manifest version that dropped it;
    /// - every search snapshot ([`Indexer::manifest_shared`]) alive when it
    ///   was retired has been dropped;
    /// - the namespace's current manifest does not reference it again;
    /// - for a segment, `in_use(namespace, segment)` is false. The server
    ///   passes the searcher's open-segment check, which also drops the
    ///   segment from its in-memory cache.
    ///
    /// `remove(relative_path)` deletes a segment directory or one deletion
    /// generation file (paths relative to the data dir) and returns the
    /// bytes it freed. Objects that fail stay queued for the next pass.
    pub fn reclaim_local(
        &self,
        grace: Duration,
        in_use: impl Fn(&NamespaceId, &SegmentId) -> bool,
        remove: impl Fn(&str) -> Result<u64, KoshaError>,
    ) -> LocalReclaimReport {
        let queued: Vec<LocalRetirement> = self.local_retired.lock().unwrap().clone();
        let mut report = LocalReclaimReport::default();
        let mut done: Vec<LocalRetirement> = Vec::new();
        for entry in queued {
            if entry.since.elapsed() < grace || entry.pins.iter().any(|p| p.strong_count() > 0) {
                report.pending += 1;
                continue;
            }
            let (referenced, committed) = {
                let handle = self
                    .namespaces
                    .lock()
                    .unwrap()
                    .get(&entry.namespace)
                    .cloned();
                handle.map_or((false, true), |handle| {
                    let state = handle.state.lock().unwrap();
                    let committed = state.published_version >= entry.after_version;
                    let listed = state
                        .manifest
                        .segments
                        .iter()
                        .any(|e| e.segment_id == entry.segment_id);
                    let referenced = match &entry.generation {
                        None => listed,
                        Some(g) => {
                            listed
                                && [
                                    &state.manifest.deletion_vectors,
                                    &state.committed.deletion_vectors,
                                ]
                                .iter()
                                .any(|dvs| {
                                    dvs.get(&entry.segment_id)
                                        .is_some_and(|v| &v.generation == g)
                                })
                        }
                    };
                    (referenced, committed)
                })
            };
            if !committed {
                report.pending += 1;
                continue;
            }
            if referenced {
                done.push(entry);
                continue;
            }
            if entry.generation.is_none() && in_use(&entry.namespace, &entry.segment_id) {
                report.pending += 1;
                continue;
            }
            let rel = entry.relative_path();
            if !self.data_dir.join(&rel).exists() {
                done.push(entry);
                continue;
            }
            match remove(&rel) {
                Ok(bytes) => {
                    report.bytes_removed += bytes;
                    if entry.generation.is_none() {
                        report.segments_removed += 1;
                    } else {
                        report.deletion_vectors_removed += 1;
                    }
                    done.push(entry);
                }
                Err(e) => {
                    eprintln!("WARN: local GC could not remove {rel}: {e}");
                    report.pending += 1;
                }
            }
        }
        if !done.is_empty() {
            self.local_retired
                .lock()
                .unwrap()
                .retain(|e| !done.iter().any(|d| d.same_object(e)));
        }
        report
    }

    /// Retired objects whose local files are still waiting for
    /// [`Indexer::reclaim_local`].
    pub fn local_retired_count(&self) -> usize {
        self.local_retired.lock().unwrap().len()
    }

    /// Whole segments the committed manifests of every namespace list as
    /// retired, waiting for object-store GC (`kosha_retired_segments`).
    pub fn retired_segment_count(&self) -> usize {
        let handles: Vec<Arc<NamespaceHandle>> =
            self.namespaces.lock().unwrap().values().cloned().collect();
        handles
            .iter()
            .map(|h| {
                h.state
                    .lock()
                    .unwrap()
                    .committed
                    .retired
                    .iter()
                    .filter(|o| o.deletion_generation.is_none())
                    .count()
            })
            .sum()
    }

    pub fn namespaces(&self) -> Vec<NamespaceId> {
        let map = self.namespaces.lock().unwrap();
        map.iter()
            .filter_map(|(id, handle)| {
                let state = handle.state.lock().unwrap();
                if state.known {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Segments `old` lists that `new` does not.
fn dropped_segments(old: &Manifest, new: &Manifest) -> Vec<(SegmentId, Option<String>)> {
    let listed: HashSet<&SegmentId> = new.segments.iter().map(|e| &e.segment_id).collect();
    old.segments
        .iter()
        .filter(|e| !listed.contains(&e.segment_id))
        .map(|e| (e.segment_id.clone(), None))
        .collect()
}

/// Deletion generations `old` committed for segments `new` still lists
/// under a different generation.
fn superseded_generations(old: &CommittedRefs, new: &Manifest) -> Vec<(SegmentId, Option<String>)> {
    old.deletion_vectors
        .iter()
        .filter(|(seg, vector)| {
            new.segments.iter().any(|e| &e.segment_id == *seg)
                && new
                    .deletion_vectors
                    .get(*seg)
                    .is_none_or(|v| v.generation != vector.generation)
        })
        .map(|(seg, vector)| (seg.clone(), Some(vector.generation.clone())))
        .collect()
}

/// Parse the flush counter from a segment ID of the form `{ns}-{counter:06x}`
/// produced by `flush_namespace`. Returns `None` for IDs from other sources
/// (e.g. compaction segments, which embed a timestamp instead of a counter).
fn merge_fields(
    mut base: Vec<kosha_core::Field>,
    patch: Vec<kosha_core::Field>,
) -> Vec<kosha_core::Field> {
    for field in patch {
        if let Some(existing) = base
            .iter_mut()
            .find(|candidate| candidate.name == field.name)
        {
            *existing = field;
        } else {
            base.push(field);
        }
    }
    base
}

/// Last occurrence of each `doc_id` wins (OpenSearch bulk last-write-wins).
fn dedupe_documents_last_wins(documents: Vec<Document>) -> Vec<Document> {
    let mut last_by_id: HashMap<String, Document> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for document in documents {
        let id = document.id.0.clone();
        if last_by_id.insert(id.clone(), document).is_none() {
            order.push(id);
        }
    }
    order
        .into_iter()
        .filter_map(|id| last_by_id.remove(&id))
        .collect()
}

fn segment_flush_counter(segment_id: &SegmentId) -> Option<u64> {
    let suffix = segment_id.0.rsplit('-').next()?;
    if suffix.len() == 6 && suffix.bytes().all(|b| b.is_ascii_hexdigit()) {
        u64::from_str_radix(suffix, 16).ok()
    } else {
        None
    }
}

/// Sum of file sizes directly inside a segment dir, for the
/// `max_merged_segment_bytes` merge cap. `None` when the dir is unreadable
/// — the planner then conservatively leaves that segment unmerged.
fn segment_dir_bytes(dir: &std::path::Path) -> Option<u64> {
    let entries = std::fs::read_dir(dir).ok()?;
    let mut total = 0u64;
    for entry in entries.flatten() {
        if let Ok(md) = entry.metadata() {
            if md.is_file() {
                total = total.saturating_add(md.len());
            }
        }
    }
    Some(total)
}

/// Standalone filter applier for delete operations (no Searcher dependency).
pub fn apply_filter_delete(
    filter: &FilterClause,
    store: &FilterStore,
    candidates: &HashSet<u32>,
) -> Result<HashSet<u32>, KoshaError> {
    // The one filter implementation, shared with search (kosha-core's
    // `docset`), so a delete matches exactly the documents a search with
    // the same filter returns.
    // A clause this segment cannot answer (a `prefix` on a numeric field)
    // fails the whole delete before anything is tombstoned.
    kosha_core::check_filter(filter, store).map_err(KoshaError::InvalidQuery)?;
    let doc_count = candidates.iter().max().map_or(0, |max| max + 1);
    let passing = kosha_core::evaluate_filter(filter, store, doc_count);
    Ok(candidates
        .iter()
        .copied()
        .filter(|doc_seq| passing.contains(*doc_seq))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::{DocumentId, Field, FilterClause};

    fn live_records(
        dir: &Path,
        ns: &NamespaceId,
        manifest: &Manifest,
    ) -> Vec<kosha_core::DocRecord> {
        manifest
            .segments
            .iter()
            .flat_map(|entry| {
                kosha_segment::SegmentReader::open(dir.join(&ns.0).join(&entry.segment_id.0))
                    .unwrap()
                    .iter_doc_records()
                    .map(|r| r.unwrap())
                    .filter(|record| {
                        !manifest
                            .tombstones
                            .get(&entry.segment_id)
                            .is_some_and(|ids| ids.contains(&record.doc_seq))
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn dropped_namespace_loses_its_buffer_and_wal_but_not_its_neighbours() {
        let dir = std::env::temp_dir().join(format!("kosha-drop-ns-{}", uuid::Uuid::new_v4()));
        let gone = NamespaceId("t/gone".into());
        let kept = NamespaceId("t/kept".into());
        let doc = |id: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", "hello")],
        };
        let idx = Indexer::new(dir.clone());
        idx.index_documents(gone.clone(), vec![doc("a")]).unwrap();
        idx.flush_namespace(&gone).unwrap();
        idx.index_documents(gone.clone(), vec![doc("b")]).unwrap();
        idx.index_documents(kept.clone(), vec![doc("c")]).unwrap();
        assert_eq!(idx.pending_doc_count(&gone), 1);
        let published = idx.manifest(&gone).unwrap().version;

        assert_eq!(idx.drop_namespace(&gone, published + 1, || {}).unwrap(), 0);
        assert!(
            idx.manifest(&gone).is_none(),
            "a dropped namespace is unknown"
        );
        assert_eq!(idx.pending_doc_count(&gone), 0);
        assert!(!idx.namespaces().contains(&gone));
        assert_eq!(idx.pending_doc_count(&kept), 1);

        // A restart replays only the surviving namespace's WAL.
        drop(idx);
        let restart = Indexer::new(dir.clone());
        assert!(
            restart.manifest(&gone).is_none(),
            "WAL must not resurrect it"
        );
        // (Counted once buffered and once as a record awaiting replay.)
        assert!(
            restart.pending_doc_count(&kept) > 0,
            "other WAL records survive"
        );
        assert_eq!(restart.pending_doc_count(&gone), 0);
        drop(restart);

        // Writing again starts a new, empty incarnation above the old
        // versions.
        let idx = Indexer::new(dir.clone());
        idx.drop_namespace(&gone, published + 1, || {}).unwrap();
        idx.index_documents(gone.clone(), vec![doc("d")]).unwrap();
        idx.flush_namespace(&gone).unwrap();
        let manifest = idx.manifest(&gone).unwrap();
        assert_eq!(manifest.version, published + 2);
        let (expected, _) = idx.publication_snapshot(&gone).unwrap();
        assert_eq!(expected, published + 1);
        let live = live_records(&dir, &gone, &manifest);
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].doc_id.0, "d");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn upsert_racing_a_flush_does_not_leave_two_live_versions() {
        let dir = std::env::temp_dir().join(format!("kosha-upsert-flush-{}", uuid::Uuid::new_v4()));
        let ns = NamespaceId("ns".into());
        let idx = Arc::new(Indexer::new(dir.clone()).with_max_concurrent_flush_writers(1));
        let doc = |value: &str| Document {
            id: DocumentId("same".into()),
            fields: vec![Field::text("text", value)],
        };
        idx.index_documents(ns.clone(), vec![doc("old")]).unwrap();
        let handle = idx.ns_handle(&ns);
        let permit = handle.writer_slots.acquire();
        std::thread::scope(|scope| {
            let flush = scope.spawn(|| idx.flush_namespace(&ns).unwrap());
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while handle.state.lock().unwrap().flushing_ids.is_empty() {
                assert!(std::time::Instant::now() < deadline);
                std::thread::yield_now();
            }
            let replacement =
                scope.spawn(|| idx.index_documents(ns.clone(), vec![doc("new")]).unwrap());
            drop(permit);
            flush.join().unwrap();
            replacement.join().unwrap();
        });
        idx.flush_namespace(&ns).unwrap();
        let live = live_records(&dir, &ns, &idx.manifest(&ns).unwrap());
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].fields[0].value, "new");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn upsert_is_atomic_without_state_lock_io_and_preserves_buffer_checkpoint() {
        let dir =
            std::env::temp_dir().join(format!("kosha-upsert-atomic-{}", uuid::Uuid::new_v4()));
        let ns = NamespaceId("ns".into());
        let idx = Indexer::new(dir.clone());
        let doc = |id: &str, value: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", value)],
        };
        idx.index_documents(
            ns.clone(),
            vec![doc("old", "first"), doc("untouched", "keep")],
        )
        .unwrap();
        idx.flush_namespace(&ns).unwrap();
        let committed = idx.manifest(&ns).unwrap();
        idx.acknowledge_manifest(&ns, &committed).unwrap();
        let old_path = dir
            .join(&ns.0)
            .join(&committed.segments[0].segment_id.0)
            .join("doc_store.bin");
        let old_bytes = std::fs::read(&old_path).unwrap();
        idx.index_documents(ns.clone(), vec![doc("pending", "buffered")])
            .unwrap();
        idx.upsert_documents_with_hook(ns.clone(), vec![doc("old", "new")], false, || {
            let handle = idx.ns_handle(&ns);
            let state = handle
                .state
                .try_lock()
                .expect("disk I/O must not hold namespace state");
            assert_eq!(state.manifest.version, committed.version);
            assert!(state.tombstones.is_empty());
        })
        .unwrap();
        let manifest = idx.manifest(&ns).unwrap();
        assert_eq!(manifest.segments.len(), 2);
        assert_eq!(manifest.segments[1].doc_count, 1);
        assert_eq!(std::fs::read(old_path).unwrap(), old_bytes);
        assert_eq!(
            manifest.wal_checkpoint, committed.wal_checkpoint,
            "pending buffer is not covered by the upsert publication"
        );
        idx.acknowledge_manifest(&ns, &manifest).unwrap();
        let restart = Indexer::new(dir.clone());
        restart.restore_manifest(ns.clone(), manifest);
        restart.flush_namespace(&ns).unwrap();
        let records = live_records(&dir, &ns, &restart.manifest(&ns).unwrap());
        assert_eq!(records.len(), 3);
        assert!(records.iter().any(|r| r.doc_id.0 == "pending"));
        assert_eq!(
            records.iter().find(|r| r.doc_id.0 == "old").unwrap().fields[0].value,
            "new"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn independent_writers_never_reuse_segment_object_keys() {
        let base = std::env::temp_dir().join("kosha-independent-writer-ids");
        let _ = std::fs::remove_dir_all(&base);
        let ns = NamespaceId("ns".into());
        let mut ids = HashSet::new();
        for writer in ["a", "b"] {
            let idx = Indexer::new(base.join(writer));
            idx.index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId(writer.into()),
                    fields: vec![Field::text("text", writer)],
                }],
            )
            .unwrap();
            idx.flush_namespace(&ns).unwrap();
            assert!(
                ids.insert(idx.manifest(&ns).unwrap().segments[0].segment_id.clone()),
                "independent writers must not overwrite the same S3 keys"
            );
        }
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn regression_delete_survives_manifest_restore_and_covers_buffer() {
        let dir = std::env::temp_dir().join("kosha-regression-delete-durable");
        let _ = std::fs::remove_dir_all(&dir);
        let idx = Indexer::new(dir.clone());
        let ns = NamespaceId("ns".into());
        for id in ["flushed", "buffered"] {
            idx.index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId(id.into()),
                    fields: vec![Field::keyword("status", "remove")],
                }],
            )
            .unwrap();
            if id == "flushed" {
                idx.flush_namespace(&ns).unwrap();
            }
        }
        let before = idx.manifest(&ns).unwrap();
        let filter = FilterClause::Term {
            term: HashMap::from([("status".into(), "remove".into())]),
        };
        let deleted = idx.delete_by_query(&ns, &before, &filter).unwrap();
        assert_eq!(deleted, 2, "deletion must include buffered documents");
        let published = idx.manifest(&ns).unwrap();
        assert!(published.version > before.version);
        let restored = Indexer::new(dir.clone());
        restored.restore_manifest(ns.clone(), published);
        assert!(restored
            .existing_ids(
                &ns,
                &[DocumentId("flushed".into()), DocumentId("buffered".into())]
            )
            .unwrap()
            .is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn delete_before_publication_survives_crash_and_later_reinsert() {
        let dir = std::env::temp_dir().join("kosha-delete-wal-recovery");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("ns".into());
        let doc = |id: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::keyword("status", "remove")],
        };
        let idx = Indexer::new(dir.clone());
        idx.index_documents(ns.clone(), vec![doc("published")])
            .unwrap();
        idx.flush_namespace(&ns).unwrap();
        let committed = idx.manifest(&ns).unwrap();
        idx.acknowledge_manifest(&ns, &committed).unwrap();
        idx.index_documents(ns.clone(), vec![doc("buffered")])
            .unwrap();
        let filter = FilterClause::Term {
            term: HashMap::from([("status".into(), "remove".into())]),
        };
        assert_eq!(idx.delete_by_query(&ns, &committed, &filter).unwrap(), 2);
        drop(idx); // no publication of the delete
        let restart = Indexer::new(dir.clone());
        restart.restore_manifest(ns.clone(), committed);
        restart.flush_namespace(&ns).unwrap();
        assert!(restart
            .existing_ids(
                &ns,
                &[
                    DocumentId("published".into()),
                    DocumentId("buffered".into())
                ]
            )
            .unwrap()
            .is_empty());
        restart
            .index_documents(ns.clone(), vec![doc("buffered")])
            .unwrap();
        restart.flush_namespace(&ns).unwrap();
        assert!(restart
            .existing_ids(&ns, &[DocumentId("buffered".into())])
            .unwrap()
            .contains(&DocumentId("buffered".into())));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn regression_recovered_upsert_replaces_committed_version() {
        let dir = std::env::temp_dir().join("kosha-regression-replay-upsert");
        let _ = std::fs::remove_dir_all(&dir);
        let idx = Indexer::new(dir.clone());
        let ns = NamespaceId("ns".into());
        let doc = |text: &str| Document {
            id: DocumentId("same".into()),
            fields: vec![Field::text("text", text)],
        };
        idx.index_documents(ns.clone(), vec![doc("old")]).unwrap();
        idx.flush_namespace(&ns).unwrap();
        let committed = idx.manifest(&ns).unwrap();
        idx.acknowledge_manifest(&ns, &committed).unwrap();
        // Crash after a new upsert's WAL append, before its segment commit.
        idx.wal.append(&ns, &[doc("new")]).unwrap();
        let restart = Indexer::new(dir.clone());
        restart.restore_manifest(ns.clone(), committed);
        restart.flush_namespace(&ns).unwrap();
        let manifest = restart.manifest(&ns).unwrap();
        let records = live_records(&dir, &ns, &manifest);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].fields[0].value, "new");
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// #241: persisted manifests carry no footers. A reader that installs a
    /// newer version must keep the footers it already parsed for segments
    /// that version still lists (zero footer I/O on refresh), and the search
    /// path can cache footers it hydrated for new segments.
    #[test]
    fn restore_manifest_carries_parsed_footers_across_versions() {
        let dir = std::env::temp_dir().join(format!("kosha-footer-carry-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let writer = Indexer::new(dir.join("writer"));
        let ns = NamespaceId("a".into());
        let doc = |id: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", id)],
        };
        writer
            .index_documents(ns.clone(), vec![doc("one")])
            .unwrap();
        writer.flush_namespace(&ns).unwrap();
        let v1 = writer.manifest(&ns).unwrap();
        let seg1 = v1.segments[0].segment_id.clone();
        assert!(
            v1.segment_footer(&seg1).is_some(),
            "writer keeps footers in memory"
        );

        // Round-trip through the persisted form, as a query pod sees it.
        let persisted = |m: &Manifest| -> Manifest {
            serde_json::from_str(&serde_json::to_string(m).unwrap()).unwrap()
        };
        let reader = Indexer::new(dir.join("reader"));
        reader.restore_manifest(ns.clone(), persisted(&v1));
        assert!(reader.manifest(&ns).unwrap().segment_footers.is_empty());

        // The search path hydrated footer.json and caches the parse.
        let footer = Arc::new(v1.segment_footer(&seg1).unwrap().clone());
        assert_eq!(reader.remember_segment_footers(&ns, [footer.clone()]), 1);
        assert_eq!(reader.remember_segment_footers(&ns, [footer.clone()]), 0);

        writer
            .index_documents(ns.clone(), vec![doc("two")])
            .unwrap();
        writer.flush_namespace(&ns).unwrap();
        let v2 = writer.manifest(&ns).unwrap();
        assert!(v2.version > v1.version);
        reader.restore_manifest(ns.clone(), persisted(&v2));
        let installed = reader.manifest(&ns).unwrap();
        assert_eq!(installed.version, v2.version);
        assert!(
            Arc::ptr_eq(&installed.segment_footers[&seg1], &footer),
            "surviving segment's parsed footer is reused, not re-read"
        );
        assert_eq!(
            installed.segment_footers.len(),
            1,
            "new segment not cached yet"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// #243: compaction reads every input doc through `iter_doc_records_raw`,
    /// which must hand sidecar vectors over, or the merged segment would
    /// lose them. The merged segment stores them in a sidecar again, and
    /// they are bit-identical (no JSON round trip since #242).
    #[test]
    fn compaction_preserves_sidecar_vectors() {
        let dir = std::env::temp_dir().join(format!("kosha-compact-docvec-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let idx = Indexer::new(dir.clone());
        let ns = NamespaceId("v".into());
        let vector = |i: usize| -> Vec<f32> { vec![i as f32 + 0.1, -(i as f32), 0.5, 1.0e-3] };
        for i in 0..3 {
            idx.index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId(format!("d{i}")),
                    fields: vec![
                        Field::text("text", format!("doc {i}")),
                        Field {
                            name: "emb".into(),
                            field_type: kosha_core::FieldType::Vector,
                            value: serde_json::to_string(&vector(i)).unwrap(),
                        },
                    ],
                }],
            )
            .unwrap();
            idx.flush_namespace(&ns).unwrap();
        }
        let before = idx.manifest(&ns).unwrap();
        assert_eq!(before.segments.len(), 3);
        assert!(before
            .segments
            .iter()
            .all(|e| before.segment_footer(&e.segment_id).unwrap().doc_vectors));

        let result = idx
            .compact_namespace_with_options(&ns, CompactOptions::full())
            .unwrap();
        assert!(result.merged);
        let after = idx.manifest(&ns).unwrap();
        assert_eq!(after.segments.len(), 1);
        let seg_dir = dir.join(&ns.0).join(&after.segments[0].segment_id.0);
        let reader = kosha_segment::SegmentReader::open(seg_dir).unwrap();
        assert!(
            reader.footer().doc_vectors,
            "merged segment uses the sidecar"
        );
        let mut seen = 0;
        for record in reader.iter_doc_records() {
            let record = record.unwrap();
            let i: usize = record.doc_id.0[1..].parse().unwrap();
            let emb = record.fields.iter().find(|f| f.name == "emb").unwrap();
            let got: Vec<f32> = serde_json::from_str(&emb.value).unwrap();
            let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
            assert_eq!(bits(&got), bits(&vector(i)), "doc {i}");
            seen += 1;
        }
        assert_eq!(seen, 3);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn object_wal_recovers_acked_writes_after_losing_the_data_dir() {
        use crate::object_wal::{MemoryObjectBackend, ObjectWalStore};
        let backend = MemoryObjectBackend::new();
        let store = || -> Arc<dyn wal::WalStore> {
            Arc::new(ObjectWalStore::new(Arc::new(backend.clone()), "ingest-0").unwrap())
        };
        let temp = || std::env::temp_dir().join(format!("kosha-s3-wal-{}", uuid::Uuid::new_v4()));
        let ns = NamespaceId("t/ns".into());
        let doc = |id: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", "durable")],
        };

        // Publish a segment first, so recovery has a checkpoint to honor.
        let first = temp();
        let idx = Indexer::with_wal_store(first.clone(), store()).unwrap();
        idx.index_documents(ns.clone(), vec![doc("flushed")])
            .unwrap();
        idx.flush_namespace(&ns).unwrap();
        let published = idx.manifest(&ns).unwrap();
        idx.acknowledge_manifest(&ns, &published).unwrap();
        idx.index_documents(ns.clone(), vec![doc("a"), doc("b")])
            .unwrap();
        // Node loss: no shutdown flush, the whole data_dir is gone. Only
        // the published segment survives (in S3; copied here the way the
        // server hydrates it before a write).
        std::mem::forget(idx);
        let second = temp();
        let copy = |from: &Path, to: &Path| {
            for segment in std::fs::read_dir(from).unwrap() {
                let segment = segment.unwrap().path();
                let target = to.join(segment.file_name().unwrap());
                std::fs::create_dir_all(&target).unwrap();
                for file in std::fs::read_dir(&segment).unwrap() {
                    let file = file.unwrap().path();
                    std::fs::copy(&file, target.join(file.file_name().unwrap())).unwrap();
                }
            }
        };
        copy(&first.join(&ns.0), &second.join(&ns.0));
        std::fs::remove_dir_all(&first).unwrap();

        let idx = Indexer::with_wal_store(second.clone(), store()).unwrap();
        idx.restore_manifest(ns.clone(), published.clone());
        assert_eq!(
            idx.pending_doc_count(&ns),
            2,
            "only unflushed writes replay"
        );
        idx.flush_namespace(&ns).unwrap();
        let manifest = idx.manifest(&ns).unwrap();
        assert_eq!(manifest.wal_origin, published.wal_origin);
        assert!(manifest.wal_checkpoint > published.wal_checkpoint);
        let docs: u32 = manifest.segments.iter().map(|s| s.doc_count).sum();
        assert_eq!(docs, 3);
        idx.acknowledge_manifest(&ns, &manifest).unwrap();
        assert!(
            !backend.keys().iter().any(|k| k.ends_with(".bin")),
            "objects at or below the published checkpoint are reclaimed: {:?}",
            backend.keys()
        );
        std::fs::remove_dir_all(second).unwrap();
    }

    #[test]
    fn regression_fresh_disk_advances_wal_past_remote_checkpoint() {
        let dir = std::env::temp_dir().join("kosha-regression-fresh-wal");
        let _ = std::fs::remove_dir_all(&dir);
        let idx = Indexer::new(dir.clone());
        let ns = NamespaceId("a".into());
        let mut manifest = NamespaceState::new().manifest;
        manifest.wal_checkpoint = 100;
        manifest.version = 5;
        idx.restore_manifest(ns.clone(), manifest);
        idx.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("new".into()),
                fields: vec![Field::text("text", "new")],
            }],
        )
        .unwrap();
        let records = wal::WalWriter::recover_sequenced(&dir.join("_wal")).unwrap();
        assert!(records[0].0 > 100);
        idx.flush_namespace(&ns).unwrap();
        assert!(idx.manifest(&ns).unwrap().wal_checkpoint > 100);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn regression_published_checkpoint_preserves_later_writes_and_replay() {
        let dir = std::env::temp_dir().join("kosha-regression-checkpoint");
        let _ = std::fs::remove_dir_all(&dir);
        let idx = Indexer::new(dir.clone());
        let ns = NamespaceId("a".into());
        let doc = |id: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", id)],
        };
        idx.index_documents(ns.clone(), vec![doc("published")])
            .unwrap();
        idx.flush_namespace(&ns).unwrap();
        let manifest = idx.manifest(&ns).unwrap();
        idx.index_documents(ns.clone(), vec![doc("later")]).unwrap();
        idx.index_documents(NamespaceId("b".into()), vec![doc("other")])
            .unwrap();
        // Crash after publishing, before reclamation: checkpoint prevents replay
        // of already committed records without dropping later writes.
        let restart = Indexer::new(dir.clone());
        restart.restore_manifest(ns.clone(), manifest.clone());
        restart.flush_namespace(&ns).unwrap();
        assert_eq!(
            restart
                .manifest(&ns)
                .unwrap()
                .segments
                .iter()
                .map(|e| e.doc_count)
                .sum::<u32>(),
            2
        );
        idx.acknowledge_manifest(&ns, &manifest).unwrap();
        let records = wal::WalWriter::recover(&dir.join("_wal")).unwrap();
        let ids: HashSet<_> = records
            .iter()
            .flat_map(|r| r.documents.iter().map(|d| d.id.0.as_str()))
            .collect();
        assert_eq!(ids, HashSet::from(["later", "other"]));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn regression_flush_preserves_other_namespace_wal() {
        let dir = std::env::temp_dir().join("kosha-regression-wal-isolation");
        let _ = std::fs::remove_dir_all(&dir);
        let idx = Indexer::new(dir.clone());
        for ns in ["a", "b"] {
            idx.index_documents(
                NamespaceId(ns.into()),
                vec![Document {
                    id: DocumentId(ns.into()),
                    fields: vec![Field::text("text", ns)],
                }],
            )
            .unwrap();
        }
        idx.flush_namespace(&NamespaceId("a".into())).unwrap();
        let recovered = wal::WalWriter::recover(&dir.join("_wal")).unwrap();
        assert!(recovered.iter().any(|r| r.namespace.0 == "b"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn flush_of_one_namespace_keeps_a_shared_group_file_until_both_checkpoint() {
        let dir = std::env::temp_dir().join(format!("kosha-wal-shared-{}", uuid::Uuid::new_v4()));
        let (a, b) = (NamespaceId("a".into()), NamespaceId("b".into()));
        let doc = |id: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", id)],
        };
        let wal_files = || {
            std::fs::read_dir(dir.join("_wal"))
                .unwrap()
                .filter(|e| {
                    e.as_ref()
                        .unwrap()
                        .path()
                        .extension()
                        .is_some_and(|x| x == "wal")
                })
                .count()
        };
        // One group commit holding both namespaces' records, then a crash.
        wal::WalWriter::new(dir.join("_wal"))
            .append_records(&[
                kosha_core::WalRecord::new(a.clone(), vec![doc("a1")]),
                kosha_core::WalRecord::new(b.clone(), vec![doc("b1")]),
            ])
            .unwrap();
        assert_eq!(wal_files(), 1);

        let idx = Indexer::new(dir.clone());
        idx.flush_namespace(&a).unwrap();
        let published_a = idx.manifest(&a).unwrap();
        idx.acknowledge_manifest(&a, &published_a).unwrap();
        assert_eq!(wal_files(), 1, "B's record still needs the shared file");
        drop(idx);

        let restart = Indexer::new(dir.clone());
        restart.restore_manifest(a.clone(), published_a);
        restart.flush_namespace(&a).unwrap();
        restart.flush_namespace(&b).unwrap();
        let count = |ns: &NamespaceId| {
            restart
                .manifest(ns)
                .unwrap()
                .segments
                .iter()
                .map(|e| e.doc_count)
                .sum::<u32>()
        };
        assert_eq!(count(&a), 1, "A's checkpointed record is not replayed");
        assert_eq!(count(&b), 1, "B is recovered after A's flush");
        let published_b = restart.manifest(&b).unwrap();
        restart.acknowledge_manifest(&b, &published_b).unwrap();
        assert_eq!(wal_files(), 0, "reclaimed once both namespaces checkpoint");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn regression_failed_wal_append_rejects_write() {
        let dir = std::env::temp_dir().join("kosha-regression-wal-error");
        let _ = std::fs::remove_dir_all(&dir);
        let idx = Indexer::new(dir.clone());
        std::fs::remove_dir_all(dir.join("_wal")).unwrap();
        std::fs::write(dir.join("_wal"), b"blocked").unwrap();
        assert!(idx
            .index_documents(
                NamespaceId("a".into()),
                vec![Document {
                    id: DocumentId("a".into()),
                    fields: vec![Field::text("text", "a")],
                }]
            )
            .is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn delete_by_query_tombstones() {
        let dir = std::env::temp_dir().join("kosha-test-delete");
        let _ = std::fs::remove_dir_all(&dir);

        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone());
        idx.index_documents(
            ns.clone(),
            vec![
                Document {
                    id: DocumentId("d1".into()),
                    fields: vec![
                        Field::text("title", "hello world"),
                        Field::keyword("status", "active"),
                    ],
                },
                Document {
                    id: DocumentId("d2".into()),
                    fields: vec![
                        Field::text("title", "goodbye world"),
                        Field::keyword("status", "deleted"),
                    ],
                },
                Document {
                    id: DocumentId("d3".into()),
                    fields: vec![
                        Field::text("title", "hello again"),
                        Field::keyword("status", "active"),
                    ],
                },
            ],
        )
        .unwrap();
        idx.flush_namespace(&ns).unwrap();

        let manifest = idx.manifest(&ns).unwrap().clone();

        // Delete docs with status=deleted.
        let filter: FilterClause =
            serde_json::from_str(r#"{"term": {"status": "deleted"}}"#).unwrap();
        let count = idx.delete_by_query(&ns, &manifest, &filter).unwrap();
        assert_eq!(count, 1);

        // Check tombstone.
        let tombstones = idx.get_tombstones(&ns).unwrap();
        assert_eq!(tombstones.len(), 1);
        for seqs in tombstones.values() {
            // d2 has doc_seq=1
            assert!(seqs.contains(&1));
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn existing_ids_sees_buffer_and_flushed_docs() {
        let dir = std::env::temp_dir().join("kosha-test-exists");
        let _ = std::fs::remove_dir_all(&dir);

        let ns = NamespaceId("exists".into());
        let idx = Indexer::new(dir.clone()).with_wal(false);
        idx.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("flushed".into()),
                fields: vec![Field::text("title", "a")],
            }],
        )
        .unwrap();
        idx.flush_namespace(&ns).unwrap();
        idx.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("buffered".into()),
                fields: vec![Field::text("title", "b")],
            }],
        )
        .unwrap();

        let found = idx
            .existing_ids(
                &ns,
                &[
                    DocumentId("flushed".into()),
                    DocumentId("buffered".into()),
                    DocumentId("missing".into()),
                ],
            )
            .unwrap();
        assert!(found.contains(&DocumentId("flushed".into())));
        assert!(found.contains(&DocumentId("buffered".into())));
        assert!(!found.contains(&DocumentId("missing".into())));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn restore_manifest_recovers_namespace_without_segment_collisions() {
        let dir = std::env::temp_dir().join("kosha-test-restore");
        let _ = std::fs::remove_dir_all(&dir);

        // Index + flush, producing one persisted segment.
        let ns = NamespaceId("tenant/idx".into());
        let idx = Indexer::new(dir.clone());
        idx.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("d1".into()),
                fields: vec![Field::text("title", "hello world")],
            }],
        )
        .unwrap();
        idx.flush_namespace(&ns).unwrap();
        let persisted = idx.manifest(&ns).unwrap().clone();
        assert_eq!(persisted.segments.len(), 1);

        let saved_dir = dir.join(&ns.0).join(&persisted.segments[0].segment_id.0);
        let saved_files: Vec<_> = std::fs::read_dir(&saved_dir)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), std::fs::read(entry.path()).unwrap())
            })
            .collect();
        // Simulate a restart on ephemeral storage: local segment files are
        // gone, but the control store still holds the manifest.
        let _ = std::fs::remove_dir_all(&dir);
        let idx2 = Indexer::new(dir.clone());
        assert!(idx2.manifest(&ns).is_none());

        idx2.restore_manifest(ns.clone(), persisted);
        assert_eq!(idx2.manifest(&ns).unwrap().segments.len(), 1);

        assert!(
            idx2.existing_ids(&ns, &[DocumentId("d1".into())]).is_err(),
            "missing segments cannot silently lose IDs"
        );
        // The server hydrates referenced S3 files before accepting writes.
        std::fs::create_dir_all(&saved_dir).unwrap();
        for (name, bytes) in saved_files {
            std::fs::write(saved_dir.join(name), bytes).unwrap();
        }

        // The next flush must not reuse the restored segment ID.
        idx2.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("d2".into()),
                fields: vec![Field::text("title", "fresh doc")],
            }],
        )
        .unwrap();
        idx2.flush_namespace(&ns).unwrap();
        let m = idx2.manifest(&ns).unwrap();
        assert_eq!(m.segments.len(), 2);
        assert_ne!(m.segments[0].segment_id, m.segments[1].segment_id);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn index_documents_upserts_by_id_without_duplicates() {
        let dir = std::env::temp_dir().join("kosha-test-index-upsert");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone()).with_wal(false);
        idx.index_documents(
            ns.clone(),
            vec![
                Document {
                    id: DocumentId("d1".into()),
                    fields: vec![Field::text("title", "old"), Field::keyword("keep", "stale")],
                },
                Document {
                    id: DocumentId("d2".into()),
                    fields: vec![Field::text("title", "preserved")],
                },
            ],
        )
        .unwrap();
        idx.flush_namespace(&ns).unwrap();

        idx.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("d1".into()),
                fields: vec![Field::text("title", "new")],
            }],
        )
        .unwrap();

        let manifest = idx.manifest(&ns).unwrap().clone();
        assert_eq!(manifest.segments.len(), 2);
        assert_eq!(
            manifest.segments[1].doc_count, 1,
            "upsert writes only the replacement"
        );
        let records = live_records(&dir, &ns, &manifest);
        assert_eq!(records.len(), 2);
        let by_id: HashMap<_, _> = records
            .iter()
            .map(|record| (record.doc_id.0.as_str(), &record.fields))
            .collect();
        let d1: HashMap<_, _> = by_id["d1"]
            .iter()
            .map(|field| (field.name.as_str(), field.value.as_str()))
            .collect();
        // Full replace — old fields not present on the new doc are dropped.
        assert_eq!(d1.get("title"), Some(&"new"));
        assert!(!d1.contains_key("keep"));
        assert_eq!(by_id["d2"][0].value, "preserved");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn index_documents_dedupes_buffer_before_flush() {
        let dir = std::env::temp_dir().join("kosha-test-index-buffer-dedupe");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone()).with_wal(false);
        idx.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("d1".into()),
                fields: vec![Field::text("title", "first")],
            }],
        )
        .unwrap();
        idx.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("d1".into()),
                fields: vec![Field::text("title", "second")],
            }],
        )
        .unwrap();
        idx.flush_namespace(&ns).unwrap();

        let manifest = idx.manifest(&ns).unwrap().clone();
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].doc_count, 1);
        let reader = kosha_segment::SegmentReader::open(
            dir.join(&ns.0).join(&manifest.segments[0].segment_id.0),
        )
        .unwrap();
        let records: Vec<_> = reader
            .iter_doc_records()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].fields[0].value, "second");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Regression test for issue #176: `flush_namespace` used to hold
    /// `state` for the entire segment-write I/O duration, so
    /// `index_documents` (cheap bookkeeping — buffer append, WAL append,
    /// upsert checks) for the same namespace queued up behind whatever
    /// flush's disk I/O currently held the lock. Under bursty small-batch
    /// writes (confirmed live: the identical `/flush` call took anywhere
    /// from 0.7s to >20s back-to-back with no other change) this produced
    /// unbounded tail latency.
    ///
    /// `writer_slots` (formerly a single `flush_io: Mutex<()>`) is what
    /// carries that I/O cost instead of `state`. Assert directly, not by
    /// timing (flaky): holding *every* writer slot — simulating every
    /// concurrent segment write this namespace is allowed to have in
    /// flight at once — must not block a concurrent `index_documents` call
    /// for the same namespace.
    #[test]
    fn writer_slots_fully_occupied_does_not_block_concurrent_state_access() {
        let dir = std::env::temp_dir().join("kosha-test-flush-io-independent");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let idx =
            std::sync::Arc::new(Indexer::new(dir.clone()).with_max_concurrent_flush_writers(3));

        let handle = idx.ns_handle(&ns);
        let _permits: Vec<_> = (0..3).map(|_| handle.writer_slots.acquire()).collect();

        let (tx, rx) = std::sync::mpsc::channel();
        let idx2 = idx.clone();
        let ns2 = ns.clone();
        std::thread::spawn(move || {
            idx2.index_documents(
                ns2,
                vec![Document {
                    id: DocumentId("d1".into()),
                    fields: vec![Field::text("title", "hello")],
                }],
            )
            .unwrap();
            tx.send(()).unwrap();
        });

        // If index_documents were blocked on writer_slots (the pre-fix
        // behavior, when it was flush_io), this would hang until the test
        // harness's own timeout — 2s is generous slack for a call that
        // should complete in microseconds when the locks are actually
        // independent.
        rx.recv_timeout(std::time::Duration::from_secs(2)).expect(
            "index_documents blocked on writer_slots — state and writer_slots \
             must be independent locks, see issue #176",
        );

        drop(_permits);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Regression test for the follow-up to #176/#177: `flush_io` (now
    /// `writer_slots`) alone stops one flush's I/O from blocking `state`,
    /// but doesn't stop N concurrent `flush_namespace` calls from each
    /// paying their own full `SegmentWriter` I/O cost, serialized one after
    /// another —
    /// `kosha_client.bulk()` calls `/flush` after every single `bulk()`
    /// call, so this is exactly the pattern real bursty small-batch writes
    /// produce. Confirmed live even after #177: 14 flush timeouts in 2
    /// minutes under sustained concurrent writes to one namespace.
    ///
    /// Proves the singleflight coordinator actually coalesces: many threads
    /// each add one document and call `flush_namespace` concurrently
    /// (barrier-synchronized to maximize overlap) — asserts (a) every
    /// document is durably present afterward (coalescing must not lose
    /// data) and (b) the number of segments created is far smaller than the
    /// number of flush calls (coalescing must actually reduce I/O, not just
    /// avoid blocking `state`).
    #[test]
    fn concurrent_flush_calls_coalesce_into_fewer_segment_writes() {
        let dir = std::env::temp_dir().join("kosha-test-flush-coalescing");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let idx = std::sync::Arc::new(Indexer::new(dir.clone()));

        const N: usize = 40;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(N));
        let handles: Vec<_> = (0..N)
            .map(|i| {
                let idx = idx.clone();
                let ns = ns.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    idx.index_documents(
                        ns.clone(),
                        vec![Document {
                            id: DocumentId(format!("d{i}")),
                            fields: vec![Field::text("title", "hello")],
                        }],
                    )
                    .unwrap();
                    barrier.wait(); // maximize concurrent flush_namespace overlap
                    idx.flush_namespace(&ns).unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let manifest = idx.manifest(&ns).unwrap().clone();
        let total_docs: u32 = manifest.segments.iter().map(|s| s.doc_count).sum();
        assert_eq!(
            total_docs,
            N as u32,
            "coalescing must not lose documents — expected {N}, found {total_docs} \
             across {} segment(s)",
            manifest.segments.len()
        );
        assert!(
            manifest.segments.len() < N / 2,
            "expected coalescing to produce far fewer than {N} segments for {N} \
             concurrent flush calls, got {} — singleflight isn't actually \
             coalescing overlapping requests",
            manifest.segments.len()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A `flush_namespace` call that piggybacks on another thread's
    /// in-flight round (rather than becoming the leader itself) must still
    /// only return once its own data is durable — not the instant it
    /// observes `in_progress`. Exercises the actual piggyback branch
    /// directly (deterministic, not relying on scheduler timing) by holding
    /// `flush_coord` in the "in progress" state on the test thread while a
    /// second thread calls `flush_namespace`, then completing that round
    /// from the test thread and confirming the waiter unblocks with the
    /// data committed.
    #[test]
    fn piggybacked_flush_waits_for_the_leaders_round_to_actually_finish() {
        let dir = std::env::temp_dir().join("kosha-test-flush-piggyback");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let idx = std::sync::Arc::new(Indexer::new(dir.clone()));

        idx.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("d1".into()),
                fields: vec![Field::text("title", "hello")],
            }],
        )
        .unwrap();

        let handle = idx.ns_handle(&ns);
        {
            let mut coord = handle.flush_coord.lock().unwrap();
            coord.in_progress = true;
        }

        let (tx, rx) = std::sync::mpsc::channel();
        let idx2 = idx.clone();
        let ns2 = ns.clone();
        std::thread::spawn(move || {
            idx2.flush_namespace(&ns2).unwrap();
            tx.send(()).unwrap();
        });

        // The waiter should be blocked (piggybacking), not returned yet —
        // give it a moment to reach the wait, then confirm it hasn't
        // finished prematurely.
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(
            rx.try_recv().is_err(),
            "flush_namespace returned before the round it piggybacked on \
             actually completed"
        );

        // Now finish the "leader" round the test thread was simulating.
        {
            let mut coord = handle.flush_coord.lock().unwrap();
            coord.in_progress = false;
            coord.generation += 1;
        }
        handle.flush_done.notify_all();

        rx.recv_timeout(std::time::Duration::from_secs(2))
            .expect("piggybacked flush_namespace never returned after its round finished");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn replace_documents_rewrites_old_versions_durably() {
        let dir = std::env::temp_dir().join("kosha-test-replace");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone());
        idx.index_documents(
            ns.clone(),
            vec![
                Document {
                    id: DocumentId("d1".into()),
                    fields: vec![Field::text("title", "old")],
                },
                Document {
                    id: DocumentId("d2".into()),
                    fields: vec![Field::text("title", "preserved")],
                },
            ],
        )
        .unwrap();
        idx.flush_namespace(&ns).unwrap();
        let old_segment = idx.manifest(&ns).unwrap().segments[0].segment_id.clone();

        idx.replace_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("d1".into()),
                fields: vec![Field::text("title", "new")],
            }],
        )
        .unwrap();

        let manifest = idx.manifest(&ns).unwrap().clone();
        assert_eq!(manifest.segments.len(), 2);
        assert_eq!(
            manifest.segments[1].doc_count, 1,
            "upsert writes only the replacement"
        );
        assert_eq!(manifest.segments[0].segment_id, old_segment);
        assert!(
            dir.join(&ns.0).join(&old_segment.0).exists(),
            "older readers retain access until cache reclamation"
        );

        let records = live_records(&dir, &ns, &manifest);
        assert_eq!(records.len(), 2);
        let values: HashMap<_, _> = records
            .iter()
            .map(|record| (record.doc_id.0.as_str(), record.fields[0].value.as_str()))
            .collect();
        assert_eq!(values.get("d1"), Some(&"new"));
        assert_eq!(values.get("d2"), Some(&"preserved"));
        assert_eq!(idx.get_tombstones(&ns).unwrap()[&old_segment].len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn replace_documents_merges_partial_field_patches() {
        let dir = std::env::temp_dir().join("kosha-test-replace-merge");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone());
        idx.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("d1".into()),
                fields: vec![
                    Field::text("title", "hello"),
                    Field::keyword("sentAt", "old"),
                ],
            }],
        )
        .unwrap();
        idx.flush_namespace(&ns).unwrap();

        idx.replace_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("d1".into()),
                fields: vec![Field::keyword("sentAt", "new")],
            }],
        )
        .unwrap();

        let manifest = idx.manifest(&ns).unwrap().clone();
        let records = live_records(&dir, &ns, &manifest);
        assert_eq!(records.len(), 1);
        let fields: HashMap<_, _> = records[0]
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field.value.as_str()))
            .collect();
        assert_eq!(fields.get("title"), Some(&"hello"));
        assert_eq!(fields.get("sentAt"), Some(&"new"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_merges_segments() {
        let dir = std::env::temp_dir().join("kosha-test-compact");
        let _ = std::fs::remove_dir_all(&dir);

        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone()).with_flush_threshold(2);

        // Index 4 docs in batches of 2 → creates 2 segments.
        for i in 0..4 {
            idx.index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId(format!("d{}", i + 1)),
                    fields: vec![Field::text("title", format!("doc number {}", i + 1))],
                }],
            )
            .unwrap();
        }

        // 2 flushes → 2 segments.
        assert_eq!(idx.manifest(&ns).unwrap().segments.len(), 2);

        // Compact (full) → should merge into 1 segment.
        let result = idx
            .compact_namespace_with_options(&ns, CompactOptions::full())
            .unwrap();
        assert!(result.merged);
        assert_eq!(idx.manifest(&ns).unwrap().segments.len(), 1);

        // Verify all 4 docs are in the merged segment.
        let seg_id = idx.manifest(&ns).unwrap().segments[0].segment_id.clone();
        let seg_dir = dir.join("test").join(&seg_id.0);
        assert!(seg_dir.exists());
        let reader = kosha_segment::SegmentReader::open(seg_dir).unwrap();
        assert_eq!(reader.doc_count(), 4);

        // Old segment directories should be deleted.
        assert_eq!(idx.manifest(&ns).unwrap().segments.len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// #242: `compact_planned` merges exactly the planned group, even when
    /// other segments are also on local disk (re-planning from local disk
    /// would grab them too), skips a planned input that is not local, and
    /// reports the consumed inputs and the output for eviction.
    #[test]
    fn compact_planned_merges_only_the_planned_group() {
        let dir =
            std::env::temp_dir().join(format!("kosha-test-compact-planned-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone()).with_flush_threshold(1);
        for i in 0..5 {
            idx.index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId(format!("d{i}")),
                    fields: vec![Field::text("title", format!("doc number {i}"))],
                }],
            )
            .unwrap();
        }
        let before = idx.manifest_cloned(&ns).unwrap();
        assert_eq!(before.segments.len(), 5);
        let ids: Vec<SegmentId> = before
            .segments
            .iter()
            .map(|e| e.segment_id.clone())
            .collect();
        // Plan s0, s1, s2; s2 is "not hydrated" (its dir is gone).
        std::fs::remove_dir_all(dir.join("test").join(&ids[2].0)).unwrap();
        let plan = MergePlan {
            inputs: before.segments[..3].to_vec(),
        };
        let result = idx.compact_planned(&ns, &plan).unwrap();
        assert!(result.merged);
        assert_eq!(result.merged_inputs, vec![ids[0].clone(), ids[1].clone()]);
        let output = result.output.clone().expect("output id");
        let after = idx.manifest_cloned(&ns).unwrap();
        let after_ids: Vec<&SegmentId> = after.segments.iter().map(|e| &e.segment_id).collect();
        assert_eq!(after.segments.len(), 4);
        for kept in [&ids[2], &ids[3], &ids[4], &output] {
            assert!(after_ids.contains(&kept), "{kept:?} missing: {after_ids:?}");
        }

        // A plan with one usable input is a no-op.
        let lone = MergePlan {
            inputs: vec![after
                .segments
                .iter()
                .find(|e| e.segment_id == ids[3])
                .unwrap()
                .clone()],
        };
        let noop = idx.compact_planned(&ns, &lone).unwrap();
        assert!(!noop.merged);
        assert!(noop.merged_inputs.is_empty());
        assert_eq!(idx.manifest_cloned(&ns).unwrap().segments.len(), 4);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_keeps_nested_attribute_columns() {
        // #206: dotted-path columns are derived from the stored container
        // at every segment build, so a merged segment has them too and its
        // stored document is still the original JSON.
        let dir =
            std::env::temp_dir().join(format!("kosha-test-compact-nested-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone()).with_flush_threshold(1);
        let authors = [
            r#"[{"name":"Ada","n":1},{"name":"Bob","n":2}]"#,
            r#"{"name":"Cy","n":3}"#,
        ];
        for (i, value) in authors.iter().enumerate() {
            idx.index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId(format!("d{i}")),
                    fields: vec![Field::keyword("authors", *value)],
                }],
            )
            .unwrap();
        }
        assert_eq!(idx.manifest(&ns).unwrap().segments.len(), 2);
        assert!(
            idx.compact_namespace_with_options(&ns, CompactOptions::full())
                .unwrap()
                .merged
        );
        let seg_id = idx.manifest(&ns).unwrap().segments[0].segment_id.clone();
        let reader = kosha_segment::SegmentReader::open(dir.join("test").join(&seg_id.0)).unwrap();
        let store = reader.filter_store();
        // Merge order is not index order: key everything by document id.
        let records: Vec<_> = reader.iter_doc_records().map(|r| r.unwrap()).collect();
        let id_of = |seq: u32| records[seq as usize].doc_id.0.clone();
        let mut names: Vec<(String, String)> = store.string_fields["authors.name"]
            .iter()
            .map(|(seq, v)| (id_of(*seq), v.clone()))
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                ("d0".to_string(), r#"["Ada","Bob"]"#.to_string()),
                ("d1".to_string(), "Cy".to_string())
            ]
        );
        let mut numbers: Vec<(String, f64)> = store.float_fields["authors.n"]
            .iter()
            .map(|(seq, v)| (id_of(*seq), *v))
            .collect();
        numbers.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            numbers,
            vec![("d0".into(), 1.0), ("d0".into(), 2.0), ("d1".into(), 3.0)]
        );
        let mut stored: Vec<(String, String)> = records
            .iter()
            .map(|r| (r.doc_id.0.clone(), r.fields[0].value.clone()))
            .collect();
        stored.sort();
        assert_eq!(
            stored,
            vec![
                ("d0".to_string(), authors[0].to_string()),
                ("d1".to_string(), authors[1].to_string())
            ]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Regression for the production incident this fixes: tiered compaction
    /// silently lost ~0.24% of a 10M-doc corpus across several rounds, with
    /// every round reporting success — traced to `iter_doc_records`
    /// silently dropping any doc whose read failed instead of surfacing an
    /// error (see that method's doc comment). A doc read failure mid-merge
    /// must now abort the whole compaction attempt instead of quietly
    /// publishing a smaller merged segment.
    #[test]
    fn compact_aborts_instead_of_silently_losing_docs_on_read_failure() {
        let dir = std::env::temp_dir().join("kosha-test-compact-read-failure");
        let _ = std::fs::remove_dir_all(&dir);

        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone()).with_flush_threshold(2);

        for i in 0..4 {
            idx.index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId(format!("d{}", i + 1)),
                    fields: vec![Field::text("title", format!("doc number {}", i + 1))],
                }],
            )
            .unwrap();
        }
        assert_eq!(idx.manifest(&ns).unwrap().segments.len(), 2);

        // Corrupt one input segment's doc_store.bin (truncate out the last
        // record's bytes, same technique as kosha-segment's
        // iter_doc_records_surfaces_read_failures_instead_of_dropping_them)
        // so a read fails partway through the merge.
        let seg_id = idx.manifest(&ns).unwrap().segments[0].segment_id.clone();
        let doc_store_path = dir.join("test").join(&seg_id.0).join("doc_store.bin");
        let full = std::fs::read(&doc_store_path).unwrap();
        std::fs::write(&doc_store_path, &full[..full.len() - 5]).unwrap();

        let result = idx.compact_namespace_with_options(&ns, CompactOptions::full());
        assert!(
            result.is_err(),
            "a doc read failure mid-merge must abort compaction loudly, not publish a smaller \
             merged segment: {result:?}"
        );

        // Nothing published: the namespace's segment list is untouched.
        assert_eq!(idx.manifest(&ns).unwrap().segments.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Regression for a race distinct from the read-failure one above: a
    /// `delete_by_query` landing *after* compaction snapshots tombstones but
    /// *before* it publishes (the window where `state` is unlocked for merge
    /// I/O — see `compact_namespace_with_options_impl`'s doc comment). The
    /// merge loop only ever consults the stale snapshot, so it copies the
    /// doc the concurrent delete just tombstoned; unless that tombstone is
    /// carried forward onto the merged segment at publish, the delete is
    /// silently discarded and the doc comes back to life the moment the
    /// merged segment replaces its inputs. `after_plan` (the test-only hook)
    /// lands the delete deterministically instead of racing real threads
    /// against merge I/O timing that's too fast in a test fixture to hit
    /// reliably.
    #[test]
    fn compact_carries_forward_a_delete_that_lands_mid_merge() {
        let dir = std::env::temp_dir().join("kosha-test-compact-tombstone-race");
        let _ = std::fs::remove_dir_all(&dir);

        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone()).with_flush_threshold(2);

        idx.index_documents(
            ns.clone(),
            vec![
                Document {
                    id: DocumentId("d1".into()),
                    fields: vec![
                        Field::text("title", "hello world"),
                        Field::keyword("status", "active"),
                    ],
                },
                Document {
                    id: DocumentId("d2".into()),
                    fields: vec![
                        Field::text("title", "goodbye world"),
                        Field::keyword("status", "active"),
                    ],
                },
            ],
        )
        .unwrap();
        idx.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("d3".into()),
                fields: vec![
                    Field::text("title", "hello again"),
                    Field::keyword("status", "active"),
                ],
            }],
        )
        .unwrap();
        // A lone 3rd doc doesn't reach `with_flush_threshold(2)` on its own —
        // flush it explicitly so it lands in its own (2nd) segment instead
        // of staying buffered.
        idx.flush_namespace(&ns).unwrap();
        assert_eq!(idx.manifest(&ns).unwrap().segments.len(), 2);

        let manifest_before = idx.manifest(&ns).unwrap();
        let filter: FilterClause =
            serde_json::from_str(r#"{"term": {"status": "active"}}"#).unwrap();

        // The hook fires once merge planning has snapshotted tombstones and
        // released `state`, exactly reproducing a delete that lands in that
        // window in production — deleting *all three* docs so the merge
        // output (had the race not been fixed) would end up with none of
        // its docs live.
        let ns_hook = ns.clone();
        let deleted = std::cell::Cell::new(0usize);
        let result = idx
            .compact_namespace_with_options_impl(&ns, CompactOptions::full(), || {
                let count = idx
                    .delete_by_query(&ns_hook, &manifest_before, &filter)
                    .unwrap();
                deleted.set(count);
            })
            .unwrap();
        assert_eq!(deleted.get(), 3, "the mid-merge delete itself must land");
        assert!(result.merged);

        // The merge output still physically contains all 3 docs (copied
        // under the stale pre-delete snapshot) — that's expected and fine,
        // the same way an already-known-tombstoned doc stays on disk in any
        // segment. What must NOT happen is the delete being lost: every doc
        // must still read back as deleted through the tombstone-aware path.
        let seg_id = idx.manifest(&ns).unwrap().segments[0].segment_id.clone();
        let reader = kosha_segment::SegmentReader::open(dir.join("test").join(&seg_id.0)).unwrap();
        assert_eq!(
            reader.doc_count(),
            3,
            "merge output should still contain all 3 docs pre-tombstone"
        );

        let tombstones = idx.get_tombstones(&ns).unwrap();
        let live_tombstones: HashSet<u32> = tombstones.get(&seg_id).cloned().unwrap_or_default();
        assert_eq!(
            live_tombstones.len(),
            3,
            "a delete landing mid-merge must be carried forward onto the merged segment, \
             not silently discarded: {live_tombstones:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_one_namespace_does_not_block_writes_to_another() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;
        use std::time::{Duration, Instant};

        let dir = std::env::temp_dir().join("kosha-test-compact-isolation");
        let _ = std::fs::remove_dir_all(&dir);

        let idx = std::sync::Arc::new(Indexer::new(dir.clone()).with_flush_threshold(2));
        let ns_a = NamespaceId("ns-a".into());
        let ns_b = NamespaceId("ns-b".into());

        // Give A enough tiny segments that a full compact does real I/O.
        for i in 0..40 {
            idx.index_documents(
                ns_a.clone(),
                vec![Document {
                    id: DocumentId(format!("a-{i}")),
                    fields: vec![Field::text("title", format!("doc a {i}"))],
                }],
            )
            .unwrap();
        }
        assert!(idx.manifest(&ns_a).unwrap().segments.len() >= 10);

        let started_compact = std::sync::Arc::new(AtomicBool::new(false));
        let finished_compact = std::sync::Arc::new(AtomicBool::new(false));
        let idx_c = idx.clone();
        let ns_a_c = ns_a.clone();
        let started_c = started_compact.clone();
        let finished_c = finished_compact.clone();
        let compact_thread = thread::spawn(move || {
            started_c.store(true, Ordering::SeqCst);
            idx_c
                .compact_namespace_with_options(&ns_a_c, CompactOptions::full())
                .unwrap();
            finished_c.store(true, Ordering::SeqCst);
        });

        while !started_compact.load(Ordering::SeqCst) {
            thread::yield_now();
        }
        // While A is compacting, a write to B must complete promptly.
        let t0 = Instant::now();
        idx.index_documents(
            ns_b.clone(),
            vec![Document {
                id: DocumentId("b-1".into()),
                fields: vec![Field::text("title", "other namespace")],
            }],
        )
        .unwrap();
        let elapsed = t0.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "write to ns-b blocked for {elapsed:?} while ns-a compacted"
        );
        assert!(
            !finished_compact.load(Ordering::SeqCst) || elapsed < Duration::from_millis(500),
            "isolation check raced past compact completion"
        );

        compact_thread.join().unwrap();
        assert_eq!(idx.manifest(&ns_a).unwrap().segments.len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_tiered_leaves_large_segments_untouched() {
        let dir = std::env::temp_dir().join("kosha-test-compact-tiered");
        let _ = std::fs::remove_dir_all(&dir);

        let ns = NamespaceId("test".into());
        let policy = CompactionPolicy {
            max_mergeable_docs: 3,
            max_segments_per_merge: 32,
            min_mergeable_segments: 2,
            trigger_mergeable_segments: 2,
            ..CompactionPolicy::default()
        };
        let idx = Indexer::new(dir.clone())
            .with_flush_threshold(2)
            .with_compaction_policy(policy.clone());

        // Two tiny segments (2 docs each) + one larger segment (4 docs).
        for i in 0..4 {
            idx.index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId(format!("small-{i}")),
                    fields: vec![Field::text("title", format!("small {i}"))],
                }],
            )
            .unwrap();
        }
        assert_eq!(idx.manifest(&ns).unwrap().segments.len(), 2);

        // Force a third segment above the mergeable doc threshold.
        idx.index_documents(
            ns.clone(),
            vec![
                Document {
                    id: DocumentId("big-1".into()),
                    fields: vec![Field::text("title", "big one")],
                },
                Document {
                    id: DocumentId("big-2".into()),
                    fields: vec![Field::text("title", "big two")],
                },
                Document {
                    id: DocumentId("big-3".into()),
                    fields: vec![Field::text("title", "big three")],
                },
                Document {
                    id: DocumentId("big-4".into()),
                    fields: vec![Field::text("title", "big four")],
                },
            ],
        )
        .unwrap();
        idx.flush_namespace(&ns).unwrap();
        let before = idx.manifest(&ns).unwrap();
        assert_eq!(before.segments.len(), 3);
        assert!(before.segments.iter().any(|e| e.doc_count >= 3));

        let result = idx
            .compact_namespace_with_options(&ns, CompactOptions::tiered(policy))
            .unwrap();
        assert!(result.merged);
        let after = idx.manifest(&ns).unwrap();
        // Two small segments merge into one; the large segment remains.
        assert_eq!(after.segments.len(), 2);
        assert!(after.segments.iter().any(|e| e.doc_count >= 3));
        assert!(
            !idx.needs_compaction(&ns)
                || after.segments.iter().filter(|e| e.doc_count < 3).count() < 2
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Proves genuine multi-writer parallelism, not just coalescing: a
    /// single `flush_namespace` call on a namespace with a large enough
    /// buffered backlog produces MULTIPLE segments from that ONE call, each
    /// written by its own concurrent `SegmentWriter`. This is the opposite
    /// direction from `concurrent_flush_calls_coalesce_into_fewer_segment_writes`
    /// (many calls -> fewer segments); together they cover both halves of
    /// the design: light bursts stay cheap (one writer, well-coalesced),
    /// sustained backlogs actually use the configured writer pool instead
    /// of draining it one segment at a time — see issue #176's follow-up
    /// discussion for the live incident (100% `/flush` timeout under
    /// sustained concurrent ingest) neither #177 nor #178 alone fixed.
    #[test]
    fn large_backlog_flush_uses_multiple_concurrent_segment_writers() {
        let dir = std::env::temp_dir().join("kosha-test-parallel-writer-backlog");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone())
            .with_flush_threshold(10_000) // avoid auto-flush interfering
            .with_max_concurrent_flush_writers(4)
            .with_min_docs_per_flush_writer(10);

        let docs: Vec<Document> = (0..100)
            .map(|i| Document {
                id: DocumentId(format!("d{i}")),
                fields: vec![Field::text("title", format!("doc {i}"))],
            })
            .collect();
        idx.index_documents(ns.clone(), docs).unwrap();

        idx.flush_namespace(&ns).unwrap();

        let manifest = idx.manifest(&ns).unwrap().clone();
        assert_eq!(
            manifest.segments.len(),
            4,
            "100 docs / 10 min-docs-per-writer, capped at 4 writers, should \
             produce exactly 4 concurrently-written segments from one flush \
             call, got {}",
            manifest.segments.len()
        );
        let total: u32 = manifest.segments.iter().map(|e| e.doc_count).sum();
        assert_eq!(
            total, 100,
            "no documents lost across concurrent chunk writers"
        );
        let seg_ids: HashSet<&SegmentId> =
            manifest.segments.iter().map(|e| &e.segment_id).collect();
        assert_eq!(
            seg_ids.len(),
            4,
            "segment IDs must stay collision-free across concurrent writers"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Regression/design test for the WAL-safety tradeoff documented on
    /// `run_one_flush_pass`: a chunk that fails inside a multi-writer flush
    /// round must not lose its documents, and must not have its WAL backing
    /// wiped without a fresh replacement. Forces the 2nd of 3 concurrent
    /// chunks to fail deterministically by pre-occupying the exact path its
    /// `SegmentWriter` needs to create as a directory with a plain file, so
    /// `create_dir_all` inside `finalize()` errors for that chunk only.
    #[test]
    fn partial_writer_failure_preserves_documents_and_wal_safety() {
        let dir = std::env::temp_dir().join("kosha-test-parallel-writer-partial-failure");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone())
            .with_flush_threshold(10_000)
            .with_max_concurrent_flush_writers(3)
            .with_min_docs_per_flush_writer(2);

        let docs: Vec<Document> = (0..6)
            .map(|i| Document {
                id: DocumentId(format!("d{i}")),
                fields: vec![Field::text("title", format!("doc {i}"))],
            })
            .collect();
        idx.index_documents(ns.clone(), docs).unwrap();

        // 6 docs / 2 min-per-writer, capped at 3 writers -> 3 chunks of 2
        // docs, segment IDs "test-000000".."test-000002" (fresh namespace,
        // counter starts at 0 -- the ns dir doesn't exist yet when
        // index_documents above initializes the buffer). Block the middle
        // chunk by occupying its segment directory path with a plain file
        // before it ever calls finalize().
        let ns_dir = dir.join(&ns.0);
        std::fs::create_dir_all(&ns_dir).unwrap();
        let prefix = idx
            .ns_handle(&ns)
            .state
            .lock()
            .unwrap()
            .buffer
            .as_ref()
            .unwrap()
            .segment_prefix
            .clone();
        let blocked_seg_dir = ns_dir.join(format!("{prefix}-000001"));
        std::fs::write(&blocked_seg_dir, b"not a directory").unwrap();

        let result = idx.flush_namespace(&ns);
        assert!(
            result.is_err(),
            "a chunk whose segment dir collides with an existing file must fail the round"
        );

        assert!(
            idx.manifest(&ns).unwrap().segments.is_empty(),
            "a failed round must not expose partial segments"
        );
        let all_ids: Vec<DocumentId> = (0..6).map(|i| DocumentId(format!("d{i}"))).collect();
        assert_eq!(idx.existing_ids(&ns, &all_ids).unwrap().len(), 6);
        let records = crate::wal::WalWriter::recover(&dir.join("_wal")).unwrap();
        assert_eq!(
            records.iter().map(|r| r.documents.len()).sum::<usize>(),
            6,
            "all records remain recoverable until publication"
        );

        // Clear the blocker and retry: the restored documents must flush
        // successfully and the namespace end up with all 6 docs.
        std::fs::remove_file(&blocked_seg_dir).unwrap();
        idx.flush_namespace(&ns).unwrap();
        let manifest = idx.manifest(&ns).unwrap().clone();
        let total: u32 = manifest.segments.iter().map(|e| e.doc_count).sum();
        assert_eq!(total, 6, "all 6 documents durable after retry");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The core correctness proof for this change: many threads sustain
    /// concurrent index+flush against ONE namespace over several
    /// overlapping rounds — the actual shape of the production incident
    /// this fixes (`decover-tech/backend`'s shadow-write mechanism mirrors
    /// every real write from multiple concurrent workers onto Kosha,
    /// sustained over minutes, not one short burst). Every document indexed
    /// must be durably present exactly once afterward: no data loss, no
    /// duplication, no segment ID collision under real concurrent
    /// parallel-writer execution — not just coalescing, which
    /// `concurrent_flush_calls_coalesce_into_fewer_segment_writes` already
    /// covers.
    #[test]
    fn concurrent_writes_to_one_namespace_no_data_loss_under_sustained_load() {
        let dir = std::env::temp_dir().join("kosha-test-parallel-writer-stress");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let idx = std::sync::Arc::new(
            Indexer::new(dir.clone())
                .with_max_concurrent_flush_writers(3)
                .with_min_docs_per_flush_writer(4),
        );

        const THREADS: usize = 8;
        const ROUNDS: usize = 15;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let idx = idx.clone();
                let ns = ns.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    for round in 0..ROUNDS {
                        idx.index_documents(
                            ns.clone(),
                            vec![Document {
                                id: DocumentId(format!("t{t}-r{round}")),
                                fields: vec![Field::text(
                                    "title",
                                    format!("thread {t} round {round}"),
                                )],
                            }],
                        )
                        .unwrap();
                        // maximize concurrent flush_namespace overlap each round
                        barrier.wait();
                        idx.flush_namespace(&ns).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let manifest = idx.manifest(&ns).unwrap().clone();

        // No segment ID collisions.
        let seg_ids: HashSet<&SegmentId> =
            manifest.segments.iter().map(|e| &e.segment_id).collect();
        assert_eq!(
            seg_ids.len(),
            manifest.segments.len(),
            "duplicate segment IDs in manifest — concurrent writers collided \
             on segment_counter"
        );

        // No data loss / no duplication: read back every doc_id from every
        // segment and compare against the exact expected set.
        let mut found_ids: HashSet<String> = HashSet::new();
        for entry in &manifest.segments {
            let reader =
                kosha_segment::SegmentReader::open(dir.join(&ns.0).join(&entry.segment_id.0))
                    .unwrap();
            for record in reader.iter_doc_records() {
                let record = record.unwrap();
                assert!(
                    found_ids.insert(record.doc_id.0.clone()),
                    "doc {} appears in more than one segment — duplicate write",
                    record.doc_id.0
                );
            }
        }
        let expected: HashSet<String> = (0..THREADS)
            .flat_map(|t| (0..ROUNDS).map(move |r| format!("t{t}-r{r}")))
            .collect();
        assert_eq!(
            found_ids, expected,
            "every indexed document must be durably present exactly once"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Manifest rebase (#230) ───────────────────────────────────────────

    fn rebase_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "kosha-rebase-{name}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn write_docs(idx: &Indexer, ns: &NamespaceId, ids: &[&str]) {
        idx.index_documents(
            ns.clone(),
            ids.iter()
                .map(|id| Document {
                    id: DocumentId((*id).into()),
                    fields: vec![Field::keyword("id", *id)],
                })
                .collect(),
        )
        .unwrap();
        idx.flush_namespace(ns).unwrap();
    }

    /// Commit the current snapshot as if the control-store CAS succeeded,
    /// and return the committed manifest with its deletes hydrated (what a
    /// competing writer would read back from the control store).
    fn commit(idx: &Indexer, ns: &NamespaceId) -> Manifest {
        let (_, hydrated) = idx.publication_snapshot(ns).unwrap();
        let (_, prepared) = idx.prepare_publication(ns).unwrap().unwrap();
        idx.acknowledge_manifest(ns, &prepared).unwrap();
        hydrated
    }

    fn delete_id(idx: &Indexer, ns: &NamespaceId, id: &str) {
        let filter: FilterClause =
            serde_json::from_value(serde_json::json!({"term": {"id": id}})).unwrap();
        let manifest = idx.manifest(ns).unwrap();
        assert_eq!(idx.delete_by_query(ns, &manifest, &filter).unwrap(), 1);
    }

    fn segment_ids(manifest: &Manifest) -> HashSet<SegmentId> {
        manifest
            .segments
            .iter()
            .map(|entry| entry.segment_id.clone())
            .collect()
    }

    /// A segment another writer published: only its manifest entry matters
    /// to a rebase, which never opens segments.
    fn foreign(remote: &mut Manifest, id: &str, doc_count: u32) -> SegmentId {
        let segment = SegmentId(id.into());
        remote.segments.push(ManifestEntry {
            segment_id: segment.clone(),
            doc_count,
        });
        segment
    }

    #[test]
    fn rebase_replays_unpublished_segments_and_deletes_onto_a_newer_remote() {
        let dir = rebase_dir("replay");
        let ns = NamespaceId("ns".into());
        let idx = Indexer::new(dir.clone());
        write_docs(&idx, &ns, &["a", "b"]);
        let base = commit(&idx, &ns);
        let s1 = base.segments[0].segment_id.clone();

        // Unpublished local work: a new segment and a delete in s1.
        write_docs(&idx, &ns, &["c"]);
        delete_id(&idx, &ns, "a");
        let local = idx.publication_snapshot(&ns).unwrap().1;
        let a_seq = *local.tombstones[&s1].iter().next().unwrap();

        // Meanwhile another writer published a segment and deleted "b".
        let mut remote = base.clone();
        remote.version = base.version + 1;
        let other = foreign(&mut remote, "other-writer-000001", 3);
        remote
            .tombstones
            .insert(s1.clone(), HashSet::from([1 - a_seq]));
        remote.tombstones.insert(other.clone(), HashSet::from([2]));
        remote.wal_origin = Some("other-writer".into());
        remote.wal_checkpoint = 77;

        assert_eq!(
            idx.rebase_onto(&ns, remote.clone()).unwrap(),
            RebaseOutcome::Rebased
        );
        let (expected, rebased) = idx.publication_snapshot(&ns).unwrap();
        assert_eq!(
            expected, remote.version,
            "CAS must target the remote version"
        );
        assert!(rebased.version > remote.version);
        assert_eq!(
            segment_ids(&rebased),
            segment_ids(&local)
                .union(&HashSet::from([other.clone()]))
                .cloned()
                .collect()
        );
        assert_eq!(rebased.tombstones[&s1], HashSet::from([0, 1]));
        assert_eq!(rebased.tombstones[&other], HashSet::from([2]));
        assert_eq!(
            rebased.wal_origin, local.wal_origin,
            "checkpoint stays this writer's"
        );
        assert_eq!(rebased.wal_checkpoint, local.wal_checkpoint);

        // Publishing the rebase makes it the base; nothing left to rebase.
        commit(&idx, &ns);
        let mut newer = idx.publication_snapshot(&ns).unwrap().1;
        newer.version += 1;
        assert_eq!(
            idx.rebase_onto(&ns, newer.clone()).unwrap(),
            RebaseOutcome::Adopted
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rebase_without_local_changes_adopts_the_remote_manifest() {
        let dir = rebase_dir("adopt");
        let ns = NamespaceId("ns".into());
        let idx = Indexer::new(dir.clone());
        write_docs(&idx, &ns, &["a"]);
        let mut remote = commit(&idx, &ns);
        remote.version += 3;
        foreign(&mut remote, "other-000001", 1);
        assert_eq!(
            idx.rebase_onto(&ns, remote.clone()).unwrap(),
            RebaseOutcome::Adopted
        );
        let (expected, adopted) = idx.publication_snapshot(&ns).unwrap();
        assert_eq!(
            (expected, adopted.version),
            (remote.version, remote.version)
        );
        assert_eq!(segment_ids(&adopted), segment_ids(&remote));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rebase_keeps_a_local_schema_change_and_otherwise_the_remote_schema() {
        let schema = |analyzer: &str| kosha_core::NamespaceSchema {
            default_analyzer: vec![analyzer.to_owned()],
            ..Default::default()
        };
        let dir = rebase_dir("schema");
        let ns = NamespaceId("ns".into());
        let idx = Indexer::new(dir.clone());
        write_docs(&idx, &ns, &["a"]);
        let base = commit(&idx, &ns);

        // Unpublished local data, while another writer set a schema: the
        // rebase must not drop the remote's schema.
        write_docs(&idx, &ns, &["b"]);
        let mut remote = base.clone();
        remote.version = base.version + 1;
        remote.schema = Some(schema("standard@2"));
        remote.wal_origin = Some("other-writer".into());
        assert_eq!(
            idx.rebase_onto(&ns, remote.clone()).unwrap(),
            RebaseOutcome::Rebased
        );
        let rebased = idx.publication_snapshot(&ns).unwrap().1;
        assert_eq!(rebased.schema, Some(schema("standard@2")));
        let published = commit(&idx, &ns);

        // A schema set locally since the last publication wins over the
        // remote's (it is the newer admin action).
        assert!(idx.set_schema(&ns, schema("whitespace")).unwrap());
        let mut remote = published.clone();
        remote.version = published.version + 1;
        remote.schema = Some(schema("keyword"));
        remote.wal_origin = Some("other-writer".into());
        assert_eq!(
            idx.rebase_onto(&ns, remote).unwrap(),
            RebaseOutcome::Rebased
        );
        let rebased = idx.publication_snapshot(&ns).unwrap().1;
        assert_eq!(rebased.schema, Some(schema("whitespace")));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rebase_aborts_when_a_remote_compaction_removed_a_locally_deleted_segment() {
        let dir = rebase_dir("remote-compaction");
        let ns = NamespaceId("ns".into());
        let idx = Indexer::new(dir.clone());
        write_docs(&idx, &ns, &["a", "b"]);
        let base = commit(&idx, &ns);
        delete_id(&idx, &ns, "a");
        let before = idx.publication_snapshot(&ns).unwrap();

        // The other writer compacted s1 away before seeing the delete.
        let mut remote = base.clone();
        remote.version += 1;
        remote.segments.clear();
        foreign(&mut remote, "ns-compact-remote", 2);

        let err = idx.rebase_onto(&ns, remote).unwrap_err();
        assert!(matches!(err, KoshaError::ManifestConflict { .. }), "{err}");
        let after = idx.publication_snapshot(&ns).unwrap();
        assert_eq!(after.0, before.0, "abort must leave the CAS anchor alone");
        assert!(
            after.1.same_contents(&before.1),
            "abort must not touch local state"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rebase_drops_a_segment_a_remote_compaction_subsumed_when_no_local_delete_targets_it() {
        let dir = rebase_dir("subsumed");
        let ns = NamespaceId("ns".into());
        let idx = Indexer::new(dir.clone());
        write_docs(&idx, &ns, &["a"]);
        let base = commit(&idx, &ns);
        write_docs(&idx, &ns, &["b"]);
        let mut remote = base.clone();
        remote.version += 1;
        remote.segments.clear();
        let merged = foreign(&mut remote, "ns-compact-remote", 1);
        assert_eq!(
            idx.rebase_onto(&ns, remote).unwrap(),
            RebaseOutcome::Rebased
        );
        let rebased = idx.publication_snapshot(&ns).unwrap().1;
        assert!(!segment_ids(&rebased).contains(&base.segments[0].segment_id));
        assert!(segment_ids(&rebased).contains(&merged));
        assert_eq!(rebased.segments.len(), 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rebase_keeps_a_local_compaction_only_while_its_inputs_are_untouched_remotely() {
        let dir = rebase_dir("local-compaction");
        let ns = NamespaceId("ns".into());
        let idx = Indexer::new(dir.clone());
        write_docs(&idx, &ns, &["a"]);
        write_docs(&idx, &ns, &["b"]);
        let base = commit(&idx, &ns);
        assert!(
            idx.compact_namespace_with_options(&ns, CompactOptions::full())
                .unwrap()
                .merged
        );
        let local = idx.publication_snapshot(&ns).unwrap();

        // Remote compacted the same inputs: publishing both would double
        // every document.
        let mut conflicting = base.clone();
        conflicting.version += 1;
        conflicting.segments.clear();
        foreign(&mut conflicting, "ns-compact-remote", 2);
        assert!(matches!(
            idx.rebase_onto(&ns, conflicting).unwrap_err(),
            KoshaError::ManifestConflict { .. }
        ));
        // Remote deleted inside an input: the local output would resurrect it.
        let mut deleted = base.clone();
        deleted.version += 1;
        deleted
            .tombstones
            .insert(base.segments[0].segment_id.clone(), HashSet::from([0]));
        assert!(matches!(
            idx.rebase_onto(&ns, deleted).unwrap_err(),
            KoshaError::ManifestConflict { .. }
        ));
        assert!(idx
            .publication_snapshot(&ns)
            .unwrap()
            .1
            .same_contents(&local.1));

        // Remote only added a segment: the compaction rebases cleanly.
        let mut appended = base.clone();
        appended.version += 1;
        let other = foreign(&mut appended, "other-000001", 1);
        assert_eq!(
            idx.rebase_onto(&ns, appended).unwrap(),
            RebaseOutcome::Rebased
        );
        let rebased = idx.publication_snapshot(&ns).unwrap().1;
        assert_eq!(
            segment_ids(&rebased),
            segment_ids(&local.1)
                .union(&HashSet::from([other]))
                .cloned()
                .collect()
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rebase_recognizes_its_own_commit_whose_reply_was_lost() {
        let dir = rebase_dir("lost-reply");
        let ns = NamespaceId("ns".into());
        let idx = Indexer::new(dir.clone());
        write_docs(&idx, &ns, &["a"]);
        commit(&idx, &ns);
        write_docs(&idx, &ns, &["b"]);
        write_docs(&idx, &ns, &["c"]);
        // The CAS of this compaction landed but its reply was lost.
        idx.compact_namespace_with_options(&ns, CompactOptions::full())
            .unwrap();
        let (_, landed) = idx.publication_snapshot(&ns).unwrap();
        idx.prepare_publication(&ns).unwrap();
        write_docs(&idx, &ns, &["d"]);
        assert_eq!(
            idx.rebase_onto(&ns, landed.clone()).unwrap(),
            RebaseOutcome::Rebased
        );
        let (expected, rebased) = idx.publication_snapshot(&ns).unwrap();
        assert_eq!(expected, landed.version);
        assert_eq!(rebased.segments.len(), landed.segments.len() + 1);
        let _ = std::fs::remove_dir_all(dir);
    }

    // ── Segment GC (#239) ───────────────────────────────────────────────

    fn gc_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kosha-test-gc-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn remove_path(dir: &Path) -> impl Fn(&str) -> Result<u64, KoshaError> + '_ {
        move |rel: &str| {
            let path = dir.join(rel);
            if path.is_dir() {
                std::fs::remove_dir_all(&path)?;
            } else {
                std::fs::remove_file(&path)?;
            }
            Ok(1)
        }
    }

    fn text_search(
        searcher: &kosha_query::Searcher,
        ns: &NamespaceId,
        manifest: &Manifest,
    ) -> usize {
        let query: kosha_core::SearchQuery =
            serde_json::from_value(serde_json::json!({"query_text": "doc", "max_results": 50}))
                .unwrap();
        searcher
            .search(ns, manifest, &query, Some(&manifest.tombstones))
            .unwrap()
            .results
            .len()
    }

    #[test]
    fn search_started_before_compaction_survives_compaction_and_local_gc() {
        let dir = gc_dir("inflight");
        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone()).with_flush_threshold(2);
        for i in 0..4 {
            idx.index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId(format!("d{i}")),
                    fields: vec![Field::text("title", format!("doc number {i}"))],
                }],
            )
            .unwrap();
        }
        commit(&idx, &ns);
        let searcher = kosha_query::Searcher::new(dir.clone());

        // A search takes its snapshot (and opens the segments) before the
        // compaction, and is still running after it publishes.
        let snapshot = idx.manifest_shared(&ns).unwrap();
        let inputs: Vec<SegmentId> = segment_ids(&snapshot).into_iter().collect();
        assert_eq!(inputs.len(), 2);
        assert_eq!(text_search(&searcher, &ns, &snapshot), 4);
        // A reader the search holds directly (the lazily-read doc store).
        let reader = Arc::new(
            kosha_segment::SegmentReader::open(dir.join("test").join(&inputs[0].0)).unwrap(),
        );
        let reader_alive = Arc::downgrade(&reader);
        let in_use = |ns: &NamespaceId, seg: &SegmentId| {
            let held = seg == &inputs[0] && reader_alive.strong_count() > 0;
            searcher.release_segment(&ns.0, &seg.0) || held
        };

        assert!(
            idx.compact_namespace_with_options(&ns, CompactOptions::full())
                .unwrap()
                .merged
        );
        let (_, published) = idx.prepare_publication(&ns).unwrap().unwrap();
        idx.acknowledge_manifest(&ns, &published).unwrap();
        let mut retired: Vec<&SegmentId> =
            published.retired.iter().map(|o| &o.segment_id).collect();
        retired.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected: Vec<&SegmentId> = inputs.iter().collect();
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(retired, expected);
        assert!(published
            .retired
            .iter()
            .all(|o| o.retired_in_version == published.version && o.retired_at_ms > 0));

        // GC with zero grace while the snapshot is held: nothing goes.
        let pass = idx.reclaim_local(Duration::ZERO, in_use, remove_path(&dir));
        assert_eq!(pass.segments_removed, 0);
        assert_eq!(pass.pending, 2);
        assert_eq!(text_search(&searcher, &ns, &snapshot), 4);
        for id in &inputs {
            assert!(dir.join("test").join(&id.0).exists());
        }

        // The search finishes; the direct reader is still held.
        drop(snapshot);
        let pass = idx.reclaim_local(Duration::ZERO, in_use, remove_path(&dir));
        assert_eq!((pass.segments_removed, pass.pending), (1, 1));
        assert!(dir.join("test").join(&inputs[0].0).exists());
        assert_eq!(reader.doc_count(), 2);
        assert_eq!(reader.iter_doc_records().filter(|r| r.is_ok()).count(), 2);

        drop(reader);
        let pass = idx.reclaim_local(Duration::ZERO, in_use, remove_path(&dir));
        assert_eq!((pass.segments_removed, pass.pending), (1, 0));
        for id in &inputs {
            assert!(!dir.join("test").join(&id.0).exists());
        }
        assert_eq!(idx.local_retired_count(), 0);
        // New searches use the merged segment.
        let fresh = idx.manifest_shared(&ns).unwrap();
        assert_eq!(text_search(&searcher, &ns, &fresh), 4);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn local_gc_waits_for_the_grace_window() {
        let dir = gc_dir("grace");
        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone()).with_flush_threshold(1);
        write_docs(&idx, &ns, &["a"]);
        write_docs(&idx, &ns, &["b"]);
        commit(&idx, &ns);
        idx.compact_namespace_with_options(&ns, CompactOptions::full())
            .unwrap();
        // Not committed yet: the control store still lists the inputs.
        let pass = idx.reclaim_local(Duration::ZERO, |_, _| false, remove_path(&dir));
        assert_eq!((pass.segments_removed, pass.pending), (0, 2));
        commit(&idx, &ns);
        let pass = idx.reclaim_local(Duration::from_secs(3600), |_, _| false, remove_path(&dir));
        assert_eq!((pass.segments_removed, pass.pending), (0, 2));
        let pass = idx.reclaim_local(Duration::ZERO, |_, _| false, remove_path(&dir));
        assert_eq!((pass.segments_removed, pass.pending), (2, 0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn superseded_deletion_generations_are_recorded_and_reclaimed() {
        let dir = gc_dir("dv");
        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone());
        write_docs(&idx, &ns, &["a", "b", "c"]);
        commit(&idx, &ns);
        delete_id(&idx, &ns, "a");
        let (_, first) = idx.prepare_publication(&ns).unwrap().unwrap();
        idx.acknowledge_manifest(&ns, &first).unwrap();
        let (seg, old) = first.deletion_vectors.iter().next().unwrap();
        let old_path = dir.join(old.path(&ns, seg).unwrap());
        assert!(old_path.exists());
        assert!(first.retired.is_empty());

        delete_id(&idx, &ns, "b");
        let (_, second) = idx.prepare_publication(&ns).unwrap().unwrap();
        // A retried prepare of the same candidate is byte-identical.
        let (_, again) = idx.prepare_publication(&ns).unwrap().unwrap();
        assert!(second.same_contents(&again));
        idx.acknowledge_manifest(&ns, &second).unwrap();
        let new = &second.deletion_vectors[seg];
        assert_ne!(new.generation, old.generation);
        assert_eq!(second.retired.len(), 1);
        assert_eq!(second.retired[0].segment_id, *seg);
        assert_eq!(
            second.retired[0].deletion_generation.as_deref(),
            Some(old.generation.as_str())
        );

        let pass = idx.reclaim_local(Duration::ZERO, |_, _| false, remove_path(&dir));
        assert_eq!(pass.deletion_vectors_removed, 1);
        assert!(!old_path.exists());
        assert!(dir.join(new.path(&ns, seg).unwrap()).exists());
        assert!(dir.join("test").join(&seg.0).join("doc_store.bin").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retirement_records_carry_forward_until_the_committed_manifest_drops_them() {
        let dir = gc_dir("carry");
        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone()).with_flush_threshold(1);
        write_docs(&idx, &ns, &["a"]);
        write_docs(&idx, &ns, &["b"]);
        commit(&idx, &ns);
        idx.compact_namespace_with_options(&ns, CompactOptions::full())
            .unwrap();
        let (_, compacted) = idx.prepare_publication(&ns).unwrap().unwrap();
        idx.acknowledge_manifest(&ns, &compacted).unwrap();
        assert_eq!(compacted.retired.len(), 2);
        assert_eq!(idx.retired_segment_count(), 2);

        // Later publishes keep the records, with their original stamps.
        write_docs(&idx, &ns, &["c"]);
        let (_, later) = idx.prepare_publication(&ns).unwrap().unwrap();
        assert_eq!(later.retired, compacted.retired);
        idx.acknowledge_manifest(&ns, &later).unwrap();

        // GC dropped them in a newer committed version: the writer adopts it
        // on restore and stops carrying them.
        let mut pruned = later.clone();
        pruned.retired.clear();
        pruned.version += 1;
        idx.restore_manifest(ns.clone(), pruned);
        write_docs(&idx, &ns, &["d"]);
        let (_, next) = idx.prepare_publication(&ns).unwrap().unwrap();
        assert!(next.retired.is_empty());
        assert_eq!(idx.retired_segment_count(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
