pub mod compaction;
pub mod object_wal;
pub mod wal;

pub use compaction::{
    needs_compaction, plan_merge_group, tombstone_heavy_segments, tombstone_ratio, CompactMode,
    CompactOptions, CompactResult, CompactionPolicy, MergePlan,
};

use std::collections::{HashMap, HashSet};
use std::io::Write;
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
    /// The documents of the flush round in progress (#255): taken out of
    /// the buffer but not yet in a committed segment, so still part of the
    /// namespace's unindexed tail. Shared with the writer threads, not
    /// copied.
    flushing_docs: Vec<Arc<Vec<Document>>>,
    /// Bumped whenever the unindexed tail (buffer, flushing documents,
    /// pending deletes) may have changed without a manifest version bump.
    /// With the manifest version and [`Indexer::instance`] it names one
    /// tail state — see [`UnindexedTail::fingerprint`].
    tail_epoch: u64,
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

/// The writes a namespace has acknowledged but not yet indexed into a
/// segment of its manifest (#255): what a strong-consistency search scans
/// exhaustively on top of the indexed segments.
///
/// It is relative to one local manifest version, `manifest_version`: the
/// documents and deletes here are exactly what that version is missing.
/// A reader whose own manifest has another version must not combine the
/// two (it would double-count a flush that published in between, or miss
/// one) — it refreshes its manifest and asks again.
///
/// Served by the ingest pod as `GET /internal/namespaces/{ns}/unindexed`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct UnindexedTail {
    /// The local manifest version the tail applies on top of.
    pub manifest_version: u64,
    /// That manifest's `wal_checkpoint`: every WAL record after it is in
    /// the tail (informational; the version is what readers compare).
    #[serde(default)]
    pub wal_checkpoint: u64,
    /// Names this exact tail state (writer process, manifest version and a
    /// tail change counter). Equal fingerprints mean equal tails, so a
    /// reader can keep what it built from one and ask with
    /// `if_none_match` next time.
    #[serde(default)]
    pub fingerprint: String,
    /// The newest unindexed version of each upserted document, oldest
    /// first. Each one supersedes any indexed copy of its id.
    #[serde(default)]
    pub documents: Vec<Document>,
    /// Ids deleted since `manifest_version` whose deletes are not in the
    /// manifest yet (WAL-recovered deletes awaiting replay).
    #[serde(default)]
    pub deleted_ids: Vec<DocumentId>,
    /// The live indexed copies (segment, doc seq) that the documents and
    /// deletes above supersede: treated as deleted by a search over
    /// `manifest_version`, so the newest version wins and deletes apply.
    #[serde(default)]
    pub masked: HashMap<SegmentId, Vec<u32>>,
    /// The reader already has this tail (`known_fingerprint` matched):
    /// documents, deletes and masks are left out.
    #[serde(default)]
    pub not_modified: bool,
    /// The reader asked for a tail relative to another manifest version
    /// than the writer's current one: documents, deletes and masks are left
    /// out, and the reader should refresh its manifest and ask again.
    #[serde(default)]
    pub stale: bool,
}

impl UnindexedTail {
    /// Nothing to scan and nothing to hide.
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
            && self.deleted_ids.is_empty()
            && self.masked.values().all(Vec::is_empty)
    }
}

/// Approximate in-memory size of a buffered document, the unit of
/// `KOSHA_MAX_UNINDEXED_BYTES` (#255): its id and every field's name and
/// value, plus a small per-field overhead.
pub fn approx_document_bytes(doc: &Document) -> u64 {
    let fields: usize = doc
        .fields
        .iter()
        .map(|field| field.name.len() + field.value.len() + 8)
        .sum();
    (doc.id.0.len() + fields) as u64
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
                branch: None,
                schema: None,
                retired: Default::default(),
                shards: None,
            },
            published_version: 0,
            published_base: PublishedBase::default(),
            pending_bases: Vec::new(),
            tombstones: HashMap::new(),
            id_index: None,
            known: false,
            wal_sequence: 0,
            flushing_ids: HashSet::new(),
            flushing_docs: Vec::new(),
            tail_epoch: 0,
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

/// `(field name, is a late-interaction bag)`: see
/// [`NamespaceHandle::vector_dims`].
type VectorDimKey = (String, bool);
type NamespaceCentroidMetadataReader<'a> = dyn Fn(&str) -> Result<Vec<u8>, KoshaError> + 'a;

/// The dimension a `Vector` field value carries, for the write-time check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VectorValueDim {
    /// A flat vector of this many dimensions.
    Flat(usize),
    /// A late-interaction bag (#205) whose tokens all have this dimension.
    Bag(usize),
    /// A bag whose tokens disagree in dimension, or include an empty one.
    RaggedBag,
}

/// `None` for an empty vector or a value that is neither a vector nor a
/// bag: nothing indexes those, and they are stored as given.
fn vector_value_dim(value: &str) -> Option<VectorValueDim> {
    if let Ok(v) = serde_json::from_str::<Vec<f32>>(value) {
        return (!v.is_empty()).then_some(VectorValueDim::Flat(v.len()));
    }
    let bag = serde_json::from_str::<Vec<Vec<f32>>>(value).ok()?;
    let dim = bag.first()?.len();
    Some(if dim > 0 && bag.iter().all(|t| t.len() == dim) {
        VectorValueDim::Bag(dim)
    } else {
        VectorValueDim::RaggedBag
    })
}

/// What this node holds for a namespace, as [`Indexer::while_quiesced`]
/// reads it.
#[derive(Debug, Clone)]
pub struct LocalNamespace {
    /// The local manifest, including segments not yet published.
    pub manifest: Manifest,
    /// Whether anything here is not in the control store yet: a manifest
    /// version that was never published, buffered or WAL-recovered writes,
    /// or pending deletes.
    pub unpublished: bool,
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
    /// Dimension of each vector field written to this namespace (#204),
    /// learned lazily from segment footers and the write buffer on the
    /// first write, then from every accepted batch. See
    /// [`Indexer::check_vector_dims`]. Keyed by field and kind: a flat
    /// vector column and a late-interaction bag field (#205, whose
    /// dimension is its tokens') are tracked apart.
    vector_dims: Mutex<Option<HashMap<VectorDimKey, usize>>>,
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

/// What [`Indexer::delete_by_query_measured`] deleted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeleteOutcome {
    /// Documents newly tombstoned.
    pub documents: usize,
    /// Their records' bytes in `doc_store.bin`, as stored (compressed when
    /// the segment's doc store is). Zero for documents in legacy segments
    /// written before `doc_store.offsets`.
    pub stored_bytes: u64,
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
    /// Random per-process id: part of every [`UnindexedTail::fingerprint`],
    /// so a restarted writer never hands out a fingerprint an earlier
    /// process used for a different tail.
    instance: String,
    /// Whether compaction writes its merged segment's documents in
    /// recursive-graph-bisection order (#385). `KOSHA_DOC_REORDER`,
    /// default off; see [`kosha_segment::DocReorder`].
    doc_reorder: kosha_segment::DocReorder,
    /// Largest merged output (live docs) compaction reorders with BP;
    /// `0` (the default) never. `KOSHA_DOC_REORDER_COMPACTION_MAX_DOCS`;
    /// see [`kosha_segment::reorder::compaction_max_docs_from_env`].
    doc_reorder_compaction_max_docs: u64,
}

impl Indexer {
    pub fn new(data_dir: PathBuf) -> Self {
        let wal_dir = data_dir.join("_wal");
        // No WAL dir = every acknowledged write would fail; stop here, like
        // the failed recovery below, instead of at the first append.
        kosha_core::create_dir_named(&wal_dir).unwrap_or_else(|e| panic!("WAL: {e}"));
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
            instance: uuid::Uuid::new_v4().simple().to_string(),
            doc_reorder: kosha_segment::DocReorder::from_env(),
            doc_reorder_compaction_max_docs: kosha_segment::reorder::compaction_max_docs_from_env(),
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
        state.tail_epoch += 1;
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

    /// Compact into segments whose documents are reordered by `mode`
    /// (#385), regardless of `KOSHA_DOC_REORDER`.
    pub fn with_doc_reorder(mut self, mode: kosha_segment::DocReorder) -> Self {
        self.doc_reorder = mode;
        self
    }

    /// Cap on the merged output (live docs) compaction reorders with BP
    /// (#385), regardless of `KOSHA_DOC_REORDER_COMPACTION_MAX_DOCS`. `0`
    /// never reorders; a larger merge streams in input order.
    pub fn with_doc_reorder_compaction_max_docs(mut self, max_docs: u64) -> Self {
        self.doc_reorder_compaction_max_docs = max_docs;
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

    /// The BM25 parameters new segments are built with.
    pub fn bm25_params(&self) -> &Bm25Params {
        &self.bm25_params
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
                    vector_dims: Mutex::new(None),
                })
            })
            .clone()
    }

    /// A sharded root (#207) holds no documents: writes go to its shards.
    /// Reaching here with one is a routing bug, refused rather than written
    /// where no search would look.
    fn refuse_sharded_root(
        handle: &NamespaceHandle,
        namespace: &NamespaceId,
    ) -> Result<(), KoshaError> {
        if handle
            .state
            .lock()
            .unwrap()
            .manifest
            .shards
            .is_some_and(|n| n > 1)
        {
            return Err(KoshaError::InvalidQuery(format!(
                "namespace '{}' is sharded; write to its shards",
                namespace.0
            )));
        }
        Ok(())
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
        manifest: &Manifest,
        filter: &FilterClause,
    ) -> Result<usize, KoshaError> {
        self.delete_by_query_measured(namespace, manifest, filter)
            .map(|outcome| outcome.documents)
    }

    /// [`Self::delete_by_query`], also reporting the doc-store bytes of the
    /// deleted documents (usage metering, #348). Measured from the
    /// in-memory `doc_store.offsets` spans, so it reads nothing more than
    /// the delete itself does.
    pub fn delete_by_query_measured(
        &self,
        namespace: &NamespaceId,
        _manifest: &Manifest,
        filter: &FilterClause,
    ) -> Result<DeleteOutcome, KoshaError> {
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
        let mut stored_bytes = 0u64;
        for entry in &state.manifest.segments {
            let seg_dir = state
                .manifest
                .segment_dir(&self.data_dir, namespace, &entry.segment_id);
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
                // `None` for a legacy segment without `doc_store.offsets`.
                if let Some((_, length)) = reader.doc_span(*seq) {
                    stored_bytes = stored_bytes.saturating_add(u64::from(length));
                }
            }
            changes.push((entry.segment_id.clone(), matching));
        }
        let total = changes.iter().map(|(_, ids)| ids.len()).sum();
        // Crash-point seam (#257): the matches are known, nothing of the
        // delete is durable or applied yet.
        fail::fail_point!("delete.persist.before");
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
        Ok(DeleteOutcome {
            documents: total,
            stored_bytes: if total > 0 { stored_bytes } else { 0 },
        })
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
                let seg_dir = state
                    .manifest
                    .segment_dir(data_dir, namespace, &entry.segment_id);
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

    /// Reject a batch whose vectors disagree in dimension with their
    /// field's (#204): each vector column has one dimension per namespace,
    /// fixed by the first vector written to it. Without this check a
    /// mismatched document would land in the buffer and could only be
    /// dropped from the index at flush time, silently.
    ///
    /// Late-interaction bags (#205) are held to the same rule for their
    /// token dimension, and a ragged bag (tokens of different lengths) is
    /// rejected outright: a segment indexes a bag field at the dimension of
    /// the first bag it sees and leaves every other bag out of the index,
    /// and a segment whose dimension differs from the query's fails every
    /// late-interaction search of the namespace.
    ///
    /// Called with the namespace's `writes` lock held, so accepted batches
    /// update the map in order.
    fn check_vector_dims(
        &self,
        namespace: &NamespaceId,
        handle: &NamespaceHandle,
        documents: &[Document],
    ) -> Result<(), KoshaError> {
        let mut vectors: Vec<(&DocumentId, VectorDimKey, usize)> = Vec::new();
        for doc in documents {
            for f in &doc.fields {
                if f.field_type != kosha_core::FieldType::Vector {
                    continue;
                }
                match vector_value_dim(&f.value) {
                    Some(VectorValueDim::Flat(dim)) => {
                        vectors.push((&doc.id, (f.name.clone(), false), dim))
                    }
                    Some(VectorValueDim::Bag(dim)) => {
                        vectors.push((&doc.id, (f.name.clone(), true), dim))
                    }
                    Some(VectorValueDim::RaggedBag) => {
                        return Err(KoshaError::InvalidQuery(format!(
                            "document '{}': late-interaction field '{}' holds token vectors \
                             of different dimensions",
                            doc.id.0, f.name
                        )));
                    }
                    None => {}
                }
            }
        }
        if vectors.is_empty() {
            return Ok(());
        }
        let mut guard = handle.vector_dims.lock().unwrap();
        if guard.is_none() {
            *guard = Some(self.known_vector_dims(namespace, handle));
        }
        let known = guard.as_mut().unwrap();
        let mut batch: HashMap<&VectorDimKey, usize> = HashMap::new();
        for (id, key, dim) in &vectors {
            let expected = known.get(key).copied().or_else(|| batch.get(key).copied());
            match expected {
                Some(expected) if expected != *dim => {
                    let (field, bag) = (&key.0, key.1);
                    return Err(KoshaError::InvalidQuery(if bag {
                        format!(
                            "document '{}': late-interaction field '{field}' has \
                             {dim}-dimensional token vectors, but '{field}' tokens in \
                             namespace '{}' have {expected}",
                            id.0, namespace.0
                        )
                    } else {
                        format!(
                            "document '{}': vector field '{field}' has {dim} dimensions, but \
                             '{field}' vectors in namespace '{}' have {expected}",
                            id.0, namespace.0
                        )
                    }));
                }
                Some(_) => {}
                None => {
                    batch.insert(key, *dim);
                }
            }
        }
        for (key, dim) in batch {
            known.insert(key.clone(), dim);
        }
        Ok(())
    }

    /// Vector field dimensions already in the namespace: from the footers
    /// of its segments (those written since #204 list their columns, and
    /// every segment lists its late-interaction fields) and from documents
    /// still in the write buffer.
    fn known_vector_dims(
        &self,
        namespace: &NamespaceId,
        handle: &NamespaceHandle,
    ) -> HashMap<VectorDimKey, usize> {
        let state = handle.state.lock().unwrap().clone();
        let mut dims = HashMap::new();
        for entry in &state.manifest.segments {
            // The manifest's cached footer when it has one; else the local
            // footer.json, wherever the segment lives (a branch's inherited
            // segments sit under their home namespace, #208).
            let local;
            let footer = match state.manifest.segment_footer(&entry.segment_id) {
                Some(footer) => footer,
                None => {
                    let seg_dir =
                        state
                            .manifest
                            .segment_dir(&self.data_dir, namespace, &entry.segment_id);
                    match kosha_segment::SegmentReader::read_footer(&seg_dir) {
                        Ok(footer) => {
                            local = footer;
                            &local
                        }
                        Err(_) => continue,
                    }
                }
            };
            for meta in footer.vector_fields.iter().flatten() {
                dims.entry((meta.name.clone(), false))
                    .or_insert(meta.dim as usize);
            }
            for meta in &footer.multi_vectors {
                dims.entry((meta.name.clone(), true))
                    .or_insert(meta.dim as usize);
            }
        }
        let buffered = state.buffer.iter().flat_map(|buf| &buf.documents);
        for field in buffered.flat_map(|doc| &doc.fields) {
            if field.field_type != kosha_core::FieldType::Vector {
                continue;
            }
            match vector_value_dim(&field.value) {
                Some(VectorValueDim::Flat(dim)) => {
                    dims.entry((field.name.clone(), false)).or_insert(dim);
                }
                Some(VectorValueDim::Bag(dim)) => {
                    dims.entry((field.name.clone(), true)).or_insert(dim);
                }
                _ => {}
            }
        }
        dims
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
        Self::refuse_sharded_root(&handle, &namespace)?;
        if self.needs_recovery_hydration(&namespace) {
            self.flush_namespace(&namespace)?;
        }
        self.check_vector_dims(&namespace, &handle, &documents)?;
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
            state.tail_epoch += 1;
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

    /// Estimate the complete documents a partial replacement would write.
    /// This is a read-only preflight so a namespace-level quota can check
    /// all shards before the first shard is written.
    pub fn merged_replacement_bytes(
        &self,
        namespace: &NamespaceId,
        documents: Vec<Document>,
    ) -> Result<u64, KoshaError> {
        let handle = self.ns_handle(namespace);
        let _compact = handle.compact.lock().unwrap();
        let _mutation = handle.mutation.lock().unwrap();
        self.ensure_id_index(namespace, &handle)?;
        let snapshot = handle.state.lock().unwrap().clone();
        dedupe_documents_last_wins(documents)
            .into_iter()
            .try_fold(0u64, |bytes, mut doc| {
                let latest = Self::live_upsert_locations(&snapshot, &doc.id)
                    .next_back()
                    .filter(|_| !snapshot.recovered_deletes.contains(&doc.id));
                self.merge_replacement(namespace, &snapshot, &mut doc, latest)?;
                Ok(bytes.saturating_add(approx_document_bytes(&doc)))
            })
    }

    fn live_upsert_locations<'a>(
        snapshot: &'a NamespaceState,
        id: &DocumentId,
    ) -> impl DoubleEndedIterator<Item = &'a (SegmentId, u32)> {
        snapshot
            .id_index
            .as_ref()
            .unwrap()
            .get(id)
            .into_iter()
            .flatten()
            .filter(|(seg, seq)| {
                !snapshot
                    .tombstones
                    .get(seg)
                    .is_some_and(|ids| ids.contains(seq))
            })
    }

    fn merge_replacement(
        &self,
        namespace: &NamespaceId,
        snapshot: &NamespaceState,
        doc: &mut Document,
        latest: Option<&(SegmentId, u32)>,
    ) -> Result<(), KoshaError> {
        let buffered = snapshot
            .buffer
            .as_ref()
            .and_then(|b| b.documents.iter().rev().find(|d| d.id == doc.id));
        let fields = if let Some(buffered) = buffered {
            buffered.fields.clone()
        } else if let Some((seg, seq)) = latest {
            let path = snapshot
                .manifest
                .segment_dir(&self.data_dir, namespace, seg);
            let reader = kosha_segment::SegmentReader::open_with_options(path, false)?;
            reader
                .doc_record_full(*seq)?
                .ok_or_else(|| KoshaError::NotFound("replacement document missing".into()))?
                .fields
        } else {
            return Ok(());
        };
        doc.fields = merge_fields(fields, std::mem::take(&mut doc.fields));
        Ok(())
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
        Self::refuse_sharded_root(&handle, &namespace)?;
        // Recovery must be resolved before assigning a new operation order.
        if self.needs_recovery_hydration(&namespace) {
            self.flush_namespace(&namespace)?;
        }
        self.check_vector_dims(&namespace, &handle, &documents)?;
        let _compact = handle.compact.lock().unwrap();
        let _mutation = handle.mutation.lock().unwrap();
        self.ensure_id_index(&namespace, &handle)?;
        let snapshot = handle.state.lock().unwrap().clone();
        before_io();
        fail::fail_point!("upsert.before_io");
        let mut locations = Vec::new();
        let mut replacements = Vec::with_capacity(count);
        for mut doc in documents {
            let from = locations.len();
            locations.extend(Self::live_upsert_locations(&snapshot, &doc.id).cloned());
            if merge {
                self.merge_replacement(&namespace, &snapshot, &mut doc, locations[from..].last())?;
            }
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
        // Crash-point seam (#257): the replacement segment is on disk; the
        // old copies are not tombstoned yet.
        fail::fail_point!("upsert.segment_written");
        let origin = self.wal.origin().to_owned();
        let mut state = handle.state.lock().unwrap();
        let ids: HashSet<_> = replacements.iter().map(|doc| &doc.id).collect();
        if let Some(buffer) = state.buffer.as_mut() {
            buffer.documents.retain(|doc| !ids.contains(&doc.id));
        }
        state.tail_epoch += 1;
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
            state.tail_epoch += 1;
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
                // Scheduling seam (#257): a piggybacker that registered its
                // request but has not started waiting yet. Waiting on a
                // generation captured above is correct whether or not the
                // leader finishes in between; default builds skip this.
                #[cfg(feature = "failpoints")]
                let coord = {
                    drop(coord);
                    fail::fail_point!("flush.piggyback.registered");
                    handle.flush_coord.lock().unwrap()
                };
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
            // Scheduling seam (#257): a leader about to start a pass.
            fail::fail_point!("flush.leader.pass");
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
            let chunks: Vec<_> = Self::take_flush_chunks_in(
                &mut state,
                self.max_concurrent_flush_writers,
                self.min_docs_per_flush_writer,
            )
            .into_iter()
            .map(|(docs, seg_id, params)| (Arc::new(docs), seg_id, params))
            .collect();
            // Still unindexed until phase 3 commits them (#255).
            state.flushing_docs = chunks.iter().map(|(docs, _, _)| Arc::clone(docs)).collect();
            state.tail_epoch += 1;
            (chunks, checkpoint, analysis)
        };
        // Scheduling seam (#257): the buffer is snapshotted, nothing is
        // written yet.
        fail::fail_point!("flush.snapshot_taken");
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
        let results: Vec<FlushChunkOutcome> = if chunks.len() == 1 {
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
        let mut failed_docs: Vec<Arc<Vec<Document>>> = Vec::new();
        let mut first_err: Option<KoshaError> = None;
        let mut succeeded: Vec<(SegmentId, Arc<Vec<Document>>, kosha_core::Footer)> = Vec::new();
        for (seg_id, docs, result) in results {
            match result {
                Ok(footer) => succeeded.push((seg_id, docs, footer)),
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                    failed_docs.push(docs);
                }
            }
        }

        // A failed round publishes no partial snapshot. Keep every input
        // in the WAL and restore it ahead of newer buffered writes.
        if let Some(error) = first_err {
            for (_, docs, _) in succeeded {
                failed_docs.push(docs);
            }
            let mut state = handle.state.lock().unwrap();
            // Released first so the chunks below unwrap without a copy.
            state.flushing_docs.clear();
            state.tail_epoch += 1;
            let mut failed_docs: Vec<Document> =
                failed_docs.into_iter().flat_map(unshare_docs).collect();
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

        // Crash-point seam (#257): every chunk's segment is on disk, none
        // is in the manifest yet.
        fail::fail_point!("flush.segment_written");

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
            state.flushing_docs.clear();
            state.tail_epoch += 1;
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
                buf.documents
                    .extend(failed_docs.into_iter().flat_map(unshare_docs));
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
        docs: Arc<Vec<Document>>,
        seg_id: SegmentId,
        bm25_params: Bm25Params,
        analysis: &kosha_core::AnalysisConfig,
    ) -> FlushChunkOutcome {
        let _permit = handle.writer_slots.acquire();
        let seg_dir = ns_dir.join(seg_id.0.as_str());
        let mut writer = SegmentWriter::new(seg_id.clone(), seg_dir).with_analysis(analysis);
        for doc in docs.iter() {
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

        fail::fail_point!("flush.segment_written");

        // WAL reclamation waits for durable manifest publication.
        let _ = (wal_enabled, wal);
        state.tail_epoch += 1;
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
        let vector_index = opts.vector_index;
        self.compact_with_selector(
            namespace,
            |manifest, seg_dir| {
                select_merge_inputs(
                    manifest,
                    &opts,
                    |seg_id| seg_dir(seg_id).exists(),
                    |seg_id| segment_dir_bytes(&seg_dir(seg_id)),
                )
            },
            vector_index,
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
            |manifest, seg_dir| {
                let inputs: Vec<ManifestEntry> = plan
                    .inputs
                    .iter()
                    .filter_map(|planned| {
                        manifest
                            .segments
                            .iter()
                            .find(|e| e.segment_id == planned.segment_id)
                    })
                    .filter(|e| seg_dir(&e.segment_id).exists())
                    .cloned()
                    .collect();
                // Two or more, or one tombstone-heavy segment (#464).
                (inputs.len() >= 2
                    || compaction::is_tombstone_rewrite(manifest, &self.compaction_policy, &inputs))
                .then_some(MergePlan { inputs })
            },
            None,
            || {},
        )
    }

    fn compact_with_selector(
        &self,
        namespace: &NamespaceId,
        select: impl FnOnce(&Manifest, &dyn Fn(&SegmentId) -> PathBuf) -> Option<MergePlan>,
        // How the merged vector indexes are built (#253); `None` reads
        // `KOSHA_VECTOR_COMPACTION`.
        vector_index: Option<kosha_segment::VectorCompactionMode>,
        after_plan: impl FnOnce(),
    ) -> Result<CompactResult, KoshaError> {
        let handle = self.ns_handle(namespace);
        let _compact_guard = handle.compact.lock().unwrap();

        let (plan, tombstones, bm25_params, segments_before, analysis, input_dirs) = {
            let mut state = handle.state.lock().unwrap();
            if !state.known {
                return Ok(CompactResult::unchanged(0));
            }
            let segments_before = state.manifest.segments.len();
            let manifest = &state.manifest;
            // A branch's inherited segments live under their home
            // namespace's prefix (#208).
            let seg_dir =
                |seg_id: &SegmentId| manifest.segment_dir(&self.data_dir, namespace, seg_id);
            let plan = match select(manifest, &seg_dir) {
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
            // Inputs are read where they live: a branch's inherited inputs
            // stay in the parent's prefix (#208). The output always goes to
            // this namespace's own prefix.
            let input_dirs: HashMap<SegmentId, PathBuf> = plan
                .inputs
                .iter()
                .map(|entry| {
                    let dir =
                        state
                            .manifest
                            .segment_dir(&self.data_dir, namespace, &entry.segment_id);
                    (entry.segment_id.clone(), dir)
                })
                .collect();
            (
                plan,
                tombstones,
                bm25_params,
                segments_before,
                analysis,
                input_dirs,
            )
        };

        after_plan();
        fail::fail_point!("compaction.planned");

        // ── Merge I/O with state lock released (DESIGN.md §7.1) ──────────
        let ns_dir = self.data_dir.join(&namespace.0);
        let seg_id = SegmentId(format!(
            "{}-compact-{}",
            namespace.0.replace('/', "_"),
            uuid::Uuid::new_v4().simple()
        ));
        let seg_dir = ns_dir.join(seg_id.0.as_str());
        // Research #388: a merge keeps its inputs' doc-store format unless
        // `KOSHA_DOC_STORE_COMPRESSION` says otherwise. Footers only; an
        // unreadable one is decided by the open below, as before.
        let any_input_compressed = plan.inputs.iter().any(|entry| {
            kosha_segment::SegmentReader::read_footer(&input_dirs[&entry.segment_id])
                .is_ok_and(|footer| footer.doc_store_compression.is_some())
        });
        let mut writer = kosha_segment::SegmentWriter::new(seg_id.clone(), seg_dir)
            .with_analysis(&analysis)
            .inherit_doc_store_compression(any_input_compressed)
            .with_vector_radius_reference(true);
        let mut any_docs = false;
        let old_segment_ids = plan.input_ids();
        let seed_vectors = vector_index
            .unwrap_or_else(kosha_segment::VectorCompactionMode::from_env)
            == kosha_segment::VectorCompactionMode::Lire;
        let vector_profile = if seed_vectors
            && std::env::var("KOSHA_VECTOR_COMPACTION_PROFILE").as_deref() == Ok("1")
        {
            writer = writer.with_vector_compaction_profile();
            Some(writer.vector_index_builds())
        } else {
            None
        };
        // (old segment_id, old doc_seq) → the doc's doc_seq in the merged
        // output, for every doc actually copied. `SegmentWriter::add_document`
        // assigns doc_seq as `self.doc_records.len()` at call time (strictly
        // in call order), so `next_new_seq` tracks it exactly. Only needed to
        // carry a tombstone forward (see the publish block below) — a doc
        // excluded here by the snapshot below never needs an entry.
        let mut seq_remap: HashMap<(SegmentId, u32), u32> = HashMap::new();
        let mut next_new_seq: u32 = 0;

        // #385: BP buffers the whole merged output in memory, so it only
        // runs when explicitly opted in for compaction and the merge is
        // small enough; otherwise the streaming path below (input order).
        let bp_compaction = self.doc_reorder == kosha_segment::DocReorder::Bp && {
            let planned_live: u64 = plan
                .inputs
                .iter()
                .map(|e| {
                    let dead = tombstones.get(&e.segment_id).map_or(0, |t| t.len()) as u64;
                    (e.doc_count as u64).saturating_sub(dead)
                })
                .sum();
            let cap = self.doc_reorder_compaction_max_docs;
            if planned_live > cap {
                eprintln!(
                    "WARN: KOSHA_DOC_REORDER=bp but compaction of {} has {planned_live} live docs \
                     > KOSHA_DOC_REORDER_COMPACTION_MAX_DOCS={cap}; merging in input order",
                    namespace.0
                );
            }
            planned_live <= cap
        };
        if bp_compaction {
            // #385: the merged output's doc_seqs follow recursive graph
            // bisection instead of input order. Every live input document
            // is buffered (memory: the whole merged output's stored
            // records, unlike the streaming path below), ordered, then
            // added in that order, so the writer's own doc_seq-keyed
            // structures follow; `seq_remap` and the vector seeds are
            // remapped from the same order. Experimental, off by default.
            let mut readers = Vec::with_capacity(plan.inputs.len());
            type Buffered = (
                usize,
                u32,
                DocumentId,
                Vec<kosha_core::Field>,
                Vec<(usize, Vec<f32>)>,
            );
            let mut buffered: Vec<Buffered> = Vec::new();
            for (input, entry) in plan.inputs.iter().enumerate() {
                let reader =
                    kosha_segment::SegmentReader::open(input_dirs[&entry.segment_id].clone())?;
                let ts = tombstones.get(&entry.segment_id);
                // `?`: a doc that fails to read aborts the compaction, as
                // in the streaming path below.
                for raw in reader.iter_doc_records_raw() {
                    let kosha_segment::RawDocRecord {
                        record: doc_rec,
                        vectors,
                    } = raw?;
                    if ts.is_some_and(|set| set.contains(&doc_rec.doc_seq)) {
                        continue;
                    }
                    buffered.push((
                        input,
                        doc_rec.doc_seq,
                        doc_rec.doc_id,
                        doc_rec.fields,
                        vectors,
                    ));
                }
                readers.push(reader);
            }
            let order = writer.bp_order(
                buffered.iter().map(|b| b.3.as_slice()),
                &kosha_segment::BpConfig::default(),
            );
            let mut input_remaps: Vec<Vec<u32>> = vec![Vec::new(); plan.inputs.len()];
            let mut slots: Vec<Option<Buffered>> = buffered.into_iter().map(Some).collect();
            for old in order {
                let (input, old_seq, doc_id, fields, vectors) = slots[old as usize]
                    .take()
                    .expect("bp_order is a permutation");
                seq_remap.insert(
                    (plan.inputs[input].segment_id.clone(), old_seq),
                    next_new_seq,
                );
                if seed_vectors {
                    let remap = &mut input_remaps[input];
                    if remap.len() <= old_seq as usize {
                        remap.resize(old_seq as usize + 1, u32::MAX);
                    }
                    remap[old_seq as usize] = next_new_seq;
                }
                next_new_seq += 1;
                writer.add_document_with_vectors(doc_id, fields, vectors);
                any_docs = true;
            }
            if seed_vectors {
                for (reader, remap) in readers.iter().zip(&input_remaps) {
                    seed_merged_vector_index(reader, remap, &mut writer);
                }
            }
        } else {
            for entry in &plan.inputs {
                let reader =
                    kosha_segment::SegmentReader::open(input_dirs[&entry.segment_id].clone())?;
                let ts = tombstones.get(&entry.segment_id);
                // This input's old doc_seq → merged doc_seq (`u32::MAX`: not
                // copied), for seeding the merged vector index (#253).
                let mut input_remap: Vec<u32> = Vec::new();
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
                    if seed_vectors {
                        let old = doc_rec.doc_seq as usize;
                        if input_remap.len() <= old {
                            input_remap.resize(old + 1, u32::MAX);
                        }
                        input_remap[old] = next_new_seq;
                    }
                    next_new_seq += 1;
                    writer.add_document_with_vectors(doc_rec.doc_id, doc_rec.fields, vectors);
                    any_docs = true;
                }
                if seed_vectors {
                    seed_merged_vector_index(&reader, &input_remap, &mut writer);
                }
            }
        }

        if !any_docs {
            let _ = std::fs::remove_dir_all(ns_dir.join(&seg_id.0));
            return Ok(CompactResult::unchanged(segments_before));
        }

        let vector_builds = writer.vector_index_builds();
        let footer = writer.finalize(bm25_params)?;
        // Staleness signals of every seeded column (#464), one line each:
        // what a KOSHA_VECTOR_REBUILD_* threshold is tuned from.
        for build in vector_builds.lock().unwrap().iter() {
            if build.lire || build.rebuild.is_some() {
                eprintln!(
                    "INFO: compaction {}/{} vector field '{}': {build}",
                    namespace.0, seg_id.0, build.field
                );
            }
        }
        let output_segment_id = seg_id.clone();
        // Crash-point seam (#257): the merged segment is on disk, the
        // manifest still lists its inputs.
        fail::fail_point!("compaction.output_written");

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

        if let Some(builds) = vector_profile {
            let mut stderr = std::io::stderr().lock();
            for build in builds.lock().unwrap().iter() {
                if let Some(profile) = &build.rewrite_profile {
                    let event = serde_json::json!({
                        "namespace": namespace.0,
                        "segment": output_segment_id.0,
                        "field": build.field,
                        "lire": build.lire,
                        "rewrite": profile,
                    });
                    let _ = writeln!(stderr, "vector_compaction_profile {event}");
                }
            }
        }

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
        let state = handle.state.lock().unwrap();
        if state.known && state.manifest.version > manifest.version {
            return;
        }
        // Relearn vector dimensions from the restored segments on the next
        // write (#204); `vector_dims` is only ever locked before `state`.
        drop(state);
        handle.vector_dims.lock().unwrap().take();
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
        state.tail_epoch += 1;
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

    /// Forget a namespace that has been removed from the control store
    /// (branch deletion, #208).
    ///
    /// Waits for in-flight writes, flushes, upserts and compactions, then
    /// refuses with `InvalidQuery` if the namespace holds anything not yet
    /// published (buffered or WAL-recovered documents, deletes, or a
    /// manifest newer than the published one): the caller flushes and
    /// publishes first, and a write that slipped in between must not be
    /// silently dropped. Otherwise runs `while_locked` (the control-store
    /// removal and object reclamation) with every namespace lock held, so
    /// no write, flush or publish can interleave, and only on its success
    /// resets the namespace to unknown. A write that arrives afterwards
    /// starts a new, empty namespace of the same name.
    pub fn drop_branch_namespace(
        &self,
        namespace: &NamespaceId,
        while_locked: impl FnOnce() -> Result<(), KoshaError>,
    ) -> Result<(), KoshaError> {
        let handle = self.ns_handle(namespace);
        // The order every writer takes them: writes -> compact -> mutation
        // -> state.
        let _writes = handle.writes.lock().unwrap();
        let _compact = handle.compact.lock().unwrap();
        let _mutation = handle.mutation.lock().unwrap();
        let mut state = handle.state.lock().unwrap();
        let buffered = state.buffer.as_ref().map_or(0, |b| b.documents.len());
        let unpublished_deletes = state.known
            && PublishedBase::of(&state.manifest, &state.tombstones).tombstones
                != state.published_base.tombstones;
        if buffered > 0
            || !state.recovered.is_empty()
            || !state.recovered_deletes.is_empty()
            || (state.known && state.manifest.version != state.published_version)
            || unpublished_deletes
        {
            return Err(KoshaError::InvalidQuery(format!(
                "namespace {} has writes that are not published yet; flush it and retry",
                namespace.0
            )));
        }
        while_locked()?;
        let wal_sequence = state.wal_sequence;
        *state = NamespaceState::new();
        // WAL sequences are process-wide; never hand one out twice.
        state.wal_sequence = wal_sequence;
        drop(state);
        // Everything was published, but WAL files can outlive their
        // checkpoint; with the manifest gone nothing would skip them on
        // replay, so discard the namespace's records (as `drop_namespace`
        // does for `DELETE /v1/namespaces/{ns}`).
        self.wal.discard_namespace(namespace)?;
        Ok(())
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
        // Backstop for callers that skipped `embedding_conflict` (the HTTP
        // layer checks first and answers 409); re-checked here, where no
        // flush can land in between.
        if let Some(conflict) = self.embedding_conflict_in(namespace, &handle, &schema) {
            return Err(KoshaError::InvalidQuery(conflict));
        }
        // Schema evolution rules (#347), re-checked here for the same
        // reason: every route that sets a schema comes through this.
        let existing = self.schema_existing_data_in(namespace, &handle);
        let current = handle.state.lock().unwrap().manifest.schema.clone();
        if let Err(refused) =
            kosha_core::schema_evolution::check(current.as_ref(), &schema, &existing)
        {
            return Err(KoshaError::InvalidQuery(refused.message));
        }
        let mut state = handle.state.lock().unwrap();
        if state.known && state.manifest.schema.as_ref() == Some(&schema) {
            return Ok(false);
        }
        state.manifest.schema = Some(schema);
        state.manifest.version += 1;
        state.known = true;
        Ok(true)
    }

    /// Why setting `schema` on `namespace` would mix two embedding models in
    /// one vector field (#209), if it would: the current schema declares a
    /// model for a field, `schema` declares a different model or dimension
    /// for it, and the namespace already holds vectors for that field
    /// (a segment whose footer lists the column, or a buffered document
    /// carrying it). A segment that recorded another model than `schema`'s
    /// conflicts the same way. `None` = the change is safe. Changing to a
    /// different model requires a reindex, which is not automatic.
    pub fn embedding_conflict(
        &self,
        namespace: &NamespaceId,
        schema: &kosha_core::NamespaceSchema,
    ) -> Option<String> {
        let handle = self.ns_handle(namespace);
        self.embedding_conflict_in(namespace, &handle, schema)
    }

    /// What `namespace` holds, as the schema evolution rules (#347,
    /// [`kosha_core::schema_evolution`]) need it: whether any document is
    /// stored or buffered, and which vector fields hold vectors (with their
    /// dimension, from the segment footers).
    pub fn schema_existing_data(
        &self,
        namespace: &NamespaceId,
    ) -> kosha_core::schema_evolution::ExistingData {
        let handle = self.ns_handle(namespace);
        self.schema_existing_data_in(namespace, &handle)
    }

    fn schema_existing_data_in(
        &self,
        namespace: &NamespaceId,
        handle: &NamespaceHandle,
    ) -> kosha_core::schema_evolution::ExistingData {
        let state = handle.state.lock().unwrap().clone();
        let mut data = kosha_core::schema_evolution::ExistingData::default();
        let buffered = state
            .buffer
            .iter()
            .flat_map(|buf| buf.documents.iter())
            .chain(state.flushing_docs.iter().flat_map(|docs| docs.iter()));
        // A buffered vector of a field the current schema embeds was
        // embedded with that model (it is recorded when it is flushed).
        let declared = state.manifest.schema.clone().unwrap_or_default();
        for doc in buffered {
            data.has_documents = true;
            for field in &doc.fields {
                if field.field_type == kosha_core::FieldType::Vector {
                    data.vector_fields.entry(field.name.clone()).or_insert(None);
                    if let Some(spec) = declared.embedding_for(&field.name) {
                        data.vector_models
                            .entry(field.name.clone())
                            .or_default()
                            .insert(spec.model.clone());
                    }
                }
            }
        }
        for entry in &state.manifest.segments {
            if entry.doc_count > 0 {
                data.has_documents = true;
            }
            let fields = match state.manifest.segment_footer(&entry.segment_id) {
                Some(footer) => footer.vector_fields.clone(),
                None => {
                    let seg_dir =
                        state
                            .manifest
                            .segment_dir(&self.data_dir, namespace, &entry.segment_id);
                    match kosha_segment::SegmentReader::read_footer(&seg_dir) {
                        Ok(footer) => footer.vector_fields,
                        // Unreadable here: assume it holds documents and
                        // say nothing about its vectors.
                        Err(_) => continue,
                    }
                }
            };
            for meta in fields.into_iter().flatten() {
                if let Some(model) = meta.embedding_model {
                    data.vector_models
                        .entry(meta.name.clone())
                        .or_default()
                        .insert(model);
                }
                data.vector_fields.insert(meta.name, Some(meta.dim));
            }
        }
        data
    }

    fn embedding_conflict_in(
        &self,
        namespace: &NamespaceId,
        handle: &NamespaceHandle,
        schema: &kosha_core::NamespaceSchema,
    ) -> Option<String> {
        let state = {
            let state = handle.state.lock().unwrap();
            let old_declares = state
                .manifest
                .schema
                .as_ref()
                .is_some_and(|old| old.embedded_fields().next().is_some());
            if !old_declares && schema.embedded_fields().next().is_none() {
                // Neither schema embeds anything: nothing to pin.
                return None;
            }
            state.clone()
        };
        // Per vector field: the models its segments recorded, and whether
        // any segment indexes it at all.
        let mut indexed: HashMap<String, Vec<Option<String>>> = HashMap::new();
        for entry in &state.manifest.segments {
            let fields = match state.manifest.segment_footer(&entry.segment_id) {
                Some(footer) => footer.vector_fields.clone(),
                None => {
                    let seg_dir =
                        state
                            .manifest
                            .segment_dir(&self.data_dir, namespace, &entry.segment_id);
                    match kosha_segment::SegmentReader::read_footer(&seg_dir) {
                        Ok(footer) => footer.vector_fields,
                        Err(_) => continue,
                    }
                }
            };
            for meta in fields.into_iter().flatten() {
                indexed
                    .entry(meta.name)
                    .or_default()
                    .push(meta.embedding_model);
            }
        }
        let buffered = |field: &str| {
            state
                .buffer
                .iter()
                .flat_map(|buf| buf.documents.iter())
                .chain(state.flushing_docs.iter().flat_map(|docs| docs.iter()))
                .any(|doc| doc.fields.iter().any(|f| f.name == field))
        };
        let conflict = |field: &str, from: &str, to: &kosha_core::EmbeddingSpec| {
            format!(
                "field {field:?} is indexed with model {from}; changing to {} ({} dims) \
                 requires a reindex (delete and re-ingest the namespace's documents, or use a \
                 new field)",
                to.model, to.dims
            )
        };
        if let Some(old) = state.manifest.schema.as_ref() {
            for (field, old_spec) in old.embedded_fields() {
                let new_spec = schema.embedding_for(field);
                if new_spec.is_some_and(|new| new.same_space(old_spec)) {
                    continue;
                }
                // Dropping the declaration leaves the stored vectors as
                // they are; clients then send their own vectors.
                let Some(new_spec) = new_spec else {
                    continue;
                };
                if indexed.contains_key(field.as_str()) || buffered(field) {
                    let from = format!("{} ({} dims)", old_spec.model, old_spec.dims);
                    return Some(conflict(field, &from, new_spec));
                }
            }
        }
        for (field, new_spec) in schema.embedded_fields() {
            let recorded = indexed.get(field.as_str()).into_iter().flatten().flatten();
            if let Some(model) = recorded.into_iter().find(|m| **m != new_spec.model) {
                return Some(conflict(field, model, new_spec));
            }
        }
        None
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
    /// namespace's records (group-commit files). Recovery skips this
    /// namespace's records in them (see `WalWriter::discard_namespace`).
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

    /// Finish discarding a deleted namespace whose storage reclaim is being
    /// retried (#347), provided nothing was written to it since
    /// [`Self::drop_namespace`]: it is still unknown, with an empty buffer,
    /// no pending deletes and no WAL records awaiting replay.
    ///
    /// Then its WAL records are discarded again (a no-op when the first
    /// discard succeeded) and `while_locked` runs under the namespace's
    /// locks, as in `drop_namespace`: the place to remove its local segment
    /// files. Returns the WAL files kept because other namespaces share
    /// them.
    ///
    /// `Ok(None)` when the namespace holds anything: a new incarnation was
    /// written (or recovered from the WAL) and its files now share the
    /// namespace's directory. Nothing is touched and `while_locked` does
    /// not run.
    pub fn discard_if_untouched(
        &self,
        namespace: &NamespaceId,
        while_locked: impl FnOnce(),
    ) -> Result<Option<usize>, KoshaError> {
        let handle = self.ns_handle(namespace);
        let _writes = handle.writes.lock().unwrap();
        let _compact = handle.compact.lock().unwrap();
        let _mutation = handle.mutation.lock().unwrap();
        {
            let state = handle.state.lock().unwrap();
            let buffered = state.buffer.as_ref().map_or(0, |b| b.documents.len());
            if state.known
                || buffered > 0
                || !state.tombstones.is_empty()
                || !state.recovered.is_empty()
                || !state.recovered_deletes.is_empty()
            {
                return Ok(None);
            }
        }
        // Still under the write locks: no record can be appended for the
        // namespace between the check and the discard.
        let shared = self.wal.discard_namespace(namespace)?;
        while_locked();
        Ok(Some(shared))
    }

    /// Run `sweep` while no write, flush, upsert, compaction or drop of
    /// `namespace` is in flight on this node (the locks
    /// [`Self::drop_namespace`] takes), handing it what this node holds for
    /// the namespace at that moment; `None` when it is unknown here.
    ///
    /// For removing local files of a deleted incarnation (#347) without
    /// touching the current one: under these locks every segment directory
    /// this writer has produced for the namespace is listed by the manifest
    /// in [`LocalNamespace`], published or not. A manifest a follower
    /// installs ([`Self::restore_manifest`]) is not held back by them.
    pub fn while_quiesced<R>(
        &self,
        namespace: &NamespaceId,
        sweep: impl FnOnce(Option<LocalNamespace>) -> R,
    ) -> R {
        let handle = self.ns_handle(namespace);
        let _writes = handle.writes.lock().unwrap();
        let _compact = handle.compact.lock().unwrap();
        let _mutation = handle.mutation.lock().unwrap();
        let local = {
            let state = handle.state.lock().unwrap();
            state.known.then(|| LocalNamespace {
                unpublished: state.published_version != state.manifest.version
                    || state
                        .buffer
                        .as_ref()
                        .is_some_and(|b| !b.documents.is_empty())
                    || !state.tombstones.is_empty()
                    || !state.recovered.is_empty()
                    || !state.recovered_deletes.is_empty(),
                manifest: state.manifest.clone(),
            })
        };
        sweep(local)
    }

    /// Forget a namespace this node only follows (a query replica's copy
    /// of a namespace deleted elsewhere): it reads as unknown until a
    /// manifest is restored for it again. Leaves the WAL alone, unlike
    /// [`Self::drop_namespace`]. Returns whether it was known.
    pub fn forget_namespace(&self, namespace: &NamespaceId) -> bool {
        let handle = {
            let map = self.namespaces.lock().unwrap();
            match map.get(namespace) {
                Some(handle) => handle.clone(),
                None => return false,
            }
        };
        let _writes = handle.writes.lock().unwrap();
        let _compact = handle.compact.lock().unwrap();
        let _mutation = handle.mutation.lock().unwrap();
        let mut state = handle.state.lock().unwrap();
        if !state.known {
            return false;
        }
        let mut fresh = NamespaceState::new();
        // Versions never go backwards on this node.
        fresh.manifest.version = state.manifest.version;
        fresh.published_version = state.published_version;
        fresh.wal_sequence = state.wal_sequence;
        *state = fresh;
        true
    }

    /// Make `namespace` a sharded root with a fixed `shards` count (#207),
    /// creating it if needed. Only a namespace that holds nothing yet — no
    /// segments, no buffered or recovered writes — can be sharded:
    /// documents already written live where unsharded routing put them,
    /// and moving them is resharding, which this does not do. Asking again
    /// for the count it already has is a no-op (`Ok(false)`); asking for a
    /// different one is refused. The caller publishes the manifest.
    pub fn set_shard_count(
        &self,
        namespace: &NamespaceId,
        shards: u32,
    ) -> Result<bool, KoshaError> {
        if !(2..=kosha_core::sharding::MAX_SHARDS).contains(&shards) {
            return Err(KoshaError::InvalidQuery(format!(
                "shards must be between 2 and {}",
                kosha_core::sharding::MAX_SHARDS
            )));
        }
        let handle = self.ns_handle(namespace);
        let _writes = handle.writes.lock().unwrap();
        let _mutation = handle.mutation.lock().unwrap();
        let mut state = handle.state.lock().unwrap();
        match state.manifest.shards {
            Some(existing) if existing == shards => return Ok(false),
            Some(existing) => {
                return Err(KoshaError::InvalidQuery(format!(
                    "namespace already has {existing} shards; resharding is not supported"
                )))
            }
            None => {}
        }
        let pending = state.buffer.as_ref().map_or(0, |b| b.documents.len())
            + state.recovered.len()
            + state.recovered_deletes.len();
        if !state.manifest.segments.is_empty() || pending > 0 {
            return Err(KoshaError::InvalidQuery(
                "namespace already holds documents; a shard count can only be set \
                 on a new, empty namespace"
                    .into(),
            ));
        }
        state.manifest.shards = Some(shards);
        state.manifest.version += 1;
        state.shared_manifest = None;
        state.known = true;
        Ok(true)
    }

    /// The fixed shard count of a sharded root namespace (#207); `None` for
    /// an ordinary namespace, a shard, or one this indexer does not know.
    pub fn shard_count(&self, namespace: &NamespaceId) -> Option<u32> {
        let map = self.namespaces.lock().unwrap();
        let handle = map.get(namespace)?;
        let state = handle.state.lock().unwrap();
        if !state.known {
            return None;
        }
        state.manifest.shards.filter(|&n| n > 1)
    }

    /// The fields `namespace`'s schema embeds server-side (#209), with
    /// their models. `None` — no schema, or one declaring no model — costs
    /// one map lookup and no allocation, like [`Self::shard_count`], so the
    /// write path of a namespace that never opted in is unchanged.
    pub fn embedded_fields(
        &self,
        namespace: &NamespaceId,
    ) -> Option<Vec<(String, kosha_core::EmbeddingSpec)>> {
        let map = self.namespaces.lock().unwrap();
        let handle = map.get(namespace)?;
        let state = handle.state.lock().unwrap();
        let schema = state.manifest.schema.as_ref()?;
        let mut fields = schema.embedded_fields().peekable();
        fields.peek()?;
        Some(
            fields
                .map(|(name, spec)| (name.clone(), spec.clone()))
                .collect(),
        )
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

    /// The committed version the writer's local manifest is based on.
    pub fn published_version(&self, namespace: &NamespaceId) -> Option<u64> {
        let handle = self.namespaces.lock().unwrap().get(namespace).cloned()?;
        let state = handle.state.lock().unwrap();
        state.known.then_some(state.published_version)
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
        manifest.retain_listed_segment_homes();
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

    /// Documents and approximate bytes ([`approx_document_bytes`]) written
    /// to `namespace` but not yet in a committed segment: the write buffer
    /// plus a flush round in progress. The backlog `KOSHA_MAX_UNINDEXED_*`
    /// caps (#255). `(0, 0)` for an unknown namespace.
    pub fn unindexed_usage(&self, namespace: &NamespaceId) -> (usize, u64) {
        let map = self.namespaces.lock().unwrap();
        let Some(handle) = map.get(namespace) else {
            return (0, 0);
        };
        let state = handle.state.lock().unwrap();
        let flushing = state.flushing_docs.iter().flat_map(|chunk| chunk.iter());
        let buffered = state.buffer.iter().flat_map(|b| b.documents.iter());
        flushing.chain(buffered).fold((0, 0), |(docs, bytes), doc| {
            (docs + 1, bytes + approx_document_bytes(doc))
        })
    }

    /// Approximate bytes not yet represented by a segment, including WAL
    /// records awaiting replay after recovery.
    pub fn pending_storage_bytes(&self, namespace: &NamespaceId) -> u64 {
        let Some(handle) = self.namespaces.lock().unwrap().get(namespace).cloned() else {
            return 0;
        };
        let state = handle.state.lock().unwrap();
        state
            .flushing_docs
            .iter()
            .flat_map(|chunk| chunk.iter())
            .chain(
                state
                    .buffer
                    .iter()
                    .flat_map(|buffer| buffer.documents.iter()),
            )
            .chain(
                state
                    .recovered
                    .iter()
                    .flat_map(|(_, record)| record.documents.iter()),
            )
            .map(approx_document_bytes)
            .fold(0, u64::saturating_add)
    }

    /// The namespace's unindexed tail (#255): every acknowledged write that
    /// is not in a segment of the current local manifest, with the indexed
    /// copies it supersedes. See [`UnindexedTail`].
    ///
    /// `expected_version`: the manifest version the caller searches. When
    /// the local version differs, the result is only a `stale` marker
    /// carrying the local version. `known_fingerprint`: the fingerprint of
    /// a tail the caller already holds; when it is still current the result
    /// is only a `not_modified` marker. Both skip copying the documents.
    ///
    /// `None` for a namespace this writer does not know. Computing the
    /// masks needs the id index (built on the first write anyway); building
    /// it reads every segment's doc metadata and fails if one is not local.
    pub fn unindexed_tail(
        &self,
        namespace: &NamespaceId,
        expected_version: Option<u64>,
        known_fingerprint: Option<&str>,
    ) -> Result<Option<UnindexedTail>, KoshaError> {
        let Some(handle) = self.namespaces.lock().unwrap().get(namespace).cloned() else {
            return Ok(None);
        };
        loop {
            {
                let state = handle.state.lock().unwrap();
                if !state.known {
                    return Ok(None);
                }
                let mut tail = UnindexedTail {
                    manifest_version: state.manifest.version,
                    wal_checkpoint: state.manifest.wal_checkpoint,
                    fingerprint: format!(
                        "{}-{}-{}",
                        self.instance, state.manifest.version, state.tail_epoch
                    ),
                    ..UnindexedTail::default()
                };
                if expected_version.is_some_and(|v| v != tail.manifest_version) {
                    tail.stale = true;
                    return Ok(Some(tail));
                }
                if known_fingerprint == Some(tail.fingerprint.as_str()) {
                    tail.not_modified = true;
                    return Ok(Some(tail));
                }
                // Oldest first: a flush round in progress holds older
                // writes than the buffer that kept filling behind it.
                let documents: Vec<Document> = state
                    .flushing_docs
                    .iter()
                    .flat_map(|chunk| chunk.iter())
                    .chain(state.buffer.iter().flat_map(|b| b.documents.iter()))
                    .cloned()
                    .collect();
                let documents = dedupe_documents_last_wins(documents);
                let upserted: HashSet<&DocumentId> = documents.iter().map(|d| &d.id).collect();
                let mut deleted_ids: Vec<DocumentId> = state
                    .recovered_deletes
                    .iter()
                    .filter(|id| !upserted.contains(id))
                    .cloned()
                    .collect();
                deleted_ids.sort_by(|a, b| a.0.cmp(&b.0));
                if documents.is_empty() && deleted_ids.is_empty() {
                    return Ok(Some(tail));
                }
                if let Some(index) = state.id_index.as_ref() {
                    let listed: HashSet<&SegmentId> = state
                        .manifest
                        .segments
                        .iter()
                        .map(|entry| &entry.segment_id)
                        .collect();
                    let mut masked: HashMap<SegmentId, Vec<u32>> = HashMap::new();
                    for id in upserted.iter().copied().chain(deleted_ids.iter()) {
                        for (segment, seq) in index.get(id).into_iter().flatten() {
                            let dead = state
                                .tombstones
                                .get(segment)
                                .is_some_and(|set| set.contains(seq));
                            if listed.contains(segment) && !dead {
                                masked.entry(segment.clone()).or_default().push(*seq);
                            }
                        }
                    }
                    for seqs in masked.values_mut() {
                        seqs.sort_unstable();
                        seqs.dedup();
                    }
                    drop(upserted);
                    tail.documents = documents;
                    tail.deleted_ids = deleted_ids;
                    tail.masked = masked;
                    return Ok(Some(tail));
                }
            }
            self.ensure_id_index(namespace, &handle)?;
        }
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
        let previous = std::mem::take(&mut manifest.deletion_vectors);
        for entry in &manifest.segments {
            if let Some(ids) = manifest
                .tombstones
                .get(&entry.segment_id)
                .filter(|ids| !ids.is_empty())
            {
                let (vector, bytes) = kosha_core::deletes::DeletionVector::encode(ids)?;
                // A bitmap a branch inherited unchanged stays where it is,
                // in the parent's prefix (#208): never copied, never
                // written to. Any change produces a new generation, which
                // goes to this namespace's own prefix.
                if let Some(inherited) = previous
                    .get(&entry.segment_id)
                    .filter(|old| old.is_inherited(namespace))
                    .filter(|old| old.generation == vector.generation)
                {
                    manifest
                        .deletion_vectors
                        .insert(entry.segment_id.clone(), inherited.clone());
                    continue;
                }
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
        let own_checkpoint = manifest
            .wal_origin
            .as_deref()
            .is_none_or(|origin| origin == self.wal.origin());
        if own_checkpoint {
            // WAL records replayed at startup that this commit covers are
            // durable in segments now. Without this, a namespace that had
            // no manifest to restore kept them forever: it reported them
            // as pending (so the idle flusher re-flushed it every tick)
            // and refused to be dropped. Found by the #257 crash harness.
            state
                .recovered
                .retain(|(seq, _)| *seq > manifest.wal_checkpoint);
        }
        drop(state);
        if self.wal_enabled && own_checkpoint {
            // The manifest is committed: a reclamation failure (an S3
            // delete timing out) only leaves files a later publish of this
            // or another namespace reclaims, and recovery filters records
            // at or below a published checkpoint. It must not turn a
            // durable write into a client error.
            // Crash-point seam (#257): the manifest is committed, the WAL
            // records it covers are not reclaimed yet.
            fail::fail_point!("wal.truncate.before");
            if let Err(error) = self.wal.clear_through(namespace, manifest.wal_checkpoint) {
                eprintln!(
                    "WARN: WAL reclamation through {} for {} failed after a committed \
                     publish (retried on the next publish): {error}",
                    manifest.wal_checkpoint, namespace.0
                );
            }
        }
        Ok(())
    }

    /// Advances routing metadata for a committed manifest without reading
    /// offsets from retained segments. Missing checkpoints are bootstrapped
    /// from the immutable offsets; a failed read leaves the old checkpoint
    /// intact, so queries can fall back.
    pub fn update_namespace_centroids(
        &self,
        namespace: &NamespaceId,
        manifest: &Manifest,
        previous: Option<&[u8]>,
        read_metadata: Option<&NamespaceCentroidMetadataReader<'_>>,
    ) -> Result<Option<Vec<u8>>, KoshaError> {
        use kosha_segment::namespace_centroids::{Snapshot, FILE_NAME};

        let path = self.data_dir.join(&namespace.0).join(FILE_NAME);
        let local = std::fs::read(&path).ok();
        let snapshot = [previous, local.as_deref()]
            .into_iter()
            .flatten()
            .filter_map(|bytes| Some((bytes, serde_json::from_slice::<Snapshot>(bytes).ok()?)))
            .filter(|(_, s)| s.directory.is_supported())
            .max_by_key(|(_, s)| s.version);
        let ids = || {
            manifest
                .segments
                .iter()
                .map(|entry| entry.segment_id.0.as_str())
        };
        if let Some((bytes, current)) = &snapshot {
            if current.matches(manifest.version, ids()) {
                // An unchanged checkpoint needs no disk rewrite. Retry an
                // earlier failed upload if it was only saved locally.
                return Ok((previous != Some(*bytes)).then(|| bytes.to_vec()));
            }
        }
        let mut snapshot = snapshot.map(|(_, s)| s).unwrap_or_else(|| Snapshot {
            version: 0,
            directory: Default::default(),
        });
        if snapshot.version > manifest.version {
            return Ok(None);
        }
        let live: HashSet<_> = ids().collect();
        let retired: Vec<_> = snapshot
            .directory
            .segment_ids()
            .filter(|id| !live.contains(id))
            .map(str::to_owned)
            .collect();
        for id in retired {
            snapshot.directory.remove_segment(&id);
        }
        for entry in &manifest.segments {
            let id = &entry.segment_id;
            if snapshot.directory.contains_segment(&id.0) {
                continue;
            }
            let dir = manifest.segment_dir(&self.data_dir, namespace, id);
            let columns = match (
                kosha_segment::SegmentReader::namespace_centroids(&dir),
                read_metadata,
            ) {
                (Err(_), Some(read)) => {
                    let relative = manifest.segment_rel_dir(namespace, id);
                    kosha_segment::SegmentReader::namespace_centroids_from(|file| {
                        read(&format!("{relative}/{file}"))
                    })?
                }
                (local, _) => local?,
            };
            snapshot.directory.insert_segment(&id.0, columns);
        }
        snapshot.version = manifest.version;
        let bytes = serde_json::to_vec(&snapshot)?;
        std::fs::create_dir_all(path.parent().unwrap())?;
        kosha_core::atomic_write(&path, &bytes)?;
        Ok(Some(bytes))
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
                // Lineage is fixed at branch time; segment homes are the
                // union (a segment id has one home wherever it is listed),
                // pruned to the merged segment list below.
                branch: merge_branch_info(remote.branch.as_ref(), state.manifest.branch.as_ref()),
                // A schema set on this writer since its last publication
                // wins (it is the newer admin action); otherwise keep the
                // remote's, which may carry another writer's change.
                schema: if state.manifest.schema != base.schema {
                    state.manifest.schema.clone()
                } else {
                    remote.schema.clone()
                },
                retired: Default::default(),
                // Fixed once set (#207): whichever side set it.
                shards: state.manifest.shards.or(remote.shards),
            };
            let mut manifest = manifest;
            manifest.retain_listed_segment_homes();
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
        self.reclaim_local_shared(grace, in_use, |_| false, remove)
    }

    /// [`Indexer::reclaim_local`], keeping (queued, not done) any object
    /// for which `shared(relative_path)` is true: a copy-on-write branch
    /// (#208) still lists it under the namespace that retired it.
    pub fn reclaim_local_shared(
        &self,
        grace: Duration,
        in_use: impl Fn(&NamespaceId, &SegmentId) -> bool,
        shared: impl Fn(&str) -> bool,
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
            if shared(&rel) {
                report.pending += 1;
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

/// One flush chunk's segment id, documents and `finalize` outcome.
type FlushChunkOutcome = (
    SegmentId,
    Arc<Vec<Document>>,
    Result<kosha_core::Footer, KoshaError>,
);

/// A flush chunk's documents back as an owned list: moved out when this is
/// the last reference (the usual case), copied otherwise.
fn unshare_docs(docs: Arc<Vec<Document>>) -> Vec<Document> {
    Arc::try_unwrap(docs).unwrap_or_else(|shared| (*shared).clone())
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

/// Hands one compaction input's vector postings to the merged segment's
/// writer (#253), so the merged index is seeded from them and repaired
/// with LIRE instead of re-clustering every vector. `remap[old_doc_seq]`
/// is the doc's merged doc_seq, `u32::MAX` (or out of range) when it was
/// not copied (deleted or tombstoned) — those members are dropped here.
///
/// Columns too small to have useful postings (fewer than
/// [`kosha_segment::LIRE_ADOPT_MIN_VECTORS`] vectors), without a clustered
/// index (legacy flat format, missing sidecar) or that fail to read are
/// skipped: the writer then absorbs their vectors one by one, or
/// clusters the column from scratch when most of it is uncovered. A
/// legacy footer, which does not name its vector column, is skipped too.
fn seed_merged_vector_index(
    reader: &kosha_segment::SegmentReader,
    remap: &[u32],
    writer: &mut kosha_segment::SegmentWriter,
) {
    let Some(fields) = reader.footer().vector_fields.clone() else {
        return;
    };
    for meta in fields {
        if !meta.offsets || (meta.count as usize) < kosha_segment::LIRE_ADOPT_MIN_VECTORS {
            continue;
        }
        let layout = match reader.vector_posting_layout(&meta.name) {
            Ok(Some(layout)) => layout,
            Ok(None) => continue,
            Err(e) => {
                eprintln!(
                    "WARN: compaction: cannot read the vector postings of {} field '{}' ({e}) \
                     — its vectors are re-clustered instead",
                    reader.segment_dir().display(),
                    meta.name
                );
                continue;
            }
        };
        let seeds = layout.postings.into_iter().map(|p| {
            let before = p.doc_seqs.len();
            let doc_seqs: Vec<u32> = p
                .doc_seqs
                .into_iter()
                .filter_map(|old| remap.get(old as usize).copied())
                .filter(|&new| new != u32::MAX)
                .collect();
            kosha_segment::VectorSeedPosting {
                centroid: p.centroid,
                dropped: before - doc_seqs.len(),
                doc_seqs,
            }
        });
        writer.seed_vector_postings_with_reference(&meta.name, seeds, meta.radius_reference);
    }
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

/// The branch lineage of a rebased manifest: either side's lineage (they
/// agree: it is fixed when the branch is created), with both sides' segment
/// homes.
fn merge_branch_info(
    remote: Option<&kosha_core::branch::BranchInfo>,
    local: Option<&kosha_core::branch::BranchInfo>,
) -> Option<kosha_core::branch::BranchInfo> {
    let mut merged = remote.or(local)?.clone();
    if let (Some(_), Some(local)) = (remote, local) {
        for (segment, home) in &local.segment_homes {
            merged
                .segment_homes
                .entry(segment.clone())
                .or_insert_with(|| home.clone());
        }
    }
    Some(merged)
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
                kosha_segment::SegmentReader::open(manifest.segment_dir(dir, ns, &entry.segment_id))
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

    /// Every file under `root`, with its bytes.
    fn tree_bytes(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
        let mut out = std::collections::BTreeMap::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    out.insert(path.clone(), std::fs::read(&path).unwrap());
                }
            }
        }
        out
    }

    fn publish(idx: &Indexer, ns: &NamespaceId) -> Manifest {
        let (_, manifest) = idx.prepare_publication(ns).unwrap().unwrap();
        idx.acknowledge_manifest(ns, &manifest).unwrap();
        manifest
    }

    fn live_ids(dir: &Path, ns: &NamespaceId, manifest: &Manifest) -> Vec<(String, String)> {
        let mut ids: Vec<_> = live_records(dir, ns, manifest)
            .into_iter()
            .map(|r| (r.doc_id.0, r.fields[0].value.clone()))
            .collect();
        ids.sort();
        ids
    }

    /// #208: a branch references its parent's segments and bitmaps in
    /// place; its writes, deletes, upserts and compactions go to its own
    /// prefix and leave every parent byte and the parent manifest as they
    /// were.
    #[test]
    fn a_branch_writes_deletes_and_compacts_without_touching_its_parent() {
        let dir = std::env::temp_dir().join(format!("kosha-branch-{}", uuid::Uuid::new_v4()));
        let parent = NamespaceId("t/prod".into());
        let child = NamespaceId("t/test".into());
        let idx = Indexer::new(dir.clone());
        let doc = |id: &str, text: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", text), Field::keyword("id", id)],
        };
        idx.index_documents(parent.clone(), vec![doc("d1", "one"), doc("d2", "two")])
            .unwrap();
        idx.flush_namespace(&parent).unwrap();
        idx.index_documents(parent.clone(), vec![doc("d3", "three"), doc("d4", "four")])
            .unwrap();
        idx.flush_namespace(&parent).unwrap();
        let filter: FilterClause = serde_json::from_str(r#"{"term": {"id": "d2"}}"#).unwrap();
        let m = idx.manifest(&parent).unwrap();
        assert_eq!(idx.delete_by_query(&parent, &m, &filter).unwrap(), 1);
        let published = publish(&idx, &parent);
        assert_eq!(published.deletion_vectors.len(), 1);
        let parent_files = tree_bytes(&dir.join(&parent.0));
        let parent_manifest = idx.manifest(&parent).unwrap();

        // Branch: the stored manifest references the parent's objects.
        let mut branch = published.branch_from(&parent, 1);
        kosha_core::deletes::hydrate(&mut branch, |segment, vector| {
            Ok(std::fs::read(dir.join(vector.path(&child, segment)?))?)
        })
        .unwrap();
        idx.restore_manifest(child.clone(), branch);
        assert!(
            !dir.join(&child.0).exists(),
            "branching must not write a byte"
        );
        let child_manifest = idx.manifest(&child).unwrap();
        assert_eq!(
            live_ids(&dir, &child, &child_manifest),
            live_ids(&dir, &parent, &parent_manifest)
        );
        // Publishing the untouched branch keeps the inherited bitmap.
        let republished = publish(&idx, &child);
        let vector = republished.deletion_vectors.values().next().unwrap();
        assert_eq!(vector.namespace.as_ref(), Some(&parent));
        assert!(!dir.join(&child.0).exists());

        // Write, delete from an inherited segment, upsert an inherited doc.
        idx.index_documents(child.clone(), vec![doc("d5", "five")])
            .unwrap();
        idx.flush_namespace(&child).unwrap();
        let filter: FilterClause = serde_json::from_str(r#"{"term": {"id": "d1"}}"#).unwrap();
        let m = idx.manifest(&child).unwrap();
        assert_eq!(idx.delete_by_query(&child, &m, &filter).unwrap(), 1);
        idx.replace_documents(child.clone(), vec![doc("d3", "THREE")])
            .unwrap();
        idx.flush_namespace(&child).unwrap();
        let published_child = publish(&idx, &child);
        for (segment, vector) in &published_child.deletion_vectors {
            if !vector.is_inherited(&child) {
                let path = vector.path(&child, segment).unwrap();
                assert!(path.starts_with("t/test/"), "{path}");
            }
        }
        for entry in &published_child.segments {
            let home = published_child.segment_home(&child, &entry.segment_id);
            assert!(
                home == &parent || entry.segment_id.0.starts_with("t_test-"),
                "{entry:?} in {home:?}"
            );
        }

        // Full compaction merges inherited and own segments into the
        // branch's prefix.
        let result = idx
            .compact_namespace_with_options(&child, CompactOptions::full())
            .unwrap();
        assert!(result.merged);
        let compacted = publish(&idx, &child);
        assert_eq!(compacted.segments.len(), 1);
        assert!(!compacted.is_inherited_segment(&child, &compacted.segments[0].segment_id));
        assert!(
            compacted.branch.as_ref().unwrap().segment_homes.is_empty(),
            "homes of compacted-away segments are pruned"
        );
        let expected_child = vec![
            ("d3".to_string(), "THREE".to_string()),
            ("d4".to_string(), "four".to_string()),
            ("d5".to_string(), "five".to_string()),
        ];
        assert_eq!(
            live_ids(&dir, &child, &idx.manifest(&child).unwrap()),
            expected_child
        );

        // The parent: same bytes, same manifest, same documents.
        assert_eq!(tree_bytes(&dir.join(&parent.0)), parent_files);
        assert!(idx
            .manifest(&parent)
            .unwrap()
            .same_contents(&parent_manifest));
        assert_eq!(
            live_ids(&dir, &parent, &idx.manifest(&parent).unwrap()),
            vec![
                ("d1".to_string(), "one".to_string()),
                ("d3".to_string(), "three".to_string()),
                ("d4".to_string(), "four".to_string()),
            ]
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// #347: the purge reads a namespace under the writer's locks.
    #[test]
    fn while_quiesced_reports_unknown_published_and_unpublished_namespaces() {
        let dir = std::env::temp_dir().join(format!("kosha-quiesced-{}", uuid::Uuid::new_v4()));
        let idx = Indexer::new(dir.clone());
        let ns = NamespaceId("t/quiesced".into());
        assert!(idx.while_quiesced(&ns, |local| local.is_none()));
        // Asking does not make the namespace known.
        assert!(idx.manifest_version(&ns).is_none());
        assert!(!idx.namespaces().contains(&ns));

        idx.restore_manifest(ns.clone(), Manifest::empty(4));
        let local = idx.while_quiesced(&ns, |local| local).unwrap();
        assert_eq!(local.manifest.version, 4);
        assert!(!local.unpublished, "a followed manifest is published");

        let doc = Document {
            id: DocumentId("a".into()),
            fields: vec![Field::text("text", "hello")],
        };
        idx.index_documents(ns.clone(), vec![doc]).unwrap();
        assert!(
            idx.while_quiesced(&ns, |local| local).unwrap().unpublished,
            "a buffered write"
        );
        idx.flush_namespace(&ns).unwrap();
        let local = idx.while_quiesced(&ns, |local| local).unwrap();
        assert!(local.unpublished, "a flushed, unpublished segment");
        assert_eq!(local.manifest.segments.len(), 1);
        publish(&idx, &ns);
        assert!(!idx.while_quiesced(&ns, |local| local).unwrap().unpublished);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn drop_namespace_refuses_unpublished_writes_and_forgets_published_ones() {
        let dir = std::env::temp_dir().join(format!("kosha-drop-{}", uuid::Uuid::new_v4()));
        let ns = NamespaceId("t/branch".into());
        let idx = Indexer::new(dir.clone());
        let doc = Document {
            id: DocumentId("a".into()),
            fields: vec![Field::text("text", "hello")],
        };
        idx.index_documents(ns.clone(), vec![doc.clone()]).unwrap();
        let mut ran = false;
        let refused = idx.drop_branch_namespace(&ns, || {
            ran = true;
            Ok(())
        });
        assert!(matches!(refused, Err(KoshaError::InvalidQuery(_))));
        assert!(!ran, "nothing is removed while writes are unpublished");
        idx.flush_namespace(&ns).unwrap();
        publish(&idx, &ns);
        // A failing removal leaves the namespace as it was.
        assert!(idx
            .drop_branch_namespace(&ns, || Err(KoshaError::ControlPlane("down".into())))
            .is_err());
        assert!(idx.manifest(&ns).is_some());
        idx.drop_branch_namespace(&ns, || Ok(())).unwrap();
        assert!(idx.manifest(&ns).is_none());
        assert!(!idx.namespaces().contains(&ns));
        // A restart has nothing to replay: the WAL was cleared on publish.
        drop(idx);
        let restarted = Indexer::new(dir.clone());
        assert_eq!(restarted.pending_doc_count(&ns), 0);
        std::fs::remove_dir_all(dir).unwrap();
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

    /// #347: retrying a deleted namespace's cleanup must never touch a new
    /// incarnation that was written in the meantime.
    #[test]
    fn discard_if_untouched_runs_only_while_nothing_was_written_since_the_drop() {
        let dir = std::env::temp_dir().join(format!("kosha-discard-{}", uuid::Uuid::new_v4()));
        let ns = NamespaceId("t/gone".into());
        let doc = |id: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", "hello")],
        };
        let idx = Indexer::new(dir.clone());
        idx.index_documents(ns.clone(), vec![doc("a")]).unwrap();
        idx.flush_namespace(&ns).unwrap();
        let published = idx.manifest(&ns).unwrap().version;

        // Live namespace: refused, the callback never runs.
        let mut ran = 0;
        assert_eq!(idx.discard_if_untouched(&ns, || ran += 1).unwrap(), None);
        assert_eq!(ran, 0);
        assert!(idx.manifest(&ns).is_some());

        // Dropped and not written since: the retry goes through, twice.
        idx.drop_namespace(&ns, published + 1, || {}).unwrap();
        assert_eq!(idx.discard_if_untouched(&ns, || ran += 1).unwrap(), Some(0));
        assert_eq!(idx.discard_if_untouched(&ns, || ran += 1).unwrap(), Some(0));
        assert_eq!(ran, 2);
        assert!(idx.manifest(&ns).is_none());

        // A buffered write to the new incarnation: refused, and the write
        // survives, in memory and in the WAL.
        idx.index_documents(ns.clone(), vec![doc("b")]).unwrap();
        assert_eq!(idx.discard_if_untouched(&ns, || ran += 1).unwrap(), None);
        assert_eq!(ran, 2);
        assert_eq!(idx.pending_doc_count(&ns), 1);
        drop(idx);
        let restarted = Indexer::new(dir.clone());
        assert!(restarted.pending_doc_count(&ns) > 0, "WAL record kept");
        // Recovered records count as written too.
        assert_eq!(
            restarted.discard_if_untouched(&ns, || ran += 1).unwrap(),
            None
        );
        assert_eq!(ran, 2);
        drop(restarted);
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

    /// #255: the unindexed tail is the buffer, relative to the local
    /// manifest version, with a fingerprint that changes with it.
    #[test]
    fn unindexed_tail_follows_buffer_and_flushes() {
        let dir = std::env::temp_dir().join(format!("kosha-tail-buffer-{}", uuid::Uuid::new_v4()));
        let idx = Indexer::new(dir.clone()).with_flush_threshold(100);
        let ns = NamespaceId("ns".into());
        let doc = |id: &str, text: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", text)],
        };
        assert!(idx.unindexed_tail(&ns, None, None).unwrap().is_none());
        idx.index_documents(ns.clone(), vec![doc("a", "one"), doc("b", "two")])
            .unwrap();
        idx.index_documents(ns.clone(), vec![doc("a", "three")])
            .unwrap();
        let tail = idx.unindexed_tail(&ns, None, None).unwrap().unwrap();
        assert_eq!(tail.manifest_version, 0);
        let ids: Vec<_> = tail.documents.iter().map(|d| d.id.0.as_str()).collect();
        assert_eq!(ids, ["b", "a"], "newest version of each id, oldest first");
        assert_eq!(tail.documents[1].fields[0].value, "three");
        assert!(tail.masked.is_empty() && tail.deleted_ids.is_empty());
        assert_eq!(idx.unindexed_usage(&ns).0, 2);
        assert!(idx.unindexed_usage(&ns).1 > 0);

        // Same state: the caller's copy is still current.
        let same = idx
            .unindexed_tail(&ns, Some(0), Some(&tail.fingerprint))
            .unwrap()
            .unwrap();
        assert!(same.not_modified && same.documents.is_empty());
        // Another manifest version: only a stale marker.
        let stale = idx.unindexed_tail(&ns, Some(7), None).unwrap().unwrap();
        assert!(stale.stale && stale.documents.is_empty());
        assert_eq!(stale.manifest_version, 0);

        idx.index_documents(ns.clone(), vec![doc("c", "four")])
            .unwrap();
        let grown = idx.unindexed_tail(&ns, None, None).unwrap().unwrap();
        assert_ne!(grown.fingerprint, tail.fingerprint);
        assert_eq!(grown.documents.len(), 3);

        idx.flush_namespace(&ns).unwrap();
        let flushed = idx.unindexed_tail(&ns, None, None).unwrap().unwrap();
        assert!(flushed.is_empty());
        assert_eq!(flushed.manifest_version, idx.manifest_version(&ns).unwrap());
        assert_eq!(idx.unindexed_usage(&ns), (0, 0));
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// #255: WAL-recovered writes awaiting replay are in the tail, with
    /// masks for the indexed copies they replace or delete.
    #[test]
    fn unindexed_tail_masks_replayed_upserts_and_deletes() {
        let dir = std::env::temp_dir().join(format!("kosha-tail-replay-{}", uuid::Uuid::new_v4()));
        let ns = NamespaceId("ns".into());
        let doc = |id: &str, text: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![
                Field::text("text", text),
                Field::keyword("status", "remove"),
            ],
        };
        let idx = Indexer::new(dir.clone());
        idx.index_documents(ns.clone(), vec![doc("same", "old"), doc("gone", "x")])
            .unwrap();
        idx.flush_namespace(&ns).unwrap();
        let committed = idx.manifest(&ns).unwrap();
        idx.acknowledge_manifest(&ns, &committed).unwrap();
        // Crash after WAL appends that never reached a segment.
        idx.wal.append(&ns, &[doc("same", "new")]).unwrap();
        idx.wal
            .append_delete(&ns, vec![DocumentId("gone".into())])
            .unwrap();
        drop(idx);
        let restart = Indexer::new(dir.clone());
        restart.restore_manifest(ns.clone(), committed.clone());
        let tail = restart.unindexed_tail(&ns, None, None).unwrap().unwrap();
        assert_eq!(tail.manifest_version, committed.version);
        assert_eq!(tail.documents.len(), 1);
        assert_eq!(tail.documents[0].fields[0].value, "new");
        assert_eq!(tail.deleted_ids, vec![DocumentId("gone".into())]);
        let segment = &committed.segments[0].segment_id;
        assert_eq!(tail.masked.get(segment), Some(&vec![0, 1]));
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

    /// Research #388: a namespace mixing raw and compressed doc stores
    /// compacts into a compressed one (with `KOSHA_DOC_STORE_COMPRESSION`
    /// unset, a merge keeps its inputs' format), every record and sidecar
    /// vector survives bit-identical, and the output keeps vectors out of
    /// the doc store.
    #[test]
    fn compaction_of_mixed_doc_store_formats_keeps_compression() {
        if kosha_segment::DocStoreCompressionMode::from_env()
            != kosha_segment::DocStoreCompressionMode::Auto
        {
            eprintln!("skipped: KOSHA_DOC_STORE_COMPRESSION is set in this environment");
            return;
        }
        let dir = std::env::temp_dir().join(format!("kosha-compact-zstd-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let idx = Indexer::new(dir.clone());
        let ns = NamespaceId("z".into());
        let vector = |i: usize| -> Vec<f32> { vec![i as f32 + 0.25, -(i as f32), 0.5, 1.0e-3] };
        let text = |i: usize| format!("passage {i} about compressed document stores and zstd");
        for seg in 0..3 {
            let docs = (0..40)
                .map(|j| {
                    let i = seg * 40 + j;
                    Document {
                        id: DocumentId(format!("d{i}")),
                        fields: vec![
                            Field::text("text", text(i)),
                            Field {
                                name: "emb".into(),
                                field_type: kosha_core::FieldType::Vector,
                                value: serde_json::to_string(&vector(i)).unwrap(),
                            },
                        ],
                    }
                })
                .collect();
            idx.index_documents(ns.clone(), docs).unwrap();
            idx.flush_namespace(&ns).unwrap();
        }
        let before = idx.manifest(&ns).unwrap();
        assert_eq!(before.segments.len(), 3);

        // Rewrite the first segment in place with a compressed doc store:
        // same documents in the same order, so every other file agrees.
        let first = before.segments[0].segment_id.clone();
        let first_dir = dir.join(&ns.0).join(&first.0);
        let rebuilt = dir.join("rebuilt");
        {
            let reader = kosha_segment::SegmentReader::open(first_dir.clone()).unwrap();
            assert!(!reader.doc_store_compressed());
            let mut w = kosha_segment::SegmentWriter::new(first.clone(), rebuilt.clone())
                .with_doc_store_zstd(Some(kosha_segment::DocStoreZstdConfig::default()));
            for raw in reader.iter_doc_records_raw() {
                let raw = raw.unwrap();
                w.add_document_with_vectors(raw.record.doc_id, raw.record.fields, raw.vectors);
            }
            let footer = w.finalize(kosha_core::Bm25Params::default()).unwrap();
            assert!(footer.doc_store_compression.is_some());
        }
        for entry in std::fs::read_dir(&rebuilt).unwrap().flatten() {
            std::fs::copy(entry.path(), first_dir.join(entry.file_name())).unwrap();
        }
        let mixed: Vec<bool> = before
            .segments
            .iter()
            .map(|e| {
                kosha_segment::SegmentReader::open(dir.join(&ns.0).join(&e.segment_id.0))
                    .unwrap()
                    .doc_store_compressed()
            })
            .collect();
        assert_eq!(mixed, vec![true, false, false], "a mixed-format namespace");

        let result = idx
            .compact_namespace_with_options(&ns, CompactOptions::full())
            .unwrap();
        assert!(result.merged);
        let after = idx.manifest(&ns).unwrap();
        assert_eq!(after.segments.len(), 1);
        let seg_dir = dir.join(&ns.0).join(&after.segments[0].segment_id.0);
        let reader = kosha_segment::SegmentReader::open(seg_dir).unwrap();
        assert!(reader.doc_store_compressed(), "compaction kept compression");
        assert!(reader.footer().doc_vectors, "vectors stay in the sidecar");
        let mut seen = 0;
        for record in reader.iter_doc_records() {
            let record = record.unwrap();
            let i: usize = record.doc_id.0[1..].parse().unwrap();
            let t = record.fields.iter().find(|f| f.name == "text").unwrap();
            assert_eq!(t.value, text(i));
            let emb = record.fields.iter().find(|f| f.name == "emb").unwrap();
            let got: Vec<f32> = serde_json::from_str(&emb.value).unwrap();
            let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
            assert_eq!(bits(&got), bits(&vector(i)), "doc {i}");
            seen += 1;
        }
        assert_eq!(seen, 120);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// #253: `lire` compaction seeds the merged vector index from the
    /// inputs' postings (remapped to merged doc_seqs, tombstoned members
    /// dropped); `rebuild` clusters from scratch. Both stay searchable by
    /// the current reader and hold every live vector exactly once.
    #[test]
    fn lire_compaction_reuses_input_postings_and_remaps_doc_seqs() {
        use kosha_segment::VectorCompactionMode;
        fn vector(i: u64) -> Vec<f32> {
            let mut s = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut next = || {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 40) as f32 / (1u64 << 23) as f32 - 1.0
            };
            let c = (i % 16) as f32;
            (0..16)
                .map(|d| if d as f32 == c { 3.0 } else { 0.0 } + next() * 0.5)
                .collect()
        }
        fn layout(
            dir: &std::path::Path,
            ns: &NamespaceId,
            seg: &SegmentId,
        ) -> Vec<(Vec<f32>, Vec<u32>)> {
            let reader = kosha_segment::SegmentReader::open(dir.join(&ns.0).join(&seg.0)).unwrap();
            reader
                .vector_posting_layout("emb")
                .unwrap()
                .unwrap()
                .postings
                .into_iter()
                .map(|p| (p.centroid, p.doc_seqs))
                .collect()
        }
        for mode in [VectorCompactionMode::Lire, VectorCompactionMode::Rebuild] {
            let dir = std::env::temp_dir().join(format!(
                "kosha-lire-compact-{mode:?}-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            // One 2,000-doc input: big enough to have its postings adopted.
            let idx = Indexer::new(dir.clone())
                .with_flush_threshold(10_000)
                .with_max_concurrent_flush_writers(1);
            let ns = NamespaceId("lire".into());
            let doc = |id: u64, v: Vec<f32>| Document {
                id: DocumentId(format!("d{id}")),
                fields: vec![
                    Field::text("text", format!("doc {id}")),
                    Field::vector("emb", v),
                ],
            };
            idx.index_documents(ns.clone(), (0..2_000).map(|i| doc(i, vector(i))).collect())
                .unwrap();
            idx.flush_namespace(&ns).unwrap();
            let big = idx.manifest(&ns).unwrap().segments[0].segment_id.clone();
            let input = layout(&dir, &ns, &big);
            // Update 4 docs (tombstones in `big`) with new vectors: at most
            // 8 of the input's postings change.
            let updated: Vec<u64> = (0..4).map(|k| k * 397).collect();
            idx.index_documents(
                ns.clone(),
                updated
                    .iter()
                    .map(|&i| doc(i, vector(10_000 + i)))
                    .collect(),
            )
            .unwrap();
            idx.flush_namespace(&ns).unwrap();

            let result = idx
                .compact_namespace_with_options(&ns, CompactOptions::full().with_vector_index(mode))
                .unwrap();
            assert!(result.merged);
            let manifest = idx.manifest(&ns).unwrap();
            assert_eq!(manifest.segments.len(), 1);
            let merged_id = manifest.segments[0].segment_id.clone();
            let merged = layout(&dir, &ns, &merged_id);

            let mut seqs: Vec<u32> = merged.iter().flat_map(|(_, s)| s.iter().copied()).collect();
            seqs.sort_unstable();
            assert_eq!(seqs, (0..2_000).collect::<Vec<u32>>(), "{mode:?}");

            // Re-normalizing a stored (unit) centroid may move its last bit.
            let same = |a: &[f32], b: &[f32]| a.iter().zip(b).all(|(x, y)| (x - y).abs() < 1e-6);
            let reused = input
                .iter()
                .filter(|(c, _)| merged.iter().any(|(m, _)| same(m, c)))
                .count();
            match mode {
                VectorCompactionMode::Lire => assert!(
                    reused * 2 >= input.len(),
                    "lire reused only {reused} of {} input postings",
                    input.len()
                ),
                // A from-scratch build re-clusters everything. Its
                // refinement (#253) converges to cluster means, so a posting
                // that is exactly one of these 16 well-separated clusters
                // in both builds can come out with the same centroid; the
                // postings the updates touched cannot.
                VectorCompactionMode::Rebuild => assert!(
                    reused < input.len(),
                    "rebuild kept all {reused} input postings"
                ),
            }

            // doc_seqs were remapped right: each doc's (current) vector
            // finds that doc.
            let reader =
                kosha_segment::SegmentReader::open(dir.join(&ns.0).join(&merged_id.0)).unwrap();
            for i in (0..2_000u64).step_by(41).chain(updated.iter().copied()) {
                let v = if updated.contains(&i) {
                    vector(10_000 + i)
                } else {
                    vector(i)
                };
                let hits = reader.knn_search(&v, 1, 1_000).unwrap().unwrap();
                let rec = reader.doc_record_full(hits[0].0).unwrap().unwrap();
                assert_eq!(rec.doc_id.0, format!("d{i}"), "{mode:?}");
            }
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    /// #204: each vector column keeps its own index across a compaction,
    /// which re-adds every document through `SegmentWriter`.
    #[test]
    fn compaction_preserves_vector_columns() {
        let dir =
            std::env::temp_dir().join(format!("kosha-compact-columns-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let idx = Indexer::new(dir.clone());
        let ns = NamespaceId("cols".into());
        let title = |i: usize| vec![i as f32 + 1.0, 0.5, -(i as f32), 0.25];
        let body = |i: usize| -> Vec<f32> { (0..8).map(|d| (d * i) as f32 * 0.1 + 0.3).collect() };
        for i in 0..4 {
            let mut fields = vec![Field::text("text", format!("doc {i}"))];
            if i != 1 {
                fields.push(Field::vector("title_emb", title(i)));
            }
            if i != 2 {
                fields.push(Field::vector("body_emb", body(i)));
            }
            idx.index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId(format!("d{i}")),
                    fields,
                }],
            )
            .unwrap();
            idx.flush_namespace(&ns).unwrap();
        }
        let result = idx
            .compact_namespace_with_options(&ns, CompactOptions::full())
            .unwrap();
        assert!(result.merged);
        let after = idx.manifest(&ns).unwrap();
        assert_eq!(after.segments.len(), 1);
        let seg_dir = dir.join(&ns.0).join(&after.segments[0].segment_id.0);
        let reader = kosha_segment::SegmentReader::open(seg_dir).unwrap();
        let columns: Vec<(String, u32, u32)> = reader
            .footer()
            .vector_fields
            .clone()
            .unwrap()
            .into_iter()
            .map(|m| (m.name, m.dim, m.count))
            .collect();
        assert_eq!(
            columns,
            vec![("title_emb".into(), 4, 3), ("body_emb".into(), 8, 3)]
        );
        for (field, query, missing) in [("title_emb", title(3), "d1"), ("body_emb", body(3), "d2")]
        {
            let hits = reader
                .knn_search_filtered(field, &query, 10, 100, None)
                .unwrap()
                .unwrap();
            let ids: Vec<String> = hits
                .iter()
                .map(|(seq, _)| reader.doc_meta(*seq).unwrap().doc_id.to_owned())
                .collect();
            assert_eq!(ids.len(), 3, "{field}: {ids:?}");
            assert_eq!(ids[0], "d3", "{field}: exact match first");
            assert!(!ids.iter().any(|id| id == missing), "{field}: {ids:?}");
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Finding 29: a query-path hydration leaves a segment on disk without
    /// its vector index files (`vector*.idx` / `vector*.offsets` are only
    /// fetched when a caller needs vectors). If compaction then merges that
    /// segment, the vectors must survive: they are read from the doc store
    /// / `doc_vectors.bin`, never from the index, and the merged index is
    /// rebuilt from them. Both vector-index modes are checked.
    ///
    /// The second half pins the loud failure: an input whose
    /// `doc_vectors.bin` is missing aborts the compaction with an error and
    /// leaves the manifest untouched, rather than writing a merged segment
    /// without the vectors.
    #[test]
    fn compaction_of_inputs_without_local_vector_index_keeps_every_vector() {
        use kosha_segment::VectorCompactionMode;
        fn vector(i: u64) -> Vec<f32> {
            let mut s = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut next = || {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 40) as f32 / (1u64 << 23) as f32 - 1.0
            };
            let c = (i % 16) as f32;
            (0..16)
                .map(|d| if d as f32 == c { 3.0 } else { 0.0 } + next() * 0.5)
                .collect()
        }
        let doc = |id: u64| Document {
            id: DocumentId(format!("d{id}")),
            fields: vec![
                Field::text("text", format!("doc {id}")),
                Field::vector("emb", vector(id)),
            ],
        };
        // Two inputs; the first is big enough (>= LIRE_ADOPT_MIN_VECTORS)
        // that `lire` would try to seed from its (missing) postings.
        let build = |dir: &std::path::Path| {
            let idx = Indexer::new(dir.to_path_buf())
                .with_flush_threshold(10_000)
                .with_max_concurrent_flush_writers(1);
            let ns = NamespaceId("vecidx".into());
            idx.index_documents(ns.clone(), (0..1_200).map(doc).collect())
                .unwrap();
            idx.flush_namespace(&ns).unwrap();
            idx.index_documents(ns.clone(), (1_200..1_250).map(doc).collect())
                .unwrap();
            idx.flush_namespace(&ns).unwrap();
            let manifest = idx.manifest(&ns).unwrap();
            assert_eq!(manifest.segments.len(), 2);
            let seg_dirs: Vec<std::path::PathBuf> = manifest
                .segments
                .iter()
                .map(|e| dir.join(&ns.0).join(&e.segment_id.0))
                .collect();
            (idx, ns, seg_dirs)
        };
        let remove_matching = |seg_dir: &std::path::Path, pred: &dyn Fn(&str) -> bool| {
            let mut removed = 0;
            for entry in std::fs::read_dir(seg_dir).unwrap().flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if pred(&name) {
                    std::fs::remove_file(entry.path()).unwrap();
                    removed += 1;
                }
            }
            removed
        };

        for mode in [VectorCompactionMode::Lire, VectorCompactionMode::Rebuild] {
            let dir = std::env::temp_dir().join(format!(
                "kosha-compact-no-vecidx-{mode:?}-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let (idx, ns, seg_dirs) = build(&dir);
            for seg_dir in &seg_dirs {
                assert!(
                    remove_matching(seg_dir, &kosha_core::is_vector_index_file) > 0,
                    "{mode:?}: input had vector index files to drop"
                );
            }

            let result = idx
                .compact_namespace_with_options(&ns, CompactOptions::full().with_vector_index(mode))
                .unwrap();
            assert!(result.merged, "{mode:?}");
            let manifest = idx.manifest(&ns).unwrap();
            assert_eq!(manifest.segments.len(), 1);
            let reader = kosha_segment::SegmentReader::open(
                dir.join(&ns.0).join(&manifest.segments[0].segment_id.0),
            )
            .unwrap();
            let columns: Vec<(String, u32)> = reader
                .footer()
                .vector_fields
                .clone()
                .unwrap()
                .into_iter()
                .map(|m| (m.name, m.count))
                .collect();
            assert_eq!(columns, vec![("emb".into(), 1_250)], "{mode:?}");
            for i in (0..1_250u64).step_by(37) {
                let hits = reader.knn_search(&vector(i), 1, 1_000).unwrap().unwrap();
                let rec = reader.doc_record_full(hits[0].0).unwrap().unwrap();
                assert_eq!(rec.doc_id.0, format!("d{i}"), "{mode:?}");
                let emb = rec.fields.iter().find(|f| f.name == "emb").unwrap();
                let got: Vec<f32> = serde_json::from_str(&emb.value).unwrap();
                let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
                assert_eq!(bits(&got), bits(&vector(i)), "{mode:?} doc {i}");
            }
            std::fs::remove_dir_all(dir).unwrap();
        }

        // Vectors stored in a sidecar that is not local: refuse to merge.
        let dir =
            std::env::temp_dir().join(format!("kosha-compact-no-docvec-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (idx, ns, seg_dirs) = build(&dir);
        let before = idx.manifest(&ns).unwrap();
        assert!(before
            .segments
            .iter()
            .all(|e| before.segment_footer(&e.segment_id).unwrap().doc_vectors));
        assert_eq!(
            remove_matching(&seg_dirs[1], &|n| n == kosha_segment::DOC_VECTORS_FILE),
            1
        );
        let err = idx
            .compact_namespace_with_options(&ns, CompactOptions::full())
            .expect_err("a merge without the input's vectors must fail");
        assert!(
            err.to_string().contains(kosha_segment::DOC_VECTORS_FILE),
            "{err}"
        );
        let after = idx.manifest(&ns).unwrap();
        assert_eq!(after.version, before.version, "manifest unchanged");
        let ids = |m: &Manifest| -> Vec<SegmentId> {
            m.segments.iter().map(|e| e.segment_id.clone()).collect()
        };
        assert_eq!(ids(&after), ids(&before));
        let dirs = std::fs::read_dir(dir.join(&ns.0))
            .unwrap()
            .flatten()
            .filter(|e| e.path().is_dir())
            .count();
        assert_eq!(dirs, 2, "no merged output left on disk");
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// #204: a vector whose dimension differs from its field's is rejected
    /// at the write API, whether the field's dimension was fixed by a
    /// flushed segment, by buffered documents, or earlier in the batch.
    #[test]
    fn vector_of_the_wrong_dimension_is_rejected_at_write() {
        let dir = std::env::temp_dir().join(format!("kosha-vector-dims-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("dims".into());
        let doc = |id: &str, field: &str, dim: usize| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::vector(field, vec![0.5; dim])],
        };
        let rejected = |r: Result<usize, KoshaError>| matches!(r, Err(KoshaError::InvalidQuery(ref m)) if m.contains("dimensions"));

        let idx = Indexer::new(dir.clone());
        idx.index_documents(ns.clone(), vec![doc("a", "emb", 4)])
            .unwrap();
        // Fixed by the buffered document.
        assert!(rejected(
            idx.index_documents(ns.clone(), vec![doc("b", "emb", 3)])
        ));
        // Within one batch.
        assert!(rejected(idx.index_documents(
            ns.clone(),
            vec![doc("c", "other", 2), doc("d", "other", 5)]
        )));
        // A different field has its own dimension.
        idx.index_documents(ns.clone(), vec![doc("e", "other", 8)])
            .unwrap();
        idx.flush_namespace(&ns).unwrap();
        assert!(rejected(
            idx.replace_documents(ns.clone(), vec![doc("a", "emb", 2)])
        ));

        // Fixed by a flushed segment's footer, for a fresh process.
        let manifest = idx.manifest(&ns).unwrap();
        let restarted = Indexer::new(dir.clone());
        restarted.restore_manifest(ns.clone(), manifest);
        assert!(rejected(
            restarted.index_documents(ns.clone(), vec![doc("f", "other", 4)])
        ));
        restarted
            .index_documents(ns.clone(), vec![doc("g", "other", 8), doc("h", "emb", 4)])
            .unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// #205: a late-interaction bag is held to its field's token
    /// dimension at write time like a flat vector, and a ragged bag is
    /// rejected. A wrong-dimension bag used to be accepted and, first in a
    /// flush, made that segment drop every correct bag from its index.
    #[test]
    fn late_interaction_bag_of_the_wrong_dimension_is_rejected_at_write() {
        let dir = std::env::temp_dir().join(format!("kosha-bag-dims-{}", uuid::Uuid::new_v4()));
        let ns = NamespaceId("bags".into());
        let bag = |id: &str, field: &str, dims: &[usize]| Document {
            id: DocumentId(id.into()),
            fields: vec![Field {
                name: field.into(),
                field_type: kosha_core::FieldType::Vector,
                value: serde_json::to_string(
                    &dims.iter().map(|&d| vec![0.5f32; d]).collect::<Vec<_>>(),
                )
                .unwrap(),
            }],
        };
        let rejected = |r: Result<usize, KoshaError>, what: &str| matches!(r, Err(KoshaError::InvalidQuery(ref m)) if m.contains(what));

        let idx = Indexer::new(dir.clone());
        idx.index_documents(ns.clone(), vec![bag("a", "tokens", &[4, 4, 4])])
            .unwrap();
        // Fixed by the buffered bag; within one batch; ragged.
        assert!(rejected(
            idx.index_documents(ns.clone(), vec![bag("b", "tokens", &[3, 3])]),
            "token vectors"
        ));
        assert!(rejected(
            idx.index_documents(
                ns.clone(),
                vec![bag("c", "other", &[2]), bag("d", "other", &[5])]
            ),
            "token vectors"
        ));
        assert!(rejected(
            idx.index_documents(ns.clone(), vec![bag("e", "tokens", &[4, 3])]),
            "different dimensions"
        ));
        // A flat vector field of the same name is tracked apart from bags.
        idx.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("f".into()),
                fields: vec![Field::vector("emb", vec![0.5; 6])],
            }],
        )
        .unwrap();
        idx.flush_namespace(&ns).unwrap();

        // Fixed by a flushed segment's footer, for a fresh process.
        let manifest = idx.manifest(&ns).unwrap();
        let restarted = Indexer::new(dir.clone());
        restarted.restore_manifest(ns.clone(), manifest);
        assert!(rejected(
            restarted.index_documents(ns.clone(), vec![bag("g", "tokens", &[8])]),
            "token vectors"
        ));
        restarted
            .index_documents(ns.clone(), vec![bag("h", "tokens", &[4, 4])])
            .unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn failed_wal_reclamation_after_a_committed_publish_is_not_an_error() {
        use crate::object_wal::{MemoryObjectBackend, ObjectWalStore};
        let backend = MemoryObjectBackend::new();
        let store: Arc<dyn wal::WalStore> =
            Arc::new(ObjectWalStore::new(Arc::new(backend.clone()), "ingest-0").unwrap());
        let dir = std::env::temp_dir().join(format!("kosha-ack-reclaim-{}", uuid::Uuid::new_v4()));
        let ns = NamespaceId("t/ns".into());
        let idx = Indexer::with_wal_store(dir.clone(), store).unwrap();
        idx.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId("a".into()),
                fields: vec![Field::text("text", "durable")],
            }],
        )
        .unwrap();
        idx.flush_namespace(&ns).unwrap();
        let published = idx.manifest(&ns).unwrap();
        // Reclamation first persists the `sequence` high-water mark; make
        // that object-store put fail.
        backend.fail_next_puts(1);
        idx.acknowledge_manifest(&ns, &published).unwrap();
        assert!(
            backend.keys().iter().any(|k| k.ends_with(".bin")),
            "the WAL object is kept for a later reclamation"
        );
        // The next publish reclaims it.
        idx.acknowledge_manifest(&ns, &published).unwrap();
        assert!(!backend.keys().iter().any(|k| k.ends_with(".bin")));
        let _ = std::fs::remove_dir_all(&dir);
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
        let store = reader.try_filter_store().unwrap();
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

    /// #385: with BP reordering on, the merged output's doc_seqs are not in
    /// input order, so a delete landing mid-merge must still be carried
    /// forward onto the right merged doc_seqs (the same `seq_remap` the
    /// reordered adds used), and pre-merge tombstones must still exclude
    /// their documents.
    #[test]
    fn bp_compaction_remaps_tombstones_to_reordered_doc_seqs() {
        let dir = std::env::temp_dir().join(format!(
            "kosha-test-compact-bp-tombstones-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let idx = Indexer::new(dir.clone())
            .with_flush_threshold(40)
            .with_doc_reorder(kosha_segment::DocReorder::Bp)
            .with_doc_reorder_compaction_max_docs(u64::MAX);
        // Two topics, interleaved, so BP really moves documents.
        let docs: Vec<Document> = (0..120)
            .map(|i| Document {
                id: DocumentId(format!("d{i:03}")),
                fields: vec![
                    Field::text(
                        "title",
                        if i % 2 == 0 {
                            "alpha beta gamma delta"
                        } else {
                            "omega sigma kappa lambda"
                        },
                    ),
                    Field::keyword("bucket", format!("b{}", i % 7)),
                ],
            })
            .collect();
        let mut docs = docs.into_iter();
        for _ in 0..3 {
            idx.index_documents(ns.clone(), docs.by_ref().take(40).collect())
                .unwrap();
            idx.flush_namespace(&ns).unwrap();
        }
        assert_eq!(idx.manifest(&ns).unwrap().segments.len(), 3);
        let m = idx.manifest(&ns).unwrap();
        let pre: FilterClause = serde_json::from_str(r#"{"term": {"bucket": "b0"}}"#).unwrap();
        assert_eq!(idx.delete_by_query(&ns, &m, &pre).unwrap(), 18);

        let mid: FilterClause = serde_json::from_str(r#"{"term": {"bucket": "b3"}}"#).unwrap();
        let ns_hook = ns.clone();
        let result = idx
            .compact_namespace_with_options_impl(&ns, CompactOptions::full(), || {
                assert_eq!(idx.delete_by_query(&ns_hook, &m, &mid).unwrap(), 17);
            })
            .unwrap();
        assert!(result.merged);

        let m = idx.manifest(&ns).unwrap();
        assert_eq!(m.segments.len(), 1);
        let seg_id = m.segments[0].segment_id.clone();
        let reader = kosha_segment::SegmentReader::open(dir.join("test").join(&seg_id.0)).unwrap();
        assert_eq!(
            reader.doc_count(),
            120 - 18,
            "pre-merge deletes are dropped"
        );
        let ids: Vec<String> = (0..reader.doc_count())
            .map(|s| reader.doc_record_full(s).unwrap().unwrap().doc_id.0)
            .collect();
        let input_order: Vec<String> = (0..120)
            .filter(|i| i % 7 != 0)
            .map(|i| format!("d{i:03}"))
            .collect();
        assert_ne!(ids, input_order, "BP should have reordered the documents");
        let tombstones = idx.get_tombstones(&ns).unwrap();
        let mut deleted: Vec<String> = tombstones[&seg_id]
            .iter()
            .map(|&s| ids[s as usize].clone())
            .collect();
        deleted.sort();
        let want: Vec<String> = (0..120)
            .filter(|i| i % 7 == 3)
            .map(|i| format!("d{i:03}"))
            .collect();
        assert_eq!(deleted, want);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// #385: BP compaction buffers the whole merged output, so it only runs
    /// when the planned live doc count (input docs minus pre-merge
    /// tombstones) is within `KOSHA_DOC_REORDER_COMPACTION_MAX_DOCS`; above
    /// it, and by default (cap 0), compaction streams in input order.
    #[test]
    fn bp_compaction_falls_back_to_input_order_above_the_doc_cap() {
        let run = |cap: Option<u64>| -> Vec<String> {
            let dir = std::env::temp_dir().join(format!(
                "kosha-test-compact-bp-cap-{}-{}",
                cap.map_or("default".to_string(), |c| c.to_string()),
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let ns = NamespaceId("test".into());
            let mut idx = Indexer::new(dir.clone())
                .with_flush_threshold(40)
                .with_doc_reorder(kosha_segment::DocReorder::Bp);
            if let Some(cap) = cap {
                idx = idx.with_doc_reorder_compaction_max_docs(cap);
            }
            let mut docs = (0..120).map(|i| Document {
                id: DocumentId(format!("d{i:03}")),
                fields: vec![
                    Field::text(
                        "title",
                        if i % 2 == 0 {
                            "alpha beta gamma delta"
                        } else {
                            "omega sigma kappa lambda"
                        },
                    ),
                    Field::keyword("bucket", format!("b{}", i % 7)),
                ],
            });
            for _ in 0..3 {
                idx.index_documents(ns.clone(), docs.by_ref().take(40).collect())
                    .unwrap();
                idx.flush_namespace(&ns).unwrap();
            }
            let m = idx.manifest(&ns).unwrap();
            let pre: FilterClause = serde_json::from_str(r#"{"term": {"bucket": "b0"}}"#).unwrap();
            assert_eq!(idx.delete_by_query(&ns, &m, &pre).unwrap(), 18);
            assert!(
                idx.compact_namespace_with_options_impl(&ns, CompactOptions::full(), || {})
                    .unwrap()
                    .merged
            );
            let m = idx.manifest(&ns).unwrap();
            let seg = &m.segments[0].segment_id;
            let reader = kosha_segment::SegmentReader::open(dir.join("test").join(&seg.0)).unwrap();
            let ids = (0..reader.doc_count())
                .map(|s| reader.doc_record_full(s).unwrap().unwrap().doc_id.0)
                .collect();
            let _ = std::fs::remove_dir_all(&dir);
            ids
        };
        let input_order: Vec<String> = (0..120)
            .filter(|i| i % 7 != 0)
            .map(|i| format!("d{i:03}"))
            .collect();
        // 120 docs - 18 pre-merge deletes = 102 live.
        assert_eq!(run(Some(101)), input_order, "over the cap: streams");
        assert_eq!(run(Some(0)), input_order, "cap 0: never reorders");
        assert_ne!(run(Some(102)), input_order, "at the cap: reorders");
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

    /// #464: one large segment with half its documents deleted. With the
    /// tombstone-ratio trigger off, tiered compaction leaves it alone, as
    /// before; with it on, the scheduler is told to compact and the pass
    /// rewrites the segment on its own, expunging the deleted documents.
    #[test]
    fn tombstone_ratio_trigger_rewrites_a_large_deleted_segment() {
        for trigger in [None, Some(3_000u32)] {
            let dir = std::env::temp_dir().join(format!(
                "kosha-test-compact-tombstone-ratio-{}",
                trigger.is_some()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let ns = NamespaceId("test".into());
            let policy = CompactionPolicy {
                max_mergeable_docs: 3,
                trigger_mergeable_segments: 2,
                tombstone_ratio_bp: trigger,
                ..CompactionPolicy::default()
            };
            let idx = Indexer::new(dir.clone()).with_compaction_policy(policy.clone());
            idx.index_documents(
                ns.clone(),
                (0..6)
                    .map(|i| Document {
                        id: DocumentId(format!("d{i}")),
                        fields: vec![
                            Field::text("title", format!("doc {i}")),
                            Field::keyword("id", format!("d{i}")),
                        ],
                    })
                    .collect(),
            )
            .unwrap();
            idx.flush_namespace(&ns).unwrap();
            for i in 0..3 {
                let m = idx.manifest(&ns).unwrap();
                let filter: FilterClause =
                    serde_json::from_str(&format!(r#"{{"term": {{"id": "d{i}"}}}}"#)).unwrap();
                assert_eq!(idx.delete_by_query(&ns, &m, &filter).unwrap(), 1);
            }
            let before = idx.manifest(&ns).unwrap();
            assert_eq!(before.segments.len(), 1);
            assert_eq!(before.segments[0].doc_count, 6);
            assert_eq!(tombstone_ratio(&before, &before.segments[0]), 0.5);

            assert_eq!(idx.needs_compaction(&ns), trigger.is_some());
            let result = idx
                .compact_namespace_with_options(&ns, CompactOptions::tiered(policy))
                .unwrap();
            assert_eq!(result.merged, trigger.is_some());
            let after = idx.manifest(&ns).unwrap();
            assert_eq!(after.segments.len(), 1);
            if trigger.is_some() {
                assert_eq!(after.segments[0].doc_count, 3, "deleted docs expunged");
                assert!(after
                    .tombstones
                    .get(&after.segments[0].segment_id)
                    .is_none_or(|t| t.is_empty()));
                assert!(!idx.needs_compaction(&ns), "nothing left over the ratio");
            } else {
                assert_eq!(after.segments[0].segment_id, before.segments[0].segment_id);
            }
            let _ = std::fs::remove_dir_all(&dir);
        }
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

    fn copy_dir(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    /// #254: a compaction-only writer (the `indexer` role) has no WAL
    /// records. Its compaction keeps the ingest writer's WAL checkpoint and
    /// origin, and so does its rebase onto a newer ingest publish: the
    /// published manifest must keep telling the ingest writer which of its
    /// WAL records are covered, or a restart would replay (or keep) them
    /// wrongly.
    #[test]
    fn compaction_writer_rebase_keeps_the_remote_wal_checkpoint_and_origin() {
        let ingest_dir = rebase_dir("wal-ingest");
        let compactor_dir = rebase_dir("wal-compactor");
        let ns = NamespaceId("ns".into());
        let ingest = Indexer::new(ingest_dir.clone());
        write_docs(&ingest, &ns, &["a"]);
        write_docs(&ingest, &ns, &["b"]);
        write_docs(&ingest, &ns, &["c"]);
        let base = commit(&ingest, &ns);
        let ingest_origin = base.wal_origin.clone();
        assert!(ingest_origin.is_some() && base.wal_checkpoint > 0);

        // The compactor installs the committed manifest (segments come from
        // object storage; here, a copy of the ingest's directory).
        copy_dir(&ingest_dir.join(&ns.0), &compactor_dir.join(&ns.0));
        let compactor = Indexer::new(compactor_dir.clone());
        assert_ne!(Some(compactor.wal.origin().to_owned()), ingest_origin);
        compactor.restore_manifest(ns.clone(), base.clone());
        let result = compactor
            .compact_namespace_with_options(&ns, CompactOptions::full())
            .unwrap();
        assert!(result.merged);
        let compacted = compactor.publication_snapshot(&ns).unwrap().1;
        assert_eq!(compacted.wal_origin, ingest_origin);
        assert_eq!(compacted.wal_checkpoint, base.wal_checkpoint);

        // Meanwhile the ingest writer flushed and published again.
        write_docs(&ingest, &ns, &["d"]);
        let remote = commit(&ingest, &ns);
        assert!(remote.wal_checkpoint > base.wal_checkpoint);

        assert_eq!(
            compactor.rebase_onto(&ns, remote.clone()).unwrap(),
            RebaseOutcome::Rebased
        );
        let (expected, rebased) = compactor.publication_snapshot(&ns).unwrap();
        assert_eq!(expected, remote.version);
        assert_eq!(
            rebased.wal_origin, remote.wal_origin,
            "origin stays the ingest's"
        );
        assert_eq!(
            rebased.wal_checkpoint, remote.wal_checkpoint,
            "checkpoint stays the ingest's"
        );
        // The merge output replaced the three inputs; the ingest's new
        // segment is kept.
        let base_ids = segment_ids(&base);
        let ids = segment_ids(&rebased);
        assert!(ids.is_disjoint(&base_ids));
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&remote.segments.last().unwrap().segment_id));
        let docs: u32 = rebased.segments.iter().map(|e| e.doc_count).sum();
        assert_eq!(docs, 4);
        let _ = std::fs::remove_dir_all(ingest_dir);
        let _ = std::fs::remove_dir_all(compactor_dir);
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
