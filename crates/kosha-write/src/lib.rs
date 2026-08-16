pub mod compaction;
pub mod wal;

pub use compaction::{
    needs_compaction, CompactMode, CompactOptions, CompactResult, CompactionPolicy, MergePlan,
};

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use kosha_core::{
    Bm25Params, Document, DocumentId, FilterClause, FilterStore, KoshaError, Manifest,
    ManifestEntry, NamespaceId, RangeBound, SegmentId,
};
use kosha_segment::SegmentWriter;

use compaction::select_merge_inputs;

struct NamespaceBuffer {
    #[allow(dead_code)]
    namespace: NamespaceId,
    documents: Vec<Document>,
    segment_counter: u64,
    bm25_params: Bm25Params,
}

/// Mutable per-namespace write state (DESIGN.md §7.1).
struct NamespaceState {
    buffer: Option<NamespaceBuffer>,
    manifest: Manifest,
    /// Tombstoned docs: segment_id → set of doc_seqs
    tombstones: HashMap<SegmentId, HashSet<u32>>,
    /// Lazy map of doc_id → flushed (segment, seq) locations.
    /// Built on first `/exists` call; invalidated across replace/compact/restore.
    id_index: Option<HashMap<DocumentId, Vec<(SegmentId, u32)>>>,
    /// True once a manifest has been created or restored for this namespace.
    /// Distinguishes "unknown namespace" from "empty namespace".
    known: bool,
}

impl NamespaceState {
    fn new() -> Self {
        Self {
            buffer: None,
            manifest: Manifest {
                version: 0,
                segments: Vec::new(),
                segment_footers: Default::default(),
            },
            tombstones: HashMap::new(),
            id_index: None,
            known: false,
        }
    }
}

struct NamespaceHandle {
    state: Mutex<NamespaceState>,
    /// At most one compaction in flight per namespace. Held across merge I/O
    /// while `state` is released (DESIGN.md §7.1).
    compact: Mutex<()>,
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
    wal: Mutex<crate::wal::WalWriter>,
    /// Whether WAL is enabled
    wal_enabled: bool,
    namespaces: Mutex<HashMap<NamespaceId, Arc<NamespaceHandle>>>,
    compaction_policy: CompactionPolicy,
}

impl Indexer {
    pub fn new(data_dir: PathBuf) -> Self {
        let wal_dir = data_dir.join("_wal");
        std::fs::create_dir_all(&wal_dir).ok();
        let backend = Box::new(kosha_core::LocalStorage::new(wal_dir.clone()));
        let wal = crate::wal::WalWriter::new(backend, wal_dir.clone());

        let idx = Self {
            data_dir,
            flush_threshold: 1000,
            bm25_params: Bm25Params::default(),
            wal: Mutex::new(wal),
            wal_enabled: true,
            namespaces: Mutex::new(HashMap::new()),
            compaction_policy: CompactionPolicy::default(),
        };

        // Recover un-flushed documents from WAL on startup.
        if let Ok(records) = crate::wal::WalWriter::recover(&wal_dir) {
            for record in &records {
                let handle = idx.ns_handle(&record.namespace);
                let mut state = handle.state.lock().unwrap();
                let buf = Self::buffer_mut_in(
                    &mut state,
                    record.namespace.clone(),
                    &idx.data_dir,
                    &idx.bm25_params,
                );
                buf.documents.extend(record.documents.clone());
            }
        }

        idx
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

    fn ns_handle(&self, namespace: &NamespaceId) -> Arc<NamespaceHandle> {
        let mut map = self.namespaces.lock().unwrap();
        map.entry(namespace.clone())
            .or_insert_with(|| {
                Arc::new(NamespaceHandle {
                    state: Mutex::new(NamespaceState::new()),
                    compact: Mutex::new(()),
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
                namespace: namespace.clone(),
                documents: Vec::new(),
                segment_counter: counter,
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
        let handle = self.ns_handle(namespace);
        let mut state = handle.state.lock().unwrap();
        let mut total = 0;
        for entry in &manifest.segments {
            let seg_dir = self.data_dir.join(&namespace.0).join(&entry.segment_id.0);
            if !seg_dir.exists() {
                continue;
            }
            let reader = kosha_segment::SegmentReader::open(seg_dir)?;
            let store = reader.filter_store();
            let all: HashSet<u32> = (0..reader.doc_count()).collect();
            let matching = crate::apply_filter_delete(filter, &store, &all)?;
            if !matching.is_empty() {
                total += matching.len();
                state
                    .tombstones
                    .entry(entry.segment_id.clone())
                    .or_default()
                    .extend(matching);
            }
        }
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
    /// local files are treated as absent for that segment.
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
                    continue;
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
    /// dropped in place; flushed versions trigger a durable segment rewrite
    /// so duplicates do not accumulate. Incoming batches are last-write-wins
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

        {
            let mut state = handle.state.lock().unwrap();
            if let Some(buf) = state.buffer.as_mut() {
                buf.documents.retain(|doc| !id_set.contains(&doc.id));
            }
            if Self::has_flushed_ids_in(&mut state, &namespace, &self.data_dir, &ids)? {
                drop(state);
                return self.rewrite_documents(namespace, documents, false);
            }

            if self.wal_enabled {
                if let Ok(mut wal) = self.wal.lock() {
                    wal.append(&namespace, &documents).ok();
                }
            }

            let flush_threshold = self.flush_threshold;
            let buf = Self::buffer_mut_in(
                &mut state,
                namespace.clone(),
                &self.data_dir,
                &self.bm25_params,
            );
            buf.documents.extend(documents);
            if buf.documents.len() >= flush_threshold {
                Self::flush_namespace_in(
                    &mut state,
                    &namespace,
                    &self.data_dir,
                    self.wal_enabled,
                    &self.wal,
                )?;
            }
        }
        Ok(count)
    }

    /// Replace documents by id without leaving tombstones behind.
    ///
    /// Immutable segments containing an old version are rewritten, preserving
    /// their other live documents. Partial replacement documents are merged
    /// field-by-field onto the latest existing copy (OpenSearch `_update`
    /// semantics). The rewritten manifests stop referencing the old segments,
    /// so replacements remain correct after a process restart.
    pub fn replace_documents(
        &self,
        namespace: NamespaceId,
        documents: Vec<Document>,
    ) -> Result<usize, KoshaError> {
        self.rewrite_documents(namespace, documents, true)
    }

    /// Rewrite segments that contain `documents`' ids, then write the new
    /// versions. When `merge` is true, patch fields onto the latest existing
    /// copy (`/replace`); when false, the incoming document fully replaces it
    /// (`/index` upsert).
    fn rewrite_documents(
        &self,
        namespace: NamespaceId,
        documents: Vec<Document>,
        merge: bool,
    ) -> Result<usize, KoshaError> {
        if documents.is_empty() {
            return Ok(0);
        }

        let handle = self.ns_handle(&namespace);
        let mut state = handle.state.lock().unwrap();

        // Include buffered versions in the segment scan.
        Self::flush_namespace_in(
            &mut state,
            &namespace,
            &self.data_dir,
            self.wal_enabled,
            &self.wal,
        )?;
        let replacement_ids: HashSet<&str> =
            documents.iter().map(|doc| doc.id.0.as_str()).collect();
        let manifest = state.manifest.clone();
        let mut replaced_segments = HashSet::new();
        let mut carried_documents = Vec::new();
        // Latest segment wins when the same id was appended more than once.
        let mut existing_fields: HashMap<String, Vec<kosha_core::Field>> = HashMap::new();

        for entry in &manifest.segments {
            let seg_dir = self.data_dir.join(&namespace.0).join(&entry.segment_id.0);
            if !seg_dir.exists() {
                continue;
            }
            let reader = kosha_segment::SegmentReader::open(seg_dir)?;
            if !reader
                .iter_doc_meta()
                .any(|meta| replacement_ids.contains(meta.doc_id))
            {
                continue;
            }

            replaced_segments.insert(entry.segment_id.clone());
            let tombstones = state.tombstones.get(&entry.segment_id).cloned();
            // `?`: same reasoning as compact_namespace_with_options — a doc
            // that fails to read must abort the rewrite, not be silently
            // excluded from carried_documents (that would durably drop it
            // from the namespace once the rewritten segment is published).
            for record in reader.iter_doc_records() {
                let record = record?;
                if tombstones
                    .as_ref()
                    .is_some_and(|set| set.contains(&record.doc_seq))
                {
                    continue;
                }
                if replacement_ids.contains(record.doc_id.0.as_str()) {
                    if merge {
                        existing_fields.insert(record.doc_id.0.clone(), record.fields.clone());
                    }
                } else {
                    carried_documents.push(Document {
                        id: record.doc_id.clone(),
                        fields: record.fields.clone(),
                    });
                }
            }
        }

        if !replaced_segments.is_empty() {
            state
                .manifest
                .segments
                .retain(|entry| !replaced_segments.contains(&entry.segment_id));
            state.manifest.version += 1;
            state.known = true;
            for segment_id in &replaced_segments {
                state.tombstones.remove(segment_id);
            }
        }

        let count = documents.len();
        for document in documents {
            let merged = if merge {
                match existing_fields.remove(&document.id.0) {
                    Some(base_fields) => Document {
                        id: document.id,
                        fields: merge_fields(base_fields, document.fields),
                    },
                    None => document,
                }
            } else {
                document
            };
            carried_documents.push(merged);
        }
        state.id_index = None;
        // Bypass upsert re-entry: these ids were just removed from live
        // segments, so the append path is correct and avoids a rewrite loop.
        Self::append_documents_in(
            &mut state,
            namespace.clone(),
            carried_documents,
            &self.data_dir,
            &self.bm25_params,
            self.flush_threshold,
            self.wal_enabled,
            &self.wal,
        )?;
        Self::flush_namespace_in(
            &mut state,
            &namespace,
            &self.data_dir,
            self.wal_enabled,
            &self.wal,
        )?;
        drop(state);

        // The manifest no longer references these immutable segments.
        for segment_id in replaced_segments {
            let seg_dir = self.data_dir.join(&namespace.0).join(segment_id.0);
            std::fs::remove_dir_all(seg_dir).ok();
        }
        Ok(count)
    }

    /// Append without upsert checks. Used by segment rewrite after old
    /// versions of the same ids have already been removed from the manifest.
    #[allow(clippy::too_many_arguments)]
    fn append_documents_in(
        state: &mut NamespaceState,
        namespace: NamespaceId,
        documents: Vec<Document>,
        data_dir: &Path,
        bm25_params: &Bm25Params,
        flush_threshold: usize,
        wal_enabled: bool,
        wal: &Mutex<crate::wal::WalWriter>,
    ) -> Result<usize, KoshaError> {
        if documents.is_empty() {
            return Ok(0);
        }
        if wal_enabled {
            if let Ok(mut wal_guard) = wal.lock() {
                wal_guard.append(&namespace, &documents).ok();
            }
        }
        let count = documents.len();
        {
            let buf = Self::buffer_mut_in(state, namespace.clone(), data_dir, bm25_params);
            buf.documents.extend(documents);
        }
        let should_flush = state
            .buffer
            .as_ref()
            .is_some_and(|b| b.documents.len() >= flush_threshold);
        if should_flush {
            Self::flush_namespace_in(state, &namespace, data_dir, wal_enabled, wal)?;
        }
        Ok(count)
    }

    fn has_flushed_ids_in(
        state: &mut NamespaceState,
        namespace: &NamespaceId,
        data_dir: &Path,
        ids: &[DocumentId],
    ) -> Result<bool, KoshaError> {
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

    pub fn flush_namespace(&self, namespace: &NamespaceId) -> Result<(), KoshaError> {
        let handle = self.ns_handle(namespace);
        let mut state = handle.state.lock().unwrap();
        Self::flush_namespace_in(
            &mut state,
            namespace,
            &self.data_dir,
            self.wal_enabled,
            &self.wal,
        )
    }

    fn flush_namespace_in(
        state: &mut NamespaceState,
        namespace: &NamespaceId,
        data_dir: &Path,
        wal_enabled: bool,
        wal: &Mutex<crate::wal::WalWriter>,
    ) -> Result<(), KoshaError> {
        let (docs, seg_id, bm25_params) = {
            let Some(buf) = state.buffer.as_mut() else {
                return Ok(());
            };
            if buf.documents.is_empty() {
                return Ok(());
            }
            let docs = std::mem::take(&mut buf.documents);
            let seg_id = SegmentId(format!(
                "{}-{:06x}",
                namespace.0.replace('/', "_"),
                buf.segment_counter
            ));
            buf.segment_counter += 1;
            let bm25_params = buf.bm25_params.clone();
            (docs, seg_id, bm25_params)
        };

        let seg_dir = data_dir.join(&namespace.0).join(seg_id.0.as_str());
        let mut writer = SegmentWriter::new(seg_id.clone(), seg_dir);
        for doc in &docs {
            writer.add_document(doc.id.clone(), doc.fields.clone());
        }
        let footer = writer.finalize(bm25_params)?;

        // WAL no longer needed for flushed data.
        if wal_enabled {
            if let Ok(mut wal_guard) = wal.lock() {
                wal_guard.clear().ok();
            }
        }

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
        let handle = self.ns_handle(namespace);
        let _compact_guard = handle.compact.lock().unwrap();

        let (plan, tombstones, bm25_params, segments_before) = {
            let mut state = handle.state.lock().unwrap();
            if !state.known {
                return Ok(CompactResult {
                    merged: false,
                    segments_before: 0,
                    segments_after: 0,
                    segments_merged: 0,
                });
            }
            let segments_before = state.manifest.segments.len();
            let ns_dir = self.data_dir.join(&namespace.0);
            let plan = match select_merge_inputs(
                &state.manifest,
                &opts,
                |seg_id| ns_dir.join(&seg_id.0).exists(),
                |seg_id| segment_dir_bytes(&ns_dir.join(&seg_id.0)),
            ) {
                Some(p) => p,
                None => {
                    return Ok(CompactResult {
                        merged: false,
                        segments_before,
                        segments_after: segments_before,
                        segments_merged: 0,
                    });
                }
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
            (plan, tombstones, bm25_params, segments_before)
        };

        after_plan();

        // ── Merge I/O with state lock released (DESIGN.md §7.1) ──────────
        let ns_dir = self.data_dir.join(&namespace.0);
        let seg_id = SegmentId(format!(
            "{}-compact-{:x}",
            namespace.0.replace('/', "_"),
            chrono_now()
        ));
        let seg_dir = ns_dir.join(seg_id.0.as_str());
        let mut writer = kosha_segment::SegmentWriter::new(seg_id.clone(), seg_dir);
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
            // incident this silent-drop used to cause. Nothing has been
            // written to `seg_dir` yet at this point (finalize() hasn't run
            // and add_document only builds in-memory state), so aborting
            // here leaves no partial output on disk to clean up.
            for doc_rec in reader.iter_doc_records() {
                let doc_rec = doc_rec?;
                if ts.is_some_and(|set| set.contains(&doc_rec.doc_seq)) {
                    continue;
                }
                seq_remap.insert((entry.segment_id.clone(), doc_rec.doc_seq), next_new_seq);
                next_new_seq += 1;
                writer.add_document(doc_rec.doc_id, doc_rec.fields);
                any_docs = true;
            }
        }

        if !any_docs {
            let _ = std::fs::remove_dir_all(ns_dir.join(&seg_id.0));
            return Ok(CompactResult {
                merged: false,
                segments_before,
                segments_after: segments_before,
                segments_merged: 0,
            });
        }

        let footer = writer.finalize(bm25_params)?;

        // ── CAS publish under state lock ─────────────────────────────────
        {
            let mut state = handle.state.lock().unwrap();
            let still_present = old_segment_ids
                .iter()
                .all(|id| state.manifest.segments.iter().any(|e| e.segment_id == *id));
            if !still_present {
                // A concurrent rewrite removed an input — drop the orphan
                // merge output rather than publishing a conflicting manifest.
                let _ = std::fs::remove_dir_all(ns_dir.join(&seg_id.0));
                return Ok(CompactResult {
                    merged: false,
                    segments_before,
                    segments_after: state.manifest.segments.len(),
                    segments_merged: 0,
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
            if !carried_tombstones.is_empty() {
                state
                    .tombstones
                    .entry(merged_segment_id)
                    .or_default()
                    .extend(carried_tombstones);
            }
            state.id_index = None;
        }

        for seg_id in &old_segment_ids {
            let seg_dir = ns_dir.join(&seg_id.0);
            if seg_dir.exists() {
                std::fs::remove_dir_all(&seg_dir).ok();
            }
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
    pub fn restore_manifest(&self, namespace: NamespaceId, manifest: Manifest) {
        let handle = self.ns_handle(&namespace);
        let mut state = handle.state.lock().unwrap();
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
        state.manifest = manifest;
        state.known = true;
    }

    pub fn manifest(&self, namespace: &NamespaceId) -> Option<Manifest> {
        self.manifest_cloned(namespace)
    }

    pub fn manifest_cloned(&self, namespace: &NamespaceId) -> Option<Manifest> {
        let map = self.namespaces.lock().unwrap();
        let handle = map.get(namespace)?;
        let state = handle.state.lock().unwrap();
        if !state.known {
            return None;
        }
        Some(state.manifest.clone())
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
fn chrono_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

pub fn apply_filter_delete(
    filter: &FilterClause,
    store: &FilterStore,
    candidates: &HashSet<u32>,
) -> Result<HashSet<u32>, KoshaError> {
    match filter {
        FilterClause::Term { term } => {
            let mut result = HashSet::new();
            for (field, value) in term {
                if let Some(entries) = store.string_fields.get(field) {
                    for &(ds, ref v) in entries {
                        if candidates.contains(&ds) && v == value {
                            result.insert(ds);
                        }
                    }
                }
            }
            Ok(result)
        }
        FilterClause::Terms { terms } => {
            let mut result = HashSet::new();
            for (field, values) in terms {
                let vs: HashSet<&str> = values.iter().map(|s| s.as_str()).collect();
                if let Some(entries) = store.string_fields.get(field) {
                    for &(ds, ref v) in entries {
                        if candidates.contains(&ds) && vs.contains(v.as_str()) {
                            result.insert(ds);
                        }
                    }
                }
            }
            Ok(result)
        }
        FilterClause::Range { range } => {
            let mut result = HashSet::new();
            for (field, bound) in range {
                if let Some(entries) = store.integer_fields.get(field) {
                    for &(ds, val) in entries {
                        if candidates.contains(&ds) && range_check_i64(val, bound) {
                            result.insert(ds);
                        }
                    }
                } else if let Some(entries) = store.float_fields.get(field) {
                    for &(ds, val) in entries {
                        if candidates.contains(&ds) && range_check_f64(val, bound) {
                            result.insert(ds);
                        }
                    }
                } else if let Some(entries) = store.string_fields.get(field) {
                    for &(ds, ref v) in entries {
                        if candidates.contains(&ds) && range_check_str(v, bound) {
                            result.insert(ds);
                        }
                    }
                }
            }
            Ok(result)
        }
        FilterClause::Bool { bool: b } => {
            let mut working: Option<HashSet<u32>> = None;
            if !b.must.is_empty() {
                let mut acc = candidates.clone();
                for c in &b.must {
                    acc = apply_filter_delete(c, store, &acc)?;
                }
                working = Some(acc);
            }
            if !b.must_not.is_empty() {
                let base = working.take().unwrap_or_else(|| candidates.clone());
                let mut excluded = HashSet::new();
                for c in &b.must_not {
                    excluded.extend(apply_filter_delete(c, store, &base)?);
                }
                working = Some(base.difference(&excluded).copied().collect());
            }
            if !b.should.is_empty() {
                let base = working.take().unwrap_or_else(|| candidates.clone());
                let mut scores: HashMap<u32, usize> = HashMap::new();
                for c in &b.should {
                    for ds in apply_filter_delete(c, store, &base)? {
                        *scores.entry(ds).or_default() += 1;
                    }
                }
                let passed: HashSet<u32> = scores
                    .into_iter()
                    .filter(|(_, c)| *c >= b.minimum_should_match)
                    .map(|(d, _)| d)
                    .collect();
                working = Some(if base.is_empty() {
                    passed
                } else {
                    base.intersection(&passed).copied().collect()
                });
            }
            Ok(working.unwrap_or_else(|| candidates.clone()))
        }
        FilterClause::MatchAll { .. } => Ok(candidates.clone()),
    }
}

fn range_check_i64(val: i64, bound: &RangeBound) -> bool {
    if let Some(ref gte) = bound.gte {
        if let Ok(b) = gte.parse::<i64>() {
            if val < b {
                return false;
            }
        }
    }
    if let Some(ref gt) = bound.gt {
        if let Ok(b) = gt.parse::<i64>() {
            if val <= b {
                return false;
            }
        }
    }
    if let Some(ref lte) = bound.lte {
        if let Ok(b) = lte.parse::<i64>() {
            if val > b {
                return false;
            }
        }
    }
    if let Some(ref lt) = bound.lt {
        if let Ok(b) = lt.parse::<i64>() {
            if val >= b {
                return false;
            }
        }
    }
    true
}
fn range_check_f64(val: f64, bound: &RangeBound) -> bool {
    if let Some(ref gte) = bound.gte {
        if let Ok(b) = gte.parse::<f64>() {
            if val < b {
                return false;
            }
        }
    }
    if let Some(ref gt) = bound.gt {
        if let Ok(b) = gt.parse::<f64>() {
            if val <= b {
                return false;
            }
        }
    }
    if let Some(ref lte) = bound.lte {
        if let Ok(b) = lte.parse::<f64>() {
            if val > b {
                return false;
            }
        }
    }
    if let Some(ref lt) = bound.lt {
        if let Ok(b) = lt.parse::<f64>() {
            if val >= b {
                return false;
            }
        }
    }
    true
}
fn range_check_str(val: &str, bound: &RangeBound) -> bool {
    if let Some(ref gte) = bound.gte {
        if val < gte.as_str() {
            return false;
        }
    }
    if let Some(ref gt) = bound.gt {
        if val <= gt.as_str() {
            return false;
        }
    }
    if let Some(ref lte) = bound.lte {
        if val > lte.as_str() {
            return false;
        }
    }
    if let Some(ref lt) = bound.lt {
        if val >= lt.as_str() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::{DocumentId, Field, FilterClause};

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

        // Simulate a restart on ephemeral storage: local segment files are
        // gone, but the control store still holds the manifest.
        let _ = std::fs::remove_dir_all(&dir);
        let idx2 = Indexer::new(dir.clone());
        assert!(idx2.manifest(&ns).is_none());

        idx2.restore_manifest(ns.clone(), persisted);
        assert_eq!(idx2.manifest(&ns).unwrap().segments.len(), 1);

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
        assert_eq!(manifest.segments.len(), 1);
        let reader = kosha_segment::SegmentReader::open(
            dir.join(&ns.0).join(&manifest.segments[0].segment_id.0),
        )
        .unwrap();
        let records: Vec<_> = reader
            .iter_doc_records()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
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
        assert_eq!(manifest.segments.len(), 1);
        assert_ne!(manifest.segments[0].segment_id, old_segment);
        assert!(!dir.join(&ns.0).join(&old_segment.0).exists());

        let reader = kosha_segment::SegmentReader::open(
            dir.join(&ns.0).join(&manifest.segments[0].segment_id.0),
        )
        .unwrap();
        let records: Vec<_> = reader
            .iter_doc_records()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 2);
        let values: HashMap<_, _> = records
            .iter()
            .map(|record| (record.doc_id.0.as_str(), record.fields[0].value.as_str()))
            .collect();
        assert_eq!(values.get("d1"), Some(&"new"));
        assert_eq!(values.get("d2"), Some(&"preserved"));
        assert!(idx.get_tombstones(&ns).is_none());

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
        let reader = kosha_segment::SegmentReader::open(
            dir.join(&ns.0).join(&manifest.segments[0].segment_id.0),
        )
        .unwrap();
        let records: Vec<_> = reader
            .iter_doc_records()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
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
        assert!(std::fs::read_dir(dir.join("test")).unwrap().count() == 1);

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
}
