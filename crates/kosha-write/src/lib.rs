pub mod wal;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use kosha_core::{
    Bm25Params, Document, DocumentId, FilterClause, FilterStore, KoshaError, Manifest,
    ManifestEntry, NamespaceId, RangeBound, SegmentId,
};
use kosha_segment::SegmentWriter;

struct NamespaceBuffer {
    #[allow(dead_code)]
    namespace: NamespaceId,
    documents: Vec<Document>,
    segment_counter: u64,
    bm25_params: Bm25Params,
}

pub struct Indexer {
    data_dir: PathBuf,
    buffers: HashMap<NamespaceId, NamespaceBuffer>,
    manifests: HashMap<NamespaceId, Manifest>,
    flush_threshold: usize,
    bm25_params: Bm25Params,
    /// Tombstoned docs: namespace → segment_id → set of doc_seqs
    tombstones: HashMap<NamespaceId, HashMap<SegmentId, HashSet<u32>>>,
    /// Lazy per-namespace map of doc_id → flushed (segment, seq) locations.
    /// Built on first `/exists` call; invalidated across replace/compact/restore.
    id_index: HashMap<NamespaceId, HashMap<DocumentId, Vec<(SegmentId, u32)>>>,
    /// Write-Ahead Log for durability
    wal: std::sync::Mutex<crate::wal::WalWriter>,
    /// Whether WAL is enabled
    wal_enabled: bool,
}

impl Indexer {
    pub fn new(data_dir: PathBuf) -> Self {
        let wal_dir = data_dir.join("_wal");
        std::fs::create_dir_all(&wal_dir).ok();
        let backend = Box::new(kosha_core::LocalStorage::new(wal_dir.clone()));
        let wal = crate::wal::WalWriter::new(backend, wal_dir.clone());

        let mut idx = Self {
            data_dir,
            buffers: HashMap::new(),
            manifests: HashMap::new(),
            flush_threshold: 1000,
            bm25_params: Bm25Params::default(),
            tombstones: HashMap::new(),
            id_index: HashMap::new(),
            wal: std::sync::Mutex::new(wal),
            wal_enabled: true,
        };

        // Recover un-flushed documents from WAL on startup.
        if let Ok(records) = crate::wal::WalWriter::recover(&wal_dir) {
            for record in &records {
                let ns = record.namespace.clone();
                let buf = idx.buffer_mut(ns);
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

    /// Delete documents matching a filter clause.
    /// Records tombstones so subsequent searches exclude them.
    pub fn delete_by_query(
        &mut self,
        namespace: &NamespaceId,
        manifest: &Manifest,
        filter: &FilterClause,
    ) -> Result<usize, KoshaError> {
        let mut total = 0;
        for entry in &manifest.segments {
            let seg_dir = self.data_dir.join(&namespace.0).join(&entry.segment_id.0);
            if !seg_dir.exists() {
                continue;
            }
            let reader = kosha_segment::SegmentReader::open(seg_dir)?;
            let store = &reader.filter_store;
            let all: HashSet<u32> = (0..reader.doc_count()).collect();
            let matching = crate::apply_filter_delete(filter, store, &all)?;
            if !matching.is_empty() {
                total += matching.len();
                self.tombstones
                    .entry(namespace.clone())
                    .or_default()
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
    ) -> Option<&HashMap<SegmentId, HashSet<u32>>> {
        self.tombstones.get(namespace)
    }

    /// Which of `ids` already exist in the namespace (buffer or flushed segments)?
    ///
    /// Flushed lookups require local segment files (hydrated cache). Missing
    /// local files are treated as absent for that segment.
    pub fn existing_ids(
        &mut self,
        namespace: &NamespaceId,
        ids: &[DocumentId],
    ) -> Result<HashSet<DocumentId>, KoshaError> {
        let mut found = HashSet::new();
        if ids.is_empty() {
            return Ok(found);
        }
        let wanted: HashSet<&DocumentId> = ids.iter().collect();

        if let Some(buf) = self.buffers.get(namespace) {
            for doc in &buf.documents {
                if wanted.contains(&doc.id) {
                    found.insert(doc.id.clone());
                }
            }
        }

        self.ensure_id_index(namespace)?;
        if let Some(index) = self.id_index.get(namespace) {
            let tombstones = self.tombstones.get(namespace);
            for id in ids {
                if found.contains(id) {
                    continue;
                }
                let Some(locs) = index.get(id) else {
                    continue;
                };
                let alive = locs.iter().any(|(seg, seq)| {
                    !tombstones
                        .and_then(|t| t.get(seg))
                        .is_some_and(|set| set.contains(seq))
                });
                if alive {
                    found.insert(id.clone());
                }
            }
        }
        Ok(found)
    }

    fn ensure_id_index(&mut self, namespace: &NamespaceId) -> Result<(), KoshaError> {
        if self.id_index.contains_key(namespace) {
            return Ok(());
        }
        let mut index: HashMap<DocumentId, Vec<(SegmentId, u32)>> = HashMap::new();
        let Some(manifest) = self.manifests.get(namespace).cloned() else {
            self.id_index.insert(namespace.clone(), index);
            return Ok(());
        };
        let data_dir = self.data_dir.clone();
        for entry in &manifest.segments {
            let seg_dir = data_dir.join(&namespace.0).join(&entry.segment_id.0);
            if !seg_dir.exists() {
                continue;
            }
            let reader = kosha_segment::SegmentReader::open_with_options(seg_dir, false)?;
            for meta in reader.iter_doc_meta() {
                index
                    .entry(meta.doc_id.clone())
                    .or_default()
                    .push((entry.segment_id.clone(), meta.doc_seq));
            }
        }
        self.id_index.insert(namespace.clone(), index);
        Ok(())
    }

    fn remember_flushed_ids(
        &mut self,
        namespace: &NamespaceId,
        seg_id: &SegmentId,
        docs: &[Document],
    ) {
        if !self.id_index.contains_key(namespace) {
            return;
        }
        let index = self.id_index.entry(namespace.clone()).or_default();
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
        &mut self,
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

        // Drop buffered prior versions so a re-index before flush does not
        // duplicate within the next segment.
        if let Some(buf) = self.buffers.get_mut(&namespace) {
            buf.documents.retain(|doc| !id_set.contains(&doc.id));
        }

        if self.has_flushed_ids(&namespace, &ids)? {
            // Durable full-document replace (no field merge).
            return self.rewrite_documents(namespace, documents, false);
        }

        // Fast path: ids are new — append like the original Phase 1 writer.
        if self.wal_enabled {
            if let Ok(mut wal) = self.wal.lock() {
                wal.append(&namespace, &documents).ok();
            }
        }

        let buf = self.buffer_mut(namespace.clone());
        buf.documents.extend(documents);
        if buf.documents.len() >= self.flush_threshold {
            self.flush_namespace(&namespace)?;
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
        &mut self,
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
        &mut self,
        namespace: NamespaceId,
        documents: Vec<Document>,
        merge: bool,
    ) -> Result<usize, KoshaError> {
        if documents.is_empty() {
            return Ok(0);
        }

        // Include buffered versions in the segment scan.
        self.flush_namespace(&namespace)?;
        let replacement_ids: HashSet<&str> =
            documents.iter().map(|doc| doc.id.0.as_str()).collect();
        let manifest = self.manifests.get(&namespace).cloned().unwrap_or(Manifest {
            version: 0,
            segments: Vec::new(),
        });
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
                .any(|meta| replacement_ids.contains(meta.doc_id.0.as_str()))
            {
                continue;
            }

            replaced_segments.insert(entry.segment_id.clone());
            let tombstones = self
                .tombstones
                .get(&namespace)
                .and_then(|by_segment| by_segment.get(&entry.segment_id));
            for record in reader.iter_doc_records() {
                if tombstones.is_some_and(|set| set.contains(&record.doc_seq)) {
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
            if let Some(current) = self.manifests.get_mut(&namespace) {
                current
                    .segments
                    .retain(|entry| !replaced_segments.contains(&entry.segment_id));
                current.version += 1;
            }
            if let Some(tombstones) = self.tombstones.get_mut(&namespace) {
                for segment_id in &replaced_segments {
                    tombstones.remove(segment_id);
                }
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
        self.id_index.remove(&namespace);
        // Bypass upsert re-entry: these ids were just removed from live
        // segments, so the append path is correct and avoids a rewrite loop.
        self.append_documents(namespace.clone(), carried_documents)?;
        self.flush_namespace(&namespace)?;

        // The manifest no longer references these immutable segments.
        for segment_id in replaced_segments {
            let seg_dir = self.data_dir.join(&namespace.0).join(segment_id.0);
            std::fs::remove_dir_all(seg_dir).ok();
        }
        Ok(count)
    }

    /// Append without upsert checks. Used by segment rewrite after old
    /// versions of the same ids have already been removed from the manifest.
    fn append_documents(
        &mut self,
        namespace: NamespaceId,
        documents: Vec<Document>,
    ) -> Result<usize, KoshaError> {
        if documents.is_empty() {
            return Ok(0);
        }
        if self.wal_enabled {
            if let Ok(mut wal) = self.wal.lock() {
                wal.append(&namespace, &documents).ok();
            }
        }
        let buf = self.buffer_mut(namespace.clone());
        let count = documents.len();
        buf.documents.extend(documents);
        if buf.documents.len() >= self.flush_threshold {
            self.flush_namespace(&namespace)?;
        }
        Ok(count)
    }

    fn has_flushed_ids(
        &mut self,
        namespace: &NamespaceId,
        ids: &[DocumentId],
    ) -> Result<bool, KoshaError> {
        if ids.is_empty() {
            return Ok(false);
        }
        self.ensure_id_index(namespace)?;
        let Some(index) = self.id_index.get(namespace) else {
            return Ok(false);
        };
        let tombstones = self.tombstones.get(namespace);
        Ok(ids.iter().any(|id| {
            index.get(id).is_some_and(|locs| {
                locs.iter().any(|(seg, seq)| {
                    !tombstones
                        .and_then(|t| t.get(seg))
                        .is_some_and(|set| set.contains(seq))
                })
            })
        }))
    }

    pub fn flush_namespace(&mut self, namespace: &NamespaceId) -> Result<(), KoshaError> {
        let data_dir = self.data_dir.clone();
        let (docs, seg_id, bm25_params) = {
            let buf = self.buffer_mut(namespace.clone());
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
        if self.wal_enabled {
            if let Ok(mut wal) = self.wal.lock() {
                wal.clear().ok();
            }
        }

        self.remember_flushed_ids(namespace, &seg_id, &docs);

        let manifest = self.manifests.entry(namespace.clone()).or_insert(Manifest {
            version: 0,
            segments: Vec::new(),
        });
        manifest.version += 1;
        manifest.segments.push(ManifestEntry {
            segment_id: seg_id,
            doc_count: footer.doc_count,
        });
        Ok(())
    }

    /// Compact segments for a namespace: merge small segments into one.
    /// Reads all existing segments, rebuilds a single merged segment,
    /// and garbage-collects the old segment directories.
    pub fn compact_namespace(&mut self, namespace: &NamespaceId) -> Result<(), KoshaError> {
        let manifest = match self.manifests.get(namespace) {
            Some(m) => m.clone(),
            None => return Ok(()),
        };

        if manifest.segments.len() <= 1 {
            return Ok(()); // Nothing to compact
        }

        let data_dir = self.data_dir.clone();
        let ns_dir = data_dir.join(&namespace.0);

        // Stream every surviving document straight into the new segment
        // writer instead of collecting every source segment's full content
        // into an `all_docs` buffer first. A namespace being compacted is,
        // by definition, made up of segments that can each be arbitrarily
        // large — holding all of them resident at once here would
        // reintroduce exactly the memory-scaling problem lazy segment
        // loading (`SegmentReader::doc_meta`/`doc_record_full`) exists to
        // fix, just on the write path instead of the read path.
        let seg_id = SegmentId(format!(
            "{}-compact-{:x}",
            namespace.0.replace('/', "_"),
            chrono_now()
        ));
        let seg_dir = data_dir.join(&namespace.0).join(seg_id.0.as_str());
        let mut writer = kosha_segment::SegmentWriter::new(seg_id.clone(), seg_dir);
        let mut old_segment_ids: Vec<SegmentId> = Vec::new();
        let mut any_docs = false;

        for entry in &manifest.segments {
            let seg_dir = ns_dir.join(&entry.segment_id.0);
            if !seg_dir.exists() {
                continue;
            }
            let reader = kosha_segment::SegmentReader::open(seg_dir)?;
            let tombstones = self
                .tombstones
                .get(namespace)
                .and_then(|t| t.get(&entry.segment_id));

            for doc_rec in reader.iter_doc_records() {
                // Skip tombstoned docs.
                if let Some(ts) = tombstones {
                    if ts.contains(&doc_rec.doc_seq) {
                        continue;
                    }
                }
                writer.add_document(doc_rec.doc_id, doc_rec.fields);
                any_docs = true;
            }
            old_segment_ids.push(entry.segment_id.clone());
        }

        if !any_docs {
            return Ok(());
        }

        let bm25_params = self.buffer_mut(namespace.clone()).bm25_params.clone();
        let footer = writer.finalize(bm25_params)?;

        // Update manifest: remove old segments, add merged segment.
        let manifest = self.manifests.get_mut(namespace).unwrap();
        manifest
            .segments
            .retain(|e| !old_segment_ids.contains(&e.segment_id));
        manifest.version += 1;
        manifest.segments.push(kosha_core::ManifestEntry {
            segment_id: seg_id,
            doc_count: footer.doc_count,
        });

        // Garbage-collect old segment directories.
        for seg_id in &old_segment_ids {
            let seg_dir = ns_dir.join(&seg_id.0);
            if seg_dir.exists() {
                std::fs::remove_dir_all(&seg_dir).ok();
            }
        }

        // Clear tombstones for compacted segments.
        if let Some(ts) = self.tombstones.get_mut(namespace) {
            for seg_id in &old_segment_ids {
                ts.remove(seg_id);
            }
        }
        self.id_index.remove(namespace);

        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<(), KoshaError> {
        let namespaces: Vec<NamespaceId> = self.buffers.keys().cloned().collect();
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
    pub fn restore_manifest(&mut self, namespace: NamespaceId, manifest: Manifest) {
        let max_counter = manifest
            .segments
            .iter()
            .filter_map(|e| segment_flush_counter(&e.segment_id))
            .max();
        if let Some(max) = max_counter {
            let buf = self.buffer_mut(namespace.clone());
            buf.segment_counter = buf.segment_counter.max(max + 1);
        }
        self.id_index.remove(&namespace);
        self.manifests.insert(namespace, manifest);
    }

    pub fn manifest(&self, namespace: &NamespaceId) -> Option<&Manifest> {
        self.manifests.get(namespace)
    }
    pub fn manifest_cloned(&self, namespace: &NamespaceId) -> Option<Manifest> {
        self.manifests.get(namespace).cloned()
    }
    pub fn namespaces(&self) -> impl Iterator<Item = &NamespaceId> {
        self.manifests.keys()
    }

    fn buffer_mut(&mut self, namespace: NamespaceId) -> &mut NamespaceBuffer {
        if !self.buffers.contains_key(&namespace) {
            let counter = if self.data_dir.join(&namespace.0).exists() {
                std::fs::read_dir(self.data_dir.join(&namespace.0))
                    .map(|e| e.filter_map(|e| e.ok()).count() as u64)
                    .unwrap_or(0)
            } else {
                0
            };
            self.buffers.insert(
                namespace.clone(),
                NamespaceBuffer {
                    namespace: namespace.clone(),
                    documents: Vec::new(),
                    segment_counter: counter,
                    bm25_params: self.bm25_params.clone(),
                },
            );
        }
        self.buffers.get_mut(&namespace).unwrap()
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
        let mut idx = Indexer::new(dir.clone());
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
        let mut idx = Indexer::new(dir.clone()).with_wal(false);
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
        let mut idx = Indexer::new(dir.clone());
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
        let mut idx2 = Indexer::new(dir.clone());
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
        let mut idx = Indexer::new(dir.clone()).with_wal(false);
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
        let records: Vec<_> = reader.iter_doc_records().collect();
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
        let mut idx = Indexer::new(dir.clone()).with_wal(false);
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
        let records: Vec<_> = reader.iter_doc_records().collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].fields[0].value, "second");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn replace_documents_rewrites_old_versions_durably() {
        let dir = std::env::temp_dir().join("kosha-test-replace");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let mut idx = Indexer::new(dir.clone());
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
        let records: Vec<_> = reader.iter_doc_records().collect();
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
        let mut idx = Indexer::new(dir.clone());
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
        let records: Vec<_> = reader.iter_doc_records().collect();
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
        let mut idx = Indexer::new(dir.clone()).with_flush_threshold(2);

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

        // Compact → should merge into 1 segment.
        idx.compact_namespace(&ns).unwrap();
        assert_eq!(idx.manifest(&ns).unwrap().segments.len(), 1);

        // Verify all 4 docs are in the merged segment.
        let seg_id = &idx.manifest(&ns).unwrap().segments[0].segment_id;
        let seg_dir = dir.join("test").join(&seg_id.0);
        assert!(seg_dir.exists());
        let reader = kosha_segment::SegmentReader::open(seg_dir).unwrap();
        assert_eq!(reader.doc_count(), 4);

        // Old segment directories should be deleted.
        assert!(std::fs::read_dir(dir.join("test")).unwrap().count() == 1);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
