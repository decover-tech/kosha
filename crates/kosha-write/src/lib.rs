use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use kosha_core::{
    Bm25Params, Document, FilterClause, FilterStore, KoshaError, Manifest, ManifestEntry,
    NamespaceId, RangeBound, SegmentId,
};
use kosha_segment::SegmentWriter;

struct NamespaceBuffer {
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
}

impl Indexer {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir, buffers: HashMap::new(), manifests: HashMap::new(),
            flush_threshold: 1000, bm25_params: Bm25Params::default(),
            tombstones: HashMap::new(),
        }
    }

    pub fn with_flush_threshold(mut self, threshold: usize) -> Self {
        self.flush_threshold = threshold; self
    }

    pub fn with_bm25_params(mut self, params: Bm25Params) -> Self {
        self.bm25_params = params; self
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
            if !seg_dir.exists() { continue; }
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

    pub fn index_documents(
        &mut self,
        namespace: NamespaceId,
        documents: Vec<Document>,
    ) -> Result<usize, KoshaError> {
        let buf = self.buffer_mut(namespace.clone());
        let count = documents.len();
        buf.documents.extend(documents);
        if buf.documents.len() >= self.flush_threshold {
            self.flush_namespace(&namespace)?;
        }
        Ok(count)
    }

    pub fn flush_namespace(&mut self, namespace: &NamespaceId) -> Result<(), KoshaError> {
        let data_dir = self.data_dir.clone();
        let (docs, seg_id, bm25_params) = {
            let buf = self.buffer_mut(namespace.clone());
            if buf.documents.is_empty() { return Ok(()); }
            let docs = std::mem::take(&mut buf.documents);
            let seg_id = SegmentId(format!("{}-{:06x}", namespace.0.replace('/', "_"), buf.segment_counter));
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

        let manifest = self.manifests.entry(namespace.clone()).or_insert(Manifest { version: 0, segments: Vec::new() });
        manifest.version += 1;
        manifest.segments.push(ManifestEntry { segment_id: seg_id, doc_count: footer.doc_count });
        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<(), KoshaError> {
        let namespaces: Vec<NamespaceId> = self.buffers.keys().cloned().collect();
        for ns in namespaces { self.flush_namespace(&ns)?; }
        Ok(())
    }

    pub fn manifest(&self, namespace: &NamespaceId) -> Option<&Manifest> { self.manifests.get(namespace) }
    pub fn manifest_cloned(&self, namespace: &NamespaceId) -> Option<Manifest> { self.manifests.get(namespace).cloned() }
    pub fn namespaces(&self) -> impl Iterator<Item = &NamespaceId> { self.manifests.keys() }

    fn buffer_mut(&mut self, namespace: NamespaceId) -> &mut NamespaceBuffer {
        if !self.buffers.contains_key(&namespace) {
            let counter = self.data_dir.join(&namespace.0).exists().then(|| {
                std::fs::read_dir(self.data_dir.join(&namespace.0))
                    .map(|e| e.filter_map(|e| e.ok()).count() as u64)
                    .unwrap_or(0)
            }).unwrap_or(0);
            self.buffers.insert(namespace.clone(), NamespaceBuffer {
                namespace: namespace.clone(), documents: Vec::new(),
                segment_counter: counter, bm25_params: self.bm25_params.clone(),
            });
        }
        self.buffers.get_mut(&namespace).unwrap()
    }
}

/// Standalone filter applier for delete operations (no Searcher dependency).
pub fn apply_filter_delete(
    filter: &FilterClause,
    store: &FilterStore,
    candidates: &HashSet<u32>,
) -> Result<HashSet<u32>, KoshaError> {
    use kosha_core::RangeBound;
    match filter {
        FilterClause::Term { term } => {
            let mut result = HashSet::new();
            for (field, value) in term {
                if let Some(entries) = store.string_fields.get(field) {
                    for &(ds, ref v) in entries {
                        if candidates.contains(&ds) && v == value { result.insert(ds); }
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
                        if candidates.contains(&ds) && vs.contains(v.as_str()) { result.insert(ds); }
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
                        if candidates.contains(&ds) && range_check_i64(val, bound) { result.insert(ds); }
                    }
                } else if let Some(entries) = store.float_fields.get(field) {
                    for &(ds, val) in entries {
                        if candidates.contains(&ds) && range_check_f64(val, bound) { result.insert(ds); }
                    }
                } else if let Some(entries) = store.string_fields.get(field) {
                    for &(ds, ref v) in entries {
                        if candidates.contains(&ds) && range_check_str(v, bound) { result.insert(ds); }
                    }
                }
            }
            Ok(result)
        }
        FilterClause::Bool { bool: b } => {
            let mut working: Option<HashSet<u32>> = None;
            if !b.must.is_empty() {
                let mut acc = candidates.clone();
                for c in &b.must { acc = apply_filter_delete(c, store, &acc)?; }
                working = Some(acc);
            }
            if !b.must_not.is_empty() {
                let base = working.take().unwrap_or_else(|| candidates.clone());
                let mut excluded = HashSet::new();
                for c in &b.must_not { excluded.extend(apply_filter_delete(c, store, &base)?); }
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
                let passed: HashSet<u32> = scores.into_iter()
                    .filter(|(_, c)| *c >= b.minimum_should_match).map(|(d, _)| d).collect();
                working = Some(if base.is_empty() { passed } else { base.intersection(&passed).copied().collect() });
            }
            Ok(working.unwrap_or_else(|| candidates.clone()))
        }
        FilterClause::MatchAll { .. } => Ok(candidates.clone()),
    }
}

fn range_check_i64(val: i64, bound: &RangeBound) -> bool {
    if let Some(ref gte) = bound.gte { if let Ok(b) = gte.parse::<i64>() { if val < b { return false; } } }
    if let Some(ref gt) = bound.gt { if let Ok(b) = gt.parse::<i64>() { if val <= b { return false; } } }
    if let Some(ref lte) = bound.lte { if let Ok(b) = lte.parse::<i64>() { if val > b { return false; } } }
    if let Some(ref lt) = bound.lt { if let Ok(b) = lt.parse::<i64>() { if val >= b { return false; } } }
    true
}
fn range_check_f64(val: f64, bound: &RangeBound) -> bool {
    if let Some(ref gte) = bound.gte { if let Ok(b) = gte.parse::<f64>() { if val < b { return false; } } }
    if let Some(ref gt) = bound.gt { if let Ok(b) = gt.parse::<f64>() { if val <= b { return false; } } }
    if let Some(ref lte) = bound.lte { if let Ok(b) = lte.parse::<f64>() { if val > b { return false; } } }
    if let Some(ref lt) = bound.lt { if let Ok(b) = lt.parse::<f64>() { if val >= b { return false; } } }
    true
}
fn range_check_str(val: &str, bound: &RangeBound) -> bool {
    if let Some(ref gte) = bound.gte { if val < gte.as_str() { return false; } }
    if let Some(ref gt) = bound.gt { if val <= gt.as_str() { return false; } }
    if let Some(ref lte) = bound.lte { if val > lte.as_str() { return false; } }
    if let Some(ref lt) = bound.lt { if val >= lt.as_str() { return false; } }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::{DocumentId, Field, FilterClause, ManifestEntry};
    use kosha_segment::SegmentWriter;

    #[test]
    fn delete_by_query_tombstones() {
        let dir = std::env::temp_dir().join("kosha-test-delete");
        let _ = std::fs::remove_dir_all(&dir);

        let ns = NamespaceId("test".into());
        let mut idx = Indexer::new(dir.clone());
        idx.index_documents(ns.clone(), vec![
            Document { id: DocumentId("d1".into()), fields: vec![
                Field::text("title", "hello world"), Field::keyword("status", "active"),
            ]},
            Document { id: DocumentId("d2".into()), fields: vec![
                Field::text("title", "goodbye world"), Field::keyword("status", "deleted"),
            ]},
            Document { id: DocumentId("d3".into()), fields: vec![
                Field::text("title", "hello again"), Field::keyword("status", "active"),
            ]},
        ]).unwrap();
        idx.flush_namespace(&ns).unwrap();

        let manifest = idx.manifest(&ns).unwrap().clone();

        // Delete docs with status=deleted.
        let filter: FilterClause = serde_json::from_str(r#"{"term": {"status": "deleted"}}"#).unwrap();
        let count = idx.delete_by_query(&ns, &manifest, &filter).unwrap();
        assert_eq!(count, 1);

        // Check tombstone.
        let tombstones = idx.get_tombstones(&ns).unwrap();
        assert_eq!(tombstones.len(), 1);
        for (_, seqs) in tombstones {
            // d2 has doc_seq=1
            assert!(seqs.contains(&1));
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
