//! Write path (DESIGN.md §7, implementation plan Epic 3):
//! per-namespace buffer, flush-to-segment, manifest publish.
//!
//! Phase 1 uses local disk as the backing store. S3 integration and
//! WAL durability will follow in later iterations.

use std::collections::HashMap;
use std::path::PathBuf;

use kosha_core::{
    Bm25Params, Document, KoshaError, Manifest, ManifestEntry, NamespaceId, SegmentId,
};
use kosha_segment::SegmentWriter;

/// In-memory buffer of documents for a single namespace.
struct NamespaceBuffer {
    #[allow(dead_code)]
    namespace: NamespaceId,
    documents: Vec<Document>,
    segment_counter: u64,
    bm25_params: Bm25Params,
}

/// The write-ahead indexer.
///
/// Collects documents per namespace and flushes them to segments on disk.
/// In Phase 1 this is strictly in-memory + local-disk; no WAL to S3 yet.
pub struct Indexer {
    /// Root directory under which namespace directories live.
    data_dir: PathBuf,
    /// Per-namespace buffers.
    buffers: HashMap<NamespaceId, NamespaceBuffer>,
    /// Namespaces and their current manifests.
    manifests: HashMap<NamespaceId, Manifest>,
    /// Documents buffered before triggering a flush.
    flush_threshold: usize,
    /// BM25 parameters for new segments.
    bm25_params: Bm25Params,
}

impl Indexer {
    /// Create a new indexer with the given data directory.
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            buffers: HashMap::new(),
            manifests: HashMap::new(),
            flush_threshold: 1000,
            bm25_params: Bm25Params::default(),
        }
    }

    /// Set the number of buffered documents that triggers an automatic flush.
    pub fn with_flush_threshold(mut self, threshold: usize) -> Self {
        self.flush_threshold = threshold;
        self
    }

    /// Set BM25 parameters for newly created segments.
    pub fn with_bm25_params(mut self, params: Bm25Params) -> Self {
        self.bm25_params = params;
        self
    }

    /// Index a batch of documents into the given namespace.
    ///
    /// Returns the number of documents indexed. If the buffer exceeds the
    /// flush threshold, a flush is triggered automatically.
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

    /// Force-flush all buffered documents for a namespace to a new segment.
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

        // Update the manifest.
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

    /// Flush all namespaces.
    pub fn flush_all(&mut self) -> Result<(), KoshaError> {
        let namespaces: Vec<NamespaceId> = self.buffers.keys().cloned().collect();
        for ns in namespaces {
            self.flush_namespace(&ns)?;
        }
        Ok(())
    }

    /// Get the current manifest for a namespace, if one exists.
    pub fn manifest(&self, namespace: &NamespaceId) -> Option<&Manifest> {
        self.manifests.get(namespace)
    }

    /// Get a copy of the manifest for a namespace.
    pub fn manifest_cloned(&self, namespace: &NamespaceId) -> Option<Manifest> {
        self.manifests.get(namespace).cloned()
    }

    /// List all known namespaces.
    pub fn namespaces(&self) -> impl Iterator<Item = &NamespaceId> {
        self.manifests.keys()
    }

    fn buffer_mut(&mut self, namespace: NamespaceId) -> &mut NamespaceBuffer {
        if !self.buffers.contains_key(&namespace) {
            let seg_dir = self.data_dir.join(&namespace.0);
            // Discover existing segment counter from disk.
            let counter = if seg_dir.exists() {
                std::fs::read_dir(&seg_dir)
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

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::{Document, DocumentId, Field};

    #[test]
    fn index_and_flush_single_namespace() {
        let dir = std::env::temp_dir().join("kosha-test-indexer-001");
        let _ = std::fs::remove_dir_all(&dir);

        let mut indexer = Indexer::new(dir.clone());
        let ns = NamespaceId("test-ns".into());

        let docs = vec![
            Document {
                id: DocumentId("d1".into()),
                fields: vec![Field {
                    name: "title".into(),
                    text: "hello world".into(),
                }],
            },
            Document {
                id: DocumentId("d2".into()),
                fields: vec![Field {
                    name: "title".into(),
                    text: "goodbye world".into(),
                }],
            },
        ];

        let count = indexer.index_documents(ns.clone(), docs).unwrap();
        assert_eq!(count, 2);

        indexer.flush_namespace(&ns).unwrap();

        let manifest = indexer.manifest(&ns).unwrap();
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].doc_count, 2);

        let seg_dir = dir.join("test-ns").join(&manifest.segments[0].segment_id.0);
        assert!(seg_dir.join("footer.json").exists());
        assert!(seg_dir.join("doc_store.bin").exists());
        assert!(seg_dir.join("inverted.idx").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn auto_flush_on_threshold() {
        let dir = std::env::temp_dir().join("kosha-test-indexer-002");
        let _ = std::fs::remove_dir_all(&dir);

        let mut indexer = Indexer::new(dir.clone()).with_flush_threshold(3);
        let ns = NamespaceId("auto-flush".into());

        let doc = || Document {
            id: DocumentId("d".into()),
            fields: vec![Field {
                name: "t".into(),
                text: "test".into(),
            }],
        };

        // First batch: 2 docs, under threshold.
        indexer
            .index_documents(ns.clone(), vec![doc(), doc()])
            .unwrap();
        // No flush should have happened yet.
        assert!(indexer.manifest(&ns).is_none());

        // Third doc pushes past threshold → auto-flush.
        indexer.index_documents(ns.clone(), vec![doc()]).unwrap();
        let manifest = indexer.manifest(&ns).unwrap();
        assert_eq!(manifest.segments.len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multiple_namespaces() {
        let dir = std::env::temp_dir().join("kosha-test-indexer-003");
        let _ = std::fs::remove_dir_all(&dir);

        let mut indexer = Indexer::new(dir.clone());
        let ns_a = NamespaceId("ns-a".into());
        let ns_b = NamespaceId("ns-b".into());

        indexer
            .index_documents(
                ns_a.clone(),
                vec![Document {
                    id: DocumentId("d1".into()),
                    fields: vec![Field {
                        name: "t".into(),
                        text: "alpha".into(),
                    }],
                }],
            )
            .unwrap();

        indexer
            .index_documents(
                ns_b.clone(),
                vec![Document {
                    id: DocumentId("d2".into()),
                    fields: vec![Field {
                        name: "t".into(),
                        text: "beta".into(),
                    }],
                }],
            )
            .unwrap();

        indexer.flush_all().unwrap();

        assert_eq!(indexer.manifest(&ns_a).unwrap().segments.len(), 1);
        assert_eq!(indexer.manifest(&ns_b).unwrap().segments.len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
