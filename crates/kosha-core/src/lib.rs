//! Shared types used across all Kosha crates.
//!
//! Maps to the data model in DESIGN.md §6.

use serde::{Deserialize, Serialize};

// ─── Identifiers ───────────────────────────────────────────────────────────

/// Identifier for a namespace — the unit of tenant isolation and physical
/// layout (DESIGN.md §6.1).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceId(pub String);

/// Identifier for an immutable segment (DESIGN.md §6.2).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentId(pub String);

/// Identifier for a document within a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DocumentId(pub String);

// ─── Document model ────────────────────────────────────────────────────────

/// A field in a document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub text: String,
}

/// A document to be indexed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: DocumentId,
    pub fields: Vec<Field>,
}

// ─── Inverted index types ──────────────────────────────────────────────────

/// A term (token) in the inverted index.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Term(pub String);

/// A posting in a postings list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Posting {
    /// Local document ID within the segment.
    pub doc_id: u32,
    /// Term frequency in this document.
    pub term_frequency: u32,
}

/// BM25 scoring parameters (§8).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bm25Params {
    pub k1: f64,
    pub b: f64,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

// ─── Segment metadata ──────────────────────────────────────────────────────

/// Per-document record stored in the doc store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocRecord {
    pub doc_id: DocumentId,
    pub doc_seq: u32,
    /// Sum of token counts across all fields.
    pub field_length: u32,
    pub fields: Vec<Field>,
}

/// Metadata about a single segment, persisted in footer.json.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Footer {
    pub segment_id: SegmentId,
    pub doc_count: u32,
    pub total_field_length: u64,
    pub avg_field_length: f64,
    pub bm25_params: Bm25Params,
    pub created_at: String,
}

// ─── Manifest ──────────────────────────────────────────────────────────────

/// A single entry in the manifest, referencing one live segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub segment_id: SegmentId,
    pub doc_count: u32,
}

/// The manifest for a namespace: the set of live segments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    pub segments: Vec<ManifestEntry>,
}

// ─── Query / result types ──────────────────────────────────────────────────

/// A search query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    pub query_text: String,
    #[serde(default = "default_max_results")]
    pub max_results: usize,
    #[serde(default)]
    pub bm25_params: Bm25Params,
}

fn default_max_results() -> usize {
    10
}

/// A scored document result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredDocument {
    pub doc_id: DocumentId,
    pub score: f64,
    pub fields: Vec<Field>,
}

/// The result of a search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub results: Vec<ScoredDocument>,
    pub total_hits: usize,
}

// ─── Indexing types ────────────────────────────────────────────────────────

/// Request to index documents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRequest {
    pub namespace: NamespaceId,
    pub documents: Vec<Document>,
}

/// Response to an index request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexResponse {
    pub indexed_count: usize,
    pub namespace: NamespaceId,
}

// ─── Errors ────────────────────────────────────────────────────────────────

/// General Kosha error type.
#[derive(Debug)]
pub enum KoshaError {
    NamespaceNotFound(NamespaceId),
    SegmentNotFound(SegmentId),
    Io(std::io::Error),
    Serde(serde_json::Error),
    NotFound(String),
}

impl std::fmt::Display for KoshaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NamespaceNotFound(id) => write!(f, "namespace not found: {}", id.0),
            Self::SegmentNotFound(id) => write!(f, "segment not found: {}", id.0),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Serde(e) => write!(f, "serialization error: {e}"),
            Self::NotFound(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for KoshaError {}

impl From<std::io::Error> for KoshaError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for KoshaError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_wrap_strings() {
        let ns = NamespaceId("org1/matter42".to_string());
        let seg = SegmentId("seg-0001".to_string());
        assert_eq!(ns.0, "org1/matter42");
        assert_ne!(ns.0, seg.0);
    }

    #[test]
    fn bm25_params_has_sensible_defaults() {
        let p = Bm25Params::default();
        assert!((p.k1 - 1.2).abs() < 1e-10);
        assert!((p.b - 0.75).abs() < 1e-10);
    }

    #[test]
    fn search_query_defaults_to_10_results() {
        let q = SearchQuery {
            query_text: "hello".into(),
            max_results: default_max_results(),
            bm25_params: Bm25Params::default(),
        };
        assert_eq!(q.max_results, 10);
    }

    #[test]
    fn doc_record_round_trip() {
        let rec = DocRecord {
            doc_id: DocumentId("d1".into()),
            doc_seq: 0,
            field_length: 5,
            fields: vec![Field {
                name: "title".into(),
                text: "hello world".into(),
            }],
        };
        let json = serde_json::to_string(&rec).unwrap();
        let parsed: DocRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.doc_id, rec.doc_id);
    }
}
