//! Shared types used across all Kosha crates.
//!
//! Maps to the data model in DESIGN.md §6.

use serde::{Deserialize, Serialize};

// ─── Identifiers ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DocumentId(pub String);

// ─── Document model ────────────────────────────────────────────────────────

/// The type of a document field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    Text,
    Keyword,
    Integer,
    Float,
    Date,
    Boolean,
}

/// A field in a document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
    pub value: String,
}

impl Field {
    pub fn text(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self { name: name.into(), field_type: FieldType::Text, value: value.into() }
    }

    pub fn keyword(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self { name: name.into(), field_type: FieldType::Keyword, value: value.into() }
    }

    pub fn integer(name: impl Into<String>, value: i64) -> Self {
        Self { name: name.into(), field_type: FieldType::Integer, value: value.to_string() }
    }

    pub fn float_val(name: impl Into<String>, value: f64) -> Self {
        Self { name: name.into(), field_type: FieldType::Float, value: value.to_string() }
    }

    pub fn date_val(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self { name: name.into(), field_type: FieldType::Date, value: value.into() }
    }

    pub fn boolean(name: impl Into<String>, value: bool) -> Self {
        Self { name: name.into(), field_type: FieldType::Boolean, value: value.to_string() }
    }
}

/// A document to be indexed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: DocumentId,
    pub fields: Vec<Field>,
}

// ─── Inverted index types ──────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Term(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Posting {
    pub doc_id: u32,
    pub term_frequency: u32,
}

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

// ─── Filter types ──────────────────────────────────────────────────────────

/// A single filter clause, matching the ES filter DSL shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FilterClause {
    Term { term: std::collections::HashMap<String, String> },
    Terms { terms: std::collections::HashMap<String, Vec<String>> },
    Range { range: std::collections::HashMap<String, RangeBound> },
    Bool { bool: BoolFilter },
    MatchAll { match_all: Option<serde_json::Value> },
}

/// Range bounds for a range filter clause.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeBound {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gte: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lte: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lt: Option<String>,
}

/// A bool filter combining multiple clauses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoolFilter {
    #[serde(default)]
    pub must: Vec<FilterClause>,
    #[serde(default)]
    pub must_not: Vec<FilterClause>,
    #[serde(default)]
    pub should: Vec<FilterClause>,
    #[serde(default = "default_minimum_should_match")]
    pub minimum_should_match: usize,
}

fn default_minimum_should_match() -> usize { 1 }

/// Sort order for a field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortOrder {
    pub order: String,
}

/// A sort specification: field name → order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortSpec {
    #[serde(flatten)]
    pub fields: std::collections::HashMap<String, SortOrder>,
}

/// Highlight configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighlightConfig {
    pub field: String,
    #[serde(default = "default_pre_tag")]
    pub pre_tags: Vec<String>,
    #[serde(default = "default_post_tag")]
    pub post_tags: Vec<String>,
}

fn default_pre_tag() -> Vec<String> { vec!["<b>".into()] }
fn default_post_tag() -> Vec<String> { vec!["</b>".into()] }

// ─── Segment metadata ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocRecord {
    pub doc_id: DocumentId,
    pub doc_seq: u32,
    pub field_length: u32,
    pub fields: Vec<Field>,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub segment_id: SegmentId,
    pub doc_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    pub segments: Vec<ManifestEntry>,
}

// ─── Query / result types ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    pub query_text: String,
    #[serde(default = "default_max_results")]
    pub max_results: usize,
    #[serde(default)]
    pub from: usize,
    #[serde(default)]
    pub bm25_params: Bm25Params,
    #[serde(default)]
    pub filter: Option<FilterClause>,
    #[serde(default)]
    pub sort: Vec<SortSpec>,
    #[serde(default)]
    pub highlight: Option<HighlightConfig>,
}

fn default_max_results() -> usize { 10 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredDocument {
    pub doc_id: DocumentId,
    pub score: f64,
    pub fields: Vec<Field>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highlights: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub results: Vec<ScoredDocument>,
    pub total_hits: usize,
}

// ─── Indexing types ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRequest {
    pub namespace: NamespaceId,
    pub documents: Vec<Document>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexResponse {
    pub indexed_count: usize,
    pub namespace: NamespaceId,
}

// ─── Errors ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum KoshaError {
    NamespaceNotFound(NamespaceId),
    SegmentNotFound(SegmentId),
    Io(std::io::Error),
    Serde(serde_json::Error),
    NotFound(String),
    InvalidFilter(String),
}

impl std::fmt::Display for KoshaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NamespaceNotFound(id) => write!(f, "namespace not found: {}", id.0),
            Self::SegmentNotFound(id) => write!(f, "segment not found: {}", id.0),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Serde(e) => write!(f, "serialization error: {e}"),
            Self::NotFound(msg) => write!(f, "{msg}"),
            Self::InvalidFilter(msg) => write!(f, "invalid filter: {msg}"),
        }
    }
}

impl std::error::Error for KoshaError {}

impl From<std::io::Error> for KoshaError { fn from(e: std::io::Error) -> Self { Self::Io(e) } }
impl From<serde_json::Error> for KoshaError { fn from(e: serde_json::Error) -> Self { Self::Serde(e) } }

// ─── Filter value store (in-memory representation) ─────────────────────────

/// Filter values for a segment, keyed by field name.
#[derive(Debug, Clone, Default)]
pub struct FilterStore {
    /// String (keyword, text) filter values: field → (doc_seq → value)
    pub string_fields: std::collections::HashMap<String, Vec<(u32, String)>>,
    /// Integer filter values: field → (doc_seq → value)
    pub integer_fields: std::collections::HashMap<String, Vec<(u32, i64)>>,
    /// Float filter values: field → (doc_seq → value)
    pub float_fields: std::collections::HashMap<String, Vec<(u32, f64)>>,
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
    fn field_constructors() {
        let f = Field::text("title", "hello world");
        assert_eq!(f.field_type, FieldType::Text);

        let f = Field::keyword("matterId", "matter-123");
        assert_eq!(f.field_type, FieldType::Keyword);

        let f = Field::integer("pageNumber", 42);
        assert_eq!(f.field_type, FieldType::Integer);

        let f = Field::boolean("hasRedlines", true);
        assert_eq!(f.field_type, FieldType::Boolean);
    }

    #[test]
    fn filter_clause_serde() {
        let json = r#"{"term": {"matterId": "matter-123"}}"#;
        let f: FilterClause = serde_json::from_str(json).unwrap();
        match f {
            FilterClause::Term { ref term } => {
                assert_eq!(term.get("matterId").unwrap(), "matter-123");
            }
            _ => panic!("expected Term"),
        }

        let json = r#"{"terms": {"documentId": ["d1", "d2"]}}"#;
        let f: FilterClause = serde_json::from_str(json).unwrap();
        match f {
            FilterClause::Terms { ref terms } => {
                assert_eq!(terms.get("documentId").unwrap().len(), 2);
            }
            _ => panic!("expected Terms"),
        }

        let json = r#"{"range": {"sentAt": {"gte": "2024-01-01", "lte": "2024-12-31"}}}"#;
        let f: FilterClause = serde_json::from_str(json).unwrap();
        match f {
            FilterClause::Range { ref range } => {
                let b = range.get("sentAt").unwrap();
                assert_eq!(b.gte.as_deref(), Some("2024-01-01"));
                assert_eq!(b.lte.as_deref(), Some("2024-12-31"));
            }
            _ => panic!("expected Range"),
        }

        let json = r#"{"bool": {"must": [{"term": {"matterId": "x"}}], "must_not": [{"term": {"status": "deleted"}}]}}"#;
        let f: FilterClause = serde_json::from_str(json).unwrap();
        match f {
            FilterClause::Bool { ref bool } => {
                assert_eq!(bool.must.len(), 1);
                assert_eq!(bool.must_not.len(), 1);
            }
            _ => panic!("expected Bool"),
        }
    }
}
