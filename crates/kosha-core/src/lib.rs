use serde::{Deserialize, Serialize};

// ─── Identifiers ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceId(pub String);
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentId(pub String);
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DocumentId(pub String);

// ─── Document model ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    Text,
    Keyword,
    Integer,
    Float,
    Date,
    Boolean,
    Vector,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
    pub value: String,
}

impl Field {
    pub fn text(n: impl Into<String>, v: impl Into<String>) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Text,
            value: v.into(),
        }
    }
    pub fn keyword(n: impl Into<String>, v: impl Into<String>) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Keyword,
            value: v.into(),
        }
    }
    pub fn integer(n: impl Into<String>, v: i64) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Integer,
            value: v.to_string(),
        }
    }
    pub fn float_val(n: impl Into<String>, v: f64) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Float,
            value: v.to_string(),
        }
    }
    pub fn date_val(n: impl Into<String>, v: impl Into<String>) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Date,
            value: v.into(),
        }
    }
    pub fn boolean(n: impl Into<String>, v: bool) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Boolean,
            value: v.to_string(),
        }
    }
    pub fn vector(n: impl Into<String>, v: Vec<f32>) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Vector,
            value: serde_json::to_string(&v).unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: DocumentId,
    pub fields: Vec<Field>,
}

// ─── Inverted index types (with positions for phrase queries) ──────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Term(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Posting {
    pub doc_id: u32,
    pub term_frequency: u32,
    pub positions: Vec<u32>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FilterClause {
    Term {
        term: std::collections::HashMap<String, String>,
    },
    Terms {
        terms: std::collections::HashMap<String, Vec<String>>,
    },
    Range {
        range: std::collections::HashMap<String, RangeBound>,
    },
    Bool {
        bool: BoolFilter,
    },
    MatchAll {
        match_all: Option<serde_json::Value>,
    },
}

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
fn default_minimum_should_match() -> usize {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortOrder {
    pub order: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortSpec {
    #[serde(flatten)]
    pub fields: std::collections::HashMap<String, SortOrder>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighlightConfig {
    pub field: String,
    #[serde(default = "default_pre_tag")]
    pub pre_tags: Vec<String>,
    #[serde(default = "default_post_tag")]
    pub post_tags: Vec<String>,
}
fn default_pre_tag() -> Vec<String> {
    vec!["<b>".into()]
}
fn default_post_tag() -> Vec<String> {
    vec!["</b>".into()]
}

// ─── Wildcard query ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WildcardQuery {
    pub field: String,
    pub pattern: String,
    #[serde(default = "default_case_insensitive")]
    pub case_insensitive: bool,
}
fn default_case_insensitive() -> bool {
    true
}

// ─── Match phrase query ────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchPhraseQuery {
    pub field: String,
    pub phrase: String,
    #[serde(default)]
    pub slop: u32,
}

// ─── Aggregation types ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Aggregation {
    Terms { terms: AggTerms },
    Cardinality { cardinality: AggCardinality },
    Composite { composite: AggComposite },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggTerms {
    pub field: String,
    #[serde(default = "default_agg_size")]
    pub size: usize,
}
fn default_agg_size() -> usize {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCardinality {
    pub field: String,
    #[serde(default = "default_precision")]
    pub precision_threshold: usize,
}
fn default_precision() -> usize {
    40000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggComposite {
    pub size: usize,
    pub sources: Vec<AggCompositeSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCompositeSource {
    #[serde(flatten)]
    pub source: std::collections::HashMap<String, AggCompositeTerms>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCompositeTerms {
    pub terms: AggCompositeField,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCompositeField {
    pub field: String,
}

/// Aggregation results keyed by aggregation name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationResults {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub per_document: Option<AggBucketResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_documents: Option<AggMetricResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_docs: Option<AggCompositeResult>,
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggBucketResult {
    pub buckets: Vec<AggBucket>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggBucket {
    pub key: String,
    pub doc_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggMetricResult {
    pub value: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCompositeResult {
    pub buckets: Vec<AggCompositeBucket>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_key: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCompositeBucket {
    pub key: std::collections::HashMap<String, String>,
    pub doc_count: usize,
}

// ─── Storage abstraction (local fs / S3 / etc.) ───────────────────────────

/// Storage backends implement this to provide file I/O for segments.
/// Kosha's core uses this instead of std::fs directly, so S3 / GCS / etc.
/// can be plugged in without modifying core code.
pub trait StorageBackend: std::fmt::Debug + Send + Sync {
    fn read(&self, path: &str) -> Result<Vec<u8>, KoshaError>;
    fn write(&self, path: &str, data: &[u8]) -> Result<(), KoshaError>;
    fn exists(&self, path: &str) -> bool;
    fn delete(&self, path: &str) -> Result<(), KoshaError>;
    fn list(&self, path: &str) -> Result<Vec<String>, KoshaError>;
    fn create_dir_all(&self, path: &str) -> Result<(), KoshaError>;
}

/// Local filesystem implementation of StorageBackend.
#[derive(Debug, Clone)]
pub struct LocalStorage {
    pub root: std::path::PathBuf,
}

impl LocalStorage {
    pub fn new(root: std::path::PathBuf) -> Self {
        Self { root }
    }
}

impl StorageBackend for LocalStorage {
    fn read(&self, path: &str) -> Result<Vec<u8>, KoshaError> {
        Ok(std::fs::read(self.root.join(path))?)
    }
    fn write(&self, path: &str, data: &[u8]) -> Result<(), KoshaError> {
        Ok(std::fs::write(self.root.join(path), data)?)
    }
    fn exists(&self, path: &str) -> bool {
        self.root.join(path).exists()
    }
    fn delete(&self, path: &str) -> Result<(), KoshaError> {
        let p = self.root.join(path);
        if p.is_dir() {
            std::fs::remove_dir_all(&p)?;
        } else {
            std::fs::remove_file(&p)?;
        }
        Ok(())
    }
    fn list(&self, path: &str) -> Result<Vec<String>, KoshaError> {
        let dir = self.root.join(path);
        let mut entries = Vec::new();
        if dir.exists() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                if let Some(name) = entry.file_name().to_str() {
                    entries.push(name.to_string());
                }
            }
        }
        Ok(entries)
    }
    fn create_dir_all(&self, path: &str) -> Result<(), KoshaError> {
        Ok(std::fs::create_dir_all(self.root.join(path))?)
    }
}

// ─── WAL (Write-Ahead Log) types ──────────────────────────────────────────

/// A single WAL record: a batch of documents for one namespace at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    pub namespace: NamespaceId,
    pub documents: Vec<Document>,
    pub timestamp: u64,
}

impl WalRecord {
    pub fn new(namespace: NamespaceId, documents: Vec<Document>) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Self {
            namespace,
            documents,
            timestamp,
        }
    }
}

/// Summary of a WAL file for recovery ordering.
#[derive(Debug, Clone)]
pub struct WalFileInfo {
    pub path: String,
    pub record_count: u32,
    pub first_timestamp: u64,
}

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

// ─── kNN query ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnnQuery {
    pub field: String,
    pub vector: Vec<f32>,
    #[serde(default = "default_knn_k")]
    pub k: usize,
    #[serde(default = "default_knn_num_candidates")]
    pub num_candidates: usize,
    #[serde(default)]
    pub filter: Option<FilterClause>,
}
fn default_knn_k() -> usize {
    10
}
fn default_knn_num_candidates() -> usize {
    100
}

/// Vector store for a segment: doc_seq → embedding vector.
#[derive(Debug, Clone, Default)]
pub struct VectorStore {
    pub vectors: Vec<(u32, Vec<f32>)>,
    pub dimensions: usize,
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
    #[serde(default)]
    pub aggs: std::collections::HashMap<String, Aggregation>,
    #[serde(default)]
    pub wildcard: Option<WildcardQuery>,
    #[serde(default)]
    pub match_phrase: Option<MatchPhraseQuery>,
    #[serde(default)]
    pub knn: Option<KnnQuery>,
}
fn default_max_results() -> usize {
    10
}

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<AggregationResults>,
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

// ─── Filter value store ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct FilterStore {
    pub string_fields: std::collections::HashMap<String, Vec<(u32, String)>>,
    pub integer_fields: std::collections::HashMap<String, Vec<(u32, i64)>>,
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
    fn posting_with_positions() {
        let p = Posting {
            doc_id: 0,
            term_frequency: 2,
            positions: vec![0, 3],
        };
        assert_eq!(p.positions.len(), 2);
        let json = serde_json::to_string(&p).unwrap();
        let back: Posting = serde_json::from_str(&json).unwrap();
        assert_eq!(back.positions, vec![0, 3]);
    }

    #[test]
    fn aggregation_serde() {
        let json = r#"{"per_document": {"terms": {"field": "documentId", "size": 1000}}}"#;
        let aggs: std::collections::HashMap<String, Aggregation> =
            serde_json::from_str(json).unwrap();
        assert!(aggs.contains_key("per_document"));

        let json = r#"{"total_documents": {"cardinality": {"field": "documentId"}}}"#;
        let aggs: std::collections::HashMap<String, Aggregation> =
            serde_json::from_str(json).unwrap();
        assert!(aggs.contains_key("total_documents"));
    }

    #[test]
    fn wildcard_query_serde() {
        let json = r#"{"field": "caseName", "pattern": "*Smith*", "case_insensitive": true}"#;
        let w: WildcardQuery = serde_json::from_str(json).unwrap();
        assert_eq!(w.pattern, "*Smith*");
        assert!(w.case_insensitive);
    }

    #[test]
    fn match_phrase_query_serde() {
        let json = r#"{"field": "content", "phrase": "tribal tax credit", "slop": 2}"#;
        let p: MatchPhraseQuery = serde_json::from_str(json).unwrap();
        assert_eq!(p.phrase, "tribal tax credit");
        assert_eq!(p.slop, 2);
    }
}
