use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ─── Control store trait (in-memory or Postgres) ──────────────────────────

/// Pluggable namespace registry and manifest store.
///
/// In-memory (`kosha_control::Controller`) is the default.
/// Postgres (`kosha_control::PgStore`) is used in production via the
/// `postgres` feature and a `DATABASE_URL` env var.
pub trait ControlStore: Send + Sync {
    fn create_namespace(&mut self, id: NamespaceId) -> Result<(), KoshaError>;
    fn ensure_namespace(&mut self, id: NamespaceId);
    fn has_namespace(&self, id: &NamespaceId) -> bool;
    fn manifest(&self, id: &NamespaceId) -> Option<&Manifest>;
    fn manifest_mut(&mut self, id: &NamespaceId) -> Option<&mut Manifest>;

    /// Owned copy of the manifest for a namespace.
    ///
    /// Default implementation clones `manifest()`; stores that cannot return
    /// a borrowed reference (e.g. Postgres) override this to read through.
    fn manifest_cloned(&self, id: &NamespaceId) -> Option<Manifest> {
        self.manifest(id).cloned()
    }

    /// Persist a manifest for a namespace (upsert, last-write-wins).
    ///
    /// Used by the node to publish the current segment list after flush so
    /// the state survives restarts. For multi-writer publish with optimistic
    /// concurrency, use `compare_and_swap_manifest` instead.
    fn save_manifest(&mut self, id: &NamespaceId, manifest: &Manifest) -> Result<(), KoshaError>;

    fn compare_and_swap_manifest(
        &mut self,
        id: &NamespaceId,
        expected_version: u64,
        new_manifest: Manifest,
    ) -> Result<(), KoshaError>;
    fn list_namespaces(&self) -> Vec<NamespaceId>;
    fn namespace_count(&self) -> usize;
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// Overwrite `path` atomically: write to a sibling temp file, then `rename`
/// over the destination. POSIX (and Windows, via the same
/// `std::fs::rename`-backed semantics) guarantees a rename within one
/// directory is atomic — a reader always sees either the old complete file
/// or the new complete one, never a truncated/partial one.
///
/// `std::fs::write` (what this replaces) truncates the destination before
/// filling it back in. That's fine for a brand-new path nobody else can see
/// yet, but every `StorageBackend::write` call here can also be an in-place
/// rewrite of a file that's already published and being concurrently read
/// (a segment mid-search, a WAL file mid-recovery) — or can simply be
/// interrupted mid-write (process kill, OOM). Either way a reader landing in
/// the truncate-then-fill window, or a later read of a write that never
/// finished, sees a short file. For JSON that surfaces as `serde_json`
/// failing with "EOF while parsing a value"; for a segment's binary files it
/// silently corrupts data that then gets faithfully copied to S3 by whatever
/// syncs the segment afterward, baking the corruption into the source of
/// truth. See PR #45 (`kosha-segment`'s narrower fix for the same class of
/// bug in the footer-rewrite admin paths) for the incident this generalizes.
///
/// The temp filename includes the PID and a per-process atomic counter so
/// concurrent writes to the *same* path never collide on the same temp file.
fn atomic_write(path: &std::path::Path, data: &[u8]) -> Result<(), KoshaError> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let dir = path.parent().ok_or_else(|| {
        KoshaError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("atomic_write: {} has no parent directory", path.display()),
        ))
    })?;
    let file_name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
        KoshaError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("atomic_write: {} has no file name", path.display()),
        ))
    })?;
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_path = dir.join(format!(".{file_name}.tmp.{}.{n}", std::process::id()));

    std::fs::write(&tmp_path, data)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

impl StorageBackend for LocalStorage {
    fn read(&self, path: &str) -> Result<Vec<u8>, KoshaError> {
        Ok(std::fs::read(self.root.join(path))?)
    }
    fn write(&self, path: &str, data: &[u8]) -> Result<(), KoshaError> {
        atomic_write(&self.root.join(path), data)
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
    /// Per string-filter-field bloom filters for segment pruning.
    ///
    /// `None` means a legacy segment written before blooms existed — callers
    /// must not prune. `Some(map)` means field inventory is known: a missing
    /// field key means the segment has no values for that field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_blooms: Option<HashMap<String, BloomFilter>>,
    /// Bloom over the segment's inverted-index vocabulary (tokenized query
    /// terms). Used to skip segments that cannot contain required BM25 terms
    /// without opening `inverted.idx`.
    ///
    /// `None` means a legacy segment — callers must not prune on query terms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub term_bloom: Option<BloomFilter>,
    /// Segment format version. `0` (the `#[serde(default)]`) means a legacy
    /// segment written before `doc_store.offsets` existed — readers must not
    /// assume that sidecar is present or trustworthy even if it happens to
    /// exist on disk. `1` means the writer emitted `doc_store.offsets`
    /// alongside `doc_store.bin`, enabling lazy per-document loading instead
    /// of parsing the whole segment into memory to open it. `2` means
    /// `inverted.idx` is in the v2 table-of-contents layout
    /// (`kosha_segment::LazyInvertedIndex`), read lazily with zero parsing
    /// at open. Informational for `inverted.idx` — that file self-describes
    /// via a magic prefix, and readers fall back to the v1 stream parse for
    /// older segments regardless of this number.
    #[serde(default)]
    pub format_version: u32,
}

/// Current segment format version written by `SegmentWriter`. See
/// `Footer::format_version`.
pub const SEGMENT_FORMAT_VERSION: u32 = 2;

// ─── Bloom filter (segment pruning) ────────────────────────────────────────

/// Target false-positive rate when sizing blooms at write time.
pub const BLOOM_TARGET_FPR: f64 = 0.01;
/// Cap on bloom bitset size per field (keeps footer.json bounded).
pub const BLOOM_MAX_BYTES: usize = 64 * 1024;

/// Compact bloom filter over string filter values in one segment field.
///
/// Negatives are definitive (safe to skip the segment). Positives still require
/// opening `filters.bin` and applying the real predicate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BloomFilter {
    /// Base64-encoded bitset.
    pub bits_b64: String,
    /// Number of bits in the bitset.
    pub num_bits: u32,
    /// Number of hash functions.
    pub k: u8,
}

impl BloomFilter {
    /// Build a bloom from unique string values at ~[`BLOOM_TARGET_FPR`].
    pub fn build<'a, I>(values: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        let unique: Vec<&str> = {
            let mut seen = HashMap::new();
            for v in values {
                seen.insert(v, ());
            }
            seen.into_keys().collect()
        };
        let n = unique.len().max(1);
        let mut num_bits = bloom_num_bits(n, BLOOM_TARGET_FPR);
        let max_bits = BLOOM_MAX_BYTES * 8;
        if num_bits > max_bits {
            num_bits = max_bits;
        }
        num_bits = num_bits.max(64);
        let k = bloom_num_hashes(num_bits, n).clamp(1, 16) as u8;
        let mut bits = vec![0u8; num_bits.div_ceil(8)];
        let mut filter = Self {
            bits_b64: String::new(),
            num_bits: num_bits as u32,
            k,
        };
        for v in unique {
            filter.insert_into(&mut bits, v);
        }
        filter.bits_b64 = encode_base64(&bits);
        filter
    }

    pub fn may_contain(&self, value: &str) -> bool {
        let bits = match decode_base64(&self.bits_b64) {
            Some(b) if !b.is_empty() && self.num_bits > 0 => b,
            _ => return true, // corrupt/empty → cannot prune
        };
        let m = self.num_bits as u64;
        let (h1, h2) = bloom_hashes(value);
        for i in 0..self.k {
            let bit = h1.wrapping_add((i as u64).wrapping_mul(h2)) % m;
            let idx = (bit / 8) as usize;
            let mask = 1u8 << (bit % 8);
            if idx >= bits.len() || bits[idx] & mask == 0 {
                return false;
            }
        }
        true
    }

    fn insert_into(&self, bits: &mut [u8], value: &str) {
        let m = self.num_bits as u64;
        let (h1, h2) = bloom_hashes(value);
        for i in 0..self.k {
            let bit = h1.wrapping_add((i as u64).wrapping_mul(h2)) % m;
            let idx = (bit / 8) as usize;
            let mask = 1u8 << (bit % 8);
            if idx < bits.len() {
                bits[idx] |= mask;
            }
        }
    }
}

/// Build per-field blooms from columnar string filter entries.
pub fn build_filter_blooms(
    string_fields: &HashMap<String, Vec<(u32, String)>>,
) -> HashMap<String, BloomFilter> {
    let mut out = HashMap::new();
    for (field, entries) in string_fields {
        if entries.is_empty() {
            continue;
        }
        let bloom = BloomFilter::build(entries.iter().map(|(_, v)| v.as_str()));
        out.insert(field.clone(), bloom);
    }
    out
}

/// Build a bloom over a segment's inverted-index term vocabulary.
pub fn build_term_bloom<'a, I>(terms: I) -> BloomFilter
where
    I: IntoIterator<Item = &'a str>,
{
    BloomFilter::build(terms)
}

/// How [`segment_may_contain_terms`] combines multiple query terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermBloomMode {
    /// All terms must possibly be present (multi-term BM25 / phrase).
    And,
    /// At least one term must possibly be present (wildcard OR expansion).
    Or,
}

/// Whether a segment with the given term bloom might contain `terms`.
///
/// Returns `true` when the segment must be opened (possible match, empty
/// term list, or legacy/`None` bloom). Returns `false` only when the bloom
/// proves the segment cannot satisfy the term constraint.
pub fn segment_may_contain_terms(
    terms: &[String],
    mode: TermBloomMode,
    bloom: Option<&BloomFilter>,
) -> bool {
    if terms.is_empty() {
        return true;
    }
    let Some(bloom) = bloom else {
        return true; // legacy footer — cannot prune
    };
    match mode {
        TermBloomMode::And => terms.iter().all(|t| bloom.may_contain(t)),
        TermBloomMode::Or => terms.iter().any(|t| bloom.may_contain(t)),
    }
}

fn bloom_num_bits(n: usize, fpr: f64) -> usize {
    if n == 0 {
        return 64;
    }
    let n = n as f64;
    let bits = (-(n) * fpr.ln() / (2f64.ln().powi(2))).ceil() as usize;
    bits.max(64)
}

fn bloom_num_hashes(num_bits: usize, n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    ((num_bits as f64 / n as f64) * 2f64.ln()).round() as usize
}

/// Stable FNV-1a based double hash (not `DefaultHasher`, which is not portable).
fn bloom_hashes(value: &str) -> (u64, u64) {
    let h1 = fnv1a64(value.as_bytes(), 0x9e3779b97f4a7c15);
    let h2 = fnv1a64(value.as_bytes(), 0xbf58476d1ce4e5b9) | 1; // odd → full period
    (h1, h2)
}

fn fnv1a64(data: &[u8], seed: u64) -> u64 {
    let mut hash = 0xcbf29ce484222325u64 ^ seed;
    for &b in data {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn encode_base64(data: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn decode_base64(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            b'=' => Some(0),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let (a, b, c, d) = (
            val(chunk[0])?,
            val(chunk[1])?,
            val(chunk[2])?,
            val(chunk[3])?,
        );
        let n = (u32::from(a) << 18) | (u32::from(b) << 12) | (u32::from(c) << 6) | u32::from(d);
        out.push(((n >> 16) & 0xff) as u8);
        if chunk[2] != b'=' {
            out.push(((n >> 8) & 0xff) as u8);
        }
        if chunk[3] != b'=' {
            out.push((n & 0xff) as u8);
        }
    }
    Some(out)
}

/// Whether a segment with the given footer blooms might satisfy `filter`.
///
/// Returns `true` when the segment must be opened (possible match, or not
/// enough bloom metadata to decide). Returns `false` only when blooms prove
/// no document in the segment can match.
pub fn segment_may_match(
    filter: &FilterClause,
    blooms: Option<&HashMap<String, BloomFilter>>,
) -> bool {
    let Some(blooms) = blooms else {
        return true; // legacy footer — cannot prune
    };
    match filter {
        // Term/Terms field maps are OR'd by FilterApplier — skip only if every
        // alternative is impossible.
        FilterClause::Term { term } => {
            term.is_empty()
                || term.iter().any(|(field, value)| match blooms.get(field) {
                    Some(bloom) => bloom.may_contain(value),
                    None => false,
                })
        }
        FilterClause::Terms { terms } => {
            terms.is_empty()
                || terms.iter().any(|(field, values)| match blooms.get(field) {
                    Some(bloom) => values.iter().any(|v| bloom.may_contain(v)),
                    None => false,
                })
        }
        FilterClause::Bool { bool: b } => {
            for child in &b.must {
                if !segment_may_match(child, Some(blooms)) {
                    return false;
                }
            }
            // FilterApplier always intersects should when non-empty.
            if !b.should.is_empty() && b.minimum_should_match > 0 {
                let matching = b
                    .should
                    .iter()
                    .filter(|c| segment_may_match(c, Some(blooms)))
                    .count();
                if matching < b.minimum_should_match {
                    return false;
                }
            }
            // must_not cannot prove a segment has no matches via bloom.
            true
        }
        FilterClause::Range { .. } | FilterClause::MatchAll { .. } => true,
    }
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
    /// Optional per-segment footer snapshot for search-time pruning/open.
    ///
    /// Older persisted manifests do not contain this field; readers fall
    /// back to `footer.json` when an entry is missing.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub segment_footers: HashMap<SegmentId, Footer>,
}

impl Manifest {
    pub fn segment_footer(&self, segment_id: &SegmentId) -> Option<&Footer> {
        self.segment_footers.get(segment_id)
    }

    pub fn remember_segment_footer(&mut self, footer: Footer) {
        self.segment_footers
            .insert(footer.segment_id.clone(), footer);
    }
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
    /// OpenSearch-compatible search_after cursor. Values align with `sort`
    /// (or `_id` when sorting by document id). Results strictly after this
    /// cursor in sort order are returned.
    #[serde(default)]
    pub search_after: Option<Vec<String>>,
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
    /// `None` (default) defers to the engine-wide `KOSHA_EXACT_TOTAL_HITS`
    /// setting (itself `false` unless set) — `total_hits` is capped at
    /// `total_hits_cap` and reported with `relation: "gte"` once exceeded,
    /// OpenSearch `track_total_hits`-style. `Some(true)` forces an exact
    /// count for this query regardless of the engine default: every AND-join
    /// intersection member is visited (no early exit), which is the cost a
    /// broad multi-term query pays to get an exact number nobody reads past
    /// page 1 of. See [`TotalHitsRelation`].
    #[serde(default)]
    pub exact_total_hits: Option<bool>,
    /// Per-query override for the capped-count threshold (only consulted
    /// when `total_hits` isn't being counted exactly). `None` defers to the
    /// engine-wide `KOSHA_TOTAL_HITS_CAP` setting (default `10_000`).
    #[serde(default)]
    pub total_hits_cap: Option<usize>,
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

/// Whether `SearchResult.total_hits` is the exact match count or a capped
/// lower bound (OpenSearch/Elasticsearch `hits.total.relation` naming).
/// See `SearchQuery.exact_total_hits`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TotalHitsRelation {
    Eq,
    Gte,
}
fn default_total_hits_relation() -> TotalHitsRelation {
    TotalHitsRelation::Eq
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub results: Vec<ScoredDocument>,
    pub total_hits: usize,
    #[serde(default = "default_total_hits_relation")]
    pub total_hits_relation: TotalHitsRelation,
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
    /// A segment's sidecar index (e.g. `doc_store.offsets`) exists but fails
    /// a sanity check (bad header, doc-count mismatch, truncated file).
    /// Callers should treat this as a signal to fall back to the legacy
    /// full-parse path for that component, not as a fatal error for the
    /// whole segment.
    CorruptSegment(String),
    /// The server is shedding load: admitting this request would push live
    /// segment memory past the configured watermark and it did not free up
    /// within the admission timeout (see `kosha_query::MemoryLedger`).
    /// Transient by construction — callers should retry with backoff, which
    /// the Python `kosha_client` already does. Maps to HTTP 429.
    Overloaded(String),
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
            Self::CorruptSegment(msg) => write!(f, "corrupt segment data: {msg}"),
            Self::Overloaded(msg) => write!(f, "overloaded: {msg}"),
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

    #[test]
    fn bloom_contains_inserted_values() {
        let bloom = BloomFilter::build(["m1", "m2", "m3"]);
        assert!(bloom.may_contain("m1"));
        assert!(bloom.may_contain("m2"));
        assert!(bloom.may_contain("m3"));
        assert!(!bloom.may_contain("matter-definitely-absent-xyz"));
    }

    #[test]
    fn bloom_roundtrips_through_serde() {
        let bloom = BloomFilter::build(["matter-a"]);
        let json = serde_json::to_string(&bloom).unwrap();
        let back: BloomFilter = serde_json::from_str(&json).unwrap();
        assert!(back.may_contain("matter-a"));
        assert!(!back.may_contain("matter-absent"));
    }

    #[test]
    fn footer_legacy_without_blooms_deserializes() {
        let json = r#"{
            "segment_id": "s1",
            "doc_count": 1,
            "total_field_length": 3,
            "avg_field_length": 3.0,
            "bm25_params": {"k1": 1.2, "b": 0.75},
            "created_at": "2026-01-01T00:00:00Z"
        }"#;
        let footer: Footer = serde_json::from_str(json).unwrap();
        assert!(footer.filter_blooms.is_none());
        assert!(footer.term_bloom.is_none());
    }

    #[test]
    fn segment_may_contain_terms_and_or() {
        let bloom = build_term_bloom(["contract", "warranty"]);
        let both = vec!["contract".into(), "warranty".into()];
        // Distinctive absent tokens avoid rare bloom false positives at 1% FPR.
        let absent = "definitely-absent-term-xyz-qqq";
        let missing = vec!["contract".into(), absent.into()];
        let only_absent = vec![absent.into()];
        assert!(segment_may_contain_terms(
            &both,
            TermBloomMode::And,
            Some(&bloom)
        ));
        assert!(!segment_may_contain_terms(
            &missing,
            TermBloomMode::And,
            Some(&bloom)
        ));
        assert!(segment_may_contain_terms(
            &missing,
            TermBloomMode::Or,
            Some(&bloom)
        ));
        assert!(!segment_may_contain_terms(
            &only_absent,
            TermBloomMode::Or,
            Some(&bloom)
        ));
        assert!(
            segment_may_contain_terms(&missing, TermBloomMode::And, None),
            "legacy footer never pruned"
        );
    }

    #[test]
    fn segment_may_match_prunes_absent_matter() {
        let blooms = HashMap::from([("matterId".into(), BloomFilter::build(["m1"]))]);
        let keep = FilterClause::Term {
            term: HashMap::from([("matterId".into(), "m1".into())]),
        };
        let skip = FilterClause::Term {
            term: HashMap::from([("matterId".into(), "m2".into())]),
        };
        assert!(segment_may_match(&keep, Some(&blooms)));
        assert!(!segment_may_match(&skip, Some(&blooms)));
        assert!(segment_may_match(&skip, None), "legacy footer never pruned");
    }

    #[test]
    fn segment_may_match_terms_and_bool() {
        let blooms = HashMap::from([
            ("matterId".into(), BloomFilter::build(["m1"])),
            ("tag".into(), BloomFilter::build(["alpha"])),
        ]);
        let terms = FilterClause::Terms {
            terms: HashMap::from([("matterId".into(), vec!["m2".into(), "m1".into()])]),
        };
        assert!(segment_may_match(&terms, Some(&blooms)));

        let terms_miss = FilterClause::Terms {
            terms: HashMap::from([("matterId".into(), vec!["m9".into()])]),
        };
        assert!(!segment_may_match(&terms_miss, Some(&blooms)));

        let must = FilterClause::Bool {
            bool: BoolFilter {
                must: vec![
                    FilterClause::Term {
                        term: HashMap::from([("matterId".into(), "m1".into())]),
                    },
                    FilterClause::Term {
                        term: HashMap::from([("tag".into(), "missing".into())]),
                    },
                ],
                must_not: vec![],
                should: vec![],
                minimum_should_match: 1,
            },
        };
        assert!(!segment_may_match(&must, Some(&blooms)));
    }

    /// Regression test for the staging incident this generalizes PR #45's
    /// fix for: `SegmentWriter::finalize()` (and every other caller of
    /// `StorageBackend::write`, e.g. WAL, the S3-hydration local cache
    /// mirror) rewrites files through `LocalStorage::write`. A reader
    /// racing a rewrite of the same path must never observe a
    /// truncated/torn file — before `atomic_write`, plain `fs::write`
    /// (truncate then fill) could hand back anywhere from 0 bytes up to a
    /// partial mix, which is exactly how staging ended up with 0-byte
    /// `doc_store.bin`/`footer.json` in already-published, already-synced
    /// segments.
    #[test]
    fn concurrent_local_storage_write_never_yields_truncated_read() {
        let dir = std::env::temp_dir().join("kosha-core-test-atomic-write-race");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let content_a = vec![b'a'; 5_000];
        let content_b = vec![b'b'; 9_000];
        LocalStorage::new(dir.clone())
            .write("segment.bin", &content_a)
            .unwrap();

        let (writer_dir, reader_dir) = (dir.clone(), dir.clone());
        let (a, b) = (content_a.clone(), content_b.clone());
        let writer = std::thread::spawn(move || {
            let backend = LocalStorage::new(writer_dir);
            for i in 0..200 {
                let data = if i % 2 == 0 { &a } else { &b };
                backend.write("segment.bin", data).unwrap();
            }
        });
        let reader = std::thread::spawn(move || {
            let backend = LocalStorage::new(reader_dir);
            for _ in 0..200 {
                let data = backend.read("segment.bin").unwrap();
                assert!(
                    data == content_a || data == content_b,
                    "read a torn/truncated write: {} bytes (expected {} or {})",
                    data.len(),
                    content_a.len(),
                    content_b.len()
                );
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();

        let leftover: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(
            leftover.is_empty(),
            "leftover temp files after atomic_write: {leftover:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
