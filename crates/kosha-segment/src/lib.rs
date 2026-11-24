pub mod analysis;
pub mod bitpack;
pub mod doc_store_zstd;
pub mod filter_columns;
pub mod filter_eval;
pub mod multivec;
pub mod reorder;
pub mod span_cache;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use instant_distance::{Builder, HnswMap, Point, Search};
use kosha_core::{
    build_filter_blooms, build_term_bloom, AggBucket, AggBucketResult, AggMetricResult,
    AggregationResults, Bm25Params, DocRecord, DocSet, DocumentId, Field, FieldType, FilterClause,
    FilterStore, Footer, KoshaError, LocalStorage, Posting, SegmentId, StorageBackend,
    VectorFieldMeta, VectorQuantization, VectorStore,
};
use kosha_core::{AnalysisConfig, AnalyzerSpec, SegmentAnalysis};

pub use analysis::Analyzer;
use doc_store_zstd::{compress_doc_store, parse_dict_trailer, rebuild_offsets, DocStoreDecoder};
pub use doc_store_zstd::{DocStoreCompressionMode, DocStoreZstdConfig, DOC_STORE_DICT_MAGIC};
pub use kosha_vector_spfresh::namespace_centroids;
pub use multivec::{LateInteractionHits, LateInteractionParams};
pub use reorder::{BpConfig, DocReorder};

/// A point in HNSW space using cosine distance.
#[derive(Clone)]
pub struct CosinePoint(pub Vec<f32>);

impl Point for CosinePoint {
    fn distance(&self, other: &Self) -> f32 {
        let dot: f32 = self.0.iter().zip(other.0.iter()).map(|(a, b)| a * b).sum();
        let na: f32 = self.0.iter().map(|x| x * x).sum::<f32>().sqrt();
        let nb: f32 = other.0.iter().map(|x| x * x).sum::<f32>().sqrt();
        if na == 0.0 || nb == 0.0 {
            return 1.0;
        }
        // Cosine distance = 1 - cosine_similarity, clamped to [0, 2]
        1.0 - (dot / (na * nb)).clamp(-1.0, 1.0)
    }
}

/// Build an HNSW index from a set of vectors.
/// Returns (HnswMap, Search) — map for searching, search for re-use.
pub fn build_hnsw(vectors: &[(u32, Vec<f32>)]) -> Option<(HnswMap<CosinePoint, u32>, Search)> {
    if vectors.is_empty() {
        return None;
    }
    let points: Vec<CosinePoint> = vectors
        .iter()
        .map(|(_, v)| CosinePoint(v.clone()))
        .collect();
    let values: Vec<u32> = vectors.iter().map(|(ds, _)| *ds).collect();
    let map = Builder::default().build(points, values);
    let search = Search::default();
    Some((map, search))
}

// ─── Segment writer ─────────────────────────────────────────────────────────

pub struct SegmentWriter {
    segment_id: SegmentId,
    output_dir: PathBuf,
    backend: Box<dyn StorageBackend>,
    /// Per-document BM25 field length, indexed by doc_seq. All the writer
    /// keeps per document: the stored record itself is streamed to disk as
    /// it is added (#242), not held until `finalize`.
    field_lengths: Vec<u32>,
    /// Streams `doc_store.bin` as documents are added. Opened lazily.
    doc_store: Option<DocStoreSink>,
    inverted_index: HashMap<String, Vec<Posting>>,
    /// Approximate heap bytes held by `inverted_index`.
    inverted_bytes: usize,
    /// Highest `inverted_bytes` seen (for tests and diagnostics).
    peak_inverted_bytes: usize,
    /// Spill `inverted_index` to a sorted run once `inverted_bytes` passes
    /// this. See [`segment_build_memory_bytes`].
    build_memory_bytes: usize,
    /// Inverted-index version `finalize` writes; see
    /// [`postings_write_version`].
    postings_write_version: u32,
    /// Leave every `postings-*.bin` shard raw (no zstd wrapper) so query
    /// nodes can range-read single terms (#245); recorded as
    /// `Footer::posting_blobs_raw`. See [`posting_blobs_raw`].
    posting_blobs_raw: bool,
    /// Sorted posting runs spilled so far, oldest (lowest doc_seq) first.
    runs: Vec<PathBuf>,
    /// First I/O error from streaming in `add_document`, which can't return
    /// one; reported by `finalize`.
    io_error: Option<KoshaError>,
    /// Set once `finalize` succeeds; otherwise `Drop` removes the partial
    /// build files.
    finalized: bool,
    /// Term bloom built by `write_inverted_index` from the merged vocabulary
    /// (the in-memory index alone no longer holds every term).
    term_bloom: Option<kosha_core::BloomFilter>,
    /// Deletion-neighbourhood bloom for fuzzy pruning (#201), built with
    /// `term_bloom`; `None` when the vocabulary is too large for one.
    fuzzy_bloom: Option<kosha_core::BloomFilter>,
    total_field_length: u64,
    filter_string: HashMap<String, Vec<(u32, String)>>,
    filter_integer: HashMap<String, Vec<(u32, i64)>>,
    filter_float: HashMap<String, Vec<(u32, f64)>>,
    /// Filter index format `finalize` writes: `1` = `filters.bin`, `2` =
    /// columnar (#244). See [`filter_columns::filters_write_version`].
    filters_write_version: u32,
    /// The column directory `write_filters` produced in columnar mode,
    /// recorded as `Footer::filter_columns`.
    filter_columns_meta: Option<Vec<kosha_core::FilterColumnMeta>>,
    /// Parsed `Vector` field values, one column per field name in
    /// first-seen order (#204). Column `i` is written to file slot `i` (see
    /// [`kosha_core::VectorFieldMeta::slot`]), so a single-column segment
    /// keeps the `vector.idx` + `vector.offsets` layout it always had.
    vector_columns: Vec<VectorColumn>,
    /// Vectors kept for `doc_vectors.bin` but not indexed: their dimension
    /// disagreed with the first vector seen for the same field in this
    /// segment. The write API rejects such documents up front; this only
    /// keeps a stray one from failing the whole flush.
    unindexed_vectors: Vec<Vec<f32>>,
    /// See [`text_filter_ignore_above`]. Captured per writer so tests can
    /// override it without touching the process-wide env.
    text_filter_ignore_above: Option<u32>,
    /// Where parsed `Vector` field values are stored (#243). See
    /// [`VectorStoreMode`].
    vector_store_mode: VectorStoreMode,
    /// `doc_vectors.bin` field-name table (sidecar mode only).
    doc_vector_fields: Vec<String>,
    /// How each vector column's posting entries are stored (#210). See
    /// [`VectorQuantizationConfig`].
    vector_quantization: VectorQuantizationConfig,
    /// `(doc_seq, field_idx, where the vector is buffered)` for each vector
    /// moved to the sidecar, in doc_seq then field order.
    doc_vector_entries: Vec<(u32, u32, VectorRef)>,
    /// Postings of compaction inputs to seed each vector column's index
    /// from, by field name, one group per input (#253). See
    /// [`Self::seed_vector_postings`].
    vector_seeds: HashMap<String, Vec<Vec<VectorSeedPosting>>>,
    /// How each column's index was built by `finalize`, for tests and
    /// diagnostics. See [`VectorIndexBuild`].
    vector_index_builds: Arc<Mutex<Vec<VectorIndexBuild>>>,
    /// Chain for `Text` fields without their own (#220).
    default_analyzer: Analyzer,
    /// Per-field chains that differ from `default_analyzer`.
    field_analyzers: HashMap<String, Analyzer>,
    /// Fields indexed with a non-default chain, for the footer.
    analyzed_fields: BTreeMap<String, AnalyzerSpec>,
    /// Whether any `Text` value went through `default_analyzer`.
    default_analyzer_used: bool,
    /// Write per-field postings, norms and statistics, and separate the
    /// positions of consecutive `Text` values (#249). See
    /// [`field_index_enabled`].
    field_index: bool,
    /// Per-field statistics for the footer's `field_index`.
    field_stats: BTreeMap<String, kosha_core::FieldStats>,
    /// Late-interaction bags (#205), written to `multivec.*` at finalize.
    multi_vectors: multivec::MultiVectorBuilder,
    /// Vector fields the namespace schema embeds server-side, with their
    /// model id (#209), recorded in the columns' footer entries.
    embedding_models: BTreeMap<String, String>,
    /// Per-record zstd for `doc_store.bin` (research #388); `None` writes
    /// the raw layout. See [`DocStoreCompressionMode`].
    doc_store_zstd: Option<DocStoreZstdConfig>,
    /// What `write_doc_store` produced, recorded as
    /// `Footer::doc_store_compression`.
    doc_store_compression_meta: Option<kosha_core::DocStoreCompression>,
}

/// Whether new segments get per-field indexes (#249):
/// `KOSHA_FIELD_INDEX`, on unless set to `off` / `0` / `false`. Off writes
/// segments exactly as before #249 (combined index only, no position
/// gaps), e.g. ahead of a rollback to an older binary.
pub fn field_index_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        parse_field_index_enabled(std::env::var("KOSHA_FIELD_INDEX").ok().as_deref())
    })
}

fn parse_field_index_enabled(raw: Option<&str>) -> bool {
    !matches!(
        raw.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("off" | "0" | "false" | "no")
    )
}

/// Append one occurrence of `key` at `pos` in document `doc_seq` to an
/// in-memory postings map, keeping `inverted_bytes` up to date.
fn push_posting(
    index: &mut HashMap<String, Vec<Posting>>,
    inverted_bytes: &mut usize,
    key: &str,
    doc_seq: u32,
    pos: u32,
) {
    let postings = match index.get_mut(key) {
        Some(postings) => postings,
        None => {
            *inverted_bytes += TERM_ENTRY_OVERHEAD + key.len();
            index.entry(key.to_string()).or_default()
        }
    };
    if let Some(last) = postings.last_mut() {
        if last.doc_id == doc_seq {
            last.term_frequency += 1;
            last.positions.push(pos);
            *inverted_bytes += 4;
            return;
        }
    }
    postings.push(Posting {
        doc_id: doc_seq,
        term_frequency: 1,
        positions: vec![pos],
    });
    *inverted_bytes += POSTING_OVERHEAD + 4;
}

impl SegmentWriter {
    pub fn new(segment_id: SegmentId, output_dir: PathBuf) -> Self {
        let backend = Box::new(LocalStorage::new(output_dir.clone()));
        Self::new_with_backend(segment_id, output_dir, backend)
    }

    /// Create a writer with a custom storage backend (e.g., S3 via kosha-client).
    pub fn new_with_backend(
        segment_id: SegmentId,
        output_dir: PathBuf,
        backend: Box<dyn StorageBackend>,
    ) -> Self {
        Self {
            segment_id,
            output_dir,
            backend,
            field_lengths: Vec::new(),
            doc_store: None,
            inverted_index: HashMap::new(),
            inverted_bytes: 0,
            peak_inverted_bytes: 0,
            build_memory_bytes: segment_build_memory_bytes(),
            postings_write_version: postings_write_version(),
            posting_blobs_raw: posting_blobs_raw(),
            runs: Vec::new(),
            io_error: None,
            finalized: false,
            term_bloom: None,
            fuzzy_bloom: None,
            total_field_length: 0,
            filter_string: HashMap::new(),
            filter_integer: HashMap::new(),
            filter_float: HashMap::new(),
            filters_write_version: filter_columns::filters_write_version(),
            filter_columns_meta: None,
            vector_columns: Vec::new(),
            unindexed_vectors: Vec::new(),
            text_filter_ignore_above: text_filter_ignore_above(),
            vector_store_mode: VectorStoreMode::from_env(),
            doc_vector_fields: Vec::new(),
            vector_quantization: VectorQuantizationConfig::from_env().clone(),
            doc_vector_entries: Vec::new(),
            vector_seeds: HashMap::new(),
            vector_index_builds: Arc::new(Mutex::new(Vec::new())),
            default_analyzer: Analyzer::new(&AnalyzerSpec::legacy()),
            field_analyzers: HashMap::new(),
            analyzed_fields: BTreeMap::new(),
            default_analyzer_used: false,
            field_index: field_index_enabled(),
            field_stats: BTreeMap::new(),
            multi_vectors: multivec::MultiVectorBuilder::default(),
            embedding_models: BTreeMap::new(),
            doc_store_zstd: match DocStoreCompressionMode::from_env() {
                DocStoreCompressionMode::Zstd => Some(DocStoreZstdConfig::from_env()),
                DocStoreCompressionMode::Auto | DocStoreCompressionMode::Off => None,
            },
            doc_store_compression_meta: None,
        }
    }

    /// Override doc-store compression (research #388): `Some` writes
    /// per-record zstd frames with a dictionary trained on this segment's
    /// records, `None` the raw layout. Production writers follow
    /// `KOSHA_DOC_STORE_COMPRESSION`; see [`DocStoreCompressionMode`].
    /// While compression is on, vectors always go to `doc_vectors.bin`.
    pub fn with_doc_store_zstd(mut self, config: Option<DocStoreZstdConfig>) -> Self {
        self.doc_store_zstd = config;
        self
    }

    /// For merges: keep the inputs' doc-store format. With
    /// `KOSHA_DOC_STORE_COMPRESSION` unset (`auto`), a merge with any
    /// compressed input writes a compressed output, so compaction never
    /// silently undoes the format; `zstd` and `none` decide on their own
    /// (`none` is the way back to the raw layout). Call before adding
    /// documents.
    pub fn inherit_doc_store_compression(self, any_input_compressed: bool) -> Self {
        match DocStoreCompressionMode::from_env() {
            DocStoreCompressionMode::Auto if any_input_compressed => {
                self.with_doc_store_zstd(Some(DocStoreZstdConfig::from_env()))
            }
            _ => self,
        }
    }

    /// Whether this writer compresses `doc_store.bin` (research #388).
    pub fn doc_store_compressed(&self) -> bool {
        self.doc_store_zstd.is_some()
    }

    /// Where vectors go: the configured mode, except that a compressed
    /// doc store never takes inline vectors (research #388): JSON floats
    /// would be the bulk of every record and compress poorly.
    fn effective_vector_store_mode(&self) -> VectorStoreMode {
        if self.doc_store_zstd.is_some() {
            VectorStoreMode::Sidecar
        } else {
            self.vector_store_mode
        }
    }

    /// Override whether this writer builds per-field indexes (#249).
    /// Production writers use `KOSHA_FIELD_INDEX`; see
    /// [`field_index_enabled`].
    pub fn with_field_index(mut self, enabled: bool) -> Self {
        self.field_index = enabled;
        self
    }

    /// Analyze `Text` fields with `config` (the namespace schema's
    /// analyzers, #220) instead of the default `standard` chain. The chains
    /// actually used are recorded in the footer, and queries are analyzed
    /// with them, so this never affects how other segments are searched.
    pub fn with_analysis(mut self, config: &AnalysisConfig) -> Self {
        self.default_analyzer = Analyzer::new(&config.default);
        self.field_analyzers = config
            .fields
            .iter()
            .filter(|(_, spec)| **spec != config.default)
            .map(|(name, spec)| (name.clone(), Analyzer::new(spec)))
            .collect();
        self.embedding_models = config.embedding_models.clone();
        self
    }

    /// The footer's `analysis`: `None` when every `Text` value went
    /// through the legacy chain, which keeps default segments
    /// byte-identical to those written before analyzers existed.
    fn segment_analysis(&self) -> Option<SegmentAnalysis> {
        if self.default_analyzer.spec().is_legacy() && self.analyzed_fields.is_empty() {
            return None;
        }
        Some(SegmentAnalysis {
            default: self.default_analyzer.spec().clone(),
            fields: self.analyzed_fields.clone(),
            default_used: self.default_analyzer_used,
        })
    }

    /// Override the in-memory postings budget before a sorted run is spilled
    /// to disk (#242). Production writers use
    /// `KOSHA_SEGMENT_BUILD_MEMORY_BYTES`; see [`segment_build_memory_bytes`].
    pub fn with_build_memory_bytes(mut self, bytes: usize) -> Self {
        self.build_memory_bytes = bytes.max(1);
        self
    }

    /// Override the inverted-index version (tests, benches and mixed-version
    /// fixtures in other crates; production uses
    /// `KOSHA_POSTINGS_WRITE_VERSION`). Accepts 4, 5 or 6.
    #[doc(hidden)]
    pub fn with_postings_write_version(mut self, version: u32) -> Self {
        assert!(
            matches!(
                version,
                SPLIT_INVERTED_VERSION
                    | SKIP_SPLIT_INVERTED_VERSION
                    | BITPACKED_SKIP_SPLIT_INVERTED_VERSION
            ),
            "unsupported postings write version {version}"
        );
        self.postings_write_version = version;
        self
    }

    /// Override whether posting shards are written raw (#245). Production
    /// writers use `KOSHA_POSTINGS_RAW_BLOBS`; see [`posting_blobs_raw`].
    pub fn with_raw_posting_blobs(mut self, raw: bool) -> Self {
        self.posting_blobs_raw = raw;
        self
    }

    /// Override the filter index format (#244): `1` writes `filters.bin`,
    /// `2` the columnar `filters.toc` + `filters-NNN.col`. Production
    /// writers use `KOSHA_FILTERS_WRITE_VERSION`; see
    /// [`filter_columns::filters_write_version`].
    pub fn with_filters_write_version(mut self, version: u32) -> Self {
        self.filters_write_version = version;
        self
    }

    /// Override where this writer stores parsed vector values (#243).
    /// Production writers use `KOSHA_VECTOR_STORE`; see [`VectorStoreMode`].
    pub fn with_vector_store_mode(mut self, mode: VectorStoreMode) -> Self {
        self.vector_store_mode = mode;
        self
    }

    /// Override how this writer stores vector posting entries (#210).
    /// Production writers use `KOSHA_VECTOR_QUANTIZATION` and
    /// `KOSHA_VECTOR_QUANTIZATION_FIELDS`; see [`VectorQuantizationConfig`].
    pub fn with_vector_quantization(mut self, config: VectorQuantizationConfig) -> Self {
        self.vector_quantization = config;
        self
    }

    /// Seed `field`'s vector index with existing postings instead of
    /// clustering its vectors from scratch (#253). Compaction calls this
    /// once per input segment with that input's postings, `doc_seqs`
    /// already remapped to this writer's doc_seqs (see
    /// [`SegmentReader::vector_posting_layout`]).
    ///
    /// The vectors themselves still come from [`Self::add_document`]:
    /// `doc_vectors.bin` needs them anyway, and the exact values are what
    /// an i8 column requantizes from and what radii are computed from. A
    /// seed only says which documents share a posting and where its
    /// centroid is. At `finalize` the largest input's postings seed the
    /// column's index
    /// ([`kosha_vector_spfresh::ClusterIndex::from_postings`]), every other
    /// input's postings — and vectors no seed covers — are absorbed into
    /// them ([`kosha_vector_spfresh::ClusterIndex::absorb_postings`]), and
    /// LIRE's split / merge / reassign repairs the postings that changed.
    /// If most of a column's vectors are not covered, or a seed's
    /// dimension disagrees with the column, the column is clustered from
    /// scratch as usual. Seeds are ignored when
    /// `KOSHA_VECTOR_WRITE_VERSION=1` (flat, unclustered).
    pub fn seed_vector_postings(
        &mut self,
        field: &str,
        postings: impl IntoIterator<Item = VectorSeedPosting>,
    ) {
        self.vector_seeds
            .entry(field.to_string())
            .or_default()
            .push(postings.into_iter().collect());
    }

    /// A handle to how `finalize` built each vector column's index — read
    /// it after `finalize` (which consumes the writer).
    pub fn vector_index_builds(&self) -> Arc<Mutex<Vec<VectorIndexBuild>>> {
        Arc::clone(&self.vector_index_builds)
    }

    /// Override the `Text` → filter-column cap for this writer (`None`
    /// disables it). Production writers use the
    /// `KOSHA_TEXT_FILTER_IGNORE_ABOVE` default; see
    /// [`text_filter_ignore_above`].
    pub fn with_text_filter_ignore_above(mut self, cap: Option<u32>) -> Self {
        self.text_filter_ignore_above = cap;
        self
    }

    pub fn add_document(&mut self, doc_id: DocumentId, fields: Vec<Field>) {
        self.add_document_with_vectors(doc_id, fields, Vec::new());
    }

    /// Recursive-graph-bisection order (#385) for `docs`, documents about
    /// to be added to this writer: `order[new] = old`, `old` indexing
    /// `docs`. Terms come from this writer's own analyzers, so they are the
    /// terms the documents' postings will hold. Adding `docs` in this order
    /// is all reordering takes: every `doc_seq`-keyed structure follows the
    /// add order. See [`reorder`].
    pub fn bp_order<'a, I>(&self, docs: I, config: &BpConfig) -> Vec<u32>
    where
        I: IntoIterator<Item = &'a [Field]>,
    {
        let mut forward = reorder::ForwardIndex::new();
        let mut terms: Vec<String> = Vec::new();
        for fields in docs {
            terms.clear();
            for field in fields {
                if field.field_type != FieldType::Text {
                    continue;
                }
                let analyzer = self
                    .field_analyzers
                    .get(&field.name)
                    .unwrap_or(&self.default_analyzer);
                terms.extend(analyzer.analyze(&field.value));
            }
            forward.push_doc(terms.iter());
        }
        forward.bp_order(config)
    }

    /// [`Self::add_document`] with some `Vector` fields' values supplied
    /// already parsed, as `(index into fields, vector)`. Compaction uses
    /// this with [`SegmentReader::iter_doc_records_raw`] so sidecar vectors
    /// pass through as f32 instead of being printed to JSON by the reader
    /// and parsed back here (#242). A supplied vector is used as is, so it
    /// stays bit-identical; the field's own value is ignored, and may be
    /// the empty sidecar placeholder. If the vector ends up stored inline
    /// (`KOSHA_VECTOR_STORE=inline`, or an empty vector) and the field's
    /// value is empty, the writer fills in its JSON, exactly as
    /// `SegmentReader::doc_record_full` would have.
    pub fn add_document_with_vectors(
        &mut self,
        doc_id: DocumentId,
        mut fields: Vec<Field>,
        mut vectors: Vec<(usize, Vec<f32>)>,
    ) {
        let doc_seq = self.field_lengths.len() as u32;
        let mut field_length: u32 = 0;
        // Vector fields whose value moves to `doc_vectors.bin` (#243); their
        // doc-store value is blanked after the loop below.
        let mut moved_to_sidecar: Vec<usize> = Vec::new();
        // Supplied vectors stored inline in place of an empty value:
        // `(field index, where it was buffered)`; `None` for an empty
        // vector, which is never buffered.
        let mut needs_json: Vec<(usize, Option<VectorRef>)> = Vec::new();
        // Position of the next `Text` value's first token (#249): each
        // value starts `position_gap` past the previous one's last token,
        // so no phrase spans two values. Always 0 without a field index.
        let mut position_base: u32 = 0;
        // This document's token count per indexed field, in first-seen
        // order (a field may occur more than once).
        let mut doc_field_lengths: Vec<(usize, u32)> = Vec::new();

        for (index, field) in fields.iter().enumerate() {
            if field.field_type == FieldType::Text {
                let analyzer = match self.field_analyzers.get(&field.name) {
                    Some(analyzer) => {
                        if !self.analyzed_fields.contains_key(&field.name) {
                            self.analyzed_fields
                                .insert(field.name.clone(), analyzer.spec().clone());
                        }
                        analyzer
                    }
                    None => {
                        self.default_analyzer_used = true;
                        &self.default_analyzer
                    }
                };
                let tokens = analyzer.analyze_with_positions(&field.value);
                field_length += tokens.len() as u32;
                let per_field = self.field_index
                    && !tokens.is_empty()
                    && kosha_core::fields::is_indexable_field_name(&field.name);
                let field_prefix = if per_field {
                    let slot = match doc_field_lengths
                        .iter()
                        .position(|&(i, _)| fields[i].name == field.name)
                    {
                        Some(slot) => slot,
                        None => {
                            doc_field_lengths.push((index, 0));
                            doc_field_lengths.len() - 1
                        }
                    };
                    doc_field_lengths[slot].1 += tokens.len() as u32;
                    kosha_core::fields::field_term_key(&field.name, "")
                } else {
                    String::new()
                };
                let mut field_key = field_prefix.clone();
                let mut last_pos = 0u32;
                for (token, pos) in &tokens {
                    let pos = position_base.saturating_add(*pos);
                    last_pos = pos;
                    push_posting(
                        &mut self.inverted_index,
                        &mut self.inverted_bytes,
                        token,
                        doc_seq,
                        pos,
                    );
                    if per_field {
                        field_key.truncate(field_prefix.len());
                        field_key.push_str(token);
                        push_posting(
                            &mut self.inverted_index,
                            &mut self.inverted_bytes,
                            &field_key,
                            doc_seq,
                            pos,
                        );
                    }
                }
                if self.field_index && !tokens.is_empty() {
                    position_base = last_pos
                        .saturating_add(1)
                        .saturating_add(kosha_core::fields::DEFAULT_POSITION_GAP);
                }
            }
            match field.field_type {
                FieldType::Keyword | FieldType::Boolean | FieldType::Date => {
                    self.filter_string
                        .entry(field.name.clone())
                        .or_default()
                        .push((doc_seq, field.value.clone()));
                    if field.field_type == FieldType::Keyword {
                        self.add_nested_columns(doc_seq, field);
                    }
                }
                FieldType::GeoPoint => {
                    // One canonical "lat,lon" entry per point (#212): what
                    // `geo_distance` and geo decays read. The stored value
                    // keeps whatever form the client sent. A value that is
                    // not a valid point is not indexed, as an unparseable
                    // Integer is not.
                    if let Some((lat, lon)) = kosha_core::rank::parse_geo_str(&field.value) {
                        self.filter_string
                            .entry(field.name.clone())
                            .or_default()
                            .push((doc_seq, kosha_core::rank::canonical_geo(lat, lon)));
                    }
                }
                FieldType::Integer => {
                    if let Ok(v) = field.value.parse::<i64>() {
                        self.filter_integer
                            .entry(field.name.clone())
                            .or_default()
                            .push((doc_seq, v));
                    }
                }
                FieldType::Float => {
                    if let Ok(v) = field.value.parse::<f64>() {
                        self.filter_float
                            .entry(field.name.clone())
                            .or_default()
                            .push((doc_seq, v));
                    }
                }
                FieldType::Text => {
                    // Clients map every JSON string to `Text` (there is no
                    // schema), so short Text values — ids, statuses, paths —
                    // are what production term filters run against and must
                    // stay filterable. Long ones are prose bodies nobody
                    // exact-matches; copying them made `filters.bin` and the
                    // footer blooms a second copy of the corpus (#243).
                    if text_value_within_cap(&field.value, self.text_filter_ignore_above) {
                        self.filter_string
                            .entry(field.name.clone())
                            .or_default()
                            .push((doc_seq, field.value.clone()));
                    }
                }
                FieldType::Vector => {
                    let supplied = vectors
                        .iter_mut()
                        .find(|(i, _)| *i == index)
                        .map(|(_, v)| std::mem::take(v));
                    let was_supplied = supplied.is_some();
                    let parsed = match supplied {
                        Some(vec) => Some(vec),
                        None => serde_json::from_str::<Vec<f32>>(&field.value).ok(),
                    };
                    if let Some(vec) = parsed {
                        // An empty vector has nothing to index (and no
                        // dimension to agree with); it stays inline.
                        if vec.is_empty() {
                            if was_supplied && field.value.is_empty() {
                                needs_json.push((index, None));
                            }
                            continue;
                        }
                        let location = self.buffer_vector(&field.name, doc_seq, vec);
                        // An empty value is the sidecar placeholder; a
                        // literal `[]` would be indistinguishable from it
                        // once blanked, so it stays inline (above).
                        if self.effective_vector_store_mode() == VectorStoreMode::Sidecar {
                            let field_idx = match self
                                .doc_vector_fields
                                .iter()
                                .position(|name| name == &field.name)
                            {
                                Some(i) => i,
                                None => {
                                    self.doc_vector_fields.push(field.name.clone());
                                    self.doc_vector_fields.len() - 1
                                }
                            };
                            self.doc_vector_entries
                                .push((doc_seq, field_idx as u32, location));
                            moved_to_sidecar.push(index);
                        } else if was_supplied && field.value.is_empty() {
                            needs_json.push((index, Some(location)));
                        }
                    } else if let Some(bag) = multivec::parse_bag(&field.value) {
                        // A bag of token vectors (#205): indexed for late
                        // interaction, and kept inline in the doc store.
                        if !self.multi_vectors.add(doc_seq, &field.name, &bag) {
                            eprintln!(
                                "WARN: late-interaction field '{}' of {:?}: token dimension {} \
                                 differs from earlier documents in this segment — stored, \
                                 not indexed",
                                field.name,
                                doc_id.0,
                                bag[0].len()
                            );
                        }
                    }
                    // Unparseable values stay inline as given: nothing
                    // indexes them, but the stored document is lossless.
                }
            }
        }
        // Norms: one posting per field this document has, whose term
        // frequency is the field's length (#249).
        for &(index, length) in &doc_field_lengths {
            let name = &fields[index].name;
            let norms = kosha_core::fields::field_norms_key(name);
            let postings = match self.inverted_index.get_mut(&norms) {
                Some(postings) => postings,
                None => {
                    self.inverted_bytes += TERM_ENTRY_OVERHEAD + norms.len();
                    self.inverted_index.entry(norms).or_default()
                }
            };
            postings.push(Posting {
                doc_id: doc_seq,
                term_frequency: length,
                positions: Vec::new(),
            });
            self.inverted_bytes += POSTING_OVERHEAD;
            let stats = self.field_stats.entry(name.clone()).or_default();
            stats.doc_count += 1;
            stats.total_length += length as u64;
        }
        for index in moved_to_sidecar {
            fields[index].value.clear();
        }
        for (index, location) in needs_json {
            let vector = location.map_or(&[][..], |location| self.buffered_vector(location));
            match serde_json::to_string(vector) {
                Ok(json) => fields[index].value = json,
                Err(e) => {
                    self.io_error.get_or_insert(e.into());
                }
            }
        }

        self.total_field_length += field_length as u64;
        self.field_lengths.push(field_length);
        self.peak_inverted_bytes = self.peak_inverted_bytes.max(self.inverted_bytes);
        if self.io_error.is_some() {
            return;
        }
        let record = DocRecord {
            doc_id,
            doc_seq,
            field_length,
            fields,
        };
        let streamed = self.doc_store_sink().and_then(|sink| sink.append(&record));
        if let Err(e) = streamed {
            self.io_error = Some(e);
            return;
        }
        if self.inverted_bytes > self.build_memory_bytes {
            if let Err(e) = self.spill_run() {
                self.io_error = Some(e);
            }
        }
    }

    /// Index a nested container's leaves under their dotted paths (#206).
    ///
    /// Filter columns only: the doc store keeps the container as sent, so
    /// `_source` round-trips it, and the container's own column entry is
    /// unchanged. Nothing is written for a value that is not a container.
    /// See `kosha_core::nested` for the per-path encoding.
    fn add_nested_columns(&mut self, doc_seq: u32, field: &Field) {
        for (path, column) in kosha_core::nested::flatten_container(&field.name, &field.value) {
            match column {
                kosha_core::nested::NestedColumn::Text(value) => {
                    self.filter_string
                        .entry(path)
                        .or_default()
                        .push((doc_seq, value));
                }
                kosha_core::nested::NestedColumn::Numbers(values) => {
                    let entries = self.filter_float.entry(path).or_default();
                    entries.extend(values.into_iter().map(|v| (doc_seq, v)));
                }
            }
        }
    }

    /// Buffer one parsed vector under its field's column (#204), creating
    /// the column on first sight. A vector whose dimension differs from the
    /// column's is kept for the doc store but not indexed — one bad
    /// document must not fail the whole flush (the write API rejects it
    /// before it gets here; see `kosha_write`).
    fn buffer_vector(&mut self, field: &str, doc_seq: u32, vec: Vec<f32>) -> VectorRef {
        let column = match self.vector_columns.iter().position(|c| c.name == field) {
            Some(i) => i,
            None => {
                self.vector_columns.push(VectorColumn {
                    name: field.to_string(),
                    dim: vec.len(),
                    vectors: Vec::new(),
                });
                self.vector_columns.len() - 1
            }
        };
        let col = &mut self.vector_columns[column];
        if col.dim != vec.len() {
            eprintln!(
                "WARN: segment {}: vector field '{field}' of doc_seq {doc_seq} has {} dims, \
                 the field's index has {} — stored but not indexed",
                self.segment_id.0,
                vec.len(),
                col.dim
            );
            self.unindexed_vectors.push(vec);
            return VectorRef::Unindexed(self.unindexed_vectors.len() as u32 - 1);
        }
        col.vectors.push((doc_seq, vec));
        VectorRef::Column(column as u32, col.vectors.len() as u32 - 1)
    }

    /// The buffered vector `location` points at.
    fn buffered_vector(&self, location: VectorRef) -> &[f32] {
        match location {
            VectorRef::Column(c, i) => &self.vector_columns[c as usize].vectors[i as usize].1,
            VectorRef::Unindexed(i) => &self.unindexed_vectors[i as usize],
        }
    }

    /// Vectors buffered so far that were not indexed because of a
    /// dimension mismatch within their field.
    pub fn unindexed_vector_count(&self) -> usize {
        self.unindexed_vectors.len()
    }

    /// Number of sorted posting runs spilled so far.
    pub fn spilled_runs(&self) -> usize {
        self.runs.len()
    }

    /// Highest approximate in-memory postings size this writer reached.
    pub fn peak_inverted_bytes(&self) -> usize {
        self.peak_inverted_bytes
    }

    /// Scratch directory for this build's partial files, inside the output
    /// directory so every rename into place stays on one filesystem.
    fn build_dir(&self) -> PathBuf {
        self.output_dir.join(BUILD_DIR)
    }

    fn doc_store_sink(&mut self) -> Result<&mut DocStoreSink, KoshaError> {
        if self.doc_store.is_none() {
            let dir = self.build_dir();
            fs::create_dir_all(&dir)?;
            self.doc_store = Some(DocStoreSink::create(dir.join("doc_store.bin"))?);
        }
        Ok(self.doc_store.as_mut().unwrap())
    }

    /// Write the in-memory postings to a sorted run file and free them.
    /// Runs hold contiguous, increasing doc_seq ranges, so a term's
    /// postings across runs are merged by concatenating in run order.
    fn spill_run(&mut self) -> Result<(), KoshaError> {
        if self.inverted_index.is_empty() {
            return Ok(());
        }
        let dir = self.build_dir();
        fs::create_dir_all(&dir)?;
        let path = dir.join(format!("run-{:05}", self.runs.len()));
        let mut terms: Vec<(String, Vec<Posting>)> = std::mem::take(&mut self.inverted_index)
            .into_iter()
            .collect();
        terms.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut out = std::io::BufWriter::new(fs::File::create(&path)?);
        for (term, postings) in &terms {
            write_run_entry(&mut out, term, postings)?;
        }
        std::io::Write::flush(&mut out)?;
        self.runs.push(path);
        self.inverted_bytes = 0;
        Ok(())
    }

    pub fn finalize(mut self, bm25_params: Bm25Params) -> Result<Footer, KoshaError> {
        if let Some(e) = self.io_error.take() {
            return Err(e);
        }
        self.backend.create_dir_all("")?;
        self.write_doc_store()?;
        self.write_doc_vectors()?;
        self.write_inverted_index()?;
        self.write_filters()?;
        let vector_fields = match vector_write_version() {
            1 => self.write_vectors()?,
            _ => self.write_vectors_v2()?,
        };
        let multi_vectors = self.multi_vectors.write(&*self.backend)?;
        let footer = self.write_footer(bm25_params, vector_fields, multi_vectors)?;
        let _ = fs::remove_dir_all(self.build_dir());
        self.finalized = true;
        Ok(footer)
    }

    /// Finishes `doc_store.bin` (unchanged byte layout) and writes
    /// `doc_store.offsets` — a sidecar sized to `doc_count`, not content
    /// size, that lets `SegmentReader` open a segment without parsing every
    /// document's full field content into memory (see `DocStoreAccess`).
    /// Records were streamed to disk by `add_document` (#242); this patches
    /// the leading doc count and moves the file into place.
    fn write_doc_store(&mut self) -> Result<(), KoshaError> {
        let doc_count = self.field_lengths.len() as u32;
        let sink = match self.doc_store.take() {
            Some(sink) => sink,
            // No documents: the same 4-byte files as before.
            None => {
                self.backend
                    .write("doc_store.bin", &doc_count.to_le_bytes())?;
                self.backend
                    .write("doc_store.offsets", &doc_count.to_le_bytes())?;
                return Ok(());
            }
        };
        let (path, offsets) = sink.finish(doc_count)?;
        let (path, offsets) = match self.doc_store_zstd {
            None => (path, offsets),
            Some(config) => {
                let out = self.build_dir().join("doc_store.zst");
                let (offsets, meta) = compress_doc_store(&path, &out, &offsets, config)?;
                let _ = fs::remove_file(&path);
                self.doc_store_compression_meta = Some(meta);
                (out, offsets)
            }
        };
        fs::rename(&path, self.output_dir.join("doc_store.bin"))?;
        self.backend.write("doc_store.offsets", &offsets)?;
        Ok(())
    }

    /// Write `doc_vectors.bin` (#243) when any vector was moved out of the
    /// doc store. See [`DocVectors`] for the layout.
    fn write_doc_vectors(&self) -> Result<(), KoshaError> {
        if self.doc_vector_entries.is_empty() {
            return Ok(());
        }
        let data_len: usize = self
            .doc_vector_entries
            .iter()
            .map(|&(_, _, v)| self.buffered_vector(v).len() * 4)
            .sum();
        let mut buf = Vec::with_capacity(16 + self.doc_vector_entries.len() * 12 + data_len);
        buf.extend_from_slice(&DOC_VECTORS_MAGIC.to_le_bytes());
        buf.extend_from_slice(&(self.doc_vector_fields.len() as u32).to_le_bytes());
        for name in &self.doc_vector_fields {
            buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
            buf.extend_from_slice(name.as_bytes());
        }
        buf.extend_from_slice(&(self.doc_vector_entries.len() as u32).to_le_bytes());
        for &(doc_seq, field_idx, v) in &self.doc_vector_entries {
            buf.extend_from_slice(&doc_seq.to_le_bytes());
            buf.extend_from_slice(&field_idx.to_le_bytes());
            buf.extend_from_slice(&(self.buffered_vector(v).len() as u32).to_le_bytes());
        }
        for &(_, _, v) in &self.doc_vector_entries {
            for x in self.buffered_vector(v) {
                buf.extend_from_slice(&x.to_le_bytes());
            }
        }
        self.backend.write(DOC_VECTORS_FILE, &buf)
    }

    /// Write `inverted.idx` in the v3 table-of-contents layout (see
    /// [`LazyInvertedIndex`] for the format and why it exists). The legacy
    /// v1 stream layout and inline v2 layout are still readable for
    /// segments already on disk/S3.
    fn write_inverted_index(&mut self) -> Result<(), KoshaError> {
        // Every spilled run plus what's still in memory, merged in term
        // order. Sorted term order is load-bearing: the reader
        // binary-searches the term table (see `LazyInvertedIndex::find`).
        // Memory: the term table plus a bounded per-term encoder; a term's
        // postings stream from the runs into it and are never all resident,
        // however many documents contain the term (#242).
        let mut sources: Vec<PostingSource> = Vec::with_capacity(self.runs.len() + 1);
        for path in &self.runs {
            sources.push(PostingSource::Run(RunReader::open(path)?));
        }
        let mut in_memory: Vec<(String, Vec<Posting>)> = std::mem::take(&mut self.inverted_index)
            .into_iter()
            .collect();
        in_memory.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        sources.push(PostingSource::memory(in_memory));
        self.inverted_bytes = 0;

        // Postings go straight to one raw file per shard; query nodes can
        // then hydrate just the TOC (`inverted.idx`) plus the query-term
        // shard(s), not every term's postings in the segment.
        let build_dir = self.build_dir();
        fs::create_dir_all(&build_dir)?;
        let mut blobs: Vec<Option<std::io::BufWriter<fs::File>>> =
            (0..POSTING_BLOB_SHARDS).map(|_| None).collect();
        let mut blob_lens: Vec<u64> = vec![0; POSTING_BLOB_SHARDS];
        let mut pool: Vec<u8> = Vec::new();
        // (term_off_in_pool, term_len, blob_id, postings_off_in_blob, postings_len)
        let mut entries: Vec<(u64, u32, u32, u64, u32)> = Vec::new();
        let write_version = self.postings_write_version;
        // Rollout/rollback compatibility: v4 (`KOSHA_POSTINGS_WRITE_VERSION=4`)
        // streams through the same encoder; see `postings_write_version`.
        let mut encoder = TermEncoder::new(
            write_version,
            &build_dir,
            term_spill_limit(self.build_memory_bytes),
        );

        let mut merger = TermMerger::new(sources)?;
        while let Some(term) = merger.next_term(&mut encoder, &self.field_lengths)? {
            let blob_id = posting_blob_id_for_term(&term);
            let blob = match &mut blobs[blob_id] {
                Some(blob) => blob,
                slot => {
                    let file = fs::File::create(build_dir.join(format!("blob-{blob_id}")))?;
                    slot.insert(std::io::BufWriter::new(file))
                }
            };
            let encoded_len = encoder.finish(blob)?;
            let encoded_len = u32::try_from(encoded_len).map_err(|_| {
                KoshaError::Io(std::io::Error::other(format!(
                    "postings for term {term:?} encode to {encoded_len} bytes, over the \
                     4 GiB per-term limit of the inverted index format"
                )))
            })?;
            let p_off = blob_lens[blob_id];
            blob_lens[blob_id] += encoded_len as u64;
            let term_off = pool.len() as u64;
            pool.extend_from_slice(term.as_bytes());
            entries.push((
                term_off,
                term.len() as u32,
                blob_id as u32,
                p_off,
                encoded_len,
            ));
        }
        drop(encoder);

        // Every term is in `pool` exactly once, sorted (no duplicates).
        // Per-field keys stay out of the bloom: each is also a combined
        // term, and queries test the bloom with that (#249).
        let combined_terms = || {
            entries
                .iter()
                .map(|&(off, len, ..)| {
                    std::str::from_utf8(&pool[off as usize..off as usize + len as usize])
                        .expect("pool holds UTF-8 terms")
                })
                .filter(|term| !kosha_core::fields::is_field_key(term))
        };
        self.term_bloom = Some(build_term_bloom(combined_terms()));
        self.fuzzy_bloom = kosha_core::fuzzy::build_fuzzy_bloom(combined_terms());

        let table_len = entries.len() as u64 * SPLIT_INVERTED_TABLE_ENTRY_LEN as u64;
        let pool_base = INVERTED_HEADER_LEN as u64 + table_len;

        let mut buf = Vec::with_capacity(pool_base as usize + pool.len());
        buf.extend_from_slice(&INVERTED_MAGIC.to_le_bytes());
        buf.extend_from_slice(&write_version.to_le_bytes());
        buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(POSTING_BLOB_SHARDS as u32).to_le_bytes());
        for (term_off, term_len, blob_id, p_off, p_len) in entries {
            buf.extend_from_slice(&(pool_base + term_off).to_le_bytes());
            buf.extend_from_slice(&term_len.to_le_bytes());
            buf.extend_from_slice(&blob_id.to_le_bytes());
            buf.extend_from_slice(&p_off.to_le_bytes());
            buf.extend_from_slice(&p_len.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
        buf.extend_from_slice(&pool);
        // TOC and every posting blob are wrapped independently (see
        // `maybe_compress_inverted`) so the lazy per-shard fetch/decode
        // semantics are unchanged — each artifact self-describes.
        let encoded_toc = maybe_compress_inverted(&buf);
        drop(buf);
        self.backend.write("inverted.idx", &encoded_toc)?;
        for (blob_id, blob) in blobs.into_iter().enumerate() {
            let Some(blob) = blob else { continue };
            let file = blob
                .into_inner()
                .map_err(|e| KoshaError::Io(e.into_error()))?;
            drop(file);
            let raw = build_dir.join(format!("blob-{blob_id}"));
            let final_path = self.output_dir.join(posting_blob_file_for_id(blob_id));
            if self.posting_blobs_raw {
                // Rangeable: the TOC's spans index these exact bytes.
                fs::rename(&raw, &final_path)?;
            } else {
                maybe_compress_inverted_file(&raw, &final_path, blob_lens[blob_id])?;
            }
        }
        for run in self.runs.drain(..) {
            let _ = fs::remove_file(run);
        }
        Ok(())
    }

    fn write_filters(&mut self) -> Result<(), KoshaError> {
        if self.filters_write_version >= 2 {
            return self.write_filter_columns();
        }
        let mut buf = Vec::new();
        let total_fields =
            self.filter_string.len() + self.filter_integer.len() + self.filter_float.len();
        buf.extend_from_slice(&(total_fields as u32).to_le_bytes());

        let mut string_names: Vec<&String> = self.filter_string.keys().collect();
        string_names.sort();
        for name in string_names {
            let entries = &self.filter_string[name];
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            buf.push(0);
            buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            for &(doc_seq, ref val) in entries {
                buf.extend_from_slice(&doc_seq.to_le_bytes());
                let val_bytes = val.as_bytes();
                buf.extend_from_slice(&(val_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(val_bytes);
            }
        }

        let mut int_names: Vec<&String> = self.filter_integer.keys().collect();
        int_names.sort();
        for name in int_names {
            let entries = &self.filter_integer[name];
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            buf.push(1);
            buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            for &(doc_seq, val) in entries {
                buf.extend_from_slice(&doc_seq.to_le_bytes());
                buf.extend_from_slice(&val.to_le_bytes());
            }
        }

        let mut float_names: Vec<&String> = self.filter_float.keys().collect();
        float_names.sort();
        for name in float_names {
            let entries = &self.filter_float[name];
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            buf.push(2);
            buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            for &(doc_seq, val) in entries {
                buf.extend_from_slice(&doc_seq.to_le_bytes());
                buf.extend_from_slice(&val.to_le_bytes());
            }
        }

        self.backend.write("filters.bin", &buf)?;
        Ok(())
    }

    /// Columnar filter index (#244): one `filters-NNN.col` per `(field,
    /// kind)` and `filters.toc`, written last so a TOC on disk implies its
    /// columns are. See [`filter_columns`] for the layout. Always writes the
    /// TOC, even with zero columns, so "TOC absent" only ever means "not
    /// hydrated yet".
    fn write_filter_columns(&mut self) -> Result<(), KoshaError> {
        let doc_count = self.field_lengths.len() as u32;
        let backend = &self.backend;
        let metas = filter_columns::build_columns(
            doc_count,
            &self.filter_string,
            &self.filter_integer,
            &self.filter_float,
            |meta, bytes| backend.write(&meta.file_name(), bytes),
        )?;
        let toc = filter_columns::FiltersToc {
            doc_count,
            columns: metas,
        };
        self.backend
            .write(kosha_core::FILTERS_TOC_FILE, &toc.encode())?;
        self.filter_columns_meta = Some(toc.columns);
        Ok(())
    }

    /// Legacy flat format: `[dim:u32][count:u32]` then `count ×
    /// [doc_seq:u32][f32×dim]`. No magic, no version — a v2 reader
    /// distinguishes this from the new format purely by the absence of
    /// `VECTOR_MAGIC` in the first 4 bytes (see `parse_vector_idx_header`).
    /// `finalize` only calls this when `KOSHA_VECTOR_WRITE_VERSION=1` is
    /// set (mixed-fleet rollback gate, see `vector_write_version`) —
    /// `write_vectors_v2` is the default.
    ///
    /// One file per vector column (#204), at the column's slot — see
    /// [`kosha_core::VectorFieldMeta::slot`].
    fn write_vectors(&self) -> Result<Vec<VectorFieldMeta>, KoshaError> {
        let mut metas = Vec::with_capacity(self.vector_columns.len());
        for (slot, column) in self.vector_columns.iter().enumerate() {
            if column.vectors.is_empty() {
                continue;
            }
            let mut buf = Vec::new();
            let dim = column.dim as u32;
            buf.extend_from_slice(&dim.to_le_bytes());
            buf.extend_from_slice(&(column.vectors.len() as u32).to_le_bytes());
            for &(doc_seq, ref v) in &column.vectors {
                buf.extend_from_slice(&doc_seq.to_le_bytes());
                for &val in v {
                    buf.extend_from_slice(&val.to_le_bytes());
                }
            }
            // The flat format has no version field to tag quantization
            // with: it is always f32 (see `write_vectors_v2`).
            let meta = VectorFieldMeta {
                name: column.name.clone(),
                dim,
                count: column.vectors.len() as u32,
                slot: slot as u32,
                offsets: false,
                quantization: VectorQuantization::F32,
                embedding_model: self.embedding_models.get(&column.name).cloned(),
            };
            self.backend.write(&meta.idx_file(), &buf)?;
            metas.push(meta);
        }
        Ok(metas)
    }

    /// v2 format (default, see `vector_write_version`): `vector.idx`
    /// (posting entries, magic-prefixed) + `vector.offsets` (centroids +
    /// byte ranges, for lazy per-posting hydration at read time). Builds a
    /// `kosha_vector_spfresh::ClusterIndex` from this segment's buffered
    /// vectors and serializes its postings — see the "Vector index format"
    /// section below for the byte layout.
    ///
    /// One index per vector column (#204), each at its column's slot: a
    /// single-column segment gets exactly the `vector.idx` +
    /// `vector.offsets` it always did, and further columns get
    /// `vector.<slot>.idx` + `vector.<slot>.offsets`.
    fn write_vectors_v2(&self) -> Result<Vec<VectorFieldMeta>, KoshaError> {
        let mut metas = Vec::with_capacity(self.vector_columns.len());
        for (slot, column) in self.vector_columns.iter().enumerate() {
            if column.vectors.is_empty() {
                continue;
            }
            // i8 entries need the newest header version: a writer pinned
            // to an older `KOSHA_VECTOR_WRITE_VERSION` for a rolling deploy
            // keeps writing f32, which every reader of that version reads.
            let requested = self.vector_quantization.for_field(&column.name);
            let quantization = if vector_write_version() >= 4 {
                requested
            } else {
                if !requested.is_f32() {
                    warn_quantization_needs_v4();
                }
                VectorQuantization::F32
            };
            let meta = VectorFieldMeta {
                name: column.name.clone(),
                dim: column.dim as u32,
                count: column.vectors.len() as u32,
                slot: slot as u32,
                offsets: true,
                quantization,
                embedding_model: self.embedding_models.get(&column.name).cloned(),
            };
            let seeds = self.vector_seeds.get(&column.name);
            let (idx_bytes, offsets_bytes, build) =
                Self::encode_vector_column(&column.vectors, seeds, quantization)?;
            self.vector_index_builds
                .lock()
                .unwrap()
                .push(VectorIndexBuild {
                    field: column.name.clone(),
                    ..build
                });
            self.backend.write(&meta.idx_file(), &idx_bytes)?;
            self.backend.write(&meta.offsets_file(), &offsets_bytes)?;
            metas.push(meta);
        }
        Ok(metas)
    }

    /// Cluster one column's vectors and encode `(vector.idx bytes,
    /// vector.offsets bytes)` for it. Every vector must share one
    /// dimension (`buffer_vector` guarantees it). With `seeds`, the index
    /// is seeded from them and repaired by LIRE when they cover enough of
    /// the column (see [`Self::seed_vector_postings`]); otherwise it is
    /// clustered from scratch.
    fn encode_vector_column(
        column: &[(u32, Vec<f32>)],
        seeds: Option<&Vec<Vec<VectorSeedPosting>>>,
        quantization: VectorQuantization,
    ) -> Result<(Vec<u8>, Vec<u8>, VectorIndexBuild), KoshaError> {
        let dim = column[0].1.len();
        // Unit-norm (write version 3, on-disk header version 2): normalize
        // once here so the read path can score with a bare dot product.
        // Clustering runs on the normalized copies too (spherical k-means —
        // the geometry cosine actually ranks by), and snapshot centroids are
        // re-normalized afterwards because a mean of unit vectors is not
        // itself unit-length.
        let unit_norm = vector_write_version() >= 3;
        let radius_aware = vector_write_version() >= 4;
        let normalized: Vec<(u32, Vec<f32>)>;
        let vectors: &[(u32, Vec<f32>)] = if unit_norm {
            normalized = column
                .iter()
                .map(|(id, v)| {
                    let mut v = v.clone();
                    kosha_vector_spfresh::normalize_in_place(&mut v);
                    (*id, v)
                })
                .collect();
            &normalized
        } else {
            column
        };
        let mut config = kosha_vector_spfresh::ClusterIndexConfig::new(dim);
        if !vector_build_refine() {
            config.build_refine_rounds = 0;
        }
        let seeded = match seeds {
            Some(seeds) if !seeds.is_empty() => seed_cluster_index(vectors, seeds, config.clone()),
            _ => None,
        };
        let (index, mut build) = match seeded {
            Some(seeded) => seeded,
            None => {
                let index =
                    kosha_vector_spfresh::ClusterIndex::build(vectors, config).map_err(|e| {
                        // A real ClusterIndex::build failure here (practically
                        // always DimensionMismatch — inconsistent vector
                        // dimensionality across documents in the same
                        // segment) means the legacy writer above would have
                        // silently written ragged, unparseable bytes instead.
                        // Fail loud rather than emit a corrupt vector.idx.
                        KoshaError::CorruptSegment(format!("failed to build vector index: {e}"))
                    })?;
                (index, VectorIndexBuild::default())
            }
        };
        let mut postings = index.snapshot();
        build.postings = postings.len();
        let format_version = if unit_norm {
            for p in &mut postings {
                kosha_vector_spfresh::normalize_in_place(&mut p.centroid);
                // Renormalizing shifts both direction and magnitude (a mean
                // of unit vectors isn't itself unit-length), which
                // invalidates the radius `ClusterIndex::snapshot` computed
                // against the pre-renormalization centroid. Recompute
                // against the final, on-disk centroid — cheap (one more
                // pass over this posting's already-small entry list, not
                // the whole segment) and only when it'll actually be
                // persisted.
                if radius_aware {
                    p.radius = p
                        .entries
                        .iter()
                        .map(|(_, v)| kosha_vector_spfresh::cosine_distance(&p.centroid, v))
                        .fold(0.0f32, f32::max);
                }
            }
            if radius_aware && quantization == VectorQuantization::I8 {
                // Radii were computed above from the exact vectors, so the
                // probe's lower bound stays a bound on the true distance.
                I8_VECTOR_INDEX_VERSION
            } else if radius_aware && quantization == VectorQuantization::RaBitQ {
                // Same: radii from exact vectors; codes are relative to the
                // final (renormalized, on-disk) centroids.
                RABITQ_VECTOR_INDEX_VERSION
            } else if radius_aware {
                RADIUS_AWARE_VECTOR_INDEX_VERSION
            } else {
                UNIT_NORM_VECTOR_INDEX_VERSION
            }
        } else {
            VECTOR_INDEX_VERSION
        };
        let (idx, offsets) = encode_vector_index_v2(dim as u32, &postings, format_version);
        Ok((idx, offsets, build))
    }

    fn write_footer(
        &self,
        bm25_params: Bm25Params,
        vector_fields: Vec<VectorFieldMeta>,
        multi_vectors: Vec<kosha_core::MultiVectorField>,
    ) -> Result<Footer, KoshaError> {
        let doc_count = self.field_lengths.len() as u32;
        let avg = if doc_count > 0 {
            self.total_field_length as f64 / doc_count as f64
        } else {
            0.0
        };
        let footer = Footer {
            segment_id: self.segment_id.clone(),
            doc_count,
            total_field_length: self.total_field_length,
            avg_field_length: avg,
            bm25_params,
            created_at: chrono_like_now(),
            filter_blooms: Some(build_filter_blooms(&self.filter_string)),
            term_bloom: Some(
                self.term_bloom
                    .clone()
                    .unwrap_or_else(|| build_term_bloom(std::iter::empty())),
            ),
            format_version: kosha_core::SEGMENT_FORMAT_VERSION,
            text_filter_ignore_above: self.text_filter_ignore_above,
            doc_vectors: !self.doc_vector_entries.is_empty(),
            posting_blobs_raw: self.posting_blobs_raw,
            filter_columns: self.filter_columns_meta.clone(),
            analysis: self.segment_analysis(),
            field_index: self.field_index.then(|| kosha_core::FieldIndex {
                position_gap: kosha_core::fields::DEFAULT_POSITION_GAP,
                fields: self.field_stats.clone(),
            }),
            // Built with the term bloom; without one, nothing to prune on.
            fuzzy_bloom: self.fuzzy_bloom.clone(),
            vector_fields: Some(vector_fields),
            multi_vectors,
            doc_store_compression: self.doc_store_compression_meta.clone(),
        };
        let json = serde_json::to_string_pretty(&footer)?;
        self.backend.write("footer.json", json.as_bytes())?;
        Ok(footer)
    }
}

/// One vector field's buffered values in a [`SegmentWriter`] (#204).
struct VectorColumn {
    name: String,
    dim: usize,
    vectors: Vec<(u32, Vec<f32>)>,
}

/// Where a [`SegmentWriter`] buffered one parsed vector.
#[derive(Clone, Copy)]
enum VectorRef {
    /// `(column, index)` into `vector_columns`.
    Column(u32, u32),
    /// Index into `unindexed_vectors`.
    Unindexed(u32),
}

/// One posting of a compaction input, handed to
/// [`SegmentWriter::seed_vector_postings`] (#253).
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSeedPosting {
    /// The input posting's stored centroid.
    pub centroid: Vec<f32>,
    /// Its members that survive into the output, as output doc_seqs.
    pub doc_seqs: Vec<u32>,
    /// Whether any member was dropped (deleted or tombstoned) on the way:
    /// the centroid is then recomputed from the survivors.
    pub dropped: bool,
}

/// How [`SegmentWriter::finalize`] built one vector column's index.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VectorIndexBuild {
    pub field: String,
    /// Seeded from compaction inputs' postings and repaired by LIRE
    /// (`false`: clustered from scratch).
    pub lire: bool,
    /// Postings adopted as-is from the largest input (non-empty ones).
    pub seeded_postings: usize,
    /// Adopted postings whose centroid was recomputed (members dropped).
    pub recentred_postings: usize,
    /// Postings of the other inputs folded into the adopted ones.
    pub absorbed_postings: usize,
    /// Vectors no seed covered, absorbed one by one.
    pub inserted_vectors: usize,
    /// LIRE work done by the final rebalance.
    pub postings_split: usize,
    pub postings_merged: usize,
    /// Postings written.
    pub postings: usize,
}

/// Compaction inputs with fewer indexed vectors in a column than this
/// contribute no postings: their vectors are absorbed into the merged
/// index one by one (#253). A small segment's postings are clustered over
/// too few points to be useful — a 50-vector segment is one posting whose
/// centroid is the mean of everything — and its few vectors are cheap to
/// place individually. Eight postings' worth at the default
/// `max_posting_size` (128).
pub const LIRE_ADOPT_MIN_VECTORS: usize = 1024;

/// How compaction builds a merged segment's vector indexes (#253):
/// `KOSHA_VECTOR_COMPACTION=lire` (the default) seeds each column from its
/// inputs' postings and repairs them with LIRE; `rebuild` clusters every
/// vector from scratch, as before. Either way the output is the same
/// on-disk format every reader already reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorCompactionMode {
    #[default]
    Lire,
    Rebuild,
}

impl VectorCompactionMode {
    /// `KOSHA_VECTOR_COMPACTION`, read once.
    pub fn from_env() -> Self {
        static MODE: OnceLock<VectorCompactionMode> = OnceLock::new();
        *MODE.get_or_init(|| Self::parse(std::env::var("KOSHA_VECTOR_COMPACTION").ok().as_deref()))
    }

    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") | Some("lire") => Self::Lire,
            Some("rebuild") => Self::Rebuild,
            Some(other) => {
                eprintln!("WARN: KOSHA_VECTOR_COMPACTION={other} not lire|rebuild — using lire");
                Self::Lire
            }
        }
    }
}

/// How many of the largest input's postings each absorbed posting is
/// matched against (by centroid) before its members pick the nearest. The
/// default `reassign_radius`: the neighbourhood LIRE already treats as
/// "nearby".
const LIRE_ABSORB_CANDIDATES: usize = 8;

/// Seeds a column's [`kosha_vector_spfresh::ClusterIndex`] from compaction
/// inputs' postings (see [`SegmentWriter::seed_vector_postings`]):
/// `vectors` is the column (doc_seq ascending, already normalized when the
/// index is unit-norm), `groups` one list of seeds per input. `None` when
/// the seeds cannot be used — a dimension mismatch, or fewer than half of
/// the column's vectors covered (absorbing the rest one by one would then
/// cost about as much as clustering, for worse postings) — and the caller
/// clusters from scratch.
fn seed_cluster_index(
    vectors: &[(u32, Vec<f32>)],
    groups: &[Vec<VectorSeedPosting>],
    config: kosha_vector_spfresh::ClusterIndexConfig,
) -> Option<(kosha_vector_spfresh::ClusterIndex, VectorIndexBuild)> {
    use kosha_vector_spfresh::PostingSnapshot;
    let dim = config.dim;
    if groups.iter().flatten().any(|s| s.centroid.len() != dim) {
        return None;
    }
    let position = |doc_seq: u32| vectors.binary_search_by_key(&doc_seq, |(id, _)| *id).ok();
    let mut covered = vec![false; vectors.len()];
    let mut recentred = vec![0usize; groups.len()];
    // Per input: its non-empty postings, members resolved to vectors.
    let mut snapshots: Vec<Vec<PostingSnapshot>> = Vec::with_capacity(groups.len());
    for (g, seeds) in groups.iter().enumerate() {
        let mut group = Vec::with_capacity(seeds.len());
        for seed in seeds {
            let mut entries = Vec::with_capacity(seed.doc_seqs.len());
            for &doc_seq in &seed.doc_seqs {
                // A doc_seq the column does not hold (its vector was not
                // indexed) or one an earlier seed already claimed is
                // dropped.
                if let Some(i) = position(doc_seq) {
                    if !std::mem::replace(&mut covered[i], true) {
                        entries.push((doc_seq, vectors[i].1.clone()));
                    }
                }
            }
            if entries.is_empty() {
                continue;
            }
            let centroid = if seed.dropped || entries.len() != seed.doc_seqs.len() {
                recentred[g] += 1;
                kosha_vector_spfresh::mean_vector(entries.iter().map(|(_, v)| v.as_slice()), dim)
            } else {
                seed.centroid.clone()
            };
            group.push(PostingSnapshot {
                centroid,
                entries,
                radius: 0.0,
            });
        }
        snapshots.push(group);
    }
    let uncovered: Vec<usize> = (0..vectors.len()).filter(|&i| !covered[i]).collect();
    if uncovered.len() * 2 > vectors.len() {
        return None;
    }
    // The largest input's postings are adopted as they are; the others'
    // overlap them (each input partitioned the same space on its own), so
    // they are absorbed rather than kept side by side.
    let members = |g: &Vec<PostingSnapshot>| g.iter().map(|p| p.entries.len()).sum::<usize>();
    let base = (0..snapshots.len()).max_by_key(|&g| members(&snapshots[g]))?;
    let mut build = VectorIndexBuild {
        lire: true,
        seeded_postings: snapshots[base].len(),
        recentred_postings: recentred[base],
        inserted_vectors: uncovered.len(),
        ..VectorIndexBuild::default()
    };
    let adopted = snapshots.swap_remove(base);
    let mut index = kosha_vector_spfresh::ClusterIndex::from_postings(adopted, config).ok()?;
    for group in snapshots {
        build.absorbed_postings += group.len();
        index.absorb_postings(group, LIRE_ABSORB_CANDIDATES).ok()?;
    }
    // A loose vector is a one-posting partition centred on itself: it goes
    // to its exact nearest posting.
    let loose = uncovered
        .into_iter()
        .map(|i| {
            let (doc_seq, v) = &vectors[i];
            PostingSnapshot {
                centroid: v.clone(),
                entries: vec![(*doc_seq, v.clone())],
                radius: 0.0,
            }
        })
        .collect();
    index.absorb_postings(loose, LIRE_ABSORB_CANDIDATES).ok()?;
    let stats = index.rebalance();
    build.postings_split = stats.postings_split;
    build.postings_merged = stats.postings_merged;
    Some((index, build))
}

/// One vector column's posting structure as stored: each posting's
/// centroid and member doc_seqs, in `vector.offsets` order. See
/// [`SegmentReader::vector_posting_layout`].
#[derive(Debug, Clone, PartialEq)]
pub struct VectorPostingLayout {
    pub dim: usize,
    /// The index's header version (see [`is_known_vector_index_version`]).
    pub version: u32,
    pub postings: Vec<VectorPostingMembers>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorPostingMembers {
    pub centroid: Vec<f32>,
    pub doc_seqs: Vec<u32>,
}

// ─── Vector index format (v2: posting-based, LIRE) ──────────────────────────
//
// `vector.idx` v2 is a magic-prefixed, self-describing file — a legacy (v1)
// file has no magic and starts directly with its `dim` field instead (a
// small u32, e.g. 128/384/1536); colliding with `VECTOR_MAGIC` would require
// an oddly specific "dimension," the same reasoning `INVERTED_MAGIC`'s doc
// comment (above `is_split_inverted_version`) uses for `inverted.idx`.
//
// Layout:
//   vector.idx:     [magic][version][dim][total_count] then, per posting in
//                    the same order `vector.offsets` lists them, that
//                    posting's live entries concatenated: [id:u32][f32×dim]
//                    each. Self-parseable as one flat (id, vector) list even
//                    without the sidecar (see `parse_vector_idx_eager`) —
//                    posting *boundaries* live only in the sidecar.
//   vector.offsets: [magic][version][dim][posting_count] then, per posting:
//                    [centroid:f32×dim][offset:u64][length:u32] into
//                    vector.idx, plus — version >= RADIUS_AWARE_VECTOR_INDEX_VERSION
//                    only — [radius:f32] (max cosine distance from centroid
//                    to any live member; see that constant's doc comment).
//                    Small — always resident once read (mirrors
//                    `doc_store.offsets`/`DocStoreAccess::Lazy`).
//
// Four-way read dispatch (see `SegmentReader::open_with_footer_options`):
// no `vector.idx` at all -> no vectors (unchanged from today); sidecar
// present and valid -> lazy (centroids resident, entries seek+read per
// posting on demand); magic present but sidecar missing/corrupt -> eager
// full parse of the v2 blob into a flat `VectorStore`, served by exact
// `flat_knn`; no magic -> legacy v1 parse, also served by `flat_knn`.
// No branch builds an ANN structure — that happens at write time only
// (issue #126: open-path `build_hnsw` was a rayon all-core, minutes-long
// build at bench scale, and admission control turned it into 100% 429s).

/// Magic prefix of a v2 `vector.idx` ("KVE2", little-endian u32).
pub const VECTOR_MAGIC: u32 = u32::from_le_bytes(*b"KVE2");
/// File name of the clustered vector index.
pub const VECTOR_IDX_FILE: &str = "vector.idx";

/// Callback a reader calls with byte ranges of one vector column's idx file
/// (`{segment_dir}/{file}`: `vector.idx`, or `vector.<n>.idx` for column
/// slot `n`, #204) it is about to read but does not hold locally (#245).
/// After it returns,
/// those ranges should be readable from the whole file or the span cache
/// (`span_cache`) for as long as the reader keeps the returned holds (the
/// fetched spans that are only in memory, see `span_cache::PendingSpan`);
/// any still missing fail the search as an IO error.
pub type VectorSpanFetch<'a> =
    &'a (dyn Fn(&Path, &str, &[(u64, u64)]) -> span_cache::SpanHolds + Sync);

/// Postings fetched per call when a restricted kNN probe runs past its
/// budget (see `knn_search_within_filtered`).
const VECTOR_PREFETCH_CHUNK: usize = 8;

/// Most postings one prefetch's scan refetches, one at a time, when a
/// posting it covered is no longer local by the time it is scanned (see
/// [`PrefetchHolds::refetch`]). A backstop for a span file evicted from
/// disk mid-probe, not a second fetch path: past it the scan fails as
/// before rather than make one serial GET per posting of a segment.
const VECTOR_REFETCH_MAX: usize = 8;

/// What [`SegmentReader::prefetch_vector_postings`] keeps readable until
/// the postings it was handed have been scanned: the spans it fetched that
/// are only in memory, and the decoded postings it skipped fetching
/// because the vector cache held them. Those are pinned here, so a cache
/// eviction between the prefetch and the scan (another request's insert,
/// or this probe's own second-touch admissions) cannot leave a skipped
/// posting with no local copy (finding 31). Dropping it early makes those
/// postings unreadable, hence `must_use`, as for [`span_cache::SpanHolds`].
///
/// It also carries the fetch, for the one bounded refetch of a posting
/// that is still not local when scanned (see [`Self::refetch`]).
#[must_use = "the prefetched postings are readable only while their holds live"]
struct PrefetchHolds<'f> {
    spans: span_cache::SpanHolds,
    decoded: HashMap<usize, SharedVectorPosting>,
    fetch: Option<VectorSpanFetch<'f>>,
    refetches: std::sync::atomic::AtomicUsize,
}

impl<'f> PrefetchHolds<'f> {
    fn new(fetch: Option<VectorSpanFetch<'f>>) -> Self {
        Self {
            spans: span_cache::SpanHolds::new(),
            decoded: HashMap::new(),
            fetch,
            refetches: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// The decoded posting `range_idx` the prefetch pinned, if any.
    fn pinned(&self, range_idx: usize) -> Option<&SharedVectorPosting> {
        self.decoded.get(&range_idx)
    }

    /// Fetch `[start, end)` of `file` once more, for a posting that is not
    /// local although the prefetch covered it: its span file was evicted
    /// from disk, or its queued write failed, after the prefetch looked.
    /// `None` without a fetch or once [`VECTOR_REFETCH_MAX`] refetches have
    /// been spent; otherwise the holds of the refetched span, to keep until
    /// it has been read.
    fn refetch(
        &self,
        segment_dir: &Path,
        file: &str,
        start: u64,
        end: u64,
    ) -> Option<span_cache::SpanHolds> {
        let fetch = self.fetch?;
        let spent = self
            .refetches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if spent >= VECTOR_REFETCH_MAX {
            return None;
        }
        eprintln!(
            "WARN: vector posting [{start}, +{}) of {}/{file} not local after its prefetch — refetching it once",
            end - start,
            segment_dir.display()
        );
        Some(fetch(segment_dir, file, &[(start, end)]))
    }
}

/// Magic prefix of the `vector.offsets` sidecar ("KVOF", little-endian u32).
pub const VECTOR_OFFSETS_MAGIC: u32 = u32::from_le_bytes(*b"KVOF");
/// Version 1: vectors and centroids stored exactly as ingested (raw
/// magnitudes) — scoring must normalize per comparison.
pub const VECTOR_INDEX_VERSION: u32 = 1;
/// Version 2: every stored vector and centroid is unit-L2-normalized at
/// write time (zero vectors stay zero). Cosine then collapses to a plain
/// dot product, so the read path skips the two norms + sqrts per
/// comparison that dominate scoring at high `dim`. Scores are unchanged in
/// meaning: dot of unit vectors *is* the cosine of the originals.
pub const UNIT_NORM_VECTOR_INDEX_VERSION: u32 = 2;
/// Version 3: unit-norm (implies everything version 2 does — see
/// `is_unit_norm_vector_index_version`) plus a `radius` field per posting in
/// `vector.offsets`: the max cosine distance from that posting's centroid to
/// any of its live members. Ranking/admitting postings by the
/// triangle-inequality lower bound `(distance(query, centroid) -
/// radius).max(0.0)` instead of raw centroid distance is what lets a
/// "diffuse" posting (mediocre centroid, but a genuinely close member
/// buried inside it) still get probed — see kosha-query's
/// `thin_global_budget_can_bury_the_true_neighbour_in_a_diluted_centroid`
/// for the failure mode this exists to fix at the source rather than
/// compensate for with a wider raw probe budget. Older sidecars (version 1
/// or 2) have no radius field on disk; readers treat that as radius = 0.0,
/// which degrades exactly to today's raw-centroid-distance ranking — no
/// regression, just no improvement, until a segment is rewritten.
pub const RADIUS_AWARE_VECTOR_INDEX_VERSION: u32 = 3;
/// Version 4 (#210): everything version 3 is (unit-norm vectors and
/// centroids, per-posting radius in `vector.offsets`), but each
/// `vector.idx` entry is scalar-quantized to i8: `[id:u32][scale:f32]
/// [i8×dim]` instead of `[id:u32][f32×dim]` — ~4x smaller at embedding
/// dimensions. `scale` is `max|x_i| / 127` of the unit vector, so `x_i ≈
/// scale * code_i` and a query scores with `scale * Σ q_i·code_i` (see
/// `kosha_vector_spfresh::dot_i8`). Centroids and radii stay f32 (they are
/// small and resident) and radii are computed from the exact vectors.
///
/// Written only when `KOSHA_VECTOR_QUANTIZATION` (or its per-field
/// override) asks for `i8`: a reader that predates this version cannot
/// read it, so the flag must stay off until every query pod runs a build
/// that knows it. The footer's `VectorFieldMeta::quantization` records the
/// choice per column; the header version alone decides how entries parse.
pub const I8_VECTOR_INDEX_VERSION: u32 = 4;
/// Version 5: everything version 3 is, but each `vector.idx` entry is a
/// RaBitQ 1-bit code of the entry's residual from its posting centroid
/// (see `kosha_vector_spfresh::encode_rabitq`): `[id:u32]
/// [residual_norm:f32][ip:f32][bits: rabitq_code_bytes(dim)]`. At
/// `dim = 1024` that is 140 bytes instead of 4100 (f32) or 1032 (i8).
/// The code needs its posting's centroid to decode, so a column without a
/// valid `vector.offsets` cannot fall back to the eager flat scan; it is
/// served as degraded instead.
///
/// Scores are estimates with a per-entry error bound; the probe rescores
/// the candidates the bound cannot rule out (at least `k *
/// vector_rescore_factor`) from `doc_vectors.bin`. Same rollout rule as
/// version 4: written only when `KOSHA_VECTOR_QUANTIZATION` (or a per-field
/// override) asks for `rabitq`, which must stay off until every query pod
/// reads this version.
pub const RABITQ_VECTOR_INDEX_VERSION: u32 = 5;

pub fn is_known_vector_index_version(version: u32) -> bool {
    version == VECTOR_INDEX_VERSION
        || version == UNIT_NORM_VECTOR_INDEX_VERSION
        || version == RADIUS_AWARE_VECTOR_INDEX_VERSION
        || version == I8_VECTOR_INDEX_VERSION
        || version == RABITQ_VECTOR_INDEX_VERSION
}

/// Whether `version`'s `vector.idx` entries are i8-quantized (see
/// [`I8_VECTOR_INDEX_VERSION`]). `==`, not `>=`: a later version is free to
/// choose another entry encoding, and must say so here.
pub fn is_i8_vector_index_version(version: u32) -> bool {
    version == I8_VECTOR_INDEX_VERSION
}

/// Whether `version`'s `vector.idx` entries are RaBitQ codes (see
/// [`RABITQ_VECTOR_INDEX_VERSION`]). `==`, like the i8 check.
pub fn is_rabitq_vector_index_version(version: u32) -> bool {
    version == RABITQ_VECTOR_INDEX_VERSION
}

/// Whether `version`'s entries score approximately (i8 or RaBitQ), so the
/// probe rescores its shortlist at full precision.
fn is_quantized_vector_index_version(version: u32) -> bool {
    is_i8_vector_index_version(version) || is_rabitq_vector_index_version(version)
}

/// Bytes of one `vector.idx` entry (id included) for `version`.
fn vector_entry_size(dim: u32, version: u32) -> usize {
    if is_i8_vector_index_version(version) {
        4 + 4 + dim as usize
    } else if is_rabitq_vector_index_version(version) {
        4 + 4 + 4 + kosha_vector_spfresh::rabitq_code_bytes(dim as usize)
    } else {
        4 + 4 * dim as usize
    }
}

/// `>=` rather than `==`: every version from [`UNIT_NORM_VECTOR_INDEX_VERSION`]
/// onward stores unit-L2-normalized vectors/centroids — treating this as a
/// threshold (matching `vector_write_version()`'s own `>= 3` gate) means a
/// future version bump that adds more fields doesn't silently break the
/// dot-only scoring fast path the way an exact `==` check would.
pub fn is_unit_norm_vector_index_version(version: u32) -> bool {
    version >= UNIT_NORM_VECTOR_INDEX_VERSION
}

/// `magic(4) + version(4) + dim(4) + total_count(4)`.
const VECTOR_IDX_HEADER_LEN: usize = 16;
/// `magic(4) + version(4) + dim(4) + posting_count(4)`.
const VECTOR_OFFSETS_HEADER_LEN: usize = 16;

/// One posting's parsed sidecar entry: its centroid plus the byte range in
/// `vector.idx` holding its live entries. `radius` is `0.0` when parsed from
/// a pre-radius-aware sidecar (version 1 or 2) — that degrades ranking
/// exactly to raw centroid distance, the pre-existing behavior, not a wrong
/// answer.
struct VectorPostingIndex {
    centroid: Vec<f32>,
    offset: u64,
    length: u32,
    radius: f32,
}

/// Serializes posting snapshots (as produced by `ClusterIndex::snapshot`) to
/// the v2 `vector.idx` + `vector.offsets` byte layout. A pure function — no
/// I/O — so the format is testable as a byte-level round trip on its own,
/// independent of `StorageBackend`/`SegmentWriter`.
fn encode_vector_index_v2(
    dim: u32,
    postings: &[kosha_vector_spfresh::PostingSnapshot],
    version: u32,
) -> (Vec<u8>, Vec<u8>) {
    let total_count: u32 = postings.iter().map(|p| p.entries.len() as u32).sum();

    let mut idx_buf = Vec::new();
    idx_buf.extend_from_slice(&VECTOR_MAGIC.to_le_bytes());
    idx_buf.extend_from_slice(&version.to_le_bytes());
    idx_buf.extend_from_slice(&dim.to_le_bytes());
    idx_buf.extend_from_slice(&total_count.to_le_bytes());

    let mut offsets_buf = Vec::new();
    offsets_buf.extend_from_slice(&VECTOR_OFFSETS_MAGIC.to_le_bytes());
    offsets_buf.extend_from_slice(&version.to_le_bytes());
    offsets_buf.extend_from_slice(&dim.to_le_bytes());
    offsets_buf.extend_from_slice(&(postings.len() as u32).to_le_bytes());

    let radius_aware = version >= RADIUS_AWARE_VECTOR_INDEX_VERSION;
    let quantized = is_i8_vector_index_version(version);
    let rabitq = is_rabitq_vector_index_version(version)
        .then(|| kosha_vector_spfresh::RaBitQRotation::new(dim as usize));
    let mut codes: Vec<i8> = Vec::with_capacity(dim as usize);
    let mut bits: Vec<u8> = Vec::new();
    for p in postings {
        let offset = idx_buf.len() as u64;
        for (id, v) in &p.entries {
            idx_buf.extend_from_slice(&id.to_le_bytes());
            if let Some(rotation) = &rabitq {
                let code = kosha_vector_spfresh::encode_rabitq(rotation, v, &p.centroid, &mut bits);
                idx_buf.extend_from_slice(&code.residual_norm.to_le_bytes());
                idx_buf.extend_from_slice(&code.ip.to_le_bytes());
                idx_buf.extend_from_slice(&bits);
            } else if quantized {
                let scale = kosha_vector_spfresh::quantize_i8(v, &mut codes);
                idx_buf.extend_from_slice(&scale.to_le_bytes());
                idx_buf.extend(codes.iter().map(|&c| c as u8));
            } else {
                for &x in v {
                    idx_buf.extend_from_slice(&x.to_le_bytes());
                }
            }
        }
        let length = (idx_buf.len() as u64 - offset) as u32;

        for &x in &p.centroid {
            offsets_buf.extend_from_slice(&x.to_le_bytes());
        }
        offsets_buf.extend_from_slice(&offset.to_le_bytes());
        offsets_buf.extend_from_slice(&length.to_le_bytes());
        if radius_aware {
            offsets_buf.extend_from_slice(&p.radius.to_le_bytes());
        }
    }

    (idx_buf, offsets_buf)
}

/// The first `VECTOR_IDX_HEADER_LEN` bytes of a `vector.idx` (fewer if the
/// file is shorter); `None` if it can't be opened or read.
fn read_vector_idx_header(idx_path: &Path) -> Option<Vec<u8>> {
    let mut header = Vec::with_capacity(VECTOR_IDX_HEADER_LEN);
    fs::File::open(idx_path)
        .ok()?
        .take(VECTOR_IDX_HEADER_LEN as u64)
        .read_to_end(&mut header)
        .ok()?;
    Some(header)
}

/// Whether `segment_dir`'s `vector.idx` is the clustered format, which is
/// always written together with a `vector.offsets` sidecar. Without the
/// sidecar such a segment is still searchable, but only by exact scan.
pub fn vector_index_needs_offsets(segment_dir: &Path) -> bool {
    vector_file_needs_offsets(segment_dir, VECTOR_IDX_FILE)
}

/// [`vector_index_needs_offsets`] for any vector column's idx file
/// (`vector.idx` or `vector.<n>.idx`, #204). `false` when the file is not
/// local.
pub fn vector_file_needs_offsets(segment_dir: &Path, idx_file: &str) -> bool {
    read_vector_idx_header(&segment_dir.join(idx_file))
        .is_some_and(|header| parse_vector_idx_header(&header).is_some())
}

/// One vector column a kNN on some field reads from a segment (#204) — see
/// [`knn_vector_columns`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnnVectorColumn {
    /// `vector.idx` or `vector.<n>.idx`.
    pub idx_file: String,
    /// Its offsets sidecar, when the column has one — or, for a legacy
    /// segment, may have one (only the idx header tells, see
    /// [`vector_file_needs_offsets`]).
    pub offsets_file: Option<String>,
    /// The single unnamed index of a segment written before vector
    /// columns existed.
    pub legacy: bool,
}

/// The vector columns a kNN on `field` reads, decided from the segment's
/// footer (#204): `field`'s own column when the footer lists columns
/// (nothing when it has none — another column's files are never fetched
/// for this query), else the one unnamed legacy index. `None` (no footer
/// known) is treated as legacy.
pub fn knn_vector_columns(footer: Option<&Footer>, field: &str) -> Vec<KnnVectorColumn> {
    match footer.and_then(|f| f.vector_fields.as_ref()) {
        Some(fields) => fields
            .iter()
            .filter(|meta| meta.name == field)
            .map(|meta| KnnVectorColumn {
                idx_file: meta.idx_file(),
                offsets_file: meta.offsets.then(|| meta.offsets_file()),
                legacy: false,
            })
            .collect(),
        None => vec![KnnVectorColumn {
            idx_file: VECTOR_IDX_FILE.to_string(),
            offsets_file: Some("vector.offsets".to_string()),
            legacy: true,
        }],
    }
}

/// Whether a kNN on a segment with this footer needs `doc_vectors.bin`
/// local: a legacy segment (no `vector_fields`) that stores its vectors
/// there, since the sidecar's field table is what binds its unnamed index
/// to a field name (#204, see [`SegmentReader::vector_field_status`]).
pub fn knn_needs_doc_vectors(footer: Option<&Footer>) -> bool {
    footer.is_some_and(|f| f.vector_fields.is_none() && f.doc_vectors)
}

/// Whether a query node should hold this segment's `doc_vectors.bin`
/// WHOLE, i.e. whether background promotion and warm-up fetch it.
///
/// Only a quantized (i8 / RaBitQ) column needs the whole file: the
/// default per-segment kNN path rescores its shortlist from it, and
/// without it returns quantized estimates as final scores
/// ([`SegmentReader::rescores_quantized_locally`]). An f32 column never
/// rescores, and every other reader works from ranges — page documents'
/// vectors ([`SegmentReader::doc_is_local`]), exact filtered kNN
/// (`knn_exact_filtered_fetching`) and global rescoring
/// (`knn_exact_scores_fetching`) — so for f32 columns the whole file (as
/// large as the f32 vector index) is never worth its download or its
/// disk. A legacy footer that lists no columns cannot say, so it keeps
/// the whole file as before. `false` when the segment has no sidecar.
pub fn doc_vectors_kept_whole(footer: &Footer) -> bool {
    footer.doc_vectors
        && footer
            .vector_fields
            .as_ref()
            .is_none_or(|cols| cols.iter().any(|c| !c.quantization.is_f32()))
}

/// An upper bound on `doc_vectors.bin`'s header length from the footer
/// alone, at least [`DOC_VECTORS_HEADER_PROBE`]: 12 bytes per entry, at
/// most one entry per document per vector column, plus the field table.
/// Reading this many bytes first fetches the whole header in one round
/// (any surplus is the first vectors). A legacy footer that lists no
/// columns gives the plain probe.
pub fn doc_vectors_header_estimate(footer: &Footer) -> u64 {
    let Some(cols) = footer.vector_fields.as_ref() else {
        return DOC_VECTORS_HEADER_PROBE;
    };
    let names: u64 = cols.iter().map(|c| 4 + c.name.len() as u64).sum();
    let entries = footer.doc_count as u64 * cols.len() as u64;
    (12 + names + 12 * entries).max(DOC_VECTORS_HEADER_PROBE)
}

/// [`doc_vectors_kept_whole`] for `segment_dir`'s local `footer.json`;
/// `false` if it can't be read (as [`segment_uses_doc_vectors`]).
pub fn segment_keeps_doc_vectors_whole(segment_dir: &Path) -> bool {
    SegmentReader::read_footer(segment_dir).is_ok_and(|footer| doc_vectors_kept_whole(&footer))
}

/// The files a kNN on `field` needs from `segment_dir` (#204), each paired
/// with whether the segment cannot be searched without it. Decided from
/// the local `footer.json`:
///
/// * a footer listing vector columns needs just `field`'s column (nothing
///   when it has none) — another column's files are never fetched for
///   this query;
/// * a legacy footer (or none readable) needs the one unnamed index —
///   `vector.offsets` only when `vector.idx` is in the clustered format,
///   see [`vector_index_needs_offsets`] — plus `doc_vectors.bin` when the
///   footer says vectors live there, since its field table is what binds
///   the unnamed index to a field name
///   (see [`SegmentReader::vector_field_status`]).
pub fn knn_vector_files(segment_dir: &Path, field: &str) -> Vec<(String, bool)> {
    let footer = SegmentReader::read_footer(segment_dir).ok();
    let mut files = Vec::new();
    for column in knn_vector_columns(footer.as_ref(), field) {
        if let Some(offsets) = column.offsets_file {
            let required =
                !column.legacy || vector_file_needs_offsets(segment_dir, &column.idx_file);
            files.push((column.idx_file, true));
            files.push((offsets, required));
        } else {
            files.push((column.idx_file, true));
        }
    }
    if knn_needs_doc_vectors(footer.as_ref()) {
        files.push((DOC_VECTORS_FILE.to_string(), true));
    }
    files
}

/// Every vector-index file `segment_dir`'s local footer lists (#204): each
/// column's idx file plus its offsets sidecar. A legacy footer (or none
/// readable) lists `vector.idx` and `vector.offsets`, whether or not the
/// segment has them.
pub fn vector_index_files(segment_dir: &Path) -> Vec<String> {
    match SegmentReader::read_footer(segment_dir)
        .ok()
        .and_then(|f| f.vector_fields)
    {
        Some(fields) => fields
            .iter()
            .flat_map(|meta| {
                let mut files = vec![meta.idx_file()];
                if meta.offsets {
                    files.push(meta.offsets_file());
                }
                files
            })
            .collect(),
        None => vec!["vector.idx".to_string(), "vector.offsets".to_string()],
    }
}

/// Whether every vector file `footer` says the segment has is local: each
/// column's idx file, its offsets sidecar where the writer emitted one,
/// and `doc_vectors.bin` when vectors are stored there (#243). For callers
/// that need a segment's vectors in full (merges, rewrites), as
/// [`filter_columns::filter_files_present`] is for its filter index.
///
/// A legacy footer (no `vector_fields`) cannot say whether the segment has
/// a vector index, so only its `doc_vectors.bin` is checked: requiring
/// `vector.idx` there would make every vectorless legacy segment
/// permanently incomplete.
pub fn vector_files_present(segment_dir: &Path, footer: &Footer) -> bool {
    let present = |name: &str| segment_dir.join(name).is_file();
    footer
        .vector_fields
        .iter()
        .flatten()
        .all(|meta| present(&meta.idx_file()) && (!meta.offsets || present(&meta.offsets_file())))
        && (!footer.doc_vectors || present(DOC_VECTORS_FILE))
}

/// Parses a v2 `vector.idx`'s header only — `(dim, total_count, version)`.
/// `None` for anything that isn't a valid, recognized-version v2 header
/// (legacy file, truncated, corrupt, unknown version) — callers fall back
/// accordingly, never panic on untrusted disk content.
fn parse_vector_idx_header(data: &[u8]) -> Option<(u32, u32, u32)> {
    if data.len() < VECTOR_IDX_HEADER_LEN || data[0..4] != VECTOR_MAGIC.to_le_bytes() {
        return None;
    }
    let mut cursor = &data[4..];
    let version = read_u32_le(&mut cursor);
    if !is_known_vector_index_version(version) {
        return None;
    }
    let dim = read_u32_le(&mut cursor);
    let total_count = read_u32_le(&mut cursor);
    Some((dim, total_count, version))
}

/// Eager fallback: parses the *entire* v2 blob into a flat vector list,
/// ignoring posting boundaries — the same shape `read_vectors` already
/// produces for legacy files. Used when the header is valid but
/// `vector.offsets` is missing/corrupt, so vector search still works (just
/// without the lazy-hydration benefit) instead of losing it for the segment
/// entirely. Validates the file's total length against the header's
/// `total_count` before trusting any per-record read — a truncated file
/// returns `None`, same "never trust blindly" discipline as
/// `try_read_doc_index`'s doc-count cross-check.
fn parse_vector_idx_eager(data: &[u8]) -> Option<VectorStore> {
    let (dim, total_count, version) = parse_vector_idx_header(data)?;
    if is_rabitq_vector_index_version(version) {
        // RaBitQ codes are residuals from posting centroids, which live in
        // the sidecar this fallback is serving without.
        return None;
    }
    let record_size = vector_entry_size(dim, version);
    // Checked: a corrupt header's dim and count can overflow the product
    // (#257 fuzzing), which must read as corruption, not wrap or panic.
    let expected_len = (total_count as usize)
        .checked_mul(record_size)
        .and_then(|body| body.checked_add(VECTOR_IDX_HEADER_LEN))?;
    if data.len() != expected_len {
        return None;
    }
    // An i8 column served without its sidecar is dequantized: the flat
    // scan only knows f32 vectors, and this path is a fallback anyway.
    let vectors = match decode_posting_entries(&data[VECTOR_IDX_HEADER_LEN..], dim, version) {
        VectorPosting::RaBitQ { .. } => return None,
        posting => posting.dequantized(),
    };
    Some(VectorStore {
        vectors,
        dimensions: dim as usize,
    })
}

/// Parses `vector.offsets` into the lazy posting index. `expected_dim` and
/// `expected_version` cross-check against `vector.idx`'s own header (already
/// parsed by the caller) — any mismatch is treated as corruption, matching
/// the same "never trust one file over the other" discipline
/// `try_read_doc_index` applies to its doc-count check. The version check
/// matters since unit-norm (v2): a stale sidecar paired with a rewritten
/// blob could otherwise flip the scoring path. Also validates total file
/// length against the header-implied size before trusting any per-record
/// read.
fn parse_vector_offsets(
    data: &[u8],
    expected_dim: u32,
    expected_version: u32,
) -> Option<Vec<VectorPostingIndex>> {
    if data.len() < VECTOR_OFFSETS_HEADER_LEN || data[0..4] != VECTOR_OFFSETS_MAGIC.to_le_bytes() {
        return None;
    }
    let mut cursor = &data[4..];
    let version = read_u32_le(&mut cursor);
    if !is_known_vector_index_version(version) || version != expected_version {
        return None;
    }
    let dim = read_u32_le(&mut cursor);
    if dim != expected_dim {
        return None;
    }
    let posting_count = read_u32_le(&mut cursor);
    let radius_aware = version >= RADIUS_AWARE_VECTOR_INDEX_VERSION;
    let record_size = 4 * dim as usize + 8 + 4 + if radius_aware { 4 } else { 0 };
    // Checked, like `parse_vector_idx_eager`: overflow is corruption.
    let expected_len = (posting_count as usize)
        .checked_mul(record_size)
        .and_then(|body| body.checked_add(VECTOR_OFFSETS_HEADER_LEN))?;
    if data.len() != expected_len {
        return None;
    }
    let mut out = Vec::with_capacity(posting_count as usize);
    for _ in 0..posting_count {
        let mut centroid = Vec::with_capacity(dim as usize);
        for _ in 0..dim {
            centroid.push(read_f32_le(&mut cursor));
        }
        let offset = read_u64_le(&mut cursor);
        let length = read_u32_le(&mut cursor);
        let radius = if radius_aware {
            read_f32_le(&mut cursor)
        } else {
            0.0
        };
        out.push(VectorPostingIndex {
            centroid,
            offset,
            length,
            radius,
        });
    }
    Some(out)
}

/// Parse a `vector.idx` / `vector.offsets` pair with the same parsers a
/// segment open uses (header, lazy sidecar, eager fallback), returning the
/// number of postings and of vectors recovered. For property tests and
/// fuzzing (#257): never panics, whatever the bytes.
#[doc(hidden)]
pub fn parse_vector_index_files(idx: &[u8], offsets: &[u8]) -> (Option<usize>, Option<usize>) {
    let Some((dim, _, version)) = parse_vector_idx_header(idx) else {
        return (None, None);
    };
    let postings = parse_vector_offsets(offsets, dim, version);
    if let Some(postings) = &postings {
        // Slice each posting the way a lazy read would, bounds-checked.
        for p in postings {
            let start = usize::try_from(p.offset).unwrap_or(usize::MAX);
            if let Some(bytes) = start
                .checked_add(p.length as usize)
                .and_then(|end| idx.get(start..end))
            {
                let _ = decode_posting_entries(bytes, dim, version);
            }
        }
    }
    let vectors = parse_vector_idx_eager(idx).map(|store| store.vectors.len());
    (postings.map(|p| p.len()), vectors)
}

/// Decodes one posting's raw entry bytes (sliced from `vector.idx` via a
/// `VectorPostingIndex`'s `offset`/`length`) for an index of `version`.
fn decode_posting_entries(mut cursor: &[u8], dim: u32, version: u32) -> VectorPosting {
    // record_size is always >= 4 (id alone, even at dim=0), so this never divides by zero.
    let record_size = vector_entry_size(dim, version);
    let count = cursor.len() / record_size;
    if is_i8_vector_index_version(version) {
        let dim = dim as usize;
        let mut ids = Vec::with_capacity(count);
        let mut scales = Vec::with_capacity(count);
        let mut codes = Vec::with_capacity(count * dim);
        for _ in 0..count {
            ids.push(read_u32_le(&mut cursor));
            scales.push(read_f32_le(&mut cursor));
            codes.extend(cursor[..dim].iter().map(|&b| b as i8));
            cursor = &cursor[dim..];
        }
        return VectorPosting::I8 {
            dim,
            ids,
            scales,
            codes,
        };
    }
    if is_rabitq_vector_index_version(version) {
        let code_bytes = kosha_vector_spfresh::rabitq_code_bytes(dim as usize);
        let mut ids = Vec::with_capacity(count);
        let mut codes = Vec::with_capacity(count);
        let mut bits = Vec::with_capacity(count * code_bytes);
        for _ in 0..count {
            ids.push(read_u32_le(&mut cursor));
            let residual_norm = read_f32_le(&mut cursor);
            let ip = read_f32_le(&mut cursor);
            codes.push(kosha_vector_spfresh::RaBitQCode { residual_norm, ip });
            bits.extend_from_slice(&cursor[..code_bytes]);
            cursor = &cursor[code_bytes..];
        }
        return VectorPosting::RaBitQ {
            code_bytes,
            ids,
            codes,
            bits,
        };
    }
    let dim = dim as usize;
    let mut ids = Vec::with_capacity(count);
    let mut vectors = Vec::with_capacity(count * dim);
    decode_f32_entries_into(cursor, dim, &mut ids, &mut vectors);
    VectorPosting::F32 { dim, ids, vectors }
}

/// Decodes a posting's f32 entries (`[doc_seq: u32][dim × f32]`, little
/// endian) into `ids` and the flat `vectors`, replacing what they held:
/// entry `i` is `ids[i]` with `vectors[i * dim..(i + 1) * dim]`. Each
/// vector is converted as one run of 4-byte chunks rather than a cursor
/// read per float, and nothing is allocated when the two `Vec`s already
/// have the capacity (#435: a probe reuses them across its postings).
/// Trailing bytes short of one entry are ignored.
fn decode_f32_entries_into(bytes: &[u8], dim: usize, ids: &mut Vec<u32>, vectors: &mut Vec<f32>) {
    ids.clear();
    vectors.clear();
    // At least 4 (the id alone, at dim 0), so `chunks_exact` cannot panic.
    let record_size = 4 + 4 * dim;
    let count = bytes.len() / record_size;
    ids.reserve(count);
    vectors.reserve(count * dim);
    for entry in bytes.chunks_exact(record_size) {
        let (id, floats) = entry.split_at(4);
        ids.push(u32::from_le_bytes([id[0], id[1], id[2], id[3]]));
        let (floats, _) = floats.as_chunks::<4>();
        vectors.extend(floats.iter().map(|b| f32::from_le_bytes(*b)));
    }
}

/// A posting's f32 entries viewed in place, without decoding: entry `i` is
/// `words[i * (dim + 1)]` (the id's bits) followed by its `dim` floats.
/// `None` when `bytes` is not 4-byte aligned or the target is big endian;
/// the caller then decodes with [`decode_f32_entries_into`]. Both give the
/// same ids and the same float bits. Trailing bytes short of one entry are
/// ignored.
fn f32_entries_in_place(bytes: &[u8], dim: usize) -> Option<&[f32]> {
    if !cfg!(target_endian = "little") {
        return None;
    }
    let record_size = 4 + 4 * dim;
    let whole = bytes.len() / record_size * record_size;
    bytemuck::try_cast_slice(&bytes[..whole]).ok()
}

/// [`for_each_f32_similarity`] over entries viewed in place by
/// [`f32_entries_in_place`].
fn for_each_f32_similarity_in_place(
    words: &[f32],
    dim: usize,
    accept: Option<&dyn Fn(u32) -> bool>,
    f32_similarity: impl Fn(&[f32]) -> f32,
    mut emit: impl FnMut(u32, f32, f32),
) {
    for entry in words.chunks_exact(dim + 1) {
        let (id, vector) = entry.split_at(1);
        let id = id[0].to_bits();
        if accept.is_some_and(|accept| !accept(id)) {
            continue;
        }
        emit(id, f32_similarity(vector), 0.0);
    }
}

/// Calls `emit(id, similarity, 0.0)` for each flat f32 entry `accept` lets
/// through (see [`decode_f32_entries_into`] for the layout).
fn for_each_f32_similarity(
    ids: &[u32],
    vectors: &[f32],
    dim: usize,
    accept: Option<&dyn Fn(u32) -> bool>,
    f32_similarity: impl Fn(&[f32]) -> f32,
    mut emit: impl FnMut(u32, f32, f32),
) {
    for (i, &id) in ids.iter().enumerate() {
        if accept.is_some_and(|accept| !accept(id)) {
            continue;
        }
        emit(id, f32_similarity(&vectors[i * dim..(i + 1) * dim]), 0.0);
    }
}

/// What [`VectorPosting::resident_bytes`] reports for an f32 posting of
/// `count` entries, known before it is decoded.
fn f32_posting_resident_bytes(count: usize, dim: usize) -> usize {
    std::mem::size_of::<VectorPosting>() + count * 4 + count * dim * 4
}

/// One vector posting's decoded live entries, in the encoding its
/// `vector.idx` stores (#210). Cached as-is, so an i8 column's posting
/// cache holds ~4x more postings in the same bytes.
#[derive(Debug, Clone, PartialEq)]
enum VectorPosting {
    /// Entry `i` is `ids[i]`, vector `vectors[i * dim..(i + 1) * dim]`:
    /// flat like the other variants (#435), so a cached posting is two
    /// allocations and its scan walks one contiguous run of floats.
    F32 {
        dim: usize,
        ids: Vec<u32>,
        vectors: Vec<f32>,
    },
    /// Entry `i` is `ids[i]`, scale `scales[i]`, codes
    /// `codes[i * dim..(i + 1) * dim]`.
    I8 {
        dim: usize,
        ids: Vec<u32>,
        scales: Vec<f32>,
        codes: Vec<i8>,
    },
    /// RaBitQ codes (version 5). Entry `i` is `ids[i]`, `codes[i]`, bits
    /// `bits[i * code_bytes..(i + 1) * code_bytes]`. Only scorable with the
    /// posting's centroid (see [`ProbeQuery`]).
    RaBitQ {
        code_bytes: usize,
        ids: Vec<u32>,
        codes: Vec<kosha_vector_spfresh::RaBitQCode>,
        bits: Vec<u8>,
    },
}

/// A kNN query prepared once per vector column, for scoring that column's
/// postings with [`VectorPosting::for_each_similarity`].
struct ProbeQuery<'q> {
    /// The query unit-normalized for quantized columns (whose entries
    /// score against a unit query); the raw query otherwise, unused there.
    unit: std::borrow::Cow<'q, [f32]>,
    /// The rotated query lookup table, for a RaBitQ column.
    rabitq: Option<std::rc::Rc<kosha_vector_spfresh::RaBitQQuery>>,
    /// An i8 entry's error bound per unit of its `scale` (#381): the
    /// estimate `scale * <q, code>` is off from the exact `<q, x>` by at
    /// most `scale * i8_error_per_scale`. See [`i8_error_per_scale`].
    /// `0.0` for every other column, and never read by the default
    /// per-segment probe (which rescores a fixed `k * factor` of an i8
    /// column regardless of bounds).
    i8_error_per_scale: f32,
}

/// How an i8 entry's error bound is derived (#381, global rescoring only;
/// `KOSHA_KNN_I8_BOUND`).
///
/// * `strict` (default): each coordinate was rounded to the nearest code,
///   so `|x_i - scale * code_i| <= scale / 2` and the estimate is within
///   `scale / 2 * ||q||_1` of the exact dot product. A guarantee, but a
///   loose one at high dimension (`||q||_1` of a unit 1024-dim query is
///   ~25).
/// * `probabilistic`: treats the rounding errors as independent and
///   uniform on `[-scale/2, scale/2]`, so the error has standard deviation
///   `scale * ||q||_2 / sqrt(12)`; the bound is `RABITQ_EPSILON0` of those
///   (the same confidence the RaBitQ bound uses). Not a guarantee.
fn i8_error_per_scale(unit_query: &[f32]) -> f32 {
    static PROBABILISTIC: OnceLock<bool> = OnceLock::new();
    let probabilistic = *PROBABILISTIC.get_or_init(|| {
        matches!(
            std::env::var("KOSHA_KNN_I8_BOUND")
                .ok()
                .as_deref()
                .map(str::trim),
            Some("probabilistic")
        )
    });
    // Slack for f32 accumulation in `dot_i8` and in the exact cosine the
    // bound is compared against.
    const SLACK: f32 = 1e-4;
    if probabilistic {
        let l2 = unit_query.iter().map(|x| x * x).sum::<f32>().sqrt();
        kosha_vector_spfresh::RABITQ_EPSILON0 * l2 / 12f32.sqrt() + SLACK
    } else {
        0.5 * unit_query.iter().map(|x| x.abs()).sum::<f32>() + SLACK
    }
}

/// `KOSHA_VECTOR_DOT_LANES`, read once: accumulator lanes of the f32 dot
/// that kNN posting scans score f32 entries with (#382). `8` (the default)
/// is [`kosha_vector_spfresh::dot`], exactly as before; `16`, `32` or `64`
/// use [`kosha_vector_spfresh::dot_lanes`], whose sums differ in the last
/// float bits. Experimental: for measuring the FMA latency chain on the
/// bench host, together with the image's `KOSHA_RUSTFLAGS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DotLanes {
    Eight,
    Sixteen,
    ThirtyTwo,
    SixtyFour,
}

impl DotLanes {
    fn from_env() -> Self {
        static LANES: OnceLock<DotLanes> = OnceLock::new();
        *LANES.get_or_init(|| Self::parse(std::env::var("KOSHA_VECTOR_DOT_LANES").ok().as_deref()))
    }

    fn parse(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            None | Some("") | Some("8") => Self::Eight,
            Some("16") => Self::Sixteen,
            Some("32") => Self::ThirtyTwo,
            Some("64") => Self::SixtyFour,
            Some(other) => {
                eprintln!("WARN: KOSHA_VECTOR_DOT_LANES={other} not 8|16|32|64 — using 8");
                Self::Eight
            }
        }
    }

    fn dot(self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Self::Eight => kosha_vector_spfresh::dot(a, b),
            Self::Sixteen => kosha_vector_spfresh::dot_lanes::<16>(a, b),
            Self::ThirtyTwo => kosha_vector_spfresh::dot_lanes::<32>(a, b),
            Self::SixtyFour => kosha_vector_spfresh::dot_lanes::<64>(a, b),
        }
    }

    /// [`kosha_vector_spfresh::cosine_similarity`] with this dot.
    fn cosine_similarity(self, a: &[f32], b: &[f32]) -> f32 {
        if self == Self::Eight {
            return kosha_vector_spfresh::cosine_similarity(a, b);
        }
        let d = self.dot(a, b);
        let (na, nb) = (self.dot(a, a).sqrt(), self.dot(b, b).sqrt());
        if na == 0.0 || nb == 0.0 {
            return 0.0;
        }
        1.0 - (1.0 - (d / (na * nb)).clamp(-1.0, 1.0))
    }
}

/// `KOSHA_VECTOR_RABITQ_ESTIMATOR`, read once: how a RaBitQ column's codes
/// are scored at query time (#382). `lut` (the default) is the per-byte
/// lookup table; `popcount` quantizes the rotated query to 4 bits and
/// scores with AND + popcount (RaBitQ §3.3.2). Query-time only: the
/// on-disk codes are the same either way.
fn rabitq_estimator() -> kosha_vector_spfresh::RaBitQEstimator {
    static ESTIMATOR: OnceLock<kosha_vector_spfresh::RaBitQEstimator> = OnceLock::new();
    *ESTIMATOR.get_or_init(|| {
        parse_rabitq_estimator(
            std::env::var("KOSHA_VECTOR_RABITQ_ESTIMATOR")
                .ok()
                .as_deref(),
        )
    })
}

fn parse_rabitq_estimator(raw: Option<&str>) -> kosha_vector_spfresh::RaBitQEstimator {
    use kosha_vector_spfresh::RaBitQEstimator;
    match raw.map(str::trim) {
        None | Some("") => RaBitQEstimator::Lut,
        Some(raw) => RaBitQEstimator::parse(raw).unwrap_or_else(|| {
            eprintln!("WARN: KOSHA_VECTOR_RABITQ_ESTIMATOR={raw} not lut|popcount — using lut");
            RaBitQEstimator::Lut
        }),
    }
}

/// x86-64 target features above the baseline that this binary was compiled
/// with (`-C target-cpu=x86-64-v3` turns on `avx2`, `fma`, `bmi2` and
/// `popcnt`). Empty on the default image and on other architectures.
fn compiled_x86_features() -> Vec<&'static str> {
    let x86 = cfg!(target_arch = "x86_64");
    [
        ("popcnt", cfg!(target_feature = "popcnt")),
        ("avx2", cfg!(target_feature = "avx2")),
        ("fma", cfg!(target_feature = "fma")),
        ("bmi2", cfg!(target_feature = "bmi2")),
        ("avx512f", cfg!(target_feature = "avx512f")),
    ]
    .into_iter()
    .filter(|&(_, enabled)| x86 && enabled)
    .map(|(name, _)| name)
    .collect()
}

/// The experimental #382 kernel configuration in effect, for one startup
/// log line: `None` when everything is at its default (neither
/// `KOSHA_VECTOR_DOT_LANES` nor `KOSHA_VECTOR_RABITQ_ESTIMATOR` set, and a
/// baseline-x86-64 build), so a default server logs nothing new. Reading it
/// also resolves both settings, so an unknown value warns at startup rather
/// than on the first kNN query. The benchmark checks this line to confirm
/// that a run's `server_env` / `rustflags` actually took effect.
pub fn experimental_vector_kernels_summary() -> Option<String> {
    let env_set = |key: &str| {
        std::env::var(key)
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
    };
    let lanes = DotLanes::from_env();
    let estimator = rabitq_estimator();
    let features = compiled_x86_features();
    let any_set = env_set("KOSHA_VECTOR_DOT_LANES")
        || env_set("KOSHA_VECTOR_RABITQ_ESTIMATOR")
        || !features.is_empty();
    any_set.then(|| format_vector_kernels_summary(lanes, estimator, &features))
}

fn format_vector_kernels_summary(
    lanes: DotLanes,
    estimator: kosha_vector_spfresh::RaBitQEstimator,
    features: &[&str],
) -> String {
    let lanes = match lanes {
        DotLanes::Eight => 8,
        DotLanes::Sixteen => 16,
        DotLanes::ThirtyTwo => 32,
        DotLanes::SixtyFour => 64,
    };
    let estimator = match estimator {
        kosha_vector_spfresh::RaBitQEstimator::Lut => "lut",
        kosha_vector_spfresh::RaBitQEstimator::Popcount4 => "popcount",
    };
    let features = if features.is_empty() {
        "baseline".to_string()
    } else {
        features.join(",")
    };
    format!(
        "vector kernels (#382, experimental): dot_lanes={lanes} \
         rabitq_estimator={estimator} x86_features={features}"
    )
}

/// The [`kosha_vector_spfresh::RaBitQQuery`] for `unit_query`. It depends
/// only on the query and its dimension (every RaBitQ code is written with
/// the same rotation), so each thread keeps the last one it built: a query
/// probing many segments builds its lookup table once per scoring thread,
/// not once per segment.
fn rabitq_query_for(unit_query: &[f32]) -> std::rc::Rc<kosha_vector_spfresh::RaBitQQuery> {
    type Last = Option<(Vec<f32>, std::rc::Rc<kosha_vector_spfresh::RaBitQQuery>)>;
    thread_local! {
        static LAST: std::cell::RefCell<Last> = const { std::cell::RefCell::new(None) };
    }
    LAST.with(|last| {
        let mut last = last.borrow_mut();
        if let Some((query, built)) = last.as_ref() {
            if query.as_slice() == unit_query {
                return std::rc::Rc::clone(built);
            }
        }
        let rotation = kosha_vector_spfresh::RaBitQRotation::new(unit_query.len());
        let built = std::rc::Rc::new(kosha_vector_spfresh::RaBitQQuery::with_estimator(
            &rotation,
            unit_query,
            rabitq_estimator(),
        ));
        *last = Some((unit_query.to_vec(), std::rc::Rc::clone(&built)));
        built
    })
}

impl VectorPosting {
    /// Approximate heap + inline bytes, for the posting cache's budget.
    fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                VectorPosting::F32 { ids, vectors, .. } => ids.len() * 4 + vectors.len() * 4,
                VectorPosting::I8 {
                    ids, scales, codes, ..
                } => ids.len() * 4 + scales.len() * 4 + codes.len(),
                VectorPosting::RaBitQ {
                    ids, codes, bits, ..
                } => {
                    ids.len() * 4
                        + codes.len() * std::mem::size_of::<kosha_vector_spfresh::RaBitQCode>()
                        + bits.len()
                }
            }
    }

    /// Calls `emit(id, similarity, error_bound)` for every entry `accept`
    /// lets through (all of them when `None`). `f32_similarity` scores an
    /// f32 entry — each caller keeps the formula it has always used. An i8
    /// entry is scored against `probe.unit` (the query, unit-normalized) as
    /// `scale * Σ q_i·code_i`, clamped like the f32 dot path: the stored
    /// vector is unit-norm, so that is its cosine. A RaBitQ entry is
    /// estimated from `probe.rabitq` and `centroid` (this posting's
    /// centroid, unit-norm), with RaBitQ's error bound; f32 and i8 entries
    /// report a bound of `0`.
    fn for_each_similarity(
        &self,
        accept: Option<&dyn Fn(u32) -> bool>,
        f32_similarity: impl Fn(&[f32]) -> f32,
        probe: &ProbeQuery<'_>,
        centroid: &[f32],
        mut emit: impl FnMut(u32, f32, f32),
    ) {
        match self {
            VectorPosting::F32 { dim, ids, vectors } => {
                for_each_f32_similarity(ids, vectors, *dim, accept, f32_similarity, emit);
            }
            VectorPosting::I8 {
                dim,
                ids,
                scales,
                codes,
            } => {
                for (i, (&id, &scale)) in ids.iter().zip(scales).enumerate() {
                    if accept.is_some_and(|accept| !accept(id)) {
                        continue;
                    }
                    let entry = &codes[i * dim..(i + 1) * dim];
                    let sim = kosha_vector_spfresh::dot_i8(&probe.unit, entry, scale);
                    emit(id, sim.clamp(-1.0, 1.0), scale * probe.i8_error_per_scale);
                }
            }
            VectorPosting::RaBitQ {
                code_bytes,
                ids,
                codes,
                bits,
            } => {
                let Some(query) = probe.rabitq.as_ref() else {
                    return;
                };
                let centroid_dot = kosha_vector_spfresh::dot(&probe.unit, centroid);
                for (i, (&id, code)) in ids.iter().zip(codes).enumerate() {
                    if accept.is_some_and(|accept| !accept(id)) {
                        continue;
                    }
                    let entry = &bits[i * code_bytes..(i + 1) * code_bytes];
                    let (sim, err) = query.estimate(code, entry, centroid_dot);
                    emit(id, sim.clamp(-1.0, 1.0), err);
                }
            }
        }
    }

    /// The entries as f32 vectors (an i8 posting dequantized). RaBitQ
    /// entries cannot be decoded without their centroid: ids only, with
    /// empty vectors — callers that need vectors exclude version 5 first.
    fn dequantized(self) -> Vec<(u32, Vec<f32>)> {
        match self {
            VectorPosting::RaBitQ { ids, .. } => {
                ids.into_iter().map(|id| (id, Vec::new())).collect()
            }
            VectorPosting::F32 { dim, ids, vectors } => ids
                .into_iter()
                .enumerate()
                .map(|(i, id)| (id, vectors[i * dim..(i + 1) * dim].to_vec()))
                .collect(),
            VectorPosting::I8 {
                dim,
                ids,
                scales,
                codes,
            } => ids
                .into_iter()
                .zip(scales)
                .enumerate()
                .map(|(i, (id, scale))| {
                    let entry = &codes[i * dim..(i + 1) * dim];
                    (id, kosha_vector_spfresh::dequantize_i8(entry, scale))
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod posting_scan_tests;

#[cfg(test)]
mod centroid_codes_tests;

#[cfg(test)]
mod centroid_cap_tests;

#[cfg(test)]
mod vector_index_v2_tests {
    use super::*;

    fn sample_postings() -> Vec<kosha_vector_spfresh::PostingSnapshot> {
        vec![
            kosha_vector_spfresh::PostingSnapshot {
                centroid: vec![1.0, 0.0],
                entries: vec![(0, vec![1.0, 0.0]), (1, vec![0.9, 0.1])],
                radius: 0.0,
            },
            kosha_vector_spfresh::PostingSnapshot {
                centroid: vec![0.0, 1.0],
                entries: vec![(2, vec![0.0, 1.0])],
                radius: 0.0,
            },
        ]
    }

    /// Distinct, non-zero, non-uniform radii so a round trip is a real
    /// check — `sample_postings()` above stays radius-0.0 for the format
    /// tests that predate radius-awareness and aren't about it.
    fn sample_postings_with_radius() -> Vec<kosha_vector_spfresh::PostingSnapshot> {
        vec![
            kosha_vector_spfresh::PostingSnapshot {
                centroid: vec![1.0, 0.0],
                entries: vec![(0, vec![1.0, 0.0]), (1, vec![0.9, 0.1])],
                radius: 0.123,
            },
            kosha_vector_spfresh::PostingSnapshot {
                centroid: vec![0.0, 1.0],
                entries: vec![(2, vec![0.0, 1.0])],
                radius: 0.456,
            },
        ]
    }

    #[test]
    fn round_trips_header_and_posting_boundaries() {
        let postings = sample_postings();
        let (idx_bytes, offsets_bytes) = encode_vector_index_v2(2, &postings, VECTOR_INDEX_VERSION);

        let (dim, total_count, version) =
            parse_vector_idx_header(&idx_bytes).expect("valid v2 header");
        assert_eq!(version, VECTOR_INDEX_VERSION);
        assert_eq!(dim, 2);
        assert_eq!(total_count, 3);

        let parsed_offsets =
            parse_vector_offsets(&offsets_bytes, dim, VECTOR_INDEX_VERSION).expect("valid sidecar");
        assert_eq!(parsed_offsets.len(), 2);
        assert_eq!(parsed_offsets[0].centroid, vec![1.0, 0.0]);
        assert_eq!(parsed_offsets[1].centroid, vec![0.0, 1.0]);

        for (i, entry) in parsed_offsets.iter().enumerate() {
            let slice =
                &idx_bytes[entry.offset as usize..(entry.offset as usize + entry.length as usize)];
            let decoded = decode_posting_entries(slice, dim, VECTOR_INDEX_VERSION);
            assert_eq!(
                f32_posting_resident_bytes(postings[i].entries.len(), dim as usize),
                decoded.resident_bytes()
            );
            assert_eq!(decoded.dequantized(), postings[i].entries);
        }
    }

    #[test]
    fn radius_round_trips_only_at_radius_aware_version() {
        let postings = sample_postings_with_radius();

        // At the radius-aware version, radius is written and read back
        // exactly.
        let (_idx_bytes, offsets_bytes) =
            encode_vector_index_v2(2, &postings, RADIUS_AWARE_VECTOR_INDEX_VERSION);
        let parsed = parse_vector_offsets(&offsets_bytes, 2, RADIUS_AWARE_VECTOR_INDEX_VERSION)
            .expect("valid sidecar");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].radius, 0.123);
        assert_eq!(parsed[1].radius, 0.456);

        // At an older version, radius is never written to disk at all (not
        // just ignored) — a pre-radius-aware sidecar is genuinely smaller,
        // and a reader sees 0.0, the value that degrades exactly to
        // pre-existing raw-centroid-distance behavior.
        let (_idx_bytes_v2, offsets_bytes_v2) =
            encode_vector_index_v2(2, &postings, UNIT_NORM_VECTOR_INDEX_VERSION);
        assert!(
            offsets_bytes_v2.len() < offsets_bytes.len(),
            "a v2 sidecar must be smaller than a v3 one for the same postings \
             — the radius field must not be written"
        );
        let parsed_v2 = parse_vector_offsets(&offsets_bytes_v2, 2, UNIT_NORM_VECTOR_INDEX_VERSION)
            .expect("valid sidecar");
        assert_eq!(parsed_v2[0].radius, 0.0);
        assert_eq!(parsed_v2[1].radius, 0.0);
    }

    #[test]
    fn eager_fallback_recovers_flat_vector_list_without_sidecar() {
        let postings = sample_postings();
        let (idx_bytes, _offsets_bytes) =
            encode_vector_index_v2(2, &postings, VECTOR_INDEX_VERSION);
        let store = parse_vector_idx_eager(&idx_bytes).expect("valid v2 blob");
        assert_eq!(store.dimensions, 2);
        let mut ids: Vec<u32> = store.vectors.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    #[test]
    fn legacy_header_has_no_magic_and_is_rejected() {
        // A legacy v1 file starts with `dim` directly — using a real
        // dimension value here proves it can never collide with the v2
        // magic.
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&128u32.to_le_bytes());
        legacy.extend_from_slice(&0u32.to_le_bytes());
        assert!(parse_vector_idx_header(&legacy).is_none());
    }

    #[test]
    fn truncated_vector_idx_is_rejected_not_panicked_on() {
        let postings = sample_postings();
        let (mut idx_bytes, _offsets_bytes) =
            encode_vector_index_v2(2, &postings, VECTOR_INDEX_VERSION);
        idx_bytes.truncate(idx_bytes.len() - 3);
        assert!(parse_vector_idx_eager(&idx_bytes).is_none());
    }

    #[test]
    fn truncated_offsets_sidecar_is_rejected_not_panicked_on() {
        let postings = sample_postings();
        let (_idx_bytes, mut offsets_bytes) =
            encode_vector_index_v2(2, &postings, VECTOR_INDEX_VERSION);
        offsets_bytes.truncate(offsets_bytes.len() - 3);
        assert!(parse_vector_offsets(&offsets_bytes, 2, VECTOR_INDEX_VERSION).is_none());
    }

    #[test]
    fn dim_mismatch_between_files_is_rejected() {
        let postings = sample_postings();
        let (_idx_bytes, offsets_bytes) =
            encode_vector_index_v2(2, &postings, VECTOR_INDEX_VERSION);
        // vector.idx claims dim=3, but the sidecar was built for dim=2.
        assert!(parse_vector_offsets(&offsets_bytes, 3, VECTOR_INDEX_VERSION).is_none());
    }

    #[test]
    fn version_mismatch_between_files_is_rejected() {
        let postings = sample_postings();
        // Sidecar written raw (v1) but vector.idx claims unit-norm (v2):
        // trusting the pair would score raw-magnitude vectors with the
        // dot-only path — reject, falling back to the eager parse.
        let (_idx_bytes, offsets_bytes) =
            encode_vector_index_v2(2, &postings, VECTOR_INDEX_VERSION);
        assert!(parse_vector_offsets(&offsets_bytes, 2, UNIT_NORM_VECTOR_INDEX_VERSION).is_none());
    }

    #[test]
    fn write_vectors_v2_then_parse_round_trips_through_a_real_backend() {
        let dir = std::env::temp_dir().join(format!("kosha-v2-vec-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let mut writer = SegmentWriter::new(SegmentId("s1".into()), dir.clone());
        writer.add_document(
            DocumentId("d0".into()),
            vec![Field {
                name: "v".into(),
                field_type: FieldType::Vector,
                value: "[1.0,0.0]".into(),
            }],
        );
        writer.add_document(
            DocumentId("d1".into()),
            vec![Field {
                name: "v".into(),
                field_type: FieldType::Vector,
                value: "[0.0,1.0]".into(),
            }],
        );
        writer.write_vectors_v2().unwrap();

        let data = fs::read(dir.join("vector.idx")).unwrap();
        let (dim, total_count, version) = parse_vector_idx_header(&data).expect("valid v2 header");
        assert_eq!(dim, 2);
        assert_eq!(total_count, 2);
        // The real writer defaults to the radius-aware version (write gate 4).
        assert_eq!(version, RADIUS_AWARE_VECTOR_INDEX_VERSION);

        let offsets_data = fs::read(dir.join("vector.offsets")).unwrap();
        let postings = parse_vector_offsets(&offsets_data, dim, version).expect("valid sidecar");
        // Two docs, opposite corners — one posting (well under
        // min_posting_size), radius must be the real distance from the
        // (unit-norm) centroid to whichever of the two is farther, not 0.0.
        assert!(
            postings.iter().any(|p| p.radius > 0.0),
            "default writer must persist a real (non-placeholder) radius"
        );
        let mut ids: Vec<u32> = postings
            .iter()
            .flat_map(|p| {
                decode_posting_entries(
                    &data[p.offset as usize..(p.offset + p.length as u64) as usize],
                    dim,
                    version,
                )
                .dequantized()
            })
            .map(|(id, _)| id)
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 1]);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn unit_norm_writes_store_unit_vectors_and_score_like_cosine() {
        let dir = std::env::temp_dir().join(format!("kosha-v2-unitnorm-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        // Deliberately non-unit magnitudes: cosine must ignore them, and the
        // stored representation must not preserve them.
        let originals: Vec<(&str, Vec<f32>)> = vec![
            ("d0", vec![3.0, 0.0, 0.0]),
            ("d1", vec![0.0, 0.5, 0.0]),
            ("d2", vec![4.0, 4.0, 0.0]),
            ("d3", vec![0.0, 0.0, 10.0]),
        ];
        let mut writer = SegmentWriter::new(SegmentId("s1".into()), dir.clone());
        for (id, v) in &originals {
            writer.add_document(
                DocumentId(id.to_string()),
                vec![Field::vector("embedding", v.clone())],
            );
        }
        writer.backend.create_dir_all("").unwrap();
        writer.write_doc_store().unwrap();
        writer.write_inverted_index().unwrap();
        writer.write_filters().unwrap();
        let vector_fields = writer.write_vectors_v2().unwrap();
        writer
            .write_footer(Bm25Params::default(), vector_fields, Vec::new())
            .unwrap();

        // On disk: header says unit-norm, and every stored vector and
        // centroid actually is unit-L2.
        let data = fs::read(dir.join("vector.idx")).unwrap();
        let (dim, _, version) = parse_vector_idx_header(&data).expect("valid header");
        assert_eq!(version, RADIUS_AWARE_VECTOR_INDEX_VERSION);
        let store = parse_vector_idx_eager(&data).expect("valid blob");
        for (_, v) in &store.vectors {
            let norm = kosha_vector_spfresh::dot(v, v).sqrt();
            assert!((norm - 1.0).abs() < 1e-5, "stored vector not unit: {norm}");
        }
        let offsets_data = fs::read(dir.join("vector.offsets")).unwrap();
        for p in parse_vector_offsets(&offsets_data, dim, version).expect("valid sidecar") {
            let norm = kosha_vector_spfresh::dot(&p.centroid, &p.centroid).sqrt();
            assert!((norm - 1.0).abs() < 1e-5, "centroid not unit: {norm}");
        }

        // Dot-only scoring must reproduce the cosine similarities of the
        // *original* raw-magnitude vectors (up to float rounding).
        let reader = SegmentReader::open(dir.clone()).unwrap();
        assert!(reader.has_lazy_vector_index());
        let query = [2.0, 1.0, 0.0];
        let results = reader
            .knn_search(&query, originals.len(), 16)
            .expect("lazy index")
            .unwrap();
        assert_eq!(results.len(), originals.len());
        for (doc_seq, sim) in results {
            let expected =
                kosha_vector_spfresh::cosine_similarity(&query, &originals[doc_seq as usize].1);
            assert!(
                (sim - expected as f64).abs() < 1e-5,
                "doc {doc_seq}: dot-path {sim} vs cosine {expected}"
            );
        }

        let _ = fs::remove_dir_all(&dir);
    }
}

// ─── Inverted index format (v2: lazy, zero-parse-at-open) ───────────────────

/// Magic prefix of a v2 `inverted.idx` ("KINV" bytes, little-endian u32).
/// A legacy (v1) file starts with its term count instead — colliding with
/// this value would require ~1.26 billion distinct terms in one segment, so
/// the magic doubles as the format discriminator for the read-side fallback.
pub const INVERTED_MAGIC: u32 = u32::from_le_bytes(*b"KINV");
/// Inline-lazy version: table + term pool + postings all in `inverted.idx`.
const INLINE_INVERTED_VERSION: u32 = 2;
/// Fixed-width split version: table + term pool in `inverted.idx`, postings in
/// `postings-*.bin` shard files.
const FIXED_SPLIT_INVERTED_VERSION: u32 = 3;
/// Varint/delta split version: same table as v3, with posting blobs encoded
/// as varints and delta-coded doc ids/positions.
pub const SPLIT_INVERTED_VERSION: u32 = 4;
/// Skip-pointer split version: v4's TOC + shard layout, but each term's
/// blob span is `[n_postings][skip table][scoring bytes][positions bytes]`.
/// Scoring (BM25/WAND) decodes only `(doc_id, tf)` pairs, block by block;
/// the per-128-posting skip entries (`last_doc_id`, byte length, `max_tf`,
/// `min_field_length`) let block-max WAND read upper bounds straight from
/// the index and skip whole blocks without ever varint-decoding them.
/// Positions are decoded only by the full [`SegmentReader::postings`] path
/// (phrase queries, legacy consumers).
pub const SKIP_SPLIT_INVERTED_VERSION: u32 = 5;
/// Experimental bit-packed skip-split version (#386): byte-for-byte the v5
/// span layout and skip table, but every *full* 128-posting scoring block
/// is frame-of-reference bit-packed (SIMD-BP128 layout, see
/// [`bitpack`]) instead of varint pairs. A term's partial last block and
/// the positions section stay v5 varints. Written only when
/// `KOSHA_POSTINGS_WRITE_VERSION=6`; never the default.
pub const BITPACKED_SKIP_SPLIT_INVERTED_VERSION: u32 = 6;

/// THE canonical "is this a split-format inverted TOC?" predicate. Every
/// site that dispatches on split-ness (the server's posting-blob hydration
/// gate, `SplitInvertedIndex::detect`, format probes) must call this
/// instead of enumerating versions locally: a version list that drifts
/// between the hydration gate and the reader is exactly the PR #83
/// silent-zero-hits bug — the reader accepts the segment while hydration
/// never fetches its blobs. Adding v6 must be a one-line change here.
pub fn is_split_inverted_version(version: u32) -> bool {
    version == FIXED_SPLIT_INVERTED_VERSION
        || version == SPLIT_INVERTED_VERSION
        || version == SKIP_SPLIT_INVERTED_VERSION
        || version == BITPACKED_SKIP_SPLIT_INVERTED_VERSION
}

/// The inverted-postings version the writer emits —
/// `KOSHA_POSTINGS_WRITE_VERSION`, accepted values `4` (varint-delta
/// split), `5` (skip-split) and `6` (experimental bit-packed skip-split,
/// #386 — opt-in only, readable only by builds that include #386); default
/// `5`. This is the mixed-fleet
/// rollout/rollback gate: a reader only understands versions up to its own
/// build, so during a rolling deploy (writer upgraded before all query
/// replicas) or ahead of a possible rollback, set `4` until the whole
/// fleet reads v5 — segments written as v5 are permanently unreadable by
/// pre-v5 binaries. Read once (segments are finalized frequently).
fn postings_write_version() -> u32 {
    static VERSION: OnceLock<u32> = OnceLock::new();
    *VERSION.get_or_init(|| {
        match std::env::var("KOSHA_POSTINGS_WRITE_VERSION")
            .ok()
            .as_deref()
        {
            Some("4") => SPLIT_INVERTED_VERSION,
            Some("5") | None => SKIP_SPLIT_INVERTED_VERSION,
            Some("6") => BITPACKED_SKIP_SPLIT_INVERTED_VERSION,
            Some(other) => {
                eprintln!(
                    "WARN: KOSHA_POSTINGS_WRITE_VERSION={other} not supported \
                     (accepted: 4, 5, 6) — writing v{SKIP_SPLIT_INVERTED_VERSION}"
                );
                SKIP_SPLIT_INVERTED_VERSION
            }
        }
    })
}

/// Default weight of a posting's radius in its probe-ranking score: the
/// full triangle-inequality lower bound `d(q, c) - radius`.
pub const DEFAULT_KNN_PROBE_RADIUS_ALPHA: f32 = 1.0;

/// The score a kNN probe ranks and admits postings by (#384):
/// `(distance - alpha * radius).max(0.0)`, where `distance` is the query's
/// cosine distance to the posting's centroid and `radius` the posting's
/// max member distance from it.
///
/// - `alpha = 1.0` (default): the triangle-inequality lower bound on the
///   distance to any member, bit-identical to the ranking before #384.
/// - `alpha = 0.0`: raw centroid distance, as SPANN and plain IVF rank.
/// - in between: partial credit for spread. At 1024 dimensions a
///   posting's radius is close to typical centroid distances, so the full
///   bound is loose and ranks many diffuse postings near zero; a smaller
///   alpha is the experiment.
///
/// The global cutoff (kosha-query `knn_plan`) and each segment's own
/// ranking both go through this function, so the two sides always compare
/// the same units.
pub fn centroid_probe_score(distance: f32, radius: f32, alpha: f32) -> f32 {
    (distance - alpha * radius).max(0.0)
}

/// `KOSHA_KNN_PROBE_RADIUS_ALPHA` (#384, experimental; default 1.0, the
/// pre-#384 ranking): the radius weight in [`centroid_probe_score`].
/// Accepts a finite value `>= 0`; anything else keeps the default with a
/// warning. Read once per process.
pub fn knn_probe_radius_alpha() -> f32 {
    static ALPHA: OnceLock<f32> = OnceLock::new();
    *ALPHA.get_or_init(|| {
        let raw = std::env::var("KOSHA_KNN_PROBE_RADIUS_ALPHA").ok();
        let alpha = parse_probe_radius_alpha(raw.as_deref());
        if let Some(raw) = raw.as_deref() {
            eprintln!(
                "INFO: KOSHA_KNN_PROBE_RADIUS_ALPHA={raw}: kNN postings ranked by \
                 max(0, d(q,c) - {alpha} * radius) (#384, experimental)"
            );
        }
        alpha
    })
}

fn parse_probe_radius_alpha(raw: Option<&str>) -> f32 {
    match raw.map(str::trim) {
        None | Some("") => DEFAULT_KNN_PROBE_RADIUS_ALPHA,
        Some(v) => match v.parse::<f32>() {
            Ok(a) if a.is_finite() && a >= 0.0 => a,
            _ => {
                eprintln!(
                    "WARN: KOSHA_KNN_PROBE_RADIUS_ALPHA={v} is not a finite number >= 0; \
                     using {DEFAULT_KNN_PROBE_RADIUS_ALPHA}"
                );
                DEFAULT_KNN_PROBE_RADIUS_ALPHA
            }
        },
    }
}

/// Whether `SegmentWriter` leaves `postings-*.bin` shards raw —
/// `KOSHA_POSTINGS_RAW_BLOBS`, `1`/`true` (default) or `0`/`false`.
///
/// Raw shards let a cold query fetch only its terms' posting spans with
/// ranged GETs (#245) instead of whole shards, at the cost of skipping the
/// whole-file zstd wrapper (`maybe_compress_inverted_file`). The postings
/// are already varint/delta coded, so that wrapper saved little. No reader
/// change is needed: the wrapper was always optional and every reader
/// passes raw bytes through, so a rollback still reads raw segments. Set
/// `0` to keep compressing shards; such segments are fetched whole as
/// before. Compaction writes through `SegmentWriter`, so its output
/// follows the same setting. Read once.
fn posting_blobs_raw() -> bool {
    static RAW: OnceLock<bool> = OnceLock::new();
    *RAW.get_or_init(|| {
        match std::env::var("KOSHA_POSTINGS_RAW_BLOBS")
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("") | Some("1") | Some("true") => true,
            Some("0") | Some("false") => false,
            Some(other) => {
                eprintln!(
                    "WARN: KOSHA_POSTINGS_RAW_BLOBS={other} not 0|1 — writing raw posting shards"
                );
                true
            }
        }
    })
}

/// Default for [`text_filter_ignore_above`]: Elasticsearch's dynamic-mapping
/// `keyword` subfield cap, so data backfilled from ES keeps exactly the
/// exact-match filterability it had there.
pub const DEFAULT_TEXT_FILTER_IGNORE_ABOVE: u32 = 256;

/// Max length, in chars, of a `Text` value `SegmentWriter` copies into
/// `filters.bin` / `filter_blooms` — `KOSHA_TEXT_FILTER_IGNORE_ABOVE`, a
/// positive integer, or `off` to copy every value (pre-#243 behavior).
/// Default [`DEFAULT_TEXT_FILTER_IGNORE_ABOVE`]. Longer values remain fully
/// searchable via BM25 and are still returned in hits; they just cannot be
/// matched by `term`/`terms`/`range` filters, sorted on, or aggregated.
/// Keyword/Boolean/Date values are never capped. Read once.
fn text_filter_ignore_above() -> Option<u32> {
    static CAP: OnceLock<Option<u32>> = OnceLock::new();
    *CAP.get_or_init(|| {
        parse_text_filter_ignore_above(
            std::env::var("KOSHA_TEXT_FILTER_IGNORE_ABOVE")
                .ok()
                .as_deref(),
        )
    })
}

fn parse_text_filter_ignore_above(raw: Option<&str>) -> Option<u32> {
    match raw.map(str::trim) {
        None | Some("") => Some(DEFAULT_TEXT_FILTER_IGNORE_ABOVE),
        Some(v) if v.eq_ignore_ascii_case("off") => None,
        Some(v) => match v.parse::<u32>() {
            Ok(n) if n > 0 => Some(n),
            _ => {
                eprintln!(
                    "WARN: KOSHA_TEXT_FILTER_IGNORE_ABOVE={v} not supported \
                     (accepted: positive integer, off) — using \
                     {DEFAULT_TEXT_FILTER_IGNORE_ABOVE}"
                );
                Some(DEFAULT_TEXT_FILTER_IGNORE_ABOVE)
            }
        },
    }
}

/// Whether a `Text` value is short enough to copy into the filter columns.
/// Counted in chars (not bytes) to match ES `ignore_above`; the byte-length
/// check short-circuits the common case without walking the string.
fn text_value_within_cap(value: &str, cap: Option<u32>) -> bool {
    let Some(cap) = cap else { return true };
    let cap = cap as usize;
    value.len() <= cap || value.chars().count() <= cap
}

// ─── Bounded-memory segment build (#242) ────────────────────────────────────
//
// A writer used to hold everything until `finalize`: every document's full
// field content, the whole positional inverted index, and every posting
// blob. Compaction feeds every live document of its inputs through one
// writer, so a merge's peak memory grew with the merged segment (the
// 22 GiB staging namespace could not be merged on the ingest pod).
//
// Now:
// - Stored records stream to `.build/doc_store.bin` as they are added;
//   per document the writer keeps only its field length and offsets entry.
// - Once the in-memory postings pass `KOSHA_SEGMENT_BUILD_MEMORY_BYTES`,
//   they are written to a sorted run file and freed. `finalize` k-way
//   merges the runs and what's left in memory by term. A term's postings
//   stream from the runs, posting by posting, into a `TermEncoder` that
//   writes the encoded span to its shard file; the encoder's sections
//   spill to scratch files past `term_spill_limit`, so even a term in
//   every document is never resident in full.
// - Each posting shard is compressed file-to-file.
//
// Output bytes are unchanged; `.build/` is removed before `finalize`
// returns, or by `Drop` if the build is abandoned.

/// Scratch directory inside a segment's output directory.
const BUILD_DIR: &str = ".build";
/// Approximate heap cost of a new term in the in-memory index (hash-map
/// slot, `String` and `Vec` headers, allocator slack), on top of its bytes.
const TERM_ENTRY_OVERHEAD: usize = 80;
/// Approximate heap cost of one `Posting` (struct plus its positions `Vec`
/// allocation), on top of 4 bytes per position.
const POSTING_OVERHEAD: usize = 48;
/// Default for `KOSHA_SEGMENT_BUILD_MEMORY_BYTES`.
pub const DEFAULT_SEGMENT_BUILD_MEMORY_BYTES: usize = 256 * 1024 * 1024;

/// In-memory postings budget per segment writer before a sorted run is
/// spilled to disk (`KOSHA_SEGMENT_BUILD_MEMORY_BYTES`, default 256 MiB).
/// Bounds the positional index part of a flush or merge; see the section
/// comment above for what else a writer holds.
pub fn segment_build_memory_bytes() -> usize {
    static BYTES: OnceLock<usize> = OnceLock::new();
    *BYTES.get_or_init(|| {
        parse_segment_build_memory_bytes(
            std::env::var("KOSHA_SEGMENT_BUILD_MEMORY_BYTES")
                .ok()
                .as_deref(),
        )
    })
}

fn parse_segment_build_memory_bytes(raw: Option<&str>) -> usize {
    match raw.map(str::trim) {
        None | Some("") => DEFAULT_SEGMENT_BUILD_MEMORY_BYTES,
        Some(v) => match v.parse::<usize>() {
            Ok(n) if n > 0 => n,
            _ => {
                eprintln!(
                    "WARN: KOSHA_SEGMENT_BUILD_MEMORY_BYTES={v} must be a positive byte count — \
                     using {DEFAULT_SEGMENT_BUILD_MEMORY_BYTES}"
                );
                DEFAULT_SEGMENT_BUILD_MEMORY_BYTES
            }
        },
    }
}

/// `doc_store.bin` being written record by record, plus its offsets table.
struct DocStoreSink {
    path: PathBuf,
    out: std::io::BufWriter<fs::File>,
    written: u64,
    offsets: Vec<u8>,
    scratch: Vec<u8>,
}

impl DocStoreSink {
    fn create(path: PathBuf) -> Result<Self, KoshaError> {
        let mut out = std::io::BufWriter::new(fs::File::create(&path)?);
        // Doc count placeholder, patched by `finish`.
        std::io::Write::write_all(&mut out, &0u32.to_le_bytes())?;
        Ok(Self {
            path,
            out,
            written: 4,
            offsets: 0u32.to_le_bytes().to_vec(),
            scratch: Vec::new(),
        })
    }

    /// Same record encoding `doc_store.bin` has always used.
    fn append(&mut self, rec: &DocRecord) -> Result<(), KoshaError> {
        let buf = &mut self.scratch;
        buf.clear();
        let id_bytes = rec.doc_id.0.as_bytes();
        buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(id_bytes);
        buf.extend_from_slice(&rec.field_length.to_le_bytes());
        buf.extend_from_slice(&(rec.fields.len() as u32).to_le_bytes());
        for field in &rec.fields {
            let name_bytes = field.name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            buf.push(field.field_type as u8);
            let val_bytes = field.value.as_bytes();
            buf.extend_from_slice(&(val_bytes.len() as u64).to_le_bytes());
            buf.extend_from_slice(val_bytes);
        }
        std::io::Write::write_all(&mut self.out, buf)?;
        self.offsets
            .extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
        self.offsets.extend_from_slice(id_bytes);
        self.offsets
            .extend_from_slice(&rec.field_length.to_le_bytes());
        self.offsets.extend_from_slice(&self.written.to_le_bytes());
        self.offsets
            .extend_from_slice(&(buf.len() as u32).to_le_bytes());
        self.written += buf.len() as u64;
        Ok(())
    }

    /// Patch the doc count into both files; returns the finished file's
    /// path and the offsets bytes.
    fn finish(mut self, doc_count: u32) -> Result<(PathBuf, Vec<u8>), KoshaError> {
        let mut file = self
            .out
            .into_inner()
            .map_err(|e| KoshaError::Io(e.into_error()))?;
        file.seek(SeekFrom::Start(0))?;
        std::io::Write::write_all(&mut file, &doc_count.to_le_bytes())?;
        self.offsets[0..4].copy_from_slice(&doc_count.to_le_bytes());
        Ok((self.path, self.offsets))
    }
}

/// Run entry: `[term_len:u32][term][count:u32]` then per posting
/// `[doc_id:u32][tf:u32][n_positions:u32][positions:u32 x n]`.
fn write_run_entry(
    out: &mut impl std::io::Write,
    term: &str,
    postings: &[Posting],
) -> Result<(), KoshaError> {
    out.write_all(&(term.len() as u32).to_le_bytes())?;
    out.write_all(term.as_bytes())?;
    out.write_all(&(postings.len() as u32).to_le_bytes())?;
    for p in postings {
        out.write_all(&p.doc_id.to_le_bytes())?;
        out.write_all(&p.term_frequency.to_le_bytes())?;
        out.write_all(&(p.positions.len() as u32).to_le_bytes())?;
        for pos in &p.positions {
            out.write_all(&pos.to_le_bytes())?;
        }
    }
    Ok(())
}

/// Reads a run file entry by entry. A term's postings are read one posting
/// at a time ([`RunReader::read_posting`]) straight into the encoder, so a
/// stopword's run slice is never materialized as a `Vec<Posting>`.
struct RunReader {
    input: std::io::BufReader<fs::File>,
}

impl RunReader {
    fn open(path: &Path) -> Result<Self, KoshaError> {
        Ok(Self {
            input: std::io::BufReader::new(fs::File::open(path)?),
        })
    }

    fn read_u32(&mut self) -> Result<u32, KoshaError> {
        let mut b = [0u8; 4];
        self.input.read_exact(&mut b)?;
        Ok(u32::from_le_bytes(b))
    }

    /// The next entry's term and posting count, or `None` at end of run.
    /// The caller must read exactly `count` postings before asking again.
    fn next_header(&mut self) -> Result<Option<(String, u32)>, KoshaError> {
        let mut b = [0u8; 4];
        match self.input.read_exact(&mut b) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let mut term = vec![0u8; u32::from_le_bytes(b) as usize];
        self.input.read_exact(&mut term)?;
        let term = String::from_utf8(term)
            .map_err(|_| KoshaError::CorruptSegment("posting run: non-UTF-8 term".into()))?;
        let count = self.read_u32()?;
        Ok(Some((term, count)))
    }

    /// One posting of the current entry: `(doc_id, tf)`, with its
    /// positions written into `positions` (cleared first).
    fn read_posting(&mut self, positions: &mut Vec<u32>) -> Result<(u32, u32), KoshaError> {
        let doc_id = self.read_u32()?;
        let term_frequency = self.read_u32()?;
        let n = self.read_u32()? as usize;
        positions.clear();
        for _ in 0..n {
            positions.push(self.read_u32()?);
        }
        Ok((doc_id, term_frequency))
    }
}

enum PostingSource {
    Run(RunReader),
    /// What was still in memory at `finalize` (at most the build budget).
    /// `current` holds the postings of the entry last returned by
    /// `next_header` until they are drained.
    Memory {
        entries: std::vec::IntoIter<(String, Vec<Posting>)>,
        current: Vec<Posting>,
    },
}

impl PostingSource {
    fn memory(entries: Vec<(String, Vec<Posting>)>) -> Self {
        Self::Memory {
            entries: entries.into_iter(),
            current: Vec::new(),
        }
    }

    fn next_header(&mut self) -> Result<Option<(String, u32)>, KoshaError> {
        match self {
            Self::Run(run) => run.next_header(),
            Self::Memory { entries, current } => Ok(entries.next().map(|(term, postings)| {
                let count = postings.len() as u32;
                *current = postings;
                (term, count)
            })),
        }
    }

    /// Feed the current entry's `count` postings to `encoder`.
    fn drain_into(
        &mut self,
        count: u32,
        encoder: &mut TermEncoder,
        field_lengths: &[u32],
        positions: &mut Vec<u32>,
    ) -> Result<(), KoshaError> {
        match self {
            Self::Run(run) => {
                for _ in 0..count {
                    let (doc_id, tf) = run.read_posting(positions)?;
                    encoder.push(doc_id, tf, positions, field_lengths)?;
                }
            }
            Self::Memory { current, .. } => {
                for p in std::mem::take(current) {
                    encoder.push(p.doc_id, p.term_frequency, &p.positions, field_lengths)?;
                }
            }
        }
        Ok(())
    }
}

/// K-way merge of term-sorted sources. Sources are given in doc_seq order
/// (each covers a later doc range than the one before), so one term's
/// postings are the concatenation of its sources' postings in source order.
///
/// Only each source's head *term* and posting count are resident; the
/// postings themselves stream from the sources into a [`TermEncoder`]
/// (#242), so a term in every document costs no more memory than a rare
/// one.
struct TermMerger {
    sources: Vec<PostingSource>,
    /// Posting count of each source's head entry.
    head_counts: Vec<u32>,
    heap: std::collections::BinaryHeap<std::cmp::Reverse<(String, usize)>>,
    positions: Vec<u32>,
}

impl TermMerger {
    fn new(mut sources: Vec<PostingSource>) -> Result<Self, KoshaError> {
        let mut head_counts = vec![0; sources.len()];
        let mut heap = std::collections::BinaryHeap::new();
        for (i, source) in sources.iter_mut().enumerate() {
            if let Some((term, count)) = source.next_header()? {
                heap.push(std::cmp::Reverse((term, i)));
                head_counts[i] = count;
            }
        }
        Ok(Self {
            sources,
            head_counts,
            heap,
            positions: Vec::new(),
        })
    }

    /// Stream source `i`'s head postings into `encoder`, then move it on.
    fn drain_and_advance(
        &mut self,
        i: usize,
        encoder: &mut TermEncoder,
        field_lengths: &[u32],
    ) -> Result<(), KoshaError> {
        self.sources[i].drain_into(
            self.head_counts[i],
            encoder,
            field_lengths,
            &mut self.positions,
        )?;
        if let Some((term, count)) = self.sources[i].next_header()? {
            self.heap.push(std::cmp::Reverse((term, i)));
            self.head_counts[i] = count;
        }
        Ok(())
    }

    /// Encode the next term's postings, from every source that has it, into
    /// `encoder`. Returns the term, or `None` when every source is done.
    fn next_term(
        &mut self,
        encoder: &mut TermEncoder,
        field_lengths: &[u32],
    ) -> Result<Option<String>, KoshaError> {
        let Some(std::cmp::Reverse((term, i))) = self.heap.pop() else {
            return Ok(None);
        };
        // Per-field postings (#249) carry no per-field length in their
        // block summaries: `min_field_length` 0 is a valid, if loose,
        // ceiling. Only the combined index is scored on the block-max WAND
        // paths. Same rule as the in-memory encoder.
        let field_lengths = if kosha_core::fields::is_field_key(&term) {
            &[][..]
        } else {
            field_lengths
        };
        self.drain_and_advance(i, encoder, field_lengths)?;
        // Equal terms pop in ascending source index: doc_seq order. A
        // source's next term is strictly greater (runs hold each term
        // once), so advancing never re-queues `term`.
        while self
            .heap
            .peek()
            .is_some_and(|std::cmp::Reverse((t, _))| *t == term)
        {
            let std::cmp::Reverse((_, j)) = self.heap.pop().unwrap();
            self.drain_and_advance(j, encoder, field_lengths)?;
        }
        Ok(Some(term))
    }
}

/// An append-only byte buffer that moves to a scratch file once it holds
/// `limit` bytes, so one term's encoded section never needs to be resident
/// in full. Reused across terms.
struct SpillBuf {
    mem: Vec<u8>,
    path: PathBuf,
    file: Option<fs::File>,
    /// Bytes already written to `file` for the current term.
    spilled: u64,
    limit: usize,
    /// Total bytes ever moved to `file` (tests and diagnostics).
    spilled_total: u64,
}

impl SpillBuf {
    fn new(path: PathBuf, limit: usize) -> Self {
        Self {
            mem: Vec::new(),
            path,
            file: None,
            spilled: 0,
            limit: limit.max(1),
            spilled_total: 0,
        }
    }

    fn len(&self) -> u64 {
        self.spilled + self.mem.len() as u64
    }

    fn maybe_spill(&mut self) -> Result<(), KoshaError> {
        if self.mem.len() < self.limit {
            return Ok(());
        }
        let file = match &mut self.file {
            Some(file) => file,
            slot => slot.insert(
                fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&self.path)?,
            ),
        };
        std::io::Write::write_all(file, &self.mem)?;
        self.spilled += self.mem.len() as u64;
        self.spilled_total += self.mem.len() as u64;
        self.mem.clear();
        Ok(())
    }

    /// Append everything buffered for this term to `out`, in order, and
    /// reset for the next term.
    fn drain_to(&mut self, out: &mut impl std::io::Write) -> Result<(), KoshaError> {
        if self.spilled > 0 {
            let file = self.file.as_mut().expect("spilled bytes have a file");
            file.seek(SeekFrom::Start(0))?;
            let copied = std::io::copy(&mut Read::by_ref(file).take(self.spilled), out)?;
            if copied != self.spilled {
                return Err(KoshaError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "term spill file shorter than written",
                )));
            }
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            self.spilled = 0;
        }
        out.write_all(&self.mem)?;
        self.mem.clear();
        Ok(())
    }
}

/// Encodes one term's postings incrementally, as they arrive in doc_seq
/// order, into exactly the bytes [`write_postings_skip_split`] (v5) or
/// [`write_postings_varint_delta`] (v4) would produce from the whole list
/// (#242). Memory per term: the v5 skip table (a few bytes per 128
/// postings) plus two [`SpillBuf`]s capped at their limit; everything past
/// that goes to scratch files and is copied to the shard on `finish`.
///
/// Both layouts put the posting count (and for v5 the skip table) before
/// the posting bytes, which is why the bytes are buffered rather than
/// written straight to the shard.
struct TermEncoder {
    version: u32,
    count: u32,
    /// v5 skip-table entries so far.
    skip: Vec<u8>,
    /// v5: scoring section. v4: the whole body.
    scoring: SpillBuf,
    /// v5 positions section (unused by v4).
    positions: SpillBuf,
    prev_doc: u32,
    // Current v5 block.
    in_block: usize,
    block_start: u64,
    block_max_tf: u32,
    block_min_fl: u32,
    block_last: u32,
    prev_block_last: u32,
    /// v6: the current block's doc ids / tfs, packed when it fills (a
    /// partial last block is written as v5 varints on `finish`).
    bp_docs: Box<[u32; SCORING_BLOCK_LEN]>,
    bp_tfs: Box<[u32; SCORING_BLOCK_LEN]>,
}

impl TermEncoder {
    fn new(version: u32, scratch_dir: &Path, spill_limit: usize) -> Self {
        Self {
            version,
            count: 0,
            skip: Vec::new(),
            scoring: SpillBuf::new(scratch_dir.join("term-scoring"), spill_limit),
            positions: SpillBuf::new(scratch_dir.join("term-positions"), spill_limit),
            prev_doc: 0,
            in_block: 0,
            block_start: 0,
            block_max_tf: 0,
            block_min_fl: u32::MAX,
            block_last: 0,
            prev_block_last: 0,
            bp_docs: Box::new([0; SCORING_BLOCK_LEN]),
            bp_tfs: Box::new([0; SCORING_BLOCK_LEN]),
        }
    }

    /// v6: emit the buffered block's scoring bytes — bit-packed when full,
    /// v5 varint pairs for a term's partial last block — then its skip
    /// entry. Bytes match [`write_postings_skip_split_v6`].
    fn flush_bp_block(&mut self) {
        let n = self.in_block;
        self.block_start = self.scoring.len();
        // `skip` is empty exactly while the term's first block is open.
        let first = self.skip.is_empty();
        if n == SCORING_BLOCK_LEN {
            let next_min = if first {
                0
            } else {
                self.prev_block_last.wrapping_add(1)
            };
            bitpack::encode_bp_block(&self.bp_docs, &self.bp_tfs, next_min, &mut self.scoring.mem);
        } else {
            let mut prev = if first { 0 } else { self.prev_block_last };
            for i in 0..n {
                put_var_u32(&mut self.scoring.mem, self.bp_docs[i].wrapping_sub(prev));
                prev = self.bp_docs[i];
                put_var_u32(&mut self.scoring.mem, self.bp_tfs[i]);
            }
        }
        self.close_block();
    }

    fn push(
        &mut self,
        doc_id: u32,
        term_frequency: u32,
        positions: &[u32],
        field_lengths: &[u32],
    ) -> Result<(), KoshaError> {
        self.count += 1;
        if self.version == SPLIT_INVERTED_VERSION {
            // v4: [doc delta][tf][n_positions][position deltas] per posting.
            let out = &mut self.scoring.mem;
            put_var_u32(out, doc_id.wrapping_sub(self.prev_doc));
            self.prev_doc = doc_id;
            put_var_u32(out, term_frequency);
            put_var_u32(out, positions.len() as u32);
            let mut prev_pos = 0u32;
            for &pos in positions {
                put_var_u32(out, pos.wrapping_sub(prev_pos));
                prev_pos = pos;
            }
            return self.scoring.maybe_spill();
        }

        let bitpacked = self.version == BITPACKED_SKIP_SPLIT_INVERTED_VERSION;
        if self.in_block == 0 {
            self.block_start = self.scoring.len();
            self.block_max_tf = 0;
            self.block_min_fl = u32::MAX;
        }
        if bitpacked {
            self.bp_docs[self.in_block] = doc_id;
            self.bp_tfs[self.in_block] = term_frequency;
        } else {
            put_var_u32(&mut self.scoring.mem, doc_id.wrapping_sub(self.prev_doc));
            put_var_u32(&mut self.scoring.mem, term_frequency);
        }
        self.prev_doc = doc_id;
        self.block_max_tf = self.block_max_tf.max(term_frequency);
        if let Some(&field_length) = field_lengths.get(doc_id as usize) {
            self.block_min_fl = self.block_min_fl.min(field_length);
        }
        self.block_last = doc_id;
        self.in_block += 1;
        if self.in_block == SCORING_BLOCK_LEN {
            if bitpacked {
                self.flush_bp_block();
            } else {
                self.close_block();
            }
        }

        let out = &mut self.positions.mem;
        put_var_u32(out, positions.len() as u32);
        let mut prev_pos = 0u32;
        for &pos in positions {
            put_var_u32(out, pos.wrapping_sub(prev_pos));
            prev_pos = pos;
        }
        self.scoring.maybe_spill()?;
        self.positions.maybe_spill()
    }

    /// Append the current block's skip entry (same fields and order as
    /// `write_postings_skip_split`).
    fn close_block(&mut self) {
        put_var_u32(
            &mut self.skip,
            self.block_last.wrapping_sub(self.prev_block_last),
        );
        self.prev_block_last = self.block_last;
        put_var_u32(
            &mut self.skip,
            (self.scoring.len() - self.block_start) as u32,
        );
        put_var_u32(&mut self.skip, self.block_max_tf);
        put_var_u32(
            &mut self.skip,
            if self.block_min_fl == u32::MAX {
                0
            } else {
                self.block_min_fl
            },
        );
        self.in_block = 0;
    }

    /// Write the finished term span to `out` and reset for the next term.
    /// Returns the span's length.
    fn finish(&mut self, out: &mut impl std::io::Write) -> Result<u64, KoshaError> {
        if self.in_block > 0 {
            if self.version == BITPACKED_SKIP_SPLIT_INVERTED_VERSION {
                self.flush_bp_block();
            } else {
                self.close_block();
            }
        }
        let mut header = Vec::with_capacity(5);
        put_var_u32(&mut header, self.count);
        let len = header.len() as u64
            + self.skip.len() as u64
            + self.scoring.len()
            + self.positions.len();
        out.write_all(&header)?;
        out.write_all(&self.skip)?;
        self.scoring.drain_to(out)?;
        self.positions.drain_to(out)?;
        self.count = 0;
        self.skip.clear();
        self.prev_doc = 0;
        self.prev_block_last = 0;
        Ok(len)
    }

    #[cfg(test)]
    fn spilled_total(&self) -> u64 {
        self.scoring.spilled_total + self.positions.spilled_total
    }
}

/// Per-buffer cap of a [`TermEncoder`]'s in-memory bytes: a sixteenth of
/// the build budget, between 4 KiB and 8 MiB.
fn term_spill_limit(build_memory_bytes: usize) -> usize {
    (build_memory_bytes / 16).clamp(4 * 1024, 8 * 1024 * 1024)
}

/// File-to-file version of [`maybe_compress_inverted`]: the same output
/// bytes rules (small files and incompressible ones stay raw), streamed so
/// a multi-GB posting shard is never held in memory. Moves the result to
/// `dest` and removes `raw`.
fn maybe_compress_inverted_file(raw: &Path, dest: &Path, raw_len: u64) -> Result<(), KoshaError> {
    if raw_len < 4096 {
        fs::rename(raw, dest)?;
        return Ok(());
    }
    let compressed_path = raw.with_extension("zst");
    {
        let mut out = std::io::BufWriter::new(fs::File::create(&compressed_path)?);
        std::io::Write::write_all(&mut out, &COMPRESSED_INVERTED_MAGIC.to_le_bytes())?;
        std::io::Write::write_all(&mut out, &COMPRESSED_INVERTED_VERSION.to_le_bytes())?;
        std::io::Write::write_all(&mut out, &raw_len.to_le_bytes())?;
        let mut encoder = zstd::stream::Encoder::new(out, INVERTED_COMPRESSION_LEVEL)?;
        // Record the size in the frame header, as `zstd::bulk` does.
        encoder.set_pledged_src_size(Some(raw_len))?;
        let mut input = std::io::BufReader::new(fs::File::open(raw)?);
        std::io::copy(&mut input, &mut encoder)?;
        let mut out = encoder.finish()?;
        std::io::Write::flush(&mut out)?;
    }
    let compressed_len = fs::metadata(&compressed_path)?.len();
    if compressed_len < raw_len {
        fs::rename(&compressed_path, dest)?;
        fs::remove_file(raw)?;
    } else {
        fs::remove_file(&compressed_path)?;
        fs::rename(raw, dest)?;
    }
    Ok(())
}

impl Drop for SegmentWriter {
    /// An abandoned build (an error mid-compaction, a writer dropped
    /// without `finalize`) leaves no partial files behind, and removes the
    /// output directory too if nothing else is in it.
    fn drop(&mut self) {
        if self.finalized {
            return;
        }
        self.doc_store = None;
        let _ = fs::remove_dir_all(self.build_dir());
        let _ = fs::remove_dir(&self.output_dir);
    }
}

// ─── Doc vectors sidecar (#243) ─────────────────────────────────────────────
//
// `doc_vectors.bin` holds every parsed `Vector` field value as raw f32 LE,
// so `doc_store.bin` no longer carries each embedding as JSON text (~14 KB
// for 1024 dims, vs 4 KB binary). `vector.idx` can't serve as the stored
// copy: from format v3 it holds unit-normalized vectors only, so the
// original magnitudes would be lost.
//
// Layout:
//   [magic "KDV1"][field_count:u32] then per field [len:u32][utf8 name]
//   [entry_count:u32] then per entry [doc_seq:u32][field_idx:u32][dim:u32]
//   then every entry's vector, f32 LE x dim, in entry order.
// Entries are sorted by doc_seq (ties keep the doc's field order), so one
// document's vectors are a binary-searchable run and one contiguous span.
// The header is 12 bytes/entry and stays resident; vectors are read with
// one seek+read per document, the way doc-store records are.

/// File name of the doc-vectors sidecar.
pub const DOC_VECTORS_FILE: &str = "doc_vectors.bin";
/// File name of the document store.
pub const DOC_STORE_FILE: &str = "doc_store.bin";

/// Whether a local file is present, non-empty and at least `complete_len`
/// bytes long — the "whole file, not a torn write" check.
fn file_is_complete(path: &Path, complete_len: u64) -> bool {
    fs::metadata(path).is_ok_and(|m| m.len() > 0 && m.len() >= complete_len)
}
/// Magic prefix of `doc_vectors.bin` ("KDV1", little-endian u32).
pub const DOC_VECTORS_MAGIC: u32 = u32::from_le_bytes(*b"KDV1");

/// Where a writer stores parsed `Vector` field values (#243).
/// `KOSHA_VECTOR_STORE=sidecar` (the default) moves them to
/// `doc_vectors.bin`; `inline` keeps the pre-#243 JSON in `doc_store.bin`.
///
/// Rollout note: binaries older than #243 don't know the sidecar. They
/// would read a sidecar segment's vector fields as empty strings, and an
/// old binary *compacting* one would drop those vectors from its output.
/// Set `inline` before rolling back past #243.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorStoreMode {
    Sidecar,
    Inline,
}

impl VectorStoreMode {
    fn from_env() -> Self {
        static MODE: OnceLock<VectorStoreMode> = OnceLock::new();
        *MODE.get_or_init(|| Self::parse(std::env::var("KOSHA_VECTOR_STORE").ok().as_deref()))
    }

    fn parse(raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") | Some("sidecar") => Self::Sidecar,
            Some("inline") => Self::Inline,
            Some(other) => {
                eprintln!("WARN: KOSHA_VECTOR_STORE={other} not sidecar|inline — using sidecar");
                Self::Sidecar
            }
        }
    }
}

/// How many bytes of `doc_vectors.bin` a header read asks for first. The
/// header is 12 bytes per entry plus the field names; one probe of this
/// size covers ~5k entries, and a larger header costs one more read of
/// exactly the missing bytes (see [`doc_vectors_header_len`]).
pub const DOC_VECTORS_HEADER_PROBE: u64 = 64 * 1024;

/// Header of `doc_vectors.bin` parsed from a prefix of the file.
struct DocVectorsHeader {
    fields: Vec<String>,
    entries: Vec<(u32, u32, u32)>,
    offsets: Vec<u64>,
    /// File length the header implies (end of the last vector).
    file_len: u64,
}

/// Outcome of parsing a `doc_vectors.bin` prefix.
enum HeaderParse {
    /// The prefix ends inside the header; at least this many bytes are
    /// needed.
    Need(u64),
    Done(DocVectorsHeader),
}

fn parse_doc_vectors_header(prefix: &[u8], what: &Path) -> Result<HeaderParse, KoshaError> {
    let corrupt = |why: &str| KoshaError::CorruptSegment(format!("{}: {why}", what.display()));
    let u32_at = |at: u64| -> Option<u32> {
        let at = at as usize;
        prefix
            .get(at..at + 4)
            .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
    };
    let Some(magic) = u32_at(0) else {
        return Ok(HeaderParse::Need(8));
    };
    if magic != DOC_VECTORS_MAGIC {
        return Err(corrupt("bad magic"));
    }
    let Some(field_count) = u32_at(4) else {
        return Ok(HeaderParse::Need(8));
    };
    let mut at: u64 = 8;
    let mut fields = Vec::with_capacity((field_count as usize).min(1024));
    for _ in 0..field_count {
        let Some(len) = u32_at(at) else {
            return Ok(HeaderParse::Need(at + 4));
        };
        let start = at + 4;
        let end = start + len as u64;
        let Some(name) = prefix.get(start as usize..end as usize) else {
            return Ok(HeaderParse::Need(end));
        };
        fields.push(String::from_utf8_lossy(name).into_owned());
        at = end;
    }
    let Some(entry_count) = u32_at(at) else {
        return Ok(HeaderParse::Need(at + 4));
    };
    at += 4;
    let header_len = at + entry_count as u64 * 12;
    if (prefix.len() as u64) < header_len {
        return Ok(HeaderParse::Need(header_len));
    }
    let mut entries = Vec::with_capacity(entry_count as usize);
    let mut offsets = Vec::with_capacity(entry_count as usize);
    let mut next = header_len;
    let mut last_seq = 0u32;
    for i in 0..entry_count as u64 {
        let base = at + i * 12;
        let (doc_seq, field_idx, dim) = (
            u32_at(base).unwrap(),
            u32_at(base + 4).unwrap(),
            u32_at(base + 8).unwrap(),
        );
        if field_idx as usize >= fields.len() {
            return Err(corrupt("entry names an unknown field"));
        }
        if doc_seq < last_seq {
            return Err(corrupt("entries not sorted by doc_seq"));
        }
        last_seq = doc_seq;
        entries.push((doc_seq, field_idx, dim));
        offsets.push(next);
        next += dim as u64 * 4;
    }
    Ok(HeaderParse::Done(DocVectorsHeader {
        fields,
        entries,
        offsets,
        file_len: next,
    }))
}

/// How many leading bytes of `doc_vectors.bin` hold its header, given a
/// prefix of the file: `Ok(Ok(len))` once the prefix contains the whole
/// header (`len` may be shorter than the prefix), `Ok(Err(need))` when at
/// least `need` bytes are required to find out. A remote reader fetches
/// [`DOC_VECTORS_HEADER_PROBE`] bytes, then the rest if asked to.
pub fn doc_vectors_header_len(prefix: &[u8]) -> Result<Result<u64, u64>, KoshaError> {
    match parse_doc_vectors_header(prefix, Path::new(DOC_VECTORS_FILE))? {
        HeaderParse::Need(n) => Ok(Err(n)),
        HeaderParse::Done(h) => Ok(Ok(h.offsets.first().copied().unwrap_or(h.file_len))),
    }
}

/// How a [`SegmentWriter`] stores each vector column's ANN posting entries
/// (#210): a default for every field plus per-field overrides.
///
/// Production writers read it once from the environment:
/// `KOSHA_VECTOR_QUANTIZATION=f32|i8|rabitq` (default `f32`) sets the default and
/// `KOSHA_VECTOR_QUANTIZATION_FIELDS=body_emb:i8,title_emb:f32` overrides
/// it per field. The namespace schema (#220) carries only analyzers so
/// far; declaring quantization per field there is follow-up work.
///
/// `i8` writes vector index version [`I8_VECTOR_INDEX_VERSION`] and
/// `rabitq` writes [`RABITQ_VECTOR_INDEX_VERSION`]; a reader built before
/// either cannot read it (it fails the kNN on that column).
/// Turn it on only once every query pod runs a build that reads it, and
/// turn it off and compact before rolling back past one. With it off (the default) segments are byte-identical to those
/// written before #210. `doc_vectors.bin` keeps full-precision vectors
/// either way: it is the document's own value, rescoring reads it, and
/// compaction re-encodes every index from it (#253).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VectorQuantizationConfig {
    pub default: VectorQuantization,
    pub fields: HashMap<String, VectorQuantization>,
}

impl VectorQuantizationConfig {
    /// Every field stored as `quantization`.
    pub fn all(quantization: VectorQuantization) -> Self {
        Self {
            default: quantization,
            fields: HashMap::new(),
        }
    }

    /// `self` with `field` stored as `quantization`.
    pub fn with_field(
        mut self,
        field: impl Into<String>,
        quantization: VectorQuantization,
    ) -> Self {
        self.fields.insert(field.into(), quantization);
        self
    }

    /// How `field`'s postings are stored.
    pub fn for_field(&self, field: &str) -> VectorQuantization {
        self.fields.get(field).copied().unwrap_or(self.default)
    }

    fn from_env() -> &'static Self {
        static CONFIG: OnceLock<VectorQuantizationConfig> = OnceLock::new();
        CONFIG.get_or_init(|| {
            Self::parse(
                std::env::var("KOSHA_VECTOR_QUANTIZATION").ok().as_deref(),
                std::env::var("KOSHA_VECTOR_QUANTIZATION_FIELDS")
                    .ok()
                    .as_deref(),
            )
        })
    }

    /// Parses the two env values. Anything unrecognised is warned about
    /// and ignored — falling back to `f32`, which every reader reads.
    pub fn parse(default: Option<&str>, fields: Option<&str>) -> Self {
        let default = match default.map(str::trim) {
            None | Some("") => VectorQuantization::F32,
            Some(raw) => VectorQuantization::parse(raw).unwrap_or_else(|| {
                eprintln!("WARN: KOSHA_VECTOR_QUANTIZATION={raw} not f32|i8|rabitq — using f32");
                VectorQuantization::F32
            }),
        };
        let mut config = Self::all(default);
        for entry in fields.unwrap_or("").split(',').map(str::trim) {
            if entry.is_empty() {
                continue;
            }
            match entry
                .rsplit_once(':')
                .and_then(|(name, q)| Some((name.trim(), VectorQuantization::parse(q)?)))
            {
                Some((name, q)) if !name.is_empty() => {
                    config.fields.insert(name.to_string(), q);
                }
                _ => eprintln!(
                    "WARN: KOSHA_VECTOR_QUANTIZATION_FIELDS entry {entry:?} not <field>:f32|i8|rabitq — ignored"
                ),
            }
        }
        config
    }
}

/// Logged once: i8 was asked for but `KOSHA_VECTOR_WRITE_VERSION` pins an
/// older index version, so the writer keeps writing f32.
fn warn_quantization_needs_v4() {
    static WARNED: OnceLock<()> = OnceLock::new();
    WARNED.get_or_init(|| {
        eprintln!(
            "WARN: i8/rabitq vector quantization needs KOSHA_VECTOR_WRITE_VERSION=4 (the default) — writing f32 vectors"
        );
    });
}

/// Parsed `doc_vectors.bin` header: field names, entries and where each
/// entry's vector starts in the file. Vectors themselves are read on
/// demand, from the local file when it is whole, otherwise from the sparse
/// span cache (#245, see [`span_cache`]).
#[derive(Debug)]
pub struct DocVectors {
    segment_dir: PathBuf,
    fields: Vec<String>,
    /// `(doc_seq, field_idx, dim)`, sorted by doc_seq.
    entries: Vec<(u32, u32, u32)>,
    /// Absolute byte offset of each entry's vector.
    offsets: Vec<u64>,
    /// Length of the complete file.
    file_len: u64,
}

/// Span-cache listings of one segment, kept across the documents of one
/// result page ([`SegmentReader::page_doc_is_local`],
/// [`SegmentReader::page_doc_record`]): a listing is a `read_dir` plus a
/// stat of every cached span, so taking one per document cost a page
/// `documents x spans` stats.
#[derive(Default)]
pub struct PageSpans {
    doc_store: Option<span_cache::SpanIndex>,
    doc_vectors: Option<span_cache::SpanIndex>,
}

/// Which sidecar vectors a visit reads, in which runs: see
/// [`DocVectors::visit_field_vectors`].
#[derive(Default)]
struct VectorRuns {
    /// Entry indices of the wanted vectors, ascending.
    wanted: Vec<usize>,
    /// `[start, end)` into `wanted`, one per run.
    runs: Vec<(usize, usize)>,
    /// The `[start, end)` file bytes of each run.
    ranges: Vec<(u64, u64)>,
}

impl DocVectors {
    /// Open a whole local `doc_vectors.bin`, checking its length against
    /// the header.
    pub fn open(path: PathBuf) -> Result<Self, KoshaError> {
        let corrupt = |why: &str| KoshaError::CorruptSegment(format!("{}: {why}", path.display()));
        let file_len = fs::metadata(&path)?.len();
        let mut file = fs::File::open(&path)?;
        let mut prefix = Vec::new();
        let mut want = DOC_VECTORS_HEADER_PROBE.min(file_len);
        let header = loop {
            if want > prefix.len() as u64 {
                let mut more = vec![0u8; (want - prefix.len() as u64) as usize];
                file.read_exact(&mut more)
                    .map_err(|e| KoshaError::CorruptSegment(format!("truncated header: {e}")))?;
                prefix.extend_from_slice(&more);
            }
            match parse_doc_vectors_header(&prefix, &path)? {
                HeaderParse::Done(header) => break header,
                HeaderParse::Need(n) if n > file_len || n <= prefix.len() as u64 => {
                    return Err(corrupt("truncated header"));
                }
                HeaderParse::Need(n) => want = n,
            }
        };
        if header.file_len != file_len {
            let header_len = header.offsets.first().copied().unwrap_or(header.file_len);
            return Err(corrupt(&format!(
                "vector data is {} bytes, header says {}",
                file_len.saturating_sub(header_len),
                header.file_len - header_len
            )));
        }
        let segment_dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
        Ok(Self::from_header(segment_dir, header))
    }

    /// Build from a cached prefix of the file (the sparse span cache's
    /// header span). `Ok(None)` when the prefix ends inside the header.
    pub fn from_prefix(segment_dir: PathBuf, prefix: &[u8]) -> Result<Option<Self>, KoshaError> {
        match parse_doc_vectors_header(prefix, &segment_dir.join(DOC_VECTORS_FILE))? {
            HeaderParse::Need(_) => Ok(None),
            HeaderParse::Done(header) => Ok(Some(Self::from_header(segment_dir, header))),
        }
    }

    fn from_header(segment_dir: PathBuf, header: DocVectorsHeader) -> Self {
        Self {
            segment_dir,
            fields: header.fields,
            entries: header.entries,
            offsets: header.offsets,
            file_len: header.file_len,
        }
    }

    /// Length of the complete file, as the header describes it.
    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    fn entry_range(&self, doc_seq: u32) -> (usize, usize) {
        let start = self.entries.partition_point(|e| e.0 < doc_seq);
        let end = self.entries.partition_point(|e| e.0 <= doc_seq);
        (start, end)
    }

    /// The `[start, end)` byte range of `doc_seq`'s vectors in the file;
    /// `None` when the document has none.
    pub fn doc_span(&self, doc_seq: u32) -> Option<(u64, u64)> {
        let (start, end) = self.entry_range(doc_seq);
        (start < end).then(|| {
            (
                self.offsets[start],
                self.offsets[end - 1] + self.entries[end - 1].2 as u64 * 4,
            )
        })
    }

    /// The sidecar's field-name table, in first-seen order.
    pub fn field_names(&self) -> &[String] {
        &self.fields
    }

    /// `(field name, vector)` for every vector stored for `doc_seq`, in the
    /// document's field order. One local read.
    pub fn vectors_for(&self, doc_seq: u32) -> Result<Vec<(&str, Vec<f32>)>, KoshaError> {
        self.vectors_for_reusing(doc_seq, &mut None)
    }

    /// [`Self::vectors_for`] keeping the span listing in `spans` across
    /// calls (see [`span_cache::read_local_range_reusing`]).
    pub fn vectors_for_reusing(
        &self,
        doc_seq: u32,
        spans: &mut Option<span_cache::SpanIndex>,
    ) -> Result<Vec<(&str, Vec<f32>)>, KoshaError> {
        let (start, end) = self.entry_range(doc_seq);
        let Some((span_start, span_end)) = self.doc_span(doc_seq) else {
            return Ok(Vec::new());
        };
        let buf = span_cache::read_local_range_reusing(
            &self.segment_dir,
            DOC_VECTORS_FILE,
            self.file_len,
            span_start,
            span_end - span_start,
            spans,
        )
        .ok_or_else(|| {
            KoshaError::NotFound(format!(
                "{DOC_VECTORS_FILE} bytes {span_start}..{span_end} for doc_seq {doc_seq} are not \
                 local in segment {:?}",
                self.segment_dir
            ))
        })?;
        let mut out = Vec::with_capacity(end - start);
        for i in start..end {
            let (_, field_idx, dim) = self.entries[i];
            let at = (self.offsets[i] - span_start) as usize;
            let vector = buf[at..at + dim as usize * 4]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect();
            out.push((self.fields[field_idx as usize].as_str(), vector));
        }
        Ok(out)
    }

    /// `field`'s vector of `doc_seq` out of `buf`, the bytes of the
    /// document's whole [`Self::doc_span`] (#381). `None` when the document
    /// has no `field` vector or `buf` is not that span's length.
    fn field_vector_in_span(&self, doc_seq: u32, field: &str, buf: &[u8]) -> Option<Vec<f32>> {
        let (start, end) = self.entry_range(doc_seq);
        let (span_start, span_end) = self.doc_span(doc_seq)?;
        if buf.len() as u64 != span_end - span_start {
            return None;
        }
        (start..end).find_map(|i| {
            let (_, field_idx, dim) = self.entries[i];
            if self.fields[field_idx as usize] != field {
                return None;
            }
            let at = (self.offsets[i] - span_start) as usize;
            Some(
                buf[at..at + dim as usize * 4]
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|b| f32::from_le_bytes(*b))
                    .collect(),
            )
        })
    }

    /// Whether the sidecar holds any vector of `field`.
    pub fn has_field(&self, field: &str) -> bool {
        self.fields.iter().any(|name| name == field)
    }

    /// Calls `visit(doc_seq, vector)` for `field`'s vector of every
    /// document in `doc_seqs` (ascending) that has one, in `doc_seq` order,
    /// and returns how many were visited. Reads the whole local file.
    ///
    /// Built for exact kNN over a filtered set (#252): vectors that sit
    /// close together in the file (gaps up to [`Self::COALESCE_GAP`]) are
    /// read with one read, so a dense set costs a few large sequential
    /// reads and a sparse one a seek per document, never a file open per
    /// document as [`Self::vectors_for`] does.
    pub fn visit_field_vectors(
        &self,
        field: &str,
        doc_seqs: &[u32],
        visit: impl FnMut(u32, &[f32]),
    ) -> Result<usize, KoshaError> {
        let plan = self.field_vector_runs(field, doc_seqs);
        if plan.wanted.is_empty() {
            return Ok(0);
        }
        let mut file = fs::File::open(self.segment_dir.join(DOC_VECTORS_FILE))?;
        self.visit_runs(&plan, visit, |start, end, buf| {
            buf.resize((end - start) as usize, 0);
            file.seek(SeekFrom::Start(start))?;
            file.read_exact(buf)?;
            Ok(())
        })
    }

    /// The byte ranges [`Self::visit_field_vectors`] reads for `field`'s
    /// vectors of `doc_seqs` (ascending), in file order: the ranges a
    /// reader without the whole file fetches into the span cache first.
    pub fn field_vector_ranges(&self, field: &str, doc_seqs: &[u32]) -> Vec<(u64, u64)> {
        self.field_vector_runs(field, doc_seqs).ranges
    }

    /// [`Self::visit_field_vectors`] reading each of
    /// [`Self::field_vector_ranges`] from the sparse span cache instead of
    /// the whole file (#245). Same vectors, same order, same arithmetic for
    /// the caller. `Err` when a range is not cached (never fetched, or
    /// evicted since).
    pub fn visit_field_vectors_cached(
        &self,
        spans: &span_cache::SpanIndex,
        field: &str,
        doc_seqs: &[u32],
        visit: impl FnMut(u32, &[f32]),
    ) -> Result<usize, KoshaError> {
        let plan = self.field_vector_runs(field, doc_seqs);
        self.visit_runs(&plan, visit, |start, end, buf| {
            *buf = spans.read(start, end).ok_or_else(|| {
                KoshaError::NotFound(format!(
                    "{DOC_VECTORS_FILE} bytes {start}..{end} are not cached in segment {:?}",
                    self.segment_dir
                ))
            })?;
            Ok(())
        })
    }

    /// Entry indices of `field`'s vectors of `doc_seqs` (ascending), and
    /// the runs they are read in.
    fn field_vector_runs(&self, field: &str, doc_seqs: &[u32]) -> VectorRuns {
        let mut plan = VectorRuns::default();
        let Some(field_idx) = self.fields.iter().position(|name| name == field) else {
            return plan;
        };
        let field_idx = field_idx as u32;
        // Entries are sorted by doc_seq, and so is `doc_seqs`.
        let mut from = 0usize;
        for &doc_seq in doc_seqs {
            let start = from + self.entries[from..].partition_point(|e| e.0 < doc_seq);
            let mut i = start;
            while i < self.entries.len() && self.entries[i].0 == doc_seq {
                if self.entries[i].1 == field_idx {
                    plan.wanted.push(i);
                }
                i += 1;
            }
            from = i;
        }
        let end_of = |i: usize| self.offsets[i] + self.entries[i].2 as u64 * 4;
        let mut run_start = 0usize;
        while run_start < plan.wanted.len() {
            let first = self.offsets[plan.wanted[run_start]];
            let mut run_end = run_start + 1;
            while run_end < plan.wanted.len()
                && self.offsets[plan.wanted[run_end]]
                    <= end_of(plan.wanted[run_end - 1]) + Self::COALESCE_GAP
                && end_of(plan.wanted[run_end]) - first <= Self::MAX_RUN_BYTES
            {
                run_end += 1;
            }
            plan.runs.push((run_start, run_end));
            plan.ranges.push((first, end_of(plan.wanted[run_end - 1])));
            run_start = run_end;
        }
        plan
    }

    /// Decode every run of `plan`, reading its bytes into the buffer with
    /// `read(start, end, buf)`, and visit its vectors in `doc_seq` order.
    fn visit_runs(
        &self,
        plan: &VectorRuns,
        mut visit: impl FnMut(u32, &[f32]),
        mut read: impl FnMut(u64, u64, &mut Vec<u8>) -> Result<(), KoshaError>,
    ) -> Result<usize, KoshaError> {
        let mut buf: Vec<u8> = Vec::new();
        let mut vector: Vec<f32> = Vec::new();
        for (&(run_start, run_end), &(span_start, span_end)) in plan.runs.iter().zip(&plan.ranges) {
            read(span_start, span_end, &mut buf)?;
            for &i in &plan.wanted[run_start..run_end] {
                let at = (self.offsets[i] - span_start) as usize;
                let dim = self.entries[i].2 as usize;
                vector.clear();
                vector.extend(
                    buf[at..at + dim * 4]
                        .as_chunks::<4>()
                        .0
                        .iter()
                        .map(|b| f32::from_le_bytes(*b)),
                );
                visit(self.entries[i].0, &vector);
            }
        }
        Ok(plan.wanted.len())
    }

    /// Largest gap between two wanted vectors that
    /// [`Self::visit_field_vectors`] reads through rather than seeking
    /// over: reading 64 KiB costs about what one more seek does.
    pub const COALESCE_GAP: u64 = 64 * 1024;

    /// Most bytes one run reads at once. Without a cap a dense candidate
    /// set (every document of a one-matter segment) is a single run the
    /// size of the whole file — a 2 GB buffer for one segment's exact kNN.
    /// 8 MiB is ~2,700 768-dim vectors per read: still a few large
    /// sequential reads, at a bounded heap cost per scoring thread.
    pub const MAX_RUN_BYTES: u64 = 8 * 1024 * 1024;

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Whether `segment_dir`'s footer says vectors live in `doc_vectors.bin`.
/// Reads the local `footer.json`; `false` if it can't be read.
pub fn segment_uses_doc_vectors(segment_dir: &Path) -> bool {
    SegmentReader::read_footer(segment_dir).is_ok_and(|footer| footer.doc_vectors)
}

/// `KOSHA_VECTOR_BUILD_REFINE`, read once: whether a from-scratch vector
/// index build (every flush, and compaction when it does not seed from
/// postings) refines bisection's postings by nearest-centroid reassignment
/// and rebalances them (#253). `off` builds exactly as before. The on-disk
/// format is the same either way.
fn vector_build_refine() -> bool {
    static REFINE: OnceLock<bool> = OnceLock::new();
    *REFINE.get_or_init(|| {
        parse_vector_build_refine(std::env::var("KOSHA_VECTOR_BUILD_REFINE").ok().as_deref())
    })
}

fn parse_vector_build_refine(raw: Option<&str>) -> bool {
    match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("on") | Some("true") | Some("1") => true,
        Some("off") | Some("false") | Some("0") => false,
        Some(other) => {
            eprintln!("WARN: KOSHA_VECTOR_BUILD_REFINE={other} not on|off — using on");
            true
        }
    }
}

/// The vector-index version `finalize` writes — `KOSHA_VECTOR_WRITE_VERSION`,
/// accepted values `1` (legacy flat, no magic — full parse + `build_hnsw` on
/// every open), `2` (posting-based, raw magnitudes), `3` (posting-based,
/// unit-norm vectors + centroids → dot-only scoring; on-disk header
/// [`UNIT_NORM_VECTOR_INDEX_VERSION`]), and `4` (posting-based, unit-norm,
/// plus a per-posting radius for triangle-inequality-bounded probe ranking;
/// on-disk header [`RADIUS_AWARE_VECTOR_INDEX_VERSION`]); default `4`. Same
/// mixed-fleet rollout/rollback purpose as `postings_write_version`: during
/// a rolling deploy (writer upgraded before every query replica understands
/// the newest version) or ahead of a possible rollback, pin an older tier
/// until the whole fleet reads it. Read once (segments are finalized
/// frequently).
fn vector_write_version() -> u32 {
    static VERSION: OnceLock<u32> = OnceLock::new();
    *VERSION.get_or_init(|| match std::env::var("KOSHA_VECTOR_WRITE_VERSION").ok().as_deref() {
        Some("1") => 1,
        Some("2") => 2,
        Some("3") => 3,
        Some("4") | None => 4,
        Some(other) => {
            eprintln!("WARN: KOSHA_VECTOR_WRITE_VERSION={other} not supported (accepted: 1, 2, 3, 4) — writing v4");
            4
        }
    })
}
/// Wrapper magic for a zstd-compressed inverted artifact — applied
/// independently to the TOC (`inverted.idx`) and to each posting blob
/// shard, so lazy per-shard fetch/decode is unchanged. The wrapped bytes
/// are the regular uncompressed layout; one decompression at read
/// restores them and every downstream code path is oblivious.
pub const COMPRESSED_INVERTED_MAGIC: u32 = u32::from_le_bytes(*b"KIZC");
const COMPRESSED_INVERTED_VERSION: u32 = 1;
const COMPRESSED_INVERTED_HEADER_LEN: usize = 16;
const INVERTED_COMPRESSION_LEVEL: i32 = 1;
/// magic + version + term_count + reserved, 4 bytes each.
const INVERTED_HEADER_LEN: usize = 16;
/// term_off u64 + term_len u32 + postings_off u64 + postings_len u32.
const INVERTED_TABLE_ENTRY_LEN: usize = 24;
/// term_off u64 + term_len u32 + blob_id u32 + postings_off u64 +
/// postings_len u32 + reserved u32.
const SPLIT_INVERTED_TABLE_ENTRY_LEN: usize = 32;
pub const POSTING_BLOB_SHARDS: usize = 256;

fn posting_blob_id_for_term(term: &str) -> usize {
    let mut hash: u32 = 2_166_136_261;
    for byte in term.as_bytes() {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(16_777_619);
    }
    (hash as usize) % POSTING_BLOB_SHARDS
}

fn posting_blob_file_for_id(blob_id: usize) -> String {
    format!("postings-{blob_id:02x}.bin")
}

pub fn posting_blob_file_for_term(term: &str) -> String {
    posting_blob_file_for_id(posting_blob_id_for_term(term))
}

pub fn all_posting_blob_files() -> Vec<String> {
    (0..POSTING_BLOB_SHARDS)
        .map(posting_blob_file_for_id)
        .collect()
}

/// Shards actually referenced by a split index's TOC. Empty shard files
/// are not written, so hydration must not request them merely because a
/// query term hashes to that shard. Does not read posting payloads.
pub fn posting_blob_inventory(
    segment_dir: &Path,
) -> Result<std::collections::HashSet<String>, KoshaError> {
    let data = read_inverted_payload(fs::read(segment_dir.join("inverted.idx"))?)?;
    let index = SplitInvertedIndex::from_bytes(data, segment_dir.to_path_buf(), None)?;
    Ok((0..index.term_count)
        .map(|i| posting_blob_file_for_id(index.raw_entry(i).2))
        .collect())
}

/// One query term's postings span in a split segment: bytes
/// `[start, start + len)` of shard `file`, from the `inverted.idx` TOC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermSpan {
    pub term: String,
    pub file: String,
    pub start: u64,
    pub len: u64,
}

/// The TOC spans of each of `terms` this split segment contains, in
/// `terms` order (terms it does not contain are left out, duplicates
/// once). Reads only the local `inverted.idx`, never a shard. The spans
/// index the shard's logical bytes, which are the stored bytes only when
/// the segment's footer says `posting_blobs_raw` — a caller range-reading
/// the stored file must check that first (#245).
pub fn posting_term_spans(
    segment_dir: &Path,
    terms: &[String],
) -> Result<Vec<TermSpan>, KoshaError> {
    let data = read_inverted_payload(fs::read(segment_dir.join("inverted.idx"))?)?;
    let index = SplitInvertedIndex::from_bytes(data, segment_dir.to_path_buf(), None)?;
    let mut seen = std::collections::HashSet::new();
    Ok(terms
        .iter()
        .filter(|t| seen.insert(t.as_str()))
        .filter_map(|term| {
            let i = index.find(term)?;
            let (_, _, blob_id, off, len) = index.raw_entry(i);
            Some(TermSpan {
                term: term.clone(),
                file: posting_blob_file_for_id(blob_id),
                start: off as u64,
                len: len as u64,
            })
        })
        .collect())
}

/// Byte breakdown of one skip-split (v5/v6) segment's postings, for the
/// #386 bit-packing experiment. Diagnostics only: nothing on the query or
/// write path calls it.
#[doc(hidden)]
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PostingsSectionStats {
    /// The `inverted.idx` TOC version (5 or 6).
    pub inverted_version: u32,
    pub terms: u64,
    pub postings: u64,
    /// Postings in full [`SCORING_BLOCK_LEN`] blocks: the only ones v6
    /// bit-packs.
    pub postings_in_full_blocks: u64,
    /// Per-term doc count + skip table, as stored.
    pub skip_bytes: u64,
    /// Scoring `(doc, tf)` sections, as stored.
    pub scoring_bytes: u64,
    /// Positions sections, as stored (the same codec in v5 and v6).
    pub positions_bytes: u64,
    /// The same scoring blocks re-encoded as v5 varints and as v6
    /// (full blocks bit-packed, tails varint): a codec-only comparison on
    /// identical postings, whatever the segment layout.
    pub scoring_bytes_as_v5: u64,
    pub scoring_bytes_as_v6: u64,
    /// Total logical (decompressed) bytes of the `postings-*.bin` shards
    /// the TOC references.
    pub shard_logical_bytes: u64,
}

/// [`PostingsSectionStats`] for a split segment whose shards are all local
/// (whole files, not span-cache fragments). Errors on v3/v4 segments, on a
/// missing shard and on any span or block that does not decode.
#[doc(hidden)]
pub fn postings_section_stats(segment_dir: &Path) -> Result<PostingsSectionStats, KoshaError> {
    let corrupt =
        |msg: String| KoshaError::CorruptSegment(format!("{}: {msg}", segment_dir.display()));
    let data = read_inverted_payload(fs::read(segment_dir.join("inverted.idx"))?)?;
    let index = SplitInvertedIndex::from_bytes(data, segment_dir.to_path_buf(), None)?;
    let inverted_version = u32::from_le_bytes(index.data[4..8].try_into().unwrap());
    let Some(bitpacked) = index.encoding.skip_split() else {
        return Err(corrupt(format!(
            "inverted version {inverted_version} is not skip-split (v5/v6)"
        )));
    };
    let mut by_blob: BTreeMap<usize, Vec<(usize, usize)>> = BTreeMap::new();
    for i in 0..index.term_count {
        let (_, _, blob_id, off, len) = index.raw_entry(i);
        by_blob.entry(blob_id).or_default().push((off, len));
    }
    let mut stats = PostingsSectionStats {
        inverted_version,
        terms: index.term_count as u64,
        ..Default::default()
    };
    let mut block = Vec::with_capacity(SCORING_BLOCK_LEN);
    let mut scratch = Vec::new();
    for (blob_id, spans) in by_blob {
        let blob = index
            .read_posting_blob(blob_id)
            .ok_or_else(|| corrupt(format!("shard {blob_id} missing or unreadable")))?;
        stats.shard_logical_bytes += blob.len() as u64;
        for (off, len) in spans {
            let bytes = off
                .checked_add(len)
                .and_then(|end| blob.get(off..end))
                .ok_or_else(|| corrupt(format!("span out of shard {blob_id} bounds")))?;
            let parsed = BlockedTermPostings::parse_with(Arc::new(bytes.to_vec()), bitpacked)
                .ok_or_else(|| corrupt(format!("span in shard {blob_id} does not parse")))?;
            stats.postings += parsed.doc_count() as u64;
            stats.skip_bytes += u64::from(parsed.scoring_base);
            stats.scoring_bytes += u64::from(parsed.positions_base - parsed.scoring_base);
            stats.positions_bytes += (bytes.len() - parsed.positions_base as usize) as u64;
            for b in 0..parsed.block_count() {
                if !parsed.read_block(b, &mut block) {
                    return Err(corrupt(format!("block {b} in shard {blob_id} corrupt")));
                }
                let prev_doc = if b == 0 {
                    0
                } else {
                    parsed.summary(b - 1).last_doc_id
                };
                scratch.clear();
                bitpack::encode_varint_block(&block, prev_doc, &mut scratch);
                stats.scoring_bytes_as_v5 += scratch.len() as u64;
                if block.len() == SCORING_BLOCK_LEN {
                    stats.postings_in_full_blocks += SCORING_BLOCK_LEN as u64;
                    let docs: [u32; SCORING_BLOCK_LEN] = std::array::from_fn(|i| block[i].doc_id);
                    let tfs: [u32; SCORING_BLOCK_LEN] =
                        std::array::from_fn(|i| block[i].term_frequency);
                    let next_min = if b == 0 { 0 } else { prev_doc.wrapping_add(1) };
                    scratch.clear();
                    bitpack::encode_bp_block(&docs, &tfs, next_min, &mut scratch);
                }
                stats.scoring_bytes_as_v6 += scratch.len() as u64;
            }
        }
    }
    Ok(stats)
}

pub fn inverted_uses_posting_blobs(segment_dir: &Path) -> bool {
    let Some(header) = read_inverted_header(&segment_dir.join("inverted.idx")) else {
        return false;
    };
    if header[0..4] != INVERTED_MAGIC.to_le_bytes() {
        return false;
    }
    let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
    // Delegates to the one canonical predicate — a version list that
    // drifts between this hydration gate and the reader is exactly the
    // PR #83 silent-zero-hits bug (cold reads skip posting-blob hydration
    // and score nothing).
    is_split_inverted_version(version)
}

/// First [`INVERTED_HEADER_LEN`] bytes of the *logical* (uncompressed)
/// inverted TOC. Raw files are read directly; KIZC-wrapped files
/// stream-decode only enough of the zstd frame to fill the header. The
/// hydration path calls this per segment per search to decide whether
/// `postings-*.bin` shards must be fetched, so it must never pay a
/// whole-file read or decompress — treating the wrapper as "not a split
/// index" here is what silently skipped posting-blob hydration and scored
/// compressed cold segments to zero hits.
fn read_inverted_header(path: &Path) -> Option<[u8; INVERTED_HEADER_LEN]> {
    use std::io::Read;

    let mut file = fs::File::open(path).ok()?;
    let mut outer = [0u8; COMPRESSED_INVERTED_HEADER_LEN];
    file.read_exact(&mut outer).ok()?;
    if outer[0..4] != COMPRESSED_INVERTED_MAGIC.to_le_bytes() {
        let mut header = [0u8; INVERTED_HEADER_LEN];
        header.copy_from_slice(&outer[..INVERTED_HEADER_LEN]);
        return Some(header);
    }
    if u32::from_le_bytes(outer[4..8].try_into().unwrap()) != COMPRESSED_INVERTED_VERSION {
        return None;
    }
    let mut header = [0u8; INVERTED_HEADER_LEN];
    let mut decoder = zstd::stream::read::Decoder::new(file).ok()?;
    decoder.read_exact(&mut header).ok()?;
    Some(header)
}

/// v2 `inverted.idx` layout — a table of contents instead of a stream:
///
/// ```text
/// header   magic:u32  version:u32  term_count:u32  reserved:u32
/// table    term_count × { term_off:u64  term_len:u32
///                          postings_off:u64  postings_len:u32 }
/// pool     all term strings back to back, in sorted term order
/// postings per term: count:u32, then count × { doc_id:u32  tf:u32
///                          npos:u32  npos × pos:u32 }
/// ```
///
/// All offsets are absolute within the file; the table is sorted by term
/// (byte order — the writer sorts), so lookup is a binary search comparing
/// pool slices, with zero parsing at open.
///
/// Why: the legacy stream layout forced `SegmentReader` to materialize the
/// *entire* vocabulary at open — a `String` per term plus a `Vec<Posting>`
/// per term plus a heap `Vec<u32>` per posting — even though a BM25 query
/// touches only its handful of query terms. For a prose corpus that eager
/// parse was the dominant resident cost of an open segment (bigger than the
/// file itself, from per-allocation overhead) and the dominant open-time
/// CPU. Here the whole file stays as one contiguous buffer (resident cost
/// == on-disk cost, which also makes `approx_segment_bytes`' on-disk proxy
/// exact for this file), and postings decode on demand, per queried term,
/// as a transient per-query cost.
///
/// All reads are bounds-checked; the table (spans + term UTF-8) is
/// validated once at open so per-query decode can't walk off the buffer.
pub struct LazyInvertedIndex {
    data: Vec<u8>,
    term_count: usize,
    /// Decoded-postings LRU — see [`PostingsCache`].
    cache: PostingsCache<usize, Arc<Vec<Posting>>>,
}

pub struct SplitInvertedIndex {
    data: Vec<u8>,
    segment_dir: PathBuf,
    term_count: usize,
    encoding: SplitPostingsEncoding,
    cache: PostingsCache<usize, Arc<Vec<Posting>>>,
    /// v5 (skip-split) only: per-term scoring views (skip table + raw
    /// scoring/positions bytes, block-decoded on demand) — cached
    /// separately from full decodes because the WAND read path never
    /// materializes `Vec<Posting>` at all. Same admission/LRU policy.
    scoring_cache: PostingsCache<usize, Arc<BlockedTermPostings>>,
    /// Set when a term the TOC *contains* could not be served because its
    /// posting-blob shard failed to read or decode (evicted from the disk
    /// cache, truncated, corrupt). Distinguishes "term absent" (a normal
    /// non-match) from "term unreadable" — without the distinction, a blob
    /// evicted after a presence check silently scores as zero hits (the
    /// PR #83 failure class). Readers drain it via
    /// [`SegmentReader::take_blob_read_failure`] and must surface an error
    /// instead of returning results that silently omit the term.
    blob_read_failure: std::sync::atomic::AtomicBool,
    /// Shards are raw (`Footer::posting_blobs_raw`), so a term whose shard
    /// is not local may be served from the sparse span cache
    /// (`postings-XX.bin.spans/`, see [`span_cache`]) that a query node
    /// fills with ranged GETs of just the query terms' spans (#245).
    posting_spans: bool,
}

/// Where one shard's term spans are read from for a batch of terms.
enum BlobSource {
    /// The whole shard (logical, decompressed bytes).
    Whole(Vec<u8>),
    /// Only fetched spans of a raw shard are local.
    Spans(span_cache::SpanIndex),
}

impl BlobSource {
    /// The `[off, off + len)` bytes of the shard's logical layout. `None`
    /// when they are not local: out of bounds for the whole shard (corrupt
    /// TOC/shard) or not covered by any span (never fetched, or evicted).
    fn term_bytes(&self, off: usize, len: usize) -> Option<std::borrow::Cow<'_, [u8]>> {
        let end = off.checked_add(len)?;
        match self {
            Self::Whole(blob) => blob.get(off..end).map(std::borrow::Cow::Borrowed),
            Self::Spans(index) => index
                .read(off as u64, end as u64)
                .map(std::borrow::Cow::Owned),
        }
    }
}

#[derive(Clone, Copy)]
enum SplitPostingsEncoding {
    Fixed32,
    VarintDelta,
    /// v5: `[skip table][scoring (doc,tf)][positions]` per term span — see
    /// [`BlockedTermPostings`] and [`write_postings_skip_split`].
    SkipSplit,
    /// v6: the v5 span with bit-packed full scoring blocks (#386).
    BitPackedSkipSplit,
}

impl SplitPostingsEncoding {
    /// `Some(bitpacked)` when term spans are skip-split (v5/v6) and so
    /// parse into a [`BlockedTermPostings`]; `None` for v3/v4.
    fn skip_split(self) -> Option<bool> {
        match self {
            Self::SkipSplit => Some(false),
            Self::BitPackedSkipSplit => Some(true),
            Self::Fixed32 | Self::VarintDelta => None,
        }
    }
}

/// Checked little-endian u32 read that advances the cursor.
fn take_u32(buf: &mut &[u8]) -> Option<u32> {
    let (head, rest) = buf.split_first_chunk::<4>()?;
    *buf = rest;
    Some(u32::from_le_bytes(*head))
}

fn put_var_u32(out: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn take_var_u32(buf: &mut &[u8]) -> Option<u32> {
    let mut value = 0u32;
    let mut shift = 0u32;
    for _ in 0..5 {
        let (&byte, rest) = buf.split_first()?;
        *buf = rest;
        value |= u32::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
    }
    None
}

fn maybe_compress_inverted(raw: &[u8]) -> Vec<u8> {
    // Small test/admin segments are not worth wrapping; this also keeps
    // tiny fixtures readable as plain `KINV` in tests and diagnostics.
    if raw.len() < 4096 {
        return raw.to_vec();
    }
    match zstd::bulk::compress(raw, INVERTED_COMPRESSION_LEVEL) {
        Ok(compressed) if compressed.len() + COMPRESSED_INVERTED_HEADER_LEN < raw.len() => {
            wrap_compressed_inverted(raw.len(), &compressed)
        }
        _ => raw.to_vec(),
    }
}

fn wrap_compressed_inverted(uncompressed_len: usize, compressed: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(COMPRESSED_INVERTED_HEADER_LEN + compressed.len());
    out.extend_from_slice(&COMPRESSED_INVERTED_MAGIC.to_le_bytes());
    out.extend_from_slice(&COMPRESSED_INVERTED_VERSION.to_le_bytes());
    out.extend_from_slice(&(uncompressed_len as u64).to_le_bytes());
    out.extend_from_slice(compressed);
    out
}

fn read_inverted_payload(data: Vec<u8>) -> Result<Vec<u8>, KoshaError> {
    if data.len() < COMPRESSED_INVERTED_HEADER_LEN
        || data[0..4] != COMPRESSED_INVERTED_MAGIC.to_le_bytes()
    {
        return Ok(data);
    }

    let corrupt = |msg: &str| KoshaError::CorruptSegment(format!("compressed inverted.idx: {msg}"));
    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    if version != COMPRESSED_INVERTED_VERSION {
        return Err(corrupt(&format!("unsupported version {version}")));
    }
    let uncompressed_len = u64::from_le_bytes(data[8..16].try_into().unwrap());
    let max_len: usize = uncompressed_len
        .try_into()
        .map_err(|_| corrupt("uncompressed length overflows usize"))?;
    let decoded = zstd::bulk::decompress(&data[COMPRESSED_INVERTED_HEADER_LEN..], max_len)
        .map_err(|e| corrupt(&format!("decompress failed: {e}")))?;
    if decoded.len() != max_len {
        return Err(corrupt("decompressed length mismatch"));
    }
    Ok(decoded)
}

/// Per-segment byte budget for [`PostingsCache`]
/// (`KOSHA_POSTINGS_CACHE_MAX_BYTES`, default 4 MiB, `0` disables). Read
/// once — segments are opened frequently and this must not cost an env
/// lookup per open.
fn postings_cache_max_bytes() -> usize {
    static BUDGET: OnceLock<usize> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("KOSHA_POSTINGS_CACHE_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4 * 1024 * 1024)
    })
}

/// Process-wide byte budget for decoded *vector* postings
/// (`KOSHA_VECTOR_POSTINGS_CACHE_MAX_BYTES`, default 512 MiB, `0` disables).
///
/// One budget for every open segment, not one per segment: a kNN query's
/// working set is hundreds of whole postings of `dim`-wide vectors (a
/// compacted segment's postings decode to over a GB), so a per-segment cap
/// large enough to help multiplied by the number of open vector segments
/// had no bound at all. [`shared_vector_cache`] holds every segment's
/// entries in one LRU under this budget, so the hottest postings across all
/// segments stay and resident decoded vector postings never exceed it.
/// Entries also report to the segment's [`PostingsMemoryAccount`], so they
/// count toward the query pod's live-bytes watermark.
fn vector_postings_cache_max_bytes() -> usize {
    static BUDGET: OnceLock<usize> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("KOSHA_VECTOR_POSTINGS_CACHE_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(512 * 1024 * 1024)
    })
}

/// Per-segment byte budget for the decoded late-interaction (`.ivf`)
/// postings LRU (`KOSHA_LATE_INTERACTION_POSTINGS_CACHE_MAX_BYTES`, default
/// 32 MiB, `0` disables) — #205. Separate from the single-vector budget so
/// a token-index working set (query tokens x `nprobe` postings per query)
/// does not evict the single-vector one. Entries report to the same
/// live-bytes account as every other postings cache.
fn late_interaction_postings_cache_max_bytes() -> usize {
    static BUDGET: OnceLock<usize> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("KOSHA_LATE_INTERACTION_POSTINGS_CACHE_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(32 * 1024 * 1024)
    })
}

/// Optional live-memory account the segment's per-segment postings LRU
/// reports its resident bytes to (Tier 1 O6). The decoded form of postings
/// lives outside the on-disk format (the per-query `PostingsCache`), so
/// `MemoryLedger`'s `approx_segment_bytes` never saw it — and a hot-term or
/// wildcard workload can grow aggregate decoded bytes across many open
/// segments without any admission signal. Wiring insert/evict deltas in
/// here closes that gap: aggregate postings memory now counts toward the
/// query pod's live-bytes watermark.
///
/// Methods are `u64` deltas (`add` for a positive insertion, `release` for
/// a negative eviction/drop); the implementor is expected to fold them into
/// its own accounting atomically. `Send + Sync` so an `Arc<dyn …>` can
/// thread through `InvertedAccess` and into rayon-scored segments.
pub trait PostingsMemoryAccount: Send + Sync {
    fn add_postings(&self, bytes: u64);
    fn release_postings(&self, bytes: u64);
}

type SharedAccount = Arc<dyn PostingsMemoryAccount + Send + Sync>;

/// Small LRU of decoded postings.
///
/// The lazy v2 format trades resident memory for a per-query decode — the
/// right trade for the vocabulary at large, but a hot query term ("the")
/// then re-decodes its (large) postings on every query, and a wildcard
/// expanding to thousands of terms re-decodes a big slice of the index per
/// query. Caching the *decoded form of recently-queried terms only* keeps
/// the v1 warm-path amortization for exactly the hot subset, while the
/// cold/open/resident wins of the lazy format stand: the text budget is a
/// few MiB per segment (see [`postings_cache_max_bytes`]), not the whole
/// vocabulary. Text caches are per segment, keyed by term-table index; the
/// vector cache is one per process (see [`shared_vector_cache`]).
///
/// Entries are `Arc`s, so a hit is a pointer clone and eviction never
/// invalidates postings an in-flight query still holds. Approximate entry
/// cost: `count × size_of::<Posting>()` + 4 bytes per position.
///
/// Every operation is O(1) amortized under the one lock: a hit re-stamps
/// the entry and appends to [`StampedFifo`] instead of searching the
/// recency order, so a warm kNN query that hits thousands of postings does
/// not pay a scan of the cache per hit.
///
/// Resident bytes are reported to each entry's [`PostingsMemoryAccount`]
/// (insert adds, evict and drop release), so aggregate decoded postings
/// count toward the query pod's live-bytes watermark.
struct PostingsCache<K, V> {
    max_bytes: usize,
    /// Boxed: a segment embeds three text caches, and inline state would
    /// make `InvertedAccess`'s variants lopsided.
    state: Box<Mutex<PostingsCacheState<K, V>>>,
    /// Account for [`Self::insert`]; the shared vector cache has none and
    /// takes each segment's account per entry ([`Self::insert_for`]).
    account: Option<SharedAccount>,
}

struct CacheEntry<V> {
    value: V,
    bytes: usize,
    /// This entry's latest stamp in [`PostingsCacheState::recency`].
    stamp: u64,
    account: Option<SharedAccount>,
}

struct PostingsCacheState<K, V> {
    entries: HashMap<K, CacheEntry<V>>,
    recency: StampedFifo<K>,
    total_bytes: usize,
    /// Keys missed recently and not yet admitted, with their stamp in
    /// `miss_order` — the second-touch history ("ghost list"), see
    /// [`PostingsCache::admit_on_miss_within`].
    misses: HashMap<K, u64>,
    miss_order: StampedFifo<K>,
}

/// Keys in the order they were last stamped. Re-stamping a key appends it
/// again instead of moving it, and the older position is skipped when it
/// reaches the front (its stamp no longer matches the owner's), so a touch
/// is O(1) and the order is exact LRU. Stale positions are dropped in one
/// pass once they outnumber the live ones, which keeps the queue under
/// `2 × live + 64` and the compaction cost amortized O(1) per touch.
struct StampedFifo<K> {
    queue: VecDeque<(K, u64)>,
    next: u64,
}

impl<K: Copy> StampedFifo<K> {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            next: 0,
        }
    }

    /// Append `key` as the most recent; returns its new stamp.
    fn stamp(&mut self, key: K) -> u64 {
        let stamp = self.next;
        self.next += 1;
        self.queue.push_back((key, stamp));
        stamp
    }

    /// Remove and return the least recently stamped key that is still
    /// current (`current(key)` is its latest stamp, `None` once gone).
    fn pop_oldest(&mut self, current: impl Fn(&K) -> Option<u64>) -> Option<K> {
        while let Some((key, stamp)) = self.queue.pop_front() {
            if current(&key) == Some(stamp) {
                return Some(key);
            }
        }
        None
    }

    /// Drop stale positions once they outnumber the `live` current ones.
    fn compact(&mut self, live: usize, current: impl Fn(&K) -> Option<u64>) {
        if self.queue.len() > 2 * live + 64 {
            self.queue
                .retain(|(key, stamp)| current(key) == Some(*stamp));
        }
    }
}

/// Capacity of the text caches' second-touch history. Big enough to
/// remember a realistic hot working set between queries; small enough that
/// a wildcard blast writing thousands of one-shot entries through it costs
/// nothing.
const POSTINGS_CACHE_MISS_RING: usize = 256;

/// Upper bound on the vector cache's second-touch history (see
/// [`PostingsCache::vector_miss_window`]): 2^18 keys, ~20 MiB of map and
/// queue at the worst case of tiny postings and a large budget.
const VECTOR_MISS_WINDOW_MAX: usize = 1 << 18;

impl<K: Copy + Eq + std::hash::Hash, V: Clone> PostingsCache<K, V> {
    fn new(max_bytes: usize, account: Option<SharedAccount>) -> Self {
        Self {
            max_bytes,
            state: Box::new(Mutex::new(PostingsCacheState {
                entries: HashMap::new(),
                recency: StampedFifo::new(),
                total_bytes: 0,
                misses: HashMap::new(),
                miss_order: StampedFifo::new(),
            })),
            account,
        }
    }

    /// The cached value for `key`, marking it most recently used.
    fn get(&self, key: K) -> Option<V> {
        if self.max_bytes == 0 {
            return None;
        }
        let mut guard = self.state.lock().unwrap();
        let st = &mut *guard;
        let entry = st.entries.get_mut(&key)?;
        entry.stamp = st.recency.stamp(key);
        let hit = entry.value.clone();
        let entries = &st.entries;
        st.recency
            .compact(entries.len(), |k| entries.get(k).map(|e| e.stamp));
        Some(hit)
    }

    /// Whether `key` is cached, without touching its recency. Keeps
    /// nothing alive: a caller that skips reading `key` elsewhere because
    /// it is cached must pin it with [`Self::get`] instead (finding 31).
    #[cfg_attr(not(test), allow(dead_code))]
    fn contains(&self, key: K) -> bool {
        self.max_bytes != 0 && self.state.lock().unwrap().entries.contains_key(&key)
    }

    /// Second-touch admission for the text caches: see
    /// [`Self::admit_on_miss_within`], with a history of
    /// [`POSTINGS_CACHE_MISS_RING`] keys.
    fn admit_on_miss(&self, key: K) -> bool {
        self.admit_on_miss_within(key, POSTINGS_CACHE_MISS_RING)
    }

    /// Second-touch admission, recorded on a cache miss: returns whether
    /// `key` was *already* missed within the last `window` distinct missed
    /// keys (→ the caller should cache the decode), and remembers the miss
    /// either way. O(1) amortized.
    ///
    /// Why not cache every decode: a wildcard query expanding to thousands
    /// of terms, or a kNN probe of a namespace scanned once, would write
    /// its entire one-shot working set through the cache each time —
    /// evicting the genuinely hot entries and paying insert/evict churn
    /// for entries that are never reused. Requiring a second miss within
    /// the window filters exactly that traffic: recurring keys get
    /// admitted on their second lookup; one-shot ones age out of the
    /// history without disturbing anything.
    fn admit_on_miss_within(&self, key: K, window: usize) -> bool {
        let mut guard = self.state.lock().unwrap();
        let st = &mut *guard;
        if st.misses.remove(&key).is_some() {
            return true;
        }
        let stamp = st.miss_order.stamp(key);
        st.misses.insert(key, stamp);
        while st.misses.len() > window {
            let misses = &st.misses;
            match st.miss_order.pop_oldest(|k| misses.get(k).copied()) {
                Some(oldest) => {
                    st.misses.remove(&oldest);
                }
                None => break,
            }
        }
        let misses = &st.misses;
        st.miss_order
            .compact(misses.len(), |k| misses.get(k).copied());
        false
    }

    /// The vector cache's second-touch window: as many keys as the budget
    /// holds entries of the current mean size (`estimate` bytes while the
    /// cache is empty), between [`POSTINGS_CACHE_MISS_RING`] and
    /// [`VECTOR_MISS_WINDOW_MAX`].
    ///
    /// This is a 2Q-style ghost list sized to the cache's own reach. A
    /// posting missed again before that many other postings were missed
    /// would also have stayed resident had it been cached, so it is
    /// admitted — however many postings one query probes, and however
    /// many segments share the cache. A posting that recurs only after
    /// more misses than the cache can hold would be evicted before its
    /// reuse anyway (one query probing more than the budget holds, run
    /// again: the LRU loop), so it is not admitted and does not churn.
    /// A fixed 256-key window admitted nothing once one query probed more
    /// than 256 postings (1,610 per segment on staging), and every warm
    /// query re-read and re-decoded ~700 MB.
    fn vector_miss_window(&self, estimate: usize) -> usize {
        let mean = {
            let st = self.state.lock().unwrap();
            if st.entries.is_empty() {
                estimate
            } else {
                st.total_bytes / st.entries.len()
            }
        };
        (self.max_bytes / mean.max(1)).clamp(POSTINGS_CACHE_MISS_RING, VECTOR_MISS_WINDOW_MAX)
    }

    /// Whether [`Self::insert`] would keep an entry of `bytes` at all.
    fn accepts(&self, bytes: usize) -> bool {
        self.max_bytes != 0 && bytes <= self.max_bytes
    }

    fn insert(&self, key: K, value: V, bytes: usize) {
        self.insert_for(key, value, bytes, self.account.as_ref());
    }

    /// [`Self::insert`] charging `account` for the entry's bytes.
    fn insert_for(&self, key: K, value: V, bytes: usize, account: Option<&SharedAccount>) {
        // An entry bigger than the whole budget would evict everything and
        // then itself churn on every query — don't cache it at all.
        if !self.accepts(bytes) {
            return;
        }
        // Capture what changed in resident bytes while holding the cache
        // lock, then report add/release deltas to the live-bytes account
        // *after* releasing it — the account (kosha-query's `MemoryLedger`)
        // takes its own lock, and the established lock-order invariant is
        // "cache locks may be held while taking the ledger lock, never the
        // reverse." Reporting outside the cache lock keeps that direction
        // clean and avoids any reentrancy if `add_postings` ever touches
        // the same cache. Evicted entries are freed out there too: one
        // insert can evict many postings, and every kNN worker of every
        // segment waits on this lock.
        let mut gone: Vec<CacheEntry<V>> = Vec::new();
        {
            let mut guard = self.state.lock().unwrap();
            let st = &mut *guard;
            let stamp = st.recency.stamp(key);
            let entry = CacheEntry {
                value,
                bytes,
                stamp,
                account: account.cloned(),
            };
            if let Some(old) = st.entries.insert(key, entry) {
                // Replacing an entry: release its bytes, add the new ones.
                st.total_bytes -= old.bytes;
                gone.push(old);
            }
            st.total_bytes += bytes;
            while st.total_bytes > self.max_bytes {
                let entries = &st.entries;
                let Some(oldest) = st.recency.pop_oldest(|k| entries.get(k).map(|e| e.stamp))
                else {
                    break;
                };
                if let Some(evicted) = st.entries.remove(&oldest) {
                    st.total_bytes -= evicted.bytes;
                    gone.push(evicted);
                }
            }
            let entries = &st.entries;
            st.recency
                .compact(entries.len(), |k| entries.get(k).map(|e| e.stamp));
        }
        release_and_free(gone);
        if let Some(account) = account {
            account.add_postings(bytes as u64);
        }
    }

    /// Remove every entry whose key `doomed` selects, releasing its bytes
    /// to its account. O(entries) under the lock; the entries are freed
    /// after it is released (a segment's postings can be thousands of
    /// allocations, and this runs from `SegmentReader::drop`). Remembered misses are left to age out
    /// of their bounded window: purging them would cost a pass over up to
    /// [`VECTOR_MISS_WINDOW_MAX`] keys under the lock every kNN shares.
    fn remove_where(&self, doomed: impl Fn(&K) -> bool) {
        let removed = {
            // Reachable from `Drop`: a poisoned lock must not panic there.
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let st = &mut *guard;
            let removed: Vec<CacheEntry<V>> = st
                .entries
                .extract_if(|key, _| doomed(key))
                .map(|(_, entry)| entry)
                .collect();
            st.total_bytes -= removed.iter().map(|entry| entry.bytes).sum::<usize>();
            let entries = &st.entries;
            st.recency
                .compact(entries.len(), |k| entries.get(k).map(|e| e.stamp));
            removed
        };
        release_and_free(removed);
    }

    /// Bytes held by the entries `selected` picks. O(entries).
    fn resident_bytes_where(&self, selected: impl Fn(&K) -> bool) -> usize {
        let st = self.state.lock().unwrap();
        st.entries
            .iter()
            .filter(|(key, _)| selected(key))
            .map(|(_, entry)| entry.bytes)
            .sum()
    }
}

/// Free entries taken out of a cache, then release their bytes with one
/// call per distinct account (dropping thousands of entries notifies the
/// ledger once). Call with the cache lock released.
fn release_and_free<V>(gone: Vec<CacheEntry<V>>) {
    let mut totals: Vec<(SharedAccount, u64)> = Vec::new();
    for entry in gone {
        let Some(account) = entry.account else {
            continue;
        };
        match totals.iter_mut().find(|(a, _)| Arc::ptr_eq(a, &account)) {
            Some((_, total)) => *total += entry.bytes as u64,
            None => totals.push((account, entry.bytes as u64)),
        }
    }
    for (account, total) in totals {
        if total > 0 {
            account.release_postings(total);
        }
    }
}

impl<K, V> Drop for PostingsCache<K, V> {
    fn drop(&mut self) {
        // Release whatever this cache still holds to the live-bytes
        // accounts, so `MemoryLedger::live` stays truthful as the owning
        // `SegmentReader` (and its caches) go away — see the trait doc.
        // Taken under the lock, reported outside it (same lock-order
        // discipline as `insert_for`).
        let gone: Vec<CacheEntry<V>> = {
            let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            st.entries.drain().map(|(_, entry)| entry).collect()
        };
        release_and_free(gone);
    }
}

/// Postings handle returned by [`SegmentReader::postings`]. Derefs to
/// `[Posting]`, so call sites treat it as a slice regardless of the
/// underlying storage: legacy (v1) segments lend a borrow into their parsed
/// map; v2 segments hand out a shared `Arc` of the on-demand decode (a
/// pointer clone on a [`PostingsCache`] hit).
pub enum PostingsRef<'a> {
    Borrowed(&'a [Posting]),
    Shared(Arc<Vec<Posting>>),
}

/// Scoring-side postings handle returned by
/// [`SegmentReader::scoring_postings_for_terms`]: v5 segments hand out the
/// block-lazy skip-table view; older formats fall back to the classic full
/// decode (shared with the phrase path and the postings cache), which the
/// caller walks exactly as before.
pub enum ScoringPostingsRef<'a> {
    Blocked(Arc<BlockedTermPostings>),
    Decoded(PostingsRef<'a>),
}

impl ScoringPostingsRef<'_> {
    /// The term's document frequency in this segment (number of postings,
    /// deleted documents included — the same `df` BM25 scores with).
    pub fn doc_count(&self) -> usize {
        match self {
            Self::Blocked(b) => b.doc_count(),
            Self::Decoded(p) => p.len(),
        }
    }
}

impl std::ops::Deref for PostingsRef<'_> {
    type Target = [Posting];
    fn deref(&self) -> &[Posting] {
        match self {
            Self::Borrowed(s) => s,
            Self::Shared(a) => a.as_slice(),
        }
    }
}

impl PostingsRef<'_> {
    /// Owned copy of the postings — clones unless this is the sole
    /// reference to a shared decode.
    pub fn into_owned(self) -> Vec<Posting> {
        match self {
            Self::Borrowed(s) => s.to_vec(),
            Self::Shared(a) => Arc::try_unwrap(a).unwrap_or_else(|a| (*a).clone()),
        }
    }
}

impl LazyInvertedIndex {
    /// Does this buffer carry the v2 magic? (`false` → legacy v1 stream.)
    fn detect(data: &[u8]) -> bool {
        data.len() >= INVERTED_HEADER_LEN && data[0..4] == INVERTED_MAGIC.to_le_bytes()
    }

    /// Validate the header and the full term table (span bounds and term
    /// UTF-8) once, so every later accessor can trust the table. One cheap
    /// pass over fixed-width entries — no postings are touched.
    fn from_bytes(
        data: Vec<u8>,
        account: Option<Arc<dyn PostingsMemoryAccount + Send + Sync>>,
    ) -> Result<Self, KoshaError> {
        let corrupt = |msg: &str| KoshaError::CorruptSegment(format!("inverted.idx: {msg}"));
        if !Self::detect(&data) {
            return Err(corrupt("missing v2 magic"));
        }
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        if version != INLINE_INVERTED_VERSION {
            return Err(corrupt(&format!("unsupported version {version}")));
        }
        let term_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
        let table_end = INVERTED_HEADER_LEN
            .checked_add(
                term_count
                    .checked_mul(INVERTED_TABLE_ENTRY_LEN)
                    .ok_or_else(|| corrupt("term table length overflows"))?,
            )
            .ok_or_else(|| corrupt("term table length overflows"))?;
        if table_end > data.len() {
            return Err(corrupt("term table extends past end of file"));
        }
        let index = Self {
            data,
            term_count,
            cache: PostingsCache::new(postings_cache_max_bytes(), account),
        };
        for i in 0..term_count {
            let (term_off, term_len, p_off, p_len) = index.raw_entry(i);
            let term_end = term_off
                .checked_add(term_len)
                .filter(|&e| e <= index.data.len());
            let postings_end = p_off.checked_add(p_len).filter(|&e| e <= index.data.len());
            if term_end.is_none() || postings_end.is_none() {
                return Err(corrupt(&format!("table entry {i} out of bounds")));
            }
            if std::str::from_utf8(&index.data[term_off..term_off + term_len]).is_err() {
                return Err(corrupt(&format!("table entry {i} term is not UTF-8")));
            }
        }
        Ok(index)
    }

    /// Raw table entry `i` as `(term_off, term_len, postings_off,
    /// postings_len)` in file-absolute byte offsets. Caller must have
    /// `i < term_count`; spans are validated by `from_bytes`.
    fn raw_entry(&self, i: usize) -> (usize, usize, usize, usize) {
        let base = INVERTED_HEADER_LEN + i * INVERTED_TABLE_ENTRY_LEN;
        let e = &self.data[base..base + INVERTED_TABLE_ENTRY_LEN];
        (
            u64::from_le_bytes(e[0..8].try_into().unwrap()) as usize,
            u32::from_le_bytes(e[8..12].try_into().unwrap()) as usize,
            u64::from_le_bytes(e[12..20].try_into().unwrap()) as usize,
            u32::from_le_bytes(e[20..24].try_into().unwrap()) as usize,
        )
    }

    /// Term `i`'s string, zero-copy from the pool. UTF-8 validated at open.
    fn term_at(&self, i: usize) -> &str {
        let (off, len, _, _) = self.raw_entry(i);
        std::str::from_utf8(&self.data[off..off + len]).unwrap_or_default()
    }

    /// Binary search over the (sorted) term table.
    fn find(&self, term: &str) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = self.term_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.term_at(mid).cmp(term) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    fn contains_term(&self, term: &str) -> bool {
        self.find(term).is_some()
    }

    /// All terms in sorted order (the table's physical order), zero-copy.
    fn all_terms(&self) -> Vec<&str> {
        (0..self.term_count).map(|i| self.term_at(i)).collect()
    }

    /// Postings for one term: a shared handle to the decoded form, served
    /// from the per-segment LRU when the term was queried recently (a
    /// pointer clone), decoded on demand otherwise. `None` for absent
    /// terms.
    fn postings(&self, term: &str) -> Option<Arc<Vec<Posting>>> {
        let i = self.find(term)?;
        if let Some(hit) = self.cache.get(i) {
            return Some(hit);
        }
        let admit = self.cache.admit_on_miss(i);
        let (postings, approx_bytes) = self.decode_postings(i)?;
        let arc = Arc::new(postings);
        if admit {
            self.cache.insert(i, Arc::clone(&arc), approx_bytes);
        }
        Some(arc)
    }

    /// Decode term `i`'s postings, returning them with their approximate
    /// in-memory cost (for the cache's byte budget). Decoding is fully
    /// bounds-checked: a corrupt postings region (which open-time
    /// validation deliberately doesn't scan — that would be the eager parse
    /// this format exists to avoid) yields `None` rather than a panic in a
    /// scoring thread.
    fn decode_postings(&self, i: usize) -> Option<(Vec<Posting>, usize)> {
        let (_, _, p_off, p_len) = self.raw_entry(i);
        decode_postings_bytes(&self.data[p_off..p_off + p_len])
    }
}

fn decode_postings_bytes(mut buf: &[u8]) -> Option<(Vec<Posting>, usize)> {
    let count = take_u32(&mut buf)? as usize;
    // Guard Vec::with_capacity against a corrupt count: a posting is at
    // least 12 bytes, so cap by what the span could physically hold.
    let mut postings = Vec::with_capacity(count.min(buf.len() / 12 + 1));
    let mut position_count = 0usize;
    for _ in 0..count {
        let doc_id = take_u32(&mut buf)?;
        let term_frequency = take_u32(&mut buf)?;
        let npos = take_u32(&mut buf)? as usize;
        let mut positions = Vec::with_capacity(npos.min(buf.len() / 4 + 1));
        for _ in 0..npos {
            positions.push(take_u32(&mut buf)?);
        }
        position_count += positions.len();
        postings.push(Posting {
            doc_id,
            term_frequency,
            positions,
        });
    }
    let approx_bytes = postings.len() * std::mem::size_of::<Posting>() + position_count * 4;
    Some((postings, approx_bytes))
}

/// Cache-miss postings reads pending against one blob: (entry index, term,
/// offset within the blob, span length, admit-to-cache).
/// `(term_table_index, output_slot, term, postings_off, postings_len, admit)`
/// for terms whose postings must be decoded from a blob shard —
/// `output_slot` carries the term's position in the caller's requested
/// order so the batched decode can emit deterministically.
type PendingBlobReads<'a> = Vec<(usize, usize, &'a str, usize, usize, bool)>;

/// v4 span codec, retained for fixtures/tests and as the byte-identity
/// reference for [`TermEncoder`]'s v4 mode (`KOSHA_POSTINGS_WRITE_VERSION=4`).
#[cfg_attr(not(test), allow(dead_code))]
fn write_postings_varint_delta(out: &mut Vec<u8>, postings: &[Posting]) {
    put_var_u32(out, postings.len() as u32);
    let mut prev_doc_id = 0u32;
    for posting in postings {
        put_var_u32(out, posting.doc_id.wrapping_sub(prev_doc_id));
        prev_doc_id = posting.doc_id;
        put_var_u32(out, posting.term_frequency);
        put_var_u32(out, posting.positions.len() as u32);
        let mut prev_pos = 0u32;
        for &pos in &posting.positions {
            put_var_u32(out, pos.wrapping_sub(prev_pos));
            prev_pos = pos;
        }
    }
}

fn decode_postings_varint_delta_bytes(mut buf: &[u8]) -> Option<(Vec<Posting>, usize)> {
    let count = take_var_u32(&mut buf)? as usize;
    let mut postings = Vec::with_capacity(count);
    let mut position_count = 0usize;
    let mut prev_doc_id = 0u32;
    for _ in 0..count {
        let doc_delta = take_var_u32(&mut buf)?;
        let doc_id = prev_doc_id.wrapping_add(doc_delta);
        prev_doc_id = doc_id;
        let term_frequency = take_var_u32(&mut buf)?;
        let npos = take_var_u32(&mut buf)? as usize;
        let mut positions = Vec::with_capacity(npos);
        let mut prev_pos = 0u32;
        for _ in 0..npos {
            let pos_delta = take_var_u32(&mut buf)?;
            let pos = prev_pos.wrapping_add(pos_delta);
            prev_pos = pos;
            positions.push(pos);
        }
        position_count += positions.len();
        postings.push(Posting {
            doc_id,
            term_frequency,
            positions,
        });
    }
    let approx_bytes = postings.len() * std::mem::size_of::<Posting>() + position_count * 4;
    Some((postings, approx_bytes))
}

/// Posting-block granularity of the v5 skip-split encoding. One skip entry
/// summarizes each block; scoring decode happens a block at a time.
pub const SCORING_BLOCK_LEN: usize = 128;

/// The scoring-side view of one posting: everything BM25/WAND needs, and
/// nothing else. `Copy` and 8 bytes flat — a decoded block is a small,
/// cache-friendly array instead of `Vec<Posting>`'s per-posting heap
/// positions vector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScoringPosting {
    pub doc_id: u32,
    pub term_frequency: u32,
}

/// One skip-table entry: the write-time summary of a 128-posting block.
/// `max_tf`/`min_field_length` feed the BM25 block upper bound without
/// touching posting bytes or the doc store; `last_doc_id` drives the
/// leapfrog gallop; the byte range delimits the block's scoring bytes so a
/// block decodes standalone (delta base = previous entry's `last_doc_id`).
#[derive(Clone, Copy, Debug)]
pub struct ScoringBlockSummary {
    pub last_doc_id: u32,
    pub max_tf: u32,
    pub min_field_length: u32,
    scoring_start: u32,
    scoring_end: u32,
}

/// Serialize one term's postings in the v5 skip-split span layout:
///
/// ```text
/// n_postings: varint
/// per block (ceil(n/128)): last_doc_id delta from previous block's last
///                          scoring byte length of the block
///                          max_tf
///                          min_field_length          (all varints)
/// scoring bytes: per posting, doc_id delta from previous posting + tf
///                (the delta chain runs across blocks; the skip table
///                provides each block's decode base)
/// positions bytes: per posting (same ordinal order), n_positions +
///                  position deltas — read only by the full-postings path
/// ```
///
/// The production writer encodes the same bytes incrementally with
/// [`TermEncoder`] (#242); this whole-list version is the reference the
/// byte-identity tests compare against.
#[cfg_attr(not(test), allow(dead_code))]
fn write_postings_skip_split(out: &mut Vec<u8>, postings: &[Posting], field_lengths: &[u32]) {
    write_postings_skip_split_inner(out, postings, field_lengths, false);
}

/// v6 ([`BITPACKED_SKIP_SPLIT_INVERTED_VERSION`]) reference writer: the v5
/// span with every full 128-posting scoring block bit-packed (see
/// [`bitpack`]); the byte-identity reference for [`TermEncoder`]'s v6 mode.
#[cfg_attr(not(test), allow(dead_code))]
fn write_postings_skip_split_v6(out: &mut Vec<u8>, postings: &[Posting], field_lengths: &[u32]) {
    write_postings_skip_split_inner(out, postings, field_lengths, true);
}

#[cfg_attr(not(test), allow(dead_code))]
fn write_postings_skip_split_inner(
    out: &mut Vec<u8>,
    postings: &[Posting],
    field_lengths: &[u32],
    bitpacked: bool,
) {
    put_var_u32(out, postings.len() as u32);

    let mut scoring: Vec<u8> = Vec::new();
    let mut prev_doc = 0u32;
    let mut prev_last = 0u32;
    for (b, block) in postings.chunks(SCORING_BLOCK_LEN).enumerate() {
        let start = scoring.len();
        let mut max_tf = 0u32;
        let mut min_fl = u32::MAX;
        let pack = bitpacked && block.len() == SCORING_BLOCK_LEN;
        if pack {
            let docs: [u32; SCORING_BLOCK_LEN] = std::array::from_fn(|i| block[i].doc_id);
            let tfs: [u32; SCORING_BLOCK_LEN] = std::array::from_fn(|i| block[i].term_frequency);
            let next_min = if b == 0 { 0 } else { prev_doc.wrapping_add(1) };
            bitpack::encode_bp_block(&docs, &tfs, next_min, &mut scoring);
            prev_doc = block[SCORING_BLOCK_LEN - 1].doc_id;
        }
        for posting in block {
            if !pack {
                put_var_u32(&mut scoring, posting.doc_id.wrapping_sub(prev_doc));
                prev_doc = posting.doc_id;
                put_var_u32(&mut scoring, posting.term_frequency);
            }
            max_tf = max_tf.max(posting.term_frequency);
            if let Some(&field_length) = field_lengths.get(posting.doc_id as usize) {
                min_fl = min_fl.min(field_length);
            }
        }
        let last = block.last().expect("chunks yields non-empty blocks").doc_id;
        put_var_u32(out, last.wrapping_sub(prev_last));
        prev_last = last;
        put_var_u32(out, (scoring.len() - start) as u32);
        put_var_u32(out, max_tf);
        // An unresolvable field length can only mean a doc_seq outside the
        // writer's own records (impossible in practice); 0 keeps the upper
        // bound conservative-valid rather than poisoning it.
        put_var_u32(out, if min_fl == u32::MAX { 0 } else { min_fl });
    }
    out.extend_from_slice(&scoring);

    for posting in postings {
        put_var_u32(out, posting.positions.len() as u32);
        let mut prev_pos = 0u32;
        for &pos in &posting.positions {
            put_var_u32(out, pos.wrapping_sub(prev_pos));
            prev_pos = pos;
        }
    }
}

/// Scoring blocks varint-decoded by [`BlockedTermPostings::read_block`]
/// since process start, across every segment and query (#385). Read with
/// [`scoring_blocks_decoded`]; a bench diffs it around a run and divides by
/// the request count to get blocks decoded per query. One relaxed atomic
/// add per decoded block (each decodes up to `SCORING_BLOCK_LEN` postings).
static SCORING_BLOCKS_DECODED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// See [`SCORING_BLOCKS_DECODED`].
pub fn scoring_blocks_decoded() -> u64 {
    SCORING_BLOCKS_DECODED.load(std::sync::atomic::Ordering::Relaxed)
}

/// One term's parsed v5 span: skip table in memory, scoring/position bytes
/// shared, blocks decoded strictly on demand — a block the WAND pruning or
/// the leapfrog gallop steps over is never varint-decoded at all.
pub struct BlockedTermPostings {
    doc_count: u32,
    blocks: Vec<ScoringBlockSummary>,
    /// The whole term span (skip table + scoring + positions).
    span: Arc<Vec<u8>>,
    scoring_base: u32,
    positions_base: u32,
    /// v6: full blocks are bit-packed ([`bitpack`]); partial blocks and
    /// all of v5 are varint pairs.
    bitpacked: bool,
}

impl BlockedTermPostings {
    /// Parse a v5 term span. Validates the skip table against the span
    /// bounds once, so per-block decode can't walk off the buffer.
    fn parse(span: Arc<Vec<u8>>) -> Option<Self> {
        Self::parse_with(span, false)
    }

    /// Parse a v6 ([`BITPACKED_SKIP_SPLIT_INVERTED_VERSION`]) term span —
    /// same skip table as v5.
    fn parse_v6(span: Arc<Vec<u8>>) -> Option<Self> {
        Self::parse_with(span, true)
    }

    fn parse_with(span: Arc<Vec<u8>>, bitpacked: bool) -> Option<Self> {
        let mut buf: &[u8] = &span;
        let doc_count = take_var_u32(&mut buf)?;
        let n_blocks = (doc_count as usize).div_ceil(SCORING_BLOCK_LEN);
        let mut blocks = Vec::with_capacity(n_blocks);
        let mut prev_last = 0u32;
        let mut scoring_off = 0u64;
        for _ in 0..n_blocks {
            let last_delta = take_var_u32(&mut buf)?;
            let len = take_var_u32(&mut buf)?;
            let max_tf = take_var_u32(&mut buf)?;
            let min_field_length = take_var_u32(&mut buf)?;
            let last_doc_id = prev_last.wrapping_add(last_delta);
            prev_last = last_doc_id;
            let scoring_start = u32::try_from(scoring_off).ok()?;
            scoring_off += len as u64;
            blocks.push(ScoringBlockSummary {
                last_doc_id,
                max_tf,
                min_field_length,
                scoring_start,
                scoring_end: u32::try_from(scoring_off).ok()?,
            });
        }
        let scoring_base = span.len() - buf.len();
        let positions_base = scoring_base as u64 + scoring_off;
        if positions_base > span.len() as u64 {
            return None; // scoring section extends past the span
        }
        Some(Self {
            doc_count,
            blocks,
            scoring_base: u32::try_from(scoring_base).ok()?,
            positions_base: u32::try_from(positions_base).ok()?,
            span,
            bitpacked,
        })
    }

    /// The term's document frequency (number of postings).
    pub fn doc_count(&self) -> usize {
        self.doc_count as usize
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub fn summary(&self, block: usize) -> &ScoringBlockSummary {
        &self.blocks[block]
    }

    /// First block at or after `from_block` whose `last_doc_id >= target`
    /// — the only block that can contain `target`. `None` = exhausted.
    pub fn find_block(&self, target: u32, from_block: usize) -> Option<usize> {
        if from_block >= self.blocks.len() {
            return None;
        }
        let rel = self.blocks[from_block..].partition_point(|b| b.last_doc_id < target);
        let idx = from_block + rel;
        (idx < self.blocks.len()).then_some(idx)
    }

    /// Decode block `block`'s `(doc_id, tf)` pairs into `out` (cleared
    /// first). Returns false on corrupt bytes — callers treat that block
    /// as contributing nothing, mirroring the lenient `Option` decode of
    /// the other formats.
    pub fn read_block(&self, block: usize, out: &mut Vec<ScoringPosting>) -> bool {
        out.clear();
        SCORING_BLOCKS_DECODED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let Some(summary) = self.blocks.get(block) else {
            return false;
        };
        let start = self.scoring_base as usize + summary.scoring_start as usize;
        let end = self.scoring_base as usize + summary.scoring_end as usize;
        let Some(buf) = self.span.get(start..end) else {
            return false;
        };
        let in_block = block_posting_count(self.doc_count as usize, block);
        let prev_doc = if block == 0 {
            0
        } else {
            self.blocks[block - 1].last_doc_id
        };
        let decoded = if self.bitpacked && in_block == SCORING_BLOCK_LEN {
            let next_min = if block == 0 {
                0
            } else {
                prev_doc.wrapping_add(1)
            };
            bitpack::decode_bp_block(buf, next_min, out)
        } else {
            bitpack::decode_varint_block(buf, in_block, prev_doc, out)
        };
        let Some(prev_doc) = decoded else {
            out.clear();
            return false;
        };
        // The delta chain must land exactly on the skip entry's
        // last_doc_id. Without this check a flipped bit in any doc-delta
        // varint still "decodes" — every doc_id after it is shifted, BM25
        // resolves doc_meta for the wrong documents, and the page returns
        // wrong results with confident scores and no error anywhere.
        if prev_doc != summary.last_doc_id {
            out.clear();
            return false;
        }
        true
    }

    /// Full decode — scoring and positions sections zipped by ordinal into
    /// the classic `Vec<Posting>`. This is the phrase-query / compat path;
    /// scoring never calls it.
    fn decode_full(&self) -> Option<(Vec<Posting>, usize)> {
        let mut postings: Vec<Posting> = Vec::with_capacity(self.doc_count as usize);
        let mut block_buf: Vec<ScoringPosting> = Vec::with_capacity(SCORING_BLOCK_LEN);
        let mut positions_buf = self.span.get(self.positions_base as usize..)?;
        let mut position_count = 0usize;
        for block in 0..self.blocks.len() {
            if !self.read_block(block, &mut block_buf) {
                return None;
            }
            for sp in &block_buf {
                let npos = take_var_u32(&mut positions_buf)? as usize;
                // Untrusted count: every position takes at least one byte,
                // so a valid `npos` can never exceed the remaining buffer.
                // Without this bound a single corrupt varint (up to
                // u32::MAX) turns into a multi-gigabyte `with_capacity`
                // request that aborts the process instead of failing the
                // decode closed.
                if npos > positions_buf.len() {
                    return None;
                }
                let mut positions = Vec::with_capacity(npos);
                let mut prev_pos = 0u32;
                for _ in 0..npos {
                    let pos = prev_pos.wrapping_add(take_var_u32(&mut positions_buf)?);
                    prev_pos = pos;
                    positions.push(pos);
                }
                position_count += npos;
                postings.push(Posting {
                    doc_id: sp.doc_id,
                    term_frequency: sp.term_frequency,
                    positions,
                });
            }
        }
        let approx_bytes = postings.len() * std::mem::size_of::<Posting>() + position_count * 4;
        Some((postings, approx_bytes))
    }

    /// Resident-byte estimate for the cache: the shared span plus the
    /// parsed skip table.
    fn approx_bytes(&self) -> usize {
        self.span.len() + self.blocks.len() * std::mem::size_of::<ScoringBlockSummary>()
    }
}

/// Number of postings in `block` for a term with `doc_count` postings.
fn block_posting_count(doc_count: usize, block: usize) -> usize {
    let start = block * SCORING_BLOCK_LEN;
    doc_count.saturating_sub(start).min(SCORING_BLOCK_LEN)
}

impl SplitInvertedIndex {
    fn detect(data: &[u8]) -> bool {
        if data.len() < INVERTED_HEADER_LEN || data[0..4] != INVERTED_MAGIC.to_le_bytes() {
            return false;
        }
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        // Same canonical predicate as the hydration gate — see
        // `is_split_inverted_version` for why these must never diverge.
        is_split_inverted_version(version)
    }

    fn from_bytes(
        data: Vec<u8>,
        segment_dir: PathBuf,
        account: Option<Arc<dyn PostingsMemoryAccount + Send + Sync>>,
    ) -> Result<Self, KoshaError> {
        let corrupt = |msg: &str| KoshaError::CorruptSegment(format!("split inverted.idx: {msg}"));
        if !Self::detect(&data) {
            return Err(corrupt("missing split v3 header"));
        }
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let encoding = match version {
            FIXED_SPLIT_INVERTED_VERSION => SplitPostingsEncoding::Fixed32,
            SPLIT_INVERTED_VERSION => SplitPostingsEncoding::VarintDelta,
            SKIP_SPLIT_INVERTED_VERSION => SplitPostingsEncoding::SkipSplit,
            BITPACKED_SKIP_SPLIT_INVERTED_VERSION => SplitPostingsEncoding::BitPackedSkipSplit,
            _ => return Err(corrupt(&format!("unsupported version {version}"))),
        };
        let term_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
        let shard_count = u32::from_le_bytes(data[12..16].try_into().unwrap()) as usize;
        if shard_count != POSTING_BLOB_SHARDS {
            return Err(corrupt(&format!("unsupported shard count {shard_count}")));
        }
        let table_end = INVERTED_HEADER_LEN
            .checked_add(
                term_count
                    .checked_mul(SPLIT_INVERTED_TABLE_ENTRY_LEN)
                    .ok_or_else(|| corrupt("term table length overflows"))?,
            )
            .ok_or_else(|| corrupt("term table length overflows"))?;
        if table_end > data.len() {
            return Err(corrupt("term table extends past end of file"));
        }
        let index = Self {
            data,
            segment_dir,
            term_count,
            encoding,
            cache: PostingsCache::new(postings_cache_max_bytes(), account.clone()),
            scoring_cache: PostingsCache::new(postings_cache_max_bytes(), account),
            blob_read_failure: std::sync::atomic::AtomicBool::new(false),
            posting_spans: false,
        };
        for i in 0..term_count {
            let (term_off, term_len, blob_id, _, _) = index.raw_entry(i);
            if blob_id >= POSTING_BLOB_SHARDS {
                return Err(corrupt(&format!("table entry {i} has invalid blob id")));
            }
            let Some(term_end) = term_off
                .checked_add(term_len)
                .filter(|&e| e <= index.data.len())
            else {
                return Err(corrupt(&format!("table entry {i} term out of bounds")));
            };
            if std::str::from_utf8(&index.data[term_off..term_end]).is_err() {
                return Err(corrupt(&format!("table entry {i} term is not UTF-8")));
            }
        }
        Ok(index)
    }

    fn raw_entry(&self, i: usize) -> (usize, usize, usize, usize, usize) {
        let base = INVERTED_HEADER_LEN + i * SPLIT_INVERTED_TABLE_ENTRY_LEN;
        let e = &self.data[base..base + SPLIT_INVERTED_TABLE_ENTRY_LEN];
        (
            u64::from_le_bytes(e[0..8].try_into().unwrap()) as usize,
            u32::from_le_bytes(e[8..12].try_into().unwrap()) as usize,
            u32::from_le_bytes(e[12..16].try_into().unwrap()) as usize,
            u64::from_le_bytes(e[16..24].try_into().unwrap()) as usize,
            u32::from_le_bytes(e[24..28].try_into().unwrap()) as usize,
        )
    }

    fn term_at(&self, i: usize) -> &str {
        let (off, len, _, _, _) = self.raw_entry(i);
        std::str::from_utf8(&self.data[off..off + len]).unwrap_or_default()
    }

    fn find(&self, term: &str) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = self.term_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.term_at(mid).cmp(term) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    fn contains_term(&self, term: &str) -> bool {
        self.find(term).is_some()
    }

    fn all_terms(&self) -> Vec<&str> {
        (0..self.term_count).map(|i| self.term_at(i)).collect()
    }

    fn postings(&self, term: &str) -> Option<Arc<Vec<Posting>>> {
        let i = self.find(term)?;
        if let Some(hit) = self.cache.get(i) {
            return Some(hit);
        }
        let admit = self.cache.admit_on_miss(i);
        let (postings, approx_bytes) = self.decode_postings(i)?;
        let arc = Arc::new(postings);
        if admit {
            self.cache.insert(i, Arc::clone(&arc), approx_bytes);
        }
        Some(arc)
    }

    fn postings_for_terms<'a>(&'a self, terms: &'a [String]) -> Vec<(&'a str, Arc<Vec<Posting>>)> {
        // The output is emitted strictly in the requested `terms` order.
        // That order is load-bearing: both the general scoring path and the
        // WAND join sum per-term float scores in whatever order this
        // returns, and callers document identical page order across paths —
        // when cache hits came first and blob-miss groups followed in
        // HashMap iteration order, two runs of the same query could sum in
        // different orders and swap docs tied at a page boundary.
        type Slot<'a> = Option<(&'a str, Arc<Vec<Posting>>)>;
        let mut slots: Vec<Slot<'a>> = Vec::new();
        slots.resize_with(terms.len(), || None);
        let mut by_blob: HashMap<usize, PendingBlobReads<'a>> = HashMap::new();

        for (slot, term) in terms.iter().enumerate() {
            let Some(i) = self.find(term) else {
                continue;
            };
            if let Some(hit) = self.cache.get(i) {
                slots[slot] = Some((term.as_str(), hit));
                continue;
            }
            let admit = self.cache.admit_on_miss(i);
            let (_, _, blob_id, p_off, p_len) = self.raw_entry(i);
            by_blob
                .entry(blob_id)
                .or_default()
                .push((i, slot, term.as_str(), p_off, p_len, admit));
        }

        for (blob_id, spans) in by_blob {
            let Some(blob) = self.open_posting_blob(blob_id) else {
                // Every term queued on this shard was TOC-present; the
                // shard itself is unreadable (evicted/corrupt).
                self.note_blob_read_failure();
                continue;
            };
            for (i, slot, term, p_off, p_len, admit) in spans {
                let Some(bytes) = self.term_bytes(&blob, blob_id, p_off, p_len) else {
                    self.note_blob_read_failure();
                    continue;
                };
                let Some((postings, approx_bytes)) = self.decode_term(&bytes) else {
                    self.note_blob_read_failure();
                    continue;
                };
                let arc = Arc::new(postings);
                if admit {
                    self.cache.insert(i, Arc::clone(&arc), approx_bytes);
                }
                slots[slot] = Some((term, arc));
            }
        }

        slots.into_iter().flatten().collect()
    }

    fn decode_postings(&self, i: usize) -> Option<(Vec<Posting>, usize)> {
        let decoded = self.decode_postings_inner(i);
        if decoded.is_none() {
            // The TOC contains this term (callers only reach here after
            // `find` succeeded) but its blob shard couldn't serve it —
            // evicted, truncated, or corrupt. Flag it so the query layer
            // can error instead of silently treating the term as absent.
            self.note_blob_read_failure();
        }
        decoded
    }

    fn decode_postings_inner(&self, i: usize) -> Option<(Vec<Posting>, usize)> {
        let (_, _, blob_id, p_off, p_len) = self.raw_entry(i);
        let blob = self.open_posting_blob(blob_id)?;
        let bytes = self.term_bytes(&blob, blob_id, p_off, p_len)?;
        self.decode_term(&bytes)
    }

    /// Full decode of one term's span, in this index's encoding.
    fn decode_term(&self, bytes: &[u8]) -> Option<(Vec<Posting>, usize)> {
        match self.encoding {
            SplitPostingsEncoding::Fixed32 => decode_postings_bytes(bytes),
            SplitPostingsEncoding::VarintDelta => decode_postings_varint_delta_bytes(bytes),
            SplitPostingsEncoding::SkipSplit => {
                BlockedTermPostings::parse(Arc::new(bytes.to_vec())).and_then(|b| b.decode_full())
            }
            SplitPostingsEncoding::BitPackedSkipSplit => {
                BlockedTermPostings::parse_v6(Arc::new(bytes.to_vec()))
                    .and_then(|b| b.decode_full())
            }
        }
    }

    fn note_blob_read_failure(&self) {
        self.blob_read_failure
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Drain the "a TOC-present term's blob shard failed to read" flag —
    /// see the field doc. Cleared on read so one surfaced error doesn't
    /// echo forever.
    fn take_blob_read_failure(&self) -> bool {
        self.blob_read_failure
            .swap(false, std::sync::atomic::Ordering::Relaxed)
    }

    /// Scoring-side batch fetch for the v5 skip-split encoding: per term, a
    /// shared [`BlockedTermPostings`] (skip table + raw bytes) instead of a
    /// fully decoded `Vec<Posting>` — positions are never touched, blocks
    /// decode on demand. Returns `None` when this segment predates v5 so
    /// the caller can fall back to the full decode path.
    fn scoring_postings_for_terms<'a>(
        &'a self,
        terms: &'a [String],
    ) -> Option<Vec<(&'a str, Arc<BlockedTermPostings>)>> {
        let bitpacked = self.encoding.skip_split()?;
        // Slot-assembled in requested-term order — same load-bearing
        // reason as `postings_for_terms`: the WAND leapfrog join sums
        // per-term float scores in whatever order this returns, and a
        // cache-hits-first / HashMap-iteration-order result would let two
        // runs of the same query sum in different orders and swap docs
        // tied at a page boundary.
        type Slot<'a> = Option<(&'a str, Arc<BlockedTermPostings>)>;
        let mut slots: Vec<Slot<'a>> = Vec::new();
        slots.resize_with(terms.len(), || None);
        let mut by_blob: HashMap<usize, PendingBlobReads<'a>> = HashMap::new();

        for (slot, term) in terms.iter().enumerate() {
            let Some(i) = self.find(term) else {
                continue;
            };
            if let Some(hit) = self.scoring_cache.get(i) {
                slots[slot] = Some((term.as_str(), hit));
                continue;
            }
            let admit = self.scoring_cache.admit_on_miss(i);
            let (_, _, blob_id, p_off, p_len) = self.raw_entry(i);
            by_blob
                .entry(blob_id)
                .or_default()
                .push((i, slot, term.as_str(), p_off, p_len, admit));
        }

        for (blob_id, spans) in by_blob {
            let Some(blob) = self.open_posting_blob(blob_id) else {
                // Every term queued on this shard was TOC-present; the
                // shard itself is unreadable (evicted/corrupt).
                self.note_blob_read_failure();
                continue;
            };
            for (i, slot, term, p_off, p_len, admit) in spans {
                let Some(bytes) = self.term_bytes(&blob, blob_id, p_off, p_len) else {
                    self.note_blob_read_failure();
                    continue;
                };
                let Some(parsed) =
                    BlockedTermPostings::parse_with(Arc::new(bytes.into_owned()), bitpacked)
                else {
                    self.note_blob_read_failure();
                    continue;
                };
                let arc = Arc::new(parsed);
                if admit {
                    let bytes = arc.approx_bytes();
                    self.scoring_cache.insert(i, Arc::clone(&arc), bytes);
                }
                slots[slot] = Some((term, arc));
            }
        }

        Some(slots.into_iter().flatten().collect())
    }

    fn read_posting_blob(&self, blob_id: usize) -> Option<Vec<u8>> {
        // Spans in the TOC refer to the uncompressed shard layout.
        let raw = fs::read(self.segment_dir.join(posting_blob_file_for_id(blob_id))).ok()?;
        read_inverted_payload(raw).ok()
    }

    /// The shard as a [`BlobSource`]: the whole local file when there is
    /// one (it always wins), otherwise — raw-shard segments only — the
    /// spans cached for it. `None` when neither exists.
    fn open_posting_blob(&self, blob_id: usize) -> Option<BlobSource> {
        if let Some(blob) = self.read_posting_blob(blob_id) {
            return Some(BlobSource::Whole(blob));
        }
        if !self.posting_spans {
            return None;
        }
        let spans =
            span_cache::SpanIndex::load(&self.segment_dir, &posting_blob_file_for_id(blob_id));
        if spans.is_empty() {
            // A promotion may have landed the whole shard and dropped the
            // spans between the two looks.
            return self.read_posting_blob(blob_id).map(BlobSource::Whole);
        }
        Some(BlobSource::Spans(spans))
    }

    /// One term's bytes from `blob`. On a span miss, looks for the whole
    /// shard once more: a background promotion lands the whole file before
    /// it drops the spans, so a span that vanished meanwhile is there.
    fn term_bytes<'b>(
        &self,
        blob: &'b BlobSource,
        blob_id: usize,
        off: usize,
        len: usize,
    ) -> Option<std::borrow::Cow<'b, [u8]>> {
        if let Some(bytes) = blob.term_bytes(off, len) {
            return Some(bytes);
        }
        if !matches!(blob, BlobSource::Spans(_)) {
            return None;
        }
        let whole = self.read_posting_blob(blob_id)?;
        let end = off.checked_add(len).filter(|&e| e <= whole.len())?;
        Some(std::borrow::Cow::Owned(whole[off..end].to_vec()))
    }
}

/// How `SegmentReader` accesses the inverted index. `Lazy` is the steady
/// state for any segment written by the current `SegmentWriter` (v2 layout,
/// see [`LazyInvertedIndex`]); `Eager` is the fallback for legacy v1
/// segments already on disk/S3 — identical behavior and cost to what every
/// segment paid before this change. Mirrors [`DocStoreAccess`].
enum InvertedAccess {
    Lazy(LazyInvertedIndex),
    Split(SplitInvertedIndex),
    Eager(HashMap<String, Vec<Posting>>),
}

impl InvertedAccess {
    fn postings(&self, term: &str) -> Option<PostingsRef<'_>> {
        match self {
            Self::Eager(map) => map.get(term).map(|v| PostingsRef::Borrowed(v.as_slice())),
            Self::Lazy(lazy) => lazy.postings(term).map(PostingsRef::Shared),
            Self::Split(split) => split.postings(term).map(PostingsRef::Shared),
        }
    }

    fn postings_for_terms<'a>(&'a self, terms: &'a [String]) -> Vec<(&'a str, PostingsRef<'a>)> {
        match self {
            Self::Eager(map) => terms
                .iter()
                .filter_map(|term| {
                    map.get(term)
                        .map(|v| (term.as_str(), PostingsRef::Borrowed(v.as_slice())))
                })
                .collect(),
            Self::Lazy(lazy) => terms
                .iter()
                .filter_map(|term| {
                    lazy.postings(term)
                        .map(|p| (term.as_str(), PostingsRef::Shared(p)))
                })
                .collect(),
            Self::Split(split) => split
                .postings_for_terms(terms)
                .into_iter()
                .map(|(term, p)| (term, PostingsRef::Shared(p)))
                .collect(),
        }
    }

    fn contains_term(&self, term: &str) -> bool {
        match self {
            Self::Eager(map) => map.contains_key(term),
            Self::Lazy(lazy) => lazy.contains_term(term),
            Self::Split(split) => split.contains_term(term),
        }
    }

    fn all_terms(&self) -> Vec<&str> {
        match self {
            Self::Eager(map) => {
                let mut terms: Vec<&String> = map.keys().collect();
                terms.sort();
                terms.into_iter().map(|s| s.as_str()).collect()
            }
            // The v2 table is written in sorted order — no sort needed.
            Self::Lazy(lazy) => lazy.all_terms(),
            Self::Split(split) => split.all_terms(),
        }
    }
}

/// A segment's term table, read in place in sorted (bytewise) order: the
/// dictionary a fuzzy query (#201) walks. Combined-index terms and
/// per-field keys alike; per-field keys sort first.
pub struct TermDictionary<'a>(TermDictionaryInner<'a>);

enum TermDictionaryInner<'a> {
    Lazy(&'a LazyInvertedIndex),
    Split(&'a SplitInvertedIndex),
    /// Legacy eager segments: their map is unordered, so sort a view once.
    Sorted(Vec<&'a str>),
}

impl kosha_core::fuzzy::SortedTerms for TermDictionary<'_> {
    fn term_count(&self) -> usize {
        match &self.0 {
            TermDictionaryInner::Lazy(lazy) => lazy.term_count,
            TermDictionaryInner::Split(split) => split.term_count,
            TermDictionaryInner::Sorted(terms) => terms.len(),
        }
    }

    fn term(&self, i: usize) -> &str {
        match &self.0 {
            TermDictionaryInner::Lazy(lazy) => lazy.term_at(i),
            TermDictionaryInner::Split(split) => split.term_at(i),
            TermDictionaryInner::Sorted(terms) => terms[i],
        }
    }
}

// ─── Segment reader ─────────────────────────────────────────────────────────

/// A document's location within `doc_store.bin`, plus the small scalar
/// fields (`doc_id`, `field_length`) that scoring needs constantly. Read
/// from the `doc_store.offsets` sidecar — proportional to document *count*,
/// never document *content* size.
///
/// Doc ids live in [`DocIndex::ids_pool`], one contiguous allocation for
/// the whole segment, not a heap `String` per document: a 1M-doc namespace
/// used to pay ~1M small allocations (plus per-`String` header overhead)
/// just to open its offsets sidecars — measured as a first-class share of
/// cold `open_total_ms` on staging.
struct DocIndexEntry {
    id_off: u32,
    id_len: u32,
    field_length: u32,
    offset: u64,
    length: u32,
}

/// Arena-backed per-document index parsed from `doc_store.offsets`.
struct DocIndex {
    /// Every document's id, concatenated. Entries slice into this by
    /// `(id_off, id_len)` — see [`DocIndex::id`].
    ids_pool: String,
    entries: Vec<DocIndexEntry>,
}

impl DocIndex {
    /// The length `doc_store.bin` has when complete: the last record's end.
    fn complete_len(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.offset + e.length as u64)
            .unwrap_or(0)
    }

    fn id(&self, doc_seq: usize) -> Option<&str> {
        let e = self.entries.get(doc_seq)?;
        self.ids_pool
            .get(e.id_off as usize..(e.id_off + e.id_len) as usize)
    }
}

/// How `SegmentReader` accesses document content. `Lazy` is the steady
/// state for any segment written by the current `SegmentWriter`: only the
/// small per-doc index is resident, and full field content is read from
/// disk on demand (`doc_record_full`) only for documents actually
/// materialized into a response. `Eager` is the fallback for segments
/// written before `doc_store.offsets` existed (see `Footer::format_version`)
/// — identical behavior/cost to what every segment paid before this change.
enum DocStoreAccess {
    Lazy {
        doc_store_path: PathBuf,
        index: DocIndex,
        /// `Some` for a compressed doc store (research #388): stored
        /// records are decoded through it before parsing.
        decoder: Option<Arc<DocStoreDecoder>>,
    },
    Eager(Vec<DocRecord>),
}

/// Zero-I/O metadata for one document — the `doc_id`/`field_length` that
/// BM25 scoring and default/`_id` sorting need, without ever touching the
/// document's full field content on disk.
pub struct DocMetaRef<'a> {
    /// Borrowed from the segment's contiguous id pool (Lazy) or the eager
    /// doc record (legacy) — `&str` rather than `&DocumentId` so the Lazy
    /// path never materializes a `DocumentId` per document.
    pub doc_id: &'a str,
    pub doc_seq: u32,
    pub field_length: u32,
}

/// `filters.bin` held raw, parsed on first use. Filter columns are only
/// touched by queries that actually filter, aggregate, or sort by field
/// values — a broad lexical query never does — yet the eager parse at open
/// was a first-class share of cold open time (staging: 50MB filters.bin
/// per segment on the worst namespace ≈ 529ms/segment opens). The raw
/// buffer's size is what `approx_segment_bytes` already budgets (on-disk
/// size of filters.bin), so the memory ledger's accounting is unchanged;
/// the parsed form is an additional cost paid only by segments whose
/// filters are actually exercised.
///
/// `state` is only ever written *after* a read has confirmed the file is
/// actually present — see `get()`. A `OnceLock` cannot express that: its
/// init closure runs at most once, so a `get()` that raced a not-yet-local
/// `filters.bin` would freeze an empty parse in permanently, even after a
/// later hydration landed the real file (see `get()`'s doc comment for the
/// incident this caused in production: a cached reader silently matched
/// zero documents forever, on a file later confirmed byte-correct on disk).
///
/// Columnar segments (#244, `Footer::filter_columns` is `Some`) follow the
/// same rule per file: `filters.toc` and each `filters-NNN.col` is cached
/// only once a read found it present. A query node hydrates only the
/// columns its queries touch, so [`Self::store_for`] adds columns to the
/// cached compatibility [`FilterStore`] as they land.
///
/// A filter file the footer says exists but that is not on local disk
/// (never hydrated, or evicted from the disk cache) is an error, never "no
/// values": read as empty, a `matterId` filter matched nothing in the
/// segment, and the searcher's per-segment filter-set cache then kept that
/// empty answer after the file came back. The error is not cached either,
/// so the next call reads the disk again.
struct LazyFilters {
    segment_dir: PathBuf,
    doc_count: u32,
    /// `Footer::filter_columns.is_some()`: the format comes from the
    /// footer, never from which files are on disk.
    columnar: bool,
    state: Mutex<FilterState>,
}

#[derive(Default)]
struct FilterState {
    /// Legacy: parsed `filters.bin`, once seen present.
    legacy: Option<Arc<FilterStore>>,
    /// Columnar: parsed `filters.toc`, once seen present.
    toc: Option<Arc<filter_columns::FiltersToc>>,
    /// Columnar: the compatibility store built from the columns loaded so
    /// far, and their ids.
    merged: Arc<FilterStore>,
    merged_ids: std::collections::BTreeSet<u32>,
    /// `filter_column` cache, by column id (columnar) or `(name, kind)`
    /// position in the legacy store.
    columns: HashMap<(String, kosha_core::FilterColumnKind), Arc<filter_columns::FilterColumn>>,
}

impl LazyFilters {
    /// Read + parse `filters.bin` on first use — from disk *at use time*,
    /// not from bytes captured at open. This matters under filters-skipping
    /// hydration: a broad query can open (and cache) a segment while
    /// `filters.bin` isn't local yet; a later filtered query hydrates the
    /// file and then hits the *cached* reader — snapshotting raw bytes at
    /// open would have frozen that reader's filters as empty forever,
    /// silently matching nothing. Reading at use time sees the
    /// now-hydrated file. (It also means broad queries keep zero filter
    /// bytes resident.)
    ///
    /// Only a read that finds the file *present* is cached. A read that
    /// finds it missing/empty is an error for that one call and leaves the
    /// cache unset, so the *next* call re-reads from disk — self-healing
    /// once hydration actually lands. The writer always emits filters.bin
    /// (even with zero fields, the header is present), so an empty raw
    /// read is unambiguously "file isn't local," never "this segment has
    /// no filterable fields" — a genuinely field-less segment's file is
    /// non-empty (just a zero-entry header) and caches normally on first
    /// read.
    fn legacy(&self, state: &mut FilterState) -> Result<Arc<FilterStore>, KoshaError> {
        if let Some(store) = state.legacy.as_ref() {
            return Ok(Arc::clone(store));
        }
        let raw = SegmentReader::read_filters_raw(&self.segment_dir);
        if raw.is_empty() {
            // Not cached: `state.legacy` stays `None`, so the next call
            // re-reads once hydration lands the file.
            return Err(self.not_local(kosha_core::FILTERS_BIN_FILE));
        }
        let store = Arc::new(SegmentReader::parse_filters(&raw)?);
        state.legacy = Some(Arc::clone(&store));
        Ok(store)
    }

    /// The error for a filter file the footer lists but that is not on
    /// local disk. See the type's doc comment.
    fn not_local(&self, file: &str) -> KoshaError {
        KoshaError::NotFound(format!(
            "{}/{file}: filter file not local (not hydrated, or evicted from the disk cache); \
             the filter cannot be evaluated",
            self.segment_dir.display()
        ))
    }

    /// `filters.toc`; an error while it is not local (not cached).
    fn toc(&self, state: &mut FilterState) -> Result<Arc<filter_columns::FiltersToc>, KoshaError> {
        if let Some(toc) = state.toc.as_ref() {
            return Ok(Arc::clone(toc));
        }
        let path = self.segment_dir.join(kosha_core::FILTERS_TOC_FILE);
        let Some(bytes) = filter_columns::read_present(&path)? else {
            return Err(self.not_local(kosha_core::FILTERS_TOC_FILE));
        };
        let toc = filter_columns::FiltersToc::decode(&bytes).map_err(|e| {
            KoshaError::CorruptSegment(format!("{}: {e}", self.segment_dir.display()))
        })?;
        if toc.doc_count != self.doc_count {
            return Err(KoshaError::CorruptSegment(format!(
                "{}: filters.toc is for {} documents, footer says {}",
                self.segment_dir.display(),
                toc.doc_count,
                self.doc_count
            )));
        }
        let toc = Arc::new(toc);
        state.toc = Some(Arc::clone(&toc));
        Ok(toc)
    }

    /// Read and decode one column file. Not local, or present but damaged,
    /// is an error: it fails the query rather than quietly matching nothing.
    fn read_column(
        &self,
        meta: &kosha_core::FilterColumnMeta,
    ) -> Result<filter_columns::FilterColumn, KoshaError> {
        let file = meta.file_name();
        let Some(bytes) = filter_columns::read_present(&self.segment_dir.join(&file))? else {
            return Err(self.not_local(&file));
        };
        let what = format!("{}/{file} ({:?})", self.segment_dir.display(), meta.name);
        filter_columns::decode_column(&bytes, meta.kind, self.doc_count, &what)
    }

    /// The compatibility [`FilterStore`] holding at least every column
    /// named in `fields` (all columns for `None`); an error when one of
    /// them is not local. Legacy segments ignore `fields`: `filters.bin`
    /// holds every column.
    fn store_for(
        &self,
        fields: Option<&std::collections::BTreeSet<String>>,
    ) -> Result<Arc<FilterStore>, KoshaError> {
        let mut state = self.state.lock().unwrap();
        if !self.columnar {
            return self.legacy(&mut state);
        }
        let toc = self.toc(&mut state)?;
        let wanted: Vec<&kosha_core::FilterColumnMeta> = toc
            .columns
            .iter()
            .filter(|c| !state.merged_ids.contains(&c.id))
            .filter(|c| fields.is_none_or(|f| f.contains(&c.name)))
            .collect();
        if wanted.is_empty() {
            return Ok(Arc::clone(&state.merged));
        }
        let loaded = wanted
            .into_iter()
            .map(|meta| Ok((meta, self.read_column(meta)?)))
            .collect::<Result<Vec<_>, KoshaError>>()?;
        // Copy-on-grow: readers holding the previous Arc keep it. Rare —
        // only when a query touches a column this segment has not loaded.
        let mut store = (*state.merged).clone();
        for (meta, column) in loaded {
            column.append_to(&meta.name, &mut store);
            state.merged_ids.insert(meta.id);
        }
        state.merged = Arc::new(store);
        Ok(Arc::clone(&state.merged))
    }

    /// One column, decoded and cached. `Ok(None)` when the segment has no
    /// such column; an error (not cached) when its file is not local.
    fn column(
        &self,
        name: &str,
        kind: kosha_core::FilterColumnKind,
    ) -> Result<Option<Arc<filter_columns::FilterColumn>>, KoshaError> {
        use filter_columns::{FilterColumn, NumericColumn, StringColumn};
        let key = (name.to_string(), kind);
        let mut state = self.state.lock().unwrap();
        if let Some(column) = state.columns.get(&key) {
            return Ok(Some(Arc::clone(column)));
        }
        let column = if self.columnar {
            let toc = self.toc(&mut state)?;
            let Some(meta) = toc.find(name, kind) else {
                return Ok(None);
            };
            self.read_column(meta)?
        } else {
            let store = self.legacy(&mut state)?;
            let column = match kind {
                kosha_core::FilterColumnKind::String => store
                    .string_fields
                    .get(name)
                    .map(|e| StringColumn::from_entries(e).map(FilterColumn::String))
                    .transpose()?,
                kosha_core::FilterColumnKind::Integer => store
                    .integer_fields
                    .get(name)
                    .map(|e| FilterColumn::Integer(NumericColumn::from_entries(e))),
                kosha_core::FilterColumnKind::Float => store
                    .float_fields
                    .get(name)
                    .map(|e| FilterColumn::Float(NumericColumn::from_entries(e))),
            };
            match column {
                Some(column) => column,
                None => return Ok(None),
            }
        };
        let column = Arc::new(column);
        state.columns.insert(key, Arc::clone(&column));
        Ok(Some(column))
    }
}

/// A vector posting's decoded live entries, shared between the per-segment
/// cache and in-flight queries (a cache hit is a pointer clone).
type SharedVectorPosting = Arc<VectorPosting>;

/// Resident state for a v2 (posting-based) vector column: centroids are
/// always in memory (small — a segment's posting count tops out in the
/// hundreds), posting entries are loaded on demand from the column's
/// `vector[.<slot>].idx` by `SegmentReader::scan_posting`. Mutually
/// exclusive with [`VectorFieldIndex::flat`] being populated — see
/// `load_vector_index`'s doc comment for the four-way dispatch.
struct LazyVectorIndex {
    vector_idx_path: PathBuf,
    /// Basename of `vector_idx_path` (`vector.idx` or `vector.<n>.idx`):
    /// the file the span cache and [`VectorSpanFetch`] name (#245).
    idx_file: String,
    /// The column's file slot — part of every posting-cache key, so two
    /// columns' postings never alias in the segment's shared cache.
    slot: u32,
    dim: usize,
    /// Every posting's centroid in one row-major matrix (#436): row `i`,
    /// `dim` floats, is posting `i`'s centroid — see [`Self::centroid`].
    /// One allocation, so the per-query scan over all of them reads memory
    /// sequentially instead of chasing a pointer per centroid.
    centroids: Vec<f32>,
    /// i8 codes of the same centroids, for the first stage of kNN planning
    /// (see [`CentroidCodes`]). Built here, in memory, when the column is
    /// loaded; nothing on disk changes. `None` for a column that is not
    /// unit-norm, with `KOSHA_KNN_PLAN_I8=0`, or when a centroid is not a
    /// sane unit-scale vector: planning then scores the f32 centroids.
    centroid_codes: Option<CentroidCodes>,
    /// A spherical cap holding every centroid, for a floor on the column's
    /// rank scores (see [`CentroidCap`]). Built at load like the codes;
    /// `None` for a column that is not unit-norm or has no sane centroids.
    centroid_cap: Option<CentroidCap>,
    /// `(offset, length)` into `vector.idx`, one per posting.
    ranges: Vec<(u64, u32)>,
    /// Parallel to `ranges` — max cosine distance from each posting's
    /// centroid to any of its live members. `0.0` for every posting when
    /// read from a pre-radius-aware sidecar (version 1 or 2), which
    /// degrades ranking exactly to raw centroid distance — see
    /// `RADIUS_AWARE_VECTOR_INDEX_VERSION`.
    radii: Vec<f32>,
    /// Stored vectors and centroids are unit-L2 (on-disk header version is
    /// [`UNIT_NORM_VECTOR_INDEX_VERSION`]) — scoring uses the dot-only fast
    /// path: normalize the query once, then `1 - dot` *is* cosine distance.
    unit_norm: bool,
    /// The idx header's version — decides how posting entries decode.
    version: u32,
    /// Entries are quantized (i8, [`I8_VECTOR_INDEX_VERSION`], or RaBitQ,
    /// [`RABITQ_VECTOR_INDEX_VERSION`]): scores are approximate, and the
    /// probe rescores its best candidates from `doc_vectors.bin` (see
    /// [`SegmentReader::with_vector_rescore_factor`]).
    quantized: bool,
    /// Entries are RaBitQ codes ([`RABITQ_VECTOR_INDEX_VERSION`]).
    rabitq: bool,
    /// The column's field name, from the footer (`None` for a legacy
    /// segment's unnamed index) — what `doc_vectors.bin` is keyed by.
    field: Option<String>,
}

impl LazyVectorIndex {
    fn from_parsed(
        idx_path: &Path,
        slot: u32,
        dim: u32,
        version: u32,
        parsed: Vec<VectorPostingIndex>,
        field: Option<String>,
    ) -> Self {
        let dim = dim as usize;
        let mut centroids = Vec::with_capacity(parsed.len() * dim);
        let mut ranges = Vec::with_capacity(parsed.len());
        let mut radii = Vec::with_capacity(parsed.len());
        for p in parsed {
            // `parse_vector_offsets` reads exactly `dim` floats per
            // posting; anything else would shift every later row.
            assert_eq!(p.centroid.len(), dim, "centroid of the wrong dimension");
            centroids.extend_from_slice(&p.centroid);
            ranges.push((p.offset, p.length));
            radii.push(p.radius);
        }
        let unit_norm = is_unit_norm_vector_index_version(version);
        let centroid_codes = (unit_norm && knn_plan_i8_from_env())
            .then(|| CentroidCodes::build(&centroids, dim))
            .flatten();
        let centroid_cap = unit_norm
            .then(|| CentroidCap::build(&centroids, dim, &radii))
            .flatten();
        LazyVectorIndex {
            vector_idx_path: idx_path.to_path_buf(),
            idx_file: idx_path.file_name().map_or_else(
                || VECTOR_IDX_FILE.to_string(),
                |n| n.to_string_lossy().into_owned(),
            ),
            slot,
            dim,
            centroids,
            centroid_codes,
            centroid_cap,
            ranges,
            radii,
            unit_norm,
            version,
            quantized: is_quantized_vector_index_version(version),
            rabitq: is_rabitq_vector_index_version(version),
            field,
        }
    }

    /// Posting `i`'s centroid: row `i` of [`Self::centroids`].
    fn centroid(&self, i: usize) -> &[f32] {
        &self.centroids[i * self.dim..(i + 1) * self.dim]
    }

    /// Every centroid, in posting order.
    fn centroid_rows(&self) -> impl Iterator<Item = &[f32]> {
        (0..self.ranges.len()).map(|i| self.centroid(i))
    }
}

/// Bytes per centroid that [`CentroidCodes`] keeps resident beyond its
/// `dim` code bytes: the row's scale and its two error coefficients.
const CENTROID_CODE_ROW_OVERHEAD: usize = 3 * std::mem::size_of::<f32>();

/// Largest dimension [`CentroidCodes`] is built for. The rounding-error
/// term of its bound assumes `dim * 2^-24` is far below 1.
const CENTROID_CODES_MAX_DIM: usize = 1 << 16;

/// i8 codes of a unit-norm column's centroids: a quarter of the f32
/// matrix, so the per-query scan over every centroid of every segment
/// (`knn_plan`, which is memory-bandwidth-bound at 1024 dimensions) reads a
/// quarter of the bytes. The codes only decide which centroids are worth
/// an exact f32 score; nothing ranked, cut off or probed ever comes from
/// them (see `SegmentReader::centroid_score_bounds`).
///
/// Quantization is [`kosha_vector_spfresh::quantize_i8`], one scale per
/// centroid. A query is quantized too, to i16
/// ([`kosha_vector_spfresh::quantize_i16`]), and the estimate of
/// `<q, c>` is `s_q * s * <q_k, k>` with the sum taken in integers
/// ([`kosha_vector_spfresh::dot_i16_i8`]).
///
/// # Error coefficients
///
/// For row `c` with codes `k` and scale `s`, let `c^ = s * k` and
/// `r = c - c^`, the residual actually left by quantization (measured
/// here, in f64, not assumed to be `s / 2` per coordinate). Likewise
/// `q^ = s_q * q_k` and `r_q = q - q^` for the query. In real arithmetic
///
/// ```text
/// <q, c> - <q^, c^> = <q, r> + <r_q, c^>
/// |<q, r>|    <= min(||q||_2 * ||r||_2,  ||q||_1 * ||r||_inf)
/// |<r_q, c^>| <= ||r_q||_2 * ||c^||_2
/// ```
///
/// (Cauchy-Schwarz and Hoelder). The `||q||_1` form is the
/// `scale / 2 * ||q||_1` bound the i8 posting path uses
/// (`KOSHA_KNN_I8_BOUND=strict`); the `||q||_2` form is about a third
/// tighter for a dense query and is what usually decides. The query term
/// is small (a 1024-dimension query keeps 14 bits a coordinate) and is
/// added per query from [`Self::max_code_norm`].
///
/// The exact side is an f32 dot product and carries a rounding error: for
/// a sum of `n` products in any order at most `gamma_n * sum |x_i * y_i|`
/// with `gamma_n = n u / (1 - n u)`, `u = 2^-24` (Higham, Accuracy and
/// Stability of Numerical Algorithms, section 3.1), and
/// `sum |q_i * c_i| <= ||q||_2 * ||c||_2`. `fp` below is
/// `(n + 8) * 2u * (||c||_2 + ||c^||_2)`, more than twice that. The
/// integer sum is exact; turning it into an f32 estimate costs three
/// roundings, which the query term also covers.
///
/// `err_l2 = ||r||_2 + fp` and `err_linf = ||r||_inf + fp` (`fp` may ride
/// on the L1 term because `||q||_2 <= ||q||_1`), each rounded up, so for
/// the computed f32 values
///
/// ```text
/// |dot(q, c) - estimate| <= min(err_l2 * ||q||_2, err_linf * ||q||_1) + query term
/// ```
#[derive(Debug)]
struct CentroidCodes {
    /// Row-major, `dim` codes per centroid, parallel to
    /// [`LazyVectorIndex::centroids`].
    codes: Vec<i8>,
    /// One scale per centroid.
    scales: Vec<f32>,
    err_l2: Vec<f32>,
    err_linf: Vec<f32>,
    /// The largest `||s * k||_2` of any row, rounded up: what a query's
    /// own quantization error is multiplied by.
    max_code_norm: f32,
}

impl CentroidCodes {
    /// `None` when the codes could not carry a guaranteed bound: an empty
    /// or absurdly wide column, or a centroid that is not finite or not
    /// roughly unit length (the writer renormalizes them, so this means a
    /// damaged or foreign sidecar; the exact path makes no such
    /// assumption).
    fn build(centroids: &[f32], dim: usize) -> Option<Self> {
        if dim == 0 || dim > CENTROID_CODES_MAX_DIM || centroids.is_empty() {
            return None;
        }
        let rows = centroids.len() / dim;
        let mut out = CentroidCodes {
            codes: Vec::with_capacity(rows * dim),
            scales: Vec::with_capacity(rows),
            err_l2: Vec::with_capacity(rows),
            err_linf: Vec::with_capacity(rows),
            max_code_norm: 0.0,
        };
        let mut row_codes: Vec<i8> = Vec::with_capacity(dim);
        // Rounds a non-negative f64 up to an f32 with room to spare for
        // the f32 arithmetic the coefficient is later used in.
        let up = |x: f64| (x * (1.0 + 1e-5)) as f32;
        for row in centroids.chunks_exact(dim) {
            if row.iter().any(|x| !x.is_finite()) {
                return None;
            }
            let scale = kosha_vector_spfresh::quantize_i8(row, &mut row_codes);
            let (mut norm2, mut approx2, mut res2, mut res_inf) = (0f64, 0f64, 0f64, 0f64);
            for (&x, &k) in row.iter().zip(&row_codes) {
                let x = f64::from(x);
                let approx = f64::from(scale) * f64::from(k);
                let r = x - approx;
                norm2 += x * x;
                approx2 += approx * approx;
                res2 += r * r;
                res_inf = res_inf.max(r.abs());
            }
            if !scale.is_finite() || norm2.sqrt() > 2.0 {
                return None;
            }
            let fp = (dim + 8) as f64 * f64::from(f32::EPSILON) * (norm2.sqrt() + approx2.sqrt());
            out.codes.extend_from_slice(&row_codes);
            out.scales.push(scale);
            out.err_l2.push(up(res2.sqrt() + fp));
            out.err_linf.push(up(res_inf + fp));
            out.max_code_norm = out.max_code_norm.max(up(approx2.sqrt()));
        }
        Some(out)
    }

    /// Posting `i`'s codes.
    fn row(&self, i: usize, dim: usize) -> &[i8] {
        &self.codes[i * dim..(i + 1) * dim]
    }
}

/// The smallest-known spherical cap around a unit-norm column's
/// centroids: a centre direction `m` and an angle `theta` with
/// `angle(m, c) <= theta` for every centroid `c`, plus the largest centroid
/// norm and the largest radius. One per loaded column (`dim` f64s), built
/// from the resident centroids; nothing on disk changes.
///
/// It gives the kNN plan a floor on every posting's rank score from one
/// dot product per segment ([`SegmentReader::knn_centroid_score_floor`]),
/// so a segment whose centroids all lie far from the query can be left out
/// of the plan without scoring them (#253). Segments that each cover the
/// whole space (hash- or time-partitioned) get a floor of `0` and are
/// scored as before; the floor only pays off once segments are spatially
/// clustered.
#[derive(Debug)]
struct CentroidCap {
    /// Unit centre, in f64.
    center: Vec<f64>,
    /// Upper bound on the angle from `center` to any centroid, radians.
    theta: f64,
    /// Upper bound on any centroid's L2 norm.
    max_norm: f64,
    /// Lower bound on any centroid's L2 norm.
    min_norm: f64,
    /// The largest radius of any posting.
    max_radius: f32,
}

impl CentroidCap {
    /// Angular slack for f64 rounding in `acos` (ill-conditioned near 0).
    const ANGLE_PAD: f64 = 1e-6;

    /// `None` when there is nothing to bound or the bound's assumptions do
    /// not hold: no centroids, a centroid or radius that is not finite, a
    /// centroid far from unit length (a damaged or foreign sidecar), or
    /// centroids that cancel out so there is no meaningful centre.
    fn build(centroids: &[f32], dim: usize, radii: &[f32]) -> Option<Self> {
        if dim == 0 || centroids.is_empty() || centroids.len() != dim * radii.len() {
            return None;
        }
        if radii.iter().any(|r| !r.is_finite()) {
            return None;
        }
        let mut center = vec![0f64; dim];
        let mut max_norm = 0f64;
        let mut min_norm = f64::INFINITY;
        for row in centroids.chunks_exact(dim) {
            if row.iter().any(|x| !x.is_finite()) {
                return None;
            }
            let norm = row
                .iter()
                .map(|&x| f64::from(x).powi(2))
                .sum::<f64>()
                .sqrt();
            if !(0.5..=2.0).contains(&norm) {
                return None;
            }
            max_norm = max_norm.max(norm);
            min_norm = min_norm.min(norm);
            for (m, &x) in center.iter_mut().zip(row) {
                *m += f64::from(x) / norm;
            }
        }
        let rows = radii.len() as f64;
        let len = center.iter().map(|x| x * x).sum::<f64>().sqrt();
        if len.is_nan() || len <= 1e-6 * rows {
            return None;
        }
        for m in &mut center {
            *m /= len;
        }
        let mut theta = 0f64;
        for row in centroids.chunks_exact(dim) {
            let norm = row
                .iter()
                .map(|&x| f64::from(x).powi(2))
                .sum::<f64>()
                .sqrt();
            let cos = center
                .iter()
                .zip(row)
                .map(|(m, &x)| m * f64::from(x))
                .sum::<f64>()
                / norm;
            theta = theta.max(cos.clamp(-1.0, 1.0).acos());
        }
        Some(Self {
            center,
            theta: theta + Self::ANGLE_PAD,
            max_norm: max_norm * (1.0 + 1e-9),
            min_norm: min_norm * (1.0 - 1e-9),
            max_radius: radii.iter().copied().fold(f32::NEG_INFINITY, f32::max),
        })
    }

    /// A floor on [`SegmentReader::centroid_score`] for every posting of
    /// the column, given `unit`, the query as [`SegmentReader::unit_query`]
    /// scales it. `None` for a query that is not finite or a radius weight
    /// that is not finite and `>= 0`.
    ///
    /// Why it holds, for posting `i` with f32 centroid `c` and radius `r`:
    ///
    /// 1. Angles between directions are a metric on the sphere, so with
    ///    `phi = angle(unit, m)` and `angle(m, c) <= theta`,
    ///    `angle(unit, c) >= max(0, phi - theta)` and, in real arithmetic,
    ///    `dot(unit, c) <= |unit| * |c| * cos(max(0, phi - theta))`
    ///    (cos is decreasing on `[0, pi]`), and `|c|` is at most
    ///    `max_norm` when that cosine is positive, at least `min_norm`
    ///    when it is negative. `phi` gets `ANGLE_PAD` taken off for its own
    ///    `acos` rounding.
    /// 2. The f32 dot the score is computed from is within
    ///    `gamma_n * |unit| * |c|` of the real one (Higham, section 3.1);
    ///    `(dim + 8) * 2^-23 * |unit| * max_norm` is more than that.
    /// 3. `d = fl(1 - clamp(x))` is non-increasing in `x` (so the bound on
    ///    `x` is clamped the same way) and rounds a value
    ///    in `[0, 2]` by at most `2^-23`.
    /// 4. [`centroid_probe_score`] is `max(fl(d - fl(alpha * r)), 0)`,
    ///    non-decreasing in `d` and, for `alpha >= 0`, non-increasing in
    ///    `r`, so `max_radius` gives the smallest score; the last
    ///    subtraction rounds by at most a half ulp of a value under
    ///    `2 + alpha * max_radius`.
    ///
    /// The result is computed in f64 with that slack (and `PAD` more) and
    /// only then rounded to f32, so the floor is never above any score.
    fn score_floor(&self, unit: &[f32], alpha: f32) -> Option<f32> {
        const PAD: f64 = 4e-6;
        if !(alpha.is_finite() && alpha >= 0.0) || unit.len() != self.center.len() {
            return None;
        }
        if unit.iter().any(|x| !x.is_finite()) {
            return None;
        }
        let norm = unit
            .iter()
            .map(|&x| f64::from(x).powi(2))
            .sum::<f64>()
            .sqrt();
        let dim = unit.len() as f64;
        let rounding = (dim + 8.0) * f64::from(f32::EPSILON) * norm * self.max_norm;
        let real_max = if norm == 0.0 {
            0.0
        } else {
            let cos = self
                .center
                .iter()
                .zip(unit)
                .map(|(m, &x)| m * f64::from(x))
                .sum::<f64>()
                / norm;
            let phi = cos.clamp(-1.0, 1.0).acos() - Self::ANGLE_PAD;
            let gap = (phi - self.theta).max(0.0);
            // `gap` can exceed pi only through the pads' arithmetic; cos is
            // not monotone past pi, so cap it there.
            let cos = gap.min(std::f64::consts::PI).cos();
            let c_norm = if cos >= 0.0 {
                self.max_norm
            } else {
                self.min_norm
            };
            norm * c_norm * cos
        };
        let x = (real_max + rounding).clamp(-1.0, 1.0);
        let distance = 1.0 - x - f64::from(f32::EPSILON);
        let radius_term = f64::from(alpha * self.max_radius);
        let rounding = f64::from(f32::EPSILON) * (2.0 + radius_term.abs());
        let floor = distance - radius_term - rounding - PAD;
        if !floor.is_finite() {
            return None;
        }
        Some(if floor <= 0.0 { 0.0 } else { floor as f32 })
    }
}

/// Resident bytes of the centroid codes a segment open builds for the
/// vector column whose `vector.offsets` sidecar is at `offsets_path` (see
/// [`CentroidCodes`]): zero when the column gets none (not unit-norm,
/// `KOSHA_KNN_PLAN_I8=0`, or no readable sidecar header).
///
/// The codes are derived at load and have no file of their own, so the
/// on-disk sizes the query layer's `approx_segment_bytes` sums do not see
/// them; it adds this. Only the sidecar's 16-byte header is read.
pub fn vector_centroid_code_bytes(offsets_path: &Path) -> u64 {
    if !knn_plan_i8_from_env() {
        return 0;
    }
    unit_norm_offsets_shape(offsets_path).map_or(0, |(dim, postings)| {
        postings.saturating_mul(dim + CENTROID_CODE_ROW_OVERHEAD as u64)
    })
}

/// Resident bytes of the [`CentroidCap`] a segment open builds for the
/// vector column whose `vector.offsets` sidecar is at `offsets_path`: its
/// `f64` centre, or zero when the column is not unit-norm or has no
/// readable sidecar header. Derived at load like the centroid codes, so
/// `approx_segment_bytes` adds it too. A unit-norm column whose centroids
/// give no cap is still charged, the conservative direction.
pub fn vector_centroid_cap_bytes(offsets_path: &Path) -> u64 {
    unit_norm_offsets_shape(offsets_path).map_or(0, |(dim, _)| {
        dim.saturating_mul(std::mem::size_of::<f64>() as u64)
    })
}

/// `(dim, postings)` from the header of a unit-norm column's
/// `vector.offsets` sidecar; `None` for any other column or an unreadable
/// header. Only the 16-byte header is read.
fn unit_norm_offsets_shape(offsets_path: &Path) -> Option<(u64, u64)> {
    let mut header = [0u8; VECTOR_OFFSETS_HEADER_LEN];
    let read = fs::File::open(offsets_path).and_then(|mut f| f.read_exact(&mut header));
    if read.is_err() || header[0..4] != VECTOR_OFFSETS_MAGIC.to_le_bytes() {
        return None;
    }
    let mut cursor = &header[4..];
    let version = read_u32_le(&mut cursor);
    let dim = read_u32_le(&mut cursor) as u64;
    let postings = read_u32_le(&mut cursor) as u64;
    (is_known_vector_index_version(version) && is_unit_norm_vector_index_version(version))
        .then_some((dim, postings))
}

/// `KOSHA_KNN_PLAN_I8` (default on): `0`, `false` or `off` turns off the
/// i8 first stage of kNN planning. Unit-norm columns are then loaded
/// without centroid codes and the plan scores every f32 centroid, as it
/// did before the stage existed. Read once per process.
pub fn knn_plan_i8_from_env() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("KOSHA_KNN_PLAN_I8")
                .ok()
                .as_deref()
                .map(str::trim),
            Some("0") | Some("false") | Some("off")
        )
    })
}

/// Guaranteed bounds on every posting's kNN rank score, from the i8
/// centroid codes: `lower[i] <= score <= upper[i]`, where `score` is the
/// f32 value [`SegmentReader::knn_centroid_distances`] computes for
/// posting `i`. See [`SegmentReader::knn_centroid_score_bounds`].
#[derive(Debug, Clone, PartialEq)]
pub struct KnnCentroidScoreBounds {
    pub lower: Vec<f32>,
    pub upper: Vec<f32>,
}

/// Centroids held in memory and scored exactly as a loaded unit-norm
/// column's are, with the same i8 codes: for timing and testing the kNN
/// planning pass at sizes no test corpus on disk reaches. Not a way to
/// search anything (it has no postings).
#[doc(hidden)]
pub struct KnnCentroidMatrix(LazyVectorIndex);

impl KnnCentroidMatrix {
    /// `centroids` is row-major, `dim` floats per row, one radius per row.
    pub fn new(dim: usize, centroids: &[f32], radii: &[f32]) -> Self {
        assert!(dim > 0 && centroids.len() == dim * radii.len());
        let parsed = centroids
            .chunks_exact(dim)
            .zip(radii)
            .map(|(centroid, &radius)| VectorPostingIndex {
                centroid: centroid.to_vec(),
                offset: 0,
                length: 0,
                radius,
            })
            .collect();
        Self(LazyVectorIndex::from_parsed(
            Path::new(VECTOR_IDX_FILE),
            0,
            dim as u32,
            RADIUS_AWARE_VECTOR_INDEX_VERSION,
            parsed,
            None,
        ))
    }

    pub fn postings(&self) -> usize {
        self.0.ranges.len()
    }

    /// See [`SegmentReader::knn_centroid_distances_with_alpha`].
    pub fn scores(&self, query: &[f32], alpha: f32) -> Vec<f32> {
        SegmentReader::centroid_scores(&self.0, query, alpha)
    }

    /// See [`SegmentReader::knn_centroid_scores_for`].
    pub fn scores_for(&self, query: &[f32], postings: &[u32], alpha: f32) -> Vec<f32> {
        SegmentReader::centroid_scores_at(&self.0, query, postings, alpha)
    }

    /// See [`SegmentReader::knn_centroid_score_bounds`].
    pub fn score_bounds(&self, query: &[f32], alpha: f32) -> Option<KnnCentroidScoreBounds> {
        SegmentReader::centroid_score_bounds(&self.0, query, alpha)
    }

    /// See [`SegmentReader::knn_centroid_score_floor`].
    pub fn score_floor(&self, query: &[f32], alpha: f32) -> Option<f32> {
        SegmentReader::centroid_score_floor(&self.0, query, alpha)
    }
}

/// One vector column of a segment, loaded (#204). Exactly one of `lazy` or
/// `flat` is populated, or neither when the column's vectors are absent or
/// unreadable (`degraded` tells those two apart).
pub struct VectorFieldIndex {
    lazy: Option<LazyVectorIndex>,
    flat: VectorStore,
    /// `true` when the column has a valid v2 `vector.idx` header but a
    /// corrupt/truncated body. `lazy` is then `None` and `flat` empty —
    /// identical, at the type level, to a column with no vectors at all.
    /// The query layer (`kosha-query`) checks this flag to tell the two
    /// apart: a genuinely vector-less segment contributes zero kNN hits
    /// legitimately, while a degraded one is silently missing real results
    /// and must be counted in `SearchResult::knn_degraded_segments`.
    degraded: bool,
}

impl VectorFieldIndex {
    /// Served by the lazy v2 posting index (rather than [`Self::flat`]).
    pub fn has_lazy_index(&self) -> bool {
        self.lazy.is_some()
    }
    /// The lazy index holds quantized codes (i8 or RaBitQ), whose probe
    /// scores are estimates until rescored from `doc_vectors.bin`.
    pub fn is_quantized(&self) -> bool {
        self.lazy.as_ref().is_some_and(|vi| vi.quantized)
    }
    /// Legacy / missing-sidecar vectors, served by exact flat scan. Empty
    /// when [`Self::has_lazy_index`].
    pub fn flat(&self) -> &VectorStore {
        &self.flat
    }
    /// See the field's doc comment.
    pub fn degraded(&self) -> bool {
        self.degraded
    }
    /// The column's dimension, when known from the index itself.
    pub fn dim(&self) -> Option<usize> {
        match &self.lazy {
            Some(vi) => Some(vi.dim),
            None if !self.flat.vectors.is_empty() => Some(self.flat.dimensions),
            None => None,
        }
    }
}

/// One vector column as a reader knows it before loading it (#204).
struct VectorFieldSlot {
    /// From the footer's `vector_fields`; `None` for the one unnamed index
    /// of a legacy segment.
    meta: Option<VectorFieldMeta>,
    idx_path: PathBuf,
    offsets_path: PathBuf,
    /// Loaded on the first kNN that targets this column, so a query on one
    /// column never reads (or needs hydrated) another column's files.
    loaded: Mutex<Option<Arc<VectorFieldIndex>>>,
}

/// How a segment relates to a `knn.field` (#204) — see
/// [`SegmentReader::vector_field_status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorFieldStatus {
    /// The footer lists the field: its own index, of this dimension.
    Indexed { dim: usize },
    /// A segment written before vector columns existed, whose single
    /// unnamed index answers a kNN on any field name it is not known to
    /// be bound away from (migration rule). `dim` is `None` when the index
    /// could not be read.
    Legacy { dim: Option<usize> },
    /// The segment has no vectors for the field.
    Absent,
}

/// [`VectorFieldStatus`] of `field` from `footer` alone, when the footer
/// lists its vector columns; `None` for a legacy footer, whose segment must
/// be opened to tell (see [`SegmentReader::vector_field_status`]).
pub fn footer_vector_field_status(footer: &Footer, field: &str) -> Option<VectorFieldStatus> {
    let fields = footer.vector_fields.as_ref()?;
    Some(
        fields
            .iter()
            .find(|meta| meta.name == field)
            .map_or(VectorFieldStatus::Absent, |meta| {
                VectorFieldStatus::Indexed {
                    dim: meta.dim as usize,
                }
            }),
    )
}

/// `(top-k hits, postings hydrated)` for a budgeted kNN probe — see
/// [`SegmentReader::knn_search_within`]. `None` when the segment has no
/// lazy v2 vector index.
pub type BudgetedKnnResult = Option<Result<(Vec<(u32, f64)>, usize), KoshaError>>;

/// What a restricted, globally budgeted kNN probe does once its budget is
/// spent and it has found fewer than `k` accepted documents (see
/// [`SegmentReader::knn_search_within_filtered`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KnnRefill {
    /// Keep probing outward until `k` are found or the segment is read in
    /// full, even in a segment the budget chose no posting from. For a
    /// filter: the budget was sized without knowing where the filter's
    /// documents are, so any segment may hold the only ones that pass.
    #[default]
    Always,
    /// Keep probing only in a segment the budget chose at least one posting
    /// from. For a restriction that is deleted documents alone: a segment
    /// outside the global cutoff is skipped exactly as it would be with no
    /// deletes, instead of being probed for `k` hits just because it holds
    /// a tombstone.
    BudgetedSegments,
}

/// How much of a column a kNN probe reads (#381, see
/// [`SegmentReader::knn_candidates_fetching`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KnnProbeBudget<'a> {
    /// The global budget: postings whose centroid lower bound is within
    /// `max_distance`, at most `max_probes` of them (see
    /// [`SegmentReader::knn_search_within`]).
    Within {
        max_distance: f32,
        max_probes: usize,
        refill: KnnRefill,
    },
    /// [`Self::Within`] with this segment's postings already chosen and
    /// ranked by the query layer (#436): the segment neither scores its
    /// centroids nor sorts them again.
    Planned {
        ranking: &'a KnnSegmentRanking,
        refill: KnnRefill,
    },
    /// The per-segment fallback: the `nprobe` nearest centroids.
    Nprobe(usize),
}

/// One segment's share of a global kNN probe budget (#436): which of its
/// postings are inside the budget, nearest first.
///
/// The query layer already scores every centroid of every segment to pick
/// the global cutoff ([`SegmentReader::knn_centroid_distances`]). Built
/// from those same scores, this hands the segment its probe list, so the
/// probe does not score and sort the segment's centroids a second time.
/// The scores are kept: a restricted probe (a filter, deleted documents)
/// that runs past the budget ranks the rest from them, and only then.
///
/// The postings chosen, and their order, are exactly the ones
/// [`SegmentReader::knn_search_within_filtered_fetching`] picks from the
/// same `max_distance` and `max_probes`: ascending score, equal scores in
/// posting order, the first `max_probes` of those at or under
/// `max_distance`.
#[derive(Debug, Clone, PartialEq)]
pub struct KnnSegmentRanking {
    /// One rank score per posting, in posting order. `None` when the plan
    /// scored only the postings that could be inside the budget
    /// ([`Self::from_candidates`]): a restricted probe that runs past the
    /// budget then scores the segment's centroids itself.
    bounds: Option<Vec<f32>>,
    /// How many postings the segment's column has.
    postings: usize,
    /// The postings inside the budget, nearest first.
    budgeted: Vec<u32>,
    max_distance: f32,
    max_probes: usize,
}

impl KnnSegmentRanking {
    /// `bounds` is one segment's [`SegmentReader::knn_centroid_distances`]
    /// for the query; `max_distance` and `max_probes` are the global
    /// cutoff and cap, in the same units.
    pub fn new(bounds: Vec<f32>, max_distance: f32, max_probes: usize) -> Self {
        let budgeted = if bounds.iter().any(|d| d.is_nan()) {
            // A NaN score has no place in an order. Keep whatever the full
            // sort makes of it, as before #436, rather than define a
            // second behaviour for an input no real query produces.
            let ranked = Self::rank_all(&bounds);
            let within = ranked
                .iter()
                .take_while(|(_, d)| *d <= max_distance)
                .count()
                .min(max_probes);
            ranked[..within].iter().map(|&(i, _)| i).collect()
        } else {
            // Without NaN, "ascending score, ties in posting order" is a
            // total order on `(score, posting)`, so the first `max_probes`
            // under the cutoff can be selected without sorting the rest.
            let within: Vec<(f32, u32)> = bounds
                .iter()
                .enumerate()
                .filter(|(_, d)| **d <= max_distance)
                .map(|(i, &d)| (d, i as u32))
                .collect();
            Self::nearest(within, max_probes)
        };
        Self {
            postings: bounds.len(),
            bounds: Some(bounds),
            budgeted,
            max_distance,
            max_probes,
        }
    }

    /// [`Self::new`] for a segment of `postings` postings of which only
    /// `candidates` were scored: `(posting, score)` pairs, each score the
    /// value [`SegmentReader::knn_centroid_distances`] has for that
    /// posting.
    ///
    /// The budgeted postings are the ones [`Self::new`] picks from the
    /// segment's full scores provided `candidates` holds every posting
    /// whose score is at or under `max_distance`: postings over the cutoff
    /// are dropped by both, and among the rest both apply the same total
    /// order. A NaN score (which the score function never produces) is
    /// never at or under a cutoff, so it is dropped here rather than
    /// ordered; the caller plans from full scores if it ever sees one.
    pub fn from_candidates(
        postings: usize,
        candidates: &[(u32, f32)],
        max_distance: f32,
        max_probes: usize,
    ) -> Self {
        let within: Vec<(f32, u32)> = candidates
            .iter()
            .filter(|(_, d)| *d <= max_distance)
            .map(|&(i, d)| (d, i))
            .collect();
        Self {
            bounds: None,
            postings,
            budgeted: Self::nearest(within, max_probes),
            max_distance,
            max_probes,
        }
    }

    /// The first `max_probes` of `within` (`(score, posting)` pairs, no
    /// NaN) in ascending score, equal scores in posting order.
    fn nearest(mut within: Vec<(f32, u32)>, max_probes: usize) -> Vec<u32> {
        let by_rank = |a: &(f32, u32), b: &(f32, u32)| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.1.cmp(&b.1))
        };
        if within.len() > max_probes {
            if max_probes > 0 {
                within.select_nth_unstable_by(max_probes - 1, by_rank);
            }
            within.truncate(max_probes);
        }
        within.sort_unstable_by(by_rank);
        within.into_iter().map(|(_, i)| i).collect()
    }

    /// The postings inside the budget, nearest first.
    pub fn budgeted(&self) -> &[u32] {
        &self.budgeted
    }

    /// The rank score of every posting, in posting order; `None` for a
    /// ranking built from candidates only ([`Self::from_candidates`]).
    pub fn bounds(&self) -> Option<&[f32]> {
        self.bounds.as_deref()
    }

    /// How many postings the ranked column has.
    pub fn postings(&self) -> usize {
        self.postings
    }

    /// The cutoff the ranking was built for.
    pub fn max_distance(&self) -> f32 {
        self.max_distance
    }

    /// The cap the ranking was built for.
    pub fn max_probes(&self) -> usize {
        self.max_probes
    }

    /// Every posting, nearest first: [`Self::budgeted`] is its prefix.
    /// Only a restricted probe that outruns its budget needs this.
    fn rank_all(bounds: &[f32]) -> Vec<(u32, f32)> {
        let mut ranked: Vec<(u32, f32)> = bounds
            .iter()
            .enumerate()
            .map(|(i, &d)| (i as u32, d))
            .collect();
        ranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked
    }
}

/// One kNN candidate with its score estimate and error bound (#381): the
/// exact cosine similarity is (with the bound's confidence) within
/// `estimate +- error`. `error == 0` means the estimate is exact.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KnnCandidate {
    pub doc_seq: u32,
    pub estimate: f64,
    pub error: f64,
}

impl KnnCandidate {
    /// Lowest score the candidate can have.
    pub fn lower(&self) -> f64 {
        self.estimate - self.error
    }

    /// Highest score the candidate can have.
    pub fn upper(&self) -> f64 {
        self.estimate + self.error
    }
}

/// The `k`-th largest lower bound of `candidates`; `None` when there are
/// fewer than `k` (nothing can be ruled out) or `k == 0`.
pub fn kth_lower_bound(candidates: &[KnnCandidate], k: usize) -> Option<f64> {
    if k == 0 || candidates.len() < k {
        return None;
    }
    let mut lower: Vec<f64> = candidates.iter().map(KnnCandidate::lower).collect();
    let (_, kth, _) = lower.select_nth_unstable_by(k - 1, |a, b| {
        b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal)
    });
    Some(*kth)
}

/// [`SegmentReader::knn_candidates_fetching`]'s answer.
#[derive(Debug, Clone, Default)]
pub struct KnnCandidates {
    /// Best estimate first.
    pub candidates: Vec<KnnCandidate>,
    /// Postings hydrated and scored.
    pub postings: usize,
    /// The column is f32: every estimate is exact and needs no rescoring.
    pub exact: bool,
}

/// [`SegmentReader::knn_exact_scores_fetching`]'s answer.
#[derive(Debug, Clone, Default)]
pub struct ExactRescore {
    /// `(doc_seq, exact cosine similarity)` for each vector read, in no
    /// particular order.
    pub scores: Vec<(u32, f64)>,
    /// Requested documents whose exact vector could not be read.
    pub unavailable: usize,
    /// Why, when `unavailable > 0`.
    pub reason: Option<String>,
    /// The whole `doc_vectors.bin` was not local: vectors came from the
    /// span cache, after ranged reads where needed.
    pub ranged: bool,
    /// Spans handed to `fetch` (not already in the span cache).
    pub ranged_spans: usize,
}

/// What a probe scored. A quantized column: every candidate, before any
/// cut, with `errors[i]` the error bound of `scored[i]`'s score. An f32
/// column (exact scores, nothing to rescore): its best `k` only, best
/// first, with `errors` empty — see [`CandidateSink`].
struct ProbedCandidates {
    scored: Vec<(u32, f64)>,
    errors: Vec<f32>,
    postings: usize,
}

/// Where the time of a kNN probe's posting scans went (#435), summed over
/// the postings scanned inside one [`with_knn_scan_stats`] scope.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KnnScanStats {
    /// Reading posting bytes from local disk (idx file or span cache).
    pub read_nanos: u64,
    /// Decoding those bytes into ids and vectors (or codes). Zero for an
    /// f32 posting scored in place from the read buffer.
    pub decode_nanos: u64,
    /// Scoring entries against the query, candidate collection included.
    pub score_nanos: u64,
    /// Scoring and ranking the segment's own centroids for a budgeted
    /// probe (#436): zero when the query layer handed the probe its
    /// ranking and the probe never ran past its budget.
    pub rank_nanos: u64,
    /// Postings served from the segment's decoded-posting cache: no read,
    /// no decode.
    pub postings_cached: usize,
    /// Postings read and decoded into an owned posting (admitted to the
    /// cache, or a quantized column's).
    pub postings_decoded: usize,
    /// f32 postings read and scored from the probe's reused buffers
    /// (in place from the read buffer on little-endian targets), without
    /// building a posting.
    pub postings_scanned: usize,
}

impl KnnScanStats {
    fn add(&mut self, other: &KnnScanStats) {
        self.read_nanos += other.read_nanos;
        self.decode_nanos += other.decode_nanos;
        self.score_nanos += other.score_nanos;
        self.rank_nanos += other.rank_nanos;
        self.postings_cached += other.postings_cached;
        self.postings_decoded += other.postings_decoded;
        self.postings_scanned += other.postings_scanned;
    }
}

thread_local! {
    /// The collector of the innermost [`with_knn_scan_stats`] scope on this
    /// thread; `None` outside one (nothing is timed then).
    static KNN_SCAN_STATS: std::cell::Cell<Option<KnnScanStats>> =
        const { std::cell::Cell::new(None) };
}

/// Runs `f` and returns, with its result, the [`KnnScanStats`] of every
/// vector posting a [`SegmentReader`] scanned on this thread meanwhile.
/// A probe is synchronous on its caller's thread, so wrapping one
/// `knn_*` call measures exactly that call. Scopes nest: an inner scope's
/// postings are not counted by the outer one.
///
/// Costs three clock reads per posting scanned; outside a scope the scan
/// reads no clock at all.
pub fn with_knn_scan_stats<R>(f: impl FnOnce() -> R) -> (R, KnnScanStats) {
    let outer = KNN_SCAN_STATS.replace(Some(KnnScanStats::default()));
    let result = f();
    let stats = KNN_SCAN_STATS.replace(outer).unwrap_or_default();
    (result, stats)
}

/// One probe's reusable state for scanning postings of one column of one
/// segment (#435): the idx file (or span files) opened once, one byte
/// buffer every posting is read into, and one decode scratch. Dropped
/// when the probe returns, so none of it outlives the segment visit.
struct PostingScan<'a> {
    vi: &'a LazyVectorIndex,
    probe: ProbeQuery<'a>,
    accept: Option<&'a dyn Fn(u32) -> bool>,
    reader: span_cache::LocalRangeReader,
    /// Decode scratch for an f32 posting that is not kept and cannot be
    /// scored in place (a big-endian target).
    ids: Vec<u32>,
    vectors: Vec<f32>,
    stats: KnnScanStats,
    /// Inside a [`with_knn_scan_stats`] scope: time the phases.
    timed: bool,
}

impl<'a> PostingScan<'a> {
    fn new(
        segment_dir: &Path,
        vi: &'a LazyVectorIndex,
        probe: ProbeQuery<'a>,
        accept: Option<&'a dyn Fn(u32) -> bool>,
    ) -> Self {
        let timed = KNN_SCAN_STATS.with(|stats| stats.get().is_some());
        Self {
            vi,
            probe,
            accept,
            reader: span_cache::LocalRangeReader::new(segment_dir, &vi.idx_file),
            ids: Vec::new(),
            vectors: Vec::new(),
            stats: KnnScanStats::default(),
            timed,
        }
    }

    fn clock(&self) -> Option<std::time::Instant> {
        self.timed.then(std::time::Instant::now)
    }
}

/// Nanoseconds since `since`, which moves to now; `0` when not timing.
fn lap_nanos(since: &mut Option<std::time::Instant>) -> u64 {
    let Some(start) = since else {
        return 0;
    };
    let now = std::time::Instant::now();
    let nanos = now.duration_since(*start).as_nanos() as u64;
    *start = now;
    nanos
}

impl Drop for PostingScan<'_> {
    fn drop(&mut self) {
        if !self.timed {
            return;
        }
        KNN_SCAN_STATS.with(|stats| {
            if let Some(mut total) = stats.get() {
                total.add(&self.stats);
                stats.set(Some(total));
            }
        });
    }
}

/// A running top-`k` entry. Ordered worst-first for a max-heap: a lower
/// score is worse, and of two equal scores the later-seen one is.
struct TopKEntry {
    score: f64,
    seq: u64,
    id: u32,
}

impl PartialEq for TopKEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for TopKEntry {}

impl PartialOrd for TopKEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopKEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.seq.cmp(&other.seq))
    }
}

/// Collects a probe's candidates as they are scored.
///
/// A quantized column keeps every candidate with its error bound: the
/// rescoring shortlist is chosen from all of them. An f32 column's scores
/// are exact and every caller cuts them to the best `k`, so only a running
/// top-`k` is kept (#435) — a probe of 30 postings no longer builds and
/// sorts a few thousand `(id, score)` pairs to keep ten.
///
/// The running top-`k` is exactly what a stable descending sort of all
/// candidates followed by `truncate(k)` selects, in the same order: of two
/// equal scores the one scored first wins, and a candidate that ties the
/// current `k`-th does not displace it.
struct CandidateSink {
    k: usize,
    top: Option<std::collections::BinaryHeap<TopKEntry>>,
    scored: Vec<(u32, f64)>,
    errors: Vec<f32>,
    count: usize,
}

impl CandidateSink {
    fn new(vi: &LazyVectorIndex, k: usize) -> Self {
        Self::with_running_top_k(k, !vi.quantized)
    }

    fn with_running_top_k(k: usize, running: bool) -> Self {
        Self {
            k,
            // Not `with_capacity(k)`: `k` is caller-supplied.
            top: running.then(std::collections::BinaryHeap::new),
            scored: Vec::new(),
            errors: Vec::new(),
            count: 0,
        }
    }

    fn push(&mut self, id: u32, similarity: f32, error: f32) {
        let score = similarity as f64;
        let seq = self.count as u64;
        self.count += 1;
        let Some(top) = self.top.as_mut() else {
            self.scored.push((id, score));
            self.errors.push(error);
            return;
        };
        if top.len() < self.k {
            top.push(TopKEntry { score, seq, id });
        } else if let Some(mut worst) = top.peek_mut() {
            if score > worst.score {
                *worst = TopKEntry { score, seq, id };
            }
        }
    }

    /// Candidates scored so far, whether or not they were kept.
    fn len(&self) -> usize {
        self.count
    }

    /// `(scored, errors)` as [`ProbedCandidates`] documents them.
    fn finish(self) -> (Vec<(u32, f64)>, Vec<f32>) {
        match self.top {
            Some(top) => (
                top.into_sorted_vec()
                    .into_iter()
                    .map(|entry| (entry.id, entry.score))
                    .collect(),
                Vec::new(),
            ),
            None => (self.scored, self.errors),
        }
    }
}

/// One stored document plus its sidecar vectors, as yielded by
/// [`SegmentReader::iter_doc_records_raw`]. `record`'s sidecar `Vector`
/// fields keep their empty placeholder value; `vectors` holds each one's
/// value as `(index into record.fields, vector)`.
#[derive(Debug, Clone)]
pub struct RawDocRecord {
    pub record: DocRecord,
    pub vectors: Vec<(usize, Vec<f32>)>,
}

/// Cap on a RaBitQ probe's bound-driven rescoring shortlist, as a multiple
/// of the `k * vector_rescore_factor` list every quantized column rescores.
/// Bounds widen on a posting with loose residuals; this keeps one such
/// segment from reading half its `doc_vectors.bin` for a single query.
const RABITQ_SHORTLIST_CAP: usize = 8;

/// How many of `scored` RaBitQ's error bounds cannot rule out of the top
/// `k`: candidates whose upper bound (`score + error`) reaches the `k`-th
/// largest lower bound (`score - error`). Everything when there are at
/// most `k` candidates.
fn rabitq_bound_shortlist_len(scored: &[(u32, f64)], errors: &[f32], k: usize) -> usize {
    if k == 0 || scored.len() <= k {
        return scored.len();
    }
    let mut lower: Vec<f64> = scored
        .iter()
        .zip(errors)
        .map(|(&(_, s), &e)| s - e as f64)
        .collect();
    let (_, kth, _) = lower.select_nth_unstable_by(k - 1, |a, b| {
        b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal)
    });
    let threshold = *kth;
    scored
        .iter()
        .zip(errors)
        .filter(|(&(_, s), &e)| s + e as f64 >= threshold)
        .count()
}

/// The `k` highest-scoring of `scored`, best first. Which of several
/// candidates that tie at the `k`-th score are kept, and the order of equal
/// scores, is whatever the unstable select leaves.
///
/// Given an f32 column's [`CandidateSink`] output (at most `k` candidates,
/// already best first) it changes nothing, so on those paths ties are the
/// sink's: first scored wins. Before #435 those paths passed every score
/// through the select, so among exactly equal scores at the cut they could
/// return other ids — same scores, different duplicates.
fn top_k_by_score(mut scored: Vec<(u32, f64)>, k: usize) -> Vec<(u32, f64)> {
    let by_score_desc =
        |a: &(u32, f64), b: &(u32, f64)| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal);
    if k == 0 {
        return Vec::new();
    }
    if scored.len() > k {
        scored.select_nth_unstable_by(k - 1, by_score_desc);
        scored.truncate(k);
    }
    scored.sort_by(by_score_desc);
    scored
}

/// Where [`SegmentReader::knn_exact_filtered`] read the candidates'
/// vectors from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactKnnSource {
    /// `doc_vectors.bin` (#243): one read per run of nearby candidates.
    Sidecar,
    /// Every posting of the column's `vector.idx`, because the segment has
    /// no local sidecar holding the field (inline-vector segments, a
    /// legacy index the sidecar does not bind, or a sidecar not hydrated
    /// locally).
    Postings,
}

/// Outcome of an exact, filtered kNN over one segment (#252).
#[derive(Debug, Clone)]
pub struct ExactKnnResult {
    /// The `k` nearest candidates, nearest first, scored by cosine
    /// similarity exactly as the posting probe scores them.
    pub hits: Vec<(u32, f64)>,
    /// Candidate vectors scored.
    pub vectors_scored: usize,
    /// Postings hydrated (zero when served from the sidecar).
    pub postings_hydrated: usize,
    pub source: ExactKnnSource,
}

pub struct SegmentReader {
    segment_dir: PathBuf,
    footer: Footer,
    doc_store: DocStoreAccess,
    inverted: InvertedAccess,
    filters: LazyFilters,
    /// The segment's vector columns (#204), empty unless opened with
    /// `load_vectors`. Each is loaded on first use.
    vector_slots: Vec<VectorFieldSlot>,
    /// Decoded vector postings LRU: the process-wide
    /// [`shared_vector_cache`], keyed by `(vector_cache_owner, slot, range
    /// index)` — the vector twin of the text path's per-segment
    /// [`PostingsCache`]. Without it every kNN query re-reads and
    /// re-decodes its `nprobe` candidate postings from `vector.idx`, even
    /// when the segment itself is warm in the `SegmentCache` — hydration
    /// I/O + decode was the dominant warm-path cost. Entries are `Arc`s
    /// (hit = pointer clone) and resident bytes report to
    /// `postings_account`, the same live-bytes account as text postings.
    /// This segment's entries leave the cache when it drops.
    vector_cache: Arc<VectorPostingsCache>,
    /// This reader's part of the vector cache's key, unique per process.
    vector_cache_owner: u64,
    /// Live-bytes account this segment's vector cache entries report to.
    postings_account: Option<SharedAccount>,
    /// Parsed `doc_vectors.bin` header, loaded on first doc-record read of a
    /// segment whose footer sets `doc_vectors` (#243). Only success is
    /// cached: the file may be hydrated after the segment was opened.
    doc_vectors: Mutex<Option<Arc<DocVectors>>>,
    /// Per-field document lengths decoded from the norms lists (#249),
    /// dense by doc_seq, for the fields queries have asked about.
    field_lengths: Mutex<HashMap<String, Arc<Vec<u32>>>>,
    /// i8 columns rescore their best `k * vector_rescore_factor` probe
    /// candidates at full precision (#210); `0` disables. See
    /// [`vector_rescore_factor`].
    vector_rescore_factor: usize,
    /// Late-interaction fields (#205) opened so far, by footer index. Only
    /// success is cached, as for `doc_vectors`.
    multi_vectors: Mutex<HashMap<usize, Arc<multivec::MultiVectorIndex>>>,
    /// Decoded `.ivf` postings of every late-interaction field, keyed by
    /// `(field index, posting)` — the late-interaction twin of
    /// `vector_cache`, budgeted by
    /// `KOSHA_LATE_INTERACTION_POSTINGS_CACHE_MAX_BYTES`.
    multi_vector_cache: multivec::IvfPostingCache,
}

/// Default of [`SegmentReader::with_vector_rescore_factor`]:
/// `KOSHA_VECTOR_RESCORE_FACTOR`, default `4`, `0` disables rescoring.
/// Only i8-quantized vector columns (#210) are rescored — an f32 column's
/// probe scores are already exact.
pub fn vector_rescore_factor() -> usize {
    static FACTOR: OnceLock<usize> = OnceLock::new();
    *FACTOR.get_or_init(|| {
        match std::env::var("KOSHA_VECTOR_RESCORE_FACTOR").ok().as_deref().map(str::trim) {
            None | Some("") => DEFAULT_VECTOR_RESCORE_FACTOR,
            Some(raw) => raw.parse().unwrap_or_else(|_| {
                eprintln!(
                    "WARN: KOSHA_VECTOR_RESCORE_FACTOR={raw} not a non-negative integer — using {DEFAULT_VECTOR_RESCORE_FACTOR}"
                );
                DEFAULT_VECTOR_RESCORE_FACTOR
            }),
        }
    })
}

const DEFAULT_VECTOR_RESCORE_FACTOR: usize = 4;

/// Decoded vector postings of every open segment, under one budget (see
/// [`vector_postings_cache_max_bytes`]).
type VectorPostingsCache = PostingsCache<(u64, usize), SharedVectorPosting>;

/// The process-wide vector postings cache every [`SegmentReader`] uses.
fn shared_vector_cache() -> Arc<VectorPostingsCache> {
    static CACHE: OnceLock<Arc<VectorPostingsCache>> = OnceLock::new();
    Arc::clone(
        CACHE.get_or_init(|| Arc::new(PostingsCache::new(vector_postings_cache_max_bytes(), None))),
    )
}

/// A new [`SegmentReader::vector_cache_owner`].
fn next_vector_cache_owner() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

impl SegmentReader {
    /// Vector cache key for `range_idx` of the column at `slot`.
    fn vector_cache_key(&self, slot: u32, range_idx: usize) -> (u64, usize) {
        (
            self.vector_cache_owner,
            ((slot as u64) << 32 | range_idx as u64) as usize,
        )
    }
}

impl Drop for SegmentReader {
    fn drop(&mut self) {
        // The shared cache outlives this reader: take its entries (and
        // their live bytes) out with it. O(entries), so skipped for the
        // (many) readers opened without vectors, which never insert.
        if self.vector_slots.is_empty() {
            return;
        }
        let owner = self.vector_cache_owner;
        self.vector_cache.remove_where(|(o, _)| *o == owner);
    }
}

impl SegmentReader {
    /// Opens a segment with vector search enabled. Kept for callers that
    /// genuinely need the vectors regardless of query shape
    /// (merge/compaction, tests). Query-time lexical search should use
    /// `open_with_options` instead — see its doc comment.
    pub fn open(segment_dir: PathBuf) -> Result<Self, KoshaError> {
        Self::open_with_options(segment_dir, true)
    }

    /// Opens a segment, enabling vector search only when `load_vectors` is
    /// true.
    ///
    /// Vector files are the dominant cost of opening a segment, yet a pure
    /// lexical/BM25 query (no `query.knn`) never touches them. A segment
    /// opened without `load_vectors` reports no vector columns at all; one
    /// opened with it loads each column on the first kNN that targets it.
    pub fn open_with_options(segment_dir: PathBuf, load_vectors: bool) -> Result<Self, KoshaError> {
        Self::open_with_footer_options(segment_dir, load_vectors, None, None)
    }

    /// Opens a segment with an already-known footer. Search can source the
    /// footer from the manifest and avoid hydrating `footer.json` on cold
    /// S3 reads; non-search callers keep using `open_with_options`.
    ///
    /// `postings_account` is an optional live-bytes sink the segment's
    /// decoded-postings cache reports its resident bytes to (Tier 1 O6) —
    /// the query path passes a handle to its `MemoryLedger` so aggregate
    /// decoded-postings memory counts toward the admission watermark; admin
    /// paths (merge/compaction/tests) pass `None` and the per-segment cap
    /// alone bounds the cache.
    pub fn open_with_footer_options(
        segment_dir: PathBuf,
        load_vectors: bool,
        footer: Option<Footer>,
        postings_account: Option<Arc<dyn PostingsMemoryAccount + Send + Sync>>,
    ) -> Result<Self, KoshaError> {
        let footer = resolve_open_footer(&segment_dir, footer)?;
        let posting_spans = footer.posting_blobs_raw;
        let filters = LazyFilters {
            segment_dir: segment_dir.clone(),
            doc_count: footer.doc_count,
            columnar: footer.filter_columns.is_some(),
            state: Mutex::new(FilterState::default()),
        };
        let vector_slots = if load_vectors {
            Self::vector_slots(&segment_dir, &footer)
        } else {
            Vec::new()
        };
        let doc_store = match &footer.doc_store_compression {
            None => match try_read_doc_index(&segment_dir, footer.doc_count) {
                Some(index) => DocStoreAccess::Lazy {
                    doc_store_path: segment_dir.join("doc_store.bin"),
                    index,
                    decoder: None,
                },
                None => DocStoreAccess::Eager(Self::read_doc_store_for_fallback(&segment_dir)?),
            },
            Some(compression) => {
                Self::open_compressed_doc_store(&segment_dir, &footer, compression)?
            }
        };
        Ok(Self {
            segment_dir: segment_dir.clone(),
            footer,
            doc_store,
            inverted: Self::read_inverted(&segment_dir, postings_account.clone(), posting_spans)?,
            filters,
            vector_slots,
            vector_cache: shared_vector_cache(),
            vector_cache_owner: next_vector_cache_owner(),
            postings_account: postings_account.clone(),
            doc_vectors: Mutex::new(None),
            field_lengths: Mutex::new(HashMap::new()),
            vector_rescore_factor: vector_rescore_factor(),
            multi_vectors: Mutex::new(HashMap::new()),
            multi_vector_cache: PostingsCache::new(
                late_interaction_postings_cache_max_bytes(),
                postings_account,
            ),
        })
    }

    /// A compressed doc store (research #388) opens only lazily: the
    /// eager full parse cannot read frames. Its index and dictionary come
    /// from `doc_store.offsets`. When that sidecar is missing or broken
    /// (bad entries, bad dictionary trailer, or a dictionary length that
    /// disagrees with the footer) and `doc_store.bin` is local, the index
    /// is rebuilt in memory from `doc_store.bin` and its dictionary tail
    /// copy: slower, still correct, as the eager fallback is for a raw
    /// doc store. An unknown codec, or a broken sidecar with no local
    /// `doc_store.bin` (or a damaged one), is an error.
    fn open_compressed_doc_store(
        segment_dir: &Path,
        footer: &Footer,
        compression: &kosha_core::DocStoreCompression,
    ) -> Result<DocStoreAccess, KoshaError> {
        let corrupt = |why: &str| {
            KoshaError::CorruptSegment(format!(
                "{}: compressed doc store ({}): {why}",
                segment_dir.display(),
                compression.codec
            ))
        };
        if compression.codec != kosha_core::DocStoreCompression::ZSTD_DICT {
            return Err(corrupt(
                "unknown codec; this binary is too old for the segment",
            ));
        }
        let index_and_dict = |parts: Option<(DocIndex, Vec<u8>)>| {
            let (index, trailer) = parts?;
            let dict = parse_dict_trailer(&trailer)?;
            (dict.len() == compression.dict_bytes as usize).then_some((index, dict))
        };
        let from_sidecar = index_and_dict(try_read_doc_index_parts(segment_dir, footer.doc_count));
        let (index, dict) = match from_sidecar {
            Some(found) => found,
            None => {
                let store_path = segment_dir.join(DOC_STORE_FILE);
                if !store_path.is_file() {
                    return Err(corrupt(
                        "doc_store.offsets (or its dictionary trailer) is missing or malformed, \
                         and doc_store.bin is not local to rebuild it from",
                    ));
                }
                eprintln!(
                    "WARN: {}: doc_store.offsets of a compressed doc store is missing or \
                     malformed; rebuilding it in memory from doc_store.bin",
                    segment_dir.display()
                );
                let rebuilt = rebuild_offsets(
                    &fs::read(&store_path)?,
                    footer.doc_count,
                    compression.dict_bytes,
                )?;
                index_and_dict(parse_doc_index_bytes(
                    &rebuilt,
                    &store_path,
                    footer.doc_count,
                ))
                .ok_or_else(|| corrupt("offsets rebuilt from doc_store.bin do not parse"))?
            }
        };
        Ok(DocStoreAccess::Lazy {
            doc_store_path: segment_dir.join("doc_store.bin"),
            index,
            decoder: Some(Arc::new(DocStoreDecoder::new(dict))),
        })
    }

    /// Whether this segment's `doc_store.bin` is compressed (research
    /// #388; `Footer::doc_store_compression`).
    pub fn doc_store_compressed(&self) -> bool {
        self.footer.doc_store_compression.is_some()
    }

    /// Bytes of the compressed doc store's dictionary held resident (0 for
    /// a raw doc store).
    pub fn doc_store_dict_bytes(&self) -> usize {
        match &self.doc_store {
            DocStoreAccess::Lazy {
                decoder: Some(d), ..
            } => d.dict_len(),
            _ => 0,
        }
    }

    /// Override how many probe candidates per wanted hit an i8 vector
    /// column rescores at full precision (#210); `0` disables rescoring.
    /// Readers default to `KOSHA_VECTOR_RESCORE_FACTOR` (see
    /// [`vector_rescore_factor`]).
    pub fn with_vector_rescore_factor(mut self, factor: usize) -> Self {
        self.vector_rescore_factor = factor;
        self
    }

    /// Bytes the decoded vector-posting cache holds right now (#210
    /// measurement; also what it reports to the live-bytes account).
    pub fn vector_cache_resident_bytes(&self) -> usize {
        let owner = self.vector_cache_owner;
        self.vector_cache.resident_bytes_where(|(o, _)| *o == owner)
    }

    /// Override the byte budget of the decoded late-interaction posting
    /// cache (#205); `0` disables it. Readers default to
    /// `KOSHA_LATE_INTERACTION_POSTINGS_CACHE_MAX_BYTES`. Call before the
    /// first search: whatever the cache already holds is dropped.
    pub fn with_late_interaction_postings_cache_max_bytes(mut self, max_bytes: usize) -> Self {
        let account = self.multi_vector_cache.account.clone();
        self.multi_vector_cache = PostingsCache::new(max_bytes, account);
        self
    }

    /// Bytes the decoded late-interaction posting cache holds right now
    /// (#205; also what it reports to the live-bytes account).
    pub fn late_interaction_cache_resident_bytes(&self) -> usize {
        self.multi_vector_cache.state.lock().unwrap().total_bytes
    }

    /// The vector columns `footer` declares, or the single unnamed legacy
    /// index of a segment written before columns existed. No I/O.
    fn vector_slots(segment_dir: &Path, footer: &Footer) -> Vec<VectorFieldSlot> {
        let slot = |meta: Option<VectorFieldMeta>, slot: u32| VectorFieldSlot {
            idx_path: segment_dir.join(kosha_core::vector_idx_file(slot)),
            offsets_path: segment_dir.join(kosha_core::vector_offsets_file(slot)),
            meta,
            loaded: Mutex::new(None),
        };
        match &footer.vector_fields {
            Some(fields) => fields
                .iter()
                .map(|meta| slot(Some(meta.clone()), meta.slot))
                .collect(),
            None => vec![slot(None, 0)],
        }
    }

    /// Whether this segment has late-interaction data for `field` (#205).
    pub fn has_multi_vector_field(&self, field: &str) -> bool {
        self.footer.multi_vectors.iter().any(|f| f.name == field)
    }

    /// Late-interaction (MaxSim) top `k` over `field` (#205). See
    /// [`multivec`] for the files and the algorithm. A segment without
    /// late-interaction data for `field` returns no hits; query tokens of a
    /// different dimension than the field's are an `InvalidQuery`.
    pub fn late_interaction_search(
        &self,
        field: &str,
        query: &[&[f32]],
        k: usize,
        params: LateInteractionParams,
        accept: Option<&dyn Fn(u32) -> bool>,
    ) -> Result<LateInteractionHits, KoshaError> {
        let Some(i) = self
            .footer
            .multi_vectors
            .iter()
            .position(|f| f.name == field)
        else {
            return Ok(LateInteractionHits::default());
        };
        let index = {
            let mut cached = self.multi_vectors.lock().unwrap();
            match cached.get(&i) {
                Some(index) => Arc::clone(index),
                None => {
                    let index = Arc::new(multivec::MultiVectorIndex::open(
                        &self.segment_dir,
                        i,
                        &self.footer.multi_vectors[i],
                    )?);
                    cached.insert(i, Arc::clone(&index));
                    index
                }
            }
        };
        if let Some(bad) = query.iter().find(|q| q.len() != index.dim()) {
            return Err(KoshaError::InvalidQuery(format!(
                "knn field '{field}' holds {}-dimensional token vectors; a query vector has {}",
                index.dim(),
                bad.len()
            )));
        }
        let normalized: Vec<Vec<f32>> = query
            .iter()
            .map(|q| {
                let mut q = q.to_vec();
                kosha_vector_spfresh::normalize_in_place(&mut q);
                q
            })
            .collect();
        index.search(&normalized, k, params, accept, &self.multi_vector_cache)
    }

    fn doc_vectors(&self) -> Result<Arc<DocVectors>, KoshaError> {
        let mut cached = self.doc_vectors.lock().unwrap();
        if let Some(dv) = cached.as_ref() {
            return Ok(Arc::clone(dv));
        }
        let path = self.segment_dir.join(DOC_VECTORS_FILE);
        let missing = || {
            KoshaError::NotFound(format!(
                "{DOC_VECTORS_FILE} missing from segment {:?}, whose footer says vectors \
                 are stored there",
                self.segment_dir
            ))
        };
        let dv = match DocVectors::open(path) {
            Ok(dv) => dv,
            // No whole file: the header may be in the sparse span cache
            // (#245), fetched by a ranged read for page materialization.
            Err(KoshaError::Io(io)) if io.kind() == std::io::ErrorKind::NotFound => {
                let prefix = span_cache::SpanIndex::load(&self.segment_dir, DOC_VECTORS_FILE)
                    .read_prefix()
                    .ok_or_else(missing)?;
                DocVectors::from_prefix(self.segment_dir.clone(), &prefix)?.ok_or_else(missing)?
            }
            Err(other) => return Err(other),
        };
        let dv = Arc::new(dv);
        *cached = Some(Arc::clone(&dv));
        Ok(dv)
    }

    /// The sidecar header if it is available locally (whole file or cached
    /// header span), without failing when it is not. `None` for segments
    /// that store vectors inline.
    fn local_doc_vectors(&self) -> Option<Arc<DocVectors>> {
        if !self.footer.doc_vectors {
            return None;
        }
        self.doc_vectors().ok()
    }

    /// Put sidecar vectors back into the placeholder (empty-valued)
    /// `Vector` fields of `record` (#243), as JSON. No-op for segments that
    /// store vectors inline.
    fn fill_doc_vectors(&self, record: &mut DocRecord) -> Result<(), KoshaError> {
        for (index, vector) in self.sidecar_vectors(record)? {
            record.fields[index].value = serde_json::to_string(&vector)?;
        }
        Ok(())
    }

    /// The sidecar vectors of `record`'s placeholder (empty-valued)
    /// `Vector` fields, as `(field index, vector)`; empty for segments that
    /// store vectors inline. A placeholder with no matching sidecar entry
    /// is corruption and fails loudly: returning the empty value would let
    /// a compaction write the document out without its vector.
    fn sidecar_vectors(&self, record: &DocRecord) -> Result<Vec<(usize, Vec<f32>)>, KoshaError> {
        self.sidecar_vectors_reusing(record, &mut None)
    }

    /// [`Self::sidecar_vectors`] keeping the span listing in `spans`.
    fn sidecar_vectors_reusing(
        &self,
        record: &DocRecord,
        spans: &mut Option<span_cache::SpanIndex>,
    ) -> Result<Vec<(usize, Vec<f32>)>, KoshaError> {
        let is_placeholder = |f: &Field| f.field_type == FieldType::Vector && f.value.is_empty();
        if !self.footer.doc_vectors || !record.fields.iter().any(is_placeholder) {
            return Ok(Vec::new());
        }
        let dv = self.doc_vectors()?;
        let mut stored = dv.vectors_for_reusing(record.doc_seq, spans)?.into_iter();
        let mut out = Vec::new();
        for (index, field) in record.fields.iter().enumerate() {
            if !is_placeholder(field) {
                continue;
            }
            match stored.next() {
                Some((name, vector)) if name == field.name => out.push((index, vector)),
                _ => {
                    return Err(KoshaError::CorruptSegment(format!(
                        "{DOC_VECTORS_FILE} in {:?} has no vector for doc_seq {} field '{}'",
                        self.segment_dir, record.doc_seq, field.name
                    )))
                }
            }
        }
        Ok(out)
    }

    /// The field a legacy segment's unnamed index belongs to, when that is
    /// knowable: its `doc_vectors.bin` (#243) names exactly one vector
    /// field. `None` — unbound — when the segment stores vectors inline,
    /// names several fields (their vectors share the one index), or the
    /// sidecar is not local.
    fn legacy_bound_field(&self) -> Option<String> {
        if !self.footer.doc_vectors {
            return None;
        }
        let dv = self.doc_vectors().ok()?;
        match dv.field_names() {
            [only] => Some(only.clone()),
            _ => None,
        }
    }

    /// The slot that serves a kNN on `field`, if any (#204): the footer's
    /// column of that name, or — migration rule — a legacy segment's
    /// unnamed index, unless its `doc_vectors.bin` binds it to a different
    /// field.
    fn vector_slot(&self, field: &str) -> Option<&VectorFieldSlot> {
        let slot = self.vector_slots.iter().find(|s| match &s.meta {
            Some(meta) => meta.name == field,
            None => true,
        })?;
        if slot.meta.is_none() {
            if let Some(bound) = self.legacy_bound_field() {
                if bound != field {
                    return None;
                }
            }
        }
        Some(slot)
    }

    /// How this segment relates to a kNN on `field` — see
    /// [`VectorFieldStatus`]. No I/O for a segment whose footer lists its
    /// vector columns; a legacy segment's unnamed index is loaded (once).
    /// A segment opened without `load_vectors` reports
    /// [`VectorFieldStatus::Absent`] for everything.
    pub fn vector_field_status(&self, field: &str) -> VectorFieldStatus {
        let Some(slot) = self.vector_slot(field) else {
            return VectorFieldStatus::Absent;
        };
        if let Some(meta) = &slot.meta {
            return VectorFieldStatus::Indexed {
                dim: meta.dim as usize,
            };
        }
        // Legacy: load the unnamed index (what the kNN itself will use
        // next, so this costs nothing extra) to learn whether it holds any
        // vectors and of what dimension. A segment that never had a vector
        // field has no `vector.idx` and loads empty.
        match self.load_slot(slot) {
            Ok(index) => match index.dim() {
                Some(dim) => VectorFieldStatus::Legacy { dim: Some(dim) },
                None if index.degraded() => VectorFieldStatus::Legacy { dim: None },
                None => VectorFieldStatus::Absent,
            },
            Err(_) => VectorFieldStatus::Legacy { dim: None },
        }
    }

    /// Names of the vector columns this segment's footer declares (empty
    /// for a legacy segment, or one opened without `load_vectors`).
    pub fn vector_field_names(&self) -> Vec<&str> {
        self.vector_slots
            .iter()
            .filter_map(|s| s.meta.as_ref().map(|m| m.name.as_str()))
            .collect()
    }

    /// The loaded index for `field` (#204), loading it on first use.
    /// `Ok(None)` when this segment has no vectors for `field` (see
    /// [`Self::vector_field_status`]).
    pub fn vector_field(&self, field: &str) -> Result<Option<Arc<VectorFieldIndex>>, KoshaError> {
        match self.vector_slot(field) {
            Some(slot) => self.load_slot(slot).map(Some),
            None => Ok(None),
        }
    }

    /// The segment's first vector column — what the field-less kNN methods
    /// below search. Single-column segments (and every legacy segment)
    /// have only that one.
    fn default_vector_field(&self) -> Option<Arc<VectorFieldIndex>> {
        let slot = self.vector_slots.first()?;
        match self.load_slot(slot) {
            Ok(loaded) => Some(loaded),
            Err(e) => {
                eprintln!(
                    "WARN: failed to load the vector index of {}: {e}",
                    self.segment_dir.display()
                );
                None
            }
        }
    }

    fn load_slot(&self, slot: &VectorFieldSlot) -> Result<Arc<VectorFieldIndex>, KoshaError> {
        let mut loaded = slot.loaded.lock().unwrap();
        if let Some(loaded) = loaded.as_ref() {
            return Ok(Arc::clone(loaded));
        }
        let index = Arc::new(Self::load_vector_index(
            &slot.idx_path,
            &slot.offsets_path,
            slot.meta.as_ref().map_or(0, |m| m.slot),
            slot.meta.as_ref().map(|m| m.name.clone()),
        )?);
        // A declared column that is neither local nor servable from its
        // offsets sidecar alone (S3 hydration fetches only the queried
        // column, and a ranged query node only its `vector[.<n>].offsets`)
        // is not cached as empty: it may be hydrated after this segment was
        // opened.
        if !index.has_lazy_index() && !slot.idx_path.exists() {
            if slot.meta.is_some() {
                return Err(KoshaError::NotFound(format!(
                    "{} missing from segment {:?}, whose footer lists it",
                    slot.idx_path.display(),
                    self.segment_dir
                )));
            }
            // A legacy (unnamed) slot with neither file local is served as
            // empty but not cached either: a segment that never had vectors
            // costs two failed stats per load, while one whose vector files
            // simply were not hydrated yet sees them once they are.
            return Ok(index);
        }
        *loaded = Some(Arc::clone(&index));
        Ok(index)
    }

    /// Loads one vector column, dispatching on its idx file's format — see
    /// the "Vector index format" section's four-way breakdown above
    /// `VECTOR_MAGIC`. At most one of `lazy` or `flat` is populated (both
    /// empty for "no vectors").
    fn load_vector_index(
        idx_path: &Path,
        offsets_path: &Path,
        slot: u32,
        field: Option<String>,
    ) -> Result<VectorFieldIndex, KoshaError> {
        let empty = |degraded| VectorFieldIndex {
            lazy: None,
            flat: VectorStore::default(),
            degraded,
        };
        // Read only the header first — `vector.idx` is hundreds of MB to
        // GBs per segment at embedding scale, and the common case below
        // (valid v2 header + a good `vector.offsets` sidecar) never needs a
        // byte past this. `.take(HEADER_LEN).read_to_end(..)` rather than
        // `read_exact`: a genuinely short file must yield fewer than
        // HEADER_LEN bytes without erroring, so `parse_vector_idx_header`'s
        // own `data.len() < VECTOR_IDX_HEADER_LEN` check still routes it to
        // the legacy fallback exactly as it did when this read the whole
        // file. A missing file (open error) is not an error either — matches
        // `read_vectors`'s existing missing-file handling.
        let Some(header_buf) = read_vector_idx_header(idx_path) else {
            // No local idx file. A query node reading postings by range
            // (#245) only holds the column's offsets sidecar, whose header
            // repeats the index's dim and version, plus whichever postings
            // it fetched into the span cache.
            return Ok(
                match Self::load_vector_index_from_offsets(idx_path, offsets_path, slot, field) {
                    Some(vi) => VectorFieldIndex {
                        lazy: Some(vi),
                        flat: VectorStore::default(),
                        degraded: false,
                    },
                    None => empty(false),
                },
            );
        };

        // A v2 magic with a version this build does not know (written by a
        // newer build) must not be parsed as the magic-less legacy format,
        // which would read the magic as a dimension. Fail the column
        // loudly instead; the query layer counts it as degraded.
        if header_buf.len() >= 8 && header_buf[0..4] == VECTOR_MAGIC.to_le_bytes() {
            let version = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());
            if !is_known_vector_index_version(version) {
                return Err(KoshaError::CorruptSegment(format!(
                    "{} has vector index version {version}, which this build cannot read",
                    idx_path.display()
                )));
            }
        }
        let Some((dim, _total_count, version)) = parse_vector_idx_header(&header_buf) else {
            // Short (<HEADER_LEN) file, or no magic/an unrecognized version
            // — legacy v1 flat format. Serve it with exact flat_knn: ANN
            // structures are built at WRITE time only. Constructing HNSW
            // here put a rayon all-core, minutes-long build on the open
            // path (issue #126: >10min for one query at 1M docs, and
            // admission control turned the stall into 100% 429s), while
            // exact scan of a flush-sized segment is milliseconds — and
            // strictly better recall.
            return Ok(VectorFieldIndex {
                lazy: None,
                flat: Self::read_vectors(idx_path)?,
                degraded: false,
            });
        };

        // Valid v2 header. Prefer the lazy path — built entirely from the
        // small `vector.offsets` sidecar, `vector.idx`'s body is never read
        // on this branch. Fall back to a full eager parse of the (already
        // self-describing) blob if the sidecar is missing or fails its own
        // sanity checks — search still works, just without the
        // lazy-hydration benefit, rather than losing vector search for the
        // column entirely.
        if let Ok(offsets_data) = fs::read(offsets_path) {
            if let Some(parsed) = parse_vector_offsets(&offsets_data, dim, version) {
                return Ok(VectorFieldIndex {
                    lazy: Some(LazyVectorIndex::from_parsed(
                        idx_path, slot, dim, version, parsed, field,
                    )),
                    flat: VectorStore::default(),
                    degraded: false,
                });
            }
            eprintln!(
                "WARN: {} present but failed validation — falling back to eager {} parse \
                 (no lazy hydration for this column)",
                offsets_path.display(),
                idx_path.display()
            );
        }

        eprintln!(
            "WARN: no usable {} — exact scan of the whole {}",
            offsets_path.display(),
            idx_path.display()
        );
        // Sidecar missing or invalid — this is the one path that genuinely
        // needs the whole body, so read it now. A read error here (e.g. a
        // TOCTOU race — the file vanished between the header read above and
        // now) degrades the same way a missing file always has, rather than
        // becoming a hard error.
        let Ok(data) = fs::read(idx_path) else {
            return Ok(empty(false));
        };
        match parse_vector_idx_eager(&data) {
            // Same rule as the legacy branch above: no query/open-path ANN
            // builds — exact flat_knn serves this column.
            Some(vs) => Ok(VectorFieldIndex {
                lazy: None,
                flat: vs,
                degraded: false,
            }),
            None => {
                // Valid header but the body itself is truncated/corrupt —
                // genuinely unrecoverable for this column's vectors. Marked
                // `degraded` (not just empty) so the query layer counts this
                // segment in `SearchResult::knn_degraded_segments` rather
                // than treating it as a legitimate vector-less segment.
                eprintln!(
                    "WARN: {} has a valid v2 header but corrupt body \
                     — this column will return no vector search results until rewritten",
                    idx_path.display()
                );
                Ok(empty(true))
            }
        }
    }

    /// The lazy posting index from a column's offsets sidecar alone, for a
    /// segment whose idx file is not local: postings are then read from
    /// the sparse span cache (`vector[.<n>].idx.spans/`) once fetched by
    /// range (#245). `None` without a valid sidecar.
    fn load_vector_index_from_offsets(
        idx_path: &Path,
        offsets_path: &Path,
        slot: u32,
        field: Option<String>,
    ) -> Option<LazyVectorIndex> {
        let data = fs::read(offsets_path).ok()?;
        if data.len() < VECTOR_OFFSETS_HEADER_LEN {
            return None;
        }
        let version = u32::from_le_bytes(data[4..8].try_into().ok()?);
        let dim = u32::from_le_bytes(data[8..12].try_into().ok()?);
        let parsed = parse_vector_offsets(&data, dim, version)?;
        Some(LazyVectorIndex::from_parsed(
            idx_path, slot, dim, version, parsed, field,
        ))
    }

    /// `true` when this segment's (first) vector column is served via the
    /// lazy v2 posting index rather than an exact flat scan.
    pub fn has_lazy_vector_index(&self) -> bool {
        self.default_vector_field()
            .is_some_and(|f| f.has_lazy_index())
    }

    /// `true` when this segment's (first) vector column exists but is
    /// currently unreadable (corrupt/truncated `vector.idx` body behind a
    /// valid v2 header) — see [`VectorFieldIndex::degraded`].
    pub fn vector_index_degraded(&self) -> bool {
        self.default_vector_field().is_some_and(|f| f.degraded())
    }

    /// `field`'s posting structure — each posting's centroid and member
    /// doc_seqs — for compaction to seed its output's index from (#253).
    /// Reads `vector.idx` once, sequentially, keeping only ids (not
    /// vectors). `Ok(None)` when the column is not served by a clustered
    /// index: absent, the legacy flat format, a missing or invalid
    /// `vector.offsets`, or a degraded body.
    pub fn vector_posting_layout(
        &self,
        field: &str,
    ) -> Result<Option<VectorPostingLayout>, KoshaError> {
        let Some(index) = self.vector_field(field)? else {
            return Ok(None);
        };
        let Some(vi) = index.lazy.as_ref() else {
            return Ok(None);
        };
        let entry_size = vector_entry_size(vi.dim as u32, vi.version);
        let mut file = std::io::BufReader::new(fs::File::open(&vi.vector_idx_path)?);
        let mut buf = Vec::new();
        let mut postings = Vec::with_capacity(vi.ranges.len());
        for (centroid, &(offset, length)) in vi.centroid_rows().zip(&vi.ranges) {
            if !(length as usize).is_multiple_of(entry_size) {
                return Err(KoshaError::CorruptSegment(format!(
                    "{}: posting at {offset} is {length} bytes, not a multiple of {entry_size}",
                    vi.vector_idx_path.display()
                )));
            }
            file.seek(SeekFrom::Start(offset))?;
            buf.resize(length as usize, 0);
            file.read_exact(&mut buf)?;
            let doc_seqs = buf
                .chunks_exact(entry_size)
                .map(|entry| u32::from_le_bytes(entry[..4].try_into().unwrap()))
                .collect();
            postings.push(VectorPostingMembers {
                centroid: centroid.to_vec(),
                doc_seqs,
            });
        }
        Ok(Some(VectorPostingLayout {
            dim: vi.dim,
            version: vi.version,
            postings,
        }))
    }

    /// Reads namespace routing metadata from existing offsets sidecars,
    /// without opening lexical files or hydrating any posting body.
    pub fn namespace_centroids(
        dir: &Path,
    ) -> Result<Vec<kosha_vector_spfresh::namespace_centroids::Column>, KoshaError> {
        Self::namespace_centroids_from(|name| Ok(fs::read(dir.join(name))?))
    }

    pub fn namespace_centroids_from(
        read: impl Fn(&str) -> Result<Vec<u8>, KoshaError>,
    ) -> Result<Vec<kosha_vector_spfresh::namespace_centroids::Column>, KoshaError> {
        use kosha_vector_spfresh::namespace_centroids::{Centroid, Column};
        let footer: Footer = serde_json::from_slice(&read("footer.json")?)?;
        let Some(fields) = footer.vector_fields else {
            return Ok(Vec::new()); // the planner falls back for legacy vector columns
        };
        fields
            .into_iter()
            .filter(|meta| meta.offsets)
            .map(|meta| {
                let file = meta.offsets_file();
                let data = read(&file)?;
                let invalid =
                    || KoshaError::CorruptSegment(format!("{file}: invalid centroid offsets"));
                let version =
                    u32::from_le_bytes(data.get(4..8).ok_or_else(invalid)?.try_into().unwrap());
                let postings = parse_vector_offsets(&data, meta.dim, version)
                    .filter(|postings| {
                        postings.iter().all(|p| {
                            p.radius.is_finite() && p.centroid.iter().all(|x| x.is_finite())
                        })
                    })
                    .ok_or_else(invalid)?;
                let unit_norm = is_unit_norm_vector_index_version(version);
                Ok(Column {
                    field: meta.name,
                    dim: meta.dim as usize,
                    centroids: postings
                        .into_iter()
                        .map(|p| Centroid {
                            vector: p.centroid,
                            radius: p.radius,
                            unit_norm,
                        })
                        .collect(),
                })
            })
            .collect()
    }

    /// The flat (legacy / missing-sidecar) vectors of this segment's first
    /// vector column — empty when it is served lazily or has none.
    pub fn vector_store(&self) -> VectorStore {
        self.default_vector_field()
            .map(|f| f.flat.clone())
            .unwrap_or_default()
    }

    /// Scores one posting of `scan`'s column: `emit(id, similarity,
    /// error_bound)` for every live entry `scan.accept` lets through (see
    /// [`VectorPosting::for_each_similarity`] for the scores).
    ///
    /// A posting in the segment's decoded cache is scored from there.
    /// Otherwise its bytes are read from the column's idx file — the whole
    /// local file when present, else the span cache a ranged cold query
    /// filled (#245) — through `scan`'s reader, which keeps the file open
    /// and reuses one buffer for the whole probe (#435). Then:
    ///
    /// * second touch (see below) of a posting the cache has room for, or
    ///   any quantized posting: decoded into an owned posting, as before;
    /// * any other f32 posting: scored in place from the read buffer, which
    ///   is 4-byte aligned (on a big-endian target, decoded into `scan`'s
    ///   reused flat scratch and scored from it). No posting is built,
    ///   nothing is allocated and nothing is copied after the read.
    ///
    /// Every path reads the same bytes as the same floats and scores them
    /// with the same kernel in the same order, so the scores are
    /// bit-identical.
    fn scan_posting(
        &self,
        scan: &mut PostingScan<'_>,
        held: &PrefetchHolds<'_>,
        range_idx: usize,
        f32_similarity: impl Fn(&[f32]) -> f32,
        emit: impl FnMut(u32, f32, f32),
    ) -> Result<(), KoshaError> {
        let vi = scan.vi;
        let centroid = vi.centroid(range_idx);
        let key = self.vector_cache_key(vi.slot, range_idx);
        let mut clock = scan.clock();
        // The prefetch's pin first: the cache may have evicted it since.
        let hit = match held.pinned(range_idx) {
            Some(pinned) => Some(Arc::clone(pinned)),
            None => self.vector_cache.get(key),
        };
        if let Some(hit) = hit {
            hit.for_each_similarity(scan.accept, f32_similarity, &scan.probe, centroid, emit);
            scan.stats.postings_cached += 1;
            scan.stats.score_nanos += lap_nanos(&mut clock);
            return Ok(());
        }
        let (offset, length) = vi.ranges[range_idx];
        let end = offset + length as u64;
        // Not local although the prefetch covered it (a span evicted from
        // disk, a failed queued write): one bounded refetch, held until
        // the bytes below are decoded. Still nothing: an IO error, which
        // the query layer reports as a degraded segment, never as no
        // matches.
        let refetched;
        let bytes = match scan.reader.read(end, offset, length as u64) {
            Some(bytes) => bytes,
            None => {
                refetched = held.refetch(&self.segment_dir, &vi.idx_file, offset, end);
                let tried = refetched.is_some();
                let read = match tried {
                    true => scan.reader.read(end, offset, length as u64),
                    false => None,
                };
                read.ok_or_else(|| {
                    KoshaError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!(
                            "vector posting [{offset}, +{length}) of {} is not local{}",
                            vi.vector_idx_path.display(),
                            if tried { " (refetched once)" } else { "" }
                        ),
                    ))
                })?
            }
        };
        scan.stats.read_nanos += lap_nanos(&mut clock);
        // Second-touch admission, as for text postings, over a history as
        // long as the cache's reach (see `vector_miss_window`): one-shot
        // probes (a cold namespace scanned once) pass through without
        // evicting a hot working set; probes that recur across queries get
        // cached on their second hydration, however many one query makes.
        let window = self.vector_cache.vector_miss_window(bytes.len());
        let admit = self.vector_cache.admit_on_miss_within(key, window);
        if !vi.quantized {
            let count = bytes.len() / vector_entry_size(vi.dim as u32, vi.version);
            let keep = admit
                && self
                    .vector_cache
                    .accepts(f32_posting_resident_bytes(count, vi.dim));
            if !keep {
                if let Some(words) = f32_entries_in_place(bytes, vi.dim) {
                    for_each_f32_similarity_in_place(
                        words,
                        vi.dim,
                        scan.accept,
                        f32_similarity,
                        emit,
                    );
                    scan.stats.postings_scanned += 1;
                    scan.stats.score_nanos += lap_nanos(&mut clock);
                    return Ok(());
                }
                decode_f32_entries_into(bytes, vi.dim, &mut scan.ids, &mut scan.vectors);
                scan.stats.decode_nanos += lap_nanos(&mut clock);
                for_each_f32_similarity(
                    &scan.ids,
                    &scan.vectors,
                    vi.dim,
                    scan.accept,
                    f32_similarity,
                    emit,
                );
                scan.stats.postings_scanned += 1;
                scan.stats.score_nanos += lap_nanos(&mut clock);
                return Ok(());
            }
        }
        let decoded = Arc::new(decode_posting_entries(bytes, vi.dim as u32, vi.version));
        if admit {
            let bytes = decoded.resident_bytes();
            self.vector_cache.insert_for(
                key,
                Arc::clone(&decoded),
                bytes,
                self.postings_account.as_ref(),
            );
        }
        scan.stats.decode_nanos += lap_nanos(&mut clock);
        decoded.for_each_similarity(scan.accept, f32_similarity, &scan.probe, centroid, emit);
        scan.stats.postings_decoded += 1;
        scan.stats.score_nanos += lap_nanos(&mut clock);
        Ok(())
    }

    /// kNN search over the lazy v2 posting index of the segment's first
    /// vector column: probe `nprobe` nearest centroids (all resident, no
    /// I/O), hydrate just those candidate postings from disk, score by
    /// cosine similarity, return the top `k`. `None` if that column has no
    /// lazy vector index — the caller (see `kosha-query`'s KNN block) falls
    /// back to an exact flat scan.
    pub fn knn_search(
        &self,
        query: &[f32],
        k: usize,
        nprobe: usize,
    ) -> Option<Result<Vec<(u32, f64)>, KoshaError>> {
        let field = self.default_vector_field()?;
        let vi = field.lazy.as_ref()?;
        Some(self.knn_search_lazy(vi, query, k, nprobe, None, None))
    }

    /// [`Self::knn_search`] over `field`'s column (#204), restricted to the
    /// documents `accept` lets through. See
    /// [`Self::knn_search_within_filtered`] for how the restriction is
    /// applied. `None` when the column has no lazy index here.
    pub fn knn_search_filtered(
        &self,
        field: &str,
        query: &[f32],
        k: usize,
        nprobe: usize,
        accept: Option<&dyn Fn(u32) -> bool>,
    ) -> Option<Result<Vec<(u32, f64)>, KoshaError>> {
        self.knn_search_filtered_fetching(field, query, k, nprobe, accept, None)
    }

    /// [`Self::knn_search_filtered`] that first hands the postings it will
    /// probe and does not hold locally to `fetch` (#245, ranged reads).
    pub fn knn_search_filtered_fetching(
        &self,
        field: &str,
        query: &[f32],
        k: usize,
        nprobe: usize,
        accept: Option<&dyn Fn(u32) -> bool>,
        fetch: Option<VectorSpanFetch<'_>>,
    ) -> Option<Result<Vec<(u32, f64)>, KoshaError>> {
        let index = match self.vector_field(field) {
            Ok(index) => index?,
            Err(e) => return Some(Err(e)),
        };
        let vi = index.lazy.as_ref()?;
        if let Err(e) = Self::check_query_dim(vi, field, query) {
            return Some(Err(e));
        }
        Some(self.knn_search_lazy(vi, query, k, nprobe, accept, fetch))
    }

    /// Hand `fetch` the ranges of the column's idx file for the postings in
    /// `range_idxs` that are neither decoded in memory nor readable
    /// locally, in one call. The caller keeps the returned holds until it
    /// has scanned those postings, and scans them with
    /// [`Self::scan_posting`] through those holds.
    ///
    /// A posting skipped because the vector cache holds it is pinned in
    /// the holds (`get`, not `contains`): the cache may evict it before the
    /// scan, and since #481 a dropped span write can leave the decoded
    /// entry as its only local copy (finding 31). Skipped always means
    /// held until scanned.
    fn prefetch_vector_postings<'f>(
        &self,
        vi: &LazyVectorIndex,
        range_idxs: impl IntoIterator<Item = usize>,
        fetch: Option<VectorSpanFetch<'f>>,
    ) -> PrefetchHolds<'f> {
        let mut holds = PrefetchHolds::new(fetch);
        let Some(fetch) = fetch else {
            return holds;
        };
        let whole_len = fs::metadata(&vi.vector_idx_path)
            .ok()
            .filter(|m| m.is_file())
            .map_or(0, |m| m.len());
        let mut spans: Option<span_cache::SpanIndex> = None;
        let mut missing = Vec::new();
        for i in range_idxs {
            let Some(&(offset, length)) = vi.ranges.get(i) else {
                continue;
            };
            let end = offset + length as u64;
            if length == 0 || whole_len >= end {
                continue;
            }
            if let Some(hit) = self.vector_cache.get(self.vector_cache_key(vi.slot, i)) {
                holds.decoded.insert(i, hit);
                continue;
            }
            let spans = spans.get_or_insert_with(|| {
                span_cache::SpanIndex::load(&self.segment_dir, &vi.idx_file)
            });
            if !spans.covers(offset, end) {
                missing.push((offset, end));
            }
        }
        if !missing.is_empty() {
            holds.spans = fetch(&self.segment_dir, &vi.idx_file, &missing);
        }
        holds
    }

    /// A query vector must have its column's dimension: scoring vectors of
    /// different lengths is meaningless (and asserts in the distance
    /// kernels).
    fn check_query_dim(vi: &LazyVectorIndex, field: &str, query: &[f32]) -> Result<(), KoshaError> {
        if vi.dim == query.len() {
            return Ok(());
        }
        Err(KoshaError::InvalidQuery(format!(
            "knn.vector has {} dimensions but vector field '{field}' is indexed with {}",
            query.len(),
            vi.dim
        )))
    }

    /// Triangle-inequality lower bound on `query`'s distance to *any*
    /// possible member of `centroids[i]`'s posting: `(distance(query,
    /// centroid) - radius).max(0.0)`, where `radius` is the max distance
    /// from that centroid to any of its own live members. For every actual
    /// member `m`, `distance(query, m) >= distance(query, centroid) -
    /// distance(centroid, m) >= distance(query, centroid) - radius` — so
    /// this bound can never overestimate how close the posting's best
    /// member could be, only underestimate (safe to rank/admit by; never
    /// wrongly excludes a posting that might contain a close match).
    /// `radius == 0.0` (pre-radius-aware segments) collapses this to plain
    /// centroid distance — today's exact behavior, unaffected.
    ///
    /// With `KOSHA_KNN_PROBE_RADIUS_ALPHA` set (#384, experimental) the
    /// radius is scaled first — see [`centroid_probe_score`]. At any alpha
    /// other than the default 1.0 the result is a ranking score, no longer
    /// a guaranteed lower bound.
    ///
    /// One score per posting of `vi`, in posting order. On a unit-norm
    /// column the centroids are unit vectors too (the writer renormalizes
    /// them), so the query is normalized once and each distance is a single
    /// dot product instead of `cosine_distance`'s three. Both the global
    /// cutoff and each segment's own ranking come from this function, so
    /// they stay in the same units.
    fn centroid_lower_bounds(vi: &LazyVectorIndex, query: &[f32]) -> Vec<f32> {
        Self::centroid_scores(vi, query, knn_probe_radius_alpha())
    }

    /// [`Self::centroid_lower_bounds`] at an explicit radius weight.
    fn centroid_scores(vi: &LazyVectorIndex, query: &[f32], alpha: f32) -> Vec<f32> {
        let unit = Self::unit_query(vi, query);
        (0..vi.ranges.len())
            .map(|i| Self::centroid_score(vi, unit.as_deref(), query, i, alpha))
            .collect()
    }

    /// Posting `i`'s rank score: the one expression every exact centroid
    /// score comes from, whether the whole column is scored or a few of
    /// its postings, so the two are the same f32 bit for bit.
    fn centroid_score(
        vi: &LazyVectorIndex,
        unit: Option<&[f32]>,
        query: &[f32],
        i: usize,
        alpha: f32,
    ) -> f32 {
        let c = vi.centroid(i);
        let distance = match unit {
            Some(unit) => 1.0 - kosha_vector_spfresh::dot(unit, c).clamp(-1.0, 1.0),
            None => kosha_vector_spfresh::cosine_distance(query, c),
        };
        centroid_probe_score(distance, vi.radii[i], alpha)
    }

    /// [`Self::centroid_score`] of each of `postings` (all in range), in
    /// the order given.
    fn centroid_scores_at(
        vi: &LazyVectorIndex,
        query: &[f32],
        postings: &[u32],
        alpha: f32,
    ) -> Vec<f32> {
        let unit = Self::unit_query(vi, query);
        postings
            .iter()
            .map(|&i| Self::centroid_score(vi, unit.as_deref(), query, i as usize, alpha))
            .collect()
    }

    /// Guaranteed bounds on every posting's [`Self::centroid_score`] from
    /// the column's i8 centroid codes, without reading an f32 centroid.
    /// `None` when there are no codes (see
    /// [`LazyVectorIndex::centroid_codes`]) or the bound's assumptions do
    /// not hold for this query; the caller then uses exact scores.
    ///
    /// Why the bounds hold, for posting `i` with f32 centroid `c`:
    ///
    /// 1. `x = dot(unit, c)` is what the exact score is computed from and
    ///    `x'` is the estimate from the codes. By [`CentroidCodes`],
    ///    `|x - x'| <= e0`, with `e0 = min(err_l2 * ||unit||_2,
    ///    err_linf * ||unit||_1) + query_err`, where `query_err` is the
    ///    query's own quantization residual times the largest code norm,
    ///    plus the rounding of the estimate.
    /// 2. The exact distance is `d = fl(1 - clamp(x))` and the estimate
    ///    `d' = fl(1 - clamp(x'))`. `clamp` to `[-1, 1]` never increases a
    ///    difference and each subtraction rounds a value in `[0, 2]`, by at
    ///    most `2^-24`, so `|d - d'| <= e0 + 2^-23`. `e = e0 + PAD` covers
    ///    that and the rounding of `d' - e` and `d' + e` themselves (each
    ///    under `2.4e-7`): `fl(d' - e) <= d <= fl(d' + e)`.
    /// 3. [`centroid_probe_score`] is `max(fl(d - fl(alpha * r)), 0)`. For
    ///    a fixed radius and alpha it is a non-decreasing function of `d`
    ///    *as computed in f32*: `fl(alpha * r)` is the same number in every
    ///    call, rounding to nearest is monotone, and so is `max`. That
    ///    needs nothing of alpha or the radius (no Lipschitz constant, no
    ///    sign), so it holds for every `KOSHA_KNN_PROBE_RADIUS_ALPHA`.
    ///    Hence `score(fl(d' - e)) <= score(d) <= score(fl(d' + e))`.
    ///
    /// A NaN would break step 3 silently (`max` turns it into `0`), so a
    /// query or an estimate that is not finite yields `None` instead.
    fn centroid_score_bounds(
        vi: &LazyVectorIndex,
        query: &[f32],
        alpha: f32,
    ) -> Option<KnnCentroidScoreBounds> {
        /// Absolute slack for the handful of f32 roundings in step 2.
        const PAD: f32 = 2e-6;
        let codes = vi.centroid_codes.as_ref()?;
        if !(alpha.is_finite() && alpha >= 0.0) {
            return None;
        }
        let unit = Self::unit_query(vi, query)?;
        if unit.iter().any(|x| !x.is_finite()) {
            return None;
        }
        // The query's norms, rounded up. A unit query's L2 norm is 1 to
        // within rounding; it is measured rather than assumed (a zero
        // query stays zero).
        let (l1, l2sq) = unit.iter().fold((0f64, 0f64), |(l1, l2sq), &x| {
            let x = f64::from(x);
            (l1 + x.abs(), l2sq + x * x)
        });
        let up = |x: f64| (x * (1.0 + 1e-5)) as f32;
        let (l1, l2) = (up(l1), up(l2sq.sqrt()));
        // The query's codes, and what quantizing it can cost against any
        // row: `||r_q||_2 * max ||c^||_2`, plus three f32 roundings of an
        // estimate no larger than `||q^||_2 * max ||c^||_2`.
        let (query_codes, query_scale) = kosha_vector_spfresh::quantize_i16(
            &unit,
            kosha_vector_spfresh::max_i16_query_code(vi.dim),
        );
        let query_residual = unit
            .iter()
            .zip(&query_codes)
            .map(|(&x, &k)| {
                let r = f64::from(x) - f64::from(query_scale) * f64::from(k);
                r * r
            })
            .sum::<f64>()
            .sqrt();
        let query_err = codes.max_code_norm
            * up(query_residual + 4.0 * f64::from(f32::EPSILON) * (l2sq.sqrt() + query_residual));
        let n = vi.ranges.len();
        let mut bounds = KnnCentroidScoreBounds {
            lower: Vec::with_capacity(n),
            upper: Vec::with_capacity(n),
        };
        let mut finite = true;
        for i in 0..n {
            let sum = kosha_vector_spfresh::dot_i16_i8(&query_codes, codes.row(i, vi.dim));
            let estimate = sum as f32 * (query_scale * codes.scales[i]);
            let e = (codes.err_l2[i] * l2).min(codes.err_linf[i] * l1) + query_err + PAD;
            finite &= estimate.is_finite() && e.is_finite();
            let distance = 1.0 - estimate.clamp(-1.0, 1.0);
            let r = vi.radii[i];
            bounds
                .lower
                .push(centroid_probe_score(distance - e, r, alpha));
            bounds
                .upper
                .push(centroid_probe_score(distance + e, r, alpha));
        }
        finite.then_some(bounds)
    }

    /// See [`CentroidCap::score_floor`].
    fn centroid_score_floor(vi: &LazyVectorIndex, query: &[f32], alpha: f32) -> Option<f32> {
        let cap = vi.centroid_cap.as_ref()?;
        cap.score_floor(&Self::unit_query(vi, query)?, alpha)
    }

    /// `query` scaled to unit length when `vi` stores unit vectors, so a
    /// stored f32 vector's cosine similarity is a bare dot product; `None`
    /// for a column that stores raw magnitudes. A zero query stays zero and
    /// scores `0` against everything, as `cosine_similarity` has it.
    fn unit_query(vi: &LazyVectorIndex, query: &[f32]) -> Option<Vec<f32>> {
        vi.unit_norm.then(|| {
            let mut unit = query.to_vec();
            kosha_vector_spfresh::normalize_in_place(&mut unit);
            unit
        })
    }

    /// Cosine similarity of the query to one stored f32 vector of `vi`:
    /// a dot product against `unit` (see [`Self::unit_query`]) when the
    /// column is unit-norm, the full cosine otherwise.
    fn f32_similarity(lanes: DotLanes, unit: Option<&[f32]>, query: &[f32], v: &[f32]) -> f32 {
        match unit {
            Some(unit) => lanes.dot(unit, v).clamp(-1.0, 1.0),
            None => lanes.cosine_similarity(query, v),
        }
    }

    /// Lower-bound distance from `query` to every posting of `field`'s
    /// column (#204; see [`Self::centroid_lower_bounds`]) — **pure CPU,
    /// zero posting I/O**: centroids and radii are resident in
    /// [`LazyVectorIndex`], loaded from the column's offsets sidecar.
    ///
    /// This is the cheap half of a global probe budget. `num_candidates` is
    /// a *whole-query* budget, but a segment can only ever rank its own
    /// postings, so the query layer collects these bounds across all
    /// segments, picks a global cutoff, and only then pays for hydration —
    /// see [`Self::knn_search_within`]. Without that split, `num_candidates`
    /// is applied per segment and total probe depth scales with segment
    /// count (200 segments x 100 = 20,000 posting reads for one top-10).
    /// Ranking by the lower bound rather than raw centroid distance is what
    /// lets a "diffuse" posting (mediocre centroid pull, but a genuinely
    /// close member buried inside it) still qualify — see
    /// `RADIUS_AWARE_VECTOR_INDEX_VERSION`. Only `field`'s centroids count:
    /// another column's postings are never probed for this query, so they
    /// must not take a share of its budget.
    ///
    /// `None` when this segment has no lazy v2 index for `field` (no
    /// vectors for it, or legacy flat vectors), which the caller treats as
    /// "cannot participate in global budgeting".
    pub fn knn_centroid_distances(&self, field: &str, query: &[f32]) -> Option<Vec<f32>> {
        let index = self.vector_field(field).ok()??;
        let vi = index.lazy.as_ref()?;
        if vi.dim != query.len() {
            return None;
        }
        Some(Self::centroid_lower_bounds(vi, query))
    }

    /// The lazy index [`Self::knn_centroid_distances`] scores for `field`
    /// and a query of `dim` dimensions, under the same conditions.
    fn with_knn_plan_index<R>(
        &self,
        field: &str,
        dim: usize,
        f: impl FnOnce(&LazyVectorIndex) -> Option<R>,
    ) -> Option<R> {
        let index = self.vector_field(field).ok()??;
        f(index.lazy.as_ref().filter(|vi| vi.dim == dim)?)
    }

    /// How many scores [`Self::knn_centroid_distances`] returns for
    /// `field` and a query of `dim` dimensions (one per posting), without
    /// computing them; `None` exactly when it returns `None`.
    pub fn knn_centroid_count(&self, field: &str, dim: usize) -> Option<usize> {
        self.with_knn_plan_index(field, dim, |vi| Some(vi.ranges.len()))
    }

    /// [`Self::knn_centroid_distances`] at an explicit radius weight
    /// instead of the process's `KOSHA_KNN_PROBE_RADIUS_ALPHA` (which is
    /// what a segment's own probe uses: pass anything else only to compare
    /// plans, never to plan a real query).
    pub fn knn_centroid_distances_with_alpha(
        &self,
        field: &str,
        query: &[f32],
        alpha: f32,
    ) -> Option<Vec<f32>> {
        self.with_knn_plan_index(field, query.len(), |vi| {
            Some(Self::centroid_scores(vi, query, alpha))
        })
    }

    /// The scores [`Self::knn_centroid_distances_with_alpha`] has for
    /// `postings` only, in the order given: the same f32 values, computed
    /// by the same expression, reading only those centroids. `None` under
    /// the same conditions, or when a posting is out of range.
    pub fn knn_centroid_scores_for(
        &self,
        field: &str,
        query: &[f32],
        postings: &[u32],
        alpha: f32,
    ) -> Option<Vec<f32>> {
        self.with_knn_plan_index(field, query.len(), |vi| {
            if postings.iter().any(|&i| i as usize >= vi.ranges.len()) {
                return None;
            }
            Some(Self::centroid_scores_at(vi, query, postings, alpha))
        })
    }

    /// Stage one of two-stage kNN planning: guaranteed lower and upper
    /// bounds on every score [`Self::knn_centroid_distances_with_alpha`]
    /// would return, computed from i8 codes of the centroids (a quarter of
    /// the memory the exact scan reads). The query layer uses them to
    /// decide which postings need an exact score
    /// ([`Self::knn_centroid_scores_for`]); the bounds themselves never
    /// rank or admit anything.
    ///
    /// `None` when the column has no codes (not unit-norm,
    /// `KOSHA_KNN_PLAN_I8=0`, no lazy index, a dimension mismatch) or the
    /// query is not finite: the caller scores the column exactly.
    pub fn knn_centroid_score_bounds(
        &self,
        field: &str,
        query: &[f32],
        alpha: f32,
    ) -> Option<KnnCentroidScoreBounds> {
        self.with_knn_plan_index(field, query.len(), |vi| {
            Self::centroid_score_bounds(vi, query, alpha)
        })
    }

    /// A floor on every score [`Self::knn_centroid_distances_with_alpha`]
    /// would return, from one comparison against a cap around the
    /// column's centroids (see [`CentroidCap`]): the kNN plan leaves a
    /// segment out without scoring its centroids when this is above the
    /// probe cutoff. `None` when the column has no cap (not unit-norm, no
    /// lazy index, a dimension mismatch) or the query is not finite.
    pub fn knn_centroid_score_floor(&self, field: &str, query: &[f32], alpha: f32) -> Option<f32> {
        self.with_knn_plan_index(field, query.len(), |vi| {
            Self::centroid_score_floor(vi, query, alpha)
        })
    }

    /// Score only the postings of the segment's first vector column whose
    /// centroid distance is `<= max_distance`, capped at `max_probes`
    /// regardless (a tie at the cutoff, or a degenerate all-zero-distance
    /// segment, must not reopen the fan-out this exists to bound). Returns
    /// the segment's top `k`.
    ///
    /// The companion to [`Self::knn_centroid_distances`]: the query layer
    /// derives `max_distance` from the global budget, so the union of work
    /// across segments is ~`num_candidates` postings for the whole query
    /// instead of `num_candidates` per segment.
    /// Returns `(top_k, postings_hydrated)` — the probe count is reported
    /// so the query layer can surface it in `SearchPhaseStats`; it is the
    /// number that makes runaway probe depth visible.
    pub fn knn_search_within(
        &self,
        query: &[f32],
        k: usize,
        max_distance: f32,
        max_probes: usize,
    ) -> BudgetedKnnResult {
        let field = self.default_vector_field()?;
        let vi = field.lazy.as_ref()?;
        Some(self.knn_search_within_lazy(
            vi,
            query,
            k,
            max_distance,
            max_probes,
            None,
            KnnRefill::Always,
            None,
        ))
    }

    /// [`Self::knn_search_within`] over `field`'s column (#204), restricted
    /// to the documents `accept` lets through (a filter, deleted documents,
    /// or both).
    ///
    /// The restriction is applied while postings are scored, not to the
    /// finished top `k`: a vector whose document is not accepted is never a
    /// candidate, so the `k` returned are the nearest *accepted* documents
    /// among the postings probed.
    ///
    /// The probe budget (`max_distance`, `max_probes`) was sized without
    /// knowing how selective `accept` is. When it has been spent and fewer
    /// than `k` accepted documents were found, probing continues outward,
    /// nearest posting first, until `k` are found or the segment has been
    /// read in full. A selective filter therefore costs more I/O in the
    /// segments that hold its documents, and always returns them: the
    /// alternative is an empty or short page for a filter that does match.
    /// This is [`KnnRefill::Always`]; a caller whose restriction is deleted
    /// documents alone passes [`KnnRefill::BudgetedSegments`] to
    /// [`Self::knn_search_within_filtered_fetching`] instead.
    pub fn knn_search_within_filtered(
        &self,
        field: &str,
        query: &[f32],
        k: usize,
        max_distance: f32,
        max_probes: usize,
        accept: Option<&dyn Fn(u32) -> bool>,
    ) -> BudgetedKnnResult {
        self.knn_search_within_filtered_fetching(
            field,
            query,
            k,
            max_distance,
            max_probes,
            accept,
            KnnRefill::Always,
            None,
        )
    }

    /// [`Self::knn_search_within_filtered`] that first hands the postings
    /// inside the budget it does not hold locally to `fetch`, in one call,
    /// and fetches further postings in chunks of
    /// [`VECTOR_PREFETCH_CHUNK`] if a restriction makes the probe run past
    /// its budget (#245, ranged reads).
    #[allow(clippy::too_many_arguments)]
    pub fn knn_search_within_filtered_fetching(
        &self,
        field: &str,
        query: &[f32],
        k: usize,
        max_distance: f32,
        max_probes: usize,
        accept: Option<&dyn Fn(u32) -> bool>,
        refill: KnnRefill,
        fetch: Option<VectorSpanFetch<'_>>,
    ) -> BudgetedKnnResult {
        let index = match self.vector_field(field) {
            Ok(index) => index?,
            Err(e) => return Some(Err(e)),
        };
        let vi = index.lazy.as_ref()?;
        if let Err(e) = Self::check_query_dim(vi, field, query) {
            return Some(Err(e));
        }
        Some(self.knn_search_within_lazy(
            vi,
            query,
            k,
            max_distance,
            max_probes,
            accept,
            refill,
            fetch,
        ))
    }

    /// [`Self::knn_search_within_filtered_fetching`] for a segment whose
    /// postings the query layer has already chosen and ranked (#436):
    /// `ranking` must have been built from this segment's
    /// [`Self::knn_centroid_distances`] for the same `field` and `query`.
    /// Same hits, same scores and same posting count as the unplanned
    /// call with `ranking`'s cutoff and cap, without scoring or sorting
    /// the segment's centroids again. A ranking that does not have one
    /// score per posting of this column is ignored and the segment ranks
    /// its own postings.
    #[allow(clippy::too_many_arguments)]
    pub fn knn_search_planned_fetching(
        &self,
        field: &str,
        query: &[f32],
        k: usize,
        ranking: &KnnSegmentRanking,
        accept: Option<&dyn Fn(u32) -> bool>,
        refill: KnnRefill,
        fetch: Option<VectorSpanFetch<'_>>,
    ) -> BudgetedKnnResult {
        let index = match self.vector_field(field) {
            Ok(index) => index?,
            Err(e) => return Some(Err(e)),
        };
        let vi = index.lazy.as_ref()?;
        if let Err(e) = Self::check_query_dim(vi, field, query) {
            return Some(Err(e));
        }
        let probed = match self.probe_within_lazy(
            vi,
            query,
            k,
            ranking.max_distance,
            ranking.max_probes,
            accept,
            refill,
            fetch,
            Some(ranking),
        ) {
            Ok(probed) => probed,
            Err(e) => return Some(Err(e)),
        };
        Some(Ok((
            self.finish_probe(vi, query, probed.scored, &probed.errors, k),
            probed.postings,
        )))
    }

    /// The candidates half of a kNN probe over `field`'s column, for
    /// global bound-driven rescoring (#381, `KOSHA_KNN_GLOBAL_RESCORE`).
    ///
    /// Probes exactly as [`Self::knn_search_within_filtered_fetching`]
    /// (or [`Self::knn_search_filtered_fetching`] for
    /// [`KnnProbeBudget::Nprobe`]) does, but neither cuts to a fixed
    /// shortlist nor rescores. Instead:
    ///
    /// * an f32 column's scores are exact: its best `k`, each with error
    ///   `0`, and [`KnnCandidates::exact`] set;
    /// * a quantized column returns every candidate whose upper bound
    ///   (`estimate + error`) reaches this segment's `k`-th largest lower
    ///   bound (`estimate - error`). The query layer's global `k`-th lower
    ///   bound is at least as high, so nothing dropped here could survive
    ///   the global cut either.
    ///
    /// Candidates come back best estimate first. `None` when the column has
    /// no lazy index here.
    pub fn knn_candidates_fetching(
        &self,
        field: &str,
        query: &[f32],
        k: usize,
        budget: KnnProbeBudget<'_>,
        accept: Option<&dyn Fn(u32) -> bool>,
        fetch: Option<VectorSpanFetch<'_>>,
    ) -> Option<Result<KnnCandidates, KoshaError>> {
        let index = match self.vector_field(field) {
            Ok(index) => index?,
            Err(e) => return Some(Err(e)),
        };
        let vi = index.lazy.as_ref()?;
        if let Err(e) = Self::check_query_dim(vi, field, query) {
            return Some(Err(e));
        }
        let probed = match budget {
            KnnProbeBudget::Within {
                max_distance,
                max_probes,
                refill,
            } => self.probe_within_lazy(
                vi,
                query,
                k,
                max_distance,
                max_probes,
                accept,
                refill,
                fetch,
                None,
            ),
            KnnProbeBudget::Planned { ranking, refill } => self.probe_within_lazy(
                vi,
                query,
                k,
                ranking.max_distance,
                ranking.max_probes,
                accept,
                refill,
                fetch,
                Some(ranking),
            ),
            KnnProbeBudget::Nprobe(nprobe) => {
                let mut unit = query.to_vec();
                if vi.unit_norm {
                    kosha_vector_spfresh::normalize_in_place(&mut unit);
                }
                self.probe_nprobe_lazy(vi, &unit, k, nprobe, accept, fetch)
            }
        };
        let probed = match probed {
            Ok(probed) => probed,
            Err(e) => return Some(Err(e)),
        };
        let postings = probed.postings;
        if !vi.quantized {
            let candidates = top_k_by_score(probed.scored, k)
                .into_iter()
                .map(|(doc_seq, estimate)| KnnCandidate {
                    doc_seq,
                    estimate,
                    error: 0.0,
                })
                .collect();
            return Some(Ok(KnnCandidates {
                candidates,
                postings,
                exact: true,
            }));
        }
        let mut candidates: Vec<KnnCandidate> = probed
            .scored
            .iter()
            .zip(probed.errors.iter().chain(std::iter::repeat(&0.0)))
            .map(|(&(doc_seq, estimate), &error)| KnnCandidate {
                doc_seq,
                estimate,
                error: error as f64,
            })
            .collect();
        if let Some(threshold) = kth_lower_bound(&candidates, k) {
            candidates.retain(|c| c.upper() >= threshold);
        }
        candidates.sort_by(|a, b| {
            b.estimate
                .partial_cmp(&a.estimate)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Some(Ok(KnnCandidates {
            candidates,
            postings,
            exact: false,
        }))
    }

    /// Rescore a bounded quantized shortlist before returning a ranked list.
    /// A missing exact score fails the probe rather than mixing estimates
    /// into an order that can change when the sidecar becomes warm.
    pub fn knn_rescore_shortlist_fetching(
        &self,
        field: &str,
        query: &[f32],
        k: usize,
        budget: KnnProbeBudget<'_>,
        accept: Option<&dyn Fn(u32) -> bool>,
        fetch: Option<VectorSpanFetch<'_>>,
    ) -> BudgetedKnnResult {
        let index = match self.vector_field(field) {
            Ok(index) => index?,
            Err(e) => return Some(Err(e)),
        };
        let vi = index.lazy.as_ref()?;
        let limit = k
            .saturating_mul(self.vector_rescore_factor.max(1))
            .saturating_mul(if vi.rabitq { RABITQ_SHORTLIST_CAP } else { 1 });
        self.knn_candidates_fetching(field, query, k, budget, accept, fetch)
            .map(|found| {
                let found = found?;
                let mut seqs: Vec<_> = found
                    .candidates
                    .into_iter()
                    .take(limit)
                    .map(|c| c.doc_seq)
                    .collect();
                if seqs.is_empty() {
                    return Ok((Vec::new(), found.postings));
                }
                seqs.sort_unstable();
                let exact = self
                    .knn_exact_from_sidecar(field, query, k, &seqs, fetch)
                    .filter(|result| result.vectors_scored == seqs.len())
                    .ok_or_else(|| {
                        KoshaError::NotFound(format!(
                            "{}: exact kNN shortlist unavailable",
                            self.segment_dir.display()
                        ))
                    })?;
                Ok((exact.hits, found.postings))
            })
    }

    /// Exact cosine similarity of `query` with `field`'s full-precision
    /// vector of each of `doc_seqs` (#381), read from `doc_vectors.bin`:
    /// the local file when it is whole, otherwise by ranged reads through
    /// `fetch` into the span cache (one call for every missing span). The
    /// sidecar's header must be local for the ranged path (whole file or
    /// cached header span); it is not fetched here.
    ///
    /// Never fails: a document whose vector cannot be read is counted in
    /// [`ExactRescore::unavailable`] and left out of `scores`, so the caller
    /// can surface that its estimate stands, instead of silently returning
    /// it as final.
    pub fn knn_exact_scores_fetching(
        &self,
        field: &str,
        query: &[f32],
        doc_seqs: &[u32],
        fetch: Option<VectorSpanFetch<'_>>,
    ) -> ExactRescore {
        let mut wanted: Vec<u32> = doc_seqs.to_vec();
        wanted.sort_unstable();
        wanted.dedup();
        let mut out = ExactRescore::default();
        if wanted.is_empty() {
            return out;
        }
        let unavailable = |mut out: ExactRescore, why: String| {
            out.unavailable = wanted.len();
            out.reason = Some(why);
            out
        };
        if !self.footer.doc_vectors {
            return unavailable(
                out,
                format!("segment stores vectors inline, no {DOC_VECTORS_FILE}"),
            );
        }
        let dv = match self.doc_vectors() {
            Ok(dv) => dv,
            Err(e) => return unavailable(out, format!("{DOC_VECTORS_FILE} header: {e}")),
        };
        if !dv.has_field(field) {
            return unavailable(
                out,
                format!("{DOC_VECTORS_FILE} holds no '{field}' vectors"),
            );
        }
        let whole = self.segment_dir.join(DOC_VECTORS_FILE);
        let whole_local =
            fs::metadata(&whole).is_ok_and(|m| m.is_file() && m.len() == dv.file_len());
        let mut exact: HashMap<u32, f64> = HashMap::with_capacity(wanted.len());
        if whole_local {
            let visited = dv.visit_field_vectors(field, &wanted, |doc_seq, v| {
                if v.len() == query.len() {
                    exact.insert(
                        doc_seq,
                        kosha_vector_spfresh::cosine_similarity(query, v) as f64,
                    );
                }
            });
            if let Err(e) = visited {
                return unavailable(out, format!("{DOC_VECTORS_FILE} read: {e}"));
            }
        } else {
            out.ranged = true;
            let spans: Vec<(u32, (u64, u64))> = wanted
                .iter()
                .filter_map(|&doc_seq| dv.doc_span(doc_seq).map(|span| (doc_seq, span)))
                .collect();
            // The span cache is listed once before the fetch and once after
            // it, never per candidate: a listing is a `read_dir` plus a stat
            // of every cached span, and the directory grows as rescoring
            // caches spans.
            let mut local = span_cache::SpanIndex::load(&self.segment_dir, DOC_VECTORS_FILE);
            if let Some(fetch) = fetch {
                let missing: Vec<(u64, u64)> = spans
                    .iter()
                    .map(|&(_, span)| span)
                    .filter(|&(start, end)| !local.covers(start, end))
                    .collect();
                if !missing.is_empty() {
                    out.ranged_spans = missing.len();
                    // The reloaded index holds whatever the fetch left only
                    // in memory, for as long as the reads below need it.
                    let _held = fetch(&self.segment_dir, DOC_VECTORS_FILE, &missing);
                    local = span_cache::SpanIndex::load(&self.segment_dir, DOC_VECTORS_FILE);
                }
            }
            let ranges: Vec<(u64, u64)> = spans.iter().map(|&(_, span)| span).collect();
            let bytes = local.read_many(&ranges);
            for ((doc_seq, (start, end)), buf) in spans.into_iter().zip(bytes) {
                // A span evicted between the listing and the read (its
                // whole file may have been promoted meanwhile): the
                // per-document read checks both. A span never cached is
                // just unavailable, with no further listing.
                let buf = buf.or_else(|| {
                    if !local.covers(start, end) {
                        return None;
                    }
                    span_cache::read_local_range(
                        &self.segment_dir,
                        DOC_VECTORS_FILE,
                        dv.file_len(),
                        start,
                        end - start,
                    )
                });
                let Some(v) = buf.and_then(|buf| dv.field_vector_in_span(doc_seq, field, &buf))
                else {
                    continue;
                };
                if v.len() == query.len() {
                    exact.insert(
                        doc_seq,
                        kosha_vector_spfresh::cosine_similarity(query, &v) as f64,
                    );
                }
            }
        }
        out.unavailable = wanted.len() - exact.len();
        if out.unavailable > 0 {
            out.reason = Some(format!(
                "{} of {} vectors not readable from {DOC_VECTORS_FILE}",
                out.unavailable,
                wanted.len()
            ));
        }
        out.scores = exact.into_iter().collect();
        out
    }

    /// Exact kNN over `field`'s column (#204), restricted to `candidates`:
    /// the ascending `doc_seq`s a filter lets through, with deleted
    /// documents already removed (#252).
    ///
    /// Every candidate's vector is scored, so the result is the true top
    /// `k` of the filtered set — no centroid is consulted. Worth it when
    /// the filter passes few documents: probing would otherwise hydrate
    /// postings full of documents the filter throws away, and keep going
    /// outward until it has found `k` that pass.
    ///
    /// Vectors are read from `doc_vectors.bin` (#243) when it holds
    /// `field` and is local — the whole file, or its header with the
    /// candidates' ranges fetched through `fetch` (see
    /// [`Self::knn_exact_from_sidecar`]); otherwise every posting of the
    /// column is hydrated and filtered (the only way to see every vector
    /// of a segment that stores them inline). Scores are the posting
    /// probe's: cosine similarity, which on the sidecar's raw vectors is
    /// the same number the index's unit-normalized copies give.
    ///
    /// `None` when the column has no lazy index here (a legacy flat index
    /// is already scanned exactly by the caller).
    pub fn knn_exact_filtered(
        &self,
        field: &str,
        query: &[f32],
        k: usize,
        candidates: &[u32],
    ) -> Option<Result<ExactKnnResult, KoshaError>> {
        self.knn_exact_filtered_fetching(field, query, k, candidates, None)
    }

    /// [`Self::knn_exact_filtered`] that, when it has to score from the
    /// vector postings, first hands every posting it does not hold
    /// locally to `fetch` in one call (#245, ranged reads).
    pub fn knn_exact_filtered_fetching(
        &self,
        field: &str,
        query: &[f32],
        k: usize,
        candidates: &[u32],
        fetch: Option<VectorSpanFetch<'_>>,
    ) -> Option<Result<ExactKnnResult, KoshaError>> {
        let index = match self.vector_field(field) {
            Ok(index) => index?,
            Err(e) => return Some(Err(e)),
        };
        let vi = index.lazy.as_ref()?;
        if let Err(e) = Self::check_query_dim(vi, field, query) {
            return Some(Err(e));
        }
        if candidates.is_empty() || k == 0 {
            return Some(Ok(ExactKnnResult {
                hits: Vec::new(),
                vectors_scored: 0,
                postings_hydrated: 0,
                source: ExactKnnSource::Sidecar,
            }));
        }
        if let Some(result) = self.knn_exact_from_sidecar(field, query, k, candidates, fetch) {
            return Some(Ok(result));
        }
        // No usable sidecar: every posting, filtered while it is scored.
        let held = self.prefetch_vector_postings(vi, 0..vi.ranges.len(), fetch);
        // A quantized column scores approximately here (it has no exact
        // vectors without the sidecar).
        let mut sink = CandidateSink::new(vi, k);
        let is_candidate = |id: u32| candidates.binary_search(&id).is_ok();
        let lanes = DotLanes::from_env();
        let unit = Self::unit_query(vi, query);
        let mut scan = PostingScan::new(
            &self.segment_dir,
            vi,
            Self::probe_query(vi, query),
            Some(&is_candidate),
        );
        for range_idx in 0..vi.ranges.len() {
            let scanned = self.scan_posting(
                &mut scan,
                &held,
                range_idx,
                |v| Self::f32_similarity(lanes, unit.as_deref(), query, v),
                |id, sim, err| sink.push(id, sim, err),
            );
            if let Err(e) = scanned {
                return Some(Err(e));
            }
        }
        let vectors_scored = sink.len();
        let (scored, _) = sink.finish();
        Some(Ok(ExactKnnResult {
            hits: top_k_by_score(scored, k),
            vectors_scored,
            postings_hydrated: vi.ranges.len(),
            source: ExactKnnSource::Postings,
        }))
    }

    /// [`Self::knn_exact_filtered_fetching`]'s sidecar path: every
    /// candidate's `field` vector from `doc_vectors.bin`, scored exactly.
    ///
    /// Read from the whole local file when it is there, otherwise — header
    /// local (whole file or cached header span) — from the candidates'
    /// ranges in the span cache, fetching the missing ones through `fetch`
    /// first. Both read the same bytes and score them with the same
    /// function in the same order, so the hits are identical either way: a
    /// query node need not hold the whole file (as large as the f32 vector
    /// index) to answer exactly.
    ///
    /// `None` sends the caller to the postings: the segment stores vectors
    /// inline, the sidecar holds no `field`, its header is not local, or a
    /// read failed (logged).
    fn knn_exact_from_sidecar(
        &self,
        field: &str,
        query: &[f32],
        k: usize,
        candidates: &[u32],
        fetch: Option<VectorSpanFetch<'_>>,
    ) -> Option<ExactKnnResult> {
        if !self.footer.doc_vectors {
            return None;
        }
        let whole = self.segment_dir.join(DOC_VECTORS_FILE).exists();
        let dv = if whole {
            match self.doc_vectors() {
                Ok(dv) => dv,
                Err(e) => {
                    eprintln!(
                        "WARN: exact kNN could not open {DOC_VECTORS_FILE} of {}: {e} — scoring from vector postings instead",
                        self.segment_dir.display()
                    );
                    return None;
                }
            }
        } else {
            match self.local_doc_vectors() {
                Some(dv) => dv,
                None => {
                    // Rare: the scoring hydration fetches the header for a
                    // filtered kNN (best-effort), so this is a shed or
                    // failed fetch, or a caller without hydration.
                    eprintln!(
                        "WARN: exact kNN on '{field}' in {}: {DOC_VECTORS_FILE} header not local \
                         (not hydrated, or its fetch failed) — scoring all {} vector postings \
                         instead of {} candidate vectors",
                        self.segment_dir.display(),
                        self.vector_field(field)
                            .ok()
                            .flatten()
                            .and_then(|index| index.lazy.as_ref().map(|vi| vi.ranges.len()))
                            .unwrap_or(0),
                        candidates.len()
                    );
                    return None;
                }
            }
        };
        if !dv.has_field(field) {
            return None;
        }
        let mut scored: Vec<(u32, f64)> = Vec::with_capacity(candidates.len());
        // A stray vector of another dimension was never indexed (see
        // `SegmentWriter::unindexed_vectors`), so it is not a candidate on
        // the probe path either.
        let score = |doc_seq: u32, v: &[f32]| {
            if v.len() == query.len() {
                scored.push((
                    doc_seq,
                    kosha_vector_spfresh::cosine_similarity(query, v) as f64,
                ));
            }
        };
        let visited = if whole {
            dv.visit_field_vectors(field, candidates, score)
        } else {
            let ranges = dv.field_vector_ranges(field, candidates);
            let mut spans = span_cache::SpanIndex::load(&self.segment_dir, DOC_VECTORS_FILE);
            let _held = if let Some(fetch) = fetch {
                let missing: Vec<(u64, u64)> = ranges
                    .iter()
                    .copied()
                    .filter(|&(start, end)| !spans.covers(start, end))
                    .collect();
                if !missing.is_empty() {
                    // Held until the vectors below are read: the reloaded
                    // index sees what the fetch left only in memory.
                    let _held = fetch(&self.segment_dir, DOC_VECTORS_FILE, &missing);
                    spans = span_cache::SpanIndex::load(&self.segment_dir, DOC_VECTORS_FILE);
                    Some(_held)
                } else {
                    None
                }
            } else {
                None
            };
            dv.visit_field_vectors_cached(&spans, field, candidates, score)
        };
        match visited {
            Ok(_) => {
                let vectors_scored = scored.len();
                Some(ExactKnnResult {
                    hits: top_k_by_score(scored, k),
                    vectors_scored,
                    postings_hydrated: 0,
                    source: ExactKnnSource::Sidecar,
                })
            }
            Err(e) => {
                eprintln!(
                    "WARN: exact kNN could not read {DOC_VECTORS_FILE} of {} ({}): {e} — scoring from vector postings instead",
                    self.segment_dir.display(),
                    if whole { "whole file" } else { "cached ranges" },
                );
                None
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn knn_search_within_lazy(
        &self,
        vi: &LazyVectorIndex,
        query: &[f32],
        k: usize,
        max_distance: f32,
        max_probes: usize,
        accept: Option<&dyn Fn(u32) -> bool>,
        refill: KnnRefill,
        fetch: Option<VectorSpanFetch<'_>>,
    ) -> Result<(Vec<(u32, f64)>, usize), KoshaError> {
        let probed = self.probe_within_lazy(
            vi,
            query,
            k,
            max_distance,
            max_probes,
            accept,
            refill,
            fetch,
            None,
        )?;
        Ok((
            self.finish_probe(vi, query, probed.scored, &probed.errors, k),
            probed.postings,
        ))
    }

    /// The probe half of [`Self::knn_search_within_lazy`]: every candidate
    /// scored, with its error bound, before any cut or rescoring.
    ///
    /// `planned` is the query layer's ranking of this segment's postings
    /// for the same cutoff and cap (#436). With it the budgeted postings
    /// are read straight off the plan; without it (a direct caller, or a
    /// plan that does not fit this column) they are ranked here, from the
    /// same scores by the same rule.
    #[allow(clippy::too_many_arguments)]
    fn probe_within_lazy(
        &self,
        vi: &LazyVectorIndex,
        query: &[f32],
        k: usize,
        max_distance: f32,
        max_probes: usize,
        accept: Option<&dyn Fn(u32) -> bool>,
        refill: KnnRefill,
        fetch: Option<VectorSpanFetch<'_>>,
        planned: Option<&KnnSegmentRanking>,
    ) -> Result<ProbedCandidates, KoshaError> {
        // This segment's postings inside the budget, nearest-first (by
        // lower bound, not raw centroid distance — see
        // `centroid_lower_bounds`), so the `max_probes` cap keeps the
        // *best* candidates when it bites. `max_distance` is itself a
        // lower-bound cutoff (computed from the same bound across all
        // segments — see `Searcher::knn_plan`), so both sides of the
        // comparison are in the same units.
        let own;
        // Timed only when the segment ranks for itself, so a probe handed
        // its ranking reports exactly zero rather than a timer's overhead.
        let mut rank_nanos = 0;
        let ranking = match planned.filter(|r| r.postings == vi.ranges.len()) {
            Some(planned) => planned,
            None => {
                let mut rank_clock = KNN_SCAN_STATS
                    .with(|stats| stats.get().is_some())
                    .then(std::time::Instant::now);
                own = KnnSegmentRanking::new(
                    Self::centroid_lower_bounds(vi, query),
                    max_distance,
                    max_probes,
                );
                rank_nanos = lap_nanos(&mut rank_clock);
                &own
            }
        };
        let budgeted = ranking.budgeted.len();
        let mut held =
            self.prefetch_vector_postings(vi, ranking.budgeted.iter().map(|&i| i as usize), fetch);

        let mut probes = 0usize;
        let mut sink = CandidateSink::new(vi, k);
        let lanes = DotLanes::from_env();
        let unit = Self::unit_query(vi, query);
        let mut scan =
            PostingScan::new(&self.segment_dir, vi, Self::probe_query(vi, query), accept);
        scan.stats.rank_nanos += rank_nanos;
        for &range_idx in &ranking.budgeted {
            probes += 1;
            self.scan_posting(
                &mut scan,
                &held,
                range_idx as usize,
                |v| Self::f32_similarity(lanes, unit.as_deref(), query, v),
                |id, sim, err| sink.push(id, sim, err),
            )?;
        }
        // Past the budget. Without a restriction this is where the probe
        // ends. With one, keep going only while the page is still short
        // (see the doc comment on the public wrapper), and, when the
        // restriction is deletes alone, only in a segment the budget
        // reached at all (see `KnnRefill`). Only here is the rest of the
        // segment ranked: most probes never get this far.
        let refills = accept.is_some()
            && sink.len() < k
            && !(refill == KnnRefill::BudgetedSegments && budgeted == 0)
            && budgeted < vi.ranges.len();
        if refills {
            let mut rank_clock = scan.clock();
            // A plan that scored only its candidates (`from_candidates`)
            // left the rest of this segment unscored: score it now, with
            // the function the plan's own scores came from.
            let scored;
            let bounds = match &ranking.bounds {
                Some(bounds) => bounds,
                None => {
                    scored = Self::centroid_lower_bounds(vi, query);
                    &scored
                }
            };
            let ranked = KnnSegmentRanking::rank_all(bounds);
            scan.stats.rank_nanos += lap_nanos(&mut rank_clock);
            for position in budgeted..ranked.len() {
                if sink.len() >= k {
                    break;
                }
                if (position - budgeted) % VECTOR_PREFETCH_CHUNK == 0 {
                    let chunk =
                        &ranked[position..(position + VECTOR_PREFETCH_CHUNK).min(ranked.len())];
                    // Every posting held so far has been scanned.
                    held = self.prefetch_vector_postings(
                        vi,
                        chunk.iter().map(|&(i, _)| i as usize),
                        fetch,
                    );
                }
                probes += 1;
                self.scan_posting(
                    &mut scan,
                    &held,
                    ranked[position].0 as usize,
                    |v| Self::f32_similarity(lanes, unit.as_deref(), query, v),
                    |id, sim, err| sink.push(id, sim, err),
                )?;
            }
        }
        drop(held);
        let (scored, errors) = sink.finish();
        Ok(ProbedCandidates {
            scored,
            errors,
            postings: probes,
        })
    }

    // Deliberately NOT radius-aware: this is the per-segment `nprobe`
    // fallback used only when `KOSHA_KNN_GLOBAL_BUDGET=0` disables global
    // budgeting entirely (an escape hatch, not the default query path).
    // `kosha_vector_spfresh::nearest_centroids`/`nearest_centroids_dot`
    // rank by raw distance and don't take a radius parameter — extending
    // them is separate, lower-priority scope than the global-budget path
    // this fix targets.
    fn knn_search_lazy(
        &self,
        vi: &LazyVectorIndex,
        query: &[f32],
        k: usize,
        nprobe: usize,
        accept: Option<&dyn Fn(u32) -> bool>,
        fetch: Option<VectorSpanFetch<'_>>,
    ) -> Result<Vec<(u32, f64)>, KoshaError> {
        // Unit-norm segments: normalize the query once, then every stored
        // comparison is a bare dot product — same cosine scores (clamped
        // identically), none of the per-comparison norms + sqrts.
        let normalized_query: Vec<f32>;
        let query: &[f32] = if vi.unit_norm {
            let mut q = query.to_vec();
            kosha_vector_spfresh::normalize_in_place(&mut q);
            normalized_query = q;
            &normalized_query
        } else {
            query
        };
        let probed = self.probe_nprobe_lazy(vi, query, k, nprobe, accept, fetch)?;
        Ok(self.finish_probe(vi, query, probed.scored, &probed.errors, k))
    }

    /// The probe half of [`Self::knn_search_lazy`]. `postings` counts the
    /// postings actually scored. A unit-norm column expects `query`
    /// already unit-normalized (scores are bare dot products).
    fn probe_nprobe_lazy(
        &self,
        vi: &LazyVectorIndex,
        query: &[f32],
        k: usize,
        nprobe: usize,
        accept: Option<&dyn Fn(u32) -> bool>,
        fetch: Option<VectorSpanFetch<'_>>,
    ) -> Result<ProbedCandidates, KoshaError> {
        // With a restriction, rank every posting so the probe can continue
        // past `nprobe` while the page is still short, as the budgeted
        // variant does.
        let rank = if accept.is_some() {
            vi.ranges.len()
        } else {
            nprobe
        };
        let candidates = if vi.dim == 0 {
            // A zero-dimension column (no writer produces one, but the
            // parsers accept it) has a centroid per posting and nothing in
            // the matrix to tell them apart by: rank the empty rows.
            let rows = vec![Vec::new(); vi.ranges.len()];
            if vi.unit_norm {
                kosha_vector_spfresh::nearest_centroids_dot(&rows, query, rank)
            } else {
                kosha_vector_spfresh::nearest_centroids(&rows, query, rank)
            }
        } else if vi.unit_norm {
            kosha_vector_spfresh::nearest_centroids_dot_rows(&vi.centroids, vi.dim, query, rank)
        } else {
            kosha_vector_spfresh::nearest_centroids_rows(&vi.centroids, vi.dim, query, rank)
        };
        let budgeted = nprobe.min(candidates.len());
        let mut held =
            self.prefetch_vector_postings(vi, candidates[..budgeted].iter().copied(), fetch);
        let mut sink = CandidateSink::new(vi, k);
        // `query` is already unit-normalized for every unit-norm column,
        // which includes every quantized one.
        let mut scan =
            PostingScan::new(&self.segment_dir, vi, Self::probe_query(vi, query), accept);
        let mut postings = 0usize;
        let lanes = DotLanes::from_env();
        for (position, &range_idx) in candidates.iter().enumerate() {
            if position >= nprobe && sink.len() >= k {
                break;
            }
            if position >= budgeted && (position - budgeted) % VECTOR_PREFETCH_CHUNK == 0 {
                let chunk =
                    &candidates[position..(position + VECTOR_PREFETCH_CHUNK).min(candidates.len())];
                // Every posting held so far has been scanned.
                held = self.prefetch_vector_postings(vi, chunk.iter().copied(), fetch);
            }
            postings += 1;
            self.scan_posting(
                &mut scan,
                &held,
                range_idx,
                |v| {
                    if vi.unit_norm {
                        lanes.dot(query, v).clamp(-1.0, 1.0)
                    } else {
                        lanes.cosine_similarity(query, v)
                    }
                },
                |id, sim, err| sink.push(id, sim, err),
            )?;
        }
        drop(held);
        let (scored, errors) = sink.finish();
        Ok(ProbedCandidates {
            scored,
            errors,
            postings,
        })
    }

    /// `query` prepared for scoring `vi`'s postings: unit-normalized when
    /// `vi` is quantized (whose entries score against a unit query;
    /// otherwise unused, so not computed), plus the rotated lookup table
    /// for a RaBitQ column (see [`rabitq_query_for`]).
    fn probe_query<'q>(vi: &LazyVectorIndex, query: &'q [f32]) -> ProbeQuery<'q> {
        let unit = if vi.quantized {
            let mut q = query.to_vec();
            kosha_vector_spfresh::normalize_in_place(&mut q);
            std::borrow::Cow::Owned(q)
        } else {
            std::borrow::Cow::Borrowed(query)
        };
        // A query of another dimension scores nothing here, as the f32
        // paths' dimension checks would have it.
        let rabitq = (vi.rabitq && unit.len() == vi.dim).then(|| rabitq_query_for(&unit));
        let i8_error_per_scale = if vi.quantized && !vi.rabitq {
            i8_error_per_scale(&unit)
        } else {
            0.0
        };
        ProbeQuery {
            unit,
            rabitq,
            i8_error_per_scale,
        }
    }

    /// The probe's top `k` of `scored`, best first.
    ///
    /// An f32 column's scores are exact: sort and cut, as always. An i8
    /// column's are approximate, so its best `k * rescore_factor` are
    /// rescored with their full-precision vectors from `doc_vectors.bin`
    /// (#210) before the cut — the quantization error then only decides
    /// which candidates make that shortlist, not their final order.
    /// Rescoring needs the sidecar locally and holding this field; without
    /// it (inline-vector segments, a factor of `0`, a sidecar not yet
    /// hydrated) the quantized scores stand.
    ///
    /// A RaBitQ column's shortlist grows to as many candidates as its error
    /// bounds (`errors[i]`, parallel to `scored`) cannot rule out of the
    /// top `k` — upper bound at or above the `k`-th best lower bound —
    /// taken best-estimate first and capped at [`RABITQ_SHORTLIST_CAP`]
    /// times the factor-sized list.
    fn finish_probe(
        &self,
        vi: &LazyVectorIndex,
        query: &[f32],
        mut scored: Vec<(u32, f64)>,
        errors: &[f32],
        k: usize,
    ) -> Vec<(u32, f64)> {
        let by_score_desc = |a: &(u32, f64), b: &(u32, f64)| {
            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
        };
        if !vi.quantized || self.vector_rescore_factor == 0 {
            scored.sort_by(by_score_desc);
            scored.truncate(k);
            return scored;
        }
        let factor_len = k.saturating_mul(self.vector_rescore_factor);
        let shortlist_len = if vi.rabitq && errors.len() == scored.len() {
            factor_len
                .max(rabitq_bound_shortlist_len(&scored, errors, k))
                .min(factor_len.saturating_mul(RABITQ_SHORTLIST_CAP))
        } else {
            factor_len
        };
        let mut shortlist = top_k_by_score(scored, shortlist_len);
        if let Some(field) = vi.field.as_deref() {
            self.rescore_exact(field, query, &mut shortlist);
        }
        shortlist.sort_by(by_score_desc);
        shortlist.truncate(k);
        shortlist
    }

    /// Whether the default per-segment kNN path (`finish_probe`) would
    /// rescore a quantized column's shortlist here (#381 observability):
    /// the same gate as [`Self::rescore_exact`], i.e. rescoring is on and
    /// the WHOLE `doc_vectors.bin` is local. When this is `false` the
    /// per-segment path returns its quantized estimates as final scores.
    pub fn rescores_quantized_locally(&self) -> bool {
        self.vector_rescore_factor > 0
            && self.footer.doc_vectors
            && self.segment_dir.join(DOC_VECTORS_FILE).exists()
    }

    /// Replaces each candidate's score with the cosine similarity of
    /// `query` and its full-precision vector in `doc_vectors.bin`, where
    /// the sidecar has one. Returns how many were rescored; `0` (scores
    /// untouched) when the sidecar is absent, lacks `field`, or fails to
    /// read.
    fn rescore_exact(&self, field: &str, query: &[f32], candidates: &mut [(u32, f64)]) -> usize {
        if candidates.is_empty()
            || !self.footer.doc_vectors
            || !self.segment_dir.join(DOC_VECTORS_FILE).exists()
        {
            return 0;
        }
        let dv = match self.doc_vectors() {
            Ok(dv) if dv.has_field(field) => dv,
            Ok(_) => return 0,
            Err(e) => {
                eprintln!(
                    "WARN: cannot rescore quantized kNN candidates of {}: {e} — keeping quantized scores",
                    self.segment_dir.display()
                );
                return 0;
            }
        };
        let mut doc_seqs: Vec<u32> = candidates.iter().map(|(id, _)| *id).collect();
        doc_seqs.sort_unstable();
        doc_seqs.dedup();
        let mut exact: HashMap<u32, f64> = HashMap::with_capacity(doc_seqs.len());
        let visited = dv.visit_field_vectors(field, &doc_seqs, |doc_seq, v| {
            if v.len() == query.len() {
                exact.insert(
                    doc_seq,
                    kosha_vector_spfresh::cosine_similarity(query, v) as f64,
                );
            }
        });
        if let Err(e) = visited {
            eprintln!(
                "WARN: cannot rescore quantized kNN candidates of {}: {e} — keeping quantized scores",
                self.segment_dir.display()
            );
            return 0;
        }
        let mut rescored = 0;
        for (id, score) in candidates.iter_mut() {
            if let Some(&s) = exact.get(id) {
                *score = s;
                rescored += 1;
            }
        }
        rescored
    }

    /// The segment's filter columns, parsed on first use (see
    /// [`LazyFilters`]); an unreadable store logs and reads as empty, so
    /// only tests use it. Search and admin paths use the fallible
    /// [`Self::filter_store_for`] / [`Self::try_filter_store`].
    #[cfg(test)]
    fn filter_store(&self) -> Arc<FilterStore> {
        self.try_filter_store().unwrap_or_else(|e| {
            eprintln!(
                "ERROR: filter columns of {} unreadable: {e}",
                self.segment_dir.display()
            );
            Arc::new(FilterStore::default())
        })
    }

    /// Every filter column of the segment. A damaged (#244) or not-local
    /// filter file is an error, never an empty store, so the request fails
    /// closed.
    pub fn try_filter_store(&self) -> Result<Arc<FilterStore>, KoshaError> {
        self.filters.store_for(None)
    }

    /// The compatibility store with at least the columns named in
    /// `fields` (#244) — see `SearchQuery::filter_column_fields`. On a
    /// columnar segment this reads only those columns, which is all a
    /// query node hydrated for the query; a legacy segment returns all of
    /// `filters.bin`. Columns loaded by earlier calls stay in the store.
    pub fn filter_store_for(
        &self,
        fields: &std::collections::BTreeSet<String>,
    ) -> Result<Arc<FilterStore>, KoshaError> {
        self.filters.store_for(Some(fields))
    }

    /// How many columns of a columnar segment have been merged into the
    /// row-shaped compatibility store ([`Self::filter_store_for`]); `0` for
    /// a legacy segment. Diagnostics and tests.
    pub fn merged_filter_store_columns(&self) -> usize {
        self.filters.state.lock().unwrap().merged_ids.len()
    }

    /// One filter column, decoded on first use and cached (#244). Works on
    /// both formats: a legacy segment builds it from `filters.bin`.
    /// `Ok(None)` when the segment has no such column. `Err` when its file
    /// is not local (never cached, so a later call sees it once hydrated)
    /// or is present but damaged.
    pub fn filter_column(
        &self,
        field: &str,
        kind: kosha_core::FilterColumnKind,
    ) -> Result<Option<Arc<filter_columns::FilterColumn>>, KoshaError> {
        self.filters.column(field, kind)
    }

    /// The documents of this segment that pass `clause` (#244).
    ///
    /// The one entry point for search and delete-by-query. A columnar
    /// segment answers from its columns — dictionary lookups and term
    /// bitmaps, zone-map-pruned numeric scans, presence bitmaps (see
    /// [`filter_eval`]) — reading only the columns the clause names. A
    /// legacy `filters.bin` segment keeps the row scan of
    /// [`kosha_core::evaluate_filter`]. Both return the same set; a damaged
    /// column fails the call instead of matching nothing.
    pub fn evaluate_filter(&self, clause: &FilterClause) -> Result<DocSet, KoshaError> {
        if self.has_filter_columns() {
            filter_eval::check_filter_columns(clause, self)?;
            return filter_eval::evaluate_filter_columns(clause, self, self.doc_count());
        }
        let mut fields = std::collections::BTreeSet::new();
        clause.for_each_field(&mut |f| {
            fields.insert(f.to_string());
            fields.insert(kosha_core::base_field_name(f).to_string());
        });
        let store = self.filter_store_for(&fields)?;
        kosha_core::check_filter(clause, &store).map_err(KoshaError::InvalidQuery)?;
        Ok(kosha_core::evaluate_filter(
            clause,
            &store,
            self.doc_count(),
        ))
    }

    /// Whether this segment stores filter values in the columnar format.
    pub fn has_filter_columns(&self) -> bool {
        self.footer.filter_columns.is_some()
    }

    pub fn footer(&self) -> &Footer {
        &self.footer
    }
    /// The segment's on-disk directory (`{data_dir}/{namespace}/{segment_id}`).
    /// Used by the query path to identify which segments hold the materialize
    /// page hits, so `doc_store.bin` can be fetched on demand for just those
    /// segments instead of being hydrated for the whole manifest up front —
    /// see `Searcher::search_with_doc_store_hydrator`.
    pub fn segment_dir(&self) -> &Path {
        &self.segment_dir
    }
    pub fn doc_count(&self) -> u32 {
        self.footer.doc_count
    }
    /// Whether this segment's per-term postings lists are guaranteed to be
    /// sorted by ascending `doc_id` (doc_seq). True for every format the
    /// current `SegmentWriter` emits (inline-lazy v2 and split v3/v4 —
    /// postings are written in insertion order, and varint-delta encoding
    /// *requires* ascending ids). False for legacy v1 `Eager` segments,
    /// whose postings are parsed from an external stream with no ordering
    /// validation — order-sensitive algorithms (the WAND leapfrog join)
    /// must fall back to order-agnostic paths for those.
    pub fn has_ordered_postings(&self) -> bool {
        !matches!(self.inverted, InvertedAccess::Eager(_))
    }
    pub fn avg_field_length(&self) -> f64 {
        self.footer.avg_field_length
    }
    pub fn bm25_params(&self) -> &Bm25Params {
        &self.footer.bm25_params
    }

    /// Postings for one term, as a slice-deref handle (see [`PostingsRef`]):
    /// legacy (v1) segments lend a borrow into their parsed map; v2 segments
    /// hand out a shared `Arc` of the on-demand decode, served from a small
    /// per-segment LRU when the term was queried recently (see
    /// [`PostingsCache`]). Call once per term per query and hold the
    /// result — don't re-fetch per document.
    pub fn postings(&self, term: &str) -> Option<PostingsRef<'_>> {
        self.inverted.postings(term)
    }

    /// Fetch postings for a batch of terms. Split-postings segments group
    /// the batch by posting shard so wildcard expansions read/decompress
    /// each shard once instead of once per matching term.
    pub fn postings_for_terms<'a>(
        &'a self,
        terms: &'a [String],
    ) -> Vec<(&'a str, PostingsRef<'a>)> {
        self.inverted.postings_for_terms(terms)
    }

    /// Postings for the BM25/WAND scoring pass: `(doc_id, tf)` only. On v5
    /// (skip-split) segments this never decodes positions and hands the
    /// scorer per-block skip summaries (upper-bound inputs + last doc ids)
    /// so pruned or galloped-over blocks are never varint-decoded at all;
    /// on older formats it degrades to the classic full decode.
    /// Drain the split index's "a TOC-present term's posting blob failed to
    /// read" flag. Queries must check this after fetching postings and
    /// surface an error: silently omitting an unreadable term returns
    /// wrong results that look like normal non-matches (the PR #83
    /// failure class — this is what makes presence caching safe against
    /// disk-cache eviction races). Inline formats never flag.
    pub fn take_blob_read_failure(&self) -> bool {
        match &self.inverted {
            InvertedAccess::Split(split) => split.take_blob_read_failure(),
            _ => false,
        }
    }

    /// On-disk span length of `term`'s postings, from the TOC this reader
    /// already holds: split (v3+) segments only, `None` otherwise or when
    /// the term is absent. Read-only metadata for offline analysis tools
    /// (research #387); the query path does not use it.
    pub fn posting_span_len(&self, term: &str) -> Option<u64> {
        match &self.inverted {
            InvertedAccess::Split(split) => split.find(term).map(|i| split.raw_entry(i).4 as u64),
            _ => None,
        }
    }

    pub fn scoring_postings_for_terms<'a>(
        &'a self,
        terms: &'a [String],
    ) -> Vec<(&'a str, ScoringPostingsRef<'a>)> {
        if let InvertedAccess::Split(split) = &self.inverted {
            if let Some(blocked) = split.scoring_postings_for_terms(terms) {
                return blocked
                    .into_iter()
                    .map(|(t, b)| (t, ScoringPostingsRef::Blocked(b)))
                    .collect();
            }
        }
        self.postings_for_terms(terms)
            .into_iter()
            .map(|(t, p)| (t, ScoringPostingsRef::Decoded(p)))
            .collect()
    }

    /// Document frequency of each of `terms` that this segment contains,
    /// for namespace-level BM25 statistics (issue #246). Goes through the
    /// same scoring-postings fetch the scoring pass uses, so on v5 segments
    /// only the term's span header is decoded and on older formats the
    /// decode lands in the postings cache the scoring pass then hits.
    /// `None` when a TOC-present term's posting blob could not be read —
    /// a partial answer would silently understate `df`.
    pub fn doc_frequencies(&self, terms: &[String]) -> Option<Vec<(String, u32)>> {
        let dfs: Vec<(String, u32)> = self
            .scoring_postings_for_terms(terms)
            .into_iter()
            .map(|(term, postings)| {
                (
                    term.to_string(),
                    u32::try_from(postings.doc_count()).unwrap_or(u32::MAX),
                )
            })
            .collect();
        if self.take_blob_read_failure() {
            return None;
        }
        Some(dfs)
    }

    /// Zero-I/O: `doc_id` + `field_length` for one document, without
    /// touching its full field content on disk. This is all BM25 scoring
    /// and default/`_id` sorting ever need — see `DocMetaRef`.
    pub fn doc_meta(&self, doc_seq: u32) -> Option<DocMetaRef<'_>> {
        match &self.doc_store {
            DocStoreAccess::Eager(records) => records.get(doc_seq as usize).map(|r| DocMetaRef {
                doc_id: &r.doc_id.0,
                doc_seq: r.doc_seq,
                field_length: r.field_length,
            }),
            DocStoreAccess::Lazy { index, .. } => {
                let entry = index.entries.get(doc_seq as usize)?;
                Some(DocMetaRef {
                    doc_id: index.id(doc_seq as usize)?,
                    doc_seq,
                    field_length: entry.field_length,
                })
            }
        }
    }

    /// On-demand: the document's full field content (`content` text + all
    /// metadata). Reads and parses only this one document's byte span from
    /// disk for `Lazy` segments — use only for documents actually being
    /// materialized into a response, not during scoring.
    pub fn doc_record_full(&self, doc_seq: u32) -> Result<Option<DocRecord>, KoshaError> {
        self.page_doc_record(doc_seq, true, &mut PageSpans::default())
    }

    /// [`Self::doc_record_full`] for one document of a result page:
    /// span-cache listings are kept in `spans` across the page's documents
    /// of this segment (one listing per file per page, not per document),
    /// and with `vectors` false the sidecar vectors are not read at all —
    /// their `Vector` fields stay empty placeholders, for a page whose
    /// `_source` returns none of them (see [`Self::page_reads_doc_vectors`]).
    pub fn page_doc_record(
        &self,
        doc_seq: u32,
        vectors: bool,
        spans: &mut PageSpans,
    ) -> Result<Option<DocRecord>, KoshaError> {
        let mut record = self.doc_record_stored_reusing(doc_seq, &mut spans.doc_store)?;
        if let Some(record) = record.as_mut().filter(|_| vectors) {
            for (index, vector) in self.sidecar_vectors_reusing(record, &mut spans.doc_vectors)? {
                record.fields[index].value = serde_json::to_string(&vector)?;
            }
        }
        Ok(record)
    }

    /// Whether a result page returning the fields `returns` keeps needs this
    /// segment's sidecar vectors: some `Vector` field it stores in
    /// `doc_vectors.bin` is returned. Only single-vector fields go there,
    /// each with a column in the footer (a bag stays inline), so the
    /// footer's column names cover the sidecar's; a legacy footer that
    /// lists none cannot say, so it reads them. `false` without a sidecar.
    pub fn page_reads_doc_vectors(&self, returns: impl Fn(&str) -> bool) -> bool {
        self.footer.doc_vectors
            && self
                .footer
                .vector_fields
                .as_ref()
                .is_none_or(|cols| cols.iter().any(|c| returns(&c.name)))
    }

    /// The record as stored in `doc_store.bin`, vector placeholders and all.
    fn doc_record_stored(&self, doc_seq: u32) -> Result<Option<DocRecord>, KoshaError> {
        self.doc_record_stored_reusing(doc_seq, &mut None)
    }

    /// [`Self::doc_record_stored`] keeping the span listing in `spans`.
    fn doc_record_stored_reusing(
        &self,
        doc_seq: u32,
        spans: &mut Option<span_cache::SpanIndex>,
    ) -> Result<Option<DocRecord>, KoshaError> {
        match &self.doc_store {
            DocStoreAccess::Eager(records) => Ok(records.get(doc_seq as usize).cloned()),
            DocStoreAccess::Lazy {
                doc_store_path,
                index,
                decoder,
            } => {
                let Some(entry) = index.entries.get(doc_seq as usize) else {
                    return Ok(None);
                };
                // The whole file when it is complete, else the sparse span
                // cache (#245). Neither → the plain read below, which fails
                // with the same IO error as before spans existed.
                let buf = match span_cache::read_local_range_reusing(
                    &self.segment_dir,
                    DOC_STORE_FILE,
                    index.complete_len(),
                    entry.offset,
                    entry.length as u64,
                    spans,
                ) {
                    Some(buf) => buf,
                    None => {
                        let mut file = fs::File::open(doc_store_path)?;
                        file.seek(SeekFrom::Start(entry.offset))?;
                        let mut buf = vec![0u8; entry.length as usize];
                        file.read_exact(&mut buf)?;
                        buf
                    }
                };
                parse_stored_doc_record(decoder.as_deref(), &buf, doc_seq).map(Some)
            }
        }
    }

    /// The document's exact byte span within `doc_store.bin`, from the
    /// offsets sidecar — `None` for out-of-range `doc_seq` or `Eager`
    /// (legacy) segments, whose records are already fully resident. This is
    /// what lets a remote store serve one document with a ranged read of
    /// `length` bytes instead of shipping the whole doc store.
    pub fn doc_span(&self, doc_seq: u32) -> Option<(u64, u32)> {
        match &self.doc_store {
            DocStoreAccess::Eager(_) => None,
            DocStoreAccess::Lazy { index, .. } => {
                let entry = index.entries.get(doc_seq as usize)?;
                Some((entry.offset, entry.length))
            }
        }
    }

    /// Whether `doc_record_full` can be served without any remote fetch:
    /// `Eager` segments hold every record in memory; `Lazy` segments need
    /// `doc_store.bin` present on local disk.
    pub fn has_local_doc_store(&self) -> bool {
        // A sidecar segment also needs `doc_vectors.bin` to produce complete
        // records (#243).
        if self.footer.doc_vectors
            && !std::fs::metadata(self.segment_dir.join(DOC_VECTORS_FILE))
                .is_ok_and(|m| m.len() > 0)
        {
            return false;
        }
        match &self.doc_store {
            DocStoreAccess::Eager(_) => true,
            DocStoreAccess::Lazy {
                doc_store_path,
                index,
                ..
            } => {
                // Existence alone is not enough: an interrupted hydration
                // write can leave a zero-byte or truncated `doc_store.bin`
                // on disk, and treating it as present makes every
                // materialize of a doc past the truncation point fail
                // ("failed to fill whole buffer") forever. The offsets
                // sidecar knows the exact expected file size — the last
                // record's end — so validate against it.
                let expected = index.complete_len();
                std::fs::metadata(doc_store_path)
                    .map(|m| m.len() >= expected && m.len() > 0)
                    .unwrap_or(false)
            }
        }
    }

    /// Whether `doc_record_full(doc_seq)` can be served from local disk —
    /// the whole doc store (see [`Self::has_local_doc_store`]), or this
    /// document's spans in the sparse span cache (#245): its record bytes
    /// and, for a sidecar segment, the vectors header plus its vector
    /// bytes. Out-of-range `doc_seq`s are "local" (there is nothing to
    /// fetch; `doc_record_full` returns `None`).
    pub fn doc_is_local(&self, doc_seq: u32) -> bool {
        self.page_doc_is_local(doc_seq, true, &mut PageSpans::default())
    }

    /// [`Self::doc_is_local`] for one document of a result page, keeping
    /// span listings in `spans` across the page's documents of this
    /// segment. With `vectors` false (the page returns no sidecar field,
    /// see [`Self::page_reads_doc_vectors`]) only the record must be local.
    pub fn page_doc_is_local(&self, doc_seq: u32, vectors: bool, spans: &mut PageSpans) -> bool {
        let vectors = vectors && self.footer.doc_vectors;
        let DocStoreAccess::Lazy { index, .. } = &self.doc_store else {
            // Eager record: local; its vectors only with the whole sidecar.
            return !vectors || self.has_local_doc_store();
        };
        let Some(entry) = index.entries.get(doc_seq as usize) else {
            return true;
        };
        let record_local =
            file_is_complete(&self.segment_dir.join(DOC_STORE_FILE), index.complete_len())
                || spans
                    .doc_store
                    .get_or_insert_with(|| {
                        span_cache::SpanIndex::load(&self.segment_dir, DOC_STORE_FILE)
                    })
                    .covers(entry.offset, entry.offset + entry.length as u64);
        if !record_local {
            return false;
        }
        if !vectors {
            return true;
        }
        let Some(dv) = self.local_doc_vectors() else {
            return false;
        };
        match dv.doc_span(doc_seq) {
            None => true,
            Some((start, end)) => {
                file_is_complete(&self.segment_dir.join(DOC_VECTORS_FILE), dv.file_len())
                    || spans
                        .doc_vectors
                        .get_or_insert_with(|| {
                            span_cache::SpanIndex::load(&self.segment_dir, DOC_VECTORS_FILE)
                        })
                        .covers(start, end)
            }
        }
    }

    /// Length `doc_store.bin` has when complete, from the offsets sidecar;
    /// `None` for `Eager` (legacy) segments.
    pub fn doc_store_len(&self) -> Option<u64> {
        match &self.doc_store {
            DocStoreAccess::Eager(_) => None,
            DocStoreAccess::Lazy { index, .. } => Some(index.complete_len()),
        }
    }

    /// Whether this segment keeps vectors in `doc_vectors.bin` (#243).
    pub fn uses_doc_vectors(&self) -> bool {
        self.footer.doc_vectors
    }

    /// The sidecar vectors' `[start, end)` byte range for `doc_seq` in
    /// `doc_vectors.bin`, read from a locally available header (whole file
    /// or cached header span). `Ok(None)`: the document has no sidecar
    /// vectors. `Err`: the header is not local (fetch
    /// `[0, doc_vectors_header_len)` first) or is corrupt.
    pub fn doc_vectors_span(&self, doc_seq: u32) -> Result<Option<(u64, u64)>, KoshaError> {
        if !self.footer.doc_vectors {
            return Ok(None);
        }
        Ok(self.doc_vectors()?.doc_span(doc_seq))
    }

    /// Whether the sidecar header is readable locally. Always `true` for
    /// segments without a sidecar.
    pub fn has_local_doc_vectors_header(&self) -> bool {
        !self.footer.doc_vectors || self.local_doc_vectors().is_some()
    }

    /// Parse one document's full record from bytes fetched elsewhere — the
    /// remote-ranged-read counterpart of `doc_record_full`. `bytes` must be
    /// exactly the span [`Self::doc_span`] reported for this `doc_seq`
    /// (checked: a wrong-length buffer means the caller fetched the wrong
    /// range, and a "successful" parse of it would be garbage).
    pub fn doc_record_from_bytes(
        &self,
        doc_seq: u32,
        bytes: &[u8],
    ) -> Result<Option<DocRecord>, KoshaError> {
        let Some((_, length)) = self.doc_span(doc_seq) else {
            return Ok(None);
        };
        if bytes.len() != length as usize {
            return Err(KoshaError::NotFound(format!(
                "doc_seq {doc_seq} span is {length} bytes but caller supplied {} in segment {:?}",
                bytes.len(),
                self.segment_dir
            )));
        }
        let decoder = match &self.doc_store {
            DocStoreAccess::Lazy { decoder, .. } => decoder.as_deref(),
            DocStoreAccess::Eager(_) => None,
        };
        let mut record = parse_stored_doc_record(decoder, bytes, doc_seq)?;
        self.fill_doc_vectors(&mut record)?;
        Ok(Some(record))
    }

    /// Iterate every document's zero-I/O metadata (`doc_id`/`field_length`)
    /// in doc_seq order.
    pub fn iter_doc_meta(&self) -> impl Iterator<Item = DocMetaRef<'_>> + '_ {
        (0..self.doc_count()).filter_map(move |seq| self.doc_meta(seq))
    }

    /// Iterate every document's full field content in doc_seq order.
    /// Streams one document at a time (bounded memory) for `Lazy` segments,
    /// rather than requiring the whole segment materialized up front — use
    /// this instead of collecting into a `Vec<DocRecord>` when processing
    /// every document in a segment (e.g. compaction).
    ///
    /// Yields `Err` for any `doc_seq` in `0..doc_count()` that fails to
    /// produce a record — a read/parse error, or (shouldn't happen, but
    /// checked rather than assumed) `doc_count` disagreeing with the actual
    /// number of stored records.
    ///
    /// Bug history: this used to be `impl Iterator<Item = DocRecord>` that
    /// silently `filter_map`'d any such failure away. Compaction
    /// (`kosha-write::compact_namespace_with_options`) and `/replace`
    /// (`rewrite_documents`) both drive this to rebuild a segment from every
    /// document in their inputs — silently dropping a doc here meant it
    /// just didn't exist in the merge/rewrite output, with no error and no
    /// log line. That's the mechanism behind a real production incident:
    /// ~0.24% of a 10M-doc benchmark corpus vanished across several tiered
    /// compaction rounds, with every round reporting success. Every caller
    /// must now propagate `Err` (e.g. via `?`) instead of continuing past
    /// it, so a read failure aborts the operation loudly rather than
    /// quietly shrinking the corpus.
    pub fn iter_doc_records(&self) -> impl Iterator<Item = Result<DocRecord, KoshaError>> + '_ {
        (0..self.doc_count()).map(move |seq| match self.doc_record_full(seq) {
            Ok(Some(rec)) => Ok(rec),
            Ok(None) => Err(KoshaError::NotFound(format!(
                "doc_seq {seq} missing from doc_store in segment {:?} (doc_count={})",
                self.segment_dir,
                self.doc_count()
            ))),
            Err(e) => Err(e),
        })
    }

    /// [`Self::iter_doc_records`] for merges: each record as stored, with
    /// its sidecar vectors returned as f32 next to it instead of printed
    /// into the placeholder fields as JSON (#242). Feed the result to
    /// [`SegmentWriter::add_document_with_vectors`]. Inline-stored vectors
    /// (`KOSHA_VECTOR_STORE=inline`, pre-#243 segments) stay in their
    /// fields as JSON, as before. Same error contract as
    /// `iter_doc_records`: callers must propagate every `Err`.
    pub fn iter_doc_records_raw(
        &self,
    ) -> impl Iterator<Item = Result<RawDocRecord, KoshaError>> + '_ {
        (0..self.doc_count()).map(move |seq| {
            let record = self.doc_record_stored(seq)?.ok_or_else(|| {
                KoshaError::NotFound(format!(
                    "doc_seq {seq} missing from doc_store in segment {:?} (doc_count={})",
                    self.segment_dir,
                    self.doc_count()
                ))
            })?;
            let vectors = self.sidecar_vectors(&record)?;
            Ok(RawDocRecord { record, vectors })
        })
    }

    pub fn contains_term(&self, term: &str) -> bool {
        self.inverted.contains_term(term)
    }

    /// The whole term table, sorted, read in place (no copy for v2+
    /// segments). See [`TermDictionary`].
    pub fn term_dictionary(&self) -> TermDictionary<'_> {
        TermDictionary(match &self.inverted {
            InvertedAccess::Lazy(lazy) => TermDictionaryInner::Lazy(lazy),
            InvertedAccess::Split(split) => TermDictionaryInner::Split(split),
            InvertedAccess::Eager(_) => TermDictionaryInner::Sorted(self.inverted.all_terms()),
        })
    }

    /// All combined-index terms in sorted order (per-field keys, #249,
    /// are left out). Zero-copy for v2 segments (borrows the term pool);
    /// allocates only the pointer Vec.
    pub fn all_terms(&self) -> Vec<&str> {
        let mut terms = self.inverted.all_terms();
        terms.retain(|t| !kosha_core::fields::is_field_key(t));
        terms
    }

    /// This segment's per-field index (#249), or `None` for a segment
    /// with only the combined index.
    pub fn field_index(&self) -> Option<&kosha_core::FieldIndex> {
        self.footer.field_index.as_ref()
    }

    /// Every document's length in `field`, indexed by doc_seq (0 for a
    /// document without the field), decoded from the field's norms list
    /// once and kept for later queries. `Ok(None)` when the segment has
    /// no per-field index. An error when the norms' posting blob can't be
    /// read: scoring without lengths would silently skew every score.
    pub fn field_lengths(&self, field: &str) -> Result<Option<Arc<Vec<u32>>>, KoshaError> {
        if self.footer.field_index.is_none() {
            return Ok(None);
        }
        if let Some(found) = self.field_lengths.lock().unwrap().get(field) {
            return Ok(Some(Arc::clone(found)));
        }
        let mut lengths = vec![0u32; self.doc_count() as usize];
        if let Some(postings) = self.postings(&kosha_core::fields::field_norms_key(field)) {
            for posting in postings.iter() {
                if let Some(slot) = lengths.get_mut(posting.doc_id as usize) {
                    *slot = posting.term_frequency;
                }
            }
        }
        if self.take_blob_read_failure() {
            return Err(KoshaError::CorruptSegment(format!(
                "segment {:?}: norms of field {field:?} unreadable (evicted or corrupt) — \
                 retry after rehydration",
                self.segment_dir
            )));
        }
        let lengths = Arc::new(lengths);
        self.field_lengths
            .lock()
            .unwrap()
            .insert(field.to_string(), Arc::clone(&lengths));
        Ok(Some(lengths))
    }

    /// Read `footer.json` without opening the rest of the segment.
    pub fn read_footer(segment_dir: &Path) -> Result<Footer, KoshaError> {
        let json = fs::read_to_string(segment_dir.join("footer.json"))?;
        Ok(serde_json::from_str(&json)?)
    }

    /// Rebuild `filter_blooms` from `filters.bin` and rewrite `footer.json` only.
    ///
    /// Used to unlock segment pruning on segments written before blooms existed.
    pub fn rewrite_filter_blooms(segment_dir: &Path) -> Result<Footer, KoshaError> {
        let mut footer = Self::read_footer(segment_dir)?;
        let store = Self::read_filters_any(segment_dir, &footer)?;
        footer.filter_blooms = Some(build_filter_blooms(&store.string_fields));
        let json = serde_json::to_string_pretty(&footer)?;
        atomic_write(&segment_dir.join("footer.json"), json.as_bytes())?;
        Ok(footer)
    }

    /// Rebuild `term_bloom` from `inverted.idx` and rewrite `footer.json` only.
    ///
    /// Used to unlock lexical segment pruning on segments written before term
    /// blooms existed.
    pub fn rewrite_term_bloom(segment_dir: &Path) -> Result<Footer, KoshaError> {
        let mut footer = Self::read_footer(segment_dir)?;
        let inverted = Self::read_inverted(segment_dir, None, false)?;
        let combined: Vec<&str> = inverted
            .all_terms()
            .into_iter()
            .filter(|term| !kosha_core::fields::is_field_key(term))
            .collect();
        footer.term_bloom = Some(build_term_bloom(combined.iter().copied()));
        // Segments written before #201 gain fuzzy pruning here too.
        footer.fuzzy_bloom = kosha_core::fuzzy::build_fuzzy_bloom(combined.iter().copied());
        let json = serde_json::to_string_pretty(&footer)?;
        atomic_write(&segment_dir.join("footer.json"), json.as_bytes())?;
        Ok(footer)
    }

    /// Rebuild both filter and term blooms into `footer.json`.
    pub fn rewrite_footer_blooms(segment_dir: &Path) -> Result<Footer, KoshaError> {
        Self::rewrite_filter_blooms(segment_dir)?;
        Self::rewrite_term_bloom(segment_dir)
    }

    /// Backfill `doc_store.offsets` for a segment written before lazy doc
    /// loading existed, and bump `footer.json`'s `format_version` so future
    /// opens use the `Lazy` path. Reads `doc_store.bin` once — the same
    /// single sequential scan `read_doc_store` already does — capturing each
    /// record's byte span as it goes, so this costs one full parse (same as
    /// today's every-open cost) rather than two. `doc_store.bin` itself is
    /// never rewritten.
    ///
    /// Directly analogous to `rewrite_filter_blooms`/`rewrite_term_bloom`: an
    /// in-place upgrade for segments that already exist on disk, so already-
    /// large namespaces don't have to wait for their next compaction cycle
    /// to stop paying the full-materialization cost on every query.
    ///
    /// A compressed doc store (research #388) is rebuilt from its frames
    /// and the dictionary copy at the end of `doc_store.bin` (see
    /// [`doc_store_zstd`]'s Recovery section), trailer included; its
    /// footer is left as it is.
    pub fn backfill_offset_tables(segment_dir: &Path) -> Result<Footer, KoshaError> {
        if let Ok(footer) = Self::read_footer(segment_dir) {
            if let Some(compression) = &footer.doc_store_compression {
                if compression.codec != kosha_core::DocStoreCompression::ZSTD_DICT {
                    return Err(KoshaError::CorruptSegment(format!(
                        "{}: unknown doc store codec {}; this binary is too old for the segment",
                        segment_dir.display(),
                        compression.codec
                    )));
                }
                let store = fs::read(segment_dir.join(DOC_STORE_FILE))?;
                let offsets = rebuild_offsets(&store, footer.doc_count, compression.dict_bytes)?;
                atomic_write(&segment_dir.join("doc_store.offsets"), &offsets)?;
                return Ok(footer);
            }
        }
        let data = fs::read(segment_dir.join("doc_store.bin"))?;
        let mut cursor = &data[..];
        let doc_count = if cursor.len() < 4 {
            0
        } else {
            read_u32_le(&mut cursor)
        };

        let mut offsets_buf = Vec::new();
        offsets_buf.extend_from_slice(&doc_count.to_le_bytes());
        for doc_seq in 0..doc_count {
            let record_start = (data.len() - cursor.len()) as u64;
            let rec = parse_one_doc_record(&mut cursor, doc_seq)?;
            let record_len = (data.len() - cursor.len()) as u64 - record_start;
            let id_bytes = rec.doc_id.0.as_bytes();
            offsets_buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
            offsets_buf.extend_from_slice(id_bytes);
            offsets_buf.extend_from_slice(&rec.field_length.to_le_bytes());
            offsets_buf.extend_from_slice(&record_start.to_le_bytes());
            offsets_buf.extend_from_slice(&(record_len as u32).to_le_bytes());
        }
        atomic_write(&segment_dir.join("doc_store.offsets"), &offsets_buf)?;

        let mut footer = Self::read_footer(segment_dir)?;
        footer.format_version = kosha_core::SEGMENT_FORMAT_VERSION;
        let json = serde_json::to_string_pretty(&footer)?;
        atomic_write(&segment_dir.join("footer.json"), json.as_bytes())?;
        Ok(footer)
    }

    /// [`SegmentReader::read_doc_store`] for the open path's eager
    /// fallback, with an error that says what actually went wrong when the
    /// bulk file is not local. Hydration fetches only `doc_store.offsets`
    /// for a Lazy segment, so a sidecar that is then rejected used to
    /// surface as a bare "I/O error: No such file or directory" 500 with no
    /// hint which file or why (#184). The server's hydration planner now
    /// validates the sidecar and fetches `doc_store.bin` before this can
    /// happen; this keeps the failure legible if some other caller doesn't.
    fn read_doc_store_for_fallback(segment_dir: &Path) -> Result<Vec<DocRecord>, KoshaError> {
        match Self::read_doc_store(segment_dir) {
            Err(KoshaError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(KoshaError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "{}: doc_store.offsets is missing or failed its sanity check, and the \
                         eager fallback needs doc_store.bin, which is not present locally",
                        segment_dir.display()
                    ),
                )))
            }
            other => other,
        }
    }

    /// Legacy full-parse path (`DocStoreAccess::Eager`) — reads the whole
    /// `doc_store.bin` at once. Used only when the `doc_store.offsets`
    /// sidecar is missing or fails its sanity check (see
    /// `try_read_doc_index`); the resulting `Vec<DocRecord>` costs exactly
    /// what every segment cost before lazy loading existed.
    fn read_doc_store(segment_dir: &Path) -> Result<Vec<DocRecord>, KoshaError> {
        refuse_compressed_full_parse(segment_dir)?;
        let data = fs::read(segment_dir.join("doc_store.bin"))?;
        let mut cursor = &data[..];
        let mut records = Vec::new();
        if cursor.len() < 4 {
            return Ok(records);
        }
        let doc_count = read_u32_le(&mut cursor);
        for doc_seq in 0..doc_count {
            records.push(parse_one_doc_record(&mut cursor, doc_seq)?);
        }
        Ok(records)
    }

    /// Read `inverted.idx` in whichever format it's in: v2 (magic-prefixed
    /// table of contents) opens lazily with zero parsing; anything else is
    /// parsed eagerly via the legacy v1 stream layout — the exact cost every
    /// segment paid before v2 existed.
    ///
    /// `posting_spans` (the footer's `posting_blobs_raw`): a split index may
    /// serve a term from the sparse span cache when its whole shard is not
    /// local (#245). Off for segments whose shards may be compressed, where
    /// a byte range of the stored file means nothing.
    fn read_inverted(
        segment_dir: &Path,
        account: Option<Arc<dyn PostingsMemoryAccount + Send + Sync>>,
        posting_spans: bool,
    ) -> Result<InvertedAccess, KoshaError> {
        let data = read_inverted_payload(fs::read(segment_dir.join("inverted.idx"))?)?;
        if SplitInvertedIndex::detect(&data) {
            let mut split =
                SplitInvertedIndex::from_bytes(data, segment_dir.to_path_buf(), account)?;
            split.posting_spans = posting_spans;
            return Ok(InvertedAccess::Split(split));
        }
        if LazyInvertedIndex::detect(&data) {
            return Ok(InvertedAccess::Lazy(LazyInvertedIndex::from_bytes(
                data, account,
            )?));
        }
        Ok(InvertedAccess::Eager(Self::parse_legacy_inverted(&data)?))
    }

    /// Legacy (v1) `inverted.idx` stream parse: term count, then per term
    /// `len/bytes/df/postings_count` + inline postings. Fully materializes
    /// the vocabulary — kept only for segments written before v2.
    fn parse_legacy_inverted(data: &[u8]) -> Result<HashMap<String, Vec<Posting>>, KoshaError> {
        let mut index = HashMap::new();
        if data.len() < 4 {
            return Ok(index);
        }
        let mut cursor = Checked::new(data, "legacy inverted.idx");
        let term_count = cursor.u32()?;
        for _ in 0..term_count {
            let term_len = cursor.u32()? as usize;
            let term = cursor.text(term_len)?;
            let _df = cursor.u32()?;
            let postings_len = cursor.u32()? as usize;
            let mut postings = Vec::with_capacity(cursor.capacity(postings_len, 12));
            for _ in 0..postings_len {
                let doc_id = cursor.u32()?;
                let term_frequency = cursor.u32()?;
                let positions_len = cursor.u32()? as usize;
                let mut positions = Vec::with_capacity(cursor.capacity(positions_len, 4));
                for _ in 0..positions_len {
                    positions.push(cursor.u32()?);
                }
                postings.push(Posting {
                    doc_id,
                    term_frequency,
                    positions,
                });
            }
            index.insert(term, postings);
        }
        Ok(index)
    }

    fn read_vectors(path: &Path) -> Result<VectorStore, KoshaError> {
        if !path.exists() {
            return Ok(VectorStore::default());
        }
        let data = fs::read(path)?;
        if data.len() < 8 {
            return Ok(VectorStore::default());
        }
        let mut cursor = Checked::new(&data, "legacy vector.idx");
        let dim = cursor.u32()? as usize;
        let count = cursor.u32()? as usize;
        let mut vectors = Vec::with_capacity(cursor.capacity(count, 4));
        for _ in 0..count {
            let doc_seq = cursor.u32()?;
            let mut v = Vec::with_capacity(cursor.capacity(dim, 4));
            for _ in 0..dim {
                v.push(cursor.f32()?);
            }
            vectors.push((doc_seq, v));
        }
        Ok(VectorStore {
            vectors,
            dimensions: dim,
        })
    }

    /// Raw bytes of `filters.bin` — empty when the file is not local (the
    /// writer always emits at least its 4-byte header). Parsing is
    /// deferred to first use; see [`LazyFilters`].
    fn read_filters_raw(segment_dir: &Path) -> Vec<u8> {
        fs::read(segment_dir.join("filters.bin")).unwrap_or_default()
    }

    /// Eagerly read every filter column, in whichever format `footer`
    /// says the segment uses — for the bloom-rewrite admin path, which
    /// always needs the parsed form. Every filter file must be local; a
    /// missing one is an error here (a rebuilt bloom must not silently
    /// drop a field, or prune the segment from every filtered query).
    fn read_filters_any(segment_dir: &Path, footer: &Footer) -> Result<FilterStore, KoshaError> {
        let filters = LazyFilters {
            segment_dir: segment_dir.to_path_buf(),
            doc_count: footer.doc_count,
            columnar: footer.filter_columns.is_some(),
            state: Mutex::new(FilterState::default()),
        };
        let store = filters.store_for(None)?;
        drop(filters);
        Ok(Arc::unwrap_or_clone(store))
    }

    /// Parse filter columns from raw `filters.bin` bytes. Empty/short input
    /// (absent file) yields an empty store.
    fn parse_filters(data: &[u8]) -> Result<FilterStore, KoshaError> {
        let mut store = FilterStore::default();
        if data.len() < 4 {
            return Ok(store);
        }
        let mut cursor = Checked::new(data, "filters.bin");
        let field_count = cursor.u32()?;
        for _ in 0..field_count {
            let name_len = cursor.u32()? as usize;
            let name = cursor.text(name_len)?;
            let field_type = cursor.u8()?;
            let entry_count = cursor.u32()? as usize;
            match field_type {
                0 => {
                    let mut entries = Vec::with_capacity(cursor.capacity(entry_count, 8));
                    for _ in 0..entry_count {
                        let doc_seq = cursor.u32()?;
                        let val_len = cursor.u32()? as usize;
                        entries.push((doc_seq, cursor.text(val_len)?));
                    }
                    store.string_fields.insert(name, entries);
                }
                1 => {
                    let mut entries = Vec::with_capacity(cursor.capacity(entry_count, 12));
                    for _ in 0..entry_count {
                        entries.push((cursor.u32()?, cursor.i64()?));
                    }
                    store.integer_fields.insert(name, entries);
                }
                2 => {
                    let mut entries = Vec::with_capacity(cursor.capacity(entry_count, 12));
                    for _ in 0..entry_count {
                        entries.push((cursor.u32()?, cursor.f64()?));
                    }
                    store.float_fields.insert(name, entries);
                }
                _ => {}
            }
        }
        Ok(store)
    }
}

impl filter_eval::ColumnSource for SegmentReader {
    fn column(
        &self,
        name: &str,
        kind: kosha_core::FilterColumnKind,
    ) -> Result<Option<Arc<filter_columns::FilterColumn>>, KoshaError> {
        self.filter_column(name, kind)
    }
}

// ─── Aggregation helper ─────────────────────────────────────────────────────

pub fn compute_aggregations(
    store: &FilterStore,
    _doc_count: u32,
    field: &str,
) -> AggregationResults {
    let mut results = AggregationResults {
        per_document: None,
        total_documents: None,
        matched_docs: None,
        extra: HashMap::new(),
    };

    if let Some(entries) = store.string_fields.get(field) {
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for (_, val) in entries {
            *counts.entry(val.as_str()).or_default() += 1;
        }
        let mut buckets: Vec<AggBucket> = counts
            .into_iter()
            .map(|(k, c)| AggBucket {
                key: k.to_string(),
                doc_count: c,
                ..Default::default()
            })
            .collect();
        buckets.sort_by_key(|b| std::cmp::Reverse(b.doc_count));
        let cardinality = buckets.len();
        results.per_document = Some(AggBucketResult { buckets });
        results.total_documents = Some(AggMetricResult { value: cardinality });
    }

    results
}

/// Parse one document's record from a cursor already positioned at the
/// start of its span in `doc_store.bin`, advancing the cursor past it.
/// Shared by the legacy full-parse path (`read_doc_store`) and the lazy
/// single-record path (`SegmentReader::doc_record_full`) so the two can
/// never silently diverge in how they interpret the wire format.
/// Parse one `doc_store.bin` record. Every length is checked against the
/// bytes left: a damaged record is `CorruptSegment`, never a panic or an
/// unbounded allocation (#257 found the unchecked slicing by fuzzing).
/// One stored record's bytes (exactly its `doc_store.offsets` span) to a
/// record: decoded first when the doc store is compressed (research #388).
fn parse_stored_doc_record(
    decoder: Option<&DocStoreDecoder>,
    stored: &[u8],
    doc_seq: u32,
) -> Result<DocRecord, KoshaError> {
    match decoder {
        None => {
            let mut cursor = stored;
            parse_one_doc_record(&mut cursor, doc_seq)
        }
        Some(decoder) => {
            let raw = decoder.decode(stored, doc_seq)?;
            let mut cursor = &raw[..];
            parse_one_doc_record(&mut cursor, doc_seq)
        }
    }
}

/// The sequential full parse of `doc_store.bin` only understands the raw
/// layout; a compressed one (research #388) is readable only through its
/// offsets. Errors (instead of parsing frames as garbage) when the local
/// `footer.json` says the doc store is compressed.
fn refuse_compressed_full_parse(segment_dir: &Path) -> Result<(), KoshaError> {
    match SegmentReader::read_footer(segment_dir) {
        Ok(footer) if footer.doc_store_compression.is_some() => {
            Err(KoshaError::CorruptSegment(format!(
                "{}: doc_store.bin is compressed and can only be read through a valid \
                 doc_store.offsets",
                segment_dir.display()
            )))
        }
        _ => Ok(()),
    }
}

fn parse_one_doc_record(cursor: &mut &[u8], doc_seq: u32) -> Result<DocRecord, KoshaError> {
    fn take<'a>(cursor: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
        if cursor.len() < n {
            return None;
        }
        let (head, rest) = cursor.split_at(n);
        *cursor = rest;
        Some(head)
    }
    fn u32_of(cursor: &mut &[u8]) -> Option<u32> {
        take(cursor, 4).map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn u64_of(cursor: &mut &[u8]) -> Option<u64> {
        take(cursor, 8).map(|b| u64::from_le_bytes(b.try_into().unwrap_or_default()))
    }
    fn text(cursor: &mut &[u8], len: Option<u64>) -> Option<String> {
        let len = usize::try_from(len?).ok()?;
        take(cursor, len).map(|b| String::from_utf8_lossy(b).into_owned())
    }
    let corrupt = || {
        KoshaError::CorruptSegment(format!(
            "doc_store.bin: record {doc_seq} is truncated or malformed"
        ))
    };
    let id_len = u32_of(cursor).map(u64::from);
    let doc_id = DocumentId(text(cursor, id_len).ok_or_else(corrupt)?);
    let field_length = u32_of(cursor).ok_or_else(corrupt)?;
    let field_count = u32_of(cursor).ok_or_else(corrupt)?;
    // A field takes at least 13 bytes (name length, type, value length).
    let mut fields = Vec::with_capacity((field_count as usize).min(cursor.len() / 13));
    for _ in 0..field_count {
        let name_len = u32_of(cursor).map(u64::from);
        let name = text(cursor, name_len).ok_or_else(corrupt)?;
        let field_type = match take(cursor, 1).ok_or_else(corrupt)?[0] {
            0 => FieldType::Text,
            1 => FieldType::Keyword,
            2 => FieldType::Integer,
            3 => FieldType::Float,
            4 => FieldType::Date,
            5 => FieldType::Boolean,
            6 => FieldType::Vector,
            7 => FieldType::GeoPoint,
            _ => FieldType::Text,
        };
        let val_len = u64_of(cursor);
        let value = text(cursor, val_len).ok_or_else(corrupt)?;
        fields.push(Field {
            name,
            field_type,
            value,
        });
    }
    Ok(DocRecord {
        doc_id,
        doc_seq,
        field_length,
        fields,
    })
}

/// The footer a segment is opened with. With no caller-supplied footer,
/// that is the segment's own `footer.json`.
///
/// A caller-supplied footer (search passes the manifest's cached copy so a
/// cold open needn't fetch `footer.json`) is trusted unless the
/// `doc_store.offsets` header disagrees with its `doc_count`. Then the
/// segment's own `footer.json`, when it is on local disk, decides: if it
/// disagrees with the supplied copy, the supplied copy is the stale one and
/// is replaced. Issue #184 found manifest footers claiming 6, 25 and 36 docs
/// for segments whose `footer.json` and `doc_store.offsets` both said 1 or 2;
/// trusting the manifest made the sanity check reject a correct sidecar and
/// fall back to a `doc_store.bin` that was never hydrated.
fn resolve_open_footer(segment_dir: &Path, supplied: Option<Footer>) -> Result<Footer, KoshaError> {
    let Some(supplied) = supplied else {
        return SegmentReader::read_footer(segment_dir);
    };
    match doc_store_offsets_header_count(segment_dir) {
        Some(declared) if declared != supplied.doc_count => {}
        _ => return Ok(supplied),
    }
    match SegmentReader::read_footer(segment_dir) {
        Ok(own) if own.segment_id == supplied.segment_id && own.doc_count != supplied.doc_count => {
            eprintln!(
                "WARN: {}: the manifest's cached footer says {} doc(s) but the segment's own \
                 footer.json says {}; using footer.json",
                segment_dir.display(),
                supplied.doc_count,
                own.doc_count
            );
            Ok(own)
        }
        _ => Ok(supplied),
    }
}

/// The doc count `doc_store.offsets` declares in its 4-byte header, or
/// `None` when the sidecar is absent or shorter than a header. Reads only
/// the header, never the entries.
fn doc_store_offsets_header_count(segment_dir: &Path) -> Option<u32> {
    let mut header = [0u8; 4];
    fs::File::open(segment_dir.join("doc_store.offsets"))
        .and_then(|mut f| f.read_exact(&mut header))
        .ok()?;
    Some(u32::from_le_bytes(header))
}

/// Whether [`SegmentReader::open_with_footer_options`], given the same
/// `footer`, would open this segment's doc store lazily from
/// `doc_store.offsets`. `false` means the open would take the eager path
/// and read `doc_store.bin`, so a caller that hydrates files before opening
/// (the server's search path) must make sure that file is local.
///
/// Runs the reader's own sanity check (same footer resolution, same full
/// parse of the sidecar), so the two cannot disagree. Costs one read of
/// the sidecar, which is small next to the S3 fetch that just landed it.
pub fn doc_store_opens_lazily(segment_dir: &Path, footer: Option<&Footer>) -> bool {
    let Ok(footer) = resolve_open_footer(segment_dir, footer.cloned()) else {
        return false;
    };
    let parts = try_read_doc_index_parts(segment_dir, footer.doc_count);
    match (&footer.doc_store_compression, parts) {
        (_, None) => false,
        (None, Some(_)) => true,
        // A compressed doc store also needs its dictionary trailer; without
        // it the reader rebuilds the index from doc_store.bin (#388), so
        // that file must be local.
        (Some(compression), Some((_, trailer))) => parse_dict_trailer(&trailer)
            .is_some_and(|dict| dict.len() == compression.dict_bytes as usize),
    }
}

/// Cheap, header-only version of [`doc_store_opens_lazily`] for checks that
/// run on every search: the sidecar exists and its declared doc count
/// matches the footer the reader would use. It does not parse the entries,
/// so a sidecar truncated mid-record still passes; the hydration planner's
/// full check catches that when the file is first fetched.
pub fn doc_store_offsets_plausible(segment_dir: &Path, footer: Option<&Footer>) -> bool {
    let Some(declared) = doc_store_offsets_header_count(segment_dir) else {
        return false;
    };
    match footer {
        Some(f) if f.doc_count == declared => true,
        // Mismatch with the supplied footer (or none supplied): resolve the
        // way the reader would. Reads footer.json only on this slow path.
        _ => resolve_open_footer(segment_dir, footer.cloned())
            .is_ok_and(|resolved| resolved.doc_count == declared),
    }
}

/// Try to read `doc_store.offsets`. Returns `None` if it's missing, or if
/// present but fails a sanity check (bad/truncated header, declared
/// `doc_count` not matching the segment's actual footer) — either way,
/// callers fall back to the legacy full-parse path for `doc_store.bin`
/// rather than trusting a stale or corrupt sidecar. Never returns `Err`:
/// a broken sidecar degrades to "slower, still correct," not a failed
/// segment open.
fn try_read_doc_index(segment_dir: &Path, expected_doc_count: u32) -> Option<DocIndex> {
    try_read_doc_index_parts(segment_dir, expected_doc_count).map(|(index, _)| index)
}

/// [`try_read_doc_index`] plus the bytes after the entries: empty for a
/// raw doc store, the dictionary trailer for a compressed one (#388).
fn try_read_doc_index_parts(
    segment_dir: &Path,
    expected_doc_count: u32,
) -> Option<(DocIndex, Vec<u8>)> {
    let path = segment_dir.join("doc_store.offsets");
    let data = match fs::read(&path) {
        Ok(data) => data,
        Err(_) => return None, // missing sidecar: legacy segment, not an error
    };
    parse_doc_index_bytes(&data, &path, expected_doc_count)
}

/// The parse behind [`try_read_doc_index_parts`], over `doc_store.offsets`
/// bytes already in memory (`path` names them in warnings).
fn parse_doc_index_bytes(
    data: &[u8],
    path: &Path,
    expected_doc_count: u32,
) -> Option<(DocIndex, Vec<u8>)> {
    let mut cursor = data;
    if cursor.len() < 4 {
        eprintln!(
            "WARN: {} is truncated (missing header); falling back to full parse",
            path.display()
        );
        return None;
    }
    let doc_count = read_u32_le(&mut cursor);
    if doc_count != expected_doc_count {
        eprintln!(
            "WARN: {} declares {doc_count} doc(s) but footer.json says {expected_doc_count}; \
             falling back to full parse",
            path.display()
        );
        return None;
    }
    let mut entries = Vec::with_capacity(doc_count as usize);
    let mut ids_pool = String::new();
    for _ in 0..doc_count {
        // Fixed minimum per entry beyond the variable-length id: field_length
        // (4) + offset (8) + length (4) = 16, plus the 4-byte id_len prefix.
        if cursor.len() < 4 {
            eprintln!(
                "WARN: {} truncated mid-record; falling back to full parse",
                path.display()
            );
            return None;
        }
        let id_len = read_u32_le(&mut cursor) as usize;
        if cursor.len() < id_len + 16 {
            eprintln!(
                "WARN: {} truncated mid-record; falling back to full parse",
                path.display()
            );
            return None;
        }
        let id_bytes = read_bytes(&mut cursor, id_len);
        let id_off = ids_pool.len() as u32;
        ids_pool.push_str(&String::from_utf8_lossy(id_bytes));
        let id_len = ids_pool.len() as u32 - id_off;
        let field_length = read_u32_le(&mut cursor);
        let offset = read_u64_le(&mut cursor);
        let length = read_u32_le(&mut cursor);
        entries.push(DocIndexEntry {
            id_off,
            id_len,
            field_length,
            offset,
            length,
        });
    }
    ids_pool.shrink_to_fit();
    Some((DocIndex { ids_pool, entries }, cursor.to_vec()))
}

// ─── Atomic file rewrite ────────────────────────────────────────────────────

// Footer and offsets rewrites on a published segment go through
// `kosha_core::atomic_write` (temp file + rename): a search reading the
// segment concurrently sees the old or the new file, never a short one.
use kosha_core::atomic_write;

// ─── Binary read helpers ────────────────────────────────────────────────────

fn read_u32_le(cursor: &mut &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&cursor[..4]);
    *cursor = &cursor[4..];
    u32::from_le_bytes(buf)
}
fn read_u64_le(cursor: &mut &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&cursor[..8]);
    *cursor = &cursor[8..];
    u64::from_le_bytes(buf)
}
fn read_f32_le(cursor: &mut &[u8]) -> f32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&cursor[..4]);
    *cursor = &cursor[4..];
    f32::from_le_bytes(buf)
}
/// Bounds-checked little-endian reads for the legacy (pre-v2) stream
/// formats, which used to slice unchecked and panic on a damaged file
/// (#257). Every read past the end is `CorruptSegment`.
struct Checked<'a> {
    data: &'a [u8],
    what: &'static str,
}

impl<'a> Checked<'a> {
    fn new(data: &'a [u8], what: &'static str) -> Self {
        Self { data, what }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], KoshaError> {
        if self.data.len() < n {
            return Err(KoshaError::CorruptSegment(format!(
                "{}: truncated ({n} bytes wanted, {} left)",
                self.what,
                self.data.len()
            )));
        }
        let (head, rest) = self.data.split_at(n);
        self.data = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], KoshaError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, KoshaError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, KoshaError> {
        self.array().map(u32::from_le_bytes)
    }
    fn i64(&mut self) -> Result<i64, KoshaError> {
        self.array().map(i64::from_le_bytes)
    }
    fn f32(&mut self) -> Result<f32, KoshaError> {
        self.array().map(f32::from_le_bytes)
    }
    fn f64(&mut self) -> Result<f64, KoshaError> {
        self.array().map(f64::from_le_bytes)
    }
    fn text(&mut self, len: usize) -> Result<String, KoshaError> {
        Ok(String::from_utf8_lossy(self.take(len)?).into_owned())
    }

    /// A capacity for `count` items of at least `min_bytes` each that the
    /// remaining bytes can actually hold, so a corrupt count cannot
    /// trigger a huge allocation.
    fn capacity(&self, count: usize, min_bytes: usize) -> usize {
        count.min(self.data.len() / min_bytes.max(1))
    }
}

fn read_bytes<'a>(cursor: &mut &'a [u8], len: usize) -> &'a [u8] {
    let result = &cursor[..len];
    *cursor = &cursor[len..];
    result
}

// ─── Tokenizer ──────────────────────────────────────────────────────────────

/// The legacy `standard@1` chain — what every segment without a footer
/// `analysis` was written with. Prefer [`Analyzer`] (or
/// [`analyze_query_for_segment`]) for anything that must match a specific
/// segment's terms.
pub fn tokenize(text: &str) -> Vec<String> {
    text.split_whitespace()
        .flat_map(|word| {
            let w = word.trim_matches(|c: char| c.is_ascii_punctuation());
            if w.is_empty() {
                None
            } else {
                Some(w.to_lowercase())
            }
        })
        .collect()
}

pub fn tokenize_with_positions(text: &str) -> Vec<(String, u32)> {
    text.split_whitespace()
        .scan(0u32, |pos, word| {
            let w = word.trim_matches(|c: char| c.is_ascii_punctuation());
            if w.is_empty() {
                Some(None)
            } else {
                let p = *pos;
                *pos += 1;
                Some(Some((w.to_lowercase(), p)))
            }
        })
        .flatten()
        .collect()
}

fn chrono_like_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let (year, month, day) = days_to_date((secs / 86400) as i64);
    let h = (secs % 86400) / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, h, m, s
    )
}

fn days_to_date(mut days: i64) -> (i64, i64, i64) {
    days += 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn probe_score_default_alpha_is_the_pre_384_lower_bound() {
        // Bit-identical to `(d - r).max(0.0)` at the default alpha, so the
        // ranking (and every result) is unchanged unless the flag is set.
        for &(d, r) in &[(0.7f32, 0.3f32), (0.2, 0.5), (1.3, 0.0), (0.41, 0.41)] {
            assert_eq!(
                centroid_probe_score(d, r, DEFAULT_KNN_PROBE_RADIUS_ALPHA).to_bits(),
                (d - r).max(0.0).to_bits()
            );
        }
        assert_eq!(parse_probe_radius_alpha(None), 1.0);
        assert_eq!(parse_probe_radius_alpha(Some("")), 1.0);
    }

    #[test]
    fn probe_score_alpha_zero_is_raw_distance_and_reorders_postings() {
        assert_eq!(centroid_probe_score(0.7, 0.3, 0.0), 0.7);
        assert!((centroid_probe_score(0.7, 0.4, 0.5) - 0.5).abs() < 1e-6);
        // A diffuse posting (far centroid, big radius) outranks a tight
        // near one under the full bound, and loses to it under raw
        // distance: the reordering the #384 experiment measures.
        let (near_tight, far_diffuse) = ((0.30f32, 0.05f32), (0.60f32, 0.45f32));
        let score = |(d, r): (f32, f32), a| centroid_probe_score(d, r, a);
        assert!(score(far_diffuse, 1.0) < score(near_tight, 1.0));
        assert!(score(far_diffuse, 0.0) > score(near_tight, 0.0));
    }

    #[test]
    fn probe_radius_alpha_parse_rejects_bad_values() {
        assert_eq!(parse_probe_radius_alpha(Some("0")), 0.0);
        assert_eq!(parse_probe_radius_alpha(Some(" 0.5 ")), 0.5);
        for bad in ["-0.1", "NaN", "inf", "abc"] {
            assert_eq!(parse_probe_radius_alpha(Some(bad)), 1.0, "{bad}");
        }
    }

    // `cap`'s `total_allocated()` (bytes ever requested, including
    // already-freed — requires its `stats` feature) rather than
    // `allocated()` (net/current-live, what `kosha-query`'s benches use for
    // a different purpose): a large transient buffer that's read and
    // dropped *within* `load_vector_index` before it returns would net to
    // ~0 on a live-bytes counter, making that idiom blind to exactly the
    // bug this test exists to catch.
    #[global_allocator]
    static ALLOC: cap::Cap<std::alloc::System> = cap::Cap::new(std::alloc::System, usize::MAX);

    #[test]
    fn vector_build_refine_parses_on_off_and_defaults_on() {
        for raw in [
            None,
            Some(""),
            Some("on"),
            Some(" ON "),
            Some("true"),
            Some("1"),
        ] {
            assert!(parse_vector_build_refine(raw), "{raw:?}");
        }
        for raw in [Some("off"), Some("Off"), Some("false"), Some("0")] {
            assert!(!parse_vector_build_refine(raw), "{raw:?}");
        }
        assert!(parse_vector_build_refine(Some("bogus")));
    }

    #[test]
    fn dot_lanes_default_to_eight_and_agree_up_to_rounding() {
        for raw in [None, Some(""), Some("8"), Some(" 8 "), Some("12")] {
            assert_eq!(DotLanes::parse(raw), DotLanes::Eight, "{raw:?}");
        }
        assert_eq!(DotLanes::parse(Some("16")), DotLanes::Sixteen);
        assert_eq!(DotLanes::parse(Some("32")), DotLanes::ThirtyTwo);
        assert_eq!(DotLanes::parse(Some("64")), DotLanes::SixtyFour);
        let a: Vec<f32> = (0..1027)
            .map(|i| ((i * 37 % 101) as f32 - 50.0) / 50.0)
            .collect();
        let b: Vec<f32> = (0..1027)
            .map(|i| ((i * 53 % 97) as f32 - 48.0) / 48.0)
            .collect();
        // The default is the existing kernels, bit for bit.
        assert_eq!(
            DotLanes::Eight.dot(&a, &b),
            kosha_vector_spfresh::dot(&a, &b)
        );
        assert_eq!(
            DotLanes::Eight.cosine_similarity(&a, &b),
            kosha_vector_spfresh::cosine_similarity(&a, &b)
        );
        let zero = vec![0.0f32; 1027];
        for lanes in [
            DotLanes::Eight,
            DotLanes::Sixteen,
            DotLanes::ThirtyTwo,
            DotLanes::SixtyFour,
        ] {
            let d = lanes.dot(&a, &b);
            assert!(
                (d - kosha_vector_spfresh::dot(&a, &b)).abs() < 1e-3,
                "{lanes:?}"
            );
            let c = lanes.cosine_similarity(&a, &b);
            let expected = kosha_vector_spfresh::cosine_similarity(&a, &b);
            assert!((c - expected).abs() < 1e-5, "{lanes:?}: {c} vs {expected}");
            assert_eq!(lanes.cosine_similarity(&a, &zero), 0.0, "{lanes:?}");
        }
    }

    #[test]
    fn vector_kernels_summary_names_the_effective_settings() {
        use kosha_vector_spfresh::RaBitQEstimator;
        assert_eq!(
            format_vector_kernels_summary(DotLanes::Eight, RaBitQEstimator::Lut, &[]),
            "vector kernels (#382, experimental): dot_lanes=8 rabitq_estimator=lut \
             x86_features=baseline"
        );
        assert_eq!(
            format_vector_kernels_summary(
                DotLanes::ThirtyTwo,
                RaBitQEstimator::Popcount4,
                &["popcnt", "avx2", "fma", "bmi2"]
            ),
            "vector kernels (#382, experimental): dot_lanes=32 rabitq_estimator=popcount \
             x86_features=popcnt,avx2,fma,bmi2"
        );
    }

    #[test]
    fn rabitq_estimator_defaults_to_lut() {
        use kosha_vector_spfresh::RaBitQEstimator;
        for raw in [None, Some(""), Some(" "), Some("lut"), Some("bogus")] {
            assert_eq!(parse_rabitq_estimator(raw), RaBitQEstimator::Lut, "{raw:?}");
        }
        for raw in [Some("popcount"), Some(" Popcount4 ")] {
            assert_eq!(
                parse_rabitq_estimator(raw),
                RaBitQEstimator::Popcount4,
                "{raw:?}"
            );
        }
    }

    /// Serialize an inverted index in the legacy v1 stream layout — the
    /// exact bytes `write_inverted_index` produced before v2 — so the
    /// fallback path can be tested against segments that predate the
    /// format change.
    fn serialize_legacy_inverted(index: &HashMap<String, Vec<Posting>>) -> Vec<u8> {
        let mut terms: Vec<&String> = index.keys().collect();
        terms.sort();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(terms.len() as u32).to_le_bytes());
        for term_str in terms {
            let postings = &index[term_str];
            let term_bytes = term_str.as_bytes();
            buf.extend_from_slice(&(term_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(term_bytes);
            // v1 wrote the postings count twice (df + count).
            buf.extend_from_slice(&(postings.len() as u32).to_le_bytes());
            buf.extend_from_slice(&(postings.len() as u32).to_le_bytes());
            for posting in postings {
                buf.extend_from_slice(&posting.doc_id.to_le_bytes());
                buf.extend_from_slice(&posting.term_frequency.to_le_bytes());
                buf.extend_from_slice(&(posting.positions.len() as u32).to_le_bytes());
                for &pos in &posting.positions {
                    buf.extend_from_slice(&pos.to_le_bytes());
                }
            }
        }
        buf
    }

    fn serialize_inline_v2_inverted(index: &HashMap<String, Vec<Posting>>) -> Vec<u8> {
        let mut terms: Vec<&String> = index.keys().collect();
        terms.sort();

        let mut pool: Vec<u8> = Vec::new();
        let mut postings_buf: Vec<u8> = Vec::new();
        let mut entries: Vec<(u64, u32, u64, u32)> = Vec::with_capacity(terms.len());
        for term_str in &terms {
            let postings = &index[*term_str];
            let term_off = pool.len() as u64;
            pool.extend_from_slice(term_str.as_bytes());
            let p_off = postings_buf.len() as u64;
            postings_buf.extend_from_slice(&(postings.len() as u32).to_le_bytes());
            for posting in postings {
                postings_buf.extend_from_slice(&posting.doc_id.to_le_bytes());
                postings_buf.extend_from_slice(&posting.term_frequency.to_le_bytes());
                postings_buf.extend_from_slice(&(posting.positions.len() as u32).to_le_bytes());
                for &pos in &posting.positions {
                    postings_buf.extend_from_slice(&pos.to_le_bytes());
                }
            }
            let p_len = (postings_buf.len() as u64 - p_off) as u32;
            entries.push((term_off, term_str.len() as u32, p_off, p_len));
        }

        let table_len = entries.len() as u64 * INVERTED_TABLE_ENTRY_LEN as u64;
        let pool_base = INVERTED_HEADER_LEN as u64 + table_len;
        let postings_base = pool_base + pool.len() as u64;
        let mut buf = Vec::with_capacity(postings_base as usize + postings_buf.len());
        buf.extend_from_slice(&INVERTED_MAGIC.to_le_bytes());
        buf.extend_from_slice(&INLINE_INVERTED_VERSION.to_le_bytes());
        buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        for (term_off, term_len, p_off, p_len) in entries {
            buf.extend_from_slice(&(pool_base + term_off).to_le_bytes());
            buf.extend_from_slice(&term_len.to_le_bytes());
            buf.extend_from_slice(&(postings_base + p_off).to_le_bytes());
            buf.extend_from_slice(&p_len.to_le_bytes());
        }
        buf.extend_from_slice(&pool);
        buf.extend_from_slice(&postings_buf);
        buf
    }

    fn write_fixed_split_inverted(dir: &Path, index: &HashMap<String, Vec<Posting>>) {
        let mut terms: Vec<&String> = index.keys().collect();
        terms.sort();

        let mut pool: Vec<u8> = Vec::new();
        let mut posting_blobs: Vec<Vec<u8>> =
            (0..POSTING_BLOB_SHARDS).map(|_| Vec::new()).collect();
        let mut entries: Vec<(u64, u32, u32, u64, u32)> = Vec::with_capacity(terms.len());
        for term_str in &terms {
            let postings = &index[*term_str];
            let term_off = pool.len() as u64;
            pool.extend_from_slice(term_str.as_bytes());
            let blob_id = posting_blob_id_for_term(term_str) as u32;
            let blob = &mut posting_blobs[blob_id as usize];
            let p_off = blob.len() as u64;
            blob.extend_from_slice(&(postings.len() as u32).to_le_bytes());
            for posting in postings {
                blob.extend_from_slice(&posting.doc_id.to_le_bytes());
                blob.extend_from_slice(&posting.term_frequency.to_le_bytes());
                blob.extend_from_slice(&(posting.positions.len() as u32).to_le_bytes());
                for &pos in &posting.positions {
                    blob.extend_from_slice(&pos.to_le_bytes());
                }
            }
            let p_len = (blob.len() as u64 - p_off) as u32;
            entries.push((term_off, term_str.len() as u32, blob_id, p_off, p_len));
        }

        let table_len = entries.len() as u64 * SPLIT_INVERTED_TABLE_ENTRY_LEN as u64;
        let pool_base = INVERTED_HEADER_LEN as u64 + table_len;
        let mut buf = Vec::with_capacity(pool_base as usize + pool.len());
        buf.extend_from_slice(&INVERTED_MAGIC.to_le_bytes());
        buf.extend_from_slice(&FIXED_SPLIT_INVERTED_VERSION.to_le_bytes());
        buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(POSTING_BLOB_SHARDS as u32).to_le_bytes());
        for (term_off, term_len, blob_id, p_off, p_len) in entries {
            buf.extend_from_slice(&(pool_base + term_off).to_le_bytes());
            buf.extend_from_slice(&term_len.to_le_bytes());
            buf.extend_from_slice(&blob_id.to_le_bytes());
            buf.extend_from_slice(&p_off.to_le_bytes());
            buf.extend_from_slice(&p_len.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
        buf.extend_from_slice(&pool);
        fs::write(dir.join("inverted.idx"), maybe_compress_inverted(&buf)).unwrap();
        for (blob_id, blob) in posting_blobs.into_iter().enumerate() {
            if !blob.is_empty() {
                fs::write(
                    dir.join(posting_blob_file_for_id(blob_id)),
                    maybe_compress_inverted(&blob),
                )
                .unwrap();
            }
        }
    }

    /// Build a two-doc segment and return (dir, the writer's in-memory
    /// inverted index captured before finalize) for fidelity comparisons.
    fn write_inverted_fixture(dir: &Path) -> HashMap<String, Vec<Posting>> {
        let _ = fs::remove_dir_all(dir);
        let mut w = SegmentWriter::new(SegmentId("test".into()), dir.to_path_buf());
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("t", "quick brown fox jumps")],
        );
        w.add_document(
            DocumentId("d2".into()),
            vec![Field::text("t", "quick fox is quick indeed")],
        );
        let expected = w.inverted_index.clone();
        w.finalize(Bm25Params::default()).unwrap();
        expected
    }

    #[test]
    fn split_inverted_roundtrips_identically_to_writer_state() {
        // The lazy on-demand decode must return byte-for-byte the same
        // postings (doc ids, tfs, positions, order) the writer held in
        // memory — for every term, plus sorted all_terms and negative
        // lookups.
        let dir = std::env::temp_dir().join("kosha-test-inverted-v2-roundtrip");
        let expected = write_inverted_fixture(&dir);

        // Sanity: the file on disk really is the split layout (v5 today).
        let raw = fs::read(dir.join("inverted.idx")).unwrap();
        assert!(
            SplitInvertedIndex::detect(&raw),
            "writer should emit the split magic-prefixed layout"
        );
        assert_eq!(
            u32::from_le_bytes(raw[4..8].try_into().unwrap()),
            SKIP_SPLIT_INVERTED_VERSION,
            "writer should emit the v5 skip-split version"
        );

        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(
            matches!(r.inverted, InvertedAccess::Split(_)),
            "split v4 file must open on the split lazy path"
        );
        for (term, postings) in &expected {
            let got = r
                .postings(term)
                .unwrap_or_else(|| panic!("missing term {term}"));
            assert_eq!(&*got, postings.as_slice(), "postings mismatch for {term}");
            assert!(r.contains_term(term));
        }
        // `all_terms` lists the combined index only (#249).
        let mut expected_terms: Vec<&str> = expected
            .keys()
            .map(|s| s.as_str())
            .filter(|t| !kosha_core::fields::is_field_key(t))
            .collect();
        expected_terms.sort();
        assert_eq!(r.all_terms(), expected_terms);
        assert!(r.postings("absent-term").is_none());
        assert!(!r.contains_term("absent-term"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn fixed_split_v3_inverted_still_opens() {
        let dir = std::env::temp_dir().join("kosha-test-inverted-v3-fallback");
        let expected = write_inverted_fixture(&dir);
        write_fixed_split_inverted(&dir, &expected);

        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(
            matches!(r.inverted, InvertedAccess::Split(_)),
            "v3 split file must still open on the split path"
        );
        for (term, postings) in &expected {
            let got = r
                .postings(term)
                .unwrap_or_else(|| panic!("missing term {term}"));
            assert_eq!(&*got, postings.as_slice(), "postings mismatch for {term}");
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn varint_delta_postings_roundtrip() {
        let postings = vec![
            Posting {
                doc_id: 7,
                term_frequency: 2,
                positions: vec![3, 9],
            },
            Posting {
                doc_id: 31,
                term_frequency: 3,
                positions: vec![1, 4, 12],
            },
        ];
        let mut encoded = Vec::new();
        write_postings_varint_delta(&mut encoded, &postings);
        let (decoded, _) = decode_postings_varint_delta_bytes(&encoded).unwrap();
        assert_eq!(decoded, postings);
    }

    #[test]
    fn inline_v2_inverted_still_opens_via_lazy_fallback() {
        let dir = std::env::temp_dir().join("kosha-test-inverted-v2-fallback");
        let expected = write_inverted_fixture(&dir);

        fs::write(
            dir.join("inverted.idx"),
            serialize_inline_v2_inverted(&expected),
        )
        .unwrap();

        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(
            matches!(r.inverted, InvertedAccess::Lazy(_)),
            "inline v2 file must still open on the lazy fallback path"
        );
        for (term, postings) in &expected {
            let got = r
                .postings(term)
                .unwrap_or_else(|| panic!("missing term {term}"));
            assert_eq!(&*got, postings.as_slice(), "postings mismatch for {term}");
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn compressed_toc_and_blobs_roundtrip_through_split_reader() {
        // The zstd wrapper applies independently to the TOC and to each
        // posting blob shard; the reader must unwrap both transparently
        // and serve identical postings.
        let dir = std::env::temp_dir().join("kosha-test-inverted-compressed");
        let expected = write_inverted_fixture(&dir);

        // Force-wrap every artifact, bypassing maybe_compress_inverted's
        // small-file threshold so tiny fixtures still exercise the path.
        let mut wrapped = 0;
        for entry in fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if name == "inverted.idx" || name.starts_with("postings-") {
                let raw = fs::read(&path).unwrap();
                let compressed = zstd::bulk::compress(&raw, INVERTED_COMPRESSION_LEVEL).unwrap();
                fs::write(&path, wrap_compressed_inverted(raw.len(), &compressed)).unwrap();
                wrapped += 1;
            }
        }
        assert!(
            wrapped >= 2,
            "fixture must have a TOC and at least one blob"
        );

        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(
            matches!(r.inverted, InvertedAccess::Split(_)),
            "compressed TOC must unwrap onto the split path"
        );
        for (term, postings) in &expected {
            let got = r
                .postings(term)
                .unwrap_or_else(|| panic!("missing term {term}"));
            assert_eq!(&*got, postings.as_slice(), "postings mismatch for {term}");
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn posting_blob_gate_detects_compressed_toc() {
        // Regression: the server's hydration path asks
        // `inverted_uses_posting_blobs` before fetching `postings-*.bin`
        // from object storage, and filters its missing-file guard on the
        // same answer. A KIZC-wrapped TOC must still read as a split
        // index — returning false made hydration skip every posting blob
        // and cold searches silently scored zero hits.
        let dir = std::env::temp_dir().join("kosha-test-posting-blob-gate");
        write_inverted_fixture(&dir);
        assert!(
            inverted_uses_posting_blobs(&dir),
            "raw split TOC must be detected"
        );

        let path = dir.join("inverted.idx");
        let raw = fs::read(&path).unwrap();
        let compressed = zstd::bulk::compress(&raw, INVERTED_COMPRESSION_LEVEL).unwrap();
        fs::write(&path, wrap_compressed_inverted(raw.len(), &compressed)).unwrap();
        assert!(
            inverted_uses_posting_blobs(&dir),
            "KIZC-wrapped split TOC must be detected"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn postings_for_terms_preserves_requested_order() {
        // The output order is load-bearing: scoring paths sum per-term
        // float scores in this order and promise deterministic pages.
        // Request terms in a chosen order twice — once fully cache-cold
        // (batched blob decode used to emit in HashMap iteration order)
        // and once fully cache-warm (hits used to jump the queue) — and
        // both must come back exactly as requested.
        let dir = std::env::temp_dir().join("kosha-test-postings-order");
        let expected = write_inverted_fixture(&dir);
        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(
            r.has_ordered_postings(),
            "current writer output must advertise ordered postings"
        );

        let mut terms: Vec<String> = expected.keys().cloned().collect();
        terms.sort();
        terms.reverse(); // deliberately not the TOC's sorted order
        for pass in ["cold", "warm"] {
            let got: Vec<&str> = r
                .postings_for_terms(&terms)
                .iter()
                .map(|(t, _)| *t)
                .collect();
            let want: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            assert_eq!(got, want, "{pass} pass must preserve requested order");
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_block_rejects_corrupted_doc_delta() {
        // One flipped bit in a scoring doc-delta varint used to "decode"
        // fine — every doc_id after it silently shifted, and BM25 scored
        // the wrong documents with no error anywhere. The block's decoded
        // end must land exactly on the skip entry's last_doc_id.
        let postings: Vec<Posting> = (0..300)
            .map(|i| Posting {
                doc_id: i * 3, // gaps so deltas are multi-valued
                term_frequency: (i % 7) + 1,
                positions: vec![i],
            })
            .collect();
        let doc_records: Vec<DocRecord> = (0..=897)
            .map(|i| DocRecord {
                doc_id: DocumentId(format!("d{i}")),
                doc_seq: i,
                field_length: 10,
                fields: Vec::new(),
            })
            .collect();
        let mut span = Vec::new();
        write_postings_skip_split(
            &mut span,
            &postings,
            &doc_records
                .iter()
                .map(|r| r.field_length)
                .collect::<Vec<_>>(),
        );

        let clean = BlockedTermPostings::parse(Arc::new(span.clone())).unwrap();
        let mut buf = Vec::new();
        assert!(clean.read_block(0, &mut buf), "clean block must decode");
        assert!(clean.decode_full().is_some(), "clean span must full-decode");

        // Corrupt one byte in the middle of block 0's scoring bytes. The
        // skip table still parses; only the decoded doc chain breaks.
        let scoring_mid = span.len() / 2;
        let mut corrupted = span.clone();
        corrupted[scoring_mid] ^= 0x01;
        if let Some(parsed) = BlockedTermPostings::parse(Arc::new(corrupted)) {
            let mut any_block_failed = false;
            let mut buf = Vec::new();
            for b in 0..parsed.block_count() {
                if !parsed.read_block(b, &mut buf) {
                    any_block_failed = true;
                }
            }
            assert!(
                any_block_failed || parsed.decode_full().is_none(),
                "a corrupted span must fail some decode closed, never \
                 return shifted doc_ids as success"
            );
        }
    }

    #[test]
    fn decode_full_rejects_oversized_position_count() {
        // A corrupt positions-count varint used to drive
        // Vec::with_capacity(up to u32::MAX) — a multi-gigabyte allocation
        // request that aborts the process. Every position needs at least
        // one byte, so a valid count can never exceed the remaining
        // buffer; the decode must fail closed instead.
        let postings: Vec<Posting> = (0..4)
            .map(|i| Posting {
                doc_id: i,
                term_frequency: 1,
                positions: vec![1, 2],
            })
            .collect();
        let doc_records: Vec<DocRecord> = (0..4)
            .map(|i| DocRecord {
                doc_id: DocumentId(format!("d{i}")),
                doc_seq: i,
                field_length: 10,
                fields: Vec::new(),
            })
            .collect();
        let mut span = Vec::new();
        write_postings_skip_split(
            &mut span,
            &postings,
            &doc_records
                .iter()
                .map(|r| r.field_length)
                .collect::<Vec<_>>(),
        );

        let clean = BlockedTermPostings::parse(Arc::new(span.clone())).unwrap();
        let (decoded, _) = clean.decode_full().unwrap();
        assert_eq!(decoded.len(), 4);

        // The positions section trails the span; overwrite its first
        // varint (the first posting's n_positions) with a 5-byte varint
        // encoding ~u32::MAX.
        let scoring_bytes: usize = (0..clean.block_count())
            .map(|b| {
                let s = clean.summary(b);
                (s.scoring_end - s.scoring_start) as usize
            })
            .sum();
        let positions_start = clean.scoring_base as usize + scoring_bytes;
        let mut corrupted = span.clone();
        corrupted.truncate(positions_start);
        corrupted.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
        let parsed = BlockedTermPostings::parse(Arc::new(corrupted))
            .expect("skip table untouched — parse must still succeed");
        assert!(
            parsed.decode_full().is_none(),
            "oversized position count must fail closed, not allocate"
        );
    }

    #[test]
    fn scoring_postings_for_terms_preserves_requested_order() {
        // Same load-bearing property as `postings_for_terms_preserves_
        // requested_order`, but for the v5 block-lazy scoring path
        // (`SegmentReader::scoring_postings_for_terms` /
        // `SplitInvertedIndex::scoring_postings_for_terms`): the WAND
        // leapfrog join sums per-term float scores in whatever order this
        // returns, so cache-hits-first / HashMap-iteration-order output
        // would let two runs of the same query swap docs tied at a page
        // boundary. Request terms out of TOC order, cold then warm.
        let dir = std::env::temp_dir().join("kosha-test-scoring-postings-order");
        let expected = write_inverted_fixture(&dir);
        let r = SegmentReader::open(dir.clone()).unwrap();

        let mut terms: Vec<String> = expected.keys().cloned().collect();
        terms.sort();
        terms.reverse(); // deliberately not the TOC's sorted order
        for pass in ["cold", "warm"] {
            let got: Vec<&str> = r
                .scoring_postings_for_terms(&terms)
                .iter()
                .map(|(t, _)| *t)
                .collect();
            let want: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
            assert_eq!(
                got, want,
                "{pass} pass must preserve requested order on the scoring path"
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn postings_for_terms_matches_individual_lookup() {
        let dir = std::env::temp_dir().join("kosha-test-postings-batch");
        let expected = write_inverted_fixture(&dir);
        let r = SegmentReader::open(dir.clone()).unwrap();

        let terms = vec![
            "quick".to_string(),
            "brown".to_string(),
            "missing".to_string(),
        ];
        let batch = r.postings_for_terms(&terms);
        let got: HashMap<&str, Vec<Posting>> = batch
            .into_iter()
            .map(|(term, postings)| (term, postings.into_owned()))
            .collect();

        assert_eq!(got.get("quick"), expected.get("quick"));
        assert_eq!(got.get("brown"), expected.get("brown"));
        assert!(!got.contains_key("missing"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_v1_inverted_still_opens_via_eager_fallback() {
        // Segments already on disk/S3 keep the v1 stream layout — the
        // reader must detect the missing magic and fall back to the eager
        // parse with identical query results.
        let dir = std::env::temp_dir().join("kosha-test-inverted-v1-fallback");
        let expected = write_inverted_fixture(&dir);

        // Overwrite the v2 file with the same index serialized as v1.
        fs::write(
            dir.join("inverted.idx"),
            serialize_legacy_inverted(&expected),
        )
        .unwrap();

        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(
            matches!(r.inverted, InvertedAccess::Eager(_)),
            "v1 file must open on the eager fallback path"
        );
        assert!(
            !r.has_ordered_postings(),
            "eager v1 postings carry no ordering guarantee — order-sensitive \
             query paths (WAND leapfrog) must not run on them"
        );
        for (term, postings) in &expected {
            let got = r
                .postings(term)
                .unwrap_or_else(|| panic!("missing term {term}"));
            assert_eq!(&*got, postings.as_slice(), "postings mismatch for {term}");
        }
        // `all_terms` lists the combined index only (#249).
        let mut expected_terms: Vec<&str> = expected
            .keys()
            .map(|s| s.as_str())
            .filter(|t| !kosha_core::fields::is_field_key(t))
            .collect();
        expected_terms.sort();
        assert_eq!(r.all_terms(), expected_terms);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn v2_inverted_with_truncated_table_is_a_clean_corrupt_error() {
        // A v2 header whose term table extends past EOF must fail open
        // with CorruptSegment — never a panic, never silent garbage.
        let dir = std::env::temp_dir().join("kosha-test-inverted-v2-truncated");
        write_inverted_fixture(&dir);

        let raw = fs::read(dir.join("inverted.idx")).unwrap();
        // Keep the header (claiming the full term count) but cut the file
        // off in the middle of the term table.
        fs::write(dir.join("inverted.idx"), &raw[..INVERTED_HEADER_LEN + 3]).unwrap();

        match SegmentReader::open(dir.clone()) {
            Err(KoshaError::CorruptSegment(msg)) => {
                assert!(msg.contains("inverted.idx"), "unexpected message: {msg}")
            }
            Err(other) => panic!("expected CorruptSegment, got {other}"),
            Ok(_) => panic!("expected CorruptSegment, got a successful open"),
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn v2_postings_cache_serves_shared_decode_and_respects_budget() {
        // Repeated lookups of the same term must return the same shared
        // decode (pointer-equal Arc — the warm-path amortization the cache
        // exists for), and the byte budget must actually evict.
        let dir = std::env::temp_dir().join("kosha-test-postings-cache");
        write_inverted_fixture(&dir);
        let r = SegmentReader::open(dir.clone()).unwrap();

        // Second-touch admission: lookup 1 records the miss (no insert),
        // lookup 2 misses again → admitted and cached, lookup 3 hits.
        let first = r.postings("quick").unwrap();
        let second = r.postings("quick").unwrap();
        if let (PostingsRef::Shared(a), PostingsRef::Shared(b)) = (&first, &second) {
            assert!(
                !Arc::ptr_eq(a, b),
                "first two lookups are both misses under second-touch admission"
            );
        } else {
            panic!("v2 segment must return shared postings handles");
        }
        let third = r.postings("quick").unwrap();
        match (&second, &third) {
            (PostingsRef::Shared(a), PostingsRef::Shared(b)) => {
                assert!(
                    Arc::ptr_eq(a, b),
                    "third lookup must be a cache hit sharing the admitted decode"
                );
            }
            _ => panic!("v2 segment must return shared postings handles"),
        }

        // Budget-eviction behavior on the cache itself: two entries whose
        // combined size exceeds the budget can't both stay resident.
        let cache = PostingsCache::new(100, None);
        let big = Arc::new(vec![Posting {
            doc_id: 1,
            term_frequency: 1,
            positions: vec![0],
        }]);
        cache.insert(0, Arc::clone(&big), 60);
        cache.insert(1, Arc::clone(&big), 60);
        assert!(
            cache.get(0).is_none(),
            "oldest entry must be evicted once the byte budget is exceeded"
        );
        assert!(cache.get(1).is_some());
        // An entry larger than the whole budget is never cached (it would
        // evict everything and still churn).
        cache.insert(2, big, 1000);
        assert!(cache.get(2).is_none());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn v2_inverted_corrupt_postings_region_degrades_to_missing_term() {
        // Open-time validation deliberately covers only the table (scanning
        // the postings region would be the eager parse v2 exists to avoid).
        // A corrupt postings span must therefore surface at decode time as
        // a bounds-checked None — not a panic inside a scoring thread.
        let dir = std::env::temp_dir().join("kosha-test-inverted-v2-bad-postings");
        let expected = write_inverted_fixture(&dir);

        fs::write(
            dir.join("inverted.idx"),
            serialize_inline_v2_inverted(&expected),
        )
        .unwrap();
        let mut raw = fs::read(dir.join("inverted.idx")).unwrap();
        // Corrupt the first postings blob's count field to a huge value the
        // span can't physically hold. The first table entry's postings_off
        // is at header + 12, as a u64.
        let p_off_pos = INVERTED_HEADER_LEN + 12;
        let p_off = u64::from_le_bytes(raw[p_off_pos..p_off_pos + 8].try_into().unwrap()) as usize;
        raw[p_off..p_off + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        fs::write(dir.join("inverted.idx"), &raw).unwrap();

        let r = SegmentReader::open(dir.clone()).unwrap();
        // The first table entry: per-field keys (#249) sort first.
        let first_term = expected.keys().min().unwrap().to_string();
        assert!(
            r.postings(&first_term).is_none(),
            "corrupt postings must decode to None, not panic or garbage"
        );
        // Other terms' postings are untouched and must still decode.
        let last_term = r.all_terms().last().unwrap().to_string();
        assert_eq!(
            &*r.postings(&last_term).unwrap(),
            expected[&last_term].as_slice()
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn filters_parse_lazily_on_first_use() {
        // Opening a segment must not parse filters.bin — only a query that
        // actually filters/aggregates/sorts-by-value pays that cost (the
        // eager parse was ~529ms/segment on the worst staging namespace).
        let dir = std::env::temp_dir().join("kosha-test-lazy-filters");
        let _ = fs::remove_dir_all(&dir);
        // The legacy `filters.bin` path; columnar segments have their own
        // tests in `filter_column_format_tests`.
        let mut w =
            SegmentWriter::new(SegmentId("s1".into()), dir.clone()).with_filters_write_version(1);
        w.add_document(
            DocumentId("d1".into()),
            vec![
                Field::text("t", "hello world"),
                Field::keyword("tag", "alpha"),
            ],
        );
        w.finalize(Bm25Params::default()).unwrap();

        let r = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        assert!(
            r.filters.state.lock().unwrap().legacy.is_none(),
            "open must not have parsed filters.bin"
        );
        let store = r.filter_store();
        assert!(store.string_fields.contains_key("tag"));
        assert!(
            r.filters.state.lock().unwrap().legacy.is_some(),
            "first filter_store() call materializes the parse"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn cached_reader_sees_filters_hydrated_after_open() {
        // Regression for the filters-skipping-hydration composition bug: a
        // broad query opens (and caches) a segment while filters.bin isn't
        // local; a later filtered query hydrates the file and reuses the
        // cached reader. Snapshotting filters.bin's bytes at open would
        // freeze that reader's filters as empty forever — silently matching
        // nothing. The reader must read the file at first *use* instead.
        let dir = std::env::temp_dir().join("kosha-test-filters-hydrated-late");
        let _ = fs::remove_dir_all(&dir);
        // The legacy `filters.bin` path; columnar segments have their own
        // tests in `filter_column_format_tests`.
        let mut w =
            SegmentWriter::new(SegmentId("s1".into()), dir.clone()).with_filters_write_version(1);
        w.add_document(
            DocumentId("d1".into()),
            vec![
                Field::text("t", "hello world"),
                Field::keyword("tag", "alpha"),
            ],
        );
        w.finalize(Bm25Params::default()).unwrap();

        // Simulate scoring-only hydration: filters.bin not local at open.
        let stash = dir.join("filters.bin.stash");
        fs::rename(dir.join("filters.bin"), &stash).unwrap();
        let r = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        assert!(r.filters.state.lock().unwrap().legacy.is_none());

        // "Hydrate" the file, then use the SAME (cached) reader.
        fs::rename(&stash, dir.join("filters.bin")).unwrap();
        let store = r.filter_store();
        assert!(
            store.string_fields.contains_key("tag"),
            "cached reader must see filters.bin hydrated after it was opened"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn cached_reader_recovers_after_first_use_races_hydration() {
        // Regression for the live-production bug this composition-only test
        // above didn't cover: `cached_reader_sees_filters_hydrated_after_open`
        // only exercises "opened before hydration, first *use* after
        // hydration" — but in production the race was "*first use* itself
        // lands before hydration" (a broad/filter-only query touches
        // `filter_store()` while `filters.bin` is still mid-fetch). A
        // `OnceLock`-backed cache would permanently freeze that premature
        // empty parse; every later query against the same cached reader —
        // even long after the file was correctly hydrated — would keep
        // silently matching zero documents. Confirmed live on staging:
        // `filters.bin` byte-verified correct on disk, yet the cached
        // reader's query still returned zero hits.
        let dir = std::env::temp_dir().join("kosha-test-filters-first-use-races-hydration");
        let _ = fs::remove_dir_all(&dir);
        // The legacy `filters.bin` path; columnar segments have their own
        // tests in `filter_column_format_tests`.
        let mut w =
            SegmentWriter::new(SegmentId("s1".into()), dir.clone()).with_filters_write_version(1);
        w.add_document(
            DocumentId("d1".into()),
            vec![
                Field::text("t", "hello world"),
                Field::keyword("tag", "alpha"),
            ],
        );
        w.finalize(Bm25Params::default()).unwrap();

        let stash = dir.join("filters.bin.stash");
        fs::rename(dir.join("filters.bin"), &stash).unwrap();
        let r = SegmentReader::open_with_options(dir.clone(), false).unwrap();

        // First use races hydration: filters.bin isn't local yet.
        let store = r.filter_store();
        assert!(
            !store.string_fields.contains_key("tag"),
            "first use before hydration sees an empty store, as expected"
        );
        assert!(
            r.filters.state.lock().unwrap().legacy.is_none(),
            "an empty read caused by a missing file must not be cached — \
             only a confirmed-present read commits to the cache"
        );

        // Hydration lands; the SAME cached reader is used again.
        fs::rename(&stash, dir.join("filters.bin")).unwrap();
        let store = r.filter_store();
        assert!(
            store.string_fields.contains_key("tag"),
            "second use after hydration must recover — a premature empty \
             first read must not be permanent"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn doc_meta_ids_come_from_the_arena_correctly() {
        // The offsets sidecar's per-doc ids live in one contiguous pool —
        // every doc's id must slice back out exactly, in order.
        let dir = std::env::temp_dir().join("kosha-test-docid-arena");
        let _ = fs::remove_dir_all(&dir);
        let mut w = SegmentWriter::new(SegmentId("s1".into()), dir.clone());
        for i in 0..50 {
            w.add_document(
                DocumentId(format!("doc-{i}-{}", "x".repeat(i % 7))),
                vec![Field::text("t", "hello")],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();

        let r = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        assert!(matches!(r.doc_store, DocStoreAccess::Lazy { .. }));
        for i in 0..50u32 {
            let meta = r.doc_meta(i).unwrap();
            assert_eq!(
                meta.doc_id,
                format!("doc-{i}-{}", "x".repeat(i as usize % 7))
            );
            assert_eq!(meta.doc_seq, i);
        }
        assert!(r.doc_meta(50).is_none());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn has_local_doc_store_rejects_truncated_and_empty_files() {
        // Regression: an interrupted hydration write can leave a zero-byte
        // or truncated `doc_store.bin` on disk. Existence-as-presence made
        // the searcher skip the hydrator and fail every materialize of a
        // doc past the truncation point ("failed to fill whole buffer"),
        // permanently. The offsets sidecar knows the expected file size —
        // presence must be validated against it.
        let dir = std::env::temp_dir().join("kosha-test-doc-store-truncation");
        write_inverted_fixture(&dir);
        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(r.has_local_doc_store(), "intact file must read as present");

        let bin = dir.join("doc_store.bin");
        let full = fs::read(&bin).unwrap();

        fs::write(&bin, &full[..full.len() / 2]).unwrap();
        assert!(
            !r.has_local_doc_store(),
            "truncated doc_store.bin must read as absent"
        );

        fs::write(&bin, b"").unwrap();
        assert!(
            !r.has_local_doc_store(),
            "zero-byte doc_store.bin must read as absent"
        );

        fs::write(&bin, &full).unwrap();
        assert!(
            r.has_local_doc_store(),
            "restored file must read as present again"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn doc_span_bytes_parse_identically_to_local_reads() {
        // The ranged-read contract: for every doc, slicing exactly
        // `doc_span` out of `doc_store.bin` and parsing it via
        // `doc_record_from_bytes` must equal `doc_record_full`'s local
        // read — this is what page materialization fetches over a ranged
        // S3 GET instead of the whole doc store.
        let dir = std::env::temp_dir().join("kosha-test-doc-span-ranged");
        let _ = fs::remove_dir_all(&dir);
        let mut w = SegmentWriter::new(SegmentId("s1".into()), dir.clone());
        for i in 0..20 {
            w.add_document(
                DocumentId(format!("d{i}")),
                vec![
                    Field::text("content", format!("hello world {}", "pad ".repeat(i * 3))),
                    Field::keyword("tag", if i % 2 == 0 { "even" } else { "odd" }),
                ],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();

        let r = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        assert!(matches!(r.doc_store, DocStoreAccess::Lazy { .. }));
        assert!(r.has_local_doc_store());

        let store = fs::read(dir.join("doc_store.bin")).unwrap();
        let mut spans = Vec::new();
        for i in 0..20u32 {
            let (offset, length) = r.doc_span(i).unwrap();
            let bytes = &store[offset as usize..(offset + length as u64) as usize];
            spans.push((i, bytes.to_vec()));
            let from_bytes = r.doc_record_from_bytes(i, bytes).unwrap().unwrap();
            let from_disk = r.doc_record_full(i).unwrap().unwrap();
            assert_eq!(from_bytes.doc_id, from_disk.doc_id);
            assert_eq!(from_bytes.field_length, from_disk.field_length);
            assert_eq!(from_bytes.fields.len(), from_disk.fields.len());
            for (a, b) in from_bytes.fields.iter().zip(from_disk.fields.iter()) {
                assert_eq!(a.name, b.name);
                assert_eq!(a.value, b.value);
                assert_eq!(a.field_type, b.field_type);
            }
        }
        assert!(r.doc_span(20).is_none(), "out-of-range doc_seq");

        // Wrong-length buffer = wrong range fetched — must error, not parse
        // garbage.
        let (_, len0) = r.doc_span(0).unwrap();
        assert!(r
            .doc_record_from_bytes(0, &store[..len0 as usize + 1])
            .is_err());

        // With doc_store.bin gone (scoring-set-only hydration), the reader
        // knows it can't serve locally, but remotely-fetched span bytes
        // still materialize every doc.
        fs::remove_file(dir.join("doc_store.bin")).unwrap();
        assert!(!r.has_local_doc_store());
        for (i, bytes) in &spans {
            let rec = r.doc_record_from_bytes(*i, bytes).unwrap().unwrap();
            assert_eq!(rec.doc_id.0, format!("d{i}"));
        }

        // Legacy Eager open (no offsets sidecar): spans don't exist, but
        // records are fully resident so no remote fetch is ever needed.
        fs::write(dir.join("doc_store.bin"), &store).unwrap();
        fs::remove_file(dir.join("doc_store.offsets")).unwrap();
        let legacy = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        assert!(matches!(legacy.doc_store, DocStoreAccess::Eager(_)));
        assert!(legacy.doc_span(0).is_none());
        assert!(legacy.has_local_doc_store());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn tokenize_with_positions_works() {
        let r = tokenize_with_positions("quick brown fox");
        assert_eq!(
            r,
            vec![
                ("quick".to_string(), 0),
                ("brown".to_string(), 1),
                ("fox".to_string(), 2),
            ]
        );
    }

    #[test]
    fn write_and_read_segment_with_positions() {
        let dir = std::env::temp_dir().join("kosha-test-seg-positions");
        let _ = fs::remove_dir_all(&dir);
        let seg_id = SegmentId("test".into());
        let mut w = SegmentWriter::new(seg_id.clone(), dir.clone());
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("t", "quick brown fox")],
        );
        w.add_document(
            DocumentId("d2".into()),
            vec![Field::text("t", "quick fox is quick")],
        );
        w.finalize(Bm25Params::default()).unwrap();

        let r = SegmentReader::open(dir.clone()).unwrap();
        // "quick" appears at positions 0 in d1 and positions 0,3 in d2
        let p = r.postings("quick").unwrap();
        assert_eq!(p.len(), 2);
        assert_eq!(p[0].positions, vec![0]);
        assert_eq!(p[1].positions, vec![0, 3]);

        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression for the bug this fixes: `iter_doc_records` used to
    /// silently drop any `doc_seq` whose read failed instead of surfacing
    /// it — see that method's doc comment for the incident this caused
    /// (compaction quietly losing ~0.24% of a 10M-doc corpus, no crash, no
    /// error). Truncating `doc_store.bin` out from under an intact
    /// `doc_store.offsets` reproduces a read failure for the last document
    /// without touching anything else.
    #[test]
    fn iter_doc_records_surfaces_read_failures_instead_of_dropping_them() {
        let dir = std::env::temp_dir().join("kosha-test-iter-doc-records-error");
        let _ = fs::remove_dir_all(&dir);
        let seg_id = SegmentId("test".into());
        let mut w = SegmentWriter::new(seg_id.clone(), dir.clone());
        for i in 0..5 {
            w.add_document(
                DocumentId(format!("d{i}")),
                vec![Field::text("t", format!("doc number {i}"))],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();

        // doc_store.offsets (built alongside doc_store.bin, still intact)
        // still claims a byte range for the last document that no longer
        // exists once doc_store.bin is truncated — doc_record_full's
        // read_exact for that doc_seq must now fail.
        let doc_store_path = dir.join("doc_store.bin");
        let full = fs::read(&doc_store_path).unwrap();
        fs::write(&doc_store_path, &full[..full.len() - 5]).unwrap();

        let r = SegmentReader::open(dir.clone()).unwrap();
        assert_eq!(
            r.doc_count(),
            5,
            "footer/offsets are untouched by the truncation"
        );

        let results: Vec<_> = r.iter_doc_records().collect();
        assert_eq!(
            results.len(),
            5,
            "the iterator must still produce one item per doc_seq — Err, not a gap"
        );
        assert!(
            results.iter().any(|res| res.is_err()),
            "the doc whose bytes were truncated away must surface as Err"
        );
        assert_eq!(
            results.iter().filter(|res| res.is_ok()).count(),
            4,
            "every doc_seq unaffected by the truncation must still read fine"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_with_options_skips_vectors_when_not_requested() {
        // Rewritten for the v2 writer default (`vector_write_version`):
        // segments finalized today carry a lazy posting index, not
        // `vector_store`/`hnsw_map` — see `v2_vector_index_opens_lazily_and_knn_search_finds_nearest`
        // for the v2-specific assertions this test used to make inline.
        // What's still true, and still worth its own test, is the
        // `load_vectors=false` contract: a lexical-only open must skip
        // *all* vector work — neither the legacy fields nor the lazy index
        // get populated — while lexical data stays fully readable.
        let dir = std::env::temp_dir().join("kosha-test-seg-lazy-vectors");
        let _ = fs::remove_dir_all(&dir);
        let seg_id = SegmentId("test".into());
        let mut w = SegmentWriter::new(seg_id.clone(), dir.clone());
        w.add_document(
            DocumentId("d1".into()),
            vec![
                Field::text("t", "quick brown fox"),
                Field::vector("contentEmbedding", vec![0.1, 0.2, 0.3]),
            ],
        );
        w.finalize(Bm25Params::default()).unwrap();

        // Lexical path: no vectors read, no lazy index built either.
        let lexical = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        assert!(lexical.vector_store().vectors.is_empty());
        assert!(!lexical.has_lazy_vector_index());
        // Lexical data is unaffected — still fully readable.
        assert!(lexical.postings("quick").is_some());

        // KNN path (and open(), its default): the v2 lazy index is present
        // and can actually find the vector.
        let knn = SegmentReader::open_with_options(dir.clone(), true).unwrap();
        assert!(knn.has_lazy_vector_index());
        let results = knn
            .knn_search(&[0.1, 0.2, 0.3], 1, 16)
            .expect("lazy index present")
            .unwrap();
        assert_eq!(results.first().map(|(id, _)| *id), Some(0));

        let default_open = SegmentReader::open(dir.clone()).unwrap();
        assert!(default_open.has_lazy_vector_index());

        let _ = fs::remove_dir_all(&dir);
    }

    /// Writes a segment the same way `finalize` does, except with
    /// `write_vectors_v2` in place of the legacy `write_vectors` — mirrors
    /// `finalize`'s body since `write_vectors_v2` isn't wired into it yet
    /// (that's the writer-cutover step, done only after this reader/query
    /// path is proven). Collapses back into a single `finalize` call once
    /// that cutover lands.
    fn write_v2_fixture(dir: &Path, vectors: &[(&str, Vec<f32>)]) -> SegmentId {
        let _ = fs::remove_dir_all(dir);
        let seg_id = SegmentId("test".into());
        let mut w = SegmentWriter::new(seg_id.clone(), dir.to_path_buf());
        for (doc_id, v) in vectors {
            w.add_document(
                DocumentId(doc_id.to_string()),
                vec![Field::vector("embedding", v.clone())],
            );
        }
        w.backend.create_dir_all("").unwrap();
        w.write_doc_store().unwrap();
        w.write_inverted_index().unwrap();
        w.write_filters().unwrap();
        let vector_fields = w.write_vectors_v2().unwrap();
        w.write_footer(Bm25Params::default(), vector_fields, Vec::new())
            .unwrap();
        seg_id
    }

    #[test]
    fn warm_knn_serves_postings_from_cache_without_touching_vector_idx() {
        let dir = std::env::temp_dir().join("kosha-test-vector-v2-posting-cache");
        write_v2_fixture(
            &dir,
            &[
                ("d0", vec![1.0, 0.0, 0.0, 0.0]),
                ("d1", vec![0.0, 1.0, 0.0, 0.0]),
                ("d2", vec![0.0, 0.0, 1.0, 0.0]),
                ("d3", vec![0.9, 0.1, 0.0, 0.0]),
            ],
        );

        let r = SegmentReader::open(dir.clone()).unwrap();
        let q = [1.0, 0.0, 0.0, 0.0];
        // Two identical searches: the first records every probed posting in
        // the second-touch admission ring, the second hydrates them again
        // and caches the decodes.
        let first = r.knn_search(&q, 2, 16).expect("lazy index").unwrap();
        let second = r.knn_search(&q, 2, 16).expect("lazy index").unwrap();
        assert_eq!(first, second);
        assert_eq!(first.first().map(|(id, _)| *id), Some(0));

        // Delete the backing file: a warm search must be served entirely
        // from the decoded-postings cache and never reopen `vector.idx`.
        fs::remove_file(dir.join("vector.idx")).unwrap();
        let third = r
            .knn_search(&q, 2, 16)
            .expect("lazy index")
            .expect("warm search must not read vector.idx");
        assert_eq!(first, third);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn v2_vector_index_opens_lazily_and_knn_search_finds_nearest() {
        let dir = std::env::temp_dir().join("kosha-test-vector-v2-lazy");
        write_v2_fixture(
            &dir,
            &[
                ("d0", vec![1.0, 0.0, 0.0, 0.0]),
                ("d1", vec![0.0, 1.0, 0.0, 0.0]),
                ("d2", vec![0.0, 0.0, 1.0, 0.0]),
                ("d3", vec![0.9, 0.1, 0.0, 0.0]),
            ],
        );

        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(
            r.has_lazy_vector_index(),
            "v2 segment should open on the lazy path"
        );
        assert!(
            r.vector_store().vectors.is_empty(),
            "legacy fields stay empty on the lazy path"
        );

        let results = r
            .knn_search(&[1.0, 0.0, 0.0, 0.0], 2, 16)
            .expect("lazy path present")
            .unwrap();
        assert_eq!(results.len(), 2);
        let top_ids: Vec<u32> = results.iter().map(|(id, _)| *id).collect();
        // doc_seq 0 ("d0", exact match) and 3 ("d3", nearest neighbor) should
        // be the two closest to the query, in either score order between
        // themselves but both ahead of docs 1/2.
        assert!(
            top_ids.contains(&0),
            "exact match must be in the top-2: {top_ids:?}"
        );
        assert!(
            top_ids.contains(&3),
            "near neighbor must be in the top-2: {top_ids:?}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_vector_index_lazy_path_never_reads_the_full_vector_idx_body() {
        // The bug this proves fixed: `load_vector_index` used to
        // `fs::read` the *entire* `vector.idx` file just to check its
        // 16-byte header, even on this — the common — path, where the
        // real `LazyVectorIndex` is built from the small `vector.offsets`
        // sidecar and the rest of `vector.idx`'s body is never consulted.
        let dir = std::env::temp_dir().join("kosha-test-vecidx-header-only-read");
        write_v2_fixture(&dir, &[("d0", vec![1.0, 0.0]), ("d1", vec![0.0, 1.0])]);

        // Balloon vector.idx well past anything a header-only read would
        // ever touch, while leaving vector.offsets (what the lazy path
        // actually relies on) untouched and small. Real segments are
        // documented elsewhere in this file as "hundreds of MB to GBs" —
        // 64 MiB is representative and safely allocatable in a test.
        let idx_path = dir.join("vector.idx");
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&idx_path)
            .unwrap();
        f.write_all(&vec![0u8; 64 * 1024 * 1024]).unwrap();
        drop(f);

        let before = ALLOC.total_allocated();
        let r = SegmentReader::open(dir.clone()).unwrap();
        let allocated = ALLOC.total_allocated().saturating_sub(before);

        assert!(
            r.has_lazy_vector_index(),
            "a valid v2 header + good sidecar must still take the lazy path \
             after padding — the padding is appended past the parsed body, \
             not corrupting it"
        );
        // `ALLOC` is process-wide, so tests running on other threads add
        // to the count while this one opens the segment (enough to cross
        // 1 MiB under a loaded full-workspace run). Reading the body would
        // allocate at least 64 MiB, so 16 MiB still separates the two.
        assert!(
            allocated < 16 * 1024 * 1024,
            "opening a segment with a 64MiB-padded vector.idx allocated \
             {allocated} bytes — the lazy path must only read the small \
             vector.offsets sidecar, not the full vector.idx body"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_vector_index_short_file_falls_back_to_legacy_path_not_an_error() {
        // Fewer than VECTOR_IDX_HEADER_LEN (16) bytes: the header-only read
        // must yield a short buffer without erroring, and
        // `parse_vector_idx_header` must see exactly what it saw when this
        // read the whole file — `data.len() < VECTOR_IDX_HEADER_LEN` — and
        // fall through to the legacy path, not treat this as a hard error.
        let dir = std::env::temp_dir().join("kosha-test-vecidx-short-file");
        // Build a full, valid segment scaffold (footer/inverted/doc_store/
        // offsets) via the shared fixture, then overwrite just vector.idx —
        // `open_with_options` needs the rest of the segment to exist.
        write_v2_fixture(&dir, &[("d0", vec![1.0, 0.0])]);
        // 4 bytes: `read_vectors` reads this as dim=<garbage>, count
        // unreadable (needs 8), so it must degrade to `VectorStore::default()`
        // rather than panicking or erroring — matches its own short-file
        // handling (`cursor.len() < 8`).
        fs::write(dir.join("vector.idx"), [1u8, 0, 0, 0]).unwrap();

        let r = SegmentReader::open_with_options(dir.clone(), true).unwrap();
        assert!(
            !r.has_lazy_vector_index(),
            "a file shorter than the header must never take the lazy path"
        );
        assert!(r.vector_store().vectors.is_empty());
        assert!(!r.vector_index_degraded());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_vector_index_legacy_v1_format_still_parses_via_read_vectors() {
        // No magic prefix at all — the pre-v2 flat format `dim(4) +
        // count(4) + records`. The header-only read must not mistake this
        // for a valid-but-truncated v2 header (it has no magic to match in
        // the first place) and must route to `Self::read_vectors`, which
        // re-reads the file independently — unaffected by how much of it
        // the header check above already consumed.
        let dir = std::env::temp_dir().join("kosha-test-vecidx-legacy-v1");
        // Same reasoning as the short-file test above: need a full, valid
        // segment scaffold before overwriting vector.idx.
        write_v2_fixture(&dir, &[("d0", vec![1.0, 0.0])]);
        let dim: u32 = 2;
        let count: u32 = 1;
        let mut buf = Vec::new();
        buf.extend_from_slice(&dim.to_le_bytes());
        buf.extend_from_slice(&count.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // doc_seq
        buf.extend_from_slice(&1.0f32.to_le_bytes());
        buf.extend_from_slice(&0.0f32.to_le_bytes());
        fs::write(dir.join("vector.idx"), &buf).unwrap();

        let r = SegmentReader::open_with_options(dir.clone(), true).unwrap();
        assert!(
            !r.has_lazy_vector_index(),
            "legacy v1 has no lazy index — served by exact flat_knn"
        );
        assert_eq!(
            r.vector_store().vectors.len(),
            1,
            "must recover the one legacy vector"
        );
        assert_eq!(r.vector_store().dimensions, 2);
        assert!(!r.vector_index_degraded());
        assert!(!vector_index_needs_offsets(&dir), "legacy flat format");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_vector_index_corrupt_body_with_no_sidecar_marks_degraded() {
        // Valid v2 header, no sidecar at all, and a body too short for the
        // header's own `total_count` — genuinely unrecoverable. This is the
        // one case that legitimately needs the second, fallback full-file
        // `fs::read` this fix adds; must still mark `degraded`, not silently
        // report "no vectors" the way a segment that never had any would.
        let dir = std::env::temp_dir().join("kosha-test-vecidx-corrupt-body");
        write_v2_fixture(&dir, &[("d0", vec![1.0, 0.0]), ("d1", vec![0.0, 1.0])]);
        fs::remove_file(dir.join("vector.offsets")).unwrap();
        assert!(vector_index_needs_offsets(&dir), "clustered format");
        // Truncate the real (valid-header) blob down to just past the
        // header — declares more records than the body actually has.
        let real = fs::read(dir.join("vector.idx")).unwrap();
        fs::write(dir.join("vector.idx"), &real[..VECTOR_IDX_HEADER_LEN + 4]).unwrap();

        let r = SegmentReader::open_with_options(dir.clone(), true).unwrap();
        assert!(!r.has_lazy_vector_index());
        assert!(r.vector_store().vectors.is_empty());
        assert!(
            r.vector_index_degraded(),
            "a valid header with an unparseable body must be marked degraded, \
             not treated as a legitimate vector-less segment"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn v2_vector_index_falls_back_to_eager_parse_when_sidecar_missing() {
        let dir = std::env::temp_dir().join("kosha-test-vector-v2-eager-fallback");
        write_v2_fixture(&dir, &[("d0", vec![1.0, 0.0]), ("d1", vec![0.0, 1.0])]);

        // Simulate a lost/corrupt sidecar — the writer always emits both,
        // but a partial hydration or disk issue could leave vector.idx
        // present without vector.offsets.
        fs::remove_file(dir.join("vector.offsets")).unwrap();

        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(
            !r.has_lazy_vector_index(),
            "missing sidecar must fall back to the eager path, not silently drop vectors"
        );
        assert_eq!(
            r.vector_store().vectors.len(),
            2,
            "eager fallback must recover every vector"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_with_no_vector_fields_has_no_vector_index_at_all() {
        let dir = std::env::temp_dir().join("kosha-test-vector-v2-no-vectors");
        let _ = fs::remove_dir_all(&dir);
        let mut w = SegmentWriter::new(SegmentId("test".into()), dir.clone());
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("t", "no embeddings here")],
        );
        w.finalize(Bm25Params::default()).unwrap();
        assert!(
            !dir.join("vector.idx").exists(),
            "no vector fields must mean no vector.idx at all"
        );

        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(!r.has_lazy_vector_index());
        assert!(r.vector_store().vectors.is_empty());
        assert!(
            r.knn_search(&[0.0], 5, 16).is_none(),
            "no lazy index means knn_search returns None, not an error"
        );
        assert!(
            !r.vector_index_degraded(),
            "a segment with no vector field at all is not degraded — it \
             legitimately has nothing to search"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_vector_idx_body_is_marked_degraded_not_silently_emptied() {
        // `vector_index` and `vector_store` end up empty here exactly as in
        // `segment_with_no_vector_fields_has_no_vector_index_at_all` above —
        // same shape, different reality: this segment's vectors exist but
        // are unreadable. `vector_index_degraded()` is the only thing that
        // tells the two apart, and it's what the query layer checks before
        // deciding whether to count the segment in `knn_degraded_segments`.
        let dir = std::env::temp_dir().join("kosha-test-vector-v2-corrupt-body");
        write_v2_fixture(
            &dir,
            &[
                ("d0", vec![1.0, 0.0, 0.0, 0.0]),
                ("d1", vec![0.0, 1.0, 0.0, 0.0]),
            ],
        );

        // Force the eager-parse fallback (drop the sidecar), then truncate
        // vector.idx's body so even the eager parse fails too — a valid v2
        // header with an unrecoverable body, the one branch of
        // `load_vector_index` that can't serve the segment any other way.
        fs::remove_file(dir.join("vector.offsets")).unwrap();
        let idx_path = dir.join("vector.idx");
        let mut bytes = fs::read(&idx_path).unwrap();
        bytes.truncate(bytes.len() - 3);
        fs::write(&idx_path, &bytes).unwrap();

        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(
            r.vector_index_degraded(),
            "corrupt body behind a valid v2 header must be marked degraded, \
             not indistinguishable from a legitimately vector-less segment"
        );
        assert!(!r.has_lazy_vector_index());
        assert!(
            r.vector_store().vectors.is_empty(),
            "a corrupt body has no recoverable vectors — degraded, not \
             silently repopulated from somewhere else"
        );
        assert!(
            r.knn_search(&[1.0, 0.0, 0.0, 0.0], 2, 16).is_none(),
            "knn_search still reports None (same as vector-less) — callers \
             must check vector_index_degraded() to tell the two apart"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn finalize_writes_filter_blooms() {
        let dir = std::env::temp_dir().join("kosha-test-seg-blooms");
        let _ = fs::remove_dir_all(&dir);
        let mut w = SegmentWriter::new(SegmentId("test".into()), dir.clone());
        w.add_document(
            DocumentId("d1".into()),
            vec![
                Field::text("content", "hello"),
                Field::text("matterId", "m1"),
            ],
        );
        w.finalize(Bm25Params::default()).unwrap();

        let footer = SegmentReader::read_footer(&dir).unwrap();
        let blooms = footer.filter_blooms.expect("blooms written");
        let matter = blooms.get("matterId").expect("matterId bloom");
        assert!(matter.may_contain("m1"));
        assert!(!matter.may_contain("m-absent"));

        let term_bloom = footer.term_bloom.expect("term bloom written");
        assert!(term_bloom.may_contain("hello"));
        assert!(!term_bloom.may_contain("absent-term-xyz"));

        let _ = fs::remove_dir_all(&dir);
    }

    /// #243: long `Text` values (prose bodies) stay out of `filters.bin` and
    /// the footer blooms, while short ones (ids, statuses — what clients
    /// actually filter on, since every JSON string arrives as `Text`) stay
    /// filterable. The long value must remain BM25-searchable and returned
    /// verbatim from the doc store.
    #[test]
    fn long_text_values_skip_filter_columns() {
        let dir = std::env::temp_dir().join("kosha-test-seg-text-ignore-above");
        let _ = fs::remove_dir_all(&dir);
        let body = "contract dispute ".repeat(40); // 680 chars
        let mut w = SegmentWriter::new(SegmentId("test".into()), dir.clone())
            .with_text_filter_ignore_above(Some(256));
        w.add_document(
            DocumentId("d1".into()),
            vec![
                Field::text("content", body.clone()),
                Field::text("matterId", "m1"),
                Field::keyword("tag", "x".repeat(1000)),
            ],
        );
        w.add_document(
            DocumentId("d2".into()),
            vec![Field::text("content", "short body")],
        );
        let footer = w.finalize(Bm25Params::default()).unwrap();
        assert_eq!(footer.text_filter_ignore_above, Some(256));

        let r = SegmentReader::open(dir.clone()).unwrap();
        let store = r.filter_store();
        assert_eq!(
            store.string_fields["matterId"],
            vec![(0, "m1".to_string())],
            "short Text stays filterable"
        );
        assert_eq!(
            store.string_fields["content"],
            vec![(1, "short body".to_string())],
            "only the over-cap Text value is dropped, per value not per field"
        );
        assert_eq!(
            store.string_fields["tag"].len(),
            1,
            "Keyword is never capped — it is the explicit exact-match type"
        );

        let blooms = SegmentReader::read_footer(&dir)
            .unwrap()
            .filter_blooms
            .expect("blooms written");
        assert!(blooms["content"].may_contain("short body"));
        assert!(blooms["matterId"].may_contain("m1"));

        assert!(
            !r.postings("dispute").unwrap().is_empty(),
            "over-cap Text is still tokenized into the inverted index"
        );
        let doc = r.doc_record_full(0).unwrap().expect("doc 0");
        let stored = doc
            .fields
            .iter()
            .find(|f| f.name == "content")
            .expect("content stored");
        assert_eq!(stored.value, body, "doc store keeps the full value");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn text_filter_cap_off_copies_every_value() {
        let dir = std::env::temp_dir().join("kosha-test-seg-text-ignore-above-off");
        let _ = fs::remove_dir_all(&dir);
        let body = "y".repeat(5000);
        let mut w = SegmentWriter::new(SegmentId("test".into()), dir.clone())
            .with_text_filter_ignore_above(None);
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("content", body.clone())],
        );
        let footer = w.finalize(Bm25Params::default()).unwrap();
        assert_eq!(footer.text_filter_ignore_above, None);
        let r = SegmentReader::open(dir.clone()).unwrap();
        assert_eq!(r.filter_store().string_fields["content"], vec![(0, body)]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn text_filter_cap_counts_chars_not_bytes() {
        // 4 chars, 12 bytes: under a 4-char cap despite exceeding it in bytes,
        // matching ES `ignore_above` (a char count).
        assert!(text_value_within_cap("日本語文", Some(4)));
        assert!(!text_value_within_cap("日本語文字", Some(4)));
        assert!(text_value_within_cap("abcd", Some(4)));
        assert!(!text_value_within_cap("abcde", Some(4)));
        assert!(text_value_within_cap(&"z".repeat(100_000), None));
    }

    #[test]
    fn parse_text_filter_ignore_above_values() {
        let d = Some(DEFAULT_TEXT_FILTER_IGNORE_ABOVE);
        assert_eq!(parse_text_filter_ignore_above(None), d);
        assert_eq!(parse_text_filter_ignore_above(Some("")), d);
        assert_eq!(parse_text_filter_ignore_above(Some("1024")), Some(1024));
        assert_eq!(parse_text_filter_ignore_above(Some(" OFF ")), None);
        assert_eq!(parse_text_filter_ignore_above(Some("0")), d);
        assert_eq!(parse_text_filter_ignore_above(Some("lots")), d);
    }

    #[test]
    fn footer_without_text_cap_field_deserializes_as_uncapped() {
        // Footers written before #243 (and every manifest row carrying one)
        // have no `text_filter_ignore_above` key.
        let dir = std::env::temp_dir().join("kosha-test-seg-text-cap-legacy-footer");
        let _ = fs::remove_dir_all(&dir);
        let mut w = SegmentWriter::new(SegmentId("test".into()), dir.clone());
        w.add_document(DocumentId("d1".into()), vec![Field::text("t", "a")]);
        w.finalize(Bm25Params::default()).unwrap();
        let mut json: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.join("footer.json")).unwrap()).unwrap();
        json.as_object_mut()
            .unwrap()
            .remove("text_filter_ignore_above");
        let footer: Footer = serde_json::from_value(json).unwrap();
        assert_eq!(footer.text_filter_ignore_above, None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rewrite_term_bloom_updates_legacy_footer() {
        let dir = std::env::temp_dir().join("kosha-test-seg-rewrite-term-bloom");
        let _ = fs::remove_dir_all(&dir);
        let mut w = SegmentWriter::new(SegmentId("test".into()), dir.clone());
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("content", "contract dispute")],
        );
        w.finalize(Bm25Params::default()).unwrap();

        let mut footer = SegmentReader::read_footer(&dir).unwrap();
        footer.term_bloom = None;
        fs::write(
            dir.join("footer.json"),
            serde_json::to_string_pretty(&footer).unwrap(),
        )
        .unwrap();
        assert!(SegmentReader::read_footer(&dir)
            .unwrap()
            .term_bloom
            .is_none());

        let rewritten = SegmentReader::rewrite_term_bloom(&dir).unwrap();
        let bloom = rewritten.term_bloom.expect("term bloom rebuilt");
        assert!(bloom.may_contain("contract"));
        assert!(bloom.may_contain("dispute"));
        assert!(!bloom.may_contain("absent-term-xyz"));

        let _ = fs::remove_dir_all(&dir);
    }

    fn three_doc_segment(tag: &str) -> (PathBuf, Footer) {
        let dir = std::env::temp_dir().join(format!(
            "kosha-test-seg-footer-reconcile-{tag}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let mut w = SegmentWriter::new(SegmentId("seg".into()), dir.clone());
        for i in 0..3 {
            w.add_document(
                DocumentId(format!("d{i}")),
                vec![Field::text("content", "hello")],
            );
        }
        let footer = w.finalize(Bm25Params::default()).unwrap();
        (dir, footer)
    }

    /// #184: a manifest-supplied footer with a stale doc count must not make
    /// the reader reject a correct sidecar when the segment's own
    /// footer.json is there to settle it.
    #[test]
    fn stale_supplied_footer_is_reconciled_against_footer_json() {
        let (dir, footer) = three_doc_segment("stale");
        let mut stale = footer.clone();
        stale.doc_count = 6;
        fs::remove_file(dir.join("doc_store.bin")).unwrap();

        assert!(doc_store_opens_lazily(&dir, Some(&footer)));
        assert!(doc_store_offsets_plausible(&dir, Some(&footer)));
        assert!(
            doc_store_opens_lazily(&dir, Some(&stale)),
            "footer.json agrees with the sidecar, so the stale copy is replaced"
        );
        assert!(doc_store_offsets_plausible(&dir, Some(&stale)));
        let reader =
            SegmentReader::open_with_footer_options(dir.clone(), false, Some(stale.clone()), None)
                .expect("opens lazily without doc_store.bin");
        assert_eq!(
            reader.footer().doc_count,
            3,
            "uses the segment's own footer"
        );
        assert!(matches!(reader.doc_store, DocStoreAccess::Lazy { .. }));

        // Without footer.json there is nothing to reconcile against: the
        // sidecar is rejected, and the eager fallback names what is missing
        // instead of a bare "No such file or directory".
        fs::remove_file(dir.join("footer.json")).unwrap();
        assert!(!doc_store_opens_lazily(&dir, Some(&stale)));
        assert!(!doc_store_offsets_plausible(&dir, Some(&stale)));
        let err = SegmentReader::open_with_footer_options(dir.clone(), false, Some(stale), None)
            .err()
            .expect("cannot open: sidecar rejected and doc_store.bin absent")
            .to_string();
        assert!(err.contains("doc_store.bin"), "{err}");
        assert!(err.contains("not present locally"), "{err}");
        let _ = fs::remove_dir_all(&dir);
    }

    /// A sidecar that really is wrong stays rejected even when footer.json
    /// agrees with the supplied footer, and a header-only check cannot see
    /// a mid-record truncation (the full check can).
    #[test]
    fn genuinely_bad_sidecar_is_rejected() {
        let (dir, footer) = three_doc_segment("bad");
        let offsets = fs::read(dir.join("doc_store.offsets")).unwrap();

        let mut wrong_count = offsets.clone();
        wrong_count[..4].copy_from_slice(&9u32.to_le_bytes());
        fs::write(dir.join("doc_store.offsets"), &wrong_count).unwrap();
        assert!(!doc_store_opens_lazily(&dir, Some(&footer)));
        assert!(!doc_store_offsets_plausible(&dir, Some(&footer)));
        assert!(!doc_store_offsets_plausible(&dir, None));
        let reader =
            SegmentReader::open_with_footer_options(dir.clone(), false, Some(footer.clone()), None)
                .unwrap();
        assert!(
            matches!(reader.doc_store, DocStoreAccess::Eager(_)),
            "doc_store.bin is local, so the eager fallback works"
        );

        let mut truncated = offsets.clone();
        truncated.truncate(offsets.len() - 3);
        fs::write(dir.join("doc_store.offsets"), &truncated).unwrap();
        assert!(!doc_store_opens_lazily(&dir, Some(&footer)));
        assert!(doc_store_offsets_plausible(&dir, Some(&footer)));

        fs::remove_file(dir.join("doc_store.offsets")).unwrap();
        assert!(!doc_store_opens_lazily(&dir, Some(&footer)));
        assert!(!doc_store_offsets_plausible(&dir, Some(&footer)));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rewrite_filter_blooms_updates_legacy_footer() {
        let dir = std::env::temp_dir().join("kosha-test-seg-rewrite-blooms");
        let _ = fs::remove_dir_all(&dir);
        let mut w = SegmentWriter::new(SegmentId("test".into()), dir.clone());
        w.add_document(
            DocumentId("d1".into()),
            vec![
                Field::text("content", "hello"),
                Field::text("matterId", "m42"),
            ],
        );
        w.finalize(Bm25Params::default()).unwrap();

        // Simulate a legacy footer without blooms.
        let mut footer = SegmentReader::read_footer(&dir).unwrap();
        footer.filter_blooms = None;
        fs::write(
            dir.join("footer.json"),
            serde_json::to_string_pretty(&footer).unwrap(),
        )
        .unwrap();
        assert!(SegmentReader::read_footer(&dir)
            .unwrap()
            .filter_blooms
            .is_none());

        let rewritten = SegmentReader::rewrite_filter_blooms(&dir).unwrap();
        let blooms = rewritten.filter_blooms.expect("blooms rebuilt");
        assert!(blooms.get("matterId").unwrap().may_contain("m42"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn backfill_offset_tables_upgrades_legacy_segment() {
        let dir = std::env::temp_dir().join("kosha-test-seg-backfill-offsets");
        let _ = fs::remove_dir_all(&dir);
        let mut w = SegmentWriter::new(SegmentId("test".into()), dir.clone());
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("content", "hello world")],
        );
        w.add_document(
            DocumentId("d2".into()),
            vec![Field::text("content", "hello moon and stars")],
        );
        w.finalize(Bm25Params::default()).unwrap();

        // Simulate a pre-lazy-loading segment: no sidecar, format_version 0.
        fs::remove_file(dir.join("doc_store.offsets")).unwrap();
        let mut footer = SegmentReader::read_footer(&dir).unwrap();
        footer.format_version = 0;
        fs::write(
            dir.join("footer.json"),
            serde_json::to_string_pretty(&footer).unwrap(),
        )
        .unwrap();

        let updated = SegmentReader::backfill_offset_tables(&dir).unwrap();
        assert_eq!(updated.format_version, kosha_core::SEGMENT_FORMAT_VERSION);
        assert!(dir.join("doc_store.offsets").exists());

        // Reopening now takes the Lazy path, and doc_meta/doc_record_full
        // return the same data as before the backfill.
        let r = SegmentReader::open(dir.clone()).unwrap();
        let d1 = r.doc_meta(0).unwrap();
        assert_eq!(d1.doc_id, "d1");
        let d2_full = r.doc_record_full(1).unwrap().unwrap();
        assert_eq!(d2_full.doc_id.0, "d2");
        assert_eq!(d2_full.fields[0].value, "hello moon and stars");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn compute_aggregations_works() {
        let mut store = FilterStore::default();
        store.string_fields.insert(
            "documentId".to_string(),
            vec![
                (0, "d1".into()),
                (1, "d2".into()),
                (2, "d1".into()),
                (3, "d3".into()),
                (4, "d1".into()),
            ],
        );
        let result = compute_aggregations(&store, 5, "documentId");
        let per_doc = result.per_document.unwrap();
        assert_eq!(per_doc.buckets.len(), 3);
        // d1 appears 3 times, d2 once, d3 once
        assert_eq!(per_doc.buckets[0].key, "d1");
        assert_eq!(per_doc.buckets[0].doc_count, 3);
    }

    /// Regression test for the staging bug: a search thread calling
    /// `read_footer` while `rewrite_term_bloom` is mid-rewrite of the same
    /// `footer.json` must never observe a truncated file. Before the
    /// `atomic_write` fix (plain `fs::write`, which truncates then fills),
    /// this reproduced `serde_json` failing with "EOF while parsing a
    /// value" under load. With rename-based atomic replace, every read sees
    /// either the fully-old or fully-new file — asserted here by requiring
    /// every single read across many racing iterations to succeed.
    #[test]
    fn concurrent_footer_rewrite_never_yields_truncated_read() {
        let dir = std::env::temp_dir().join("kosha-test-seg-atomic-footer-race");
        let _ = fs::remove_dir_all(&dir);
        let seg_id = SegmentId("test".into());
        let mut w = SegmentWriter::new(seg_id, dir.clone());
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("t", "quick brown fox")],
        );
        w.finalize(Bm25Params::default()).unwrap();

        let writer_dir = dir.clone();
        let writer = std::thread::spawn(move || {
            for _ in 0..200 {
                SegmentReader::rewrite_term_bloom(&writer_dir).unwrap();
            }
        });

        let reader_dir = dir.clone();
        let reader = std::thread::spawn(move || {
            let mut reads = 0usize;
            while reads < 200 {
                // A racing read must always parse cleanly — either the
                // pre-rewrite or post-rewrite footer, never a partial one.
                SegmentReader::read_footer(&reader_dir)
                    .expect("footer read raced a rewrite and saw a truncated file");
                reads += 1;
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();

        // atomic_write's temp files must never linger after a successful run.
        let leftover_tmp: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(
            leftover_tmp.is_empty(),
            "leftover temp files after atomic_write: {leftover_tmp:?}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Integration-level regression test for the staging incident:
    /// `SegmentWriter::finalize()` writes `doc_store.bin`/`inverted.idx`/
    /// `filters.bin`/`footer.json` through `LocalStorage::write`
    /// (`kosha-core`). If a second `finalize()` targets the same segment
    /// directory concurrently with a reader — the scenario staging hit via
    /// a segment-id collision under rapid small flushes — a non-atomic
    /// write there is exactly how `paragraph_index_hnsw` ended up with
    /// permanently 0-byte `doc_store.bin`/`footer.json` in already-published
    /// segments (that corruption then got faithfully copied to S3 by
    /// whatever synced the segment afterward). This exercises the real
    /// multi-file `finalize()` write sequence end to end, not just a single
    /// file, complementing the lower-level `kosha-core` write/read test.
    #[test]
    fn concurrent_segment_finalize_never_yields_truncated_read() {
        let dir = std::env::temp_dir().join("kosha-test-seg-finalize-race");
        let _ = fs::remove_dir_all(&dir);

        // Publish an initial, fully-valid segment at this path so the
        // reader thread always has something parseable from the start.
        let seg_id = SegmentId("test".into());
        let mut w = SegmentWriter::new(seg_id, dir.clone());
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("t", "quick brown fox")],
        );
        w.finalize(Bm25Params::default()).unwrap();

        let writer_dir = dir.clone();
        let writer = std::thread::spawn(move || {
            for i in 0..50 {
                let mut w = SegmentWriter::new(SegmentId("test".into()), writer_dir.clone());
                w.add_document(
                    DocumentId(format!("d{i}")),
                    vec![Field::text("t", "lazy dog jumps over the fence")],
                );
                w.finalize(Bm25Params::default()).unwrap();
            }
        });
        let reader_dir = dir.clone();
        let reader = std::thread::spawn(move || {
            for _ in 0..50 {
                // A concurrent finalize() rewriting this same directory's
                // files must never leave open() looking at a torn file.
                SegmentReader::open(reader_dir.clone())
                    .expect("segment open raced a concurrent finalize() and saw a torn file");
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();

        let _ = fs::remove_dir_all(&dir);
    }

    /// Write a v4 (varint-delta, no skip table) split index for `index` —
    /// the format the production writer emitted before v5 — so fallback
    /// behavior stays covered now that fresh segments are always v5.
    fn write_varint_split_inverted(dir: &Path, index: &HashMap<String, Vec<Posting>>) {
        let mut terms: Vec<&String> = index.keys().collect();
        terms.sort();

        let mut pool: Vec<u8> = Vec::new();
        let mut posting_blobs: Vec<Vec<u8>> =
            (0..POSTING_BLOB_SHARDS).map(|_| Vec::new()).collect();
        let mut entries: Vec<(u64, u32, u32, u64, u32)> = Vec::with_capacity(terms.len());
        for term_str in &terms {
            let postings = &index[*term_str];
            let term_off = pool.len() as u64;
            pool.extend_from_slice(term_str.as_bytes());
            let blob_id = posting_blob_id_for_term(term_str) as u32;
            let blob = &mut posting_blobs[blob_id as usize];
            let p_off = blob.len() as u64;
            write_postings_varint_delta(blob, postings);
            let p_len = (blob.len() as u64 - p_off) as u32;
            entries.push((term_off, term_str.len() as u32, blob_id, p_off, p_len));
        }

        let table_len = entries.len() as u64 * SPLIT_INVERTED_TABLE_ENTRY_LEN as u64;
        let pool_base = INVERTED_HEADER_LEN as u64 + table_len;
        let mut buf = Vec::with_capacity(pool_base as usize + pool.len());
        buf.extend_from_slice(&INVERTED_MAGIC.to_le_bytes());
        buf.extend_from_slice(&SPLIT_INVERTED_VERSION.to_le_bytes());
        buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(POSTING_BLOB_SHARDS as u32).to_le_bytes());
        for (term_off, term_len, blob_id, p_off, p_len) in entries {
            buf.extend_from_slice(&(pool_base + term_off).to_le_bytes());
            buf.extend_from_slice(&term_len.to_le_bytes());
            buf.extend_from_slice(&blob_id.to_le_bytes());
            buf.extend_from_slice(&p_off.to_le_bytes());
            buf.extend_from_slice(&p_len.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
        buf.extend_from_slice(&pool);
        fs::write(dir.join("inverted.idx"), maybe_compress_inverted(&buf)).unwrap();
        for (blob_id, blob) in posting_blobs.into_iter().enumerate() {
            if !blob.is_empty() {
                fs::write(
                    dir.join(posting_blob_file_for_id(blob_id)),
                    maybe_compress_inverted(&blob),
                )
                .unwrap();
            }
        }
    }

    /// Multi-block postings fixture: `n` postings with irregular doc_id
    /// gaps, varied tfs, and a mix of empty and non-empty position lists.
    fn mk_blocked_postings(n: usize) -> Vec<Posting> {
        (0..n)
            .map(|i| Posting {
                doc_id: (i * 3 + (i % 7)) as u32,
                term_frequency: (i % 13 + 1) as u32,
                positions: if i % 4 == 0 {
                    Vec::new()
                } else {
                    (0..(i % 5)).map(|j| (j * 2 + i % 3) as u32).collect()
                },
            })
            .collect()
    }

    #[test]
    fn skip_split_span_round_trips_scoring_blocks_and_positions() {
        // 300 postings → 3 blocks (128/128/44). The span must reproduce,
        // per block: the exact (doc_id, tf) pairs, write-time skip
        // summaries matching a brute-force pass, and a full decode
        // identical to the input (positions included).
        let postings = mk_blocked_postings(300);
        let doc_records: Vec<DocRecord> = (0..1000)
            .map(|i| DocRecord {
                doc_id: DocumentId(format!("d{i}")),
                doc_seq: i as u32,
                field_length: (i % 97 + 5) as u32,
                fields: Vec::new(),
            })
            .collect();
        let mut span = Vec::new();
        write_postings_skip_split(
            &mut span,
            &postings,
            &doc_records
                .iter()
                .map(|r| r.field_length)
                .collect::<Vec<_>>(),
        );
        let parsed = BlockedTermPostings::parse(Arc::new(span)).expect("span must parse");

        assert_eq!(parsed.doc_count(), 300);
        assert_eq!(parsed.block_count(), 3);

        let mut buf = Vec::new();
        for (b, chunk) in postings.chunks(SCORING_BLOCK_LEN).enumerate() {
            let summary = *parsed.summary(b);
            assert_eq!(summary.last_doc_id, chunk.last().unwrap().doc_id);
            assert_eq!(
                summary.max_tf,
                chunk.iter().map(|p| p.term_frequency).max().unwrap()
            );
            assert_eq!(
                summary.min_field_length,
                chunk
                    .iter()
                    .map(|p| doc_records[p.doc_id as usize].field_length)
                    .min()
                    .unwrap()
            );
            assert!(parsed.read_block(b, &mut buf), "block {b} must decode");
            let expected: Vec<ScoringPosting> = chunk
                .iter()
                .map(|p| ScoringPosting {
                    doc_id: p.doc_id,
                    term_frequency: p.term_frequency,
                })
                .collect();
            assert_eq!(buf, expected, "block {b} scoring mismatch");
        }

        let (full, _) = parsed.decode_full().expect("full decode");
        assert_eq!(full, postings, "positions must survive the round trip");

        // find_block: exact hits, between-blocks targets, and past-the-end.
        assert_eq!(parsed.find_block(0, 0), Some(0));
        let b1_first = postings[SCORING_BLOCK_LEN].doc_id;
        assert_eq!(parsed.find_block(b1_first, 0), Some(1));
        assert_eq!(parsed.find_block(b1_first, 2), Some(2));
        let last = postings.last().unwrap().doc_id;
        assert_eq!(parsed.find_block(last, 0), Some(2));
        assert_eq!(parsed.find_block(last + 1, 0), None);
    }

    #[test]
    fn skip_split_truncated_span_fails_closed() {
        // Every truncation point must yield a clean parse failure or a
        // clean block/full decode failure — never a panic, never data from
        // past the span.
        let postings = mk_blocked_postings(200);
        let doc_records: Vec<DocRecord> = (0..700)
            .map(|i| DocRecord {
                doc_id: DocumentId(format!("d{i}")),
                doc_seq: i as u32,
                field_length: 10,
                fields: Vec::new(),
            })
            .collect();
        let mut span = Vec::new();
        write_postings_skip_split(
            &mut span,
            &postings,
            &doc_records
                .iter()
                .map(|r| r.field_length)
                .collect::<Vec<_>>(),
        );

        for cut in 0..span.len() {
            let Some(parsed) = BlockedTermPostings::parse(Arc::new(span[..cut].to_vec())) else {
                continue; // clean parse rejection
            };
            // Parse can succeed on a truncated scoring/positions section —
            // block reads and full decode must then fail closed.
            let mut buf = Vec::new();
            for b in 0..parsed.block_count() {
                let _ = parsed.read_block(b, &mut buf); // must not panic
            }
            let _ = parsed.decode_full(); // must not panic
        }
    }

    #[test]
    fn varint_split_v4_still_opens_and_falls_back_to_decoded_scoring() {
        // Segments written by the previous (v4) writer: full postings
        // still decode, and the scoring API degrades to the Decoded
        // variant instead of pretending there's a skip table.
        let dir = std::env::temp_dir().join("kosha-test-inverted-v4-fallback");
        let expected = write_inverted_fixture(&dir);
        write_varint_split_inverted(&dir, &expected);

        let r = SegmentReader::open(dir.clone()).unwrap();
        for (term, postings) in &expected {
            let got = r
                .postings(term)
                .unwrap_or_else(|| panic!("missing term {term}"));
            assert_eq!(&*got, postings.as_slice(), "postings mismatch for {term}");
        }
        let terms: Vec<String> = expected.keys().cloned().collect();
        let scoring = r.scoring_postings_for_terms(&terms);
        assert_eq!(scoring.len(), terms.len());
        for (term, s) in scoring {
            match s {
                ScoringPostingsRef::Decoded(p) => {
                    assert_eq!(&*p, expected[term].as_slice());
                }
                ScoringPostingsRef::Blocked(_) => {
                    panic!("v4 segment must not hand out skip-table views")
                }
            }
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn skip_split_v5_scoring_is_blocked_and_matches_full_decode() {
        let dir = std::env::temp_dir().join("kosha-test-inverted-v5-scoring");
        let expected = write_inverted_fixture(&dir);

        // The freshly-written segment is v5 — and it must pass the posting
        // -blob hydration gate (the PR #83 silent-zero-hits regression
        // class: a split version missing from the gate never hydrates its
        // blobs on cold reads).
        assert!(
            inverted_uses_posting_blobs(&dir),
            "v5 must be recognized by the hydration gate"
        );

        let r = SegmentReader::open(dir.clone()).unwrap();
        let terms: Vec<String> = expected.keys().cloned().collect();
        let mut buf = Vec::new();
        for (term, s) in r.scoring_postings_for_terms(&terms) {
            let ScoringPostingsRef::Blocked(blocked) = s else {
                panic!("v5 segment must hand out skip-table views")
            };
            let full = &expected[term];
            assert_eq!(blocked.doc_count(), full.len());
            let mut flat: Vec<ScoringPosting> = Vec::new();
            for b in 0..blocked.block_count() {
                assert!(blocked.read_block(b, &mut buf));
                flat.extend_from_slice(&buf);
            }
            let expected_flat: Vec<ScoringPosting> = full
                .iter()
                .map(|p| ScoringPosting {
                    doc_id: p.doc_id,
                    term_frequency: p.term_frequency,
                })
                .collect();
            assert_eq!(flat, expected_flat, "scoring stream mismatch for {term}");
        }
        let _ = fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod doc_vectors_tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kosha-docvec-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn vector_field(name: &str, values: &[f32]) -> Field {
        Field {
            name: name.into(),
            field_type: FieldType::Vector,
            value: serde_json::to_string(values).unwrap(),
        }
    }

    fn write(dir: &Path, mode: VectorStoreMode, docs: Vec<(&str, Vec<Field>)>) -> Footer {
        let mut writer = SegmentWriter::new(SegmentId("s".into()), dir.to_path_buf())
            .with_vector_store_mode(mode);
        for (id, fields) in docs {
            writer.add_document(DocumentId(id.into()), fields);
        }
        writer.finalize(Bm25Params::default()).unwrap()
    }

    fn parsed(value: &str) -> Vec<f32> {
        serde_json::from_str(value).unwrap()
    }

    #[test]
    fn sidecar_round_trips_vectors_and_keeps_field_order() {
        let dir = scratch("roundtrip");
        // Not representable exactly in decimal: the f32 must survive.
        let a = vec![0.1f32, -1.25, 3.0e-7, f32::MAX];
        let b = vec![2.5f32, 0.0, -0.333_333_34, 1.0];
        let footer = write(
            &dir,
            VectorStoreMode::Sidecar,
            vec![
                // One vector field per doc: several per doc don't build a
                // vector.idx today in either mode (#204).
                (
                    "d0",
                    vec![
                        Field::text("title", "first doc"),
                        vector_field("emb", &a),
                        Field::keyword("tag", "x"),
                    ],
                ),
                ("d1", vec![Field::text("title", "no vectors here")]),
                (
                    "d2",
                    vec![vector_field("emb", &b), Field::text("title", "third")],
                ),
            ],
        );
        assert!(footer.doc_vectors);
        assert!(dir.join(DOC_VECTORS_FILE).exists());
        let stored = fs::read_to_string(dir.join("footer.json")).unwrap();
        assert!(stored.contains("\"doc_vectors\": true"), "{stored}");

        let reader = SegmentReader::open(dir.clone()).unwrap();
        let d0 = reader.doc_record_full(0).unwrap().unwrap();
        let names: Vec<&str> = d0.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["title", "emb", "tag"], "field order preserved");
        assert_eq!(parsed(&d0.fields[1].value), a);
        assert_eq!(d0.fields[1].field_type, FieldType::Vector);

        let d1 = reader.doc_record_full(1).unwrap().unwrap();
        assert_eq!(d1.fields.len(), 1);
        let d2 = reader.doc_record_full(2).unwrap().unwrap();
        assert_eq!(parsed(&d2.fields[0].value), b);

        // Every path that reads records sees full vectors, compaction's
        // `iter_doc_records` included.
        let all: Vec<DocRecord> = reader.iter_doc_records().map(|r| r.unwrap()).collect();
        assert_eq!(parsed(&all[2].fields[0].value), b);

        // The ranged-read path parses raw doc-store bytes.
        let (offset, len) = reader.doc_span(0).unwrap();
        let raw = fs::read(dir.join("doc_store.bin")).unwrap();
        let from_bytes = reader
            .doc_record_from_bytes(0, &raw[offset as usize..offset as usize + len as usize])
            .unwrap()
            .unwrap();
        assert_eq!(parsed(&from_bytes.fields[1].value), a);

        // And kNN still works: vector.idx is built from the same vectors.
        let hits = reader
            .knn_search(&b, 3, 8)
            .expect("segment has vectors")
            .unwrap();
        assert!(hits.iter().any(|&(seq, _)| seq == 2), "{hits:?}");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn doc_store_holds_placeholders_not_vector_json() {
        let dir = scratch("placeholder");
        write(
            &dir,
            VectorStoreMode::Sidecar,
            vec![("d0", vec![vector_field("emb", &[0.123_456_79, 9.5])])],
        );
        let raw = fs::read(dir.join("doc_store.bin")).unwrap();
        let text = String::from_utf8_lossy(&raw);
        assert!(
            !text.contains("0.12345679"),
            "vector JSON left in doc store"
        );
        let mut cursor = &raw[4..];
        let rec = parse_one_doc_record(&mut cursor, 0).unwrap();
        assert_eq!(rec.fields[0].name, "emb");
        assert_eq!(rec.fields[0].value, "", "placeholder");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn inline_mode_keeps_pre_243_layout() {
        let dir = scratch("inline");
        let json = "[0.5,1.5]";
        let footer = write(
            &dir,
            VectorStoreMode::Inline,
            vec![(
                "d0",
                vec![Field {
                    name: "emb".into(),
                    field_type: FieldType::Vector,
                    value: json.into(),
                }],
            )],
        );
        assert!(!footer.doc_vectors);
        assert!(!dir.join(DOC_VECTORS_FILE).exists());
        assert!(!fs::read_to_string(dir.join("footer.json"))
            .unwrap()
            .contains("doc_vectors"));
        let reader = SegmentReader::open(dir.clone()).unwrap();
        assert_eq!(
            reader.doc_record_full(0).unwrap().unwrap().fields[0].value,
            json
        );
        fs::remove_dir_all(dir).unwrap();
    }

    /// Unparseable vector JSON is never indexed, but stays inline verbatim
    /// so the stored document is lossless, next to moved vectors.
    #[test]
    fn unparseable_vectors_stay_inline() {
        let dir = scratch("odd");
        let bad = Field {
            name: "emb".into(),
            field_type: FieldType::Vector,
            value: "not a vector".into(),
        };
        let footer = write(
            &dir,
            VectorStoreMode::Sidecar,
            vec![
                ("bad", vec![bad]),
                ("good", vec![vector_field("emb", &[1.0, 2.0])]),
            ],
        );
        assert!(footer.doc_vectors);
        let reader = SegmentReader::open(dir.clone()).unwrap();
        assert_eq!(
            reader.doc_record_full(0).unwrap().unwrap().fields[0].value,
            "not a vector"
        );
        assert_eq!(
            parsed(&reader.doc_record_full(1).unwrap().unwrap().fields[0].value),
            vec![1.0, 2.0]
        );
        fs::remove_dir_all(dir).unwrap();
    }

    /// A sidecar segment without its sidecar must fail loudly, never hand
    /// out empty vector values a compaction would then write back out.
    #[test]
    fn missing_sidecar_is_an_error_and_not_local() {
        let dir = scratch("missing");
        write(
            &dir,
            VectorStoreMode::Sidecar,
            vec![("d0", vec![vector_field("emb", &[1.0, 2.0])])],
        );
        let sidecar = fs::read(dir.join(DOC_VECTORS_FILE)).unwrap();
        fs::remove_file(dir.join(DOC_VECTORS_FILE)).unwrap();
        let reader = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        assert!(!reader.has_local_doc_store());
        let err = reader.doc_record_full(0).unwrap_err().to_string();
        assert!(err.contains(DOC_VECTORS_FILE), "{err}");
        assert!(reader.iter_doc_records().next().unwrap().is_err());

        // Hydrated later (the server fetches it on demand): now it works,
        // with no reopen.
        fs::write(dir.join(DOC_VECTORS_FILE), &sidecar).unwrap();
        assert!(reader.has_local_doc_store());
        let rec = reader.doc_record_full(0).unwrap().unwrap();
        assert_eq!(parsed(&rec.fields[0].value), vec![1.0, 2.0]);
        fs::remove_dir_all(dir).unwrap();
    }

    /// #245: with neither whole file local, a reader serves documents from
    /// the sparse span cache — records from `doc_store.bin.spans`, vectors
    /// via a header span plus the doc's vector span — and `doc_is_local`
    /// reports exactly which documents are covered.
    #[test]
    fn documents_are_served_from_the_span_cache() {
        let dir = scratch("spans");
        write(
            &dir,
            VectorStoreMode::Sidecar,
            (0..5)
                .map(|i| {
                    (
                        ["d0", "d1", "d2", "d3", "d4"][i],
                        vec![
                            Field::text("t", format!("text {i}")),
                            vector_field("emb", &[i as f32, 0.5]),
                        ],
                    )
                })
                .collect(),
        );
        let ds = fs::read(dir.join(DOC_STORE_FILE)).unwrap();
        let dv = fs::read(dir.join(DOC_VECTORS_FILE)).unwrap();
        fs::remove_file(dir.join(DOC_STORE_FILE)).unwrap();
        fs::remove_file(dir.join(DOC_VECTORS_FILE)).unwrap();
        let reader = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        assert!(!reader.has_local_doc_store());
        assert!(!reader.doc_is_local(2));
        assert!(!reader.has_local_doc_vectors_header());
        assert!(reader.doc_vectors_span(2).is_err());
        assert!(reader.doc_record_full(2).is_err());

        let put = |file: &str, s: u64, e: u64, src: &[u8]| {
            let d = span_cache::spans_dir(&dir, file);
            fs::create_dir_all(&d).unwrap();
            fs::write(
                d.join(span_cache::span_file_name(s, e)),
                &src[s as usize..e as usize],
            )
            .unwrap();
        };
        // The header length is discoverable from a short prefix.
        let header_len = match doc_vectors_header_len(&dv[..10]).unwrap() {
            Err(need) => {
                assert!(need > 10);
                doc_vectors_header_len(&dv).unwrap().unwrap()
            }
            Ok(len) => len,
        };
        assert_eq!(header_len, 8 + 4 + 3 + 4 + 5 * 12);
        put(DOC_VECTORS_FILE, 0, header_len, &dv);
        assert!(reader.has_local_doc_vectors_header());
        let (vs, ve) = reader.doc_vectors_span(2).unwrap().unwrap();
        assert_eq!(ve - vs, 8);
        let (off, len) = reader.doc_span(2).unwrap();
        put(DOC_STORE_FILE, off, off + len as u64, &ds);
        assert!(!reader.doc_is_local(2), "vector span still missing");
        put(DOC_VECTORS_FILE, vs, ve, &dv);
        assert!(reader.doc_is_local(2));
        assert!(!reader.doc_is_local(3));

        let rec = reader.doc_record_full(2).unwrap().unwrap();
        assert_eq!(rec.fields[0].value, "text 2");
        let v: Vec<f32> = serde_json::from_str(&rec.fields[1].value).unwrap();
        assert_eq!(v, vec![2.0, 0.5]);
        assert!(reader.doc_record_full(3).is_err());

        // Whole files win once they land, spans or not.
        fs::write(dir.join(DOC_STORE_FILE), &ds).unwrap();
        fs::write(dir.join(DOC_VECTORS_FILE), &dv).unwrap();
        assert!(reader.doc_is_local(3));
        let rec = reader.doc_record_full(3).unwrap().unwrap();
        assert_eq!(rec.fields[0].value, "text 3");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn header_parse_from_prefix_matches_open() {
        let dir = scratch("prefix");
        write(
            &dir,
            VectorStoreMode::Sidecar,
            vec![("d0", vec![vector_field("emb", &[1.0, 2.0, 3.0])])],
        );
        let bytes = fs::read(dir.join(DOC_VECTORS_FILE)).unwrap();
        let whole = DocVectors::open(dir.join(DOC_VECTORS_FILE)).unwrap();
        let from_prefix = DocVectors::from_prefix(dir.clone(), &bytes[..32])
            .unwrap()
            .unwrap();
        assert_eq!(whole.doc_span(0), from_prefix.doc_span(0));
        assert_eq!(whole.file_len(), bytes.len() as u64);
        assert_eq!(from_prefix.file_len(), bytes.len() as u64);
        assert!(DocVectors::from_prefix(dir.clone(), &bytes[..9])
            .unwrap()
            .is_none());
        assert!(DocVectors::from_prefix(dir.clone(), b"XXXXXXXXXXXX").is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    /// The batch reader behind exact filtered kNN (#252): only the named
    /// field, only the asked-for documents, in order — across both
    /// coalesced runs and gaps wide enough to seek over.
    #[test]
    fn visit_field_vectors_reads_one_field_of_the_wanted_documents() {
        let dir = scratch("visit");
        // 3000 docs x 2 fields x 8 dims x 4 B = 192 KB: a gap of 100
        // documents is 6.4 KB (read through), one of 2000 is 128 KB (seek).
        let docs: Vec<(String, Vec<Field>)> = (0..3000)
            .map(|i| {
                let a: Vec<f32> = (0..8).map(|d| (i * 8 + d) as f32).collect();
                let b: Vec<f32> = a.iter().map(|x| -x).collect();
                (
                    format!("d{i}"),
                    vec![vector_field("a", &a), vector_field("b", &b)],
                )
            })
            .collect();
        write(
            &dir,
            VectorStoreMode::Sidecar,
            docs.iter()
                .map(|(id, f)| (id.as_str(), f.clone()))
                .collect(),
        );
        let dv = DocVectors::open(dir.join(DOC_VECTORS_FILE)).unwrap();
        assert!(dv.has_field("b"));
        assert!(!dv.has_field("c"));
        let wanted = [0u32, 1, 100, 200, 2200, 2999, 5000];
        let mut seen = Vec::new();
        let n = dv
            .visit_field_vectors("b", &wanted, |doc_seq, v| seen.push((doc_seq, v.to_vec())))
            .unwrap();
        assert_eq!(n, 6, "doc_seq 5000 has no vector");
        let expected: Vec<(u32, Vec<f32>)> = wanted[..6]
            .iter()
            .map(|&i| (i, (0..8).map(|d| -((i * 8 + d) as f32)).collect()))
            .collect();
        assert_eq!(seen, expected);
        assert_eq!(dv.visit_field_vectors("c", &wanted, |_, _| {}).unwrap(), 0);
        assert_eq!(dv.visit_field_vectors("a", &[], |_, _| {}).unwrap(), 0);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn truncated_or_garbled_sidecar_is_rejected() {
        let dir = scratch("corrupt");
        write(
            &dir,
            VectorStoreMode::Sidecar,
            vec![("d0", vec![vector_field("emb", &[1.0, 2.0, 3.0])])],
        );
        let path = dir.join(DOC_VECTORS_FILE);
        let good = fs::read(&path).unwrap();
        for bad in [
            good[..good.len() - 1].to_vec(),
            good[..6].to_vec(),
            [b"XXXX".as_slice(), &good[4..]].concat(),
        ] {
            fs::write(&path, &bad).unwrap();
            assert!(DocVectors::open(path.clone()).is_err());
            let reader = SegmentReader::open_with_options(dir.clone(), false).unwrap();
            assert!(reader.doc_record_full(0).is_err());
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn parse_vector_store_mode() {
        assert_eq!(VectorStoreMode::parse(None), VectorStoreMode::Sidecar);
        assert_eq!(VectorStoreMode::parse(Some("")), VectorStoreMode::Sidecar);
        assert_eq!(
            VectorStoreMode::parse(Some("INLINE")),
            VectorStoreMode::Inline
        );
        assert_eq!(
            VectorStoreMode::parse(Some(" sidecar ")),
            VectorStoreMode::Sidecar
        );
        assert_eq!(
            VectorStoreMode::parse(Some("bogus")),
            VectorStoreMode::Sidecar
        );
    }

    /// Acceptance criterion from #243: the doc store shrinks by the vector
    /// JSON share. 1024-dim embeddings plus a short text field.
    #[test]
    fn sidecar_removes_the_vector_json_share_of_the_doc_store() {
        let docs = |n: usize| -> Vec<(String, Vec<Field>)> {
            (0..n)
                .map(|i| {
                    let v: Vec<f32> = (0..1024)
                        .map(|d| ((i * 1024 + d) as f32 * 0.618_034).sin() * 0.05)
                        .collect();
                    (
                        format!("doc{i}"),
                        vec![
                            Field::text("title", format!("paragraph {i} about something")),
                            vector_field("embedding", &v),
                        ],
                    )
                })
                .collect()
        };
        let size = |mode: VectorStoreMode| -> (u64, u64) {
            let dir = scratch("size");
            let d = docs(200);
            write(
                &dir,
                mode,
                d.iter().map(|(id, f)| (id.as_str(), f.clone())).collect(),
            );
            let ds = fs::metadata(dir.join("doc_store.bin")).unwrap().len();
            let dv = fs::metadata(dir.join(DOC_VECTORS_FILE)).map_or(0, |m| m.len());
            fs::remove_dir_all(dir).unwrap();
            (ds, dv)
        };
        let (inline_ds, _) = size(VectorStoreMode::Inline);
        let (sidecar_ds, sidecar_dv) = size(VectorStoreMode::Sidecar);
        eprintln!(
            "doc_store.bin inline={inline_ds} sidecar={sidecar_ds} doc_vectors.bin={sidecar_dv}"
        );
        assert!(
            sidecar_ds * 20 < inline_ds,
            "doc store should drop by >95%: {inline_ds} -> {sidecar_ds}"
        );
        assert!(
            sidecar_ds + sidecar_dv < inline_ds / 2,
            "binary vectors are smaller than their JSON: {inline_ds} -> {}",
            sidecar_ds + sidecar_dv
        );
    }
}

#[cfg(test)]
mod bounded_build_tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kosha-bounded-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    /// A few thousand docs with a Zipf-ish vocabulary: shared stopwords
    /// (long posting lists across every run), mid-frequency words and
    /// per-doc unique terms, plus filters and a vector. `stop` is the
    /// very-high-df term: in every doc, 1 to 9 times, so its encoded
    /// postings are far bigger than a small build's per-term spill limit.
    fn corpus(n: usize) -> Vec<(DocumentId, Vec<Field>)> {
        (0..n)
            .map(|i| {
                let text = format!(
                    "the quick brown fox number {i} jumps over the lazy dog topic{} \
                     unique{i}term the end word{}{}",
                    i % 37,
                    (i * 7919) % 1013,
                    " stop".repeat(1 + i % 9)
                );
                (
                    DocumentId(format!("doc-{i:05}")),
                    vec![
                        Field::text("body", text),
                        Field::keyword("bucket", format!("b{}", i % 11)),
                        Field {
                            name: "rank".into(),
                            field_type: FieldType::Integer,
                            value: (i % 97).to_string(),
                        },
                        Field {
                            name: "emb".into(),
                            field_type: FieldType::Vector,
                            value: serde_json::to_string(&[
                                (i % 13) as f32,
                                1.0,
                                (i % 5) as f32 * 0.5,
                            ])
                            .unwrap(),
                        },
                    ],
                )
            })
            .collect()
    }

    fn build(
        dir: &Path,
        budget: usize,
        docs: &[(DocumentId, Vec<Field>)],
    ) -> (Footer, usize, usize) {
        build_version(dir, budget, docs, SKIP_SPLIT_INVERTED_VERSION)
    }

    fn build_version(
        dir: &Path,
        budget: usize,
        docs: &[(DocumentId, Vec<Field>)],
        version: u32,
    ) -> (Footer, usize, usize) {
        let mut w = SegmentWriter::new(SegmentId("seg".into()), dir.to_path_buf())
            .with_build_memory_bytes(budget)
            .with_postings_write_version(version);
        for (id, fields) in docs {
            w.add_document(id.clone(), fields.clone());
        }
        let runs = w.spilled_runs();
        let peak = w.peak_inverted_bytes();
        (w.finalize(Bm25Params::default()).unwrap(), runs, peak)
    }

    fn files(dir: &Path) -> Vec<(String, Vec<u8>)> {
        let mut out: Vec<(String, Vec<u8>)> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap())
            .filter(|e| e.file_type().unwrap().is_file())
            .map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                (name, fs::read(e.path()).unwrap())
            })
            .collect();
        out.sort();
        out
    }

    /// The spill path must be invisible in the output: a segment built with
    /// a tiny budget (dozens of runs, and a 4 KiB per-term encoder limit
    /// that the very-high-df `stop` spills past) is byte-for-byte the
    /// segment built fully in memory, footer aside from its timestamp.
    #[test]
    fn spilled_build_is_byte_identical_to_in_memory_build() {
        let docs = corpus(3_000);
        let (a_dir, b_dir) = (scratch("mem"), scratch("spill"));
        let (fa, runs_a, _) = build(&a_dir, usize::MAX, &docs);
        let (fb, runs_b, _) = build(&b_dir, 64 * 1024, &docs);
        assert_eq!(runs_a, 0);
        assert!(runs_b >= 10, "expected many runs, got {runs_b}");
        assert_eq!(term_spill_limit(64 * 1024), 4 * 1024);

        assert_same_files(&a_dir, &b_dir, &fa, &fb);
        assert!(!b_dir.join(BUILD_DIR).exists(), "scratch removed");

        // And it reads back: postings, blooms and stored docs.
        let r = SegmentReader::open(b_dir.clone()).unwrap();
        assert_eq!(r.doc_count(), 3_000);
        let the = r.postings("the").unwrap();
        assert_eq!(the.len(), 3_000);
        assert!(the.windows(2).all(|w| w[0].doc_id < w[1].doc_id));
        assert_eq!(the[42].positions, vec![0, 8, 13]);
        assert_eq!(r.postings("unique1234term").unwrap()[0].doc_id, 1234);
        // The high-df term: every doc, right tf and positions, and its
        // bytes are what the whole-list reference encoder produces.
        let stop = r.postings("stop").unwrap().into_owned();
        assert_eq!(stop.len(), 3_000);
        for (i, p) in stop.iter().enumerate() {
            assert_eq!(p.doc_id, i as u32);
            assert_eq!(p.term_frequency as usize, 1 + i % 9);
            assert_eq!(
                p.positions,
                (16..16 + (1 + i as u32 % 9)).collect::<Vec<_>>()
            );
        }
        let field_lengths: Vec<u32> = r.iter_doc_meta().map(|m| m.field_length).collect();
        let mut reference = Vec::new();
        write_postings_skip_split(&mut reference, &stop, &field_lengths);
        assert!(reference.len() > 3 * term_spill_limit(64 * 1024));
        let blob = read_inverted_payload(
            fs::read(b_dir.join(posting_blob_file_for_term("stop"))).unwrap(),
        )
        .unwrap();
        assert!(
            blob.windows(reference.len()).any(|w| w == reference),
            "streamed `stop` span equals the reference encoding"
        );
        assert!(fb
            .term_bloom
            .as_ref()
            .unwrap()
            .may_contain("unique2999term"));
        let rec = r.doc_record_full(2_999).unwrap().unwrap();
        assert_eq!(rec.doc_id.0, "doc-02999");
        fs::remove_dir_all(a_dir).unwrap();
        fs::remove_dir_all(b_dir).unwrap();
    }

    /// `KOSHA_POSTINGS_WRITE_VERSION=4` streams through the same encoder:
    /// spilled and in-memory v4 builds are identical and read back.
    #[test]
    fn spilled_v4_build_is_byte_identical_to_in_memory_build() {
        let docs = corpus(2_000);
        let (a_dir, b_dir) = (scratch("mem4"), scratch("spill4"));
        let (fa, _, _) = build_version(&a_dir, usize::MAX, &docs, SPLIT_INVERTED_VERSION);
        let (fb, runs_b, _) = build_version(&b_dir, 64 * 1024, &docs, SPLIT_INVERTED_VERSION);
        assert!(runs_b >= 5, "expected several runs, got {runs_b}");
        assert_same_files(&a_dir, &b_dir, &fa, &fb);
        let toc = read_inverted_payload(fs::read(b_dir.join("inverted.idx")).unwrap()).unwrap();
        assert_eq!(toc[4..8], SPLIT_INVERTED_VERSION.to_le_bytes());
        let r = SegmentReader::open(b_dir.clone()).unwrap();
        let stop = r.postings("stop").unwrap().into_owned();
        assert_eq!(stop.len(), 2_000);
        let mut reference = Vec::new();
        write_postings_varint_delta(&mut reference, &stop);
        let blob = read_inverted_payload(
            fs::read(b_dir.join(posting_blob_file_for_term("stop"))).unwrap(),
        )
        .unwrap();
        assert!(blob.windows(reference.len()).any(|w| w == reference));
        fs::remove_dir_all(a_dir).unwrap();
        fs::remove_dir_all(b_dir).unwrap();
    }

    /// The streaming encoder emits exactly the whole-list encoders' bytes,
    /// for both versions, across block boundaries, spills and reuse.
    #[test]
    fn term_encoder_matches_whole_list_encoders() {
        let dir = scratch("encoder");
        fs::create_dir_all(&dir).unwrap();
        let field_lengths: Vec<u32> = (0..5_000u32).map(|i| 3 + (i * 31) % 200).collect();
        let mut x = 7u64;
        let mut rand = move |m: u32| {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((x >> 33) as u32) % m
        };
        // Lengths around the block size, and one in every doc.
        let mut terms: Vec<Vec<Posting>> = Vec::new();
        for n in [1usize, 2, 127, 128, 129, 255, 256, 257, 1_000, 5_000] {
            let mut doc = 0u32;
            let postings = (0..n)
                .map(|k| {
                    doc = if n == 5_000 {
                        k as u32
                    } else {
                        doc + 1 + rand(3)
                    };
                    let tf = 1 + rand(6);
                    let mut pos = 0;
                    let positions = (0..tf)
                        .map(|_| {
                            pos += rand(400);
                            pos
                        })
                        .collect();
                    Posting {
                        doc_id: doc,
                        term_frequency: tf,
                        positions,
                    }
                })
                .collect();
            terms.push(postings);
        }
        for version in [
            SKIP_SPLIT_INVERTED_VERSION,
            SPLIT_INVERTED_VERSION,
            BITPACKED_SKIP_SPLIT_INVERTED_VERSION,
        ] {
            // A 64-byte limit spills nearly every posting.
            let mut encoder = TermEncoder::new(version, &dir, 64);
            for postings in &terms {
                let mut expected = Vec::new();
                if version == SPLIT_INVERTED_VERSION {
                    write_postings_varint_delta(&mut expected, postings);
                } else if version == BITPACKED_SKIP_SPLIT_INVERTED_VERSION {
                    write_postings_skip_split_v6(&mut expected, postings, &field_lengths);
                } else {
                    write_postings_skip_split(&mut expected, postings, &field_lengths);
                }
                for p in postings {
                    encoder
                        .push(p.doc_id, p.term_frequency, &p.positions, &field_lengths)
                        .unwrap();
                }
                let mut got = Vec::new();
                let len = encoder.finish(&mut got).unwrap();
                assert_eq!(len as usize, got.len());
                assert!(
                    got == expected,
                    "v{version}, {} postings: streamed bytes differ",
                    postings.len()
                );
            }
            assert!(encoder.spilled_total() > 0, "the spill files were used");
        }
        fs::remove_dir_all(dir).unwrap();
    }

    /// Sum of all `postings-*.bin` shard bytes in a segment directory.
    fn postings_bytes(dir: &Path) -> u64 {
        files(dir)
            .iter()
            .filter(|(n, _)| n.starts_with("postings"))
            .map(|(_, b)| b.len() as u64)
            .sum()
    }

    /// v6 (#386): spilled and in-memory builds are byte-identical, the TOC
    /// says v6, and every term reads back exactly as from a v5 build of the
    /// same documents — full postings, skip table and every scoring block.
    #[test]
    fn v6_build_reads_back_identically_to_v5() {
        let docs = corpus(3_000);
        let (v5_dir, mem_dir, spill_dir) =
            (scratch("v6ref5"), scratch("v6mem"), scratch("v6spill"));
        build_version(&v5_dir, usize::MAX, &docs, SKIP_SPLIT_INVERTED_VERSION);
        let (fa, _, _) = build_version(
            &mem_dir,
            usize::MAX,
            &docs,
            BITPACKED_SKIP_SPLIT_INVERTED_VERSION,
        );
        let (fb, runs, _) = build_version(
            &spill_dir,
            64 * 1024,
            &docs,
            BITPACKED_SKIP_SPLIT_INVERTED_VERSION,
        );
        assert!(runs >= 5, "expected several runs, got {runs}");
        assert_same_files(&mem_dir, &spill_dir, &fa, &fb);
        let toc = read_inverted_payload(fs::read(mem_dir.join("inverted.idx")).unwrap()).unwrap();
        assert_eq!(
            toc[4..8],
            BITPACKED_SKIP_SPLIT_INVERTED_VERSION.to_le_bytes()
        );

        let r5 = SegmentReader::open(v5_dir.clone()).unwrap();
        let r6 = SegmentReader::open(mem_dir.clone()).unwrap();
        let terms: Vec<String> = r5.all_terms().iter().map(|t| t.to_string()).collect();
        assert_eq!(
            r6.all_terms(),
            terms.iter().map(|t| t.as_str()).collect::<Vec<_>>()
        );
        assert!(r5.postings("stop").unwrap().len() >= 2 * SCORING_BLOCK_LEN);
        for term in &terms {
            assert_eq!(
                &*r5.postings(term).unwrap(),
                &*r6.postings(term).unwrap(),
                "{term}"
            );
        }
        let s5 = r5.scoring_postings_for_terms(&terms);
        let s6 = r6.scoring_postings_for_terms(&terms);
        assert_eq!(s5.len(), terms.len());
        assert_eq!(s6.len(), terms.len());
        let (mut a, mut b) = (Vec::new(), Vec::new());
        let mut full_blocks = 0;
        for ((t5, p5), (t6, p6)) in s5.iter().zip(&s6) {
            assert_eq!(t5, t6);
            let (ScoringPostingsRef::Blocked(p5), ScoringPostingsRef::Blocked(p6)) = (p5, p6)
            else {
                panic!("v5 and v6 must both take the blocked scoring path");
            };
            assert!(!p5.bitpacked && p6.bitpacked);
            assert_eq!(p5.doc_count(), p6.doc_count());
            assert_eq!(p5.block_count(), p6.block_count());
            for blk in 0..p5.block_count() {
                let (x, y) = (p5.summary(blk), p6.summary(blk));
                assert_eq!(
                    (x.last_doc_id, x.max_tf, x.min_field_length),
                    (y.last_doc_id, y.max_tf, y.min_field_length)
                );
                assert!(p5.read_block(blk, &mut a));
                assert!(p6.read_block(blk, &mut b));
                assert_eq!(a, b, "{t5} block {blk}");
                full_blocks += usize::from(a.len() == SCORING_BLOCK_LEN);
            }
        }
        assert!(full_blocks > 10, "corpus must exercise bit-packed blocks");
        let (b5, b6) = (postings_bytes(&v5_dir), postings_bytes(&mem_dir));
        assert!(
            b6 < b5,
            "v6 postings {b6} B should be smaller than v5 {b5} B"
        );
        for d in [v5_dir, mem_dir, spill_dir] {
            fs::remove_dir_all(d).unwrap();
        }
    }

    /// Compaction across versions (#386): re-merging a v6 segment's raw
    /// records with a v5 writer (rollback) and back to v6 (roll forward)
    /// yields the same postings for every term.
    #[test]
    fn v6_segments_compact_to_v5_and_back() {
        let docs = corpus(600);
        let (v6_dir, v5_dir, back_dir) = (scratch("cmp6"), scratch("cmp5"), scratch("cmp6b"));
        build_version(
            &v6_dir,
            usize::MAX,
            &docs,
            BITPACKED_SKIP_SPLIT_INVERTED_VERSION,
        );
        let remerge_to = |src: &Path, dst: &Path, version: u32| {
            let r = SegmentReader::open(src.to_path_buf()).unwrap();
            let mut w = SegmentWriter::new(SegmentId("c".into()), dst.to_path_buf())
                .with_postings_write_version(version);
            for raw in r.iter_doc_records_raw() {
                let raw = raw.unwrap();
                w.add_document_with_vectors(raw.record.doc_id, raw.record.fields, raw.vectors);
            }
            w.finalize(Bm25Params::default()).unwrap();
        };
        remerge_to(&v6_dir, &v5_dir, SKIP_SPLIT_INVERTED_VERSION);
        remerge_to(&v5_dir, &back_dir, BITPACKED_SKIP_SPLIT_INVERTED_VERSION);
        let toc = read_inverted_payload(fs::read(v5_dir.join("inverted.idx")).unwrap()).unwrap();
        assert_eq!(toc[4..8], SKIP_SPLIT_INVERTED_VERSION.to_le_bytes());
        let (r6, r5, rb) = (
            SegmentReader::open(v6_dir.clone()).unwrap(),
            SegmentReader::open(v5_dir.clone()).unwrap(),
            SegmentReader::open(back_dir.clone()).unwrap(),
        );
        let terms = r6.all_terms();
        assert_eq!(terms, r5.all_terms());
        for term in terms {
            let p = r6.postings(term).unwrap();
            assert_eq!(&*p, &*r5.postings(term).unwrap(), "{term}");
            assert_eq!(&*p, &*rb.postings(term).unwrap(), "{term}");
        }
        assert_eq!(postings_bytes(&v6_dir), postings_bytes(&back_dir));
        for d in [v6_dir, v5_dir, back_dir] {
            fs::remove_dir_all(d).unwrap();
        }
    }

    /// `postings_section_stats` (#386 diagnostics): on v5 and v6 builds of
    /// the same documents, the stored scoring bytes equal the re-encoded
    /// size in the segment's own codec, the re-encoded sizes agree across
    /// the two builds, positions are byte-identical in size, and the
    /// sections tile the shards exactly.
    #[test]
    fn postings_section_stats_match_both_codecs() {
        let docs = corpus(1_500);
        let (d5, d6) = (scratch("stats5"), scratch("stats6"));
        build_version(&d5, usize::MAX, &docs, SKIP_SPLIT_INVERTED_VERSION);
        build_version(
            &d6,
            usize::MAX,
            &docs,
            BITPACKED_SKIP_SPLIT_INVERTED_VERSION,
        );
        let s5 = postings_section_stats(&d5).unwrap();
        let s6 = postings_section_stats(&d6).unwrap();
        assert_eq!(s5.inverted_version, SKIP_SPLIT_INVERTED_VERSION);
        assert_eq!(s6.inverted_version, BITPACKED_SKIP_SPLIT_INVERTED_VERSION);
        assert_eq!(s5.scoring_bytes, s5.scoring_bytes_as_v5);
        assert_eq!(s6.scoring_bytes, s6.scoring_bytes_as_v6);
        assert_eq!(s5.scoring_bytes_as_v5, s6.scoring_bytes_as_v5);
        assert_eq!(s5.scoring_bytes_as_v6, s6.scoring_bytes_as_v6);
        assert!(s6.scoring_bytes < s5.scoring_bytes);
        assert_eq!(
            (s5.terms, s5.postings, s5.postings_in_full_blocks),
            (s6.terms, s6.postings, s6.postings_in_full_blocks)
        );
        assert!(s5.postings_in_full_blocks > 0);
        assert_eq!(s5.positions_bytes, s6.positions_bytes);
        for s in [&s5, &s6] {
            assert_eq!(
                s.skip_bytes + s.scoring_bytes + s.positions_bytes,
                s.shard_logical_bytes
            );
        }
        for d in [d5, d6] {
            fs::remove_dir_all(d).unwrap();
        }
    }

    /// Default writer stays v5: v6 is opt-in only.
    #[test]
    fn default_writer_does_not_emit_v6() {
        // The env gate is process-global; only assert when unset.
        if std::env::var_os("KOSHA_POSTINGS_WRITE_VERSION").is_none() {
            assert_eq!(postings_write_version(), SKIP_SPLIT_INVERTED_VERSION);
        }
        let dir = scratch("v6default");
        let docs = corpus(50);
        let mut w = SegmentWriter::new(SegmentId("seg".into()), dir.clone());
        for (id, fields) in &docs {
            w.add_document(id.clone(), fields.clone());
        }
        w.finalize(Bm25Params::default()).unwrap();
        let toc = read_inverted_payload(fs::read(dir.join("inverted.idx")).unwrap()).unwrap();
        assert_eq!(
            toc[4..8],
            postings_write_version().to_le_bytes(),
            "writer follows the env gate"
        );
        fs::remove_dir_all(dir).unwrap();
    }

    /// Corrupting any byte of a v6 span's bit-packed scoring section must
    /// never yield a block whose decode succeeds with different doc ids.
    #[test]
    fn v6_corrupt_scoring_bytes_fail_closed() {
        let postings: Vec<Posting> = (0..300u32)
            .map(|i| Posting {
                doc_id: i * 5 + (i % 3),
                term_frequency: 1 + i % 4,
                positions: vec![i % 7],
            })
            .collect();
        let field_lengths = vec![10u32; 2_000];
        let mut span = Vec::new();
        write_postings_skip_split_v6(&mut span, &postings, &field_lengths);
        let clean = BlockedTermPostings::parse_v6(Arc::new(span.clone())).unwrap();
        let mut reference = Vec::new();
        let mut expect = Vec::new();
        for blk in 0..clean.block_count() {
            assert!(clean.read_block(blk, &mut reference));
            expect.push(reference.clone());
        }
        let scoring = clean.scoring_base as usize..clean.positions_base as usize;
        let mut out = Vec::new();
        for i in scoring {
            for bit in [0u8, 3, 7] {
                let mut bad = span.clone();
                bad[i] ^= 1 << bit;
                let Some(parsed) = BlockedTermPostings::parse_v6(Arc::new(bad)) else {
                    continue;
                };
                for (blk, want) in expect.iter().enumerate() {
                    if parsed.read_block(blk, &mut out) {
                        // A flipped tf bit is undetectable (as in v5) and
                        // only changes a score; doc ids must never move.
                        let got: Vec<u32> = out.iter().map(|p| p.doc_id).collect();
                        let w: Vec<u32> = want.iter().map(|p| p.doc_id).collect();
                        assert_eq!(got, w, "byte {i} bit {bit} block {blk}");
                    }
                }
            }
        }
    }

    fn assert_same_files(a_dir: &Path, b_dir: &Path, fa: &Footer, fb: &Footer) {
        let (a, b) = (files(a_dir), files(b_dir));
        let names: Vec<&str> = a.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, b.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>());
        assert!(names.iter().any(|n| n.starts_with("postings")), "{names:?}");
        for ((name, x), (_, y)) in a.iter().zip(&b) {
            if name == "footer.json" {
                continue;
            }
            assert!(
                x == y,
                "{name} differs between in-memory and spilled builds"
            );
        }
        let mut fa_json = serde_json::to_value(fa).unwrap();
        let mut fb_json = serde_json::to_value(fb).unwrap();
        fa_json["created_at"] = serde_json::Value::Null;
        fb_json["created_at"] = serde_json::Value::Null;
        assert_eq!(fa_json, fb_json);
    }

    /// Tricky f32s: their JSON round trip is exact too, but the raw path
    /// must not depend on that.
    fn vector_docs(n: usize) -> Vec<(DocumentId, Vec<Field>)> {
        (0..n)
            .map(|i| {
                let v = [
                    1.0f32 / 3.0 + i as f32,
                    -0.0,
                    f32::MIN_POSITIVE / 7.0,
                    f32::MAX,
                    std::f32::consts::PI * i as f32,
                ];
                let mut fields = vec![
                    Field::text("body", format!("doc {i} text")),
                    Field::keyword("tag", format!("t{}", i % 3)),
                    Field {
                        name: "emb".into(),
                        field_type: FieldType::Vector,
                        value: serde_json::to_string(&v).unwrap(),
                    },
                ];
                if i % 2 == 0 {
                    fields.push(Field::text("notes", "after the vector"));
                }
                (DocumentId(format!("v{i}")), fields)
            })
            .collect()
    }

    fn write_segment(dir: &Path, mode: VectorStoreMode, docs: &[(DocumentId, Vec<Field>)]) {
        let mut w = SegmentWriter::new(SegmentId("v".into()), dir.to_path_buf())
            .with_vector_store_mode(mode);
        for (id, fields) in docs {
            w.add_document(id.clone(), fields.clone());
        }
        w.finalize(Bm25Params::default()).unwrap();
    }

    /// Re-write `src` through the merge path (`iter_doc_records_raw` +
    /// `add_document_with_vectors`).
    fn remerge(src: &Path, dst: &Path, mode: VectorStoreMode) {
        let r = SegmentReader::open(src.to_path_buf()).unwrap();
        let mut w = SegmentWriter::new(SegmentId("v".into()), dst.to_path_buf())
            .with_vector_store_mode(mode);
        for raw in r.iter_doc_records_raw() {
            let raw = raw.unwrap();
            w.add_document_with_vectors(raw.record.doc_id, raw.record.fields, raw.vectors);
        }
        w.finalize(Bm25Params::default()).unwrap();
    }

    fn vector_bits(dir: &Path) -> Vec<(String, String, Vec<u32>)> {
        let r = SegmentReader::open(dir.to_path_buf()).unwrap();
        let mut out = Vec::new();
        for rec in r.iter_doc_records() {
            let rec = rec.unwrap();
            for f in rec
                .fields
                .iter()
                .filter(|f| f.field_type == FieldType::Vector)
            {
                let v: Vec<f32> = serde_json::from_str(&f.value).unwrap();
                out.push((
                    rec.doc_id.0.clone(),
                    f.name.clone(),
                    v.iter().map(|x| x.to_bits()).collect(),
                ));
            }
        }
        out
    }

    /// The merge path hands sidecar vectors over as f32: a sidecar →
    /// sidecar re-merge reproduces every file, and sidecar ↔ inline
    /// conversions keep the vectors bit-identical.
    #[test]
    fn raw_records_pass_vectors_through_bit_identically() {
        let docs = vector_docs(300);
        let sidecar = scratch("vec-sidecar");
        let inline = scratch("vec-inline");
        write_segment(&sidecar, VectorStoreMode::Sidecar, &docs);
        write_segment(&inline, VectorStoreMode::Inline, &docs);
        let expected = vector_bits(&sidecar);
        assert_eq!(expected.len(), 300);
        assert_eq!(vector_bits(&inline), expected);

        let raw = SegmentReader::open(sidecar.clone()).unwrap();
        let first = raw.iter_doc_records_raw().next().unwrap().unwrap();
        assert!(first.record.fields[2].value.is_empty(), "placeholder kept");
        assert_eq!(first.vectors.len(), 1);
        assert_eq!(first.vectors[0].0, 2, "index into the record's fields");
        let want: Vec<f32> = serde_json::from_str(&docs[0].1[2].value).unwrap();
        assert_eq!(
            first.vectors[0]
                .1
                .iter()
                .map(|x| x.to_bits())
                .collect::<Vec<_>>(),
            want.iter().map(|x| x.to_bits()).collect::<Vec<_>>()
        );

        // sidecar → sidecar: same bytes, file for file.
        let again = scratch("vec-sidecar2");
        remerge(&sidecar, &again, VectorStoreMode::Sidecar);
        let (fa, fb) = (
            SegmentReader::read_footer(&sidecar).unwrap(),
            SegmentReader::read_footer(&again).unwrap(),
        );
        assert_same_files(&sidecar, &again, &fa, &fb);

        // sidecar → inline: the writer fills in the JSON, exactly as
        // `doc_record_full` does, so the doc store matches an inline build.
        let to_inline = scratch("vec-to-inline");
        remerge(&sidecar, &to_inline, VectorStoreMode::Inline);
        assert!(!SegmentReader::read_footer(&to_inline).unwrap().doc_vectors);
        assert_eq!(
            fs::read(to_inline.join("doc_store.bin")).unwrap(),
            fs::read(inline.join("doc_store.bin")).unwrap()
        );
        assert_eq!(vector_bits(&to_inline), expected);

        // inline → sidecar: nothing supplied, parsed from the JSON.
        let to_sidecar = scratch("vec-to-sidecar");
        remerge(&inline, &to_sidecar, VectorStoreMode::Sidecar);
        assert_eq!(
            fs::read(to_sidecar.join(DOC_VECTORS_FILE)).unwrap(),
            fs::read(sidecar.join(DOC_VECTORS_FILE)).unwrap()
        );
        assert_eq!(vector_bits(&to_sidecar), expected);

        for d in [sidecar, inline, again, to_inline, to_sidecar] {
            fs::remove_dir_all(d).unwrap();
        }
    }

    /// A placeholder with no sidecar entry fails the raw read too.
    #[test]
    fn raw_records_reject_a_missing_sidecar_vector() {
        let dir = scratch("vec-missing");
        write_segment(&dir, VectorStoreMode::Sidecar, &vector_docs(3));
        // Rewrite the sidecar with doc 1's entries dropped.
        let mut w = SegmentWriter::new(SegmentId("x".into()), scratch("vec-missing-src"))
            .with_vector_store_mode(VectorStoreMode::Sidecar);
        let docs = vector_docs(3);
        w.add_document(docs[0].0.clone(), docs[0].1.clone());
        w.add_document(docs[2].0.clone(), docs[2].1.clone());
        let other = w.output_dir.clone();
        w.finalize(Bm25Params::default()).unwrap();
        fs::copy(other.join(DOC_VECTORS_FILE), dir.join(DOC_VECTORS_FILE)).unwrap();
        let r = SegmentReader::open(dir.clone()).unwrap();
        let results: Vec<_> = r.iter_doc_records_raw().collect();
        assert!(results
            .iter()
            .any(|r| matches!(r, Err(KoshaError::CorruptSegment(_)))));
        fs::remove_dir_all(dir).unwrap();
        fs::remove_dir_all(other).unwrap();
    }

    /// The in-memory postings never exceed the budget by more than one
    /// document's worth before a spill.
    #[test]
    fn in_memory_postings_stay_within_budget() {
        let docs = corpus(2_000);
        let dir = scratch("budget");
        let budget = 32 * 1024;
        let (_, runs, peak) = build(&dir, budget, &docs);
        assert!(runs > 0);
        assert!(peak <= budget + 4 * 1024, "peak {peak} vs budget {budget}");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn abandoned_build_leaves_nothing_behind() {
        let dir = scratch("abandon");
        {
            let mut w = SegmentWriter::new(SegmentId("seg".into()), dir.clone())
                .with_build_memory_bytes(1024);
            for (id, fields) in corpus(200) {
                w.add_document(id, fields);
            }
            assert!(w.spilled_runs() > 0);
            assert!(dir.join(BUILD_DIR).join("doc_store.bin").exists());
        }
        assert!(!dir.exists(), "partial build removed on drop");
    }

    #[test]
    fn empty_segment_still_writes_empty_doc_store() {
        let dir = scratch("empty");
        let w = SegmentWriter::new(SegmentId("seg".into()), dir.clone());
        let footer = w.finalize(Bm25Params::default()).unwrap();
        assert_eq!(footer.doc_count, 0);
        assert_eq!(
            fs::read(dir.join("doc_store.bin")).unwrap(),
            0u32.to_le_bytes()
        );
        assert!(!dir.join(BUILD_DIR).exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn parse_build_memory_bytes() {
        assert_eq!(
            parse_segment_build_memory_bytes(None),
            DEFAULT_SEGMENT_BUILD_MEMORY_BYTES
        );
        assert_eq!(parse_segment_build_memory_bytes(Some("1048576")), 1_048_576);
        assert_eq!(
            parse_segment_build_memory_bytes(Some("0")),
            DEFAULT_SEGMENT_BUILD_MEMORY_BYTES
        );
        assert_eq!(
            parse_segment_build_memory_bytes(Some("lots")),
            DEFAULT_SEGMENT_BUILD_MEMORY_BYTES
        );
    }
}

#[cfg(test)]
mod posting_span_tests {
    //! Raw posting shards and ranged term reads (#245, part 2).
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kosha-postspan-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    /// 3,000 docs over a 400-word vocabulary: every shard a query term
    /// lands in is well over the 4 KiB compression threshold.
    fn build(dir: &Path, raw: bool) -> Footer {
        let mut w = SegmentWriter::new(SegmentId("s".into()), dir.to_path_buf())
            .with_raw_posting_blobs(raw);
        for i in 0..3000u32 {
            let words: Vec<String> = (0..12)
                .map(|j| format!("w{}", (i * 7 + j * 13) % 400))
                .collect();
            let text = format!("alpha beta {} gamma", words.join(" "));
            w.add_document(
                kosha_core::DocumentId(format!("d{i}")),
                vec![Field::text("t", text)],
            );
        }
        w.finalize(Bm25Params::default()).unwrap()
    }

    fn shard_files(dir: &Path) -> Vec<PathBuf> {
        let mut v: Vec<PathBuf> = fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.is_file()
                    && p.file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with("postings-")
            })
            .collect();
        v.sort();
        v
    }

    fn is_wrapped(bytes: &[u8]) -> bool {
        bytes.len() >= 4 && bytes[0..4] == COMPRESSED_INVERTED_MAGIC.to_le_bytes()
    }

    #[test]
    fn raw_writer_leaves_shards_unwrapped_and_flags_the_footer() {
        let raw_dir = scratch("raw");
        let zst_dir = scratch("zst");
        assert!(build(&raw_dir, true).posting_blobs_raw);
        assert!(!build(&zst_dir, false).posting_blobs_raw);
        assert!(shard_files(&raw_dir)
            .iter()
            .all(|p| !is_wrapped(&fs::read(p).unwrap())));
        assert!(shard_files(&zst_dir)
            .iter()
            .any(|p| is_wrapped(&fs::read(p).unwrap())));
        // The flag round-trips through footer.json, and a footer without it
        // (every pre-#245 segment) reads as `false`.
        let footer = SegmentReader::read_footer(&raw_dir).unwrap();
        assert!(footer.posting_blobs_raw);
        let mut json: serde_json::Value =
            serde_json::from_slice(&fs::read(zst_dir.join("footer.json")).unwrap()).unwrap();
        assert!(
            json.get("posting_blobs_raw").is_none(),
            "omitted when false"
        );
        json.as_object_mut().unwrap().remove("posting_blobs_raw");
        let old: Footer = serde_json::from_value(json).unwrap();
        assert!(!old.posting_blobs_raw);

        // Both read identically when whole (a raw shard is what any reader,
        // old or new, already accepts: the wrapper was always optional).
        let a = SegmentReader::open_with_options(raw_dir.clone(), false).unwrap();
        let b = SegmentReader::open_with_options(zst_dir.clone(), false).unwrap();
        for term in ["alpha", "w7", "w399", "gamma"] {
            let pa: Vec<_> = a.postings(term).unwrap().to_vec();
            let pb: Vec<_> = b.postings(term).unwrap().to_vec();
            assert_eq!(pa, pb, "{term}");
        }
        let raw_size: u64 = shard_files(&raw_dir)
            .iter()
            .map(|p| fs::metadata(p).unwrap().len())
            .sum();
        let zst_size: u64 = shard_files(&zst_dir)
            .iter()
            .map(|p| fs::metadata(p).unwrap().len())
            .sum();
        println!("posting shards: raw {raw_size} bytes, zstd {zst_size} bytes");
        let _ = fs::remove_dir_all(&raw_dir);
        let _ = fs::remove_dir_all(&zst_dir);
    }

    /// Copy just `terms`' spans of every shard into the span cache and
    /// remove the whole shards — the state a ranged cold query leaves.
    fn keep_only_spans(dir: &Path, terms: &[String]) -> u64 {
        let spans = posting_term_spans(dir, terms).unwrap();
        let mut total = 0;
        for s in &spans {
            let shard = fs::read(dir.join(&s.file)).unwrap();
            let d = span_cache::spans_dir(dir, &s.file);
            fs::create_dir_all(&d).unwrap();
            let (a, b) = (s.start as usize, (s.start + s.len) as usize);
            fs::write(
                d.join(span_cache::span_file_name(s.start, s.start + s.len)),
                &shard[a..b],
            )
            .unwrap();
            total += s.len;
        }
        for p in shard_files(dir) {
            fs::remove_file(p).unwrap();
        }
        total
    }

    #[test]
    fn term_spans_serve_postings_when_the_shard_is_absent() {
        let dir = scratch("spans");
        build(&dir, true);
        let whole = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        let terms: Vec<String> = ["alpha", "w7", "gamma"].map(String::from).to_vec();
        let expected: Vec<Vec<Posting>> = terms
            .iter()
            .map(|t| whole.postings(t).unwrap().to_vec())
            .collect();
        drop(whole);

        let spans =
            posting_term_spans(&dir, &[terms.clone(), vec!["absent".into()]].concat()).unwrap();
        assert_eq!(spans.len(), 3, "terms the TOC lacks are left out");
        let shard_bytes: u64 = shard_files(&dir)
            .iter()
            .map(|p| fs::metadata(p).unwrap().len())
            .sum();
        let span_bytes = keep_only_spans(&dir, &terms);
        assert!(
            span_bytes * 4 < shard_bytes,
            "{span_bytes} of {shard_bytes}"
        );

        let reader = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        for (t, want) in terms.iter().zip(&expected) {
            assert_eq!(&reader.postings(t).unwrap().to_vec(), want, "{t}");
        }
        let batch = reader.postings_for_terms(&terms);
        assert_eq!(batch.len(), 3);
        let scoring = reader.scoring_postings_for_terms(&terms);
        assert_eq!(scoring.len(), 3);
        assert!(!reader.take_blob_read_failure());

        // A TOC-present term with neither shard nor span is an error, not a
        // silent non-match (PR #83 class).
        assert!(reader.postings("w8").is_none());
        assert!(reader.take_blob_read_failure());
        let more = vec!["w8".to_string()];
        assert!(reader.scoring_postings_for_terms(&more).is_empty());
        assert!(reader.take_blob_read_failure());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn spans_are_ignored_for_segments_without_raw_shards() {
        let dir = scratch("noraw");
        build(&dir, true);
        let terms = vec!["alpha".to_string()];
        keep_only_spans(&dir, &terms);
        // Same bytes, but the footer no longer vouches that shards are raw.
        let mut footer = SegmentReader::read_footer(&dir).unwrap();
        footer.posting_blobs_raw = false;
        fs::write(
            dir.join("footer.json"),
            serde_json::to_vec(&footer).unwrap(),
        )
        .unwrap();
        let reader = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        assert!(reader.postings("alpha").is_none());
        assert!(reader.take_blob_read_failure());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn whole_shard_wins_over_spans() {
        let dir = scratch("wins");
        build(&dir, true);
        let terms = vec!["alpha".to_string()];
        let saved: Vec<(PathBuf, Vec<u8>)> = shard_files(&dir)
            .into_iter()
            .map(|p| {
                let b = fs::read(&p).unwrap();
                (p, b)
            })
            .collect();
        keep_only_spans(&dir, &terms);
        // Corrupt the span: if the reader used it, decoding would differ.
        let span = posting_term_spans(&dir, &terms).unwrap().remove(0);
        let d = span_cache::spans_dir(&dir, &span.file);
        let name = span_cache::span_file_name(span.start, span.start + span.len);
        fs::write(d.join(name), vec![0xffu8; span.len as usize]).unwrap();
        for (p, b) in &saved {
            fs::write(p, b).unwrap();
        }
        let reader = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        assert_eq!(reader.postings("alpha").unwrap().len(), 3000);
        assert!(!reader.take_blob_read_failure());
        let _ = fs::remove_dir_all(&dir);
    }
}

/// #244: the columnar filter format (`filters.toc` + `filters-NNN.col`).
#[cfg(test)]
mod filter_column_format_tests {
    use super::*;
    use kosha_core::{FilterColumnKind, FILTERS_TOC_FILE};
    use std::collections::BTreeSet;

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("kosha-test-fcol-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    /// Documents covering every filterable field type, a field holding two
    /// kinds, a multi-valued field, a field only some documents have, a
    /// long Text value over the cap and a Vector (never a filter column).
    fn docs() -> Vec<(DocumentId, Vec<Field>)> {
        let long = "prose ".repeat(100);
        let mut out = Vec::new();
        for i in 0..20u32 {
            let mut fields = vec![
                Field::text("body", format!("hello world {i}")),
                Field::text(
                    "content",
                    if i == 3 {
                        long.clone()
                    } else {
                        format!("short {i}")
                    },
                ),
                Field::keyword("matterId", format!("m{}", i % 3)),
                Field::boolean("inScope", i % 2 == 0),
                Field::date_val("createdAt", format!("2024-01-{:02}", i + 1)),
                Field::integer("pages", i as i64 * 10 - 50),
                Field::float_val("score", i as f64 / 4.0),
                Field::vector("emb", vec![i as f32, 1.0]),
            ];
            if i % 5 == 0 {
                fields.push(Field::keyword("sparse", "rare"));
                fields.push(Field::keyword("matterId", "extra"));
            }
            if i == 7 {
                fields.push(Field::integer("mixed", 7));
            } else {
                fields.push(Field::keyword("mixed", format!("k{i}")));
            }
            out.push((DocumentId(format!("d{i}")), fields));
        }
        out
    }

    fn write(dir: &Path, version: u32, docs: &[(DocumentId, Vec<Field>)]) -> Footer {
        let mut w = SegmentWriter::new(SegmentId("s".into()), dir.to_path_buf())
            .with_filters_write_version(version)
            .with_text_filter_ignore_above(Some(256));
        for (id, fields) in docs {
            w.add_document(id.clone(), fields.clone());
        }
        w.finalize(Bm25Params::default()).unwrap()
    }

    fn sorted(store: &FilterStore) -> String {
        let mut s: Vec<String> = store
            .string_fields
            .iter()
            .map(|(k, v)| format!("s {k} {v:?}"))
            .chain(
                store
                    .integer_fields
                    .iter()
                    .map(|(k, v)| format!("i {k} {v:?}")),
            )
            .chain(
                store
                    .float_fields
                    .iter()
                    .map(|(k, v)| format!("f {k} {v:?}")),
            )
            .collect();
        s.sort();
        s.join("\n")
    }

    fn blooms(footer: &Footer) -> String {
        let sorted: std::collections::BTreeMap<_, _> =
            footer.filter_blooms.as_ref().unwrap().iter().collect();
        serde_json::to_string(&sorted).unwrap()
    }

    fn fields(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn columnar_round_trip_matches_filters_bin_for_every_field_type() {
        let docs = docs();
        let v1 = temp_dir("v1");
        let v2 = temp_dir("v2");
        let f1 = write(&v1, 1, &docs);
        let f2 = write(&v2, 2, &docs);

        assert!(f1.filter_columns.is_none());
        assert!(v1.join("filters.bin").is_file());
        assert!(!v1.join(FILTERS_TOC_FILE).exists());
        let metas = f2.filter_columns.clone().expect("v2 footer lists columns");
        assert!(!v2.join("filters.bin").exists(), "v2 writes no filters.bin");
        assert!(v2.join(FILTERS_TOC_FILE).is_file());
        for m in &metas {
            assert_eq!(fs::metadata(v2.join(m.file_name())).unwrap().len(), m.bytes);
        }
        // Same blooms either way: pruning does not depend on the format.
        assert_eq!(blooms(&f1), blooms(&f2));

        let r1 = SegmentReader::open(v1.clone()).unwrap();
        let r2 = SegmentReader::open(v2.clone()).unwrap();
        assert!(r2.has_filter_columns() && !r1.has_filter_columns());
        let s1 = r1.try_filter_store().unwrap();
        let s2 = r2.try_filter_store().unwrap();
        assert_eq!(
            sorted(&s1),
            sorted(&s2),
            "adapter rebuilds filters.bin's store exactly"
        );

        // Every kind is present, the two-kind field is two columns, the
        // long Text value is capped out (#243) and vectors never appear.
        assert!(s2.string_fields.contains_key("inScope"));
        assert!(s2.string_fields.contains_key("createdAt"));
        assert_eq!(s2.integer_fields["pages"].len(), 20);
        assert_eq!(s2.float_fields["score"].len(), 20);
        assert_eq!(s2.integer_fields["mixed"], vec![(7, 7)]);
        assert_eq!(s2.string_fields["mixed"].len(), 19);
        assert_eq!(s2.string_fields["sparse"].len(), 4);
        assert_eq!(s2.string_fields["content"].len(), 19);
        assert!(!s2.string_fields["content"].iter().any(|(d, _)| *d == 3));
        assert!(!s2.string_fields.contains_key("emb"));
        assert_eq!(
            metas.iter().filter(|m| m.name == "mixed").count(),
            2,
            "a field holding two kinds is two columns"
        );

        // Per-column API, both formats, same answer.
        for r in [&r1, &r2] {
            let col = r
                .filter_column("matterId", FilterColumnKind::String)
                .unwrap()
                .unwrap();
            let filter_columns::FilterColumn::String(s) = &*col else {
                panic!()
            };
            assert_eq!(s.term_count(), 4);
            assert_eq!(
                s.term_bitmap("extra").unwrap().iter().collect::<Vec<_>>(),
                vec![0, 5, 10, 15]
            );
            let col = r
                .filter_column("pages", FilterColumnKind::Integer)
                .unwrap()
                .unwrap();
            assert_eq!(col.len(), 20);
            assert!(r
                .filter_column("pages", FilterColumnKind::Float)
                .unwrap()
                .is_none());
            assert!(r
                .filter_column("nope", FilterColumnKind::String)
                .unwrap()
                .is_none());
        }

        // Stats in the footer and TOC.
        let pages = metas.iter().find(|m| m.name == "pages").unwrap();
        assert_eq!(
            pages.stats.min,
            Some(kosha_core::FilterStatValue::Integer(-50))
        );
        assert_eq!(
            pages.stats.max,
            Some(kosha_core::FilterStatValue::Integer(140))
        );
        assert_eq!(
            (pages.stats.values, pages.stats.docs, pages.stats.distinct),
            (20, 20, 20)
        );
        let matter = metas.iter().find(|m| m.name == "matterId").unwrap();
        assert_eq!(
            (
                matter.stats.values,
                matter.stats.docs,
                matter.stats.distinct
            ),
            (24, 20, 4)
        );
        let toc = filter_columns::FiltersToc::decode(&fs::read(v2.join(FILTERS_TOC_FILE)).unwrap())
            .unwrap();
        assert_eq!(toc.columns, metas);

        let _ = fs::remove_dir_all(&v1);
        let _ = fs::remove_dir_all(&v2);
    }

    #[test]
    fn segment_with_no_filter_fields_and_empty_segment() {
        for (name, docs) in [
            (
                "nofields",
                vec![(
                    DocumentId("a".into()),
                    vec![Field::text("body", "x ".repeat(300))],
                )],
            ),
            ("nodocs", Vec::new()),
        ] {
            let dir = temp_dir(name);
            let footer = write(&dir, 2, &docs);
            assert_eq!(footer.filter_columns, Some(Vec::new()));
            assert!(
                dir.join(FILTERS_TOC_FILE).is_file(),
                "the TOC is always written"
            );
            let r = SegmentReader::open(dir.clone()).unwrap();
            let store = r.try_filter_store().unwrap();
            assert!(store.string_fields.is_empty() && store.integer_fields.is_empty());
            assert!(
                r.filters.state.lock().unwrap().toc.is_some(),
                "a present TOC is cached"
            );
            let _ = fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn filter_store_for_reads_only_the_named_columns() {
        let dir = temp_dir("subset");
        let footer = write(&dir, 2, &docs());
        let metas = footer.filter_columns.unwrap();
        let matter = metas.iter().find(|m| m.name == "matterId").unwrap();
        // A query node hydrated only the TOC and the matterId column.
        for m in &metas {
            if m.id != matter.id {
                fs::remove_file(dir.join(m.file_name())).unwrap();
            }
        }
        let r = SegmentReader::open(dir.clone()).unwrap();
        let store = r.filter_store_for(&fields(&["matterId"])).unwrap();
        assert_eq!(store.string_fields["matterId"].len(), 24);
        assert_eq!(store.string_fields.len(), 1);
        assert!(store.integer_fields.is_empty());

        // A later query touches `pages`, whose column lands afterwards: the
        // read while it is not local is an error (never "no values"), is
        // not cached, and the store grows once it is local.
        assert!(matches!(
            r.filter_store_for(&fields(&["matterId", "pages"])),
            Err(KoshaError::NotFound(_))
        ));
        assert!(matches!(
            r.filter_column("pages", FilterColumnKind::Integer),
            Err(KoshaError::NotFound(_))
        ));
        // A column the segment does not have is still "no values".
        assert!(r
            .filter_column("nope", FilterColumnKind::String)
            .unwrap()
            .is_none());
        let v2 = temp_dir("subset-src");
        write(&v2, 2, &docs());
        let pages = metas.iter().find(|m| m.name == "pages").unwrap();
        fs::copy(v2.join(pages.file_name()), dir.join(pages.file_name())).unwrap();
        let grown = r.filter_store_for(&fields(&["pages"])).unwrap();
        assert_eq!(grown.integer_fields["pages"].len(), 20);
        assert!(
            grown.string_fields.contains_key("matterId"),
            "earlier columns stay"
        );
        assert!(r
            .filter_column("pages", FilterColumnKind::Integer)
            .unwrap()
            .is_some());

        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&v2);
    }

    #[test]
    fn missing_toc_is_retried_not_cached() {
        let dir = temp_dir("late-toc");
        write(&dir, 2, &docs());
        let stash = dir.join("toc.stash");
        fs::rename(dir.join(FILTERS_TOC_FILE), &stash).unwrap();
        let r = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        assert!(matches!(r.try_filter_store(), Err(KoshaError::NotFound(_))));
        assert!(r
            .filter_column("matterId", FilterColumnKind::String)
            .is_err());
        assert!(r.filters.state.lock().unwrap().toc.is_none());
        fs::rename(&stash, dir.join(FILTERS_TOC_FILE)).unwrap();
        let store = r.try_filter_store().unwrap();
        assert!(store.string_fields.contains_key("matterId"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_or_truncated_column_fails_closed() {
        let dir = temp_dir("corrupt");
        let footer = write(&dir, 2, &docs());
        let metas = footer.filter_columns.unwrap();
        let matter = metas.iter().find(|m| m.name == "matterId").unwrap();
        let path = dir.join(matter.file_name());
        let good = fs::read(&path).unwrap();

        // Truncated.
        fs::write(&path, &good[..good.len() / 2]).unwrap();
        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(matches!(
            r.try_filter_store(),
            Err(KoshaError::CorruptSegment(_))
        ));
        assert!(r.filter_store_for(&fields(&["matterId"])).is_err());
        assert!(r
            .filter_column("matterId", FilterColumnKind::String)
            .is_err());
        // Columns the query does not touch still load.
        assert!(r.filter_store_for(&fields(&["pages"])).is_ok());

        // A flipped byte in the body.
        let mut bad = good.clone();
        let mid = bad.len() - 3;
        bad[mid] ^= 0xFF;
        fs::write(&path, &bad).unwrap();
        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(r.filter_store_for(&fields(&["matterId"])).is_err());

        // Corrupt TOC.
        fs::write(&path, &good).unwrap();
        let toc_path = dir.join(FILTERS_TOC_FILE);
        let mut toc = fs::read(&toc_path).unwrap();
        toc.truncate(toc.len() - 1);
        fs::write(&toc_path, &toc).unwrap();
        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(r.try_filter_store().is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn bloom_rewrite_reads_columnar_segments() {
        let dir = temp_dir("bloom-rewrite");
        let footer = write(&dir, 2, &docs());
        let mut stripped = footer.clone();
        stripped.filter_blooms = None;
        fs::write(
            dir.join("footer.json"),
            serde_json::to_string(&stripped).unwrap(),
        )
        .unwrap();
        let rebuilt = SegmentReader::rewrite_filter_blooms(&dir).unwrap();
        assert_eq!(blooms(&rebuilt), blooms(&footer));
        assert_eq!(rebuilt.filter_columns, footer.filter_columns);
        // A column that is not local fails the rebuild instead of dropping
        // that field's bloom.
        let any = &footer.filter_columns.as_ref().unwrap()[0];
        fs::remove_file(dir.join(any.file_name())).unwrap();
        assert!(SegmentReader::rewrite_filter_blooms(&dir).is_err());
        let _ = fs::remove_dir_all(&dir);

        // Same for a legacy segment's `filters.bin`: rebuilt from nothing,
        // its blooms would prune the segment from every filtered query.
        let dir = temp_dir("bloom-rewrite-v1");
        write(&dir, 1, &docs());
        fs::remove_file(dir.join(kosha_core::FILTERS_BIN_FILE)).unwrap();
        assert!(matches!(
            SegmentReader::rewrite_filter_blooms(&dir),
            Err(KoshaError::NotFound(_))
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    /// Compaction re-adds every stored document to a fresh writer, so
    /// its output format is whatever the writer gate says, whatever the
    /// inputs were: v1 inputs become columnar under `=2`, and columnar
    /// inputs go back to `filters.bin` on a rollback to `=1`.
    #[test]
    fn rewriting_a_segment_follows_the_writer_gate_both_ways() {
        let src_v1 = temp_dir("merge-src-v1");
        let src_v2 = temp_dir("merge-src-v2");
        write(&src_v1, 1, &docs());
        write(&src_v2, 2, &docs());
        let expected = sorted(
            &SegmentReader::open(src_v1.clone())
                .unwrap()
                .try_filter_store()
                .unwrap(),
        );
        for (src, target) in [(&src_v1, 2u32), (&src_v2, 1u32)] {
            let out = temp_dir(&format!("merge-out-{target}"));
            let reader = SegmentReader::open(src.clone()).unwrap();
            let mut w = SegmentWriter::new(SegmentId("m".into()), out.clone())
                .with_filters_write_version(target)
                .with_text_filter_ignore_above(Some(256));
            for raw in reader.iter_doc_records_raw() {
                let raw = raw.unwrap();
                w.add_document_with_vectors(raw.record.doc_id, raw.record.fields, raw.vectors);
            }
            let footer = w.finalize(Bm25Params::default()).unwrap();
            assert_eq!(footer.filter_columns.is_some(), target == 2);
            assert_eq!(out.join("filters.bin").exists(), target == 1);
            let merged = SegmentReader::open(out.clone()).unwrap();
            assert_eq!(sorted(&merged.try_filter_store().unwrap()), expected);
            let _ = fs::remove_dir_all(&out);
        }
        let _ = fs::remove_dir_all(&src_v1);
        let _ = fs::remove_dir_all(&src_v2);
    }

    #[test]
    fn footer_without_filter_columns_parses_as_legacy() {
        // A footer.json from before #244 has no `filter_columns` key.
        let dir = temp_dir("legacy-footer");
        let footer = write(&dir, 1, &docs());
        let json = serde_json::to_string(&footer).unwrap();
        assert!(!json.contains("filter_columns"));
        let back: Footer = serde_json::from_str(&json).unwrap();
        assert!(back.filter_columns.is_none());
        assert_eq!(back.filter_files_for(None), vec!["filters.bin".to_string()]);
        let _ = fs::remove_dir_all(&dir);
    }
}

/// Global bound-driven rescoring (#381): the segment half — candidates
/// with error bounds, and exact scores from `doc_vectors.bin` (local or
/// ranged) with unavailability reported rather than hidden.
#[cfg(test)]
mod global_rescore_tests {
    use super::*;
    use std::collections::HashSet;

    const DIM: usize = 32;
    const DOCS: u64 = 600;

    fn scratch(name: &str) -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kosha-global-rescore-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn vector(seed: u64) -> Vec<f32> {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..DIM)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
            })
            .collect()
    }

    fn cosine(a: &[f32], b: &[f32]) -> f64 {
        let dot: f64 = a.iter().zip(b).map(|(x, y)| *x as f64 * *y as f64).sum();
        let na: f64 = a.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
        let nb: f64 = b.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
        dot / (na * nb)
    }

    /// A segment of `DOCS` vectors in `quantization`, plus the vectors by
    /// doc_seq (documents are added in doc_seq order).
    fn segment(name: &str, quantization: VectorQuantization) -> (PathBuf, Vec<Vec<f32>>) {
        let dir = scratch(name);
        let mut w = SegmentWriter::new(SegmentId("s".into()), dir.clone())
            .with_vector_store_mode(VectorStoreMode::Sidecar)
            .with_vector_quantization(VectorQuantizationConfig::all(quantization));
        let mut vectors = Vec::new();
        for i in 0..DOCS {
            let v = vector(i + 1);
            vectors.push(v.clone());
            w.add_document(DocumentId(format!("d{i}")), vec![Field::vector("emb", v)]);
        }
        let footer = w.finalize(Bm25Params::default()).unwrap();
        assert!(footer.doc_vectors);
        (dir, vectors)
    }

    fn brute_force(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<(u32, f64)> {
        let scored: Vec<(u32, f64)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u32, cosine(query, v)))
            .collect();
        top_k_by_score(scored, k)
    }

    /// Every posting: only the quantization, never the probe budget, can
    /// cost a neighbour here.
    const ALL: KnnProbeBudget = KnnProbeBudget::Nprobe(1_000_000);

    #[test]
    fn i8_bounds_hold_and_bound_driven_rescoring_is_exact() {
        let (dir, vectors) = segment("i8", VectorQuantization::I8);
        let reader = SegmentReader::open(dir.clone()).unwrap();
        let k = 10;
        for q in 0..20u64 {
            let query = vector(900_000 + q);
            let found = reader
                .knn_candidates_fetching("emb", &query, k, ALL, None, None)
                .unwrap()
                .unwrap();
            assert!(!found.exact);
            assert!(found.postings > 0);
            // The strict i8 bound is a guarantee, not a confidence.
            for c in &found.candidates {
                let exact = cosine(&query, &vectors[c.doc_seq as usize]);
                assert!(
                    (c.estimate - exact).abs() <= c.error,
                    "doc {} estimate {} exact {exact} error {}",
                    c.doc_seq,
                    c.estimate,
                    c.error
                );
            }
            // So the true top k always survives the segment's prune.
            let want = brute_force(&vectors, &query, k);
            let ids: HashSet<u32> = found.candidates.iter().map(|c| c.doc_seq).collect();
            for (doc_seq, _) in &want {
                assert!(ids.contains(doc_seq), "true neighbour {doc_seq} pruned");
            }
            // The prune keeps strictly fewer than every vector.
            assert!(found.candidates.len() < DOCS as usize);
            // Rescored exactly, the shortlist's top k is the brute force's.
            let seqs: Vec<u32> = found.candidates.iter().map(|c| c.doc_seq).collect();
            let rescored = reader.knn_exact_scores_fetching("emb", &query, &seqs, None);
            assert_eq!(rescored.unavailable, 0, "{:?}", rescored.reason);
            assert!(!rescored.ranged);
            let got = top_k_by_score(rescored.scores, k);
            let got_ids: Vec<u32> = got.iter().map(|h| h.0).collect();
            let want_ids: Vec<u32> = want.iter().map(|h| h.0).collect();
            assert_eq!(got_ids, want_ids);
            for ((_, g), (_, w)) in got.iter().zip(&want) {
                assert!((g - w).abs() < 1e-5);
            }
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn f32_candidates_are_the_exact_top_k() {
        let (dir, vectors) = segment("f32", VectorQuantization::F32);
        let reader = SegmentReader::open(dir.clone()).unwrap();
        let query = vector(4242);
        let found = reader
            .knn_candidates_fetching("emb", &query, 10, ALL, None, None)
            .unwrap()
            .unwrap();
        assert!(found.exact);
        assert_eq!(found.candidates.len(), 10);
        assert!(found.candidates.iter().all(|c| c.error == 0.0));
        let want: Vec<u32> = brute_force(&vectors, &query, 10)
            .into_iter()
            .map(|h| h.0)
            .collect();
        let got: Vec<u32> = found.candidates.iter().map(|c| c.doc_seq).collect();
        assert_eq!(got, want);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rabitq_bound_driven_rescoring_recalls_the_brute_force_top_k() {
        // RaBitQ's bound holds with a confidence (epsilon0 = 1.9 standard
        // deviations), not always: recall is high, not guaranteed.
        let (dir, vectors) = segment("rabitq", VectorQuantization::RaBitQ);
        let reader = SegmentReader::open(dir.clone()).unwrap();
        let k = 10;
        let (mut recall, mut held, mut total) = (0.0f64, 0usize, 0usize);
        let queries = 20u64;
        for q in 0..queries {
            let query = vector(700_000 + q);
            let found = reader
                .knn_candidates_fetching("emb", &query, k, ALL, None, None)
                .unwrap()
                .unwrap();
            assert!(!found.exact);
            for c in &found.candidates {
                total += 1;
                let exact = cosine(&query, &vectors[c.doc_seq as usize]);
                held += usize::from((c.estimate - exact).abs() <= c.error);
            }
            let seqs: Vec<u32> = found.candidates.iter().map(|c| c.doc_seq).collect();
            let rescored = reader.knn_exact_scores_fetching("emb", &query, &seqs, None);
            assert_eq!(rescored.unavailable, 0);
            let got: HashSet<u32> = top_k_by_score(rescored.scores, k)
                .into_iter()
                .map(|h| h.0)
                .collect();
            let want = brute_force(&vectors, &query, k);
            recall += want.iter().filter(|h| got.contains(&h.0)).count() as f64 / k as f64;
        }
        let recall = recall / queries as f64;
        assert!(recall >= 0.95, "recall@10 {recall}");
        assert!(
            held as f64 >= 0.9 * total as f64,
            "bounds held {held}/{total}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn ranged_shortlist_matches_local_rabitq_bound_expansion() {
        let (dir, _) = segment("rabitq-shortlist", VectorQuantization::RaBitQ);
        let reader = SegmentReader::open(dir.clone())
            .unwrap()
            .with_vector_rescore_factor(1);
        let k = 10;
        let mut used_expanded_candidates = false;
        for seed in 700_000..700_020 {
            let query = vector(seed);
            let expected = reader
                .knn_search_filtered_fetching("emb", &query, k, 1_000_000, None, None)
                .unwrap()
                .unwrap();
            let (actual, _) = reader
                .knn_rescore_shortlist_fetching("emb", &query, k, ALL, None, None)
                .unwrap()
                .unwrap();
            assert_eq!(actual.len(), expected.len());
            for (&(seq, score), &(expected_seq, expected_score)) in actual.iter().zip(&expected) {
                assert_eq!(seq, expected_seq);
                assert!((score - expected_score).abs() < 1e-6);
            }
            let estimated = reader
                .knn_candidates_fetching("emb", &query, k, ALL, None, None)
                .unwrap()
                .unwrap();
            let narrow: HashSet<_> = estimated
                .candidates
                .iter()
                .take(k)
                .map(|c| c.doc_seq)
                .collect();
            used_expanded_candidates |= actual.iter().any(|(seq, _)| !narrow.contains(seq));
        }
        assert!(used_expanded_candidates);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ranged_shortlist_fetches_only_candidates_that_pass_the_bound() {
        let dir = scratch("bound-filter");
        let mut writer = SegmentWriter::new(SegmentId("s".into()), dir.clone())
            .with_vector_store_mode(VectorStoreMode::Sidecar)
            .with_vector_quantization(VectorQuantizationConfig::all(VectorQuantization::I8));
        let query = vector(1);
        for i in 0..8 {
            let v = if i == 0 {
                query.clone()
            } else {
                query.iter().map(|&x| -x).collect()
            };
            writer.add_document(DocumentId(format!("d{i}")), vec![Field::vector("emb", v)]);
        }
        writer.finalize(Bm25Params::default()).unwrap();
        let path = dir.join(DOC_VECTORS_FILE);
        let dv = DocVectors::open(path.clone()).unwrap();
        let wanted = dv.field_vector_ranges("emb", &[0]);
        let bytes = fs::read(&path).unwrap();
        fs::remove_file(path).unwrap();
        let spans = span_cache::spans_dir(&dir, DOC_VECTORS_FILE);
        fs::create_dir_all(&spans).unwrap();
        let put = |start: u64, end: u64| {
            fs::write(
                spans.join(span_cache::span_file_name(start, end)),
                &bytes[start as usize..end as usize],
            )
            .unwrap();
        };
        put(0, doc_vectors_header_len(&bytes).unwrap().unwrap());
        let fetch = |_: &Path, file: &str, ranges: &[(u64, u64)]| {
            assert_eq!(file, DOC_VECTORS_FILE);
            assert_eq!(ranges, wanted);
            for &(start, end) in ranges {
                put(start, end);
            }
            span_cache::SpanHolds::new()
        };
        let reader = SegmentReader::open(dir.clone())
            .unwrap()
            .with_vector_rescore_factor(8);
        let (hits, _) = reader
            .knn_rescore_shortlist_fetching("emb", &query, 1, ALL, None, Some(&fetch))
            .unwrap()
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, 0);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn exact_scores_report_a_missing_sidecar_and_read_ranged_spans() {
        let (dir, vectors) = segment("ranged", VectorQuantization::I8);
        let query = vector(31337);
        let seqs: Vec<u32> = vec![3, 17, 250, 599];
        let dv = fs::read(dir.join(DOC_VECTORS_FILE)).unwrap();
        fs::remove_file(dir.join(DOC_VECTORS_FILE)).unwrap();

        // No sidecar at all (not even its header): every candidate is
        // reported unavailable, with a reason — never silently final.
        let reader = SegmentReader::open(dir.clone()).unwrap();
        let missing = reader.knn_exact_scores_fetching("emb", &query, &seqs, None);
        assert_eq!(missing.unavailable, seqs.len());
        assert!(missing.scores.is_empty());
        assert!(missing
            .reason
            .as_deref()
            .unwrap()
            .contains(DOC_VECTORS_FILE));

        // Header cached, vectors not: ranged reads through `fetch`.
        let put = |s: u64, e: u64| {
            let d = span_cache::spans_dir(&dir, DOC_VECTORS_FILE);
            fs::create_dir_all(&d).unwrap();
            fs::write(
                d.join(span_cache::span_file_name(s, e)),
                &dv[s as usize..e as usize],
            )
            .unwrap();
        };
        let header_len = doc_vectors_header_len(&dv).unwrap().unwrap();
        put(0, header_len);
        let reader = SegmentReader::open(dir.clone()).unwrap();

        // Without a fetcher the vectors are not local: unavailable.
        let no_fetch = reader.knn_exact_scores_fetching("emb", &query, &seqs, None);
        assert!(no_fetch.ranged);
        assert_eq!(no_fetch.unavailable, seqs.len());

        let calls = Mutex::new(Vec::<(String, usize)>::new());
        let fetch = |seg: &Path, file: &str, ranges: &[(u64, u64)]| {
            assert_eq!(seg, dir.as_path());
            calls.lock().unwrap().push((file.to_string(), ranges.len()));
            for &(s, e) in ranges {
                put(s, e);
            }
            span_cache::SpanHolds::new()
        };
        let fetched = reader.knn_exact_scores_fetching("emb", &query, &seqs, Some(&fetch));
        assert!(fetched.ranged);
        assert_eq!(fetched.unavailable, 0, "{:?}", fetched.reason);
        assert_eq!(fetched.ranged_spans, seqs.len());
        assert_eq!(
            *calls.lock().unwrap(),
            vec![(DOC_VECTORS_FILE.to_string(), seqs.len())],
            "one fetch call for every missing span"
        );
        for (doc_seq, score) in &fetched.scores {
            let want = cosine(&query, &vectors[*doc_seq as usize]);
            assert!((score - want).abs() < 1e-5);
        }
        // Spans now cached: no second fetch.
        let again = reader.knn_exact_scores_fetching("emb", &query, &seqs, Some(&fetch));
        assert_eq!(again.unavailable, 0);
        assert_eq!(again.ranged_spans, 0);
        assert_eq!(calls.lock().unwrap().len(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn kth_lower_bound_needs_k_candidates() {
        let c = |doc_seq, estimate, error| KnnCandidate {
            doc_seq,
            estimate,
            error,
        };
        let cands = [c(0, 0.9, 0.1), c(1, 0.8, 0.0), c(2, 0.5, 0.2)];
        assert_eq!(kth_lower_bound(&cands, 0), None);
        assert_eq!(kth_lower_bound(&cands, 4), None);
        assert!((kth_lower_bound(&cands, 1).unwrap() - 0.8).abs() < 1e-12);
        let second = kth_lower_bound(&cands, 2).unwrap();
        assert!((second - 0.8).abs() < 1e-12);
        assert!((kth_lower_bound(&cands, 3).unwrap() - 0.3).abs() < 1e-12);
    }

    /// Microbenchmark for the ranged rescore read (#381): a segment whose
    /// `doc_vectors.bin` is not local and whose span cache holds one span
    /// per document (`docs` spans), read for `selected` candidates. Compares
    /// the per-candidate `vectors_for` loop (one span-directory listing per
    /// candidate, the PR's first version) with `knn_exact_scores_fetching`
    /// (one listing per call). Timing only, so ignored by default:
    /// `cargo test -p kosha-segment --release -- --ignored --nocapture
    /// ranged_rescore_read_microbench`.
    #[test]
    #[ignore]
    fn ranged_rescore_read_microbench() {
        const BENCH_DIM: usize = 1024;
        let docs: u64 = std::env::var("MICROBENCH_DOCS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4000);
        let selected: usize = 400;
        let dir = scratch("microbench");
        let mut w = SegmentWriter::new(SegmentId("s".into()), dir.clone())
            .with_vector_store_mode(VectorStoreMode::Sidecar)
            .with_vector_quantization(VectorQuantizationConfig::all(VectorQuantization::I8));
        let gen = |seed: u64| -> Vec<f32> {
            let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            (0..BENCH_DIM)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
                })
                .collect()
        };
        for i in 0..docs {
            w.add_document(
                DocumentId(format!("d{i}")),
                vec![Field::vector("emb", gen(i + 1))],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();
        let dv = fs::read(dir.join(DOC_VECTORS_FILE)).unwrap();
        fs::remove_file(dir.join(DOC_VECTORS_FILE)).unwrap();
        let spans_dir = span_cache::spans_dir(&dir, DOC_VECTORS_FILE);
        fs::create_dir_all(&spans_dir).unwrap();
        let header_len = doc_vectors_header_len(&dv).unwrap().unwrap();
        let put = |s: u64, e: u64| {
            fs::write(
                spans_dir.join(span_cache::span_file_name(s, e)),
                &dv[s as usize..e as usize],
            )
            .unwrap();
        };
        put(0, header_len);
        let reader = SegmentReader::open(dir.clone()).unwrap();
        let header = reader.doc_vectors().unwrap();
        for doc_seq in 0..docs as u32 {
            let (s, e) = header.doc_span(doc_seq).unwrap();
            put(s, e);
        }
        let query = gen(424_242);
        let seqs: Vec<u32> = (0..selected as u64)
            .map(|i| ((i * 7919) % docs) as u32)
            .collect();
        let rounds = 5;
        let t = std::time::Instant::now();
        for _ in 0..rounds {
            let mut n = 0;
            for &doc_seq in &seqs {
                n += header.vectors_for(doc_seq).unwrap().len();
            }
            assert_eq!(n, seqs.len());
        }
        let per_candidate = t.elapsed().as_secs_f64() * 1e3 / rounds as f64;
        let t = std::time::Instant::now();
        for _ in 0..rounds {
            let r = reader.knn_exact_scores_fetching("emb", &query, &seqs, None);
            assert_eq!(r.unavailable, 0, "{:?}", r.reason);
        }
        let batched = t.elapsed().as_secs_f64() * 1e3 / rounds as f64;
        println!(
            "ranged rescore read: {docs} cached spans, {selected} candidates, dim {BENCH_DIM}: \
             per-candidate listing {per_candidate:.2} ms/call, one listing {batched:.2} ms/call"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod doc_store_zstd_tests {
    //! Research #388: per-record zstd doc store with a per-segment
    //! dictionary, behind `KOSHA_DOC_STORE_COMPRESSION` (default off).
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "kosha-test-docstore-zstd-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    /// Passage-like text: a shared vocabulary with per-document variation.
    fn passage(i: usize) -> String {
        const WORDS: &[&str] = &[
            "the",
            "of",
            "and",
            "a",
            "to",
            "in",
            "is",
            "for",
            "that",
            "with",
            "search",
            "index",
            "segment",
            "document",
            "query",
            "storage",
            "cache",
            "latency",
            "vector",
            "term",
            "posting",
            "block",
            "compression",
            "dictionary",
            "record",
            "page",
            "range",
            "read",
        ];
        let mut s = 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(i as u64 + 1) | 1;
        let mut out = format!("passage {i}:");
        for _ in 0..(40 + i % 30) {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            out.push(' ');
            out.push_str(WORDS[(s % WORDS.len() as u64) as usize]);
        }
        out
    }

    fn fields(i: usize, with_vector: bool) -> Vec<Field> {
        let mut f = vec![
            Field::text("text", passage(i)),
            Field::keyword("tag", if i.is_multiple_of(3) { "fizz" } else { "plain" }),
            Field::integer("n", i as i64),
        ];
        if with_vector {
            f.push(Field::vector(
                "emb",
                (0..8).map(|d| (i * 8 + d) as f32 * 0.125).collect(),
            ));
        }
        f
    }

    fn write(
        dir: &Path,
        docs: usize,
        zstd: Option<DocStoreZstdConfig>,
        mode: VectorStoreMode,
    ) -> Footer {
        let mut w = SegmentWriter::new(SegmentId("z".into()), dir.to_path_buf())
            .with_vector_store_mode(mode)
            .with_doc_store_zstd(zstd);
        for i in 0..docs {
            w.add_document(DocumentId(format!("d{i}")), fields(i, true));
        }
        w.finalize(Bm25Params::default()).unwrap()
    }

    fn assert_same(a: &DocRecord, b: &DocRecord) {
        assert_eq!(a.doc_id, b.doc_id);
        assert_eq!(a.doc_seq, b.doc_seq);
        assert_eq!(a.field_length, b.field_length);
        assert_eq!(a.fields.len(), b.fields.len());
        for (x, y) in a.fields.iter().zip(&b.fields) {
            assert_eq!(
                (&x.name, x.field_type, &x.value),
                (&y.name, y.field_type, &y.value)
            );
        }
    }

    #[test]
    fn default_writer_is_raw_and_footer_has_no_new_key() {
        let dir = scratch("default");
        let footer = write(&dir, 30, None, VectorStoreMode::Sidecar);
        assert!(footer.doc_store_compression.is_none());
        let json = fs::read_to_string(dir.join("footer.json")).unwrap();
        assert!(!json.contains("doc_store_compression"));
        let r = SegmentReader::open(dir.clone()).unwrap();
        assert!(!r.doc_store_compressed());
        assert_eq!(r.doc_store_dict_bytes(), 0);
        // No trailer after the entries: the offsets file is exactly what
        // the pre-#388 writer produced.
        let (_, trailer) = try_read_doc_index_parts(&dir, 30).unwrap();
        assert!(trailer.is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    /// Round trip: every record (sidecar vectors filled back in) equals the
    /// raw-layout segment's, through `doc_record_full`, `iter_doc_records`
    /// and `iter_doc_records_raw`; and the compressed store is smaller.
    #[test]
    fn compressed_round_trips_every_record_and_is_smaller() {
        let raw_dir = scratch("rt-raw");
        let z_dir = scratch("rt-zstd");
        let docs = 400;
        write(&raw_dir, docs, None, VectorStoreMode::Sidecar);
        let footer = write(
            &z_dir,
            docs,
            Some(DocStoreZstdConfig::default()),
            VectorStoreMode::Sidecar,
        );
        let meta = footer.doc_store_compression.clone().expect("compressed");
        assert_eq!(meta.codec, kosha_core::DocStoreCompression::ZSTD_DICT);
        assert!(meta.dict_bytes > 0, "a dictionary was trained");
        let back: Footer =
            serde_json::from_str(&fs::read_to_string(z_dir.join("footer.json")).unwrap()).unwrap();
        assert_eq!(back.doc_store_compression, Some(meta.clone()));

        let raw = SegmentReader::open(raw_dir.clone()).unwrap();
        let z = SegmentReader::open(z_dir.clone()).unwrap();
        assert!(z.doc_store_compressed());
        assert_eq!(z.doc_store_dict_bytes(), meta.dict_bytes as usize);
        assert!(z.has_local_doc_store());
        for seq in 0..docs as u32 {
            assert_same(
                &raw.doc_record_full(seq).unwrap().unwrap(),
                &z.doc_record_full(seq).unwrap().unwrap(),
            );
            let m = z.doc_meta(seq).unwrap();
            assert_eq!(m.doc_id, format!("d{seq}"));
        }
        assert!(z.doc_record_full(docs as u32).unwrap().is_none());
        let a: Vec<_> = raw.iter_doc_records().map(Result::unwrap).collect();
        let b: Vec<_> = z.iter_doc_records().map(Result::unwrap).collect();
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(&b) {
            assert_same(x, y);
        }
        for (x, y) in raw.iter_doc_records_raw().zip(z.iter_doc_records_raw()) {
            let (x, y) = (x.unwrap(), y.unwrap());
            assert_same(&x.record, &y.record);
            assert_eq!(x.vectors, y.vectors);
        }
        let size = |d: &Path| fs::metadata(d.join(DOC_STORE_FILE)).unwrap().len();
        // The file is the records, then the dictionary copy and 8 bytes.
        let z_records = z.doc_store_len().unwrap();
        assert_eq!(size(&z_dir), z_records + meta.dict_bytes as u64 + 8);
        assert!(
            z_records * 2 < size(&raw_dir),
            "compressed records {z_records} vs raw {}",
            size(&raw_dir)
        );
        let _ = fs::remove_dir_all(&raw_dir);
        let _ = fs::remove_dir_all(&z_dir);
    }

    /// The ranged-read contract of #245 holds on stored (compressed)
    /// bytes: the `doc_span` slice, and only it, materializes the record,
    /// fetched remotely (`doc_record_from_bytes`) or from the sparse span
    /// cache with `doc_store.bin` absent.
    #[test]
    fn ranged_reads_fetch_only_the_compressed_span() {
        let dir = scratch("ranged");
        let docs = 200;
        write(
            &dir,
            docs,
            Some(DocStoreZstdConfig::default()),
            VectorStoreMode::Sidecar,
        );
        let r = SegmentReader::open(dir.clone()).unwrap();
        let store = fs::read(dir.join(DOC_STORE_FILE)).unwrap();
        // The records end where the dictionary's tail copy starts.
        let (dict_tail, records_end) = doc_store_zstd::read_dict_tail(&store).unwrap();
        assert_eq!(dict_tail.len(), r.doc_store_dict_bytes());
        assert_eq!(r.doc_store_len(), Some(records_end as u64));
        let expected: Vec<DocRecord> = (0..docs as u32)
            .map(|s| r.doc_record_full(s).unwrap().unwrap())
            .collect();
        let mut spans = Vec::new();
        let mut prev_end = 4u64;
        for seq in 0..docs as u32 {
            let (off, len) = r.doc_span(seq).unwrap();
            assert_eq!(off, prev_end, "records are back to back");
            prev_end = off + len as u64;
            let bytes = store[off as usize..prev_end as usize].to_vec();
            assert_same(
                &r.doc_record_from_bytes(seq, &bytes).unwrap().unwrap(),
                &expected[seq as usize],
            );
            spans.push((off, bytes));
        }
        // Wrong-length buffer: an error, never garbage.
        let (_, len0) = r.doc_span(0).unwrap();
        assert!(r
            .doc_record_from_bytes(0, &store[4..4 + len0 as usize + 1])
            .is_err());

        // Scoring-set-only hydration: doc_store.bin gone, only some spans
        // persisted in the span cache.
        fs::remove_file(dir.join(DOC_STORE_FILE)).unwrap();
        assert!(!r.has_local_doc_store());
        let page = [3u32, 77, 150];
        let sdir = span_cache::spans_dir(&dir, DOC_STORE_FILE);
        fs::create_dir_all(&sdir).unwrap();
        for &seq in &page {
            let (off, bytes) = &spans[seq as usize];
            fs::write(
                sdir.join(span_cache::span_file_name(*off, off + bytes.len() as u64)),
                bytes,
            )
            .unwrap();
        }
        for seq in 0..docs as u32 {
            let local = r.doc_is_local(seq);
            assert_eq!(local, page.contains(&seq), "doc {seq}");
            if local {
                assert_same(
                    &r.doc_record_full(seq).unwrap().unwrap(),
                    &expected[seq as usize],
                );
            } else {
                assert!(r.doc_record_full(seq).is_err());
            }
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// Vectors never go inline into a compressed doc store, even when
    /// `KOSHA_VECTOR_STORE=inline` asks for it.
    #[test]
    fn compression_forces_the_vector_sidecar() {
        let dir = scratch("inline");
        let footer = write(
            &dir,
            40,
            Some(DocStoreZstdConfig::default()),
            VectorStoreMode::Inline,
        );
        assert!(footer.doc_vectors, "vectors moved to doc_vectors.bin");
        assert!(dir.join(DOC_VECTORS_FILE).exists());
        let r = SegmentReader::open(dir.clone()).unwrap();
        for raw in r.iter_doc_records_raw() {
            let raw = raw.unwrap();
            let emb = raw.record.fields.iter().find(|f| f.name == "emb").unwrap();
            assert!(
                emb.value.is_empty(),
                "only the sidecar placeholder is stored"
            );
            assert_eq!(raw.vectors.len(), 1);
        }
        let rec = r.doc_record_full(5).unwrap().unwrap();
        let emb = rec.fields.iter().find(|f| f.name == "emb").unwrap();
        let got: Vec<f32> = serde_json::from_str(&emb.value).unwrap();
        assert_eq!(
            got,
            (0..8).map(|d| (40 + d) as f32 * 0.125).collect::<Vec<_>>()
        );
        // Without compression, inline mode still means inline.
        let raw_dir = scratch("inline-raw");
        assert!(!write(&raw_dir, 40, None, VectorStoreMode::Inline).doc_vectors);
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&raw_dir);
    }

    /// Tiny segments train no dictionary; a disabled dictionary is plain
    /// zstd. Both still round-trip, and records zstd cannot shrink stay
    /// raw inside the compressed store.
    #[test]
    fn small_segments_and_no_dictionary_round_trip() {
        for (name, docs, config) in [
            ("tiny", 3usize, DocStoreZstdConfig::default()),
            ("nodict", 50, DocStoreZstdConfig::with_dict_bytes(3, 0)),
        ] {
            let dir = scratch(name);
            let footer = write(&dir, docs, Some(config), VectorStoreMode::Sidecar);
            assert_eq!(
                footer.doc_store_compression.as_ref().unwrap().dict_bytes,
                0,
                "{name}"
            );
            let r = SegmentReader::open(dir.clone()).unwrap();
            for seq in 0..docs as u32 {
                let rec = r.doc_record_full(seq).unwrap().unwrap();
                assert_eq!(rec.doc_id.0, format!("d{seq}"));
                assert_eq!(rec.fields[0].value, passage(seq as usize));
            }
            let _ = fs::remove_dir_all(&dir);
        }
        // An empty segment stays the 4-byte raw layout.
        let dir = scratch("empty");
        let w = SegmentWriter::new(SegmentId("e".into()), dir.clone())
            .with_doc_store_zstd(Some(DocStoreZstdConfig::default()));
        let footer = w.finalize(Bm25Params::default()).unwrap();
        assert!(footer.doc_store_compression.is_none());
        assert_eq!(SegmentReader::open(dir.clone()).unwrap().doc_count(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    /// The format is the footer's call: a compressed segment never falls
    /// back to the eager full parse (it would read frames as garbage), an
    /// unknown codec is refused, and a reader that ignores the footer (a
    /// pre-#388 binary) fails to parse a record instead of returning one.
    #[test]
    fn compressed_segments_fail_loudly_instead_of_misreading() {
        let dir = scratch("loud");
        write(
            &dir,
            60,
            Some(DocStoreZstdConfig::default()),
            VectorStoreMode::Sidecar,
        );
        let r = SegmentReader::open(dir.clone()).unwrap();
        let (off, len) = r.doc_span(7).unwrap();
        let store = fs::read(dir.join(DOC_STORE_FILE)).unwrap();
        let stored = &store[off as usize..(off + len as u64) as usize];
        let mut cursor = stored;
        assert!(
            parse_one_doc_record(&mut cursor, 7).is_err(),
            "an old reader's parse of a frame errors"
        );

        // Unknown codec.
        let footer_path = dir.join("footer.json");
        let good = fs::read_to_string(&footer_path).unwrap();
        let mut footer: Footer = serde_json::from_str(&good).unwrap();
        footer.doc_store_compression.as_mut().unwrap().codec = "zstd-v9".into();
        fs::write(&footer_path, serde_json::to_string(&footer).unwrap()).unwrap();
        assert!(matches!(
            SegmentReader::open(dir.clone()),
            Err(KoshaError::CorruptSegment(_))
        ));
        fs::write(&footer_path, &good).unwrap();

        // Broken sidecar and no local doc_store.bin: an error, never the
        // eager parse.
        let offsets_path = dir.join("doc_store.offsets");
        let offsets = fs::read(&offsets_path).unwrap();
        let store_path = dir.join(DOC_STORE_FILE);
        fs::write(&offsets_path, &offsets[..offsets.len() - 10]).unwrap();
        fs::remove_file(&store_path).unwrap();
        assert!(!doc_store_opens_lazily(&dir, None));
        assert!(matches!(
            SegmentReader::open(dir.clone()),
            Err(KoshaError::CorruptSegment(_))
        ));
        // doc_store.bin with a damaged dictionary tail cannot rebuild it.
        let mut bad_tail = store.clone();
        let n = bad_tail.len();
        bad_tail[n - 1] ^= 0xFF;
        fs::write(&store_path, &bad_tail).unwrap();
        assert!(SegmentReader::open(dir.clone()).is_err());
        assert!(SegmentReader::backfill_offset_tables(&dir).is_err());
        fs::write(&store_path, &store).unwrap();
        fs::write(&offsets_path, &offsets).unwrap();
        assert!(SegmentReader::open(dir.clone()).is_ok());
        let _ = fs::remove_dir_all(&dir);
    }

    /// `doc_store.offsets` is not a single point of failure: with it
    /// missing, cut (dictionary trailer gone) or stale against the footer,
    /// the reader rebuilds the index from `doc_store.bin` and its
    /// dictionary tail copy, and `backfill_offset_tables` writes back a
    /// sidecar byte-identical to the writer's.
    #[test]
    fn broken_offsets_are_rebuilt_from_doc_store_bin() {
        for (name, dict) in [
            ("rebuild", DocStoreZstdConfig::default()),
            ("rebuild-nodict", DocStoreZstdConfig::with_dict_bytes(3, 0)),
        ] {
            let dir = scratch(name);
            let docs = 150;
            write(&dir, docs, Some(dict), VectorStoreMode::Sidecar);
            let expected: Vec<DocRecord> = {
                let r = SegmentReader::open(dir.clone()).unwrap();
                (0..docs as u32)
                    .map(|s| r.doc_record_full(s).unwrap().unwrap())
                    .collect()
            };
            let offsets_path = dir.join("doc_store.offsets");
            let offsets = fs::read(&offsets_path).unwrap();
            let check = |r: &SegmentReader| {
                assert!(r.doc_store_compressed());
                for seq in 0..docs as u32 {
                    assert_same(
                        &r.doc_record_full(seq).unwrap().unwrap(),
                        &expected[seq as usize],
                    );
                }
            };
            let mut stale = offsets.clone();
            stale[0..4].copy_from_slice(&(docs as u32 + 1).to_le_bytes());
            for broken in [
                None,
                Some(offsets[..offsets.len() - 10].to_vec()),
                Some(stale),
            ] {
                match &broken {
                    None => fs::remove_file(&offsets_path).unwrap(),
                    Some(bytes) => fs::write(&offsets_path, bytes).unwrap(),
                }
                assert!(!doc_store_opens_lazily(&dir, None), "{name}");
                check(&SegmentReader::open(dir.clone()).unwrap());
                SegmentReader::backfill_offset_tables(&dir).unwrap();
                assert_eq!(fs::read(&offsets_path).unwrap(), offsets, "{name}");
                assert!(doc_store_opens_lazily(&dir, None), "{name}");
                check(&SegmentReader::open(dir.clone()).unwrap());
            }
            let _ = fs::remove_dir_all(&dir);
        }
    }
}
