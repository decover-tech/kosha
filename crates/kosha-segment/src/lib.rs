use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use instant_distance::{Builder, HnswMap, Point, Search};
use kosha_core::{
    build_filter_blooms, build_term_bloom, AggBucket, AggBucketResult, AggMetricResult,
    AggregationResults, Bm25Params, DocRecord, DocumentId, Field, FieldType, FilterStore, Footer,
    KoshaError, LocalStorage, Posting, SegmentId, StorageBackend, VectorStore,
};

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
    #[allow(dead_code)]
    output_dir: PathBuf,
    backend: Box<dyn StorageBackend>,
    doc_records: Vec<DocRecord>,
    inverted_index: HashMap<String, Vec<Posting>>,
    total_field_length: u64,
    filter_string: HashMap<String, Vec<(u32, String)>>,
    filter_integer: HashMap<String, Vec<(u32, i64)>>,
    filter_float: HashMap<String, Vec<(u32, f64)>>,
    vectors: Vec<(u32, Vec<f32>)>,
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
            doc_records: Vec::new(),
            inverted_index: HashMap::new(),
            total_field_length: 0,
            filter_string: HashMap::new(),
            filter_integer: HashMap::new(),
            filter_float: HashMap::new(),
            vectors: Vec::new(),
        }
    }

    pub fn add_document(&mut self, doc_id: DocumentId, fields: Vec<Field>) {
        let doc_seq = self.doc_records.len() as u32;
        let mut field_length: u32 = 0;

        for field in &fields {
            if field.field_type == FieldType::Text {
                let tokens = tokenize_with_positions(&field.value);
                field_length += tokens.len() as u32;
                for (token, pos) in tokens {
                    let postings = self.inverted_index.entry(token).or_default();
                    if let Some(last) = postings.last_mut() {
                        if last.doc_id == doc_seq {
                            last.term_frequency += 1;
                            last.positions.push(pos);
                            continue;
                        }
                    }
                    postings.push(Posting {
                        doc_id: doc_seq,
                        term_frequency: 1,
                        positions: vec![pos],
                    });
                }
            }
            match field.field_type {
                FieldType::Keyword | FieldType::Boolean | FieldType::Date => {
                    self.filter_string
                        .entry(field.name.clone())
                        .or_default()
                        .push((doc_seq, field.value.clone()));
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
                    self.filter_string
                        .entry(field.name.clone())
                        .or_default()
                        .push((doc_seq, field.value.clone()));
                }
                FieldType::Vector => {
                    if let Ok(vec) = serde_json::from_str::<Vec<f32>>(&field.value) {
                        self.vectors.push((doc_seq, vec));
                    }
                }
            }
        }

        self.total_field_length += field_length as u64;
        self.doc_records.push(DocRecord {
            doc_id,
            doc_seq,
            field_length,
            fields,
        });
    }

    pub fn finalize(self, bm25_params: Bm25Params) -> Result<Footer, KoshaError> {
        self.backend.create_dir_all("")?;
        self.write_doc_store()?;
        self.write_inverted_index()?;
        self.write_filters()?;
        self.write_vectors()?;
        let footer = self.write_footer(bm25_params)?;
        Ok(footer)
    }

    /// Writes `doc_store.bin` (unchanged byte layout) and, alongside it,
    /// `doc_store.offsets` — a sidecar sized to `doc_count`, not content
    /// size, that lets `SegmentReader` open a segment without parsing every
    /// document's full field content into memory (see `DocStoreAccess`).
    /// Offsets are captured as a free byproduct of the same loop that
    /// already builds `buf` in doc_seq order — no second pass.
    fn write_doc_store(&self) -> Result<(), KoshaError> {
        let mut buf = Vec::new();
        let mut offsets_buf = Vec::new();
        let doc_count = self.doc_records.len() as u32;
        buf.extend_from_slice(&doc_count.to_le_bytes());
        offsets_buf.extend_from_slice(&doc_count.to_le_bytes());
        for rec in &self.doc_records {
            let record_start = buf.len() as u64;
            let id_bytes = rec.doc_id.0.as_bytes();
            buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(id_bytes);
            buf.extend_from_slice(&rec.field_length.to_le_bytes());
            let field_count = rec.fields.len() as u32;
            buf.extend_from_slice(&field_count.to_le_bytes());
            for field in &rec.fields {
                let name_bytes = field.name.as_bytes();
                buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(name_bytes);
                buf.push(field.field_type as u8);
                let val_bytes = field.value.as_bytes();
                buf.extend_from_slice(&(val_bytes.len() as u64).to_le_bytes());
                buf.extend_from_slice(val_bytes);
            }
            let record_len = (buf.len() as u64 - record_start) as u32;

            offsets_buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
            offsets_buf.extend_from_slice(id_bytes);
            offsets_buf.extend_from_slice(&rec.field_length.to_le_bytes());
            offsets_buf.extend_from_slice(&record_start.to_le_bytes());
            offsets_buf.extend_from_slice(&record_len.to_le_bytes());
        }
        self.backend.write("doc_store.bin", &buf)?;
        self.backend.write("doc_store.offsets", &offsets_buf)?;
        Ok(())
    }

    /// Write `inverted.idx` in the v3 table-of-contents layout (see
    /// [`LazyInvertedIndex`] for the format and why it exists). The legacy
    /// v1 stream layout and inline v2 layout are still readable for
    /// segments already on disk/S3.
    fn write_inverted_index(&self) -> Result<(), KoshaError> {
        let mut terms: Vec<&String> = self.inverted_index.keys().collect();
        // Sorted term order is load-bearing: the reader binary-searches the
        // term table (see `LazyInvertedIndex::find`).
        terms.sort();

        // Serialize the string pool and sharded postings blobs first,
        // recording each term's spans. Query nodes can then hydrate just
        // the TOC (`inverted.idx`) plus the query-term shard(s), not every
        // term's postings in the segment.
        let mut pool: Vec<u8> = Vec::new();
        let mut posting_blobs: Vec<Vec<u8>> =
            (0..POSTING_BLOB_SHARDS).map(|_| Vec::new()).collect();
        // (term_off_in_pool, term_len, blob_id, postings_off_in_blob, postings_len)
        let mut entries: Vec<(u64, u32, u32, u64, u32)> = Vec::with_capacity(terms.len());
        for term_str in &terms {
            let postings = &self.inverted_index[*term_str];
            let term_off = pool.len() as u64;
            pool.extend_from_slice(term_str.as_bytes());
            let blob_id = posting_blob_id_for_term(term_str) as u32;
            let blob = &mut posting_blobs[blob_id as usize];
            let p_off = blob.len() as u64;
            write_postings_skip_split(blob, postings, &self.doc_records);
            let p_len = (blob.len() as u64 - p_off) as u32;
            entries.push((term_off, term_str.len() as u32, blob_id, p_off, p_len));
        }

        let table_len = entries.len() as u64 * SPLIT_INVERTED_TABLE_ENTRY_LEN as u64;
        let pool_base = INVERTED_HEADER_LEN as u64 + table_len;

        let mut buf = Vec::with_capacity(pool_base as usize + pool.len());
        buf.extend_from_slice(&INVERTED_MAGIC.to_le_bytes());
        buf.extend_from_slice(&SKIP_SPLIT_INVERTED_VERSION.to_le_bytes());
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
        let encoded = maybe_compress_inverted(&buf);
        self.backend.write("inverted.idx", &encoded)?;
        for (blob_id, blob) in posting_blobs.into_iter().enumerate() {
            if !blob.is_empty() {
                let encoded = maybe_compress_inverted(&blob);
                self.backend
                    .write(&posting_blob_file_for_id(blob_id), &encoded)?;
            }
        }
        Ok(())
    }

    fn write_filters(&self) -> Result<(), KoshaError> {
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

    fn write_vectors(&self) -> Result<(), KoshaError> {
        if self.vectors.is_empty() {
            return Ok(());
        }
        // Write vector.idx (raw vectors for flat kNN)
        let mut buf = Vec::new();
        let dim = self.vectors[0].1.len() as u32;
        buf.extend_from_slice(&dim.to_le_bytes());
        buf.extend_from_slice(&(self.vectors.len() as u32).to_le_bytes());
        for &(doc_seq, ref v) in &self.vectors {
            buf.extend_from_slice(&doc_seq.to_le_bytes());
            for &val in v {
                buf.extend_from_slice(&val.to_le_bytes());
            }
        }
        self.backend.write("vector.idx", &buf)?;

        Ok(())
    }

    fn write_footer(&self, bm25_params: Bm25Params) -> Result<Footer, KoshaError> {
        let doc_count = self.doc_records.len() as u32;
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
            term_bloom: Some(build_term_bloom(
                self.inverted_index.keys().map(|s| s.as_str()),
            )),
            format_version: kosha_core::SEGMENT_FORMAT_VERSION,
        };
        let json = serde_json::to_string_pretty(&footer)?;
        self.backend.write("footer.json", json.as_bytes())?;
        Ok(footer)
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

pub fn inverted_uses_posting_blobs(segment_dir: &Path) -> bool {
    let Some(header) = read_inverted_header(&segment_dir.join("inverted.idx")) else {
        return false;
    };
    if header[0..4] != INVERTED_MAGIC.to_le_bytes() {
        return false;
    }
    let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
    // Every split version lives here, including v5 — omitting a new split
    // version from this gate is exactly the PR #83 silent-zero-hits bug
    // (cold reads skip posting-blob hydration and score nothing).
    version == FIXED_SPLIT_INVERTED_VERSION
        || version == SPLIT_INVERTED_VERSION
        || version == SKIP_SPLIT_INVERTED_VERSION
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
    cache: PostingsCache<Arc<Vec<Posting>>>,
}

pub struct SplitInvertedIndex {
    data: Vec<u8>,
    segment_dir: PathBuf,
    term_count: usize,
    encoding: SplitPostingsEncoding,
    cache: PostingsCache<Arc<Vec<Posting>>>,
    /// v5 (skip-split) only: per-term scoring views (skip table + raw
    /// scoring/positions bytes, block-decoded on demand) — cached
    /// separately from full decodes because the WAND read path never
    /// materializes `Vec<Posting>` at all. Same admission/LRU policy.
    scoring_cache: PostingsCache<Arc<BlockedTermPostings>>,
}

#[derive(Clone, Copy)]
enum SplitPostingsEncoding {
    Fixed32,
    VarintDelta,
    /// v5: `[skip table][scoring (doc,tf)][positions]` per term span — see
    /// [`BlockedTermPostings`] and [`write_postings_skip_split`].
    SkipSplit,
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

/// Small per-segment LRU of decoded postings, keyed by term-table index.
///
/// The lazy v2 format trades resident memory for a per-query decode — the
/// right trade for the vocabulary at large, but a hot query term ("the")
/// then re-decodes its (large) postings on every query, and a wildcard
/// expanding to thousands of terms re-decodes a big slice of the index per
/// query. Caching the *decoded form of recently-queried terms only* keeps
/// the v1 warm-path amortization for exactly the hot subset, while the
/// cold/open/resident wins of the lazy format stand: the budget is a few
/// MiB per segment (see [`postings_cache_max_bytes`]), not the whole
/// vocabulary.
///
/// Entries are `Arc`s, so a hit is a pointer clone and eviction never
/// invalidates postings an in-flight query still holds. Approximate entry
/// cost: `count × size_of::<Posting>()` + 4 bytes per position.
///
/// Resident bytes are reported to the optional [`PostingsMemoryAccount`]
/// (insert adds, evict and drop release), so a fleet of open segments'
/// aggregate decoded postings counts toward the query pod's live-bytes
/// watermark — bounded by admission, not just the per-segment cap here.
struct PostingsCache<V> {
    max_bytes: usize,
    state: Mutex<PostingsCacheState<V>>,
    account: Option<Arc<dyn PostingsMemoryAccount + Send + Sync>>,
}

struct PostingsCacheState<V> {
    entries: HashMap<usize, (V, usize)>,
    recency: VecDeque<usize>,
    total_bytes: usize,
    /// Recently-missed term indices (bounded ring) for second-touch
    /// admission — see [`PostingsCache::admit_on_miss`].
    recent_misses: VecDeque<usize>,
}

/// Capacity of the second-touch admission ring. Big enough to remember a
/// realistic hot working set between queries; small enough that a wildcard
/// blast writing thousands of one-shot entries through it costs nothing.
const POSTINGS_CACHE_MISS_RING: usize = 256;

impl<V> Default for PostingsCacheState<V> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            recency: VecDeque::new(),
            total_bytes: 0,
            recent_misses: VecDeque::new(),
        }
    }
}

impl<V: Clone> PostingsCache<V> {
    fn new(
        max_bytes: usize,
        account: Option<Arc<dyn PostingsMemoryAccount + Send + Sync>>,
    ) -> Self {
        Self {
            max_bytes,
            state: Mutex::new(PostingsCacheState::default()),
            account,
        }
    }

    fn get(&self, term_index: usize) -> Option<V> {
        if self.max_bytes == 0 {
            return None;
        }
        let mut st = self.state.lock().unwrap();
        let hit = st.entries.get(&term_index).map(|(p, _)| p.clone());
        if hit.is_some() {
            if let Some(pos) = st.recency.iter().position(|k| *k == term_index) {
                st.recency.remove(pos);
            }
            st.recency.push_back(term_index);
        }
        hit
    }

    /// Second-touch admission, recorded on a cache miss: returns whether
    /// this term was *already* missed recently (→ caller should cache the
    /// decode), and remembers the miss either way.
    ///
    /// Why not cache every decode: a wildcard query expanding to thousands
    /// of terms would write its entire one-shot working set through the
    /// cache each time — evicting the genuinely hot terms and paying
    /// insert/evict churn for entries that are never reused. Requiring a
    /// second miss within the ring's horizon filters exactly that traffic:
    /// hot query terms recur and get admitted on their second lookup;
    /// wildcard blasts pass through without disturbing anything.
    fn admit_on_miss(&self, term_index: usize) -> bool {
        let mut st = self.state.lock().unwrap();
        if let Some(pos) = st.recent_misses.iter().position(|k| *k == term_index) {
            st.recent_misses.remove(pos);
            return true;
        }
        if st.recent_misses.len() >= POSTINGS_CACHE_MISS_RING {
            st.recent_misses.pop_front();
        }
        st.recent_misses.push_back(term_index);
        false
    }

    fn insert(&self, term_index: usize, postings: V, bytes: usize) {
        // An entry bigger than the whole budget would evict everything and
        // then itself churn on every query — don't cache it at all.
        if self.max_bytes == 0 || bytes > self.max_bytes {
            return;
        }
        // Capture what changed in resident bytes while holding the cache
        // lock, then report add/release deltas to the live-bytes account
        // *after* releasing it — the account (kosha-query's `MemoryLedger`)
        // takes its own lock, and the established lock-order invariant is
        // "cache locks may be held while taking the ledger lock, never the
        // reverse." Reporting outside the cache lock keeps that direction
        // clean and avoids any reentrancy if `add_postings` ever touches
        // the same segment's cache.
        let to_add: u64 = bytes as u64;
        let mut to_release: Vec<u64> = Vec::new();
        {
            let mut st = self.state.lock().unwrap();
            match st.entries.insert(term_index, (postings, bytes)) {
                None => {
                    st.recency.push_back(term_index);
                    st.total_bytes += bytes;
                }
                Some((_, old_bytes)) => {
                    // Replacing an existing entry: its old resident bytes
                    // were previously reported as added; release them, and
                    // the new bytes get added below.
                    st.total_bytes = st.total_bytes - old_bytes + bytes;
                    to_release.push(old_bytes as u64);
                }
            }
            while st.total_bytes > self.max_bytes {
                let Some(oldest) = st.recency.pop_front() else {
                    break;
                };
                if let Some((_, size)) = st.entries.remove(&oldest) {
                    st.total_bytes -= size;
                    to_release.push(size as u64);
                }
            }
        }
        if let Some(account) = &self.account {
            for r in &to_release {
                account.release_postings(*r);
            }
            if to_add > 0 {
                account.add_postings(to_add);
            }
        }
    }
}

impl<V> Drop for PostingsCache<V> {
    fn drop(&mut self) {
        // Release whatever this segment's postings cache still holds to the
        // live-bytes account, so `MemoryLedger::live` stays truthful as the
        // owning `SegmentReader` (and its caches) go away — see the trait
        // doc. Snapshot under the lock, report outside (same lock-order
        // discipline as `insert`).
        let total: u64 = self
            .state
            .lock()
            .unwrap()
            .total_bytes
            .try_into()
            .unwrap_or(0);
        if total > 0 {
            if let Some(account) = &self.account {
                account.release_postings(total);
            }
        }
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

/// v4 span codec, retained for reading fixtures/tests — the production
/// writer moved to [`write_postings_skip_split`] (v5).
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
fn write_postings_skip_split(out: &mut Vec<u8>, postings: &[Posting], doc_records: &[DocRecord]) {
    put_var_u32(out, postings.len() as u32);

    let mut scoring: Vec<u8> = Vec::new();
    let mut prev_doc = 0u32;
    let mut prev_last = 0u32;
    for block in postings.chunks(SCORING_BLOCK_LEN) {
        let start = scoring.len();
        let mut max_tf = 0u32;
        let mut min_fl = u32::MAX;
        for posting in block {
            put_var_u32(&mut scoring, posting.doc_id.wrapping_sub(prev_doc));
            prev_doc = posting.doc_id;
            put_var_u32(&mut scoring, posting.term_frequency);
            max_tf = max_tf.max(posting.term_frequency);
            if let Some(record) = doc_records.get(posting.doc_id as usize) {
                min_fl = min_fl.min(record.field_length);
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
}

impl BlockedTermPostings {
    /// Parse a v5 term span. Validates the skip table against the span
    /// bounds once, so per-block decode can't walk off the buffer.
    fn parse(span: Arc<Vec<u8>>) -> Option<Self> {
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
        let Some(summary) = self.blocks.get(block) else {
            return false;
        };
        let start = self.scoring_base as usize + summary.scoring_start as usize;
        let end = self.scoring_base as usize + summary.scoring_end as usize;
        let Some(mut buf) = self.span.get(start..end) else {
            return false;
        };
        let in_block = block_posting_count(self.doc_count as usize, block);
        let mut prev_doc = if block == 0 {
            0
        } else {
            self.blocks[block - 1].last_doc_id
        };
        for _ in 0..in_block {
            let Some(doc_delta) = take_var_u32(&mut buf) else {
                return false;
            };
            let Some(term_frequency) = take_var_u32(&mut buf) else {
                return false;
            };
            let doc_id = prev_doc.wrapping_add(doc_delta);
            prev_doc = doc_id;
            out.push(ScoringPosting {
                doc_id,
                term_frequency,
            });
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
        version == FIXED_SPLIT_INVERTED_VERSION
            || version == SPLIT_INVERTED_VERSION
            || version == SKIP_SPLIT_INVERTED_VERSION
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
            let Some(blob) = self.read_posting_blob(blob_id) else {
                continue;
            };
            for (i, slot, term, p_off, p_len, admit) in spans {
                let Some(p_end) = p_off.checked_add(p_len).filter(|&e| e <= blob.len()) else {
                    continue;
                };
                let decoded = match self.encoding {
                    SplitPostingsEncoding::Fixed32 => decode_postings_bytes(&blob[p_off..p_end]),
                    SplitPostingsEncoding::VarintDelta => {
                        decode_postings_varint_delta_bytes(&blob[p_off..p_end])
                    }
                    SplitPostingsEncoding::SkipSplit => {
                        BlockedTermPostings::parse(Arc::new(blob[p_off..p_end].to_vec()))
                            .and_then(|b| b.decode_full())
                    }
                };
                let Some((postings, approx_bytes)) = decoded else {
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
        let (_, _, blob_id, p_off, p_len) = self.raw_entry(i);
        let blob = self.read_posting_blob(blob_id)?;
        let p_end = p_off.checked_add(p_len).filter(|&e| e <= blob.len())?;
        match self.encoding {
            SplitPostingsEncoding::Fixed32 => decode_postings_bytes(&blob[p_off..p_end]),
            SplitPostingsEncoding::VarintDelta => {
                decode_postings_varint_delta_bytes(&blob[p_off..p_end])
            }
            SplitPostingsEncoding::SkipSplit => {
                BlockedTermPostings::parse(Arc::new(blob[p_off..p_end].to_vec()))
                    .and_then(|b| b.decode_full())
            }
        }
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
        if !matches!(self.encoding, SplitPostingsEncoding::SkipSplit) {
            return None;
        }
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
            let Some(blob) = self.read_posting_blob(blob_id) else {
                continue;
            };
            for (i, slot, term, p_off, p_len, admit) in spans {
                let Some(p_end) = p_off.checked_add(p_len).filter(|&e| e <= blob.len()) else {
                    continue;
                };
                let Some(parsed) =
                    BlockedTermPostings::parse(Arc::new(blob[p_off..p_end].to_vec()))
                else {
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
struct LazyFilters {
    segment_dir: PathBuf,
    parsed: OnceLock<FilterStore>,
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
    fn get(&self) -> &FilterStore {
        self.parsed.get_or_init(|| {
            let raw = SegmentReader::read_filters_raw(&self.segment_dir);
            if raw.is_empty() {
                // The writer always emits filters.bin (even with zero
                // fields, the header is present) — an empty read here
                // means the file isn't local, i.e. a hydration/eviction
                // gap, not a segment without filters. Parse yields an
                // empty store either way; make the abnormal case loud
                // instead of silently matching nothing.
                eprintln!(
                    "WARN: filters.bin missing/empty at first use for {} —                      filtered queries against this segment will match nothing                      until it is re-opened with the file present",
                    self.segment_dir.display()
                );
            }
            SegmentReader::parse_filters(&raw)
        })
    }
}

pub struct SegmentReader {
    segment_dir: PathBuf,
    footer: Footer,
    doc_store: DocStoreAccess,
    inverted: InvertedAccess,
    filters: LazyFilters,
    pub vector_store: VectorStore,
    pub hnsw_map: Option<HnswMap<CosinePoint, u32>>,
}

impl SegmentReader {
    /// Opens a segment, always loading vectors and building the HNSW graph.
    /// Kept for callers that genuinely need the vector store regardless of
    /// query shape (merge/compaction, tests). Query-time lexical search
    /// should use `open_with_options` instead — see its doc comment.
    pub fn open(segment_dir: PathBuf) -> Result<Self, KoshaError> {
        Self::open_with_options(segment_dir, true)
    }

    /// Opens a segment, loading the vector store and building its HNSW graph
    /// only when `load_vectors` is true.
    ///
    /// `vector.idx` reads and `build_hnsw` are the dominant cost of opening a
    /// segment (especially under emulation, e.g. amd64-under-Rosetta), yet a
    /// pure lexical/BM25 query (no `query.knn`) never touches either. Skipping
    /// both for that case is what keeps keyword search latency independent of
    /// embedding volume.
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
        let (vs, hm) = if load_vectors {
            let vs = Self::read_vectors(&segment_dir)?;
            let hm = build_hnsw(&vs.vectors).map(|(m, _)| m);
            (vs, hm)
        } else {
            (VectorStore::default(), None)
        };
        let footer = match footer {
            Some(footer) => footer,
            None => Self::read_footer(&segment_dir)?,
        };
        let doc_store = match try_read_doc_index(&segment_dir, footer.doc_count) {
            Some(index) => DocStoreAccess::Lazy {
                doc_store_path: segment_dir.join("doc_store.bin"),
                index,
            },
            None => DocStoreAccess::Eager(Self::read_doc_store(&segment_dir)?),
        };
        Ok(Self {
            segment_dir: segment_dir.clone(),
            footer,
            doc_store,
            inverted: Self::read_inverted(&segment_dir, postings_account)?,
            filters: LazyFilters {
                segment_dir: segment_dir.clone(),
                parsed: OnceLock::new(),
            },
            vector_store: vs,
            hnsw_map: hm,
        })
    }

    /// The segment's filter columns, parsed on first use (see
    /// [`LazyFilters`]). Queries without filters/aggregations/field sorts
    /// never call this, and never pay the parse.
    pub fn filter_store(&self) -> &FilterStore {
        self.filters.get()
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
        match &self.doc_store {
            DocStoreAccess::Eager(records) => Ok(records.get(doc_seq as usize).cloned()),
            DocStoreAccess::Lazy {
                doc_store_path,
                index,
            } => {
                let Some(entry) = index.entries.get(doc_seq as usize) else {
                    return Ok(None);
                };
                let mut file = fs::File::open(doc_store_path)?;
                file.seek(SeekFrom::Start(entry.offset))?;
                let mut buf = vec![0u8; entry.length as usize];
                file.read_exact(&mut buf)?;
                let mut cursor = &buf[..];
                Ok(Some(parse_one_doc_record(&mut cursor, doc_seq)))
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
        match &self.doc_store {
            DocStoreAccess::Eager(_) => true,
            DocStoreAccess::Lazy {
                doc_store_path,
                index,
            } => {
                // Existence alone is not enough: an interrupted hydration
                // write can leave a zero-byte or truncated `doc_store.bin`
                // on disk, and treating it as present makes every
                // materialize of a doc past the truncation point fail
                // ("failed to fill whole buffer") forever. The offsets
                // sidecar knows the exact expected file size — the last
                // record's end — so validate against it.
                let expected = index
                    .entries
                    .last()
                    .map(|e| e.offset + e.length as u64)
                    .unwrap_or(0);
                std::fs::metadata(doc_store_path)
                    .map(|m| m.len() >= expected && m.len() > 0)
                    .unwrap_or(false)
            }
        }
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
        let mut cursor = bytes;
        Ok(Some(parse_one_doc_record(&mut cursor, doc_seq)))
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

    pub fn contains_term(&self, term: &str) -> bool {
        self.inverted.contains_term(term)
    }

    /// All terms in sorted order. Zero-copy for v2 segments (borrows the
    /// term pool); allocates only the pointer Vec.
    pub fn all_terms(&self) -> Vec<&str> {
        self.inverted.all_terms()
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
        let store = Self::read_filters(segment_dir)?;
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
        let inverted = Self::read_inverted(segment_dir, None)?;
        footer.term_bloom = Some(build_term_bloom(inverted.all_terms()));
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
    pub fn backfill_offset_tables(segment_dir: &Path) -> Result<Footer, KoshaError> {
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
            let rec = parse_one_doc_record(&mut cursor, doc_seq);
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

    /// Legacy full-parse path (`DocStoreAccess::Eager`) — reads the whole
    /// `doc_store.bin` at once. Used only when the `doc_store.offsets`
    /// sidecar is missing or fails its sanity check (see
    /// `try_read_doc_index`); the resulting `Vec<DocRecord>` costs exactly
    /// what every segment cost before lazy loading existed.
    fn read_doc_store(segment_dir: &Path) -> Result<Vec<DocRecord>, KoshaError> {
        let data = fs::read(segment_dir.join("doc_store.bin"))?;
        let mut cursor = &data[..];
        let mut records = Vec::new();
        if cursor.len() < 4 {
            return Ok(records);
        }
        let doc_count = read_u32_le(&mut cursor);
        for doc_seq in 0..doc_count {
            records.push(parse_one_doc_record(&mut cursor, doc_seq));
        }
        Ok(records)
    }

    /// Read `inverted.idx` in whichever format it's in: v2 (magic-prefixed
    /// table of contents) opens lazily with zero parsing; anything else is
    /// parsed eagerly via the legacy v1 stream layout — the exact cost every
    /// segment paid before v2 existed.
    fn read_inverted(
        segment_dir: &Path,
        account: Option<Arc<dyn PostingsMemoryAccount + Send + Sync>>,
    ) -> Result<InvertedAccess, KoshaError> {
        let data = read_inverted_payload(fs::read(segment_dir.join("inverted.idx"))?)?;
        if SplitInvertedIndex::detect(&data) {
            return Ok(InvertedAccess::Split(SplitInvertedIndex::from_bytes(
                data,
                segment_dir.to_path_buf(),
                account,
            )?));
        }
        if LazyInvertedIndex::detect(&data) {
            return Ok(InvertedAccess::Lazy(LazyInvertedIndex::from_bytes(
                data, account,
            )?));
        }
        Ok(InvertedAccess::Eager(Self::parse_legacy_inverted(&data)))
    }

    /// Legacy (v1) `inverted.idx` stream parse: term count, then per term
    /// `len/bytes/df/postings_count` + inline postings. Fully materializes
    /// the vocabulary — kept only for segments written before v2.
    fn parse_legacy_inverted(data: &[u8]) -> HashMap<String, Vec<Posting>> {
        let mut cursor = data;
        let mut index = HashMap::new();
        if cursor.len() < 4 {
            return index;
        }
        let term_count = read_u32_le(&mut cursor);
        for _ in 0..term_count {
            let term_len = read_u32_le(&mut cursor) as usize;
            let term_bytes = read_bytes(&mut cursor, term_len);
            let term = String::from_utf8_lossy(term_bytes).to_string();
            let _df = read_u32_le(&mut cursor);
            let postings_len = read_u32_le(&mut cursor) as usize;
            let mut postings = Vec::with_capacity(postings_len);
            for _ in 0..postings_len {
                let doc_id = read_u32_le(&mut cursor);
                let term_frequency = read_u32_le(&mut cursor);
                let positions_len = read_u32_le(&mut cursor) as usize;
                let mut positions = Vec::with_capacity(positions_len);
                for _ in 0..positions_len {
                    positions.push(read_u32_le(&mut cursor));
                }
                postings.push(Posting {
                    doc_id,
                    term_frequency,
                    positions,
                });
            }
            index.insert(term, postings);
        }
        index
    }

    fn read_vectors(segment_dir: &Path) -> Result<VectorStore, KoshaError> {
        let path = segment_dir.join("vector.idx");
        if !path.exists() {
            return Ok(VectorStore::default());
        }
        let data = fs::read(&path)?;
        let mut cursor = &data[..];
        if cursor.len() < 8 {
            return Ok(VectorStore::default());
        }
        let dim = read_u32_le(&mut cursor) as usize;
        let count = read_u32_le(&mut cursor) as usize;
        let mut vectors = Vec::with_capacity(count);
        for _ in 0..count {
            let doc_seq = read_u32_le(&mut cursor);
            let mut v = Vec::with_capacity(dim);
            for _ in 0..dim {
                v.push(read_f32_le(&mut cursor));
            }
            vectors.push((doc_seq, v));
        }
        Ok(VectorStore {
            vectors,
            dimensions: dim,
        })
    }

    /// Raw bytes of `filters.bin` — empty when the file is absent (a
    /// segment with no filter fields). Parsing is deferred to first use;
    /// see [`LazyFilters`].
    fn read_filters_raw(segment_dir: &Path) -> Vec<u8> {
        fs::read(segment_dir.join("filters.bin")).unwrap_or_default()
    }

    /// Eagerly read + parse `filters.bin` — the pre-lazy behavior, kept for
    /// the bloom-rewrite admin path which always needs the parsed form.
    fn read_filters(segment_dir: &Path) -> Result<FilterStore, KoshaError> {
        Ok(Self::parse_filters(&Self::read_filters_raw(segment_dir)))
    }

    /// Parse filter columns from raw `filters.bin` bytes. Empty/short input
    /// (absent file) yields an empty store.
    fn parse_filters(data: &[u8]) -> FilterStore {
        let mut cursor = data;
        let mut store = FilterStore::default();
        if cursor.len() < 4 {
            return store;
        }
        let field_count = read_u32_le(&mut cursor);
        for _ in 0..field_count {
            let name_len = read_u32_le(&mut cursor) as usize;
            let name_bytes = read_bytes(&mut cursor, name_len);
            let name = String::from_utf8_lossy(name_bytes).to_string();
            let field_type = cursor[0];
            cursor = &cursor[1..];
            let entry_count = read_u32_le(&mut cursor) as usize;
            match field_type {
                0 => {
                    let mut entries = Vec::with_capacity(entry_count);
                    for _ in 0..entry_count {
                        let doc_seq = read_u32_le(&mut cursor);
                        let val_len = read_u32_le(&mut cursor) as usize;
                        let val_bytes = read_bytes(&mut cursor, val_len);
                        entries.push((doc_seq, String::from_utf8_lossy(val_bytes).to_string()));
                    }
                    store.string_fields.insert(name, entries);
                }
                1 => {
                    let mut entries = Vec::with_capacity(entry_count);
                    for _ in 0..entry_count {
                        entries.push((read_u32_le(&mut cursor), read_i64_le(&mut cursor)));
                    }
                    store.integer_fields.insert(name, entries);
                }
                2 => {
                    let mut entries = Vec::with_capacity(entry_count);
                    for _ in 0..entry_count {
                        entries.push((read_u32_le(&mut cursor), read_f64_le(&mut cursor)));
                    }
                    store.float_fields.insert(name, entries);
                }
                _ => {}
            }
        }
        store
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
fn parse_one_doc_record(cursor: &mut &[u8], doc_seq: u32) -> DocRecord {
    let id_len = read_u32_le(cursor) as usize;
    let id_bytes = read_bytes(cursor, id_len);
    let doc_id = DocumentId(String::from_utf8_lossy(id_bytes).to_string());
    let field_length = read_u32_le(cursor);
    let field_count = read_u32_le(cursor);
    let mut fields = Vec::with_capacity(field_count as usize);
    for _ in 0..field_count {
        let name_len = read_u32_le(cursor) as usize;
        let name_bytes = read_bytes(cursor, name_len);
        let name = String::from_utf8_lossy(name_bytes).to_string();
        let field_type = match cursor[0] {
            0 => FieldType::Text,
            1 => FieldType::Keyword,
            2 => FieldType::Integer,
            3 => FieldType::Float,
            4 => FieldType::Date,
            5 => FieldType::Boolean,
            6 => FieldType::Vector,
            _ => FieldType::Text,
        };
        *cursor = &cursor[1..];
        let val_len = read_u64_le(cursor) as usize;
        let val_bytes = read_bytes(cursor, val_len);
        let value = String::from_utf8_lossy(val_bytes).to_string();
        fields.push(Field {
            name,
            field_type,
            value,
        });
    }
    DocRecord {
        doc_id,
        doc_seq,
        field_length,
        fields,
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
    let path = segment_dir.join("doc_store.offsets");
    let data = match fs::read(&path) {
        Ok(data) => data,
        Err(_) => return None, // missing sidecar: legacy segment, not an error
    };
    let mut cursor = &data[..];
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
    Some(DocIndex { ids_pool, entries })
}

// ─── Atomic file rewrite ────────────────────────────────────────────────────

/// Overwrite `path` atomically: write to a sibling temp file, then `rename`
/// over the destination. POSIX (and Windows, via `MoveFileEx`-equivalent
/// semantics `std::fs::rename` uses) guarantees a rename within the same
/// directory is atomic — a concurrent reader always sees either the old
/// complete file or the new complete one, never a truncated/partial one.
///
/// This matters specifically for files that get rewritten *in place* on a
/// segment that's already published and actively being searched (unlike the
/// original write during segment build, which completes before the segment
/// is registered in the manifest and so can never race a reader). Plain
/// `fs::write` truncates the destination before filling it back in, so a
/// reader landing in that window sees a short read — for `footer.json` that
/// surfaces as `serde_json` failing with "EOF while parsing a value" on an
/// in-flight search.
///
/// The temp filename includes the PID and a per-process atomic counter so
/// concurrent rewrites of the *same* file (e.g. two overlapping admin
/// requests) never collide on the same temp path.
fn atomic_write(path: &Path, data: &[u8]) -> Result<(), KoshaError> {
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

    fs::write(&tmp_path, data)?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

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
fn read_i64_le(cursor: &mut &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&cursor[..8]);
    *cursor = &cursor[8..];
    i64::from_le_bytes(buf)
}
fn read_f32_le(cursor: &mut &[u8]) -> f32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&cursor[..4]);
    *cursor = &cursor[4..];
    f32::from_le_bytes(buf)
}
fn read_f64_le(cursor: &mut &[u8]) -> f64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&cursor[..8]);
    *cursor = &cursor[8..];
    f64::from_le_bytes(buf)
}
fn read_bytes<'a>(cursor: &mut &'a [u8], len: usize) -> &'a [u8] {
    let result = &cursor[..len];
    *cursor = &cursor[len..];
    result
}

// ─── Tokenizer ──────────────────────────────────────────────────────────────

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
        let mut expected_terms: Vec<&str> = expected.keys().map(|s| s.as_str()).collect();
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
        let mut expected_terms: Vec<&str> = expected.keys().map(|s| s.as_str()).collect();
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
        let first_term = r.all_terms()[0].to_string();
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
        let mut w = SegmentWriter::new(SegmentId("s1".into()), dir.clone());
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
            r.filters.parsed.get().is_none(),
            "open must not have parsed filters.bin"
        );
        let store = r.filter_store();
        assert!(store.string_fields.contains_key("tag"));
        assert!(
            r.filters.parsed.get().is_some(),
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
        let mut w = SegmentWriter::new(SegmentId("s1".into()), dir.clone());
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
        assert!(r.filters.parsed.get().is_none());

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
    fn open_with_options_skips_vectors_and_hnsw_when_not_requested() {
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

        // Lexical path: no vectors read, no HNSW built.
        let lexical = SegmentReader::open_with_options(dir.clone(), false).unwrap();
        assert!(lexical.vector_store.vectors.is_empty());
        assert!(lexical.hnsw_map.is_none());
        // Lexical data is unaffected — still fully readable.
        assert!(lexical.postings("quick").is_some());

        // KNN path (and open(), its default): vectors + HNSW present.
        let knn = SegmentReader::open_with_options(dir.clone(), true).unwrap();
        assert_eq!(knn.vector_store.vectors.len(), 1);
        assert!(knn.hnsw_map.is_some());

        let default_open = SegmentReader::open(dir.clone()).unwrap();
        assert_eq!(default_open.vector_store.vectors.len(), 1);
        assert!(default_open.hnsw_map.is_some());

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
        write_postings_skip_split(&mut span, &postings, &doc_records);
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
        write_postings_skip_split(&mut span, &postings, &doc_records);

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
