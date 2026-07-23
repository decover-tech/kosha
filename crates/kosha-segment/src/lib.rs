use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use instant_distance::{Builder, HnswMap, Point, Search};
use kosha_core::{
    AggBucket, AggBucketResult, AggMetricResult, AggregationResults, Bm25Params, DocRecord,
    DocumentId, Field, FieldType, FilterStore, Footer, KoshaError, LocalStorage, Posting,
    SegmentId, StorageBackend, VectorStore,
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

    fn write_doc_store(&self) -> Result<(), KoshaError> {
        let mut buf = Vec::new();
        let doc_count = self.doc_records.len() as u32;
        buf.extend_from_slice(&doc_count.to_le_bytes());
        for rec in &self.doc_records {
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
        }
        self.backend.write("doc_store.bin", &buf)?;
        Ok(())
    }

    fn write_inverted_index(&self) -> Result<(), KoshaError> {
        let mut terms: Vec<&String> = self.inverted_index.keys().collect();
        terms.sort();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(terms.len() as u32).to_le_bytes());
        for term_str in terms {
            let postings = &self.inverted_index[term_str];
            let term_bytes = term_str.as_bytes();
            buf.extend_from_slice(&(term_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(term_bytes);
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
        self.backend.write("inverted.idx", &buf)?;
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
        };
        let json = serde_json::to_string_pretty(&footer)?;
        self.backend.write("footer.json", json.as_bytes())?;
        Ok(footer)
    }
}

// ─── Segment reader ─────────────────────────────────────────────────────────

pub struct SegmentReader {
    #[allow(dead_code)]
    segment_dir: PathBuf,
    footer: Footer,
    pub doc_records: Vec<DocRecord>,
    pub inverted_index: HashMap<String, Vec<Posting>>,
    pub filter_store: FilterStore,
    pub vector_store: VectorStore,
    pub hnsw_map: Option<HnswMap<CosinePoint, u32>>,
}

impl SegmentReader {
    pub fn open(segment_dir: PathBuf) -> Result<Self, KoshaError> {
        let vs = Self::read_vectors(&segment_dir)?;
        let hm = build_hnsw(&vs.vectors).map(|(m, _)| m);
        Ok(Self {
            segment_dir: segment_dir.clone(),
            footer: Self::read_footer(&segment_dir)?,
            doc_records: Self::read_doc_store(&segment_dir)?,
            inverted_index: Self::read_inverted_index(&segment_dir)?,
            filter_store: Self::read_filters(&segment_dir)?,
            vector_store: vs,
            hnsw_map: hm,
        })
    }

    pub fn footer(&self) -> &Footer {
        &self.footer
    }
    pub fn doc_count(&self) -> u32 {
        self.footer.doc_count
    }
    pub fn avg_field_length(&self) -> f64 {
        self.footer.avg_field_length
    }
    pub fn bm25_params(&self) -> &Bm25Params {
        &self.footer.bm25_params
    }

    pub fn postings(&self, term: &str) -> Option<&[Posting]> {
        self.inverted_index.get(term).map(|v| v.as_slice())
    }

    pub fn doc_record(&self, doc_seq: u32) -> Option<&DocRecord> {
        self.doc_records.get(doc_seq as usize)
    }

    pub fn contains_term(&self, term: &str) -> bool {
        self.inverted_index.contains_key(term)
    }

    pub fn all_terms(&self) -> Vec<&str> {
        let mut terms: Vec<&String> = self.inverted_index.keys().collect();
        terms.sort();
        terms.into_iter().map(|s| s.as_str()).collect()
    }

    fn read_footer(segment_dir: &Path) -> Result<Footer, KoshaError> {
        let json = fs::read_to_string(segment_dir.join("footer.json"))?;
        Ok(serde_json::from_str(&json)?)
    }

    fn read_doc_store(segment_dir: &Path) -> Result<Vec<DocRecord>, KoshaError> {
        let data = fs::read(segment_dir.join("doc_store.bin"))?;
        let mut cursor = &data[..];
        let mut records = Vec::new();
        if cursor.len() < 4 {
            return Ok(records);
        }
        let doc_count = read_u32_le(&mut cursor);
        for doc_seq in 0..doc_count {
            let id_len = read_u32_le(&mut cursor) as usize;
            let id_bytes = read_bytes(&mut cursor, id_len);
            let doc_id = DocumentId(String::from_utf8_lossy(id_bytes).to_string());
            let field_length = read_u32_le(&mut cursor);
            let field_count = read_u32_le(&mut cursor);
            let mut fields = Vec::with_capacity(field_count as usize);
            for _ in 0..field_count {
                let name_len = read_u32_le(&mut cursor) as usize;
                let name_bytes = read_bytes(&mut cursor, name_len);
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
                cursor = &cursor[1..];
                let val_len = read_u64_le(&mut cursor) as usize;
                let val_bytes = read_bytes(&mut cursor, val_len);
                let value = String::from_utf8_lossy(val_bytes).to_string();
                fields.push(Field {
                    name,
                    field_type,
                    value,
                });
            }
            records.push(DocRecord {
                doc_id,
                doc_seq,
                field_length,
                fields,
            });
        }
        Ok(records)
    }

    fn read_inverted_index(
        segment_dir: &Path,
    ) -> Result<HashMap<String, Vec<Posting>>, KoshaError> {
        let data = fs::read(segment_dir.join("inverted.idx"))?;
        let mut cursor = &data[..];
        let mut index = HashMap::new();
        if cursor.len() < 4 {
            return Ok(index);
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
        Ok(index)
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

    fn read_filters(segment_dir: &Path) -> Result<FilterStore, KoshaError> {
        let path = segment_dir.join("filters.bin");
        if !path.exists() {
            return Ok(FilterStore::default());
        }
        let data = fs::read(&path)?;
        let mut cursor = &data[..];
        let mut store = FilterStore::default();
        if cursor.len() < 4 {
            return Ok(store);
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
        Ok(store)
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
}
