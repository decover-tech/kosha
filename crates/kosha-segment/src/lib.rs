//! Segment format — the immutable, self-contained unit of durable storage
//! (DESIGN.md §6.2, implementation plan Epic 2).
//!
//! A segment is a directory on disk (or prefix in S3) containing:
//! - `doc_store.bin`  — serialized [`DocRecord`] entries
//! - `inverted.idx`   — binary inverted index (term → postings list)
//! - `filters.bin`    — reserved for filter column data (Phase 2)
//! - `footer.json`    — JSON [`Footer`] metadata
//!
//! All integer values are stored in little-endian format.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use kosha_core::{
    Bm25Params, DocRecord, DocumentId, Field, Footer, KoshaError, Posting, SegmentId,
};

// ─── Segment writer ─────────────────────────────────────────────────────────

/// Builds a segment directory on disk.
pub struct SegmentWriter {
    segment_id: SegmentId,
    output_dir: PathBuf,

    /// Accumulated doc records, in insertion order.
    doc_records: Vec<DocRecord>,

    /// Inverted index: term → postings list.
    /// doc_id values are the 0-based index into doc_records.
    inverted_index: HashMap<String, Vec<Posting>>,

    /// Total number of tokens across all documents.
    total_field_length: u64,
}

impl SegmentWriter {
    /// Create a new writer that will write into `output_dir`.
    pub fn new(segment_id: SegmentId, output_dir: PathBuf) -> Self {
        Self {
            segment_id,
            output_dir,
            doc_records: Vec::new(),
            inverted_index: HashMap::new(),
            total_field_length: 0,
        }
    }

    /// Add a document to the segment being built.
    /// The document's text is tokenized on whitespace/punctuation and added
    /// to the in-memory inverted index.
    pub fn add_document(&mut self, doc_id: DocumentId, fields: Vec<Field>) {
        let doc_seq = self.doc_records.len() as u32;

        let mut field_length: u32 = 0;

        for field in &fields {
            let tokens = tokenize(&field.text);
            field_length += tokens.len() as u32;

            for token in tokens {
                let postings = self.inverted_index.entry(token).or_default();
                if let Some(last) = postings.last_mut() {
                    if last.doc_id == doc_seq {
                        last.term_frequency += 1;
                        continue;
                    }
                }
                postings.push(Posting {
                    doc_id: doc_seq,
                    term_frequency: 1,
                });
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

    /// Flush all accumulated data to disk and return the written [`Footer`].
    pub fn finalize(self, bm25_params: Bm25Params) -> Result<Footer, KoshaError> {
        fs::create_dir_all(&self.output_dir)?;

        self.write_doc_store()?;
        self.write_inverted_index()?;
        let footer = self.write_footer(bm25_params)?;

        Ok(footer)
    }

    fn doc_store_path(&self) -> PathBuf {
        self.output_dir.join("doc_store.bin")
    }

    fn inverted_index_path(&self) -> PathBuf {
        self.output_dir.join("inverted.idx")
    }

    fn footer_path(&self) -> PathBuf {
        self.output_dir.join("footer.json")
    }

    fn write_doc_store(&self) -> Result<(), KoshaError> {
        let path = self.doc_store_path();
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

                let text_bytes = field.text.as_bytes();
                buf.extend_from_slice(&(text_bytes.len() as u64).to_le_bytes());
                buf.extend_from_slice(text_bytes);
            }
        }

        fs::write(&path, &buf)?;
        Ok(())
    }

    fn write_inverted_index(&self) -> Result<(), KoshaError> {
        let path = self.inverted_index_path();

        // Sort terms for deterministic output.
        let mut terms: Vec<&String> = self.inverted_index.keys().collect();
        terms.sort();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(terms.len() as u32).to_le_bytes());

        for term_str in terms {
            let postings = &self.inverted_index[term_str];
            let term_bytes = term_str.as_bytes();

            buf.extend_from_slice(&(term_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(term_bytes);

            let df = postings.len() as u32;
            buf.extend_from_slice(&df.to_le_bytes());

            buf.extend_from_slice(&(postings.len() as u32).to_le_bytes());
            for posting in postings {
                buf.extend_from_slice(&posting.doc_id.to_le_bytes());
                buf.extend_from_slice(&posting.term_frequency.to_le_bytes());
            }
        }

        fs::write(&path, &buf)?;
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
        fs::write(self.footer_path(), json.as_bytes())?;
        Ok(footer)
    }
}

// ─── Segment reader ─────────────────────────────────────────────────────────

/// Reads a segment directory from disk.
pub struct SegmentReader {
    #[allow(dead_code)]
    segment_dir: PathBuf,
    footer: Footer,

    /// Doc records indexed by doc_seq.
    doc_records: Vec<DocRecord>,

    /// Inverted index: term → postings list.
    inverted_index: HashMap<String, Vec<Posting>>,
}

impl SegmentReader {
    /// Open and fully load a segment from `segment_dir`.
    pub fn open(segment_dir: PathBuf) -> Result<Self, KoshaError> {
        let footer = Self::read_footer(&segment_dir)?;
        let doc_records = Self::read_doc_store(&segment_dir)?;
        let inverted_index = Self::read_inverted_index(&segment_dir)?;

        Ok(Self {
            segment_dir,
            footer,
            doc_records,
            inverted_index,
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

    /// Get postings for a term, if present.
    pub fn postings(&self, term: &str) -> Option<&[Posting]> {
        self.inverted_index.get(term).map(|v| v.as_slice())
    }

    /// Get the doc record for a local doc_seq.
    pub fn doc_record(&self, doc_seq: u32) -> Option<&DocRecord> {
        self.doc_records.get(doc_seq as usize)
    }

    /// Check whether a term exists in the index.
    pub fn contains_term(&self, term: &str) -> bool {
        self.inverted_index.contains_key(term)
    }

    /// Iterate over all terms in the index.
    pub fn terms(&self) -> impl Iterator<Item = &str> {
        let mut terms: Vec<&String> = self.inverted_index.keys().collect();
        terms.sort();
        terms.into_iter().map(|s| s.as_str())
    }

    fn read_footer(segment_dir: &Path) -> Result<Footer, KoshaError> {
        let path = segment_dir.join("footer.json");
        let json = fs::read_to_string(&path)?;
        Ok(serde_json::from_str(&json)?)
    }

    fn read_doc_store(segment_dir: &Path) -> Result<Vec<DocRecord>, KoshaError> {
        let path = segment_dir.join("doc_store.bin");
        let data = fs::read(&path)?;
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

                let text_len = read_u64_le(&mut cursor) as usize;
                let text_bytes = read_bytes(&mut cursor, text_len);
                let text = String::from_utf8_lossy(text_bytes).to_string();

                fields.push(Field { name, text });
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

    fn read_inverted_index(segment_dir: &Path) -> Result<HashMap<String, Vec<Posting>>, KoshaError> {
        let path = segment_dir.join("inverted.idx");
        let data = fs::read(&path)?;
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
                postings.push(Posting {
                    doc_id,
                    term_frequency,
                });
            }

            index.insert(term, postings);
        }

        Ok(index)
    }
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

fn read_bytes<'a>(cursor: &mut &'a [u8], len: usize) -> &'a [u8] {
    let result = &cursor[..len];
    *cursor = &cursor[len..];
    result
}

// ─── Tokenizer ──────────────────────────────────────────────────────────────

/// Simple tokenizer: split on whitespace and punctuation, lowercase.
pub fn tokenize(text: &str) -> Vec<String> {
    text.split_whitespace()
        .flat_map(|word| {
            // Strip leading/trailing punctuation.
            let word = word.trim_matches(|c: char| c.is_ascii_punctuation());
            if word.is_empty() {
                None
            } else {
                Some(word.to_lowercase())
            }
        })
        .collect()
}

/// Return a simplified ISO-8601 timestamp string (no external dep).
fn chrono_like_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    // Format as UTC ISO-8601 (no sub-second precision).
    let days_since_epoch = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    // Simple day calculation from epoch (1970-01-01).
    let (year, month, day) = days_to_date(days_since_epoch as i64);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

fn days_to_date(mut days: i64) -> (i64, i64, i64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    days += 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::{DocumentId, Field, SegmentId};

    #[test]
    fn tokenize_simple_text() {
        let tokens = tokenize("Hello World! This is a test.");
        assert_eq!(tokens, vec!["hello", "world", "this", "is", "a", "test"]);
    }

    #[test]
    fn tokenize_empty_text() {
        let tokens = tokenize("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn tokenize_punctuation_only() {
        let tokens = tokenize("!!! ??? ...");
        assert!(tokens.is_empty());
    }

    #[test]
    fn write_and_read_segment() {
        let dir = std::env::temp_dir().join("kosha-test-segment");
        let _ = fs::remove_dir_all(&dir);

        let seg_id = SegmentId("test-seg-001".into());
        let mut writer = SegmentWriter::new(seg_id.clone(), dir.clone());

        writer.add_document(
            DocumentId("doc1".into()),
            vec![Field {
                name: "title".into(),
                text: "quick brown fox".into(),
            }],
        );
        writer.add_document(
            DocumentId("doc2".into()),
            vec![Field {
                name: "title".into(),
                text: "lazy dog".into(),
            }],
        );

        let footer = writer.finalize(Bm25Params::default()).unwrap();
        assert_eq!(footer.doc_count, 2);
        assert!((footer.avg_field_length - 2.5).abs() < 1e-10);

        // Now read it back.
        let reader = SegmentReader::open(dir.clone()).unwrap();
        assert_eq!(reader.doc_count(), 2);
        assert!(reader.contains_term("quick"));
        assert!(reader.contains_term("brown"));
        assert!(reader.contains_term("fox"));
        assert!(reader.contains_term("lazy"));
        assert!(reader.contains_term("dog"));

        let postings = reader.postings("quick").unwrap();
        assert_eq!(postings.len(), 1);
        assert_eq!(postings[0].doc_id, 0);
        assert_eq!(postings[0].term_frequency, 1);

        let doc = reader.doc_record(0).unwrap();
        assert_eq!(doc.doc_id.0, "doc1");

        let doc = reader.doc_record(1).unwrap();
        assert_eq!(doc.doc_id.0, "doc2");

        // Cleanup.
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_with_multi_word_repeated_terms() {
        let dir = std::env::temp_dir().join("kosha-test-segment-repeat");
        let _ = fs::remove_dir_all(&dir);

        let seg_id = SegmentId("test-seg-002".into());
        let mut writer = SegmentWriter::new(seg_id, dir.clone());

        writer.add_document(
            DocumentId("d1".into()),
            vec![Field {
                name: "body".into(),
                text: "foo bar foo".into(),
            }],
        );

        let _footer = writer.finalize(Bm25Params::default()).unwrap();

        let reader = SegmentReader::open(dir).unwrap();
        let postings = reader.postings("foo").unwrap();
        assert_eq!(postings.len(), 1);
        assert_eq!(postings[0].term_frequency, 2);

        let postings = reader.postings("bar").unwrap();
        assert_eq!(postings[0].term_frequency, 1);
    }

    #[test]
    fn reader_returns_none_for_missing_term() {
        let dir = std::env::temp_dir().join("kosha-test-segment-missing");
        let _ = fs::remove_dir_all(&dir);

        let seg_id = SegmentId("test-seg-003".into());
        let mut writer = SegmentWriter::new(seg_id, dir.clone());
        writer.add_document(
            DocumentId("d1".into()),
            vec![Field {
                name: "t".into(),
                text: "hello".into(),
            }],
        );
        writer.finalize(Bm25Params::default()).unwrap();

        let reader = SegmentReader::open(dir).unwrap();
        assert!(reader.postings("nonexistent").is_none());
    }
}
