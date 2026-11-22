//! Compressed document store (research #388, experimental, default off).
//!
//! `doc_store.bin` keeps its raw layout by default. With
//! `KOSHA_DOC_STORE_COMPRESSION=zstd`, a writer stores each record as its
//! own zstd frame, compressed with a dictionary trained on a sample of the
//! segment's own records. Per-record frames keep random access: the
//! `doc_store.offsets` entries give each frame's `(offset, length)` exactly
//! as they give a raw record's, so the ranged page reads of #245
//! (`kosha-server/src/doc_spans.rs`) still fetch only the page's records,
//! now fewer bytes each.
//!
//! # Layout (only when `Footer::doc_store_compression` is `Some`)
//!
//! - `doc_store.bin`: `[doc_count:u32]` then one stored record per
//!   document, back to back. A stored record is either a zstd frame (it
//!   starts with the zstd magic `28 B5 2F FD` and records its content size)
//!   or the raw record, kept when compressing it would not shrink it. A
//!   raw record starts with its id length as a `u32`, which can never equal
//!   the zstd magic (that would be a 4 GiB id), so the two never collide.
//! - `doc_store.offsets`: the usual header and entries (spans of the stored
//!   bytes), then a trailer `[magic "KDZ1":u32][dict_len:u32][dict]`.
//!   Readers that predate #388 stop after the entries and never look at the
//!   trailer; `doc_store.offsets` is fetched for every lazily opened
//!   segment anyway, so the dictionary needs no new file to hydrate.
//!
//! The footer, not the bytes, decides the format: a reader decompresses
//! only when `Footer::doc_store_compression` names a codec it knows, and
//! refuses to open a segment naming one it does not.
//!
//! Vectors never sit in a compressed doc store: a writer with compression
//! on always moves them to `doc_vectors.bin` (#243), whatever
//! `KOSHA_VECTOR_STORE` says.

use std::borrow::Cow;
use std::fs;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use kosha_core::{DocStoreCompression, KoshaError};

/// Magic of the dictionary trailer in `doc_store.offsets` ("KDZ1").
pub const DOC_STORE_DICT_MAGIC: u32 = u32::from_le_bytes(*b"KDZ1");

/// The zstd frame magic, little-endian, as a stored record starts with it.
const ZSTD_FRAME_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Fewer records than this train no dictionary (plain zstd per record):
/// the trainer needs a spread of samples to find anything shared.
const MIN_TRAIN_RECORDS: usize = 16;

/// Longest prefix of one record fed to the trainer.
const MAX_SAMPLE_RECORD_BYTES: usize = 128 * 1024;

/// Decoders kept per segment for reuse (each holds a digested dictionary).
const DECODER_POOL_MAX: usize = 32;

/// `KOSHA_DOC_STORE_COMPRESSION`: whether new segments compress
/// `doc_store.bin` (research #388).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocStoreCompressionMode {
    /// Unset (the default): fresh segments are raw; a merge keeps its
    /// inputs' format (compressed output if any input is compressed).
    Auto,
    /// `none` / `off`: always raw, merges included (the way back).
    Off,
    /// `zstd`: always compressed.
    Zstd,
}

impl DocStoreCompressionMode {
    pub fn from_env() -> Self {
        static MODE: OnceLock<DocStoreCompressionMode> = OnceLock::new();
        *MODE.get_or_init(|| {
            Self::parse(std::env::var("KOSHA_DOC_STORE_COMPRESSION").ok().as_deref())
        })
    }

    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") | Some("auto") => Self::Auto,
            Some("none") | Some("off") | Some("raw") => Self::Off,
            Some("zstd") => Self::Zstd,
            Some(other) => {
                eprintln!(
                    "WARN: KOSHA_DOC_STORE_COMPRESSION={other} not auto|none|zstd — using auto"
                );
                Self::Auto
            }
        }
    }
}

/// Tuning of a compressed doc store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DocStoreZstdConfig {
    /// zstd level (`KOSHA_DOC_STORE_ZSTD_LEVEL`, default 3).
    pub level: i32,
    /// Dictionary size to train (`KOSHA_DOC_STORE_ZSTD_DICT_BYTES`,
    /// default 64 KiB); `0` compresses each record without a dictionary.
    pub dict_bytes: usize,
    /// Record bytes sampled for training
    /// (`KOSHA_DOC_STORE_ZSTD_SAMPLE_BYTES`, default 100x the dictionary,
    /// at most 16 MiB), taken at an even stride across the segment.
    pub sample_bytes: usize,
}

impl Default for DocStoreZstdConfig {
    fn default() -> Self {
        Self::with_dict_bytes(3, 64 * 1024)
    }
}

impl DocStoreZstdConfig {
    /// `level` and `dict_bytes` with the default sample size for them.
    pub fn with_dict_bytes(level: i32, dict_bytes: usize) -> Self {
        Self {
            level,
            dict_bytes,
            sample_bytes: (dict_bytes * 100).min(16 * 1024 * 1024),
        }
    }

    pub fn from_env() -> Self {
        static CONFIG: OnceLock<DocStoreZstdConfig> = OnceLock::new();
        *CONFIG.get_or_init(|| {
            let var = |key: &str| std::env::var(key).ok();
            Self::parse(
                var("KOSHA_DOC_STORE_ZSTD_LEVEL").as_deref(),
                var("KOSHA_DOC_STORE_ZSTD_DICT_BYTES").as_deref(),
                var("KOSHA_DOC_STORE_ZSTD_SAMPLE_BYTES").as_deref(),
            )
        })
    }

    pub fn parse(
        level: Option<&str>,
        dict_bytes: Option<&str>,
        sample_bytes: Option<&str>,
    ) -> Self {
        let default = Self::default();
        let level = level
            .and_then(|v| v.trim().parse::<i32>().ok())
            .filter(|l| (1..=22).contains(l))
            .unwrap_or(default.level);
        let dict_bytes = dict_bytes
            .and_then(|v| v.trim().parse::<usize>().ok())
            .map(|d| d.min(4 * 1024 * 1024))
            .unwrap_or(default.dict_bytes);
        let mut config = Self::with_dict_bytes(level, dict_bytes);
        if let Some(sample) = sample_bytes.and_then(|v| v.trim().parse::<usize>().ok()) {
            config.sample_bytes = sample.min(256 * 1024 * 1024);
        }
        config
    }
}

/// One `doc_store.offsets` entry, borrowed from the file's bytes.
struct OffsetsEntry<'a> {
    id: &'a [u8],
    field_length: u32,
    offset: u64,
    length: u32,
}

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

/// Every entry of a raw-layout `doc_store.offsets` buffer.
fn parse_offsets_entries(data: &[u8]) -> Option<Vec<OffsetsEntry<'_>>> {
    let mut cursor = data;
    let count = u32_of(&mut cursor)? as usize;
    let mut entries = Vec::with_capacity(count.min(cursor.len() / 20));
    for _ in 0..count {
        let id_len = u32_of(&mut cursor)? as usize;
        let id = take(&mut cursor, id_len)?;
        let field_length = u32_of(&mut cursor)?;
        let offset = u64_of(&mut cursor)?;
        let length = u32_of(&mut cursor)?;
        entries.push(OffsetsEntry {
            id,
            field_length,
            offset,
            length,
        });
    }
    Some(entries)
}

/// The dictionary in a `doc_store.offsets` trailer (the bytes after the
/// entries). `None`: no trailer, or a malformed one.
pub(crate) fn parse_dict_trailer(trailer: &[u8]) -> Option<Vec<u8>> {
    let mut cursor = trailer;
    if u32_of(&mut cursor)? != DOC_STORE_DICT_MAGIC {
        return None;
    }
    let len = u32_of(&mut cursor)? as usize;
    let dict = take(&mut cursor, len)?;
    cursor.is_empty().then(|| dict.to_vec())
}

/// Train a dictionary on an even-stride sample of the raw records. Empty
/// when disabled, when the segment is too small, or when training fails
/// (the store is then plain per-record zstd, still correct).
fn train_dictionary(
    raw_path: &Path,
    entries: &[OffsetsEntry<'_>],
    config: DocStoreZstdConfig,
) -> Result<Vec<u8>, KoshaError> {
    if config.dict_bytes == 0 || entries.len() < MIN_TRAIN_RECORDS {
        return Ok(Vec::new());
    }
    let total: u64 = entries.iter().map(|e| e.length as u64).sum();
    let budget = config.sample_bytes.max(config.dict_bytes) as u64;
    let stride = (total / budget.max(1)).max(1) as usize;
    let mut file = fs::File::open(raw_path)?;
    let mut samples = Vec::with_capacity(budget.min(total) as usize);
    let mut sizes = Vec::new();
    for entry in entries.iter().step_by(stride) {
        if samples.len() as u64 >= budget {
            break;
        }
        let len = (entry.length as usize).min(MAX_SAMPLE_RECORD_BYTES);
        if len == 0 {
            continue;
        }
        let start = samples.len();
        samples.resize(start + len, 0);
        file.seek(SeekFrom::Start(entry.offset))?;
        file.read_exact(&mut samples[start..])?;
        sizes.push(len);
    }
    if sizes.len() < MIN_TRAIN_RECORDS {
        return Ok(Vec::new());
    }
    match zstd::dict::from_continuous(&samples, &sizes, config.dict_bytes) {
        Ok(dict) => Ok(dict),
        Err(e) => {
            eprintln!(
                "WARN: {}: zstd dictionary training failed ({e}); compressing records \
                 without a dictionary",
                raw_path.display()
            );
            Ok(Vec::new())
        }
    }
}

/// Rewrite a finished raw `doc_store.bin` (`raw_path`, with its offsets
/// `raw_offsets`) as a compressed one at `out_path`. Returns the new
/// `doc_store.offsets` bytes (dictionary trailer included) and the footer
/// entry describing it.
pub(crate) fn compress_doc_store(
    raw_path: &Path,
    out_path: &Path,
    raw_offsets: &[u8],
    config: DocStoreZstdConfig,
) -> Result<(Vec<u8>, DocStoreCompression), KoshaError> {
    let corrupt = || {
        KoshaError::CorruptSegment(format!(
            "{}: doc store offsets being compressed are malformed",
            raw_path.display()
        ))
    };
    let entries = parse_offsets_entries(raw_offsets).ok_or_else(corrupt)?;
    let dict = train_dictionary(raw_path, &entries, config)?;
    let mut compressor = if dict.is_empty() {
        zstd::bulk::Compressor::new(config.level)?
    } else {
        zstd::bulk::Compressor::with_dictionary(config.level, &dict)?
    };
    // The dictionary is per segment, so its id would only cost bytes; the
    // frame keeps its content size, which the reader sizes its buffer by.
    compressor.set_parameter(zstd::zstd_safe::CParameter::DictIdFlag(false))?;
    compressor.set_parameter(zstd::zstd_safe::CParameter::ContentSizeFlag(true))?;

    let mut input = BufReader::new(fs::File::open(raw_path)?);
    let mut out = BufWriter::new(fs::File::create(out_path)?);
    out.write_all(&raw_offsets[0..4])?;
    let mut offsets = Vec::with_capacity(raw_offsets.len() + 8 + dict.len());
    offsets.extend_from_slice(&raw_offsets[0..4]);
    let mut written = 4u64;
    let mut position = 0u64;
    let mut record = Vec::new();
    let mut packed = Vec::new();
    for entry in &entries {
        if entry.offset != position {
            input.seek(SeekFrom::Start(entry.offset))?;
        }
        record.resize(entry.length as usize, 0);
        input.read_exact(&mut record)?;
        position = entry.offset + entry.length as u64;
        packed.clear();
        packed.reserve(zstd::zstd_safe::compress_bound(record.len()));
        compressor.compress_to_buffer(&record[..], &mut packed)?;
        let stored: &[u8] = if packed.len() < record.len() {
            &packed
        } else {
            if record.starts_with(&ZSTD_FRAME_MAGIC) {
                return Err(corrupt());
            }
            &record
        };
        out.write_all(stored)?;
        offsets.extend_from_slice(&(entry.id.len() as u32).to_le_bytes());
        offsets.extend_from_slice(entry.id);
        offsets.extend_from_slice(&entry.field_length.to_le_bytes());
        offsets.extend_from_slice(&written.to_le_bytes());
        offsets.extend_from_slice(&(stored.len() as u32).to_le_bytes());
        written += stored.len() as u64;
    }
    out.flush()?;
    offsets.extend_from_slice(&DOC_STORE_DICT_MAGIC.to_le_bytes());
    offsets.extend_from_slice(&(dict.len() as u32).to_le_bytes());
    offsets.extend_from_slice(&dict);
    Ok((
        offsets,
        DocStoreCompression {
            codec: DocStoreCompression::ZSTD_DICT.to_string(),
            level: config.level,
            dict_bytes: dict.len() as u32,
        },
    ))
}

/// Decodes one segment's stored records: holds its dictionary and a small
/// pool of zstd contexts with it already loaded.
pub(crate) struct DocStoreDecoder {
    dict: Vec<u8>,
    pool: Mutex<Vec<zstd::bulk::Decompressor<'static>>>,
}

impl DocStoreDecoder {
    pub(crate) fn new(dict: Vec<u8>) -> Self {
        Self {
            dict,
            pool: Mutex::new(Vec::new()),
        }
    }

    /// Bytes the decoder keeps resident (the dictionary; pooled contexts
    /// are not counted).
    pub(crate) fn dict_len(&self) -> usize {
        self.dict.len()
    }

    /// The raw record for one stored record's bytes.
    pub(crate) fn decode<'a>(
        &self,
        stored: &'a [u8],
        doc_seq: u32,
    ) -> Result<Cow<'a, [u8]>, KoshaError> {
        if !stored.starts_with(&ZSTD_FRAME_MAGIC) {
            return Ok(Cow::Borrowed(stored));
        }
        let corrupt = |why: String| {
            KoshaError::CorruptSegment(format!(
                "doc_store.bin: compressed record {doc_seq} is malformed: {why}"
            ))
        };
        let size = match zstd::zstd_safe::get_frame_content_size(stored) {
            Ok(Some(size)) if size <= u32::MAX as u64 => size as usize,
            other => return Err(corrupt(format!("frame content size {other:?}"))),
        };
        let pooled = self.pool.lock().unwrap().pop();
        let mut decompressor = match pooled {
            Some(d) => d,
            None if self.dict.is_empty() => zstd::bulk::Decompressor::new()?,
            None => zstd::bulk::Decompressor::with_dictionary(&self.dict)?,
        };
        let mut out = Vec::with_capacity(size);
        let result = decompressor.decompress_to_buffer(stored, &mut out);
        {
            let mut pool = self.pool.lock().unwrap();
            if pool.len() < DECODER_POOL_MAX {
                pool.push(decompressor);
            }
        }
        match result {
            Ok(n) if n == size => Ok(Cow::Owned(out)),
            Ok(n) => Err(corrupt(format!("{n} bytes, frame says {size}"))),
            Err(e) => Err(corrupt(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_parses_with_auto_default() {
        assert_eq!(
            DocStoreCompressionMode::parse(None),
            DocStoreCompressionMode::Auto
        );
        assert_eq!(
            DocStoreCompressionMode::parse(Some("")),
            DocStoreCompressionMode::Auto
        );
        assert_eq!(
            DocStoreCompressionMode::parse(Some(" ZSTD ")),
            DocStoreCompressionMode::Zstd
        );
        assert_eq!(
            DocStoreCompressionMode::parse(Some("none")),
            DocStoreCompressionMode::Off
        );
        assert_eq!(
            DocStoreCompressionMode::parse(Some("off")),
            DocStoreCompressionMode::Off
        );
        assert_eq!(
            DocStoreCompressionMode::parse(Some("lz4")),
            DocStoreCompressionMode::Auto
        );
    }

    #[test]
    fn config_parses_and_clamps() {
        assert_eq!(
            DocStoreZstdConfig::parse(None, None, None),
            DocStoreZstdConfig::default()
        );
        let c = DocStoreZstdConfig::parse(Some("9"), Some("16384"), None);
        assert_eq!(
            (c.level, c.dict_bytes, c.sample_bytes),
            (9, 16384, 1_638_400)
        );
        let c = DocStoreZstdConfig::parse(Some("99"), Some("0"), Some("1000"));
        assert_eq!((c.level, c.dict_bytes, c.sample_bytes), (3, 0, 1000));
    }

    #[test]
    fn trailer_round_trips_and_rejects_garbage() {
        let mut t = DOC_STORE_DICT_MAGIC.to_le_bytes().to_vec();
        t.extend_from_slice(&3u32.to_le_bytes());
        t.extend_from_slice(b"abc");
        assert_eq!(parse_dict_trailer(&t).as_deref(), Some(&b"abc"[..]));
        assert!(parse_dict_trailer(&t[..t.len() - 1]).is_none());
        assert!(parse_dict_trailer(&[]).is_none());
        let mut extra = t.clone();
        extra.push(0);
        assert!(parse_dict_trailer(&extra).is_none());
    }

    #[test]
    fn raw_records_pass_through_and_bad_frames_error() {
        let decoder = DocStoreDecoder::new(Vec::new());
        let raw = [5u8, 0, 0, 0, b'h'];
        assert!(matches!(decoder.decode(&raw, 0).unwrap(), Cow::Borrowed(_)));
        let frame = zstd::bulk::compress(b"hello hello hello", 3).unwrap();
        assert_eq!(&*decoder.decode(&frame, 0).unwrap(), b"hello hello hello");
        assert!(decoder.decode(&frame[..frame.len() - 2], 0).is_err());
    }
}
