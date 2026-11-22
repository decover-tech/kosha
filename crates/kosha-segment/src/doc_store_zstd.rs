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
//!   After the last record comes a copy of the dictionary, read from the
//!   end of the file: `[dict][dict_len:u32][magic "KDZ1":u32]`. No offsets
//!   entry points into it, so ranged page reads never fetch it.
//! - `doc_store.offsets`: the usual header and entries (spans of the stored
//!   bytes), then a trailer `[magic "KDZ1":u32][dict_len:u32][dict]`.
//!   Readers that predate #388 stop after the entries and never look at the
//!   trailer; `doc_store.offsets` is fetched for every lazily opened
//!   segment anyway, so the dictionary needs no new file to hydrate.
//!
//! # Recovery
//!
//! `doc_store.offsets` is the fast path, not the only copy of anything:
//! frames are self-delimiting (`ZSTD_findFrameCompressedSize`), raw records
//! parse, and the dictionary has its second copy at the end of
//! `doc_store.bin`. [`rebuild_offsets`] regenerates the whole sidecar,
//! trailer included, from `doc_store.bin` alone. The reader uses it when
//! the sidecar is missing or broken and `doc_store.bin` is local (slower,
//! still correct, as for a raw doc store), and
//! `SegmentReader::backfill_offset_tables` uses it to write a new one.
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

/// Decoders kept per segment for reuse. Each holds a zstd context plus a
/// digested copy of the dictionary (roughly 100-200 KiB), and none of it
/// is charged to the postings memory ledger, so the pool stays small: a
/// burst beyond it allocates a context per decode and drops it after.
const DECODER_POOL_MAX: usize = 4;

/// Upper bound on a frame's decompressed size per stored byte. zstd's
/// densest encoding is an RLE block (a 3-byte header plus 1 byte for up to
/// 128 KiB), so a valid frame never claims more than ~32 KiB per stored
/// byte; this is twice that. A damaged header that claims more is an
/// error, not a multi-GiB allocation.
const MAX_FRAME_EXPANSION: usize = 64 * 1024;

/// Bytes of the dictionary tail copy's fixed part in `doc_store.bin`
/// (`[dict_len:u32][magic:u32]`).
const DICT_TAIL_FIXED: usize = 8;

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
    // Second copy of the dictionary, after the last record, so the
    // sidecar can be rebuilt from doc_store.bin alone (see Recovery).
    out.write_all(&dict)?;
    out.write_all(&(dict.len() as u32).to_le_bytes())?;
    out.write_all(&DOC_STORE_DICT_MAGIC.to_le_bytes())?;
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
        let cap = stored.len().saturating_mul(MAX_FRAME_EXPANSION) as u64;
        let size = match zstd::zstd_safe::get_frame_content_size(stored) {
            Ok(Some(size)) if size <= u32::MAX as u64 && size <= cap => size as usize,
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

/// The dictionary copy at the end of a compressed `doc_store.bin`, and
/// where the records end (the copy's start). `None`: no valid tail.
pub(crate) fn read_dict_tail(store: &[u8]) -> Option<(&[u8], usize)> {
    let fixed_at = store.len().checked_sub(DICT_TAIL_FIXED)?;
    let mut cursor = &store[fixed_at..];
    let dict_len = u32_of(&mut cursor)? as usize;
    if u32_of(&mut cursor)? != DOC_STORE_DICT_MAGIC {
        return None;
    }
    let records_end = fixed_at.checked_sub(dict_len)?;
    if records_end < 4 {
        return None;
    }
    Some((&store[records_end..fixed_at], records_end))
}

/// Regenerate a compressed segment's `doc_store.offsets` (entries and
/// dictionary trailer) from its `doc_store.bin` bytes alone. Errors when
/// the file is not a complete compressed doc store of `expected_doc_count`
/// records whose dictionary is `expected_dict_len` bytes long.
pub(crate) fn rebuild_offsets(
    store: &[u8],
    expected_doc_count: u32,
    expected_dict_len: u32,
) -> Result<Vec<u8>, KoshaError> {
    let corrupt = |why: String| {
        KoshaError::CorruptSegment(format!(
            "doc_store.bin: cannot rebuild doc_store.offsets of a compressed doc store: {why}"
        ))
    };
    let mut head = store;
    let doc_count = u32_of(&mut head).ok_or_else(|| corrupt("no header".into()))?;
    if doc_count != expected_doc_count {
        return Err(corrupt(format!(
            "{doc_count} record(s), footer.json says {expected_doc_count}"
        )));
    }
    let (dict, records_end) =
        read_dict_tail(store).ok_or_else(|| corrupt("no dictionary tail".into()))?;
    if dict.len() != expected_dict_len as usize {
        return Err(corrupt(format!(
            "dictionary tail is {} bytes, footer.json says {expected_dict_len}",
            dict.len()
        )));
    }
    let decoder = DocStoreDecoder::new(dict.to_vec());
    let mut offsets = Vec::with_capacity(4 + doc_count as usize * 32 + 8 + dict.len());
    offsets.extend_from_slice(&doc_count.to_le_bytes());
    let mut pos = 4usize;
    for doc_seq in 0..doc_count {
        let rest = &store[pos.min(records_end)..records_end];
        let len = if rest.starts_with(&ZSTD_FRAME_MAGIC) {
            zstd::zstd_safe::find_frame_compressed_size(rest)
                .map_err(|e| corrupt(format!("record {doc_seq}: frame size: {e}")))?
        } else {
            let mut cursor = rest;
            crate::parse_one_doc_record(&mut cursor, doc_seq)?;
            rest.len() - cursor.len()
        };
        let stored = rest
            .get(..len)
            .ok_or_else(|| corrupt(format!("record {doc_seq} runs past the records")))?;
        let raw = decoder.decode(stored, doc_seq)?;
        let mut cursor = &raw[..];
        let record = crate::parse_one_doc_record(&mut cursor, doc_seq)?;
        if !cursor.is_empty() {
            return Err(corrupt(format!("record {doc_seq} has trailing bytes")));
        }
        let id = record.doc_id.0.as_bytes();
        offsets.extend_from_slice(&(id.len() as u32).to_le_bytes());
        offsets.extend_from_slice(id);
        offsets.extend_from_slice(&record.field_length.to_le_bytes());
        offsets.extend_from_slice(&(pos as u64).to_le_bytes());
        offsets.extend_from_slice(&(len as u32).to_le_bytes());
        pos += len;
    }
    if pos != records_end {
        return Err(corrupt(format!(
            "records end at byte {pos}, the dictionary tail starts at {records_end}"
        )));
    }
    offsets.extend_from_slice(&DOC_STORE_DICT_MAGIC.to_le_bytes());
    offsets.extend_from_slice(&(dict.len() as u32).to_le_bytes());
    offsets.extend_from_slice(dict);
    Ok(offsets)
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

    /// A frame header claiming far more content than its stored bytes
    /// could hold is refused before any allocation; a legitimately dense
    /// frame (1 MiB of zeros) is not.
    #[test]
    fn implausible_frame_content_size_is_refused() {
        let decoder = DocStoreDecoder::new(Vec::new());
        let zeros = vec![0u8; 1 << 20];
        let dense = zstd::bulk::compress(&zeros, 3).unwrap();
        assert_eq!(&*decoder.decode(&dense, 0).unwrap(), &zeros[..]);

        let frame = zstd::bulk::compress(b"hello hello hello", 3).unwrap();
        let fhd = frame[4];
        assert!(fhd & 0x20 != 0, "single-segment frame expected");
        let fcs_len = match fhd >> 6 {
            0 => 1,
            1 => 2,
            2 => 4,
            _ => 8,
        };
        assert_eq!(fhd & 0x03, 0, "no dictionary id expected");
        // Rewrite the content size as an 8-byte 3 GiB.
        let mut forged = frame[..4].to_vec();
        forged.push(fhd | 0xC0);
        forged.extend_from_slice(&(3u64 << 30).to_le_bytes());
        forged.extend_from_slice(&frame[5 + fcs_len..]);
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&forged).unwrap(),
            Some(3u64 << 30)
        );
        let err = decoder.decode(&forged, 0).unwrap_err().to_string();
        assert!(err.contains("frame content size"), "{err}");
    }

    #[test]
    fn dict_tail_parses_and_rejects_garbage() {
        let mut store = 2u32.to_le_bytes().to_vec();
        store.extend_from_slice(b"rec");
        store.extend_from_slice(b"dict!");
        store.extend_from_slice(&5u32.to_le_bytes());
        store.extend_from_slice(&DOC_STORE_DICT_MAGIC.to_le_bytes());
        assert_eq!(read_dict_tail(&store), Some((&b"dict!"[..], 7)));
        assert!(read_dict_tail(&store[..store.len() - 1]).is_none());
        assert!(read_dict_tail(&[0u8; 6]).is_none());
        let n = store.len();
        let mut huge = store.clone();
        huge[n - 8..n - 4].copy_from_slice(&1000u32.to_le_bytes());
        assert!(read_dict_tail(&huge).is_none());
    }
}
