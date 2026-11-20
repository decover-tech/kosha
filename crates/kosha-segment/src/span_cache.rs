//! Sparse span cache for segment files that are read by byte range (#245).
//!
//! A cold query materializes its result page from a handful of documents.
//! Instead of downloading the whole `doc_store.bin` (and `doc_vectors.bin`)
//! of every segment that holds a page hit, the query node fetches just the
//! byte ranges those documents occupy and persists each fetched range here,
//! so a warm repeat is served from local disk with no S3 request.
//!
//! Layout: `{segment_dir}/{file}.spans/{start:016x}-{end:016x}`, one file
//! per fetched range holding exactly the bytes `[start, end)` of `{file}`.
//! The range lives in the name, so the directory listing *is* the index and
//! there is nothing else to keep consistent. A span file whose length does
//! not match its name (a write that was interrupted, or one still being
//! written) is ignored, so a reader never parses partial bytes.
//!
//! The real file path (`{segment_dir}/{file}`) is never written partially:
//! every existence check in the hydration path treats a file there as
//! complete. The whole file, once present, always wins over spans.
//!
//! Span files live under the cache root like any other segment file, so the
//! disk cache counts them toward its byte budget and its LRU may evict any
//! of them; an evicted span is just a miss that the next query re-fetches.

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Suffix of the sparse cache directory next to a segment file.
pub const SPANS_SUFFIX: &str = ".spans";

/// `{segment_dir}/{file}.spans`.
pub fn spans_dir(segment_dir: &Path, file: &str) -> PathBuf {
    segment_dir.join(format!("{file}{SPANS_SUFFIX}"))
}

/// The file name a span `[start, end)` is stored under.
pub fn span_file_name(start: u64, end: u64) -> String {
    format!("{start:016x}-{end:016x}")
}

/// Inverse of [`span_file_name`]; `None` for anything else in the directory
/// (temp files, stray names).
pub fn parse_span_file_name(name: &str) -> Option<(u64, u64)> {
    let (start, end) = name.split_once('-')?;
    if start.len() != 16 || end.len() != 16 {
        return None;
    }
    let start = u64::from_str_radix(start, 16).ok()?;
    let end = u64::from_str_radix(end, 16).ok()?;
    (end > start).then_some((start, end))
}

/// The complete spans cached for one segment file, sorted by start.
#[derive(Debug, Default, Clone)]
pub struct SpanIndex {
    dir: PathBuf,
    spans: Vec<(u64, u64)>,
}

impl SpanIndex {
    /// List `{segment_dir}/{file}.spans`. A missing directory is an empty
    /// index. Spans whose file length disagrees with their name are left
    /// out.
    pub fn load(segment_dir: &Path, file: &str) -> Self {
        let dir = spans_dir(segment_dir, file);
        let mut spans = Vec::new();
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let Some((start, end)) = entry.file_name().to_str().and_then(parse_span_file_name)
                else {
                    continue;
                };
                if entry.metadata().is_ok_and(|m| m.len() == end - start) {
                    spans.push((start, end));
                }
            }
        }
        spans.sort_unstable();
        Self { dir, spans }
    }

    /// A cached span containing all of `[start, end)`, if any.
    pub fn covering(&self, start: u64, end: u64) -> Option<(u64, u64)> {
        // Spans may overlap (two queries fetching neighbouring ranges), so
        // any span starting at or before `start` may be the one.
        let upto = self.spans.partition_point(|&(s, _)| s <= start);
        self.spans[..upto]
            .iter()
            .rev()
            .find(|&&(_, e)| e >= end)
            .copied()
    }

    pub fn covers(&self, start: u64, end: u64) -> bool {
        start == end || self.covering(start, end).is_some()
    }

    /// Read `[start, end)` out of whichever cached span holds it. `None`
    /// when no span covers it or the span vanished (evicted) meanwhile.
    pub fn read(&self, start: u64, end: u64) -> Option<Vec<u8>> {
        if start == end {
            return Some(Vec::new());
        }
        let (s, e) = self.covering(start, end)?;
        let mut file = fs::File::open(self.dir.join(span_file_name(s, e))).ok()?;
        file.seek(SeekFrom::Start(start - s)).ok()?;
        let mut buf = vec![0u8; (end - start) as usize];
        file.read_exact(&mut buf).ok()?;
        Some(buf)
    }

    /// The cached span starting at offset 0 (a file header), read whole.
    pub fn read_prefix(&self) -> Option<Vec<u8>> {
        let &(_, end) = self
            .spans
            .iter()
            .filter(|&&(s, _)| s == 0)
            .max_by_key(|&&(_, e)| e)?;
        self.read(0, end)
    }

    pub fn spans(&self) -> &[(u64, u64)] {
        &self.spans
    }

    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    /// Bytes held on disk (overlaps counted twice, as they are stored).
    pub fn total_bytes(&self) -> u64 {
        self.spans.iter().map(|(s, e)| e - s).sum()
    }
}

/// Read `[start, start + len)` of `segment_dir/file` from local disk: the
/// whole file when it is present and at least `complete_len` bytes long,
/// otherwise the sparse span cache. `None` when neither holds the range.
///
/// The whole file is checked again after a span miss: a background
/// promotion can land the file and drop the spans between the two looks.
pub fn read_local_range(
    segment_dir: &Path,
    file: &str,
    complete_len: u64,
    start: u64,
    len: u64,
) -> Option<Vec<u8>> {
    let whole = segment_dir.join(file);
    let read_whole = || -> Option<Vec<u8>> {
        let meta = fs::metadata(&whole).ok()?;
        if meta.len() == 0 || meta.len() < complete_len {
            return None;
        }
        let mut f = fs::File::open(&whole).ok()?;
        f.seek(SeekFrom::Start(start)).ok()?;
        let mut buf = vec![0u8; len as usize];
        f.read_exact(&mut buf).ok()?;
        Some(buf)
    };
    read_whole()
        .or_else(|| SpanIndex::load(segment_dir, file).read(start, start + len))
        .or_else(read_whole)
}

/// Merge `[start, end)` ranges whose gap is at most `max_gap` bytes into
/// one fetch each — reading a little dead space between two documents is
/// cheaper than a second round trip. Input order does not matter; empty
/// ranges are dropped.
pub fn coalesce(mut ranges: Vec<(u64, u64)>, max_gap: u64) -> Vec<(u64, u64)> {
    ranges.retain(|(s, e)| e > s);
    ranges.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for (s, e) in ranges {
        match out.last_mut() {
            Some(last) if s <= last.1.saturating_add(max_gap) => last.1 = last.1.max(e),
            _ => out.push((s, e)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("kosha-span-cache-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn put(seg: &Path, file: &str, start: u64, bytes: &[u8]) {
        let dir = spans_dir(seg, file);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join(span_file_name(start, start + bytes.len() as u64)),
            bytes,
        )
        .unwrap();
    }

    #[test]
    fn name_round_trips_and_rejects_junk() {
        let name = span_file_name(10, 4096);
        assert_eq!(parse_span_file_name(&name), Some((10, 4096)));
        assert_eq!(parse_span_file_name("tmp.123"), None);
        assert_eq!(parse_span_file_name(&span_file_name(5, 5)), None);
    }

    #[test]
    fn coalesce_merges_within_gap_only() {
        let merged = coalesce(vec![(100, 200), (0, 50), (230, 300), (10_000, 10_010)], 32);
        assert_eq!(merged, vec![(0, 50), (100, 300), (10_000, 10_010)]);
        assert_eq!(coalesce(vec![(0, 10), (5, 8), (9, 9)], 0), vec![(0, 10)]);
    }

    #[test]
    fn index_serves_contained_ranges_and_ignores_torn_spans() {
        let seg = tmp("index");
        put(&seg, "doc_store.bin", 100, &[7u8; 50]);
        // A torn write: the name claims 100 bytes, the file holds 3.
        let dir = spans_dir(&seg, "doc_store.bin");
        fs::write(dir.join(span_file_name(1000, 1100)), b"abc").unwrap();
        let idx = SpanIndex::load(&seg, "doc_store.bin");
        assert_eq!(idx.spans(), &[(100, 150)]);
        assert!(idx.covers(110, 150));
        assert!(!idx.covers(90, 120));
        assert!(!idx.covers(1000, 1010));
        assert_eq!(idx.read(120, 125), Some(vec![7u8; 5]));
        assert_eq!(idx.total_bytes(), 50);
        let _ = fs::remove_dir_all(&seg);
    }

    #[test]
    fn whole_file_wins_over_spans() {
        let seg = tmp("whole");
        put(&seg, "doc_store.bin", 0, b"SPAN");
        assert_eq!(
            read_local_range(&seg, "doc_store.bin", 4, 0, 4).as_deref(),
            Some(&b"SPAN"[..])
        );
        fs::write(seg.join("doc_store.bin"), b"FILE").unwrap();
        assert_eq!(
            read_local_range(&seg, "doc_store.bin", 4, 0, 4).as_deref(),
            Some(&b"FILE"[..])
        );
        // A short (truncated) whole file is not trusted.
        fs::write(seg.join("doc_store.bin"), b"FI").unwrap();
        assert_eq!(
            read_local_range(&seg, "doc_store.bin", 4, 0, 4).as_deref(),
            Some(&b"SPAN"[..])
        );
        let _ = fs::remove_dir_all(&seg);
    }
}
