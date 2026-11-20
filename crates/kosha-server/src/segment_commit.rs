//! Segment commit marker (`_COMMIT`) — the object that makes an uploaded
//! segment count as durable in S3 (issue #231).
//!
//! A segment is a directory of independent S3 objects, and each `PutObject`
//! is atomic on its own, but a segment upload is not: a process that dies
//! after `doc_store.bin` and before `inverted.idx` leaves a prefix that
//! *looks* like a segment. The old durability check ("any object exists
//! under the prefix") treated that as durable, so the boot-time
//! reconciliation sweep skipped re-uploading it.
//!
//! The fix is the usual commit-record pattern: upload every data file
//! first, then write `_COMMIT` last. It lists each file with its size and
//! CRC32. A segment is durable only when `_COMMIT` exists and every file it
//! lists is present at the recorded size. Readers that fetch whole files can
//! check the CRC to detect corruption.
//!
//! Segments uploaded before this marker existed have no `_COMMIT`. Re-uploading
//! all of them at boot would be prohibitively expensive (staging alone has
//! thousands), so they fall back to a weaker check: every file in
//! `REQUIRED_SEGMENT_FILES` is present and non-empty. That is still strictly
//! stronger than the old "any object" check.

use serde::{Deserialize, Serialize};

/// Object name of the commit marker inside a segment prefix. The leading
/// underscore keeps it out of every segment-file name the readers know.
pub const COMMIT_FILE: &str = "_COMMIT";

/// Current `_COMMIT` format version.
pub const COMMIT_FORMAT: u32 = 1;

/// Files that must all be present for a segment to be usable by the searcher.
/// `vector.idx` is intentionally excluded: lexical queries never load it, so
/// its absence must not make a segment count as incomplete.
///
/// The filter index is required too, but in one of two formats (#244):
/// see [`FILTER_INDEX_FILES`].
pub const REQUIRED_SEGMENT_FILES: [&str; 3] = ["footer.json", "doc_store.bin", "inverted.idx"];

/// A segment's filter index: the row-oriented `filters.bin`, or the
/// columnar `filters.toc` (#244), whose column files a `_COMMIT` lists like
/// any other file. A commit must list one of these. A segment without a
/// `_COMMIT` predates the columnar format, so it must have `filters.bin`.
pub const FILTER_INDEX_FILES: [&str; 2] = ["filters.bin", "filters.toc"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitFile {
    pub name: String,
    pub size: u64,
    /// CRC32 (IEEE) of the full file contents.
    pub crc32: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentCommit {
    pub format: u32,
    pub files: Vec<CommitFile>,
}

impl SegmentCommit {
    pub fn new(mut files: Vec<CommitFile>) -> Self {
        files.sort_by(|a, b| a.name.cmp(&b.name));
        Self {
            format: COMMIT_FORMAT,
            files,
        }
    }

    pub fn entry(name: &str, data: &[u8]) -> CommitFile {
        CommitFile {
            name: name.to_string(),
            size: data.len() as u64,
            crc32: crc32fast::hash(data),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("SegmentCommit serializes")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let c: Self =
            serde_json::from_slice(bytes).map_err(|e| format!("invalid {COMMIT_FILE}: {e}"))?;
        if c.format != COMMIT_FORMAT {
            return Err(format!(
                "unsupported {COMMIT_FILE} format {} (expected {COMMIT_FORMAT})",
                c.format
            ));
        }
        Ok(c)
    }

    /// Insert or replace one file's entry. Used when an admin backfill
    /// rewrites a single file (e.g. `footer.json`) in an already-committed
    /// segment.
    pub fn upsert(&mut self, file: CommitFile) {
        match self.files.iter_mut().find(|f| f.name == file.name) {
            Some(existing) => *existing = file,
            None => {
                self.files.push(file);
                self.files.sort_by(|a, b| a.name.cmp(&b.name));
            }
        }
    }

    pub fn get(&self, name: &str) -> Option<&CommitFile> {
        self.files.iter().find(|f| f.name == name)
    }

    /// Check one fetched file against its recorded entry. Files the commit
    /// does not list are accepted (`Ok`), because there is nothing to check
    /// them against.
    pub fn verify(&self, name: &str, data: &[u8]) -> Result<(), String> {
        let Some(entry) = self.get(name) else {
            return Ok(());
        };
        if data.len() as u64 != entry.size {
            return Err(format!(
                "{name}: size {} != committed size {}",
                data.len(),
                entry.size
            ));
        }
        let crc = crc32fast::hash(data);
        if crc != entry.crc32 {
            return Err(format!(
                "{name}: crc32 {crc:08x} != committed {:08x}",
                entry.crc32
            ));
        }
        Ok(())
    }
}

/// Why a remote segment listing did or didn't pass the durability check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Durability {
    /// `_COMMIT` present and every listed file matches its recorded size.
    Committed,
    /// No `_COMMIT` (written before the marker existed), but every required
    /// file is present and non-empty.
    LegacyComplete,
    /// Not durable; the reason is logged.
    Incomplete(String),
}

impl Durability {
    pub fn is_durable(&self) -> bool {
        matches!(self, Self::Committed | Self::LegacyComplete)
    }
}

/// Pure durability decision over an S3 listing of one segment prefix
/// (`(basename, size)` pairs) plus the parsed `_COMMIT`, when the listing
/// contained one. `commit` is `Some(Err(_))` when `_COMMIT` was listed but
/// could not be fetched or parsed. That case is not durable: a marker we
/// cannot read proves nothing.
pub fn assess_listing(
    listing: &[(String, u64)],
    commit: Option<Result<&SegmentCommit, &str>>,
) -> Durability {
    let size_of = |name: &str| {
        listing
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, size)| *size)
    };
    match commit {
        Some(Ok(commit)) => {
            for f in &commit.files {
                match size_of(&f.name) {
                    // Deletion generations are not segment data: they are
                    // validated by the manifest's checksum, uploaded on
                    // their own, and garbage-collected once superseded
                    // (#239). A commit from before that may still list one.
                    None if kosha_core::gc::deletion_generation_of(&f.name).is_some() => {}
                    None => return Durability::Incomplete(format!("missing {}", f.name)),
                    Some(s) if s != f.size => {
                        return Durability::Incomplete(format!(
                            "{} has size {s}, committed {}",
                            f.name, f.size
                        ))
                    }
                    Some(_) => {}
                }
            }
            // A commit that doesn't cover the required files (should never
            // happen — the writer refuses to commit without them) is not a
            // usable segment either.
            for req in REQUIRED_SEGMENT_FILES {
                if commit.get(req).is_none() {
                    return Durability::Incomplete(format!("{COMMIT_FILE} does not list {req}"));
                }
            }
            if !FILTER_INDEX_FILES.iter().any(|f| commit.get(f).is_some()) {
                return Durability::Incomplete(format!(
                    "{COMMIT_FILE} lists no filter index ({})",
                    FILTER_INDEX_FILES.join(" or ")
                ));
            }
            Durability::Committed
        }
        Some(Err(e)) => Durability::Incomplete(format!("unreadable {COMMIT_FILE}: {e}")),
        None => {
            for req in REQUIRED_SEGMENT_FILES.iter().chain(&["filters.bin"]) {
                match size_of(req) {
                    None => return Durability::Incomplete(format!("missing {req}")),
                    Some(0) => return Durability::Incomplete(format!("{req} is empty")),
                    Some(_) => {}
                }
            }
            Durability::LegacyComplete
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_listing() -> Vec<(String, u64)> {
        vec![
            ("doc_store.bin".into(), 100),
            ("filters.bin".into(), 10),
            ("footer.json".into(), 5),
            ("inverted.idx".into(), 50),
        ]
    }

    fn commit_for(listing: &[(String, u64)]) -> SegmentCommit {
        SegmentCommit::new(
            listing
                .iter()
                .map(|(n, s)| CommitFile {
                    name: n.clone(),
                    size: *s,
                    crc32: 0,
                })
                .collect(),
        )
    }

    #[test]
    fn a_collected_deletion_generation_does_not_make_a_segment_incomplete() {
        let mut listing = full_listing();
        let generation = format!("deletes-{}.roaring", "ab".repeat(32));
        listing.push((generation.clone(), 8));
        let commit = commit_for(&listing);
        listing.retain(|(n, _)| *n != generation);
        assert_eq!(
            assess_listing(&listing, Some(Ok(&commit))),
            Durability::Committed
        );
    }

    #[test]
    fn committed_segment_is_durable() {
        let listing = full_listing();
        let c = commit_for(&listing);
        let mut with_marker = listing.clone();
        with_marker.push((COMMIT_FILE.into(), 1));
        assert_eq!(
            assess_listing(&with_marker, Some(Ok(&c))),
            Durability::Committed
        );
    }

    /// Acceptance criterion from #231: a segment missing any required file
    /// must not count as durable, so reconciliation re-uploads it. This is
    /// the exact failure the old "any object exists" check let through.
    /// #244: a columnar segment has `filters.toc` and column files instead
    /// of `filters.bin`; its commit covers them like any other file.
    #[test]
    fn committed_columnar_segment_is_durable_and_checks_its_columns() {
        let listing: Vec<(String, u64)> = vec![
            ("doc_store.bin".into(), 100),
            ("filters-000.col".into(), 30),
            ("filters.toc".into(), 12),
            ("footer.json".into(), 5),
            ("inverted.idx".into(), 50),
        ];
        let c = commit_for(&listing);
        assert_eq!(
            assess_listing(&listing, Some(Ok(&c))),
            Durability::Committed
        );
        let missing_column: Vec<_> = listing
            .iter()
            .filter(|(n, _)| n != "filters-000.col")
            .cloned()
            .collect();
        assert!(!assess_listing(&missing_column, Some(Ok(&c))).is_durable());
        // A commit listing no filter index at all is not a usable segment.
        let no_filters: Vec<_> = listing
            .iter()
            .filter(|(n, _)| !n.starts_with("filters"))
            .cloned()
            .collect();
        let c = commit_for(&no_filters);
        assert!(!assess_listing(&no_filters, Some(Ok(&c))).is_durable());
        // Without a commit (pre-#231), only the legacy format qualifies.
        assert!(!assess_listing(&listing, None).is_durable());
    }

    #[test]
    fn legacy_segment_missing_any_required_file_is_not_durable() {
        for missing in REQUIRED_SEGMENT_FILES.iter().chain(&["filters.bin"]) {
            let listing: Vec<_> = full_listing()
                .into_iter()
                .filter(|(n, _)| n != missing)
                .collect();
            let d = assess_listing(&listing, None);
            assert!(
                !d.is_durable(),
                "missing {missing} must not be durable: {d:?}"
            );
        }
        assert_eq!(
            assess_listing(&full_listing(), None),
            Durability::LegacyComplete
        );
    }

    #[test]
    fn only_doc_store_uploaded_is_not_durable() {
        let listing = vec![("doc_store.bin".to_string(), 100u64)];
        assert!(!assess_listing(&listing, None).is_durable());
    }

    #[test]
    fn legacy_zero_byte_required_file_is_not_durable() {
        let mut listing = full_listing();
        listing[0].1 = 0;
        assert!(!assess_listing(&listing, None).is_durable());
    }

    #[test]
    fn committed_file_with_wrong_size_or_missing_is_not_durable() {
        let listing = full_listing();
        let c = commit_for(&listing);
        let mut short = listing.clone();
        short[1].1 = 9;
        assert!(!assess_listing(&short, Some(Ok(&c))).is_durable());
        let missing: Vec<_> = listing[1..].to_vec();
        assert!(!assess_listing(&missing, Some(Ok(&c))).is_durable());
    }

    #[test]
    fn committed_extra_files_are_checked_too() {
        let mut listing = full_listing();
        listing.push(("vector.idx".into(), 1000));
        let c = commit_for(&listing);
        let without_vec: Vec<_> = listing[..4].to_vec();
        assert!(!assess_listing(&without_vec, Some(Ok(&c))).is_durable());
    }

    /// #204: a further vector column's files are committed like any other,
    /// so a segment missing one is not durable.
    #[test]
    fn committed_vector_column_files_are_checked() {
        let mut listing = full_listing();
        listing.push(("vector.idx".into(), 1000));
        listing.push(("vector.offsets".into(), 40));
        listing.push(("vector.1.idx".into(), 800));
        listing.push(("vector.1.offsets".into(), 40));
        let c = commit_for(&listing);
        assert!(assess_listing(&listing, Some(Ok(&c))).is_durable());
        let without: Vec<_> = listing
            .iter()
            .filter(|(name, _)| name != "vector.1.idx")
            .cloned()
            .collect();
        assert!(!assess_listing(&without, Some(Ok(&c))).is_durable());
    }

    #[test]
    fn unreadable_commit_is_not_durable() {
        assert!(!assess_listing(&full_listing(), Some(Err("boom"))).is_durable());
    }

    #[test]
    fn verify_detects_size_and_crc_mismatch() {
        let c = SegmentCommit::new(vec![SegmentCommit::entry("footer.json", b"hello")]);
        assert!(c.verify("footer.json", b"hello").is_ok());
        assert!(c.verify("footer.json", b"hellO").is_err());
        assert!(c.verify("footer.json", b"hello!").is_err());
        assert!(c.verify("unlisted", b"anything").is_ok());
    }

    #[test]
    fn upsert_replaces_entry_and_roundtrips() {
        let mut c = SegmentCommit::new(vec![SegmentCommit::entry("footer.json", b"a")]);
        c.upsert(SegmentCommit::entry("footer.json", b"bb"));
        c.upsert(SegmentCommit::entry("doc_store.offsets", b"ccc"));
        assert_eq!(c.files.len(), 2);
        assert_eq!(c.get("footer.json").unwrap().size, 2);
        let back = SegmentCommit::from_bytes(&c.to_bytes()).unwrap();
        assert_eq!(back, c);
        assert_eq!(back.files[0].name, "doc_store.offsets");
    }
}
