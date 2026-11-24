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
//!
//! A fetched span is written to disk off the request path (kosha-server's
//! background span writer), so between the fetch and the rename its bytes
//! are a [`PendingSpan`] in memory, which every reader here serves exactly
//! like a span file. A pending span whose write is queued counts as cached
//! ([`SpanIndex::covers`]); one whose write was dropped (the writer's queue
//! was full) stays readable only while whoever fetched it holds it, and
//! never counts as cached, so no other reader skips a fetch on its account.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, PoisonError, RwLock, Weak};
use std::time::{Duration, SystemTime};

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

/// Bytes `[start, start + len)` of a segment file, fetched and held in
/// memory while (or instead of) being written to its span directory. While
/// any clone is alive, every reader in this module serves the range from
/// these bytes; the last drop unregisters it.
pub struct PendingSpan {
    dir: PathBuf,
    start: u64,
    bytes: Vec<u8>,
    queued: bool,
    /// Where the bytes are counted while held only (a dropped write), so
    /// the owner can bound and report them; decremented on the last drop.
    held: Option<Arc<AtomicU64>>,
}

/// Pending spans a reader must keep alive until it has read them: the
/// spans a fetch could not queue for writing (see [`PendingSpan`]).
/// Dropping it early makes those ranges unreadable, hence `must_use`.
#[must_use = "the fetched spans are readable only while their holds live"]
#[derive(Debug, Default, Clone)]
pub struct SpanHolds(Vec<Arc<PendingSpan>>);

impl SpanHolds {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Bytes held.
    pub fn bytes(&self) -> u64 {
        self.0.iter().map(|p| p.bytes.len() as u64).sum()
    }

    pub fn append(&mut self, other: &mut SpanHolds) {
        self.0.append(&mut other.0);
    }
}

impl Extend<Arc<PendingSpan>> for SpanHolds {
    fn extend<I: IntoIterator<Item = Arc<PendingSpan>>>(&mut self, iter: I) {
        self.0.extend(iter);
    }
}

impl Extend<SpanHolds> for SpanHolds {
    fn extend<I: IntoIterator<Item = SpanHolds>>(&mut self, iter: I) {
        for mut holds in iter {
            self.append(&mut holds);
        }
    }
}

impl FromIterator<Arc<PendingSpan>> for SpanHolds {
    fn from_iter<I: IntoIterator<Item = Arc<PendingSpan>>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// Read-mostly: every [`SpanIndex::load`] with pending spans alive reads
/// it; only registering and the last drop of a pending span write it.
type PendingRegistry = RwLock<HashMap<PathBuf, Vec<Weak<PendingSpan>>>>;

fn pending_registry() -> &'static PendingRegistry {
    static REGISTRY: OnceLock<PendingRegistry> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

/// Pending spans alive in the process, so a reader with none to consider
/// (the common case) skips the registry lock.
static PENDING_LIVE: AtomicUsize = AtomicUsize::new(0);

impl PendingSpan {
    /// Make `bytes`, the range starting at `start` of
    /// `{segment_dir}/{file}`, readable through every span reader until the
    /// returned value and all its clones drop. `queued`: a write of it to
    /// the span directory is queued, so it counts as cached until then (the
    /// writer renames the file into place before it lets go).
    pub fn register(
        segment_dir: &Path,
        file: &str,
        start: u64,
        bytes: Vec<u8>,
        queued: bool,
    ) -> Arc<Self> {
        Self::registered(spans_dir(segment_dir, file), start, bytes, queued, None)
    }

    /// [`Self::register`] for a span whose write was dropped: readable only
    /// (never counted as cached), its bytes added to `held` until the last
    /// clone drops.
    pub fn register_held(
        segment_dir: &Path,
        file: &str,
        start: u64,
        bytes: Vec<u8>,
        held: Arc<AtomicU64>,
    ) -> Arc<Self> {
        held.fetch_add(bytes.len() as u64, Ordering::AcqRel);
        Self::registered(
            spans_dir(segment_dir, file),
            start,
            bytes,
            false,
            Some(held),
        )
    }

    fn registered(
        dir: PathBuf,
        start: u64,
        bytes: Vec<u8>,
        queued: bool,
        held: Option<Arc<AtomicU64>>,
    ) -> Arc<Self> {
        let span = Arc::new(Self {
            dir,
            start,
            bytes,
            queued,
            held,
        });
        let mut registry = pending_registry()
            .write()
            .unwrap_or_else(|e| e.into_inner());
        registry
            .entry(span.dir.clone())
            .or_default()
            .push(Arc::downgrade(&span));
        PENDING_LIVE.fetch_add(1, Ordering::AcqRel);
        span
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> u64 {
        self.start + self.bytes.len() as u64
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Where the span file goes: `{segment_dir}/{file}.spans/{name}`.
    pub fn dest(&self) -> PathBuf {
        self.dir.join(span_file_name(self.start, self.end()))
    }

    /// The span directory it belongs to.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The whole file the span is a range of: `{segment_dir}/{file}`.
    pub fn whole_path(&self) -> PathBuf {
        let name = self
            .dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let file = name.strip_suffix(SPANS_SUFFIX).unwrap_or(&name);
        self.dir.with_file_name(file)
    }

    /// The live pending spans of one span directory, sorted by start.
    fn of_dir(dir: &Path) -> Vec<Arc<Self>> {
        if PENDING_LIVE.load(Ordering::Acquire) == 0 {
            return Vec::new();
        }
        let mut live: Vec<Arc<Self>> = {
            let registry = pending_registry().read().unwrap_or_else(|e| e.into_inner());
            registry
                .get(dir)
                .map(|spans| spans.iter().filter_map(Weak::upgrade).collect())
                .unwrap_or_default()
        };
        live.sort_unstable_by_key(|p| (p.start, p.end()));
        live
    }
}

impl Drop for PendingSpan {
    fn drop(&mut self) {
        if let Some(held) = &self.held {
            held.fetch_sub(self.bytes.len() as u64, Ordering::AcqRel);
        }
        let mut registry = pending_registry()
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(spans) = registry.get_mut(&self.dir) {
            // This span's own weak entry is dead by now (its strong count
            // reached zero), as is any other that has not been pruned yet.
            spans.retain(|w| w.strong_count() > 0);
            if spans.is_empty() {
                registry.remove(&self.dir);
            }
        }
        PENDING_LIVE.fetch_sub(1, Ordering::AcqRel);
    }
}

impl std::fmt::Debug for PendingSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingSpan")
            .field("dest", &self.dest())
            .field("queued", &self.queued)
            .finish()
    }
}

/// The complete spans cached for one segment file, sorted by start: the
/// span files on disk plus the [`PendingSpan`]s alive when it was taken,
/// which it holds, so their bytes stay readable for as long as it lives.
#[derive(Debug, Default, Clone)]
pub struct SpanIndex {
    dir: PathBuf,
    /// What counts as cached: `on_disk` plus queued pending spans.
    spans: Arc<Vec<(u64, u64)>>,
    on_disk: Arc<Vec<(u64, u64)>>,
    /// Sorted by start.
    pending: Vec<Arc<PendingSpan>>,
}

/// Listings of span directories, keyed by directory, each valid while the
/// directory's modification time is the one it was taken at.
///
/// A listing is a `read_dir` plus a stat of every span file, and a segment
/// warmed by ranged reads holds hundreds of spans per file. Every reader
/// that lists per segment visit (a result page's documents and vectors,
/// exact rescoring, a kNN probe) paid that on every query: on the 10M
/// vector bench, materializing a 10-hit page took 14 ms and rescoring ~100
/// vectors 6.6 ms, almost all of it listings of `doc_vectors.bin.spans`.
///
/// Why the modification time is enough: a span appears by a rename into
/// the directory and disappears by an unlink, and both change the
/// directory's mtime; span files are never rewritten in place (see
/// kosha-server's `SpanWriteQueue::write`). A listing is only cached when the
/// directory had been quiet for [`LISTING_QUIET`] when it was taken, so a
/// change landing in the same timestamp tick as the listing cannot hide
/// behind an equal mtime.
type ListingCache = RwLock<Listings>;

/// A cached listing: the directory's mtime when listed, its spans, and
/// the [`listing_bytes`] it was charged.
type Listing = (SystemTime, Arc<Vec<(u64, u64)>>, usize);

/// The cached listings and an estimate of the heap they hold.
#[derive(Debug, Default)]
struct Listings {
    map: HashMap<PathBuf, Listing>,
    /// Sum of the charges in `map`.
    bytes: usize,
}

/// Heap held by one cached listing: the map slot, the key's path, and the
/// shared span vector (its allocation, not just its length). An estimate
/// within allocator rounding; the spans dominate for any warmed directory.
fn listing_bytes(dir: &Path, spans: &Arc<Vec<(u64, u64)>>) -> usize {
    std::mem::size_of::<(PathBuf, Listing)>()
        + 1 // hashbrown control byte
        + dir.as_os_str().len()
        + 2 * std::mem::size_of::<usize>() // Arc counts
        + std::mem::size_of::<Vec<(u64, u64)>>()
        + spans.capacity() * std::mem::size_of::<(u64, u64)>()
}

impl Listings {
    /// Caches `spans` as `dir`'s listing at `at`, within `max_dirs`
    /// directories and `max_bytes` of [`listing_bytes`]. A listing larger
    /// than `max_bytes` alone is not cached. When the new listing does not
    /// fit, every listing is dropped (as when the count bound is hit): the
    /// dropped ones are returned, for the caller to free after releasing
    /// the lock, together with any listing `dir` had before.
    fn store(
        &mut self,
        dir: PathBuf,
        at: SystemTime,
        spans: Arc<Vec<(u64, u64)>>,
        max_dirs: usize,
        max_bytes: usize,
    ) -> Garbage {
        let replaced = self.remove(&dir);
        let bytes = listing_bytes(&dir, &spans);
        if bytes > max_bytes {
            return (None, replaced);
        }
        let evicted = (self.map.len() >= max_dirs || self.bytes + bytes > max_bytes)
            .then(|| std::mem::take(self));
        self.map.insert(dir, (at, spans, bytes));
        self.bytes += bytes;
        (evicted, replaced)
    }

    fn remove(&mut self, dir: &Path) -> Option<Arc<Vec<(u64, u64)>>> {
        let (_, old, charged) = self.map.remove(dir)?;
        self.bytes = self.bytes.saturating_sub(charged);
        Some(old)
    }
}

/// Listings removed from the cache (every one, on eviction; the one a
/// directory had before), to be dropped outside the cache's lock: freeing
/// 16,384 listings is not free.
type Garbage = (Option<Listings>, Option<Arc<Vec<(u64, u64)>>>);

fn listing_cache() -> &'static ListingCache {
    static CACHE: OnceLock<ListingCache> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// How long a span directory must have been unmodified for its listing to
/// be cached: longer than any filesystem's timestamp granularity.
const LISTING_QUIET: Duration = Duration::from_secs(2);

/// Entries kept before the cache is dropped and refilled; a bound on
/// memory for a node that has listed very many segments, not a tuning
/// knob (one entry is 16 bytes per span).
const LISTING_CACHE_MAX_DIRS: usize = 16_384;

/// Heap the cached listings may hold ([`listing_bytes`]), whatever the
/// count: the count alone does not bound memory, since one directory can
/// hold any number of spans. 64 MiB is 4M spans, ~250 per directory at
/// the count bound (the 10M vector bench's per-segment
/// `doc_vectors.bin.spans` holds ~730); 0.26% of a 24 GiB live-memory
/// watermark. Not a tuning knob.
const LISTING_CACHE_MAX_BYTES: usize = 64 << 20;

/// `KOSHA_SPAN_LISTING_CACHE=0` (or `off`/`false`) lists the directory on
/// every [`SpanIndex::load`], as before. Read once.
fn listing_cache_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("KOSHA_SPAN_LISTING_CACHE")
                .ok()
                .as_deref()
                .map(str::trim),
            Some("0") | Some("off") | Some("false")
        )
    })
}

fn dir_mtime(dir: &Path) -> Option<SystemTime> {
    fs::metadata(dir).and_then(|m| m.modified()).ok()
}

impl SpanIndex {
    /// List `{segment_dir}/{file}.spans`. A missing directory is an empty
    /// index. Spans whose file length disagrees with their name are left
    /// out.
    ///
    /// Served from the listing cache when the directory has not changed
    /// since it was last listed (one stat instead of a `read_dir` and a
    /// stat per span); see [`ListingCache`].
    pub fn load(segment_dir: &Path, file: &str) -> Self {
        let dir = spans_dir(segment_dir, file);
        let pending = PendingSpan::of_dir(&dir);
        let mut index = Self::load_on_disk(dir);
        if !pending.is_empty() {
            let mut spans = index.on_disk.to_vec();
            spans.extend(
                pending
                    .iter()
                    .filter(|p| p.queued)
                    .map(|p| (p.start, p.end())),
            );
            spans.sort_unstable();
            spans.dedup();
            index.spans = Arc::new(spans);
            index.pending = pending;
        }
        index
    }

    /// The span files of `dir` alone (see [`Self::load`]).
    fn load_on_disk(dir: PathBuf) -> Self {
        if !listing_cache_enabled() {
            return Self::list(dir);
        }
        let Some(before) = dir_mtime(&dir) else {
            // No directory: an empty index, and nothing worth caching.
            return Self {
                dir,
                ..Self::default()
            };
        };
        if let Some((at, spans, _)) = listing_cache()
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .map
            .get(&dir)
        {
            if *at == before {
                return Self::from_disk(dir, Arc::clone(spans));
            }
        }
        let listed = Self::list(dir);
        // Cache only a listing of a directory that did not change while
        // it was listed and had been quiet for a while before that.
        let cacheable = SystemTime::now()
            .duration_since(before)
            .is_ok_and(|age| age >= LISTING_QUIET)
            && dir_mtime(&listed.dir) == Some(before);
        // Taken and released here; what it drops is freed after.
        let garbage = {
            let mut cache = listing_cache()
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            if cacheable {
                cache.store(
                    listed.dir.clone(),
                    before,
                    Arc::clone(&listed.on_disk),
                    LISTING_CACHE_MAX_DIRS,
                    LISTING_CACHE_MAX_BYTES,
                )
            } else {
                (None, cache.remove(&listed.dir))
            }
        };
        drop(garbage);
        listed
    }

    /// The directory, listed now.
    fn list(dir: PathBuf) -> Self {
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
        Self::from_disk(dir, Arc::new(spans))
    }

    fn from_disk(dir: PathBuf, on_disk: Arc<Vec<(u64, u64)>>) -> Self {
        Self {
            dir,
            spans: Arc::clone(&on_disk),
            on_disk,
            pending: Vec::new(),
        }
    }

    /// A cached span containing all of `[start, end)`, if any.
    pub fn covering(&self, start: u64, end: u64) -> Option<(u64, u64)> {
        covering_in(&self.spans, start, end)
    }

    pub fn covers(&self, start: u64, end: u64) -> bool {
        start == end || self.covering(start, end).is_some()
    }

    /// The pending span holding all of `[start, end)`, if any.
    fn pending_covering(&self, start: u64, end: u64) -> Option<&PendingSpan> {
        let upto = self.pending.partition_point(|p| p.start <= start);
        self.pending[..upto]
            .iter()
            .rev()
            .find(|p| p.end() >= end)
            .map(Arc::as_ref)
    }

    /// `[start, end)` out of a pending span, when one holds it.
    fn read_pending(&self, start: u64, end: u64) -> Option<&[u8]> {
        let p = self.pending_covering(start, end)?;
        Some(&p.bytes[(start - p.start) as usize..(end - p.start) as usize])
    }

    /// Read `[start, end)` out of whichever cached span holds it. `None`
    /// when no span covers it or the span vanished (evicted) meanwhile.
    pub fn read(&self, start: u64, end: u64) -> Option<Vec<u8>> {
        if start == end {
            return Some(Vec::new());
        }
        if let Some(bytes) = self.read_pending(start, end) {
            return Some(bytes.to_vec());
        }
        let (s, e) = covering_in(&self.on_disk, start, end)?;
        let mut file = fs::File::open(self.dir.join(span_file_name(s, e))).ok()?;
        file.seek(SeekFrom::Start(start - s)).ok()?;
        let mut buf = vec![0u8; (end - start) as usize];
        file.read_exact(&mut buf).ok()?;
        Some(buf)
    }

    /// [`Self::read`] for many ranges, in order, without re-listing the
    /// directory: consecutive ranges inside the same cached span share one
    /// open file handle (#381: exact rescoring reads hundreds of vectors
    /// per segment, and a listing per vector cost a `read_dir` plus a stat
    /// of every cached span each time). `None` for a range no span covers
    /// or whose span vanished (evicted) meanwhile.
    pub fn read_many(&self, ranges: &[(u64, u64)]) -> Vec<Option<Vec<u8>>> {
        let mut open: Option<((u64, u64), fs::File)> = None;
        ranges
            .iter()
            .map(|&(start, end)| {
                if start == end {
                    return Some(Vec::new());
                }
                if let Some(bytes) = self.read_pending(start, end) {
                    return Some(bytes.to_vec());
                }
                let span = covering_in(&self.on_disk, start, end)?;
                if open.as_ref().is_none_or(|(s, _)| *s != span) {
                    open = fs::File::open(self.dir.join(span_file_name(span.0, span.1)))
                        .ok()
                        .map(|f| (span, f));
                }
                let (_, file) = open.as_mut()?;
                file.seek(SeekFrom::Start(start - span.0)).ok()?;
                let mut buf = vec![0u8; (end - start) as usize];
                file.read_exact(&mut buf).ok()?;
                Some(buf)
            })
            .collect()
    }

    /// The cached span starting at offset 0 (a file header), read whole.
    pub fn read_prefix(&self) -> Option<Vec<u8>> {
        let end = self
            .on_disk
            .iter()
            .copied()
            .chain(self.pending.iter().map(|p| (p.start, p.end())))
            .filter(|&(s, _)| s == 0)
            .map(|(_, e)| e)
            .max()?;
        self.read(0, end)
    }

    pub fn spans(&self) -> &[(u64, u64)] {
        &self.spans
    }

    /// No span to read from, on disk or in memory.
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty() && self.pending.is_empty()
    }

    /// Bytes cached (overlaps counted twice, as they are stored).
    pub fn total_bytes(&self) -> u64 {
        self.spans.iter().map(|(s, e)| e - s).sum()
    }
}

/// A span of sorted `spans` containing all of `[start, end)`, if any.
fn covering_in(spans: &[(u64, u64)], start: u64, end: u64) -> Option<(u64, u64)> {
    // Spans may overlap (two queries fetching neighbouring ranges), so any
    // span starting at or before `start` may be the one.
    let upto = spans.partition_point(|&(s, _)| s <= start);
    spans[..upto]
        .iter()
        .rev()
        .find(|&&(_, e)| e >= end)
        .copied()
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
    read_local_range_reusing(segment_dir, file, complete_len, start, len, &mut None)
}

/// [`read_local_range`] for a caller that reads many ranges of the same
/// file in a row (a kNN probe hydrating its postings): the span listing is
/// kept in `spans` between calls instead of being rebuilt per range. One
/// listing is a `read_dir` plus a stat of every cached span, and a segment
/// warmed by ranged reads holds hundreds of spans, so a listing per posting
/// cost more than the read itself and did not scale across query threads.
///
/// A listing that does not serve the range is rebuilt once before giving
/// up, so spans fetched (or evicted) since it was taken are seen. Pass the
/// same `spans` only for the same `segment_dir` and `file`.
pub fn read_local_range_reusing(
    segment_dir: &Path,
    file: &str,
    complete_len: u64,
    start: u64,
    len: u64,
    spans: &mut Option<SpanIndex>,
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
        .or_else(|| {
            if let Some(buf) = spans.as_ref().and_then(|s| s.read(start, start + len)) {
                return Some(buf);
            }
            let fresh = SpanIndex::load(segment_dir, file);
            let buf = fresh.read(start, start + len);
            *spans = Some(fresh);
            buf
        })
        .or_else(read_whole)
}

/// Fill `buf` from `file` at `offset`, without moving a shared cursor.
/// `false` when the file is shorter than that (or the read fails).
#[cfg(unix)]
fn read_exact_at(file: &fs::File, buf: &mut [u8], offset: u64) -> bool {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset).is_ok()
}

#[cfg(not(unix))]
fn read_exact_at(mut file: &fs::File, buf: &mut [u8], offset: u64) -> bool {
    file.seek(SeekFrom::Start(offset)).is_ok() && file.read_exact(buf).is_ok()
}

/// [`read_local_range_reusing`] for a caller that reads many ranges of one
/// file back to back and wants neither a `stat` + `open` + `seek` nor a
/// fresh buffer per range (#435: a warm kNN probe reads ~30 postings of a
/// segment's `vector.idx`, and that bookkeeping cost about as much as the
/// bytes).
///
/// The whole file is opened at most once and read with positional reads;
/// a span file's handle is kept for as long as consecutive ranges fall in
/// the same span; every range lands in one buffer owned by the reader.
/// The sources and their order are those of [`read_local_range_reusing`]:
///
/// 1. the whole file, when it holds at least `complete_len` bytes;
/// 2. the span cache, through a listing kept between reads and rebuilt
///    once when it does not serve a range (a span fetched or evicted since
///    the listing was taken);
/// 3. the whole file again, reopened: a background promotion can land the
///    file and drop the spans between the two looks.
///
/// A handle that is already open keeps serving a file or span that is
/// unlinked meanwhile: those bytes are still the bytes of the range. Use
/// one reader for one `segment_dir` and `file`, and drop it when the run
/// of reads ends.
#[derive(Debug)]
pub struct LocalRangeReader {
    whole_path: PathBuf,
    segment_dir: PathBuf,
    file: String,
    /// The whole file and its length when it was opened.
    whole: Option<(fs::File, u64)>,
    whole_tried: bool,
    spans: Option<SpanIndex>,
    /// The span file the last span read came from.
    span_file: Option<((u64, u64), fs::File)>,
    /// Words, not bytes, so every range read lands 4-byte aligned and a
    /// caller can view an f32 posting in place (#435).
    buf: Vec<u32>,
}

/// The first `n` bytes of `buf`, which holds at least that many.
fn buf_bytes(buf: &mut [u32], n: usize) -> &mut [u8] {
    &mut bytemuck::cast_slice_mut::<u32, u8>(buf)[..n]
}

impl LocalRangeReader {
    pub fn new(segment_dir: &Path, file: &str) -> Self {
        Self {
            whole_path: segment_dir.join(file),
            segment_dir: segment_dir.to_path_buf(),
            file: file.to_string(),
            whole: None,
            whole_tried: false,
            spans: None,
            span_file: None,
            buf: Vec::new(),
        }
    }

    /// The bytes `[start, start + len)` of the file, valid until the next
    /// read and starting at a 4-byte aligned address. `None` when neither
    /// the whole file nor a cached span holds the range; never a short or
    /// zero-filled slice.
    pub fn read(&mut self, complete_len: u64, start: u64, len: u64) -> Option<&[u8]> {
        let n = usize::try_from(len).ok()?;
        if n == 0 {
            return Some(&[]);
        }
        let end = start.checked_add(len)?;
        let words = n.div_ceil(4);
        if self.buf.len() < words {
            self.buf.resize(words, 0);
        }
        let found = self.read_whole(complete_len, start, n, false)
            || self.read_span(start, end, n)
            || self.read_whole(complete_len, start, n, true);
        found.then(|| &bytemuck::cast_slice::<u32, u8>(&self.buf)[..n])
    }

    fn read_whole(&mut self, complete_len: u64, start: u64, n: usize, reopen: bool) -> bool {
        if reopen || !self.whole_tried {
            self.whole_tried = true;
            self.whole = fs::File::open(&self.whole_path).ok().and_then(|file| {
                let len = file.metadata().ok().filter(|m| m.is_file())?.len();
                Some((file, len))
            });
        }
        let Some((file, file_len)) = &self.whole else {
            return false;
        };
        // A short (truncated) whole file is not trusted for this range.
        if *file_len == 0 || *file_len < complete_len {
            return false;
        }
        read_exact_at(file, buf_bytes(&mut self.buf, n), start)
    }

    fn read_span(&mut self, start: u64, end: u64, n: usize) -> bool {
        if self.spans.is_some() && self.read_listed_span(start, end, n) {
            return true;
        }
        self.spans = Some(SpanIndex::load(&self.segment_dir, &self.file));
        self.span_file = None;
        self.read_listed_span(start, end, n)
    }

    fn read_listed_span(&mut self, start: u64, end: u64, n: usize) -> bool {
        let Some(spans) = &self.spans else {
            return false;
        };
        if let Some(bytes) = spans.read_pending(start, end) {
            buf_bytes(&mut self.buf, n).copy_from_slice(bytes);
            return true;
        }
        let Some(span) = covering_in(&spans.on_disk, start, end) else {
            return false;
        };
        if self
            .span_file
            .as_ref()
            .is_none_or(|(open, _)| *open != span)
        {
            self.span_file = fs::File::open(spans.dir.join(span_file_name(span.0, span.1)))
                .ok()
                .map(|file| (span, file));
        }
        let Some((_, file)) = &self.span_file else {
            return false;
        };
        if read_exact_at(file, buf_bytes(&mut self.buf, n), start - span.0) {
            return true;
        }
        self.span_file = None;
        false
    }
}

/// Merge `[start, end)` ranges whose gap is at most `max_gap` bytes into
/// one fetch each — reading a little dead space between two documents is
/// cheaper than a second round trip. Input order does not matter; empty
/// ranges are dropped.
pub fn coalesce(ranges: Vec<(u64, u64)>, max_gap: u64) -> Vec<(u64, u64)> {
    coalesce_capped(ranges, max_gap, u64::MAX)
}

/// [`coalesce`] that stops growing a merged range once it would pass
/// `max_len` bytes, so a dense set of ranges (every vector posting of a
/// column, every candidate vector of a one-matter segment) becomes many
/// bounded GETs — fetched concurrently, each holding the in-flight budget
/// for its own bytes — instead of one GET the size of the file, held in
/// memory whole. An input range is never split (a reader needs each one
/// inside a single cached span), so one larger than `max_len` is fetched
/// as it is, and an input that overlaps a merged range already at the cap
/// starts a new one overlapping it: the overlap (at most that input's
/// length) is fetched twice rather than leaving the input split across
/// two spans.
pub fn coalesce_capped(mut ranges: Vec<(u64, u64)>, max_gap: u64, max_len: u64) -> Vec<(u64, u64)> {
    ranges.retain(|(s, e)| e > s);
    ranges.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for (s, e) in ranges {
        match out.last_mut() {
            Some(last)
                if s <= last.1.saturating_add(max_gap) && last.1.max(e) - last.0 <= max_len =>
            {
                last.1 = last.1.max(e)
            }
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

    /// Set a span directory's modification time, as a quiet directory's.
    fn set_dir_mtime(seg: &Path, file: &str, at: SystemTime) {
        fs::File::open(spans_dir(seg, file))
            .unwrap()
            .set_modified(at)
            .unwrap();
    }

    #[test]
    fn listing_is_cached_until_the_directory_changes() {
        let seg = tmp("listing-cache");
        let file = "doc_vectors.bin";
        put(&seg, file, 0, &[1; 8]);
        put(&seg, file, 100, &[2; 8]);
        let quiet = SystemTime::now() - Duration::from_secs(60);
        set_dir_mtime(&seg, file, quiet);
        assert_eq!(SpanIndex::load(&seg, file).spans(), &[(0, 8), (100, 108)]);

        // A span added behind the cache's back (mtime put back, which no
        // real writer does) is not seen: the listing came from the cache.
        put(&seg, file, 200, &[3; 8]);
        set_dir_mtime(&seg, file, quiet);
        assert_eq!(SpanIndex::load(&seg, file).spans(), &[(0, 8), (100, 108)]);

        // What a real writer does (the rename moves the mtime): seen.
        set_dir_mtime(&seg, file, quiet + Duration::from_secs(1));
        assert_eq!(
            SpanIndex::load(&seg, file).spans(),
            &[(0, 8), (100, 108), (200, 208)]
        );

        // An eviction (unlink) moves it too.
        fs::remove_file(spans_dir(&seg, file).join(span_file_name(100, 108))).unwrap();
        set_dir_mtime(&seg, file, quiet + Duration::from_secs(2));
        assert_eq!(SpanIndex::load(&seg, file).spans(), &[(0, 8), (200, 208)]);
        let _ = fs::remove_dir_all(&seg);
    }

    #[test]
    fn a_recently_changed_directory_is_listed_every_time() {
        // Inside the quiet window a change can share the listing's
        // timestamp, so nothing is cached and every span shows up at once.
        let seg = tmp("listing-fresh");
        let file = "doc_store.bin";
        put(&seg, file, 0, &[1; 4]);
        assert_eq!(SpanIndex::load(&seg, file).spans(), &[(0, 4)]);
        put(&seg, file, 10, &[2; 4]);
        assert_eq!(SpanIndex::load(&seg, file).spans(), &[(0, 4), (10, 14)]);
        fs::remove_dir_all(spans_dir(&seg, file)).unwrap();
        assert!(SpanIndex::load(&seg, file).is_empty());
        let _ = fs::remove_dir_all(&seg);
    }

    #[test]
    fn listings_are_bounded_by_bytes_as_well_as_count() {
        let spans = |n: usize| Arc::new(vec![(0u64, 1u64); n]);
        let at = SystemTime::UNIX_EPOCH;
        let one = listing_bytes(Path::new("/s/0"), &spans(1000));
        // Room for three 1000-span listings, and a count bound far away.
        let max = 3 * one + one / 2;
        let mut cache = Listings::default();
        for i in 0..3 {
            let (evicted, replaced) = cache.store(
                PathBuf::from(format!("/s/{i}")),
                at,
                spans(1000),
                1 << 20,
                max,
            );
            assert!(evicted.is_none() && replaced.is_none());
        }
        assert_eq!((cache.map.len(), cache.bytes), (3, 3 * one));

        // Re-listing a directory replaces its charge instead of adding one.
        let (evicted, replaced) = cache.store(PathBuf::from("/s/1"), at, spans(1000), 1 << 20, max);
        assert!(evicted.is_none() && replaced.is_some());
        assert_eq!(cache.bytes, 3 * one);

        // A fourth does not fit: the cache is dropped (handed back to be
        // freed outside the lock) and holds only the new listing.
        let (evicted, _) = cache.store(PathBuf::from("/s/3"), at, spans(1000), 1 << 20, max);
        assert_eq!(evicted.map(|e| (e.map.len(), e.bytes)), Some((3, 3 * one)));
        assert_eq!((cache.map.len(), cache.bytes), (1, one));

        // A listing over the whole bound is not cached, and takes the
        // directory's stale listing out with it.
        let (evicted, replaced) = cache.store(PathBuf::from("/s/3"), at, spans(4000), 1 << 20, max);
        assert!(evicted.is_none() && replaced.is_some());
        assert_eq!((cache.map.len(), cache.bytes), (0, 0));

        // The count bound still applies.
        for i in 0..5 {
            let _ = cache.store(
                PathBuf::from(format!("/c/{i}")),
                at,
                spans(1),
                2,
                usize::MAX,
            );
        }
        assert!(cache.map.len() <= 2);
        let charged: usize = cache.map.values().map(|v| v.2).sum();
        assert_eq!(cache.bytes, charged);
    }

    #[test]
    fn a_poisoned_listing_cache_still_serves() {
        let seg = tmp("listing-poison");
        let file = "doc_vectors.bin";
        put(&seg, file, 0, &[1; 8]);
        set_dir_mtime(&seg, file, SystemTime::now() - Duration::from_secs(60));
        // A thread that panics holding the lock poisons it.
        let _ = std::thread::spawn(|| {
            let _guard = listing_cache()
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            panic!("poison the listing cache");
        })
        .join();
        assert!(listing_cache().is_poisoned());
        assert_eq!(SpanIndex::load(&seg, file).spans(), &[(0, 8)]);
        assert_eq!(SpanIndex::load(&seg, file).spans(), &[(0, 8)]);
        let _ = fs::remove_dir_all(&seg);
    }

    /// `cargo test --release -p kosha-segment --lib -- --ignored
    /// --nocapture listing_cost`: one listing of a 730-span directory (the
    /// 10M vector bench's `doc_vectors.bin.spans` per segment), listed and
    /// from the cache.
    #[test]
    #[ignore = "microbench"]
    fn listing_cost() {
        let seg = tmp("listing-cost");
        let file = "doc_vectors.bin";
        for i in 0..730u64 {
            put(&seg, file, i * 8192, &[0; 4096]);
        }
        set_dir_mtime(&seg, file, SystemTime::now() - Duration::from_secs(60));
        let iters = 2000;
        let t = std::time::Instant::now();
        for _ in 0..iters {
            assert_eq!(SpanIndex::list(spans_dir(&seg, file)).spans().len(), 730);
        }
        let listed = t.elapsed().as_secs_f64() * 1e6 / iters as f64;
        let t = std::time::Instant::now();
        for _ in 0..iters {
            assert_eq!(SpanIndex::load(&seg, file).spans().len(), 730);
        }
        let cached = t.elapsed().as_secs_f64() * 1e6 / iters as f64;
        println!("listing 730 spans: {listed:.1} us listed, {cached:.2} us cached");
        let _ = fs::remove_dir_all(&seg);
    }

    /// A pending span serves every reader from memory while it lives; a
    /// queued one counts as cached, a dropped one only reads; the last
    /// drop unregisters it, and a reader that captured it keeps it.
    #[test]
    fn pending_spans_read_like_span_files_while_held() {
        let seg = tmp("pending");
        let file = "vector.idx";
        put(&seg, file, 0, b"DISK");
        let bytes: Vec<u8> = (0..32u8).collect();
        let queued = PendingSpan::register(&seg, file, 100, bytes.clone(), true);
        let held = Arc::new(AtomicU64::new(0));
        let dropped = PendingSpan::register_held(&seg, file, 200, vec![9u8; 8], held.clone());
        assert_eq!(held.load(Ordering::Relaxed), 8);
        assert_eq!(queued.whole_path(), seg.join(file));
        let idx = SpanIndex::load(&seg, file);
        assert_eq!(idx.spans(), &[(0, 4), (100, 132)]);
        assert!(idx.covers(104, 108) && !queued.dest().exists());
        assert!(!idx.covers(200, 208), "a dropped span is not cached");
        assert_eq!(idx.read(104, 108).as_deref(), Some(&bytes[4..8]));
        assert_eq!(idx.read(202, 204).as_deref(), Some(&[9u8, 9][..]));
        assert_eq!(
            idx.read_many(&[(0, 4), (130, 132), (200, 201)]),
            vec![
                Some(b"DISK".to_vec()),
                Some(bytes[30..].to_vec()),
                Some(vec![9])
            ]
        );
        assert_eq!(
            read_local_range(&seg, file, u64::MAX, 110, 2).as_deref(),
            Some(&bytes[10..12])
        );
        let mut reader = LocalRangeReader::new(&seg, file);
        assert_eq!(reader.read(u64::MAX, 0, 4), Some(&b"DISK"[..]));
        assert_eq!(reader.read(u64::MAX, 120, 4), Some(&bytes[20..24]));
        assert_eq!(reader.read(u64::MAX, 207, 1), Some(&[9u8][..]));

        drop((queued, dropped));
        // Captured by `idx` and `reader`: still readable through them.
        assert_eq!(idx.read(104, 108).as_deref(), Some(&bytes[4..8]));
        assert_eq!(reader.read(u64::MAX, 200, 2), Some(&[9u8, 9][..]));
        drop((idx, reader));
        assert_eq!(
            held.load(Ordering::Relaxed),
            0,
            "the last drop releases the count"
        );
        let idx = SpanIndex::load(&seg, file);
        assert_eq!(idx.spans(), &[(0, 4)]);
        assert_eq!(idx.read(104, 108), None);
        assert_eq!(read_local_range(&seg, file, u64::MAX, 200, 2), None);
        let _ = fs::remove_dir_all(&seg);
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
    fn coalesce_capped_bounds_merged_ranges_without_splitting_inputs() {
        // Contiguous ranges merge up to the cap, then start a new range.
        let dense: Vec<(u64, u64)> = (0..10).map(|i| (i * 10, i * 10 + 10)).collect();
        assert_eq!(
            coalesce_capped(dense.clone(), 0, 30),
            vec![(0, 30), (30, 60), (60, 90), (90, 100)]
        );
        // No cap: the same as `coalesce`.
        assert_eq!(
            coalesce_capped(dense.clone(), 0, u64::MAX),
            coalesce(dense, 0)
        );
        // An input overlapping a merged range at the cap starts a new
        // range that still holds all of it.
        assert_eq!(
            coalesce_capped(vec![(0, 20), (15, 30)], 0, 20),
            vec![(0, 20), (15, 30)]
        );
        // An input larger than the cap is kept whole, never split.
        assert_eq!(
            coalesce_capped(vec![(0, 5), (5, 100), (100, 110)], 0, 20),
            vec![(0, 5), (5, 100), (100, 110)]
        );
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

    #[test]
    fn reused_listing_sees_spans_added_and_removed_after_it_was_taken() {
        let seg = tmp("reuse");
        put(&seg, "vector.idx", 0, b"AAAA");
        let mut spans = None;
        let read = |start, spans: &mut _| {
            read_local_range_reusing(&seg, "vector.idx", 99, start, 4, spans)
        };
        assert_eq!(read(0, &mut spans).as_deref(), Some(&b"AAAA"[..]));
        assert_eq!(spans.as_ref().map(|s| s.spans().len()), Some(1));
        // Fetched after the listing: one rebuild finds it.
        put(&seg, "vector.idx", 10, b"BBBB");
        assert_eq!(read(10, &mut spans).as_deref(), Some(&b"BBBB"[..]));
        assert_eq!(spans.as_ref().map(|s| s.spans().len()), Some(2));
        // Evicted after the listing: a miss, not stale bytes or a panic.
        fs::remove_file(spans_dir(&seg, "vector.idx").join(span_file_name(0, 4))).unwrap();
        assert_eq!(read(0, &mut spans), None);
        assert_eq!(read(10, &mut spans).as_deref(), Some(&b"BBBB"[..]));
        let _ = fs::remove_dir_all(&seg);
    }

    #[test]
    fn range_reader_serves_whole_file_then_spans_and_reuses_its_buffer() {
        let seg = tmp("reader-whole");
        let bytes: Vec<u8> = (0..=255u8).collect();
        fs::write(seg.join("vector.idx"), &bytes).unwrap();
        let mut reader = LocalRangeReader::new(&seg, "vector.idx");
        assert_eq!(reader.read(256, 10, 4), Some(&bytes[10..14]));
        // A shorter read after a longer one returns exactly its range.
        assert_eq!(reader.read(256, 0, 200), Some(&bytes[..200]));
        assert_eq!(reader.read(256, 250, 2), Some(&bytes[250..252]));
        assert_eq!(reader.read(256, 5, 0), Some(&[][..]));
        // Past the end of the file: a miss, never zero-filled bytes.
        assert_eq!(reader.read(256, 250, 10), None);
        // A whole file shorter than the caller says is complete is not
        // trusted, and there is no span to fall back to.
        assert_eq!(reader.read(1000, 10, 4), None);
        // The open handle keeps serving after the file is unlinked.
        fs::remove_file(seg.join("vector.idx")).unwrap();
        assert_eq!(reader.read(256, 100, 8), Some(&bytes[100..108]));
        let _ = fs::remove_dir_all(&seg);
    }

    #[test]
    fn range_reader_follows_spans_added_evicted_and_promoted_mid_run() {
        let seg = tmp("reader-spans");
        put(&seg, "vector.idx", 0, b"AAAA");
        put(&seg, "vector.idx", 100, b"CCCCDDDD");
        let mut reader = LocalRangeReader::new(&seg, "vector.idx");
        assert_eq!(reader.read(4, 0, 4), Some(&b"AAAA"[..]));
        // Two ranges of one span, then back to another span.
        assert_eq!(reader.read(104, 100, 4), Some(&b"CCCC"[..]));
        assert_eq!(reader.read(108, 104, 4), Some(&b"DDDD"[..]));
        assert_eq!(reader.read(4, 1, 2), Some(&b"AA"[..]));
        // Fetched after the listing was taken: one rebuild finds it.
        put(&seg, "vector.idx", 10, b"BBBB");
        assert_eq!(reader.read(14, 10, 4), Some(&b"BBBB"[..]));
        // A range no source holds is a miss.
        assert_eq!(reader.read(54, 50, 4), None);
        // Evicted after the listing: a miss once its handle is gone (a
        // reader that still holds the handle may keep serving the bytes).
        fs::remove_file(spans_dir(&seg, "vector.idx").join(span_file_name(0, 4))).unwrap();
        assert_eq!(reader.read(14, 10, 4), Some(&b"BBBB"[..]));
        assert_eq!(reader.read(4, 0, 4), None);
        // Promoted mid-run: the whole file lands and the spans are dropped.
        let mut whole = vec![b'w'; 108];
        whole[..4].copy_from_slice(b"WXYZ");
        fs::write(seg.join("vector.idx"), &whole).unwrap();
        fs::remove_dir_all(spans_dir(&seg, "vector.idx")).unwrap();
        assert_eq!(reader.read(4, 0, 4), Some(&b"WXYZ"[..]));
        assert_eq!(reader.read(54, 50, 4), Some(&b"wwww"[..]));
        let _ = fs::remove_dir_all(&seg);
    }

    #[test]
    fn range_reader_agrees_with_read_local_range() {
        let seg = tmp("reader-agree");
        let bytes: Vec<u8> = (0..200u32).map(|i| (i * 7 % 251) as u8).collect();
        put(&seg, "f.bin", 20, &bytes[20..90]);
        put(&seg, "f.bin", 80, &bytes[80..160]);
        let mut reader = LocalRangeReader::new(&seg, "f.bin");
        for start in (0..200u64).step_by(7) {
            for len in [0u64, 1, 9, 40, 90] {
                let expect = read_local_range(&seg, "f.bin", start + len, start, len);
                let got = reader.read(start + len, start, len);
                if let Some(got) = got.filter(|got| !got.is_empty()) {
                    assert_eq!(got.as_ptr() as usize % 4, 0, "aligned [{start}, +{len})");
                }
                assert_eq!(got.map(<[u8]>::to_vec), expect, "[{start}, +{len})");
            }
        }
        let _ = fs::remove_dir_all(&seg);
    }
}
