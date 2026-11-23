//! Ranged page materialization from S3 (#245, "Option B").
//!
//! A cold search hydrates only the scoring set; the documents of the
//! returned page are fetched afterwards (see
//! `kosha_query::DocStoreHydrator`). Before #245 that meant the whole
//! `doc_store.bin` (+ `doc_vectors.bin`) of every segment holding a page
//! hit, hundreds of MB to return ten documents. This module fetches just
//! the page documents' byte ranges instead:
//!
//!   1. Spans come from the open segment: `doc_store.offsets` gives each
//!      record's `(offset, length)`; for a sidecar segment the
//!      `doc_vectors.bin` header (fetched once with a ranged read of its
//!      first [`DOC_VECTORS_HEADER_PROBE`] bytes, plus the remainder if the
//!      header is longer) gives each document's vector range.
//!   2. Spans of one file closer than `coalesce_gap` are merged into one
//!      GET, and every GET of the page goes out concurrently, so a cold
//!      page costs one round trip (two for a sidecar segment's first page).
//!   3. Every fetched range is persisted into the sparse span cache
//!      (`kosha_segment::span_cache`) under the disk cache's accounting, so
//!      it counts toward `KOSHA_CACHE_MAX_BYTES` and is LRU-evictable, and
//!      a warm repeat reads it locally with zero S3 requests — the property
//!      PR #84 established and that the earlier, non-persisting ranged path
//!      lacked.
//!   4. A segment that keeps getting ranged reads (`promote_after_gets`
//!      GETs or `promote_after_bytes` bytes) is promoted: the whole file is
//!      hydrated in the background, and once it lands its spans are
//!      dropped. The whole file always wins over spans when both exist.
//!
//! Segments the ranged path cannot serve fall back to the whole file:
//! legacy segments without `doc_store.offsets` (their records are parsed
//! eagerly, so they never ask), and any segment whose ranged fetch fails.
//!
//! `KOSHA_DOC_STORE_READ_MODE=whole` restores the pre-#245 whole-file path.
#![cfg_attr(not(feature = "s3"), allow(dead_code))]

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};

use kosha_cache::Cache;
use kosha_core::KoshaError;
use kosha_query::PageSegment;
use kosha_segment::span_cache::{self, SpanIndex};
use kosha_segment::{
    doc_vectors_header_len, DOC_STORE_FILE, DOC_VECTORS_FILE, DOC_VECTORS_HEADER_PROBE,
};

/// How page documents are fetched (`KOSHA_DOC_STORE_READ_MODE`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ReadMode {
    /// Whole `doc_store.bin` (+ `doc_vectors.bin`) per page segment — the
    /// pre-#245 path, kept for rollback.
    Whole,
    /// Ranged GETs into the sparse span cache (#245).
    #[default]
    Ranged,
}

impl ReadMode {
    pub fn from_env() -> Self {
        Self::from_env_var("KOSHA_DOC_STORE_READ_MODE")
    }

    /// The same `whole|ranged` switch under another variable (postings use
    /// `KOSHA_POSTINGS_READ_MODE`).
    pub fn from_env_var(var: &str) -> Self {
        Self::parse_var(var, std::env::var(var).ok().as_deref())
    }

    #[cfg(test)]
    pub(crate) fn parse(raw: Option<&str>) -> Self {
        Self::parse_var("KOSHA_DOC_STORE_READ_MODE", raw)
    }

    fn parse_var(var: &str, raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") | Some("ranged") => Self::Ranged,
            Some("whole") => Self::Whole,
            Some(other) => {
                eprintln!("WARN: {var}={other} not whole|ranged — using ranged");
                Self::Ranged
            }
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Whole => "whole",
            Self::Ranged => "ranged",
        }
    }
}

/// Tuning for the ranged path. A zero promotion threshold disables that
/// trigger; both zero disables promotion.
#[derive(Clone, Debug)]
pub struct SpanConfig {
    /// Spans of one file at most this far apart share a GET
    /// (`KOSHA_DOC_SPAN_COALESCE_GAP_BYTES`, default 64 KiB).
    pub coalesce_gap: u64,
    /// Promote a segment to whole-file hydration after this many ranged
    /// GETs (`KOSHA_DOC_SPAN_PROMOTE_GETS`, default 64).
    pub promote_after_gets: u64,
    /// ... or after this many ranged bytes
    /// (`KOSHA_DOC_SPAN_PROMOTE_BYTES`, default 16 MiB).
    pub promote_after_bytes: u64,
}

impl Default for SpanConfig {
    fn default() -> Self {
        Self {
            coalesce_gap: 64 * 1024,
            promote_after_gets: 64,
            promote_after_bytes: 16 * 1024 * 1024,
        }
    }
}

impl SpanConfig {
    pub fn from_env() -> Self {
        Self::from_env_prefix("KOSHA_DOC_SPAN", Self::default())
    }

    /// `{prefix}_COALESCE_GAP_BYTES`, `{prefix}_PROMOTE_GETS` and
    /// `{prefix}_PROMOTE_BYTES`, each falling back to `defaults`.
    pub fn from_env_prefix(prefix: &str, defaults: Self) -> Self {
        let get = |key: &str, default: u64| {
            std::env::var(format!("{prefix}_{key}"))
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(default)
        };
        Self {
            coalesce_gap: get("COALESCE_GAP_BYTES", defaults.coalesce_gap),
            promote_after_gets: get("PROMOTE_GETS", defaults.promote_after_gets),
            promote_after_bytes: get("PROMOTE_BYTES", defaults.promote_after_bytes),
        }
    }
}

/// One GET per range: `(logical path under the data dir, offset, length)`.
/// Results are index-aligned with the input. A result may be shorter than
/// asked only when the range runs past the end of the object.
pub trait RangeFetcher {
    fn fetch(&self, ranges: &[(String, u64, u64)]) -> Vec<Result<Vec<u8>, KoshaError>>;
}

/// What one query's page hydration fetched. Logged on the `search timing:`
/// line and summed into [`DocStoreReadCounters`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PageFetchStats {
    /// Sequential S3 round trips (batches of concurrent GETs) the page cost.
    pub rounds: u64,
    pub ranged_gets: u64,
    pub ranged_bytes: u64,
    pub whole_files: u64,
    pub whole_bytes: u64,
}

impl PageFetchStats {
    pub fn add(&mut self, other: &Self) {
        self.rounds += other.rounds;
        self.ranged_gets += other.ranged_gets;
        self.ranged_bytes += other.ranged_bytes;
        self.whole_files += other.whole_files;
        self.whole_bytes += other.whole_bytes;
    }
}

/// Process-lifetime totals, reported by `GET /stats` (there is no
/// `/metrics` endpoint yet, #223).
#[derive(Default)]
pub struct DocStoreReadCounters {
    ranged_gets: AtomicU64,
    ranged_bytes: AtomicU64,
    whole_files: AtomicU64,
    whole_bytes: AtomicU64,
    promotions_queued: AtomicU64,
    promotions_done: AtomicU64,
}

impl DocStoreReadCounters {
    pub fn record(&self, stats: &PageFetchStats) {
        self.ranged_gets
            .fetch_add(stats.ranged_gets, Ordering::Relaxed);
        self.ranged_bytes
            .fetch_add(stats.ranged_bytes, Ordering::Relaxed);
        self.whole_files
            .fetch_add(stats.whole_files, Ordering::Relaxed);
        self.whole_bytes
            .fetch_add(stats.whole_bytes, Ordering::Relaxed);
    }

    pub fn promotion_queued(&self) {
        self.promotions_queued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn promotion_done(&self) {
        self.promotions_done.fetch_add(1, Ordering::Relaxed);
    }

    pub fn to_json(&self, mode: ReadMode) -> serde_json::Value {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        serde_json::json!({
            "mode": mode.as_str(),
            "ranged_gets": get(&self.ranged_gets),
            "ranged_bytes": get(&self.ranged_bytes),
            "whole_files": get(&self.whole_files),
            "whole_bytes": get(&self.whole_bytes),
            "promotions_queued": get(&self.promotions_queued),
            "promotions_done": get(&self.promotions_done),
        })
    }
}

/// Result of [`SpanHydrator::hydrate`].
#[derive(Debug, Default)]
pub struct SpanOutcome {
    /// Sequential batches of ranged GETs (S3 round trips).
    pub rounds: u64,
    /// Ranged GETs issued and bytes they returned.
    pub gets: u64,
    pub bytes: u64,
    /// Segments the ranged path could not serve; the caller fetches their
    /// whole files instead.
    pub whole_fallback: Vec<PathBuf>,
    /// Segments that just crossed a promotion threshold; the caller queues
    /// a background whole-file hydration and then calls
    /// [`SpanHydrator::finish_promotion`].
    pub promote: Vec<PathBuf>,
    /// A range was refused by the request's byte ceiling or the in-flight
    /// budget. Not retried as a whole-file fetch (that would be larger).
    pub shed: bool,
}

#[derive(Default)]
struct Heat {
    gets: u64,
    bytes: u64,
}

/// Fetches page documents' spans into the sparse span cache and decides
/// when a segment is hot enough to promote. One per process.
pub struct SpanHydrator {
    data_dir: PathBuf,
    cfg: SpanConfig,
    heat: Mutex<HashMap<PathBuf, Heat>>,
    /// Segments with a promotion queued or running.
    promoting: Mutex<HashSet<PathBuf>>,
}

/// Where one fetched range goes once it arrives.
struct Pending {
    seg: usize,
    file: &'static str,
    start: u64,
    /// `Some(len)`: the response must be exactly this long.
    exact: Option<u64>,
}

impl SpanHydrator {
    pub fn new(data_dir: PathBuf, cfg: SpanConfig) -> Self {
        Self {
            data_dir,
            cfg,
            heat: Mutex::new(HashMap::new()),
            promoting: Mutex::new(HashSet::new()),
        }
    }

    pub fn config(&self) -> &SpanConfig {
        &self.cfg
    }

    /// Ranged GETs and bytes `segment_dir` has drawn since its heat last
    /// reset — what put it over a promotion threshold, for the promotion's
    /// start line.
    pub fn heat(&self, segment_dir: &Path) -> Option<(u64, u64)> {
        let heat = self.heat.lock().unwrap();
        heat.get(segment_dir).map(|h| (h.gets, h.bytes))
    }

    fn rel(&self, segment_dir: &Path) -> Option<String> {
        segment_dir
            .strip_prefix(&self.data_dir)
            .ok()
            .map(|p| p.to_string_lossy().replace('\\', "/"))
    }

    /// Make every listed page document readable locally by fetching its
    /// spans. Two rounds at most: sidecar headers that are not local yet,
    /// then all record and vector spans of the page in one concurrent
    /// batch.
    pub fn hydrate(
        &self,
        pages: &[PageSegment<'_>],
        fetcher: &dyn RangeFetcher,
        cache: &Cache,
    ) -> SpanOutcome {
        let mut out = SpanOutcome::default();
        let mut fallback = vec![false; pages.len()];
        let mut seg_gets = vec![0u64; pages.len()];
        let mut seg_bytes = vec![0u64; pages.len()];
        let rels: Vec<Option<String>> = pages.iter().map(|p| self.rel(p.segment_dir)).collect();

        for (i, page) in pages.iter().enumerate() {
            // No offsets sidecar (legacy segment) or a segment outside the
            // data dir: nothing to range over.
            if rels[i].is_none() || page.reader.doc_store_len().is_none() {
                fallback[i] = true;
            }
        }

        // ── Round 1: sidecar headers ─────────────────────────────────────
        let header_segs: Vec<usize> = (0..pages.len())
            .filter(|&i| !fallback[i] && !pages[i].reader.has_local_doc_vectors_header())
            .collect();
        if !header_segs.is_empty() {
            let targets: Vec<(&Path, &str)> = header_segs
                .iter()
                .map(|&i| (pages[i].segment_dir, rels[i].as_deref().unwrap()))
                .collect();
            let fetched = fetch_doc_vectors_headers(&targets, fetcher, cache);
            out.rounds += fetched.rounds;
            out.shed |= fetched.shed;
            for (&i, seg) in header_segs.iter().zip(fetched.segments) {
                out.gets += seg.gets;
                out.bytes += seg.bytes;
                seg_gets[i] += seg.gets;
                seg_bytes[i] += seg.bytes;
                // A shed fetch is not retried as a whole file (larger).
                fallback[i] = seg.status == HeaderStatus::Failed;
            }
        }

        // ── Round 2: record + vector spans ───────────────────────────────
        let mut reqs: Vec<(String, u64, u64)> = Vec::new();
        let mut pending: Vec<Pending> = Vec::new();
        for (i, page) in pages.iter().enumerate() {
            if fallback[i] {
                continue;
            }
            let reader = page.reader;
            let dir = page.segment_dir;
            let ds_whole = reader.doc_store_len().is_some_and(|len| {
                std::fs::metadata(dir.join(DOC_STORE_FILE))
                    .is_ok_and(|m| m.len() > 0 && m.len() >= len)
            });
            let dv_whole = std::fs::metadata(dir.join(DOC_VECTORS_FILE)).is_ok_and(|m| m.len() > 0);
            let ds_idx = SpanIndex::load(dir, DOC_STORE_FILE);
            let dv_idx = SpanIndex::load(dir, DOC_VECTORS_FILE);
            let mut ds_ranges = Vec::new();
            let mut dv_ranges = Vec::new();
            for &doc_seq in &page.doc_seqs {
                match reader.doc_span(doc_seq) {
                    Some((off, len)) => {
                        let end = off + len as u64;
                        if !ds_whole && !ds_idx.covers(off, end) {
                            ds_ranges.push((off, end));
                        }
                    }
                    // Out of range: `doc_record_full` returns `None` for it
                    // and the page skips it; nothing to fetch.
                    None => continue,
                }
                if reader.uses_doc_vectors() {
                    match reader.doc_vectors_span(doc_seq) {
                        Ok(Some((s, e))) => {
                            if !dv_whole && !dv_idx.covers(s, e) {
                                dv_ranges.push((s, e));
                            }
                        }
                        Ok(None) => {}
                        Err(_) => {
                            fallback[i] = true;
                            break;
                        }
                    }
                }
            }
            if fallback[i] {
                continue;
            }
            let rel = rels[i].as_deref().unwrap();
            for (file, ranges) in [(DOC_STORE_FILE, ds_ranges), (DOC_VECTORS_FILE, dv_ranges)] {
                for (s, e) in span_cache::coalesce(ranges, self.cfg.coalesce_gap) {
                    reqs.push((format!("{rel}/{file}"), s, e - s));
                    pending.push(Pending {
                        seg: i,
                        file,
                        start: s,
                        exact: Some(e - s),
                    });
                }
            }
        }
        if !reqs.is_empty() {
            out.rounds += 1;
            let results = fetcher.fetch(&reqs);
            for (p, result) in pending.into_iter().zip(results) {
                out.gets += 1;
                seg_gets[p.seg] += 1;
                match result {
                    Ok(bytes) if p.exact.is_none_or(|n| n == bytes.len() as u64) => {
                        out.bytes += bytes.len() as u64;
                        seg_bytes[p.seg] += bytes.len() as u64;
                        let page = &pages[p.seg];
                        self.persist(
                            cache,
                            page.segment_dir,
                            &rels[p.seg],
                            p.file,
                            p.start,
                            &bytes,
                        );
                    }
                    Ok(bytes) => {
                        eprintln!(
                            "WARN: ranged read of {}/{} at {} returned {} bytes, expected {:?}; \
                             falling back to the whole file",
                            pages[p.seg].segment_dir.display(),
                            p.file,
                            p.start,
                            bytes.len(),
                            p.exact
                        );
                        fallback[p.seg] = true;
                    }
                    Err(KoshaError::Overloaded(_)) => out.shed = true,
                    Err(_) => fallback[p.seg] = true,
                }
            }
        }

        // ── Promotion bookkeeping ────────────────────────────────────────
        let mut heat = self.heat.lock().unwrap();
        let mut promoting = self.promoting.lock().unwrap();
        for (i, page) in pages.iter().enumerate() {
            if fallback[i] {
                out.whole_fallback.push(page.segment_dir.to_path_buf());
                continue;
            }
            if seg_gets[i] == 0 {
                continue;
            }
            let h = heat.entry(page.segment_dir.to_path_buf()).or_default();
            h.gets += seg_gets[i];
            h.bytes += seg_bytes[i];
            let hot = (self.cfg.promote_after_gets > 0 && h.gets >= self.cfg.promote_after_gets)
                || (self.cfg.promote_after_bytes > 0 && h.bytes >= self.cfg.promote_after_bytes);
            if hot && promoting.insert(page.segment_dir.to_path_buf()) {
                out.promote.push(page.segment_dir.to_path_buf());
            }
        }
        out
    }

    /// Write one fetched range into the span cache and account it in the
    /// disk cache. Written to a temp name and renamed, so a reader never
    /// sees a partial span (it would ignore one anyway: the name carries
    /// the length). Failure to persist is logged, not fatal: the page then
    /// fails to materialize and the client retries, like any IO error.
    fn persist(
        &self,
        cache: &Cache,
        segment_dir: &Path,
        rel: &Option<String>,
        file: &str,
        start: u64,
        bytes: &[u8],
    ) {
        if let Some(rel) = rel {
            persist_span(cache, segment_dir, rel, file, start, bytes);
        }
    }

    /// A background whole-file hydration for `segment_dir` finished. When
    /// the whole files `landed`, its spans are redundant (the whole file
    /// wins every read) and are dropped from disk and from the cache's
    /// accounting. Either way the segment's heat resets, so a failed
    /// promotion may be retried after another threshold's worth of reads.
    pub fn finish_promotion(&self, cache: &Cache, segment_dir: &Path, landed: bool) {
        self.promoting.lock().unwrap().remove(segment_dir);
        self.heat.lock().unwrap().remove(segment_dir);
        if !landed {
            return;
        }
        let Some(rel) = self.rel(segment_dir) else {
            return;
        };
        for file in [DOC_STORE_FILE, DOC_VECTORS_FILE] {
            let key = format!("{rel}/{file}{}", span_cache::SPANS_SUFFIX);
            if let Err(e) = cache.evict_dir_unless_pinned(&key) {
                eprintln!("WARN: failed to drop span cache {key}: {e}");
            }
        }
    }
}

/// Write one fetched range `[start, start + bytes.len())` of
/// `{segment_dir}/{file}` into the span cache and account it in the disk
/// cache under `{rel}/{file}.spans/…`. Written to a temp name and renamed,
/// so a reader never sees a partial span (it would ignore one anyway: the
/// name carries the length). Failure to persist is logged, not fatal: the
/// read that needed it then fails and the client retries, like any IO
/// error.
pub(crate) fn persist_span(
    cache: &Cache,
    segment_dir: &Path,
    rel: &str,
    file: &str,
    start: u64,
    bytes: &[u8],
) {
    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
    if bytes.is_empty() {
        return;
    }
    let dir = span_cache::spans_dir(segment_dir, file);
    let name = span_cache::span_file_name(start, start + bytes.len() as u64);
    let dest = dir.join(&name);
    if std::fs::metadata(&dest).is_ok_and(|m| m.len() == bytes.len() as u64) {
        return;
    }
    let tmp = dir.join(format!(
        ".tmp.{}.{}",
        std::process::id(),
        TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let written = std::fs::create_dir_all(&dir)
        .and_then(|_| std::fs::write(&tmp, bytes))
        .and_then(|_| std::fs::rename(&tmp, &dest));
    match written {
        Ok(()) => {
            cache.note_external_write(&format!("{rel}/{file}{}/{name}", span_cache::SPANS_SUFFIX))
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            eprintln!("WARN: failed to persist span {}: {e}", dest.display());
        }
    }
}

/// How fetching one segment's `doc_vectors.bin` header went — see
/// [`fetch_doc_vectors_headers`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderStatus {
    /// The header is now in the span cache.
    Cached,
    /// The object is missing, short or corrupt.
    Failed,
    /// Refused by the request's byte ceiling or the in-flight budget.
    Shed,
}

/// One segment's share of a [`fetch_doc_vectors_headers`] call.
#[derive(Debug, Clone, Copy)]
pub struct HeaderFetch {
    pub status: HeaderStatus,
    pub gets: u64,
    pub bytes: u64,
}

/// The result of [`fetch_doc_vectors_headers`]: one entry per target, in
/// order, plus the sequential rounds it took.
#[derive(Debug, Default)]
pub struct HeadersFetched {
    pub segments: Vec<HeaderFetch>,
    pub rounds: u64,
    pub shed: bool,
}

/// Whether `doc_vectors.bin`'s header is readable locally: the whole file,
/// or a span from offset 0 in the sparse span cache (only ever persisted
/// once it parsed as a complete header).
pub fn doc_vectors_header_local(segment_dir: &Path) -> bool {
    std::fs::metadata(segment_dir.join(DOC_VECTORS_FILE)).is_ok_and(|m| m.len() > 0)
        || SpanIndex::load(segment_dir, DOC_VECTORS_FILE)
            .spans()
            .iter()
            .any(|&(start, _)| start == 0)
}

/// Fetch the `doc_vectors.bin` header of each `(segment_dir, rel)` target
/// by range into the span cache — never the whole file. Two rounds at
/// most: a [`DOC_VECTORS_HEADER_PROBE`]-byte probe, then the rest of any
/// header longer than that.
pub fn fetch_doc_vectors_headers(
    targets: &[(&Path, &str)],
    fetcher: &dyn RangeFetcher,
    cache: &Cache,
) -> HeadersFetched {
    let mut out = HeadersFetched {
        segments: vec![
            HeaderFetch {
                status: HeaderStatus::Failed,
                gets: 0,
                bytes: 0,
            };
            targets.len()
        ],
        ..Default::default()
    };
    if targets.is_empty() {
        return out;
    }
    let key = |rel: &str| format!("{rel}/{DOC_VECTORS_FILE}");
    let probes: Vec<(String, u64, u64)> = targets
        .iter()
        .map(|(_, rel)| (key(rel), 0, DOC_VECTORS_HEADER_PROBE))
        .collect();
    out.rounds += 1;
    let mut rest: Vec<(usize, Vec<u8>, u64)> = Vec::new();
    for (i, result) in fetcher.fetch(&probes).into_iter().enumerate() {
        let seg = &mut out.segments[i];
        seg.gets += 1;
        let prefix = match result {
            Ok(bytes) => bytes,
            Err(e) => {
                if matches!(e, KoshaError::Overloaded(_)) {
                    seg.status = HeaderStatus::Shed;
                    out.shed = true;
                }
                continue;
            }
        };
        seg.bytes += prefix.len() as u64;
        match doc_vectors_header_len(&prefix) {
            Ok(Ok(_)) => {
                let (dir, rel) = targets[i];
                persist_span(cache, dir, rel, DOC_VECTORS_FILE, 0, &prefix);
                seg.status = HeaderStatus::Cached;
            }
            // A full probe that ended inside the header: fetch the rest. A
            // short probe was the whole file, so a header still incomplete
            // there is corrupt.
            Ok(Err(need)) if prefix.len() as u64 == DOC_VECTORS_HEADER_PROBE => {
                rest.push((i, prefix, need));
            }
            _ => {}
        }
    }
    if rest.is_empty() {
        return out;
    }
    let reqs: Vec<(String, u64, u64)> = rest
        .iter()
        .map(|(i, prefix, need)| {
            (
                key(targets[*i].1),
                prefix.len() as u64,
                need - prefix.len() as u64,
            )
        })
        .collect();
    out.rounds += 1;
    let results = fetcher.fetch(&reqs);
    for ((i, mut prefix, need), result) in rest.into_iter().zip(results) {
        let seg = &mut out.segments[i];
        seg.gets += 1;
        match result {
            Ok(bytes) => {
                seg.bytes += bytes.len() as u64;
                prefix.extend_from_slice(&bytes);
                if prefix.len() as u64 >= need
                    && matches!(doc_vectors_header_len(&prefix), Ok(Ok(_)))
                {
                    let (dir, rel) = targets[i];
                    persist_span(cache, dir, rel, DOC_VECTORS_FILE, 0, &prefix);
                    seg.status = HeaderStatus::Cached;
                }
            }
            Err(e) => {
                if matches!(e, KoshaError::Overloaded(_)) {
                    seg.status = HeaderStatus::Shed;
                    out.shed = true;
                }
            }
        }
    }
    out
}

/// Segments waiting for a background whole-file promotion. FIFO; the
/// [`SpanHydrator`] already dedups, so a segment is queued at most once at
/// a time.
#[derive(Default)]
pub struct PromotionQueue {
    queue: Mutex<VecDeque<PathBuf>>,
    cv: Condvar,
}

impl PromotionQueue {
    pub fn push(&self, segment_dir: PathBuf) {
        self.queue.lock().unwrap().push_back(segment_dir);
        self.cv.notify_one();
    }

    /// The next queued segment, waiting up to `timeout` for one.
    pub fn pop_timeout(&self, timeout: std::time::Duration) -> Option<PathBuf> {
        let q = self.queue.lock().unwrap();
        let (mut q, _) = self
            .cv
            .wait_timeout_while(q, timeout, |q| q.is_empty())
            .unwrap();
        q.pop_front()
    }

    pub fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }
}

/// [`RangeFetcher`] over S3 ranged GETs, charged to one request's
/// accountant and the process-wide in-flight byte budget.
#[cfg(feature = "s3")]
pub struct S3RangeFetcher<'a> {
    pub s3: &'a crate::s3_storage::S3Storage,
    pub concurrency: usize,
    pub budget: &'a std::sync::Arc<crate::hydration_budget::HydrationBudget>,
    pub request: &'a std::sync::Arc<crate::hydration_budget::RequestHydration>,
}

#[cfg(feature = "s3")]
impl RangeFetcher for S3RangeFetcher<'_> {
    fn fetch(&self, ranges: &[(String, u64, u64)]) -> Vec<Result<Vec<u8>, KoshaError>> {
        self.s3
            .read_ranges(ranges, self.concurrency, Some(self.budget), self.request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use kosha_core::{
        Bm25Params, DocumentId, Field, Manifest, ManifestEntry, NamespaceId, SearchQuery, SegmentId,
    };
    use kosha_query::Searcher;
    use kosha_segment::{SegmentReader, SegmentWriter};

    /// A fake object store over a local copy of the segments, counting
    /// every GET and the bytes it returns — the "counting fake S3" #245
    /// asks for. Ranges past EOF come back short, like S3.
    struct CountingStore {
        root: PathBuf,
        gets: AtomicU64,
        bytes: AtomicU64,
        fail: bool,
    }

    impl CountingStore {
        fn new(root: PathBuf) -> Self {
            Self {
                root,
                gets: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
                fail: false,
            }
        }
        fn gets(&self) -> u64 {
            self.gets.load(Ordering::SeqCst)
        }
        fn bytes(&self) -> u64 {
            self.bytes.load(Ordering::SeqCst)
        }
    }

    impl RangeFetcher for CountingStore {
        fn fetch(&self, ranges: &[(String, u64, u64)]) -> Vec<Result<Vec<u8>, KoshaError>> {
            ranges
                .iter()
                .map(|(key, off, len)| {
                    self.gets.fetch_add(1, Ordering::SeqCst);
                    if self.fail {
                        return Err(KoshaError::NotFound("injected".into()));
                    }
                    let all = std::fs::read(self.root.join(key))?;
                    let start = (*off as usize).min(all.len());
                    let end = (*off + *len).min(all.len() as u64) as usize;
                    self.bytes.fetch_add((end - start) as u64, Ordering::SeqCst);
                    Ok(all[start..end].to_vec())
                })
                .collect()
        }
    }

    struct Fixture {
        root: PathBuf,
        data: PathBuf,
        remote: PathBuf,
        ns: NamespaceId,
        manifest: Manifest,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn scratch(name: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kosha-doc-spans-{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// `segs` segments of `docs` documents with ~1 KB of text each. Docs
    /// whose index satisfies `needle(i)` contain the word "needle". With
    /// `vectors`, every doc also carries a 64-dim sidecar vector. The
    /// "remote" copy keeps everything; the local data dir keeps only the
    /// scoring set (no `doc_store.bin` / `doc_vectors.bin`), the state a
    /// cold query leaves.
    fn fixture(
        name: &str,
        segs: usize,
        docs: usize,
        vectors: bool,
        needle: impl Fn(usize) -> bool,
    ) -> Fixture {
        fixture_with(name, segs, docs, vectors, |_| false, needle)
    }

    /// [`fixture`] where segment `s` gets a zstd doc store (research #388)
    /// when `zstd(s)`.
    fn fixture_with(
        name: &str,
        segs: usize,
        docs: usize,
        vectors: bool,
        zstd: impl Fn(usize) -> bool,
        needle: impl Fn(usize) -> bool,
    ) -> Fixture {
        let root = scratch(name);
        let data = root.join("data");
        let remote = root.join("remote");
        let ns = NamespaceId("ns".into());
        let mut entries = Vec::new();
        for s in 0..segs {
            let id = SegmentId(format!("s{s}"));
            let remote_dir = remote.join(&ns.0).join(&id.0);
            let mut w = SegmentWriter::new(id.clone(), remote_dir.clone())
                .with_doc_store_zstd(zstd(s).then(kosha_segment::DocStoreZstdConfig::default));
            for i in 0..docs {
                let word = if needle(i) { "needle" } else { "hay" };
                let filler = format!(" filler{s}x{i}").repeat(60);
                let mut fields = vec![Field::text("t", format!("{word} {i}{filler}"))];
                if vectors {
                    fields.push(Field::vector(
                        "emb",
                        (0..64).map(|d| (i * 64 + d) as f32 * 0.5).collect(),
                    ));
                }
                w.add_document(DocumentId(format!("s{s}-d{i}")), fields);
            }
            w.finalize(Bm25Params::default()).unwrap();
            let local_dir = data.join(&ns.0).join(&id.0);
            std::fs::create_dir_all(&local_dir).unwrap();
            for entry in std::fs::read_dir(&remote_dir).unwrap().flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name == DOC_STORE_FILE || name == DOC_VECTORS_FILE || entry.path().is_dir() {
                    continue;
                }
                std::fs::copy(entry.path(), local_dir.join(&name)).unwrap();
            }
            entries.push(ManifestEntry {
                segment_id: id,
                doc_count: docs as u32,
            });
        }
        Fixture {
            root,
            data,
            remote,
            ns,
            manifest: Manifest {
                version: 1,
                segments: entries,
                segment_footers: Default::default(),
                footer_crc32: Default::default(),
                wal_checkpoint: 0,
                wal_origin: None,
                tombstones: Default::default(),
                deletion_vectors: Default::default(),
                schema: None,
                retired: Default::default(),
                shards: None,
                branch: None,
            },
        }
    }

    fn query(text: &str, k: usize) -> SearchQuery {
        serde_json::from_value(serde_json::json!({"query_text": text, "max_results": k})).unwrap()
    }

    /// Run one search whose page hydration goes through `hydrator`,
    /// returning the result and the summed outcome.
    fn search(
        f: &Fixture,
        searcher: &Searcher,
        hydrator: &SpanHydrator,
        store: &CountingStore,
        cache: &Cache,
        q: &SearchQuery,
    ) -> (kosha_core::SearchResult, SpanOutcome, usize) {
        let outcome = Mutex::new(SpanOutcome::default());
        let calls = Mutex::new(0usize);
        let ensure = |pages: &[PageSegment<'_>]| {
            *calls.lock().unwrap() += 1;
            let o = hydrator.hydrate(pages, store, cache);
            let mut acc = outcome.lock().unwrap();
            acc.gets += o.gets;
            acc.bytes += o.bytes;
            acc.whole_fallback.extend(o.whole_fallback);
            acc.promote.extend(o.promote);
        };
        let (result, _) = searcher
            .search_with_doc_store_hydrator(&f.ns, &f.manifest, q, None, Some(&ensure))
            .expect("search must materialize its page");
        (
            result,
            outcome.into_inner().unwrap(),
            calls.into_inner().unwrap(),
        )
    }

    fn doc_store_size(f: &Fixture, seg: usize) -> u64 {
        std::fs::metadata(
            f.remote
                .join(&f.ns.0)
                .join(format!("s{seg}"))
                .join(DOC_STORE_FILE),
        )
        .unwrap()
        .len()
    }

    #[test]
    fn read_mode_parses_with_ranged_default() {
        assert_eq!(ReadMode::parse(None), ReadMode::Ranged);
        assert_eq!(ReadMode::parse(Some("WHOLE")), ReadMode::Whole);
        assert_eq!(ReadMode::parse(Some(" ranged ")), ReadMode::Ranged);
        assert_eq!(ReadMode::parse(Some("bogus")), ReadMode::Ranged);
    }

    /// Acceptance (#245): a cold top-10 query makes about one GET per page
    /// segment and fetches a small fraction of the doc store; the warm
    /// repeat makes zero GETs (the PR #84 regression guard).
    #[test]
    fn cold_page_is_one_get_per_segment_and_warm_repeat_is_free() {
        // Hits cluster in docs 1000..1004 of each segment, ~5 KB apart.
        let f = fixture("cold", 3, 2000, false, |i| (1000..1004).contains(&i));
        let store = CountingStore::new(f.remote.clone());
        let cache = Cache::new(f.data.clone());
        let scoring_set = cache.total_size();
        let hydrator = SpanHydrator::new(f.data.clone(), SpanConfig::default());
        let searcher = Searcher::new(f.data.clone());
        let q = query("needle", 10);

        let (r, o, calls) = search(&f, &searcher, &hydrator, &store, &cache, &q);
        assert_eq!(r.results.len(), 10);
        for doc in &r.results {
            let (seg, i) = doc.doc_id.0.split_once("-d").unwrap();
            assert!(doc.fields[0]
                .value
                .starts_with(&format!("needle {i} filler{}", &seg[1..])));
        }
        assert_eq!(calls, 1);
        let page_segments = 3;
        assert_eq!(
            store.gets(),
            page_segments,
            "one coalesced GET per page segment"
        );
        assert_eq!(o.gets, store.gets());
        assert_eq!(o.bytes, store.bytes());
        let total: u64 = (0..3).map(|s| doc_store_size(&f, s)).sum();
        println!(
            "cold top-10: {} GETs, {} bytes ranged vs {total} bytes of doc_store.bin",
            store.gets(),
            store.bytes()
        );
        assert!(
            store.bytes() * 50 < total,
            "fetched {} of {total} doc-store bytes",
            store.bytes()
        );
        assert!(o.whole_fallback.is_empty());
        // Never a partial file at the real path; spans are counted by the
        // disk cache.
        for s in 0..3 {
            let seg = f.data.join(&f.ns.0).join(format!("s{s}"));
            assert!(!seg.join(DOC_STORE_FILE).exists());
            assert!(!SpanIndex::load(&seg, DOC_STORE_FILE).is_empty());
        }
        assert_eq!(cache.total_size() - scoring_set, store.bytes());

        // Warm repeat, same process and a fresh one (spans are on disk).
        let (r2, _, calls2) = search(&f, &searcher, &hydrator, &store, &cache, &q);
        assert_eq!(r2.results.len(), 10);
        assert_eq!(calls2, 0, "every page document is local");
        assert_eq!(store.gets(), page_segments, "warm repeat makes zero GETs");
        let restarted = Searcher::new(f.data.clone());
        let (r3, _, calls3) = search(&f, &restarted, &hydrator, &store, &cache, &q);
        assert_eq!(r3.results.len(), 10);
        assert_eq!(calls3, 0);
        assert_eq!(store.gets(), page_segments);
    }

    #[test]
    fn coalescing_gap_controls_gets_per_segment() {
        // Four hits spread ~250 KB apart in one segment.
        let f = fixture("gap", 1, 1000, false, |i| i % 250 == 3);
        let q = query("needle", 10);
        for (gap, want) in [(0u64, 4u64), (u64::MAX / 4, 1)] {
            let data = f.data.join(&f.ns.0).join("s0");
            let _ = std::fs::remove_dir_all(span_cache::spans_dir(&data, DOC_STORE_FILE));
            let store = CountingStore::new(f.remote.clone());
            let cache = Cache::new(f.data.clone());
            let cfg = SpanConfig {
                coalesce_gap: gap,
                ..SpanConfig::default()
            };
            let hydrator = SpanHydrator::new(f.data.clone(), cfg);
            let searcher = Searcher::new(f.data.clone());
            let (r, _, _) = search(&f, &searcher, &hydrator, &store, &cache, &q);
            assert_eq!(r.results.len(), 4);
            assert_eq!(store.gets(), want, "gap {gap}");
        }
    }

    /// Sidecar segments (#243): the vectors header is fetched by a ranged
    /// read, then each page doc's vectors along with its record; vectors
    /// come back intact, and the warm repeat is free.
    #[test]
    fn sidecar_vectors_are_served_from_spans() {
        let f = fixture("sidecar", 2, 6000, true, |i| i == 5500 || i == 5501);
        let seg0 = f.remote.join(&f.ns.0).join("s0");
        assert!(
            seg0.join(DOC_VECTORS_FILE).exists(),
            "fixture must write a sidecar"
        );
        let dv_size = std::fs::metadata(seg0.join(DOC_VECTORS_FILE))
            .unwrap()
            .len();
        // 6000 entries x 12 B: a header longer than one probe.
        const { assert!(6000 * 12 > DOC_VECTORS_HEADER_PROBE) };
        let store = CountingStore::new(f.remote.clone());
        let cache = Cache::new(f.data.clone());
        let hydrator = SpanHydrator::new(f.data.clone(), SpanConfig::default());
        let searcher = Searcher::new(f.data.clone());
        let q = query("needle", 10);

        let (r, o, _) = search(&f, &searcher, &hydrator, &store, &cache, &q);
        assert_eq!(r.results.len(), 4);
        for doc in &r.results {
            let i: usize = doc.doc_id.0.split_once("-d").unwrap().1.parse().unwrap();
            let emb = doc.fields.iter().find(|fl| fl.name == "emb").unwrap();
            let v: Vec<f32> = serde_json::from_str(&emb.value).unwrap();
            assert_eq!(
                v,
                (0..64)
                    .map(|d| (i * 64 + d) as f32 * 0.5)
                    .collect::<Vec<_>>()
            );
        }
        // Per segment: header probe + header rest + one record GET + one
        // vector GET.
        assert_eq!(store.gets(), 2 * 4);
        let ds_size = doc_store_size(&f, 0) + doc_store_size(&f, 1);
        println!(
            "cold sidecar page: {} GETs, {} bytes ranged vs {} bytes of doc_store.bin + \
             doc_vectors.bin",
            store.gets(),
            store.bytes(),
            ds_size + 2 * dv_size
        );
        assert!(o.whole_fallback.is_empty());
        assert!(
            store.bytes() * 5 < 2 * dv_size,
            "fetched {} bytes",
            store.bytes()
        );
        for s in 0..2 {
            let seg = f.data.join(&f.ns.0).join(format!("s{s}"));
            assert!(!seg.join(DOC_VECTORS_FILE).exists());
        }

        let before = store.gets();
        let (r2, _, calls) = search(&f, &searcher, &hydrator, &store, &cache, &q);
        assert_eq!(r2.results.len(), 4);
        assert_eq!(calls, 0);
        assert_eq!(store.gets(), before);
    }

    /// Research #388: in a namespace mixing a zstd doc store (s0) with a
    /// raw one (s1), a cold page still costs one ranged GET per segment for
    /// the records (plus the vector header and spans), fetches exactly the
    /// page documents' stored spans, which are smaller in the compressed
    /// segment, and materializes every field and vector intact; the warm
    /// repeat is free.
    #[test]
    fn mixed_format_namespace_serves_compressed_spans() {
        let f = fixture_with(
            "zstd-mixed",
            2,
            3000,
            true,
            |s| s == 0,
            |i| (1500..1503).contains(&i),
        );
        let readers: Vec<SegmentReader> = (0..2)
            .map(|s| SegmentReader::open(f.remote.join(&f.ns.0).join(format!("s{s}"))).unwrap())
            .collect();
        assert!(readers[0].doc_store_compressed());
        assert!(!readers[1].doc_store_compressed());
        let page_span = |r: &SegmentReader| -> u64 {
            (1500..1503u32)
                .map(|i| r.doc_span(i).unwrap().1 as u64)
                .sum()
        };
        let (z_span, raw_span) = (page_span(&readers[0]), page_span(&readers[1]));
        assert!(
            z_span < raw_span,
            "compressed spans {z_span} vs raw {raw_span}"
        );

        let store = CountingStore::new(f.remote.clone());
        let cache = Cache::new(f.data.clone());
        let hydrator = SpanHydrator::new(f.data.clone(), SpanConfig::default());
        let searcher = Searcher::new(f.data.clone());
        let q = query("needle", 10);
        let (r, o, _) = search(&f, &searcher, &hydrator, &store, &cache, &q);
        assert_eq!(r.results.len(), 6);
        for doc in &r.results {
            let (seg, i) = doc.doc_id.0.split_once("-d").unwrap();
            let i: usize = i.parse().unwrap();
            let t = doc.fields.iter().find(|fl| fl.name == "t").unwrap();
            assert!(t
                .value
                .starts_with(&format!("needle {i} filler{}", &seg[1..])));
            let emb = doc.fields.iter().find(|fl| fl.name == "emb").unwrap();
            let v: Vec<f32> = serde_json::from_str(&emb.value).unwrap();
            assert_eq!(
                v,
                (0..64)
                    .map(|d| (i * 64 + d) as f32 * 0.5)
                    .collect::<Vec<_>>()
            );
        }
        assert!(o.whole_fallback.is_empty());
        // Per segment: vector header probe, one coalesced record GET, one
        // coalesced vector GET (3000 x 12 B header fits one probe).
        assert_eq!(store.gets(), 2 * 3);
        let ds_bytes = |s: usize| {
            let seg = f.data.join(&f.ns.0).join(format!("s{s}"));
            assert!(!seg.join(DOC_STORE_FILE).exists());
            SpanIndex::load(&seg, DOC_STORE_FILE).total_bytes()
        };
        // Only the page's own stored records were fetched (they are
        // adjacent, so one span each, coalesced exactly).
        assert_eq!(ds_bytes(0), z_span);
        assert_eq!(ds_bytes(1), raw_span);
        println!(
            "mixed cold page: record bytes zstd {z_span} vs raw {raw_span}; doc_store.bin \
             {} (zstd) vs {} (raw)",
            doc_store_size(&f, 0),
            doc_store_size(&f, 1)
        );

        let before = store.gets();
        let (r2, _, calls) = search(&f, &searcher, &hydrator, &store, &cache, &q);
        assert_eq!(r2.results.len(), 6);
        assert_eq!(calls, 0);
        assert_eq!(store.gets(), before);
    }

    /// Promotion: after N ranged GETs the segment is handed back once for
    /// whole-file hydration; when that lands its spans are dropped (and
    /// un-counted) and reads come from the whole file.
    #[test]
    fn hot_segment_is_promoted_and_its_spans_dropped() {
        let f = fixture("promote", 1, 1000, false, |i| i % 100 == 0);
        let store = CountingStore::new(f.remote.clone());
        let cache = Cache::new(f.data.clone());
        let scoring_set = cache.total_size();
        let cfg = SpanConfig {
            coalesce_gap: 0,
            promote_after_gets: 3,
            promote_after_bytes: 0,
        };
        let hydrator = SpanHydrator::new(f.data.clone(), cfg);
        let searcher = Searcher::new(f.data.clone());
        let seg = f.data.join(&f.ns.0).join("s0");

        // Page 1 (2 docs → 2 GETs): below the threshold.
        let mut q = query("needle", 2);
        let (_, o1, _) = search(&f, &searcher, &hydrator, &store, &cache, &q);
        assert_eq!(o1.gets, 2);
        assert!(o1.promote.is_empty());
        // Page 2 (next 2 docs): crosses 3 GETs → promoted exactly once.
        q.from = 2;
        let (_, o2, _) = search(&f, &searcher, &hydrator, &store, &cache, &q);
        assert_eq!(o2.promote, vec![seg.clone()]);
        // Still hot while the promotion is pending: not queued again.
        q.from = 4;
        let (_, o3, _) = search(&f, &searcher, &hydrator, &store, &cache, &q);
        assert!(o3.gets > 0);
        assert!(o3.promote.is_empty());

        // The background hydration lands the whole file.
        let spans_before = SpanIndex::load(&seg, DOC_STORE_FILE).total_bytes();
        assert!(spans_before > 0);
        assert_eq!(cache.total_size() - scoring_set, spans_before);
        std::fs::copy(
            f.remote.join(&f.ns.0).join("s0").join(DOC_STORE_FILE),
            seg.join(DOC_STORE_FILE),
        )
        .unwrap();
        hydrator.finish_promotion(&cache, &seg, true);
        assert!(!span_cache::spans_dir(&seg, DOC_STORE_FILE).exists());
        assert_eq!(
            cache.total_size(),
            scoring_set,
            "dropped spans leave the cache total"
        );

        // Every page now reads from the whole file: no hydrator call.
        let gets = store.gets();
        q.from = 0;
        q.max_results = 10;
        let (r, _, calls) = search(&f, &searcher, &hydrator, &store, &cache, &q);
        assert_eq!(r.results.len(), 10);
        assert_eq!(calls, 0);
        assert_eq!(store.gets(), gets);
    }

    #[test]
    fn failed_ranges_and_legacy_segments_fall_back_to_whole_files() {
        let f = fixture("fallback", 1, 50, false, |i| i < 3);
        let mut store = CountingStore::new(f.remote.clone());
        store.fail = true;
        let cache = Cache::new(f.data.clone());
        let hydrator = SpanHydrator::new(f.data.clone(), SpanConfig::default());
        let seg = f.data.join(&f.ns.0).join("s0");
        let reader = SegmentReader::open(seg.clone()).unwrap();
        let pages = [PageSegment {
            segment_dir: &seg,
            reader: &reader,
            doc_seqs: vec![0, 1, 2],
        }];
        let o = hydrator.hydrate(&pages, &store, &cache);
        assert_eq!(o.whole_fallback, vec![seg.clone()]);
        assert!(o.promote.is_empty());

        // Legacy: no `doc_store.offsets` → eager records, no spans to range
        // over (whole-file path, no GET).
        let legacy = f.root.join("legacy").join("ns").join("s0");
        std::fs::create_dir_all(&legacy).unwrap();
        let remote_seg = f.remote.join(&f.ns.0).join("s0");
        for entry in std::fs::read_dir(&remote_seg).unwrap().flatten() {
            if entry.path().is_file() && entry.file_name() != "doc_store.offsets" {
                std::fs::copy(entry.path(), legacy.join(entry.file_name())).unwrap();
            }
        }
        let legacy_reader = SegmentReader::open(legacy.clone()).unwrap();
        assert_eq!(legacy_reader.doc_store_len(), None);
        let store = CountingStore::new(f.remote.clone());
        let hydrator = SpanHydrator::new(f.root.join("legacy"), SpanConfig::default());
        let pages = [PageSegment {
            segment_dir: &legacy,
            reader: &legacy_reader,
            doc_seqs: vec![0],
        }];
        let o = hydrator.hydrate(&pages, &store, &cache);
        assert_eq!(o.whole_fallback, vec![legacy]);
        assert_eq!(store.gets(), 0);
    }

    #[test]
    fn promotion_queue_pops_in_order_and_times_out_empty() {
        let q = PromotionQueue::default();
        assert_eq!(q.pop_timeout(std::time::Duration::from_millis(1)), None);
        q.push(PathBuf::from("a"));
        q.push(PathBuf::from("b"));
        assert_eq!(q.len(), 2);
        assert_eq!(
            q.pop_timeout(std::time::Duration::ZERO),
            Some(PathBuf::from("a"))
        );
        assert_eq!(
            q.pop_timeout(std::time::Duration::ZERO),
            Some(PathBuf::from("b"))
        );
    }

    /// A legacy segment's `doc_vectors.bin` binds its unnamed vector index
    /// to a field: a kNN needs only that header, fetched by range into the
    /// span cache, never the whole file (as large as the index itself).
    #[test]
    fn legacy_doc_vectors_header_is_fetched_by_range_and_binds_the_index() {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../kosha-segment/tests/fixtures/legacy-vector-seg");
        let root = std::env::temp_dir().join(format!(
            "kosha-dv-header-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let (remote, data) = (root.join("remote"), root.join("data"));
        let (remote_seg, seg) = (remote.join("ns/legacy-vec"), data.join("ns/legacy-vec"));
        std::fs::create_dir_all(&remote_seg).unwrap();
        std::fs::create_dir_all(&seg).unwrap();
        for entry in std::fs::read_dir(&fixture).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap();
            std::fs::copy(&path, remote_seg.join(name)).unwrap();
            if name != DOC_VECTORS_FILE {
                std::fs::copy(&path, seg.join(name)).unwrap();
            }
        }
        assert!(!doc_vectors_header_local(&seg));

        let store = CountingStore::new(remote.clone());
        let cache = Cache::new(data.clone());
        let fetched = fetch_doc_vectors_headers(&[(&seg, "ns/legacy-vec")], &store, &cache);
        assert_eq!(fetched.segments[0].status, HeaderStatus::Cached);
        assert_eq!(fetched.rounds, 1);
        assert!(doc_vectors_header_local(&seg));
        assert!(!seg.join(DOC_VECTORS_FILE).exists(), "whole file fetched");
        let header_len = {
            let all = std::fs::read(remote_seg.join(DOC_VECTORS_FILE)).unwrap();
            doc_vectors_header_len(&all).unwrap().unwrap()
        };
        assert!(
            header_len
                < std::fs::metadata(remote_seg.join(DOC_VECTORS_FILE))
                    .unwrap()
                    .len()
        );

        // The header alone binds the unnamed index to `emb`.
        let reader = SegmentReader::open_with_options(seg.clone(), true).unwrap();
        assert_eq!(
            reader.vector_field_status("emb"),
            kosha_segment::VectorFieldStatus::Legacy { dim: Some(4) }
        );
        assert_eq!(
            reader.vector_field_status("title_emb"),
            kosha_segment::VectorFieldStatus::Absent
        );

        // A missing object fails the segment, without a whole-file fetch.
        let missing = data.join("ns/gone");
        std::fs::create_dir_all(&missing).unwrap();
        let fetched = fetch_doc_vectors_headers(&[(&missing, "ns/gone")], &store, &cache);
        assert_eq!(fetched.segments[0].status, HeaderStatus::Failed);
        std::fs::remove_dir_all(&root).ok();
    }
}
