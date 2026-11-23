//! Ranged posting reads from S3 (#245, part 2).
//!
//! Before this, a cold BM25 query fetched every whole `postings-XX.bin`
//! shard its terms hash to, in every candidate segment: a shard holds the
//! postings of ~1/256 of the segment's vocabulary, so a three-term query
//! pulled megabytes per segment to read a few KB of postings. Segments
//! written with raw shards (`Footer::posting_blobs_raw`, the writer default
//! since #245) store each shard's bytes exactly as the `inverted.idx` TOC
//! indexes them, so this module instead:
//!
//!   1. reads each query term's `(shard, postings_off, postings_len)` from
//!      the local TOC (`kosha_segment::posting_term_spans`; the TOC is part
//!      of the scoring scaffolding fetched one round earlier);
//!   2. drops spans already local (whole shard, or a cached span),
//!      coalesces the rest per shard (`coalesce_gap`) and fetches them all
//!      in one concurrent batch of ranged GETs — one S3 round trip;
//!   3. persists every range in the sparse span cache
//!      (`{segment}/postings-XX.bin.spans/`, written by the background span
//!      writer, see `span_writes`), under the disk cache's byte
//!      accounting, where `SplitInvertedIndex` reads it when the whole
//!      shard is absent. A warm repeat therefore makes no GET;
//!   4. promotes a shard that keeps being range-read (`promote_after_gets`
//!      / `promote_after_bytes`) to a whole local file in the background,
//!      then drops its spans. The whole file always wins over spans.
//!
//! A term span is a term's complete postings, positions included, so phrase
//! queries work unchanged. A range that fails is retried as the whole shard;
//! a term that is still unreadable when scored fails the search (the
//! reader's `blob_read_failure`), never silently scores as absent.
//!
//! Segments without the footer flag (written before #245, or with
//! `KOSHA_POSTINGS_RAW_BLOBS=0`) may hold zstd-wrapped shards whose byte
//! ranges mean nothing, so they keep whole-shard hydration, as do wildcard
//! queries (which read every term). `KOSHA_POSTINGS_READ_MODE=whole`
//! restores whole shards everywhere.
#![cfg_attr(not(feature = "s3"), allow(dead_code))]

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use kosha_cache::Cache;
use kosha_core::KoshaError;
use kosha_segment::span_cache::{self, SpanHolds, SpanIndex};

use crate::doc_spans::{RangeFetcher, ReadMode, SpanConfig};
use crate::span_writes::SpanWriteQueue;

/// Default tuning: postings of one shard are usually close together (terms
/// hash to shards, so one query's terms rarely share one), so the gap
/// mostly matters for repeated queries over neighbouring terms. A shard is
/// promoted after 32 ranged GETs or 8 MiB.
pub fn default_config() -> SpanConfig {
    SpanConfig {
        coalesce_gap: 64 * 1024,
        promote_after_gets: 32,
        promote_after_bytes: 8 * 1024 * 1024,
    }
}

/// `KOSHA_POSTING_SPAN_{COALESCE_GAP_BYTES,PROMOTE_GETS,PROMOTE_BYTES}`.
pub fn config_from_env() -> SpanConfig {
    SpanConfig::from_env_prefix("KOSHA_POSTING_SPAN", default_config())
}

/// `KOSHA_POSTINGS_READ_MODE=whole|ranged`, default `ranged`.
pub fn read_mode_from_env() -> ReadMode {
    ReadMode::from_env_var("KOSHA_POSTINGS_READ_MODE")
}

/// Key under which a term is recorded in the server's posting-blob presence
/// cache once its postings are known to be readable locally (or known to be
/// absent from the segment). Shard entries are bare file names
/// (`postings-XX.bin`), which never contain `:`, so the two cannot collide.
pub fn presence_key(term: &str) -> String {
    format!("term:{term}")
}

/// What one query's posting fetch did.
#[derive(Debug, Default)]
pub struct PostingSpanOutcome {
    /// Sequential batches of ranged GETs (0 or 1).
    pub rounds: u64,
    pub gets: u64,
    pub bytes: u64,
    /// `(segment rel path, presence key)` for every term now known readable
    /// locally or absent from that segment. Only span files on disk confirm
    /// a term: a span still in memory (its write queued or dropped) is
    /// confirmed by a later query's local check once it has landed.
    pub confirmed: Vec<(String, String)>,
    /// `(segment dir, shard file)` whose ranges failed; the caller fetches
    /// the whole shard instead.
    pub whole_fallback: Vec<(PathBuf, String)>,
    /// Why the first `whole_fallback` shard could not be served by range,
    /// for the fallback's log line. Set on the failure path only.
    pub fallback_error: Option<String>,
    /// Segments whose `inverted.idx` could not be read (`{rel}/inverted.idx`).
    pub missing: Vec<String>,
    /// Shard paths that just crossed a promotion threshold.
    pub promote: Vec<PathBuf>,
    /// A range was refused by the request's byte ceiling or the in-flight
    /// budget.
    pub shed: bool,
    /// Fetched spans the write queue could not take: readable only while
    /// held, so the caller keeps them until the query has been scored.
    pub held: SpanHolds,
}

#[derive(Default)]
struct Heat {
    gets: u64,
    bytes: u64,
}

/// Fetches query terms' posting spans into the span cache and tracks shard
/// heat for promotion. One per process.
pub struct PostingSpanHydrator {
    data_dir: PathBuf,
    cfg: SpanConfig,
    /// Keyed by shard path (`{segment}/postings-XX.bin`).
    heat: Mutex<HashMap<PathBuf, Heat>>,
    promoting: Mutex<HashSet<PathBuf>>,
}

struct Pending {
    seg: usize,
    file: String,
    start: u64,
    len: u64,
}

impl PostingSpanHydrator {
    pub fn new(data_dir: PathBuf, cfg: SpanConfig) -> Self {
        Self {
            data_dir,
            cfg,
            heat: Mutex::new(HashMap::new()),
            promoting: Mutex::new(HashSet::new()),
        }
    }

    pub fn coalesce_gap(&self) -> u64 {
        self.cfg.coalesce_gap
    }

    fn rel(&self, segment_dir: &Path) -> Option<String> {
        segment_dir
            .strip_prefix(&self.data_dir)
            .ok()
            .map(|p| p.to_string_lossy().replace('\\', "/"))
    }

    /// Make `terms`' postings readable locally in each of `segments` (all
    /// raw-shard segments whose `inverted.idx` is local), with at most one
    /// batch of ranged GETs.
    pub fn hydrate(
        &self,
        segments: &[PathBuf],
        terms: &[String],
        fetcher: &dyn RangeFetcher,
        writes: &SpanWriteQueue,
    ) -> PostingSpanOutcome {
        let mut out = PostingSpanOutcome::default();
        let mut reqs: Vec<(String, u64, u64)> = Vec::new();
        let mut pending: Vec<Pending> = Vec::new();
        // Per segment: the terms waiting on a fetch, with their span.
        let mut waiting: Vec<Vec<kosha_segment::TermSpan>> = vec![Vec::new(); segments.len()];
        let rels: Vec<Option<String>> = segments.iter().map(|s| self.rel(s)).collect();

        for (i, seg) in segments.iter().enumerate() {
            let Some(rel) = rels[i].as_deref() else {
                continue;
            };
            let spans = match kosha_segment::posting_term_spans(seg, terms) {
                Ok(spans) => spans,
                Err(e) => {
                    eprintln!("WARN: cannot plan posting spans for {rel}: {e}");
                    out.missing.push(format!("{rel}/inverted.idx"));
                    continue;
                }
            };
            // Terms the TOC lacks are absent from the segment: nothing to
            // fetch, ever.
            let present: HashSet<&str> = spans.iter().map(|s| s.term.as_str()).collect();
            for term in terms {
                if !present.contains(term.as_str()) {
                    out.confirmed.push((rel.to_string(), presence_key(term)));
                }
            }
            let mut by_file: HashMap<&str, Vec<(u64, u64)>> = HashMap::new();
            let mut indexes: HashMap<String, SpanIndex> = HashMap::new();
            for span in &spans {
                let end = span.start + span.len;
                let whole_local = std::fs::metadata(seg.join(&span.file))
                    .is_ok_and(|m| m.is_file() && m.len() >= end && m.len() > 0);
                let index = indexes
                    .entry(span.file.clone())
                    .or_insert_with(|| SpanIndex::load(seg, &span.file));
                if whole_local || index.covers_on_disk(span.start, end) {
                    out.confirmed
                        .push((rel.to_string(), presence_key(&span.term)));
                } else if index.covers(span.start, end) {
                    // Fetched by another query, its write still queued:
                    // readable now, on disk soon. Nothing to fetch, but not
                    // confirmed until it is on disk.
                } else {
                    by_file
                        .entry(span.file.as_str())
                        .or_default()
                        .push((span.start, end));
                    waiting[i].push(span.clone());
                }
            }
            let mut files: Vec<_> = by_file.into_iter().collect();
            files.sort_by(|a, b| a.0.cmp(b.0));
            for (file, ranges) in files {
                for (s, e) in
                    span_cache::coalesce_capped(ranges, self.cfg.coalesce_gap, MAX_RANGE_BYTES)
                {
                    reqs.push((format!("{rel}/{file}"), s, e - s));
                    pending.push(Pending {
                        seg: i,
                        file: file.to_string(),
                        start: s,
                        len: e - s,
                    });
                }
            }
        }
        if reqs.is_empty() {
            return out;
        }

        out.rounds = 1;
        let results = fetcher.fetch(&reqs);
        // Per (segment, shard): GETs and bytes, and whether any range failed.
        let mut shard_gets: HashMap<(usize, String), (u64, u64)> = HashMap::new();
        let mut failed: HashSet<(usize, String)> = HashSet::new();
        for (p, result) in pending.into_iter().zip(results) {
            out.gets += 1;
            let key = (p.seg, p.file.clone());
            let heat = shard_gets.entry(key.clone()).or_default();
            heat.0 += 1;
            match result {
                Ok(bytes) if bytes.len() as u64 == p.len => {
                    out.bytes += p.len;
                    heat.1 += p.len;
                    out.held.extend(writes.persist(
                        &segments[p.seg],
                        rels[p.seg].as_deref().unwrap_or_default(),
                        &p.file,
                        p.start,
                        bytes,
                    ));
                }
                Ok(bytes) => {
                    eprintln!(
                        "WARN: ranged read of {}/{} at {} returned {} bytes, expected {}; \
                         fetching the whole shard",
                        segments[p.seg].display(),
                        p.file,
                        p.start,
                        bytes.len(),
                        p.len
                    );
                    out.bytes += bytes.len() as u64;
                    out.fallback_error
                        .get_or_insert_with(|| format!("returned {} bytes", bytes.len()));
                    failed.insert(key);
                }
                Err(KoshaError::Overloaded(_)) => out.shed = true,
                Err(e) => {
                    out.fallback_error.get_or_insert_with(|| e.to_string());
                    failed.insert(key);
                }
            }
        }

        // Confirm terms whose span is now on disk (a write that landed
        // already; the rest are confirmed by a later query's local check).
        // A failed shard goes whole; its terms are confirmed by the next
        // query's local check.
        for (i, spans) in waiting.iter().enumerate() {
            let Some(rel) = rels[i].as_deref() else {
                continue;
            };
            let mut indexes: HashMap<&str, SpanIndex> = HashMap::new();
            for span in spans {
                if failed.contains(&(i, span.file.clone())) {
                    continue;
                }
                let index = indexes
                    .entry(span.file.as_str())
                    .or_insert_with(|| SpanIndex::load(&segments[i], &span.file));
                if index.covers_on_disk(span.start, span.start + span.len) {
                    out.confirmed
                        .push((rel.to_string(), presence_key(&span.term)));
                }
            }
        }
        let mut failed: Vec<_> = failed.into_iter().collect();
        failed.sort();
        for (i, file) in failed {
            out.whole_fallback.push((segments[i].clone(), file));
        }

        // Promotion bookkeeping, per shard.
        let mut heat = self.heat.lock().unwrap();
        let mut promoting = self.promoting.lock().unwrap();
        let mut shards: Vec<_> = shard_gets.into_iter().collect();
        shards.sort_by(|a, b| a.0.cmp(&b.0));
        for ((i, file), (gets, bytes)) in shards {
            let shard = segments[i].join(&file);
            let h = heat.entry(shard.clone()).or_default();
            h.gets += gets;
            h.bytes += bytes;
            let hot = (self.cfg.promote_after_gets > 0 && h.gets >= self.cfg.promote_after_gets)
                || (self.cfg.promote_after_bytes > 0 && h.bytes >= self.cfg.promote_after_bytes);
            if hot && promoting.insert(shard.clone()) {
                out.promote.push(shard);
            }
        }
        out
    }

    /// A background whole-shard hydration of `shard` finished. When it
    /// `landed`, the shard's spans are dropped (the whole file wins every
    /// read). Either way its heat resets.
    pub fn finish_promotion(&self, cache: &Cache, shard: &Path, landed: bool) {
        self.promoting.lock().unwrap().remove(shard);
        self.heat.lock().unwrap().remove(shard);
        if !landed {
            return;
        }
        let Some(rel) = self.rel(shard) else {
            return;
        };
        let key = format!("{rel}{}", span_cache::SPANS_SUFFIX);
        if let Err(e) = cache.evict_dir_unless_pinned(&key) {
            eprintln!("WARN: failed to drop span cache {key}: {e}");
        }
    }
}

/// Most bytes one coalesced ranged GET asks for — of kNN postings and
/// sidecar vectors ([`fetch_file_spans`]), term postings and page records
/// — a whole-object download's part size
/// (`s3_storage::DOWNLOAD_PART_BYTES`). A single input range larger than
/// this (one huge posting or record) is still one GET: ranges are never
/// split, a reader needs each inside one cached span.
pub(crate) const MAX_RANGE_BYTES: u64 = 16 * 1024 * 1024;
#[cfg(feature = "s3")]
const _: () = assert!(MAX_RANGE_BYTES == crate::s3_storage::DOWNLOAD_PART_BYTES);

/// Fetch `ranges` (`[start, end)`, any order) of `{segment_dir}/{file}`
/// into the span cache in one batch of coalesced ranged GETs, skipping what
/// is already cached. The writes are queued ([`SpanWriteQueue`]), never
/// done here: this runs inside a segment's scoring task on the rayon pool. Used for kNN postings of `vector.idx`, whose offsets
/// the reader takes from `vector.offsets` (#245). Returns the GETs, bytes,
/// and whether any range failed for a reason other than load shedding (the
/// caller then fetches the whole file) or was shed.
pub fn fetch_file_spans(
    rel: &str,
    segment_dir: &Path,
    file: &str,
    ranges: &[(u64, u64)],
    coalesce_gap: u64,
    fetcher: &dyn RangeFetcher,
    writes: &SpanWriteQueue,
) -> FileSpanOutcome {
    let mut out = FileSpanOutcome::default();
    let have = SpanIndex::load(segment_dir, file);
    let need: Vec<(u64, u64)> = ranges
        .iter()
        .copied()
        .filter(|&(s, e)| !have.covers(s, e))
        .collect();
    // Bounded like a whole-object download's parts: an exact kNN can ask
    // for every vector posting of a column, or every candidate vector of a
    // one-matter segment (~2 GB on staging), and as one coalesced range
    // that was a single GET held in memory whole.
    let merged = span_cache::coalesce_capped(need, coalesce_gap, MAX_RANGE_BYTES);
    if merged.is_empty() {
        return out;
    }
    let reqs: Vec<(String, u64, u64)> = merged
        .iter()
        .map(|&(s, e)| (format!("{rel}/{file}"), s, e - s))
        .collect();
    let results = fetcher.fetch(&reqs);
    for ((s, e), result) in merged.into_iter().zip(results) {
        out.gets += 1;
        match result {
            Ok(bytes) if bytes.len() as u64 == e - s => {
                out.bytes += bytes.len() as u64;
                out.held
                    .extend(writes.persist(segment_dir, rel, file, s, bytes));
            }
            Ok(bytes) => {
                out.bytes += bytes.len() as u64;
                out.error.get_or_insert_with(|| {
                    format!("returned {} bytes, expected {}", bytes.len(), e - s)
                });
                out.failed = true;
            }
            Err(KoshaError::Overloaded(_)) => out.shed = true,
            Err(err) => {
                out.error.get_or_insert_with(|| err.to_string());
                out.failed = true;
            }
        }
    }
    out
}

/// See [`fetch_file_spans`].
#[derive(Debug, Default, Clone)]
pub struct FileSpanOutcome {
    pub gets: u64,
    pub bytes: u64,
    pub failed: bool,
    /// The first failure's cause when `failed`, for the fallback log line.
    pub error: Option<String>,
    pub shed: bool,
    /// Fetched spans the write queue could not take: readable only while
    /// held, so the reader keeps them until it has read the ranges.
    pub held: SpanHolds,
}

/// Process-lifetime posting read totals for `GET /stats`.
#[derive(Default)]
pub struct PostingReadCounters {
    ranged_gets: AtomicU64,
    ranged_bytes: AtomicU64,
    whole_files: AtomicU64,
    whole_bytes: AtomicU64,
    promotions_queued: AtomicU64,
    promotions_done: AtomicU64,
}

/// One query's posting fetch, for the `search timing:` line.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PostingFetchStats {
    pub rounds: u64,
    pub ranged_gets: u64,
    pub ranged_bytes: u64,
    pub whole_files: u64,
    pub whole_bytes: u64,
}

impl PostingFetchStats {
    pub fn add(&mut self, other: &Self) {
        self.rounds = self.rounds.max(other.rounds);
        self.ranged_gets += other.ranged_gets;
        self.ranged_bytes += other.ranged_bytes;
        self.whole_files += other.whole_files;
        self.whole_bytes += other.whole_bytes;
    }
}

impl PostingReadCounters {
    pub fn record(&self, stats: &PostingFetchStats) {
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::doc_spans::SpanHydrator;
    use kosha_core::{
        Bm25Params, DocumentId, Field, Manifest, ManifestEntry, NamespaceId, SearchQuery, SegmentId,
    };
    use kosha_query::{PageSegment, Searcher};
    use kosha_segment::SegmentWriter;

    /// Counting fake object store over the "remote" copy: every `fetch`
    /// call is one round trip (its GETs go out concurrently), every range
    /// one GET. Ranges past EOF come back short, like S3.
    struct Store {
        root: PathBuf,
        rounds: AtomicU64,
        gets: AtomicU64,
        bytes: AtomicU64,
        fail: std::sync::atomic::AtomicBool,
    }

    impl Store {
        fn new(root: PathBuf) -> Self {
            Self {
                root,
                rounds: AtomicU64::new(0),
                gets: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
                fail: Default::default(),
            }
        }
        fn snapshot(&self) -> (u64, u64, u64) {
            (
                self.rounds.load(Ordering::SeqCst),
                self.gets.load(Ordering::SeqCst),
                self.bytes.load(Ordering::SeqCst),
            )
        }
    }

    impl RangeFetcher for Store {
        fn fetch(&self, ranges: &[(String, u64, u64)]) -> Vec<Result<Vec<u8>, KoshaError>> {
            if !ranges.is_empty() {
                self.rounds.fetch_add(1, Ordering::SeqCst);
            }
            ranges
                .iter()
                .map(|(key, off, len)| {
                    self.gets.fetch_add(1, Ordering::SeqCst);
                    if self.fail.load(Ordering::SeqCst) {
                        return Err(KoshaError::NotFound("injected".into()));
                    }
                    let all = std::fs::read(self.root.join(key))?;
                    let start = (*off as usize).min(all.len());
                    let end = off.saturating_add(*len).min(all.len() as u64) as usize;
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
        segs: Vec<PathBuf>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    const DIM: usize = 32;

    fn vector_for(seed: usize) -> Vec<f32> {
        let mut x = seed as u64 * 2_654_435_761 + 1;
        (0..DIM)
            .map(|_| {
                x = x
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                ((x >> 33) as f32 / (1u64 << 31) as f32) - 0.5
            })
            .collect()
    }

    /// `segs` raw-shard segments of `docs` docs over a 2,000-word
    /// vocabulary, each doc ~40 words, so every shard is tens of KB and a
    /// query term's postings a small slice of it. Docs `i % 97 == 0`
    /// contain the phrase "quick brown fox". With `vectors`, each doc has a
    /// 32-dim vector. Remote holds everything; the local data dir starts
    /// empty (a cold node).
    fn fixture(name: &str, segs: usize, docs: usize, vectors: bool) -> Fixture {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "kosha-posting-spans-{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data");
        let remote = root.join("remote");
        let ns = NamespaceId("ns".into());
        let mut entries = Vec::new();
        let mut seg_dirs = Vec::new();
        for s in 0..segs {
            let id = SegmentId(format!("s{s}"));
            let remote_dir = remote.join(&ns.0).join(&id.0);
            let mut w =
                SegmentWriter::new(id.clone(), remote_dir.clone()).with_raw_posting_blobs(true);
            for i in 0..docs {
                let n = s * docs + i;
                let mut words: Vec<String> = (0..40)
                    .map(|j| format!("w{}", (n * 31 + j * 977) % 2000))
                    .collect();
                if i % 97 == 0 {
                    words.splice(5..5, ["quick", "brown", "fox"].map(String::from));
                } else if i % 13 == 0 {
                    // The same words, not as a phrase.
                    words.splice(5..5, ["fox", "brown", "quick"].map(String::from));
                }
                let mut fields = vec![Field::text("t", words.join(" "))];
                if vectors {
                    fields.push(Field::vector("emb", vector_for(n)));
                }
                w.add_document(DocumentId(format!("s{s}-d{i}")), fields);
            }
            let footer = w.finalize(Bm25Params::default()).unwrap();
            assert!(footer.posting_blobs_raw);
            seg_dirs.push(data.join(&ns.0).join(&id.0));
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
            segs: seg_dirs,
        }
    }

    impl Fixture {
        fn rel(&self, seg: &Path) -> String {
            seg.strip_prefix(&self.data)
                .unwrap()
                .to_string_lossy()
                .into_owned()
        }

        fn remote_bytes(&self) -> u64 {
            fn walk(p: &Path) -> u64 {
                std::fs::read_dir(p)
                    .unwrap()
                    .flatten()
                    .map(|e| {
                        if e.path().is_dir() {
                            walk(&e.path())
                        } else {
                            e.metadata().unwrap().len()
                        }
                    })
                    .sum()
            }
            walk(&self.remote)
        }

        /// Round 1, the scaffolding: every segment's small files in one
        /// concurrent batch, as `ensure_scoring_files_local` does.
        fn scaffold(&self, store: &Store, vectors: bool) {
            let mut files = vec!["footer.json", "inverted.idx", "doc_store.offsets"];
            if vectors {
                files.push("vector.offsets");
            }
            let mut reqs = Vec::new();
            for seg in &self.segs {
                for f in &files {
                    reqs.push((format!("{}/{f}", self.rel(seg)), 0, u64::MAX / 2));
                }
            }
            for ((key, _, _), r) in reqs.iter().zip(store.fetch(&reqs)) {
                let path = self.data.join(key);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, r.unwrap()).unwrap();
            }
        }
    }

    fn query(v: serde_json::Value) -> SearchQuery {
        serde_json::from_value(v).unwrap()
    }

    /// The ids a fully local node (the remote copy itself) returns.
    fn reference(f: &Fixture, q: &SearchQuery) -> Vec<String> {
        Searcher::new(f.remote.clone())
            .search(&f.ns, &f.manifest, q, None)
            .unwrap()
            .results
            .into_iter()
            .map(|r| r.doc_id.0)
            .collect()
    }

    fn writes() -> SpanWriteQueue {
        SpanWriteQueue::new(crate::span_writes::SPAN_WRITE_QUEUE_BYTES)
    }

    /// [`PostingSpanHydrator::hydrate`] with its span writes done before it
    /// returns, as the background writer would do them.
    fn hydrate_now(
        postings: &PostingSpanHydrator,
        segs: &[PathBuf],
        terms: &[String],
        store: &dyn RangeFetcher,
        cache: &Cache,
    ) -> PostingSpanOutcome {
        let writes = writes();
        let o = postings.hydrate(segs, terms, store, &writes);
        writes.drain(cache);
        o
    }

    /// Postings round then the search, whose page docs and kNN postings go
    /// through ranged reads on `store`.
    fn search(
        f: &Fixture,
        searcher: &Searcher,
        postings: &PostingSpanHydrator,
        store: &Store,
        cache: &Cache,
        q: &SearchQuery,
    ) -> Result<(Vec<String>, PostingSpanOutcome), KoshaError> {
        // The background writer, run inline: every span is on disk before
        // the read that needs it.
        search_writing(f, searcher, postings, store, &writes(), Some(cache), q)
    }

    /// [`search`] queueing its span writes on `writes`, which are written
    /// after each fetch when `drain` names the cache, and otherwise left
    /// for a writer that is not running, as a stalled background writer
    /// would leave them. The request holds what the queue drops, as the
    /// server does.
    fn search_writing(
        f: &Fixture,
        searcher: &Searcher,
        postings: &PostingSpanHydrator,
        store: &Store,
        writes: &SpanWriteQueue,
        drain: Option<&Cache>,
        q: &SearchQuery,
    ) -> Result<(Vec<String>, PostingSpanOutcome), KoshaError> {
        let drain = || {
            if let Some(cache) = drain {
                writes.drain(cache);
            }
        };
        let held: Mutex<SpanHolds> = Mutex::new(Vec::new());
        // The terms the server's hydration asks for: every reading of the
        // query (#220) plus per-field keys and norms (#249). The fixture's
        // segments use the default analysis.
        let terms = kosha_query::QueryAnalysis::new(q).union_terms([None]);
        let mut outcome = if terms.is_empty() {
            PostingSpanOutcome::default()
        } else {
            postings.hydrate(&f.segs, &terms, store, writes)
        };
        drain();
        held.lock().unwrap().append(&mut outcome.held);
        let docs = SpanHydrator::new(f.data.clone(), SpanConfig::default());
        let ensure = |pages: &[PageSegment<'_>]| {
            let o = docs.hydrate(pages, store, writes);
            drain();
            held.lock().unwrap().extend(o.held);
        };
        let fetch_vectors = |seg: &Path, file: &str, ranges: &[(u64, u64)]| {
            let o = fetch_file_spans(&f.rel(seg), seg, file, ranges, 64 * 1024, store, writes);
            drain();
            o.held
        };
        let (result, _) = searcher.search_with_hydrators(
            &f.ns,
            &f.manifest,
            q,
            None,
            Some(&ensure),
            Some(&fetch_vectors),
        )?;
        Ok((
            result.results.into_iter().map(|r| r.doc_id.0).collect(),
            outcome,
        ))
    }

    /// Acceptance (#245): a cold BM25 query is three sequential S3 round
    /// trips (scaffolding, postings, page docs) and fetches a small
    /// fraction of the segments; the warm repeat makes zero GETs.
    #[test]
    fn cold_bm25_query_is_three_rounds_and_warm_repeat_is_free() {
        let f = fixture("bm25", 3, 3000, false);
        let store = Store::new(f.remote.clone());
        let cache = Cache::new(f.data.clone());
        let postings = PostingSpanHydrator::new(f.data.clone(), default_config());
        let q = query(serde_json::json!({"query_text": "w0 w977 w1954", "max_results": 10}));
        let want = reference(&f, &q);
        assert_eq!(want.len(), 10);

        f.scaffold(&store, false);
        let searcher = Searcher::new(f.data.clone());
        let (got, o) = search(&f, &searcher, &postings, &store, &cache, &q).unwrap();
        assert_eq!(got, want, "ranged reads return the fully local answer");
        let (rounds, gets, bytes) = store.snapshot();
        let total = f.remote_bytes();
        // A fetched term is confirmed local only once its span is on disk,
        // which the background writer does after the fetch returns: the
        // next local check confirms every query term in every segment,
        // without a GET.
        assert!(o.confirmed.is_empty(), "{:?}", o.confirmed);
        let gets_before = store.snapshot().1;
        let terms = kosha_query::QueryAnalysis::new(&q).union_terms([None]);
        let recheck = hydrate_now(&postings, &f.segs, &terms, &store, &cache);
        let confirmed: HashSet<_> = recheck.confirmed.iter().collect();
        for seg in &f.segs {
            for term in ["w0", "w977", "w1954"] {
                assert!(
                    confirmed.contains(&(f.rel(seg), presence_key(term))),
                    "{term}"
                );
            }
        }
        assert_eq!(store.snapshot().1, gets_before);
        println!(
            "cold BM25: {rounds} round trips, {gets} GETs, {bytes} bytes of {total} \
             ({:.2}%); postings {} GETs / {} bytes",
            bytes as f64 * 100.0 / total as f64,
            o.gets,
            o.bytes
        );
        // What whole-shard hydration would have pulled for these terms.
        let whole_shards: u64 = f
            .segs
            .iter()
            .flat_map(|seg| {
                ["w0", "w977", "w1954"].map(|t| {
                    f.remote
                        .join(f.rel(seg))
                        .join(kosha_segment::posting_blob_file_for_term(t))
                })
            })
            .map(|p| std::fs::metadata(p).map_or(0, |m| m.len()))
            .sum();
        println!("  whole shards for the same terms: {whole_shards} bytes");
        assert!(o.bytes * 10 < whole_shards, "{} of {whole_shards}", o.bytes);
        assert_eq!(rounds, 3, "scaffolding, postings, page docs");
        assert_eq!(o.rounds, 1);
        assert!(o.whole_fallback.is_empty() && o.missing.is_empty() && !o.shed);
        assert!(bytes * 10 < total, "fetched {bytes} of {total}");
        // Three terms × three segments: at most one GET per term per
        // segment (terms hash to different shards).
        assert!(o.gets <= 9, "{} posting GETs", o.gets);
        for seg in &f.segs {
            assert!(
                std::fs::read_dir(seg).unwrap().flatten().all(|e| !e
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".bin")
                    || e.file_name() == "doc_store.offsets"),
                "no whole shard or doc store was fetched"
            );
        }

        // Warm: same searcher, and a fresh one (spans persist on disk).
        let (got2, o2) = search(&f, &searcher, &postings, &store, &cache, &q).unwrap();
        assert_eq!(got2, want);
        assert_eq!(o2.gets, 0);
        let fresh = Searcher::new(f.data.clone());
        let (got3, _) = search(&f, &fresh, &postings, &store, &cache, &q).unwrap();
        assert_eq!(got3, want);
        assert_eq!(
            store.snapshot(),
            (rounds, gets, bytes),
            "warm repeat: zero GETs"
        );
    }

    /// Phrase queries need positions, which live in the same term span.
    #[test]
    fn phrase_query_works_through_ranged_reads() {
        let f = fixture("phrase", 2, 2000, false);
        let store = Store::new(f.remote.clone());
        let cache = Cache::new(f.data.clone());
        let postings = PostingSpanHydrator::new(f.data.clone(), default_config());
        let q = query(serde_json::json!({
            "query_text": "quick brown fox",
            "match_phrase": {"field": "t", "phrase": "quick brown fox"},
            "max_results": 50
        }));
        let want = reference(&f, &q);
        // 2,000 docs / 97 per segment, two segments: only the true phrase
        // docs, not the shuffled ones.
        assert_eq!(want.len(), 2 * 2000_usize.div_ceil(97));
        f.scaffold(&store, false);
        let searcher = Searcher::new(f.data.clone());
        let (got, _) = search(&f, &searcher, &postings, &store, &cache, &q).unwrap();
        assert_eq!(got, want);
        assert_eq!(store.snapshot().0, 3);
    }

    /// An evicted span must fail the search, never score as zero hits.
    #[test]
    fn missing_span_fails_the_search_instead_of_dropping_the_term() {
        let f = fixture("evict", 1, 2000, false);
        let store = Store::new(f.remote.clone());
        let cache = Cache::new(f.data.clone());
        let postings = PostingSpanHydrator::new(f.data.clone(), default_config());
        let q = query(serde_json::json!({"query_text": "w17", "max_results": 10}));
        f.scaffold(&store, false);
        let searcher = Searcher::new(f.data.clone());
        search(&f, &searcher, &postings, &store, &cache, &q).unwrap();
        // Evict every posting span; a fresh searcher has no decoded cache.
        for e in std::fs::read_dir(&f.segs[0]).unwrap().flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name.starts_with("postings-") && name.ends_with(".spans") {
                std::fs::remove_dir_all(e.path()).unwrap();
            }
        }
        let fresh = Searcher::new(f.data.clone());
        let err = fresh.search(&f.ns, &f.manifest, &q, None);
        assert!(err.is_err(), "unreadable term must error: {err:?}");
    }

    #[test]
    fn failed_ranges_fall_back_to_the_whole_shard() {
        let f = fixture("fail", 1, 1000, false);
        let store = Store::new(f.remote.clone());
        let cache = Cache::new(f.data.clone());
        let postings = PostingSpanHydrator::new(f.data.clone(), default_config());
        f.scaffold(&store, false);
        store.fail.store(true, Ordering::SeqCst);
        let terms = vec!["w17".to_string(), "absent-term".to_string()];
        let o = hydrate_now(&postings, &f.segs, &terms, &store, &cache);
        assert_eq!(o.whole_fallback.len(), 1);
        assert_eq!(
            o.whole_fallback[0].1,
            kosha_segment::posting_blob_file_for_term("w17")
        );
        // The absent term is confirmed; w17 is not.
        assert_eq!(
            o.confirmed,
            vec![(f.rel(&f.segs[0]), presence_key("absent-term"))]
        );
    }

    #[test]
    fn hot_shard_is_promoted_once_and_its_spans_dropped() {
        let f = fixture("promote", 1, 2000, false);
        let store = Store::new(f.remote.clone());
        let cache = Cache::new(f.data.clone());
        f.scaffold(&store, false);
        let before = cache.total_size();
        let cfg = SpanConfig {
            coalesce_gap: 0,
            promote_after_gets: 2,
            promote_after_bytes: 0,
        };
        let postings = PostingSpanHydrator::new(f.data.clone(), cfg);
        // Two different terms in the same shard.
        let file = kosha_segment::posting_blob_file_for_term("w1");
        let mut same: Vec<String> = (2..2000)
            .map(|i| format!("w{i}"))
            .filter(|t| kosha_segment::posting_blob_file_for_term(t) == file)
            .take(3)
            .collect();
        same.insert(0, "w1".into());
        let o1 = hydrate_now(&postings, &f.segs, &same[..1], &store, &cache);
        assert!(o1.promote.is_empty());
        let o2 = hydrate_now(&postings, &f.segs, &same[1..2], &store, &cache);
        let shard = f.segs[0].join(&file);
        assert_eq!(o2.promote, vec![shard.clone()]);
        let o3 = hydrate_now(&postings, &f.segs, &same[2..3], &store, &cache);
        assert!(o3.promote.is_empty(), "queued once while pending");
        assert!(cache.total_size() > before);

        // The promoter lands the whole shard, then drops the spans.
        std::fs::copy(f.remote.join(f.rel(&f.segs[0])).join(&file), &shard).unwrap();
        cache.note_external_write(&format!("{}/{file}", f.rel(&f.segs[0])));
        let with_whole = cache.total_size();
        postings.finish_promotion(&cache, &shard, true);
        assert!(SpanIndex::load(&f.segs[0], &file).is_empty());
        assert!(cache.total_size() < with_whole);
        // Every term of the shard is now local: no GET.
        let o4 = hydrate_now(&postings, &f.segs, &same, &store, &cache);
        assert_eq!(o4.gets, 0);
        assert_eq!(o4.confirmed.len(), same.len());
    }

    /// kNN: only `vector.offsets` is fetched up front; the probed postings
    /// come by range, and a warm repeat fetches nothing.
    #[test]
    fn knn_probes_fetch_only_their_postings() {
        let f = fixture("knn", 2, 3000, true);
        let store = Store::new(f.remote.clone());
        let cache = Cache::new(f.data.clone());
        let postings = PostingSpanHydrator::new(f.data.clone(), default_config());
        let q = query(serde_json::json!({
            "query_text": "",
            "knn": {"field": "emb", "vector": vector_for(1234), "k": 5, "num_candidates": 4},
            "max_results": 5
        }));
        let want = reference(&f, &q);
        assert_eq!(want.len(), 5);
        f.scaffold(&store, true);
        let after_scaffold = store.snapshot();
        let searcher = Searcher::new(f.data.clone());
        let (got, _) = search(&f, &searcher, &postings, &store, &cache, &q).unwrap();
        assert_eq!(got, want);
        let (_, gets, bytes) = store.snapshot();
        let vector_idx: u64 = f
            .segs
            .iter()
            .map(|s| {
                std::fs::metadata(f.remote.join(f.rel(s)).join("vector.idx"))
                    .unwrap()
                    .len()
            })
            .sum();
        let vector_bytes: u64 = f
            .segs
            .iter()
            .map(|s| SpanIndex::load(s, kosha_segment::VECTOR_IDX_FILE).total_bytes())
            .sum();
        println!(
            "cold kNN: {} GETs after scaffolding, {vector_bytes} vector bytes of {vector_idx}",
            gets - after_scaffold.1
        );
        assert!(vector_bytes > 0);
        assert!(
            vector_bytes * 4 < vector_idx,
            "{vector_bytes} of {vector_idx}"
        );
        for s in &f.segs {
            assert!(!s.join("vector.idx").exists());
        }
        let fresh = Searcher::new(f.data.clone());
        let (got2, _) = search(&f, &fresh, &postings, &store, &cache, &q).unwrap();
        assert_eq!(got2, want);
        assert_eq!(store.snapshot().1, gets, "warm kNN repeat: zero GETs");
        assert_eq!(store.snapshot().2, bytes);
    }

    /// No span is written on a scoring thread. With the background writer
    /// stalled outright, cold kNN and BM25 searches running concurrently on
    /// a single rayon thread all finish with the right answer and nothing
    /// lands on disk: the probes read the fetched postings from memory.
    /// Before, each fetched span was written by the scoring task itself
    /// before it read it back, so a writer stalled in the kernel's dirty
    /// throttling stalled the rayon pool and every search queued behind it.
    /// Once the writer runs, the spans are on disk and a repeat is free.
    #[test]
    fn stalled_span_writer_never_blocks_scoring() {
        let f = fixture("stalled-writer", 2, 3000, true);
        let store = Store::new(f.remote.clone());
        let cache = Cache::new(f.data.clone());
        let postings = PostingSpanHydrator::new(f.data.clone(), default_config());
        let knn = query(serde_json::json!({
            "query_text": "",
            "knn": {"field": "emb", "vector": vector_for(1234), "k": 5, "num_candidates": 4},
            "max_results": 5
        }));
        let bm25 = query(serde_json::json!({"query_text": "w0 w977 w1954", "max_results": 10}));
        let want = [reference(&f, &knn), reference(&f, &bm25)];
        f.scaffold(&store, true);
        let writes = writes();
        let searcher = Searcher::new(f.data.clone());
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        // Four concurrent searches whose parallel scoring all runs on one
        // rayon thread: one blocked task would block them all.
        let got: Vec<Vec<String>> = std::thread::scope(|scope| {
            let runs: Vec<_> = [&knn, &bm25, &knn, &bm25]
                .into_iter()
                .map(|q| {
                    let (f, searcher, postings, store, writes, pool) =
                        (&f, &searcher, &postings, &store, &writes, &pool);
                    scope.spawn(move || {
                        pool.install(|| {
                            search_writing(f, searcher, postings, store, writes, None, q)
                                .unwrap()
                                .0
                        })
                    })
                })
                .collect();
            runs.into_iter().map(|r| r.join().unwrap()).collect()
        });
        assert_eq!(
            got,
            [&want[0], &want[1], &want[0], &want[1]].map(|w| w.clone())
        );
        assert!(writes.queued_bytes() > 0, "the spans wait for the writer");
        for seg in &f.segs {
            for file in [
                kosha_segment::VECTOR_IDX_FILE,
                kosha_segment::DOC_STORE_FILE,
            ] {
                assert!(
                    !span_cache::spans_dir(seg, file).exists(),
                    "nothing written while the writer is stalled: {}",
                    seg.display()
                );
            }
        }

        writes.drain(&cache);
        assert_eq!(writes.queued_bytes(), 0);
        let gets = store.snapshot().1;
        for (q, want) in [&knn, &bm25].into_iter().zip(&want) {
            let (got, _) = search(
                &f,
                &Searcher::new(f.data.clone()),
                &postings,
                &store,
                &cache,
                q,
            )
            .unwrap();
            assert_eq!(&got, want);
        }
        assert_eq!(
            store.snapshot().1,
            gets,
            "warm repeats read the written spans"
        );
    }

    /// A span write queue that can take nothing drops every write; the
    /// searches that fetched the spans still answer correctly from the
    /// spans they hold, and nothing is cached for the next one.
    #[test]
    fn dropped_span_writes_still_answer_correctly() {
        let f = fixture("dropped-writes", 2, 3000, true);
        let store = Store::new(f.remote.clone());
        let postings = PostingSpanHydrator::new(f.data.clone(), default_config());
        let knn = query(serde_json::json!({
            "query_text": "",
            "knn": {"field": "emb", "vector": vector_for(1234), "k": 5, "num_candidates": 4},
            "max_results": 5
        }));
        let bm25 = query(serde_json::json!({"query_text": "w0 w977 w1954", "max_results": 10}));
        f.scaffold(&store, true);
        let full = SpanWriteQueue::new(0);
        for q in [&knn, &bm25] {
            let want = reference(&f, q);
            let searcher = Searcher::new(f.data.clone());
            let (got, o) =
                search_writing(&f, &searcher, &postings, &store, &full, None, q).unwrap();
            assert_eq!(got, want);
            if q.query_text.is_empty() {
                continue;
            }
            // Terms whose spans were only held are not confirmed local.
            let absent = o.confirmed.len();
            let gets = store.snapshot().1;
            let (again, o2) =
                search_writing(&f, &searcher, &postings, &store, &full, None, q).unwrap();
            assert_eq!(again, want);
            assert!(
                store.snapshot().1 > gets,
                "nothing was cached: fetched again"
            );
            assert_eq!(o2.confirmed.len(), absent);
        }
        assert_eq!(full.queued_bytes(), 0);
        let stats = full.to_json();
        assert!(stats["dropped"].as_u64().unwrap() > 0, "{stats}");
        assert_eq!(stats["written"], 0);
        assert!(!span_cache::spans_dir(&f.segs[0], kosha_segment::VECTOR_IDX_FILE).exists());
    }

    /// Scale report, not a CI test: `KOSHA_BENCH_SEGS` segments of
    /// `KOSHA_BENCH_DOCS` docs, `KOSHA_BENCH_QUERIES` distinct cold
    /// three-term queries (a fresh local data dir each), each S3 round trip
    /// charged `KOSHA_BENCH_S3_MS` (default 35 ms). Reports cold p50/p99
    /// and bytes for ranged postings vs whole-shard postings.
    ///
    /// `cargo test --release -p kosha-server --all-features
    ///  cold_bm25_scale_report -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn cold_bm25_scale_report() {
        let env = |k: &str, d: usize| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        };
        let (segs, docs, queries) = (
            env("KOSHA_BENCH_SEGS", 8),
            env("KOSHA_BENCH_DOCS", 25_000),
            env("KOSHA_BENCH_QUERIES", 20),
        );
        let rtt = std::time::Duration::from_millis(env("KOSHA_BENCH_S3_MS", 35) as u64);
        let f = fixture("scale", segs, docs, false);
        let total = f.remote_bytes();
        struct Slow<'a>(&'a Store, std::time::Duration);
        impl RangeFetcher for Slow<'_> {
            fn fetch(&self, r: &[(String, u64, u64)]) -> Vec<Result<Vec<u8>, KoshaError>> {
                if !r.is_empty() {
                    std::thread::sleep(self.1);
                }
                self.0.fetch(r)
            }
        }
        type Row = (String, f64, u64, u64, u64, u64);
        let mut rows: Vec<Row> = Vec::new();
        for mode in ["ranged", "whole"] {
            for qi in 0..queries {
                let _ = std::fs::remove_dir_all(&f.data);
                let store = Store::new(f.remote.clone());
                let slow = Slow(&store, rtt);
                let cache = Cache::new(f.data.clone());
                let base = 50 + qi * 97;
                let text = format!("w{base} w{} w{}", (base + 977) % 2000, (base + 1954) % 2000);
                let q = query(serde_json::json!({"query_text": text, "max_results": 10}));
                let t = std::time::Instant::now();
                std::thread::sleep(rtt);
                f.scaffold(&store, false);
                let terms = kosha_segment::tokenize(&q.query_text);
                let posting_bytes = if mode == "ranged" {
                    let postings = PostingSpanHydrator::new(f.data.clone(), default_config());
                    hydrate_now(&postings, &f.segs, &terms, &slow, &cache).bytes
                } else {
                    // Whole shards, one concurrent batch.
                    let mut reqs = Vec::new();
                    for seg in &f.segs {
                        let mut files: Vec<String> = terms
                            .iter()
                            .map(|t| kosha_segment::posting_blob_file_for_term(t))
                            .collect();
                        files.sort();
                        files.dedup();
                        for file in files {
                            if f.remote.join(f.rel(seg)).join(&file).exists() {
                                reqs.push((format!("{}/{file}", f.rel(seg)), 0, u64::MAX / 2));
                            }
                        }
                    }
                    let mut n = 0;
                    for ((key, _, _), r) in reqs.iter().zip(slow.fetch(&reqs)) {
                        let bytes = r.unwrap();
                        n += bytes.len() as u64;
                        std::fs::write(f.data.join(key), bytes).unwrap();
                    }
                    n
                };
                let docs_h = SpanHydrator::new(f.data.clone(), SpanConfig::default());
                let writes = writes();
                let ensure = |pages: &[PageSegment<'_>]| {
                    let _ = docs_h.hydrate(pages, &slow, &writes);
                    writes.drain(&cache);
                };
                Searcher::new(f.data.clone())
                    .search_with_doc_store_hydrator(&f.ns, &f.manifest, &q, None, Some(&ensure))
                    .unwrap();
                let ms = t.elapsed().as_secs_f64() * 1e3;
                let (rounds, gets, bytes) = store.snapshot();
                rows.push((mode.into(), ms, rounds, gets, bytes, posting_bytes));
            }
        }
        println!(
            "corpus: {segs} segments x {docs} docs = {} docs, {:.1} MB in S3; \
             {queries} cold queries per mode, {} ms per S3 round trip",
            segs * docs,
            total as f64 / 1e6,
            rtt.as_millis()
        );
        for mode in ["ranged", "whole"] {
            let r: Vec<_> = rows.iter().filter(|r| r.0 == mode).collect();
            let mut ms: Vec<f64> = r.iter().map(|r| r.1).collect();
            ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let pct = |p: f64| ms[((p / 100.0) * (ms.len() - 1) as f64).round() as usize];
            let avg = |f: &dyn Fn(&&Row) -> u64| r.iter().map(f).sum::<u64>() / r.len() as u64;
            println!(
                "{mode:>6}: cold p50 {:.0} ms p99 {:.0} ms | rounds {} | GETs {} | \
                 bytes {:.2} MB ({:.2}% of corpus) | postings {:.1} KB",
                pct(50.0),
                pct(99.0),
                r.iter().map(|r| r.2).max().unwrap(),
                avg(&|r| r.3),
                avg(&|r| r.4) as f64 / 1e6,
                avg(&|r| r.4) as f64 * 100.0 / total as f64,
                avg(&|r| r.5) as f64 / 1e3,
            );
        }
    }

    #[test]
    fn read_mode_and_config_env_names() {
        assert_eq!(default_config().promote_after_gets, 32);
        assert_eq!(presence_key("x"), "term:x");
    }
}
