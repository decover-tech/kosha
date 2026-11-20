//! Kosha node binary.
//!
//! One binary, three roles (DESIGN.md §5): `ingest`, `query`, `compaction`.
//! Phase 1 HTTP/JSON API:
//!   - `GET  /healthz`           → 200 OK (liveness probe)
//!   - `POST /index              ` → upsert documents by id into a namespace
//!   - `POST /exists             ` → batch doc_id existence check
//!   - `POST /replace            ` → partial field merge by id (OpenSearch `_update`)
//!   - `GET  /search             ` → BM25 search across a namespace

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
#[cfg(feature = "s3")]
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

/// Extract the tenant prefix from a namespace for isolation.
fn tenant_namespace(tenant: &str, namespace: &str) -> String {
    format!("{tenant}/{namespace}")
}

mod auth;
mod compaction_sched;
mod doc_spans;
#[cfg(all(test, feature = "s3"))]
mod doc_store_fallback_tests;
#[cfg(feature = "s3")]
mod hydration_budget;
mod manifest_follow;
#[cfg(feature = "migrate")]
mod migrate;
mod posting_spans;
mod rerank;
#[cfg(feature = "s3")]
mod s3_storage;
#[cfg(feature = "s3")]
mod s3_wal;
#[cfg(feature = "s3")]
mod segment_commit;
mod segment_gc;
mod warm;
mod warm_run;

#[cfg(feature = "s3")]
use hydration_budget::{HydrationBudget, RequestHydration};
use kosha_cache::Cache;
#[cfg(feature = "s3")]
use kosha_core::{segment_may_contain_terms, TermBloomMode};
use kosha_core::{ControlStore, IndexRequest, IndexResponse, KoshaError, NamespaceId, SearchQuery};
use kosha_core::{LocalStorage, StorageBackend};
use kosha_query::DocStoreHydrator;
#[cfg(feature = "s3")]
use kosha_query::PageSegment;
use kosha_query::Searcher;
use kosha_segment::SegmentReader;
use kosha_write::Indexer;

// ─── Application state ──────────────────────────────────────────────────────

struct AppState {
    controller: Mutex<Box<dyn ControlStore>>,
    admin_auth_key: Option<String>,
    internal_service_key: Option<String>,
    publication_locks: Mutex<HashMap<NamespaceId, Arc<Mutex<()>>>>,
    /// Hydrated segment files that failed their `_COMMIT` size/CRC check
    /// (#231), counting refetch failures again. Reported in `/stats`.
    #[cfg(feature = "s3")]
    segment_checksum_failures: std::sync::atomic::AtomicU64,
    /// Lazy segments whose `doc_store.offsets` was missing from S3 or
    /// rejected by the reader, so a search fetched `doc_store.bin` instead
    /// (#179, #184). Each one should be repaired with
    /// `POST /v1/admin/backfill-offset-tables`. Reported in `/stats`.
    #[cfg(feature = "s3")]
    doc_store_fallbacks: std::sync::atomic::AtomicU64,
    indexer: Indexer,
    searcher: Searcher,
    /// Local segment / SSD cache root (`KOSHA_DATA_DIR`). Never authoritative
    /// when S3 is enabled — losing it is a cache-miss event only.
    data_dir: PathBuf,
    /// Read-through cache handle over `data_dir` (DESIGN.md §9).
    cache: Cache,
    control_plane_kind: &'static str,
    #[cfg(feature = "s3")]
    s3_storage: Option<s3_storage::S3Storage>,
    /// Max in-flight S3 GETs when hydrating segments for a search
    /// (`KOSHA_HYDRATE_CONCURRENCY`, default 16). See `ensure_segments_local`.
    #[cfg(feature = "s3")]
    hydrate_concurrency: usize,
    /// Byte ceiling on hydration volume in flight at once
    /// (`KOSHA_HYDRATE_BYTE_BUDGET`, default 1GiB). Unlike
    /// `hydrate_concurrency` (file count), this bounds volume — applied two
    /// ways, because the two hydration paths learn file sizes at different
    /// times:
    ///   * pre-GET, by `chunk_by_byte_budget` in `hydrate_from_s3_budgeted`,
    ///     wherever a `list_with_sizes` already supplied sizes (the
    ///     full-segment path);
    ///   * post-send, by `hydration_budget`, from each response's
    ///     `Content-Length` — the only option on the scoring path, which
    ///     skips the per-segment LIST on purpose and so passes size `0` for
    ///     every file.
    #[cfg(feature = "s3")]
    hydrate_byte_budget: u64,
    /// Process-wide in-flight byte gate shared by every hydration fetch —
    /// see [`hydration_budget`]. Capacity is `hydrate_byte_budget`.
    #[cfg(feature = "s3")]
    hydration_budget: Arc<HydrationBudget>,
    /// Ceiling on the bytes one search request may hydrate before it is shed
    /// with a 429 (`KOSHA_HYDRATE_REQUEST_MAX_BYTES`, default 2GiB). A cold
    /// search that would pull multiple GB is not a search that will finish
    /// inside anyone's client timeout; failing it fast beats grinding
    /// through the fetch while holding a hydration permit.
    #[cfg(feature = "s3")]
    hydrate_request_max_bytes: u64,
    /// GET fan-out for known-small-file hydration — the scoring set,
    /// footer prefetch, and page doc stores (`ensure_scoring_files_local`
    /// / `ensure_files_local`). Separate from `hydrate_concurrency`
    /// (default 16), which was sized for full-segment fetches with
    /// multi-hundred-MB doc stores: small-object S3 throughput is
    /// latency-bound, so a cold scoring fetch of hundreds of ~1-50MB files
    /// wants a much wider fan-out than a batch of doc stores does.
    /// `KOSHA_SCORING_HYDRATE_CONCURRENCY`, default 64.
    #[cfg(feature = "s3")]
    scoring_hydrate_concurrency: usize,
    /// Segments this process has confirmed are durably uploaded to S3 (every
    /// file present, not just the local directory) — see
    /// `sync_unsynced_segments_to_s3`. Keyed by `(namespace, segment_id)`
    /// rather than local path since it's checked before the segment
    /// necessarily has a local directory reference at hand in every caller.
    /// Populated by the boot-time reconciliation sweep and by every
    /// successful `sync_to_s3` thereafter; never removed (segments are
    /// immutable, so once durable, always durable).
    #[cfg(feature = "s3")]
    synced_segments: Mutex<std::collections::HashSet<(NamespaceId, kosha_core::SegmentId)>>,
    /// Segments currently being hydrated from S3, keyed by local segment
    /// path. Guards against every concurrent request against a cold
    /// namespace independently re-fetching the same segments — see
    /// `ensure_segments_local`, which is the only place this is touched.
    /// Only callers that *insert* a fresh entry ("owners") perform the S3
    /// fetch; callers that find an existing entry ("waiters") block on its
    /// `SegmentFetch` instead of redundantly downloading the same files.
    #[cfg(feature = "s3")]
    in_flight_segments: Mutex<HashMap<PathBuf, Arc<SegmentFetch>>>,
    /// Server-wide ceiling on concurrent hydration *operations* (as
    /// distinct from `hydrate_concurrency`, which bounds the S3 GET
    /// fan-out *within* a single hydration operation). Without this,
    /// thread-per-connection means an unbounded number of concurrent
    /// requests can each be hydrating a distinct cold segment batch at
    /// once, each fanning out up to `hydrate_concurrency` GETs — the
    /// combination is what produced the staging OOMs this exists to fix.
    /// `KOSHA_MAX_CONCURRENT_HYDRATIONS`, default 4.
    #[cfg(feature = "s3")]
    hydration_semaphore: Semaphore,
    /// How page documents are fetched from S3: ranged spans into the
    /// sparse span cache (default) or whole files
    /// (`KOSHA_DOC_STORE_READ_MODE`, #245). See [`doc_spans`].
    doc_read_mode: doc_spans::ReadMode,
    /// Ranged page-document fetcher + promotion bookkeeping (#245).
    #[cfg(feature = "s3")]
    span_hydrator: doc_spans::SpanHydrator,
    /// Segments hot enough on the ranged path to hydrate whole, drained by
    /// the `kosha-doc-promote` thread (`spawn_doc_store_promoter`).
    #[cfg(feature = "s3")]
    doc_store_promotions: doc_spans::PromotionQueue,
    /// Process totals of page-document bytes fetched, for `GET /stats`.
    doc_store_reads: doc_spans::DocStoreReadCounters,
    /// How query-term postings are fetched for raw-shard segments: ranged
    /// term spans (default) or whole shards (`KOSHA_POSTINGS_READ_MODE`,
    /// #245). See [`posting_spans`].
    posting_read_mode: doc_spans::ReadMode,
    /// Ranged posting fetcher + shard promotion bookkeeping (#245).
    #[cfg(feature = "s3")]
    posting_span_hydrator: posting_spans::PostingSpanHydrator,
    /// Shards (`{segment}/postings-XX.bin`) hot enough on the ranged path
    /// to hydrate whole, drained by the `kosha-postings-promote` thread.
    #[cfg(feature = "s3")]
    posting_promotions: doc_spans::PromotionQueue,
    /// Process totals of posting bytes fetched, for `GET /stats`.
    posting_reads: posting_spans::PostingReadCounters,
    /// Ceiling on concurrent `/search` executions
    /// (`KOSHA_MAX_CONCURRENT_SEARCHES`, default 8). Complements the
    /// searcher's own live-bytes admission gate (`kosha_query::MemoryLedger`):
    /// the ledger bounds pinned *segment* memory, while this bounds the
    /// per-request working memory the ledger can't see (candidate vectors,
    /// scoring buffers) and keeps rayon from timeslicing across an unbounded
    /// number of concurrent scoring passes. Thread-per-connection means
    /// excess searches queue here rather than all running at once.
    search_semaphore: Semaphore,
    /// Cross-replica hydration coordinator (DESIGN.md §9 gap: N query
    /// replicas cold-starting on the same namespace independently re-fetch
    /// the same segments from S3). `None` when Postgres isn't configured —
    /// coordination degrades gracefully to "every pod fetches its own
    /// copy," i.e. today's behavior, never to a stuck request. See
    /// `kosha_control::HydrationLeaseStore` and this file's `hydrate_files`.
    #[cfg(all(feature = "s3", feature = "postgres"))]
    hydration_leases: Option<kosha_control::HydrationLeaseStore>,
    /// This pod's own `host:port`, published to `hydration_leases` when it
    /// wins a claim so other replicas know where to fetch the bytes from.
    /// Built from the `POD_IP` downward-API env var (deployment-query.yaml)
    /// and the HTTP port. `None` (and hydration coordination inert) if
    /// unset, e.g. local/dev runs.
    #[cfg(all(feature = "s3", feature = "postgres"))]
    own_addr: Option<String>,
    /// HTTP client for fetching already-hydrated segment bytes from a peer
    /// pod's `GET /internal/segment/...` instead of S3 — see `hydrate_files`.
    /// Separate from `write_http_client` (query→ingest forwarding): distinct
    /// purpose, distinct timeout profile (short — a peer that isn't
    /// responding quickly should be abandoned in favor of falling back to
    /// S3, not held onto).
    #[cfg(all(feature = "s3", feature = "postgres"))]
    peer_http_client: Option<reqwest::blocking::Client>,
    /// Dedicated `KOSHA_INTERNAL_API_KEY` for pod-to-pod hydration. `None` disables peer fetches
    /// (falls back to S3, same as an unset `own_addr`).
    #[cfg(all(feature = "s3", feature = "postgres"))]
    internal_auth_key: Option<String>,
    /// Weak self-reference so request handlers can hand an owning `Arc` to
    /// background job threads (see `handle_backfill_offset_tables`). Set by
    /// `new_shared` (the only constructor `main` uses); a bare `new` leaves
    /// it dangling and background jobs refuse to start.
    self_ref: Weak<AppState>,
    /// Namespaces with an offsets-backfill job currently running — one job
    /// per namespace at a time.
    backfill_jobs: Mutex<std::collections::HashSet<String>>,
    /// Compaction jobs and per-namespace scheduler state (#242): every pass,
    /// scheduled or requested through the API, is registered here, so the
    /// global concurrency budget and one-pass-per-namespace hold across both.
    compaction: compaction_sched::CompactionRegistry,
    /// Segment GC settings (`KOSHA_GC_GRACE`, `KOSHA_LOCAL_GC_GRACE`, …)
    /// and counters (#239).
    gc_settings: segment_gc::GcSettings,
    gc_stats: segment_gc::GcStats,
    /// Serializes `POST /v1/admin/gc` runs.
    gc_lock: Mutex<()>,
    /// Namespace-level cached hydration verdict (Tier 1 O2). Keyed by
    /// `(namespace, manifest_version)`: every search request currently
    /// pays per-segment freshness/existence checks inside
    /// `hydrate_segments_for_search` — footer prefetch, bloom-prune,
    /// scoring-set file checks, posting-blob checks — even for a fully-
    /// warm namespace where the entire result is "all local." For a 1M-
    /// doc namespace with 17 segments that's ~17 `is_file` syscalls per
    /// file type per query — 15–130 ms of zero-fetch overhead per warm
    /// query, a hard floor OpenSearch doesn't pay.
    ///
    /// The verdict is keyed by manifest version (`manifest.version: u64`)
    /// so it's *automatically* invalidated the moment a new segment
    /// publishes — no manual invalidation on compaction or flush, no TTL
    /// guessing. After hydration confirms a file class is local for every
    /// segment (on a cold namespace) the corresponding
    /// [`HydrationVerdict`] flag is set for that `(namespace, version)`
    /// pair, and subsequent warm requests whose own needs are covered by
    /// what's been verified so far short-circuit `hydrate_segments_for_search`
    /// entirely — see `HydrationVerdict` for why this is per-file-class,
    /// not a flat bool (a query that doesn't need `filters.bin`/`vector.idx`
    /// never verifies it, so a later query that *does* need one must not
    /// trust a verdict that never checked it). Failure safety:
    /// if a disk-cache eviction removes a segment's scoring file after
    /// the verdict is cached, the search itself returns an IO error
    /// (`open_segment` fails to open `inverted.idx`), the caller's
    /// existing 503 + client retry path kicks in, and the verdict is
    /// invalidated on the error path here so the next request re-checks
    /// and re-hydrates the missing file. The collection grows
    /// unboundedly in principle (one entry per manifest bump per
    /// namespace) but each value is two bools, and manifest versions roll
    /// slowly enough that even a long-lived pod holds at most
    /// low-thousands entries; per-query cost is a single `HashMap` lookup,
    /// vs. 17 syscalls on the legacy path.
    completed_hydrations: Mutex<HashMap<(String, u64), HydrationVerdict>>,
    /// Whole-response cache for POST /search — see [`ResultCache`].
    result_cache: Mutex<ResultCache>,
    /// Rerank providers (`SearchQuery.rerank`) and their metrics; see
    /// [`rerank`].
    rerankers: rerank::RerankRegistry,
    /// Posting-blob presence cache (Tier 1 O3), the sibling optimization to
    /// `completed_hydrations` above: that verdict can never cover posting
    /// blobs (PR #89 — they're fetched per query term, not per segment), so
    /// `ensure_query_posting_blobs` pays a full `fs::metadata` stat sweep
    /// over every `(segment, term-shard)` pair on *every* request, even a
    /// fully-warm one on the cached-verdict fast path. At 167 segments ×
    /// ~5 term-shards per query that's 800+ stats — done twice, once to
    /// decide what to fetch and again to compute `missing` — measured as a
    /// flat ~86–92 ms tax on every warm query at that scale (it was ~10 ms
    /// at 1M docs / 17 segments; the cost scales linearly with segment
    /// count).
    ///
    /// Keyed by namespace, one entry holding the manifest version it was
    /// populated against plus the `{segment_relative_path -> {file_name}}`
    /// pairs physically confirmed present for that version so far. A
    /// lookup against a different (newer) manifest version than the stored
    /// one is treated as a miss — same free invalidation-on-publish as
    /// `completed_hydrations` — and *replaces* the stale entry outright
    /// rather than leaving it to accumulate: unlike that cache's 16-byte
    /// `(u64, u64)` tuples, this one holds real path/file strings, so
    /// stale-version entries are worth actively pruning instead of just
    /// tolerating unbounded growth.
    ///
    /// Purely additive within a version — an entry only ever gains
    /// confirmed-present pairs from real `fs::metadata` stats, never a
    /// pair it hasn't itself verified — so a cache hit is exactly as
    /// trustworthy as the stat it stands in for **as of the moment it was
    /// taken**. Unlike a scoring-set file (footer/`inverted.idx`/offsets),
    /// whose absence surfaces as a hard `SegmentReader::open` IO error that
    /// self-heals via the existing error-path verdict invalidation, a
    /// missing posting blob does **not** error — `scoring_postings_for_terms`
    /// just returns no postings for that shard, i.e. quietly fewer/zero
    /// hits (this asymmetry is exactly what PR #89 had to fix once already:
    /// the reader accepting a segment while its blobs silently weren't
    /// fetched). A positive verdict here that outlives a real eviction —
    /// `self.cache` LRU-evicts under `KOSHA_CACHE_MAX_BYTES` pressure — must
    /// therefore never be trusted past that eviction. Every stored entry is
    /// stamped with `self.cache.eviction_generation()` as of the moment it
    /// was verified, and a read compares that stamp against the *current*
    /// generation: any eviction anywhere (not just of this exact file —
    /// tracking that precisely isn't worth the plumbing given how rare
    /// eviction is relative to query volume) invalidates the snapshot,
    /// falling back to a real stat. See `Cache::eviction_generation`'s doc
    /// comment for the full rationale.
    ///
    /// The inner map is `Arc`-wrapped so a read only bumps a refcount
    /// (no per-request clone of a potentially-large map) and a write uses
    /// `Arc::make_mut` (copy-on-write only when a concurrent reader is
    /// still holding the old snapshot).
    ///
    /// Residual hazard the eviction-generation guard does *not* close: a
    /// shard evicted *between* a presence-cache hit (snapshot taken at
    /// request entry) and the actual `scoring_postings_for_terms` read on
    /// the cached segment reader — `eviction_generation` won't move again
    /// until the next eviction, and the read returns no postings for the
    /// now-missing shard (the PR #83 silent-term-drop class), not an
    /// error. Closing that race needs a per-read failure signal from the
    /// segment layer, complementary to this generation guard; tracked as
    /// follow-up work. Until it lands, correctness here depends on the
    /// `KOSHA_CACHE_MAX_BYTES` bound being generous enough to never LRU
    /// posting blobs of in-flight warm segments — the cache is sized for
    /// that today, but the invariant is not enforced.
    posting_blob_presence: Mutex<PostingBlobPresenceCache>,
    /// Readiness-gated warmup (Tier 1 O2): when `KOSHA_WARMUP_NAMESPACES`
    /// is set, a background thread prefetches the scoring-set files
    /// (`footer.json` + `inverted.idx` + `doc_store.offsets` [+ `filters.bin`/
    /// `vector.idx` per an empty query]) for each listed namespace before the
    /// pod is marked ready. `warmup_complete` flips to `true` exactly once
    /// per process, after the warmup pass (success or fail-open — a cold
    /// fallback still works, only the first-query latency is at stake).
    ///
    /// `/readyz` returns 503 while this is `false` and a warmup is in
    /// flight; `true` (or no configured warmup) returns 200. `/healthz`,
    /// used for *liveness*, stays always-200 so the restart loop is never
    /// influenced by a long warmup — the cold-pod incident class from
    /// RESULTS.md that HPA scale-out kept adding replicas in.
    warmup_complete: Arc<std::sync::atomic::AtomicBool>,
    /// Warmup configuration captured at boot: the list of namespaces to
    /// prefetch, empty when warmup is disabled. Kept on `AppState` so the
    /// `/readyz` handler can report what's pending.
    warmup_namespaces: Vec<String>,
    /// `KOSHA_WARMUP_POSTING_BLOBS` (default true): whether warmup also
    /// bulk-prefetches the posting-blob shards for every segment, not just
    /// the static scoring set. This is the cold-start-convoy fix: without
    /// it, warmup leaves all `postings-XX.bin` traffic to first-touch
    /// queries, which saturate the hydrator and queue behind each other
    /// for the first minutes after a cold start.
    warmup_posting_blobs: bool,
    /// `KOSHA_WARMUP_HYDRATE_SEGMENT_CACHE` (default true): whether warmup
    /// also opens every vector-bearing segment into `Searcher`'s live
    /// `segment_cache` (via `Searcher::warm_segments`), not just prefetching
    /// `vector.idx`/`vector.offsets` bytes to local disk. Without this, the
    /// bytes are fast to open but nothing has opened them yet — the first
    /// wave of real kNN queries still pays for every first-touch
    /// `open_segment` itself, which at corpus scale (every segment, on
    /// every query — see `knn_plan`) reproduces the same
    /// cold-start convoy this field's sibling exists to prevent. Its own
    /// flag rather than folding into `warmup_posting_blobs`: this phase
    /// mutates the *live* `segment_cache`/`MemoryLedger` that concurrent
    /// real queries also touch, a materially different risk tier than
    /// phases 1-4's pure disk-byte prefetch.
    warmup_hydrate_segment_cache: bool,
    /// Usage-driven warming (#213): read tracking, the persisted warm set,
    /// the byte budget, re-warm on manifest change and their reports. See
    /// `warm` and `warm_run`.
    warming: warm_run::Warming,
    /// `KOSHA_INGEST_HOST` — set only on query-role pods (deployment-query.yaml).
    /// When present, write-path requests this pod receives are forwarded here
    /// instead of executed locally — see `is_write_path`/`forward_to_ingest`.
    /// `None` on the ingest pod itself, which never forwards.
    ingest_host: Option<String>,
    /// Built once, lazily, only when `ingest_host` is set — no cost on the
    /// ingest pod. Bounded timeout, no built-in retry: a failed forward
    /// (e.g. the ingest pod mid-`Recreate` restart) fails fast and relies on
    /// the *caller's* own retry logic (kosha_client already has
    /// max_retries/retry_on_timeout), rather than retrying twice.
    write_http_client: Option<reqwest::blocking::Client>,
    /// Query-role manifest following (#232): refresher cadence, per-namespace
    /// last-confirmed-current times (lag) and control-plane error counters.
    /// Unused on the ingest pod, whose manifests are authoritative.
    manifest_follow: manifest_follow::ManifestFollow,
}

/// Per-segment single-flight completion signal used by
/// `ensure_segments_local`'s in-flight registry. `done` flips to `true`
/// exactly once, when the owning caller's fetch attempt finishes —
/// successfully or not; waiters only learn the actual outcome by re-checking
/// `AppState::segment_is_complete` themselves afterward, same as the owner
/// does, so a failed fetch is never mistaken for a successful one.
#[cfg(feature = "s3")]
#[derive(Default)]
struct SegmentFetch {
    done: Mutex<bool>,
    cv: Condvar,
}

/// Minimal counting semaphore built on `Mutex`/`Condvar`.
///
/// Both users of this (S3 hydration, search admission) are synchronous from
/// the caller's point of view — they run on the request-handling thread —
/// so a blocking semaphore matches the rest of the file's concurrency
/// model. (`std` has no built-in semaphore; pulling in
/// `tokio::sync::Semaphore` would mean adding the `sync` feature to a crate
/// that otherwise only uses tokio internally, behind a blocking boundary,
/// to run S3 SDK futures.)
#[derive(Default)]
struct SemState {
    permits: usize,
    /// Requests currently blocked in `acquire*`. Bounding this is what
    /// turns overload into shedding instead of unbounded queueing.
    waiters: usize,
}

struct Semaphore {
    state: Mutex<SemState>,
    cv: Condvar,
    /// Max requests allowed to wait for a permit. `0` = unbounded (the
    /// pre-#146 behavior).
    max_waiters: usize,
    /// How long a request may wait before being shed. `None` = forever.
    wait_timeout: Option<Duration>,
}

impl Semaphore {
    fn new(permits: usize) -> Self {
        Self::with_queue_limits(permits, 0, None)
    }

    fn with_queue_limits(
        permits: usize,
        max_waiters: usize,
        wait_timeout: Option<Duration>,
    ) -> Self {
        Self {
            state: Mutex::new(SemState {
                permits,
                waiters: 0,
            }),
            cv: Condvar::new(),
            max_waiters,
            wait_timeout,
        }
    }

    /// Block until a permit is available, then hold it until the returned
    /// guard is dropped. Unbounded — kept for callers where waiting is
    /// always preferable to failing.
    fn acquire(&self) -> SemaphorePermit<'_> {
        let mut st = self.state.lock().unwrap();
        while st.permits == 0 {
            st = self.cv.wait(st).unwrap();
        }
        st.permits -= 1;
        SemaphorePermit { sem: self }
    }

    /// Acquire a permit, or shed (`None`) if the queue is already too deep
    /// or the wait exceeds `wait_timeout`.
    ///
    /// This is the I/O-side analogue of [`kosha_query::MemoryLedger`]'s
    /// admission gate. The ledger bounds *bytes*; nothing bounded the
    /// accumulation of work when the machine simply could not keep up.
    /// Measured on the 10M vector namespace: with 8 permits and a service
    /// time inflated by storage saturation, arrivals queued until
    /// `queue_ms=118,942` against `score_ms=2,256` — every request waiting
    /// two minutes for a result its client had long since abandoned, while
    /// the box thrashed its disk and even sshd was starved of I/O.
    ///
    /// Failing fast is strictly better for everyone: the client gets a 429
    /// it can retry with backoff (kosha_client already does), and the
    /// server stops accepting work it has no capacity to finish. A queue
    /// deeper than the service rate can drain is not throughput, it is
    /// latency nobody asked for.
    fn acquire_or_shed(&self) -> Option<SemaphorePermit<'_>> {
        let mut st = self.state.lock().unwrap();
        if st.permits > 0 {
            st.permits -= 1;
            return Some(SemaphorePermit { sem: self });
        }
        // Fast-fail on a queue that is already saturated — waiting only to
        // time out later would burn the client's deadline for nothing.
        if self.max_waiters > 0 && st.waiters >= self.max_waiters {
            return None;
        }
        let deadline = self.wait_timeout.map(|t| Instant::now() + t);
        st.waiters += 1;
        let outcome = loop {
            if st.permits > 0 {
                st.permits -= 1;
                break true;
            }
            match deadline {
                None => st = self.cv.wait(st).unwrap(),
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        break false;
                    }
                    let (guard, _) = self.cv.wait_timeout(st, d - now).unwrap();
                    st = guard;
                }
            }
        };
        st.waiters -= 1;
        outcome.then(|| SemaphorePermit { sem: self })
    }

    #[cfg(test)]
    fn waiters(&self) -> usize {
        self.state.lock().unwrap().waiters
    }
}

struct SemaphorePermit<'a> {
    sem: &'a Semaphore,
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        let mut st = self.sem.state.lock().unwrap();
        st.permits += 1;
        self.sem.cv.notify_one();
    }
}

/// What one hydration pass actually did: which segments are still
/// incomplete, and how much this request pulled from S3. Bytes/files count
/// only this caller's owned fetches — segments another in-flight request
/// was already downloading contribute wait time (visible in the caller's
/// hydrate wall clock), not bytes. Not cfg-gated so non-S3 builds can log
/// zeroed hydration fields through the same code path.
#[derive(Default)]
struct HydrationOutcome {
    missing: Vec<String>,
    files_fetched: usize,
    bytes_fetched: u64,
    /// `true` when this pass's `to_hydrate` set was narrowed by a
    /// per-query bloom/term-prune check (see `hydrate_segments_for_search`)
    /// rather than covering every segment in the manifest. A "success"
    /// verdict from a bloom-pruned pass only proves *this query's* narrow
    /// segment subset is hydrated — it says nothing about the segments a
    /// *different* filter value would need. `completed_hydrations` is
    /// keyed on `(namespace, manifest_version)` only, shared across every
    /// query shape, so callers deciding whether to cache a "namespace
    /// fully hydrated" verdict must check this and skip caching when it's
    /// `true` — see the incident this caused: one query's lucky
    /// bloom-pruned subset got cached as "done" for the whole namespace,
    /// and every later, differently-filtered query silently scored
    /// against only that same stale subset forever (not self-healing —
    /// only a pod restart, which drops the cache, recovered it).
    bloom_pruned: bool,
    /// `true` when this request hit its hydration byte ceiling
    /// (`KOSHA_HYDRATE_REQUEST_MAX_BYTES`) or waited out the process-wide
    /// in-flight byte budget, and stopped fetching on purpose.
    ///
    /// `missing` is populated either way — the files really aren't local —
    /// but the distinction matters to the client: a shed is load-shedding
    /// (HTTP 429, retry with backoff, the same contract
    /// `Semaphore::acquire_or_shed` and `kosha_query::MemoryLedger` already
    /// use), whereas a non-shed `missing` is a genuine S3 failure (HTTP
    /// 503). Reporting a shed as a 503 would tell a caller its data is
    /// broken when the server merely declined to pull multiple GB inside
    /// one request.
    shed: bool,
    /// Bytes the page's documents pulled from S3 after scoring (#245):
    /// ranged spans or whole files. Filled in after the search runs.
    page: doc_spans::PageFetchStats,
    /// Sequential S3 round trips before scoring: the footer phase (legacy
    /// manifests only), the scoring scaffolding, and the postings (#245).
    /// The page's own rounds are in `page.rounds`.
    rounds: u64,
    /// What the query-term postings cost (#245): ranged term spans or
    /// whole shards. Its `rounds` are included in `rounds`.
    postings: posting_spans::PostingFetchStats,
    /// kNN postings fetched by range from `vector.idx` while scoring
    /// (#245). Segments fetch concurrently, so `rounds` is the most any one
    /// segment needed. Filled in after the search runs.
    vectors: posting_spans::PostingFetchStats,
}

/// Cached verdict for `completed_hydrations`: NOT a single "namespace is
/// fully hydrated" bool, but which *file classes* have actually been
/// confirmed local for every segment in this `(namespace, manifest_version)`
/// — because whether a hydration pass even looks at `filters.bin` or
/// `vector.idx` depends entirely on the query that drove it
/// (`query_needs_filter_store` / `query.knn.is_some()`). A pass for a plain
/// unfiltered, unsorted, non-kNN query — including warmup's own probe query,
/// see `warm_run::scaffolding_probe_query` — legitimately reports
/// `outcome.missing.is_empty()` with *neither* file ever having been checked. Recording that success as a
/// flat `true` let a *later* query that actually needs `filters.bin` (a
/// `query.filter`, a non-`_score`/`_id` sort, or `aggs`) or `vector.idx` (a
/// `knn` query) take the cached-verdict fast path (`ensure_query_posting_blobs`
/// only — no scoring-set check at all) and silently run against a segment
/// whose `filters.bin`/`vector.idx` was never fetched, scoring against
/// nothing. Confirmed live: a `documentId`-filtered, sorted, paginated
/// chunk-visibility check landed 100% zero-hit on every retry for several
/// minutes on a pod whose own warmup pass (or an earlier plain-text query)
/// had already cached a bare "hydrated" verdict for that manifest version —
/// not self-healing short of a new segment publish (bumps the version key)
/// or a pod restart (drops the map), the same non-recovery signature as
/// #166/#167.
///
/// Each flag is set only by a pass that itself needed (and therefore
/// verified, via `hydrate_segments_for_search`'s unconditional per-segment
/// check) that file class *and* was not bloom-pruned (`!bloom_pruned` — see
/// [`HydrationOutcome::bloom_pruned`]; a bloom-pruned pass only verifies a
/// query-specific segment subset, never the whole namespace, for either
/// file class). Flags accumulate (`|=`) across passes rather than being
/// overwritten, so e.g. a sorted query verifying `filters.bin` and a later
/// kNN query verifying `vector.idx` combine into one verdict that covers
/// both for everything after. A query needing neither file class is
/// trivially covered regardless of the flags (`covers(false, false)` is
/// always `true`) — the common warm-baseline-search case keeps today's
/// single-`HashMap`-lookup fast path.
///
/// Columnar filter segments (#244) make the filter class finer still: a
/// pass hydrates `filters.toc` plus only the columns *its* query names, so
/// the verdict records which column names were verified. A later query is
/// covered only if every column it names was (`filters.bin` segments are
/// verified whole by any filter pass, so the field set is what decides).
///
/// Vectors are verified per `knn.field` (#204): a kNN hydrates only the
/// files of the vector column it searches, so a pass for one column says
/// nothing about another's.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
struct HydrationVerdict {
    /// A filter-needing pass verified every segment's `filters.bin` or
    /// `filters.toc`.
    filters_verified: bool,
    /// Filter column names verified local in every columnar segment.
    filter_fields_verified: std::collections::BTreeSet<String>,
    /// Vector columns (`knn.field`s) verified local in every segment.
    vector_fields_verified: std::collections::BTreeSet<String>,
}

impl HydrationVerdict {
    /// Whether this verdict can be trusted to skip a full
    /// `hydrate_segments_for_search` pass for a query with these needs.
    /// `needs_filters` is the query's filter column names, `None` when it
    /// needs no filter files at all. `knn_field` is the query's
    /// `knn.field`, `None` for a non-kNN query.
    fn covers(&self, needs_filters: Option<&FilterFields>, knn_field: Option<&str>) -> bool {
        let filters_ok = needs_filters.is_none_or(|fields| {
            self.filters_verified && fields.is_subset(&self.filter_fields_verified)
        });
        filters_ok && knn_field.is_none_or(|field| self.vector_fields_verified.contains(field))
    }

    /// Record what a successful, non-bloom-pruned pass for a query with
    /// these needs actually verified, merging with (not replacing) whatever
    /// was already known for this manifest version.
    fn merge(&mut self, needs_filters: Option<&FilterFields>, knn_field: Option<&str>) {
        if let Some(fields) = needs_filters {
            self.filters_verified = true;
            self.filter_fields_verified.extend(fields.iter().cloned());
        }
        if let Some(field) = knn_field {
            self.vector_fields_verified.insert(field.to_string());
        }
    }
}

/// Whether the files a kNN on `field` reads from `seg_path` are local
/// (#204): `field`'s column only, as `footer` lists it (see
/// [`kosha_segment::knn_vector_columns`]). In ranged mode (#245) a column
/// whose offsets sidecar is local is complete: its probed postings are read
/// by range during the search. Otherwise the idx file is needed, plus the
/// sidecar where one exists — a legacy segment's only for the clustered
/// format: a clustered column without it is served by exact scan of every
/// vector (seconds on a large segment). A legacy segment that stores its
/// vectors in `doc_vectors.bin` also needs that file, whose field table
/// binds its unnamed index.
#[cfg(feature = "s3")]
fn knn_files_complete(
    seg_path: &Path,
    footer: Option<&kosha_core::Footer>,
    field: &str,
    vectors_ranged: bool,
) -> bool {
    for column in kosha_segment::knn_vector_columns(footer, field) {
        let offsets_local = column
            .offsets_file
            .as_ref()
            .is_some_and(|f| file_present_nonempty(&seg_path.join(f)));
        if vectors_ranged && offsets_local {
            continue;
        }
        if !file_present_nonempty(&seg_path.join(&column.idx_file)) {
            return false;
        }
        if column.offsets_file.is_some()
            && !offsets_local
            && (!column.legacy
                || kosha_segment::vector_file_needs_offsets(seg_path, &column.idx_file))
        {
            return false;
        }
    }
    !kosha_segment::knn_needs_doc_vectors(footer)
        || file_present_nonempty(&seg_path.join(kosha_segment::DOC_VECTORS_FILE))
}

/// The vector key a query's [`HydrationVerdict`] records and checks: its
/// `knn.field` for a single-vector kNN (#204), and a key of its own for a
/// late-interaction kNN (#205), whose pass hydrates the field's multivec
/// files rather than its single-vector column. `None` without kNN.
fn knn_verdict_key(query: &SearchQuery) -> Option<String> {
    query.knn.as_ref().map(|knn| {
        if knn.is_late_interaction() {
            format!("late_interaction:{}", knn.field)
        } else {
            knn.field.clone()
        }
    })
}

/// Filter column names a query reads (`SearchQuery::filter_column_fields`).
type FilterFields = std::collections::BTreeSet<String>;

/// Segments this caller must fetch itself, paired with the completion
/// signal it must flip via `complete_owned` once done.
#[cfg(feature = "s3")]
type OwnedFetches = Vec<(PathBuf, Arc<SegmentFetch>)>;

/// Partition `seg_paths` into segments this caller must fetch itself
/// (`owned`) and segments another in-flight caller is already fetching
/// (`waiting`), atomically with respect to `in_flight`: exactly one caller
/// becomes the owner for any given not-yet-cached segment.
///
/// Free function (rather than an `AppState` method) so it only depends on
/// the in-flight map and an `is_complete` predicate — this lets tests drive
/// it directly with a fake predicate/fetch instead of needing a real
/// `S3Storage`/network.
#[cfg(feature = "s3")]
fn partition_for_hydration(
    in_flight: &Mutex<HashMap<PathBuf, Arc<SegmentFetch>>>,
    seg_paths: &[PathBuf],
    is_complete: impl Fn(&Path) -> bool,
) -> (OwnedFetches, Vec<Arc<SegmentFetch>>) {
    let mut owned = Vec::new();
    let mut waiting = Vec::new();
    let mut guard = in_flight.lock().unwrap();
    for seg_path in seg_paths {
        if is_complete(seg_path) {
            continue;
        }
        match guard.get(seg_path) {
            Some(existing) => waiting.push(Arc::clone(existing)),
            None => {
                let entry = Arc::new(SegmentFetch::default());
                guard.insert(seg_path.clone(), Arc::clone(&entry));
                owned.push((seg_path.clone(), entry));
            }
        }
    }
    (owned, waiting)
}

/// Signal that `owned`'s fetch attempt is finished (regardless of outcome)
/// and remove each entry from `in_flight` so a future cache miss can retry
/// it — must be called exactly once per batch returned by
/// `partition_for_hydration`, whether the fetch succeeded or failed.
#[cfg(feature = "s3")]
fn complete_owned(in_flight: &Mutex<HashMap<PathBuf, Arc<SegmentFetch>>>, owned: &OwnedFetches) {
    let mut guard = in_flight.lock().unwrap();
    for (seg_path, entry) in owned {
        guard.remove(seg_path);
        let mut done = entry.done.lock().unwrap();
        *done = true;
        entry.cv.notify_all();
    }
}

/// Block until every entry in `waiting` has been signalled complete by its
/// owner (see `complete_owned`).
#[cfg(feature = "s3")]
fn wait_for(waiting: &[Arc<SegmentFetch>]) {
    for entry in waiting {
        let mut done = entry.done.lock().unwrap();
        while !*done {
            done = entry.cv.wait(done).unwrap();
        }
    }
}

/// Greedily group `(path, size)` pairs into batches whose total projected
/// size doesn't exceed `budget`, preserving input order — used by
/// `hydrate_from_s3_budgeted` so one hydration request can't queue more
/// than `budget` bytes of concurrent S3 fetches at once.
///
/// A single file larger than `budget` still gets its own (necessarily
/// over-budget) chunk rather than being starved forever: the budget bounds
/// how much *extra* concurrent work piles up around it, not any individual
/// file's unavoidable size. An empty `files` returns no chunks.
#[cfg(feature = "s3")]
fn chunk_by_byte_budget(files: &[(String, u64)], budget: u64) -> Vec<Vec<(String, u64)>> {
    let mut chunks: Vec<Vec<(String, u64)>> = Vec::new();
    let mut current: Vec<(String, u64)> = Vec::new();
    let mut current_size: u64 = 0;

    for (path, size) in files {
        if !current.is_empty() && current_size.saturating_add(*size) > budget {
            chunks.push(std::mem::take(&mut current));
            current_size = 0;
        }
        current_size = current_size.saturating_add(*size);
        current.push((path.clone(), *size));
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// Whether a segment file named `name` belongs in a hydration fetch list,
/// given whether the caller needs vectors. Factored out of
/// `ensure_segments_local` so the "skip vector.idx" rule has a direct unit
/// test instead of needing a real `S3Storage` to exercise.
#[cfg(feature = "s3")]
fn hydration_wants_file(name: &str, needs_vectors: bool) -> bool {
    // `vector.offsets` is the v2 posting-index sidecar (see kosha-segment's
    // "Vector index format" section) — it must be gated identically to
    // `vector.idx`, not just added to the "always fetch" bucket. The
    // original `needs_vectors || name != "vector.idx"` form only excluded
    // the one hardcoded name; naively extending it to `name !=
    // "vector.offsets"` too would have flipped this to `needs_vectors ||
    // true` for the sidecar (always fetch it, regardless of
    // `needs_vectors`) — the opposite bug from the one this function
    // exists to prevent. Every vector column's files (#204:
    // `vector.<n>.idx` / `vector.<n>.offsets`) are gated the same way.
    if kosha_core::is_vector_index_file(name) {
        needs_vectors
    } else {
        true
    }
}

/// Segments in `manifest` not yet recorded as synced in `synced`, in
/// manifest order.
///
/// Pure/testable half of `sync_unsynced_segments_to_s3`'s fix: the bug it
/// closes was re-deriving "the segment(s) to sync" from the manifest as
/// just "whichever one is last" on every call, which under concurrent
/// flushes could skip segments a *different* concurrent publish call also
/// thought weren't its job. Driving the decision off a durable synced-set
/// instead — "everything not yet recorded, regardless of position" — is
/// what this function tests in isolation from any actual S3 I/O.
#[cfg(feature = "s3")]
fn segments_needing_sync<'a>(
    manifest: &'a kosha_core::Manifest,
    synced: &std::collections::HashSet<(NamespaceId, kosha_core::SegmentId)>,
    ns: &NamespaceId,
) -> Vec<&'a kosha_core::ManifestEntry> {
    manifest
        .segments
        .iter()
        .filter(|e| !synced.contains(&(ns.clone(), e.segment_id.clone())))
        .collect()
}

/// How many times one publish rebases onto a newer remote manifest before
/// giving up with `ManifestConflict` (HTTP 409).
const MANIFEST_REBASE_ATTEMPTS: u32 = 3;

/// Wall-clock Unix epoch milliseconds.
fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Pause before rebase attempt `attempt` (2..): a few milliseconds, growing,
/// with jitter so two writers that keep colliding fall out of step.
fn rebase_backoff(attempt: u32) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let base_ms = 5u64 << attempt.min(6);
    Duration::from_millis(base_ms + u64::from(nanos) % base_ms)
}

/// HTTP response for a failed manifest publication: a conflict another
/// writer caused (and a rebase could not resolve) is a 409 the client can
/// retry; anything else is a server error.
fn publication_error(context: &str, error: &KoshaError) -> String {
    let status = if matches!(error, KoshaError::ManifestConflict { .. }) {
        409
    } else {
        500
    };
    json_error(status, &format!("{context}: {error}"))
}

/// Upload a segment directory to S3 and commit it: every data file first,
/// in a fixed (sorted) order, then the `_COMMIT` marker last, listing each
/// file's size and CRC32 (#231). Only a segment whose `_COMMIT` landed
/// counts as durable (`S3Storage::segment_durability`), so a crash or a
/// failed PUT partway through leaves a prefix that reconciliation will
/// re-upload instead of trusting.
///
/// Refuses to commit a directory that is missing any required segment file
/// locally: committing it would publish a segment that can never be opened.
///
/// Free function (not an `AppState` method) so both `AppState::sync_to_s3`
/// and the boot-time reconciliation sweep in `AppState::new`, which runs
/// before `self` exists, can share it.
/// Whether a local segment has its whole filter index (#244):
/// `filters.bin`, or `filters.toc` plus every column its footer lists.
/// The footer decides the format. A footer that does not parse is held to
/// the pre-#244 rule (`filters.bin` present); opening such a segment fails
/// on the footer anyway.
#[cfg(feature = "s3")]
fn filter_index_complete(seg_dir: &Path) -> bool {
    match SegmentReader::read_footer(seg_dir) {
        Ok(footer) => kosha_segment::filter_columns::filter_files_present(seg_dir, &footer),
        Err(_) => seg_dir.join(kosha_core::FILTERS_BIN_FILE).is_file(),
    }
}

#[cfg(feature = "s3")]
fn sync_segment_dir_to_s3(
    s3: &s3_storage::S3Storage,
    data_dir: &Path,
    seg_dir: &Path,
) -> Result<(), String> {
    let rel = seg_dir
        .strip_prefix(data_dir)
        .unwrap_or(seg_dir)
        .to_string_lossy()
        .into_owned();
    if !seg_dir.is_dir() {
        return Err(format!("segment {rel} not on local disk"));
    }
    for req in segment_commit::REQUIRED_SEGMENT_FILES {
        if !file_present_nonempty(&seg_dir.join(req)) {
            return Err(format!(
                "segment {rel} is missing required file {req} locally"
            ));
        }
    }
    if !filter_index_complete(seg_dir) {
        return Err(format!(
            "segment {rel} is missing filter index files locally"
        ));
    }
    s3.sync_segment_dir(Path::new(&rel))
        .map_err(|e| format!("S3 upload of segment {rel} failed: {e}"))
}

/// Parts the environment normally decides, supplied directly (tests).
#[derive(Default)]
struct StateOptions {
    /// WAL store instead of `KOSHA_WAL_MODE`.
    wal_store: Option<Arc<dyn kosha_write::wal::WalStore>>,
    /// Control store instead of `DATABASE_URL`.
    control_store: Option<Box<dyn ControlStore>>,
}

fn fatal(message: &str) -> ! {
    eprintln!("FATAL: {message}");
    std::process::exit(1)
}

/// `KOSHA_WAL_MODE` (#236): `local` (default) keeps the WAL in
/// `data_dir/_wal`; `s3` puts every group commit in the segment bucket
/// before acknowledging it, under `_wal/{KOSHA_WAL_WRITER_ID}/`, so an
/// acknowledged write survives losing the node. Only a node that accepts
/// writes uses it: a query pod forwards writes to ingest.
fn wal_store_from_env(
    #[cfg(feature = "s3")] s3: Option<&s3_storage::S3Storage>,
) -> Option<Arc<dyn kosha_write::wal::WalStore>> {
    let mode = std::env::var("KOSHA_WAL_MODE")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match mode.as_str() {
        "" | "local" => {
            println!(
                "WAL: local (KOSHA_WAL_MODE=local); acked writes are not S3-durable until flushed"
            );
            None
        }
        "s3" => {
            if std::env::var("KOSHA_INGEST_HOST").is_ok_and(|h| !h.is_empty()) {
                println!("WAL: writes are forwarded to ingest; KOSHA_WAL_MODE=s3 applies there");
                return None;
            }
            #[cfg(feature = "s3")]
            {
                let Some(s3) = s3 else {
                    fatal("KOSHA_WAL_MODE=s3 needs S3 storage (set KOSHA_S3_BUCKET)");
                };
                let writer = std::env::var("KOSHA_WAL_WRITER_ID")
                    .ok()
                    .map(|w| w.trim().to_owned())
                    .filter(|w| !w.is_empty())
                    .unwrap_or_else(|| "default".into());
                let timeout = s3_wal::timeout_from_env();
                let backend = s3_wal::S3WalBackend::new(s3, timeout)
                    .unwrap_or_else(|e| fatal(&format!("cannot start the S3 WAL client: {e}")));
                let store =
                    kosha_write::object_wal::ObjectWalStore::new(Arc::new(backend), &writer)
                        .unwrap_or_else(|e| fatal(&e.to_string()));
                if let Err(e) = store.verify_conditional_put() {
                    fatal(&format!(
                        "KOSHA_WAL_MODE=s3: the object store must honor If-None-Match: * on \
                         PutObject so one writer owns each WAL key ({e})"
                    ));
                }
                println!(
                    "WAL: s3 (bucket={} key={}/{}, timeout {timeout:?}); writes are acked after the S3 PUT",
                    s3.bucket(),
                    s3.prefix(),
                    store.root()
                );
                Some(Arc::new(store))
            }
            #[cfg(not(feature = "s3"))]
            fatal("KOSHA_WAL_MODE=s3 needs a build with the `s3` feature")
        }
        other => fatal(&format!(
            "unknown KOSHA_WAL_MODE {other:?}: use local or s3"
        )),
    }
}

impl AppState {
    fn new(data_dir: PathBuf) -> Self {
        Self::with_options(data_dir, StateOptions::default())
    }

    fn with_options(data_dir: PathBuf, options: StateOptions) -> Self {
        std::fs::create_dir_all(&data_dir).ok();
        // Segment files must live under data_dir (indexer/searcher paths).
        // KOSHA_CACHE_DIR is accepted for deploy overlays but Phase 1 always
        // materializes segments in KOSHA_DATA_DIR so paths stay consistent.
        if let Ok(extra) = std::env::var("KOSHA_CACHE_DIR") {
            let extra_path = PathBuf::from(&extra);
            if extra_path != data_dir {
                println!(
                    "WARN: KOSHA_CACHE_DIR={extra} differs from KOSHA_DATA_DIR; using data_dir for segment cache ({})",
                    data_dir.display()
                );
            }
        }
        // `KOSHA_CACHE_MAX_BYTES`: bound the NVMe cache and evict LRU files
        // once exceeded (DESIGN.md §9). Unset = unbounded (legacy behavior).
        let cache_max_bytes: Option<u64> = std::env::var("KOSHA_CACHE_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok());
        let cache = Cache::with_max_bytes(data_dir.clone(), cache_max_bytes);
        // `KOSHA_FLUSH_THRESHOLD`: docs buffered per namespace before an
        // auto-flush writes a new immutable segment (kosha-write's
        // Indexer::index_documents). Default (1000) is tuned for steady-state
        // production write volume, not bulk backfills — at that threshold a
        // 10M-doc load produces ~10k segments before any compaction. Bulk
        // loaders (and migrate.rs's offline --flush-docs, default 20000)
        // should raise this so fewer, larger segments get created.
        let flush_threshold: usize = std::env::var("KOSHA_FLUSH_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|n| *n > 0)
            .unwrap_or(1000);
        // `KOSHA_MAX_CONCURRENT_FLUSH_WRITERS` / `KOSHA_MIN_DOCS_PER_FLUSH_WRITER`:
        // bound on how many `SegmentWriter`s a single flush round can run
        // concurrently for one namespace, and how much buffered backlog a
        // round needs before it splits across more than one (see
        // kosha-write's `run_one_flush_pass`). Defaults: 4 writers, and
        // `flush_threshold` docs per writer — so an ordinary threshold flush
        // writes ONE segment, and only a real backlog (>= 2 x threshold,
        // a namespace that's fallen behind under sustained concurrent
        // ingest, issue #176's follow-up) fans out across the writer pool.
        // The previous fixed default of 250 split every >=500-doc flush
        // into 250-doc segments, which was the dominant segment-sprawl
        // source once clients stopped flushing per batch (#241: 20k docs
        // loaded in 500-doc batches -> 80 segments). Set
        // `KOSHA_MAX_CONCURRENT_FLUSH_WRITERS=1` to fall back to strictly
        // single-writer-per-namespace behavior.
        let max_concurrent_flush_writers: usize =
            std::env::var("KOSHA_MAX_CONCURRENT_FLUSH_WRITERS")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|n| *n > 0)
                .unwrap_or(4);
        let min_docs_per_flush_writer: usize = std::env::var("KOSHA_MIN_DOCS_PER_FLUSH_WRITER")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|n| *n > 0)
            .unwrap_or(flush_threshold);
        #[cfg(feature = "s3")]
        let s3_storage = {
            match s3_storage::S3Config::from_env() {
                Some(cfg) => {
                    let bucket = cfg.bucket.clone();
                    let prefix = cfg.prefix.clone();
                    let endpoint = cfg.endpoint.clone();
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build();
                    match rt {
                        Ok(rt) => {
                            let fut = s3_storage::S3Storage::new(data_dir.clone(), cfg);
                            match rt.block_on(fut) {
                                Ok(s3) => {
                                    println!(
                                        "S3 storage enabled: bucket={bucket} prefix={prefix:?} endpoint={endpoint:?} local_cache={}",
                                        data_dir.display()
                                    );
                                    Some(s3)
                                }
                                Err(e) => {
                                    eprintln!("Failed to init S3 storage: {e}");
                                    None
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to create tokio runtime: {e}");
                            None
                        }
                    }
                }
                None => {
                    println!(
                        "S3 storage disabled (KOSHA_S3_BUCKET unset); segments are local-only under {}",
                        data_dir.display()
                    );
                    None
                }
            }
        };

        let wal_store = options.wal_store.or_else(|| {
            wal_store_from_env(
                #[cfg(feature = "s3")]
                s3_storage.as_ref(),
            )
        });
        let indexer = match wal_store {
            Some(store) => Indexer::with_wal_store(data_dir.clone(), store)
                .unwrap_or_else(|e| fatal(&format!("cannot open the WAL: {e}"))),
            None => Indexer::new(data_dir.clone()),
        };
        let indexer = indexer
            .with_flush_threshold(flush_threshold)
            .with_max_concurrent_flush_writers(max_concurrent_flush_writers)
            .with_min_docs_per_flush_writer(min_docs_per_flush_writer);

        // `KOSHA_SEGMENT_CACHE_CAPACITY` / `KOSHA_SEGMENT_CACHE_MAX_BYTES`:
        // how many parsed segments the searcher keeps resident in memory
        // across queries, and the approximate byte budget for them.
        // Segments are immutable, so this is a pure memory/latency
        // trade-off, not a staleness one — see `kosha_query::SegmentCache`.
        // The byte budget is the one that actually bounds worst-case
        // memory: an unfiltered query can open dozens of segments in one
        // shot (nothing to bloom-prune), well under a generous count cap,
        // while still exhausting the container's memory if those segments
        // are individually large.
        let segment_cache_capacity: usize = std::env::var("KOSHA_SEGMENT_CACHE_CAPACITY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(kosha_query::DEFAULT_SEGMENT_CACHE_CAPACITY);
        let segment_cache_max_bytes: u64 = std::env::var("KOSHA_SEGMENT_CACHE_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(kosha_query::DEFAULT_SEGMENT_CACHE_MAX_BYTES);
        // `KOSHA_SEGMENT_LIVE_MAX_BYTES`: watermark for *live* segment
        // memory — cache-resident plus pinned by in-flight searches. This
        // is what actually bounds RSS under concurrent broad queries; the
        // cache budget above only bounds idle memory (see
        // `kosha_query::MemoryLedger` for the staging OOM this fixes).
        // Default: 2× the cache budget. Size it below the container memory
        // limit minus process baseline + working-memory headroom.
        let segment_live_max_bytes: u64 = std::env::var("KOSHA_SEGMENT_LIVE_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| {
                segment_cache_max_bytes.saturating_mul(kosha_query::DEFAULT_LIVE_BYTES_FACTOR)
            });
        // `KOSHA_ADMISSION_TIMEOUT_MS`: how long a search waits for live
        // memory to free up before being shed with HTTP 429 (which
        // kosha_client's retry/backoff already handles).
        let admission_timeout = std::env::var("KOSHA_ADMISSION_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(Duration::from_millis)
            .unwrap_or(kosha_query::DEFAULT_ADMISSION_TIMEOUT);
        let searcher = Searcher::with_memory_limits(
            data_dir.clone(),
            segment_cache_capacity,
            segment_cache_max_bytes,
            segment_live_max_bytes,
            admission_timeout,
        );

        // ── Control plane: in-memory or Postgres ─────────────────────────
        let mut control_plane_kind: &'static str = "in-memory";
        let control_store: Box<dyn ControlStore> = if let Some(store) = options.control_store {
            store
        } else if let Ok(db_url) = std::env::var("DATABASE_URL") {
            #[cfg(feature = "postgres")]
            match kosha_control::PgStore::connect_with_retry(&db_url) {
                Ok(store) => {
                    control_plane_kind = "postgres";
                    println!("control plane: postgres ({})", redact_database_url(&db_url));
                    Box::new(store)
                }
                Err(e) => {
                    // DATABASE_URL being *set* means Postgres is this
                    // deployment's intended control plane, not an optional
                    // extra — a pod that can't reach it after
                    // `connect_with_retry`'s retries has an empty namespace
                    // registry and cannot correctly serve any namespace.
                    // Silently falling back used to leave exactly that pod
                    // running with `/healthz`/`/readyz` both green, so
                    // Kubernetes kept routing real traffic to it
                    // indefinitely (only a manual pod restart recovered
                    // it — see the incident this fixes). Exiting here
                    // instead means the process never binds the listener,
                    // so no probe ever reports this pod healthy and the
                    // ordinary CrashLoopBackOff/restart cycle takes over —
                    // the standard, alertable way to surface "an essential
                    // dependency is unreachable at boot".
                    //
                    // `KOSHA_POSTGRES_FALLBACK_ON_ERROR=1` is an explicit
                    // escape hatch back to the old (dangerous) silent
                    // fallback, for emergency ops use only.
                    let fallback_allowed = std::env::var("KOSHA_POSTGRES_FALLBACK_ON_ERROR")
                        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                        .unwrap_or(false);
                    if fallback_allowed {
                        eprintln!(
                            "WARN: failed to connect to postgres after retries, falling back to \
                             in-memory (KOSHA_POSTGRES_FALLBACK_ON_ERROR=1): {e}"
                        );
                        Box::new(kosha_control::Controller::new())
                    } else {
                        eprintln!(
                            "FATAL: failed to connect to postgres after retries: {e}. \
                             DATABASE_URL is set, so an empty in-memory control plane would \
                             silently serve zero namespaces while still passing health checks. \
                             Exiting so Kubernetes restarts this pod instead. Set \
                             KOSHA_POSTGRES_FALLBACK_ON_ERROR=1 to opt back into the old \
                             silent-fallback behavior."
                        );
                        std::process::exit(1);
                    }
                }
            }
            #[cfg(not(feature = "postgres"))]
            {
                println!(
                    "control plane: in-memory (DATABASE_URL set but postgres feature disabled)"
                );
                let _ = db_url;
                Box::new(kosha_control::Controller::new())
            }
        } else {
            println!("control plane: in-memory (no DATABASE_URL)");
            Box::new(kosha_control::Controller::new())
        };

        // ── Restore manifests from the control store ───────────────────────
        // The Indexer starts empty on every boot; without this, previously
        // indexed namespaces vanish from search after a restart. Segment
        // files are re-fetched from S3 lazily at search/write time
        // (`ensure_segments_local`) when local disk is ephemeral.
        //
        // Query pods forward every write elsewhere (KOSHA_INGEST_HOST set)
        // and so never call publish_namespace/sync_to_s3 themselves — only
        // the ingest pod needs `synced_segments` seeded at all. Gating the
        // reconciliation sweep below on that (rather than running it
        // unconditionally on every pod) avoids adding S3 list_objects_v2
        // calls per segment to every query-pod boot/HPA scale-out, which
        // would work directly against the "gate readiness on actual
        // warmth" goal.
        #[cfg(feature = "s3")]
        let is_ingest_role = std::env::var("KOSHA_INGEST_HOST")
            .ok()
            .filter(|h| !h.is_empty())
            .is_none();
        #[cfg(feature = "s3")]
        let mut synced_segments = std::collections::HashSet::new();
        let mut restored = 0usize;
        let mut restored_segments = 0usize;
        #[cfg(feature = "s3")]
        let mut reconciled_missing = 0usize;
        for ns in control_store.list_namespaces() {
            if let Some(mut manifest) = control_store.manifest_cloned(&ns) {
                if !manifest.segments.is_empty() {
                    restored_segments += manifest.segments.len();

                    // ── Boot-time S3 durability reconciliation ──────────
                    // Closes the gap `sync_unsynced_segments_to_s3`'s doc
                    // comment describes for data already affected by it
                    // before this fix: a segment this process never
                    // confirmed as uploaded (fresh in-memory
                    // `synced_segments`, always empty at boot) gets a
                    // ground-truth S3 check here rather than waiting for
                    // this namespace's *next* flush to notice — which, for
                    // an otherwise-idle namespace, might be never.
                    #[cfg(feature = "s3")]
                    if is_ingest_role {
                        if let Some(ref s3) = s3_storage {
                            for entry in &manifest.segments {
                                let rel = format!("{}/{}", ns.0, entry.segment_id.0);
                                let durability = s3.segment_durability(&rel);
                                if durability.is_durable() {
                                    synced_segments.insert((ns.clone(), entry.segment_id.clone()));
                                    continue;
                                }
                                let seg_path = data_dir.join(&ns.0).join(&entry.segment_id.0);
                                match sync_segment_dir_to_s3(s3, &data_dir, &seg_path) {
                                    Ok(()) => {
                                        synced_segments
                                            .insert((ns.clone(), entry.segment_id.clone()));
                                        reconciled_missing += 1;
                                        println!(
                                            "reconciliation: re-uploaded segment not durable in \
                                             S3 ({durability:?}): {rel}"
                                        );
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "WARN: reconciliation found segment {rel} not durable \
                                             in S3 ({durability:?}) and could not re-upload it \
                                             ({e}) — data loss unless another copy exists"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                kosha_core::deletes::hydrate(&mut manifest, |segment, vector| {
                    let path = vector.path(&ns, segment)?;
                    #[cfg(feature = "s3")]
                    if let Some(ref s3) = s3_storage {
                        return s3.read(&path);
                    }
                    Ok(std::fs::read(data_dir.join(path))?)
                })
                .expect("cannot restore manifest deletion bitmaps");
                indexer.restore_manifest(ns, manifest);
                restored += 1;
            }
        }
        println!(
            "control plane: restored {restored} namespace(s), {restored_segments} segment ref(s); cache_root={} size_bytes={}",
            cache.root().display(),
            cache.total_size()
        );
        #[cfg(feature = "s3")]
        if reconciled_missing > 0 {
            println!(
                "reconciliation: uploaded {reconciled_missing} segment(s) that were missing \
                 from S3 at boot"
            );
        }

        let ingest_host = std::env::var("KOSHA_INGEST_HOST")
            .ok()
            .filter(|h| !h.is_empty());
        let write_http_client = ingest_host.as_ref().map(|h| {
            println!("write-path requests will be forwarded to ingest at {h}");
            reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(
                    // Matches the server's own request I/O timeout
                    // (`KOSHA_HTTP_IO_TIMEOUT_SECS`). The previous default of 30 s
                    // was shorter than a busy ingest node can take to answer a
                    // write that waits for a flush: concurrent bulk writes queue
                    // behind each other's flushes, and the ones that waited
                    // longest came back as a 503 "forward to ingest failed"
                    // although the ingest node then completed them.
                    std::env::var("KOSHA_INGEST_PROXY_TIMEOUT_SECS")
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .filter(|n| *n > 0)
                        .unwrap_or(300),
                ))
                .build()
                .expect("failed to build ingest-forwarding HTTP client")
        });

        // ── Cross-replica hydration coordination (s3 + postgres only) ──────
        // See `hydration_lease` module doc + this file's `hydrate_files` for
        // the full protocol. `own_addr` needs POD_IP (deployment-query.yaml's
        // downward-API env var); either piece missing just leaves
        // coordination inert (every pod fetches its own copy, as before).
        #[cfg(all(feature = "s3", feature = "postgres"))]
        let own_addr = std::env::var("POD_IP")
            .ok()
            .filter(|ip| !ip.is_empty())
            .map(|ip| {
                let port = std::env::var("KOSHA_HTTP_PORT").unwrap_or_else(|_| "8080".into());
                format!("{ip}:{port}")
            });
        #[cfg(all(feature = "s3", feature = "postgres"))]
        let hydration_leases = match (std::env::var("DATABASE_URL").ok(), &own_addr) {
            (Some(db_url), Some(_)) => match kosha_control::HydrationLeaseStore::new(&db_url) {
                Ok(store) => {
                    println!("hydration coordination: enabled (postgres leases)");
                    Some(store)
                }
                Err(e) => {
                    eprintln!(
                        "WARN: hydration lease store unavailable ({e}); replicas will not \
                         coordinate S3 fetches"
                    );
                    None
                }
            },
            (None, _) => {
                println!("hydration coordination: disabled (no DATABASE_URL)");
                None
            }
            (_, None) => {
                println!("hydration coordination: disabled (no POD_IP)");
                None
            }
        };
        #[cfg(all(feature = "s3", feature = "postgres"))]
        let peer_http_client = hydration_leases.as_ref().map(|_| {
            reqwest::blocking::Client::builder()
                // Short: a peer that isn't answering quickly should be
                // abandoned in favor of the S3 fallback, not held onto —
                // see `fetch_from_peer`.
                .timeout(Duration::from_secs(5))
                .build()
                .expect("failed to build peer-hydration HTTP client")
        });
        #[cfg(all(feature = "s3", feature = "postgres"))]
        let internal_auth_key = std::env::var("KOSHA_INTERNAL_API_KEY")
            .ok()
            .filter(|k| !k.is_empty());

        // `KOSHA_WARMUP_NAMESPACES` (Tier 1 O2): a comma-separated list of
        // namespaces this query pod prefetches the scoring set for before
        // marking itself ready. Empty (or S3 disabled) → no warmup, pod is
        // ready immediately. The thread that does the actual fetch is
        // spawned by `spawn_warmup` from `main`, *after* state construction
        // (it needs the live `Arc<AppState>`); `warmup_complete` starts
        // `true` and only flips to `false` when there's work to do.
        let warmup_namespaces: Vec<String> = std::env::var("KOSHA_WARMUP_NAMESPACES")
            .ok()
            .map(|v| {
                v.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        // KOSHA_WARMUP_POSTING_BLOBS=0 restricts warmup to the static
        // scoring set (pre-#TBD behavior). Default on: the whole point of
        // warming a namespace is that the first real queries don't convoy
        // behind first-touch posting-blob hydration — measured on the 10M
        // MSMarco cold run as a ~3-minute window where p99 is queue-bound
        // at 7–12s while steady-state is 529ms (RESULTS.md round 6).
        let warmup_posting_blobs =
            env_flag_default_on(std::env::var("KOSHA_WARMUP_POSTING_BLOBS").ok().as_deref());
        let warmup_hydrate_segment_cache = env_flag_default_on(
            std::env::var("KOSHA_WARMUP_HYDRATE_SEGMENT_CACHE")
                .ok()
                .as_deref(),
        );
        if !warmup_namespaces.is_empty() {
            println!(
                "warmup: configured for {} namespace(s): {} (posting blobs: {}, segment cache: {})",
                warmup_namespaces.len(),
                warmup_namespaces.join(", "),
                if warmup_posting_blobs { "on" } else { "off" },
                if warmup_hydrate_segment_cache {
                    "on"
                } else {
                    "off"
                },
            );
        }

        // Resolved once: it is both the per-batch pre-GET chunk size and the
        // capacity of the process-wide in-flight gate (see the field docs on
        // `hydrate_byte_budget`).
        #[cfg(feature = "s3")]
        let hydrate_byte_budget = std::env::var("KOSHA_HYDRATE_BYTE_BUDGET")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|n| *n > 0)
            .unwrap_or(1024 * 1024 * 1024);
        #[cfg(feature = "s3")]
        let span_hydrator =
            doc_spans::SpanHydrator::new(data_dir.clone(), doc_spans::SpanConfig::from_env());
        #[cfg(feature = "s3")]
        let posting_span_hydrator = posting_spans::PostingSpanHydrator::new(
            data_dir.clone(),
            posting_spans::config_from_env(),
        );

        Self {
            controller: Mutex::new(control_store),
            admin_auth_key: std::env::var("KOSHA_ADMIN_API_KEY")
                .ok()
                .filter(|k| !k.is_empty()),
            internal_service_key: std::env::var("KOSHA_INTERNAL_API_KEY")
                .ok()
                .filter(|k| !k.is_empty()),
            publication_locks: Mutex::new(HashMap::new()),
            #[cfg(feature = "s3")]
            segment_checksum_failures: std::sync::atomic::AtomicU64::new(0),
            #[cfg(feature = "s3")]
            doc_store_fallbacks: std::sync::atomic::AtomicU64::new(0),
            indexer,
            searcher,
            data_dir,
            cache,
            control_plane_kind,
            #[cfg(feature = "s3")]
            s3_storage,
            #[cfg(feature = "s3")]
            hydrate_concurrency: std::env::var("KOSHA_HYDRATE_CONCURRENCY")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|n| *n > 0)
                .unwrap_or(16),
            // 1GiB default. Process-wide, not per-batch:
            // KOSHA_MAX_CONCURRENT_HYDRATIONS batches all reserve against
            // the same `hydration_budget`, so adding hydration ops no longer
            // multiplies the bound the way the pre-GET chunker alone did. (Process heap is bounded
            // separately, and by construction: response bodies stream to
            // disk — see `s3_storage::stream_body_to_file`.)
            #[cfg(feature = "s3")]
            hydrate_byte_budget,
            #[cfg(feature = "s3")]
            hydration_budget: Arc::new(HydrationBudget::new(
                hydrate_byte_budget,
                hydration_budget::DEFAULT_BUDGET_WAIT,
            )),
            #[cfg(feature = "s3")]
            hydrate_request_max_bytes: std::env::var("KOSHA_HYDRATE_REQUEST_MAX_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|n| *n > 0)
                .unwrap_or(hydration_budget::DEFAULT_REQUEST_MAX_BYTES),
            #[cfg(feature = "s3")]
            scoring_hydrate_concurrency: std::env::var("KOSHA_SCORING_HYDRATE_CONCURRENCY")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|n| *n > 0)
                .unwrap_or(64),
            #[cfg(feature = "s3")]
            synced_segments: Mutex::new(synced_segments),
            #[cfg(feature = "s3")]
            in_flight_segments: Mutex::new(HashMap::new()),
            // Each hydration operation can itself fan out up to
            // `hydrate_concurrency` (default 16) concurrent S3 GETs, so a
            // small operation-level ceiling still allows meaningful GET
            // parallelism (default 4 ops × 16 fan-out = 64 concurrent GETs
            // worst case) while capping how many *independent* segment
            // batches (and their buffered bytes) can be resident at once.
            #[cfg(feature = "s3")]
            hydration_semaphore: Semaphore::new(
                std::env::var("KOSHA_MAX_CONCURRENT_HYDRATIONS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .filter(|n| *n > 0)
                    .unwrap_or(4),
            ),
            doc_read_mode: doc_spans::ReadMode::from_env(),
            #[cfg(feature = "s3")]
            span_hydrator,
            #[cfg(feature = "s3")]
            doc_store_promotions: doc_spans::PromotionQueue::default(),
            doc_store_reads: doc_spans::DocStoreReadCounters::default(),
            posting_read_mode: posting_spans::read_mode_from_env(),
            #[cfg(feature = "s3")]
            posting_span_hydrator,
            #[cfg(feature = "s3")]
            posting_promotions: doc_spans::PromotionQueue::default(),
            posting_reads: posting_spans::PostingReadCounters::default(),
            search_semaphore: {
                let permits = std::env::var("KOSHA_MAX_CONCURRENT_SEARCHES")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(8);
                // Queue bound + wait cap: overload must shed, not pile up.
                // Default queue depth is 4x the permits — enough to ride out
                // a burst, far short of the two-minute queues that formed on
                // the 10M vector namespace when the disk saturated.
                let max_waiters = std::env::var("KOSHA_SEARCH_QUEUE_DEPTH")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(permits * 4);
                let wait_ms: u64 = std::env::var("KOSHA_SEARCH_QUEUE_TIMEOUT_MS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(10_000);
                Semaphore::with_queue_limits(
                    permits,
                    max_waiters,
                    (wait_ms > 0).then(|| Duration::from_millis(wait_ms)),
                )
            },
            #[cfg(all(feature = "s3", feature = "postgres"))]
            hydration_leases,
            #[cfg(all(feature = "s3", feature = "postgres"))]
            own_addr,
            #[cfg(all(feature = "s3", feature = "postgres"))]
            peer_http_client,
            #[cfg(all(feature = "s3", feature = "postgres"))]
            internal_auth_key,
            self_ref: Weak::new(),
            backfill_jobs: Mutex::new(std::collections::HashSet::new()),
            compaction: compaction_sched::CompactionRegistry::new(
                compaction_sched::SchedulerConfig::from_env(),
            ),
            gc_settings: segment_gc::GcSettings::from_env(),
            gc_stats: segment_gc::GcStats::default(),
            gc_lock: Mutex::new(()),
            completed_hydrations: Mutex::new(HashMap::new()),
            result_cache: Mutex::new(ResultCache::new(result_cache_budget())),
            rerankers: rerank::RerankRegistry::from_env(),
            posting_blob_presence: Mutex::new(HashMap::new()),
            // Warmup starts ready; `spawn_warmup` flips `false` only when
            // there's configured work to do (and back to `true` on
            // completion / fail-open). See the field's doc comment.
            warmup_complete: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            warmup_namespaces,
            warmup_posting_blobs,
            warmup_hydrate_segment_cache,
            warming: warm_run::Warming::from_env(),
            ingest_host,
            write_http_client,
            manifest_follow: manifest_follow::ManifestFollow::new(
                manifest_follow::refresh_interval_from_env(),
            ),
        }
    }

    /// Construct inside an `Arc` with `self_ref` wired up — the constructor
    /// `main` uses so handlers can spawn background jobs that own the state.
    fn new_shared(data_dir: PathBuf) -> Arc<Self> {
        Arc::new_cyclic(|weak| {
            let mut state = Self::new(data_dir);
            state.self_ref = weak.clone();
            state
        })
    }

    /// Kick off the readiness-gated warmup pass (Tier 1 O2), if one is
    /// configured. Called from `main` after `new_shared`, *before* `serve`,
    /// so the listener is bound but `/readyz` stays 503 until the prefetch
    /// finishes — keeping HPA-scaled pods out of the Service endpoints
    /// while they're still cold-thrashing (the staging incident class in
    /// RESULTS.md).
    ///
    /// Fail-open throughout: a warmup pass that errors leaves
    /// `warmup_complete` flipped to `true` either way at the end — a cold
    /// first query still works via the live `hydrate_segments_for_search`
    /// path, only its latency is at stake, never correctness.
    fn spawn_warmup(self: &Arc<Self>) {
        #[cfg(not(feature = "s3"))]
        {
            // No durable tier → nothing to prefetch; pod is ready as-is.
        }
        #[cfg(feature = "s3")]
        {
            // Usage-driven warming (#213) needs object storage to read the
            // persisted warm set from; explicit namespaces warm either way.
            let usage = self.warming.cfg.usage && self.s3_storage.is_some();
            if self.warmup_namespaces.is_empty() && !usage {
                return;
            }
            self.warmup_complete
                .store(false, std::sync::atomic::Ordering::SeqCst);
            let state = Arc::clone(self);
            std::thread::Builder::new()
                .name("kosha-warmup".into())
                .spawn(move || {
                    let start = Instant::now();
                    state.run_boot_warmup(&*state);
                    state
                        .warmup_complete
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    println!(
                        "warmup: complete in {:.1}s — pod is ready",
                        start.elapsed().as_secs_f64()
                    );
                })
                .expect("failed to spawn warmup thread");
        }
    }

    /// Start the idle-buffer flush timer (#241) on a pod that accepts
    /// writes. Clients no longer `/flush` after every bulk batch — that cut
    /// one segment per ≤500-doc batch — so something must publish a buffer
    /// that never reaches `KOSHA_FLUSH_THRESHOLD`: the tail of a bulk load,
    /// or a trickle of single-document writes. This is that something, and
    /// it bounds write-to-visible latency at about `max_age` (plus one tick)
    /// the way Elasticsearch's `refresh_interval` does.
    ///
    /// `KOSHA_FLUSH_MAX_AGE_MS` (default 1000; `0` disables): how long a
    /// namespace may hold pending (buffered or WAL-recovered) documents
    /// before they're flushed and published. Age is measured from the first
    /// tick that saw the namespace non-empty, so a buffer that keeps
    /// growing is still flushed on schedule rather than postponed forever.
    /// Query-role pods (`KOSHA_INGEST_HOST` set) forward writes and hold no
    /// buffers, so they never start it. An explicit `/flush` stays an
    /// immediate visibility barrier.
    fn spawn_idle_flush_timer(self: &Arc<Self>) {
        let forwards_writes = std::env::var("KOSHA_INGEST_HOST")
            .ok()
            .is_some_and(|h| !h.is_empty());
        if forwards_writes {
            return;
        }
        let Some(max_age) = flush_max_age_from_env() else {
            return;
        };
        let tick = idle_flush_tick(max_age);
        let state = Arc::clone(self);
        std::thread::Builder::new()
            .name("kosha-idle-flush".into())
            .spawn(move || {
                let mut pending_since = HashMap::new();
                loop {
                    std::thread::sleep(tick);
                    run_idle_flush_tick(&state, &mut pending_since, max_age, Instant::now());
                }
            })
            .expect("failed to spawn idle-flush thread");
    }

    /// Start the background compaction scheduler (#242) on a pod that
    /// accepts writes. Every `KOSHA_COMPACTION_INTERVAL_MS` (default 30 s,
    /// `0` disables) it starts a tiered pass on each namespace that
    /// `needs_compaction` flags, up to `KOSHA_COMPACTION_MAX_CONCURRENT`
    /// passes at once (default 1, API jobs included). The idle-flush timer
    /// cuts a segment per interval for slowly written namespaces; this is
    /// what keeps their segment count bounded without anyone calling
    /// `/v1/admin/compact-namespace`. Selection, fairness and backoff are in
    /// `compaction_sched`. Query-role pods own no segments and never start it.
    fn spawn_compaction_scheduler(self: &Arc<Self>) {
        let forwards_writes = std::env::var("KOSHA_INGEST_HOST")
            .ok()
            .is_some_and(|h| !h.is_empty());
        if forwards_writes {
            return;
        }
        let Some(interval) = self.compaction.config().interval else {
            println!("compaction scheduler disabled (KOSHA_COMPACTION_INTERVAL_MS=0)");
            return;
        };
        println!(
            "compaction scheduler: every {} ms, max {} concurrent pass(es)",
            interval.as_millis(),
            self.compaction.config().max_concurrent
        );
        let state = Arc::clone(self);
        std::thread::Builder::new()
            .name("kosha-compaction-scheduler".into())
            .spawn(move || loop {
                std::thread::sleep(interval);
                run_compaction_scheduler_tick(&state, Instant::now());
            })
            .expect("failed to spawn compaction scheduler thread");
    }

    /// Start local segment reclamation (#239) on every pod. Every
    /// `KOSHA_LOCAL_GC_INTERVAL_MS` (default 60 s, `0` disables) it removes
    /// the local files of segments and deletion generations this node's
    /// manifest dropped, once `KOSHA_LOCAL_GC_GRACE` (default 5 min) has
    /// passed and no search can still read them. See [`Self::run_local_gc`].
    fn spawn_local_gc(self: &Arc<Self>) {
        let Some(interval) = self.gc_settings.local_interval else {
            println!("local segment GC disabled (KOSHA_LOCAL_GC_INTERVAL_MS=0)");
            return;
        };
        println!(
            "local segment GC: every {} ms, grace {} s",
            interval.as_millis(),
            self.gc_settings.local_grace.as_secs()
        );
        let state = Arc::clone(self);
        std::thread::Builder::new()
            .name("kosha-local-gc".into())
            .spawn(move || loop {
                std::thread::sleep(interval);
                state.run_local_gc(state.gc_settings.local_grace);
            })
            .expect("failed to spawn local GC thread");
    }

    /// One local reclamation pass. A segment's directory is removed only
    /// when the indexer's checks pass (grace, committed, no live search
    /// snapshot) *and* the searcher holds no open reader of it; the removal
    /// goes through the disk cache so its size accounting and
    /// `eviction_generation` (which invalidates posting-blob presence
    /// verdicts) stay right.
    fn run_local_gc(&self, grace: Duration) -> kosha_write::LocalReclaimReport {
        let report = self.indexer.reclaim_local(
            grace,
            |ns, seg| self.searcher.release_segment(&ns.0, &seg.0),
            |rel| self.cache.evict(rel),
        );
        self.gc_stats.record_local(&report);
        if report.segments_removed + report.deletion_vectors_removed > 0 {
            println!(
                "local GC: removed {} segment(s) and {} deletion generation(s), {:.1} MB; \
                 {} still waiting",
                report.segments_removed,
                report.deletion_vectors_removed,
                report.bytes_removed as f64 / (1024.0 * 1024.0),
                report.pending
            );
        }
        report
    }

    /// The object store segment GC sweeps: S3 when configured, otherwise
    /// the local data directory (single-node deployments, tests).
    fn with_gc_store<T>(&self, f: impl FnOnce(&dyn StorageBackend) -> T) -> T {
        #[cfg(feature = "s3")]
        if let Some(ref s3) = self.s3_storage {
            return f(&s3_storage::S3GcStore(s3));
        }
        f(&LocalStorage::new(self.data_dir.clone()))
    }

    /// Object-store GC over `namespaces` (#239): see `kosha_core::gc`.
    fn run_segment_gc(
        &self,
        namespaces: &[NamespaceId],
        config: &kosha_core::gc::GcConfig,
    ) -> Vec<kosha_core::gc::NamespaceGcReport> {
        let _serial = self.gc_lock.lock().unwrap();
        let all = self.controller.lock().unwrap().list_namespaces();
        let mut reports = Vec::new();
        for ns in namespaces {
            let others: Vec<NamespaceId> = all.iter().filter(|n| *n != ns).cloned().collect();
            let mut load = || Ok(self.controller.lock().unwrap().manifest_cloned(ns));
            let mut commit = |expected: u64, manifest: kosha_core::Manifest| {
                // Same lock as a local publish, so this node's writer sees
                // the pruned version as an ordinary concurrent commit.
                let lock = self.publication_lock(ns);
                let _guard = lock.lock().unwrap();
                self.controller
                    .lock()
                    .unwrap()
                    .compare_and_swap_manifest(ns, expected, manifest)
            };
            let report = self.with_gc_store(|store| {
                kosha_core::gc::collect_namespace(
                    store,
                    ns,
                    &others,
                    &mut load,
                    &mut commit,
                    config,
                )
            });
            let report = match report {
                Ok(report) => report,
                Err(e) => kosha_core::gc::NamespaceGcReport {
                    namespace: ns.0.clone(),
                    dry_run: config.dry_run,
                    aborted: Some(format!("GC failed: {e}")),
                    ..Default::default()
                },
            };
            self.gc_stats.record_run(&report);
            if !config.dry_run {
                println!(
                    "segment GC: ns={} version={} deleted {} segment(s), {} deletion \
                     generation(s), {} object(s), {:.1} MB; pruned {} record(s){}",
                    report.namespace,
                    report.manifest_version,
                    report.segments_deleted,
                    report.deletion_vectors_deleted,
                    report.objects_deleted,
                    report.bytes_reclaimed as f64 / (1024.0 * 1024.0),
                    report.records_pruned,
                    report
                        .aborted
                        .as_deref()
                        .map(|a| format!("; aborted: {a}"))
                        .unwrap_or_default()
                );
            }
            reports.push(report);
        }
        self.gc_stats
            .runs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.gc_stats
            .last_run_ms
            .store(unix_millis(), std::sync::atomic::Ordering::Relaxed);
        reports
    }

    /// Readiness-gate predicate used by the `/readyz` handler: true when no
    /// warmup is configured or it has finished (success or fail-open). Not a
    /// health signal — liveness stays on `/healthz`.
    fn is_ready(&self) -> bool {
        self.warmup_complete
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Make sure a query replica's manifest for `ns` is fresh enough for a
    /// search (#232). No-op on the ingest pod.
    ///
    /// The background refresher (`spawn_manifest_refresher`) keeps followed
    /// namespaces within about one `KOSHA_MANIFEST_REFRESH_MS` of the
    /// control store, so by default a search touches neither the control
    /// store nor its lock. It refreshes synchronously only when
    /// - the namespace was never loaded or never confirmed current here,
    /// - the request's `max_staleness_seconds` is below the current lag, or
    /// - the refresher is disabled (`KOSHA_MANIFEST_REFRESH_MS=0`), the
    ///   pre-#232 check-before-every-search behavior.
    ///
    /// A control-plane error never fails a search for a namespace this pod
    /// has loaded: it keeps serving the last-known-good manifest (DESIGN.md
    /// §17) and the lag keeps growing. Other errors (an unreadable
    /// deletion vector for the new version) still fail the request.
    fn ensure_query_manifest_fresh(
        &self,
        ns: &NamespaceId,
        max_staleness: Option<Duration>,
    ) -> Result<(), KoshaError> {
        if self.ingest_host.is_none() {
            return Ok(());
        }
        let now = Instant::now();
        let loaded = self.indexer.manifest_version(ns).is_some();
        let lag = self.manifest_follow.lag(ns, now);
        let max_staleness = max_staleness.or(match self.manifest_follow.interval {
            Some(_) => None,
            None => Some(Duration::ZERO),
        });
        let refresh = match (loaded, lag, max_staleness) {
            (false, _, _) | (true, None, _) => true,
            (true, Some(lag), Some(max)) => lag > max,
            (true, Some(_), None) => false,
        };
        if !refresh {
            return Ok(());
        }
        // While the control store is failing, don't make every search wait
        // out its timeout under the controller lock: serve last-known-good
        // and let the refresher find out when it is back.
        let window = self
            .manifest_follow
            .interval
            .unwrap_or(Duration::from_secs(1))
            .max(Duration::from_millis(100));
        if loaded && self.manifest_follow.failing_recently(now, window) {
            return Ok(());
        }
        match self.refresh_query_namespace(ns, false) {
            Ok(()) => {
                self.manifest_follow.record_poll_ok();
                Ok(())
            }
            Err(error @ KoshaError::ControlPlane(_)) => {
                self.manifest_follow
                    .record_error(&format!("search refresh of '{}'", ns.0), &error);
                if loaded {
                    Ok(())
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Bring a query replica's manifest for `ns` up to the stored version:
    /// one cheap version read, then the manifest itself only if it moved.
    /// `NamespaceNotFound` when the control store has no such namespace
    /// (never created, or deleted with `DELETE /v1/namespaces/{ns}`), so a
    /// replica that still holds a deleted namespace answers 404 instead of
    /// serving its old documents.
    fn refresh_query_namespace(&self, ns: &NamespaceId, warm: bool) -> Result<(), KoshaError> {
        let observed_at = Instant::now();
        let stored = self.controller.lock().unwrap().manifest_version(ns)?;
        match stored {
            Some(stored) => self.follow_stored_version(ns, stored, observed_at, warm),
            None => Err(KoshaError::NamespaceNotFound(ns.clone())),
        }
    }

    /// Install the stored manifest for `ns` if `stored` (read at
    /// `observed_at`) is ahead of the local one. Shared by the per-search
    /// path and the background refresher.
    ///
    /// The swap is `Indexer::restore_manifest`; a search already running
    /// holds its own `Arc` from `Indexer::manifest_shared` and finishes on
    /// the manifest it started with. When `warm` and the namespace is in
    /// `KOSHA_WARMUP_NAMESPACES`, the new version's footers and scoring set
    /// are fetched before the swap, so the first search on it is not cold.
    fn follow_stored_version(
        &self,
        ns: &NamespaceId,
        stored: u64,
        observed_at: Instant,
        warm: bool,
    ) -> Result<(), KoshaError> {
        if self
            .indexer
            .manifest_version(ns)
            .is_some_and(|local| local >= stored)
        {
            self.manifest_follow.confirm(ns, stored, observed_at);
            return Ok(());
        }
        self.manifest_follow.note_stored_version(ns, stored);
        let Some(mut manifest) = self.controller.lock().unwrap().try_manifest_cloned(ns)? else {
            return Ok(());
        };
        let version = manifest.version;
        if self
            .indexer
            .manifest_version(ns)
            .is_none_or(|local| local < version)
        {
            kosha_core::deletes::hydrate(&mut manifest, |segment, vector| {
                let path = vector.path(ns, segment)?;
                #[cfg(feature = "s3")]
                if let Some(ref s3) = self.s3_storage {
                    return s3.read(&path);
                }
                Ok(std::fs::read(self.data_dir.join(path))?)
            })?;
            if warm {
                self.prewarm_followed_manifest(ns, &manifest);
            }
            println!(
                "manifest refreshed: ns={} version={} segments={}",
                ns.0,
                manifest.version,
                manifest.segments.len()
            );
            self.indexer.restore_manifest(ns.clone(), manifest);
        }
        self.manifest_follow.confirm(ns, version, observed_at);
        Ok(())
    }

    /// Fetch the scoring set of a manifest about to be installed, for
    /// namespaces listed in `KOSHA_WARMUP_NAMESPACES` (same probe as boot
    /// warmup's first phase). Fail-open: a miss only costs the first search
    /// on the new version some latency.
    fn prewarm_followed_manifest(&self, ns: &NamespaceId, manifest: &kosha_core::Manifest) {
        #[cfg(feature = "s3")]
        {
            if self.s3_storage.is_none() || !self.warmup_namespaces.contains(&ns.0) {
                return;
            }
            let query = warmup_probe_query();
            let outcome = self.hydrate_segments_for_search(
                ns,
                manifest,
                &query,
                &RequestHydration::unlimited(),
            );
            if outcome.missing.is_empty() && !outcome.bloom_pruned {
                self.completed_hydrations
                    .lock()
                    .unwrap()
                    .entry((ns.0.clone(), manifest.version))
                    .or_default()
                    .merge(None, None);
            } else {
                eprintln!(
                    "warn: manifest refresh: prewarm of '{}' v{} left {} segment(s) cold",
                    ns.0,
                    manifest.version,
                    outcome.missing.len()
                );
            }
        }
        #[cfg(not(feature = "s3"))]
        let _ = (ns, manifest);
    }

    /// Start the query-role manifest refresher (#232). Every
    /// `KOSHA_MANIFEST_REFRESH_MS` (default 500; `0` disables) it reads every
    /// namespace's stored version in one query and installs the ones that
    /// advanced (`follow_stored_version`). The ingest pod never starts it.
    /// The thread holds only a `Weak` and exits when the state is dropped.
    fn spawn_manifest_refresher(self: &Arc<Self>) {
        if self.ingest_host.is_none() {
            return;
        }
        let Some(interval) = self.manifest_follow.interval else {
            println!(
                "manifest refresher disabled (KOSHA_MANIFEST_REFRESH_MS=0); \
                 query pods check the control store before every search"
            );
            return;
        };
        println!("manifest refresher: every {} ms", interval.as_millis());
        let weak = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("kosha-manifest-refresher".into())
            .spawn(move || loop {
                let Some(state) = weak.upgrade() else {
                    return;
                };
                state.run_manifest_refresh_tick();
                drop(state);
                std::thread::sleep(interval);
            })
            .expect("failed to spawn manifest refresher thread");
    }

    /// One refresher pass: a single batched version read, then fetch and
    /// swap only namespaces whose version moved. Errors leave the
    /// last-known-good manifests in place.
    ///
    /// A namespace loaded here that the poll no longer lists was deleted
    /// (`DELETE /v1/namespaces/{ns}` on the ingest pod): it is forgotten,
    /// with its caches, so the next search on it loads afresh and answers
    /// 404 instead of serving the deleted documents.
    fn run_manifest_refresh_tick(&self) {
        let observed_at = Instant::now();
        let versions = self.controller.lock().unwrap().manifest_versions();
        let versions = match versions {
            Ok(versions) => versions,
            Err(error) => {
                self.manifest_follow.record_error("version poll", &error);
                return;
            }
        };
        let listed: std::collections::HashSet<&NamespaceId> =
            versions.iter().map(|(ns, _)| ns).collect();
        for ns in self.indexer.namespaces() {
            if listed.contains(&ns) {
                continue;
            }
            // Re-check alone: the namespace may have been created (and
            // loaded by a search) after the batched poll.
            match self.controller.lock().unwrap().manifest_version(&ns) {
                Ok(None) => {}
                Ok(Some(_)) => continue,
                Err(error) => {
                    self.manifest_follow
                        .record_error(&format!("deletion check of '{}'", ns.0), &error);
                    continue;
                }
            }
            if self.indexer.forget_namespace(&ns) {
                println!(
                    "manifest refresh: ns={} deleted from the control store; forgotten",
                    ns.0
                );
            }
            self.manifest_follow.forget(&ns);
            self.forget_namespace_caches(&ns);
        }
        let mut control_plane_ok = true;
        for (ns, stored) in versions {
            match self.follow_stored_version(&ns, stored, observed_at, true) {
                Ok(()) => {}
                Err(error @ KoshaError::ControlPlane(_)) => {
                    control_plane_ok = false;
                    self.manifest_follow
                        .record_error(&format!("fetch of '{}'", ns.0), &error);
                }
                Err(error) => eprintln!(
                    "warn: manifest refresh: cannot install '{}' v{stored}: {error} — \
                     serving the previous version",
                    ns.0
                ),
            }
        }
        if control_plane_ok {
            self.manifest_follow.record_poll_ok();
        }
    }

    /// Read the namespace's committed manifest with its deletion vectors
    /// resolved, the way a query replica installs one.
    fn hydrated_remote_manifest(
        &self,
        ns: &NamespaceId,
    ) -> Result<Option<kosha_core::Manifest>, KoshaError> {
        let Some(mut manifest) = self.controller.lock().unwrap().manifest_cloned(ns) else {
            return Ok(None);
        };
        kosha_core::deletes::hydrate(&mut manifest, |segment, vector| {
            let path = vector.path(ns, segment)?;
            #[cfg(feature = "s3")]
            if let Some(ref s3) = self.s3_storage {
                return s3.read(&path);
            }
            Ok(std::fs::read(self.data_dir.join(path))?)
        })?;
        Ok(Some(manifest))
    }

    /// Install a manifest built outside the write path (admin import) by
    /// CAS against the version the control store holds right now. Never
    /// rebases: a concurrent publish wins and this fails with
    /// `ManifestConflict`, leaving both the store and the indexer alone.
    fn import_manifest(
        &self,
        ns: &NamespaceId,
        mut manifest: kosha_core::Manifest,
    ) -> Result<(), KoshaError> {
        let lock = self.publication_lock(ns);
        let _guard = lock.lock().unwrap();
        let expected = self
            .controller
            .lock()
            .unwrap()
            .manifest_version(ns)?
            .unwrap_or(0);
        let local = self.indexer.manifest_version(ns).unwrap_or(0);
        manifest.version = manifest.version.max(expected.max(local) + 1);
        // Whatever the replaced manifest referenced is now garbage (#239).
        if let Some(previous) = self.controller.lock().unwrap().manifest_cloned(ns) {
            let now = unix_millis();
            kosha_core::gc::record_retirements(
                &kosha_core::gc::CommittedRefs::of(&previous),
                &mut manifest,
                |_, _| now,
            );
        }
        self.controller.lock().unwrap().compare_and_swap_manifest(
            ns,
            expected,
            manifest.clone(),
        )?;
        #[cfg(feature = "s3")]
        {
            // Imported segments are read from S3; there is nothing local to
            // upload for them.
            let mut synced = self.synced_segments.lock().unwrap();
            for entry in &manifest.segments {
                synced.insert((ns.clone(), entry.segment_id.clone()));
            }
        }
        self.indexer.restore_manifest(ns.clone(), manifest);
        Ok(())
    }

    fn publication_lock(&self, ns: &NamespaceId) -> Arc<Mutex<()>> {
        self.publication_locks
            .lock()
            .unwrap()
            .entry(ns.clone())
            .or_default()
            .clone()
    }

    /// Drop every per-namespace cache entry this node keeps for a deleted
    /// namespace. Versions never repeat across a delete and re-create, so
    /// this is hygiene (memory) rather than correctness, except for the
    /// result cache on a writer that searched unpublished versions.
    fn forget_namespace_caches(&self, ns: &NamespaceId) {
        self.completed_hydrations
            .lock()
            .unwrap()
            .retain(|(cached, _), _| cached != &ns.0);
        self.result_cache.lock().unwrap().forget_namespace(&ns.0);
        self.posting_blob_presence.lock().unwrap().remove(&ns.0);
        #[cfg(feature = "s3")]
        self.synced_segments
            .lock()
            .unwrap()
            .retain(|(cached, _)| cached != ns);
    }

    fn publish_namespace(&self, ns: &NamespaceId) -> Result<(), KoshaError> {
        self.publish_namespace_with(ns, |manifest| {
            #[cfg(feature = "s3")]
            if self.s3_storage.is_some() {
                self.sync_manifest_to_s3(ns, manifest)?;
            }
            let _ = manifest;
            Ok(())
        })
    }

    /// Serialize publications per namespace and use the same immutable
    /// snapshot for object upload and manifest commit. Failed uploads never
    /// expose a manifest; failed commits remain retryable.
    ///
    /// A `ManifestConflict` means another writer committed since this
    /// writer's last publish. The remote manifest is read back, this
    /// writer's unpublished delta is rebased onto it
    /// ([`Indexer::rebase_onto`]), and the CAS is retried, up to
    /// [`MANIFEST_REBASE_ATTEMPTS`] times with a short jittered pause. A
    /// conflict the rebase cannot resolve (see `rebase_onto`) is returned
    /// as `ManifestConflict`, which the HTTP layer maps to 409.
    fn publish_namespace_with(
        &self,
        ns: &NamespaceId,
        upload: impl Fn(&kosha_core::Manifest) -> Result<(), KoshaError>,
    ) -> Result<(), KoshaError> {
        let lock = self.publication_lock(ns);
        let _guard = lock.lock().unwrap();
        let mut rebases = 0;
        loop {
            let Some((expected_version, manifest)) = self.indexer.prepare_publication(ns)? else {
                return Ok(());
            };
            let started = Instant::now();
            upload(&manifest)?;
            let committed = self.controller.lock().unwrap().compare_and_swap_manifest(
                ns,
                expected_version,
                manifest.clone(),
            );
            match committed {
                Ok(()) => {
                    self.indexer.acknowledge_manifest(ns, &manifest)?;
                    println!(
                        "manifest published: ns={} version={} segments={} ms={:.1}",
                        ns.0,
                        manifest.version,
                        manifest.segments.len(),
                        started.elapsed().as_secs_f64() * 1e3
                    );
                    return Ok(());
                }
                Err(conflict @ KoshaError::ManifestConflict { .. })
                    if rebases < MANIFEST_REBASE_ATTEMPTS =>
                {
                    rebases += 1;
                    let Some(remote) = self.hydrated_remote_manifest(ns)? else {
                        return Err(conflict);
                    };
                    eprintln!(
                        "manifest conflict on namespace {} (expected version {expected_version}, \
                         found {}): rebasing, attempt {rebases}/{MANIFEST_REBASE_ATTEMPTS}",
                        ns.0, remote.version
                    );
                    #[cfg(feature = "s3")]
                    {
                        // The committed manifest's segments are durable by
                        // the publish protocol; the other writer uploaded
                        // them and this node has nothing local to send.
                        let mut synced = self.synced_segments.lock().unwrap();
                        for entry in &remote.segments {
                            synced.insert((ns.clone(), entry.segment_id.clone()));
                        }
                    }
                    if self.indexer.rebase_onto(ns, remote)? == kosha_write::RebaseOutcome::Adopted
                    {
                        return Ok(());
                    }
                    if rebases > 1 {
                        std::thread::sleep(rebase_backoff(rebases));
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Upload every segment in `ns`'s current manifest that this process
    /// hasn't already durably synced to S3.
    ///
    /// Used to upload only `manifest.segments.last()` — correct for one
    /// flush at a time, but not under concurrent flushes: the bulk loader's
    /// N-way concurrent batches can each trigger an auto-flush, and two
    /// `publish_namespace` calls racing each other can both read the
    /// manifest *after* both flushes appended, so both see the same
    /// segment as "last" — whichever segment got superseded as "last" by a
    /// later append before its own publish call ran was never uploaded by
    /// anyone. This is what caused 174 of 376 segments in a real benchmark
    /// run to exist only on the ingest node's local disk: an ingest node
    /// loss would have been a permanent, silent loss of that data.
    ///
    /// Tracking exactly which segments this process has already uploaded
    /// (`synced_segments`) and re-scanning the whole current manifest on
    /// every publish call closes the race: whichever call runs — first,
    /// last, concurrently with others — picks up everything still
    /// outstanding, and steady-state (one flush at a time) still only
    /// re-examines one new segment per call, same cost as before.
    #[cfg(feature = "s3")]
    fn sync_manifest_to_s3(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
    ) -> Result<(), KoshaError> {
        let pending: Vec<kosha_core::SegmentId> = {
            let synced = self.synced_segments.lock().unwrap();
            segments_needing_sync(manifest, &synced, ns)
                .into_iter()
                .map(|e| e.segment_id.clone())
                .collect()
        };
        let Some(ref s3) = self.s3_storage else {
            return Ok(());
        };
        for segment_id in pending {
            let seg_path = self.data_dir.join(&ns.0).join(&segment_id.0);
            // Only mark synced once the segment is committed (`_COMMIT`
            // written last, #231). A partial upload must be retried by a
            // future publish call, never treated as durable.
            sync_segment_dir_to_s3(s3, &self.data_dir, &seg_path).map_err(KoshaError::NotFound)?;
            self.synced_segments
                .lock()
                .unwrap()
                .insert((ns.clone(), segment_id));
        }
        if let Some(ref s3) = self.s3_storage {
            for (segment, vector) in &manifest.deletion_vectors {
                let path = vector.path(ns, segment)?;
                let bytes = std::fs::read(self.data_dir.join(&path))?;
                s3.write(&path, &bytes)?;
            }
        }
        Ok(())
    }

    /// See [`segment_commit::REQUIRED_SEGMENT_FILES`].
    #[cfg(feature = "s3")]
    const REQUIRED_SEGMENT_FILES: [&'static str; 3] = segment_commit::REQUIRED_SEGMENT_FILES;

    #[cfg(feature = "s3")]
    fn segment_is_complete(seg_path: &Path) -> bool {
        Self::REQUIRED_SEGMENT_FILES
            .iter()
            .all(|f| seg_path.join(f).is_file())
            && filter_index_complete(seg_path)
    }

    /// Ensure every listed segment directory is available locally, fanning
    /// out S3 downloads for all of them (and all their files) as a single
    /// batch instead of hydrating segment-by-segment, file-by-file.
    ///
    /// DESIGN.md §8 step 4 calls for per-segment retrieval "fanned out
    /// across the query node's worker pool" — the old code instead awaited
    /// one blocking `s3.read()` at a time, so total hydration latency was
    /// the sum of every S3 GET across every segment/file in the manifest.
    /// For a namespace with many segments (or one big enough to need many),
    /// that serial chain is what turned a single search into tens of
    /// seconds to multiple minutes.
    ///
    /// Returns the relative paths of segments that are still incomplete
    /// after the attempt. Every failure along the way (S3 list, S3 GET, or
    /// the final local write) is logged as a `WARN` and otherwise
    /// swallowed so one bad segment doesn't abort hydrating the rest — but
    /// that means a caller that ignores this return value can't tell "this
    /// segment has no data" from "hydration failed for this segment," and
    /// would silently search a partial corpus. Callers must check this and
    /// fail the request rather than return a deceptively successful empty
    /// result.
    ///
    /// Concurrent callers racing on the same not-yet-cached segment(s) are
    /// coalesced via `in_flight_segments`: only the first ("owner") actually
    /// hits S3 for a given segment, and the rest ("waiters") block until the
    /// owner finishes, then fall through to the same `segment_is_complete`
    /// recheck the owner uses — so a failed fetch is visible to waiters too,
    /// not silently treated as success. `hydration_semaphore` additionally
    /// bounds how many *owner* hydration batches run at once, server-wide.
    ///
    /// `needs_vectors` gates whether each segment's `vector.idx` is fetched
    /// at all. It's deliberately excluded from `REQUIRED_SEGMENT_FILES`
    /// (lexical queries never load it — see that const's doc comment), but
    /// until now `ensure_segments_local` still fetched it unconditionally
    /// anyway, because it just downloaded whatever S3 listed for each
    /// segment. For a vector-bearing namespace `vector.idx` is the largest
    /// file per segment by a wide margin, so every non-knn search — which
    /// is all current traffic — was paying to hydrate a file it was
    /// guaranteed never to open. Pass `true` from any caller that must
    /// preserve a segment's full fidelity (index/replace/compact/admin
    /// rewrite paths); the search path passes `query.knn.is_some()`.
    #[cfg(feature = "s3")]
    fn ensure_segments_local(
        &self,
        seg_paths: &[PathBuf],
        needs_vectors: bool,
    ) -> HydrationOutcome {
        let Some(ref s3) = self.s3_storage else {
            return HydrationOutcome::default();
        };

        // Full-segment hydration serves index/replace/compact/admin work,
        // not a user-facing search: a partial fetch there is worse than a
        // slow one, so these never shed on a byte ceiling. They still
        // reserve against the process-wide in-flight budget.
        let request = RequestHydration::unlimited();

        let mut outcome = HydrationOutcome::default();
        let (owned, waiting) = partition_for_hydration(
            &self.in_flight_segments,
            seg_paths,
            Self::segment_is_complete,
        );

        if !owned.is_empty() {
            // Bound total concurrent hydration *operations* server-wide —
            // released as soon as this operation's fetch is done, before
            // waiters below get to run.
            let _permit = self.hydration_semaphore.acquire();

            let mut logical_paths: Vec<(String, u64)> = Vec::new();
            for (seg_path, _) in &owned {
                let Ok(rel_path) = seg_path.strip_prefix(&self.data_dir) else {
                    continue;
                };
                let s3_prefix = rel_path.to_string_lossy().into_owned();
                match s3.list_with_sizes(&s3_prefix) {
                    Ok(files) if !files.is_empty() => {
                        logical_paths.extend(
                            files
                                .into_iter()
                                .filter(|(name, _size)| hydration_wants_file(name, needs_vectors))
                                .map(|(name, size)| (format!("{s3_prefix}/{name}"), size)),
                        );
                    }
                    Ok(_) => {
                        eprintln!("WARN: no S3 objects found for segment {s3_prefix}");
                    }
                    Err(e) => {
                        eprintln!("WARN: S3 list failed for segment {s3_prefix}: {e}");
                    }
                }
            }

            if !logical_paths.is_empty() {
                println!(
                    "cache miss: hydrating {} file(s) across {} segment(s) from S3 (fan-out={}, byte-budget={})",
                    logical_paths.len(),
                    owned.len(),
                    self.hydrate_concurrency,
                    self.hydrate_byte_budget
                );
                outcome.files_fetched += logical_paths.len();
                outcome.bytes_fetched += logical_paths.iter().map(|(_, size)| size).sum::<u64>();
                // Routed through `hydrate_files`, which pins the batch
                // against cache eviction for its whole duration (#62's
                // convergence fix) and coordinates the fetch across
                // replicas when the lease store is configured. Full-segment
                // fetches keep the conservative fan-out — batches here can
                // include multi-hundred-MB doc stores.
                // Files already on disk before this call were verified (or
                // trusted) when they first landed; only check what this call
                // actually fetches, so a warm segment isn't re-read in full.
                let fetched: Vec<(String, u64)> = logical_paths
                    .iter()
                    .filter(|(p, _)| !file_present_nonempty(&self.data_dir.join(p)))
                    .cloned()
                    .collect();
                self.hydrate_files(s3, &logical_paths, self.hydrate_concurrency, &request);

                // Check freshly fetched files against the segment's `_COMMIT`
                // (#231). A mismatch is deleted and refetched once. If it
                // still mismatches, it is deleted again, so the segment
                // reports as incomplete (`outcome.missing`) and the caller
                // fails the request instead of serving corrupt bytes.
                let bad = verify_fetched_against_commit(&self.data_dir, &fetched);
                if !bad.is_empty() {
                    self.segment_checksum_failures
                        .fetch_add(bad.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    let retry: Vec<(String, u64)> = fetched
                        .iter()
                        .filter(|(p, _)| bad.contains(p))
                        .cloned()
                        .collect();
                    self.hydrate_files(s3, &retry, self.hydrate_concurrency, &request);
                    let still_bad = verify_fetched_against_commit(&self.data_dir, &retry);
                    if !still_bad.is_empty() {
                        self.segment_checksum_failures.fetch_add(
                            still_bad.len() as u64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        eprintln!(
                            "ERROR: segment file(s) fail their _COMMIT checksum after refetch, \
                             refusing to serve: {}",
                            still_bad.join(", ")
                        );
                    }
                }
            }

            drop(_permit);
            complete_owned(&self.in_flight_segments, &owned);
        }

        wait_for(&waiting);

        outcome.missing = seg_paths
            .iter()
            .filter(|p| !Self::segment_is_complete(p))
            .filter_map(|p| p.strip_prefix(&self.data_dir).ok())
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        outcome
    }

    /// Fetch a batch of `(logical_path, projected_size_bytes)` pairs from
    /// S3, chunked so no single `read_many` call is ever asked to queue more
    /// than `hydrate_byte_budget` bytes' worth of files at once.
    ///
    /// `hydrate_concurrency` alone bounds *file count* in flight, not
    /// volume — a batch of `hydrate_concurrency` (default 16) large HNSW
    /// vector files is a very different footprint than 16 footer.jsons, and
    /// nothing before this compared a batch's actual byte size against the
    /// pod's capacity before firing it off. Sizes come from
    /// `list_with_sizes`' S3-reported object sizes, projected ahead of any
    /// fetch.
    ///
    /// That projection is only available where the caller already paid for a
    /// LIST. Callers that deliberately skip it (`ensure_scoring_files_local`
    /// and `ensure_files_local`, which know their file names a priori) pass
    /// size `0`, so this chunker cannot bound them at all — every one of
    /// their files lands in a single chunk. `request` and the process-wide
    /// `hydration_budget` are what cover that case, from the `Content-Length`
    /// each GET response carries; see [`hydration_budget`].
    #[cfg(feature = "s3")]
    fn hydrate_from_s3_budgeted(
        &self,
        s3: &s3_storage::S3Storage,
        files: &[(String, u64)],
        concurrency: usize,
        request: &Arc<RequestHydration>,
    ) {
        let chunks = chunk_by_byte_budget(files, self.hydrate_byte_budget);
        let total_chunks = chunks.len();
        for (chunk_idx, chunk) in chunks.into_iter().enumerate() {
            // A request that blew its byte ceiling mid-batch stops here
            // rather than working through the remaining chunks it has
            // already decided not to serve.
            if request.is_shed() {
                println!(
                    "hydrate_files shed: stopped at chunk {}/{} after {:.1}MB \
                     (KOSHA_HYDRATE_REQUEST_MAX_BYTES)",
                    chunk_idx + 1,
                    total_chunks,
                    request.fetched_bytes() as f64 / (1024.0 * 1024.0),
                );
                break;
            }
            let paths: Vec<String> = chunk.into_iter().map(|(path, _size)| path).collect();
            let t_chunk = Instant::now();
            let results = s3.read_many(&paths, concurrency, Some(&self.hydration_budget), request);
            // Aggregate per-file timing into the cold-read "next lever"
            // instrument (plan Tier 1 O13): rank-orders GET vs write vs
            // fan-out so the *next* change (raise concurrency vs skip bytes
            // vs reduce writes) is chosen from data, not guess. `cached`
            // files (already local) and failures are tracked separately.
            // `budget_wait_ms` is the exception to the per-chunk framing —
            // it is the *request's* cumulative wait on the in-flight byte
            // budget so far, which is the figure worth reading when asking
            // whether `KOSHA_HYDRATE_BYTE_BUDGET` is the thing throttling
            // cold search.
            let mut fetched = 0u64;
            let mut bytes = 0u64;
            let mut get_sum = 0.0f64;
            let mut write_sum = 0.0f64;
            let mut get_max = 0.0f64;
            let mut cached = 0u64;
            let mut failed = 0u64;
            let mut shed = 0u64;
            for (path, result) in &results {
                match result {
                    Ok(stats) => {
                        if stats.cached {
                            cached += 1;
                        } else {
                            fetched += 1;
                            bytes += stats.bytes;
                            get_sum += stats.get_ms;
                            write_sum += stats.write_ms;
                            if stats.get_ms > get_max {
                                get_max = stats.get_ms;
                            }
                        }
                        self.cache.note_external_write(path);
                    }
                    // A shed is a deliberate admission decision, not a
                    // download failure — counting it as one would turn one
                    // over-budget request into hundreds of WARN lines and
                    // hide real S3 errors in the noise.
                    Err(KoshaError::Overloaded(_)) => {
                        shed += 1;
                    }
                    Err(e) => {
                        failed += 1;
                        eprintln!("WARN: S3 download failed for {path}: {e}");
                    }
                }
            }
            let wall_ms = t_chunk.elapsed().as_secs_f64() * 1e3;
            println!(
                "hydrate_files timing: chunk={}/{} fetched={} cached={} failed={} shed={} \
                 bytes={:.1}MB wall_ms={:.1} get_ms={:.1}(max={:.1}) write_ms={:.1} \
                 budget_wait_ms={:.1} concurrency={}",
                chunk_idx + 1,
                total_chunks,
                fetched,
                cached,
                failed,
                shed,
                bytes as f64 / (1024.0 * 1024.0),
                wall_ms,
                get_sum,
                get_max,
                write_sum,
                request.wait_ms(),
                concurrency,
            );
        }
    }

    /// Ensure one named file (e.g. `footer.json`) is local for every segment
    /// in `seg_paths`, in a single fanned-out batch rather than one S3 GET
    /// per segment.
    ///
    /// This is the bloom-prune prefetch used by `hydrate_segments_for_search`
    /// before it decides which segments are even worth fully hydrating. The
    /// file name is already known (unlike `ensure_segments_local`, which
    /// lists each segment's directory first), so this skips straight to
    /// `read_many` — no per-segment `s3.list()` round trip either.
    #[cfg(feature = "s3")]
    /// Returns `(files_fetched, bytes_fetched)` — already-local files cost
    /// nothing and aren't counted.
    /// Fetch what materializing documents from these segments needs:
    /// `doc_store.bin`, plus `doc_vectors.bin` for segments whose footer
    /// says vectors were moved there (#243). Only those segments are asked
    /// for the sidecar, so older segments never trigger a GET for a file
    /// they don't have.
    #[cfg(feature = "s3")]
    fn ensure_doc_store_local(
        &self,
        seg_paths: &[PathBuf],
        request: &Arc<RequestHydration>,
    ) -> (usize, u64) {
        let (mut files, mut bytes) = self.ensure_files_local(seg_paths, "doc_store.bin", request);
        let with_sidecar: Vec<PathBuf> = seg_paths
            .iter()
            .filter(|p| kosha_segment::segment_uses_doc_vectors(p))
            .cloned()
            .collect();
        if !with_sidecar.is_empty() {
            let (f, b) =
                self.ensure_files_local(&with_sidecar, kosha_segment::DOC_VECTORS_FILE, request);
            files += f;
            bytes += b;
        }
        (files, bytes)
    }

    /// Make the page documents a search is about to materialize readable
    /// locally (the [`DocStoreHydrator`] the search handlers pass down).
    ///
    /// `ranged` (default, #245): fetch just the documents' spans with
    /// coalesced ranged GETs into the sparse span cache, under the
    /// hydration semaphore and the request's byte accountant; segments the
    /// ranged path cannot serve (legacy, or a failed range) and every
    /// segment in `whole` mode get the whole files instead, as before #245.
    /// Segments hot enough on the ranged path are queued for background
    /// whole-file promotion.
    #[cfg(feature = "s3")]
    fn hydrate_page_docs(
        &self,
        pages: &[PageSegment<'_>],
        request: &Arc<RequestHydration>,
    ) -> doc_spans::PageFetchStats {
        let mut stats = doc_spans::PageFetchStats::default();
        let whole: Vec<PathBuf> = match (self.doc_read_mode, &self.s3_storage) {
            (_, None) => return stats,
            (doc_spans::ReadMode::Whole, Some(_)) => {
                pages.iter().map(|p| p.segment_dir.to_path_buf()).collect()
            }
            (doc_spans::ReadMode::Ranged, Some(s3)) => {
                let fetcher = doc_spans::S3RangeFetcher {
                    s3,
                    concurrency: self.scoring_hydrate_concurrency,
                    budget: &self.hydration_budget,
                    request,
                };
                let outcome = {
                    let _permit = self.hydration_semaphore.acquire();
                    self.span_hydrator.hydrate(pages, &fetcher, &self.cache)
                };
                stats.rounds = outcome.rounds;
                stats.ranged_gets = outcome.gets;
                stats.ranged_bytes = outcome.bytes;
                for seg in outcome.promote {
                    self.doc_store_reads.promotion_queued();
                    self.doc_store_promotions.push(seg);
                }
                outcome.whole_fallback
            }
        };
        if !whole.is_empty() {
            let (files, bytes) = self.ensure_doc_store_local(&whole, request);
            stats.rounds += 1;
            stats.whole_files = files as u64;
            stats.whole_bytes = bytes;
        }
        self.doc_store_reads.record(&stats);
        stats
    }

    /// Start the thread that promotes hot segments from ranged reads to a
    /// whole local `doc_store.bin` (+ `doc_vectors.bin`), then drops their
    /// now-redundant spans (#245). One segment at a time, each under a
    /// hydration permit, so promotions queue behind live queries' fetches
    /// instead of competing with them.
    fn spawn_doc_store_promoter(self: &Arc<Self>) {
        #[cfg(feature = "s3")]
        {
            if self.s3_storage.is_none() || self.doc_read_mode != doc_spans::ReadMode::Ranged {
                return;
            }
            // `Weak`, re-upgraded per wait: the thread must not keep the
            // state alive on its own.
            let weak = Arc::downgrade(self);
            std::thread::Builder::new()
                .name("kosha-doc-promote".into())
                .spawn(move || loop {
                    let Some(state) = weak.upgrade() else { return };
                    if let Some(seg) = state
                        .doc_store_promotions
                        .pop_timeout(Duration::from_secs(1))
                    {
                        state.promote_doc_store(&seg);
                    }
                })
                .expect("failed to spawn doc-store promotion thread");
        }
    }

    /// Hydrate one segment's whole doc store for promotion and drop its
    /// spans once it is in place.
    #[cfg(feature = "s3")]
    fn promote_doc_store(&self, seg: &Path) {
        let t = Instant::now();
        let (files, bytes) = {
            let _permit = self.hydration_semaphore.acquire();
            self.ensure_doc_store_local(&[seg.to_path_buf()], &RequestHydration::unlimited())
        };
        let landed = file_present_nonempty(&seg.join(kosha_segment::DOC_STORE_FILE))
            && (!kosha_segment::segment_uses_doc_vectors(seg)
                || file_present_nonempty(&seg.join(kosha_segment::DOC_VECTORS_FILE)));
        self.span_hydrator
            .finish_promotion(&self.cache, seg, landed);
        if landed {
            self.doc_store_reads.promotion_done();
            self.doc_store_reads.record(&doc_spans::PageFetchStats {
                whole_files: files as u64,
                whole_bytes: bytes,
                ..Default::default()
            });
        }
        println!(
            "doc store promote: seg={} landed={landed} fetched {files} file(s) ({:.1} MB) in {:.0} ms",
            seg.display(),
            bytes as f64 / (1024.0 * 1024.0),
            t.elapsed().as_secs_f64() * 1e3,
        );
    }

    #[cfg(feature = "s3")]
    fn ensure_files_local(
        &self,
        seg_paths: &[PathBuf],
        file_name: &str,
        request: &Arc<RequestHydration>,
    ) -> (usize, u64) {
        let Some(ref s3) = self.s3_storage else {
            return (0, 0);
        };

        let mut logical_paths: Vec<(String, u64)> = Vec::new();
        for seg_path in seg_paths {
            if file_present_nonempty(&seg_path.join(file_name)) {
                continue;
            }
            let Ok(rel_path) = seg_path.strip_prefix(&self.data_dir) else {
                continue;
            };
            // Size unknown without a LIST round trip — 0 is fine: the byte
            // budget then can't chunk this batch, but these are single
            // known-name files (footers), small by construction.
            logical_paths.push((format!("{}/{file_name}", rel_path.to_string_lossy()), 0));
        }

        if logical_paths.is_empty() {
            return (0, 0);
        }
        self.hydrate_files(
            s3,
            &logical_paths,
            self.scoring_hydrate_concurrency,
            request,
        );
        // Bytes counted post-fetch from local metadata (sizes weren't known
        // up front); already-local files were filtered out above so this
        // counts only what this call actually moved.
        let mut files = 0usize;
        let mut bytes = 0u64;
        for (path, _) in &logical_paths {
            if let Ok(m) = std::fs::metadata(self.data_dir.join(path)) {
                files += 1;
                bytes += m.len();
            }
        }
        (files, bytes)
    }

    /// Fetch a batch of `(logical_path, projected_size_bytes)` pairs into
    /// the local cache, using cross-replica lease coordination when it's
    /// configured (`hydration_leases`) so a segment cold across the whole
    /// fleet costs one S3 GET total instead of one per replica — see the
    /// `kosha_control::hydration_lease` module doc for the protocol.
    ///
    /// Falls back to fetching every path from S3 directly whenever
    /// coordination isn't configured (no Postgres / no `POD_IP`), a claim
    /// attempt errors, or a peer fetch doesn't pan out — this never blocks a
    /// caller on the coordination layer itself being unavailable.
    ///
    /// Every path in the batch is pinned in the disk cache for the whole
    /// call — otherwise a later file's write in the *same* batch can evict
    /// an earlier file's write once the working set gets close to
    /// `KOSHA_CACHE_MAX_BYTES`, which makes hydration spin without ever
    /// converging (observed in production as an incomplete-segment count
    /// climbing 37→128→193 instead of shrinking). Unpinned unconditionally
    /// afterward regardless of outcome.
    #[cfg(feature = "s3")]
    fn hydrate_files(
        &self,
        s3: &s3_storage::S3Storage,
        files: &[(String, u64)],
        concurrency: usize,
        request: &Arc<RequestHydration>,
    ) {
        if files.is_empty() {
            return;
        }
        for (path, _) in files {
            self.cache.pin(path);
        }
        #[cfg(feature = "postgres")]
        {
            if let (Some(leases), Some(own_addr)) = (&self.hydration_leases, &self.own_addr) {
                self.hydrate_files_coordinated(s3, files, leases, own_addr, concurrency, request);
                for (path, _) in files {
                    self.cache.unpin(path);
                }
                return;
            }
        }
        self.hydrate_from_s3_budgeted(s3, files, concurrency, request);
        for (path, _) in files {
            self.cache.unpin(path);
        }
    }

    /// Cross-replica half of `hydrate_files`: claim each path via
    /// `hydration_leases`, fetch owned paths from S3 as one batch (same
    /// concurrency profile as before this feature existed), and fetch
    /// paths another replica already owns from that replica directly over
    /// HTTP instead of touching S3 — the actual request-volume win.
    #[cfg(all(feature = "s3", feature = "postgres"))]
    fn hydrate_files_coordinated(
        &self,
        s3: &s3_storage::S3Storage,
        files: &[(String, u64)],
        leases: &kosha_control::HydrationLeaseStore,
        own_addr: &str,
        concurrency: usize,
        request: &Arc<RequestHydration>,
    ) {
        // Comfortably longer than a single-file S3 GET + local write should
        // ever take, so a lease only outlives its owner when that owner has
        // actually stalled or crashed — see `fetch_from_peer`'s own budget
        // for how long a waiter tolerates that before giving up on the peer.
        const LEASE_TTL: Duration = Duration::from_secs(30);

        let mut fetch_from_s3: Vec<(String, u64)> = Vec::new();
        let mut owned_leases: Vec<String> = Vec::new();

        for (path, size) in files {
            match leases.try_claim(path, own_addr, LEASE_TTL) {
                kosha_control::HydrationLease::Owner => {
                    owned_leases.push(path.clone());
                    fetch_from_s3.push((path.clone(), *size));
                }
                kosha_control::HydrationLease::OwnedBy(peer_addr) => {
                    match self.fetch_from_peer(&peer_addr, path) {
                        Ok(bytes) => {
                            // Peer-fetched bytes are hydration too — charge
                            // them so a request can't route around its
                            // ceiling just because another replica happened
                            // to own the lease. (`fetch_from_peer` buffers
                            // the file whole; making *that* stream needs a
                            // raw-bytes response path on
                            // `GET /internal/segment`, tracked separately.)
                            request.charge(bytes.len() as u64);
                            if let Err(e) = self.cache.put_bytes(path, &bytes) {
                                eprintln!(
                                    "WARN: failed to persist peer-fetched {path} from \
                                     {peer_addr}: {e}"
                                );
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "WARN: peer fetch of {path} from {peer_addr} failed ({e}); \
                                 falling back to S3"
                            );
                            fetch_from_s3.push((path.clone(), *size));
                        }
                    }
                }
            }
        }

        // Owned (plus peer-fallback) paths go straight to S3, through the
        // same byte-budgeted chunking as uncoordinated hydration.
        self.hydrate_from_s3_budgeted(s3, &fetch_from_s3, concurrency, request);

        // Release only after our own fetch attempt is done (success or
        // failure) — releasing earlier could let a waiter's peer-fetch land
        // on us before the bytes actually exist locally.
        for path in &owned_leases {
            leases.release(path, own_addr);
        }
    }

    /// Fetch one already-hydrated segment file from a peer replica's
    /// `GET /internal/segment/<path>` instead of S3 — used when
    /// `hydration_lease` reports another replica already owns the fetch.
    ///
    /// Polls with a short bounded backoff rather than a single shot: we
    /// lost the cross-replica race, so the owner may still be mid-download
    /// when we ask. Gives up well before a caller would consider the whole
    /// request stuck — any error here is meant to be handled by falling
    /// back to fetching straight from S3, not by failing the request.
    #[cfg(all(feature = "s3", feature = "postgres"))]
    fn fetch_from_peer(&self, peer_addr: &str, logical_path: &str) -> Result<Vec<u8>, KoshaError> {
        let Some(ref client) = self.peer_http_client else {
            return Err(KoshaError::NotFound(
                "no peer HTTP client configured".into(),
            ));
        };

        let url = format!("http://{peer_addr}/internal/segment/{logical_path}");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut delay = Duration::from_millis(100);
        loop {
            let mut req = client.get(&url);
            if let Some(ref key) = self.internal_auth_key {
                req = req.bearer_auth(key);
            }
            match req.send() {
                Ok(resp) if resp.status().as_u16() == 200 => {
                    let encoded = resp.text().map_err(|e| {
                        KoshaError::NotFound(format!("peer response read failed: {e}"))
                    })?;
                    use base64::Engine;
                    return base64::engine::general_purpose::STANDARD
                        .decode(encoded.trim())
                        .map_err(|e| {
                            KoshaError::NotFound(format!("peer response decode failed: {e}"))
                        });
                }
                Ok(resp) if resp.status().as_u16() == 404 => {
                    if std::time::Instant::now() >= deadline {
                        return Err(KoshaError::NotFound(format!(
                            "peer {peer_addr} never produced {logical_path} within budget"
                        )));
                    }
                    std::thread::sleep(delay);
                    delay = (delay * 2).min(Duration::from_millis(500));
                }
                Ok(resp) => {
                    return Err(KoshaError::NotFound(format!(
                        "peer {peer_addr} returned {}",
                        resp.status()
                    )));
                }
                Err(e) => {
                    return Err(KoshaError::NotFound(format!(
                        "peer {peer_addr} unreachable: {e}"
                    )));
                }
            }
        }
    }

    /// Hydrate only the **scoring set** for segments that might match the
    /// query (footer filter + term bloom prune first), skipping the bulk
    /// `doc_store.bin` for Lazy segments. Returns the relative paths of
    /// segments still incomplete after hydration was attempted — see
    /// [`AppState::segment_is_scoring_complete`]. An empty result means
    /// every segment the search needs for *scoring* is present; a non-empty
    /// one means the caller must not proceed (search would be incomplete).
    ///
    /// `doc_store.bin` is fetched later, on demand, for just the segments
    /// holding the materialize page — see
    /// [`kosha_query::Searcher::search_with_doc_store_hydrator`].
    ///
    /// Returns a [`HydrationOutcome`]: the missing list above plus how many
    /// files/bytes this request actually pulled from S3.
    #[cfg(feature = "s3")]
    fn hydrate_segments_for_search(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
        query: &SearchQuery,
        request: &Arc<RequestHydration>,
    ) -> HydrationOutcome {
        let term_prune = kosha_query::term_bloom_prune_mode(query);
        // The query is analyzed per segment with the chains its footer
        // records (#220), so which terms (and posting-blob shards) a
        // segment needs depends on its footer.
        let query_analysis = kosha_query::QueryAnalysis::new(query);
        let needs_bloom_check = query.has_prunable_filter() || term_prune.is_some();
        // Single-vector kNN reads its column's index; late interaction
        // (#205) reads the field's multivec files instead (below).
        let knn_field = query
            .knn
            .as_ref()
            .filter(|knn| !knn.is_late_interaction())
            .map(|knn| knn.field.as_str());
        let filter_fields = query_filter_fields(query);
        let all_seg_paths: Vec<PathBuf> = manifest
            .segments
            .iter()
            .map(|entry| self.data_dir.join(&ns.0).join(&entry.segment_id.0))
            .collect();

        let missing_manifest_footer_paths: Vec<PathBuf> = manifest
            .segments
            .iter()
            .filter(|entry| manifest.segment_footer(&entry.segment_id).is_none())
            .map(|entry| self.data_dir.join(&ns.0).join(&entry.segment_id.0))
            .collect();

        // A footer rewritten after commit (bloom rebuild) is recorded by CRC
        // in the manifest. A local copy that doesn't match is the old one:
        // remove it so the hydration below fetches the current file.
        for entry in &manifest.segments {
            let Some(&expected) = manifest.footer_crc32.get(&entry.segment_id) else {
                continue;
            };
            if manifest.segment_footer(&entry.segment_id).is_some() {
                continue;
            }
            let path = self
                .data_dir
                .join(&ns.0)
                .join(&entry.segment_id.0)
                .join("footer.json");
            if let Ok(bytes) = std::fs::read(&path) {
                if crc32fast::hash(&bytes) != expected {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }

        // New manifests carry the search footer metadata inline, so cold
        // search can skip the footer-first S3 phase entirely. Legacy
        // manifests fall back to hydrating footer.json for pruning and
        // format detection.
        let (footer_files, footer_bytes) =
            self.ensure_files_local(&missing_manifest_footer_paths, "footer.json", request);

        // Persisted manifests no longer carry footers (#241), so the
        // fallback above is now the normal path for segments this pod has
        // not seen yet. Parse each such footer once and cache it on the
        // in-memory manifest: later queries (and later manifest versions,
        // via `restore_manifest`'s carry-over) skip both the S3 fetch and
        // the local parse for it.
        let fetched_footers: HashMap<kosha_core::SegmentId, Arc<kosha_core::Footer>> = manifest
            .segments
            .iter()
            .filter(|entry| manifest.segment_footer(&entry.segment_id).is_none())
            .filter_map(|entry| {
                let seg_path = self.data_dir.join(&ns.0).join(&entry.segment_id.0);
                let bytes = std::fs::read(seg_path.join("footer.json")).ok()?;
                // Still not the recorded rewrite (e.g. S3 fetch failed and
                // left nothing usable): don't cache or prune with it.
                if manifest
                    .footer_crc32
                    .get(&entry.segment_id)
                    .is_some_and(|&expected| crc32fast::hash(&bytes) != expected)
                {
                    return None;
                }
                let footer: kosha_core::Footer = serde_json::from_slice(&bytes).ok()?;
                // A footer.json from a different segment id (a corrupt or
                // misplaced file) must not be cached under this id.
                (footer.segment_id == entry.segment_id)
                    .then(|| (entry.segment_id.clone(), Arc::new(footer)))
            })
            .collect();
        if !fetched_footers.is_empty() {
            self.indexer
                .remember_segment_footers(ns, fetched_footers.values().cloned());
        }

        let segment_footer = |entry: &kosha_core::ManifestEntry| {
            manifest
                .segment_footer(&entry.segment_id)
                .or_else(|| fetched_footers.get(&entry.segment_id).map(Arc::as_ref))
        };
        // Path -> the segment's footer, borrowed: the manifest's cached copy,
        // else the one just parsed from local footer.json. Having one means
        // footer.json need not be (re)fetched or re-parsed. It does NOT mean
        // the writer emitted a doc_store.offsets sidecar (#179/#184) - see
        // `segment_is_lazy_capable`.
        let manifest_footers: HashMap<PathBuf, &kosha_core::Footer> = manifest
            .segments
            .iter()
            .filter_map(|entry| {
                segment_footer(entry)
                    .map(|footer| (self.data_dir.join(&ns.0).join(&entry.segment_id.0), footer))
            })
            .collect();
        let analyses: Vec<Option<&kosha_core::SegmentAnalysis>> = manifest
            .segments
            .iter()
            .map(|entry| segment_footer(entry).and_then(|f| f.analysis.as_ref()))
            .collect();
        let posting_files =
            posting_blob_files_for_query(query, &query_analysis, analyses.iter().copied());
        let posting_terms =
            posting_terms_for_query(query, &query_analysis, analyses.iter().copied());
        let stats_terms = kosha_query::namespace_stats_terms(query, analyses.iter().copied())
            .filter(|t| !t.is_empty());
        let mut to_hydrate: Vec<PathBuf> = Vec::new();
        let mut stats_only: Vec<PathBuf> = Vec::new();
        for (entry, seg_path) in manifest.segments.iter().zip(all_seg_paths.iter().cloned()) {
            // Borrowed, never cloned: footers hold every bloom. A segment
            // with no cached footer was already read (once) into
            // `fetched_footers` above.
            let footer = segment_footer(entry);
            match segment_search_hydration(
                query,
                needs_bloom_check,
                term_prune.map(|mode| (&query_analysis, mode)),
                stats_terms.as_deref(),
                footer,
            ) {
                SegmentHydration::Scoring => to_hydrate.push(seg_path),
                SegmentHydration::StatsOnly => stats_only.push(seg_path),
                SegmentHydration::Skip => {}
            }
        }
        // The filter files each segment needs for this query (#244): its
        // whole `filters.bin`, or `filters.toc` plus only the columns the
        // query names, as its footer says. Footers are all known by now:
        // cached on the manifest or read from `footer.json` above.
        let mut filter_files: HashMap<PathBuf, Vec<String>> = HashMap::new();
        if let Some(fields) = &filter_fields {
            for entry in &manifest.segments {
                let seg_path = self.data_dir.join(&ns.0).join(&entry.segment_id.0);
                let footer = manifest
                    .segment_footer(&entry.segment_id)
                    .or_else(|| fetched_footers.get(&entry.segment_id).map(Arc::as_ref));
                let files = segment_filter_files(footer, &seg_path, fields);
                filter_files.insert(seg_path, files);
            }
        }
        let mut outcome = self.ensure_scoring_files_local(
            &to_hydrate,
            knn_field,
            &filter_files,
            &manifest_footers,
            request,
        );
        let postings_outcome = self.ensure_query_postings(
            ns,
            manifest,
            &to_hydrate,
            &posting_terms,
            &posting_files,
            request,
        );
        outcome.files_fetched += postings_outcome.files_fetched;
        outcome.bytes_fetched += postings_outcome.bytes_fetched;
        outcome.missing.extend(postings_outcome.missing);
        outcome.rounds += postings_outcome.rounds;
        outcome.postings = postings_outcome.postings;
        // Late interaction (#205) reads the queried field's bag store and
        // token index, which only some segments have (their footers say
        // which); single-vector kNN never fetches them.
        if let Some(knn) = query.knn.as_ref().filter(|k| k.is_late_interaction()) {
            let scoring: std::collections::HashSet<&PathBuf> = to_hydrate.iter().collect();
            let mut by_file: std::collections::BTreeMap<String, Vec<PathBuf>> = Default::default();
            for (entry, seg_path) in manifest.segments.iter().zip(&all_seg_paths) {
                if !scoring.contains(seg_path) {
                    continue;
                }
                let Some(i) = segment_footer(entry)
                    .and_then(|f| f.multi_vectors.iter().position(|m| m.name == knn.field))
                else {
                    continue;
                };
                for file in [
                    kosha_segment::multivec::bags_file(i),
                    kosha_segment::multivec::ivf_file(i),
                ] {
                    by_file.entry(file).or_default().push(seg_path.clone());
                }
            }
            for (file, paths) in by_file {
                let (files, bytes) = self.ensure_files_local(&paths, &file, request);
                outcome.files_fetched += files;
                outcome.bytes_fetched += bytes;
                // Still missing: the search must not run (and the verdict
                // must not be cached) without them.
                outcome.missing.extend(
                    paths
                        .iter()
                        .filter(|p| !file_present_nonempty(&p.join(&file)))
                        .filter_map(|p| p.strip_prefix(&self.data_dir).ok())
                        .map(|p| p.to_string_lossy().into_owned()),
                );
            }
        }
        if !stats_only.is_empty() {
            // Best effort: whatever is still missing afterwards doesn't fail
            // the search. The searcher then can't read that segment's `df`
            // and scores the whole search with per-segment statistics
            // (`bm25_stats: segment_fallback`) — the pre-#246 behavior —
            // rather than with a `df` that leaves the segment out. The
            // terms' postings come the same way as for scored segments:
            // ranged spans where the segment allows (#245).
            let stats_files = self.ensure_scoring_files_local(
                &stats_only,
                None,
                &HashMap::new(),
                &manifest_footers,
                request,
            );
            let stats_postings = self.ensure_query_postings(
                ns,
                manifest,
                &stats_only,
                &posting_terms,
                &posting_files,
                request,
            );
            outcome.files_fetched += stats_files.files_fetched + stats_postings.files_fetched;
            outcome.bytes_fetched += stats_files.bytes_fetched + stats_postings.bytes_fetched;
        }
        outcome.files_fetched += footer_files;
        outcome.bytes_fetched += footer_bytes;
        if footer_files > 0 {
            outcome.rounds += 1;
        }
        // `to_hydrate` only covers every manifest segment when the bloom
        // check above was skipped entirely (an unfiltered query) — a
        // filtered/term-pruned query's `to_hydrate` is a proper subset,
        // chosen by *this* query's own filter value. A caller must not
        // treat this outcome as proof the whole namespace is hydrated; see
        // `HydrationOutcome::bloom_pruned`.
        outcome.bloom_pruned = needs_bloom_check;
        // A shed request's `missing` list is real (those files genuinely
        // aren't local), but the caller must not report it as a hydration
        // *failure* — nothing failed, the request declined to pull that
        // much. See `HydrationOutcome::shed`.
        outcome.shed = request.is_shed();
        outcome
    }

    /// The files a segment needs to be *scorable* — strictly less than the
    /// full segment. `doc_store.bin` is **not** required when the
    /// `doc_store.offsets` sidecar is present (Lazy open: scoring reads only
    /// the small offsets sidecar, never the bulk doc store). Legacy segments
    /// without offsets fall back to eager open, which does need
    /// `doc_store.bin`. Vector files are required only for kNN queries, and
    /// then only those of the column `knn_field` names (#204, see
    /// [`kosha_segment::knn_vector_files`]).
    ///
    /// Contrast [`AppState::REQUIRED_SEGMENT_FILES`] / `segment_is_complete`,
    /// which is the *full* set used by the compaction/merge path (those read
    /// every document and must have `doc_store.bin`).
    ///
    /// `manifest_footer` is the manifest's cached footer for the segment, if
    /// it has one: with it, `footer.json` need not be local.
    ///
    /// Doc-store access is satisfied by `doc_store.bin`, or by a
    /// `doc_store.offsets` whose header agrees with the footer the reader
    /// will use. A sidecar the reader would reject is not enough on its own:
    /// the reader then falls back to `doc_store.bin`, and if that was never
    /// hydrated the search fails with "No such file or directory" (#184).
    /// Counting such a segment incomplete sends it back through
    /// `ensure_scoring_files_local`, which fetches the bulk file.
    #[cfg(all(test, feature = "s3"))]
    fn segment_is_scoring_complete(
        seg_path: &Path,
        knn_field: Option<&str>,
        needs_filters: bool,
        manifest_footer: Option<&kosha_core::Footer>,
    ) -> bool {
        let filter_files: Vec<String> = if needs_filters {
            vec![kosha_core::FILTERS_BIN_FILE.to_string()]
        } else {
            Vec::new()
        };
        Self::scoring_complete(seg_path, knn_field, false, &filter_files, manifest_footer)
    }

    /// [`Self::segment_is_scoring_complete`] with `vectors_ranged`: kNN
    /// postings are read by range (#245), so a clustered segment needs only
    /// its `vector.offsets` locally, not the whole `vector.idx`.
    #[cfg(feature = "s3")]
    ///
    /// `filter_files` is what this query needs of the segment's filter
    /// index (#244): empty for a query that reads no filter columns,
    /// `filters.bin` for a legacy segment, `filters.toc` plus the query's
    /// columns for a columnar one.
    fn scoring_complete(
        seg_path: &Path,
        knn_field: Option<&str>,
        vectors_ranged: bool,
        filter_files: &[String],
        manifest_footer: Option<&kosha_core::Footer>,
    ) -> bool {
        if manifest_footer.is_none() && !file_present_nonempty(&seg_path.join("footer.json")) {
            return false;
        }
        if !file_present_nonempty(&seg_path.join("inverted.idx")) {
            return false;
        }
        if !filter_files
            .iter()
            .all(|f| file_present_nonempty(&seg_path.join(f)))
        {
            return false;
        }
        let doc_access_ok = file_present_nonempty(&seg_path.join("doc_store.bin"))
            || (file_present_nonempty(&seg_path.join("doc_store.offsets"))
                && kosha_segment::doc_store_offsets_plausible(seg_path, manifest_footer));
        if !doc_access_ok {
            return false;
        }
        // `vector.offsets` is required only where it exists: legacy flat
        // segments never have one. A clustered segment without it is served
        // by exact scan of every vector (seconds on a large segment), so it
        // is not complete until the sidecar has been fetched.
        if let Some(field) = knn_field {
            let local_footer;
            let footer = match manifest_footer {
                Some(footer) => Some(footer),
                None => {
                    local_footer = SegmentReader::read_footer(seg_path).ok();
                    local_footer.as_ref()
                }
            };
            if !knn_files_complete(seg_path, footer, field, vectors_ranged) {
                return false;
            }
        }
        true
    }

    /// Scoring-set-only counterpart of [`AppState::ensure_segments_local`]:
    /// fetch exactly the files scoring needs for each segment — no
    /// `s3.list()` round trip (file names are known), and no bulk
    /// `doc_store.bin` for Lazy segments. Single-flight coalescing reuses
    /// the same `in_flight_segments` map so concurrent cold searches on the
    /// same namespace share one fetch, and the same `hydration_semaphore`
    /// bounds server-wide concurrent hydration operations.
    ///
    /// A compaction/merge caller that is a *waiter* behind a search owner
    /// here only sees scoring-set files fetched; it then reports those
    /// segments as not-yet-complete via its own full-segment predicate and
    /// the admin path surfaces per-segment errors (its existing tolerance
    /// for partial S3 fetches) rather than reading a partial corpus — same
    /// graceful-degradation contract those admin paths already have.
    #[cfg(feature = "s3")]
    fn ensure_scoring_files_local(
        &self,
        seg_paths: &[PathBuf],
        knn_field: Option<&str>,
        filter_files: &HashMap<PathBuf, Vec<String>>,
        manifest_footers: &HashMap<PathBuf, &kosha_core::Footer>,
        request: &Arc<RequestHydration>,
    ) -> HydrationOutcome {
        let Some(ref s3) = self.s3_storage else {
            return HydrationOutcome::default();
        };

        let vectors_ranged = self.posting_read_mode == doc_spans::ReadMode::Ranged;
        let filters_of = |p: &Path| filter_files.get(p).map(Vec::as_slice).unwrap_or(&[]);
        let mut outcome = HydrationOutcome::default();
        let (owned, waiting) = partition_for_hydration(&self.in_flight_segments, seg_paths, |p| {
            Self::scoring_complete(
                p,
                knn_field,
                vectors_ranged,
                filters_of(p),
                manifest_footers.get(p).copied(),
            )
        });

        if !owned.is_empty() {
            let _permit = self.hydration_semaphore.acquire();

            // Which vector files a kNN needs is read from each segment's
            // footer (#204), so a footer known neither from the manifest nor
            // locally must be fetched before the list below is built.
            if knn_field.is_some() {
                let without_footer: Vec<PathBuf> = owned
                    .iter()
                    .map(|(p, _)| p.clone())
                    .filter(|p| {
                        !manifest_footers.contains_key(p)
                            && !file_present_nonempty(&p.join("footer.json"))
                    })
                    .collect();
                if !without_footer.is_empty() {
                    let (files, bytes) =
                        self.ensure_files_local(&without_footer, "footer.json", request);
                    outcome.files_fetched += files;
                    outcome.bytes_fetched += bytes;
                }
            }

            // Sizes are unknown without a per-segment LIST round trip —
            // avoiding that round trip is half the point of this path (file
            // names are known a priori). 0 sizes mean `chunk_by_byte_budget`
            // cannot chunk this batch at all: every file below lands in one
            // chunk, fanned out `scoring_hydrate_concurrency` (64) wide.
            //
            // That used to be waved off as "the scoring set excludes the
            // bulk doc stores, so per-file sizes are small." It isn't:
            // `inverted.idx` grows with a segment's vocabulary, and one cold
            // search on staging pulled 177 files / 4,974 MB through here
            // (~28 MB average) in a single chunk. Volume on this path is
            // instead bounded by `hydration_budget`, from each response's
            // `Content-Length` — see the `hydrate_byte_budget` field docs.
            let mut logical_paths: Vec<(String, u64)> = Vec::new();
            // Segments sent to score with only their offsets sidecar. Each
            // is checked after the fetch below: a sidecar that is missing
            // from S3 or that the reader would reject falls back to
            // `doc_store.bin` (#179, #184).
            let mut lazy_segments: Vec<&PathBuf> = Vec::new();
            for (seg_path, _) in &owned {
                let Ok(rel_path) = seg_path.strip_prefix(&self.data_dir) else {
                    continue;
                };
                let s3_prefix = rel_path.to_string_lossy().into_owned();

                // `footer.json` is needed only for legacy manifests that do
                // not carry a footer snapshot.
                if !manifest_footers.contains_key(seg_path)
                    && !file_present_nonempty(&seg_path.join("footer.json"))
                {
                    logical_paths.push((format!("{s3_prefix}/footer.json"), 0));
                }
                if !file_present_nonempty(&seg_path.join("inverted.idx")) {
                    logical_paths.push((format!("{s3_prefix}/inverted.idx"), 0));
                }
                // Only the filter files this query reads: a columnar
                // segment's TOC plus the columns the query names (#244),
                // in the same round as the other scoring files.
                for file in filters_of(seg_path) {
                    if !file_present_nonempty(&seg_path.join(file)) {
                        logical_paths.push((format!("{s3_prefix}/{file}"), 0));
                    }
                }

                // doc_store: Lazy segments need only the offsets sidecar;
                // legacy (format_version 0) segments need the full
                // doc_store.bin because eager open parses it. See
                // `segment_is_lazy_capable` for how that is decided and
                // what it used to get wrong.
                let has_offsets = file_present_nonempty(&seg_path.join("doc_store.offsets"));
                let has_doc_store = file_present_nonempty(&seg_path.join("doc_store.bin"));
                let lazy_capable = has_offsets
                    || segment_is_lazy_capable(
                        manifest_footers.get(seg_path).map(|f| f.format_version),
                        || {
                            SegmentReader::read_footer(seg_path)
                                .ok()
                                .map(|ft| ft.format_version)
                        },
                    );
                if lazy_capable {
                    if !has_offsets {
                        logical_paths.push((format!("{s3_prefix}/doc_store.offsets"), 0));
                    }
                    if !has_doc_store {
                        lazy_segments.push(seg_path);
                    }
                } else if !has_doc_store {
                    logical_paths.push((format!("{s3_prefix}/doc_store.bin"), 0));
                }

                // Only the queried column's files (#204), as the segment's
                // footer lists them. Ranged mode (#245) fetches a clustered
                // column's offsets sidecar only: its probed postings are
                // read by range during the search, and the idx file is
                // fetched whole only for a column that turns out to have no
                // sidecar (below). A legacy segment's `vector.offsets`
                // exists only for the clustered format, and there is no
                // footer signal to predict that (vector.idx is
                // deliberately self-describing), so it is always attempted:
                // a segment that doesn't have one just shows up as a
                // per-file miss in `s3.read_many`'s accounting, not a
                // hydration failure. Without the sidecar the lazy path
                // never activates for S3-backed segments, and every kNN
                // query silently takes the (still correct, just slower)
                // eager fallback.
                if let Some(field) = knn_field {
                    let local_footer;
                    let footer = match manifest_footers.get(seg_path) {
                        Some(footer) => Some(*footer),
                        None => {
                            local_footer = SegmentReader::read_footer(seg_path).ok();
                            local_footer.as_ref()
                        }
                    };
                    for column in kosha_segment::knn_vector_columns(footer, field) {
                        let mut files: Vec<&str> = Vec::new();
                        if let Some(offsets) = &column.offsets_file {
                            files.push(offsets);
                        }
                        if !vectors_ranged || column.offsets_file.is_none() {
                            files.push(&column.idx_file);
                        }
                        for file in files {
                            if !file_present_nonempty(&seg_path.join(file)) {
                                logical_paths.push((format!("{s3_prefix}/{file}"), 0));
                            }
                        }
                    }
                    if kosha_segment::knn_needs_doc_vectors(footer)
                        && !file_present_nonempty(&seg_path.join(kosha_segment::DOC_VECTORS_FILE))
                    {
                        logical_paths.push((
                            format!("{s3_prefix}/{}", kosha_segment::DOC_VECTORS_FILE),
                            0,
                        ));
                    }
                }
            }

            if !logical_paths.is_empty() {
                println!(
                    "cache miss: hydrating {} scoring file(s) across {} segment(s) \
                     (fan-out={}); doc stores deferred to page materialize for Lazy segments",
                    logical_paths.len(),
                    owned.len(),
                    self.scoring_hydrate_concurrency
                );
                self.hydrate_files(
                    s3,
                    &logical_paths,
                    self.scoring_hydrate_concurrency,
                    request,
                );
                outcome.rounds += 1;
                // Bytes counted post-fetch (sizes weren't known up front);
                // every path here was absent before the fetch, so this
                // counts only what this call actually moved.
                for (path, _) in &logical_paths {
                    if let Ok(m) = std::fs::metadata(self.data_dir.join(path)) {
                        outcome.files_fetched += 1;
                        outcome.bytes_fetched += m.len();
                    }
                }
            }

            // Ranged mode: a segment with no `vector.offsets` is a legacy
            // flat (or unclustered) one, searched by exact scan of the whole
            // `vector.idx`. One more round, for those segments only.
            if let (Some(field), true) = (knn_field, vectors_ranged) {
                let mut legacy: Vec<(String, u64)> = Vec::new();
                for (p, _) in &owned {
                    let Ok(rel) = p.strip_prefix(&self.data_dir) else {
                        continue;
                    };
                    let local_footer;
                    let footer = match manifest_footers.get(p) {
                        Some(footer) => Some(*footer),
                        None => {
                            local_footer = SegmentReader::read_footer(p).ok();
                            local_footer.as_ref()
                        }
                    };
                    for column in kosha_segment::knn_vector_columns(footer, field) {
                        let Some(offsets) = &column.offsets_file else {
                            continue;
                        };
                        if !file_present_nonempty(&p.join(offsets))
                            && !file_present_nonempty(&p.join(&column.idx_file))
                        {
                            legacy.push((
                                format!("{}/{}", rel.to_string_lossy(), column.idx_file),
                                0,
                            ));
                        }
                    }
                }
                if !legacy.is_empty() {
                    self.hydrate_files(s3, &legacy, self.scoring_hydrate_concurrency, request);
                    outcome.rounds += 1;
                    for (path, _) in &legacy {
                        if let Ok(m) = std::fs::metadata(self.data_dir.join(path)) {
                            outcome.files_fetched += 1;
                            outcome.bytes_fetched += m.len();
                        }
                    }
                }
            }

            // Doc-store fallback (#179, #184). A Lazy segment goes to
            // scoring with only `doc_store.offsets`; `doc_store.bin` is
            // deferred to page materialize. That is only safe if the reader
            // will actually use the sidecar. Two ways it won't:
            //
            // * The sidecar is not in S3 (a partial upload, or a legacy
            //   segment whose manifest footer claimed otherwise), or the GET
            //   failed. The segment would reach scoring with neither
            //   doc-store file, the one state the reader can't open, and the
            //   whole search used to 503 with "segment hydration failed".
            // * The sidecar is there but fails the reader's sanity check
            //   (doc-count mismatch, truncation). The reader falls back to
            //   parsing `doc_store.bin`, which was never fetched, and the
            //   search used to 500 with "No such file or directory".
            //
            // Either way, fetch `doc_store.bin` for that segment and serve
            // the search. It is the expensive path, and it self-heals: once
            // the bulk file is local, later queries are served from disk.
            // `footer.json` comes along for a rejected sidecar so the reader
            // can check the manifest's cached footer against the segment's
            // own (see `kosha_segment::doc_store_opens_lazily`).
            let mut fallback_paths: Vec<(String, u64)> = Vec::new();
            // A shed request skipped its fetches on purpose; its sidecars
            // are absent because nothing asked for them, and it must not
            // go on to pull bulk doc stores either.
            let lazy_segments = if request.is_shed() {
                Vec::new()
            } else {
                lazy_segments
            };
            for seg_path in lazy_segments {
                let manifest_footer = manifest_footers.get(seg_path).copied();
                let Some(reason) = doc_store_fallback_reason(seg_path, manifest_footer) else {
                    continue;
                };
                let Ok(rel_path) = seg_path.strip_prefix(&self.data_dir) else {
                    continue;
                };
                let s3_prefix = rel_path.to_string_lossy().into_owned();
                eprintln!(
                    "WARN: segment {s3_prefix}: {}; falling back to the eager doc_store.bin path \
                     for this search. Repair the sidecar with \
                     POST /v1/admin/backfill-offset-tables, otherwise every cold query on this \
                     segment pays for the full doc store.",
                    reason.describe()
                );
                self.doc_store_fallbacks
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                fallback_paths.push((format!("{s3_prefix}/doc_store.bin"), 0));
                if reason == DocStoreFallbackReason::Rejected
                    && !file_present_nonempty(&seg_path.join("footer.json"))
                {
                    fallback_paths.push((format!("{s3_prefix}/footer.json"), 0));
                }
            }
            if !fallback_paths.is_empty() {
                self.hydrate_files(
                    s3,
                    &fallback_paths,
                    self.scoring_hydrate_concurrency,
                    request,
                );
                for (path, _) in &fallback_paths {
                    if let Ok(m) = std::fs::metadata(self.data_dir.join(path)) {
                        outcome.files_fetched += 1;
                        outcome.bytes_fetched += m.len();
                    }
                }
            }

            drop(_permit);
            complete_owned(&self.in_flight_segments, &owned);
        }

        wait_for(&waiting);

        outcome.missing = seg_paths
            .iter()
            .filter(|p| {
                !Self::scoring_complete(
                    p,
                    knn_field,
                    vectors_ranged,
                    filters_of(p),
                    manifest_footers.get(*p).copied(),
                )
            })
            .filter_map(|p| p.strip_prefix(&self.data_dir).ok())
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        outcome
    }

    /// Per-query posting-blob ensure for the cached-verdict fast path: the
    /// namespace-level verdict can never cover posting blobs, because they
    /// are fetched per query term (each term hashes to one of the
    /// [`kosha_segment::POSTING_BLOB_SHARDS`] shard files). Sub-ms when the
    /// shards are already local and the presence cache has already seen
    /// them this manifest version — see `posting_blob_presence`.
    #[cfg(feature = "s3")]
    fn ensure_query_posting_blobs(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
        query: &SearchQuery,
        request: &Arc<RequestHydration>,
    ) -> HydrationOutcome {
        // Footers are local whenever the namespace verdict is cached (it
        // means every segment's scoring set is present).
        let analyses: Vec<Option<kosha_core::SegmentAnalysis>> = manifest
            .segments
            .iter()
            .map(|entry| {
                let dir = self.data_dir.join(&ns.0).join(&entry.segment_id.0);
                manifest
                    .footer_or_read(&entry.segment_id, || {
                        kosha_segment::SegmentReader::read_footer(&dir).ok()
                    })
                    .and_then(|footer| footer.analysis.clone())
            })
            .collect();
        let query_analysis = kosha_query::QueryAnalysis::new(query);
        let posting_files = posting_blob_files_for_query(
            query,
            &query_analysis,
            analyses.iter().map(Option::as_ref),
        );
        let posting_terms =
            posting_terms_for_query(query, &query_analysis, analyses.iter().map(Option::as_ref));
        if posting_files.is_empty() {
            return HydrationOutcome::default();
        }
        let seg_paths: Vec<PathBuf> = manifest
            .segments
            .iter()
            .map(|entry| self.data_dir.join(&ns.0).join(&entry.segment_id.0))
            .collect();
        self.ensure_query_postings(
            ns,
            manifest,
            &seg_paths,
            &posting_terms,
            &posting_files,
            request,
        )
    }

    /// Make the query terms' postings local in `seg_paths` (#245).
    ///
    /// Raw-shard segments (`Footer::posting_blobs_raw`) in `ranged` mode get
    /// just the terms' spans, one batch of ranged GETs for all of them
    /// (see [`posting_spans`]); a shard whose range fails is fetched whole.
    /// Every other segment — older or compressed-shard segments, wildcard
    /// queries, `KOSHA_POSTINGS_READ_MODE=whole` — gets whole shards via
    /// [`Self::ensure_posting_blobs_local`], exactly as before.
    #[cfg(feature = "s3")]
    fn ensure_query_postings(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
        seg_paths: &[PathBuf],
        terms: &[String],
        posting_files: &[String],
        request: &Arc<RequestHydration>,
    ) -> HydrationOutcome {
        let Some(ref s3) = self.s3_storage else {
            return HydrationOutcome::default();
        };
        if posting_files.is_empty() {
            return HydrationOutcome::default();
        }
        // Empty `terms` (wildcard or fuzzy: the terms are only known once
        // each segment's dictionary is expanded) means whole shards.
        let ranged_mode =
            self.posting_read_mode == doc_spans::ReadMode::Ranged && !terms.is_empty();
        if !ranged_mode {
            let mut o = self.ensure_posting_blobs_local(
                ns,
                manifest.version,
                seg_paths,
                posting_files,
                request,
            );
            o.postings = whole_posting_stats(&o);
            o.rounds = o.postings.rounds;
            self.posting_reads.record(&o.postings);
            return o;
        }

        // Presence cache first: a warm repeat confirms every term of every
        // segment here without opening a footer or a TOC.
        let eviction_gen = self.cache.eviction_generation();
        let cached = {
            let cache = self.posting_blob_presence.lock().unwrap();
            posting_blob_cache_lookup(&cache, &ns.0, manifest.version, eviction_gen)
        };
        let keys: Vec<String> = terms
            .iter()
            .map(|t| posting_spans::presence_key(t))
            .collect();
        let ids: HashMap<&Path, &kosha_core::SegmentId> = manifest
            .segments
            .iter()
            .map(|e| (Path::new(&e.segment_id.0), &e.segment_id))
            .collect();
        let mut ranged: Vec<PathBuf> = Vec::new();
        let mut whole: Vec<PathBuf> = Vec::new();
        for seg in seg_paths {
            let Ok(rel) = seg.strip_prefix(&self.data_dir) else {
                continue;
            };
            let rel = rel.to_string_lossy();
            // Every term confirmed (ranged segment), or every shard
            // confirmed (whole-shard segment): nothing to do, and no footer
            // read to find out which kind it is.
            if cached.get(rel.as_ref()).is_some_and(|set| {
                keys.iter().all(|k| set.contains(k))
                    || posting_files.iter().all(|f| set.contains(f))
            }) {
                continue;
            }
            let raw = seg
                .file_name()
                .and_then(|name| ids.get(Path::new(name)))
                .and_then(|id| {
                    manifest
                        .footer_or_read(id, || SegmentReader::read_footer(seg).ok())
                        .map(|f| f.posting_blobs_raw)
                })
                .unwrap_or(false);
            // Only split segments have shards; `posting_term_spans` needs
            // the local TOC, fetched by the scaffolding round.
            if raw && kosha_segment::inverted_uses_posting_blobs(seg) {
                ranged.push(seg.clone());
            } else {
                whole.push(seg.clone());
            }
        }

        let mut outcome = HydrationOutcome::default();
        let mut stats = posting_spans::PostingFetchStats::default();
        if !ranged.is_empty() {
            let fetcher = doc_spans::S3RangeFetcher {
                s3,
                concurrency: self.scoring_hydrate_concurrency,
                budget: &self.hydration_budget,
                request,
            };
            let o = {
                let _permit = self.hydration_semaphore.acquire();
                self.posting_span_hydrator
                    .hydrate(&ranged, terms, &fetcher, &self.cache)
            };
            stats.rounds += o.rounds;
            stats.ranged_gets += o.gets;
            stats.ranged_bytes += o.bytes;
            outcome.bytes_fetched += o.bytes;
            outcome.missing.extend(o.missing);
            outcome.shed |= o.shed;
            for shard in o.promote {
                self.posting_reads.promotion_queued();
                self.posting_promotions.push(shard);
            }
            if !o.confirmed.is_empty() {
                let write_gen = self.cache.eviction_generation();
                let mut cache = self.posting_blob_presence.lock().unwrap();
                posting_blob_cache_insert(
                    &mut cache,
                    &ns.0,
                    manifest.version,
                    write_gen,
                    o.confirmed,
                );
            }
            // A failed range: fetch that shard whole, one more round.
            let mut by_seg: Vec<(PathBuf, Vec<String>)> = Vec::new();
            for (seg, file) in o.whole_fallback {
                match by_seg.last_mut() {
                    Some((last, files)) if *last == seg => files.push(file),
                    _ => by_seg.push((seg, vec![file])),
                }
            }
            let mut fallback_round = false;
            for (seg, files) in by_seg {
                let w = self.ensure_posting_blobs_local(
                    ns,
                    manifest.version,
                    std::slice::from_ref(&seg),
                    &files,
                    request,
                );
                fallback_round |= w.files_fetched > 0 || !w.missing.is_empty();
                stats.whole_files += w.files_fetched as u64;
                stats.whole_bytes += w.bytes_fetched;
                outcome.files_fetched += w.files_fetched;
                outcome.bytes_fetched += w.bytes_fetched;
                outcome.missing.extend(w.missing);
            }
            stats.rounds += u64::from(fallback_round);
        }
        if !whole.is_empty() {
            let w = self.ensure_posting_blobs_local(
                ns,
                manifest.version,
                &whole,
                posting_files,
                request,
            );
            let ws = whole_posting_stats(&w);
            stats.rounds += ws.rounds;
            stats.whole_files += ws.whole_files;
            stats.whole_bytes += ws.whole_bytes;
            outcome.files_fetched += w.files_fetched;
            outcome.bytes_fetched += w.bytes_fetched;
            outcome.missing.extend(w.missing);
        }
        self.posting_reads.record(&stats);
        outcome.rounds = stats.rounds;
        outcome.postings = stats;
        outcome.shed |= request.is_shed();
        outcome
    }

    /// The [`kosha_query::VectorSpanFetch`] the search handlers pass down:
    /// fetch the probed kNN postings' `vector.idx` ranges of one segment
    /// into the span cache (#245). A range that fails (not shed) falls back
    /// to the whole `vector.idx`, so the probe still reads every posting it
    /// needs; anything still missing marks the segment degraded.
    #[cfg(feature = "s3")]
    fn hydrate_vector_postings(
        &self,
        seg: &Path,
        file: &str,
        ranges: &[(u64, u64)],
        request: &Arc<RequestHydration>,
    ) -> posting_spans::PostingFetchStats {
        let mut stats = posting_spans::PostingFetchStats::default();
        let Some(ref s3) = self.s3_storage else {
            return stats;
        };
        let Some(rel) = seg
            .strip_prefix(&self.data_dir)
            .ok()
            .map(|p| p.to_string_lossy().replace('\\', "/"))
        else {
            return stats;
        };
        let fetcher = doc_spans::S3RangeFetcher {
            s3,
            concurrency: self.scoring_hydrate_concurrency,
            budget: &self.hydration_budget,
            request,
        };
        let o = posting_spans::fetch_file_spans(
            &rel,
            seg,
            file,
            ranges,
            self.posting_span_hydrator.coalesce_gap(),
            &fetcher,
            &self.cache,
        );
        stats.rounds = u64::from(o.gets > 0);
        stats.ranged_gets = o.gets;
        stats.ranged_bytes = o.bytes;
        if o.failed {
            let (files, bytes) = self.ensure_files_local(&[seg.to_path_buf()], file, request);
            stats.rounds += 1;
            stats.whole_files = files as u64;
            stats.whole_bytes = bytes;
        }
        self.posting_reads.record(&stats);
        stats
    }

    /// Start the thread that promotes hot posting shards from ranged reads
    /// to whole local files, then drops their spans (#245). Same shape as
    /// [`Self::spawn_doc_store_promoter`].
    fn spawn_posting_promoter(self: &Arc<Self>) {
        #[cfg(feature = "s3")]
        {
            if self.s3_storage.is_none() || self.posting_read_mode != doc_spans::ReadMode::Ranged {
                return;
            }
            let weak = Arc::downgrade(self);
            std::thread::Builder::new()
                .name("kosha-postings-promote".into())
                .spawn(move || loop {
                    let Some(state) = weak.upgrade() else { return };
                    if let Some(shard) =
                        state.posting_promotions.pop_timeout(Duration::from_secs(1))
                    {
                        state.promote_posting_shard(&shard);
                    }
                })
                .expect("failed to spawn posting promotion thread");
        }
    }

    /// Hydrate one whole posting shard for promotion and drop its spans
    /// once it is in place.
    #[cfg(feature = "s3")]
    fn promote_posting_shard(&self, shard: &Path) {
        let (Some(seg), Some(file)) = (
            shard.parent(),
            shard.file_name().map(|n| n.to_string_lossy().into_owned()),
        ) else {
            return;
        };
        let t = Instant::now();
        let (files, bytes) = {
            let _permit = self.hydration_semaphore.acquire();
            self.ensure_files_local(&[seg.to_path_buf()], &file, &RequestHydration::unlimited())
        };
        let landed = file_present_nonempty(shard);
        self.posting_span_hydrator
            .finish_promotion(&self.cache, shard, landed);
        if landed {
            self.posting_reads.promotion_done();
            self.posting_reads
                .record(&posting_spans::PostingFetchStats {
                    whole_files: files as u64,
                    whole_bytes: bytes,
                    ..Default::default()
                });
        }
        println!(
            "posting shard promote: shard={} landed={landed} fetched {files} file(s) \
             ({:.1} MB) in {:.0} ms",
            shard.display(),
            bytes as f64 / (1024.0 * 1024.0),
            t.elapsed().as_secs_f64() * 1e3,
        );
    }

    #[cfg(feature = "s3")]
    fn ensure_posting_blobs_local(
        &self,
        ns: &NamespaceId,
        manifest_version: u64,
        seg_paths: &[PathBuf],
        posting_files: &[String],
        request: &Arc<RequestHydration>,
    ) -> HydrationOutcome {
        let Some(ref s3) = self.s3_storage else {
            return HydrationOutcome::default();
        };
        if posting_files.is_empty() {
            return HydrationOutcome::default();
        }

        let _permit = self.hydration_semaphore.acquire();

        // Presence-cache read (Tier 1 O3): an `Arc` snapshot (refcount
        // bump, no clone) of whichever (segment, shard) pairs are already
        // confirmed present for this exact `(namespace, manifest_version,
        // eviction_generation)`. A stale/absent entry — different manifest
        // version, or an eviction happened anywhere since it was written —
        // yields an empty map, i.e. this degrades to the pre-cache full
        // stat sweep, never a false "present." See the field's doc comment
        // for why the eviction-generation check exists.
        let eviction_gen = self.cache.eviction_generation();
        let cached = {
            let cache = self.posting_blob_presence.lock().unwrap();
            posting_blob_cache_lookup(&cache, &ns.0, manifest_version, eviction_gen)
        };

        let (mut logical_paths, mut newly_confirmed) = plan_posting_blob_fetch(
            seg_paths,
            posting_files,
            &self.data_dir,
            &cached,
            kosha_segment::inverted_uses_posting_blobs,
            file_present_nonempty,
        );

        let mut outcome = HydrationOutcome::default();
        match prune_unwritten_posting_blobs(&self.data_dir, &mut logical_paths) {
            Ok(absent) => newly_confirmed.extend(absent),
            Err(error) => {
                outcome.missing.push(error.to_string());
                return outcome;
            }
        }
        if !logical_paths.is_empty() {
            println!(
                "cache miss: hydrating {} posting blob(s) across {} segment(s) (fan-out={})",
                logical_paths.len(),
                seg_paths.len(),
                self.scoring_hydrate_concurrency
            );
            self.hydrate_files(
                s3,
                &logical_paths,
                self.scoring_hydrate_concurrency,
                request,
            );
        }

        // Post-fetch verification only needs to re-stat what we actually
        // attempted to fetch — everything else was already resolved above
        // (via a cache hit or the pre-fetch stat in `plan_posting_blob_fetch`),
        // so re-checking the entire (segment × shard) cross-product a
        // second time, as the pre-cache code did, is redundant.
        for (rel, _) in &logical_paths {
            let path = self.data_dir.join(rel);
            match std::fs::metadata(&path) {
                Ok(m) if m.is_file() && m.len() > 0 => {
                    outcome.files_fetched += 1;
                    outcome.bytes_fetched += m.len();
                    if let Some((seg_rel, file_name)) = rel.rsplit_once('/') {
                        newly_confirmed.push((seg_rel.to_string(), file_name.to_string()));
                    }
                }
                _ => outcome.missing.push(rel.clone()),
            }
        }

        // ── A shard that was never written is not a missing file ──
        // The writer skips a posting shard no term hashed into, which is
        // common in small segments. A query term that hashes to such a
        // shard is simply absent from that segment, but the fetch above
        // asked S3 for the file anyway, got a 404, and the whole search
        // failed with a 503 "segment hydration failed".
        //
        // For each segment with a shard still missing, list what S3 holds
        // for it. A shard that is not in the listing was never written:
        // drop it from `missing` and remember it as resolved, so later
        // queries do not ask again. A shard that IS listed but did not
        // arrive is a real failure and stays in `missing`. So does
        // everything for a segment whose listing fails.
        //
        // Dropping a shard here cannot hide lost data: the scorer checks
        // every query term against the segment's term table and fails the
        // search if a term the table contains has no readable shard.
        if !outcome.missing.is_empty() {
            let mut by_segment: HashMap<String, Vec<String>> = HashMap::new();
            for rel in outcome.missing.drain(..) {
                match rel.rsplit_once('/') {
                    Some((seg_rel, file_name)) => by_segment
                        .entry(seg_rel.to_string())
                        .or_default()
                        .push(file_name.to_string()),
                    None => by_segment.entry(rel).or_default().push(String::new()),
                }
            }
            for (seg_rel, files) in by_segment {
                // S3 itself, not the local-fallback listing: only S3 can
                // say a file was never uploaded. An empty listing means the
                // whole segment is absent there, which is a failure, not
                // 256 shards that happen to be empty.
                let listed: Option<std::collections::HashSet<String>> = s3
                    .list_remote_with_sizes(&seg_rel)
                    .ok()
                    .filter(|entries| !entries.is_empty())
                    .map(|entries| entries.into_iter().map(|(name, _)| name).collect());
                for file_name in files {
                    let never_written = listed
                        .as_ref()
                        .is_some_and(|names| !file_name.is_empty() && !names.contains(&file_name));
                    if never_written {
                        newly_confirmed.push((seg_rel.clone(), file_name));
                    } else if file_name.is_empty() {
                        outcome.missing.push(seg_rel.clone());
                    } else {
                        outcome.missing.push(format!("{seg_rel}/{file_name}"));
                    }
                }
            }
        }

        // Fold every pair this call physically confirmed present — via
        // cache-miss stat or post-fetch verification — back into the
        // cache, one lock acquisition regardless of how many pairs were
        // checked. Re-reads the eviction generation: if an eviction
        // happened mid-call, entries are stamped with the *later* value,
        // which is still correct — every stamped pair was itself verified
        // by a real stat taken during this call, at or after that point.
        if !newly_confirmed.is_empty() {
            let write_gen = self.cache.eviction_generation();
            let mut cache = self.posting_blob_presence.lock().unwrap();
            posting_blob_cache_insert(
                &mut cache,
                &ns.0,
                manifest_version,
                write_gen,
                newly_confirmed,
            );
        }

        // Set here as well as in `hydrate_segments_for_search`: on a warm
        // namespace the cached hydration verdict routes a query straight to
        // this function, and its outcome is the only one the handler sees.
        outcome.shed = request.is_shed();
        outcome
    }

    /// Re-upload files that an admin backfill rewrote in place inside an
    /// already-uploaded segment (e.g. `footer.json`), then refresh that
    /// segment's `_COMMIT` so its recorded sizes/CRCs match the new bytes.
    /// Without the refresh, a rewritten file would fail the commit check
    /// (`S3Storage::segment_durability`) and hydration verification, even
    /// though it is valid. Legacy segments with no `_COMMIT` are left
    /// without one: a marker built from only the rewritten files would
    /// wrongly vouch for the rest of the segment.
    ///
    /// The marker is still written last, so a failure partway through
    /// leaves the old marker and fails the size/CRC check for the rewritten
    /// file. Reconciliation then re-uploads the segment.
    #[cfg(feature = "s3")]
    fn sync_files_to_s3(&self, seg_dir: &Path, file_names: &[&str]) -> Result<(), String> {
        let Some(ref s3) = self.s3_storage else {
            return Ok(());
        };
        let rel = seg_dir
            .strip_prefix(&self.data_dir)
            .map_err(|_| format!("{} is outside data_dir", seg_dir.display()))?
            .to_string_lossy()
            .into_owned();
        let commit = s3
            .read_segment_commit(&rel)
            .map_err(|e| format!("read {rel}/{}: {e}", segment_commit::COMMIT_FILE))?;
        let mut entries = Vec::new();
        for file_name in file_names {
            let path = seg_dir.join(file_name);
            if !path.is_file() {
                continue;
            }
            let s3_path = format!("{rel}/{file_name}");
            let data = std::fs::read(&path)
                .map_err(|e| format!("failed to read {s3_path} for upload: {e}"))?;
            s3.write(&s3_path, &data)
                .map_err(|e| format!("S3 upload failed for {s3_path}: {e}"))?;
            entries.push(segment_commit::SegmentCommit::entry(file_name, &data));
        }
        if let Some(mut commit) = commit {
            for e in entries {
                commit.upsert(e);
            }
            let commit_path = format!("{rel}/{}", segment_commit::COMMIT_FILE);
            s3.write(&commit_path, &commit.to_bytes())
                .map_err(|e| format!("S3 upload failed for {commit_path}: {e}"))?;
        }
        Ok(())
    }
}

/// Verify freshly fetched segment files (`logical_path` = `ns/segment/file`,
/// relative to `data_dir`) against the `_COMMIT` of their segment, when that
/// segment has one locally. Every file that fails is deleted from local disk
/// and returned. Files of segments without a local `_COMMIT` (legacy
/// segments, or a marker that wasn't fetched) and files the marker doesn't
/// list are accepted unchecked.
#[cfg(feature = "s3")]
fn verify_fetched_against_commit(data_dir: &Path, fetched: &[(String, u64)]) -> Vec<String> {
    let mut commits: HashMap<String, Option<segment_commit::SegmentCommit>> = HashMap::new();
    let mut bad = Vec::new();
    for (logical, _) in fetched {
        let Some((seg_rel, name)) = logical.rsplit_once('/') else {
            continue;
        };
        if name == segment_commit::COMMIT_FILE {
            continue;
        }
        let commit = commits.entry(seg_rel.to_string()).or_insert_with(|| {
            let path = data_dir.join(seg_rel).join(segment_commit::COMMIT_FILE);
            match std::fs::read(&path) {
                Ok(bytes) => match segment_commit::SegmentCommit::from_bytes(&bytes) {
                    Ok(c) => Some(c),
                    Err(e) => {
                        eprintln!("WARN: ignoring unreadable {}: {e}", path.display());
                        None
                    }
                },
                Err(_) => None,
            }
        });
        let Some(commit) = commit else {
            continue;
        };
        let path = data_dir.join(logical);
        // A file that didn't arrive at all is hydration's "missing" case,
        // reported separately. Here we only judge bytes that landed.
        let Ok(data) = std::fs::read(&path) else {
            continue;
        };
        if let Err(e) = commit.verify(name, &data) {
            eprintln!("ERROR: checksum mismatch in segment {seg_rel}: {e}");
            std::fs::remove_file(&path).ok();
            bad.push(logical.clone());
        }
    }
    bad
}

/// Parse a default-on boolean env toggle: unset or any other value → true;
/// `0`, `false`, or `off` (whitespace-trimmed) → false. Used for
/// `KOSHA_WARMUP_POSTING_BLOBS`.
fn env_flag_default_on(raw: Option<&str>) -> bool {
    raw.map(|v| !matches!(v.trim(), "0" | "false" | "off"))
        .unwrap_or(true)
}

/// Whether hydration should fetch a segment's `doc_store.offsets` sidecar
/// (Lazy open) or its full `doc_store.bin` (legacy eager open), for a
/// segment with no sidecar on local disk yet.
///
/// The sidecar arrived with `Footer::format_version` 1, so the version is
/// the signal. Compare against 1, not `SEGMENT_FORMAT_VERSION`: that
/// constant moves with every format change (already 2 for the lazy inverted
/// index) and would misclassify every v1 segment as legacy, re-fetching all
/// their doc stores.
///
/// `manifest_format_version` comes from the manifest's cached footer, when
/// it has one. It counts only when it is genuinely present, i.e. at least 1:
/// `format_version` is `#[serde(default)]`, so a footer that never carried
/// the field reads as 0, which here means "unknown", not a claim. Unknown
/// falls through to the segment's own `footer.json` (`disk_format_version`,
/// read only when needed), and with no answer there either, to legacy.
/// Guessing legacy wrongly costs one `doc_store.bin` fetch; guessing Lazy
/// wrongly used to cost the request.
///
/// This used to treat *the presence* of a manifest footer as proof of a
/// sidecar. Manifests cache a footer for every segment they list, legacy
/// ones included, so legacy segments were sent chasing a `doc_store.offsets`
/// that was never written (#179, #184). Even when the manifest claims a
/// sidecar and is wrong, `ensure_scoring_files_local` now falls back to
/// `doc_store.bin` instead of failing.
#[cfg(feature = "s3")]
fn segment_is_lazy_capable(
    manifest_format_version: Option<u32>,
    disk_format_version: impl FnOnce() -> Option<u32>,
) -> bool {
    match manifest_format_version.filter(|&v| v >= 1) {
        Some(_) => true,
        None => disk_format_version().is_some_and(|v| v >= 1),
    }
}

/// Why a segment classified Lazy cannot be scored from its sidecar alone.
#[cfg(feature = "s3")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DocStoreFallbackReason {
    /// `doc_store.offsets` is not on local disk after hydration: absent
    /// from S3, or the GET failed.
    Missing,
    /// `doc_store.offsets` is local but the reader would reject it.
    Rejected,
}

#[cfg(feature = "s3")]
impl DocStoreFallbackReason {
    fn describe(self) -> &'static str {
        match self {
            Self::Missing => "doc_store.offsets could not be fetched from S3",
            Self::Rejected => "doc_store.offsets failed the reader's sanity check",
        }
    }
}

/// Whether a segment that hydrated as Lazy must also fetch `doc_store.bin`
/// before it can be opened, and why. `None` when `doc_store.bin` is already
/// local or the reader will open the sidecar. See the fallback in
/// `AppState::ensure_scoring_files_local`.
#[cfg(feature = "s3")]
fn doc_store_fallback_reason(
    seg_path: &Path,
    manifest_footer: Option<&kosha_core::Footer>,
) -> Option<DocStoreFallbackReason> {
    if file_present_nonempty(&seg_path.join("doc_store.bin")) {
        return None;
    }
    if !file_present_nonempty(&seg_path.join("doc_store.offsets")) {
        return Some(DocStoreFallbackReason::Missing);
    }
    if !kosha_segment::doc_store_opens_lazily(seg_path, manifest_footer) {
        return Some(DocStoreFallbackReason::Rejected);
    }
    None
}

/// Redact the password portion of a Postgres URL for logs.
/// Whether a segment artifact is present *and usable* on local disk. Bare
/// existence is not enough for hydration decisions: interrupted non-atomic
/// writes (pre-atomic-rename builds) could leave zero-byte files behind, and
/// no valid segment artifact is ever empty — treating one as present makes
/// every hydration layer skip the refetch and every reader fail forever.
#[cfg(feature = "s3")]
fn file_present_nonempty(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|m| m.is_file() && m.len() > 0)
        .unwrap_or(false)
}

/// Decide which `(segment, posting-shard)` pairs in `seg_paths ×
/// posting_files` still need fetching, given `cached` — a snapshot of
/// pairs the caller has already confirmed present for the current
/// `(namespace, manifest_version, eviction_generation)` (see
/// `AppState::posting_blob_presence`). Free function, independent of
/// `AppState`/`S3Storage` (mirrors `partition_for_hydration`), so it can be
/// unit-tested with fake `is_split`/`is_present` predicates instead of real
/// segment files on disk.
///
/// `(logical_paths_to_fetch, newly_confirmed_present)` — see
/// `plan_posting_blob_fetch`'s doc comment for what each side means.
#[cfg(feature = "s3")]
type PostingBlobFetchPlan = (Vec<(String, u64)>, Vec<(String, String)>);

/// Returns `(logical_paths_to_fetch, newly_confirmed_present)`:
/// `logical_paths_to_fetch` are `(s3_relative_path, 0)` pairs (size unknown
/// up front, same convention `ensure_posting_blobs_local`'s callers already
/// use) still absent locally; `newly_confirmed_present` are pairs this call
/// verified present via a real stat (cache miss but found on disk) — the
/// caller folds these back into the presence cache.
#[cfg(feature = "s3")]
fn plan_posting_blob_fetch(
    seg_paths: &[PathBuf],
    posting_files: &[String],
    data_dir: &Path,
    cached: &HashMap<String, std::collections::HashSet<String>>,
    is_split: impl Fn(&Path) -> bool,
    is_present: impl Fn(&Path) -> bool,
) -> PostingBlobFetchPlan {
    let mut logical_paths: Vec<(String, u64)> = Vec::new();
    let mut newly_confirmed: Vec<(String, String)> = Vec::new();

    for seg_path in seg_paths {
        let Ok(rel_path) = seg_path.strip_prefix(data_dir) else {
            continue;
        };
        let s3_prefix = rel_path.to_string_lossy().into_owned();
        let cached_shards = cached.get(&s3_prefix);
        // Every shard this query needs is already confirmed for this
        // segment: nothing to plan. Checked before `is_split`, which opens
        // the segment's `inverted.idx` to read its format version. That
        // probe ran for every candidate segment on every search, to learn
        // something that cannot change: a segment with confirmed shards is
        // a split segment. On staging the stage it belongs to took 33 ms of
        // a 50 ms warm keyword search and 95 ms of a 117 ms phrase search,
        // growing with the number of candidate segments.
        if cached_shards.is_some_and(|set| posting_files.iter().all(|f| set.contains(f.as_str()))) {
            continue;
        }
        if !is_split(seg_path) {
            continue;
        }
        for file_name in posting_files {
            if cached_shards.is_some_and(|set| set.contains(file_name.as_str())) {
                continue; // already confirmed present this (version, eviction_generation)
            }
            if is_present(&seg_path.join(file_name)) {
                newly_confirmed.push((s3_prefix.clone(), file_name.clone()));
            } else {
                logical_paths.push((format!("{s3_prefix}/{file_name}"), 0));
            }
        }
    }

    (logical_paths, newly_confirmed)
}

/// Cache a TOC-proven absence as resolved, just like a present shard:
/// immutable segments cannot later gain terms. Unreadable TOCs fail closed.
#[cfg(feature = "s3")]
fn prune_unwritten_posting_blobs(
    data_dir: &Path,
    paths: &mut Vec<(String, u64)>,
) -> Result<Vec<(String, String)>, KoshaError> {
    let mut inventories = HashMap::new();
    for (path, _) in paths.iter() {
        if let Some((segment, _)) = path.rsplit_once('/') {
            if !inventories.contains_key(segment) {
                inventories.insert(
                    segment.to_owned(),
                    kosha_segment::posting_blob_inventory(&data_dir.join(segment))?,
                );
            }
        }
    }
    let mut absent = Vec::new();
    paths.retain(|(path, _)| {
        let Some((segment, file)) = path.rsplit_once('/') else {
            return true;
        };
        if inventories[segment].contains(file) {
            true
        } else {
            absent.push((segment.to_owned(), file.to_owned()));
            false
        }
    });
    Ok(absent)
}

/// `posting_blob_presence` type alias — spelled out at each use site
/// otherwise, this keeps the lookup/insert signatures readable. Not
/// `cfg`-gated: the `AppState` field itself isn't (matches
/// `completed_hydrations`, so non-`s3` builds still have a well-typed,
/// always-empty field rather than needing their own cfg'd variant).
type PostingBlobPresenceCache = HashMap<
    String,
    (
        u64,
        u64,
        Arc<HashMap<String, std::collections::HashSet<String>>>,
    ),
>;

/// Read side of the posting-blob presence cache: an `Arc` snapshot (cheap
/// refcount bump) of the confirmed-present shard map for `ns`, or an empty
/// map if there's no entry, it's for a different manifest version, or an
/// eviction has happened anywhere since it was written. Free function
/// (mirrors `partition_for_hydration`/`plan_posting_blob_fetch`) so the
/// exact staleness rules are unit-testable without `AppState`.
#[cfg(feature = "s3")]
fn posting_blob_cache_lookup(
    cache: &PostingBlobPresenceCache,
    ns: &str,
    manifest_version: u64,
    eviction_gen: u64,
) -> Arc<HashMap<String, std::collections::HashSet<String>>> {
    match cache.get(ns) {
        Some((v, g, segs)) if *v == manifest_version && *g == eviction_gen => Arc::clone(segs),
        _ => Arc::new(HashMap::new()),
    }
}

/// Write side: fold `newly_confirmed` pairs into `cache` for `ns`. If the
/// existing entry (if any) belongs to a different manifest version or
/// eviction generation than `manifest_version`/`write_gen`, it's replaced
/// outright rather than merged into — a stale entry's pairs are not known
/// to still be valid, so keeping them around would either waste memory
/// (dead manifest version) or reintroduce the exact stale-positive hazard
/// the eviction-generation check exists to close.
#[cfg(feature = "s3")]
fn posting_blob_cache_insert(
    cache: &mut PostingBlobPresenceCache,
    ns: &str,
    manifest_version: u64,
    write_gen: u64,
    newly_confirmed: Vec<(String, String)>,
) {
    let entry = cache
        .entry(ns.to_string())
        .or_insert_with(|| (manifest_version, write_gen, Arc::new(HashMap::new())));
    if entry.0 != manifest_version || entry.1 != write_gen {
        *entry = (manifest_version, write_gen, Arc::new(HashMap::new()));
    }
    let map = Arc::make_mut(&mut entry.2);
    for (seg_rel, file_name) in newly_confirmed {
        map.entry(seg_rel).or_default().insert(file_name);
    }
}

fn redact_database_url(url: &str) -> String {
    // postgresql://user:password@host/db → postgresql://user:***@host/db
    if let Some((scheme_user, rest)) = url.split_once("://") {
        if let Some((userinfo, hostpart)) = rest.split_once('@') {
            let user = userinfo.split(':').next().unwrap_or("user");
            return format!("{scheme_user}://{user}:***@{hostpart}");
        }
    }
    url.to_string()
}

// ─── Main ───────────────────────────────────────────────────────────────────

/// The empty probe query boot warmup and the manifest refresher hydrate
/// with: no terms, filters or kNN, so only the scoring scaffolding
/// (`footer.json`, `inverted.idx`, `doc_store.offsets`) is fetched.
#[cfg(any(feature = "s3", test))]
fn warmup_probe_query() -> kosha_core::SearchQuery {
    kosha_core::SearchQuery {
        query_text: String::new(),
        max_results: 0,
        bm25_params: Default::default(),
        from: 0,
        filter: None,
        sort: Vec::new(),
        search_after: None,
        highlight: None,
        aggs: std::collections::HashMap::new(),
        wildcard: None,
        fuzzy: None,
        match_phrase: None,
        knn: None,
        exact_total_hits: None,
        total_hits_cap: None,
        operator: None,
        no_cache: None,
        collapse: None,
        limit_per: None,
        rank_by: None,
        source_includes: None,
        source_excludes: None,
        rank: None,
        mode: None,
        fields: Vec::new(),
        rerank: Default::default(),
    }
}

fn main() {
    if std::env::args().nth(1).as_deref() == Some("migrate") {
        #[cfg(feature = "migrate")]
        {
            if let Err(error) = migrate::run(std::env::args().skip(2)) {
                eprintln!("migration failed: {error}");
                std::process::exit(1);
            }
            return;
        }
        #[cfg(not(feature = "migrate"))]
        {
            eprintln!("migration failed: this binary was built without the `migrate` feature");
            std::process::exit(1);
        }
    }

    // Load keys and fail closed before anything else, so a misconfigured
    // pod exits instead of serving unauthenticated traffic.
    auth::init_global();

    let role = std::env::var("KOSHA_ROLE").unwrap_or_else(|_| "query".into());
    let port = std::env::var("KOSHA_HTTP_PORT").unwrap_or_else(|_| "8080".into());
    let data_dir = std::env::var("KOSHA_DATA_DIR").unwrap_or_else(|_| "/var/lib/kosha/data".into());
    // `set_read_timeout`/`set_write_timeout` (see `serve`) are per-syscall,
    // not a connection-lifetime budget — a handler that takes far longer
    // than this to produce a response still delivers it fine, as long as
    // the read of the request and the write of the response each
    // individually complete within the window (see
    // `slow_handler_still_delivers_its_response_after_short_io_timeout_elapses`).
    // So this isn't a per-request processing deadline; it only needs to
    // bound how long a stalled/malicious client can hold a handler thread
    // hostage (the original motivation — see `stalled_connection_does_not_
    // block_healthz`). 30s was tuned for that, not for how long a cold
    // namespace's first query can legitimately take: a broad search that
    // has to hydrate an entire large namespace from S3 can take minutes
    // (~3-5 min observed for a ~27GB namespace), and every read/write on
    // that connection was still individually fast — so 30s was never
    // actually the bottleneck for *this* case, but see this file's
    // "Follow-ups" note in the BM25 benchmark RESULTS.md for the more
    // fundamental fix (a fast, honest 503 + Retry-After / progress signal
    // instead of a client just waiting in silence either way).
    let io_timeout_secs: u64 = std::env::var("KOSHA_HTTP_IO_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let addr = format!("0.0.0.0:{port}");

    let state = AppState::new_shared(PathBuf::from(data_dir.clone()));
    // Bind the listener first so a slow warmup doesn't delay the port;
    // `/readyz` keeps HPA/kube endpoints from sending traffic while it
    // runs (Tier 1 O2). Liveness (`/healthz`) is unaffected.
    state.spawn_warmup();
    state.spawn_rewarmer();
    state.spawn_warm_set_persister();
    state.spawn_doc_store_promoter();
    state.spawn_posting_promoter();
    state.spawn_idle_flush_timer();
    state.spawn_compaction_scheduler();
    state.spawn_manifest_refresher();
    state.spawn_local_gc();
    let listener = TcpListener::bind(&addr).expect("failed to bind HTTP listener");
    println!("kosha-server role={role} listening on {addr} data_dir={data_dir}");

    serve(listener, state, Duration::from_secs(io_timeout_secs));
}

/// Accept loop: one thread per connection.
///
/// A single slow or stalled client must never block the server — including
/// the /healthz probe, whose connection would otherwise sit in the accept
/// queue behind whatever is stuck. Socket-level timeouts bound how long a
/// stalled client can hold its handler thread.
fn serve(listener: TcpListener, state: Arc<AppState>, io_timeout: Duration) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                stream.set_read_timeout(Some(io_timeout)).ok();
                stream.set_write_timeout(Some(io_timeout)).ok();
                let state = Arc::clone(&state);
                std::thread::spawn(move || {
                    if let Err(err) = handle(&state, stream) {
                        eprintln!("request error: {err}");
                    }
                });
            }
            Err(err) => eprintln!("accept error: {err}"),
        }
    }
}

// ─── Request handling ───────────────────────────────────────────────────────

fn handle(state: &AppState, mut stream: TcpStream) -> Result<(), KoshaError> {
    let started = Instant::now();
    let mut reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|e| KoshaError::NotFound(format!("failed to clone stream: {e}")))?,
    );

    let mut request_line = String::new();
    reader.read_line(&mut request_line).ok();

    let mut headers = HashMap::new();
    let mut content_length: usize = 0;
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            // EOF, socket timeout, or any other read error ends the headers.
            Ok(0) | Err(_) => break,
            Ok(_) => {
                if line == "\r\n" {
                    break;
                }
                let line = line.trim_end();
                if let Some((key, value)) = line.split_once(':') {
                    let k = key.trim().to_lowercase();
                    let v = value.trim();
                    headers.insert(k, v.to_string());
                    if let Some(len) = headers.get("content-length") {
                        content_length = len.parse().unwrap_or(0);
                    }
                }
            }
        }
    }

    // Reject oversized bodies from the declared length, before reading (or
    // allocating) any of it.
    let max_body = auth::max_body_bytes();
    if content_length > max_body {
        let resp = json_error(
            413,
            &format!(
                "request body of {content_length} bytes exceeds the {max_body}-byte limit \
                 (KOSHA_MAX_BODY_BYTES)"
            ),
        );
        stream.write_all(resp.as_bytes()).ok();
        return Ok(());
    }

    let mut body = Vec::new();
    if content_length > 0 {
        reader
            .take(content_length as u64)
            .read_to_end(&mut body)
            .ok();
    }

    // Liveness/readiness probes hit this with no Authorization header (k8s
    // httpGet probes don't support that out of the box) — must stay
    // unauthenticated, per the doc comment at the top of this file.
    if request_line.starts_with("GET /healthz") || request_line.starts_with("GET /v1/healthz") {
        stream
            .write_all(json_ok(&serde_json::json!({"status": "ok"})).as_bytes())
            .ok();
        return Ok(());
    }

    // `/readyz` (Tier 1 O2): distinct from `/healthz` (liveness — always
    // 200) so a query pod mid cold-read-warmup stays alive-but-not-ready
    // and HPA/kube endpoints remove it from Service rotation until its
    // configured hot namespaces are prefetched. Probes hit this
    // unauthenticated, same as `/healthz`.
    if request_line.starts_with("GET /readyz") || request_line.starts_with("GET /v1/readyz") {
        if state.is_ready() {
            stream
                .write_all(json_ok(&serde_json::json!({"status": "ready"})).as_bytes())
                .ok();
        } else {
            let body = serde_json::json!({
                "status": "warming",
                "warmup_namespaces": state.warmup_namespaces,
                "boot_namespaces": *state.warming.boot_namespaces.lock().unwrap(),
            })
            .to_string();
            let resp = format!(
                "HTTP/1.1 503 Service Unavailable\r\ncontent-length: {}\r\n\
                 content-type: application/json\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(resp.as_bytes()).ok();
        }
        return Ok(());
    }

    // ── API key authentication ──────────────────────────────────────────
    // Per proto/kosha/v1/kosha.proto: Authorization: Bearer <key> or X-Api-Key.
    let authentication = if service_authorized(&headers, state.admin_auth_key.as_deref())
        || service_authorized(&headers, state.internal_service_key.as_deref())
    {
        Ok("service".to_string())
    } else {
        authenticate(&headers)
    };
    // ── Query/ingest role routing ────────────────────────────────────────
    // A query-role pod (KOSHA_INGEST_HOST set) forwards write-path requests
    // to the single ingest pod instead of executing them locally — see
    // is_write_path/forward_to_ingest. Every other request (all reads, and
    // everything on the ingest pod itself, which never forwards) is handled
    // locally exactly as before this existed.
    let response = match (
        &authentication,
        &state.ingest_host,
        &state.write_http_client,
    ) {
        (Err(resp), _, _) => resp.clone(),
        (Ok(_), Some(ingest_host), Some(client)) if is_write_path(&request_line) => {
            forward_to_ingest(client, ingest_host, &request_line, &headers, &body)
        }
        (Ok(tenant), _, _) => route(&request_line, &headers, &body, tenant, state),
    };
    log_request(
        &request_line,
        authentication.as_deref().unwrap_or("-"),
        body.len(),
        &response,
        started,
    );
    stream.write_all(response.as_bytes()).ok();
    Ok(())
}

/// One line per API request: who asked for what, the status returned and how
/// long it took. A successful search is skipped because it already prints
/// the more detailed `search timing:` line; probes never reach here.
fn log_request(
    request_line: &str,
    tenant: &str,
    body_bytes: usize,
    response: &str,
    started: Instant,
) {
    let status = response.get(9..12).unwrap_or("???");
    let mut parts = request_line.split_whitespace();
    let (method, target) = (parts.next().unwrap_or("-"), parts.next().unwrap_or("-"));
    let path = target.split('?').next().unwrap_or(target);
    if status == "200" && path.ends_with("/search") {
        return;
    }
    println!(
        "request: {method} {path} status={status} tenant={tenant} ms={:.1} in_bytes={body_bytes} out_bytes={}",
        started.elapsed().as_secs_f64() * 1e3,
        response.len()
    );
}

/// Extract and validate the API key from request headers.
/// Returns the tenant id on success, or a 401 response string on failure.
fn authenticate(headers: &HashMap<String, String>) -> Result<String, String> {
    let store = match auth::global() {
        // Only reachable with an explicit `KOSHA_AUTH=disabled`; an empty
        // key set no longer means "auth off" (see `auth::init_global`).
        auth::Auth::Disabled => return Ok("dev".to_string()),
        auth::Auth::Keys(store) => store,
    };

    let api_key: Option<&str> = headers
        .get("authorization")
        .map(|v| v.as_str())
        .and_then(|v| v.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").map(|v| v.as_str()))
        .or_else(|| {
            // Check case-insensitively for x-api-key (raw headers may preserve case)
            headers.iter().find_map(|(k, v)| {
                if k.eq_ignore_ascii_case("x-api-key") {
                    Some(v.as_str())
                } else {
                    None
                }
            })
        })
        .map(str::trim);

    match api_key {
        Some(key) => match store.lookup(key) {
            Some(tenant) => Ok(tenant),
            None => Err(json_error_body(401, "invalid API key")),
        },
        None => Err(json_error_body(
            401,
            "missing API key — use Authorization: Bearer <key> or X-Api-Key header",
        )),
    }
}

/// True for every write-mutating route `route()` dispatches to a
/// mutation-performing handler (index/flush/replace/delete/admin) — the set
/// a query pod forwards to `KOSHA_INGEST_HOST` instead of handling locally.
/// False for read-only routes (search/exists/stats/healthz), which every
/// pod always handles itself.
///
/// **Keep this in sync with `route()`'s own dispatch table below** — this
/// intentionally mirrors it rather than sharing one lookup, since `route()`
/// needs to actually execute the matched handler while this only needs a
/// yes/no classification before a handler ever runs. If a new write route
/// is added to `route()`, add its prefix here too.
fn is_write_path(request_line: &str) -> bool {
    // Namespace lifecycle: create and delete.
    if is_create_namespace_route(request_line) || bare_namespace(request_line, "DELETE").is_some() {
        return true;
    }
    // v1 tenant-scoped write routes.
    if extract_namespace(request_line, "POST /v1/namespaces/", "/documents").is_some() {
        return true;
    }
    if extract_namespace(request_line, "POST /v1/namespaces/", "/flush").is_some() {
        return true;
    }
    if extract_namespace(request_line, "POST /v1/namespaces/", "/delete").is_some() {
        return true;
    }
    if extract_namespace(request_line, "POST /v1/namespaces/", "/schema").is_some() {
        return true;
    }
    // v1 admin routes — all mutate segment/manifest state.
    if request_line.starts_with("POST /v1/admin/") {
        return true;
    }
    // Compaction jobs and scheduler state live on the write pod that runs
    // them; a query pod has none, so these reads are forwarded too.
    if request_line.starts_with("GET /v1/admin/compaction-") {
        return true;
    }
    // Legacy (pre-v1) write routes.
    if request_line.starts_with("POST /index")
        || request_line.starts_with("POST /replace")
        || request_line.starts_with("POST /flush")
        || request_line.starts_with("POST /delete")
    {
        return true;
    }
    // Everything else — /search, /exists, /stats, /healthz, and their v1
    // equivalents — is read-only.
    false
}

/// Forward a write-path request to the single ingest pod, verbatim, and
/// relay its response back exactly as received. Used only by query-role
/// pods (`AppState::ingest_host`/`write_http_client` are `None` on the
/// ingest pod itself, which never calls this).
///
/// Single attempt, bounded timeout, no retry here — if the ingest pod is
/// mid-`Recreate` restart this fails fast with a clear error; the caller's
/// own retry logic (kosha_client's max_retries/retry_on_timeout) is what
/// recovers, exactly as it would against a single pod restarting today.
fn forward_to_ingest(
    client: &reqwest::blocking::Client,
    ingest_host: &str,
    request_line: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> String {
    let Some((method, path)) = parse_method_and_path(request_line) else {
        return json_error(400, "malformed request line");
    };

    let url = format!("{}{}", ingest_host.trim_end_matches('/'), path);
    let mut req = client.request(
        method
            .parse::<reqwest::Method>()
            .unwrap_or(reqwest::Method::POST),
        &url,
    );
    for (k, v) in headers {
        // Host/content-length are set by reqwest itself from the URL/body;
        // forwarding the original values would just be wrong for the new
        // destination.
        if k.eq_ignore_ascii_case("host") || k.eq_ignore_ascii_case("content-length") {
            continue;
        }
        req = req.header(k, v);
    }
    if !body.is_empty() {
        req = req.body(body.to_vec());
    }

    match req.send() {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let upstream_body = resp.text().unwrap_or_default();
            raw_response(status, &upstream_body)
        }
        Err(e) => json_error(
            503,
            &format!("forward to ingest ({ingest_host}) failed: {e}"),
        ),
    }
}

/// Parse `"METHOD /path HTTP/1.1\r\n"` into `("METHOD", "/path")`.
fn parse_method_and_path(request_line: &str) -> Option<(&str, &str)> {
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;
    Some((method, path))
}

fn request_key(headers: &HashMap<String, String>) -> Option<&str> {
    headers
        .get("authorization")
        .and_then(|v| v.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").map(String::as_str))
}

fn service_authorized(headers: &HashMap<String, String>, expected: Option<&str>) -> bool {
    expected
        .filter(|s| !s.is_empty())
        .is_some_and(|expected| request_key(headers) == Some(expected))
}

fn valid_namespace_component(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name
            .chars()
            .any(|c| c == '/' || c == '\\' || c.is_control())
}

/// Authorize before dispatch, including compatibility and internal routes.
fn route(
    request_line: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
    tenant: &str,
    state: &AppState,
) -> String {
    let Some((method, path)) = parse_method_and_path(request_line) else {
        return json_error(400, "invalid request line");
    };
    if path.starts_with("/v1/admin/") {
        if !service_authorized(headers, state.admin_auth_key.as_deref()) {
            return json_error(403, "admin credential required");
        }
    } else if path.starts_with("/internal/") {
        if !service_authorized(headers, state.internal_service_key.as_deref()) {
            return json_error(403, "internal service credential required");
        }
    } else {
        if !valid_namespace_component(tenant) {
            return json_error(403, "invalid tenant");
        }
        if let Some(rest) = path.strip_prefix("/v1/namespaces/") {
            // `/v1/namespaces/{ns}/{action}`, or the bare
            // `/v1/namespaces/{ns}` that `DELETE` addresses.
            let ns = match rest.rsplit_once('/') {
                Some((ns, _action)) => ns,
                None => rest.split('?').next().unwrap_or(rest),
            };
            if !valid_namespace_component(&url_decode(ns)) {
                return json_error(400, "invalid namespace");
            }
        }
        let bare_path = path.split('?').next().unwrap_or(path);
        let is_legacy = matches!(
            bare_path,
            "/index" | "/exists" | "/replace" | "/search" | "/flush" | "/delete" | "/stats"
        );
        if is_legacy {
            if !auth::legacy_routes_enabled() {
                return json_error(
                    410,
                    "unversioned routes are disabled (KOSHA_LEGACY_ROUTES=off); \
                     use /v1/namespaces/{ns}/...",
                );
            }
            auth::record_legacy_use(tenant, bare_path);
        }
        if path == "/stats" || path == "/v1/stats" {
            return if service_authorized(headers, state.admin_auth_key.as_deref()) {
                handle_stats(state)
            } else {
                json_error(403, "use namespace-scoped stats")
            };
        }
        if method == "POST"
            && [
                "/index", "/exists", "/replace", "/search", "/flush", "/delete",
            ]
            .contains(&bare_path)
        {
            let mut value: serde_json::Value = match serde_json::from_slice(body) {
                Ok(v) => v,
                Err(_) => return json_error(400, "invalid JSON"),
            };
            let Some(ns) = value.get("namespace").and_then(|v| v.as_str()) else {
                return json_error(400, "missing namespace");
            };
            if !valid_namespace_component(ns) {
                return json_error(400, "invalid namespace; use an unqualified namespace name");
            }
            value["namespace"] = serde_json::json!(tenant_namespace(tenant, ns));
            return route_inner(
                request_line,
                headers,
                &serde_json::to_vec(&value).unwrap(),
                tenant,
                state,
            );
        }
        if method == "GET" && bare_path == "/search" {
            let Some((_, query)) = path.split_once('?') else {
                return json_error(400, "missing query parameters");
            };
            let mut pairs = Vec::new();
            let mut found = false;
            for pair in query.split('&') {
                if let Some(raw) = pair.strip_prefix("ns=") {
                    if found {
                        return json_error(400, "duplicate namespace");
                    }
                    let ns = url_decode(raw);
                    if !valid_namespace_component(&ns) {
                        return json_error(400, "invalid namespace");
                    }
                    // The original escaped component is preserved; tenant names
                    // are escaped too so they cannot inject query parameters.
                    let encoded_tenant: String =
                        tenant.bytes().map(|b| format!("%{b:02X}")).collect();
                    pairs.push(format!("ns={encoded_tenant}%2F{raw}"));
                    found = true;
                } else {
                    pairs.push(pair.to_string());
                }
            }
            if !found {
                return json_error(400, "missing ns");
            }
            let scoped = format!("GET /search?{} HTTP/1.1", pairs.join("&"));
            return route_inner(&scoped, headers, body, tenant, state);
        }
        if !path.starts_with("/v1/namespaces/")
            && !["/healthz", "/v1/healthz", "/v1/namespaces"].contains(&bare_path)
        {
            return json_error(404, "not found");
        }
    }
    route_inner(request_line, headers, body, tenant, state)
}

fn route_inner(
    request_line: &str,
    _headers: &HashMap<String, String>,
    body: &[u8],
    tenant: &str,
    state: &AppState,
) -> String {
    // ── v1 proto-defined routes ────────────────────────────────────────────
    // proto/kosha/v1/kosha.proto is the canonical source of truth for paths.
    if request_line.starts_with("GET /v1/healthz") {
        return json_ok(&serde_json::json!({"status": "ok"}));
    }

    if is_create_namespace_route(request_line) {
        return handle_create_namespace(body, tenant, state);
    }

    if let Some(ns) = bare_namespace(request_line, "DELETE") {
        return handle_delete_namespace(&ns, tenant, state);
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/documents") {
        return handle_index_with_ns(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/search") {
        return handle_search_post_with_ns(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/flush") {
        return handle_flush_with_ns(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/delete") {
        return handle_delete_with_ns(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/exists") {
        return handle_exists_with_ns(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/schema") {
        return handle_put_schema_with_ns(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "GET /v1/namespaces/", "/schema") {
        return handle_get_schema_with_ns(&ns, tenant, state);
    }

    if request_line.starts_with("GET /v1/stats") {
        return handle_stats(state);
    }

    if let Some(ns) = extract_namespace(request_line, "GET /v1/namespaces/", "/stats") {
        return handle_namespace_stats(&ns, tenant, state);
    }

    // ── Admin routes (Postgres-backed only) ────────────────────────────────
    if request_line.starts_with("POST /v1/admin/api-keys") {
        return handle_create_api_key(body, tenant, state);
    }

    if request_line.starts_with("POST /v1/admin/rebuild-filter-blooms") {
        return handle_rebuild_filter_blooms(body, tenant, state);
    }

    if request_line.starts_with("POST /v1/admin/backfill-offset-tables") {
        return handle_backfill_offset_tables(body, tenant, state);
    }

    if request_line.starts_with("POST /v1/admin/compact-namespace") {
        return handle_compact_namespace(body, tenant, state);
    }

    if request_line.starts_with("GET /v1/admin/warmup-status") {
        return json_ok(&state.warmup_status_json());
    }

    if request_line.starts_with("GET /v1/admin/compaction-status") {
        return handle_compaction_status(state);
    }

    if let Some(rest) = request_line.strip_prefix("GET /v1/admin/compaction-jobs/") {
        let job_id = rest.split([' ', '?']).next().unwrap_or_default();
        return handle_compaction_job(job_id, state);
    }

    if request_line.starts_with("POST /v1/admin/import-namespace") {
        return handle_import_namespace(body, tenant, state);
    }

    if request_line.starts_with("POST /v1/admin/gc ")
        || request_line.starts_with("POST /v1/admin/gc?")
    {
        return handle_segment_gc(body, state);
    }

    // ── Internal, pod-to-pod only ────────────────────────────────────────
    // Never called by clients — only by a peer replica that lost the
    // `hydration_leases` claim race for this file (see `fetch_from_peer`).
    // Authorized by the route wrapper using KOSHA_INTERNAL_API_KEY; ordinary
    // tenant credentials cannot retrieve another tenant's cached files.
    if request_line.starts_with("GET /internal/segment/") {
        #[cfg(all(feature = "s3", feature = "postgres"))]
        {
            let path = request_line
                .strip_prefix("GET /internal/segment/")
                .and_then(|rest| rest.split(' ').next())
                .unwrap_or("");
            return handle_internal_segment(&url_decode(path), state);
        }
        #[cfg(not(all(feature = "s3", feature = "postgres")))]
        {
            return json_error(404, "not found");
        }
    }

    // ── Legacy Phase 1 routes (backward compat) ────────────────────────────
    // These will be removed after DecoverAI cuts over to the v1 paths.
    if request_line.starts_with("GET /healthz") {
        return json_ok(&serde_json::json!({"status": "ok"}));
    }

    if request_line.starts_with("POST /index") {
        return handle_index(body, state);
    }

    if request_line.starts_with("POST /exists") {
        return handle_exists(body, state);
    }

    if request_line.starts_with("POST /replace") {
        return handle_replace(body, state);
    }

    if request_line.starts_with("GET /search") {
        return handle_search_get(request_line, state);
    }

    if request_line.starts_with("POST /search") {
        return handle_search_post(body, state);
    }

    if request_line.starts_with("POST /flush") {
        return handle_flush(body, state);
    }

    if request_line.starts_with("POST /delete") {
        return handle_delete(body, state);
    }

    if request_line.starts_with("GET /stats") {
        return handle_stats(state);
    }

    json_error(404, "not found")
}

/// Extract a namespace from a v1 path: `METHOD /v1/namespaces/{ns}/suffix ...`
///
/// `prefix` includes the HTTP method (e.g. "POST /v1/namespaces/"), but the
/// request line's path segment doesn't — match the method against the full
/// request line, then strip only the path portion of `prefix` from the path.
fn extract_namespace(request_line: &str, prefix: &str, suffix: &str) -> Option<String> {
    if !request_line.starts_with(prefix) {
        return None;
    }
    let after_method = request_line.split(' ').nth(1)?;
    let path_prefix = prefix.split(' ').nth(1)?;
    let after_prefix = after_method.strip_prefix(path_prefix)?;
    let ns = after_prefix.strip_suffix(suffix)?;
    if ns.is_empty() || ns.contains('/') {
        return None;
    }
    Some(url_decode(ns))
}

/// `POST /v1/namespaces` (optionally with a query string).
fn is_create_namespace_route(request_line: &str) -> bool {
    parse_method_and_path(request_line).is_some_and(|(method, path)| {
        method == "POST" && path.split('?').next() == Some("/v1/namespaces")
    })
}

/// The namespace of a bare `METHOD /v1/namespaces/{ns}` path (no action
/// suffix), URL-decoded. `None` for any other method or path shape.
fn bare_namespace(request_line: &str, method: &str) -> Option<String> {
    let (m, path) = parse_method_and_path(request_line)?;
    if m != method {
        return None;
    }
    let rest = path.strip_prefix("/v1/namespaces/")?;
    let ns = rest.split('?').next()?;
    if ns.is_empty() || ns.contains('/') {
        return None;
    }
    Some(url_decode(ns))
}

// ─── POST /v1/namespaces, DELETE /v1/namespaces/{ns} ────────────────────────

/// Proto `FieldType` names (`proto/kosha/v1/kosha.proto`), by enum number.
const SCHEMA_FIELD_TYPES: [&str; 8] = [
    "FIELD_TYPE_UNSPECIFIED",
    "FIELD_TYPE_TEXT",
    "FIELD_TYPE_KEYWORD",
    "FIELD_TYPE_INTEGER",
    "FIELD_TYPE_FLOAT",
    "FIELD_TYPE_DATE",
    "FIELD_TYPE_BOOLEAN",
    "FIELD_TYPE_VECTOR",
];

/// Validate a `NamespaceSchema` from a `CreateNamespaceRequest` and return
/// it in canonical proto-JSON form: every field has `type` (the
/// `FIELD_TYPE_*` name), `indexed`, `stored` and `analyzer`. A field type
/// may be given as the enum name, the bare name in any case (`"text"`,
/// `"Keyword"`), or the enum number, and under `type` or `field_type`.
/// Unknown keys and unknown types are rejected rather than ignored. The
/// analysis extensions `POST /v1/namespaces/{ns}/schema` takes
/// (`default_analyzer`, `default_stopwords`, and a field's `stopwords`)
/// pass through unchanged.
fn parse_namespace_schema(value: &serde_json::Value) -> Result<serde_json::Value, String> {
    const EXTENSIONS: [&str; 2] = ["default_analyzer", "default_stopwords"];
    let obj = value
        .as_object()
        .ok_or("schema must be an object with a 'fields' map")?;
    if let Some(key) = obj
        .keys()
        .find(|k| *k != "fields" && !EXTENSIONS.contains(&k.as_str()))
    {
        return Err(format!("unknown schema key '{key}'"));
    }
    let fields = match obj.get("fields") {
        None | Some(serde_json::Value::Null) => serde_json::Map::new(),
        Some(serde_json::Value::Object(map)) => map.clone(),
        Some(_) => return Err("schema.fields must be an object".into()),
    };
    let mut out = serde_json::Map::new();
    for (name, field) in fields {
        if name.is_empty() {
            return Err("schema field names must be non-empty".into());
        }
        let field = field
            .as_object()
            .ok_or_else(|| format!("schema field '{name}' must be an object"))?;
        let mut canonical = serde_json::Map::new();
        let mut field_type = SCHEMA_FIELD_TYPES[0];
        let mut seen_type = false;
        for (key, v) in field {
            match key.as_str() {
                "type" | "field_type" => {
                    if seen_type {
                        return Err(format!("schema field '{name}' names its type twice"));
                    }
                    seen_type = true;
                    field_type = match v {
                        serde_json::Value::Number(n) => n
                            .as_u64()
                            .and_then(|i| SCHEMA_FIELD_TYPES.get(i as usize).copied()),
                        serde_json::Value::String(t) => {
                            let upper = t.trim().to_ascii_uppercase();
                            let bare = upper.strip_prefix("FIELD_TYPE_").unwrap_or(&upper);
                            SCHEMA_FIELD_TYPES
                                .iter()
                                .copied()
                                .find(|known| known.strip_prefix("FIELD_TYPE_") == Some(bare))
                        }
                        _ => None,
                    }
                    .ok_or_else(|| format!("schema field '{name}' has unknown type {v}"))?;
                }
                "indexed" | "stored" => {
                    let flag = v
                        .as_bool()
                        .ok_or_else(|| format!("schema field '{name}': '{key}' must be a bool"))?;
                    canonical.insert(key.clone(), serde_json::json!(flag));
                }
                "analyzer" => {
                    let analyzers: Vec<String> = match v {
                        serde_json::Value::String(a) => vec![a.clone()],
                        serde_json::Value::Array(items) => items
                            .iter()
                            .map(|a| a.as_str().map(str::to_owned))
                            .collect::<Option<_>>()
                            .ok_or_else(|| {
                                format!("schema field '{name}': 'analyzer' must be strings")
                            })?,
                        _ => {
                            return Err(format!(
                                "schema field '{name}': 'analyzer' must be a list of strings"
                            ))
                        }
                    };
                    canonical.insert("analyzer".into(), serde_json::json!(analyzers));
                }
                "stopwords" => {
                    canonical.insert(key.clone(), v.clone());
                }
                other => return Err(format!("schema field '{name}' has unknown key '{other}'")),
            }
        }
        canonical.insert("type".into(), serde_json::json!(field_type));
        canonical
            .entry("indexed")
            .or_insert(serde_json::json!(false));
        canonical
            .entry("stored")
            .or_insert(serde_json::json!(false));
        canonical
            .entry("analyzer")
            .or_insert(serde_json::json!(Vec::<String>::new()));
        out.insert(name, serde_json::Value::Object(canonical));
    }
    let mut schema = serde_json::json!({ "fields": out });
    for key in EXTENSIONS {
        if let Some(v) = obj.get(key) {
            schema[key] = v.clone();
        }
    }
    // The analyzers must resolve, as `POST /v1/namespaces/{ns}/schema`
    // requires.
    let typed: kosha_core::NamespaceSchema =
        serde_json::from_value(schema.clone()).map_err(|e| e.to_string())?;
    typed.resolve().map_err(|e| e.to_string())?;
    Ok(schema)
}

/// `POST /v1/namespaces` — `CreateNamespaceRequest` → `CreateNamespaceResponse`.
///
/// The namespace is created inside the caller's tenant. 409 if it already
/// exists, 400 for an invalid name or schema. Explicit creation is
/// optional: namespaces still auto-create on first write.
///
/// The schema is validated, echoed back in canonical form, and stored as
/// the namespace schema exactly as `POST /v1/namespaces/{ns}/schema` would
/// (its analysis settings apply to the segments written from now on).
fn handle_create_namespace(body: &[u8], tenant: &str, state: &AppState) -> String {
    let request: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };
    let Some(name) = request.get("namespace").and_then(|v| v.as_str()) else {
        return json_error(400, "missing 'namespace'");
    };
    if !valid_namespace_component(name) {
        return json_error(400, "invalid namespace; use an unqualified namespace name");
    }
    let schema = match request.get("schema") {
        None | Some(serde_json::Value::Null) => serde_json::json!({ "fields": {} }),
        Some(raw) => match parse_namespace_schema(raw) {
            Ok(schema) => schema,
            Err(e) => return json_error(400, &format!("invalid schema: {e}")),
        },
    };
    let typed_schema: kosha_core::NamespaceSchema =
        serde_json::from_value(schema.clone()).unwrap_or_default();
    let id = NamespaceId(tenant_namespace(tenant, name));

    let lock = state.publication_lock(&id);
    let guard = lock.lock().unwrap();
    // A namespace this writer already holds (written to, not yet
    // registered) exists as far as the caller can tell.
    if state.indexer.manifest_version(&id).is_some() {
        return json_error(409, &format!("namespace '{name}' already exists"));
    }
    let created = state
        .controller
        .lock()
        .unwrap()
        .create_namespace(id.clone());
    match created {
        Ok(()) => {}
        Err(KoshaError::NamespaceExists(_)) => {
            return json_error(409, &format!("namespace '{name}' already exists"));
        }
        Err(e) => return json_error(500, &format!("create namespace failed: {e}")),
    }
    let manifest = state
        .controller
        .lock()
        .unwrap()
        .manifest_cloned(&id)
        .unwrap_or_else(|| kosha_core::Manifest::empty(0));
    // Searchable (empty) right away on this node; query replicas pick the
    // manifest up on their next refresh.
    state.indexer.restore_manifest(id.clone(), manifest.clone());
    println!(
        "namespace created: ns={} version={}",
        id.0, manifest.version
    );
    // Publishing takes the publication lock itself.
    drop(guard);
    let mut manifest = manifest;
    if typed_schema != kosha_core::NamespaceSchema::default() {
        match state.indexer.set_schema(&id, typed_schema) {
            Ok(true) => {
                if let Err(e) = state.publish_namespace(&id) {
                    return json_error(500, &format!("schema publication failed: {e}"));
                }
                if let Some(published) = state.indexer.manifest(&id) {
                    manifest = published;
                }
            }
            Ok(false) => {}
            Err(KoshaError::InvalidQuery(message)) => return json_error(400, &message),
            Err(e) => return json_error(500, &format!("schema update failed: {e}")),
        }
    }
    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    json_ok(&serde_json::json!({
        "id": name,
        "schema": schema,
        "document_count": manifest.segments.iter().map(|s| s.doc_count as u64).sum::<u64>(),
        "segment_count": manifest.segments.len(),
        "version": manifest.version,
        "created_at": created_at,
    }))
}

/// `DELETE /v1/namespaces/{ns}` — `DeleteNamespaceResponse`. 404 if the
/// caller's tenant has no such namespace.
///
/// Under the namespace's publication lock, in order:
/// 1. the control store drops the namespace and its manifest, keeping the
///    highest version it reached so a re-created namespace continues above
///    it (query replicas see a new version, never an old cached one);
/// 2. the writer forgets the namespace: manifest, write buffer, deletes and
///    every WAL record of it, so neither a flush nor a restart brings the
///    documents back;
/// 3. this node's caches for it are purged and its local segment
///    directory removed;
/// 4. with S3, every object under the namespace's prefix is deleted,
///    synchronously, before the response. A failure there is reported
///    (`storage_reclaimed: false`) but does not undo the delete: the
///    leftover objects are unreferenced, and a re-created namespace never
///    reuses their keys (segment ids carry a random writer id).
///
/// Query replicas keep their own disk cache of the deleted segments until
/// LRU eviction (`KOSHA_CACHE_MAX_BYTES`); they answer 404 once they see
/// the control store has no manifest (the next refresher tick, see
/// `run_manifest_refresh_tick`, or a search with `max_staleness_seconds: 0`).
fn handle_delete_namespace(name: &str, tenant: &str, state: &AppState) -> String {
    let id = NamespaceId(tenant_namespace(tenant, name));
    let lock = state.publication_lock(&id);
    let _guard = lock.lock().unwrap();
    let local_version = state.indexer.manifest_version(&id);
    let deleted = state
        .controller
        .lock()
        .unwrap()
        .delete_namespace(&id, local_version.unwrap_or(0));
    let (manifest, retired) = match deleted {
        Ok(deleted) => deleted,
        // Known only to this writer (written to but not yet registered, or
        // WAL-recovered under the in-memory control plane): still delete it.
        Err(KoshaError::NamespaceNotFound(_)) if local_version.is_some() => (
            state
                .indexer
                .manifest_cloned(&id)
                .unwrap_or_else(|| kosha_core::Manifest::empty(0)),
            local_version.unwrap_or(0),
        ),
        Err(KoshaError::NamespaceNotFound(_)) => {
            return json_error(404, &format!("namespace '{name}' not found"));
        }
        Err(e) => return json_error(500, &format!("delete namespace failed: {e}")),
    };
    let next_version =
        kosha_core::initial_manifest_version(Some(retired.max(local_version.unwrap_or(0))));
    // Local segment files go while the writer still holds the namespace's
    // locks, so a write to the new incarnation cannot lose its fresh segment.
    let mut local_cleanup = Ok(());
    let dropped = state.indexer.drop_namespace(&id, next_version, || {
        local_cleanup = state.cache.evict_prefix(&id.0);
    });
    match dropped {
        Ok(0) => {}
        Ok(shared) => println!(
            "namespace {} deleted: {shared} WAL file(s) shared with other namespaces are kept \
             until those flush; recovery skips this namespace's records in them",
            id.0
        ),
        Err(e) => {
            return json_error(
                500,
                &format!(
                    "namespace '{name}' was removed from the control store, but discarding its \
                     write-ahead log failed: {e}"
                ),
            )
        }
    }
    state.forget_namespace_caches(&id);
    let mut storage_reclaimed = true;
    if let Err(e) = local_cleanup {
        eprintln!(
            "WARN: namespace {} deleted, local segment cleanup failed: {e}",
            id.0
        );
        storage_reclaimed = false;
    }
    #[cfg(feature = "s3")]
    if let Some(ref s3) = state.s3_storage {
        match s3.delete_prefix(&id.0) {
            Ok(objects) => println!(
                "namespace deleted: ns={} s3_objects_deleted={objects}",
                id.0
            ),
            Err(e) => {
                eprintln!(
                    "WARN: namespace {} deleted, but deleting its S3 objects failed: {e}",
                    id.0
                );
                storage_reclaimed = false;
            }
        }
    }
    println!(
        "namespace deleted: ns={} version={} segments={} next_version={next_version}",
        id.0,
        manifest.version,
        manifest.segments.len()
    );
    json_ok(&serde_json::json!({
        "namespace": name,
        "acknowledged": true,
        "storage_reclaimed": storage_reclaimed,
    }))
}

// ─── POST /index ────────────────────────────────────────────────────────────

fn handle_index(body: &[u8], state: &AppState) -> String {
    let request: IndexRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };
    // A GeoPoint that is not a point would be stored but never indexed, so
    // `geo_distance` and geo decays could never see it: refuse it (#212).
    for doc in &request.documents {
        for field in &doc.fields {
            if field.field_type == kosha_core::FieldType::GeoPoint
                && kosha_core::rank::parse_geo_str(&field.value).is_none()
            {
                return json_error(
                    400,
                    &format!(
                        "document {:?}: GeoPoint field {:?} must be \"lat,lon\", \
                         {{\"lat\": .., \"lon\": ..}} or [lon, lat] within range, got {:?}",
                        doc.id.0, field.name, field.value
                    ),
                );
            }
        }
    }

    let ns = request.namespace;

    // Upsert may rewrite immutable segments that hold prior versions of the
    // same ids, so materialize any S3-backed segment directories first.
    // Batched across the whole manifest in one fanned-out call rather than
    // one `ensure_segment_local` round trip per segment — for a namespace
    // with hundreds of segments the per-segment loop turned every index
    // call into hundreds of sequential S3 list+GET round trips.
    #[cfg(feature = "s3")]
    {
        let manifest = {
            let indexer = &state.indexer;
            indexer.manifest_cloned(&ns)
        };
        if let Some(manifest) = manifest {
            let paths: Vec<PathBuf> = manifest
                .segments
                .iter()
                .map(|entry| state.data_dir.join(&ns.0).join(&entry.segment_id.0))
                .collect();
            // Rewrite/lookup paths must see a segment's full fidelity,
            // including vectors — unlike the search path, there's no
            // per-call signal here for whether vectors matter, so always
            // fetch them.
            let outcome = state.ensure_segments_local(&paths, true);
            if let Some(error) = hydration_error_response(&outcome) {
                return error;
            }
        }
    }

    let (count, manifest_changed) = {
        let indexer = &state.indexer;
        let version_before = indexer.manifest(&ns).map(|m| m.version);
        let count = match indexer.index_documents(ns.clone(), request.documents) {
            Ok(c) => c,
            // A rejected document (e.g. a vector of the wrong dimension
            // for its field, #204) is the client's error.
            Err(KoshaError::InvalidQuery(message)) => return json_error(400, &message),
            Err(e) => return json_error(500, &format!("indexing error: {e}")),
        };
        // index_documents auto-flushes when the buffer hits the threshold
        // (or rewrites on id collision), which mutates the manifest —
        // detect that via the version.
        let manifest_changed = indexer.manifest(&ns).map(|m| m.version) != version_before;
        (count, manifest_changed)
    };

    // Ensure namespace is registered in the controller.
    {
        let mut ctrl = state.controller.lock().unwrap();
        ctrl.ensure_namespace(ns.clone());
    }

    // An auto-flush updated the segment list — publish manifest + S3 so the
    // write survives restarts (explicit flushes go through publish_namespace too).
    if manifest_changed || state.indexer.manifest_cloned(&ns).is_some() {
        if let Err(e) = state.publish_namespace(&ns) {
            return publication_error("publication failed", &e);
        }
    }

    json_ok(&IndexResponse {
        indexed_count: count,
        namespace: ns,
    })
}

// ─── POST /exists ───────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct ExistsRequest {
    namespace: NamespaceId,
    ids: Vec<String>,
}

fn handle_exists(body: &[u8], state: &AppState) -> String {
    let request: ExistsRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };

    // Existence scans immutable segments, so materialize any S3-backed
    // segment directories before entering the indexer lock. Batched across
    // the whole manifest — see handle_index for why this isn't a per-segment
    // loop.
    #[cfg(feature = "s3")]
    {
        let manifest = {
            let indexer = &state.indexer;
            indexer.manifest_cloned(&request.namespace)
        };
        if let Some(manifest) = manifest {
            let paths: Vec<PathBuf> = manifest
                .segments
                .iter()
                .map(|entry| {
                    state
                        .data_dir
                        .join(&request.namespace.0)
                        .join(&entry.segment_id.0)
                })
                .collect();
            // Rewrite/lookup paths must see a segment's full fidelity,
            // including vectors — unlike the search path, there's no
            // per-call signal here for whether vectors matter, so always
            // fetch them.
            let outcome = state.ensure_segments_local(&paths, true);
            if let Some(error) = hydration_error_response(&outcome) {
                return error;
            }
        }
    }

    let ids: Vec<kosha_core::DocumentId> = request
        .ids
        .iter()
        .cloned()
        .map(kosha_core::DocumentId)
        .collect();
    let existing = {
        let indexer = &state.indexer;
        match indexer.existing_ids(&request.namespace, &ids) {
            Ok(set) => set,
            Err(e) => return json_error(500, &format!("exists error: {e}")),
        }
    };
    let mut present = Vec::new();
    let mut missing = Vec::new();
    for id in &request.ids {
        if existing.contains(&kosha_core::DocumentId(id.clone())) {
            present.push(id.clone());
        } else {
            missing.push(id.clone());
        }
    }
    json_ok(&serde_json::json!({
        "namespace": request.namespace.0,
        "existing": present,
        "missing": missing,
    }))
}

// ─── POST /replace ──────────────────────────────────────────────────────────

fn handle_replace(body: &[u8], state: &AppState) -> String {
    let request: IndexRequest = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(error) => return json_error(400, &format!("invalid JSON: {error}")),
    };
    let namespace = request.namespace;

    // Replacement scans immutable segments, so materialize any S3-backed
    // segment directories before entering the indexer lock. Batched across
    // the whole manifest — see handle_index for why this isn't a per-segment
    // loop.
    #[cfg(feature = "s3")]
    {
        let manifest = {
            let indexer = &state.indexer;
            indexer.manifest_cloned(&namespace)
        };
        if let Some(manifest) = manifest {
            let paths: Vec<PathBuf> = manifest
                .segments
                .iter()
                .map(|entry| state.data_dir.join(&namespace.0).join(&entry.segment_id.0))
                .collect();
            // Rewrite/lookup paths must see a segment's full fidelity,
            // including vectors — unlike the search path, there's no
            // per-call signal here for whether vectors matter, so always
            // fetch them.
            let outcome = state.ensure_segments_local(&paths, true);
            if let Some(error) = hydration_error_response(&outcome) {
                return error;
            }
        }
    }

    let count = {
        let indexer = &state.indexer;
        match indexer.replace_documents(namespace.clone(), request.documents) {
            Ok(count) => count,
            Err(KoshaError::InvalidQuery(message)) => return json_error(400, &message),
            Err(error) => return json_error(500, &format!("replacement error: {error}")),
        }
    };
    {
        let mut controller = state.controller.lock().unwrap();
        controller.ensure_namespace(namespace.clone());
    }
    if let Err(e) = state.publish_namespace(&namespace) {
        return publication_error("publication failed", &e);
    }

    json_ok(&IndexResponse {
        indexed_count: count,
        namespace,
    })
}

// ─── POST /flush ────────────────────────────────────────────────────────────

/// Make one namespace's pending writes visible: recovery-hydrate if a WAL
/// replay needs the flushed segments local, cut a segment from the write
/// buffer, then publish (S3 upload + manifest CAS). The single-namespace
/// body of `POST /flush`, shared with the idle-buffer flush timer
/// (`spawn_idle_flush_timer`) so both take exactly the same path. `Err`
/// carries a ready-to-send JSON error response.
fn flush_and_publish_namespace(state: &AppState, namespace: &NamespaceId) -> Result<(), String> {
    #[cfg(feature = "s3")]
    if state.indexer.needs_recovery_hydration(namespace) {
        if let Some(manifest) = state.indexer.manifest_cloned(namespace) {
            let paths: Vec<_> = manifest
                .segments
                .iter()
                .map(|e| state.data_dir.join(&namespace.0).join(&e.segment_id.0))
                .collect();
            let outcome = state.ensure_segments_local(&paths, true);
            if let Some(error) = hydration_error_response(&outcome) {
                return Err(error);
            }
        }
    }
    state
        .indexer
        .flush_namespace(namespace)
        .map_err(|e| json_error(500, &format!("flush error: {e}")))?;
    state
        .publish_namespace(namespace)
        .map_err(|e| publication_error("publication failed", &e))
}

fn handle_flush(body: &[u8], state: &AppState) -> String {
    let req: std::collections::HashMap<String, String> =
        serde_json::from_slice(body).unwrap_or_default();
    let ns = req.get("namespace").cloned();

    if let Some(name) = ns {
        return match flush_and_publish_namespace(state, &NamespaceId(name)) {
            Ok(()) => json_ok(&serde_json::json!({"status": "flushed"})),
            Err(error) => error,
        };
    }

    #[cfg(feature = "s3")]
    for namespace in state.indexer.namespaces() {
        if state.indexer.needs_recovery_hydration(&namespace) {
            if let Some(manifest) = state.indexer.manifest_cloned(&namespace) {
                let paths: Vec<_> = manifest
                    .segments
                    .iter()
                    .map(|e| state.data_dir.join(&namespace.0).join(&e.segment_id.0))
                    .collect();
                let outcome = state.ensure_segments_local(&paths, true);
                if let Some(error) = hydration_error_response(&outcome) {
                    return error;
                }
            }
        }
    }

    if let Err(e) = state.indexer.flush_all() {
        return json_error(500, &format!("flush error: {e}"));
    }

    // Publish manifest(s) to the control store and upload segments to S3.
    for ns_id in &state.indexer.namespaces() {
        if let Err(e) = state.publish_namespace(ns_id) {
            return publication_error("publication failed", &e);
        }
    }

    json_ok(&serde_json::json!({"status": "flushed"}))
}

/// Default for `KOSHA_FLUSH_MAX_AGE_MS` — Elasticsearch's default
/// `refresh_interval`, which is what the Python client's ES-shaped callers
/// already expect from a bulk write without `refresh`.
const DEFAULT_FLUSH_MAX_AGE_MS: u64 = 1000;

fn flush_max_age_from_env() -> Option<Duration> {
    parse_flush_max_age(std::env::var("KOSHA_FLUSH_MAX_AGE_MS").ok().as_deref())
}

/// `None` disables the idle-flush timer (`0`); unparsable values fall back
/// to the default rather than silently disabling visibility.
fn parse_flush_max_age(raw: Option<&str>) -> Option<Duration> {
    let ms = match raw.map(str::trim) {
        None | Some("") => DEFAULT_FLUSH_MAX_AGE_MS,
        Some(v) => match v.parse::<u64>() {
            Ok(ms) => ms,
            Err(_) => {
                eprintln!(
                    "WARN: KOSHA_FLUSH_MAX_AGE_MS={v} not a number of milliseconds — \
                     using {DEFAULT_FLUSH_MAX_AGE_MS}"
                );
                DEFAULT_FLUSH_MAX_AGE_MS
            }
        },
    };
    (ms > 0).then(|| Duration::from_millis(ms))
}

/// Poll a quarter as often as the age bound so visibility latency is at
/// most ~1.25 × `max_age`, but never spin faster than every 50 ms.
fn idle_flush_tick(max_age: Duration) -> Duration {
    (max_age / 4).max(Duration::from_millis(50))
}

/// One pass of the idle-flush timer. `pending_since` remembers when each
/// namespace was first seen with pending documents; a namespace whose
/// pending age reaches `max_age` is flushed and published, and forgotten
/// once empty. Returns the namespaces it flushed successfully (for tests).
/// A failed flush is logged and retried on a later tick — its entry is kept,
/// so it stays due.
fn run_idle_flush_tick(
    state: &AppState,
    pending_since: &mut HashMap<NamespaceId, Instant>,
    max_age: Duration,
    now: Instant,
) -> Vec<NamespaceId> {
    let mut flushed = Vec::new();
    let namespaces = state.indexer.namespaces();
    pending_since.retain(|ns, _| namespaces.contains(ns));
    for ns in namespaces {
        if state.indexer.pending_doc_count(&ns) == 0 {
            pending_since.remove(&ns);
            continue;
        }
        let since = *pending_since.entry(ns.clone()).or_insert(now);
        if now.saturating_duration_since(since) < max_age {
            continue;
        }
        match flush_and_publish_namespace(state, &ns) {
            Ok(()) => {
                pending_since.remove(&ns);
                flushed.push(ns);
            }
            Err(error) => eprintln!("idle flush of namespace {} failed: {error}", ns.0),
        }
    }
    flushed
}

// ─── POST /delete (delete by query) ──────────────────────────────────────────

fn handle_delete(body: &[u8], state: &AppState) -> String {
    let body_val: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };

    let ns = match body_val.get("namespace").and_then(|v| v.as_str()) {
        Some(n) => NamespaceId(n.to_string()),
        None => return json_error(400, "missing 'namespace'"),
    };

    // Extract filter from body — supports ES-style "query" field.
    let filter_val = body_val.get("filter").or_else(|| body_val.get("query"));
    let filter: kosha_core::FilterClause = match filter_val {
        Some(v) => match serde_json::from_value(v.clone()) {
            Ok(f) => f,
            Err(e) => return json_error(400, &format!("invalid filter: {e}")),
        },
        None => return json_error(400, "missing 'filter' or 'query'"),
    };

    #[cfg(feature = "s3")]
    if let Some(manifest) = state.indexer.manifest_cloned(&ns) {
        let paths: Vec<PathBuf> = manifest
            .segments
            .iter()
            .map(|e| state.data_dir.join(&ns.0).join(&e.segment_id.0))
            .collect();
        let outcome = state.ensure_segments_local(&paths, false);
        if let Some(error) = hydration_error_response(&outcome) {
            return error;
        }
    }

    let (_manifest, count) = {
        let indexer = &state.indexer;
        let manifest = match indexer.manifest_cloned(&ns) {
            Some(m) => m,
            None => return json_error(404, &format!("namespace '{}' not found", ns.0)),
        };
        let old_manifest = manifest.clone();
        match indexer.delete_by_query(&ns, &old_manifest, &filter) {
            Ok(c) => (old_manifest, c),
            // A filter the data cannot answer (e.g. `prefix` on a numeric
            // field) is the client's mistake, not a server fault.
            Err(e @ KoshaError::InvalidQuery(_)) => return json_error(400, &e.to_string()),
            Err(e) => return json_error(500, &format!("delete error: {e}")),
        }
    };

    if let Err(e) = state.publish_namespace(&ns) {
        return publication_error("deletion publication failed", &e);
    }

    json_ok(&serde_json::json!({
        "deleted": count,
        "namespace": ns.0,
    }))
}

// ─── GET /stats ──────────────────────────────────────────────────────────────

fn handle_stats(state: &AppState) -> String {
    // Snapshot indexer stats before the cache-size walk — that walk can take
    // tens of seconds on a large cache and must not interleave with the
    // per-namespace bookkeeping below.
    let (namespaces, total_docs, total_segments) = {
        let indexer = &state.indexer;
        let mut namespaces: Vec<serde_json::Value> = Vec::new();
        let mut total_docs: u64 = 0;
        let mut total_segments: usize = 0;

        for ns in indexer.namespaces() {
            let manifest = match indexer.manifest(&ns) {
                Some(m) => m,
                None => continue,
            };
            let ns_docs: u64 = manifest.segments.iter().map(|s| s.doc_count as u64).sum();
            total_docs += ns_docs;
            total_segments += manifest.segments.len();

            let mut entry = serde_json::json!({
                "namespace": ns.0,
                "documents": ns_docs,
                "segments": manifest.segments.len(),
                "version": manifest.version,
                "manifest_version": manifest.version,
            });
            if state.ingest_host.is_some() {
                add_manifest_follow_stats(state, &ns, &mut entry);
            }
            namespaces.push(entry);
        }
        (namespaces, total_docs, total_segments)
    };

    #[cfg(feature = "s3")]
    let (s3_enabled, s3_bucket, s3_prefix) = match &state.s3_storage {
        Some(s3) => (
            true,
            Some(s3.bucket().to_string()),
            Some(s3.prefix().to_string()),
        ),
        None => (false, None, None),
    };
    #[cfg(not(feature = "s3"))]
    let (s3_enabled, s3_bucket, s3_prefix): (bool, Option<String>, Option<String>) =
        (false, None, None);
    #[cfg(feature = "s3")]
    let segment_checksum_failures = state
        .segment_checksum_failures
        .load(std::sync::atomic::Ordering::Relaxed);
    #[cfg(not(feature = "s3"))]
    let segment_checksum_failures = 0u64;
    #[cfg(feature = "s3")]
    let doc_store_offsets_fallbacks = state
        .doc_store_fallbacks
        .load(std::sync::atomic::Ordering::Relaxed);
    #[cfg(not(feature = "s3"))]
    let doc_store_offsets_fallbacks = 0u64;

    json_ok(&serde_json::json!({
        "total_documents": total_docs,
        "total_segments": total_segments,
        "namespaces": namespaces,
        "control_plane": state.control_plane_kind,
        "cache_root": state.cache.root().display().to_string(),
        "cache_size_bytes": state.cache.total_size(),
        "s3_enabled": s3_enabled,
        "segment_checksum_failures": segment_checksum_failures,
        "doc_store_offsets_fallbacks": doc_store_offsets_fallbacks,
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
        "doc_store_reads": state.doc_store_reads.to_json(state.doc_read_mode),
        "posting_reads": state.posting_reads.to_json(state.posting_read_mode),
        "manifest_refresh": state
            .ingest_host
            .is_some()
            .then(|| state.manifest_follow.summary_json(Instant::now())),
        "gc": state.gc_stats.to_json(
            state.indexer.retired_segment_count(),
            state.indexer.local_retired_count(),
        ),
        "rerank": state.rerankers.stats_json(),
    }))
}

/// Per-namespace manifest-following fields for a query replica's stats
/// (#232): the stored version last seen, when the local manifest was last
/// confirmed current, and how long ago that was (`manifest_lag_seconds`,
/// `null` if never — e.g. the control plane has been down since boot).
fn add_manifest_follow_stats(state: &AppState, ns: &NamespaceId, entry: &mut serde_json::Value) {
    let status = state.manifest_follow.status(ns);
    let lag = state.manifest_follow.lag(ns, Instant::now());
    entry["stored_manifest_version"] = serde_json::json!(status.as_ref().map(|s| s.stored_version));
    entry["last_refresh_at"] = serde_json::json!(status
        .as_ref()
        .map(|s| manifest_follow::unix_seconds(s.last_refresh_at)));
    entry["manifest_lag_seconds"] = serde_json::json!(lag.map(|d| d.as_secs_f64()));
}

// ─── POST /search (JSON body, supports filters) ─────────────────────────────

fn handle_search_post(body: &[u8], state: &AppState) -> String {
    // Parse the full JSON body that includes namespace alongside SearchQuery fields.
    let body_val: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };

    let ns = match body_val.get("namespace").and_then(|v| v.as_str()) {
        Some(n) => NamespaceId(n.to_string()),
        None => return json_error(400, "missing 'namespace' in search body"),
    };

    // Query replicas only (#232): how stale a manifest this search accepts.
    let max_staleness =
        match manifest_follow::parse_max_staleness(body_val.get("max_staleness_seconds")) {
            Ok(v) => v,
            Err(e) => return json_error(400, &e),
        };

    let query: kosha_core::SearchQuery = match serde_json::from_value(body_val) {
        Ok(q) => q,
        Err(e) => return json_error(400, &format!("invalid search query: {e}")),
    };
    // Reject out-of-range parameters before touching storage or the
    // result cache (the searcher checks again for non-HTTP callers).
    if let Err(e) = query.validate() {
        return search_error_response(&e);
    }
    // A reranked request searches for its first stage (top `top_n` from
    // offset 0) and is reranked and paged after scoring; see
    // `kosha_query::rerank`. Unknown providers are a 400 before any work.
    let reranker = match query.rerank {
        Some(ref spec) => match state.rerankers.get(&spec.provider) {
            Some(provider) => Some(Arc::clone(provider)),
            None => {
                return json_error(
                    400,
                    &format!(
                        "unknown rerank provider '{}'; configured: {:?}",
                        spec.provider,
                        state.rerankers.names()
                    ),
                )
            }
        },
        None => None,
    };
    let first_stage = query.first_stage_query();
    // What hydration and the searcher see. The cache key below uses the
    // full request, so rerank parameters are part of it.
    let search_query = first_stage.as_ref().unwrap_or(&query);

    if let Err(e) = state.ensure_query_manifest_fresh(&ns, max_staleness) {
        return search_error_response(&e);
    }

    // Shared, not copied: see `Indexer::manifest_shared`.
    let Some(snapshot) = state.indexer.manifest_shared(&ns) else {
        return json_error(404, &format!("namespace '{}' not found", ns.0));
    };
    let manifest: &kosha_core::Manifest = &snapshot;
    let tombstones = Some(&manifest.tombstones);

    // ── Whole-response result cache ─────────────────────────────────────
    // Real query traffic is Zipfian in queries too; identical requests
    // against an unchanged namespace re-pay the full scoring pass today.
    // Keyed by (namespace, manifest_version, canonical query): a publish
    // bumps the version and orphans stale entries, the same free
    // invalidation the hydration-verdict and blob-presence caches use.
    // kNN queries are cacheable too — ANN probing is deterministic for a
    // fixed manifest — but their embedding is digested rather than
    // inlined (see `canonical_query_for_cache`): a 1024-dim vector is
    // ~14KB of JSON. Deletions are part of a versioned snapshot; immutable
    // generation references also participate in cache identity.
    // A `rank_by` decaying from `"now"` (#212) answers differently a moment
    // later on an unchanged manifest, so it is never cached.
    let time_dependent = query
        .rank_by
        .as_ref()
        .is_some_and(|rank_by| rank_by.is_time_dependent());
    let cache_key =
        if result_cache_budget() > 0 && !query.no_cache.unwrap_or(false) && !time_dependent {
            canonical_query_for_cache(&query).map(|mut canonical| {
                let mut generations: Vec<_> = manifest
                    .deletion_vectors
                    .iter()
                    .map(|(segment, vector)| (&segment.0, &vector.generation))
                    .collect();
                generations.sort_unstable();
                canonical.push_str(&serde_json::to_string(&generations).unwrap());
                (ns.0.clone(), manifest.version, canonical)
            })
        } else {
            None
        };
    if let Some(ref key) = cache_key {
        if let Some(resp) = state.result_cache.lock().unwrap().lookup(key) {
            // Still demand for this namespace (#213): a cached response is
            // one eviction or publish away from reading the same files.
            state.record_search_access(
                &ns,
                manifest,
                search_query,
                &kosha_query::SearchPhaseStats::default(),
                &HydrationOutcome::default(),
            );
            return resp;
        }
    }

    // One accountant for everything this request hydrates — footer prefetch,
    // scoring set, posting blobs, and the page's doc stores all share it, so
    // the ceiling covers the request's whole cost rather than each batch's.
    // See `hydration_budget`.
    #[cfg(feature = "s3")]
    let hydration_request = Arc::new(RequestHydration::new(state.hydrate_request_max_bytes));
    // Footer-first hydrate: bloom-prune before downloading full segments.
    let t_hydrate = Instant::now();
    #[cfg(feature = "s3")]
    let hydration = {
        // Namespace-level cached hydration verdict (Tier 1 O2): skip
        // `hydrate_segments_for_search` entirely when we've already
        // confirmed — for this `(namespace, manifest_version)` pair — that
        // every scoring-set file is local. The verdict is invalidated
        // automatically when a new segment publishes (manifest versions are
        // bumped at publish time), and explicitly on any search error
        // below (a disk-cache eviction could remove a scoring file after
        // the verdict was cached). On a fully-warm namespace this collapses
        // the 15–130 ms per-query hydration-check floor to a single
        // HashMap lookup.
        let verdict_key = (ns.0.clone(), manifest.version);
        // What THIS query actually needs verified — see `HydrationVerdict`.
        // A cached verdict from an earlier pass that never checked
        // `filters.bin`/`vector.idx` (e.g. an unfiltered plain-text query,
        // or warmup's own probe) must not be trusted by a query that does
        // need one of them; `covers` enforces that per-file-class check
        // instead of trusting a flat "namespace hydrated" bool.
        let needs_filters = query_filter_fields(search_query);
        let needs_vectors_key = knn_verdict_key(search_query);
        let needs_vectors = needs_vectors_key.as_deref();
        let cached_covers = state
            .completed_hydrations
            .lock()
            .unwrap()
            .get(&verdict_key)
            .is_some_and(|v| v.covers(needs_filters.as_ref(), needs_vectors));
        let outcome = if cached_covers {
            // The cached verdict can only ever cover the *static* scoring
            // set (footers/TOCs/offsets/filters). Posting blobs are fetched
            // per query — each term hashes to one of the 256 shard files —
            // so a namespace can be "fully hydrated" for every query seen
            // so far and still lack the shard a fresh term needs. Skipping
            // this ensure silently scored fresh terms against absent
            // shards as zero hits (non-deterministically per pod). It is
            // sub-ms when the shards are already local: one metadata stat
            // per term-shard per split segment.
            state.ensure_query_posting_blobs(&ns, manifest, search_query, &hydration_request)
        } else {
            let outcome =
                state.hydrate_segments_for_search(&ns, manifest, search_query, &hydration_request);
            // Cache what this pass verified: the static scoring set is
            // local for this manifest version, so future warm queries with
            // matching-or-lesser needs skip the per-segment static checks
            // (posting blobs are still ensured per query above).
            //
            // Only when `!bloom_pruned`: a filtered/term-pruned query's
            // `to_hydrate` set only covers the segments *its own* filter
            // value's bloom check admitted, not every segment in the
            // manifest — caching that narrow success as "namespace fully
            // hydrated" (this verdict is shared across every query shape
            // via the namespace+version key) would let a later,
            // differently-filtered query skip hydrating segments it
            // actually needs, silently undercounting. Confirmed live: a
            // query pod got stuck this way after its first-ever query on a
            // namespace happened to be a filtered one, understating a
            // filtered count by up to 80% for every later query on that
            // pod until it was restarted (which drops this cache).
            //
            // Merged (`|=` via `HydrationVerdict::merge`), not overwritten:
            // a query needing only `filters.bin` must not erase a
            // `vectors_verified` flag an earlier kNN pass already earned,
            // and vice versa.
            if outcome.missing.is_empty() && !outcome.bloom_pruned {
                state
                    .completed_hydrations
                    .lock()
                    .unwrap()
                    .entry(verdict_key)
                    .or_default()
                    .merge(needs_filters.as_ref(), needs_vectors);
            }
            outcome
        };
        if let Some(error) = hydration_error_response(&outcome) {
            return error;
        }
        outcome
    };
    #[cfg(not(feature = "s3"))]
    let hydration = HydrationOutcome::default();
    let hydrate_ms = t_hydrate.elapsed().as_secs_f64() * 1e3;

    // Bound concurrent scoring passes — see `AppState::search_semaphore`.
    // Acquired after hydration (which has its own ceiling) so a search
    // waiting on S3 doesn't also hold a scoring slot.
    let t_queue = Instant::now();
    let Some(slot) = state.search_semaphore.acquire_or_shed() else {
        // Shed rather than queue: see `Semaphore::acquire_or_shed`.
        return json_error(
            429,
            "search overloaded: scoring queue is saturated — retry with backoff",
        );
    };
    let queue_ms = t_queue.elapsed().as_secs_f64() * 1e3;

    // On-demand page documents: scoring hydrated only the offsets sidecar
    // for Lazy segments, so the searcher asks back for the page's
    // documents that are not local yet. `hydrate_page_docs` fetches their
    // spans with ranged GETs (or whole files, `KOSHA_DOC_STORE_READ_MODE=
    // whole`) and **persists** them, so a warm repeat is served from local
    // disk with no S3 request (#245; the PR #84 property).
    #[cfg(feature = "s3")]
    let page_fetch = Mutex::new(doc_spans::PageFetchStats::default());
    #[cfg(feature = "s3")]
    let ensure_doc_store = |pages: &[PageSegment<'_>]| {
        // Same accountant as the scoring-set fetch above: the request pays
        // for its page's bytes, so the request's ceiling has to cover them.
        let stats = state.hydrate_page_docs(pages, &hydration_request);
        page_fetch.lock().unwrap().add(&stats);
    };
    #[cfg(feature = "s3")]
    let doc_store_hydrator: DocStoreHydrator<'_> = Some(&ensure_doc_store);
    #[cfg(not(feature = "s3"))]
    let doc_store_hydrator: DocStoreHydrator<'_> = None;
    // kNN postings a segment probes but does not hold: fetched by range
    // into the span cache, on the same accountant (#245).
    #[cfg(feature = "s3")]
    let vector_fetch_stats = Mutex::new(posting_spans::PostingFetchStats::default());
    #[cfg(feature = "s3")]
    let fetch_vectors = |seg: &Path, file: &str, ranges: &[(u64, u64)]| {
        let stats = state.hydrate_vector_postings(seg, file, ranges, &hydration_request);
        vector_fetch_stats.lock().unwrap().add(&stats);
    };
    #[cfg(feature = "s3")]
    let vector_fetch: Option<kosha_query::VectorSpanFetch<'_>> = Some(&fetch_vectors);
    #[cfg(not(feature = "s3"))]
    let vector_fetch: Option<kosha_query::VectorSpanFetch<'_>> = None;

    match state.searcher.search_with_hydrators(
        &ns,
        manifest,
        search_query,
        tombstones,
        doc_store_hydrator,
        vector_fetch,
    ) {
        Ok((mut result, phases)) => {
            // Scoring is done; a reranker round trip must not hold a
            // scoring slot.
            drop(slot);
            #[cfg(feature = "s3")]
            let hydration = HydrationOutcome {
                page: *page_fetch.lock().unwrap(),
                vectors: *vector_fetch_stats.lock().unwrap(),
                ..hydration
            };
            log_search_timing(
                &ns,
                &hydration,
                hydrate_ms,
                queue_ms,
                &phases,
                result.total_hits,
                result.knn_degraded_segments,
            );
            state.record_search_access(&ns, manifest, search_query, &phases, &hydration);
            if let Some(ref provider) = reranker {
                let (reranked, call) = kosha_query::rerank::apply(&query, result, provider);
                result = reranked;
                if let Some(call) = call {
                    log_rerank(&ns, &call);
                    state.rerankers.record(&call);
                }
            }
            let resp = json_ok(&result);
            // Only successful responses enter the cache, after the search
            // proved every needed file readable. A search that fell back to
            // per-segment BM25 statistics (issue #246) because some
            // segment's footer or postings weren't local isn't cached
            // either: the miss is usually transient (best-effort stats
            // hydration), and caching it would pin the degraded scores for
            // the rest of this manifest version. Nor is a degraded rerank
            // (a transient fallback that would serve the unreranked order
            // until the namespace next changes).
            let cacheable = phases.bm25_stats != kosha_query::Bm25StatsUsed::SegmentFallback
                && result.rerank_degraded != Some(true);
            if let Some(key) = cache_key.filter(|_| cacheable) {
                state.result_cache.lock().unwrap().insert(key, &resp);
            }
            resp
        }
        Err(e) => {
            // A search failure (segment file IO error after eviction, or
            // admission shedding) likely means the cached hydration verdict
            // is stale: a scoring-set file we recorded as "all local" is no
            // longer there. Invalidate the verdict so the retry's
            // `hydrate_segments_for_search` actually re-checks physical
            // existence and re-fetches what's missing instead of trusting
            // the cache and failing again. Also drop the posting-blob
            // presence entry for this namespace: the eviction-generation
            // guard already covers a `self.cache` LRU eviction, but this is
            // a cheap belt-and-suspenders reset on any observed failure,
            // and matters for the non-eviction case where the searcher
            // itself failed for another reason.
            #[cfg(feature = "s3")]
            {
                state
                    .completed_hydrations
                    .lock()
                    .unwrap()
                    .remove(&(ns.0.clone(), manifest.version));
                state.posting_blob_presence.lock().unwrap().remove(&ns.0);
                // A page-materialize fetch that crossed the request's byte
                // ceiling leaves the doc store absent, which surfaces here
                // as an IO error. It is a shed, not a fault — report it as
                // one.
                if hydration_request.is_shed() {
                    return hydration_shed_response();
                }
            }
            search_error_response(&e)
        }
    }
}

/// Whole-response cache for `POST /search` — see the cache block in
/// [`handle_search_post`] for keying and exclusion rules. Bounded by
/// Canonical query string for [`ResultCache`] keys. Non-kNN queries are
/// their serde JSON verbatim (unchanged from the original cache). A kNN
/// query has its embedding replaced by a compact digest — dimension plus a
/// 128-bit content hash (two independently-seeded `DefaultHasher` passes
/// over the raw f32 bits) — because inlining a 1024-dim vector puts ~14KB
/// of JSON in every key: memory bloat and a long string compare per
/// lookup. 128 bits keeps accidental collisions out of practical reach;
/// a collision would silently serve another vector's results, so 64 bits
/// (birthday-bounded) was not acceptable for a correctness-bearing key.
fn canonical_query_for_cache(query: &SearchQuery) -> Option<String> {
    let Some(ref knn) = query.knn else {
        return serde_json::to_string(query).ok();
    };
    use std::hash::{Hash, Hasher};
    let mut h1 = std::collections::hash_map::DefaultHasher::new();
    let mut h2 = std::collections::hash_map::DefaultHasher::new();
    // Independent second stream via a fixed seed prefix; hashing bit
    // patterns (not floats) keeps NaN/-0.0 handling exact and total.
    for &v in &knn.vector {
        v.to_bits().hash(&mut h1);
    }
    (0xa5a5_5a5a_u64, knn.vector.len() as u64).hash(&mut h2);
    for &v in &knn.vector {
        v.to_bits().hash(&mut h2);
    }
    // Late interaction (#205): every token vector, with its boundaries, so
    // two bags with the same flattened floats never share a key.
    let bag = knn.vectors.as_deref().unwrap_or_default();
    for token in bag {
        (0x5a5a_a5a5_u64, token.len() as u64).hash(&mut h1);
        (0x5a5a_a5a5_u64, token.len() as u64).hash(&mut h2);
        for &v in token {
            v.to_bits().hash(&mut h1);
            v.to_bits().hash(&mut h2);
        }
    }
    let digest = format!(
        "dim{}:bag{}:{:016x}{:016x}",
        knn.vector.len(),
        bag.len(),
        h1.finish(),
        h2.finish()
    );
    let mut stripped = query.clone();
    if let Some(ref mut k) = stripped.knn {
        k.vector = Vec::new();
        k.vectors = k.vectors.as_ref().map(|_| Vec::new());
    }
    serde_json::to_string(&stripped)
        .ok()
        .map(|canonical| format!("{canonical}|knn_vec:{digest}"))
}

/// `KOSHA_RESULT_CACHE_MAX_BYTES` (default 64MiB, `0` disables); LRU by
/// last-use tick with linear-scan eviction (entry counts are small at
/// realistic budgets). Response strings are cached verbatim, so a hit is
/// one map lookup plus a clone.
struct ResultCache {
    entries: HashMap<(String, u64, String), ResultCacheEntry>,
    bytes: usize,
    budget: usize,
    tick: u64,
    hits: u64,
    misses: u64,
}

struct ResultCacheEntry {
    resp: String,
    bytes: usize,
    last_used: u64,
}

impl ResultCache {
    fn new(budget: usize) -> Self {
        Self {
            entries: HashMap::new(),
            bytes: 0,
            budget,
            tick: 0,
            hits: 0,
            misses: 0,
        }
    }

    fn lookup(&mut self, key: &(String, u64, String)) -> Option<String> {
        self.tick += 1;
        let tick = self.tick;
        if let Some(e) = self.entries.get_mut(key) {
            e.last_used = tick;
            self.hits += 1;
            Some(e.resp.clone())
        } else {
            self.misses += 1;
            None
        }
    }

    fn forget_namespace(&mut self, namespace: &str) {
        let mut freed = 0;
        self.entries.retain(|(ns, _, _), entry| {
            let keep = ns != namespace;
            if !keep {
                freed += entry.bytes;
            }
            keep
        });
        self.bytes -= freed;
    }

    fn insert(&mut self, key: (String, u64, String), resp: &str) {
        if self.budget == 0 {
            return;
        }
        let entry_bytes = resp.len() + key.0.len() + key.2.len() + 64;
        // A single oversized response (huge page sizes) must not evict the
        // whole cache to store one entry nobody may repeat.
        if entry_bytes > self.budget / 4 {
            return;
        }
        if let Some(old) = self.entries.remove(&key) {
            self.bytes -= old.bytes;
        }
        self.tick += 1;
        self.bytes += entry_bytes;
        self.entries.insert(
            key,
            ResultCacheEntry {
                resp: resp.to_owned(),
                bytes: entry_bytes,
                last_used: self.tick,
            },
        );
        while self.bytes > self.budget {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            if let Some(e) = self.entries.remove(&oldest) {
                self.bytes -= e.bytes;
            }
        }
    }
}

/// `KOSHA_RESULT_CACHE_MAX_BYTES` — byte budget for [`ResultCache`]
/// (default 64MiB, `0` disables). Read once, same rationale as the other
/// cache knobs.
fn result_cache_budget() -> usize {
    static BUDGET: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("KOSHA_RESULT_CACHE_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64 * 1024 * 1024)
    })
}

// ─── GET /search (query params, simple queries) ─────────────────────────────

fn handle_search_get(request_line: &str, state: &AppState) -> String {
    // Parse query string from the request line.
    // request_line looks like: "GET /search?ns=...&q=...&max_results=... HTTP/1.1"
    let query_string = request_line
        .split(' ')
        .nth(1)
        .and_then(|path| path.split('?').nth(1))
        .unwrap_or("");

    let params: HashMap<String, String> = query_string
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| (k.to_string(), url_decode(v)))
        .collect();

    let ns = match params.get("ns") {
        Some(ns) => NamespaceId(ns.clone()),
        None => return json_error(400, "missing 'ns' query parameter"),
    };

    let query_text = match params.get("q") {
        Some(q) => q.clone(),
        None => return json_error(400, "missing 'q' query parameter"),
    };

    let max_results: usize = params
        .get("max_results")
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    // `k1` / `b` are optional; an absent one uses the segment's build-time
    // value. A present but unparsable one is a 400 — it used to fall back
    // to the default silently, so a typo looked like it had no effect.
    let parse_param = |name: &str| -> Result<Option<f64>, String> {
        match params.get(name) {
            None => Ok(None),
            Some(v) => v
                .parse::<f64>()
                .map(Some)
                .map_err(|_| format!("'{name}' must be a number, got '{v}'")),
        }
    };
    // `bm25_stats=segment|namespace` (issue #246): where IDF / avgdl come
    // from. Absent means the engine default.
    let stats = match params.get("bm25_stats") {
        None => None,
        Some(v) => match kosha_core::Bm25StatsScope::parse(v) {
            Some(scope) => Some(scope),
            None => {
                return json_error(
                    400,
                    &format!("'bm25_stats' must be 'namespace' or 'segment', got '{v}'"),
                )
            }
        },
    };
    let bm25_params = match (parse_param("k1"), parse_param("b")) {
        (Ok(k1), Ok(b)) => kosha_core::Bm25Overrides { k1, b, stats },
        (Err(e), _) | (_, Err(e)) => return json_error(400, &e),
    };

    let query = SearchQuery {
        query_text,
        max_results,
        bm25_params,
        from: 0,
        filter: None,
        sort: vec![],
        search_after: None,
        highlight: None,
        aggs: std::collections::HashMap::new(),
        wildcard: None,
        fuzzy: None,
        match_phrase: None,
        knn: None,
        exact_total_hits: None,
        total_hits_cap: None,
        operator: None,
        no_cache: None,
        collapse: None,
        limit_per: None,
        rank_by: None,
        source_includes: None,
        source_excludes: None,
        rank: None,
        mode: None,
        // `fields=title^2,content` (issue #249).
        fields: params
            .get("fields")
            .map(|v| {
                v.split(',')
                    .map(str::trim)
                    .filter(|f| !f.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        rerank: None,
    };
    if let Err(e) = query.validate() {
        return search_error_response(&e);
    }

    let max_staleness = match manifest_follow::parse_max_staleness(
        params
            .get("max_staleness_seconds")
            .map(|v| serde_json::Value::String(v.clone()))
            .as_ref(),
    ) {
        Ok(v) => v,
        Err(e) => return json_error(400, &e),
    };
    if let Err(e) = state.ensure_query_manifest_fresh(&ns, max_staleness) {
        return search_error_response(&e);
    }

    // Shared, not copied: see `Indexer::manifest_shared`.
    let Some(snapshot) = state.indexer.manifest_shared(&ns) else {
        return json_error(404, &format!("namespace '{}' not found", ns.0));
    };
    let manifest: &kosha_core::Manifest = &snapshot;
    let tombstones = Some(&manifest.tombstones);

    // One accountant for everything this request hydrates — footer prefetch,
    // scoring set, posting blobs, and the page's doc stores all share it, so
    // the ceiling covers the request's whole cost rather than each batch's.
    // See `hydration_budget`.
    #[cfg(feature = "s3")]
    let hydration_request = Arc::new(RequestHydration::new(state.hydrate_request_max_bytes));

    let t_hydrate = Instant::now();
    #[cfg(feature = "s3")]
    let hydration = {
        // Same cached-verdict short-circuit as `handle_search_post` —
        // see that function for the rationale (Tier 1 O2) and
        // `HydrationVerdict` for why this is per-file-class, not a flat
        // bool.
        let verdict_key = (ns.0.clone(), manifest.version);
        let needs_filters = query_filter_fields(&query);
        let needs_vectors_key = knn_verdict_key(&query);
        let needs_vectors = needs_vectors_key.as_deref();
        let cached_covers = state
            .completed_hydrations
            .lock()
            .unwrap()
            .get(&verdict_key)
            .is_some_and(|v| v.covers(needs_filters.as_ref(), needs_vectors));
        let outcome = if cached_covers {
            // Posting blobs are per-query and outside the cached verdict —
            // see `handle_search_post` for the full rationale.
            state.ensure_query_posting_blobs(&ns, manifest, &query, &hydration_request)
        } else {
            let outcome =
                state.hydrate_segments_for_search(&ns, manifest, &query, &hydration_request);
            // Only merge when unfiltered — see `HydrationOutcome::bloom_pruned`
            // and `handle_search_post` for the full rationale.
            if outcome.missing.is_empty() && !outcome.bloom_pruned {
                state
                    .completed_hydrations
                    .lock()
                    .unwrap()
                    .entry(verdict_key)
                    .or_default()
                    .merge(needs_filters.as_ref(), needs_vectors);
            }
            outcome
        };
        if let Some(error) = hydration_error_response(&outcome) {
            return error;
        }
        outcome
    };
    #[cfg(not(feature = "s3"))]
    let hydration = HydrationOutcome::default();
    let hydrate_ms = t_hydrate.elapsed().as_secs_f64() * 1e3;

    // Bound concurrent scoring passes — see `AppState::search_semaphore`.
    // Acquired after hydration (which has its own ceiling) so a search
    // waiting on S3 doesn't also hold a scoring slot.
    let t_queue = Instant::now();
    let Some(_slot) = state.search_semaphore.acquire_or_shed() else {
        // Shed rather than queue: see `Semaphore::acquire_or_shed`.
        return json_error(
            429,
            "search overloaded: scoring queue is saturated — retry with backoff",
        );
    };
    let queue_ms = t_queue.elapsed().as_secs_f64() * 1e3;

    // On-demand page documents — see `handle_search_post` and
    // `hydrate_page_docs`.
    #[cfg(feature = "s3")]
    let page_fetch = Mutex::new(doc_spans::PageFetchStats::default());
    #[cfg(feature = "s3")]
    let ensure_doc_store = |pages: &[PageSegment<'_>]| {
        // Same accountant as the scoring-set fetch above: the request pays
        // for its page's bytes, so the request's ceiling has to cover them.
        let stats = state.hydrate_page_docs(pages, &hydration_request);
        page_fetch.lock().unwrap().add(&stats);
    };
    #[cfg(feature = "s3")]
    let doc_store_hydrator: DocStoreHydrator<'_> = Some(&ensure_doc_store);
    #[cfg(not(feature = "s3"))]
    let doc_store_hydrator: DocStoreHydrator<'_> = None;
    // kNN postings a segment probes but does not hold: fetched by range
    // into the span cache, on the same accountant (#245).
    #[cfg(feature = "s3")]
    let vector_fetch_stats = Mutex::new(posting_spans::PostingFetchStats::default());
    #[cfg(feature = "s3")]
    let fetch_vectors = |seg: &Path, file: &str, ranges: &[(u64, u64)]| {
        let stats = state.hydrate_vector_postings(seg, file, ranges, &hydration_request);
        vector_fetch_stats.lock().unwrap().add(&stats);
    };
    #[cfg(feature = "s3")]
    let vector_fetch: Option<kosha_query::VectorSpanFetch<'_>> = Some(&fetch_vectors);
    #[cfg(not(feature = "s3"))]
    let vector_fetch: Option<kosha_query::VectorSpanFetch<'_>> = None;

    match state.searcher.search_with_hydrators(
        &ns,
        manifest,
        &query,
        tombstones,
        doc_store_hydrator,
        vector_fetch,
    ) {
        Ok((result, phases)) => {
            #[cfg(feature = "s3")]
            let hydration = HydrationOutcome {
                page: *page_fetch.lock().unwrap(),
                vectors: *vector_fetch_stats.lock().unwrap(),
                ..hydration
            };
            log_search_timing(
                &ns,
                &hydration,
                hydrate_ms,
                queue_ms,
                &phases,
                result.total_hits,
                result.knn_degraded_segments,
            );
            state.record_search_access(&ns, manifest, &query, &phases, &hydration);
            json_ok(&result)
        }
        Err(e) => {
            // Same verdict invalidation as `handle_search_post` — see
            // that function for the rationale (a stale verdict could
            // cause the retry to also skip hydration and fail again).
            #[cfg(feature = "s3")]
            {
                state
                    .completed_hydrations
                    .lock()
                    .unwrap()
                    .remove(&(ns.0.clone(), manifest.version));
                // A page-materialize fetch that crossed the request's byte
                // ceiling leaves the doc store absent, which surfaces here
                // as an IO error. It is a shed, not a fault — report it as
                // one.
                if hydration_request.is_shed() {
                    return hydration_shed_response();
                }
            }
            search_error_response(&e)
        }
    }
}

// ─── v1 route handlers (proto-defined paths, tenant-scoped namespaces) ─────

fn handle_index_with_ns(namespace: &str, tenant: &str, body: &[u8], state: &AppState) -> String {
    let scoped_ns = tenant_namespace(tenant, namespace);
    let mut body_val: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => body_val_fallback(body),
    };
    if let Some(obj) = body_val.as_object_mut() {
        obj.insert("namespace".into(), serde_json::json!(scoped_ns));
    }
    let modified_body = serde_json::to_vec(&body_val).unwrap_or_else(|_| body.to_vec());
    handle_index(&modified_body, state)
}

/// `POST /v1/namespaces/{ns}/schema` — set the namespace schema (#220).
/// The body is a proto `NamespaceSchema` (`{"fields": {...}}`), or one
/// wrapped as `{"schema": {...}}` like `CreateNamespaceRequest`. Only the
/// analysis settings are honored. New segments use it; existing segments
/// keep the analysis their footer records and stay searchable with it, so
/// the change reaches old documents when compaction rewrites them.
fn handle_put_schema_with_ns(
    namespace: &str,
    tenant: &str,
    body: &[u8],
    state: &AppState,
) -> String {
    let ns = NamespaceId(tenant_namespace(tenant, namespace));
    let value: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return json_error(400, "invalid JSON"),
    };
    let value = match value.get("schema") {
        Some(inner) => inner.clone(),
        None => value,
    };
    let schema: kosha_core::NamespaceSchema = match serde_json::from_value(value) {
        Ok(schema) => schema,
        Err(e) => return json_error(400, &format!("invalid schema: {e}")),
    };
    let changed = match state.indexer.set_schema(&ns, schema.clone()) {
        Ok(changed) => changed,
        Err(KoshaError::InvalidQuery(message)) => return json_error(400, &message),
        Err(e) => return json_error(500, &format!("schema update failed: {e}")),
    };
    if changed {
        if let Err(e) = state.publish_namespace(&ns) {
            return json_error(500, &format!("publication failed: {e}"));
        }
    }
    json_ok(&serde_json::json!({
        "namespace": namespace,
        "schema": schema,
        "changed": changed,
    }))
}

/// `GET /v1/namespaces/{ns}/schema` — the namespace schema, `{}` if none.
fn handle_get_schema_with_ns(namespace: &str, tenant: &str, state: &AppState) -> String {
    let ns = NamespaceId(tenant_namespace(tenant, namespace));
    // An admin read: always confirm against the control store on a query
    // replica (no-op on the ingest pod).
    if let Err(e) = state.ensure_query_manifest_fresh(&ns, Some(Duration::ZERO)) {
        return json_error(404, &format!("{e}"));
    }
    match state.indexer.manifest_shared(&ns) {
        Some(manifest) => json_ok(&serde_json::json!({
            "namespace": namespace,
            "schema": manifest.schema.clone().unwrap_or_default(),
        })),
        None => json_error(404, "namespace not found"),
    }
}

fn handle_search_post_with_ns(
    namespace: &str,
    tenant: &str,
    body: &[u8],
    state: &AppState,
) -> String {
    let scoped_ns = tenant_namespace(tenant, namespace);
    let mut body_val: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return json_error(400, "invalid JSON"),
    };
    if let Some(obj) = body_val.as_object_mut() {
        obj.insert("namespace".into(), serde_json::json!(scoped_ns));
    }
    let modified_body = serde_json::to_vec(&body_val).unwrap_or_else(|_| body.to_vec());
    handle_search_post(&modified_body, state)
}

fn handle_flush_with_ns(namespace: &str, tenant: &str, body: &[u8], state: &AppState) -> String {
    let scoped_ns = tenant_namespace(tenant, namespace);
    let mut body_val: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => serde_json::json!({}),
    };
    if let Some(obj) = body_val.as_object_mut() {
        obj.insert("namespace".into(), serde_json::json!(scoped_ns));
    }
    let modified_body = serde_json::to_vec(&body_val).unwrap_or_else(|_| body.to_vec());
    handle_flush(&modified_body, state)
}

fn handle_delete_with_ns(namespace: &str, tenant: &str, body: &[u8], state: &AppState) -> String {
    let scoped_ns = tenant_namespace(tenant, namespace);
    let mut body_val: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return json_error(400, "invalid JSON"),
    };
    if let Some(obj) = body_val.as_object_mut() {
        obj.insert("namespace".into(), serde_json::json!(scoped_ns));
    }
    let modified_body = serde_json::to_vec(&body_val).unwrap_or_else(|_| body.to_vec());
    handle_delete(&modified_body, state)
}

fn handle_exists_with_ns(namespace: &str, tenant: &str, body: &[u8], state: &AppState) -> String {
    let scoped_ns = tenant_namespace(tenant, namespace);
    let mut req: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };
    if let Some(obj) = req.as_object_mut() {
        obj.insert("namespace".into(), serde_json::json!(scoped_ns));
    }
    let modified = serde_json::to_vec(&req).unwrap_or_else(|_| body.to_vec());
    handle_exists(&modified, state)
}

fn handle_namespace_stats(namespace: &str, tenant: &str, state: &AppState) -> String {
    let scoped_ns = tenant_namespace(tenant, namespace);
    // On a query replica, follow the control store: a namespace created or
    // deleted on the ingest node shows up (or 404s) here too.
    if let Err(e) =
        state.ensure_query_manifest_fresh(&NamespaceId(scoped_ns.clone()), Some(Duration::ZERO))
    {
        return match e {
            KoshaError::NamespaceNotFound(_) => {
                json_error(404, &format!("namespace '{namespace}' not found"))
            }
            e => json_error(500, &format!("stats error: {e}")),
        };
    }
    let indexer = &state.indexer;
    let manifest = match indexer.manifest(&NamespaceId(scoped_ns.clone())) {
        Some(m) => m,
        None => return json_error(404, &format!("namespace '{namespace}' not found")),
    };
    let ns_docs: u64 = manifest.segments.iter().map(|s| s.doc_count as u64).sum();
    let mut body = serde_json::json!({
        "namespace": namespace,
        "documents": ns_docs,
        "segments": manifest.segments.len(),
        "version": manifest.version,
        "manifest_version": manifest.version,
    });
    if state.ingest_host.is_some() {
        add_manifest_follow_stats(state, &NamespaceId(scoped_ns), &mut body);
    }
    json_ok(&body)
}

/// POST /v1/admin/rebuild-filter-blooms — rewrite footer blooms from segment files.
///
/// Request body: `{"namespace": "paragraph_index_hnsw"}` (tenant-scoped when
/// authenticated). Hydrates each segment, rebuilds `filter_blooms` (from
/// `filters.bin`) and `term_bloom` (from `inverted.idx`), and uploads the
/// updated `footer.json` to S3 when configured.
fn handle_rebuild_filter_blooms(body: &[u8], tenant: &str, state: &AppState) -> String {
    let req: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };
    let ns_raw = match req.get("namespace").and_then(|v| v.as_str()) {
        Some(n) => n,
        None => return json_error(400, "missing 'namespace'"),
    };
    // Prefer the name as given (migration uses bare index names); fall back
    // to tenant-scoped lookup used by v1 routes.
    let ns = {
        let indexer = &state.indexer;
        if ns_raw.contains('/') {
            let ns = NamespaceId(ns_raw.to_string());
            if indexer.manifest_cloned(&ns).is_some() {
                ns
            } else {
                return json_error(404, &format!("namespace '{}' not found", ns.0));
            }
        } else {
            let bare = NamespaceId(ns_raw.to_string());
            if indexer.manifest_cloned(&bare).is_some() {
                bare
            } else {
                let scoped = NamespaceId(tenant_namespace(tenant, ns_raw));
                if indexer.manifest_cloned(&scoped).is_some() {
                    scoped
                } else {
                    return json_error(404, &format!("namespace '{ns_raw}' not found"));
                }
            }
        }
    };

    let manifest = {
        let indexer = &state.indexer;
        indexer
            .manifest_cloned(&ns)
            .expect("namespace existence checked above")
    };

    // Hydrate every segment in one fanned-out batch rather than one
    // ensure_segment_local round trip per segment — see handle_index.
    let seg_paths: Vec<PathBuf> = manifest
        .segments
        .iter()
        .map(|entry| state.data_dir.join(&ns.0).join(&entry.segment_id.0))
        .collect();
    #[cfg(feature = "s3")]
    // Admin routes rewrite or merge segments — must preserve vectors.
    state.ensure_segments_local(&seg_paths, true);

    let mut new_footers = Vec::new();
    let mut rebuilt = 0usize;
    let mut errors = Vec::new();
    for (entry, seg_path) in manifest.segments.iter().zip(seg_paths.iter()) {
        if !seg_path.exists() {
            errors.push(format!("{}: segment not local", entry.segment_id.0));
            continue;
        }
        match SegmentReader::rewrite_footer_blooms(seg_path) {
            Ok(footer) => {
                rebuilt += 1;
                #[cfg(feature = "s3")]
                if let Err(e) = state.sync_files_to_s3(seg_path, &["footer.json"]) {
                    errors.push(format!(
                        "{}: rebuilt locally but S3 sync failed: {e}",
                        entry.segment_id.0
                    ));
                    continue;
                }
                // The CRC of the rewritten file travels in the persisted
                // manifest (footers themselves don't, #241) so replicas
                // drop their cached footer and stale local footer.json.
                match std::fs::read(seg_path.join("footer.json")) {
                    Ok(bytes) => new_footers.push((footer, crc32fast::hash(&bytes))),
                    Err(e) => errors.push(format!(
                        "{}: rebuilt but footer.json unreadable: {e}",
                        entry.segment_id.0
                    )),
                }
            }
            Err(e) => errors.push(format!("{}: {e}", entry.segment_id.0)),
        }
    }

    // Searches prune with the footer copies inside the manifest, not the
    // footer.json files just rewritten. Without installing the new footers
    // and publishing, the rebuild changed files nothing reads: this node
    // kept pruning with the old blooms, and so did every query replica.
    if state.indexer.replace_segment_footers(&ns, new_footers) > 0 {
        if let Err(e) = state.publish_namespace(&ns) {
            errors.push(format!("blooms rebuilt but manifest publish failed: {e}"));
        }
    }

    json_ok(&serde_json::json!({
        "namespace": ns.0,
        "segments": manifest.segments.len(),
        "rebuilt": rebuilt,
        "errors": errors,
    }))
}

fn handle_backfill_offset_tables(body: &[u8], tenant: &str, state: &AppState) -> String {
    let req: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };
    let ns_raw = match req.get("namespace").and_then(|v| v.as_str()) {
        Some(n) => n,
        None => return json_error(400, "missing 'namespace'"),
    };
    // Prefer the name as given (migration uses bare index names); fall back
    // to tenant-scoped lookup used by v1 routes.
    let ns = {
        let indexer = &state.indexer;
        if ns_raw.contains('/') {
            let ns = NamespaceId(ns_raw.to_string());
            if indexer.manifest_cloned(&ns).is_some() {
                ns
            } else {
                return json_error(404, &format!("namespace '{}' not found", ns.0));
            }
        } else {
            let bare = NamespaceId(ns_raw.to_string());
            if indexer.manifest_cloned(&bare).is_some() {
                bare
            } else {
                let scoped = NamespaceId(tenant_namespace(tenant, ns_raw));
                if indexer.manifest_cloned(&scoped).is_some() {
                    scoped
                } else {
                    return json_error(404, &format!("namespace '{ns_raw}' not found"));
                }
            }
        }
    };

    let manifest = {
        let indexer = &state.indexer;
        indexer
            .manifest_cloned(&ns)
            .expect("namespace existence checked above")
    };
    let total = manifest.segments.len();

    // The work runs on a background thread: hydrating + parsing a legacy
    // namespace's doc stores takes minutes — far past the query→ingest
    // forward proxy's timeout and the HTTP io timeout, which is why the
    // previous synchronous version always answered into a dead socket with
    // no way to observe what happened.
    let Some(job_state) = state.self_ref.upgrade() else {
        return json_error(500, "server not initialized for background jobs");
    };
    {
        let mut jobs = state.backfill_jobs.lock().unwrap();
        if !jobs.insert(ns.0.clone()) {
            return json_ok(&serde_json::json!({
                "namespace": ns.0,
                "status": "already_running",
                "segments": total,
            }));
        }
    }

    let job_ns = ns.clone();
    std::thread::spawn(move || {
        let state = job_state;
        let ns = job_ns;
        let mut done = 0usize;
        let mut skipped = 0usize;
        let mut failed = 0usize;
        for (i, entry) in manifest.segments.iter().enumerate() {
            let seg_path = state.data_dir.join(&ns.0).join(&entry.segment_id.0);

            // Resumable: a segment whose sidecar already reached S3 is done —
            // re-running after a crash/restart only pays for the remainder.
            #[cfg(feature = "s3")]
            let already_done = state
                .s3_storage
                .as_ref()
                .map(|s3| {
                    let rel = format!("{}/{}", ns.0, entry.segment_id.0);
                    s3.list_with_sizes(&rel)
                        .map(|files| files.iter().any(|(n, _)| n == "doc_store.offsets"))
                        .unwrap_or(false)
                })
                .unwrap_or(false);
            #[cfg(not(feature = "s3"))]
            let already_done = seg_path.join("doc_store.offsets").is_file();
            if already_done {
                skipped += 1;
                continue;
            }

            // Per-segment fetch of only the two files the backfill actually
            // reads (doc_store.bin to derive spans, footer.json to bump
            // format_version) — not the whole segment, and not the whole
            // namespace up front: bounded disk pressure, and the cache can
            // evict processed segments while later ones stream in.
            #[cfg(feature = "s3")]
            {
                let backfill_request = RequestHydration::unlimited();
                state.ensure_files_local(
                    std::slice::from_ref(&seg_path),
                    "footer.json",
                    &backfill_request,
                );
                state.ensure_files_local(
                    std::slice::from_ref(&seg_path),
                    "doc_store.bin",
                    &backfill_request,
                );
            }

            match SegmentReader::backfill_offset_tables(&seg_path) {
                Ok(_) => {
                    #[cfg(feature = "s3")]
                    if let Err(e) =
                        state.sync_files_to_s3(&seg_path, &["doc_store.offsets", "footer.json"])
                    {
                        eprintln!(
                            "WARN: backfill: ns={} segment={} rewritten locally but S3 sync \
                             failed: {e}",
                            ns.0, entry.segment_id.0,
                        );
                    }
                    done += 1;
                    println!(
                        "backfill progress: ns={} segment={} ({}/{total}) ok",
                        ns.0,
                        entry.segment_id.0,
                        i + 1,
                    );
                }
                Err(e) => {
                    failed += 1;
                    eprintln!(
                        "WARN: backfill failed: ns={} segment={} ({}/{total}): {e}",
                        ns.0,
                        entry.segment_id.0,
                        i + 1,
                    );
                }
            }
        }
        println!(
            "backfill complete: ns={} backfilled={done} skipped={skipped} failed={failed} total={total}",
            ns.0,
        );
        state.backfill_jobs.lock().unwrap().remove(&ns.0);
    });

    json_ok(&serde_json::json!({
        "namespace": ns.0,
        "status": "started",
        "segments": total,
    }))
}

/// POST /v1/admin/gc — garbage-collect superseded segments and deletion
/// generations from the object store (#239).
///
/// Request body (all optional):
///   {"namespace": "tenant/ns",       // default: every namespace
///    "dry_run": true,                // default true: report only
///    "grace_seconds": 3600,          // default KOSHA_GC_GRACE
///    "orphan_grace_seconds": 86400}  // default KOSHA_GC_ORPHAN_GRACE
///
/// A dry run lists every object a real run would delete and the bytes it
/// would reclaim. A real run deletes each segment's `_COMMIT` first, then
/// its data files, and drops the deleted objects' retirement records from
/// the manifest. Anything the current manifest references is never a
/// candidate, and a namespace whose manifest version moves during the run
/// is left alone. Local copies are removed separately by each pod's local
/// GC, once no search can still read them.
fn handle_segment_gc(body: &[u8], state: &AppState) -> String {
    let req: segment_gc::GcRequest = if body.iter().all(u8::is_ascii_whitespace) {
        match serde_json::from_str("{}") {
            Ok(r) => r,
            Err(e) => return json_error(500, &e.to_string()),
        }
    } else {
        match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return json_error(400, &format!("invalid GC request: {e}")),
        }
    };
    let all = state.controller.lock().unwrap().list_namespaces();
    let namespaces = match &req.namespace {
        Some(name) => {
            let ns = NamespaceId(name.clone());
            if !all.contains(&ns) {
                return json_error(404, &format!("namespace '{name}' not found"));
            }
            vec![ns]
        }
        None => all,
    };
    let grace = req
        .grace_seconds
        .map(Duration::from_secs)
        .unwrap_or(state.gc_settings.grace);
    let orphan_grace = req
        .orphan_grace_seconds
        .map(Duration::from_secs)
        .unwrap_or(state.gc_settings.orphan_grace);
    let config = kosha_core::gc::GcConfig {
        dry_run: req.dry_run,
        grace_ms: grace.as_millis() as u64,
        orphan_grace_ms: orphan_grace.as_millis() as u64,
        now_ms: unix_millis(),
    };
    let reports = state.run_segment_gc(&namespaces, &config);
    let sum = |f: fn(&kosha_core::gc::NamespaceGcReport) -> u64| reports.iter().map(f).sum::<u64>();
    json_ok(&serde_json::json!({
        "dry_run": req.dry_run,
        "grace_seconds": grace.as_secs(),
        "orphan_grace_seconds": orphan_grace.as_secs(),
        "candidates": sum(|r| r.candidates.len() as u64),
        "reclaimable_bytes": sum(|r| r.reclaimable_bytes),
        "segments_deleted": sum(|r| r.segments_deleted as u64),
        "deletion_vectors_deleted": sum(|r| r.deletion_vectors_deleted as u64),
        "bytes_reclaimed": sum(|r| r.bytes_reclaimed),
        "aborted_namespaces": reports.iter().filter(|r| r.aborted.is_some()).count(),
        "namespaces": reports,
        "stats": state.gc_stats.to_json(
            state.indexer.retired_segment_count(),
            state.indexer.local_retired_count(),
        ),
    }))
}

/// POST /v1/admin/compact-namespace — run one compaction pass on a namespace.
///
/// Defaults to size-tiered compaction (DESIGN.md §7.1): only small segments
/// are merged, under the indexer's `CompactionPolicy`. Pass
/// `"mode": "full"` for emergency all-to-one merge. Merge I/O does not hold
/// a process-wide indexer lock; only the target namespace's compact lock is
/// held for the duration.
///
/// Request body: {"namespace": "paragraph_index_hnsw", "mode": "tiered"|"full",
///                "max_segment_bytes": 5368709120,   // optional; 0 = uncapped
///                "async": true}                     // optional (#242)
///
/// Merged output size is capped (default 5GiB, Lucene-style). Each group is
/// planned from S3 object sizes before anything is hydrated, and only that
/// group is downloaded, merged, published and then evicted from local disk
/// (#242), so a pass needs about `2 x cap` of local disk, not the whole
/// namespace. `full` loops greedy smallest-first groups inside the one
/// pass until none fits under the cap (~ceil(total_bytes/cap) segments);
/// looping the endpoint until `segments_after` stops dropping still works
/// and is now a no-op after the first call. `max_segment_bytes: 0` restores
/// the legacy all-to-one merge, which hydrates everything.
///
/// `"async": true` answers `202` with a `job_id` at once and runs the pass
/// on a background thread; poll `GET /v1/admin/compaction-jobs/{job_id}`,
/// whose `progress` reports groups, segments merged and bytes hydrated and
/// evicted after every group. A `full` pass over a big namespace can run
/// for hours, far past the query→ingest proxy and HTTP timeouts. Without
/// `async` the call blocks as before. Either way the pass is registered with
/// the compaction scheduler: a second pass on the same namespace gets `409`
/// with the running job's id.
fn handle_compact_namespace(body: &[u8], tenant: &str, state: &AppState) -> String {
    let req: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };
    let ns_raw = match req.get("namespace").and_then(|v| v.as_str()) {
        Some(n) => n,
        None => return json_error(400, "missing 'namespace'"),
    };
    let mode = match req.get("mode").and_then(|v| v.as_str()).unwrap_or("tiered") {
        "tiered" => kosha_write::CompactMode::Tiered,
        "full" => kosha_write::CompactMode::Full,
        other => {
            return json_error(
                400,
                &format!("invalid mode '{other}' (expected tiered|full)"),
            );
        }
    };
    let run_async = req.get("async").and_then(|v| v.as_bool()).unwrap_or(false);
    // Prefer the name as given (migration uses bare index names); fall back
    // to tenant-scoped lookup used by v1 routes.
    let ns = {
        if ns_raw.contains('/') {
            let ns = NamespaceId(ns_raw.to_string());
            if state.indexer.manifest_cloned(&ns).is_some() {
                ns
            } else {
                return json_error(404, &format!("namespace '{}' not found", ns.0));
            }
        } else {
            let bare = NamespaceId(ns_raw.to_string());
            if state.indexer.manifest_cloned(&bare).is_some() {
                bare
            } else {
                let scoped = NamespaceId(tenant_namespace(tenant, ns_raw));
                if state.indexer.manifest_cloned(&scoped).is_some() {
                    scoped
                } else {
                    return json_error(404, &format!("namespace '{ns_raw}' not found"));
                }
            }
        }
    };
    let opts = compact_options(
        state,
        mode,
        req.get("max_segment_bytes").and_then(|v| v.as_u64()),
    );

    // Resolve the background handle before registering, so a refusal can't
    // leave a job stuck in `running`.
    let job_state = if run_async {
        match state.self_ref.upgrade() {
            Some(s) => Some(s),
            None => return json_error(500, "server not initialized for background jobs"),
        }
    } else {
        None
    };
    let job_id = match state.compaction.try_start(
        &ns,
        compact_mode_name(mode),
        compaction_sched::Trigger::Api,
        Instant::now(),
    ) {
        Ok(id) => id,
        Err(running) => {
            return json_error(
                409,
                &format!(
                    "a compaction pass is already running on namespace '{}' (job {running}); \
                     poll GET /v1/admin/compaction-jobs/{running}",
                    ns.0
                ),
            );
        }
    };

    if let Some(job_state) = job_state {
        let response = serde_json::json!({
            "job_id": job_id,
            "namespace": ns.0,
            "mode": compact_mode_name(mode),
            "state": "running",
            "status_url": format!("/v1/admin/compaction-jobs/{job_id}"),
        });
        std::thread::Builder::new()
            .name("kosha-compaction-job".into())
            .spawn(move || run_compaction_job(&job_state, &ns, opts, &job_id))
            .expect("failed to spawn compaction job thread");
        return json_accepted(&response);
    }

    match run_compaction_job(state, &ns, opts, &job_id) {
        Ok(outcome) => json_ok(&serde_json::json!({
            "job_id": job_id,
            "namespace": ns.0,
            "mode": compact_mode_name(mode),
            "merged": outcome.merged,
            "segments_before": outcome.segments_before,
            "segments_after": outcome.segments_after,
            "segments_merged": outcome.segments_merged,
            "max_segment_bytes": outcome.max_segment_bytes,
            "not_hydrated": outcome.not_hydrated,
            "groups": outcome.groups,
            "bytes_hydrated": outcome.bytes_hydrated,
            "bytes_evicted": outcome.bytes_evicted,
            "peak_local_bytes": outcome.peak_local_bytes,
            "not_evicted": outcome.not_evicted,
        })),
        Err(error) => json_error(500, &error),
    }
}

/// GET /v1/admin/compaction-status — scheduler config and counters, every
/// known namespace's segment count and `needs_compaction`, plus running and
/// recent jobs. Served by the write pod (query pods forward it).
fn handle_compaction_status(state: &AppState) -> String {
    let live: Vec<(compaction_sched::Candidate, usize)> = state
        .indexer
        .namespaces()
        .into_iter()
        .filter_map(|ns| {
            let manifest = state.indexer.manifest_cloned(&ns)?;
            let needs = kosha_write::needs_compaction(&manifest, state.indexer.compaction_policy());
            Some((
                compaction_sched::Candidate {
                    namespace: ns,
                    manifest_version: manifest.version,
                    needs_compaction: needs,
                },
                manifest.segments.len(),
            ))
        })
        .collect();
    let forwards_writes = state.ingest_host.is_some();
    let enabled = !forwards_writes && state.compaction.config().interval.is_some();
    json_ok(&state.compaction.status_json(enabled, &live))
}

/// GET /v1/admin/compaction-jobs/{job_id} — one job's state and result.
/// Finished jobs are kept for the last `FINISHED_JOBS_RETAINED` passes.
fn handle_compaction_job(job_id: &str, state: &AppState) -> String {
    match state.compaction.job(job_id) {
        Some(job) => json_ok(&job),
        None => json_error(404, &format!("compaction job '{job_id}' not found")),
    }
}

fn compact_mode_name(mode: kosha_write::CompactMode) -> &'static str {
    match mode {
        kosha_write::CompactMode::Tiered => "tiered",
        kosha_write::CompactMode::Full => "full",
    }
}

/// The indexer's policy with the merge-size cap resolved. Precedence:
/// policy default (5 GiB) < `KOSHA_COMPACT_MAX_SEGMENT_BYTES` < request
/// body. `0` disables the cap (the legacy all-to-one full merge, the
/// 167→1 merge that put the 10M bench box into reclaim stalls; opt back in
/// explicitly if that's really wanted).
fn compact_options(
    state: &AppState,
    mode: kosha_write::CompactMode,
    request_max_segment_bytes: Option<u64>,
) -> kosha_write::CompactOptions {
    let mut policy = state.indexer.compaction_policy().clone();
    if let Some(n) = std::env::var("KOSHA_COMPACT_MAX_SEGMENT_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
    {
        policy.max_merged_segment_bytes = n;
    }
    if let Some(n) = request_max_segment_bytes {
        policy.max_merged_segment_bytes = n;
    }
    kosha_write::CompactOptions {
        mode,
        policy,
        vector_index: None,
    }
}

/// Run one registered compaction pass and record its outcome. The caller
/// must already hold the job's slot from `CompactionRegistry::try_start`.
fn run_compaction_job(
    state: &AppState,
    ns: &NamespaceId,
    opts: kosha_write::CompactOptions,
    job_id: &str,
) -> Result<compaction_sched::PassOutcome, String> {
    let started = Instant::now();
    let mode = compact_mode_name(opts.mode);
    let outcome = compaction_pass(state, ns, opts, job_id);
    let version = state.indexer.manifest_version(ns).unwrap_or(0);
    let elapsed_ms = started.elapsed().as_millis();
    match &outcome {
        Ok(o) => println!(
            "compaction: job={job_id} ns={} mode={mode} merged={} segments {}→{} \
             (merged {} in {} group(s)) hydrated={}B evicted={}B peak_local={}B \
             not_hydrated={} not_evicted={} in {elapsed_ms} ms",
            ns.0,
            o.merged,
            o.segments_before,
            o.segments_after,
            o.segments_merged,
            o.groups,
            o.bytes_hydrated,
            o.bytes_evicted,
            o.peak_local_bytes,
            o.not_hydrated.len(),
            o.not_evicted.len(),
        ),
        Err(e) => eprintln!(
            "compaction: job={job_id} ns={} mode={mode} failed after {elapsed_ms} ms: {e}",
            ns.0
        ),
    }
    state
        .compaction
        .finish(job_id, outcome.clone(), version, Instant::now());
    outcome
}

/// Where a compaction pass learns segment sizes, hydrates its inputs and
/// whether it may evict them afterwards. `AppState` is the real one (S3
/// when configured, local disk otherwise); tests swap in a fake object
/// store so the plan/hydrate/evict sequence runs without S3.
trait CompactionStorage {
    /// Bytes each manifest segment would occupy once hydrated (with
    /// vectors), learned without downloading it. A segment missing from
    /// the map has an unknown size and never joins a capped group.
    fn segment_sizes(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
    ) -> HashMap<kosha_core::SegmentId, u64>;
    /// Make these segment directories local, vectors included.
    fn hydrate(&self, seg_paths: &[PathBuf]);
    /// Whether superseded inputs live on in object storage, so their local
    /// copies are only a cache and may be removed after the merge
    /// publishes. Local-only mode returns `false`: there the directory is
    /// the only copy, and deleting it is segment GC, not cache eviction.
    fn evicts_superseded(&self) -> bool;
}

impl CompactionStorage for AppState {
    fn segment_sizes(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
    ) -> HashMap<kosha_core::SegmentId, u64> {
        #[cfg_attr(not(feature = "s3"), allow(unused_mut))]
        let mut remote: HashMap<String, u64> = HashMap::new();
        #[cfg(feature = "s3")]
        if let Some(ref s3) = self.s3_storage {
            // Compaction rewrites segments, so it hydrates (and must size)
            // vectors too.
            match s3.segment_sizes(&ns.0, |file| hydration_wants_file(file, true)) {
                Ok(sizes) => remote = sizes,
                Err(e) => eprintln!(
                    "WARN: compaction: S3 size listing failed for {}: {e}; \
                     only local segments can be planned",
                    ns.0
                ),
            }
        }
        let ns_dir = self.data_dir.join(&ns.0);
        manifest
            .segments
            .iter()
            .filter_map(|entry| {
                let id = &entry.segment_id;
                // S3 is authoritative: a local copy may be partial (a
                // lexical search hydrates without vector.idx). Local size
                // only for segments S3 doesn't list, i.e. local-only mode
                // or a flush not yet published.
                let bytes = remote
                    .get(&id.0)
                    .copied()
                    .filter(|b| *b > 0)
                    .or_else(|| local_dir_bytes(&ns_dir.join(&id.0)))?;
                Some((id.clone(), bytes))
            })
            .collect()
    }

    fn hydrate(&self, seg_paths: &[PathBuf]) {
        #[cfg(feature = "s3")]
        self.ensure_segments_local(seg_paths, true);
        let _ = seg_paths;
    }

    fn evicts_superseded(&self) -> bool {
        #[cfg(feature = "s3")]
        {
            self.s3_storage.is_some()
        }
        #[cfg(not(feature = "s3"))]
        {
            false
        }
    }
}

/// Total bytes of the files under a local directory (recursively); `None`
/// when it does not exist.
fn local_dir_bytes(dir: &std::path::Path) -> Option<u64> {
    fn walk(dir: &std::path::Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if meta.is_dir() {
                walk(&entry.path(), total);
            } else {
                *total = total.saturating_add(meta.len());
            }
        }
    }
    if !dir.is_dir() {
        return None;
    }
    let mut total = 0;
    walk(dir, &mut total);
    Some(total)
}

/// One compaction pass plus publication.
///
/// Plan, then hydrate (#242): each merge group is chosen from segment
/// sizes in object storage (`CompactionStorage::segment_sizes`) before
/// anything is downloaded, only that group is hydrated, merged and
/// published, and then the inputs, now superseded, leave local disk. Peak
/// local disk for a group is its inputs plus its output, about
/// `2 x max_segment_bytes`, however big the namespace is. The old `full`
/// pass hydrated every segment first (22 GiB on staging).
///
/// A `tiered` pass merges at most one group. A `full` pass loops group by
/// group until no group fits under the cap; every group is published on
/// its own, so a pass that fails halfway keeps the groups it finished.
/// `max_segment_bytes: 0` (uncapped) is the legacy all-to-one merge and
/// still hydrates every segment. Planned inputs that fail to hydrate are
/// left out of the merge and reported in `not_hydrated`.
fn compaction_pass(
    state: &AppState,
    ns: &NamespaceId,
    opts: kosha_write::CompactOptions,
    job_id: &str,
) -> Result<compaction_sched::PassOutcome, String> {
    compaction_pass_with(state, state, ns, opts, |progress| {
        state.compaction.progress(job_id, progress);
    })
}

fn compaction_pass_with(
    state: &AppState,
    storage: &dyn CompactionStorage,
    ns: &NamespaceId,
    opts: kosha_write::CompactOptions,
    mut on_group: impl FnMut(&compaction_sched::PassOutcome),
) -> Result<compaction_sched::PassOutcome, String> {
    let segments_before = state
        .indexer
        .manifest_cloned(ns)
        .ok_or_else(|| format!("namespace '{}' not found", ns.0))?
        .segments
        .len();
    let ns_dir = state.data_dir.join(&ns.0);
    let mut outcome = compaction_sched::PassOutcome {
        merged: false,
        segments_before,
        segments_after: segments_before,
        segments_merged: 0,
        max_segment_bytes: opts.policy.max_merged_segment_bytes,
        not_hydrated: Vec::new(),
        groups: 0,
        bytes_hydrated: 0,
        bytes_evicted: 0,
        peak_local_bytes: 0,
        not_evicted: Vec::new(),
    };
    let group_bytes = |ids: &[kosha_core::SegmentId]| -> u64 {
        ids.iter()
            .filter_map(|id| local_dir_bytes(&ns_dir.join(&id.0)))
            .sum()
    };

    loop {
        let Some(manifest) = state.indexer.manifest_cloned(ns) else {
            break;
        };
        let sizes = storage.segment_sizes(ns, &manifest);
        let Some(plan) =
            kosha_write::plan_merge_group(&manifest, &opts, |id| sizes.get(id).copied())
        else {
            break;
        };
        drop(manifest);
        let ids = plan.input_ids();

        // Hydrate only this group, in one fanned-out batch (see
        // `ensure_segments_local`). Compaction rewrites segments, so it
        // must preserve vectors.
        let local_before = group_bytes(&ids);
        let paths: Vec<PathBuf> = ids.iter().map(|id| ns_dir.join(&id.0)).collect();
        storage.hydrate(&paths);
        let local_inputs = group_bytes(&ids);
        outcome.bytes_hydrated += local_inputs.saturating_sub(local_before);
        for path in &paths {
            if !path.exists() {
                if let Some(name) = path.file_name() {
                    let name = name.to_string_lossy().into_owned();
                    if !outcome.not_hydrated.contains(&name) {
                        outcome.not_hydrated.push(name);
                    }
                }
            }
        }

        let result = state
            .indexer
            .compact_planned(ns, &plan)
            .map_err(|e| format!("compaction error: {e}"))?;
        if !result.merged {
            // Hydration failed for all but one input, or a concurrent
            // manifest change removed an input. Re-planning would pick the
            // same group again.
            break;
        }

        // Persist the merged manifest and upload the new segment to S3,
        // the same path a normal flush uses.
        state
            .publish_namespace(ns)
            .map_err(|e| format!("publication failed: {e}"))?;

        let output_bytes = result
            .output
            .as_ref()
            .and_then(|id| local_dir_bytes(&ns_dir.join(&id.0)))
            .unwrap_or(0);
        outcome.peak_local_bytes = outcome
            .peak_local_bytes
            .max(local_inputs.saturating_add(output_bytes));
        if storage.evicts_superseded() {
            let (evicted, kept) = evict_superseded_inputs(state, ns, &result.merged_inputs);
            outcome.bytes_evicted += evicted;
            outcome.not_evicted.extend(kept);
        }

        outcome.merged = true;
        outcome.groups += 1;
        outcome.segments_merged += result.segments_merged;
        outcome.segments_after = state
            .indexer
            .manifest_cloned(ns)
            .map(|m| m.segments.len())
            .unwrap_or(0);
        on_group(&outcome);

        if opts.mode != kosha_write::CompactMode::Full || opts.policy.max_merged_segment_bytes == 0
        {
            break;
        }
    }

    outcome.segments_after = state
        .indexer
        .manifest_cloned(ns)
        .map(|m| m.segments.len())
        .unwrap_or(0);
    Ok(outcome)
}

/// Remove the local copies of merge inputs a published merge superseded.
/// Returns the bytes freed and the segments kept: one the manifest still
/// references (never evicted), or one with a file pinned by an in-flight
/// hydration or warmup (`Cache::evict_dir_unless_pinned`). A kept copy is
/// harmless; it is just cache that ordinary LRU eviction will reclaim.
///
/// A search still running against the pre-merge manifest that needs an
/// evicted segment re-hydrates it from S3: removal here only drops cache,
/// and the S3 objects are untouched.
fn evict_superseded_inputs(
    state: &AppState,
    ns: &NamespaceId,
    inputs: &[kosha_core::SegmentId],
) -> (u64, Vec<String>) {
    let live: std::collections::HashSet<kosha_core::SegmentId> = state
        .indexer
        .manifest_cloned(ns)
        .map(|m| m.segments.into_iter().map(|e| e.segment_id).collect())
        .unwrap_or_default();
    let mut freed = 0u64;
    let mut kept = Vec::new();
    for id in inputs {
        if live.contains(id) {
            kept.push(id.0.clone());
            continue;
        }
        match state
            .cache
            .evict_dir_unless_pinned(&format!("{}/{}", ns.0, id.0))
        {
            Ok(Some(bytes)) => freed += bytes,
            Ok(None) => kept.push(id.0.clone()),
            Err(e) => {
                eprintln!(
                    "WARN: compaction: could not evict superseded segment {}/{}: {e}",
                    ns.0, id.0
                );
                kept.push(id.0.clone());
            }
        }
    }
    (freed, kept)
}

/// One scheduler tick: find namespaces `needs_compaction` flags, let the
/// registry pick the due ones under the global budget, and start a tiered
/// pass on a background thread for each. Never blocks on a merge.
fn run_compaction_scheduler_tick(state: &Arc<AppState>, now: Instant) -> Vec<NamespaceId> {
    let policy = state.indexer.compaction_policy().clone();
    let candidates: Vec<compaction_sched::Candidate> = state
        .indexer
        .namespaces()
        .into_iter()
        .filter_map(|ns| {
            let manifest = state.indexer.manifest_cloned(&ns)?;
            Some(compaction_sched::Candidate {
                needs_compaction: kosha_write::needs_compaction(&manifest, &policy),
                manifest_version: manifest.version,
                namespace: ns,
            })
        })
        .collect();
    let mut started = Vec::new();
    for ns in state.compaction.select_due(&candidates, now) {
        let job_id = match state.compaction.try_start(
            &ns,
            "tiered",
            compaction_sched::Trigger::Scheduler,
            now,
        ) {
            Ok(id) => id,
            Err(_) => continue, // an API job got there first
        };
        let opts = compact_options(state, kosha_write::CompactMode::Tiered, None);
        let job_state = Arc::clone(state);
        let job_ns = ns.clone();
        let spawned = std::thread::Builder::new()
            .name("kosha-compaction-job".into())
            .spawn(move || {
                let _ = run_compaction_job(&job_state, &job_ns, opts, &job_id);
            });
        if let Err(e) = spawned {
            eprintln!(
                "compaction scheduler: failed to spawn pass for {}: {e}",
                ns.0
            );
            continue;
        }
        started.push(ns);
    }
    started
}

/// Build an import manifest from recovered segment footers: entries sorted
/// by segment id (deterministic regardless of S3 listing order), doc counts
/// from the footers themselves, version 1 (imports start a fresh version
/// history — the old one is what was lost).
fn manifest_from_footers(footers: Vec<kosha_core::Footer>) -> kosha_core::Manifest {
    let mut footers = footers;
    footers.sort_by(|a, b| a.segment_id.0.cmp(&b.segment_id.0));
    let mut segments = Vec::with_capacity(footers.len());
    let mut segment_footers = std::collections::HashMap::with_capacity(footers.len());
    for footer in footers {
        segments.push(kosha_core::ManifestEntry {
            segment_id: footer.segment_id.clone(),
            doc_count: footer.doc_count,
        });
        segment_footers.insert(footer.segment_id.clone(), std::sync::Arc::new(footer));
    }
    kosha_core::Manifest {
        version: 1,
        segments,
        segment_footers,
        footer_crc32: Default::default(),
        wal_checkpoint: 0,
        wal_origin: None,
        tombstones: Default::default(),
        deletion_vectors: Default::default(),
        schema: None,
        retired: Default::default(),
    }
}

/// POST /v1/admin/import-namespace — rebuild a namespace's control-plane
/// manifest from its segment objects in S3.
///
/// Disaster recovery and snapshot restore: object storage survives while
/// the control plane is lost (dropped postgres, restored-from-snapshot
/// bench stack, region migration). Every fact the manifest needs already
/// lives in the segments' `footer.json` files — this lists the namespace's
/// S3 prefix, fetches every footer, rebuilds the manifest, and persists it
/// through the normal control-store path.
///
/// Request body: {"namespace": "...", "overwrite": false}
/// Refuses (409) if the namespace already has segments unless
/// `"overwrite": true` — a live namespace's history must never be
/// clobbered by an accidental import.
#[allow(unused_variables)]
fn handle_import_namespace(body: &[u8], _tenant: &str, state: &AppState) -> String {
    #[cfg(not(feature = "s3"))]
    {
        json_error(400, "import-namespace requires the s3 feature")
    }
    #[cfg(feature = "s3")]
    {
        let req: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
        };
        let Some(ns_raw) = req.get("namespace").and_then(|v| v.as_str()) else {
            return json_error(400, "missing 'namespace'");
        };
        let overwrite = req
            .get("overwrite")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let ns = NamespaceId(ns_raw.to_string());

        if !overwrite {
            let published = state.controller.lock().unwrap().manifest_cloned(&ns);
            if let Some(m) = published.or_else(|| state.indexer.manifest_cloned(&ns)) {
                if !m.segments.is_empty() {
                    return json_error(
                        409,
                        &format!(
                            "namespace '{}' already has {} segment(s); pass \"overwrite\": true \
                             to replace its manifest",
                            ns.0,
                            m.segments.len()
                        ),
                    );
                }
            }
        }

        let Some(ref s3) = state.s3_storage else {
            return json_error(400, "S3 not configured on this pod");
        };
        // Segment ids are the namespace prefix's child "directories"
        // (paginated common-prefix listing — `list_with_sizes` filters out
        // nested keys and reads one page, so it can't enumerate segments).
        let mut seg_ids = match s3.list_child_dirs(&ns.0) {
            Ok(l) => l,
            Err(e) => return json_error(500, &format!("S3 list failed: {e}")),
        };
        seg_ids.sort();
        seg_ids.dedup();
        if seg_ids.is_empty() {
            return json_error(
                404,
                &format!("no segment directories found under '{}/' in S3", ns.0),
            );
        }

        let footer_paths: Vec<String> = seg_ids
            .iter()
            .map(|seg| format!("{}/{}/footer.json", ns.0, seg))
            .collect();
        s3.read_many(&footer_paths, 16, None, &RequestHydration::unlimited());

        let mut footers = Vec::with_capacity(seg_ids.len());
        let mut unreadable = Vec::new();
        for (seg, rel) in seg_ids.iter().zip(&footer_paths) {
            let local = state.data_dir.join(rel);
            match std::fs::read(&local)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<kosha_core::Footer>(&bytes).ok())
            {
                Some(f) => footers.push(f),
                None => unreadable.push(seg.clone()),
            }
        }
        // All-or-nothing: a partial import silently shrinks the namespace,
        // which is exactly the silent-data-loss class this endpoint exists
        // to recover from.
        if !unreadable.is_empty() {
            return json_error(
                500,
                &format!(
                    "{} of {} segment footer(s) unreadable ({}); refusing partial import",
                    unreadable.len(),
                    seg_ids.len(),
                    unreadable.join(", ")
                ),
            );
        }

        let mut manifest = manifest_from_footers(footers);
        // Footers carry each segment's analysis, so search needs nothing
        // else; keep a schema this node already knows for future writes.
        manifest.schema = state
            .indexer
            .manifest_cloned(&ns)
            .and_then(|existing| existing.schema);
        let segments = manifest.segments.len();
        let documents: u64 = manifest.segments.iter().map(|e| e.doc_count as u64).sum();
        {
            let mut ctrl = state.controller.lock().unwrap();
            ctrl.ensure_namespace(ns.clone());
        }
        if let Err(e) = state.import_manifest(&ns, manifest) {
            return publication_error("publication failed", &e);
        }

        json_ok(&serde_json::json!({
            "namespace": ns.0,
            "segments": segments,
            "documents": documents,
        }))
    }
}

/// POST /v1/admin/api-keys — create a new API key for a tenant.
///
/// Requires the dedicated KOSHA_ADMIN_API_KEY credential.
/// Request body: {"tenant_id": "acme-corp", "description": "staging key"}
fn handle_create_api_key(body: &[u8], _tenant: &str, state: &AppState) -> String {
    let req: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return json_error(400, "invalid JSON"),
    };

    let tenant_id = match req.get("tenant_id").and_then(|v| v.as_str()) {
        Some(t) => t,
        None => return json_error(400, "missing 'tenant_id'"),
    };

    let description = req
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // Create via the shared Postgres pool (only with the postgres feature).
    #[cfg(feature = "postgres")]
    {
        let _ = state;
        let auth::Auth::Keys(store) = auth::global() else {
            return json_error(400, "API keys are disabled (KOSHA_AUTH=disabled)");
        };
        if std::env::var("DATABASE_URL").is_err() {
            return json_error(400, "Postgres not configured (DATABASE_URL not set)");
        }
        let Some(pg) = store.pg() else {
            return json_error(500, "failed to connect to postgres");
        };
        match pg.create_api_key(tenant_id, description) {
            Ok(api_key) => {
                // Usable on this pod right away; other pods pick it up on
                // their next key refresh.
                store.insert_db_key(api_key.clone(), tenant_id.to_string());
                json_ok(&serde_json::json!({
                    "api_key": api_key,
                    "tenant_id": tenant_id,
                    "description": description,
                }))
            }
            Err(e) => json_error(500, &format!("failed to create key: {e}")),
        }
    }

    #[cfg(not(feature = "postgres"))]
    {
        let _ = tenant_id;
        let _ = description;
        json_error(
            400,
            "admin API requires postgres feature (compile with --features postgres)",
        )
    }
}

/// If the body is not valid JSON, try parsing as a raw object with just the documents field.
fn body_val_fallback(body: &[u8]) -> serde_json::Value {
    // Try parsing as a JSON object with a documents array
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) {
        return v;
    }
    serde_json::json!({})
}

/// What one segment needs hydrated for a search — see
/// [`segment_search_hydration`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(feature = "s3")]
enum SegmentHydration {
    /// The full scoring set: the segment may hold a hit.
    Scoring,
    /// Only the inverted index and the query terms' posting blobs: the
    /// segment can't hold a hit (its filter or AND-term bloom rules it
    /// out), but it may contain a query term, and namespace-level BM25
    /// statistics (issue #246) count its documents in that term's `df`.
    StatsOnly,
    /// Nothing.
    Skip,
}

/// Decide what a segment needs hydrated for `query` from its footer's
/// blooms. `stats_terms` is [`kosha_query::namespace_stats_terms`]: when
/// set, a segment the query's own pruning skips is still hydrated for
/// `df` if its term bloom admits any one of them (OR — a segment holding
/// only some terms of an AND query still contributes to their `df`).
/// No footer means it can't be pruned, so it gets the full scoring set.
#[cfg(feature = "s3")]
fn segment_search_hydration(
    query: &SearchQuery,
    needs_bloom_check: bool,
    term_prune: Option<(&kosha_query::QueryAnalysis, TermBloomMode)>,
    stats_terms: Option<&[String]>,
    footer: Option<&kosha_core::Footer>,
) -> SegmentHydration {
    let Some(footer) = footer.filter(|_| needs_bloom_check) else {
        return SegmentHydration::Scoring;
    };
    let admitted = query.segment_may_match_filters(footer.filter_blooms.as_ref())
        && term_prune.is_none_or(|(analysis, mode)| {
            kosha_query::segment_may_match_query_terms(analysis, mode, footer)
        });
    if admitted {
        SegmentHydration::Scoring
    } else if stats_terms.is_some_and(|terms| {
        segment_may_contain_terms(terms, TermBloomMode::Or, footer.term_bloom.as_ref())
    }) {
        SegmentHydration::StatsOnly
    } else {
        SegmentHydration::Skip
    }
}

/// Whether scoring/materialization needs `filters.bin`. Broad lexical
/// queries with default score/_id ranking do not: they use only postings,
/// doc metadata, and page doc stores.
#[cfg(feature = "s3")]
fn query_needs_filter_store(query: &SearchQuery) -> bool {
    query.needs_filter_columns()
}

/// The filter files a query reading `fields` needs from one segment
/// (#244): `filters.bin`, or `filters.toc` plus the named columns, as the
/// segment's footer says. `footer` is the cached copy when there is one;
/// otherwise the local `footer.json` decides. With no footer at all the
/// segment cannot be scored anyway (`footer.json` is itself missing), so
/// the legacy name is as good as any.
#[cfg(feature = "s3")]
fn segment_filter_files(
    footer: Option<&kosha_core::Footer>,
    seg_path: &Path,
    fields: &FilterFields,
) -> Vec<String> {
    match footer {
        Some(footer) => footer.filter_files_for(Some(fields)),
        None => SegmentReader::read_footer(seg_path)
            .map(|f| f.filter_files_for(Some(fields)))
            .unwrap_or_else(|_| vec![kosha_core::FILTERS_BIN_FILE.to_string()]),
    }
}

/// The filter column names a query reads, or `None` when it needs no
/// filter files (#244). A legacy segment needs its whole `filters.bin`
/// either way; a columnar one needs `filters.toc` plus these columns.
#[cfg(feature = "s3")]
fn query_filter_fields(query: &SearchQuery) -> Option<FilterFields> {
    query_needs_filter_store(query).then(|| query.filter_column_fields())
}

/// The terms whose postings a query reads, for ranged posting reads
/// (#245): every term any segment's analysis reads the query as (#220),
/// including per-field keys (#249) — the same terms
/// [`posting_blob_files_for_query`] maps to shards. Empty for a wildcard or
/// fuzzy query, whose terms are only known once each segment's dictionary
/// is expanded; those read whole shards.
#[cfg(feature = "s3")]
fn posting_terms_for_query<'a>(
    query: &SearchQuery,
    analysis: &kosha_query::QueryAnalysis,
    analyses: impl IntoIterator<Item = Option<&'a kosha_core::SegmentAnalysis>>,
) -> Vec<String> {
    if query.wildcard.is_some() || query.fuzzy.is_some() {
        return Vec::new();
    }
    analysis.union_terms(analyses)
}

/// [`posting_spans::PostingFetchStats`] of a whole-shard pass: one round
/// when it fetched or failed anything.
#[cfg(feature = "s3")]
fn whole_posting_stats(o: &HydrationOutcome) -> posting_spans::PostingFetchStats {
    posting_spans::PostingFetchStats {
        rounds: u64::from(o.files_fetched > 0 || !o.missing.is_empty()),
        whole_files: o.files_fetched as u64,
        whole_bytes: o.bytes_fetched,
        ..Default::default()
    }
}

#[cfg(feature = "s3")]
fn posting_blob_files_for_query<'a>(
    query: &SearchQuery,
    analysis: &kosha_query::QueryAnalysis,
    analyses: impl IntoIterator<Item = Option<&'a kosha_core::SegmentAnalysis>>,
) -> Vec<String> {
    // Wildcard and fuzzy (#201) terms are only known once each segment's
    // dictionary is expanded: every shard may hold one.
    if query.wildcard.is_some() || query.fuzzy.is_some() {
        return kosha_segment::all_posting_blob_files();
    }

    // Every term any segment's reading of the query holds (#220).
    let mut files = std::collections::HashSet::new();
    for term in analysis.union_terms(analyses) {
        files.insert(kosha_segment::posting_blob_file_for_term(&term));
    }
    let mut files: Vec<String> = files.into_iter().collect();
    files.sort();
    files
}

/// The error response a hydration outcome demands, or `None` when the search
/// may proceed.
///
/// Both search handlers share this so the shed/failure distinction can't
/// drift between them: a shed is load-shedding (429, retry with backoff —
/// the same contract `Semaphore::acquire_or_shed` and
/// `kosha_query::MemoryLedger` already use), a non-shed `missing` list is a
/// genuine S3 failure (503). The shed check comes first because a shed
/// request also has a non-empty `missing` list — it stopped fetching on
/// purpose — and reporting that as a 503 would tell the caller its data is
/// broken when the server merely declined to pull multiple GB in one
/// request.
#[cfg(feature = "s3")]
fn hydration_error_response(outcome: &HydrationOutcome) -> Option<String> {
    if outcome.shed {
        return Some(hydration_shed_response());
    }
    if !outcome.missing.is_empty() {
        return Some(json_error(503, &hydration_failed_message(&outcome.missing)));
    }
    None
}

/// The 429 a hydration shed produces. Shared by the pre-scoring check and
/// the post-scoring one: page materialization fetches doc stores through the
/// same request accountant (`ensure_doc_store`), so a request can also cross
/// its ceiling *after* scoring — at which point the missing doc store
/// surfaces as an IO error. Reporting that as a 500 would call a deliberate
/// admission decision a server bug.
#[cfg(feature = "s3")]
fn hydration_shed_response() -> String {
    json_error(
        429,
        "search overloaded: this query needs more segment data than one request \
         may hydrate — the namespace is still warming, retry with backoff",
    )
}

/// Error message for a search that can't proceed because hydrating one or
/// more required segments from S3 failed — returning results anyway would
/// silently look like "0 hits" for a namespace that actually has data.
#[cfg(feature = "s3")]
fn hydration_failed_message(missing: &[String]) -> String {
    const MAX_LISTED: usize = 10;
    let listed: Vec<&str> = missing
        .iter()
        .take(MAX_LISTED)
        .map(|s| s.as_str())
        .collect();
    let suffix = if missing.len() > MAX_LISTED {
        format!(" (+{} more)", missing.len() - MAX_LISTED)
    } else {
        String::new()
    };
    format!(
        "segment hydration failed for {} segment(s), search would be incomplete: {}{}",
        missing.len(),
        listed.join(", "),
        suffix
    )
}

// ─── Internal (pod-to-pod) routes ───────────────────────────────────────────

/// `GET /internal/segment/<relative-path>` — serve an already-cached
/// segment file's bytes to a peer replica, for `fetch_from_peer`.
/// `relative_path` is validated the same way any other cache key is
/// (`Cache::path_for` rejects `..`/absolute-path traversal). 404 (not an
/// error — the caller retries against the lease owner) when the path isn't
/// cached on this pod, whether because it's not the owner or hydration is
/// still in flight.
#[cfg(all(feature = "s3", feature = "postgres"))]
fn handle_internal_segment(relative_path: &str, state: &AppState) -> String {
    let local_path = match state.cache.path_for(relative_path) {
        Ok(p) => p,
        Err(_) => return json_error(400, "invalid path"),
    };
    match std::fs::read(&local_path) {
        Ok(bytes) => segment_response(&bytes),
        Err(_) => json_error(404, "not cached here"),
    }
}

/// Base64-encoded body response for `handle_internal_segment` — this
/// server's response plumbing works in `String`s throughout (see
/// `json_ok`/`raw_response` below), so raw segment bytes travel as base64
/// rather than needing a separate raw-bytes response path just for this one
/// route. `fetch_from_peer` is the corresponding decoder.
#[cfg(all(feature = "s3", feature = "postgres"))]
fn segment_response(bytes: &[u8]) -> String {
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    format!(
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: application/octet-stream\r\nx-kosha-encoding: base64\r\nconnection: close\r\n\r\n{encoded}",
        encoded.len()
    )
}

// ─── JSON response helpers ──────────────────────────────────────────────────

fn json_ok<T: serde::Serialize>(value: &T) -> String {
    let body = serde_json::to_string(value).unwrap_or_else(|_| "{}".into());
    format!(
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// One structured line per search request — the cold-read baseline
/// instrument (Day 1 of the cold-read plan). Greppable as
/// `search timing:`; every phase is wall-clock ms except `open_total_ms`
/// (summed across rayon workers — read with `opens_cold` as average
/// cold-open cost). `hydrate_files`/`hydrate_mb` count only this request's
/// own S3 fetches; a fully-warm NVMe cache logs 0/0.0 with a small
/// residual `hydrate_ms` (local existence checks).
fn log_search_timing(
    ns: &NamespaceId,
    hydration: &HydrationOutcome,
    hydrate_ms: f64,
    queue_ms: f64,
    phases: &kosha_query::SearchPhaseStats,
    total_hits: usize,
    // See `kosha_core::SearchResult::knn_degraded_segments` — `None` (a
    // non-kNN query) logs the same as `Some(0)` here; this line is a human/
    // grep-facing skim, not the API contract that needs the distinction.
    knn_degraded_segments: Option<u32>,
) {
    let total_ms = hydrate_ms
        + queue_ms
        + phases.admit_wall_ms
        + phases.bm25_stats_ms
        + phases.score_wall_ms
        + phases.materialize_wall_ms;
    println!(
        "search timing: ns={} total_ms={:.1} hydrate_ms={:.1} hydrate_files={} \
         hydrate_mb={:.1} queue_ms={:.1} admit_ms={:.1} bm25_stats={} bm25_stats_ms={:.2} \
         score_ms={:.1} materialize_ms={:.1} opens_cold={} opens_cached={} \
         open_total_ms={:.1} knn_postings={} knn_mode={} knn_exact_vectors={} \
         knn_filtered={} knn_degraded={} hits={} page_gets={} \
         page_ranged_kb={:.1} page_whole_files={} page_whole_mb={:.1} s3_rounds={} \
         s3_bytes={} postings_gets={} postings_ranged_kb={:.1} postings_whole_files={} \
         postings_whole_mb={:.1} vector_gets={} vector_ranged_kb={:.1} \
         vector_whole_files={}",
        ns.0,
        total_ms,
        hydrate_ms,
        hydration.files_fetched,
        hydration.bytes_fetched as f64 / (1024.0 * 1024.0),
        queue_ms,
        phases.admit_wall_ms,
        phases.bm25_stats.as_str(),
        phases.bm25_stats_ms,
        phases.score_wall_ms,
        phases.materialize_wall_ms,
        phases.open_cold,
        phases.open_cached,
        phases.open_total_ms,
        phases.knn_postings_probed,
        phases.knn_mode.map_or("-", |mode| mode.as_str()),
        phases.knn_exact_vectors,
        phases
            .knn_filtered_candidates
            .map_or_else(|| "-".to_string(), |n| n.to_string()),
        knn_degraded_segments.unwrap_or(0),
        total_hits,
        hydration.page.ranged_gets,
        hydration.page.ranged_bytes as f64 / 1024.0,
        hydration.page.whole_files,
        hydration.page.whole_bytes as f64 / (1024.0 * 1024.0),
        hydration.rounds + hydration.vectors.rounds + hydration.page.rounds,
        hydration.bytes_fetched
            + hydration.vectors.ranged_bytes
            + hydration.vectors.whole_bytes
            + hydration.page.ranged_bytes
            + hydration.page.whole_bytes,
        hydration.postings.ranged_gets,
        hydration.postings.ranged_bytes as f64 / 1024.0,
        hydration.postings.whole_files,
        hydration.postings.whole_bytes as f64 / (1024.0 * 1024.0),
        hydration.vectors.ranged_gets,
        hydration.vectors.ranged_bytes as f64 / 1024.0,
        hydration.vectors.whole_files,
    );
}

/// One line per provider call, next to `search timing:`. Greppable as
/// `rerank:`.
fn log_rerank(ns: &NamespaceId, call: &kosha_query::rerank::RerankCall) {
    let outcome = match call.outcome {
        kosha_query::rerank::RerankOutcome::Ok => "ok".to_string(),
        kosha_query::rerank::RerankOutcome::Timeout => "timeout".to_string(),
        kosha_query::rerank::RerankOutcome::Error(ref e) => format!("error ({e})"),
    };
    println!(
        "rerank: ns={} provider={} docs={} ms={:.1} billed_units={} outcome={outcome}",
        ns.0,
        call.provider,
        call.documents,
        call.latency.as_secs_f64() * 1e3,
        call.billed_units.unwrap_or(0.0),
    );
}

/// Map a search failure onto the right HTTP status: load-shedding
/// (`KoshaError::Overloaded`, see `kosha_query::MemoryLedger`) is a
/// transient 429 the client should retry with backoff, not a 500; a
/// control-store outage on a never-loaded namespace is a 503; an
/// out-of-range request is a 400.
fn search_error_response(e: &KoshaError) -> String {
    match e {
        KoshaError::Overloaded(_) => json_error(429, &format!("search overloaded: {e}")),
        KoshaError::ControlPlane(_) => json_error(503, &format!("search unavailable: {e}")),
        KoshaError::InvalidQuery(_) => json_error(400, &e.to_string()),
        // A query replica learns from the control store that the namespace
        // does not exist (never created, or deleted).
        KoshaError::NamespaceNotFound(_) => json_error(404, &e.to_string()),
        _ => json_error(500, &format!("search error: {e}")),
    }
}

/// `202 Accepted` with a JSON body: a background job was started.
fn json_accepted<T: serde::Serialize>(value: &T) -> String {
    let body = serde_json::to_string(value).unwrap_or_else(|_| "{}".into());
    format!(
        "HTTP/1.1 202 Accepted\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn json_error(status_code: u16, message: &str) -> String {
    // The reason for an error response; `request:` carries the rest.
    eprintln!("WARN: request failed: status={status_code} {message}");
    let body = serde_json::json!({"error": message}).to_string();
    let status_line = match status_code {
        400 => "400 Bad Request",
        401 => "401 Unauthorized",
        403 => "403 Forbidden",
        404 => "404 Not Found",
        409 => "409 Conflict",
        410 => "410 Gone",
        413 => "413 Payload Too Large",
        429 => "429 Too Many Requests",
        500 => "500 Internal Server Error",
        503 => "503 Service Unavailable",
        _ => "500 Internal Server Error",
    };
    format!(
        "HTTP/1.1 {status_line}\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Returns *just* the JSON body (no HTTP headers) for an error response.
/// Used by `authenticate()` which constructs its own HTTP response.
fn json_error_body(status_code: u16, message: &str) -> String {
    let body = serde_json::json!({"error": message}).to_string();
    let status_line = match status_code {
        403 => "403 Forbidden",
        400 => "400 Bad Request",
        401 => "401 Unauthorized",
        404 => "404 Not Found",
        500 => "500 Internal Server Error",
        _ => "500 Internal Server Error",
    };
    format!(
        "HTTP/1.1 {status_line}\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Build a raw HTTP response with an arbitrary status code and a body
/// relayed verbatim (not re-serialized) — used by `forward_to_ingest` to
/// pass through the ingest pod's actual status/body untouched. Unlike
/// `json_error`, which always wraps its message in `{"error": ...}`, this
/// must not alter the body at all: some existing clients (e.g. kosha_client)
/// key off specific status codes like 404 in their own error handling, so
/// silently coercing an unrecognized code to 500 the way `json_error`'s
/// fallback does would be a real correctness bug for a proxy specifically.
fn raw_response(status_code: u16, body: &str) -> String {
    let reason = match status_code {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Response",
    };
    format!(
        "HTTP/1.1 {status_code} {reason}\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Minimal URL decoder: decodes %XX and + → space.
fn url_decode(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(c) = chars.next() {
        match c {
            '+' => result.push(' '),
            '%' => {
                let hi = chars.next().and_then(|c| c.to_digit(16)).unwrap_or(0);
                let lo = chars.next().and_then(|c| c.to_digit(16)).unwrap_or(0);
                result.push((hi as u8 * 16 + lo as u8) as char);
            }
            _ => result.push(c),
        }
    }
    result
}

impl Drop for AppState {
    fn drop(&mut self) {
        let _ = self.indexer.flush_all();
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::{Document, DocumentId, Field};
    use std::fs;

    #[derive(Clone)]
    struct SharedManifestStore {
        inner: Arc<Mutex<kosha_control::Controller>>,
        lose_next_commit_reply: Arc<std::sync::atomic::AtomicBool>,
        /// Commit this manifest (over the stored version) right after the
        /// next `manifest_version` read: a writer racing the reader.
        commit_on_next_version_read: Arc<Mutex<Option<kosha_core::Manifest>>>,
    }
    impl ControlStore for SharedManifestStore {
        fn create_namespace(&mut self, id: NamespaceId) -> Result<(), KoshaError> {
            self.inner.lock().unwrap().create_namespace(id)
        }
        fn ensure_namespace(&mut self, id: NamespaceId) {
            self.inner.lock().unwrap().ensure_namespace(id);
        }
        fn has_namespace(&self, id: &NamespaceId) -> bool {
            self.inner.lock().unwrap().has_namespace(id)
        }
        fn manifest(&self, _: &NamespaceId) -> Option<&kosha_core::Manifest> {
            None
        }
        fn manifest_mut(&mut self, _: &NamespaceId) -> Option<&mut kosha_core::Manifest> {
            None
        }
        fn manifest_cloned(&self, id: &NamespaceId) -> Option<kosha_core::Manifest> {
            self.inner.lock().unwrap().manifest(id).cloned()
        }
        fn manifest_version(&self, id: &NamespaceId) -> Result<Option<u64>, KoshaError> {
            let mut inner = self.inner.lock().unwrap();
            let version = inner.manifest(id).map(|manifest| manifest.version);
            if let Some(racing) = self.commit_on_next_version_read.lock().unwrap().take() {
                inner
                    .compare_and_swap_manifest(id, version.unwrap_or(0), racing)
                    .unwrap();
            }
            Ok(version)
        }
        fn save_manifest(
            &mut self,
            id: &NamespaceId,
            manifest: &kosha_core::Manifest,
        ) -> Result<(), KoshaError> {
            self.inner.lock().unwrap().save_manifest(id, manifest)
        }
        fn compare_and_swap_manifest(
            &mut self,
            id: &NamespaceId,
            expected: u64,
            manifest: kosha_core::Manifest,
        ) -> Result<(), KoshaError> {
            self.inner
                .lock()
                .unwrap()
                .compare_and_swap_manifest(id, expected, manifest)?;
            if self
                .lose_next_commit_reply
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(KoshaError::NotFound("injected lost commit reply".into()));
            }
            Ok(())
        }
        fn list_namespaces(&self) -> Vec<NamespaceId> {
            self.inner.lock().unwrap().list_namespaces().to_vec()
        }
        fn namespace_count(&self) -> usize {
            self.inner.lock().unwrap().namespace_count()
        }
        fn delete_namespace(
            &mut self,
            id: &NamespaceId,
            retire_at_least: u64,
        ) -> Result<(kosha_core::Manifest, u64), KoshaError> {
            self.inner
                .lock()
                .unwrap()
                .delete_namespace(id, retire_at_least)
        }
    }

    impl SharedManifestStore {
        fn new(inner: Arc<Mutex<kosha_control::Controller>>) -> Self {
            Self {
                inner,
                lose_next_commit_reply: Default::default(),
                commit_on_next_version_read: Default::default(),
            }
        }
    }

    // ── Namespace lifecycle: POST /v1/namespaces, DELETE /v1/namespaces/{ns}

    fn status_of(response: &str) -> u16 {
        response
            .get(9..12)
            .and_then(|code| code.parse().ok())
            .unwrap_or(0)
    }

    fn body_of(response: &str) -> serde_json::Value {
        let (_, body) = response.split_once("\r\n\r\n").expect("no body");
        serde_json::from_str(body).unwrap_or(serde_json::Value::Null)
    }

    fn v1(state: &AppState, tenant: &str, line: &str, body: serde_json::Value) -> String {
        let bytes = if body.is_null() {
            Vec::new()
        } else {
            serde_json::to_vec(&body).unwrap()
        };
        route(
            &format!("{line} HTTP/1.1\r\n"),
            &HashMap::new(),
            &bytes,
            tenant,
            state,
        )
    }

    fn one_doc(id: &str, text: &str) -> serde_json::Value {
        serde_json::json!({"documents": [
            {"id": id, "fields": [{"name": "title", "field_type": "Text", "value": text}]}
        ]})
    }

    #[test]
    fn namespace_create_conflicts_validates_and_is_tenant_scoped() {
        let state = test_state();
        let create =
            |tenant: &str, body: serde_json::Value| v1(&state, tenant, "POST /v1/namespaces", body);

        let created = create("acme", serde_json::json!({"namespace": "docs"}));
        assert_eq!(status_of(&created), 200, "{created}");
        let body = body_of(&created);
        assert_eq!(body["id"], "docs");
        assert_eq!(body["document_count"], 0);
        assert_eq!(body["segment_count"], 0);
        assert_eq!(body["schema"], serde_json::json!({"fields": {}}));
        assert!(body["created_at"].as_u64().unwrap() > 0);
        assert!(state
            .controller
            .lock()
            .unwrap()
            .has_namespace(&NamespaceId("acme/docs".into())));

        // Duplicate → 409, not 500.
        let dup = create("acme", serde_json::json!({"namespace": "docs"}));
        assert_eq!(status_of(&dup), 409, "{dup}");
        // The same name in another tenant is a different namespace.
        let other = create("globex", serde_json::json!({"namespace": "docs"}));
        assert_eq!(status_of(&other), 200, "{other}");

        // An explicitly created namespace is searchable (empty) right away.
        let search = v1(
            &state,
            "acme",
            "POST /v1/namespaces/docs/search",
            serde_json::json!({"query_text": "anything"}),
        );
        assert_eq!(status_of(&search), 200, "{search}");
        assert_eq!(body_of(&search)["total_hits"], 0);
        let stats = v1(
            &state,
            "acme",
            "GET /v1/namespaces/docs/stats",
            serde_json::Value::Null,
        );
        assert_eq!(status_of(&stats), 200, "{stats}");

        // Invalid names → 400.
        for bad in [
            serde_json::json!({"namespace": "a/b"}),
            serde_json::json!({"namespace": ""}),
            serde_json::json!({"namespace": ".."}),
            serde_json::json!({"namespace": 7}),
            serde_json::json!({}),
        ] {
            let resp = create("acme", bad.clone());
            assert_eq!(status_of(&resp), 400, "{bad} → {resp}");
        }
        let not_json = route(
            "POST /v1/namespaces HTTP/1.1\r\n",
            &HashMap::new(),
            b"{",
            "acme",
            &state,
        );
        assert_eq!(status_of(&not_json), 400, "{not_json}");

        // Schema: validated and returned in canonical proto-JSON form.
        let with_schema = create(
            "acme",
            serde_json::json!({"namespace": "typed", "schema": {"fields": {
                "body": {"type": "text", "indexed": true, "analyzer": ["standard"]},
                "sku": {"type": "FIELD_TYPE_KEYWORD", "stored": true},
                "qty": {"field_type": 3},
            }}}),
        );
        assert_eq!(status_of(&with_schema), 200, "{with_schema}");
        assert_eq!(
            body_of(&with_schema)["schema"],
            serde_json::json!({"fields": {
                "body": {"type": "FIELD_TYPE_TEXT", "indexed": true, "stored": false, "analyzer": ["standard"]},
                "sku": {"type": "FIELD_TYPE_KEYWORD", "indexed": false, "stored": true, "analyzer": []},
                "qty": {"type": "FIELD_TYPE_INTEGER", "indexed": false, "stored": false, "analyzer": []},
            }})
        );
        for bad_schema in [
            serde_json::json!({"fields": {"x": {"type": "geo_point"}}}),
            serde_json::json!({"fields": {"x": {"type": 99}}}),
            serde_json::json!({"fields": {"x": {"typo": true}}}),
            serde_json::json!({"fields": {"x": {"indexed": "yes"}}}),
            serde_json::json!({"fields": []}),
            serde_json::json!({"mappings": {}}),
        ] {
            let resp = create(
                "acme",
                serde_json::json!({"namespace": "bad-schema", "schema": bad_schema}),
            );
            assert_eq!(status_of(&resp), 400, "{bad_schema} → {resp}");
        }
        assert!(!state
            .controller
            .lock()
            .unwrap()
            .has_namespace(&NamespaceId("acme/bad-schema".into())));

        // Auto-create on first write still works without an explicit create,
        // and an explicit create afterwards is a conflict.
        let indexed = v1(
            &state,
            "acme",
            "POST /v1/namespaces/auto/documents",
            one_doc("d", "x"),
        );
        assert_eq!(status_of(&indexed), 200, "{indexed}");
        let late = create("acme", serde_json::json!({"namespace": "auto"}));
        assert_eq!(status_of(&late), 409, "{late}");
    }

    #[test]
    fn namespace_delete_round_trip_is_tenant_scoped_and_recreate_starts_empty() {
        let state = test_state();
        let ok = |resp: String| {
            assert_eq!(status_of(&resp), 200, "{resp}");
            body_of(&resp)
        };
        ok(v1(
            &state,
            "acme",
            "POST /v1/namespaces",
            serde_json::json!({"namespace": "docs"}),
        ));
        ok(v1(
            &state,
            "acme",
            "POST /v1/namespaces/docs/documents",
            one_doc("d1", "quick fox"),
        ));
        ok(v1(
            &state,
            "acme",
            "POST /v1/namespaces/docs/flush",
            serde_json::json!({}),
        ));
        // Buffered, never flushed: must not survive the delete either.
        ok(v1(
            &state,
            "acme",
            "POST /v1/namespaces/docs/documents",
            one_doc("d2", "quick dog"),
        ));
        let search = || {
            v1(
                &state,
                "acme",
                "POST /v1/namespaces/docs/search",
                serde_json::json!({"query_text": "quick"}),
            )
        };
        assert_eq!(ok(search())["total_hits"], 1);
        let old_version = state
            .controller
            .lock()
            .unwrap()
            .manifest_version(&NamespaceId("acme/docs".into()))
            .unwrap()
            .unwrap();
        let seg_dir = state.data_dir.join("acme/docs");
        assert!(seg_dir.is_dir());

        // Tenant B cannot see or delete tenant A's namespace.
        let foreign = v1(
            &state,
            "globex",
            "DELETE /v1/namespaces/docs",
            serde_json::Value::Null,
        );
        assert_eq!(status_of(&foreign), 404, "{foreign}");
        let foreign_search = v1(
            &state,
            "globex",
            "POST /v1/namespaces/docs/search",
            serde_json::json!({"query_text": "quick"}),
        );
        assert_eq!(status_of(&foreign_search), 404, "{foreign_search}");
        assert_eq!(ok(search())["total_hits"], 1);

        let deleted = ok(v1(
            &state,
            "acme",
            "DELETE /v1/namespaces/docs",
            serde_json::Value::Null,
        ));
        assert_eq!(deleted["namespace"], "docs");
        assert_eq!(deleted["acknowledged"], true);
        assert_eq!(deleted["storage_reclaimed"], true);
        assert_eq!(status_of(&search()), 404);
        let stats = v1(
            &state,
            "acme",
            "GET /v1/namespaces/docs/stats",
            serde_json::Value::Null,
        );
        assert_eq!(status_of(&stats), 404, "{stats}");
        assert!(!seg_dir.exists(), "local segments are removed");
        let again = v1(
            &state,
            "acme",
            "DELETE /v1/namespaces/docs",
            serde_json::Value::Null,
        );
        assert_eq!(status_of(&again), 404, "{again}");
        assert!(state.result_cache.lock().unwrap().entries.is_empty());

        // Re-create starts empty, above every version the old one used.
        let created = ok(v1(
            &state,
            "acme",
            "POST /v1/namespaces",
            serde_json::json!({"namespace": "docs"}),
        ));
        assert!(
            created["version"].as_u64().unwrap() > old_version,
            "{created}"
        );
        assert_eq!(ok(search())["total_hits"], 0);
        // Flushing the new incarnation must not bring back the buffered doc.
        ok(v1(
            &state,
            "acme",
            "POST /v1/namespaces/docs/flush",
            serde_json::json!({}),
        ));
        assert_eq!(ok(search())["total_hits"], 0);
        ok(v1(
            &state,
            "acme",
            "POST /v1/namespaces/docs/documents",
            one_doc("d3", "quick cat"),
        ));
        ok(v1(
            &state,
            "acme",
            "POST /v1/namespaces/docs/flush",
            serde_json::json!({}),
        ));
        let hits = ok(search());
        assert_eq!(hits["total_hits"], 1, "{hits}");
        assert!(hits.to_string().contains("d3"), "{hits}");

        // Deleting a namespace that only ever auto-created works the same.
        ok(v1(
            &state,
            "acme",
            "POST /v1/namespaces/auto/documents",
            one_doc("a", "x"),
        ));
        ok(v1(
            &state,
            "acme",
            "DELETE /v1/namespaces/auto",
            serde_json::Value::Null,
        ));
        let gone = v1(
            &state,
            "acme",
            "POST /v1/namespaces/auto/search",
            serde_json::json!({"query_text": "x"}),
        );
        assert_eq!(status_of(&gone), 404, "{gone}");
    }

    #[test]
    fn namespace_delete_discards_the_wal_so_a_restart_does_not_resurrect_it() {
        let state = test_state();
        let dir = state.data_dir.clone();
        let ns = NamespaceId("acme/docs".into());
        let kept = NamespaceId("acme/kept".into());
        for name in ["docs", "kept"] {
            let resp = v1(
                &state,
                "acme",
                &format!("POST /v1/namespaces/{name}/documents"),
                one_doc("d", "buffered only"),
            );
            assert_eq!(status_of(&resp), 200, "{resp}");
        }
        assert!(state.indexer.pending_doc_count(&ns) > 0);
        let resp = v1(
            &state,
            "acme",
            "DELETE /v1/namespaces/docs",
            serde_json::Value::Null,
        );
        assert_eq!(status_of(&resp), 200, "{resp}");
        drop(state);

        let restarted = AppState::new(dir);
        assert!(restarted.indexer.manifest(&ns).is_none());
        assert_eq!(restarted.indexer.pending_doc_count(&ns), 0);
        assert!(restarted.indexer.pending_doc_count(&kept) > 0);
    }

    #[test]
    fn namespace_routes_reject_bad_paths() {
        let state = test_state();
        for (line, code) in [
            ("DELETE /v1/namespaces/..", 400),
            ("DELETE /v1/namespaces/", 400),
            ("DELETE /v1/namespaces/%2F", 400),
            ("GET /v1/namespaces/docs", 404),
            ("GET /v1/namespaces", 404),
            ("DELETE /v1/namespaces", 404),
            ("DELETE /v1/namespaces/missing", 404),
        ] {
            let resp = v1(&state, "acme", line, serde_json::Value::Null);
            assert_eq!(status_of(&resp), code, "{line} → {resp}");
        }
    }

    /// A query replica forwards both lifecycle routes to the ingest node
    /// over real sockets, and its own reads follow the control store they
    /// share: create → index → search → delete → 404 → re-create → empty.
    #[test]
    fn query_replica_forwards_namespace_create_and_delete() {
        use std::io::Read;

        let shared = Arc::new(Mutex::new(kosha_control::Controller::new()));
        let ingest = test_state();
        *ingest.controller.lock().unwrap() = Box::new(SharedManifestStore::new(shared.clone()));
        let mut query = AppState::new(ingest.data_dir.clone());
        *query.controller.lock().unwrap() = Box::new(SharedManifestStore::new(shared.clone()));

        let ingest_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let ingest_addr = ingest_listener.local_addr().unwrap();
        let ingest = Arc::new(ingest);
        let ingest_handle = ingest.clone();
        std::thread::spawn(move || serve(ingest_listener, ingest, Duration::from_secs(30)));
        query.ingest_host = Some(format!("http://{ingest_addr}"));
        query.write_http_client = Some(
            reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap(),
        );
        let query_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let query_addr = query_listener.local_addr().unwrap();
        std::thread::spawn(move || serve(query_listener, Arc::new(query), Duration::from_secs(30)));

        let call =
            |method: &str, path: &str, body: serde_json::Value| -> (u16, serde_json::Value) {
                let body = if body.is_null() {
                    String::new()
                } else {
                    body.to_string()
                };
                let mut s = TcpStream::connect(query_addr).unwrap();
                s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
                s.write_all(
                    format!(
                        "{method} {path} HTTP/1.1\r\nhost: q\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .unwrap();
                let mut resp = String::new();
                s.read_to_string(&mut resp).unwrap();
                (status_of(&resp), body_of(&resp))
            };
        // `max_staleness_seconds: 0`: read the control store on every
        // search (#232), so each step sees the ingest node's last change.
        let search = || {
            call(
                "POST",
                "/v1/namespaces/docs/search",
                serde_json::json!({"query_text": "quick", "max_staleness_seconds": 0}),
            )
        };
        let ns = NamespaceId("dev/docs".into());

        let (code, body) = call(
            "POST",
            "/v1/namespaces",
            serde_json::json!({"namespace": "docs"}),
        );
        assert_eq!(code, 200, "{body}");
        assert!(shared.lock().unwrap().has_namespace(&ns));
        assert!(
            ingest_handle.indexer.manifest_version(&ns).is_some(),
            "the create ran on the ingest node"
        );
        let (code, body) = call(
            "POST",
            "/v1/namespaces",
            serde_json::json!({"namespace": "docs"}),
        );
        assert_eq!(code, 409, "{body}");
        assert_eq!(search().0, 200, "the replica sees the new, empty namespace");

        assert_eq!(
            call(
                "POST",
                "/v1/namespaces/docs/documents",
                one_doc("d1", "quick fox")
            )
            .0,
            200
        );
        assert_eq!(
            call("POST", "/v1/namespaces/docs/flush", serde_json::json!({})).0,
            200
        );
        let (code, body) = search();
        assert_eq!(
            (code, body["total_hits"].clone()),
            (200, serde_json::json!(1)),
            "{body}"
        );

        let (code, body) = call("DELETE", "/v1/namespaces/docs", serde_json::Value::Null);
        assert_eq!(code, 200, "{body}");
        assert_eq!(body["acknowledged"], true);
        assert!(!shared.lock().unwrap().has_namespace(&ns));
        assert!(
            ingest_handle.indexer.manifest_version(&ns).is_none(),
            "the delete ran on the ingest node"
        );
        assert_eq!(search().0, 404);
        assert_eq!(
            call("GET", "/v1/namespaces/docs/stats", serde_json::Value::Null).0,
            404
        );
        assert_eq!(
            call("DELETE", "/v1/namespaces/docs", serde_json::Value::Null).0,
            404
        );

        // Re-create: the replica still holds the old incarnation in memory
        // (and its result cache has the old answer), but the new one's
        // versions are all higher, so it serves the new, empty namespace.
        assert_eq!(
            call(
                "POST",
                "/v1/namespaces",
                serde_json::json!({"namespace": "docs"})
            )
            .0,
            200
        );
        let (code, body) = search();
        assert_eq!(
            (code, body["total_hits"].clone()),
            (200, serde_json::json!(0)),
            "{body}"
        );
    }

    #[cfg(feature = "s3")]
    #[test]
    fn no_op_publication_preserves_legacy_inline_delete_representation() {
        let state = test_state();
        let ns = NamespaceId("legacy-deletes".into());
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId("d".into()),
                    fields: vec![Field::text("text", "hello")],
                }],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        let mut legacy = state.indexer.manifest(&ns).unwrap();
        legacy.version += 1;
        legacy.tombstones.insert(
            legacy.segments[0].segment_id.clone(),
            [0].into_iter().collect(),
        );
        state
            .controller
            .lock()
            .unwrap()
            .save_manifest(&ns, &legacy)
            .unwrap();
        state.indexer.restore_manifest(ns.clone(), legacy.clone());
        state.publish_namespace(&ns).unwrap();
        assert!(state
            .controller
            .lock()
            .unwrap()
            .manifest_cloned(&ns)
            .unwrap()
            .same_contents(&legacy));
    }

    #[test]
    fn tiny_upsert_segments_only_hydrate_shards_present_in_the_toc() {
        let state = test_state();
        let segment = state.data_dir.join("ns/s1");
        let mut writer =
            kosha_segment::SegmentWriter::new(kosha_core::SegmentId("s1".into()), segment.clone());
        writer.add_document(DocumentId("d".into()), vec![Field::text("text", "hello")]);
        writer.finalize(kosha_core::Bm25Params::default()).unwrap();
        let present = kosha_segment::posting_blob_file_for_term("hello");
        let absent = kosha_segment::all_posting_blob_files()
            .into_iter()
            .find(|file| file != &present)
            .unwrap();
        fs::remove_file(segment.join(&present)).unwrap(); // evicted, still required
        let mut fetch = vec![
            (format!("ns/s1/{present}"), 0),
            (format!("ns/s1/{absent}"), 0),
        ];
        let resolved = prune_unwritten_posting_blobs(&state.data_dir, &mut fetch).unwrap();
        assert_eq!(fetch, vec![(format!("ns/s1/{present}"), 0)]);
        assert_eq!(resolved, vec![("ns/s1".into(), absent)]);
        fs::write(segment.join("inverted.idx"), b"corrupt").unwrap();
        assert!(prune_unwritten_posting_blobs(&state.data_dir, &mut fetch).is_err());
    }

    #[test]
    fn deletion_upload_failure_keeps_old_manifest_and_wal_for_retry() {
        let state = test_state();
        let ns = NamespaceId("delete-upload".into());
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId("d".into()),
                    fields: vec![Field::text("text", "hello")],
                }],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();
        let old = state
            .controller
            .lock()
            .unwrap()
            .manifest_cloned(&ns)
            .unwrap();
        state
            .indexer
            .delete_by_query(
                &ns,
                &old,
                &kosha_core::FilterClause::MatchAll {
                    match_all: Some(serde_json::json!({})),
                },
            )
            .unwrap();
        let failed = state.publish_namespace_with(&ns, |candidate| {
            assert_eq!(candidate.deletion_vectors.len(), 1);
            for (segment, vector) in &candidate.deletion_vectors {
                let bytes = std::fs::read(state.data_dir.join(vector.path(&ns, segment)?))?;
                assert_eq!(vector.decode(&bytes, 1)?.len(), 1);
            }
            Err(KoshaError::NotFound(
                "injected sidecar upload failure".into(),
            ))
        });
        assert!(failed.is_err());
        assert!(state
            .controller
            .lock()
            .unwrap()
            .manifest_cloned(&ns)
            .unwrap()
            .same_contents(&old));
        assert!(
            kosha_write::wal::WalWriter::recover(&state.data_dir.join("_wal"))
                .unwrap()
                .iter()
                .any(|r| !r.deleted_ids.is_empty())
        );
        state.publish_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap(); // identical bitmap snapshot retry

        assert!(
            kosha_write::wal::WalWriter::recover(&state.data_dir.join("_wal"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn lost_commit_reply_is_retryable_without_losing_the_next_flush() {
        let shared = shared_store();
        shared
            .lose_next_commit_reply
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let state = test_state();
        *state.controller.lock().unwrap() = Box::new(shared.clone());
        let ns = NamespaceId("retry".into());
        for (id, expect_error) in [("first", true), ("second", false)] {
            state
                .indexer
                .index_documents(
                    ns.clone(),
                    vec![Document {
                        id: DocumentId(id.into()),
                        fields: vec![Field::text("text", id)],
                    }],
                )
                .unwrap();
            state.indexer.flush_namespace(&ns).unwrap();
            let result = state.publish_namespace_with(&ns, |_| Ok(()));
            assert_eq!(result.is_err(), expect_error);
            if expect_error {
                assert!(
                    !kosha_write::wal::WalWriter::recover(&state.data_dir.join("_wal"))
                        .unwrap()
                        .is_empty()
                );
                state.publish_namespace_with(&ns, |_| Ok(())).unwrap();
                // No-op flush is safe too: version equality cannot authorize a
                // different snapshot, but must permit an identical retry.
                state.publish_namespace_with(&ns, |_| Ok(())).unwrap();
            }
        }
        assert_eq!(shared.manifest_cloned(&ns).unwrap().segments.len(), 2);
        assert!(
            kosha_write::wal::WalWriter::recover(&state.data_dir.join("_wal"))
                .unwrap()
                .is_empty()
        );
    }

    /// An ingest node in `KOSHA_WAL_MODE=s3`, against a fake bucket.
    fn s3_wal_node(
        dir: PathBuf,
        bucket: &kosha_write::object_wal::MemoryObjectBackend,
        control: &SharedManifestStore,
    ) -> AppState {
        let store =
            kosha_write::object_wal::ObjectWalStore::new(Arc::new(bucket.clone()), "ingest-0")
                .unwrap();
        AppState::with_options(
            dir,
            StateOptions {
                wal_store: Some(Arc::new(store)),
                control_store: Some(Box::new(control.clone())),
            },
        )
    }

    fn index_body(ns: &str, ids: &[&str]) -> Vec<u8> {
        serde_json::to_vec(&IndexRequest {
            namespace: NamespaceId(ns.into()),
            documents: ids
                .iter()
                .map(|id| Document {
                    id: DocumentId((*id).into()),
                    fields: vec![Field::text("text", "survivor")],
                })
                .collect(),
        })
        .unwrap()
    }

    fn wal_objects(bucket: &kosha_write::object_wal::MemoryObjectBackend) -> usize {
        bucket.keys().iter().filter(|k| k.ends_with(".bin")).count()
    }

    #[test]
    fn s3_wal_acked_index_survives_losing_the_node() {
        let bucket = kosha_write::object_wal::MemoryObjectBackend::new();
        let control = shared_store();
        let search = br#"{"namespace":"t/ns","query_text":"survivor"}"#;

        let first = test_dir();
        let node = s3_wal_node(first.clone(), &bucket, &control);
        let ack = handle_index(&index_body("t/ns", &["a", "b"]), &node);
        assert!(ack.starts_with("HTTP/1.1 200"), "{ack}");
        assert_eq!(wal_objects(&bucket), 1, "acked after the PUT");
        // Node loss: no shutdown flush, and the data_dir is gone.
        std::mem::forget(node);
        fs::remove_dir_all(&first).unwrap();

        let second = test_dir();
        let node = s3_wal_node(second.clone(), &bucket, &control);
        let flush = handle_flush(br#"{"namespace":"t/ns"}"#, &node);
        assert!(flush.starts_with("HTTP/1.1 200"), "{flush}");
        let found = handle_search_post(search, &node);
        assert!(found.contains("\"total_hits\":2"), "{found}");
        assert_eq!(wal_objects(&bucket), 0, "published checkpoint reclaims");
        let manifest = control
            .manifest_cloned(&NamespaceId("t/ns".into()))
            .unwrap();
        assert!(manifest.wal_checkpoint > 0);
        assert!(manifest.wal_origin.is_some());

        // Losing this node too replays nothing: the checkpoint covers it.
        std::mem::forget(node);
        let third = test_dir();
        let node = s3_wal_node(third.clone(), &bucket, &control);
        assert_eq!(
            node.indexer.pending_doc_count(&NamespaceId("t/ns".into())),
            0
        );
        for dir in [second, third] {
            fs::remove_dir_all(dir).ok();
        }
    }

    #[test]
    fn s3_wal_put_failure_fails_the_write_and_acks_nothing() {
        let bucket = kosha_write::object_wal::MemoryObjectBackend::new();
        let control = shared_store();
        let dir = test_dir();
        let node = s3_wal_node(dir.clone(), &bucket, &control);
        let ns = NamespaceId("t/ns".into());

        bucket.fail_next_puts(1);
        let response = handle_index(&index_body("t/ns", &["lost"]), &node);
        assert!(response.starts_with("HTTP/1.1 5"), "{response}");
        assert_eq!(wal_objects(&bucket), 0);
        assert_eq!(node.indexer.pending_doc_count(&ns), 0, "not buffered");

        let ok = handle_index(&index_body("t/ns", &["kept"]), &node);
        assert!(ok.starts_with("HTTP/1.1 200"), "{ok}");
        let flush = handle_flush(br#"{"namespace":"t/ns"}"#, &node);
        assert!(flush.starts_with("HTTP/1.1 200"), "{flush}");

        // A delete is acknowledged only after its WAL PUT too.
        bucket.fail_next_puts(1);
        let delete = handle_delete(br#"{"namespace":"t/ns","filter":{"match_all":{}}}"#, &node);
        assert!(delete.starts_with("HTTP/1.1 5"), "{delete}");
        let search = br#"{"namespace":"t/ns","query_text":"survivor"}"#;
        let found = handle_search_post(search, &node);
        assert!(found.contains("\"total_hits\":1"), "{found}");

        std::mem::forget(node);
        fs::remove_dir_all(&dir).unwrap();
        let node = s3_wal_node(test_dir(), &bucket, &control);
        let found = handle_search_post(search, &node);
        assert!(!found.contains("\"lost\""), "{found}");
        fs::remove_dir_all(&node.data_dir).ok();
    }

    fn shared_store() -> SharedManifestStore {
        SharedManifestStore {
            inner: Arc::new(Mutex::new(kosha_control::Controller::new())),
            lose_next_commit_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            commit_on_next_version_read: Arc::new(Mutex::new(None)),
        }
    }

    /// A control store that can be taken down: version reads, the batched
    /// poll and manifest fetches fail with `ControlPlane` while `down`, the
    /// way `PgStore` reports an unreachable Postgres (#232).
    #[derive(Clone)]
    struct OutageStore {
        inner: SharedManifestStore,
        down: Arc<std::sync::atomic::AtomicBool>,
        reads: Arc<std::sync::atomic::AtomicU64>,
    }
    impl OutageStore {
        fn check(&self) -> Result<(), KoshaError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.down.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(KoshaError::ControlPlane("injected outage".into()));
            }
            Ok(())
        }
    }
    impl ControlStore for OutageStore {
        fn create_namespace(&mut self, id: NamespaceId) -> Result<(), KoshaError> {
            self.inner.create_namespace(id)
        }
        fn ensure_namespace(&mut self, id: NamespaceId) {
            self.inner.ensure_namespace(id);
        }
        fn has_namespace(&self, id: &NamespaceId) -> bool {
            self.inner.has_namespace(id)
        }
        fn manifest(&self, _: &NamespaceId) -> Option<&kosha_core::Manifest> {
            None
        }
        fn manifest_mut(&mut self, _: &NamespaceId) -> Option<&mut kosha_core::Manifest> {
            None
        }
        fn manifest_cloned(&self, id: &NamespaceId) -> Option<kosha_core::Manifest> {
            self.try_manifest_cloned(id).ok().flatten()
        }
        fn try_manifest_cloned(
            &self,
            id: &NamespaceId,
        ) -> Result<Option<kosha_core::Manifest>, KoshaError> {
            self.check()?;
            Ok(self.inner.manifest_cloned(id))
        }
        fn manifest_version(&self, id: &NamespaceId) -> Result<Option<u64>, KoshaError> {
            self.check()?;
            self.inner.manifest_version(id)
        }
        fn manifest_versions(&self) -> Result<Vec<(NamespaceId, u64)>, KoshaError> {
            self.check()?;
            self.inner.inner.lock().unwrap().manifest_versions()
        }
        fn save_manifest(
            &mut self,
            id: &NamespaceId,
            manifest: &kosha_core::Manifest,
        ) -> Result<(), KoshaError> {
            self.inner.save_manifest(id, manifest)
        }
        fn compare_and_swap_manifest(
            &mut self,
            id: &NamespaceId,
            expected: u64,
            manifest: kosha_core::Manifest,
        ) -> Result<(), KoshaError> {
            self.inner.compare_and_swap_manifest(id, expected, manifest)
        }
        fn list_namespaces(&self) -> Vec<NamespaceId> {
            self.inner.list_namespaces()
        }
        fn namespace_count(&self) -> usize {
            self.inner.namespace_count()
        }
        fn delete_namespace(
            &mut self,
            id: &NamespaceId,
            retire_at_least: u64,
        ) -> Result<(kosha_core::Manifest, u64), KoshaError> {
            self.inner.delete_namespace(id, retire_at_least)
        }
    }

    fn outage_store() -> OutageStore {
        OutageStore {
            inner: shared_store(),
            down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            reads: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// An ingest state and a separate query-role state over one shared
    /// control store and data directory (standing in for S3).
    fn ingest_and_query(store: &OutageStore, refresh: Option<Duration>) -> (AppState, AppState) {
        let ingest = test_state();
        *ingest.controller.lock().unwrap() = Box::new(store.clone());
        let mut query = AppState::new(ingest.data_dir.clone());
        query.ingest_host = Some("http://ingest.invalid".into());
        query.manifest_follow = manifest_follow::ManifestFollow::new(refresh);
        *query.controller.lock().unwrap() = Box::new(store.clone());
        (ingest, query)
    }

    fn index_text_and_publish(state: &AppState, ns: &NamespaceId, ids: &[&str]) {
        state
            .indexer
            .index_documents(
                ns.clone(),
                ids.iter()
                    .map(|id| Document {
                        id: DocumentId((*id).into()),
                        fields: vec![Field::text("text", "hello follower")],
                    })
                    .collect(),
            )
            .unwrap();
        state.indexer.flush_namespace(ns).unwrap();
        state.publish_namespace(ns).unwrap();
    }

    fn total_hits(response: &str) -> u64 {
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let body = &response[response.find("\r\n\r\n").unwrap() + 4..];
        let value: serde_json::Value = serde_json::from_str(body).unwrap();
        value["total_hits"].as_u64().unwrap()
    }

    fn stats_json(state: &AppState) -> serde_json::Value {
        let response = handle_stats(state);
        serde_json::from_str(&response[response.find("\r\n\r\n").unwrap() + 4..]).unwrap()
    }

    /// #232 acceptance (a): docs indexed and flushed through the ingest pod
    /// become searchable on a separate query-role pod within the refresh
    /// interval — no restart, and no control-store read on the search path.
    #[test]
    fn query_replica_follows_ingest_publishes_without_restart() {
        let store = outage_store();
        let ns = NamespaceId("tenant/follow".into());
        let (ingest, query) = ingest_and_query(&store, Some(Duration::from_millis(20)));
        index_text_and_publish(&ingest, &ns, &["a"]);
        let query = Arc::new(query);
        query.spawn_manifest_refresher();
        let search = br#"{"namespace":"tenant/follow","query_text":"hello"}"#;
        let deadline = Instant::now() + Duration::from_secs(10);
        while query.indexer.manifest_version(&ns).is_none() {
            assert!(
                Instant::now() < deadline,
                "refresher never loaded the namespace"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(total_hits(&handle_search_post(search, &query)), 1);

        index_text_and_publish(&ingest, &ns, &["b", "c"]);
        let published = ingest.indexer.manifest_version(&ns).unwrap();
        let start = Instant::now();
        while query.indexer.manifest_version(&ns) != Some(published) {
            assert!(Instant::now() < deadline, "refresher never caught up");
            std::thread::sleep(Duration::from_millis(5));
        }
        // Generous bound for CI noise; the cadence is 20 ms.
        assert!(start.elapsed() < Duration::from_secs(5));
        let reads = store.reads.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(total_hits(&handle_search_post(search, &query)), 3);
        // Searches inside the staleness bound do not touch the control store
        // (the refresher may have polled meanwhile, hence `<=` on the delta).
        let polls_meanwhile = store.reads.load(std::sync::atomic::Ordering::SeqCst) - reads;
        assert!(polls_meanwhile <= 3, "{polls_meanwhile}");

        let stats = stats_json(&query);
        let entry = stats["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["namespace"] == "tenant/follow")
            .unwrap()
            .clone();
        assert_eq!(entry["manifest_version"], published);
        assert!(entry["last_refresh_at"].as_f64().unwrap() > 0.0);
        assert!(entry["manifest_lag_seconds"].as_f64().unwrap() < 5.0);
        assert_eq!(stats["manifest_refresh"]["enabled"], true);
        assert_eq!(stats["manifest_refresh"]["interval_ms"], 20);
        // The ingest pod's stats carry no follower block.
        assert!(stats_json(&ingest)["manifest_refresh"].is_null());
    }

    /// #232: `max_staleness_seconds` forces a synchronous refresh when the
    /// last confirmation is older than the bound; without it a search on a
    /// confirmed namespace serves the local manifest (the refresher's job).
    #[test]
    fn max_staleness_knob_refreshes_synchronously() {
        let store = outage_store();
        let ns = NamespaceId("tenant/knob".into());
        // A refresher is configured but not running: only searches refresh.
        let (ingest, query) = ingest_and_query(&store, Some(Duration::from_secs(3600)));
        index_text_and_publish(&ingest, &ns, &["a"]);
        let search = br#"{"namespace":"tenant/knob","query_text":"hello"}"#;
        // Never loaded → synchronous load.
        assert_eq!(total_hits(&handle_search_post(search, &query)), 1);
        index_text_and_publish(&ingest, &ns, &["b"]);
        assert_eq!(total_hits(&handle_search_post(search, &query)), 1);
        let loose =
            br#"{"namespace":"tenant/knob","query_text":"hello","max_staleness_seconds":3600}"#;
        assert_eq!(total_hits(&handle_search_post(loose, &query)), 1);
        let strict =
            br#"{"namespace":"tenant/knob","query_text":"hello","max_staleness_seconds":0}"#;
        assert_eq!(total_hits(&handle_search_post(strict, &query)), 2);
        let bad = br#"{"namespace":"tenant/knob","query_text":"hello","max_staleness_seconds":-1}"#;
        assert!(handle_search_post(bad, &query).starts_with("HTTP/1.1 400"));
        // The GET route takes it as a query parameter.
        index_text_and_publish(&ingest, &ns, &["c"]);
        let get = handle_search_get(
            "GET /search?ns=tenant/knob&q=hello&max_staleness_seconds=0 HTTP/1.1",
            &query,
        );
        assert_eq!(total_hits(&get), 3);
    }

    /// A namespace deleted on the ingest pod stops answering on a query
    /// replica within one refresher tick, for searches that leave
    /// `max_staleness_seconds` at its default (the refresher never lists a
    /// deleted namespace, so nothing else would invalidate it).
    #[test]
    fn query_replica_forgets_a_deleted_namespace_on_the_next_tick() {
        let store = outage_store();
        let ns = NamespaceId("tenant/gone".into());
        let kept = NamespaceId("tenant/kept".into());
        let (ingest, query) = ingest_and_query(&store, Some(Duration::from_secs(3600)));
        index_text_and_publish(&ingest, &ns, &["a"]);
        index_text_and_publish(&ingest, &kept, &["k"]);
        query.run_manifest_refresh_tick();
        let loose = br#"{"namespace":"tenant/gone","query_text":"hello"}"#;
        assert_eq!(total_hits(&handle_search_post(loose, &query)), 1);
        // Twice, so the second is a result-cache hit keyed by the version.
        assert_eq!(total_hits(&handle_search_post(loose, &query)), 1);

        let resp = handle_delete_namespace("gone", "tenant", &ingest);
        assert_eq!(status_of(&resp), 200, "{resp}");
        query.run_manifest_refresh_tick();
        assert!(query.indexer.manifest_version(&ns).is_none());
        let resp = handle_search_post(loose, &query);
        assert!(resp.starts_with("HTTP/1.1 404"), "{resp}");
        // Other namespaces are untouched.
        let other = br#"{"namespace":"tenant/kept","query_text":"hello"}"#;
        assert_eq!(total_hits(&handle_search_post(other, &query)), 1);

        // Re-created: the replica loads the new, empty incarnation.
        index_text_and_publish(&ingest, &ns, &["b", "c"]);
        query.run_manifest_refresh_tick();
        assert_eq!(total_hits(&handle_search_post(loose, &query)), 2);
    }

    /// #232 acceptance (b): a search that took its manifest snapshot before
    /// a swap completes against that snapshot.
    #[test]
    fn snapshot_taken_before_swap_survives_it() {
        let store = outage_store();
        let ns = NamespaceId("tenant/snap".into());
        let (ingest, query) = ingest_and_query(&store, Some(Duration::from_millis(20)));
        index_text_and_publish(&ingest, &ns, &["a"]);
        query.run_manifest_refresh_tick();
        let snapshot = query.indexer.manifest_shared(&ns).unwrap();
        let before = snapshot.version;

        index_text_and_publish(&ingest, &ns, &["b", "c"]);
        query.run_manifest_refresh_tick();
        let current = query.indexer.manifest_shared(&ns).unwrap();
        assert!(current.version > before);
        assert_eq!(snapshot.version, before, "the held snapshot never changes");
        assert_eq!(snapshot.segments.len(), 1);

        let mut probe = warmup_probe_query();
        probe.query_text = "hello".into();
        probe.max_results = 10;
        let old = query
            .searcher
            .search(&ns, &snapshot, &probe, Some(&snapshot.tombstones))
            .unwrap();
        assert_eq!(old.total_hits, 1, "in-flight search sees its own snapshot");
        let new = query
            .searcher
            .search(&ns, &current, &probe, Some(&current.tombstones))
            .unwrap();
        assert_eq!(new.total_hits, 3);
    }

    /// #232 acceptance (c): with the control plane down, searches keep
    /// succeeding on the last-known-good manifest (even ones asking for
    /// strict freshness), the lag metric grows and errors are counted; the
    /// pod catches up once the store is back. A namespace never loaded
    /// still fails, with 503.
    #[test]
    fn control_plane_outage_serves_last_known_good_and_lag_grows() {
        let store = outage_store();
        let ns = NamespaceId("tenant/outage".into());
        let (ingest, query) = ingest_and_query(&store, Some(Duration::from_millis(20)));
        index_text_and_publish(&ingest, &ns, &["a"]);
        query.run_manifest_refresh_tick();
        let search = br#"{"namespace":"tenant/outage","query_text":"hello"}"#;
        let strict =
            br#"{"namespace":"tenant/outage","query_text":"hello","max_staleness_seconds":0}"#;
        assert_eq!(total_hits(&handle_search_post(search, &query)), 1);

        store.down.store(true, std::sync::atomic::Ordering::SeqCst);
        index_text_and_publish(&ingest, &ns, &["b"]);
        query.run_manifest_refresh_tick();
        let lag_of = |state: &AppState| {
            stats_json(state)["namespaces"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["namespace"] == "tenant/outage")
                .unwrap()["manifest_lag_seconds"]
                .as_f64()
                .unwrap()
        };
        let lag_early = lag_of(&query);
        std::thread::sleep(Duration::from_millis(60));
        query.run_manifest_refresh_tick();
        assert_eq!(total_hits(&handle_search_post(search, &query)), 1);
        assert_eq!(total_hits(&handle_search_post(strict, &query)), 1);
        let lag_late = lag_of(&query);
        assert!(lag_late >= lag_early + 0.05, "{lag_early} -> {lag_late}");
        let summary = stats_json(&query)["manifest_refresh"].clone();
        assert!(summary["control_plane_errors_total"].as_u64().unwrap() >= 2);
        assert!(summary["consecutive_errors"].as_u64().unwrap() >= 2);
        assert!(summary["last_error"]
            .as_str()
            .unwrap()
            .contains("injected outage"));
        assert!(summary["max_manifest_lag_seconds"].as_f64().unwrap() >= lag_late);

        // Never loaded here: nothing to fall back on.
        let unknown = br#"{"namespace":"tenant/never","query_text":"hello"}"#;
        let failed = handle_search_post(unknown, &query);
        assert!(failed.starts_with("HTTP/1.1 503"), "{failed}");

        store.down.store(false, std::sync::atomic::Ordering::SeqCst);
        query.run_manifest_refresh_tick();
        assert_eq!(total_hits(&handle_search_post(search, &query)), 2);
        assert!(lag_of(&query) < lag_late);
        assert_eq!(
            stats_json(&query)["manifest_refresh"]["consecutive_errors"],
            0
        );
    }

    /// `KOSHA_MANIFEST_REFRESH_MS=0`: no refresher, so every search checks
    /// the control store (the pre-#232 behavior) — and an outage still
    /// serves last-known-good.
    #[test]
    fn disabled_refresher_checks_before_every_search() {
        let store = outage_store();
        let ns = NamespaceId("tenant/sync".into());
        let (ingest, query) = ingest_and_query(&store, None);
        index_text_and_publish(&ingest, &ns, &["a"]);
        let search = br#"{"namespace":"tenant/sync","query_text":"hello"}"#;
        assert_eq!(total_hits(&handle_search_post(search, &query)), 1);
        index_text_and_publish(&ingest, &ns, &["b"]);
        assert_eq!(total_hits(&handle_search_post(search, &query)), 2);
        store.down.store(true, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(total_hits(&handle_search_post(search, &query)), 2);
        // Spawning is a no-op when disabled.
        Arc::new(query).spawn_manifest_refresher();
    }

    fn write_and_flush(state: &AppState, ns: &NamespaceId, id: &str) {
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId(id.into()),
                    fields: vec![Field::keyword("id", id)],
                }],
            )
            .unwrap();
        state.indexer.flush_namespace(ns).unwrap();
    }

    fn wal_is_empty(state: &AppState) -> bool {
        kosha_write::wal::WalWriter::recover(&state.data_dir.join("_wal"))
            .unwrap()
            .is_empty()
    }

    fn segment_set(manifest: &kosha_core::Manifest) -> std::collections::HashSet<String> {
        manifest
            .segments
            .iter()
            .map(|entry| entry.segment_id.0.clone())
            .collect()
    }

    /// #230: two independent writers publish to one namespace at the same
    /// time. The loser of the CAS rebases onto the winner's manifest and
    /// publishes too — no restart, no lost segment, and each writer's WAL
    /// is reclaimed through its own checkpoint.
    #[test]
    fn competing_writers_both_publish_by_rebasing_without_losing_segments() {
        let shared = shared_store();
        let ns = NamespaceId("competing".into());
        shared.inner.lock().unwrap().ensure_namespace(ns.clone());
        let first = test_state();
        let second = test_state();
        for (state, id) in [(&first, "first"), (&second, "second")] {
            *state.controller.lock().unwrap() = Box::new(shared.clone());
            write_and_flush(state, &ns, id);
        }
        let flushed: std::collections::HashSet<String> = [&first, &second]
            .iter()
            .flat_map(|state| segment_set(&state.indexer.manifest(&ns).unwrap()))
            .collect();
        // Both writers prepare against version 0 before either commits.
        let barrier = std::sync::Barrier::new(2);
        let publish = |state: &AppState| {
            let first_upload = std::sync::atomic::AtomicBool::new(true);
            state.publish_namespace_with(&ns, |_| {
                if first_upload.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    barrier.wait();
                }
                Ok(())
            })
        };
        let outcomes = std::thread::scope(|scope| {
            let a = scope.spawn(|| publish(&first));
            let b = scope.spawn(|| publish(&second));
            (a.join().unwrap(), b.join().unwrap())
        });
        outcomes.0.unwrap();
        outcomes.1.unwrap();

        let committed = shared.manifest_cloned(&ns).unwrap();
        assert_eq!(segment_set(&committed), flushed, "no segment may be lost");
        assert_eq!(committed.version, 2);
        assert!(wal_is_empty(&first) && wal_is_empty(&second));

        // Whichever writer is now stale keeps publishing without a restart.
        // (Writes need every listed segment local; S3 hydration would fetch
        // the other writer's, here a copy stands in for it.)
        let share_segments = || {
            for (from, to) in [(&first, &second), (&second, &first)] {
                copy_dir(&from.data_dir.join(&ns.0), &to.data_dir.join(&ns.0));
            }
        };
        for (round, state) in [&first, &second, &first].into_iter().enumerate() {
            share_segments();
            write_and_flush(state, &ns, &format!("later-{round}"));
            let response = handle_flush(br#"{"namespace":"competing"}"#, state);
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
            assert!(wal_is_empty(state));
        }
        let committed = shared.manifest_cloned(&ns).unwrap();
        assert_eq!(committed.segments.len(), 5);
        assert!(flushed.is_subset(&segment_set(&committed)));
        for state in [&first, &second] {
            assert!(segment_set(&state.indexer.manifest(&ns).unwrap())
                .is_subset(&segment_set(&committed)));
        }

        // A restart replays nothing that is already published.
        let restarted = Indexer::new(first.data_dir.clone());
        restarted.restore_manifest(ns.clone(), committed);
        assert_eq!(restarted.pending_doc_count(&ns), 0);
    }

    /// #230: a remote compaction removed the segment a local, unpublished
    /// delete targets. The rebase must abort (publishing the delete against
    /// a segment that no longer exists would drop it), surface as 409, and
    /// keep the delete in the WAL.
    #[test]
    fn remote_compaction_of_a_locally_deleted_segment_aborts_the_rebase_with_409() {
        let shared = shared_store();
        let ns = NamespaceId("compacted".into());
        let compactor = test_state();
        let deleter = test_state();
        for state in [&compactor, &deleter] {
            *state.controller.lock().unwrap() = Box::new(shared.clone());
        }
        write_and_flush(&compactor, &ns, "a");
        write_and_flush(&compactor, &ns, "b");
        compactor.publish_namespace(&ns).unwrap();
        let v1 = shared.manifest_cloned(&ns).unwrap();

        // The deleter starts from v1, with v1's segments local (as if
        // hydrated from S3).
        copy_dir(
            &compactor.data_dir.join(&ns.0),
            &deleter.data_dir.join(&ns.0),
        );
        deleter.indexer.restore_manifest(ns.clone(), v1.clone());

        assert!(
            compactor
                .indexer
                .compact_namespace_with_options(&ns, kosha_write::CompactOptions::full())
                .unwrap()
                .merged
        );
        compactor.publish_namespace(&ns).unwrap();
        let v2 = shared.manifest_cloned(&ns).unwrap();
        assert_ne!(segment_set(&v1), segment_set(&v2));

        let response = handle_delete(
            br#"{"namespace":"compacted","filter":{"term":{"id":"a"}}}"#,
            &deleter,
        );
        assert!(response.starts_with("HTTP/1.1 409"), "{response}");
        assert!(
            shared.manifest_cloned(&ns).unwrap().same_contents(&v2),
            "an aborted rebase must not publish"
        );
        assert!(
            kosha_write::wal::WalWriter::recover(&deleter.data_dir.join("_wal"))
                .unwrap()
                .iter()
                .any(|record| !record.deleted_ids.is_empty()),
            "the delete stays durable for a later recovery"
        );
        let flush = handle_flush(br#"{"namespace":"compacted"}"#, &deleter);
        assert!(flush.starts_with("HTTP/1.1 409"), "{flush}");
    }

    /// #230: admin import CASes against the version it read and never
    /// overwrites a manifest another writer committed in between.
    #[test]
    fn import_refuses_to_clobber_a_concurrently_published_manifest() {
        let shared = shared_store();
        let ns = NamespaceId("imported".into());
        let state = test_state();
        *state.controller.lock().unwrap() = Box::new(shared.clone());
        let manifest = |version: u64, segment: &str| kosha_core::Manifest {
            version,
            segments: vec![kosha_core::ManifestEntry {
                segment_id: kosha_core::SegmentId(segment.into()),
                doc_count: 1,
            }],
            segment_footers: Default::default(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            schema: None,
            retired: Default::default(),
        };
        state.import_manifest(&ns, manifest(1, "seed")).unwrap();
        assert_eq!(shared.manifest_cloned(&ns).unwrap().version, 1);

        // Another writer commits v2 right after the import reads v1.
        *shared.commit_on_next_version_read.lock().unwrap() = Some(manifest(2, "ingest"));
        let err = state
            .import_manifest(&ns, manifest(1, "imported"))
            .unwrap_err();
        assert!(matches!(err, KoshaError::ManifestConflict { .. }), "{err}");
        let stored = shared.manifest_cloned(&ns).unwrap();
        assert_eq!(
            (stored.version, stored.segments[0].segment_id.0.as_str()),
            (2, "ingest")
        );
        assert_eq!(
            state.indexer.manifest(&ns).unwrap().segments[0]
                .segment_id
                .0,
            "seed",
            "a refused import must not change the indexer either"
        );
        assert!(publication_error("publication failed", &err).starts_with("HTTP/1.1 409"));
    }

    fn copy_dir(from: &Path, to: &Path) {
        fs::create_dir_all(to).unwrap();
        for entry in fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    struct FailingManifestStore(kosha_control::Controller);
    impl ControlStore for FailingManifestStore {
        fn create_namespace(&mut self, id: NamespaceId) -> Result<(), KoshaError> {
            self.0.create_namespace(id)
        }
        fn ensure_namespace(&mut self, id: NamespaceId) {
            self.0.ensure_namespace(id);
        }
        fn has_namespace(&self, id: &NamespaceId) -> bool {
            self.0.has_namespace(id)
        }
        fn manifest(&self, id: &NamespaceId) -> Option<&kosha_core::Manifest> {
            self.0.manifest(id)
        }
        fn manifest_mut(&mut self, id: &NamespaceId) -> Option<&mut kosha_core::Manifest> {
            self.0.manifest_mut(id)
        }
        fn save_manifest(
            &mut self,
            _: &NamespaceId,
            _: &kosha_core::Manifest,
        ) -> Result<(), KoshaError> {
            Err(KoshaError::NotFound("injected publication failure".into()))
        }
        fn compare_and_swap_manifest(
            &mut self,
            _: &NamespaceId,
            _: u64,
            _: kosha_core::Manifest,
        ) -> Result<(), KoshaError> {
            Err(KoshaError::NotFound("injected publication failure".into()))
        }
        fn list_namespaces(&self) -> Vec<NamespaceId> {
            self.0.list_namespaces().to_vec()
        }
        fn namespace_count(&self) -> usize {
            self.0.namespace_count()
        }
    }

    #[test]
    fn regression_manifest_is_not_visible_until_upload_succeeds() {
        let state = test_state();
        let ns = NamespaceId("upload".into());
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId("d".into()),
                    fields: vec![Field::text("text", "hello")],
                }],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        assert!(state
            .publish_namespace_with(&ns, |_| {
                assert!(state
                    .controller
                    .lock()
                    .unwrap()
                    .manifest_cloned(&ns)
                    .is_none());
                Err(KoshaError::NotFound("injected upload failure".into()))
            })
            .is_err());
        assert!(state
            .controller
            .lock()
            .unwrap()
            .manifest_cloned(&ns)
            .is_none());
        state.publish_namespace_with(&ns, |_| Ok(())).unwrap();
        assert_eq!(
            state
                .controller
                .lock()
                .unwrap()
                .manifest_cloned(&ns)
                .unwrap()
                .segments
                .len(),
            1
        );
    }

    #[test]
    fn regression_delete_covers_namespace_with_only_buffered_documents() {
        let state = test_state();
        let ns = NamespaceId("buffer-only".into());
        state
            .indexer
            .index_documents(
                ns,
                vec![Document {
                    id: DocumentId("d".into()),
                    fields: vec![Field::keyword("status", "remove")],
                }],
            )
            .unwrap();
        let response = handle_delete(
            br#"{"namespace":"buffer-only","filter":{"match_all":{}}}"#,
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("\"deleted\":1"), "{response}");
        let repeated = handle_delete(
            br#"{"namespace":"buffer-only","filter":{"match_all":{}}}"#,
            &state,
        );
        assert!(repeated.contains("\"deleted\":0"), "{repeated}");
    }

    /// #199 / #217 with #170's rule: `prefix` through `/delete`. Every
    /// malformed body is a 400 that deletes nothing (never a match-all), a
    /// prefix on a numeric field is a 400 that deletes nothing, and a good
    /// one deletes exactly the matching documents across segments.
    #[test]
    fn delete_by_prefix_parses_strictly_and_spans_segments() {
        let state = test_state();
        let ns = NamespaceId("prefix-delete".into());
        for batch in [
            [("a1", "/matters/2026/a"), ("a2", "/matters/2025/b")],
            [("b1", "/matters/2026/c"), ("b2", "/MATTERS/2026/d")],
        ] {
            state
                .indexer
                .index_documents(
                    ns.clone(),
                    batch
                        .iter()
                        .enumerate()
                        .map(|(i, (id, path))| Document {
                            id: DocumentId((*id).into()),
                            fields: vec![
                                Field::text("text", "hello"),
                                Field::keyword("path", *path),
                                Field::integer("page", i as i64),
                            ],
                        })
                        .collect(),
                )
                .unwrap();
            state.indexer.flush_namespace(&ns).unwrap();
        }
        assert_eq!(state.indexer.manifest(&ns).unwrap().segments.len(), 2);
        let total = |state: &AppState| -> String {
            handle_search_post(
                br#"{"namespace":"prefix-delete","query_text":"hello","no_cache":true}"#,
                state,
            )
        };
        assert!(total(&state).contains("\"total_hits\":4"));
        let found = handle_search_post(
            br#"{"namespace":"prefix-delete","query_text":"hello",
                 "filter":{"prefix":{"path":"/matters/2026/"}}}"#,
            &state,
        );
        assert!(found.contains("\"total_hits\":2"), "{found}");
        let numeric = handle_search_post(
            br#"{"namespace":"prefix-delete","query_text":"hello",
                 "filter":{"prefix":{"page":"1"}}}"#,
            &state,
        );
        assert!(numeric.starts_with("HTTP/1.1 400"), "{numeric}");
        assert!(numeric.contains("numeric field"), "{numeric}");

        for filter in [
            r#"{"prefix": {"path": 7}}"#,
            r#"{"prefix": {"path": null}}"#,
            r#"{"prefix": {"path": "/a", "other": "/b"}}"#,
            r#"{"prefix": {}}"#,
            r#"{"prefix": {"path": {"value": "/a", "boost": 1}}}"#,
            r#"{"prefix": {"path": {"case_insensitive": true}}}"#,
            r#"{"prefix": {"path": {"value": "/a", "case_insensitive": 1}}}"#,
            r#"{"bool": {"must_not": [{"prefix": {"path": ["/a"]}}]}}"#,
            // Numeric: parses, but the data cannot answer it.
            r#"{"prefix": {"page": "1"}}"#,
            r#"{"bool": {"must_not": [{"prefix": {"page": "1"}}]}}"#,
        ] {
            let body = format!(r#"{{"namespace":"prefix-delete","filter":{filter}}}"#);
            let response = handle_delete(body.as_bytes(), &state);
            assert!(response.starts_with("HTTP/1.1 400"), "{filter}: {response}");
            assert!(
                total(&state).contains("\"total_hits\":4"),
                "{filter} deleted something"
            );
        }

        let response = handle_delete(
            br#"{"namespace":"prefix-delete","filter":{"prefix":{"path":"/matters/2026/"}}}"#,
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("\"deleted\":2"), "{response}");
        // Case-insensitive takes the upper-case one too; "a2" survives.
        let response = handle_delete(
            br#"{"namespace":"prefix-delete","query":{"prefix":{"path":
                {"value":"/matters/2026/","case_insensitive":true}}}}"#,
            &state,
        );
        assert!(response.contains("\"deleted\":1"), "{response}");
        let rest = total(&state);
        assert!(rest.contains("\"total_hits\":1"), "{rest}");
        assert!(rest.contains("\"a2\""), "{rest}");
    }

    #[test]
    fn regression_query_replica_observes_published_deletes() {
        let ingest = test_state();
        let ns = NamespaceId("tenant/ns".into());
        ingest
            .indexer
            .index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId("d".into()),
                    fields: vec![Field::text("text", "hello")],
                }],
            )
            .unwrap();
        ingest.indexer.flush_namespace(&ns).unwrap();
        ingest.publish_namespace(&ns).unwrap();
        let mut query = AppState::new(ingest.data_dir.clone());
        query.ingest_host = Some("http://ingest.invalid".into());
        let copy_manifest = || {
            ingest
                .controller
                .lock()
                .unwrap()
                .manifest_cloned(&ns)
                .unwrap()
        };
        query
            .controller
            .lock()
            .unwrap()
            .save_manifest(&ns, &copy_manifest())
            .unwrap();
        // Strict freshness: no refresher runs in this test, so every search
        // must re-check the control store (#232's `max_staleness_seconds`).
        let search = br#"{"namespace":"tenant/ns","query_text":"hello","max_staleness_seconds":0}"#;
        assert!(handle_search_post(search, &query).contains("\"total_hits\":1"));
        let response = handle_delete(
            br#"{"namespace":"tenant/ns","filter":{"match_all":{}}}"#,
            &ingest,
        );
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let published = copy_manifest();
        assert!(
            published.tombstones.is_empty(),
            "published deletes live in sidecars"
        );
        assert_eq!(published.deletion_vectors.len(), 1);
        let (segment, vector) = published.deletion_vectors.iter().next().unwrap();
        let path = ingest.data_dir.join(vector.path(&ns, segment).unwrap());
        let bytes = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        query
            .controller
            .lock()
            .unwrap()
            .save_manifest(&ns, &published)
            .unwrap();
        let failed = handle_search_post(search, &query);
        assert!(
            failed.starts_with("HTTP/1.1 500"),
            "missing deletion generation must fail closed: {failed}"
        );
        std::fs::write(&path, bytes).unwrap();
        assert!(handle_search_post(search, &query).contains("\"total_hits\":0"));
        let hits = query.result_cache.lock().unwrap().hits;
        assert!(handle_search_post(search, &query).contains("\"total_hits\":0"));
        assert_eq!(
            query.result_cache.lock().unwrap().hits,
            hits + 1,
            "deletes must not disable result caching"
        );
    }

    #[test]
    fn search_honors_and_validates_bm25_params() {
        // Issue #247: per-request k1/b reach the scorer over HTTP (both
        // POST and GET), are part of the result-cache key, and out-of-range
        // or unparsable values are a 400 rather than a silent default.
        let state = test_state();
        let ns = NamespaceId("bm25ns".into());
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![
                    Document {
                        id: DocumentId("long".into()),
                        fields: vec![Field::text(
                            "text",
                            format!("alpha alpha alpha{}", " filler".repeat(60)),
                        )],
                    },
                    Document {
                        id: DocumentId("short".into()),
                        fields: vec![Field::text("text", "alpha")],
                    },
                    Document {
                        id: DocumentId("other".into()),
                        fields: vec![Field::text("text", "unrelated words")],
                    },
                ],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();

        let post = |params: &str| {
            let body =
                format!(r#"{{"namespace":"bm25ns","query_text":"alpha","bm25_params":{params}}}"#);
            handle_search_post(body.as_bytes(), &state)
        };
        let first_hit = |resp: &str| -> String {
            let body = &resp[resp.find("\r\n\r\n").unwrap() + 4..];
            let v: serde_json::Value = serde_json::from_str(body).unwrap();
            v["results"][0]["doc_id"].as_str().unwrap().to_string()
        };
        // Run each twice so the second answer comes from the result cache:
        // the cache must key on the parameters, not just the query text.
        for _ in 0..2 {
            let b0 = post(r#"{"k1":1.2,"b":0.0}"#);
            assert!(b0.starts_with("HTTP/1.1 200"), "{b0}");
            assert_eq!(first_hit(&b0), "long", "b=0 ignores length");
            let b1 = post(r#"{"k1":1.2,"b":1.0}"#);
            assert_eq!(first_hit(&b1), "short", "b=1 penalizes length");
        }
        let get = |qs: &str| {
            handle_search_get(
                &format!("GET /search?ns=bm25ns&q=alpha{qs} HTTP/1.1"),
                &state,
            )
        };
        assert_eq!(first_hit(&get("&b=0")), "long");
        assert_eq!(first_hit(&get("&k1=1.2&b=1")), "short");

        for bad in [
            r#"{"k1":-1}"#,
            r#"{"b":1.5}"#,
            r#"{"b":-0.5}"#,
            r#"{"k1":1.2,"b":2}"#,
        ] {
            let resp = post(bad);
            assert!(resp.starts_with("HTTP/1.1 400"), "{bad}: {resp}");
            assert!(resp.contains("bm25_params"), "{bad}: {resp}");
        }
        for bad in ["&k1=-0.5", "&b=1.01", "&b=NaN", "&k1=abc", "&b="] {
            let resp = get(bad);
            assert!(resp.starts_with("HTTP/1.1 400"), "{bad}: {resp}");
        }
    }

    /// #204 over HTTP: two vector columns of different dimensions in one
    /// namespace; `knn.field` picks the column, an unindexed field or a
    /// wrong-dimension vector is a 400, and so is indexing a vector whose
    /// dimension disagrees with its field's.
    #[test]
    fn knn_field_selects_the_vector_column_over_http() {
        let state = test_state();
        let ns = NamespaceId("columnsns".into());
        let doc = |id: &str, title: Option<[f32; 2]>, body: Option<[f32; 3]>| {
            let mut fields = vec![Field::text("text", "x")];
            if let Some(v) = title {
                fields.push(Field::vector("title_emb", v.to_vec()));
            }
            if let Some(v) = body {
                fields.push(Field::vector("body_emb", v.to_vec()));
            }
            Document {
                id: DocumentId(id.into()),
                fields,
            }
        };
        let index = |docs: Vec<Document>| {
            let body = serde_json::to_vec(&kosha_core::IndexRequest {
                namespace: ns.clone(),
                documents: docs,
            })
            .unwrap();
            handle_index(&body, &state)
        };
        let resp = index(vec![
            doc("t1", Some([1.0, 0.0]), None),
            doc("t2", Some([0.0, 1.0]), Some([0.0, 0.0, 1.0])),
            doc("b1", None, Some([1.0, 0.0, 0.0])),
            doc("b2", None, Some([0.0, 1.0, 0.0])),
        ]);
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        // 2 dims where `body_emb` has 3.
        let bad = Document {
            id: DocumentId("bad2".into()),
            fields: vec![Field::vector("body_emb", vec![1.0, 0.0])],
        };
        let resp = index(vec![bad]);
        assert!(resp.starts_with("HTTP/1.1 400"), "{resp}");
        assert!(resp.contains("body_emb"), "{resp}");
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();

        let post = |knn: &str| {
            let body = format!(r#"{{"namespace":"columnsns","query_text":"","knn":{knn}}}"#);
            handle_search_post(body.as_bytes(), &state)
        };
        let ids = |resp: &str| -> Vec<String> {
            assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
            let body = &resp[resp.find("\r\n\r\n").unwrap() + 4..];
            let v: serde_json::Value = serde_json::from_str(body).unwrap();
            v["results"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| h["doc_id"].as_str().unwrap().to_string())
                .collect()
        };
        let title = ids(&post(
            r#"{"field":"title_emb","vector":[1.0,0.0],"k":10,"num_candidates":100}"#,
        ));
        assert_eq!(title, vec!["t1", "t2"], "only title_emb documents");
        let body = ids(&post(
            r#"{"field":"body_emb","vector":[1.0,0.0,0.0],"k":10,"num_candidates":100}"#,
        ));
        let mut sorted = body.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["b1", "b2", "t2"], "only body_emb documents");
        assert_eq!(body[0], "b1", "{body:?}");

        let resp = post(r#"{"field":"nope","vector":[1.0,0.0],"k":3}"#);
        assert!(resp.starts_with("HTTP/1.1 400"), "{resp}");
        assert!(resp.contains("nope"), "{resp}");
        let resp = post(r#"{"field":"body_emb","vector":[1.0,0.0],"k":3}"#);
        assert!(resp.starts_with("HTTP/1.1 400"), "{resp}");
    }

    #[test]
    fn metric_aggregations_over_http_and_unknown_types_are_400() {
        // Issues #196 / #218 / #202: metric aggregations, top-level and
        // nested under `terms`, over `/search`; an unknown or misspelled
        // aggregation type is a 400 naming the valid types.
        let state = test_state();
        let ns = NamespaceId("metricns".into());
        let doc = |id: &str, cat: &str, amount: f64| Document {
            id: DocumentId(id.into()),
            fields: vec![
                Field::text("text", "invoice"),
                Field::keyword("category", cat),
                Field::float_val("amount", amount),
            ],
        };
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![doc("1", "a", 1.0), doc("2", "a", 2.5), doc("3", "b", 4.0)],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();

        let post = |aggs: &str| {
            let body = format!(
                r#"{{"namespace":"metricns","query_text":"invoice","max_results":0,"aggs":{aggs}}}"#
            );
            handle_search_post(body.as_bytes(), &state)
        };
        let resp = post(
            r#"{"avg_amount":{"avg":{"field":"amount"}},
                "by_cat":{"terms":{"field":"category"},"aggs":{"total":{"sum":{"field":"amount"}}}}}"#,
        );
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        let v: serde_json::Value =
            serde_json::from_str(&resp[resp.find("\r\n\r\n").unwrap() + 4..]).unwrap();
        let aggs = &v["aggregations"];
        assert_eq!(aggs["avg_amount"]["value"], 2.5, "{v}");
        let a = aggs["by_cat"]["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|b| b["key"] == "a")
            .unwrap();
        assert_eq!(a["total"]["value"], 3.5, "{v}");

        for (bad, needle) in [
            (
                r#"{"x":{"averag":{"field":"amount"}}}"#,
                "unknown aggregation type `averag`",
            ),
            (
                r#"{"x":{"stats":{"field":"amount"}}}"#,
                "unknown aggregation type `stats`",
            ),
            (
                r#"{"x":{"terms":{"field":"category"},"aggs":{"y":{"terms":{"field":"category"}}}}}"#,
                "only metric aggregations",
            ),
        ] {
            let resp = post(bad);
            assert!(resp.starts_with("HTTP/1.1 400"), "{bad}: {resp}");
            assert!(resp.contains(needle), "{bad}: {resp}");
        }
    }

    #[test]
    fn histogram_aggregations_over_http_and_bad_intervals_are_400() {
        // Issue #219: `histogram` / `date_histogram` over `/search`, and a
        // garbage interval or unsupported calendar interval is a 400.
        let state = test_state();
        let ns = NamespaceId("histns".into());
        let doc = |id: &str, bytes: i64, at: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![
                Field::text("text", "invoice"),
                Field::integer("bytes", bytes),
                Field::date_val("sentAt", at),
            ],
        };
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![
                    doc("1", 100, "2024-06-15T00:10:00Z"),
                    doc("2", 1024, "2024-06-15T00:59:59Z"),
                    doc("3", 3000, "2024-06-15T02:00:00Z"),
                ],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();

        let post = |aggs: &str| {
            let body = format!(
                r#"{{"namespace":"histns","query_text":"invoice","max_results":0,"aggs":{aggs}}}"#
            );
            handle_search_post(body.as_bytes(), &state)
        };
        let resp = post(
            r#"{"by_size":{"histogram":{"field":"bytes","interval":1024}},
                "over_time":{"date_histogram":{"field":"sentAt","fixed_interval":"1h"}}}"#,
        );
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        let v: serde_json::Value =
            serde_json::from_str(&resp[resp.find("\r\n\r\n").unwrap() + 4..]).unwrap();
        let aggs = &v["aggregations"];
        assert_eq!(
            aggs["by_size"],
            serde_json::json!({"buckets": [
                {"key": 0.0, "doc_count": 1},
                {"key": 1024.0, "doc_count": 1},
                {"key": 2048.0, "doc_count": 1},
            ]}),
            "{v}"
        );
        assert_eq!(
            aggs["over_time"],
            serde_json::json!({"buckets": [
                {"key": 1_718_409_600_000_i64, "key_as_string": "2024-06-15T00:00:00.000Z", "doc_count": 2},
                {"key": 1_718_413_200_000_i64, "key_as_string": "2024-06-15T01:00:00.000Z", "doc_count": 0},
                {"key": 1_718_416_800_000_i64, "key_as_string": "2024-06-15T02:00:00.000Z", "doc_count": 1},
            ]}),
            "{v}"
        );

        for (bad, needle) in [
            (
                r#"{"x":{"date_histogram":{"field":"sentAt","fixed_interval":"1 banana"}}}"#,
                "invalid `fixed_interval`",
            ),
            (
                r#"{"x":{"date_histogram":{"field":"sentAt","calendar_interval":"1M"}}}"#,
                "fixed_interval",
            ),
            (
                r#"{"x":{"histogram":{"field":"bytes","interval":0}}}"#,
                "greater than 0",
            ),
            (
                r#"{"x":{"histogram":{"field":"bytes","interval":-1}}}"#,
                "greater than 0",
            ),
            (
                r#"{"x":{"date_histogram":{"field":"sentAt","fixed_interval":"1ms"}}}"#,
                "more than the limit",
            ),
        ] {
            let resp = post(bad);
            assert!(resp.starts_with("HTTP/1.1 400"), "{bad}: {resp}");
            assert!(resp.contains(needle), "{bad}: {resp}");
        }
    }

    #[test]
    fn field_restricted_search_over_http_and_through_the_result_cache() {
        // Issue #249: `fields` reaches the scorer over POST and GET, is part
        // of the result-cache key (the same text with other fields must not
        // be served a cached answer), and bad specs are a 400.
        let state = test_state();
        let ns = NamespaceId("fieldsns".into());
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![
                    Document {
                        id: DocumentId("t".into()),
                        fields: vec![
                            Field::text("title", "alpha"),
                            Field::text("content", "other words"),
                        ],
                    },
                    Document {
                        id: DocumentId("c".into()),
                        fields: vec![
                            Field::text("title", "other words"),
                            Field::text("content", "alpha"),
                        ],
                    },
                ],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();

        let ids = |resp: &str| -> Vec<String> {
            assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
            let body = &resp[resp.find("\r\n\r\n").unwrap() + 4..];
            let v: serde_json::Value = serde_json::from_str(body).unwrap();
            v["results"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| h["doc_id"].as_str().unwrap().to_string())
                .collect()
        };
        let post = |fields: &str| {
            let body =
                format!(r#"{{"namespace":"fieldsns","query_text":"alpha","fields":{fields}}}"#);
            handle_search_post(body.as_bytes(), &state)
        };
        for _ in 0..2 {
            assert_eq!(ids(&post(r#"["title"]"#)), ["t"]);
            assert_eq!(ids(&post(r#"["content"]"#)), ["c"]);
            assert_eq!(ids(&post(r#"["title^3","content"]"#)), ["t", "c"]);
            assert_eq!(ids(&post(r#"["title","content^3"]"#)), ["c", "t"]);
        }
        let get = |qs: &str| {
            handle_search_get(
                &format!("GET /search?ns=fieldsns&q=alpha{qs} HTTP/1.1"),
                &state,
            )
        };
        assert_eq!(ids(&get("&fields=content")), ["c"]);
        assert_eq!(ids(&get("&fields=title%5E3,content")), ["t", "c"]);
        for bad in [r#"["title^0"]"#, r#"["ti*"]"#, r#"["a","a"]"#] {
            let resp = post(bad);
            assert!(resp.starts_with("HTTP/1.1 400"), "{bad}: {resp}");
        }
        assert!(get("&fields=title%5Ex").starts_with("HTTP/1.1 400"));
    }

    #[test]
    fn limit_per_diversifies_over_http() {
        // Issue #211: one author holds 20 of 26 matches and all the best
        // ones; `limit_per` keeps the page full with at most 2 of them,
        // and is part of the result-cache key.
        let state = test_state();
        let ns = NamespaceId("limitns".into());
        let mut docs = Vec::new();
        for i in 0..20 {
            docs.push(Document {
                id: DocumentId(format!("dom-{i:02}")),
                fields: vec![
                    Field::text("text", "breach breach"),
                    Field::keyword("author", "dom"),
                ],
            });
        }
        for i in 0..6 {
            docs.push(Document {
                id: DocumentId(format!("other-{i}")),
                fields: vec![
                    Field::text("text", "breach of a much longer contract text"),
                    Field::keyword("author", format!("author{i}")),
                ],
            });
        }
        state.indexer.index_documents(ns.clone(), docs).unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();

        let post = |extra: &str| {
            let body = format!(
                r#"{{"namespace":"limitns","query_text":"breach","max_results":5{extra}}}"#
            );
            handle_search_post(body.as_bytes(), &state)
        };
        let doms = |resp: &str| -> (usize, usize) {
            assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
            let body = &resp[resp.find("\r\n\r\n").unwrap() + 4..];
            let v: serde_json::Value = serde_json::from_str(body).unwrap();
            let hits = v["results"].as_array().unwrap();
            let dom = hits
                .iter()
                .filter(|h| h["doc_id"].as_str().unwrap().starts_with("dom-"))
                .count();
            assert_eq!(v["total_hits"], 26, "counted before the cap: {v}");
            (hits.len(), dom)
        };
        // Uncapped first, so a cache keyed without limit_per would serve it.
        assert_eq!(doms(&post("")), (5, 5));
        let capped = r#","limit_per":{"field":"author","max":2}"#;
        assert_eq!(doms(&post(capped)), (5, 2));
        assert_eq!(doms(&post(capped)), (5, 2));
        for bad in [
            r#","limit_per":{"field":"author","max":0}"#,
            r#","limit_per":{"field":"author"}"#,
            r#","limit_per":{"field":"author","max":2},"collapse":{"field":"author"}"#,
        ] {
            let resp = post(bad);
            assert!(resp.starts_with("HTTP/1.1 400"), "{bad}: {resp}");
        }
    }

    #[test]
    fn rank_by_and_geo_over_http() {
        // Issue #212: GeoPoint ingest, a geo_distance filter, a recency
        // decay and a distance decay over HTTP, rank_by in the cache key,
        // and "now" never served from the cache.
        let state = test_state();
        let index = |docs: serde_json::Value| {
            let body = serde_json::json!({"namespace": "rankns", "documents": docs});
            handle_index(body.to_string().as_bytes(), &state)
        };
        let bad = index(serde_json::json!([{"id": "x", "fields": [
            {"name": "loc", "field_type": "GeoPoint", "value": "somewhere"}
        ]}]));
        assert!(bad.starts_with("HTTP/1.1 400"), "{bad}");
        let ok = index(serde_json::json!([
            {"id": "old", "fields": [
                {"name": "text", "field_type": "Text", "value": "breach breach breach"},
                {"name": "published", "field_type": "Date", "value": "2025-11-19"},
                {"name": "loc", "field_type": "GeoPoint", "value": "{\"lat\": 52.52, \"lon\": 13.405}"}
            ]},
            {"id": "recent", "fields": [
                {"name": "text", "field_type": "Text", "value": "breach of the supply contract"},
                {"name": "published", "field_type": "Date", "value": "2026-11-12"},
                {"name": "loc", "field_type": "GeoPoint", "value": "48.8566,2.3522"}
            ]}
        ]));
        assert!(ok.starts_with("HTTP/1.1 200"), "{ok}");
        let ns = NamespaceId("rankns".into());
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();

        let post = |extra: &str| -> serde_json::Value {
            let body = format!(r#"{{"namespace":"rankns","query_text":"breach"{extra}}}"#);
            let resp = handle_search_post(body.as_bytes(), &state);
            assert!(resp.starts_with("HTTP/1.1 200"), "{extra}: {resp}");
            serde_json::from_str(&resp[resp.find("\r\n\r\n").unwrap() + 4..]).unwrap()
        };
        let order = |v: &serde_json::Value| -> Vec<String> {
            v["results"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| h["doc_id"].as_str().unwrap().to_string())
                .collect()
        };
        // Plain first, so a cache keyed without rank_by would serve it.
        assert_eq!(order(&post("")), ["old", "recent"]);
        let recency = r#","rank_by":{"functions":[{"decay":{"field":"published","function":"exp","origin":"2026-11-19","scale":"30d"}}]}"#;
        let v = post(recency);
        assert_eq!(order(&v), ["recent", "old"]);
        assert!(
            v["results"][0]["rank_details"]["base_score"].is_number(),
            "{v}"
        );
        assert_eq!(order(&post(recency)), ["recent", "old"]);

        // Within 500 km of Paris: only "recent".
        let near_paris =
            r#","filter":{"geo_distance":{"distance":"500km","loc":{"lat":48.85,"lon":2.35}}}"#;
        assert_eq!(order(&post(near_paris)), ["recent"]);
        // Nearest to Berlin first.
        let berlin = r#","rank_by":{"functions":[{"decay":{"field":"loc","function":"gauss","origin":{"lat":52.5,"lon":13.4},"scale":"200km"}}],"boost_mode":"replace"}"#;
        assert_eq!(order(&post(berlin)), ["old", "recent"]);

        // A fixed origin is cacheable; "now" is computed per request and
        // never cached.
        let hits_before = state.result_cache.lock().unwrap().hits;
        post(recency);
        assert_eq!(state.result_cache.lock().unwrap().hits, hits_before + 1);
        let hits_before = hits_before + 1;
        let now = r#","rank_by":{"functions":[{"decay":{"field":"published","function":"linear","origin":"now","scale":"3650d"}}]}"#;
        post(now);
        post(now);
        assert_eq!(state.result_cache.lock().unwrap().hits, hits_before);

        for bad in [
            r#","rank_by":{"functions":[]}"#,
            r#","rank_by":{"functions":[{"field_value":{"field":"votes"}}],"bogus":1}"#,
            r#","rank_by":{"functions":[{"field_value":{"field":"votes"}}]},"sort":[{"published":{"order":"desc"}}]"#,
            r#","filter":{"geo_distance":{"distance":"far","loc":"0,0"}}"#,
        ] {
            let body = format!(r#"{{"namespace":"rankns","query_text":"breach"{bad}}}"#);
            let resp = handle_search_post(body.as_bytes(), &state);
            assert!(resp.starts_with("HTTP/1.1 400"), "{bad}: {resp}");
        }
    }

    #[test]
    fn fuzzy_search_over_http() {
        // Issue #201: a fuzzy clause over HTTP, validated, cached under its
        // own key (not the plain query's).
        let state = test_state();
        let ns = NamespaceId("fuzzyns".into());
        let doc = |id: &str, text: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", text)],
        };
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![
                    doc("hit", "we receive goods"),
                    doc("exact", "recieve is a common typo"),
                    doc("miss", "rice and sieve"),
                ],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();
        let post = |extra: &str| -> String {
            let body = format!(r#"{{"namespace":"fuzzyns","query_text":""{extra}}}"#);
            handle_search_post(body.as_bytes(), &state)
        };
        let order = |resp: &str| -> Vec<String> {
            assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
            let v: serde_json::Value =
                serde_json::from_str(&resp[resp.find("\r\n\r\n").unwrap() + 4..]).unwrap();
            v["results"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| h["doc_id"].as_str().unwrap().to_string())
                .collect()
        };
        let fuzzy = r#","fuzzy":{"value":"recieve","fuzziness":1}"#;
        // The exact term first, the one-edit match after it.
        assert_eq!(order(&post(fuzzy)), ["exact", "hit"]);
        assert_eq!(order(&post(fuzzy)), ["exact", "hit"]);
        let exact_only = r#","fuzzy":{"value":"recieve","fuzziness":0}"#;
        assert_eq!(order(&post(exact_only)), ["exact"]);
        for bad in [
            r#","fuzzy":{"value":"recieve","fuzziness":3}"#,
            r#","fuzzy":{"value":"recieve","max_expansions":0}"#,
            r#","fuzzy":{"value":""}"#,
            r#","fuzzy":{"value":"x"},"wildcard":{"field":"text","pattern":"r*"}"#,
        ] {
            let resp = post(bad);
            assert!(resp.starts_with("HTTP/1.1 400"), "{bad}: {resp}");
        }
    }

    #[test]
    fn hybrid_search_fuses_with_rrf_over_http() {
        // Issue #228: `rank.rrf` over HTTP — fused score plus per-retriever
        // ranks in the response, vector-only hits kept (#250), parameters
        // validated, and the result cache keyed on them.
        let state = test_state();
        let ns = NamespaceId("hybridns".into());
        let doc = |id: &str, text: &str, v: [f32; 2]| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", text), Field::vector("emb", v.to_vec())],
        };
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![
                    doc("lex_top", "breach breach breach breach", [-1.0, 0.1]),
                    doc("both", "breach breach notice", [0.99, 0.05]),
                    doc("vec_top", "unrelated", [1.0, 0.0]),
                    doc("weak", "breach and many other words here", [0.0, 1.0]),
                ],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();

        let post = |rank: &str| {
            let body = format!(
                r#"{{"namespace":"hybridns","query_text":"breach","max_results":3,
                    "knn":{{"field":"emb","vector":[1.0,0.0],"k":2,"num_candidates":1000}}{rank}}}"#
            );
            handle_search_post(body.as_bytes(), &state)
        };
        let parse = |resp: &str| -> serde_json::Value {
            let body = &resp[resp.find("\r\n\r\n").unwrap() + 4..];
            serde_json::from_str(body).unwrap()
        };
        for _ in 0..2 {
            let resp = post(r#","rank":{"rrf":{"rank_window_size":3,"rank_constant":60}}"#);
            assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
            let v = parse(&resp);
            let hits = v["results"].as_array().unwrap();
            assert_eq!(hits[0]["doc_id"], "both", "{v}");
            assert_eq!(hits[0]["rank_details"]["lexical_rank"], 2, "{v}");
            assert_eq!(hits[0]["rank_details"]["knn_rank"], 2, "{v}");
            let fused = hits[0]["score"].as_f64().unwrap();
            assert!((fused - 2.0 / 62.0).abs() < 1e-12, "{v}");
            assert!(
                hits.iter()
                    .any(|h| h["doc_id"] == "vec_top"
                        && h["rank_details"].get("lexical_rank").is_none()),
                "vector-only hit kept: {v}"
            );
            // 3 lexical matches + vec_top from the vector window.
            assert_eq!(v["total_hits"], 4, "{v}");

            // Same request, different rank_constant: different scores, so
            // the cache cannot have served the first response.
            let v1 = parse(&post(
                r#","rank":{"rrf":{"rank_window_size":3,"rank_constant":1}}"#,
            ));
            let fused1 = v1["results"][0]["score"].as_f64().unwrap();
            assert!((fused1 - 2.0 / 3.0).abs() < 1e-12, "{v1}");
        }
        for bad in [
            r#","rank":{"rrf":{"rank_window_size":2}}"#,
            r#","rank":{"rrf":{"rank_constant":0}}"#,
            r#","rank":{"rrf":{"bogus":1}}"#,
            r#","mode":"lexical""#,
        ] {
            let resp = post(bad);
            assert!(resp.starts_with("HTTP/1.1 400"), "{bad}: {resp}");
        }
        let resp = post(r#","mode":"hybrid""#);
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
    }

    /// Scores a document by how often `term` occurs in it, after `delay`.
    struct CountingReranker {
        name: &'static str,
        term: &'static str,
        delay: Duration,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl kosha_query::rerank::RerankProvider for CountingReranker {
        fn name(&self) -> &str {
            self.name
        }
        fn rerank(
            &self,
            request: &kosha_query::rerank::RerankRequest,
            _timeout: Duration,
        ) -> Result<kosha_query::rerank::RerankResponse, kosha_query::rerank::RerankError> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            std::thread::sleep(self.delay);
            Ok(kosha_query::rerank::RerankResponse {
                scores: request
                    .documents
                    .iter()
                    .enumerate()
                    .map(|(i, d)| (i, d.matches(self.term).count() as f64))
                    .collect(),
                billed_units: Some(1.0),
            })
        }
    }

    #[test]
    fn hybrid_search_reranks_over_http() {
        // Issue #251: rerank after RRF over HTTP — reorder, both scores,
        // timeout fallback with `rerank_degraded`, cache keyed on rerank
        // params (and degraded responses not cached), metrics in /stats.
        let mut state = test_state();
        let fast = Arc::new(CountingReranker {
            name: "mock",
            term: "notice",
            delay: Duration::ZERO,
            calls: Default::default(),
        });
        let slow = Arc::new(CountingReranker {
            name: "slow",
            term: "notice",
            delay: Duration::from_millis(1_500),
            calls: Default::default(),
        });
        state.rerankers = rerank::RerankRegistry::with_providers(vec![fast.clone(), slow.clone()]);
        let ns = NamespaceId("rerankns".into());
        let doc = |id: &str, text: &str, v: [f32; 2]| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", text), Field::vector("emb", v.to_vec())],
        };
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![
                    doc("lex_top", "breach breach breach breach", [-1.0, 0.1]),
                    doc("both", "breach breach notice", [0.99, 0.05]),
                    doc("vec_top", "unrelated", [1.0, 0.0]),
                    doc("weak", "breach notice notice notice and words", [0.0, 1.0]),
                ],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();

        let post = |rerank: &str| {
            let body = format!(
                r#"{{"namespace":"rerankns","query_text":"breach","max_results":2,
                    "source_excludes":["text","emb"],
                    "knn":{{"field":"emb","vector":[1.0,0.0],"k":2,"num_candidates":1000}},
                    "rank":{{"rrf":{{"rank_window_size":4}}}}{rerank}}}"#
            );
            handle_search_post(body.as_bytes(), &state)
        };
        let parse = |resp: &str| -> serde_json::Value {
            assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
            serde_json::from_str(&resp[resp.find("\r\n\r\n").unwrap() + 4..]).unwrap()
        };
        let ids = |v: &serde_json::Value| -> Vec<String> {
            v["results"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| h["doc_id"].as_str().unwrap().to_string())
                .collect()
        };

        let fused = parse(&post(""));
        assert_eq!(ids(&fused), ["both", "lex_top"], "{fused}");
        assert!(fused.get("rerank_degraded").is_none());

        let rerank = r#","rerank":{"provider":"mock","field":"text","top_n":4}"#;
        for round in 0..2 {
            let v = parse(&post(rerank));
            // "weak" has three "notice"s; RRF had it last.
            assert_eq!(ids(&v), ["weak", "both"], "{v}");
            assert_eq!(v["rerank_degraded"], false);
            let top = &v["results"][0];
            assert_eq!(top["score"], 3.0);
            assert_eq!(top["rerank_score"], 3.0);
            assert!(top["fused_score"].as_f64().unwrap() < 1.0 / 61.0, "{v}");
            assert!(top["rank_details"]["lexical_rank"].is_u64(), "{v}");
            assert_eq!(
                v["results"][1]["fused_score"], fused["results"][0]["score"],
                "{v}"
            );
            // The field was read by the reranker but stays excluded.
            assert!(top["fields"].as_array().unwrap().is_empty(), "{v}");
            // The second round is served from the result cache.
            let calls = fast.calls.load(std::sync::atomic::Ordering::Relaxed);
            assert_eq!(calls, 1, "round {round}");
        }
        // Different rerank params: a different cache key, a new call.
        let v = parse(&post(
            r#","rerank":{"provider":"mock","field":"text","top_n":3}"#,
        ));
        assert_eq!(fast.calls.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(v["rerank_degraded"], false);

        // Timeout: fused order, flagged, and not cached.
        let timeout = r#","rerank":{"provider":"slow","field":"text","top_n":4,"timeout_ms":50}"#;
        for expected_calls in 1..=2 {
            let started = Instant::now();
            let v = parse(&post(timeout));
            assert!(started.elapsed() < Duration::from_millis(1_000));
            assert_eq!(ids(&v), ids(&fused), "{v}");
            assert_eq!(v["rerank_degraded"], true);
            assert_eq!(v["rerank_degraded_reason"], "timeout after 50 ms");
            assert_eq!(v["results"][0]["score"], fused["results"][0]["score"]);
            assert_eq!(v["results"][0]["fused_score"], fused["results"][0]["score"]);
            assert!(v["results"][0].get("rerank_score").is_none());
            assert_eq!(
                slow.calls.load(std::sync::atomic::Ordering::Relaxed),
                expected_calls
            );
        }

        for (bad, why) in [
            (
                r#","rerank":{"provider":"nope","field":"text"}"#,
                "unknown rerank provider",
            ),
            (
                r#","rerank":{"provider":"mock","field":"text","top_n":1}"#,
                "top_n",
            ),
            (
                r#","rerank":{"provider":"mock","field":"text","top_n":10}"#,
                "rank_window_size",
            ),
            (r#","rerank":{"provider":"mock"}"#, "field"),
        ] {
            let resp = post(bad);
            assert!(
                resp.starts_with("HTTP/1.1 400") && resp.contains(why),
                "{bad}: {resp}"
            );
        }

        let stats = handle_stats(&state);
        let stats: serde_json::Value =
            serde_json::from_str(&stats[stats.find("\r\n\r\n").unwrap() + 4..]).unwrap();
        let m = &stats["rerank"]["metrics"];
        assert_eq!(m["mock"]["calls"], 2, "{stats}");
        assert_eq!(m["mock"]["ok"], 2);
        assert_eq!(m["mock"]["documents"], 7);
        assert_eq!(m["mock"]["billed_units"], 2.0);
        assert_eq!(m["slow"]["timeouts"], 2);
        assert_eq!(
            stats["rerank"]["providers"],
            serde_json::json!(["mock", "slow"])
        );
    }

    #[cfg(feature = "s3")]
    #[test]
    fn filter_pruned_segments_still_hydrate_for_namespace_bm25_stats() {
        // Issue #246: a segment the filter rules out holds no hit, but its
        // documents count toward a query term's `df`, so it needs its
        // inverted index + posting blob (never filters/vectors) — unless
        // its term bloom says no query term is there, or the request
        // asked for per-segment statistics.
        let footer = |matter: &str, terms: &[&str]| kosha_core::Footer {
            segment_id: kosha_core::SegmentId("s".into()),
            doc_count: 1,
            total_field_length: 1,
            avg_field_length: 1.0,
            bm25_params: Default::default(),
            created_at: String::new(),
            filter_blooms: Some(HashMap::from([(
                "matter".to_string(),
                kosha_core::BloomFilter::build([matter]),
            )])),
            term_bloom: Some(kosha_core::build_term_bloom(terms.iter().copied())),
            format_version: kosha_core::SEGMENT_FORMAT_VERSION,
            text_filter_ignore_above: None,
            doc_vectors: false,
            posting_blobs_raw: false,
            filter_columns: None,
            analysis: None,
            field_index: None,
            fuzzy_bloom: kosha_core::fuzzy::build_fuzzy_bloom(terms.iter().copied()),
            vector_fields: None,
            multi_vectors: Vec::new(),
        };
        let mut query: SearchQuery = serde_json::from_str(
            r#"{"query_text": "contract breach", "filter": {"term": {"matter": "m1"}}}"#,
        )
        .unwrap();
        let decide = |query: &SearchQuery, f: &kosha_core::Footer| {
            let term_prune = kosha_query::term_bloom_prune_mode(query);
            let analysis = kosha_query::QueryAnalysis::new(query);
            let needs_bloom_check = query.has_prunable_filter() || term_prune.is_some();
            let stats_terms = kosha_query::namespace_stats_terms(query, [None]);
            segment_search_hydration(
                query,
                needs_bloom_check,
                term_prune.map(|mode| (&analysis, mode)),
                stats_terms.as_deref(),
                Some(f),
            )
        };
        let same_matter = footer("m1", &["contract", "breach"]);
        let other_matter = footer("m2", &["contract", "breach"]);
        let other_matter_one_term = footer("m2", &["contract"]);
        let same_matter_one_term = footer("m1", &["breach"]);
        let no_terms = footer("m2", &["unrelated"]);
        assert_eq!(decide(&query, &same_matter), SegmentHydration::Scoring);
        assert_eq!(decide(&query, &other_matter), SegmentHydration::StatsOnly);
        assert_eq!(
            decide(&query, &other_matter_one_term),
            SegmentHydration::StatsOnly
        );
        // AND term pruning skips it for scoring, not for `df(breach)`.
        assert_eq!(
            decide(&query, &same_matter_one_term),
            SegmentHydration::StatsOnly
        );
        assert_eq!(decide(&query, &no_terms), SegmentHydration::Skip);

        query.bm25_params.stats = Some(kosha_core::Bm25StatsScope::Segment);
        assert_eq!(decide(&query, &same_matter), SegmentHydration::Scoring);
        assert_eq!(decide(&query, &other_matter), SegmentHydration::Skip);
        assert_eq!(
            decide(&query, &same_matter_one_term),
            SegmentHydration::Skip
        );

        // Fuzzy (#201): a segment is hydrated when its bloom admits some
        // expansion of the token, skipped when it admits none, and every
        // posting-blob shard is fetched (expansions are per segment).
        let fuzzy: SearchQuery = serde_json::from_str(
            r#"{"query_text": "", "fuzzy": {"value": "braech", "fuzziness": 1}}"#,
        )
        .unwrap();
        assert_eq!(decide(&fuzzy, &other_matter), SegmentHydration::Scoring);
        assert_eq!(decide(&fuzzy, &no_terms), SegmentHydration::Skip);
        let mut old_bloom = no_terms.clone();
        old_bloom.fuzzy_bloom = None;
        assert_eq!(decide(&fuzzy, &old_bloom), SegmentHydration::Scoring);
        assert_eq!(
            posting_blob_files_for_query(&fuzzy, &kosha_query::QueryAnalysis::new(&fuzzy), [None]),
            kosha_segment::all_posting_blob_files()
        );
    }

    /// Ranged posting reads (#245) fetch exactly the terms the searcher
    /// will read: every analysis reading of the query (#220) and its
    /// per-field keys and norms (#249). Fuzzy and wildcard terms are only
    /// known per segment, so those read whole shards (no terms).
    #[test]
    fn posting_terms_follow_analysis_and_per_field_keys() {
        let terms = |json: &str| {
            let q: SearchQuery = serde_json::from_str(json).unwrap();
            posting_terms_for_query(&q, &kosha_query::QueryAnalysis::new(&q), [None])
        };
        let plain = terms(r#"{"query_text": "Contract breach"}"#);
        assert_eq!(plain, ["breach", "contract"]);
        let fielded = terms(r#"{"query_text": "breach", "fields": ["title^2"]}"#);
        assert!(fielded.contains(&kosha_core::fields::field_term_key("title", "breach")));
        assert!(fielded.contains(&kosha_core::fields::field_norms_key("title")));
        assert!(
            terms(r#"{"query_text": "", "fuzzy": {"value": "braech", "fuzziness": 1}}"#).is_empty()
        );
        assert!(
            terms(r#"{"query_text": "", "wildcard": {"field": "text", "pattern": "bre*"}}"#)
                .is_empty()
        );
    }

    #[test]
    fn segment_fallback_bm25_results_are_not_result_cached() {
        // Issue #246: when a segment the query's filter prunes can't be read
        // for `df`, the search scores with per-segment statistics. That
        // degraded answer must not be result-cached, or it would outlive
        // the (usually transient) miss for the whole manifest version.
        let state = test_state();
        let ns = NamespaceId("fallbackns".into());
        let doc = |id: &str, text: &str, matter: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", text), Field::keyword("matter", matter)],
        };
        state
            .indexer
            .index_documents(ns.clone(), vec![doc("r", "rare common", "m1")])
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state
            .indexer
            .index_documents(
                ns.clone(),
                (0..9)
                    .map(|i| doc(&format!("c{i}"), "rare filler", "m2"))
                    .collect(),
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();

        // The m2 segment: pruned by the filter, but holds `rare`.
        let pruned = fs::read_dir(state.data_dir.join(&ns.0))
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| {
                SegmentReader::read_footer(p).is_ok_and(|f| {
                    f.filter_blooms
                        .as_ref()
                        .and_then(|b| b.get("matter"))
                        .is_some_and(|b| b.may_contain("m2"))
                })
            })
            .expect("m2 segment");
        let inverted = pruned.join("inverted.idx");
        let hidden = pruned.join("inverted.idx.hidden");

        let search = || {
            let resp = handle_search_post(
                br#"{"namespace":"fallbackns","query_text":"rare","filter":{"term":{"matter":"m1"}}}"#,
                &state,
            );
            assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
            let body = &resp[resp.find("\r\n\r\n").unwrap() + 4..];
            let v: serde_json::Value = serde_json::from_str(body).unwrap();
            assert_eq!(v["results"][0]["doc_id"], "r");
            v["results"][0]["score"].as_f64().unwrap()
        };
        // dl = 2. Namespace: N = 10, df = 10, avgdl = 2. Segment: N = 1, df = 1.
        let namespace_expected = (1.0f64 + 0.5 / 10.5).ln();
        let segment_expected = (1.0f64 + 0.5 / 1.5).ln();

        fs::rename(&inverted, &hidden).unwrap();
        assert!((search() - segment_expected).abs() < 1e-9);
        fs::rename(&hidden, &inverted).unwrap();
        assert!((search() - namespace_expected).abs() < 1e-9);
        // The namespace-stats answer is cached as usual.
        assert!((search() - namespace_expected).abs() < 1e-9);
    }

    #[test]
    fn search_uses_namespace_bm25_stats_with_segment_switch() {
        // Issue #246: IDF comes from the whole namespace by default, so a
        // document scores the same however the namespace is split into
        // segments; `bm25_params.stats` / `bm25_stats=` switch back to
        // per-segment statistics, keyed separately in the result cache.
        let state = test_state();
        let ns = NamespaceId("statsns".into());
        let doc = |id: &str, text: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", text)],
        };
        // Segment 1: the only "rare" doc. Segment 2: many docs without it.
        state
            .indexer
            .index_documents(ns.clone(), vec![doc("r", "rare common")])
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state
            .indexer
            .index_documents(
                ns.clone(),
                (0..9)
                    .map(|i| doc(&format!("c{i}"), "common filler"))
                    .collect(),
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();

        let score = |resp: &str| -> f64 {
            assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
            let body = &resp[resp.find("\r\n\r\n").unwrap() + 4..];
            let v: serde_json::Value = serde_json::from_str(body).unwrap();
            assert_eq!(v["results"][0]["doc_id"], "r");
            v["results"][0]["score"].as_f64().unwrap()
        };
        let post = |params: &str| {
            let body =
                format!(r#"{{"namespace":"statsns","query_text":"rare","bm25_params":{params}}}"#);
            handle_search_post(body.as_bytes(), &state)
        };
        let get = |qs: &str| {
            handle_search_get(
                &format!("GET /search?ns=statsns&q=rare{qs} HTTP/1.1"),
                &state,
            )
        };
        // N = 10 docs, df = 1, avgdl = 2, dl = 2.
        let namespace_expected = (1.0f64 + (10.0 - 1.0 + 0.5) / 1.5).ln();
        // Segment-local: N = 1, df = 1.
        let segment_expected = (1.0f64 + 0.5 / 1.5).ln();
        for _ in 0..2 {
            assert!((score(&post("{}")) - namespace_expected).abs() < 1e-9);
            assert!((score(&post(r#"{"stats":"segment"}"#)) - segment_expected).abs() < 1e-9);
            assert!((score(&get("")) - namespace_expected).abs() < 1e-9);
            assert!((score(&get("&bm25_stats=segment")) - segment_expected).abs() < 1e-9);
            assert!((score(&get("&bm25_stats=namespace")) - namespace_expected).abs() < 1e-9);
        }
        let resp = post(r#"{"stats":"shard"}"#);
        assert!(resp.starts_with("HTTP/1.1 400"), "{resp}");
        let resp = get("&bm25_stats=shard");
        assert!(resp.starts_with("HTTP/1.1 400"), "{resp}");
        assert!(resp.contains("bm25_stats"), "{resp}");
    }

    #[test]
    fn regression_admin_credential_is_distinct_from_tenant_and_peer_keys() {
        let mut state = test_state();
        state.admin_auth_key = Some("admin-secret".into());
        state.internal_service_key = Some("peer-secret".into());
        let admin = HashMap::from([("authorization".into(), "Bearer admin-secret".into())]);
        let peer = HashMap::from([("authorization".into(), "Bearer peer-secret".into())]);
        assert!(route("GET /stats HTTP/1.1", &admin, b"", "service", &state)
            .starts_with("HTTP/1.1 200"));
        assert!(route(
            "POST /v1/admin/api-keys HTTP/1.1",
            &peer,
            b"{}",
            "service",
            &state
        )
        .starts_with("HTTP/1.1 403"));
        assert!(route(
            "POST /v1/admin/api-keys HTTP/1.1",
            &admin,
            b"{}",
            "service",
            &state
        )
        .starts_with("HTTP/1.1 400"));
    }

    #[test]
    fn regression_flush_does_not_acknowledge_failed_publication() {
        let state = test_state();
        *state.controller.lock().unwrap() =
            Box::new(FailingManifestStore(kosha_control::Controller::new()));
        state
            .indexer
            .index_documents(
                NamespaceId("ns".into()),
                vec![Document {
                    id: DocumentId("d".into()),
                    fields: vec![Field::text("text", "hello")],
                }],
            )
            .unwrap();
        let response = handle_flush(br#"{"namespace":"ns"}"#, &state);
        assert!(response.starts_with("HTTP/1.1 500"), "{response}");
        assert!(
            !kosha_write::wal::WalWriter::recover(&state.data_dir.join("_wal"))
                .unwrap()
                .is_empty(),
            "failed publication must retain recovery records"
        );
    }

    fn index_one(state: &AppState, ns: &str, id: &str) {
        state
            .indexer
            .index_documents(
                NamespaceId(ns.into()),
                vec![Document {
                    id: DocumentId(id.into()),
                    fields: vec![Field::text("text", "hello")],
                }],
            )
            .unwrap();
    }

    fn published_segments(state: &AppState, ns: &str) -> usize {
        state
            .controller
            .lock()
            .unwrap()
            .manifest_cloned(&NamespaceId(ns.into()))
            .map_or(0, |m| m.segments.len())
    }

    /// #241: a buffer below `KOSHA_FLUSH_THRESHOLD` becomes visible once it
    /// has been pending for `max_age`, without anyone calling `/flush` —
    /// and not before.
    #[test]
    fn idle_flush_tick_publishes_buffer_after_max_age() {
        let state = test_state();
        let max_age = Duration::from_millis(1000);
        let t0 = Instant::now();
        let mut since = HashMap::new();

        index_one(&state, "ns", "d1");
        assert!(run_idle_flush_tick(&state, &mut since, max_age, t0).is_empty());
        assert_eq!(
            state.indexer.pending_doc_count(&NamespaceId("ns".into())),
            1
        );

        // More writes don't reset the clock: age counts from first sighting.
        index_one(&state, "ns", "d2");
        let early = t0 + Duration::from_millis(999);
        assert!(run_idle_flush_tick(&state, &mut since, max_age, early).is_empty());
        assert_eq!(published_segments(&state, "ns"), 0, "nothing visible yet");

        let due = t0 + max_age;
        assert_eq!(
            run_idle_flush_tick(&state, &mut since, max_age, due),
            vec![NamespaceId("ns".into())]
        );
        assert_eq!(
            state.indexer.pending_doc_count(&NamespaceId("ns".into())),
            0
        );
        assert_eq!(
            published_segments(&state, "ns"),
            1,
            "one segment for both docs"
        );
        assert!(since.is_empty(), "flushed namespace is forgotten");

        // Empty namespace: nothing to do, no new segment.
        let later = due + max_age * 3;
        assert!(run_idle_flush_tick(&state, &mut since, max_age, later).is_empty());
        assert_eq!(published_segments(&state, "ns"), 1);
    }

    /// A namespace whose buffer was drained by someone else (explicit
    /// `/flush`, threshold auto-flush) between ticks restarts its clock
    /// instead of being flushed early on the next write.
    #[test]
    fn idle_flush_tick_restarts_clock_after_external_flush() {
        let state = test_state();
        let max_age = Duration::from_millis(1000);
        let t0 = Instant::now();
        let mut since = HashMap::new();

        index_one(&state, "ns", "d1");
        run_idle_flush_tick(&state, &mut since, max_age, t0);
        let flushed = handle_flush(br#"{"namespace":"ns"}"#, &state);
        assert!(flushed.starts_with("HTTP/1.1 200"), "{flushed}");
        run_idle_flush_tick(&state, &mut since, max_age, t0 + Duration::from_millis(500));
        assert!(since.is_empty());

        index_one(&state, "ns", "d2");
        let t1 = t0 + Duration::from_millis(1200);
        assert!(
            run_idle_flush_tick(&state, &mut since, max_age, t1).is_empty(),
            "new pending batch starts its own clock at t1"
        );
        assert_eq!(
            run_idle_flush_tick(&state, &mut since, max_age, t1 + max_age),
            vec![NamespaceId("ns".into())]
        );
        assert_eq!(published_segments(&state, "ns"), 2);
    }

    /// A failed publication keeps the namespace due, so the next tick
    /// retries instead of waiting another full `max_age`.
    #[test]
    fn idle_flush_tick_retries_failed_publication() {
        let state = test_state();
        *state.controller.lock().unwrap() =
            Box::new(FailingManifestStore(kosha_control::Controller::new()));
        let max_age = Duration::from_millis(1000);
        let t0 = Instant::now();
        let mut since = HashMap::new();
        index_one(&state, "ns", "d1");
        run_idle_flush_tick(&state, &mut since, max_age, t0);
        assert!(run_idle_flush_tick(&state, &mut since, max_age, t0 + max_age).is_empty());
        assert_eq!(since.get(&NamespaceId("ns".into())), Some(&t0), "still due");
    }

    #[test]
    fn parse_flush_max_age_values() {
        let d = Some(Duration::from_millis(DEFAULT_FLUSH_MAX_AGE_MS));
        assert_eq!(parse_flush_max_age(None), d);
        assert_eq!(parse_flush_max_age(Some("")), d);
        assert_eq!(
            parse_flush_max_age(Some("250")),
            Some(Duration::from_millis(250))
        );
        assert_eq!(parse_flush_max_age(Some("0")), None);
        assert_eq!(parse_flush_max_age(Some("soon")), d);
        assert_eq!(
            idle_flush_tick(Duration::from_millis(1000)),
            Duration::from_millis(250)
        );
        assert_eq!(
            idle_flush_tick(Duration::from_millis(40)),
            Duration::from_millis(50)
        );
    }

    #[test]
    fn regression_legacy_routes_cannot_read_another_tenant() {
        let state = test_state();
        let ns = NamespaceId("victim/private".into());
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId("secret".into()),
                    fields: vec![Field::text("text", "secret")],
                }],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        for (line, body) in [
            (
                "POST /search HTTP/1.1",
                br#"{"namespace":"victim/private","query_text":"secret"}"#.as_slice(),
            ),
            (
                "GET /search?ns=victim%2Fprivate&q=secret HTTP/1.1",
                b"".as_slice(),
            ),
            (
                "POST /search-unsafe HTTP/1.1",
                br#"{"namespace":"victim/private","query_text":"secret"}"#.as_slice(),
            ),
        ] {
            let response = route(line, &HashMap::new(), body, "attacker", &state);
            assert!(!response.starts_with("HTTP/1.1 200"), "{response}");
        }
    }

    #[test]
    fn regression_admin_and_peer_routes_require_service_credentials() {
        let state = test_state();
        for path in [
            "POST /v1/admin/api-keys",
            "POST /v1/admin/import-namespace",
            "GET /internal/segment/victim/private/footer.json",
        ] {
            let response = route(
                &format!("{path} HTTP/1.1"),
                &HashMap::new(),
                b"{}",
                "ordinary",
                &state,
            );
            assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        }
    }

    #[test]
    fn regression_namespace_traversal_is_rejected() {
        let response = route(
            "POST /v1/namespaces/../../victim/documents HTTP/1.1",
            &HashMap::new(),
            br#"{"documents":[]}"#,
            "attacker",
            &test_state(),
        );
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    }

    #[test]
    fn saturated_search_queue_sheds_instead_of_queueing_forever() {
        // The 10M vector namespace's failure mode: 8 permits, service time
        // inflated by storage saturation, and `acquire()` waiting forever —
        // so arrivals accumulated until queue_ms=118,942 against
        // score_ms=2,256. Every one of those requests was waiting two
        // minutes for a result its client had already given up on, while
        // the box thrashed. Overload must shed, not queue.
        let sem = Arc::new(Semaphore::with_queue_limits(
            1,
            2,
            Some(Duration::from_millis(50)),
        ));

        // The only permit is taken; two waiters may queue behind it.
        let held = sem.acquire_or_shed().expect("first acquire must succeed");

        let mut waiting = Vec::new();
        for _ in 0..2 {
            let s = Arc::clone(&sem);
            waiting.push(std::thread::spawn(move || s.acquire_or_shed().is_some()));
        }
        // Let both reach the wait loop so the queue is genuinely full.
        let t0 = Instant::now();
        while sem.waiters() < 2 && t0.elapsed() < Duration::from_secs(2) {
            std::thread::yield_now();
        }
        assert_eq!(sem.waiters(), 2, "both waiters should be queued");

        // Queue is at its bound → the next arrival is shed IMMEDIATELY,
        // not made to burn its deadline first.
        let t = Instant::now();
        assert!(
            sem.acquire_or_shed().is_none(),
            "arrival beyond the queue bound must be shed"
        );
        assert!(
            t.elapsed() < Duration::from_millis(40),
            "shedding must be immediate, not a timeout wait ({:?})",
            t.elapsed()
        );

        // The queued waiters time out rather than blocking forever.
        for h in waiting {
            assert!(!h.join().unwrap(), "queued waiter should time out and shed");
        }

        // Once the permit comes back, service resumes normally.
        drop(held);
        assert!(
            sem.acquire_or_shed().is_some(),
            "a freed permit must be acquirable again"
        );
    }

    #[test]
    fn unbounded_semaphore_still_waits_for_a_permit() {
        // `Semaphore::new` keeps the old unbounded behavior for callers
        // where waiting beats failing (hydration), so this change is scoped
        // to the search path.
        let sem = Arc::new(Semaphore::new(1));
        let held = sem.acquire();
        let s = Arc::clone(&sem);
        let t = std::thread::spawn(move || {
            let _p = s.acquire();
            true
        });
        std::thread::sleep(Duration::from_millis(30));
        assert!(!t.is_finished(), "must still be waiting, not shed");
        drop(held);
        assert!(t.join().unwrap());
    }

    #[test]
    fn env_flag_default_on_semantics() {
        // Unset → on (the KOSHA_WARMUP_POSTING_BLOBS default).
        assert!(env_flag_default_on(None));
        // Only the explicit disable spellings turn it off.
        for off in ["0", "false", "off", " 0 ", "  false"] {
            assert!(!env_flag_default_on(Some(off)), "{off:?} should disable");
        }
        // Anything else — including enable-ish and garbage values — stays on,
        // so a typo can never silently disable warmup blob prefetch.
        for on in ["1", "true", "on", "", "yes", "FALSE"] {
            assert!(env_flag_default_on(Some(on)), "{on:?} should stay on");
        }
    }

    #[cfg(feature = "s3")]
    #[test]
    fn segment_is_complete_requires_all_core_files_but_not_vector_idx() {
        let dir = std::env::temp_dir().join("kosha-test-segment-complete");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        assert!(
            !AppState::segment_is_complete(&dir),
            "empty dir must not count as complete"
        );

        for f in ["footer.json", "doc_store.bin", "inverted.idx"] {
            fs::write(dir.join(f), b"x").unwrap();
        }
        assert!(
            !AppState::segment_is_complete(&dir),
            "missing filters.bin must still be incomplete"
        );

        fs::write(dir.join("filters.bin"), b"x").unwrap();
        assert!(
            AppState::segment_is_complete(&dir),
            "all four core files present (vector.idx not required) must be complete"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// #244: a columnar segment has no `filters.bin`; it is complete once
    /// `filters.toc` and every column its footer lists are local.
    #[cfg(feature = "s3")]
    #[test]
    fn columnar_segment_is_complete_with_toc_and_every_column() {
        let dir = std::env::temp_dir().join(format!(
            "kosha-test-columnar-complete-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let mut w =
            kosha_segment::SegmentWriter::new(kosha_core::SegmentId("c".into()), dir.clone())
                .with_filters_write_version(2);
        w.add_document(
            kosha_core::DocumentId("d".into()),
            vec![
                kosha_core::Field::keyword("a", "x"),
                kosha_core::Field::integer("b", 1),
            ],
        );
        let footer = w.finalize(kosha_core::Bm25Params::default()).unwrap();
        assert!(!dir.join("filters.bin").exists());
        assert!(AppState::segment_is_complete(&dir));
        let col = footer.filter_columns.as_ref().unwrap()[1].file_name();
        let stash = dir.join("col.stash");
        fs::rename(dir.join(&col), &stash).unwrap();
        assert!(
            !AppState::segment_is_complete(&dir),
            "a missing column makes a columnar segment incomplete"
        );
        fs::rename(&stash, dir.join(&col)).unwrap();
        fs::remove_file(dir.join("filters.toc")).unwrap();
        assert!(
            !AppState::segment_is_complete(&dir),
            "so does a missing TOC"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression for the S3 durability gap this fixes: 174 of 376
    /// segments in a real benchmark run were never uploaded because
    /// `publish_namespace` used to sync only `manifest.segments.last()` —
    /// under concurrent flushes, two publish calls could both see a
    /// different segment as "last" and each assume the other one wasn't
    /// theirs to upload. Driving sync off a durable synced-set instead must
    /// find *every* not-yet-synced segment, not just whichever is newest.
    #[cfg(feature = "s3")]
    #[test]
    fn segments_needing_sync_finds_everything_not_yet_recorded_synced() {
        let ns = NamespaceId("bench".into());
        let manifest = kosha_core::Manifest {
            version: 3,
            segments: vec![
                kosha_core::ManifestEntry {
                    segment_id: kosha_core::SegmentId("seg-1".into()),
                    doc_count: 10,
                },
                kosha_core::ManifestEntry {
                    segment_id: kosha_core::SegmentId("seg-2".into()),
                    doc_count: 10,
                },
                kosha_core::ManifestEntry {
                    segment_id: kosha_core::SegmentId("seg-3".into()),
                    doc_count: 10,
                },
            ],
            segment_footers: Default::default(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            schema: None,
            retired: Default::default(),
        };
        // Simulate: an earlier publish call already confirmed seg-2
        // synced (e.g. it was "last" at the time it ran), but seg-1 and
        // seg-3 — including seg-3, the segment that's actually "last" in
        // the current manifest — are still outstanding.
        let mut synced = std::collections::HashSet::new();
        synced.insert((ns.clone(), kosha_core::SegmentId("seg-2".into())));

        let pending = segments_needing_sync(&manifest, &synced, &ns);
        let pending_ids: Vec<&str> = pending.iter().map(|e| e.segment_id.0.as_str()).collect();
        assert_eq!(
            pending_ids,
            vec!["seg-1", "seg-3"],
            "must find every unsynced segment (including non-'last' seg-1), not just the newest"
        );
    }

    #[cfg(feature = "s3")]
    #[test]
    fn hydration_wants_file_fetches_every_columnar_filter_file() {
        // Full-segment hydration (index/compact/admin) needs every column
        // (#244); only the search path narrows to the query's columns.
        for f in ["filters.toc", "filters-000.col", "filters-123.col"] {
            assert!(hydration_wants_file(f, false), "{f}");
        }
    }

    #[cfg(feature = "s3")]
    #[test]
    fn hydration_wants_file_skips_vector_idx_unless_needed() {
        assert!(
            !hydration_wants_file("vector.idx", false),
            "a query that doesn't need vectors must not fetch vector.idx"
        );
        assert!(
            hydration_wants_file("vector.idx", true),
            "a knn query must still fetch vector.idx"
        );
        // vector.offsets (the v2 posting-index sidecar) must be gated
        // identically to vector.idx, not lumped into the "always fetch"
        // bucket — a naive `name != "vector.idx"` extension would silently
        // regress this to "always fetch the sidecar," reintroducing the
        // exact bandwidth waste `needs_vectors` gating exists to prevent.
        assert!(
            !hydration_wants_file("vector.offsets", false),
            "a query that doesn't need vectors must not fetch vector.offsets either"
        );
        assert!(
            hydration_wants_file("vector.offsets", true),
            "a knn query must still fetch vector.offsets"
        );
        // Further vector columns (#204) are gated the same way.
        for f in ["vector.1.idx", "vector.1.offsets", "vector.12.idx"] {
            assert!(!hydration_wants_file(f, false), "{f}");
            assert!(hydration_wants_file(f, true), "{f}");
        }
        for f in [
            "footer.json",
            "doc_store.bin",
            "inverted.idx",
            "filters.bin",
        ] {
            assert!(
                hydration_wants_file(f, false),
                "{f} must always be fetched regardless of needs_vectors"
            );
        }
    }

    /// Core regression for the presence-cache optimization: a
    /// `(segment, shard)` pair already recorded in `cached` must not be
    /// re-stated — the whole point is to turn an `fs::metadata` syscall
    /// into a `HashSet` lookup on a warm namespace.
    #[cfg(feature = "s3")]
    #[test]
    fn plan_posting_blob_fetch_skips_the_format_probe_when_all_shards_are_cached() {
        use std::cell::Cell;

        let seg = PathBuf::from("/data/ns/seg-1");
        let posting_files = vec!["postings-00.bin".to_string(), "postings-01.bin".to_string()];
        let mut cached = HashMap::new();
        cached.insert(
            "ns/seg-1".to_string(),
            std::collections::HashSet::from([
                "postings-00.bin".to_string(),
                "postings-01.bin".to_string(),
            ]),
        );
        let probes = Cell::new(0);
        let plan = |files: &[String]| {
            plan_posting_blob_fetch(
                std::slice::from_ref(&seg),
                files,
                Path::new("/data"),
                &cached,
                |_| {
                    probes.set(probes.get() + 1);
                    true
                },
                |_| false,
            )
        };

        // Both shards confirmed: the segment is skipped without opening it.
        let (to_fetch, confirmed) = plan(&posting_files);
        assert_eq!(probes.get(), 0);
        assert!(to_fetch.is_empty() && confirmed.is_empty());

        // A shard that is not confirmed still goes through the probe and is
        // planned for fetching.
        let (to_fetch, _) = plan(&["postings-02.bin".to_string()]);
        assert_eq!(probes.get(), 1);
        assert_eq!(to_fetch, vec![("ns/seg-1/postings-02.bin".to_string(), 0)]);
    }

    #[test]
    fn plan_posting_blob_fetch_skips_stat_for_cached_pairs() {
        use std::cell::Cell;

        let seg = PathBuf::from("/data/ns/seg-1");
        let posting_files = vec!["postings-00.bin".to_string(), "postings-01.bin".to_string()];

        let mut cached = HashMap::new();
        cached.insert(
            "ns/seg-1".to_string(),
            std::collections::HashSet::from(["postings-00.bin".to_string()]),
        );

        let stat_calls = Cell::new(0);
        let (logical_paths, newly_confirmed) = plan_posting_blob_fetch(
            std::slice::from_ref(&seg),
            &posting_files,
            Path::new("/data"),
            &cached,
            |_| true, // every segment is split-format
            |path| {
                stat_calls.set(stat_calls.get() + 1);
                path.ends_with("postings-01.bin") // only the uncached shard "exists"
            },
        );

        assert_eq!(
            stat_calls.get(),
            1,
            "the cached (seg-1, postings-00.bin) pair must never reach the is_present stat"
        );
        assert!(
            logical_paths.is_empty(),
            "postings-01.bin was found present by the stat, so nothing needs fetching"
        );
        assert_eq!(
            newly_confirmed,
            vec![("ns/seg-1".to_string(), "postings-01.bin".to_string())],
            "only the freshly-stated, freshly-present pair should be reported back for caching"
        );
    }

    /// A shard neither cached nor present on disk must be queued for
    /// fetch, not silently dropped.
    #[cfg(feature = "s3")]
    #[test]
    fn plan_posting_blob_fetch_queues_absent_uncached_shards() {
        let seg = PathBuf::from("/data/ns/seg-1");
        let posting_files = vec!["postings-00.bin".to_string()];
        let cached = HashMap::new();

        let (logical_paths, newly_confirmed) = plan_posting_blob_fetch(
            std::slice::from_ref(&seg),
            &posting_files,
            Path::new("/data"),
            &cached,
            |_| true,
            |_| false, // nothing is present locally
        );

        assert_eq!(
            logical_paths,
            vec![("ns/seg-1/postings-00.bin".to_string(), 0)],
            "an absent, uncached shard must be queued for fetch"
        );
        assert!(newly_confirmed.is_empty());
    }

    /// A non-split-format segment must be skipped entirely — no stat calls,
    /// no fetch, no cache entries — matching the pre-cache
    /// `inverted_uses_posting_blobs` gate this wraps.
    #[cfg(feature = "s3")]
    #[test]
    fn plan_posting_blob_fetch_skips_non_split_segments() {
        use std::cell::Cell;

        let seg = PathBuf::from("/data/ns/seg-legacy");
        let posting_files = vec!["postings-00.bin".to_string()];
        let cached = HashMap::new();
        let stat_calls = Cell::new(0);

        let (logical_paths, newly_confirmed) = plan_posting_blob_fetch(
            std::slice::from_ref(&seg),
            &posting_files,
            Path::new("/data"),
            &cached,
            |_| false, // not split-format
            |_| {
                stat_calls.set(stat_calls.get() + 1);
                true
            },
        );

        assert_eq!(
            stat_calls.get(),
            0,
            "a non-split segment must never be stat'd"
        );
        assert!(logical_paths.is_empty());
        assert!(newly_confirmed.is_empty());
    }

    /// The invalidation contract the task asked for: a lookup against a
    /// newer manifest version than what's stored must miss (same
    /// free-invalidation-on-publish `completed_hydrations` gets), and an
    /// insert against that newer version must replace the stale entry
    /// rather than merge into it.
    #[cfg(feature = "s3")]
    #[test]
    fn posting_blob_cache_misses_and_resets_on_manifest_version_bump() {
        let mut cache: PostingBlobPresenceCache = HashMap::new();
        posting_blob_cache_insert(
            &mut cache,
            "ns1",
            /* manifest_version */ 1,
            /* eviction_gen */ 0,
            vec![("ns1/seg-1".to_string(), "postings-00.bin".to_string())],
        );

        // Same version, same eviction generation: hit.
        let hit = posting_blob_cache_lookup(&cache, "ns1", 1, 0);
        assert!(hit.get("ns1/seg-1").unwrap().contains("postings-00.bin"));

        // A new manifest published (version bumped 1 -> 2): must miss.
        let miss = posting_blob_cache_lookup(&cache, "ns1", 2, 0);
        assert!(
            miss.is_empty(),
            "a newer manifest version than the cached entry must be treated as a full miss"
        );

        // Inserting under the new version must replace, not merge with,
        // the stale v1 entry.
        posting_blob_cache_insert(
            &mut cache,
            "ns1",
            2,
            0,
            vec![("ns1/seg-2".to_string(), "postings-00.bin".to_string())],
        );
        let after_bump = posting_blob_cache_lookup(&cache, "ns1", 2, 0);
        assert!(
            after_bump.get("ns1/seg-1").is_none(),
            "the v1 entry must not survive into the v2 generation"
        );
        assert!(after_bump.get("ns1/seg-2").is_some());
    }

    /// The safety net this PR adds beyond the manifest-version key: a
    /// disk-cache eviction (which — unlike a missing scoring-set file —
    /// does *not* surface as a search error for posting blobs, see the
    /// `posting_blob_presence` field's doc comment) must invalidate a
    /// same-manifest-version cache entry, not just a version bump.
    #[cfg(feature = "s3")]
    #[test]
    fn posting_blob_cache_misses_on_eviction_generation_change_even_if_version_unchanged() {
        let mut cache: PostingBlobPresenceCache = HashMap::new();
        posting_blob_cache_insert(
            &mut cache,
            "ns1",
            1,
            /* eviction_gen */ 5,
            vec![("ns1/seg-1".to_string(), "postings-00.bin".to_string())],
        );

        assert!(!posting_blob_cache_lookup(&cache, "ns1", 1, 5).is_empty());
        assert!(
            posting_blob_cache_lookup(&cache, "ns1", 1, 6).is_empty(),
            "same manifest version but a later eviction generation must still miss — \
             an eviction may have removed the exact blob this entry vouched for"
        );
    }

    /// `Cache::evict` must bump `eviction_generation` only when it actually
    /// removes something — a no-op evict of an already-absent key must not
    /// spuriously invalidate every presence-cache entry on the server.
    #[cfg(feature = "s3")]
    #[test]
    fn cache_eviction_generation_bumps_only_on_real_removal() {
        let dir = std::env::temp_dir().join("kosha-test-eviction-generation");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let cache = Cache::new(dir.clone());
        assert_eq!(cache.eviction_generation(), 0);

        // Evicting a key that was never written is a no-op.
        let _ = cache.evict("never-written.bin");
        assert_eq!(
            cache.eviction_generation(),
            0,
            "a no-op evict of an absent key must not bump the generation"
        );

        cache.put_bytes("real-file.bin", b"hello").unwrap();
        cache.evict("real-file.bin").unwrap();
        assert_eq!(
            cache.eviction_generation(),
            1,
            "evicting a file that was actually present must bump the generation"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[cfg(feature = "s3")]
    #[test]
    fn chunk_by_byte_budget_keeps_each_chunk_at_or_under_budget() {
        let files = vec![
            ("a".to_string(), 40u64),
            ("b".to_string(), 40u64),
            ("c".to_string(), 40u64),
            ("d".to_string(), 10u64),
        ];
        let chunks = chunk_by_byte_budget(&files, 100);

        // a+b = 80 (fits), +c would be 120 (over budget) so c starts a new
        // chunk; c+d = 50 (fits).
        assert_eq!(
            chunks,
            vec![
                vec![("a".to_string(), 40), ("b".to_string(), 40)],
                vec![("c".to_string(), 40), ("d".to_string(), 10)],
            ]
        );
        for chunk in &chunks {
            let total: u64 = chunk.iter().map(|(_, size)| size).sum();
            assert!(total <= 100, "chunk total {total} exceeded budget");
        }
    }

    #[cfg(feature = "s3")]
    #[test]
    fn chunk_by_byte_budget_never_starves_a_single_oversized_file() {
        let files = vec![("huge".to_string(), 500u64)];
        let chunks = chunk_by_byte_budget(&files, 100);
        assert_eq!(
            chunks,
            vec![vec![("huge".to_string(), 500)]],
            "a file bigger than the whole budget must still get its own chunk, not be dropped"
        );
    }

    #[cfg(feature = "s3")]
    #[test]
    fn chunk_by_byte_budget_empty_input_yields_no_chunks() {
        assert!(chunk_by_byte_budget(&[], 100).is_empty());
    }

    fn test_footer(id: &str, docs: u32, format_version: u32) -> kosha_core::Footer {
        kosha_core::Footer {
            segment_id: kosha_core::SegmentId(id.into()),
            doc_count: docs,
            total_field_length: docs as u64 * 7,
            avg_field_length: 7.0,
            bm25_params: kosha_core::Bm25Params::default(),
            created_at: "2026-08-11T00:00:00Z".into(),
            filter_blooms: None,
            term_bloom: None,
            format_version,
            text_filter_ignore_above: None,
            doc_vectors: false,
            analysis: Default::default(),
            field_index: Default::default(),
            filter_columns: Default::default(),
            fuzzy_bloom: Default::default(),
            posting_blobs_raw: Default::default(),
            vector_fields: Default::default(),
            multi_vectors: Default::default(),
        }
    }

    /// #179/#184's misclassification. Manifests cache a footer for every
    /// segment they list, legacy ones included, so "the manifest has a
    /// footer" says nothing about whether a `doc_store.offsets` exists.
    #[cfg(feature = "s3")]
    #[test]
    fn manifest_footer_presence_alone_does_not_make_a_segment_lazy_capable() {
        let disk = |v: Option<u32>| move || v;
        assert!(
            !segment_is_lazy_capable(Some(0), disk(None)),
            "format_version 0 (or absent, which reads as 0) is no claim of a sidecar"
        );
        assert!(
            segment_is_lazy_capable(Some(1), disk(None)),
            "1 introduced it"
        );
        assert!(
            segment_is_lazy_capable(Some(2), disk(None)),
            "later versions keep it"
        );
        assert!(
            segment_is_lazy_capable(Some(0), disk(Some(2))),
            "an unknown manifest version defers to the segment's own footer.json"
        );
        assert!(
            !segment_is_lazy_capable(Some(0), disk(Some(0))),
            "both legacy: eager"
        );
        assert!(segment_is_lazy_capable(None, disk(Some(1))));
        assert!(
            !segment_is_lazy_capable(None, disk(None)),
            "no footer anywhere: assume legacy and fetch doc_store.bin"
        );
        // The disk footer is not read when the manifest already answers.
        assert!(segment_is_lazy_capable(Some(2), || -> Option<u32> {
            panic!("footer.json must not be read")
        }));
    }

    /// `format_version` is `#[serde(default)]`: a footer that never carried
    /// it must deserialize as 0, which the classifier treats as unknown. A
    /// legacy footer must never come back claiming the current version.
    #[test]
    fn legacy_footer_json_without_format_version_stays_unversioned() {
        let legacy = r#"{"segment_id":"paragraph_index_hnsw-000958","doc_count":1,
            "total_field_length":38,"avg_field_length":38.0,
            "bm25_params":{"k1":1.2,"b":0.75},"created_at":"2026-07-31T11:17:57Z"}"#;
        let footer: kosha_core::Footer = serde_json::from_str(legacy).unwrap();
        assert_eq!(footer.format_version, 0);
        // A manifest row written before #241 inlined footers the same way.
        let manifest: kosha_core::Manifest = serde_json::from_value(serde_json::json!({
            "version": 3,
            "segments": [{"segment_id": "paragraph_index_hnsw-000958", "doc_count": 1}],
            "segment_footers": {
                "paragraph_index_hnsw-000958": serde_json::from_str::<serde_json::Value>(legacy).unwrap()
            }
        }))
        .unwrap();
        let cached = manifest
            .segment_footer(&kosha_core::SegmentId("paragraph_index_hnsw-000958".into()))
            .unwrap();
        assert_eq!(cached.format_version, 0);
        #[cfg(feature = "s3")]
        assert!(!segment_is_lazy_capable(
            Some(cached.format_version),
            || None
        ));
    }

    /// The fallback trigger: which Lazy segments must also fetch
    /// `doc_store.bin`, and why.
    #[cfg(feature = "s3")]
    #[test]
    fn doc_store_fallback_reason_covers_missing_and_rejected_sidecars() {
        let dir =
            std::env::temp_dir().join(format!("kosha-test-fallback-reason-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let mut writer =
            kosha_segment::SegmentWriter::new(kosha_core::SegmentId("seg".into()), dir.clone());
        for i in 0..3 {
            writer.add_document(
                DocumentId(format!("d{i}")),
                vec![Field::text("text", "hello")],
            );
        }
        let footer = writer.finalize(kosha_core::Bm25Params::default()).unwrap();
        let bin = fs::read(dir.join("doc_store.bin")).unwrap();
        let offsets = fs::read(dir.join("doc_store.offsets")).unwrap();

        fs::remove_file(dir.join("doc_store.bin")).unwrap();
        assert_eq!(doc_store_fallback_reason(&dir, Some(&footer)), None);

        let mut stale = footer.clone();
        stale.doc_count = 6;
        fs::remove_file(dir.join("footer.json")).unwrap();
        assert_eq!(
            doc_store_fallback_reason(&dir, Some(&stale)),
            Some(DocStoreFallbackReason::Rejected),
            "a stale manifest count with no footer.json to check against is rejected"
        );
        fs::write(
            dir.join("footer.json"),
            serde_json::to_vec(&footer).unwrap(),
        )
        .unwrap();
        assert_eq!(
            doc_store_fallback_reason(&dir, Some(&stale)),
            None,
            "the segment's own footer.json agrees with the sidecar: it is used"
        );

        let mut bad = offsets.clone();
        bad.truncate(offsets.len() - 3);
        fs::write(dir.join("doc_store.offsets"), &bad).unwrap();
        assert_eq!(
            doc_store_fallback_reason(&dir, Some(&footer)),
            Some(DocStoreFallbackReason::Rejected),
            "truncated mid-record"
        );

        fs::remove_file(dir.join("doc_store.offsets")).unwrap();
        assert_eq!(
            doc_store_fallback_reason(&dir, Some(&footer)),
            Some(DocStoreFallbackReason::Missing)
        );

        fs::write(dir.join("doc_store.bin"), &bin).unwrap();
        assert_eq!(
            doc_store_fallback_reason(&dir, Some(&footer)),
            None,
            "doc_store.bin already local: nothing to fetch"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// #204: a kNN's scoring set holds only the queried column's files, as
    /// the segment's footer lists them.
    #[cfg(feature = "s3")]
    #[test]
    fn scoring_complete_needs_only_the_queried_vector_column() {
        let dir =
            std::env::temp_dir().join(format!("kosha-test-scoring-columns-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut w =
            kosha_segment::SegmentWriter::new(kosha_core::SegmentId("s".into()), dir.clone());
        w.add_document(
            DocumentId("d".into()),
            vec![
                Field::text("text", "x"),
                Field::vector("title_emb", vec![1.0, 0.0]),
                Field::vector("body_emb", vec![1.0, 0.0, 0.0]),
            ],
        );
        w.finalize(Default::default()).unwrap();
        assert_eq!(
            kosha_segment::knn_vector_files(&dir, "body_emb"),
            vec![
                ("vector.1.idx".to_string(), true),
                ("vector.1.offsets".to_string(), true)
            ]
        );
        assert!(kosha_segment::knn_vector_files(&dir, "nope").is_empty());
        let complete =
            |field| AppState::segment_is_scoring_complete(&dir, Some(field), false, None);
        assert!(complete("title_emb") && complete("body_emb") && complete("nope"));
        fs::remove_file(dir.join("vector.1.idx")).unwrap();
        assert!(
            complete("title_emb"),
            "body_emb's files are not title_emb's"
        );
        assert!(!complete("body_emb"));
        assert!(
            complete("nope"),
            "a column the segment does not have needs no files"
        );
        assert_eq!(
            kosha_segment::vector_index_files(&dir),
            vec![
                "vector.idx",
                "vector.offsets",
                "vector.1.idx",
                "vector.1.offsets"
            ]
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[cfg(feature = "s3")]
    #[test]
    fn segment_is_scoring_complete_offsets_sidecar_replaces_doc_store_bin() {
        // Scoring completeness is strictly weaker than full completeness: a
        // Lazy segment with `doc_store.offsets` is scorable *without*
        // `doc_store.bin` (lexical), `vector.idx` is required only for kNN,
        // and a legacy segment without offsets still needs `doc_store.bin`
        // (its eager open parses it).
        let dir = std::env::temp_dir().join("kosha-test-scoring-complete");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let footer = test_footer("kosha-test-scoring-complete", 1, 2);

        assert!(
            !AppState::segment_is_scoring_complete(&dir, None, false, None),
            "empty dir must not be scoring-complete"
        );

        fs::write(dir.join("inverted.idx"), b"x").unwrap();
        // The header is all the per-search check reads: one doc, matching
        // the footer.
        fs::write(dir.join("doc_store.offsets"), 1u32.to_le_bytes()).unwrap();
        assert!(
            !AppState::segment_is_scoring_complete(&dir, None, false, None),
            "without a manifest footer, footer.json on disk is still required"
        );
        assert!(
            AppState::segment_is_scoring_complete(&dir, None, false, Some(&footer)),
            "a manifest footer snapshot must stand in for a missing footer.json"
        );
        fs::write(
            dir.join("footer.json"),
            serde_json::to_vec(&footer).unwrap(),
        )
        .unwrap();
        assert!(
            AppState::segment_is_scoring_complete(&dir, None, false, None),
            "offsets sidecar (no doc_store.bin, no filters.bin) must be broad lexical scoring-complete"
        );
        assert!(
            !AppState::segment_is_scoring_complete(&dir, None, true, None),
            "queries using filters/sorts/aggs must still require filters.bin"
        );
        fs::write(dir.join("filters.bin"), b"x").unwrap();
        assert!(
            AppState::segment_is_scoring_complete(&dir, None, true, None),
            "filters.bin present must make filter/sort/agg queries scoring-complete"
        );
        assert!(
            !AppState::segment_is_scoring_complete(&dir, Some("emb"), true, None),
            "kNN scoring-complete must require vector.idx (still missing)"
        );

        fs::write(dir.join("vector.idx"), b"x").unwrap();
        assert!(
            AppState::segment_is_scoring_complete(&dir, Some("emb"), true, None),
            "vector.idx present must make it kNN scoring-complete"
        );
        assert!(
            !dir.join("vector.offsets").exists(),
            "sanity: this assertion must be exercising the no-sidecar case"
        );
        assert!(
            AppState::segment_is_scoring_complete(&dir, Some("emb"), true, None),
            "vector.offsets (the v2 posting-index sidecar) must be optional for scoring \
             completeness — legacy v1 segments never have one, and the reader falls back to \
             an eager parse of vector.idx alone when it's absent, same as doc_store's own \
             sidecar-optional tolerance above"
        );
        let _ = fs::remove_dir_all(&dir);

        // Legacy segment: no offsets sidecar → doc_store.bin carries
        // eligibility; remove it and scoring is no longer complete.
        let legacy = std::env::temp_dir().join("kosha-test-scoring-complete-legacy");
        let _ = fs::remove_dir_all(&legacy);
        fs::create_dir_all(&legacy).unwrap();
        for f in [
            "footer.json",
            "inverted.idx",
            "filters.bin",
            "doc_store.bin",
        ] {
            fs::write(legacy.join(f), b"x").unwrap();
        }
        assert!(
            AppState::segment_is_scoring_complete(&legacy, None, true, None),
            "legacy segment with doc_store.bin (no offsets) must be scoring-complete"
        );
        fs::remove_file(legacy.join("doc_store.bin")).unwrap();
        assert!(
            !AppState::segment_is_scoring_complete(&legacy, None, true, None),
            "legacy segment without offsets and without doc_store.bin is not scorable"
        );
        let _ = fs::remove_dir_all(&legacy);
    }

    /// A shed and a hydration failure both leave a non-empty `missing`
    /// list; only the status code tells the client which happened, and only
    /// one of them means "your data is broken."
    #[cfg(feature = "s3")]
    #[test]
    fn a_shed_hydration_is_a_429_while_a_real_failure_is_a_503() {
        let shed = HydrationOutcome {
            missing: vec!["ns/seg-1".to_string()],
            shed: true,
            ..Default::default()
        };
        let response = hydration_error_response(&shed).expect("shed must not proceed");
        assert!(response.starts_with("HTTP/1.1 429"), "got: {response}");

        let failed = HydrationOutcome {
            missing: vec!["ns/seg-1".to_string()],
            shed: false,
            ..Default::default()
        };
        let response = hydration_error_response(&failed).expect("failure must not proceed");
        assert!(response.starts_with("HTTP/1.1 503"), "got: {response}");

        assert!(
            hydration_error_response(&HydrationOutcome::default()).is_none(),
            "a clean hydration proceeds"
        );
    }

    #[cfg(feature = "s3")]
    #[test]
    fn hydration_failed_message_lists_and_truncates() {
        let few = vec!["ns/seg-1".to_string(), "ns/seg-2".to_string()];
        let msg = hydration_failed_message(&few);
        assert!(msg.contains("2 segment(s)"));
        assert!(msg.contains("ns/seg-1"));
        assert!(msg.contains("ns/seg-2"));
        assert!(!msg.contains("more)"));

        let many: Vec<String> = (0..15).map(|i| format!("ns/seg-{i}")).collect();
        let msg = hydration_failed_message(&many);
        assert!(msg.contains("15 segment(s)"));
        assert!(msg.contains("(+5 more)"));
    }

    /// Every write route `route()` actually dispatches to a mutating
    /// handler must also be classified `true` here — this is the drift risk
    /// called out in `is_write_path`'s own doc comment. If this test starts
    /// failing after adding a new write route to `route()`, that's the
    /// signal to update `is_write_path` too, not to weaken this test.
    #[test]
    fn is_write_path_matches_every_mutating_route() {
        for line in [
            "POST /v1/namespaces/foo/documents HTTP/1.1\r\n",
            "POST /v1/namespaces/foo/flush HTTP/1.1\r\n",
            "POST /v1/namespaces/foo/delete HTTP/1.1\r\n",
            "POST /v1/namespaces/foo/schema HTTP/1.1\r\n",
            "POST /v1/admin/api-keys HTTP/1.1\r\n",
            "POST /v1/admin/rebuild-filter-blooms HTTP/1.1\r\n",
            "POST /v1/admin/backfill-offset-tables HTTP/1.1\r\n",
            "POST /v1/admin/compact-namespace HTTP/1.1\r\n",
            "GET /v1/admin/compaction-status HTTP/1.1\r\n",
            "GET /v1/admin/compaction-jobs/cj-1-1 HTTP/1.1\r\n",
            "POST /index HTTP/1.1\r\n",
            "POST /replace HTTP/1.1\r\n",
            "POST /flush HTTP/1.1\r\n",
            "POST /delete HTTP/1.1\r\n",
            "POST /v1/namespaces HTTP/1.1\r\n",
            "DELETE /v1/namespaces/foo HTTP/1.1\r\n",
        ] {
            assert!(is_write_path(line), "expected write-path: {line:?}");
        }
    }

    #[test]
    fn is_write_path_excludes_every_read_route() {
        for line in [
            "GET /v1/healthz HTTP/1.1\r\n",
            "GET /healthz HTTP/1.1\r\n",
            "POST /v1/namespaces/foo/search HTTP/1.1\r\n",
            "POST /v1/namespaces/foo/exists HTTP/1.1\r\n",
            "GET /v1/stats HTTP/1.1\r\n",
            "GET /v1/namespaces/foo/stats HTTP/1.1\r\n",
            "GET /v1/namespaces/foo/schema HTTP/1.1\r\n",
            "GET /search?ns=foo&q=bar HTTP/1.1\r\n",
            "POST /search HTTP/1.1\r\n",
            "POST /exists HTTP/1.1\r\n",
            "GET /stats HTTP/1.1\r\n",
            "GET /v1/namespaces/foo HTTP/1.1\r\n",
            "POST /v1/namespaces/foo HTTP/1.1\r\n",
            "DELETE /v1/namespaces/foo/documents HTTP/1.1\r\n",
        ] {
            assert!(!is_write_path(line), "expected read-path: {line:?}");
        }
    }

    #[test]
    fn raw_response_relays_arbitrary_status_and_body_verbatim() {
        let body = r#"{"indexed_count":5,"namespace":"foo"}"#;
        let resp = raw_response(200, body);
        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"), "{resp}");
        assert!(resp.ends_with(body), "{resp}");

        // The specific regression this exists to prevent: json_error's
        // fallback silently coerces any unrecognized code to 500, which
        // would be wrong for a proxy relaying a real upstream 404 —
        // kosha_client's own error handling branches on exactly that code.
        let not_found = raw_response(404, r#"{"error":"namespace 'x' not found"}"#);
        assert!(
            not_found.starts_with("HTTP/1.1 404 Not Found\r\n"),
            "{not_found}"
        );

        // An upstream code this function has no reason phrase for must
        // still relay the real numeric code, not silently become 500.
        let unmapped = raw_response(418, r#"{"error":"teapot"}"#);
        assert!(unmapped.starts_with("HTTP/1.1 418 "), "{unmapped}");
    }

    /// End-to-end over a real socket: a fake "ingest" server that always
    /// answers 201 with a fixed body, and forward_to_ingest against it.
    /// Exercises the actual reqwest path, not just the pure classifier
    /// above — this is the only place in this file's tests that spins up a
    /// second TCP listener to stand in for a peer service.
    #[test]
    fn forward_to_ingest_relays_method_path_and_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let seen_request_line = Arc::new(Mutex::new(String::new()));
        let seen_body = Arc::new(Mutex::new(Vec::new()));
        let (rl_clone, body_clone) = (Arc::clone(&seen_request_line), Arc::clone(&seen_body));

        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap();
            *rl_clone.lock().unwrap() = request_line;

            let mut content_length = 0usize;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some((k, v)) = line.trim_end().split_once(':') {
                    if k.trim().eq_ignore_ascii_case("content-length") {
                        content_length = v.trim().parse().unwrap_or(0);
                    }
                }
            }
            let mut body = vec![0u8; content_length];
            if content_length > 0 {
                reader.read_exact(&mut body).unwrap();
            }
            *body_clone.lock().unwrap() = body;

            let mut stream = stream;
            let resp_body = r#"{"indexed_count":1,"namespace":"foo"}"#;
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 201 Created\r\ncontent-length: {}\r\n\r\n{resp_body}",
                        resp_body.len()
                    )
                    .as_bytes(),
                )
                .unwrap();
        });

        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let mut headers = HashMap::new();
        headers.insert("authorization".to_string(), "Bearer test-key".to_string());
        let body = br#"{"namespace":"foo","documents":[]}"#;
        let response = forward_to_ingest(
            &client,
            &format!("http://{addr}"),
            "POST /index HTTP/1.1\r\n",
            &headers,
            body,
        );

        server.join().unwrap();
        assert_eq!(
            *seen_request_line.lock().unwrap(),
            "POST /index HTTP/1.1\r\n"
        );
        assert_eq!(*seen_body.lock().unwrap(), body.to_vec());
        assert!(
            response.starts_with("HTTP/1.1 201 Created\r\n"),
            "{response}"
        );
        assert!(
            response.ends_with(r#"{"indexed_count":1,"namespace":"foo"}"#),
            "{response}"
        );
    }

    // ── Segment hydration coalescing ────────────────────────────────────
    //
    // `partition_for_hydration` / `complete_owned` / `wait_for` are the
    // free functions `ensure_segments_local` uses to single-flight
    // concurrent S3 hydration. They're deliberately independent of
    // `S3Storage` (no real network / AWS creds needed) so the coalescing
    // logic itself can be exercised directly with many real OS threads.

    #[cfg(feature = "s3")]
    #[test]
    fn concurrent_requests_for_same_segment_coalesce_to_one_fetch() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::Barrier;

        let in_flight: Mutex<HashMap<PathBuf, Arc<SegmentFetch>>> = Mutex::new(HashMap::new());
        // Stand-in for `segment_is_complete`: false until the one fetcher
        // "hydrates" it.
        let hydrated = AtomicBool::new(false);
        let fetch_count = AtomicUsize::new(0);
        let seg_path = PathBuf::from("ns/seg-1");
        let n = 8;
        let barrier = Barrier::new(n);

        std::thread::scope(|scope| {
            for _ in 0..n {
                scope.spawn(|| {
                    barrier.wait(); // maximize actual overlap between threads
                    let (owned, waiting) = partition_for_hydration(
                        &in_flight,
                        std::slice::from_ref(&seg_path),
                        |_| hydrated.load(Ordering::SeqCst),
                    );
                    if !owned.is_empty() {
                        fetch_count.fetch_add(1, Ordering::SeqCst);
                        // Simulate a slow S3 fetch so the other threads have
                        // time to observe the in-flight entry and become
                        // waiters instead of also becoming owners.
                        std::thread::sleep(Duration::from_millis(50));
                        hydrated.store(true, Ordering::SeqCst);
                        complete_owned(&in_flight, &owned);
                    } else {
                        wait_for(&waiting);
                    }
                });
            }
        });

        assert_eq!(
            fetch_count.load(Ordering::SeqCst),
            1,
            "only one thread should have performed the fetch; the rest must coalesce onto it"
        );
        assert!(
            in_flight.lock().unwrap().is_empty(),
            "in-flight entry must be removed after completion so a future cache miss can retry"
        );
        assert!(hydrated.load(Ordering::SeqCst));
    }

    // ── bloom_pruned: a filtered query's hydration success must not be
    //    cached as "whole namespace hydrated" ──────────────────────────
    //
    // Regression for the live-production bug: `completed_hydrations` is
    // keyed on `(namespace, manifest_version)` only, shared across every
    // query shape. A filtered query's `to_hydrate` set inside
    // `hydrate_segments_for_search` is narrowed by *that query's own*
    // bloom check — caching its success as "namespace fully hydrated"
    // let a later, differently-filtered query silently skip hydrating the
    // segments it actually needed, understating counts by up to 80% until
    // the pod was restarted (which drops the in-memory cache — the only
    // reason it ever "recovered").

    #[cfg(feature = "s3")]
    #[test]
    fn hydrate_segments_for_search_flags_bloom_pruned_only_when_filtered() {
        let state = test_state();
        let ns = NamespaceId("bloom-pruned-test".into());
        let manifest = kosha_core::Manifest {
            version: 1,
            segments: vec![kosha_core::ManifestEntry {
                segment_id: kosha_core::SegmentId("seg-1".into()),
                doc_count: 10,
            }],
            segment_footers: HashMap::new(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            schema: None,
            retired: Default::default(),
        };
        let base_query = kosha_core::SearchQuery {
            query_text: String::new(),
            max_results: 10,
            bm25_params: Default::default(),
            from: 0,
            filter: None,
            sort: Vec::new(),
            search_after: None,
            highlight: None,
            aggs: std::collections::HashMap::new(),
            wildcard: None,
            fuzzy: None,
            match_phrase: None,
            knn: None,
            exact_total_hits: None,
            total_hits_cap: None,
            operator: None,
            no_cache: None,
            collapse: None,
            limit_per: None,
            rank_by: None,
            source_includes: None,
            source_excludes: None,
            rank: None,
            mode: None,
            fields: Vec::new(),
            rerank: None,
        };

        let unfiltered = state.hydrate_segments_for_search(
            &ns,
            &manifest,
            &base_query,
            &RequestHydration::unlimited(),
        );
        assert!(
            !unfiltered.bloom_pruned,
            "an unfiltered query's to_hydrate set covers every segment — safe to cache as \
             'namespace hydrated'"
        );

        let mut filtered_query = base_query.clone();
        filtered_query.filter = Some(kosha_core::FilterClause::Term {
            term: HashMap::from([("matter_id".to_string(), "some-matter".to_string())]),
        });
        let filtered = state.hydrate_segments_for_search(
            &ns,
            &manifest,
            &filtered_query,
            &RequestHydration::unlimited(),
        );
        assert!(
            filtered.bloom_pruned,
            "a filtered query's to_hydrate set only covers the segments its own filter value's \
             bloom check admitted — must NOT be eligible to cache as 'namespace hydrated', or a \
             later, differently-filtered query would silently skip segments it actually needs"
        );
    }

    // ── HydrationVerdict: a pass that never needed filters.bin/vector.idx
    //    must not let a later query that DOES need one take the cached-
    //    verdict fast path ─────────────────────────────────────────────
    //
    // Same bug family as #166 (LazyFilters) and #167 (bloom_pruned), one
    // layer up: `completed_hydrations` used to be a flat `bool`, so ANY
    // successful, non-bloom-pruned pass — including warmup's own probe
    // query (unfiltered, unsorted, non-kNN — see `warm_run::scaffolding_probe_query`) or an
    // ordinary plain-text search — got cached as "namespace hydrated" for
    // that manifest version. But `hydrate_segments_for_search` only ever
    // checks `filters.bin`/`vector.idx` when the *driving* query needs
    // them (`query_needs_filter_store` / `query.knn.is_some()`); a plain
    // query's success proves nothing about whether either file was ever
    // fetched. A later query on the SAME pod for the SAME manifest
    // version that DOES need `filters.bin` — e.g. a `documentId`-filtered,
    // sorted, `search_after`-paginated chunk-visibility check, structurally
    // identical to what `post_index_processing` in the `backend` repo runs
    // right after indexing — would see `cached_complete == true` and take
    // the `ensure_query_posting_blobs`-only fast path, which never looks
    // at `filters.bin` at all. If that file hadn't been fetched yet, the
    // query would silently score against an empty/absent filter store —
    // every retry, indefinitely, since nothing about a plain query
    // succeeding again would ever trigger a real check. Not self-healing
    // short of a new segment publish (bumps the version key) or a pod
    // restart (drops the map) — the same non-recovery signature as
    // #166/#167.
    //
    // Fix: `completed_hydrations` now stores a [`HydrationVerdict`] with
    // independent `filters_verified`/`vectors_verified` flags, each set
    // only by a pass that itself needed (and therefore actually checked)
    // that file class.

    #[test]
    fn hydration_verdict_covers_only_what_was_actually_verified() {
        let fields =
            |names: &[&str]| -> FilterFields { names.iter().map(|s| s.to_string()).collect() };
        let none = fields(&[]);
        let mut v = HydrationVerdict::default();
        assert!(
            v.covers(None, None),
            "a query needing neither file class is trivially covered by any verdict"
        );
        assert!(
            !v.covers(Some(&none), None),
            "an unverified verdict must not cover a query that needs filters.bin"
        );
        assert!(
            !v.covers(None, Some("emb")),
            "an unverified verdict must not cover a query that needs vector.idx"
        );

        // A pass that itself needed only filters verifies only that.
        v.merge(Some(&none), None);
        assert!(v.covers(None, None));
        assert!(v.covers(Some(&none), None));
        assert!(
            !v.covers(None, Some("emb")),
            "verifying filters.bin must not also mark vector.idx as verified"
        );
        assert!(!v.covers(Some(&none), Some("emb")));

        // A later pass needing vector.idx merges in, not overwrites.
        v.merge(None, Some("emb"));
        assert!(
            v.covers(Some(&none), Some("emb")),
            "flags accumulate across passes — filters_verified from an earlier pass must \
             survive a later pass that only verified vectors"
        );
        assert!(
            !v.covers(None, Some("other_emb")),
            "verifying one vector column's files must not cover a kNN on another (#204)"
        );
    }

    /// #244: columnar segments hydrate only the columns a query names, so a
    /// verdict earned by a `matterId` filter must not cover a later query
    /// on `documentId` — its column may never have been fetched.
    #[test]
    fn hydration_verdict_tracks_which_filter_columns_were_verified() {
        let fields =
            |names: &[&str]| -> FilterFields { names.iter().map(|s| s.to_string()).collect() };
        let mut v = HydrationVerdict::default();
        v.merge(Some(&fields(&["matterId"])), None);
        assert!(v.covers(Some(&fields(&["matterId"])), None));
        assert!(
            v.covers(Some(&fields(&[])), None),
            "TOC-only needs are covered"
        );
        assert!(
            !v.covers(Some(&fields(&["documentId"])), None),
            "a column no pass fetched must not be assumed local"
        );
        assert!(!v.covers(Some(&fields(&["matterId", "documentId"])), None));
        v.merge(Some(&fields(&["documentId"])), None);
        assert!(
            v.covers(Some(&fields(&["matterId", "documentId"])), None),
            "verified columns accumulate across passes"
        );
    }

    /// #244 acceptance: a filter on one keyword field of a columnar
    /// segment hydrates only `filters.toc` and that field's column, and
    /// the query answers exactly as from the whole segment. "S3" is a fully
    /// written segment directory; the query node's directory gets only the
    /// scoring files plus what `segment_filter_files` asks for.
    #[cfg(feature = "s3")]
    #[test]
    fn columnar_filter_query_hydrates_only_the_toc_and_its_column() {
        let root = std::env::temp_dir().join(format!("kosha-244-hydration-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let remote = root.join("remote/ns/seg");
        let local_root = root.join("local");
        let local = local_root.join("ns/seg");
        let mut w =
            kosha_segment::SegmentWriter::new(kosha_core::SegmentId("seg".into()), remote.clone())
                .with_filters_write_version(2);
        for d in 0..2000u32 {
            w.add_document(
                kosha_core::DocumentId(format!("d{d}")),
                vec![
                    kosha_core::Field::text("t", format!("contract clause {d}")),
                    kosha_core::Field::keyword("matterId", format!("m{}", d % 5)),
                    kosha_core::Field::text("documentId", format!("doc-{d}")),
                    kosha_core::Field::text("path", format!("/matters/m{}/f{d}.pdf", d % 5)),
                    kosha_core::Field::integer("page", (d % 300) as i64),
                    kosha_core::Field::float_val("confidence", d as f64 / 2000.0),
                ],
            );
        }
        let footer = w.finalize(kosha_core::Bm25Params::default()).unwrap();
        assert!(footer.filter_columns.is_some());

        let mut query: SearchQuery = serde_json::from_value(serde_json::json!({
            "query_text": "contract",
            "max_results": 5,
            "filter": {"term": {"matterId.keyword": "m3"}}
        }))
        .unwrap();
        query.exact_total_hits = Some(true);
        let fields = query_filter_fields(&query).expect("a filtered query needs filter columns");
        let wanted = segment_filter_files(Some(&footer), &local, &fields);
        let matter = footer
            .filter_columns
            .as_ref()
            .unwrap()
            .iter()
            .find(|c| c.name == "matterId")
            .unwrap();
        assert_eq!(
            wanted,
            vec![kosha_core::FILTERS_TOC_FILE.to_string(), matter.file_name()],
            "only the TOC and the one column"
        );

        // Hydrate: scoring files + the wanted filter files, nothing else.
        std::fs::create_dir_all(&local).unwrap();
        for f in [
            "footer.json",
            "inverted.idx",
            "doc_store.offsets",
            "doc_store.bin",
        ] {
            std::fs::copy(remote.join(f), local.join(f)).unwrap();
        }
        for entry in std::fs::read_dir(&remote).unwrap().flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("postings-") {
                std::fs::copy(entry.path(), local.join(&name)).unwrap();
            }
        }
        assert!(
            !AppState::scoring_complete(&local, None, false, &wanted, Some(&footer)),
            "not scorable before its filter files land"
        );
        let mut hydrated = 0u64;
        for f in &wanted {
            hydrated += std::fs::copy(remote.join(f), local.join(f)).unwrap();
        }
        assert!(AppState::scoring_complete(
            &local,
            None,
            false,
            &wanted,
            Some(&footer)
        ));
        let all_filter_bytes: u64 = footer
            .filter_files_for(None)
            .iter()
            .map(|f| std::fs::metadata(remote.join(f)).unwrap().len())
            .sum();
        println!(
            "columnar hydration: {hydrated} of {all_filter_bytes} filter bytes for a one-column filter"
        );
        assert!(
            hydrated * 4 < all_filter_bytes,
            "one column of five must be a small share: {hydrated} of {all_filter_bytes}"
        );
        for c in footer.filter_columns.as_ref().unwrap() {
            if c.id != matter.id {
                assert!(!local.join(c.file_name()).exists());
            }
        }

        // Same answer as against the whole segment.
        let manifest = kosha_core::Manifest {
            version: 1,
            segments: vec![kosha_core::ManifestEntry {
                segment_id: kosha_core::SegmentId("seg".into()),
                doc_count: 2000,
            }],
            segment_footers: HashMap::new(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            schema: None,
            retired: Default::default(),
        };
        let ns = NamespaceId("ns".into());
        let partial = kosha_query::Searcher::new(local_root.clone())
            .search(&ns, &manifest, &query, None)
            .unwrap();
        let full = kosha_query::Searcher::new(root.join("remote"))
            .search(&ns, &manifest, &query, None)
            .unwrap();
        assert_eq!(partial.total_hits, 400);
        assert_eq!(partial.total_hits, full.total_hits);
        let ids = |r: &kosha_core::SearchResult| {
            r.results
                .iter()
                .map(|d| d.doc_id.0.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&partial), ids(&full));

        // A later query on another column is not covered by what landed.
        let mut other = query.clone();
        other.filter = Some(
            serde_json::from_value(serde_json::json!({"range": {"page": {"gte": "10"}}})).unwrap(),
        );
        let other_files =
            segment_filter_files(Some(&footer), &local, &query_filter_fields(&other).unwrap());
        assert!(!AppState::scoring_complete(
            &local,
            None,
            false,
            &other_files,
            Some(&footer)
        ));

        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(feature = "s3")]
    #[test]
    fn unfiltered_pass_success_does_not_let_a_filtered_sorted_paginated_query_skip_its_own_check() {
        let state = test_state();
        let ns = NamespaceId("chunk-lookup-hydration-test".into());
        let manifest = kosha_core::Manifest {
            version: 1,
            segments: vec![kosha_core::ManifestEntry {
                segment_id: kosha_core::SegmentId("seg-1".into()),
                doc_count: 10,
            }],
            segment_footers: HashMap::new(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            schema: None,
            retired: Default::default(),
        };
        let base_query = kosha_core::SearchQuery {
            query_text: String::new(),
            max_results: 10,
            bm25_params: Default::default(),
            from: 0,
            filter: None,
            sort: Vec::new(),
            search_after: None,
            highlight: None,
            aggs: std::collections::HashMap::new(),
            wildcard: None,
            fuzzy: None,
            match_phrase: None,
            knn: None,
            exact_total_hits: None,
            total_hits_cap: None,
            operator: None,
            no_cache: None,
            collapse: None,
            limit_per: None,
            rank_by: None,
            source_includes: None,
            source_excludes: None,
            rank: None,
            mode: None,
            fields: Vec::new(),
            rerank: None,
        };
        let verdict_key = (ns.0.clone(), manifest.version);

        // Step 1: a plain unfiltered/unsorted/non-kNN pass — exactly
        // warmup's own probe query shape — succeeds and (per the existing
        // `!bloom_pruned` gate) is eligible to merge into the cache.
        let warmup_like = base_query.clone();
        let needs_filters_1 = query_filter_fields(&warmup_like);
        let needs_vectors_1 = warmup_like.knn.as_ref().map(|knn| knn.field.as_str());
        assert!(
            needs_filters_1.is_none() && needs_vectors_1.is_none(),
            "warmup's probe query must not need filters.bin/vector.idx — this is exactly \
             what makes its success unable to prove either file is local"
        );
        let outcome = state.hydrate_segments_for_search(
            &ns,
            &manifest,
            &warmup_like,
            &RequestHydration::unlimited(),
        );
        assert!(outcome.missing.is_empty() && !outcome.bloom_pruned);
        state
            .completed_hydrations
            .lock()
            .unwrap()
            .entry(verdict_key.clone())
            .or_default()
            .merge(needs_filters_1.as_ref(), needs_vectors_1);

        // Step 2: a documentId-filtered, sorted, search_after-paginated
        // chunk-visibility query — the shape `post_index_processing`
        // sends — arrives next on the SAME pod, SAME manifest version.
        let mut chunk_lookup = base_query.clone();
        chunk_lookup.filter = Some(kosha_core::FilterClause::Term {
            term: HashMap::from([("documentId".to_string(), "doc-42".to_string())]),
        });
        chunk_lookup.sort = vec![kosha_core::SortSpec {
            fields: HashMap::from([(
                "chunkIndex".to_string(),
                kosha_core::SortOrder {
                    order: "asc".to_string(),
                },
            )]),
        }];
        chunk_lookup.search_after = Some(vec!["0".to_string()]);
        let needs_filters_2 = query_filter_fields(&chunk_lookup);
        let needs_vectors_2 = chunk_lookup.knn.as_ref().map(|knn| knn.field.as_str());
        assert!(
            needs_filters_2.is_some(),
            "a documentId term filter needs filters.bin"
        );

        let cached = state
            .completed_hydrations
            .lock()
            .unwrap()
            .get(&verdict_key)
            .cloned();
        let cached_covers =
            cached.is_some_and(|v| v.covers(needs_filters_2.as_ref(), needs_vectors_2));
        assert!(
            !cached_covers,
            "an unfiltered pass that never checked filters.bin must not let a later, \
             filtered+sorted+paginated query believe filters.bin is local — it must fall \
             through to a real hydrate_segments_for_search pass, exactly as if nothing had \
             been cached yet"
        );

        // Step 3 (the fix's other half): once a pass that itself needs
        // filters.bin — and is not bloom-pruned, e.g. an unfiltered query
        // sorted by a real field — succeeds, filters_verified becomes
        // true and DOES cover a later query that only needs filters.bin.
        let mut sorted_unfiltered = base_query.clone();
        sorted_unfiltered.sort = vec![kosha_core::SortSpec {
            fields: HashMap::from([(
                "chunkIndex".to_string(),
                kosha_core::SortOrder {
                    order: "asc".to_string(),
                },
            )]),
        }];
        let needs_filters_3 = query_filter_fields(&sorted_unfiltered);
        let needs_vectors_3 = sorted_unfiltered.knn.as_ref().map(|knn| knn.field.as_str());
        assert!(
            needs_filters_3.is_some(),
            "sorting by a real field needs filters.bin"
        );
        let outcome3 = state.hydrate_segments_for_search(
            &ns,
            &manifest,
            &sorted_unfiltered,
            &RequestHydration::unlimited(),
        );
        assert!(
            outcome3.missing.is_empty() && !outcome3.bloom_pruned,
            "unfiltered (no query.filter/term-prune) means to_hydrate covers every segment — \
             safe to merge"
        );
        state
            .completed_hydrations
            .lock()
            .unwrap()
            .entry(verdict_key.clone())
            .or_default()
            .merge(needs_filters_3.as_ref(), needs_vectors_3);

        let cached_after = state
            .completed_hydrations
            .lock()
            .unwrap()
            .get(&verdict_key)
            .cloned();
        assert!(
            cached_after.is_some_and(|v| v.covers(needs_filters_3.as_ref(), None)),
            "a pass that itself verified filters.bin must let a later query needing only \
             filters.bin take the fast path"
        );
    }

    #[cfg(feature = "s3")]
    #[test]
    fn hydration_failure_unblocks_waiters_instead_of_hanging_or_faking_success() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Barrier;

        let in_flight: Mutex<HashMap<PathBuf, Arc<SegmentFetch>>> = Mutex::new(HashMap::new());
        let seg_path = PathBuf::from("ns/seg-failing");
        let n = 4;
        let barrier = Barrier::new(n);
        let waiters_that_saw_incomplete = AtomicUsize::new(0);

        std::thread::scope(|scope| {
            for _ in 0..n {
                scope.spawn(|| {
                    barrier.wait();
                    // `is_complete` always returns false — the segment never
                    // becomes available, i.e. the owner's fetch permanently
                    // fails.
                    let (owned, waiting) = partition_for_hydration(
                        &in_flight,
                        std::slice::from_ref(&seg_path),
                        |_| false,
                    );
                    if !owned.is_empty() {
                        std::thread::sleep(Duration::from_millis(50));
                        // The fetch "failed": nothing marks the segment
                        // complete, but completion must still be signalled
                        // so waiters don't hang forever.
                        complete_owned(&in_flight, &owned);
                    } else {
                        wait_for(&waiting);
                        // A waiter must be released promptly (this test
                        // would otherwise hang) and, upon rechecking
                        // completion itself exactly like the owner does,
                        // see the segment as still incomplete rather than
                        // assuming the owner's fetch succeeded.
                        waiters_that_saw_incomplete.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
        });

        assert_eq!(
            waiters_that_saw_incomplete.load(Ordering::SeqCst),
            n - 1,
            "every waiter must be unblocked after the owner's failed fetch"
        );
        assert!(in_flight.lock().unwrap().is_empty());
    }

    #[cfg(feature = "s3")]
    #[test]
    fn semaphore_bounds_concurrent_permits() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let sem = Semaphore::new(2);
        let active = AtomicUsize::new(0);
        let max_seen = AtomicUsize::new(0);

        std::thread::scope(|scope| {
            for _ in 0..6 {
                scope.spawn(|| {
                    let _permit = sem.acquire();
                    let cur = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(cur, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(20));
                    active.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });

        assert!(
            max_seen.load(Ordering::SeqCst) <= 2,
            "semaphore must never let more than 2 permits be held concurrently, saw {}",
            max_seen.load(Ordering::SeqCst)
        );
    }

    // ── Cross-replica hydration: internal peer endpoint ────────────────

    #[cfg(all(feature = "s3", feature = "postgres"))]
    #[test]
    fn internal_segment_route_serves_cached_bytes_as_base64() {
        let mut state = test_state();
        state.internal_service_key = Some("peer-test-secret".into());
        state
            .cache
            .put_bytes("ns/seg/doc_store.bin", b"hello segment")
            .unwrap();

        let response = route(
            "GET /internal/segment/ns/seg/doc_store.bin HTTP/1.1\r\n",
            &HashMap::from([("authorization".into(), "Bearer peer-test-secret".into())]),
            b"",
            "test",
            &state,
        );
        let (status, headers, body) = parse(&response);
        assert!(status.starts_with("HTTP/1.1 200 OK"), "status: {status}");
        assert!(
            headers.to_lowercase().contains("x-kosha-encoding: base64"),
            "headers: {headers}"
        );

        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(body.trim())
            .expect("body must be valid base64");
        assert_eq!(decoded, b"hello segment");
    }

    #[cfg(all(feature = "s3", feature = "postgres"))]
    #[test]
    fn internal_segment_route_404s_when_not_cached() {
        let mut state = test_state();
        state.internal_service_key = Some("peer-test-secret".into());
        let response = route(
            "GET /internal/segment/ns/seg/missing.bin HTTP/1.1\r\n",
            &HashMap::from([("authorization".into(), "Bearer peer-test-secret".into())]),
            b"",
            "test",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 404"), "response: {response}");
    }

    #[cfg(all(feature = "s3", feature = "postgres"))]
    #[test]
    fn internal_segment_route_rejects_path_traversal() {
        let mut state = test_state();
        state.internal_service_key = Some("peer-test-secret".into());
        let response = route(
            "GET /internal/segment/../../etc/passwd HTTP/1.1\r\n",
            &HashMap::from([("authorization".into(), "Bearer peer-test-secret".into())]),
            b"",
            "test",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 400"), "response: {response}");
    }

    /// End-to-end over a real socket, mirroring
    /// `forward_to_ingest_relays_method_path_and_response`: a real "peer"
    /// server (its own `AppState`/`serve()` loop) with a segment file
    /// already in its cache, and `fetch_from_peer` against it — exercises
    /// the actual reqwest + base64 round trip, not just the pure route
    /// handler above.
    #[cfg(all(feature = "s3", feature = "postgres"))]
    #[test]
    fn fetch_from_peer_round_trips_bytes_over_real_http() {
        let mut peer = test_state();
        peer.internal_service_key = Some("peer-test-secret".into());
        peer.cache
            .put_bytes("ns/seg/doc_store.bin", b"peer bytes")
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || serve(listener, Arc::new(peer), Duration::from_secs(5)));

        // `peer_http_client` is only populated by `AppState::new` when
        // hydration coordination is fully configured (POD_IP + Postgres) —
        // build one directly here so this test can exercise `fetch_from_peer`
        // without standing up a real Postgres instance for the claim side of
        // the protocol, which this call doesn't touch.
        let mut requester = test_state();
        requester.internal_auth_key = Some("peer-test-secret".into());
        requester.peer_http_client = Some(
            reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
        );

        let bytes = requester
            .fetch_from_peer(&addr.to_string(), "ns/seg/doc_store.bin")
            .expect("peer fetch should succeed");
        assert_eq!(bytes, b"peer bytes");
    }

    #[cfg(all(feature = "s3", feature = "postgres"))]
    #[test]
    fn fetch_from_peer_fails_fast_when_peer_unreachable() {
        let mut requester = test_state();
        requester.peer_http_client = Some(
            reqwest::blocking::Client::builder()
                .timeout(Duration::from_millis(500))
                .build()
                .unwrap(),
        );
        // Port 1 refuses connections immediately rather than timing out, so
        // this stays fast even though a 404 response would make
        // `fetch_from_peer` retry for up to its own 10s budget.
        let result = requester.fetch_from_peer("127.0.0.1:1", "ns/seg/doc_store.bin");
        assert!(result.is_err());
    }

    #[test]
    fn healthz_returns_200_ok() {
        let response = route(
            "GET /healthz HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &test_state(),
        );
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("\"status\":\"ok\""));
    }

    #[test]
    fn unknown_path_returns_404() {
        let response = route(
            "GET /nope HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &test_state(),
        );
        assert!(response.starts_with("HTTP/1.1 404 Not Found"));
    }

    #[test]
    fn response_is_well_formed_http11() {
        let response = route(
            "GET /healthz HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &test_state(),
        );
        let (status, headers, body) = parse(&response);
        assert!(status.starts_with("HTTP/1.1 "));
        assert!(headers.contains(&format!("content-length: {}", body.len())));
        assert!(headers.contains("content-type: application/json"));
        assert!(headers.contains("connection: close"));
    }

    #[test]
    fn index_endpoint_works() {
        let state = test_state();
        let req = IndexRequest {
            namespace: NamespaceId("test-ns".into()),
            documents: vec![Document {
                id: DocumentId("doc1".into()),
                fields: vec![Field::text("title", "hello world")],
            }],
        };
        let body = serde_json::to_vec(&req).unwrap();
        let response = route(
            "POST /index HTTP/1.1\r\n",
            &HashMap::new(),
            &body,
            "test",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("\"indexed_count\":1"));
    }

    #[test]
    fn exists_endpoint_reports_present_and_missing() {
        let state = test_state();
        let ns = NamespaceId("exists-ns".into());
        let req = IndexRequest {
            namespace: ns.clone(),
            documents: vec![Document {
                id: DocumentId("present".into()),
                fields: vec![Field::text("title", "hello")],
            }],
        };
        let body = serde_json::to_vec(&req).unwrap();
        let _ = route(
            "POST /index HTTP/1.1\r\n",
            &HashMap::new(),
            &body,
            "test",
            &state,
        );
        let _ = route(
            "POST /flush HTTP/1.1\r\n",
            &HashMap::new(),
            br#"{"namespace":"exists-ns"}"#,
            "test",
            &state,
        );

        let exists_body = br#"{"namespace":"exists-ns","ids":["present","absent"]}"#;
        let response = route(
            "POST /exists HTTP/1.1\r\n",
            &HashMap::new(),
            exists_body,
            "test",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("\"existing\":[\"present\"]"),
            "{response}"
        );
        assert!(response.contains("\"missing\":[\"absent\"]"), "{response}");
    }

    #[test]
    fn compact_namespace_endpoint_merges_segments_and_preserves_docs() {
        let state = test_state();
        let ns = NamespaceId("compact-ns".into());

        // Three separate index+flush round trips → three segments, same as
        // the flush-storm pattern that produced hundreds of tiny segments in
        // production (each flush call appends exactly one segment).
        for i in 0..3 {
            let req = IndexRequest {
                namespace: ns.clone(),
                documents: vec![Document {
                    id: DocumentId(format!("doc{i}")),
                    fields: vec![Field::text("title", format!("segment number {i}"))],
                }],
            };
            route(
                "POST /index HTTP/1.1\r\n",
                &HashMap::new(),
                &serde_json::to_vec(&req).unwrap(),
                "test",
                &state,
            );
            route(
                "POST /flush HTTP/1.1\r\n",
                &HashMap::new(),
                br#"{"namespace":"compact-ns"}"#,
                "test",
                &state,
            );
        }

        let before = state
            .indexer
            .manifest_cloned(&NamespaceId(tenant_namespace("test", &ns.0)))
            .unwrap()
            .segments
            .len();
        assert_eq!(before, 3, "expected one segment per flush");

        let response = handle_compact_namespace(
            br#"{"namespace":"compact-ns","mode":"full"}"#,
            "test",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("\"segments_before\":3"), "{response}");
        assert!(response.contains("\"segments_after\":1"), "{response}");
        assert!(response.contains("\"mode\":\"full\""), "{response}");

        let after = state
            .indexer
            .manifest_cloned(&NamespaceId(tenant_namespace("test", &ns.0)))
            .unwrap()
            .segments
            .len();
        assert_eq!(
            after, 1,
            "full compaction should merge all segments into one"
        );

        // Every document from every pre-compaction segment must still be
        // findable — compaction must never silently drop documents.
        for i in 0..3 {
            let search = route(
                &format!("GET /search?ns=compact-ns&q=segment+number+{i} HTTP/1.1\r\n"),
                &HashMap::new(),
                b"",
                "test",
                &state,
            );
            assert!(
                search.contains(&format!("\"doc{i}\"")),
                "doc{i} missing after compaction: {search}"
            );
        }
    }

    #[test]
    fn gc_endpoint_dry_runs_then_collects_compaction_retired_segments() {
        let state = test_state();
        let ns = NamespaceId(tenant_namespace("test", "gc-ns"));
        for i in 0..3 {
            let req = IndexRequest {
                namespace: NamespaceId("gc-ns".into()),
                documents: vec![Document {
                    id: DocumentId(format!("doc{i}")),
                    fields: vec![Field::text("title", format!("segment number {i}"))],
                }],
            };
            route(
                "POST /index HTTP/1.1\r\n",
                &HashMap::new(),
                &serde_json::to_vec(&req).unwrap(),
                "test",
                &state,
            );
            route(
                "POST /flush HTTP/1.1\r\n",
                &HashMap::new(),
                br#"{"namespace":"gc-ns"}"#,
                "test",
                &state,
            );
        }
        let inputs: Vec<String> = state
            .indexer
            .manifest_cloned(&ns)
            .unwrap()
            .segments
            .iter()
            .map(|e| e.segment_id.0.clone())
            .collect();
        assert_eq!(inputs.len(), 3);
        let response =
            handle_compact_namespace(br#"{"namespace":"gc-ns","mode":"full"}"#, "test", &state);
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        let published = state
            .controller
            .lock()
            .unwrap()
            .manifest_cloned(&ns)
            .unwrap();
        assert_eq!(published.retired.len(), 3, "compaction records its inputs");

        // Admin-only.
        let denied = route(
            "POST /v1/admin/gc HTTP/1.1\r\n",
            &HashMap::new(),
            b"{}",
            "test",
            &state,
        );
        assert!(denied.contains("403"), "{denied}");

        // Default grace: everything is too fresh to collect.
        let body = serde_json::json!({"namespace": ns.0}).to_string();
        let response = handle_segment_gc(body.as_bytes(), &state);
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        let json: serde_json::Value =
            serde_json::from_str(response.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(json["dry_run"], true);
        assert_eq!(json["candidates"], 0);

        // Zero grace, dry run: exactly the three inputs, nothing deleted.
        let body = serde_json::json!({"namespace": ns.0, "grace_seconds": 0}).to_string();
        let response = handle_segment_gc(body.as_bytes(), &state);
        let json: serde_json::Value =
            serde_json::from_str(response.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        let mut listed: Vec<String> = json["namespaces"][0]["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["segment_id"].as_str().unwrap().to_string())
            .collect();
        listed.sort();
        let mut expected = inputs.clone();
        expected.sort();
        assert_eq!(listed, expected);
        assert!(json["reclaimable_bytes"].as_u64().unwrap() > 0);
        for id in &inputs {
            assert!(state.data_dir.join(&ns.0).join(id).exists());
        }

        // Real run.
        let body = serde_json::json!({"namespace": ns.0, "grace_seconds": 0, "dry_run": false})
            .to_string();
        let response = handle_segment_gc(body.as_bytes(), &state);
        let json: serde_json::Value =
            serde_json::from_str(response.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(json["segments_deleted"], 3, "{json}");
        assert_eq!(json["aborted_namespaces"], 0, "{json}");
        assert!(
            json["stats"]["kosha_gc_bytes_reclaimed_total"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(json["stats"]["kosha_gc_segments_deleted_total"], 3);
        for id in &inputs {
            assert!(!state
                .data_dir
                .join(&ns.0)
                .join(id)
                .join("doc_store.bin")
                .exists());
        }
        let pruned = state
            .controller
            .lock()
            .unwrap()
            .manifest_cloned(&ns)
            .unwrap();
        assert!(pruned.retired.is_empty());
        assert_eq!(pruned.version, published.version + 1);

        // Local GC has nothing left to remove; the queue drains.
        state.run_local_gc(Duration::ZERO);
        assert_eq!(state.indexer.local_retired_count(), 0);

        // The writer rebases onto the pruned version on its next publish,
        // and every document is still searchable.
        let req = IndexRequest {
            namespace: NamespaceId("gc-ns".into()),
            documents: vec![Document {
                id: DocumentId("doc9".into()),
                fields: vec![Field::text("title", "segment number 9")],
            }],
        };
        route(
            "POST /index HTTP/1.1\r\n",
            &HashMap::new(),
            &serde_json::to_vec(&req).unwrap(),
            "test",
            &state,
        );
        let flushed = route(
            "POST /flush HTTP/1.1\r\n",
            &HashMap::new(),
            br#"{"namespace":"gc-ns"}"#,
            "test",
            &state,
        );
        assert!(flushed.starts_with("HTTP/1.1 200 OK"), "{flushed}");
        for i in [0, 1, 2, 9] {
            let search = route(
                &format!("GET /search?ns=gc-ns&q=segment+number+{i} HTTP/1.1\r\n"),
                &HashMap::new(),
                b"",
                "test",
                &state,
            );
            assert!(search.contains(&format!("\"doc{i}\"")), "doc{i}: {search}");
        }
        let stats = handle_stats(&state);
        assert!(stats.contains("kosha_retired_segments"), "{stats}");
        assert!(stats.contains("kosha_gc_bytes_reclaimed_total"), "{stats}");
    }

    #[test]
    fn local_gc_keeps_compacted_inputs_until_searches_release_them() {
        let state = test_state();
        let ns = NamespaceId(tenant_namespace("test", "lgc-ns"));
        for i in 0..2 {
            let req = IndexRequest {
                namespace: NamespaceId("lgc-ns".into()),
                documents: vec![Document {
                    id: DocumentId(format!("doc{i}")),
                    fields: vec![Field::text("title", format!("segment number {i}"))],
                }],
            };
            route(
                "POST /index HTTP/1.1\r\n",
                &HashMap::new(),
                &serde_json::to_vec(&req).unwrap(),
                "test",
                &state,
            );
            route(
                "POST /flush HTTP/1.1\r\n",
                &HashMap::new(),
                br#"{"namespace":"lgc-ns"}"#,
                "test",
                &state,
            );
        }
        // An in-flight search: its snapshot, taken before compaction.
        let snapshot = state.indexer.manifest_shared(&ns).unwrap();
        let inputs: Vec<kosha_core::SegmentId> = snapshot
            .segments
            .iter()
            .map(|e| e.segment_id.clone())
            .collect();
        let query: SearchQuery =
            serde_json::from_value(serde_json::json!({"query_text": "segment"})).unwrap();
        assert_eq!(
            state
                .searcher
                .search(&ns, &snapshot, &query, None)
                .unwrap()
                .results
                .len(),
            2
        );
        let response =
            handle_compact_namespace(br#"{"namespace":"lgc-ns","mode":"full"}"#, "test", &state);
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        let generation = state.cache.eviction_generation();

        let report = state.run_local_gc(Duration::ZERO);
        assert_eq!((report.segments_removed, report.pending), (0, 2));
        assert_eq!(
            state
                .searcher
                .search(&ns, &snapshot, &query, None)
                .unwrap()
                .results
                .len(),
            2,
            "a search on the pre-compaction snapshot still works after local GC"
        );
        drop(snapshot);
        let report = state.run_local_gc(Duration::ZERO);
        assert_eq!((report.segments_removed, report.pending), (2, 0));
        for id in &inputs {
            assert!(!state.data_dir.join(&ns.0).join(&id.0).exists());
        }
        assert!(state.cache.eviction_generation() > generation);
        let fresh = state.indexer.manifest_shared(&ns).unwrap();
        assert_eq!(
            state
                .searcher
                .search(&ns, &fresh, &query, None)
                .unwrap()
                .results
                .len(),
            2
        );
    }

    /// `test_state`, but behind the `Arc` `main` uses, so `self_ref`
    /// resolves and background jobs (async compaction, scheduler passes)
    /// can start.
    fn test_shared_state() -> Arc<AppState> {
        let dir = test_state().data_dir.clone();
        AppState::new_shared(dir)
    }

    /// Index `n` single-doc segments into `test/<name>` via index+flush.
    fn seed_segments(state: &AppState, name: &str, n: usize) -> NamespaceId {
        for i in 0..n {
            let req = IndexRequest {
                namespace: NamespaceId(name.into()),
                documents: vec![Document {
                    id: DocumentId(format!("doc{i}")),
                    fields: vec![Field::text("title", format!("segment number {i}"))],
                }],
            };
            route(
                "POST /index HTTP/1.1\r\n",
                &HashMap::new(),
                &serde_json::to_vec(&req).unwrap(),
                "test",
                state,
            );
            route(
                "POST /flush HTTP/1.1\r\n",
                &HashMap::new(),
                format!(r#"{{"namespace":"{name}"}}"#).as_bytes(),
                "test",
                state,
            );
        }
        let ns = NamespaceId(tenant_namespace("test", name));
        assert_eq!(
            state.indexer.manifest_cloned(&ns).unwrap().segments.len(),
            n
        );
        ns
    }

    fn wait_for_job(state: &AppState, job_id: &str) -> compaction_sched::JobRecord {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let job = state.compaction.job(job_id).expect("job registered");
            if job.state != compaction_sched::JobState::Running {
                return job;
            }
            assert!(Instant::now() < deadline, "job {job_id} still running");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn json_body(response: &str) -> serde_json::Value {
        let body = response.split("\r\n\r\n").nth(1).unwrap_or_default();
        serde_json::from_str(body).unwrap_or_else(|e| panic!("{e}: {response}"))
    }

    /// #242: nothing used to call `needs_compaction`. A scheduler tick must
    /// start a tiered pass on a namespace past the trigger, the pass must
    /// keep every doc, and once the namespace no longer needs compaction the
    /// next tick must leave it alone.
    #[test]
    fn scheduler_tick_compacts_namespaces_past_the_trigger() {
        let state = test_shared_state();
        let trigger = state.indexer.compaction_policy().trigger_mergeable_segments;
        let busy = seed_segments(&state, "sched-busy", trigger);
        let quiet = seed_segments(&state, "sched-quiet", 2);

        let started = run_compaction_scheduler_tick(&state, Instant::now());
        assert_eq!(
            started,
            vec![busy.clone()],
            "only the namespace past the trigger"
        );

        let status = json_body(&handle_compaction_status(&state));
        let job_id = status["running_jobs"][0]["job_id"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| {
                status["recent_jobs"][0]["job_id"]
                    .as_str()
                    .map(str::to_owned)
            })
            .expect("scheduled job is listed");
        let job = wait_for_job(&state, &job_id);
        assert_eq!(job.state, compaction_sched::JobState::Succeeded, "{job:?}");
        assert_eq!(job.trigger, compaction_sched::Trigger::Scheduler);
        let result = job.result.unwrap();
        assert!(result.merged);
        assert_eq!(result.segments_before, trigger);
        assert!(result.segments_after < trigger, "{result:?}");

        for i in 0..trigger {
            let search = route(
                &format!("GET /search?ns=sched-busy&q=segment+number+{i} HTTP/1.1\r\n"),
                &HashMap::new(),
                b"",
                "test",
                &state,
            );
            assert!(
                search.contains(&format!("\"doc{i}\"")),
                "doc{i} lost: {search}"
            );
        }

        assert!(
            run_compaction_scheduler_tick(&state, Instant::now()).is_empty(),
            "neither namespace needs compaction now"
        );
        let status = json_body(&handle_compaction_status(&state));
        let rows = status["namespaces"].as_array().unwrap();
        let row = |ns: &NamespaceId| {
            rows.iter()
                .find(|r| r["namespace"] == ns.0.as_str())
                .unwrap()
                .clone()
        };
        assert_eq!(row(&busy)["stats"]["merges"], 1);
        assert_eq!(row(&busy)["needs_compaction"], false);
        assert_eq!(row(&quiet)["stats"]["passes"], 0, "never selected");
    }

    #[test]
    fn async_compaction_returns_202_and_the_job_reports_the_result() {
        let state = test_shared_state();
        seed_segments(&state, "async-ns", 3);
        let response = handle_compact_namespace(
            br#"{"namespace":"async-ns","mode":"full","async":true}"#,
            "test",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 202 Accepted"), "{response}");
        let accepted = json_body(&response);
        let job_id = accepted["job_id"].as_str().unwrap().to_owned();
        assert_eq!(
            accepted["status_url"],
            format!("/v1/admin/compaction-jobs/{job_id}")
        );

        wait_for_job(&state, &job_id);
        let polled = handle_compaction_job(&job_id, &state);
        let job = json_body(&polled);
        assert_eq!(job["state"], "succeeded", "{polled}");
        assert_eq!(job["trigger"], "api");
        assert_eq!(job["mode"], "full");
        assert_eq!(job["result"]["segments_before"], 3);
        assert_eq!(job["result"]["segments_after"], 1);

        let missing = handle_compaction_job("nope", &state);
        assert!(missing.starts_with("HTTP/1.1 404"), "{missing}");
    }

    #[test]
    fn second_compaction_on_a_busy_namespace_gets_409_with_the_running_job() {
        let state = test_shared_state();
        let ns = seed_segments(&state, "busy-ns", 2);
        let running = state
            .compaction
            .try_start(
                &ns,
                "tiered",
                compaction_sched::Trigger::Scheduler,
                Instant::now(),
            )
            .unwrap();
        for body in [
            br#"{"namespace":"busy-ns"}"#.as_slice(),
            br#"{"namespace":"busy-ns","async":true}"#.as_slice(),
        ] {
            let response = handle_compact_namespace(body, "test", &state);
            assert!(response.starts_with("HTTP/1.1 409"), "{response}");
            assert!(response.contains(&running), "{response}");
        }
    }

    /// In-process stand-in for S3 in the #242 tests: a "remote" directory
    /// with the same `<ns>/<segment>/<file>` layout as the bucket. Sizes come
    /// from the remote copies (falling back to local, as `AppState` does for
    /// a segment not yet listed in S3), hydration copies whole segment
    /// directories down, and every hydrate call records which segments were
    /// on local disk right after it.
    /// (requested segment ids, segment dirs local afterwards, local bytes of
    /// the requested group afterwards).
    type HydrateCall = (Vec<String>, Vec<String>, u64);

    struct FakeObjectStore {
        remote: PathBuf,
        data_dir: PathBuf,
        /// Segments whose size S3 "can't report": they must never join a
        /// capped group, so never be hydrated by compaction.
        unknown_size: std::collections::HashSet<String>,
        /// Per hydrate call: (requested segment ids, segment dirs local
        /// afterwards, local bytes of the requested group afterwards).
        calls: Mutex<Vec<HydrateCall>>,
    }

    impl FakeObjectStore {
        fn local_segments(&self, ns: &NamespaceId) -> Vec<String> {
            let mut out: Vec<String> = fs::read_dir(self.data_dir.join(&ns.0))
                .map(|rd| {
                    rd.flatten()
                        .filter(|e| e.path().is_dir())
                        .map(|e| e.file_name().to_string_lossy().into_owned())
                        .filter(|n| !n.starts_with('_') && !n.starts_with('.'))
                        .collect()
                })
                .unwrap_or_default();
            out.sort();
            out
        }
    }

    fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
        fs::create_dir_all(to).unwrap();
        for entry in fs::read_dir(from).unwrap().flatten() {
            let dest = to.join(entry.file_name());
            if entry.path().is_dir() {
                copy_tree(&entry.path(), &dest);
            } else {
                fs::copy(entry.path(), dest).unwrap();
            }
        }
    }

    impl CompactionStorage for FakeObjectStore {
        fn segment_sizes(
            &self,
            ns: &NamespaceId,
            manifest: &kosha_core::Manifest,
        ) -> HashMap<kosha_core::SegmentId, u64> {
            manifest
                .segments
                .iter()
                .filter(|e| !self.unknown_size.contains(&e.segment_id.0))
                .filter_map(|e| {
                    let rel = std::path::Path::new(&ns.0).join(&e.segment_id.0);
                    let bytes = local_dir_bytes(&self.remote.join(&rel))
                        .or_else(|| local_dir_bytes(&self.data_dir.join(&rel)))?;
                    Some((e.segment_id.clone(), bytes))
                })
                .collect()
        }

        fn hydrate(&self, seg_paths: &[PathBuf]) {
            let mut requested = Vec::new();
            let mut ns = None;
            let mut group_bytes = 0;
            for path in seg_paths {
                let rel = path.strip_prefix(&self.data_dir).unwrap();
                let remote = self.remote.join(rel);
                if !path.exists() && remote.is_dir() {
                    copy_tree(&remote, path);
                }
                group_bytes += local_dir_bytes(path).unwrap_or(0);
                requested.push(path.file_name().unwrap().to_string_lossy().into_owned());
                ns = rel
                    .parent()
                    .map(|p| NamespaceId(p.to_string_lossy().into_owned()));
            }
            let local = ns.map(|ns| self.local_segments(&ns)).unwrap_or_default();
            self.calls
                .lock()
                .unwrap()
                .push((requested, local, group_bytes));
        }

        fn evicts_superseded(&self) -> bool {
            true
        }
    }

    /// #242 acceptance: on a cold writer (nothing local, every segment only
    /// in object storage) a `full` pass over a namespace several times the
    /// cap must plan from remote sizes, hydrate one capped group at a time,
    /// evict each group's inputs once its merge is published, keep peak
    /// local disk at about `2 x cap`, never touch a segment whose size is
    /// unknown, and lose no document.
    #[test]
    fn full_pass_hydrates_one_capped_group_at_a_time_and_evicts_inputs() {
        let state = test_state();
        let n = 10;
        let ns = seed_segments(&state, "bounded-disk", n);
        let manifest = state.indexer.manifest_cloned(&ns).unwrap();
        let originals: Vec<String> = manifest
            .segments
            .iter()
            .map(|e| e.segment_id.0.clone())
            .collect();

        // Move every segment to the fake bucket: the writer starts cold.
        let remote = state.data_dir.with_extension("remote");
        let _ = fs::remove_dir_all(&remote);
        let mut sizes = HashMap::new();
        for id in &originals {
            let local = state.data_dir.join(&ns.0).join(id);
            sizes.insert(id.clone(), local_dir_bytes(&local).unwrap());
            copy_tree(&local, &remote.join(&ns.0).join(id));
            fs::remove_dir_all(&local).unwrap();
        }
        let unknown = originals[4].clone();
        let store = FakeObjectStore {
            remote: remote.clone(),
            data_dir: state.data_dir.clone(),
            unknown_size: [unknown.clone()].into_iter().collect(),
            calls: Mutex::new(Vec::new()),
        };
        assert!(store.local_segments(&ns).is_empty(), "cold start");

        // A cap that fits about three of these segments, so the namespace
        // (10 segments) is several caps big.
        let largest = *sizes.values().max().unwrap();
        let total: u64 = sizes.values().sum();
        let cap = largest * 7 / 2;
        assert!(
            total > 2 * cap,
            "namespace must exceed the cap: {total} vs {cap}"
        );
        let mut opts = compact_options(&state, kosha_write::CompactMode::Full, Some(cap));
        opts.policy.max_merged_segment_bytes = cap;

        let mut progress = Vec::new();
        let outcome = compaction_pass_with(&state, &store, &ns, opts, |p| {
            progress.push((p.groups, store.local_segments(&ns)));
        })
        .unwrap();

        let calls = store.calls.lock().unwrap().clone();
        eprintln!(
            "bounded-disk: {n} segments, total {total} B, cap {cap} B, groups {}, \
             hydrated {} B, evicted {} B, peak local {} B ({:.2} x cap)",
            outcome.groups,
            outcome.bytes_hydrated,
            outcome.bytes_evicted,
            outcome.peak_local_bytes,
            outcome.peak_local_bytes as f64 / cap as f64
        );
        assert!(calls.len() >= 2, "several groups: {calls:?}");
        assert_eq!(outcome.groups, calls.len(), "{outcome:?}");
        for (requested, local_after, group_bytes) in &calls {
            // Only the planned group's original segments are ever local:
            // nothing outside the group was downloaded, and every earlier
            // group's inputs are gone already.
            let originals_local: Vec<&String> = local_after
                .iter()
                .filter(|s| originals.contains(s))
                .collect();
            for seg in &originals_local {
                assert!(
                    requested.contains(seg),
                    "{seg} is local but not in the planned group {requested:?}"
                );
            }
            assert!(!requested.contains(&unknown), "unknown size hydrated");
            assert!(
                *group_bytes <= cap,
                "group {requested:?} = {group_bytes} > cap {cap}"
            );
        }
        // After each published group, its inputs have left local disk.
        for (_, local) in &progress {
            assert!(
                local.iter().all(|s| !originals.contains(s)),
                "superseded inputs still local after a group: {local:?}"
            );
        }

        let after = state.indexer.manifest_cloned(&ns).unwrap();
        let after_ids: Vec<String> = after
            .segments
            .iter()
            .map(|e| e.segment_id.0.clone())
            .collect();
        assert!(after_ids.contains(&unknown), "unknown size never merges");
        assert!(after.segments.len() < n, "{after_ids:?}");
        assert_eq!(outcome.segments_after, after.segments.len());
        assert_eq!(outcome.segments_before, n);
        assert!(outcome.merged);
        assert!(outcome.not_hydrated.is_empty(), "{outcome:?}");
        assert!(outcome.not_evicted.is_empty(), "{outcome:?}");
        // Merged originals were downloaded once and evicted once.
        let merged_original_bytes: u64 = originals
            .iter()
            .filter(|s| !after_ids.contains(s))
            .map(|s| sizes[s])
            .sum();
        assert_eq!(outcome.bytes_hydrated, merged_original_bytes, "{outcome:?}");
        assert!(
            outcome.bytes_evicted >= merged_original_bytes,
            "{outcome:?}"
        );
        assert!(
            outcome.peak_local_bytes > 0 && outcome.peak_local_bytes <= 2 * cap + cap / 4,
            "peak {} vs 2 x cap {}",
            outcome.peak_local_bytes,
            2 * cap
        );
        // Local disk now holds only merged outputs, never the namespace.
        for seg in store.local_segments(&ns) {
            assert!(!originals.contains(&seg), "{seg} still local");
        }

        // Every doc is still searchable once the query side hydrates what
        // it needs (the unmerged unknown-size segment is still remote).
        let paths: Vec<PathBuf> = after_ids
            .iter()
            .map(|id| state.data_dir.join(&ns.0).join(id))
            .collect();
        store.hydrate(&paths);
        for i in 0..n {
            let search = route(
                &format!("GET /search?ns=bounded-disk&q=segment+number+{i} HTTP/1.1\r\n"),
                &HashMap::new(),
                b"",
                "test",
                &state,
            );
            assert!(
                search.contains(&format!("\"doc{i}\"")),
                "doc{i} lost: {search}"
            );
        }

        // Converged: a second pass finds nothing that fits and hydrates
        // nothing.
        let before_calls = store.calls.lock().unwrap().len();
        let again =
            compaction_pass_with(&state, &store, &ns, opts_for(&state, cap), |_| {}).unwrap();
        assert!(!again.merged, "{again:?}");
        assert_eq!(store.calls.lock().unwrap().len(), before_calls);
        let _ = fs::remove_dir_all(&remote);
    }

    fn opts_for(state: &AppState, cap: u64) -> kosha_write::CompactOptions {
        compact_options(state, kosha_write::CompactMode::Full, Some(cap))
    }

    /// A `tiered` pass plans the same way and merges at most one group; it
    /// never hydrates a segment it can't merge.
    #[test]
    fn tiered_pass_hydrates_only_its_planned_group() {
        let state = test_state();
        let ns = seed_segments(&state, "tiered-disk", 6);
        let ids: Vec<String> = state
            .indexer
            .manifest_cloned(&ns)
            .unwrap()
            .segments
            .iter()
            .map(|e| e.segment_id.0.clone())
            .collect();
        let remote = state.data_dir.with_extension("remote");
        let _ = fs::remove_dir_all(&remote);
        for id in &ids {
            let local = state.data_dir.join(&ns.0).join(id);
            copy_tree(&local, &remote.join(&ns.0).join(id));
            fs::remove_dir_all(&local).unwrap();
        }
        let store = FakeObjectStore {
            remote: remote.clone(),
            data_dir: state.data_dir.clone(),
            unknown_size: Default::default(),
            calls: Mutex::new(Vec::new()),
        };
        let mut opts = compact_options(&state, kosha_write::CompactMode::Tiered, None);
        opts.policy.max_segments_per_merge = 3;
        let outcome = compaction_pass_with(&state, &store, &ns, opts, |_| {}).unwrap();
        assert!(outcome.merged);
        assert_eq!(outcome.groups, 1);
        assert_eq!(outcome.segments_merged, 3);
        let calls = store.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0.len(), 3, "{calls:?}");
        assert_eq!(
            store
                .local_segments(&ns)
                .iter()
                .filter(|s| ids.contains(s))
                .count(),
            0,
            "inputs evicted, nothing else hydrated"
        );
        let _ = fs::remove_dir_all(&remote);
    }

    /// Eviction never removes a segment the manifest still references or
    /// one with a pinned file (an in-flight hydration or warmup).
    #[test]
    fn eviction_skips_live_and_pinned_segments() {
        let state = test_state();
        let ns = seed_segments(&state, "evict-guard", 3);
        let ids: Vec<kosha_core::SegmentId> = state
            .indexer
            .manifest_cloned(&ns)
            .unwrap()
            .segments
            .iter()
            .map(|e| e.segment_id.clone())
            .collect();
        // Still in the manifest: kept.
        let (freed, kept) = evict_superseded_inputs(&state, &ns, &ids[..1]);
        assert_eq!(freed, 0);
        assert_eq!(kept, vec![ids[0].0.clone()]);
        assert!(state.data_dir.join(&ns.0).join(&ids[0].0).exists());

        // Merge all three, then pin a file of one superseded input.
        let plan = kosha_write::MergePlan {
            inputs: state.indexer.manifest_cloned(&ns).unwrap().segments,
        };
        assert!(state.indexer.compact_planned(&ns, &plan).unwrap().merged);
        let pinned_key = format!("{}/{}/doc_store.bin", ns.0, ids[1].0);
        state.cache.pin(&pinned_key);
        let (freed, kept) = evict_superseded_inputs(&state, &ns, &ids);
        assert!(freed > 0);
        assert_eq!(kept, vec![ids[1].0.clone()]);
        assert!(!state.data_dir.join(&ns.0).join(&ids[0].0).exists());
        assert!(state.data_dir.join(&ns.0).join(&ids[1].0).exists());
        assert!(!state.data_dir.join(&ns.0).join(&ids[2].0).exists());
        state.cache.unpin(&pinned_key);
    }

    /// Local-only mode (no object storage): the local directory is the only
    /// copy, so a pass must never delete superseded inputs. That is GC's
    /// job, not cache eviction's.
    #[test]
    fn local_only_mode_does_not_evict() {
        let state = test_state();
        assert!(!CompactionStorage::evicts_superseded(&state));
    }

    #[test]
    fn compact_namespace_endpoint_404s_for_unknown_namespace() {
        let state = test_state();
        let response =
            handle_compact_namespace(br#"{"namespace":"does-not-exist"}"#, "test", &state);
        assert!(response.starts_with("HTTP/1.1 404"), "{response}");
    }

    #[test]
    fn replace_endpoint_removes_old_document_version() {
        let state = test_state();
        let namespace = NamespaceId("test-replace".into());
        let initial = IndexRequest {
            namespace: namespace.clone(),
            documents: vec![Document {
                id: DocumentId("doc1".into()),
                fields: vec![Field::text("title", "old value")],
            }],
        };
        route(
            "POST /index HTTP/1.1\r\n",
            &HashMap::new(),
            &serde_json::to_vec(&initial).unwrap(),
            "test",
            &state,
        );
        state.indexer.flush_namespace(&namespace).unwrap();

        let replacement = IndexRequest {
            namespace,
            documents: vec![Document {
                id: DocumentId("doc1".into()),
                fields: vec![Field::text("title", "new value")],
            }],
        };
        let response = route(
            "POST /replace HTTP/1.1\r\n",
            &HashMap::new(),
            &serde_json::to_vec(&replacement).unwrap(),
            "test",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 200 OK"));

        let old = route(
            "GET /search?ns=test-replace&q=old HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &state,
        );
        let new = route(
            "GET /search?ns=test-replace&q=new HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &state,
        );
        assert!(old.contains("\"total_hits\":0"));
        assert!(new.contains("\"total_hits\":1"));
    }

    #[test]
    fn index_then_search() {
        let state = test_state();
        let ns = "test-ns-search";

        // Index a document.
        let req = IndexRequest {
            namespace: NamespaceId(ns.into()),
            documents: vec![
                Document {
                    id: DocumentId("d1".into()),
                    fields: vec![Field::text("title", "quick brown fox")],
                },
                Document {
                    id: DocumentId("d2".into()),
                    fields: vec![Field::text("title", "lazy dog")],
                },
            ],
        };
        let body = serde_json::to_vec(&req).unwrap();
        let index_resp = route(
            "POST /index HTTP/1.1\r\n",
            &HashMap::new(),
            &body,
            "test",
            &state,
        );
        assert!(index_resp.contains("\"indexed_count\":2"));

        // Trigger flush so search can read from disk.
        {
            let indexer = &state.indexer;
            indexer
                .flush_namespace(&NamespaceId(tenant_namespace("test", ns)))
                .unwrap();
        }

        // Now search for "quick".
        let search_resp = route(
            &format!("GET /search?ns={ns}&q=quick HTTP/1.1\r\n"),
            &HashMap::new(),
            b"",
            "test",
            &state,
        );
        assert!(search_resp.starts_with("HTTP/1.1 200 OK"));
        assert!(search_resp.contains("\"total_hits\":1"));

        // Search for "dog".
        let search_resp2 = route(
            &format!("GET /search?ns={ns}&q=dog HTTP/1.1\r\n"),
            &HashMap::new(),
            b"",
            "test",
            &state,
        );
        assert!(search_resp2.starts_with("HTTP/1.1 200 OK"));
        assert!(search_resp2.contains("\"total_hits\":1"));
    }

    #[test]
    fn nested_attributes_round_trip_through_the_http_api() {
        // #206: a Keyword field holding a JSON object or array of objects
        // is filterable, sortable and aggregatable by dotted path, across
        // two flushed segments, and comes back exactly as sent.
        let state = test_state();
        let author = serde_json::json!({"name": "Ada", "org": "acme", "age": 36});
        let index = |id: &str, author: &serde_json::Value, authors: serde_json::Value| {
            let body = serde_json::to_vec(&serde_json::json!({"documents": [{"id": id, "fields": [
                {"name": "title", "field_type": "Text", "value": "report"},
                {"name": "author", "field_type": "Keyword", "value": author.to_string()},
                {"name": "authors", "field_type": "Keyword", "value": authors.to_string()}
            ]}]}))
            .unwrap();
            let resp = route(
                "POST /v1/namespaces/nested/documents HTTP/1.1\r\n",
                &HashMap::new(),
                &body,
                "acme-corp",
                &state,
            );
            assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
            state
                .indexer
                .flush_namespace(&NamespaceId("acme-corp/nested".into()))
                .unwrap();
        };
        index(
            "d1",
            &author,
            serde_json::json!([{"name": "Ada", "org": "acme"}, {"name": "Bob", "org": "beta"}]),
        );
        index(
            "d2",
            &serde_json::json!({"name": "Bob", "org": "beta", "age": 41}),
            serde_json::json!([{"name": "Bob", "org": "acme"}]),
        );
        let search = |query: serde_json::Value| {
            let resp = route(
                "POST /v1/namespaces/nested/search HTTP/1.1\r\n",
                &HashMap::new(),
                &serde_json::to_vec(&query).unwrap(),
                "acme-corp",
                &state,
            );
            assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
            json_body(&resp)
        };
        let ids = |body: &serde_json::Value| -> Vec<String> {
            body["results"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["doc_id"].as_str().unwrap().to_string())
                .collect()
        };

        let body = search(serde_json::json!({
            "query_text": "report", "max_results": 5,
            "filter": {"bool": {"must": [{"term": {"authors.name": "Ada"}},
                                         {"term": {"authors.org": "beta"}}]}}
        }));
        assert_eq!(ids(&body), vec!["d1"], "flattened: matches across elements");
        let fields = body["results"][0]["fields"].as_array().unwrap();
        let stored = fields.iter().find(|f| f["name"] == "author").unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(stored["value"].as_str().unwrap()).unwrap(),
            author
        );
        assert!(fields.iter().all(|f| f["name"] != "author.name"));

        let body = search(serde_json::json!({
            "query_text": "report", "max_results": 5,
            "sort": [{"author.age": {"order": "desc"}}],
            "aggs": {"names": {"terms": {"field": "authors.name"}}}
        }));
        assert_eq!(ids(&body), vec!["d2", "d1"]);
        let mut buckets: Vec<(String, u64)> = body["aggregations"]["names"]["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| {
                (
                    b["key"].as_str().unwrap().into(),
                    b["doc_count"].as_u64().unwrap(),
                )
            })
            .collect();
        buckets.sort();
        assert_eq!(buckets, vec![("Ada".into(), 1), ("Bob".into(), 2)]);
    }

    #[test]
    fn v1_tenant_scoped_index_then_search_works() {
        // Regression test: extract_namespace previously could never match
        // (it stripped a method-prefixed string like "POST /v1/namespaces/"
        // from a path-only token), and handle_index_with_ns previously
        // discarded its tenant-scoped body and forwarded the original
        // request instead — both silently 404'd every v1 namespace route.
        let state = test_state();
        let body = serde_json::to_vec(&serde_json::json!({
            "documents": [{"id": "d1", "fields": [{"name": "title", "field_type": "Text", "value": "quick brown fox"}]}]
        }))
        .unwrap();
        let index_resp = route(
            "POST /v1/namespaces/my-index/documents HTTP/1.1\r\n",
            &HashMap::new(),
            &body,
            "acme-corp",
            &state,
        );
        assert!(
            index_resp.starts_with("HTTP/1.1 200 OK"),
            "index response: {index_resp}"
        );
        assert!(index_resp.contains("\"indexed_count\":1"));

        {
            let indexer = &state.indexer;
            indexer
                .flush_namespace(&NamespaceId("acme-corp/my-index".into()))
                .unwrap();
        }

        let search_body =
            serde_json::to_vec(&serde_json::json!({"query_text": "quick", "max_results": 5}))
                .unwrap();
        let search_resp = route(
            "POST /v1/namespaces/my-index/search HTTP/1.1\r\n",
            &HashMap::new(),
            &search_body,
            "acme-corp",
            &state,
        );
        assert!(
            search_resp.starts_with("HTTP/1.1 200 OK"),
            "search response: {search_resp}"
        );
        assert!(search_resp.contains("\"total_hits\":1"));
    }

    #[test]
    fn namespace_schema_analyzers_apply_to_new_segments_and_compaction() {
        // Issue #220: the schema's analyzer reaches the write path through
        // the manifest, segments written before the change keep matching
        // with the analysis they were written with, and compaction
        // re-analyzes them under the current schema.
        let state = test_state();
        let ns = NamespaceId("acme/shop".into());
        let call = |line: &str, body: serde_json::Value| {
            let resp = route(
                line,
                &HashMap::new(),
                &serde_json::to_vec(&body).unwrap(),
                "acme",
                &state,
            );
            let status: u16 = resp[9..12].parse().unwrap();
            let body = &resp[resp.find("\r\n\r\n").unwrap() + 4..];
            (
                status,
                serde_json::from_str::<serde_json::Value>(body).unwrap(),
            )
        };
        let index = |id: &str, text: &str| {
            let (status, _) = call(
                "POST /v1/namespaces/shop/documents HTTP/1.1\r\n",
                serde_json::json!({"documents": [{"id": id, "fields": [
                    {"name": "body", "field_type": "Text", "value": text}]}]}),
            );
            assert_eq!(status, 200);
            state.indexer.flush_namespace(&ns).unwrap();
            state.publish_namespace(&ns).unwrap();
        };
        let search = |text: &str| -> Vec<String> {
            let (status, body) = call(
                "POST /v1/namespaces/shop/search HTTP/1.1\r\n",
                serde_json::json!({"query_text": text, "no_cache": true}),
            );
            assert_eq!(status, 200, "{body}");
            let mut ids: Vec<String> = body["results"]
                .as_array()
                .unwrap()
                .iter()
                .map(|hit| hit["doc_id"].as_str().unwrap().to_string())
                .collect();
            ids.sort();
            ids
        };

        index("before", "Running shoes");

        let (status, body) = call(
            "POST /v1/namespaces/shop/schema HTTP/1.1\r\n",
            serde_json::json!({"fields": {"body": {"analyzer": ["standard", "soundex"]}}}),
        );
        assert_eq!(status, 400, "unknown filter is rejected: {body}");
        let schema = serde_json::json!({"schema": {"fields": {
            "body": {"type": "FIELD_TYPE_TEXT", "analyzer": ["standard", "stem"]}}}});
        let (status, body) = call(
            "POST /v1/namespaces/shop/schema HTTP/1.1\r\n",
            schema.clone(),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["changed"], true);
        let (_, body) = call("POST /v1/namespaces/shop/schema HTTP/1.1\r\n", schema);
        assert_eq!(body["changed"], false, "same schema again is a no-op");
        let (status, body) = call(
            "GET /v1/namespaces/shop/schema HTTP/1.1\r\n",
            serde_json::json!({}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["schema"]["fields"]["body"]["analyzer"][1], "stem");

        index("after", "they run daily");

        // The old segment is still read with the legacy chain: "running"
        // finds it, "run" does not (it never stemmed anything) — but it
        // does not go dark either.
        assert_eq!(search("running"), ["after", "before"]);
        assert_eq!(search("run"), ["after"]);

        // Compaction re-analyzes the old document under the new schema.
        let result = state
            .indexer
            .compact_namespace_with_options(&ns, kosha_write::CompactOptions::full())
            .unwrap();
        assert!(result.merged);
        state.publish_namespace(&ns).unwrap();
        assert_eq!(search("run"), ["after", "before"]);
        assert_eq!(search("running"), ["after", "before"]);
    }

    #[test]
    fn search_missing_namespace_returns_404() {
        let state = test_state();
        let response = route(
            "GET /search?ns=nonexistent&q=test HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 404 Not Found"));
    }

    #[test]
    fn search_missing_params_returns_400() {
        let state = test_state();
        let response = route(
            "GET /search HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 400 Bad Request"));
    }

    #[test]
    fn flush_persists_manifest_to_control_store() {
        // Regression: manifests were only kept in the in-process indexer map
        // and never written to the control store, so a restart made every
        // namespace vanish from search.
        let state = test_state();
        let ns = "test-ns-persist";

        let req = IndexRequest {
            namespace: NamespaceId(ns.into()),
            documents: vec![Document {
                id: DocumentId("doc1".into()),
                fields: vec![Field::text("title", "hello world")],
            }],
        };
        let body = serde_json::to_vec(&req).unwrap();
        route(
            "POST /index HTTP/1.1\r\n",
            &HashMap::new(),
            &body,
            "test",
            &state,
        );

        let flush_body = serde_json::to_vec(&serde_json::json!({"namespace": ns})).unwrap();
        let resp = route(
            "POST /flush HTTP/1.1\r\n",
            &HashMap::new(),
            &flush_body,
            "test",
            &state,
        );
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "flush: {resp}");

        let ctrl = state.controller.lock().unwrap();
        let m = ctrl
            .manifest(&NamespaceId(tenant_namespace("test", ns)))
            .expect("flush must persist the manifest to the control store");
        assert_eq!(m.version, 1);
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.segments[0].doc_count, 1);
    }

    #[test]
    fn auto_flush_during_index_persists_manifest() {
        // Regression: index_documents auto-flushes when the buffer hits the
        // flush threshold (1000) — that manifest update was never persisted.
        let state = test_state();
        let ns = "test-ns-autoflush";

        let docs: Vec<Document> = (0..1000)
            .map(|i| Document {
                id: DocumentId(format!("doc{i}")),
                fields: vec![Field::text("title", format!("document number {i}"))],
            })
            .collect();
        let req = IndexRequest {
            namespace: NamespaceId(ns.into()),
            documents: docs,
        };
        let body = serde_json::to_vec(&req).unwrap();
        let resp = route(
            "POST /index HTTP/1.1\r\n",
            &HashMap::new(),
            &body,
            "test",
            &state,
        );
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "index: {resp}");

        let ctrl = state.controller.lock().unwrap();
        let m = ctrl
            .manifest(&NamespaceId(tenant_namespace("test", ns)))
            .expect("auto-flush must persist the manifest to the control store");
        // A single auto-flush round no longer means a single segment: with
        // parallel flush writers (see kosha-write's run_one_flush_pass), a
        // 1000-doc buffer that crosses the default
        // min_docs_per_flush_writer (250) threshold is split across
        // multiple concurrently-written segments instead of one. The
        // regression this test actually guards against -- that auto-flush
        // persists the manifest to the control store at all -- only cares
        // that every buffered document survived, not how many segments it
        // landed in.
        let total_docs: u32 = m.segments.iter().map(|e| e.doc_count).sum();
        assert_eq!(total_docs, 1000);
        assert!(!m.segments.is_empty());
    }

    #[test]
    fn stalled_connection_does_not_block_healthz() {
        // Regression: the accept loop used to handle one connection at a
        // time, so a single stalled client blocked every subsequent request
        // — including the /healthz probe.
        use std::io::Read;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(test_state());
        std::thread::spawn(move || serve(listener, state, Duration::from_secs(30)));

        // A client that connects but never sends anything.
        let _stalled = TcpStream::connect(addr).unwrap();
        std::thread::sleep(Duration::from_millis(100)); // let the server accept it

        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.write_all(b"GET /healthz HTTP/1.1\r\nhost: x\r\n\r\n")
            .unwrap();
        let mut resp = String::new();
        s.read_to_string(&mut resp).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "response: {resp}");
    }

    /// Regression/documentation for the BM25 benchmark re-run's bug #3
    /// (`scripts/bench/bm25_scale/RESULTS.md`): "30s HTTP io-timeout makes
    /// cold namespaces unqueryable." `serve` sets `set_read_timeout`/
    /// `set_write_timeout` once, right after `accept()` — this confirms
    /// that's a per-syscall timeout, not a connection-lifetime budget: a
    /// handler that takes far longer than `io_timeout` to produce a
    /// response must still deliver it, as long as the write of the
    /// response itself (once finally invoked) completes within the window.
    /// If a future change made this a cumulative per-connection deadline
    /// instead, cold queries against large namespaces would go back to
    /// being silently dropped with no response — exactly the incident this
    /// guards against.
    #[test]
    fn slow_handler_still_delivers_its_response_after_short_io_timeout_elapses() {
        use std::io::Read;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let io_timeout = Duration::from_millis(200);

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                stream.set_read_timeout(Some(io_timeout)).ok();
                stream.set_write_timeout(Some(io_timeout)).ok();
                std::thread::spawn(move || {
                    let mut buf = [0u8; 1024];
                    let _ = stream.read(&mut buf);
                    // Stand-in for a cold-hydration search: work that
                    // outlives io_timeout many times over, with no socket
                    // I/O at all while it runs.
                    std::thread::sleep(io_timeout * 5);
                    let _ = stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok");
                });
            }
        });

        let mut client = TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        client.write_all(b"GET /slow HTTP/1.1\r\n\r\n").unwrap();
        let mut resp = String::new();
        client.read_to_string(&mut resp).unwrap();
        assert!(
            resp.starts_with("HTTP/1.1 200 OK"),
            "a slow-to-produce response must still arrive, not be silently dropped: {resp:?}"
        );
    }

    fn parse(response: &str) -> (&str, &str, &str) {
        let (head, body) = response
            .split_once("\r\n\r\n")
            .expect("response must have a header/body separator");
        let mut lines = head.splitn(2, "\r\n");
        let status_line = lines.next().expect("missing status line");
        let headers = lines.next().unwrap_or("");
        (status_line, headers, body)
    }

    fn test_state() -> AppState {
        AppState::new(test_dir())
    }

    fn test_dir() -> PathBuf {
        // A per-process counter, not just the clock: macOS `SystemTime` has
        // microsecond resolution, so tests running in parallel could get the
        // same nanos, share a data dir, and race on its WAL `origin` file.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kosha-test-server-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    // ─── Multi-field highlighting (#195) ─────────────────────────────────

    fn highlight_state(ns: &str) -> AppState {
        let state = test_state();
        let ns = NamespaceId(ns.into());
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId("d1".into()),
                    fields: vec![
                        Field::text("title", "Contract law basics"),
                        Field::text("body", "A contract needs an offer"),
                        Field::keyword("tag", "contract"),
                    ],
                }],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();
        state
    }

    fn search_body(response: &str) -> &str {
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        &response[response.find("\r\n\r\n").unwrap() + 4..]
    }

    /// #195: the legacy single-field request form must answer byte for byte
    /// what it did before multi-field highlighting existed: the flat
    /// `highlights` list, and no per-field `highlight` map. The expected
    /// body was captured from origin/main before this change.
    #[test]
    fn legacy_single_field_highlight_response_is_unchanged() {
        let state = highlight_state("hl/legacy");
        let req = br#"{"namespace":"hl/legacy","query_text":"contract","no_cache":true,
            "highlight":{"field":"body","pre_tags":["<em>"],"post_tags":["</em>"]}}"#;
        let body = search_body(&handle_search_post(req, &state)).to_string();
        assert_eq!(body, LEGACY_HIGHLIGHT_SNAPSHOT);
        // Default tags, the minimal legacy form.
        let req = br#"{"namespace":"hl/legacy","query_text":"contract","no_cache":true,
            "highlight":{"field":"title"}}"#;
        let value: serde_json::Value =
            serde_json::from_str(search_body(&handle_search_post(req, &state))).unwrap();
        let hit = &value["results"][0];
        assert_eq!(
            hit["highlights"],
            serde_json::json!(["<b>Contract</b> law basics"])
        );
        assert!(hit.get("highlight").is_none(), "{hit}");
    }

    const LEGACY_HIGHLIGHT_SNAPSHOT: &str = r#"{"results":[{"doc_id":"d1","score":0.39556284962119864,"fields":[{"name":"title","field_type":"Text","value":"Contract law basics"},{"name":"body","field_type":"Text","value":"A contract needs an offer"},{"name":"tag","field_type":"Keyword","value":"contract"}],"highlights":["A <em>contract</em> needs an offer"]}],"total_hits":1,"total_hits_relation":"eq"}"#;

    /// #195: two fields in one request, each with its own tags (falling
    /// back to the top-level tags, then the defaults); a requested field the
    /// document lacks, or that is not text, is skipped silently.
    #[test]
    fn multi_field_highlight_uses_per_field_tags() {
        let state = highlight_state("hl/multi");
        let req = br#"{"namespace":"hl/multi","query_text":"contract",
            "highlight":{"pre_tags":["<i>"],"post_tags":["</i>"],"fields":{
                "title":{"pre_tags":["<mark>"],"post_tags":["</mark>"]},
                "body":{},
                "missing":{},
                "tag":{}}}}"#;
        let value: serde_json::Value =
            serde_json::from_str(search_body(&handle_search_post(req, &state))).unwrap();
        let hit = &value["results"][0];
        assert_eq!(
            hit["highlight"],
            serde_json::json!({
                "title": ["<mark>Contract</mark> law basics"],
                "body": ["A <i>contract</i> needs an offer"],
            })
        );
        assert!(hit.get("highlights").is_none(), "{hit}");

        // No top-level tags: per-field tags, then the `<b>` defaults. The
        // same field asked for in the legacy form must not hit the cache
        // entry of the `fields` form (different response shape).
        let req = br#"{"namespace":"hl/multi","query_text":"contract",
            "highlight":{"fields":{"title":{},"body":{"pre_tags":["["],"post_tags":["]"]}}}}"#;
        let value: serde_json::Value =
            serde_json::from_str(search_body(&handle_search_post(req, &state))).unwrap();
        assert_eq!(
            value["results"][0]["highlight"],
            serde_json::json!({
                "title": ["<b>Contract</b> law basics"],
                "body": ["A [contract] needs an offer"],
            })
        );
        let req = br#"{"namespace":"hl/multi","query_text":"contract",
            "highlight":{"field":"title"}}"#;
        let value: serde_json::Value =
            serde_json::from_str(search_body(&handle_search_post(req, &state))).unwrap();
        assert_eq!(
            value["results"][0]["highlights"],
            serde_json::json!(["<b>Contract</b> law basics"])
        );
        assert!(value["results"][0].get("highlight").is_none());

        // Only unhighlightable fields requested: no map at all.
        let req = br#"{"namespace":"hl/multi","query_text":"contract",
            "highlight":{"fields":{"tag":{},"nope":{}}}}"#;
        let value: serde_json::Value =
            serde_json::from_str(search_body(&handle_search_post(req, &state))).unwrap();
        assert!(value["results"][0].get("highlight").is_none());
    }

    /// #195: `field` and `fields` together is a 400, as is neither.
    #[test]
    fn conflicting_highlight_field_and_fields_is_400() {
        let state = highlight_state("hl/bad");
        let both = br#"{"namespace":"hl/bad","query_text":"contract",
            "highlight":{"field":"body","fields":{"title":{}}}}"#;
        let response = handle_search_post(both, &state);
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert!(response.contains("not both"), "{response}");
        let neither = br#"{"namespace":"hl/bad","query_text":"contract","highlight":{}}"#;
        let response = handle_search_post(neither, &state);
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        let dup = br#"{"namespace":"hl/bad","query_text":"contract",
            "highlight":{"fields":[{"body":{}},{"body":{}}]}}"#;
        assert!(handle_search_post(dup, &state).starts_with("HTTP/1.1 400"));
    }

    fn rc_key(ns: &str, v: u64, q: &str) -> (String, u64, String) {
        (ns.to_string(), v, q.to_string())
    }

    #[test]
    fn knn_cache_keys_digest_vectors_and_distinguish_them() {
        let base = kosha_core::SearchQuery {
            query_text: String::new(),
            max_results: 10,
            bm25_params: Default::default(),
            from: 0,
            filter: None,
            sort: Vec::new(),
            search_after: None,
            highlight: None,
            aggs: std::collections::HashMap::new(),
            wildcard: None,
            fuzzy: None,
            match_phrase: None,
            knn: None,
            exact_total_hits: None,
            total_hits_cap: None,
            operator: None,
            no_cache: None,
            collapse: None,
            limit_per: None,
            rank_by: None,
            source_includes: None,
            source_excludes: None,
            rank: None,
            mode: None,
            fields: Vec::new(),
            rerank: None,
        };
        let with_vec = |v: Vec<f32>, k: usize| {
            let mut q = base.clone();
            q.knn = Some(kosha_core::KnnQuery {
                field: "text_emb".into(),
                vector: v,
                k,
                num_candidates: 100,
                filter: None,
                vectors: None,
                scoring: None,
            });
            q
        };

        // Non-kNN: canonical == plain serde JSON (unchanged behavior).
        assert_eq!(
            canonical_query_for_cache(&base),
            serde_json::to_string(&base).ok()
        );

        let a = canonical_query_for_cache(&with_vec(vec![0.1_f32; 1024], 10)).unwrap();
        let a2 = canonical_query_for_cache(&with_vec(vec![0.1_f32; 1024], 10)).unwrap();
        assert_eq!(a, a2, "same vector must produce the same key");

        // One-bit difference in one component must change the key.
        let mut v = vec![0.1_f32; 1024];
        v[517] = f32::from_bits(v[517].to_bits() ^ 1);
        let b = canonical_query_for_cache(&with_vec(v, 10)).unwrap();
        assert_ne!(a, b, "different vectors must produce different keys");

        // Late interaction (#205): the bag is digested with its token
        // boundaries, never inlined, and a single `vector` with the same
        // floats is a different query.
        let with_bag = |bag: Vec<Vec<f32>>| {
            let mut q = with_vec(Vec::new(), 10);
            q.knn.as_mut().unwrap().vectors = Some(bag);
            canonical_query_for_cache(&q).unwrap()
        };
        let bag = with_bag(vec![vec![0.25, 0.5], vec![0.75, 1.0]]);
        assert_eq!(bag, with_bag(vec![vec![0.25, 0.5], vec![0.75, 1.0]]));
        assert_ne!(bag, with_bag(vec![vec![0.25], vec![0.5, 0.75, 1.0]]));
        assert_ne!(bag, with_bag(vec![vec![0.25, 0.5], vec![0.75, 0.5]]));
        assert_ne!(
            bag,
            canonical_query_for_cache(&with_vec(vec![0.25, 0.5, 0.75, 1.0], 10)).unwrap()
        );
        assert!(
            !bag.contains("0.75"),
            "no raw token vector in the key: {bag}"
        );

        // Non-vector knn params still flow through the canonical JSON.
        let c = canonical_query_for_cache(&with_vec(vec![0.1_f32; 1024], 20)).unwrap();
        assert_ne!(a, c, "k must remain part of the key");

        // Hybrid fusion parameters change the response, so they must
        // change the key (issue #228): the whole-response cache must never
        // serve a response fused with another request's `rank`.
        let hybrid = |rank: serde_json::Value| {
            let mut q = with_vec(vec![0.1_f32; 1024], 10);
            q.query_text = "breach".into();
            q.rank = serde_json::from_value(rank).unwrap();
            canonical_query_for_cache(&q).unwrap()
        };
        let default_rrf = hybrid(serde_json::Value::Null);
        let c60 = hybrid(serde_json::json!({"rrf": {"rank_constant": 60}}));
        let c1 = hybrid(serde_json::json!({"rrf": {"rank_constant": 1}}));
        let w50 = hybrid(serde_json::json!({"rrf": {"rank_window_size": 50}}));
        assert_ne!(c60, c1, "rank_constant must be part of the key");
        assert_ne!(c60, w50, "rank_window_size must be part of the key");
        assert_ne!(default_rrf, c1);
        let mut moded = with_vec(vec![0.1_f32; 1024], 10);
        moded.query_text = "breach".into();
        moded.mode = Some(kosha_core::RetrievalMode::Hybrid);
        assert_ne!(canonical_query_for_cache(&moded).unwrap(), default_rrf);

        // `limit_per` changes the page (issue #211), on both key shapes.
        let limited = |max: usize, vector: bool| {
            let mut q = if vector {
                with_vec(vec![0.1_f32; 1024], 10)
            } else {
                base.clone()
            };
            q.limit_per = Some(kosha_core::LimitPerSpec {
                field: "author".into(),
                max,
            });
            canonical_query_for_cache(&q).unwrap()
        };
        assert_ne!(limited(2, false), canonical_query_for_cache(&base).unwrap());
        assert_ne!(limited(2, false), limited(3, false));
        assert_ne!(limited(2, true), a, "limit_per must be part of the kNN key");
        assert_ne!(limited(2, true), limited(3, true));

        // The whole point: no raw embedding in the key.
        assert!(
            a.len() < 1024,
            "key must stay compact (digested), got {} bytes",
            a.len()
        );
    }

    #[test]
    fn import_manifest_is_deterministic_and_complete() {
        let footer = |id: &str, docs: u32| kosha_core::Footer {
            segment_id: kosha_core::SegmentId(id.into()),
            doc_count: docs,
            total_field_length: docs as u64 * 7,
            avg_field_length: 7.0,
            bm25_params: kosha_core::Bm25Params::default(),
            created_at: "2026-08-11T00:00:00Z".into(),
            filter_blooms: None,
            term_bloom: None,
            format_version: kosha_core::SEGMENT_FORMAT_VERSION,
            text_filter_ignore_above: None,
            doc_vectors: false,
            posting_blobs_raw: false,
            filter_columns: None,
            analysis: None,
            field_index: None,
            fuzzy_bloom: None,
            vector_fields: None,
            multi_vectors: Vec::new(),
        };
        // Deliberately unsorted input: S3 listing order must not leak into
        // the rebuilt manifest.
        let m = manifest_from_footers(vec![
            footer("ns-000002", 30),
            footer("ns-000000", 10),
            footer("ns-000001", 20),
        ]);
        assert_eq!(m.version, 1, "imports start a fresh version history");
        let ids: Vec<&str> = m.segments.iter().map(|e| e.segment_id.0.as_str()).collect();
        assert_eq!(ids, vec!["ns-000000", "ns-000001", "ns-000002"]);
        let docs: u64 = m.segments.iter().map(|e| e.doc_count as u64).sum();
        assert_eq!(docs, 60);
        assert_eq!(
            m.segment_footers.len(),
            3,
            "every footer is cached in memory on the importing pod (not \
             persisted — #241), so its first searches skip footer hydration"
        );
    }

    #[test]
    fn result_cache_hits_evicts_and_isolates_versions() {
        let mut c = ResultCache::new(10_000);
        let k1 = rc_key("ns", 1, "q1");
        assert!(c.lookup(&k1).is_none(), "cold lookup misses");
        c.insert(k1.clone(), "resp1");
        assert_eq!(c.lookup(&k1).as_deref(), Some("resp1"), "hit after insert");
        assert_eq!((c.hits, c.misses), (1, 1));

        // A manifest publish bumps the version: same query, new key, miss.
        let k1v2 = rc_key("ns", 2, "q1");
        assert!(c.lookup(&k1v2).is_none(), "version bump orphans entries");

        // Byte-budget LRU: filling past the budget evicts oldest-used.
        let mut c = ResultCache::new(1_000);
        for i in 0..20 {
            c.insert(rc_key("ns", 1, &format!("q{i}")), &"x".repeat(100));
        }
        assert!(c.bytes <= 1_000, "stays within budget ({} bytes)", c.bytes);
        assert!(
            c.lookup(&rc_key("ns", 1, "q0")).is_none(),
            "oldest entry evicted"
        );
        assert!(
            c.lookup(&rc_key("ns", 1, "q19")).is_some(),
            "newest entry retained"
        );
    }

    #[test]
    fn result_cache_rejects_oversized_and_zero_budget() {
        let mut c = ResultCache::new(1_000);
        let big = "x".repeat(400); // > budget/4 with key overhead
        c.insert(rc_key("ns", 1, "big"), &big);
        assert!(
            c.lookup(&rc_key("ns", 1, "big")).is_none(),
            "one oversized response must not monopolize the cache"
        );

        let mut c = ResultCache::new(0);
        c.insert(rc_key("ns", 1, "q"), "resp");
        assert!(c.lookup(&rc_key("ns", 1, "q")).is_none(), "0 disables");
    }

    #[test]
    fn redact_database_url_hides_password() {
        assert_eq!(
            redact_database_url("postgresql://kosha:secret@host:5432/kosha"),
            "postgresql://kosha:***@host:5432/kosha"
        );
        assert_eq!(
            redact_database_url("postgresql://localhost/kosha"),
            "postgresql://localhost/kosha"
        );
    }

    // ── Usage-driven warming (#213) ─────────────────────────────────────

    /// Stands in for S3 in warming tests: `remote` is the ingest pod's
    /// data dir (every published segment), `local` the query pod's cache,
    /// and `warm_sets` the bucket prefix the warm set is persisted under.
    struct MirrorStorage {
        remote: PathBuf,
        local: PathBuf,
        warm_sets: PathBuf,
    }

    impl MirrorStorage {
        fn new(remote: &Path, local: &Path, warm_sets: &Path) -> Self {
            Self {
                remote: remote.to_path_buf(),
                local: local.to_path_buf(),
                warm_sets: warm_sets.to_path_buf(),
            }
        }
    }

    impl warm_run::WarmStorage for MirrorStorage {
        fn class_sizes(
            &self,
            ns: &NamespaceId,
        ) -> Result<HashMap<(String, warm::FileClass), u64>, String> {
            Ok(warm_run::local_class_sizes(&self.remote.join(&ns.0)))
        }

        fn warm_batch(
            &self,
            ns: &NamespaceId,
            _manifest: &kosha_core::Manifest,
            class: warm::FileClass,
            segments: &[String],
        ) -> warm_run::BatchOutcome {
            let mut o = warm_run::BatchOutcome::default();
            for seg in segments {
                let from = self.remote.join(&ns.0).join(seg);
                let to = self.local.join(&ns.0).join(seg);
                fs::create_dir_all(&to).unwrap();
                for f in fs::read_dir(&from).unwrap().flatten() {
                    let name = f.file_name().to_string_lossy().into_owned();
                    // Files no class names (`_COMMIT`) travel with the
                    // scaffolding so the segment opens.
                    let c = warm::FileClass::of_file(&name).unwrap_or(warm::FileClass::Scoring);
                    let dest = to.join(&name);
                    if c != class || !f.path().is_file() || dest.exists() {
                        continue;
                    }
                    o.bytes += fs::copy(f.path(), &dest).unwrap();
                    o.files += 1;
                }
            }
            o
        }

        fn load_warm_set(&self, role: &str) -> Result<Option<warm::WarmSet>, String> {
            match fs::read(self.warm_sets.join(warm::warm_set_path(role))) {
                Ok(b) => Ok(Some(serde_json::from_slice(&b).unwrap())),
                Err(_) => Ok(None),
            }
        }

        fn store_warm_set(&self, role: &str, set: &warm::WarmSet) -> Result<(), String> {
            let path = self.warm_sets.join(warm::warm_set_path(role));
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, serde_json::to_vec(set).unwrap()).map_err(|e| e.to_string())
        }
    }

    /// An ingest pod publishing to `shared`, with namespaces pre-created.
    fn warm_ingest(shared: &SharedManifestStore, namespaces: &[&str]) -> AppState {
        for ns in namespaces {
            shared
                .inner
                .lock()
                .unwrap()
                .ensure_namespace(NamespaceId(tenant_namespace("test", ns)));
        }
        let ingest = test_state();
        *ingest.controller.lock().unwrap() = Box::new(shared.clone());
        ingest
    }

    /// A query replica (writes forwarded, manifests from `shared`) with an
    /// empty cache and default warming configuration.
    fn warm_query_replica(shared: &SharedManifestStore) -> AppState {
        let mut q = test_state();
        q.ingest_host = Some("http://ingest.invalid".into());
        q.warmup_namespaces.clear();
        q.warming =
            warm_run::Warming::new(warm::WarmConfig::default(), warm::AccessConfig::default());
        *q.controller.lock().unwrap() = Box::new(shared.clone());
        q
    }

    fn scratch_dir(tag: &str) -> PathBuf {
        let dir = test_state().data_dir.join(tag);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn search_hits(state: &AppState, ns: &str) -> String {
        handle_search_post(
            format!(r#"{{"namespace":"{ns}","query_text":"segment"}}"#).as_bytes(),
            state,
        )
    }

    fn stored_segments(shared: &SharedManifestStore, ns: &NamespaceId) -> Vec<String> {
        shared
            .inner
            .lock()
            .unwrap()
            .manifest(ns)
            .unwrap()
            .segments
            .iter()
            .map(|e| e.segment_id.0.clone())
            .collect()
    }

    /// Acceptance (a): after a compaction publishes, the re-warm poll on a
    /// query pod installs the new manifest and makes the merged segment's
    /// files local — for a namespace that is hot from traffic, with no
    /// explicit warmup list — before the next user query arrives, so that
    /// query pays no cold fetch. A namespace nobody queries is left to lazy
    /// hydration, as before.
    #[test]
    fn rewarm_makes_a_compacted_segment_local_before_the_next_search() {
        let shared = shared_store();
        let ingest = warm_ingest(&shared, &["hot", "idle"]);
        let ns = seed_segments(&ingest, "hot", 3);
        let idle = seed_segments(&ingest, "idle", 1);
        let mut query = warm_query_replica(&shared);
        // Hot means at least two decayed searches here: 3 on `hot`, 1 on
        // `idle`.
        let mut cfg = warm::WarmConfig::default();
        cfg.limits.min_score = 2.0;
        query.warming = warm_run::Warming::new(cfg, warm::AccessConfig::default());
        let storage = MirrorStorage::new(&ingest.data_dir, &query.data_dir, &scratch_dir("ws"));
        // The replica already holds today's segments (earlier traffic).
        copy_tree(&ingest.data_dir.join("test"), &query.data_dir.join("test"));
        for _ in 0..3 {
            assert!(search_hits(&query, &ns.0).contains("\"total_hits\":3"));
        }
        assert!(search_hits(&query, &idle.0).contains("\"total_hits\":1"));
        assert!(
            query.run_rewarm_tick(&storage).is_none(),
            "nothing changed yet"
        );

        let response =
            handle_compact_namespace(br#"{"namespace":"hot","mode":"full"}"#, "test", &ingest);
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        let merged = stored_segments(&shared, &ns);
        assert_eq!(merged.len(), 1);
        let merged_dir = query.data_dir.join(&ns.0).join(&merged[0]);
        assert!(!merged_dir.exists(), "the merged segment starts cold");
        // A flush publishes a new segment on the idle namespace too.
        let idle_before = stored_segments(&shared, &idle);
        index_one(&ingest, &idle.0, "late");
        route(
            "POST /flush HTTP/1.1\r\n",
            &HashMap::new(),
            br#"{"namespace":"idle"}"#,
            "test",
            &ingest,
        );
        assert_eq!(stored_segments(&shared, &idle).len(), idle_before.len() + 1);

        let report = query
            .run_rewarm_tick(&storage)
            .expect("a hot namespace's manifest changed");
        assert_eq!(report.trigger, "rewarm");
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(report.warmed.iter().all(|i| i.segment == merged[0]));
        assert!(report.skipped.is_empty());
        assert_eq!(report.namespaces.len(), 1, "idle is not re-warmed");
        // Installed before any user query asked for it...
        assert_eq!(
            query.indexer.manifest_version(&ns),
            shared
                .inner
                .lock()
                .unwrap()
                .manifest(&ns)
                .map(|m| m.version)
        );
        // ...and every file of the merged segment is local.
        for f in fs::read_dir(ingest.data_dir.join(&ns.0).join(&merged[0]))
            .unwrap()
            .flatten()
        {
            assert!(
                merged_dir.join(f.file_name()).is_file(),
                "{:?} not warmed",
                f.file_name()
            );
        }
        // The idle namespace's new segment stays cold until queried.
        let idle_new: Vec<String> = stored_segments(&shared, &idle)
            .into_iter()
            .filter(|s| !idle_before.contains(s))
            .collect();
        assert!(!query.data_dir.join(&idle.0).join(&idle_new[0]).exists());

        // The next search is served from local files.
        assert!(search_hits(&query, &ns.0).contains("\"total_hits\":3"));
        // Compacted inputs no longer count; a second poll has nothing to do.
        let rates = query.warming.tracker.namespace(&ns.0).unwrap();
        assert!(rates.segments.iter().all(|(s, _)| s == &merged[0]));
        assert!(query.run_rewarm_tick(&storage).is_none());
    }

    /// The manifest refresher (#232) and the re-warm poll (#213) both
    /// install new manifests on a query pod. When the refresher gets there
    /// first, the re-warm poll must still see the segments the new version
    /// added and warm them, rather than compare the already-installed
    /// manifest with itself.
    #[test]
    fn rewarm_warms_segments_the_manifest_refresher_installed_first() {
        let shared = shared_store();
        let ingest = warm_ingest(&shared, &["hot"]);
        let ns = seed_segments(&ingest, "hot", 3);
        let mut query = warm_query_replica(&shared);
        let mut cfg = warm::WarmConfig::default();
        cfg.limits.min_score = 2.0;
        query.warming = warm_run::Warming::new(cfg, warm::AccessConfig::default());
        let storage = MirrorStorage::new(&ingest.data_dir, &query.data_dir, &scratch_dir("wr"));
        copy_tree(&ingest.data_dir.join("test"), &query.data_dir.join("test"));
        for _ in 0..3 {
            assert!(search_hits(&query, &ns.0).contains("\"total_hits\":3"));
        }
        assert!(query.run_rewarm_tick(&storage).is_none());

        let response =
            handle_compact_namespace(br#"{"namespace":"hot","mode":"full"}"#, "test", &ingest);
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        let merged = stored_segments(&shared, &ns);
        assert_eq!(merged.len(), 1);
        // The refresher installs the compacted manifest first.
        query.run_manifest_refresh_tick();
        assert_eq!(
            query.indexer.manifest_version(&ns),
            shared
                .inner
                .lock()
                .unwrap()
                .manifest(&ns)
                .map(|m| m.version)
        );
        let merged_dir = query.data_dir.join(&ns.0).join(&merged[0]);
        assert!(!merged_dir.join("inverted.idx").exists(), "still cold");

        let report = query
            .run_rewarm_tick(&storage)
            .expect("the refresher's install is still news to the warmer");
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(report.warmed.iter().any(|i| i.segment == merged[0]));
        assert!(merged_dir.join("inverted.idx").is_file());
        assert!(query.run_rewarm_tick(&storage).is_none());
    }

    /// Acceptance (b): traffic shifts from namespace A to B; the pod
    /// persists its warm set; a restarted pod with an empty cache and the
    /// same (empty) `KOSHA_WARMUP_NAMESPACES` warms B, not A.
    #[test]
    fn restarted_pod_warms_the_namespace_traffic_shifted_to() {
        let shared = shared_store();
        let ingest = warm_ingest(&shared, &["a", "b"]);
        let a = seed_segments(&ingest, "a", 2);
        let b = seed_segments(&ingest, "b", 2);
        let warm_sets = scratch_dir("warmsets");

        let pod1 = warm_query_replica(&shared);
        copy_tree(&ingest.data_dir.join("test"), &pod1.data_dir.join("test"));
        let storage1 = MirrorStorage::new(&ingest.data_dir, &pod1.data_dir, &warm_sets);
        for _ in 0..20 {
            assert!(search_hits(&pod1, &a.0).contains("\"total_hits\":2"));
        }
        // Two hours (12 half-lives) later, all traffic is on B.
        pod1.warming.tracker.advance(Duration::from_secs(2 * 3600));
        for _ in 0..10 {
            assert!(search_hits(&pod1, &b.0).contains("\"total_hits\":2"));
        }
        let rates = pod1.warming.tracker.namespace(&b.0).unwrap();
        // Repeats are served from the result cache: still demand for the
        // namespace's scaffolding and postings, but no page read.
        assert!(
            rates.hits.postings > 9.0 && rates.hits.doc_store > 0.9,
            "{rates:?}"
        );
        assert_eq!(pod1.persist_warm_set(&storage1), Ok(1), "only B is hot");
        drop(pod1);

        let pod2 = warm_query_replica(&shared);
        assert!(pod2.warmup_namespaces.is_empty(), "no environment change");
        let storage2 = MirrorStorage::new(&ingest.data_dir, &pod2.data_dir, &warm_sets);
        let report = pod2.run_boot_warmup(&storage2);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let warmed: Vec<&str> = report
            .namespaces
            .iter()
            .map(|n| n.namespace.as_str())
            .collect();
        assert_eq!(warmed, vec![b.0.as_str()]);
        assert_eq!(report.namespaces[0].source, Some(warm::Source::Usage));
        assert_eq!(report.skipped_items, 0);
        for seg in stored_segments(&shared, &b) {
            let dir = pod2.data_dir.join(&b.0).join(&seg);
            assert!(dir.join("inverted.idx").is_file(), "{seg} scaffolding");
            assert!(dir.join("doc_store.bin").is_file(), "{seg} doc store");
        }
        assert!(!pod2.data_dir.join(&a.0).exists(), "A is no longer hot");
        assert!(search_hits(&pod2, &b.0).contains("\"total_hits\":2"));
        let status = pod2.warmup_status_json();
        assert_eq!(status["warm_set"]["loaded_namespaces"], 1);
        assert_eq!(status["boot"]["namespaces"][0]["namespace"], b.0.as_str());
    }

    /// Acceptance (c): the boot pass stops at `KOSHA_WARMUP_BUDGET_BYTES`
    /// and reports what it warmed and what it skipped (log, and
    /// `GET /v1/admin/warmup-status`); a later re-warm spends only what the
    /// budget has left.
    #[test]
    fn warmup_honors_the_byte_budget_and_reports_warmed_and_skipped() {
        let shared = shared_store();
        let ingest = warm_ingest(&shared, &["big"]);
        let ns = seed_segments(&ingest, "big", 3);
        let sizes = warm_run::local_class_sizes(&ingest.data_dir.join(&ns.0));
        let scoring: u64 = sizes
            .iter()
            .filter(|((_, c), _)| *c == warm::FileClass::Scoring)
            .map(|(_, b)| *b)
            .sum();
        let total: u64 = sizes.values().sum();
        assert!(total > scoring, "segments have more than scaffolding");

        let mut pod = warm_query_replica(&shared);
        pod.warmup_namespaces = vec![ns.0.clone()];
        pod.admin_auth_key = Some("admin-key".into());
        pod.warming = warm_run::Warming::new(
            warm::WarmConfig {
                budget_bytes: Some(scoring),
                ..Default::default()
            },
            warm::AccessConfig::default(),
        );
        let storage = MirrorStorage::new(&ingest.data_dir, &pod.data_dir, &scratch_dir("ws"));
        let report = pod.run_boot_warmup(&storage);

        assert_eq!(report.budget_bytes, Some(scoring));
        assert_eq!(report.warmed_items, 3);
        assert_eq!(report.warmed_bytes, scoring);
        assert!(report
            .warmed
            .iter()
            .all(|i| i.class == warm::FileClass::Scoring));
        assert!(report.skipped_items > 0);
        assert_eq!(report.skipped_bytes, total - scoring);
        assert!(report
            .skipped
            .iter()
            .all(|i| i.class != warm::FileClass::Scoring));
        for seg in stored_segments(&shared, &ns) {
            let dir = pod.data_dir.join(&ns.0).join(&seg);
            assert!(dir.join("inverted.idx").is_file());
            assert!(!dir.join("doc_store.bin").exists(), "over budget");
        }

        let mut headers = HashMap::new();
        headers.insert("authorization".to_string(), "Bearer admin-key".to_string());
        let response = route(
            "GET /v1/admin/warmup-status HTTP/1.1",
            &headers,
            b"",
            "service",
            &pod,
        );
        let status = json_body(&response);
        assert_eq!(status["boot"]["warmed_items"], 3);
        assert_eq!(status["boot"]["skipped_items"], report.skipped_items);
        assert!(!status["boot"]["skipped"].as_array().unwrap().is_empty());
        assert_eq!(status["budget_remaining_bytes"], 0);
        assert_eq!(status["config"]["budget_bytes"], scoring);

        // A flush adds a segment; the budget is spent, so the re-warm
        // reports it skipped rather than overrunning.
        index_one(&ingest, &ns.0, "late");
        route(
            "POST /flush HTTP/1.1\r\n",
            &HashMap::new(),
            br#"{"namespace":"big"}"#,
            "test",
            &ingest,
        );
        let rewarm = pod.run_rewarm_tick(&storage).expect("manifest changed");
        assert_eq!(rewarm.available_bytes, Some(0));
        assert_eq!(rewarm.warmed_items, 0);
        assert!(rewarm.skipped_items > 0);
        assert_eq!(pod.warmup_status_json()["rewarms"][0]["warmed_items"], 0);
    }

    #[test]
    fn searches_record_reads_per_segment_and_class() {
        let state = test_state();
        let ns = seed_segments(&state, "tracked", 2);
        let response = search_hits(&state, &ns.0);
        assert!(response.contains("\"total_hits\":2"), "{response}");
        let rates = state.warming.tracker.namespace(&ns.0).unwrap();
        assert!((rates.hits.scoring - 1.0).abs() < 1e-3);
        assert!((rates.hits.postings - 1.0).abs() < 1e-3);
        assert!((rates.hits.doc_store - 1.0).abs() < 1e-3, "page read");
        assert_eq!(rates.hits.vectors, 0.0);
        assert_eq!(rates.segments.len(), 2);
        for (_, v) in &rates.segments {
            assert!((v[warm::FileClass::DocStore.index()] - 1.0).abs() < 1e-3);
        }
    }
}
