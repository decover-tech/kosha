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
#[cfg(feature = "s3")]
mod hydration_budget;
#[cfg(feature = "migrate")]
mod migrate;
#[cfg(feature = "s3")]
mod s3_storage;
#[cfg(feature = "s3")]
mod segment_commit;

#[cfg(feature = "s3")]
use hydration_budget::{HydrationBudget, RequestHydration};
use kosha_cache::Cache;
#[cfg(feature = "s3")]
use kosha_core::StorageBackend;
#[cfg(feature = "s3")]
use kosha_core::{segment_may_contain_terms, TermBloomMode};
use kosha_core::{ControlStore, IndexRequest, IndexResponse, KoshaError, NamespaceId, SearchQuery};
use kosha_query::DocStoreHydrator;
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
    /// every query — see `global_probe_cutoff`) reproduces the same
    /// cold-start convoy this field's sibling exists to prevent. Its own
    /// flag rather than folding into `warmup_posting_blobs`: this phase
    /// mutates the *live* `segment_cache`/`MemoryLedger` that concurrent
    /// real queries also touch, a materially different risk tier than
    /// phases 1-4's pure disk-byte prefetch.
    warmup_hydrate_segment_cache: bool,
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
}

/// Cached verdict for `completed_hydrations`: NOT a single "namespace is
/// fully hydrated" bool, but which *file classes* have actually been
/// confirmed local for every segment in this `(namespace, manifest_version)`
/// — because whether a hydration pass even looks at `filters.bin` or
/// `vector.idx` depends entirely on the query that drove it
/// (`query_needs_filter_store` / `query.knn.is_some()`). A pass for a plain
/// unfiltered, unsorted, non-kNN query — including warmup's own probe query,
/// see `run_warmup` — legitimately reports `outcome.missing.is_empty()` with
/// *neither* file ever having been checked. Recording that success as a
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
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
struct HydrationVerdict {
    filters_verified: bool,
    vectors_verified: bool,
}

impl HydrationVerdict {
    /// Whether this verdict can be trusted to skip a full
    /// `hydrate_segments_for_search` pass for a query with these needs.
    fn covers(&self, needs_filters: bool, needs_vectors: bool) -> bool {
        (!needs_filters || self.filters_verified) && (!needs_vectors || self.vectors_verified)
    }

    /// Record what a successful, non-bloom-pruned pass for a query with
    /// these needs actually verified, merging with (not replacing) whatever
    /// was already known for this manifest version.
    fn merge(&mut self, needs_filters: bool, needs_vectors: bool) {
        self.filters_verified |= needs_filters;
        self.vectors_verified |= needs_vectors;
    }
}

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
    // exists to prevent.
    if name == "vector.idx" || name == "vector.offsets" {
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
    s3.sync_segment_dir(Path::new(&rel))
        .map_err(|e| format!("S3 upload of segment {rel} failed: {e}"))
}

impl AppState {
    fn new(data_dir: PathBuf) -> Self {
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
        let indexer = Indexer::new(data_dir.clone())
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

        // ── Control plane: in-memory or Postgres ─────────────────────────
        let mut control_plane_kind: &'static str = "in-memory";
        let control_store: Box<dyn ControlStore> = if let Ok(db_url) = std::env::var("DATABASE_URL")
        {
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
            completed_hydrations: Mutex::new(HashMap::new()),
            result_cache: Mutex::new(ResultCache::new(result_cache_budget())),
            posting_blob_presence: Mutex::new(HashMap::new()),
            // Warmup starts ready; `spawn_warmup` flips `false` only when
            // there's configured work to do (and back to `true` on
            // completion / fail-open). See the field's doc comment.
            warmup_complete: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            warmup_namespaces,
            warmup_posting_blobs,
            warmup_hydrate_segment_cache,
            ingest_host,
            write_http_client,
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
        if self.warmup_namespaces.is_empty() {
            return;
        }
        #[cfg(not(feature = "s3"))]
        {
            // No durable tier → nothing to prefetch; pod is ready as-is.
            return;
        }
        #[cfg(feature = "s3")]
        {
            self.warmup_complete
                .store(false, std::sync::atomic::Ordering::SeqCst);
            let state = Arc::clone(self);
            std::thread::Builder::new()
                .name("kosha-warmup".into())
                .spawn(move || state.run_warmup())
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

    /// Readiness-gate predicate used by the `/readyz` handler: true when no
    /// warmup is configured or it has finished (success or fail-open). Not a
    /// health signal — liveness stays on `/healthz`.
    fn is_ready(&self) -> bool {
        self.warmup_complete
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(feature = "s3")]
    fn run_warmup(&self) {
        let start = std::time::Instant::now();
        let total = self.warmup_namespaces.len();
        for (idx, ns_name) in self.warmup_namespaces.iter().enumerate() {
            let ns = NamespaceId(ns_name.clone());
            let (manifest, tombstones) = {
                let Some(m) = self.indexer.manifest_cloned(&ns) else {
                    eprintln!("warmup: namespace '{ns_name}' not found in control plane; skipping");
                    continue;
                };
                let t = self.indexer.get_tombstones(&ns);
                (m, t)
            };
            if manifest.segments.is_empty() {
                println!("warmup: '{ns_name}' has no segments; nothing to prefetch");
                continue;
            }
            // Empty query → bloom-prune is a no-op, no posting blobs or
            // doc spans are fetched — just the scoring scaffolding
            // (footer.json + inverted.idx + doc_store.offsets) for every
            // segment in the manifest. The first real query then pays
            // only for its term shards + page doc spans.
            let query = kosha_core::SearchQuery {
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
                match_phrase: None,
                knn: None,
                exact_total_hits: None,
                total_hits_cap: None,
                operator: None,
                no_cache: None,
                collapse: None,
                limit_per: None,
                source_includes: None,
                source_excludes: None,
                rank: None,
                mode: None,
                fields: Vec::new(),
            };
            let needs_filters = query_needs_filter_store(&query);
            let needs_vectors = query.knn.is_some();
            let t_ns = std::time::Instant::now();
            let outcome = self.hydrate_segments_for_search(
                &ns,
                &manifest,
                &query,
                &RequestHydration::unlimited(),
            );
            let _ = tombstones;
            let dur = t_ns.elapsed();
            // query.filter is always None above, so bloom_pruned is always
            // false here — this condition is defensive consistency with
            // the search-path callers (see HydrationOutcome::bloom_pruned),
            // not a behavior change for warmup itself.
            if outcome.missing.is_empty() && !outcome.bloom_pruned {
                // Mirror the search path's verdict caching (handle_search_post):
                // record only what this pass actually verified — see
                // `HydrationVerdict`. Warmup's probe query is unfiltered,
                // unsorted, and non-kNN, so `needs_filters`/`needs_vectors`
                // are both false here: this merge is a no-op today (neither
                // flag is set), which is exactly the point — warmup's own
                // success must NOT be mistaken for "filters.bin/vector.idx
                // are local," or a later filtered/sorted/kNN query on this
                // same pod would wrongly take the cached-verdict fast path
                // and silently skip hydrating the file it actually needs.
                self.completed_hydrations
                    .lock()
                    .unwrap()
                    .entry((ns.0.clone(), manifest.version))
                    .or_default()
                    .merge(needs_filters, needs_vectors);
                println!(
                    "warmup: [{}/{}] '{ns_name}' ready: fetched {} file(s), {:.1} MB in {:.1}s",
                    idx + 1,
                    total,
                    outcome.files_fetched,
                    outcome.bytes_fetched as f64 / (1024.0 * 1024.0),
                    dur.as_secs_f64(),
                );
            } else {
                eprintln!(
                    "warn: warmup: [{}/{}] '{ns_name}' partial — {} segment(s) still missing \
                     after fetch ({:.1}s); live cold-query fallback (HTTP 503 + client retry) \
                     will cover the rest",
                    idx + 1,
                    total,
                    outcome.missing.len(),
                    dur.as_secs_f64(),
                );
            }

            // Phase 2 — bulk posting-blob prefetch (the cold-start-convoy
            // fix). The scoring-set pass above leaves every `postings-XX.bin`
            // shard to first-touch queries; at 8 QPS those all fan out to S3
            // at once, saturate the hydrator, and queue behind each other
            // (measured: ~3 minutes of 7–12s p99 after a cold start, vs
            // 529ms steady-state). Prefetching the shards here moves that
            // transient off the query path entirely.
            //
            // Batched in small segment groups on purpose:
            // `ensure_posting_blobs_local` holds a `hydration_semaphore`
            // permit (default 4) for the duration of each call, so one
            // whole-namespace call would starve live queries arriving
            // mid-warmup — recreating the convoy in a different queue. Short
            // batches interleave with query traffic, and each batch's
            // verified shards land in the presence cache as it completes.
            // Requires `inverted.idx` local to detect split segments, which
            // phase 1 just ensured.
            if self.warmup_posting_blobs {
                let t_blobs = std::time::Instant::now();
                let all_blobs = kosha_segment::all_posting_blob_files();
                let seg_paths: Vec<PathBuf> = manifest
                    .segments
                    .iter()
                    .map(|entry| self.data_dir.join(&ns.0).join(&entry.segment_id.0))
                    .collect();
                let mut blob_files = 0usize;
                let mut blob_bytes = 0u64;
                let mut blob_missing = 0usize;
                for chunk in seg_paths.chunks(8) {
                    let o = self.ensure_posting_blobs_local(
                        &ns,
                        manifest.version,
                        chunk,
                        &all_blobs,
                        &RequestHydration::unlimited(),
                    );
                    blob_files += o.files_fetched;
                    blob_bytes += o.bytes_fetched;
                    blob_missing += o.missing.len();
                }
                if blob_missing == 0 {
                    println!(
                        "warmup: [{}/{}] '{ns_name}' posting blobs ready: fetched {} blob(s), \
                         {:.1} MB in {:.1}s",
                        idx + 1,
                        total,
                        blob_files,
                        blob_bytes as f64 / (1024.0 * 1024.0),
                        t_blobs.elapsed().as_secs_f64(),
                    );
                } else {
                    eprintln!(
                        "warn: warmup: [{}/{}] '{ns_name}' posting blobs partial — {} blob(s) \
                         still missing after fetch ({:.1}s); first-touch queries will hydrate \
                         the rest",
                        idx + 1,
                        total,
                        blob_missing,
                        t_blobs.elapsed().as_secs_f64(),
                    );
                }

                // Phase 3 — doc stores. Page materialization (Option A, see
                // `ensure_doc_store` in `handle_search_post`) asks for the
                // WHOLE `doc_store.bin` of every page-hit segment, so on a
                // cold pod the first queries pay full-file S3 fetches — for
                // large compacted segments that is a multi-GB in-memory GET
                // on the query path (the round-7 incident: a 10.6GB merged
                // doc store failed repeatedly under memory pressure and
                // 100% of queries 503'd). Prefetching here moves that cost
                // into the readiness gate, batched like the blobs so the
                // hydration semaphore is never held for the whole namespace.
                // Chunks of 2, not 8: post-compaction doc stores are the
                // largest files in a segment, and a fat batch of them holds
                // the hydration permit (and a large slice of the in-flight
                // byte budget) away from live queries for its whole
                // duration. This used to be a heap constraint as well — the
                // fetch buffered each body whole — which streaming to disk
                // has removed, so whether 2 is still the right number is now
                // a throughput question to measure, not a memory one.
                let t_ds = std::time::Instant::now();
                let mut ds_files = 0usize;
                let mut ds_bytes = 0u64;
                let warmup_request = RequestHydration::unlimited();
                for chunk in seg_paths.chunks(2) {
                    let (files, bytes) = self.ensure_doc_store_local(chunk, &warmup_request);
                    ds_files += files;
                    ds_bytes += bytes;
                }
                if ds_files > 0 {
                    println!(
                        "warmup: [{}/{}] '{ns_name}' doc stores ready: fetched {} file(s), \
                         {:.1} MB in {:.1}s",
                        idx + 1,
                        total,
                        ds_files,
                        ds_bytes as f64 / (1024.0 * 1024.0),
                        t_ds.elapsed().as_secs_f64(),
                    );
                }

                // Phase 4 — vector indexes, only for namespaces that have
                // them. A cold kNN query otherwise hydrates `vector.idx`
                // (hundreds of MB to GBs per segment at embedding scale)
                // on the query path — the doc-store lesson (#123/#126) in
                // vector form.
                //
                // 2026-08-15: previously probed only the *first* segment's
                // tiny `vector.offsets` sidecar and skipped this whole phase
                // for the namespace if that one segment lacked it, on the
                // assumption that "segments are format-homogeneous within a
                // namespace in practice." A bulk migration can violate that:
                // a single flush batch with zero embedded source documents
                // produces one vector-less segment sitting alongside
                // hundreds of segments that do have vectors — the single-
                // segment probe then silently skipped vector warmup for the
                // *entire* namespace, leaving `vector.idx`/`vector.offsets`
                // never hydrated and every kNN query returning empty with no
                // error (`load_vector_index`'s local-file-missing path is
                // deliberately silent). `ensure_files_local` already
                // tolerates per-segment missing files (see its doc comment),
                // so there's no need for the namespace-wide gate at all —
                // always attempt the fetch and let each segment's own
                // presence/absence of `vector.offsets` decide for itself.
                let t_vec = std::time::Instant::now();
                let mut vec_files = 0usize;
                let mut vec_bytes = 0u64;
                for chunk in seg_paths.chunks(8) {
                    let (files, bytes) =
                        self.ensure_files_local(chunk, "vector.offsets", &warmup_request);
                    vec_files += files;
                    vec_bytes += bytes;
                }
                // Same chunks-of-2 as doc stores, for the same
                // permit-fairness reason.
                for chunk in seg_paths.chunks(2) {
                    let (files, bytes) =
                        self.ensure_files_local(chunk, "vector.idx", &warmup_request);
                    vec_files += files;
                    vec_bytes += bytes;
                }
                if vec_files > 0 {
                    println!(
                        "warmup: [{}/{}] '{ns_name}' vector indexes ready: fetched {} file(s), \
                         {:.1} MB in {:.1}s",
                        idx + 1,
                        total,
                        vec_files,
                        vec_bytes as f64 / (1024.0 * 1024.0),
                        t_vec.elapsed().as_secs_f64(),
                    );
                }

                // Phase 5 — actually open every segment into the live
                // `Searcher::segment_cache`, not just prefetch its bytes.
                // Phase 4 above makes `open_segment` *fast* on first
                // touch; without this, it's still first-touch — the
                // first wave of real kNN queries pays for opening every
                // segment itself (global_probe_cutoff touches all of
                // them on every query), reproducing the same convoy
                // phase 4 exists to prevent, just one query later.
                if self.warmup_hydrate_segment_cache {
                    let t_cache = std::time::Instant::now();
                    match self.searcher.warm_segments(&ns, &manifest, true) {
                        Ok(o) => println!(
                            "warmup: [{}/{}] '{ns_name}' segment cache warmed: {} opened, \
                             {} already cached, ~{:.1} MB charged in {:.1}s",
                            idx + 1,
                            total,
                            o.opened,
                            o.already_cached,
                            o.bytes as f64 / (1024.0 * 1024.0),
                            t_cache.elapsed().as_secs_f64(),
                        ),
                        Err(e) => eprintln!(
                            "warn: warmup: [{}/{}] '{ns_name}' segment cache warm failed: \
                             {e} — first real kNN queries will cold-open (fail-open)",
                            idx + 1,
                            total,
                        ),
                    }
                }
            }
        }
        self.warmup_complete
            .store(true, std::sync::atomic::Ordering::SeqCst);
        println!(
            "warmup: complete in {:.1}s — pod is ready",
            start.elapsed().as_secs_f64()
        );
    }

    /// Persist the indexer's current manifest for a namespace into the
    /// control store, so the segment list survives restarts.
    fn refresh_query_namespace(&self, ns: &NamespaceId) -> Result<(), KoshaError> {
        if self.ingest_host.is_some() {
            // Ask for the version first. This runs before every search on a
            // query replica, and it used to read and parse the namespace's
            // whole manifest row each time (megabytes for a few hundred
            // segments) while holding the control-store lock every other
            // search was waiting on, then clone the local manifest to
            // compare one integer. The manifest is only fetched when the
            // stored version is ahead.
            let stored = self
                .controller
                .lock()
                .unwrap()
                .manifest_version(ns)
                .ok_or_else(|| KoshaError::NamespaceNotFound(ns.clone()))?;
            if self
                .indexer
                .manifest_version(ns)
                .is_some_and(|local| local >= stored)
            {
                return Ok(());
            }
            let mut manifest = self
                .controller
                .lock()
                .unwrap()
                .manifest_cloned(ns)
                .ok_or_else(|| KoshaError::NamespaceNotFound(ns.clone()))?;
            if self
                .indexer
                .manifest_version(ns)
                .is_none_or(|local| local < manifest.version)
            {
                kosha_core::deletes::hydrate(&mut manifest, |segment, vector| {
                    let path = vector.path(ns, segment)?;
                    #[cfg(feature = "s3")]
                    if let Some(ref s3) = self.s3_storage {
                        return s3.read(&path);
                    }
                    Ok(std::fs::read(self.data_dir.join(path))?)
                })?;
                self.indexer.restore_manifest(ns.clone(), manifest);
            }
        }
        Ok(())
    }

    fn persist_manifest(&self, ns: &NamespaceId) -> Result<(), KoshaError> {
        if let Some(manifest) = self.indexer.manifest_cloned(ns) {
            self.controller
                .lock()
                .unwrap()
                .save_manifest(ns, &manifest)?;
        }
        Ok(())
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
    fn publish_namespace_with(
        &self,
        ns: &NamespaceId,
        upload: impl FnOnce(&kosha_core::Manifest) -> Result<(), KoshaError>,
    ) -> Result<(), KoshaError> {
        let lock = self
            .publication_locks
            .lock()
            .unwrap()
            .entry(ns.clone())
            .or_default()
            .clone();
        let _guard = lock.lock().unwrap();
        let Some((expected_version, manifest)) = self.indexer.prepare_publication(ns)? else {
            return Ok(());
        };
        upload(&manifest)?;
        self.controller.lock().unwrap().compare_and_swap_manifest(
            ns,
            expected_version,
            manifest.clone(),
        )?;
        self.indexer.acknowledge_manifest(ns, &manifest)?;
        Ok(())
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
    const REQUIRED_SEGMENT_FILES: [&'static str; 4] = segment_commit::REQUIRED_SEGMENT_FILES;

    #[cfg(feature = "s3")]
    fn segment_is_complete(seg_path: &Path) -> bool {
        Self::REQUIRED_SEGMENT_FILES
            .iter()
            .all(|f| seg_path.join(f).is_file())
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
        let needs_vectors = query.knn.is_some();
        let needs_filters = query_needs_filter_store(query);
        let all_seg_paths: Vec<PathBuf> = manifest
            .segments
            .iter()
            .map(|entry| self.data_dir.join(&ns.0).join(&entry.segment_id.0))
            .collect();
        let paths_with_manifest_footer: std::collections::HashSet<PathBuf> = manifest
            .segments
            .iter()
            .filter(|entry| manifest.segment_footer(&entry.segment_id).is_some())
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
        let analyses: Vec<Option<&kosha_core::SegmentAnalysis>> = manifest
            .segments
            .iter()
            .map(|entry| segment_footer(entry).and_then(|f| f.analysis.as_ref()))
            .collect();
        let posting_files =
            posting_blob_files_for_query(query, &query_analysis, analyses.iter().copied());
        let stats_terms = kosha_query::namespace_stats_terms(query, analyses.iter().copied())
            .filter(|t| !t.is_empty());
        let mut to_hydrate: Vec<PathBuf> = Vec::new();
        let mut stats_only: Vec<PathBuf> = Vec::new();
        for (entry, seg_path) in manifest.segments.iter().zip(all_seg_paths) {
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
        let mut outcome = self.ensure_scoring_files_local(
            &to_hydrate,
            needs_vectors,
            needs_filters,
            &paths_with_manifest_footer,
            request,
        );
        let postings_outcome = self.ensure_posting_blobs_local(
            ns,
            manifest.version,
            &to_hydrate,
            &posting_files,
            request,
        );
        outcome.files_fetched += postings_outcome.files_fetched;
        outcome.bytes_fetched += postings_outcome.bytes_fetched;
        outcome.missing.extend(postings_outcome.missing);
        if !stats_only.is_empty() {
            // Best effort: whatever is still missing afterwards doesn't fail
            // the search. The searcher then can't read that segment's `df`
            // and scores the whole search with per-segment statistics
            // (`bm25_stats: segment_fallback`) — the pre-#246 behavior —
            // rather than with a `df` that leaves the segment out.
            let stats_files = self.ensure_scoring_files_local(
                &stats_only,
                false,
                false,
                &paths_with_manifest_footer,
                request,
            );
            let stats_postings = self.ensure_posting_blobs_local(
                ns,
                manifest.version,
                &stats_only,
                &posting_files,
                request,
            );
            outcome.files_fetched += stats_files.files_fetched + stats_postings.files_fetched;
            outcome.bytes_fetched += stats_files.bytes_fetched + stats_postings.bytes_fetched;
        }
        outcome.files_fetched += footer_files;
        outcome.bytes_fetched += footer_bytes;
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
    /// `doc_store.bin`. `vector.idx` is required only for kNN queries.
    ///
    /// Contrast [`AppState::REQUIRED_SEGMENT_FILES`] / `segment_is_complete`,
    /// which is the *full* set used by the compaction/merge path (those read
    /// every document and must have `doc_store.bin`).
    #[cfg(feature = "s3")]
    fn segment_is_scoring_complete(
        seg_path: &Path,
        needs_vectors: bool,
        needs_filters: bool,
        has_manifest_footer: bool,
    ) -> bool {
        if !has_manifest_footer && !file_present_nonempty(&seg_path.join("footer.json")) {
            return false;
        }
        if !file_present_nonempty(&seg_path.join("inverted.idx")) {
            return false;
        }
        if needs_filters && !file_present_nonempty(&seg_path.join("filters.bin")) {
            return false;
        }
        let doc_access_ok = file_present_nonempty(&seg_path.join("doc_store.offsets"))
            || file_present_nonempty(&seg_path.join("doc_store.bin"));
        if !doc_access_ok {
            return false;
        }
        // Deliberately does NOT also require vector.offsets: it's an
        // optional sidecar (legacy v1 segments never have one; the reader
        // falls back to an eager parse of vector.idx alone when it's
        // absent — same tolerance doc_access_ok above gives
        // doc_store.offsets vs. doc_store.bin). `ensure_scoring_files_local`
        // still proactively *attempts* to fetch it when present on S3 (see
        // its comment) — this predicate just doesn't block "can we score"
        // on that attempt having succeeded, since scoring is correct
        // either way.
        if needs_vectors && !file_present_nonempty(&seg_path.join("vector.idx")) {
            return false;
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
        needs_vectors: bool,
        needs_filters: bool,
        paths_with_manifest_footer: &std::collections::HashSet<PathBuf>,
        request: &Arc<RequestHydration>,
    ) -> HydrationOutcome {
        let Some(ref s3) = self.s3_storage else {
            return HydrationOutcome::default();
        };

        let mut outcome = HydrationOutcome::default();
        let (owned, waiting) = partition_for_hydration(&self.in_flight_segments, seg_paths, |p| {
            Self::segment_is_scoring_complete(
                p,
                needs_vectors,
                needs_filters,
                paths_with_manifest_footer.contains(p),
            )
        });

        if !owned.is_empty() {
            let _permit = self.hydration_semaphore.acquire();

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
            for (seg_path, _) in &owned {
                let Ok(rel_path) = seg_path.strip_prefix(&self.data_dir) else {
                    continue;
                };
                let s3_prefix = rel_path.to_string_lossy().into_owned();

                // `footer.json` is needed only for legacy manifests that do
                // not carry a footer snapshot.
                if !paths_with_manifest_footer.contains(seg_path)
                    && !file_present_nonempty(&seg_path.join("footer.json"))
                {
                    logical_paths.push((format!("{s3_prefix}/footer.json"), 0));
                }
                if !file_present_nonempty(&seg_path.join("inverted.idx")) {
                    logical_paths.push((format!("{s3_prefix}/inverted.idx"), 0));
                }
                if needs_filters && !file_present_nonempty(&seg_path.join("filters.bin")) {
                    logical_paths.push((format!("{s3_prefix}/filters.bin"), 0));
                }

                // doc_store: Lazy segments need only the offsets sidecar;
                // legacy (format_version 0) segments need the full
                // doc_store.bin because eager open parses it. Decide via the
                // already-local footer's format_version. NOTE: compare
                // against 1 — the version that introduced the offsets
                // sidecar — not SEGMENT_FORMAT_VERSION, which moves with
                // every format change (it's already 2 for the lazy inverted
                // index) and would misclassify every v1 segment as legacy,
                // silently re-fetching all their doc stores.
                let has_offsets = file_present_nonempty(&seg_path.join("doc_store.offsets"));
                let lazy_capable = has_offsets
                    || paths_with_manifest_footer.contains(seg_path)
                    || SegmentReader::read_footer(seg_path)
                        .map(|ft| ft.format_version >= 1)
                        .unwrap_or(false);
                if lazy_capable {
                    if !has_offsets {
                        logical_paths.push((format!("{s3_prefix}/doc_store.offsets"), 0));
                    }
                } else if !file_present_nonempty(&seg_path.join("doc_store.bin")) {
                    logical_paths.push((format!("{s3_prefix}/doc_store.bin"), 0));
                }

                if needs_vectors {
                    if !file_present_nonempty(&seg_path.join("vector.idx")) {
                        logical_paths.push((format!("{s3_prefix}/vector.idx"), 0));
                    }
                    // vector.offsets is the v2 posting-index sidecar — like
                    // doc_store.offsets, only some segments have one
                    // (legacy v1 segments never do; the reader falls back
                    // to an eager parse of vector.idx alone when it's
                    // absent). Unlike doc_store's sidecar, there's no
                    // Footer.format_version signal to predict its existence
                    // (vector.idx is deliberately self-describing,
                    // independent of Footer — same as inverted.idx), so
                    // this always attempts the fetch when vectors are
                    // needed; a segment that doesn't have one just shows up
                    // as a per-file miss in `s3.read_many`'s accounting,
                    // not a hydration failure. Without this, the lazy path
                    // never activates for S3-backed segments — the sidecar
                    // never lands locally before `SegmentReader::open`, so
                    // every S3-backed KNN query silently takes the (still
                    // correct, just slower) eager fallback.
                    if !file_present_nonempty(&seg_path.join("vector.offsets")) {
                        logical_paths.push((format!("{s3_prefix}/vector.offsets"), 0));
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

            drop(_permit);
            complete_owned(&self.in_flight_segments, &owned);
        }

        wait_for(&waiting);

        outcome.missing = seg_paths
            .iter()
            .filter(|p| {
                !Self::segment_is_scoring_complete(
                    p,
                    needs_vectors,
                    needs_filters,
                    paths_with_manifest_footer.contains(*p),
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
        let posting_files = posting_blob_files_for_query(
            query,
            &kosha_query::QueryAnalysis::new(query),
            analyses.iter().map(Option::as_ref),
        );
        if posting_files.is_empty() {
            return HydrationOutcome::default();
        }
        let seg_paths: Vec<PathBuf> = manifest
            .segments
            .iter()
            .map(|entry| self.data_dir.join(&ns.0).join(&entry.segment_id.0))
            .collect();
        self.ensure_posting_blobs_local(ns, manifest.version, &seg_paths, &posting_files, request)
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
    state.spawn_idle_flush_timer();
    state.spawn_compaction_scheduler();
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
    let tenant = match authentication {
        Ok(t) => t,
        Err(resp) => {
            stream.write_all(resp.as_bytes()).ok();
            return Ok(());
        }
    };

    // ── Query/ingest role routing ────────────────────────────────────────
    // A query-role pod (KOSHA_INGEST_HOST set) forwards write-path requests
    // to the single ingest pod instead of executing them locally — see
    // is_write_path/forward_to_ingest. Every other request (all reads, and
    // everything on the ingest pod itself, which never forwards) is handled
    // locally exactly as before this existed.
    let response = match (&state.ingest_host, &state.write_http_client) {
        (Some(ingest_host), Some(client)) if is_write_path(&request_line) => {
            forward_to_ingest(client, ingest_host, &request_line, &headers, &body)
        }
        _ => route(&request_line, &headers, &body, &tenant, state),
    };
    stream.write_all(response.as_bytes()).ok();
    Ok(())
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
            let Some((ns, _action)) = rest.rsplit_once('/') else {
                return json_error(400, "invalid namespace path");
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
        if !path.starts_with("/v1/namespaces/") && !["/healthz", "/v1/healthz"].contains(&path) {
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

// ─── POST /index ────────────────────────────────────────────────────────────

fn handle_index(body: &[u8], state: &AppState) -> String {
    let request: IndexRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };

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
            return json_error(500, &format!("publication failed: {e}"));
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
            Err(error) => return json_error(500, &format!("replacement error: {error}")),
        }
    };
    {
        let mut controller = state.controller.lock().unwrap();
        controller.ensure_namespace(namespace.clone());
    }
    if let Err(e) = state.publish_namespace(&namespace) {
        return json_error(500, &format!("publication failed: {e}"));
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
        .map_err(|e| json_error(500, &format!("publication failed: {e}")))
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
            return json_error(500, &format!("publication failed: {e}"));
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
            Err(e) => return json_error(500, &format!("delete error: {e}")),
        }
    };

    if let Err(e) = state.publish_namespace(&ns) {
        return json_error(500, &format!("deletion publication failed: {e}"));
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

            namespaces.push(serde_json::json!({
                "namespace": ns.0,
                "documents": ns_docs,
                "segments": manifest.segments.len(),
                "version": manifest.version,
            }));
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

    json_ok(&serde_json::json!({
        "total_documents": total_docs,
        "total_segments": total_segments,
        "namespaces": namespaces,
        "control_plane": state.control_plane_kind,
        "cache_root": state.cache.root().display().to_string(),
        "cache_size_bytes": state.cache.total_size(),
        "s3_enabled": s3_enabled,
        "segment_checksum_failures": segment_checksum_failures,
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
    }))
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

    let query: kosha_core::SearchQuery = match serde_json::from_value(body_val) {
        Ok(q) => q,
        Err(e) => return json_error(400, &format!("invalid search query: {e}")),
    };
    // Reject out-of-range parameters before touching storage or the
    // result cache (the searcher checks again for non-HTTP callers).
    if let Err(e) = query.validate() {
        return search_error_response(&e);
    }

    if let Err(e) = state.refresh_query_namespace(&ns) {
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
    let cache_key = if result_cache_budget() > 0 && !query.no_cache.unwrap_or(false) {
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
        let needs_filters = query_needs_filter_store(&query);
        let needs_vectors = query.knn.is_some();
        let cached_covers = state
            .completed_hydrations
            .lock()
            .unwrap()
            .get(&verdict_key)
            .is_some_and(|v| v.covers(needs_filters, needs_vectors));
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
            state.ensure_query_posting_blobs(&ns, manifest, &query, &hydration_request)
        } else {
            let outcome =
                state.hydrate_segments_for_search(&ns, manifest, &query, &hydration_request);
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
                    .merge(needs_filters, needs_vectors);
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

    // On-demand `doc_store.bin` for page materialization (Option A): scoring
    // hydrated only the offsets sidecar for Lazy segments, so the searcher
    // asks back for the whole `doc_store.bin` once per distinct page-hit
    // segment — fetched via the existing `ensure_files_local` and
    // **persisted to the disk cache**, so every subsequent warm query
    // against the same segment serves the page from local seeks (sub-ms)
    // instead of re-paying N per-doc S3 ranged GETs every time.
    #[cfg(feature = "s3")]
    let ensure_doc_store = |segs: &[std::path::PathBuf]| {
        // Same accountant as the scoring-set fetch above: the page's doc
        // stores are hundreds of MB per segment and were previously outside
        // every byte gate — the request pays for them, so the request's
        // ceiling has to cover them.
        let (files, bytes) = state.ensure_doc_store_local(segs, &hydration_request);
        if files > 0 {
            println!(
                "page hydrate: fetched {files} doc store(s) ({:.1} MB)",
                bytes as f64 / (1024.0 * 1024.0),
            );
        }
    };
    #[cfg(feature = "s3")]
    let doc_store_hydrator: DocStoreHydrator<'_> = Some(&ensure_doc_store);
    #[cfg(not(feature = "s3"))]
    let doc_store_hydrator: DocStoreHydrator<'_> = None;

    match state.searcher.search_with_doc_store_hydrator(
        &ns,
        manifest,
        &query,
        tombstones,
        doc_store_hydrator,
    ) {
        Ok((result, phases)) => {
            log_search_timing(
                &ns,
                &hydration,
                hydrate_ms,
                queue_ms,
                &phases,
                result.total_hits,
                result.knn_degraded_segments,
            );
            let resp = json_ok(&result);
            // Only successful responses enter the cache, after the search
            // proved every needed file readable.
            if let Some(key) = cache_key {
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
    let digest = format!(
        "dim{}:{:016x}{:016x}",
        knn.vector.len(),
        h1.finish(),
        h2.finish()
    );
    let mut stripped = query.clone();
    if let Some(ref mut k) = stripped.knn {
        k.vector = Vec::new();
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
        match_phrase: None,
        knn: None,
        exact_total_hits: None,
        total_hits_cap: None,
        operator: None,
        no_cache: None,
        collapse: None,
        limit_per: None,
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
    };
    if let Err(e) = query.validate() {
        return search_error_response(&e);
    }

    if let Err(e) = state.refresh_query_namespace(&ns) {
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
        let needs_filters = query_needs_filter_store(&query);
        let needs_vectors = query.knn.is_some();
        let cached_covers = state
            .completed_hydrations
            .lock()
            .unwrap()
            .get(&verdict_key)
            .is_some_and(|v| v.covers(needs_filters, needs_vectors));
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
                    .merge(needs_filters, needs_vectors);
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

    // On-demand `doc_store.bin` page materialization — see
    // `handle_search_post` for the rationale (Option A: persist the whole
    // `doc_store.bin` per page-segment on first miss, then local seeks).
    #[cfg(feature = "s3")]
    let ensure_doc_store = |segs: &[std::path::PathBuf]| {
        // Same accountant as the scoring-set fetch above: the page's doc
        // stores are hundreds of MB per segment and were previously outside
        // every byte gate — the request pays for them, so the request's
        // ceiling has to cover them.
        let (files, bytes) = state.ensure_doc_store_local(segs, &hydration_request);
        if files > 0 {
            println!(
                "page hydrate: fetched {files} doc store(s) ({:.1} MB)",
                bytes as f64 / (1024.0 * 1024.0),
            );
        }
    };
    #[cfg(feature = "s3")]
    let doc_store_hydrator: DocStoreHydrator<'_> = Some(&ensure_doc_store);
    #[cfg(not(feature = "s3"))]
    let doc_store_hydrator: DocStoreHydrator<'_> = None;

    match state.searcher.search_with_doc_store_hydrator(
        &ns,
        manifest,
        &query,
        tombstones,
        doc_store_hydrator,
    ) {
        Ok((result, phases)) => {
            log_search_timing(
                &ns,
                &hydration,
                hydrate_ms,
                queue_ms,
                &phases,
                result.total_hits,
                result.knn_degraded_segments,
            );
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
    if let Err(e) = state.refresh_query_namespace(&ns) {
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
    let indexer = &state.indexer;
    let manifest = match indexer.manifest(&NamespaceId(scoped_ns.clone())) {
        Some(m) => m,
        None => return json_error(404, &format!("namespace '{namespace}' not found")),
    };
    let ns_docs: u64 = manifest.segments.iter().map(|s| s.doc_count as u64).sum();
    json_ok(&serde_json::json!({
        "namespace": namespace,
        "documents": ns_docs,
        "segments": manifest.segments.len(),
        "version": manifest.version,
    }))
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
/// Merged output size is capped (default 5GiB, Lucene-style): `full` under a
/// cap merges one greedy smallest-first group per pass — loop the endpoint
/// until `segments_after` stops dropping to converge on
/// ~ceil(total_bytes/cap) segments. `max_segment_bytes: 0` restores the
/// legacy all-to-one merge.
///
/// `"async": true` answers `202` with a `job_id` at once and runs the pass
/// on a background thread; poll `GET /v1/admin/compaction-jobs/{job_id}`. A
/// `full` pass hydrates every segment first and can run for hours on a big
/// namespace, far past the query→ingest proxy and HTTP timeouts. Without
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
    kosha_write::CompactOptions { mode, policy }
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
    let outcome = compaction_pass(state, ns, opts);
    let version = state.indexer.manifest_version(ns).unwrap_or(0);
    let elapsed_ms = started.elapsed().as_millis();
    match &outcome {
        Ok(o) => println!(
            "compaction: job={job_id} ns={} mode={mode} merged={} segments {}→{} \
             (merged {}) not_hydrated={} in {elapsed_ms} ms",
            ns.0,
            o.merged,
            o.segments_before,
            o.segments_after,
            o.segments_merged,
            o.not_hydrated.len(),
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

/// One compaction pass plus publication: hydrate what the pass may read,
/// merge, publish.
///
/// A `full` pass hydrates every segment (it may merge any of them). A
/// `tiered` pass hydrates only its possible inputs, the segments under
/// `max_mergeable_docs`, so the scheduler never pulls a whole namespace
/// (22 GiB on staging) to merge a few small segments. Segments still not
/// local afterwards are skipped by the merge, left unmerged, and reported
/// in `not_hydrated`.
fn compaction_pass(
    state: &AppState,
    ns: &NamespaceId,
    opts: kosha_write::CompactOptions,
) -> Result<compaction_sched::PassOutcome, String> {
    let manifest = state
        .indexer
        .manifest_cloned(ns)
        .ok_or_else(|| format!("namespace '{}' not found", ns.0))?;
    let segments_before = manifest.segments.len();
    let candidate_paths: Vec<PathBuf> = compaction_hydration_set(&manifest, &opts)
        .into_iter()
        .map(|id| state.data_dir.join(&ns.0).join(&id.0))
        .collect();
    drop(manifest);
    // Hydrate every candidate in one fanned-out batch rather than one
    // ensure_segment_local round trip per segment — see handle_index.
    // Sequential round trips were the bottleneck behind the old synchronous
    // endpoint's timeouts: 441 of them before compaction could start.
    #[cfg(feature = "s3")]
    // Compaction rewrites segments, so it must preserve vectors.
    state.ensure_segments_local(&candidate_paths, true);
    let not_hydrated: Vec<String> = candidate_paths
        .iter()
        .filter(|p| !p.exists())
        .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .collect();

    let max_segment_bytes = opts.policy.max_merged_segment_bytes;
    let result = state
        .indexer
        .compact_namespace_with_options(ns, opts)
        .map_err(|e| format!("compaction error: {e}"))?;

    // Persist the merged manifest and upload the new segment to S3, the
    // same path a normal flush uses.
    if result.merged {
        state
            .publish_namespace(ns)
            .map_err(|e| format!("publication failed: {e}"))?;
    }

    let segments_after = state
        .indexer
        .manifest_cloned(ns)
        .map(|m| m.segments.len())
        .unwrap_or(0);
    Ok(compaction_sched::PassOutcome {
        merged: result.merged,
        segments_before,
        segments_after,
        segments_merged: result.segments_merged,
        max_segment_bytes,
        not_hydrated,
    })
}

/// Segments a pass may read, so must be local: all of them for `full`,
/// only those under `max_mergeable_docs` (possible tiered inputs) for
/// `tiered`. Same eligibility rule as `select_merge_inputs`.
fn compaction_hydration_set(
    manifest: &kosha_core::Manifest,
    opts: &kosha_write::CompactOptions,
) -> Vec<kosha_core::SegmentId> {
    manifest
        .segments
        .iter()
        .filter(|entry| {
            opts.mode == kosha_write::CompactMode::Full
                || entry.doc_count < opts.policy.max_mergeable_docs
        })
        .map(|entry| entry.segment_id.clone())
        .collect()
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
            if let Some(m) = state.indexer.manifest_cloned(&ns) {
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
        state.indexer.restore_manifest(ns.clone(), manifest);
        {
            let mut ctrl = state.controller.lock().unwrap();
            ctrl.ensure_namespace(ns.clone());
        }
        if let Err(e) = state.persist_manifest(&ns) {
            return json_error(500, &format!("publication failed: {e}"));
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

#[cfg(feature = "s3")]
fn posting_blob_files_for_query<'a>(
    query: &SearchQuery,
    analysis: &kosha_query::QueryAnalysis,
    analyses: impl IntoIterator<Item = Option<&'a kosha_core::SegmentAnalysis>>,
) -> Vec<String> {
    if query.wildcard.is_some() {
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
         open_total_ms={:.1} knn_postings={} knn_degraded={} hits={}",
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
        knn_degraded_segments.unwrap_or(0),
        total_hits,
    );
}

/// Map a search failure onto the right HTTP status: load-shedding
/// (`KoshaError::Overloaded`, see `kosha_query::MemoryLedger`) is a
/// transient 429 the client should retry with backoff, not a 500.
fn search_error_response(e: &KoshaError) -> String {
    match e {
        KoshaError::Overloaded(_) => json_error(429, &format!("search overloaded: {e}")),
        KoshaError::InvalidQuery(_) => json_error(400, &e.to_string()),
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
        let shared = SharedManifestStore {
            inner: Arc::new(Mutex::new(kosha_control::Controller::new())),
            lose_next_commit_reply: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        };
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

    #[test]
    fn competing_writers_cannot_overwrite_a_published_manifest() {
        let shared = SharedManifestStore {
            inner: Arc::new(Mutex::new(kosha_control::Controller::new())),
            lose_next_commit_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let ns = NamespaceId("competing".into());
        shared.inner.lock().unwrap().ensure_namespace(ns.clone());
        let first = test_state();
        let second = test_state();
        for (state, id) in [(&first, "first"), (&second, "second")] {
            *state.controller.lock().unwrap() = Box::new(shared.clone());
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
        }
        let barrier = std::sync::Barrier::new(2);
        let outcomes = std::thread::scope(|scope| {
            let a = scope.spawn(|| {
                first.publish_namespace_with(&ns, |_| {
                    barrier.wait();
                    Ok(())
                })
            });
            let b = scope.spawn(|| {
                second.publish_namespace_with(&ns, |_| {
                    barrier.wait();
                    Ok(())
                })
            });
            (a.join().unwrap(), b.join().unwrap())
        });
        assert_eq!(
            usize::from(outcomes.0.is_ok()) + usize::from(outcomes.1.is_ok()),
            1,
            "exactly one independent writer may publish the initial version"
        );
        let (winner, loser) = if outcomes.0.is_ok() {
            (&first, &second)
        } else {
            (&second, &first)
        };
        let committed = shared.manifest_cloned(&ns).unwrap();
        assert_eq!(
            committed.segments[0].segment_id,
            winner.indexer.manifest(&ns).unwrap().segments[0].segment_id
        );
        let rejected_doc = if outcomes.0.is_ok() {
            "second"
        } else {
            "first"
        };
        let restarted_loser = Indexer::new(loser.data_dir.clone());
        restarted_loser.restore_manifest(ns.clone(), committed.clone());
        assert!(
            restarted_loser
                .existing_ids(&ns, &[DocumentId(rejected_doc.into())])
                .unwrap()
                .contains(&DocumentId(rejected_doc.into())),
            "the winner's WAL checkpoint must not discard the loser's unrelated recovery records"
        );
        let retry = handle_flush(br#"{"namespace":"competing"}"#, loser);
        assert!(
            retry.starts_with("HTTP/1.1 500"),
            "stale writer must still fail: {retry}"
        );
        assert!(
            !kosha_write::wal::WalWriter::recover(&loser.data_dir.join("_wal"))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            shared.manifest_cloned(&ns).unwrap().segments[0].segment_id,
            committed.segments[0].segment_id
        );
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
        let search = br#"{"namespace":"tenant/ns","query_text":"hello"}"#;
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
            analysis: None,
            field_index: None,
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

        assert!(
            !AppState::segment_is_scoring_complete(&dir, false, false, false),
            "empty dir must not be scoring-complete"
        );

        for f in ["inverted.idx", "doc_store.offsets"] {
            fs::write(dir.join(f), b"x").unwrap();
        }
        assert!(
            !AppState::segment_is_scoring_complete(&dir, false, false, false),
            "without a manifest footer, footer.json on disk is still required"
        );
        assert!(
            AppState::segment_is_scoring_complete(&dir, false, false, true),
            "a manifest footer snapshot must stand in for a missing footer.json"
        );
        fs::write(dir.join("footer.json"), b"x").unwrap();
        assert!(
            AppState::segment_is_scoring_complete(&dir, false, false, false),
            "offsets sidecar (no doc_store.bin, no filters.bin) must be broad lexical scoring-complete"
        );
        assert!(
            !AppState::segment_is_scoring_complete(&dir, false, true, false),
            "queries using filters/sorts/aggs must still require filters.bin"
        );
        fs::write(dir.join("filters.bin"), b"x").unwrap();
        assert!(
            AppState::segment_is_scoring_complete(&dir, false, true, false),
            "filters.bin present must make filter/sort/agg queries scoring-complete"
        );
        assert!(
            !AppState::segment_is_scoring_complete(&dir, true, true, false),
            "kNN scoring-complete must require vector.idx (still missing)"
        );

        fs::write(dir.join("vector.idx"), b"x").unwrap();
        assert!(
            AppState::segment_is_scoring_complete(&dir, true, true, false),
            "vector.idx present must make it kNN scoring-complete"
        );
        assert!(
            !dir.join("vector.offsets").exists(),
            "sanity: this assertion must be exercising the no-sidecar case"
        );
        assert!(
            AppState::segment_is_scoring_complete(&dir, true, true, false),
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
            AppState::segment_is_scoring_complete(&legacy, false, true, false),
            "legacy segment with doc_store.bin (no offsets) must be scoring-complete"
        );
        fs::remove_file(legacy.join("doc_store.bin")).unwrap();
        assert!(
            !AppState::segment_is_scoring_complete(&legacy, false, true, false),
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
            match_phrase: None,
            knn: None,
            exact_total_hits: None,
            total_hits_cap: None,
            operator: None,
            no_cache: None,
            collapse: None,
            limit_per: None,
            source_includes: None,
            source_excludes: None,
            rank: None,
            mode: None,
            fields: Vec::new(),
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
    // query (unfiltered, unsorted, non-kNN — see `run_warmup`) or an
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
        let mut v = HydrationVerdict::default();
        assert!(
            v.covers(false, false),
            "a query needing neither file class is trivially covered by any verdict"
        );
        assert!(
            !v.covers(true, false),
            "an unverified verdict must not cover a query that needs filters.bin"
        );
        assert!(
            !v.covers(false, true),
            "an unverified verdict must not cover a query that needs vector.idx"
        );

        // A pass that itself needed only filters.bin verifies only that.
        v.merge(true, false);
        assert!(v.covers(false, false));
        assert!(v.covers(true, false));
        assert!(
            !v.covers(false, true),
            "verifying filters.bin must not also mark vector.idx as verified"
        );
        assert!(!v.covers(true, true));

        // A later pass needing vector.idx merges in, not overwrites.
        v.merge(false, true);
        assert!(
            v.covers(true, true),
            "flags accumulate across passes — filters_verified from an earlier pass must \
             survive a later pass that only verified vectors"
        );
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
            match_phrase: None,
            knn: None,
            exact_total_hits: None,
            total_hits_cap: None,
            operator: None,
            no_cache: None,
            collapse: None,
            limit_per: None,
            source_includes: None,
            source_excludes: None,
            rank: None,
            mode: None,
            fields: Vec::new(),
        };
        let verdict_key = (ns.0.clone(), manifest.version);

        // Step 1: a plain unfiltered/unsorted/non-kNN pass — exactly
        // warmup's own probe query shape — succeeds and (per the existing
        // `!bloom_pruned` gate) is eligible to merge into the cache.
        let warmup_like = base_query.clone();
        let needs_filters_1 = query_needs_filter_store(&warmup_like);
        let needs_vectors_1 = warmup_like.knn.is_some();
        assert!(
            !needs_filters_1 && !needs_vectors_1,
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
            .merge(needs_filters_1, needs_vectors_1);

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
        let needs_filters_2 = query_needs_filter_store(&chunk_lookup);
        let needs_vectors_2 = chunk_lookup.knn.is_some();
        assert!(
            needs_filters_2,
            "a documentId term filter needs filters.bin"
        );

        let cached = state
            .completed_hydrations
            .lock()
            .unwrap()
            .get(&verdict_key)
            .copied();
        let cached_covers = cached.is_some_and(|v| v.covers(needs_filters_2, needs_vectors_2));
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
        let needs_filters_3 = query_needs_filter_store(&sorted_unfiltered);
        let needs_vectors_3 = sorted_unfiltered.knn.is_some();
        assert!(needs_filters_3, "sorting by a real field needs filters.bin");
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
            .merge(needs_filters_3, needs_vectors_3);

        let cached_after = state
            .completed_hydrations
            .lock()
            .unwrap()
            .get(&verdict_key)
            .copied();
        assert!(
            cached_after.is_some_and(|v| v.covers(true, false)),
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

    /// The tiered pass a scheduler runs must not hydrate segments it can
    /// never merge (doc_count >= max_mergeable_docs): those are the large
    /// ones, and pulling them is what made the old endpoint hydrate 22 GiB.
    #[test]
    fn tiered_pass_hydrates_only_mergeable_segments() {
        let entry = |id: &str, docs: u32| kosha_core::ManifestEntry {
            segment_id: kosha_core::SegmentId(id.into()),
            doc_count: docs,
        };
        let manifest = kosha_core::Manifest {
            version: 3,
            segments: vec![
                entry("small", 1_000),
                entry("big", 2_000_000),
                entry("edge", 50_000),
            ],
            segment_footers: Default::default(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            schema: None,
        };
        let policy = kosha_write::CompactionPolicy::default();
        assert_eq!(policy.max_mergeable_docs, 50_000);
        let ids = |opts: &kosha_write::CompactOptions| -> Vec<String> {
            compaction_hydration_set(&manifest, opts)
                .into_iter()
                .map(|id| id.0)
                .collect()
        };
        assert_eq!(
            ids(&kosha_write::CompactOptions::tiered(policy)),
            vec!["small"]
        );
        assert_eq!(
            ids(&kosha_write::CompactOptions::full()),
            vec!["small", "big", "edge"]
        );
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
        AppState::new(dir)
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
            match_phrase: None,
            knn: None,
            exact_total_hits: None,
            total_hits_cap: None,
            operator: None,
            no_cache: None,
            collapse: None,
            limit_per: None,
            source_includes: None,
            source_excludes: None,
            rank: None,
            mode: None,
            fields: Vec::new(),
        };
        let with_vec = |v: Vec<f32>, k: usize| {
            let mut q = base.clone();
            q.knn = Some(kosha_core::KnnQuery {
                field: "text_emb".into(),
                vector: v,
                k,
                num_candidates: 100,
                filter: None,
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
            analysis: None,
            field_index: None,
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
}
