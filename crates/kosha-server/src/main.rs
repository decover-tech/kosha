//! Kosha node binary.
//!
//! One binary, four roles (`KOSHA_ROLE`, see `role.rs` and DESIGN.md §5):
//! `query`, `ingest`, `indexer` and `standalone`.
//! Phase 1 HTTP/JSON API:
//!   - `GET  /healthz`           → 200 OK (liveness probe)
//!   - `GET  /metrics`           → Prometheus text format (docs/metrics.md)
//!   - `POST /index              ` → upsert documents by id into a namespace
//!   - `POST /exists             ` → batch doc_id existence check
//!   - `POST /replace            ` → partial field merge by id (OpenSearch `_update`)
//!   - `GET  /search             ` → BM25 search across a namespace

use std::collections::HashMap;
use std::net::TcpListener;
#[cfg(feature = "s3")]
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

/// Extract the tenant prefix from a namespace for isolation.
fn tenant_namespace(tenant: &str, namespace: &str) -> String {
    format!("{tenant}/{namespace}")
}

/// The tenant owning internal namespace `ns` (`{tenant}/...`), or `None`
/// for a pre-#262 bare namespace.
fn namespace_tenant(ns: &str) -> Option<&str> {
    ns.split_once('/').map(|(tenant, _)| tenant)
}

/// The tenant `dispatch` assigns to the admin and internal service
/// credentials. Operator traffic is never tenant-limited (#341).
const SERVICE_TENANT: &str = "service";

mod auth;
mod branching;
mod cache_reads;
mod compaction_sched;
mod dashboard;
mod doc_spans;
#[cfg(all(test, feature = "s3"))]
mod doc_store_fallback_tests;
mod embedding;
#[cfg(test)]
mod embedding_server_tests;
mod fair_admission;
mod http;
#[cfg(feature = "s3")]
mod hydration_budget;
mod internal_tls;
mod manifest_follow;
mod metrics;
#[cfg(feature = "migrate")]
mod migrate;
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
mod namespace_migrate;
mod posting_spans;
mod rerank;
mod role;
#[cfg(feature = "s3")]
mod s3_storage;
#[cfg(feature = "s3")]
mod s3_wal;
// The crash-point harness uploads to an in-memory bucket through the same
// commit-marker sequence as S3, so it needs this module without `s3`.
#[cfg(all(test, feature = "failpoints"))]
mod crash_points_tests;
#[cfg(any(feature = "s3", all(test, feature = "failpoints")))]
#[cfg_attr(not(feature = "s3"), allow(dead_code))]
mod segment_commit;
mod segment_gc;
mod tenant_limits;
mod unindexed;
mod warm;
mod warm_run;
mod writer_lease;
#[cfg(test)]
mod writer_lease_tests;

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
    /// `KOSHA_ROLE`, resolved and validated (#254, `role.rs`).
    role: role::Role,
    /// Writer leases this process holds and namespaces it was fenced from
    /// (#254). Disabled on query and indexer pods and with
    /// `KOSHA_WRITER_LEASE=off`.
    writer_leases: writer_lease::WriterLeases,
    /// `KOSHA_COMPACTION_ON_INGEST` (#254): whether a writer pod also runs
    /// the compaction scheduler.
    compaction_on_ingest: bool,
    admin_auth_key: Option<String>,
    internal_service_key: Option<String>,
    publication_locks: Mutex<HashMap<NamespaceId, Arc<Mutex<()>>>>,
    /// Serializes namespace branch creation and deletion (#208), so a
    /// deletion's reference set cannot miss a branch being created from
    /// the namespace it deletes.
    branch_lock: Mutex<()>,
    /// Prometheus metrics served at `GET /metrics` (#223).
    metrics: metrics::Metrics,
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
    ///
    /// Waiters queue per tenant and are served round robin (#341, see
    /// [`fair_admission`]), so one tenant's burst cannot queue everyone.
    search_semaphore: fair_admission::FairSemaphore,
    /// Per-tenant quotas and rate limits (#341, `KOSHA_TENANT_LIMITS_FILE`).
    /// Inert when no limits file is configured.
    tenant_limits: tenant_limits::TenantLimiter,
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
    /// `http` or `https` for peer segment fetches (`KOSHA_INTERNAL_TLS`,
    /// see `internal_tls`).
    #[cfg(all(feature = "s3", feature = "postgres"))]
    peer_url_scheme: &'static str,
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
    /// Native embedding providers (#209): embed text sent for a vector
    /// field whose schema declares a model, and `knn.query_text`.
    embedders: embedding::EmbedderRegistry,
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
    /// Merged read views of sharded namespaces (#207), keyed by root: the
    /// root and shard manifests a view was built from, and the view. Reused
    /// while every one of those is still the indexer's current shared
    /// manifest — see [`AppState::sharded_view`].
    shard_views: Mutex<HashMap<NamespaceId, ShardView>>,
    /// `KOSHA_DEFAULT_CONSISTENCY` (#255); `None` = the built-in default,
    /// see `AppState::default_consistency`.
    default_consistency: Option<unindexed::Consistency>,
    /// `KOSHA_MAX_UNINDEXED_BYTES` / `KOSHA_MAX_UNINDEXED_DOCS` (#255).
    unindexed_cap: unindexed::UnindexedCap,
    /// Strong-consistency search state: built tails, the tail HTTP client.
    unindexed: unindexed::UnindexedState,
    /// `KOSHA_DASHBOARD` and the per-namespace recent-query rings (#214).
    dashboard: dashboard::Dashboard,
}

/// One cached merged manifest over a sharded namespace (#207).
struct ShardView {
    root: Arc<kosha_core::Manifest>,
    shards: Vec<Option<Arc<kosha_core::Manifest>>>,
    merged: Arc<kosha_core::Manifest>,
}

impl ShardView {
    fn is_current(
        &self,
        root: &Arc<kosha_core::Manifest>,
        shards: &[Option<Arc<kosha_core::Manifest>>],
    ) -> bool {
        Arc::ptr_eq(&self.root, root)
            && self.shards.len() == shards.len()
            && self.shards.iter().zip(shards).all(|pair| match pair {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            })
    }
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

/// Minimal counting semaphore built on `Mutex`/`Condvar`, for S3
/// hydration: synchronous from the caller's point of view (it runs on the
/// request-handling thread), so a blocking semaphore matches the rest of the
/// file's concurrency model. (`std` has no built-in semaphore; pulling in
/// `tokio::sync::Semaphore` would mean running async code behind a blocking
/// boundary.) Search admission uses [`fair_admission::FairSemaphore`]
/// instead, which sheds and queues per tenant (#146, #341).
#[cfg_attr(not(feature = "s3"), allow(dead_code))]
struct Semaphore {
    permits: Mutex<usize>,
    cv: Condvar,
}

#[cfg_attr(not(feature = "s3"), allow(dead_code))]
impl Semaphore {
    fn new(permits: usize) -> Self {
        Self {
            permits: Mutex::new(permits),
            cv: Condvar::new(),
        }
    }

    /// Block until a permit is available, then hold it until the returned
    /// guard is dropped. Unbounded: for callers where waiting is always
    /// preferable to failing.
    fn acquire(&self) -> SemaphorePermit<'_> {
        let mut permits = self.permits.lock().unwrap();
        while *permits == 0 {
            permits = self.cv.wait(permits).unwrap();
        }
        *permits -= 1;
        SemaphorePermit { sem: self }
    }
}

struct SemaphorePermit<'a> {
    sem: &'a Semaphore,
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        *self.sem.permits.lock().unwrap() += 1;
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
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
    bloom_pruned: bool,
    /// `true` when this request hit its hydration byte ceiling
    /// (`KOSHA_HYDRATE_REQUEST_MAX_BYTES`) or waited out the process-wide
    /// in-flight byte budget, and stopped fetching on purpose.
    ///
    /// `missing` is populated either way — the files really aren't local —
    /// but the distinction matters to the client: a shed is load-shedding
    /// (HTTP 429, retry with backoff, the same contract
    /// `FairSemaphore::try_acquire` and `kosha_query::MemoryLedger` already
    /// use), whereas a non-shed `missing` is a genuine S3 failure (HTTP
    /// 503). Reporting a shed as a 503 would tell a caller its data is
    /// broken when the server merely declined to pull multiple GB inside
    /// one request.
    shed: bool,
    /// Why `shed` is set, for the 429's log line.
    shed_reason: Option<String>,
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

impl HydrationOutcome {
    /// Mark the outcome shed (never unmark) if `request` was, with why.
    fn set_shed(&mut self, request: &RequestHydration) {
        if request.is_shed() {
            self.shed = true;
            self.shed_reason = Some(request.shed_reason());
        }
    }
}

impl HydrationOutcome {
    /// Sequential S3 round trips: pre-scoring (scaffolding + postings),
    /// kNN postings and the page's documents.
    fn s3_rounds(&self) -> u64 {
        self.rounds + self.vectors.rounds + self.page.rounds
    }

    /// Bytes pulled from S3: whole scaffolding/postings files, kNN posting
    /// ranges and page documents (ranged or whole).
    fn s3_bytes(&self) -> u64 {
        self.bytes_fetched
            + self.vectors.ranged_bytes
            + self.vectors.whole_bytes
            + self.page.ranged_bytes
            + self.page.whole_bytes
    }

    /// Nothing this search read came from S3, and nothing it needed was
    /// missing or shed: the namespace is warm for this query.
    ///
    /// Every fetch path lands in one of the counters checked here: whole
    /// scaffolding, footer, filter and postings files in `files_fetched` /
    /// `bytes_fetched` (ranged postings bytes too, with their GETs in
    /// `postings.ranged_gets`), kNN `vector.idx` ranges or whole files in
    /// `vectors`, and the page's doc-store spans or whole files in `page`.
    /// A GET that was shed still counts in its `ranged_gets`, and any fetch
    /// round counts in a `rounds`, so a fetch that failed or was declined
    /// also reads as not cached. Manifest-following reads on a query
    /// replica (#232) are metadata, not this search's data, and are not
    /// counted.
    fn fully_cached(&self) -> bool {
        self.missing.is_empty()
            && !self.shed
            && self.files_fetched == 0
            && self.s3_rounds() == 0
            && self.s3_bytes() == 0
            && self.postings.ranged_gets == 0
            && self.postings.whole_files == 0
            && self.vectors.ranged_gets == 0
            && self.vectors.whole_files == 0
            && self.page.ranged_gets == 0
            && self.page.whole_files == 0
    }
}

/// `fully_cached` for a search's timings: nothing came from S3
/// ([`HydrationOutcome::fully_cached`]) and the search did not score around
/// anything absent — no kNN segment degraded for a missing `vector.idx`
/// posting, and no fall back to per-segment BM25 statistics because some
/// footer or postings could not be read. A gate waiting for this (the
/// bench's hot-phase warmup, like turbopuffer's `cache_hit_ratio == 1.0`)
/// must not accept a degraded answer as warm.
fn search_fully_cached(
    hydration: &HydrationOutcome,
    phases: &kosha_query::SearchPhaseStats,
    knn_degraded_segments: u32,
) -> bool {
    hydration.fully_cached()
        && knn_degraded_segments == 0
        && phases.bm25_stats != kosha_query::Bm25StatsUsed::SegmentFallback
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
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
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
/// vectors in `doc_vectors.bin` also needs that file's header, whose field
/// table binds its unnamed index — the whole file or just the header span
/// in the span cache.
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
    !knn_wants_doc_vectors_header(footer, field) || doc_spans::doc_vectors_header_local(seg_path)
}

/// `KOSHA_KNN_GLOBAL_RESCORE` (#381), read once for the hydration
/// predicates (the searcher reads the same variable).
#[cfg(feature = "s3")]
fn knn_global_rescore_on() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(kosha_query::knn_global_rescore_from_env)
}

/// Whether a kNN on `field` needs `doc_vectors.bin`'s header local in a
/// segment with this footer: a legacy segment's always (its field table
/// binds the unnamed index), and, with global rescoring on (#381), a
/// quantized column's, because the pass reads exact vectors by range and
/// a range needs the header to locate it. Fetching it here, with the
/// scoring files, makes it part of a fully cached query, rather than
/// depending on whether an earlier query happened to put this segment on
/// a page. Off by default: the predicate is then exactly the legacy one.
#[cfg(feature = "s3")]
fn knn_wants_doc_vectors_header(footer: Option<&kosha_core::Footer>, field: &str) -> bool {
    knn_wants_doc_vectors_header_with(footer, field, knn_global_rescore_on())
}

/// [`knn_wants_doc_vectors_header`] with the flag passed in, for tests.
#[cfg(feature = "s3")]
fn knn_wants_doc_vectors_header_with(
    footer: Option<&kosha_core::Footer>,
    field: &str,
    global_rescore: bool,
) -> bool {
    kosha_segment::knn_needs_doc_vectors(footer)
        || (global_rescore
            && footer.is_some_and(|f| {
                f.doc_vectors
                    && f.vector_fields.as_ref().is_some_and(|cols| {
                        cols.iter()
                            .any(|c| c.name == field && !c.quantization.is_f32())
                    })
            }))
}

/// The vector key a query's [`HydrationVerdict`] records and checks: its
/// `knn.field` for a single-vector kNN (#204), and a key of its own for a
/// late-interaction kNN (#205), whose pass hydrates the field's multivec
/// files rather than its single-vector column. `None` without kNN.
#[cfg_attr(not(feature = "s3"), allow(dead_code))]
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
        // A branch's inherited segment is durable by construction: it was
        // listed by a published parent manifest (#208).
        .filter(|e| !manifest.is_inherited_segment(ns, &e.segment_id))
        .filter(|e| !synced.contains(&synced_key(manifest, ns, &e.segment_id)))
        .collect()
}

/// `synced_segments` key: the segment's *home* namespace and id, so a
/// branch and its parent share one durability record per object (#208).
#[cfg(feature = "s3")]
fn synced_key(
    manifest: &kosha_core::Manifest,
    ns: &NamespaceId,
    segment: &kosha_core::SegmentId,
) -> (NamespaceId, kosha_core::SegmentId) {
    (manifest.segment_home(ns, segment).clone(), segment.clone())
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
    let status = match error {
        KoshaError::ManifestConflict { .. } => 409,
        // Another writer owns the namespace (#254): retry, by when its
        // lease would have lapsed, against whichever pod owns it now.
        KoshaError::Fenced { .. } => {
            let ttl =
                writer_lease::ttl_from(std::env::var("KOSHA_WRITER_LEASE_TTL_MS").ok().as_deref());
            return retry_after(
                json_error(503, &format!("{context}: {error}")),
                ttl.as_secs().max(1),
            );
        }
        _ => 500,
    };
    json_error(status, &format!("{context}: {error}"))
}

/// Add a `Retry-After` header to a ready-made response, right after its
/// status line.
fn retry_after(response: String, secs: u64) -> String {
    response.replacen("\r\n", &format!("\r\nretry-after: {secs}\r\n"), 1)
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
    /// Role instead of `KOSHA_ROLE`.
    role: Option<role::Role>,
    /// Writer lease TTL instead of `KOSHA_WRITER_LEASE_TTL_MS`.
    writer_lease_ttl: Option<Duration>,
    /// Writer leases on or off instead of `KOSHA_WRITER_LEASE` (still off
    /// on a role that accepts no writes).
    writer_lease: Option<bool>,
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
    role: role::Role,
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
            if role == role::Role::Query {
                println!("WAL: writes are forwarded to ingest; KOSHA_WAL_MODE=s3 applies there");
                return None;
            }
            // An indexer accepts no writes. Opening the S3 WAL would recover
            // (and could flush) the ingest writer's records if both shared
            // a KOSHA_WAL_WRITER_ID, so it keeps an empty local WAL.
            if role == role::Role::Indexer {
                println!("WAL: indexer role accepts no writes; KOSHA_WAL_MODE=s3 ignored");
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
        let role = options.role.unwrap_or_else(|| {
            role::resolve_role(&role::RoleEnv::from_process())
                .unwrap_or_else(|e| fatal(&e))
                .role
        });
        if let Err(e) = std::fs::create_dir_all(&data_dir) {
            eprintln!("ERROR: cannot create {}: {e}", data_dir.display());
        }
        // Before the disk cache scans `data_dir`: transient tail segments of
        // an earlier process are never reused (#255).
        unindexed::remove_stale_tail_segments(&data_dir);
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
        // `KOSHA_CACHE_POLICY` (#389, research): opt-in scan-resistant
        // eviction; unset keeps the legacy order.
        let cache = Cache::with_options(
            data_dir.clone(),
            cache_reads::cache_options_from_env(cache_max_bytes),
        );
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
                role,
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
        #[cfg_attr(not(feature = "postgres"), allow(unused_mut))]
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
        let is_ingest_role = role.accepts_writes();
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
                    // An indexer publishes compactions over manifests the
                    // ingest writer committed: their segments are durable
                    // by the publish protocol, and reconciling them is the
                    // ingest pod's job.
                    #[cfg(feature = "s3")]
                    if role == role::Role::Indexer {
                        for entry in &manifest.segments {
                            synced_segments.insert(synced_key(&manifest, &ns, &entry.segment_id));
                        }
                    }
                    #[cfg(feature = "s3")]
                    if is_ingest_role {
                        if let Some(ref s3) = s3_storage {
                            for entry in &manifest.segments {
                                // A branch's inherited segment is checked
                                // (and keyed) where it lives (#208).
                                let rel = manifest.segment_rel_dir(&ns, &entry.segment_id);
                                let key = synced_key(&manifest, &ns, &entry.segment_id);
                                let durability = s3.segment_durability(&rel);
                                if durability.is_durable() {
                                    synced_segments.insert(key);
                                    continue;
                                }
                                let seg_path =
                                    manifest.segment_dir(&data_dir, &ns, &entry.segment_id);
                                match sync_segment_dir_to_s3(s3, &data_dir, &seg_path) {
                                    Ok(()) => {
                                        synced_segments.insert(key);
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

        // The forwarding target. `resolve_role` guarantees it is set
        // exactly when the role is `query`.
        let ingest_host = std::env::var("KOSHA_INGEST_HOST")
            .ok()
            .filter(|h| !h.is_empty() && role == role::Role::Query);
        let query_role = role == role::Role::Query;

        // ── Writer lease (#254) ───────────────────────────────────────────
        let lease_ttl = options.writer_lease_ttl.unwrap_or_else(|| {
            writer_lease::ttl_from(std::env::var("KOSHA_WRITER_LEASE_TTL_MS").ok().as_deref())
        });
        let lease_enabled = role.accepts_writes()
            && options.writer_lease.unwrap_or_else(|| {
                writer_lease::enabled_from(std::env::var("KOSHA_WRITER_LEASE").ok().as_deref())
            });
        #[cfg_attr(not(feature = "postgres"), allow(unused_mut))]
        let mut lease_backend = writer_lease::LeaseBackend::ControlStore;
        #[cfg(feature = "postgres")]
        if lease_enabled && control_plane_kind == "postgres" {
            if let Ok(db_url) = std::env::var("DATABASE_URL") {
                match kosha_control::PgWriterLeaseStore::new(&db_url) {
                    Ok(store) => lease_backend = writer_lease::LeaseBackend::Postgres(store),
                    Err(e) => fatal(&format!("cannot connect the writer lease pool: {e}")),
                }
            }
        }
        let writer_leases =
            writer_lease::WriterLeases::new(lease_enabled, lease_ttl, lease_backend);
        if lease_enabled {
            println!(
                "writer lease: on (holder {}, ttl {} ms)",
                writer_leases.holder(),
                lease_ttl.as_millis()
            );
        } else if role.accepts_writes() {
            println!("writer lease: off (KOSHA_WRITER_LEASE=off); manifest CAS is unfenced");
        }
        let compaction_on_ingest =
            role::compaction_on_ingest(std::env::var("KOSHA_COMPACTION_ON_INGEST").ok().as_deref());
        // TLS between pods (#343). Fails closed on a bad configuration.
        let internal_tls = internal_tls::global();
        println!("internal TLS: {}", internal_tls.describe());
        let write_http_client = ingest_host.as_ref().map(|h| {
            match internal_tls.check_ingest_host(h) {
                Ok(None) => {}
                Ok(Some(warning)) => eprintln!("WARN: {warning}"),
                Err(e) => fatal(&e),
            }
            println!("write-path requests will be forwarded to ingest at {h}");
            internal_tls
                .client_builder()
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
            internal_tls
                .client_builder()
                // Short: a peer that isn't answering quickly should be
                // abandoned in favor of the S3 fallback, not held onto —
                // see `fetch_from_peer`. The blocking client applies it to
                // each read, not the whole transfer, so a large streamed
                // file is not cut off.
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

        let tenant_limits = tenant_limits::TenantLimiter::from_env()
            .unwrap_or_else(|e| fatal(&format!("tenant limits: {e}")));
        // Every process-wide cache splits its budget between tenants by
        // their plan weight, with `KOSHA_CACHE_TENANT_MIN_BYTES` as each
        // active tenant's floor (#342).
        let tenant_shares =
            kosha_core::tenancy::TenantShares::from_env().with_weights(tenant_limits.weight_fn());
        cache.set_tenant_shares(tenant_shares.clone());
        searcher.set_tenant_shares(tenant_shares.clone());
        let mut result_cache = ResultCache::new(result_cache_budget());
        result_cache.shares = tenant_shares;

        Self {
            controller: Mutex::new(control_store),
            role,
            writer_leases,
            compaction_on_ingest,
            admin_auth_key: std::env::var("KOSHA_ADMIN_API_KEY")
                .ok()
                .filter(|k| !k.is_empty()),
            internal_service_key: std::env::var("KOSHA_INTERNAL_API_KEY")
                .ok()
                .filter(|k| !k.is_empty()),
            publication_locks: Mutex::new(HashMap::new()),
            branch_lock: Mutex::new(()),
            metrics: metrics::Metrics::from_env(),
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
                // Per-tenant share of the queue (#341). `0`: only the total
                // bound applies.
                let per_tenant = std::env::var("KOSHA_SEARCH_QUEUE_DEPTH_PER_TENANT")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                fair_admission::FairSemaphore::new(
                    permits,
                    max_waiters,
                    per_tenant,
                    (wait_ms > 0).then(|| Duration::from_millis(wait_ms)),
                )
            },
            tenant_limits,
            #[cfg(all(feature = "s3", feature = "postgres"))]
            hydration_leases,
            #[cfg(all(feature = "s3", feature = "postgres"))]
            own_addr,
            #[cfg(all(feature = "s3", feature = "postgres"))]
            peer_http_client,
            #[cfg(all(feature = "s3", feature = "postgres"))]
            peer_url_scheme: internal_tls.peer_scheme(),
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
            result_cache: Mutex::new(result_cache),
            rerankers: rerank::RerankRegistry::from_env(),
            embedders: embedding::EmbedderRegistry::from_env(),
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
            shard_views: Mutex::new(HashMap::new()),
            default_consistency: unindexed::Consistency::from_env(),
            unindexed_cap: unindexed::UnindexedCap::from_env(),
            unindexed: unindexed::UnindexedState::new(query_role),
            dashboard: dashboard::Dashboard::from_env(),
        }
    }

    /// Construct inside an `Arc` with `self_ref` wired up — the constructor
    /// `main` uses so handlers can spawn background jobs that own the state.
    #[cfg_attr(not(test), allow(dead_code))]
    fn new_shared(data_dir: PathBuf) -> Arc<Self> {
        Arc::new_cyclic(|weak| {
            let mut state = Self::new(data_dir);
            state.self_ref = weak.clone();
            state
        })
    }

    /// [`Self::new_shared`] with the role `main` already resolved.
    fn new_shared_as(data_dir: PathBuf, role: role::Role) -> Arc<Self> {
        Arc::new_cyclic(|weak| {
            let mut state = Self::with_options(
                data_dir,
                StateOptions {
                    role: Some(role),
                    ..Default::default()
                },
            );
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
        if !self.role.accepts_writes() {
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
    /// `compaction_sched`. Runs on `indexer` and `standalone` pods, and on
    /// `ingest` unless `KOSHA_COMPACTION_ON_INGEST=false` (#254); query
    /// pods own no segments and never start it.
    fn spawn_compaction_scheduler(self: &Arc<Self>) {
        if !self.runs_compaction_scheduler() {
            if self.role == role::Role::Ingest {
                println!(
                    "compaction scheduler: off on ingest (KOSHA_COMPACTION_ON_INGEST=false); \
                     an indexer pod compacts"
                );
            }
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

    /// Whether this pod runs the background compaction scheduler (#254).
    fn runs_compaction_scheduler(&self) -> bool {
        self.role
            .runs_compaction_scheduler(self.compaction_on_ingest)
    }

    /// Renew this writer's leases every TTL/3 (#254). A lease another
    /// writer took over fences its namespace (see [`Self::fence_namespace`]).
    fn spawn_writer_lease_renewer(self: &Arc<Self>) {
        if !self.writer_leases.enabled() {
            return;
        }
        let every = (self.writer_leases.ttl() / 3).max(Duration::from_millis(100));
        let weak = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("kosha-writer-lease".into())
            .spawn(move || loop {
                std::thread::sleep(every);
                let Some(state) = weak.upgrade() else {
                    return;
                };
                state.renew_writer_leases();
            })
            .expect("failed to spawn writer lease thread");
    }

    /// One renewal pass over every held writer lease.
    fn renew_writer_leases(&self) {
        for ns in self.writer_leases.renew_all(&self.controller) {
            let lock = self.publication_lock(&ns);
            let _guard = lock.lock().unwrap();
            self.fence_namespace(&ns, "its writer lease was taken over");
        }
    }

    /// The fencing token this writer presents for `ns` (#254), taking the
    /// lease if it holds none. `Ok(None)` when leases are off. A refused
    /// acquire of a lease this writer held before means another writer
    /// took it over: the namespace is fenced.
    fn writer_token(&self, ns: &NamespaceId) -> Result<Option<u64>, KoshaError> {
        let held_before = self.writer_leases.held_before(ns);
        match self.writer_leases.token(ns, &self.controller) {
            Err(fenced @ KoshaError::Fenced { .. }) => {
                if held_before {
                    let lock = self.publication_lock(ns);
                    let _guard = lock.lock().unwrap();
                    self.fence_namespace(ns, "another writer took its lease over");
                }
                Err(fenced)
            }
            other => other,
        }
    }

    /// Refuse a write to `ns` this writer does not own (#254): `Err` is a
    /// ready-to-send 503 + `Retry-After`. Takes the lease when it is free,
    /// so the first write to a namespace claims it.
    fn ensure_writable(&self, ns: &NamespaceId) -> Result<(), String> {
        match self.writer_token(ns) {
            Ok(_) => Ok(()),
            Err(error) => Err(self.write_refusal(&error)),
        }
    }

    /// 503 + `Retry-After` for a write this writer may not make.
    fn write_refusal(&self, error: &KoshaError) -> String {
        retry_after(
            json_error(503, &format!("write refused: {error}")),
            self.writer_leases.retry_after_secs(),
        )
    }

    /// Another writer owns `ns` (#254). Stop accepting writes for it and
    /// drop this writer's unpublished local state the way a query replica
    /// installs a manifest: forget the namespace, then restore the
    /// committed one. Writes this writer acknowledged but never published
    /// stay in its WAL (they are not replayed while the namespace's WAL
    /// checkpoint belongs to the other writer). The caller holds `ns`'s
    /// publication lock.
    fn fence_namespace(&self, ns: &NamespaceId, why: &str) {
        if self.writer_leases.lost(ns) {
            self.metrics.record_writer_fenced();
            eprintln!(
                "WARN: writer lease: ns={} fenced ({why}); refusing writes for it and dropping \
                 unpublished local state",
                ns.0
            );
        }
        self.indexer.forget_namespace(ns);
        self.forget_namespace_caches(ns);
        match self.hydrated_remote_manifest(ns) {
            Ok(Some(remote)) => {
                #[cfg(feature = "s3")]
                {
                    let mut synced = self.synced_segments.lock().unwrap();
                    for entry in &remote.segments {
                        synced.insert(synced_key(&remote, ns, &entry.segment_id));
                    }
                }
                self.indexer.restore_manifest(ns.clone(), remote);
            }
            Ok(None) => {}
            Err(error) => eprintln!(
                "WARN: writer lease: cannot reload ns={} after fencing: {error}; it reloads on \
                 the next successful write",
                ns.0
            ),
        }
    }

    /// Indexer role (#254): bring `ns` up to the committed manifest before
    /// planning a compaction. A namespace with nothing unpublished here
    /// installs the committed version the way a query replica does; one
    /// with an unpublished compaction keeps it, and its publish rebases.
    fn indexer_follow_namespace(&self, ns: &NamespaceId) -> Result<(), KoshaError> {
        let lock = self.publication_lock(ns);
        let _guard = lock.lock().unwrap();
        let clean = self
            .indexer
            .publication_snapshot(ns)
            .is_none_or(|(published, local)| published == local.version);
        let local = self.indexer.manifest_version(ns);
        let Some(remote) = self.hydrated_remote_manifest(ns)? else {
            if local.is_some() && clean {
                self.indexer.forget_namespace(ns);
                self.forget_namespace_caches(ns);
            }
            return Ok(());
        };
        if !clean || local.is_some_and(|v| v >= remote.version) {
            return Ok(());
        }
        #[cfg(feature = "s3")]
        {
            let mut synced = self.synced_segments.lock().unwrap();
            for entry in &remote.segments {
                synced.insert(synced_key(&remote, ns, &entry.segment_id));
            }
        }
        self.indexer.restore_manifest(ns.clone(), remote);
        Ok(())
    }

    /// Indexer role: learn every namespace the control store lists (and
    /// newer versions of the known ones), skipping namespaces a pass is
    /// compacting right now. Runs before each scheduler tick.
    fn indexer_follow_all(&self) {
        let versions = match self.controller.lock().unwrap().manifest_versions() {
            Ok(versions) => versions,
            Err(error) => {
                eprintln!("WARN: indexer: control store version poll failed: {error}");
                return;
            }
        };
        let running = self.compaction.running_namespaces();
        for (ns, stored) in versions {
            if running.contains(&ns) || self.indexer.manifest_version(&ns) == Some(stored) {
                continue;
            }
            if let Err(error) = self.indexer_follow_namespace(&ns) {
                eprintln!("WARN: indexer: cannot follow ns={}: {error}", ns.0);
            }
        }
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
        if self.indexer.local_retired_count() == 0 {
            return kosha_write::LocalReclaimReport::default();
        }
        // Objects a branch still reads through its ancestor's prefix stay
        // (#208). Without every manifest there is no telling which, so the
        // pass is skipped rather than guessing.
        let branches = match self.branch_references() {
            Ok(refs) => refs,
            Err(e) => {
                eprintln!("WARN: local GC skipped: {e}");
                return kosha_write::LocalReclaimReport {
                    pending: self.indexer.local_retired_count(),
                    ..Default::default()
                };
            }
        };
        let report = self.indexer.reclaim_local_shared(
            grace,
            |ns, seg| {
                // A shard's segment (#207) is also cached under the root
                // namespace, by its merged-view id: release both.
                let merged = merged_view_alias(ns, seg)
                    .is_some_and(|(root, id)| self.searcher.release_segment(&root.0, &id.0));
                self.searcher.release_segment(&ns.0, &seg.0) | merged
            },
            |rel| branches.holds_segment_dir(rel) || branches.holds_file(rel),
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

    /// Every namespace and every manifest, for GC's reference sets (#208):
    /// a branch lists segments (and deletion bitmaps) under its parent's
    /// prefix, which the parent's GC must keep however long ago the parent
    /// itself dropped them. Fallible on purpose: a reference set that
    /// silently missed a manifest would reclaim live objects, so the
    /// namespace list comes from the fallible `manifest_versions` (a
    /// failed `list_namespaces` reads as empty), and any unreadable
    /// manifest fails the whole call. Call with `branch_lock` held.
    fn every_manifest(&self) -> Result<(Vec<NamespaceId>, NamespaceManifests), String> {
        let mut all = self
            .controller
            .lock()
            .unwrap()
            .manifest_versions()
            .map_err(|e| format!("cannot list manifests: {e}"))?
            .into_iter()
            .map(|(ns, _)| ns)
            .collect::<Vec<_>>();
        for ns in self.controller.lock().unwrap().list_namespaces() {
            if !all.contains(&ns) {
                all.push(ns);
            }
        }
        let mut manifests = Vec::new();
        for ns in &all {
            match self.controller.lock().unwrap().try_manifest_cloned(ns) {
                Ok(Some(manifest)) => manifests.push((ns.clone(), manifest)),
                Ok(None) => {}
                Err(e) => return Err(format!("{}: {e}", ns.0)),
            }
        }
        Ok((all, manifests))
    }

    /// What every branch manifest references (#208), for local GC: a
    /// copy-on-write branch reads segments and deletion bitmaps under its
    /// ancestors' prefixes, so an ancestor dropping them from its own
    /// manifest does not free their local files.
    fn branch_references(&self) -> Result<kosha_core::branch::References, String> {
        let _lineage = self.branch_lock.lock().unwrap();
        let (_, manifests) = self.every_manifest()?;
        kosha_core::branch::References::collect(
            manifests
                .iter()
                .filter(|(_, manifest)| manifest.branch.is_some())
                .map(|(ns, manifest)| (ns, manifest)),
        )
        .map_err(|e| format!("cannot resolve branch references: {e}"))
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
        // No branch is created or deleted while GC decides what is still
        // referenced (#208).
        let _lineage = self.branch_lock.lock().unwrap();
        let mut reports = Vec::new();
        let (all, manifests) = match self.every_manifest() {
            Ok(found) => found,
            Err(e) => {
                return namespaces
                    .iter()
                    .map(|ns| kosha_core::gc::NamespaceGcReport {
                        namespace: ns.0.clone(),
                        dry_run: config.dry_run,
                        aborted: Some(format!(
                            "GC skipped: cannot read every manifest to find shared segments \
                             ({e})"
                        )),
                        ..Default::default()
                    })
                    .collect();
            }
        };
        for ns in namespaces {
            let others: Vec<NamespaceId> = all.iter().filter(|n| *n != ns).cloned().collect();
            let referenced = kosha_core::branch::References::collect(
                manifests
                    .iter()
                    .filter(|(other, manifest)| other != ns && manifest.branch.is_some())
                    .map(|(other, manifest)| (other, manifest)),
            );
            let config = match referenced {
                Ok(refs) => kosha_core::gc::GcConfig {
                    referenced_elsewhere: Some(Arc::new(refs)),
                    ..config.clone()
                },
                Err(e) => {
                    reports.push(kosha_core::gc::NamespaceGcReport {
                        namespace: ns.0.clone(),
                        dry_run: config.dry_run,
                        aborted: Some(format!("GC skipped: cannot resolve branch references: {e}")),
                        ..Default::default()
                    });
                    continue;
                }
            };
            let config = &config;
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
            // A shard (#207) is warmed when its sharded root is listed.
            let listed = self.warmup_namespaces.contains(&ns.0)
                || kosha_core::sharding::shard_index_of(ns).is_some_and(|_| {
                    ns.0.rsplit_once('/')
                        .is_some_and(|(root, _)| self.warmup_namespaces.iter().any(|w| w == root))
                });
            if self.s3_storage.is_none() || !listed {
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
        self.hydrate_deletes(ns, &mut manifest)?;
        Ok(Some(manifest))
    }

    /// Install a manifest built outside the write path (admin import) by
    /// CAS against the version the control store holds right now. Never
    /// rebases: a concurrent publish wins and this fails with
    /// `ManifestConflict`, leaving both the store and the indexer alone.
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
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
                synced.insert(synced_key(&manifest, ns, &entry.segment_id));
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
        self.shard_views.lock().unwrap().remove(ns);
        // A deleted namespace's built unindexed tail (#255) and its
        // transient segment go too.
        self.drop_cached_tail(ns);
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
            // The crash-point harness's in-memory bucket, when one is
            // installed on this thread; never compiled into a server.
            #[cfg(all(test, feature = "failpoints"))]
            crash_points_tests::upload_hook(&self.data_dir, ns, manifest)?;
            let _ = manifest;
            Ok(())
        })
    }

    /// Merged manifest over the shards of sharded root `ns` (#207), whose
    /// current shared manifest is `root`. Shards never written are absent
    /// (empty). Rebuilt only when the root or a shard's manifest changed.
    fn sharded_view(
        &self,
        ns: &NamespaceId,
        root: Arc<kosha_core::Manifest>,
    ) -> Arc<kosha_core::Manifest> {
        let count = root.shards.unwrap_or(1);
        let shards: Vec<Option<Arc<kosha_core::Manifest>>> =
            kosha_core::sharding::shard_namespaces(ns, count)
                .iter()
                .map(|shard| self.indexer.manifest_shared(shard))
                .collect();
        let mut views = self.shard_views.lock().unwrap();
        if let Some(view) = views.get(ns).filter(|v| v.is_current(&root, &shards)) {
            return Arc::clone(&view.merged);
        }
        let merged = Arc::new(kosha_core::sharding::merge_shard_manifests(&root, &shards));
        views.insert(
            ns.clone(),
            ShardView {
                root,
                shards,
                merged: Arc::clone(&merged),
            },
        );
        merged
    }

    /// Cache footers parsed during a search's hydration on the manifest(s)
    /// that list their segments. A merged sharded view's segment ids are
    /// `_shard-XXXX/{id}`: those footers belong to the shard namespace.
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
    fn remember_search_footers(
        &self,
        ns: &NamespaceId,
        footers: &HashMap<kosha_core::SegmentId, Arc<kosha_core::Footer>>,
    ) {
        let sharded = self.indexer.shard_count(ns).is_some();
        let mut by_shard: HashMap<NamespaceId, Vec<Arc<kosha_core::Footer>>> = HashMap::new();
        for (segment, footer) in footers {
            let target = match kosha_core::sharding::split_merged_segment_id(segment) {
                Some((index, _)) if sharded => kosha_core::sharding::shard_namespace(ns, index),
                _ => ns.clone(),
            };
            by_shard.entry(target).or_default().push(Arc::clone(footer));
        }
        for (target, footers) in by_shard {
            self.indexer.remember_segment_footers(&target, footers);
        }
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
    ///
    /// A writer pod presents its writer-lease token on the CAS (#254). A
    /// `Fenced` rejection is never rebased: the namespace is fenced
    /// ([`Self::fence_namespace`]) and the error returned (HTTP 503).
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
            // Before the upload: a writer that cannot own the namespace
            // uploads nothing.
            let held_before = self.writer_leases.held_before(ns);
            let token = match self.writer_leases.token(ns, &self.controller) {
                Ok(token) => token,
                Err(fenced @ KoshaError::Fenced { .. }) => {
                    if held_before {
                        self.fence_namespace(ns, "another writer took its lease over");
                    }
                    return Err(fenced);
                }
                Err(error) => return Err(error),
            };
            // Crash-point seams (#257): segment files are local only; then
            // uploaded, but no manifest references them.
            fail::fail_point!("flush.before_upload");
            upload(&manifest)?;
            fail::fail_point!("publish.before_cas");
            let committed = self
                .controller
                .lock()
                .unwrap()
                .compare_and_swap_manifest_fenced(ns, expected_version, manifest.clone(), token);
            match committed {
                Ok(()) => {
                    // Crash-point seam (#257): committed remotely, not yet
                    // acknowledged locally (WAL not reclaimed).
                    fail::fail_point!("publish.after_cas_before_ack");
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
                            synced.insert(synced_key(&remote, ns, &entry.segment_id));
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
                Err(fenced @ KoshaError::Fenced { .. }) => {
                    self.fence_namespace(ns, "a manifest commit was fenced");
                    return Err(fenced);
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
            let seg_path = manifest.segment_dir(&self.data_dir, ns, &segment_id);
            // Only mark synced once the segment is committed (`_COMMIT`
            // written last, #231). A partial upload must be retried by a
            // future publish call, never treated as durable.
            sync_segment_dir_to_s3(s3, &self.data_dir, &seg_path).map_err(KoshaError::NotFound)?;
            self.synced_segments
                .lock()
                .unwrap()
                .insert(synced_key(manifest, ns, &segment_id));
        }
        if let Some(ref s3) = self.s3_storage {
            for (segment, vector) in &manifest.deletion_vectors {
                // Inherited from a branch parent (#208): already durable in
                // the parent's prefix, and not this namespace's to write.
                if vector.is_inherited(ns) {
                    continue;
                }
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
                    "hydrate_files shed: stopped at chunk {}/{}: {}{}",
                    chunk_idx + 1,
                    total_chunks,
                    request.shed_reason(),
                    request_id()
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
            self.metrics
                .record_hydrations(metrics::HydrationSource::S3, fetched);
            self.metrics
                .record_hydrations(metrics::HydrationSource::Local, cached);
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

    /// Pin every segment directory of `manifest` in the disk cache until
    /// the returned guard drops.
    fn pin_segments(&self, ns: &NamespaceId, manifest: &kosha_core::Manifest) -> SegmentPins<'_> {
        let keys: Vec<String> = manifest
            .segments
            .iter()
            .filter_map(|entry| {
                let dir = manifest.segment_dir(&self.data_dir, ns, &entry.segment_id);
                dir.strip_prefix(&self.data_dir)
                    .ok()
                    .map(|rel| rel.to_string_lossy().into_owned())
            })
            .collect();
        for key in &keys {
            self.cache.pin(key);
        }
        SegmentPins {
            cache: &self.cache,
            keys,
        }
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
                    self.metrics.record_lease(metrics::LeaseOutcome::Won);
                    owned_leases.push(path.clone());
                    fetch_from_s3.push((path.clone(), *size));
                }
                kosha_control::HydrationLease::OwnedBy(peer_addr) => {
                    // Streams the file straight into the cache (#259).
                    match self.fetch_from_peer(&peer_addr, path) {
                        Ok(bytes) => {
                            self.metrics
                                .record_lease(metrics::LeaseOutcome::StreamedFromPeer);
                            self.metrics
                                .record_hydrations(metrics::HydrationSource::Peer, 1);
                            // Peer-fetched bytes are hydration too — charge
                            // them so a request can't route around its
                            // ceiling just because another replica happened
                            // to own the lease.
                            request.charge(bytes);
                        }
                        Err(e) => {
                            self.metrics.record_lease(metrics::LeaseOutcome::Timeout);
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
    /// The body streams to a temp file that is renamed into the cache when
    /// complete, so memory stays O(chunk) whatever the file size (#259).
    /// Returns the bytes written.
    ///
    /// Polls with a short bounded backoff rather than a single shot: we
    /// lost the cross-replica race, so the owner may still be mid-download
    /// when we ask. Gives up well before a caller would consider the whole
    /// request stuck — any error here is meant to be handled by falling
    /// back to fetching straight from S3, not by failing the request.
    #[cfg(all(feature = "s3", feature = "postgres"))]
    fn fetch_from_peer(&self, peer_addr: &str, logical_path: &str) -> Result<u64, KoshaError> {
        let Some(ref client) = self.peer_http_client else {
            return Err(KoshaError::NotFound(
                "no peer HTTP client configured".into(),
            ));
        };

        let url = format!(
            "{}://{peer_addr}/internal/segment/{logical_path}",
            self.peer_url_scheme
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut delay = Duration::from_millis(100);
        loop {
            let mut req = client.get(&url);
            if let Some(ref key) = self.internal_auth_key {
                req = req.bearer_auth(key);
            }
            match req.send() {
                Ok(resp) if resp.status().as_u16() == 200 => {
                    return self.store_peer_response(resp, logical_path).map_err(|e| {
                        KoshaError::NotFound(format!("peer {peer_addr} transfer failed: {e}"))
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

    /// Write a peer's `200` segment response into the cache under
    /// `logical_path`: to a temp file first, renamed into place only once
    /// complete, so a reader never sees a partial file.
    ///
    /// Current peers send raw bytes. A peer on a build from before #259
    /// sends the whole file base64-encoded with `x-kosha-encoding: base64`;
    /// that is still decoded, so a rolling upgrade with mixed peers works.
    #[cfg(all(feature = "s3", feature = "postgres"))]
    fn store_peer_response(
        &self,
        mut resp: reqwest::blocking::Response,
        logical_path: &str,
    ) -> Result<u64, KoshaError> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dest = self.cache.path_for(logical_path)?;
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = dest.with_file_name(format!(
            ".{}.peer-{}-{}.tmp",
            dest.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("segment"),
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let legacy_base64 = resp
            .headers()
            .get("x-kosha-encoding")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("base64"));
        let declared = resp.content_length();

        let written = (|| -> Result<u64, KoshaError> {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp)?;
            let written = if legacy_base64 {
                let encoded = resp
                    .text()
                    .map_err(|e| KoshaError::NotFound(format!("read: {e}")))?;
                use base64::Engine;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(encoded.trim())
                    .map_err(|e| KoshaError::NotFound(format!("base64 decode: {e}")))?;
                file.write_all(&bytes)?;
                bytes.len() as u64
            } else {
                let mut out = std::io::BufWriter::with_capacity(256 * 1024, &mut file);
                let n = std::io::copy(&mut resp, &mut out)?;
                out.flush()?;
                if let Some(expected) = declared.filter(|expected| *expected != n) {
                    return Err(KoshaError::NotFound(format!(
                        "short body: {n} of {expected} bytes"
                    )));
                }
                n
            };
            Ok(written)
        })();
        match written {
            Ok(n) => {
                if let Err(e) = std::fs::rename(&tmp, &dest) {
                    let _ = std::fs::remove_file(&tmp);
                    return Err(e.into());
                }
                self.cache.note_external_write(logical_path);
                Ok(n)
            }
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                Err(e)
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
            .map(|entry| manifest.segment_dir(&self.data_dir, ns, &entry.segment_id))
            .collect();

        let missing_manifest_footer_paths: Vec<PathBuf> = manifest
            .segments
            .iter()
            .filter(|entry| manifest.segment_footer(&entry.segment_id).is_none())
            .map(|entry| manifest.segment_dir(&self.data_dir, ns, &entry.segment_id))
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
            let path = manifest
                .segment_dir(&self.data_dir, ns, &entry.segment_id)
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
                let seg_path = manifest.segment_dir(&self.data_dir, ns, &entry.segment_id);
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
                // misplaced file) must not be cached under this id. In a
                // merged sharded view (#207) the entry id carries the
                // shard's path component; the footer keeps the shard-local
                // id it was written with.
                let own_id = kosha_core::sharding::split_merged_segment_id(&entry.segment_id)
                    .map_or_else(|| entry.segment_id.clone(), |(_, local)| local);
                (footer.segment_id == entry.segment_id || footer.segment_id == own_id)
                    .then(|| (entry.segment_id.clone(), Arc::new(footer)))
            })
            .collect();
        if !fetched_footers.is_empty() {
            self.remember_search_footers(ns, &fetched_footers);
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
                segment_footer(entry).map(|footer| {
                    (
                        manifest.segment_dir(&self.data_dir, ns, &entry.segment_id),
                        footer,
                    )
                })
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
                let seg_path = manifest.segment_dir(&self.data_dir, ns, &entry.segment_id);
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
        outcome.set_shed(request);
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
            let complete = Self::scoring_complete(
                p,
                knn_field,
                vectors_ranged,
                filters_of(p),
                manifest_footers.get(p).copied(),
            );
            // A search's segment found with its scoring files already local
            // is a disk-cache hit for its tenant; one that must hydrate is a
            // miss (#342, `kosha_cache_{hits,misses}{cache="nvme"}`).
            if let Ok(rel) = p.strip_prefix(&self.data_dir) {
                let rel = rel.to_string_lossy();
                self.cache
                    .record_lookup(kosha_core::tenancy::tenant_of_segment_dir(&rel), complete);
            }
            complete
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
            // Legacy segments whose `doc_vectors.bin` header a kNN needs,
            // with their object prefix: fetched by range after this round.
            let mut doc_vector_headers: Vec<(&Path, String)> = Vec::new();
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
                    // Only the header of a legacy `doc_vectors.bin` (its
                    // field table binds the unnamed index), by range below:
                    // the whole file is as large as the index itself. With
                    // global rescoring on (#381), a quantized column's
                    // header too (see `knn_wants_doc_vectors_header`).
                    if knn_wants_doc_vectors_header(footer, field)
                        && !doc_spans::doc_vectors_header_local(seg_path)
                    {
                        doc_vector_headers.push((seg_path.as_path(), s3_prefix.clone()));
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

            // Legacy `doc_vectors.bin` headers, by range into the span
            // cache (the reader binds the unnamed index from that span).
            if !doc_vector_headers.is_empty() && !request.is_shed() {
                let fetcher = doc_spans::S3RangeFetcher {
                    s3,
                    concurrency: self.scoring_hydrate_concurrency,
                    budget: &self.hydration_budget,
                    request,
                };
                let targets: Vec<(&Path, &str)> = doc_vector_headers
                    .iter()
                    .map(|(dir, rel)| (*dir, rel.as_str()))
                    .collect();
                let fetched = doc_spans::fetch_doc_vectors_headers(&targets, &fetcher, &self.cache);
                outcome.rounds += fetched.rounds;
                outcome.shed |= fetched.shed;
                for seg in &fetched.segments {
                    outcome.bytes_fetched += seg.bytes;
                    if seg.status == doc_spans::HeaderStatus::Cached {
                        outcome.files_fetched += 1;
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

    /// A warm search whose cached hydration verdict covers its scoring set
    /// never reaches `ensure_scoring_files_local`, where per-segment
    /// `kosha_cache_{hits,misses}{cache="nvme"}` are otherwise counted, so
    /// its segments are counted as hits here (#342).
    #[cfg(feature = "s3")]
    fn record_warm_nvme_hits(&self, ns: &NamespaceId, manifest: &kosha_core::Manifest) {
        self.cache.record_lookups(
            kosha_core::tenancy::tenant_of_namespace(&ns.0),
            manifest.segments.len() as u64,
            0,
        );
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
                let dir = manifest.segment_dir(&self.data_dir, ns, &entry.segment_id);
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
            .map(|entry| manifest.segment_dir(&self.data_dir, ns, &entry.segment_id))
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
        outcome.set_shed(request);
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
        // A failed range of `doc_vectors.bin` (only global rescoring, #381,
        // asks for those) is NOT retried as the whole file: that is the
        // full-precision sidecar the codes-only tier exists to keep remote,
        // ~200 MB per segment. The pass reports the vectors it could not
        // read as unavailable (and the segment degraded) instead.
        if o.failed && file != kosha_segment::DOC_VECTORS_FILE {
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
        outcome.set_shed(request);
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

/// Namespaces with their manifests (see `AppState::every_manifest`).
type NamespaceManifests = Vec<(NamespaceId, kosha_core::Manifest)>;

/// For a shard namespace's segment (#207): the root namespace and the
/// merged-view id a search of the sharded root caches its reader under.
/// `None` for an ordinary namespace.
pub(crate) fn merged_view_alias(
    ns: &NamespaceId,
    segment: &kosha_core::SegmentId,
) -> Option<(NamespaceId, kosha_core::SegmentId)> {
    let index = kosha_core::sharding::shard_index_of(ns)?;
    let (root, _) = ns.0.rsplit_once('/')?;
    Some((
        NamespaceId(root.to_string()),
        kosha_core::sharding::merged_segment_id(index, segment),
    ))
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

#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
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
    if std::env::args().nth(1).as_deref() == Some("migrate-namespace") {
        std::process::exit(namespace_migrate::main(std::env::args().skip(2)));
    }
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

    // Before anything else reads the environment: a pod whose KOSHA_ROLE
    // contradicts the rest of its configuration exits here (#254).
    let role = match role::resolve_role(&role::RoleEnv::from_process()) {
        Ok(resolved) => {
            if let Some(warning) = resolved.warning {
                eprintln!("WARN: {warning}");
            }
            resolved.role
        }
        Err(message) => fatal(&message),
    };
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
    // #382 experiment flags: one line only when one is set or the image was
    // built above baseline x86-64, so a default server logs nothing new.
    if let Some(summary) = kosha_segment::experimental_vector_kernels_summary() {
        println!("kosha-server {summary}");
    }

    let state = AppState::new_shared_as(PathBuf::from(data_dir.clone()), role);
    // Pre-#262 bare namespaces no tenant can reach (#344).
    namespace_migrate::warn_unreachable_bare_namespaces(
        &state.controller.lock().unwrap().list_namespaces(),
    );
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
    state.spawn_writer_lease_renewer();
    state.spawn_manifest_refresher();
    state.spawn_local_gc();
    let listener = TcpListener::bind(&addr).expect("failed to bind HTTP listener");
    let config =
        http::Config::from_env(Duration::from_secs(io_timeout_secs), auth::max_body_bytes());
    let scheme = if config.tls.is_some() {
        "https"
    } else {
        "http"
    };
    println!(
        "kosha-server role={role} listening on {scheme}://{addr} data_dir={data_dir} \
         max_connections={} max_inflight={}",
        config.max_connections, config.max_inflight
    );

    if let Err(error) = serve_until(
        listener,
        Arc::clone(&state),
        config,
        http::os_shutdown_signal(),
    ) {
        fatal(&format!("HTTP server failed: {error}"));
    }
    // SIGTERM/SIGINT: the listener is closed and in-flight requests are
    // drained (or the grace period ran out). Hand writer leases back so the
    // next writer pod can take them now instead of after
    // KOSHA_WRITER_LEASE_TTL_MS. Unflushed writes stay in the WAL, which the
    // next owner replays, exactly as after a crash.
    let released = state.writer_leases.release_all(&state.controller);
    println!("kosha-server stopped; released {released} writer lease(s)");
    std::process::exit(0);
}

/// Serve HTTP on `listener` until `shutdown` resolves, then drain (see
/// `http.rs`). Every request runs [`dispatch`] on the server's bounded
/// blocking pool; the rayon scoring pool is untouched.
fn serve_until(
    listener: TcpListener,
    state: Arc<AppState>,
    config: http::Config,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let handler: http::Handler = Arc::new(move |request| dispatch(&state, request));
    http::run(listener, config, handler, shutdown)
}

/// Serve until the process exits, with the environment's HTTP settings.
/// Tests only: `main` uses [`serve_until`] with the OS shutdown signal.
#[cfg(test)]
fn serve(listener: TcpListener, state: Arc<AppState>, io_timeout: Duration) {
    let config = http::Config::from_env(io_timeout, auth::max_body_bytes());
    serve_until(listener, state, config, std::future::pending()).expect("HTTP server failed");
}

// ─── Request handling ───────────────────────────────────────────────────────

/// Handle one parsed request: probes, the dashboard, authentication, role
/// routing, then [`route`]. Synchronous; the HTTP server (`http.rs`) has
/// already enforced the body-size cap and read the body.
fn dispatch(state: &AppState, request: http::Request) -> http::Response {
    let started = Instant::now();
    let request_line = request.request_line();
    let headers = request.headers;
    let body = request.body;

    set_request_id(headers.get("x-request-id").map(String::as_str));

    // Liveness/readiness probes hit this with no Authorization header (k8s
    // httpGet probes don't support that out of the box) — must stay
    // unauthenticated, per the doc comment at the top of this file.
    if request_line.starts_with("GET /healthz") || request_line.starts_with("GET /v1/healthz") {
        return http::Response::from_raw(json_ok(&serde_json::json!({"status": "ok"})));
    }

    // `/readyz` (Tier 1 O2): distinct from `/healthz` (liveness — always
    // 200) so a query pod mid cold-read-warmup stays alive-but-not-ready
    // and HPA/kube endpoints remove it from Service rotation until its
    // configured hot namespaces are prefetched. Probes hit this
    // unauthenticated, same as `/healthz`.
    if request_line.starts_with("GET /readyz") || request_line.starts_with("GET /v1/readyz") {
        if state.is_ready() {
            return http::Response::from_raw(json_ok(&serde_json::json!({"status": "ready"})));
        }
        let body = serde_json::json!({
            "status": "warming",
            "warmup_namespaces": state.warmup_namespaces,
            "boot_namespaces": *state.warming.boot_namespaces.lock().unwrap(),
        })
        .to_string();
        return http::Response::json(503, body);
    }

    // `/metrics` (#223): Prometheus scrapes, unauthenticated by default like
    // the probes above. Never forwarded to the ingest pod (see
    // `is_write_path`) and answered ahead of the indexer's route gate
    // (#254): each pod, whatever its role, reports its own process.
    if request_line.starts_with("GET /metrics ") || request_line.starts_with("GET /metrics?") {
        return http::Response::from_raw(handle_metrics(state, &headers));
    }

    // Built-in dashboard (#214): its static assets hold no data and are
    // served without a key; every data call the page makes comes back
    // through the authenticated API below. 404 unless KOSHA_DASHBOARD is on.
    if let Some(resp) = dashboard::static_response(&request_line, state.dashboard.enabled()) {
        return http::Response::from_raw(resp);
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
        (Err(resp), _, _) => http::Response::from_raw(resp.clone()),
        // An indexer (#254) serves only the compaction admin API.
        (Ok(_), _, _)
            if state.role == role::Role::Indexer && !role::Role::indexer_serves(&request_line) =>
        {
            http::Response::from_raw(json_error(
                404,
                "not served by an indexer pod (KOSHA_ROLE=indexer): it serves only \
                 /v1/admin/compact* and /v1/admin/compaction-*",
            ))
        }
        (Ok(_), Some(ingest_host), Some(client)) if is_write_path(&request_line) => {
            http::Response::from_raw(forward_to_ingest(
                client,
                ingest_host,
                &request_line,
                &headers,
                &body,
            ))
        }
        // Pod-to-pod segment transfer streams the file instead of building
        // a `String` (#259). Any other credential falls through to `route`,
        // which answers 403.
        (Ok(_), _, _)
            if request_line.starts_with("GET /internal/segment/")
                && service_authorized(&headers, state.internal_service_key.as_deref()) =>
        {
            internal_segment_response(&request_line, state)
        }
        (Ok(tenant), _, _) => http::Response::from_raw(serve_tenant_request(
            &request_line,
            &headers,
            &body,
            tenant,
            state,
        )),
    };
    log_request(
        &request_line,
        authentication.as_deref().unwrap_or("-"),
        body.len(),
        &response,
        started,
    );
    response
}

thread_local! {
    /// " rid=<id>" for the request this connection thread is serving, or
    /// empty. A caller's `X-Request-Id` on every log line of its request
    /// lets one slow or failed request be found in the pod log.
    static REQUEST_ID: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
}

fn set_request_id(header: Option<&str>) {
    let id: String = header
        .unwrap_or("")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .take(64)
        .collect();
    REQUEST_ID.with(|r| {
        *r.borrow_mut() = if id.is_empty() {
            id
        } else {
            format!(" rid={id}")
        }
    });
}

fn request_id() -> String {
    REQUEST_ID.with(|r| r.borrow().clone())
}

/// Every cache's per-tenant figures for `kosha_cache_*` (#342).
fn cache_tenant_samples(state: &AppState) -> Vec<metrics::CacheTenantSample> {
    let result = state.result_cache.lock().unwrap().tenants.stats();
    [
        ("nvme", state.cache.tenant_stats()),
        ("segment", state.searcher.segment_cache_tenant_stats()),
        ("result", result),
    ]
    .into_iter()
    .flat_map(|(cache, stats)| {
        stats
            .into_iter()
            .map(move |stats| metrics::CacheTenantSample { cache, stats })
    })
    .collect()
}

/// `GET /metrics`: the Prometheus text exposition of [`metrics::Metrics`],
/// with the gauges and counters the server already keeps elsewhere read at
/// scrape time. `KOSHA_METRICS=off` hides the route (404);
/// `KOSHA_METRICS_AUTH=admin` requires the admin credential (403 without).
fn handle_metrics(state: &AppState, headers: &HashMap<String, String>) -> String {
    match state.metrics.config.exposure {
        metrics::Exposure::Off => return json_error(404, "not found"),
        metrics::Exposure::Admin
            if !service_authorized(headers, state.admin_auth_key.as_deref()) =>
        {
            return json_error(403, "admin credential required");
        }
        _ => {}
    }
    #[cfg(feature = "s3")]
    let segment_checksum_failures = state
        .segment_checksum_failures
        .load(std::sync::atomic::Ordering::Relaxed);
    #[cfg(not(feature = "s3"))]
    let segment_checksum_failures = 0u64;
    let body = state.metrics.render(&metrics::Scrape {
        search_queue_depth: state.search_semaphore.waiters() as u64,
        nvme_cache_bytes: state.cache.total_size(),
        segment_cache_bytes: state.searcher.segment_cache_bytes(),
        nvme_evictions: state.cache.eviction_generation(),
        segment_checksum_failures,
        cache_tenants: cache_tenant_samples(state),
    });
    format!(
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: {}\r\n\r\n{body}",
        body.len(),
        metrics::CONTENT_TYPE,
    )
}

// ─── Per-tenant limits (#341) ──────────────────────────────────────────────

/// [`route`] an authenticated request this pod handles itself, after its
/// tenant's request-entry limits (#341): a search takes a qps token and a
/// concurrent-query slot (held until the response is built); a write route
/// is checked against the tenant's body size cap and write bytes per
/// second. A query pod forwards writes before this runs, so write limits
/// are enforced where writes execute: on the ingest pod.
fn serve_tenant_request(
    request_line: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
    tenant: &str,
    state: &AppState,
) -> String {
    let mut _query_slot = None;
    if tenant_is_limited(state, tenant) {
        if is_search_route(request_line) {
            match state.tenant_limits.admit_query(tenant) {
                Ok(slot) => _query_slot = Some(slot),
                Err(rejection) => return tenant_rejection(state, tenant, &rejection),
            }
        } else if is_write_path(request_line) {
            if let Err(rejection) = state.tenant_limits.admit_write_body(tenant, body.len()) {
                return tenant_rejection(state, tenant, &rejection);
            }
        }
    }
    route(request_line, headers, body, tenant, state)
}

/// Whether `tenant`'s requests are subject to tenant limits at all.
fn tenant_is_limited(state: &AppState, tenant: &str) -> bool {
    state.tenant_limits.enabled() && tenant != SERVICE_TENANT
}

/// A search route, in any of its forms: `POST /v1/namespaces/{ns}/search`,
/// and the legacy `POST /search` and `GET /search?...`.
fn is_search_route(request_line: &str) -> bool {
    parse_method_and_path(request_line).is_some_and(|(method, path)| {
        let path = path.split('?').next().unwrap_or(path);
        ((method == "POST" || method == "GET") && path == "/search")
            || (method == "POST"
                && path
                    .strip_prefix("/v1/namespaces/")
                    .and_then(|rest| rest.strip_suffix("/search"))
                    .is_some_and(|ns| !ns.is_empty() && !ns.contains('/')))
    })
}

/// The 429 for a request refused by a tenant limit: `Retry-After` plus a
/// JSON body `{error, reason, limit, retry_after_ms}`. Counted in
/// `kosha_tenant_rejections_total`.
fn tenant_rejection(
    state: &AppState,
    tenant: &str,
    rejection: &tenant_limits::Rejection,
) -> String {
    state
        .metrics
        .record_tenant_rejection(rejection.reason, tenant);
    eprintln!(
        "WARN: request refused: status=429 tenant={tenant} reason={} limit={} retry_after_ms={}{}",
        rejection.reason.as_str(),
        rejection.limit,
        rejection.retry_after_ms,
        request_id()
    );
    let body = rejection.body().to_string();
    retry_after(
        format!(
            "HTTP/1.1 429 Too Many Requests\r\ncontent-length: {}\r\ncontent-type: application/json\r\n\r\n{body}",
            body.len()
        ),
        rejection.retry_after_secs(),
    )
}

/// The tenant whose limits apply to writes into `ns`, with its limits.
/// `None` when nothing limits it (no limits configured, a bare namespace,
/// or operator traffic).
fn write_limits(
    state: &AppState,
    ns: &NamespaceId,
) -> Option<(String, tenant_limits::TenantLimits)> {
    let tenant = namespace_tenant(&ns.0)?;
    if !tenant_is_limited(state, tenant) {
        return None;
    }
    let limits = state.tenant_limits.limits(tenant)?;
    Some((tenant.to_string(), limits))
}

/// Whether `ns` does not exist yet, so writing to (or creating) it would
/// add a namespace.
fn namespace_is_new(state: &AppState, ns: &NamespaceId) -> bool {
    state.indexer.manifest_version(ns).is_none()
        && !state.controller.lock().unwrap().has_namespace(ns)
}

/// Namespaces `tenant` holds: top-level names only, as `GET /v1/namespaces`
/// lists them (shards and branches are not counted).
fn tenant_namespace_count(state: &AppState, tenant: &str) -> u64 {
    let prefix = format!("{tenant}/");
    let all = state.controller.lock().unwrap().list_namespaces();
    let mut names: Vec<&str> = all
        .iter()
        .filter_map(|ns| ns.0.strip_prefix(&prefix))
        .filter(|name| !name.is_empty() && !name.contains('/'))
        .collect();
    names.sort_unstable();
    names.dedup();
    names.len() as u64
}

/// The namespace quota: refuse to add `ns` when its tenant already holds
/// `namespaces` of them. Checked on explicit creation and on the first
/// write that would auto-create a namespace. Per pod and approximate under
/// concurrent creations.
fn check_namespace_quota(state: &AppState, ns: &NamespaceId) -> Result<(), String> {
    let Some((tenant, limits)) = write_limits(state, ns) else {
        return Ok(());
    };
    let Some(max) = limits.namespaces else {
        return Ok(());
    };
    if namespace_is_new(state, ns) && tenant_namespace_count(state, &tenant) >= max {
        return Err(tenant_rejection(
            state,
            &tenant,
            &tenant_limits::Rejection::quota(tenant_limits::RejectReason::NamespaceQuota, max),
        ));
    }
    Ok(())
}

/// Documents in `ns`: committed and not deleted, plus writes not yet
/// flushed (over every shard of a sharded namespace). An estimate: an
/// unflushed upsert of an existing id counts twice until it is flushed.
fn namespace_doc_estimate(state: &AppState, ns: &NamespaceId) -> u64 {
    let shards = state.indexer.shard_count(ns);
    let committed = state.indexer.manifest_shared(ns).map_or(0, |m| {
        let m = match shards {
            Some(_) => state.sharded_view(ns, m),
            None => m,
        };
        let docs: u64 = m.segments.iter().map(|s| s.doc_count as u64).sum();
        let deleted: u64 = m.deletion_vectors.values().map(|v| v.count).sum();
        docs.saturating_sub(deleted)
    });
    let mut pending = state.indexer.pending_doc_count(ns) as u64;
    if let Some(count) = shards {
        for shard in kosha_core::sharding::shard_namespaces(ns, count) {
            pending += state.indexer.pending_doc_count(&shard) as u64;
        }
    }
    committed + pending
}

/// The widest vector in `documents` (a bag of vectors counts its widest).
fn widest_vector(documents: &[kosha_core::Document]) -> usize {
    documents
        .iter()
        .flat_map(|doc| doc.fields.iter())
        .filter(|f| f.field_type == kosha_core::FieldType::Vector)
        .map(|f| {
            if let Ok(v) = serde_json::from_str::<Vec<f32>>(&f.value) {
                v.len()
            } else if let Ok(bag) = serde_json::from_str::<Vec<Vec<f32>>>(&f.value) {
                bag.iter().map(Vec::len).max().unwrap_or(0)
            } else {
                0
            }
        })
        .max()
        .unwrap_or(0)
}

/// The vector dimension limit for `documents` bound for `ns`.
fn check_vector_dims(
    state: &AppState,
    ns: &NamespaceId,
    documents: &[kosha_core::Document],
) -> Result<(), String> {
    let Some((tenant, limits)) = write_limits(state, ns) else {
        return Ok(());
    };
    let Some(max) = limits.vector_dims else {
        return Ok(());
    };
    if widest_vector(documents) > max as usize {
        return Err(tenant_rejection(
            state,
            &tenant,
            &tenant_limits::Rejection::quota(
                tenant_limits::RejectReason::VectorDims,
                u64::from(max),
            ),
        ));
    }
    Ok(())
}

/// Per-tenant write limits for an index or replace of `documents` into
/// `ns` (#341), before they are embedded: the namespace quota (a write
/// auto-creates its namespace), the documents-per-namespace quota, vector
/// dimensions of supplied vectors, then the write docs per second. `Err`
/// carries the ready-to-send 429.
fn check_document_write(
    state: &AppState,
    ns: &NamespaceId,
    documents: &[kosha_core::Document],
) -> Result<(), String> {
    let Some((tenant, limits)) = write_limits(state, ns) else {
        return Ok(());
    };
    check_namespace_quota(state, ns)?;
    if let Some(max) = limits.docs_per_namespace {
        if namespace_doc_estimate(state, ns) + documents.len() as u64 > max {
            return Err(tenant_rejection(
                state,
                &tenant,
                &tenant_limits::Rejection::quota(tenant_limits::RejectReason::DocQuota, max),
            ));
        }
    }
    check_vector_dims(state, ns, documents)?;
    state
        .tenant_limits
        .admit_write_docs(&tenant, documents.len())
        .map_err(|rejection| tenant_rejection(state, &tenant, &rejection))
}

/// The vector dimension limit again after native embedding (#209), for a
/// namespace whose schema embeds text: the model's vectors count too.
fn check_embedded_dims(
    state: &AppState,
    ns: &NamespaceId,
    documents: &[kosha_core::Document],
) -> Result<(), String> {
    if write_limits(state, ns).is_none() || state.indexer.embedded_fields(ns).is_none() {
        return Ok(());
    }
    check_vector_dims(state, ns, documents)
}

/// Per-request search bookkeeping for `kosha_search_requests_total`, filled
/// in by the search handlers.
#[derive(Default)]
struct SearchObservation {
    /// Set when the scoring queue shed the request.
    shed: std::cell::Cell<Option<metrics::QueueShed>>,
    /// The searched namespace, recorded only with
    /// `KOSHA_METRICS_NAMESPACE_LABELS=on`.
    namespace: std::cell::RefCell<Option<String>>,
    /// The tenant owning the searched namespace: whose queue the search
    /// waits in for a scoring slot (#341).
    tenant: std::cell::RefCell<String>,
}

impl SearchObservation {
    fn namespace(&self, state: &AppState, ns: &NamespaceId) {
        *self.tenant.borrow_mut() = namespace_tenant(&ns.0).unwrap_or("").to_string();
        if state.metrics.config.namespace_labels {
            *self.namespace.borrow_mut() = Some(ns.0.clone());
        }
    }

    /// Acquire a scoring slot in the tenant's fair queue, recording a shed.
    fn scoring_slot<'a>(&self, state: &'a AppState) -> Option<fair_admission::FairPermit<'a>> {
        let tenant = self.tenant.borrow();
        let weight = state.tenant_limits.weight(&tenant);
        match state.search_semaphore.try_acquire(&tenant, weight) {
            Ok(slot) => Some(slot),
            Err(why) => {
                self.shed.set(Some(why));
                state.metrics.record_queue_shed();
                None
            }
        }
    }
}

/// Run one search handler and record its outcome and latency.
fn observe_search(state: &AppState, run: impl FnOnce(&SearchObservation) -> String) -> String {
    let started = Instant::now();
    let observation = SearchObservation::default();
    let response = run(&observation);
    let outcome = metrics::SearchOutcome::classify(&response, observation.shed.get());
    state.metrics.record_search(
        outcome,
        started.elapsed().as_secs_f64(),
        observation.namespace.borrow().as_deref(),
    );
    response
}

/// One line per API request: who asked for what, the status returned and how
/// long it took. A successful search is skipped because it already prints
/// the more detailed `search timing:` line; probes never reach here.
fn log_request(
    request_line: &str,
    tenant: &str,
    body_bytes: usize,
    response: &http::Response,
    started: Instant,
) {
    let status = response.status();
    let mut parts = request_line.split_whitespace();
    let (method, target) = (parts.next().unwrap_or("-"), parts.next().unwrap_or("-"));
    let path = target.split('?').next().unwrap_or(target);
    if status == 200 && path.ends_with("/search") {
        return;
    }
    println!(
        "request: {method} {path} status={status} tenant={tenant} ms={:.1} in_bytes={body_bytes} out_bytes={}{}",
        started.elapsed().as_secs_f64() * 1e3,
        response.body_len(),
        request_id()
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
    if extract_namespace(request_line, "POST /v1/namespaces/", "/shards").is_some() {
        return true;
    }
    // Branch create/delete (#208) write manifests and reclaim objects.
    if extract_namespace(request_line, "POST /v1/namespaces/", "/branch").is_some()
        || extract_namespace(request_line, "DELETE /v1/namespaces/", "/branch").is_some()
    {
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

/// A tenant id, or a namespace or branch name a tenant supplies. Names
/// starting with `_` or `.` are reserved for internal and legacy layouts
/// (`<tenant>/_shard-NNNN` would otherwise be accounted to the shared
/// `_legacy` tenant — see [`kosha_core::tenancy::is_reserved_name`]).
fn valid_namespace_component(name: &str) -> bool {
    !name.is_empty()
        && !kosha_core::tenancy::is_reserved_name(name)
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

    if is_list_namespaces_route(request_line) {
        return handle_list_namespaces(tenant, state);
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

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/shards") {
        return handle_put_shards_with_ns(&ns, tenant, body, state);
    }

    // ── Namespace branching (#208) ─────────────────────────────────────────
    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/branch") {
        return branching::handle_branch_create(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "GET /v1/namespaces/", "/branches") {
        return branching::handle_branch_list(&ns, tenant, state);
    }

    if let Some(ns) = extract_namespace(request_line, "DELETE /v1/namespaces/", "/branch") {
        return branching::handle_branch_delete(&ns, tenant, state);
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

    // `GET /internal/segment/...` (pod-to-pod) never reaches here: it
    // streams a file, so `dispatch` answers it (`internal_segment_response`)
    // once the internal credential checks out. `route` still answers 403
    // to any other credential.

    // A query pod's strong-consistency search reads the ingest pod's
    // unindexed tail (#255). Authorized like the segment route above.
    if let Some(rest) = request_line.strip_prefix("GET /internal/namespaces/") {
        let target = rest.split(' ').next().unwrap_or("");
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        return match path.strip_suffix("/unindexed") {
            Some(ns) if !ns.is_empty() => {
                unindexed::handle_internal_unindexed(&url_decode(ns), query, state)
            }
            _ => json_error(404, "not found"),
        };
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

/// `GET /v1/namespaces` (optionally with a query string).
fn is_list_namespaces_route(request_line: &str) -> bool {
    parse_method_and_path(request_line).is_some_and(|(method, path)| {
        method == "GET" && path.split('?').next() == Some("/v1/namespaces")
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
    if let Err(response) = check_namespace_quota(state, &id) {
        return response;
    }

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

/// Every other namespace whose manifest lists a segment or deletion
/// bitmap stored under `id`'s prefix (a branch of it, or of one of its
/// branches, #208).
fn branches_referencing(
    state: &AppState,
    id: &NamespaceId,
) -> Result<Vec<NamespaceId>, KoshaError> {
    let below = format!("{}/", id.0);
    let homed_here = |home: &NamespaceId| home == id || home.0.starts_with(&below);
    let mut out = Vec::new();
    let others = state.controller.lock().unwrap().list_namespaces();
    for other in others {
        if other == *id || other.0.starts_with(&below) {
            continue;
        }
        let manifest = state
            .controller
            .lock()
            .unwrap()
            .try_manifest_cloned(&other)?;
        let Some(manifest) = manifest else {
            continue;
        };
        let refers = manifest
            .branch
            .as_ref()
            .is_some_and(|branch| branch.segment_homes.values().any(homed_here))
            || manifest
                .deletion_vectors
                .values()
                .filter_map(|vector| vector.namespace.as_ref())
                .any(homed_here);
        if refers {
            out.push(other);
        }
    }
    Ok(out)
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
    // No branch is created from (or deleted under) this namespace while it
    // goes (#208).
    let _lineage = state.branch_lock.lock().unwrap();
    // Deleting a namespace deletes everything under its prefix. A branch
    // still listing segments or deletion bitmaps that live there would be
    // left pointing at nothing, so the branches go first.
    match branches_referencing(state, &id) {
        Ok(branches) if branches.is_empty() => {}
        Ok(branches) => {
            return json_error(
                409,
                &format!(
                    "namespace '{name}' has branches that share its segments ({}); delete \
                     them first (DELETE /v1/namespaces/{{branch}}/branch)",
                    branches
                        .iter()
                        .map(|b| b.0.split_once('/').map_or(b.0.as_str(), |(_, n)| n))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )
        }
        Err(e) => return json_error(503, &format!("control store: {e}")),
    }
    // A branch goes through DELETE .../branch, which also reclaims the
    // prefixes of deleted ancestor branches only it kept alive; deleting
    // it here would leak those for good.
    match state.controller.lock().unwrap().try_manifest_cloned(&id) {
        Ok(Some(manifest)) if manifest.branch.is_some() => {
            return json_error(
                409,
                &format!(
                    "namespace '{name}' is a branch; delete it with \
                     DELETE /v1/namespaces/{name}/branch"
                ),
            )
        }
        Ok(_) => {}
        Err(e) => return json_error(503, &format!("control store: {e}")),
    }
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
    // A sharded namespace (#207): its shards are namespaces of their own,
    // nested under its prefix (so the local and S3 cleanup below covers
    // their files). Drop them from the control store and the writer too,
    // or their manifests, buffers and WAL records would outlive the root.
    let shard_ids = manifest
        .shards
        .filter(|&n| n > 1)
        .map(|n| kosha_core::sharding::shard_namespaces(&id, n))
        .unwrap_or_default();
    for shard in &shard_ids {
        let shard_local = state.indexer.manifest_version(shard);
        let shard_deleted = state
            .controller
            .lock()
            .unwrap()
            .delete_namespace(shard, shard_local.unwrap_or(0));
        let shard_retired = match shard_deleted {
            Ok((_, retired)) => retired,
            Err(KoshaError::NamespaceNotFound(_)) => shard_local.unwrap_or(0),
            Err(e) => {
                return json_error(
                    500,
                    &format!("delete namespace failed: shard {}: {e}", shard.0),
                )
            }
        };
        let shard_next =
            kosha_core::initial_manifest_version(Some(shard_retired.max(shard_local.unwrap_or(0))));
        if let Err(e) = state.indexer.drop_namespace(shard, shard_next, || {}) {
            return json_error(
                500,
                &format!(
                    "namespace '{name}': discarding the write-ahead log of shard {} failed: {e}",
                    shard.0
                ),
            );
        }
        state.forget_namespace_caches(shard);
    }
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
    let mut documents = request.documents;
    if let Err(response) = check_document_write(state, &ns, &documents) {
        return response;
    }
    if let Err(response) = embed_documents(state, &ns, &mut documents) {
        return response;
    }
    if let Err(response) = check_embedded_dims(state, &ns, &documents) {
        return response;
    }
    match write_routed(state, &ns, documents, index_into) {
        Ok(count) => {
            state.metrics.record_indexed(count as u64);
            json_ok(&IndexResponse {
                indexed_count: count,
                namespace: ns,
            })
        }
        Err(response) => response,
    }
}

/// Native embedding at ingest (#209). For each field the namespace schema
/// declares an `embedding` for, a value that is text (not a JSON number
/// array) is embedded with the declared model and replaced by the vector,
/// typed `Vector`. Runs before the write reaches the indexer, so the WAL,
/// the doc store, replay and compaction only ever see vectors and never
/// need an embedder. A supplied number array must have the declared
/// dimension. One provider call per field per request (the provider
/// batches further).
///
/// A namespace whose schema declares no model returns at once: the cost is
/// the lookup [`kosha_write::Indexer::embedded_fields`] documents.
/// `Err` carries a ready-to-send error response.
fn embed_documents(
    state: &AppState,
    ns: &NamespaceId,
    documents: &mut [kosha_core::Document],
) -> Result<(), String> {
    let Some(fields) = state.indexer.embedded_fields(ns) else {
        return Ok(());
    };
    for (field, spec) in &fields {
        // (document, field) positions of the texts to embed.
        let mut positions: Vec<(usize, usize)> = Vec::new();
        for (d, doc) in documents.iter().enumerate() {
            for (f, value) in doc.fields.iter().enumerate() {
                if value.name != *field {
                    continue;
                }
                match embedding::supplied_vector(&value.value) {
                    Some(Ok(vector)) if vector.len() != spec.dims as usize => {
                        return Err(json_error(
                            400,
                            &format!(
                                "document {:?}: field {field:?} has a {}-dimensional vector, \
                                 but its embedding model {} has {} dimensions",
                                doc.id.0,
                                vector.len(),
                                spec.model,
                                spec.dims
                            ),
                        ));
                    }
                    // A vector already, or a bag of them: the vector path
                    // takes it as it always has.
                    Some(_) => {}
                    None => positions.push((d, f)),
                }
            }
        }
        if positions.is_empty() {
            continue;
        }
        let embedder = state
            .embedders
            .get(spec)
            .map_err(|e| json_error(e.status(), e.message()))?;
        let texts: Vec<&str> = positions
            .iter()
            .map(|&(d, f)| documents[d].fields[f].value.as_str())
            .collect();
        let vectors = embedding::embed_checked(embedder.as_ref(), spec, &texts)
            .map_err(|e| json_error(e.status(), e.message()))?;
        for ((d, f), vector) in positions.into_iter().zip(vectors) {
            let value = &mut documents[d].fields[f];
            value.value = serde_json::to_string(&vector).unwrap_or_default();
            value.field_type = kosha_core::FieldType::Vector;
        }
    }
    Ok(())
}

/// Native embedding at query time (#209): replace `knn.query_text` with
/// the vector of the model the namespace schema declares for `knn.field`,
/// and record the model so segments written under another one are skipped
/// (see `kosha_query::segment_embedding_model_mismatch`). Only a kNN that
/// carries `query_text` reaches the schema or an embedder. `Err` carries a
/// ready-to-send error response.
fn resolve_knn_query_text(
    state: &AppState,
    ns: &NamespaceId,
    max_staleness: Option<Duration>,
    query: &mut kosha_core::SearchQuery,
) -> Result<(), String> {
    let Some(knn) = query.knn.as_mut() else {
        return Ok(());
    };
    let Some(text) = knn.query_text.take() else {
        return Ok(());
    };
    // A query replica embeds with the schema it follows: bring it current
    // first (the search below does the same and is then a no-op).
    state
        .ensure_query_manifest_fresh(ns, max_staleness)
        .map_err(|e| search_error_response(&e))?;
    let Some(manifest) = state.indexer.manifest_shared(ns) else {
        return Err(json_error(404, &format!("namespace '{}' not found", ns.0)));
    };
    let Some(spec) = manifest
        .schema
        .as_ref()
        .and_then(|schema| schema.embedding_for(&knn.field))
        .cloned()
    else {
        return Err(json_error(
            400,
            &format!(
                "knn.query_text needs an embedding model for field '{}', and the namespace \
                 schema declares none; send knn.vector instead",
                knn.field
            ),
        ));
    };
    let embedder = state
        .embedders
        .get(&spec)
        .map_err(|e| json_error(e.status(), e.message()))?;
    let mut vectors = embedding::embed_checked(embedder.as_ref(), &spec, &[text.as_str()])
        .map_err(|e| json_error(e.status(), e.message()))?;
    knn.vector = vectors.pop().unwrap_or_default();
    knn.embedding_model = Some(spec.model);
    Ok(())
}

/// Group documents by the shard their id routes to (#207), keeping each
/// shard's documents in request order.
fn route_to_shards(
    ns: &NamespaceId,
    shards: u32,
    documents: Vec<kosha_core::Document>,
) -> Vec<(NamespaceId, Vec<kosha_core::Document>)> {
    let mut grouped: std::collections::BTreeMap<u32, Vec<kosha_core::Document>> =
        Default::default();
    for doc in documents {
        let shard = kosha_core::sharding::shard_for_document(&doc.id.0, shards);
        grouped.entry(shard).or_default().push(doc);
    }
    grouped
        .into_iter()
        .map(|(shard, docs)| (kosha_core::sharding::shard_namespace(ns, shard), docs))
        .collect()
}

/// Apply a document write to `ns`, or — for a sharded namespace (#207) — to
/// each shard its documents route to, publishing each shard. Shards are
/// written one after another: a failure part-way leaves earlier shards
/// written, which a client retry of the whole (idempotent, id-keyed) batch
/// repairs. Returns how many documents were written.
fn write_routed(
    state: &AppState,
    ns: &NamespaceId,
    documents: Vec<kosha_core::Document>,
    write: fn(&AppState, &NamespaceId, Vec<kosha_core::Document>) -> Result<usize, String>,
) -> Result<usize, String> {
    let Some(shards) = state.indexer.shard_count(ns) else {
        return write(state, ns, documents);
    };
    let mut total = 0;
    for (shard, docs) in route_to_shards(ns, shards, documents) {
        total += write(state, &shard, docs)?;
    }
    Ok(total)
}

/// `POST /index` for one (unsharded or shard) namespace. `Err` carries a
/// ready-to-send error response.
fn index_into(
    state: &AppState,
    ns: &NamespaceId,
    documents: Vec<kosha_core::Document>,
) -> Result<usize, String> {
    let ns = ns.clone();
    // Only the namespace's writer-lease holder accepts writes (#254).
    state.ensure_writable(&ns)?;
    // Unindexed backlog at its cap (#255): refuse before buffering more.
    // Per written namespace, so a sharded namespace caps each shard.
    if let Some(response) = state.unindexed_backpressure(&ns) {
        return Err(response);
    }

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
                .map(|entry| manifest.segment_dir(&state.data_dir, &ns, &entry.segment_id))
                .collect();
            // Rewrite/lookup paths must see a segment's full fidelity,
            // including vectors — unlike the search path, there's no
            // per-call signal here for whether vectors matter, so always
            // fetch them.
            let outcome = state.ensure_segments_local(&paths, true);
            if let Some(error) = hydration_error_response(&outcome) {
                return Err(error);
            }
        }
    }

    let (count, manifest_changed) = {
        let indexer = &state.indexer;
        let version_before = indexer.manifest(&ns).map(|m| m.version);
        let count = match indexer.index_documents(ns.clone(), documents) {
            Ok(c) => c,
            // A rejected document (e.g. a vector of the wrong dimension
            // for its field, #204) is the client's error.
            Err(KoshaError::InvalidQuery(message)) => return Err(json_error(400, &message)),
            Err(e) => return Err(json_error(500, &format!("indexing error: {e}"))),
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
            return Err(publication_error("publication failed", &e));
        }
    }

    Ok(count)
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

    let ids: Vec<kosha_core::DocumentId> = request
        .ids
        .iter()
        .cloned()
        .map(kosha_core::DocumentId)
        .collect();
    // A sharded namespace (#207): each id can only live in the shard it
    // routes to, so ask each shard about its own ids.
    let existing = match state.indexer.shard_count(&request.namespace) {
        None => match existing_ids_in(state, &request.namespace, &ids) {
            Ok(set) => set,
            Err(response) => return response,
        },
        Some(shards) => {
            let mut grouped: std::collections::BTreeMap<u32, Vec<kosha_core::DocumentId>> =
                Default::default();
            for id in ids {
                let shard = kosha_core::sharding::shard_for_document(&id.0, shards);
                grouped.entry(shard).or_default().push(id);
            }
            let mut existing = std::collections::HashSet::new();
            for (shard, ids) in grouped {
                let shard = kosha_core::sharding::shard_namespace(&request.namespace, shard);
                if state.indexer.manifest_version(&shard).is_none() {
                    continue;
                }
                match existing_ids_in(state, &shard, &ids) {
                    Ok(set) => existing.extend(set),
                    Err(response) => return response,
                }
            }
            existing
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

/// The subset of `ids` live in one (unsharded or shard) namespace. `Err`
/// carries a ready-to-send error response.
fn existing_ids_in(
    state: &AppState,
    namespace: &NamespaceId,
    ids: &[kosha_core::DocumentId],
) -> Result<std::collections::HashSet<kosha_core::DocumentId>, String> {
    // Existence scans immutable segments, so materialize any S3-backed
    // segment directories before entering the indexer lock. Batched across
    // the whole manifest — see handle_index for why this isn't a per-segment
    // loop.
    #[cfg(feature = "s3")]
    {
        if let Some(manifest) = state.indexer.manifest_cloned(namespace) {
            let paths: Vec<PathBuf> = manifest
                .segments
                .iter()
                .map(|entry| manifest.segment_dir(&state.data_dir, namespace, &entry.segment_id))
                .collect();
            // Rewrite/lookup paths must see a segment's full fidelity,
            // including vectors — unlike the search path, there's no
            // per-call signal here for whether vectors matter, so always
            // fetch them.
            let outcome = state.ensure_segments_local(&paths, true);
            if let Some(error) = hydration_error_response(&outcome) {
                return Err(error);
            }
        }
    }
    state
        .indexer
        .existing_ids(namespace, ids)
        .map_err(|e| json_error(500, &format!("exists error: {e}")))
}

// ─── POST /replace ──────────────────────────────────────────────────────────

fn handle_replace(body: &[u8], state: &AppState) -> String {
    let request: IndexRequest = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(error) => return json_error(400, &format!("invalid JSON: {error}")),
    };
    let namespace = request.namespace;
    let mut documents = request.documents;
    if let Err(response) = check_document_write(state, &namespace, &documents) {
        return response;
    }
    if let Err(response) = embed_documents(state, &namespace, &mut documents) {
        return response;
    }
    if let Err(response) = check_embedded_dims(state, &namespace, &documents) {
        return response;
    }
    match write_routed(state, &namespace, documents, replace_into) {
        Ok(count) => {
            state.metrics.record_indexed(count as u64);
            json_ok(&IndexResponse {
                indexed_count: count,
                namespace,
            })
        }
        Err(response) => response,
    }
}

/// `POST /replace` for one (unsharded or shard) namespace. `Err` carries a
/// ready-to-send error response.
fn replace_into(
    state: &AppState,
    namespace: &NamespaceId,
    documents: Vec<kosha_core::Document>,
) -> Result<usize, String> {
    let namespace = namespace.clone();
    state.ensure_writable(&namespace)?;
    if let Some(response) = state.unindexed_backpressure(&namespace) {
        return Err(response);
    }

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
                .map(|entry| manifest.segment_dir(&state.data_dir, &namespace, &entry.segment_id))
                .collect();
            // Rewrite/lookup paths must see a segment's full fidelity,
            // including vectors — unlike the search path, there's no
            // per-call signal here for whether vectors matter, so always
            // fetch them.
            let outcome = state.ensure_segments_local(&paths, true);
            if let Some(error) = hydration_error_response(&outcome) {
                return Err(error);
            }
        }
    }

    let count = {
        let indexer = &state.indexer;
        match indexer.replace_documents(namespace.clone(), documents) {
            Ok(count) => count,
            Err(KoshaError::InvalidQuery(message)) => return Err(json_error(400, &message)),
            Err(error) => return Err(json_error(500, &format!("replacement error: {error}"))),
        }
    };
    {
        let mut controller = state.controller.lock().unwrap();
        controller.ensure_namespace(namespace.clone());
    }
    if let Err(e) = state.publish_namespace(&namespace) {
        return Err(publication_error("publication failed", &e));
    }

    Ok(count)
}

// ─── POST /flush ────────────────────────────────────────────────────────────

/// Make one namespace's pending writes visible: recovery-hydrate if a WAL
/// replay needs the flushed segments local, cut a segment from the write
/// buffer, then publish (S3 upload + manifest CAS). The single-namespace
/// body of `POST /flush`, shared with the idle-buffer flush timer
/// (`spawn_idle_flush_timer`) so both take exactly the same path. `Err`
/// carries a ready-to-send JSON error response.
fn flush_and_publish_namespace(state: &AppState, namespace: &NamespaceId) -> Result<(), String> {
    // A sharded namespace (#207) flushes and publishes each shard; the
    // root itself never buffers documents.
    if let Some(shards) = state.indexer.shard_count(namespace) {
        for shard in kosha_core::sharding::shard_namespaces(namespace, shards) {
            if state.indexer.manifest_version(&shard).is_some()
                || state.indexer.pending_doc_count(&shard) > 0
            {
                flush_and_publish_namespace(state, &shard)?;
            }
        }
        return Ok(());
    }
    #[cfg(feature = "s3")]
    if state.indexer.needs_recovery_hydration(namespace) {
        if let Some(manifest) = state.indexer.manifest_cloned(namespace) {
            let paths: Vec<_> = manifest
                .segments
                .iter()
                .map(|e| manifest.segment_dir(&state.data_dir, namespace, &e.segment_id))
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
            Ok(()) => {
                state.metrics.record_flush();
                json_ok(&serde_json::json!({"status": "flushed"}))
            }
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
                    .map(|e| manifest.segment_dir(&state.data_dir, &namespace, &e.segment_id))
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

    state.metrics.record_flush();
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

    let deleted = match state.indexer.shard_count(&ns) {
        None => delete_in(state, &ns, &filter),
        // A sharded namespace (#207): delete in every shard that exists.
        Some(shards) => kosha_core::sharding::shard_namespaces(&ns, shards)
            .into_iter()
            .filter(|shard| state.indexer.manifest_version(shard).is_some())
            .try_fold(0, |total, shard| {
                delete_in(state, &shard, &filter).map(|count| total + count)
            }),
    };
    match deleted {
        Ok(count) => json_ok(&serde_json::json!({
            "deleted": count,
            "namespace": ns.0,
        })),
        Err(response) => response,
    }
}

/// Delete by query in one (unsharded or shard) namespace and publish.
/// `Err` carries a ready-to-send error response.
fn delete_in(
    state: &AppState,
    ns: &NamespaceId,
    filter: &kosha_core::FilterClause,
) -> Result<usize, String> {
    state.ensure_writable(ns)?;
    #[cfg(feature = "s3")]
    if let Some(manifest) = state.indexer.manifest_cloned(ns) {
        let paths: Vec<PathBuf> = manifest
            .segments
            .iter()
            .map(|e| manifest.segment_dir(&state.data_dir, ns, &e.segment_id))
            .collect();
        let outcome = state.ensure_segments_local(&paths, false);
        if let Some(error) = hydration_error_response(&outcome) {
            return Err(error);
        }
    }

    let manifest = match state.indexer.manifest_cloned(ns) {
        Some(m) => m,
        None => return Err(json_error(404, &format!("namespace '{}' not found", ns.0))),
    };
    let count = state
        .indexer
        .delete_by_query(ns, &manifest, filter)
        .map_err(|e| match e {
            // A filter the data cannot answer (e.g. `prefix` on a numeric
            // field) is the client's mistake, not a server fault.
            KoshaError::InvalidQuery(_) => json_error(400, &e.to_string()),
            e => json_error(500, &format!("delete error: {e}")),
        })?;

    if let Err(e) = state.publish_namespace(ns) {
        return Err(publication_error("deletion publication failed", &e));
    }
    Ok(count)
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
            // A sharded root (#207) lists no segments itself; its shards
            // appear as their own `{root}/_shard-XXXX` entries.
            if let Some(shards) = manifest.shards {
                entry["shards"] = serde_json::json!(shards);
            }
            if state.ingest_host.is_some() {
                add_manifest_follow_stats(state, &ns, &mut entry);
            } else {
                add_unindexed_stats(state, &ns, &mut entry);
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
        // Process-wide count of scoring blocks varint-decoded (#385); a
        // bench diffs it around a run for blocks decoded per query.
        "scoring_blocks_decoded": kosha_segment::scoring_blocks_decoded(),
        "manifest_refresh": state
            .ingest_host
            .is_some()
            .then(|| state.manifest_follow.summary_json(Instant::now())),
        "gc": state.gc_stats.to_json(
            state.indexer.retired_segment_count(),
            state.indexer.local_retired_count(),
        ),
        "rerank": state.rerankers.stats_json(),
        "embedding_providers": state.embedders.providers(),
    }))
}

/// The writer's unindexed backlog of a namespace (#255): what strong
/// searches scan exhaustively and what `KOSHA_MAX_UNINDEXED_*` caps.
fn add_unindexed_stats(state: &AppState, ns: &NamespaceId, entry: &mut serde_json::Value) {
    let (docs, bytes) = state.indexer.unindexed_usage(ns);
    entry["unindexed_documents"] = serde_json::json!(docs);
    entry["unindexed_bytes"] = serde_json::json!(bytes);
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
    observe_search(state, |obs| search_post(body, state, obs))
}

fn search_post(body: &[u8], state: &AppState, obs: &SearchObservation) -> String {
    let t_request = Instant::now();
    // Parse the full JSON body that includes namespace alongside SearchQuery fields.
    let body_val: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };
    // `"timings": true` asks for the phase breakdown in the response, while
    // the dashboard is on (#214). Otherwise ignored, like any unknown key.
    let want_timings = state.dashboard.enabled()
        && body_val.get("timings") == Some(&serde_json::Value::Bool(true));

    let ns = match body_val.get("namespace").and_then(|v| v.as_str()) {
        Some(n) => NamespaceId(n.to_string()),
        None => return json_error(400, "missing 'namespace' in search body"),
    };
    obs.namespace(state, &ns);

    // Query replicas only (#232): how stale a manifest this search accepts.
    let max_staleness =
        match manifest_follow::parse_max_staleness(body_val.get("max_staleness_seconds")) {
            Ok(v) => v,
            Err(e) => return json_error(400, &e),
        };

    // Read-your-writes (#255): whether to also search unindexed writes.
    let consistency = match unindexed::Consistency::from_json(body_val.get("consistency")) {
        Ok(c) => c.unwrap_or_else(|| state.default_consistency()),
        Err(e) => return json_error(400, &e),
    };

    let mut query: kosha_core::SearchQuery = match serde_json::from_value(body_val) {
        Ok(q) => q,
        Err(e) => return json_error(400, &format!("invalid search query: {e}")),
    };
    // Reject out-of-range parameters before touching storage or the
    // result cache (the searcher checks again for non-HTTP callers).
    if let Err(e) = query.validate() {
        return search_error_response(&e);
    }
    // `knn.query_text` becomes `knn.vector` here (#209), before the cache
    // key and the searcher see the query.
    if let Err(response) = resolve_knn_query_text(state, &ns, max_staleness, &mut query) {
        return response;
    }
    let query = query;
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

    // Shared, not copied: see `Indexer::manifest_shared`. A sharded
    // namespace (#207) is searched through one merged view of its shards.
    // Under strong consistency `view.search` is the manifest plus the
    // namespace's (or each shard's) unindexed tail (#255); hydration and
    // every manifest-version keyed cache use the plain snapshot,
    // `view.base`.
    let view = match state.search_view(&ns, consistency, max_staleness) {
        Ok(Some(view)) => view,
        Ok(None) => return json_error(404, &format!("namespace '{}' not found", ns.0)),
        Err(response) => return response,
    };
    let manifest: &kosha_core::Manifest = &view.base;
    let searched: &kosha_core::Manifest = &view.search;
    let tombstones = Some(&searched.tombstones);

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
    // A search that included unindexed writes (#255) answers for more than
    // `manifest.version`, so it neither reads nor fills the cache.
    let cache_key = if result_cache_budget() > 0
        && !query.no_cache.unwrap_or(false)
        && !time_dependent
        && view.is_plain()
    {
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
            if state.dashboard.enabled() {
                let body = response_body_json(&resp);
                let hits = body["results"].as_array().map_or(0, Vec::len);
                let degraded = body["knn_degraded_segments"].as_u64().unwrap_or(0) > 0;
                // A kNN-degraded answer is never inserted (see `cacheable`
                // below), but a hit must not claim a degraded body warm
                // either way.
                let timings = dashboard::SearchTimings {
                    total_ms: t_request.elapsed().as_secs_f64() * 1e3,
                    cached: true,
                    fully_cached: !degraded,
                    ..Default::default()
                };
                state
                    .dashboard
                    .record(&ns.0, dashboard::QuerySample::new(&timings, hits, degraded));
                if want_timings {
                    return with_search_timings(body, &timings);
                }
            }
            return resp;
        }
    }

    // One accountant for everything this request hydrates — footer prefetch,
    // scoring set, posting blobs, and the page's doc stores all share it, so
    // the ceiling covers the request's whole cost rather than each batch's.
    // See `hydration_budget`.
    #[cfg(feature = "s3")]
    let hydration_request = Arc::new(RequestHydration::new(state.hydrate_request_max_bytes));
    // Hold this search's segments in the disk cache until it returns: once
    // the working set reaches KOSHA_CACHE_MAX_BYTES, files fetched for it
    // otherwise evicted files it had just checked, and it failed with
    // "segment hydration failed".
    let _segment_pins = state.pin_segments(&ns, manifest);
    // Read stamp for the disk-cache policy (#389): files this search
    // hydrates do not count its own read as a re-read. 0 when off.
    let cache_read_at = state.begin_cache_read();
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
            state.record_warm_nvme_hits(&ns, manifest);
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
    let Some(slot) = obs.scoring_slot(state) else {
        // Shed rather than queue: see `FairSemaphore::try_acquire`.
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
        searched,
        search_query,
        tombstones,
        doc_store_hydrator,
        vector_fetch,
    ) {
        Ok((mut result, phases)) => {
            // Scoring is done; a reranker round trip must not hold a
            // scoring slot.
            drop(slot);
            result.exhaustive_search_count = view.exhaustive_search_count();
            result.consistency_fallback = view.fallback.clone();
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
            state.metrics.record_search_phases(
                hydrate_ms,
                queue_ms,
                phases.score_wall_ms,
                result.knn_degraded_segments.unwrap_or(0),
            );
            state.record_search_access(&ns, manifest, search_query, &phases, &hydration);
            state.record_cache_reads(&ns, manifest, search_query, &phases, cache_read_at);
            // Before a rerank replaces `result`.
            let knn_degraded = result.knn_degraded_segments.unwrap_or(0);
            if let Some(ref provider) = reranker {
                let (reranked, call) = kosha_query::rerank::apply(&query, result, provider);
                result = reranked;
                if let Some(call) = call {
                    log_rerank(&ns, &call);
                    state.rerankers.record(&call);
                }
            }
            let timings = dashboard::SearchTimings {
                total_ms: t_request.elapsed().as_secs_f64() * 1e3,
                hydrate_ms,
                queue_ms,
                admit_ms: phases.admit_wall_ms,
                bm25_stats_ms: phases.bm25_stats_ms,
                score_ms: phases.score_wall_ms,
                materialize_ms: phases.materialize_wall_ms,
                cached: false,
                s3_rounds: hydration.s3_rounds(),
                s3_bytes: hydration.s3_bytes(),
                fully_cached: search_fully_cached(&hydration, &phases, knn_degraded),
                knn_postings: phases.knn_mode.map(|_| phases.knn_postings_probed),
                knn_cutoff_rule: phases.knn_cutoff_rule.map(|rule| rule.as_str()),
            };
            state.dashboard.record(
                &ns.0,
                dashboard::QuerySample::new(
                    &timings,
                    result.results.len(),
                    result.knn_degraded_segments.unwrap_or(0) > 0,
                ),
            );
            let resp = json_ok(&result);
            // Only successful responses enter the cache, after the search
            // proved every needed file readable. A search that fell back to
            // per-segment BM25 statistics (issue #246) because some
            // segment's footer or postings weren't local isn't cached
            // either: the miss is usually transient (best-effort stats
            // hydration), and caching it would pin the degraded scores for
            // the rest of this manifest version. Nor is a degraded rerank
            // (a transient fallback that would serve the unreranked order
            // until the namespace next changes), nor a kNN answer that
            // skipped a segment (usually a `vector.idx` not yet local):
            // pinned, it would serve the partial answer, and read as fully
            // cached to a warmup gate, until the namespace next changes.
            let cacheable = phases.bm25_stats != kosha_query::Bm25StatsUsed::SegmentFallback
                && result.rerank_degraded != Some(true)
                && knn_degraded == 0;
            if let Some(key) = cache_key.filter(|_| cacheable) {
                state.result_cache.lock().unwrap().insert(key, &resp);
            }
            if want_timings {
                return with_search_timings(response_body_json(&resp), &timings);
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

/// The JSON body of a response this process built (`Null` if none).
fn response_body_json(response: &str) -> serde_json::Value {
    response
        .split_once("\r\n\r\n")
        .and_then(|(_, body)| serde_json::from_str(body).ok())
        .unwrap_or(serde_json::Value::Null)
}

/// A search response with `timings` added (#214). Built per request, after
/// the result cache stored the plain response.
fn with_search_timings(mut body: serde_json::Value, timings: &dashboard::SearchTimings) -> String {
    if let Some(obj) = body.as_object_mut() {
        obj.insert("timings".into(), serde_json::json!(timings));
    }
    json_ok(&body)
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
///
/// Tenants (#342): the key's namespace is the full `<tenant>/<name>` id,
/// so two tenants with the same namespace name and query never share an
/// entry. Bytes are accounted per tenant, and eviction takes the least
/// recently used entry of the tenant furthest over its weighted share of
/// the budget (see [`kosha_core::tenancy::TenantShares`]).
struct ResultCache {
    entries: HashMap<(String, u64, String), ResultCacheEntry>,
    bytes: usize,
    budget: usize,
    tick: u64,
    hits: u64,
    misses: u64,
    tenants: kosha_core::tenancy::TenantLedger,
    shares: kosha_core::tenancy::TenantShares,
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
            tenants: Default::default(),
            shares: kosha_core::tenancy::TenantShares::from_env(),
        }
    }

    fn lookup(&mut self, key: &(String, u64, String)) -> Option<String> {
        self.tick += 1;
        let tick = self.tick;
        let tenant = kosha_core::tenancy::tenant_of_namespace(&key.0);
        if let Some(e) = self.entries.get_mut(key) {
            e.last_used = tick;
            self.hits += 1;
            self.tenants.hit(tenant);
            Some(e.resp.clone())
        } else {
            self.misses += 1;
            self.tenants.miss(tenant);
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
        self.tenants.sub_bytes(
            kosha_core::tenancy::tenant_of_namespace(namespace),
            freed as u64,
        );
    }

    /// The next entry to evict: the least recently used entry of the
    /// tenant furthest over its share (#342).
    fn next_victim(&self) -> Option<(String, u64, String)> {
        let mut oldest: HashMap<&str, (&(String, u64, String), &ResultCacheEntry)> = HashMap::new();
        for (key, entry) in &self.entries {
            let tenant = kosha_core::tenancy::tenant_of_namespace(&key.0);
            let slot = oldest.entry(tenant).or_insert((key, entry));
            if entry.last_used < slot.1.last_used {
                *slot = (key, entry);
            }
        }
        let usage = self.tenants.usage();
        self.shares
            .pick_victim(self.budget as u64, &usage, |t| {
                oldest.get(t).map(|(_, e)| e.bytes as u64)
            })
            .and_then(|t| oldest.get(t))
            .or_else(|| oldest.values().min_by_key(|(_, e)| e.last_used))
            .map(|(k, _)| (*k).clone())
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
        let tenant = kosha_core::tenancy::tenant_of_namespace(&key.0).to_string();
        if let Some(old) = self.entries.remove(&key) {
            self.bytes -= old.bytes;
            self.tenants.sub_bytes(&tenant, old.bytes as u64);
        }
        self.tick += 1;
        self.bytes += entry_bytes;
        self.tenants.add_bytes(&tenant, entry_bytes as u64);
        self.entries.insert(
            key,
            ResultCacheEntry {
                resp: resp.to_owned(),
                bytes: entry_bytes,
                last_used: self.tick,
            },
        );
        while self.bytes > self.budget {
            let Some(victim) = self.next_victim() else {
                break;
            };
            if let Some(e) = self.entries.remove(&victim) {
                self.bytes -= e.bytes;
                let tenant = kosha_core::tenancy::tenant_of_namespace(&victim.0);
                self.tenants.sub_bytes(tenant, e.bytes as u64);
                self.tenants.evicted(tenant);
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
    observe_search(state, |obs| search_get(request_line, state, obs))
}

fn search_get(request_line: &str, state: &AppState, obs: &SearchObservation) -> String {
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
    obs.namespace(state, &ns);

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
    let consistency = match params.get("consistency") {
        None => state.default_consistency(),
        Some(raw) => match unindexed::Consistency::parse(raw) {
            Some(c) => c,
            None => {
                return json_error(
                    400,
                    &format!("'consistency' must be 'strong' or 'eventual', got '{raw}'"),
                )
            }
        },
    };

    let max_staleness = match manifest_follow::parse_max_staleness(
        params
            .get("max_staleness_seconds")
            .map(|v| serde_json::Value::String(v.clone()))
            .as_ref(),
    ) {
        Ok(v) => v,
        Err(e) => return json_error(400, &e),
    };
    // Shared, not copied: see `Indexer::manifest_shared`. A sharded
    // namespace (#207) is searched through one merged view of its shards.
    // Under strong consistency `view.search` is the manifest plus the
    // namespace's (or each shard's) unindexed tail (#255); hydration and
    // every manifest-version keyed cache use the plain snapshot,
    // `view.base`.
    let view = match state.search_view(&ns, consistency, max_staleness) {
        Ok(Some(view)) => view,
        Ok(None) => return json_error(404, &format!("namespace '{}' not found", ns.0)),
        Err(response) => return response,
    };
    let manifest: &kosha_core::Manifest = &view.base;
    let searched: &kosha_core::Manifest = &view.search;
    let tombstones = Some(&searched.tombstones);

    // One accountant for everything this request hydrates — footer prefetch,
    // scoring set, posting blobs, and the page's doc stores all share it, so
    // the ceiling covers the request's whole cost rather than each batch's.
    // See `hydration_budget`.
    #[cfg(feature = "s3")]
    let hydration_request = Arc::new(RequestHydration::new(state.hydrate_request_max_bytes));
    // Hold this search's segments in the disk cache until it returns: once
    // the working set reaches KOSHA_CACHE_MAX_BYTES, files fetched for it
    // otherwise evicted files it had just checked, and it failed with
    // "segment hydration failed".
    let _segment_pins = state.pin_segments(&ns, manifest);

    let cache_read_at = state.begin_cache_read();
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
            state.record_warm_nvme_hits(&ns, manifest);
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
    let Some(_slot) = obs.scoring_slot(state) else {
        // Shed rather than queue: see `FairSemaphore::try_acquire`.
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
        searched,
        &query,
        tombstones,
        doc_store_hydrator,
        vector_fetch,
    ) {
        Ok((mut result, phases)) => {
            result.exhaustive_search_count = view.exhaustive_search_count();
            result.consistency_fallback = view.fallback.clone();
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
            state.metrics.record_search_phases(
                hydrate_ms,
                queue_ms,
                phases.score_wall_ms,
                result.knn_degraded_segments.unwrap_or(0),
            );
            state.record_search_access(&ns, manifest, &query, &phases, &hydration);
            state.record_cache_reads(&ns, manifest, &query, &phases, cache_read_at);
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
/// analysis settings and vector fields' `embedding` (#209) are honored. New
/// segments use it; existing segments keep the analysis their footer
/// records and stay searchable with it, so the change reaches old
/// documents when compaction rewrites them. Changing the embedding model of
/// a field that already holds vectors is a 409 (see
/// `kosha_write::Indexer::embedding_conflict`).
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
    if let Err(e) = schema.validate_embeddings() {
        return json_error(400, &format!("invalid schema: {e}"));
    }
    // Model pinning (#209): a field's embedding model cannot change while
    // vectors of the old one are indexed. Checked on every shard before
    // any manifest changes, so a refused change leaves none half-applied.
    let mut pinned = vec![ns.clone()];
    if let Some(shards) = state.indexer.shard_count(&ns) {
        pinned.extend(kosha_core::sharding::shard_namespaces(&ns, shards));
    }
    for target in &pinned {
        if let Some(conflict) = state.indexer.embedding_conflict(target, &schema) {
            return json_error(409, &conflict);
        }
    }
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
    // A sharded namespace (#207): every shard writes segments, so every
    // shard's manifest carries the schema too. The root's copy is the one
    // `GET .../schema` reports.
    if let Some(shards) = state.indexer.shard_count(&ns) {
        if let Err(response) = propagate_schema_to_shards(state, &ns, shards, &schema) {
            return response;
        }
    }
    json_ok(&serde_json::json!({
        "namespace": namespace,
        "schema": schema,
        "changed": changed,
    }))
}

/// Set `schema` on every shard of sharded root `ns` and publish the shards
/// it changed. `Err` carries a ready-to-send error response.
fn propagate_schema_to_shards(
    state: &AppState,
    ns: &NamespaceId,
    shards: u32,
    schema: &kosha_core::NamespaceSchema,
) -> Result<(), String> {
    for shard in kosha_core::sharding::shard_namespaces(ns, shards) {
        let changed = state
            .indexer
            .set_schema(&shard, schema.clone())
            .map_err(|e| json_error(500, &format!("schema update failed: {e}")))?;
        if changed {
            state
                .publish_namespace(&shard)
                .map_err(|e| json_error(500, &format!("publication failed: {e}")))?;
        }
    }
    Ok(())
}

/// `POST /v1/namespaces/{ns}/shards` — create `{ns}` as a sharded namespace
/// with a fixed shard count (#207): `{"shards": N}`, `2 <= N <= 256`.
///
/// Only a namespace holding no documents yet can be sharded (it is the
/// creation-time setting; resharding is not supported). Repeating the call
/// with the same count is a no-op; a different count is a 400. `1` asks
/// for an ordinary namespace and changes nothing.
///
/// Every shard namespace is registered in the control store up front, so
/// query replicas follow all of them from the first search, and inherits
/// the root's schema if one was set first.
fn handle_put_shards_with_ns(
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
    let Some(shards) = value.get("shards").and_then(|v| v.as_u64()) else {
        return json_error(400, "missing integer 'shards'");
    };
    if shards == 1 {
        return match state.indexer.shard_count(&ns) {
            Some(existing) => json_error(
                400,
                &format!("namespace already has {existing} shards; resharding is not supported"),
            ),
            None => json_ok(&serde_json::json!({
                "namespace": namespace,
                "shards": 1,
                "changed": false,
            })),
        };
    }
    let shards = match u32::try_from(shards) {
        Ok(n) => n,
        Err(_) => return json_error(400, "shards out of range"),
    };
    let changed = match state.indexer.set_shard_count(&ns, shards) {
        Ok(changed) => changed,
        Err(KoshaError::InvalidQuery(message)) => return json_error(400, &message),
        Err(e) => return json_error(500, &format!("sharding failed: {e}")),
    };
    let shard_ids = kosha_core::sharding::shard_namespaces(&ns, shards);
    {
        let mut controller = state.controller.lock().unwrap();
        for shard in &shard_ids {
            controller.ensure_namespace(shard.clone());
        }
        controller.ensure_namespace(ns.clone());
    }
    for shard in &shard_ids {
        if state.indexer.manifest_version(shard).is_none() {
            let stored = state.controller.lock().unwrap().try_manifest_cloned(shard);
            if let Ok(Some(stored)) = stored {
                state.indexer.restore_manifest(shard.clone(), stored);
            }
        }
    }
    // Unconditionally: a retry after a failed publish (`changed` is then
    // false) must still commit the shard count. Publishing an already
    // committed manifest is a no-op.
    if let Err(e) = state.publish_namespace(&ns) {
        return publication_error("publication failed", &e);
    }
    if let Some(schema) = state
        .indexer
        .manifest_shared(&ns)
        .and_then(|m| m.schema.clone())
    {
        if let Err(response) = propagate_schema_to_shards(state, &ns, shards, &schema) {
            return response;
        }
    }
    json_ok(&serde_json::json!({
        "namespace": namespace,
        "shards": shards,
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
    // Observed here rather than through `handle_search_post`, so a body
    // rejected before it reaches the search is still counted.
    observe_search(state, |obs| {
        search_post_with_ns(namespace, tenant, body, state, obs)
    })
}

fn search_post_with_ns(
    namespace: &str,
    tenant: &str,
    body: &[u8],
    state: &AppState,
    obs: &SearchObservation,
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
    search_post(&modified_body, state, obs)
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

/// `GET /v1/namespaces` (#214): the caller's namespaces, from the control
/// store, with the `{tenant}/` prefix stripped. Shard namespaces
/// (`{ns}/_shard-XXXX`) and anything else nested under a namespace are
/// internal and left out; a sharded namespace reports totals over its
/// shards, as its stats do. Counts are `null` for a namespace whose
/// manifest this pod cannot load right now.
fn handle_list_namespaces(tenant: &str, state: &AppState) -> String {
    let prefix = format!("{tenant}/");
    let all = state.controller.lock().unwrap().list_namespaces();
    let mut names: Vec<String> = all
        .into_iter()
        .filter_map(|ns| ns.0.strip_prefix(&prefix).map(str::to_string))
        .filter(|name| !name.is_empty() && !name.contains('/'))
        .collect();
    names.sort_unstable();
    names.dedup();
    let namespaces: Vec<serde_json::Value> = names
        .into_iter()
        .map(|name| {
            let id = NamespaceId(tenant_namespace(tenant, &name));
            // A query replica loads a namespace it has not served yet.
            let manifest = state
                .ensure_query_manifest_fresh(&id, None)
                .ok()
                .and_then(|()| state.indexer.manifest_shared(&id))
                .map(|m| match state.indexer.shard_count(&id) {
                    Some(_) => state.sharded_view(&id, m),
                    None => m,
                });
            match manifest {
                Some(m) => serde_json::json!({
                    "name": name,
                    "documents": m.segments.iter().map(|s| s.doc_count as u64).sum::<u64>(),
                    "segments": m.segments.len(),
                    "manifest_version": m.version,
                }),
                None => serde_json::json!({
                    "name": name,
                    "documents": null,
                    "segments": null,
                    "manifest_version": null,
                }),
            }
        })
        .collect();
    json_ok(&serde_json::json!({ "namespaces": namespaces }))
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
    let root_id = NamespaceId(scoped_ns.clone());
    let manifest = match indexer.manifest_shared(&root_id) {
        Some(m) => m,
        None => return json_error(404, &format!("namespace '{namespace}' not found")),
    };
    // A sharded namespace (#207) reports totals over its merged view, plus
    // a per-shard breakdown.
    let shards = indexer.shard_count(&root_id);
    let manifest = match shards {
        Some(_) => state.sharded_view(&root_id, manifest),
        None => manifest,
    };
    let ns_docs: u64 = manifest.segments.iter().map(|s| s.doc_count as u64).sum();
    let mut body = serde_json::json!({
        "namespace": namespace,
        "documents": ns_docs,
        "segments": manifest.segments.len(),
        "version": manifest.version,
        "manifest_version": manifest.version,
    });
    if let Some(shards) = shards {
        body["shards"] = serde_json::json!(shards);
        body["shard_stats"] =
            serde_json::json!(kosha_core::sharding::shard_namespaces(&root_id, shards)
                .iter()
                .enumerate()
                .map(|(index, shard)| {
                    let m = indexer.manifest_shared(shard);
                    serde_json::json!({
                        "shard": index,
                        "documents": m.as_ref().map_or(0, |m| m
                            .segments
                            .iter()
                            .map(|s| s.doc_count as u64)
                            .sum::<u64>()),
                        "segments": m.as_ref().map_or(0, |m| m.segments.len()),
                        "manifest_version": m.as_ref().map_or(0, |m| m.version),
                    })
                })
                .collect::<Vec<_>>());
    }
    // A branch (#208) reports its lineage and how much it still shares.
    if let Some(branch) = &manifest.branch {
        let ns = NamespaceId(scoped_ns.clone());
        let shared = manifest
            .segments
            .iter()
            .filter(|entry| manifest.is_inherited_segment(&ns, &entry.segment_id))
            .count();
        body["branch"] = serde_json::json!({
            "parent": branch.parent.0.split_once('/').map_or(branch.parent.0.as_str(), |(_, n)| n),
            "parent_version": branch.parent_version,
            "shared_segments": shared,
        });
    }
    // Only while the dashboard is on (#214), so default output is unchanged.
    if let Some(recent) = state.dashboard.recent_queries_json(&scoped_ns) {
        body["recent_queries"] = recent;
    }
    if state.ingest_host.is_some() {
        add_manifest_follow_stats(state, &NamespaceId(scoped_ns), &mut body);
    } else {
        add_unindexed_stats(state, &NamespaceId(scoped_ns), &mut body);
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
        .map(|entry| manifest.segment_dir(&state.data_dir, &ns, &entry.segment_id))
        .collect();
    #[cfg(feature = "s3")]
    // Admin routes rewrite or merge segments — must preserve vectors.
    state.ensure_segments_local(&seg_paths, true);

    let mut new_footers = Vec::new();
    let mut rebuilt = 0usize;
    let mut inherited = 0usize;
    let mut errors = Vec::new();
    for (entry, seg_path) in manifest.segments.iter().zip(seg_paths.iter()) {
        // A branch never rewrites a file in its parent's prefix (#208):
        // run the rebuild on the namespace that owns the segment.
        if manifest.is_inherited_segment(&ns, &entry.segment_id) {
            inherited += 1;
            continue;
        }
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
        "skipped_inherited": inherited,
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
            let seg_path = manifest.segment_dir(&state.data_dir, &ns, &entry.segment_id);
            // A branch never writes into its parent's prefix (#208).
            if manifest.is_inherited_segment(&ns, &entry.segment_id) {
                skipped += 1;
                continue;
            }

            // Resumable: a segment whose sidecar already reached S3 is done —
            // re-running after a crash/restart only pays for the remainder.
            #[cfg(feature = "s3")]
            let already_done = state
                .s3_storage
                .as_ref()
                .map(|s3| {
                    let rel = manifest.segment_rel_dir(&ns, &entry.segment_id);
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
        referenced_elsewhere: None,
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
    // An indexer (#254) may not have seen the namespace yet.
    if state.role == role::Role::Indexer {
        state.indexer_follow_all();
    }
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

    // A sharded namespace (#207) holds its segments in its shards: run one
    // pass per shard and report each.
    if let Some(shards) = state.indexer.shard_count(&ns) {
        let mut passes = Vec::new();
        for shard in kosha_core::sharding::shard_namespaces(&ns, shards) {
            if state.indexer.manifest_version(&shard).is_none() {
                continue;
            }
            match compact_one(state, &shard, mode, opts.clone(), run_async) {
                Ok(pass) => passes.push(pass),
                Err((status, message)) => return json_error(status, &message),
            }
        }
        let body = serde_json::json!({
            "namespace": ns.0,
            "mode": compact_mode_name(mode),
            "shards": passes,
        });
        return if run_async {
            json_accepted(&body)
        } else {
            json_ok(&body)
        };
    }

    match compact_one(state, &ns, mode, opts, run_async) {
        Ok(body) if run_async => json_accepted(&body),
        Ok(body) => json_ok(&body),
        Err((status, message)) => json_error(status, &message),
    }
}

/// Register and run (or, `run_async`, start) one compaction pass on `ns`.
/// The JSON body on success; `(status, message)` on refusal or failure.
fn compact_one(
    state: &AppState,
    ns: &NamespaceId,
    mode: kosha_write::CompactMode,
    opts: kosha_write::CompactOptions,
    run_async: bool,
) -> Result<serde_json::Value, (u16, String)> {
    // Resolve the background handle before registering, so a refusal can't
    // leave a job stuck in `running`.
    let job_state = if run_async {
        match state.self_ref.upgrade() {
            Some(s) => Some(s),
            None => {
                return Err((500, "server not initialized for background jobs".into()));
            }
        }
    } else {
        None
    };
    let job_id = match state.compaction.try_start(
        ns,
        compact_mode_name(mode),
        compaction_sched::Trigger::Api,
        Instant::now(),
    ) {
        Ok(id) => id,
        Err(running) => {
            return Err((
                409,
                format!(
                    "a compaction pass is already running on namespace '{}' (job {running}); \
                     poll GET /v1/admin/compaction-jobs/{running}",
                    ns.0
                ),
            ));
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
        let ns = ns.clone();
        std::thread::Builder::new()
            .name("kosha-compaction-job".into())
            .spawn(move || run_compaction_job(&job_state, &ns, opts, &job_id))
            .expect("failed to spawn compaction job thread");
        return Ok(response);
    }

    match run_compaction_job(state, ns, opts, &job_id) {
        Ok(outcome) => Ok(serde_json::json!({
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
        Err(error) => Err((500, error)),
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
    let enabled = state.runs_compaction_scheduler() && state.compaction.config().interval.is_some();
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
        manifest
            .segments
            .iter()
            .filter_map(|entry| {
                let id = &entry.segment_id;
                // S3 is authoritative: a local copy may be partial (a
                // lexical search hydrates without vector.idx). Local size
                // only for segments S3 doesn't list, i.e. local-only mode
                // or a flush not yet published.
                let bytes =
                    remote.get(&id.0).copied().filter(|b| *b > 0).or_else(|| {
                        local_dir_bytes(&manifest.segment_dir(&self.data_dir, ns, id))
                    })?;
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
    // An indexer plans on the latest committed manifest (#254); its
    // publish still rebases onto anything committed during the merge.
    if state.role == role::Role::Indexer {
        state
            .indexer_follow_namespace(ns)
            .map_err(|e| format!("cannot read the committed manifest: {e}"))?;
    }
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
    let group_bytes =
        |paths: &[PathBuf]| -> u64 { paths.iter().filter_map(|path| local_dir_bytes(path)).sum() };

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
        let ids = plan.input_ids();
        // A branch's inherited inputs live under their home namespace's
        // prefix (#208).
        let paths: Vec<PathBuf> = ids
            .iter()
            .map(|id| manifest.segment_dir(&state.data_dir, ns, id))
            .collect();
        drop(manifest);

        // Hydrate only this group, in one fanned-out batch (see
        // `ensure_segments_local`). Compaction rewrites segments, so it
        // must preserve vectors.
        let local_before = group_bytes(&paths);
        storage.hydrate(&paths);
        let local_inputs = group_bytes(&paths);
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
        fail::fail_point!("compaction.before_publish");
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
    // An indexer (#254) learns namespaces and their new segments from the
    // control store, not from writes.
    if state.role == role::Role::Indexer {
        state.indexer_follow_all();
    }
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
#[cfg_attr(not(feature = "s3"), allow(dead_code))]
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
        branch: None,
        schema: None,
        retired: Default::default(),
        shards: None,
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
        let _ = state;
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
/// the same contract `FairSemaphore::try_acquire` and
/// `kosha_query::MemoryLedger` already use), a non-shed `missing` list is a
/// genuine S3 failure (503). The shed check comes first because a shed
/// request also has a non-empty `missing` list — it stopped fetching on
/// purpose — and reporting that as a 503 would tell the caller its data is
/// broken when the server merely declined to pull multiple GB in one
/// request.
#[cfg(feature = "s3")]
fn hydration_error_response(outcome: &HydrationOutcome) -> Option<String> {
    if outcome.shed {
        eprintln!(
            "WARN: search shed: {}{}",
            outcome.shed_reason.as_deref().unwrap_or("-"),
            request_id()
        );
        return Some(hydration_shed_response());
    }
    if !outcome.missing.is_empty() {
        let message = hydration_failed_message(&outcome.missing);
        eprintln!("ERROR: {message}{}", request_id());
        return Some(json_error(503, &message));
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

/// `GET /internal/segment/<relative-path>` — stream an already-cached
/// segment file's raw bytes to a peer replica, for `fetch_from_peer`, as
/// `application/octet-stream` in O(chunk) memory (#259). Only called once
/// the internal service credential checked out (see `dispatch`); ordinary
/// tenant credentials cannot retrieve another tenant's cached files.
///
/// `relative_path` is validated the same way any other cache key is
/// (`Cache::path_for` rejects `..`/absolute-path traversal). 404 (not an
/// error — the caller retries against the lease owner) when the path isn't
/// cached on this pod, whether because it's not the owner or hydration is
/// still in flight.
fn internal_segment_response(request_line: &str, state: &AppState) -> http::Response {
    #[cfg(all(feature = "s3", feature = "postgres"))]
    {
        let raw = request_line
            .strip_prefix("GET /internal/segment/")
            .and_then(|rest| rest.split(' ').next())
            .unwrap_or("");
        let relative_path = url_decode(raw.split('?').next().unwrap_or(raw));
        let local_path = match state.cache.path_for(&relative_path) {
            Ok(p) => p,
            Err(_) => return http::Response::from_raw(json_error(400, "invalid path")),
        };
        // Files land in the cache by rename, complete, so an open file is
        // a whole one; its length is fixed from here on.
        let opened = std::fs::File::open(&local_path)
            .and_then(|file| file.metadata().map(|meta| (file, meta)));
        match opened {
            Ok((file, meta)) if meta.is_file() => {
                http::Response::file(file, meta.len(), "application/octet-stream")
            }
            _ => http::Response::from_raw(json_error(404, "not cached here")),
        }
    }
    #[cfg(not(all(feature = "s3", feature = "postgres")))]
    {
        let _ = (request_line, state);
        http::Response::from_raw(json_error(404, "not found"))
    }
}

// ─── JSON response helpers ──────────────────────────────────────────────────

fn json_ok<T: serde::Serialize>(value: &T) -> String {
    let body = serde_json::to_string(value).unwrap_or_else(|_| "{}".into());
    format!(
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: application/json\r\n\r\n{body}",
        body.len()
    )
}

/// Disk-cache pins on a search's segment directories, released on drop.
struct SegmentPins<'a> {
    cache: &'a Cache,
    keys: Vec<String>,
}

impl Drop for SegmentPins<'_> {
    fn drop(&mut self) {
        for key in &self.keys {
            self.cache.unpin(key);
        }
    }
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
        + phases.materialize_wall_ms
        + phases.knn_rescore_ms;
    // Global bound-driven kNN rescoring (#381) logs its counters only when
    // it ran (`KOSHA_KNN_GLOBAL_RESCORE`), so the line is unchanged by
    // default.
    let mut knn_rescore = if phases.knn_global_rescore {
        format!(
            " knn_rescore_ms={:.1} knn_rescore_candidates={} knn_rescore_selected={} \
             knn_rescored={} knn_rescore_unavailable={} knn_rescore_ranged_spans={}",
            phases.knn_rescore_ms,
            phases.knn_rescore_candidates,
            phases.knn_rescore_selected,
            phases.knn_rescored,
            phases.knn_rescore_unavailable,
            phases.knn_rescore_ranged_spans,
        )
    } else {
        String::new()
    };
    // Per-segment path counters (#381, `KOSHA_KNN_RESCORE_STATS`, or with
    // the flag above): quantized segments probed there, and how many of
    // them returned unrescored estimates.
    if phases.knn_rescore_stats {
        knn_rescore.push_str(&format!(
            " knn_seg_quantized={} knn_seg_unrescored={}",
            phases.knn_seg_quantized, phases.knn_seg_unrescored,
        ));
    }
    println!(
        "search timing: ns={} total_ms={:.1} hydrate_ms={:.1} hydrate_files={} \
         hydrate_mb={:.1} queue_ms={:.1} admit_ms={:.1} bm25_stats={} bm25_stats_ms={:.2} \
         score_ms={:.1} materialize_ms={:.1} opens_cold={} opens_cached={} \
         open_total_ms={:.1} knn_postings={} knn_segments={} knn_centroids={} knn_plan_ms={:.1} knn_mode={} knn_exact_vectors={} \
         knn_filtered={} knn_degraded={} hits={} page_gets={} \
         page_ranged_kb={:.1} page_whole_files={} page_whole_mb={:.1} s3_rounds={} \
         s3_bytes={} postings_gets={} postings_ranged_kb={:.1} postings_whole_files={} \
         postings_whole_mb={:.1} vector_gets={} vector_ranged_kb={:.1} \
         vector_whole_files={}{}{}{}",
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
        phases.knn_segments_probed,
        phases.knn_centroids_scanned,
        phases.knn_plan_ms,
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
        hydration.s3_rounds(),
        hydration.s3_bytes(),
        hydration.postings.ranged_gets,
        hydration.postings.ranged_bytes as f64 / 1024.0,
        hydration.postings.whole_files,
        hydration.postings.whole_bytes as f64 / (1024.0 * 1024.0),
        hydration.vectors.ranged_gets,
        hydration.vectors.ranged_bytes as f64 / 1024.0,
        hydration.vectors.whole_files,
        // #384: only with the experimental relative cutoff on, so the
        // default line is unchanged.
        phases
            .knn_cutoff_rule
            .map_or_else(String::new, |rule| format!(
                " knn_cutoff_rule={}",
                rule.as_str()
            )),
        knn_rescore,
        request_id(),
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
        "HTTP/1.1 202 Accepted\r\ncontent-length: {}\r\ncontent-type: application/json\r\n\r\n{body}",
        body.len()
    )
}

fn json_error(status_code: u16, message: &str) -> String {
    // The reason for an error response; `request:` carries the rest.
    eprintln!(
        "WARN: request failed: status={status_code} {message}{}",
        request_id()
    );
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
        502 => "502 Bad Gateway",
        503 => "503 Service Unavailable",
        _ => "500 Internal Server Error",
    };
    format!(
        "HTTP/1.1 {status_line}\r\ncontent-length: {}\r\ncontent-type: application/json\r\n\r\n{body}",
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
        "HTTP/1.1 {status_line}\r\ncontent-length: {}\r\ncontent-type: application/json\r\n\r\n{body}",
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
        "HTTP/1.1 {status_code} {reason}\r\ncontent-length: {}\r\ncontent-type: application/json\r\n\r\n{body}",
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
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpStream;
    use std::path::Path;

    #[derive(Clone)]
    pub(crate) struct SharedManifestStore {
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
        fn remove_namespace(
            &mut self,
            id: &NamespaceId,
        ) -> Result<Option<kosha_core::Manifest>, KoshaError> {
            Ok(self.inner.lock().unwrap().remove_namespace(id))
        }
    }

    impl SharedManifestStore {
        pub(crate) fn new(inner: Arc<Mutex<kosha_control::Controller>>) -> Self {
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

    /// #342: a warm search served from the cached hydration verdict still
    /// counts its segments as NVMe hits for its tenant.
    #[cfg(feature = "s3")]
    #[test]
    fn warm_searches_count_nvme_hits_for_their_tenant() {
        let state = test_state();
        let ok = |resp: String| {
            assert_eq!(status_of(&resp), 200, "{resp}");
        };
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
        let nvme_hits = || {
            state
                .cache
                .tenant_stats()
                .into_iter()
                .find(|s| s.tenant == "acme")
                .map_or(0, |s| s.hits)
        };
        let search = |text: &str| {
            ok(v1(
                &state,
                "acme",
                "POST /v1/namespaces/docs/search",
                serde_json::json!({"query_text": text}),
            ))
        };
        // The first (unpruned) pass verifies the scoring set and caches the
        // verdict; the second, a different query so the result cache does
        // not answer it, is served from that verdict.
        search("");
        assert!(state
            .completed_hydrations
            .lock()
            .unwrap()
            .keys()
            .any(|(ns, _)| ns == "acme/docs"));
        let after_first = nvme_hits();
        search("quick");
        assert!(
            nvme_hits() > after_first,
            "the warm search counted its segment as a hit ({after_first} → {})",
            nvme_hits()
        );
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
            // Reserved: `acme/_shard-0001` would be accounted to `_legacy`.
            serde_json::json!({"namespace": "_shard-0001"}),
            serde_json::json!({"namespace": "_internal"}),
            serde_json::json!({"namespace": ".hidden"}),
        ] {
            let resp = create("acme", bad.clone());
            assert_eq!(status_of(&resp), 400, "{bad} → {resp}");
        }
        // Nor can a tenant reach one through a namespace route, and a
        // tenant id with a reserved prefix is refused outright.
        let shard_route = v1(
            &state,
            "acme",
            "POST /v1/namespaces/_shard-0001/search",
            serde_json::json!({"query_text": "x"}),
        );
        assert_eq!(status_of(&shard_route), 400, "{shard_route}");
        let bad_tenant = create("_legacy", serde_json::json!({"namespace": "docs"}));
        assert_eq!(status_of(&bad_tenant), 403, "{bad_tenant}");
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
        // Strong consistency (#255, the default) sees the buffered write
        // too.
        assert_eq!(ok(search())["total_hits"], 2);
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
        assert_eq!(ok(search())["total_hits"], 2);

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
            // `GET /v1/namespaces` lists the tenant's namespaces (#214).
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
                let resp = http::read_response_string(&mut s).unwrap();
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

    #[cfg(feature = "s3")]
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
                ..Default::default()
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

    pub(crate) fn shared_store() -> SharedManifestStore {
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
    pub(crate) struct OutageStore {
        inner: SharedManifestStore,
        down: Arc<std::sync::atomic::AtomicBool>,
        reads: Arc<std::sync::atomic::AtomicU64>,
        /// `list_namespaces` reads as empty, the way `PgStore` reports a
        /// failed query there (it has no error to return).
        pub(crate) list_fails: Arc<std::sync::atomic::AtomicBool>,
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
            if self.list_fails.load(std::sync::atomic::Ordering::SeqCst) {
                return Vec::new();
            }
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
        fn remove_namespace(
            &mut self,
            id: &NamespaceId,
        ) -> Result<Option<kosha_core::Manifest>, KoshaError> {
            self.check()?;
            self.inner.remove_namespace(id)
        }
    }

    pub(crate) fn outage_store() -> OutageStore {
        OutageStore {
            inner: shared_store(),
            down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            reads: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            list_fails: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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

    /// #208: a query replica follows a branch like any namespace, and drops
    /// it once the ingest pod deletes it, instead of serving a manifest
    /// whose own objects were just reclaimed. The parent is unaffected.
    #[test]
    fn query_replica_follows_branches_and_forgets_deleted_ones() {
        let store = outage_store();
        let parent = NamespaceId("tenant/prod".into());
        let child = NamespaceId("tenant/exp".into());
        let (ingest, query) = ingest_and_query(&store, Some(Duration::from_secs(3600)));
        index_text_and_publish(&ingest, &parent, &["a", "b"]);
        ingest.branch_namespace(&parent, &child).unwrap();
        index_text_and_publish(&ingest, &child, &["c"]);
        let search = |ns: &str| {
            let body = format!(r#"{{"namespace":"{ns}","query_text":"hello"}}"#);
            total_hits(&handle_search_post(body.as_bytes(), &query))
        };
        assert_eq!(search("tenant/exp"), 3);
        assert_eq!(search("tenant/prod"), 2);
        assert!(query
            .indexer
            .manifest_shared(&child)
            .unwrap()
            .branch
            .is_some());

        ingest.delete_branch(&child).unwrap();
        query.run_manifest_refresh_tick();
        assert!(query.indexer.manifest_version(&child).is_none());
        assert_eq!(search("tenant/prod"), 2);
        assert!(handle_search_post(
            br#"{"namespace":"tenant/exp","query_text":"hello"}"#,
            &query
        )
        .starts_with("HTTP/1.1 404"));
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
            branch: None,
            schema: None,
            retired: Default::default(),
            shards: None,
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
        // garbage interval or unsupported calendar interval is a 400;
        // #300-#302: calendar months, `time_zone` and bounds.
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

        // Issues #300-#302: calendar months, time zones and bounds.
        let resp = post(
            r#"{"month":{"date_histogram":{"field":"sentAt","calendar_interval":"1M"}},
                "la_days":{"date_histogram":{"field":"sentAt","calendar_interval":"1d","time_zone":"America/Los_Angeles"}},
                "ist_hours":{"date_histogram":{"field":"sentAt","fixed_interval":"1h","time_zone":"+05:30","min_doc_count":1}},
                "window":{"date_histogram":{"field":"sentAt","fixed_interval":"1h",
                          "extended_bounds":{"min":"2024-06-14T23:00:00Z","max":"2024-06-15T03:00:00Z"},
                          "hard_bounds":{"max":"2024-06-15T01:30:00Z"}}},
                "sizes":{"histogram":{"field":"bytes","interval":1024,"extended_bounds":{"min":0,"max":4096},"hard_bounds":{"min":500}}}}"#,
        );
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        let v: serde_json::Value =
            serde_json::from_str(&resp[resp.find("\r\n\r\n").unwrap() + 4..]).unwrap();
        let aggs = &v["aggregations"];
        assert_eq!(
            aggs["month"],
            serde_json::json!({"buckets": [
                {"key": 1_717_200_000_000_i64, "key_as_string": "2024-06-01T00:00:00.000Z", "doc_count": 3},
            ]}),
            "{v}"
        );
        // 2024-06-15 00:10Z is 17:10 on the 14th in Los Angeles (PDT).
        assert_eq!(
            aggs["la_days"],
            serde_json::json!({"buckets": [
                {"key": 1_718_348_400_000_i64, "key_as_string": "2024-06-14T00:00:00.000-07:00", "doc_count": 3},
            ]}),
            "{v}"
        );
        // Hours on the local clock: 00:10Z is 05:40 in India.
        assert_eq!(
            aggs["ist_hours"]["buckets"][0],
            serde_json::json!({"key": 1_718_407_800_000_i64, "key_as_string": "2024-06-15T05:00:00.000+05:30", "doc_count": 1}),
            "{v}"
        );
        assert_eq!(aggs["ist_hours"]["buckets"].as_array().unwrap().len(), 3);
        // Extended to 23:00 on the 14th, clipped by the hard max at the
        // 01:00 bucket; the 02:00 document is dropped.
        let window: Vec<(String, u64)> = aggs["window"]["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| {
                (
                    b["key_as_string"].as_str().unwrap().to_string(),
                    b["doc_count"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            window,
            vec![
                ("2024-06-14T23:00:00.000Z".to_string(), 0),
                ("2024-06-15T00:00:00.000Z".to_string(), 2),
                ("2024-06-15T01:00:00.000Z".to_string(), 0),
            ],
            "{v}"
        );
        assert_eq!(
            aggs["sizes"],
            serde_json::json!({"buckets": [
                {"key": 1024.0, "doc_count": 1},
                {"key": 2048.0, "doc_count": 1},
                {"key": 3072.0, "doc_count": 0},
                {"key": 4096.0, "doc_count": 0},
            ]}),
            "{v}"
        );

        for (bad, needle) in [
            (
                r#"{"x":{"date_histogram":{"field":"sentAt","fixed_interval":"1 banana"}}}"#,
                "invalid `fixed_interval`",
            ),
            (
                r#"{"x":{"date_histogram":{"field":"sentAt","calendar_interval":"2M"}}}"#,
                "single unit",
            ),
            (
                r#"{"x":{"date_histogram":{"field":"sentAt","fixed_interval":"1h","time_zone":"Mars/Base"}}}"#,
                "unknown `time_zone`",
            ),
            (
                r#"{"x":{"date_histogram":{"field":"sentAt","fixed_interval":"1h","time_zone":"+25:00"}}}"#,
                "invalid `time_zone`",
            ),
            (
                r#"{"x":{"date_histogram":{"field":"sentAt","fixed_interval":"1h","extended_bounds":{"min":"2024-06-16","max":"2024-06-15"}}}}"#,
                "is after `max`",
            ),
            (
                r#"{"x":{"date_histogram":{"field":"sentAt","fixed_interval":"1h","hard_bounds":{"min":"next week"}}}}"#,
                "must be a date",
            ),
            (
                r#"{"x":{"histogram":{"field":"bytes","interval":1,"hard_bounds":{"min":5,"max":1}}}}"#,
                "is greater than `max`",
            ),
            (
                r#"{"x":{"histogram":{"field":"bytes","interval":1,"extended_bounds":{"min":0,"max":20000}}}}"#,
                "more than the limit",
            ),
            (
                r#"{"x":{"date_histogram":{"field":"sentAt","fixed_interval":"1h","keyed":true}}}"#,
                "not supported yet",
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
            doc_store_compression: None,
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
    #[cfg(feature = "s3")]
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
        let sem = Arc::new(fair_admission::FairSemaphore::new(
            1,
            2,
            0,
            Some(Duration::from_millis(50)),
        ));

        // The only permit is taken; two waiters may queue behind it.
        let held = sem.try_acquire("t", 1).expect("first acquire must succeed");

        let mut waiting = Vec::new();
        for _ in 0..2 {
            let s = Arc::clone(&sem);
            waiting.push(std::thread::spawn(move || s.try_acquire("t", 1).is_ok()));
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
            sem.try_acquire("t", 1).is_err(),
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
            sem.try_acquire("t", 1).is_ok(),
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
            branch: None,
            schema: None,
            retired: Default::default(),
            shards: None,
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

    #[cfg(feature = "s3")]
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

    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
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
            doc_store_compression: None,
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
    /// Global kNN rescoring (#381) reads a quantized column's exact
    /// vectors by range, so with the flag on its `doc_vectors.bin` header
    /// joins the kNN scoring set; with it off (the default) and for f32
    /// columns, the predicate is the legacy one.
    #[cfg(feature = "s3")]
    #[test]
    fn global_rescore_wants_the_doc_vectors_header_of_quantized_columns() {
        let footer = |quantization| {
            let dir = std::env::temp_dir().join(format!(
                "kosha-test-dv-header-{quantization:?}-{}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&dir);
            let mut w =
                kosha_segment::SegmentWriter::new(kosha_core::SegmentId("s".into()), dir.clone())
                    .with_vector_store_mode(kosha_segment::VectorStoreMode::Sidecar)
                    .with_vector_quantization(kosha_segment::VectorQuantizationConfig::all(
                        quantization,
                    ));
            for i in 0..8 {
                w.add_document(
                    DocumentId(format!("d{i}")),
                    vec![Field::vector("emb", vec![1.0, i as f32, 0.5, 0.0])],
                );
            }
            let footer = w.finalize(Default::default()).unwrap();
            let _ = fs::remove_dir_all(&dir);
            assert!(footer.doc_vectors && footer.vector_fields.is_some());
            footer
        };
        let i8 = footer(kosha_core::VectorQuantization::I8);
        let f32 = footer(kosha_core::VectorQuantization::F32);
        assert!(!knn_wants_doc_vectors_header_with(Some(&i8), "emb", false));
        assert!(knn_wants_doc_vectors_header_with(Some(&i8), "emb", true));
        assert!(!knn_wants_doc_vectors_header_with(Some(&i8), "other", true));
        assert!(!knn_wants_doc_vectors_header_with(Some(&f32), "emb", true));
        assert!(!knn_wants_doc_vectors_header_with(None, "emb", false));
    }

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
            "POST /v1/namespaces/foo/shards HTTP/1.1\r\n",
            "POST /v1/namespaces/foo/branch HTTP/1.1\r\n",
            "DELETE /v1/namespaces/foo/branch HTTP/1.1\r\n",
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

    /// `/metrics` (#223) is per-pod: a query pod must answer it itself,
    /// never forward it to the ingest pod.
    #[test]
    fn metrics_is_not_a_write_path() {
        assert!(!is_write_path("GET /metrics HTTP/1.1\r\n"));
        assert!(!is_write_path("GET /metrics?x=1 HTTP/1.1\r\n"));
    }

    /// An indexer (#254) serves only the compaction admin API, but
    /// `/metrics` is per-pod and answered ahead of that gate.
    #[test]
    fn indexer_pod_serves_metrics() {
        let mut state = test_state();
        state.role = role::Role::Indexer;
        let addr = serve_state(state);
        let samples = scrape_samples(addr);
        assert!(
            samples
                .iter()
                .any(|s| s.name == "kosha_search_requests_total"),
            "{} samples",
            samples.len()
        );
        let other = http(addr, "GET /v1/stats HTTP/1.1\r\nhost: x\r\n\r\n");
        assert!(other.starts_with("HTTP/1.1 404"), "{other}");
    }

    /// Send one raw HTTP request to a live `serve` and return the response.
    fn http(addr: std::net::SocketAddr, request: &str) -> String {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        s.write_all(request.as_bytes()).unwrap();
        // Framed by content-length: the server keeps the connection open.
        http::read_response_string(&mut s).unwrap()
    }

    fn http_json(addr: std::net::SocketAddr, line: &str, body: &serde_json::Value) -> String {
        let body = body.to_string();
        http(
            addr,
            &format!(
                "{line} HTTP/1.1\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
    }

    fn serve_state(state: AppState) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(state);
        std::thread::spawn(move || serve(listener, state, Duration::from_secs(30)));
        addr
    }

    fn scrape(addr: std::net::SocketAddr, auth: &str) -> String {
        http(
            addr,
            &format!("GET /metrics HTTP/1.1\r\nhost: x\r\n{auth}\r\n"),
        )
    }

    fn scrape_samples(addr: std::net::SocketAddr) -> Vec<metrics::tests::Sample> {
        let resp = scrape(addr, "");
        let (status, headers, body) = parse(&resp);
        assert_eq!(status, "HTTP/1.1 200 OK", "{resp}");
        assert!(
            headers.contains("content-type: text/plain; version=0.0.4; charset=utf-8"),
            "{headers}"
        );
        metrics::tests::parse_exposition(body).unwrap_or_else(|e| panic!("{e}\n{body}"))
    }

    /// #223 acceptance: searches through the real HTTP server move the
    /// counters, and every counter only ever goes up between scrapes.
    #[test]
    fn metrics_counters_increase_monotonically_across_searches() {
        let addr = serve_state(test_state());
        let search = || {
            let resp = http_json(
                addr,
                "POST /v1/namespaces/docs/search",
                &serde_json::json!({"query_text": "quick", "max_results": 5}),
            );
            assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        };
        let indexed = http_json(
            addr,
            "POST /v1/namespaces/docs/documents",
            &serde_json::json!({"documents": [
                {"id": "a", "fields": [{"name": "title", "field_type": "Text", "value": "quick fox"}]},
                {"id": "b", "fields": [{"name": "title", "field_type": "Text", "value": "quick dog"}]}
            ]}),
        );
        assert!(indexed.starts_with("HTTP/1.1 200"), "{indexed}");
        let flushed = http_json(
            addr,
            "POST /v1/namespaces/docs/flush",
            &serde_json::json!({}),
        );
        assert!(flushed.starts_with("HTTP/1.1 200"), "{flushed}");
        for _ in 0..3 {
            search();
        }
        let bad = http_json(
            addr,
            "POST /v1/namespaces/docs/search",
            &serde_json::json!({"query_text": "quick", "max_results": -1}),
        );
        assert!(!bad.starts_with("HTTP/1.1 200"), "{bad}");
        // A body that is not JSON at all is rejected before the search
        // handler proper, and still counts.
        let garbled = http(
            addr,
            "POST /v1/namespaces/docs/search HTTP/1.1\r\ncontent-length: 4\r\n\r\n{bad",
        );
        assert!(garbled.starts_with("HTTP/1.1 400"), "{garbled}");

        let first = scrape_samples(addr);
        let ok = |s: &[metrics::tests::Sample]| {
            metrics::tests::value(s, "kosha_search_requests_total", &[("outcome", "ok")]).unwrap()
        };
        assert_eq!(ok(&first), 3.0);
        assert_eq!(
            metrics::tests::value(
                &first,
                "kosha_search_requests_total",
                &[("outcome", "error")]
            ),
            Some(2.0)
        );
        assert_eq!(
            metrics::tests::value(&first, "kosha_index_documents_total", &[]),
            Some(2.0)
        );
        assert_eq!(
            metrics::tests::value(&first, "kosha_flush_total", &[]),
            Some(1.0)
        );
        assert_eq!(
            metrics::tests::value(&first, "kosha_search_duration_seconds_count", &[]),
            Some(3.0)
        );

        for _ in 0..2 {
            search();
        }
        let second = scrape_samples(addr);
        assert_eq!(ok(&second), 5.0);
        // Counters and histogram series never decrease between scrapes.
        let key = |s: &metrics::tests::Sample| format!("{}{:?}", s.name, s.labels);
        let before: HashMap<String, f64> = first.iter().map(|s| (key(s), s.value)).collect();
        let cumulative = |name: &str| {
            name.ends_with("_total")
                || name.ends_with("_bucket")
                || name.ends_with("_count")
                || name.ends_with("_sum")
        };
        for s in second.iter().filter(|s| cumulative(&s.name)) {
            let was = before[&key(s)];
            assert!(s.value >= was, "{} went from {was} to {}", key(s), s.value);
        }
        assert!(
            metrics::tests::value(&second, "kosha_search_duration_seconds_count", &[]).unwrap()
                > metrics::tests::value(&first, "kosha_search_duration_seconds_count", &[])
                    .unwrap()
        );
    }

    /// #223: `/metrics` needs no API key by default, needs the admin key
    /// under `KOSHA_METRICS_AUTH=admin`, and is gone under `KOSHA_METRICS=off`.
    #[test]
    fn metrics_route_access_modes() {
        let open = serve_state(test_state());
        assert!(scrape(open, "").starts_with("HTTP/1.1 200 OK"));

        let mut state = test_state();
        state.admin_auth_key = Some("admin-secret".into());
        state.metrics = metrics::Metrics::new(metrics::MetricsConfig {
            exposure: metrics::Exposure::Admin,
            ..metrics::MetricsConfig::default()
        });
        let admin = serve_state(state);
        let denied = scrape(admin, "");
        assert!(denied.starts_with("HTTP/1.1 403"), "{denied}");
        let wrong = scrape(admin, "authorization: Bearer tenant-key\r\n");
        assert!(wrong.starts_with("HTTP/1.1 403"), "{wrong}");
        let allowed = scrape(admin, "authorization: Bearer admin-secret\r\n");
        assert!(allowed.starts_with("HTTP/1.1 200 OK"), "{allowed}");
        assert!(allowed.contains("kosha_search_requests_total"), "{allowed}");

        let mut state = test_state();
        state.metrics = metrics::Metrics::new(metrics::MetricsConfig {
            exposure: metrics::Exposure::Off,
            ..metrics::MetricsConfig::default()
        });
        let off = serve_state(state);
        let gone = scrape(off, "");
        assert!(gone.starts_with("HTTP/1.1 404"), "{gone}");
    }

    /// A search the scoring queue sheds after waiting is counted as
    /// `timeout` and in `kosha_search_queue_rejections_total`.
    #[test]
    fn queue_timeout_search_is_counted() {
        let mut state = test_state();
        // No permits and no queue: every scored search is shed at once.
        state.search_semaphore =
            fair_admission::FairSemaphore::new(0, 1, 0, Some(Duration::from_millis(1)));
        let ns = NamespaceId("t/docs".into());
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId("d1".into()),
                    fields: vec![Field::text("title", "quick fox")],
                }],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();
        let resp = handle_search_post(
            br#"{"namespace":"t/docs","query_text":"quick","max_results":5}"#,
            &state,
        );
        assert!(resp.starts_with("HTTP/1.1 429"), "{resp}");
        let text = state.metrics.render(&metrics::Scrape::default());
        let samples = metrics::tests::parse_exposition(&text).unwrap();
        assert_eq!(
            metrics::tests::value(
                &samples,
                "kosha_search_requests_total",
                &[("outcome", "timeout")]
            ),
            Some(1.0),
            "one waiter allowed, so the search waited 1ms and timed out"
        );
        assert_eq!(
            metrics::tests::value(&samples, "kosha_search_queue_rejections_total", &[]),
            Some(1.0)
        );
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
            "GET /v1/namespaces/foo/branches HTTP/1.1\r\n",
            "GET /search?ns=foo&q=bar HTTP/1.1\r\n",
            "POST /search HTTP/1.1\r\n",
            "POST /exists HTTP/1.1\r\n",
            "GET /stats HTTP/1.1\r\n",
            "GET /v1/namespaces/foo HTTP/1.1\r\n",
            "POST /v1/namespaces/foo HTTP/1.1\r\n",
            "DELETE /v1/namespaces/foo/documents HTTP/1.1\r\n",
            "GET /v1/namespaces HTTP/1.1\r\n",
            "GET /v1/namespaces?x=1 HTTP/1.1\r\n",
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
            branch: None,
            schema: None,
            retired: Default::default(),
            shards: None,
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
            shards: None,
            branch: None,
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
            branch: None,
            schema: None,
            retired: Default::default(),
            shards: None,
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
    fn internal_segment_route_serves_cached_bytes_raw() {
        let mut state = test_state();
        state.internal_service_key = Some("peer-test-secret".into());
        state
            .cache
            .put_bytes("ns/seg/doc_store.bin", b"hello segment\x00\xff")
            .unwrap();

        let mut response = dispatch(
            &state,
            test_request(
                "GET /internal/segment/ns/seg/doc_store.bin HTTP/1.1\r\n\
                 authorization: Bearer peer-test-secret\r\n\r\n",
            ),
        );
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.header("content-type"),
            Some("application/octet-stream")
        );
        assert_eq!(response.header("x-kosha-encoding"), None);
        assert_eq!(response.body_len(), 15);
        assert_eq!(response.body_bytes(), b"hello segment\x00\xff");
    }

    #[cfg(all(feature = "s3", feature = "postgres"))]
    #[test]
    fn internal_segment_route_requires_the_internal_credential() {
        let mut state = test_state();
        state.internal_service_key = Some("peer-test-secret".into());
        state.admin_auth_key = Some("admin-secret".into());
        state
            .cache
            .put_bytes("ns/seg/doc_store.bin", b"hello segment")
            .unwrap();
        let response = dispatch(
            &state,
            test_request(
                "GET /internal/segment/ns/seg/doc_store.bin HTTP/1.1\r\n\
                 authorization: Bearer admin-secret\r\n\r\n",
            ),
        );
        assert_eq!(response.status(), 403);
    }

    #[cfg(all(feature = "s3", feature = "postgres"))]
    #[test]
    fn internal_segment_route_404s_when_not_cached() {
        let mut state = test_state();
        state.internal_service_key = Some("peer-test-secret".into());
        let response = dispatch(
            &state,
            test_request(
                "GET /internal/segment/ns/seg/missing.bin HTTP/1.1\r\n\
                 authorization: Bearer peer-test-secret\r\n\r\n",
            ),
        );
        assert_eq!(response.status(), 404);
    }

    #[cfg(all(feature = "s3", feature = "postgres"))]
    #[test]
    fn internal_segment_route_rejects_path_traversal() {
        let mut state = test_state();
        state.internal_service_key = Some("peer-test-secret".into());
        let response = dispatch(
            &state,
            test_request(
                "GET /internal/segment/../../etc/passwd HTTP/1.1\r\n\
                 authorization: Bearer peer-test-secret\r\n\r\n",
            ),
        );
        assert_eq!(response.status(), 400);
    }

    /// Parse a raw request (request line, headers, blank line, body) into
    /// the [`http::Request`] the server hands to `dispatch`.
    fn test_request(raw: &str) -> http::Request {
        let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((raw, ""));
        let mut lines = head.split("\r\n");
        let mut first = lines.next().unwrap_or("").split_whitespace();
        let method = first.next().unwrap_or("GET").to_string();
        let target = first.next().unwrap_or("/").to_string();
        let headers = lines
            .filter_map(|line| {
                let (k, v) = line.split_once(':')?;
                Some((k.trim().to_ascii_lowercase(), v.trim().to_string()))
            })
            .collect();
        http::Request {
            method,
            target,
            headers,
            body: body.as_bytes().to_vec(),
        }
    }

    /// End-to-end over a real socket, mirroring
    /// `forward_to_ingest_relays_method_path_and_response`: a real "peer"
    /// server (its own `AppState`/`serve()` loop) with a segment file
    /// already in its cache, and `fetch_from_peer` against it — exercises
    /// the actual reqwest round trip into the requester's cache, not just
    /// the pure route handler above.
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

        let written = requester
            .fetch_from_peer(&addr.to_string(), "ns/seg/doc_store.bin")
            .expect("peer fetch should succeed");
        assert_eq!(written, 10);
        let local = requester.cache.path_for("ns/seg/doc_store.bin").unwrap();
        assert_eq!(fs::read(local).unwrap(), b"peer bytes");
    }

    /// A requester with a peer HTTP client and the shared internal key.
    #[cfg(all(feature = "s3", feature = "postgres"))]
    fn peer_requester() -> AppState {
        let mut requester = test_state();
        requester.internal_auth_key = Some("peer-test-secret".into());
        requester.peer_http_client = Some(
            reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
        );
        requester
    }

    /// #259 acceptance: a large segment file moves peer to peer as raw
    /// bytes, streamed on both ends, and arrives intact.
    #[cfg(all(feature = "s3", feature = "postgres"))]
    #[test]
    fn fetch_from_peer_streams_a_64_mib_file_intact() {
        const SIZE: usize = 64 * 1024 * 1024;
        let mut peer = test_state();
        peer.internal_service_key = Some("peer-test-secret".into());
        // Deterministic, incompressible-looking bytes (xorshift).
        let mut data = vec![0u8; SIZE];
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for chunk in data.chunks_mut(8) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            chunk.copy_from_slice(&x.to_le_bytes()[..chunk.len()]);
        }
        let expected_crc = crc32fast::hash(&data);
        peer.cache.put_bytes("ns/big/doc_store.bin", &data).unwrap();
        drop(data);
        let addr = serve_state(peer);

        let requester = peer_requester();
        let written = requester
            .fetch_from_peer(&addr.to_string(), "ns/big/doc_store.bin")
            .expect("peer fetch should succeed");
        assert_eq!(written, SIZE as u64);
        let local = requester.cache.path_for("ns/big/doc_store.bin").unwrap();
        let received = fs::read(&local).unwrap();
        assert_eq!(received.len(), SIZE);
        assert_eq!(crc32fast::hash(&received), expected_crc);
        // Accounted in the cache like any other hydrated file, and no temp
        // file left behind.
        assert!(requester.cache.total_size() >= SIZE as u64);
        let leftovers: Vec<_> = fs::read_dir(local.parent().unwrap())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    /// Rolling upgrade: a peer still on a pre-#259 build answers with the
    /// whole file base64-encoded and `x-kosha-encoding: base64`. The new
    /// client must still decode it.
    #[cfg(all(feature = "s3", feature = "postgres"))]
    #[test]
    fn fetch_from_peer_accepts_a_legacy_base64_peer() {
        use base64::Engine;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let payload: Vec<u8> = (0..=255u8).cycle().take(5000).collect();
        let encoded = base64::engine::general_purpose::STANDARD.encode(&payload);
        let seen_auth = Arc::new(Mutex::new(String::new()));
        let seen = Arc::clone(&seen_auth);
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if line.to_ascii_lowercase().starts_with("authorization:") {
                    *seen.lock().unwrap() = line.trim().to_string();
                }
            }
            let mut stream = stream;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: application/octet-stream\r\n\
                 x-kosha-encoding: base64\r\nconnection: close\r\n\r\n{encoded}",
                encoded.len()
            )
            .unwrap();
        });

        let requester = peer_requester();
        let written = requester
            .fetch_from_peer(&addr.to_string(), "ns/old/postings.bin")
            .expect("legacy peer fetch should succeed");
        assert_eq!(written, payload.len() as u64);
        let local = requester.cache.path_for("ns/old/postings.bin").unwrap();
        assert_eq!(fs::read(local).unwrap(), payload);
        assert_eq!(
            seen_auth.lock().unwrap().as_str(),
            "authorization: Bearer peer-test-secret"
        );
    }

    /// A peer that hangs up mid-body must not leave a truncated file in the
    /// cache: the caller falls back to S3 instead.
    #[cfg(all(feature = "s3", feature = "postgres"))]
    #[test]
    fn fetch_from_peer_discards_a_truncated_body() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }
            let mut stream = stream;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 1000\r\n\r\nonly a few bytes")
                .unwrap();
        });
        let requester = peer_requester();
        assert!(requester
            .fetch_from_peer(&addr.to_string(), "ns/cut/doc_store.bin")
            .is_err());
        let local = requester.cache.path_for("ns/cut/doc_store.bin").unwrap();
        assert!(!local.exists());
        let leftovers: Vec<_> = fs::read_dir(local.parent().unwrap())
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
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
        // Keep-alive (#259): the server frames responses by length and
        // leaves the connection open.
        assert!(!headers.contains("connection: close"));
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
        let resp = http::read_response_string(&mut s).unwrap();
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

    pub(crate) fn test_dir() -> PathBuf {
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

    // Only change since capture: the additive top-level
    // `exhaustive_search_count` (#255), present on every search response.
    const LEGACY_HIGHLIGHT_SNAPSHOT: &str = r#"{"results":[{"doc_id":"d1","score":0.39556284962119864,"fields":[{"name":"title","field_type":"Text","value":"Contract law basics"},{"name":"body","field_type":"Text","value":"A contract needs an offer"},{"name":"tag","field_type":"Keyword","value":"contract"}],"highlights":["A <em>contract</em> needs an offer"]}],"total_hits":1,"total_hits_relation":"eq","exhaustive_search_count":0}"#;

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
                query_text: None,
                embedding_model: None,
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
            doc_store_compression: None,
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

    // ─── Read-your-writes (#255) ─────────────────────────────────────────

    /// An ingest pod and a query pod over one control store and data
    /// directory. The query pod reads the ingest pod's unindexed tail through
    /// the real internal route (`route`, internal credential included); only
    /// the HTTP hop is replaced. `before_fetch` runs on every fetch, before
    /// the route is called (to inject a race).
    fn read_your_writes_pods(
        store: &OutageStore,
        before_fetch: impl Fn(&AppState, &NamespaceId) + Send + Sync + 'static,
    ) -> (Arc<AppState>, AppState) {
        let (mut ingest, mut query) = ingest_and_query(store, Some(Duration::from_secs(3600)));
        ingest.internal_service_key = Some("tail-key".into());
        let ingest = Arc::new(ingest);
        let writer = Arc::clone(&ingest);
        query.unindexed.fetcher = Some(Box::new(move |ns, version, known| {
            before_fetch(&writer, ns);
            let mut line = format!(
                "GET /internal/namespaces/{}/unindexed?manifest_version={version}",
                unindexed::encode_path_segment(&ns.0)
            );
            if let Some(known) = known {
                line.push_str("&if_none_match=");
                line.push_str(&unindexed::encode_path_segment(known));
            }
            line.push_str(" HTTP/1.1");
            let headers =
                HashMap::from([("authorization".to_string(), "Bearer tail-key".to_string())]);
            let response = route(&line, &headers, b"", "service", &writer);
            if !response.starts_with("HTTP/1.1 200") {
                return Err(KoshaError::Io(std::io::Error::other(response)));
            }
            let body = &response[response.find("\r\n\r\n").unwrap() + 4..];
            Ok(serde_json::from_str(body).unwrap())
        }));
        (ingest, query)
    }

    fn ryw_doc(id: &str, text: &str, status: &str) -> Document {
        Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", text), Field::keyword("status", status)],
        }
    }

    fn ryw_index(state: &AppState, ns: &str, docs: Vec<Document>) -> String {
        let body = serde_json::to_vec(&IndexRequest {
            namespace: NamespaceId(ns.into()),
            documents: docs,
        })
        .unwrap();
        handle_index(&body, state)
    }

    fn ryw_search(state: &AppState, body: serde_json::Value) -> serde_json::Value {
        let response = handle_search_post(body.to_string().as_bytes(), state);
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        serde_json::from_str(&response[response.find("\r\n\r\n").unwrap() + 4..]).unwrap()
    }

    fn hit_ids(result: &serde_json::Value) -> Vec<String> {
        result["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["doc_id"].as_str().unwrap().to_string())
            .collect()
    }

    /// #255 acceptance: `/index` then an immediate strong search on a query
    /// pod returns the document, with no flush; eventual does not.
    #[test]
    fn strong_search_on_query_pod_sees_unflushed_write() {
        let store = outage_store();
        let (ingest, query) = read_your_writes_pods(&store, |_, _| {});
        let ns = "tenant/ryw-index";
        let response = ryw_index(&ingest, ns, vec![ryw_doc("a", "hello world", "x")]);
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let ns_id = NamespaceId(ns.into());
        assert_eq!(ingest.indexer.pending_doc_count(&ns_id), 1, "not flushed");

        let strong = ryw_search(
            &query,
            serde_json::json!({"namespace": ns, "query_text": "hello", "consistency": "strong"}),
        );
        assert_eq!(hit_ids(&strong), ["a"]);
        assert_eq!(strong["total_hits"], 1);
        assert_eq!(strong["exhaustive_search_count"], 1);
        // Fields come back from the transient segment like any hit.
        assert_eq!(strong["results"][0]["fields"][0]["value"], "hello world");

        // The default is strong (the pod can read the tail).
        let default = ryw_search(
            &query,
            serde_json::json!({"namespace": ns, "query_text": "hello"}),
        );
        assert_eq!(hit_ids(&default), ["a"]);

        let eventual = ryw_search(
            &query,
            serde_json::json!({"namespace": ns, "query_text": "hello", "consistency": "eventual"}),
        );
        assert_eq!(eventual["total_hits"], 0);
        assert_eq!(eventual["exhaustive_search_count"], 0);

        // The ingest pod reads its own tail.
        let local = ryw_search(
            &ingest,
            serde_json::json!({"namespace": ns, "query_text": "hello"}),
        );
        assert_eq!(hit_ids(&local), ["a"]);
        assert_eq!(local["exhaustive_search_count"], 1);

        // The GET route takes it as a query parameter.
        let get = handle_search_get(
            "GET /search?ns=tenant/ryw-index&q=hello&consistency=strong HTTP/1.1",
            &query,
        );
        assert_eq!(total_hits(&get), 1);
        let get = handle_search_get(
            "GET /search?ns=tenant/ryw-index&q=hello&consistency=eventual HTTP/1.1",
            &query,
        );
        assert_eq!(total_hits(&get), 0);
        let bad = handle_search_get(
            "GET /search?ns=tenant/ryw-index&q=hello&consistency=sometimes HTTP/1.1",
            &query,
        );
        assert!(bad.starts_with("HTTP/1.1 400"), "{bad}");
        let bad = handle_search_post(
            br#"{"namespace":"tenant/ryw-index","query_text":"hello","consistency":"weak"}"#,
            &query,
        );
        assert!(bad.starts_with("HTTP/1.1 400"), "{bad}");

        // A second write changes the tail; the next search sees both, and
        // after a flush the documents come from the segment instead.
        ryw_index(&ingest, ns, vec![ryw_doc("b", "hello again", "x")]);
        let both = ryw_search(
            &query,
            serde_json::json!({"namespace": ns, "query_text": "hello"}),
        );
        assert_eq!(both["total_hits"], 2);
        assert_eq!(both["exhaustive_search_count"], 2);
        flush_and_publish_namespace(&ingest, &ns_id).unwrap();
        let flushed = ryw_search(
            &query,
            serde_json::json!({"namespace": ns, "query_text": "hello"}),
        );
        assert_eq!(flushed["total_hits"], 2, "no double count after the flush");
        assert_eq!(flushed["exhaustive_search_count"], 0);
    }

    /// #255 acceptance: an upsert shows only the newest version, whether
    /// the old one is unindexed or indexed.
    #[test]
    fn strong_search_returns_only_the_newest_version() {
        let store = outage_store();
        let (ingest, query) = read_your_writes_pods(&store, |_, _| {});
        let ns = "tenant/ryw-upsert";
        // Both versions unindexed.
        ryw_index(&ingest, ns, vec![ryw_doc("a", "alpha original", "x")]);
        ryw_index(&ingest, ns, vec![ryw_doc("a", "beta rewritten", "x")]);
        for state in [&*ingest, &query] {
            let all = ryw_search(
                state,
                serde_json::json!({"namespace": ns, "query_text": "", "filter": {"match_all": {}}}),
            );
            assert_eq!(hit_ids(&all), ["a"]);
            assert_eq!(all["results"][0]["fields"][0]["value"], "beta rewritten");
            let old = ryw_search(
                state,
                serde_json::json!({"namespace": ns, "query_text": "original"}),
            );
            assert_eq!(old["total_hits"], 0);
        }
        // Indexed old version, then an upsert.
        flush_and_publish_namespace(&ingest, &NamespaceId(ns.into())).unwrap();
        ryw_index(&ingest, ns, vec![ryw_doc("a", "gamma third", "x")]);
        let all = ryw_search(
            &query,
            serde_json::json!({"namespace": ns, "query_text": "", "filter": {"match_all": {}}}),
        );
        assert_eq!(hit_ids(&all), ["a"]);
        assert_eq!(all["total_hits"], 1);
        assert_eq!(all["results"][0]["fields"][0]["value"], "gamma third");
    }

    /// #255: the tail's masks hide the indexed copies it supersedes (the
    /// WAL-replay case, where an unindexed write replaces or deletes a
    /// document that is still live in a segment).
    #[test]
    fn tail_masks_hide_superseded_indexed_copies() {
        let store = outage_store();
        let (ingest, mut query) = ingest_and_query(&store, Some(Duration::from_secs(3600)));
        let ns = NamespaceId("tenant/ryw-mask".into());
        ingest
            .indexer
            .index_documents(
                ns.clone(),
                vec![
                    ryw_doc("a", "old words", "x"),
                    ryw_doc("b", "other words", "x"),
                ],
            )
            .unwrap();
        ingest.indexer.flush_namespace(&ns).unwrap();
        ingest.publish_namespace(&ns).unwrap();
        let manifest = ingest.indexer.manifest(&ns).unwrap();
        let segment = manifest.segments[0].segment_id.clone();
        let version = manifest.version;
        query.unindexed.fetcher = Some(Box::new(move |_, asked, known| {
            let mut tail = kosha_write::UnindexedTail {
                manifest_version: version,
                fingerprint: "crafted".into(),
                ..Default::default()
            };
            assert_eq!(asked, version);
            if known == Some("crafted") {
                tail.not_modified = true;
                return Ok(tail);
            }
            tail.documents = vec![ryw_doc("a", "new words", "x")];
            tail.deleted_ids = vec![DocumentId("b".into())];
            tail.masked = HashMap::from([(segment.clone(), vec![0, 1])]);
            Ok(tail)
        }));
        let body = serde_json::json!({"namespace": "tenant/ryw-mask", "query_text": "words"});
        for _ in 0..2 {
            // The second search reuses the built tail (`not_modified`).
            let strong = ryw_search(&query, body.clone());
            assert_eq!(hit_ids(&strong), ["a"]);
            assert_eq!(strong["total_hits"], 1);
            assert_eq!(strong["results"][0]["fields"][0]["value"], "new words");
        }
        let mut eventual = body.clone();
        eventual["consistency"] = "eventual".into();
        let eventual = ryw_search(&query, eventual);
        assert_eq!(eventual["total_hits"], 2);
        assert!(eventual["results"]
            .as_array()
            .unwrap()
            .iter()
            .any(|hit| hit["fields"][0]["value"] == "old words"));
    }

    /// #255 acceptance: deletes (by query, of unindexed and indexed
    /// documents) take effect for strong searches immediately.
    #[test]
    fn strong_search_hides_deleted_documents() {
        let store = outage_store();
        let (ingest, query) = read_your_writes_pods(&store, |_, _| {});
        let ns = "tenant/ryw-delete";
        ryw_index(&ingest, ns, vec![ryw_doc("old", "kept words", "drop")]);
        flush_and_publish_namespace(&ingest, &NamespaceId(ns.into())).unwrap();
        ryw_index(
            &ingest,
            ns,
            vec![
                ryw_doc("a", "kept words", "drop"),
                ryw_doc("b", "kept words", "keep"),
            ],
        );
        let before = ryw_search(
            &query,
            serde_json::json!({"namespace": ns, "query_text": "kept"}),
        );
        assert_eq!(before["total_hits"], 3);
        let delete = handle_delete(
            br#"{"namespace":"tenant/ryw-delete","filter":{"term":{"status":"drop"}}}"#,
            &ingest,
        );
        assert!(delete.starts_with("HTTP/1.1 200"), "{delete}");
        for state in [&*ingest, &query] {
            let after = ryw_search(
                state,
                serde_json::json!({"namespace": ns, "query_text": "kept"}),
            );
            assert_eq!(hit_ids(&after), ["b"]);
            assert_eq!(after["total_hits"], 1);
        }
    }

    /// #255: a flush that publishes between the query pod's manifest
    /// snapshot and its tail fetch is neither missed nor double-counted:
    /// the stale tail is refused, the manifest refreshed and the tail
    /// fetched again.
    #[test]
    fn strong_search_survives_a_flush_racing_the_tail_fetch() {
        let store = outage_store();
        let race = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let armed = Arc::clone(&race);
        let (ingest, query) = read_your_writes_pods(&store, move |writer, ns| {
            if armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                flush_and_publish_namespace(writer, ns).unwrap();
            }
        });
        let ns = "tenant/ryw-race";
        ryw_index(&ingest, ns, vec![ryw_doc("a", "racing words", "x")]);
        // Load the namespace on the query pod at the pre-flush version.
        let first = ryw_search(
            &query,
            serde_json::json!({"namespace": ns, "query_text": "racing"}),
        );
        assert_eq!(first["total_hits"], 1);
        ryw_index(&ingest, ns, vec![ryw_doc("b", "racing words", "x")]);
        race.store(true, std::sync::atomic::Ordering::SeqCst);
        let raced = ryw_search(
            &query,
            serde_json::json!({"namespace": ns, "query_text": "racing"}),
        );
        assert!(
            !race.load(std::sync::atomic::Ordering::SeqCst),
            "the race ran"
        );
        assert_eq!(raced["total_hits"], 2);
        assert_eq!(hit_ids(&raced).len(), 2);
        assert_eq!(raced["exhaustive_search_count"], 0, "both now indexed");
    }

    /// Local GC and tail eviction release a shard's segment under the
    /// sharded root's merged-view id too (#207).
    #[test]
    fn shard_segments_alias_to_the_merged_view() {
        let seg = kosha_core::SegmentId("S".into());
        let shard = kosha_core::sharding::shard_namespace(&NamespaceId("t/ns".into()), 3);
        let (root, merged) = merged_view_alias(&shard, &seg).unwrap();
        assert_eq!(root.0, "t/ns");
        assert_eq!(merged, kosha_core::sharding::merged_segment_id(3, &seg));
        assert!(merged_view_alias(&NamespaceId("t/ns".into()), &seg).is_none());
    }

    /// #255: a writer that flushed but has not published (an upload in
    /// flight, or publication failing) is ahead of every manifest a query
    /// pod can read. A strong search there is served from the published
    /// manifest, flagged, instead of failing with 503 for the whole window.
    #[test]
    fn strong_search_falls_back_to_eventual_while_publication_is_pending() {
        let store = outage_store();
        let (ingest, query) = read_your_writes_pods(&store, |_, _| {});
        let ns = "tenant/ryw-pending";
        let ns_id = NamespaceId(ns.into());
        ryw_index(&ingest, ns, vec![ryw_doc("a", "pending words", "x")]);
        flush_and_publish_namespace(&ingest, &ns_id).unwrap();
        ryw_index(&ingest, ns, vec![ryw_doc("b", "pending words", "x")]);
        // Flushed locally, not published.
        ingest.indexer.flush_namespace(&ns_id).unwrap();

        let pending = ryw_search(
            &query,
            serde_json::json!({"namespace": ns, "query_text": "pending"}),
        );
        assert_eq!(hit_ids(&pending), ["a"]);
        let reason = pending["consistency_fallback"].as_str().unwrap();
        assert!(reason.starts_with("eventual"), "{reason}");

        flush_and_publish_namespace(&ingest, &ns_id).unwrap();
        let published = ryw_search(
            &query,
            serde_json::json!({"namespace": ns, "query_text": "pending"}),
        );
        assert_eq!(published["total_hits"], 2);
        assert!(published.get("consistency_fallback").is_none());
    }

    /// #255: under strong consistency a tail that cannot be read fails the
    /// search; it never silently degrades to eventual.
    #[test]
    fn strong_search_fails_when_the_tail_is_unreadable() {
        let store = outage_store();
        let (ingest, mut query) = ingest_and_query(&store, Some(Duration::from_secs(3600)));
        index_text_and_publish(&ingest, &NamespaceId("tenant/ryw-down".into()), &["a"]);
        // No internal credential and no fetcher: the default is eventual,
        // an explicit strong request is refused with the reason.
        let plain = br#"{"namespace":"tenant/ryw-down","query_text":"hello"}"#;
        assert_eq!(total_hits(&handle_search_post(plain, &query)), 1);
        let strong =
            br#"{"namespace":"tenant/ryw-down","query_text":"hello","consistency":"strong"}"#;
        let refused = handle_search_post(strong, &query);
        assert!(refused.starts_with("HTTP/1.1 503"), "{refused}");
        assert!(refused.contains("KOSHA_INTERNAL_API_KEY"), "{refused}");
        // A failing fetch is a 503 too.
        query.unindexed.fetcher = Some(Box::new(|_, _, _| {
            Err(KoshaError::Io(std::io::Error::other("ingest down")))
        }));
        let failed = handle_search_post(strong, &query);
        assert!(failed.starts_with("HTTP/1.1 503"), "{failed}");
        assert!(failed.contains("ingest down"), "{failed}");
        let eventual =
            br#"{"namespace":"tenant/ryw-down","query_text":"hello","consistency":"eventual"}"#;
        assert_eq!(total_hits(&handle_search_post(eventual, &query)), 1);
        // The internal route needs the internal credential.
        let denied = route(
            "GET /internal/namespaces/tenant%2Fryw-down/unindexed HTTP/1.1",
            &HashMap::new(),
            b"",
            "tenant",
            &ingest,
        );
        assert!(denied.starts_with("HTTP/1.1 403"), "{denied}");
    }

    /// #255 acceptance: past the unindexed cap `/index` and `/replace`
    /// answer 429 with a retry hint until a flush drains the backlog.
    #[test]
    fn unindexed_backlog_cap_backpressures_writes() {
        let mut state = test_state();
        state.unindexed_cap = unindexed::UnindexedCap {
            max_bytes: 0,
            max_docs: Some(2),
        };
        let ns = "tenant/ryw-cap";
        let ok = ryw_index(
            &state,
            ns,
            vec![ryw_doc("a", "one", "x"), ryw_doc("b", "two", "x")],
        );
        assert!(ok.starts_with("HTTP/1.1 200"), "{ok}");
        let refused = ryw_index(&state, ns, vec![ryw_doc("c", "three", "x")]);
        assert!(refused.starts_with("HTTP/1.1 429"), "{refused}");
        assert!(refused.contains("\r\nretry-after: 1\r\n"), "{refused}");
        assert!(refused.contains("KOSHA_MAX_UNINDEXED_DOCS"), "{refused}");
        let replace = serde_json::to_vec(&IndexRequest {
            namespace: NamespaceId(ns.into()),
            documents: vec![ryw_doc("a", "patched", "x")],
        })
        .unwrap();
        assert!(handle_replace(&replace, &state).starts_with("HTTP/1.1 429"));
        let stats = stats_json(&state);
        let entry = stats["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["namespace"] == ns)
            .unwrap()
            .clone();
        assert_eq!(entry["unindexed_documents"], 2);
        assert!(entry["unindexed_bytes"].as_u64().unwrap() > 0);

        flush_and_publish_namespace(&state, &NamespaceId(ns.into())).unwrap();
        let accepted = ryw_index(&state, ns, vec![ryw_doc("c", "three", "x")]);
        assert!(accepted.starts_with("HTTP/1.1 200"), "{accepted}");

        // The byte cap works the same way.
        state.unindexed_cap = unindexed::UnindexedCap {
            max_bytes: 1,
            max_docs: None,
        };
        assert!(ryw_index(&state, ns, vec![ryw_doc("d", "four", "x")]).starts_with("HTTP/1.1 429"));
    }

    /// #255 acceptance: `search_after` pages and capped `total_hits` stay
    /// coherent when unindexed hits are merged with indexed ones.
    #[test]
    fn strong_search_paginates_across_indexed_and_unindexed_hits() {
        let store = outage_store();
        let (ingest, query) = read_your_writes_pods(&store, |_, _| {});
        let ns = "tenant/ryw-page";
        let docs = |ids: &[&str]| {
            ids.iter()
                .map(|id| ryw_doc(id, "page words", "x"))
                .collect()
        };
        ryw_index(&ingest, ns, docs(&["d01", "d03", "d05", "d07", "d09"]));
        flush_and_publish_namespace(&ingest, &NamespaceId(ns.into())).unwrap();
        ryw_index(&ingest, ns, docs(&["d02", "d04", "d06", "d08", "d10"]));

        let mut seen = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut body = serde_json::json!({
                "namespace": ns,
                "query_text": "page",
                "max_results": 3,
                "sort": [{"_id": {"order": "asc"}}],
            });
            if let Some(ref after) = cursor {
                body["search_after"] = serde_json::json!([after]);
            }
            let page = ryw_search(&query, body);
            assert_eq!(page["total_hits"], 10);
            assert_eq!(page["exhaustive_search_count"], 5);
            let ids = hit_ids(&page);
            if ids.is_empty() {
                break;
            }
            cursor = ids.last().cloned();
            seen.extend(ids);
        }
        let expected: Vec<String> = (1..=10).map(|i| format!("d{i:02}")).collect();
        assert_eq!(seen, expected, "every document once, in order");

        let capped = ryw_search(
            &query,
            serde_json::json!({"namespace": ns, "query_text": "page", "total_hits_cap": 7}),
        );
        assert_eq!(capped["total_hits"], 7);
        assert_eq!(capped["total_hits_relation"], "gte");
        let exact = ryw_search(
            &query,
            serde_json::json!({"namespace": ns, "query_text": "page", "exact_total_hits": true}),
        );
        assert_eq!(exact["total_hits"], 10);
        assert_eq!(exact["total_hits_relation"], "eq");
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

    // ─── Namespace sharding (#207) ──────────────────────────────────────

    fn shard_v1(state: &AppState, line: &str, body: serde_json::Value) -> serde_json::Value {
        let response = route(
            &format!("{line} HTTP/1.1\r\n"),
            &HashMap::new(),
            &serde_json::to_vec(&body).unwrap(),
            "acme",
            state,
        );
        assert!(response.starts_with("HTTP/1.1 200"), "{line}: {response}");
        json_body(&response)
    }

    /// A small corpus with skewed term frequencies, a keyword, a number and
    /// a few duplicate-scoring documents, so ranking, ties and filters all
    /// get exercised.
    fn sharding_corpus(range: std::ops::Range<usize>) -> serde_json::Value {
        const WORDS: [&str; 8] = [
            "contract", "breach", "court", "damages", "appeal", "motion", "witness", "evidence",
        ];
        let docs: Vec<serde_json::Value> = range
            .map(|i| {
                let mut seed = (i as u64).wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
                let mut text: Vec<&str> = (0..(i % 7 + 2))
                    .map(|_| {
                        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
                        WORDS[(seed >> 33) as usize % 8]
                    })
                    .collect();
                if i % 5 == 0 {
                    text.push("contract");
                }
                let kind = ["memo", "brief", "order"][i % 3];
                serde_json::json!({
                    "id": format!("doc-{i:04}"),
                    "fields": [
                        {"name": "body", "field_type": "Text", "value": text.join(" ")},
                        {"name": "kind", "field_type": "Keyword", "value": kind},
                        {"name": "rank", "field_type": "Integer", "value": format!("{}", (i * 37) % 101)},
                    ]
                })
            })
            .collect();
        serde_json::json!({ "documents": docs })
    }

    fn hits(result: &serde_json::Value) -> Vec<(String, f64)> {
        result["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| {
                (
                    d["doc_id"].as_str().unwrap().to_string(),
                    d["score"].as_f64().unwrap(),
                )
            })
            .collect()
    }

    /// Index, update, delete, flush and compact the same writes into an
    /// unsharded and a 4-shard namespace.
    fn load_plain_and_sharded(state: &AppState) {
        let created = shard_v1(
            state,
            "POST /v1/namespaces/sharded/shards",
            serde_json::json!({"shards": 4}),
        );
        assert_eq!(created["changed"], true);
        for ns in ["plain", "sharded"] {
            for batch in [0..120, 120..250, 250..300] {
                shard_v1(
                    state,
                    &format!("POST /v1/namespaces/{ns}/documents"),
                    sharding_corpus(batch),
                );
                shard_v1(
                    state,
                    &format!("POST /v1/namespaces/{ns}/flush"),
                    serde_json::json!({}),
                );
            }
            // Upserts of existing ids (they must replace, not duplicate).
            let mut update = sharding_corpus(0..40);
            for doc in update["documents"].as_array_mut().unwrap() {
                doc["fields"][0]["value"] = serde_json::json!("appeal appeal granted");
            }
            shard_v1(
                state,
                &format!("POST /v1/namespaces/{ns}/documents"),
                update,
            );
            shard_v1(
                state,
                &format!("POST /v1/namespaces/{ns}/flush"),
                serde_json::json!({}),
            );
            let deleted = shard_v1(
                state,
                &format!("POST /v1/namespaces/{ns}/delete"),
                serde_json::json!({"filter": {"term": {"kind": "order"}}}),
            );
            assert_eq!(deleted["deleted"], 100, "{ns}: {deleted}");
        }
    }

    #[test]
    fn sharded_namespace_matches_unsharded_results() {
        let state = test_state();
        load_plain_and_sharded(&state);

        // Writes really were spread over the shards, each with its own
        // manifest and segments.
        let stats = json_body(&handle_namespace_stats("sharded", "acme", &state));
        assert_eq!(stats["shards"], 4);
        let per_shard = stats["shard_stats"].as_array().unwrap();
        assert_eq!(per_shard.len(), 4);
        for shard in per_shard {
            assert!(shard["segments"].as_u64().unwrap() >= 2, "{stats}");
        }
        let plain_stats = json_body(&handle_namespace_stats("plain", "acme", &state));
        assert_eq!(stats["documents"], plain_stats["documents"]);
        let root = state
            .indexer
            .manifest_shared(&NamespaceId("acme/sharded".into()))
            .unwrap();
        assert!(root.segments.is_empty(), "the root holds no segments");

        let queries = [
            serde_json::json!({"query_text": "contract", "max_results": 400}),
            serde_json::json!({"query_text": "appeal witness", "max_results": 25}),
            serde_json::json!({"query_text": "court damages evidence", "max_results": 10, "from": 5}),
            serde_json::json!({"query_text": "motion", "max_results": 400,
                "filter": {"term": {"kind": "memo"}}}),
            serde_json::json!({"query_text": "", "max_results": 50, "filter": {"match_all": {}},
                "sort": [{"rank": {"order": "desc"}}, {"_id": {"order": "asc"}}]}),
            serde_json::json!({"query_text": "breach", "max_results": 5, "exact_total_hits": false,
                "aggs": {"kinds": {"terms": {"field": "kind"}}}}),
        ];
        let check = |phase: &str| {
            for query in &queries {
                let plain = shard_v1(&state, "POST /v1/namespaces/plain/search", query.clone());
                let sharded = shard_v1(&state, "POST /v1/namespaces/sharded/search", query.clone());
                assert_eq!(hits(&plain), hits(&sharded), "{phase}: {query}");
                assert_eq!(
                    plain["total_hits"], sharded["total_hits"],
                    "{phase}: {query}"
                );
                assert_eq!(
                    plain["total_hits_relation"], sharded["total_hits_relation"],
                    "{phase}: {query}"
                );
                assert_eq!(
                    plain["aggregations"], sharded["aggregations"],
                    "{phase}: {query}"
                );
                assert!(
                    !hits(&plain).is_empty() || query["query_text"] == "",
                    "{query}"
                );
            }
        };
        check("after flush");

        // Compact both (the sharded root compacts every shard) and compare
        // again.
        for ns in ["acme/plain", "acme/sharded"] {
            let response = handle_compact_namespace(
                &serde_json::to_vec(&serde_json::json!({"namespace": ns, "mode": "full"})).unwrap(),
                "acme",
                &state,
            );
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        }
        let stats = json_body(&handle_namespace_stats("sharded", "acme", &state));
        for shard in stats["shard_stats"].as_array().unwrap() {
            assert_eq!(shard["segments"], 1, "{stats}");
        }
        check("after compaction");

        // Exists is answered by the shard each id routes to.
        let exists = shard_v1(
            &state,
            "POST /v1/namespaces/sharded/exists",
            serde_json::json!({"ids": ["doc-0000", "doc-0002", "doc-0298", "nope"]}),
        );
        // doc-0002 was a deleted `order`.
        assert_eq!(
            exists["existing"],
            serde_json::json!(["doc-0000", "doc-0298"])
        );
        assert_eq!(exists["missing"], serde_json::json!(["doc-0002", "nope"]));
    }

    #[test]
    fn sharded_pagination_returns_each_document_once() {
        let state = test_state();
        load_plain_and_sharded(&state);
        for sort in [
            serde_json::json!([{"rank": {"order": "desc"}}, {"_id": {"order": "asc"}}]),
            serde_json::json!([{"_id": {"order": "asc"}}]),
        ] {
            let mut seen = Vec::new();
            let mut plain_seen = Vec::new();
            let mut after: Option<serde_json::Value> = None;
            loop {
                let mut query = serde_json::json!({"query_text": "", "max_results": 7,
                    "filter": {"match_all": {}}, "sort": sort});
                if let Some(ref after) = after {
                    query["search_after"] = after.clone();
                }
                let page = shard_v1(&state, "POST /v1/namespaces/sharded/search", query.clone());
                let plain = shard_v1(&state, "POST /v1/namespaces/plain/search", query);
                let results = page["results"].as_array().unwrap().clone();
                assert_eq!(hits(&page), hits(&plain));
                if results.is_empty() {
                    break;
                }
                let last = results.last().unwrap();
                let id = last["doc_id"].as_str().unwrap().to_string();
                after = Some(if sort.as_array().unwrap().len() == 2 {
                    let rank = last["fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|f| f["name"] == "rank")
                        .unwrap()["value"]
                        .clone();
                    serde_json::json!([rank, id])
                } else {
                    serde_json::json!([id])
                });
                seen.extend(hits(&page).into_iter().map(|(id, _)| id));
                plain_seen.extend(hits(&plain).into_iter().map(|(id, _)| id));
            }
            let unique: std::collections::HashSet<&String> = seen.iter().collect();
            assert_eq!(
                unique.len(),
                seen.len(),
                "a document was returned twice: {sort}"
            );
            assert_eq!(seen.len(), 200, "every live document exactly once: {sort}");
            assert_eq!(seen, plain_seen);
        }
    }

    /// Read-your-writes (#255) over a sharded namespace (#207): every
    /// shard's unindexed tail is searched, relative to its own manifest.
    #[test]
    fn strong_search_sees_unflushed_writes_in_every_shard() {
        let state = test_state();
        shard_v1(
            &state,
            "POST /v1/namespaces/live/shards",
            serde_json::json!({"shards": 4}),
        );
        shard_v1(
            &state,
            "POST /v1/namespaces/live/documents",
            sharding_corpus(0..20),
        );
        shard_v1(
            &state,
            "POST /v1/namespaces/live/flush",
            serde_json::json!({}),
        );
        shard_v1(
            &state,
            "POST /v1/namespaces/live/documents",
            sharding_corpus(20..40),
        );
        let count = |consistency: &str| {
            shard_v1(
                &state,
                "POST /v1/namespaces/live/search",
                serde_json::json!({"query_text": "", "max_results": 100,
                                   "filter": {"terms": {"kind": ["memo", "brief", "order"]}},
                                   "consistency": consistency}),
            )["total_hits"]
                .as_u64()
                .unwrap()
        };
        let strong = shard_v1(
            &state,
            "POST /v1/namespaces/live/search",
            serde_json::json!({"query_text": "", "max_results": 100,
                               "filter": {"terms": {"kind": ["memo", "brief", "order"]}}}),
        );
        assert_eq!(strong["total_hits"], 40, "{strong}");
        assert_eq!(strong["exhaustive_search_count"], 20, "{strong}");
        assert_eq!(count("eventual"), 20);
        shard_v1(
            &state,
            "POST /v1/namespaces/live/flush",
            serde_json::json!({}),
        );
        assert_eq!(count("strong"), 40);
        assert_eq!(count("eventual"), 40);
    }

    #[test]
    fn deleting_a_sharded_namespace_deletes_its_shards() {
        let state = test_state();
        shard_v1(
            &state,
            "POST /v1/namespaces/gone/shards",
            serde_json::json!({"shards": 3}),
        );
        shard_v1(
            &state,
            "POST /v1/namespaces/gone/documents",
            sharding_corpus(0..30),
        );
        shard_v1(
            &state,
            "POST /v1/namespaces/gone/flush",
            serde_json::json!({}),
        );
        let root = NamespaceId("acme/gone".into());
        let shards = kosha_core::sharding::shard_namespaces(&root, 3);
        assert!(shards
            .iter()
            .any(|shard| state.indexer.manifest_version(shard).is_some()));
        let response = route(
            "DELETE /v1/namespaces/gone HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "acme",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        for shard in &shards {
            assert!(
                state.indexer.manifest(shard).is_none(),
                "{} outlived its root",
                shard.0
            );
            assert!(
                state
                    .controller
                    .lock()
                    .unwrap()
                    .manifest_cloned(shard)
                    .is_none(),
                "{} still in the control store",
                shard.0
            );
        }
    }

    #[test]
    fn shard_count_is_fixed_at_creation() {
        let state = test_state();
        // A namespace that already has documents cannot be sharded.
        shard_v1(
            &state,
            "POST /v1/namespaces/full/documents",
            sharding_corpus(0..3),
        );
        let refused = route(
            "POST /v1/namespaces/full/shards HTTP/1.1\r\n",
            &HashMap::new(),
            br#"{"shards": 4}"#,
            "acme",
            &state,
        );
        assert!(refused.starts_with("HTTP/1.1 400"), "{refused}");
        // Asking for one shard is an ordinary namespace: nothing changes.
        assert_eq!(
            shard_v1(
                &state,
                "POST /v1/namespaces/full/shards",
                serde_json::json!({"shards": 1})
            )["changed"],
            false
        );

        assert_eq!(
            shard_v1(
                &state,
                "POST /v1/namespaces/fresh/shards",
                serde_json::json!({"shards": 3})
            )["changed"],
            true
        );
        assert_eq!(
            shard_v1(
                &state,
                "POST /v1/namespaces/fresh/shards",
                serde_json::json!({"shards": 3})
            )["changed"],
            false
        );
        for bad in [
            r#"{"shards": 5}"#,
            r#"{"shards": 1}"#,
            r#"{"shards": 0}"#,
            r#"{"shards": 9999}"#,
            r#"{}"#,
        ] {
            let response = route(
                "POST /v1/namespaces/fresh/shards HTTP/1.1\r\n",
                &HashMap::new(),
                bad.as_bytes(),
                "acme",
                &state,
            );
            assert!(response.starts_with("HTTP/1.1 400"), "{bad}: {response}");
        }
        // The root refuses direct writes; every shard is registered in the
        // control store and the root manifest records the count.
        let root = NamespaceId("acme/fresh".into());
        assert!(state
            .indexer
            .index_documents(
                root.clone(),
                vec![Document {
                    id: DocumentId("x".into()),
                    fields: vec![Field::text("body", "x")],
                }]
            )
            .is_err());
        let controller = state.controller.lock().unwrap();
        assert_eq!(controller.manifest_cloned(&root).unwrap().shards, Some(3));
        for shard in kosha_core::sharding::shard_namespaces(&root, 3) {
            assert!(controller.has_namespace(&shard), "{}", shard.0);
        }
    }

    #[test]
    fn sharded_namespace_inherits_and_propagates_schema() {
        let state = test_state();
        let schema = serde_json::json!({"fields": {"name": {"analyzer": ["standard", "stem"]}}});
        shard_v1(&state, "POST /v1/namespaces/s/schema", schema.clone());
        shard_v1(
            &state,
            "POST /v1/namespaces/s/shards",
            serde_json::json!({"shards": 2}),
        );
        shard_v1(&state, "POST /v1/namespaces/p/schema", schema);
        let root = NamespaceId("acme/s".into());
        for shard in kosha_core::sharding::shard_namespaces(&root, 2) {
            assert!(state
                .indexer
                .manifest_shared(&shard)
                .unwrap()
                .schema
                .is_some());
        }
        // The analyzer reaches every shard's segments.
        let docs = serde_json::json!({"documents": (0..20).map(|i| serde_json::json!({
            "id": format!("n{i}"),
            "fields": [{"name": "name", "field_type": "Text",
                "value": if i % 4 == 0 { "running late" } else { "walked home" }}],
        })).collect::<Vec<_>>()});
        for ns in ["s", "p"] {
            shard_v1(
                &state,
                &format!("POST /v1/namespaces/{ns}/documents"),
                docs.clone(),
            );
            shard_v1(
                &state,
                &format!("POST /v1/namespaces/{ns}/flush"),
                serde_json::json!({}),
            );
        }
        let query = serde_json::json!({"query_text": "runs", "max_results": 50});
        let sharded = shard_v1(&state, "POST /v1/namespaces/s/search", query.clone());
        let plain = shard_v1(&state, "POST /v1/namespaces/p/search", query);
        assert_eq!(sharded["total_hits"], 5, "{sharded}");
        assert_eq!(hits(&sharded), hits(&plain));

        // A later schema change reaches every shard too.
        let other = serde_json::json!({"fields": {"name": {"analyzer": ["standard"]}}});
        shard_v1(&state, "POST /v1/namespaces/s/schema", other);
        let root_schema = state.indexer.manifest_shared(&root).unwrap().schema.clone();
        assert!(root_schema.is_some());
        for shard in kosha_core::sharding::shard_namespaces(&root, 2) {
            assert_eq!(
                state.indexer.manifest_shared(&shard).unwrap().schema,
                root_schema
            );
        }
    }

    /// A query replica follows the root and every shard manifest from the
    /// control store and searches the merged view.
    #[test]
    fn query_replica_searches_sharded_namespace() {
        let store = outage_store();
        let (ingest, query) = ingest_and_query(&store, None);
        shard_v1(
            &ingest,
            "POST /v1/namespaces/docs/shards",
            serde_json::json!({"shards": 4}),
        );
        shard_v1(
            &ingest,
            "POST /v1/namespaces/docs/documents",
            sharding_corpus(0..60),
        );
        shard_v1(
            &ingest,
            "POST /v1/namespaces/docs/flush",
            serde_json::json!({}),
        );
        let body = serde_json::json!({"query_text": "contract", "max_results": 100});
        let on_ingest = shard_v1(&ingest, "POST /v1/namespaces/docs/search", body.clone());
        let on_query = shard_v1(&query, "POST /v1/namespaces/docs/search", body.clone());
        assert!(!hits(&on_ingest).is_empty());
        assert_eq!(hits(&on_ingest), hits(&on_query));

        // A later publish to one shard is picked up too.
        shard_v1(
            &ingest,
            "POST /v1/namespaces/docs/documents",
            sharding_corpus(60..90),
        );
        shard_v1(
            &ingest,
            "POST /v1/namespaces/docs/flush",
            serde_json::json!({}),
        );
        let on_ingest = shard_v1(&ingest, "POST /v1/namespaces/docs/search", body.clone());
        let on_query = shard_v1(&query, "POST /v1/namespaces/docs/search", body);
        assert_eq!(hits(&on_ingest), hits(&on_query));
        assert_eq!(on_ingest["total_hits"], on_query["total_hits"]);
    }

    /// Manifests written before sharding existed carry no `shards` field and
    /// keep behaving as ordinary namespaces.
    #[test]
    fn manifests_without_a_shard_count_stay_unsharded() {
        let legacy: kosha_core::Manifest =
            serde_json::from_str(r#"{"version": 3, "segments": []}"#).unwrap();
        assert_eq!(legacy.shards, None);
        assert!(!serde_json::to_string(&legacy).unwrap().contains("shards"));
        let state = test_state();
        let ns = NamespaceId("acme/legacy".into());
        state
            .controller
            .lock()
            .unwrap()
            .save_manifest(&ns, &legacy)
            .unwrap();
        state.indexer.restore_manifest(ns.clone(), legacy);
        assert_eq!(state.indexer.shard_count(&ns), None);
        shard_v1(
            &state,
            "POST /v1/namespaces/legacy/documents",
            sharding_corpus(0..5),
        );
        shard_v1(
            &state,
            "POST /v1/namespaces/legacy/flush",
            serde_json::json!({}),
        );
        let result = shard_v1(
            &state,
            "POST /v1/namespaces/legacy/search",
            serde_json::json!({"query_text": "contract"}),
        );
        assert!(result["total_hits"].as_u64().unwrap() > 0);
    }

    // ─── Dashboard (#214) ────────────────────────────────────────────────

    fn dashboard_state(enabled: bool) -> AppState {
        let mut state = test_state();
        state.dashboard = dashboard::Dashboard::new(enabled);
        state
    }

    /// One request through `dispatch`, the entry point every socket
    /// request takes, with no credentials at all.
    fn raw_request(state: &AppState, request: &str) -> String {
        dispatch(state, test_request(request)).into_raw_string()
    }

    fn header_value<'a>(response: &'a str, name: &str) -> Option<&'a str> {
        let head = response.split("\r\n\r\n").next()?;
        head.lines().skip(1).find_map(|line| {
            let (k, v) = line.split_once(':')?;
            k.trim().eq_ignore_ascii_case(name).then(|| v.trim())
        })
    }

    #[test]
    fn dashboard_is_404_when_off_and_served_without_a_key_when_on() {
        let off = dashboard_state(false);
        let resp = raw_request(&off, "GET /dashboard HTTP/1.1\r\n\r\n");
        assert!(resp.starts_with("HTTP/1.1 404"), "{resp}");

        let on = dashboard_state(true);
        let resp = raw_request(&on, "GET /dashboard HTTP/1.1\r\n\r\n");
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        assert!(header_value(&resp, "content-type")
            .unwrap()
            .starts_with("text/html"));
        assert!(resp.contains("<title>Kosha dashboard</title>"));
        for asset in ["/dashboard/app.js", "/dashboard/style.css"] {
            let resp = raw_request(&on, &format!("GET {asset} HTTP/1.1\r\n\r\n"));
            assert!(resp.starts_with("HTTP/1.1 200"), "{asset}: {resp}");
            let csp = header_value(&resp, "content-security-policy").unwrap();
            assert!(csp.starts_with("default-src 'self'"), "{csp}");
            assert_eq!(header_value(&resp, "x-frame-options"), Some("DENY"));
        }
    }

    #[test]
    fn list_namespaces_is_tenant_scoped_and_hides_shards() {
        let state = test_state();
        for (tenant, ns) in [("acme", "docs"), ("acme", "logs"), ("globex", "secret")] {
            let resp = v1(
                &state,
                tenant,
                "POST /v1/namespaces",
                serde_json::json!({ "namespace": ns }),
            );
            assert_eq!(status_of(&resp), 200, "{resp}");
        }
        v1(
            &state,
            "acme",
            "POST /v1/namespaces/docs/documents",
            one_doc("d1", "breach of contract"),
        );
        v1(
            &state,
            "acme",
            "POST /v1/namespaces/docs/flush",
            serde_json::json!({}),
        );
        // A sharded namespace: its shards are internal namespaces.
        shard_v1(
            &state,
            "POST /v1/namespaces/sharded/shards",
            serde_json::json!({"shards": 2}),
        );
        shard_v1(
            &state,
            "POST /v1/namespaces/sharded/documents",
            sharding_corpus(0..20),
        );
        shard_v1(
            &state,
            "POST /v1/namespaces/sharded/flush",
            serde_json::json!({}),
        );
        assert!(state
            .controller
            .lock()
            .unwrap()
            .list_namespaces()
            .iter()
            .any(|ns| ns.0.starts_with("acme/sharded/_shard-")));

        let resp = v1(
            &state,
            "acme",
            "GET /v1/namespaces",
            serde_json::Value::Null,
        );
        assert_eq!(status_of(&resp), 200, "{resp}");
        let listed = body_of(&resp)["namespaces"].as_array().unwrap().clone();
        let names: Vec<&str> = listed.iter().map(|n| n["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["docs", "logs", "sharded"], "{resp}");
        let docs = &listed[0];
        assert_eq!(docs["documents"], 1);
        assert_eq!(docs["segments"], 1);
        assert!(docs["manifest_version"].as_u64().unwrap() > 0);
        assert_eq!(listed[1]["documents"], 0);
        // A sharded namespace reports totals over its shards.
        assert_eq!(listed[2]["documents"], 20, "{resp}");
        assert!(!resp.contains("_shard-"), "{resp}");

        let resp = v1(
            &state,
            "globex",
            "GET /v1/namespaces",
            serde_json::Value::Null,
        );
        let listed = body_of(&resp)["namespaces"].clone();
        assert_eq!(
            listed,
            serde_json::json!([{
                "name": "secret", "documents": 0, "segments": 0,
                "manifest_version": listed[0]["manifest_version"],
            }])
        );
        assert!(!resp.contains("docs") && !resp.contains("acme"), "{resp}");

        let resp = v1(
            &state,
            "initech",
            "GET /v1/namespaces",
            serde_json::Value::Null,
        );
        assert_eq!(body_of(&resp)["namespaces"], serde_json::json!([]));
        // The bare per-namespace path is still not a route.
        let resp = v1(
            &state,
            "acme",
            "GET /v1/namespaces/docs",
            serde_json::Value::Null,
        );
        assert_eq!(status_of(&resp), 404, "{resp}");
    }

    #[test]
    fn hydration_outcome_with_no_fetch_is_fully_cached() {
        let o = HydrationOutcome::default();
        assert!(o.fully_cached());
        assert_eq!(o.s3_rounds(), 0);
        assert_eq!(o.s3_bytes(), 0);
        // Bloom pruning narrows what is checked, it fetches nothing.
        let pruned = HydrationOutcome {
            bloom_pruned: true,
            ..Default::default()
        };
        assert!(pruned.fully_cached());
    }

    #[test]
    fn any_fetch_missing_file_or_shed_is_not_fully_cached() {
        type SetPosting = fn(&mut posting_spans::PostingFetchStats);
        type SetPage = fn(&mut doc_spans::PageFetchStats);
        let one = |f: fn(&mut HydrationOutcome)| {
            let mut o = HydrationOutcome::default();
            f(&mut o);
            o
        };
        let mut cases: Vec<(String, HydrationOutcome)> = vec![
            ("missing".into(), one(|o| o.missing = vec!["seg/f".into()])),
            ("shed".into(), one(|o| o.shed = true)),
            ("files_fetched".into(), one(|o| o.files_fetched = 1)),
            ("bytes_fetched".into(), one(|o| o.bytes_fetched = 1)),
            ("rounds".into(), one(|o| o.rounds = 1)),
            (
                "postings.ranged_gets".into(),
                one(|o| o.postings.ranged_gets = 1),
            ),
            (
                "postings.whole_files".into(),
                one(|o| o.postings.whole_files = 1),
            ),
        ];
        let posting_fields: [(&str, SetPosting); 5] = [
            ("rounds", |s| s.rounds = 1),
            ("ranged_gets", |s| s.ranged_gets = 1),
            ("ranged_bytes", |s| s.ranged_bytes = 1),
            ("whole_files", |s| s.whole_files = 1),
            ("whole_bytes", |s| s.whole_bytes = 1),
        ];
        for (name, set) in posting_fields {
            let mut o = HydrationOutcome::default();
            set(&mut o.vectors);
            cases.push((format!("vectors.{name}"), o));
        }
        let page_fields: [(&str, SetPage); 5] = [
            ("rounds", |s| s.rounds = 1),
            ("ranged_gets", |s| s.ranged_gets = 1),
            ("ranged_bytes", |s| s.ranged_bytes = 1),
            ("whole_files", |s| s.whole_files = 1),
            ("whole_bytes", |s| s.whole_bytes = 1),
        ];
        for (name, set) in page_fields {
            let mut o = HydrationOutcome::default();
            set(&mut o.page);
            cases.push((format!("page.{name}"), o));
        }
        for (name, o) in &cases {
            assert!(!o.fully_cached(), "{name}");
        }
    }

    #[test]
    fn s3_totals_add_every_path_once() {
        let o = HydrationOutcome {
            // Whole files plus ranged postings bytes, which the postings
            // pass already folds into `bytes_fetched`.
            bytes_fetched: 1_000,
            rounds: 2,
            postings: posting_spans::PostingFetchStats {
                rounds: 1,
                ranged_gets: 3,
                ranged_bytes: 400,
                ..Default::default()
            },
            vectors: posting_spans::PostingFetchStats {
                rounds: 1,
                ranged_gets: 2,
                ranged_bytes: 20,
                whole_files: 1,
                whole_bytes: 300,
            },
            page: doc_spans::PageFetchStats {
                rounds: 1,
                ranged_gets: 1,
                ranged_bytes: 5,
                whole_files: 1,
                whole_bytes: 7_000,
            },
            ..Default::default()
        };
        assert_eq!(o.s3_rounds(), 2 + 1 + 1);
        assert_eq!(o.s3_bytes(), 1_000 + 20 + 300 + 5 + 7_000);
    }

    #[test]
    fn a_degraded_search_is_not_fully_cached() {
        let local = HydrationOutcome::default();
        let phases = kosha_query::SearchPhaseStats::default();
        assert!(search_fully_cached(&local, &phases, 0));
        assert!(!search_fully_cached(&local, &phases, 1));
        let fallback = kosha_query::SearchPhaseStats {
            bm25_stats: kosha_query::Bm25StatsUsed::SegmentFallback,
            ..Default::default()
        };
        assert!(!search_fully_cached(&local, &fallback, 0));
        let fetched = HydrationOutcome {
            files_fetched: 1,
            bytes_fetched: 10,
            ..Default::default()
        };
        assert!(!search_fully_cached(&fetched, &phases, 0));
    }

    fn searched_state(enabled: bool) -> AppState {
        let state = dashboard_state(enabled);
        v1(
            &state,
            "acme",
            "POST /v1/namespaces/docs/documents",
            one_doc("d1", "breach of contract"),
        );
        v1(
            &state,
            "acme",
            "POST /v1/namespaces/docs/flush",
            serde_json::json!({}),
        );
        state
    }

    fn search_with_timings(state: &AppState) -> serde_json::Value {
        let resp = v1(
            state,
            "acme",
            "POST /v1/namespaces/docs/search",
            serde_json::json!({"query_text": "contract", "timings": true}),
        );
        assert_eq!(status_of(&resp), 200, "{resp}");
        body_of(&resp)
    }

    #[test]
    fn recent_queries_and_timings_appear_only_when_the_dashboard_is_on() {
        let state = searched_state(true);
        let first = search_with_timings(&state);
        assert_eq!(first["total_hits"], 1);
        let timings = &first["timings"];
        assert_eq!(timings["cached"], false, "{first}");
        for phase in ["total_ms", "hydrate_ms", "score_ms", "admit_ms", "queue_ms"] {
            assert!(timings[phase].as_f64().unwrap() >= 0.0, "{phase}: {first}");
        }
        // No S3 here: every byte the search read was local.
        assert_eq!(timings["s3_rounds"], 0, "{first}");
        assert_eq!(timings["s3_bytes"], 0, "{first}");
        assert_eq!(timings["fully_cached"], true, "{first}");
        // kNN probe depth (#384) is reported only for kNN searches.
        assert!(timings.get("knn_postings").is_none(), "{first}");
        assert!(timings.get("knn_cutoff_rule").is_none(), "{first}");
        // The identical repeat is a result-cache hit: still recorded, and
        // fully cached (it read nothing at all).
        let second = search_with_timings(&state);
        assert_eq!(second["timings"]["cached"], true, "{second}");
        assert_eq!(second["timings"]["fully_cached"], true, "{second}");
        assert_eq!(second["timings"]["s3_rounds"], 0, "{second}");
        assert_eq!(second["timings"]["s3_bytes"], 0, "{second}");
        assert_eq!(second["results"], first["results"]);
        // Without the opt-in the response is the plain one.
        let plain = v1(
            &state,
            "acme",
            "POST /v1/namespaces/docs/search",
            serde_json::json!({"query_text": "contract"}),
        );
        assert!(body_of(&plain).get("timings").is_none(), "{plain}");

        let stats = body_of(&handle_namespace_stats("docs", "acme", &state));
        let recent = &stats["recent_queries"];
        assert_eq!(recent["count"], 3, "{stats}");
        assert_eq!(recent["capacity"], dashboard::RING_CAPACITY);
        assert_eq!(recent["degraded"], 0);
        let samples = recent["samples"].as_array().unwrap();
        assert_eq!(samples[0]["hits"], 1);
        assert_eq!(samples[0]["cached"], false);
        assert_eq!(samples[1]["cached"], true);
        // Another tenant's namespace of the same name has its own ring.
        v1(
            &state,
            "globex",
            "POST /v1/namespaces",
            serde_json::json!({"namespace": "docs"}),
        );
        let other = body_of(&handle_namespace_stats("docs", "globex", &state));
        assert_eq!(other["recent_queries"]["count"], 0, "{other}");

        let state = searched_state(false);
        let result = search_with_timings(&state);
        assert_eq!(result["total_hits"], 1);
        assert!(result.get("timings").is_none(), "{result}");
        let stats = body_of(&handle_namespace_stats("docs", "acme", &state));
        assert!(stats.get("recent_queries").is_none(), "{stats}");
    }
    // ─── Per-tenant cache isolation (#342) ────────────────────────────────

    /// Two tenants with the same namespace name and the same query get
    /// separate result-cache entries: neither can be served the other's
    /// cached response, on the first search or on a repeat.
    #[test]
    fn result_cache_never_serves_one_tenant_another_tenants_entry() {
        let state = test_state();
        for (tenant, doc, text) in [
            ("acme", "acme-doc", "breach of contract by acme"),
            ("globex", "globex-doc", "breach of contract by globex"),
        ] {
            let indexed = v1(
                &state,
                tenant,
                "POST /v1/namespaces/docs/documents",
                one_doc(doc, text),
            );
            assert_eq!(status_of(&indexed), 200, "{indexed}");
            let flushed = v1(
                &state,
                tenant,
                "POST /v1/namespaces/docs/flush",
                serde_json::json!({}),
            );
            assert_eq!(status_of(&flushed), 200, "{flushed}");
        }
        let search = |tenant: &str| -> Vec<String> {
            let resp = v1(
                &state,
                tenant,
                "POST /v1/namespaces/docs/search",
                serde_json::json!({"query_text": "breach contract"}),
            );
            assert_eq!(status_of(&resp), 200, "{resp}");
            body_of(&resp)["results"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| h["doc_id"].as_str().unwrap().to_string())
                .collect()
        };
        for _ in 0..2 {
            assert_eq!(search("acme"), ["acme-doc"]);
            assert_eq!(search("globex"), ["globex-doc"]);
        }
        let cache = state.result_cache.lock().unwrap();
        let mut namespaces: Vec<&str> = cache.entries.keys().map(|k| k.0.as_str()).collect();
        namespaces.sort_unstable();
        assert_eq!(namespaces, ["acme/docs", "globex/docs"]);
        assert_eq!(
            (cache.hits, cache.misses),
            (2, 2),
            "each tenant hit its own"
        );
        let stats = cache.tenants.stats();
        assert_eq!(stats.len(), 2);
        assert!(stats
            .iter()
            .all(|s| s.hits == 1 && s.misses == 1 && s.bytes > 0));
    }

    /// One tenant filling the result cache evicts its own entries, not a
    /// quieter tenant's, while that tenant is under its share.
    #[test]
    fn result_cache_evicts_from_the_tenant_over_its_share() {
        let mut c = ResultCache::new(10_000);
        c.shares = kosha_core::tenancy::TenantShares::new(1_000);
        for i in 0..5 {
            c.insert(rc_key("beta/docs", 1, &format!("q{i}")), &"x".repeat(200));
        }
        for i in 0..200 {
            c.insert(rc_key("acme/big", 1, &format!("q{i}")), &"x".repeat(200));
        }
        assert!(c.bytes <= 10_000);
        for i in 0..5 {
            assert!(
                c.lookup(&rc_key("beta/docs", 1, &format!("q{i}")))
                    .is_some(),
                "beta's entry q{i} survived acme's flood"
            );
        }
        let stats = c.tenants.stats();
        let acme = stats.iter().find(|s| s.tenant == "acme").unwrap();
        let beta = stats.iter().find(|s| s.tenant == "beta").unwrap();
        assert!(acme.evictions > 0);
        assert_eq!(beta.evictions, 0);
        assert_eq!(acme.bytes + beta.bytes, c.bytes as u64);
    }

    // ─── Per-tenant quotas and rate limits (#341) ────────────────────────

    fn limited_state(json: &str) -> AppState {
        let mut state = test_state();
        let config = tenant_limits::LimitsConfig::parse(json, None).unwrap();
        state.tenant_limits = tenant_limits::TenantLimiter::new(Arc::new(
            tenant_limits::ConfigLimitsProvider::from_config(config),
        ));
        state
    }

    /// One authenticated request as `tenant`, through the tenant limits.
    fn as_tenant(state: &AppState, tenant: &str, line: &str, body: serde_json::Value) -> String {
        let bytes = if body.is_null() {
            Vec::new()
        } else {
            serde_json::to_vec(&body).unwrap()
        };
        serve_tenant_request(
            &format!("{line} HTTP/1.1"),
            &HashMap::new(),
            &bytes,
            tenant,
            state,
        )
    }

    fn header_of(response: &str, name: &str) -> Option<String> {
        let (head, _) = response.split_once("\r\n\r\n")?;
        head.split("\r\n").skip(1).find_map(|line| {
            let (k, v) = line.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case(name)
                .then(|| v.trim().to_string())
        })
    }

    fn assert_rejected(response: &str, reason: &str) -> serde_json::Value {
        assert_eq!(status_of(response), 429, "{response}");
        let retry: u64 = header_of(response, "retry-after")
            .expect("Retry-After header")
            .parse()
            .unwrap();
        assert!(retry >= 1, "{response}");
        let body = body_of(response);
        assert_eq!(body["reason"], reason, "{response}");
        assert!(body["error"].is_string());
        assert!(body["limit"].is_number());
        assert!(body["retry_after_ms"].as_u64().unwrap() >= 1);
        body
    }

    fn rejections(state: &AppState) -> Vec<metrics::tests::Sample> {
        let text = state.metrics.render(&metrics::Scrape::default());
        metrics::tests::parse_exposition(&text)
            .unwrap()
            .into_iter()
            .filter(|s| s.name == "kosha_tenant_rejections_total")
            .collect()
    }

    fn search_docs(state: &AppState, tenant: &str) -> String {
        as_tenant(
            state,
            tenant,
            "POST /v1/namespaces/docs/search",
            serde_json::json!({"query_text": "hello"}),
        )
    }

    #[test]
    fn tenant_over_its_qps_gets_429_while_another_tenant_is_served() {
        let state = limited_state(
            r#"{"default_plan": "free", "plans": {"free": {"qps": 2}},
                "tenants": {"big": {"qps": 1000}}}"#,
        );
        for tenant in ["small", "big"] {
            let created = as_tenant(
                &state,
                tenant,
                "POST /v1/namespaces",
                serde_json::json!({"namespace": "docs"}),
            );
            assert_eq!(status_of(&created), 200, "{created}");
        }
        assert_eq!(status_of(&search_docs(&state, "small")), 200);
        assert_eq!(status_of(&search_docs(&state, "small")), 200);
        let refused = search_docs(&state, "small");
        let body = assert_rejected(&refused, "tenant_qps_exceeded");
        assert_eq!(body["limit"], 2);
        assert!(body["retry_after_ms"].as_u64().unwrap() <= 500);
        // The legacy route form is a search too.
        let legacy = as_tenant(
            &state,
            "small",
            "POST /search",
            serde_json::json!({"namespace": "docs", "query_text": "x"}),
        );
        assert_rejected(&legacy, "tenant_qps_exceeded");
        // Another tenant is served meanwhile.
        for _ in 0..10 {
            let ok = search_docs(&state, "big");
            assert_eq!(status_of(&ok), 200, "{ok}");
        }
        // Non-search reads are not charged to the qps bucket.
        let stats = as_tenant(
            &state,
            "small",
            "GET /v1/namespaces/docs/stats",
            serde_json::Value::Null,
        );
        assert_eq!(status_of(&stats), 200, "{stats}");
        let counted = metrics::tests::value(
            &rejections(&state),
            "kosha_tenant_rejections_total",
            &[("reason", "tenant_qps_exceeded")],
        );
        assert_eq!(counted, Some(2.0));
    }

    /// Through `dispatch`, as the HTTP server calls it: the 429 carries the
    /// `Retry-After` header and the reason.
    #[test]
    fn tenant_qps_rejection_through_dispatch() {
        // Unit tests run with auth disabled: every request is tenant `dev`.
        let state = limited_state(r#"{"tenants": {"dev": {"qps": 1}}}"#);
        let create = dispatch(
            &state,
            test_request("POST /v1/namespaces HTTP/1.1\r\n\r\n{\"namespace\":\"docs\"}"),
        );
        assert_eq!(create.status(), 200);
        let search = || {
            dispatch(
                &state,
                test_request(
                    "POST /v1/namespaces/docs/search HTTP/1.1\r\n\r\n{\"query_text\":\"x\"}",
                ),
            )
        };
        assert_eq!(search().status(), 200);
        let mut refused = search();
        assert_eq!(refused.status(), 429);
        assert_eq!(refused.header("retry-after"), Some("1"));
        let body: serde_json::Value = serde_json::from_slice(&refused.body_bytes()).unwrap();
        assert_eq!(body["reason"], "tenant_qps_exceeded");
        assert_eq!(body["limit"], 1);
    }

    #[test]
    fn namespace_doc_and_dims_quotas_are_rejected_on_ingest() {
        let state = limited_state(
            r#"{"default_plan": "p", "plans": {"p": {"namespaces": 1,
                "docs_per_namespace": 2, "vector_dims": 3, "max_body_bytes": 4096}}}"#,
        );
        let create = |name: &str| {
            as_tenant(
                &state,
                "acme",
                "POST /v1/namespaces",
                serde_json::json!({"namespace": name}),
            )
        };
        assert_eq!(status_of(&create("one")), 200);
        assert_rejected(&create("two"), "namespace_quota_exceeded");
        // A first write would auto-create a namespace: same quota.
        let auto = as_tenant(
            &state,
            "acme",
            "POST /v1/namespaces/three/documents",
            one_doc("d1", "hello"),
        );
        assert_rejected(&auto, "namespace_quota_exceeded");
        // Re-creating an existing namespace is still the usual 409.
        assert_eq!(status_of(&create("one")), 409);
        // Another tenant has its own quota.
        let other = as_tenant(
            &state,
            "globex",
            "POST /v1/namespaces",
            serde_json::json!({"namespace": "two"}),
        );
        assert_eq!(status_of(&other), 200, "{other}");

        let index = |body: serde_json::Value| {
            as_tenant(&state, "acme", "POST /v1/namespaces/one/documents", body)
        };
        let two_docs = serde_json::json!({"documents": [
            {"id": "a", "fields": [{"name": "title", "field_type": "Text", "value": "x"}]},
            {"id": "b", "fields": [{"name": "title", "field_type": "Text", "value": "y"}]}
        ]});
        assert_eq!(status_of(&index(two_docs)), 200);
        let body = assert_rejected(&index(one_doc("c", "z")), "doc_quota_exceeded");
        assert_eq!(body["limit"], 2);
        // Still over after a flush: committed documents count.
        let flush = as_tenant(
            &state,
            "acme",
            "POST /v1/namespaces/one/flush",
            serde_json::json!({}),
        );
        assert_eq!(status_of(&flush), 200, "{flush}");
        assert_rejected(&index(one_doc("c", "z")), "doc_quota_exceeded");

        let vector = |id: &str, dims: usize| {
            let v: Vec<f32> = (0..dims).map(|i| i as f32).collect();
            serde_json::json!({"documents": [{"id": id, "fields": [
                {"name": "emb", "field_type": "Vector", "value": serde_json::to_string(&v).unwrap()}
            ]}]})
        };
        let globex_write = |body: serde_json::Value| {
            as_tenant(&state, "globex", "POST /v1/namespaces/two/documents", body)
        };
        let body = assert_rejected(&globex_write(vector("v1", 4)), "vector_dims_exceeded");
        assert_eq!(body["limit"], 3);
        let fits = globex_write(vector("v2", 3));
        assert_eq!(status_of(&fits), 200, "{fits}");

        let big = globex_write(one_doc("big", &"x".repeat(5000)));
        let body = assert_rejected(&big, "body_too_large");
        assert_eq!(body["limit"], 4096);
    }

    #[test]
    fn tenant_write_rate_is_limited_per_tenant() {
        let state =
            limited_state(r#"{"default_plan": "p", "plans": {"p": {"write_docs_per_sec": 2}}}"#);
        let index = |tenant: &str, id: &str| {
            as_tenant(
                &state,
                tenant,
                "POST /v1/namespaces/docs/documents",
                one_doc(id, "hello"),
            )
        };
        assert_eq!(status_of(&index("a", "1")), 200);
        assert_eq!(status_of(&index("a", "2")), 200);
        assert_rejected(&index("a", "3"), "tenant_write_rate_exceeded");
        assert_eq!(status_of(&index("b", "1")), 200);
    }

    #[test]
    fn operator_traffic_is_never_tenant_limited() {
        let state =
            limited_state(r#"{"default_plan": "p", "plans": {"p": {"qps": 1, "namespaces": 0}}}"#);
        assert!(!tenant_is_limited(&state, SERVICE_TENANT));
        assert!(tenant_is_limited(&state, "acme"));
        assert!(write_limits(&state, &NamespaceId("service/docs".into())).is_none());
        assert!(write_limits(&state, &NamespaceId("bare".into())).is_none());
        let created = as_tenant(
            &state,
            SERVICE_TENANT,
            "POST /v1/namespaces",
            serde_json::json!({"namespace": "docs"}),
        );
        assert_eq!(status_of(&created), 200, "{created}");
    }

    #[test]
    fn without_limits_config_nothing_is_limited() {
        let state = test_state();
        assert!(!state.tenant_limits.enabled());
        for i in 0..30 {
            let created = as_tenant(
                &state,
                "acme",
                "POST /v1/namespaces",
                serde_json::json!({"namespace": format!("ns{i}")}),
            );
            assert_eq!(status_of(&created), 200, "{created}");
        }
        for i in 0..50 {
            let indexed = as_tenant(
                &state,
                "acme",
                "POST /v1/namespaces/ns0/documents",
                one_doc(&format!("d{i}"), "hello"),
            );
            assert_eq!(status_of(&indexed), 200, "{indexed}");
        }
        for _ in 0..100 {
            let found = as_tenant(
                &state,
                "acme",
                "POST /v1/namespaces/ns0/search",
                serde_json::json!({"query_text": "hello"}),
            );
            assert_eq!(status_of(&found), 200, "{found}");
        }
        assert!(rejections(&state).iter().all(|s| s.value == 0.0));
    }

    #[test]
    fn search_route_classification() {
        assert!(is_search_route("POST /v1/namespaces/docs/search HTTP/1.1"));
        assert!(is_search_route("POST /search HTTP/1.1"));
        assert!(is_search_route("GET /search?ns=docs&q=x HTTP/1.1"));
        assert!(!is_search_route(
            "POST /v1/namespaces/docs/documents HTTP/1.1"
        ));
        assert!(!is_search_route("GET /v1/namespaces/docs/stats HTTP/1.1"));
        assert!(!is_search_route("POST /v1/namespaces/a/b/search HTTP/1.1"));
    }
}

/// TLS between pods (#343), end to end over real sockets with throwaway
/// certificates: peer segment fetches and query→ingest forwarding.
#[cfg(test)]
mod internal_tls_tests {
    use super::*;
    use crate::internal_tls::{test_pki, InternalTls};
    use crate::tests::SharedManifestStore;

    /// Serve `state` over HTTPS with the PKI's server certificate; with
    /// `mtls`, `/internal/*` also needs a client certificate from its CA.
    fn serve_tls(state: Arc<AppState>, pki: &test_pki::Pki, mtls: bool) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut config = http::Config::new(Duration::from_secs(30), auth::max_body_bytes());
        config.tls = Some((pki.server_cert.clone(), pki.server_key.clone()));
        if mtls {
            config.client_ca = Some(pki.ca.clone());
        }
        std::thread::spawn(move || {
            serve_until(listener, state, config, std::future::pending())
                .expect("HTTPS server failed")
        });
        addr
    }

    /// The internal TLS settings a pod would read from its environment.
    fn internal_tls(
        pki: &test_pki::Pki,
        client_cert: bool,
        server_name: Option<&str>,
    ) -> InternalTls {
        let path = |p: &std::path::Path| Some(p.to_str().unwrap().to_string());
        InternalTls::from_lookup(|name| match name {
            "KOSHA_INTERNAL_TLS" => Some("on".into()),
            "KOSHA_INTERNAL_TLS_CA_PATH" => path(&pki.ca),
            "KOSHA_INTERNAL_TLS_CLIENT_CERT_PATH" if client_cert => path(&pki.client_cert),
            "KOSHA_INTERNAL_TLS_CLIENT_KEY_PATH" if client_cert => path(&pki.client_key),
            "KOSHA_INTERNAL_TLS_SERVER_NAME" => server_name.map(str::to_string),
            _ => None,
        })
        .unwrap()
    }

    #[cfg(all(feature = "s3", feature = "postgres"))]
    #[test]
    fn peer_segment_fetch_runs_over_mutual_tls() {
        let pki = test_pki::generate("peer-fetch");
        let mut peer = AppState::new(tests::test_dir());
        peer.internal_service_key = Some("peer-test-secret".into());
        peer.cache
            .put_bytes("ns/seg/doc_store.bin", b"peer bytes over tls")
            .unwrap();
        let addr = serve_tls(Arc::new(peer), &pki, true).to_string();

        let requester = |tls: &InternalTls| {
            let mut state = AppState::new(tests::test_dir());
            state.internal_auth_key = Some("peer-test-secret".into());
            state.peer_url_scheme = tls.peer_scheme();
            state.peer_http_client = Some(
                tls.client_builder()
                    .timeout(Duration::from_secs(5))
                    .build()
                    .unwrap(),
            );
            state
        };

        // Peers are dialed by pod IP; the certificate is checked against
        // the pinned KOSHA_INTERNAL_TLS_SERVER_NAME instead.
        let tls = internal_tls(&pki, true, Some("kosha-internal"));
        assert_eq!(tls.peer_scheme(), "https");
        let state = requester(&tls);
        let written = state
            .fetch_from_peer(&addr, "ns/seg/doc_store.bin")
            .expect("peer fetch over TLS should succeed");
        assert_eq!(written, 19);
        let local = state.cache.path_for("ns/seg/doc_store.bin").unwrap();
        assert_eq!(std::fs::read(local).unwrap(), b"peer bytes over tls");

        // No client certificate: the peer's mTLS check answers 403.
        let state = requester(&internal_tls(&pki, false, Some("kosha-internal")));
        let err = state
            .fetch_from_peer(&addr, "ns/seg/doc_store.bin")
            .unwrap_err();
        assert!(err.to_string().contains("403"), "{err}");

        // A pinned name the certificate does not carry fails verification.
        let state = requester(&internal_tls(&pki, true, Some("someone-else")));
        let err = state
            .fetch_from_peer(&addr, "ns/seg/doc_store.bin")
            .unwrap_err();
        assert!(err.to_string().contains("unreachable"), "{err}");

        // Plain HTTP against the TLS peer gets nothing usable.
        let state = requester(&InternalTls::off());
        assert_eq!(state.peer_url_scheme, "http");
        assert!(state
            .fetch_from_peer(&addr, "ns/seg/doc_store.bin")
            .is_err());
    }

    #[test]
    fn query_forwards_writes_to_ingest_over_tls() {
        let pki = test_pki::generate("forward");
        let shared = Arc::new(Mutex::new(kosha_control::Controller::new()));
        let ingest = AppState::new(tests::test_dir());
        *ingest.controller.lock().unwrap() = Box::new(SharedManifestStore::new(shared.clone()));
        let ingest = Arc::new(ingest);
        let ingest_addr = serve_tls(Arc::clone(&ingest), &pki, true);

        let tls = internal_tls(&pki, true, None);
        let ingest_host = format!("https://localhost:{}", ingest_addr.port());
        assert_eq!(tls.check_ingest_host(&ingest_host), Ok(None));
        let mut query = AppState::new(ingest.data_dir.clone());
        *query.controller.lock().unwrap() = Box::new(SharedManifestStore::new(shared.clone()));
        query.ingest_host = Some(ingest_host);
        query.write_http_client = Some(
            tls.client_builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap(),
        );

        let send = |method: &str, target: &str, body: serde_json::Value| {
            let request = http::Request {
                method: method.into(),
                target: target.into(),
                headers: HashMap::from([("content-type".into(), "application/json".into())]),
                body: body.to_string().into_bytes(),
            };
            dispatch(&query, request)
        };

        let response = send(
            "POST",
            "/v1/namespaces",
            serde_json::json!({"namespace": "docs"}),
        );
        assert_eq!(response.status(), 200, "{response:?}");
        let ns = NamespaceId("dev/docs".into());
        assert!(
            ingest.indexer.manifest_version(&ns).is_some(),
            "the create ran on the ingest node, over TLS"
        );
        let response = send(
            "POST",
            "/v1/namespaces/docs/documents",
            serde_json::json!({"documents": [
                {"id": "d1", "fields": [{"name": "title", "field_type": "Text", "value": "quick fox"}]}
            ]}),
        );
        assert_eq!(response.status(), 200, "{response:?}");
        assert_eq!(ingest.indexer.pending_doc_count(&ns), 1);
    }
}
