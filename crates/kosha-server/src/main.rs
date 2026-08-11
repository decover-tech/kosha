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

/// Map of valid API keys → tenant id.
///
/// Loaded from, in priority order:
///   1. Postgres `kosha.api_keys` table (if DATABASE_URL + postgres feature)
///   2. `KOSHA_API_KEYS` env var (format: `key1=tenant1,key2=tenant2`)
///   3. `KOSHA_API_KEY` env var (single key, tenant = "default")
///   4. Empty (dev mode — no auth required)
static API_KEYS: once_cell::sync::Lazy<HashMap<String, String>> =
    once_cell::sync::Lazy::new(load_api_keys);

fn load_api_keys() -> HashMap<String, String> {
    // 1. Postgres-backed keys (staging/production).
    if let Ok(db_url) = std::env::var("DATABASE_URL") {
        #[cfg(feature = "postgres")]
        match kosha_control::PgStore::new(&db_url) {
            Ok(store) => {
                let keys = store.list_api_keys(None).unwrap_or_default();
                if !keys.is_empty() {
                    let map: HashMap<String, String> = keys
                        .into_iter()
                        .map(|(key, tenant, _desc)| (key, tenant))
                        .collect();
                    println!("api keys: loaded {} key(s) from postgres", map.len());
                    return map;
                }
                println!("api keys: no keys found in postgres, falling back to env vars");
            }
            Err(e) => {
                eprintln!("api keys: failed to connect to postgres: {e}, falling back to env vars");
            }
        }
        #[cfg(not(feature = "postgres"))]
        {
            let _ = db_url;
            println!("api keys: DATABASE_URL set but postgres feature disabled, using env vars");
        }
    }

    // 2. Multi-tenant env var.
    if let Ok(keys) = std::env::var("KOSHA_API_KEYS") {
        let map: HashMap<_, _> = keys
            .split(',')
            .filter_map(|pair| {
                pair.split_once('=')
                    .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
            })
            .collect();
        if !map.is_empty() {
            println!(
                "api keys: loaded {} key(s) from KOSHA_API_KEYS env var",
                map.len()
            );
            return map;
        }
    }

    // 3. Single-tenant env var.
    if let Ok(key) = std::env::var("KOSHA_API_KEY") {
        println!("api keys: single key from KOSHA_API_KEY env var");
        let mut m = HashMap::new();
        m.insert(key, "default".to_string());
        return m;
    }

    // 4. Dev mode — no auth.
    println!("api keys: none configured — dev mode (no auth)");
    HashMap::new()
}

/// Extract the tenant prefix from a namespace for isolation.
fn tenant_namespace(tenant: &str, namespace: &str) -> String {
    format!("{tenant}/{namespace}")
}

#[cfg(feature = "migrate")]
mod migrate;
#[cfg(feature = "s3")]
mod s3_storage;

use kosha_cache::Cache;
#[cfg(feature = "s3")]
use kosha_core::StorageBackend;
#[cfg(feature = "s3")]
use kosha_core::{segment_may_contain_terms, segment_may_match, TermBloomMode};
use kosha_core::{ControlStore, IndexRequest, IndexResponse, KoshaError, NamespaceId, SearchQuery};
use kosha_query::DocStoreHydrator;
use kosha_query::Searcher;
#[cfg(feature = "s3")]
use kosha_segment::tokenize;
use kosha_segment::SegmentReader;
use kosha_write::Indexer;

// ─── Application state ──────────────────────────────────────────────────────

struct AppState {
    controller: Mutex<Box<dyn ControlStore>>,
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
    /// Byte ceiling on how much a single hydration batch will queue for
    /// concurrent S3 download at once (`KOSHA_HYDRATE_BYTE_BUDGET`, default
    /// 1GiB). Unlike `hydrate_concurrency` (file count), this bounds actual
    /// memory — see `hydrate_from_s3_budgeted`/`chunk_by_byte_budget`.
    #[cfg(feature = "s3")]
    hydrate_byte_budget: u64,
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
    /// Bearer credential used for pod-to-pod hydration requests — the same
    /// `KOSHA_API_KEY` bootstrap secret every query pod already carries, so
    /// no separate internal secret is needed. `None` disables peer fetches
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
    /// guessing. After hydration confirms every scoring-set file is local
    /// (on a cold namespace) the verdict is set to `true` for that
    /// `(namespace, version)` pair, and subsequent warm requests short-
    /// circuit `hydrate_segments_for_search` entirely. Failure safety:
    /// if a disk-cache eviction removes a segment's scoring file after
    /// the verdict is cached, the search itself returns an IO error
    /// (`open_segment` fails to open `inverted.idx`), the caller's
    /// existing 503 + client retry path kicks in, and the verdict is
    /// invalidated on the error path here so the next request re-checks
    /// and re-hydrates the missing file. The collection grows
    /// unboundedly in principle (one entry per manifest bump per
    /// namespace) but each is a `(u64, u64)` tuple — 16 bytes — and
    /// manifest versions roll slowly enough that even a long-lived pod
    /// holds at most low-thousands entries; per-query cost is a single
    /// `HashMap` lookup, vs. 17 syscalls on the legacy path.
    completed_hydrations: Mutex<HashMap<(String, u64), bool>>,
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
struct Semaphore {
    permits: Mutex<usize>,
    cv: Condvar,
}

impl Semaphore {
    fn new(permits: usize) -> Self {
        Self {
            permits: Mutex::new(permits),
            cv: Condvar::new(),
        }
    }

    /// Block until a permit is available, then hold it until the returned
    /// guard is dropped.
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
        let mut permits = self.sem.permits.lock().unwrap();
        *permits += 1;
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

/// Upload every file in `seg_dir` to S3. Returns `true` only if every file
/// uploaded successfully — a partial upload must not be treated as durable.
///
/// Free function (not an `AppState` method) so both `AppState::sync_to_s3`
/// and the boot-time reconciliation sweep in `AppState::new` — which runs
/// before `self` exists — can share it.
#[cfg(feature = "s3")]
fn sync_segment_dir_to_s3(s3: &s3_storage::S3Storage, data_dir: &Path, seg_dir: &Path) -> bool {
    if !seg_dir.exists() {
        return false;
    }
    let Ok(entries) = std::fs::read_dir(seg_dir) else {
        return false;
    };
    let mut all_ok = true;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let rel = seg_dir
            .strip_prefix(data_dir)
            .unwrap_or(seg_dir)
            .to_string_lossy();
        let s3_path = format!("{rel}/{name}");
        match std::fs::read(&path) {
            Ok(data) => {
                if let Err(e) = s3.write(&s3_path, &data) {
                    eprintln!("WARN: S3 upload failed for {s3_path}: {e}");
                    all_ok = false;
                }
            }
            Err(e) => {
                eprintln!("WARN: failed to read {s3_path} for upload: {e}");
                all_ok = false;
            }
        }
    }
    all_ok
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
        let indexer = Indexer::new(data_dir.clone()).with_flush_threshold(flush_threshold);

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
            match kosha_control::PgStore::new(&db_url) {
                Ok(store) => {
                    control_plane_kind = "postgres";
                    println!("control plane: postgres ({})", redact_database_url(&db_url));
                    Box::new(store)
                }
                Err(e) => {
                    eprintln!(
                        "WARN: failed to connect to postgres, falling back to in-memory: {e}"
                    );
                    Box::new(kosha_control::Controller::new())
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
            if let Some(manifest) = control_store.manifest_cloned(&ns) {
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
                                if s3.segment_durable_in_s3(&rel) {
                                    synced_segments.insert((ns.clone(), entry.segment_id.clone()));
                                    continue;
                                }
                                let seg_path = data_dir.join(&ns.0).join(&entry.segment_id.0);
                                if sync_segment_dir_to_s3(s3, &data_dir, &seg_path) {
                                    synced_segments.insert((ns.clone(), entry.segment_id.clone()));
                                    reconciled_missing += 1;
                                    println!(
                                        "reconciliation: uploaded segment missing from S3: {rel}"
                                    );
                                } else {
                                    eprintln!(
                                        "WARN: reconciliation found segment {rel} durable in \
                                         neither S3 nor local disk — data loss, cannot recover"
                                    );
                                }
                            }
                        }
                    }

                    indexer.restore_manifest(ns, manifest);
                    restored += 1;
                }
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
                    std::env::var("KOSHA_INGEST_PROXY_TIMEOUT_SECS")
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .filter(|n| *n > 0)
                        .unwrap_or(30),
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
        let internal_auth_key = std::env::var("KOSHA_API_KEY")
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
        if !warmup_namespaces.is_empty() {
            println!(
                "warmup: configured for {} namespace(s): {} (posting blobs: {})",
                warmup_namespaces.len(),
                warmup_namespaces.join(", "),
                if warmup_posting_blobs { "on" } else { "off" },
            );
        }

        Self {
            controller: Mutex::new(control_store),
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
            // 1GiB default: with the default KOSHA_MAX_CONCURRENT_HYDRATIONS
            // (4), worst case is 4 owner batches × 1GiB = 4GiB of hydration
            // buffers at once — comfortable inside the query pod's 16Gi
            // limit (deployment-query-resources-patch.yaml) alongside
            // KOSHA_SEGMENT_CACHE_MAX_BYTES and process baseline.
            #[cfg(feature = "s3")]
            hydrate_byte_budget: std::env::var("KOSHA_HYDRATE_BYTE_BUDGET")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|n| *n > 0)
                .unwrap_or(1024 * 1024 * 1024),
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
            search_semaphore: Semaphore::new(
                std::env::var("KOSHA_MAX_CONCURRENT_SEARCHES")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .filter(|n| *n > 0)
                    .unwrap_or(8),
            ),
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
            completed_hydrations: Mutex::new(HashMap::new()),
            result_cache: Mutex::new(ResultCache::new(result_cache_budget())),
            posting_blob_presence: Mutex::new(HashMap::new()),
            // Warmup starts ready; `spawn_warmup` flips `false` only when
            // there's configured work to do (and back to `true` on
            // completion / fail-open). See the field's doc comment.
            warmup_complete: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            warmup_namespaces,
            warmup_posting_blobs,
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
                bm25_params: kosha_core::Bm25Params { k1: 1.2, b: 0.75 },
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
            };
            let t_ns = std::time::Instant::now();
            let outcome = self.hydrate_segments_for_search(&ns, &manifest, &query);
            let _ = tombstones;
            let dur = t_ns.elapsed();
            if outcome.missing.is_empty() {
                // Mirror the search path's verdict caching (handle_search_post):
                // the static scoring set is now local for this manifest
                // version, so the first real queries skip the per-segment
                // stat sweep instead of each re-deriving this verdict.
                self.completed_hydrations
                    .lock()
                    .unwrap()
                    .insert((ns.0.clone(), manifest.version), true);
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
                    let o =
                        self.ensure_posting_blobs_local(&ns, manifest.version, chunk, &all_blobs);
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
                // Chunks of 2, not 8: the S3 fetch buffers each file's whole
                // body in memory, and post-compaction doc stores are the
                // largest files in a segment — two multi-GB bodies in flight
                // is plenty.
                let t_ds = std::time::Instant::now();
                let mut ds_files = 0usize;
                let mut ds_bytes = 0u64;
                for chunk in seg_paths.chunks(2) {
                    let (files, bytes) = self.ensure_files_local(chunk, "doc_store.bin");
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
                // vector form. Probe one segment's tiny `vector.offsets`
                // sidecar first: segments are format-homogeneous within a
                // namespace in practice, and a text-only namespace skips
                // the whole phase without issuing per-segment fetches for
                // files that don't exist.
                let probe = &seg_paths[..1.min(seg_paths.len())];
                let _ = self.ensure_files_local(probe, "vector.offsets");
                let namespace_has_vectors = probe
                    .first()
                    .map(|p| p.join("vector.offsets").is_file())
                    .unwrap_or(false);
                if namespace_has_vectors {
                    let t_vec = std::time::Instant::now();
                    let mut vec_files = 0usize;
                    let mut vec_bytes = 0u64;
                    for chunk in seg_paths.chunks(8) {
                        let (files, bytes) = self.ensure_files_local(chunk, "vector.offsets");
                        vec_files += files;
                        vec_bytes += bytes;
                    }
                    // Same chunks-of-2 as doc stores: vector.idx bodies are
                    // buffered whole in memory during fetch.
                    for chunk in seg_paths.chunks(2) {
                        let (files, bytes) = self.ensure_files_local(chunk, "vector.idx");
                        vec_files += files;
                        vec_bytes += bytes;
                    }
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
    fn persist_manifest(&self, ns: &NamespaceId) {
        let manifest = self.indexer.manifest_cloned(ns);
        if let Some(manifest) = manifest {
            let mut ctrl = self.controller.lock().unwrap();
            match ctrl.save_manifest(ns, &manifest) {
                Ok(()) => {
                    println!(
                        "control plane: saved manifest ns={} version={} segments={} backend={}",
                        ns.0,
                        manifest.version,
                        manifest.segments.len(),
                        self.control_plane_kind
                    );
                }
                Err(e) => {
                    eprintln!("WARN: failed to persist manifest for '{}': {e}", ns.0);
                }
            }
        }
    }

    /// After a flush: persist the manifest and upload any not-yet-synced
    /// segment(s) to S3.
    fn publish_namespace(&self, ns: &NamespaceId) {
        self.persist_manifest(ns);
        #[cfg(feature = "s3")]
        self.sync_unsynced_segments_to_s3(ns);
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
    fn sync_unsynced_segments_to_s3(&self, ns: &NamespaceId) {
        let Some(manifest) = self.indexer.manifest_cloned(ns) else {
            return;
        };
        let pending: Vec<kosha_core::SegmentId> = {
            let synced = self.synced_segments.lock().unwrap();
            segments_needing_sync(&manifest, &synced, ns)
                .into_iter()
                .map(|e| e.segment_id.clone())
                .collect()
        };
        for segment_id in pending {
            let seg_path = self.data_dir.join(&ns.0).join(&segment_id.0);
            // Only mark synced on full success — a partial failure (some
            // files uploaded, some didn't) must be retried by a future
            // publish call, never treated as durable.
            if self.sync_to_s3(&seg_path) {
                self.synced_segments
                    .lock()
                    .unwrap()
                    .insert((ns.clone(), segment_id));
            }
        }
    }

    /// Sync a segment directory to S3. Returns `true` only if every file in
    /// the directory uploaded successfully — callers use this to decide
    /// whether the segment is now durable (see `sync_unsynced_segments_to_s3`
    /// and the boot-time reconciliation sweep in `AppState::new`); a partial
    /// upload must not be recorded as done.
    #[cfg(feature = "s3")]
    fn sync_to_s3(&self, seg_dir: &Path) -> bool {
        let Some(ref s3) = self.s3_storage else {
            return false;
        };
        sync_segment_dir_to_s3(s3, &self.data_dir, seg_dir)
    }

    /// The files that must all be present for a segment to be usable by the
    /// searcher. `vector.idx` is intentionally excluded — Phase 1 lexical
    /// queries never load it (see `kosha_query`'s lazy vector loading), so
    /// its absence must not count as an incomplete segment.
    #[cfg(feature = "s3")]
    const REQUIRED_SEGMENT_FILES: [&'static str; 4] = [
        "footer.json",
        "doc_store.bin",
        "inverted.idx",
        "filters.bin",
    ];

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
                self.hydrate_files(s3, &logical_paths, self.hydrate_concurrency);
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
    /// S3, chunked so no single `read_many` call is ever asked to hold more
    /// than `hydrate_byte_budget` bytes' worth of files concurrently
    /// in-flight.
    ///
    /// `hydrate_concurrency` alone bounds *file count* in flight, not
    /// memory — a batch of `hydrate_concurrency` (default 16) large HNSW
    /// vector files is a very different footprint than 16 footer.jsons, and
    /// nothing before this compared a batch's actual byte size against the
    /// pod's memory before firing it off. Sizes come from `list_with_sizes`'
    /// S3-reported object sizes, projected ahead of any fetch — this is the
    /// one dimension none of the other hydration knobs
    /// (`KOSHA_HYDRATE_CONCURRENCY`, `KOSHA_MAX_CONCURRENT_HYDRATIONS`) or
    /// cache knobs (`KOSHA_CACHE_MAX_BYTES`, `KOSHA_SEGMENT_CACHE_MAX_BYTES`)
    /// bound.
    #[cfg(feature = "s3")]
    fn hydrate_from_s3_budgeted(
        &self,
        s3: &s3_storage::S3Storage,
        files: &[(String, u64)],
        concurrency: usize,
    ) {
        let chunks = chunk_by_byte_budget(files, self.hydrate_byte_budget);
        let total_chunks = chunks.len();
        for (chunk_idx, chunk) in chunks.into_iter().enumerate() {
            let paths: Vec<String> = chunk.into_iter().map(|(path, _size)| path).collect();
            let t_chunk = Instant::now();
            let results = s3.read_many(&paths, concurrency);
            // Aggregate per-file timing into the cold-read "next lever"
            // instrument (plan Tier 1 O13): rank-orders GET vs write vs
            // fan-out so the *next* change (raise concurrency vs skip bytes
            // vs reduce writes) is chosen from data, not guess. `cached`
            // files (already local) and failures are tracked separately.
            let mut fetched = 0u64;
            let mut bytes = 0u64;
            let mut get_sum = 0.0f64;
            let mut write_sum = 0.0f64;
            let mut get_max = 0.0f64;
            let mut cached = 0u64;
            let mut failed = 0u64;
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
                    Err(e) => {
                        failed += 1;
                        eprintln!("WARN: S3 download failed for {path}: {e}");
                    }
                }
            }
            let wall_ms = t_chunk.elapsed().as_secs_f64() * 1e3;
            println!(
                "hydrate_files timing: chunk={}/{} fetched={} cached={} failed={} \
                 bytes={:.1}MB wall_ms={:.1} get_ms={:.1}(max={:.1}) write_ms={:.1} concurrency={}",
                chunk_idx + 1,
                total_chunks,
                fetched,
                cached,
                failed,
                bytes as f64 / (1024.0 * 1024.0),
                wall_ms,
                get_sum,
                get_max,
                write_sum,
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
    fn ensure_files_local(&self, seg_paths: &[PathBuf], file_name: &str) -> (usize, u64) {
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
        self.hydrate_files(s3, &logical_paths, self.scoring_hydrate_concurrency);
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
                self.hydrate_files_coordinated(s3, files, leases, own_addr, concurrency);
                for (path, _) in files {
                    self.cache.unpin(path);
                }
                return;
            }
        }
        self.hydrate_from_s3_budgeted(s3, files, concurrency);
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
        self.hydrate_from_s3_budgeted(s3, &fetch_from_s3, concurrency);

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
    ) -> HydrationOutcome {
        let term_prune = term_bloom_prune_for_query(query);
        let needs_bloom_check = query.filter.is_some() || term_prune.is_some();
        let needs_vectors = query.knn.is_some();
        let needs_filters = query_needs_filter_store(query);
        let posting_files = posting_blob_files_for_query(query);
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

        // New manifests carry the search footer metadata inline, so cold
        // search can skip the footer-first S3 phase entirely. Legacy
        // manifests fall back to hydrating footer.json for pruning and
        // format detection.
        let (footer_files, footer_bytes) =
            self.ensure_files_local(&missing_manifest_footer_paths, "footer.json");

        let mut to_hydrate: Vec<PathBuf> = Vec::new();
        for (entry, seg_path) in manifest.segments.iter().zip(all_seg_paths) {
            if needs_bloom_check {
                let footer = manifest
                    .segment_footer(&entry.segment_id)
                    .cloned()
                    .or_else(|| SegmentReader::read_footer(&seg_path).ok());
                if let Some(footer) = footer.as_ref() {
                    if let Some(ref filter) = query.filter {
                        if !segment_may_match(filter, footer.filter_blooms.as_ref()) {
                            continue;
                        }
                    }
                    if let Some((ref terms, mode)) = term_prune {
                        if !segment_may_contain_terms(terms, mode, footer.term_bloom.as_ref()) {
                            continue;
                        }
                    }
                }
            }
            to_hydrate.push(seg_path);
        }
        let mut outcome = self.ensure_scoring_files_local(
            &to_hydrate,
            needs_vectors,
            needs_filters,
            &paths_with_manifest_footer,
        );
        let postings_outcome =
            self.ensure_posting_blobs_local(ns, manifest.version, &to_hydrate, &posting_files);
        outcome.files_fetched += postings_outcome.files_fetched;
        outcome.bytes_fetched += postings_outcome.bytes_fetched;
        outcome.missing.extend(postings_outcome.missing);
        outcome.files_fetched += footer_files;
        outcome.bytes_fetched += footer_bytes;
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
            // names are known a priori). 0 sizes mean the byte budget can't
            // chunk the batch, which is acceptable: the scoring set
            // excludes the bulk doc stores, so per-file sizes are small.
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
                self.hydrate_files(s3, &logical_paths, self.scoring_hydrate_concurrency);
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
    ) -> HydrationOutcome {
        let posting_files = posting_blob_files_for_query(query);
        if posting_files.is_empty() {
            return HydrationOutcome::default();
        }
        let seg_paths: Vec<PathBuf> = manifest
            .segments
            .iter()
            .map(|entry| self.data_dir.join(&ns.0).join(&entry.segment_id.0))
            .collect();
        self.ensure_posting_blobs_local(ns, manifest.version, &seg_paths, &posting_files)
    }

    #[cfg(feature = "s3")]
    fn ensure_posting_blobs_local(
        &self,
        ns: &NamespaceId,
        manifest_version: u64,
        seg_paths: &[PathBuf],
        posting_files: &[String],
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

        let (logical_paths, mut newly_confirmed) = plan_posting_blob_fetch(
            seg_paths,
            posting_files,
            &self.data_dir,
            &cached,
            kosha_segment::inverted_uses_posting_blobs,
            file_present_nonempty,
        );

        let mut outcome = HydrationOutcome::default();
        if !logical_paths.is_empty() {
            println!(
                "cache miss: hydrating {} posting blob(s) across {} segment(s) (fan-out={})",
                logical_paths.len(),
                seg_paths.len(),
                self.scoring_hydrate_concurrency
            );
            self.hydrate_files(s3, &logical_paths, self.scoring_hydrate_concurrency);
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

        outcome
    }

    /// Upload a single file from a local segment dir to S3.
    #[cfg(feature = "s3")]
    fn sync_file_to_s3(&self, seg_dir: &Path, file_name: &str) {
        let Some(ref s3) = self.s3_storage else {
            return;
        };
        let path = seg_dir.join(file_name);
        if !path.is_file() {
            return;
        }
        let Ok(rel) = seg_dir.strip_prefix(&self.data_dir) else {
            return;
        };
        let s3_path = format!("{}/{file_name}", rel.to_string_lossy());
        match std::fs::read(&path) {
            Ok(data) => {
                if let Err(e) = s3.write(&s3_path, &data) {
                    eprintln!("WARN: S3 upload failed for {s3_path}: {e}");
                }
            }
            Err(e) => eprintln!("WARN: failed to read {s3_path} for upload: {e}"),
        }
    }
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
        if !is_split(seg_path) {
            continue;
        }
        let Ok(rel_path) = seg_path.strip_prefix(data_dir) else {
            continue;
        };
        let s3_prefix = rel_path.to_string_lossy().into_owned();
        let cached_shards = cached.get(&s3_prefix);
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
    let tenant = match authenticate(&headers) {
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
    // Dev mode: if no keys configured, allow all requests.
    if API_KEYS.is_empty() {
        return Ok("dev".to_string());
    }

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
        });

    match api_key {
        Some(key) => match API_KEYS.get(key) {
            Some(tenant) => Ok(tenant.clone()),
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
    // v1 admin routes — all mutate segment/manifest state.
    if request_line.starts_with("POST /v1/admin/") {
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

fn route(
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

    // ── Internal, pod-to-pod only ────────────────────────────────────────
    // Never called by clients — only by a peer replica that lost the
    // `hydration_leases` claim race for this file (see `fetch_from_peer`).
    // Goes through the same tenant API-key auth as every other route above
    // (no separate internal secret); peers present their own KOSHA_API_KEY.
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
            state.ensure_segments_local(&paths, true);
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
    if manifest_changed {
        state.publish_namespace(&ns);
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
            state.ensure_segments_local(&paths, true);
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
            state.ensure_segments_local(&paths, true);
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
    state.publish_namespace(&namespace);

    json_ok(&IndexResponse {
        indexed_count: count,
        namespace,
    })
}

// ─── POST /flush ────────────────────────────────────────────────────────────

fn handle_flush(body: &[u8], state: &AppState) -> String {
    let req: std::collections::HashMap<String, String> =
        serde_json::from_slice(body).unwrap_or_default();
    let ns = req.get("namespace").cloned();

    {
        let indexer = &state.indexer;
        match ns {
            Some(ref name) => {
                let namespace = NamespaceId(name.clone());
                if let Err(e) = indexer.flush_namespace(&namespace) {
                    return json_error(500, &format!("flush error: {e}"));
                }
            }
            None => {
                if let Err(e) = indexer.flush_all() {
                    return json_error(500, &format!("flush error: {e}"));
                }
            }
        }
    }

    // Publish manifest(s) to the control store and upload segments to S3.
    match ns {
        Some(ref name) => state.publish_namespace(&NamespaceId(name.clone())),
        None => {
            let all: Vec<NamespaceId> = {
                let indexer = &state.indexer;
                indexer.namespaces()
            };
            for ns_id in &all {
                state.publish_namespace(ns_id);
            }
        }
    }

    json_ok(&serde_json::json!({"status": "flushed"}))
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

    json_ok(&serde_json::json!({
        "total_documents": total_docs,
        "total_segments": total_segments,
        "namespaces": namespaces,
        "control_plane": state.control_plane_kind,
        "cache_root": state.cache.root().display().to_string(),
        "cache_size_bytes": state.cache.total_size(),
        "s3_enabled": s3_enabled,
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

    let (manifest, tombstones) = {
        let indexer = &state.indexer;
        let m = match indexer.manifest_cloned(&ns) {
            Some(m) => m,
            None => return json_error(404, &format!("namespace '{}' not found", ns.0)),
        };
        let t = indexer.get_tombstones(&ns);
        (m, t)
    };

    // ── Whole-response result cache ─────────────────────────────────────
    // Real query traffic is Zipfian in queries too; identical requests
    // against an unchanged namespace re-pay the full scoring pass today.
    // Keyed by (namespace, manifest_version, canonical query): a publish
    // bumps the version and orphans stale entries, the same free
    // invalidation the hydration-verdict and blob-presence caches use.
    // kNN queries are cacheable too — ANN probing is deterministic for a
    // fixed manifest — but their embedding is digested rather than
    // inlined (see `canonical_query_for_cache`): a 1024-dim vector is
    // ~14KB of JSON, which would bloat every key and turn each lookup
    // into a long string compare. Deliberately NOT cached: requests that
    // opt out (`no_cache`) and namespaces with live tombstones —
    // tombstones flow to the searcher outside the manifest, so a
    // version-keyed entry could serve deleted docs (verify-then-relax is
    // a follow-up, skip-when-present is the safe v1).
    let has_tombstones = tombstones
        .as_ref()
        .is_some_and(|t| t.values().any(|s| !s.is_empty()));
    let cache_key =
        if result_cache_budget() > 0 && !query.no_cache.unwrap_or(false) && !has_tombstones {
            canonical_query_for_cache(&query)
                .map(|canonical| (ns.0.clone(), manifest.version, canonical))
        } else {
            None
        };
    if let Some(ref key) = cache_key {
        if let Some(resp) = state.result_cache.lock().unwrap().lookup(key) {
            return resp;
        }
    }

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
        let cached_complete = state
            .completed_hydrations
            .lock()
            .unwrap()
            .get(&verdict_key)
            .copied()
            .unwrap_or(false);
        let outcome = if cached_complete {
            // The cached verdict can only ever cover the *static* scoring
            // set (footers/TOCs/offsets/filters). Posting blobs are fetched
            // per query — each term hashes to one of the 256 shard files —
            // so a namespace can be "fully hydrated" for every query seen
            // so far and still lack the shard a fresh term needs. Skipping
            // this ensure silently scored fresh terms against absent
            // shards as zero hits (non-deterministically per pod). It is
            // sub-ms when the shards are already local: one metadata stat
            // per term-shard per split segment.
            state.ensure_query_posting_blobs(&ns, &manifest, &query)
        } else {
            let outcome = state.hydrate_segments_for_search(&ns, &manifest, &query);
            // Cache the verdict on success: the static scoring set is
            // local for this manifest version, so future warm queries
            // skip the per-segment static checks (posting blobs are
            // still ensured per query above).
            if outcome.missing.is_empty() {
                state
                    .completed_hydrations
                    .lock()
                    .unwrap()
                    .insert(verdict_key, true);
            }
            outcome
        };
        if !outcome.missing.is_empty() {
            return json_error(503, &hydration_failed_message(&outcome.missing));
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
    let _slot = state.search_semaphore.acquire();
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
        let (files, bytes) = state.ensure_files_local(segs, "doc_store.bin");
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
        &manifest,
        &query,
        tombstones.as_ref(),
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

    let k1: f64 = params.get("k1").and_then(|v| v.parse().ok()).unwrap_or(1.2);
    let b: f64 = params.get("b").and_then(|v| v.parse().ok()).unwrap_or(0.75);

    let query = SearchQuery {
        query_text,
        max_results,
        bm25_params: kosha_core::Bm25Params { k1, b },
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
    };

    let (manifest, tombstones) = {
        let indexer = &state.indexer;
        let m = match indexer.manifest_cloned(&ns) {
            Some(m) => m,
            None => return json_error(404, &format!("namespace '{}' not found", ns.0)),
        };
        let t = indexer.get_tombstones(&ns);
        (m, t)
    };

    let t_hydrate = Instant::now();
    #[cfg(feature = "s3")]
    let hydration = {
        // Same cached-verdict short-circuit as `handle_search_post` —
        // see that function for the rationale (Tier 1 O2).
        let verdict_key = (ns.0.clone(), manifest.version);
        let cached_complete = state
            .completed_hydrations
            .lock()
            .unwrap()
            .get(&verdict_key)
            .copied()
            .unwrap_or(false);
        let outcome = if cached_complete {
            // Posting blobs are per-query and outside the cached verdict —
            // see `handle_search_post` for the full rationale.
            state.ensure_query_posting_blobs(&ns, &manifest, &query)
        } else {
            let outcome = state.hydrate_segments_for_search(&ns, &manifest, &query);
            if outcome.missing.is_empty() {
                state
                    .completed_hydrations
                    .lock()
                    .unwrap()
                    .insert(verdict_key, true);
            }
            outcome
        };
        if !outcome.missing.is_empty() {
            return json_error(503, &hydration_failed_message(&outcome.missing));
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
    let _slot = state.search_semaphore.acquire();
    let queue_ms = t_queue.elapsed().as_secs_f64() * 1e3;

    // On-demand `doc_store.bin` page materialization — see
    // `handle_search_post` for the rationale (Option A: persist the whole
    // `doc_store.bin` per page-segment on first miss, then local seeks).
    #[cfg(feature = "s3")]
    let ensure_doc_store = |segs: &[std::path::PathBuf]| {
        let (files, bytes) = state.ensure_files_local(segs, "doc_store.bin");
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
        &manifest,
        &query,
        tombstones.as_ref(),
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
            );
            json_ok(&result)
        }
        Err(e) => {
            // Same verdict invalidation as `handle_search_post` — see
            // that function for the rationale (a stale verdict could
            // cause the retry to also skip hydration and fail again).
            #[cfg(feature = "s3")]
            state
                .completed_hydrations
                .lock()
                .unwrap()
                .remove(&(ns.0.clone(), manifest.version));
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

    let mut rebuilt = 0usize;
    let mut errors = Vec::new();
    for (entry, seg_path) in manifest.segments.iter().zip(seg_paths.iter()) {
        if !seg_path.exists() {
            errors.push(format!("{}: segment not local", entry.segment_id.0));
            continue;
        }
        match SegmentReader::rewrite_footer_blooms(seg_path) {
            Ok(_) => {
                rebuilt += 1;
                #[cfg(feature = "s3")]
                state.sync_file_to_s3(seg_path, "footer.json");
            }
            Err(e) => errors.push(format!("{}: {e}", entry.segment_id.0)),
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
                state.ensure_files_local(std::slice::from_ref(&seg_path), "footer.json");
                state.ensure_files_local(std::slice::from_ref(&seg_path), "doc_store.bin");
            }

            match SegmentReader::backfill_offset_tables(&seg_path) {
                Ok(_) => {
                    #[cfg(feature = "s3")]
                    {
                        state.sync_file_to_s3(&seg_path, "doc_store.offsets");
                        state.sync_file_to_s3(&seg_path, "footer.json");
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
///                "max_segment_bytes": 5368709120}   // optional; 0 = uncapped
///
/// Merged output size is capped (default 5GiB, Lucene-style): `full` under a
/// cap merges one greedy smallest-first group per pass — loop the endpoint
/// until `segments_after` stops dropping to converge on
/// ~ceil(total_bytes/cap) segments. `max_segment_bytes: 0` restores the
/// legacy all-to-one merge.
///
/// Every existing segment should hydrate locally first (compaction reads each
/// one via `SegmentReader::open`) — for a namespace with hundreds of segments
/// this call can take a while.
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

    let (segments_before, not_hydrated) = {
        let manifest = state
            .indexer
            .manifest_cloned(&ns)
            .expect("namespace existence checked above");
        // Hydrate every segment in one fanned-out batch rather than one
        // ensure_segment_local round trip per segment — see handle_index.
        // This was the actual bottleneck behind this endpoint's own
        // timeouts: 441 sequential S3 round trips before compaction could
        // even start reading anything.
        let seg_paths: Vec<PathBuf> = manifest
            .segments
            .iter()
            .map(|entry| state.data_dir.join(&ns.0).join(&entry.segment_id.0))
            .collect();
        #[cfg(feature = "s3")]
        // Admin routes rewrite or merge segments — must preserve vectors.
        state.ensure_segments_local(&seg_paths, true);
        // compact_namespace silently skips (and keeps unmerged) any segment
        // that isn't present locally after this — safe, but worth surfacing
        // so a caller knows compaction was partial.
        let not_hydrated: Vec<String> = manifest
            .segments
            .iter()
            .zip(seg_paths.iter())
            .filter(|(_, p)| !p.exists())
            .map(|(entry, _)| entry.segment_id.0.clone())
            .collect();
        (manifest.segments.len(), not_hydrated)
    };

    let mut policy = state.indexer.compaction_policy().clone();
    // Merge-size cap precedence: policy default (5GiB) < env < request body.
    // `0` disables the cap (legacy all-to-one full merge — the 167→1 merge
    // that put the 10M bench box into reclaim stalls; opt back in explicitly
    // if that's really wanted).
    if let Some(n) = std::env::var("KOSHA_COMPACT_MAX_SEGMENT_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
    {
        policy.max_merged_segment_bytes = n;
    }
    if let Some(n) = req.get("max_segment_bytes").and_then(|v| v.as_u64()) {
        policy.max_merged_segment_bytes = n;
    }
    let max_segment_bytes = policy.max_merged_segment_bytes;
    let opts = kosha_write::CompactOptions { mode, policy };
    let result = match state.indexer.compact_namespace_with_options(&ns, opts) {
        Ok(r) => r,
        Err(e) => return json_error(500, &format!("compaction error: {e}")),
    };

    // Persist the merged manifest and upload the new segment to S3 — same
    // path a normal flush uses (sync_latest_segment_to_s3 picks up the
    // compacted segment because compact_namespace pushes it last).
    if result.merged {
        state.publish_namespace(&ns);
    }

    let segments_after = state
        .indexer
        .manifest_cloned(&ns)
        .map(|m| m.segments.len())
        .unwrap_or(0);

    json_ok(&serde_json::json!({
        "namespace": ns.0,
        "mode": match mode {
            kosha_write::CompactMode::Tiered => "tiered",
            kosha_write::CompactMode::Full => "full",
        },
        "merged": result.merged,
        "segments_before": segments_before,
        "segments_after": segments_after,
        "segments_merged": result.segments_merged,
        "max_segment_bytes": max_segment_bytes,
        "not_hydrated": not_hydrated,
    }))
}

/// POST /v1/admin/api-keys — create a new API key for a tenant.
///
/// Requires existing admin API key (from KOSHA_API_KEY env var or DB).
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

    // Try to create via PgStore (only works with postgres feature).
    #[cfg(feature = "postgres")]
    {
        let controller = state.controller.lock().unwrap();
        // Downcast the Box<dyn ControlStore> to see if it's a PgStore.
        // We use a simple approach: look for "postgres" in the store type name.
        // Check if DATABASE_URL is set (indicating PgStore).
        drop(controller);

        if std::env::var("DATABASE_URL").is_err() {
            return json_error(400, "Postgres not configured (DATABASE_URL not set)");
        }

        // Re-create connection for the admin operation.
        // This is a simplified approach — in production, share the pool.
        match std::env::var("DATABASE_URL") {
            Ok(db_url) => match kosha_control::PgStore::new(&db_url) {
                Ok(store) => match store.create_api_key(tenant_id, description) {
                    Ok(api_key) => json_ok(&serde_json::json!({
                        "api_key": api_key,
                        "tenant_id": tenant_id,
                        "description": description,
                    })),
                    Err(e) => json_error(500, &format!("failed to create key: {e}")),
                },
                Err(e) => json_error(500, &format!("failed to connect to postgres: {e}")),
            },
            Err(_) => json_error(400, "Postgres not configured"),
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

/// Lexical terms used for footer term-bloom prune before hydrate/open.
///
/// Wildcard queries cannot be pruned here — expansion needs the segment
/// vocabulary. Phrase and multi-term BM25 use AND semantics.
#[cfg(feature = "s3")]
fn term_bloom_prune_for_query(query: &SearchQuery) -> Option<(Vec<String>, TermBloomMode)> {
    if query.wildcard.is_some() {
        return None;
    }
    if let Some(ref mp) = query.match_phrase {
        let terms = tokenize(&mp.phrase);
        if terms.is_empty() {
            return None;
        }
        return Some((terms, TermBloomMode::And));
    }
    let terms = tokenize(&query.query_text);
    if terms.is_empty() {
        None
    } else {
        Some((terms, TermBloomMode::And))
    }
}

/// Whether scoring/materialization needs `filters.bin`. Broad lexical
/// queries with default score/_id ranking do not: they use only postings,
/// doc metadata, and page doc stores.
#[cfg(feature = "s3")]
fn query_needs_filter_store(query: &SearchQuery) -> bool {
    query.filter.is_some()
        || !query.aggs.is_empty()
        || query.sort.iter().any(|spec| {
            spec.fields
                .keys()
                .any(|field| field != "_score" && field != "_id")
        })
}

#[cfg(feature = "s3")]
fn posting_blob_files_for_query(query: &SearchQuery) -> Vec<String> {
    if query.wildcard.is_some() {
        return kosha_segment::all_posting_blob_files();
    }

    let mut files = std::collections::HashSet::new();
    for term in tokenize(&query.query_text) {
        files.insert(kosha_segment::posting_blob_file_for_term(&term));
    }
    if let Some(ref phrase) = query.match_phrase {
        for term in tokenize(&phrase.phrase) {
            files.insert(kosha_segment::posting_blob_file_for_term(&term));
        }
    }
    let mut files: Vec<String> = files.into_iter().collect();
    files.sort();
    files
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
) {
    let total_ms = hydrate_ms
        + queue_ms
        + phases.admit_wall_ms
        + phases.score_wall_ms
        + phases.materialize_wall_ms;
    println!(
        "search timing: ns={} total_ms={:.1} hydrate_ms={:.1} hydrate_files={} \
         hydrate_mb={:.1} queue_ms={:.1} admit_ms={:.1} score_ms={:.1} \
         materialize_ms={:.1} opens_cold={} opens_cached={} open_total_ms={:.1} hits={}",
        ns.0,
        total_ms,
        hydrate_ms,
        hydration.files_fetched,
        hydration.bytes_fetched as f64 / (1024.0 * 1024.0),
        queue_ms,
        phases.admit_wall_ms,
        phases.score_wall_ms,
        phases.materialize_wall_ms,
        phases.open_cold,
        phases.open_cached,
        phases.open_total_ms,
        total_hits,
    );
}

/// Map a search failure onto the right HTTP status: load-shedding
/// (`KoshaError::Overloaded`, see `kosha_query::MemoryLedger`) is a
/// transient 429 the client should retry with backoff, not a 500.
fn search_error_response(e: &KoshaError) -> String {
    match e {
        KoshaError::Overloaded(_) => json_error(429, &format!("search overloaded: {e}")),
        _ => json_error(500, &format!("search error: {e}")),
    }
}

fn json_error(status_code: u16, message: &str) -> String {
    let body = serde_json::json!({"error": message}).to_string();
    let status_line = match status_code {
        400 => "400 Bad Request",
        404 => "404 Not Found",
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
            "POST /v1/admin/api-keys HTTP/1.1\r\n",
            "POST /v1/admin/rebuild-filter-blooms HTTP/1.1\r\n",
            "POST /v1/admin/backfill-offset-tables HTTP/1.1\r\n",
            "POST /v1/admin/compact-namespace HTTP/1.1\r\n",
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
        let state = test_state();
        state
            .cache
            .put_bytes("ns/seg/doc_store.bin", b"hello segment")
            .unwrap();

        let response = route(
            "GET /internal/segment/ns/seg/doc_store.bin HTTP/1.1\r\n",
            &HashMap::new(),
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
        let response = route(
            "GET /internal/segment/ns/seg/missing.bin HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &test_state(),
        );
        assert!(response.starts_with("HTTP/1.1 404"), "response: {response}");
    }

    #[cfg(all(feature = "s3", feature = "postgres"))]
    #[test]
    fn internal_segment_route_rejects_path_traversal() {
        let response = route(
            "GET /internal/segment/../../etc/passwd HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &test_state(),
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
        let peer = test_state();
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

        let before = state.indexer.manifest_cloned(&ns).unwrap().segments.len();
        assert_eq!(before, 3, "expected one segment per flush");

        let response = route(
            "POST /v1/admin/compact-namespace HTTP/1.1\r\n",
            &HashMap::new(),
            br#"{"namespace":"compact-ns","mode":"full"}"#,
            "test",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("\"segments_before\":3"), "{response}");
        assert!(response.contains("\"segments_after\":1"), "{response}");
        assert!(response.contains("\"mode\":\"full\""), "{response}");

        let after = state.indexer.manifest_cloned(&ns).unwrap().segments.len();
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
    fn compact_namespace_endpoint_404s_for_unknown_namespace() {
        let state = test_state();
        let response = route(
            "POST /v1/admin/compact-namespace HTTP/1.1\r\n",
            &HashMap::new(),
            br#"{"namespace":"does-not-exist"}"#,
            "test",
            &state,
        );
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
            indexer.flush_namespace(&NamespaceId(ns.into())).unwrap();
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
            .manifest(&NamespaceId(ns.into()))
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
            .manifest(&NamespaceId(ns.into()))
            .expect("auto-flush must persist the manifest to the control store");
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.segments[0].doc_count, 1000);
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
        let dir = std::env::temp_dir().join(format!(
            "kosha-test-server-{}",
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
            bm25_params: kosha_core::Bm25Params::default(),
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

        // The whole point: no raw embedding in the key.
        assert!(
            a.len() < 1024,
            "key must stay compact (digested), got {} bytes",
            a.len()
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
