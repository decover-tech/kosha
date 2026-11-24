# Guarantees and limits

This page lists what Kosha promises about durability, consistency and
visibility, the limits it enforces, and how fast it is.

Every number here comes from one of two sources, and the Source or Result file
column says which:

- **Code:** the file that holds the default and the `KOSHA_*` variable that
  overrides it. CI runs `scripts/docs/check_guarantees.py`, which reads each
  of these constants from the code and fails if the page shows a different
  value.
- **Measurement:** a committed results file, with the date, the commit and
  the CI run id where there is one. The same script checks that the file
  exists and contains the run ids and dates quoted here.

When nothing committed backs a claim, the page says **not yet measured**
instead of giving a number. Defaults are per pod unless the row says
otherwise. Every override is described in
[configuration.md](configuration.md).

Status codes on this page are those of the #350 error model, which this
build ships (`crates/kosha-server/src/api_error.rs`, documented in
[api/errors.md](api/errors.md)). Every error body carries a stable `code`
and a `retryable` flag. A retryable error also carries `retry_after_ms` and
a `Retry-After` header. A non-retryable one carries neither. Where a cell
names a `code`, CI checks its HTTP status and retryability against
`ErrorCode::status` and `ErrorCode::retryable` in that file.

---

## Guarantees

### Durability on acknowledgement

An index, replace or delete request returns success only after its records
are durable in the write-ahead log (WAL). What "durable" means depends on
`KOSHA_WAL_MODE`:

| Mode | Acked write survives | Acked write is lost by |
| --- | --- | --- |
| `local` (the default) | process crash and pod restart on the same node and disk. The WAL file is fsynced, and so is its directory, before the ack | loss of the node's disk before the next flush uploads a segment to S3. The server says this at startup: "acked writes are not S3-durable until flushed" |
| `s3` | loss of the node. Each group commit is a conditional PUT (`If-None-Match: *`) to `_wal/{writer_id}/` in the bucket, and the ack waits for it | nothing short of losing the bucket. A PUT that fails or times out fails the write (HTTP 500), and nothing in it is acked. The server will not start against a store that ignores conditional PUT |

Concurrent writes share a commit. One leader collects the records that are
waiting, writes them as one WAL file (or one PUT), syncs once, and then acks
every caller in the group. A lone writer never waits for the window.

| Setting | Value | Source |
| --- | --- | --- |
| Group-commit window | 2 ms | `crates/kosha-write/src/wal.rs` (`GroupCommitConfig::default`), override `KOSHA_WAL_GROUP_COMMIT_MS` |
| Group-commit size cut | 4 MiB | `crates/kosha-write/src/wal.rs` (`GroupCommitConfig::default`), override `KOSHA_WAL_GROUP_COMMIT_BYTES` |
| S3 WAL PUT timeout | 10 s | `crates/kosha-server/src/s3_wal.rs` (`timeout_from_env`), override `KOSHA_WAL_S3_TIMEOUT_MS` |

Ack latency:

| Measurement | p50 | p99 | Result file |
| --- | --- | --- | --- |
| Local WAL ack, 1 writer, developer laptop (macOS `F_FULLFSYNC`), 2026-11-22, commit `e174058` | 7.99 ms | 8.22 ms | `scripts/bench/results/2026-11-22-wal-group-commit.md` |
| Local WAL ack, 16 concurrent writers, same machine and run, 2026-11-22, commit `e174058` | 10.93 ms | 14.95 ms | `scripts/bench/results/2026-11-22-wal-group-commit.md` |

The laptop rows show how group commit behaves: 16 writers share about one
fsync per 16 appends. They are not production numbers.

- Local WAL ack latency on production hardware (Linux, EBS): **not yet
  measured.**
- S3 WAL ack latency against real S3: **not yet measured.** The README's S3
  WAL table comes from a simulated PUT model (`crates/kosha-write/benches/s3_wal_ack.rs`),
  not from a bucket.

### Consistency

- **Read-your-writes is the default.** A search with no `consistency` field
  is `strong`. It searches the indexed segments and also scans the ingest
  pod's unindexed tail. A document is therefore searchable as soon as the
  write that carried it returns, before any flush. `KOSHA_DEFAULT_CONSISTENCY`
  changes the default (`crates/kosha-server/src/unindexed.rs`,
  `AppState::default_consistency`).
- **Exception: a query pod that cannot read the tail.** A query pod without
  `KOSHA_INTERNAL_API_KEY` cannot fetch the ingest pod's tail, so its default
  is `eventual`. An explicit `"consistency": "strong"` on such a pod fails
  with a message that says why. Set the internal key on every pod to get
  read-your-writes everywhere.
- **Exception: a writer ahead of the published manifest.** If the tail and
  the manifest disagree, the search retries and then returns 503. If the
  writer has moved past the manifest the query pod has, the search falls
  back to `eventual` and sets `consistency_fallback` in the response.
- **`eventual`** searches only what has been flushed into segments and
  published in the manifest. See write-to-searchable latency below.

### Cross-node visibility

Query pods learn about new segments by polling the control store for
manifest versions. A pod that cannot reach the control store keeps serving
its last good manifest.

| Setting | Value | Source |
| --- | --- | --- |
| Manifest poll interval | 500 ms | `crates/kosha-server/src/manifest_follow.rs` (`DEFAULT_REFRESH_MS`), override `KOSHA_MANIFEST_REFRESH_MS` (`0` checks on every search) |

A search can ask for fresher data with `max_staleness_seconds`. That makes
the pod check the manifest version before it answers.

### Write-to-searchable latency

| Read | Visible after |
| --- | --- |
| `strong` (the default) | the write's acknowledgement. The tail scan sees it immediately |
| `eventual`, same pod | the next flush, plus the time to build the segment. A flush is triggered by the idle-flush timer, by the size threshold below, by a delete-by-query on the namespace (it flushes the buffer and publishes), or by an explicit flush |
| `eventual`, another query pod | the next flush, plus up to one manifest poll interval |

| Setting | Value | Source |
| --- | --- | --- |
| Idle flush | 1000 ms | `crates/kosha-server/src/main.rs` (`DEFAULT_FLUSH_MAX_AGE_MS`), override `KOSHA_FLUSH_MAX_AGE_MS` (`0` disables the timer) |
| Size flush | 1000 docs per namespace | `crates/kosha-server/src/main.rs` (`flush_threshold`), override `KOSHA_FLUSH_THRESHOLD` |
| Idle flush check | every 250 ms (a quarter of the idle flush, at least 50 ms) | `crates/kosha-server/src/main.rs` (`idle_flush_tick`), follows `KOSHA_FLUSH_MAX_AGE_MS` |

The idle timer measures a namespace's age from its oldest pending document.
With no other trigger, a namespace becomes due for an idle flush about
1.25 s after its oldest pending write (1000 ms plus one 250 ms check). That
is when the flush starts, not when the document is visible, and there is no
upper bound on visibility. One timer thread flushes and publishes every due
namespace in turn and waits one check interval after each pass, so a
document also waits for its own segment build and publish and for the
flushes and publishes of every namespace ahead of it in that pass and the
one before. With many active namespaces or large buffers this adds seconds.
That is the likely source of the tail in the first measurement below (21
namespaces, p99 4.6 s); that run did not record flush timings.
A document written while older ones are pending is published sooner.

| Measurement | p50 | p99 | Result file |
| --- | --- | --- | --- |
| Write-to-visible, `eventual` read, idle and size flush only (no deletes), one standalone pod on a developer laptop, 3 min, 70 samples, 2026-11-22, commit `e174058` | 0.46 s | 4.642 s | `scripts/soak/results/2026-11-22-local-no-delete.md` |
| Write-to-visible, `eventual` read, mixed workload with 20% delete-by-query (deletes also flush), same setup, 60 samples, 2026-11-22, commit `e174058` | 0.027 s | 3.659 s | `scripts/soak/results/2026-11-22-local.md` |

Use the first row for the idle-flush path; its p99 is well over the
1.25 s idle-flush trigger, most likely because of the serialized flushes
described above. In the second row, deletes
flushed the namespaces often, so its p50 is not a typical write-to-visible
time. Both runs are short and local, and each p99 is the single slowest
sample. They are not production guarantees.

- Write-to-visible on production hardware, across pods (ingest to query
  pod): **not yet measured.** The soak driver measures it
  (`scripts/soak/soak.py`). The CI smoke runs with the idle flush off, and
  no report from the 24-hour stage soak has been committed.

---

## Limits

### What #352 asks for, at a glance

| Item | Limit today | Tested to |
| --- | --- | --- |
| Docs per namespace | **Not enforced** by default. Opt-in per tenant: `docs_per_namespace` | 10M documents (BM25 and 1024-dim vector), see Latency |
| Bytes per namespace | **Not enforced** by default. Opt-in per tenant: `bytes_per_namespace`, an ingest admission estimate. A free billing plan can separately cap a tenant's average `stored_bytes` per month (HTTP 402) | about 249 GB (10M × 1024-dim vectors), see Latency |
| Namespaces per tenant | **Not enforced** by default. Opt-in per tenant: `namespaces` | not yet measured |
| Max document size | **No per-document limit.** The request body cap bounds it: see "Request body" under [Hard limits](#hard-limits) | not yet measured |
| Max vector dimensions | **No global limit** on vectors you supply. All vectors in one namespace field must have the same length. Server-side embedding is capped: see "Server-side embedding dimensions" under [Hard limits](#hard-limits). Opt-in per tenant: `vector_dims` | 1024 dims |
| Max top-k (`max_results`, `knn.k`) | **Not enforced.** For the defaults see [Defaults that bound a response](#defaults-that-bound-a-response). No `from + size` window limit exists | not yet measured |
| Request size | See "Request body" under [Hard limits](#hard-limits) | not yet measured |

This table restates no numbers from the code. Each value lives in one
checked row below, so it cannot drift from the code without CI failing.

An unbounded `max_results`, `knn.k` or `num_candidates` costs memory and CPU
on the pod that serves it. The memory admission and search queue limits
below shed such a search with 429 rather than letting it take the pod down.
Hard caps on these are not yet set (remaining work after #352).

### Hard limits

These apply to every request, whatever the tenant's plan.

| Limit | Value | When exceeded | Source |
| --- | --- | --- | --- |
| Request body | 64 MiB | 413 `payload_too_large`, not retryable, before the body is read when `Content-Length` is sent | `crates/kosha-server/src/auth.rs` (`max_body_bytes`), override `KOSHA_MAX_BODY_BYTES` |
| Histogram buckets per aggregation | 10,000 | 400 | `crates/kosha-core/src/histogram.rs` (`MAX_HISTOGRAM_BUCKETS`), no override |
| Server-side embedding dimensions | 4096 | schema rejected (400) | `crates/kosha-core/src/analysis.rs` (`MAX_EMBEDDING_DIMS`), no override |
| Shards per namespace | 256 | 400 | `crates/kosha-core/src/sharding.rs` (`MAX_SHARDS`), no override |
| Rerank `top_n` | 1000 | 400 | `crates/kosha-core/src/lib.rs` (`MAX_RERANK_TOP_N`), no override |
| Rerank `timeout_ms` | 60000 ms | 400 | `crates/kosha-core/src/lib.rs` (`MAX_RERANK_TIMEOUT_MS`), no override |
| `rank_by` window | 10,000 | 400 | `crates/kosha-core/src/rank.rs` (`MAX_RANK_BY_WINDOW`), no override |
| Fuzzy `max_expansions` | 1024 | 400 | `crates/kosha-core/src/fuzzy.rs` (`MAX_MAX_EXPANSIONS`), no override |
| Fuzzy edit distance | 2 | 400 | `crates/kosha-core/src/fuzzy.rs` (`MAX_FUZZY_EDITS`), no override |
| New namespace or branch name | 120 bytes of UTF-8 | 400 `invalid_argument`, not retryable, when the namespace or branch is created (explicitly or by a first write). An existing longer name stays reachable | `crates/kosha-core/src/tenancy.rs` (`MAX_NAMESPACE_NAME_BYTES`), no override |
| New API key's tenant id | 80 bytes of UTF-8 | 400 `invalid_argument`, not retryable, when the key is registered. Existing keys are not re-checked | `crates/kosha-core/src/tenancy.rs` (`MAX_TENANT_ID_BYTES`), no override |

Aggregations nest one level deep: a bucket aggregation may hold metric
sub-aggregations only. `terms` and `composite` sizes are not capped.

Namespace and branch names are one path component: not empty, not `.` or
`..`, with no `/`, `\` or control characters, and not of the form
`_shard-NNNN`. A new name's length is capped (the "New namespace or
branch name" row above): a namespace is a directory, its segment directory
names embed the tenant and the namespace, and filesystems cap a path
component at 255 bytes. A new API key's tenant id is capped for the same
reason (the "New API key's tenant id" row). A namespace created before the
cap with a longer name can still be searched, written and deleted.

### Defaults that bound a response

These are not refusals. They decide how much a request returns when it does
not say.

| Default | Value | Source |
| --- | --- | --- |
| `max_results` (top-k) default | 10 | `crates/kosha-core/src/lib.rs` (`default_max_results`), set per request |
| `knn.k` default | 10 | `crates/kosha-core/src/lib.rs` (`default_knn_k`), set per request |
| `knn.num_candidates` default | 100 | `crates/kosha-core/src/lib.rs` (`default_knn_num_candidates`), set per request |
| Total hits counted | 10,000 | `crates/kosha-query/src/lib.rs` (`total_hits_cap_engine_default`), override `KOSHA_TOTAL_HITS_CAP`, or per request `total_hits_cap` / `exact_total_hits` |
| Text value copied into filter columns | 256 chars | `crates/kosha-segment/src/lib.rs` (`DEFAULT_TEXT_FILTER_IGNORE_ABOVE`), override `KOSHA_TEXT_FILTER_IGNORE_ABOVE` (`off` disables) |

Past the total-hits cap, the response sets `total_hits_relation: "gte"`.
Longer text values are still fully searchable. They are only left out of
the term filter columns.

### Per-pod capacity

Each pod sheds load rather than queueing without bound. Over these limits a
request gets a retryable 503, 429 or 408 with `Retry-After`. The server did
not apply it, so it is safe to retry.

| Limit | Value | When exceeded | Source |
| --- | --- | --- | --- |
| Open HTTP connections | 1024 | 503 `unavailable`, retryable, with `Retry-After` (below), connection closed | `crates/kosha-server/src/http.rs` (`Config::new`), override `KOSHA_HTTP_MAX_CONNECTIONS` |
| Requests in flight | 256 | 503 `unavailable`, retryable, with `Retry-After` (below) | `crates/kosha-server/src/http.rs` (`Config::new`), override `KOSHA_HTTP_MAX_INFLIGHT` |
| Concurrent searches | 8 | queued up to the search queue depth (below), then 429 `overloaded`, retryable | `crates/kosha-server/src/main.rs` (search admission), override `KOSHA_MAX_CONCURRENT_SEARCHES` |
| Search queue wait | 10,000 ms | 429 `overloaded`, retryable | `crates/kosha-server/src/main.rs` (search admission), override `KOSHA_SEARCH_QUEUE_TIMEOUT_MS` |
| Request body read timeout | 300 s | 408 `request_timeout`, retryable | `crates/kosha-server/src/main.rs` (`io_timeout_secs`), override `KOSHA_HTTP_IO_TIMEOUT_SECS` |
| Search queue depth | 4 × permits | 429 `overloaded`, retryable | `crates/kosha-server/src/main.rs` (search admission, `permits * 4`), override `KOSHA_SEARCH_QUEUE_DEPTH` |
| Unindexed backlog per namespace | 2 GiB | index and replace get 429 `overloaded`, retryable, with `Retry-After` (below) until flushes catch up | `crates/kosha-server/src/unindexed.rs` (`DEFAULT_MAX_UNINDEXED_BYTES`), override `KOSHA_MAX_UNINDEXED_BYTES` (`0` disables) |
| Connection and in-flight shed `Retry-After` | 1 s | sent with the 503 | `crates/kosha-server/src/api_error.rs` (`DEFAULT_RETRY_AFTER`, which `busy` in `http.rs` uses because it sets no delay of its own), no override |
| Unindexed backlog `Retry-After` | 1 s | sent with the 429 | `crates/kosha-server/src/unindexed.rs` (`BACKPRESSURE_RETRY_AFTER_SECS`), no override |

Memory admission (`KOSHA_SEGMENT_LIVE_MAX_BYTES`, `KOSHA_ADMISSION_TIMEOUT_MS`)
and the per-search hydration ceiling (`KOSHA_HYDRATE_REQUEST_MAX_BYTES`) also
shed searches with 429. See "Backpressure and memory admission" and
"Hydration" in [configuration.md](configuration.md).

### Per-tenant limits (opt-in)

Without `KOSHA_TENANT_LIMITS_FILE`, or a billing plan with `limits`, **no
tenant is limited**. No plan is built in. Each limit is enforced per pod.
Query limits apply on the pod that serves the search, and write limits on
the ingest pod. A tenant's effective QPS therefore grows with the number of
query replicas. Admin and internal credentials are never limited.

| Field | Limits | Refusal |
| --- | --- | --- |
| `qps` | searches per second | 429 `tenant_qps_exceeded`, retryable, with `Retry-After` |
| `concurrent_queries` | searches in flight | 429 `tenant_concurrency_exceeded`, retryable, with `Retry-After` |
| `write_docs_per_sec`, `write_bytes_per_sec` | write rate | 429 `tenant_write_rate_exceeded`, retryable, with `Retry-After` |
| `namespaces` | namespaces held (branches count, shards do not) | 403 `namespace_quota_exceeded`, not retryable, no `Retry-After` |
| `docs_per_namespace` | committed plus unflushed documents | 403 `doc_quota_exceeded`, not retryable, no `Retry-After` |
| `bytes_per_namespace` | estimated owned segment and unflushed bytes plus the incoming write | 403 `permission_denied` with `details.reason: "storage_quota_exceeded"`, not retryable, no `Retry-After` |
| `vector_dims` | widest vector, supplied or embedded | 403 `vector_dims_exceeded`, not retryable, no `Retry-After` |
| `max_body_bytes` | request body, under the pod cap | 413 `payload_too_large`, not retryable, no `Retry-After` |

Every tenant refusal keeps the pre-#350 `reason` and `limit` fields as
deprecated aliases (`reason` is still `body_too_large` for the body cap).
A quota or body-cap refusal tells the client nothing about when to retry:
retrying the same request cannot succeed until an operator raises the
limit or the tenant frees capacity.

The namespace and document quotas are approximate. Concurrent creations can
pass the namespace count together, and an unflushed upsert of an existing id
counts twice until it is flushed. The file format is in "Tenant limits" in
[configuration.md](configuration.md).

---

## Latency

### What warm and cold mean

- **Warm:** the namespace's segment files are on the pod's local disk and
  its segments are open, so a search does no S3 reads. With the result
  cache on, a repeated query is served from the cache. Rows marked "cache
  off" send `no_cache: true`.
- **Fresh pod (warmup-gated):** a newly started pod that waited for
  `/readyz`. Readiness waits until warmup has preloaded the namespace, which
  took 7.3 minutes for the 23 GB BM25 namespace. This is not an on-demand
  cold read.
- **Cold:** the first searches after a pod starts with an empty local cache
  and no warmup. Each search fetches the segment blobs it needs from S3
  before scoring.

### Measured

All rows ran at 8 QPS for 30 minutes per phase, top-k 10, on the bench
cluster. The engine was main with #362 and #373. The results were recorded
in `scripts/bench/bm25_scale/RESULTS.md` by commit `51f4549`.

| Measurement | p50 | p90 | p99 | Errors | Result file |
| --- | --- | --- | --- | --- | --- |
| BM25, 10M MSMarco docs, warm, cache off, 2026-11-20/21, run 44892212058, commit `51f4549` | 29.0 ms | 63.5 ms | 170 ms | 0 | `scripts/bench/bm25_scale/RESULTS.md` |
| BM25, 10M docs, warm, cache on, 2026-11-20/21, run 44892212058, commit `51f4549` | 1.7 ms | 12.1 ms | 66 ms | 0 | `scripts/bench/bm25_scale/RESULTS.md` |
| BM25, 10M docs, fresh pod (warmup-gated), cache off, 2026-11-20/21, run 44892212058, commit `51f4549` | 29.0 ms | 66.4 ms | 175 ms | 0 | `scripts/bench/bm25_scale/RESULTS.md` |
| BM25, 10M docs, fresh pod (warmup-gated), cache off, 2026-11-20/21, run 44843615336, commit `51f4549` | 32.7 ms | 74.5 ms | 201 ms | 0 | `scripts/bench/bm25_scale/RESULTS.md` |
| Vector, 10M × 1024-dim, k=10, `num_candidates`=100, warm, cache on, 2026-11-20/21, run 44876437446, commit `51f4549` | 88.0 ms | 111 ms | 222 ms | 0 | `scripts/bench/bm25_scale/RESULTS.md` |
| Vector, 10M × 1024-dim, fresh pod (warmup-gated), cache off, 2026-11-20/21, run 44876437446, commit `51f4549` | 88.5 ms | 113 ms | 607 ms | 263 × 429 | `scripts/bench/bm25_scale/RESULTS.md` |

Vector recall@10 against exact search was 0.90 mean (100 queries). Preloading
the 249 GB vector namespace took 1 h 55 min, and the next run's warmup timed
out at 2 h.

### Not yet measured

- **Cold, on current main:** not yet measured, for BM25 or vector. The only
  on-demand cold run in `RESULTS.md` is round 6 (2026-08-09) on an older
  build, so it is not quoted here as current.
- **BM25 at 1M documents:** not yet measured. This is the like-for-like size
  for comparing with other vendors' published BM25 numbers.
- **Warm latency with the result cache off, for vectors:** not yet measured.
- **Recovery time** after a pod crash (WAL replay) or a node loss: not yet
  measured.

The DESIGN.md targets (warm p50 ≤ 80 ms and p99 ≤ 300 ms, cold p99 ≤ 1.5 s)
are design goals, not guarantees or measurements.

---

## Keeping this page accurate

`python3 scripts/docs/check_guarantees.py` runs in CI. It fails when any of
the following happens:

- a code constant cited above changes and this page does not;
- a row cites a source file without naming its `KOSHA_*` override;
- a row cites code that the script does not check;
- a refusal or "limit today" cell states a number other than an HTTP status
  code, instead of pointing at the checked row that holds it;
- a refusal cell names an error `code` whose status or retryability in
  `crates/kosha-server/src/api_error.rs` differs from the cell;
- a cited results file is missing, or does not contain the run id or date
  quoted;
- a cited commit is not in git history;
- the page goes back to the pre-#350 "Refusal today" status codes while
  `proto/kosha/v1/errors.proto` (#350) exists.

To change a number, change the code or commit a new results file, then
update this page in the same PR.
