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

Status codes on this page are the ones the server returns today. The `/v1`
error model in #350 changes some of them. Tenant quota refusals move from
429 to 403, and the tenant body cap moves from 429 to 413. Those rows say so.

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
| `eventual`, same pod | the next flush: the idle-flush timer or the size threshold below, plus the time to build the segment |
| `eventual`, another query pod | the next flush, plus up to one manifest poll interval |

| Setting | Value | Source |
| --- | --- | --- |
| Idle flush | 1000 ms | `crates/kosha-server/src/main.rs` (`DEFAULT_FLUSH_MAX_AGE_MS`), override `KOSHA_FLUSH_MAX_AGE_MS` (`0` disables the timer) |
| Size flush | 1000 docs per namespace | `crates/kosha-server/src/main.rs` (`flush_threshold`), override `KOSHA_FLUSH_THRESHOLD` |

| Measurement | p50 | p99 | Result file |
| --- | --- | --- | --- |
| Write-to-visible, `eventual` read, idle flush on, one standalone pod on a developer laptop, 3 min, 60 samples, 2026-11-22, commit `e174058` | 0.027 s | 3.659 s | `scripts/soak/results/2026-11-22-local.md` |

That run is short and local, and its p99 is the single slowest of 60
samples. It is not a production guarantee.

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
| Bytes per namespace | **Not enforced.** There is no per-namespace storage quota. A free billing plan can cap a tenant's average `stored_bytes` per month (HTTP 402) | about 249 GB (10M × 1024-dim vectors), see Latency |
| Namespaces per tenant | **Not enforced** by default. Opt-in per tenant: `namespaces` | not yet measured |
| Max document size | **No per-document limit.** The request body cap (64 MiB below) bounds it | not yet measured |
| Max vector dimensions | **No global limit** on vectors you supply. All vectors in one namespace field must have the same length. Server-side embedding is capped at 4096 (below). Opt-in per tenant: `vector_dims` | 1024 dims |
| Max top-k (`max_results`, `knn.k`) | **Not enforced.** Defaults are 10 (below). No `from + size` window limit exists | not yet measured |
| Request size | 64 MiB (below) | not yet measured |

An unbounded `max_results`, `knn.k` or `num_candidates` costs memory and CPU
on the pod that serves it. The memory admission and search queue limits
below shed such a search with 429 rather than letting it take the pod down.
Hard caps on these are tracked in #350.

### Hard limits

These apply to every request, whatever the tenant's plan.

| Limit | Value | When exceeded | Source |
| --- | --- | --- | --- |
| Request body | 64 MiB | 413, before the body is read when `Content-Length` is sent | `crates/kosha-server/src/auth.rs` (`max_body_bytes`), override `KOSHA_MAX_BODY_BYTES` |
| Histogram buckets per aggregation | 10,000 | 400 | `crates/kosha-core/src/histogram.rs` (`MAX_HISTOGRAM_BUCKETS`), no override |
| Server-side embedding dimensions | 4096 | schema rejected (400) | `crates/kosha-core/src/analysis.rs` (`MAX_EMBEDDING_DIMS`), no override |
| Shards per namespace | 256 | 400 | `crates/kosha-core/src/sharding.rs` (`MAX_SHARDS`), no override |
| Rerank `top_n` | 1000 | 400 | `crates/kosha-core/src/lib.rs` (`MAX_RERANK_TOP_N`), no override |
| Rerank `timeout_ms` | 60000 ms | 400 | `crates/kosha-core/src/lib.rs` (`MAX_RERANK_TIMEOUT_MS`), no override |
| `rank_by` window | 10,000 | 400 | `crates/kosha-core/src/rank.rs` (`MAX_RANK_BY_WINDOW`), no override |
| Fuzzy `max_expansions` | 1024 | 400 | `crates/kosha-core/src/fuzzy.rs` (`MAX_MAX_EXPANSIONS`), no override |
| Fuzzy edit distance | 2 | 400 | `crates/kosha-core/src/fuzzy.rs` (`MAX_FUZZY_EDITS`), no override |

Aggregations nest one level deep: a bucket aggregation may hold metric
sub-aggregations only. `terms` and `composite` sizes are not capped.

Namespace and branch names are one path component: not empty, not `.` or
`..`, with no `/`, `\` or control characters, and not of the form
`_shard-NNNN`. There is no length limit.

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

Past the total-hits cap, `total_hits` is reported with `relation: "gte"`.
Longer text values are still fully searchable. They are only left out of
the term filter columns.

### Per-pod capacity

Each pod sheds load rather than queueing without bound. Over these limits a
request gets 503 or 429 with `Retry-After`, and it is safe to retry.

| Limit | Value | When exceeded | Source |
| --- | --- | --- | --- |
| Open HTTP connections | 1024 | 503, `Retry-After: 1`, connection closed | `crates/kosha-server/src/http.rs` (`Config::new`), override `KOSHA_HTTP_MAX_CONNECTIONS` |
| Requests in flight | 256 | 503, `Retry-After: 1` | `crates/kosha-server/src/http.rs` (`Config::new`), override `KOSHA_HTTP_MAX_INFLIGHT` |
| Concurrent searches | 8 | queued (depth 4 × permits by default), then 429 | `crates/kosha-server/src/main.rs` (search admission), override `KOSHA_MAX_CONCURRENT_SEARCHES` |
| Search queue wait | 10,000 ms | 429 | `crates/kosha-server/src/main.rs` (search admission), override `KOSHA_SEARCH_QUEUE_TIMEOUT_MS` |
| Request body read timeout | 300 s | 408 | `crates/kosha-server/src/main.rs` (`io_timeout_secs`), override `KOSHA_HTTP_IO_TIMEOUT_SECS` |
| Unindexed backlog per namespace | 2 GiB | index and replace get 429, `Retry-After: 1`, until flushes catch up | `crates/kosha-server/src/unindexed.rs` (`DEFAULT_MAX_UNINDEXED_BYTES`), override `KOSHA_MAX_UNINDEXED_BYTES` (`0` disables) |

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

| Field | Limits | Refusal today | After #350 |
| --- | --- | --- | --- |
| `qps` | searches per second | 429 `tenant_qps_exceeded` | 429, retryable |
| `concurrent_queries` | searches in flight | 429 `tenant_concurrency_exceeded` | 429, retryable |
| `write_docs_per_sec`, `write_bytes_per_sec` | write rate | 429 `tenant_write_rate_exceeded` | 429, retryable |
| `namespaces` | namespaces held (branches count, shards do not) | 429 `namespace_quota_exceeded`, retry 60 s | 403, not retryable |
| `docs_per_namespace` | committed plus unflushed documents | 429 `doc_quota_exceeded`, retry 60 s | 403, not retryable |
| `vector_dims` | widest vector, supplied or embedded | 429 `vector_dims_exceeded`, retry 60 s | 403, not retryable |
| `max_body_bytes` | write body, under the pod cap | 429 `body_too_large`, retry 60 s | 413, not retryable |

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
- a cited results file is missing, or does not contain the run id or date
  quoted;
- a cited commit is not in git history.

To change a number, change the code or commit a new results file, then
update this page in the same PR.
