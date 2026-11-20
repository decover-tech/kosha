# Configuration reference

Every `KOSHA_*` environment variable the Rust code reads, grouped by area. Defaults are taken
from the read site in the code, not from deployment manifests; the file and function are
listed so you can check them.

To list every variable the code reads:

    grep -rho 'KOSHA_[A-Z_0-9]*' --include='*.rs' crates/ | sort -u

`crates/kosha-server/tests/configuration_doc.rs` fails `cargo test` when a non-bench
`KOSHA_*` variable in `crates/` has no entry here. If you add a variable, add its row in the
same PR.

Conventions used below:

- **Bytes** values are plain integers (`268435456`, not `256MiB`). The table shows the
  default in both forms.
- **Flag (default on)**: only `0`, `false` or `off` turn it off; anything else, including a
  typo, leaves it on.
- **Flag (default off)**: only `1` or `true` (any case) turn it on, unless the row says
  otherwise.
- Unless a row says otherwise, a value that does not parse falls back to the default.
- Variables marked **s3** or **postgres** only take effect in a binary built with that Cargo
  feature (`cargo build -p kosha-server --features s3,postgres`; the Docker image is built
  with both).
- Many values are read once per process and cached. Restart the pod to change them.

## Storage and S3

S3 is enabled by setting `KOSHA_S3_BUCKET` in a binary built with the `s3` feature. Without
it, segments stay on local disk under `KOSHA_DATA_DIR`. Read in `S3Config::from_env`
(`crates/kosha-server/src/s3_storage.rs`).

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_S3_BUCKET` | Bucket holding segments. Setting it enables S3. `kosha-server migrate` requires it | unset (S3 disabled, local-only) | bucket name |
| `KOSHA_S3_PREFIX` | Key prefix inside the bucket, e.g. `kosha/` | empty | string |
| `KOSHA_S3_ENDPOINT` | Custom S3-compatible endpoint (MinIO: `http://localhost:9000`). Falls back to `AWS_ENDPOINT_URL` | unset (real AWS) | URL |
| `KOSHA_S3_FORCE_PATH_STYLE` | Path-style URLs (`endpoint/bucket/key`), needed by MinIO. `1`, `true` or `yes` turn it on; any other value turns it off | on when an endpoint is set, otherwise off | flag |
| `KOSHA_S3_ACCESS_KEY` | Static access key. Falls back to `AWS_ACCESS_KEY_ID`, then the AWS SDK default credential chain | unset | string |
| `KOSHA_S3_SECRET_KEY` | Static secret key. Falls back to `AWS_SECRET_ACCESS_KEY` | unset | string |
| `KOSHA_UPLOAD_CONCURRENCY` | Files of one segment uploaded in parallel (`upload_concurrency`) | `16` | files |
| `KOSHA_DATA_DIR` | Local directory for segments, the WAL and the read-through cache. Also read by `migrate` | `/var/lib/kosha/data` | path |
| `KOSHA_CACHE_DIR` | Accepted for older deployment overlays but **ignored**: segments always live in `KOSHA_DATA_DIR`. A different value only logs a warning | unset | path |

The region comes from `AWS_DEFAULT_REGION`, then `AWS_REGION`, then `us-east-1` when a custom
endpoint is set.

## Cache

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_CACHE_MAX_BYTES` | Size bound for the on-disk (NVMe) segment-file cache; least recently used files are evicted past it (`AppState::new`) | unset (unbounded) | bytes |
| `KOSHA_SEGMENT_CACHE_CAPACITY` | Parsed segments kept in memory across queries | `4096` (`kosha_query::DEFAULT_SEGMENT_CACHE_CAPACITY`) | segments |
| `KOSHA_SEGMENT_CACHE_MAX_BYTES` | Approximate memory budget for those cached parsed segments | `4294967296` (4 GiB) | bytes |
| `KOSHA_RESULT_CACHE_MAX_BYTES` | Whole-response cache for `POST /search`; `0` disables (`result_cache_budget`) | `67108864` (64 MiB) | bytes |
| `KOSHA_POSTINGS_CACHE_MAX_BYTES` | Decoded text-postings LRU, **per open segment**; `0` disables (`kosha-segment`) | `4194304` (4 MiB) | bytes |
| `KOSHA_VECTOR_POSTINGS_CACHE_MAX_BYTES` | Decoded vector-postings LRU, per open segment; `0` disables | `33554432` (32 MiB) | bytes |

## Hydration (fetching segments from S3)

All **s3**. Read in `AppState::new` (`crates/kosha-server/src/main.rs`).

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_HYDRATE_CONCURRENCY` | Parallel S3 GETs within one full-segment hydration | `16` | requests |
| `KOSHA_SCORING_HYDRATE_CONCURRENCY` | Parallel GETs for small files: the scoring set, footer prefetch and page doc stores | `64` | requests |
| `KOSHA_MAX_CONCURRENT_HYDRATIONS` | Independent hydration operations in flight per process | `4` | operations |
| `KOSHA_HYDRATE_BYTE_BUDGET` | Process-wide cap on in-flight hydration bytes; also the pre-GET chunk size | `1073741824` (1 GiB) | bytes |
| `KOSHA_HYDRATE_REQUEST_MAX_BYTES` | Bytes one search may hydrate before it is shed | `2147483648` (2 GiB) | bytes |
| `KOSHA_DOC_STORE_READ_MODE` | How a page's documents are fetched: `ranged` (GETs for just those documents' byte spans in `doc_store.bin` / `doc_vectors.bin`, #245) or `whole` (the whole files, the pre-#245 path) (`doc_spans.rs`) | `ranged` | enum |
| `KOSHA_DOC_SPAN_COALESCE_GAP_BYTES` | Ranged doc-store reads: spans of one file at most this far apart share a GET. The `KOSHA_DOC_SPAN` prefix names this group | `65536` (64 KiB) | bytes |
| `KOSHA_DOC_SPAN_PROMOTE_GETS` | Ranged doc-store reads: promote a segment to whole-file hydration after this many ranged GETs; `0` disables this trigger | `64` | GETs |
| `KOSHA_DOC_SPAN_PROMOTE_BYTES` | ... or after this many ranged bytes; `0` disables this trigger | `16777216` (16 MiB) | bytes |
| `KOSHA_POSTINGS_READ_MODE` | How posting blobs are fetched: `ranged` (each query term's span) or `whole` (whole shards) (`posting_spans.rs`) | `ranged` | enum |
| `KOSHA_POSTING_SPAN_{COALESCE_GAP_BYTES,PROMOTE_GETS,PROMOTE_BYTES}` | The same three settings for ranged posting reads (the `KOSHA_POSTING_SPAN` prefix) | `65536`, `32`, `8388608` (8 MiB) | bytes, GETs, bytes |

Peers share cold segments through hydration leases when the binary has **s3** and
**postgres**, `DATABASE_URL` is set and `POD_IP` is set (the pod's own address, with
`KOSHA_HTTP_PORT`). Peers authenticate with `KOSHA_INTERNAL_API_KEY`.

## Backpressure and memory admission

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_MAX_CONCURRENT_SEARCHES` | Searches that run at once | `8` | searches |
| `KOSHA_SEARCH_QUEUE_DEPTH` | Searches that may wait for a slot; beyond it requests are shed | 4 × `KOSHA_MAX_CONCURRENT_SEARCHES` (`32`) | searches |
| `KOSHA_SEARCH_QUEUE_TIMEOUT_MS` | How long a queued search waits for a slot; `0` waits without a deadline | `10000` | milliseconds |
| `KOSHA_SEGMENT_LIVE_MAX_BYTES` | Limit on live segment memory (cached plus pinned by running searches). Searches wait for memory above it | 2 × `KOSHA_SEGMENT_CACHE_MAX_BYTES` (8 GiB) | bytes |
| `KOSHA_ADMISSION_TIMEOUT_MS` | How long a search waits for live memory before HTTP 429 | `15000` (`kosha_query::DEFAULT_ADMISSION_TIMEOUT`) | milliseconds |
| `KOSHA_MAX_BODY_BYTES` | Largest accepted request body; HTTP 413 above it (`auth::max_body_bytes`) | `67108864` (64 MiB) | bytes |

## Query and relevance

Read once in `crates/kosha-query/src/lib.rs`.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_BM25_STATS` | Server-wide BM25 statistics scope: `namespace` or `segment` (debugging). A request's `bm25_params.stats` overrides it | `namespace` | enum |
| `KOSHA_EXACT_TOTAL_HITS` | Default for `exact_total_hits` when a request omits it | off | flag |
| `KOSHA_TOTAL_HITS_CAP` | Default `total_hits` cap (`relation: gte` above it). `0` is treated as `1` | `10000` | hits |

## kNN / vector search

Read once in `crates/kosha-query/src/lib.rs`.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_KNN_GLOBAL_BUDGET` | Treat `knn.num_candidates` as one probe budget for the whole query. `0`, `false` or `off` gives each segment its own budget (old behavior) | on | flag |
| `KOSHA_KNN_BUDGET_FRACTION` | Lower bound on the probe budget, as a fraction of the namespace's centroids. Must be > 0 | `0.05` | fraction |
| `KOSHA_KNN_BUDGET_MAX_CAP` | Ceiling on that lower bound. A larger explicit `num_candidates` is still honored | `20000` | centroids |
| `KOSHA_KNN_EXACT_THRESHOLD` | A filtered kNN with at most this many filter-passing documents namespace-wide scores every passing document's vector exactly instead of probing (#252). `0` disables exact filtered kNN | `50000` | documents |
| `KOSHA_KNN_EXACT_FRACTION` | A segment where at most this fraction of documents pass the filter is searched exactly too (#252) | `0.01` | fraction |
| `KOSHA_KNN_FILTER_BUDGET_MAX_SCALE` | Most a filtered kNN widens the global probe budget by (`1 / selectivity`, #252) | `8` | multiplier |
| `KOSHA_LATE_INTERACTION_NPROBE` | Token-index postings probed per query token by a late-interaction (`knn.vectors`) search, before candidates are re-scored exactly (#205) | `16` | postings |
| `KOSHA_DEFAULT_CONSISTENCY` | Consistency of a search that does not ask for one: `strong` (also search acknowledged but unindexed writes) or `eventual` (indexed segments only), #255. Unset: `strong`, except on a query pod without `KOSHA_INTERNAL_API_KEY`, which defaults to `eventual` | unset | enum |
| `KOSHA_TAIL_FETCH_TIMEOUT_MS` | Query pods: timeout of the fetch of the ingest pod's unindexed tail for a strong search (#255) | `10000` | ms |
| `KOSHA_MAX_UNINDEXED_BYTES` | Per-namespace cap on acknowledged but unindexed writes; past it `/index` and `/replace` return 429 with `retry-after` (#255). `0` disables | `2147483648` (2 GiB) | bytes |
| `KOSHA_MAX_UNINDEXED_DOCS` | Per-namespace cap on unindexed documents, as above (#255) | unset | documents |

## Warmup

Query pods warm the listed namespaces before `/readyz` passes. Needs **s3**. Read in
`AppState::new`.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_WARMUP_NAMESPACES` | Comma-separated namespaces to prefetch at boot. The manifest refresher also prefetches a new version's scoring set for them before swapping it in | empty (no warmup, ready at once) | list |
| `KOSHA_WARMUP_POSTING_BLOBS` | Also prefetch posting blobs, not only the scoring set | on | flag (default on) |
| `KOSHA_WARMUP_HYDRATE_SEGMENT_CACHE` | Also open the segments into the in-memory segment cache | on | flag (default on) |
| `KOSHA_WARMUP_USAGE` | Usage-driven warming (#213): track reads, persist the warm set and warm it at boot. Off leaves only `KOSHA_WARMUP_NAMESPACES` (`warm.rs`) | on | flag (default on) |
| `KOSHA_WARMUP_BUDGET_BYTES` | Ceiling on the bytes warming keeps local (the boot pass, then boot plus every re-warm). Accepts a `K`/`M`/`G`/`T` suffix | unset (unlimited) | bytes |
| `KOSHA_WARMSET_TOP_NAMESPACES` | Hottest namespaces kept in the warm set | `8` | namespaces |
| `KOSHA_WARMSET_TOP_SEGMENTS` | Hottest segments kept per warm namespace | `512` | segments |
| `KOSHA_WARMSET_MIN_HITS` | A namespace below this decayed read count is not warmed | `1.0` | reads |
| `KOSHA_WARMSET_PERSIST_SECS` | How often the warm set is persisted; `0` disables | `60` | seconds |
| `KOSHA_REWARM_INTERVAL_MS` | How often a query pod polls its hot namespaces' manifest versions to re-warm a new version; `0` disables | `5000` | milliseconds |
| `KOSHA_ACCESS_HALF_LIFE_SECS` | Half-life of a read in the access counters that rank the warm set | `600` | seconds |
| `KOSHA_ACCESS_MAX_SEGMENTS` | Most segments the access tracker follows; past it the coldest tenth is dropped | `50000` | segments |

## Control plane

The control plane is in memory unless `DATABASE_URL` is set in a binary built with
**postgres**.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_POSTGRES_FALLBACK_ON_ERROR` | When `DATABASE_URL` is set but Postgres is unreachable at boot, fall back to the in-memory store instead of exiting. Emergency use only | off (exit) | flag |
| `KOSHA_MANIFEST_REFRESH_MS` | How often query pods poll for new manifest versions; `0` disables the refresher and pods check before every search (`manifest_follow.rs`) | `500` | milliseconds |
| `KOSHA_API_KEY_REFRESH_SECS` | How often API keys are reloaded from Postgres (`auth.rs`) | `30` | seconds |

## HTTP server and auth

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_HTTP_PORT` | HTTP listen port (binds `0.0.0.0`) | `8080` | port |
| `KOSHA_HTTP_IO_TIMEOUT_SECS` | Per-syscall socket read/write timeout. Bounds stalled clients, not how long a request may take | `300` | seconds |
| `KOSHA_AUTH` | `disabled` (or `off`, `false`, `0`, `none`) turns authentication off; every request becomes tenant `dev`. Local development only | enabled | flag |
| `KOSHA_API_KEY` | One tenant API key, mapped to tenant `default` | unset | string |
| `KOSHA_API_KEYS` | Comma-separated `key=tenant` pairs | unset | list |
| `KOSHA_ADMIN_API_KEY` | Key for `/v1/admin/*` and global `/stats`. Unset disables those routes | unset | string |
| `KOSHA_INTERNAL_API_KEY` | Key peers use for `GET /internal/segment/...` hydration. Unset disables peer-file access | unset | string |
| `KOSHA_LEGACY_ROUTES` | `off` (or `disabled`, `false`, `0`, `none`) makes the unversioned Phase 1 routes return 410 | on | flag |

The server refuses to start when no credential is configured (no env key, no admin or internal
key, no Postgres keys) unless `KOSHA_AUTH=disabled`.

## Roles

There is no role switch. A pod forwards writes when `KOSHA_INGEST_HOST` is set.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_INGEST_HOST` | Set on query pods: every mutating route is forwarded to this base URL, and the idle-flush timer and compaction scheduler do not run. Unset = this pod accepts writes | unset (writer) | URL |
| `KOSHA_INGEST_PROXY_TIMEOUT_SECS` | Timeout for a forwarded write | `300` | seconds |
| `KOSHA_ROLE` | Label printed in the startup log line. Has no other effect | `query` | string |

## Write path and flush

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_FLUSH_THRESHOLD` | Buffered documents per namespace that trigger an auto-flush. Must be > 0 | `1000` | documents |
| `KOSHA_FLUSH_MAX_AGE_MS` | Longest pending documents wait before an idle flush; `0` disables the timer | `1000` | milliseconds |
| `KOSHA_MAX_CONCURRENT_FLUSH_WRITERS` | Segment writers one flush round may run for one namespace; `1` means a single writer | `4` | writers |
| `KOSHA_MIN_DOCS_PER_FLUSH_WRITER` | Backlog per writer before a flush splits across several writers | value of `KOSHA_FLUSH_THRESHOLD` | documents |
| `KOSHA_SEGMENT_BUILD_MEMORY_BYTES` | In-memory postings per segment build before spilling sorted runs to disk (`kosha-segment`) | `268435456` (256 MiB) | bytes |

## WAL

Read in `GroupCommitConfig::from_env` (`crates/kosha-write/src/wal.rs`).

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_WAL_GROUP_COMMIT_MS` | How long a commit waits for more records once concurrent writers are seen; `0` commits when the previous fsync ends. A single writer never waits | `2` | milliseconds |
| `KOSHA_WAL_GROUP_COMMIT_BYTES` | Size at which a commit group is cut | `4194304` (4 MiB) | bytes |
| `KOSHA_WAL_MODE` | `local` keeps the WAL on the ingest pod's disk; `s3` writes every group-commit file to S3 (create-if-absent) before acknowledging it (#236). `s3` needs **s3** and `KOSHA_S3_BUCKET` (`main.rs`) | `local` | enum |
| `KOSHA_WAL_WRITER_ID` | `KOSHA_WAL_MODE=s3`: the writer stream, stored under `_wal/{id}/`. Give each ingest writer its own | `default` | string |
| `KOSHA_WAL_S3_TIMEOUT_MS` | `KOSHA_WAL_MODE=s3`: deadline for each S3 WAL call (`s3_wal.rs`) | `10000` | milliseconds |

## Compaction

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_COMPACTION_INTERVAL_MS` | Background compaction scheduler period on writer pods; `0` disables it (`compaction_sched.rs`). Benchmarks and the conformance suite set `0` | `30000` | milliseconds |
| `KOSHA_COMPACTION_MAX_CONCURRENT` | Compaction passes at once, including API jobs. Must be > 0 | `1` | passes |
| `KOSHA_COMPACT_MAX_SEGMENT_BYTES` | Largest merged segment; `0` removes the cap. A request's own value overrides it (`compact_options`) | `5368709120` (5 GiB) | bytes |

## Segment garbage collection

Read in `GcSettings::from_env` (`crates/kosha-server/src/segment_gc.rs`). Durations take plain
seconds or a unit (`90s`, `5m`, `1h`).

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_GC_GRACE` | How long a retired segment or deletion generation stays in the object store after the publish that dropped it, before `POST /v1/admin/gc` may delete it | `1h` | duration |
| `KOSHA_GC_ORPHAN_GRACE` | How old an object no manifest ever referenced must be before GC deletes it | `24h` | duration |
| `KOSHA_LOCAL_GC_GRACE` | How long a retired segment's local files stay after the publish that dropped it (and after every search that could read them ends) | `5m` | duration |
| `KOSHA_LOCAL_GC_INTERVAL_MS` | How often each pod reclaims retired local segments; `0` disables | `60000` | milliseconds |

## Segment format (rollout and rollback gates)

Writers emit the newest format by default. Pin an older version during a rolling deploy, or
before rolling back, until every reader understands the newer one. Read once in
`crates/kosha-segment/src/lib.rs`.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_POSTINGS_WRITE_VERSION` | Inverted-postings version written: `4` or `5` | `5` | version |
| `KOSHA_VECTOR_WRITE_VERSION` | Vector-index version written: `1` (flat), `2`, `3` or `4` (posting-based) | `4` | version |
| `KOSHA_VECTOR_STORE` | Where vector field values are stored: `sidecar` (`doc_vectors.bin`) or `inline` (in `doc_store.bin`). Set `inline` before rolling back past #243 | `sidecar` | enum |
| `KOSHA_VECTOR_QUANTIZATION` | How new segments store vector posting entries: `f32` or `i8` (scalar-quantized, vector index version 4, #210). Turn `i8` on only once every query pod reads version 4; turn it off and compact before rolling back past it | `f32` | enum |
| `KOSHA_VECTOR_QUANTIZATION_FIELDS` | Per-field overrides of `KOSHA_VECTOR_QUANTIZATION`, e.g. `body_emb:i8,title_emb:f32` | empty | list |
| `KOSHA_VECTOR_RESCORE_FACTOR` | An i8 column rescores its best `k * factor` probe candidates with the f32 vectors in `doc_vectors.bin` (#210); `0` disables | `4` | multiplier |
| `KOSHA_VECTOR_COMPACTION` | How compaction builds merged vector indexes: `lire` seeds each column from its largest input's postings and repairs them with SPFresh's LIRE split / merge / reassign; `rebuild` re-clusters every vector (#253) | `lire` | enum |
| `KOSHA_TEXT_FILTER_IGNORE_ABOVE` | Longest `Text` value copied into filter columns, like Elasticsearch's `ignore_above`; `off` copies every value | `256` | characters |
| `KOSHA_FIELD_INDEX` | Per-field inverted indexes (#249); `off` writes segments exactly as before. Set it and compact before rolling back past #249 | on | flag (default on) |
| `KOSHA_FILTERS_WRITE_VERSION` | Filter index written: `1` (`filters.bin`) or `2` (columnar `filters.toc` + `filters-NNN.col`, #244) (`filter_columns.rs`) | `1` | version |
| `KOSHA_POSTINGS_RAW_BLOBS` | Leave `postings-*.bin` shards uncompressed so a cold query can fetch term spans by range (#245); `0` keeps compressing them | on | flag |

## Reranking

Read in `RerankRegistry::from_env` (`crates/kosha-server/src/rerank.rs`). For each provider name
`N` (upper-cased, non-alphanumerics as `_`), `KOSHA_RERANK_N_URL`, `KOSHA_RERANK_N_API_KEY` and
`KOSHA_RERANK_N_MODEL` configure it: for example `KOSHA_RERANK_COHERE_API_KEY`,
`KOSHA_RERANK_VOYAGE_URL` / `KOSHA_RERANK_VOYAGE_API_KEY`, or `KOSHA_RERANK_MY_MODEL_URL` for a
provider named `my-model`. Keys are never logged or returned.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_RERANK_PROVIDERS` | Provider names a request's `rerank.provider` may use | `cohere` | list |
| `KOSHA_RERANK_N_URL` | Provider endpoint (Cohere `POST /v2/rerank` shape). Required for any provider other than `cohere` | `cohere`: `https://api.cohere.com/v2/rerank` | URL |
| `KOSHA_RERANK_N_API_KEY` | Sent as `Authorization: Bearer ...`. `cohere` also reads `COHERE_API_KEY`, and is registered only when it has a key | unset | string |
| `KOSHA_RERANK_N_MODEL` | Model used when a request names none | `cohere`: `rerank-v3.5` | string |

## CLI and Python client

Read by `kosha` (`crates/kosha-cli`). The Python client (`clients/python`) reads
`KOSHA_HOST` and `KOSHA_API_KEY` too.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_HOST` | Server base URL (same as `--host`) | profile host, then `http://localhost:8080` | URL |
| `KOSHA_API_KEY` | API key (same as `--api-key`) | profile key | string |
| `KOSHA_PROFILE` | Profile from the config file (same as `--profile`) | the file's `default_profile` | name |
| `KOSHA_CONFIG` | Config file path (same as `--config`) | `~/.kosha/config.toml` | path |
| `KOSHA_STAGING_API_KEY` | Not read directly. A profile can name any variable to read its key from (`api_key_env`); this is the name used in the CLI README example and tests | n/a | string |

## Tests only

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_TEST_DATABASE_URL` | Disposable Postgres for the ignored `postgres_cas` test in `kosha-control` | unset (test ignored) | URL |
| `KOSHA_STATS_BENCH_SEGMENTS` | Segment count for an `#[ignore]`d stats timing test in `kosha-query` | test default | segments |
| `KOSHA_RRF_BENCH_BACKGROUND` | Background document count for an `#[ignore]`d RRF timing test in `kosha-query` | test default | documents |
| `KOSHA_INTERLEAVING_SCHEDULES` | Randomized schedules per scenario in `kosha-write`'s `interleavings` test (needs `--features failpoints`; see `docs/testing.md`). The nightly fuzz workflow runs 2500 | `250` | schedules |

## Benchmark harness (`KOSHA_BENCH_*`)

These are read only by the benches under `crates/*/benches/` and never by the server. Each
bench's header comment documents its own knobs. The common ones:

| Variable | Used for |
| --- | --- |
| `KOSHA_BENCH_JSON` | Write machine-readable results (used by the `bench-compare` CI job) |
| `KOSHA_BENCH_DOCS`, `KOSHA_BENCH_SEGS`, `KOSHA_BENCH_VOCAB`, `KOSHA_BENCH_BODY_WORDS`, `KOSHA_BENCH_CORPUS`, `KOSHA_BENCH_CORPUS_MB`, `KOSHA_BENCH_SEGMENT_MIB` | Corpus shape |
| `KOSHA_BENCH_DIM`, `KOSHA_BENCH_K`, `KOSHA_BENCH_TOPK` | Vector and top-k query shape |
| `KOSHA_BENCH_COLD_ITERS`, `KOSHA_BENCH_WARM_ITERS`, `KOSHA_BENCH_DURATION_SECS`, `KOSHA_BENCH_THREADS`, `KOSHA_BENCH_TIMEOUT_MS` | Run length and concurrency |
| `KOSHA_BENCH_S3_MS` | Simulated S3 latency |
| `KOSHA_BENCH_BATCH_SIZE`, `KOSHA_BENCH_BACKLOG_DOCS`, `KOSHA_BENCH_FLUSH_THRESHOLD`, `KOSHA_BENCH_MAX_CONCURRENT_FLUSH_WRITERS`, `KOSHA_BENCH_MIN_DOCS_PER_FLUSH_WRITER`, `KOSHA_BENCH_WAL_APPENDS`, `KOSHA_BENCH_WAL_ENABLED` | Write-path benches (`kosha-write`) |

## Other environment variables

These do not start with `KOSHA_` but change server behavior:

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | Postgres control plane and API-key registry (**postgres**). Required by `kosha-server migrate` |
| `POD_IP` | This pod's IP, used as its peer-hydration address (**s3** + **postgres**) |
| `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `AWS_REGION` | Fallbacks for the `KOSHA_S3_*` settings above |
