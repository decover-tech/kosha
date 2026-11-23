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

### Encryption at rest (S3 server-side encryption)

Kosha can request server-side encryption (SSE) on every object it writes to S3 (#343):
segment files, `_COMMIT` markers, deletion vectors, `migrate` uploads and S3 WAL group
commits, including the conditional `If-None-Match` puts. The settings are read in
`SseConfig::from_lookup` (`crates/kosha-server/src/s3_storage.rs`) and validated at startup.
The node **fails closed**: an unknown mode, a malformed tenant map, or a KMS-only setting
without `KOSHA_S3_SSE=aws:kms` logs `FATAL: invalid S3 encryption config: ...` and exits,
so a typo never falls back to weaker encryption. The startup log prints the mode, for example
`S3 server-side encryption: mode=aws:kms default_key=configured bucket_key=on tenant_keys=2`.
Key ids are never logged.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_S3_SSE` | `none` sends no SSE header, so the bucket's default encryption applies. `AES256` requests SSE-S3. `aws:kms` requests SSE-KMS. Case-insensitive | `none` | enum |
| `KOSHA_S3_SSE_KMS_KEY_ID` | KMS key (key id, key ARN or `alias/...`) for objects with no per-tenant key. Requires `aws:kms`. Unset with `aws:kms` means the AWS-managed `aws/s3` key | unset | key id |
| `KOSHA_S3_SSE_BUCKET_KEY` | `true` or `false`: S3 Bucket Keys, which cut KMS request cost. Requires `aws:kms`. Unset sends no header, so the bucket's setting applies | unset | flag |
| `KOSHA_S3_TENANT_KMS_KEYS` | Per-tenant KMS keys (BYOK) as `tenant=key-id,tenant2=key-id2`. Requires `aws:kms`. A tenant not listed uses `KOSHA_S3_SSE_KMS_KEY_ID` | unset | map |
| `KOSHA_S3_TENANT_KMS_KEYS_FILE` | The same map as a JSON file, `{"tenant": "key-id"}` (for example a mounted Secret). Set at most one of this and `KOSHA_S3_TENANT_KMS_KEYS` | unset | path |

**Which key an object uses.** Every object under a namespace is
`{namespace}/{segment}/{file}`, and a tenant-scoped namespace is `{tenant}/{name}` (branches
and shards stay under the same tenant). The tenant is the namespace's first component, and its
key comes from the tenant map, falling back to the default key. These objects have no tenant
and always use the default key:

- objects of a bare namespace, such as `migrate` without `--namespace`;
- **WAL objects** (`{prefix}/_wal/{writer_id}/...`). A WAL stream belongs to one ingest writer,
  and one group commit holds the records of every tenant that wrote in that window, so it cannot
  carry a per-tenant key. A tenant that requires its own key for all of its data at rest should
  keep this in mind when `KOSHA_WAL_MODE=s3`: WAL objects are short-lived (reclaimed once a
  checkpoint covers them), but they are encrypted with the default key.

**Reads need no change.** S3 decrypts SSE-S3 and SSE-KMS objects transparently on `GET`, but
with SSE-KMS the pod's role needs `kms:Decrypt` (reads) and `kms:GenerateDataKey` (writes,
including multipart uploads) on every key in use. See the IAM policy in `k8s/README.md`.

**Turning SSE on** affects new objects only. Existing objects keep the encryption they were
written with and stay readable (subject to the KMS permissions above).

#### Key rotation

- **KMS automatic rotation** (recommended): enable rotation on the key in KMS. KMS keeps the
  old key material for decryption, so nothing is re-encrypted and no Kosha config changes.
- **Switching to a different key** (a new `KOSHA_S3_SSE_KMS_KEY_ID` or a new tenant entry):
  roll out the new value. New objects use the new key at once. Old objects keep the old key
  and stay readable as long as the role can still `kms:Decrypt` with it, so keep the old key
  enabled and in the IAM policy. Compaction rewrites segments into new objects, so the old
  key's share of the data shrinks over time. The tenant map (including
  `KOSHA_S3_TENANT_KMS_KEYS_FILE`) is read once at startup, so editing a mounted file takes
  effect when the pods restart. The test
  `rotating_a_tenant_key_applies_to_new_writes_and_old_objects_stay_readable` in
  `s3_storage.rs` checks this: after a rotation, PUTs, multipart uploads and copies carry the
  new key, and reads send no key at all.
- **Hard cutover** (the old key must stop being used): after the rollout, re-encrypt the
  remaining objects in place, then disable the old key once nothing references it:

  ```sh
  aws s3 cp s3://$BUCKET/$PREFIX/acme/ s3://$BUCKET/$PREFIX/acme/ --recursive \
    --sse aws:kms --sse-kms-key-id "$NEW_KEY_ARN"
  ```

  The copy rewrites each object with the same bytes, so `_COMMIT` checksums stay valid. Run it
  per tenant prefix (or for the whole prefix with the default key), and check with
  `aws s3api head-object` that `SSEKMSKeyId` is the new key before disabling the old one.

#### Local cache volumes

SSE covers S3 only. Each pod also keeps segments, the read-through cache and (with
`KOSHA_WAL_MODE=local`) the WAL on local disk under `KOSHA_DATA_DIR`, so that disk must be
encrypted too. This is a deployment requirement, not something Kosha enforces:

- **EBS** (root volumes, `emptyDir` on the node's root disk, PVCs): turn on EBS encryption by
  default for the account and region, or set `encrypted: "true"` (and optionally `kmsKeyId`)
  on the StorageClass, and `blockDeviceMappings[].ebs.encrypted: true` in the Karpenter
  `EC2NodeClass`.
- **NVMe instance store** (the ingest pool): the data on NVMe instance-store volumes of current
  (Nitro) instance types is encrypted by the hardware with a per-instance key that is
  destroyed when the instance stops. Use only Nitro instance types in that `NodePool`.
  Software encryption (for example LUKS in the node user-data) is an option where policy
  requires a customer-managed key.

See `k8s/README.md` ("Encryption at rest") for an IAM policy and a StorageClass example.

## Cache

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_CACHE_MAX_BYTES` | Size bound for the on-disk (NVMe) segment-file cache; past it, files are evicted least recently used first from the tenant furthest over its share (`AppState::new`) | unset (unbounded) | bytes |
| `KOSHA_CACHE_TENANT_MIN_BYTES` | Guaranteed floor per active tenant in the byte-bounded process-wide caches: NVMe, parsed segments and results (#342, `kosha_core::tenancy`). The parsed-segment cache counts shares and this floor in entries (the floor over the average entry size its budget allows) when its entry bound, not its byte bound, is the one exceeded. The entry-bounded corpus-statistics and score-ceiling caches split their entries by the same weights with a floor of one entry. Decoded postings are cached per open segment, so they leave memory with their segment. Each cache splits its budget across the tenants holding bytes in it by weight (the tenant's plan `weight` from `KOSHA_TENANT_LIMITS_FILE`, default 1), evicts from the tenant furthest over its share, and never takes a tenant below this floor while another tenant can give. Capped at an equal split of the cache's budget, so the floors always fit. Bare pre-tenant namespace ids share one `_legacy` tenant | `67108864` (64 MiB) | bytes |
| `KOSHA_CACHE_POLICY` | Research (#389), experimental: eviction policy under `KOSHA_CACHE_MAX_BYTES`. `legacy` keeps #342's per-tenant least-recently-used order. `sieve` or `s3fifo` use per-search read hints so files read once do not displace re-read ones. The policy runs per tenant: #342 still picks which tenant gives up bytes (`KOSHA_CACHE_TENANT_MIN_BYTES`, fair shares), and that tenant's own SIEVE/S3-FIFO state picks the file, so one tenant's evictions never touch another tenant's policy state. No effect without `KOSHA_CACHE_MAX_BYTES` (`cache_reads.rs`) | unset (`legacy`) | `legacy` / `sieve` / `s3fifo` |
| `KOSHA_CACHE_ACCOUNTING` | Research (#389). Superseded by #342: the cache now counts each tracked file once on every path, so `exact` changes nothing. Accepted so existing configs and bench inputs keep working | unset (`legacy`) | `legacy` / `exact` |
| `KOSHA_CACHE_TRACE_PATH` | Research (#389): append one JSON line per successful search (segments, classes, posting shards, page segments read) for `scripts/bench/cache_sim/cache_sim.py`. Keep it outside `KOSHA_DATA_DIR` | unset (off) | file path |
| `KOSHA_SEGMENT_CACHE_CAPACITY` | Parsed segments kept in memory across queries | `4096` (`kosha_query::DEFAULT_SEGMENT_CACHE_CAPACITY`) | segments |
| `KOSHA_SEGMENT_CACHE_MAX_BYTES` | Approximate memory budget for those cached parsed segments | `4294967296` (4 GiB) | bytes |
| `KOSHA_RESULT_CACHE_MAX_BYTES` | Whole-response cache for `POST /search`; `0` disables (`result_cache_budget`) | `67108864` (64 MiB) | bytes |
| `KOSHA_POSTINGS_CACHE_MAX_BYTES` | Decoded text-postings LRU, **per open segment**; `0` disables (`kosha-segment`) | `4194304` (4 MiB) | bytes |
| `KOSHA_VECTOR_POSTINGS_CACHE_MAX_BYTES` | Decoded vector-postings LRU, **one budget for the whole process** (every open segment's postings in one LRU); also counted in `KOSHA_SEGMENT_LIVE_MAX_BYTES`. Keep it a stated share of that watermark; `0` disables | `536870912` (512 MiB) | bytes |
| `KOSHA_SPAN_LISTING_CACHE` | Cache each `*.spans` directory's listing in memory, valid while the directory's modification time is unchanged (a span is added by a rename and removed by an unlink, and both change it). Saves a `read_dir` and a stat per span on every ranged local read of a segment warmed by ranged fetches. `0`, `off` or `false` lists the directory every time | `1` | bool |

## Hydration (fetching segments from S3)

All **s3**. Read in `AppState::new` (`crates/kosha-server/src/main.rs`).

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_HYDRATE_CONCURRENCY` | Parallel S3 GETs within one full-segment hydration | `16` | requests |
| `KOSHA_SCORING_HYDRATE_CONCURRENCY` | Parallel GETs for small files: the scoring set, footer prefetch and page doc stores | `64` | requests |
| `KOSHA_MAX_CONCURRENT_HYDRATIONS` | Independent hydration operations in flight per process | `4` | operations |
| `KOSHA_HYDRATE_BYTE_BUDGET` | Process-wide cap on in-flight hydration bytes; also the pre-GET chunk size | `1073741824` (1 GiB) | bytes |
| `KOSHA_HYDRATE_TENANT_MAX_INFLIGHT_BYTES` | Cap on one tenant's in-flight hydration bytes, so one tenant's cold start cannot hold all of the pod's S3 bandwidth (#342, `hydration_budget.rs`). Work-conserving: the cap binds only while another tenant holds or waits for hydration budget, so a lone tenant (or a pod serving only pre-#262 bare namespaces) may use the whole budget. A single object larger than the cap is admitted when it fits the global budget and no other tenant is waiting | half of `KOSHA_HYDRATE_BYTE_BUDGET` | bytes |
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
| `KOSHA_SEARCH_QUEUE_DEPTH` | Searches that may wait for a slot; beyond it requests are shed. A full queue is shared fairly: an arrival from a tenant holding less of it takes the place of the newest waiter of the tenant holding the most, which is shed instead | 4 × `KOSHA_MAX_CONCURRENT_SEARCHES` (`32`) | searches |
| `KOSHA_SEARCH_QUEUE_TIMEOUT_MS` | How long a queued search waits for a slot; `0` waits without a deadline | `10000` | milliseconds |
| `KOSHA_SEARCH_QUEUE_DEPTH_PER_TENANT` | Searches one tenant may have waiting for a slot; beyond it that tenant's searches are shed while other tenants still queue. Waiters are served round robin across tenants (#341, `fair_admission.rs`). `0`: only `KOSHA_SEARCH_QUEUE_DEPTH` applies | `0` | searches |
| `KOSHA_SEGMENT_LIVE_MAX_BYTES` | Limit on live segment memory (cached plus pinned by running searches). Searches wait for memory above it | 2 × `KOSHA_SEGMENT_CACHE_MAX_BYTES` (8 GiB) | bytes |
| `KOSHA_ADMISSION_TIMEOUT_MS` | How long a search waits for live memory before HTTP 429 | `15000` (`kosha_query::DEFAULT_ADMISSION_TIMEOUT`) | milliseconds |
| `KOSHA_ADMISSION_TENANT_SHARES` | `on` (or `true`, `1`) splits `KOSHA_SEGMENT_LIVE_MAX_BYTES` between tenants while they compete for it (#342, `kosha_query::MemoryLedger`). Live and reserved bytes are counted per tenant either way. When on, a tenant that has a search running and already holds more than its weighted share (the weights of `KOSHA_TENANT_LIMITS_FILE`, split over the tenants holding or waiting for live memory) is not admitted again while another tenant's search waits for memory within that tenant's own share, so the memory its searches free goes to that waiter first. A waiter asking for more than its share (for one, a search larger than the whole limit) holds nobody back. A tenant held back first evicts its own idle cached segments, and goes ahead if that brings it back under its share; otherwise its search waits and is shed with HTTP 429 after `KOSHA_ADMISSION_TIMEOUT_MS`. Work-conserving: it binds only while another tenant waits, so a lone tenant (or a pod serving only pre-#262 bare namespaces) keeps the whole limit, and a tenant with no search running is never held back. Any other value stops startup | `off` | `on` / `off` |
| `KOSHA_MAX_BODY_BYTES` | Largest accepted request body; HTTP 413 above it (`auth::max_body_bytes`) | `67108864` (64 MiB) | bytes |

A search the pod sheds gets HTTP 429 with `Retry-After: 1` and a JSON body
`{"error", "reason", "retry_after_ms"}`. `reason` is `search_queue_full` (the whole queue is
at `KOSHA_SEARCH_QUEUE_DEPTH`), `search_queue_timeout` (no slot within
`KOSHA_SEARCH_QUEUE_TIMEOUT_MS`), `tenant_queue_full` (the tenant is at
`KOSHA_SEARCH_QUEUE_DEPTH_PER_TENANT`), `search_hydration_limit` (the query needs more
segment data than one request may hydrate, `KOSHA_HYDRATE_REQUEST_MAX_BYTES`) or
`search_overloaded` (no live memory within `KOSHA_ADMISSION_TIMEOUT_MS`, or another memory or
hydration budget refused the search). `tenant_queue_full` is a tenant rejection: its body
also has `limit`, and it is counted in `kosha_tenant_rejections_total` (see Tenant limits
below).

One tenant cannot use the queue bound to get another tenant's searches shed. When the queue
is at `KOSHA_SEARCH_QUEUE_DEPTH` and a search arrives from a tenant with at least two fewer
searches waiting than the tenant with the most, that tenant's newest waiting search is shed
(`search_queue_full`) and the arrival queues in its place. An arrival is itself shed with
`search_queue_full` only when its tenant already holds as large a share of the queue as any
other. This needs no configuration; `KOSHA_SEARCH_QUEUE_DEPTH_PER_TENANT` additionally caps
each tenant before the queue is full.

## Tenant limits

Per-tenant quotas and rate limits (#341, `tenant_limits.rs`). Without
`KOSHA_TENANT_LIMITS_FILE` nothing is limited. Limits are enforced per pod: query limits on the
pod that serves the search, write limits on the pod that executes the write (the ingest pod; a
query pod forwards writes first). Operator traffic (the admin and internal service credentials)
is never limited. A request over a limit gets HTTP 429 with `Retry-After` and a JSON body
`{"error", "reason", "limit", "retry_after_ms"}`, where `reason` is one of
`tenant_qps_exceeded`, `tenant_concurrency_exceeded`, `tenant_write_rate_exceeded`,
`namespace_quota_exceeded`, `doc_quota_exceeded`, `vector_dims_exceeded` or `body_too_large`.
Rate limits give the time until the token bucket holds the request; quotas give 60 seconds.
Each refusal is counted in `kosha_tenant_rejections_total` (see `metrics.md`). A query pod
relays a write refused by the ingest pod unchanged: the same status, `Retry-After` and body.
A search shed by `KOSHA_SEARCH_QUEUE_DEPTH_PER_TENANT` is answered and counted the same way,
with `reason` `tenant_queue_full`, whether or not a limits file is set.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_TENANT_LIMITS_FILE` | JSON file of plans and per-tenant overrides (format below). A file that cannot be read or parsed at startup stops the pod | unset (no limits) | path |
| `KOSHA_TENANT_DEFAULT_PLAN` | Plan for tenants without one of their own; overrides the file's `default_plan`. Requires `KOSHA_TENANT_LIMITS_FILE` | file's `default_plan` | plan name |
| `KOSHA_TENANT_LIMITS_RELOAD_SECS` | How often the file is re-read. A reload that fails keeps the previous limits. `0` never reloads | `30` | seconds |

The file:

```json
{
  "default_plan": "standard",
  "plans": {
    "standard": {"qps": 50, "concurrent_queries": 8, "write_docs_per_sec": 2000,
                 "write_bytes_per_sec": 16777216, "namespaces": 100,
                 "docs_per_namespace": 10000000, "vector_dims": 4096,
                 "max_body_bytes": 33554432, "weight": 1}
  },
  "tenants": {"acme": {"plan": "standard", "qps": 200, "weight": 4}}
}
```

A tenant's limits are its plan's (its own `plan`, else the default plan) with its `tenants`
fields laid over them. A tenant with no plan and no entry is unlimited, and so is any field
left out. An unknown key anywhere in the file, including in a `tenants` entry, is an error: at
startup it stops the pod, and a reload keeps the previous limits.

| Field | Limits | Checked |
| --- | --- | --- |
| `qps` | Searches per second; the bucket holds one second's worth | each search, after authentication |
| `concurrent_queries` | Searches in flight at once | each search |
| `write_docs_per_sec` | Documents per second in index and replace requests. A batch bigger than one second's worth is admitted from a full bucket and paid for afterwards | each index or replace |
| `write_bytes_per_sec` | Request body bytes per second on write routes | each write request |
| `max_body_bytes` | Largest write request body, under the pod-wide `KOSHA_MAX_BODY_BYTES` | each write request |
| `namespaces` | Namespaces the tenant holds (shards not counted; a branch is a namespace and counts) | namespace creation, branch creation, and a write that would auto-create one |
| `docs_per_namespace` | Documents in a namespace: committed and not deleted plus not yet flushed. An unflushed upsert of an existing id counts twice until flushed | each index or replace |
| `vector_dims` | Widest vector a document may carry, supplied or produced by native embedding | each index or replace |
| `weight` | Permits per round-robin turn in the search queue (default 1) | search admission |

Limits stored in the control plane, cluster-wide rate limits across replicas and per-namespace
storage quotas are not implemented yet (Phase H).

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
| `KOSHA_KNN_GLOBAL_RESCORE` | Experimental (#381). `1`, `true` or `on`: an unfiltered kNN collects every segment's candidates with their error bounds, computes the `k`-th lower bound across the namespace, and rescores only the quantized candidates whose upper bound reaches it, reading f32 vectors from `doc_vectors.bin` locally or by ranged read. Candidates with no readable exact vector keep their estimate and their segment is counted in `knn_degraded_segments` (the default path returns them silently). Filtered, `limit_per` and late-interaction kNN keep the default path. With it on, a kNN also hydrates the `doc_vectors.bin` header of every segment whose queried column is quantized (with the scoring files, by range), and a failed ranged read of `doc_vectors.bin` is never retried as the whole file | off | flag |
| `KOSHA_KNN_GLOBAL_RESCORE_MAX` | Experimental. With `KOSHA_KNN_GLOBAL_RESCORE`, the most quantized candidates one query reads exact vectors for. Without it every candidate the error bounds cannot rule out is rescored, which can be a large share of all candidates when estimates are close together. With it only the best by estimate are rescored and the rest dropped, so the result is no longer guaranteed by the bounds. `0` or unset: no cap | unset | count |
| `KOSHA_KNN_RESCORE_STATS` | Experimental (#381), observability only. `1`, `true` or `on`: a kNN counts the segments whose quantized column the default per-segment path probed, and how many of them returned unrescored estimates because `doc_vectors.bin` was not whole and local, and appends `knn_seg_quantized=` / `knn_seg_unrescored=` to the `search timing:` line (printed only for slow searches unless `KOSHA_SLOW_SEARCH_LOG_MS=0`). Results are unchanged. Implied by `KOSHA_KNN_GLOBAL_RESCORE` | off | flag |
| `KOSHA_KNN_I8_BOUND` | Error bound of an i8 vector entry under `KOSHA_KNN_GLOBAL_RESCORE` (#381; read in `kosha-segment`): `strict` (`scale / 2 * ‖q‖₁`, guaranteed) or `probabilistic` (`1.9 * scale * ‖q‖₂ / √12`, not guaranteed, much tighter at high dimension) | `strict` | mode |
| `KOSHA_KNN_FILTER_BUDGET_MAX_SCALE` | Most a filtered kNN widens the global probe budget by (`1 / selectivity`, #252) | `8` | multiplier |
| `KOSHA_KNN_RELATIVE_CUTOFF_EPS` | Experimental (#384). Unfiltered kNN probes a posting only if its rank score is at most `(1 + eps)` times the best posting's score (SPANN's relative cutoff), never fewer than `KOSHA_KNN_RELATIVE_MIN_PROBES` postings and never more than the fixed budget. While on, each such search reports which rule set its cutoff (`timings.knn_cutoff_rule` and `knn_cutoff_rule=` on the `search timing:` line, which prints only for slow searches unless `KOSHA_SLOW_SEARCH_LOG_MS=0`: `budget`, `eps`, `floor` or `zero_best`). Unset, empty, negative or unparseable: off | unset (off) | ratio |
| `KOSHA_KNN_RELATIVE_MIN_PROBES` | Experimental (#384). Fewest postings the relative cutoff may leave a query with | the query's `num_candidates` | postings |
| `KOSHA_KNN_PROBE_RADIUS_ALPHA` | Experimental (#384). Weight of a posting's radius in its rank score `max(0, d(q, c) - alpha * radius)`, used by the global cutoff and each segment's probe order. `1` is the triangle-inequality lower bound (unchanged); `0` ranks by raw centroid distance. Read once in `crates/kosha-segment/src/lib.rs` | `1.0` | weight |
| `KOSHA_KNN_PLAN_I8` | Two-stage kNN planning. A unit-norm vector column keeps i8 codes of its centroids in memory (built at load, about a quarter of the f32 centroids' bytes, no on-disk change); a query first bounds every centroid's rank score from the codes, then scores in f32 only the centroids that could be inside the probe budget. The bounds are guaranteed, so the plan (cutoff, postings probed, their order) and the results are identical to scoring every centroid. `0`, `false` or `off` loads no codes and scores every f32 centroid. `knn_plan_exact_centroids=` on the `search timing:` line counts the f32-scored centroids. Read once in `crates/kosha-segment/src/lib.rs` | `1` | flag |
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
| `KOSHA_API_KEY_REFRESH_SECS` | How often API keys are reloaded from Postgres (`auth.rs`). Revocations made on another pod arrive with the next successful reload. A pod whose reloads keep failing refuses operator admin credentials once its snapshot is ten intervals old, until a reload succeeds; tenant keys keep working from the last snapshot | `30` | seconds |

Namespaces written before tenant scoping (#262) have bare ids that no tenant can reach; the
server lists them in a `WARN` at boot. `kosha-server migrate-namespace --from <bare> --to
<tenant>/<name>` moves one. It reads `DATABASE_URL`, the S3 variables, `KOSHA_DATA_DIR` and
`KOSHA_WAL_MODE` like the server does. See "Migrating pre-#262 namespaces" in
`k8s/README.md` for what it does and the staging/prod procedure.

## Usage metering

Off by default (#348). With `KOSHA_USAGE_METERING=on` a pod records usage events per tenant and
namespace and delivers them to the usage store: `kosha.usage_events` in Postgres (migration 005)
when `DATABASE_URL` is set in a binary built with **postgres**, otherwise an in-memory store that
is per pod and lost on restart (local development only).

What is recorded:

- A search that answers 200: `queries` (1), `bytes_returned` (the response body) and
  `bytes_fetched` (bytes it pulled from object storage, whole files and ranged reads alike; 0 for
  a warm or result-cached search). `bytes_fetched` measures cache misses and is for cost
  attribution, not billing.
- An accepted document write (`/documents`, legacy `/index` and `/replace`): `bytes_written`
  (each document's id plus every field's name and value, as submitted) and `documents_written`.
  Recorded on the pod that executes the write.
- An applied delete by query (`POST /v1/namespaces/{ns}/delete`, legacy `/delete`):
  `documents_deleted` and `bytes_deleted`, the deleted documents' records in the doc store as
  stored (compressed when the doc store is; 0 for documents in segments written before
  `doc_store.offsets`). Nothing for a delete that matched nothing. The space itself comes back in
  `stored_bytes` once compaction rewrites the segments. Deleting a whole namespace or tenant is not
  a usage event: its deletion record (#347) and the end of its snapshots account for it.
- Once an hour, on a pod that accepts writes: `stored_bytes`, the object bytes of every segment
  the namespace's manifest references. Shards are added up under their root namespace; a segment
  a branch inherits is counted on the namespace that owns it. The WAL and superseded segments not
  yet garbage-collected are not included.
- With it, `vector_dimensions`: dimensions times vectors stored, `dim x count` over every vector
  column plus `dim x tokens` over every late-interaction field of the same segments. The numbers
  come from each segment's footer, read once per segment (from memory, the local disk or object
  storage) and kept for as long as a manifest lists the segment, so a snapshot reads nothing more.
  No format changed. A segment written before vector columns existed (#204) counts 0 until
  compaction rewrites it. The summary reports `vector_dimension_hours` and
  `vector_dimensions_latest`, like the stored-bytes fields.

`GET /v1/usage` (the caller's tenant) and `GET /v1/admin/usage[?tenant=]` (admin key) summarize the
raw events over `?from=&to=` (Unix seconds, `to` exclusive; default the last 30 days). The summary
adds `bytes_queried`, the billing meter for query volume: each search priced at its namespace's
`stored_bytes` snapshot current at that hour. It does not depend on what the search read, so it is
the same for whole-file, ranged and fully cached searches. `stored_byte_hours` is the sum of the
hourly snapshots in the window; an hour with no writer running has no snapshot and contributes
nothing (`stored_snapshots` counts the hours that did).

The summary also reports `documents_deleted` and `bytes_deleted`. The pipeline's own counters are
on `/metrics` (`kosha_usage_*`, see [metrics.md](metrics.md)).

Delivery: a request appends its event to a spool file and returns; a background thread sends
sealed spool files to the store and deletes each one after the store accepted it. The store ignores
an event it already has, so a resend after a crash counts once. Events survive a store outage (up
to the spool cap; beyond it they are dropped and counted in `pipeline.dropped_events` of the admin
route) and a process restart on the same disk. A host crash or a pod replaced with an empty disk
loses the events appended since the last flush.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_USAGE_METERING` | `on`, `true` or `1` turns usage metering on for this pod | off | flag |
| `KOSHA_USAGE_SPOOL_DIR` | Directory of the local event spool. Put it on a persistent volume to keep undelivered events across pod replacement. One directory per running pod: a starting pod takes over every spool file it finds, so two live pods sharing a directory lose events | `<KOSHA_DATA_DIR>/_usage` | path |
| `KOSHA_USAGE_SPOOL_MAX_BYTES` | Cap on undelivered events on disk; further events are dropped and counted | `268435456` | bytes |
| `KOSHA_USAGE_FLUSH_MS` | How often the spool is delivered to the store. Minimum 100 | `5000` | milliseconds |
| `KOSHA_USAGE_SNAPSHOT_TICK_MS` | How often a writer checks for namespaces without a `stored_bytes` snapshot this hour (one object listing per namespace per hour). Minimum 1000 | `300000` | milliseconds |

### Reconciling stored bytes with object storage

`GET /v1/admin/usage/reconcile` (admin key, on a pod with metering on) and the one-shot
`kosha-server reconcile-usage` compare each namespace's latest `stored_bytes` snapshot with what
object storage holds under the namespace's prefix: the S3 bucket, or `KOSHA_DATA_DIR` on a node
without one. They read listings only (one LIST page at a time, never an object body) and change
nothing.

A snapshot counts every object under `<namespace>/<segment>/` for the segments the manifest lists
and the namespace owns, shards added up under their root. The report puts each object under a
namespace's prefix in one class, so that the comparison is between like numbers:

- `referenced_bytes`: under a segment the manifest lists and owns. This is what is compared with
  `snapshot_bytes`. It includes superseded deletion bitmaps inside such a segment
  (`superseded_deletion_bytes`), which the snapshot also counts until segment GC removes them.
- `referenced_unmetered_bytes`: referenced by a branch's manifest and counted by no snapshot: a
  segment a branch still lists after the namespace that wrote it compacted it away, and a deletion
  bitmap a branch wrote for a segment it inherits.
- `orphaned_bytes`: referenced by no manifest. `retired_bytes` (a publish recorded dropping the
  segment; GC deletes it after `KOSHA_GC_GRACE`), `untracked_bytes` (no manifest ever listed it: a
  crashed flush, an upload not published yet) and `stray_bytes` (a file directly under the prefix).

`status` per namespace: `match` or `mismatch` (the difference against the tolerance),
`storage_without_snapshot` (objects, and no snapshot inside the window: storage nobody is metered
for), `snapshot_without_storage`, `deleted` (a deleted namespace, #347, whose prefix is empty; its
last snapshots are still in the window), `deleted_with_storage` (objects remain under a deleted
namespace's prefix; `deletion` carries the record), `snapshot_after_deletion` (a deleted namespace
snapshotted again in an hour that began after its deletion: a writer is still metering it) and
`empty`, plus `removed` and `held_by_branch` (below). Everything but `match`, `deleted`, `empty`,
`removed` and `held_by_branch` is out of tolerance. `control_plane` says whether the namespace is
`live`, `deleted` (a deletion record covers it) or `unknown`.

Deleting a branch and `migrate-namespace` write no deletion record, so such a namespace reads as
`unknown`. It is not a fault: its last snapshots stay inside the window for a while, and deleting a
branch keeps, under the deleted branch's prefix, the segments a descendant branch inherited from
it. An `unknown` namespace is therefore `removed` when its prefix is empty and `held_by_branch`
when every object under it is still listed by a branch's manifest (`referenced_unmetered_bytes`;
do not delete such a prefix by hand), provided its latest snapshot, if any, is stamped with an
earlier hour than the current one. A snapshot from the current hour says a writer may still be
metering it, and the row is judged like any other (`snapshot_without_storage`, `mismatch`); so is
one with orphaned objects (`storage_without_snapshot`). A namespace removed this way between the
top of the hour and the run is reported once for that reason. Prefixes the control
plane does not list are found by listing the storage root and each tenant's directory; at most
1,000 of them get a row. Top-level names starting with `_` or `.` that are not namespaces (`_wal`,
`_usage`, `_warmset`) are named in `skipped_prefixes` and not listed.

A difference is within tolerance when it is at most the larger of `tolerance` x the larger of the
two numbers and `tolerance_bytes`. The byte floor never passes a non-zero snapshot against no
referenced bytes at all: an `unknown` prefix whose objects no manifest covers reads `mismatch`, and
a live namespace with an empty prefix reads `snapshot_without_storage`, however small the
snapshot. The snapshot window starts at the top of the hour `max_snapshot_age` before now, so a
one-hour window still sees the previous hour's snapshot before the current one is taken. The snapshot of an hour is taken in the first minutes of that
hour and the listing is taken now, so a namespace that was written to or compacted in between
differs by that much: run the job a few minutes past the hour, or allow for an hour of growth. On
a node with S3, a segment flushed but not uploaded yet is in the snapshot (from local disk) and
not in the bucket.

The command prints the report as one JSON document on stdout and exits `0` when every namespace is
within tolerance, `1` when one is not, `2` when the run could not finish (nothing was compared).
Flags: `--tenant <id>`, `--tolerance <fraction>`, `--tolerance-bytes <n>`,
`--max-snapshot-age <duration>`, `--pretty`; the route takes the same names as query parameters
(`?tenant=&tolerance=&tolerance_bytes=&max_snapshot_age=`) and answers `200` with
`"within_tolerance": false` rather than an error status. The command needs a binary built with
**postgres** and `DATABASE_URL` (it reads namespaces, deletion records and usage events there),
and the same S3 variables as the server. `k8s/base/cronjob-usage-reconcile.yaml` runs it daily,
shipped suspended.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_USAGE_RECONCILE_TOLERANCE` | Allowed difference between a snapshot and the referenced bytes, as a fraction of the larger of the two. A value that is not a number between 0 and 1 fails the run | `0.05` | fraction |
| `KOSHA_USAGE_RECONCILE_TOLERANCE_BYTES` | Allowed difference in bytes whatever the fraction allows, so small namespaces do not fail on one flush | `1048576` | bytes |
| `KOSHA_USAGE_RECONCILE_MAX_SNAPSHOT_AGE` | A snapshot older than this does not count: the namespace is reported as `storage_without_snapshot`. At least one hour | `3h` | duration |

## Billing

Off by default (#349). With `KOSHA_BILLING=on` a pod loads a plan catalog, refuses requests of
tenants that are over their free tier or behind on payment, applies each plan's quota defaults, and
serves the billing API. It needs usage metering (`KOSHA_USAGE_METERING=on`) on every pod: charges
and free-tier limits are computed from the usage events. Accounts and invoices are kept in
`kosha.billing_accounts` and `kosha.billing_invoices` in Postgres (migration 007) when
`DATABASE_URL` is set in a binary built with **postgres**, otherwise in memory (per pod, lost on
restart; local development only).

**Plans.** `KOSHA_BILLING_PLANS_FILE` is a JSON price list. No prices are built in; the numbers
below are an example. It is read at startup and reloaded by `POST /v1/admin/billing/catalog/reload`
(on the pod that serves it) or, with `KOSHA_BILLING_PLANS_WATCH=on`, by every pod when the file's
modification time or size changes (checked at each standings refresh). A reload is applied only
when the file parses, validates and still has every plan an account is on; otherwise the pod keeps
the catalog it has, logs why, and shows it as `catalog_error` in `GET /v1/admin/billing/accounts`.
A catalog that uses the fields added in #349's second part (`upgrade_plan`,
`dunning.retry_after_days`, the `vector_dimensions` meter) is refused by older binaries: roll the
binary out before the file.

```json
{
  "default_plan": "free",
  "upgrade_plan": "payg",
  "dunning": {"payment_terms_days": 14, "read_only_after_days": 7, "suspend_after_days": 21,
              "retry_after_days": [3, 5, 7]},
  "plans": {
    "free": {
      "kind": "free",
      "meters": {"queries": {"included": 100000}, "stored_bytes": {"included": 1073741824}},
      "limits": {"qps": 5, "namespaces": 3}
    },
    "payg": {
      "kind": "usage",
      "base_fee_cents": 2500,
      "meters": {
        "queries": {"included": 1000000, "unit": 1000000, "unit_price_cents": 400},
        "bytes_queried": {"unit": 1099511627776, "unit_price_cents": 500},
        "bytes_written": {"unit": 1073741824, "unit_price_cents": 200},
        "stored_bytes": {"included": 1073741824, "unit": 1073741824, "unit_price_cents": 33}
      }
    },
    "enterprise": {"kind": "enterprise", "base_fee_cents": 500000}
  }
}
```

- `kind` is `free` (included amounts are hard limits, nothing is charged), `usage` (use above the
  included amount is charged) or `enterprise` (rated like `usage`, never restricted automatically
  for non-payment).
- `meters` prices the billable meters: `queries`, `bytes_queried`, `bytes_written` (see "Usage
  metering"), `stored_bytes`, the average bytes stored over the month (byte-hours divided by the
  hours in the month), and `vector_dimensions`, the average vector dimensions stored, likewise. On
  a free plan `stored_bytes` and `vector_dimensions` are compared with what is stored now. The first `included` are free; the rest cost `unit_price_cents` per `unit`.
  A meter a plan does not list is neither charged nor limited.
- `limits` are the plan's tenant quota defaults, with the fields of a plan in
  `KOSHA_TENANT_LIMITS_FILE` ("Tenant limits"). Where that file also gives the tenant a limit, the
  file wins, field by field.
- `upgrade_plan` is the plan a tenant moves to by completing a self-service checkout (below). It
  must not be a free plan. Without it checkout is off.
- `default_plan` is the plan of a tenant nobody put on one. Without it such a tenant is not
  billed, limited or restricted, which is what happens to every existing tenant when billing is
  first turned on.

**Rating.** A period is a calendar month, UTC. Amounts are integer US cents; each invoice line is
rounded half up and the invoice total is the sum of its lines. `POST /v1/admin/billing/close-period`
(`{"period": "YYYY-MM"}`, default last month) is the monthly job: one invoice per tenant, id
`<tenant>/<YYYY-MM>`. It is safe to run again: an existing invoice is never re-rated, and one that
could not be handed to the payment provider is handed over by the next run. Nothing schedules it;
the scheduler below runs it, or call it from a CronJob once every pod has delivered its usage
for the month (usage that arrives after a period is closed is not on the invoice). An invoice that
totals zero is recorded as paid.

**Proration.** A plan change made through `POST /v1/admin/billing/accounts` or a completed checkout
is recorded (`kosha.billing_plan_changes`, migration 011). A month with recorded changes is cut at
each change, and each stretch is rated on the plan in force during it: the usage recorded inside
the stretch, with the plan's base fee and every meter's included amount multiplied by the
stretch's share of the month (milliseconds) and rounded half up. Storage and vector dimensions are
the stretch's byte-hours (dimension-hours) divided by the hours of the whole month, so a constant
size costs the same however the month is cut. A free stretch charges nothing. Each line of such an
invoice names its plan (`lines[].plan`). The invoice is issued on the plan in force at the end of
the month, even when it is closed after a later change. A month without a recorded change is rated
on one plan, exactly as before; so is every month of a tenant whose plan was changed before
migration 011, or by an older binary. An account created inside the month was on the default plan
(if any) before it. The free-tier limits and the `accrued` charge of `GET /v1/billing` use the
current plan for the whole month to date.

**Payment provider.** With `KOSHA_BILLING_PROVIDER=stripe` an invoice is sent to Stripe (a
customer per tenant, an invoice with one item per charged line). A customer with a default payment
method is charged automatically (`collection_method=charge_automatically`); anyone else is sent the
invoice to pay on Stripe's hosted page (`send_invoice`, due when Kosha's invoice is). Without a
provider invoices are only recorded, and a payment collected elsewhere is recorded with
`POST /v1/admin/billing/invoices/mark-paid` (`{"invoice_id": ...}`). Amounts are US dollars;
other currencies and tax are not supported (Stripe Tax is not used, and no tax line is added).

**Webhooks.** With `KOSHA_STRIPE_WEBHOOK_SECRET` set, `POST /v1/billing/webhooks/stripe` takes
Stripe's event deliveries. Point a Stripe webhook endpoint at it (at least `invoice.paid`,
`invoice.payment_failed`, `invoice.finalized`, `invoice.voided`, `checkout.session.completed`,
`customer.updated` and `payment_method.detached`). It needs no API key; each delivery must carry a
`Stripe-Signature` whose `v1` HMAC-SHA256 of `"<t>.<body>"` matches one of the configured secrets
(compared in constant time), with `t` within `KOSHA_STRIPE_WEBHOOK_TOLERANCE_SECS` of the pod's
clock. To roll the endpoint secret, set both secrets (comma-separated) on the pods, roll it in
Stripe, then remove the old one. Every event id is recorded once applied
(`kosha.billing_webhook_events`), so a redelivery is acknowledged and changes nothing. A refused
signature answers 400; an event that could not be applied (store down) answers 503 and Stripe
delivers it again. Events: `invoice.paid` marks the invoice paid, `invoice.voided` void,
`invoice.payment_failed` records a failure for dunning, `invoice.finalized` records Stripe's
invoice id if the submission did not get to; `checkout.session.completed` completes a self-service
upgrade; `customer.updated` and `payment_method.detached` keep the payment method on file
current. Subscription events and anything else are acknowledged and ignored: Kosha rates and
invoices usage itself and does not use Stripe subscriptions. `POST /v1/admin/billing/sync`
(polling) stays as the fallback for a missed delivery. The route is answered by whichever pod
receives it, never forwarded.

**Self-service upgrade.** With a provider, `KOSHA_BILLING_RETURN_URL` and `upgrade_plan` in the
catalog, `POST /v1/billing/checkout` (`{"success_url"?, "cancel_url"?}`, a tenant key with the
`admin` scope) answers `{"id", "url"}`: a Stripe Checkout page in setup mode where the user enters
a card. When it completes, the `checkout.session.completed` webhook makes the card the customer's
default and moves the tenant to `upgrade_plan` (an enterprise tenant keeps its plan). From then on
its invoices are charged automatically. `POST /v1/billing/portal` (`{"return_url"?}`, `admin`
scope) opens Stripe's customer portal for changing the card and seeing invoices; it answers 409
before the tenant has a Stripe customer. Redirect URLs default to `KOSHA_BILLING_RETURN_URL` (with
`?checkout=success` / `?checkout=cancel`) and must be on its origin. Both routes stay open to a
read-only or suspended tenant. `GET /v1/billing` adds `payment_method_on_file` and
`upgrade_plan`.

**Dunning.** A failed payment (the webhook, or a failed retry) is recorded per invoice. When the
tenant has a payment method on file, the invoice is retried on the `retry_after_days` schedule
(days after the first failure, each entry after the previous retry; `[3, 5, 7]` retries on days 3,
8 and 15) and at once after a new payment method is put on file. Set `"retry_after_days": []` when
Stripe's own automatic retries (Smart Retries) are on, or both will retry. Each open invoice gets a
notice at each stage it reaches: `payment_failed`, `overdue` (past due), `read_only` and
`suspended` (enterprise plans are never restricted and get neither), and `recovered` once a dunned
invoice is paid. A notice goes out once per invoice (claimed in `kosha.billing_notices` before it
is sent, released if sending fails); a pass that finds several new stages sends only the most
severe one. Notices go to the log, or as a JSON `POST` to `KOSHA_BILLING_NOTIFY_URL` with
`x-kosha-notice-id` and, with `KOSHA_BILLING_NOTIFY_SECRET`, an `x-kosha-signature` in Stripe's
scheme; any 2xx answer is delivery. `POST /v1/admin/billing/dunning` runs a pass now. The account
state itself still follows the oldest unpaid invoice's due date (below); dunning collects and
tells, it does not change the clock.

**Scheduler.** `KOSHA_BILLING_SCHEDULER=on` runs the jobs in the server. Every
`KOSHA_BILLING_SCHEDULER_INTERVAL_SECS` each pod with it on tries to take the `billing-scheduler`
lease (`kosha.billing_leases`, three intervals long); the holder flushes its usage spool, closes
last month once `KOSHA_BILLING_CLOSE_DELAY_SECS` have passed since it ended (unless
`kosha.billing_jobs` records it closed without errors), syncs payments from the provider every
`KOSHA_BILLING_SYNC_INTERVAL_SECS`, runs a dunning pass and refreshes its standings. Every step is
idempotent, so two pods running a tick at once (clocks that disagree about the lease) do no harm.
Without Postgres the lease is per pod. Only last month is closed automatically: a month missed
while the scheduler was off is closed with `close-period`.

**Enforcement.** A refused request gets `402 Payment Required` with a JSON body
`{error, reason, plan, state}`:

- `free_tier_exceeded` (also `meter`, `used`, `included`): on a free plan, a meter's month-to-date
  use has reached its included amount. `queries` and `bytes_queried` block searches;
  `bytes_written` and `stored_bytes` (what is stored now) block writes. Moving the tenant to a paid
  plan (`POST /v1/admin/billing/accounts`, `{"tenant", "plan"}`) lifts it, as does the next month.
- `billing_read_only`: an invoice is `read_only_after_days` past due. Writes are refused; searches
  and deletes are not.
- `billing_suspended`: an invoice is `suspend_after_days` past due. Everything is refused except
  `GET /v1/billing` and `GET /v1/usage`.

An invoice is due `payment_terms_days` after it is issued. The state follows the oldest unpaid
invoice and is computed, not stored: once the invoice is paid or voided the account is `active`
again. Admin and pod-to-pod credentials are never restricted.

Each pod enforces from a snapshot it rebuilds every `KOSHA_BILLING_REFRESH_SECS`, so a tenant can
run past a free allowance, and a change made through another pod takes effect, up to one interval
late (plus `KOSHA_USAGE_FLUSH_MS` for usage). The refresh reads every tenant's usage for the month
so far when a free plan with limits is in use. A pod that cannot reach the stores keeps its last
snapshot; a pod that has never read them restricts nobody and applies no plan quotas (only the
limits file) until its first refresh succeeds.

`GET /v1/billing` shows a tenant its plan, state, month-to-date usage with the charge accrued so
far, and its invoices. `GET /v1/admin/billing/accounts` and
`GET /v1/admin/billing/invoices[?tenant=]` are the operator's views.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_BILLING` | `on`, `true` or `1` turns billing on for this pod. Startup fails without usage metering or a valid plans file | off | flag |
| `KOSHA_BILLING_PLANS_FILE` | The plan catalog (JSON, above). Required with `KOSHA_BILLING=on` | unset | path |
| `KOSHA_BILLING_REFRESH_SECS` | How often a pod rebuilds its snapshot of tenant standings | `60` | seconds |
| `KOSHA_BILLING_PROVIDER` | `stripe`, or `none` to only record invoices | `none` | name |
| `KOSHA_STRIPE_API_KEY` | Stripe secret key. Required with `KOSHA_BILLING_PROVIDER=stripe`; keep it in a secret, never in a tracked file | unset | secret |
| `KOSHA_BILLING_PLANS_WATCH` | `on` makes every pod reload the plans file when its modification time or size changes (checked every `KOSHA_BILLING_REFRESH_SECS`) | off | flag |
| `KOSHA_STRIPE_WEBHOOK_SECRET` | Stripe webhook signing secret(s) (`whsec_...`), comma-separated while rolling one. Turns `POST /v1/billing/webhooks/stripe` on; keep it in a secret | unset (route off) | secret |
| `KOSHA_STRIPE_WEBHOOK_TOLERANCE_SECS` | How far a delivery's signed timestamp may be from the pod's clock | `300` | seconds |
| `KOSHA_BILLING_RETURN_URL` | Where Stripe sends users back after checkout or the portal (`https://`, or `http://` for localhost). Turns `POST /v1/billing/checkout` and `POST /v1/billing/portal` on; redirect URLs a tenant passes must share its origin | unset (routes off) | URL |
| `KOSHA_BILLING_NOTIFY_URL` | HTTP endpoint that receives dunning notices as JSON (`https://`, or `http://` for localhost). Unset: notices are logged | unset (log) | URL |
| `KOSHA_BILLING_NOTIFY_SECRET` | Signs each notice (`x-kosha-signature`, HMAC-SHA256 in Stripe's scheme) | unset (unsigned) | secret |
| `KOSHA_BILLING_SCHEDULER` | `on` runs close-period, sync and dunning in the server, one pod at a time | off | flag |
| `KOSHA_BILLING_SCHEDULER_INTERVAL_SECS` | How often each pod tries to run a scheduler tick. Minimum 10 | `900` | seconds |
| `KOSHA_BILLING_CLOSE_DELAY_SECS` | How long after a month ends the scheduler closes it, for every pod to deliver its usage | `7200` | seconds |
| `KOSHA_BILLING_SYNC_INTERVAL_SECS` | How often the scheduler polls the provider for payments (the webhook fallback) | `3600` | seconds |
| `KOSHA_STRIPE_API_BASE` | Base URL of the Stripe API. Must be `https://`; plain `http://` is accepted for `localhost`, `127.0.0.1` and `[::1]` only (the secret key is sent to it), and startup fails otherwise | `https://api.stripe.com` | URL |

## HTTP server and auth

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_HTTP_PORT` | HTTP listen port (binds `0.0.0.0`) | `8080` | port |
| `KOSHA_HTTP_IO_TIMEOUT_SECS` | How long a request body may take to arrive (HTTP 408 after), and how long one response write may stall on a client that stopped reading. Also caps the default `KOSHA_HTTP_IDLE_TIMEOUT_SECS`. Bounds stalled clients, not how long a request may take (`http.rs`) | `300` | seconds |
| `KOSHA_HTTP_IDLE_TIMEOUT_SECS` | How long the server waits for a complete request head, counted from when it starts waiting: on a new connection that is the header-read timeout, and between requests on a kept-alive connection it is the idle timeout. The connection is closed after it | `30` (or `KOSHA_HTTP_IO_TIMEOUT_SECS` if lower) | seconds |
| `KOSHA_HTTP_MAX_CONNECTIONS` | Open connections served at once. A connection over the cap gets HTTP 503 with `Retry-After: 1` and is closed | `1024` | connections |
| `KOSHA_HTTP_MAX_INFLIGHT` | Requests running a handler at once (on a bounded blocking pool; the async side has at most 4 threads). A request over the cap gets HTTP 503 with `Retry-After: 1`. `GET /healthz` is answered without a slot | `256` | requests |
| `KOSHA_HTTP_SHUTDOWN_GRACE_SECS` | On SIGTERM or SIGINT the listener closes, idle connections close, and in-flight requests get this long to finish. The pod then releases its writer leases and exits | `25` | seconds |
| `KOSHA_TLS_CERT_PATH` | PEM certificate chain. With `KOSHA_TLS_KEY_PATH`, the listener serves HTTPS only. Setting just one of the two is a startup error | unset (HTTP) | path |
| `KOSHA_TLS_KEY_PATH` | PEM private key (PKCS#8, PKCS#1 or SEC1) for `KOSHA_TLS_CERT_PATH` | unset | path |
| `KOSHA_TLS_CLIENT_CA_PATH` | PEM CA bundle for client certificates (mTLS, #343). A presented certificate must chain to it or the handshake fails, and `/internal/*` answers 403 on a connection that presented none. Other routes still accept clients without a certificate. Needs `KOSHA_TLS_CERT_PATH`/`KOSHA_TLS_KEY_PATH`, or the server refuses to start | unset (no client certificates) | path |
| `KOSHA_AUTH` | `disabled` (or `off`, `false`, `0`, `none`) turns authentication off; every request becomes tenant `dev`. Local development only | enabled | flag |
| `KOSHA_API_KEY` | One tenant API key, mapped to tenant `default` | unset | string |
| `KOSHA_API_KEYS` | Comma-separated `key=tenant` pairs. An entry may add `;`-separated attributes (#346): `scopes=read+write` (`+`-separated subset of `read`, `write`, `admin`), `namespaces=docs+wiki` (the only namespaces the key may touch) and `expires_at=<unix seconds>`, e.g. `sk-ci=acme;scopes=read;namespaces=docs`. An entry without attributes has every scope, every namespace and no expiry, as before. A malformed attribute is a startup error that names the entry by position and tenant, never by key | unset | list |
| `KOSHA_API_KEY_AUTO_SCRUB` | `off` (or `disabled`, `false`, `0`) stops the server removing, by itself, the plaintext that API keys created before hashed keys still hold in `kosha.api_keys` (#346). On, every server with Postgres runs a scrub round on each key refresh; it rewrites those rows `KOSHA_API_KEY_SCRUB_GRACE_SECS` after the last sign of a server build from before hashed keys (one that could still read the plaintext). Off, run `kosha admin scrub-plaintext-api-keys` by hand once every pod is upgraded. Any other value is a startup error | `on` | `on` \| `off` |
| `KOSHA_API_KEY_SCRUB_GRACE_SECS` | How long the automatic scrub waits after the database last showed a sign of a pre-hashed-keys server: the first start of a server with the automatic scrub, or a key row such a server inserted. Longer than your slowest rolling deploy; `0` scrubs on the next round | `86400` | seconds |
| `KOSHA_ADMIN_API_KEY` | Key for `/v1/admin/*` and global `/stats`. Unset disables those routes | unset | string |
| `KOSHA_ADMIN_BOOTSTRAP_KEY` | `disabled` (or `off`, `false`, `0`) makes the server refuse `KOSHA_ADMIN_API_KEY` everywhere, so only per-operator credentials (`POST /v1/admin/operators`, Postgres) reach `/v1/admin/*`, global `/stats` and an admin-gated `/metrics`. Set it once every person and every automated caller of the shared key (the segment-GC CronJob, CI, scrapers) has its own operator credential: anything still sending the shared key gets `401`. Set it back to `enabled` to break glass. Any other value is a startup error (the value is not printed, in case it is a key). While the shared key is off (disabled here, or `KOSHA_ADMIN_API_KEY` unset), revoking or rotating operator credentials so that none stays usable, or the last one lapses within a day, is refused with `409` unless the request says `"force": true`. With it disabled the server warns at startup, and about hourly, when no operator credential is usable or the last one expires within a week; a disabled key also no longer counts as a configured credential for the fail-closed startup check | `enabled` | `enabled` \| `disabled` |
| `KOSHA_INTERNAL_API_KEY` | Key peers use for `GET /internal/segment/...` hydration. Unset disables peer-file access | unset | string |
| `KOSHA_LEGACY_ROUTES` | `off` (or `disabled`, `false`, `0`, `none`) makes the unversioned Phase 1 routes return 410 | on | flag |
| `KOSHA_DASHBOARD` | `on` (or `true`, `1`, `yes`, `enabled`) serves the read-only web UI at `/dashboard`, adds `recent_queries` to `GET /v1/namespaces/{ns}/stats`, and honors `"timings": true` on searches. See [dashboard.md](dashboard.md). `dashboard.rs`, `Dashboard::from_env` | off | flag |

The server refuses to start when no credential is configured (no env key, no admin or internal
key, no Postgres keys) unless `KOSHA_AUTH=disabled`.

The server speaks HTTP/1.1 with keep-alive (#259): responses carry a `content-length` and the
connection stays open for the next request. `KOSHA_TLS_CERT_PATH` turns HTTPS on for every
route on the port, client-facing and `/internal/*` alike. The clients that call other pods are
configured separately, in the next section.

### Self-service console

Sign-up, sessions, OAuth/OIDC sign-in and the web console at `/console` (#345); see
[organizations.md](organizations.md#self-service-console). All of it is off unless
`KOSHA_CONSOLE` is on. Flags here accept `enabled`/`on`/`true`/`1`/`yes` and
`disabled`/`off`/`false`/`0`/`no`; any other value, and any other configuration error while
the console is on, stops the server at startup (`console.rs`, `Console::from_env`). Use
Postgres in production: without it accounts live in one process's memory.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_CONSOLE` | Serve the console API (`/v1/console/*`) and web UI (`/console`). Off: both are 404 | off | flag |
| `KOSHA_CONSOLE_PUBLIC_URL` | The origin users reach the console at, e.g. `https://kosha.example.com` (no path). Required with `KOSHA_CONSOLE`. Email links and OAuth redirect URIs are built from it, and a state-changing request whose `Origin` differs is refused. `http://` only for `localhost`/`127.0.0.1` | unset | URL |
| `KOSHA_SIGNUP` | Open self-serve sign-up (email + password, and account creation through Google/GitHub). Off: accounts come from invites, SSO or the operator | off | flag |
| `KOSHA_SIGNUP_EMAIL_DOMAINS` | Comma-separated domains open sign-up is limited to (`example.com,example.org`). Empty: any domain | unset | list |
| `KOSHA_SESSION_TTL_HOURS` | Absolute lifetime of a console session | `24` | hours |
| `KOSHA_SESSION_IDLE_MINUTES` | A session unused for this long ends | `120` | minutes |
| `KOSHA_CONSOLE_SECRET_KEY` | 32 bytes (64 hex characters or base64) that seal organizations' OIDC client secrets in Postgres (AES-256-GCM). Unset: organization SSO is off. Keep it in a secret; changing it makes stored client secrets unreadable (owners re-enter them) | unset | key |
| `KOSHA_CONSOLE_CLIENT_IP_HEADER` | Header a trusted proxy puts the client address in (e.g. `x-forwarded-for`; the right-most entry is used), for the per-IP attempt limits. Unset: the TCP peer address. Set it only behind a proxy that overwrites the header | unset (peer address) | header name |
| `KOSHA_MAILER` | `log` prints mails (with their links) to the server log, for development; `smtp` sends through `KOSHA_SMTP_*` (`mailer.rs`) | `log` | `log` \| `smtp` |
| `KOSHA_SMTP_HOST` | SMTP relay host. Required with `KOSHA_MAILER=smtp` | unset | host |
| `KOSHA_SMTP_PORT` | SMTP relay port | `587` (`465` with `KOSHA_SMTP_TLS=tls`) | port |
| `KOSHA_SMTP_TLS` | `starttls` (required, never falls back to plain text), `tls` (implicit TLS) or `none` (plain text, no credentials allowed: a relay on the same host). Certificates are verified against the OS trust store | `starttls` | mode |
| `KOSHA_SMTP_USERNAME` | SMTP `AUTH PLAIN` user; set with `KOSHA_SMTP_PASSWORD` or not at all | unset | string |
| `KOSHA_SMTP_PASSWORD` | SMTP password | unset | string |
| `KOSHA_MAIL_FROM` | Sender address of console mails. Required with `KOSHA_MAILER=smtp` | unset | address |
| `KOSHA_OAUTH_GOOGLE_CLIENT_ID` | Google OAuth client (web application). With the secret, turns on "Continue with Google". Redirect URI to register: `<public URL>/v1/console/oauth/google/callback` | unset (off) | string |
| `KOSHA_OAUTH_GOOGLE_CLIENT_SECRET` | Its client secret | unset | string |
| `KOSHA_OAUTH_GITHUB_CLIENT_ID` | GitHub OAuth app. With the secret, turns on "Continue with GitHub". Callback URL: `<public URL>/v1/console/oauth/github/callback` | unset (off) | string |
| `KOSHA_OAUTH_GITHUB_CLIENT_SECRET` | Its client secret | unset | string |

Organization SSO (OIDC) is configured per organization in the console, not here; its
redirect URI is `<public URL>/v1/console/sso/callback`. SAML is not supported yet.

### TLS between pods

Pod-to-pod traffic is peer hydration (`GET /internal/segment/...`, query pod to query pod) and
ingest traffic (forwarded writes and the strong-consistency tail read, query pod to
`KOSHA_INGEST_HOST`). The clients for both are built in `internal_tls.rs`
(`InternalTls::from_lookup`), read once at startup. The settings **fail closed**: an unknown
mode, a missing or unparsable file, half a client identity, or a TLS setting while the mode is
`off` logs `FATAL: internal TLS: ...` and exits. The startup log prints
`internal TLS: on (internal CA, client_cert=yes, server_name=kosha-internal)` or similar.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_INTERNAL_TLS` | `off`: plain `http://` to peers, as before. `on`: peers are fetched over `https://` and verified against `KOSHA_INTERNAL_TLS_CA_PATH`, and an `https://` `KOSHA_INGEST_HOST` is verified the same way (an `http://` one logs a warning). `required`: as `on`, and the pod refuses to start with an `http://` `KOSHA_INGEST_HOST`, without `KOSHA_TLS_CERT_PATH`, or with a `DATABASE_URL` that does not force TLS (see "TLS to Postgres" below) | `off` | enum |
| `KOSHA_INTERNAL_TLS_CA_PATH` | PEM CA bundle peer certificates must chain to. Required unless `off`. Only this bundle is trusted for internal traffic, not the platform roots | unset | path |
| `KOSHA_INTERNAL_TLS_CLIENT_CERT_PATH` | PEM client certificate chain, presented to peers that ask for one (their `KOSHA_TLS_CLIENT_CA_PATH`). Set it together with `KOSHA_INTERNAL_TLS_CLIENT_KEY_PATH` or not at all | unset (no client certificate) | path |
| `KOSHA_INTERNAL_TLS_CLIENT_KEY_PATH` | PEM private key for `KOSHA_INTERNAL_TLS_CLIENT_CERT_PATH` | unset | path |
| `KOSHA_INTERNAL_TLS_SERVER_NAME` | DNS name peer certificates are checked against instead of the host in the URL. Peers are dialed by pod IP (`POD_IP`), which a certificate issued ahead of time cannot list; with this set, one certificate with this DNS name serves every pod. Unset, a certificate must match the URL host (an IP SAN for peer fetches) | unset (URL host) | DNS name |

To turn it on across a cluster:

1. Issue a certificate signed by an internal CA whose SANs include the ingest Service name and
   a fixed name such as `kosha-internal` (`k8s/base/certificate-internal-tls.example.yaml` is a
   cert-manager example). The same certificate can serve as the client identity.
2. Mount it on every pod and set `KOSHA_TLS_CERT_PATH`/`KOSHA_TLS_KEY_PATH` (server side) and
   `KOSHA_INTERNAL_TLS=on`, `KOSHA_INTERNAL_TLS_CA_PATH`, `KOSHA_INTERNAL_TLS_SERVER_NAME`
   (client side), and switch `KOSHA_INGEST_HOST` to `https://`. HTTPS is per port, so the
   kubelet probes need `scheme: HTTPS` too.
3. Optionally require mTLS on `/internal/*` with `KOSHA_TLS_CLIENT_CA_PATH` and give every pod
   `KOSHA_INTERNAL_TLS_CLIENT_CERT_PATH`/`KEY_PATH`. Roll the clients out first, then the
   server setting.
4. Set `KOSHA_INTERNAL_TLS=required` once every pod is on TLS.

During a rollout, a pod on `on` that reaches a peer still serving plain HTTP fails that peer
fetch and falls back to S3, so mixed pods cost extra S3 reads, not errors. Forwarded writes have
no such fallback: switch `KOSHA_INGEST_HOST` to `https://` only after the ingest pod serves
HTTPS. Certificates are
read at startup; a renewed certificate takes effect when the pod restarts.

#### TLS to Postgres

Every pod also talks to the control-plane database: manifests, namespace and tenant names,
leases and the API-key registry. That connection is configured in `DATABASE_URL` itself, with
the usual Postgres parameters, and applies to every pool the server opens (control plane,
writer lease, hydration leases, API keys) and to `migrate` and `migrate-namespace`:

| `sslmode=` | Encrypted | Server verified |
| --- | --- | --- |
| `disable`, `allow` | no | no |
| `prefer` (the default when unset) | only if the server offers TLS; falls back to plaintext | no |
| `require` | yes, or the connection fails | no |
| `verify-ca` | yes | certificate chain |
| `verify-full` | yes | certificate chain and host name |

`verify-ca` and `verify-full` check the server certificate against `sslrootcert=<path>`, a PEM
CA bundle mounted into the pod (for a managed database, the provider's CA bundle). Without
`sslrootcert` they use the public web roots, which a private or provider CA is not part of.
For example:

```
postgres://kosha:<password>@<host>:5432/kosha?sslmode=verify-full&sslrootcert=/etc/kosha/pg/ca.crt
```

`PGSSLMODE` and `PGSSLROOTCERT` are read when the URL does not name the parameter. The client
ignores a `PGSSLMODE` it cannot parse, surrounding whitespace or a trailing newline included
(a Secret made with `echo` has one), and uses `prefer`; the check below does not accept such a value
as forcing TLS.

`KOSHA_INTERNAL_TLS` holds this hop to the same rule as the others. With `required`, the
server, `migrate` and `migrate-namespace` exit before opening a connection unless the
`sslmode` in effect is `require`, `verify-ca` or `verify-full`. With `on`, any other value (or
none) logs one `WARN:` line at startup. With `off` nothing is checked. Neither message prints
the URL. The check reads only the mode, so `migrate` and `migrate-namespace` do not need the
`KOSHA_INTERNAL_TLS_*` files mounted.

`GET /internal/segment/...` streams the cached file as raw `application/octet-stream`. Peers on
builds from before #259 send base64 with `x-kosha-encoding: base64`; the client still decodes
that, so a rolling upgrade with mixed peers works.

## Metrics

`GET /metrics` serves Prometheus text format. The metric set is listed in
[`metrics.md`](metrics.md). Read once at startup in `metrics::MetricsConfig::from_env`.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_METRICS` | `off` (or `false`, `0`) makes `GET /metrics` return 404 | on | flag |
| `KOSHA_METRICS_AUTH` | `admin` requires the `KOSHA_ADMIN_API_KEY` credential on `GET /metrics` (403 without it). `none` (or unset) leaves it open, like `/healthz`. Any other value is treated as `admin` | `none` | `none` \| `admin` |
| `KOSHA_METRICS_NAMESPACE_LABELS` | `on` (or `true`, `1`) adds a `namespace` label to `kosha_search_requests_total`. One series per namespace and outcome, and it publishes tenant namespace names to whoever can scrape. See `metrics.md` | off | flag |
| `KOSHA_METRICS_TENANT_LABELS` | `on` (or `true`, `1`) adds a `tenant` label to `kosha_tenant_rejections_total` and the `kosha_cache_*` families (#342). One series per tenant and reason (or cache), and it publishes tenant ids to whoever can scrape. See `metrics.md` | off | flag |
| `KOSHA_SLOW_SEARCH_LOG_MS` | A search prints its `search timing:` log line (per-phase ms, S3 fetches, kNN counters) only when the larger of its `total_ms` (sum of the scored phases) and `wall_ms` (the handler's wall time up to the built response) is at least this. Result-cache hits never print it. `0` prints it for every search, as before; set it when a bench or eval reads those lines. Read once in `main.rs` (`slow_search_log_ms`); an unparseable value logs one `WARN:` at startup and falls back to the default | `1000` | ms |

## Roles

`KOSHA_ROLE` decides what a pod does (#254, `crates/kosha-server/src/role.rs`). An explicit role
is checked against the rest of the environment, and a pod whose configuration contradicts it exits
with status 1 at startup.

| Role | Writes | Compaction scheduler | Routes | Requires |
| --- | --- | --- | --- | --- |
| `query` | forwarded to `KOSHA_INGEST_HOST` | never | all (mutating routes forwarded) | `KOSHA_INGEST_HOST` |
| `ingest` | executed, under the writer lease | when `KOSHA_COMPACTION_ON_INGEST` is true | all | no `KOSHA_INGEST_HOST` |
| `indexer` | none | always | `/healthz`, `/readyz`, `POST /v1/admin/compact*`, `GET /v1/admin/compaction-*`; everything else 404 | `DATABASE_URL`, no `KOSHA_INGEST_HOST` |
| `standalone` | executed, under the writer lease | when `KOSHA_COMPACTION_ON_INGEST` is true | all | no `KOSHA_INGEST_HOST` |

An unset `KOSHA_ROLE` keeps the pre-#254 rule and logs a warning: `KOSHA_INGEST_HOST` set means
`query`, unset means `standalone`. The legacy value `compaction` is refused (it used to start a
second, uncoordinated writer); use `indexer`. An indexer publishes compaction results with manifest
CAS and rebase, without a WAL (`KOSHA_WAL_MODE=s3` is ignored there) and without a writer lease.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_ROLE` | `query`, `ingest`, `indexer` or `standalone` (see above). `query` and `ingest` also name the persisted warm set | inferred from `KOSHA_INGEST_HOST` | enum |
| `KOSHA_INGEST_HOST` | Query pods: the base URL every mutating route is forwarded to. Must be set exactly when the role is `query` | unset | URL |
| `KOSHA_INGEST_PROXY_TIMEOUT_SECS` | Timeout for a forwarded write | `300` | seconds |

## Writer lease and fencing

A writer pod (`ingest`, `standalone`) takes a per-namespace lease in the control store
(`kosha.writer_leases`, migration 003) before it accepts writes for a namespace, renews it every
TTL/3 in the background, and presents the lease's fencing token on every manifest CAS. A takeover
after the TTL raises the token, and the CAS of a writer holding an older token is rejected
(`Fenced`, HTTP 503 with `Retry-After`). A fenced writer stops accepting writes for the namespace
and drops its unpublished local state; it takes the lease again only once the other writer's lease
has lapsed. Writes it acknowledged but never published stay in its WAL.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_WRITER_LEASE` | `off` disables leases and fencing: plain CAS, the pre-#254 behaviour | on | flag |
| `KOSHA_WRITER_LEASE_TTL_MS` | Lease lifetime; renewed every TTL/3. Minimum 300. Also the `Retry-After` of a refused write | `30000` | milliseconds |

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
| `KOSHA_COMPACTION_INTERVAL_MS` | Background compaction scheduler period on pods that run it (`indexer`, `standalone`, and `ingest` unless `KOSHA_COMPACTION_ON_INGEST=false`); `0` disables it (`compaction_sched.rs`). Benchmarks and the conformance suite set `0` | `30000` | milliseconds |
| `KOSHA_COMPACTION_ON_INGEST` | Whether a writer pod (`ingest`, `standalone`) runs the compaction scheduler. Set `false` once an `indexer` pod compacts (k8s base does). The admin compaction API still works either way | `true` | flag |
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

## Deleted namespaces (cache purge and retry)

Read in `PurgeSettings::from_env` (`crates/kosha-server/src/deletion_purge.rs`). Both are off
unless set. A value that is not a whole number of milliseconds stops the server at startup.
See "Deleting a namespace" in the README for what a pass removes and when.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_DELETION_PURGE_INTERVAL_MS` | How often this pod (any role) reads the deletion records it has not handled and removes the deleted namespaces' and tenants' cached copies from its memory and local disk. The first pass runs at boot and reads every record. Unset or `0`: off, and cached copies leave only by eviction | unset (off) | milliseconds |
| `KOSHA_DELETION_RESUME_INTERVAL_MS` | How often a writer pod (`ingest` or `standalone`) retries the storage reclaim of pending namespace deletion records, starting at boot. A record that stays pending waits twice as long before each further try, up to one hour. Tenant records are not retried. Each retry takes the namespace's writer lease first and is skipped while another writer holds it; with `KOSHA_WRITER_LEASE=off` there is no such guard, so do not set this where two writer pods can overlap. Ignored on `query` and `indexer` pods. Unset or `0`: off | unset (off) | milliseconds |

## Segment format (rollout and rollback gates)

Writers emit the newest format by default. Pin an older version during a rolling deploy, or
before rolling back, until every reader understands the newer one. Read once in
`crates/kosha-segment/src/lib.rs`.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_POSTINGS_WRITE_VERSION` | Inverted-postings version written: `4`, `5` or `6`. `6` is experimental (#386): v5 with bit-packed 128-posting scoring blocks, readable only by builds that include #386, so set it only once every query pod does | `5` | version |
| `KOSHA_VECTOR_WRITE_VERSION` | Vector-index version written: `1` (flat), `2`, `3` or `4` (posting-based) | `4` | version |
| `KOSHA_VECTOR_STORE` | Where vector field values are stored: `sidecar` (`doc_vectors.bin`) or `inline` (in `doc_store.bin`). Set `inline` before rolling back past #243 | `sidecar` | enum |
| `KOSHA_DOC_STORE_COMPRESSION` | Experimental (research #388). `zstd`: new segments store each `doc_store.bin` record as its own zstd frame with a per-segment trained dictionary (kept in the `doc_store.offsets` trailer), and vectors always go to `doc_vectors.bin`. `auto`: fresh segments raw, merges keep their inputs' format. `none`: always raw (compact with this before rolling back past #388; older readers cannot read compressed segments) | `auto` | enum |
| `KOSHA_DOC_STORE_ZSTD_LEVEL` | zstd level for a compressed doc store (1-22) | `3` | int |
| `KOSHA_DOC_STORE_ZSTD_DICT_BYTES` | Dictionary size trained per segment; `0` compresses records without a dictionary | `65536` | bytes |
| `KOSHA_DOC_STORE_ZSTD_SAMPLE_BYTES` | Record bytes sampled (even stride) to train the dictionary | 100x dictionary, max 16 MiB | bytes |
| `KOSHA_VECTOR_QUANTIZATION` | How new segments store vector posting entries: `f32`, `i8` (scalar-quantized, vector index version 4, #210) or `rabitq` (RaBitQ 1-bit codes against the posting centroid, vector index version 5; ~30x smaller than `f32` at 1024 dims). Turn a quantized mode on only once every query pod reads its version; turn it off and compact before rolling back past it | `f32` | enum |
| `KOSHA_VECTOR_QUANTIZATION_FIELDS` | Per-field overrides of `KOSHA_VECTOR_QUANTIZATION`, e.g. `body_emb:rabitq,title_emb:f32` | empty | list |
| `KOSHA_VECTOR_RESCORE_FACTOR` | A quantized column rescores its best `k * factor` probe candidates with the f32 vectors in `doc_vectors.bin` (#210). A `rabitq` column also rescores every candidate its error bounds cannot rule out of the top `k`, up to `8 * k * factor`. Rescoring needs the whole `doc_vectors.bin` local; without it the estimates stand. `0` disables | `4` | multiplier |
| `KOSHA_VECTOR_DOT_LANES` | Experimental (#382): accumulator lanes of the f32 dot that the SPFresh posting scans (exact restricted scan, `knn_search_within_lazy`, `knn_search_lazy`) score f32 posting entries with: `8`, `16`, `32` or `64`. Not covered: centroid ranking, quantized-column rescoring against `doc_vectors.bin`, sidecar/HNSW exact search. `8` is the existing kernel, bit for bit; wider ones differ in the last float bits. Meant to be measured together with an image built with `KOSHA_RUSTFLAGS` (Dockerfile build arg) | `8` | lanes |
| `KOSHA_VECTOR_RABITQ_ESTIMATOR` | Experimental (#382): how a `rabitq` column's codes are scored at query time. `lut` reads a 256-entry f32 table per code byte (128 KiB per query at 1024 dims); `popcount` quantizes the rotated query to 4 bits and scores with AND + popcount (RaBitQ §3.3.2), with an error bound widened by the query rounding. Query-time only, same on-disk codes; unknown values fall back to `lut`. On the default baseline-x86-64 image there is no `popcnt` instruction, so the count is a software popcount; measure it with an image built with `KOSHA_RUSTFLAGS="-C target-cpu=x86-64-v3"` (or `+popcnt`). When either #382 variable is set, or the image is built above baseline x86-64, the server logs one `vector kernels (#382, experimental): ...` line at startup | `lut` | enum |
| `KOSHA_VECTOR_BUILD_REFINE` | Whether a from-scratch vector index build (every flush, `KOSHA_VECTOR_COMPACTION=rebuild`, and compaction's fallback) refines balanced bisection's postings by nearest-centroid reassignment and a LIRE rebalance, tightening posting radii (#253). `off` builds exactly as before. Same on-disk format either way | `on` | enum |
| `KOSHA_DOC_REORDER` | Experimental (#385). `bp` writes compaction outputs, and `kosha-build-segments` segments (unless `--doc-reorder` is given), with documents renumbered by recursive graph bisection so documents sharing terms get nearby `doc_seq`s. Same on-disk format; results are unchanged (ties break on `_id`). Compaction only reorders when `KOSHA_DOC_REORDER_COMPACTION_MAX_DOCS` also allows it. `off` writes in ingest order exactly as before | `off` | enum |
| `KOSHA_DOC_REORDER_COMPACTION_MAX_DOCS` | Experimental (#385). With `KOSHA_DOC_REORDER=bp`, compaction reorders a merge only when its live documents (inputs minus pre-merge deletes) number at most this; BP compaction buffers the whole merged output (stored fields and sidecar vectors) in memory, so a larger merge streams in input order and logs a warning. `0` never reorders in compaction | `0` | documents |
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

## Native embedding

Read in `EmbedderRegistry::from_env` (`crates/kosha-server/src/embedding.rs`, #209). A vector
field opts in through the namespace schema
(`{"fields": {"emb": {"embedding": {"model": "...", "dims": 384, "provider": "http"}}}}`); the server
then embeds text sent for that field at ingest and `knn.query_text` at query time. Ingest and
query pods need the same settings; an `indexer` pod needs none (compaction copies vectors and
never embeds). A schema naming a model this pod has no provider for answers
`503` to text writes and `query_text` searches; namespaces without a declared model never call a
provider. The API key is never logged or returned.

| Variable | Purpose | Default | Units |
| --- | --- | --- | --- |
| `KOSHA_EMBEDDING_PROVIDERS` | Enabled providers: `http` (OpenAI-compatible server) and/or `mock` (deterministic hashed bag of words, for tests and local development only) | unset (no native embedding) | list |
| `KOSHA_EMBEDDING_URL` | Base URL of the OpenAI-compatible server; `/embeddings` is appended. Required for `http` | unset | URL |
| `KOSHA_EMBEDDING_API_KEY` | Sent as `Authorization: Bearer ...` | unset | string |
| `KOSHA_EMBEDDING_TIMEOUT_MS` | Timeout of one provider request | `10000` | ms |
| `KOSHA_EMBEDDING_BATCH_SIZE` | Texts per provider request | `64` | count |

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
| `KOSHA_RELEVANCE_JSON` | Write the `relevance` bench's report (used by the `relevance-compare` CI job and for `scripts/relevance/baseline.json`) |
| `KOSHA_RELEVANCE_FIXTURE` | Fixture directory for the `relevance` bench when no argument is given (default `scripts/relevance/fixtures/synthetic`) |
| `KOSHA_RELEVANCE_NUM_CANDIDATES` | kNN `num_candidates` in the `relevance` bench (default 1: the engine's `KOSHA_KNN_BUDGET_FRACTION` floor sets the probe budget) |
| `KOSHA_PLAN_BENCH_SEGS`, `KOSHA_PLAN_BENCH_CENTROIDS`, `KOSHA_PLAN_BENCH_DIM`, `KOSHA_PLAN_BENCH_QUERIES`, `KOSHA_PLAN_BENCH_ITERS`, `KOSHA_PLAN_BENCH_NUM_CANDIDATES` | Size of the kNN planning microbench (`knn_plan_microbench`, an ignored test in `crates/kosha-query/src/tests/knn_plan_two_stage.rs`, run in release mode): segments, centroids per segment, dimensions, distinct queries, timed passes over them, and the query's `num_candidates`. Defaults 196, 582, 1024, 16, 20, 100: the 10M vector bench's centroid count |
| `KOSHA_BENCH_BATCH_SIZE`, `KOSHA_BENCH_BACKLOG_DOCS`, `KOSHA_BENCH_FLUSH_THRESHOLD`, `KOSHA_BENCH_MAX_CONCURRENT_FLUSH_WRITERS`, `KOSHA_BENCH_MIN_DOCS_PER_FLUSH_WRITER`, `KOSHA_BENCH_WAL_APPENDS`, `KOSHA_BENCH_WAL_ENABLED` | Write-path benches (`kosha-write`) |

## Other environment variables

These do not start with `KOSHA_` but change server behavior:

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | Postgres control plane and API-key registry (**postgres**). Required by `kosha-server migrate`. Add `sslmode=verify-full&sslrootcert=<path>` to encrypt the connection (see "TLS to Postgres") |
| `PGSSLMODE`, `PGSSLROOTCERT` | Fallbacks for `sslmode` and `sslrootcert` when `DATABASE_URL` does not name them |
| `POD_IP` | This pod's IP, used as its peer-hydration address (**s3** + **postgres**) |
| `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `AWS_REGION` | Fallbacks for the `KOSHA_S3_*` settings above |
