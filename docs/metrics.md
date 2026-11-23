# Metrics

Every Kosha pod, whatever its `KOSHA_ROLE` (`query`, `ingest`, `indexer` or `standalone`), serves
`GET /metrics` on its HTTP port (`8080` by default) in the Prometheus text exposition format,
version 0.0.4 (`content-type: text/plain; version=0.0.4; charset=utf-8`). Each pod reports its
own process: a query pod answers `/metrics` itself and never forwards it to the ingest pod, and
an indexer serves it alongside its compaction admin API.

The pods in `k8s/base/deployment.yaml`, `k8s/base/deployment-query.yaml` and
`k8s/base/deployment-indexer.yaml` carry the
`prometheus.io/scrape`, `prometheus.io/port` and `prometheus.io/path` annotations, so a
Prometheus using the usual annotation-based pod discovery collects them with no extra
configuration.

With TLS between pods turned on (`k8s/components/internal-tls`), the port is HTTPS-only and
the component adds `prometheus.io/scheme: https`. The scrape job then has to honor that
annotation and trust the internal CA; see `k8s/README.md`, "Encryption in transit".

## Access

| Setting | Effect |
| --- | --- |
| default | Unauthenticated, like `/healthz` and `/readyz` |
| `KOSHA_METRICS_AUTH=admin` | Requires the `KOSHA_ADMIN_API_KEY` credential or an operator credential (`Authorization: Bearer <key>` or `X-Api-Key`); 403 without it. With no admin key configured and no operator credential, every scrape is refused |
| `KOSHA_METRICS=off` | `GET /metrics` returns 404 |

The default exposes no tenant data: no series carries a namespace or tenant label. Leave the
port reachable only from inside the cluster, as for the probes.

## Metric set

Latency histograms share one set of buckets, chosen for this service: a warm search answers
in under a millisecond, and a cold one that hydrates from S3 takes from hundreds of
milliseconds to minutes. Upper bounds, in seconds:
`0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300`,
plus `+Inf`.

### Search

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `kosha_search_requests_total` | counter | `outcome` | Search requests (`POST` and `GET`, every route form) by outcome. `ok`: HTTP 200. `rejected`: shed with HTTP 429, because the scoring queue was full (`KOSHA_SEARCH_QUEUE_DEPTH`, or the tenant's `KOSHA_SEARCH_QUEUE_DEPTH_PER_TENANT`) or the request hit a hydration or memory ceiling. `timeout`: shed with HTTP 429 after waiting `KOSHA_SEARCH_QUEUE_TIMEOUT_MS` for a scoring slot. `error`: any other status, including 4xx client errors |
| `kosha_search_duration_seconds` | histogram | none | Server-side wall-clock time of searches with outcome `ok`, from the handler's start to the response being ready. Includes result-cache hits |
| `kosha_search_phase_seconds` | histogram | `phase` | Time a scored search spent in each phase. `queue`: waiting for a scoring slot. `hydrate`: making the segment files it needs local (near zero when warm). `score`: the scoring pass. The same numbers as `queue_ms`, `hydrate_ms` and `score_ms` in the `search timing:` log line. Result-cache hits are not observed |
| `kosha_search_queue_depth` | gauge | none | Searches waiting for a scoring slot at scrape time |
| `kosha_search_queue_rejections_total` | counter | none | Searches the scoring queue shed, whether because it was full (including a waiting search shed to make room for a tenant holding less of the full queue) or because the wait timed out. Each is also counted in `kosha_search_requests_total` as `rejected` or `timeout` |
| `kosha_knn_degraded_segments_total` | counter | none | Segments a kNN search could not search by vector and skipped (the response's `knn_degraded_segments`, summed). Any increase means vector search silently returned fewer candidates |

### Writes

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `kosha_index_documents_total` | counter | none | Documents accepted by index and replace requests on this pod (the ingest pod; a query pod forwards writes) |
| `kosha_flush_total` | counter | none | Flush requests that succeeded. The idle-flush timer is not counted |
| `kosha_writer_fenced_total` | counter | none | Namespaces this writer fenced itself from because another writer took its writer lease (`KOSHA_WRITER_LEASE`). Any increase means two writers contended for a namespace; this one now refuses its writes |

### Tenant limits

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `kosha_tenant_rejections_total` | counter | `reason` | Requests refused with HTTP 429 by a per-tenant limit or quota (#341, `KOSHA_TENANT_LIMITS_FILE`). `reason`: `tenant_qps_exceeded`, `tenant_concurrency_exceeded`, `tenant_write_rate_exceeded`, `namespace_quota_exceeded`, `doc_quota_exceeded`, `vector_dims_exceeded`, `body_too_large` or `tenant_queue_full`, the same code as the response body's `reason`. A search refused by a limit from the file never reaches the scoring queue, so it is not counted in `kosha_search_requests_total`. `tenant_queue_full` is a search the scoring queue shed because its tenant already had `KOSHA_SEARCH_QUEUE_DEPTH_PER_TENANT` searches waiting; it is also counted in `kosha_search_queue_rejections_total` and as `rejected` in `kosha_search_requests_total`. Every other reason stays at zero without a limits file |

### Caches and hydration

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `kosha_cache_bytes` | gauge | `cache` (`tenant`) | `nvme`: bytes in the local read-through file cache (`KOSHA_DATA_DIR`). `segment`: approximate bytes of parsed segments held in memory (the `KOSHA_SEGMENT_CACHE_MAX_BYTES` ledger). `result`: bytes of cached `POST /search` responses (`KOSHA_RESULT_CACHE_MAX_BYTES`). Not reported for `corpus_stats` and `ceiling`, which are bounded by entry count |
| `kosha_cache_evictions_total` | counter | `cache` (`tenant`) | `nvme`: evictions from the local file cache, one per file or directory (such as a segment) removed, whether by the `KOSHA_CACHE_MAX_BYTES` limit or by an explicit eviction. `segment`, `result`: entries evicted to stay within the cache's budget. `corpus_stats`, `ceiling`: manifest versions evicted to stay within the cache's entry bound (a version replaced by a newer one of the same namespace is not counted) |
| `kosha_cache_hits_total` | counter | `cache` (`tenant`) | Lookups that found their entry (#342). `nvme`: a search's segment whose scoring files were already local, counted per segment checked by a hydration pass and, for a warm search served from the cached hydration verdict, per manifest segment. `segment`: a parsed segment found in memory. `result`: a search answered from the result cache. `corpus_stats`: a search whose namespace corpus statistics (BM25 totals) were cached for its manifest version. `ceiling`: the same for per-segment score ceilings |
| `kosha_cache_misses_total` | counter | `cache` (`tenant`) | Lookups that did not (#342). `nvme`: a search's segment that had to be hydrated. `segment`: a segment opened and parsed. `result`: a cacheable search that ran. `corpus_stats`, `ceiling`: the manifest version's entry had to be built |
| `kosha_hydrations_total` | counter | `source` | Segment files a hydration needed, by where they came from. `s3`: fetched from the object store. `peer`: streamed from the replica holding the hydration lease. `local`: already in the local cache when the fetch ran. Ranged span reads of doc stores and postings are not counted. Stays at zero without S3 |
| `kosha_hydration_lease_outcome_total` | counter | `outcome` | Cross-replica hydration lease claims (S3 and Postgres builds with `POD_IP` set). `won`: this replica fetches the file. `streamed_from_peer`: another replica held the lease and served the bytes. `timeout`: the holder did not serve the bytes within the peer-fetch budget (or the fetch failed), and the file was fetched from S3 instead |
| `kosha_segment_checksum_failures_total` | counter | none | Hydrated segment files that failed their `_COMMIT` size or CRC check, refetch failures included. The same number as `segment_checksum_failures` in `GET /v1/stats` |

Values that already exist elsewhere in the server (cache bytes, NVMe evictions, checksum
failures, queue depth) are read when Prometheus scrapes, not counted twice.

The purge of deleted namespaces (`KOSHA_DELETION_PURGE_INTERVAL_MS`, #347) adds no family to
`/metrics`. A segment directory it removes counts in `kosha_cache_evictions_total{cache="nvme"}`
like any other explicit eviction. Its own counters are per pod, in the `deletion_purge` block of
`GET /v1/stats`:

| Field | Meaning |
| --- | --- |
| `enabled`, `interval_ms` | Whether the purge runs on this pod, and how often |
| `caught_up` | Whether a pass since boot has read the deletion records to the end |
| `last_pass_at_ms`, `watermark_ms` | When the last pass started (Unix ms, this pod's clock), and the newest `requested_at_ms` among the records handled |
| `pending_sweeps` | Subjects waiting for another sweep: the repeat sweep after a deletion, or one that left something behind or failed |
| `last_error` | The first error of the last pass, `null` when it had none |
| `kosha_deletion_purge_passes_total` | Passes run |
| `kosha_deletion_purge_records_total` | Deletion records handled for the first time |
| `kosha_deletion_purge_sweeps_total` | Sweeps of one namespace or tenant, first and repeat sweeps together |
| `kosha_deletion_purge_segment_dirs_removed_total`, `kosha_deletion_purge_bytes_removed_total` | Segment directories removed from the local disk, and their bytes |
| `kosha_deletion_purge_parsed_segments_dropped_total` | Parsed segments dropped from memory |
| `kosha_deletion_purge_deferred_total` | Directories or namespaces a sweep left for a later one (written to within the last 5 s, pinned, being fetched, or held by a writer's unpublished state) |
| `kosha_deletion_purge_errors_total` | Passes or sweeps that failed (control store unreadable, a file that could not be removed) |
| `kosha_deletion_resume_attempts_total`, `kosha_deletion_resume_completed_total` | Automatic retries of pending namespace records on a writer pod (`KOSHA_DELETION_RESUME_INTERVAL_MS`), and how many completed the record |
| `kosha_deletion_resume_fenced_total` | Automatic retries skipped because the namespace's writer lease could not be taken (another writer pod holds it, or the lease store could not be reached) |

Each cache keeps these figures per tenant (#342): the tenant is the first component of the
`<tenant>/<name>` namespace id, and bare namespace ids from before tenants existed count as
one shared `_legacy` tenant. Without tenant labels each `kosha_cache_*` family has one series
per cache, summed over tenants; with `KOSHA_METRICS_TENANT_LABELS=on` it has one series per
cache and tenant instead (see [Tenant labels](#tenant-labels)). The labelled NVMe evictions add
up to the unlabelled count.

## Namespace labels

`KOSHA_METRICS_NAMESPACE_LABELS=on` adds a `namespace` label to
`kosha_search_requests_total`, and to no other metric. The label value is the internal,
tenant-qualified namespace name (`tenant/namespace`).

The trade-off:

- **Cardinality.** Each namespace adds up to four series. Fine for tens of namespaces; for
  thousands it inflates Prometheus memory and query cost.
- **Cross-tenant leakage.** Everyone who can read `/metrics` sees every tenant's namespace
  names and request rates. Pair it with `KOSHA_METRICS_AUTH=admin` if scrapers are not
  trusted with that.
- **Cost.** Without the label, recording a search is a few atomic adds. With it, each search
  also takes a shared read lock to find its series (and a write lock the first time a
  namespace is seen).

Per-namespace dashboards are tracked separately (#214).

## Tenant labels

`KOSHA_METRICS_TENANT_LABELS=on` adds a `tenant` label to `kosha_tenant_rejections_total`
and to the `kosha_cache_bytes`, `kosha_cache_hits_total`, `kosha_cache_misses_total` and
`kosha_cache_evictions_total` families, and to no other metric. The same trade-off as
namespace labels applies: one series per tenant and reason (or cache), every scraper sees
tenant ids, and recording a rejection takes the family's lock to find its series. Rejections
are rare next to searches, so the cost is off the hot path; the cache figures are kept by the
caches regardless and only read per scrape.

## Overhead

Recording is atomic adds only, with no locks on the search path (unless namespace labels are
on). The histograms are fixed-bucket atomic counters rather than `prometheus-client`'s
mutex-guarded histogram. Rendering, done only per scrape, takes the queue's mutex once to
read its depth.

## Checking the output

    curl -s localhost:8080/metrics | promtool check metrics

CI runs `promtool check metrics` against output captured from the
`metrics::tests::dump_exposition_for_promtool` test.
