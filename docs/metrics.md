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
| `kosha_tenant_rejections_total` | counter | `reason` | Requests refused by a per-tenant limit or quota (#341): rates with 429, quotas with 403, the tenant body cap with 413 (#350). `reason`: `tenant_qps_exceeded`, `tenant_concurrency_exceeded`, `tenant_write_rate_exceeded`, `namespace_quota_exceeded`, `doc_quota_exceeded`, `storage_quota_exceeded`, `vector_dims_exceeded`, `body_too_large` or `tenant_queue_full`, the same code as the response body's `reason`. A search refused by a tenant limit never reaches the scoring queue, so it is not counted in `kosha_search_requests_total`. `tenant_queue_full` is a search the scoring queue shed because its tenant already had `KOSHA_SEARCH_QUEUE_DEPTH_PER_TENANT` searches waiting; it is also counted in `kosha_search_queue_rejections_total` and as `rejected` in `kosha_search_requests_total`. Every other reason stays at zero without a limits file or billing plan limits |
| `kosha_legacy_requests_total` | counter | `route` | Requests served on a deprecated unversioned route (`/index`, `/search`, ...; #350). Watch it reach zero before turning `KOSHA_LEGACY_ROUTES` off; see docs/api/versioning.md |

### Caches and hydration

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `kosha_cache_bytes` | gauge | `cache` (`tenant`) | `nvme`: bytes in the local read-through file cache (`KOSHA_DATA_DIR`). `segment`: approximate bytes of parsed segments held in memory (the `KOSHA_SEGMENT_CACHE_MAX_BYTES` ledger). `result`: bytes of cached `POST /search` responses (`KOSHA_RESULT_CACHE_MAX_BYTES`). Not reported for `corpus_stats` and `ceiling`, which are bounded by entry count |
| `kosha_cache_evictions_total` | counter | `cache` (`tenant`) | `nvme`: evictions from the local file cache, one per file or directory (such as a segment) removed, whether by the `KOSHA_CACHE_MAX_BYTES` limit or by an explicit eviction. A span directory dropped because a promotion landed its whole file is not counted: nothing became less local. `segment`, `result`: entries evicted to stay within the cache's budget. `corpus_stats`, `ceiling`: manifest versions evicted to stay within the cache's entry bound (a version replaced by a newer one of the same namespace is not counted) |
| `kosha_cache_hits_total` | counter | `cache` (`tenant`) | Lookups that found their entry (#342). `nvme`: a search's segment whose scoring files were already local, counted per segment checked by a hydration pass and, for a warm search served from the cached hydration verdict, per manifest segment. `segment`: a parsed segment found in memory. `result`: a search answered from the result cache. `corpus_stats`: a search whose namespace corpus statistics (BM25 totals) were cached for its manifest version. `ceiling`: the same for per-segment score ceilings |
| `kosha_cache_misses_total` | counter | `cache` (`tenant`) | Lookups that did not (#342). `nvme`: a search's segment that had to be hydrated. `segment`: a segment opened and parsed. `result`: a cacheable search that ran. `corpus_stats`, `ceiling`: the manifest version's entry had to be built |
| `kosha_hydrations_total` | counter | `source` | Segment files a hydration needed, by where they came from. `s3`: fetched from the object store. `peer`: streamed from the replica holding the hydration lease. `local`: already in the local cache when the fetch ran. Ranged span reads of doc stores and postings are not counted. Stays at zero without S3 |
| `kosha_hydration_lease_outcome_total` | counter | `outcome` | Cross-replica hydration lease claims (S3 and Postgres builds with `POD_IP` set). `won`: this replica fetches the file. `streamed_from_peer`: another replica held the lease and served the bytes. `timeout`: the holder did not serve the bytes within the peer-fetch budget (or the fetch failed), and the file was fetched from S3 instead |
| `kosha_segment_checksum_failures_total` | counter | none | Hydrated segment files that failed their `_COMMIT` size or CRC check, refetch failures included. The same number as `segment_checksum_failures` in `GET /v1/stats` |
| `kosha_span_writes_total` | counter | `outcome` | Fetched byte ranges (kNN postings, term postings, page documents, sidecar headers) by what became of their write into the span cache. `written`: written by the background span writer. `dropped`: the writer's queue (256 MiB) was full; the span was held in memory by the request that fetched it and not cached. `inline`: queue and held cap (1 GiB) both full; written on the fetching thread (a scoring thread for kNN), which brings back the disk stall this writer removes. Expected 0 at target load: **alert on any increase**, it means the cache disk is far behind the fetch rate. `failed`: a write that did not complete (it invalidates the posting presence cache). The same numbers as `span_writes` in `GET /v1/stats` |
| `kosha_span_writes_dropped_bytes_total` | counter | none | Bytes of the `dropped` span writes above |
| `kosha_span_write_queue_bytes` | gauge | none | Span bytes queued for the background writer (at most 256 MiB, plus never-dropped header spans) |
| `kosha_span_held_bytes` | gauge | none | Bytes of dropped span writes held in memory by in-flight requests (at most 1 GiB) |

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
| `kosha_deletion_resume_attempts_total`, `kosha_deletion_resume_completed_total` | Automatic retries of pending namespace, branch and tenant records on a writer pod (`KOSHA_DELETION_RESUME_INTERVAL_MS`), and how many finished the storage reclaim |
| `kosha_deletion_resume_fenced_total` | Automatic retries skipped because the namespace's writer lease could not be taken (another writer pod holds it, or the lease store could not be reached) |
| `pod_id`, `require_acks` | This pod's id in the pod registry, and whether deletions it records wait for acknowledgements (`KOSHA_DELETION_REQUIRE_ACKS`) |
| `kosha_deletion_acks_written_total` | Acknowledgements this pod wrote (one per record with `acks_required`, after a clean sweep of its subject) |
| `kosha_deletion_records_finalized_total` | Records this pod saw complete once their acknowledgements were in |
| `kosha_deletion_heartbeat_errors_total` | Pod-registry heartbeats that failed (control store unreachable); a pod that cannot heartbeat stops being waited for after `KOSHA_POD_LIVENESS_MS` |

Namespace exports (`KOSHA_EXPORTS`, #347) add no family to `/metrics` either. Their per-pod
counters are in the `exports` block of `GET /v1/stats`:

| Field | Meaning |
| --- | --- |
| `enabled`, `running` | Whether exports are served, and how many this pod is writing now (at most 2) |
| `kosha_exports_started_total` | Exports started on this pod (`POST .../exports`) |
| `kosha_exports_completed_total`, `kosha_exports_failed_total` | Exports this pod finished, and those that failed (their objects are deleted) |
| `kosha_exports_expired_total` | Completed exports the sweep expired (objects deleted) after `KOSHA_EXPORT_TTL_SECS` |
| `kosha_exports_deleted_total` | Exports deleted by their owner or with their namespace or tenant |

Each cache keeps these figures per tenant (#342): the tenant is the first component of the
`<tenant>/<name>` namespace id, and bare namespace ids from before tenants existed count as
one shared `_legacy` tenant. Without tenant labels each `kosha_cache_*` family has one series
per cache, summed over tenants; with `KOSHA_METRICS_TENANT_LABELS=on` it has one series per
cache and tenant instead (see [Tenant labels](#tenant-labels)). The labelled NVMe evictions add
up to the unlabelled count.

### Usage metering

Present only on a pod with `KOSHA_USAGE_METERING=on` (#348). Each pod reports its own pipeline:
the events it produced, its spool, and its deliveries to the usage store. The same counters (since
process start) are in `pipeline` of `GET /v1/admin/usage`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `kosha_usage_events_emitted_total` | counter | none | Usage events this pod produced: searches, writes, deletes and snapshots. Spooled plus dropped |
| `kosha_usage_events_spooled_total` | counter | none | Events appended to the local spool |
| `kosha_usage_events_delivered_total` | counter | none | Events the usage store accepted (flushed). A file resent after a crash counts again here, not in the store |
| `kosha_usage_events_dropped_total` | counter | none | Events dropped because the spool was at `KOSHA_USAGE_SPOOL_MAX_BYTES` or unwritable. **Alert on any increase**: those events are lost for billing |
| `kosha_usage_spool_malformed_lines_total` | counter | none | Spool lines that did not parse when delivered (a write cut short by a crash) |
| `kosha_usage_sink_errors_total` | counter | none | Flushes that failed (usage store unreachable, spool file error). Events stay spooled and are retried every `KOSHA_USAGE_FLUSH_MS` |
| `kosha_usage_spool_bytes` | gauge | none | Bytes of events waiting in the spool. Growing steadily means delivery is failing |
| `kosha_usage_last_delivery_timestamp_seconds` | gauge | none | Unix time of the last flush that delivered the whole spool; 0 before the first |
| `kosha_usage_snapshot_runs_total` | counter | none | Runs of the snapshot tick (`KOSHA_USAGE_SNAPSHOT_TICK_MS`, writer pods only) |
| `kosha_usage_snapshots_total` | counter | `meter` | Hourly namespace snapshots emitted, `stored_bytes` or `vector_dimensions` |
| `kosha_usage_snapshot_failures_total` | counter | none | Namespace snapshots a tick could not take (object listing or segment footer read failed); retried next tick |
| `kosha_usage_footer_reads_total` | counter | none | Segment footers read for the `vector_dimensions` meter. Each segment's footer is read once; a rate that keeps up with the snapshot rate means the cache is not working |

### Billing

Present only on a pod with `KOSHA_BILLING=on` (#349).

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `kosha_billing_webhook_events_total` | counter | `outcome` | Stripe webhook deliveries. `processed`: applied (including events billing ignores). `duplicate`: an event id processed before. `bad_signature`: refused with 400 (missing, stale or wrong `Stripe-Signature`; a steady rate means a wrong `KOSHA_STRIPE_WEBHOOK_SECRET`). `invalid`: signed but not an event. `failed`: a store or provider error, answered 503 so Stripe delivers it again |
| `kosha_billing_scheduler_runs_total` | counter | `result` | Scheduler ticks (`KOSHA_BILLING_SCHEDULER=on`). `ok`, `failed` (a step reported an error; see the log), `not_leader` (another pod holds the lease) |
| `kosha_billing_scheduler_last_run_timestamp_seconds` | gauge | none | Unix time of the last tick this pod ran as the leader; 0 if none. Across pods, alert when the maximum is older than a few intervals |
| `kosha_billing_notices_total` | counter | `outcome` | Dunning notices: `sent`, or `failed` (offered again on the next pass) |
| `kosha_billing_payment_retries_total` | counter | none | Payments retried by dunning |
| `kosha_billing_payments_collected_total` | counter | none | Invoices a dunning retry collected |
| `kosha_billing_catalog_reloads_total` | counter | `outcome` | Plan catalog reloads: `applied`, or `rejected` (invalid, or drops a plan an account is on; the old catalog stays) |
| `kosha_billing_refresh_failures_total` | counter | none | Standings refreshes that failed; the pod enforces its previous standings |

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

`KOSHA_METRICS_TENANT_LABELS=on` adds a `tenant` label to `kosha_legacy_requests_total` and `kosha_tenant_rejections_total`
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

For example alert rules, an internal tenant dashboard recipe, and incident runbooks, see
[Managed-service operations](operations/managed-service.md).

## Checking the output

    curl -s localhost:8080/metrics | promtool check metrics

CI runs `promtool check metrics` against output captured from the
`metrics::tests::dump_exposition_for_promtool` test.
