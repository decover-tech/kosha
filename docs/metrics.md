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

## Access

| Setting | Effect |
| --- | --- |
| default | Unauthenticated, like `/healthz` and `/readyz` |
| `KOSHA_METRICS_AUTH=admin` | Requires the `KOSHA_ADMIN_API_KEY` credential (`Authorization: Bearer <key>` or `X-Api-Key`); 403 without it. With no admin key configured, every scrape is refused |
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
| `kosha_search_requests_total` | counter | `outcome` | Search requests (`POST` and `GET`, every route form) by outcome. `ok`: HTTP 200. `rejected`: shed with HTTP 429, because the scoring queue was full (`KOSHA_SEARCH_QUEUE_DEPTH`) or the request hit a hydration or memory ceiling. `timeout`: shed with HTTP 429 after waiting `KOSHA_SEARCH_QUEUE_TIMEOUT_MS` for a scoring slot. `error`: any other status, including 4xx client errors |
| `kosha_search_duration_seconds` | histogram | none | Server-side wall-clock time of searches with outcome `ok`, from the handler's start to the response being ready. Includes result-cache hits |
| `kosha_search_phase_seconds` | histogram | `phase` | Time a scored search spent in each phase. `queue`: waiting for a scoring slot. `hydrate`: making the segment files it needs local (near zero when warm). `score`: the scoring pass. The same numbers as `queue_ms`, `hydrate_ms` and `score_ms` in the `search timing:` log line. Result-cache hits are not observed |
| `kosha_search_queue_depth` | gauge | none | Searches waiting for a scoring slot at scrape time |
| `kosha_search_queue_rejections_total` | counter | none | Searches the scoring queue shed, whether because it was full or because the wait timed out. Each is also counted in `kosha_search_requests_total` as `rejected` or `timeout` |
| `kosha_knn_degraded_segments_total` | counter | none | Segments a kNN search could not search by vector and skipped (the response's `knn_degraded_segments`, summed). Any increase means vector search silently returned fewer candidates |

### Writes

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `kosha_index_documents_total` | counter | none | Documents accepted by index and replace requests on this pod (the ingest pod; a query pod forwards writes) |
| `kosha_flush_total` | counter | none | Flush requests that succeeded. The idle-flush timer is not counted |
| `kosha_writer_fenced_total` | counter | none | Namespaces this writer fenced itself from because another writer took its writer lease (`KOSHA_WRITER_LEASE`). Any increase means two writers contended for a namespace; this one now refuses its writes |

### Caches and hydration

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `kosha_cache_bytes` | gauge | `cache` | `nvme`: bytes in the local read-through file cache (`KOSHA_DATA_DIR`). `segment`: approximate bytes of parsed segments held in memory (the `KOSHA_SEGMENT_CACHE_MAX_BYTES` ledger) |
| `kosha_cache_evictions_total` | counter | `cache` | `nvme`: evictions from the local file cache, one per file or directory (such as a segment) removed, whether by the `KOSHA_CACHE_MAX_BYTES` LRU or by an explicit eviction |
| `kosha_hydrations_total` | counter | `source` | Segment files a hydration needed, by where they came from. `s3`: fetched from the object store. `peer`: streamed from the replica holding the hydration lease. `local`: already in the local cache when the fetch ran. Ranged span reads of doc stores and postings are not counted. Stays at zero without S3 |
| `kosha_hydration_lease_outcome_total` | counter | `outcome` | Cross-replica hydration lease claims (S3 and Postgres builds with `POD_IP` set). `won`: this replica fetches the file. `streamed_from_peer`: another replica held the lease and served the bytes. `timeout`: the holder did not serve the bytes within the peer-fetch budget (or the fetch failed), and the file was fetched from S3 instead |
| `kosha_segment_checksum_failures_total` | counter | none | Hydrated segment files that failed their `_COMMIT` size or CRC check, refetch failures included. The same number as `segment_checksum_failures` in `GET /v1/stats` |

Values that already exist elsewhere in the server (cache bytes, NVMe evictions, checksum
failures, queue depth) are read when Prometheus scrapes, not counted twice.

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

## Overhead

Recording is atomic adds only, with no locks on the search path (unless namespace labels are
on). The histograms are fixed-bucket atomic counters rather than `prometheus-client`'s
mutex-guarded histogram. Rendering, done only per scrape, takes the queue's mutex once to
read its depth.

## Checking the output

    curl -s localhost:8080/metrics | promtool check metrics

CI runs `promtool check metrics` against output captured from the
`metrics::tests::dump_exposition_for_promtool` test.
