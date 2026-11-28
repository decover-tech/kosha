# How we replaced Elasticsearch

DecoverAI built Kosha to take the place of Elasticsearch and OpenSearch for product search. This repository is that engine: an Apache-2.0 search service in Rust. The application cutover is not in this tree. What is here is the architecture, the tradeoffs, the features that are still missing, and the only benchmark numbers the repository actually contains.

It is not a drop-in Elasticsearch cluster, and it is not a claim that Kosha is cheaper by some factor. The design doc prints per-gigabyte planning prices and then says they are targets, not measurements. Read those as targets.

One page is easy to misread on a first visit. [docs/local-development.md](../local-development.md) is a note about a local Tilt overlay. It still says that overlay had not been pointed at Kosha, and it still describes a lexical-only phase. The engine has moved past that note: BM25, kNN, and hybrid search are implemented. Use the [feature reference](../reference/features.md) for what runs, and this post for why the storage model looks like this.

## The problem we actually had

OpenSearch gave us the retrieval shape the product needed: BM25, cosine over embeddings, filters, `search_after`, a fusion of the lexical and vector candidate lists, and a rerank pass. The operational model came with it.

A data node holds the shards it serves. Scaling query traffic means moving or copying those shards. An idle tenant still occupies heap and disk, because the shard is the unit of capacity. Losing a node is a cluster event: relocation, a window of reduced replication, an operator. For a corpus that is large, bursty, and mostly cold, that is a bad fit. Most of the index is not being queried most of the time, and we were still paying for it as hot storage.

The goal was not a faster Lucene. The goal was an index that treats object storage as the only durable copy and treats a compute node as disposable.

## The shape of the system

Three roles, and only one of them is the source of truth.

**S3 holds immutable segment files.** A namespace (the isolation unit, roughly an index) is a versioned manifest: an ordered list of segments. A segment is a directory of files: inverted index, document store, filter columns, blooms, and, when the namespace has vectors, a cluster-and-posting ANN index. Publication is a compare-and-swap of the manifest. Query nodes that have not seen the new manifest yet keep serving the previous one.

**Local NVMe is a cache.** A search that needs a file and does not have it fetches that file (or a byte range) from S3 and keeps it locally. Cache loss is latency. It is not data loss, as long as the durable copy is the object store and the write-ahead log is not sitting only on the node that just died. The default WAL is local. `KOSHA_WAL_MODE=s3` puts the log in the bucket so an ingest-node failure can replay it. That mode waits for a PUT before acknowledging a write. The latency table in the auth doc is from a simulated PUT, not from a timed run against S3. [docs/guarantees.md](../guarantees.md) says the real-S3 number is not measured.

**Compute is replaceable.** A query process can be killed. Another one starts, reads the manifest from Postgres, and fills its cache from S3 as queries arrive (or preloads a namespace at boot). Ingest is not freely replaceable in the same way: one writer owns a namespace at a time, fenced by a lease. Two ingest processes must not append the same WAL.

The control plane is Postgres when `DATABASE_URL` is set, and an in-memory catalog when it is not. The in-memory catalog is what the single-container quickstart uses. It does not survive a restart. A deployment that has to is the compose stack: Postgres, MinIO or S3, and the server.

The HTTP API is JSON under `/v1`. The protobuf description also sketches gRPC. gRPC is not implemented. Clients send `Authorization: Bearer` or `X-Api-Key`. With no credential configured, the process exits unless `KOSHA_AUTH=disabled` is set on purpose.

## What "replaced Elasticsearch" means

DecoverAI's application used a slice of OpenSearch, not the whole product. Kosha implements that slice, and a fair amount around it, and rejects the rest in the client instead of pretending.

Implemented on the server, and described in [the feature reference](../reference/features.md):

- BM25, with per-request `k1` and `b`, AND/OR, per-field boosts, phrases with slop, fuzzy matching, and block-max WAND on the single-index path.
- Filtered kNN over a cluster-and-posting index, with an exact path when the filter is selective enough, and optional int8 quantization plus a full-precision rescore.
- Hybrid search by reciprocal rank fusion, including a weighted multi-query form.
- Filters (`term`, `terms`, `range`, `exists`, `prefix`, `glob`, `geo_distance`, boolean combinations), sort, `search_after`, highlighting, and aggregations (`terms`, `composite`, `histogram`, `date_histogram`, and the usual metrics, including percentiles).
- Compaction, copy-on-write branches, namespace sharding, and an optional external reranker.

The Python package `kosha-client` accepts Elasticsearch and OpenSearch request bodies and translates them. The TypeScript and Go SDKs speak `/v1` only. The translation matrix is generated from the conformance cases and checked in CI: [docs/compat/opensearch.md](../compat/opensearch.md). As of this writing the query DSL section is 10 supported, 6 partial, 13 rejected. Partial rows are the ones to read twice. `multi_match` is scored as `most_fields` for every type. `function_score` keeps the inner query and ignores the functions. A bare `match_all` matches nothing unless it is written as a filter. `indices.flush` on the compatibility client is a successful no-op; `indices.refresh` is the call that flushes.

Moving an existing index is [docs/migration/from-elasticsearch.md](../migration/from-elasticsearch.md). There is no server-side `_search` or `_bulk`. The compatibility is in the client.

## Tradeoffs

**Cold is a warmup, not a per-query tax we have fully characterized.** A query node with an empty cache has to fetch what the query touches. On the large MS MARCO runs, the number we publish is after that preload. turbopuffer's published "cold" is `disable_cache: true` on every query. Kosha has no equivalent switch, so a phase that runs after preload is a warm engine. The results file says so. Early attempts to benchmark a cold 10M namespace on a shared staging cluster were worse than a slow number: compaction ran the ingest pod out of memory, and cold hydration on several query replicas evicted pods and hurt unrelated tenants. Those failures are written up in the same results file, including the bugs they found. Quoting only the November warm row without that history would be a sales page.

**The durable path and the fast path are different disks.** S3 is durable and slow to start from. NVMe is fast and disposable. A deployment that leaves the WAL on local disk will lose acknowledged, unflushed writes if that disk goes away. A deployment that puts the WAL in S3 pays a PUT on the write path. Group commit amortizes that across writers. The bench for it, checked in under `scripts/bench/results/`, is a local fsync measurement and a simulated PUT. Do not quote it as an S3 latency.

**One writer.** Ingest for a namespace is single-threaded in the sense that one lease holder publishes. Query nodes scale out. Ingest does not, except by sharding the namespace into several, each with its own manifest. That is a real ceiling, and it is why the offline segment builder exists: the 10M vector load through the server was about 485 docs/s, and the offline builder did the text corpus at about 199k docs/s before attaching the segments.

**Compatibility is explicit and incomplete.** Unsupported DSL is a 400 from the client, or a documented divergence. A few compatibility methods still acknowledge a call they do not perform (`indices.put_mapping`, `indices.flush`). That is a bug relative to the expectation those names create, tracked as [issue #594](https://github.com/decover-tech/Kosha/issues/594), not a feature.

**The single-container demo is not the production storage layout.** `docker run decoverai/kosha` with an API key and no bucket uses the local disk and an in-memory catalog. It is the right way to see a search in a minute. It is the wrong mental model of a restart. The compose file is the small durable layout. The Kubernetes manifests are the multi-tenant one.

## What is not supported yet

From the feature reference and the compatibility matrix, the gaps that matter if you are coming from Elasticsearch:

- Scripted scoring, `query_string`, nested documents, parent/child, span queries, suggestions, and scroll. Use `search_after`.
- gRPC.
- Synonyms, n-grams, stemmers other than English, and full Unicode case folding. `standard@2` splits Han characters one per token.
- Fuzzy matching does not use block-max WAND, is not highlighted, and does not combine with a field list or a phrase in the same lexical query.
- Changing a native embedding model does not reindex the field. Late-interaction vectors are not produced by that embedding hook.
- There is no centroid index shared by every segment. A kNN query still considers each segment's centroids, under one probe budget.
- Delete-by-id is not a separate call. Delete takes a filter. Bulk `delete` actions in the Python client come back as item errors.
- SAML. The console speaks OIDC, Google, and GitHub.

A longer list is [in the feature reference](../reference/features.md#not-supported-yet).

## Benchmarks, without the slide

The numbers worth citing are in [docs/benchmarks.md](../benchmarks.md), copied from `scripts/bench/bm25_scale/RESULTS.md`.

On the bench VM (the workflow uses an `m7i.8xlarge`), 10M MS MARCO passages, 8 QPS, top 10, warm, November 2026, Actions run 44892212058:

- BM25, result cache off: **29.0 / 63.5 / 170 ms** (p50 / p90 / p99), 0 errors.
- BM25, result cache on: **1.7 / 12.1 / 66 ms**.

The cache-on median is a hit path. The cache-off p99 is almost entirely scoring, on queries that were already expensive at the median. It is not a queue.

kNN, 10M × 1024-d, warm, cache on, run 44876437446: **88.0 / 111 / 222 ms**, recall@10 mean 0.90 on 100 queries against brute force. The same run's fresh pod returned 263 responses with `429` before the warm phase. The warm-up of the ~249 GB vector working set was disk-write bound (about two hours).

An OpenSearch number does exist, and it is the wrong corpus to set beside those rows. In August 2026 a staging domain, 2× `r8g.large.search`, answered a synthetic 10M-doc Zipf corpus at warm **12.3 / 24.8 / 62.1 ms**. Kosha did not complete that comparison. The domain's OpenSearch version is not recorded. Elasticsearch has no checked-in result at all.

turbopuffer publishes full-text latencies for 1M documents. Several earlier Kosha write-ups compared a 10M run to those 1M figures. The results file corrects that. Their vector figures are for 10M documents and are vendor-reported. The results file states a warm gap of 6.3× at p50 and 8.2× at p99 (88 ms vs their published 14 ms, on Kosha's 1024-d f32 corpus). Their pages disagree on whether that published figure is 1024-d or 768-d f16. Quote the gap with that mismatch, or do not quote it.

To reproduce the scale run, use the benchmark workflow and [scripts/bench/bm25_scale/README.md](../../scripts/bench/bm25_scale/README.md). It needs the bench account's AWS credentials and a snapshot of the corpus. To reproduce a small side-by-side on one machine, use [scripts/bench/compare](../../scripts/bench/compare/README.md). That harness pins OpenSearch 2.18.0 and `decoverai/kosha:0.1.0`, generates a few thousand synthetic documents, and leaves the result as a placeholder in git until you run it. A few thousand documents will not confirm or refute the table above. They will show you that both engines are loaded with the same bytes and that `run_benchmark.py` records latency and top-k overlap.

## Cost

Idle namespaces are the reason the storage split exists: an idle namespace should cost its object-storage bytes and a catalog row, not a hot shard. That is an architectural argument. It is not a measured bill.

DESIGN.md §16 lists about $0.16–0.24/GB-month on the OpenSearch side and about $0.023/GB-month for S3 Standard, and latency targets of warm p50 ≤ 80 ms and p99 ≤ 300 ms. The paragraph under the table says those are directional planning targets, not measured benchmarks. The November BM25 cache-off median (29 ms) happened to land inside the read-latency target on that one workload. The design doc's dollar figures were not re-measured against an invoice.

`scripts/bench/render_report.py` can render an HTML page that multiplies public AWS list prices by a measured index size and by a duty cycle. The script says the output is an estimate, and the tree does not contain a filled-in report. Do not turn either document into "7× cheaper."

## Run it

The image CI pushes for public pulls is `decoverai/kosha`, not `ravidecoverai/kosha`. The latter namespace still has tags; its `:latest` was last updated on 2026-07-25. Release tags and the current `:main` are on `decoverai/kosha`. Details are in [docs/development.md](../development.md#which-docker-image).

```bash
docker run --rm -p 8080:8080 \
  -e KOSHA_API_KEY=sk-kosha-dev \
  -e KOSHA_ROLE=standalone \
  decoverai/kosha:latest
```

Then the curl sequence in the [README](../../README.md#60-second-quickstart). Python: `pip install kosha-client`. The server is Apache-2.0.

If you are migrating a real index, start with the compatibility matrix and the migration guide, and assume the first interesting query is one the matrix marks partial or rejected.
