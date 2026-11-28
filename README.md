# Kosha

[![CI](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml/badge.svg)](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**Apache-2.0 search engine in Rust. S3 is the source of truth, local NVMe is a cache, and compute nodes are disposable.** BM25, kNN, and hybrid search behind an Elasticsearch-shaped API.

DecoverAI built Kosha to replace Elasticsearch. It is not a faster Lucene cluster. Durable segments live in object storage, a query node is a cache in front of that bucket, and losing a node means warming another cache. Idle data costs storage bytes, not a hot shard.

Architecture: [DESIGN.md](DESIGN.md). What the server actually promises, and what it does not: [docs/guarantees.md](docs/guarantees.md).

## Why it exists

Elasticsearch and OpenSearch fuse compute and storage. Every data node holds its shards, scaling queries means moving those shards, and an idle tenant still occupies heap and disk. Recovery is a cluster operation.

Kosha splits those roles. Segment files in S3 are the only copy that has to survive. Each process keeps a read-through NVMe cache. Adding a query node is starting a process near the bucket. The API is close to the Elasticsearch JSON that applications already send, translated by the [Python client](clients/python); it is not a full Query DSL. The gaps are listed in [the compatibility matrix](docs/compat/opensearch.md) and under [What is not supported yet](#what-is-not-supported-yet).

## 60-second quickstart

The published image is [`decoverai/kosha`](https://hub.docker.com/r/decoverai/kosha) (`:latest` and `:0.1.0` for releases, `:main` on every merge to `main`). The server refuses to start until it has an API key. This container keeps segments on its own disk and the catalog in memory: stop the container and the data is gone. For MinIO, Postgres, and a restart-safe stack, use [the development compose file](docs/development.md#local-s3-minio).

```bash
docker run --rm -p 8080:8080 \
  -e KOSHA_API_KEY=sk-kosha-dev \
  -e KOSHA_ROLE=standalone \
  decoverai/kosha:latest
```

In another shell:

```bash
curl -s localhost:8080/healthz

curl -s -X POST localhost:8080/v1/namespaces/demo/documents \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer sk-kosha-dev' \
  -d '{"documents":[
    {"id":"1","fields":[{"name":"title","field_type":"Text","value":"breach of contract"}]},
    {"id":"2","fields":[{"name":"title","field_type":"Text","value":"merger antitrust review"}]}
  ]}'

curl -s -X POST localhost:8080/v1/namespaces/demo/flush \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer sk-kosha-dev' \
  -d '{}'

curl -s -X POST localhost:8080/v1/namespaces/demo/search \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer sk-kosha-dev' \
  -d '{"query_text":"breach","max_results":10}'
```

The search body returns a `results` array. Each hit has `doc_id` and `score`. The same flow with the CLI or the Python client is [docs/quickstart.md](docs/quickstart.md).

## Benchmarks

Numbers below are copied from [scripts/bench/bm25_scale/RESULTS.md](scripts/bench/bm25_scale/RESULTS.md). They are not a head-to-head against Elasticsearch. Methodology, hardware, dataset, and the commands to rerun them: [docs/benchmarks.md](docs/benchmarks.md).

Kosha, warm, 8 QPS, top 10, 30 minutes. BM25 is 10M MS MARCO passages. kNN is 10M documents × 1024-d f32. The bench workflow's VM is an m7i.8xlarge (32 vCPU, 128 GB); the November rows do not restate the instance type themselves.

| Workload | p50 | p90 | p99 | Source |
| --- | ---: | ---: | ---: | --- |
| BM25, result cache off | 29.0 ms | 63.5 ms | 170 ms | GitHub Actions run [44892212058](https://github.com/decover-tech/Kosha/actions/runs/44892212058), 2026-11-20/21 |
| BM25, result cache on | 1.7 ms | 12.1 ms | 66 ms | same run |
| kNN, result cache on | 88.0 ms | 111 ms | 222 ms | run [44876437446](https://github.com/decover-tech/Kosha/actions/runs/44876437446). Recall@10 vs exact search on 100 queries: mean 0.90, median 1.0 |

OpenSearch has one recorded BM25 run in that file, on a **different** corpus (synthetic 10M docs, ~9 GB, not MS MARCO) on 2× `r8g.large.search`: warm **12.3 / 24.8 / 62.1 ms**. Kosha did not finish that run. Do not read the two tables as a comparison.

A matched Kosha vs OpenSearch (or Elasticsearch) run on one corpus is **not measured**. The local harness is [scripts/bench/compare](scripts/bench/compare/README.md). Its result cells are placeholders until you run it.

There is no measured "Nx cheaper than OpenSearch" result in this repo. [DESIGN.md](DESIGN.md) §16 prints per-GB planning figures and labels them as targets, not measurements.

## Clients and docs

| | | |
| --- | --- | --- |
| Server image | [Docker Hub `decoverai/kosha`](https://hub.docker.com/r/decoverai/kosha) | `docker pull decoverai/kosha:latest` |
| Python | [PyPI `kosha-client`](https://pypi.org/project/kosha-client/) | `pip install kosha-client` |
| TypeScript (preview) | [`clients/typescript`](clients/typescript) | `npm install @decover/kosha` |
| Go (preview) | [`clients/go`](clients/go) | `go get github.com/decover-tech/kosha/clients/go` |
| CLI | not on crates.io yet | `cargo install --path crates/kosha-cli` |

Which image name is the one CI publishes, and why an older `ravidecoverai/kosha` tag still exists: [docs/development.md](docs/development.md#which-docker-image).

| Topic | |
| --- | --- |
| Feature reference (query, indexing, analyzers, vectors) | [docs/reference/features.md](docs/reference/features.md) |
| HTTP API, including delete | [docs/reference/http-api.md](docs/reference/http-api.md) |
| Elasticsearch / OpenSearch compatibility | [docs/compat/opensearch.md](docs/compat/opensearch.md) |
| Moving an index | [docs/migration/from-elasticsearch.md](docs/migration/from-elasticsearch.md) |
| Configuration | [docs/configuration.md](docs/configuration.md) |
| Guarantees and limits | [docs/guarantees.md](docs/guarantees.md) |
| Authorization, WAL, upgrade order | [docs/reference/auth.md](docs/reference/auth.md) |
| Exports, schema changes | [docs/reference/exports.md](docs/reference/exports.md), [docs/reference/schema.md](docs/reference/schema.md) |
| Build, test, Docker, layout | [docs/development.md](docs/development.md) |
| How DecoverAI replaced Elasticsearch | [docs/blog/how-we-replaced-elasticsearch.md](docs/blog/how-we-replaced-elasticsearch.md) |
| Kubernetes | [k8s/README.md](k8s/README.md) |

Multi-tenant cells install from [`k8s/saas`](k8s/saas). A single-customer stack installs from `k8s/base`. Upgrades have to follow the manifest and WAL order in [docs/reference/auth.md](docs/reference/auth.md#s3-wal-kosha_wal_modes3). See [docs/deployment-model.md](docs/deployment-model.md).

## What works today

BM25 (including phrases, fuzzy, per-field boosts, and block-max WAND), filtered kNN, hybrid reciprocal rank fusion, filters, highlighting, a bounded set of aggregations, compaction, and a Postgres control plane. The tables, request shapes, and limits are [docs/reference/features.md](docs/reference/features.md).

### What is not supported yet

- A full Elasticsearch Query DSL. `script_score`, `query_string`, nested documents, and a lot of the rest are rejected by the Python client rather than silently dropped. The matrix is [docs/compat/opensearch.md](docs/compat/opensearch.md).
- gRPC. The proto describes it; HTTP/JSON is the transport that runs.
- Several analyzer features: synonyms, n-grams, non-English stemmers, full Unicode case folding.
- A namespace-level centroid index. Each segment still has its own vector postings.
- The longer list, including rerank and fuzzy limits, is in [docs/reference/features.md](docs/reference/features.md#not-supported-yet).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
