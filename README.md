# Kosha

[![CI](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml/badge.svg)](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml)
[![rustfmt](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml/badge.svg?job=fmt)](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml)
[![test](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml/badge.svg?job=test)](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml)

A storage-disaggregated search engine: **S3 is the source of truth, local NVMe
SSD is a transparent cache, and compute nodes are disposable.** Kosha is being
built to replace Elasticsearch/OpenSearch and is intended to be reusable as a general-purpose, schema-driven search service.

See [DESIGN.md](DESIGN.md) for the full architecture.

> **Status: Phase 1 complete — BM25 lexical + kNN/ANN search implemented.**
> All seven crates have functional BM25 indexing, query, filtering,
> aggregation, and HNSW vector search. RRF fusion and rerank are Phase 2
> (DESIGN.md §3.1).

## Supported features

### Query

| Feature | Notes |
| --- | --- |
| BM25 lexical search | Tunable `k1` / `b` per request (defaults 1.2 / 0.75) |
| `operator: and` / `or` | AND (default) requires every query term; OR scores the union |
| Block-max WAND | Skip-list postings, rarest-first probe ordering, leapfrog AND join |
| `match_phrase` | Positional postings, configurable `slop` |
| `wildcard` | Per-field pattern match, case-insensitive by default |
| kNN / ANN vector search | Cluster-and-posting on-disk vector index with centroid probing and triangle-inequality lower bounds; global probe budget; flat/HNSW fallback for older segments |
| Hybrid retrieval | A `knn` clause and `query_text` in one request; vector and BM25 hits merged per segment |
| Filters | `term`, `terms`, `range` (`gte`/`gt`/`lte`/`lt`), `bool` (`must` / `must_not` / `should` / `minimum_should_match`), `match_all` |
| kNN-scoped filters | `knn.filter` restricts vector candidates; the top-level `filter` still governs the merged result set |
| Sorting | Multi-field `sort` over stored fields, plus `_id` |
| Pagination | `from` / `max_results`, and an OpenSearch-style `search_after` cursor |
| Highlighting | Per-field, with configurable `pre_tags` / `post_tags` |
| Aggregations | `terms`, `cardinality`, `composite` |
| Total-hit accounting | Capped counting with an `eq` / `gte` relation (`track_total_hits`-style), overridable per query via `exact_total_hits` / `total_hits_cap` |
| Degradation signal | `knn_degraded_segments` reports segments whose vector search failed, so a 200 with silently missing neighbors is visible to the caller |

### Indexing and writes

- Bulk document indexing, with field types `Text`, `Keyword`, `Integer`, `Float`, `Date`, `Boolean`, `Vector`.
- Upsert by document id — segments holding a prior version of an id are rewritten.
- Delete by query (tombstone-based) and a document `exists` check.
- Write-ahead log for buffered documents, replayed on restart.
- Auto-flush at a configurable document threshold (`KOSHA_FLUSH_THRESHOLD`), plus an explicit `flush`.
- Size-tiered compaction with a cap on merged-segment size (5 GiB default), triggered by the admin endpoint or the compaction CronJob.

### Storage and caching

- S3 as the source of truth; any S3-compatible endpoint works (MinIO locally, path-style supported).
- Local NVMe read-through cache, size-bounded with LRU eviction.
- Lazy segment loading — the doc store, filter columns, and postings are read on demand, with ranged GETs for doc-store pages instead of whole-blob fetches.
- In-memory parsed-segment cache governed by a live-bytes ledger and an admission gate, with a per-request hydration byte budget and bounded hydration concurrency.
- Posting-blob presence cache, postings cache, and vector-postings cache.
- Whole-response result cache for `POST /search` (bypassable per query with `no_cache`).
- Cross-replica hydration leases in Postgres: one pod fetches a cold segment from S3, its peers stream the bytes from it over `GET /internal/segment/...` rather than stampeding S3.
- Bloom filters over terms and filter fields, so a segment that cannot match is skipped without being read.
- Namespace warmup on boot, gated behind `/readyz` so traffic is not routed to a cold pod.

### Control plane and operations

- Namespace registry and manifest store, in-memory by default or Postgres-backed (`postgres` feature + `DATABASE_URL`), with compare-and-swap manifest publishes.
- Multi-tenancy: every API key maps to a tenant prefix that scopes all namespace access.
- Read/write split: query pods forward every mutating route to `KOSHA_INGEST_HOST` and serve reads locally.
- Backpressure controls: max concurrent searches, search queue depth and timeout, hydration concurrency, admission timeout.
- `GET /healthz` (liveness), `GET /readyz` (readiness), `GET /v1/stats` and per-namespace stats.
- Admin endpoints: create API key, rebuild filter blooms, backfill offset tables, compact a namespace, import a namespace.
- Schema migrations via `kosha-server migrate`.

### API and clients

- HTTP/JSON `/v1` API — `documents`, `search`, `flush`, `delete`, `exists`, `stats` — with the Phase 1 unversioned routes still served for backward compatibility.
- `proto/kosha/v1/kosha.proto` is the canonical API contract and the source for generated stubs and the OpenAPI spec.
- `kosha` CLI: health, index, search, flush, delete, stats, admin commands, named profiles, `--json` output, and a `kosha curl` escape hatch.
- OpenSearch-compatible Python client: `search`, `index`, `bulk`, `count`, `update`, `delete_by_query`, `update_by_query`, `scroll`, plus `indices` and `tasks` namespaces, translating the ES query DSL to Kosha's native shape.

### Not supported yet

- RRF fusion and reranking (Phase 2, DESIGN.md §3.1).
- Configurable analyzers — tokenization is fixed (whitespace split, ASCII-punctuation trim, lowercase). The `analyzer` field exists in the schema proto but is not honored.
- gRPC — the service is defined in the proto, but HTTP/JSON is the only implemented transport.
- `kosha-vector-spfresh` (SPFresh/SPANN with LIRE rebalancing) is a standalone, benchmarked prototype; it is not wired into the segment format or the query path.

## Repository layout

    crates/
      kosha-core      shared types, data model, filter/query DSL  — Epic 2
      kosha-segment   segment format: inverted idx, doc store,    — Epic 2
                     filter columns, vector store, HNSW graph
      kosha-write     document buffer + flush-to-segment          — Epic 3
      kosha-cache     NVMe SSD read-through cache (§9)            — Epic 4
      kosha-query     BM25 scorer, kNN/ANN search, aggregations,  — Epic 5
                     wildcard, match phrase, filtering
      kosha-control   in-memory namespace + manifest store        — Epic 6
      kosha-server    HTTP API (healthz, index, search, stats,    — Epic 8
                     delete, flush)
      kosha-cli       Remote `kosha` CLI (profiles, search,       — Epic 12
                     index, curl escape hatch)
    clients/
      python/kosha_client  OpenSearch-compatible Python client    — Epic 11
                           (spec → codegen → thin client)
      python/README.md     how the client is structured
    proto/
      buf.yaml             Buf module config
      kosha/v1/kosha.proto  Canonical API contract (source of truth) — Epic 1
    gen/                   Generated stubs + OpenAPI spec (git-ignored)
    tools/codegen/         Code generation scripts and docs
    docs/                  Development and integration guides
    DESIGN.md              Architecture document (v1 draft)

## Quickstart (test as a customer would)

```bash
# 1. Start Kosha locally
docker compose up --build

# 2. Install the CLI (or use cargo run -p kosha-cli -- …)
cargo install --path crates/kosha-cli

# 3. In another terminal, explore the index
export KOSHA_HOST=http://localhost:8080
export KOSHA_API_KEY=sk-kosha-dev

kosha health
kosha index -n quickstart-demo --file crates/kosha-cli/examples/docs.jsonl
kosha flush -n quickstart-demo
kosha search -n quickstart-demo "breach"
kosha stats -n quickstart-demo
```

See [crates/kosha-cli/README.md](crates/kosha-cli/README.md) for profiles
(`~/.kosha/config.toml`), `--json` output, and the `kosha curl` escape hatch.

The Python client still works the same way for application code:

```python
from kosha_client import KoshaClient

client = KoshaClient(
    hosts="https://app.kosha.io",
    api_key="sk-acme-corp-xxx",
)
client.ping()  # True
```

Both the CLI and the Python client respect `KOSHA_HOST` and `KOSHA_API_KEY`.
`scripts/quickstart.py` remains as a scripted alternative to the CLI flow.

## Development

Prerequisites: Rust stable (via [rustup](https://rustup.rs) — the repo pins the
channel via `rust-toolchain.toml`), Docker, and optionally `pre-commit`.

    cargo build                                # build the workspace
    cargo test                                 # run unit tests
    cargo fmt --all -- --check                 # formatting (CI gate)
    cargo clippy --all-targets -- -D warnings  # linting (CI gate)
    pre-commit install                         # optional: run gates on commit

### Local S3 (MinIO)

    docker compose up -d minio createbuckets

MinIO serves an S3-compatible API on `localhost:9000` (web console on `:9001`,
credentials `kosha` / `kosha-dev-secret`) with the `dsearch-dev` bucket
auto-created.

### Run with Docker

Pre-built multi-arch images are published to GHCR on every merge to `main`
(`:main`) and for every `v*` tag (`:0.2.5`, `:0.2`, `:latest`):

    docker pull ghcr.io/decover-tech/kosha:latest
    docker run --rm -p 8080:8080 ghcr.io/decover-tech/kosha:latest
    curl localhost:8080/healthz   # -> ok

Or build locally:

    docker build -t kosha:latest .
    docker run --rm -p 8080:8080 kosha:latest

Or bring up the whole local stack (MinIO + server): `docker compose up --build`.

### Run alongside the Decover backend (Tilt)

The backend repo's Tilt setup builds and deploys `kosha` into the local k8s
cluster as `kosha-service` (`:8080` HTTP, `:50051` gRPC). See
[docs/local-development.md](docs/local-development.md) — including how the
OpenSearch → Kosha swap will work once the read/write path lands.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache-2.0 (see [LICENSE](LICENSE)). The final open-source license choice is
confirmed before the v0.1 release (implementation plan step 126).
