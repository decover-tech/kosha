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

    docker build -t kosha:latest .
    docker run --rm -p 8080:8080 kosha:latest
    curl localhost:8080/healthz   # -> ok

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
