# Kosha

A storage-disaggregated search engine: **S3 is the source of truth, local NVMe
SSD is a transparent cache, and compute nodes are disposable.** Kosha is being
built to replace Elasticsearch/OpenSearch for Decover's search workloads, and
is intended to be reusable as a general-purpose, schema-driven search service.

See [DESIGN.md](DESIGN.md) for the full architecture.

> **Status: early development (Phase 1 — BM25 lexical search only).**
> The crate layout below is a skeleton; no query/index functionality exists
> yet. Vector/ANN retrieval, RRF fusion, and rerank are Phase 2
> (DESIGN.md §3.1).

## Repository layout

    crates/
      kosha-core      shared types (namespace/segment ids)
      kosha-segment   segment file format (DESIGN.md §6.2)        — Epic 2
      kosha-write     WAL, buffer, flush, compaction (§7)         — Epic 3
      kosha-cache     NVMe SSD read-through cache (§9)            — Epic 4
      kosha-query     BM25 read path (§8)                         — Epic 5
      kosha-control   namespace registry + manifest store (§5)    — Epic 6
      kosha-server    node binary: ingest/query/compaction + API  — Epic 8
    proto/            protobuf definitions (dsearch.proto)        — Epic 1
    docs/             development and integration guides
    DESIGN.md         architecture document (v1 draft)

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
