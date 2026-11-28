# Development

Repository layout and how to build, test, and run Kosha locally. Moved from the README.
Configuration of every `KOSHA_*` variable is [configuration.md](configuration.md).

## Repository layout

    crates/
      kosha-core            shared types, data model, filter/query DSL, analyzers
      kosha-segment         segment format: inverted index, doc store, filter
                            columns, blooms, posting-based vector index
      kosha-vector-spfresh  vector clustering used to build vector.idx, plus an
                            incremental SPFresh/LIRE prototype
      kosha-write           WAL, per-namespace buffer, flush-to-segment, compaction
      kosha-cache           NVMe SSD read-through cache (DESIGN.md §9)
      kosha-query           BM25 + WAND, kNN/ANN, RRF fusion, filters, sorting,
                            highlighting, aggregations, segment cache
      kosha-control         namespace registry + manifest store (in-memory or
                            Postgres), hydration leases, SQL migrations
      kosha-server          HTTP /v1 API, auth, S3 upload/hydration, warmup,
                            compaction scheduler, `migrate`
      kosha-cli             remote `kosha` CLI (profiles, search, index, curl)
      kosha-build-segments  offline parallel segment builder
    clients/
      python/kosha_client   OpenSearch-compatible Python client
                            (spec → codegen → thin client)
      python/README.md      how the client is structured
      typescript/           TypeScript SDK (@decover/kosha)
      go/                   Go SDK (github.com/decover-tech/kosha/clients/go)
      conformance/          shared SDK conformance cases, fault proxy, retry policy
    proto/
      buf.yaml              Buf module config
      kosha/v1/kosha.proto  canonical API contract (source of truth)
    gen/                    generated stubs + OpenAPI spec (git-ignored)
    tools/codegen/          code generation scripts and docs
    scripts/conformance/    end-to-end BM25 + filter correctness suite
    scripts/integration/    ingest + query pods over MinIO + Postgres (CI job)
    scripts/bench/          large-scale benchmark harness
    docs/                   configuration reference, development guides
    DESIGN.md               architecture document

[CONTRIBUTING.md](../CONTRIBUTING.md#where-does-my-change-go) maps each crate to the kind of
change that belongs in it.

## Development

Prerequisites: Rust stable (via [rustup](https://rustup.rs) — the repo pins the
channel via `rust-toolchain.toml`), Docker, and optionally `pre-commit`.

    cargo build                                                # build the workspace
    cargo test --all-features                                  # tests, incl. s3/postgres code (CI gate)
    cargo fmt --all -- --check                                 # formatting (CI gate)
    cargo clippy --all-targets --all-features -- -D warnings   # linting (CI gate)
    pre-commit install                                         # optional: run gates on commit

Every `KOSHA_*` environment variable the server reads, with its default and units, is listed
in [docs/configuration.md](configuration.md). [CONTRIBUTING.md](../CONTRIBUTING.md) covers
the full contributor workflow: crate map, feature flags, benchmarks and PR conventions.

The Postgres CAS integration test races independent writers and checks retries.
Run it against a dedicated disposable database:

    KOSHA_TEST_DATABASE_URL=postgres://... cargo test -p kosha-control --features postgres --test postgres_cas --test orgs -- --ignored

To measure single-document upsert write amplification against a real 500 MiB
segment (including its stored filter payload):

    cargo bench -p kosha-write --bench upsert_write_amplification

### Local S3 (MinIO)

    docker compose up -d minio createbuckets

MinIO serves an S3-compatible API on `localhost:9000` (web console on `:9001`,
credentials `kosha` / `kosha-dev-secret`) with the `dsearch-dev` bucket
auto-created.

### Which Docker image

The image CI publishes for public pulls is
[`decoverai/kosha`](https://hub.docker.com/r/decoverai/kosha) on Docker Hub.
`.github/workflows/release.yml` sets `DOCKERHUB_IMAGE: decoverai/kosha`. A
push to `main` moves `:main` (amd64). A `v*` tag also pushes `:latest`, the
full version, and the minor version, as a multi-arch manifest (amd64 and
arm64). Production Kubernetes (`k8s/prod/kustomization.yaml`) and the EKS
deploy action use the same name. Staging pulls a different registry: ECR
`decover/kosha`, not Docker Hub.

`ravidecoverai/kosha` is a second Docker Hub repository. It is not named
anywhere in this repo. As of 2026-11-28 its `:latest`, `:0.1`, and `:0.1.0`
tags were last updated on 2026-07-25, and its `:main` tag was updated on
2026-11-27. The release workflow in this checkout does not push there.
Use `decoverai/kosha` for the current server. The July tags on the other
name are an older build.

The server will not start until it has a credential. A bare
`docker run decoverai/kosha:latest` exits. Pass `KOSHA_API_KEY`, or set
`KOSHA_AUTH=disabled` for a local process that should accept every request
as tenant `dev`. The container below keeps segments on its own disk and the
catalog in memory.

    docker pull decoverai/kosha:latest
    docker run --rm -p 8080:8080 \
      -e KOSHA_API_KEY=sk-kosha-dev \
      -e KOSHA_ROLE=standalone \
      decoverai/kosha:latest
    curl localhost:8080/healthz   # -> ok

Indexing and the first search are in the README. The same binary built from
this checkout needs the key as well:

    docker build -t kosha:latest .
    docker run --rm -p 8080:8080 \
      -e KOSHA_API_KEY=sk-kosha-dev \
      -e KOSHA_ROLE=standalone \
      kosha:latest

Or bring up the whole local stack (MinIO + Postgres + server): `docker compose up --build`.
That compose file builds the image itself (`image: kosha:latest`) and sets
`KOSHA_API_KEY=sk-kosha-dev`.

### Run alongside the Decover backend (Tilt)

The backend repo's Tilt setup builds and deploys `kosha` into the local k8s
cluster as `kosha-service` (`:8080` HTTP, `:50051` gRPC). See
[docs/local-development.md](local-development.md) — including how the
OpenSearch → Kosha swap will work once the read/write path lands.
