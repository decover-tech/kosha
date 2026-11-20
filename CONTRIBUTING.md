# Contributing to Kosha

Kosha is a search engine that keeps its data in S3 and uses local SSD as a cache. It supports
BM25 lexical search, kNN/ANN vector search, hybrid RRF retrieval, filters, aggregations,
compaction and a Postgres-backed control plane. The [README](README.md#supported-features)
lists what works today, and [DESIGN.md](DESIGN.md) explains the architecture. This file covers
what you need to go from a fresh clone to a merged pull request.

## Contents

- [Find something to work on](#find-something-to-work-on)
- [Development setup](#development-setup)
- [Where does my change go?](#where-does-my-change-go)
- [Feature flags](#feature-flags)
- [Local stack (MinIO and Postgres)](#local-stack-minio-and-postgres)
- [Before opening a PR: the CI gates](#before-opening-a-pr-the-ci-gates)
- [Benchmarks](#benchmarks)
- [The `---kosha-bench---` commit section](#the-kosha-bench-commit-section)
- [Commits and pull requests](#commits-and-pull-requests)
- [Conventions](#conventions)
- [License](#license)

## Find something to work on

1. Browse issues labelled
   [`good first issue`](https://github.com/decover-tech/kosha/issues?q=is%3Aopen+label%3A%22good+first+issue%22).
   They are self-contained and say where to look. `help wanted` marks larger items
   where help is welcome.
2. Other labels tell you the area (`area:vector`, `area:perf`, `area:durability`, ...)
   and the effort (`size:S` / `size:M` / `size:L`).
3. Comment on the issue to say you are taking it, so two people do not do the same work.
   If the issue does not say how it should be done, describe your plan in a comment before
   you write much code.
4. Use [Where does my change go?](#where-does-my-change-go) to find the crate.

To report a bug or request a feature, use the issue templates. They ask for the version,
deployment, storage backend and the request that reproduces the problem.

## Development setup

1. Install Rust stable via [rustup](https://rustup.rs). The repo pins the
   toolchain channel and components in `rust-toolchain.toml`; rustup picks it
   up automatically. Some dependencies (TLS and crypto) also need a C compiler:
   the Xcode command-line tools on macOS, `build-essential` on Debian/Ubuntu.
2. Install Docker (for the local MinIO S3 stand-in and image builds).
3. Python 3.9+ if you work on `clients/python` or run the conformance suite.
4. (Recommended) `pip install pre-commit && pre-commit install` to run the
   formatting/lint gates on every commit. The config enables both the
   `pre-commit` and `prepare-commit-msg` hook types (see
   `default_install_hook_types` in `.pre-commit-config.yaml`), so the
   `kosha-bench-commit-msg` hook is installed as well. It appends a
   `---kosha-bench---` section to each commit message; see
   [below](#the-kosha-bench-commit-section). A one-time
   `pre-commit install --hook-type prepare-commit-msg` is enough on clones
   that pre-date this option; the hook never blocks a commit if the bench fails.

Then check that everything builds and passes:

    git clone https://github.com/decover-tech/kosha.git && cd kosha
    cargo build
    cargo test --all-features

Every `KOSHA_*` environment variable the server reads is listed, with its default, in
[docs/configuration.md](docs/configuration.md).

## Where does my change go?

The workspace is a set of crates under `crates/`, with a strict dependency direction:
`kosha-core` at the bottom, then `kosha-vector-spfresh` and `kosha-segment`, then
`kosha-query` / `kosha-write` / `kosha-cache` / `kosha-control`, and the `kosha-server` binary
at the top. `kosha-cli` and `kosha-build-segments` are separate binaries.

| Crate | Owns | Change it when you are... |
| --- | --- | --- |
| `kosha-core` | Shared types: document and field values, `SearchQuery` and the filter DSL (`FilterClause`), `Manifest`, analyzers (`analysis.rs`), deletion bitmaps, errors | adding a request field or filter type, or changing a type several crates share |
| `kosha-segment` | The immutable on-disk segment format: footer, inverted index and posting blobs, doc store, filter columns and blooms, vector index, the segment writer and its bounded-memory build | changing what is written to or read from a segment file |
| `kosha-query` | The read path: BM25 scoring, block-max WAND, phrase and wildcard queries, filtering, kNN/ANN probing, RRF fusion, sorting, pagination, highlighting, aggregations, the segment cache and memory ledger | changing how a search is scored, filtered, fused or aggregated |
| `kosha-write` | The write path: WAL and group commit, per-namespace buffers, flush to segment, deletes and upserts, size-tiered compaction | changing how documents become segments or how segments are merged |
| `kosha-cache` | The on-disk (NVMe) read-through cache over segment files, LRU eviction | changing local cache sizing or eviction |
| `kosha-control` | The control plane: namespace registry and manifest store (in memory, or Postgres with the `postgres` feature), compare-and-swap publishes, hydration leases, SQL migrations | changing what is stored in Postgres or how manifests are published |
| `kosha-server` | The `kosha-server` binary: HTTP `/v1` routes, auth, S3 upload and hydration, warmup, write forwarding from query pods, the compaction scheduler, manifest refresher, `migrate` | adding or changing an endpoint, an env var, or how the pieces are connected |
| `kosha-cli` | The `kosha` command-line client (profiles, search, index, admin, `kosha curl`) | adding a CLI command or flag |
| `kosha-build-segments` | Offline parallel segment builder; output is attached with `/v1/admin/import-namespace` | changing bulk offline index builds |
| `kosha-vector-spfresh` | SPFresh/SPANN-style clustering. `kosha-segment` uses its `ClusterIndex` to build each segment's posting-based `vector.idx`. Its incremental rebalancing (LIRE split/merge/reassign) is a prototype the server does not use | changing how vectors are clustered into postings, or experimenting with incremental ANN updates |

Outside `crates/`:

- `proto/kosha/v1/kosha.proto` is the canonical `/v1` API contract. An API change starts
  here (`make proto-lint`), then goes into `kosha-core` and `kosha-server`.
- `clients/python/` is the OpenSearch-compatible Python client (`kosha-client` on PyPI).
- `scripts/conformance/` is the end-to-end BM25 and filter correctness suite;
  `scripts/bench/` holds the large-scale benchmark harness.
- `docs/` has the configuration reference and integration guides.

Rules of thumb:

- A new request option usually touches `proto/` (contract), `kosha-core` (type),
  `kosha-query` (behavior), `kosha-server` (parsing/validation) and the README features
  table. Python client support is a separate change in `clients/python`.
- A new tuning knob is an env var read in `kosha-server` (or once, via `OnceLock`, in the
  crate that uses it) and **must** get a row in `docs/configuration.md`. A test fails if it
  does not.
- Anything that changes bytes on disk (segment files, manifest, WAL) must stay readable by
  existing data in S3 and Postgres. Follow the existing pattern: a version number in the
  header, readers that accept every older version, and an env var that pins the writer to
  the previous version for rolling deploys (see `KOSHA_POSTINGS_WRITE_VERSION`).

## Feature flags

A lot of code is behind Cargo features, and **plain `cargo test` skips it**:

| Feature | Crate | Enables |
| --- | --- | --- |
| `s3` | `kosha-server` | S3 upload and hydration, warmup, peer hydration |
| `postgres` | `kosha-server`, `kosha-control` | Postgres control plane, API-key registry, hydration leases |
| `migrate` | `kosha-server` | `kosha-server migrate` (Elasticsearch to Kosha backfill); implies `s3` + `postgres` |

CI builds, lints and tests with `--all-features`, and so should you. The Docker image is
built with `--features migrate`, which turns all three on.

## Local stack (MinIO and Postgres)

**`cargo test --all-features` does not need Docker, MinIO or Postgres.** The one test that
needs a real Postgres is `#[ignore]`d (see the table below). You need the stack to run the
server end to end:

    docker compose up -d minio createbuckets   # S3 only
    docker compose up --build                  # MinIO + Postgres + kosha-server on :8080

MinIO serves S3 on `localhost:9000` (console on `:9001`, credentials `kosha` /
`kosha-dev-secret`) with the `dsearch-dev` bucket created. Postgres listens on
`localhost:5432` (`kosha` / `kosha-dev`, database `kosha`). To run a server from source
against them:

    KOSHA_AUTH=disabled KOSHA_DATA_DIR=/tmp/kosha-data \
    KOSHA_S3_BUCKET=dsearch-dev KOSHA_S3_PREFIX=kosha/ \
    KOSHA_S3_ENDPOINT=http://localhost:9000 \
    KOSHA_S3_ACCESS_KEY=kosha KOSHA_S3_SECRET_KEY=kosha-dev-secret \
    DATABASE_URL=postgres://kosha:kosha-dev@localhost:5432/kosha \
    cargo run -p kosha-server --features s3,postgres

What needs the stack:

| Check | Needs |
| --- | --- |
| `cargo test --all-features` | nothing |
| Conformance suite, `scripts/conformance/run.py --spawn target/release/kosha-server` | nothing (in-memory control plane; the restart checkpoint is skipped) |
| Conformance suite as CI runs it (with a cold restart) | `docker compose -f docker-compose.yml -f scripts/conformance/docker-compose.conformance.yml up --build -d`; see [scripts/conformance/README.md](scripts/conformance/README.md) |
| `postgres_cas` integration test (`#[ignore]`d) | a **disposable** Postgres: `KOSHA_TEST_DATABASE_URL=postgres://... cargo test -p kosha-control --features postgres --test postgres_cas -- --ignored` |
| Quickstart (`kosha` CLI or `make quickstart`) | `docker compose up --build` |

The conformance suite and the `scripts/bench/` harness set `KOSHA_COMPACTION_INTERVAL_MS=0`
so that background compaction does not change the segment layout under them. Do the same when you need
deterministic segments.

## Before opening a PR: the CI gates

All of these are CI gates. Run them locally first:

    cargo fmt --all -- --check
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --all-features --locked
    make client-python-test      # if you touched clients/python
    docker build -t kosha:ci .   # if you touched the Dockerfile or build config

CI also runs `buf lint` on `proto/` (`make proto-lint`), the bench-hook script tests
(`make bench-hook-test`) and the conformance suite against the Docker Compose stack.

## Benchmarks

Microbenchmarks are ordinary `cargo bench` targets:

| Location | Benches |
| --- | --- |
| `crates/kosha-query/benches/` | `search_latency`, `scoring_profile`, `topk_blockmax`, `filtered_broad_query`, `segment_memory`, `warm_materialize`, `clone_vs_sort`, `knn_open_search`, `vector_segment_open`, `vector_segment_accounting` |
| `crates/kosha-write/benches/` | `upsert_write_amplification`, `sustained_concurrent_flush`, `wal_group_commit` |
| `crates/kosha-segment/benches/` | `tokenizer` |
| `crates/kosha-vector-spfresh/benches/` | `build_query_cost` |

    cargo bench -p kosha-query --bench search_latency

Five benches use Criterion (`search_latency`, `scoring_profile`, `clone_vs_sort`,
`vector_segment_open`, `build_query_cost`), so a before/after comparison is built in:

    git switch main      && cargo bench -p kosha-query --bench search_latency -- --save-baseline before
    git switch my-branch && cargo bench -p kosha-query --bench search_latency -- --baseline before

The rest are plain binaries that print a table of timings; run them on both trees and compare
the tables.

Each bench's header comment says what it measures and which `KOSHA_BENCH_*` variables
change its size (listed in [docs/configuration.md](docs/configuration.md#benchmark-harness-kosha_bench_)).
`scripts/bench/` holds the multi-million-document harness that compares against OpenSearch.

**When to include numbers.** A PR whose purpose is performance (`perf(...)`), or one that
changes a hot path (scoring, posting decode, segment open, hydration, the write path), should
include before/after numbers from the relevant bench in the PR description: the command,
the machine, and the result. Correctness fixes and docs do not need them. For same-repo
branches, the `bench-compare` workflow also posts a `segment_memory` before/after comment on
every PR to `main`. Use it as a smoke signal, not as proof.

## The `---kosha-bench---` commit section

Kosha records the cold and warm query latency of the `segment_memory` microbench in commit
messages, between `---kosha-bench---` and `---end kosha-bench---` markers. It is added in two
places:

- **Locally**, by the `prepare-commit-msg` hook (`scripts/commit_bench_section.sh hook`)
  when `pre-commit` is installed. It runs the bench before the commit is sealed; if cargo is
  missing or the bench fails, it prints a warning and commits without the section.
- **In CI**, by `.github/workflows/pre-commit.yml`. On every push to a PR it runs the bench,
  **amends the PR head commit** to add the section, and **force-pushes** that commit back to
  your branch.

What this means for you:

- If your head commit already has the markers (because your local hook added them), the
  workflow does nothing.
- Otherwise, after you push, the branch on GitHub has a different head SHA from your local
  one. **Run `git pull --rebase` before your next push**, or the push is rejected as
  non-fast-forward. Only the head commit's message changed, so the rebase is trivial.
  Do not force-push over the amended commit if someone else has pushed to the branch since.
- The workflow uses `--force-with-lease`, so it never overwrites a push it has not seen.
- **PRs from forks are skipped** (Actions cannot push to a fork). Your fork branch is never
  rewritten; install the local hook if you want the section, but it is not required to merge.

## Commits and pull requests

- Commit subjects follow [Conventional Commits](https://www.conventionalcommits.org/):
  `type(scope): summary`, for example `feat(query): ...`, `fix(wal): ...`,
  `perf(wand): ...`, `docs(readme): ...`, `test(conformance): ...`, `ci(bench-compare): ...`,
  `chore: ...`. The scope is usually the crate or area.
- Keep a PR to one change. Reference the issue in the description (`Closes #123`).
- Fill in the PR template: what changed, how you tested it, and whether it changes the
  on-disk segment format, the manifest, or the `/v1` wire contract. Those need a
  compatibility story: existing segments in S3 and manifests in Postgres must stay readable,
  and existing clients must keep working.
- Add tests for every behavior you change. A bug fix should come with a test that fails
  without it.
- If user-visible behavior changes, update the README (features table, Not supported yet) and,
  for configuration, `docs/configuration.md`, in the same PR.
- A maintainer reviews every PR. All CI gates must be green before merge.

## Conventions

- Unsafe code is forbidden workspace-wide (`[workspace.lints]`).
- Keep crate boundaries as in the table above: shared types go in `kosha-core`, and lower
  crates never depend on higher ones (`kosha-segment` knows nothing about `kosha-query` or
  `kosha-server`). DESIGN.md explains the reasons.
- Prefer measured claims: comments and PR descriptions that mention performance should
  name the bench or test that shows it.
- Do not put customer names, internal hostnames, private registries or credentials in code,
  docs, tests or examples. Use neutral placeholders (`example.com`, `<registry>/kosha:<tag>`).

## License

Kosha is licensed under the Apache License 2.0 (see [LICENSE](LICENSE)). By submitting a
contribution you agree that it is licensed under the same terms, as described in section 5 of
the license.
