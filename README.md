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
> aggregation, HNSW vector search, and RRF hybrid fusion. Rerank is Phase 2
> (DESIGN.md §3.1).

## Install

| Artifact | Where | Get it |
| --- | --- | --- |
| Server image | [Docker Hub — `ravidecoverai/kosha`](https://hub.docker.com/r/ravidecoverai/kosha) | `docker pull ravidecoverai/kosha:latest` |
| Python client | [PyPI — `kosha-client`](https://pypi.org/project/kosha-client/) | `pip install kosha-client` |
| CLI | not published to crates.io yet | `cargo install --path crates/kosha-cli` |

## Supported features

### Query

| Feature | Notes |
| --- | --- |
| BM25 lexical search | `k1` / `b` tunable per request: `bm25_params: {"k1": 2.0, "b": 0.3}` on `POST /search`, `k1=` / `b=` on `GET /search`. Either may be omitted and falls back to the segment's build-time value (1.2 / 0.75 unless built otherwise). `k1` must be ≥ 0 and `b` in [0, 1]; anything else (or non-numeric) is a 400. WAND pruning bounds are recomputed for the request's values, so results match exhaustive scoring. `k3` is not supported. IDF and `avgdl` are namespace-level (summed over all segments), so a document scores the same whichever segment holds it; `bm25_params: {"stats": "segment"}` / `bm25_stats=segment` (or `KOSHA_BM25_STATS=segment` server-wide) switches back to per-segment statistics for debugging. Wildcard queries use per-segment statistics |
| `operator: and` / `or` | AND (default) requires every query term; OR scores the union |
| Block-max WAND | Skip-list postings, rarest-first probe ordering, leapfrog AND join |
| `match_phrase` | Positional postings, configurable `slop`. A word a `stop` filter removed still counts as a position, so `"state of the art"` matches across the removed stopwords and `"state art"` does not |
| Configurable analyzers | Per-field `analyzer` in the namespace schema: `standard`, `standard@2` (Unicode), `keyword`, `lowercase`, `whitespace`, plus `lowercase` / `stop` / `stem` filters. See [Analyzers](#analyzers) |
| `wildcard` | Per-field pattern match, case-insensitive by default |
| kNN / ANN vector search | Cluster-and-posting on-disk vector index with centroid probing and triangle-inequality lower bounds; global probe budget; flat/HNSW fallback for older segments |
| Hybrid retrieval (RRF) | A lexical query (`query_text` / `wildcard` / `match_phrase`) plus a `knn` clause is fused with Reciprocal Rank Fusion, `score = Σ 1/(rank_constant + rank)`. Tune with `"rank": {"rrf": {"rank_window_size": 100, "rank_constant": 60}}` (Elasticsearch names and defaults: window = `max_results`, constant = 60; window must be ≥ `max_results`). Each retriever is ranked across the whole namespace before fusing: the lexical list is the best `rank_window_size` BM25 hits, the vector list the nearest `min(knn.k, rank_window_size)`. Vector-only hits are kept. Each hit returns the fused `score` plus `rank_details` (`lexical_rank` / `lexical_score` / `knn_rank` / `knn_score`). The fused list is at most `rank_window_size` long; `from` / `max_results`, `sort`, `collapse` and `search_after` (e.g. on `[_score desc, _id asc]`) apply within it. `total_hits` and aggregations count the lexical matches plus the vector window. Optional `"mode": "lexical" \| "semantic" \| "hybrid"` is checked against the clauses present |
| Filters | `term`, `terms`, `range` (`gte`/`gt`/`lte`/`lt`), `bool` (`must` / `must_not` / `should` / `minimum_should_match`), `match_all` |
| Multiple vector columns | A namespace can hold several `Vector` fields (e.g. a 384-dim `title_emb` and a 1536-dim `body_emb`); each segment builds one index per field and `knn.field` picks the one searched. Each field has one dimension per namespace, fixed by the first vector written to it: a document whose vector disagrees is rejected with a 400, and so is a `knn` naming a field no segment indexes or a query vector of the wrong dimension. Segments written before this change keep serving any `knn.field` from their single index, unless their `doc_vectors.bin` names exactly one field (#204) |
| kNN-scoped filters | `knn.filter` restricts only the vector list; the top-level `filter` restricts both the lexical and the vector list |
| Sorting | Multi-field `sort` over stored fields, plus `_id` |
| Pagination | `from` / `max_results`, and an OpenSearch-style `search_after` cursor |
| Highlighting | Per-field, with configurable `pre_tags` / `post_tags` |
| Aggregations | `terms`, `cardinality`, `composite` |
| Total-hit accounting | Capped counting with an `eq` / `gte` relation (`track_total_hits`-style), overridable per query via `exact_total_hits` / `total_hits_cap` |
| Degradation signal | `knn_degraded_segments` reports segments whose vector search failed, so a 200 with silently missing neighbors is visible to the caller |

### Indexing and writes

- Bulk document indexing, with field types `Text`, `Keyword`, `Integer`, `Float`, `Date`, `Boolean`, `Vector`.
- `Text` values longer than 256 chars (`KOSHA_TEXT_FILTER_IGNORE_ABOVE`; `off` disables the cap) are full-text searchable and returned in hits, but are not copied into the filter columns, so `term`/`terms`/`range` filters, `sort`, and aggregations do not see them. This matches Elasticsearch's dynamic-mapping `keyword` `ignore_above: 256`. Use `Keyword` for a long value that must be exact-matched.
- Vector field values are stored once, as binary f32, in each segment's `doc_vectors.bin` instead of as JSON text in `doc_store.bin` (`KOSHA_VECTOR_STORE=sidecar`, the default). Hits and compaction still see the full vector, which comes back as the f32 values that were indexed, re-serialized as JSON. Its text can differ from what was sent: digits beyond f32 precision are lost, and so is number formatting. Set `KOSHA_VECTOR_STORE=inline` to keep the old layout. **Before rolling back to a release older than this change, set `inline` and compact:** older binaries can't read the sidecar, and an older binary compacting a sidecar segment drops its vectors.
- Each `Vector` field gets its own ANN index per segment (#204). A segment with one vector field is byte-identical to before. **A release older than this change reads only a segment's first vector column, for any `knn.field`, and fails to compact a segment whose columns differ in dimension:** don't roll back past it once documents with several vector fields have been flushed.
- Upsert by document id — segments holding a prior version of an id are rewritten.
- Delete by query (tombstone-based) and a document `exists` check.
- Write-ahead log for buffered documents, replayed on restart.
- Auto-flush at a configurable document threshold (`KOSHA_FLUSH_THRESHOLD`), plus an explicit `flush` that makes writes visible immediately. A buffer that stays below the threshold is flushed and published once it has been pending for `KOSHA_FLUSH_MAX_AGE_MS` (default 1000; `0` disables), similar to Elasticsearch's `refresh_interval`. The Python client flushes only on `refresh=True` / `"wait_for"` or `indices.refresh()`, not after every bulk batch.
- Size-tiered compaction with a cap on merged-segment size (5 GiB default). A background scheduler on the write pod runs a tiered pass on any namespace past the segment trigger every `KOSHA_COMPACTION_INTERVAL_MS` (default 30000; `0` disables), at most `KOSHA_COMPACTION_MAX_CONCURRENT` passes at a time (default 1). A tiered pass hydrates only the small segments it may merge. `POST /v1/admin/compact-namespace` also accepts `"async": true` and returns `202` with a job id; poll `GET /v1/admin/compaction-jobs/{id}`, or see scheduler state in `GET /v1/admin/compaction-status`. Building a segment (flush or merge) keeps at most `KOSHA_SEGMENT_BUILD_MEMORY_BYTES` (default 256 MiB) of postings in memory: stored documents stream to disk as they're added, and postings over the budget spill to sorted runs that are merged on disk when the segment is finished.

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
- Read/write split: query pods forward every mutating route to `KOSHA_INGEST_HOST` and serve reads locally. A background refresher on each query pod polls every namespace's manifest version in one control-store query every `KOSHA_MANIFEST_REFRESH_MS` (default 500; `0` disables it and the pod checks before every search instead) and swaps in manifests that advanced; searches already running finish on the snapshot they started with, and namespaces in `KOSHA_WARMUP_NAMESPACES` have the new version's scoring set fetched before the swap. Searches don't touch the control store unless they pass `max_staleness_seconds` (body field or query parameter) and the namespace was last confirmed current longer ago than that. If the control store is unreachable, query pods keep serving their last-known-good manifests and only a namespace the pod never loaded fails (503). `GET /v1/stats` on a query pod reports per-namespace `manifest_version`, `stored_manifest_version`, `last_refresh_at` (Unix seconds) and `manifest_lag_seconds`, plus a `manifest_refresh` block with error counters and the maximum lag.
- Backpressure controls: max concurrent searches, search queue depth and timeout, hydration concurrency, admission timeout.
- `GET /healthz` (liveness), `GET /readyz` (readiness), `GET /v1/stats` and per-namespace stats.
- Admin endpoints: create API key, rebuild filter blooms, backfill offset tables, compact a namespace (sync or async), compaction job and scheduler status, import a namespace.
- Schema migrations via `kosha-server migrate`.

### API and clients

- HTTP/JSON `/v1` API — `documents`, `search`, `flush`, `delete`, `exists`, `stats` — with the Phase 1 unversioned routes still served for backward compatibility.
- `proto/kosha/v1/kosha.proto` is the canonical API contract and the source for generated stubs and the OpenAPI spec.
- `kosha` CLI (built from `crates/kosha-cli`, not yet on crates.io): health, index, search, flush, delete, stats, admin commands, named profiles, `--json` output, and a `kosha curl` escape hatch.
- OpenSearch-compatible Python client, [`kosha-client` on PyPI](https://pypi.org/project/kosha-client/): `search`, `index`, `bulk`, `count`, `update`, `delete_by_query`, `update_by_query`, `scroll`, plus `indices` and `tasks` namespaces, translating the ES query DSL to Kosha's native shape.

### Not supported yet

- Reranking (Phase 2, DESIGN.md §3.1). Per-retriever weights for RRF.
- Analyzer features beyond the list in [Analyzers](#analyzers): per-language stemmers other than English, synonyms, n-grams, CJK dictionary segmentation (`standard@2` splits Han ideographs one per token), full Unicode case folding (`ß` and `SS` stay distinct). Other `FieldSchema` members (`type`, `indexed`, `stored`) are stored but not enforced — field types still come with each document.
- gRPC — the service is defined in the proto, but HTTP/JSON is the only implemented transport.
- `kosha-vector-spfresh` (SPFresh/SPANN with LIRE rebalancing) is a standalone, benchmarked prototype; it is not wired into the segment format or the query path.

## Analyzers

A namespace schema chooses how each `Text` field is analyzed (issues #220, #221). Set it with
`POST /v1/namespaces/{ns}/schema` (read it back with `GET`); the body is a proto `NamespaceSchema`:

```json
{"fields": {
   "title": {"analyzer": ["standard@2", "stop", "stem"]},
   "sku":   {"analyzer": ["keyword"]},
   "body":  {"analyzer": ["standard", "stop"], "stopwords": ["the", "a"]}},
 "default_analyzer": ["standard@2"]}
```

The first name picks the analyzer, every later name appends a filter:

| Analyzer | Tokens |
| --- | --- |
| `standard` (default) | Legacy tokenizer: split on whitespace, trim ASCII punctuation from each word's edges, lowercase. `hello,world` is one token |
| `standard@2` | Unicode: UAX #29 word boundaries (`unicode-segmentation`), also split at `.`/`:` between letters, lowercase. `hello,world` → `hello` `world`; `naïve.café` → `naïve` `café`; `3.14` stays whole; Han ideographs are one token each |
| `keyword` | The whole value, untouched — one token, case-sensitive |
| `lowercase` | The whole value, lowercased — one token |
| `whitespace` | Split on whitespace; case and punctuation kept |

| Filter | Effect |
| --- | --- |
| `lowercase` | Unicode lowercase |
| `stop` | Drop stopwords: Lucene's 33-word English list, or the field's `stopwords` (`default_stopwords` for `default_analyzer`). Applied to the query too; a query of only stopwords matches nothing |
| `stem` | English Snowball stemmer (`rust-stemmers`): `running`, `runs` and `run` all match each other |

`default_analyzer` covers every `Text` field the schema does not list (clients send every JSON
string as `Text`). All text fields share one term space, so a query matches a document if it
matches one analysis of the query across its fields — e.g. a stemmed `title` term and an unstemmed
`body` term together.

**Compatibility.** Each segment records the chains it was written with in its footer, and every
query is analyzed per segment with those chains, so changing the schema (or the tokenizer version)
never makes existing segments return zero hits: they answer as they were written until compaction
rewrites them, re-analyzing their stored documents under the current schema. An invalid schema is
rejected with a 400 and changes nothing.

**Why `standard` still means the legacy tokenizer.** The Unicode tokenizer is opt-in
(`standard@2`), not the new default, for three reasons: segments written with no schema stay
byte-identical to every earlier release; old segments' terms (`hello,world`) are only reachable
by the legacy reading anyway; and rollback stays safe. A binary older than this change ignores the
footer `analysis` field and analyzes every query the legacy way, so after **rolling back** it,
segments written with any non-default analyzer return wrong or missing hits for affected terms
until they are rewritten. Before rolling back past this change, remove the schema (`POST` one with
no analyzers) and run a full compaction (`POST /v1/admin/compact-namespace`) so every segment is
legacy again. Flipping the default to `standard@2` later only needs a new default here: old
segments keep their recorded version.

Throughput on the repository's Markdown (16 MB, ~2 KB documents; `cargo bench -p kosha-segment
--bench tokenizer`, Apple M5): `standard` 249 MB/s, `standard@2` 147 MB/s (0.59x),
`standard@2` + `stop` + `stem` 41 MB/s; whole segment build 61 / 50 / 28 MB/s (`standard@2`
costs 0.82x of a legacy build; the Snowball stemmer is most of the rest). All-ASCII words take a
hand-written UAX #29 fast path checked against `unicode-segmentation` by a differential test.

## Authorization and durability

All client namespace routes, including legacy `/index`, `/search`, `/flush`,
`/replace`, `/exists`, and `/delete`, resolve an **unqualified namespace name**
inside the authenticated tenant. Client-supplied paths and qualified names are
rejected. Legacy callers that previously accessed bare stored namespaces need
an explicit migration to tenant-scoped namespaces; this change does not rename
existing data automatically.

- Set `KOSHA_ADMIN_API_KEY` for `/v1/admin/*` and global `/stats` access.
- Set a separate `KOSHA_INTERNAL_API_KEY` on every peer participating in segment
  hydration. Tenant API keys do not authorize admin or peer-file access.
- Both service capabilities are disabled when their respective secret is unset.
  Ordinary API-key configuration remains `KOSHA_API_KEY` / `KOSHA_API_KEYS` or
  the Postgres key registry. Namespace-scoped stats remain available to tenants.
- The server refuses to start when no credential is configured at all, unless
  `KOSHA_AUTH=disabled` is set explicitly (local development only). Postgres keys
  are re-read every `KOSHA_API_KEY_REFRESH_SECS` (default 30), request bodies are
  capped at `KOSHA_MAX_BODY_BYTES` (default 64 MiB, 413 above it), and
  `KOSHA_LEGACY_ROUTES=off` turns the unversioned routes into 410s.

A successful flush uploads its segment snapshot before publishing its manifest.
Upload and manifest-store failures return errors and remain retryable. Buffered
writes use a local, fsynced WAL; they are not S3-durable until a successful flush.
Concurrent appends are group-committed: one WAL file and one fsync cover every
record queued while the previous commit ran, and each caller is acknowledged
only after the fsync covering its own record. Once the previous group held more
than one record, the committer waits up to `KOSHA_WAL_GROUP_COMMIT_MS` (default
2; `0` disables the wait) for more, and cuts a group at
`KOSHA_WAL_GROUP_COMMIT_BYTES` (default 4 MiB). A lone writer never waits.
Every record has its own log sequence number. WAL records are reclaimed only
through the published namespace checkpoint: a file shared by several namespaces
is deleted once each of them has a published checkpoint past its records there,
so a flush never removes later writes or another namespace's records. Partial
parallel flush failures restore the entire snapshot for retry.

Publication uses compare-and-swap against the writer's last committed manifest
version. Concurrent writers cannot replace each other's snapshots. A stale
writer reads the newer manifest back (resolving its deletion sidecars), rebases
its unpublished delta onto it and retries the CAS, up to 3 times with jittered
backoff. The delta is the segments it added, the segments it compacted away and
its new deletes; deletes on a segment both sides list are merged, and the
published WAL checkpoint stays the rebasing writer's own. A writer with nothing
unpublished just adopts the newer manifest. The rebase aborts, leaving local
state and the WAL untouched, when merging would lose data: both writers
compacted the same segment, the other writer deleted documents in a segment this
writer compacted away, or the other writer compacted away a segment that carries
this writer's unpublished deletes. An unresolved conflict returns HTTP 409.
Restarting the writer with its existing data directory loads the published
manifest and replays its pending writes from the WAL. Retrying an identical
committed snapshot is safe after a lost commit response, and a rebase
recognizes such a commit as its own. Segment object keys include a random
writer identifier to avoid collisions before publication. Migration
(`kosha-server migrate`) and `POST /v1/admin/import-namespace` also publish by
CAS against the version they read. They never rebase, and they fail instead of
overwriting a manifest another writer committed (409 for import).

Each WAL directory has a persistent origin identifier. New manifest checkpoints
apply only to that origin, so another writer's checkpoint cannot discard local
pending writes during recovery. Preserve the entire WAL directory, including
its `origin` file, across restarts. Legacy checkpoints without an origin retain
their previous recovery behavior; upgrade all writers before concurrent use.

Deletes include buffered documents and WAL-log resolved document IDs before
applying them. New WAL files hold one or more checksummed v3 frames (the CRC
covers the record's sequence number and payload); vectors are stored as binary
f32 rather than JSON text. Recovery keeps every complete frame of a torn final
file, discards only the incomplete tail, and rejects corrupt records. v1 and v2
files remain readable.
Deletes and writes replay in order, so reinserting a deleted ID remains valid.

Deletion sets are immutable `deletes-{sha256}.roaring` sidecars referenced by the
manifest. Publication uploads these sidecars before CAS, including new generations
for segments uploaded earlier. Query pods load and validate the referenced
bitmaps before installing refreshed manifests; a missing or corrupt bitmap fails
the request. Result caching includes deletion generations, and WAND filters
removed documents before counting or scoring them.

Upserts retain old segments, mark prior copies deleted, and write only incoming
documents into a new segment. Partial replacements read the affected document
spans and merge fields. Segment I/O runs outside the namespace state lock; the
new segment and deletion markers become visible together. Compaction reclaims
these dead copies later. Superseded files remain available to older snapshots;
S3 garbage collection is still deferred.

**Upgrade note:** Postgres `segments_json` now stores the complete manifest
object, including tombstones and checkpoints. New servers can read the old entry
arrays. Old servers cannot read the new object format: upgrade ingest and query
roles together before resuming writes, and do not roll back readers without a
manifest-format migration. Servers predating deletion-vector support ignore
those references, and older WAL readers cannot replay v2 or v3 frames: upgrade
all roles together before accepting writes and preserve the WAL directory. Rolling
back requires converting both formats. The SQL column needs no schema change.

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

The Python client ([`kosha-client` on PyPI](https://pypi.org/project/kosha-client/))
still works the same way for application code:

```bash
pip install kosha-client
```

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

The Postgres CAS integration test races independent writers and checks retries.
Run it against a dedicated disposable database:

    KOSHA_TEST_DATABASE_URL=postgres://... cargo test -p kosha-control --features postgres --test postgres_cas -- --ignored

To measure single-document upsert write amplification against a real 500 MiB
segment (including its stored filter payload):

    cargo bench -p kosha-write --bench upsert_write_amplification

### Local S3 (MinIO)

    docker compose up -d minio createbuckets

MinIO serves an S3-compatible API on `localhost:9000` (web console on `:9001`,
credentials `kosha` / `kosha-dev-secret`) with the `dsearch-dev` bucket
auto-created.

### Run with Docker

Pre-built images are published to Docker Hub at
[`ravidecoverai/kosha`](https://hub.docker.com/r/ravidecoverai/kosha) — `:main`
on every merge to `main`, and `:latest` plus the semver tags (`:0.1.0`, `:0.1`)
for every `v*` tag. Tagged releases are multi-arch (amd64 + arm64); `:main` is
amd64 only.

    docker pull ravidecoverai/kosha:latest
    docker run --rm -p 8080:8080 ravidecoverai/kosha:latest
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
