# Local development & the OpenSearch → Kosha swap

> **This page is a local-overlay note, and parts of it are older than the engine.**
> It describes the Decover backend's Tilt setup and an acceptance test that had
> not been flipped when the page was written. It also describes a lexical-only
> phase. BM25, kNN, and hybrid search are implemented now; the current list is
> [the feature reference](reference/features.md). The public write-up of the
> storage model is [how we replaced Elasticsearch](blog/how-we-replaced-elasticsearch.md).

How Kosha fits into the Decover backend's local environment (Tilt + the k8s
`dev` overlay), and how the local acceptance test — **run the app
with Kosha instead of OpenSearch** — was planned.

## Where Kosha runs locally

The backend repo (`../backend`) wires Kosha into the local cluster:

- `deployments/k8s/kosha/` — dedicated Kubernetes namespace `kosha` with
  `dev` / `stage` / `prod` overlays (`Deployment/kosha` +
  `Service/kosha-service`, HTTP `:8080`, gRPC `:50051`).
- Local Tilt includes `../kosha/dev` from `deployments/k8s/dev`.
- `Tiltfile` — `docker_build("kosha", "../Kosha")` plus a `kosha` resource
  with port-forwards `8081:8080` (HTTP; host port 8080 is taken by bach) and
  `50051:50051` (gRPC).
- Backend clients reach Kosha at
  `http://kosha-service.kosha.svc.cluster.local:8080`.

So `tilt up` in the backend repo builds this repo's Dockerfile and runs Kosha
in the `kosha` namespace next to the rest of the stack. Nothing routes traffic
to it yet (sage/pandora/celery stay on OpenSearch in the dev overlay).

Sanity check from the host:

    curl localhost:8081/healthz   # -> ok

## Local acceptance test (agreed scope)

**Milestone:** on the local k8s cluster, Sage's lexical-search path
(`search_filter.lexical_search=True`) is served by Kosha instead of
OpenSearch — and the application still runs. Semantic/hybrid retrieval keeps
going to OpenSearch; testing lexical only on local k8s is the explicit goal
for this milestone.

What has to be true for it:

- Epics 1–3, 5, 6, 8 done in this repo (API, segment format, write path,
  BM25 read path, control plane, API server). Epic 4 (SSD cache) can be a
  naive local-disk implementation for local purposes — S3/MinIO is the source
  of truth regardless.
- Epic 11 partially done in the backend: the Sage-side Kosha client wrapper
  for the lexical path only, plus a way to get test documents in (dual-write
  from pandora, or a small backfill replay).
- The configmap seam below flipped for the lexical path only.

## The OpenSearch seam

Every backend service (sage, bach, pandora, celery, hulk) reads its search
endpoint from one configmap key:

    deployments/k8s/dev/configmap.yaml
      elasticsearch_host: "http://elasticsearch-service:9200"

That value flows into `ELASTICSEARCH_HOST` → `common/settings.py` →
`common/index/connection_factory.py` (an `opensearchpy` client). One line
changes the search endpoint for the whole local stack.

## The swap — and what blocks it today

The flip itself is:

    elasticsearch_host: "http://kosha-service:8080"

...plus removing the `elasticsearch` StatefulSet from the dev overlay.

**Do not flip yet.** Sage talks to OpenSearch via `opensearchpy` and emits
OpenSearch query DSL (`script_score`, `knn`, `_bulk`, index templates in
pandora's init-indices). Kosha's API is the protobuf/HTTP surface in
DESIGN.md §12 — not the OpenSearch REST protocol. The swap therefore lands in
two steps, per the design:

1. **Epics 2–8 (this repo):** segment format, write path, SSD cache, BM25
   read path, control plane, API server. Kosha can actually index and serve.
2. **Epic 11 (backend repo):** a Kosha client wrapper in
   `common/index/paragraph_repository.py` / `connection_factory.py` that
   translates Sage's calls into Kosha `SearchRequest`s (DESIGN.md §12). Then
   `elasticsearch_host` is superseded by a backend-selector env var, and the
   configmap flip above (or its equivalent) completes the cutover.

Until then, keep OpenSearch running; Kosha deploys alongside, idle.

### Phase 1 scope reminder

Even after Epic 11, Phase 1 Kosha is BM25 lexical only — semantic retrieval
(`script_score` / `knn` paths) stays on OpenSearch until Phase 2
(DESIGN.md §3.1). The local test matrix is therefore:

| Workload                                          | Backend after Epic 11 (Phase 1)        |
| ------------------------------------------------- | -------------------------------------- |
| Lexical search (`lexical_search=True`)            | Kosha                                  |
| Semantic / hybrid search                          | OpenSearch (until Phase 2)             |
| Case-law indices (`USE_OPENSEARCH_FOR_CASELAWS`)  | AWS-managed OpenSearch (out of scope)  |

## Standalone (no backend)

This repo is self-contained for engine development:

    docker compose up --build   # Postgres + MinIO + kosha-server

Phase 1 persistence knobs (also used by stage/prod Deployments):

| Env | Purpose |
|-----|---------|
| `DATABASE_URL` | Postgres control plane (`kosha.manifests`) |
| `KOSHA_S3_BUCKET` / `KOSHA_S3_PREFIX` | Durable segment store |
| `KOSHA_S3_ENDPOINT` | MinIO / custom S3 endpoint (path-style by default) |
| `KOSHA_S3_ACCESS_KEY` / `KOSHA_S3_SECRET_KEY` | Or standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` |
| `KOSHA_DATA_DIR` | Local SSD/cache root (never authoritative when S3 is on) |

Auth knobs (see `crates/kosha-server/src/auth.rs` and the README's tenant
isolation section):

| Env | Purpose |
|-----|---------|
| `KOSHA_API_KEY` | Tenant key for tenant `default` |
| `KOSHA_API_KEYS` | Tenant keys, `key1=tenant1,key2=tenant2`; an entry may narrow its key: `key=tenant;scopes=read;namespaces=docs+wiki;expires_at=<unix secs>` |
| `KOSHA_ADMIN_API_KEY` | Admin credential for `/v1/admin/*` and global `/stats` |
| `KOSHA_ADMIN_BOOTSTRAP_KEY=disabled` | Refuse `KOSHA_ADMIN_API_KEY`, leaving only per-operator credentials (`POST /v1/admin/operators`) |
| `KOSHA_INTERNAL_API_KEY` | Peer credential for `/internal/*`: segment hydration, and query pods reading the ingest pod's unindexed writes for strong-consistency searches (#255) |
| `KOSHA_DEFAULT_CONSISTENCY` | `strong` (default) or `eventual`: whether a search that does not say also scans unindexed writes. A query pod without `KOSHA_INTERNAL_API_KEY` defaults to `eventual` |
| `KOSHA_MAX_UNINDEXED_BYTES` / `KOSHA_MAX_UNINDEXED_DOCS` | Per-namespace cap on unindexed writes (default 2 GiB / unset; `0` disables); past it `/index` and `/replace` return 429 |
| `KOSHA_TAIL_FETCH_TIMEOUT_MS` | Query pod timeout for reading the ingest pod's unindexed tail (default 10000) |
| `KOSHA_AUTH=disabled` | Run with auth off (every request is tenant `dev`). Without it, a server with no credential configured (env or Postgres `kosha.api_keys`) refuses to start |
| `KOSHA_API_KEY_REFRESH_SECS` | How often Postgres keys are re-read (default 30), so created keys work and deleted keys stop working without a restart |
| `KOSHA_MAX_BODY_BYTES` | Request body cap, larger bodies get 413 (default 64 MiB) |
| `KOSHA_LEGACY_ROUTES=on` | Temporarily enable the deprecated unversioned `/index`, `/search`, ... routes. They answer 410 by default (`KOSHA_LEGACY_ROUTES_TENANTS` opts in named tenants). Served requests are logged and counted in `kosha_legacy_requests_total` |

On startup the server logs which control plane and S3 backend it bound, and
how many manifests it restored. `/stats` exposes `control_plane`,
`cache_root`, `cache_size_bytes`, and `s3_enabled`.
