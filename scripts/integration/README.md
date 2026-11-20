# Integration suite (ingest + query over MinIO + Postgres)

Runs the read/write split end to end, the way `k8s/base` deploys it: one
ingest pod that accepts writes and one query pod (`KOSHA_INGEST_HOST` set)
that forwards writes to it and serves searches from the manifests the ingest
pod publishes. Both are built from the current commit and share MinIO (S3) and
Postgres (the control plane); each has its own local data volume. A Python
runner drives them over HTTP and checks every answer exactly.

CI runs it on every PR (`.github/workflows/integration.yml`, 15-minute
timeout), followed by the Postgres CAS tests that plain `cargo test` skips:
`cargo test -p kosha-control --features postgres --test postgres_cas -- --ignored`
against a database created in the compose Postgres (one test thread until
#330, a race between concurrent first-time migrations, is fixed).

It reuses the conformance suite (`scripts/conformance/`): its HTTP client,
deterministic corpus, query set and BM25/filter reference model. Where
conformance checks one server's answers through a write lifecycle, this suite
checks that the **query pod** gives the same exact answers as the model after
each step taken on the ingest pod, plus what only a two-pod deployment can get
wrong.

## Run it

Needs Docker. Python 3.9+ standard library only.

```bash
docker compose -f docker-compose.yml \
  -f scripts/integration/docker-compose.integration.yml up --build -d
python3 scripts/integration/run.py \
  --ingest http://localhost:8080 --query http://localhost:8081 \
  --restart-cmd scripts/integration/compose_restart.sh --out report.json
# Postgres CAS tests against the same stack:
docker compose -f docker-compose.yml \
  -f scripts/integration/docker-compose.integration.yml \
  exec -T postgres createdb -U kosha kosha_cas_test
KOSHA_TEST_DATABASE_URL=postgres://kosha:kosha-dev@localhost:5432/kosha_cas_test \
  cargo test -p kosha-control --features postgres --test postgres_cas -- --ignored --test-threads=1
# Tear down (volumes included):
docker compose -f docker-compose.yml \
  -f scripts/integration/docker-compose.integration.yml down -v
```

Without Docker you can still run:

```bash
python3 scripts/integration/run.py --self-test   # the runner's helpers, against a fake server
cargo build --release -p kosha-server
python3 scripts/integration/run.py --spawn target/release/kosha-server
```

`--spawn` starts one local server that plays both pods. Scenarios that need
two processes or a restart (b, c, h) are skipped, and the rest run against the
single server.

Useful flags: `--only a,g` (run some scenarios), `--seed N`, `--docs N`
(main-lifecycle corpus size, default 4000), `--loaders N` / `--loader-docs N`
(scenario d), `--strict`, `--verbose`.

## The stack

`docker-compose.integration.yml` is an overlay on the repo-root
`docker-compose.yml`. It parks the base `kosha-server` behind a profile and adds:

| Service | Port | Role |
| --- | --- | --- |
| `kosha-ingest` | 8080 | `KOSHA_ROLE=ingest`, `KOSHA_WAL_MODE=s3`: acknowledged writes are in S3 before the ack |
| `kosha-query` | 8081 | `KOSHA_ROLE=query`, `KOSHA_INGEST_HOST=http://kosha-ingest:8080`, manifest refresher every 200 ms |

Both use the same S3, `DATABASE_URL` and keys: `sk-kosha-dev` (tenant
`default`), `sk-tenant-a` / `sk-tenant-b`, admin `sk-kosha-admin-dev`. As in
the conformance overlay, segments come only from explicit flushes (no idle-flush
timer, no background compaction, one flush writer), so the reference model
can predict exact BM25 scores.

`compose_restart.sh <kosha-ingest|kosha-query> [--wipe]` restarts one pod,
optionally deleting its data volume first.

## Scenarios

Each check is named `<scenario>.<step>@<pod>:<aspect>` in the JSON report
(`--out`), which also records each scenario's duration. Scenarios a, b, f, e,
c and h share one namespace: at every step the conformance query set (BM25,
filters, sorts, pagination) runs on **both** pods and every answer must match
the model exactly. Before checking, the runner waits up to 30 s for the query
pod to catch up (`:converged`), using searches only. `stats` would force a
refresh and hide a broken refresher.

| | Scenario | What it does | Issues |
| --- | --- | --- | --- |
| a | ingest publish | Index and flush on ingest, then exact answers on the query pod, which has its own empty volume and must fetch every segment from S3. Three more flushes then reach the already-loaded query pod through the background refresher alone. | #230 (CAS publish after S3 durability), #231 (`_COMMIT` marker), #232 (refresher) |
| b | forwarded writes | Index, upsert, delete-by-query and flush sent to the query pod are forwarded to ingest. A `strong` search on the query pod sees unflushed writes. | #255 |
| f | deletes | Delete by id (`terms` on a `slot` keyword) on ingest and delete-by-query through the query pod. Visible on both pods. Every later checkpoint (after compaction and both restarts) checks that the deleted ids stay deleted. | #237 |
| d | bulk loaders | 8 concurrent loader threads (half through each pod) index 500 docs each in batches with interleaved flushes, then upsert every fifth doc. The final contents on both pods must be exact: every id, no extras, newest revision. | |
| g | tenant isolation | Tenants A and B own namespaces with the same name. Key A must be refused (4xx, nothing leaked) on B's namespace on both pods, over the v1 routes (search, stats, exists, documents, delete, flush, drop), the legacy routes (`POST /search`, `GET /search`, `/index`, `/replace`, `/delete`, `/flush`, `/exists`), an admin route and global stats. Legacy routes scope an unqualified name to the caller, and A's legacy delete-all leaves B intact. | #234 |
| e | compaction | `POST /v1/admin/compact-namespace`, then exact answers on both pods. | |
| c | ingest restart | Index without flushing, restart ingest, flush, check. Then the same with the ingest volume wiped: the acknowledged writes come back from the S3 WAL and the manifests from Postgres. | #236, boot reconciliation |
| h | query restart | Query pod restarted with an empty volume serves everything from S3. | #232 |

## Known bugs

`expected_failures.json` has the same format as the conformance one: fnmatch
globs over check ids, each mapped to a bug with its issue. A listed check that
fails is reported as `xfail` and does not fail the job. Anything else that
fails does. With `--strict`, a listed check that passes also fails the job.
When you fix the bug, delete its entry.

## Out of scope

Fault injection and crash points (#257), performance gates, multi-replica
query fleets and hydration-lease peer streaming.
