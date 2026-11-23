# Fault-injection suite (#357)

Process-level faults against the ingest + query compose stack. #257 covers crash points
inside one process (failpoints under `cargo test`, see `docs/testing.md`). This suite
SIGKILLs the real server, starves it of S3 and Postgres through
[toxiproxy](https://github.com/Shopify/toxiproxy), and boots several pods against an
unmigrated database. After each fault it checks that nothing acknowledged was lost.

CI runs it nightly, on demand, and on PRs that touch `crates/kosha-server`,
`crates/kosha-write`, `crates/kosha-control`, `scripts/faults` or `scripts/soak`
(`.github/workflows/faults.yml`). The same job then runs the 5-minute soak smoke
(`scripts/soak/`).

## Run it

Needs Docker. Python 3.9+ standard library only. Run from the repo root.

```bash
COMPOSE="docker compose -f docker-compose.yml \
  -f scripts/integration/docker-compose.integration.yml \
  -f scripts/faults/docker-compose.faults.yml"
$COMPOSE up --build -d
python3 scripts/faults/run_faults.py --out faults-report.json
$COMPOSE down -v
```

Useful flags: `--only kill-mid-flush,first-boot`, `--flush-docs N`, `--segments N`,
`--kill-delay S`, `--toxic-seconds S`, `--cut-seconds S`, `--max-recovery S`, `--strict`.
`python3 scripts/faults/run_faults.py --self-test` checks the runner's logic (SigV4 against
AWS's published example, manifest parsing, the toxiproxy client, the workload's acked/unacked
bookkeeping, expected failures, the report) against fakes, with no Docker.

## The stack

`docker-compose.faults.yml` is an overlay on the repo-root `docker-compose.yml` and
`scripts/integration/docker-compose.integration.yml` (ingest pod on 8080, query pod on 8081;
see `scripts/integration/README.md`). It adds:

| Service | Port | Role |
| --- | --- | --- |
| `toxiproxy` | 8474 (API) | Proxy `minio` (`toxiproxy:19000` -> `minio:9000`) and `postgres` (`toxiproxy:15432` -> `postgres:5432`), from `toxiproxy.json` |
| `kosha-ingest`, `kosha-query` | unchanged | `KOSHA_S3_ENDPOINT` and `DATABASE_URL` point at the proxies |
| `kosha-boot-1..3` | 8091-8093 | Profile `first-boot` only: one ingest and two query pods on a fresh database `${KOSHA_FIRSTBOOT_DB}`, started together by the first-boot scenario |

The runner reads Postgres with `psql` inside the `postgres` container and MinIO on
`localhost:9000` (SigV4-signed `HEAD`), both directly, past the proxies, so a fault never
hides an invariant violation.

## Scenarios

Each scenario writes to its own namespace, `faults-<scenario>-<run id>`. A background
writer sends a 50-document batch to the ingest pod every 50 ms. It records each batch that
returns 200 as acknowledged and each other batch as unacknowledged (refused, reset, timed out
or 5xx: it may or may not have landed).

| Scenario | Fault |
| --- | --- |
| `kill-mid-flush` | Buffer `--flush-docs` (40,000) documents, start a flush, SIGKILL the ingest pod `--kill-delay` seconds later, then start it. A 150 ms S3 latency toxic keeps the segment upload in flight at the kill; the report records whether the flush had returned (`flush_in_flight_at_kill`). |
| `kill-mid-compaction` | Flush `--segments` (8) segments, start an async full compaction (`POST /v1/admin/compact-namespace`), SIGKILL the ingest pod while it runs (job state recorded), then start it. A synchronous compaction must then succeed, and the count must still be exact. |
| `s3-toxics` | On the MinIO proxy, one at a time for `--toxic-seconds` each: 1 s latency (±250 ms), `timeout` (connections stall and close after 2 s), `reset_peer` on half the connections. A flush is attempted under each. After recovery, a SIGKILL restart; the acked writes and the count are checked again. |
| `postgres-cut` | The Postgres proxy disabled for `--cut-seconds` (10) with a flush attempted, then re-enabled; then a real `docker compose restart postgres` (failover approximation). After recovery, a SIGKILL restart and the same re-checks. |
| `first-boot` | `createdb` a fresh database, then start `kosha-boot-1..3` in one `compose up`. Every pod must stay running and become ready, and a write through the ingest pod must be readable on both query pods. Guards the concurrent-migration fix of #330 (advisory lock). |

## Checks

Check ids are `<scenario>:<aspect>[@<pod>]`. After each fault the runner waits for recovery,
re-sends every unacknowledged batch until it is acknowledged (upserts are idempotent, so the
final contents are exactly every id ever sent), flushes, and checks:

| Check | Meaning |
| --- | --- |
| `:recovered` | The ingest pod acknowledged a write and a flush again within `--max-recovery` (120 s) of the fault ending |
| `:acked-present` | Every acknowledged id exists (`POST .../exists` on the ingest pod): zero acknowledged-write loss |
| `:doc-count@ingest`, `@query` | `match_all` total equals every id ever written, exactly (strong read on ingest; eventual read on the query pod after it catches up) |
| `:manifest-committed` | Every segment of every row of `kosha.manifests` has its `_COMMIT` object in MinIO |
| `:manifest-during-fault` | The same, sampled every `--watch-every` (2) seconds while the fault was on. Unreadable samples (Postgres away on purpose) are counted, not failed |
| `:redrive-after-restart`, `:acked-present-after-restart`, `:doc-count-after-restart` | `s3-toxics` and `postgres-cut`: after a SIGKILL restart, batches still unacknowledged are re-sent, and the acked ids and exact count are checked again |

## Report

`--out` writes JSON with `summary` and `checks`, like the integration report, plus:

- `recovery`: one entry per scenario. It has `ready_s` (kill to `/readyz`), `first_ack_s` and
  `first_flush_s` (fault end to the first acknowledged write and flush), `restart_ready_s`,
  `unacked_batches`, `write_failures` by status, and scenario details (whether the flush was in
  flight at the kill, the compaction job state, each toxic phase's flush status).
- `scenarios`: the duration and error of each scenario.
- `config`: the flags.

## Known bugs

`expected_failures.json` uses the integration suite's format: fnmatch globs over check ids,
each mapped to a bug with its issue. A listed check that fails is reported as `xfail` and does
not fail the job. With `--strict`, a listed check that passes also fails the job. Delete each
entry when its bug is fixed.

The list is empty now, so every check gates. The `s3-toxics` recovery and pre-restart count
checks were listed for #368, which this suite found: during the S3 toxics the WAL writer made
more failed PUTs to one key than it remembers, forgot the one that had landed, and then took
its own object for another writer's. It fenced itself, and the ingest pod refused every write
until it restarted.

## Not covered here

The issue's spot-reclaim case (node loss) is the ingest restart with a wiped volume, which the
integration suite already runs (`scripts/integration`, scenario c). Throttling as S3 `503 SlowDown` responses
needs an HTTP-level fault proxy; toxiproxy works at TCP level.
