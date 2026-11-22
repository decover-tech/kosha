# Soak driver (#357)

`soak.py` runs a mixed multi-tenant ingest and query load for a fixed time, then checks the
exact document count of every namespace and applies pass/fail thresholds. The same driver
runs the 5-minute smoke in CI and the 24-hour production soak against the stage cluster.
Python 3.9+ standard library only.

## What it does

- **Layout**: one large namespace (`--large-docs`) plus `--namespaces` namespaces whose sizes
  follow a Zipf distribution (`--zipf-docs` in total, exponent `--zipf-s`). They are spread
  round-robin over the tenants in `--tenant-keys` (`key=tenant,...`). All of them are
  preloaded and flushed first. Names are `<--prefix>-<--run-id>-large` and `...-z0001`, and so
  on.
- **Writes**: `--writers` threads at `--docs-per-s` in total, in `--write-batch` requests. Each
  request is an upsert of new ids, a re-upsert of live ids (`--update-frac`), or a delete by id
  (`--delete-frac`), on a namespace picked in proportion to its size. Each thread owns the ids
  with `id % writers == thread`, so the driver knows every namespace's exact live set. A write
  that is not acknowledged is retried for `--write-retry-s`. Upserts and deletes by id are
  idempotent. If a write never gets an acknowledgement, its namespace is marked uncertain and
  the run fails.
- **Queries**: BM25, kNN (`emb`, `--dim` floats) and filtered BM25 (`term` on `cat`). The
  `--mix` option sets the share of each. Queries are sent open loop at a rate that ramps from
  `--qps-start` to `--qps-end`, round-robin over the `--query-url` pods. At most
  `--query-concurrency` are in flight; queries over that limit are dropped and counted.
- **Write-to-visible latency**: every `--visible-every` seconds, one freshly acknowledged
  insert is polled for with an eventual-consistency search on a query pod, until it shows up or
  `--visible-timeout` passes.
- **Over time**: every `--sample-every` seconds, the driver records `/metrics` of every pod
  (search queue depth, segment and NVMe cache bytes, RSS when exported, documents indexed,
  flushes, writer fencing), the large namespace's stats (documents, segments, manifest
  version), the segment total over all soak namespaces (`GET /v1/namespaces` per tenant), and
  the request counters.
- **Optional background work**: `--flush-every` and `--compact-every` flush and compact from
  the driver. Leave both at 0 when the server flushes and compacts by itself, as on k8s.
  `--replace-cmd` runs every `--replace-every` seconds to replace pods.
- **Flood mode** (#341, optional): `--flood-tenant-key` names one key of `--tenant-keys`. That
  tenant also sends BM25 searches at `--flood-multiplier` (default 10) times `--flood-qps`,
  which should be its configured query limit on the server (`qps` in
  `KOSHA_TENANT_LIMITS_FILE`); with `--flood-qps 0` the base is the run's current query rate.
  Flood requests have their own counters and are not part of the request totals or the 5xx
  and connection-error gates. The report's `flood` section holds the flooder's requests and
  429 rate, and under `victim` the other tenants' query counts, 429s and p50/p99 (overall and
  per window). The victim p99 covers only the queries that returned 200, so the victims'
  429s, 5xx and connection errors have their own gates (`--victim-max-429-rate`,
  `--victim-max-error-rate`, both 0 by default): a flood that gets the other tenants shed
  fails the run even when the few that were served were fast. Write-to-visible samples are
  not taken from the flood tenant.
- **At the end**: the driver flushes every namespace. It then requires each `match_all` total
  to equal the live set exactly: on the write URL with a strong read, and on every query URL
  with an eventual read once the pod has caught up (`--final-timeout`).

## Thresholds

| Flag | Gate | Default |
| --- | --- | --- |
| `--max-p99-ms` | p99 of each query class over the whole run | 1000 |
| `--max-visible-p99-s` | write-to-visible p99, and no sample may stay invisible | 30 |
| `--max-5xx-rate` | 5xx responses / all requests | 0.001 |
| `--max-conn-error-rate` | connection errors and timeouts / all requests | 0.01 |
| `--max-segments` | segments of the large namespace at the end (0 = off) | 0 |
| `--victim-p99-ms` | flood mode: p99 of the other tenants' queries during the flood | -1 (off) |
| `--victim-max-429-rate` | flood mode: 429 responses / the other tenants' queries during the flood | 0 |
| `--victim-max-error-rate` | flood mode: 5xx responses and connection errors / the other tenants' queries during the flood | 0 |
| flood mode | the flooder got at least one 429 (otherwise the server has no limit on it), and at least one query of the other tenants returned 200 | |
| always | exact count everywhere, no uncertain namespace, every query class ran | |

Set a gate to `-1` to turn it off. The report (`--out`) holds every verdict and the overall
and per-window (`--window`) p50/p99/max of each class, so the p99 during ingest can be read
hour by hour. It also holds the visibility percentiles, request counts by kind and status,
the timeline, events (pod replacements, writes refused, samples never visible) and the final
counts. Exit status is 0 only when every gate held.

## Smoke (5 minutes)

`--smoke` sets these defaults: 300 s, 20 Zipf namespaces, 20k + 10k docs, 300 docs/s, 4 to 12
QPS, `--flush-every 2` and `--compact-every 60`. The driver flushes and compacts because the
compose stack disables the server's idle flush and background compaction. `faults.yml` runs it
after the fault scenarios. To run it locally against the same stack:

```bash
docker compose -f docker-compose.yml \
  -f scripts/integration/docker-compose.integration.yml up --build -d
python3 scripts/soak/soak.py --smoke \
  --write-url http://localhost:8080 --query-url http://localhost:8081 --out soak-report.json
```

Or against one local server (no S3, in-memory control plane):

```bash
cargo build --release -p kosha-server
KOSHA_API_KEY=sk-kosha-dev KOSHA_API_KEYS=sk-tenant-a=tenant-a,sk-tenant-b=tenant-b \
  KOSHA_ADMIN_API_KEY=sk-kosha-admin-dev KOSHA_DATA_DIR=/tmp/kosha-soak \
  target/release/kosha-server &
python3 scripts/soak/soak.py --smoke --write-url http://localhost:8080
```

`python3 scripts/soak/soak.py --self-test` tests the pure logic and runs a short soak against
an in-process fake server, including one that loses an acknowledged write, which the exact
count must catch, and a flood run against a fake that rate limits one tenant.

A noisy-neighbour run against a local server whose limits file gives `tenant-a` a `qps` of 20:

```bash
python3 scripts/soak/soak.py --smoke --write-url http://localhost:8080 \
  --flood-tenant-key sk-tenant-a --flood-qps 20 --flood-multiplier 10 --victim-p99-ms 500
```

## The 24-hour soak on the stage cluster

The defaults without `--smoke` are the 24-hour run: 86,400 s, one 10M-document namespace, 1,000
Zipf namespaces holding 2M documents, 1,500 docs/s, 8 to 32 QPS, and the server's own
flushing and compaction. The 24-hour run is an ops run. CI does not run it.

1. **Deploy the build under test** to stage (`k8s/stage`, floating `:main` tag). Background
   compaction stays on: the `kosha-indexer` Deployment and the stage compaction CronJob. Pin
   three query replicas for the run, so pod replacement always leaves two serving:

   ```bash
   kubectl -n kosha patch hpa kosha-query -p '{"spec":{"minReplicas":3}}'
   kubectl -n kosha rollout status deploy/kosha-query
   ```

2. **Create soak tenants.** Use dedicated keys, never customer ones, and create them with the
   admin API (`scripts/create_api_key.py`, or `kosha admin create-api-key`). For example, use
   four tenants `soak-t1` to `soak-t4`.

3. **Run the driver next to the cluster**, not over a laptop link: a pod in the cluster, or a
   VM in the same VPC. Writes go through `kosha-service`, which forwards them to ingest, the way
   clients send them. Reads go to the same service (it balances over the query pods):

   ```bash
   kubectl -n kosha run soak --image=python:3.12-slim --restart=Never -- sleep infinity
   kubectl -n kosha cp scripts/soak/soak.py soak:/soak.py
   kubectl -n kosha exec -it soak -- python3 /soak.py \
     --write-url http://kosha-service:8080 \
     --query-url http://kosha-service:8080 \
     --tenant-keys "$SOAK_KEYS" \
     --admin-key "$KOSHA_ADMIN_KEY" \
     --run-id stage-$(date +%Y%m%d) \
     --preload-threads 16 \
     --max-p99-ms 500 --max-visible-p99-s 30 --max-5xx-rate 0.0005 \
     --max-segments 400 \
     --out /soak-report.json
   ```

   The preload of about 12M documents takes about an hour at 5k docs/s. The duration counts
   from the end of the preload. To preload ahead of time, first run with `--duration 0`. That
   run preloads, checks the counts and exits. Then start the 24-hour run with `--no-preload`
   and the same `--run-id` and sizes.

4. **Replace a query pod every hour.** Use one of these two methods:

   - from the driver, which records each replacement as an event with its exit status:

     ```bash
     --replace-every 3600 \
     --replace-cmd 'kubectl -n kosha delete pod \
         $(kubectl -n kosha get pod -l app=kosha,role=query -o name | shuf -n1) --wait=false'
     ```

     The driver pod needs `kubectl` and a service account that may delete pods in `kosha`.
   - or from a workstation, in parallel with the run:

     ```bash
     while sleep 3600; do
       kubectl -n kosha delete "$(kubectl -n kosha get pod -l app=kosha,role=query -o name | shuf -n1)" --wait=false
     done
     ```

   Watch that each new replica becomes ready, and how long it takes to warm. The connection-
   error and 5xx gates cover the requests that were in flight on the deleted pod. The
   per-window p99 in the report shows the cold-cache cost after each replacement.

5. **Collect** `/soak-report.json` (`kubectl cp`), and keep it with the run's Grafana snapshot.
   Check memory and disk for a plateau: the report samples RSS and cache bytes when the pods
   export them, and the dashboards show the rest. Then undo the HPA patch and delete the
   `soak` pod. Drop the soak namespaces with `DELETE /v1/namespaces/{ns}` per namespace. After
   a run they no longer hold exactly the preload, so a later `--no-preload` run needs a
   fresh preload under a new `--run-id`.
