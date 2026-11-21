# Kosha k8s manifests

Kustomize layout for running `kosha-server` in its own **`kosha`**
namespace. Manifests stay product-generic: no customer account names,
bucket names, or IAM role ARNs are committed here.

    k8s/
      base/     Namespace, ServiceAccount, ConfigMap, three Deployments
                (kosha = ingest, kosha-query = query, kosha-indexer =
                compaction), two Services
                (kosha-service, kosha-ingest-service), an HPA for the query
                tier — env-agnostic defaults only (includes kosha NVMe
                node-pool scheduling for the ingest Deployment; see below)
      stage/    floating `:main` image tag + larger memory for warm hydration
      prod/     versioned release image tag

## Query/ingest split and request routing

Kosha runs as three Deployments sharing one binary, differentiated by
`KOSHA_ROLE` (set explicitly per-Deployment, overriding the shared
ConfigMap). The role is validated at startup (#254): `query` requires
`KOSHA_INGEST_HOST`, `ingest`/`indexer` must not set it, `indexer` requires
`DATABASE_URL`, and a contradiction exits the pod with a clear message. See
`docs/configuration.md#roles`.

- **`kosha`** (`role: ingest`) — the single, pinned write target. Exactly
  one replica, on the dedicated NVMe pool (see below), `Recreate` strategy.
  Since #254 it holds a per-namespace writer lease in Postgres
  (`kosha.writer_leases`) and every manifest CAS checks the lease's fencing
  token, so a second writer started by mistake is refused (503) instead of
  silently rebasing onto the first. Keep `replicas: 1` + `Recreate` anyway:
  the lease makes a second writer safe, not useful, and binaries from before
  #254 do not check fencing (roll every writer to a fencing build before
  relying on it). A restarted pod that did not release its lease waits up to
  `KOSHA_WRITER_LEASE_TTL_MS` (30 s) before it accepts writes for a
  namespace the old pod held. It sets `KOSHA_COMPACTION_ON_INGEST=false`.
- **`kosha-indexer`** (`role: indexer`) — compaction only
  (`deployment-indexer.yaml`, #254). One replica, `Recreate`. Runs the
  compaction scheduler, follows the Postgres manifests, merges segments it
  hydrates from S3 and publishes by manifest CAS + rebase. Serves only
  probes, `POST /v1/admin/compact*` and `GET /v1/admin/compaction-*`
  (everything else is 404); no Service routes to it. Stage/prod overlays for
  its resources are left to ops.
- **`kosha-query`** (`role: query`) — stateless, horizontally-scaled reads.
  No hostPath, no NVMe pinning, an `emptyDir` cache that cold-starts from
  the Postgres manifest + S3 segments (bounded, ~15-20s for a full
  namespace). Scaled by `hpa-query.yaml` on CPU utilization — requires the
  cluster's metrics-server to actually be healthy (`kubectl top pods -n
  kosha` returning real numbers, not `FailedGetResourceMetric`) or the HPA
  is a no-op.

**Clients only ever need `kosha-service`** — for both reads and writes,
exactly like a single OpenSearch cluster endpoint. `kosha-service` selects
`role: query`; a query pod handles reads itself and *transparently forwards*
write-path requests (`/index`, `/flush`, `/replace`, `/delete`,
`/v1/admin/*`) to `kosha-ingest-service` (which selects `role: ingest`),
relaying the response back verbatim. See `crates/kosha-server/src/main.rs`'s
`is_write_path`/`forward_to_ingest` for the implementation.
`kosha-ingest-service` is not meant for direct
client use; it exists as the forwarding target and an optional bypass for
admin/migration scripts.

One extra internal network hop per write (client → query pod → ingest pod
→ back) is the accepted tradeoff for zero client-side routing logic and no
cross-repo config change. Writes aren't the CPU bottleneck here — search
scoring is — so this is cheap.

## Contract with the infra / deploy environment

| Field | In this repo | Provided at deploy time |
|-------|--------------|-------------------------|
| K8s namespace | `kosha` | — |
| ServiceAccount | `kosha` | IRSA annotation |
| Service DNS (client-facing) | `kosha-service.kosha.svc.cluster.local` | — |
| Service DNS (internal, ingest-only) | `kosha-ingest-service.kosha.svc.cluster.local` | — |
| S3 bucket / prefix | — | `KOSHA_S3_BUCKET`, `KOSHA_S3_PREFIX` |
| AWS region | — | `KOSHA_AWS_REGION` (default `us-east-1`) |
| IRSA role ARN | — | `KOSHA_IRSA_ROLE_ARN` |
| Postgres URL / API key | — | `KOSHA_DATABASE_URL`, `KOSHA_API_KEY` |
| EKS cluster name | — | `EKS_CLUSTER_NAME` |

Customer-specific staging/prod wiring (bucket names, role ARNs, cluster
names) lives in **infra Terraform outputs** and **GitHub Environment
secrets** for this repo's deploy workflows — not in these YAML files.

Clients reach Kosha at `http://kosha-service.kosha.svc.cluster.local:8080` —
for both reads and writes; see the split section above for why this hasn't
changed despite Kosha now running as two Deployments.

## The kosha node pool (Karpenter, NVMe instance store)

This section is about the **ingest Deployment only** (`base/deployment.yaml`,
`role: ingest`). The query tier (`base/deployment-query.yaml`) deliberately
does not use this pool or a hostPath cache — see the split section above.

The ingest pod runs on a dedicated Karpenter pool whose nodes have a local
NVMe instance store, formatted and mounted at `/var/lib/kosha-cache` by the
`EC2NodeClass` user-data. `base/deployment.yaml` is pinned to that pool:

- `nodeSelector: kosha.dev/instance-store: nvme` — the label is unique to
  the pool
- toleration for `dedicated=kosha:NoSchedule` — the pool's taint keeps
  everything else off it
- the `cache` volume is a `hostPath` on `/var/lib/kosha-cache`, mounted at
  `/var/cache/kosha` (so `KOSHA_DATA_DIR` / `KOSHA_CACHE_DIR` in the
  ConfigMap are unchanged)

Consequences worth knowing:

- **`strategy: Recreate`.** Two ingest pods must never share a node, or a
  rolling update's surge pod would write the same host directory as the
  outgoing pod. Recreate costs a few seconds of downtime per deploy; this
  Deployment stays a single replica permanently — see the split section
  above for why (uncoordinated segment-counter writes, not just a
  hostPath scheduling detail). The historical note this replaced said "if
  Kosha ever needs >1 replica, add pod anti-affinity instead" — that's now
  answered by the query tier existing as a *separate* Deployment with its
  own `preferredDuringSchedulingIgnoredDuringExecution` anti-affinity
  (`deployment-query.yaml`), not by scaling this one.
- **Cache capacity is the node's, not the pod's.** hostPath usage is not
  tracked by the kubelet, so `ephemeral-storage` requests/limits are sized
  for the container filesystem only (logs, `/tmp`) — deliberately small, so
  scheduling isn't constrained against the node's small root EBS volume.
  Sizing the cache means choosing instance types in the `NodePool`.
- **The cache survives pod restarts, not node replacement.** Contents are
  rebuildable from the Postgres manifest + S3 segments, so a Karpenter node
  swap costs a cold hydration, not data. The exception is the default local
  WAL (`data_dir/_wal`): acknowledged but unflushed writes are lost with the
  node. Set `KOSHA_WAL_MODE=s3` on the ingest Deployment to keep the WAL in
  the bucket instead (see the root README, "S3 WAL").
- **Prod needs the same pool before it deploys.** These constraints live in
  `base/`, so `k8s/prod` inherits them; without the label and taint on the
  prod cluster, the pod sits `Pending` forever.

## Deploying

`.github/workflows/release.yml` / `deploy.yml` call
`.github/actions/deploy-eks`, which:

1. Reads Environment secrets (DB URL, API key, S3, IRSA, cluster)
2. `kustomize edit set image` + generates `kosha-secret`
3. Patches ConfigMap / ServiceAccount with the env-specific S3 + IRSA values
4. `kubectl apply -k` and rolls out **both** `deployment/kosha` (ingest) and
   `deployment/kosha-query` (query) in namespace `kosha`, waiting on each in
   turn — a failed rollout on either one fails the deploy.

### Required GitHub Environment secrets

| Secret | Purpose |
|--------|---------|
| `AWS_GITHUB_ROLE_ARN` | OIDC deploy role |
| `KOSHA_DATABASE_URL` | Postgres URL for control plane. Staging points at the in-cluster Postgres StatefulSet in `decoverai-services` (`postgres-service.decoverai-services.svc.cluster.local:5432/kosha`, not the old RDS instance) — see backend's `deployments/k8s/base/postgres.yaml`. Cross-namespace, so the FQDN is required; the bare `postgres-service` short name won't resolve from the `kosha` namespace. |
| `KOSHA_API_KEY` | Bootstrap tenant API key (tenant `default`). Admin and peer access use the separate `KOSHA_ADMIN_API_KEY` / `KOSHA_INTERNAL_API_KEY`. The base Deployments read `KOSHA_INTERNAL_API_KEY` from the optional `kosha-secret` key `internal-api-key`; until it is set, query pods serve searches with `eventual` consistency (#255). The server refuses to start when no credential is configured at all. |
| `KOSHA_S3_BUCKET` | Segment bucket name |
| `KOSHA_GC_GRACE` / `KOSHA_GC_ORPHAN_GRACE` | Segment GC (#239): how long a retired segment (default `1h`) or a never-referenced orphan (default `24h`) stays in S3 before `POST /v1/admin/gc` deletes it. `KOSHA_LOCAL_GC_GRACE` (default `5m`) is the same for local disk. |
| `KOSHA_S3_PREFIX` | Key prefix (e.g. `segments/`) |
| `KOSHA_IRSA_ROLE_ARN` | `eks.amazonaws.com/role-arn` value |
| `EKS_CLUSTER_NAME` | Target EKS cluster |
| `KOSHA_AWS_REGION` | Optional, default `us-east-1` |

`rbac.yaml` is a one-time, human-reviewed bootstrap per cluster
(`kubectl apply -f k8s/base/rbac.yaml`).

## Migrating data from OpenSearch → Kosha

`k8s/stage/es-to-kosha-migration-job.yaml` is a one-shot direct backfill
(DESIGN.md §14). Apply by hand after the `:main` image containing migrate
is deployed:

    kubectl apply -f k8s/stage/es-to-kosha-migration-job.yaml
    kubectl logs -n kosha -f job/es-to-kosha-migration

Notes:

- **Scheduling:** the Job has *no* nodeSelector/toleration for the kosha
  pool, on purpose — it uses its own `emptyDir` scratch, and sharing the
  pool's `/var/lib/kosha-cache` would mean writing the live server's data
  directory. It therefore needs some other schedulable nodegroup in the
  cluster. If the kosha pool is now the only one, add the toleration +
  nodeSelector to `scripts/gen_es_migration_job.py` and drop the Job's
  `ephemeral-storage` request to fit the node's root EBS volume (its
  `emptyDir` is kubelet-root-backed, *not* the NVMe mount).
- **IAM prerequisite:** the `kosha` service account IRSA already writes S3;
  it must also allow `es:ESHttp*` on the staging OpenSearch domain. Without
  that policy the Job fails immediately with an explicit 403.
- `DATABASE_URL` comes from `kosha-secret`; S3/data-dir settings come from
  `kosha-config`. No Kosha API key is needed because this bypasses HTTP.
- A full run **replaces** the namespace manifest with newly-built segments
  (segment IDs continue past any prior/partial run). Old S3 segments become
  unreferenced; segment GC (`POST /v1/admin/gc`, or the suspended
  `kosha-segment-gc` CronJob) deletes them once they are older than
  `KOSHA_GC_ORPHAN_GRACE` (default 24h). Run it with `"dry_run": true`
  first to see what it would delete. During migration, the
  namespace is only partially populated, so keep Sage reads on OpenSearch
  until the Job completes. No restart is needed afterwards: query pods pick
  up the published manifest within `KOSHA_MANIFEST_REFRESH_MS` (default
  500 ms); check `manifest_version` / `manifest_lag_seconds` for the
  namespace in `GET /v1/stats` on a query pod to confirm.
- **Delta catch-up** (missing ids only): pass `--ids-file /path/ids.txt`
  (one OpenSearch `_id` per line). This implies `--append` — new segments are
  added to the existing manifest without wiping prior data. Prefer this over
  re-indexing ids that already exist.
- Use `POST /exists` `{"namespace","ids"}` to compute a missing-id set for
  `--ids-file`. Prefer `POST /replace` for partial field merges; `/index`
  upserts the full document by id.
- For a 500-document smoke test, run one alias into a scratch namespace by
  adding `--limit 500 --namespace migration-smoke` and retaining exactly one
  `--index`.
- Regenerate the manifest after CLI/Job changes with
  `make migration-job-manifest`. OpenSearch host/IAM for that Job are
  environment-specific — edit or regenerate before use.

## Migrating pre-#262 namespaces

Since #262 every client namespace resolves to `<tenant>/<name>`, and the
segment prefix in S3 embeds that id. Namespaces written before #262 live
under bare ids (`paragraph_index_hnsw`, the bench corpora) that no tenant
can address. A server booting on such a control plane logs:

    WARN: 2 namespace(s) predate tenant scoping (#262) and are unreachable through /v1/namespaces: ...

`kosha-server migrate-namespace` (#344) moves one bare namespace to
`<tenant>/<name>`. It is an offline command (a Job, or by hand), not an
endpoint. It needs `DATABASE_URL` and the same S3 settings as the server
(`KOSHA_S3_BUCKET`, `KOSHA_S3_PREFIX`, ...), so `kosha-config` and
`kosha-secret` are all it needs. Without `KOSHA_S3_BUCKET` it moves
segments inside `KOSHA_DATA_DIR`.

    kosha-server migrate-namespace --list-unreachable
    kosha-server migrate-namespace --from <bare> --to <tenant>/<name> [--dry-run] [--keep-source]

What a run does, in order:

1. Takes the writer lease (#254) on both ids. A live writer holding either
   lease makes it refuse: stop the writer first.
2. Refuses when the WAL holds records for the bare id past the manifest's
   checkpoint. With `KOSHA_WAL_MODE=s3` it reads every stream under `_wal/`
   in the bucket. With a local WAL it reads `KOSHA_DATA_DIR/_wal`, which
   lives on the ingest pod's disk; when that is not mounted, let the
   writer's idle flush run, stop the writer and pass `--skip-wal-check`.
3. Copies every object of the manifest's segments (data files, footers,
   deletion bitmaps, `_COMMIT` markers last) from `<bare>/` to
   `<tenant>/<name>/` with server-side `CopyObject`. Objects already copied at
   the same size are skipped. Objects under `<bare>/` that belong to no
   segment of the manifest are left alone: if a tenant is named like the
   bare namespace, its namespaces share the prefix.
4. Checks that every copied object is at the destination with the same
   size, and that counts and bytes match.
5. Re-keys the control plane in one Postgres transaction:
   `kosha.namespaces`, `kosha.manifests`, `kosha.writer_leases` (the token
   is carried over; the old id's lease is bumped and expired) and
   `kosha.retired_namespaces` (the bare id is retired at its last version).
   It fails, and changes nothing, when `<tenant>/<name>` already exists or
   the manifest moved since step 2.
6. Reopens the manifest under `<tenant>/<name>` and checks the segment list
   and document count.
7. Deletes the copied objects under `<bare>/` (not with `--keep-source`).

A rerun is safe at any point. Before step 5 it starts over and skips what
is already copied; after step 5 it finds the new id in the control plane
and only finishes steps 6 and 7 (so `--keep-source` now, plain rerun
later, deletes the source when you are ready).

It refuses a sharded namespace and a namespace whose segments a branch of
another namespace reads (#208); move or delete those by hand.

### Staging / prod procedure

Do this for each environment before it takes a build past #262, or right
after (the data is invisible, not lost, until it is moved).

1. List what needs moving and pick the tenant for each namespace (the
   tenant of the API key its clients use; `default` for `KOSHA_API_KEY`):

       kubectl -n kosha exec deploy/kosha -- kosha-server migrate-namespace --list-unreachable

2. Let the writer flush, then stop it, so nothing writes the bare id while
   it moves. No client can write a bare id since #262, and the idle flush
   (`KOSHA_FLUSH_MAX_AGE_MS`, default 1 s; check it is not `0`) publishes
   whatever the writer still buffers or recovered from its WAL, bare
   namespaces included. Give it a few seconds after boot, then:

       kubectl -n kosha scale deploy/kosha --replicas=0

   Query pods can keep serving other namespaces.

3. Dry run, then the real run, from a one-off pod with the server's
   configuration. With a local WAL, mount the ingest data dir or add
   `--skip-wal-check` (only after step 2):

       kubectl -n kosha run kosha-migrate-ns --rm -it --restart=Never \
         --image=<registry>/decover/kosha:<tag> \
         --overrides='{"spec":{"serviceAccountName":"kosha","containers":[{"name":"kosha-migrate-ns","image":"<registry>/decover/kosha:<tag>","args":["migrate-namespace","--from","<bare>","--to","<tenant>/<name>","--dry-run"],"envFrom":[{"configMapRef":{"name":"kosha-config"}},{"secretRef":{"name":"kosha-secret"}}]}]}}'

   Check the plan (objects, bytes, documents), then run it again without
   `--dry-run`. Use `--keep-source` for prod's first run if you want the
   old objects kept until clients have checked the data.

4. Start the writer and restart the query pods, so every pod drops its
   view of the bare id:

       kubectl -n kosha scale deploy/kosha --replicas=1
       kubectl -n kosha rollout restart deploy/kosha-query

5. Verify as the tenant. The document count must equal the one the run
   printed:

       curl -H "Authorization: Bearer <tenant key>" "$KOSHA/v1/namespaces/<name>/stats"

   The boot warning no longer lists the namespace. Local disk caches still
   hold the old copies under `<bare>/`; they are only a cache and can be
   deleted.

## Local verification

    kustomize build k8s/stage
    kubectl apply --dry-run=client -k k8s/stage
    # with real cluster credentials:
    kubectl apply --dry-run=server -k k8s/stage
