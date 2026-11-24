# Kosha k8s manifests

Kustomize layout for running `kosha-server` in its own **`kosha`**
namespace. Manifests stay product-generic: no customer account names,
bucket names, or IAM role ARNs are committed here.

For segment bucket versioning, PostgreSQL PITR and the quarterly namespace
recovery drill, see [Backups and namespace point-in-time recovery](../docs/operations/backup-restore.md).

    k8s/
      base/     Namespace, ServiceAccount, ConfigMap, three Deployments
                (kosha = ingest, kosha-query = query, kosha-indexer =
                compaction), two Services
                (kosha-service, kosha-ingest-service), an HPA for the query
                tier — env-agnostic defaults only (includes kosha NVMe
                node-pool scheduling for the ingest Deployment; see below)
      components/internal-tls/
                opt-in: TLS between pods + HTTPS probes (see "Encryption in
                transit"); not included by stage or prod
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

  The deployment and HPA start at three replicas. Zone spread requires
  eligible worker capacity across three AZs to place one replica in each;
  hostname anti-affinity prefers distinct nodes. The PDB keeps two query
  replicas available during voluntary evictions. See the
  [HA deployment and failover runbook](../docs/high-availability.md)
  before rolling this out or changing the ingest replica count.

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

## Encryption at rest

Both places tenant data rests must be encrypted. The full reference (env
vars, which key an object uses, key rotation) is in
`docs/configuration.md`, "Encryption at rest".

**S3.** Set `KOSHA_S3_SSE=aws:kms` (and optionally
`KOSHA_S3_SSE_KMS_KEY_ID`, `KOSHA_S3_SSE_BUCKET_KEY=true` and per-tenant
`KOSHA_S3_TENANT_KMS_KEYS` / `KOSHA_S3_TENANT_KMS_KEYS_FILE`) in the
ConfigMap; see the commented example in `base/configmap.yaml`. A key on the
control-plane tenant record (`kosha.tenants.kms_key_id`) takes precedence over
the configured tenant map. Every pod
that reads or writes S3 (ingest, query, indexer, the GC CronJob and
`migrate`) runs as the `kosha` service account, so its IRSA role needs KMS
access to every key in use, the old key included until a rotation's hard
cutover is done:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "KoshaSegmentKms",
      "Effect": "Allow",
      "Action": ["kms:Decrypt", "kms:GenerateDataKey"],
      "Resource": [
        "arn:aws:kms:us-east-1:111122223333:key/DEFAULT-KEY-ID",
        "arn:aws:kms:us-east-1:111122223333:key/TENANT-KEY-ID"
      ],
      "Condition": {
        "StringEquals": { "kms:ViaService": "s3.us-east-1.amazonaws.com" }
      }
    }
  ]
}
```

Each key's policy must also allow that role (or delegate to IAM). With S3
Bucket Keys on, KMS sees the bucket ARN rather than the object ARN in the
encryption context, so a key policy that conditions on
`kms:EncryptionContext:aws:s3:arn` must name the bucket. A misconfigured key
shows up as `AccessDenied` on the first upload or hydration after the
rollout, so roll out to stage first.

**Local disk.** The ingest pod's NVMe instance store
(`/var/lib/kosha-cache`) is encrypted by the hardware on Nitro instance
types, with a per-instance key destroyed when the instance stops, so keep
the `NodePool` to Nitro types. The query and indexer pods' `emptyDir`
caches and every node's root volume are EBS: enable EBS encryption by
default for the account, and set `encrypted: true` in the `EC2NodeClass`
`blockDeviceMappings`. A PVC-backed cache needs an encrypted StorageClass:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: kosha-encrypted-gp3
provisioner: ebs.csi.aws.com
parameters:
  type: gp3
  encrypted: "true"
  # Optional: a customer-managed key instead of aws/ebs.
  # kmsKeyId: arn:aws:kms:us-east-1:111122223333:key/EBS-KEY-ID
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
```

## Encryption in transit

The reference for every setting is `docs/configuration.md`, "TLS between
pods", "TLS to Postgres" and "TLS to S3". Neither is turned on by the stage or prod
overlays; turning them on is a per-cluster step.

**Between pods.** `components/internal-tls` is an opt-in kustomize
component. It sets the `KOSHA_TLS_*` and `KOSHA_INTERNAL_TLS*` values in the
ConfigMap (HTTPS on port 8080, mTLS on `/internal/*`, HTTPS for peer
hydration and write forwarding), switches `KOSHA_INGEST_HOST` on the query
tier to `https://`, patches the readiness, startup and liveness probes
of all three Deployments to `scheme: HTTPS`, and adds the
`prometheus.io/scheme: https` pod annotation. Setting the certificate
variables without the probe change makes every probe fail, so use the
component rather than editing the ConfigMap. To include it from an overlay:

```yaml
# k8s/<overlay>/kustomization.yaml
resources:
  - ../base
components:
  - ../components/internal-tls
```

Before applying it:

1. Create the `kosha-internal-tls` Secret (`tls.crt`, `tls.key`, `ca.crt`)
   in the `kosha` namespace. `base/certificate-internal-tls.example.yaml`
   is a cert-manager example. The Deployments already mount it at
   `/etc/kosha/tls`.
2. Move every other client of port 8080 to `https://` in the same change:
   HTTPS is per port, so a pod with a certificate no longer answers plain
   HTTP. The component switches `base/cronjob-segment-gc.yaml` to HTTPS and
   mounts the CA for it. It does not patch `stage/cronjob-compaction.yaml`
   (switch its `curl` URL to `https://` and add `--cacert` with the Secret
   mounted). Outside this repo, that is every service calling `kosha-service`;
   add their hostnames to the certificate SANs.
3. Check the Prometheus scrape job. `/metrics` is on the same port, so it
   is HTTPS-only too (it needs no client certificate). The component adds
   `prometheus.io/scheme: https` next to the existing `prometheus.io/*`
   annotations, but that only takes effect if the scrape job relabels
   `__meta_kubernetes_pod_annotation_prometheus_io_scheme` onto
   `__scheme__`, and the job must be able to verify the certificate: give
   it the internal CA (`tls_config.ca_file`, with
   `server_name: kosha-internal` since pods are scraped by IP) or set
   `insecure_skip_verify: true`. A job that keeps scraping `http://` fails
   every scrape while the pods stay Ready, so watch `up` for the Kosha
   targets after the rollout.
4. Expect forwarded writes to fail while the rollout has ingest and query
   pods on different schemes. Peer hydration falls back to S3 instead.

The migrate Jobs (`base/job-migrate-namespace.yaml`,
`stage/es-to-kosha-migration-job.yaml`) and the usage reconciliation CronJob
(`base/cronjob-usage-reconcile.yaml`, #348) read the same ConfigMap but do not
need the `kosha-internal-tls` Secret: they make no pod-to-pod connection
and read only the `KOSHA_INTERNAL_TLS` mode, to apply the Postgres rule
below.

The component sets `KOSHA_INTERNAL_TLS=on`. Once every pod is on TLS, patch
it to `required` in the overlay so a pod with a plaintext hop refuses to
start.

**To Postgres.** The connection is encrypted by the URL in `kosha-secret`
(`database-url`, from the `KOSHA_DATABASE_URL` Environment secret), not by
a ConfigMap value:

    postgres://kosha:<password>@<host>:5432/kosha?sslmode=verify-full&sslrootcert=/etc/kosha/pg/ca.crt

`sslrootcert` is the CA that signed the database's certificate, mounted
into every pod that has `DATABASE_URL` (the three Deployments and the
migrate Jobs), for example from a ConfigMap:

```yaml
# overlay patch, per Deployment
spec:
  template:
    spec:
      containers:
        - name: kosha
          volumeMounts:
            - name: pg-ca
              mountPath: /etc/kosha/pg
              readOnly: true
      volumes:
        - name: pg-ca
          configMap:
            name: kosha-pg-ca   # key: ca.crt
```

`sslmode=require` encrypts without verifying the server and needs no CA.
With `KOSHA_INTERNAL_TLS=required`, a URL whose `sslmode` is anything else
than `require`, `verify-ca` or `verify-full` stops the pod at startup.

## Deploying

### Fresh install on an existing cluster

Use `scripts/kosha_cluster.py` from this checkout with Python 3.10+ and
`kubectl`. Before installing:

- Set up a kubeconfig context and permissions to deploy the base manifests.
  The cluster needs the [Kosha NVMe node pool](#the-kosha-node-pool-karpenter-nvme-instance-store)
  for ingest. The base manifests include the query PodDisruptionBudget, so
  the deploying identity also needs `policy/poddisruptionbudgets` (see
  `base/rbac-query-pdb.yaml`), and the three query replicas need schedulable
  capacity in three availability zones, or the install waits on the query
  rollout ([HA deployment](../docs/high-availability.md)).
- Provision an empty Postgres database and an unused S3 bucket/prefix.
  Give the Kosha service account access to S3 (and KMS if required). On
  EKS with IRSA, pass the role ARN below; the role must trust
  `system:serviceaccount:kosha:kosha` in this cluster. Leave creation of
  that service account to the installer.
- In namespace `kosha`, provision the Secret `kosha-secret` with keys
  `database-url` and `bootstrap-api-key` using your usual secret tooling.
  The installer neither creates nor overwrites that Secret.

```sh
python3 scripts/kosha_cluster.py --context customer-eks install \
  --image "ravidecoverai/kosha:${KOSHA_RELEASE_TAG}" \
  --s3-bucket customer-kosha --s3-prefix segments/ --aws-region us-east-1 \
  --irsa-role-arn arn:aws:iam::123456789012:role/kosha \
  --confirm-empty-storage
python3 scripts/kosha_cluster.py --context customer-eks status
```

Set `KOSHA_RELEASE_TAG` to a published tag or specify the image by digest.
The installer creates the `k8s/base` workloads and supporting resources
with cluster-specific S3, image, and optional IRSA settings, then waits
for ingest, query, and indexer to roll out. `status` is a read-only snapshot
of the three Deployments' images and readiness counts.

**Install only.** The command refuses to run if any Kosha Deployment
already exists, including after a partial install. Do not use it with
storage from a previous installation, even on a new Kubernetes cluster:
`--confirm-empty-storage` is your confirmation, not a storage check. For
upgrades or rollbacks, follow the [manifest and WAL compatibility
rules](../README.md#s3-wal-kosha_wal_modes3) in the main README before
resuming writes; old binaries cannot read the new formats.

### Stage and production workflows

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
  `make migration-job-manifest MIGRATION_INDICES="<index> ..."` (the exact
  index names or aliases to copy; nothing is defaulted). OpenSearch host/IAM
  for that Job are environment-specific — edit or regenerate before use.
- To copy through the public `/v1` API instead (no S3 or Postgres access),
  use `scripts/copy_es_to_kosha.py`; see
  [docs/migration/from-elasticsearch.md](../docs/migration/from-elasticsearch.md).

## Migrating pre-#262 namespaces

Since #262 every client namespace resolves to `<tenant>/<name>`, and the
segment prefix in S3 embeds that id. Namespaces written before #262 live
under bare ids (`paragraph_index_hnsw`, the bench corpora) that no tenant
can address. A server booting on such a control plane logs:

    WARN: 2 namespace(s) predate tenant scoping (#262) and are unreachable through /v1/namespaces: ...

`kosha-server migrate-namespace` (#344) moves bare namespaces to
`<tenant>/<name>`, one at a time or as a batch. It is an offline command (a
Job, or by hand), not an endpoint. It needs `DATABASE_URL` and the same S3
settings as the server (`KOSHA_S3_BUCKET`, `KOSHA_S3_PREFIX`, ...), so
`kosha-config` and `kosha-secret` are all it needs. Without
`KOSHA_S3_BUCKET` it moves segments inside `KOSHA_DATA_DIR`.

    kosha-server migrate-namespace --list-unreachable
    kosha-server migrate-namespace --from <bare> --to <tenant>/<name> [--dry-run] [--keep-source]
    kosha-server migrate-namespace --map <file> [--dry-run] [--keep-source]
    kosha-server migrate-namespace --all-unreachable --tenant <tenant> [--dry-run] [--keep-source]
    kosha-server migrate-namespace --verify (--map <file> | --from <bare> --to <tenant>/<name> | --all-unreachable)

A map file holds one `bare=tenant/name` per line (`#` starts a comment), or
JSON: `{"bare": "tenant/name", ...}` or
`[{"from": "bare", "to": "tenant/name"}, ...]`. A bare id or destination
listed twice is rejected before anything runs. `--all-unreachable --tenant
<t>` maps every bare id `b` to `<t>/<b>`, for a deployment with one tenant.

A batch runs the steps below for each namespace in order. When it refuses
one (a live writer, a WAL tail, a collision, a branch) it prints the
reason and moves on to the next. It ends with a summary table, one row per
namespace (`moved`, `would move`, `skipped`, `refused`, with documents,
objects and the reason), and exits non-zero if it refused any. `--dry-run`
covers the whole batch. A rerun skips the namespaces already moved, so fix
whatever it refused and run the same batch again.

`--verify` re-checks a finished migration and changes nothing. For each
pair it checks that the bare id is gone from the control plane, the
destination exists, and every segment the destination's manifest lists has
objects under `<tenant>/<name>/`. It prints one row per namespace (`ok` or
`FAILED`, documents, segments), notes source objects a `--keep-source` run
left behind, and lists any bare namespace that is still unreachable. It
exits non-zero if a row failed or a bare namespace is left. After an
`--all-unreachable` run, `--verify --all-unreachable` can only check that
no bare id is left. Use a map file to get the per-namespace checks.

What a run does for each namespace, in order:

1. Takes the writer lease (#254) on both ids. A live writer holding either
   lease makes it refuse: stop the writer first.
2. Refuses when the WAL holds records for the bare id past the manifest's
   checkpoint. With `KOSHA_WAL_MODE=s3` it reads every stream under `_wal/`
   in the bucket. With a local WAL it reads `KOSHA_DATA_DIR/_wal`, which
   lives on the ingest pod's disk; when that is not mounted, let the
   writer's idle flush run, stop the writer and pass `--skip-wal-check`
   (but see step 5: prefer mounting the WAL).
3. Copies every object of the manifest's segments (data files, footers,
   deletion bitmaps, `_COMMIT` markers last) from `<bare>/` to
   `<tenant>/<name>/` with server-side `CopyObject`. Objects already copied at
   the same size are skipped. Objects under `<bare>/` that belong to no
   segment of the manifest are left alone: if a tenant is named like the
   bare namespace, its namespaces share the prefix.
4. Checks that every copied object is at the destination with the same
   size, and that counts and bytes match.
5. Marks the bare id discarded in every WAL stream it checked, through its
   highest LSN there (the `discarded` WAL meta entry a namespace DELETE
   writes). Without the bare id's manifest, a writer's recovery would
   otherwise replay its already-flushed records still sitting in group
   files and republish a stale bare namespace. `--skip-wal-check` cannot
   do this: a WAL group file still holding records of the bare id replays
   them on the writer's next boot.
6. Re-keys the control plane in one Postgres transaction:
   `kosha.namespaces`, `kosha.manifests`, `kosha.writer_leases` (the token
   is carried over; the old id's lease is bumped and expired) and
   `kosha.retired_namespaces` (the bare id is retired at its last version).
   It fails, and changes nothing, when `<tenant>/<name>` already exists or
   the manifest moved since step 2.
7. Reopens the manifest under `<tenant>/<name>` and checks the segment list
   and document count.
8. Deletes the copied objects under `<bare>/` (not with `--keep-source`).

A rerun is safe at any point. Before step 6 it starts over and skips what
is already copied; after step 6 it finds the new id in the control plane
and only deletes source objects (so `--keep-source` now, plain rerun
later, deletes the source when you are ready). From then on the new id
is authoritative, so a rerun never copies: it deletes each source object
whose destination object exists, and leaves the others alone.

It refuses a sharded namespace and a namespace whose segments a branch of
another namespace reads (#208); move or delete those by hand.

### Staging / prod procedure

Do this for each environment before it takes a build past #262, or right
after (the data is invisible, not lost, until it is moved).

1. List what needs moving and pick the tenant for each namespace (the
   tenant of the API key its clients use; `default` for `KOSHA_API_KEY`).
   Write one `bare=tenant/name` line per namespace into the
   `namespaces.map` key of the ConfigMap in
   `k8s/base/job-migrate-namespace.yaml`, and set that file's image to the
   tag the cluster runs:

       kubectl -n kosha exec deploy/kosha -- kosha-server migrate-namespace --list-unreachable

2. Let the writer flush, then stop it, so nothing writes the bare ids while
   they move. No client can write a bare id since #262, and the idle flush
   (`KOSHA_FLUSH_MAX_AGE_MS`, default 1 s; check it is not `0`) publishes
   whatever the writer still buffers or recovered from its WAL, bare
   namespaces included. Give it a few seconds after boot, then:

       kubectl -n kosha scale deploy/kosha --replicas=0

   Query pods can keep serving other namespaces.

3. Dry run the whole batch. The Job ships with `MODE: dry-run`. With a
   local WAL (no `KOSHA_WAL_MODE=s3`) the Job cannot read the ingest pod's
   WAL, so set `EXTRA_ARGS` to `--skip-wal-check`, but only after step 2:

       kubectl apply -f k8s/base/job-migrate-namespace.yaml
       kubectl -n kosha logs -f job/kosha-migrate-namespace

   Check each namespace's plan (objects, bytes, documents) and the summary
   table. Every row should read `would move`. Fix anything `refused`
   before going on.

4. Real run: set `MODE` to `run` and run the Job again. Add
   `--keep-source` to `EXTRA_ARGS` for prod's first run if you want the old
   objects kept until clients have checked the data:

       kubectl -n kosha delete job kosha-migrate-namespace
       kubectl apply -f k8s/base/job-migrate-namespace.yaml
       kubectl -n kosha logs -f job/kosha-migrate-namespace

   The Job fails if it refused any namespace. The log says why for each
   one. Fix the cause and repeat this step: the namespaces already moved
   show as `skipped`.

5. Start the writer and restart the query pods, so every pod drops its
   view of the bare ids:

       kubectl -n kosha scale deploy/kosha --replicas=1
       kubectl -n kosha rollout restart deploy/kosha-query

6. Verify. Set `MODE` to `verify` and run the Job once more. It must end
   with `verify: OK` and `unreachable bare namespaces: none`, and every
   row's `DOCS` must equal what step 4 printed for that namespace:

       kubectl -n kosha delete job kosha-migrate-namespace
       kubectl apply -f k8s/base/job-migrate-namespace.yaml
       kubectl -n kosha logs -f job/kosha-migrate-namespace

   Spot-check a namespace as its tenant too:

       curl -H "Authorization: Bearer <tenant key>" "$KOSHA/v1/namespaces/<name>/stats"

   The boot warning no longer lists any namespace. After a `--keep-source`
   run, the verify rows note the source objects left behind: rerun step 4
   without `--keep-source` when you are ready to delete them. Local disk
   caches still hold the old copies under `<bare>/`. They are only a cache
   and can be deleted.

To move a single namespace by hand instead, run the same command from a
one-off pod with the server's configuration:

    kubectl -n kosha run kosha-migrate-ns --rm -it --restart=Never \
      --image=<registry>/decover/kosha:<tag> \
      --overrides='{"spec":{"serviceAccountName":"kosha","containers":[{"name":"kosha-migrate-ns","image":"<registry>/decover/kosha:<tag>","args":["migrate-namespace","--from","<bare>","--to","<tenant>/<name>","--dry-run"],"envFrom":[{"configMapRef":{"name":"kosha-config"}},{"secretRef":{"name":"kosha-secret"}}]}]}}'

## Local verification

    kustomize build k8s/stage
    kubectl apply --dry-run=client -k k8s/stage
    # with real cluster credentials:
    kubectl apply --dry-run=server -k k8s/stage
