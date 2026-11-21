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

## Encryption at rest

Both places tenant data rests must be encrypted. The full reference (env
vars, which key an object uses, key rotation) is in
`docs/configuration.md`, "Encryption at rest".

**S3.** Set `KOSHA_S3_SSE=aws:kms` (and optionally
`KOSHA_S3_SSE_KMS_KEY_ID`, `KOSHA_S3_SSE_BUCKET_KEY=true` and per-tenant
`KOSHA_S3_TENANT_KMS_KEYS` / `KOSHA_S3_TENANT_KMS_KEYS_FILE`) in the
ConfigMap; see the commented example in `base/configmap.yaml`. Every pod
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

Encryption in transit between pods (peer hydration, read-to-write
forwarding) is not covered yet; it follows the HTTP server replacement
(#259).

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

## Local verification

    kustomize build k8s/stage
    kubectl apply --dry-run=client -k k8s/stage
    # with real cluster credentials:
    kubectl apply --dry-run=server -k k8s/stage
