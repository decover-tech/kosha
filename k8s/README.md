# Kosha k8s manifests

Kustomize layout for running `kosha-server` in its own **`kosha`**
namespace. Manifests stay product-generic: no customer account names,
bucket names, or IAM role ARNs are committed here.

    k8s/
      base/     Namespace, ServiceAccount, ConfigMap, two Deployments
                (kosha = ingest, kosha-query = query), two Services
                (kosha-service, kosha-ingest-service), an HPA for the query
                tier — env-agnostic defaults only (includes kosha NVMe
                node-pool scheduling for the ingest Deployment; see below)
      stage/    floating `:main` image tag + larger memory for warm hydration
      prod/     versioned release image tag

## Query/ingest split and request routing

Kosha runs as two Deployments sharing one binary, differentiated by
`KOSHA_ROLE` (set explicitly per-Deployment, overriding the shared
ConfigMap):

- **`kosha`** (`role: ingest`) — the single, pinned write target. Exactly
  one replica, on the dedicated NVMe pool (see below). Two pods writing the
  same namespace concurrently could each pick the same next segment ID and
  silently clobber each other's data (`kosha-write::Indexer`'s segment
  counter has no cross-pod coordination) — this Deployment must never scale
  beyond 1.
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
`is_write_path`/`forward_to_ingest` for the implementation — `KOSHA_ROLE`
being decorative (read but never branched on) was true until this existed;
now it gates this one thing. `kosha-ingest-service` is not meant for direct
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
  swap costs a cold hydration, not data.
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
| `KOSHA_API_KEY` | Bootstrap / client API key |
| `KOSHA_S3_BUCKET` | Segment bucket name |
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
    kubectl rollout restart deployment/kosha -n kosha

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
  unreferenced; they can be garbage-collected later. During migration, the
  namespace is only partially populated, so keep Sage reads on OpenSearch
  until the Job completes and Kosha is restarted.
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
