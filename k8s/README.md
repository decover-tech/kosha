# Kosha k8s manifests

Kustomize layout for running `kosha-server` in its own **`kosha`**
namespace. Manifests stay product-generic: no customer account names,
bucket names, or IAM role ARNs are committed here.

    k8s/
      base/     Namespace, ServiceAccount, ConfigMap, Deployment, Service —
                env-agnostic defaults only (includes kosha NVMe node-pool
                scheduling; see below)
      stage/    floating `:main` image tag + larger memory for warm hydration
      prod/     versioned release image tag

## Contract with the infra / deploy environment

| Field | In this repo | Provided at deploy time |
|-------|--------------|-------------------------|
| K8s namespace | `kosha` | — |
| ServiceAccount | `kosha` | IRSA annotation |
| Service DNS | `kosha-service.kosha.svc.cluster.local` | — |
| S3 bucket / prefix | — | `KOSHA_S3_BUCKET`, `KOSHA_S3_PREFIX` |
| AWS region | — | `KOSHA_AWS_REGION` (default `us-east-1`) |
| IRSA role ARN | — | `KOSHA_IRSA_ROLE_ARN` |
| Postgres URL / API key | — | `KOSHA_DATABASE_URL`, `KOSHA_API_KEY` |
| EKS cluster name | — | `EKS_CLUSTER_NAME` |

Customer-specific staging/prod wiring (bucket names, role ARNs, cluster
names) lives in **infra Terraform outputs** and **GitHub Environment
secrets** for this repo's deploy workflows — not in these YAML files.

Clients reach Kosha at `http://kosha-service.kosha.svc.cluster.local:8080`.

## The kosha node pool (Karpenter, NVMe instance store)

Kosha runs on a dedicated Karpenter pool whose nodes have a local NVMe
instance store, formatted and mounted at `/var/lib/kosha-cache` by the
`EC2NodeClass` user-data. `base/deployment.yaml` is pinned to that pool:

- `nodeSelector: kosha.dev/instance-store: nvme` — the label is unique to
  the pool
- toleration for `dedicated=kosha:NoSchedule` — the pool's taint keeps
  everything else off it
- the `cache` volume is a `hostPath` on `/var/lib/kosha-cache`, mounted at
  `/var/cache/kosha` (so `KOSHA_DATA_DIR` / `KOSHA_CACHE_DIR` in the
  ConfigMap are unchanged)

Consequences worth knowing:

- **`strategy: Recreate`.** Two kosha pods must never share a node, or a
  rolling update's surge pod would write the same host directory as the
  outgoing pod. Recreate costs a few seconds of downtime per deploy; the
  Deployment is a single replica regardless. If Kosha ever needs >1
  replica, add a `requiredDuringSchedulingIgnoredDuringExecution` pod
  anti-affinity on `app: kosha` over `kubernetes.io/hostname` instead.
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
4. `kubectl apply -k` and rolls out `deployment/kosha` in namespace `kosha`

### Required GitHub Environment secrets

| Secret | Purpose |
|--------|---------|
| `AWS_GITHUB_ROLE_ARN` | OIDC deploy role |
| `KOSHA_DATABASE_URL` | Postgres URL for control plane |
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
