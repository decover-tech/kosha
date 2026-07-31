# Kosha k8s manifests

Kustomize layout for running `kosha-server` in its own `kosha` namespace on
the real EKS clusters (`decoverai-nonprod` staging, `decoverai-prod`
production).

    k8s/
      base/     Namespace, ServiceAccount, ConfigMap, Deployment, Service —
                env-agnostic defaults only
      stage/    tracks the floating `:main` tag (ECR, what every merge to
                main pushes); configmap/serviceaccount patches plus a
                larger memory ceiling for warm hydration
      prod/     tracks versioned releases (Docker Hub, public); same S3/IRSA
                patch shape, with prod's own bucket + role (keeps base
                resource sizes until sized separately)

S3-backed segment storage and IRSA are wired in (`KOSHA_S3_BUCKET`,
`KOSHA_S3_PREFIX`, `AWS_DEFAULT_REGION` via ConfigMap; `eks.amazonaws.com/
role-arn` on the ServiceAccount) — these values were confirmed against the
already-running staging deployment (see below), not derived from the `infra`
Terraform repo directly. **Production's values follow the same
`{name_prefix}-kosha[-segments]` naming convention but are not independently
verified** (this session's AWS credentials only reached the nonprod
account) — confirm `decoverai-prod-kosha-segments` and
`arn:aws:iam::992382824254:role/decoverai-prod-kosha` actually exist before
the first production deploy.

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
  `base/`, so `k8s/prod` inherits them; without the label and taint on
  `decoverai-prod`, the pod sits `Pending` forever. Add it to that cluster's
  Karpenter config alongside the other unverified prod prerequisites below.

Backend services reach Kosha at `kosha-service.kosha.svc.cluster.local:8080`
(HTTP) / `:50051` (gRPC) — same DNS shape as the local dev setup documented
in `docs/local-development.md`.

## Deploying

`.github/workflows/release.yml` applies these automatically:

- `deploy-staging` — on push to `main`, after the multi-arch image merge job
- `deploy-production` — on `v*` tags, after the multi-arch image merge job

Each job assumes the AWS OIDC role in `secrets.AWS_GITHUB_ROLE_ARN` (scoped
per-environment via GitHub Environments `staging` / `production`), points
`kubectl` at the right cluster, uses `kustomize edit set image` to pin the
image that was just published, generates the `kosha-secret` Secret in-cluster
(not committed to git — see below), `kubectl apply`s, and force-restarts the
rollout (a `kubectl apply` alone is a no-op when a floating tag string like
`:main` hasn't changed).

### Prerequisites

- **staging — done.** GitHub Environment `staging` exists with
  `KOSHA_DATABASE_URL` / `KOSHA_API_KEY` (copied from the already-working
  live secret). EKS access: `GitHubActionsKoshaRole`
  (`arn:aws:iam::010928200670:role/GitHubActionsKoshaRole` — the same role
  already used for the ECR push, whose OIDC trust already covers
  `refs/heads/main`) was granted an EKS Access Entry mapping it to the
  `kosha-ci` Kubernetes group, and `k8s/base/rbac.yaml` (Role/RoleBinding
  scoped to the `kosha` namespace, plus a ClusterRole/ClusterRoleBinding
  scoped via `resourceNames` to just the `kosha` Namespace object) was
  applied by hand to `decoverai-nonprod`. Verified with a full
  `kubectl apply --dry-run=server` impersonating that exact role+group:
  all 5 objects the pipeline manages come back `unchanged`. Nothing left
  to do for `deploy-staging` to run.
- **production — not done.** Needs its own GitHub Environment secrets
  (`AWS_GITHUB_ROLE_ARN` for a role in the prod account, 992382824254 —
  doesn't exist yet; `KOSHA_DATABASE_URL`; `KOSHA_API_KEY`), plus the same
  `k8s/base/rbac.yaml` bootstrap + EKS Access Entry on `decoverai-prod`
  once that role exists.

`rbac.yaml` is intentionally not part of the kustomization the pipeline
applies (see the comment in `k8s/base/kustomization.yaml` for why) — it's a
one-time, human-reviewed `kubectl apply -f k8s/base/rbac.yaml` per cluster.

## A staging deployment already exists

As of this writing, `kosha` is already running in the `kosha` namespace on
`decoverai-nonprod` — deployed manually, ahead of this pipeline existing.
`kustomize build k8s/stage` was checked (`kubectl apply --dry-run=server`)
against the live cluster and every object comes back `configured` (an
update-in-place), not an error — so once the GitHub Environment secrets
above exist, the pipeline's `kubectl apply` will converge onto the existing
Deployment/Service/ConfigMap/ServiceAccount rather than replacing them,
with no need to delete anything first.

## Migrating staging data from OpenSearch → Kosha

`k8s/stage/es-to-kosha-migration-job.yaml` is a one-shot direct backfill
(DESIGN.md §14). `kosha-server migrate` sliced-scrolls each shared OpenSearch
alias (`paragraph_index_hnsw`, `page_index`, `findings_index`,
`document_index`, `completions_index`, `cases_index`), builds 20k-document
Kosha segments in-process with the WAL disabled, uploads each immutable
segment to S3, and publishes its Postgres manifest entry. It bypasses the
HTTP `/index` path entirely. Exact alias names keep Kosha namespaces aligned
with what Sage reads; missing names are skipped.

The Job is deliberately **not** part of the kustomize overlay — apply it by
hand after the `:main` image containing the migrate subcommand is deployed:

    # optional: preview what would move, without writing anything
    kubectl apply -f k8s/stage/es-to-kosha-migration-job.yaml --dry-run=server

    kubectl apply -f k8s/stage/es-to-kosha-migration-job.yaml
    kubectl logs -n kosha -f job/es-to-kosha-migration
    # after successful completion, reload manifests from Postgres:
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
  `--ids-file`. Prefer `POST /replace` (not `/index`) when rewriting an
  existing `doc_id` — `/index` still appends.
- For a 500-document smoke test, run one alias into a scratch namespace by
  adding `--limit 500 --namespace migration-smoke` and retaining exactly one
  `--index`.
- Regenerate the manifest after CLI/Job changes with
  `make migration-job-manifest`.

## Local verification

    kustomize build k8s/stage            # render, no cluster needed
    kubectl apply --dry-run=client -k k8s/stage   # validate against the k8s API schema
    # with real cluster credentials (aws eks update-kubeconfig --name decoverai-nonprod):
    kubectl apply --dry-run=server -k k8s/stage   # validate against the live API server, no changes persisted
