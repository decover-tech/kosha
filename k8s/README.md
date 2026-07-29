# Kosha k8s manifests

Kustomize layout for running `kosha-server` in its own `kosha` namespace on
the real EKS clusters (`decoverai-nonprod` staging, `decoverai-prod`
production).

    k8s/
      base/     Namespace, ServiceAccount, ConfigMap, Deployment, Service —
                env-agnostic defaults only
      stage/    tracks the floating `:main` tag (ECR, what every merge to
                main pushes); configmap-patch.yaml / serviceaccount-patch.yaml
                layer in staging's S3 bucket + IRSA role
      prod/     tracks versioned releases (Docker Hub, public); same S3/IRSA
                patch shape, with prod's own bucket + role

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

`k8s/stage/es-to-kosha-migration-job.yaml` is a self-contained one-shot
backfill (DESIGN.md §14): a ConfigMap carrying `scripts/copy_es_to_kosha.py`
plus a `batch/v1` Job that scrolls every backend search index on the staging
OpenSearch domain (`paragraph_index_hnsw`, `page_index`, `findings_index`,
`document_index`, `completions_index`, `cases_index` — copied by exact alias
name so Kosha namespaces match what Sage reads; missing names are skipped)
and replays each doc into the same-named Kosha namespace
via `kosha-service.kosha.svc.cluster.local:8080`. It is deliberately **not**
part of the kustomize overlay — apply it by hand, once:

    # optional: preview what would move, without writing anything
    kubectl apply -f k8s/stage/es-to-kosha-migration-job.yaml --dry-run=server

    kubectl apply -f k8s/stage/es-to-kosha-migration-job.yaml
    kubectl logs -n kosha -f job/es-to-kosha-migration

Notes:

- **ES auth is SigV4 via the node instance profile.** The Job intentionally
  has no `serviceAccountName`, so boto3 resolves the same EC2 node identity
  the backend pods use today against the ES domain. Don't point it at the
  `kosha` IRSA role (S3-only).
- **Kosha auth** comes from `kosha-secret` (`bootstrap-api-key`), same
  namespace, same secret the server Deployment uses.
- Re-running is safe (doc ids are preserved → upserts). To smoke-test first,
  edit the Job's command to add `--dry-run` (counts only) or
  `--limit 500 --namespace migration-smoke` (small copy into a scratch
  namespace), then `kubectl delete job es-to-kosha-migration` and re-apply.
- After changing `scripts/copy_es_to_kosha.py`, regenerate the manifest with
  `make migration-job-manifest` and re-apply.

## Local verification

    kustomize build k8s/stage            # render, no cluster needed
    kubectl apply --dry-run=client -k k8s/stage   # validate against the k8s API schema
    # with real cluster credentials (aws eks update-kubeconfig --name decoverai-nonprod):
    kubectl apply --dry-run=server -k k8s/stage   # validate against the live API server, no changes persisted
