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

### Prerequisites (one-time, manual — not automated by this repo)

Before these jobs can succeed end to end, someone with access needs to:

1. Create GitHub Environments `staging` and `production` on this repo, each
   with:
   - `AWS_GITHUB_ROLE_ARN` — an OIDC-assumable role that can
     `eks:DescribeCluster` on the target cluster *and* has EKS RBAC (an
     access entry or `aws-auth` mapping) granting it write access
     (Deployments/Services/Secrets/ConfigMaps) in the `kosha` namespace.
     Staging can likely reuse the existing ECR-push role (same account,
     010928200670) with the RBAC grant added; production needs its own role
     in the prod account.
   - `KOSHA_DATABASE_URL` — Postgres connection string for that
     environment's control plane.
   - `KOSHA_API_KEY` — the bootstrap API key `kosha-server` should accept
     (this is also the value backend clients must be configured with, so
     client and server agree).
2. Confirm the `kosha` namespace's node pool can actually pull from ECR
   (staging) — this is normal EKS node-role ECR auth, already working for
   every other in-account image, so almost certainly a non-issue.

## A staging deployment already exists

As of this writing, `kosha` is already running in the `kosha` namespace on
`decoverai-nonprod` — deployed manually, ahead of this pipeline existing.
`kustomize build k8s/stage` was checked (`kubectl apply --dry-run=server`)
against the live cluster and every object comes back `configured` (an
update-in-place), not an error — so once the GitHub Environment secrets
above exist, the pipeline's `kubectl apply` will converge onto the existing
Deployment/Service/ConfigMap/ServiceAccount rather than replacing them,
with no need to delete anything first.

## Local verification

    kustomize build k8s/stage            # render, no cluster needed
    kubectl apply --dry-run=client -k k8s/stage   # validate against the k8s API schema
    # with real cluster credentials (aws eks update-kubeconfig --name decoverai-nonprod):
    kubectl apply --dry-run=server -k k8s/stage   # validate against the live API server, no changes persisted
