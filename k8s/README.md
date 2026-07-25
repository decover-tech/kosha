# Kosha k8s manifests

Kustomize layout for running `kosha-server` in its own `kosha` namespace on
the real EKS clusters (`decoverai-nonprod` staging, `decoverai-prod`
production).

    k8s/
      base/     Namespace, ServiceAccount, ConfigMap, Deployment, Service
      stage/    tracks the floating `:main` tag (ECR, what every merge to
                main pushes)
      prod/     tracks versioned releases (Docker Hub, public)

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

## Not yet wired in (fast-follow)

S3-backed durable segment storage (bucket, IRSA role for the `kosha`
ServiceAccount, `KOSHA_S3_*` env vars) is intentionally out of scope for
now. `KOSHA_S3_BUCKET` is left unset, so `kosha-server` runs local-disk-only
— a supported, explicit mode (see `crates/kosha-server/src/s3_storage.rs`).
Revisit once IRSA/S3 provisioning is ready.

## Local verification

    kustomize build k8s/stage            # render, no cluster needed
    kubectl apply --dry-run=client -k k8s/stage   # validate against the k8s API schema
