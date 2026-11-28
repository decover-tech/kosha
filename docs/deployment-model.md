# Deployment model: multi-tenant SaaS (#359)

**Status:** accepted. **Decision owner:** Kosha maintainers. **Issue:** #359 (Phase K, tracked in #361).

## Decision

Kosha is offered first as a **multi-tenant SaaS**: we run a small number of shared
Kosha *cells*, and every customer is an organization with tenants inside a cell.
A cell is one Kosha stack (ingest, query, indexer, Postgres control plane, one S3
bucket/prefix) deployed with the [`k8s/saas`](../k8s/saas) overlay.

Customers are onboarded through the control-plane APIs (console sign-up or
`POST /v1/admin/orgs`, then projects, tenants and scoped API keys; see
[organizations](organizations.md)), never by installing a stack for them. A
cell's first admin access is the admin bootstrap credential (`admin-bootstrap-key`
in `kosha-secret`, set as `KOSHA_ADMIN_API_KEY`), which operators replace with
per-operator credentials.
BYOC and dedicated clusters are **deferred**, not rejected.

## Why now

The multi-tenant prerequisites that #361 listed as the price of SaaS are on `main`:

- Isolation: per-tenant quotas, rate limits and fair admission (#341), cache
  isolation (#342), SSE-KMS with per-tenant keys and TLS between pods (#343).
- Control plane: organizations and console (#345), scoped API keys (#346),
  tenant lifecycle (#347), usage metering (#348), Stripe billing (#349).
- API: legacy unversioned routes default off (#350) and the rest of Phase I.
- Operations: multi-AZ (#354), SLO kit (#355), PITR (#356), soak (#357).

With those in place, one shared cell is cheaper to run and upgrade than one stack per
customer, and the operational surface (one fleet, one release train) is smaller.

## What this unblocks

- A deployable SaaS cell: `k8s/saas` with multi-tenant-safe defaults
  ([k8s/README.md, "Multi-tenant SaaS cells"](../k8s/README.md#multi-tenant-saas-cells)).
- #360, reframed for the SaaS fleet: `scripts/kosha_cluster.py` installs a cell
  (`install --overlay saas`), upgrades it in the order the
  [manifest/WAL compatibility rules](reference/auth.md#s3-wal-kosha_wal_modes3) require
  (`upgrade`), and reports health per cell (`status`).
- Onboarding design partners as organizations in a shared cell, metered and billed
  through the existing usage and billing pipeline.

## What this defers

- Provisioning or upgrading a Kosha stack per customer, and any Terraform or
  tooling for customer cloud accounts.
- Support access into customer-operated clusters. In a SaaS cell, operators use
  the admin API (`KOSHA_ADMIN_API_KEY` or an operator OIDC identity) and their own
  kubeconfig; customers never get cluster access.
- Customer-controlled release timing: every tenant in a cell runs the cell's release.

## How BYOC / dedicated stays possible

Nothing in this decision removes the single-customer path:

- The same image and `k8s/base` still install a standalone stack
  (`kosha_cluster.py install`, the default `--overlay base`).
- Every SaaS feature is a configuration switch (`KOSHA_CONSOLE`, `KOSHA_BILLING`,
  `KOSHA_TENANT_LIMITS_FILE`, ...), so a dedicated stack is either `k8s/base` or a
  SaaS cell that holds one organization.
- Per-tenant KMS keys (`kosha.tenants.kms_key_id`) already let a customer bring
  their own key inside a shared cell, which covers some data-control asks that
  would otherwise need a dedicated deployment.
- `install`, `upgrade` and `status` take a kubeconfig context, not a cloud account,
  so they work against a cluster in a customer account once it exists.

Revisit when a design partner requires their own cloud account or cluster: open a
new issue for a `k8s/dedicated` overlay and customer-account provisioning, priced
against the data in the TODOs below.

## Suggested order (replaces #361)

1. Phases G–J prerequisites. Done.
2. Phase K: this decision, the `k8s/saas` overlay and the cell tooling (#359, #360).
3. Stand up the first SaaS cell and onboard design partners as organizations,
   against the draft SLOs in [operations/managed-service.md](operations/managed-service.md).
4. Fill in the billing catalog (`k8s/saas/billing-plans.json`) and tune the
   default tenant limits (`k8s/saas/tenant-limits.json`) from the pricing hypothesis.
5. Open self-service sign-up (`KOSHA_SIGNUP=on`) once a mailer and paid plans exist.
6. BYOC / dedicated only if a design partner requires it (see above).

## Open items

- **TODO(design-partners):** list the design partners for the first cell, their
  expected data volume and query load, region, and whether any requires BYOK,
  a dedicated cell or BYOC. Not recorded here until confirmed.
- **TODO(pricing):** record the pricing hypothesis (plan names, included usage, unit
  prices, free tier) and translate it into `k8s/saas/billing-plans.json` and the
  plan limits in `k8s/saas/tenant-limits.json`. The overlay ships an empty billing
  catalog so no tenant is billed or restricted by a guessed price.
