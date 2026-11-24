# Multi-AZ deployment and Postgres failover

## Regional placement and residency

Choose a home region with the customer before provisioning. A deployment
serves that region only: provision the Kubernetes cluster and node pools,
Postgres writer and standbys, and the S3 bucket for segments and the WAL
in that same region. Set the regional endpoint in `DATABASE_URL` and
`AWS_DEFAULT_REGION` (or `AWS_REGION`) and `KOSHA_S3_BUCKET` for S3. Keep
backups, KMS keys, logs and other data-bearing telemetry in that region
as well. Leave cross-region replication and exports disabled unless the
customer has agreed to those destinations. For a different residency
region, provision a separate deployment with separate credentials and
storage; there is no per-namespace region switch or automatic
cross-region failover.

## Deployment

- Provision general worker capacity in at least three AZs. The query
  deployment and HPA start at three replicas, with an AZ spread constraint and
  preferred hostname anti-affinity. Verify their actual placement: topology
  spread works over eligible nodes, so the manifest cannot supply missing
  AZ capacity. After losing an AZ the remaining zones can host replacements.
  The query PDB protects two replicas from voluntary disruptions, not AZ loss.
  Have a cluster administrator apply `k8s/base/rbac-query-pdb.yaml` before the first
  release that includes the PDB so the deployer can manage it.
- Provide eligible NVMe nodes in all three AZs for ingest rescheduling.
  Keep ingest at **one replica with `Recreate`**. Per-namespace leases fence
  stale publishers but the Service does not route by leaseholder, and
  host-local WALs cannot follow a takeover. Do not point concurrent ingest
  replicas at the same S3 WAL stream either: conditional PUTs do not turn
  its independently allocated sequence into a multi-writer WAL. Following
  rescheduling the replacement may need to wait for the previous writer
  lease to expire (30 seconds by default).
- Local WAL mode can lose acknowledged but unflushed writes if the ingest
  node is lost. Before claiming zero acknowledged-write loss, switch to
  `KOSHA_WAL_MODE=s3` with a regional, multi-AZ storage class and a stable
  `KOSHA_WAL_WRITER_ID` reused by the replacement. Drain and publish all
  pending writes while the old disk is still available, or perform the
  switch on the old disk so the server can migrate the existing WAL. Verify
  S3 WAL recovery before fault testing.
- Provision RDS Multi-AZ or Aurora PostgreSQL with a failover target in
  another AZ and synchronous durability for acknowledged commits. Use the
  managed **writer** endpoint DNS name in `DATABASE_URL`, not the reader
  endpoint, an instance IP, or a pinned DNS record. Verify TLS against the
  endpoint and use working DNS resolution from each node pool. The pools
  discard connections to a demoted read-only server. Existing
  stage/prod overlays do not provision database infrastructure.

## Outage behavior

Warm query pods can continue eventual-consistency searches using the last
loaded manifest and available segments. Freshness can lag until Postgres
recovers; new pods and namespaces without a loaded manifest cannot use this
fallback. Strong-consistency reads also depend on the ingest pod. Health
probes do not restart a pod merely because Postgres is failing over.

Before a mutating API request executes on the receiving pod, a short
(500 ms) write-endpoint check rejects unreachable or read-only Postgres
with HTTP 503 and `Retry-After: 1`, before the operation modifies data.
An outage starting after this check can still interrupt an in-flight
write. A manifest publication whose outcome cannot be established returns
a non-retryable 503 with `details.outcome: "unknown"`; callers must not
blindly replay it. Manifest reads and SQL failures before CAS commit can
retry an eligible transient error twice with backoff. CAS retries retain
the original version, snapshot and fencing token; an ambiguous commit is
not replayed by the SQL retry loop. A branch whose manifest CAS failed
before attempting to commit returns a retryable 503 if the name is still
free. If creating a branch under a previously deleted name already left
an empty namespace, the response is a non-retryable 503 with
`details.outcome: "branch_not_created"`: replaying would conflict with
that namespace. A failed publication for an already staged document
mutation may still apply later, so its response remains non-retryable.

## Pre-production failover test

Run only against an isolated deployment with a disposable database and test
tenants. Start the [existing soak driver](../scripts/soak/README.md) over the
normal service endpoint. Use the agreed availability, latency and visibility
gates; keep the JSON report and metrics. Confirm query placement across three
AZs and verify WAL mode before starting. Run the faults separately:

1. Fail over the database through the RDS or Aurora control plane during
   steady reads, writes and manifest publication. Confirm warmed eventual
   searches remain available, mutation probes return prompt 503s while the
   writer endpoint is unavailable, and fresh writes and manifest refresh
   resume after promotion without a server restart. Check for conflicts or
   regressions in manifest versions.
2. Simulate loss of one AZ using the isolated cluster's approved fault tooling. For an
   ingest-loss trial, take the active ingest pod and its node-local disk out
   of service after an S3-WAL acknowledgement but before flush; confirm
   replacement in a surviving AZ, lease expiry or takeover, and WAL replay.
   Test a query AZ loss under load as well; an ordinary eviction only tests
   the PDB, not loss of the AZ.
3. Check the soak driver's exact final counts and uncertainty verdict.
   Also record uniquely identified canary writes acknowledged around the
   fault, with their expected contents. After recovery and flush, verify
   all of those IDs and values through the query service; aggregate counts
   alone cannot prove none were lost or replaced. Review fault-window
   latencies and error rates against the SLO before marking the test passed.
