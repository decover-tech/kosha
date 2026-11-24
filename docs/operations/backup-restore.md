# Backups and namespace point-in-time recovery

The Postgres manifest is the commit record for a namespace: it holds the
version, segment IDs, schema, deletion vectors, and shard or branch metadata.
S3 holds the segment files. Recover both to the same UTC time. Do not sync
the current S3 prefix or use a logical dump as a substitute for PITR.

## Objectives

| Objective | Initial target |
| --- | --- |
| RPO | At most 5 minutes of **published** namespace changes lost in an unplanned failure. |
| RTO | At most 4 hours from declaring recovery to passing cold queries in an isolated deployment, for a namespace up to 100 GiB in the primary region. |
| Recovery window | Up to 35 days, starting when continuous Postgres recovery and S3 versioning are both verified. |

These are operating targets, not guarantees. PITR restores what was in the
published manifest, not acknowledged writes still in the Kosha WAL, and does
not cover simultaneous loss of the database backups and versioned bucket.
For a planned checkpoint, flush and verify the manifest before selecting its
timestamp. For an incident, measure loss from the last good publication and
record any acknowledged but unpublished writes separately.

## Configure backups

### S3 segment bucket

Use a dedicated bucket per environment. Check the existing lifecycle settings
and bucket owner before applying changes; `put-bucket-lifecycle-configuration`
**replaces** all rules. Merge the provided rule into the existing configuration
if any rules are already present, and remove overlapping rules that expire
historical versions sooner or archive them out of the RTO.

```sh
aws s3api put-bucket-versioning --bucket "$SEGMENT_BUCKET" \
  --expected-bucket-owner "$AWS_ACCOUNT_ID" --versioning-configuration Status=Enabled
aws s3api put-bucket-lifecycle-configuration --bucket "$SEGMENT_BUCKET" \
  --expected-bucket-owner "$AWS_ACCOUNT_ID" \
  --lifecycle-configuration file://k8s/segment-bucket-lifecycle.json
aws s3api get-bucket-versioning --bucket "$SEGMENT_BUCKET"
aws s3api get-bucket-lifecycle-configuration --bucket "$SEGMENT_BUCKET"
```

Allow AWS's recommended 15 minutes after first enabling versioning before
writing objects for the first protected checkpoint. The rule keeps noncurrent
versions for 40 days (a 5-day margin), cleans up expired delete markers, and
does not expire live segments. Limit explicit version deletion, lifecycle edits,
and versioning suspension to separate operator roles, not the application role.
Retain KMS keys and decrypt grants for the entire window.

**Erasure prerequisite:** Kosha's ordinary deletes on a versioned bucket
create delete markers. Its online deletion status does not prove that S3
noncurrent versions or Postgres backups have been physically erased. Get
approval for the backup retention period and restricted recovery access before
enabling this policy. Where policy requires earlier erasure, explicitly purge
the affected retained versions (coordinated with branch references), account
for any affected Postgres backups, and record the resulting recovery gap.
Never serve a restored view that would reverse a later erasure.

### PostgreSQL

Use full/base backups plus continuous WAL retention. Keep WAL and base backups
encrypted, in a separate backup location with separate access. Alert when the
latest restorable time is more than 5 minutes behind, a base backup is missing,
archival fails, or either store no longer covers the 35-day window. Verify the
*earliest* and *latest* restorable times; success of the last backup is not
enough to prove that the WAL chain is intact.

- **Amazon RDS:** Set automated backup retention to 35 days (for example
  `aws rds modify-db-instance --db-instance-identifier "$INSTANCE" --backup-retention-period 35`,
  applied in the approved maintenance window). Monitor
  `EarliestRestorableTime` and `LatestRestorableTime` with
  `aws rds describe-db-instances`. Use
  `aws rds restore-db-instance-to-point-in-time --source-db-instance-identifier "$INSTANCE"
  --target-db-instance-identifier "$PITR_CLONE" --restore-time "$TARGET_UTC"`
  with isolated subnet and security group settings. Wait for the clone to
  become available; do not reuse the production instance identifier or attach
  application traffic.
- **Self-managed PostgreSQL (including a Kubernetes StatefulSet):** Install
  pgBackRest alongside PostgreSQL and provision a dedicated backup bucket.
  Set `wal_level = replica`, `archive_mode = on`,
  `archive_timeout = '60s'`, and
  `archive_command = 'pgbackrest --stanza=kosha archive-push %p'` in
  `postgresql.conf`. A reconnect does not enable archive mode; schedule a
  Postgres restart. Example pgBackRest configuration (supply deployment
  values, and a workload role or injected credentials):

  ```ini
  [global]
  repo1-type=s3
  repo1-path=/postgres/kosha
  repo1-s3-bucket=REPLACE_WITH_BACKUP_BUCKET
  repo1-s3-endpoint=s3.us-east-1.amazonaws.com
  repo1-s3-region=us-east-1
  repo1-retention-full=42
  repo1-retention-full-type=time

  [kosha]
  pg1-path=/var/lib/postgresql/data
  ```

  As the Postgres user, run `pgbackrest --stanza=kosha stanza-create` and
  `pgbackrest --stanza=kosha check`. Schedule a full backup weekly
  (`pgbackrest --stanza=kosha --type=full backup`) and a differential backup
  daily on the other days. Monitor `pg_stat_archiver`, pgBackRest backup age
  and recovery coverage. For recovery, use a separate instance with an empty
  data directory, the same Postgres major version, and read-only access to the
  repository. Run
  `pgbackrest --stanza=kosha --pg1-path="$CLONE_PGDATA" --type=time
  --target="$TARGET_UTC" --target-action=pause --archive-mode=off restore`.
  Start the clone with hot standby enabled and restricted connectivity; verify
  from its Postgres log that recovery reached the target and paused. Do not
  let the clone archive to the primary repository or replay beyond the target.

## Recover one namespace

1. Pick a UTC whole-second target within both stores' verified recovery window.
   Provision the PostgreSQL PITR clone for that exact time. The tool trusts
   this operator/provider step; the manifest table is not a history table.
   Keep the clone immutable to clients and use a read-only database role.
2. Provision an **empty, isolated** destination database with Kosha migrations
   applied (e.g. boot the current Kosha binary once against it and stop it)
   and a new, empty versioned bucket with appropriate default encryption.
   Use unique destinations per attempt and no other writers. Keep the
   application stopped until apply succeeds. Do not reuse a disk, WAL, cache,
   production endpoint, or application credentials.
3. Configure three libpq services (via `PGSERVICEFILE` or
   `~/.pg_service.conf`): `kosha-pitr` (clone), `kosha-current-ledger`
   (the authoritative current Postgres erasure ledger, with only
   `SELECT` on `kosha.deletion_records`), and `kosha-restore` (destination).
   Require TLS in deployed environments and use a protected passfile or
   workload credentials. The source S3 role needs bucket versioning and
   version listing/read access (`s3:GetBucketVersioning`,
   `s3:ListBucketVersions`, `s3:GetObjectVersion`) plus decrypt access.
   The destination role needs versioning/list, put and object head/read
   access, and any necessary KMS and multipart permissions. Do not give the
   restore job production write access. Coordinate with the erasure operator
   to prevent erasures racing validation and serving the restored view;
   recheck the current ledger before exposing it.
4. From the repository root, install the operator dependencies in an isolated
   environment (`python -m pip install boto3 'psycopg[binary]'`, Python 3.10+).
   Plan first (no writes). Then repeat with the destination options and
   `--apply`:

   ```sh
   python scripts/restore_namespace.py --namespace 'tenant/index' --at "$TARGET_UTC" \
     --recovered-db kosha-pitr --ledger-db kosha-current-ledger \
     --source-bucket "$SEGMENT_BUCKET" --source-prefix "$SEGMENT_PREFIX"

   python scripts/restore_namespace.py --namespace 'tenant/index' --at "$TARGET_UTC" \
     --recovered-db kosha-pitr --ledger-db kosha-current-ledger \
     --source-bucket "$SEGMENT_BUCKET" --source-prefix "$SEGMENT_PREFIX" \
     --destination-db kosha-restore --destination-bucket "$RECOVERY_BUCKET" \
     --destination-prefix recovery/ --scratch "$ENCRYPTED_SCRATCH" --apply
   ```

   Use the exact source `KOSHA_S3_PREFIX` (empty if unset).
   `--source-profile` and `--destination-profile` select separate AWS
   roles; `--source-endpoint` and `--destination-endpoint` support S3-compatible
   services. Scratch must fit the largest referenced object; files are
   verified and transferred one at a time. The tool requires `_COMMIT`
   markers (repair old unmarked segments before declaring them protected).
   It refuses unavailable objects, delete markers effective at the target,
   ambiguous S3 timestamps, and checksum mismatches. A tie between bitmap
   uploads before the target is accepted only if there is no tied delete
   marker and one version matches the manifest's SHA-256. Keep the same target
   across stores; to try a different timestamp, recover Postgres again.
5. The tool reports the root and any shard manifest versions, object count
   and bytes. It transfers only live segment inventories and the referenced
   deletion generations, including inherited branch paths in the source
   tree. It inserts the original manifest rows atomically **after**
   object verification; it does not copy other namespaces, WAL, auth,
   usage, leases or billing data. On failure, do not serve the destination:
   Postgres rolls back, but partial S3 uploads may remain. Discard the
   destination and retry with a fresh bucket. No step overwrites production.
6. Boot an isolated Kosha deployment with a clean cache, the destination DB,
   bucket and prefix, and a new credential for the restored tenant. Keep
   the deployment private and allow only stats and search routes. Disable
   compaction (`KOSHA_COMPACTION_INTERVAL_MS=0`) and do not deploy a GC job
   or configure a route to production ingest. Validate stats and known positive
   and negative search results (including deletions) on a cold start. This
   workflow stops at verification; serving or importing recovered data into
   production requires separate approval and a controlled cutover.

## Quarterly restore drill

Run every quarter and after backup, retention or storage changes. Use the
managed-service on-call rotation and a representative disposable namespace,
not customer data. Record start time (the simulated incident declaration).

1. Write and flush a known dataset, including a document to delete before
   the target. Record IDs, counts, a search query and its expected results,
   root/shard manifest versions, and time. Pick a checkpoint after flush,
   clear of the S3 timestamp resolution. Then write and flush a new document
   and compact so the old segment is no longer current. For the disposable
   test, verify that versioned storage can still serve a segment hidden by
   a later delete marker. Leave another namespace as a negative control.
2. Verify both recoverable windows and recover the database and namespace
   using the procedure above. Start with empty cache/disk and compare
   manifest versions and search results to the checkpoint. Assert the
   post-target document, pre-target deletion and other namespace are absent.
3. Save the target, last good publication, latest recoverable publication,
   measured RPO and elapsed RTO, restored bytes and verification results in
   the operations log. Treat incomplete coverage, discrepancies or a miss
   against either objective as an incident follow-up with a named owner and
   a repeat drill after the fix. Remove the recovery bucket (including its
   versions), databases and any scratch data under the approved retention
   policy after verification.
