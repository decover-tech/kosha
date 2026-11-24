# Backups and point-in-time restore

A namespace is two things: its manifest, a row in the control-plane
Postgres (`kosha.manifests`), and the immutable segment and deletion-bitmap
objects that manifest lists in the segment bucket. A backup of a namespace
at time T is the manifest row as of T plus every object it listed. Kosha
never rewrites a segment object in place, so the objects only need to
survive deletion; the manifest needs Postgres point-in-time recovery
(PITR).

This page covers what to configure, the restore procedure, the recovery
objectives, and the quarterly drill that verifies them.

## What to configure

### Segment bucket: versioning and lifecycle

Segment GC (`POST /v1/admin/gc`, [configuration](../configuration.md#segment-garbage-collection))
deletes objects no current manifest lists, and a compaction or a namespace
deletion makes that most of what an old manifest listed. With versioning on,
a delete only adds a delete marker; the data stays as a noncurrent version
that a restore can bring back.

```sh
aws s3api put-bucket-versioning --bucket "$KOSHA_S3_BUCKET" \
  --versioning-configuration Status=Enabled
aws s3api put-bucket-lifecycle-configuration --bucket "$KOSHA_S3_BUCKET" \
  --lifecycle-configuration file://lifecycle.json
```

`lifecycle.json`:

```json
{
  "Rules": [
    {
      "ID": "kosha-noncurrent-retention",
      "Status": "Enabled",
      "Filter": {},
      "NoncurrentVersionExpiration": { "NoncurrentDays": 35 },
      "Expiration": { "ExpiredObjectDeleteMarker": true },
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
    }
  ]
}
```

- `NoncurrentDays` is the object half of the restore window. It must be at
  least the Postgres PITR retention plus `KOSHA_GC_GRACE`; 35 days covers a
  30-day PITR window with margin. A shorter value makes restore points older
  than it unrestorable (the dry run below reports the missing objects).
- Versioning costs storage for every reclaimed object until it expires.
  `GET /v1/admin/usage/reconcile` reports current objects only.
- Kosha needs no new permission for this: a restore reads current objects.
  The operator who brings back noncurrent versions needs
  `s3:ListBucketVersions`, `s3:GetObjectVersion` and `s3:PutObject`.
- S3-compatible stores (MinIO and others) accept the same configuration;
  check that yours supports `NoncurrentVersionExpiration`.

With `KOSHA_WAL_MODE=s3` the WAL lives in the same bucket under `_wal/`;
it is not needed for a restore, and the same rule keeps its history.

### Control-plane Postgres: PITR

Run Postgres with continuous WAL archiving and a retention window:

- Managed (Amazon RDS / Aurora, Cloud SQL, Azure Database): turn on
  automated backups with point-in-time restore and set the retention
  (assumed below: 30 days).
- Self-managed: `archive_mode = on`, an `archive_command` (or a tool such
  as pgBackRest or WAL-G) shipping WAL to storage outside the database host,
  periodic base backups, and `archive_timeout = 60s` so a quiet database
  still closes a WAL segment every minute.

The `kosha` schema is small (one row per namespace plus keys and leases),
so a recovery replays quickly; the time is dominated by the provider
creating the instance.

## Recovery objectives

These are the targets this procedure is built and drilled against. They are
assumptions, not measured guarantees: change them here when your
deployment's requirements differ, and record what each drill measures.

| Objective | Target | What it depends on |
| --- | --- | --- |
| RPO, control plane | 5 minutes | Postgres WAL archiving lag (`archive_timeout`, provider backup cadence) |
| RPO, namespace data | documents flushed to segments before T | A restore point holds the segments its manifest lists; acked writes still only in the WAL at T are not part of it (see [durability](../guarantees.md#durability-on-acknowledgement)) |
| Restore window | 30 days | The shorter of the PITR retention and `NoncurrentDays` minus `KOSHA_GC_GRACE` |
| RTO, one namespace | 1 hour | Recovering Postgres to a scratch instance, then one restore call; the call moves no segment bytes |
| RTO, whole control plane | 4 hours | Recovering the production Postgres in place, then restarting the pods |

## Restoring a namespace to a point in time

The restore creates a new namespace from the old manifest and never writes
to the namespace it was taken from, so it cannot destroy anything that is
there now.

1. **Recover Postgres to T into a scratch instance** (not the production
   one): your provider's "restore to point in time", or a base backup plus
   `recovery_target_time = 'T'`.
2. **Read the manifest row** from the scratch instance:

   ```sql
   SELECT version, segments_json FROM kosha.manifests
    WHERE namespace_id = '<tenant>/<namespace>';
   ```

3. **Dry run** against the running service with the admin key:

   ```sh
   curl -sS -X POST "$KOSHA_URL/v1/admin/restore-namespace" \
     -H "Authorization: Bearer $KOSHA_ADMIN_API_KEY" \
     -d '{"namespace": "<tenant>/<namespace>",
          "target": "<tenant>/<namespace>-restored",
          "manifest": "<segments_json>", "version": <version>,
          "dry_run": true}'
   ```

   `manifest` takes the `segments_json` text as a string or the manifest
   object itself. The answer lists the segments, documents and deletion
   bitmaps the manifest references, and `restorable`. Each segment gets the
   same check a cold read applies (its `_COMMIT` and file sizes, or every
   required file for segments written before `_COMMIT` existed).
4. **Bring back missing objects.** For each path in
   `verification.missing` (logical paths: the S3 key is `KOSHA_S3_PREFIX`
   followed by the path; at most 50 are listed, `missing_total` counts
   all), copy its newest noncurrent version over the delete marker:

   ```sh
   aws s3api list-object-versions --bucket "$KOSHA_S3_BUCKET" --prefix "<path>"
   aws s3api copy-object --bucket "$KOSHA_S3_BUCKET" --key "<key>" \
     --copy-source "$KOSHA_S3_BUCKET/<key>?versionId=<version id>"
   ```

   A segment path names a directory: restore every object under it,
   `_COMMIT` last. Repeat the dry run until `restorable` is `true`.
5. **Restore**: the same call without `dry_run`. It is refused with `409`
   if any object is still missing or the target exists, and with `400` for
   a sharded namespace (not supported yet).
6. **Verify**: `GET /v1/namespaces/<namespace>-restored/stats` and a few
   known queries. The restored namespace serves reads and takes writes at
   once.

The restored namespace is recorded as a branch of the original (its
`stats` show `parent` and `parent_version`, the restored manifest version):
it lists the original's objects where they are, and segment GC keeps them
for as long as it does. Consequences:

- `DELETE /v1/namespaces/{ns}` on the original is refused while the
  restored namespace shares its segments. A full compaction of the restored
  namespace (`POST /v1/admin/compact-namespace`) rewrites them under its
  own prefix and ends the sharing.
- Remove a restore you no longer need with
  `DELETE /v1/namespaces/{ns}/branch`, which deletes only what nothing
  else references.
- To serve the restored data under the original name, point clients at the
  new name, or: fully compact the restored namespace, delete the original,
  and branch the restored one under the original name
  (`POST /v1/namespaces/{restored}/branch` `{"name": "<original>"}`).

**Whole control plane lost** (the database itself, not one namespace):
recover Postgres in place to T and restart the pods; every namespace comes
back at its T manifest, and step 4 applies to any object reported missing.
Without any usable Postgres backup, `POST /v1/admin/import-namespace`
rebuilds a namespace's manifest from the footers under its prefix. That
loses deletion bitmaps and branch structure, so it is the last resort.

## Quarterly restore drill

Once a quarter, and after any change to the bucket, the database or this
procedure:

1. Pick a namespace that took writes and a compaction in the last week, and
   a time T at least `KOSHA_GC_GRACE` plus one day old, so GC has
   reclaimed objects its T manifest listed.
2. Follow steps 1 to 6 with `target` `<namespace>-drill-<yyyy>q<n>`, timing
   from the start of step 1 to the end of step 6.
3. Check: the restored document count equals the sum of `doc_count` in the
   T manifest minus its deletions; a query whose answer at T is known
   (from logs, or a canary document written just before T) returns it.
4. Record the date, T, the measured restore time against the RTO, the
   number of noncurrent versions brought back, and anything that went
   wrong, in your operations log.
5. Delete the drill namespace with `DELETE /v1/namespaces/{ns}/branch` and
   drop the scratch Postgres instance.

A drill that misses a target is an incident: fix the configuration or
change the target here.

The automated part of this drill runs on every CI build:
`restore::tests::restores_a_namespace_to_a_point_in_time` in
`crates/kosha-server` takes a restore point, lets the namespace move on and
GC reclaim what it listed, checks that the restore is refused while objects
are missing, brings them back as S3 versioning would, and restores exactly
the documents of the restore point.
