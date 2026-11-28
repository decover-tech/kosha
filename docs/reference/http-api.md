# HTTP API

Routes and deletion behavior moved from the README. The generated OpenAPI
document remains the contract.

## HTTP routes

Every `/v1/namespaces` route is scoped to the caller's tenant: `{ns}` is an
unqualified name, stored as `{tenant}/{ns}`, and another tenant's namespace of
the same name is invisible (404). Query pods (`KOSHA_INGEST_HOST` set) forward
the routes marked *write* to the ingest pod.

The API reference is the generated, checked-in OpenAPI 3 document:

- [`api/openapi/v1/kosha.openapi.json`](../../api/openapi/v1/kosha.openapi.json):
  every public route, with request and response schemas that match the wire
  (checked by `scripts/api/check_wire.py`). Render it with any OpenAPI viewer,
  or generate a client from it (see [tools/codegen](../../tools/codegen/README.md)).
- [`api/openapi/v1/kosha-admin.openapi.json`](../../api/openapi/v1/kosha-admin.openapi.json):
  the operator routes (`/v1/admin/*`, `GET /v1/stats`; admin key only, beta).
- [`docs/api/errors.md`](../api/errors.md): the error model and every
  error code. [`docs/api/versioning.md`](../api/versioning.md): what may
  change inside `/v1`, deprecation, and the legacy-route timeline.
  [`api/CHANGELOG.md`](../../api/CHANGELOG.md): contract changes.

The public routes in brief:

| Routes | Notes |
|---|---|
| `POST`/`GET /v1/namespaces`, `DELETE /v1/namespaces/{ns}` | Create (*write*), list, delete (*write*; see below). |
| `POST /v1/namespaces/{ns}/documents`, `/replace`, `/flush`, `/delete` | *write*. Upsert by id; replace by id merging partial documents; make buffered writes searchable; delete by query. |
| `POST /v1/namespaces/{ns}/search`, `/exists`, `GET .../stats` | Reads. In a split deployment, ingest serves existence checks so they include buffered writes. |
| `POST /v1/namespaces/{ns}/read` | Read, *beta* (#586). One stored field of one document, by `id` or by a keyword `path` (`path_field`, default `path`), whole or as `{"lines": {"start": 10, "end": 40}}` (1-based, inclusive), `{"page": 3}` (form-feed separated pages, as `pdftotext` writes them) or `{"bytes": {"start": 0, "end": 4096}}`. The response carries `text` with `start_line`/`end_line`, `page`, `byte_start`/`byte_end` and the totals; a cut longer than `max_bytes` (default 64 KiB, at most 1 MiB) is shortened at a line end and marked `truncated`, so read on from `byte_end`. A range past the end is an empty cut; a missing document or field is a 404, a path naming two documents a 400. It fetches only that document's doc-store record (a ranged GET when cold), not a span inside it. |
| `POST`/`GET /v1/namespaces/{ns}/schema`, `POST .../shards` | Schema (see [Analyzers](features.md#analyzers)); sharding ([Namespace sharding](features.md#namespace-sharding)). |
| `POST .../branch`, `GET .../branches`, `DELETE .../branch` | Copy-on-write branches (#208). |
| `GET /v1/usage`, `GET /v1/billing` | Usage metering and billing, when enabled. |
| `GET /v1/healthz` | Liveness (SDK `ping`). |

Operational endpoints, unversioned and outside the spec (see
[docs/operations/endpoints.md](../operations/endpoints.md)): `GET /healthz`
and `GET /readyz` (unauthenticated probes, also served as `/v1/healthz` and
`/v1/readyz`), `GET /metrics` ([docs/metrics.md](../metrics.md)) and
`GET /dashboard` ([docs/dashboard.md](../dashboard.md)).

**Deleting a namespace** (`DELETE /v1/namespaces/{ns}`) runs on the ingest pod
under the namespace's publication lock:

1. The control store drops the namespace and its manifest. It keeps the highest
   manifest version the namespace reached (`kosha.retired_namespaces` in
   Postgres), and a re-created namespace of the same name starts above it, so a
   query pod's caches, which are keyed by manifest version, can never serve the
   deleted data.
2. The writer discards the namespace's manifest, write buffer, pending deletes
   and every WAL record for it, so neither a later flush nor a restart brings the
   documents back. A group-commit WAL file that also holds other namespaces'
   records is kept until those flush, and the deletion is recorded durably in
   the WAL stream (`discarded`), so recovery skips the deleted namespace's
   records in it.
3. Its local segment directory and per-namespace caches are removed.
4. With S3 configured, every object under the namespace's prefix
   (`{KOSHA_S3_PREFIX}/{tenant}/{ns}/`) is deleted before the response returns,
   and the prefix is then listed again. If the delete fails or the listing is
   not empty, the namespace is still deleted and the response says
   `"storage_reclaimed": false`; the leftover objects are unreferenced, and a
   re-created namespace never reuses their keys.

The pod that runs the delete also drops, in the request, what it holds in memory
for the namespace: parsed segments, corpus statistics, score ceilings, result
pages and hydration verdicts. The namespace's exports are deleted too (objects
and rows) and counted on the record (`exports_deleted`).

Query pods answer 404 for the namespace once they see the control store has no
manifest for it: within one manifest refresh (`KOSHA_MANIFEST_REFRESH_MS`,
default 500 ms), or at once for a search with `max_staleness_seconds: 0`. Their local disk copies of its segments are not removed
eagerly; the LRU cache (`KOSHA_CACHE_MAX_BYTES`) evicts them, or the purge below. Re-creating the
namespace, explicitly or by writing to it, starts it empty.

With `KOSHA_NAMESPACE_DELETE_CONFIRM=on` (off by default, so existing callers
keep working) the request must name the namespace again:
`DELETE /v1/namespaces/docs?confirm=docs`; anything else is a 400.

**S3 erasure.** Deletion aborts unfinished multipart uploads and permanently
removes all object versions and delete markers under the namespace's S3
prefix. It checks again for versions, markers, and uploads before reporting
storage reclaimed. The S3 role needs `s3:ListBucketVersions`,
`s3:DeleteObjectVersion`, `s3:ListBucketMultipartUploads`, and
`s3:AbortMultipartUpload`. If multipart listing is denied, versions and
markers are still removed, but the deletion record stays pending and the
response reports `storage_reclaimed: false`.

With a custom S3 endpoint, multipart listing scans the bucket and filters
keys by the namespace prefix. Some MinIO versions only expose uploads known
to the running server in this listing, so it cannot verify erasure of uploads
left by an earlier MinIO process. Hard delete does not purge versioned S3 WAL
objects under the separate `_wal/{writer_id}/` prefix.

**Cached copies on every pod.** With `KOSHA_DELETION_PURGE_INTERVAL_MS` set
(off by default), each pod of any role reads the deletion records it has not
handled yet, at boot and then on that interval, and for every deleted
namespace or tenant:

- drops what it holds in memory for it: parsed segments, corpus statistics and
  score ceilings, cached result pages, hydration verdicts, the unindexed tail;
- removes its segment directories (byte-range spans included) from the local
  disk.

A namespace re-created under the same name shares the directory. Segment ids
are never reused, so the pod keeps exactly the segments a current manifest
lists (the control store's, a branch's, or its own unpublished ones, read
under the writer's locks) and removes the rest. The retired version on the
record tells a manifest of the deleted incarnation from one of the new. A
directory written to in the last 5 seconds, pinned, or being fetched is left
for the next sweep. So is a namespace on a writer pod whose first publish
registered it after the sweep listed the control store: the writer asks the
store again under its locks before it removes anything.

**Per-pod acknowledgement.** Every pod with the purge on keeps a row in the
pod registry (`kosha.pods`, a heartbeat per pass) and, after a clean sweep of
a deleted subject, writes an acknowledgement (`kosha.deletion_acks`) for each
of its records that asks for one. A deletion under an hour old is
acknowledged only after its settle sweep (60 s after the first), and only
once the writer verified its storage. The heartbeat runs on its own timer,
so a long pass does not make a live pod look dead. With `KOSHA_DELETION_REQUIRE_ACKS=on` on the
writer pod, a record is written with `acks_required`: once storage is
verified it stays `pending` (`storage_verified: true`) until every *live* pod
has acknowledged it, and only then becomes `completed`. A pod is live while its
last heartbeat is younger than `KOSHA_POD_LIVENESS_MS` (default 5 minutes, by
the database's clock); a pod that died stops being waited for after that
window and is listed as `expired`. A pod that restarts handles every record
again in its first pass. A tenant is acknowledged only by a sweep that ran
after the tenant's storage reclaim finished. `GET /v1/admin/deletions/{id}`
shows, for such a record, `acks: {complete, live_pods, acknowledged, missing,
expired, liveness_ms}`. Any purge pod completes a record once nobody live is
missing (it checks every 60 s, so a dead pod's expiry is noticed).

What holds for a pod that has the purge on and can read the control store,
with or without acknowledgements:

- it handles a record in the first pass that starts after the delete commits,
  so within one interval plus the length of a pass. (Records are read in
  request-time order with a 10-minute look-back; a delete whose transaction
  stayed open longer than that is only seen after the pod restarts.)
- for a deletion less than an hour old it sweeps the same namespace again 60
  seconds later, and it repeats any sweep every 60 seconds while one left
  something or failed. That removes what a search or fetch already running
  wrote after the first sweep;
- it sweeps a deleted tenant again every 60 seconds for as long as the tenant's
  record is `pending`, and once more after the record is `completed`. A tenant
  record is written before the tenant's namespaces are deleted, and a sweep
  keeps whatever the control store still lists, so the sweep after completion
  is the one that removes what has no record of its own: the tenant's branches
  and directories no namespace was ever registered for. For those, the window
  starts when the tenant record completes, not when the delete was requested;
- a pod that was down handles every record in its first pass after boot.

Not covered: a pod with the purge off (with acknowledgements required it is
not in the registry, so the record does not wait for it either: turn the
purge on everywhere); parsed segments a search already running still holds,
until it ends; a pod that cannot read the control store, until it can (it
cannot acknowledge either, so the record waits until its heartbeat expires);
a writer pod holding unpublished writes for a namespace another pod deleted,
until they are published or dropped (it does not acknowledge meanwhile); freed
disk blocks and the operating system's page cache. `GET /v1/stats`
reports each pod's progress
under `deletion_purge` (`caught_up`, `pending_sweeps`, `last_error`, counters).

**Deletion records.** Step 1 also writes a deletion record
(`kosha.deletion_records` in Postgres, migration 004) in the same transaction
that drops the namespace, and the response returns it as `deletion`:

```json
{"namespace": "docs", "acknowledged": true, "storage_reclaimed": true,
 "deletion": {"id": "del-…", "scope": "namespace", "subject": "acme/docs",
              "tenant_id": "acme", "state": "completed", "attempts": 1,
              "objects_deleted": 42, "last_error": null,
              "requested_at_ms": 1790000000000, "completed_at_ms": 1790000000412}}
```

A record is `pending` until step 4 has deleted the prefix **and** listed it
empty (and the namespace's exports are gone); only then is it `completed`. A
record holds identifiers and counters, never document content, and is kept as
the evidence that the erasure finished.

**Exactly what `completed` proves.** Without `acks_required`: the control
store has no row of the subject, the deleting pod's memory, WAL and disk hold
nothing of it, its exports are deleted, and a listing of its object-storage
prefix came back empty after the delete. Other pods' caches are then purged by
each pod within the window above, but the record does not prove it. With
`acks_required` (`KOSHA_DELETION_REQUIRE_ACKS=on`): all of that, **and** every
purge pod that was live (heartbeat within `KOSHA_POD_LIVENESS_MS`) when the
record completed had swept the subject cleanly from its memory and local disk
and said so; the acknowledging pods are listed on the record. It never proves
anything about pods without the purge, freed disk blocks, or backups of the
bucket or database kept outside Kosha.

Scopes: `namespace`; `tenant`; `branch` (a branch deleted with
`DELETE /v1/namespaces/{ns}/branch`, or with its tenant: complete once every
object of its prefix that no remaining manifest lists is gone, since objects a
branch of the branch still lists stay by design and go with that branch); and
`migrated` (the bare source id of `kosha-server migrate-namespace`, written by
the tool after the control-plane rename, `tenant_id` the destination's tenant,
completed when the copied source objects are deleted, pending with
`--keep-source`).

| Method and path | Notes |
|---|---|
| `GET /v1/admin/deletions` | Records, oldest first. Filters: `?state=pending\|completed`, `?tenant=<id>`, `?limit=<n>`. |
| `GET /v1/admin/deletions/{id}` | One record. |
| `POST /v1/admin/deletions/resume` | *write*. Retry every pending record (or `{"id": "…"}` for one). Returns the records and how many are still pending. |
| `DELETE /v1/admin/tenants/{tenant}` | *write*. Delete a tenant; see below. |

A pending namespace record whose namespace was re-created in the meantime is
not retried: the new incarnation writes into the same prefix. It completes
after that namespace is deleted again. With the in-memory control store (no
`DATABASE_URL`) records last only as long as the process.

With `KOSHA_DELETION_RESUME_INTERVAL_MS` set (off by default), a writer pod
(`ingest` or `standalone`) retries pending namespace, branch and tenant
records itself (namespaces and branches first), at boot and then on that
interval, under the same rule. A record that stays pending waits twice as long
before each further try, up to one hour. A pending tenant record is a standing
order: each retry deletes whatever keys and namespaces the tenant id has when
it runs. A record that waits only for acknowledgements is not retried; a
`migrated` record is finished by re-running `migrate-namespace`.

A retry deletes the namespace's whole prefix in object storage, unattended,
and the first one runs at boot, when a second writer pod can still be alive.
Each automatic retry therefore takes the namespace's writer lease first and is
skipped (and paused like a failed one) while another writer holds it; with the
lease held no other writer can commit a manifest for the namespace between the
"not re-created" check and the delete. A lease taken only for the retry is
released again. With `KOSHA_WRITER_LEASE=off` nothing fences the other writer:
do not set `KOSHA_DELETION_RESUME_INTERVAL_MS` where two writer pods can run at
the same time (a rolling restart of the ingest pod included). The admin resume
call takes no lease; run it against the pod that is the active writer.

**Deleting a tenant** (`DELETE /v1/admin/tenants/{tenant}`, admin key) needs
the body `{"confirm": "<tenant>"}` and is permanent. In order: the tenant's
API keys are deleted from `kosha.api_keys` (and from the ingest pod's key
snapshot at once; other pods drop them on their next refresh,
`KOSHA_API_KEY_REFRESH_SECS`), its branches are deleted, each namespace is
deleted as above with its own record, and whatever is left under
`{KOSHA_S3_PREFIX}/{tenant}/` is deleted and the prefix listed empty. The
response is 200 with `"completed": true` when all of that held, and 202 with
the reasons in `errors` when something is left; the tenant's own record then
stays `pending` and `POST /v1/admin/deletions/resume` (or repeating the
request) continues it. Keys set through `KOSHA_API_KEYS` / `KOSHA_API_KEY`
cannot be removed at runtime, so a tenant that still has one stays pending
until the deployment's environment changes. Tenant ids starting with `_` or
`.` are refused, and so is a tenant whose id is also the name of an unscoped
(pre-#262) namespace, which stores its segments under the same prefix.
