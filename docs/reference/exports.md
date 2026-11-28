# Exports

How a caller takes a namespace's data back out. Moved from the README.

## Exports

A customer can take back everything a namespace stores (#347). Opt-in:
`KOSHA_EXPORTS=on` (see [docs/configuration.md](../configuration.md)). Every route needs the
*read* scope on the namespace, like a search.

```
POST /v1/namespaces/docs/exports                      {"include_vectors": true, "upload_url": "https://..."}
GET  /v1/namespaces/docs/exports                      every export of the namespace
GET  /v1/namespaces/docs/exports/{id}                 status, parts, presigned download URLs (S3)
GET  /v1/namespaces/docs/exports/{id}/download?part=0 one part, streamed
DELETE /v1/namespaces/docs/exports/{id}               delete it now
```

- **Format**: gzip-compressed JSON Lines, one document per line in the index API's own form
  (`{"id", "fields": [{"name", "field_type", "value"}]}`), so lines go straight back to
  `POST /v1/namespaces/{ns}/documents`. `Vector` fields are included unless
  `"include_vectors": false`. Large exports are split into parts of at most
  `KOSHA_EXPORT_PART_BYTES` of JSON; each part is a complete gzip member, so the parts
  concatenated in order are one valid `.jsonl.gz`. Parquet is not offered: the `parquet` and
  `arrow` crates would be the heaviest dependency tree in the server, for a format any customer
  can derive from these lines.
- **Consistency**: starting an export flushes and publishes the namespace (every acknowledged
  write is in it) and pins the manifest it reads (every shard's, for a sharded namespace) on the
  export record. Segment GC and the local GC keep whatever a pinned manifest references while the
  export is pending or running, so a compaction mid-export cannot take a segment away. Deletes
  are applied as of the pinned version.
- **Jobs**: the record (`kosha.exports`, migration 010) is durable. A writer pod claims and runs
  it and refreshes the claim every 30 s on a timer of its own, while writing and while
  delivering; if that pod dies, a writer pod's sweep (every 60 s) claims it after 10 minutes and
  runs it again from the start against the same pins. Each attempt writes under its own
  `attempt-{n}/` prefix, and a run that lost its claim removes only its own files. States:
  `pending`, `running`, `completed`, `failed` (objects deleted, `error` says why), `expired`.
  At most `KOSHA_EXPORT_MAX_ACTIVE` (default 2) pending or running exports per tenant (429).
- **Delivery**: parts are stored under `_exports/{tenant}/{id}/attempt-{n}/` in the bucket (with the
  tenant's SSE key, #343), or under the data directory without S3. On S3 the status route returns
  a presigned `GET` URL per part valid for `KOSHA_EXPORT_PRESIGN_SECS`; any pod streams a part
  through `.../download`. An optional `upload_url` (a presigned PUT URL in the customer's bucket)
  receives the whole export as one object: it must be `https`, carry no credentials before the
  host, and every address its host resolves to must be public (loopback, private, link-local,
  CGNAT, multicast, reserved and documentation ranges are refused, for IPv4, IPv6 and
  IPv4-mapped forms; 6to4, Teredo, local-use NAT64 and discard-only IPv6 are refused too). The
  upload connects to the address that was checked, ignores `HTTP(S)_PROXY`, and follows no
  redirects. The URL is kept only until the delivery ends and is never returned; the record shows
  `delivery_host`, `delivery_state` and `delivery_error`. A failed delivery leaves the export
  completed and downloadable.
- **Lifetime and erasure**: a completed export expires after `KOSHA_EXPORT_TTL_SECS` (default 7
  days): its objects are deleted and it reads `expired`. Deleting the namespace deletes its
  exports, deleting the tenant deletes all of them and the whole `_exports/{tenant}/` prefix, and
  the deletion record counts them (`exports_deleted`). A row is deleted only after its objects
  are confirmed gone; until then it stays (`expired`, unpinned) and the deletion record stays
  `pending`, so the retry finds it. Failed and expired rows are removed after 30 days. An export
  id is answered only together with the caller's tenant and the namespace in the path; anything
  else is a 404.
- **Listing**: `GET .../exports` is newest first, at most `?limit=` (default and maximum 100)
  per page; `next` is the `?before=` of the following page.
- **Turned off**: with `KOSHA_EXPORTS` off only starting an export is refused (404); existing
  exports can still be read, downloaded and deleted. A writer pod booting with exports off fails
  any export still pending or running, so its pinned segments are released to GC.
