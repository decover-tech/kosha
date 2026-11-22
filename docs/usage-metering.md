# Usage metering

Kosha meters usage per tenant and namespace (#348) so that a month of usage can be reproduced
from raw events and checked against the bucket. The settings are in
[`configuration.md`](configuration.md#usage-metering); the code is
`crates/kosha-control/src/usage.rs` (event model and store) and
`crates/kosha-server/src/usage.rs` (recorder, snapshots, API, reconciliation).

## Meters

| Meter | Kind | Counted when | Quantity |
| --- | --- | --- | --- |
| `write_bytes` | counter | An index or replace request succeeds (on the ingest pod) | Request body bytes |
| `write_docs` | counter | The same | Documents accepted |
| `queries` | counter | A search or exists request answers 200 (on the pod that served it, result-cache hits included) | 1 per request |
| `query_bytes_returned` | counter | The same | Response body bytes |
| `query_bytes_scanned` | counter | A search answers 200 | The namespace's stored bytes as of this pod's latest snapshot. An approximation: the query path does not expose the bytes a search actually read. Query pods with S3 list each namespace's prefix hourly for this, without emitting snapshots; a query pod without S3 counts 0. Exists requests count 0 |
| `storage_bytes` | snapshot | Hourly, on writer pods | The namespace's S3 prefix size (S3 builds), else its local segment directory size. Shards count under their root |
| `vector_dim_docs` | snapshot | The same | Σ over segments and vector fields of dimension × vectors, from the segment footers the writer holds (deleted documents are not subtracted) |

A counter's events add up what happened in their window. A snapshot samples a level once per
period, so its sum over a month is a level-hours figure (byte-hours for `storage_bytes`); its
latest value is the current level. Deleting a namespace writes a final zero snapshot.

Namespaces are reported without their `<tenant>/` prefix. Namespace ids from before tenants
existed belong to the `_legacy` tenant.

## Events and idempotency

Every pod counts into one row of counters per namespace for the current window
(`KOSHA_USAGE_FLUSH_SECS`, default 60 s). A flusher thread seals each window once it has ended
and appends its events to `kosha.usage_events`. An event's id is a hash of the source (pod
name plus a per-process nonce), window, tenant, namespace and meter; the table's primary key is
that id and appends are `INSERT ... ON CONFLICT DO NOTHING`, so a retried flush never counts
twice. Snapshot ids leave the pod out, so two pods (or a restarted one) snapshotting the same
namespace in the same hour store one event.

Events that fail to append stay in memory and are retried on the next tick (up to 500,000
events; beyond that the oldest are dropped with a warning).

**Loss bound.** A graceful shutdown seals the open window and flushes. A hard pod loss (OOM
kill, node loss) loses what was not yet appended: the open window, at most
`KOSHA_USAGE_FLUSH_SECS` of that pod's counts, plus any sealed windows still pending because the
store was unreachable.

## API

`GET /v1/usage?from=&to=&namespace=` — the caller's own usage (tenant API key). `from` and `to`
are Unix seconds or ISO-8601 dates (`2026-11-01`, `2026-11-01T00:00:00Z`); the range is
`[from, to)` on each event's window start. Default: from the start of the current UTC month to
now. `namespace` limits the answer to one namespace.

```json
{"tenant": "acme", "from": 1793491200, "to": 1795046400,
 "namespaces": [{"namespace": "docs", "meters": {"queries": 1200, "write_docs": 50000}}],
 "totals": {"queries": 1200, "write_docs": 50000}}
```

`GET /v1/admin/usage?tenant_id=&from=&to=` (admin credential) — the same for any tenant, plus
`pending_events` on the answering pod. With `raw=true` it returns the raw `events` instead,
from which the totals can be recomputed.

`POST /v1/admin/usage/reconcile` with `{"tenant_id": "acme"}` or `{}` for every tenant (admin
credential, forwarded to the ingest pod, S3 builds only) — compares each namespace's latest
`storage_bytes` snapshot with a live listing of its S3 prefix. It reports `metered_bytes`,
`live_bytes`, `drift_bytes`, `drift_ratio` (drift over live bytes) for every namespace, lists
those beyond `KOSHA_USAGE_RECONCILE_TOLERANCE` (default 2%) in `outside_tolerance`, and sets
`kosha_usage_reconcile_drift_ratio{tenant,namespace}` (see [`metrics.md`](metrics.md)).
Namespaces that exist but were never metered are included with `metered_bytes` 0.

Not built yet: a console view, export to a queue or warehouse, reconciliation against S3
Inventory reports and a scheduled reconciliation job.
