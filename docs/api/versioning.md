# API versioning and deprecation

This is the policy for the public Kosha HTTP/JSON API (ADR-350). The
contract is the generated OpenAPI 3 document
[`api/openapi/v1/kosha.openapi.json`](../../api/openapi/v1/kosha.openapi.json);
error codes are in [errors.md](errors.md); every change is in
[`api/CHANGELOG.md`](../../api/CHANGELOG.md).

**The OpenAPI document is the wire format. Proto3 canonical JSON is not.**
`proto/kosha/v1/*.proto` is written to produce that document (snake_case
names, string enums with the server's spellings, integers as JSON numbers),
but a protobuf JSON encoder would emit `CONSISTENCY_STRONG`-style enums and
quoted int64s that the server does not accept.

## Two version numbers

- **Major version**: the URL prefix, `/v1`. A breaking change to a stable
  operation ships as `/v2` (proto package `kosha.v2`, its own document);
  `/v1` keeps working beside it.
- **Contract revision**: `info.version` in the document, `MAJOR.MINOR.PATCH`,
  independent of the server's crate version. MINOR for additive changes,
  PATCH for documentation. Every response carries it as
  `Kosha-Api-Version`, so a client can tell which features a server has.
  Until v1 is declared GA the revision is `0.x`, and corrections that make
  the spec match the wire may still change it.

## What may change inside v1

Additive only:

- new operations, new optional request fields, new response fields;
- new error codes (clients handle an unknown `code` by status and
  `retryable`), new `details` keys, new enum values in responses;
- new headers.

Breaking (needs `/v2`):

- removing or renaming an operation, field or enum value;
- a new required request field, or a type change;
- the same condition answered with a different `code` or status, or
  `retryable` flipped;
- a changed default, a lowered limit, tightened validation of input that
  was accepted.

Behavioural breaks no tool can catch (the last two bullets) are a PR-review
checklist item.

**Unknown request fields** are accepted and ignored in v1 (existing
clients, including the Python client's OpenSearch translator, send some);
v2 will reject them.

### Client obligations

SDKs (#351) are tested against these:

- ignore unknown response fields, headers and enum values;
- branch on `code`, never parse `error`; treat an unknown `code` by its
  HTTP status and `retryable`;
- retry only when `retryable` is true, after `Retry-After`, with jitter and
  a bounded number of attempts.

## Stability levels

Each operation carries `x-stability-level`:

- `stable` (the default): the rules above.
- `beta`: may change with 90 days' notice and a changelog entry. Today:
  branch create, list and delete; `PutShards`; `GetUsage`; `GetBilling`;
  and the whole admin document.

A level only ever moves toward `stable`.

## Deprecation

- The proto marks the item `deprecated`, so the document says
  `deprecated: true`.
- Responses from a deprecated operation carry `Deprecation: @<unix time>`
  (RFC 9745) and a `Link: <...>; rel="deprecation"`. Things that will
  actually be removed (the legacy routes; all of v1 after v2 is GA) also
  carry `Sunset: <date>` (RFC 8594).
- A Sunset date can be pushed later, never earlier.
- After v2 is GA, v1 is supported for at least 12 months, with at least 12
  months between the first `Sunset` header and removal.

## Legacy routes

The unversioned Phase 1 routes predate `/v1`:

| Legacy | Replacement |
|---|---|
| `POST /index` | `POST /v1/namespaces/{namespace}/documents` |
| `POST /replace` | `POST /v1/namespaces/{namespace}/replace` |
| `POST /exists` | `POST /v1/namespaces/{namespace}/exists` |
| `POST /search`, `GET /search?ns=...` | `POST /v1/namespaces/{namespace}/search` |
| `POST /flush` | `POST /v1/namespaces/{namespace}/flush` (flush-all: iterate `GET /v1/namespaces`) |
| `POST /delete` | `POST /v1/namespaces/{namespace}/delete` |
| `GET /stats` | `GET /v1/namespaces/{namespace}/stats` (all namespaces: admin `GET /v1/stats`) |

While they are served, every legacy response carries
`Deprecation: @1797292800` (2026-12-15), `Sunset: Tue, 01 Jun 2027 00:00:00 GMT`
and the `Link` to this section, and is counted in
`kosha_legacy_requests_total{route}` (plus `tenant` with
`KOSHA_METRICS_TENANT_LABELS=on`) and the `legacy route usage:` log line.

Two settings control them:

- `KOSHA_LEGACY_ROUTES=on|off`: the pod default, `on` until the
  self-hosted flip below. Off, a legacy route answers
  `410 legacy_route_disabled` with `details.replacement`.
- `KOSHA_LEGACY_ROUTES_TENANTS=a,b`: tenants that keep the legacy routes
  while the default is off.

`/healthz`, `/readyz` and `/metrics` are operational endpoints, not legacy
routes: they are never gated (see
[../operations/endpoints.md](../operations/endpoints.md)).

### Timeline

| When | Step |
|---|---|
| The release that ships this (target 2026-12-15) | Headers and counter live; Kosha's own clients, bench and tools use only `/v1`; kosha-client 0.3.0. |
| Once the ES migration script is on `/v1` (#353) and stage shows 7 days of zero legacy traffic (or stragglers are allowlisted) | Stage: `KOSHA_LEGACY_ROUTES=off`. |
| After stage is clean and prod shows 7 days of zero | Prod: the same. |
| 2027-03-01 | Self-hosted built-in default flips to `off`. |
| 2027-06-01 (Sunset) | Handlers removed, provided `kosha_legacy_requests_total` read zero on managed deployments for 14 days; otherwise the Sunset is extended and announced. After removal the paths keep answering `410 legacy_route_disabled` for the rest of v1. |

Rollback at any point: set `KOSHA_LEGACY_ROUTES=on` again.

## Changing the contract

1. Edit `proto/kosha/v1/kosha.proto` (and the row in
   `crates/kosha-server/src/routes.rs` for a new route).
2. `make openapi`; bump `info.version`; add an `api/CHANGELOG.md` entry.
3. `make api-check`, `cargo test -p kosha-server public_routes_match_spec`,
   and `python3 scripts/api/check_wire.py --spawn target/debug/kosha-server`.

CI runs all three; see [tools/codegen/README.md](../../tools/codegen/README.md).
