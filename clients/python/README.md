# Python client for Kosha

## Structure

Following the **spec → codegen → thin client** pattern (see `DESIGN.md §12.1`):

```
kosha_client/
  __init__.py          # Public API — exports KoshaClient
  stubs/               # GENERATED — do not edit by hand
    models.py           # Request/response types from OpenAPI spec
    api.py              # HTTP client methods from OpenAPI spec
  transport.py         # THIN — auth, retry, timeout, logging
  compat.py            # THIN — opensearch compatibility shim (_Serializer, _Transport)
  client.py            # THIN — KoshaClient that delegates to stubs
```

## Layers

| Layer | Origin | Responsibility |
|-------|--------|----------------|
| `stubs/` | **Auto-generated** from `proto/kosha/v1/kosha.proto` via OpenAPI → openapi-generator | Request serialization, URL construction, response deserialization |
| `transport.py` | **Hand-written** (~30 lines) | Bearer/JWT auth injection, exponential backoff retry, log correlation |
| `compat.py` | **Hand-written** (~30 lines) | `_Serializer` / `_Transport` duck-types for `opensearchpy.helpers.bulk` |
| `client.py` | **Thin hand-written** (~100 lines) | `KoshaClient(search, index, bulk, count, ...)` that translates OpenSearch-style calls to stub methods |

## Regenerating stubs

```bash
# From repo root — generate OpenAPI spec from proto
make openapi
# The spec lands at gen/openapi/kosha.yaml

# Generate Python stubs
npx @openapitools/openapi-generator-cli generate \
  -i gen/openapi/kosha.yaml \
  -g python \
  -o clients/python/kosha_client/stubs
```

## Current state

Phase 1 ships with a lightweight hand-written client (`client.py`) that directly calls the
Kosha HTTP API. As the API stabilises, the hand-written code will shrink to just `transport.py`
+ `compat.py` + the delegation layer in `client.py`.

## Changelog

### 0.2.11.2

Keep-alive on top of 0.2.11.1 (which it includes); nothing else from 0.2.12+ is included.

- Connections are kept alive and reused, instead of one connection (one DNS lookup, one TCP
  handshake) per request. A process keeps at most 10 idle connections per host, for 15 s
  idle (the server closes at 30 s) and 60 s in total, so new server pods get traffic; all
  clients for the same host share them, and the pool is thread-safe and reset in a forked
  child. A read whose reused connection turns out closed is resent once on a new
  connection; a write takes the connection-error retry. Timeouts and errors are those of
  `urlopen` as before. A host that the `*_proxy` environment sends through a proxy still
  uses `urlopen` per request.

### 0.2.11.1

Retry-only release on top of 0.2.11; nothing else from 0.2.12+ is included.

- **Behavior change:** the transport retries what the server asks it to retry. A `429`
  (any request) and a `502`/`503`/`504` on a read (`GET`, or a `POST` to the `search`
  route) are retried after the server's `Retry-After` plus jitter, or a full-jitter
  exponential backoff without one. Attempts in total are `max_retries` (default 3), and no
  retry starts later than `retry_budget` seconds (default 20) after the first attempt; a
  `Retry-After` that does not fit the budget (a quota refusal says 60 s) raises at once.
  `5xx` on writes, `500` and every other `4xx` (including `402`) are raised immediately, as
  before. `retry_budget=0` restores the old behavior.
- The attempts and the budget apply **per HTTP call**: `bulk` (one call per namespace and
  operation, plus a flush per namespace, per `helpers.bulk` chunk) can wait up to
  `retry_budget` once per call.
- An error body that is not a JSON object now raises `KoshaRequestError` with the raw body as
  `error`, instead of an `AttributeError`.
