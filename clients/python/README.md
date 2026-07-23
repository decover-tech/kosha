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
