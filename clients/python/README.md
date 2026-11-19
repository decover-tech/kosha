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

## Index introspection

`client.indices.get_mapping(index=...)` returns the namespace's real schema, read from
`GET /v1/namespaces/{ns}/schema` and translated to Elasticsearch types:

| Kosha `FieldType` | ES mapping type |
|-------------------|-----------------|
| `FIELD_TYPE_TEXT` | `text` |
| `FIELD_TYPE_KEYWORD` | `keyword` |
| `FIELD_TYPE_INTEGER` | `long` |
| `FIELD_TYPE_FLOAT` | `double` |
| `FIELD_TYPE_DATE` | `date` |
| `FIELD_TYPE_BOOLEAN` | `boolean` |
| `FIELD_TYPE_VECTOR` | `knn_vector` |

Kosha is schemaless, so a namespace with no registered schema, or a server that predates the
schema route (404/405), returns `{"<index>": {"mappings": {"properties": {}}}}`. A field the
schema lists without a type appears as `{}`; the client never guesses a type.

`client.indices.get_settings(index=...)` reports only `{"index": {"provided_name": "<index>"}}`.
Kosha has no shards, replicas or refresh interval per namespace, so none are reported.

## Creating and deleting indices

`client.indices.create(index=..., body=...)` calls `POST /v1/namespaces`. Plain top-level
fields in `body["mappings"]["properties"]` are sent as the namespace schema (ES type to
`FieldType`, the reverse of the table above, plus `integer`/`short`/`byte` → `INTEGER`,
`float`/`half_float`/`scaled_float` → `FLOAT`, `date_nanos` → `DATE`, `dense_vector` →
`VECTOR`); object, nested and other types with no Kosha equivalent are left out, and
`settings` are ignored. Creating an index that exists raises `KoshaRequestError` with status
409; `ignore=400` (what ES callers pass for `resource_already_exists_exception`) or
`ignore=409` returns the error body instead. Creating first is optional: Kosha still creates a
namespace on its first write.

`client.indices.delete(index=...)` calls `DELETE /v1/namespaces/{ns}` for each named index and
permanently removes its documents and segments. A missing index raises `KoshaRequestError`
with status 404 (ES `NotFoundError`), unless `ignore_unavailable=True` or `ignore=404` is
passed. `client.indices.exists(index=...)` reads `GET /v1/namespaces/{ns}/stats`: 404 means
absent; other errors are raised.

## Changelog

### 0.2.15

- **Behavior change:** `indices.delete()` deletes the namespace (`DELETE /v1/namespaces/{ns}`)
  instead of logging and returning `{"acknowledged": true}` without deleting anything. A
  missing index now raises a 404 `KoshaRequestError`.
- `indices.create()` creates the namespace (`POST /v1/namespaces`) with a schema translated
  from the ES mappings, and raises 409 when it already exists.
- `indices.exists()` uses the namespace stats route and raises errors other than 404 instead
  of reporting them as "absent".

### 0.2.14

- **Behavior change:** `indices.get_mapping()` no longer returns a fixed, made-up set of
  fields for every index. It returns the namespace's schema, or empty `properties` when there
  is none. Code that read fields from the old placeholder mapping now gets no fields and should
  handle an empty mapping. `indices.get()` reports the same mapping.
- **Behavior change:** `indices.get_settings()` no longer reports `refresh_interval`,
  `number_of_shards`, `number_of_replicas` or `knn`; it returns only `provided_name`.
- `get_mapping` / `get_settings` / `get` accept a list or comma-separated string of indices.
- Removed application-specific field names from examples, tests and docstrings.
