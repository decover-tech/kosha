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

## Supported APIs

`KoshaClient` stands in for `opensearchpy.OpenSearch`. These methods are implemented:

| Method | Notes |
|--------|-------|
| `search` | Query DSL is translated to Kosha's query; a clause Kosha cannot express raises `KoshaRequestError(400)` instead of being dropped. |
| `count` | A zero-hit search with an exact total. |
| `get` | Fetch one document by id. See below. |
| `mget` | Fetch several documents by id. See below. |
| `msearch` | Several searches. See below. |
| `index`, `bulk`, `update` | `bulk` handles `index`/`create`/`update`; `delete` actions report 501. |
| `delete_by_query` | The whole query must translate as a filter. |
| `update_by_query` | Scripts support a small Painless subset (field copies, literals, `params.<map>[_did]` lookups). |
| `scroll` | Raises `NotImplementedError`: use `from`/`size` or `search_after`. |
| `indices.*` | `create`, `exists`, `delete`, `refresh`, `get`, `get_mapping`, `get_settings`, `put_mapping`, `put_settings`, `flush`, `close`. |
| `tasks.get` | Polls `update_by_query(wait_for_completion=False)` tasks. |

### `get` and `mget`

Kosha has no fetch-by-id route, and its filters cannot match `_id`. `get(index, id)` is a
one-hit search that seeks to the id: sort by `_id` descending with
`search_after: [id + "\0"]`. `id + "\0"` is the next string after `id`, so the first hit is
the largest id `<= id`, which is the document itself when it exists. The response has the ES
shape `{_index, _id, _version: 1, found, _source}`, and `_source` decodes exactly as in `search`.

- A missing document, or an index that does not exist yet, returns `{"found": false}`.
  **It does not raise.** opensearch-py raises `NotFoundError` here; with this client, check
  `found`.
- `_source` (bool, name, comma list or list), `_source_includes` and `_source_excludes`
  filter fields as in `search`. `refresh=True` flushes the index first.
- `get` sees what search sees. A document still in the server's write buffer (up to ~1 s,
  `KOSHA_FLUSH_MAX_AGE_MS`) is not found yet; there is no realtime GET.

`mget(body, index=None)` accepts `{"docs": [{"_index"?, "_id", "_source"?}]}` or
`{"ids": [...]}` and returns `{"docs": [...]}` in request order, with `found: false` entries for
misses. It runs one `get`-style search per distinct document (ids cannot be combined into one
filter), so it is N round trips. A lookup the server rejects becomes an entry with an `error`
object instead of failing the whole call.

### `msearch`

`msearch(body, index=None)` takes the ES multi-search body as NDJSON (`str` or `bytes`) or as a
list alternating headers and search bodies (dicts or JSON strings). A header without `index`
searches `index`. **It is N sequential `POST /search` requests, not one batched request**:
Kosha has no multi-search endpoint. Each search goes through the same translation as `search`.
A search that fails becomes `{"error": {...}, "status": <code>}` in its slot and the rest still
run; successful items carry `"status": 200`. A malformed body (not JSON, or a header with no
search body) raises `KoshaRequestError(400)` before anything runs.

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

## Changelog

### 0.2.15

- Added `get`, `mget` and `msearch` (issue #198). They raised `AttributeError` before.

### 0.2.14

- **Behavior change:** `indices.get_mapping()` no longer returns a fixed, made-up set of
  fields for every index. It returns the namespace's schema, or empty `properties` when there
  is none. Code that read fields from the old placeholder mapping now gets no fields and should
  handle an empty mapping. `indices.get()` reports the same mapping.
- **Behavior change:** `indices.get_settings()` no longer reports `refresh_interval`,
  `number_of_shards`, `number_of_replicas` or `knn`; it returns only `provided_name`.
- `get_mapping` / `get_settings` / `get` accept a list or comma-separated string of indices.
- Removed application-specific field names from examples, tests and docstrings.
