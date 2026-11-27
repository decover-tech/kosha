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
# The spec lands at api/openapi/v1/kosha.openapi.json (checked in)

# Generate Python stubs
npx @openapitools/openapi-generator-cli generate \
  -i api/openapi/v1/kosha.openapi.json \
  -g python \
  -o clients/python/kosha_client/stubs
```

## Current state

Phase 1 ships with a lightweight hand-written client (`client.py`) that directly calls the
Kosha HTTP API. As the API stabilises, the hand-written code will shrink to just `transport.py`
+ `compat.py` + the delegation layer in `client.py`.

Since 0.3.0 the client calls only the `/v1` API (`/v1/namespaces/{ns}/documents`,
`.../replace`, `.../search`, ...), so it works against servers with
`KOSHA_LEGACY_ROUTES=off`; 0.2.x needs the legacy routes on.

**Server compatibility.** Upgrade the server before upgrading the SDK.
Partial updates (`update()`, bulk `update` actions, `update_by_query`) use
`POST /v1/namespaces/{ns}/replace` and require a server that supports it
(API contract 0.10.0 or later). The SDK surfaces any error from that
operation rather than retrying it on the deprecated route. `KoshaRequestError`
carries the server's error model: `code`, `retryable`, `retry_after_ms` and
`request_id` (see [docs/api/errors.md](../../docs/api/errors.md)), plus
`attempts`.

**Retries.** Every request follows the retry policy the TypeScript and Go
SDKs share ([clients/conformance/RETRY.md](../conformance/RETRY.md)): an
error the server marks `"retryable": true` (or a bare 429/503 from an older
server or a proxy) is retried after the server's `retry_after_ms` /
`Retry-After`, else with jittered backoff; `"retryable": false` (a quota) is
never retried. `max_retries` (default 3) counts attempts, so `max_retries=1`
fails fast. A connection that could not be opened is retried for every
operation; a timeout after the request was sent only for idempotent ones
(search, upsert, merge, delete-by-query, flush, every GET), unless you pass
`retry_non_idempotent=True`. `retry_on_timeout=False` turns off every
transport-error retry (timeouts, resets, refused connections, DNS
failures), as in 0.2.x; HTTP error responses still follow the policy.

**Native helpers.** `search_all(namespace, body, page_size=None)` yields
every hit of a native `/v1` search body, paging with `search_after` (the
sort gets an `_id` tie-break); `bulk_index(namespace, documents, ...)`
upserts native documents in batches by count and bytes, with bounded
concurrency and per-batch failures (`BulkError.result`). A batch closes
before its request body (the bytes actually sent, envelope included) would
pass `max_bytes_per_batch`, so the cap can be set to a tenant's
`max_body_bytes`. They take Kosha's
own bodies, not OpenSearch DSL, and behave like `searchAll` / `bulkIndex`
in the TypeScript and Go SDKs.

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
- `get` sees what search sees. Under the server's default `strong` consistency (#255) that
  includes documents still in its write buffer; under `eventual` a buffered document (up to
  ~1 s, `KOSHA_FLUSH_MAX_AGE_MS`) is not found yet.

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
| `FIELD_TYPE_GEO_POINT` | `geo_point` |

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

## Rerank

A Cohere rerank over the top search hits, which Sage runs today as

```python
hits = client.search(index, body)["hits"]["hits"]
co.rerank(model="rerank-v3.5", query=q, documents=[h["_source"]["content"] for h in hits], top_n=50)
```

becomes one call; the server calls the provider and holds the API key:

```python
resp = client.search(index, body, rerank=KoshaClient.rerank_spec("content", model="rerank-v3.5", top_n=50))
```

`rerank` can also go in the body (Cohere's `rank_fields: ["content"]` is accepted for `field`).
Each hit's `_score` is the rerank score, with `_fused_score` (search stage) and `_rerank_score`
alongside. If the provider times out or fails, hits come back in search order and
`resp["rerank"] == {"degraded": True, "reason": ...}`.

## Agent tools and the MCP server

`AgentTools` (`kosha_client/agent.py`) is a thin SDK for AI agents over the native `/v1` API: one
method per tool, plain JSON-able arguments, and results sized for a model's context window.
`kosha-mcp` serves the same tools over the [Model Context Protocol](https://modelcontextprotocol.io)
(issue #584).

```bash
pip install 'kosha-client[mcp]'
export KOSHA_HOST=https://kosha.example.com KOSHA_API_KEY=sk-... KOSHA_NAMESPACE=docs
kosha-mcp                                  # stdio, for Claude Desktop / Claude Code / Cursor
kosha-mcp --transport http --port 8765     # streamable HTTP at http://127.0.0.1:8765/mcp
```

The HTTP endpoint has no authentication of its own: every caller acts with the server's
`KOSHA_API_KEY`. It binds loopback by default and refuses another `--host` unless you also
pass `--allow-remote`, which you should only do behind an authenticating proxy.

| Tool | API call | Notes |
|------|----------|-------|
| `search` | `POST /search` with `query_text` | BM25; optional `filter`, `fields`, `include_fields`. |
| `semantic_search` | `POST /search` with `knn` | `query` (server-side embedder) or `vector`; `hybrid` fuses BM25 and kNN with RRF. |
| `filter` | `POST /search` with only `filter` | Unscored; with `sort`, a full page returns `next_search_after` for the next one (`_id` breaks ties). |
| `count` | `POST /search`, `max_results: 0`, `exact_total_hits` | Returns `{"count", "relation"}`. |
| `aggregate` | `POST /search` with `aggs` | No hits returned. |
| `list_branches` | `GET /branches` | |
| `branch` | `POST /branch` | Only with `--allow-writes`; needs an `admin` key. |
| `index_documents` | `POST /documents` then `POST /flush` | Only with `--allow-writes`; needs a `write` key. |

Every hit is `{"id", "score", "path"?, "fields", "truncated_fields"?, "highlight"?}`. `id` is the
stable document id. `path` is lifted out of the `path` field (`AgentTools(path_field=...)`) so an
agent can narrow with `{"prefix": {"path": "/contracts/2024/"}}`. Hits per call are capped at 100
(`max_results_cap`), each stored string is cut to 500 characters (`snippet_chars`) and named in
`truncated_fields`, and vectors are never returned. Without `--allow-writes` every tool is
annotated read-only, so a `read`-scoped key is enough. API errors reach the agent as tool errors
carrying the server's message.

Claude Desktop config (`claude_desktop_config.json`): see `examples/claude_desktop_config.json`.
From Python, without MCP: see `examples/agent_tools.py`.

## Ingesting PDF, DOCX and HTML files

`kosha-ingest` (`kosha_client/ingest.py`, issue #587) turns a folder of `.pdf`, `.docx`,
`.html`/`.htm`, `.txt` and `.md` files into Kosha documents. Extraction runs client-side
with [pypdf](https://pypi.org/project/pypdf/) and
[python-docx](https://pypi.org/project/python-docx/), both under the `ingest` extra.

```bash
pip install 'kosha-client[ingest]'
export KOSHA_HOST=https://kosha.example.com KOSHA_API_KEY=sk-...   # a write key
kosha-ingest ./shared-drive --namespace docs --prefix /drive      # one document per file
kosha-ingest ./shared-drive --namespace docs-pages --per-page     # one document per page
```

The command prints `{"indexed", "unchanged", "unsupported", "scanned", "failed"}` and exits 1
when a file failed to extract. Each document has these fields:

| Field | Type | Value |
|-------|------|-------|
| `path` | Keyword | `--prefix` + the path under the folder, e.g. `/drive/contracts/2024/msa.pdf`. |
| `body` | Text | The extracted text. Pages are separated by a form feed (`\f`). |
| `page` | Integer | With `--per-page` only: the 1-based page number. |
| `title` | Text | From the PDF metadata, DOCX core properties or HTML `<title>`, when one is set. |
| `mime_type`, `content_sha256`, `source` | Keyword | `source` defaults to `kosha-ingest` (`--source`). |
| `size_bytes`, `page_count` | Integer | |
| `modified_at` | Date | The file's mtime, in UTC. |
| `scanned` | Boolean | `true` for a PDF with no text layer. OCR is out of scope, so its body is empty. |

PDF pages are the PDF's own pages. A DOCX page ends at an explicit page break; Word's layout
pages are not known without rendering. HTML, `.md` and `.txt` files are one page, except that
form feeds in a `.txt` file (`pdftotext` output) split it into pages.

With `--per-page`, a search hit carries its page number. Without it, `read(path, page=n)` (#586)
returns page `n` of the document.

Re-running the command is idempotent. A document's id comes from its `path`, so a changed file
replaces its old document. A file with an unchanged `content_sha256` is skipped; `--force`
re-indexes it anyway. With `--per-page`, pages beyond a shortened file's new length are deleted.

## Changelog

### Unreleased

- `AgentTools` and the `kosha-mcp` MCP server (stdio and streamable HTTP), behind the new
  `mcp` extra (issue #584). See "Agent tools and the MCP server".
- `kosha-ingest`: PDF/DOCX/HTML/text ingestion with page boundaries, behind the new `ingest`
  extra (issue #587). See "Ingesting PDF, DOCX and HTML files".

### 0.3.1

- **Behavior change (type inference):** a `_source` value that is a list of equal-length numeric
  lists, each at least 8 long (per-token embeddings from a ColBERT-style model), is now sent as
  one `Vector` field holding the JSON bag, which the server indexes for late-interaction
  (MaxSim) search (issue #205). It used to be sent as a `Keyword` holding the same JSON. Ragged
  lists, lists of shorter tuples (polygons, bounding-box pairs) and non-numeric nested lists keep
  the `Keyword` mapping. Documents read back unchanged either way. If you index such values
  today and filter on them by exact match (a `Keyword` behavior), keep that field out of the
  upgrade or reindex: new writes send it as `Vector`, which is not filterable.
- `knn` passes `vectors` (a bag of query token vectors) and `scoring` (`"cosine"` or
  `"late_interaction"`) through to the server. `vector` with `vectors`, or either with
  `query_text`, is refused.

### 0.3.0

- Calls only the `/v1` API (see "Current state").
- **Behavior change:** HTTP errors are retried under the shared policy
  ([clients/conformance/RETRY.md](../conformance/RETRY.md)): 429 and 503,
  and any error with `"retryable": true`, are retried (up to `max_retries`
  attempts in total, 3 by default) after the server's delay. `max_retries=1`
  restores fail-fast.
- **Behavior change:** a timeout after the request was sent is no longer
  retried for non-idempotent operations (namespace create, schema put,
  branches, shards) unless `retry_non_idempotent=True`. Merges sent to the
  deprecated `POST /replace` (pre-0.9 servers) still count as idempotent.
- `retry_on_timeout=False` still stops every transport-error retry,
  refused connections and DNS failures included, as in 0.2.x.
- **Behavior change:** request bodies are compact UTF-8 JSON (no spaces,
  non-ASCII text unescaped), the same bytes the TypeScript and Go SDKs send.
  The server reads them the same; bodies with non-ASCII text are smaller.
- `KoshaRequestError` gains `code` (falling back to a tenant-limit
  `reason`), `retryable`, `retry_after_ms`, `request_id` and `attempts`.
- New `search_all()` and `bulk_index()` native helpers.
- The 0.2.17 retry rules are replaced by the shared policy above (503 is
  retried for writes too; 502/504 only for idempotent operations). The
  0.2.17 `retry_budget` (seconds) is still accepted and sets
  `total_budget_ms`. Connections stay kept alive and pooled as in 0.2.17.

### 0.2.17

- **Behavior change:** the transport retries what the server asks it to retry. A `429`
  (any request: the server sends it only for work it refused before starting) and a
  `502`/`503`/`504` on a read (search, count, get, `GET` routes) are retried after the
  server's `Retry-After` plus jitter, or a full-jitter exponential backoff without one.
  Attempts in total are `max_retries` (default 3), and no retry starts later than
  `retry_budget` seconds (default 20) after the first attempt; a `Retry-After` that does not
  fit the budget (a quota refusal says 60 s) raises at once. `5xx` on writes, `500` and
  every other `4xx` (including `402`) are raised immediately, as before.
  `retry_budget=0` restores the old behavior.
- The attempts and the budget apply **per HTTP call**. A method that makes several calls
  multiplies them: `msearch` (one search per body), `mget` (one search per id), `bulk`
  (one call per namespace and operation, per `helpers.bulk` chunk) and `refresh=True` writes
  (an extra flush) can each wait up to `retry_budget` once per call.
- An error body that is not a JSON object (a proxy's HTML page, a JSON list) now raises
  `KoshaRequestError` with the raw body as `error`, instead of an `AttributeError`.
- Connections are kept alive and reused, instead of one connection (one DNS lookup, one TCP
  handshake) per request. A process keeps at most 10 idle connections per host, for 15 s
  idle (the server closes at 30 s) and 60 s in total, so new server pods get traffic; all
  clients for the same host share them, and the pool is thread-safe and reset in a forked
  child. A read whose reused connection turns out closed is resent once on a new
  connection; a write takes the connection-error retry. Timeouts and errors are those of
  `urlopen` as before. A host that the `*_proxy` environment sends through a proxy still
  uses `urlopen` per request.

### 0.2.16

- Added the `consistency` option, `KoshaClient(..., consistency="strong" | "eventual")`
  (issue #255). It is sent with every search (`search`, `count`, `get`, `mget`, `msearch`);
  `None`, the default, leaves it to the server, which defaults to `strong`: writes are
  searchable as soon as they return, without a flush. With `consistency="strong"`,
  `refresh="wait_for"` no longer flushes (the write is already searchable); `refresh=True` and
  `indices.refresh()` still flush.

### 0.2.15

- Added `get`, `mget` and `msearch` (issue #198). They raised `AttributeError` before.
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
