# @decover/kosha

TypeScript client for [Kosha](https://github.com/decover-tech/kosha)'s `/v1`
HTTP API. Server-side JavaScript only (Node 22.12+, Bun, Deno): API keys are
secrets, so do not ship them to a browser.

- ESM, zero runtime dependencies, typed from the checked-in OpenAPI document
  (`api/openapi/v1/kosha.openapi.json`).
- Retries with backoff on `429`/`503`, honouring `retry_after_ms` and
  `Retry-After` ([the shared policy](../conformance/RETRY.md)).
- `searchAll`: every hit of a search, paged with `search_after`.
- `bulkIndex`: batching by count and bytes, bounded concurrency, per-batch
  failures.

## Compatibility

| SDK | API | kosha-server |
|---|---|---|
| 0.x (preview) | `/v1` | any server with `/v1`; error codes and `retryable` need API contract 0.10.0 or later (#350) |

Older servers still work: errors without a `code` surface as `code:
"unknown"` (or the tenant-limit `reason`), and retries fall back to the
status rules in RETRY.md.

## Install

```sh
npm install @decover/kosha
```

## Use

```ts
import { Kosha, KoshaError } from "@decover/kosha";

// apiKey defaults to $KOSHA_API_KEY, baseUrl to $KOSHA_HOST.
const kosha = new Kosha({ baseUrl: "https://kosha.example.com", apiKey: process.env.KOSHA_API_KEY });

await kosha.createNamespace("docs", {
  fields: {
    title: { type: "text", stored: true, indexed: true },
    tag: { type: "keyword", stored: true, indexed: true },
  },
});
const docs = kosha.namespace("docs");

await docs.upsert([
  { id: "1", fields: [{ name: "title", field_type: "Text", value: "hello world" }, { name: "tag", field_type: "Keyword", value: "a" }] },
]);

const page = await docs.search({ query_text: "hello", max_results: 10 });
console.log(page.total_hits, page.results?.map((h) => h.doc_id));
```

Bodies use the wire's snake_case names, exactly as in the OpenAPI document.

### Every hit: `searchAll`

```ts
for await (const hit of docs.searchAll({ query_text: "", filter: { term: { tag: "a" } }, sort: [{ tag: { order: "asc" } }] }, { pageSize: 500 })) {
  console.log(hit.doc_id);
}
```

The sort always ends in an `_id` tie-break (added when missing; the default
sort is `_id` ascending). The cursor for the next page is built from the
last hit: `_id` is its id, `_score` its score, any other key its stored
value, so **sort fields must be stored** (`stored: true`); when you set
`source_includes`, searchAll adds the sort fields to it. A document that
exists and is unchanged for the whole iteration is yielded exactly once, in
sort order; this is not a snapshot. `from`, `search_after`, `rerank` and kNN
or hybrid bodies are rejected with `KoshaConfigError`.

### Many documents: `bulkIndex`

```ts
const result = await docs.bulkIndex(generateDocs(), { maxDocsPerBatch: 500, maxBytesPerBatch: 8 << 20, concurrency: 2 });
// { indexed, batches, failures: [{ batchIndex, ids, error }] }
```

`documents` may be any iterable or async iterable. A batch closes at
`maxDocsPerBatch` documents, or before its request body (the JSON sent,
envelope included) would pass `maxBytesPerBatch` bytes, so the cap can be a
tenant's `max_body_bytes`;
one larger document is sent alone and the server's verdict reported. Each
batch goes through the retry policy. A document whose id is in a batch still
in flight waits for it, and within a batch the last occurrence of an id
wins. With `onError: "stop"` (the default) the first failed batch stops
dispatching, in-flight batches finish, and a `BulkError` carrying the
partial `result` is thrown; `onError: "continue"` returns failures in the
result. `bulkIndex` never flushes.

### Errors

Every failure is a `KoshaError`:

| field | |
|---|---|
| `status` | HTTP status; 0 when no response arrived |
| `code` | the server's error code (docs/api/errors.md), else `"unknown"`; branch on this |
| `retryable`, `retryAfterMs` | the server's verdict and requested delay |
| `requestId` | quote it in bug reports |
| `attempts` | attempts made before giving up |

Subclasses: `KoshaConnectionError`, `KoshaTimeoutError`, `KoshaConfigError`
(nothing was sent) and `BulkError`.

### Retries and options

Defaults: 3 attempts in total, 250 ms backoff base, 8 s cap, server delays
honoured up to 30 s, 60 s total budget, 30 s per-attempt timeout. Override
per client (`new Kosha({ retry: { maxAttempts: 5 }, timeoutMs })`) or per call
(`{ signal, timeoutMs, maxAttempts, retryNonIdempotent, headers }`). A
timed-out namespace create or schema put is not replayed unless you pass
`retryNonIdempotent: true`.

`kosha.request(method, path, body)` reaches any `/v1` route without a typed
method through the same retry and error handling.

## Develop

```sh
npm ci
npm run gen        # regenerate src/generated/schema.ts (or `make clients-gen`)
npm run typecheck && npm test && npm run build
```

The shared conformance suite (`../conformance`) runs with
`npm run test:conformance` against a live server; see its README.
