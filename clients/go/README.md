# Kosha Go SDK

```go
import kosha "github.com/decover-tech/kosha/clients/go"
```

A client for [Kosha](https://github.com/decover-tech/kosha)'s `/v1` HTTP
API. Standard library only; request and response types are generated from
the checked-in OpenAPI document (`api/openapi/v1/kosha.openapi.json`).

- Retries with backoff on `429`/`503`, honouring `retry_after_ms` and
  `Retry-After` ([the shared policy](../conformance/RETRY.md)).
- `SearchAll`: every hit of a search as an `iter.Seq2`, paged with `search_after`.
- `BulkIndex`: batching by count and bytes, bounded concurrency, per-batch
  failures.

## Compatibility

| SDK | API | kosha-server | Go |
|---|---|---|---|
| 0.x (preview) | `/v1` | any server with `/v1`; error codes and `retryable` need API contract 0.10.0 or later (#350) | 1.25+ |

## Use

```go
ctx := context.Background()
// The key and URL default to $KOSHA_API_KEY and $KOSHA_HOST.
c, err := kosha.NewClient(kosha.WithBaseURL("https://kosha.example.com"), kosha.WithAPIKey(key))
if err != nil { ... }

_, err = c.CreateNamespace(ctx, "docs", &kosha.NamespaceSchema{
	Fields: &map[string]kosha.FieldSchema{
		"title": {Type: kosha.Ptr("text"), Stored: kosha.Ptr(true), Indexed: kosha.Ptr(true)},
		"tag":   {Type: kosha.Ptr("keyword"), Stored: kosha.Ptr(true), Indexed: kosha.Ptr(true)},
	},
})
docs := c.Namespace("docs")

_, err = docs.Upsert(ctx, []kosha.Document{{Id: "1", Fields: []kosha.Field{
	{Name: "title", FieldType: kosha.FieldTypeText, Value: "hello world"},
	{Name: "tag", FieldType: kosha.FieldTypeKeyword, Value: "a"},
}}})

page, err := docs.Search(ctx, kosha.SearchRequest{QueryText: "hello", MaxResults: kosha.Ptr[int64](10)})
```

### Every hit: `SearchAll`

```go
req := kosha.SearchRequest{Sort: &[]map[string]any{{"tag": map[string]any{"order": "asc"}}}}
for hit, err := range docs.SearchAll(ctx, req, 500) {
	if err != nil { return err }
	fmt.Println(*hit.DocId)
}
```

The sort always ends in an `_id` tie-break (added when missing; the default
sort is `_id` ascending). The next page's cursor comes from the last hit
(`_id`: its id, `_score`: its score, any other key: its stored value, or for
a list value the element the server sorted by: the smallest ascending, the
largest descending), so **sort fields must be stored**; the sort fields are
added to `SourceIncludes` when it is set and removed from `SourceExcludes`.
A document that exists and is unchanged for the whole
iteration is yielded exactly once, in sort order; this is not a snapshot.

### Many documents: `BulkIndex`

```go
res, err := docs.BulkIndex(ctx, documents, kosha.BulkOptions{MaxDocsPerBatch: 500, Concurrency: 2})
var be *kosha.BulkError
if errors.As(err, &be) { /* be.Result.Failures[i].IDs, .Err */ }
```

`BulkIndexSeq` takes an `iter.Seq[Document]` for streams. A batch closes at
`MaxDocsPerBatch` (500), or before the request body (the JSON sent, envelope
included) would pass `MaxBytesPerBatch` (8 MiB), so the cap can be a
tenant's `max_body_bytes`; one larger document
is sent alone. A document whose id is in a batch still in flight waits for
it; within a batch the last occurrence wins. By default the first failed
batch stops dispatching and returns `*BulkError` with the partial result;
`ContinueOnError` reports failures in the result instead. Never flushes.

### Errors

- `*kosha.Error`: an HTTP error response. `Code` is the server's error code
  (docs/api/errors.md), else `"unknown"`; also `StatusCode`, `Retryable`,
  `RetryAfter`, `RequestID`, `Details`, `Attempts`, `Body`. Status-class
  sentinels work with `errors.Is`: `ErrNotFound`, `ErrRateLimited`,
  `ErrUnavailable`, `ErrInvalidArgument`, ...
- `*kosha.TransportError`: no response (DNS, connection, timeout). It wraps
  the cause, so `errors.Is(err, context.DeadlineExceeded)` works.
- `*kosha.ConfigError` (`errors.Is(err, kosha.ErrConfig)`): nothing was sent.

### Retries and options

Defaults (`kosha.DefaultRetryPolicy`): 3 attempts in total, 250 ms backoff
base, 8 s cap, server delays honoured up to 30 s, 60 s total budget; 30 s
per-attempt timeout. Client options: `WithRetryPolicy`, `WithTimeout`,
`WithHTTPClient`, `WithHeader`. Per call: `WithMaxAttempts`,
`WithCallTimeout`, `WithRetryNonIdempotent`, `WithCallHeader`. Cancelling
`ctx` stops retries and sleeps.

`c.Do(ctx, method, path, body, &out)` reaches any `/v1` route without a typed
method through the same retry and error handling.

## Develop

```sh
make clients-gen          # regenerate zz_generated_types.go (oapi-codegen, pinned in tools/go.mod)
go vet ./... && go test -race ./...
```

The shared conformance suite (`../conformance`) runs with
`go test -tags conformance -run Conformance ./...` against a live server;
see its README.
