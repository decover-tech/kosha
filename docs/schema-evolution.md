# Schema evolution

Which namespace schema changes apply online and which need the namespace's
documents re-indexed (#347). The rules are enforced by
`POST /v1/namespaces/{ns}/schema` and by `POST /v1/namespaces` with a
`schema`; the implementation is `kosha_core::schema_evolution`.

## Why some changes need a reindex

Kosha keeps the analysis a segment was written with in its footer, so a
schema change never makes an existing segment unsearchable. Some changes still
leave old and new documents disagreeing about a field: the same text analyzed
two ways, a field that is a keyword in one segment and an integer in the next,
or vectors of two dimensions in one column. Queries over such a namespace
would match old and new documents differently. Those changes are refused once
the namespace holds documents.

A namespace "holds documents" when any of its segments (or, for a sharded
namespace, any shard's) has documents, or documents are waiting in the write
buffer. Deleted documents count until compaction rewrites their segments.

## The rules

| Change | No documents yet | Namespace holds documents |
|---|---|---|
| Add a field | allowed | allowed |
| Remove a field | allowed | allowed: it stops being indexed with its own settings; documents already indexed keep it in their segments |
| Declare a type for a field that had none | allowed | allowed |
| Change a field's type (`text` → `keyword`, …) | allowed | **409 `requires_reindex`** |
| Change a field's `analyzer` or `stopwords` | allowed | **409 `requires_reindex`** |
| Change `default_analyzer` or `default_stopwords` | allowed | **409 `requires_reindex`** (field `default_analyzer`) |
| Change a vector field's `embedding.dims` | allowed | **409 `requires_reindex`** |
| Change a field's `indexed` / `stored` flags (the filterable/sortable switches) | allowed | **409 `requires_reindex`** |
| Change a vector field's embedding model | allowed | 409 from model pinning (#209) while vectors of the old model are indexed |

Type names compare by meaning: `"text"`, `"FIELD_TYPE_TEXT"` and `1` are the
same type. An unset `indexed` or `stored` flag equals `false`.

## The error

```json
HTTP/1.1 409 Conflict

{
  "error": "requires_reindex: field \"title\": changing its analyzer requires a reindex because the namespace holds documents (delete and re-ingest them, or use a new field)",
  "code": "requires_reindex",
  "field": "title",
  "reason": "analyzer_change"
}
```

`reason` is one of `analyzer_change`, `flags_change`, or an object
`{"type_change": {"from", "to"}}` / `{"vector_dims_change": {"from", "to"}}`.
Nothing changes when a request is refused; a sharded namespace's shards are
checked before any manifest is written.

## Reindexing

Kosha does not reindex automatically. To apply a refused change:

1. Export the documents (`GET /v1/namespaces/{ns}/export`, paging with the
   `x-kosha-next-cursor` header), or re-read them from the source system.
2. Create a new namespace with the new schema and index the documents into it,
   or delete the namespace (`DELETE /v1/namespaces/{ns}`), set the schema while
   it is empty, and index again.

Alternatively, put the new analysis on a new field name: adding a field is
always online.
