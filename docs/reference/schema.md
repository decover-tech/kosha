# Schema evolution

How field types change on a live namespace. Moved from the README.

## Schema evolution

A namespace schema (`POST /v1/namespaces/{ns}/schema`, or `schema` on `POST /v1/namespaces`) can
change while the namespace holds documents only in ways those documents can follow (#347). The
rules live in `kosha_core::schema_evolution` and are enforced by the writer for every route that
sets a schema, shards of a sharded namespace included. On a namespace without documents any valid
schema applies.

| Change | With documents |
| --- | --- |
| Add a field (any settings) | online |
| Remove a field from the schema | online: new segments use the defaults |
| Change `type`, `indexed` or `stored` (declare, change or drop) | online, reported as `declarative_changed`: the engine does not read them, so documents keep the types they were sent with and are indexed and stored as before |
| Change `analyzer` / `stopwords` of a field, `default_analyzer`, `default_stopwords` | online, lazily: new segments use the new chain, existing segments answer with the chain their footer records until compaction re-analyzes them (`POST /v1/admin/compact-namespace` with `"mode": "full"`) or a reindex |
| Declare an `embedding` on a field holding no vectors | online |
| Drop a field's `embedding` | online: stored vectors stay, clients send vectors again |
| Change a field's embedding model or `dims` while it holds vectors | **reindex** (`embedding_change`) |
| Declare an `embedding` whose model differs from the model the field's stored vectors were embedded with (also after dropping the old declaration in an earlier request) | **reindex** (`embedding_change`) |
| Declare an `embedding` whose `dims` differ from the vectors the field holds | **reindex** (`vector_dims_change`) |

A refused change is a **409** with `{"error", "field", "rule", "reindex_required": true}` and
changes nothing; an invalid schema is still a 400. A successful response lists the online
`changes` (`field`, `change`, `effect`). Only the embedding rules can refuse a change: they are
judged against the models and dimensions the stored segments record, not only the previous
schema, and buffered documents are flushed before an embedding declaration changes so their
model is recorded first; removing a declaration and adding another one cannot get around them.

Analyzer changes stay online, unlike in Elasticsearch, because every segment records the chains
it was written with and queries are analyzed per segment ([Analyzers](features.md#analyzers)), so no
segment ever returns wrong hits; what changes is only when old documents pick up the new chain.

**Reindex path.** Create a new namespace with the new schema (`POST /v1/namespaces` with
`schema`), export the old one ([Exports](exports.md#exports)) and send its lines to the new namespace's
`/documents`, switch readers over, then delete the old namespace. A branch is no shortcut: it
shares the parent's segments, which keep their old settings.
