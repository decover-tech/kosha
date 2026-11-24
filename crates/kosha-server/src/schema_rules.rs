//! Schema evolution rules on the HTTP surface (#347). The rules themselves
//! are in [`kosha_core::schema_evolution`]; `kosha_write::Indexer::set_schema`
//! enforces them too, so every route that changes a schema (`POST
//! /v1/namespaces/{ns}/schema`, `POST /v1/namespaces` with a schema, the
//! shards of a sharded namespace) is covered. This module turns a refusal
//! into a 409 that names the field and the rule.

use super::*;
use kosha_core::schema_evolution::{self, SchemaChange, SchemaChangeError};

/// The 409 for a change that needs a reindex:
/// `{"error", "field", "rule", "reindex_required": true}`.
pub(crate) fn refusal(refused: &SchemaChangeError) -> String {
    let body = serde_json::json!({
        "error": refused.message,
        "field": refused.field,
        "rule": refused.rule,
        "reindex_required": true,
    })
    .to_string();
    format!(
        "HTTP/1.1 409 Conflict\r\ncontent-length: {}\r\ncontent-type: application/json\r\n\r\n{body}",
        body.len()
    )
}

/// Check `schema` against what `targets` (a namespace, or a sharded root
/// and its shards) hold now. `Ok` lists the online changes of the first
/// target; `Err` is the ready-made 409.
pub(crate) fn check(
    state: &AppState,
    targets: &[NamespaceId],
    schema: &kosha_core::NamespaceSchema,
) -> Result<Vec<SchemaChange>, String> {
    // A segment records the embedding model of its vectors from the schema
    // in force when it is flushed. Before an embedding declaration changes,
    // buffered vectors are flushed under the current one, so the rules (and
    // every later change) see the model they were embedded with: dropping a
    // declaration and adding another one cannot slip past them.
    if let Some(root) = targets.first() {
        let current = state
            .indexer
            .manifest_cloned(root)
            .and_then(|manifest| manifest.schema);
        let buffered =
            state.indexer.pending_doc_count(root) > 0 || state.indexer.shard_count(root).is_some();
        if buffered && schema_evolution::changes_embeddings(current.as_ref(), schema) {
            flush_and_publish_namespace(state, root)?;
        }
    }
    let mut first = None;
    for target in targets {
        let current = state
            .indexer
            .manifest_cloned(target)
            .and_then(|manifest| manifest.schema);
        let existing = state.indexer.schema_existing_data(target);
        match schema_evolution::check(current.as_ref(), schema, &existing) {
            Ok(changes) => {
                first.get_or_insert(changes);
            }
            Err(refused) => return Err(refusal(&refused)),
        }
    }
    Ok(first.unwrap_or_default())
}

/// Whether an `InvalidQuery` from `Indexer::set_schema` is a refused
/// change (409) rather than an invalid schema (400).
pub(crate) fn is_reindex_refusal(message: &str) -> bool {
    message.contains("requires a reindex")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> AppState {
        AppState::new(crate::tests::test_dir())
    }

    fn put(state: &AppState, body: serde_json::Value) -> String {
        handle_put_schema_with_ns("docs", "acme", body.to_string().as_bytes(), state)
    }

    fn status(response: &str) -> u16 {
        response
            .split(' ')
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    fn body(response: &str) -> serde_json::Value {
        serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap()
    }

    fn index_vector(state: &AppState, ns: &NamespaceId, dims: usize) {
        let vector: Vec<f32> = (0..dims).map(|i| i as f32).collect();
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![kosha_core::Document {
                    id: kosha_core::DocumentId("d1".into()),
                    fields: vec![
                        kosha_core::Field::text("title", "hello"),
                        kosha_core::Field {
                            name: "emb".into(),
                            field_type: kosha_core::FieldType::Vector,
                            value: serde_json::to_string(&vector).unwrap(),
                        },
                    ],
                }],
            )
            .unwrap();
    }

    /// Declarative settings change online and are reported; only rules the
    /// engine depends on (embeddings against stored vectors) answer 409.
    #[test]
    fn schema_route_applies_online_changes_and_refuses_reindex_ones() {
        let state = state();
        let ns = NamespaceId("acme/docs".into());
        let first = put(
            &state,
            serde_json::json!({"fields": {"price": {"type": "FIELD_TYPE_FLOAT"}}}),
        );
        assert_eq!(status(&first), 200, "{first}");
        index_vector(&state, &ns, 3);

        // With documents, `type` / `stored` changes are declarative.
        let retyped = put(
            &state,
            serde_json::json!({"fields": {"price": {"type": "FIELD_TYPE_KEYWORD", "stored": false}}}),
        );
        assert_eq!(status(&retyped), 200, "{retyped}");
        assert_eq!(
            body(&retyped)["changes"][0]["change"],
            "declarative_changed"
        );

        // Adding a field and changing an analyzer stay online.
        let added = put(
            &state,
            serde_json::json!({"fields": {
                "price": {"type": "FIELD_TYPE_KEYWORD", "stored": false},
                "title": {"analyzer": ["standard@2"]}}}),
        );
        assert_eq!(status(&added), 200, "{added}");
        assert_eq!(body(&added)["changes"][0]["change"], "field_added");

        // An embedding whose dims differ from the stored vectors: 409.
        let refused = put(
            &state,
            serde_json::json!({"fields": {
                "emb": {"embedding": {"model": "m", "dims": 4}}}}),
        );
        assert_eq!(status(&refused), 409, "{refused}");
        let refused = body(&refused);
        assert_eq!(refused["field"], "emb");
        assert_eq!(refused["rule"], "vector_dims_change");
        assert_eq!(refused["reindex_required"], true);
        // Nothing was applied.
        let current = state.indexer.manifest_cloned(&ns).unwrap().schema.unwrap();
        assert!(!current.fields.contains_key("emb"));
    }

    /// Declaring model `a`, dropping the declaration, then declaring model
    /// `b` over the same vectors is refused: the model is read from what the
    /// segments recorded, and the buffered vectors are flushed (recording
    /// `a`) before the declaration changes.
    #[test]
    fn removing_then_re_adding_an_embedding_cannot_change_the_model() {
        let state = state();
        let ns = NamespaceId("acme/docs".into());
        let declare = |model: &str| {
            put(
                &state,
                serde_json::json!({"fields": {
                    "emb": {"embedding": {"model": model, "dims": 3}}}}),
            )
        };
        assert_eq!(status(&declare("a")), 200);
        // Vectors arrive (as if embedded with `a`), still buffered.
        index_vector(&state, &ns, 3);
        let dropped = put(&state, serde_json::json!({"fields": {}}));
        assert_eq!(status(&dropped), 200, "{dropped}");
        let refused = declare("b");
        assert_eq!(status(&refused), 409, "{refused}");
        assert_eq!(body(&refused)["rule"], "embedding_change");
        assert_eq!(status(&declare("a")), 200);
    }
}
