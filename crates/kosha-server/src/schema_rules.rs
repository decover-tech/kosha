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

    #[test]
    fn schema_route_applies_online_changes_and_refuses_reindex_ones() {
        let state = state();
        let ns = NamespaceId("acme/docs".into());
        // Empty namespace: any valid schema applies.
        let first = put(
            &state,
            serde_json::json!({"fields": {"price": {"type": "FIELD_TYPE_FLOAT"}}}),
        );
        assert_eq!(status(&first), 200, "{first}");
        // Changing the declared type of an empty namespace is still fine.
        let retyped = put(
            &state,
            serde_json::json!({"fields": {"price": {"type": "FIELD_TYPE_INTEGER"}}}),
        );
        assert_eq!(status(&retyped), 200, "{retyped}");

        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![kosha_core::Document {
                    id: kosha_core::DocumentId("d1".into()),
                    fields: vec![kosha_core::Field::text("title", "hello")],
                }],
            )
            .unwrap();

        // With a document: a type change is a 409 naming field and rule.
        let refused = put(
            &state,
            serde_json::json!({"fields": {"price": {"type": "FIELD_TYPE_KEYWORD"}}}),
        );
        assert_eq!(status(&refused), 409, "{refused}");
        let refused = body(&refused);
        assert_eq!(refused["field"], "price");
        assert_eq!(refused["rule"], "type_change");
        assert_eq!(refused["reindex_required"], true);
        // Nothing was applied.
        let current = state.indexer.manifest_cloned(&ns).unwrap().schema.unwrap();
        assert_eq!(
            current.fields["price"].field_type,
            Some(serde_json::json!("FIELD_TYPE_INTEGER"))
        );

        // Adding a field and changing an analyzer stay online.
        let added = put(
            &state,
            serde_json::json!({"fields": {
                "price": {"type": "FIELD_TYPE_INTEGER"},
                "title": {"analyzer": ["standard@2"]}}}),
        );
        assert_eq!(status(&added), 200, "{added}");
        let changes = body(&added)["changes"].clone();
        assert_eq!(changes[0]["field"], "title");
        assert_eq!(changes[0]["change"], "field_added");

        let stored = put(
            &state,
            serde_json::json!({"fields": {
                "price": {"type": "FIELD_TYPE_INTEGER", "stored": false},
                "title": {"analyzer": ["standard@2"]}}}),
        );
        assert_eq!(status(&stored), 409, "{stored}");
        assert_eq!(body(&stored)["rule"], "stored_change");
    }
}
