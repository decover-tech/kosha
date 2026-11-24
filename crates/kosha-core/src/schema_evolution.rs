//! Schema evolution rules (#347): which changes to a namespace schema
//! apply online, and which need a reindex.
//!
//! Kosha stores documents with their own field types, and every segment
//! records the analysis and embedding model it was written with (#220,
//! #209). The engine reads a schema's analyzers and embeddings; `type`,
//! `indexed` and `stored` are declarative only (stored, returned, never
//! read when indexing or searching). That decides the rules:
//!
//! | Change | On a namespace with documents |
//! | --- | --- |
//! | Add a field (any settings) | online |
//! | Remove a field from the schema | online: new segments use the defaults |
//! | Change `type`, `indexed` or `stored` | online, reported as `declarative_changed`: the engine does not read them, so existing documents are unchanged |
//! | Change a field's `analyzer` / `stopwords`, `default_analyzer`, `default_stopwords` | online, lazily: new segments use the new chain; existing segments answer with the chain their footer records until compaction re-analyzes them (or a reindex) |
//! | Declare an `embedding` on a field that holds no vectors | online |
//! | Drop a field's `embedding` | online: stored vectors stay; clients send vectors again |
//! | Declare an `embedding` whose model differs from a model the field's stored vectors were embedded with | **reindex** (`embedding_change`) |
//! | Change a field's embedding model or `dims` while it holds vectors | **reindex** (`embedding_change`) |
//! | Declare an `embedding` whose `dims` differ from the vectors the field already holds | **reindex** (`vector_dims_change`) |
//!
//! The embedding rules are judged against what the namespace *holds*
//! (each segment records the model its vectors were embedded with), not
//! only against the previous schema, so removing a declaration and adding
//! a different one in a second request is refused the same way. The HTTP
//! handler flushes buffered documents before an embedding declaration
//! changes, so their model is recorded first.
//!
//! On a namespace without documents every valid schema applies. A refused
//! change is a 409 naming the field and the rule; nothing is applied.
//!
//! **Reindex path.** Create a new namespace with the new schema
//! (`POST /v1/namespaces` with `schema`), re-ingest the documents into it
//! (an export, #347, gives them back in the index API's own JSON form),
//! switch readers over, then delete the old namespace.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::analysis::{EmbeddingSpec, FieldSchema, NamespaceSchema};

/// Proto `FieldType` names (`proto/kosha/v1/kosha.proto`), by enum number.
pub const FIELD_TYPE_NAMES: [&str; 8] = [
    "FIELD_TYPE_UNSPECIFIED",
    "FIELD_TYPE_TEXT",
    "FIELD_TYPE_KEYWORD",
    "FIELD_TYPE_INTEGER",
    "FIELD_TYPE_FLOAT",
    "FIELD_TYPE_DATE",
    "FIELD_TYPE_BOOLEAN",
    "FIELD_TYPE_VECTOR",
];

/// What the namespace already holds, as far as the rules need it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExistingData {
    /// Any document in a segment or buffered.
    pub has_documents: bool,
    /// Vector fields with indexed (or buffered) vectors, with their
    /// dimension when known.
    pub vector_fields: BTreeMap<String, Option<u32>>,
    /// Per vector field, the embedding models its stored vectors were
    /// embedded with (recorded in segment footers). Vectors a client sent
    /// itself record none.
    pub vector_models: BTreeMap<String, BTreeSet<String>>,
}

/// A change that applies online.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaChange {
    /// The field, or `*` for the namespace-wide defaults.
    pub field: String,
    /// `field_added`, `field_removed`, `declarative_changed`,
    /// `analyzer_changed`, `embedding_declared`, `embedding_removed`,
    /// `settings_changed`.
    pub change: &'static str,
    /// What happens to existing documents.
    pub effect: &'static str,
}

/// A change that needs a reindex.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaChangeError {
    pub field: String,
    /// `embedding_change` or `vector_dims_change`.
    pub rule: &'static str,
    pub message: String,
}

const REINDEX: &str = "create a new namespace with the new schema, re-ingest the documents (an \
     export returns them in the index API's form), then delete the old namespace";

const DECLARATIVE: &str = "declarative only: the engine does not read type, indexed or stored, \
     so existing and new documents are indexed and stored as before";

/// A field type as its canonical `FIELD_TYPE_*` name (case-insensitive;
/// numbers are proto enum values). `None` when absent or unspecified.
pub fn canonical_type(value: Option<&serde_json::Value>) -> Option<String> {
    let name = match value? {
        serde_json::Value::Number(n) => FIELD_TYPE_NAMES.get(n.as_u64()? as usize)?.to_string(),
        serde_json::Value::String(s) => {
            let upper = s.trim().to_ascii_uppercase();
            if upper.starts_with("FIELD_TYPE_") {
                upper
            } else {
                format!("FIELD_TYPE_{upper}")
            }
        }
        _ => return None,
    };
    (name != FIELD_TYPE_NAMES[0]).then_some(name)
}

fn declarative_of(field: &FieldSchema) -> (Option<String>, bool, bool) {
    (
        canonical_type(field.field_type.as_ref()),
        field.indexed.unwrap_or(true),
        field.stored.unwrap_or(true),
    )
}

fn analysis_of(field: Option<&FieldSchema>) -> (Vec<String>, Option<Vec<String>>) {
    field.map_or((Vec::new(), None), |f| {
        (
            f.analyzer.clone(),
            f.stopwords.clone().filter(|w| !w.is_empty()),
        )
    })
}

fn refuse(field: &str, rule: &'static str, what: String) -> SchemaChangeError {
    SchemaChangeError {
        field: field.to_string(),
        rule,
        message: format!(
            "field {field:?}: {what} requires a reindex: the namespace already holds vectors \
             for it ({rule}); {REINDEX}"
        ),
    }
}

/// Whether declaring `to` on `field` is compatible with what is stored.
fn check_embedding(
    field: &str,
    from: Option<&EmbeddingSpec>,
    to: &EmbeddingSpec,
    data: &ExistingData,
) -> Result<(), SchemaChangeError> {
    if !data.has_documents {
        return Ok(());
    }
    if let Some(Some(dims)) = data.vector_fields.get(field) {
        if *dims != to.dims {
            return Err(refuse(
                field,
                "vector_dims_change",
                format!(
                    "declaring an embedding of {} dims on vectors of {dims} dims",
                    to.dims
                ),
            ));
        }
    }
    if let Some(model) = data
        .vector_models
        .get(field)
        .and_then(|models| models.iter().find(|m| **m != to.model))
    {
        return Err(refuse(
            field,
            "embedding_change",
            format!(
                "declaring model {} ({} dims) over vectors embedded with {model}",
                to.model, to.dims
            ),
        ));
    }
    if let Some(from) = from {
        if !from.same_space(to) && data.vector_fields.contains_key(field) {
            return Err(refuse(
                field,
                "embedding_change",
                format!(
                    "changing the embedding from {} ({} dims) to {} ({} dims)",
                    from.model, from.dims, to.model, to.dims
                ),
            ));
        }
    }
    Ok(())
}

/// Check moving a namespace from `old` to `new` against the rules (see the
/// module doc). `Ok` lists the online changes (empty when nothing
/// changes); `Err` is the first change that needs a reindex.
pub fn check(
    old: Option<&NamespaceSchema>,
    new: &NamespaceSchema,
    data: &ExistingData,
) -> Result<Vec<SchemaChange>, SchemaChangeError> {
    let empty = NamespaceSchema::default();
    let old = old.unwrap_or(&empty);
    let mut changes = Vec::new();
    let names: BTreeSet<&String> = old.fields.keys().chain(new.fields.keys()).collect();
    for name in names {
        let before = old.fields.get(name);
        let after = new.fields.get(name);
        // The embedding rules first, on every path: they are judged against
        // the stored vectors, whatever the previous schema said.
        if let Some(to) = after.and_then(|f| f.embedding.as_ref()) {
            let from = before.and_then(|f| f.embedding.as_ref());
            if from.is_none_or(|from| !from.same_space(to)) {
                check_embedding(name, from, to, data)?;
            }
        }
        match (before, after) {
            (None, Some(_)) => changes.push(SchemaChange {
                field: name.clone(),
                change: "field_added",
                effect: "applies to new segments; existing documents keep their fields",
            }),
            (Some(_), None) => changes.push(SchemaChange {
                field: name.clone(),
                change: "field_removed",
                effect: "new segments use the namespace defaults for this field; existing \
                         segments keep what they recorded until compaction rewrites them",
            }),
            (Some(before), Some(after)) => {
                // Equal, or equal once spelled the same way (`"float"` and
                // `"FIELD_TYPE_FLOAT"`, an absent flag and `true`).
                let same = before.embedding == after.embedding
                    && analysis_of(Some(before)) == analysis_of(Some(after))
                    && declarative_of(before) == declarative_of(after);
                if before == after || same {
                    continue;
                }
                let at = changes.len();
                match (&before.embedding, &after.embedding) {
                    (None, Some(_)) => changes.push(SchemaChange {
                        field: name.clone(),
                        change: "embedding_declared",
                        effect: "text sent for the field is embedded from now on; stored \
                                 vectors are unchanged",
                    }),
                    (Some(_), None) => changes.push(SchemaChange {
                        field: name.clone(),
                        change: "embedding_removed",
                        effect: "stored vectors stay; clients send vectors for the field again",
                    }),
                    (Some(from), Some(to)) if !from.same_space(to) => changes.push(SchemaChange {
                        field: name.clone(),
                        change: "embedding_declared",
                        effect: "the field holds no vectors yet; new text is embedded with \
                                 the new model",
                    }),
                    _ => {}
                }
                if analysis_of(Some(before)) != analysis_of(Some(after)) {
                    changes.push(SchemaChange {
                        field: name.clone(),
                        change: "analyzer_changed",
                        effect: "new segments use the new chain; existing segments answer \
                                 with the chain they recorded until compaction re-analyzes \
                                 them (POST /v1/admin/compact-namespace) or a reindex",
                    });
                }
                if declarative_of(before) != declarative_of(after) {
                    changes.push(SchemaChange {
                        field: name.clone(),
                        change: "declarative_changed",
                        effect: DECLARATIVE,
                    });
                }
                if changes.len() == at {
                    changes.push(SchemaChange {
                        field: name.clone(),
                        change: "settings_changed",
                        effect: "applies to new segments",
                    });
                }
            }
            (None, None) => {}
        }
    }
    let old_default = (&old.default_analyzer, &old.default_stopwords);
    let new_default = (&new.default_analyzer, &new.default_stopwords);
    if old_default != new_default {
        changes.push(SchemaChange {
            field: "*".into(),
            change: "analyzer_changed",
            effect: "new segments use the new default chain; existing segments answer with \
                     the chain they recorded until compaction re-analyzes them \
                     (POST /v1/admin/compact-namespace) or a reindex",
        });
    }
    Ok(changes)
}

/// Whether moving from `old` to `new` changes any field's embedding
/// declaration (the HTTP handler flushes buffered documents first, so
/// their model is recorded before the rules judge the change).
pub fn changes_embeddings(old: Option<&NamespaceSchema>, new: &NamespaceSchema) -> bool {
    let empty = NamespaceSchema::default();
    let old = old.unwrap_or(&empty);
    let declared = |schema: &NamespaceSchema| -> BTreeMap<String, EmbeddingSpec> {
        schema
            .embedded_fields()
            .map(|(name, spec)| (name.clone(), spec.clone()))
            .collect()
    };
    declared(old) != declared(new)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(json: serde_json::Value) -> NamespaceSchema {
        serde_json::from_value(json).unwrap()
    }

    fn with_docs() -> ExistingData {
        ExistingData {
            has_documents: true,
            ..ExistingData::default()
        }
    }

    fn spec(model: &str, dims: u32) -> EmbeddingSpec {
        EmbeddingSpec {
            model: model.into(),
            dims,
            provider: None,
        }
    }

    fn embedded(model: &str, dims: u32) -> NamespaceSchema {
        let mut schema = NamespaceSchema::default();
        schema.fields.insert(
            "emb".into(),
            FieldSchema {
                embedding: Some(spec(model, dims)),
                ..FieldSchema::default()
            },
        );
        schema
    }

    #[test]
    fn additions_removals_and_analyzer_changes_are_online() {
        let old = schema(serde_json::json!({"fields": {"title": {"analyzer": ["standard"]}}}));
        let new = schema(serde_json::json!({
            "fields": {"title": {"analyzer": ["standard@2", "stem"]}, "sku": {"type": "keyword"}},
            "default_analyzer": ["standard@2"]
        }));
        let changes = check(Some(&old), &new, &with_docs()).unwrap();
        let kinds: Vec<(&str, &str)> = changes
            .iter()
            .map(|c| (c.field.as_str(), c.change))
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("sku", "field_added"),
                ("title", "analyzer_changed"),
                ("*", "analyzer_changed")
            ]
        );
        assert_eq!(
            check(Some(&new), &old, &with_docs()).unwrap()[0].change,
            "field_removed"
        );
        assert!(check(Some(&old), &old, &with_docs()).unwrap().is_empty());
    }

    /// `type`, `indexed` and `stored` are not read by the engine: changing
    /// them is online and reported as declarative (#347 review).
    #[test]
    fn declarative_settings_change_online_and_are_reported_as_such() {
        let old = schema(serde_json::json!({"fields": {"price": {"type": "FIELD_TYPE_FLOAT"}}}));
        for new in [
            serde_json::json!({"fields": {"price": {"type": 2}}}),
            serde_json::json!({"fields": {"price": {"type": "float", "indexed": false}}}),
            serde_json::json!({"fields": {"price": {"type": "float", "stored": false}}}),
            serde_json::json!({"fields": {"price": {}}}),
        ] {
            let changes = check(Some(&old), &schema(new.clone()), &with_docs()).unwrap();
            assert_eq!(changes.len(), 1, "{new}");
            assert_eq!(changes[0].change, "declarative_changed", "{new}");
        }
        // The same type spelled differently is no change.
        let same = schema(serde_json::json!({"fields": {"price": {"type": "float"}}}));
        assert!(check(Some(&old), &same, &with_docs()).unwrap().is_empty());
    }

    #[test]
    fn embedding_rules_follow_the_vectors_already_held() {
        let old = embedded("a", 3);
        let new = embedded("b", 3);
        let mut data = with_docs();
        // No vectors yet: a model change is fine.
        assert!(check(Some(&old), &new, &data).is_ok());
        data.vector_fields.insert("emb".into(), Some(3));
        assert_eq!(
            check(Some(&old), &new, &data).unwrap_err().rule,
            "embedding_change"
        );
        // Declaring an embedding over client vectors of another dimension.
        let plain = NamespaceSchema::default();
        assert_eq!(
            check(Some(&plain), &embedded("a", 4), &data)
                .unwrap_err()
                .rule,
            "vector_dims_change"
        );
        assert!(check(Some(&plain), &embedded("a", 3), &data).is_ok());
        // Dropping the declaration is online.
        assert_eq!(
            check(Some(&old), &plain, &data).unwrap()[0].change,
            "field_removed"
        );
    }

    /// Removing a declaration and adding a different model in a second
    /// request is judged against the recorded models, so it is refused too.
    #[test]
    fn remove_then_re_add_cannot_change_the_model_of_stored_vectors() {
        let mut data = with_docs();
        data.vector_fields.insert("emb".into(), Some(3));
        data.vector_models
            .insert("emb".into(), BTreeSet::from(["a".to_string()]));
        let plain = NamespaceSchema::default();
        // Step one: drop the declaration (online).
        assert!(check(Some(&embedded("a", 3)), &plain, &data).is_ok());
        // Step two: a different model on the same vectors.
        let refused = check(Some(&plain), &embedded("b", 3), &data).unwrap_err();
        assert_eq!(
            (refused.field.as_str(), refused.rule),
            ("emb", "embedding_change")
        );
        // The model the vectors were embedded with is fine again.
        assert!(check(Some(&plain), &embedded("a", 3), &data).is_ok());
        // Removing the whole field entry and re-adding it is the same.
        let mut typed = embedded("b", 3);
        typed.fields.get_mut("emb").unwrap().field_type = Some(serde_json::json!("vector"));
        assert!(check(Some(&plain), &typed, &data).is_err());
    }

    #[test]
    fn embedding_declarations_are_detected() {
        assert!(changes_embeddings(None, &embedded("a", 3)));
        assert!(changes_embeddings(
            Some(&embedded("a", 3)),
            &embedded("b", 3)
        ));
        assert!(!changes_embeddings(
            Some(&embedded("a", 3)),
            &embedded("a", 3)
        ));
    }
}
