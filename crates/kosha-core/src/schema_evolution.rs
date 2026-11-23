//! Schema evolution rules (#347): which changes to a namespace schema
//! apply online, and which need a reindex.
//!
//! Kosha stores documents with their own field types, and every segment
//! records the analysis and embedding model it was written with (#220,
//! #209). That decides the rules:
//!
//! | Change | On a namespace with documents |
//! | --- | --- |
//! | Add a field (any settings) | online |
//! | Remove a field from the schema | online: new segments use the defaults |
//! | Declare a `type` for a field that had none, or drop the declaration | online |
//! | Change a field's `analyzer` / `stopwords`, `default_analyzer`, `default_stopwords` | online, lazily: new segments use the new chain; existing segments answer with the chain their footer records until compaction re-analyzes them (or a reindex) |
//! | Declare an `embedding` on a field that holds no vectors | online |
//! | Drop a field's `embedding` | online: stored vectors stay; clients send vectors again |
//! | Change a declared `type` | **reindex** (`type_change`) |
//! | Change `indexed` or `stored` | **reindex** (`indexed_change`, `stored_change`): existing documents were written under the old setting |
//! | Change a field's embedding model or `dims` while it holds vectors | **reindex** (`embedding_change`) |
//! | Declare an `embedding` whose `dims` differ from the vectors the field already holds | **reindex** (`vector_dims_change`) |
//!
//! On a namespace without documents every valid schema applies. A refused
//! change is a 409 naming the field and the rule; nothing is applied.
//!
//! **Reindex path.** Create a branch or a new namespace with the new
//! schema (`POST /v1/namespaces` with `schema`), re-ingest the documents
//! into it (an export, #347, gives them back in the index API's own JSON
//! form), switch readers over, then delete the old namespace.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::analysis::{FieldSchema, NamespaceSchema};

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
}

/// A change that applies online.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaChange {
    /// The field, or `*` for the namespace-wide defaults.
    pub field: String,
    /// `field_added`, `field_removed`, `type_declared`, `analyzer_changed`,
    /// `embedding_declared`, `embedding_removed`, `settings_changed`.
    pub change: &'static str,
    /// What happens to existing documents.
    pub effect: &'static str,
}

/// A change that needs a reindex.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaChangeError {
    pub field: String,
    /// `type_change`, `indexed_change`, `stored_change`,
    /// `embedding_change`, `vector_dims_change`.
    pub rule: &'static str,
    pub message: String,
}

const REINDEX: &str = "create a new namespace (or branch) with the new schema, re-ingest the \
     documents (an export returns them in the index API's form), then delete the old namespace";

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

fn effective_flag(flag: Option<bool>) -> bool {
    flag.unwrap_or(true)
}

fn analysis_of(field: Option<&FieldSchema>) -> (Vec<String>, Option<Vec<String>>) {
    field.map_or((Vec::new(), None), |f| {
        (
            f.analyzer.clone(),
            f.stopwords.clone().filter(|w| !w.is_empty()),
        )
    })
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
        let refuse = |rule: &'static str, what: String| -> SchemaChangeError {
            SchemaChangeError {
                field: name.clone(),
                rule,
                message: format!(
                    "field {name:?}: {what} requires a reindex: the namespace already holds \
                     documents written under the current setting ({rule}); {REINDEX}"
                ),
            }
        };
        match (before, after) {
            (None, Some(after)) => {
                if let (Some(spec), Some(Some(dims))) =
                    (&after.embedding, data.vector_fields.get(name))
                {
                    if spec.dims != *dims && data.has_documents {
                        return Err(refuse(
                            "vector_dims_change",
                            format!(
                                "declaring an embedding of {} dims on vectors of {dims} dims",
                                spec.dims
                            ),
                        ));
                    }
                }
                changes.push(SchemaChange {
                    field: name.clone(),
                    change: "field_added",
                    effect: "applies to new segments; existing documents keep their fields",
                });
            }
            (Some(_), None) => changes.push(SchemaChange {
                field: name.clone(),
                change: "field_removed",
                effect: "new segments use the namespace defaults for this field; existing \
                         segments keep what they recorded until compaction rewrites them",
            }),
            (Some(before), Some(after)) => {
                if before == after {
                    continue;
                }
                let (old_type, new_type) = (
                    canonical_type(before.field_type.as_ref()),
                    canonical_type(after.field_type.as_ref()),
                );
                if data.has_documents {
                    if let (Some(from), Some(to)) = (&old_type, &new_type) {
                        if from != to {
                            return Err(refuse(
                                "type_change",
                                format!("changing the type from {from} to {to}"),
                            ));
                        }
                    }
                    if effective_flag(before.indexed) != effective_flag(after.indexed) {
                        return Err(refuse(
                            "indexed_change",
                            format!(
                                "changing indexed from {} to {}",
                                effective_flag(before.indexed),
                                effective_flag(after.indexed)
                            ),
                        ));
                    }
                    if effective_flag(before.stored) != effective_flag(after.stored) {
                        return Err(refuse(
                            "stored_change",
                            format!(
                                "changing stored from {} to {}",
                                effective_flag(before.stored),
                                effective_flag(after.stored)
                            ),
                        ));
                    }
                }
                let holds_vectors = data.vector_fields.contains_key(name);
                match (&before.embedding, &after.embedding) {
                    (Some(from), Some(to)) if !from.same_space(to) => {
                        if holds_vectors && data.has_documents {
                            return Err(refuse(
                                "embedding_change",
                                format!(
                                    "changing the embedding from {} ({} dims) to {} ({} dims)",
                                    from.model, from.dims, to.model, to.dims
                                ),
                            ));
                        }
                    }
                    (None, Some(to)) => {
                        if let Some(Some(dims)) = data.vector_fields.get(name) {
                            if to.dims != *dims && data.has_documents {
                                return Err(refuse(
                                    "vector_dims_change",
                                    format!(
                                        "declaring an embedding of {} dims on vectors of {dims} \
                                         dims",
                                        to.dims
                                    ),
                                ));
                            }
                        }
                        changes.push(SchemaChange {
                            field: name.clone(),
                            change: "embedding_declared",
                            effect: "text sent for the field is embedded from now on; stored \
                                     vectors are unchanged",
                        });
                    }
                    (Some(_), None) => changes.push(SchemaChange {
                        field: name.clone(),
                        change: "embedding_removed",
                        effect: "stored vectors stay; clients send vectors for the field again",
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
                if old_type != new_type {
                    changes.push(SchemaChange {
                        field: name.clone(),
                        change: "type_declared",
                        effect: "declarative only; documents keep the types they were sent with",
                    });
                }
                if changes.last().is_none_or(|c| &c.field != name) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::EmbeddingSpec;

    fn schema(json: serde_json::Value) -> NamespaceSchema {
        serde_json::from_value(json).unwrap()
    }

    fn with_docs() -> ExistingData {
        ExistingData {
            has_documents: true,
            ..ExistingData::default()
        }
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

    #[test]
    fn type_indexed_and_stored_changes_need_a_reindex_only_with_documents() {
        let old = schema(serde_json::json!({"fields": {"price": {"type": "FIELD_TYPE_FLOAT"}}}));
        let new = schema(serde_json::json!({"fields": {"price": {"type": 2}}}));
        let error = check(Some(&old), &new, &with_docs()).unwrap_err();
        assert_eq!((error.field.as_str(), error.rule), ("price", "type_change"));
        assert!(error
            .message
            .contains("FIELD_TYPE_FLOAT to FIELD_TYPE_KEYWORD"));
        assert!(check(Some(&old), &new, &ExistingData::default()).is_ok());
        // Same type spelled differently is no change.
        let same = schema(serde_json::json!({"fields": {"price": {"type": "float"}}}));
        assert!(check(Some(&old), &same, &with_docs()).is_ok());
        // Declaring a type where there was none is online.
        let untyped = schema(serde_json::json!({"fields": {"price": {}}}));
        assert_eq!(
            check(Some(&untyped), &old, &with_docs()).unwrap()[0].change,
            "type_declared"
        );

        let indexed = schema(serde_json::json!({"fields": {"body": {"indexed": false}}}));
        let plain = schema(serde_json::json!({"fields": {"body": {"indexed": true}}}));
        assert_eq!(
            check(Some(&plain), &indexed, &with_docs())
                .unwrap_err()
                .rule,
            "indexed_change"
        );
        let stored = schema(serde_json::json!({"fields": {"body": {"stored": false}}}));
        assert_eq!(
            check(Some(&plain), &stored, &with_docs()).unwrap_err().rule,
            "stored_change"
        );
    }

    #[test]
    fn embedding_rules_follow_the_vectors_already_held() {
        let spec = |model: &str, dims: u32| EmbeddingSpec {
            model: model.into(),
            dims,
            provider: None,
        };
        let mut old = NamespaceSchema::default();
        old.fields.insert(
            "emb".into(),
            FieldSchema {
                embedding: Some(spec("a", 3)),
                ..FieldSchema::default()
            },
        );
        let mut new = old.clone();
        new.fields.get_mut("emb").unwrap().embedding = Some(spec("b", 3));
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
        let mut declared = plain.clone();
        declared.fields.insert(
            "emb".into(),
            FieldSchema {
                embedding: Some(spec("a", 4)),
                ..FieldSchema::default()
            },
        );
        assert_eq!(
            check(Some(&plain), &declared, &data).unwrap_err().rule,
            "vector_dims_change"
        );
        declared.fields.get_mut("emb").unwrap().embedding = Some(spec("a", 3));
        assert!(check(Some(&plain), &declared, &data).is_ok());
        // Dropping the declaration is online.
        assert_eq!(
            check(Some(&old), &plain, &data).unwrap()[0].change,
            "field_removed"
        );
    }
}
