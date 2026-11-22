//! Schema evolution rules (#347): which namespace schema changes apply
//! online and which need the namespace's documents re-indexed.
//!
//! Segments record the analysis they were written with, so a schema edit
//! never changes how an existing segment is searched. Some edits are still
//! unsafe once a namespace holds documents, because old and new documents
//! would disagree about a field: the same text analyzed two ways, a field
//! that is a keyword in one segment and an integer in the next, or vectors
//! of two dimensions in one column. Those are refused with
//! [`SchemaChangeError`] (HTTP 409, `requires_reindex`) naming the field.
//!
//! Allowed online, documents or not:
//! - adding a field;
//! - removing a field (documents already indexed keep it in their
//!   segments; new documents are analyzed with the default chain);
//! - an embedding model change is not judged here: #209 model pinning
//!   (`kosha_write::Indexer::embedding_conflict`) already refuses it while
//!   vectors of the old model are indexed.
//!
//! Allowed only while the namespace holds no documents:
//! - changing a field's type (declaring a type for a field that had none
//!   is not a change);
//! - changing a field's analyzer or stopwords, or the namespace's
//!   `default_analyzer` / `default_stopwords`;
//! - changing a vector field's `embedding.dims`;
//! - changing a field's `indexed` / `stored` flags (the schema's
//!   filterable/sortable switches).
//!
//! See `docs/schema-evolution.md`.

use std::fmt;

use serde::Serialize;

use crate::analysis::{FieldSchema, NamespaceSchema};

/// The name a schema error reports for the namespace-wide default
/// analyzer (`default_analyzer` / `default_stopwords`).
pub const DEFAULT_ANALYZER_FIELD: &str = "default_analyzer";

/// What one accepted schema change does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    FieldAdded,
    FieldRemoved,
    TypeChanged,
    AnalyzerChanged,
    VectorDimsChanged,
    FlagsChanged,
    EmbeddingChanged,
    DefaultAnalyzerChanged,
}

/// One accepted change, by field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Change {
    pub field: String,
    pub kind: ChangeKind,
}

/// Why a change needs a reindex.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReindexReason {
    TypeChange { from: String, to: String },
    AnalyzerChange,
    VectorDimsChange { from: u32, to: u32 },
    FlagsChange,
}

/// A schema change refused while the namespace holds documents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaChangeError {
    /// The field the change touches ([`DEFAULT_ANALYZER_FIELD`] for the
    /// namespace-wide default analyzer).
    pub field: String,
    pub reason: ReindexReason,
}

impl SchemaChangeError {
    /// Machine-readable error code clients branch on.
    pub const CODE: &'static str = "requires_reindex";
}

impl fmt::Display for SchemaChangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match &self.reason {
            ReindexReason::TypeChange { from, to } => {
                format!("changing its type from {from} to {to}")
            }
            ReindexReason::AnalyzerChange => "changing its analyzer".to_string(),
            ReindexReason::VectorDimsChange { from, to } => {
                format!("changing its vector dimension from {from} to {to}")
            }
            ReindexReason::FlagsChange => "changing its indexed/stored flags".to_string(),
        };
        write!(
            f,
            "requires_reindex: field {:?}: {what} requires a reindex because the namespace \
             holds documents (delete and re-ingest them, or use a new field)",
            self.field
        )
    }
}

impl std::error::Error for SchemaChangeError {}

/// Proto `FieldType` names by enum number, without the `FIELD_TYPE_` prefix.
const FIELD_TYPES: [&str; 8] = [
    "UNSPECIFIED",
    "TEXT",
    "KEYWORD",
    "INTEGER",
    "FLOAT",
    "DATE",
    "BOOLEAN",
    "VECTOR",
];

/// A field's declared type, normalized: `"text"`, `"FIELD_TYPE_TEXT"` and
/// `1` are all `TEXT`. `None` when undeclared or unspecified. An unknown
/// spelling is kept as written (upper-cased), so two different unknown
/// values still compare unequal.
fn normalized_type(field: &FieldSchema) -> Option<String> {
    let name = match field.field_type.as_ref()? {
        serde_json::Value::Number(n) => FIELD_TYPES.get(n.as_u64()? as usize)?.to_string(),
        serde_json::Value::String(s) => {
            let upper = s.trim().to_ascii_uppercase();
            upper
                .strip_prefix("FIELD_TYPE_")
                .unwrap_or(&upper)
                .to_string()
        }
        serde_json::Value::Null => return None,
        other => other.to_string(),
    };
    (name != "UNSPECIFIED").then_some(name)
}

fn analysis_of(field: &FieldSchema) -> (&[String], Option<&Vec<String>>) {
    (&field.analyzer, field.stopwords.as_ref())
}

fn flags_of(field: &FieldSchema) -> (bool, bool) {
    (
        field.indexed.unwrap_or(false),
        field.stored.unwrap_or(false),
    )
}

/// Judge the change from `old` to `new`. `has_docs`: whether the namespace
/// (any of its shards) holds documents, indexed or buffered. `Ok` lists
/// the changes (empty for an identical schema); `Err` names the first
/// field whose change requires a reindex.
pub fn check_schema_change(
    old: &NamespaceSchema,
    new: &NamespaceSchema,
    has_docs: bool,
) -> Result<Vec<Change>, SchemaChangeError> {
    let mut changes = Vec::new();
    let refuse = |field: &str, reason: ReindexReason| SchemaChangeError {
        field: field.to_string(),
        reason,
    };
    let mut note = |field: &str, kind: ChangeKind| {
        changes.push(Change {
            field: field.to_string(),
            kind,
        })
    };

    if old.default_analyzer != new.default_analyzer
        || old.default_stopwords != new.default_stopwords
    {
        if has_docs {
            return Err(refuse(
                DEFAULT_ANALYZER_FIELD,
                ReindexReason::AnalyzerChange,
            ));
        }
        note(DEFAULT_ANALYZER_FIELD, ChangeKind::DefaultAnalyzerChanged);
    }

    for (name, before) in &old.fields {
        let Some(after) = new.fields.get(name) else {
            note(name, ChangeKind::FieldRemoved);
            continue;
        };
        let (from, to) = (normalized_type(before), normalized_type(after));
        if let (Some(from), Some(to)) = (&from, &to) {
            if from != to {
                if has_docs {
                    return Err(refuse(
                        name,
                        ReindexReason::TypeChange {
                            from: from.clone(),
                            to: to.clone(),
                        },
                    ));
                }
                note(name, ChangeKind::TypeChanged);
            }
        } else if from != to {
            // Declaring a type for an untyped field (or dropping the
            // declaration) changes nothing about the data already indexed.
            note(name, ChangeKind::TypeChanged);
        }
        if analysis_of(before) != analysis_of(after) {
            if has_docs {
                return Err(refuse(name, ReindexReason::AnalyzerChange));
            }
            note(name, ChangeKind::AnalyzerChanged);
        }
        match (&before.embedding, &after.embedding) {
            (Some(b), Some(a)) if b.dims != a.dims => {
                if has_docs {
                    return Err(refuse(
                        name,
                        ReindexReason::VectorDimsChange {
                            from: b.dims,
                            to: a.dims,
                        },
                    ));
                }
                note(name, ChangeKind::VectorDimsChanged);
            }
            (b, a) if b != a => note(name, ChangeKind::EmbeddingChanged),
            _ => {}
        }
        if flags_of(before) != flags_of(after) {
            if has_docs {
                return Err(refuse(name, ReindexReason::FlagsChange));
            }
            note(name, ChangeKind::FlagsChanged);
        }
    }
    for name in new.fields.keys() {
        if !old.fields.contains_key(name) {
            note(name, ChangeKind::FieldAdded);
        }
    }
    Ok(changes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::EmbeddingSpec;

    fn field(ty: &str) -> FieldSchema {
        FieldSchema {
            field_type: Some(serde_json::json!(ty)),
            ..Default::default()
        }
    }

    fn schema(fields: &[(&str, FieldSchema)]) -> NamespaceSchema {
        NamespaceSchema {
            fields: fields
                .iter()
                .map(|(n, f)| (n.to_string(), f.clone()))
                .collect(),
            ..Default::default()
        }
    }

    fn analyzed(ty: &str, analyzer: &[&str]) -> FieldSchema {
        FieldSchema {
            analyzer: analyzer.iter().map(|s| s.to_string()).collect(),
            ..field(ty)
        }
    }

    fn vector(dims: u32) -> FieldSchema {
        FieldSchema {
            embedding: Some(EmbeddingSpec {
                model: "m".into(),
                dims,
                provider: None,
            }),
            ..field("vector")
        }
    }

    fn flagged(ty: &str, indexed: bool, stored: bool) -> FieldSchema {
        FieldSchema {
            indexed: Some(indexed),
            stored: Some(stored),
            ..field(ty)
        }
    }

    enum Expect {
        Ok(Vec<(&'static str, ChangeKind)>),
        Reindex(&'static str),
    }

    #[test]
    fn schema_change_rules_table() {
        let base = schema(&[("title", analyzed("text", &["standard"]))]);
        let with_docs_default = NamespaceSchema {
            default_analyzer: vec!["standard".into(), "stem".into()],
            ..base.clone()
        };
        let cases: Vec<(&str, NamespaceSchema, NamespaceSchema, bool, Expect)> = vec![
            (
                "identical schema",
                base.clone(),
                base.clone(),
                true,
                Expect::Ok(vec![]),
            ),
            (
                "add a field with documents",
                base.clone(),
                schema(&[
                    ("title", analyzed("text", &["standard"])),
                    ("tag", field("keyword")),
                ]),
                true,
                Expect::Ok(vec![("tag", ChangeKind::FieldAdded)]),
            ),
            (
                "remove a field with documents",
                base.clone(),
                schema(&[]),
                true,
                Expect::Ok(vec![("title", ChangeKind::FieldRemoved)]),
            ),
            (
                "type change with documents",
                base.clone(),
                schema(&[("title", analyzed("keyword", &["standard"]))]),
                true,
                Expect::Reindex("title"),
            ),
            (
                "type change without documents",
                base.clone(),
                schema(&[("title", analyzed("keyword", &["standard"]))]),
                false,
                Expect::Ok(vec![("title", ChangeKind::TypeChanged)]),
            ),
            (
                "same type, different spelling",
                base.clone(),
                schema(&[("title", analyzed("FIELD_TYPE_TEXT", &["standard"]))]),
                true,
                Expect::Ok(vec![]),
            ),
            (
                "same type by enum number",
                schema(&[("n", field("integer"))]),
                schema(&[(
                    "n",
                    FieldSchema {
                        field_type: Some(serde_json::json!(3)),
                        ..Default::default()
                    },
                )]),
                true,
                Expect::Ok(vec![]),
            ),
            (
                "declaring a type for an untyped field",
                schema(&[("n", FieldSchema::default())]),
                schema(&[("n", field("integer"))]),
                true,
                Expect::Ok(vec![("n", ChangeKind::TypeChanged)]),
            ),
            (
                "analyzer change with documents",
                base.clone(),
                schema(&[("title", analyzed("text", &["standard", "stem"]))]),
                true,
                Expect::Reindex("title"),
            ),
            (
                "analyzer change without documents",
                base.clone(),
                schema(&[("title", analyzed("text", &["standard", "stem"]))]),
                false,
                Expect::Ok(vec![("title", ChangeKind::AnalyzerChanged)]),
            ),
            (
                "stopwords change with documents",
                base.clone(),
                schema(&[(
                    "title",
                    FieldSchema {
                        stopwords: Some(vec!["a".into()]),
                        ..analyzed("text", &["standard"])
                    },
                )]),
                true,
                Expect::Reindex("title"),
            ),
            (
                "default analyzer change with documents",
                base.clone(),
                with_docs_default.clone(),
                true,
                Expect::Reindex(DEFAULT_ANALYZER_FIELD),
            ),
            (
                "default analyzer change without documents",
                base.clone(),
                with_docs_default,
                false,
                Expect::Ok(vec![(
                    DEFAULT_ANALYZER_FIELD,
                    ChangeKind::DefaultAnalyzerChanged,
                )]),
            ),
            (
                "vector dims change with documents",
                schema(&[("emb", vector(8))]),
                schema(&[("emb", vector(16))]),
                true,
                Expect::Reindex("emb"),
            ),
            (
                "vector dims change without documents",
                schema(&[("emb", vector(8))]),
                schema(&[("emb", vector(16))]),
                false,
                Expect::Ok(vec![("emb", ChangeKind::VectorDimsChanged)]),
            ),
            (
                "embedding model change is left to #209 pinning",
                schema(&[("emb", vector(8))]),
                schema(&[(
                    "emb",
                    FieldSchema {
                        embedding: Some(EmbeddingSpec {
                            model: "other".into(),
                            dims: 8,
                            provider: None,
                        }),
                        ..field("vector")
                    },
                )]),
                true,
                Expect::Ok(vec![("emb", ChangeKind::EmbeddingChanged)]),
            ),
            (
                "flags change without documents",
                schema(&[("tag", flagged("keyword", false, false))]),
                schema(&[("tag", flagged("keyword", true, true))]),
                false,
                Expect::Ok(vec![("tag", ChangeKind::FlagsChanged)]),
            ),
            (
                "flags change with documents",
                schema(&[("tag", flagged("keyword", false, false))]),
                schema(&[("tag", flagged("keyword", true, false))]),
                true,
                Expect::Reindex("tag"),
            ),
            (
                "unset flags equal false",
                schema(&[("tag", field("keyword"))]),
                schema(&[("tag", flagged("keyword", false, false))]),
                true,
                Expect::Ok(vec![]),
            ),
        ];
        for (name, old, new, has_docs, expect) in cases {
            let got = check_schema_change(&old, &new, has_docs);
            match expect {
                Expect::Ok(want) => {
                    let want: Vec<Change> = want
                        .into_iter()
                        .map(|(f, kind)| Change {
                            field: f.into(),
                            kind,
                        })
                        .collect();
                    assert_eq!(got, Ok(want), "{name}");
                }
                Expect::Reindex(field) => {
                    let err = got.expect_err(name);
                    assert_eq!(err.field, field, "{name}");
                    let message = err.to_string();
                    assert!(message.contains("requires_reindex"), "{name}: {message}");
                    assert!(message.contains(field), "{name}: {message}");
                }
            }
        }
    }

    #[test]
    fn errors_name_the_types_and_dimensions() {
        let err = check_schema_change(
            &schema(&[("n", field("integer"))]),
            &schema(&[("n", field("text"))]),
            true,
        )
        .unwrap_err();
        assert_eq!(
            err.reason,
            ReindexReason::TypeChange {
                from: "INTEGER".into(),
                to: "TEXT".into()
            }
        );
        let err = check_schema_change(
            &schema(&[("v", vector(4))]),
            &schema(&[("v", vector(2))]),
            true,
        )
        .unwrap_err();
        assert!(err.to_string().contains("from 4 to 2"), "{err}");
    }
}
