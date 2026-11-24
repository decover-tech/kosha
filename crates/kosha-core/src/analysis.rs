//! Analyzer configuration: the namespace schema's `analyzer` setting
//! (issue #220) and the per-segment record of how a segment's text was
//! analyzed (issues #220, #221).
//!
//! The implementations live in `kosha_segment::analysis`; this module holds
//! only the serializable descriptions, because the segment [`Footer`]
//! records them and every crate reads footers.
//!
//! **Why a segment records its analysis.** The inverted index holds
//! whatever terms the analyzer produced when the segment was written. A
//! query only matches if its text is analyzed the same way, so a change
//! to an analyzer — a new tokenizer version, a stemmer, a schema edit —
//! would silently make older segments return zero hits for the affected
//! terms. Instead each segment records the exact chains it used
//! ([`SegmentAnalysis`]), and the query is analyzed *per segment* with that
//! segment's chains. Old segments keep answering with the analysis they
//! were written with until compaction rewrites them under the current
//! schema.
//!
//! [`Footer`]: crate::Footer

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Built-in English stopword list (Lucene's `EnglishAnalyzer` default set).
///
/// Resolved specs copy the list into the footer instead of referring to
/// it by name, so changing this list later can never change what an
/// existing segment means.
pub const ENGLISH_STOPWORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with",
];

/// The legacy `standard` tokenizer: split on whitespace, trim ASCII
/// punctuation from each word's edges. Every segment written before
/// analyzers were versioned used it.
pub const STANDARD_TOKENIZER_V1: u32 = 1;
/// Unicode `standard` tokenizer: UAX #29 word boundaries (issue #221).
pub const STANDARD_TOKENIZER_V2: u32 = 2;
/// What a bare `standard` (or no analyzer at all) resolves to. Kept at the
/// legacy version so default segments stay byte-identical and an older
/// binary reading them sees exactly what it wrote; the Unicode tokenizer
/// is opt-in via `standard@2`. See README "Analyzers".
pub const DEFAULT_STANDARD_TOKENIZER_VERSION: u32 = STANDARD_TOKENIZER_V1;

/// How text is split into tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenizerKind {
    /// Word tokenizer; behavior depends on [`AnalyzerSpec::version`].
    Standard,
    /// Split on Unicode whitespace only; tokens keep their punctuation.
    Whitespace,
    /// The whole value is one token, untouched.
    Keyword,
}

/// One token filter, applied in order after the tokenizer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TokenFilterSpec {
    /// Unicode lowercase (`str::to_lowercase`).
    Lowercase,
    /// Drop these tokens. A dropped token still consumes a position, so
    /// a phrase query keeps the gap (Lucene's position-increment
    /// semantics) and a phrase spanning a removed stopword still matches.
    Stop { words: Vec<String> },
    /// Snowball stemmer (`rust-stemmers`). Only `english` exists today.
    Stem { language: String },
}

/// A fully resolved analysis chain: everything needed to reproduce a
/// segment's terms, with nothing looked up by name at query time.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AnalyzerSpec {
    pub tokenizer: TokenizerKind,
    /// Tokenizer version. Only meaningful for [`TokenizerKind::Standard`]
    /// (`1` legacy, `2` UAX #29); `0` for the others.
    #[serde(default)]
    pub version: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filters: Vec<TokenFilterSpec>,
}

impl AnalyzerSpec {
    /// The chain every pre-#220 segment was written with.
    pub fn legacy() -> Self {
        Self::standard(STANDARD_TOKENIZER_V1)
    }

    fn standard(version: u32) -> Self {
        Self {
            tokenizer: TokenizerKind::Standard,
            version,
            filters: vec![TokenFilterSpec::Lowercase],
        }
    }

    pub fn is_legacy(&self) -> bool {
        // Field by field rather than `== Self::legacy()`: no allocation, and
        // it runs per segment on the query hot path.
        self.tokenizer == TokenizerKind::Standard
            && self.version == STANDARD_TOKENIZER_V1
            && self.filters == [TokenFilterSpec::Lowercase]
    }

    /// Resolve a schema `analyzer` list: the first name picks the analyzer
    /// (`standard`, `standard@1`, `standard@2`, `keyword`, `lowercase`,
    /// `whitespace`), every later name appends a filter (`lowercase`,
    /// `stop`, `stem`). An empty list is `standard`. `stopwords`, when
    /// given, replaces the built-in English list for `stop`.
    pub fn resolve(names: &[String], stopwords: Option<&[String]>) -> Result<Self, String> {
        // Proto `repeated` fields cannot be absent, only empty.
        let stopwords = stopwords.filter(|words| !words.is_empty());
        let mut names = names.iter().map(|n| n.trim());
        let mut spec = match names.next() {
            None | Some("standard") => Self::standard(DEFAULT_STANDARD_TOKENIZER_VERSION),
            Some("standard@1") => Self::standard(STANDARD_TOKENIZER_V1),
            Some("standard@2") => Self::standard(STANDARD_TOKENIZER_V2),
            Some("keyword") => Self {
                tokenizer: TokenizerKind::Keyword,
                version: 0,
                filters: Vec::new(),
            },
            // Whole value, lowercased: case-insensitive exact match.
            Some("lowercase") => Self {
                tokenizer: TokenizerKind::Keyword,
                version: 0,
                filters: vec![TokenFilterSpec::Lowercase],
            },
            // Whitespace split, case and punctuation preserved.
            Some("whitespace") => Self {
                tokenizer: TokenizerKind::Whitespace,
                version: 0,
                filters: Vec::new(),
            },
            Some(other) => {
                return Err(format!(
                    "unknown analyzer {other:?}; expected one of standard, standard@1, \
                     standard@2, keyword, lowercase, whitespace"
                ))
            }
        };
        for name in names {
            let filter = match name {
                "lowercase" => TokenFilterSpec::Lowercase,
                "stop" => TokenFilterSpec::Stop {
                    words: match stopwords {
                        Some(words) => words.to_vec(),
                        None => ENGLISH_STOPWORDS.iter().map(|w| w.to_string()).collect(),
                    },
                },
                "stem" | "stem_english" => TokenFilterSpec::Stem {
                    language: "english".into(),
                },
                other => {
                    return Err(format!(
                        "unknown token filter {other:?}; expected lowercase, stop or stem"
                    ))
                }
            };
            spec.filters.push(filter);
        }
        if stopwords.is_some()
            && !spec
                .filters
                .iter()
                .any(|f| matches!(f, TokenFilterSpec::Stop { .. }))
        {
            return Err("`stopwords` given without a `stop` filter in `analyzer`".into());
        }
        Ok(spec)
    }
}

/// Proto `NamespaceSchema`. Kosha is otherwise schemaless (clients send
/// typed fields with every document), so only the analysis and embedding
/// settings are honored; the other members are accepted and stored.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NamespaceSchema {
    #[serde(default)]
    pub fields: BTreeMap<String, FieldSchema>,
    /// Kosha extension: the analyzer for `Text` fields the schema does not
    /// list. Clients map every JSON string to `Text`, so this is how a
    /// whole namespace opts into e.g. `["standard@2", "stop", "stem"]`
    /// without naming each field. Empty = `standard`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub default_analyzer: Vec<String>,
    /// Stopword override for `default_analyzer`'s `stop` filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_stopwords: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FieldSchema {
    #[serde(
        default,
        rename = "type",
        alias = "field_type",
        skip_serializing_if = "Option::is_none"
    )]
    pub field_type: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stored: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub analyzer: Vec<String>,
    /// Kosha extension: replaces the built-in English list for this
    /// field's `stop` filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopwords: Option<Vec<String>>,
    /// Kosha extension (#209): the server embeds text sent for this
    /// `Vector` field (and `knn.query_text` against it) with this model.
    /// Absent = the client supplies vectors, as always.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<EmbeddingSpec>,
}

pub const SCHEMA_FIELD_TYPES: [&str; 8] = [
    "FIELD_TYPE_UNSPECIFIED",
    "FIELD_TYPE_TEXT",
    "FIELD_TYPE_KEYWORD",
    "FIELD_TYPE_INTEGER",
    "FIELD_TYPE_FLOAT",
    "FIELD_TYPE_DATE",
    "FIELD_TYPE_BOOLEAN",
    "FIELD_TYPE_VECTOR",
];

pub fn canonical_schema_field_type(value: &serde_json::Value) -> Option<&'static str> {
    match value {
        serde_json::Value::Number(n) => n
            .as_u64()
            .and_then(|i| usize::try_from(i).ok())
            .and_then(|i| SCHEMA_FIELD_TYPES.get(i).copied()),
        serde_json::Value::String(t) => {
            let upper = t.trim().to_ascii_uppercase();
            let bare = upper.strip_prefix("FIELD_TYPE_").unwrap_or(&upper);
            if matches!(bare, "GEO_POINT" | "GEOPOINT") {
                return Some("FIELD_TYPE_GEO_POINT");
            }
            SCHEMA_FIELD_TYPES
                .iter()
                .copied()
                .find(|known| known.strip_prefix("FIELD_TYPE_") == Some(bare))
        }
        _ => None,
    }
}

/// Largest `embedding.dims` a schema may declare.
pub const MAX_EMBEDDING_DIMS: u32 = 4096;

/// A vector field's server-side embedding model (#209).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EmbeddingSpec {
    /// Model id, as the embedding provider names it. Recorded in each
    /// segment's footer so vectors of two models are never compared.
    pub model: String,
    /// Dimension of the model's vectors.
    pub dims: u32,
    /// The vector distance metric. Only cosine is supported; omission
    /// preserves the cosine behavior of earlier schemas.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distance_metric: Option<String>,
    /// Which configured provider serves the model (`mock`, `http`, `onnx`).
    /// Omit for the default HTTP/mock provider; ONNX must be explicit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
}

impl EmbeddingSpec {
    /// Shape checks: a non-empty model and `dims` in `1..=4096`.
    pub fn validate(&self) -> Result<(), String> {
        if self.model.trim().is_empty() {
            return Err("embedding.model must not be empty".into());
        }
        if self.dims == 0 || self.dims > MAX_EMBEDDING_DIMS {
            return Err(format!(
                "embedding.dims must be in 1..={MAX_EMBEDDING_DIMS}, got {}",
                self.dims
            ));
        }
        if self
            .distance_metric
            .as_deref()
            .is_some_and(|metric| metric != "cosine")
        {
            return Err("embedding.distance_metric must be cosine".into());
        }
        Ok(())
    }

    /// Whether vectors of `self` and `other` live in the same space.
    pub fn same_space(&self, other: &EmbeddingSpec) -> bool {
        self.model == other.model && self.dims == other.dims
    }
}

impl FieldSchema {
    fn effective_type(&self) -> Option<&'static str> {
        match &self.field_type {
            Some(declared) => canonical_schema_field_type(declared),
            None => self.embedding.as_ref().map(|_| "FIELD_TYPE_VECTOR"),
        }
    }
}

impl NamespaceSchema {
    /// The first incompatible change to a previously declared field type.
    /// An embedding implies a vector type. Other undeclared types may be
    /// declared online.
    pub fn field_type_conflict(&self, proposed: &Self) -> Option<String> {
        for (name, field) in &self.fields {
            let new = proposed.fields.get(name);
            let compatible = match field.effective_type() {
                Some(old) if old == SCHEMA_FIELD_TYPES[0] => true,
                Some(old) => new.and_then(FieldSchema::effective_type) == Some(old),
                None => {
                    field.field_type.is_none()
                        || new.and_then(|f| f.field_type.as_ref()) == field.field_type.as_ref()
                }
            };
            if compatible {
                continue;
            }
            return Some(format!(
                "changing or removing the declared type of field {name:?} requires a reindex; \
                 export and re-ingest the documents into a namespace with the new schema, \
                 or add a new field"
            ));
        }
        None
    }

    /// The fields that declare an embedding model. Empty for every schema
    /// that does not opt in.
    pub fn embedded_fields(&self) -> impl Iterator<Item = (&String, &EmbeddingSpec)> {
        self.fields
            .iter()
            .filter_map(|(name, field)| field.embedding.as_ref().map(|spec| (name, spec)))
    }

    /// The embedding model declared for `field`, if any.
    pub fn embedding_for(&self, field: &str) -> Option<&EmbeddingSpec> {
        self.fields.get(field)?.embedding.as_ref()
    }

    /// Check every field's `embedding` (#209).
    pub fn validate_embeddings(&self) -> Result<(), String> {
        for (name, spec) in self.embedded_fields() {
            spec.validate()
                .map_err(|e| format!("field {name:?}: {e}"))?;
        }
        Ok(())
    }

    /// Resolve every analyzer name now, so a bad schema is rejected when
    /// it is set rather than when a segment is written.
    pub fn resolve(&self) -> Result<AnalysisConfig, String> {
        self.validate_embeddings()?;
        let default =
            AnalyzerSpec::resolve(&self.default_analyzer, self.default_stopwords.as_deref())
                .map_err(|e| format!("default_analyzer: {e}"))?;
        let mut fields = BTreeMap::new();
        for (name, field) in &self.fields {
            if field.analyzer.is_empty() && field.stopwords.is_none() {
                continue;
            }
            let spec = AnalyzerSpec::resolve(&field.analyzer, field.stopwords.as_deref())
                .map_err(|e| format!("field {name:?}: {e}"))?;
            fields.insert(name.clone(), spec);
        }
        let embedding_models = self
            .embedded_fields()
            .map(|(name, spec)| (name.clone(), spec.model.clone()))
            .collect();
        Ok(AnalysisConfig {
            default,
            fields,
            embedding_models,
        })
    }
}

/// A namespace's resolved analysis settings, handed to the segment writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalysisConfig {
    /// Chain for `Text` fields not in `fields`.
    pub default: AnalyzerSpec,
    /// Per-field chains.
    pub fields: BTreeMap<String, AnalyzerSpec>,
    /// Vector fields the schema embeds server-side, with their model id
    /// (#209). The segment writer records the model in each such column's
    /// [`crate::VectorFieldMeta::embedding_model`].
    pub embedding_models: BTreeMap<String, String>,
}

impl Default for AnalysisConfig {
    fn default() -> Self {
        Self {
            default: AnalyzerSpec::legacy(),
            fields: BTreeMap::new(),
            embedding_models: BTreeMap::new(),
        }
    }
}

impl AnalysisConfig {
    /// The chain for a `Text` field called `name`.
    pub fn spec_for(&self, name: &str) -> &AnalyzerSpec {
        self.fields.get(name).unwrap_or(&self.default)
    }
}

/// How one segment's text was analyzed, recorded in its footer
/// (`Footer::analysis`). Absent from the footer (`None`) means every
/// `Text` value went through [`AnalyzerSpec::legacy`] — true of every
/// segment written before #220 and of every segment written since with the
/// default settings, which is what keeps those segments byte-identical.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentAnalysis {
    /// Chain for `Text` fields that `fields` does not list.
    pub default: AnalyzerSpec,
    /// `Text` fields this segment indexed with a chain other than
    /// `default`. Only fields that actually occur are listed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, AnalyzerSpec>,
    /// Whether any `Text` value in this segment went through `default`.
    #[serde(default)]
    pub default_used: bool,
}

impl SegmentAnalysis {
    /// The distinct chains whose terms this segment's inverted index may
    /// hold — the chains a query has to be analyzed with to search it.
    /// Never empty: a segment with no text falls back to `default`.
    pub fn chains(&self) -> Vec<&AnalyzerSpec> {
        let mut chains: Vec<&AnalyzerSpec> = Vec::new();
        if self.default_used || self.fields.is_empty() {
            chains.push(&self.default);
        }
        for spec in self.fields.values() {
            if !chains.contains(&spec) {
                chains.push(spec);
            }
        }
        chains
    }
}

/// The chains to analyze a query with for a segment whose footer holds
/// `analysis` (`None` = legacy).
pub fn segment_query_chains(analysis: Option<&SegmentAnalysis>) -> Vec<AnalyzerSpec> {
    match analysis {
        None => vec![AnalyzerSpec::legacy()],
        Some(analysis) => analysis.chains().into_iter().cloned().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn no_analyzer_and_standard_resolve_to_legacy() {
        assert!(AnalyzerSpec::resolve(&[], None).unwrap().is_legacy());
        assert!(AnalyzerSpec::resolve(&names(&["standard"]), None)
            .unwrap()
            .is_legacy());
        assert!(AnalyzerSpec::resolve(&names(&["standard@1"]), None)
            .unwrap()
            .is_legacy());
        assert!(!AnalyzerSpec::resolve(&names(&["standard@2"]), None)
            .unwrap()
            .is_legacy());
    }

    #[test]
    fn filters_resolve_in_order_and_unknown_names_are_rejected() {
        let spec = AnalyzerSpec::resolve(&names(&["standard", "stop", "stem"]), None).unwrap();
        assert_eq!(spec.filters.len(), 3);
        assert!(
            matches!(spec.filters[1], TokenFilterSpec::Stop { ref words } if words.len() == ENGLISH_STOPWORDS.len())
        );
        assert!(AnalyzerSpec::resolve(&names(&["snowflake"]), None).is_err());
        assert!(AnalyzerSpec::resolve(&names(&["standard", "soundex"]), None).is_err());
        let custom = names(&["foo"]);
        let spec = AnalyzerSpec::resolve(&names(&["whitespace", "stop"]), Some(custom.as_slice()))
            .unwrap();
        assert_eq!(
            spec.filters,
            vec![TokenFilterSpec::Stop {
                words: vec!["foo".into()]
            }]
        );
        assert!(AnalyzerSpec::resolve(&names(&["standard"]), Some(custom.as_slice())).is_err());
    }

    #[test]
    fn schema_json_matches_proto_shape() {
        let schema: NamespaceSchema = serde_json::from_str(
            r#"{"fields": {"title": {"type": "FIELD_TYPE_TEXT", "indexed": true,
                "analyzer": ["standard", "stem"]}, "sku": {"analyzer": ["keyword"]},
                "body": {"type": "FIELD_TYPE_TEXT"}}}"#,
        )
        .unwrap();
        let config = schema.resolve().unwrap();
        assert!(config.default.is_legacy());
        assert_eq!(config.fields.len(), 2, "body has no analyzer: default");
        assert_eq!(config.spec_for("sku").tokenizer, TokenizerKind::Keyword);
        assert!(config.spec_for("body").is_legacy());
    }

    #[test]
    fn embedding_spec_parses_validates_and_stays_optional() {
        let schema: NamespaceSchema = serde_json::from_str(
            r#"{"fields": {"emb": {"type": "FIELD_TYPE_VECTOR",
                "embedding": {"model": "mock-1", "dims": 8, "provider": "mock"}},
                "title": {"analyzer": ["standard"]}}}"#,
        )
        .unwrap();
        let config = schema.resolve().unwrap();
        assert_eq!(config.embedding_models.get("emb").unwrap(), "mock-1");
        assert_eq!(schema.embedding_for("emb").unwrap().dims, 8);
        assert!(schema.embedding_for("title").is_none());
        // Fields without one serialize exactly as before.
        let title = serde_json::to_string(&schema.fields["title"]).unwrap();
        assert!(!title.contains("embedding"), "{title}");
        for bad in [
            r#"{"model": "", "dims": 8}"#,
            r#"{"model": "m", "dims": 0}"#,
            r#"{"model": "m", "dims": 4097}"#,
        ] {
            let json = format!(r#"{{"fields": {{"emb": {{"embedding": {bad}}}}}}}"#);
            let schema: NamespaceSchema = serde_json::from_str(&json).unwrap();
            assert!(schema.resolve().is_err(), "{bad}");
        }
    }

    #[test]
    fn footer_analysis_chains_dedupe_and_never_empty() {
        let stem = AnalyzerSpec::resolve(&names(&["standard", "stem"]), None).unwrap();
        let analysis = SegmentAnalysis {
            default: AnalyzerSpec::legacy(),
            fields: BTreeMap::from([("a".into(), stem.clone()), ("b".into(), stem.clone())]),
            default_used: false,
        };
        assert_eq!(analysis.chains(), vec![&stem]);
        let empty = SegmentAnalysis {
            default: stem.clone(),
            fields: BTreeMap::new(),
            default_used: false,
        };
        assert_eq!(empty.chains(), vec![&stem]);
        assert_eq!(segment_query_chains(None), vec![AnalyzerSpec::legacy()]);
    }
}
