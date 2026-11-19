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
        *self == Self::legacy()
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
/// typed fields with every document), so only the analysis settings are
/// honored; the other `FieldSchema` members are accepted and stored.
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
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
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
}

impl NamespaceSchema {
    /// Resolve every analyzer name now, so a bad schema is rejected when
    /// it is set rather than when a segment is written.
    pub fn resolve(&self) -> Result<AnalysisConfig, String> {
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
        Ok(AnalysisConfig { default, fields })
    }
}

/// A namespace's resolved analysis settings, handed to the segment writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalysisConfig {
    /// Chain for `Text` fields not in `fields`.
    pub default: AnalyzerSpec,
    /// Per-field chains.
    pub fields: BTreeMap<String, AnalyzerSpec>,
}

impl Default for AnalysisConfig {
    fn default() -> Self {
        Self {
            default: AnalyzerSpec::legacy(),
            fields: BTreeMap::new(),
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
