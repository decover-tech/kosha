//! Per-field inverted indexes and multi-field BM25 (issue #249).
//!
//! A segment's `inverted.idx` has always held one combined index: every
//! `Text` field of a document tokenized into the same postings, with one
//! length per document. That index is kept as is (the `_all` field), so
//! every existing query and every segment already on S3 behaves exactly
//! as before. Segments written since #249 add, in the same inverted index:
//!
//! * **field postings** under the key [`field_term_key`]`(field, term)`:
//!   the postings of `term` in `field` only;
//! * one **norms** list per field under [`field_norms_key`]`(field)`: a
//!   posting per document that has the field, whose term frequency is the
//!   document's token count in that field (its BM25 length);
//! * per-field `doc_count` / `total_length` in the footer
//!   ([`FieldIndex`]), from which per-field `avgdl` is derived.
//!
//! Field keys start with a NUL byte, which no analyzer emits for real
//! text, so they never collide with a combined-index term and sort ahead
//! of all of them. They go through the same posting blobs, caches and
//! hydration as every other term; only the term bloom leaves them out (a
//! field term is always also a combined-index term, so the bloom is
//! checked with the plain term — see [`bloom_term`]).
//!
//! Positions in both indexes are shifted so that consecutive `Text` values
//! of a document are [`FieldIndex::position_gap`] positions apart. A
//! phrase therefore cannot match across two fields (or two values of one
//! field) in the combined index either.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::analysis::{AnalyzerSpec, SegmentAnalysis};

/// The pseudo-field naming the combined index (every `Text` field). Also
/// accepted as `*` in a phrase's `field`.
pub const ALL_FIELD: &str = "_all";

/// Positions between the last token of one `Text` value and the first
/// token of the next one in the same document. Large enough that no
/// realistic phrase span plus slop reaches across it; the query side
/// clamps the slop so that none can (see [`max_phrase_slop`]).
pub const DEFAULT_POSITION_GAP: u32 = 1 << 16;

/// Per-field statistics of one segment, recorded in its footer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldStats {
    /// Documents with at least one token in this field.
    pub doc_count: u32,
    /// Tokens in this field over all documents.
    pub total_length: u64,
}

/// Footer record of a segment's per-field index (`Footer::field_index`).
/// Absent on every segment written before #249: such a segment has only
/// the combined index, and positions restart at 0 in each field.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldIndex {
    /// Positions between consecutive `Text` values of a document.
    pub position_gap: u32,
    /// Every `Text` field with at least one token in this segment.
    #[serde(default)]
    pub fields: BTreeMap<String, FieldStats>,
}

/// Inverted-index key of `term` in `field`'s own postings.
pub fn field_term_key(field: &str, term: &str) -> String {
    let mut key = String::with_capacity(field.len() + term.len() + 2);
    key.push('\0');
    key.push_str(field);
    key.push('\0');
    key.push_str(term);
    key
}

/// Inverted-index key of `field`'s norms list (per-document lengths).
pub fn field_norms_key(field: &str) -> String {
    field_term_key(field, "")
}

/// Whether an inverted-index key belongs to a per-field index rather than
/// the combined one.
pub fn is_field_key(key: &str) -> bool {
    key.starts_with('\0')
}

/// The term to test against a segment's term bloom for `key`: the bloom
/// holds only combined-index terms, and a field term is always one of
/// them too.
pub fn bloom_term(key: &str) -> &str {
    if !is_field_key(key) {
        return key;
    }
    match key[1..].find('\0') {
        Some(i) => &key[i + 2..],
        None => key,
    }
}

/// Whether a `Text` field gets its own postings. `_all` is reserved for
/// the combined index, and a NUL would make the key ambiguous.
pub fn is_indexable_field_name(name: &str) -> bool {
    !name.is_empty() && name != ALL_FIELD && !name.contains('\0')
}

/// The analysis chain a segment used for `field`. `_all` and fields the
/// segment never saw use the segment's default chain.
pub fn segment_field_chain(analysis: Option<&SegmentAnalysis>, field: &str) -> AnalyzerSpec {
    match analysis {
        None => AnalyzerSpec::legacy(),
        Some(analysis) => analysis
            .fields
            .get(field)
            .unwrap_or(&analysis.default)
            .clone(),
    }
}

/// The most slop a phrase may use on a segment whose values are
/// `position_gap` apart, for a phrase whose query positions span `span`:
/// past that, a match could straddle two values.
pub fn max_phrase_slop(position_gap: u32, span: u32) -> u32 {
    position_gap.saturating_sub(span).saturating_sub(1)
}

/// One entry of a multi-field query: `title^2` is `{field: "title",
/// boost: 2.0}`.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldBoost {
    pub field: String,
    pub boost: f64,
}

impl FieldBoost {
    /// Parse `name` or `name^boost`. The boost must be a finite number
    /// above zero. Field-name patterns (`title*`) are not supported.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let spec = spec.trim();
        let (field, boost) = match spec.rsplit_once('^') {
            Some((field, boost)) => {
                let value: f64 = boost
                    .trim()
                    .parse()
                    .map_err(|_| format!("fields: bad boost in {spec:?}"))?;
                if !value.is_finite() || value <= 0.0 {
                    return Err(format!(
                        "fields: boost in {spec:?} must be a finite number > 0"
                    ));
                }
                (field.trim(), value)
            }
            None => (spec, 1.0),
        };
        if field == "*" {
            return Ok(Self {
                field: ALL_FIELD.to_string(),
                boost,
            });
        }
        if field.is_empty() || field.contains('\0') {
            return Err(format!("fields: bad field name in {spec:?}"));
        }
        if field.contains('*') || field.contains('?') {
            return Err(format!(
                "fields: field-name patterns are not supported ({spec:?}); list the fields"
            ));
        }
        Ok(Self {
            field: field.to_string(),
            boost,
        })
    }

    pub fn is_all(&self) -> bool {
        self.field == ALL_FIELD
    }
}

/// Parse a request's `fields` list. Duplicate fields are an error (their
/// boosts would be ambiguous).
pub fn parse_field_boosts(specs: &[String]) -> Result<Vec<FieldBoost>, String> {
    let mut out: Vec<FieldBoost> = Vec::with_capacity(specs.len());
    for spec in specs {
        let parsed = FieldBoost::parse(spec)?;
        if out.iter().any(|f| f.field == parsed.field) {
            return Err(format!("fields: {:?} is listed twice", parsed.field));
        }
        out.push(parsed);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip_to_bloom_terms() {
        let key = field_term_key("title", "rust");
        assert!(is_field_key(&key));
        assert_eq!(bloom_term(&key), "rust");
        assert_eq!(bloom_term("rust"), "rust");
        assert_eq!(bloom_term(&field_norms_key("title")), "");
        assert!(!is_field_key("rust"));
        // Field keys sort ahead of every combined-index term.
        assert!(key.as_str() < "a");
    }

    #[test]
    fn boosts_parse_and_reject_bad_specs() {
        let parsed = parse_field_boosts(&["title^2".into(), "content".into()]).unwrap();
        assert_eq!(
            parsed,
            vec![
                FieldBoost {
                    field: "title".into(),
                    boost: 2.0
                },
                FieldBoost {
                    field: "content".into(),
                    boost: 1.0
                },
            ]
        );
        assert_eq!(FieldBoost::parse("*").unwrap().field, ALL_FIELD);
        assert_eq!(FieldBoost::parse("body^0.5").unwrap().boost, 0.5);
        for bad in [
            "title^0",
            "title^-1",
            "title^x",
            "title^inf",
            "",
            "ti*le",
            "^2",
        ] {
            assert!(
                FieldBoost::parse(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
        assert!(parse_field_boosts(&["a".into(), "a^2".into()]).is_err());
    }

    #[test]
    fn slop_is_clamped_below_the_gap() {
        assert_eq!(max_phrase_slop(100, 3), 96);
        assert_eq!(max_phrase_slop(2, 5), 0);
    }
}
