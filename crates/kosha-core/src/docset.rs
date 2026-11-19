//! Filter evaluation over a whole segment at once.
//!
//! A filter clause is evaluated against a segment's [`FilterStore`] into a
//! [`DocSet`]: one bit per document, set when the document passes. Asking
//! "does this document pass?" is then a single bit test, which is what lets
//! the scoring loops apply a filter *while* they walk postings instead of
//! scoring every match first and filtering afterwards.
//!
//! The set depends only on the clause and the segment, and segments are
//! immutable, so callers cache it per segment (see kosha-query). A request
//! that repeats a filter seen before, which is every request scoped to the
//! same matter, pays for the evaluation once.
//!
//! This is the one implementation of the filter language. The search path
//! and delete-by-query both go through it, so a clause can never match one
//! set of documents for a search and another for a delete.

use std::collections::{HashMap, HashSet};

use crate::{list_elements, lookup_field, BoolFilter, FilterClause, FilterStore, RangeBound};

/// A set of `doc_seq`s within one segment, as a bitmap.
///
/// A plain `Vec<u64>` rather than a compressed bitmap: a segment holds at
/// most tens of thousands of documents, so the whole set is a few kilobytes
/// and a membership test is one shift and one AND with no branching on a
/// container type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocSet {
    words: Vec<u64>,
    len: u32,
}

impl DocSet {
    /// An empty set over documents `0..len`.
    pub fn empty(len: u32) -> Self {
        Self {
            words: vec![0; (len as usize).div_ceil(64)],
            len,
        }
    }

    /// The set of every document `0..len`.
    pub fn full(len: u32) -> Self {
        let mut set = Self {
            words: vec![u64::MAX; (len as usize).div_ceil(64)],
            len,
        };
        set.clear_tail();
        set
    }

    /// Bits past `len` in the last word must stay zero, so `count` and
    /// `iter` never report a document the segment does not have.
    fn clear_tail(&mut self) {
        let used = (self.len % 64) as u64;
        if used != 0 {
            if let Some(last) = self.words.last_mut() {
                *last &= (1u64 << used) - 1;
            }
        }
    }

    /// Number of documents the set ranges over (not how many are in it).
    pub fn universe(&self) -> u32 {
        self.len
    }

    /// Adds `doc_seq`. A `doc_seq` outside `0..len` is ignored: it names no
    /// document of this segment.
    #[inline]
    pub fn insert(&mut self, doc_seq: u32) {
        if doc_seq < self.len {
            self.words[(doc_seq / 64) as usize] |= 1u64 << (doc_seq % 64);
        }
    }

    #[inline]
    pub fn contains(&self, doc_seq: u32) -> bool {
        doc_seq < self.len && self.words[(doc_seq / 64) as usize] & (1u64 << (doc_seq % 64)) != 0
    }

    /// How many documents are in the set.
    pub fn count(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|w| *w == 0)
    }

    /// True when every document `0..len` is in the set.
    pub fn is_full(&self) -> bool {
        self.count() == self.len as usize
    }

    /// How many documents are in both sets, without building the
    /// intersection.
    pub fn intersection_count(&self, other: &DocSet) -> usize {
        debug_assert_eq!(self.len, other.len);
        self.words
            .iter()
            .zip(&other.words)
            .map(|(a, b)| (a & b).count_ones() as usize)
            .sum()
    }

    pub fn intersect_with(&mut self, other: &DocSet) {
        debug_assert_eq!(self.len, other.len);
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a &= *b;
        }
    }

    pub fn union_with(&mut self, other: &DocSet) {
        debug_assert_eq!(self.len, other.len);
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a |= *b;
        }
    }

    pub fn subtract(&mut self, other: &DocSet) {
        debug_assert_eq!(self.len, other.len);
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a &= !*b;
        }
    }

    /// The members, in ascending `doc_seq` order.
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.words.iter().enumerate().flat_map(|(index, &word)| {
            let base = index as u32 * 64;
            let mut rest = word;
            std::iter::from_fn(move || {
                if rest == 0 {
                    return None;
                }
                let bit = rest.trailing_zeros();
                rest &= rest - 1;
                Some(base + bit)
            })
        })
    }
}

/// The documents of a segment that pass `clause`.
///
/// `doc_count` is the segment's document count; the result ranges over
/// `0..doc_count`.
pub fn evaluate_filter(clause: &FilterClause, store: &FilterStore, doc_count: u32) -> DocSet {
    match clause {
        FilterClause::Term { term } => {
            // Several fields in one `term` object are alternatives (OR).
            let mut set = DocSet::empty(doc_count);
            for (field, value) in term {
                mark_equal(&mut set, store, field, std::slice::from_ref(value));
            }
            set
        }
        FilterClause::Terms { terms } => {
            let mut set = DocSet::empty(doc_count);
            for (field, values) in terms {
                mark_equal(&mut set, store, field, values);
            }
            set
        }
        FilterClause::Range { range } => {
            let mut set = DocSet::empty(doc_count);
            for (field, bound) in range {
                mark_in_range(&mut set, store, field, bound);
            }
            set
        }
        FilterClause::Bool { bool: b } => evaluate_bool(b, store, doc_count),
        FilterClause::MatchAll { .. } => DocSet::full(doc_count),
        FilterClause::Exists { exists } => {
            let mut set = DocSet::empty(doc_count);
            let field = exists.field.as_str();
            if let Some(entries) = lookup_field(&store.string_fields, field) {
                for (doc_seq, value) in entries {
                    // An empty list is "no value", as in OpenSearch.
                    if list_elements(value).is_none_or(|elements| !elements.is_empty()) {
                        set.insert(*doc_seq);
                    }
                }
            }
            if let Some(entries) = lookup_field(&store.integer_fields, field) {
                entries.iter().for_each(|(doc_seq, _)| set.insert(*doc_seq));
            }
            if let Some(entries) = lookup_field(&store.float_fields, field) {
                entries.iter().for_each(|(doc_seq, _)| set.insert(*doc_seq));
            }
            set
        }
    }
}

/// Marks every document whose `field` equals one of `values`.
///
/// A field lives in exactly one of the store's three typed columns. The
/// value arrives as a string either way, so for a numeric column it is
/// parsed and compared as a number: `{"term": {"page": "7"}}` matches the
/// integer 7. Matching only the string column (the earlier behaviour) meant
/// a term filter on an Integer or Float field never matched anything.
fn mark_equal(set: &mut DocSet, store: &FilterStore, field: &str, values: &[String]) {
    if let Some(entries) = lookup_field(&store.string_fields, field) {
        let wanted: HashSet<&str> = values.iter().map(String::as_str).collect();
        for (doc_seq, value) in entries {
            // The whole value, or any element of a list value (see
            // `list_elements`). The leading-bracket test keeps the JSON
            // parse off every ordinary scalar.
            if wanted.contains(value.as_str())
                || (value.starts_with('[')
                    && list_elements(value).is_some_and(|elements| {
                        elements.iter().any(|e| wanted.contains(e.as_str()))
                    }))
            {
                set.insert(*doc_seq);
            }
        }
    } else if let Some(entries) = lookup_field(&store.integer_fields, field) {
        let wanted: HashSet<i64> = values
            .iter()
            .filter_map(|v| v.trim().parse().ok())
            .collect();
        if wanted.is_empty() {
            return;
        }
        for (doc_seq, value) in entries {
            if wanted.contains(value) {
                set.insert(*doc_seq);
            }
        }
    } else if let Some(entries) = lookup_field(&store.float_fields, field) {
        let wanted: Vec<f64> = values
            .iter()
            .filter_map(|v| v.trim().parse().ok())
            .collect();
        if wanted.is_empty() {
            return;
        }
        for (doc_seq, value) in entries {
            if wanted.contains(value) {
                set.insert(*doc_seq);
            }
        }
    }
}

fn mark_in_range(set: &mut DocSet, store: &FilterStore, field: &str, bound: &RangeBound) {
    if let Some(entries) = lookup_field(&store.integer_fields, field) {
        for (doc_seq, value) in entries {
            if in_range_i64(*value, bound) {
                set.insert(*doc_seq);
            }
        }
    } else if let Some(entries) = lookup_field(&store.float_fields, field) {
        for (doc_seq, value) in entries {
            if in_range_f64(*value, bound) {
                set.insert(*doc_seq);
            }
        }
    } else if let Some(entries) = lookup_field(&store.string_fields, field) {
        for (doc_seq, value) in entries {
            if in_range_str(value, bound) {
                set.insert(*doc_seq);
            }
        }
    }
}

/// `must` narrows, `must_not` subtracts, and a non-empty `should` requires
/// at least `max(1, minimum_should_match)` of its clauses. A `bool` with no
/// clauses at all passes every document.
fn evaluate_bool(b: &BoolFilter, store: &FilterStore, doc_count: u32) -> DocSet {
    let mut set = DocSet::full(doc_count);
    for clause in &b.must {
        set.intersect_with(&evaluate_filter(clause, store, doc_count));
        if set.is_empty() {
            return set;
        }
    }
    for clause in &b.must_not {
        set.subtract(&evaluate_filter(clause, store, doc_count));
    }
    if !b.should.is_empty() {
        let need = b.minimum_should_match.max(1);
        if need == 1 {
            let mut any = DocSet::empty(doc_count);
            for clause in &b.should {
                any.union_with(&evaluate_filter(clause, store, doc_count));
            }
            set.intersect_with(&any);
        } else {
            let mut matched: HashMap<u32, usize> = HashMap::new();
            for clause in &b.should {
                for doc_seq in evaluate_filter(clause, store, doc_count).iter() {
                    *matched.entry(doc_seq).or_default() += 1;
                }
            }
            let mut enough = DocSet::empty(doc_count);
            for (doc_seq, count) in matched {
                if count >= need {
                    enough.insert(doc_seq);
                }
            }
            set.intersect_with(&enough);
        }
    }
    set
}

// A bound that does not parse as the column's type is ignored rather than
// failing the comparison, as it always has been.

fn in_range_i64(value: i64, bound: &RangeBound) -> bool {
    let parse = |s: &Option<String>| s.as_ref().and_then(|s| s.trim().parse::<i64>().ok());
    parse(&bound.gte).is_none_or(|b| value >= b)
        && parse(&bound.gt).is_none_or(|b| value > b)
        && parse(&bound.lte).is_none_or(|b| value <= b)
        && parse(&bound.lt).is_none_or(|b| value < b)
}

fn in_range_f64(value: f64, bound: &RangeBound) -> bool {
    let parse = |s: &Option<String>| s.as_ref().and_then(|s| s.trim().parse::<f64>().ok());
    parse(&bound.gte).is_none_or(|b| value >= b)
        && parse(&bound.gt).is_none_or(|b| value > b)
        && parse(&bound.lte).is_none_or(|b| value <= b)
        && parse(&bound.lt).is_none_or(|b| value < b)
}

fn in_range_str(value: &str, bound: &RangeBound) -> bool {
    bound.gte.as_deref().is_none_or(|b| value >= b)
        && bound.gt.as_deref().is_none_or(|b| value > b)
        && bound.lte.as_deref().is_none_or(|b| value <= b)
        && bound.lt.as_deref().is_none_or(|b| value < b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> FilterStore {
        let mut store = FilterStore::default();
        store.string_fields.insert(
            "matter".into(),
            vec![
                (0, "a".into()),
                (1, "a".into()),
                (2, "b".into()),
                (3, "c".into()),
            ],
        );
        store
            .integer_fields
            .insert("page".into(), vec![(0, 1), (1, 7), (2, 7), (3, 10)]);
        store
            .float_fields
            .insert("score".into(), vec![(0, 0.5), (1, 1.5), (2, 2.5), (3, 2.5)]);
        store
    }

    fn eval(json: &str) -> Vec<u32> {
        let clause: FilterClause = serde_json::from_str(json).unwrap();
        evaluate_filter(&clause, &store(), 4).iter().collect()
    }

    #[test]
    fn docset_basics_and_tail_bits() {
        let full = DocSet::full(70);
        assert_eq!(full.count(), 70);
        assert!(full.is_full());
        assert!(full.contains(69));
        assert!(!full.contains(70), "past the end is never a member");
        assert_eq!(full.iter().last(), Some(69));

        let mut set = DocSet::empty(70);
        assert!(set.is_empty());
        set.insert(3);
        set.insert(64);
        set.insert(500); // outside the segment: ignored
        assert_eq!(set.iter().collect::<Vec<_>>(), vec![3, 64]);

        let mut rest = DocSet::full(70);
        rest.subtract(&set);
        assert_eq!(rest.count(), 68);
        assert!(!rest.contains(64));
        assert_eq!(DocSet::full(0).count(), 0);
        assert_eq!(DocSet::full(64).count(), 64);
    }

    #[test]
    fn term_and_terms() {
        assert_eq!(eval(r#"{"term": {"matter": "a"}}"#), vec![0, 1]);
        assert_eq!(
            eval(r#"{"terms": {"matter": ["b", "c", "zz"]}}"#),
            vec![2, 3]
        );
        assert_eq!(eval(r#"{"term": {"matter": "nope"}}"#), Vec::<u32>::new());
        assert_eq!(eval(r#"{"term": {"unknown": "a"}}"#), Vec::<u32>::new());
    }

    #[test]
    fn keyword_sub_field_names_resolve_to_the_stored_field() {
        // OpenSearch's exact-match twin of a field; one column in Kosha.
        assert_eq!(eval(r#"{"term": {"matter.keyword": "a"}}"#), vec![0, 1]);
        assert_eq!(eval(r#"{"terms": {"matter.raw": ["b"]}}"#), vec![2]);
        assert_eq!(
            eval(r#"{"range": {"page.keyword": {"gte": "10"}}}"#),
            vec![3]
        );
    }

    #[test]
    fn term_on_a_numeric_field_compares_as_a_number() {
        assert_eq!(eval(r#"{"term": {"page": "7"}}"#), vec![1, 2]);
        assert_eq!(eval(r#"{"terms": {"page": ["1", "10"]}}"#), vec![0, 3]);
        assert_eq!(eval(r#"{"term": {"score": "2.5"}}"#), vec![2, 3]);
        assert_eq!(eval(r#"{"term": {"page": "seven"}}"#), Vec::<u32>::new());
    }

    #[test]
    fn range_per_column_type() {
        assert_eq!(
            eval(r#"{"range": {"page": {"gte": "7", "lt": "10"}}}"#),
            vec![1, 2]
        );
        assert_eq!(eval(r#"{"range": {"score": {"gt": "1.5"}}}"#), vec![2, 3]);
        assert_eq!(eval(r#"{"range": {"matter": {"gte": "b"}}}"#), vec![2, 3]);
    }

    #[test]
    fn bool_combinations() {
        assert_eq!(eval(r#"{"bool": {}}"#), vec![0, 1, 2, 3]);
        assert_eq!(
            eval(r#"{"bool": {"must": [{"term": {"matter": "a"}}, {"term": {"page": "7"}}]}}"#),
            vec![1]
        );
        assert_eq!(
            eval(r#"{"bool": {"must_not": [{"term": {"matter": "a"}}]}}"#),
            vec![2, 3]
        );
        assert_eq!(
            eval(r#"{"bool": {"should": [{"term": {"matter": "a"}}, {"term": {"matter": "c"}}]}}"#),
            vec![0, 1, 3]
        );
        // A non-empty should is required even next to a must.
        assert_eq!(
            eval(
                r#"{"bool": {"must": [{"term": {"page": "7"}}],
                             "should": [{"term": {"matter": "b"}}]}}"#
            ),
            vec![2]
        );
        assert_eq!(
            eval(
                r#"{"bool": {"should": [{"term": {"matter": "a"}}, {"term": {"page": "7"}},
                                        {"term": {"score": "2.5"}}],
                             "minimum_should_match": 2}}"#
            ),
            vec![1, 2]
        );
        // The backend's nesting: bool.filter -> bool.filter -> bool.should.
        assert_eq!(
            eval(
                r#"{"bool": {"filter": [{"bool": {"filter": [{"bool": {"should": [
                        {"term": {"matter": "b"}}, {"term": {"matter": "c"}}]}}]}}]}}"#
            ),
            vec![2, 3]
        );
        assert_eq!(eval(r#"{"match_all": {}}"#), vec![0, 1, 2, 3]);
    }

    #[test]
    fn list_values_match_by_element() {
        let mut store = FilterStore::default();
        store.string_fields.insert(
            "tags".into(),
            vec![
                (0, r#"["urgent","sealed"]"#.into()),
                (1, r#"["draft"]"#.into()),
                (2, "[]".into()),
                (3, "urgent".into()),
            ],
        );
        let eval = |json: &str| -> Vec<u32> {
            let clause: FilterClause = serde_json::from_str(json).unwrap();
            evaluate_filter(&clause, &store, 4).iter().collect()
        };
        assert_eq!(eval(r#"{"term": {"tags.keyword": "urgent"}}"#), vec![0, 3]);
        assert_eq!(
            eval(r#"{"terms": {"tags": ["draft", "sealed"]}}"#),
            vec![0, 1]
        );
        assert_eq!(eval(r#"{"term": {"tags": "nope"}}"#), Vec::<u32>::new());
        // An empty list is not a value.
        assert_eq!(eval(r#"{"exists": {"field": "tags"}}"#), vec![0, 1, 3]);
        // The bloom has to admit the elements, or the segment is pruned
        // before the filter is ever evaluated.
        let blooms = crate::build_filter_blooms(&store.string_fields);
        assert!(blooms["tags"].may_contain("sealed"));
        assert!(blooms["tags"].may_contain("urgent"));
    }

    #[test]
    fn exists_matches_documents_with_a_value_in_any_column_type() {
        let mut store = store();
        store
            .string_fields
            .insert("tag".into(), vec![(1, "x".into())]);
        store.integer_fields.insert("startMs".into(), vec![(2, 5)]);
        let eval = |json: &str| -> Vec<u32> {
            let clause: FilterClause = serde_json::from_str(json).unwrap();
            evaluate_filter(&clause, &store, 4).iter().collect()
        };
        assert_eq!(eval(r#"{"exists": {"field": "tag"}}"#), vec![1]);
        assert_eq!(eval(r#"{"exists": {"field": "startMs"}}"#), vec![2]);
        assert_eq!(eval(r#"{"exists": {"field": "nope"}}"#), Vec::<u32>::new());
        assert_eq!(
            eval(r#"{"bool": {"must_not": [{"exists": {"field": "tag"}}]}}"#),
            vec![0, 2, 3]
        );
    }
}
