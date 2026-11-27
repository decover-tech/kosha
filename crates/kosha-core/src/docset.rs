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

use crate::{
    list_elements, lookup_field, BoolFilter, FilterClause, FilterStore, GlobFilter, PrefixFilter,
    RangeBound,
};

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

    /// Adds every member of `bitmap` (a roaring bitmap of `doc_seq`s, as a
    /// columnar filter index stores per term). Members outside `0..len`
    /// are ignored, as by [`Self::insert`].
    pub fn insert_bitmap(&mut self, bitmap: &roaring::RoaringBitmap) {
        for doc_seq in bitmap.range(..self.len) {
            self.words[(doc_seq / 64) as usize] |= 1u64 << (doc_seq % 64);
        }
    }

    /// Removes every `doc_seq` in `doc_seqs` (e.g. a segment's deleted
    /// documents). Ones outside `0..len` are ignored.
    pub fn remove_all<'a>(&mut self, doc_seqs: impl IntoIterator<Item = &'a u32>) {
        for &doc_seq in doc_seqs {
            if doc_seq < self.len {
                self.words[(doc_seq / 64) as usize] &= !(1u64 << (doc_seq % 64));
            }
        }
    }

    /// Whether any member lies in `first..=last`. A scoring loop asks this
    /// per postings block to skip blocks the filter rules out entirely.
    pub fn any_in_range(&self, first: u32, last: u32) -> bool {
        let last = last.min(self.len.saturating_sub(1));
        if first > last || self.len == 0 {
            return false;
        }
        let (fw, lw) = ((first / 64) as usize, (last / 64) as usize);
        let low_mask = u64::MAX << (first % 64);
        let high_mask = u64::MAX >> (63 - last % 64);
        if fw == lw {
            return self.words[fw] & low_mask & high_mask != 0;
        }
        self.words[fw] & low_mask != 0
            || self.words[fw + 1..lw].iter().any(|w| *w != 0)
            || self.words[lw] & high_mask != 0
    }

    /// The members as a roaring bitmap, for consumers that want the
    /// compressed form (e.g. a filtered vector probe, #252).
    pub fn to_bitmap(&self) -> roaring::RoaringBitmap {
        roaring::RoaringBitmap::from_sorted_iter(self.iter()).expect("iter is ascending")
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
                    // An empty list is "no value", as in OpenSearch. A
                    // list of objects (a nested container, see
                    // `crate::nested`) has no plain elements but is a value.
                    if list_elements(value).is_none_or(|elements| !elements.is_empty())
                        || crate::nested::parse_container(value).is_some()
                    {
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
        FilterClause::GeoDistance { geo_distance: geo } => {
            // `GeoPoint` values sit in the string column as canonical
            // "lat,lon" text, one entry per value (a document with several
            // points has several entries; any one within the distance
            // matches). Anything else in the column does not parse as a
            // point and never matches.
            let mut set = DocSet::empty(doc_count);
            if let Some(entries) = lookup_field(&store.string_fields, &geo.field) {
                for (doc_seq, value) in entries {
                    let within = crate::rank::parse_geo_str(value).is_some_and(|(lat, lon)| {
                        crate::rank::haversine_meters(geo.lat, geo.lon, lat, lon) <= geo.distance
                    });
                    if within {
                        set.insert(*doc_seq);
                    }
                }
            }
            set
        }
        FilterClause::Prefix { prefix } => {
            let mut set = DocSet::empty(doc_count);
            mark_prefixed(&mut set, store, prefix);
            set
        }
        FilterClause::Glob { glob } => {
            // A linear scan of the column, like `prefix`; the columnar
            // evaluator seeks to the glob's literal prefix instead.
            let mut set = DocSet::empty(doc_count);
            if let Some(entries) = lookup_field(&store.string_fields, &glob.field) {
                for (doc_seq, value) in entries {
                    if glob_matches(glob, value) {
                        set.insert(*doc_seq);
                    }
                }
            }
            set
        }
    }
}

/// Rejects a clause this segment's columns cannot answer, before it is
/// evaluated.
///
/// Today that is a `prefix` on a numeric (Integer / Float) column: a
/// prefix of a number's decimal text is not a meaningful filter, and
/// quietly matching nothing would hide the mistake. [`evaluate_filter`]
/// itself stays infallible (and matches nothing for such a clause), so
/// every caller that takes a filter from a request runs this first: the
/// search path, `FilterApplier`, and delete-by-query.
pub fn check_filter(clause: &FilterClause, store: &FilterStore) -> Result<(), String> {
    match clause {
        FilterClause::Prefix { prefix } => {
            let field = prefix.field.as_str();
            if lookup_field(&store.string_fields, field).is_none()
                && (lookup_field(&store.integer_fields, field).is_some()
                    || lookup_field(&store.float_fields, field).is_some())
            {
                return Err(prefix_on_numeric_error(field));
            }
            Ok(())
        }
        FilterClause::Glob { glob } => {
            let field = glob.field.as_str();
            if lookup_field(&store.string_fields, field).is_none()
                && (lookup_field(&store.integer_fields, field).is_some()
                    || lookup_field(&store.float_fields, field).is_some())
            {
                return Err(glob_on_numeric_error(field));
            }
            Ok(())
        }
        FilterClause::Bool { bool: b } => b
            .must
            .iter()
            .chain(&b.must_not)
            .chain(&b.should)
            .try_for_each(|child| check_filter(child, store)),
        FilterClause::Term { .. }
        | FilterClause::Terms { .. }
        | FilterClause::Range { .. }
        | FilterClause::MatchAll { .. }
        | FilterClause::Exists { .. }
        | FilterClause::GeoDistance { .. } => Ok(()),
    }
}

/// The error [`check_filter`] reports for a `prefix` on a numeric field,
/// shared with the columnar evaluator so both formats say the same thing.
pub fn prefix_on_numeric_error(field: &str) -> String {
    format!(
        "prefix filter on `{field}` is not supported: it is a numeric \
         field; use `range` or `term` instead"
    )
}

/// The error [`check_filter`] reports for a `glob` on a numeric field.
pub fn glob_on_numeric_error(field: &str) -> String {
    format!(
        "glob filter on `{field}` is not supported: it is a numeric \
         field; use `range` or `term` instead"
    )
}

/// Whether `value` (or, for a list value, one of its elements) matches the
/// whole glob. Lists follow the same rule as [`prefix_matches`]: only the
/// elements are matched, never the JSON text.
pub fn glob_matches(glob: &GlobFilter, value: &str) -> bool {
    match value
        .starts_with('[')
        .then(|| list_elements(value))
        .flatten()
    {
        Some(elements) => elements.iter().any(|e| glob.is_match(e)),
        None => glob.is_match(value),
    }
}

/// Whether `value` (or, for a list value, one of its elements) starts with
/// `prefix`; see [`mark_prefixed`]. Shared with the columnar evaluator,
/// which tests each distinct dictionary term once.
pub fn prefix_matches(prefix: &PrefixFilter, value: &str) -> bool {
    let matches = |candidate: &str| {
        if prefix.case_insensitive {
            candidate
                .to_lowercase()
                .starts_with(&prefix.value.to_lowercase())
        } else {
            candidate.starts_with(&prefix.value)
        }
    };
    match value
        .starts_with('[')
        .then(|| list_elements(value))
        .flatten()
    {
        Some(elements) => elements.iter().any(|e| matches(e)),
        None => matches(value),
    }
}

/// Marks every document whose string `field` starts with the prefix.
///
/// A list value (see [`list_elements`]) matches when any element does, and
/// is matched on its elements only, never on its JSON text: otherwise a
/// prefix of `[` would match every list, and the empty prefix would match
/// an empty list, which `exists` treats as "no value". With that rule an
/// empty prefix matches exactly the documents `exists` matches.
///
/// This is a linear scan of the column. Filter columns are stored in
/// `doc_seq` order, not sorted by value, so there is no range to seek to;
/// the result is cached per segment like every other filter (see
/// kosha-query's `TrackedSegment::filter_set`), so the scan is paid once
/// per segment and clause. A value-sorted column that answers a prefix
/// with a range scan is a possible follow-up.
///
/// Numeric columns never match here; [`check_filter`] rejects them up
/// front.
fn mark_prefixed(set: &mut DocSet, store: &FilterStore, prefix: &PrefixFilter) {
    let Some(entries) = lookup_field(&store.string_fields, &prefix.field) else {
        return;
    };
    // The prefix is folded once, not once per value.
    let needle = if prefix.case_insensitive {
        prefix.value.to_lowercase()
    } else {
        prefix.value.clone()
    };
    let matches = |candidate: &str| {
        if prefix.case_insensitive {
            candidate.to_lowercase().starts_with(&needle)
        } else {
            candidate.starts_with(&needle)
        }
    };
    for (doc_seq, value) in entries {
        let hit = match value
            .starts_with('[')
            .then(|| list_elements(value))
            .flatten()
        {
            Some(elements) => elements.iter().any(|e| matches(e)),
            None => matches(value),
        };
        if hit {
            set.insert(*doc_seq);
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
            // A list value is in range when any element is (#206): the
            // multi-valued semantics `term` already has.
            let hit = match value
                .starts_with('[')
                .then(|| list_elements(value))
                .flatten()
            {
                Some(elements) => elements.iter().any(|e| in_range_str(e, bound)),
                None => in_range_str(value, bound),
            };
            if hit {
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
    fn docset_ranges_bitmaps_and_removal() {
        let mut set = DocSet::empty(200);
        for d in [3, 64, 130, 199] {
            set.insert(d);
        }
        assert!(set.any_in_range(0, 3));
        assert!(!set.any_in_range(4, 63));
        assert!(set.any_in_range(4, 64));
        assert!(set.any_in_range(65, 500), "last clamps to the universe");
        assert!(!set.any_in_range(65, 129));
        assert!(!set.any_in_range(10, 5));
        assert!(!DocSet::empty(0).any_in_range(0, 0));
        // Brute force against contains on every window.
        for first in 0..200 {
            for last in first..200 {
                let want = (first..=last).any(|d| set.contains(d));
                assert_eq!(set.any_in_range(first, last), want, "{first}..={last}");
            }
        }

        let bitmap = set.to_bitmap();
        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![3, 64, 130, 199]);
        let mut copy = DocSet::empty(150);
        copy.insert_bitmap(&bitmap);
        assert_eq!(copy.iter().collect::<Vec<_>>(), vec![3, 64, 130]);
        copy.remove_all(&[64, 999]);
        assert_eq!(copy.iter().collect::<Vec<_>>(), vec![3, 130]);
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
    fn multi_valued_columns_match_on_any_value() {
        // What the segment writer stores for a flattened array of objects
        // (#206): JSON-list text in the string column, one float entry per
        // number, so a doc_seq can repeat.
        let mut store = FilterStore::default();
        store.string_fields.insert(
            "authors.name".into(),
            vec![(0, r#"["Ada","Bob"]"#.into()), (1, "Cy".into())],
        );
        store
            .float_fields
            .insert("authors.n".into(), vec![(0, 1.0), (0, 9.0), (1, 5.0)]);
        store.string_fields.insert(
            "authors".into(),
            vec![(0, r#"[{"name":"Ada"},{"name":"Bob"}]"#.into())],
        );
        let eval = |json: &str| -> Vec<u32> {
            let clause: FilterClause = serde_json::from_str(json).unwrap();
            evaluate_filter(&clause, &store, 2).iter().collect()
        };
        assert_eq!(eval(r#"{"term": {"authors.name": "Bob"}}"#), vec![0]);
        assert_eq!(eval(r#"{"term": {"authors.n": "9"}}"#), vec![0]);
        assert_eq!(
            eval(r#"{"range": {"authors.n": {"gte": "5"}}}"#),
            vec![0, 1]
        );
        assert_eq!(eval(r#"{"range": {"authors.n": {"lt": "2"}}}"#), vec![0]);
        assert_eq!(
            eval(r#"{"range": {"authors.name": {"gte": "B", "lt": "C"}}}"#),
            vec![0]
        );
        assert_eq!(eval(r#"{"exists": {"field": "authors"}}"#), vec![0]);
        assert_eq!(eval(r#"{"exists": {"field": "authors.n"}}"#), vec![0, 1]);
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

    /// Issue #194: an empty string is a value (as in OpenSearch, where `""`
    /// is non-null), so `exists` matches it; an absent field, an empty
    /// list and a list of only nulls are not.
    #[test]
    fn exists_counts_empty_string_but_not_empty_list() {
        let mut store = FilterStore::default();
        store.string_fields.insert(
            "code".into(),
            vec![
                (0, "".into()),
                (1, "x".into()),
                (2, "[]".into()),
                (3, "[null]".into()),
                (4, r#"[""]"#.into()),
                // 5: no `code`
            ],
        );
        let eval = |json: &str| -> Vec<u32> {
            let clause: FilterClause = serde_json::from_str(json).unwrap();
            evaluate_filter(&clause, &store, 6).iter().collect()
        };
        assert_eq!(eval(r#"{"exists": {"field": "code"}}"#), vec![0, 1, 4]);
        assert_eq!(
            eval(r#"{"bool": {"must_not": [{"exists": {"field": "code"}}]}}"#),
            vec![2, 3, 5]
        );
    }

    /// Codes for the `prefix` acceptance criteria, plus a list value, a
    /// document without the field, and a mixed-case value.
    fn prefix_store() -> FilterStore {
        let mut store = store();
        store.string_fields.insert(
            "code".into(),
            vec![
                (0, "abc".into()),
                (1, "ab".into()),
                (2, "a".into()),
                (3, "xab".into()),
                (4, "ABC".into()),
                (5, r#"["zz","abd"]"#.into()),
                (6, "[]".into()),
                // 7: no `code`
            ],
        );
        store
    }

    fn eval_prefix(json: &str) -> Vec<u32> {
        let clause: FilterClause = serde_json::from_str(json).unwrap();
        let store = prefix_store();
        check_filter(&clause, &store).unwrap();
        evaluate_filter(&clause, &store, 8).iter().collect()
    }

    #[test]
    fn prefix_matches_values_that_start_with_it() {
        // "ab" matches "abc" and "ab", not "a" or "xab"; a list matches on
        // any element.
        assert_eq!(eval_prefix(r#"{"prefix": {"code": "ab"}}"#), vec![0, 1, 5]);
        assert_eq!(eval_prefix(r#"{"prefix": {"code": "abc"}}"#), vec![0]);
        assert_eq!(
            eval_prefix(r#"{"prefix": {"code": "q"}}"#),
            Vec::<u32>::new()
        );
        assert_eq!(
            eval_prefix(r#"{"prefix": {"nope": "a"}}"#),
            Vec::<u32>::new()
        );
        // A list's JSON text is never matched, only its elements.
        assert_eq!(
            eval_prefix(r#"{"prefix": {"code": "["}}"#),
            Vec::<u32>::new()
        );
        // `.keyword` resolves to the stored column.
        assert_eq!(
            eval_prefix(r#"{"prefix": {"code.keyword": "ab"}}"#),
            vec![0, 1, 5]
        );
    }

    #[test]
    fn prefix_is_case_sensitive_unless_asked() {
        assert_eq!(eval_prefix(r#"{"prefix": {"code": "AB"}}"#), vec![4]);
        assert_eq!(
            eval_prefix(r#"{"prefix": {"code": {"value": "ab", "case_insensitive": true}}}"#),
            vec![0, 1, 4, 5]
        );
        assert_eq!(
            eval_prefix(r#"{"prefix": {"code": {"value": "aB", "case_insensitive": true}}}"#),
            vec![0, 1, 4, 5]
        );
        assert_eq!(
            eval_prefix(r#"{"prefix": {"code": {"value": "ab", "case_insensitive": false}}}"#),
            vec![0, 1, 5]
        );
    }

    #[test]
    fn empty_prefix_matches_exactly_what_exists_matches() {
        let store = prefix_store();
        let run = |json: &str| -> Vec<u32> {
            let clause: FilterClause = serde_json::from_str(json).unwrap();
            evaluate_filter(&clause, &store, 8).iter().collect()
        };
        let exists = run(r#"{"exists": {"field": "code"}}"#);
        assert_eq!(exists, vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(run(r#"{"prefix": {"code": ""}}"#), exists);
        assert_eq!(
            run(r#"{"prefix": {"code": {"value": "", "case_insensitive": true}}}"#),
            exists
        );
    }

    #[test]
    fn prefix_composes_inside_bool() {
        // must: prefix AND a numeric term (page 7 is docs 1 and 2).
        assert_eq!(
            eval_prefix(
                r#"{"bool": {"must": [{"prefix": {"code": "a"}}, {"term": {"page": "7"}}]}}"#
            ),
            vec![1, 2]
        );
        // must_not
        assert_eq!(
            eval_prefix(r#"{"bool": {"must_not": [{"prefix": {"code": "ab"}}]}}"#),
            vec![2, 3, 4, 6, 7]
        );
        // should
        assert_eq!(
            eval_prefix(
                r#"{"bool": {"should": [{"prefix": {"code": "x"}},
                                        {"prefix": {"code": "AB"}}]}}"#
            ),
            vec![3, 4]
        );
        // With a range, the motivating case from #199.
        assert_eq!(
            eval_prefix(
                r#"{"bool": {"filter": [{"prefix": {"code": "ab"}},
                                        {"range": {"page": {"gte": "7"}}}]}}"#
            ),
            vec![1]
        );
    }

    #[test]
    fn prefix_on_a_numeric_field_is_rejected() {
        let store = prefix_store();
        for json in [
            r#"{"prefix": {"page": "1"}}"#,
            r#"{"prefix": {"score": "2"}}"#,
            r#"{"prefix": {"page.keyword": "1"}}"#,
            r#"{"bool": {"must_not": [{"prefix": {"page": "1"}}]}}"#,
            r#"{"bool": {"should": [{"term": {"code": "a"}}, {"prefix": {"page": "1"}}]}}"#,
        ] {
            let clause: FilterClause = serde_json::from_str(json).unwrap();
            let err = check_filter(&clause, &store).expect_err(json);
            assert!(err.contains("numeric field"), "{json}: {err}");
        }
        // A field the segment does not have is not an error: another
        // segment may hold it as a string.
        let clause: FilterClause = serde_json::from_str(r#"{"prefix": {"nope": "1"}}"#).unwrap();
        assert!(check_filter(&clause, &store).is_ok());
        // Other clauses on numeric fields are unaffected.
        let clause: FilterClause = serde_json::from_str(r#"{"term": {"page": "7"}}"#).unwrap();
        assert!(check_filter(&clause, &store).is_ok());
    }
}

#[cfg(test)]
mod glob_tests {
    use super::*;

    fn glob(json: &str) -> GlobFilter {
        match serde_json::from_str::<FilterClause>(json).unwrap() {
            FilterClause::Glob { glob } => glob,
            other => panic!("expected Glob, got {other:?}"),
        }
    }

    #[test]
    fn glob_syntax_is_path_aware() {
        for (pattern, value, want) in [
            ("/contracts/*.pdf", "/contracts/a.pdf", true),
            ("/contracts/*.pdf", "/contracts/2024/a.pdf", false),
            ("/contracts/**/*.pdf", "/contracts/2024/q1/a.pdf", true),
            ("/contracts/**/*.pdf", "/contracts/a.pdf", true),
            ("**/contracts/**", "/x/contracts/y/z.txt", true),
            ("**/contracts/**", "/x/contract/y", false),
            ("/a/**", "/a", false),
            ("/a/?.txt", "/a/b.txt", true),
            ("/a/?.txt", "/a/bb.txt", false),
            ("/a/?.txt", "/a//.txt", false),
            ("/a/[bc].txt", "/a/c.txt", true),
            ("/a/[!bc].txt", "/a/c.txt", false),
            ("/a/*.{pdf,docx}", "/a/x.docx", true),
            ("/a/*.{pdf,docx}", "/a/x.txt", false),
            ("/a/\\*.txt", "/a/*.txt", true),
            ("/a/\\*.txt", "/a/b.txt", false),
            ("/A/*", "/a/b", false),
            // Classes never match `/`, negated or not.
            ("/docs/*[!x]*.pdf", "/docs/2026/a.pdf", false),
            ("/docs/*[!x]*.pdf", "/docs/a.pdf", true),
            ("/a[ -~]b", "/a/b", false),
            ("/a[ -~]b", "/a-b", true),
            ("/a/[]x].txt", "/a/].txt", true),
            ("/a/[a-c-].txt", "/a/-.txt", true),
            ("/a/{,x}y", "/a/y", true),
            // Characters, not bytes.
            ("/docs/[é]*.pdf", "/docs/ê.pdf", false),
            ("/docs/[é]*.pdf", "/docs/é1.pdf", true),
            ("/docs/?.pdf", "/docs/é.pdf", true),
        ] {
            let g = GlobFilter::new("path", pattern, false).unwrap();
            assert_eq!(g.is_match(value), want, "{pattern} vs {value}");
        }
        let ci = GlobFilter::new("path", "/A/*.PDF", true).unwrap();
        assert!(ci.is_match("/a/x.pdf"));
        assert_eq!(ci.literal_prefix(), "", "folding case has no byte prefix");
        let ci = GlobFilter::new("path", "/É/*", true).unwrap();
        assert!(ci.is_match("/é/x") && !ci.is_match("/e/x"));
    }

    #[test]
    fn literal_prefix_is_a_prefix_of_every_match() {
        for (pattern, prefix) in [
            ("/contracts/**/*.pdf", "/contracts/"),
            ("**/x", ""),
            ("/a/b.txt", "/a/b.txt"),
            ("/a/{b,c}", "/a/"),
            ("/a/[bc]", "/a/"),
            ("/a?", "/a"),
            ("/a\\*", "/a"),
        ] {
            let g = GlobFilter::new("path", pattern, false).unwrap();
            assert_eq!(g.literal_prefix(), prefix, "{pattern}");
        }
        let values = [
            "/a", "/a/", "/a/b", "/a/b.txt", "/a/c", "/ab", "/a*", "/a/x/y", "b",
        ];
        for pattern in [
            "/a*", "/a/**", "/a/{b,c}", "/a?", "/a/**/y", "/a\\*", "/a/*",
        ] {
            let g = GlobFilter::new("path", pattern, false).unwrap();
            for v in values {
                if g.is_match(v) {
                    assert!(v.starts_with(g.literal_prefix()), "{pattern} matched {v}");
                }
            }
        }
    }

    #[test]
    fn glob_parses_round_trips_and_rejects_malformed() {
        let g = glob(r#"{"glob": {"path": "**/*.pdf"}}"#);
        assert_eq!((g.field.as_str(), g.pattern.as_str()), ("path", "**/*.pdf"));
        assert!(!g.case_insensitive);
        let g = glob(r#"{"glob": {"path": {"value": "/A/*", "case_insensitive": true}}}"#);
        assert!(g.case_insensitive && g.is_match("/a/b"));
        for json in [
            r#"{"glob":{"path":"/a/**"}}"#,
            r#"{"glob":{"path":{"value":"/A/*","case_insensitive":true}}}"#,
        ] {
            let parsed: FilterClause = serde_json::from_str(json).unwrap();
            let out = serde_json::to_string(&parsed).unwrap();
            let again: FilterClause = serde_json::from_str(&out).unwrap();
            assert_eq!(serde_json::to_string(&again).unwrap(), out, "{json}");
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&out).unwrap(),
                serde_json::from_str::<serde_json::Value>(json).unwrap()
            );
        }
        for (json, needle) in [
            (r#"{"glob": {"path": 7}}"#, "must be a string"),
            (r#"{"glob": {"a": "x", "b": "y"}}"#, "exactly one field"),
            (r#"{"glob": {}}"#, "no field"),
            (
                r#"{"glob": {"path": {"value": "/a", "boost": 2}}}"#,
                "unknown glob option",
            ),
            (r#"{"glob": {"path": {}}}"#, "missing `value`"),
            (r#"{"glob": {"path": "/a/[b"}}"#, "invalid pattern"),
            (r#"{"glob": {"path": "/a/{b"}}"#, "invalid pattern"),
            (r#"{"glob": {"path": "/a/{b,{c}}"}}"#, "nested"),
            (r#"{"glob": {"path": "/a/[z-a]"}}"#, "invalid pattern"),
            (r#"{"glob": {"path": "/a\\"}}"#, "dangling"),
        ] {
            let err = serde_json::from_str::<FilterClause>(json).unwrap_err();
            assert!(err.to_string().contains(needle), "{json}: {err}");
        }
    }

    #[test]
    fn hostile_globs_are_rejected_without_exhausting_the_stack() {
        let deep = format!("{}{}", "{a,".repeat(50_000), "}".repeat(50_000));
        assert!(GlobFilter::new("p", deep, false)
            .unwrap_err()
            .contains("longer"));
        let deep = format!("{}{}", "{a,".repeat(1_000), "}".repeat(1_000));
        assert!(GlobFilter::new("p", deep, false)
            .unwrap_err()
            .contains("nested"));
        let wide = "[a-z]".repeat(crate::MAX_GLOB_PATTERN_BYTES / 5);
        assert!(GlobFilter::new("p", wide, false).is_ok());
    }

    #[test]
    fn glob_evaluates_scalars_and_list_elements_and_rejects_numbers() {
        let mut store = FilterStore::default();
        store.string_fields.insert(
            "path".into(),
            vec![
                (0, "/c/a.pdf".into()),
                (1, "/c/b.txt".into()),
                (2, r#"["/x","/c/z.pdf"]"#.into()),
                (3, "[]".into()),
            ],
        );
        store.integer_fields.insert("page".into(), vec![(0, 1)]);
        let g = FilterClause::Glob {
            glob: GlobFilter::new("path", "/c/*.pdf", false).unwrap(),
        };
        let set = evaluate_filter(&g, &store, 4);
        assert_eq!(set.iter().collect::<Vec<_>>(), vec![0, 2]);
        let all = FilterClause::Glob {
            glob: GlobFilter::new("path", "**", false).unwrap(),
        };
        assert_eq!(
            evaluate_filter(&all, &store, 4).iter().collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let numeric = FilterClause::Glob {
            glob: GlobFilter::new("page", "1*", false).unwrap(),
        };
        assert!(check_filter(&numeric, &store)
            .unwrap_err()
            .contains("numeric field"));
    }
}
