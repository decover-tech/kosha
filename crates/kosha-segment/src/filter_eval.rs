//! Filter evaluation straight from the columnar filter index (#244).
//!
//! [`kosha_core::evaluate_filter`] answers a clause by scanning every
//! `(doc_seq, value)` pair of the fields it names in a [`FilterStore`]. On
//! a columnar segment the same answer comes from the column structures:
//!
//! - `term` / `terms` on a string column: a binary search of the sorted
//!   dictionary, then that term's roaring bitmap. A list value (JSON text
//!   such as `["a","b"]`) also matches per element, through the column's
//!   [`ListIndex`](crate::filter_columns::ListIndex).
//! - `term` / `terms` on a numeric column: the zone maps rule out blocks
//!   whose `[min, max]` holds none of the wanted values; only the other
//!   blocks are scanned.
//! - `range` on a numeric column: the zone maps rule out blocks outside
//!   the range, take blocks entirely inside it without a scan (integers),
//!   and scan only the blocks that straddle a bound.
//! - `range` on a string column: the dictionary's ordinal range, then
//!   either those terms' bitmaps or one pass over the ordinals when the
//!   range spans many terms.
//! - `exists`: the presence bitmap (less documents whose only values are
//!   empty lists).
//! - `bool`: set algebra over the results; `must_not` subtracts from the
//!   all-documents set.
//!
//! The result is the segment's [`DocSet`], the same type the legacy path
//! builds, so callers (the scoring loops' pre-filter, delete-by-query, a
//! later filtered vector probe) do not care which format answered.
//!
//! Every rule mirrors `kosha_core::docset` exactly, field-name resolution
//! (`x.keyword` falls back to `x`) and the per-kind precedence included,
//! so the two formats return identical sets. The differential tests in
//! this crate and in kosha-query hold them to that.

use std::collections::HashSet;
use std::sync::Arc;

use kosha_core::{BoolFilter, DocSet, FilterClause, FilterColumnKind, KoshaError, RangeBound};

use crate::filter_columns::{ColumnValue, FilterColumn, NumericColumn, StringColumn, ZONE_BLOCK};

/// Where the evaluator gets columns from. `Ok(None)` means the segment has
/// no such column, so it holds no values. A column the segment has but
/// whose file is not local must be an error, never `Ok(None)`: read as
/// "no values" it silently drops the segment's matches.
pub trait ColumnSource {
    fn column(
        &self,
        name: &str,
        kind: FilterColumnKind,
    ) -> Result<Option<Arc<FilterColumn>>, KoshaError>;
}

/// `field`'s column of `kind`, or its base name's (`x.keyword` -> `x`) when
/// the exact name has none — `kosha_core::lookup_field` over columns.
pub fn resolve_column(
    source: &dyn ColumnSource,
    field: &str,
    kind: FilterColumnKind,
) -> Result<Option<Arc<FilterColumn>>, KoshaError> {
    if let Some(column) = source.column(field, kind)? {
        return Ok(Some(column));
    }
    let base = kosha_core::base_field_name(field);
    if base.len() == field.len() {
        return Ok(None);
    }
    source.column(base, kind)
}

fn string_column(
    source: &dyn ColumnSource,
    field: &str,
) -> Result<Option<Arc<FilterColumn>>, KoshaError> {
    resolve_column(source, field, FilterColumnKind::String)
}

/// The documents of a `doc_count`-document segment that pass `clause`.
/// Reject a `prefix` on a numeric field anywhere in `clause`, as
/// `kosha_core::check_filter` does for `filters.bin` segments, even where
/// evaluation would short-circuit past it (an empty `must`). Reads only
/// the columns `prefix` clauses name.
pub fn check_filter_columns(
    clause: &FilterClause,
    source: &dyn ColumnSource,
) -> Result<(), KoshaError> {
    match clause {
        FilterClause::Prefix { prefix } => {
            if string_column(source, &prefix.field)?.is_some() {
                return Ok(());
            }
            for kind in [FilterColumnKind::Integer, FilterColumnKind::Float] {
                if resolve_column(source, &prefix.field, kind)?.is_some() {
                    return Err(KoshaError::InvalidQuery(
                        kosha_core::docset::prefix_on_numeric_error(&prefix.field),
                    ));
                }
            }
            Ok(())
        }
        FilterClause::Bool { bool: b } => b
            .must
            .iter()
            .chain(&b.must_not)
            .chain(&b.should)
            .try_for_each(|child| check_filter_columns(child, source)),
        _ => Ok(()),
    }
}

pub fn evaluate_filter_columns(
    clause: &FilterClause,
    source: &dyn ColumnSource,
    doc_count: u32,
) -> Result<DocSet, KoshaError> {
    Ok(match clause {
        FilterClause::Term { term } => {
            // Several fields in one `term` object are alternatives (OR).
            let mut set = DocSet::empty(doc_count);
            for (field, value) in term {
                mark_equal(&mut set, source, field, std::slice::from_ref(value))?;
            }
            set
        }
        FilterClause::Terms { terms } => {
            let mut set = DocSet::empty(doc_count);
            for (field, values) in terms {
                mark_equal(&mut set, source, field, values)?;
            }
            set
        }
        FilterClause::Range { range } => {
            let mut set = DocSet::empty(doc_count);
            for (field, bound) in range {
                mark_in_range(&mut set, source, field, bound)?;
            }
            set
        }
        FilterClause::Bool { bool: b } => evaluate_bool(b, source, doc_count)?,
        FilterClause::Prefix { prefix } => {
            // `kosha_core::docset`'s rule over the dictionary: each distinct
            // term is tested once (list terms on their elements) and its
            // documents taken from the term's bitmap. A numeric field is a
            // client error, as `kosha_core::check_filter` reports it.
            let mut set = DocSet::empty(doc_count);
            match string_column(source, &prefix.field)? {
                Some(column) => {
                    if let FilterColumn::String(c) = &*column {
                        for ord in 0..c.term_count() as u32 {
                            if kosha_core::docset::prefix_matches(prefix, c.term(ord)) {
                                c.for_each_doc(ord, |doc| set.insert(doc));
                            }
                        }
                    }
                }
                None => {
                    for kind in [FilterColumnKind::Integer, FilterColumnKind::Float] {
                        if resolve_column(source, &prefix.field, kind)?.is_some() {
                            return Err(KoshaError::InvalidQuery(
                                kosha_core::docset::prefix_on_numeric_error(&prefix.field),
                            ));
                        }
                    }
                }
            }
            set
        }
        FilterClause::MatchAll { .. } => DocSet::full(doc_count),
        FilterClause::Exists { exists } => {
            let mut set = DocSet::empty(doc_count);
            let field = exists.field.as_str();
            if let Some(column) = string_column(source, field)? {
                if let FilterColumn::String(c) = &*column {
                    mark_string_exists(&mut set, c);
                }
            }
            for kind in [FilterColumnKind::Integer, FilterColumnKind::Float] {
                if let Some(column) = resolve_column(source, field, kind)? {
                    set.insert_bitmap(column.present());
                }
            }
            set
        }
        FilterClause::GeoDistance { geo_distance: geo } => {
            // `kosha_core::docset`'s rule over the dictionary: `GeoPoint`
            // values are canonical "lat,lon" strings, so each distinct
            // point is tested once and its documents taken from the
            // term's bitmap. Terms that do not parse as a point never
            // match.
            let mut set = DocSet::empty(doc_count);
            if let Some(column) = string_column(source, &geo.field)? {
                if let FilterColumn::String(c) = &*column {
                    for ord in 0..c.term_count() as u32 {
                        let within = kosha_core::rank::parse_geo_str(c.term(ord)).is_some_and(
                            |(lat, lon)| {
                                kosha_core::rank::haversine_meters(geo.lat, geo.lon, lat, lon)
                                    <= geo.distance
                            },
                        );
                        if within {
                            c.for_each_doc(ord, |doc| set.insert(doc));
                        }
                    }
                }
            }
            set
        }
    })
}

/// An empty list is "no value", as in OpenSearch.
fn mark_string_exists(set: &mut DocSet, column: &StringColumn) {
    let empty = &column.list_index().empty;
    if empty.is_empty() {
        set.insert_bitmap(column.present());
        return;
    }
    for (&doc, ord) in column.docs().iter().zip(column.ords()) {
        if empty.binary_search(ord).is_err() {
            set.insert(doc);
        }
    }
}

/// `kosha_core::docset::mark_equal` over columns: the string column if
/// the field has one, else the integer column, else the float column.
fn mark_equal(
    set: &mut DocSet,
    source: &dyn ColumnSource,
    field: &str,
    values: &[String],
) -> Result<(), KoshaError> {
    if let Some(column) = string_column(source, field)? {
        if let FilterColumn::String(c) = &*column {
            let mut ords: Vec<u32> = Vec::new();
            let lists = c.list_index();
            for value in values {
                ords.extend(c.ordinal(value));
                if let Some(list_ords) = lists.elements.get(value.as_str()) {
                    ords.extend_from_slice(list_ords);
                }
            }
            ords.sort_unstable();
            ords.dedup();
            for ord in ords {
                c.for_each_doc(ord, |doc| set.insert(doc));
            }
        }
    } else if let Some(column) = resolve_column(source, field, FilterColumnKind::Integer)? {
        if let FilterColumn::Integer(c) = &*column {
            let wanted: HashSet<i64> = values
                .iter()
                .filter_map(|v| v.trim().parse().ok())
                .collect();
            if !wanted.is_empty() {
                mark_numeric(
                    set,
                    c,
                    |lo, hi| wanted.iter().any(|w| lo <= *w && *w <= hi),
                    |v| wanted.contains(&v),
                );
            }
        }
    } else if let Some(column) = resolve_column(source, field, FilterColumnKind::Float)? {
        if let FilterColumn::Float(c) = &*column {
            let wanted: Vec<f64> = values
                .iter()
                .filter_map(|v| v.trim().parse().ok())
                .collect();
            if !wanted.is_empty() {
                mark_numeric(
                    set,
                    c,
                    // An all-NaN block has NaN bounds and is skipped; NaN
                    // never equals anything, so nothing is lost.
                    |lo, hi| wanted.iter().any(|w| lo <= *w && *w <= hi),
                    |v| wanted.contains(&v),
                );
            }
        }
    }
    Ok(())
}

/// Scan the blocks of `column` that `block_may_match(min, max)` keeps,
/// marking entries that pass `matches`.
fn mark_numeric<T: ColumnValue>(
    set: &mut DocSet,
    column: &NumericColumn<T>,
    block_may_match: impl Fn(T, T) -> bool,
    matches: impl Fn(T) -> bool,
) {
    let (docs, values) = (column.docs(), column.values());
    for (block, &(lo, hi)) in column.zones().iter().enumerate() {
        if !block_may_match(lo, hi) {
            continue;
        }
        let start = block * ZONE_BLOCK;
        let end = (start + ZONE_BLOCK).min(values.len());
        for i in start..end {
            if matches(values[i]) {
                set.insert(docs[i]);
            }
        }
    }
}

/// `kosha_core::docset::mark_in_range` over columns: integer, else float,
/// else string. A bound that does not parse as the column's type is
/// ignored, as it always has been.
fn mark_in_range(
    set: &mut DocSet,
    source: &dyn ColumnSource,
    field: &str,
    bound: &RangeBound,
) -> Result<(), KoshaError> {
    if let Some(column) = resolve_column(source, field, FilterColumnKind::Integer)? {
        if let FilterColumn::Integer(c) = &*column {
            mark_integer_range(set, c, bound);
        }
    } else if let Some(column) = resolve_column(source, field, FilterColumnKind::Float)? {
        if let FilterColumn::Float(c) = &*column {
            mark_float_range(set, c, bound);
        }
    } else if let Some(column) = string_column(source, field)? {
        if let FilterColumn::String(c) = &*column {
            mark_string_range(set, c, bound);
        }
    }
    Ok(())
}

fn mark_integer_range(set: &mut DocSet, column: &NumericColumn<i64>, bound: &RangeBound) {
    let parse = |s: &Option<String>| s.as_ref().and_then(|s| s.trim().parse::<i64>().ok());
    // Fold the four bounds into one inclusive `[lo, hi]`.
    let mut lo = i64::MIN;
    let mut hi = i64::MAX;
    if let Some(b) = parse(&bound.gte) {
        lo = lo.max(b);
    }
    if let Some(b) = parse(&bound.gt) {
        match b.checked_add(1) {
            Some(b) => lo = lo.max(b),
            None => return, // `> i64::MAX`: nothing
        }
    }
    if let Some(b) = parse(&bound.lte) {
        hi = hi.min(b);
    }
    if let Some(b) = parse(&bound.lt) {
        match b.checked_sub(1) {
            Some(b) => hi = hi.min(b),
            None => return, // `< i64::MIN`: nothing
        }
    }
    if lo > hi {
        return;
    }
    let (docs, values) = (column.docs(), column.values());
    for (block, &(zmin, zmax)) in column.zones().iter().enumerate() {
        if zmax < lo || zmin > hi {
            continue;
        }
        let start = block * ZONE_BLOCK;
        let end = (start + ZONE_BLOCK).min(values.len());
        if lo <= zmin && zmax <= hi {
            // The whole block is inside the range: no value comparisons.
            for &doc in &docs[start..end] {
                set.insert(doc);
            }
            continue;
        }
        for i in start..end {
            let v = values[i];
            if lo <= v && v <= hi {
                set.insert(docs[i]);
            }
        }
    }
}

fn mark_float_range(set: &mut DocSet, column: &NumericColumn<f64>, bound: &RangeBound) {
    let parse = |s: &Option<String>| s.as_ref().and_then(|s| s.trim().parse::<f64>().ok());
    let (gte, gt, lte, lt) = (
        parse(&bound.gte),
        parse(&bound.gt),
        parse(&bound.lte),
        parse(&bound.lt),
    );
    let matches = |v: f64| {
        gte.is_none_or(|b| v >= b)
            && gt.is_none_or(|b| v > b)
            && lte.is_none_or(|b| v <= b)
            && lt.is_none_or(|b| v < b)
    };
    // A block can be skipped only when its (NaN-free) bounds prove no
    // value passes. NaN zone bounds compare false, so such blocks are kept
    // and scanned, which is always correct. Blocks are never taken whole:
    // a zone ignores NaN, and NaN fails every bound.
    let block_may_match = |zmin: f64, zmax: f64| {
        !(gte.is_some_and(|b| zmax < b)
            || gt.is_some_and(|b| zmax <= b)
            || lte.is_some_and(|b| zmin > b)
            || lt.is_some_and(|b| zmin >= b))
    };
    mark_numeric(set, column, block_may_match, matches);
}

/// Above this many terms in a string range, one pass over the per-entry
/// ordinals beats decoding a bitmap per term.
const RANGE_BITMAP_TERMS: u32 = 64;

fn mark_string_range(set: &mut DocSet, column: &StringColumn, bound: &RangeBound) {
    // Each bound narrows the ordinal range independently (byte order is
    // the dictionary's order and `str`'s), then they intersect.
    let mut range = 0..column.term_count() as u32;
    let mut narrow = |r: std::ops::Range<u32>| {
        range = range.start.max(r.start)..range.end.min(r.end);
    };
    if let Some(b) = bound.gte.as_deref() {
        narrow(column.ordinal_range(Some((b, true)), None));
    }
    if let Some(b) = bound.gt.as_deref() {
        narrow(column.ordinal_range(Some((b, false)), None));
    }
    if let Some(b) = bound.lte.as_deref() {
        narrow(column.ordinal_range(None, Some((b, true))));
    }
    if let Some(b) = bound.lt.as_deref() {
        narrow(column.ordinal_range(None, Some((b, false))));
    }
    let lists = column.list_index();
    if !lists.lists.is_empty() || !lists.empty.is_empty() {
        // A list term is in range when any of its elements is (#206), as
        // in `kosha_core::docset::mark_in_range`; its own JSON text is not
        // compared, also for a nested container with no plain element.
        let list_ords: HashSet<u32> = lists
            .lists
            .iter()
            .chain(&lists.empty)
            .copied()
            .filter(|ord| column.term(*ord).starts_with('['))
            .collect();
        let mut ords: Vec<u32> = Vec::new();
        if range.end.saturating_sub(range.start) <= RANGE_BITMAP_TERMS {
            ords.extend(range.clone().filter(|ord| !list_ords.contains(ord)));
        } else if range.start < range.end {
            // As below: one pass over the entries beats a bitmap per term.
            for (&doc, ord) in column.docs().iter().zip(column.ords()) {
                if range.contains(ord) && !list_ords.contains(ord) {
                    set.insert(doc);
                }
            }
        }
        for (element, list) in &lists.elements {
            if in_range_str(element, bound) {
                ords.extend_from_slice(list);
            }
        }
        ords.sort_unstable();
        ords.dedup();
        for ord in ords {
            column.for_each_doc(ord, |doc| set.insert(doc));
        }
        return;
    }
    if range.start >= range.end {
        return;
    }
    if range.end - range.start <= RANGE_BITMAP_TERMS {
        for ord in range {
            column.for_each_doc(ord, |doc| set.insert(doc));
        }
    } else {
        for (&doc, ord) in column.docs().iter().zip(column.ords()) {
            if range.contains(ord) {
                set.insert(doc);
            }
        }
    }
}

/// `kosha_core::docset`'s string range test, on one value.
fn in_range_str(value: &str, bound: &RangeBound) -> bool {
    bound.gte.as_deref().is_none_or(|b| value >= b)
        && bound.gt.as_deref().is_none_or(|b| value > b)
        && bound.lte.as_deref().is_none_or(|b| value <= b)
        && bound.lt.as_deref().is_none_or(|b| value < b)
}

/// `must` narrows, `must_not` subtracts, and a non-empty `should` requires
/// at least `max(1, minimum_should_match)` of its clauses — exactly
/// `kosha_core::docset::evaluate_bool`.
fn evaluate_bool(
    b: &BoolFilter,
    source: &dyn ColumnSource,
    doc_count: u32,
) -> Result<DocSet, KoshaError> {
    let mut set = DocSet::full(doc_count);
    for clause in &b.must {
        set.intersect_with(&evaluate_filter_columns(clause, source, doc_count)?);
        if set.is_empty() {
            return Ok(set);
        }
    }
    for clause in &b.must_not {
        set.subtract(&evaluate_filter_columns(clause, source, doc_count)?);
    }
    if !b.should.is_empty() {
        let need = b.minimum_should_match.max(1);
        if need == 1 {
            let mut any = DocSet::empty(doc_count);
            for clause in &b.should {
                any.union_with(&evaluate_filter_columns(clause, source, doc_count)?);
            }
            set.intersect_with(&any);
        } else {
            let mut matched = vec![0u32; doc_count as usize];
            for clause in &b.should {
                for doc_seq in evaluate_filter_columns(clause, source, doc_count)?.iter() {
                    matched[doc_seq as usize] += 1;
                }
            }
            let mut enough = DocSet::empty(doc_count);
            for (doc_seq, count) in matched.into_iter().enumerate() {
                if count as usize >= need {
                    enough.insert(doc_seq as u32);
                }
            }
            set.intersect_with(&enough);
        }
    }
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SegmentReader, SegmentWriter};
    use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};
    use std::path::{Path, PathBuf};

    const DOCS: u32 = 10_000;

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("kosha-test-feval-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// A corpus over several zone-map blocks with every case the rules
    /// distinguish: list values (with empty and odd lists), multi-valued
    /// fields, a field with two kinds, sparse and near-unique fields,
    /// NaN/inf/-0.0 floats and i64 extremes.
    fn corpus() -> Vec<(DocumentId, Vec<Field>)> {
        let tags = [
            r#"["urgent","sealed"]"#,
            r#"["draft"]"#,
            "[]",
            "urgent",
            " []",
            "[null]",
            "[1, true]",
            r#"["a", "urgent"]"#,
        ];
        (0..DOCS)
            .map(|i| {
                let mut fields = vec![
                    Field::text("t", format!("word {i}")),
                    Field::keyword("matterId", format!("m{}", i % 7)),
                    Field::keyword("id", format!("id-{i:05}")),
                    Field::integer("seq", i as i64),
                    Field::integer("page", (i % 400) as i64),
                    Field::boolean("inScope", i % 3 != 0),
                    Field::date_val(
                        "createdAt",
                        format!("2024-{:02}-{:02}", i % 12 + 1, i % 28 + 1),
                    ),
                ];
                let conf = match i {
                    _ if i % 97 == 0 => f64::NAN,
                    _ if i % 89 == 0 => f64::INFINITY,
                    _ if i % 83 == 0 => f64::NEG_INFINITY,
                    _ if i % 79 == 0 => -0.0,
                    _ => (i % 100) as f64 / 100.0,
                };
                fields.push(Field::float_val("conf", conf));
                if i % 8 != 7 {
                    fields.push(Field::keyword("tags", tags[(i % 8) as usize]));
                }
                if i % 11 == 0 {
                    fields.push(Field::keyword("matterId", "extra"));
                    fields.push(Field::integer("page", 9_000));
                }
                if i % 500 == 0 {
                    fields.push(Field::keyword("sparse", "rare"));
                }
                if i == 4321 {
                    fields.push(Field::integer("extreme", i64::MAX));
                } else if i == 1234 {
                    fields.push(Field::integer("extreme", i64::MIN));
                }
                // A nested container (list of objects) beside plain values.
                match i % 5 {
                    0 => fields.push(Field::keyword("authors", r#"[{"name":"Ada"}]"#)),
                    1 => fields.push(Field::keyword("authors", format!("Bob{i}"))),
                    _ => {}
                }
                // A high-cardinality column holding a few lists.
                if i % 1000 == 0 {
                    fields.push(Field::keyword("codes", format!(r#"["c-{:05}"]"#, i + 3)));
                } else {
                    fields.push(Field::keyword("codes", format!("c-{i:05}")));
                }
                if i % 5 == 0 {
                    fields.push(Field::integer("mixed", (i % 13) as i64));
                } else {
                    fields.push(Field::keyword("mixed", format!("k{}", i % 13)));
                }
                (DocumentId(format!("d{i}")), fields)
            })
            .collect()
    }

    fn build(dir: &Path, version: u32, docs: &[(DocumentId, Vec<Field>)]) -> SegmentReader {
        let mut w = SegmentWriter::new(SegmentId("s".into()), dir.to_path_buf())
            .with_filters_write_version(version);
        for (id, fields) in docs {
            w.add_document(id.clone(), fields.clone());
        }
        w.finalize(Bm25Params::default()).unwrap();
        SegmentReader::open(dir.to_path_buf()).unwrap()
    }

    fn clauses() -> Vec<serde_json::Value> {
        use serde_json::json;
        vec![
            json!({"term": {"matterId": "m3"}}),
            json!({"term": {"matterId.keyword": "extra"}}),
            json!({"term": {"matterId.raw": "nope"}}),
            json!({"terms": {"matterId": ["m1", "m2", "zz", "extra"]}}),
            json!({"term": {"id": "id-00042"}}),
            json!({"terms": {"id": ["id-00001", "id-09999", "id-10000"]}}),
            json!({"term": {"tags": "urgent"}}),
            json!({"term": {"tags.keyword": "sealed"}}),
            json!({"terms": {"tags": ["draft", "a", "1", "true"]}}),
            json!({"term": {"tags": "[]"}}),
            json!({"term": {"tags": "null"}}),
            json!({"exists": {"field": "tags"}}),
            json!({"exists": {"field": "tags.keyword"}}),
            json!({"exists": {"field": "page"}}),
            json!({"exists": {"field": "conf"}}),
            json!({"exists": {"field": "mixed"}}),
            json!({"exists": {"field": "nope"}}),
            json!({"term": {"page": "7"}}),
            json!({"term": {"page": " 9000 "}}),
            json!({"terms": {"page": ["1", "399", "x", "7.5"]}}),
            json!({"term": {"page": "seven"}}),
            json!({"term": {"conf": "0.5"}}),
            json!({"term": {"conf": "0"}}),
            json!({"term": {"conf": "-0"}}),
            json!({"term": {"conf": "NaN"}}),
            json!({"term": {"conf": "inf"}}),
            json!({"term": {"inScope": "true"}}),
            json!({"term": {"mixed": "3"}}),
            json!({"term": {"mixed": "k3"}}),
            json!({"range": {"mixed": {"gte": "5"}}}),
            json!({"range": {"seq": {"gte": "100", "lt": "8200"}}}),
            json!({"range": {"seq": {"gt": "4095", "lte": "4096"}}}),
            json!({"range": {"seq": {"gt": "9999"}}}),
            json!({"range": {"seq": {"lt": "0"}}}),
            json!({"range": {"seq": {}}}),
            json!({"range": {"seq": {"gte": "7.5"}}}),
            json!({"range": {"seq.keyword": {"lte": "17"}}}),
            json!({"range": {"page": {"gte": "390", "lte": "9000"}}}),
            json!({"range": {"extreme": {"gt": "9223372036854775807"}}}),
            json!({"range": {"extreme": {"lt": "-9223372036854775808"}}}),
            json!({"range": {"extreme": {"gte": "0"}}}),
            json!({"range": {"conf": {"gte": "0.25", "lt": "0.5"}}}),
            json!({"range": {"conf": {"gt": "0.98"}}}),
            json!({"range": {"conf": {"lte": "-1"}}}),
            json!({"range": {"conf": {"gte": "NaN"}}}),
            json!({"range": {"conf": {}}}),
            json!({"range": {"conf": {"gte": "x"}}}),
            json!({"range": {"createdAt": {"gte": "2024-03-01", "lt": "2024-05-01"}}}),
            json!({"range": {"id": {"gte": "id-00100", "lte": "id-00130"}}}),
            json!({"range": {"id": {"gt": "id-00100"}}}),
            json!({"range": {"id": {"gt": "z"}}}),
            json!({"range": {"matterId": {"gte": "m2", "lt": "m5"}}}),
            json!({"range": {"tags": {"gte": "[", "lt": "\\"}}}),
            json!({"range": {"authors": {"gte": "A", "lt": "z"}}}),
            json!({"range": {"authors": {"gte": "[", "lt": "\\"}}}),
            json!({"range": {"codes": {"gte": "c-00100", "lt": "c-09000"}}}),
            json!({"range": {"codes": {"gte": "[", "lte": "c-00010"}}}),
            json!({"match_all": {}}),
            json!({"bool": {}}),
            json!({"bool": {"must": [{"term": {"matterId": "m1"}}, {"range": {"page": {"lt": "50"}}}]}}),
            json!({"bool": {"must_not": [{"term": {"inScope": "true"}}]}}),
            json!({"bool": {"must_not": [{"exists": {"field": "sparse"}}]}}),
            json!({"bool": {"should": [{"term": {"matterId": "m1"}}, {"term": {"tags": "draft"}}]}}),
            json!({"bool": {"should": [{"term": {"matterId": "m1"}}, {"term": {"tags": "draft"}},
                                       {"range": {"seq": {"lt": "3000"}}}],
                            "minimum_should_match": 2}}),
            json!({"bool": {"filter": [{"bool": {"should": [{"term": {"sparse": "rare"}},
                                                            {"term": {"id": "id-00003"}}]}}],
                            "must_not": [{"range": {"seq": {"gte": "9000"}}}]}}),
            json!({"bool": {"must": [{"term": {"matterId": "nope"}}],
                            "should": [{"term": {"matterId": "m1"}}]}}),
            json!({"term": {"matterId": "m1", "tags": "draft"}}),
        ]
    }

    #[test]
    fn columnar_evaluation_matches_the_row_scan_for_every_clause() {
        let dir = temp_dir("diff");
        let docs = corpus();
        let legacy = build(&dir.join("v1"), 1, &docs);
        let columnar = build(&dir.join("v2"), 2, &docs);
        assert!(!legacy.has_filter_columns() && columnar.has_filter_columns());
        for json in clauses() {
            let clause: FilterClause = serde_json::from_value(json.clone()).unwrap();
            let want = legacy.evaluate_filter(&clause).unwrap();
            let got = columnar.evaluate_filter(&clause).unwrap();
            assert_eq!(
                got.iter().collect::<Vec<_>>(),
                want.iter().collect::<Vec<_>>(),
                "clause {json}"
            );
            // And the row scan over the columnar segment's own store.
            let rows = kosha_core::evaluate_filter(
                &clause,
                &columnar.try_filter_store().unwrap(),
                columnar.doc_count(),
            );
            assert_eq!(got, rows, "clause {json} (rows of the v2 segment)");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn exists_matches_indexed_values_and_must_not_matches_the_complement() {
        use serde_json::json;

        let dir = temp_dir("exists");
        let docs = [
            (
                DocumentId("present".into()),
                vec![
                    Field::text("body", ""),
                    Field::keyword("tag", ""),
                    Field::integer("page", 0),
                    Field::float_val("confidence", 0.0),
                    Field::boolean("active", false),
                    Field::date_val("sentAt", "2026-01-01"),
                    Field::geo_point("location", 0.0, 0.0),
                ],
            ),
            (
                DocumentId("missing".into()),
                vec![Field::keyword("other", "value")],
            ),
            (
                DocumentId("empty_list".into()),
                vec![Field::keyword("tag", "[]")],
            ),
        ];
        for version in [1, 2] {
            let reader = build(&dir.join(format!("v{version}")), version, &docs);
            let matches = |filter| {
                let clause = serde_json::from_value(filter).unwrap();
                reader
                    .evaluate_filter(&clause)
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            };
            for field in [
                "body",
                "tag",
                "page",
                "confidence",
                "active",
                "sentAt",
                "location",
            ] {
                let exists = json!({"exists": {"field": field}});
                assert_eq!(matches(exists.clone()), [0], "v{version}, {field}");
                assert_eq!(
                    matches(json!({"bool": {"must_not": [exists]}})),
                    [1, 2],
                    "v{version}, {field}"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn clauses_select_documents() {
        // Guard against a vacuous differential test: the corpus must make
        // the interesting clauses non-trivial.
        let dir = temp_dir("nontrivial");
        let columnar = build(&dir, 2, &corpus());
        let count = |json: serde_json::Value| {
            let clause: FilterClause = serde_json::from_value(json).unwrap();
            columnar.evaluate_filter(&clause).unwrap().count()
        };
        use serde_json::json;
        assert_eq!(count(json!({"term": {"id": "id-00042"}})), 1);
        assert_eq!(
            count(json!({"range": {"seq": {"gte": "100", "lt": "8200"}}})),
            8100
        );
        assert_eq!(
            count(json!({"range": {"seq": {"gt": "4095", "lte": "4096"}}})),
            1
        );
        // "urgent" plain and inside a list (docs i%8==7 carry no tags, so
        // the third spelling never lands).
        assert_eq!(count(json!({"term": {"tags": "urgent"}})), 2 * 1250);
        assert_eq!(count(json!({"term": {"tags": "true"}})), 1250);
        // `[]`, ` []` and `[null]` are not values; docs i%8==7 have none.
        assert_eq!(count(json!({"exists": {"field": "tags"}})), 4 * 1250);
        assert_eq!(count(json!({"range": {"extreme": {"gte": "0"}}})), 1);
        // Only the plain "Bob…" values: a nested container's own JSON text
        // is never compared, as in `kosha_core::evaluate_filter`.
        assert_eq!(
            count(json!({"range": {"authors": {"gte": "A", "lt": "z"}}})),
            2000
        );
        // Plain c-00100..c-08999 minus the 8 lists among them, plus the
        // lists whose element (i + 3) is in range: i = 1000..8000.
        assert_eq!(
            count(json!({"range": {"codes": {"gte": "c-00100", "lt": "c-09000"}}})),
            8900 - 8 + 8
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_and_damaged_columns() {
        let dir = temp_dir("missing");
        let columnar = build(&dir, 2, &corpus());
        let footer = columnar.footer().clone();
        let meta = footer
            .filter_columns
            .as_ref()
            .unwrap()
            .iter()
            .find(|c| c.name == "matterId")
            .unwrap()
            .clone();
        let path = dir.join(meta.file_name());
        let bytes = std::fs::read(&path).unwrap();
        let clause: FilterClause =
            serde_json::from_value(serde_json::json!({"term": {"matterId": "m1"}})).unwrap();

        // Not local: an error (never "matches nothing"), and not cached.
        std::fs::remove_file(&path).unwrap();
        let fresh = SegmentReader::open(dir.clone()).unwrap();
        assert!(matches!(
            fresh.evaluate_filter(&clause),
            Err(KoshaError::NotFound(_))
        ));
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(fresh.evaluate_filter(&clause).unwrap().count(), 1429);

        // Damaged: fails closed.
        std::fs::write(&path, &bytes[..bytes.len() - 3]).unwrap();
        let fresh = SegmentReader::open(dir.clone()).unwrap();
        assert!(matches!(
            fresh.evaluate_filter(&clause),
            Err(KoshaError::CorruptSegment(_))
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn list_index_and_ordinal_ranges() {
        let entries: Vec<(u32, String)> = [
            r#"["a","b"]"#,
            "[]",
            " [\"x\"]",
            "[null]",
            "a",
            "b",
            r#"["b"]"#,
        ]
        .iter()
        .enumerate()
        .map(|(i, v)| (i as u32, v.to_string()))
        .collect();
        let c = StringColumn::from_entries(&entries).unwrap();
        let index = c.list_index();
        let terms = |ords: &[u32]| ords.iter().map(|o| c.term(*o)).collect::<Vec<_>>();
        assert_eq!(
            terms(&index.elements["b"]),
            vec![r#"["a","b"]"#, r#"["b"]"#]
        );
        assert_eq!(terms(&index.elements["a"]), vec![r#"["a","b"]"#]);
        // Leading space: not matched per element, but its emptiness counts.
        assert!(!index.elements.contains_key("x"));
        assert_eq!(terms(&index.empty), vec!["[]", "[null]"]);

        let all = c.term_count() as u32;
        assert_eq!(c.ordinal_range(None, None), 0..all);
        let r = c.ordinal_range(Some(("a", true)), Some(("b", true)));
        assert_eq!(terms(&r.collect::<Vec<_>>()), vec!["a", "b"]);
        let r = c.ordinal_range(Some(("a", false)), Some(("b", false)));
        assert!(r.is_empty());
        let r = c.ordinal_range(Some(("zz", true)), Some(("a", true)));
        assert!(r.is_empty());
        let mut docs = Vec::new();
        c.for_each_doc(c.ordinal("a").unwrap(), |d| docs.push(d));
        assert_eq!(docs, vec![4]);
    }
}
