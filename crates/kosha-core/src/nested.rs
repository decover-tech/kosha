//! Nested attributes: objects and arrays inside a document (#206).
//!
//! Kosha's wire model is flat: every field is `{name, field_type, value}`
//! with a string value. A *nested container* is a `Keyword` field whose
//! value is the JSON text of an object (`{"name":"Ada","org":"acme"}`) or
//! of an array that holds objects or arrays
//! (`[{"name":"Ada"},{"name":"Bob"}]`). Arrays of plain values
//! (`["a","b"]`) are not containers: they already work as multi-valued
//! fields through [`crate::list_elements`].
//!
//! At segment build the writer flattens a container's leaves into filter
//! columns named by dotted path (`author.name`), and only into the filter
//! columns. The stored document keeps the field exactly as it was sent, so
//! `_source` round-trips the nesting. Filters, sorts and aggregations then
//! address a path like any other field name.
//!
//! Semantics are *flattened*, like Elasticsearch's `object` type rather
//! than `nested`: the leaves of every element of an array of objects are
//! pooled per path. For
//! `authors: [{name: Ada, org: acme}, {name: Bob, org: beta}]`,
//! `authors.name = Ada AND authors.org = beta` matches, even though no
//! single author is both.
//!
//! Storage per path, per document:
//! - all leaves numbers: one float-column entry per value (a document can
//!   have several entries). Numbers always go to the float column, never
//!   the integer one, so a path cannot be split across two column types
//!   from one document to the next.
//! - otherwise (strings, booleans, or a mix): the string column. One value
//!   is stored as itself; several are stored as their JSON-list text
//!   (`["Ada","Bob"]`), the multi-value convention the filter and
//!   aggregation code already read element by element. Numbers in a mixed
//!   path keep their JSON text; booleans are `true` / `false`.
//! - `null`, `{}` and `[]` contribute no value. Arrays inside arrays are
//!   flattened.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::{Aggregation, FilterClause, SearchQuery};

/// The values one dotted path holds in one document, ready for a filter
/// column. See the module docs for the encoding.
#[derive(Debug, Clone, PartialEq)]
pub enum NestedColumn {
    /// A string-column value: a scalar, or the JSON-list text of several.
    Text(String),
    /// Float-column values, one entry each.
    Numbers(Vec<f64>),
}

/// The parsed value of a nested container, or `None` when `value` is not
/// one. Only a value that starts with `{` or `[` is parsed at all.
pub fn parse_container(value: &str) -> Option<Value> {
    let trimmed = value.trim();
    let first = trimmed.as_bytes().first()?;
    if *first != b'{' && *first != b'[' {
        return None;
    }
    match serde_json::from_str::<Value>(trimmed).ok()? {
        v @ Value::Object(_) => Some(v),
        Value::Array(items) if items.iter().any(|i| i.is_object() || i.is_array()) => {
            Some(Value::Array(items))
        }
        _ => None,
    }
}

enum Leaf<'a> {
    Str(&'a str),
    Num(&'a serde_json::Number),
    Bool(bool),
}

fn collect_leaves<'a>(value: &'a Value, path: &str, out: &mut BTreeMap<String, Vec<Leaf<'a>>>) {
    match value {
        Value::Null => {}
        Value::String(s) => out.entry(path.to_string()).or_default().push(Leaf::Str(s)),
        Value::Number(n) => out.entry(path.to_string()).or_default().push(Leaf::Num(n)),
        Value::Bool(b) => out
            .entry(path.to_string())
            .or_default()
            .push(Leaf::Bool(*b)),
        Value::Array(items) => {
            for item in items {
                collect_leaves(item, path, out);
            }
        }
        Value::Object(map) => {
            for (key, child) in map {
                collect_leaves(child, &format!("{path}.{key}"), out);
            }
        }
    }
}

/// The dotted-path columns a field contributes, or an empty list when the
/// field is not a nested container. Paths come back sorted.
///
/// Plain values sitting directly in a top-level array next to objects
/// (`[1, {"a": 2}]`) belong to the container's own name, which the
/// container's own column already covers; they are not repeated here.
pub fn flatten_container(name: &str, value: &str) -> Vec<(String, NestedColumn)> {
    let Some(parsed) = parse_container(value) else {
        return Vec::new();
    };
    let mut leaves = BTreeMap::new();
    collect_leaves(&parsed, name, &mut leaves);
    leaves
        .into_iter()
        .filter(|(path, _)| path != name)
        .map(|(path, values)| {
            let column = encode_column(&values);
            (path, column)
        })
        .collect()
}

fn encode_column(values: &[Leaf<'_>]) -> NestedColumn {
    if values.iter().all(|v| matches!(v, Leaf::Num(_))) {
        return NestedColumn::Numbers(
            values
                .iter()
                .filter_map(|v| match v {
                    Leaf::Num(n) => n.as_f64().filter(|f| f.is_finite()),
                    _ => None,
                })
                .collect(),
        );
    }
    let strings: Vec<String> = values
        .iter()
        .map(|v| match v {
            Leaf::Str(s) => (*s).to_string(),
            Leaf::Num(n) => n.to_string(),
            Leaf::Bool(b) => b.to_string(),
        })
        .collect();
    match strings.as_slice() {
        // A lone string that itself looks like a JSON list would be read
        // back as that list's elements; wrap it so it stays one value.
        [one] if !looks_like_list(one) => NestedColumn::Text(one.clone()),
        _ => NestedColumn::Text(serde_json::to_string(&strings).unwrap_or_default()),
    }
}

fn looks_like_list(s: &str) -> bool {
    let t = s.trim();
    t.starts_with('[') && t.ends_with(']')
}

/// The string leaves at `path` inside a container, for highlighting a
/// dotted path. `path` is relative to the container (`bio` for
/// `author.bio` when the container is `author`).
pub fn leaf_strings(container: &Value, path: &str) -> Vec<String> {
    let mut leaves = BTreeMap::new();
    collect_leaves(container, "", &mut leaves);
    let key = format!(".{path}");
    leaves
        .remove(&key)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|leaf| match leaf {
            Leaf::Str(s) => Some(s.to_string()),
            _ => None,
        })
        .collect()
}

// ─── Field references in a query ──────────────────────────────────────────

fn filter_paths<'a>(clause: &'a FilterClause, out: &mut Vec<&'a str>) {
    match clause {
        FilterClause::Term { term } => out.extend(term.keys().map(String::as_str)),
        FilterClause::Terms { terms } => out.extend(terms.keys().map(String::as_str)),
        FilterClause::Range { range } => out.extend(range.keys().map(String::as_str)),
        FilterClause::Exists { exists } => out.push(&exists.field),
        FilterClause::MatchAll { .. } => {}
        FilterClause::Bool { bool: b } => {
            for c in b.must.iter().chain(&b.should).chain(&b.must_not) {
                filter_paths(c, out);
            }
        }
    }
}

fn aggregation_paths<'a>(agg: &'a Aggregation, out: &mut Vec<&'a str>) {
    match agg {
        Aggregation::Terms { terms, .. } => out.push(&terms.field),
        Aggregation::Cardinality { cardinality } => out.push(&cardinality.field),
        Aggregation::Composite { composite, .. } => {
            for source in &composite.sources {
                out.extend(source.source.values().map(|s| s.terms.field.as_str()));
            }
        }
        _ => {
            if let Some((_, metric)) = agg.metric() {
                out.push(&metric.field);
            }
        }
    }
    for sub in agg.sub_aggregations().into_iter().flat_map(|s| s.values()) {
        aggregation_paths(sub, out);
    }
}

impl SearchQuery {
    /// Every document field this query names in a filter (including a
    /// kNN pre-filter), sort, aggregation, collapse or highlight, in that
    /// order, without duplicates. `_score` and `_id` are not fields.
    pub fn field_paths(&self) -> Vec<&str> {
        let mut out = Vec::new();
        if let Some(filter) = &self.filter {
            filter_paths(filter, &mut out);
        }
        if let Some(filter) = self.knn.as_ref().and_then(|k| k.filter.as_ref()) {
            filter_paths(filter, &mut out);
        }
        for spec in &self.sort {
            out.extend(spec.fields.keys().map(String::as_str));
        }
        let mut names: Vec<&String> = self.aggs.keys().collect();
        names.sort();
        for name in names {
            aggregation_paths(&self.aggs[name], &mut out);
        }
        if let Some(collapse) = &self.collapse {
            out.push(&collapse.field);
        }
        if let Some(highlight) = &self.highlight {
            out.push(&highlight.field);
        }
        let mut seen = std::collections::HashSet::new();
        out.retain(|f| *f != "_score" && *f != "_id" && seen.insert(*f));
        out
    }

    /// Rejects a field path the namespace's schema does not know, for a
    /// namespace that has one. `known` answers for a stored name;
    /// `<field>.keyword` / `.raw` resolve to `<field>` first, as they do
    /// everywhere else (see [`crate::base_field_name`]). A namespace
    /// without a schema should not call this: Kosha stays permissive there.
    pub fn validate_field_paths(&self, known: impl Fn(&str) -> bool) -> Result<(), String> {
        for path in self.field_paths() {
            if !known(path) && !known(crate::base_field_name(path)) {
                return Err(format!(
                    "unknown field path {path:?}: not in the namespace schema"
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(name: &str, json: &str) -> Vec<(String, NestedColumn)> {
        flatten_container(name, json)
    }

    #[test]
    fn object_flattens_to_dotted_paths() {
        assert_eq!(
            cols(
                "author",
                r#"{"name":"Ada","org":"acme","age":36,"active":true,"x":null,
                   "addr":{"city":"London"}}"#
            ),
            vec![
                ("author.active".into(), NestedColumn::Text("true".into())),
                (
                    "author.addr.city".into(),
                    NestedColumn::Text("London".into())
                ),
                ("author.age".into(), NestedColumn::Numbers(vec![36.0])),
                ("author.name".into(), NestedColumn::Text("Ada".into())),
                ("author.org".into(), NestedColumn::Text("acme".into())),
            ]
        );
    }

    #[test]
    fn array_of_objects_pools_leaves_per_path() {
        assert_eq!(
            cols(
                "authors",
                r#"[{"name":"Ada","org":"acme","n":1},{"name":"Bob","org":"beta","n":2.5},
                    {"tags":["x",["y"]]}]"#
            ),
            vec![
                ("authors.n".into(), NestedColumn::Numbers(vec![1.0, 2.5])),
                (
                    "authors.name".into(),
                    NestedColumn::Text(r#"["Ada","Bob"]"#.into())
                ),
                (
                    "authors.org".into(),
                    NestedColumn::Text(r#"["acme","beta"]"#.into())
                ),
                (
                    "authors.tags".into(),
                    NestedColumn::Text(r#"["x","y"]"#.into())
                ),
            ]
        );
    }

    #[test]
    fn mixed_types_fall_back_to_strings_and_lone_list_text_is_wrapped() {
        assert_eq!(
            cols("o", r#"{"v":[1,"a"],"w":"[not a list]"}"#),
            vec![
                ("o.v".into(), NestedColumn::Text(r#"["1","a"]"#.into())),
                (
                    "o.w".into(),
                    NestedColumn::Text(r#"["[not a list]"]"#.into())
                ),
            ]
        );
    }

    #[test]
    fn non_containers_contribute_nothing() {
        for value in ["plain", r#"["a","b"]"#, "[1,2]", "[]", "{not json", "7", ""] {
            assert!(cols("f", value).is_empty(), "{value}");
            assert!(parse_container(value).is_none(), "{value}");
        }
        assert!(cols("f", "{}").is_empty());
        // Scalars beside objects in a top-level array stay with the
        // container's own column.
        assert_eq!(
            cols("f", r#"[1, {"a": 2}]"#),
            vec![("f.a".into(), NestedColumn::Numbers(vec![2.0]))]
        );
    }

    #[test]
    fn leaf_strings_resolve_a_relative_path() {
        let v: Value =
            serde_json::from_str(r#"[{"bio":"likes cats"},{"bio":"likes dogs","n":1}]"#).unwrap();
        assert_eq!(leaf_strings(&v, "bio"), vec!["likes cats", "likes dogs"]);
        assert!(leaf_strings(&v, "n").is_empty());
        assert!(leaf_strings(&v, "nope").is_empty());
    }

    #[test]
    fn field_paths_and_validation() {
        let query: SearchQuery = serde_json::from_value(serde_json::json!({
            "query_text": "x",
            "filter": {"bool": {"must": [{"term": {"author.name": "Ada"}}],
                                "must_not": [{"exists": {"field": "author.org"}}]}},
            "sort": [{"author.age": {"order": "desc"}}, {"_id": {"order": "asc"}}],
            "aggs": {"by": {"terms": {"field": "tags.keyword"},
                            "aggs": {"m": {"max": {"field": "author.age"}}}}},
            "highlight": {"field": "author.bio"}
        }))
        .unwrap();
        assert_eq!(
            query.field_paths(),
            vec![
                "author.name",
                "author.org",
                "author.age",
                "tags.keyword",
                "author.bio"
            ]
        );
        let schema = [
            "author.name",
            "author.org",
            "author.age",
            "tags",
            "author.bio",
        ];
        assert!(query.validate_field_paths(|p| schema.contains(&p)).is_ok());
        let err = query
            .validate_field_paths(|p| p != "author.org" && schema.contains(&p))
            .unwrap_err();
        assert!(err.contains("author.org"), "{err}");
    }
}
