//! `kosha search` flags and their mapping onto [`SearchQuery`].
//!
//! Every flag here is an alternative to setting the same field in a
//! `--body` document, so the whole struct is one clap group that `--body`
//! conflicts with. The mapping itself ([`build_search_query`]) is pure, so
//! the flag → `SearchQuery` contract is unit-tested without a server.

use std::collections::HashMap;
use std::fs;

use clap::Args;
use kosha_core::{
    Aggregation, FilterClause, HighlightConfig, KnnQuery, QueryOperator, SearchQuery, SearchResult,
    SortOrder, SortSpec,
};
use serde_json::Value;

/// One `--sort` flag: `FIELD`, `FIELD:asc` or `FIELD:desc`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    pub field: String,
    pub descending: bool,
}

impl SortKey {
    fn order(&self) -> &'static str {
        if self.descending {
            "desc"
        } else {
            "asc"
        }
    }
}

/// clap value parser for `--sort`. A bare field sorts ascending; the only
/// suffixes are `:asc` and `:desc` (any case). Anything else is rejected
/// here rather than passed through for the server to misread.
pub fn parse_sort_key(raw: &str) -> Result<SortKey, String> {
    let raw = raw.trim();
    let (field, descending) = match raw.rsplit_once(':') {
        Some((field, order)) => match order.to_ascii_lowercase().as_str() {
            "asc" => (field, false),
            "desc" => (field, true),
            _ => {
                return Err(format!(
                    "invalid sort order {order:?} (expected FIELD, FIELD:asc or FIELD:desc)"
                ))
            }
        },
        None => (raw, false),
    };
    if field.trim().is_empty() {
        return Err("sort field is empty (expected FIELD, FIELD:asc or FIELD:desc)".into());
    }
    Ok(SortKey {
        field: field.trim().to_string(),
        descending,
    })
}

/// The `kosha search` flags that map onto `SearchQuery` fields. `--max`
/// and the positional query sit outside the group: `--max` predates it and
/// has always been allowed next to `--body`.
#[derive(Debug, Clone, Default, Args)]
#[group(id = "search_flags", multiple = true)]
pub struct SearchArgs {
    /// Filter clause JSON (inline or @file), e.g. '{"term":{"matterId":"m1"}}'
    #[arg(long, value_name = "JSON")]
    pub filter: Option<String>,

    /// Force an exact total_hits count instead of the default capped (gte)
    /// count (SearchQuery.exact_total_hits).
    #[arg(long)]
    pub exact: bool,

    /// Match a document containing ANY query term instead of requiring
    /// every term (SearchQuery.operator = "or").
    #[arg(long)]
    pub or: bool,

    /// Skip this many hits before the page (SearchQuery.from). Ignored by
    /// the server when --search-after is set.
    #[arg(long, value_name = "N")]
    pub from: Option<usize>,

    /// Sort key, repeatable, applied in order: FIELD (ascending),
    /// FIELD:asc or FIELD:desc. `_score` and `_id` are built in.
    #[arg(long, value_name = "FIELD[:asc|:desc]", value_parser = parse_sort_key)]
    pub sort: Vec<SortKey>,

    /// Cursor value, repeatable: one per --sort flag, in the same order
    /// (or a single `_id` value when there is no --sort). Plain output
    /// prints the cursor for the next page.
    #[arg(long = "search-after", value_name = "VALUE")]
    pub search_after: Vec<String>,

    /// Vector field to run a kNN search on. With a query string as well,
    /// the request is hybrid (lexical + kNN fused by RRF).
    #[arg(long, value_name = "FIELD", requires = "knn_vector")]
    pub knn_field: Option<String>,

    /// Query vector: an inline JSON array or @file.json
    #[arg(long, value_name = "JSON|@FILE", requires = "knn_field")]
    pub knn_vector: Option<String>,

    /// Nearest neighbours to retrieve (default: --from + --max)
    #[arg(long, value_name = "N", requires = "knn_field")]
    pub k: Option<usize>,

    /// Size of the kNN candidate list (default: max(100, k))
    #[arg(long, value_name = "N", requires = "knn_field")]
    pub num_candidates: Option<usize>,

    /// Filter clause JSON applied to the kNN leg only (inline or @file)
    #[arg(long, value_name = "JSON", requires = "knn_field")]
    pub knn_filter: Option<String>,

    /// Return highlighted fragments of this field with each hit
    #[arg(long, value_name = "FIELD")]
    pub highlight: Option<String>,

    /// Tag inserted before each highlighted term (default <b>)
    #[arg(long, value_name = "TAG", requires = "highlight")]
    pub pre_tag: Option<String>,

    /// Tag inserted after each highlighted term (default </b>)
    #[arg(long, value_name = "TAG", requires = "highlight")]
    pub post_tag: Option<String>,

    /// Aggregations JSON object (inline or @file), the same shape as
    /// SearchQuery.aggs, e.g. '{"by_court":{"terms":{"field":"court"}}}'
    #[arg(long, value_name = "JSON|@FILE")]
    pub aggs: Option<String>,

    /// Bypass the server's result cache for this request
    /// (SearchQuery.no_cache)
    #[arg(long)]
    pub no_cache: bool,
}

/// Read a JSON argument: `@path` reads the file, anything else is parsed
/// inline. The convention `--body` has always used.
pub fn read_json_arg(src: &str) -> Result<Value, String> {
    if let Some(path) = src.strip_prefix('@') {
        let text = fs::read_to_string(path).map_err(|e| format!("cannot read {path}: {e}"))?;
        serde_json::from_str(&text).map_err(|e| format!("{path}: invalid JSON: {e}"))
    } else {
        serde_json::from_str(src).map_err(|e| format!("invalid JSON: {e}"))
    }
}

fn parse_json_flag<T: serde::de::DeserializeOwned>(flag: &str, src: &str) -> Result<T, String> {
    let value = read_json_arg(src).map_err(|e| format!("{flag}: {e}"))?;
    serde_json::from_value(value).map_err(|e| format!("{flag}: {e}"))
}

/// Build the `SearchQuery` the flags describe. `Err` is a usage error
/// (bad JSON, a cursor that does not line up with the sort keys) and is
/// reported before any request is sent.
///
/// Unset flags leave their field at the engine default: `exact` and `or`
/// map to `None` rather than an eager `Some(false)` / `Some(And)`, so the
/// server's own defaults still apply.
pub fn build_search_query(
    query_text: String,
    max_results: usize,
    args: &SearchArgs,
) -> Result<SearchQuery, String> {
    let from = args.from.unwrap_or(0);

    let sort: Vec<SortSpec> = args
        .sort
        .iter()
        .map(|key| SortSpec {
            fields: HashMap::from([(
                key.field.clone(),
                SortOrder {
                    order: key.order().to_string(),
                },
            )]),
        })
        .collect();

    let search_after = if args.search_after.is_empty() {
        None
    } else {
        let expected = sort.len().max(1);
        if args.search_after.len() != expected {
            return Err(if sort.is_empty() {
                format!(
                    "--search-after was given {} times; without --sort the cursor is a \
                     single _id value",
                    args.search_after.len()
                )
            } else {
                format!(
                    "--search-after was given {} times but --sort {} times; the cursor \
                     needs one value per sort key, in the same order",
                    args.search_after.len(),
                    sort.len()
                )
            });
        }
        Some(args.search_after.clone())
    };

    let filter = args
        .filter
        .as_deref()
        .map(|src| parse_json_flag::<FilterClause>("--filter", src))
        .transpose()?;

    let knn = match (&args.knn_field, &args.knn_vector) {
        (Some(field), Some(vector_src)) => {
            let vector: Vec<f32> = parse_json_flag("--knn-vector", vector_src)
                .map_err(|e| format!("{e} (expected a JSON array of numbers)"))?;
            if vector.is_empty() {
                return Err("--knn-vector: the vector is empty".into());
            }
            let k = args.k.unwrap_or((from + max_results).max(1));
            if k == 0 {
                return Err("--k must be at least 1".into());
            }
            let num_candidates = args.num_candidates.unwrap_or(k.max(100));
            let filter = args
                .knn_filter
                .as_deref()
                .map(|src| parse_json_flag::<FilterClause>("--knn-filter", src))
                .transpose()?;
            Some(KnnQuery {
                query_text: None,
                embedding_model: None,
                field: field.clone(),
                vector,
                k,
                num_candidates,
                filter,
                vectors: None,
                scoring: None,
            })
        }
        // clap's `requires` makes these unreachable from the command line.
        (Some(_), None) => return Err("--knn-field needs --knn-vector".into()),
        (None, Some(_)) => return Err("--knn-vector needs --knn-field".into()),
        (None, None) => None,
    };

    // Built from the wire shape rather than a struct literal: the request
    // JSON (`field`, `pre_tags`, `post_tags`) is the stable contract, and an
    // unset tag is left out so the server's default applies.
    let highlight = match &args.highlight {
        Some(field) => {
            let mut wire = serde_json::json!({ "field": field });
            if let Some(tag) = &args.pre_tag {
                wire["pre_tags"] = serde_json::json!([tag]);
            }
            if let Some(tag) = &args.post_tag {
                wire["post_tags"] = serde_json::json!([tag]);
            }
            Some(
                serde_json::from_value::<HighlightConfig>(wire)
                    .map_err(|e| format!("--highlight: {e}"))?,
            )
        }
        None => None,
    };

    let aggs: HashMap<String, Aggregation> = match args.aggs.as_deref() {
        Some(src) => parse_json_flag("--aggs", src)?,
        None => HashMap::new(),
    };

    Ok(SearchQuery {
        query_text,
        max_results,
        from,
        bm25_params: Default::default(),
        filter,
        sort,
        search_after,
        highlight,
        aggs,
        wildcard: None,
        match_phrase: None,
        knn,
        exact_total_hits: args.exact.then_some(true),
        total_hits_cap: None,
        operator: args.or.then_some(QueryOperator::Or),
        no_cache: args.no_cache.then_some(true),
        collapse: None,
        source_includes: None,
        source_excludes: None,
        rank: None,
        mode: None,
        fields: Default::default(),
        fuzzy: Default::default(),
        limit_per: Default::default(),
        rank_by: Default::default(),
        rerank: Default::default(),
    })
}

/// The `search_after` cursor that continues after `result`'s last hit:
/// one value per sort key, read off that hit (`_id` is the document id,
/// `_score` the score, anything else the stored field, empty when the
/// document has none — the server's spelling of "missing").
///
/// `None` without sort keys (a score-ordered page has no stable cursor;
/// page with `--from` instead) or when the page is not full, since a short
/// page is the last one.
pub fn next_search_after(
    result: &SearchResult,
    sort: &[SortSpec],
    max_results: usize,
) -> Option<Vec<String>> {
    if sort.is_empty() || result.results.is_empty() || result.results.len() < max_results {
        return None;
    }
    let last = result.results.last()?;
    let values = sort
        .iter()
        .flat_map(|spec| spec.fields.keys())
        .map(|field| match field.as_str() {
            "_id" => last.doc_id.0.clone(),
            "_score" => last.score.to_string(),
            name => last
                .fields
                .iter()
                .find(|f| f.name == name)
                .map(|f| f.value.clone())
                .unwrap_or_default(),
        })
        .collect();
    Some(values)
}

/// Quote a value for a POSIX shell so the printed cursor can be pasted.
pub fn shell_quote(value: &str) -> String {
    let safe = !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.:/@+,=".contains(c));
    if safe {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', r"'\''"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::{DocumentId, Field, ScoredDocument, TotalHitsRelation};

    fn args() -> SearchArgs {
        SearchArgs::default()
    }

    fn sort_pairs(query: &SearchQuery) -> Vec<(String, String)> {
        query
            .sort
            .iter()
            .flat_map(|spec| spec.fields.iter())
            .map(|(f, o)| (f.clone(), o.order.clone()))
            .collect()
    }

    #[test]
    fn sort_key_parsing() {
        assert_eq!(
            parse_sort_key("created_at").unwrap(),
            SortKey {
                field: "created_at".into(),
                descending: false
            }
        );
        assert!(!parse_sort_key("created_at:asc").unwrap().descending);
        assert!(parse_sort_key("created_at:desc").unwrap().descending);
        assert!(parse_sort_key("created_at:DESC").unwrap().descending);
        // Only the last colon is the order separator.
        assert_eq!(parse_sort_key("a:b:desc").unwrap().field, "a:b");
        let err = parse_sort_key("created_at:descending").unwrap_err();
        assert!(err.contains("descending"), "{err}");
        assert!(parse_sort_key("created_at:").is_err());
        assert!(parse_sort_key(":desc").is_err());
        assert!(parse_sort_key("").is_err());
    }

    #[test]
    fn sort_flags_map_to_sort_specs_in_order() {
        let mut a = args();
        a.sort = vec![
            parse_sort_key("pageNumber:desc").unwrap(),
            parse_sort_key("_id").unwrap(),
        ];
        let q = build_search_query("contract".into(), 4, &a).unwrap();
        assert_eq!(
            sort_pairs(&q),
            vec![
                ("pageNumber".to_string(), "desc".to_string()),
                ("_id".to_string(), "asc".to_string())
            ]
        );
        // Serialized as the server expects: [{"pageNumber":{"order":"desc"}}, ...]
        let json = serde_json::to_value(&q).unwrap();
        assert_eq!(
            json["sort"],
            serde_json::json!([{"pageNumber": {"order": "desc"}}, {"_id": {"order": "asc"}}])
        );
    }

    #[test]
    fn search_after_count_must_match_sort_count() {
        let mut a = args();
        a.sort = vec![
            parse_sort_key("pageNumber:desc").unwrap(),
            parse_sort_key("_id").unwrap(),
        ];
        a.search_after = vec!["9".into()];
        let err = build_search_query("q".into(), 10, &a).unwrap_err();
        assert!(err.contains("1 times but --sort 2 times"), "{err}");

        a.search_after = vec!["9".into(), "s1-docA-p09".into()];
        let q = build_search_query("q".into(), 10, &a).unwrap();
        assert_eq!(
            q.search_after,
            Some(vec!["9".to_string(), "s1-docA-p09".to_string()])
        );

        // Without --sort the cursor is one _id.
        let mut a = args();
        a.search_after = vec!["d1".into(), "d2".into()];
        assert!(build_search_query("q".into(), 10, &a)
            .unwrap_err()
            .contains("single _id"));
        a.search_after = vec!["d1".into()];
        assert_eq!(
            build_search_query("q".into(), 10, &a).unwrap().search_after,
            Some(vec!["d1".to_string()])
        );

        // No cursor at all stays None, not Some(vec![]).
        assert_eq!(
            build_search_query("q".into(), 10, &args())
                .unwrap()
                .search_after,
            None
        );
    }

    #[test]
    fn from_flag_maps_to_from() {
        let mut a = args();
        assert_eq!(build_search_query("q".into(), 10, &a).unwrap().from, 0);
        a.from = Some(20);
        assert_eq!(build_search_query("q".into(), 10, &a).unwrap().from, 20);
    }

    #[test]
    fn knn_vector_inline_and_at_file() {
        let mut a = args();
        a.knn_field = Some("embedding".into());
        a.knn_vector = Some("[0.1, 0.2, 0.3]".into());
        let q = build_search_query(String::new(), 5, &a).unwrap();
        let knn = q.knn.clone().expect("knn set");
        assert_eq!(knn.field, "embedding");
        assert_eq!(knn.vector, vec![0.1, 0.2, 0.3]);
        // Defaults: k fills the page, num_candidates at least 100.
        assert_eq!(knn.k, 5);
        assert_eq!(knn.num_candidates, 100);
        assert!(knn.filter.is_none());
        assert!(!q.is_hybrid());

        let dir = std::env::temp_dir().join(format!("kosha-cli-knn-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("vec.json");
        std::fs::write(&path, "[1, 0, -1.5]\n").unwrap();
        a.knn_vector = Some(format!("@{}", path.display()));
        a.k = Some(7);
        a.num_candidates = Some(50);
        a.knn_filter = Some(r#"{"term":{"court":"9th"}}"#.into());
        let q = build_search_query("breach".into(), 5, &a).unwrap();
        let knn = q.knn.as_ref().unwrap();
        assert_eq!(knn.vector, vec![1.0, 0.0, -1.5]);
        assert_eq!(knn.k, 7);
        assert_eq!(knn.num_candidates, 50);
        assert!(knn.filter.is_some());
        assert!(q.is_hybrid(), "query text + knn is a hybrid request");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn knn_k_default_covers_from_plus_max() {
        let mut a = args();
        a.knn_field = Some("v".into());
        a.knn_vector = Some("[1]".into());
        a.from = Some(150);
        let knn = build_search_query(String::new(), 10, &a)
            .unwrap()
            .knn
            .unwrap();
        assert_eq!(knn.k, 160);
        assert_eq!(knn.num_candidates, 160);
    }

    #[test]
    fn knn_vector_errors_are_clear() {
        let mut a = args();
        a.knn_field = Some("v".into());
        a.knn_vector = Some(r#"["a"]"#.into());
        assert!(build_search_query(String::new(), 5, &a)
            .unwrap_err()
            .contains("JSON array of numbers"));
        a.knn_vector = Some("[]".into());
        assert!(build_search_query(String::new(), 5, &a)
            .unwrap_err()
            .contains("empty"));
        a.knn_vector = Some("@/definitely/not/here.json".into());
        assert!(build_search_query(String::new(), 5, &a)
            .unwrap_err()
            .contains("cannot read"));
    }

    #[test]
    fn highlight_aggs_and_no_cache() {
        let mut a = args();
        a.highlight = Some("content".into());
        let q = build_search_query("q".into(), 10, &a).unwrap();
        let wire = serde_json::to_string(&q.highlight).unwrap();
        assert!(wire.contains("\"content\""), "{wire}");
        // No tags given: none are sent, so the server's default applies.
        assert!(!wire.contains("pre_tags"), "default tags: {wire}");

        a.pre_tag = Some("<em>".into());
        a.post_tag = Some("</em>".into());
        a.aggs = Some(r#"{"by_court":{"terms":{"field":"court","size":5}}}"#.into());
        a.no_cache = true;
        let q = build_search_query("q".into(), 10, &a).unwrap();
        let wire = serde_json::to_string(&q.highlight).unwrap();
        assert!(wire.contains("\"<em>\""), "{wire}");
        assert!(wire.contains("\"</em>\""), "{wire}");
        assert!(!wire.contains("<b>"), "{wire}");
        assert!(q.aggs.contains_key("by_court"));
        assert_eq!(q.no_cache, Some(true));

        assert_eq!(
            build_search_query("q".into(), 10, &args())
                .unwrap()
                .no_cache,
            None
        );

        a.aggs = Some("[1]".into());
        assert!(build_search_query("q".into(), 10, &a)
            .unwrap_err()
            .starts_with("--aggs"));
    }

    fn hit(id: &str, score: f64, fields: &[(&str, &str)]) -> ScoredDocument {
        ScoredDocument {
            doc_id: DocumentId(id.into()),
            score,
            fields: fields.iter().map(|(n, v)| Field::text(*n, *v)).collect(),
            highlights: None,
            rank_details: None,
            fused_score: Default::default(),
            highlight: Default::default(),
            rerank_score: Default::default(),
        }
    }

    fn result(hits: Vec<ScoredDocument>) -> SearchResult {
        SearchResult {
            total_hits: hits.len(),
            results: hits,
            total_hits_relation: TotalHitsRelation::Eq,
            aggregations: None,
            knn_degraded_segments: None,
            rerank_degraded: Default::default(),
            rerank_degraded_reason: Default::default(),
            knn_mode: None,
            knn_filtered_candidates: None,
            exhaustive_search_count: 0,
        }
    }

    #[test]
    fn next_cursor_reads_the_last_hit() {
        let mut a = args();
        a.sort = vec![
            parse_sort_key("pageNumber:desc").unwrap(),
            parse_sort_key("missing").unwrap(),
            parse_sort_key("_score:desc").unwrap(),
            parse_sort_key("_id").unwrap(),
        ];
        let q = build_search_query("q".into(), 2, &a).unwrap();
        let r = result(vec![
            hit("p12", 1.0, &[("pageNumber", "12")]),
            hit("p09", 2.5, &[("pageNumber", "9")]),
        ]);
        assert_eq!(
            next_search_after(&r, &q.sort, 2),
            Some(vec![
                "9".to_string(),
                String::new(),
                "2.5".to_string(),
                "p09".to_string()
            ])
        );
        // A short page is the last page; no sort means no cursor.
        assert_eq!(next_search_after(&r, &q.sort, 3), None);
        assert_eq!(next_search_after(&r, &[], 2), None);
        assert_eq!(next_search_after(&result(vec![]), &q.sort, 0), None);
    }

    #[test]
    fn shell_quote_only_quotes_when_needed() {
        assert_eq!(shell_quote("s1-docA-p09"), "s1-docA-p09");
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
    }
}
