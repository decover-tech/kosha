//! Aggregations are scoped to the request's matches (#215).
//!
//! An aggregation counts the documents that match `query_text` and
//! `filter` — every one of them, not just the page — as Elasticsearch and
//! OpenSearch do. Each case here is checked against a brute-force count
//! over the corpus, over several segments, over row (`filters.bin`),
//! columnar (#244) and mixed layouts, and at several page sizes.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use kosha_core::{
    Bm25Params, DocumentId, Field, Manifest, ManifestEntry, NamespaceId, SearchQuery, SearchResult,
    SegmentId, TotalHitsRelation,
};
use kosha_query::Searcher;
use kosha_segment::SegmentWriter;
use serde_json::{json, Value};

/// Documents per segment and segments per layout.
const PER_SEGMENT: usize = 40;
const SEGMENTS: usize = 3;
const OWNERS: [&str; 5] = ["ana", "bo", "cy", "di", "ed"];
const REGIONS: [&str; 3] = ["north", "south", "west"];

#[derive(Clone, Debug)]
struct Doc {
    id: String,
    /// Index within its segment (its `doc_seq`).
    seq: u32,
    segment: usize,
    words: Vec<&'static str>,
    /// `open` for even documents, `closed` for odd: a filter on it keeps
    /// exactly half of the corpus.
    status: &'static str,
    owner: &'static str,
    region: &'static str,
    qty: Option<i64>,
    price: Option<f64>,
    vector: Vec<f32>,
}

/// 120 documents. Every value but `status` is a function of `i / 2`, so
/// each open document has a closed twin with the same owner, region and
/// metrics: `status: open` halves every count and sum exactly.
fn corpus() -> Vec<Doc> {
    (0..PER_SEGMENT * SEGMENTS)
        .map(|i| {
            let pair = i / 2;
            let mut words = vec!["memo"];
            if pair % 3 == 0 {
                words.push("breach");
            }
            if pair % 2 == 0 || i % 5 == 0 {
                words.push("contract");
            }
            let angle = i as f32 * 0.37;
            Doc {
                id: format!("d{i:03}"),
                seq: (i % PER_SEGMENT) as u32,
                segment: i / PER_SEGMENT,
                words,
                status: if i % 2 == 0 { "open" } else { "closed" },
                owner: OWNERS[pair % OWNERS.len()],
                region: REGIONS[(pair / 2) % REGIONS.len()],
                qty: (pair % 5 != 4).then_some((pair % 9) as i64 - 3),
                price: (pair % 4 != 3).then_some((pair % 11) as f64 * 1.25 + 0.5),
                vector: vec![angle.cos(), angle.sin(), (i % 7) as f32 * 0.1, 1.0],
            }
        })
        .collect()
}

fn temp_dir(tag: &str) -> PathBuf {
    // Tests run in parallel in one process and the clock can repeat, so a
    // per-process counter keeps two layouts from sharing (and deleting) a dir.
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "kosha-agg-scope-{tag}-{}-{seq}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn empty_manifest() -> Manifest {
    serde_json::from_value(json!({
        "version": 1,
        "segments": [],
        "wal_checkpoint": 0,
    }))
    .unwrap()
}

/// One segment per entry of `versions`, written with that filter format
/// (`1` = `filters.bin`, `2` = columnar). `segment_of` places each doc.
fn build(
    dir: &Path,
    docs: &[Doc],
    versions: &[u32],
    segment_of: impl Fn(&Doc) -> usize,
) -> Manifest {
    let ns = NamespaceId("ns".into());
    let mut manifest = empty_manifest();
    for (s, version) in versions.iter().enumerate() {
        let id = SegmentId(format!("seg{s}"));
        let mut writer = SegmentWriter::new(id.clone(), dir.join(&ns.0).join(&id.0))
            .with_filters_write_version(*version);
        for doc in docs.iter().filter(|d| segment_of(d) == s) {
            let mut fields = vec![
                Field::text("body", doc.words.join(" ")),
                Field::keyword("status", doc.status),
                Field::keyword("owner", doc.owner),
                Field::keyword("region", doc.region),
                Field::vector("emb", doc.vector.clone()),
            ];
            if let Some(qty) = doc.qty {
                fields.push(Field::integer("qty", qty));
            }
            if let Some(price) = doc.price {
                fields.push(Field::float_val("price", price));
            }
            writer.add_document(DocumentId(doc.id.clone()), fields);
        }
        let footer = writer.finalize(Bm25Params::default()).unwrap();
        assert_eq!(footer.filter_columns.is_some(), *version >= 2);
        manifest.segments.push(ManifestEntry {
            segment_id: id,
            doc_count: footer.doc_count,
        });
        manifest.remember_segment_footer(footer);
    }
    manifest
}

/// A built namespace: `name` for messages.
struct Layout {
    name: &'static str,
    dir: PathBuf,
    manifest: Manifest,
}

fn layouts(docs: &[Doc]) -> Vec<Layout> {
    let specs: [(&'static str, Vec<u32>); 4] = [
        ("row", vec![1; SEGMENTS]),
        ("columnar", vec![2; SEGMENTS]),
        ("mixed", vec![1, 2, 1]),
        ("columnar-single", vec![2]),
    ];
    specs
        .into_iter()
        .map(|(name, versions)| {
            let dir = temp_dir(name);
            let manifest = if versions.len() == 1 {
                build(&dir, docs, &versions, |_| 0)
            } else {
                build(&dir, docs, &versions, |d| d.segment)
            };
            assert_eq!(manifest.segments.len(), versions.len());
            Layout {
                name,
                dir,
                manifest,
            }
        })
        .collect()
}

fn aggs() -> Value {
    json!({
        "owners": {"terms": {"field": "owner", "size": 100}},
        "owners_kw": {"terms": {"field": "owner.keyword", "size": 100}},
        "statuses": {"terms": {"field": "status", "size": 100}},
        "distinct_owners": {"cardinality": {"field": "owner"}},
        "distinct_regions": {"cardinality": {"field": "region"}},
        "pairs": {"composite": {"size": 1000, "sources": [
            {"owner": {"terms": {"field": "owner"}}},
            {"region": {"terms": {"field": "region"}}}
        ]}},
        "qty_min": {"min": {"field": "qty"}},
        "qty_max": {"max": {"field": "qty"}},
        "qty_sum": {"sum": {"field": "qty"}},
        "qty_avg": {"avg": {"field": "qty"}},
        "qty_count": {"value_count": {"field": "qty"}},
        "price_min": {"min": {"field": "price"}},
        "price_max": {"max": {"field": "price"}},
        "price_sum": {"sum": {"field": "price"}},
        "price_avg": {"avg": {"field": "price"}},
        "price_count": {"value_count": {"field": "price"}},
    })
}

/// The scoping a request asks for, and the same predicate for brute force.
#[derive(Clone, Copy, Debug)]
enum Scope {
    /// `filter: match_all`, no text: every live document.
    Everything,
    /// Query text only: the "no filter" case, whose output is unchanged.
    Text(&'static str),
    /// A filter only.
    Filter(&'static str),
    /// Both together.
    Both(&'static str, &'static str),
}

impl Scope {
    fn keeps(self, doc: &Doc) -> bool {
        let text = |q: &str| q.split(' ').all(|w| doc.words.contains(&w));
        match self {
            Scope::Everything => true,
            Scope::Text(q) => text(q),
            Scope::Filter(status) => doc.status == status,
            Scope::Both(q, status) => text(q) && doc.status == status,
        }
    }

    fn request(self, max_results: usize, from: usize, sort: Option<Value>) -> Value {
        let mut q = json!({
            "max_results": max_results,
            "from": from,
            "aggs": aggs(),
            "exact_total_hits": true,
        });
        let (text, filter) = match self {
            Scope::Everything => ("", Some(json!({"match_all": {}}))),
            Scope::Text(t) => (t, None),
            Scope::Filter(s) => ("", Some(json!({"term": {"status": s}}))),
            Scope::Both(t, s) => (t, Some(json!({"term": {"status": s}}))),
        };
        q["query_text"] = json!(text);
        if let Some(filter) = filter {
            q["filter"] = filter;
        }
        if let Some(sort) = sort {
            q["sort"] = sort;
        }
        q
    }
}

fn scopes() -> Vec<Scope> {
    vec![
        Scope::Everything,
        Scope::Text("breach"),
        Scope::Text("contract"),
        Scope::Text("breach contract"),
        Scope::Filter("open"),
        Scope::Filter("closed"),
        Scope::Both("breach", "open"),
        Scope::Both("contract", "closed"),
        Scope::Both("breach contract", "closed"),
    ]
}

fn metric(values: &[f64]) -> [Value; 5] {
    if values.is_empty() {
        return [Value::Null, Value::Null, json!(0.0), Value::Null, json!(0)];
    }
    let sum: f64 = values.iter().sum();
    [
        json!(values.iter().copied().fold(f64::INFINITY, f64::min)),
        json!(values.iter().copied().fold(f64::NEG_INFINITY, f64::max)),
        json!(sum),
        json!(sum / values.len() as f64),
        json!(values.len()),
    ]
}

/// What every aggregation of [`aggs`] should report over `matched`,
/// normalized the way [`normalize`] normalizes a response.
fn brute_force(matched: &[&Doc]) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    let terms = |key: fn(&Doc) -> &'static str| {
        let mut counts: BTreeMap<&str, u64> = BTreeMap::new();
        for d in matched {
            *counts.entry(key(d)).or_default() += 1;
        }
        json!(counts)
    };
    out.insert("owners".into(), terms(|d| d.owner));
    out.insert("owners_kw".into(), terms(|d| d.owner));
    out.insert("statuses".into(), terms(|d| d.status));
    let distinct = |key: fn(&Doc) -> &'static str| {
        json!(matched
            .iter()
            .map(|d| key(d))
            .collect::<BTreeSet<_>>()
            .len())
    };
    out.insert("distinct_owners".into(), distinct(|d| d.owner));
    out.insert("distinct_regions".into(), distinct(|d| d.region));
    let mut pairs: BTreeMap<String, u64> = BTreeMap::new();
    for d in matched {
        *pairs
            .entry(format!("{}|{}", d.owner, d.region))
            .or_default() += 1;
    }
    out.insert("pairs".into(), json!(pairs));
    let qty: Vec<f64> = matched
        .iter()
        .filter_map(|d| d.qty)
        .map(|q| q as f64)
        .collect();
    let price: Vec<f64> = matched.iter().filter_map(|d| d.price).collect();
    for (field, values) in [("qty", qty), ("price", price)] {
        for (kind, value) in ["min", "max", "sum", "avg", "count"]
            .iter()
            .zip(metric(&values))
        {
            out.insert(format!("{field}_{kind}"), value);
        }
    }
    out
}

/// A response's aggregations in [`brute_force`]'s shape: terms and
/// composite as `key -> doc_count` maps, cardinality and metrics as their
/// `value`. Floats are rounded so summation order cannot matter.
fn normalize(result: &SearchResult) -> BTreeMap<String, Value> {
    let aggs = serde_json::to_value(result.aggregations.as_ref().unwrap()).unwrap();
    let mut out = BTreeMap::new();
    for name in ["owners", "owners_kw", "statuses"] {
        let buckets: BTreeMap<String, u64> = aggs[name]["buckets"]
            .as_array()
            .unwrap_or_else(|| panic!("{name}: {aggs}"))
            .iter()
            .map(|b| {
                (
                    b["key"].as_str().unwrap().to_string(),
                    b["doc_count"].as_u64().unwrap(),
                )
            })
            .collect();
        out.insert(name.to_string(), json!(buckets));
    }
    for name in ["distinct_owners", "distinct_regions"] {
        out.insert(name.to_string(), aggs[name]["value"].clone());
    }
    let pairs: BTreeMap<String, u64> = aggs["pairs"]["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| {
            (
                format!(
                    "{}|{}",
                    b["key"]["owner"].as_str().unwrap(),
                    b["key"]["region"].as_str().unwrap()
                ),
                b["doc_count"].as_u64().unwrap(),
            )
        })
        .collect();
    out.insert("pairs".into(), json!(pairs));
    for field in ["qty", "price"] {
        for kind in ["min", "max", "sum", "avg", "count"] {
            let name = format!("{field}_{kind}");
            out.insert(name.clone(), aggs[&name]["value"].clone());
        }
    }
    round_floats(out)
}

fn round_floats(map: BTreeMap<String, Value>) -> BTreeMap<String, Value> {
    map.into_iter()
        .map(|(k, v)| {
            let v = match v.as_f64() {
                Some(f) if !v.is_u64() => json!((f * 1e9).round() / 1e9),
                // Integral metric values may come back as 3 or 3.0.
                Some(f) if k.ends_with("_count") || k.starts_with("distinct") => json!(f as u64),
                _ => v,
            };
            (k, v)
        })
        .collect()
}

fn search(
    layout: &Layout,
    request: &Value,
    tombstones: Option<&HashMap<SegmentId, HashSet<u32>>>,
) -> SearchResult {
    let q: SearchQuery = serde_json::from_value(request.clone()).unwrap();
    Searcher::new(layout.dir.clone())
        .search(&NamespaceId("ns".into()), &layout.manifest, &q, tombstones)
        .unwrap_or_else(|e| panic!("{}: {request}: {e}", layout.name))
}

fn bucket_sum(result: &SearchResult, name: &str) -> u64 {
    let aggs = serde_json::to_value(result.aggregations.as_ref().unwrap()).unwrap();
    aggs[name]["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["doc_count"].as_u64().unwrap())
        .sum()
}

/// Every scope, over every layout, at every page shape: the aggregations
/// equal the brute-force count over all matches, single-valued terms
/// buckets add up to `total_hits`, and the page size never changes them.
#[test]
fn aggregations_match_brute_force_for_every_scope_layout_and_page() {
    let docs = corpus();
    let built = layouts(&docs);
    // Page shapes: no hits, a page much smaller than the match count, a
    // later page, everything, and a field sort (a different collection
    // path from the score-ranked one).
    let pages: [(usize, usize, Option<Value>); 5] = [
        (0, 0, None),
        (3, 0, None),
        (2, 5, None),
        (1000, 0, None),
        (
            4,
            0,
            Some(json!([{"price": {"order": "asc"}}, {"_id": {"order": "asc"}}])),
        ),
    ];
    for scope in scopes() {
        let matched: Vec<&Doc> = docs.iter().filter(|d| scope.keeps(d)).collect();
        assert!(
            !matched.is_empty() && matched.len() < docs.len() || matches!(scope, Scope::Everything),
            "vacuous scope {scope:?}"
        );
        let want = round_floats(brute_force(&matched));
        for layout in &built {
            for (max_results, from, sort) in &pages {
                let request = scope.request(*max_results, *from, sort.clone());
                let result = search(layout, &request, None);
                let what = format!("{} {scope:?} page={max_results}+{from}", layout.name);
                assert_eq!(result.total_hits, matched.len(), "{what}");
                assert_eq!(normalize(&result), want, "{what}");
                // A single-valued field: one bucket per matching document.
                assert_eq!(
                    bucket_sum(&result, "owners") as usize,
                    result.total_hits,
                    "{what}"
                );
                assert_eq!(
                    bucket_sum(&result, "statuses") as usize,
                    result.total_hits,
                    "{what}"
                );
                assert_eq!(
                    bucket_sum(&result, "pairs") as usize,
                    result.total_hits,
                    "{what}"
                );
            }
        }
    }
    for layout in built {
        let _ = std::fs::remove_dir_all(layout.dir);
    }
}

/// Without a filter the output is what it always was for the matches;
/// with `status: open` (half the corpus, each open document twinned with
/// a closed one) every count and sum is exactly halved and every extreme
/// and average is unchanged — over every layout.
#[test]
fn a_filter_excluding_half_the_corpus_halves_the_counts() {
    let docs = corpus();
    let built = layouts(&docs);
    for layout in &built {
        let all = search(layout, &Scope::Everything.request(0, 0, None), None);
        let open = search(layout, &Scope::Filter("open").request(0, 0, None), None);
        let closed = search(layout, &Scope::Filter("closed").request(0, 0, None), None);
        assert_eq!(all.total_hits, docs.len(), "{}", layout.name);
        assert_eq!(open.total_hits * 2, all.total_hits, "{}", layout.name);
        let (all, open, closed) = (normalize(&all), normalize(&open), normalize(&closed));
        for name in ["owners", "pairs"] {
            let halved: BTreeMap<String, u64> = all[name]
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.as_u64().unwrap() / 2))
                .collect();
            assert_eq!(open[name], json!(halved), "{} {name}", layout.name);
            assert_eq!(closed[name], json!(halved), "{} {name}", layout.name);
        }
        assert_eq!(
            open["statuses"],
            json!({"open": docs.len() / 2}),
            "{}",
            layout.name
        );
        for name in ["qty_count", "price_count", "qty_sum", "price_sum"] {
            let half = all[name].as_f64().unwrap() / 2.0;
            assert!(
                (open[name].as_f64().unwrap() - half).abs() < 1e-9,
                "{} {name}: {} vs {half}",
                layout.name,
                open[name]
            );
        }
        for name in [
            "qty_min",
            "qty_max",
            "qty_avg",
            "price_min",
            "price_max",
            "price_avg",
            "distinct_owners",
            "distinct_regions",
        ] {
            assert_eq!(open[name], all[name], "{} {name}", layout.name);
        }
    }
    for layout in built {
        let _ = std::fs::remove_dir_all(layout.dir);
    }
}

/// Deleted documents leave the buckets along with the hits.
#[test]
fn tombstoned_documents_are_not_aggregated() {
    let docs = corpus();
    let built = layouts(&docs);
    let deleted = |d: &Doc| d.seq % 4 == 1 || (d.segment == 2 && d.seq < 10);
    for layout in built
        .iter()
        .filter(|l| l.manifest.segments.len() == SEGMENTS)
    {
        let mut tombstones: HashMap<SegmentId, HashSet<u32>> = HashMap::new();
        for d in docs.iter().filter(|d| deleted(d)) {
            tombstones
                .entry(SegmentId(format!("seg{}", d.segment)))
                .or_default()
                .insert(d.seq);
        }
        for scope in scopes() {
            let matched: Vec<&Doc> = docs
                .iter()
                .filter(|d| scope.keeps(d) && !deleted(d))
                .collect();
            for max_results in [0, 3] {
                let result = search(
                    layout,
                    &scope.request(max_results, 0, None),
                    Some(&tombstones),
                );
                let what = format!("{} {scope:?} page={max_results}", layout.name);
                assert_eq!(result.total_hits, matched.len(), "{what}");
                assert_eq!(
                    normalize(&result),
                    round_floats(brute_force(&matched)),
                    "{what}"
                );
            }
        }
    }
    for layout in built {
        let _ = std::fs::remove_dir_all(layout.dir);
    }
}

fn brute_force_knn<'a>(
    docs: &'a [Doc],
    target: &[f32],
    k: usize,
    status: Option<&str>,
) -> Vec<&'a Doc> {
    let cosine = |v: &[f32]| -> f64 {
        let dot: f64 = v
            .iter()
            .zip(target)
            .map(|(a, b)| (*a as f64) * (*b as f64))
            .sum();
        let n = |x: &[f32]| x.iter().map(|a| (*a as f64).powi(2)).sum::<f64>().sqrt();
        dot / (n(v) * n(target))
    };
    let mut nearest: Vec<&Doc> = docs
        .iter()
        .filter(|d| status.is_none_or(|s| d.status == s))
        .collect();
    nearest.sort_by(|a, b| cosine(&b.vector).total_cmp(&cosine(&a.vector)));
    nearest.truncate(k);
    nearest
}

#[test]
fn vector_only_aggregations_cover_the_namespace_top_k_before_pagination() {
    let docs = corpus();
    let built = layouts(&docs);
    let target = [1.0f32, 0.0, 0.3, 1.0];
    let k = 7;
    for status in [None, Some("open")] {
        let matched = brute_force_knn(&docs, &target, k, status);
        let want = round_floats(brute_force(&matched));
        for layout in &built {
            for (from, size, field_sort) in
                [(0, 0, false), (0, 1, false), (2, 3, true), (0, k, false)]
            {
                let mut request = json!({
                    "query_text": "",
                    "from": from,
                    "max_results": size,
                    "knn": {"field": "emb", "vector": target, "k": k, "num_candidates": 200},
                    "aggs": aggs(),
                    "exact_total_hits": false,
                    "total_hits_cap": k,
                });
                if let Some(status) = status {
                    request["knn"]["filter"] = json!({"term": {"status": status}});
                }
                if field_sort {
                    request["sort"] =
                        json!([{"owner": {"order": "asc"}}, {"_id": {"order": "asc"}}]);
                }
                let result = search(layout, &request, None);
                let what = format!("{} {status:?} page={size}+{from}", layout.name);
                assert_eq!(bucket_sum(&result, "owners") as usize, k, "{what}");
                assert_eq!(result.total_hits, k, "{what}");
                assert_eq!(result.total_hits_relation, TotalHitsRelation::Eq, "{what}");
                assert_eq!(normalize(&result), want, "{what}");
                let mut page = matched.clone();
                if field_sort {
                    page.sort_by_key(|doc| (doc.owner, &doc.id));
                }
                assert_eq!(
                    result
                        .results
                        .iter()
                        .map(|hit| &hit.doc_id.0)
                        .collect::<Vec<_>>(),
                    page.iter()
                        .skip(from)
                        .take(size)
                        .map(|doc| &doc.id)
                        .collect::<Vec<_>>(),
                    "{what}"
                );
            }
        }
    }
    for layout in built {
        let _ = std::fs::remove_dir_all(layout.dir);
    }
}

#[test]
fn collapsed_vector_aggregations_keep_distinct_groups_in_the_top_k() {
    let mut docs = corpus();
    docs.truncate(3);
    // The two closest vectors share an owner; the other group is in
    // a different segment and must not be crowded out of the top two.
    for (i, doc) in docs.iter_mut().enumerate() {
        doc.vector = vec![1.0, i as f32 * 0.1, 0.0, 0.0];
    }
    let dir = temp_dir("collapsed");
    let layout = Layout {
        name: "collapsed",
        manifest: build(&dir, &docs, &[1, 2], |doc| usize::from(doc.owner == "bo")),
        dir,
    };
    let mut request = json!({
        "query_text": "",
        "max_results": 2,
        "knn": {"field": "emb", "vector": [1.0, 0.0, 0.0, 0.0], "k": 2, "num_candidates": 20},
        "collapse": {"field": "owner"},
        "aggs": aggs(),
    });
    for (representative, sort) in [(0, None), (1, Some(json!([{"status": {"order": "asc"}}])))] {
        if let Some(sort) = sort {
            request["sort"] = sort;
        }
        let result = search(&layout, &request, None);
        assert_eq!(result.total_hits, 2);
        assert_eq!(
            result
                .results
                .iter()
                .map(|hit| &hit.doc_id.0)
                .collect::<Vec<_>>(),
            vec![&docs[representative].id, &docs[2].id]
        );
        assert_eq!(
            normalize(&result),
            round_floats(brute_force(&[&docs[representative], &docs[2]]))
        );
    }
    std::fs::remove_dir_all(layout.dir).unwrap();
}

#[test]
fn field_sorted_vector_collapse_keeps_the_nearest_groups() {
    let mut docs = corpus();
    docs.truncate(4);
    docs[3].owner = "cy";
    // Ana's closest vector ranks first, but field sorting chooses her
    // other document, whose vector ranks last.
    for (doc, distance) in docs.iter_mut().zip([0.0, 0.3, 0.1, 0.2]) {
        doc.vector = vec![1.0, distance, 0.0, 0.0];
    }
    let dir = temp_dir("field-sorted-collapse");
    let layout = Layout {
        name: "field-sorted-collapse",
        manifest: build(&dir, &docs, &[1, 2], |doc| usize::from(doc.owner != "ana")),
        dir,
    };
    let request = json!({
        "query_text": "",
        "max_results": 2,
        "knn": {"field": "emb", "vector": [1.0, 0.0, 0.0, 0.0], "k": 2, "num_candidates": 20},
        "collapse": {"field": "owner"},
        "sort": [{"status": {"order": "asc"}}],
        "aggs": aggs(),
    });
    let result = search(&layout, &request, None);
    assert_eq!(result.total_hits, 2);
    assert_eq!(
        result
            .results
            .iter()
            .map(|hit| &hit.doc_id.0)
            .collect::<Vec<_>>(),
        vec![&docs[1].id, &docs[2].id]
    );
    assert_eq!(
        normalize(&result),
        round_floats(brute_force(&[&docs[1], &docs[2]]))
    );
    std::fs::remove_dir_all(layout.dir).unwrap();
}

/// A hybrid request aggregates over what its `total_hits` counts: the
/// lexical matches plus the vector window's other members, on row and
/// columnar segments alike, with a page smaller than either.
#[test]
fn hybrid_aggregations_cover_lexical_matches_and_the_vector_window() {
    let docs = corpus();
    let built = layouts(&docs);
    let target = [1.0f32, 0.0, 0.3, 1.0];
    let k = 7;
    for (text, status) in [("breach", None), ("breach", Some("open"))] {
        let passes = |d: &Doc| status.is_none_or(|s| d.status == s);
        let nearest = brute_force_knn(&docs, &target, k, status);
        let mut ids: BTreeSet<&str> = nearest.iter().map(|d| d.id.as_str()).collect();
        let lexical: Vec<&Doc> = docs
            .iter()
            .filter(|d| passes(d) && d.words.contains(&"breach"))
            .collect();
        ids.extend(lexical.iter().map(|d| d.id.as_str()));
        let matched: Vec<&Doc> = docs
            .iter()
            .filter(|d| ids.contains(d.id.as_str()))
            .collect();
        assert!(
            matched.len() > lexical.len(),
            "the vector window adds documents"
        );
        let want = round_floats(brute_force(&matched));
        for layout in &built {
            let mut request = json!({
                "query_text": text,
                "max_results": 3,
                "knn": {"field": "emb", "vector": target, "k": k, "num_candidates": 200},
                // The vector window is min(k, rank_window_size); the
                // window defaults to the page size (#228).
                "rank": {"rrf": {"rank_window_size": 50}},
                "aggs": aggs(),
                "exact_total_hits": true,
            });
            if let Some(s) = status {
                request["filter"] = json!({"term": {"status": s}});
            }
            let result = search(layout, &request, None);
            let what = format!("{} {text} {status:?}", layout.name);
            assert_eq!(result.total_hits, matched.len(), "{what}");
            assert_eq!(normalize(&result), want, "{what}");
            assert_eq!(
                bucket_sum(&result, "owners") as usize,
                result.total_hits,
                "{what}"
            );
        }
    }
    for layout in built {
        let _ = std::fs::remove_dir_all(layout.dir);
    }
}
