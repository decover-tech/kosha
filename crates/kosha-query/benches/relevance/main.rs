//! Relevance and recall regression harness (#258). No server: the corpus is
//! written through the real `kosha_write::Indexer` into a temp dir and read
//! back with the real `Searcher`, so the numbers move exactly when the
//! engine's ranking or vector index changes.
//!
//! Measures, against a fixture directory (format: scripts/relevance/README.md):
//!
//!   * nDCG@10 and recall@100 against the TREC qrels for three modes —
//!     `lexical` (`query_text`, operator OR), `vector` (`knn`) and
//!     `hybrid` (both, fused by RRF);
//!   * filtered-kNN recall@10 against exact brute force at 100%, 50%, 10%,
//!     1% and 0.1% selectivity, for the probe path alone
//!     (`KOSHA_KNN_EXACT_THRESHOLD=0`) and for the default planner.
//!   * nDCG@10 and MRR@10 deltas against exact vector search, and
//!     KMR curves (tie-tolerant recall@10 vs vectors scanned), including OOD queries.
//!
//! Everything is deterministic: the same tree and fixture give the same
//! report, so a base-vs-head delta is exact, not noise.
//!
//! ```text
//! cargo bench -p kosha-query --bench relevance                        # synthetic fixture
//! cargo bench -p kosha-query --bench relevance -- path/to/fixture     # any fixture
//! KOSHA_RELEVANCE_JSON=/tmp/rel.json cargo bench -p kosha-query --bench relevance
//! ```
//!
//! Environment:
//!   KOSHA_RELEVANCE_JSON            write the machine-readable report here
//!   KOSHA_RELEVANCE_FIXTURE         fixture dir when no argument is given
//!   KOSHA_RELEVANCE_NUM_CANDIDATES  kNN `num_candidates` (default 1)
//!
//! `num_candidates` defaults to 1 so the probe budget is the engine's own
//! floor, `KOSHA_KNN_BUDGET_FRACTION` (5%) of the namespace's centroids —
//! the budget a large namespace actually gets, and the one a change to the
//! vector index or the planner moves. Engine knobs apply as they do in the
//! server; the report records the ones that are set.

#[allow(dead_code)]
mod metrics;
mod vector_eval;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Instant;

use kosha_core::{Document, DocumentId, Field, FilterClause, Manifest, NamespaceId, SearchQuery};
use kosha_query::Searcher;
use kosha_segment::SegmentReader;
use kosha_write::Indexer;
use metrics::{
    brute_force_top_k, ndcg_at, parse_qrels, recall_at, reciprocal_rank_at, ExactNeighbors, Mean,
    Qrels,
};
use serde_json::{json, Value};

const NDCG_K: usize = 10;
const RECALL_K: usize = 100;
const KNN_K: usize = 10;
const VECTOR_FIELD: &str = "vector";
/// Integer attribute the harness adds to every document: its rank in a
/// fixed pseudo-random permutation, so `< ceil(n × s)` passes exactly a
/// fraction `s` of the corpus, spread evenly over it.
const SELECTIVITY_FIELD: &str = "kosha_relevance_sel";
const SELECTIVITIES: [(&str, f64); 5] = [
    ("100%", 1.0),
    ("50%", 0.5),
    ("10%", 0.1),
    ("1%", 0.01),
    ("0.1%", 0.001),
];
/// Engine settings recorded in the report when set.
const RECORDED_ENV: [&str; 7] = [
    "KOSHA_KNN_BUDGET_FRACTION",
    "KOSHA_KNN_EXACT_THRESHOLD",
    "KOSHA_KNN_EXACT_FRACTION",
    "KOSHA_RELEVANCE_NUM_CANDIDATES",
    // #384 experiments (off by default).
    "KOSHA_KNN_RELATIVE_CUTOFF_EPS",
    "KOSHA_KNN_RELATIVE_MIN_PROBES",
    "KOSHA_KNN_PROBE_RADIUS_ALPHA",
];

struct Query {
    qid: String,
    text: Option<String>,
    vector: Option<Vec<f32>>,
    filter: Option<Value>,
    ood: bool,
}

struct Fixture {
    docs: Vec<Document>,
    /// `(id, vector)` in corpus order, for brute force.
    vectors: Vec<(String, Vec<f32>)>,
    /// Selectivity rank per entry of `vectors`.
    sel_rank: Vec<u64>,
    queries: Vec<Query>,
    qrels: Qrels,
}

fn workspace_root() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    root.canonicalize().unwrap_or(root)
}

/// `cargo bench` runs the binary from the crate directory, so a relative
/// path that does not exist there is taken relative to the workspace root.
fn resolve(path: &str) -> PathBuf {
    let p = PathBuf::from(path);
    if p.is_absolute() || p.exists() {
        p
    } else {
        workspace_root().join(p)
    }
}

fn fixture_dir() -> PathBuf {
    // `cargo bench` passes `--bench`; any other non-flag argument is the dir.
    if let Some(arg) = std::env::args().skip(1).find(|a| !a.starts_with('-')) {
        return resolve(&arg);
    }
    match std::env::var("KOSHA_RELEVANCE_FIXTURE") {
        Ok(p) if !p.is_empty() => resolve(&p),
        _ => workspace_root().join("scripts/relevance/fixtures/synthetic"),
    }
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn jsonl(path: &Path) -> Vec<Value> {
    read(path)
        .lines()
        .enumerate()
        .filter(|(_, l)| !l.trim().is_empty())
        .map(|(n, l)| {
            serde_json::from_str(l)
                .unwrap_or_else(|e| panic!("{} line {}: {e}", path.display(), n + 1))
        })
        .collect()
}

fn as_vector(v: &Value, what: &str) -> Vec<f32> {
    v.as_array()
        .unwrap_or_else(|| panic!("{what}: vector must be an array of numbers"))
        .iter()
        .map(|x| {
            x.as_f64()
                .unwrap_or_else(|| panic!("{what}: vector must be an array of numbers"))
                as f32
        })
        .collect()
}

/// splitmix64: a fixed, platform-independent hash for the permutation.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn load_fixture(dir: &Path) -> Fixture {
    let meta: Value = match std::fs::read_to_string(dir.join("fixture.json")) {
        Ok(s) => serde_json::from_str(&s).expect("fixture.json"),
        Err(_) => json!({}),
    };
    let keyword: Vec<String> = meta["keyword_fields"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    let rows = jsonl(&dir.join("corpus.jsonl"));
    let n = rows.len() as u64;
    // Rank of each document in a permutation ordered by hash of its index.
    let mut order: Vec<u64> = (0..n).collect();
    order.sort_by_key(|i| (mix(*i), *i));
    let mut sel_rank = vec![0u64; n as usize];
    for (rank, i) in order.iter().enumerate() {
        sel_rank[*i as usize] = rank as u64;
    }

    let mut docs = Vec::with_capacity(rows.len());
    let mut vectors = Vec::new();
    let mut vector_rank = Vec::new();
    let mut seen = HashSet::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        let id = row["id"]
            .as_str()
            .unwrap_or_else(|| panic!("corpus line {}: `id` must be a string", i + 1))
            .to_string();
        // Indexing keeps only the last version of a repeated id, so brute force
        // over every row would rank vectors the index never searched.
        assert!(
            seen.insert(id.clone()),
            "corpus line {}: duplicate id {id}",
            i + 1
        );
        let mut fields = Vec::new();
        if let Some(obj) = row["fields"].as_object() {
            for (name, v) in obj {
                let field = match v {
                    Value::String(s) if keyword.contains(name) => Field::keyword(name, s),
                    Value::String(s) => Field::text(name, s),
                    Value::Bool(b) => Field::boolean(name, *b),
                    Value::Number(x) if x.is_i64() => Field::integer(name, x.as_i64().unwrap()),
                    Value::Number(x) => Field::float_val(name, x.as_f64().unwrap()),
                    Value::Null => continue,
                    other => panic!("corpus {id}: field {name}: unsupported value {other}"),
                };
                fields.push(field);
            }
        }
        if !row["vector"].is_null() {
            let v = as_vector(&row["vector"], &format!("corpus {id}"));
            fields.push(Field::vector(VECTOR_FIELD, v.clone()));
            vectors.push((id.clone(), v));
            vector_rank.push(sel_rank[i]);
        }
        fields.push(Field::integer(SELECTIVITY_FIELD, sel_rank[i] as i64));
        docs.push(Document {
            id: DocumentId(id),
            fields,
        });
    }

    let queries = jsonl(&dir.join("queries.jsonl"))
        .into_iter()
        .map(|q| {
            let qid = match &q["qid"] {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                other => panic!("queries: qid must be a string, got {other}"),
            };
            Query {
                text: q["text"].as_str().map(str::to_string),
                vector: (!q["vector"].is_null())
                    .then(|| as_vector(&q["vector"], &format!("query {qid}"))),
                filter: (!q["filter"].is_null()).then(|| q["filter"].clone()),
                ood: match q["distribution"].as_str() {
                    None | Some("id") => false,
                    Some("ood") => true,
                    Some(other) => panic!("query {qid}: unknown distribution {other}"),
                },
                qid,
            }
        })
        .collect();
    let qrels = parse_qrels(&read(&dir.join("qrels.txt"))).unwrap_or_else(|e| panic!("{e}"));
    Fixture {
        docs,
        vectors,
        sel_rank: vector_rank,
        queries,
        qrels,
    }
}

fn build(root: &Path, ns: &NamespaceId, docs: Vec<Document>) -> Manifest {
    let _ = std::fs::remove_dir_all(root);
    let idx = Indexer::new(root.to_path_buf());
    for chunk in docs.chunks(500) {
        idx.index_documents(ns.clone(), chunk.to_vec())
            .expect("index documents");
    }
    idx.flush_namespace(ns).expect("flush");
    idx.manifest(ns).expect("manifest after flush")
}

fn search_query(body: Value) -> SearchQuery {
    let q: SearchQuery = serde_json::from_value(body.clone())
        .unwrap_or_else(|e| panic!("bad search request {body}: {e}"));
    q.validate()
        .unwrap_or_else(|e| panic!("invalid search request {body}: {e}"));
    q
}

fn knn(vector: &[f32], k: usize, num_candidates: usize, filter: Option<&Value>) -> Value {
    let mut knn = json!({
        "field": VECTOR_FIELD,
        "vector": vector,
        "k": k,
        "num_candidates": num_candidates,
    });
    if let Some(f) = filter {
        knn["filter"] = f.clone();
    }
    knn
}

/// The hit ids in rank order, plus the kNN work the search did (postings
/// probed, vectors scored exactly) for the report's diagnostics.
fn search(
    searcher: &Searcher,
    ns: &NamespaceId,
    manifest: &Manifest,
    q: &SearchQuery,
    work: &mut KnnWork,
) -> Vec<String> {
    let (r, stats) = searcher
        .search_with_stats(ns, manifest, q, Some(&manifest.tombstones))
        .unwrap_or_else(|e| panic!("search failed: {e}"));
    work.postings += stats.knn_postings_probed;
    work.exact_vectors += stats.knn_exact_vectors;
    work.searches += 1;
    r.results.into_iter().map(|d| d.doc_id.0).collect()
}

#[derive(Default)]
struct KnnWork {
    postings: usize,
    exact_vectors: usize,
    searches: usize,
}

impl KnnWork {
    fn per_search(&self, total: usize) -> f64 {
        total as f64 / self.searches.max(1) as f64
    }
}

struct Report {
    metrics: serde_json::Map<String, Value>,
    rows: Vec<(String, Option<f64>, usize, f64)>,
}

impl Report {
    /// `value` is what compare.py gates on; the kNN work is a diagnostic
    /// (it explains a recall change but is never compared).
    fn put(&mut self, name: String, mean: Mean, work: &KnnWork) {
        let postings = work.per_search(work.postings);
        self.metrics.insert(
            name.clone(),
            json!({
                "value": mean.value(),
                "queries": mean.n,
                "knn_postings_per_query": postings,
                "knn_exact_vectors_per_query": work.per_search(work.exact_vectors),
            }),
        );
        self.rows.push((name, mean.value(), mean.n, postings));
    }
}

fn matching_ids<'a>(readers: &'a [SegmentReader], filter: &FilterClause) -> HashSet<&'a str> {
    readers
        .iter()
        .flat_map(|reader| {
            reader
                .evaluate_filter(filter)
                .expect("evaluate exact-search filter")
                .iter()
                .map(|seq| reader.doc_meta(seq).expect("filter document").doc_id)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn vector_vs_exact(
    probe: &Searcher,
    ns: &NamespaceId,
    manifest: &Manifest,
    readers: &[SegmentReader],
    fx: &Fixture,
    num_candidates: usize,
    report: &mut Report,
) -> Value {
    let mut ann = [Mean::default(); 2];
    let mut exact = [Mean::default(); 2];
    let mut work = KnnWork::default();
    for q in &fx.queries {
        let (Some(vector), Some(judged)) = (q.vector.as_deref(), fx.qrels.get(&q.qid)) else {
            continue;
        };
        let request = search_query(json!({
            "query_text": "",
            "max_results": KNN_K,
            "knn": knn(vector, KNN_K, num_candidates, q.filter.as_ref()),
        }));
        let filter = request.knn.as_ref().and_then(|knn| knn.filter.as_ref());
        let passing = filter.map(|filter| matching_ids(readers, filter));
        let candidates = fx
            .vectors
            .iter()
            .filter(|(id, _)| passing.as_ref().is_none_or(|set| set.contains(id.as_str())))
            .map(|(id, v)| (id.as_str(), v.as_slice()));
        let truth = brute_force_top_k(vector, candidates, KNN_K);
        let hits = search(probe, ns, manifest, &request, &mut work);
        for (means, hits) in [(&mut ann, &hits), (&mut exact, &truth)] {
            means[0].add(ndcg_at(hits, judged, KNN_K));
            means[1].add(reciprocal_rank_at(hits, judged, KNN_K));
        }
    }
    ["ndcg", "mrr"]
        .into_iter()
        .enumerate()
        .map(|(i, metric)| {
            let name = format!("{metric}@{KNN_K}");
            report.put(format!("vector_ann.{name}"), ann[i], &work);
            let ann_value = ann[i].value();
            let exact_value = exact[i].value();
            (
                name,
                json!({
                    "ann": ann_value,
                    "exact": exact_value,
                    "delta": ann_value.zip(exact_value).map(|(a, e)| a - e),
                    "queries": ann[i].n,
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>()
        .into()
}

fn main() {
    let started = Instant::now();
    let dir = fixture_dir();
    let num_candidates: usize = std::env::var("KOSHA_RELEVANCE_NUM_CANDIDATES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let mut fx = load_fixture(&dir);
    let n_docs = fx.docs.len();
    let root = std::env::temp_dir().join(format!("kosha-relevance-{}", std::process::id()));
    let ns = NamespaceId("relevance".into());
    let t = Instant::now();
    let manifest = build(&root, &ns, std::mem::take(&mut fx.docs));
    let build_s = t.elapsed().as_secs_f64();
    eprintln!(
        "fixture {}: {n_docs} docs ({} with vectors) in {} segments, built in {build_s:.1}s; \
         {} queries, {} judged; num_candidates {num_candidates}",
        dir.display(),
        fx.vectors.len(),
        manifest.segments.len(),
        fx.queries.len(),
        fx.qrels.len(),
    );

    let mut report = Report {
        metrics: serde_json::Map::new(),
        rows: Vec::new(),
    };
    let searcher = Searcher::new(root.clone());

    // ── Ranked retrieval against the qrels ────────────────────────────────
    for mode in ["lexical", "vector", "hybrid"] {
        let (mut ndcg, mut recall) = (Mean::default(), Mean::default());
        let mut work = KnnWork::default();
        for q in &fx.queries {
            let Some(judged) = fx.qrels.get(&q.qid) else {
                continue;
            };
            let text = q.text.as_deref().filter(|t| !t.trim().is_empty());
            let vector = q.vector.as_deref();
            let mut body = json!({"max_results": RECALL_K, "query_text": ""});
            match (mode, text, vector) {
                ("lexical", Some(t), _) => {
                    body["query_text"] = json!(t);
                    body["operator"] = json!("or");
                }
                ("vector", _, Some(v)) => {
                    body["knn"] = knn(v, RECALL_K, num_candidates, q.filter.as_ref());
                }
                ("hybrid", Some(t), Some(v)) => {
                    body["query_text"] = json!(t);
                    body["operator"] = json!("or");
                    body["knn"] = knn(v, RECALL_K, num_candidates, q.filter.as_ref());
                    body["rank"] = json!({"rrf": {}});
                }
                _ => continue,
            }
            if let Some(f) = &q.filter {
                body["filter"] = f.clone();
            }
            let hits = search(&searcher, &ns, &manifest, &search_query(body), &mut work);
            ndcg.add(ndcg_at(&hits, judged, NDCG_K));
            recall.add(recall_at(&hits, judged, RECALL_K));
        }
        report.put(format!("{mode}.ndcg@{NDCG_K}"), ndcg, &work);
        report.put(format!("{mode}.recall@{RECALL_K}"), recall, &work);
    }

    // ── Filtered kNN recall@10 vs brute force ─────────────────────────────
    let probe = Searcher::new(root.clone())
        .with_knn_exact_policy(0, kosha_query::DEFAULT_KNN_EXACT_FRACTION);
    let readers: Vec<_> = manifest
        .segments
        .iter()
        .map(|segment| {
            SegmentReader::open(root.join(&ns.0).join(&segment.segment_id.0))
                .expect("open fixture segment")
        })
        .collect();
    let exact_deltas = vector_vs_exact(
        &probe,
        &ns,
        &manifest,
        &readers,
        &fx,
        num_candidates,
        &mut report,
    );
    let mut curves = serde_json::Map::new();
    let mut ood_queries = 0;
    for (distribution, ood) in [("id", false), ("ood", true)] {
        let qvecs: Vec<_> = fx
            .queries
            .iter()
            .filter(|q| q.ood == ood)
            .filter_map(|q| q.vector.as_deref())
            .collect();
        if ood {
            ood_queries = qvecs.len();
        }
        if qvecs.is_empty() {
            continue;
        }
        let prefix = if ood { "ood." } else { "" };
        curves.insert(
            distribution.to_string(),
            vector_eval::kmr_curve(&readers, &qvecs, &fx.vectors),
        );
        for (label, s) in SELECTIVITIES {
            // Ranks are over every document; a corpus with vectorless documents
            // simply has fewer vectors passing.
            let cut = ((n_docs as f64) * s).ceil() as u64;
            let filter =
                (s < 1.0).then(|| json!({"range": {SELECTIVITY_FIELD: {"lt": cut.to_string()}}}));
            let truths: Vec<_> = qvecs
                .iter()
                .map(|q| {
                    let pass = fx
                        .vectors
                        .iter()
                        .zip(&fx.sel_rank)
                        .filter(|(_, r)| **r < cut)
                        .map(|((id, v), _)| (id.as_str(), v.as_slice()));
                    ExactNeighbors::new(q, pass, KNN_K)
                })
                .collect();
            for (policy, s_searcher) in [("probe", &probe), ("planned", &searcher)] {
                let mut recall = Mean::default();
                let mut work = KnnWork::default();
                for (q, truth) in qvecs.iter().zip(&truths) {
                    let body = json!({
                        "query_text": "",
                        "max_results": KNN_K,
                        "knn": knn(q, KNN_K, num_candidates, filter.as_ref()),
                    });
                    let hits = search(s_searcher, &ns, &manifest, &search_query(body), &mut work);
                    recall.add(truth.recall(&hits));
                }
                report.put(
                    format!("{prefix}filtered_knn.{policy}@{label}.recall@{KNN_K}"),
                    recall,
                    &work,
                );
            }
        }
    }
    let _ = std::fs::remove_dir_all(&root);

    // ── Output ────────────────────────────────────────────────────────────
    println!("| metric | value | queries | kNN postings/query |");
    println!("|---|---:|---:|---:|");
    for (name, value, n, postings) in &report.rows {
        let v = value.map_or("n/a".to_string(), |v| format!("{v:.4}"));
        println!("| {name} | {v} | {n} | {postings:.1} |");
    }
    let elapsed = started.elapsed().as_secs_f64();
    eprintln!("relevance harness finished in {elapsed:.1}s");

    if let Some(path) = std::env::var("KOSHA_RELEVANCE_JSON")
        .ok()
        .filter(|p| !p.is_empty())
    {
        // Relative to the workspace root, not the crate dir cargo runs in.
        let path = workspace_root().join(path);
        let env: serde_json::Map<String, Value> = RECORDED_ENV
            .iter()
            .filter_map(|k| std::env::var(k).ok().map(|v| (k.to_string(), json!(v))))
            .collect();
        let out = json!({
            "schema": 1,
            "kind": "kosha-relevance",
            "fixture": {
                "name": dir.file_name().map(|s| s.to_string_lossy().into_owned()),
                "docs": n_docs,
                "vectors": fx.vectors.len(),
                "queries": fx.queries.len(),
                "ood_vector_queries": ood_queries,
                "judged_queries": fx.qrels.len(),
                "segments": manifest.segments.len(),
            },
            "config": {
                "ndcg_k": NDCG_K,
                "recall_k": RECALL_K,
                "knn_k": KNN_K,
                "num_candidates": num_candidates,
                "lexical_operator": "or",
                "hybrid_fusion": "rrf",
                "selectivities": SELECTIVITIES.iter().map(|(l, _)| *l).collect::<Vec<_>>(),
                "env": env,
            },
            // No wall-clock numbers: the report is a pure function of the
            // tree and the fixture, so it can be committed as a baseline.
            "metrics": report.metrics,
            "vector_vs_exact": exact_deltas,
            "kmr": curves,
        });
        std::fs::write(&path, serde_json::to_string_pretty(&out).unwrap() + "\n")
            .unwrap_or_else(|e| panic!("write KOSHA_RELEVANCE_JSON={}: {e}", path.display()));
        eprintln!("report written to {}", path.display());
    }
}
