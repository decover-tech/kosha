//! Relevance evaluation of late-interaction (ColBERT-style) retrieval
//! (#205) on a corpus of token embeddings with relevance judgments.
//!
//! ```text
//! # a tiny synthetic corpus in the input format, to try the harness:
//! kosha-late-interaction-eval gen --out /tmp/li-corpus
//! kosha-late-interaction-eval eval --corpus /tmp/li-corpus --json /tmp/li-eval.json
//! ```
//!
//! # Input (`--corpus DIR`)
//!
//! * `docs.jsonl` — one document per line:
//!   `{"id": "D1", "tokens": [[f32; dim], ...]}`, the passage's token
//!   embeddings (ColBERTv2: 128-dim, one row per non-padding token).
//! * `queries.jsonl` — one query per line, same shape:
//!   `{"id": "Q1", "tokens": [[f32; dim], ...]}`.
//! * `qrels.tsv` — relevance judgments, whitespace-separated, one per line:
//!   `query_id doc_id relevance` (BEIR's `qrels/*.tsv`; a `query-id`
//!   header line is skipped) or TREC's `query_id 0 doc_id relevance`.
//!   Relevance `> 0` is relevant; nDCG uses it as the graded gain.
//!
//! # What it does
//!
//! Indexes every document into in-process segments (`--segment-docs` per
//! segment) with two fields: the bag as a late-interaction field, and its
//! mean-pooled tokens as a single-vector field. Then, per query, it ranks
//! the corpus three ways and scores each against the qrels:
//!
//! * `late_interaction` — Kosha's late-interaction search at the given
//!   `--nprobe` / `--num-candidates` (what a query gets in production);
//! * `exact_maxsim` — the same search with every posting probed and every
//!   document re-scored, which is exact MaxSim (the index's ceiling);
//! * `single_vector_pooled` — single-vector kNN of the mean-pooled query
//!   against the mean-pooled documents (`--single-nprobe` per segment).
//!
//! It reports recall@k and nDCG@k per method (queries without a relevant
//! document in the qrels are skipped and counted), each approximate
//! method's overlap with the exact MaxSim top k, and p50 latency. `--json`
//! writes the same numbers as JSON.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use kosha_core::{Bm25Params, DocumentId, Field, FieldType, SegmentId};
use kosha_segment::{LateInteractionParams, SegmentReader, SegmentWriter};
use serde::{Deserialize, Serialize};

const BAG_FIELD: &str = "tokens";
const POOLED_FIELD: &str = "pooled";

#[derive(Parser)]
#[command(name = "kosha-late-interaction-eval")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write a small synthetic corpus in the input format: documents are
    /// bags of tokens around a few topics, and each query is a noisy copy
    /// of part of one document, which is its one relevant document.
    Gen {
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 400)]
        docs: usize,
        #[arg(long, default_value_t = 40)]
        queries: usize,
        #[arg(long, default_value_t = 32)]
        dim: usize,
        #[arg(long, default_value_t = 24)]
        tokens: usize,
        #[arg(long, default_value_t = 7)]
        seed: u64,
    },
    /// Index a corpus and report recall@k / nDCG@k per retrieval method.
    Eval {
        /// Directory holding docs.jsonl, queries.jsonl and qrels.tsv
        #[arg(long)]
        corpus: PathBuf,
        /// Where the segments are built; must be absent or empty (default:
        /// a temporary directory, removed afterwards)
        #[arg(long)]
        work_dir: Option<PathBuf>,
        #[arg(long, default_value_t = 50_000)]
        segment_docs: usize,
        #[arg(long, default_value_t = 10)]
        k: usize,
        /// Token-index postings probed per query token (the server's
        /// KOSHA_LATE_INTERACTION_NPROBE)
        #[arg(long, default_value_t = 16)]
        nprobe: usize,
        /// Candidates re-scored exactly, per segment (`knn.num_candidates`)
        #[arg(long, default_value_t = 100)]
        num_candidates: usize,
        /// Postings probed per segment by the single-vector baseline
        #[arg(long, default_value_t = 32)]
        single_nprobe: usize,
        /// Skip the exact-MaxSim pass (it scores every token of every
        /// document per query)
        #[arg(long)]
        skip_exact: bool,
        /// Evaluate at most this many queries (default: all)
        #[arg(long)]
        max_queries: Option<usize>,
        /// Also write the report as JSON here
        #[arg(long)]
        json: Option<PathBuf>,
    },
}

#[derive(Serialize, Deserialize)]
struct Item {
    id: String,
    tokens: Vec<Vec<f32>>,
}

/// One method's scores, averaged over the evaluated queries.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct MethodReport {
    pub recall_at_k: f64,
    pub ndcg_at_k: f64,
    /// Share of the exact-MaxSim top k this method also returned (`None`
    /// when the exact pass was skipped).
    pub overlap_with_exact: Option<f64>,
    pub p50_ms: f64,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Report {
    pub docs: usize,
    pub tokens: usize,
    pub dim: usize,
    pub segments: usize,
    pub k: usize,
    pub nprobe: usize,
    pub num_candidates: usize,
    pub single_nprobe: usize,
    pub queries_evaluated: usize,
    /// Queries skipped because the qrels hold no relevant document for
    /// them that is in the corpus.
    pub queries_without_judgments: usize,
    pub late_interaction: MethodReport,
    pub exact_maxsim: Option<MethodReport>,
    pub single_vector_pooled: MethodReport,
}

fn main() {
    match Cli::parse().command {
        Command::Gen {
            out,
            docs,
            queries,
            dim,
            tokens,
            seed,
        } => gen(&out, docs, queries, dim, tokens, seed),
        Command::Eval {
            corpus,
            work_dir,
            segment_docs,
            k,
            nprobe,
            num_candidates,
            single_nprobe,
            skip_exact,
            max_queries,
            json,
        } => {
            let temp = work_dir.is_none();
            let work = work_dir.unwrap_or_else(|| {
                std::env::temp_dir().join(format!("kosha-li-eval-{}", std::process::id()))
            });
            if temp {
                let _ = std::fs::remove_dir_all(&work);
            } else {
                assert!(
                    std::fs::read_dir(&work).map_or(true, |mut d| d.next().is_none()),
                    "--work-dir {} must be absent or empty",
                    work.display()
                );
            }
            let report = eval(
                &corpus,
                &work,
                &EvalParams {
                    segment_docs: segment_docs.max(1),
                    k: k.max(1),
                    nprobe,
                    num_candidates,
                    single_nprobe,
                    exact: !skip_exact,
                    max_queries,
                },
            );
            if temp {
                let _ = std::fs::remove_dir_all(&work);
            }
            print_report(&report);
            if let Some(path) = json {
                std::fs::write(&path, serde_json::to_vec_pretty(&report).unwrap())
                    .unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
            }
        }
    }
}

// ─── Synthetic corpus ──────────────────────────────────────────────────────

/// xorshift64* — deterministic, dependency-free.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn uniform(&mut self) -> f32 {
        ((self.next_u64() >> 40) as f32 + 0.5) / (1u64 << 24) as f32
    }
    fn normal(&mut self) -> f32 {
        let (u, v) = (self.uniform(), self.uniform());
        (-2.0 * u.ln()).sqrt() * (std::f32::consts::TAU * v).cos()
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

fn write_jsonl(path: &Path, items: &[Item]) {
    let mut w = BufWriter::new(
        std::fs::File::create(path).unwrap_or_else(|e| panic!("create {}: {e}", path.display())),
    );
    for item in items {
        serde_json::to_writer(&mut w, item).unwrap();
        w.write_all(b"\n").unwrap();
    }
    w.flush().unwrap();
}

fn gen(out: &Path, docs: usize, queries: usize, dim: usize, tokens: usize, seed: u64) {
    std::fs::create_dir_all(out).expect("create out dir");
    let mut rng = Rng(seed | 1);
    let topics = (docs / 8).max(4);
    let centers: Vec<Vec<f32>> = (0..topics)
        .map(|_| (0..dim).map(|_| rng.normal()).collect())
        .collect();
    let near = |rng: &mut Rng, c: &[f32], spread: f32| -> Vec<f32> {
        c.iter().map(|x| x + spread * rng.normal()).collect()
    };
    // Each document mixes three topics, so its mean-pooled vector blurs
    // them while its tokens keep them apart.
    let doc_items: Vec<Item> = (0..docs)
        .map(|d| {
            let picks: Vec<usize> = (0..3).map(|_| rng.below(topics)).collect();
            Item {
                id: format!("D{d}"),
                tokens: (0..tokens)
                    .map(|t| near(&mut rng, &centers[picks[t % 3]], 0.7))
                    .collect(),
            }
        })
        .collect();
    let mut qrels = String::from("query-id\tcorpus-id\tscore\n");
    let query_items: Vec<Item> = (0..queries)
        .map(|q| {
            let target = rng.below(docs);
            // A few of the target's own tokens, perturbed.
            let n = (tokens / 4).max(2);
            let query_tokens = (0..n)
                .map(|_| {
                    let t = &doc_items[target].tokens[rng.below(tokens)];
                    near(&mut rng, t, 0.3)
                })
                .collect();
            qrels.push_str(&format!("Q{q}\tD{target}\t1\n"));
            Item {
                id: format!("Q{q}"),
                tokens: query_tokens,
            }
        })
        .collect();
    write_jsonl(&out.join("docs.jsonl"), &doc_items);
    write_jsonl(&out.join("queries.jsonl"), &query_items);
    std::fs::write(out.join("qrels.tsv"), qrels).unwrap();
    println!(
        "wrote {docs} docs x {tokens} tokens, {queries} queries, dim {dim} to {}",
        out.display()
    );
}

// ─── Evaluation ────────────────────────────────────────────────────────────

struct EvalParams {
    segment_docs: usize,
    k: usize,
    nprobe: usize,
    num_candidates: usize,
    single_nprobe: usize,
    exact: bool,
    max_queries: Option<usize>,
}

fn read_jsonl(path: &Path) -> impl Iterator<Item = Item> {
    let file = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let path = path.to_path_buf();
    BufReader::new(file)
        .lines()
        .enumerate()
        .filter_map(move |(n, line)| {
            let line = line.unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
            if line.trim().is_empty() {
                return None;
            }
            Some(serde_json::from_str(&line).unwrap_or_else(|e| {
                panic!(
                    "{}:{}: not {{\"id\", \"tokens\"}}: {e}",
                    path.display(),
                    n + 1
                )
            }))
        })
}

/// `query id -> (doc id -> relevance)`, relevant (`> 0`) judgments only.
fn read_qrels(path: &Path) -> HashMap<String, HashMap<String, f64>> {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let mut qrels: HashMap<String, HashMap<String, f64>> = HashMap::new();
    for (n, line) in text.lines().enumerate() {
        let cols: Vec<&str> = line.split_whitespace().collect();
        let (q, d, rel) = match cols.as_slice() {
            [] => continue,
            [q, d, rel] | [q, _, d, rel] => (*q, *d, *rel),
            _ => panic!("{}:{}: expected 3 or 4 columns", path.display(), n + 1),
        };
        let Ok(rel) = rel.parse::<f64>() else {
            if n == 0 {
                continue; // header
            }
            panic!(
                "{}:{}: relevance {rel:?} is not a number",
                path.display(),
                n + 1
            );
        };
        if rel > 0.0 {
            qrels
                .entry(q.to_string())
                .or_default()
                .insert(d.to_string(), rel);
        }
    }
    qrels
}

fn mean(tokens: &[Vec<f32>]) -> Vec<f32> {
    let mut m = vec![0.0f32; tokens[0].len()];
    for t in tokens {
        for (a, b) in m.iter_mut().zip(t) {
            *a += b / tokens.len() as f32;
        }
    }
    m
}

/// Checks a bag is non-empty and of dimension `dim` (the first bag's).
fn check_bag(kind: &str, item: &Item, dim: &mut Option<usize>) {
    let first = item
        .tokens
        .first()
        .unwrap_or_else(|| panic!("{kind} {:?} has no tokens", item.id));
    let d = *dim.get_or_insert(first.len());
    assert!(
        d > 0 && item.tokens.iter().all(|t| t.len() == d),
        "{kind} {:?}: every token must have dimension {d}",
        item.id
    );
}

/// Builds the segments; returns `(segment dirs, docs, tokens, dim)`.
fn build(corpus: &Path, work: &Path, segment_docs: usize) -> (Vec<PathBuf>, usize, usize, usize) {
    let mut dirs = Vec::new();
    let mut writer: Option<SegmentWriter> = None;
    let (mut docs, mut tokens, mut in_segment) = (0usize, 0usize, 0usize);
    let mut dim = None;
    let mut seen = HashSet::new();
    for item in read_jsonl(&corpus.join("docs.jsonl")) {
        check_bag("document", &item, &mut dim);
        assert!(
            seen.insert(item.id.clone()),
            "duplicate doc id {:?}",
            item.id
        );
        let w = writer.get_or_insert_with(|| {
            let dir = work.join(format!("s{}", dirs.len()));
            dirs.push(dir.clone());
            SegmentWriter::new(SegmentId(format!("s{}", dirs.len() - 1)), dir)
        });
        w.add_document(
            DocumentId(item.id.clone()),
            vec![
                Field::vector(POOLED_FIELD, mean(&item.tokens)),
                Field {
                    name: BAG_FIELD.into(),
                    field_type: FieldType::Vector,
                    value: serde_json::to_string(&item.tokens).unwrap(),
                },
            ],
        );
        docs += 1;
        tokens += item.tokens.len();
        in_segment += 1;
        if in_segment == segment_docs {
            writer
                .take()
                .unwrap()
                .finalize(Bm25Params::default())
                .unwrap();
            in_segment = 0;
        }
    }
    if let Some(w) = writer {
        w.finalize(Bm25Params::default()).unwrap();
    }
    assert!(docs > 0, "docs.jsonl holds no documents");
    (dirs, docs, tokens, dim.unwrap_or(0))
}

/// Namespace top `k` doc ids, merging each segment's `(doc_seq, score)`.
fn merge_top_k(
    readers: &[SegmentReader],
    k: usize,
    per_segment: impl Fn(&SegmentReader) -> Vec<(u32, f64)>,
) -> Vec<String> {
    let mut hits: Vec<(f64, String)> = Vec::new();
    for reader in readers {
        for (seq, score) in per_segment(reader) {
            let id = reader
                .doc_meta(seq)
                .expect("hit has a doc")
                .doc_id
                .to_string();
            hits.push((score, id));
        }
    }
    hits.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    hits.into_iter().take(k).map(|(_, id)| id).collect()
}

fn recall(ranked: &[String], relevant: &HashMap<String, f64>) -> f64 {
    let found = ranked.iter().filter(|d| relevant.contains_key(*d)).count();
    found as f64 / relevant.len() as f64
}

fn ndcg(ranked: &[String], relevant: &HashMap<String, f64>, k: usize) -> f64 {
    let dcg: f64 = ranked
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, d)| relevant.get(d).copied().unwrap_or(0.0) / (i as f64 + 2.0).log2())
        .sum();
    let mut ideal: Vec<f64> = relevant.values().copied().collect();
    ideal.sort_by(|a, b| b.total_cmp(a));
    let idcg: f64 = ideal
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, g)| g / (i as f64 + 2.0).log2())
        .sum();
    if idcg > 0.0 {
        dcg / idcg
    } else {
        0.0
    }
}

fn overlap(ranked: &[String], exact: &[String]) -> f64 {
    if exact.is_empty() {
        return 1.0;
    }
    let exact: HashSet<&String> = exact.iter().collect();
    ranked.iter().filter(|d| exact.contains(d)).count() as f64 / exact.len() as f64
}

fn p50_ms(mut v: Vec<Duration>) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort();
    v[v.len() / 2].as_secs_f64() * 1e3
}

#[derive(Default)]
struct Totals {
    recall: f64,
    ndcg: f64,
    overlap: f64,
    latency: Vec<Duration>,
}

impl Totals {
    fn add(
        &mut self,
        ranked: &[String],
        relevant: &HashMap<String, f64>,
        k: usize,
        took: Duration,
    ) {
        self.recall += recall(ranked, relevant);
        self.ndcg += ndcg(ranked, relevant, k);
        self.latency.push(took);
    }
    fn report(self, n: usize, exact: bool) -> MethodReport {
        let n = n.max(1) as f64;
        MethodReport {
            recall_at_k: self.recall / n,
            ndcg_at_k: self.ndcg / n,
            overlap_with_exact: exact.then(|| self.overlap / n),
            p50_ms: p50_ms(self.latency),
        }
    }
}

fn eval(corpus: &Path, work: &Path, p: &EvalParams) -> Report {
    let t = Instant::now();
    let (dirs, docs, tokens, dim) = build(corpus, work, p.segment_docs);
    eprintln!(
        "indexed {docs} docs ({tokens} tokens, dim {dim}) into {} segment(s) in {:.1} s",
        dirs.len(),
        t.elapsed().as_secs_f64()
    );
    let readers: Vec<SegmentReader> = dirs
        .iter()
        .map(|d| SegmentReader::open(d.clone()).expect("open segment"))
        .collect();
    let qrels = read_qrels(&corpus.join("qrels.tsv"));
    let mut doc_ids: HashSet<String> = HashSet::with_capacity(docs);
    for r in &readers {
        for seq in 0..r.doc_count() {
            if let Some(meta) = r.doc_meta(seq) {
                doc_ids.insert(meta.doc_id.to_string());
            }
        }
    }

    let li_params = LateInteractionParams {
        nprobe: p.nprobe,
        num_candidates: p.num_candidates,
    };
    let exact_params = LateInteractionParams {
        nprobe: usize::MAX,
        num_candidates: usize::MAX,
    };
    let (mut li, mut exact, mut single) = (Totals::default(), Totals::default(), Totals::default());
    let (mut evaluated, mut skipped) = (0usize, 0usize);
    let mut qdim = Some(dim);
    for query in read_jsonl(&corpus.join("queries.jsonl")) {
        if p.max_queries.is_some_and(|max| evaluated >= max) {
            break;
        }
        let relevant: HashMap<String, f64> = qrels
            .get(&query.id)
            .map(|r| {
                r.iter()
                    .filter(|(d, _)| doc_ids.contains(*d))
                    .map(|(d, g)| (d.clone(), *g))
                    .collect()
            })
            .unwrap_or_default();
        if relevant.is_empty() {
            skipped += 1;
            continue;
        }
        check_bag("query", &query, &mut qdim);
        evaluated += 1;
        let q_tokens: Vec<&[f32]> = query.tokens.iter().map(Vec::as_slice).collect();
        let late = |params: LateInteractionParams| {
            let t = Instant::now();
            let ranked = merge_top_k(&readers, p.k, |r| {
                r.late_interaction_search(BAG_FIELD, &q_tokens, p.k, params, None)
                    .expect("late-interaction search")
                    .hits
            });
            (ranked, t.elapsed())
        };

        let (li_ranked, li_took) = late(li_params);
        let pooled = mean(&query.tokens);
        let t = Instant::now();
        let single_ranked = merge_top_k(&readers, p.k, |r| {
            r.knn_search_filtered(POOLED_FIELD, &pooled, p.k, p.single_nprobe, None)
                .expect("the pooled field has a clustered index")
                .expect("single-vector search")
        });
        let single_took = t.elapsed();
        li.add(&li_ranked, &relevant, p.k, li_took);
        single.add(&single_ranked, &relevant, p.k, single_took);
        if p.exact {
            let (exact_ranked, exact_took) = late(exact_params);
            li.overlap += overlap(&li_ranked, &exact_ranked);
            single.overlap += overlap(&single_ranked, &exact_ranked);
            exact.overlap += 1.0;
            exact.add(&exact_ranked, &relevant, p.k, exact_took);
        }
    }
    assert!(
        evaluated > 0,
        "no query has a relevant document in the corpus (check the qrels' ids)"
    );
    Report {
        docs,
        tokens,
        dim,
        segments: readers.len(),
        k: p.k,
        nprobe: p.nprobe,
        num_candidates: p.num_candidates,
        single_nprobe: p.single_nprobe,
        queries_evaluated: evaluated,
        queries_without_judgments: skipped,
        late_interaction: li.report(evaluated, p.exact),
        exact_maxsim: p.exact.then(|| exact.report(evaluated, true)),
        single_vector_pooled: single.report(evaluated, p.exact),
    }
}

fn print_report(r: &Report) {
    println!(
        "corpus: {} docs, {} tokens ({:.1}/doc), dim {}, {} segment(s); {} queries evaluated, \
         {} without judgments",
        r.docs,
        r.tokens,
        r.tokens as f64 / r.docs.max(1) as f64,
        r.dim,
        r.segments,
        r.queries_evaluated,
        r.queries_without_judgments
    );
    println!(
        "late interaction: nprobe {} per query token, {} candidates re-scored per segment; \
         single vector: nprobe {} per segment",
        r.nprobe, r.num_candidates, r.single_nprobe
    );
    let k = r.k;
    println!(
        "{:<34} {:>10} {:>10} {:>16} {:>9}",
        "method",
        format!("recall@{k}"),
        format!("nDCG@{k}"),
        format!("overlap exact@{k}"),
        "p50 ms"
    );
    let row = |name: &str, m: &MethodReport| {
        println!(
            "{:<34} {:>10.4} {:>10.4} {:>16} {:>9.2}",
            name,
            m.recall_at_k,
            m.ndcg_at_k,
            m.overlap_with_exact
                .map_or_else(|| "-".to_string(), |o| format!("{o:.4}")),
            m.p50_ms
        );
    };
    row("late interaction (Kosha)", &r.late_interaction);
    if let Some(exact) = &r.exact_maxsim {
        row("exact MaxSim", exact);
    }
    row("single vector (mean-pooled)", &r.single_vector_pooled);
}
