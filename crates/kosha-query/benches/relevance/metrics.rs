//! Pure ranking-quality metrics for the `relevance` bench (#258): TREC
//! qrels parsing, nDCG@k, recall@k and set recall against brute force.
//! Kept free of any Kosha type so `tests/relevance_metrics.rs` can include
//! it with `#[path]` and unit-test it under `cargo test`.

use std::collections::{BTreeMap, HashMap, HashSet};

/// Graded judgments: `qid -> (doc id -> grade)`. Only grades above zero
/// are kept; a judged-irrelevant document is the same as an unjudged one
/// for nDCG and recall.
pub type Qrels = BTreeMap<String, HashMap<String, u32>>;

/// Parses TREC qrels (`qid iteration docid grade`, whitespace separated).
/// Blank lines and lines starting with `#` are skipped. A document judged
/// twice for one query keeps the higher grade.
pub fn parse_qrels(text: &str) -> Result<Qrels, String> {
    let mut qrels = Qrels::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let cols: Vec<&str> = line.split_whitespace().collect();
        let [qid, _iter, doc, grade] = cols[..] else {
            return Err(format!(
                "qrels line {}: want `qid 0 docid grade`, got {line:?}",
                n + 1
            ));
        };
        let grade: i64 = grade
            .parse()
            .map_err(|_| format!("qrels line {}: grade {grade:?} is not an integer", n + 1))?;
        let judged = qrels.entry(qid.to_string()).or_default();
        if grade > 0 {
            let g = judged.entry(doc.to_string()).or_insert(0);
            *g = (*g).max(grade as u32);
        }
    }
    qrels.retain(|_, docs| !docs.is_empty());
    Ok(qrels)
}

fn gain(grade: u32) -> f64 {
    (2f64).powi(grade as i32) - 1.0
}

fn discount(rank: usize) -> f64 {
    // rank is 0-based: the first hit is divided by log2(2) = 1.
    (rank as f64 + 2.0).log2()
}

/// nDCG@k with exponential gain `2^grade - 1` and a `log2(rank + 1)`
/// discount (the trec_eval / Elasticsearch `dcg` formula). `None` when the
/// query has no relevant document, so it cannot be scored.
pub fn ndcg_at(ranked: &[String], judged: &HashMap<String, u32>, k: usize) -> Option<f64> {
    let mut ideal: Vec<u32> = judged.values().copied().filter(|g| *g > 0).collect();
    if ideal.is_empty() {
        return None;
    }
    ideal.sort_unstable_by(|a, b| b.cmp(a));
    let idcg: f64 = ideal
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, g)| gain(*g) / discount(i))
        .sum();
    let mut seen = HashSet::new();
    let dcg: f64 = ranked
        .iter()
        .take(k)
        .enumerate()
        .filter(|(_, id)| seen.insert(id.as_str()))
        .map(|(i, id)| gain(judged.get(id).copied().unwrap_or(0)) / discount(i))
        .sum();
    Some(dcg / idcg)
}

/// Fraction of the relevant (grade > 0) documents found in the first `k`
/// hits. `None` when the query has no relevant document.
pub fn recall_at(ranked: &[String], judged: &HashMap<String, u32>, k: usize) -> Option<f64> {
    let relevant = judged.values().filter(|g| **g > 0).count();
    if relevant == 0 {
        return None;
    }
    let found: HashSet<&str> = ranked
        .iter()
        .take(k)
        .filter(|id| judged.get(*id).is_some_and(|g| *g > 0))
        .map(String::as_str)
        .collect();
    Some(found.len() as f64 / relevant as f64)
}

/// Fraction of `truth` (the exact neighbours) that `found` contains. An
/// empty truth (nothing passes the filter) is `None`.
pub fn set_recall(found: &[String], truth: &[String]) -> Option<f64> {
    if truth.is_empty() {
        return None;
    }
    let found: HashSet<&str> = found.iter().map(String::as_str).collect();
    let hits = truth.iter().filter(|t| found.contains(t.as_str())).count();
    Some(hits as f64 / truth.len() as f64)
}

/// Running mean over the queries that could be scored.
#[derive(Debug, Default, Clone, Copy)]
pub struct Mean {
    pub sum: f64,
    pub n: usize,
}

impl Mean {
    pub fn add(&mut self, v: Option<f64>) {
        if let Some(v) = v {
            self.sum += v;
            self.n += 1;
        }
    }

    pub fn value(&self) -> Option<f64> {
        (self.n > 0).then(|| self.sum / self.n as f64)
    }
}

/// Cosine similarity in f64, the metric Kosha's kNN ranks by.
pub fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let (mut dot, mut na, mut nb) = (0f64, 0f64, 0f64);
    for (x, y) in a.iter().zip(b) {
        let (x, y) = (*x as f64, *y as f64);
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// The `k` ids with the highest cosine to `q` among `candidates`
/// (`(id, vector)`), ties broken by id so the truth set is deterministic.
pub fn brute_force_top_k<'a>(
    q: &[f32],
    candidates: impl Iterator<Item = (&'a str, &'a [f32])>,
    k: usize,
) -> Vec<String> {
    let mut scored: Vec<(f64, &str)> = candidates.map(|(id, v)| (cosine(q, v), id)).collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    scored
        .into_iter()
        .take(k)
        .map(|(_, id)| id.to_string())
        .collect()
}
