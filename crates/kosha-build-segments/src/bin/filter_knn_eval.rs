//! Predicate-aware posting ranking for filtered kNN (#465, research).
//!
//! The query path widens a filtered query's global probe budget by
//! `1 / selectivity` (capped at `KOSHA_KNN_FILTER_BUDGET_MAX_SCALE`, 8),
//! assuming the passing documents are spread evenly over the postings.
//! This tool measures what that costs when they are not, and what ranking
//! only the postings holding at least one passing document saves:
//!
//! - `baseline`: the query path today — the global cutoff over every
//!   posting at the widened budget, probes filtered by the predicate.
//! - `pass-aware`: postings with no passing document are ranked last
//!   (bound `+inf`), so the cutoff is taken over the others only, and the
//!   budget is widened by the selectivity *within* them.
//!
//! Each runs at the same base budgets, so the two can be compared at
//! matched recall@k (brute force over the passing documents). For each
//! filter the tool also reports the share of `baseline`'s probed postings
//! that held no passing document, and the filter's query correlation in
//! the spirit of ACORN (Patel et al., SIGMOD 2024): the mean similarity of
//! the filtered top-k minus that of a random filter's top-k at the same
//! selectivity (`> 0`: passing documents sit near the query; `< 0`: far).
//!
//! Filters, per selectivity `s` (`--selectivities`, default 10%, 1%,
//! 0.1%):
//!
//! - `random`: each document passes with probability `s` (uncorrelated).
//! - `clustered`: the `s·N` documents nearest one fixed random corpus
//!   vector, the same for every query (a tenant or category that follows
//!   the embedding).
//! - `near`: per query, the `s·N` documents nearest the query (positive
//!   correlation).
//! - `far`: per query, the `s·N` documents farthest from it (negative).
//!
//! ```text
//! kosha-vector-quant-eval gen --out /tmp/c --docs 200000 --dim 384
//! kosha-build-segments --shards-dir /tmp/c --embeddings-dir /tmp/c \
//!     --emb-dim 384 --namespace ns --out-dir /tmp/segs --docs-per-segment 20000
//! kosha-filter-knn-eval --embeddings-dir /tmp/c --dim 384 --segments /tmp/segs/ns
//! ```
//!
//! Brute force holds every corpus vector and every query's similarities in
//! memory: meant for synthetic or sampled corpora, like
//! `kosha-cluster-eval`.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use clap::Parser;
use kosha_segment::{KnnRefill, KnnSegmentRanking, SegmentReader};

#[derive(Parser)]
#[command(name = "kosha-filter-knn-eval")]
struct Cli {
    /// Directory with the `emb-*.f32` / `emb-*.ids` shards the segments
    /// were built from (ground truth)
    #[arg(long)]
    embeddings_dir: PathBuf,
    #[arg(long, default_value_t = 1024)]
    dim: usize,
    /// `<out_dir>/<namespace>`: the segment set to probe
    #[arg(long)]
    segments: PathBuf,
    #[arg(long, default_value = "text_emb")]
    field: String,
    #[arg(long, default_value_t = 100)]
    queries: usize,
    #[arg(long, default_value_t = 10)]
    k: usize,
    #[arg(long, value_delimiter = ',', default_value = "0.1,0.01,0.001")]
    selectivities: Vec<f64>,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "random,clustered,near,far"
    )]
    filters: Vec<String>,
    /// Base (unfiltered) whole-query posting budgets to sweep; each
    /// strategy widens them by its own selectivity estimate
    #[arg(long, value_delimiter = ',', default_value = "10,20,40,80,160,320")]
    budgets: Vec<usize>,
    /// Cap on the selectivity widening, as `KOSHA_KNN_FILTER_BUDGET_MAX_SCALE`
    #[arg(long, default_value_t = 8.0)]
    max_scale: f64,
    /// Recall@k the matched-recall summary compares strategies at
    #[arg(long, default_value_t = 0.9)]
    target_recall: f64,
    #[arg(long, default_value_t = 7)]
    seed: u64,
}

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
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn normalize(v: &mut [f32]) {
    let n = dot(v, v).sqrt();
    if n > 0.0 {
        v.iter_mut().for_each(|x| *x /= n);
    }
}

fn load_embeddings(dir: &Path, dim: usize) -> (Vec<String>, Vec<Vec<f32>>) {
    let mut shards: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("emb-") && n.ends_with(".f32"))
        })
        .collect();
    shards.sort();
    let (mut ids, mut vecs) = (Vec::new(), Vec::new());
    for shard in shards {
        let names = std::fs::read_to_string(shard.with_extension("ids")).unwrap();
        let mut bytes = Vec::new();
        std::fs::File::open(&shard)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        for (id, chunk) in names.lines().zip(bytes.chunks_exact(dim * 4)) {
            ids.push(id.to_string());
            let mut v: Vec<f32> = chunk
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect();
            normalize(&mut v);
            vecs.push(v);
        }
    }
    (ids, vecs)
}

/// One open segment: its reader, each doc seq's corpus index, and each
/// posting's members as corpus indices.
struct Segment {
    reader: SegmentReader,
    corpus_index: Vec<usize>,
    members: Vec<Vec<usize>>,
}

fn open_segments(dir: &Path, field: &str, index_of: &HashMap<&str, usize>) -> Vec<Segment> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.join("footer.json").exists())
        .collect();
    dirs.sort();
    dirs.into_iter()
        .map(|d| {
            let reader = SegmentReader::open(d).expect("open segment");
            let corpus_index: Vec<usize> = (0..reader.doc_count())
                .map(|seq| {
                    let id = reader.doc_meta(seq).unwrap().doc_id.to_string();
                    *index_of
                        .get(id.as_str())
                        .unwrap_or_else(|| panic!("doc {id} not in the embeddings"))
                })
                .collect();
            let layout = reader
                .vector_posting_layout(field)
                .expect("read posting layout")
                .expect("lazy v2 vector index");
            let members = layout
                .postings
                .iter()
                .map(|p| {
                    p.doc_seqs
                        .iter()
                        .map(|&s| corpus_index[s as usize])
                        .collect()
                })
                .collect();
            Segment {
                reader,
                corpus_index,
                members,
            }
        })
        .collect()
}

/// The `n` corpus indices with the highest `score`.
fn top_n(scores: &[f32], n: usize) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..scores.len()).collect();
    if n < idx.len() {
        idx.select_nth_unstable_by(n, |&a, &b| scores[b].total_cmp(&scores[a]));
        idx.truncate(n);
    }
    idx
}

fn filtered_top_k(sims: &[f32], pass: &[bool], k: usize) -> Vec<usize> {
    let masked: Vec<f32> = sims
        .iter()
        .zip(pass)
        .map(|(&s, &p)| if p { s } else { f32::NEG_INFINITY })
        .collect();
    let mut top = top_n(&masked, k);
    top.retain(|&i| pass[i]);
    top
}

fn kth_smallest(mut all: Vec<f32>, budget: usize) -> f32 {
    if budget == 0 || all.len() <= budget {
        return f32::INFINITY;
    }
    all.select_nth_unstable_by(budget - 1, f32::total_cmp);
    all[budget - 1]
}

#[derive(Default, Clone, Copy)]
struct Tally {
    recall: f64,
    postings: f64,
    empty_postings: f64,
}

#[derive(Clone, Copy, PartialEq)]
enum Strategy {
    Baseline,
    PassAware,
}

/// One filtered query under `strategy` at base `budget`: (recall@k,
/// postings fetched, fetched postings with no passing document).
#[allow(clippy::too_many_arguments)]
fn run(
    segments: &[Segment],
    distances: &[Vec<f32>],
    field: &str,
    query: &[f32],
    k: usize,
    pass: &[bool],
    truth: &[usize],
    budget: usize,
    max_scale: f64,
    strategy: Strategy,
) -> Tally {
    let widen = |s: f64| {
        if s > 0.0 && s < 1.0 {
            ((budget as f64) * (1.0 / s).min(max_scale)).ceil() as usize
        } else {
            budget
        }
    };
    let passes: Vec<Vec<usize>> = segments
        .iter()
        .map(|seg| {
            seg.members
                .iter()
                .map(|m| m.iter().filter(|&&i| pass[i]).count())
                .collect()
        })
        .collect();
    let bounds: Vec<Vec<f32>> = match strategy {
        Strategy::Baseline => distances.to_vec(),
        Strategy::PassAware => distances
            .iter()
            .zip(&passes)
            .map(|(d, p)| {
                d.iter()
                    .zip(p)
                    .map(|(&d, &p)| if p > 0 { d } else { f32::INFINITY })
                    .collect()
            })
            .collect(),
    };
    let (mut members, mut passing) = (0usize, 0usize);
    for (seg, p) in segments.iter().zip(&passes) {
        for (m, &n) in seg.members.iter().zip(p) {
            if strategy == Strategy::Baseline || n > 0 {
                members += m.len();
                passing += n;
            }
        }
    }
    let selectivity = passing as f64 / members.max(1) as f64;
    let widened = widen(selectivity);
    // `pass-aware` never takes the cutoff past its last finite bound, so a
    // budget wider than the postings with a passing document stops there.
    let finite: Vec<f32> = bounds
        .iter()
        .flatten()
        .copied()
        .filter(|d| d.is_finite())
        .collect();
    let last = finite.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let cutoff = match (strategy, kth_smallest(finite, widened)) {
        (Strategy::PassAware, c) if c.is_infinite() => last,
        (_, c) => c,
    };
    let mut hits: Vec<(f64, usize)> = Vec::new();
    let (mut fetched, mut empty) = (0usize, 0usize);
    for ((seg, b), p) in segments.iter().zip(&bounds).zip(&passes) {
        // Search skips a segment where the filter matches nothing.
        if p.iter().all(|&n| n == 0) {
            continue;
        }
        let accept = |seq: u32| pass[seg.corpus_index[seq as usize]];
        let ranking = KnnSegmentRanking::new(b.clone(), cutoff, widened);
        let (seg_hits, probes) = seg
            .reader
            .knn_search_planned_fetching(
                field,
                query,
                k,
                &ranking,
                Some(&accept),
                KnnRefill::Always,
                None,
            )
            .expect("lazy v2 vector index")
            .expect("probe");
        fetched += probes;
        // Probes run nearest bound first: the first `probes` of this
        // segment's order are the postings fetched.
        let mut order: Vec<usize> = (0..b.len()).collect();
        order.sort_by(|&x, &y| b[x].total_cmp(&b[y]));
        empty += order.iter().take(probes).filter(|&&j| p[j] == 0).count();
        hits.extend(
            seg_hits
                .into_iter()
                .map(|(seq, score)| (score, seg.corpus_index[seq as usize])),
        );
    }
    hits.sort_by(|a, b| b.0.total_cmp(&a.0));
    hits.truncate(k);
    let found = hits.iter().filter(|(_, i)| truth.contains(i)).count();
    Tally {
        recall: if truth.is_empty() {
            1.0
        } else {
            found as f64 / truth.len() as f64
        },
        postings: fetched as f64,
        empty_postings: empty as f64,
    }
}

fn main() {
    let cli = Cli::parse();
    let (ids, vectors) = load_embeddings(&cli.embeddings_dir, cli.dim);
    assert!(!vectors.is_empty(), "no embeddings loaded");
    let n = vectors.len();
    let index_of: HashMap<&str, usize> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.as_str(), i))
        .collect();
    let segments = open_segments(&cli.segments, &cli.field, &index_of);
    let total_postings: usize = segments.iter().map(|s| s.members.len()).sum();
    println!(
        "corpus: {n} x {}-dim vectors, {} segments, {total_postings} postings",
        cli.dim,
        segments.len()
    );
    let mut rng = Rng(cli.seed | 1);
    let queries: Vec<Vec<f32>> = (0..cli.queries)
        .map(|_| {
            let base = &vectors[(rng.next_u64() % n as u64) as usize];
            let mut q: Vec<f32> = base
                .iter()
                .map(|x| x + 0.5 * rng.normal() / (cli.dim as f32).sqrt())
                .collect();
            normalize(&mut q);
            q
        })
        .collect();
    let sims: Vec<Vec<f32>> = queries
        .iter()
        .map(|q| vectors.iter().map(|v| dot(q, v)).collect())
        .collect();
    let anchor = &vectors[(rng.next_u64() % n as u64) as usize];
    let anchor_sims: Vec<f32> = vectors.iter().map(|v| dot(anchor, v)).collect();
    let distances: Vec<Vec<Vec<f32>>> = queries
        .iter()
        .map(|q| {
            segments
                .iter()
                .map(|s| {
                    s.reader
                        .knn_centroid_distances(&cli.field, q)
                        .expect("lazy v2 vector index")
                })
                .collect()
        })
        .collect();
    let random_pass = |s: f64, salt: u64| -> Vec<bool> {
        let mut r = Rng((cli.seed ^ salt).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        (0..n).map(|_| (r.uniform() as f64) < s).collect()
    };
    // A segment set built with `--max-docs` holds fewer documents than the
    // embeddings: only indexed ones can pass, so the truth stays reachable.
    let mut indexed = vec![false; n];
    for seg in &segments {
        seg.corpus_index.iter().for_each(|&i| indexed[i] = true);
    }
    let n_indexed = indexed.iter().filter(|&&x| x).count();
    let random_pass = |s: f64, salt: u64| -> Vec<bool> {
        let mut pass = random_pass(s, salt);
        pass.iter_mut().zip(&indexed).for_each(|(p, &x)| *p &= x);
        pass
    };
    let mask = |chosen: Vec<usize>| {
        let mut pass = vec![false; n];
        chosen.into_iter().for_each(|i| pass[i] = true);
        pass
    };
    let unindexed_low = |scores: &[f32]| -> Vec<f32> {
        scores
            .iter()
            .zip(&indexed)
            .map(|(&s, &x)| if x { s } else { f32::NEG_INFINITY })
            .collect()
    };

    println!(
        "\n{:>9} {:>6} {:>6} {:>12} {:>7} {:>8} {:>8} {:>8}",
        "filter", "sel", "corr", "strategy", "budget", "recall", "postings", "empty%"
    );
    let mut summary = Vec::new();
    for filter in &cli.filters {
        for &s in &cli.selectivities {
            let count = ((s * n_indexed as f64).round() as usize).clamp(1, n_indexed.max(1));
            let fixed = match filter.as_str() {
                "random" => Some(random_pass(s, 0x51)),
                "clustered" => Some(mask(top_n(&unindexed_low(&anchor_sims), count))),
                "near" | "far" => None,
                other => panic!("--filters: unknown filter {other:?}"),
            };
            let control = random_pass(s, 0xC0);
            let passes: Vec<Vec<bool>> = sims
                .iter()
                .map(|qs| match (&fixed, filter.as_str()) {
                    (Some(p), _) => p.clone(),
                    (None, "near") => mask(top_n(&unindexed_low(qs), count)),
                    _ => {
                        let neg: Vec<f32> = qs.iter().map(|x| -x).collect();
                        mask(top_n(&unindexed_low(&neg), count))
                    }
                })
                .collect();
            let truths: Vec<Vec<usize>> = sims
                .iter()
                .zip(&passes)
                .map(|(qs, p)| filtered_top_k(qs, p, cli.k))
                .collect();
            let mean_sim = |qs: &[f32], top: &[usize]| {
                top.iter().map(|&i| qs[i] as f64).sum::<f64>() / top.len().max(1) as f64
            };
            let corr = sims
                .iter()
                .zip(&truths)
                .map(|(qs, t)| mean_sim(qs, t) - mean_sim(qs, &filtered_top_k(qs, &control, cli.k)))
                .sum::<f64>()
                / sims.len() as f64;
            for (strategy, label) in [
                (Strategy::Baseline, "baseline"),
                (Strategy::PassAware, "pass-aware"),
            ] {
                let mut points = Vec::new();
                for &budget in &cli.budgets {
                    let mut t = Tally::default();
                    for (qi, q) in queries.iter().enumerate() {
                        let r = run(
                            &segments,
                            &distances[qi],
                            &cli.field,
                            q,
                            cli.k,
                            &passes[qi],
                            &truths[qi],
                            budget,
                            cli.max_scale,
                            strategy,
                        );
                        t.recall += r.recall;
                        t.postings += r.postings;
                        t.empty_postings += r.empty_postings;
                    }
                    let nq = queries.len() as f64;
                    let empty = 100.0 * t.empty_postings / t.postings.max(1.0);
                    let (recall, postings) = (t.recall / nq, t.postings / nq);
                    println!(
                        "{filter:>9} {s:>6} {corr:>6.3} {label:>12} {budget:>7} {recall:>8.3} {postings:>8.1} {empty:>7.1}%"
                    );
                    points.push((recall, postings));
                }
                let matched = points
                    .iter()
                    .find(|(r, _)| *r >= cli.target_recall)
                    .map(|&(_, p)| p);
                summary.push((filter.clone(), s, label, matched));
            }
        }
    }

    println!(
        "\npostings/query at recall@{} >= {} (first budget reaching it):",
        cli.k, cli.target_recall
    );
    println!(
        "{:>9} {:>6} {:>12} {:>12} {:>7}",
        "filter", "sel", "baseline", "pass-aware", "ratio"
    );
    for pair in summary.chunks(2) {
        let [(filter, s, _, base), (_, _, _, aware)] = pair else {
            continue;
        };
        let show = |p: &Option<f64>| p.map_or("-".to_string(), |p| format!("{p:.1}"));
        let ratio = match (base, aware) {
            (Some(b), Some(a)) if *a > 0.0 => format!("{:.2}x", b / a),
            _ => "-".into(),
        };
        println!(
            "{filter:>9} {s:>6} {:>12} {:>12} {ratio:>7}",
            show(base),
            show(aware)
        );
    }
}
