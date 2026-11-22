//! Compares segment layouts of one corpus under the query path's global
//! kNN probe budget (#380, research): postings probed, segments touched,
//! and recall@k against exact brute force, swept over budgets, so layouts
//! can be compared at matched recall.
//!
//! ```text
//! kosha-vector-quant-eval gen --out /tmp/corpus --docs 200000 --dim 384
//! kosha-build-segments --shards-dir /tmp/corpus --embeddings-dir /tmp/corpus \
//!     --emb-dim 384 --namespace input --out-dir /tmp/segs --docs-per-segment 5000
//! kosha-build-segments ... --namespace nsclust --cluster-layout namespace
//! kosha-cluster-eval --embeddings-dir /tmp/corpus --dim 384 \
//!     --layout input=/tmp/segs/input --layout nsclust=/tmp/segs/nsclust
//! ```
//!
//! The search is the one `knn_plan` + `score_segment` run: every segment's
//! centroid lower bounds (`knn_centroid_distances`) are pooled, the cutoff
//! is the `budget`-th smallest, and each segment probes only postings
//! within it (`knn_search_within_filtered`, capped at `budget`). Budgets
//! are absolute posting counts here (the server floors `num_candidates` to
//! `KOSHA_KNN_BUDGET_FRACTION` of the namespace's centroids, 5% by
//! default; `--fractions` sweeps that form instead).
//!
//! Brute force needs every corpus vector in memory: meant for synthetic or
//! sampled corpora on a workstation, not the 10M bench corpus (use the
//! server plus `recall_check.py` there).

use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use kosha_segment::SegmentReader;

#[derive(Parser)]
#[command(name = "kosha-cluster-eval")]
struct Cli {
    /// Directory with the `emb-*.f32` / `emb-*.ids` shards the layouts
    /// were built from (ground truth)
    #[arg(long)]
    embeddings_dir: PathBuf,
    #[arg(long, default_value_t = 1024)]
    dim: usize,
    /// `name=<out_dir>/<namespace>`, repeatable: one segment set per layout
    #[arg(long = "layout", required = true)]
    layouts: Vec<String>,
    #[arg(long, default_value = "text_emb")]
    field: String,
    #[arg(long, default_value_t = 200)]
    queries: usize,
    #[arg(long, default_value_t = 10)]
    k: usize,
    /// Absolute whole-query posting budgets to sweep
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "25,50,100,200,400,800,1600"
    )]
    budgets: Vec<usize>,
    /// Budgets as fractions of each layout's total centroid count (the
    /// server's `KOSHA_KNN_BUDGET_FRACTION` form); added to `--budgets`
    #[arg(long, value_delimiter = ',')]
    fractions: Vec<f64>,
    /// Recall@k the matched-recall summary compares layouts at
    #[arg(long, default_value_t = 0.95)]
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

fn segment_dirs(ns_dir: &Path) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(ns_dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", ns_dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.join("footer.json").exists())
        .collect();
    dirs.sort();
    dirs
}

struct QueryCost {
    ids: Vec<String>,
    postings: usize,
    segments: usize,
}

/// The query path's global-budget kNN over `readers`, with its cost.
fn search(
    readers: &[SegmentReader],
    distances: &[Vec<f32>],
    field: &str,
    query: &[f32],
    k: usize,
    budget: usize,
) -> QueryCost {
    let mut all: Vec<f32> = distances.iter().flatten().copied().collect();
    let cutoff = if all.len() <= budget {
        f32::INFINITY
    } else {
        all.select_nth_unstable_by(budget - 1, |a, b| {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        });
        all[budget - 1]
    };
    let mut hits: Vec<(f64, String)> = Vec::new();
    let (mut postings, mut segments) = (0, 0);
    for (reader, d) in readers.iter().zip(distances) {
        // Same skip the server gets for free: no centroid inside the cutoff
        // means the probe hydrates nothing.
        if !d.iter().any(|&x| x <= cutoff) {
            continue;
        }
        let (seg_hits, probes) = reader
            .knn_search_within_filtered(field, query, k, cutoff, budget, None)
            .expect("lazy v2 vector index")
            .expect("probe");
        postings += probes;
        if probes > 0 {
            segments += 1;
        }
        for (seq, score) in seg_hits {
            hits.push((score, reader.doc_meta(seq).unwrap().doc_id.to_string()));
        }
    }
    hits.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    QueryCost {
        ids: hits.into_iter().take(k).map(|(_, id)| id).collect(),
        postings,
        segments,
    }
}

struct Point {
    budget: usize,
    recall: f64,
    postings: f64,
    segments: f64,
    segments_p99: usize,
    ms: f64,
}

fn main() {
    let cli = Cli::parse();
    let (ids, vectors) = load_embeddings(&cli.embeddings_dir, cli.dim);
    assert!(!vectors.is_empty(), "no embeddings loaded");
    println!("corpus: {} x {}-dim vectors", vectors.len(), cli.dim);
    let mut rng = Rng(cli.seed | 1);
    let queries: Vec<Vec<f32>> = (0..cli.queries)
        .map(|_| {
            let base = &vectors[(rng.next_u64() % vectors.len() as u64) as usize];
            let mut q: Vec<f32> = base
                .iter()
                .map(|x| x + 0.5 * rng.normal() / (cli.dim as f32).sqrt())
                .collect();
            normalize(&mut q);
            q
        })
        .collect();
    let truth: Vec<Vec<String>> = queries
        .iter()
        .map(|q| {
            let mut scored: Vec<(f32, usize)> = vectors
                .iter()
                .enumerate()
                .map(|(i, v)| (dot(q, v), i))
                .collect();
            scored.select_nth_unstable_by(cli.k - 1, |a, b| b.0.partial_cmp(&a.0).unwrap());
            scored.truncate(cli.k);
            scored.into_iter().map(|(_, i)| ids[i].clone()).collect()
        })
        .collect();
    drop(vectors);

    let mut summary: Vec<(String, usize, usize, Option<Point>)> = Vec::new();
    for spec in &cli.layouts {
        let (name, dir) = spec
            .split_once('=')
            .unwrap_or_else(|| panic!("--layout {spec:?}: expected name=dir"));
        let segs = segment_dirs(Path::new(dir));
        let readers: Vec<SegmentReader> = segs
            .iter()
            .map(|s| SegmentReader::open(s.clone()).expect("open segment"))
            .collect();
        let dists: Vec<Vec<Vec<f32>>> = queries
            .iter()
            .map(|q| {
                readers
                    .iter()
                    .map(|r| {
                        r.knn_centroid_distances(&cli.field, q)
                            .expect("segment has a lazy v2 index for the field")
                    })
                    .collect()
            })
            .collect();
        let centroids: usize = dists[0].iter().map(Vec::len).sum();
        let mut budgets = cli.budgets.clone();
        budgets.extend(
            cli.fractions
                .iter()
                .map(|f| ((centroids as f64 * f).ceil() as usize).max(1)),
        );
        budgets.sort_unstable();
        budgets.dedup();
        // Postings whose radius-aware lower bound is 0 tie at the front of
        // every budget: a budget below this count cannot prune them.
        let zero_lb = dists
            .iter()
            .map(|d| d.iter().flatten().filter(|&&x| x <= 0.0).count())
            .sum::<usize>() as f64
            / dists.len() as f64;
        let zero_lb_segments = dists
            .iter()
            .map(|d| d.iter().filter(|s| s.iter().any(|&x| x <= 0.0)).count())
            .sum::<usize>() as f64
            / dists.len() as f64;
        println!(
            "\n## {name}: {} segments, {centroids} posting centroids ({:.0} vectors/posting); \
             per query {zero_lb:.1} postings in {zero_lb_segments:.1} segments have lower bound 0\n",
            readers.len(),
            ids.len() as f64 / centroids.max(1) as f64
        );
        println!(
            "| budget | recall@{k} | postings/query | segments/query | segments p99 | ms/query |",
            k = cli.k
        );
        println!("|---|---|---|---|---|---|");
        let mut best: Option<Point> = None;
        for &budget in &budgets {
            let t = Instant::now();
            let costs: Vec<QueryCost> = queries
                .iter()
                .zip(&dists)
                .map(|(q, d)| search(&readers, d, &cli.field, q, cli.k, budget))
                .collect();
            let ms = t.elapsed().as_secs_f64() * 1e3 / queries.len() as f64;
            let n = costs.len() as f64;
            let recall = costs
                .iter()
                .zip(&truth)
                .map(|(c, t)| c.ids.iter().filter(|id| t.contains(id)).count() as f64)
                .sum::<f64>()
                / (n * cli.k as f64);
            let mut segs: Vec<usize> = costs.iter().map(|c| c.segments).collect();
            segs.sort_unstable();
            let p = Point {
                budget,
                recall,
                postings: costs.iter().map(|c| c.postings).sum::<usize>() as f64 / n,
                segments: segs.iter().sum::<usize>() as f64 / n,
                segments_p99: segs[((segs.len() * 99).div_ceil(100)).saturating_sub(1)],
                ms,
            };
            println!(
                "| {} | {:.3} | {:.1} | {:.1} | {} | {:.2} |",
                p.budget, p.recall, p.postings, p.segments, p.segments_p99, p.ms
            );
            if best.is_none() && p.recall >= cli.target_recall {
                best = Some(p);
            }
        }
        summary.push((name.to_string(), readers.len(), centroids, best));
    }

    println!(
        "\n## Matched recall: cheapest swept budget reaching recall@{} >= {}\n",
        cli.k, cli.target_recall
    );
    println!(
        "| layout | segments | centroids | budget | recall | postings/query | segments/query |"
    );
    println!("|---|---|---|---|---|---|---|");
    for (name, segs, cents, best) in &summary {
        match best {
            Some(p) => println!(
                "| {name} | {segs} | {cents} | {} | {:.3} | {:.1} | {:.1} |",
                p.budget, p.recall, p.postings, p.segments
            ),
            None => println!("| {name} | {segs} | {cents} | not reached | - | - | - |"),
        }
    }
}
