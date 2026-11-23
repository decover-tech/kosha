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
    /// were built from (ground truth). Not used by `--trace`.
    #[arg(long, required_unless_present = "trace")]
    embeddings_dir: Option<PathBuf>,
    /// Probe-trace mode (K10 step 0): for the queries in `--trace-queries`
    /// over the single `--layout`, write per-(query, posting) data that a
    /// stopping rule can be replayed against offline, to
    /// `<prefix>.meta.json` plus raw little-endian arrays (see
    /// [`run_trace`]). Truth is brute force over every posting's members.
    #[arg(long, value_name = "PREFIX")]
    trace: Option<PathBuf>,
    /// JSONL of `{"id": str, "vector": [f32]}`, one query per line
    #[arg(long, requires = "trace")]
    trace_queries: Option<PathBuf>,
    /// Best member similarities kept per (query, posting), descending
    #[arg(long, default_value_t = 32)]
    trace_top: usize,
    /// Global nearest neighbours (with doc ids) kept per query
    #[arg(long, default_value_t = 64)]
    trace_neighbours: usize,
    /// Also run the real budgeted probe (`knn_search_within_filtered`, at
    /// the process's `KOSHA_KNN_PROBE_RADIUS_ALPHA`) at these absolute
    /// budgets and store its top-k ids, so a replay of the trace can be
    /// checked against the code path it models
    #[arg(long, value_delimiter = ',', requires = "trace")]
    trace_check_budgets: Vec<usize>,
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

/// Dot product with 16 independent accumulators, so it vectorizes (the
/// trace scores every member against every query: ~3e11 multiply-adds at
/// 1.8M x 110).
fn dot16(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0f32; 16];
    let ((ac, ar), (bc, br)) = (a.as_chunks::<16>(), b.as_chunks::<16>());
    let tail: f32 = ar.iter().zip(br).map(|(x, y)| x * y).sum();
    for (x, y) in ac.iter().zip(bc) {
        for i in 0..16 {
            acc[i] += x[i] * y[i];
        }
    }
    acc.iter().sum::<f32>() + tail
}

/// Keeps the `cap` largest `(score, payload)` seen, unordered until
/// [`Self::sorted`].
struct TopN<T> {
    cap: usize,
    items: Vec<(f32, T)>,
}

impl<T: Copy> TopN<T> {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            items: Vec::with_capacity(2 * cap),
        }
    }
    fn push(&mut self, score: f32, t: T) {
        self.items.push((score, t));
        if self.items.len() >= 2 * self.cap {
            self.shrink();
        }
    }
    fn shrink(&mut self) {
        if self.items.len() > self.cap {
            self.items
                .select_nth_unstable_by(self.cap - 1, |a, b| b.0.total_cmp(&a.0));
            self.items.truncate(self.cap);
        }
    }
    fn sorted(mut self) -> Vec<(f32, T)> {
        self.shrink();
        self.items.sort_by(|a, b| b.0.total_cmp(&a.0));
        self.items
    }
}

fn write_f32s(path: &Path, values: &[f32]) {
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    std::fs::write(path, bytes).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
}

fn write_u32s(path: &Path, values: &[u32]) {
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    std::fs::write(path, bytes).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
}

/// `--trace`: the data a per-query probe-stopping rule needs, so rules
/// can be replayed offline without re-reading vectors (K10 step 0).
///
/// Postings are numbered globally in segment order (`P` of them, `Q`
/// queries). Written next to `PREFIX`:
/// - `.dist.f32` `[Q][P]`: the alpha-0 rank score `knn_plan` sorts by,
///   `1 - cos(q, centroid)` (`knn_centroid_distances_with_alpha`, 0);
/// - `.c0cos.f32` `[Q][P]`: cos(centroid, the query's nearest centroid in
///   the same segment) — the geometry Quake's APS bisector model needs;
/// - `.top.f32` `[Q][P][TOP]`: the posting's best member similarities,
///   descending (`-inf` padded): enough to rebuild the exact top-k of any
///   probed set, the global truth, and a leave-one-out truth;
/// - `.post.u32` `[P][3]`: segment index, bytes in `vector.idx`, members;
/// - `.meta.json`: shapes, segment ids, query ids, and each query's global
///   nearest `--trace-neighbours` `(doc_id, sim, posting)`.
///
/// Memory: `Q x P x (TOP + 2) x 4` bytes (1.8M, Q 110, P 20,316, TOP 32:
/// 304 MB) plus one batch of decoded postings; vectors are streamed.
fn run_trace(cli: &Cli, prefix: &Path) {
    let qpath = cli.trace_queries.as_ref().expect("--trace-queries");
    let (mut qids, mut queries) = (Vec::new(), Vec::new());
    for line in std::fs::read_to_string(qpath).unwrap().lines() {
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        qids.push(v["id"].as_str().unwrap().to_string());
        let mut q: Vec<f32> = v["vector"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_f64().unwrap() as f32)
            .collect();
        normalize(&mut q);
        queries.push(q);
    }
    assert_eq!(cli.layouts.len(), 1, "--trace takes exactly one --layout");
    let dir = cli.layouts[0]
        .split_once('=')
        .map_or(cli.layouts[0].as_str(), |(_, d)| d);
    let readers: Vec<SegmentReader> = segment_dirs(Path::new(dir))
        .into_iter()
        .map(|s| SegmentReader::open(s).expect("open segment"))
        .collect();
    let (nq, top) = (queries.len(), cli.trace_top);
    println!(
        "trace: {nq} queries over {} segments in {dir}",
        readers.len()
    );

    // Per segment: (dist [q][p_local], centroids) — postings numbered in
    // segment order, so posting p of segment s is global `base[s] + p`.
    let mut dist: Vec<Vec<f32>> = vec![Vec::new(); nq];
    let mut c0cos: Vec<Vec<f32>> = vec![Vec::new(); nq];
    let mut tops: Vec<Vec<f32>> = vec![Vec::new(); nq];
    let mut post: Vec<u32> = Vec::new();
    let mut nbrs: Vec<TopN<(u32, u32, u32)>> =
        (0..nq).map(|_| TopN::new(cli.trace_neighbours)).collect();
    let started = Instant::now();
    for (s, reader) in readers.iter().enumerate() {
        let base = post.len() / 3;
        let seg_dist: Vec<Vec<f32>> = queries
            .iter()
            .map(|q| {
                reader
                    .knn_centroid_distances_with_alpha(&cli.field, q, 0.0)
                    .expect("segment has a lazy v2 index for the field")
            })
            .collect();
        let mut centroids: Vec<Vec<f32>> = Vec::new();
        // One batch of decoded postings scored in parallel: (local index,
        // ids, vectors). 256 postings of ~89 x 1536-d ≈ 140 MB.
        let mut batch: Vec<(usize, Vec<u32>, Vec<f32>)> = Vec::new();
        let flush = |batch: &mut Vec<(usize, Vec<u32>, Vec<f32>)>,
                     tops: &mut Vec<Vec<f32>>,
                     nbrs: &mut Vec<TopN<(u32, u32, u32)>>| {
            use rayon::prelude::*;
            let scored: Vec<Vec<Vec<(f32, u32)>>> = batch
                .par_iter()
                .map(|(_, ids, vecs)| {
                    let mut per_q: Vec<TopN<u32>> = (0..nq).map(|_| TopN::new(top)).collect();
                    for (id, v) in ids.iter().zip(vecs.chunks_exact(cli.dim)) {
                        for (t, q) in per_q.iter_mut().zip(&queries) {
                            t.push(dot16(q, v), *id);
                        }
                    }
                    per_q.into_iter().map(TopN::sorted).collect()
                })
                .collect();
            for ((p, _, _), per_q) in batch.iter().zip(scored) {
                for (qi, list) in per_q.into_iter().enumerate() {
                    let row = &mut tops[qi];
                    let at = (base + p) * top;
                    if row.len() < at + top {
                        row.resize(at + top, f32::NEG_INFINITY);
                    }
                    for (j, &(sim, seq)) in list.iter().enumerate() {
                        row[at + j] = sim;
                        nbrs[qi].push(sim, (s as u32, (base + p) as u32, seq));
                    }
                }
            }
            batch.clear();
        };
        let ok = reader
            .for_each_f32_vector_posting(&cli.field, |p, centroid, bytes, ids, vecs| {
                centroids.push(centroid.to_vec());
                post.extend([s as u32, bytes, ids.len() as u32]);
                batch.push((p, ids.to_vec(), vecs.to_vec()));
                if batch.len() == 256 {
                    flush(&mut batch, &mut tops, &mut nbrs);
                }
            })
            .expect("read vector.idx");
        assert!(ok, "segment {s}: not a lazy f32 column");
        flush(&mut batch, &mut tops, &mut nbrs);
        for qi in 0..nq {
            let d = &seg_dist[qi];
            let nearest = (0..d.len()).min_by(|&a, &b| d[a].total_cmp(&d[b])).unwrap();
            let c0 = &centroids[nearest];
            dist[qi].extend_from_slice(d);
            c0cos[qi].extend(centroids.iter().map(|c| dot16(c0, c)));
            tops[qi].resize((post.len() / 3) * top, f32::NEG_INFINITY);
        }
        println!(
            "  segment {s}: {} postings, {:.0}s elapsed",
            centroids.len(),
            started.elapsed().as_secs_f64()
        );
    }
    let np = post.len() / 3;
    let with = |ext: &str| {
        let mut p = prefix.as_os_str().to_owned();
        p.push(ext);
        PathBuf::from(p)
    };
    write_f32s(&with(".dist.f32"), &dist.concat());
    write_f32s(&with(".c0cos.f32"), &c0cos.concat());
    write_f32s(&with(".top.f32"), &tops.concat());
    write_u32s(&with(".post.u32"), &post);
    let neighbours: Vec<serde_json::Value> = nbrs
        .into_iter()
        .map(|t| {
            t.sorted()
                .into_iter()
                .map(|(sim, (s, p, seq))| {
                    let id = readers[s as usize].doc_meta(seq).unwrap().doc_id;
                    serde_json::json!([id, sim, p])
                })
                .collect()
        })
        .collect();
    let mut checks = serde_json::Map::new();
    if !cli.trace_check_budgets.is_empty() {
        let dists: Vec<Vec<Vec<f32>>> = queries
            .iter()
            .map(|q| {
                readers
                    .iter()
                    .map(|r| r.knn_centroid_distances(&cli.field, q).unwrap())
                    .collect()
            })
            .collect();
        for &budget in &cli.trace_check_budgets {
            let ids: Vec<Vec<String>> = queries
                .iter()
                .zip(&dists)
                .map(|(q, d)| search(&readers, d, &cli.field, q, cli.k, budget).ids)
                .collect();
            checks.insert(budget.to_string(), serde_json::json!(ids));
        }
    }
    let meta = serde_json::json!({
        "probe_check": checks,
        "k": cli.k,
        "queries": nq,
        "postings": np,
        "top": top,
        "dim": cli.dim,
        "segments": readers.iter().map(|r| r.segment_dir().display().to_string()).collect::<Vec<_>>(),
        "query_ids": qids,
        "neighbours": neighbours,
    });
    std::fs::write(with(".meta.json"), meta.to_string()).unwrap();
    println!(
        "trace written: {} queries x {np} postings in {:.0}s",
        nq,
        started.elapsed().as_secs_f64()
    );
}

fn main() {
    let cli = Cli::parse();
    if let Some(prefix) = &cli.trace {
        return run_trace(&cli, prefix);
    }
    let (ids, vectors) = load_embeddings(cli.embeddings_dir.as_ref().unwrap(), cli.dim);
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
