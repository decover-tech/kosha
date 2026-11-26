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
//! default; `--fractions` converts fractions of the unpruned layout's
//! centroid count to fixed budgets, so pruning never changes the sweep grid).
//! `--segment-nprobes` also sweeps coarse pruning on the same segments;
//! segment opens are modeled, since the evaluator pre-opens all readers.
//!
//! Brute force needs every corpus vector in memory: meant for synthetic or
//! sampled corpora on a workstation, not the 10M bench corpus (use the
//! server plus `recall_check.py` there).
//!
//! Add `--radius-study` to recompute p50, p90 and max from the supplied
//! exact embeddings and the existing posting memberships (no rewrite).
//! For example, add `--radius-study --fractions 0.01,0.02,0.03,0.05`
//! to the invocation above.
//! The study reports clamp-to-zero rates and matched-recall curves with
//! absolute probe counts, including cutoff ties, and vectors scanned.
//! It isolates ordering: no ANN entry quantization, latency, production
//! budget floor, filters, or delete overlays are modeled.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use kosha_segment::{centroid_probe_score, KnnSegmentRanking, PostingRadii, SegmentReader};

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
    /// Fixed budgets as fractions of each unpruned layout's centroid count;
    /// added to `--budgets` and held constant across segment probe counts
    #[arg(long, value_delimiter = ',')]
    fractions: Vec<f64>,
    /// Also run with these coarse segment probe counts (0 = pruning off)
    #[arg(long, value_delimiter = ',')]
    segment_nprobes: Vec<usize>,
    /// Recall@k the matched-recall summary compares layouts at
    #[arg(long, default_value_t = 0.95)]
    target_recall: f64,
    #[arg(long, default_value_t = 7)]
    seed: u64,
    /// Compare max/p90/p50/raw probe ordering on existing posting layouts
    /// with exact member scores, independently of index format and runtime flags
    #[arg(long)]
    radius_study: bool,
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
    if cli.radius_study {
        radius_study(&cli, &ids, &vectors, &queries);
        return;
    }
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

    let mut nprobes = cli.segment_nprobes;
    nprobes.push(0);
    nprobes.sort_unstable();
    nprobes.dedup();
    let mut summary = Vec::new();
    for spec in &cli.layouts {
        let (name, dir) = spec
            .split_once('=')
            .unwrap_or_else(|| panic!("--layout {spec:?}: expected name=dir"));
        let segs = segment_dirs(Path::new(dir));
        let readers: Vec<SegmentReader> = segs
            .iter()
            .map(|s| SegmentReader::open(s.clone()).expect("open segment"))
            .collect();
        let centroids: usize = readers
            .iter()
            .map(|r| {
                r.knn_centroid_count(&cli.field, cli.dim)
                    .expect("segment has a lazy v2 index for the field")
            })
            .sum();
        let summaries: Vec<_> = readers
            .iter()
            .enumerate()
            .filter_map(|(i, r)| {
                let fields = r.footer().vector_fields.as_ref()?;
                let field = fields.iter().find(|f| f.name == cli.field)?;
                Some((i, field.knn_segment_centroid.as_deref()?))
            })
            .collect();
        for &nprobe in &nprobes {
            let dists: Vec<Vec<Vec<f32>>> = queries
                .iter()
                .map(|q| {
                    let pruned: std::collections::HashSet<_> =
                        kosha_segment::knn_pruned_by_centroid(q, summaries.iter().copied(), nprobe)
                            .into_iter()
                            .collect();
                    readers
                        .iter()
                        .enumerate()
                        .map(|(i, r)| {
                            if pruned.contains(&i) {
                                return Vec::new();
                            }
                            r.knn_centroid_distances(&cli.field, q)
                                .expect("segment has a lazy v2 index for the field")
                        })
                        .collect()
                })
                .collect();
            let opened = dists
                .iter()
                .map(|d| d.iter().filter(|distances| !distances.is_empty()).count())
                .sum::<usize>() as f64
                / queries.len() as f64;
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
                "\n## {name}, segment nprobe {nprobe}: {} segments, {centroids} posting centroids ({:.0} vectors/posting); \
                 {opened:.1} segments opened per query; \
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
            summary.push((name.to_string(), nprobe, readers.len(), opened, best));
        }
    }

    println!(
        "\n## Matched recall: cheapest swept budget reaching recall@{} >= {}\n",
        cli.k, cli.target_recall
    );
    println!(
        "| layout | segment nprobe (0 = off) | segments | budget | recall | postings/query | segments opened/query | segments probed/query |"
    );
    println!("|---|---|---|---|---|---|---|---|");
    for (name, nprobe, segs, opened, best) in &summary {
        match best {
            Some(p) => println!(
                "| {name} | {nprobe} | {segs} | {} | {:.3} | {:.1} | {opened:.1} | {:.1} |",
                p.budget, p.recall, p.postings, p.segments
            ),
            None => {
                println!("| {name} | {nprobe} | {segs} | not reached | - | - | {opened:.1} | - |")
            }
        }
    }
}

struct RadiusPosting {
    members: Vec<usize>,
    radii: PostingRadii,
}

type RadiusMode = (&'static str, fn(PostingRadii) -> f32);

fn rank_cutoff(mut scores: Vec<f32>, budget: usize) -> f32 {
    if scores.len() <= budget {
        f32::INFINITY
    } else {
        *scores.select_nth_unstable_by(budget - 1, f32::total_cmp).1
    }
}

fn keep_top_k(ids: &mut Vec<usize>, scores: &[f32], k: usize) {
    if ids.len() > k {
        ids.select_nth_unstable_by(k - 1, |&a, &b| {
            scores[b].total_cmp(&scores[a]).then(a.cmp(&b))
        });
        ids.truncate(k);
    }
}

fn radius_study(cli: &Cli, ids: &[String], vectors: &[Vec<f32>], queries: &[Vec<f32>]) {
    assert!((1..=vectors.len()).contains(&cli.k) && !queries.is_empty());
    assert!(cli.budgets.iter().all(|&b| b > 0));
    let by_id: HashMap<&str, usize> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.as_str(), i))
        .collect();
    assert_eq!(by_id.len(), ids.len(), "duplicate embedding IDs");
    for spec in &cli.layouts {
        let (name, dir) = spec.split_once('=').expect("--layout expects name=dir");
        let readers: Vec<_> = segment_dirs(Path::new(dir))
            .into_iter()
            .map(|dir| SegmentReader::open(dir).expect("open segment"))
            .collect();
        let mut seen = vec![false; ids.len()];
        let postings: Vec<Vec<_>> = readers
            .iter()
            .map(|reader| {
                let layout = reader
                    .vector_posting_layout(&cli.field)
                    .expect("read posting layout")
                    .expect("segment has clustered postings for field");
                assert_eq!(layout.dim, cli.dim);
                layout
                    .postings
                    .into_iter()
                    .map(|posting| {
                        let members: Vec<_> = posting
                            .doc_seqs
                            .iter()
                            .map(|&seq| {
                                let id =
                                    reader.doc_meta(seq).expect("posting member exists").doc_id;
                                let i = by_id[id];
                                assert!(
                                    !std::mem::replace(&mut seen[i], true),
                                    "duplicate posting member {id}"
                                );
                                i
                            })
                            .collect();
                        let mut centroid = posting.centroid;
                        normalize(&mut centroid);
                        let mut distances: Vec<_> = members
                            .iter()
                            .map(|&i| 1.0 - dot(&centroid, &vectors[i]).clamp(-1.0, 1.0))
                            .collect();
                        RadiusPosting {
                            members,
                            radii: PostingRadii::from_distances(&mut distances),
                        }
                    })
                    .collect()
            })
            .collect();
        assert!(
            seen.into_iter().all(|present| present),
            "layout is missing corpus vectors"
        );
        let total: usize = postings.iter().map(Vec::len).sum();
        println!(
            "\n## {name}: {} segments, {total} postings, {} queries",
            readers.len(),
            queries.len()
        );
        let modes: [RadiusMode; 4] = [
            ("max", |r| r.max),
            ("p90", |r| r.p90),
            ("p50", |r| r.p50),
            ("raw", |_| 0.0),
        ];
        for &(mode, radius) in &modes[..3] {
            let mut values: Vec<_> = postings.iter().flatten().map(|p| radius(p.radii)).collect();
            let spread = PostingRadii::from_distances(&mut values);
            println!(
                "{mode} radii across postings: p50={:.4} p90={:.4} max={:.4}",
                spread.p50, spread.p90, spread.max
            );
        }
        let mut budgets = cli.budgets.clone();
        budgets.extend(
            cli.fractions
                .iter()
                .map(|f| ((total as f64 * f).ceil() as usize).max(1)),
        );
        budgets.sort_unstable();
        budgets.dedup();
        let n = queries.len() as f64;
        let mut zero_scores = [0; 4];
        let mut scanned = vec![vec![(0usize, 0usize); budgets.len()]; modes.len()];
        let mut recall = vec![vec![Vec::with_capacity(queries.len()); budgets.len()]; modes.len()];
        for query in queries {
            let candidate_scores: Vec<_> = vectors.iter().map(|v| dot(query, v)).collect();
            let mut truth: Vec<_> = (0..vectors.len()).collect();
            keep_top_k(&mut truth, &candidate_scores, cli.k);
            let raw: Vec<_> = readers
                .iter()
                .map(|reader| {
                    reader
                        .knn_centroid_distances_with_alpha(&cli.field, query, 0.0)
                        .expect("score centroids")
                })
                .collect();
            for (m, &(_, radius)) in modes.iter().enumerate() {
                let scores: Vec<Vec<_>> = postings
                    .iter()
                    .zip(&raw)
                    .map(|(ps, ds)| {
                        assert_eq!(ps.len(), ds.len());
                        ps.iter()
                            .zip(ds)
                            .map(|(p, &d)| centroid_probe_score(d, radius(p.radii), 1.0))
                            .collect()
                    })
                    .collect();
                zero_scores[m] += scores.iter().flatten().filter(|&&s| s == 0.0).count();
                for (b, &budget) in budgets.iter().enumerate() {
                    let cutoff = rank_cutoff(scores.iter().flatten().copied().collect(), budget);
                    let mut found = Vec::new();
                    for (ps, ds) in postings.iter().zip(&scores) {
                        let ranking = KnnSegmentRanking::new(ds.clone(), cutoff, budget);
                        scanned[m][b].0 += ranking.budgeted().len();
                        for &p in ranking.budgeted() {
                            found.extend_from_slice(&ps[p as usize].members);
                        }
                    }
                    scanned[m][b].1 += found.len();
                    keep_top_k(&mut found, &candidate_scores, cli.k);
                    let hits = found.iter().filter(|i| truth.contains(i)).count();
                    recall[m][b].push(hits);
                }
            }
        }
        println!("| mode | zero scores | budget | mean recall@{} | p10 recall | postings/query | vectors/query |", cli.k);
        println!("|---|---|---|---|---|---|---|");
        for (m, &(mode, _)) in modes.iter().enumerate() {
            let mut best: Option<f64> = None;
            for (b, budget) in budgets.iter().enumerate() {
                let zero = 100.0 * zero_scores[m] as f64 / (n * total as f64);
                let hits = &mut recall[m][b];
                hits.sort_unstable();
                let mean = hits.iter().sum::<usize>() as f64 / (n * cli.k as f64);
                let p10 = hits[hits.len().div_ceil(10) - 1] as f64 / cli.k as f64;
                let (posts, members) = scanned[m][b];
                let posts = posts as f64 / n;
                let members = members as f64 / n;
                println!("| {mode} | {zero:.2}% | {budget} | {mean:.4} | {p10:.2} | {posts:.1} | {members:.1} |");
                if mean >= cli.target_recall {
                    best = Some(best.map_or(posts, |prior| prior.min(posts)));
                }
            }
            match best {
                Some(posts) => println!(
                    "{mode}: {posts:.1} postings/query at mean recall >= {}",
                    cli.target_recall
                ),
                None => println!(
                    "{mode}: target mean recall {} not reached",
                    cli.target_recall
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn radius_study_counts_ties_and_ranks_exact_hits() {
        let a = vec![0.0, 0.0];
        let b = vec![0.0, 0.4, 0.7];
        let cutoff = rank_cutoff(a.iter().chain(&b).copied().collect(), 1);
        let a = KnnSegmentRanking::new(a, cutoff, 1);
        let b = KnnSegmentRanking::new(b, cutoff, 1);
        assert_eq!(a.budgeted(), &[0]);
        assert_eq!(b.budgeted(), &[0]);
        let mut found = vec![3, 1, 0];
        keep_top_k(&mut found, &[0.5, 0.7, 1.0, 0.5], 2);
        found.sort_unstable();
        assert_eq!(found, [0, 1]);
    }
}
