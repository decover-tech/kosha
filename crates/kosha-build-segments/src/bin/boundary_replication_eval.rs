//! Offline recall/scan/space experiment for #457. See
//! `docs/research/457-boundary-replication.md` for the corpus and plot commands.

#[path = "../../../kosha-vector-spfresh/tests/common/mod.rs"]
mod fixtures;

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, Read};
use std::path::PathBuf;

use clap::Parser;
use kosha_build_segments::head::{Head, HeadParams};
use kosha_segment::centroid_probe_score;
use kosha_vector_spfresh::replication::{boundary_replicas, Assignment, Replica};
use kosha_vector_spfresh::{
    dot, encode_rabitq, normalize_in_place, rabitq_code_bytes, ClusterIndex, ClusterIndexConfig,
    PostingSnapshot, RaBitQCode, RaBitQEstimator, RaBitQQuery, RaBitQRotation,
};
use rayon::prelude::*;

const K: usize = 10;

#[derive(Parser)]
#[command(name = "kosha-boundary-replication-eval")]
struct Cli {
    /// Embedding shards from fetch_msmarco_embeddings.py. Omit for the recall-test fixtures.
    #[arg(long)]
    embeddings_dir: Option<PathBuf>,
    #[arg(long, default_value_t = 1024, value_parser = positive)]
    dim: usize,
    #[arg(long, default_value_t = 1_000_000, value_parser = positive)]
    docs: usize,
    #[arg(long, default_value_t = 1000, value_parser = positive)]
    queries: usize,
    /// Input-order segment size (also sizes the namespace head).
    #[arg(long, value_parser = positive)]
    docs_per_segment: Option<usize>,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "0.005,0.01,0.02,0.03,0.05,0.075,0.1,0.15,0.2"
    )]
    probes: Vec<f64>,
    /// Cap extra entries as a fraction of the original document count.
    #[arg(long, value_delimiter = ',', default_value = "0.2,3")]
    spill_fractions: Vec<f64>,
    #[arg(long, default_value_t = 0.2)]
    epsilon: f32,
    #[arg(long, default_value_t = 1.0)]
    lambda: f32,
    #[arg(long, default_value_t = 1.0)]
    radius_alpha: f32,
    /// Exact shortlist size for RaBitQ, after deduplication.
    #[arg(long, default_value_t = 40, value_parser = positive)]
    rescore: usize,
}

fn positive(raw: &str) -> Result<usize, String> {
    let n: usize = raw
        .parse()
        .map_err(|_| format!("invalid integer {raw:?}"))?;
    (n > 0)
        .then_some(n)
        .ok_or_else(|| "must be positive".into())
}

struct Corpus {
    vectors: Vec<Vec<f32>>,
    queries: Vec<Vec<f32>>,
    dim: usize,
}

fn read_vectors(path: PathBuf, dim: usize, count: usize) -> io::Result<Vec<Vec<f32>>> {
    let file = File::open(&path)?;
    let width = (dim * 4) as u64;
    let len = file.metadata()?.len();
    if !len.is_multiple_of(width) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} has an incomplete vector", path.display()),
        ));
    }
    let available = (len / width) as usize;
    let mut input = BufReader::new(file);
    let mut bytes = vec![0; width as usize];
    let mut vectors = Vec::with_capacity(count.min(available));
    for _ in 0..count.min(available) {
        input.read_exact(&mut bytes)?;
        let mut vector: Vec<f32> = bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| f32::from_le_bytes(*b))
            .collect();
        if !vector.iter().all(|x| x.is_finite()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} has a non-finite vector", path.display()),
            ));
        }
        normalize_in_place(&mut vector);
        vectors.push(vector);
    }
    Ok(vectors)
}

fn corpus(cli: &Cli) -> io::Result<Corpus> {
    if let Some(dir) = &cli.embeddings_dir {
        let mut shards = Vec::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("emb-") && n.ends_with(".f32"))
            {
                shards.push(path);
            }
        }
        shards.sort();
        let mut vectors = Vec::new();
        for path in shards {
            vectors.extend(read_vectors(path, cli.dim, cli.docs - vectors.len())?);
            if vectors.len() == cli.docs {
                break;
            }
        }
        if vectors.len() != cli.docs {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "requested {} documents, only {} available",
                    cli.docs,
                    vectors.len()
                ),
            ));
        }
        let all_queries = read_vectors(dir.join("queries_emb.f32"), cli.dim, usize::MAX)?;
        if all_queries.len() < cli.queries {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "requested {} queries, only {} available",
                    cli.queries,
                    all_queries.len()
                ),
            ));
        }
        let queries = (0..cli.queries)
            .map(|i| all_queries[i * all_queries.len() / cli.queries].clone())
            .collect();
        Ok(Corpus {
            vectors,
            queries,
            dim: cli.dim,
        })
    } else {
        let dim = 32;
        let mut vectors: Vec<_> = fixtures::gen_clustered_dataset(dim, 20, 500, 1)
            .into_iter()
            .map(|(_, v)| v)
            .collect();
        let mut queries = fixtures::gen_queries(dim, 20, 5, 2);
        for v in vectors.iter_mut().chain(&mut queries) {
            normalize_in_place(v);
        }
        Ok(Corpus {
            vectors,
            queries,
            dim,
        })
    }
}

fn segments(corpus: &Corpus, size: usize, namespace: bool) -> Vec<Vec<u32>> {
    let count = corpus.vectors.len();
    let n = count.min(131_072);
    let leaves = count.div_ceil(size).min((n / 2).max(1));
    if !namespace || leaves == 1 {
        return (0..count as u32)
            .collect::<Vec<_>>()
            .chunks(size)
            .map(<[u32]>::to_vec)
            .collect();
    }
    let clusters = (leaves * 16).min(n / 2);
    let sample: Vec<_> = (0..n)
        .map(|i| corpus.vectors[i * count / n].clone())
        .collect();
    let head = Head::build(
        &sample,
        corpus.dim,
        HeadParams {
            leaves,
            clusters,
            iters: 10,
            seed: 380,
        },
    );
    let buckets: Vec<_> = corpus.vectors.par_iter().map(|v| head.route(v)).collect();
    let mut segments = vec![Vec::new(); leaves];
    for (id, bucket) in buckets.into_iter().enumerate() {
        segments[bucket].push(id as u32);
    }
    segments.retain(|s| !s.is_empty());
    segments
}

fn build(corpus: &Corpus, groups: &[Vec<u32>]) -> Vec<Vec<PostingSnapshot>> {
    groups
        .par_iter()
        .map(|ids| {
            let vectors: Vec<_> = ids
                .iter()
                .map(|&id| (id, corpus.vectors[id as usize].clone()))
                .collect();
            let index = ClusterIndex::build(&vectors, ClusterIndexConfig::new(corpus.dim)).unwrap();
            index.snapshot()
        })
        .collect()
}

struct Posting {
    centroid: Vec<f32>,
    radius: f32,
    ids: Vec<u32>,
    codes: Vec<RaBitQCode>,
    bits: Vec<u8>,
}

fn plan(primary: &[Vec<PostingSnapshot>], assignment: Assignment) -> Vec<Replica> {
    let plans: Vec<_> = primary
        .par_iter()
        .map(|segment| boundary_replicas(segment, assignment))
        .collect();
    let mut offset = 0;
    let mut replicas = Vec::new();
    for (segment, plan) in primary.iter().zip(plans) {
        replicas.extend(plan.into_iter().map(|mut r| {
            r.posting += offset;
            r
        }));
        offset += segment.len();
    }
    replicas
}

fn postings(
    corpus: &Corpus,
    primary: &[Vec<PostingSnapshot>],
    replicas: &[Replica],
    rotation: &RaBitQRotation,
) -> Vec<Posting> {
    let mut copies: Vec<_> = primary
        .iter()
        .flatten()
        .map(|p| {
            let mut centroid = p.centroid.clone();
            normalize_in_place(&mut centroid);
            Posting {
                centroid,
                radius: p.radius,
                ids: p.entries.iter().map(|(id, _)| *id).collect(),
                codes: Vec::new(),
                bits: Vec::new(),
            }
        })
        .collect();
    for replica in replicas {
        let posting = &mut copies[replica.posting];
        let distance = 1.0
            - dot(&posting.centroid, &corpus.vectors[replica.doc_seq as usize]).clamp(-1.0, 1.0);
        posting.radius = posting.radius.max(distance);
        posting.ids.push(replica.doc_seq);
    }
    copies.par_iter_mut().for_each(|p| {
        let width = rabitq_code_bytes(corpus.dim);
        p.bits.reserve_exact(width * p.ids.len());
        p.codes.reserve_exact(p.ids.len());
        let mut bits = Vec::new();
        for &id in &p.ids {
            p.codes.push(encode_rabitq(
                rotation,
                &corpus.vectors[id as usize],
                &p.centroid,
                &mut bits,
            ));
            p.bits.extend_from_slice(&bits);
        }
    });
    copies
}

fn top(mut scored: Vec<(u32, f32)>, n: usize) -> Vec<(u32, f32)> {
    let order = |a: &(u32, f32), b: &(u32, f32)| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0));
    if scored.len() > n {
        scored.select_nth_unstable_by(n, order);
        scored.truncate(n);
    }
    scored.sort_unstable_by(order);
    scored
}

#[derive(Clone, Copy)]
struct Point {
    scans: usize,
    unique: usize,
    hits_f32: usize,
    hits_rabitq: usize,
    rescored: usize,
}

fn curve(
    corpus: &Corpus,
    postings: &[Posting],
    query: &[f32],
    truth: &[(u32, f32)],
    budgets: &[usize],
    cli: &Cli,
    rotation: &RaBitQRotation,
) -> Vec<Point> {
    let prepared = RaBitQQuery::with_estimator(rotation, query, RaBitQEstimator::Popcount4);
    let mut rank: Vec<_> = postings
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let dp = dot(&p.centroid, query);
            (
                i,
                dp,
                centroid_probe_score(1.0 - dp.clamp(-1.0, 1.0), p.radius, cli.radius_alpha),
            )
        })
        .collect();
    let order =
        |a: &(usize, f32, f32), b: &(usize, f32, f32)| a.2.total_cmp(&b.2).then(a.0.cmp(&b.0));
    let last = budgets[budgets.len() - 1];
    if last < rank.len() {
        rank.select_nth_unstable_by(last, order);
        rank.truncate(last);
    }
    rank.sort_unstable_by(order);
    let width = rabitq_code_bytes(corpus.dim);
    let mut exact = HashMap::new();
    let mut estimated = HashMap::new();
    let mut previous = 0;
    let mut scans = 0;
    budgets
        .iter()
        .map(|&budget| {
            for &(i, dp, _) in &rank[previous..budget] {
                let p = &postings[i];
                scans += p.ids.len();
                for (j, &id) in p.ids.iter().enumerate() {
                    exact
                        .entry(id)
                        .or_insert_with(|| dot(query, &corpus.vectors[id as usize]));
                    let (score, _) =
                        prepared.estimate(&p.codes[j], &p.bits[j * width..(j + 1) * width], dp);
                    estimated
                        .entry(id)
                        .and_modify(|best: &mut f32| *best = best.max(score))
                        .or_insert(score);
                }
            }
            previous = budget;
            let f32_top = top(exact.iter().map(|(&id, &s)| (id, s)).collect(), K);
            let shortlist = top(
                estimated.iter().map(|(&id, &s)| (id, s)).collect(),
                cli.rescore,
            );
            let rescored = shortlist.len();
            let rabitq_top = top(
                shortlist
                    .into_iter()
                    .map(|(id, _)| (id, exact[&id]))
                    .collect(),
                K,
            );
            let hits = |results: &[(u32, f32)]| {
                results
                    .iter()
                    .filter(|(id, _)| truth.iter().any(|(t, _)| t == id))
                    .count()
            };
            Point {
                scans,
                unique: exact.len(),
                hits_f32: hits(&f32_top),
                hits_rabitq: hits(&rabitq_top),
                rescored,
            }
        })
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if !cli.probes.iter().all(|p| *p > 0.0 && *p <= 1.0)
        || !cli.spill_fractions.iter().all(|p| *p > 0.0 && *p <= 3.0)
        || ![cli.epsilon, cli.lambda, cli.radius_alpha]
            .iter()
            .all(|n| n.is_finite() && *n >= 0.0)
        || cli.rescore < K
        || cli.docs_per_segment == Some(1)
        || cli.docs < K
        || cli.docs > u32::MAX as usize
    {
        return Err("probe fractions must be in (0, 1], spill fractions in (0, 3], epsilon/lambda/radius alpha finite and nonnegative, docs/rescore at least 10 (docs must fit u32), and segment size at least 2".into());
    }
    let corpus = corpus(&cli)?;
    let dataset = if cli.embeddings_dir.is_some() {
        "msmarco"
    } else {
        "synthetic"
    };
    let segment_size =
        cli.docs_per_segment
            .unwrap_or(if dataset == "msmarco" { 50_000 } else { 2000 });
    let truth: Vec<_> = corpus
        .queries
        .par_iter()
        .map(|q| {
            top(
                corpus
                    .vectors
                    .iter()
                    .enumerate()
                    .map(|(i, v)| (i as u32, dot(q, v)))
                    .collect(),
                K,
            )
        })
        .collect();
    let rotation = RaBitQRotation::new(corpus.dim);
    let count = corpus.vectors.len();
    let queries = corpus.queries.len();
    let methods = [
        ("baseline", 1, None),
        (
            "spann",
            2,
            Some(Assignment::Spann {
                epsilon: cli.epsilon,
                max_copies: 2,
            }),
        ),
        (
            "spann",
            4,
            Some(Assignment::Spann {
                epsilon: cli.epsilon,
                max_copies: 4,
            }),
        ),
        ("soar", 2, Some(Assignment::Soar { lambda: cli.lambda })),
    ];
    println!("dataset,layout,assignment,max_copies,spill_fraction,encoding,docs,queries,postings_probed,recall_at_10,p10_recall,mean_scanned,mean_unique,mean_rescored,index_bytes,extra_entries");
    for (layout, namespace) in [("input", false), ("namespace", true)] {
        let groups = segments(&corpus, segment_size, namespace);
        eprintln!(
            "{dataset}/{layout}: building {count} vectors in {} segments",
            groups.len()
        );
        let primary = build(&corpus, &groups);
        let total: usize = primary.iter().map(Vec::len).sum();
        let mut budgets: Vec<_> = cli
            .probes
            .iter()
            .map(|f| ((*f * total as f64).ceil() as usize).clamp(1, total))
            .collect();
        budgets.sort_unstable();
        budgets.dedup();
        let overhead = 32 * groups.len() + total * (4 * corpus.dim + 20);
        for (method, max_copies, assignment) in methods {
            let mut replicas = assignment.map_or_else(Vec::new, |a| plan(&primary, a));
            replicas.sort_by(|a, b| a.priority.total_cmp(&b.priority));
            let fractions = if assignment.is_none() {
                &[0.0][..]
            } else {
                &cli.spill_fractions
            };
            let mut cached = None;
            for &fraction in fractions {
                let added = replicas
                    .len()
                    .min((fraction * count as f64).floor() as usize);
                eprintln!("{dataset}/{layout}/{method}: max copies {max_copies}, spill cap {fraction}, {added} extra entries");
                if cached.as_ref().is_none_or(|(size, _)| *size != added) {
                    let postings = postings(&corpus, &primary, &replicas[..added], &rotation);
                    let results: Vec<_> = corpus
                        .queries
                        .par_iter()
                        .zip(&truth)
                        .map(|(q, truth)| {
                            curve(&corpus, &postings, q, truth, &budgets, &cli, &rotation)
                        })
                        .collect();
                    cached = Some((added, results));
                }
                let results = &cached.as_ref().unwrap().1;
                let f32_bytes = overhead + 4 * (count + added) + 4 * corpus.dim * count;
                let rabitq_bytes =
                    overhead + (count + added) * (12 + rabitq_code_bytes(corpus.dim));
                for (i, budget) in budgets.iter().enumerate() {
                    let avg = |get: fn(&Point) -> usize| {
                        results.iter().map(|r| get(&r[i])).sum::<usize>() as f64 / queries as f64
                    };
                    let scans = avg(|p| p.scans);
                    let unique = avg(|p| p.unique);
                    for (encoding, get_hits, rescored, bytes) in [
                        (
                            "f32",
                            (|p: &Point| p.hits_f32) as fn(&Point) -> usize,
                            0.0,
                            f32_bytes,
                        ),
                        (
                            "rabitq",
                            |p: &Point| p.hits_rabitq,
                            avg(|p| p.rescored),
                            rabitq_bytes,
                        ),
                    ] {
                        let recall = avg(get_hits) / K as f64;
                        let mut hits: Vec<_> = results.iter().map(|r| get_hits(&r[i])).collect();
                        let p10 = *hits.select_nth_unstable((queries - 1) / 10).1 as f64 / K as f64;
                        println!("{dataset},{layout},{method},{max_copies},{fraction},{encoding},{count},{queries},{budget},{recall:.6},{p10:.2},{scans:.2},{unique:.2},{rescored:.2},{bytes},{added}");
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_caps_leaves_at_half_the_sample() {
        let corpus = Corpus {
            dim: 2,
            vectors: fixtures::gen_clustered_dataset(2, 1, 101, 1)
                .into_iter()
                .map(|(_, mut v)| {
                    normalize_in_place(&mut v);
                    v
                })
                .collect(),
            queries: Vec::new(),
        };
        assert_eq!(segments(&corpus, 2, false).len(), 51);
        let groups = segments(&corpus, 2, true);
        assert!((2..=50).contains(&groups.len()));
        let mut ids: Vec<_> = groups.into_iter().flatten().collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..101).collect::<Vec<_>>());
    }

    #[test]
    fn duplicated_entries_count_as_scan_work_not_extra_results() {
        let cli = Cli::parse_from(["eval"]);
        let corpus = Corpus {
            dim: 2,
            vectors: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            queries: vec![vec![1.0, 0.0]],
        };
        let primary = vec![vec![
            PostingSnapshot {
                centroid: vec![1.0, 0.0],
                entries: vec![(0, corpus.vectors[0].clone())],
                radius: 0.0,
            },
            PostingSnapshot {
                centroid: vec![0.0, 1.0],
                entries: vec![(1, corpus.vectors[1].clone())],
                radius: 0.0,
            },
        ]];
        let rotation = RaBitQRotation::new(2);
        let replicas = plan(&primary, Assignment::Soar { lambda: 1.0 });
        let postings = postings(&corpus, &primary, &replicas, &rotation);
        assert_eq!(replicas.len(), 2);
        let results = curve(
            &corpus,
            &postings,
            &corpus.queries[0],
            &[(0, 1.0), (1, 0.0)],
            &[1, 2],
            &cli,
            &rotation,
        );
        assert_eq!(results[0].scans, 2);
        assert_eq!(results[1].scans, 4);
        assert_eq!(results[1].unique, 2);
        assert_eq!(results[1].rescored, 2);
        assert_eq!(results[1].hits_f32, 2);
        assert_eq!(results[1].hits_rabitq, 2);
    }
}
