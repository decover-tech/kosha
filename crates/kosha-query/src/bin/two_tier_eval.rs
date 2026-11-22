//! `kosha-two-tier-eval` — offline two-tier impact-index analysis (#387).
//!
//! Reads a namespace's segment directories and a query file, and reports
//! for each head fraction what share of queries a per-term high-impact
//! head certifies the exact top-k for, with an exhaustive-oracle check,
//! plus local-footprint and tail-fetch estimates. See
//! `kosha_query::two_tier` for the method and
//! `docs/research/387-two-tier-index.md` for the design it informs.
//!
//! ```text
//! kosha-two-tier-eval --segments-root /data/two-tier/msmarco-10m \
//!     [--segment-list ids.txt] --queries /data/queries.txt \
//!     [--max-queries 2000] [--k 10] [--head-fractions 0.1,0.2,0.3] \
//!     [--min-split-df 128] [--stats namespace|segment] [--footprint] \
//!     [--threads N] [--out report.json] [--per-query rows.jsonl]
//! ```

use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use kosha_query::two_tier::{self, Config, StatsScope};

fn usage() -> ! {
    eprintln!(
        "usage: kosha-two-tier-eval --segments-root DIR [--segment-list FILE] --queries FILE \
         [--max-queries N] [--k 10] [--head-fractions 0.1,0.2,0.3] [--min-split-df 128] \
         [--stats namespace|segment] [--footprint] [--threads N] [--out FILE] [--per-query FILE]"
    );
    std::process::exit(2)
}

fn main() {
    let mut root: Option<PathBuf> = None;
    let mut list: Option<PathBuf> = None;
    let mut queries: Option<PathBuf> = None;
    let mut max_queries = usize::MAX;
    let mut config = Config::default();
    let mut do_footprint = false;
    let mut threads: Option<usize> = None;
    let mut out: Option<PathBuf> = None;
    let mut per_query: Option<PathBuf> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().unwrap_or_else(|| usage());
        match arg.as_str() {
            "--segments-root" => root = Some(value().into()),
            "--segment-list" => list = Some(value().into()),
            "--queries" => queries = Some(value().into()),
            "--max-queries" => max_queries = value().parse().unwrap_or_else(|_| usage()),
            "--k" => config.k = value().parse().unwrap_or_else(|_| usage()),
            "--head-fractions" => {
                config.head_fractions = value()
                    .split(',')
                    .map(|f| f.trim().parse::<f64>().unwrap_or_else(|_| usage()))
                    .collect();
                if config.head_fractions.is_empty()
                    || config.head_fractions.len() > two_tier::MAX_FRACTIONS
                    || config
                        .head_fractions
                        .iter()
                        .any(|f| !(*f > 0.0 && *f <= 1.0))
                {
                    eprintln!("--head-fractions: 1..=8 values in (0, 1]");
                    usage();
                }
            }
            "--min-split-df" => config.min_split_df = value().parse().unwrap_or_else(|_| usage()),
            "--stats" => {
                config.stats = match value().as_str() {
                    "namespace" => StatsScope::Namespace,
                    "segment" => StatsScope::Segment,
                    _ => usage(),
                }
            }
            "--footprint" => do_footprint = true,
            "--threads" => threads = Some(value().parse().unwrap_or_else(|_| usage())),
            "--out" => out = Some(value().into()),
            "--per-query" => per_query = Some(value().into()),
            "-h" | "--help" => usage(),
            _ => {
                eprintln!("unknown argument {arg}");
                usage()
            }
        }
    }
    let (Some(root), Some(queries_path)) = (root, queries) else {
        usage()
    };
    if let Some(n) = threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .build_global()
            .expect("rayon pool");
    }

    let ids: Option<Vec<String>> = list.map(|p| {
        std::fs::read_to_string(&p)
            .unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect()
    });
    let dirs = two_tier::segment_dirs(&root, ids.as_deref())
        .unwrap_or_else(|e| panic!("list {}: {e}", root.display()));
    if dirs.is_empty() {
        eprintln!("no segments under {}", root.display());
        std::process::exit(1);
    }
    let t = Instant::now();
    let segments = two_tier::open_segments(&dirs).unwrap_or_else(|e| panic!("open segments: {e}"));
    let docs: u64 = segments.iter().map(|s| s.doc_count() as u64).sum();
    eprintln!(
        "opened {} segments ({docs} docs) in {:.1}s",
        segments.len(),
        t.elapsed().as_secs_f64()
    );

    // Plain text, one query per line; a `qid<TAB>text` line keeps the text.
    let queries: Vec<String> = std::fs::read_to_string(&queries_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", queries_path.display()))
        .lines()
        .map(|l| l.split_once('\t').map_or(l, |(_, text)| text).trim())
        .filter(|l| !l.is_empty())
        .take(max_queries)
        .map(str::to_string)
        .collect();

    let t = Instant::now();
    let (report, rows) = two_tier::analyze_queries(&segments, config.clone(), &queries)
        .unwrap_or_else(|e| panic!("analysis failed: {e}"));
    let query_secs = t.elapsed().as_secs_f64();
    eprintln!("analyzed {} queries in {query_secs:.1}s", rows.len());

    let footprint = do_footprint.then(|| {
        let t = Instant::now();
        let fp =
            two_tier::footprint(&segments, &config).unwrap_or_else(|e| panic!("footprint: {e}"));
        eprintln!("footprint pass in {:.1}s", t.elapsed().as_secs_f64());
        fp
    });

    for f in &report.fractions {
        for b in &f.bounds {
            println!(
                "head {:>4.0}% {:<12} set-certified {:>6.2}%  rank-certified {:>6.2}%  \
                 (with ties {:>6.2}%)  head-only exact {:>6.2}%  overlap@k {:.3}  violations {}  \
                 tail GETs/uncertified query mean {:.1} p99 {}",
                f.head_fraction * 100.0,
                b.bound,
                b.set_certified * 100.0,
                b.rank_certified * 100.0,
                b.set_certified_with_ties * 100.0,
                b.head_answer_exact * 100.0,
                b.mean_overlap_at_k,
                b.violations,
                b.tail_gets_mean,
                b.tail_gets_p99,
            );
        }
        if f.touched_full_scoring_bytes > 0 {
            println!(
                "           touched by the query set: scoring bytes head/full {:.3}; span bytes head-est/full {:.3}",
                f.touched_head_scoring_bytes as f64 / f.touched_full_scoring_bytes as f64,
                if f.touched_span_bytes > 0 {
                    f.touched_head_span_bytes_est as f64 / f.touched_span_bytes as f64
                } else {
                    f64::NAN
                },
            );
        }
    }
    if let Some(fp) = &footprint {
        for f in &fp.fractions {
            println!(
                "footprint head {:>4.0}%: postings {:.3}  scoring bytes {:.3}  span bytes (est) {:.3}  \
                 tail frontier points {} (max/term {})",
                f.head_fraction * 100.0,
                f.head_postings as f64 / fp.postings.max(1) as f64,
                f.head_scoring_bytes as f64 / fp.full_scoring_bytes.max(1) as f64,
                if fp.span_bytes > 0 {
                    f.head_span_bytes_est as f64 / fp.span_bytes as f64
                } else {
                    f64::NAN
                },
                f.tail_frontier_points,
                f.tail_frontier_points_max,
            );
        }
    }

    let json = serde_json::json!({
        "report": report,
        "footprint": footprint,
        "query_pass_seconds": query_secs,
    });
    let text = serde_json::to_string_pretty(&json).expect("serialize");
    match &out {
        Some(p) => std::fs::write(p, text).unwrap_or_else(|e| panic!("write {}: {e}", p.display())),
        None => println!("{text}"),
    }
    if let Some(p) = per_query {
        let mut f = std::io::BufWriter::new(
            std::fs::File::create(&p).unwrap_or_else(|e| panic!("create {}: {e}", p.display())),
        );
        for row in &rows {
            serde_json::to_writer(&mut f, row).expect("serialize row");
            f.write_all(b"\n").expect("write row");
        }
    }
}
