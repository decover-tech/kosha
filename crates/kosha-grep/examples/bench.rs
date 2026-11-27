//! #588 benchmark: indexed grep against ripgrep on a directory.
//!
//! ```text
//! cargo run --release -p kosha-grep --example bench -- DIR [--iters N] [--cold-iters N] [--drop-caches] [--block-kib N]
//! ```
//!
//! `--block-kib N` indexes each file as blocks of whole lines of about N KiB,
//! so a candidate costs one ranged read of its block instead of the file.
//!
//! Prints a Markdown report: index build and size (non-positional, plus the
//! estimated extra for a positional index), then per pattern the candidate
//! rate, bytes read, agreement with `rg`, and p50/p99 latency warm (index
//! and documents in memory) and cold (index loaded from its file, documents
//! read from disk; with `--drop-caches`, after `sudo` drops the page cache).
//! `rg` runs over the same directory with the same flags as the oracle.

use std::fs;
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use kosha_grep::corpus::{ripgrep, ripgrep_command, Corpus};
use kosha_grep::{positional_bytes, Grep, GrepOptions, TrigramIndex};

const SELECTIVE: &[&str] = &[
    "unsafe impl Send for",
    r"#\[derive\(Debug, Clone, Copy, PartialEq, Eq, Hash\)\]",
    r"serde_json::from_str",
    r"AtomicUsize::new\(0\)",
    r"\bunreachable_unchecked\b",
    r"MAX_[A-Z_]+_SIZE",
    r"CVE-\d{4}-\d+",
    r"(?i)copyright \(c\) 20\d\d",
    r"fn (main|run)\(\)",
    "TODO|FIXME|XXX",
    "colou?r",
    "this string appears nowhere zqx",
];

const UNSELECTIVE: &[&str] = &[r"\d{4}-\d{2}-\d{2}", r"\w+\s*=\s*\d{6,}", r"[A-Z]{2}\d{3}"];

/// Budget for the fallback-scan demonstration.
const SCAN_BUDGET: u64 = 64 << 20;

struct Args {
    dir: PathBuf,
    iters: usize,
    cold_iters: usize,
    drop_caches: bool,
    block_kib: Option<usize>,
}

fn args() -> Args {
    let mut it = std::env::args().skip(1);
    let mut a = Args {
        dir: PathBuf::new(),
        iters: 10,
        cold_iters: 3,
        drop_caches: false,
        block_kib: None,
    };
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--iters" => a.iters = it.next().and_then(|v| v.parse().ok()).expect("--iters N"),
            "--cold-iters" => {
                a.cold_iters = it
                    .next()
                    .and_then(|v| v.parse().ok())
                    .expect("--cold-iters N")
            }
            "--drop-caches" => a.drop_caches = true,
            "--block-kib" => {
                a.block_kib = Some(
                    it.next()
                        .and_then(|v| v.parse().ok())
                        .expect("--block-kib N"),
                )
            }
            _ => a.dir = PathBuf::from(arg),
        }
    }
    assert!(
        !a.dir.as_os_str().is_empty(),
        "usage: bench DIR [--iters N] [--cold-iters N] [--drop-caches] [--block-kib N]"
    );
    a
}

fn drop_caches() {
    let ok = Command::new("sudo")
        .args([
            "-n",
            "sh",
            "-c",
            "sync && echo 3 > /proc/sys/vm/drop_caches",
        ])
        .status()
        .is_ok_and(|s| s.success());
    assert!(ok, "--drop-caches needs passwordless sudo");
}

fn pct(samples: &mut [Duration], p: f64) -> Duration {
    samples.sort_unstable();
    let i = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples[i]
}

fn ms(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1e3)
}

fn mib(b: u64) -> String {
    format!("{:.1}", b as f64 / f64::from(1 << 20))
}

fn rg_path() -> PathBuf {
    let path = std::env::var_os("PATH").expect("PATH");
    std::env::split_paths(&path)
        .map(|d| d.join("rg"))
        .find(|p| p.is_file())
        .expect("rg on PATH")
}

fn main() -> io::Result<()> {
    let a = args();
    let rg = rg_path();
    let mut texts: Vec<Vec<u8>> = Vec::new();
    let t = Instant::now();
    let corpus = Corpus::load(&a.dir, a.block_kib.map(|k| k << 10), |b| texts.push(b))?;
    let build = t.elapsed();
    let stats = corpus.index.stats();
    let positional: u64 = texts.iter().map(|b| positional_bytes(b)).sum();
    let index_file =
        std::env::temp_dir().join(format!("kosha-grep-bench-{}.idx", std::process::id()));
    corpus
        .index
        .write_to(BufWriter::new(fs::File::create(&index_file)?))?;
    let rg_version = String::from_utf8_lossy(&Command::new(&rg).arg("--version").output()?.stdout)
        .lines()
        .next()
        .unwrap_or_default()
        .to_string();

    println!("## Corpus and index\n");
    println!("| | |\n|---|---|");
    println!("| Corpus | `{}` |", a.dir.display());
    println!("| Files | {} |", corpus.paths.len());
    println!(
        "| Indexed documents | {} ({}) |",
        stats.docs,
        a.block_kib
            .map_or("whole files".to_string(), |k| format!("blocks of ~{k} KiB"))
    );
    println!("| Raw text | {} MiB |", mib(stats.raw_bytes));
    println!("| Distinct trigrams | {} |", stats.distinct_trigrams);
    println!("| Postings (trigram, doc) | {} |", stats.postings);
    println!(
        "| Index build (read + index, 1 thread) | {} s |",
        format_args!("{:.1}", build.as_secs_f64())
    );
    println!(
        "| Non-positional index | {} MiB ({:.1}% of raw) |",
        mib(stats.serialized_bytes),
        stats.overhead() * 100.0
    );
    println!(
        "| Positional index, estimated | {} MiB ({:.1}% of raw) |",
        mib(stats.serialized_bytes + positional),
        (stats.serialized_bytes + positional) as f64 / stats.raw_bytes as f64 * 100.0
    );
    println!("| Oracle | {rg_version} |");
    println!(
        "| Runs | {} warm, {} cold{} |\n",
        a.iters,
        a.cold_iters,
        if a.drop_caches {
            " (page cache dropped)"
        } else {
            " (page cache not dropped)"
        }
    );

    println!("## Patterns\n");
    println!("| Pattern | Trigrams | Candidates | Bytes read (MiB) | Fewer bytes | Matches | = rg | Warm p50 / p99 ms | Cold p50 / p99 ms | rg warm p50 ms | rg cold p50 ms |");
    println!("|---|---|---|---|---|---|---|---|---|---|---|");
    let opts = GrepOptions::default();
    for pattern in SELECTIVE.iter().chain(UNSELECTIVE) {
        bench(
            &a,
            &rg,
            &corpus,
            &texts,
            &index_file,
            pattern,
            &opts,
            stats.raw_bytes,
        )?;
    }
    let budget = GrepOptions {
        max_bytes: Some(SCAN_BUDGET),
        ..Default::default()
    };
    println!("\n## Fallback budget\n");
    println!(
        "`max_bytes` = {} MiB on patterns with no required trigram:\n",
        mib(SCAN_BUDGET)
    );
    println!("| Pattern | Candidates | Bytes read (MiB) | Truncated | Matches | Matches ⊆ rg |");
    println!("|---|---|---|---|---|---|");
    for pattern in [r"\w+", r"^\s*$", "."] {
        let grep = Grep::new(&corpus.index);
        let o = grep
            .run(pattern, &budget, |d| {
                Ok::<_, io::Error>(texts[d as usize].as_slice())
            })
            .expect("pattern");
        let ours = corpus.hits(&o.matches);
        let subset = ours.is_subset(&ripgrep(&rg, &a.dir, pattern, false)?);
        println!(
            "| `{}` | {} | {} | {:?} | {} | {} |",
            pattern.replace('|', r"\|"),
            o.candidates,
            mib(o.bytes_read),
            o.truncated,
            o.matches.len(),
            if subset { "yes" } else { "**no**" }
        );
    }
    fs::remove_file(&index_file)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn bench(
    a: &Args,
    rg: &Path,
    corpus: &Corpus,
    texts: &[Vec<u8>],
    index_file: &Path,
    pattern: &str,
    opts: &GrepOptions,
    raw: u64,
) -> io::Result<()> {
    let grep = Grep::new(&corpus.index);
    let run_warm = || {
        grep.run(pattern, opts, |d| {
            Ok::<_, io::Error>(texts[d as usize].as_slice())
        })
        .expect("pattern")
    };
    let outcome = run_warm();
    let agree = corpus.hits(&outcome.matches) == ripgrep(rg, &corpus.root, pattern, false)?;
    let mut warm: Vec<Duration> = (0..a.iters)
        .map(|_| {
            let t = Instant::now();
            run_warm();
            t.elapsed()
        })
        .collect();
    let mut cold = Vec::new();
    for _ in 0..a.cold_iters {
        if a.drop_caches {
            drop_caches();
        }
        let t = Instant::now();
        let index = TrigramIndex::read_from(io::BufReader::new(fs::File::open(index_file)?))?;
        Grep::new(&index)
            .run(pattern, opts, |d| corpus.read(d))
            .expect("pattern");
        cold.push(t.elapsed());
    }
    let time_rg = |drop: bool| -> io::Result<Duration> {
        if drop {
            drop_caches();
        }
        let t = Instant::now();
        ripgrep_command(rg, &corpus.root, pattern, false).output()?;
        Ok(t.elapsed())
    };
    let mut rg_warm = (0..a.iters)
        .map(|_| time_rg(false))
        .collect::<io::Result<Vec<_>>>()?;
    let mut rg_cold = (0..a.cold_iters)
        .map(|_| time_rg(a.drop_caches))
        .collect::<io::Result<Vec<_>>>()?;
    let reduction = if outcome.bytes_read == 0 {
        "all".to_string()
    } else {
        format!("{:.0}×", raw as f64 / outcome.bytes_read as f64)
    };
    println!(
        "| `{}` | {} | {} ({:.2}%) | {} | {} | {} | {} | {} / {} | {} / {} | {} | {} |",
        pattern.replace('|', r"\|"),
        outcome.query.trigram_count(),
        outcome.candidates,
        outcome.candidates as f64 / corpus.index.len() as f64 * 100.0,
        mib(outcome.bytes_read),
        reduction,
        outcome.matches.len(),
        if agree { "yes" } else { "**no**" },
        ms(pct(&mut warm, 0.5)),
        ms(pct(&mut warm, 0.99)),
        ms(pct(&mut cold, 0.5)),
        ms(pct(&mut cold, 0.99)),
        ms(pct(&mut rg_warm, 0.5)),
        ms(pct(&mut rg_cold, 0.5)),
    );
    Ok(())
}
