//! Measures posting-ordered doc_seqs (#456, research) on segment sets built
//! by `kosha-build-segments`, e.g. one with `--doc-reorder off` and one
//! with `--doc-reorder posting` from the same corpus.
//!
//! ```text
//! kosha-build-segments ... --namespace off --doc-reorder off
//! kosha-build-segments ... --namespace posting --doc-reorder posting
//! kosha-posting-order-eval --set /tmp/segs/off --set /tmp/segs/posting
//! ```
//!
//! Per set: postings, entries, how many postings are one contiguous
//! doc_seq range, and doc_seq runs per posting (the ranged
//! `doc_vectors.bin` reads it takes to rescore all of a posting's
//! members), plus the bytes the per-entry `doc_seq:u32` takes in
//! `vector.idx`, which a `(base, length)` range per posting would replace.

use std::path::{Path, PathBuf};

use clap::Parser;
use kosha_segment::reorder::doc_seq_runs;
use kosha_segment::SegmentReader;

#[derive(Parser)]
#[command(name = "kosha-posting-order-eval")]
struct Args {
    /// Namespace directory of one build (`<out-dir>/<namespace>`);
    /// repeat to compare builds
    #[arg(long = "set", required = true)]
    sets: Vec<PathBuf>,
    #[arg(long, default_value = "text_emb")]
    field: String,
}

#[derive(Default)]
struct SetStats {
    segments: usize,
    docs: u64,
    postings: u64,
    entries: u64,
    contiguous: u64,
    runs: u64,
    idx_bytes: u64,
}

fn segment_dirs(set: &Path) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(set)
        .unwrap_or_else(|e| panic!("read {}: {e}", set.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.join("footer.json").exists())
        .collect();
    dirs.sort();
    dirs
}

fn measure(set: &Path, field: &str) -> SetStats {
    let mut stats = SetStats::default();
    for dir in segment_dirs(set) {
        let reader = SegmentReader::open(dir.clone())
            .unwrap_or_else(|e| panic!("open {}: {e}", dir.display()));
        let Some(layout) = reader
            .vector_posting_layout(field)
            .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        else {
            eprintln!("skip {}: no clustered index for {field}", dir.display());
            continue;
        };
        stats.segments += 1;
        stats.docs += reader.doc_count() as u64;
        stats.idx_bytes += reader
            .footer()
            .vector_fields
            .iter()
            .flatten()
            .find(|meta| meta.name == field)
            .and_then(|meta| std::fs::metadata(dir.join(meta.idx_file())).ok())
            .map_or(0, |m| m.len());
        for posting in &layout.postings {
            let runs = doc_seq_runs(&posting.doc_seqs) as u64;
            stats.postings += 1;
            stats.entries += posting.doc_seqs.len() as u64;
            stats.runs += runs;
            stats.contiguous += u64::from(runs == 1);
        }
    }
    stats
}

fn pct(n: u64, d: u64) -> f64 {
    100.0 * n as f64 / d.max(1) as f64
}

fn main() {
    let args = Args::parse();
    for set in &args.sets {
        let s = measure(set, &args.field);
        println!(
            "== {} ({} segments, {} docs)",
            set.display(),
            s.segments,
            s.docs
        );
        println!(
            "postings {}  entries {}  contiguous {} ({:.1}%)  runs/posting {:.2}",
            s.postings,
            s.entries,
            s.contiguous,
            pct(s.contiguous, s.postings),
            s.runs as f64 / s.postings.max(1) as f64,
        );
        println!(
            "per-entry doc_seq bytes {} ({:.1}% of vector.idx {})",
            4 * s.entries,
            pct(4 * s.entries, s.idx_bytes),
            s.idx_bytes,
        );
    }
}
