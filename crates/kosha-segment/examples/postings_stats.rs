//! Postings byte breakdown over a set of segments (#386 diagnostics).
//!
//!     cargo run --release -p kosha-segment --example postings_stats -- <dir>...
//!
//! Each `<dir>` is a segment directory (it holds `inverted.idx`) or a
//! directory searched recursively for them, e.g. a local copy of a bench
//! snapshot's `segments/<namespace>/`. Every segment's shards must be
//! whole local files. Prints, summed over all segments: segment count and
//! TOC versions, postings in full 128-posting blocks, skip / scoring /
//! positions bytes as stored, and the scoring bytes re-encoded as v5
//! varints and as v6 bit-packed blocks (a codec-only comparison on the
//! same postings). The last line is the same data as one JSON object.
//! Read-only: it never writes to the segments.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use kosha_segment::{postings_section_stats, PostingsSectionStats};

fn find_segments(dir: &Path, out: &mut Vec<PathBuf>) {
    if dir.join("inverted.idx").is_file() {
        out.push(dir.to_path_buf());
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut children: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    children.sort();
    for child in children {
        find_segments(&child, out);
    }
}

fn pct(part: u64, whole: u64) -> f64 {
    100.0 * part as f64 / whole.max(1) as f64
}

fn main() {
    let roots: Vec<PathBuf> = std::env::args().skip(1).map(PathBuf::from).collect();
    if roots.is_empty() {
        eprintln!("usage: postings_stats <segment-or-parent-dir>...");
        std::process::exit(2);
    }
    let mut segments = Vec::new();
    for root in &roots {
        find_segments(root, &mut segments);
    }
    if segments.is_empty() {
        eprintln!("no segments (directories with inverted.idx) found");
        std::process::exit(1);
    }
    let mut versions: BTreeMap<u32, u64> = BTreeMap::new();
    let mut total = PostingsSectionStats::default();
    let mut file_bytes = 0u64;
    let mut failed = 0u64;
    for seg in &segments {
        match postings_section_stats(seg) {
            Ok(s) => {
                *versions.entry(s.inverted_version).or_default() += 1;
                total.terms += s.terms;
                total.postings += s.postings;
                total.postings_in_full_blocks += s.postings_in_full_blocks;
                total.skip_bytes += s.skip_bytes;
                total.scoring_bytes += s.scoring_bytes;
                total.positions_bytes += s.positions_bytes;
                total.scoring_bytes_as_v5 += s.scoring_bytes_as_v5;
                total.scoring_bytes_as_v6 += s.scoring_bytes_as_v6;
                total.shard_logical_bytes += s.shard_logical_bytes;
                if let Ok(entries) = std::fs::read_dir(seg) {
                    file_bytes += entries
                        .flatten()
                        .filter(|e| {
                            let name = e.file_name();
                            let name = name.to_string_lossy();
                            name.starts_with("postings-") && name.ends_with(".bin")
                        })
                        .filter_map(|e| e.metadata().ok())
                        .map(|m| m.len())
                        .sum::<u64>();
                }
            }
            Err(e) => {
                failed += 1;
                eprintln!("{}: {e}", seg.display());
            }
        }
    }
    let t = &total;
    let ok = segments.len() as u64 - failed;
    println!("segments {ok} read, {failed} failed; inverted versions {versions:?}");
    println!(
        "terms {}  postings {}  in full blocks {:.1}%",
        t.terms,
        t.postings,
        pct(t.postings_in_full_blocks, t.postings)
    );
    println!(
        "postings-*.bin on disk {file_bytes} B; logical {} B = skip {} ({:.1}%) + scoring {} ({:.1}%) + positions {} ({:.1}%)",
        t.shard_logical_bytes,
        t.skip_bytes,
        pct(t.skip_bytes, t.shard_logical_bytes),
        t.scoring_bytes,
        pct(t.scoring_bytes, t.shard_logical_bytes),
        t.positions_bytes,
        pct(t.positions_bytes, t.shard_logical_bytes)
    );
    println!(
        "scoring re-encoded: v5 {} B ({:.3} B/posting), v6 {} B ({:.3} B/posting), v6/v5 {:+.1}%",
        t.scoring_bytes_as_v5,
        t.scoring_bytes_as_v5 as f64 / t.postings.max(1) as f64,
        t.scoring_bytes_as_v6,
        t.scoring_bytes_as_v6 as f64 / t.postings.max(1) as f64,
        100.0 * (t.scoring_bytes_as_v6 as f64 / t.scoring_bytes_as_v5.max(1) as f64 - 1.0)
    );
    let versions_json: Vec<String> = versions
        .iter()
        .map(|(v, n)| format!("\"{v}\": {n}"))
        .collect();
    println!(
        "{{\"segments\": {ok}, \"failed\": {failed}, \"inverted_versions\": {{{}}}, \"terms\": {}, \"postings\": {}, \"postings_in_full_blocks\": {}, \"postings_file_bytes\": {file_bytes}, \"shard_logical_bytes\": {}, \"skip_bytes\": {}, \"scoring_bytes\": {}, \"positions_bytes\": {}, \"scoring_bytes_as_v5\": {}, \"scoring_bytes_as_v6\": {}}}",
        versions_json.join(", "),
        t.terms,
        t.postings,
        t.postings_in_full_blocks,
        t.shard_logical_bytes,
        t.skip_bytes,
        t.scoring_bytes,
        t.positions_bytes,
        t.scoring_bytes_as_v5,
        t.scoring_bytes_as_v6
    );
    if failed > 0 {
        std::process::exit(1);
    }
}
