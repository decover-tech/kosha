//! Re-encodes a namespace's f32 vector column as 1-bit RaBitQ codes,
//! segment by segment, into a new directory: the scratch namespace of the
//! codes-scan A/B (`KOSHA_KNN_RABITQ_BUDGET_FRACTION`), on exactly the
//! same postings, centroids and documents as the f32 original.
//!
//! ```text
//! # <src> holds the namespace's segment directories, all files local
//! # (e.g. `aws s3 sync s3://<bucket>/<prefix>/default/paragraph_index_hnsw/ src/`)
//! kosha-reencode-rabitq --src src --dst dst --field contentEmbedding
//! # upload dst/ to the scratch namespace's prefix (default/exp_rabitq) and
//! # attach it with POST /v1/admin/import-namespace
//! ```
//!
//! Every file is hard-linked into `<dst>/<segment>/` (copied where links
//! fail), then [`SegmentReader::rewrite_vector_column_rabitq`] replaces
//! the column's `vector.idx`, `vector.offsets` and `footer.json` there by
//! rename, so `<src>` is never modified. A segment without the field or
//! without `doc_vectors.bin` fails the run: a partial scratch namespace
//! would mix encodings without saying so.

use std::path::{Path, PathBuf};

use clap::Parser;
use kosha_segment::SegmentReader;
use rayon::prelude::*;

#[derive(Parser)]
#[command(name = "kosha-reencode-rabitq")]
struct Args {
    /// Namespace directory: one subdirectory per segment, each with its
    /// `footer.json`.
    #[arg(long)]
    src: PathBuf,
    /// Output namespace directory (created; must not hold those segments).
    #[arg(long)]
    dst: PathBuf,
    /// Vector field to re-encode.
    #[arg(long)]
    field: String,
    /// Segments re-encoded at once (0: rayon's default). Each holds one
    /// posting plus its column's old and new `vector.offsets` (~2 x 23 MB
    /// for a 3.7K-posting 1536-d segment).
    #[arg(long, default_value_t = 0)]
    threads: usize,
}

fn link_or_copy(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir(dst)?;
    for entry in std::fs::read_dir(src)? {
        let path = entry?.path();
        if path.is_file() {
            let to = dst.join(path.file_name().unwrap());
            if std::fs::hard_link(&path, &to).is_err() {
                std::fs::copy(&path, &to)?;
            }
        }
    }
    Ok(())
}

/// Bytes of the files in `dir`.
fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|d| {
            d.flatten()
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

fn main() {
    let args = Args::parse();
    if args.threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(args.threads)
            .build_global()
            .expect("thread pool");
    }
    let mut segments: Vec<PathBuf> = std::fs::read_dir(&args.src)
        .expect("read --src")
        .map(|e| e.expect("read --src").path())
        .filter(|p| p.join("footer.json").is_file())
        .collect();
    segments.sort();
    assert!(
        !segments.is_empty(),
        "no segments under {}",
        args.src.display()
    );
    std::fs::create_dir_all(&args.dst).expect("create --dst");
    let results: Vec<Result<(PathBuf, u64, u64), String>> = segments
        .par_iter()
        .map(|src| {
            let dst = args.dst.join(src.file_name().unwrap());
            link_or_copy(src, &dst).map_err(|e| format!("{}: {e}", dst.display()))?;
            SegmentReader::rewrite_vector_column_rabitq(&dst, &args.field)
                .map_err(|e| e.to_string())?;
            Ok((dst.clone(), dir_bytes(src), dir_bytes(&dst)))
        })
        .collect();
    let mut failed = 0;
    let (mut before, mut after) = (0u64, 0u64);
    for result in results {
        match result {
            Ok((dst, b, a)) => {
                println!(
                    "ok {} {:.1} MB -> {:.1} MB",
                    dst.display(),
                    b as f64 / 1e6,
                    a as f64 / 1e6
                );
                before += b;
                after += a;
            }
            Err(e) => {
                eprintln!("ERROR: {e}");
                failed += 1;
            }
        }
    }
    println!(
        "segments={} failed={failed} total {:.1} MB -> {:.1} MB",
        segments.len(),
        before as f64 / 1e6,
        after as f64 / 1e6
    );
    if failed > 0 {
        std::process::exit(1);
    }
}
