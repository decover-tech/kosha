//! Tokenizer / analyzer throughput (issues #220, #221). Analysis runs once
//! per document per `Text` field on the write path, so a regression here
//! is a write-throughput regression.
//!
//! Corpus: every Markdown file in the repository (README, DESIGN, docs —
//! real English prose with code, tables and punctuation), repeated to
//! ~`KOSHA_BENCH_CORPUS_MB` (default 16) MB and split into ~2 KB
//! "documents". `KOSHA_BENCH_CORPUS=<path>` benchmarks a file of your own
//! instead.
//!
//!     cargo bench -p kosha-segment --bench tokenizer

use std::path::Path;
use std::time::Instant;

use kosha_core::{AnalysisConfig, AnalyzerSpec, Bm25Params, DocumentId, Field, SegmentId};
use kosha_segment::{Analyzer, SegmentWriter};

fn markdown_corpus(root: &Path, out: &mut String) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    let mut paths: Vec<_> = entries.flatten().map(|e| e.path()).collect();
    paths.sort();
    for path in paths {
        let name = path.file_name().unwrap().to_string_lossy();
        if name.starts_with('.') || name == "target" || name == "node_modules" {
            continue;
        }
        if path.is_dir() {
            markdown_corpus(&path, out);
        } else if path.extension().is_some_and(|e| e == "md") {
            if let Ok(text) = std::fs::read_to_string(&path) {
                out.push_str(&text);
                out.push('\n');
            }
        }
    }
}

fn main() {
    let target_mb: usize = std::env::var("KOSHA_BENCH_CORPUS_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);
    let seed = match std::env::var("KOSHA_BENCH_CORPUS") {
        Ok(path) => std::fs::read_to_string(path).expect("read KOSHA_BENCH_CORPUS"),
        Err(_) => {
            let mut text = String::new();
            markdown_corpus(
                &Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."),
                &mut text,
            );
            text
        }
    };
    assert!(!seed.is_empty(), "empty corpus");
    let mut corpus = String::with_capacity(target_mb << 20);
    while corpus.len() < target_mb << 20 {
        corpus.push_str(&seed);
    }
    // ~2 KB documents, cut on char boundaries.
    let mut docs: Vec<&str> = Vec::new();
    let mut rest = corpus.as_str();
    while !rest.is_empty() {
        let mut cut = rest.len().min(2048);
        while !rest.is_char_boundary(cut) {
            cut += 1;
        }
        docs.push(&rest[..cut]);
        rest = &rest[cut..];
    }
    let bytes = corpus.len() as f64;
    println!(
        "corpus: {:.1} MB in {} documents ({} KB seed)",
        bytes / (1 << 20) as f64,
        docs.len(),
        seed.len() >> 10
    );
    println!(
        "{:<32} {:>10} {:>12} {:>12} {:>10}",
        "analyzer", "MB/s", "Mtokens/s", "tokens", "vs legacy"
    );
    let mut legacy_mbps = None;
    for names in [
        &["standard"][..],
        &["standard@2"],
        &["standard", "stop", "stem"],
        &["standard@2", "stop", "stem"],
        &["whitespace"],
        &["keyword"],
    ] {
        let names: Vec<String> = names.iter().map(|s| s.to_string()).collect();
        let analyzer = Analyzer::new(&AnalyzerSpec::resolve(&names, None).unwrap());
        // Warm-up, then best of 3.
        let mut tokens = 0usize;
        for doc in docs.iter().take(500) {
            tokens += analyzer.analyze_with_positions(doc).len();
        }
        let mut best = f64::MAX;
        for _ in 0..3 {
            tokens = 0;
            let start = Instant::now();
            for doc in &docs {
                tokens += std::hint::black_box(analyzer.analyze_with_positions(doc)).len();
            }
            best = best.min(start.elapsed().as_secs_f64());
        }
        let mbps = bytes / (1 << 20) as f64 / best;
        let ratio = legacy_mbps.map_or(1.0, |legacy: f64| mbps / legacy);
        legacy_mbps.get_or_insert(mbps);
        println!(
            "{:<32} {:>10.1} {:>12.2} {:>12} {:>9.2}x",
            format!("{names:?}"),
            mbps,
            tokens as f64 / best / 1e6,
            tokens,
            ratio
        );
    }

    // The same corpus through the whole segment writer (analysis, postings,
    // doc store, finalize): the write-path cost the tokenizer is part of.
    println!();
    println!(
        "{:<32} {:>10} {:>12} {:>10}",
        "segment build", "MB/s", "docs/s", "vs legacy"
    );
    let mut legacy_mbps = None;
    for names in [
        &["standard"][..],
        &["standard@2"],
        &["standard@2", "stop", "stem"],
    ] {
        let names: Vec<String> = names.iter().map(|s| s.to_string()).collect();
        let config = AnalysisConfig {
            default: AnalyzerSpec::resolve(&names, None).unwrap(),
            fields: Default::default(),
        };
        let mut best = f64::MAX;
        for round in 0..3 {
            let dir = std::env::temp_dir().join(format!(
                "kosha-tokenizer-bench-{}-{round}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let start = Instant::now();
            let mut writer =
                SegmentWriter::new(SegmentId("bench".into()), dir.clone()).with_analysis(&config);
            for (i, doc) in docs.iter().enumerate() {
                writer.add_document(DocumentId(format!("d{i}")), vec![Field::text("body", *doc)]);
            }
            writer.finalize(Bm25Params::default()).unwrap();
            best = best.min(start.elapsed().as_secs_f64());
            let _ = std::fs::remove_dir_all(&dir);
        }
        let mbps = bytes / (1 << 20) as f64 / best;
        let ratio = legacy_mbps.map_or(1.0, |legacy: f64| mbps / legacy);
        legacy_mbps.get_or_insert(mbps);
        println!(
            "{:<32} {:>10.1} {:>12.0} {:>9.2}x",
            format!("{names:?}"),
            mbps,
            docs.len() as f64 / best,
            ratio
        );
    }
}
