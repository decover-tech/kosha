//! Measures the segment cache's byte *accounting* against reality.
//!
//! `kosha_query::approx_segment_bytes` is what the segment cache's byte
//! budget AND the admission ledger's per-search estimate are both denominated
//! in. It has to track what a parsed `SegmentReader` really keeps resident,
//! which is far less than the segment's on-disk size:
//! `SegmentReader::open_with_footer_options` holds `doc_store.bin` as
//! `DocStoreAccess::Lazy` (path + offsets index) and `vector.idx` as
//! `LazyVectorIndex` (centroids + ranges).
//!
//! Charging the full on-disk size instead was issue #136: a 120-230x
//! over-charge that made every kNN query evict the entire cache and then
//! serialize on the admission ledger's anti-starvation path.
//!
//! This prints both, per segment, using the same counting global allocator
//! (`cap`) the `segment_memory` bench uses — exact allocated bytes, not RSS.
//!
//! ```text
//! cargo bench -p kosha-query --bench vector_segment_accounting
//! ```

use std::alloc::System;
use std::hint::black_box;

use cap::Cap;
use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};
use kosha_query::approx_segment_bytes;
use kosha_segment::{SegmentReader, SegmentWriter};

#[global_allocator]
static ALLOC: Cap<System> = Cap::new(System, usize::MAX);

fn live_bytes() -> usize {
    ALLOC.allocated()
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() {
    // Defaults chosen to mirror the 10M MSMarco vector run's *shape*: a fat
    // doc_store (real text bodies) plus 1024-dim embeddings, at 1/100th the
    // per-segment doc count so it runs in seconds.
    let docs = env_usize("KOSHA_BENCH_DOCS", 500);
    let dim = env_usize("KOSHA_BENCH_DIM", 1024);
    let body_words = env_usize("KOSHA_BENCH_BODY_WORDS", 300);

    let root = std::env::temp_dir().join("kosha-bench-vector-accounting");
    let _ = std::fs::remove_dir_all(&root);
    let seg_dir = root.join("ns").join("s0");

    eprintln!("building 1 segment: {docs} docs × {body_words} words × {dim}-dim vectors …");
    let mut w = SegmentWriter::new(SegmentId("s0".into()), seg_dir.clone());
    for d in 0..docs {
        // A realistic-ish body so doc_store.bin is genuinely large relative
        // to the postings — the MSMarco shape, where doc_store dominates.
        let body: String = (0..body_words)
            .map(|i| format!("w{} ", (d * 7 + i * 13) % 5000))
            .collect();
        let vector: Vec<f32> = (0..dim).map(|i| ((d * dim + i) as f32).sin()).collect();
        w.add_document(
            DocumentId(format!("d{d}")),
            vec![Field::text("t", body), Field::vector("emb", vector)],
        );
    }
    w.finalize(Bm25Params::default()).unwrap();

    for load_vectors in [false, true] {
        // Measure allocated bytes held by a live reader.
        let before = live_bytes();
        let reader = SegmentReader::open_with_options(seg_dir.clone(), load_vectors).unwrap();
        let resident = live_bytes().saturating_sub(before) as u64;
        black_box(&reader);
        drop(reader);

        let charged = approx_segment_bytes(&seg_dir, load_vectors);
        let variant = if load_vectors { "kNN  " } else { "BM25 " };
        println!(
            "{variant} charged {:>9.2} MiB | actually resident {:>8.3} MiB | overcharge {:>7.1}x",
            mib(charged),
            mib(resident),
            charged as f64 / resident.max(1) as f64,
        );
    }

    println!();
    println!("per-file on-disk sizes:");
    for f in [
        "doc_store.bin",
        "doc_store.offsets",
        "inverted.idx",
        "filters.bin",
        "footer.json",
        "vector.idx",
        "vector.offsets",
    ] {
        let n = std::fs::metadata(seg_dir.join(f))
            .map(|m| m.len())
            .unwrap_or(0);
        println!("  {f:<20} {:>9.3} MiB", mib(n));
    }

    let _ = std::fs::remove_dir_all(&root);
}
