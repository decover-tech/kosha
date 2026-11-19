//! Peak heap of building one segment, with and without the posting spill
//! (#242). Its own test binary so the process-wide peak counter only sees
//! these builds.

use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};
use kosha_segment::SegmentWriter;
use std::path::PathBuf;

#[global_allocator]
static ALLOC: cap::Cap<std::alloc::System> = cap::Cap::new(std::alloc::System, usize::MAX);

const DOCS: usize = 20_000;
const MIB: usize = 1024 * 1024;

/// ~1.6 KB of prose per document from a 5,000-word vocabulary, generated
/// on the fly so the corpus itself never sits in memory.
fn doc(i: usize) -> (DocumentId, Vec<Field>) {
    let mut text = String::with_capacity(1700);
    let mut x = (i as u64).wrapping_mul(6364136223846793005).wrapping_add(1);
    for _ in 0..240 {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let word = (x >> 33) % 5_000;
        text.push_str(&format!("w{word} "));
    }
    (
        DocumentId(format!("doc-{i:06}")),
        vec![
            Field::text("body", text),
            Field::keyword("bucket", format!("b{}", i % 11)),
        ],
    )
}

/// Peak live heap above the starting point while building the segment.
fn build_peak(name: &str, budget: Option<usize>) -> usize {
    let dir: PathBuf =
        std::env::temp_dir().join(format!("kosha-build-memory-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let base = ALLOC.allocated();
    let mut writer = SegmentWriter::new(SegmentId(name.into()), dir.clone());
    if let Some(budget) = budget {
        writer = writer.with_build_memory_bytes(budget);
    }
    for i in 0..DOCS {
        let (id, fields) = doc(i);
        writer.add_document(id, fields);
    }
    writer.finalize(Bm25Params::default()).unwrap();
    let peak = ALLOC.max_allocated().saturating_sub(base);
    std::fs::remove_dir_all(&dir).unwrap();
    peak
}

#[test]
fn spilling_bounds_peak_heap_of_a_segment_build() {
    // Bounded build first: the counter only ever rises, so the larger,
    // unbounded build has to come second to be measured.
    let bounded = build_peak("bounded", Some(8 * MIB));
    let unbounded = build_peak("unbounded", Some(usize::MAX));
    eprintln!(
        "peak heap for {DOCS} docs: unbounded {:.1} MiB, 8 MiB budget {:.1} MiB",
        unbounded as f64 / MIB as f64,
        bounded as f64 / MIB as f64
    );
    assert!(
        bounded * 3 < unbounded,
        "spilling should cut peak heap by well over 3x: {bounded} vs {unbounded}"
    );
    assert!(
        bounded < 48 * MIB,
        "bounded build peaked at {bounded} bytes"
    );
}
