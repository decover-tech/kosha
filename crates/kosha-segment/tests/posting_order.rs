//! Posting-ordered doc_seqs (#456, research): `SegmentWriter::posting_order`
//! numbers a segment's documents posting by posting, so each posting of
//! the ordered column covers one contiguous doc_seq range.

use std::path::{Path, PathBuf};

use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};
use kosha_segment::reorder::doc_seq_runs;
use kosha_segment::{SegmentReader, SegmentWriter};

const DIM: usize = 16;
const DOCS: u64 = 3_000;

fn temp_dir(name: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-posting-order-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Deterministic pseudo-random value stream in `[-1, 1)`.
fn noise(seed: u64, n: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    (0..n)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
        })
        .collect()
}

/// Doc `i`: one of 24 cluster centres plus jitter, a `tenant` keyword
/// that follows ingest order (not the vector), and every 10th doc has no
/// vector.
fn doc(i: u64) -> (DocumentId, Vec<Field>) {
    let mut fields = vec![
        Field::text("body", format!("document {i}")),
        Field::keyword("tenant", format!("t{}", i % 7)),
    ];
    if i % 10 != 9 {
        let centre = noise(1_000_000 + i % 24, DIM);
        let v = centre
            .iter()
            .zip(noise(i, DIM))
            .map(|(c, j)| c + 0.3 * j)
            .collect();
        fields.push(Field::vector("emb", v));
    }
    (DocumentId(format!("doc-{i:05}")), fields)
}

fn build(dir: &Path, posting_order: bool) {
    let docs: Vec<_> = (0..DOCS).map(doc).collect();
    let mut w = SegmentWriter::new(SegmentId("po".into()), dir.to_path_buf());
    if posting_order {
        let p = w
            .posting_order(docs.iter().map(|(_, f)| f.as_slice()), "emb")
            .expect("vectors present");
        let mut sorted = p.order.clone();
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            (0..DOCS as u32).collect::<Vec<_>>(),
            "a permutation"
        );
        let mut slots: Vec<_> = docs.into_iter().map(Some).collect();
        for old in p.order {
            let (id, fields) = slots[old as usize].take().unwrap();
            w.add_document(id, fields);
        }
        w.seed_vector_postings("emb", p.seeds);
    } else {
        for (id, fields) in docs {
            w.add_document(id, fields);
        }
    }
    w.finalize(Bm25Params::default()).unwrap();
}

/// `(postings, postings that are one contiguous range, total runs)`.
fn contiguity(dir: &Path) -> (usize, usize, usize) {
    let reader = SegmentReader::open(dir.to_path_buf()).unwrap();
    let layout = reader.vector_posting_layout("emb").unwrap().unwrap();
    let runs: Vec<usize> = layout
        .postings
        .iter()
        .map(|p| doc_seq_runs(&p.doc_seqs))
        .collect();
    (
        runs.len(),
        runs.iter().filter(|&&r| r == 1).count(),
        runs.iter().sum(),
    )
}

#[test]
fn posting_order_makes_postings_contiguous() {
    let (off_dir, po_dir) = (temp_dir("off"), temp_dir("posting"));
    build(&off_dir, false);
    build(&po_dir, true);
    let (off_postings, off_contiguous, off_runs) = contiguity(&off_dir);
    let (po_postings, po_contiguous, po_runs) = contiguity(&po_dir);
    assert!(off_postings > 4 && po_postings > 4);
    // Ingest order scatters every posting; posting order makes (nearly)
    // every posting one range, and documents without a vector go last.
    assert!(
        off_contiguous * 10 < off_postings,
        "{off_contiguous}/{off_postings}"
    );
    assert!(
        po_contiguous * 10 >= po_postings * 9,
        "{po_contiguous}/{po_postings}"
    );
    assert!(po_runs * 4 < off_runs, "{po_runs} vs {off_runs}");

    let po = SegmentReader::open(po_dir.clone()).unwrap();
    let layout = po.vector_posting_layout("emb").unwrap().unwrap();
    let indexed = layout
        .postings
        .iter()
        .map(|p| p.doc_seqs.len())
        .sum::<usize>() as u32;
    let max_seq = layout
        .postings
        .iter()
        .flat_map(|p| &p.doc_seqs)
        .max()
        .copied();
    assert_eq!(max_seq, Some(indexed - 1), "vector docs come first");
}

#[test]
fn posting_order_keeps_documents_and_filters() {
    let (off_dir, po_dir) = (temp_dir("off-f"), temp_dir("posting-f"));
    build(&off_dir, false);
    build(&po_dir, true);
    let off = SegmentReader::open(off_dir).unwrap();
    let po = SegmentReader::open(po_dir).unwrap();
    assert_eq!(off.doc_count(), po.doc_count());
    let filter: kosha_core::FilterClause =
        serde_json::from_value(serde_json::json!({"term": {"tenant": "t3"}})).unwrap();
    let ids = |r: &SegmentReader| {
        let set = r.evaluate_filter(&filter).unwrap();
        let mut ids: Vec<String> = set
            .iter()
            .map(|seq| r.doc_meta(seq).unwrap().doc_id.to_string())
            .collect();
        ids.sort();
        ids
    };
    let (a, b) = (ids(&off), ids(&po));
    assert_eq!(a.len(), (0..DOCS).filter(|i| i % 7 == 3).count());
    assert_eq!(a, b);
}

#[test]
fn posting_order_without_vectors_is_none() {
    let dir = temp_dir("none");
    let w = SegmentWriter::new(SegmentId("po".into()), dir);
    let docs = [vec![Field::text("body", "no vectors here")]];
    assert!(w
        .posting_order(docs.iter().map(|f| f.as_slice()), "emb")
        .is_none());
}
