//! Peak heap of a merge (#242): several input segments read back through
//! `iter_doc_records_raw` and fed to one `SegmentWriter`, the way
//! compaction does, when one term appears many times in every document.
//! Its own test binary so the process-wide peak counter only sees this.
//!
//! The acceptance bound: peak heap stays under the build budget plus
//! O(vocabulary) plus about 30 B per merged document for the doc-store
//! offsets (no vectors; those are #253). Before the streaming per-term
//! encoder, `finalize` materialized the stopword's full posting list
//! across every run (about 32 B per posting plus 4 B per position), which
//! grows with the merged segment and breaks the bound.

use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};
use kosha_segment::{SegmentReader, SegmentWriter};
use std::path::{Path, PathBuf};

#[global_allocator]
static ALLOC: cap::Cap<std::alloc::System> = cap::Cap::new(std::alloc::System, usize::MAX);

const MIB: usize = 1024 * 1024;
const INPUTS: usize = 4;
/// Occurrences of the stopword in every document.
const STOPWORD_TF: usize = 32;

/// `STOPWORD_TF` copies of `the` plus 40 words from a 20,000-word
/// vocabulary, generated on the fly.
fn doc(i: usize) -> (DocumentId, Vec<Field>) {
    let mut text = "the ".repeat(STOPWORD_TF);
    let mut x = (i as u64).wrapping_mul(6364136223846793005).wrapping_add(1);
    for _ in 0..40 {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        text.push_str(&format!("w{} ", (x >> 33) % 20_000));
    }
    (
        DocumentId(format!("doc-{i:07}")),
        vec![Field::text("body", text)],
    )
}

fn scratch(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("kosha-merge-memory-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Build `INPUTS` segments holding `docs` documents between them.
fn build_inputs(root: &Path, docs: usize) -> Vec<PathBuf> {
    let per = docs / INPUTS;
    (0..INPUTS)
        .map(|k| {
            let dir = root.join(format!("in-{k}"));
            let mut w = SegmentWriter::new(SegmentId(format!("in-{k}")), dir.clone())
                .with_build_memory_bytes(MIB);
            for i in k * per..(k + 1) * per {
                let (id, fields) = doc(i);
                w.add_document(id, fields);
            }
            w.finalize(Bm25Params::default()).unwrap();
            dir
        })
        .collect()
}

/// Merge `inputs` into one segment under `budget`, as compaction does.
/// Returns the peak live heap above the starting point.
fn merge_peak(inputs: &[PathBuf], out: &Path, budget: usize) -> usize {
    let base = ALLOC.allocated();
    let mut w = SegmentWriter::new(SegmentId("merged".into()), out.to_path_buf())
        .with_build_memory_bytes(budget);
    for input in inputs {
        let reader = SegmentReader::open(input.clone()).unwrap();
        for raw in reader.iter_doc_records_raw() {
            let raw = raw.unwrap();
            w.add_document_with_vectors(raw.record.doc_id, raw.record.fields, raw.vectors);
        }
    }
    let footer = w.finalize(Bm25Params::default()).unwrap();
    let peak = ALLOC.max_allocated().saturating_sub(base);
    assert_eq!(
        footer.doc_count as usize,
        inputs.len() * (footer.doc_count as usize / inputs.len())
    );
    peak
}

#[test]
fn merge_peak_heap_is_bounded_when_a_term_is_in_every_doc() {
    let budget = 4 * MIB;
    let root = scratch("root");
    let mut results = Vec::new();
    // Smaller merge first: the peak counter only ever rises, so each
    // measurement is an upper bound on its own merge's peak.
    for docs in [50_000usize, 200_000] {
        let inputs = build_inputs(&root.join(docs.to_string()), docs);
        let out = root.join(format!("out-{docs}"));
        let peak = merge_peak(&inputs, &out, budget);

        let reader = SegmentReader::open(out.clone()).unwrap();
        assert_eq!(reader.doc_count() as usize, docs);
        let the = reader.postings("the").unwrap();
        assert_eq!(the.len(), docs);
        assert!(the.iter().all(|p| p.term_frequency as usize == STOPWORD_TF));
        drop(the);
        drop(reader);

        // Budget + 64 B per doc + 6 MiB. Per doc: the doc-store offsets
        // (~31 B here, up to 2x with `Vec` growth), field lengths (4 B, also
        // growth-padded) and the open input's offsets table (~35 B per doc
        // of one input). The constant covers the vocabulary (term table,
        // string pool, bloom), run read buffers and the per-term encoder.
        let bound = budget + docs * 64 + 6 * MIB;
        // What the old merge held for the stopword alone at `finalize`.
        let stopword_bytes = docs * (32 + 4 * STOPWORD_TF);
        eprintln!(
            "merge of {docs} docs ({INPUTS} inputs, {} MiB budget): peak heap {:.1} MiB, \
             bound {:.1} MiB; a materialized `the` posting list alone is {:.1} MiB",
            budget / MIB,
            peak as f64 / MIB as f64,
            bound as f64 / MIB as f64,
            stopword_bytes as f64 / MIB as f64,
        );
        results.push((docs, peak, bound));
        std::fs::remove_dir_all(root.join(docs.to_string())).unwrap();
        std::fs::remove_dir_all(&out).unwrap();
    }
    for (docs, peak, bound) in results {
        assert!(
            peak <= bound,
            "merge of {docs} docs peaked at {peak} bytes, over the {bound}-byte bound"
        );
    }
    let _ = std::fs::remove_dir_all(root);
}
