//! Exact filtered kNN without a whole local `doc_vectors.bin`.
//!
//! A query node never promotes an f32 segment's sidecar whole (it is as
//! large as the f32 vector index and no f32 reader needs all of it), so
//! exact filtered kNN (#252) reads the candidates' vectors by range. These
//! tests pin that the ranged read returns exactly what the whole file does
//! — same documents, same score bits — and that a segment with a quantized
//! column is still the one that keeps the whole file.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use kosha_core::{Bm25Params, DocumentId, Field, SegmentId, VectorQuantization};
use kosha_segment::{
    doc_vectors_header_len, doc_vectors_kept_whole, span_cache, ExactKnnSource, SegmentReader,
    SegmentWriter, VectorQuantizationConfig, VectorStoreMode, DOC_VECTORS_FILE,
};

fn temp_dir(name: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-dv-remote-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Deterministic pseudo-random vector in `[-1, 1)^dim`.
fn vector(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    (0..dim)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
        })
        .collect()
}

/// A sidecar segment of `n` documents with a `dim`-dimensional `emb` and a
/// small second vector field, so the sidecar interleaves two fields.
fn write(
    dir: &Path,
    n: usize,
    dim: usize,
    quantization: VectorQuantizationConfig,
) -> kosha_core::Footer {
    let mut w = SegmentWriter::new(SegmentId("dv".into()), dir.to_path_buf())
        .with_vector_store_mode(VectorStoreMode::Sidecar)
        .with_vector_quantization(quantization);
    for i in 0..n {
        w.add_document(
            DocumentId(format!("doc-{i:05}")),
            vec![
                Field::text("body", format!("document {i}")),
                Field::vector("emb", vector(i as u64, dim)),
                Field::vector("title_emb", vector(1_000_000 + i as u64, 8)),
            ],
        );
    }
    w.finalize(Bm25Params::default()).unwrap()
}

/// A copy of `seg` as a query node holds it after this change: every file
/// but `doc_vectors.bin`, whose header alone sits in the span cache (the
/// scoring hydration fetches it for a filtered kNN).
fn node_copy(seg: &Path, name: &str, with_header: bool) -> PathBuf {
    let node = temp_dir(name);
    for entry in std::fs::read_dir(seg).unwrap().flatten() {
        let file = entry.file_name();
        if file == DOC_VECTORS_FILE || entry.path().is_dir() {
            continue;
        }
        std::fs::copy(entry.path(), node.join(&file)).unwrap();
    }
    if with_header {
        let all = std::fs::read(seg.join(DOC_VECTORS_FILE)).unwrap();
        let len = doc_vectors_header_len(&all).unwrap().unwrap() as usize;
        put_span(&node, 0, &all[..len]);
    }
    node
}

fn put_span(node: &Path, start: u64, bytes: &[u8]) {
    let dir = span_cache::spans_dir(node, DOC_VECTORS_FILE);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(span_cache::span_file_name(
            start,
            start + bytes.len() as u64,
        )),
        bytes,
    )
    .unwrap();
}

/// Hits as `(doc_seq, score bits)`: equal only if bit-identical.
fn bits(hits: &[(u32, f64)]) -> Vec<(u32, u64)> {
    hits.iter().map(|&(id, s)| (id, s.to_bits())).collect()
}

/// Every eleventh document passes (a sparse matter), or all of them.
fn exact_is_identical_without_the_whole_file(n: usize, dim: usize, every: u32) {
    let seg = temp_dir("seg");
    write(&seg, n, dim, VectorQuantizationConfig::default());
    let file_len = std::fs::metadata(seg.join(DOC_VECTORS_FILE)).unwrap().len();
    let candidates: Vec<u32> = (0..n as u32).filter(|i| i % every == 0).collect();
    let query = vector(424_242, dim);

    let whole = SegmentReader::open(seg.clone()).unwrap();
    let want = whole
        .knn_exact_filtered("emb", &query, 10, &candidates)
        .unwrap()
        .unwrap();
    assert_eq!(want.source, ExactKnnSource::Sidecar);
    assert_eq!(want.hits.len(), 10);

    // The node fetches ranges from the "remote" segment into its span cache.
    let node = node_copy(&seg, "node", true);
    let remote = std::fs::read(seg.join(DOC_VECTORS_FILE)).unwrap();
    let fetched = AtomicU64::new(0);
    let max_range = AtomicU64::new(0);
    let fetch = |dir: &Path, file: &str, ranges: &[(u64, u64)]| {
        assert_eq!(file, DOC_VECTORS_FILE);
        for &(s, e) in ranges {
            fetched.fetch_add(e - s, Ordering::Relaxed);
            max_range.fetch_max(e - s, Ordering::Relaxed);
            put_span(dir, s, &remote[s as usize..e as usize]);
        }
    };
    let reader = SegmentReader::open(node.clone()).unwrap();
    let got = reader
        .knn_exact_filtered_fetching("emb", &query, 10, &candidates, Some(&fetch))
        .unwrap()
        .unwrap();
    assert!(!node.join(DOC_VECTORS_FILE).exists(), "never fetched whole");
    assert_eq!(got.source, ExactKnnSource::Sidecar, "not the posting scan");
    assert_eq!(got.postings_hydrated, 0);
    assert_eq!(got.vectors_scored, want.vectors_scored);
    assert_eq!(bits(&got.hits), bits(&want.hits), "bit-identical hits");
    let fetched = fetched.load(Ordering::Relaxed);
    assert!(
        fetched > 0 && fetched <= file_len,
        "{fetched} of {file_len}"
    );
    assert!(
        max_range.load(Ordering::Relaxed) <= kosha_segment::DocVectors::MAX_RUN_BYTES,
        "one run never asks for more than MAX_RUN_BYTES"
    );

    // A warm repeat reads the cached ranges: no fetch, same answer.
    let again = SegmentReader::open(node.clone()).unwrap();
    let no_fetch = |_: &Path, _: &str, r: &[(u64, u64)]| panic!("warm repeat fetched {r:?}");
    let warm = again
        .knn_exact_filtered_fetching("emb", &query, 10, &candidates, Some(&no_fetch))
        .unwrap()
        .unwrap();
    assert_eq!(bits(&warm.hits), bits(&want.hits));

    for dir in [seg, node] {
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[test]
fn exact_filtered_knn_reads_sparse_candidates_by_range_with_identical_hits() {
    exact_is_identical_without_the_whole_file(3_000, 64, 11);
}

/// Every document passes (a one-matter segment): one dense run per
/// `MAX_RUN_BYTES`, not one read the size of the file.
#[test]
fn exact_filtered_knn_reads_a_dense_set_in_bounded_runs_with_identical_hits() {
    // 2,100 x 1024-dim f32 vectors = 8.6 MB > MAX_RUN_BYTES (8 MiB).
    exact_is_identical_without_the_whole_file(2_100, 1024, 1);
}

/// With neither the whole file nor its header (a shed header fetch) the
/// exact path still answers, from the vector postings, as before.
#[test]
fn exact_filtered_knn_without_the_header_scores_postings() {
    let seg = temp_dir("seg");
    write(&seg, 600, 32, VectorQuantizationConfig::default());
    let node = node_copy(&seg, "node", false);
    let candidates: Vec<u32> = (0..600).filter(|i| i % 7 == 0).collect();
    let query = vector(99, 32);
    let reader = SegmentReader::open(node.clone()).unwrap();
    let got = reader
        .knn_exact_filtered("emb", &query, 5, &candidates)
        .unwrap()
        .unwrap();
    assert_eq!(got.source, ExactKnnSource::Postings);
    assert_eq!(got.hits.len(), 5);
    let want = SegmentReader::open(seg.clone())
        .unwrap()
        .knn_exact_filtered("emb", &query, 5, &candidates)
        .unwrap()
        .unwrap();
    assert_eq!(
        got.hits.iter().map(|h| h.0).collect::<Vec<_>>(),
        want.hits.iter().map(|h| h.0).collect::<Vec<_>>()
    );
    for dir in [seg, node] {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// Only a quantized column's segment keeps `doc_vectors.bin` whole: its
/// per-segment rescoring reads it, and without it returns estimates.
#[test]
fn only_quantized_columns_keep_the_sidecar_whole() {
    let f32_seg = temp_dir("f32");
    let f32_footer = write(&f32_seg, 50, 16, VectorQuantizationConfig::default());
    assert!(f32_footer.doc_vectors);
    assert!(!doc_vectors_kept_whole(&f32_footer));
    assert!(!kosha_segment::segment_keeps_doc_vectors_whole(&f32_seg));

    let i8_seg = temp_dir("i8");
    let i8_footer = write(
        &i8_seg,
        50,
        16,
        VectorQuantizationConfig::default().with_field("emb", VectorQuantization::I8),
    );
    assert!(doc_vectors_kept_whole(&i8_footer));
    assert!(kosha_segment::segment_keeps_doc_vectors_whole(&i8_seg));
    let reader = SegmentReader::open(i8_seg.clone()).unwrap();
    assert!(reader.rescores_quantized_locally(), "whole file rescores");

    // A legacy footer listing no columns cannot say: kept whole, as before.
    let legacy = kosha_core::Footer {
        vector_fields: None,
        ..f32_footer.clone()
    };
    assert!(doc_vectors_kept_whole(&legacy));
    // No sidecar at all: nothing to keep.
    let inline = kosha_core::Footer {
        doc_vectors: false,
        ..i8_footer
    };
    assert!(!doc_vectors_kept_whole(&inline));
    for dir in [f32_seg, i8_seg] {
        let _ = std::fs::remove_dir_all(dir);
    }
}
