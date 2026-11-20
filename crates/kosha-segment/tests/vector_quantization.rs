//! i8 scalar-quantized vector postings (#210).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use kosha_core::{Bm25Params, DocumentId, Field, SegmentId, VectorQuantization};
use kosha_segment::{
    SegmentReader, SegmentWriter, VectorQuantizationConfig, VectorStoreMode,
    I8_VECTOR_INDEX_VERSION, RADIUS_AWARE_VECTOR_INDEX_VERSION,
};

const DIM: usize = 64;

fn temp_dir(name: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-vector-quant-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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

fn docs(n: usize) -> Vec<(String, Vec<Field>)> {
    (0..n)
        .map(|i| {
            (
                format!("doc-{i:05}"),
                vec![
                    Field::text("body", format!("document {i}")),
                    Field::vector("emb", vector(i as u64, DIM)),
                    Field::vector("title_emb", vector(1_000_000 + i as u64, 8)),
                ],
            )
        })
        .collect()
}

fn write(
    dir: &Path,
    mode: VectorStoreMode,
    quantization: VectorQuantizationConfig,
    docs: Vec<(String, Vec<Field>)>,
) -> kosha_core::Footer {
    let mut w = SegmentWriter::new(SegmentId("quant".into()), dir.to_path_buf())
        .with_vector_store_mode(mode)
        .with_vector_quantization(quantization);
    for (id, fields) in docs {
        w.add_document(DocumentId(id), fields);
    }
    w.finalize(Bm25Params::default()).unwrap()
}

fn i8_emb() -> VectorQuantizationConfig {
    VectorQuantizationConfig::default().with_field("emb", VectorQuantization::I8)
}

fn files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_file())
        .map(|p| {
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read(&p).unwrap(),
            )
        })
        .collect()
}

fn header_version(idx: &[u8]) -> u32 {
    u32::from_le_bytes(idx[4..8].try_into().unwrap())
}

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let dot: f64 = a.iter().zip(b).map(|(x, y)| *x as f64 * *y as f64).sum();
    let na: f64 = a.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    let nb: f64 = b.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    dot / (na * nb)
}

fn brute_force(n: usize, query: &[f32], k: usize) -> Vec<(u32, f64)> {
    let mut scored: Vec<(u32, f64)> = (0..n)
        .map(|i| (i as u32, cosine(query, &vector(i as u64, DIM))))
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    scored.truncate(k);
    scored
}

/// Exhaustive probe: every posting is read.
fn knn(reader: &SegmentReader, query: &[f32], k: usize) -> Vec<(u32, f64)> {
    reader
        .knn_search_filtered("emb", query, k, 10_000, None)
        .expect("column has a lazy index")
        .unwrap()
}

/// Budgeted probe with no cutoff: every posting is read.
fn knn_within(reader: &SegmentReader, query: &[f32], k: usize) -> Vec<(u32, f64)> {
    reader
        .knn_search_within_filtered("emb", query, k, f32::MAX, usize::MAX, None)
        .expect("column has a lazy index")
        .unwrap()
        .0
}

fn recall(got: &[(u32, f64)], want: &[(u32, f64)]) -> f64 {
    let want: Vec<u32> = want.iter().map(|(id, _)| *id).collect();
    got.iter().filter(|(id, _)| want.contains(id)).count() as f64 / want.len() as f64
}

#[test]
fn i8_column_is_a_quarter_of_the_f32_index_and_tagged_in_the_footer() {
    let n = 600;
    let f32_dir = temp_dir("f32");
    let i8_dir = temp_dir("i8");
    let f32_footer = write(
        &f32_dir,
        VectorStoreMode::Sidecar,
        VectorQuantizationConfig::default(),
        docs(n),
    );
    let i8_footer = write(&i8_dir, VectorStoreMode::Sidecar, i8_emb(), docs(n));

    let quant = |footer: &kosha_core::Footer, name: &str| {
        footer
            .vector_fields
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.name == name)
            .unwrap()
            .quantization
    };
    assert_eq!(quant(&f32_footer, "emb"), VectorQuantization::F32);
    assert_eq!(quant(&i8_footer, "emb"), VectorQuantization::I8);
    // The per-field override leaves the other column at the default.
    assert_eq!(quant(&i8_footer, "title_emb"), VectorQuantization::F32);

    let footer_json = std::fs::read_to_string(i8_dir.join("footer.json")).unwrap();
    assert!(
        footer_json.contains("\"quantization\": \"i8\""),
        "{footer_json}"
    );
    assert_eq!(footer_json.matches("quantization").count(), 1);
    let f32_json = std::fs::read_to_string(f32_dir.join("footer.json")).unwrap();
    assert!(
        !f32_json.contains("quantization"),
        "f32 footers are unchanged"
    );

    let f32_files = files(&f32_dir);
    let i8_files = files(&i8_dir);
    assert_eq!(
        header_version(&f32_files["vector.idx"]),
        RADIUS_AWARE_VECTOR_INDEX_VERSION
    );
    assert_eq!(
        header_version(&i8_files["vector.idx"]),
        I8_VECTOR_INDEX_VERSION
    );
    // Entry bytes: [id][f32 x 64] = 260 vs [id][scale][i8 x 64] = 72.
    let entries = |idx: &[u8]| (idx.len() - 16) / n;
    assert_eq!(entries(&f32_files["vector.idx"]), 4 + 4 * DIM);
    assert_eq!(entries(&i8_files["vector.idx"]), 4 + 4 + DIM);
    // The f32 column, the sidecar of exact vectors and everything else are
    // untouched by quantizing `emb`.
    for name in [
        "vector.1.idx",
        "vector.1.offsets",
        "doc_vectors.bin",
        "doc_store.bin",
    ] {
        assert_eq!(f32_files[name], i8_files[name], "{name}");
    }
    // Centroids stay f32: the sidecar differs only in its version and the
    // idx offsets it points at, not in size.
    assert_eq!(
        f32_files["vector.offsets"].len(),
        i8_files["vector.offsets"].len()
    );
}

#[test]
fn f32_segments_are_byte_identical_with_the_flag_off() {
    let default_dir = temp_dir("default");
    let explicit_dir = temp_dir("explicit");
    write(
        &default_dir,
        VectorStoreMode::Sidecar,
        VectorQuantizationConfig::default(),
        docs(200),
    );
    write(
        &explicit_dir,
        VectorStoreMode::Sidecar,
        VectorQuantizationConfig::parse(Some("f32"), Some("emb:f32")),
        docs(200),
    );
    let (a, b) = (files(&default_dir), files(&explicit_dir));
    assert_eq!(a.keys().collect::<Vec<_>>(), b.keys().collect::<Vec<_>>());
    for (name, bytes) in &a {
        if name == "footer.json" {
            continue; // carries a timestamp
        }
        assert_eq!(bytes, &b[name], "{name}");
    }
}

#[test]
fn rescoring_restores_exact_scores_and_recall() {
    let n = 2000;
    let dir = temp_dir("recall");
    write(&dir, VectorStoreMode::Sidecar, i8_emb(), docs(n));
    let rescored = SegmentReader::open(dir.clone()).unwrap();
    let raw = SegmentReader::open(dir.clone())
        .unwrap()
        .with_vector_rescore_factor(0);

    let (mut recall_rescored, mut recall_raw) = (0.0, 0.0);
    let queries = 20;
    for q in 0..queries {
        let query = vector(500_000 + q, DIM);
        let want = brute_force(n, &query, 10);
        for probe in [knn, knn_within] {
            let got = probe(&rescored, &query, 10);
            assert_eq!(got.len(), 10);
            // Rescored hits carry the exact cosine, not the i8 estimate.
            for (id, score) in &got {
                let exact = cosine(&query, &vector(*id as u64, DIM));
                assert!((score - exact).abs() < 1e-5, "{score} vs {exact}");
            }
            recall_rescored += recall(&got, &want) / 2.0;

            let got = probe(&raw, &query, 10);
            for (id, score) in &got {
                let exact = cosine(&query, &vector(*id as u64, DIM));
                assert!(
                    (score - exact).abs() < 0.02,
                    "i8 estimate {score} vs {exact}"
                );
            }
            recall_raw += recall(&got, &want) / 2.0;
        }
    }
    recall_rescored /= queries as f64;
    recall_raw /= queries as f64;
    assert!(
        recall_rescored >= 0.99,
        "rescored recall@10 {recall_rescored}"
    );
    assert!(recall_raw >= 0.85, "i8-only recall@10 {recall_raw}");
}

#[test]
fn filtered_probe_and_exact_path_work_on_i8_columns() {
    let n = 500;
    let dir = temp_dir("filtered");
    write(&dir, VectorStoreMode::Sidecar, i8_emb(), docs(n));
    let reader = SegmentReader::open(dir.clone()).unwrap();
    let query = vector(77_777, DIM);
    let even = |id: u32| id.is_multiple_of(2);
    let got = reader
        .knn_search_within_filtered("emb", &query, 5, f32::MAX, usize::MAX, Some(&even))
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(got.len(), 5);
    assert!(got.iter().all(|(id, _)| id.is_multiple_of(2)));

    let candidates: Vec<u32> = (0..n as u32).filter(|id| id.is_multiple_of(2)).collect();
    let exact = reader
        .knn_exact_filtered("emb", &query, 5, &candidates)
        .unwrap()
        .unwrap();
    assert_eq!(
        exact.hits.iter().map(|h| h.0).collect::<Vec<_>>(),
        got.iter().map(|h| h.0).collect::<Vec<_>>()
    );
}

#[test]
fn inline_vector_segments_score_with_i8_estimates() {
    // No `doc_vectors.bin` to rescore from: the i8 scores stand.
    let n = 400;
    let dir = temp_dir("inline");
    write(&dir, VectorStoreMode::Inline, i8_emb(), docs(n));
    assert!(!dir.join("doc_vectors.bin").exists());
    let reader = SegmentReader::open(dir.clone()).unwrap();
    let query = vector(31_337, DIM);
    let got = knn(&reader, &query, 10);
    assert_eq!(got.len(), 10);
    assert!(recall(&got, &brute_force(n, &query, 10)) >= 0.7);
    for (id, score) in &got {
        let exact = cosine(&query, &vector(*id as u64, DIM));
        assert!((score - exact).abs() < 0.02);
    }
}

#[test]
fn i8_column_without_its_sidecar_is_served_dequantized() {
    let n = 300;
    let dir = temp_dir("eager");
    write(&dir, VectorStoreMode::Sidecar, i8_emb(), docs(n));
    std::fs::remove_file(dir.join("vector.offsets")).unwrap();
    let reader = SegmentReader::open(dir.clone()).unwrap();
    let index = reader.vector_field("emb").unwrap().unwrap();
    assert!(!index.has_lazy_index());
    let flat = &index.flat().vectors;
    assert_eq!(flat.len(), n);
    for (id, v) in flat {
        // Unit-norm copy of the original, to within the quantization step.
        assert!(cosine(v, &vector(*id as u64, DIM)) > 0.999);
    }
}

#[test]
fn an_unknown_vector_index_version_is_rejected_loudly() {
    let dir = temp_dir("unknown-version");
    write(&dir, VectorStoreMode::Sidecar, i8_emb(), docs(50));
    let path = dir.join("vector.idx");
    let mut idx = std::fs::read(&path).unwrap();
    idx[4..8].copy_from_slice(&99u32.to_le_bytes());
    std::fs::write(&path, idx).unwrap();
    let reader = SegmentReader::open(dir.clone()).unwrap();
    let err = reader.vector_field("emb").err().expect("must not load");
    assert!(err.to_string().contains("version 99"), "{err}");
    // The other column is unaffected.
    assert!(reader.vector_field("title_emb").unwrap().is_some());
}

#[test]
fn posting_cache_holds_i8_entries() {
    let n = 1000;
    let f32_dir = temp_dir("cache-f32");
    let i8_dir = temp_dir("cache-i8");
    write(
        &f32_dir,
        VectorStoreMode::Sidecar,
        VectorQuantizationConfig::default(),
        docs(n),
    );
    write(&i8_dir, VectorStoreMode::Sidecar, i8_emb(), docs(n));
    let warm = |dir: &Path| {
        let reader = SegmentReader::open(dir.to_path_buf()).unwrap();
        let query = vector(4242, DIM);
        // Second-touch admission: the second probe caches every posting.
        knn(&reader, &query, 10);
        knn(&reader, &query, 10);
        reader.vector_cache_resident_bytes()
    };
    let (f32_bytes, i8_bytes) = (warm(&f32_dir), warm(&i8_dir));
    assert!(i8_bytes > 0);
    assert!(
        f32_bytes >= 3 * i8_bytes,
        "f32 cache {f32_bytes} B vs i8 cache {i8_bytes} B"
    );
}

#[test]
fn quantization_env_values_parse() {
    let config = VectorQuantizationConfig::parse(Some(" I8 "), Some("a:f32, b:i8,bad,:i8,c:f16"));
    assert_eq!(config.default, VectorQuantization::I8);
    assert_eq!(config.for_field("a"), VectorQuantization::F32);
    assert_eq!(config.for_field("b"), VectorQuantization::I8);
    assert_eq!(config.for_field("c"), VectorQuantization::I8);
    assert_eq!(config.fields.len(), 2);
    assert_eq!(
        VectorQuantizationConfig::parse(None, None),
        VectorQuantizationConfig::default()
    );
    assert_eq!(
        VectorQuantizationConfig::parse(Some("f16"), None).default,
        VectorQuantization::F32
    );
}

/// Compaction re-adds every document from `iter_doc_records` (see
/// `kosha-write`'s compaction loop), which reads vectors from
/// `doc_vectors.bin` or the doc store — never from the quantized index. An
/// i8 segment therefore compacts losslessly: rewriting it as f32 gives
/// exactly the index a direct f32 build of the same documents gives.
#[test]
fn compacting_an_i8_segment_rebuilds_from_full_precision_vectors() {
    for mode in [VectorStoreMode::Sidecar, VectorStoreMode::Inline] {
        let n = 300;
        let i8_dir = temp_dir("compact-in");
        write(&i8_dir, mode, i8_emb(), docs(n));
        let reader = SegmentReader::open(i8_dir.clone()).unwrap();

        let rewritten_dir = temp_dir("compact-out");
        let mut w = SegmentWriter::new(SegmentId("quant".into()), rewritten_dir.clone())
            .with_vector_store_mode(mode)
            .with_vector_quantization(VectorQuantizationConfig::default());
        for record in reader.iter_doc_records() {
            let record = record.unwrap();
            let i: u64 = record.doc_id.0[4..].parse().unwrap();
            let emb = record.fields.iter().find(|f| f.name == "emb").unwrap();
            let v: Vec<f32> = serde_json::from_str(&emb.value).unwrap();
            assert_eq!(
                v,
                vector(i, DIM),
                "{mode:?}: doc {i} keeps its exact vector"
            );
            w.add_document(record.doc_id, record.fields);
        }
        w.finalize(Bm25Params::default()).unwrap();

        let direct_dir = temp_dir("compact-direct");
        write(
            &direct_dir,
            mode,
            VectorQuantizationConfig::default(),
            docs(n),
        );
        let (rewritten, direct) = (files(&rewritten_dir), files(&direct_dir));
        for name in [
            "vector.idx",
            "vector.offsets",
            "vector.1.idx",
            "vector.1.offsets",
        ] {
            assert_eq!(rewritten[name], direct[name], "{mode:?}: {name}");
        }
    }
}
