//! Multiple vector columns per segment (#204).
//!
//! `tests/fixtures/legacy-vector-seg` (vectors in `doc_vectors.bin`) and
//! `tests/fixtures/legacy-vector-seg-inline` (vectors inline in the doc
//! store) were written by the segment writer as it was before vector
//! columns existed (origin/main at dd6e523) from the documents in
//! [`legacy_docs`]: one 4-dim `emb` field, a single `vector.idx` +
//! `vector.offsets`, and no `vector_fields` in the footer.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use kosha_core::{Bm25Params, DocumentId, Field, FieldType, SegmentId, VectorFieldMeta};
use kosha_segment::{SegmentReader, SegmentWriter, VectorFieldStatus, VectorStoreMode};

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn temp_dir(name: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-vector-columns-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The fixture generator's vectors (see the module docs).
fn legacy_vec(i: usize) -> Vec<f32> {
    let a = i as f32 * 0.7;
    vec![
        a.sin(),
        a.cos(),
        (a * 1.3).sin(),
        ((i % 5) as f32) / 5.0 - 0.4,
    ]
}

fn legacy_docs() -> Vec<(String, Vec<Field>)> {
    (0..24)
        .map(|i| {
            (
                format!("legacy-{i:02}"),
                vec![
                    Field::text("body", format!("legacy document number {i}")),
                    Field::vector("emb", legacy_vec(i)),
                ],
            )
        })
        .collect()
}

fn write(dir: &Path, mode: VectorStoreMode, docs: Vec<(String, Vec<Field>)>) -> kosha_core::Footer {
    let mut w = SegmentWriter::new(SegmentId("legacy-vec".into()), dir.to_path_buf())
        .with_vector_store_mode(mode)
        .with_text_filter_ignore_above(Some(256))
        // The fixtures predate per-field indexes (#249), which change the
        // lexical files; the vector files are what this suite compares.
        .with_field_index(false)
        .with_filters_write_version(1);
    for (id, fields) in docs {
        w.add_document(DocumentId(id), fields);
    }
    w.finalize(Bm25Params::default()).unwrap()
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

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    (dot / (na * nb)) as f64
}

/// Exact top-`k` doc_seqs of `vectors` for `query`.
fn brute_force(vectors: &[(u32, Vec<f32>)], query: &[f32], k: usize) -> Vec<u32> {
    let mut scored: Vec<(u32, f64)> = vectors
        .iter()
        .map(|(seq, v)| (*seq, cosine(query, v)))
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    scored.into_iter().take(k).map(|(seq, _)| seq).collect()
}

/// Every stored vector of `field` in the segment, by doc_seq, read back
/// from the documents themselves (not from any index).
fn stored_vectors(reader: &SegmentReader, field: &str) -> Vec<(u32, Vec<f32>)> {
    (0..reader.footer().doc_count)
        .filter_map(|seq| {
            let record = reader.doc_record_full(seq).unwrap()?;
            let f = record
                .fields
                .iter()
                .find(|f| f.name == field && f.field_type == FieldType::Vector)?;
            Some((seq, serde_json::from_str(&f.value).unwrap()))
        })
        .collect()
}

/// Exhaustive probe of `field`'s column: every posting is read, so the
/// result must equal brute force.
fn knn(reader: &SegmentReader, field: &str, query: &[f32], k: usize) -> Vec<u32> {
    reader
        .knn_search_filtered(field, query, k, 10_000, None)
        .expect("column has a lazy index")
        .unwrap()
        .into_iter()
        .map(|(seq, _)| seq)
        .collect()
}

#[test]
fn single_column_segment_is_byte_identical_to_the_pre_columns_writer() {
    for (name, mode) in [
        ("legacy-vector-seg", VectorStoreMode::Sidecar),
        ("legacy-vector-seg-inline", VectorStoreMode::Inline),
    ] {
        let dir = temp_dir(name);
        let footer = write(&dir, mode, legacy_docs());
        assert_eq!(
            footer.vector_fields,
            Some(vec![VectorFieldMeta {
                name: "emb".into(),
                dim: 4,
                count: 24,
                slot: 0,
                offsets: true,
                quantization: Default::default(),
                embedding_model: None,
                radius_reference: None,
            }])
        );
        let expected = files(&fixture(name));
        let actual = files(&dir);
        assert_eq!(
            expected.keys().collect::<Vec<_>>(),
            actual.keys().collect::<Vec<_>>(),
            "{name}: same set of files — no new vector files for one column"
        );
        for (file, bytes) in &expected {
            if file == "footer.json" {
                let strip = |b: &[u8]| {
                    let mut v: serde_json::Value = serde_json::from_slice(b).unwrap();
                    let o = v.as_object_mut().unwrap();
                    o.remove("created_at");
                    o.remove("vector_fields");
                    // Optional footer metadata added on main after the
                    // fixtures were written (#201, #245).
                    o.remove("fuzzy_bloom");
                    o.remove("posting_blobs_raw");
                    v
                };
                assert_eq!(strip(bytes), strip(&actual[file]), "{name}: footer.json");
            } else {
                assert!(
                    *bytes == actual[file],
                    "{name}: {file} differs from the fixture"
                );
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[test]
fn legacy_segment_serves_knn_through_the_migration_rule() {
    let query = [0.3, 0.9, -0.2, 0.1];
    for name in ["legacy-vector-seg", "legacy-vector-seg-inline"] {
        let reader = SegmentReader::open(fixture(name)).unwrap();
        assert!(
            reader.footer().vector_fields.is_none(),
            "{name}: legacy footer"
        );
        assert!(reader.vector_field_names().is_empty());
        assert_eq!(
            reader.vector_field_status("emb"),
            VectorFieldStatus::Legacy { dim: Some(4) },
            "{name}"
        );
        let truth = stored_vectors(&reader, "emb");
        assert_eq!(truth.len(), 24);
        assert_eq!(
            knn(&reader, "emb", &query, 5),
            brute_force(&truth, &query, 5)
        );
    }

    // `doc_vectors.bin` names exactly one field: that binds the unnamed
    // index to it, so another field name finds nothing here.
    let bound = SegmentReader::open(fixture("legacy-vector-seg")).unwrap();
    assert_eq!(
        bound.vector_field_status("title_emb"),
        VectorFieldStatus::Absent
    );
    assert!(bound.vector_field("title_emb").unwrap().is_none());
    // Inline vectors leave the field unknown: the index answers any name.
    let unbound = SegmentReader::open(fixture("legacy-vector-seg-inline")).unwrap();
    assert_eq!(
        unbound.vector_field_status("title_emb"),
        VectorFieldStatus::Legacy { dim: Some(4) }
    );
}

fn two_column_docs() -> Vec<(String, Vec<Field>)> {
    (0..60)
        .map(|i| {
            let a = i as f32 * 0.37;
            let mut fields = vec![Field::text("body", format!("doc {i}"))];
            // Every third doc has no title vector, every fifth no body one.
            if i % 3 != 0 {
                fields.push(Field::vector(
                    "title_emb",
                    vec![a.sin(), a.cos(), (2.0 * a).sin(), 0.5],
                ));
            }
            if i % 5 != 0 {
                fields.push(Field::vector(
                    "body_emb",
                    (0..8).map(|d| ((d as f32 + 1.0) * a).cos()).collect(),
                ));
            }
            (format!("d{i:02}"), fields)
        })
        .collect()
}

#[test]
fn two_columns_of_different_dimensions_get_their_own_indexes() {
    for mode in [VectorStoreMode::Sidecar, VectorStoreMode::Inline] {
        let dir = temp_dir("two-columns");
        let footer = write(&dir, mode, two_column_docs());
        let fields = footer.vector_fields.clone().unwrap();
        assert_eq!(
            fields
                .iter()
                .map(|m| (m.name.as_str(), m.dim, m.count, m.slot))
                .collect::<Vec<_>>(),
            vec![("title_emb", 4, 40, 0), ("body_emb", 8, 48, 1)]
        );
        for file in [
            "vector.idx",
            "vector.offsets",
            "vector.1.idx",
            "vector.1.offsets",
        ] {
            assert!(dir.join(file).exists(), "{file}");
        }

        let reader = SegmentReader::open(dir.clone()).unwrap();
        assert_eq!(reader.vector_field_names(), vec!["title_emb", "body_emb"]);
        assert_eq!(
            reader.vector_field_status("title_emb"),
            VectorFieldStatus::Indexed { dim: 4 }
        );
        assert_eq!(
            reader.vector_field_status("body_emb"),
            VectorFieldStatus::Indexed { dim: 8 }
        );
        assert_eq!(
            reader.vector_field_status("nope"),
            VectorFieldStatus::Absent
        );
        assert!(reader.vector_field("nope").unwrap().is_none());
        assert!(reader.knn_centroid_distances("nope", &[1.0; 4]).is_none());

        for (field, query) in [
            ("title_emb", vec![0.2, 0.9, 0.1, 0.5]),
            ("body_emb", (0..8).map(|d| (d as f32 * 0.4).sin()).collect()),
        ] {
            let truth = stored_vectors(&reader, field);
            let got = knn(&reader, field, &query, 7);
            assert_eq!(got, brute_force(&truth, &query, 7), "{field}");
            // Only documents that have this column can be its neighbours.
            let with_field: Vec<u32> = truth.iter().map(|(seq, _)| *seq).collect();
            assert!(got.iter().all(|seq| with_field.contains(seq)), "{field}");
            // Every posting of the column belongs to it: an unbounded
            // probe returns exactly the column's documents.
            let mut all = knn(&reader, field, &query, 1_000);
            all.sort_unstable();
            assert_eq!(all, with_field, "{field}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[test]
fn columns_are_independent_and_load_lazily() {
    // The title column of a two-column segment is byte-identical to a
    // segment holding the title vectors alone.
    let two = temp_dir("two");
    write(&two, VectorStoreMode::Sidecar, two_column_docs());
    let one = temp_dir("one");
    let title_only = two_column_docs()
        .into_iter()
        .map(|(id, fields)| {
            let fields = fields
                .into_iter()
                .filter(|f| f.name != "body_emb")
                .collect();
            (id, fields)
        })
        .collect();
    write(&one, VectorStoreMode::Sidecar, title_only);
    for file in ["vector.idx", "vector.offsets"] {
        assert!(
            std::fs::read(two.join(file)).unwrap() == std::fs::read(one.join(file)).unwrap(),
            "{file}"
        );
    }

    // A lexical open touches no vector file at all.
    let lexical = SegmentReader::open_with_options(two.clone(), false).unwrap();
    assert!(lexical.vector_field("title_emb").unwrap().is_none());

    // A kNN on one column never reads the other column's files: with
    // `body_emb`'s files gone, `title_emb` still searches, and `body_emb`
    // reports the missing file instead of silently answering empty.
    std::fs::remove_file(two.join("vector.1.idx")).unwrap();
    std::fs::remove_file(two.join("vector.1.offsets")).unwrap();
    let reader = SegmentReader::open(two.clone()).unwrap();
    assert!(reader
        .knn_search_filtered("title_emb", &[1.0, 0.0, 0.0, 0.5], 3, 100, None)
        .unwrap()
        .is_ok());
    assert!(reader.vector_field("body_emb").is_err());

    std::fs::remove_dir_all(&two).ok();
    std::fs::remove_dir_all(&one).ok();
}

#[test]
fn a_vector_of_the_wrong_dimension_is_stored_but_not_indexed() {
    let dir = temp_dir("mismatch");
    let mut w = SegmentWriter::new(SegmentId("s".into()), dir.clone())
        .with_vector_store_mode(VectorStoreMode::Sidecar);
    w.add_document(
        DocumentId("a".into()),
        vec![Field::vector("emb", vec![1.0, 0.0, 0.0])],
    );
    w.add_document(
        DocumentId("b".into()),
        vec![Field::vector("emb", vec![0.0, 1.0])],
    );
    w.add_document(
        DocumentId("c".into()),
        vec![Field::vector("emb", vec![0.0, 1.0, 0.0])],
    );
    assert_eq!(w.unindexed_vector_count(), 1);
    // The flush succeeds: one stray document does not fail it.
    let footer = w.finalize(Bm25Params::default()).unwrap();
    let meta = &footer.vector_fields.unwrap()[0];
    assert_eq!((meta.dim, meta.count), (3, 2));

    let reader = SegmentReader::open(dir.clone()).unwrap();
    // Still stored losslessly.
    let b = reader.doc_record_full(1).unwrap().unwrap();
    assert_eq!(b.fields[0].value, "[0.0,1.0]");
    let all = knn(&reader, "emb", &[0.0, 1.0, 0.0], 10);
    assert_eq!(all, vec![2, 0]);
    std::fs::remove_dir_all(&dir).ok();
}
