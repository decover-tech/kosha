//! Property tests for the segment decoders (#257): random documents
//! round-trip through a segment build and open; footers, filter columns
//! and late-interaction bags round-trip through their codecs; and random
//! byte mutation or truncation of any file of a valid segment makes the
//! reader return errors, never panic.
//!
//! 64 cases per property by default to keep `cargo test` fast; set
//! `PROPTEST_CASES` for a deeper run. The fuzz targets in `fuzz/` drive
//! the same entry points with coverage guidance.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use kosha_core::{
    Bm25Params, DocumentId, Field, FieldType, FilterClause, FilterColumnKind, Footer, SegmentId,
};
use kosha_segment::filter_columns::{decode_column, encode_column, FilterColumn, NumericColumn};
use kosha_segment::multivec::{open_multivec_files, parse_bag};
use kosha_segment::{parse_vector_index_files, SegmentReader, SegmentWriter};
use proptest::prelude::*;

fn config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    ProptestConfig {
        cases,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "kosha-decoder-prop-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            copy_dir(&path, &to.join(entry.file_name()));
        } else {
            std::fs::copy(&path, to.join(entry.file_name())).unwrap();
        }
    }
}

const WORDS: [&str; 6] = ["alpha", "beta", "gamma", "delta", "kosha", "segment"];

/// One valid segment with every kind of file a writer produces: text,
/// keyword, integer and float filters, a vector column and a late-
/// interaction bag field. Built once; each case mutates a copy.
fn template() -> &'static (PathBuf, Vec<String>) {
    static TEMPLATE: OnceLock<(PathBuf, Vec<String>)> = OnceLock::new();
    TEMPLATE.get_or_init(|| {
        let dir = temp_dir("template");
        let _ = std::fs::remove_dir_all(&dir);
        let mut writer = SegmentWriter::new(SegmentId("tpl".into()), dir.clone());
        for i in 0..24u32 {
            let f = i as f32;
            let bag = vec![vec![1.0, f * 0.1, 0.5], vec![0.2, 1.0, f * 0.05]];
            writer.add_document(
                DocumentId(format!("doc-{i}")),
                vec![
                    Field::text(
                        "body",
                        format!(
                            "{} {} {i}",
                            WORDS[i as usize % 6],
                            WORDS[(i as usize + 2) % 6]
                        ),
                    ),
                    Field::keyword("tag", WORDS[i as usize % 3]),
                    Field::integer("n", i as i64),
                    Field::float_val("x", f as f64 / 3.0),
                    Field::vector("emb", vec![f, 1.0 - f / 24.0, 0.5, f % 3.0]),
                    Field {
                        name: "tokens".into(),
                        field_type: FieldType::Vector,
                        value: serde_json::to_string(&bag).unwrap(),
                    },
                ],
            );
        }
        writer.finalize(Bm25Params::default()).unwrap();
        let mut files: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .filter_map(|e| e.file_name().to_str().map(str::to_owned))
            .collect();
        files.sort();
        (dir, files)
    })
}

/// Read everything a search, a delete and a compaction would read. Errors
/// are fine; a panic fails the property.
fn exercise(dir: PathBuf) {
    let Ok(reader) = SegmentReader::open(dir) else {
        return;
    };
    // Bounded: a corrupt footer can claim any doc count.
    let docs = reader.doc_count().min(1000);
    for seq in 0..docs + 2 {
        let _ = reader.doc_meta(seq);
        let _ = reader.doc_record_full(seq);
    }
    for record in reader.iter_doc_records_raw().take(1000) {
        let _ = record;
    }
    for filter in [
        r#"{"term": {"tag": "beta"}}"#,
        r#"{"range": {"n": {"gte": "3", "lt": "12"}}}"#,
        r#"{"range": {"x": {"gt": "1.5"}}}"#,
    ] {
        let clause: FilterClause = serde_json::from_str(filter).unwrap();
        let _ = reader.evaluate_filter(&clause);
    }
    for word in WORDS {
        let _ = reader.postings(word).map(|p| p.len());
    }
    let _ = reader.take_blob_read_failure();
    let _ = reader.field_lengths("body");
    if let Ok(Some(_)) = reader.vector_field("emb") {
        let _ = reader.knn_search(&[1.0, 0.5, 0.5, 1.0], 5, 4);
        let _ = reader.vector_posting_layout("emb");
    }
    let q: [&[f32]; 2] = [&[1.0, 0.2, 0.5], &[0.2, 1.0, 0.1]];
    let _ = reader.late_interaction_search(
        "tokens",
        &q,
        3,
        kosha_segment::LateInteractionParams {
            nprobe: 4,
            num_candidates: 8,
        },
        None,
    );
}

#[derive(Debug, Clone)]
enum Damage {
    Flip(Vec<(prop::sample::Index, u8)>),
    Truncate(prop::sample::Index),
    Empty,
}

fn damage() -> impl Strategy<Value = Damage> {
    prop_oneof![
        6 => prop::collection::vec((any::<prop::sample::Index>(), 1u8..=255), 1..6)
            .prop_map(Damage::Flip),
        3 => any::<prop::sample::Index>().prop_map(Damage::Truncate),
        1 => Just(Damage::Empty),
    ]
}

fn apply(bytes: &mut Vec<u8>, damage: &Damage) {
    match damage {
        Damage::Flip(flips) if !bytes.is_empty() => {
            for (at, mask) in flips {
                let i = at.index(bytes.len());
                bytes[i] ^= mask;
            }
        }
        Damage::Flip(_) => {}
        Damage::Truncate(at) => {
            let len = if bytes.is_empty() {
                0
            } else {
                at.index(bytes.len())
            };
            bytes.truncate(len);
        }
        Damage::Empty => bytes.clear(),
    }
}

fn doc_fields() -> impl Strategy<Value = Vec<Field>> {
    (
        prop::collection::vec(prop::sample::select(WORDS.to_vec()), 1..6),
        prop::sample::select(WORDS.to_vec()),
        any::<i32>(),
        -1e6f64..1e6,
    )
        .prop_map(|(words, tag, n, x)| {
            vec![
                Field::text("body", words.join(" ")),
                Field::keyword("tag", tag),
                Field::integer("n", n as i64),
                Field::float_val("x", x),
            ]
        })
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn random_documents_round_trip_through_a_segment(
        docs in prop::collection::btree_map("[a-z0-9]{1,8}", doc_fields(), 1..20),
    ) {
        let dir = temp_dir("roundtrip");
        let mut writer = SegmentWriter::new(SegmentId("rt".into()), dir.clone());
        for (id, fields) in &docs {
            writer.add_document(DocumentId(id.clone()), fields.clone());
        }
        let footer = writer.finalize(Bm25Params::default()).unwrap();
        prop_assert_eq!(footer.doc_count as usize, docs.len());
        let reader = SegmentReader::open(dir.clone()).unwrap();
        let mut seen = BTreeMap::new();
        for seq in 0..reader.doc_count() {
            let record = reader.doc_record_full(seq).unwrap().unwrap();
            let values: Vec<(String, String)> = record
                .fields
                .iter()
                .map(|f| (f.name.clone(), f.value.clone()))
                .collect();
            seen.insert(record.doc_id.0.clone(), values);
        }
        let expected: BTreeMap<String, Vec<(String, String)>> = docs
            .iter()
            .map(|(id, fields)| {
                (id.clone(), fields.iter().map(|f| (f.name.clone(), f.value.clone())).collect())
            })
            .collect();
        prop_assert_eq!(seen, expected);

        // The footer survives a JSON round trip unchanged.
        let on_disk = SegmentReader::read_footer(&dir).unwrap();
        let json = serde_json::to_value(&on_disk).unwrap();
        let back: Footer = serde_json::from_value(json.clone()).unwrap();
        prop_assert_eq!(serde_json::to_value(&back).unwrap(), json);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn damaged_segment_files_never_panic_the_reader(
        file in any::<prop::sample::Index>(),
        damage in damage(),
    ) {
        let (template, files) = template();
        let name = &files[file.index(files.len())];
        let dir = temp_dir("damaged");
        copy_dir(template, &dir);
        let mut bytes = std::fs::read(dir.join(name)).unwrap();
        apply(&mut bytes, &damage);
        std::fs::write(dir.join(name), &bytes).unwrap();
        exercise(dir.clone());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn damaged_footer_json_never_panics(damage in damage()) {
        let (template, _) = template();
        let mut bytes = std::fs::read(template.join("footer.json")).unwrap();
        apply(&mut bytes, &damage);
        let _ = serde_json::from_slice::<Footer>(&bytes);
    }

    #[test]
    fn integer_columns_round_trip_and_reject_damage(
        values in prop::collection::btree_map(0u32..64, any::<i64>(), 0..40),
        damage in damage(),
    ) {
        let entries: Vec<(u32, i64)> = values.into_iter().collect();
        let column = FilterColumn::Integer(NumericColumn::from_entries(&entries));
        let bytes = encode_column(&column, 64);
        let back = decode_column(&bytes, FilterColumnKind::Integer, 64, "prop").unwrap();
        prop_assert_eq!(&back, &column);
        let mut damaged = bytes.clone();
        apply(&mut damaged, &damage);
        let _ = decode_column(&damaged, FilterColumnKind::Integer, 64, "prop");
        // Truncation always loses data the header promised.
        if damaged.len() < bytes.len() {
            prop_assert!(decode_column(&damaged, FilterColumnKind::Integer, 64, "prop").is_err());
        }
    }

    #[test]
    fn float_columns_round_trip(
        values in prop::collection::btree_map(0u32..64, -1e9f64..1e9, 0..40),
        damage in damage(),
    ) {
        let entries: Vec<(u32, f64)> = values.into_iter().collect();
        let column = FilterColumn::Float(NumericColumn::from_entries(&entries));
        let bytes = encode_column(&column, 64);
        let back = decode_column(&bytes, FilterColumnKind::Float, 64, "prop").unwrap();
        prop_assert_eq!(&back, &column);
        let mut damaged = bytes;
        apply(&mut damaged, &damage);
        let _ = decode_column(&damaged, FilterColumnKind::Float, 64, "prop");
    }

    #[test]
    fn arbitrary_column_bytes_never_panic(
        bytes in prop::collection::vec(any::<u8>(), 0..512),
        doc_count in 0u32..1 << 20,
    ) {
        for kind in [FilterColumnKind::String, FilterColumnKind::Integer, FilterColumnKind::Float] {
            let _ = decode_column(&bytes, kind, doc_count, "prop");
        }
    }

    #[test]
    fn bags_round_trip(
        dim in 1usize..6,
        tokens in prop::collection::vec(prop::collection::vec(-1e3f32..1e3, 6), 1..6),
    ) {
        let bag: Vec<Vec<f32>> = tokens.into_iter().map(|t| t[..dim].to_vec()).collect();
        let parsed = parse_bag(&serde_json::to_string(&bag).unwrap());
        prop_assert_eq!(parsed, Some(bag.clone()));
        // A ragged bag is not a bag.
        let mut ragged = bag;
        ragged[0].push(0.0);
        if ragged.len() > 1 {
            prop_assert_eq!(parse_bag(&serde_json::to_string(&ragged).unwrap()), None);
        }
    }

    #[test]
    fn arbitrary_bag_text_never_panics(text in ".{0,64}") {
        let _ = parse_bag(&text);
    }

    #[test]
    fn damaged_multivec_files_error_never_panic(
        which in 0usize..2,
        damage in damage(),
    ) {
        let (template, _) = template();
        let dir = temp_dir("multivec");
        copy_dir(template, &dir);
        let name = [kosha_segment::multivec::bags_file(0), kosha_segment::multivec::ivf_file(0)]
            [which]
            .clone();
        prop_assert_eq!(open_multivec_files(&dir, 0, 3, 24).unwrap(), 48);
        let mut bytes = std::fs::read(dir.join(&name)).unwrap();
        apply(&mut bytes, &damage);
        std::fs::write(dir.join(&name), &bytes).unwrap();
        let _ = open_multivec_files(&dir, 0, 3, 24);
        // Footer metadata that disagrees with the files is an error.
        prop_assert!(open_multivec_files(&dir, 0, 4, 24).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn damaged_vector_index_never_panics(
        target in 0usize..2,
        damage in damage(),
    ) {
        let (template, _) = template();
        let files = kosha_segment::vector_index_files(template);
        let idx = std::fs::read(template.join(&files[0])).unwrap();
        let offsets = std::fs::read(template.join(&files[1])).unwrap();
        let (postings, vectors) = parse_vector_index_files(&idx, &offsets);
        prop_assert!(postings.is_some() && vectors == Some(24));
        let (mut idx, mut offsets) = (idx, offsets);
        apply(if target == 0 { &mut idx } else { &mut offsets }, &damage);
        let _ = parse_vector_index_files(&idx, &offsets);
    }
}

// ─── Regressions for panics the properties found (#257) ──────────────────

fn damaged_copy(tag: &str, file: &str, edit: impl FnOnce(&mut Vec<u8>)) -> PathBuf {
    let (template, _) = template();
    let dir = temp_dir(tag);
    copy_dir(template, &dir);
    let mut bytes = std::fs::read(dir.join(file)).unwrap();
    edit(&mut bytes);
    std::fs::write(dir.join(file), &bytes).unwrap();
    dir
}

fn is_corrupt<T: std::fmt::Debug>(result: Result<T, kosha_core::KoshaError>) -> bool {
    matches!(result, Err(kosha_core::KoshaError::CorruptSegment(_)))
}

#[test]
fn doc_store_record_with_an_overlong_length_is_corrupt_not_a_panic() {
    // Record 0 follows the 4-byte doc count; its first word is the id
    // length. Unchecked, this sliced past the end of the record.
    let dir = damaged_copy("doc-store-len", "doc_store.bin", |b| b[7] = 0x50);
    let reader = SegmentReader::open(dir.clone()).unwrap();
    assert!(is_corrupt(reader.doc_record_full(0)));
    assert!(
        reader.doc_record_full(1).unwrap().is_some(),
        "others still read"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn truncated_legacy_filters_bin_is_corrupt_not_a_panic() {
    let dir = damaged_copy("filters-cut", "filters.bin", |b| b.truncate(b.len() / 2));
    let reader = SegmentReader::open(dir.clone()).unwrap();
    if !reader.has_filter_columns() {
        let clause: FilterClause = serde_json::from_str(r#"{"term": {"tag": "beta"}}"#).unwrap();
        assert!(is_corrupt(reader.evaluate_filter(&clause).map(|_| ())));
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn vector_index_headers_whose_sizes_overflow_are_rejected() {
    let files = kosha_segment::vector_index_files(&template().0);
    let mut idx = std::fs::read(template().0.join(&files[0])).unwrap();
    let mut offsets = std::fs::read(template().0.join(&files[1])).unwrap();
    // dim and count (bytes 8..16 of each header) at u32::MAX: their
    // product overflowed the expected-length arithmetic.
    idx[8..16].fill(0xff);
    offsets[8..16].fill(0xff);
    assert_eq!(parse_vector_index_files(&idx, &offsets), (None, None));
}

#[test]
fn multivec_bag_header_whose_size_overflows_is_corrupt() {
    // Header words: magic, version, dim, doc count, token count.
    let bags = kosha_segment::multivec::bags_file(0);
    let dir = damaged_copy("bags-overflow", &bags, |b| {
        b[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        b[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
    });
    assert!(is_corrupt(open_multivec_files(&dir, 0, u32::MAX, 24)));
    std::fs::remove_dir_all(&dir).ok();
}
