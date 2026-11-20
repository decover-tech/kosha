//! Analyzer compatibility (issues #220, #221).
//!
//! `tests/fixtures/legacy-seg` was written by the segment writer as it was
//! before analyzers existed (commit 30d0ff0, `feat/rrf-hybrid`) from the
//! documents in [`fixture_docs`]. A segment written today with no analyzer
//! — or with an explicit `["standard"]` — and without per-field indexes
//! (`KOSHA_FIELD_INDEX=off`, #249) must be byte-identical to it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use kosha_core::{
    AnalysisConfig, AnalyzerSpec, Bm25Params, DocumentId, Field, FieldSchema, NamespaceSchema,
    SegmentId,
};
use kosha_segment::{SegmentReader, SegmentWriter};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-seg")
}

fn fixture_docs() -> Vec<(&'static str, Vec<Field>)> {
    vec![
        (
            "d1",
            vec![
                Field::text("body", "hello,world from the legacy tokenizer"),
                Field::keyword("kind", "a"),
            ],
        ),
        (
            "d2",
            vec![
                Field::text("body", "Running shoes are state of the art"),
                Field::text("title", "Shoes!"),
                Field::keyword("kind", "b"),
            ],
        ),
        (
            "d3",
            vec![
                Field::text("body", "naïve.café foo(bar) 日本語テキスト"),
                Field::integer("n", 3),
            ],
        ),
    ]
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "kosha-analyzers-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write(dir: &Path, config: Option<&AnalysisConfig>) -> kosha_core::Footer {
    // Per-field indexes (#249) add keys to the inverted index; with them
    // off a segment is exactly what the pre-analyzer writer produced.
    let mut writer = SegmentWriter::new(SegmentId("legacy-seg".into()), dir.to_path_buf())
        .with_text_filter_ignore_above(Some(256))
        .with_field_index(false);
    if let Some(config) = config {
        writer = writer.with_analysis(config);
    }
    for (id, fields) in fixture_docs() {
        writer.add_document(DocumentId(id.into()), fields);
    }
    writer.finalize(Bm25Params::default()).unwrap()
}

fn files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_file())
        .map(|path| {
            (
                path.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read(&path).unwrap(),
            )
        })
        .collect()
}

fn footer_without_timestamp(bytes: &[u8]) -> serde_json::Value {
    let mut value: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    value.as_object_mut().unwrap().remove("created_at");
    // `fuzzy_bloom` (#201) is optional footer metadata added after the
    // fixture was written; readers without it ignore the key and readers
    // with it treat its absence as "can't prune". Everything else must
    // match byte for byte.
    value.as_object_mut().unwrap().remove("fuzzy_bloom");
    // Likewise `posting_blobs_raw` (#245): it records that no posting
    // shard was zstd-wrapped, which readers before it never needed to know
    // (the wrapper was always optional); the shards themselves must still
    // match byte for byte below.
    value.as_object_mut().unwrap().remove("posting_blobs_raw");
    // Additive since #204: a segment without vectors says so explicitly
    // (`[]`), which no reader before #204 looks at.
    if value.get("vector_fields") == Some(&serde_json::json!([])) {
        value.as_object_mut().unwrap().remove("vector_fields");
    }
    value
}

fn assert_identical_to_fixture(dir: &Path) {
    let expected = files(&fixture_dir());
    let actual = files(dir);
    assert_eq!(
        expected.keys().collect::<Vec<_>>(),
        actual.keys().collect::<Vec<_>>(),
        "same set of segment files"
    );
    for (name, bytes) in &expected {
        if name == "footer.json" {
            // Only `created_at` may differ (and `filter_blooms` key order,
            // which is a `HashMap`): compare as JSON values.
            assert_eq!(
                footer_without_timestamp(bytes),
                footer_without_timestamp(&actual[name]),
                "footer.json"
            );
            assert!(
                !String::from_utf8_lossy(&actual[name]).contains("analysis"),
                "a default segment's footer has no `analysis` key"
            );
        } else {
            assert!(*bytes == actual[name], "{name} differs from the fixture");
        }
    }
}

#[test]
fn default_segment_is_byte_identical_to_pre_analyzer_segment() {
    let dir = temp_dir("default");
    let footer = write(&dir, None);
    assert!(footer.analysis.is_none());
    assert_identical_to_fixture(&dir);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn explicit_standard_schema_is_byte_identical_to_pre_analyzer_segment() {
    let schema = NamespaceSchema {
        fields: BTreeMap::from([
            (
                "body".to_string(),
                FieldSchema {
                    analyzer: vec!["standard".into()],
                    ..Default::default()
                },
            ),
            (
                "title".to_string(),
                FieldSchema {
                    analyzer: vec!["standard@1".into()],
                    ..Default::default()
                },
            ),
        ]),
        default_analyzer: vec!["standard".into()],
        default_stopwords: None,
    };
    let dir = temp_dir("standard");
    let footer = write(&dir, Some(&schema.resolve().unwrap()));
    assert!(footer.analysis.is_none());
    assert_identical_to_fixture(&dir);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn pre_analyzer_segment_reads_as_legacy() {
    let reader = SegmentReader::open(fixture_dir()).unwrap();
    assert!(reader.footer().analysis.is_none());
    // The legacy tokenizer's terms, interior punctuation and all.
    assert!(reader.contains_term("hello,world"));
    assert!(reader.contains_term("naïve.café"));
    assert!(!reader.contains_term("world"));
}

#[test]
fn non_default_chains_are_recorded_in_the_footer() {
    let schema = NamespaceSchema {
        fields: BTreeMap::from([(
            "title".to_string(),
            FieldSchema {
                analyzer: vec!["standard".into(), "stem".into()],
                ..Default::default()
            },
        )]),
        default_analyzer: vec!["standard@2".into()],
        default_stopwords: None,
    };
    let config = schema.resolve().unwrap();
    let dir = temp_dir("recorded");
    let footer = write(&dir, Some(&config));
    let analysis = footer.analysis.expect("non-default chains are recorded");
    assert_eq!(analysis.default, config.default);
    assert!(analysis.default_used);
    assert_eq!(analysis.fields.keys().collect::<Vec<_>>(), ["title"]);

    // Round-trips through footer.json.
    let reader = SegmentReader::open(dir.clone()).unwrap();
    assert_eq!(reader.footer().analysis.as_ref().unwrap(), &analysis);
    // Unicode tokenizer on `body`, stemmer on `title`.
    assert!(reader.contains_term("world"));
    assert!(reader.contains_term("café"));
    assert!(reader.contains_term("shoe"));
    assert!(!reader.contains_term("hello,world"));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn schema_fields_that_never_occur_are_not_recorded() {
    let config = AnalysisConfig {
        default: AnalyzerSpec::legacy(),
        fields: BTreeMap::from([(
            "absent".to_string(),
            AnalyzerSpec::resolve(&["keyword".to_string()], None).unwrap(),
        )]),
    };
    let dir = temp_dir("absent");
    let footer = write(&dir, Some(&config));
    assert!(
        footer.analysis.is_none(),
        "no text went through a non-legacy chain: still a legacy segment"
    );
    std::fs::remove_dir_all(&dir).ok();
}
