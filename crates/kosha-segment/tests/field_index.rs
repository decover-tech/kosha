//! Per-field inverted indexes (#249): what the writer puts on disk, and
//! that the combined index stays the one every pre-#249 query reads.

use std::path::{Path, PathBuf};

use kosha_core::fields::{field_norms_key, field_term_key, DEFAULT_POSITION_GAP};
use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};
use kosha_segment::{SegmentReader, SegmentWriter};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "kosha-field-index-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn docs() -> Vec<(&'static str, Vec<Field>)> {
    vec![
        (
            "d1",
            vec![
                Field::text("title", "Rust async"),
                Field::text("content", "runtime internals explained"),
                Field::keyword("kind", "a"),
            ],
        ),
        (
            "d2",
            vec![
                Field::text("content", "async rust in practice"),
                Field::text("tags", "rust"),
                Field::text("tags", "web"),
            ],
        ),
        ("d3", vec![Field::integer("n", 3)]),
    ]
}

fn write(dir: &Path, field_index: bool) -> kosha_core::Footer {
    let mut writer = SegmentWriter::new(SegmentId("seg".into()), dir.to_path_buf())
        .with_field_index(field_index);
    for (id, fields) in docs() {
        writer.add_document(DocumentId(id.into()), fields);
    }
    writer.finalize(Bm25Params::default()).unwrap()
}

#[test]
fn writer_records_per_field_postings_norms_and_stats() {
    let dir = temp_dir("on");
    let footer = write(&dir, true);
    let index = footer.field_index.as_ref().expect("field index recorded");
    assert_eq!(index.position_gap, DEFAULT_POSITION_GAP);
    assert_eq!(index.fields["title"].doc_count, 1);
    assert_eq!(index.fields["title"].total_length, 2);
    assert_eq!(index.fields["content"].doc_count, 2);
    assert_eq!(index.fields["content"].total_length, 7);
    // A repeated field adds up within the document.
    assert_eq!(index.fields["tags"].doc_count, 1);
    assert_eq!(index.fields["tags"].total_length, 2);
    // Combined statistics are unchanged by the field index.
    assert_eq!(footer.total_field_length, 11);

    let reader = SegmentReader::open(dir.clone()).unwrap();
    let title_rust = reader.postings(&field_term_key("title", "rust")).unwrap();
    assert_eq!(
        title_rust.iter().map(|p| p.doc_id).collect::<Vec<_>>(),
        vec![0]
    );
    let content_rust = reader.postings(&field_term_key("content", "rust")).unwrap();
    assert_eq!(
        content_rust.iter().map(|p| p.doc_id).collect::<Vec<_>>(),
        vec![1]
    );
    // The combined index still holds every field's terms.
    let all_rust = reader.postings("rust").unwrap();
    assert_eq!(
        all_rust.iter().map(|p| p.doc_id).collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert!(reader
        .postings(&field_term_key("title", "runtime"))
        .is_none());

    // Norms: one posting per document with the field, tf = its length.
    let norms = reader.postings(&field_norms_key("content")).unwrap();
    assert_eq!(
        norms
            .iter()
            .map(|p| (p.doc_id, p.term_frequency))
            .collect::<Vec<_>>(),
        vec![(0, 3), (1, 4)]
    );
    assert_eq!(
        *reader.field_lengths("title").unwrap().unwrap(),
        vec![2, 0, 0]
    );
    assert_eq!(
        *reader.field_lengths("absent").unwrap().unwrap(),
        vec![0, 0, 0]
    );

    // Field keys are neither combined-index terms nor in the term bloom.
    assert!(reader.all_terms().iter().all(|t| !t.starts_with('\0')));
    let bloom = footer.term_bloom.as_ref().unwrap();
    assert!(bloom.may_contain("rust"));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn consecutive_text_values_are_a_position_gap_apart() {
    let dir = temp_dir("gap");
    write(&dir, true);
    let reader = SegmentReader::open(dir.clone()).unwrap();
    // d1: title "rust async" at 0, 1; content starts past the gap.
    let async_d1 = reader.postings("async").unwrap();
    assert_eq!(async_d1[0].positions, vec![1]);
    let runtime = reader.postings("runtime").unwrap();
    assert_eq!(runtime[0].positions, vec![2 + DEFAULT_POSITION_GAP]);
    // The field postings carry the same positions as the combined index.
    let content_runtime = reader
        .postings(&field_term_key("content", "runtime"))
        .unwrap();
    assert_eq!(content_runtime[0].positions, runtime[0].positions);
    // Two values of one field are separated the same way.
    let web = reader.postings(&field_term_key("tags", "web")).unwrap();
    let tags_rust = reader.postings(&field_term_key("tags", "rust")).unwrap();
    assert!(web[0].positions[0] - tags_rust[0].positions[0] > DEFAULT_POSITION_GAP);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn field_index_off_writes_the_combined_index_only() {
    let dir = temp_dir("off");
    let footer = write(&dir, false);
    assert!(footer.field_index.is_none());
    let reader = SegmentReader::open(dir.clone()).unwrap();
    assert!(reader.field_index().is_none());
    assert!(reader.field_lengths("title").unwrap().is_none());
    assert!(reader.postings(&field_term_key("title", "rust")).is_none());
    // Positions restart at 0 in every field, as before #249.
    assert_eq!(reader.postings("runtime").unwrap()[0].positions, vec![0]);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn combined_index_terms_and_doc_sets_match_with_and_without_field_index() {
    let on = temp_dir("cmp-on");
    let off = temp_dir("cmp-off");
    let footer_on = write(&on, true);
    let footer_off = write(&off, false);
    assert_eq!(footer_on.term_bloom, footer_off.term_bloom);
    assert_eq!(footer_on.total_field_length, footer_off.total_field_length);
    let (on_reader, off_reader) = (
        SegmentReader::open(on.clone()).unwrap(),
        SegmentReader::open(off.clone()).unwrap(),
    );
    assert_eq!(on_reader.all_terms(), off_reader.all_terms());
    for term in off_reader.all_terms() {
        let a = on_reader.postings(term).unwrap();
        let b = off_reader.postings(term).unwrap();
        let summary = |p: &[kosha_core::Posting]| {
            p.iter()
                .map(|p| (p.doc_id, p.term_frequency))
                .collect::<Vec<_>>()
        };
        assert_eq!(summary(&a), summary(&b), "{term}");
    }
    std::fs::remove_dir_all(&on).ok();
    std::fs::remove_dir_all(&off).ok();
}
