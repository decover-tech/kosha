//! Shared fixtures for the fuzz targets (#257): a valid template segment
//! to splice fuzz input into, and a reader workout that touches every
//! decoder a search, delete or compaction would.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use kosha_core::{Bm25Params, DocumentId, Field, FieldType, FilterClause, SegmentId};
use kosha_segment::{SegmentReader, SegmentWriter};

pub const WORDS: [&str; 6] = ["alpha", "beta", "gamma", "delta", "kosha", "segment"];

/// One valid segment with every file kind a writer produces (text,
/// keyword/integer/float filters, a vector column, a late-interaction
/// bag field), built once per process, and its sorted file names.
pub fn template_segment() -> &'static (PathBuf, Vec<String>) {
    static TEMPLATE: OnceLock<(PathBuf, Vec<String>)> = OnceLock::new();
    TEMPLATE.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("kosha-fuzz-template-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut writer = SegmentWriter::new(SegmentId("fuzz".into()), dir.clone());
        for i in 0..16u32 {
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
                    Field::vector("emb", vec![f, 1.0 - f / 16.0, 0.5, f % 3.0]),
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

/// A private, writable copy of the template segment for this process.
pub fn work_segment() -> &'static PathBuf {
    static WORK: OnceLock<PathBuf> = OnceLock::new();
    WORK.get_or_init(|| {
        let (template, files) = template_segment();
        let dir = std::env::temp_dir().join(format!("kosha-fuzz-work-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for name in files {
            std::fs::copy(template.join(name), dir.join(name)).unwrap();
        }
        dir
    })
}

/// Read everything a search, a delete and a compaction would read.
/// Errors are expected; a panic is a finding.
pub fn exercise(dir: &Path) {
    let Ok(reader) = SegmentReader::open(dir.to_path_buf()) else {
        return;
    };
    let docs = reader.doc_count().min(256);
    for seq in 0..docs + 2 {
        let _ = reader.doc_meta(seq);
        let _ = reader.doc_record_full(seq);
    }
    for record in reader.iter_doc_records_raw().take(256) {
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
