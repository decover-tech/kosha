//! Compaction rebuilds late-interaction files (#205) from the bags kept in
//! the doc store.

use kosha_core::{Document, DocumentId, Field, FieldType, NamespaceId};
use kosha_segment::SegmentReader;
use kosha_write::{CompactOptions, Indexer};

#[test]
fn compaction_rebuilds_multivec_files() {
    let dir = std::env::temp_dir().join(format!("kosha-li-compact-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let idx = Indexer::new(dir.clone());
    let ns = NamespaceId("li".into());
    let bag = |i: usize| -> Vec<Vec<f32>> {
        vec![
            vec![1.0, i as f32 * 0.1, 0.0],
            vec![0.0, 1.0, i as f32 * 0.2],
        ]
    };
    for i in 0..3 {
        idx.index_documents(
            ns.clone(),
            vec![Document {
                id: DocumentId(format!("d{i}")),
                fields: vec![
                    Field::text("text", format!("doc {i}")),
                    Field {
                        name: "tokens".into(),
                        field_type: FieldType::Vector,
                        value: serde_json::to_string(&bag(i)).unwrap(),
                    },
                ],
            }],
        )
        .unwrap();
        idx.flush_namespace(&ns).unwrap();
    }
    assert_eq!(idx.manifest(&ns).unwrap().segments.len(), 3);
    let result = idx
        .compact_namespace_with_options(&ns, CompactOptions::full())
        .unwrap();
    assert!(result.merged);
    let after = idx.manifest(&ns).unwrap();
    assert_eq!(after.segments.len(), 1);
    let seg_dir = dir.join(&ns.0).join(&after.segments[0].segment_id.0);
    let reader = SegmentReader::open(seg_dir).unwrap();
    let meta = &reader.footer().multi_vectors;
    assert_eq!(meta.len(), 1);
    assert_eq!((meta[0].docs, meta[0].tokens), (3, 6));
    let q = bag(2);
    let hits = reader
        .late_interaction_search(
            "tokens",
            &[q[0].as_slice(), q[1].as_slice()],
            1,
            kosha_segment::LateInteractionParams {
                nprobe: 16,
                num_candidates: 10,
            },
            None,
        )
        .unwrap();
    let top = reader.doc_meta(hits.hits[0].0).unwrap();
    assert_eq!(top.doc_id, "d2");
    let _ = std::fs::remove_dir_all(&dir);
}
