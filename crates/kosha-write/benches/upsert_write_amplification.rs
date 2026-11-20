//! cargo bench -p kosha-write --bench upsert_write_amplification
//! KOSHA_BENCH_SEGMENT_MIB defaults to 500. The corpus consists of real
//! stored keyword payloads, not padding: a rewrite must copy those documents.
use kosha_core::{
    Bm25Params, Document, DocumentId, Field, Manifest, ManifestEntry, NamespaceId, SegmentId,
};
use kosha_segment::SegmentWriter;
use kosha_write::Indexer;
use std::{path::Path, time::Instant};
fn bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|e| {
            let p = e.unwrap().path();
            if p.is_dir() {
                bytes(&p)
            } else {
                p.metadata().unwrap().len()
            }
        })
        .sum()
}
fn main() {
    let mib: usize = std::env::var("KOSHA_BENCH_SEGMENT_MIB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);
    let dir = std::env::temp_dir().join(format!("kosha-upsert-bench-{}", uuid::Uuid::new_v4()));
    let ns = NamespaceId("bench".into());
    let segment = SegmentId("base".into());
    let segment_path = dir.join("bench/base");
    let mut writer = SegmentWriter::new(segment.clone(), segment_path.clone());
    // Keyword payload is stored in both doc_store and filters (~2 MiB/doc).
    let payload = "x".repeat(1024 * 1024);
    for n in 0..mib.div_ceil(2).max(2) {
        writer.add_document(
            DocumentId(format!("doc-{n}")),
            vec![Field::keyword("payload", &payload)],
        );
    }
    let footer = writer.finalize(Bm25Params::default()).unwrap();
    let mut manifest = Manifest {
        version: 1,
        segments: vec![ManifestEntry {
            segment_id: segment.clone(),
            doc_count: footer.doc_count,
        }],
        segment_footers: Default::default(),
        footer_crc32: Default::default(),
        wal_checkpoint: 0,
        wal_origin: None,
        tombstones: Default::default(),
        deletion_vectors: Default::default(),
        schema: None,
        retired: Default::default(),
    };
    manifest.remember_segment_footer(footer);
    let idx = Indexer::new(dir.clone());
    idx.restore_manifest(ns.clone(), manifest);
    // Build the per-ID metadata lookup once, as in a running ingest server.
    idx.existing_ids(&ns, &[DocumentId("doc-0".into())])
        .unwrap();
    let base_bytes = bytes(&segment_path);
    let before = bytes(&dir);
    let started = Instant::now();
    idx.index_documents(
        ns.clone(),
        vec![Document {
            id: DocumentId("doc-0".into()),
            fields: vec![Field::keyword("payload", "updated")],
        }],
    )
    .unwrap();
    let (_, published) = idx.prepare_publication(&ns).unwrap().unwrap();
    let elapsed = started.elapsed();
    let added = bytes(&dir) - before;
    assert_eq!(
        published.segments[0].segment_id, segment,
        "old segment must be retained"
    );
    assert_eq!(published.segments[1].doc_count, 1);
    assert!(
        added < 1024 * 1024,
        "one small upsert must not rewrite the corpus: {added} bytes"
    );
    println!(
        "{}",
        serde_json::json!({"segment_bytes": base_bytes, "upsert_added_bytes": added, "upsert_ms": elapsed.as_secs_f64() * 1000.0})
    );
    std::fs::remove_dir_all(dir).unwrap();
}
