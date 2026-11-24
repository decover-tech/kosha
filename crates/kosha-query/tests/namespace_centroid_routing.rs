use std::collections::HashSet;
use std::path::Path;

use kosha_core::{Document, DocumentId, Field, KnnQuery, Manifest, NamespaceId, SearchQuery};
use kosha_query::Searcher;
use kosha_segment::namespace_centroids::{Directory, Snapshot, FILE_NAME};
use kosha_segment::SegmentReader;
use kosha_write::{CompactOptions, Indexer};

fn append(idx: &Indexer, ns: &NamespaceId, range: std::ops::Range<usize>) -> Vec<Document> {
    let docs: Vec<_> = range
        .map(|i| {
            let sign = if i / 20 < 10 { 1.0 } else { -1.0 };
            let offset = (i % 20) as f32 * 0.001 + (i / 20 % 10) as f32 * 0.02;
            Document {
                id: DocumentId(format!("doc-{i:05}")),
                fields: vec![Field::vector("emb", vec![sign, offset, 0.0, 0.0])],
            }
        })
        .collect();
    idx.index_documents(ns.clone(), docs.clone()).unwrap();
    idx.flush_namespace(ns).unwrap();
    docs
}

fn commit(indexer: &Indexer, ns: &NamespaceId) -> Manifest {
    let (_, manifest) = indexer.prepare_publication(ns).unwrap().unwrap();
    indexer.acknowledge_manifest(ns, &manifest).unwrap();
    indexer
        .update_namespace_centroids(ns, &manifest, None, |_| {
            panic!("routing opened metadata outside the locally flushed segment set")
        })
        .unwrap();
    manifest
}

fn snapshot(root: &Path, ns: &NamespaceId) -> Snapshot {
    serde_json::from_slice(&std::fs::read(root.join(&ns.0).join(FILE_NAME)).unwrap()).unwrap()
}

fn rebuild(root: &Path, ns: &NamespaceId, manifest: &Manifest) -> Directory {
    let mut directory = Directory::default();
    for entry in &manifest.segments {
        directory.insert_segment(
            &entry.segment_id.0,
            SegmentReader::namespace_centroids(&manifest.segment_dir(root, ns, &entry.segment_id))
                .unwrap(),
        );
    }
    directory
}

#[test]
fn flush_and_compaction_maintain_the_same_directory_as_rebuilding() {
    let root = tempfile::tempdir().unwrap();
    let ns = NamespaceId("incremental".into());
    let idx = Indexer::new(root.path().into()).with_wal(false);
    append(&idx, &ns, 0..20);
    let first = commit(&idx, &ns);
    let retained = first.segment_dir(root.path(), &ns, &first.segments[0].segment_id);
    let offsets = retained.join("vector.offsets");
    let hidden = retained.join("hidden-offsets");
    std::fs::rename(&offsets, &hidden).unwrap();
    append(&idx, &ns, 20..40);
    let second = commit(&idx, &ns); // the retained sidecar cannot be read here
    std::fs::rename(&hidden, &offsets).unwrap();
    assert_eq!(
        snapshot(root.path(), &ns).directory,
        rebuild(root.path(), &ns, &second)
    );

    assert!(
        idx.compact_namespace_with_options(&ns, CompactOptions::full())
            .unwrap()
            .merged
    );
    let compacted = commit(&idx, &ns);
    let checkpoint = snapshot(root.path(), &ns);
    assert_eq!(checkpoint.version, compacted.version);
    assert_eq!(compacted.segments.len(), 1);
    assert_eq!(checkpoint.directory, rebuild(root.path(), &ns, &compacted));
}

fn query(vector: Vec<f32>) -> SearchQuery {
    let mut query: SearchQuery = serde_json::from_value(serde_json::json!({
        "query_text": "",
        "max_results": 10
    }))
    .unwrap();
    query.knn = Some(KnnQuery {
        query_text: None,
        embedding_model: None,
        field: "emb".into(),
        vector,
        k: 10,
        num_candidates: 10,
        filter: None,
        scoring: None,
        vectors: None,
    });
    query
}

#[test]
fn namespace_lookup_keeps_probe_count_flat_and_recall_matches_the_old_plan() {
    let root = tempfile::tempdir().unwrap();
    let ns = NamespaceId("routing".into());
    let idx = Indexer::new(root.path().into()).with_wal(false);
    let mut live = Vec::new();
    for i in 0..10 {
        live.extend(append(&idx, &ns, (i * 20)..((i + 1) * 20)));
    }
    let small = commit(&idx, &ns);
    assert_eq!(small.segments.len(), 10);
    let routed = Searcher::new(root.path().into()).with_knn_namespace_centroids(true);
    let old = Searcher::new(root.path().into()).with_knn_namespace_centroids(false);
    let q = query(vec![1.0, 0.0713, 0.0, 0.0]);
    let (_, small_stats) = routed.search_with_stats(&ns, &small, &q, None).unwrap();
    assert_eq!(small_stats.knn_postings_probed, 10);
    assert_eq!(small_stats.knn_centroids_scanned, 10);

    for i in 10..1000 {
        live.extend(append(&idx, &ns, (i * 20)..((i + 1) * 20)));
    }
    let big = commit(&idx, &ns);
    assert_eq!(big.segments.len(), 1000);

    let mut total_routed = 0;
    let mut total_old = 0;
    for target in [0.0041f32, 0.0553, 0.0981, 0.1422, 0.1913] {
        let q = query(vec![1.0, target, 0.0, 0.0]);
        let (routed_result, stats) = routed.search_with_stats(&ns, &big, &q, None).unwrap();
        let (old_result, old_stats) = old.search_with_stats(&ns, &big, &q, None).unwrap();
        assert_eq!(stats.knn_postings_probed, small_stats.knn_postings_probed);
        assert_eq!(
            stats.knn_centroids_scanned,
            small_stats.knn_centroids_scanned
        );
        assert_eq!(old_stats.knn_centroids_scanned, 1000);
        let mut truth: Vec<_> = live
            .iter()
            .enumerate()
            .map(|(i, doc)| {
                let sign = if i / 20 < 10 { 1.0 } else { -1.0 };
                let offset = (i % 20) as f64 * 0.001 + (i / 20 % 10) as f64 * 0.02;
                let similarity = (sign + target as f64 * offset) / (1.0 + offset * offset).sqrt();
                (&doc.id, similarity)
            })
            .collect();
        truth.sort_by(|a, b| b.1.total_cmp(&a.1));
        let truth: HashSet<_> = truth[..10].iter().map(|(id, _)| *id).collect();
        total_routed += routed_result
            .results
            .iter()
            .filter(|hit| truth.contains(&hit.doc_id))
            .count();
        total_old += old_result
            .results
            .iter()
            .filter(|hit| truth.contains(&hit.doc_id))
            .count();
    }
    let (routed_recall, old_recall) = (total_routed as f64 / 50.0, total_old as f64 / 50.0);
    eprintln!("10→1000 segments: 10→10 probes; recall@10: routed {routed_recall:.4}, current {old_recall:.4}");
    assert!(old_recall >= 0.99);
    assert!((routed_recall - old_recall).abs() <= 0.005);

    let mut stale = big.clone();
    stale.version += 1;
    let (_, stats) = routed.search_with_stats(&ns, &stale, &q, None).unwrap();
    assert_eq!(stats.knn_centroids_scanned, 1000);
}
