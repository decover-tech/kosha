//! Pinned regressions for two closed durability bugs (#257).
//!
//! - #233 (fixed in 0b21da7, "group commit, per-record LSNs ..."): a flush
//!   used to wipe the whole WAL, including other namespaces' unflushed
//!   records, and WAL files had no fsync or checksum. Now reclamation is per
//!   namespace and per LSN, and every v3 frame's CRC covers its LSN and
//!   payload.
//! - #238 (fixed in f38f6a7, PR #263): `delete_by_query` only looked at
//!   flushed segments, so a matching buffered document survived the delete
//!   and became searchable after the next flush. Now the delete flushes the
//!   buffer first and WAL-logs the deleted ids.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use kosha_core::{Document, DocumentId, Field, FilterClause, NamespaceId};
use kosha_segment::SegmentReader;
use kosha_write::wal::WalWriter;
use kosha_write::Indexer;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "kosha-regression-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn doc(id: &str, grp: &str) -> Document {
    Document {
        id: DocumentId(id.into()),
        fields: vec![
            Field::text("body", format!("document {id}")),
            Field::keyword("grp", grp),
        ],
    }
}

/// Publish `ns` the way the server does after a successful compare-and-
/// swap: this is what reclaims WAL records.
fn publish(idx: &Indexer, ns: &NamespaceId) {
    if let Some((_, manifest)) = idx.prepare_publication(ns).unwrap() {
        idx.acknowledge_manifest(ns, &manifest).unwrap();
    }
}

/// Live document ids in flushed segments, with how often each is live.
fn live_ids(idx: &Indexer, dir: &Path, ns: &NamespaceId) -> BTreeMap<String, usize> {
    let mut live = BTreeMap::new();
    let Some(manifest) = idx.manifest_cloned(ns) else {
        return live;
    };
    let tombstones = idx.get_tombstones(ns).unwrap_or_default();
    for entry in &manifest.segments {
        let reader = SegmentReader::open(manifest.segment_dir(dir, ns, &entry.segment_id)).unwrap();
        let dead = tombstones.get(&entry.segment_id);
        for seq in 0..reader.doc_count() {
            if dead.is_some_and(|d| d.contains(&seq)) {
                continue;
            }
            let id = reader.doc_meta(seq).unwrap().doc_id.to_owned();
            *live.entry(id).or_insert(0) += 1;
        }
    }
    live
}

fn wal_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir.join("_wal"))
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .collect();
    files.sort();
    files
}

// ─── #233 ────────────────────────────────────────────────────────────────

#[test]
fn issue_233_flushing_one_namespace_keeps_another_namespaces_wal_records() {
    let dir = temp_dir("233-isolation");
    let (a, b) = (NamespaceId("t/a".into()), NamespaceId("t/b".into()));
    {
        let idx = Indexer::new(dir.clone());
        idx.index_documents(a.clone(), vec![doc("a1", "x"), doc("a2", "x")])
            .unwrap();
        idx.index_documents(b.clone(), vec![doc("b1", "x")])
            .unwrap();
        idx.index_documents(a.clone(), vec![doc("a3", "x")])
            .unwrap();
        // Flush and publish only `b`: its checkpoint reclaims WAL, but
        // must not take `a`'s records with it.
        idx.flush_namespace(&b).unwrap();
        publish(&idx, &b);
        assert!(!wal_files(&dir).is_empty(), "a's records were reclaimed");
        // Process death: no shutdown flush.
        std::mem::forget(idx);
    }
    let recovered = WalWriter::recover(&dir.join("_wal")).unwrap();
    let ids: HashSet<&str> = recovered
        .iter()
        .filter(|r| r.namespace == a)
        .flat_map(|r| r.documents.iter().map(|d| d.id.0.as_str()))
        .collect();
    assert_eq!(ids, HashSet::from(["a1", "a2", "a3"]));

    let idx = Indexer::new(dir.clone());
    idx.flush_namespace(&a).unwrap();
    let live = live_ids(&idx, &dir, &a);
    assert_eq!(
        live.keys().map(String::as_str).collect::<Vec<_>>(),
        ["a1", "a2", "a3"]
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn issue_233_wal_checksum_covers_payload_and_lsn() {
    let dir = temp_dir("233-checksum");
    let ns = NamespaceId("t/crc".into());
    {
        let idx = Indexer::new(dir.clone());
        idx.index_documents(ns.clone(), vec![doc("d1", "x")])
            .unwrap();
        std::mem::forget(idx);
    }
    let file = wal_files(&dir).pop().expect("one WAL file");
    let pristine = std::fs::read(&file).unwrap();
    assert_eq!(WalWriter::recover(&dir.join("_wal")).unwrap().len(), 1);

    // v3 frame header: magic(4) version(4) lsn(8) len(8) crc(4), payload.
    const LSN_AT: usize = 8;
    const PAYLOAD_AT: usize = 28;
    for (at, what) in [(LSN_AT, "LSN"), (PAYLOAD_AT + 6, "payload")] {
        let mut corrupt = pristine.clone();
        corrupt[at] ^= 0x01;
        std::fs::write(&file, &corrupt).unwrap();
        let error = WalWriter::recover(&dir.join("_wal"))
            .expect_err(&format!("a flipped {what} bit must fail recovery"));
        assert!(
            error.to_string().contains("checksum"),
            "{what}: unexpected error {error}"
        );
    }
    // A truncated (torn) tail is not corruption: it is dropped.
    std::fs::write(&file, &pristine[..pristine.len() - 3]).unwrap();
    assert!(WalWriter::recover(&dir.join("_wal")).unwrap().is_empty());
    std::fs::remove_dir_all(&dir).ok();
}

// ─── #238 ────────────────────────────────────────────────────────────────

fn drop_filter() -> FilterClause {
    serde_json::from_str(r#"{"term": {"grp": "drop"}}"#).unwrap()
}

#[test]
fn issue_238_delete_by_query_removes_buffered_documents() {
    let dir = temp_dir("238-buffered");
    let ns = NamespaceId("t/del".into());
    let idx = Indexer::new(dir.clone());
    idx.index_documents(ns.clone(), vec![doc("flushed", "drop"), doc("k1", "keep")])
        .unwrap();
    idx.flush_namespace(&ns).unwrap();
    // Buffered, never flushed: what #238 left behind.
    idx.index_documents(ns.clone(), vec![doc("buffered", "drop"), doc("k2", "keep")])
        .unwrap();
    assert_eq!(idx.pending_doc_count(&ns), 2);

    let manifest = idx.manifest_cloned(&ns).unwrap();
    let deleted = idx.delete_by_query(&ns, &manifest, &drop_filter()).unwrap();
    assert_eq!(deleted, 2, "the count includes the buffered match");

    idx.flush_namespace(&ns).unwrap();
    let live = live_ids(&idx, &dir, &ns);
    assert_eq!(
        live.keys().map(String::as_str).collect::<Vec<_>>(),
        ["k1", "k2"],
        "a deleted buffered document reappeared after flush"
    );
    assert!(live.values().all(|&n| n == 1));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn issue_238_buffered_delete_survives_a_crash_before_flush() {
    let dir = temp_dir("238-crash");
    let ns = NamespaceId("t/del".into());
    {
        let idx = Indexer::new(dir.clone());
        idx.index_documents(ns.clone(), vec![doc("buffered", "drop"), doc("k", "keep")])
            .unwrap();
        let manifest = idx.manifest_cloned(&ns).unwrap();
        assert_eq!(
            idx.delete_by_query(&ns, &manifest, &drop_filter()).unwrap(),
            1
        );
        // Crash between the delete and any publish.
        std::mem::forget(idx);
    }
    let idx = Indexer::new(dir.clone());
    idx.flush_namespace(&ns).unwrap();
    let live = live_ids(&idx, &dir, &ns);
    assert_eq!(live.keys().map(String::as_str).collect::<Vec<_>>(), ["k"]);
    std::fs::remove_dir_all(&dir).ok();
}
