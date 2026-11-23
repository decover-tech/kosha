//! A search pins only what it reads in the disk cache.
//!
//! All Decover matters share one namespace and a matter is a filter, so a
//! search that pinned every segment of its namespace (#414) stopped every
//! eviction there while it ran: under steady traffic the cache grew past
//! `KOSHA_CACHE_MAX_BYTES` without bound. These tests run real cold
//! searches against a fake bucket (see `doc_store_fallback_tests`) on a
//! query node whose cache cap is smaller than the namespace.

use super::doc_store_fallback_tests::{cold_node, temp_dir, FakeS3};
use super::*;
use kosha_core::{Document, DocumentId, Field};
use std::fs;

const NS: &str = "segment-pins";
/// One segment per matter; each matter's documents hold a word no other
/// matter's do, so a search for it in its matter prunes the other segments
/// by filter bloom and by term bloom (no namespace-`df` reads either).
const MATTERS: [(&str, &str); 4] = [
    ("m0", "alpha"),
    ("m1", "beta"),
    ("m2", "gamma"),
    ("m3", "delta"),
];
const DOCS_PER_MATTER: usize = 40;

/// The namespace, uploaded to a fresh fake bucket, and its manifest.
fn uploaded_matters() -> (FakeS3, kosha_core::Manifest) {
    let writer_dir = temp_dir("pins-writer");
    let state = AppState::new(writer_dir.clone());
    let ns = NamespaceId(NS.into());
    for (matter, word) in MATTERS {
        let docs = (0..DOCS_PER_MATTER)
            .map(|i| Document {
                id: DocumentId(format!("{word}-{i}")),
                fields: vec![
                    Field::text(
                        "text",
                        format!("{word} document number {i} of the {matter} matter"),
                    ),
                    Field::keyword("matter_id", matter),
                ],
            })
            .collect();
        state.indexer.index_documents(ns.clone(), docs).unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
    }
    let manifest = state.indexer.manifest_cloned(&ns).unwrap();
    assert_eq!(
        manifest.segments.len(),
        MATTERS.len(),
        "one segment per matter"
    );
    let bucket_root = temp_dir("pins-bucket");
    for entry in &manifest.segments {
        let src = writer_dir.join(NS).join(&entry.segment_id.0);
        let dst = bucket_root.join(NS).join(&entry.segment_id.0);
        fs::create_dir_all(&dst).unwrap();
        for file in fs::read_dir(&src).unwrap().flatten() {
            if file.path().is_file() {
                fs::copy(file.path(), dst.join(file.file_name())).unwrap();
            }
        }
    }
    (FakeS3::start(bucket_root), manifest)
}

/// A cold query node over the uploaded namespace, its disk cache capped at
/// `cap` bytes (`None`: unbounded).
fn node(s3: &FakeS3, manifest: &kosha_core::Manifest, cap: Option<u64>) -> AppState {
    let mut state = cold_node(s3, NS, manifest.clone());
    state.cache = Cache::with_max_bytes(state.data_dir.clone(), cap);
    state
}

/// Search `word`, filtered to `matter` when given; the status and hit ids.
fn search(state: &AppState, word: &str, matter: Option<&str>) -> (u16, Vec<String>) {
    let mut body = serde_json::json!({
        "namespace": NS,
        "query_text": word,
        "max_results": 500,
        "no_cache": true,
    });
    if let Some(matter) = matter {
        body["filter"] = serde_json::json!({"term": {"matter_id": matter}});
    }
    let response = handle_search_post(body.to_string().as_bytes(), state);
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(body).unwrap_or_default();
    let mut ids: Vec<String> = body["results"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(|h| h["doc_id"].as_str().map(str::to_string))
        .collect();
    ids.sort();
    (status, ids)
}

fn expected(word: &str) -> Vec<String> {
    let mut ids: Vec<String> = (0..DOCS_PER_MATTER)
        .map(|i| format!("{word}-{i}"))
        .collect();
    ids.sort();
    ids
}

/// Bytes cached under segment `i`'s directory.
fn segment_bytes(state: &AppState, manifest: &kosha_core::Manifest, i: usize) -> u64 {
    fn walk(dir: &Path) -> u64 {
        fs::read_dir(dir).map_or(0, |entries| {
            entries
                .flatten()
                .map(|e| {
                    let path = e.path();
                    if path.is_dir() {
                        walk(&path)
                    } else {
                        e.metadata().map_or(0, |m| m.len())
                    }
                })
                .sum()
        })
    }
    walk(
        &state
            .data_dir
            .join(NS)
            .join(&manifest.segments[i].segment_id.0),
    )
}

/// What one filtered search of each matter adds to an unbounded cold cache,
/// in `MATTERS` order (searching m1, m2, m3, then m0).
fn bytes_per_matter(s3: &FakeS3, manifest: &kosha_core::Manifest) -> Vec<u64> {
    let state = node(s3, manifest, None);
    let mut added = vec![0u64; MATTERS.len()];
    for i in [1, 2, 3, 0] {
        let (matter, word) = MATTERS[i];
        let before = state.cache.total_size();
        let (status, ids) = search(&state, word, Some(matter));
        assert_eq!(status, 200);
        assert_eq!(ids, expected(word));
        added[i] = state.cache.total_size() - before;
        assert!(added[i] > 0, "a cold search fetches its segment");
    }
    added
}

/// While a search that prunes to one segment is in flight, the other
/// segments' files stay evictable: its own fetches push the cache over its
/// cap, and the cache evicts them to get back under it. With every segment
/// of the namespace pinned for the request (#414), nothing could be
/// evicted, and the cache ended the search over its cap by the whole
/// fetch.
#[test]
fn a_pruned_search_leaves_other_segments_evictable() {
    let (s3, manifest) = uploaded_matters();
    let added = bytes_per_matter(&s3, &manifest);
    // Room for matters 1-3 and half of matter 0's fetch: searching matter 0
    // last has to evict at least the other half from matters 1-3.
    let cap = added[1] + added[2] + added[3] + added[0] / 2;
    let state = node(&s3, &manifest, Some(cap));
    for i in [1, 2, 3] {
        let (matter, word) = MATTERS[i];
        assert_eq!(search(&state, word, Some(matter)), (200, expected(word)));
    }
    let others_before: u64 = (1..4).map(|i| segment_bytes(&state, &manifest, i)).sum();
    assert!(
        state.cache.total_size() <= cap,
        "matters 1-3 fit under the cap"
    );

    let (matter, word) = MATTERS[0];
    assert_eq!(search(&state, word, Some(matter)), (200, expected(word)));

    let others_after: u64 = (1..4).map(|i| segment_bytes(&state, &manifest, i)).sum();
    assert!(
        state.cache.total_size() <= cap,
        "the search's own fetches evicted unpinned files of the segments it pruned: \
         size {} cap {cap}",
        state.cache.total_size()
    );
    assert!(
        others_after < others_before,
        "files of the pruned segments were evicted while the search ran \
         ({others_before} -> {others_after} bytes)"
    );
}

/// A filtered search on a cold node whose cap is smaller than the namespace
/// still answers every matter in full, and so does an unfiltered search:
/// whatever a search reads stays pinned while it runs, so its own fetches
/// cannot evict a file it has checked (the #414 failure: "segment hydration
/// failed" although the segment was in S3).
#[test]
fn searches_succeed_when_the_cache_is_smaller_than_the_namespace() {
    let (s3, manifest) = uploaded_matters();
    let added = bytes_per_matter(&s3, &manifest);
    let namespace: u64 = added.iter().sum();
    let cap = *added.iter().max().unwrap() + 1;
    assert!(cap < namespace);
    let state = node(&s3, &manifest, Some(cap));
    for round in 0..2 {
        for (matter, word) in MATTERS {
            assert_eq!(
                search(&state, word, Some(matter)),
                (200, expected(word)),
                "round {round}, matter {matter}"
            );
        }
    }
    // Reads every segment: held over the cap while it runs, then evictable.
    let (status, ids) = search(&state, "document", None);
    assert_eq!(status, 200);
    assert_eq!(ids.len(), MATTERS.len() * DOCS_PER_MATTER);
    let (matter, word) = MATTERS[2];
    assert_eq!(search(&state, word, Some(matter)), (200, expected(word)));
}
