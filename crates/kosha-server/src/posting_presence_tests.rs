//! A posting presence miss on a segment whose shards are already whole
//! local files is settled by shard name, without opening `inverted.idx`.
//!
//! Run8 (staging): after every landed promotion wiped the presence cache,
//! each distinct keyword query re-read and decoded every segment's
//! `inverted.idx` (13 segments, ~450 ms) in `PostingSpanHydrator::hydrate`
//! only to find every shard local. These tests run `ensure_query_postings`
//! on a query node over the fake bucket of `doc_store_fallback_tests`.

use super::doc_store_fallback_tests::{cold_node, uploaded_namespace, FakeS3, NS};
use super::*;
use std::fs;

/// A query node whose first segment (the `alpha` documents) is wholly
/// local, as warm-up or promotion leaves it, with its `inverted.idx` cut in
/// half: the format probe still sees a split segment, but any read of the
/// term table fails, so a plan that opens it reports the segment missing.
fn node_with_local_shards() -> (FakeS3, kosha_core::Manifest, AppState, PathBuf) {
    let (s3, manifest) = uploaded_namespace();
    let state = cold_node(&s3, NS, manifest.clone());
    let rel = format!("{NS}/{}", manifest.segments[0].segment_id.0);
    let seg = state.data_dir.join(&rel);
    fs::create_dir_all(&seg).unwrap();
    for file in fs::read_dir(s3.root.join(&rel)).unwrap().flatten() {
        fs::copy(file.path(), seg.join(file.file_name())).unwrap();
    }
    let toc = fs::read(seg.join("inverted.idx")).unwrap();
    fs::write(seg.join("inverted.idx"), &toc[..toc.len() / 2]).unwrap();
    assert!(
        kosha_segment::inverted_uses_posting_blobs(&seg),
        "still probed as a split segment with posting shards"
    );
    assert!(
        kosha_segment::posting_term_spans(&seg, &["alpha".to_string()]).is_err(),
        "the term table is unreadable"
    );
    (s3, manifest, state, seg)
}

fn ensure(state: &AppState, manifest: &kosha_core::Manifest, seg: &Path) -> HydrationOutcome {
    let terms = vec!["alpha".to_string()];
    let files = vec![kosha_segment::posting_blob_file_for_term("alpha")];
    state.ensure_query_postings(
        &NamespaceId(NS.into()),
        manifest,
        &[seg.to_path_buf()],
        &terms,
        &files,
        &Arc::new(RequestHydration::new(u64::MAX)),
    )
}

#[test]
fn a_presence_miss_with_local_shards_does_not_read_the_term_table() {
    let (s3, manifest, state, seg) = node_with_local_shards();
    let shard = kosha_segment::posting_blob_file_for_term("alpha");
    assert!(seg.join(&shard).is_file(), "the shard is whole and local");

    let outcome = ensure(&state, &manifest, &seg);
    assert!(
        outcome.missing.is_empty(),
        "planned from inverted.idx instead of the local shard: {:?}",
        outcome.missing
    );
    assert_eq!(outcome.rounds, 0);
    assert_eq!(outcome.bytes_fetched, 0);
    assert!(s3.gets.lock().unwrap().is_empty(), "nothing fetched");

    // Confirmed by shard name, so the next query hits the presence cache.
    let rel = seg
        .strip_prefix(&state.data_dir)
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let cache = state.posting_blob_presence.lock().unwrap();
    let confirmed =
        posting_blob_cache_lookup(&cache, NS, manifest.version, state.presence_generation());
    assert!(confirmed.get(&rel).is_some_and(|set| set.contains(&shard)));
}

/// The shortcut needs every shard whole: a segment missing one still plans
/// from its term table (here unreadable, so the plan reports it).
#[test]
fn a_missing_shard_still_plans_from_the_term_table() {
    let (_s3, manifest, state, seg) = node_with_local_shards();
    fs::remove_file(seg.join(kosha_segment::posting_blob_file_for_term("alpha"))).unwrap();

    let outcome = ensure(&state, &manifest, &seg);
    assert!(
        outcome.missing.iter().any(|m| m.ends_with("/inverted.idx")),
        "{:?}",
        outcome.missing
    );
}

/// After a failed span write the presence generation is no longer the
/// cache's eviction generation. Every presence insert must stamp the same
/// one: a shards-local confirmation and a ranged one in the same namespace
/// then keep each other (both hit), instead of each insert wiping the other
/// and every following query re-planning from `inverted.idx` (~450 ms).
#[test]
fn shards_local_and_ranged_presence_agree_after_a_failed_span_write() {
    let (_s3, manifest, state, seg) = node_with_local_shards();
    let gone = state.data_dir.join(NS).join("gone-segment");
    assert!(state
        .span_writes
        .persist(
            &gone,
            &format!("{NS}/gone-segment"),
            "postings-00.bin",
            0,
            vec![1; 8]
        )
        .is_none());
    state.span_writes.drain(&state.cache);
    assert_eq!(state.span_writes.failures(), 1, "the injected write failed");
    assert_ne!(
        state.presence_generation(),
        state.cache.eviction_generation()
    );

    // A ranged confirmation of another segment, as `ensure_query_postings`
    // stamps it, then a shards-local one of this segment.
    let other = (format!("{NS}/other"), "postings-07.bin".to_string());
    posting_blob_cache_insert(
        &mut state.posting_blob_presence.lock().unwrap(),
        NS,
        manifest.version,
        state.presence_generation(),
        vec![other.clone()],
    );
    let outcome = ensure(&state, &manifest, &seg);
    assert!(outcome.missing.is_empty(), "{:?}", outcome.missing);

    let rel = seg
        .strip_prefix(&state.data_dir)
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let shard = kosha_segment::posting_blob_file_for_term("alpha");
    let cache = state.posting_blob_presence.lock().unwrap();
    let hits = posting_blob_cache_lookup(&cache, NS, manifest.version, state.presence_generation());
    assert!(
        hits.get(&rel).is_some_and(|set| set.contains(&shard)),
        "shards-local kept"
    );
    assert!(
        hits.get(&other.0).is_some_and(|set| set.contains(&other.1)),
        "the ranged confirmation was not wiped"
    );
}

/// #197: the first lookup of a segment misses the presence cache, the
/// repeat hits it, both counted for the namespace's tenant and exported as
/// `kosha_cache_{hits,misses}_total{cache="posting_blob_presence"}`.
#[test]
fn presence_lookups_are_counted_for_metrics() {
    let (_s3, manifest, state, seg) = node_with_local_shards();
    let presence = |state: &AppState| {
        cache_tenant_samples(state)
            .into_iter()
            .filter(|s| s.cache == "posting_blob_presence")
            .map(|s| (s.stats.hits, s.stats.misses))
            .fold((0, 0), |(h, m), (sh, sm)| (h + sh, m + sm))
    };
    assert_eq!(presence(&state), (0, 0));

    assert!(ensure(&state, &manifest, &seg).missing.is_empty());
    assert_eq!(presence(&state), (0, 1), "first lookup misses");
    assert!(ensure(&state, &manifest, &seg).missing.is_empty());
    assert_eq!(presence(&state), (1, 1), "the repeat hits");

    let samples = metrics::tests::parse_exposition(&state.metrics.render(&metrics::Scrape {
        cache_tenants: cache_tenant_samples(&state),
        ..Default::default()
    }))
    .unwrap();
    let presence = [("cache", "posting_blob_presence")];
    assert_eq!(
        metrics::tests::value(&samples, "kosha_cache_hits_total", &presence),
        Some(1.0)
    );
    assert_eq!(
        metrics::tests::value(&samples, "kosha_cache_misses_total", &presence),
        Some(1.0)
    );
}
