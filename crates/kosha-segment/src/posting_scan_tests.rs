//! The kNN posting scan (#435): one open idx file and one reused buffer
//! per probe, flat f32 postings, a running top-k.
//!
//! Every test compares against [`reference_within`], which is the probe as
//! it was before #435 — each posting decoded on its own into
//! `Vec<(u32, Vec<f32>)>`, every score pushed, a stable sort, a cut — fed
//! from the bytes of `vector.idx` read whole. Ids must match and scores
//! must be bit-identical.

use super::*;
use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

const DIM: usize = 24;
const DOCS: usize = 900;
const FIELD: &str = "emb";

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }

    fn coord(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 23) as f32 * 2.0 - 1.0
    }

    fn vector(&mut self) -> Vec<f32> {
        (0..DIM).map(|_| self.coord()).collect()
    }
}

/// `DOCS` vectors; every seventh repeats an earlier one, so equal scores
/// (and their order) are exercised.
fn corpus() -> Vec<Vec<f32>> {
    corpus_of(DOCS)
}

/// [`corpus`] of `docs` vectors (its first `DOCS` are [`corpus`]).
fn corpus_of(docs: usize) -> Vec<Vec<f32>> {
    let mut rng = Lcg(0x435);
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(docs);
    for i in 0..docs {
        if i % 7 == 6 {
            vectors.push(vectors[i / 2].clone());
        } else {
            vectors.push(rng.vector());
        }
    }
    vectors
}

fn queries() -> Vec<Vec<f32>> {
    let mut rng = Lcg(0xC0FFEE);
    let mut queries: Vec<Vec<f32>> = (0..4).map(|_| rng.vector()).collect();
    // A query equal to a stored (and repeated) vector: ties at the top.
    queries.push(corpus()[3].clone());
    queries
}

struct Fixture {
    dir: PathBuf,
    reader: SegmentReader,
    /// `vector.idx` as written.
    idx: Vec<u8>,
}

impl Fixture {
    fn new(name: &str) -> Self {
        Self::with_docs(name, DOCS)
    }

    /// A fixture over [`corpus_of`]`(docs)`.
    fn with_docs(name: &str, docs: usize) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kosha-posting-scan-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        let mut writer = SegmentWriter::new(SegmentId("scan".into()), dir.clone());
        for (i, v) in corpus_of(docs).into_iter().enumerate() {
            writer.add_document(DocumentId(format!("d{i}")), vec![Field::vector(FIELD, v)]);
        }
        writer.finalize(Bm25Params::default()).unwrap();
        // Force the postings fallback of exact filtered kNN.
        let _ = fs::remove_file(dir.join(DOC_VECTORS_FILE));
        let idx = fs::read(dir.join(VECTOR_IDX_FILE)).unwrap();
        let reader = SegmentReader::open_with_options(dir.clone(), true).unwrap();
        let fixture = Self { dir, reader, idx };
        assert!(
            fixture.index().lazy.as_ref().unwrap().ranges.len() >= 6,
            "the fixture needs several postings"
        );
        fixture
    }

    fn index(&self) -> Arc<VectorFieldIndex> {
        self.reader.vector_field(FIELD).unwrap().unwrap()
    }

    /// Give the reader a cache of its own with this budget (the shared
    /// one is process-wide and other tests use it concurrently).
    fn set_cache_budget(&mut self, max_bytes: usize) {
        self.reader.vector_cache = Arc::new(PostingsCache::new(max_bytes, None));
    }

    /// Move `vector.idx` into the span cache: `per_span` consecutive
    /// postings (in file order) to a span file.
    fn move_idx_to_spans(&self, per_span: usize) {
        let index = self.index();
        let vi = index.lazy.as_ref().unwrap();
        let mut ranges: Vec<(u64, u64)> = vi
            .ranges
            .iter()
            .map(|&(offset, length)| (offset, offset + length as u64))
            .collect();
        ranges.sort_unstable();
        let spans = span_cache::spans_dir(&self.dir, VECTOR_IDX_FILE);
        fs::create_dir_all(&spans).unwrap();
        for group in ranges.chunks(per_span) {
            let (start, end) = (group[0].0, group[group.len() - 1].1);
            fs::write(
                spans.join(span_cache::span_file_name(start, end)),
                &self.idx[start as usize..end as usize],
            )
            .unwrap();
        }
        fs::remove_file(self.dir.join(VECTOR_IDX_FILE)).unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// The f32 decode as it was before #435: one cursor read per float, one
/// `Vec` per vector.
fn reference_decode(mut cursor: &[u8], dim: usize) -> Vec<(u32, Vec<f32>)> {
    let count = cursor.len() / (4 + 4 * dim);
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let id = read_u32_le(&mut cursor);
        let mut v = Vec::with_capacity(dim);
        for _ in 0..dim {
            v.push(read_f32_le(&mut cursor));
        }
        out.push((id, v));
    }
    out
}

/// `(top k, postings probed)` of the budgeted probe as it was before #435,
/// scored from `idx` (the whole `vector.idx`).
#[allow(clippy::too_many_arguments)]
fn reference_within(
    vi: &LazyVectorIndex,
    idx: &[u8],
    query: &[f32],
    k: usize,
    max_distance: f32,
    max_probes: usize,
    accept: Option<&dyn Fn(u32) -> bool>,
    refill: KnnRefill,
) -> (Vec<(u32, f64)>, usize) {
    let mut ranked: Vec<(usize, f32)> = SegmentReader::centroid_lower_bounds(vi, query)
        .into_iter()
        .enumerate()
        .collect();
    ranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let budgeted = ranked
        .iter()
        .take_while(|(_, d)| *d <= max_distance)
        .count()
        .min(max_probes);
    let lanes = DotLanes::from_env();
    let unit = SegmentReader::unit_query(vi, query);
    let mut scored: Vec<(u32, f64)> = Vec::new();
    let mut probes = 0;
    for (position, &(range_idx, _)) in ranked.iter().enumerate() {
        if position >= budgeted
            && (accept.is_none()
                || scored.len() >= k
                || (refill == KnnRefill::BudgetedSegments && budgeted == 0))
        {
            break;
        }
        probes += 1;
        let (offset, length) = vi.ranges[range_idx];
        let bytes = &idx[offset as usize..offset as usize + length as usize];
        for (id, v) in reference_decode(bytes, vi.dim) {
            if accept.is_some_and(|accept| !accept(id)) {
                continue;
            }
            let sim = SegmentReader::f32_similarity(lanes, unit.as_deref(), query, &v);
            scored.push((id, sim as f64));
        }
    }
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(k);
    (scored, probes)
}

fn bits(hits: &[(u32, f64)]) -> Vec<(u32, u64)> {
    hits.iter().map(|&(id, s)| (id, s.to_bits())).collect()
}

/// Probe budgets worth checking: everything, a strict prefix, nothing.
const BUDGETS: [(f32, usize); 4] = [
    (f32::INFINITY, usize::MAX),
    (f32::INFINITY, 3),
    (0.9, usize::MAX),
    (f32::INFINITY, 0),
];

/// Every budget, `k` and query against the reference, unrestricted.
fn assert_matches_reference(fixture: &Fixture, context: &str) {
    let index = fixture.index();
    let vi = index.lazy.as_ref().unwrap();
    for (q, query) in queries().iter().enumerate() {
        for (max_distance, max_probes) in BUDGETS {
            for k in [0usize, 1, 10, 137, DOCS * 2] {
                let expect = reference_within(
                    vi,
                    &fixture.idx,
                    query,
                    k,
                    max_distance,
                    max_probes,
                    None,
                    KnnRefill::Always,
                );
                let got = fixture
                    .reader
                    .knn_search_within_filtered(FIELD, query, k, max_distance, max_probes, None)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    (bits(&got.0), got.1),
                    (bits(&expect.0), expect.1),
                    "{context}: query {q}, k {k}, budget ({max_distance}, {max_probes})"
                );
            }
        }
    }
}

#[test]
fn whole_file_source_matches_the_pre_435_probe() {
    let mut fixture = Fixture::new("whole");
    // Nothing is ever cached: every posting is read from the idx file and
    // scored from the probe's buffers.
    fixture.set_cache_budget(0);
    assert_matches_reference(&fixture, "whole file");
    let query = &queries()[0];
    let ((_, probes), stats) = with_knn_scan_stats(|| {
        fixture
            .reader
            .knn_search_within(query, 10, f32::INFINITY, usize::MAX)
            .unwrap()
            .unwrap()
    });
    assert!(probes >= 6);
    assert_eq!(stats.postings_scanned, probes);
    assert_eq!(stats.postings_decoded + stats.postings_cached, 0);
    assert_eq!(fixture.reader.vector_cache_resident_bytes(), 0);
}

#[test]
fn span_file_source_matches_and_follows_eviction_and_promotion() {
    for per_span in [1usize, 3, usize::MAX] {
        let mut fixture = Fixture::new("spans");
        fixture.set_cache_budget(0);
        // Load the column (centroids, ranges) while the idx is whole, as a
        // segment opened by a ranged cold query has it from the sidecar.
        let index = fixture.index();
        fixture.move_idx_to_spans(per_span);
        assert_matches_reference(&fixture, &format!("{per_span} posting(s) per span"));

        // A span evicted: its postings are an IO error, never zero matches.
        let spans = span_cache::spans_dir(&fixture.dir, VECTOR_IDX_FILE);
        let victim = fs::read_dir(&spans)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        fs::remove_file(&victim).unwrap();
        let query = &queries()[0];
        let probe = |max_probes| {
            fixture
                .reader
                .knn_search_within_filtered(FIELD, query, 10, f32::INFINITY, max_probes, None)
                .unwrap()
        };
        assert!(
            matches!(probe(usize::MAX), Err(KoshaError::Io(_))),
            "a posting with no local bytes must fail the probe"
        );

        // Promoted: the whole file lands and the spans go away.
        fs::write(fixture.dir.join(VECTOR_IDX_FILE), &fixture.idx).unwrap();
        fs::remove_dir_all(&spans).unwrap();
        assert_matches_reference(&fixture, "promoted to the whole file");
        drop(index);
    }
}

#[test]
fn spans_fetched_promoted_or_evicted_between_postings_of_one_probe() {
    // The span listing and the open handles are taken once per probe. A
    // probe whose spans change under it must still read every posting from
    // whichever source holds it by then.
    let query = &queries()[1];
    let all_postings = |fixture: &Fixture| fixture.index().lazy.as_ref().unwrap().ranges.len();
    let nothing = |_: u32| false;

    // Fetched mid-probe: the span cache starts empty and the ranged-read
    // callback writes what each refill chunk asks for, after the reader
    // took its listing. Rejecting every document keeps the probe going to
    // the last posting.
    let mut fixture = Fixture::new("midprobe-fetch");
    fixture.set_cache_budget(0);
    let postings = all_postings(&fixture);
    assert!(postings > VECTOR_PREFETCH_CHUNK.min(4));
    fs::remove_file(fixture.dir.join(VECTOR_IDX_FILE)).unwrap();
    let calls = std::sync::atomic::AtomicUsize::new(0);
    let fetch = |dir: &Path, file: &str, ranges: &[(u64, u64)]| {
        calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let spans = span_cache::spans_dir(dir, file);
        fs::create_dir_all(&spans).unwrap();
        for &(start, end) in ranges {
            fs::write(
                spans.join(span_cache::span_file_name(start, end)),
                &fixture.idx[start as usize..end as usize],
            )
            .unwrap();
        }
        span_cache::SpanHolds::new()
    };
    let (hits, probes) = fixture
        .reader
        .knn_search_within_filtered_fetching(
            FIELD,
            query,
            5,
            f32::INFINITY,
            1,
            Some(&nothing),
            KnnRefill::Always,
            Some(&fetch),
        )
        .unwrap()
        .unwrap();
    assert!(hits.is_empty());
    assert_eq!(probes, postings, "every posting was read");
    assert!(calls.load(std::sync::atomic::Ordering::Relaxed) >= 2);
    assert_matches_reference(&fixture, "spans fetched one posting at a time");

    // Promoted mid-probe: while the first posting is being scored, the
    // whole file lands and every span is dropped.
    let mut fixture = Fixture::new("midprobe-promote");
    fixture.set_cache_budget(0);
    let index = fixture.index();
    let vi = index.lazy.as_ref().unwrap();
    fixture.move_idx_to_spans(1);
    let spans = span_cache::spans_dir(&fixture.dir, VECTOR_IDX_FILE);
    let seen = std::sync::atomic::AtomicUsize::new(0);
    let promote_then_accept = |_: u32| {
        if seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
            fs::write(fixture.dir.join(VECTOR_IDX_FILE), &fixture.idx).unwrap();
            fs::remove_dir_all(&spans).unwrap();
        }
        true
    };
    let got = fixture
        .reader
        .knn_search_within_filtered(
            FIELD,
            query,
            10,
            f32::INFINITY,
            usize::MAX,
            Some(&promote_then_accept),
        )
        .unwrap()
        .unwrap();
    let all = |_: u32| true;
    let expect = reference_within(
        vi,
        &fixture.idx,
        query,
        10,
        f32::INFINITY,
        usize::MAX,
        Some(&all),
        KnnRefill::Always,
    );
    assert_eq!((bits(&got.0), got.1), (bits(&expect.0), expect.1));
    assert!(!spans.exists());

    // Evicted mid-probe, nothing promoted: an IO error, not a short page.
    let mut fixture = Fixture::new("midprobe-evict");
    fixture.set_cache_budget(0);
    fixture.index();
    fixture.move_idx_to_spans(1);
    let spans = span_cache::spans_dir(&fixture.dir, VECTOR_IDX_FILE);
    let seen = std::sync::atomic::AtomicUsize::new(0);
    let evict_then_accept = |_: u32| {
        if seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
            fs::remove_dir_all(&spans).unwrap();
        }
        true
    };
    let failed = fixture
        .reader
        .knn_search_within_filtered(
            FIELD,
            query,
            10,
            f32::INFINITY,
            usize::MAX,
            Some(&evict_then_accept),
        )
        .unwrap();
    assert!(matches!(failed, Err(KoshaError::Io(_))), "{failed:?}");
}

#[test]
fn first_touch_second_touch_and_cache_hit_score_identically() {
    let fixture = Fixture::new("touch");
    let query = &queries()[2];
    let index = fixture.index();
    let vi = index.lazy.as_ref().unwrap();
    let expect = reference_within(
        vi,
        &fixture.idx,
        query,
        10,
        f32::INFINITY,
        usize::MAX,
        None,
        KnnRefill::Always,
    );
    let run = || {
        with_knn_scan_stats(|| {
            fixture
                .reader
                .knn_search_within(query, 10, f32::INFINITY, usize::MAX)
                .unwrap()
                .unwrap()
        })
    };
    let postings = vi.ranges.len();
    // First touch: scored from the reused buffers, nothing cached.
    let (first, stats) = run();
    assert_eq!(bits(&first.0), bits(&expect.0));
    assert_eq!(
        (
            stats.postings_scanned,
            stats.postings_decoded,
            stats.postings_cached
        ),
        (postings, 0, 0)
    );
    assert_eq!(fixture.reader.vector_cache_resident_bytes(), 0);
    // Second touch: decoded into flat postings and admitted.
    let (second, stats) = run();
    assert_eq!(bits(&second.0), bits(&expect.0));
    assert_eq!(
        (
            stats.postings_scanned,
            stats.postings_decoded,
            stats.postings_cached
        ),
        (0, postings, 0)
    );
    let entries: usize = vi
        .ranges
        .iter()
        .map(|&(_, length)| length as usize / (4 + 4 * DIM))
        .sum();
    assert_eq!(
        fixture.reader.vector_cache_resident_bytes(),
        postings * std::mem::size_of::<VectorPosting>() + entries * (4 + 4 * DIM),
        "a flat posting costs its ids and floats, no per-vector overhead"
    );
    // Third: all from the cache, with the idx file gone.
    fs::remove_file(fixture.dir.join(VECTOR_IDX_FILE)).unwrap();
    let (third, stats) = run();
    assert_eq!(bits(&third.0), bits(&expect.0));
    assert_eq!(
        (
            stats.postings_scanned,
            stats.postings_decoded,
            stats.postings_cached
        ),
        (0, 0, postings)
    );
    assert_eq!(stats.read_nanos + stats.decode_nanos, 0);
}

#[test]
fn a_probe_straddling_the_cache_admit_boundary_scores_identically() {
    // A budget of about two and a half postings: of one probe's postings
    // some are cache hits, some are admitted (evicting others), and the
    // rest are scored from the buffers. Whatever the mix, same answer.
    let mut fixture = Fixture::new("boundary");
    let index = fixture.index();
    let vi = index.lazy.as_ref().unwrap();
    let largest = vi.ranges.iter().map(|&(_, length)| length).max().unwrap() as usize;
    let largest_resident = f32_posting_resident_bytes(largest / (4 + 4 * DIM), DIM);
    fixture.set_cache_budget(largest_resident * 5 / 2);
    let mut seen = KnnScanStats::default();
    for round in 0..4 {
        for (q, query) in queries().iter().enumerate() {
            for k in [1usize, 10, DOCS] {
                let expect = reference_within(
                    vi,
                    &fixture.idx,
                    query,
                    k,
                    f32::INFINITY,
                    usize::MAX,
                    None,
                    KnnRefill::Always,
                );
                let (got, stats) = with_knn_scan_stats(|| {
                    fixture
                        .reader
                        .knn_search_within(query, k, f32::INFINITY, usize::MAX)
                        .unwrap()
                        .unwrap()
                });
                assert_eq!(
                    bits(&got.0),
                    bits(&expect.0),
                    "round {round} query {q} k {k}"
                );
                assert_eq!(
                    stats.postings_scanned + stats.postings_decoded + stats.postings_cached,
                    vi.ranges.len()
                );
                seen.add(&stats);
                assert!(
                    fixture.reader.vector_cache_resident_bytes() <= largest_resident * 5 / 2,
                    "the cache stays inside its budget"
                );
            }
        }
    }
    assert!(seen.postings_scanned > 0, "{seen:?}");
    assert!(seen.postings_decoded > 0, "{seen:?}");
    assert!(seen.postings_cached > 0, "{seen:?}");

    // A budget smaller than any posting: a second touch is not decoded for
    // a cache that would refuse it.
    let smallest = vi.ranges.iter().map(|&(_, length)| length).min().unwrap() as usize;
    let mut fixture = Fixture::new("boundary-tiny");
    fixture.set_cache_budget(smallest / 2);
    let query = &queries()[0];
    for _ in 0..3 {
        let (_, stats) = with_knn_scan_stats(|| {
            fixture
                .reader
                .knn_search_within(query, 10, f32::INFINITY, usize::MAX)
                .unwrap()
                .unwrap()
        });
        assert_eq!(stats.postings_decoded + stats.postings_cached, 0);
        assert!(stats.postings_scanned > 0);
    }
    assert_eq!(fixture.reader.vector_cache_resident_bytes(), 0);
}

#[test]
fn filtered_and_tombstoned_probes_keep_their_refill_behaviour() {
    let fixture = Fixture::new("refill");
    let index = fixture.index();
    let vi = index.lazy.as_ref().unwrap();
    let sparse = |id: u32| id % 97 == 5;
    let half = |id: u32| id.is_multiple_of(2);
    let nothing = |_: u32| false;
    let accepts: [(&str, &dyn Fn(u32) -> bool); 3] =
        [("sparse", &sparse), ("half", &half), ("nothing", &nothing)];
    // Run everything twice: the second pass meets admitted and cached
    // postings.
    for pass in 0..2 {
        for (q, query) in queries().iter().enumerate() {
            for (name, accept) in accepts {
                for refill in [KnnRefill::Always, KnnRefill::BudgetedSegments] {
                    for (max_distance, max_probes) in BUDGETS {
                        for k in [1usize, 10, 300] {
                            let expect = reference_within(
                                vi,
                                &fixture.idx,
                                query,
                                k,
                                max_distance,
                                max_probes,
                                Some(accept),
                                refill,
                            );
                            let got = fixture
                                .reader
                                .knn_search_within_filtered_fetching(
                                    FIELD,
                                    query,
                                    k,
                                    max_distance,
                                    max_probes,
                                    Some(accept),
                                    refill,
                                    None,
                                )
                                .unwrap()
                                .unwrap();
                            assert_eq!(
                                (bits(&got.0), got.1),
                                (bits(&expect.0), expect.1),
                                "pass {pass} query {q} accept {name} {refill:?} k {k} \
                                 budget ({max_distance}, {max_probes})"
                            );
                        }
                    }
                }
            }
        }
    }
    // The refill really ran past the budget, and really stopped early.
    let query = &queries()[0];
    let probes = |accept: &dyn Fn(u32) -> bool, refill| {
        fixture
            .reader
            .knn_search_within_filtered_fetching(
                FIELD,
                query,
                3,
                f32::INFINITY,
                1,
                Some(accept),
                refill,
                None,
            )
            .unwrap()
            .unwrap()
            .1
    };
    assert_eq!(probes(&nothing, KnnRefill::Always), vi.ranges.len());
    assert_eq!(probes(&half, KnnRefill::Always), 1);
    assert!(probes(&sparse, KnnRefill::Always) > 1);
    let none_budgeted = fixture
        .reader
        .knn_search_within_filtered_fetching(
            FIELD,
            query,
            3,
            f32::INFINITY,
            0,
            Some(&half),
            KnnRefill::BudgetedSegments,
            None,
        )
        .unwrap()
        .unwrap();
    assert_eq!(none_budgeted, (Vec::new(), 0));
}

#[test]
fn every_entry_point_equals_a_brute_force_scan() {
    let fixture = Fixture::new("brute");
    let index = fixture.index();
    let vi = index.lazy.as_ref().unwrap();
    assert!(vi.unit_norm && !vi.quantized);
    let stored = corpus();
    for (q, query) in queries().iter().enumerate() {
        for k in [1usize, 10, 64] {
            // The pre-#435 probe over every posting…
            let (expect, _) = reference_within(
                vi,
                &fixture.idx,
                query,
                k,
                f32::INFINITY,
                usize::MAX,
                None,
                KnnRefill::Always,
            );
            // …which is itself the true top k: an independent f64 cosine
            // over the documents as they were written agrees on the scores
            // (ids too, wherever scores are not tied).
            let mut naive: Vec<(u32, f64)> = stored
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let dot: f64 = query
                        .iter()
                        .zip(v)
                        .map(|(a, b)| *a as f64 * *b as f64)
                        .sum();
                    let nq: f64 = query
                        .iter()
                        .map(|a| (*a as f64).powi(2))
                        .sum::<f64>()
                        .sqrt();
                    let nv: f64 = v.iter().map(|a| (*a as f64).powi(2)).sum::<f64>().sqrt();
                    (i as u32, dot / (nq * nv))
                })
                .collect();
            naive.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            naive.truncate(k);
            assert_eq!(expect.len(), k);
            for (got, want) in expect.iter().zip(&naive) {
                assert!(
                    (got.1 - want.1).abs() < 1e-5,
                    "query {q}: {got:?} vs {want:?}"
                );
            }

            let within = fixture
                .reader
                .knn_search_within(query, k, f32::INFINITY, usize::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(bits(&within.0), bits(&expect), "within, query {q} k {k}");

            // Per-segment fallback, probing every posting. It ranks
            // postings by raw centroid distance, so equal scores may come
            // back in another order: compare as sets of (id, score).
            let sorted = |mut hits: Vec<(u32, u64)>| {
                hits.sort_unstable();
                hits
            };
            let nprobe = fixture
                .reader
                .knn_search_filtered(FIELD, query, k, vi.ranges.len(), None)
                .unwrap()
                .unwrap();
            let scores = |hits: &[(u32, f64)]| -> Vec<u64> {
                hits.iter().map(|&(_, s)| s.to_bits()).collect()
            };
            assert_eq!(scores(&nprobe), scores(&expect), "nprobe, query {q} k {k}");

            // Global-rescore candidates: an f32 column's exact best k.
            let candidates = fixture
                .reader
                .knn_candidates_fetching(
                    FIELD,
                    query,
                    k,
                    KnnProbeBudget::Within {
                        max_distance: f32::INFINITY,
                        max_probes: usize::MAX,
                        refill: KnnRefill::Always,
                    },
                    None,
                    None,
                )
                .unwrap()
                .unwrap();
            assert!(candidates.exact);
            assert_eq!(candidates.postings, vi.ranges.len());
            assert!(candidates.candidates.iter().all(|c| c.error == 0.0));
            let as_hits: Vec<(u32, f64)> = candidates
                .candidates
                .iter()
                .map(|c| (c.doc_seq, c.estimate))
                .collect();
            assert_eq!(bits(&as_hits), bits(&expect), "candidates, query {q} k {k}");

            // Exact filtered kNN from the postings, every document allowed.
            let all: Vec<u32> = (0..DOCS as u32).collect();
            let exact = fixture
                .reader
                .knn_exact_filtered(FIELD, query, k, &all)
                .unwrap()
                .unwrap();
            assert_eq!(exact.source, ExactKnnSource::Postings);
            assert_eq!(exact.vectors_scored, DOCS);
            assert_eq!(exact.postings_hydrated, vi.ranges.len());
            assert_eq!(
                scores(&exact.hits),
                scores(&expect),
                "exact, query {q} k {k}"
            );
            // …and restricted to a candidate list.
            let some: Vec<u32> = (0..DOCS as u32).filter(|id| id % 3 == 1).collect();
            let keep = |id: u32| id % 3 == 1;
            let (expect_some, _) = reference_within(
                vi,
                &fixture.idx,
                query,
                k,
                f32::INFINITY,
                usize::MAX,
                Some(&keep),
                KnnRefill::Always,
            );
            let exact = fixture
                .reader
                .knn_exact_filtered(FIELD, query, k, &some)
                .unwrap()
                .unwrap();
            assert_eq!(exact.vectors_scored, some.len());
            assert_eq!(
                sorted(bits(&exact.hits)),
                sorted(bits(&expect_some)),
                "exact filtered, query {q} k {k}"
            );
        }
    }
}

#[test]
fn flat_decode_equals_the_per_float_decode() {
    let mut rng = Lcg(7);
    let mut ids = vec![99u32; 3];
    let mut vectors = vec![9.0f32; 500];
    for dim in [0usize, 1, 3, 8, 24, 33] {
        for count in [0usize, 1, 2, 17] {
            for trailing in [0usize, 1, 3] {
                let mut bytes = Vec::new();
                for _ in 0..count {
                    bytes.extend_from_slice(&(rng.next() as u32).to_le_bytes());
                    for _ in 0..dim {
                        // Raw bit patterns: NaNs and denormals included.
                        bytes.extend_from_slice(&(rng.next() as u32).to_le_bytes());
                    }
                }
                bytes.extend(std::iter::repeat_n(0xAB, trailing.min(3 + 4 * dim)));
                let expect = reference_decode(&bytes, dim);
                // The scratch is reused, larger or smaller than needed.
                decode_f32_entries_into(&bytes, dim, &mut ids, &mut vectors);
                assert_eq!(ids.len(), expect.len());
                assert_eq!(vectors.len(), expect.len() * dim);
                for (i, (id, v)) in expect.iter().enumerate() {
                    assert_eq!(ids[i], *id);
                    let got: Vec<u32> = vectors[i * dim..(i + 1) * dim]
                        .iter()
                        .map(|x| x.to_bits())
                        .collect();
                    let want: Vec<u32> = v.iter().map(|x| x.to_bits()).collect();
                    assert_eq!(got, want, "dim {dim} count {count} trailing {trailing}");
                }
                // The owned decode is the same entries.
                let version = RADIUS_AWARE_VECTOR_INDEX_VERSION;
                let posting = decode_posting_entries(&bytes, dim as u32, version);
                assert_eq!(
                    posting.resident_bytes(),
                    f32_posting_resident_bytes(expect.len(), dim)
                );
                let owned = posting.dequantized();
                assert_eq!(owned.len(), expect.len());
                for ((id, v), (want_id, want)) in owned.iter().zip(&expect) {
                    assert_eq!(id, want_id);
                    assert!(v.iter().zip(want).all(|(a, b)| a.to_bits() == b.to_bits()));
                }
            }
        }
    }
}

#[test]
fn running_top_k_equals_a_stable_sort_and_cut() {
    let mut rng = Lcg(11);
    for round in 0..200 {
        let n = (rng.next() % 300) as usize;
        // Few distinct scores, so most candidates tie with others.
        let levels = 1 + (rng.next() % 12) as u32;
        let scored: Vec<(u32, f32)> = (0..n)
            .map(|i| {
                let level = (rng.next() % levels as u64) as f32;
                (i as u32 ^ 0x5A5A, level / levels as f32 * 2.0 - 1.0)
            })
            .collect();
        for k in [0usize, 1, 2, 10, 299, 1000] {
            let mut sink = CandidateSink::with_running_top_k(k, true);
            let mut all = CandidateSink::with_running_top_k(k, false);
            for &(id, score) in &scored {
                sink.push(id, score, 0.0);
                all.push(id, score, 0.25);
            }
            assert_eq!(sink.len(), n);
            let mut expect: Vec<(u32, f64)> =
                scored.iter().map(|&(id, s)| (id, s as f64)).collect();
            let (kept, errors) = all.finish();
            assert_eq!(kept, expect, "a quantized column keeps every candidate");
            assert_eq!(errors, vec![0.25; n]);
            expect.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            expect.truncate(k);
            let (top, errors) = sink.finish();
            assert_eq!(top, expect, "round {round} k {k}");
            assert!(errors.is_empty());
        }
    }
}

/// The two paths that cut with `top_k_by_score` (global-rescore candidates
/// and the postings fallback of exact filtered kNN) used to hand it every
/// score; they now hand it the sink's running top-`k`. Against the old cut
/// over every score: the scores are the same, position by position, and so
/// is every hit above the `k`-th score. Only which of several documents
/// that tie exactly at the cut are returned may differ (the old select
/// kept ties in no particular order; the sink keeps the first scored).
#[test]
fn top_k_cut_paths_differ_from_the_old_select_only_among_ties_at_the_cut() {
    let fixture = Fixture::new("ties");
    let index = fixture.index();
    let vi = index.lazy.as_ref().unwrap();
    let all_docs: Vec<u32> = (0..DOCS as u32).collect();
    let mut cuts_through_a_tie = 0;
    for (q, query) in queries().iter().enumerate() {
        // Every score, as the pre-#435 probe collected them.
        let (full, _) = reference_within(
            vi,
            &fixture.idx,
            query,
            DOCS * 2,
            f32::INFINITY,
            usize::MAX,
            None,
            KnnRefill::Always,
        );
        assert_eq!(full.len(), DOCS);
        let score_of: HashMap<u32, u64> = bits(&full).into_iter().collect();
        for k in 1..=80usize {
            let old = top_k_by_score(full.clone(), k);
            let kth = old[k - 1].1;
            if full[k].1 == kth {
                cuts_through_a_tie += 1;
            }
            let candidates: Vec<(u32, f64)> = fixture
                .reader
                .knn_candidates_fetching(
                    FIELD,
                    query,
                    k,
                    KnnProbeBudget::Within {
                        max_distance: f32::INFINITY,
                        max_probes: usize::MAX,
                        refill: KnnRefill::Always,
                    },
                    None,
                    None,
                )
                .unwrap()
                .unwrap()
                .candidates
                .iter()
                .map(|c| (c.doc_seq, c.estimate))
                .collect();
            let exact = fixture
                .reader
                .knn_exact_filtered(FIELD, query, k, &all_docs)
                .unwrap()
                .unwrap();
            assert_eq!(exact.source, ExactKnnSource::Postings);
            for (path, got) in [("candidates", &candidates), ("exact", &exact.hits)] {
                let context = format!("{path}, query {q} k {k}");
                let scores = |hits: &[(u32, f64)]| -> Vec<u64> {
                    hits.iter().map(|&(_, s)| s.to_bits()).collect()
                };
                assert_eq!(scores(got), scores(&old), "{context}: scores");
                // Each hit is a distinct document carrying its own score.
                let ids: HashSet<u32> = got.iter().map(|&(id, _)| id).collect();
                assert_eq!(ids.len(), k, "{context}: distinct ids");
                for &(id, score) in got.iter() {
                    assert_eq!(score_of[&id], score.to_bits(), "{context}: doc {id}");
                }
                // Above the k-th score there is no choice to make.
                let above = |hits: &[(u32, f64)]| -> HashSet<u32> {
                    hits.iter()
                        .filter(|&&(_, s)| s > kth)
                        .map(|&(id, _)| id)
                        .collect()
                };
                assert_eq!(above(got), above(&old), "{context}: ids above the cut");
            }
            // The candidates path scores in probe order, so its ties are
            // the reference's: first scored first.
            assert_eq!(bits(&candidates), bits(&full[..k]), "query {q} k {k}");
        }
    }
    assert!(
        cuts_through_a_tie > 0,
        "the fixture should put equal scores on both sides of some cut"
    );
}

#[test]
fn scan_stats_are_scoped_and_off_outside_a_scope() {
    let fixture = Fixture::new("stats");
    let query = &queries()[0];
    let run = || {
        fixture
            .reader
            .knn_search_within(query, 5, f32::INFINITY, 2)
            .unwrap()
            .unwrap()
    };
    // Outside a scope nothing is collected…
    run();
    let ((), empty) = with_knn_scan_stats(|| ());
    assert_eq!(empty, KnnScanStats::default());
    // …an inner scope keeps its postings to itself…
    let ((inner, hits), outer) = with_knn_scan_stats(|| {
        let (hits, inner) = with_knn_scan_stats(run);
        (inner, hits)
    });
    assert_eq!(hits.1, 2);
    assert_eq!(
        inner.postings_scanned + inner.postings_decoded + inner.postings_cached,
        2
    );
    assert_eq!(outer, KnnScanStats::default());
    // …and a probe that fails still reports what it scanned.
    let mut fixture = Fixture::new("stats-fail");
    fixture.set_cache_budget(0);
    fixture.index();
    fs::remove_file(fixture.dir.join(VECTOR_IDX_FILE)).unwrap();
    let (result, stats) = with_knn_scan_stats(|| {
        fixture
            .reader
            .knn_search_within(query, 5, f32::INFINITY, 2)
            .unwrap()
    });
    assert!(matches!(result, Err(KoshaError::Io(_))));
    assert_eq!(stats.postings_scanned, 0);
}

// ── #436: the query layer's ranking of a segment's postings ──────────

/// The budgeted postings as the probe picked them before #436: every
/// score in a stable sort, the prefix under the cutoff, capped.
fn legacy_budgeted(bounds: &[f32], max_distance: f32, max_probes: usize) -> Vec<u32> {
    let mut ranked: Vec<(usize, f32)> = bounds.iter().copied().enumerate().collect();
    ranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let budgeted = ranked
        .iter()
        .take_while(|(_, d)| *d <= max_distance)
        .count()
        .min(max_probes);
    ranked[..budgeted].iter().map(|&(i, _)| i as u32).collect()
}

#[test]
fn segment_ranking_selects_what_the_full_sort_did() {
    // Many exact ties (also across the cutoff and across the cap), both
    // zeros, and infinities.
    let mut rng = Lcg(0x436);
    let values = [-0.0f32, 0.0, 0.25, 0.25, 0.5, 0.75, 1.0, 2.0, f32::INFINITY];
    for len in [0usize, 1, 2, 7, 64, 300] {
        let bounds: Vec<f32> = (0..len)
            .map(|_| {
                if rng.next().is_multiple_of(3) {
                    rng.coord().abs()
                } else {
                    values[(rng.next() >> 33) as usize % values.len()]
                }
            })
            .collect();
        for max_distance in [
            f32::NEG_INFINITY,
            0.0,
            0.25,
            0.6,
            2.0,
            f32::INFINITY,
            f32::NAN,
        ] {
            for max_probes in [0usize, 1, 2, 5, len / 2, len, len + 3, usize::MAX] {
                let ranking = KnnSegmentRanking::new(bounds.clone(), max_distance, max_probes);
                assert_eq!(
                    ranking.budgeted(),
                    legacy_budgeted(&bounds, max_distance, max_probes),
                    "len {len}, cutoff {max_distance}, cap {max_probes}"
                );
                // The full ranking a refill continues from starts with it.
                let all = KnnSegmentRanking::rank_all(ranking.bounds().unwrap());
                let prefix: Vec<u32> = all[..ranking.budgeted().len()]
                    .iter()
                    .map(|&(i, _)| i)
                    .collect();
                assert_eq!(prefix, ranking.budgeted());
                assert_eq!(all.len(), len);
            }
        }
    }
    // A NaN score keeps whatever the full sort makes of it.
    let bounds = [0.5f32, f32::NAN, 0.1, 0.5, 0.9];
    for max_probes in [0usize, 2, usize::MAX] {
        assert_eq!(
            KnnSegmentRanking::new(bounds.to_vec(), 0.6, max_probes).budgeted(),
            legacy_budgeted(&bounds, 0.6, max_probes)
        );
    }
}

/// Cutoffs taken from the segment's own scores for `query`, so some fall
/// exactly on a posting's score, plus the open ends.
fn plan_cutoffs(bounds: &[f32]) -> Vec<f32> {
    let mut sorted = bounds.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    vec![
        f32::NEG_INFINITY,
        sorted[0],
        sorted[sorted.len() / 3],
        (sorted[1] + sorted[2]) / 2.0,
        sorted[sorted.len() - 1],
        f32::INFINITY,
    ]
}

#[test]
fn planned_probe_matches_the_segment_ranking_its_own_postings() {
    let mut fixture = Fixture::new("planned");
    fixture.set_cache_budget(0);
    let index = fixture.index();
    let vi = index.lazy.as_ref().unwrap();
    let postings = vi.ranges.len();
    let even = |id: u32| id.is_multiple_of(2);
    // So few pass that the probe runs past any budget, to the last posting.
    let rare = |id: u32| id % 300 == 7;
    let none = |_: u32| false;
    type Accept<'a> = Option<&'a dyn Fn(u32) -> bool>;
    let accepts: [(&str, Accept<'_>); 4] = [
        ("unrestricted", None),
        ("even", Some(&even)),
        ("rare", Some(&rare)),
        ("none", Some(&none)),
    ];
    let mut refilled = 0usize;
    for (q, query) in queries().iter().enumerate() {
        let bounds = fixture.reader.knn_centroid_distances(FIELD, query).unwrap();
        assert_eq!(bounds.len(), postings);
        for max_distance in plan_cutoffs(&bounds) {
            for max_probes in [0usize, 1, 3, postings, usize::MAX] {
                let ranking = KnnSegmentRanking::new(bounds.clone(), max_distance, max_probes);
                // The ranking two-stage planning hands over: only the
                // postings that could be inside the budget carry a score
                // (here: those under the cutoff and every third other).
                let candidates: Vec<(u32, f32)> = bounds
                    .iter()
                    .enumerate()
                    .filter(|(i, &d)| d <= max_distance || i % 3 == 0)
                    .map(|(i, &d)| (i as u32, d))
                    .collect();
                let partial = KnnSegmentRanking::from_candidates(
                    postings,
                    &candidates,
                    max_distance,
                    max_probes,
                );
                assert_eq!(partial.budgeted(), ranking.budgeted());
                for (name, accept) in accepts {
                    for refill in [KnnRefill::Always, KnnRefill::BudgetedSegments] {
                        for k in [0usize, 1, 10, DOCS * 2] {
                            let context = format!(
                                "query {q}, cutoff {max_distance}, cap {max_probes}, {name}, \
                                 {refill:?}, k {k}"
                            );
                            // The probe as it was before #435 and #436.
                            let expect = reference_within(
                                vi,
                                &fixture.idx,
                                query,
                                k,
                                max_distance,
                                max_probes,
                                accept,
                                refill,
                            );
                            let planned = fixture
                                .reader
                                .knn_search_planned_fetching(
                                    FIELD, query, k, &ranking, accept, refill, None,
                                )
                                .unwrap()
                                .unwrap();
                            let unplanned = fixture
                                .reader
                                .knn_search_within_filtered_fetching(
                                    FIELD,
                                    query,
                                    k,
                                    max_distance,
                                    max_probes,
                                    accept,
                                    refill,
                                    None,
                                )
                                .unwrap()
                                .unwrap();
                            assert_eq!(
                                (bits(&planned.0), planned.1),
                                (bits(&unplanned.0), unplanned.1),
                                "planned vs unplanned: {context}"
                            );
                            assert_eq!(planned.1, expect.1, "postings probed: {context}");
                            // A ranking without the scores of the postings
                            // outside the budget: a refill scores the
                            // segment itself and probes the same postings.
                            let from_partial = fixture
                                .reader
                                .knn_search_planned_fetching(
                                    FIELD, query, k, &partial, accept, refill, None,
                                )
                                .unwrap()
                                .unwrap();
                            assert_eq!(
                                (bits(&from_partial.0), from_partial.1),
                                (bits(&planned.0), planned.1),
                                "candidates-only ranking: {context}"
                            );
                            // Above a tie at the cut the reference (a
                            // stable sort of everything) and the running
                            // top-k agree; compare scores, which are
                            // exact, and ids where no tie decides.
                            let scores = |hits: &[(u32, f64)]| -> Vec<u64> {
                                hits.iter().map(|&(_, s)| s.to_bits()).collect()
                            };
                            assert_eq!(scores(&planned.0), scores(&expect.0), "{context}");
                            if planned.1 > ranking.budgeted().len() {
                                refilled += 1;
                            }

                            // The candidates path takes the same plan.
                            let candidates = |budget| {
                                fixture
                                    .reader
                                    .knn_candidates_fetching(FIELD, query, k, budget, accept, None)
                                    .unwrap()
                                    .unwrap()
                            };
                            let with_plan = candidates(KnnProbeBudget::Planned {
                                ranking: &ranking,
                                refill,
                            });
                            let with_partial = candidates(KnnProbeBudget::Planned {
                                ranking: &partial,
                                refill,
                            });
                            assert_eq!(with_partial.candidates, with_plan.candidates, "{context}");
                            assert_eq!(with_partial.postings, with_plan.postings, "{context}");
                            let without = candidates(KnnProbeBudget::Within {
                                max_distance,
                                max_probes,
                                refill,
                            });
                            assert_eq!(with_plan.candidates, without.candidates, "{context}");
                            assert_eq!(with_plan.postings, without.postings, "{context}");
                        }
                    }
                }
            }
        }
    }
    assert!(refilled > 0, "some probe must have run past its budget");
}

#[test]
fn a_ranking_of_another_shape_is_ignored() {
    let fixture = Fixture::new("misfit");
    let query = &queries()[1];
    let mut bounds = fixture.reader.knn_centroid_distances(FIELD, query).unwrap();
    // One score too many: not this column's ranking. Its first posting
    // would otherwise be the only one probed.
    bounds.push(0.0);
    let misfit = KnnSegmentRanking::new(bounds, f32::INFINITY, 2);
    let got = fixture
        .reader
        .knn_search_planned_fetching(FIELD, query, 10, &misfit, None, KnnRefill::Always, None)
        .unwrap()
        .unwrap();
    let own = fixture
        .reader
        .knn_search_within_filtered(FIELD, query, 10, f32::INFINITY, 2, None)
        .unwrap()
        .unwrap();
    assert_eq!((bits(&got.0), got.1), (bits(&own.0), own.1));
}

#[test]
fn centroids_are_one_row_major_matrix() {
    let fixture = Fixture::new("flat-centroids");
    let index = fixture.index();
    let vi = index.lazy.as_ref().unwrap();
    assert_eq!(vi.centroids.len(), vi.ranges.len() * vi.dim);
    assert_eq!(vi.centroid_rows().count(), vi.ranges.len());
    // Row `i` is what the offsets sidecar holds for posting `i`, and what
    // compaction is handed.
    let offsets = fs::read(fixture.dir.join("vector.offsets")).unwrap();
    let parsed = parse_vector_offsets(&offsets, vi.dim as u32, vi.version).unwrap();
    let layout = fixture
        .reader
        .vector_posting_layout(FIELD)
        .unwrap()
        .unwrap();
    assert_eq!(parsed.len(), vi.ranges.len());
    for (i, p) in parsed.iter().enumerate() {
        assert_eq!(vi.centroid(i), p.centroid.as_slice(), "posting {i}");
        assert_eq!(layout.postings[i].centroid, p.centroid, "posting {i}");
    }
}

#[test]
fn a_planned_probe_inside_its_budget_reports_no_ranking_time() {
    let mut fixture = Fixture::new("planned-rank-time");
    fixture.set_cache_budget(0);
    let query = &queries()[0];
    let bounds = fixture.reader.knn_centroid_distances(FIELD, query).unwrap();
    let ranking = KnnSegmentRanking::new(bounds, f32::INFINITY, 3);
    // Exactly zero, not "small": the stat tells a segment that ranked for
    // itself from one the plan ranked for.
    for _ in 0..20 {
        let (hits, stats) = with_knn_scan_stats(|| {
            fixture
                .reader
                .knn_search_planned_fetching(
                    FIELD,
                    query,
                    5,
                    &ranking,
                    None,
                    KnnRefill::Always,
                    None,
                )
                .unwrap()
                .unwrap()
        });
        assert_eq!(hits.1, 3);
        assert_eq!(stats.rank_nanos, 0);
    }
    // A segment left to rank for itself does report it.
    let ranked_here = (0..20).any(|_| {
        let (_, stats) = with_knn_scan_stats(|| {
            fixture
                .reader
                .knn_search_within(query, 5, f32::INFINITY, 3)
                .unwrap()
                .unwrap()
        });
        stats.rank_nanos > 0
    });
    assert!(ranked_here);
}

#[test]
fn per_segment_probe_respects_opt_in_radius_quantiles() {
    let mut fixture = Fixture::new("nprobe-quantiles");
    fixture.set_cache_budget(0);
    let postings = [
        kosha_vector_spfresh::PostingSnapshot {
            centroid: vec![0.6, 0.8],
            entries: vec![(0, vec![0.6, 0.8])],
            radius: 0.0,
        },
        kosha_vector_spfresh::PostingSnapshot {
            centroid: vec![0.0, 1.0],
            entries: vec![
                (1, vec![1.0, 0.0]),
                (2, vec![-1.0, 0.0]),
                (3, vec![0.0, 1.0]),
            ],
            radius: 1.0,
        },
    ];
    let path = fixture.dir.join(VECTOR_IDX_FILE);
    let query = [1.0, 0.0];
    for (version, mode, expected) in [
        (QUANTILE_VECTOR_INDEX_VERSION, KnnProbeRadius::Max, 0),
        (RADIUS_AWARE_VECTOR_INDEX_VERSION, KnnProbeRadius::P50, 0),
        (QUANTILE_VECTOR_INDEX_VERSION, KnnProbeRadius::P50, 1),
        (QUANTILE_VECTOR_INDEX_VERSION, KnnProbeRadius::P90, 1),
    ] {
        let (idx, offsets) = encode_vector_index_v2(2, &postings, version);
        fs::write(&path, idx).unwrap();
        let records = parse_vector_offsets(&offsets, 2, version).unwrap();
        let mut vi = LazyVectorIndex::from_parsed(&path, 0, 2, version, records, None);
        vi.probe_radius = mode;
        let hits = fixture
            .reader
            .knn_search_lazy(&vi, &query, 1, 1, None, None)
            .unwrap();
        assert_eq!(hits[0].0, expected, "{version} {mode:?}");
    }
}

/// A column whose header says dimension zero (no writer produces one, but
/// neither parser rejects it) still has one, empty, centroid per posting:
/// the per-segment `nprobe` probe ranks and scans those postings, as it
/// did when centroids were a `Vec` each.
#[test]
fn nprobe_probe_of_a_zero_dimension_column_still_scans_its_postings() {
    let fixture = Fixture::new("dim-zero");
    let version = fixture.index().lazy.as_ref().unwrap().version;
    assert!(!is_quantized_vector_index_version(version));
    // Three postings of ids only (an f32 entry of dimension 0 is its id).
    let postings: [&[u32]; 3] = [&[0, 1], &[2], &[3, 4, 5]];
    let header = |magic: u32, count: u32| {
        let mut bytes = magic.to_le_bytes().to_vec();
        bytes.extend(version.to_le_bytes());
        bytes.extend(0u32.to_le_bytes());
        bytes.extend(count.to_le_bytes());
        bytes
    };
    let mut idx = header(VECTOR_MAGIC, 6);
    let mut offsets = header(VECTOR_OFFSETS_MAGIC, postings.len() as u32);
    for ids in postings {
        offsets.extend((idx.len() as u64).to_le_bytes());
        offsets.extend((ids.len() as u32 * 4).to_le_bytes());
        if version >= RADIUS_AWARE_VECTOR_INDEX_VERSION {
            offsets.extend(0f32.to_le_bytes());
        }
        for id in ids {
            idx.extend(id.to_le_bytes());
        }
    }
    fs::write(fixture.dir.join(VECTOR_IDX_FILE), idx).unwrap();
    fs::write(fixture.dir.join("vector.offsets"), offsets).unwrap();
    let reader = SegmentReader::open_with_options(fixture.dir.clone(), true).unwrap();
    let index = reader.vector_field(FIELD).unwrap().unwrap();
    let vi = index.lazy.as_ref().unwrap();
    assert_eq!((vi.dim, vi.ranges.len()), (0, 3));

    let ids = |hits: Vec<(u32, f64)>| {
        let mut ids: Vec<u32> = hits.into_iter().map(|(id, _)| id).collect();
        ids.sort_unstable();
        ids
    };
    let all = reader
        .knn_search_filtered(FIELD, &[], 10, 3, None)
        .unwrap()
        .unwrap();
    assert_eq!(ids(all), vec![0, 1, 2, 3, 4, 5]);
    // Restricted: every posting is ranked, so the probe runs past `nprobe`
    // until the page fills.
    let odd = |id: u32| id % 2 == 1;
    let filtered = reader
        .knn_search_filtered(FIELD, &[], 10, 1, Some(&odd))
        .unwrap()
        .unwrap();
    assert_eq!(ids(filtered), vec![1, 3, 5]);
}

// ── Vector postings cache: admission, recency, memory bound ──────────

/// A segment of `docs` random `dim`-wide vectors in its own directory.
fn write_vector_segment(name: &str, docs: usize, dim: usize) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("kosha-vcache-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let mut writer = SegmentWriter::new(SegmentId(name.into()), dir.clone());
    let mut rng = Lcg(docs as u64 ^ 0x5EED);
    for i in 0..docs {
        let v = (0..dim).map(|_| rng.coord()).collect();
        writer.add_document(DocumentId(format!("d{i}")), vec![Field::vector(FIELD, v)]);
    }
    writer.finalize(Bm25Params::default()).unwrap();
    let _ = fs::remove_file(dir.join(DOC_VECTORS_FILE));
    dir
}

/// Live bytes reported by the caches, as the query layer's ledger sees
/// them.
#[derive(Default)]
struct CountingAccount(std::sync::atomic::AtomicI64);

impl PostingsMemoryAccount for CountingAccount {
    fn add_postings(&self, bytes: u64) {
        self.0
            .fetch_add(bytes as i64, std::sync::atomic::Ordering::SeqCst);
    }
    fn release_postings(&self, bytes: u64) {
        self.0
            .fetch_sub(bytes as i64, std::sync::atomic::Ordering::SeqCst);
    }
}

impl CountingAccount {
    fn live(&self) -> i64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Staging probed 1,610 postings per segment per query; the 256-key
/// second-touch ring forgot each one before the next query asked for it
/// again, so a repeated query never got a cache hit. Through the real
/// probe path: a query probing far more than 256 postings is admitted on
/// its second run and served from the cache on its third.
#[test]
fn a_probe_of_more_postings_than_the_text_ring_is_admitted_on_its_second_run() {
    let dir = write_vector_segment("wide", 60_000, 4);
    let mut reader = SegmentReader::open_with_options(dir.clone(), true).unwrap();
    reader.vector_cache = Arc::new(PostingsCache::new(usize::MAX / 2, None));
    let postings = {
        let index = reader.vector_field(FIELD).unwrap().unwrap();
        index.lazy.as_ref().unwrap().ranges.len()
    };
    assert!(
        postings > 2 * POSTINGS_CACHE_MISS_RING,
        "the probe must outnumber the text ring: {postings} postings"
    );
    let query = [0.25f32, -0.5, 0.75, 0.1];
    let run = || {
        with_knn_scan_stats(|| {
            reader
                .knn_search_within(&query, 10, f32::INFINITY, usize::MAX)
                .unwrap()
                .unwrap()
        })
        .1
    };
    let counts = |s: KnnScanStats| (s.postings_scanned, s.postings_decoded, s.postings_cached);
    assert_eq!(counts(run()), (postings, 0, 0), "first run: one-shot");
    assert_eq!(counts(run()), (0, postings, 0), "second run: admitted");
    assert_eq!(counts(run()), (0, 0, postings), "third run: all cached");
    drop(reader);
    let _ = fs::remove_dir_all(&dir);
}

/// One query that probes more postings than the cache can hold, repeated,
/// would only churn an LRU (each posting evicted before its reuse): the
/// second-touch window is the cache's reach, so it is not admitted and
/// nothing is decoded for nothing.
#[test]
fn a_probe_larger_than_the_cache_reach_passes_through() {
    let dir = write_vector_segment("reach", 60_000, 4);
    let mut reader = SegmentReader::open_with_options(dir.clone(), true).unwrap();
    let (postings, largest) = {
        let index = reader.vector_field(FIELD).unwrap().unwrap();
        let vi = index.lazy.as_ref().unwrap();
        let largest = vi.ranges.iter().map(|&(_, l)| l as usize).max().unwrap();
        (
            vi.ranges.len(),
            f32_posting_resident_bytes(largest / (4 + 16), 4),
        )
    };
    // Room for at most 128 postings, far fewer than one probe touches.
    reader.vector_cache = Arc::new(PostingsCache::new(largest * 128, None));
    assert!(postings > 4 * 128, "{postings} postings");
    let query = [0.25f32, -0.5, 0.75, 0.1];
    for _ in 0..3 {
        let (_, stats) = with_knn_scan_stats(|| {
            reader
                .knn_search_within(&query, 10, f32::INFINITY, usize::MAX)
                .unwrap()
                .unwrap()
        });
        assert_eq!(stats.postings_scanned, postings, "{stats:?}");
    }
    drop(reader);
    let _ = fs::remove_dir_all(&dir);
}

/// Every segment's vector postings share one budget: however many
/// segments are open, resident decoded postings stay under it, the
/// ledger's view (each segment's account) matches what is resident, and a
/// dropped segment takes its entries and their live bytes with it.
#[test]
fn vector_postings_of_all_segments_stay_under_one_budget() {
    let dirs: Vec<PathBuf> = (0..4)
        .map(|i| write_vector_segment(&format!("bound{i}"), 3_000, 8))
        .collect();
    let largest = dirs
        .iter()
        .map(|dir| {
            let reader = SegmentReader::open_with_options(dir.clone(), true).unwrap();
            let index = reader.vector_field(FIELD).unwrap().unwrap();
            let vi = index.lazy.as_ref().unwrap();
            let l = vi.ranges.iter().map(|&(_, l)| l as usize).max().unwrap();
            f32_posting_resident_bytes(l / (4 + 32), 8)
        })
        .max()
        .unwrap();
    // Readers opened the normal way share the one process-wide cache…
    {
        let a = SegmentReader::open_with_options(dirs[0].clone(), true).unwrap();
        let b = SegmentReader::open_with_options(dirs[1].clone(), true).unwrap();
        assert!(Arc::ptr_eq(&a.vector_cache, &b.vector_cache));
        assert_ne!(a.vector_cache_owner, b.vector_cache_owner);
    }
    // …here a private one, about one segment's worth: four segments'
    // working sets cannot all fit.
    let budget = largest * 60;
    let cache = Arc::new(PostingsCache::new(budget, None));
    let accounts: Vec<Arc<CountingAccount>> = (0..dirs.len()).map(|_| Arc::default()).collect();
    let mut readers: Vec<SegmentReader> = dirs
        .iter()
        .zip(&accounts)
        .map(|(dir, account)| {
            let account: SharedAccount = Arc::clone(account) as SharedAccount;
            let mut r =
                SegmentReader::open_with_footer_options(dir.clone(), true, None, Some(account))
                    .unwrap();
            r.vector_cache = Arc::clone(&cache);
            r
        })
        .collect();
    let total = |readers: &[SegmentReader]| -> usize {
        readers
            .iter()
            .map(|r| r.vector_cache_resident_bytes())
            .sum()
    };
    let mut peak = 0;
    for round in 0..4 {
        for (i, reader) in readers.iter().enumerate() {
            let query: Vec<f32> = (0..8).map(|d| ((round + i + d) % 5) as f32 - 2.0).collect();
            reader
                .knn_search_within(&query, 10, f32::INFINITY, usize::MAX)
                .unwrap()
                .unwrap();
            let resident = total(&readers);
            assert!(resident <= budget, "{resident} > {budget}");
            peak = peak.max(resident);
        }
    }
    assert!(peak > budget / 2, "the cache was used: peak {peak}");
    for (reader, account) in readers.iter().zip(&accounts) {
        assert_eq!(account.live(), reader.vector_cache_resident_bytes() as i64);
    }
    let gone = readers.remove(0);
    drop(gone);
    assert_eq!(
        accounts[0].live(),
        0,
        "a dropped segment releases its bytes"
    );
    assert_eq!(total(&readers), cache.state.lock().unwrap().total_bytes);
    drop(readers);
    assert_eq!(cache.state.lock().unwrap().total_bytes, 0);
    for dir in dirs {
        let _ = fs::remove_dir_all(&dir);
    }
}

/// Hits re-stamp instead of moving entries; eviction must still follow
/// exact LRU order. Checked against a plain ordered-list model over a
/// long random mix of hits, inserts and membership checks (which must not
/// count as use), and the recency queue must stay compacted.
#[test]
fn postings_cache_evicts_in_exact_lru_order() {
    const CAP: usize = 40;
    let cache = PostingsCache::<usize, usize>::new(CAP, None);
    let mut model: VecDeque<usize> = VecDeque::new();
    let mut rng = Lcg(42);
    for step in 0..200_000 {
        let key = (rng.next() % 64) as usize;
        match rng.next() % 3 {
            0 => {
                let hit = cache.get(key);
                let pos = model.iter().position(|k| *k == key);
                assert_eq!(hit.is_some(), pos.is_some(), "step {step} key {key}");
                if let Some(pos) = pos {
                    model.remove(pos);
                    model.push_back(key);
                }
            }
            1 => assert_eq!(cache.contains(key), model.contains(&key)),
            _ => {
                cache.insert(key, key, 1);
                if let Some(pos) = model.iter().position(|k| *k == key) {
                    model.remove(pos);
                }
                model.push_back(key);
                if model.len() > CAP {
                    model.pop_front();
                }
            }
        }
        let st = cache.state.lock().unwrap();
        assert!(st.recency.queue.len() <= 2 * st.entries.len() + 64);
    }
    let st = cache.state.lock().unwrap();
    let mut order: Vec<(u64, usize)> = st.entries.iter().map(|(k, e)| (e.stamp, *k)).collect();
    order.sort_unstable();
    let order: Vec<usize> = order.into_iter().map(|(_, k)| k).collect();
    assert_eq!(order, model.into_iter().collect::<Vec<_>>());
}

/// The text caches keep their 256-key second-touch ring: a key missed
/// again within 256 other misses is admitted, one missed after more is
/// not.
#[test]
fn text_second_touch_ring_is_unchanged() {
    let cache = PostingsCache::<usize, u8>::new(usize::MAX, None);
    for k in 0..POSTINGS_CACHE_MISS_RING {
        assert!(!cache.admit_on_miss(k));
    }
    assert!(cache.admit_on_miss(0), "within the ring");
    for k in 1_000..1_000 + POSTINGS_CACHE_MISS_RING {
        assert!(!cache.admit_on_miss(k));
    }
    assert!(!cache.admit_on_miss(1), "pushed out of the ring");
    let st = cache.state.lock().unwrap();
    assert!(st.misses.len() <= POSTINGS_CACHE_MISS_RING);
    assert!(st.miss_order.queue.len() <= 2 * st.misses.len() + 64);
}

/// Per-hit cost at 5,000 entries (staging's posting count per segment):
/// the O(1) re-stamp against the old recency scan (`position` + `remove`
/// + `push_back` on a `VecDeque`). Run with:
///
/// `cargo test --release -p kosha-segment per_hit_cost -- --ignored --nocapture`
#[test]
#[ignore]
fn per_hit_cost_at_five_thousand_entries() {
    const N: usize = 5_000;
    const HITS: usize = 2_000_000;
    let cache = PostingsCache::<usize, Arc<u8>>::new(usize::MAX / 2, None);
    let value = Arc::new(0u8);
    for k in 0..N {
        cache.insert(k, Arc::clone(&value), 1);
    }
    let mut rng = Lcg(7);
    let keys: Vec<usize> = (0..HITS)
        .map(|_| (rng.next() % N as u64) as usize)
        .collect();
    let t = std::time::Instant::now();
    for k in &keys {
        std::hint::black_box(cache.get(*k));
    }
    let new_ns = t.elapsed().as_nanos() as f64 / HITS as f64;
    type OldLru = (HashMap<usize, Arc<u8>>, VecDeque<usize>);
    let old: Mutex<OldLru> = Mutex::new((
        (0..N).map(|k| (k, Arc::clone(&value))).collect(),
        (0..N).collect(),
    ));
    let t = std::time::Instant::now();
    for k in &keys {
        let mut st = old.lock().unwrap();
        let hit = st.0.get(k).cloned();
        if let Some(pos) = st.1.iter().position(|x| x == k) {
            st.1.remove(pos);
        }
        st.1.push_back(*k);
        std::hint::black_box(hit);
    }
    let old_ns = t.elapsed().as_nanos() as f64 / HITS as f64;
    println!("per hit at {N} entries: new {new_ns:.1} ns, old {old_ns:.1} ns");
}

/// Freeing a decoded posting can mean thousands of deallocations; the
/// shared vector cache's lock (which every kNN worker takes) must not be
/// held while evicted or removed entries are freed.
#[test]
fn evicted_and_removed_entries_are_freed_outside_the_cache_lock() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static CACHE: OnceLock<PostingsCache<(u64, usize), Probe>> = OnceLock::new();
    static FREED: AtomicUsize = AtomicUsize::new(0);
    static FREED_LOCKED: AtomicUsize = AtomicUsize::new(0);
    #[derive(Clone)]
    struct Probe(#[allow(dead_code)] Arc<u8>);
    impl Drop for Probe {
        fn drop(&mut self) {
            // Last reference (the cache's): is the cache lock free?
            if Arc::strong_count(&self.0) == 1 {
                FREED.fetch_add(1, Ordering::SeqCst);
                if CACHE.get().unwrap().state.try_lock().is_err() {
                    FREED_LOCKED.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }
    let cache = CACHE.get_or_init(|| PostingsCache::new(4, None));
    let account: Arc<CountingAccount> = Arc::default();
    let shared: SharedAccount = Arc::clone(&account) as SharedAccount;
    // Evictions: each insert past four entries evicts the oldest.
    for k in 0..10 {
        cache.insert_for((0, k), Probe(Arc::new(0)), 1, Some(&shared));
    }
    // Replacement of an existing key frees the old value.
    cache.insert_for((0, 9), Probe(Arc::new(0)), 1, Some(&shared));
    // A dropped segment's entries.
    cache.remove_where(|(owner, _)| *owner == 0);
    assert_eq!(
        FREED.load(Ordering::SeqCst),
        11,
        "6 evicted, 1 replaced, 4 removed"
    );
    assert_eq!(FREED_LOCKED.load(Ordering::SeqCst), 0);
    assert_eq!(account.live(), 0);
}

// ---- Finding 31: what the prefetch skips must stay readable until scanned ----

type SpanFetch<'a> = dyn Fn(&Path, &str, &[(u64, u64)]) -> span_cache::SpanHolds + Sync + 'a;

/// A fetch as the server makes it when its span write queue is full
/// (#481): the fetched bytes are held in memory by the fetching request
/// only and never reach disk. Counts its calls; runs `hook(call)` before
/// it returns.
fn held_fetch<'a>(
    idx: &'a [u8],
    held: &'a Arc<AtomicU64>,
    calls: &'a AtomicUsize,
    hook: &'a (dyn Fn(usize) + Sync),
) -> impl Fn(&Path, &str, &[(u64, u64)]) -> span_cache::SpanHolds + Sync + 'a {
    move |dir: &Path, file: &str, ranges: &[(u64, u64)]| {
        let call = calls.fetch_add(1, Ordering::Relaxed);
        let holds = ranges
            .iter()
            .map(|&(s, e)| {
                span_cache::PendingSpan::register_held(
                    dir,
                    file,
                    s,
                    idx[s as usize..e as usize].to_vec(),
                    held.clone(),
                )
            })
            .collect::<span_cache::SpanHolds>();
        hook(call);
        holds
    }
}

/// A budgeted probe of every posting of `fixture` for `query`, through
/// `fetch`.
fn probe_all(
    fixture: &Fixture,
    query: &[f32],
    fetch: &SpanFetch<'_>,
) -> Result<Vec<(u32, u64)>, KoshaError> {
    fixture
        .reader
        .knn_search_within_filtered_fetching(
            FIELD,
            query,
            10,
            f32::INFINITY,
            usize::MAX,
            None,
            KnnRefill::Always,
            Some(fetch),
        )
        .unwrap()
        .map(|(hits, _)| bits(&hits))
}

/// Finding 31, deterministic: postings the prefetch skips because the
/// decoded cache holds them, evicted before the scan reaches them, whose
/// span writes were dropped (so the decoded entry was their only local
/// copy). The eviction inside the fetch stands in for another request's
/// inserts into the shared cache. Failed with "vector posting … is not
/// local" while the prefetch only checked `contains()`.
#[test]
fn a_cached_posting_the_prefetch_skips_stays_readable_when_evicted() {
    let mut fixture = Fixture::new("pin-evicted");
    fixture.set_cache_budget(usize::MAX / 2);
    let cache = Arc::clone(&fixture.reader.vector_cache);
    fs::remove_file(fixture.dir.join(VECTOR_IDX_FILE)).unwrap();
    let query = &queries()[1];
    let held = Arc::new(AtomicU64::new(0));
    let calls = AtomicUsize::new(0);
    let fetch = held_fetch(&fixture.idx, &held, &calls, &|_| {});
    // First touch, then second touch admits every posting into the
    // decoded cache; no span ever reaches disk.
    let want = probe_all(&fixture, query, &fetch).unwrap();
    probe_all(&fixture, query, &fetch).unwrap();
    assert_eq!(held.load(Ordering::Relaxed), 0, "holds released");
    assert!(!span_cache::spans_dir(&fixture.dir, VECTOR_IDX_FILE).exists());
    let postings = fixture.index().lazy.as_ref().unwrap().ranges.len();
    // One posting not cached, so the prefetch calls the fetch; it skips
    // every other one, and the fetch then evicts them all.
    cache.remove_where(|&(_, k)| k == 0);
    let calls = AtomicUsize::new(0);
    let evict_all = |_| cache.remove_where(|_| true);
    let fetch = held_fetch(&fixture.idx, &held, &calls, &evict_all);
    let got = probe_all(&fixture, query, &fetch);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "one prefetch, no refetch: the {} skipped postings were pinned",
        postings - 1
    );
    assert_eq!(got.expect("every skipped posting is read"), want);
}

/// Finding 31 with no hook and one thread: a cache that holds half of one
/// probe's postings. The probe's own second-touch admissions evict
/// postings its prefetch skipped as cached, whose bytes were held-only
/// (dropped writes). Failed on the fourth run while the prefetch only
/// checked `contains()`.
#[test]
fn a_probe_evicting_its_own_skipped_postings_still_reads_them() {
    let query = &queries()[1];
    let held = Arc::new(AtomicU64::new(0));
    let calls = AtomicUsize::new(0);
    // Calibrate: the decoded bytes of every posting this probe reads.
    let mut cal = Fixture::new("pin-cal");
    cal.set_cache_budget(usize::MAX / 2);
    fs::remove_file(cal.dir.join(VECTOR_IDX_FILE)).unwrap();
    let fetch = held_fetch(&cal.idx, &held, &calls, &|_| {});
    let want = probe_all(&cal, query, &fetch).unwrap();
    probe_all(&cal, query, &fetch).unwrap();
    let total = cal.reader.vector_cache.resident_bytes_where(|_| true);
    assert!(total > 0);

    let mut fixture = Fixture::new("pin-self");
    fixture.set_cache_budget(total / 2);
    fs::remove_file(fixture.dir.join(VECTOR_IDX_FILE)).unwrap();
    let fetch = held_fetch(&fixture.idx, &held, &calls, &|_| {});
    for run in 0..4 {
        let before = calls.load(Ordering::Relaxed);
        let got = probe_all(&fixture, query, &fetch);
        assert_eq!(got.unwrap_or_else(|e| panic!("run {run}: {e}")), want);
        assert!(
            calls.load(Ordering::Relaxed) - before <= 1,
            "run {run}: one prefetch, no refetch: the skipped postings were pinned"
        );
    }
    assert_eq!(held.load(Ordering::Relaxed), 0, "holds released");
}

/// Control: the same sequence as the eviction test, but every fetch's
/// write lands on disk (as before #481): evicted postings are read from
/// their span files. Passes with or without the pins.
#[test]
fn evicted_cached_postings_with_spans_on_disk_are_read_from_disk() {
    let mut fixture = Fixture::new("pin-control");
    fixture.set_cache_budget(usize::MAX / 2);
    let cache = Arc::clone(&fixture.reader.vector_cache);
    fs::remove_file(fixture.dir.join(VECTOR_IDX_FILE)).unwrap();
    let query = &queries()[1];
    let idx = fixture.idx.clone();
    let write_fetch = |dir: &Path, file: &str, ranges: &[(u64, u64)]| {
        let spans = span_cache::spans_dir(dir, file);
        fs::create_dir_all(&spans).unwrap();
        for &(s, e) in ranges {
            fs::write(
                spans.join(span_cache::span_file_name(s, e)),
                &idx[s as usize..e as usize],
            )
            .unwrap();
        }
        span_cache::SpanHolds::new()
    };
    let want = probe_all(&fixture, query, &write_fetch).unwrap();
    probe_all(&fixture, query, &write_fetch).unwrap();
    cache.remove_where(|&(_, k)| k == 0);
    let evicting = |dir: &Path, file: &str, ranges: &[(u64, u64)]| {
        let holds = write_fetch(dir, file, ranges);
        cache.remove_where(|_| true);
        holds
    };
    assert_eq!(probe_all(&fixture, query, &evicting).unwrap(), want);
}

/// `fixture` with every posting in its own span file and `vector.idx`
/// gone, except posting 0's span (so a probe's prefetch calls the fetch).
/// Returns the postings' ranges.
fn spans_but_the_first(fixture: &mut Fixture) -> Vec<(u64, u64)> {
    fixture.set_cache_budget(0);
    fixture.move_idx_to_spans(1);
    let index = fixture.index();
    let ranges: Vec<(u64, u64)> = index
        .lazy
        .as_ref()
        .unwrap()
        .ranges
        .iter()
        .map(|&(o, l)| (o, o + l as u64))
        .collect();
    let spans = span_cache::spans_dir(&fixture.dir, VECTOR_IDX_FILE);
    fs::remove_file(spans.join(span_cache::span_file_name(ranges[0].0, ranges[0].1))).unwrap();
    ranges
}

/// Backstop (b): span files the prefetch saw on disk, evicted from disk
/// before the scan reads them. Each such posting is refetched once and
/// read, so the answer is exact rather than a "not local" degraded
/// segment.
#[test]
fn a_posting_evicted_from_disk_after_the_prefetch_is_refetched_once() {
    let mut fixture = Fixture::new("refetch");
    let query = &queries()[1];
    let want = {
        let calls = AtomicUsize::new(0);
        let held = Arc::new(AtomicU64::new(0));
        let mut whole = Fixture::new("refetch-want");
        whole.set_cache_budget(0);
        let fetch = held_fetch(&whole.idx, &held, &calls, &|_| {});
        probe_all(&whole, query, &fetch).unwrap()
    };
    let ranges = spans_but_the_first(&mut fixture);
    let evicted = 3;
    assert!(evicted <= VECTOR_REFETCH_MAX && ranges.len() > evicted + 1);
    let spans = span_cache::spans_dir(&fixture.dir, VECTOR_IDX_FILE);
    let held = Arc::new(AtomicU64::new(0));
    let calls = AtomicUsize::new(0);
    let evict = |call: usize| {
        if call == 0 {
            for &(s, e) in &ranges[1..=evicted] {
                fs::remove_file(spans.join(span_cache::span_file_name(s, e))).unwrap();
            }
        }
    };
    let fetch = held_fetch(&fixture.idx, &held, &calls, &evict);
    assert_eq!(probe_all(&fixture, query, &fetch).unwrap(), want);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1 + evicted,
        "the prefetch, then one refetch per evicted posting"
    );
    assert_eq!(held.load(Ordering::Relaxed), 0, "refetched spans released");
}

/// The backstop is bounded: past [`VECTOR_REFETCH_MAX`] refetches in one
/// prefetch's scan, a posting that is not local fails the probe as an IO
/// error, as before, instead of one serial fetch per posting.
#[test]
fn refetches_past_the_bound_fail_as_not_local() {
    let mut fixture = Fixture::new("refetch-bound");
    let query = &queries()[1];
    let ranges = spans_but_the_first(&mut fixture);
    assert!(ranges.len() > VECTOR_REFETCH_MAX + 1);
    let spans = span_cache::spans_dir(&fixture.dir, VECTOR_IDX_FILE);
    let held = Arc::new(AtomicU64::new(0));
    let calls = AtomicUsize::new(0);
    let evict_all = |call: usize| {
        if call == 0 {
            fs::remove_dir_all(&spans).unwrap();
        }
    };
    let fetch = held_fetch(&fixture.idx, &held, &calls, &evict_all);
    let err = probe_all(&fixture, query, &fetch).unwrap_err();
    assert!(
        matches!(&err, KoshaError::Io(e) if e.to_string().contains("is not local")),
        "{err}"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1 + VECTOR_REFETCH_MAX);
}

/// Decoded bytes of every posting a probe of `query` reads in a fixture of
/// `docs` vectors, and that probe's answer.
fn probe_footprint(docs: usize, query: &[f32]) -> (usize, Vec<(u32, u64)>) {
    let held = Arc::new(AtomicU64::new(0));
    let calls = AtomicUsize::new(0);
    let mut cal = Fixture::with_docs("pin-footprint", docs);
    cal.set_cache_budget(usize::MAX / 2);
    fs::remove_file(cal.dir.join(VECTOR_IDX_FILE)).unwrap();
    let fetch = held_fetch(&cal.idx, &held, &calls, &|_| {});
    let want = probe_all(&cal, query, &fetch).unwrap();
    probe_all(&cal, query, &fetch).unwrap();
    (cal.reader.vector_cache.resident_bytes_where(|_| true), want)
}

/// Finding 31 on a multi-thread pool: a probe of one segment large enough
/// to be split across threads (#484 scores a budgeted probe's postings in
/// runs of 16 on the current rayon pool), over a cache that holds half of
/// it, with held-only fetches. The probe's own admissions, inserted by
/// whichever run scans them, evict postings that the serial prefetch
/// skipped and that other runs have yet to scan; the pins must keep every
/// one of them readable for the whole probe, across all runs. On a branch
/// without the split this is the serial case on a pool thread.
#[test]
fn a_probe_on_a_thread_pool_reads_every_posting_its_prefetch_skipped() {
    const POOL_DOCS: usize = 8_000;
    let query = &queries()[1];
    let (total, want) = probe_footprint(POOL_DOCS, query);
    let mut fixture = Fixture::with_docs("pin-pool", POOL_DOCS);
    let postings = fixture.index().lazy.as_ref().unwrap().ranges.len();
    assert!(postings >= 64, "{postings} postings: too few to split");
    fixture.set_cache_budget(total / 2);
    fs::remove_file(fixture.dir.join(VECTOR_IDX_FILE)).unwrap();
    let held = Arc::new(AtomicU64::new(0));
    let calls = AtomicUsize::new(0);
    let fetch = held_fetch(&fixture.idx, &held, &calls, &|_| {});
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    for run in 0..4 {
        let before = calls.load(Ordering::Relaxed);
        let got = pool.install(|| probe_all(&fixture, query, &fetch));
        assert_eq!(got.unwrap_or_else(|e| panic!("run {run}: {e}")), want);
        assert!(
            calls.load(Ordering::Relaxed) - before <= 1,
            "run {run}: one prefetch, no refetch: the skipped postings were pinned"
        );
    }
    assert_eq!(held.load(Ordering::Relaxed), 0, "holds released");
}

/// Finding 31 across requests: several probes of one segment at once,
/// on their own threads, over a cache that holds a quarter of what one
/// reads,
/// with held-only fetches. Each one's admissions evict what the others'
/// prefetches skipped. Every probe must return its serial answer with one
/// prefetch and no refetch. A stress test: without the pins it fails on
/// about a third of runs here (the deterministic cases are the tests
/// above).
#[test]
fn concurrent_probes_evicting_each_others_skipped_postings_stay_exact() {
    let queries = queries();
    let wants: Vec<(usize, Vec<(u32, u64)>)> =
        queries.iter().map(|q| probe_footprint(DOCS, q)).collect();
    let total = wants.iter().map(|(bytes, _)| *bytes).max().unwrap();
    let mut fixture = Fixture::new("pin-concurrent");
    fixture.set_cache_budget(total / 4);
    fs::remove_file(fixture.dir.join(VECTOR_IDX_FILE)).unwrap();
    let held = Arc::new(AtomicU64::new(0));
    let calls = AtomicUsize::new(0);
    let fetch = held_fetch(&fixture.idx, &held, &calls, &|_| {});
    const ROUNDS: usize = 20;
    std::thread::scope(|scope| {
        for (query, (_, want)) in queries.iter().zip(&wants) {
            let (fixture, fetch) = (&fixture, &fetch);
            scope.spawn(move || {
                for round in 0..ROUNDS {
                    let got = probe_all(fixture, query, fetch);
                    assert_eq!(&got.unwrap_or_else(|e| panic!("round {round}: {e}")), want);
                }
            });
        }
    });
    assert!(
        calls.load(Ordering::Relaxed) <= queries.len() * ROUNDS,
        "one prefetch per probe, no refetch: the skipped postings were pinned"
    );
    assert_eq!(held.load(Ordering::Relaxed), 0, "holds released");
}
