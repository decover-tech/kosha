//! End-to-end warm-cache BM25 latency for issue #37.
//!
//! Builds a single in-memory-cached segment with ~25k matching docs (mirrors
//! the staging `paragraph_index_hnsw` "contract" hit count), then times
//! `Searcher::search` with `max_results: 5`.
//!
//! ## Before / after validation
//!
//! Criterion baselines are the source of truth for improvement claims:
//! ```text
//! # on the pre-change tree (or after temporarily reverting the fix):
//! cargo bench -p kosha-query --bench search_latency -- --save-baseline before
//! # on the post-change tree:
//! cargo bench -p kosha-query --bench search_latency -- --baseline before
//! ```
//! Criterion prints `change: time: [lo% mid% hi%]` — require a statistically
//! significant improvement (`Performance has improved`) before landing.
//!
//! Validated improvements (Criterion `--baseline`, p < 0.05):
//! - deferred materialization + `select_nth` (`warm_bm25_25053_hits_page5`):
//!   ~28.5 ms → ~2.5 ms (**−91%**)
//! - AND HashMap posting lookup (`warm_bm25_and_25053_hits_page5`):
//!   ~209 ms → ~5.2 ms (**−97%**)
//! - term-bloom multi-segment skip (`bm25_term_bloom_20segs_cache1`,
//!   cache capacity 1): ~7.6 ms → ~0.41 ms (**−95%**)
//! - parallel segment scoring, 11-core machine (`bm25_broad_query_53segs_all_match`
//!   — 53 segments, none bloom-prunable, all resident): ~6.66 ms → ~2.73 ms
//!   (**−58%**, p < 0.05)
//!
//! For phase-level remaining-cost analysis see `scoring_profile`.

use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use kosha_core::{
    Bm25Params, DocumentId, Field, Manifest, ManifestEntry, NamespaceId, SearchQuery, SegmentId,
};
use kosha_query::Searcher;
use kosha_segment::SegmentWriter;

/// Hit count from the live staging benchmark in issue #37.
const HIT_COUNT: usize = 25_053;
const PAGE_SIZE: usize = 5;

fn build_corpus(dir: &PathBuf) -> (NamespaceId, Manifest) {
    let _ = std::fs::remove_dir_all(dir);
    let ns = NamespaceId("bench".into());
    let seg_dir = dir.join(&ns.0).join("s1");
    let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir);

    for i in 0..HIT_COUNT {
        // Every doc matches "contract"; content sized like a paragraph + metadata.
        let content = format!(
            "contract dispute paragraph {i}: breach of warranty, indemnity, \
             and termination. {}",
            "lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(20)
        );
        w.add_document(
            DocumentId(format!("id-{i}")),
            vec![
                Field::text("content", content),
                Field::keyword("custodian", format!("user-{}", i % 50)),
                Field::keyword("documentId", format!("doc-{}", i / 3)),
                Field::keyword("bates", format!("BATES{i:08}")),
                Field::date_val("date", "2024-06-15T00:00:00Z"),
                Field::keyword("matterId", format!("matter-{}", i % 10)),
            ],
        );
    }
    w.finalize(Bm25Params::default()).unwrap();

    let manifest = Manifest {
        version: 1,
        segments: vec![ManifestEntry {
            segment_id: SegmentId("s1".into()),
            doc_count: HIT_COUNT as u32,
        }],
        segment_footers: Default::default(),
    };
    (ns, manifest)
}

fn mk_query(text: &str) -> SearchQuery {
    SearchQuery {
        query_text: text.into(),
        max_results: PAGE_SIZE,
        from: 0,
        bm25_params: Bm25Params::default(),
        filter: None,
        sort: vec![],
        search_after: None,
        highlight: None,
        aggs: HashMap::new(),
        wildcard: None,
        match_phrase: None,
        knn: None,
    }
}

/// Many segments, only one contains the rare query term — term blooms should
/// skip opening the rest.
const TERM_BLOOM_SEGS: usize = 20;
const TERM_BLOOM_DOCS_PER_SEG: usize = 500;

fn build_term_bloom_corpus(dir: &PathBuf) -> (NamespaceId, Manifest) {
    let _ = std::fs::remove_dir_all(dir);
    let ns = NamespaceId("bench-term-bloom".into());
    let mut entries = Vec::with_capacity(TERM_BLOOM_SEGS);
    for s in 0..TERM_BLOOM_SEGS {
        let seg_id = format!("s{s}");
        let seg_dir = dir.join(&ns.0).join(&seg_id);
        let mut w = SegmentWriter::new(SegmentId(seg_id.clone()), seg_dir);
        for i in 0..TERM_BLOOM_DOCS_PER_SEG {
            let content = if s == 0 {
                format!("raretermxyz paragraph {i} with shared padding text")
            } else {
                format!("ordinary vocabulary paragraph {i} segment {s} padding text")
            };
            w.add_document(
                DocumentId(format!("s{s}-d{i}")),
                vec![Field::text("content", content)],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();
        entries.push(ManifestEntry {
            segment_id: SegmentId(seg_id),
            doc_count: TERM_BLOOM_DOCS_PER_SEG as u32,
        });
    }
    (
        ns,
        Manifest {
            version: 1,
            segments: entries,
            segment_footers: Default::default(),
        },
    )
}

/// Mirrors `paragraph_index_hnsw`'s shape in staging (53 segments) with a
/// query term present in every segment, so nothing is bloom-prunable and
/// every segment must actually be opened and scored — the case parallel
/// segment scoring targets. Each segment gets enough docs that per-segment
/// scoring cost is non-trivial; a purely sequential loop pays for all 53
/// segments back-to-back on one core, while `par_iter` spreads them across
/// the machine's cores.
const BROAD_QUERY_SEGS: usize = 53;
const BROAD_QUERY_DOCS_PER_SEG: usize = 1_000;

fn build_broad_query_corpus(dir: &PathBuf) -> (NamespaceId, Manifest) {
    let _ = std::fs::remove_dir_all(dir);
    let ns = NamespaceId("bench-broad-query".into());
    let mut entries = Vec::with_capacity(BROAD_QUERY_SEGS);
    for s in 0..BROAD_QUERY_SEGS {
        let seg_id = format!("s{s}");
        let seg_dir = dir.join(&ns.0).join(&seg_id);
        let mut w = SegmentWriter::new(SegmentId(seg_id.clone()), seg_dir);
        for i in 0..BROAD_QUERY_DOCS_PER_SEG {
            let content = format!(
                "contract dispute paragraph {i} in segment {s}: breach of \
                 warranty, indemnity, and termination. {}",
                "lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(20)
            );
            w.add_document(
                DocumentId(format!("s{s}-d{i}")),
                vec![Field::text("content", content)],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();
        entries.push(ManifestEntry {
            segment_id: SegmentId(seg_id),
            doc_count: BROAD_QUERY_DOCS_PER_SEG as u32,
        });
    }
    (
        ns,
        Manifest {
            version: 1,
            segments: entries,
            segment_footers: Default::default(),
        },
    )
}

fn bench_search_latency(c: &mut Criterion) {
    let dir = std::env::temp_dir().join("kosha-bench-search-latency-e2e");
    let (ns, manifest) = build_corpus(&dir);
    let searcher = Searcher::new(dir.clone());
    let single = mk_query("contract");
    // Multi-term AND — every doc contains both tokens, so intersection ≈ HIT_COUNT.
    // This is the path that previously re-scanned postings with `.find()`.
    let and_query = mk_query("contract dispute");

    // Warm the in-memory segment cache so we measure compute, not disk I/O.
    let warm = searcher.search(&ns, &manifest, &single, None).unwrap();
    assert_eq!(warm.total_hits, HIT_COUNT);
    assert_eq!(warm.results.len(), PAGE_SIZE);
    let warm_and = searcher.search(&ns, &manifest, &and_query, None).unwrap();
    assert_eq!(warm_and.total_hits, HIT_COUNT);
    assert_eq!(warm_and.results.len(), PAGE_SIZE);

    let mut group = c.benchmark_group("issue37_e2e_search");
    group.throughput(Throughput::Elements(HIT_COUNT as u64));
    group.sample_size(30);
    group.bench_function("warm_bm25_25053_hits_page5", |b| {
        b.iter(|| {
            let r = searcher
                .search(
                    black_box(&ns),
                    black_box(&manifest),
                    black_box(&single),
                    None,
                )
                .unwrap();
            black_box(r.total_hits);
            black_box(r.results.len());
        })
    });
    group.bench_function("warm_bm25_and_25053_hits_page5", |b| {
        b.iter(|| {
            let r = searcher
                .search(
                    black_box(&ns),
                    black_box(&manifest),
                    black_box(&and_query),
                    None,
                )
                .unwrap();
            black_box(r.total_hits);
            black_box(r.results.len());
        })
    });
    group.finish();

    let _ = std::fs::remove_dir_all(&dir);

    // ── Term-bloom multi-segment skip ────────────────────────────────────
    // Cache holds only one parsed segment so a no-prune baseline must
    // re-parse every segment each query — matching the "can't keep the whole
    // namespace resident" case term blooms are meant to fix. (A fully warm
    // in-memory cache of all segments would make footer reads look slower
    // than free Arc clones, which is the wrong comparison.)
    let tb_dir = std::env::temp_dir().join("kosha-bench-search-latency-term-bloom");
    let (tb_ns, tb_manifest) = build_term_bloom_corpus(&tb_dir);
    let tb_searcher = Searcher::with_segment_cache_limits(tb_dir.clone(), 1, u64::MAX);
    let tb_query = mk_query("raretermxyz");
    let tb_warm = tb_searcher
        .search(&tb_ns, &tb_manifest, &tb_query, None)
        .unwrap();
    assert_eq!(tb_warm.total_hits, TERM_BLOOM_DOCS_PER_SEG);

    let mut tb_group = c.benchmark_group("term_bloom_multiseg");
    tb_group.throughput(Throughput::Elements(
        (TERM_BLOOM_SEGS * TERM_BLOOM_DOCS_PER_SEG) as u64,
    ));
    tb_group.sample_size(30);
    tb_group.bench_function("bm25_term_bloom_20segs_cache1", |b| {
        b.iter(|| {
            let r = tb_searcher
                .search(
                    black_box(&tb_ns),
                    black_box(&tb_manifest),
                    black_box(&tb_query),
                    None,
                )
                .unwrap();
            black_box(r.total_hits);
            black_box(r.results.len());
        })
    });
    tb_group.finish();

    let _ = std::fs::remove_dir_all(&tb_dir);

    // ── Broad query across every segment (parallel segment scoring) ──────
    // Cache holds all 53 segments so we measure open+score compute, not
    // disk I/O — this is the "every segment matches, nothing is
    // bloom-prunable" case a wide/common query hits in production.
    let bq_dir = std::env::temp_dir().join("kosha-bench-search-latency-broad-query");
    let (bq_ns, bq_manifest) = build_broad_query_corpus(&bq_dir);
    let bq_searcher = Searcher::with_segment_cache_capacity(bq_dir.clone(), BROAD_QUERY_SEGS + 1);
    let bq_query = mk_query("contract");
    let bq_warm = bq_searcher
        .search(&bq_ns, &bq_manifest, &bq_query, None)
        .unwrap();
    assert_eq!(
        bq_warm.total_hits,
        BROAD_QUERY_SEGS * BROAD_QUERY_DOCS_PER_SEG
    );

    let mut bq_group = c.benchmark_group("broad_query_multiseg");
    bq_group.throughput(Throughput::Elements(
        (BROAD_QUERY_SEGS * BROAD_QUERY_DOCS_PER_SEG) as u64,
    ));
    bq_group.sample_size(30);
    bq_group.bench_function("bm25_broad_query_53segs_all_match", |b| {
        b.iter(|| {
            let r = bq_searcher
                .search(
                    black_box(&bq_ns),
                    black_box(&bq_manifest),
                    black_box(&bq_query),
                    None,
                )
                .unwrap();
            black_box(r.total_hits);
            black_box(r.results.len());
        })
    });
    bq_group.finish();

    let _ = std::fs::remove_dir_all(&bq_dir);
}

criterion_group!(benches, bench_search_latency);
criterion_main!(benches);
