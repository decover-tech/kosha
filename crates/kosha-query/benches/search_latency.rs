//! End-to-end warm-cache BM25 latency for issue #37.
//!
//! Builds a single in-memory-cached segment with ~25k matching docs (mirrors
//! the staging `paragraph_index_hnsw` "contract" hit count), then times
//! `Searcher::search` with `max_results: 5`.
//!
//! Before / after comparison (deferred field materialization):
//! ```text
//! # on the pre-fix tree:
//! cargo bench -p kosha-query --bench search_latency -- --save-baseline before
//! # on the post-fix tree:
//! cargo bench -p kosha-query --bench search_latency -- --baseline before
//! ```

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
    };
    (ns, manifest)
}

fn mk_query() -> SearchQuery {
    SearchQuery {
        query_text: "contract".into(),
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

fn bench_search_latency(c: &mut Criterion) {
    let dir = std::env::temp_dir().join("kosha-bench-search-latency-e2e");
    let (ns, manifest) = build_corpus(&dir);
    let searcher = Searcher::new(dir.clone());
    let query = mk_query();

    // Warm the in-memory segment cache so we measure compute, not disk I/O.
    let warm = searcher.search(&ns, &manifest, &query, None).unwrap();
    assert_eq!(warm.total_hits, HIT_COUNT);
    assert_eq!(warm.results.len(), PAGE_SIZE);

    let mut group = c.benchmark_group("issue37_e2e_search");
    group.throughput(Throughput::Elements(HIT_COUNT as u64));
    group.sample_size(30);
    group.bench_function("warm_bm25_25053_hits_page5", |b| {
        b.iter(|| {
            let r = searcher
                .search(
                    black_box(&ns),
                    black_box(&manifest),
                    black_box(&query),
                    None,
                )
                .unwrap();
            black_box(r.total_hits);
            black_box(r.results.len());
        })
    });
    group.finish();

    let _ = std::fs::remove_dir_all(&dir);
}

criterion_group!(benches, bench_search_latency);
criterion_main!(benches);
