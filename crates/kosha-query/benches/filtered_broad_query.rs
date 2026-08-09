//! End-to-end before/after for the general-path lazy-`DocumentId` fix:
//! a broad single-term query *with a filter clause* — `query.filter.is_some()`
//! disqualifies the block-max WAND gate outright, so this always takes
//! `score_segment`'s general path, with tens of thousands of per-segment
//! matches funneling into a 10-doc page. This is the realistic shape the
//! isolated `general_path_docid` microbench predicted a win for (unlike
//! `segment_memory`'s wildcard case, which is dominated by per-matched-term
//! postings decode across many expanded terms, not by candidate
//! materialization).
//!
//! Run:
//! ```text
//! cargo bench -p kosha-query --bench filtered_broad_query --release
//! ```

use std::hint::black_box;
use std::time::Instant;

use kosha_core::{
    Bm25Params, DocumentId, Field, FilterClause, Manifest, ManifestEntry, NamespaceId, SearchQuery,
    SegmentId,
};
use kosha_query::Searcher;
use kosha_segment::SegmentWriter;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn median_ms(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let segs = env_usize("KOSHA_BENCH_SEGS", 8);
    let docs = env_usize("KOSHA_BENCH_DOCS", 4000);
    let iters = 10;

    let work = std::env::temp_dir().join("kosha-bench-filtered-broad-query");
    let _ = std::fs::remove_dir_all(&work);
    let ns = NamespaceId("bench".into());
    let mut entries = Vec::with_capacity(segs);
    for s in 0..segs {
        let seg_id = SegmentId(format!("s{s}"));
        let seg_dir = work.join(&ns.0).join(&seg_id.0);
        let mut w = SegmentWriter::new(seg_id.clone(), seg_dir);
        for d in 0..docs {
            // Every doc matches "the" (broad term) and passes the filter
            // (same custodian value everywhere) — forces the general path
            // to build the full per-segment match set before any cut.
            w.add_document(
                DocumentId(format!("s{s}-d{d}")),
                vec![
                    Field::text("t", format!("the contract dispute paragraph {d}")),
                    Field::keyword("custodian", "alice"),
                ],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();
        entries.push(ManifestEntry {
            segment_id: seg_id,
            doc_count: docs as u32,
        });
    }
    let manifest = Manifest {
        version: 1,
        segments: entries,
        segment_footers: Default::default(),
    };
    let searcher = Searcher::new(work);

    let query = SearchQuery {
        query_text: "the".into(),
        max_results: 10,
        from: 0,
        bm25_params: Bm25Params::default(),
        filter: Some(FilterClause::Term {
            term: std::collections::HashMap::from([("custodian".into(), "alice".into())]),
        }),
        sort: vec![],
        search_after: None,
        highlight: None,
        aggs: Default::default(),
        wildcard: None,
        match_phrase: None,
        knn: None,
    };

    // Warm the segment cache.
    let (warm, _) = searcher
        .search_with_stats(&ns, &manifest, &query, None)
        .unwrap();
    println!(
        "\n  {} segs × {} docs = {} total, total_hits = {}, page = {}",
        segs,
        docs,
        segs * docs,
        warm.total_hits,
        warm.results.len()
    );

    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        let (r, _) = searcher
            .search_with_stats(&ns, &manifest, &query, None)
            .unwrap();
        black_box(&r);
        samples.push(t.elapsed().as_secs_f64() * 1e3);
    }
    println!(
        "  warm filtered-broad-query (\"the\" + filter): median {:.3} ms (min {:.3}, max {:.3}, n={})",
        median_ms(samples.clone()),
        samples.iter().cloned().fold(f64::INFINITY, f64::min),
        samples.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        samples.len()
    );
}
