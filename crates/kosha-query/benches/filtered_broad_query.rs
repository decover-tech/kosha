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
//! #244: also builds the corpus in both filter formats (`filters.bin` and
//! the columnar `filters.toc` + `filters-NNN.col`) and reports the filter
//! bytes the query has to hydrate in each. Every document carries several
//! other filterable fields the query never names, as production documents
//! do; with columns the query reads only the TOC and the `custodian`
//! column. The columnar run deletes every other column file first, so it
//! also proves nothing else is read.
//!
//! #244 part 2: also reports the filter's evaluation cost per format, the
//! work a segment's filter-set cache does on a miss (the first request with
//! a new filter): a row scan of `filters.bin` vs the columnar term bitmap.
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
use kosha_segment::{SegmentReader, SegmentWriter};

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
    for (label, version) in [("filters.bin (v1)", 1u32), ("columnar (v2)", 2)] {
        run(
            &work.join(format!("v{version}")),
            label,
            version,
            segs,
            docs,
            iters,
        );
    }
}

fn run(work: &std::path::Path, label: &str, version: u32, segs: usize, docs: usize, iters: usize) {
    let ns = NamespaceId("bench".into());
    let mut entries = Vec::with_capacity(segs);
    let mut footers = Vec::with_capacity(segs);
    for s in 0..segs {
        let seg_id = SegmentId(format!("s{s}"));
        let seg_dir = work.join(&ns.0).join(&seg_id.0);
        let mut w = SegmentWriter::new(seg_id.clone(), seg_dir).with_filters_write_version(version);
        for d in 0..docs {
            // Every doc matches "the" (broad term) and passes the filter
            // (same custodian value everywhere) — forces the general path
            // to build the full per-segment match set before any cut.
            w.add_document(
                DocumentId(format!("s{s}-d{d}")),
                vec![
                    Field::text("t", format!("the contract dispute paragraph {d}")),
                    Field::keyword("custodian", "alice"),
                    // Filterable fields this query never names.
                    Field::text("documentId", format!("doc-{s}-{}", d / 8)),
                    Field::text(
                        "path",
                        format!("/matters/m{}/folder{}/file{d}.pdf", d % 7, d % 31),
                    ),
                    Field::keyword("matterId", format!("matter-{}", d % 7)),
                    Field::boolean("inScope", d % 3 != 0),
                    Field::date_val(
                        "createdAt",
                        format!("2024-{:02}-{:02}", d % 12 + 1, d % 28 + 1),
                    ),
                    Field::integer("page", (d % 400) as i64),
                    Field::float_val("confidence", (d % 100) as f64 / 100.0),
                ],
            );
        }
        footers.push(w.finalize(Bm25Params::default()).unwrap());
        entries.push(ManifestEntry {
            segment_id: seg_id,
            doc_count: docs as u32,
        });
    }
    let manifest = Manifest {
        version: 1,
        segments: entries,
        segment_footers: Default::default(),
        footer_crc32: Default::default(),
        wal_checkpoint: 0,
        wal_origin: None,
        tombstones: Default::default(),
        deletion_vectors: Default::default(),
    };
    let query = bench_query();

    // Filter bytes a query node hydrates for this query: everything the
    // footer says the query's fields need. Columnar: delete the rest, so
    // the timed search below proves it reads nothing else.
    let fields = query.filter_column_fields();
    let (mut needed, mut all) = (0u64, 0u64);
    for (entry, footer) in manifest.segments.iter().zip(&footers) {
        let dir = work.join(&ns.0).join(&entry.segment_id.0);
        let wanted = footer.filter_files_for(Some(&fields));
        for file in footer.filter_files_for(None) {
            let len = std::fs::metadata(dir.join(&file)).unwrap().len();
            all += len;
            if wanted.contains(&file) {
                needed += len;
            } else {
                std::fs::remove_file(dir.join(&file)).unwrap();
            }
        }
    }
    println!(
        "\n  [{label}] filter bytes hydrated for the query: {needed} of {all} ({:.1}%) across {segs} segments",
        needed as f64 * 100.0 / all.max(1) as f64
    );

    // Uncached filter evaluation, summed over the segments: what each
    // segment's filter-set cache pays on a miss.
    let filter = query.filter.clone().unwrap();
    let readers: Vec<SegmentReader> = manifest
        .segments
        .iter()
        .map(|e| SegmentReader::open(work.join(&ns.0).join(&e.segment_id.0)).unwrap())
        .collect();
    for r in &readers {
        black_box(r.evaluate_filter(&filter).unwrap()); // parse / decode once
    }
    let mut eval = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        for r in &readers {
            black_box(r.evaluate_filter(&filter).unwrap());
        }
        eval.push(t.elapsed().as_secs_f64() * 1e3);
    }
    println!(
        "  uncached filter evaluation, all {segs} segments: median {:.3} ms",
        median_ms(eval)
    );
    drop(readers);

    let searcher = Searcher::new(work.to_path_buf());

    // Warm the segment cache.
    let (warm, _) = searcher
        .search_with_stats(&ns, &manifest, &query, None)
        .unwrap();
    assert!(warm.total_hits > 0, "the filter must match");
    println!(
        "  {} segs × {} docs = {} total, total_hits = {}, page = {}",
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

fn bench_query() -> SearchQuery {
    SearchQuery {
        query_text: "the".into(),
        max_results: 10,
        from: 0,
        bm25_params: Default::default(),
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
        exact_total_hits: None,
        total_hits_cap: None,
        operator: None,
        no_cache: None,
        collapse: None,
        source_includes: None,
        source_excludes: None,
    }
}
