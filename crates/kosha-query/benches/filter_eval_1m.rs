//! #244: filter evaluation on one 1M-document segment, `filters.bin` (v1,
//! row scan through `kosha_core::evaluate_filter`) vs columnar (v2, term
//! bitmaps, zone maps, presence bitmaps through `kosha_segment::filter_eval`).
//!
//! Times `SegmentReader::evaluate_filter`, which is what a segment's
//! filter-set cache calls on a miss: the cost of the first query with a
//! new filter on a segment, and of every delete-by-query. Both readers
//! are warmed first (filter columns / `filters.bin` parsed and cached), so
//! this measures evaluation only, not I/O or parsing.
//!
//! Also times a `terms` + `cardinality` aggregation over a filtered match
//! set end to end through `Searcher` (filter sets cached after the first
//! request), which on v2 reads the columns' ordinal and value arrays.
//!
//! Run:
//! ```text
//! cargo bench -p kosha-query --bench filter_eval_1m
//! KOSHA_BENCH_DOCS=200000 cargo bench -p kosha-query --bench filter_eval_1m
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

fn time_ms(iters: usize, mut f: impl FnMut()) -> f64 {
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        f();
        samples.push(t.elapsed().as_secs_f64() * 1e3);
    }
    median_ms(samples)
}

fn clause(json: serde_json::Value) -> FilterClause {
    serde_json::from_value(json).unwrap()
}

fn main() {
    let docs = env_usize("KOSHA_BENCH_DOCS", 1_000_000);
    let iters = env_usize("KOSHA_BENCH_ITERS", 15);
    let work = std::env::temp_dir().join("kosha-bench-filter-eval-1m");
    let _ = std::fs::remove_dir_all(&work);

    let ids: Vec<String> = (0..50).map(|i| format!("doc-{:07}", i * 19_997)).collect();
    let cases: Vec<(&str, FilterClause)> = vec![
        (
            "term   matterId (1%)",
            clause(serde_json::json!({"term": {"matterId": "matter-42"}})),
        ),
        (
            "term   custodian (5%)",
            clause(serde_json::json!({"term": {"custodian": "custodian-7"}})),
        ),
        (
            "term   documentId (1 doc)",
            clause(serde_json::json!({"term": {"documentId": "doc-0123456"}})),
        ),
        (
            "terms  matterId x10 (10%)",
            clause(serde_json::json!({"terms": {"matterId":
                (0..10).map(|i| format!("matter-{i}")).collect::<Vec<_>>()}})),
        ),
        (
            "terms  documentId x50",
            clause(serde_json::json!({"terms": {"documentId": ids}})),
        ),
        (
            "range  page int (1%, unsorted)",
            clause(serde_json::json!({"range": {"page": {"gte": "100", "lt": "200"}}})),
        ),
        (
            "range  createdMs int (1%, zone-pruned)",
            clause(serde_json::json!({"range": {"createdMs":
                {"gte": (docs as i64 / 2).to_string(), "lt": (docs as i64 / 2 + docs as i64 / 100).to_string()}}})),
        ),
        (
            "range  confidence float (10%)",
            clause(serde_json::json!({"range": {"confidence": {"gte": "0.5", "lt": "0.6"}}})),
        ),
        (
            "range  createdAt date string (1 month)",
            clause(
                serde_json::json!({"range": {"createdAt": {"gte": "2024-03-01", "lt": "2024-04-01"}}}),
            ),
        ),
        (
            "bool   term & !term & range",
            clause(serde_json::json!({"bool": {
                "must": [{"term": {"custodian": "custodian-3"}},
                         {"range": {"page": {"lt": "5000"}}}],
                "must_not": [{"term": {"inScope": "false"}}]}})),
        ),
    ];

    let mut table: Vec<(String, f64, f64)> = Vec::new();
    let mut agg_ms = Vec::new();
    for version in [1u32, 2] {
        let dir = work.join(format!("v{version}"));
        let seg_dir = dir.join("bench").join("s0");
        let t = Instant::now();
        build(&seg_dir, version, docs);
        let bytes: u64 = std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("filters"))
            .map(|e| e.metadata().unwrap().len())
            .sum();
        println!(
            "  v{version}: built {docs} docs in {:.1}s, filter files {:.1} MB",
            t.elapsed().as_secs_f64(),
            bytes as f64 / 1e6
        );
        let reader = SegmentReader::open(seg_dir.clone()).unwrap();
        for (i, (label, c)) in cases.iter().enumerate() {
            // Warm: parse/hydrate whatever the format reads.
            let n = reader.evaluate_filter(c).unwrap().count();
            let ms = time_ms(iters, || {
                black_box(reader.evaluate_filter(c).unwrap());
            });
            if version == 1 {
                table.push((format!("{label} [{n} docs]"), ms, 0.0));
            } else {
                assert_eq!(
                    table[i].0,
                    format!("{label} [{n} docs]"),
                    "formats disagree"
                );
                table[i].2 = ms;
            }
        }

        // Aggregations through the search path.
        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s0".into()),
                doc_count: docs as u32,
            }],
            segment_footers: Default::default(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            schema: None,
            retired: Default::default(),
            shards: None,
            branch: None,
        };
        let query: SearchQuery = serde_json::from_value(serde_json::json!({
            "query_text": "", "max_results": 0,
            "filter": {"terms": {"matterId": (0..10).map(|i| format!("matter-{i}")).collect::<Vec<_>>()}},
            "aggs": {"by_custodian": {"terms": {"field": "custodian", "size": 10}},
                     "pages": {"cardinality": {"field": "page"}}},
            "exact_total_hits": true
        }))
        .unwrap();
        let searcher = Searcher::new(dir.clone());
        let ns = NamespaceId("bench".into());
        black_box(searcher.search(&ns, &manifest, &query, None).unwrap());
        agg_ms.push(time_ms(iters.min(7), || {
            black_box(searcher.search(&ns, &manifest, &query, None).unwrap());
        }));
    }

    println!("\n  filter evaluation on one {docs}-doc segment (median of {iters}, warm, uncached)");
    println!(
        "  {:<52} {:>12} {:>12} {:>9}",
        "case", "v1 rows ms", "v2 cols ms", "speedup"
    );
    for (label, v1, v2) in &table {
        println!("  {label:<52} {v1:>12.3} {v2:>12.3} {:>8.1}x", v1 / v2);
    }
    println!(
        "  {:<52} {:>12.3} {:>12.3} {:>8.1}x",
        "search: terms+cardinality aggs over a 10% filter",
        agg_ms[0],
        agg_ms[1],
        agg_ms[0] / agg_ms[1]
    );
    let _ = std::fs::remove_dir_all(&work);
}

fn build(seg_dir: &std::path::Path, version: u32, docs: usize) {
    let mut w = SegmentWriter::new(SegmentId("s0".into()), seg_dir.to_path_buf())
        .with_filters_write_version(version);
    for d in 0..docs {
        // A cheap pseudo-random permutation, so `page` and `matterId` are
        // spread across the segment rather than sorted.
        let h = (d as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32;
        w.add_document(
            DocumentId(format!("d{d}")),
            vec![
                Field::text("t", "x"),
                Field::keyword("matterId", format!("matter-{}", h % 100)),
                Field::keyword("custodian", format!("custodian-{}", h % 20)),
                Field::keyword("documentId", format!("doc-{d:07}")),
                Field::boolean("inScope", !h.is_multiple_of(3)),
                Field::date_val(
                    "createdAt",
                    format!("2024-{:02}-{:02}", h % 12 + 1, h % 28 + 1),
                ),
                Field::integer("page", (h % 10_000) as i64),
                // Ingest-ordered, like a timestamp: zone maps prune it.
                Field::integer("createdMs", d as i64),
                Field::float_val("confidence", (h % 1000) as f64 / 1000.0),
            ],
        );
    }
    w.finalize(Bm25Params::default()).unwrap();
}
