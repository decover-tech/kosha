//! Research #387: the offline two-tier analysis must (a) score exactly as
//! the engine does — its exhaustive oracle matches `Searcher` OR top-k
//! under namespace-level statistics — and (b) never certify a head-only
//! answer that differs from the oracle, for any head fraction or bound.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use kosha_core::{
    Bm25Params, DocumentId, Field, Manifest, ManifestEntry, NamespaceId, QueryOperator,
    SearchQuery, SegmentId,
};
use kosha_query::two_tier::{self, Config, StatsScope};
use kosha_query::Searcher;
use kosha_segment::span_cache::{PendingSpan, SpanHolds};
use kosha_segment::{SegmentWriter, TIER_TAIL_FILE};

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
    fn unit(&mut self) -> f64 {
        self.next() as f64 / (1u64 << 53) as f64
    }
    /// Zipf-ish rank in `0..n`.
    fn zipf(&mut self, n: usize) -> usize {
        let u = self.unit();
        ((u * u * u) * n as f64) as usize
    }
}

const VOCAB: usize = 3000;

/// Segments of different sizes and length distributions, so segment-local
/// and namespace statistics really differ.
fn build(root: &Path, ns: &NamespaceId, sizes: &[usize]) -> Manifest {
    let _ = std::fs::remove_dir_all(root);
    let mut entries = Vec::new();
    for (s, &docs) in sizes.iter().enumerate() {
        let id = SegmentId(format!("seg{s}"));
        let mut w = SegmentWriter::new(id.clone(), root.join(&ns.0).join(&id.0));
        let mut rng = Lcg(0xC0FFEE + s as u64 * 7919);
        for d in 0..docs {
            let len = 5 + (rng.unit() * (40.0 + 60.0 * s as f64)) as usize;
            let words: Vec<String> = (0..len).map(|_| format!("w{}", rng.zipf(VOCAB))).collect();
            w.add_document(
                DocumentId(format!("s{s}-d{d}")),
                vec![Field::text("t", words.join(" "))],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();
        entries.push(ManifestEntry {
            segment_id: id,
            doc_count: docs as u32,
        });
    }
    Manifest {
        version: 1,
        segments: entries,
        segment_footers: Default::default(),
        footer_crc32: Default::default(),
        wal_checkpoint: 0,
        wal_origin: None,
        tombstones: Default::default(),
        deletion_vectors: Default::default(),
        branch: None,
        schema: None,
        retired: Default::default(),
        shards: None,
    }
}

fn queries(n: usize) -> Vec<String> {
    let mut rng = Lcg(42);
    (0..n)
        .map(|i| {
            let len = 1 + i % 12;
            (0..len)
                .map(|_| format!("w{}", rng.zipf(VOCAB)))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

#[test]
fn tiered_search_certifies_or_fetches_and_matches_full_postings() {
    let root = std::env::temp_dir().join(format!("kosha-tiered-live-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let full = root.join("full");
    let tiered = root.join("tiered");
    let ns = NamespaceId("tt".into());
    let mut manifest = build(&full, &ns, &[]);
    for s in 0..3 {
        let id = SegmentId(format!("seg{s}"));
        let version = if s == 2 { 6 } else { 5 };
        let docs = 160 + s * 10;
        for (dir, use_tier) in [(&full, false), (&tiered, s == 0)] {
            let writer = SegmentWriter::new(id.clone(), dir.join(&ns.0).join(&id.0))
                .with_postings_write_version(version);
            let mut writer = if use_tier {
                writer
                    .with_two_tier_head_fraction(0.25)
                    .with_build_memory_bytes(8192)
            } else {
                writer
            };
            for d in 0..docs {
                let strong_count = if d < 10 { 25 + d } else { 1 };
                let text = format!(
                    "{}flat {}",
                    "strong ".repeat(strong_count),
                    "filler ".repeat(s)
                );
                writer.add_document(
                    DocumentId(format!("s{s}-d{d:03}")),
                    vec![Field::text("t", text)],
                );
            }
            writer.finalize(Bm25Params::default()).unwrap();
        }
        manifest.segments.push(ManifestEntry {
            segment_id: id,
            doc_count: docs as u32,
        });
    }

    let seg = tiered.join(&ns.0).join("seg0");
    let version = std::fs::read(seg.join("inverted.idx")).unwrap();
    assert_eq!(&version[4..8], &7u32.to_le_bytes());
    assert!(kosha_segment::tier_files_present(&seg));
    let remote = std::fs::read(seg.join(TIER_TAIL_FILE)).unwrap();
    std::fs::remove_file(seg.join(TIER_TAIL_FILE)).unwrap();
    assert!(!kosha_segment::tier_files_present(&seg));
    let fetched = AtomicUsize::new(0);
    let fetch = |dir: &Path, file: &str, ranges: &[(u64, u64)]| -> SpanHolds {
        assert_eq!(dir, seg);
        assert_eq!(file, TIER_TAIL_FILE);
        fetched.fetch_add(ranges.len(), Ordering::Relaxed);
        ranges
            .iter()
            .map(|&(start, end)| {
                PendingSpan::register(
                    dir,
                    file,
                    start,
                    remote[start as usize..end as usize].to_vec(),
                    false,
                )
            })
            .collect()
    };

    let oracle = Searcher::new(full);
    let exact = Searcher::new(tiered.clone()).with_two_tier_exact(true);
    let off = Searcher::new(tiered).with_two_tier_exact(false);
    let scores = |r: &kosha_core::SearchResult| {
        r.results
            .iter()
            .map(|hit| (hit.doc_id.0.clone(), hit.score.to_bits()))
            .collect::<Vec<_>>()
    };
    for (searcher, text, exact_count, fetch_count) in [
        (&exact, "strong", true, 0),
        (&exact, "strong strong", false, 0),
        (&exact, "strong missingword", false, 0),
        (&exact, "flat", false, 1),
        (&exact, "strong flat", false, 2),
        (&off, "strong", true, 1),
    ] {
        let query: SearchQuery = serde_json::from_value(serde_json::json!({
            "query_text": text,
            "max_results": 10,
            "operator": QueryOperator::Or,
            "exact_total_hits": exact_count,
            "total_hits_cap": 15,
        }))
        .unwrap();
        let expected = oracle.search(&ns, &manifest, &query, None).unwrap();
        let (actual, _) = searcher
            .search_with_hydrators(&ns, &manifest, &query, None, None, Some(&fetch))
            .unwrap();
        assert_eq!(fetched.swap(0, Ordering::Relaxed), fetch_count, "{text}");
        assert_eq!(scores(&actual), scores(&expected), "{text}");
        assert_eq!(actual.total_hits, expected.total_hits, "{text}");
        assert_eq!(
            actual.total_hits_relation, expected.total_hits_relation,
            "{text}"
        );
    }

    let needs_tail: SearchQuery = serde_json::from_value(serde_json::json!({
        "query_text": "flat", "max_results": 10,
    }))
    .unwrap();
    let cold = Searcher::new(root.join("tiered")).with_two_tier_exact(true);
    assert!(cold.search(&ns, &manifest, &needs_tail, None).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn oracle_matches_engine_and_certification_is_sound() {
    let root = std::env::temp_dir().join(format!("kosha-two-tier-{}", std::process::id()));
    let ns = NamespaceId("tt".into());
    let manifest = build(&root, &ns, &[1500, 700, 2500]);
    let dirs: Vec<_> = manifest
        .segments
        .iter()
        .map(|e| root.join(&ns.0).join(&e.segment_id.0))
        .collect();
    let segments = two_tier::open_segments(&dirs).unwrap();
    let qs = queries(120);
    let config = Config {
        head_fractions: vec![0.05, 0.1, 0.3, 1.0],
        k: 10,
        min_split_df: 16,
        stats: StatsScope::Namespace,
    };
    let (report, rows) = two_tier::analyze_queries(&segments, config, &qs).unwrap();
    assert_eq!(report.queries_analyzed, qs.len());
    assert_eq!(report.segments_skipped_multi_reading, 0);
    assert_eq!(report.queries_skipped_time_budget, 0);
    assert_eq!(report.split_segments, 3);
    assert!(report.fixed_local_bytes > 0, "{:?}", report.local_files);

    // (a) The oracle is the engine's answer.
    let searcher = Searcher::new(root.clone());
    for row in &rows {
        let query: SearchQuery = serde_json::from_value(serde_json::json!({
            "query_text": qs[row.query],
            "max_results": 10,
            "operator": QueryOperator::Or,
        }))
        .unwrap();
        let result = searcher.search(&ns, &manifest, &query, None).unwrap();
        let engine: Vec<(String, f64)> = result
            .results
            .iter()
            .map(|d| (d.doc_id.0.clone(), d.score))
            .collect();
        let oracle: Vec<(String, f64)> = row
            .oracle
            .iter()
            .map(|&(s, d, score)| (segments[s as usize].doc_id(d).unwrap(), score))
            .collect();
        // The serialized external form the bench cross-check compares.
        assert_eq!(row.oracle_hits, oracle);
        assert_eq!(row.text, qs[row.query]);
        assert_eq!(engine.len(), oracle.len(), "query {:?}", qs[row.query]);
        for (e, o) in engine.iter().zip(&oracle) {
            assert!(
                (e.1 - o.1).abs() <= 1e-9 * e.1.abs().max(1.0),
                "query {:?}: engine {engine:?} oracle {oracle:?}",
                qs[row.query]
            );
        }
        // Ids may only differ among equal scores (tie order).
        let kth = oracle.last().map_or(0.0, |o| o.1);
        let strictly_above = |v: &[(String, f64)]| {
            let mut ids: Vec<String> = v
                .iter()
                .filter(|x| x.1 > kth + 1e-9 * kth.abs().max(1.0))
                .map(|x| x.0.clone())
                .collect();
            ids.sort();
            ids
        };
        assert_eq!(
            strictly_above(&engine),
            strictly_above(&oracle),
            "query {:?}",
            qs[row.query]
        );
    }

    // (b) Certification is sound everywhere, and trivially complete for a
    // 100% head (no tails: every query certifies up to exact score ties).
    let mut certified_somewhere = 0;
    for (j, f) in report.fractions.iter().enumerate() {
        assert_eq!(f.bounds.len(), two_tier::BOUNDS.len());
        for b in &f.bounds {
            assert_eq!(
                b.violations, 0,
                "fraction {} bound {}",
                f.head_fraction, b.bound
            );
            assert!(b.rank_certified <= b.set_certified);
            assert!(b.score_exact <= b.rank_certified);
            assert!(b.set_certified <= b.set_certified_with_ties);
        }
        let (dl, max, loose) = (&f.bounds[0], &f.bounds[1], &f.bounds[2]);
        if f.head_fraction < 1.0 {
            certified_somewhere += (dl.set_certified > 0.0) as usize;
        } else {
            assert_eq!(max.set_certified_with_ties, 1.0);
            assert_eq!(dl.set_certified_with_ties, 1.0);
            // No tails: nothing to add, so the local text ratio is 1.
            assert_eq!(f.touched_tail_frontier_bytes, 0);
            assert_eq!(f.touched_local_ratio, Some(1.0));
        }
        if let Some(r) = f.touched_local_ratio {
            assert!(
                r > 0.0 && r <= 1.0 + 1e-9,
                "fraction {}: {r}",
                f.head_fraction
            );
        }
        for row in &rows {
            // Tighter bounds certify a superset of the queries.
            let d = &row.decisions[j];
            assert!(
                d[0].set_certified >= d[1].set_certified,
                "{:?}",
                qs[row.query]
            );
            assert!(
                d[1].set_certified >= d[2].set_certified,
                "{:?}",
                qs[row.query]
            );
            assert!(d[0].rank_certified >= d[1].rank_certified);
            assert!(d[1].rank_certified >= d[2].rank_certified);
            assert!(d[0].score_exact >= d[1].score_exact);
            assert!(d[1].score_exact >= d[2].score_exact);
            for x in d {
                if x.rank_certified {
                    assert!(x.head_answer_exact);
                }
                if x.score_exact {
                    assert!(x.rank_certified && x.tail_gets == 0);
                }
            }
        }
        assert!(dl.set_certified >= max.set_certified && max.set_certified >= loose.set_certified);
    }
    assert!(
        certified_somewhere > 0,
        "some partial head certifies something"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn segment_stats_scope_is_sound_too() {
    let root = std::env::temp_dir().join(format!("kosha-two-tier-seg-{}", std::process::id()));
    let ns = NamespaceId("tt".into());
    let manifest = build(&root, &ns, &[900, 400]);
    let dirs: Vec<_> = manifest
        .segments
        .iter()
        .map(|e| root.join(&ns.0).join(&e.segment_id.0))
        .collect();
    let segments = two_tier::open_segments(&dirs).unwrap();
    let config = Config {
        head_fractions: vec![0.1, 0.2],
        k: 5,
        min_split_df: 8,
        stats: StatsScope::Segment,
    };
    let (report, _) = two_tier::analyze_queries(&segments, config.clone(), &queries(60)).unwrap();
    for f in &report.fractions {
        assert!(f.bounds.iter().all(|b| b.violations == 0));
    }
    // Footprint: heads are at most the whole, and grow with the fraction.
    let fp = two_tier::footprint(&segments, &config).unwrap();
    assert!(fp.postings > 0 && fp.full_scoring_bytes > 0 && fp.span_bytes > 0);
    let mut prev = 0;
    for f in &fp.fractions {
        assert!(f.head_postings <= fp.postings && f.head_postings >= prev);
        prev = f.head_postings;
        assert!(f.head_scoring_bytes <= fp.full_scoring_bytes + fp.terms * 8);
    }
    let by_kind: HashMap<&str, u64> = fp.files.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    assert!(by_kind.get("postings_shards").copied().unwrap_or(0) > 0);
    // The ratio counts the files that stay local either way.
    assert!(by_kind.get("inverted_toc").copied().unwrap_or(0) > 0);
    assert_eq!(fp.split_segments, 2);
    assert!(fp.fixed_local_bytes >= by_kind["inverted_toc"]);
    assert_eq!(
        fp.local_text_bytes_today,
        fp.span_bytes + fp.fixed_local_bytes
    );
    for f in &fp.fractions {
        assert_eq!(
            f.local_text_bytes,
            f.head_span_bytes_est
                + f.tail_frontier_bytes
                + f.tail_toc_bytes_est
                + fp.fixed_local_bytes
        );
        let r = f.local_text_ratio.unwrap();
        assert!(r > 0.0 && r < 1.0 + 1e-9, "{r}");
        assert_eq!(
            f.tail_toc_bytes_est,
            f.split_terms * two_tier::TAIL_TOC_ENTRY_BYTES_EST
        );
    }

    // A zero time budget analyzes nothing and says so.
    let (r0, rows0) = two_tier::analyze_queries_until(
        &segments,
        config.clone(),
        &queries(60),
        Some(std::time::Instant::now()),
    )
    .unwrap();
    assert!(rows0.is_empty());
    assert_eq!(r0.queries_skipped_time_budget, 60);
    let _ = std::fs::remove_dir_all(&root);
}

/// The byte accounting the footprint relies on: re-encoded scoring bytes
/// plus position bytes reproduce each v5 term span's on-disk length.
#[test]
fn scoring_section_bytes_match_v5_spans() {
    fn varint(v: u32) -> u64 {
        match v {
            0..=0x7f => 1,
            0x80..=0x3fff => 2,
            0x4000..=0x1f_ffff => 3,
            0x20_0000..=0x0fff_ffff => 4,
            _ => 5,
        }
    }
    let root = std::env::temp_dir().join(format!("kosha-two-tier-bytes-{}", std::process::id()));
    let ns = NamespaceId("tt".into());
    let manifest = build(&root, &ns, &[2000]);
    let dir = root.join(&ns.0).join(&manifest.segments[0].segment_id.0);
    let reader = kosha_segment::SegmentReader::open(dir.clone()).unwrap();
    let terms: Vec<String> = reader
        .all_terms()
        .into_iter()
        .step_by(7)
        .map(str::to_string)
        .collect();
    let spans = kosha_segment::posting_term_spans(&dir, &terms).unwrap();
    assert!(!spans.is_empty());
    let mut checked = 0;
    for span in spans {
        // The reader's in-memory TOC lookup the analysis uses agrees.
        assert_eq!(reader.posting_span_len(&span.term), Some(span.len));
        let postings = reader.postings(&span.term).unwrap();
        let scoring = two_tier::scoring_section_bytes(postings.iter().map(|p| {
            let dl = reader.doc_meta(p.doc_id).unwrap().field_length;
            (p.doc_id, p.term_frequency, dl)
        }));
        let positions: u64 = postings
            .iter()
            .map(|p| {
                let mut prev = 0u32;
                varint(p.positions.len() as u32)
                    + p.positions
                        .iter()
                        .map(|&x| {
                            let d = varint(x.wrapping_sub(prev));
                            prev = x;
                            d
                        })
                        .sum::<u64>()
            })
            .sum();
        assert_eq!(scoring + positions, span.len, "term {}", span.term);
        checked += 1;
    }
    assert!(checked > 50);
    let _ = std::fs::remove_dir_all(&root);
}
