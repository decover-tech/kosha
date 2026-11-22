//! Research #387: the offline two-tier analysis must (a) score exactly as
//! the engine does — its exhaustive oracle matches `Searcher` OR top-k
//! under namespace-level statistics — and (b) never certify a head-only
//! answer that differs from the oracle, for any head fraction or bound.

use std::collections::HashMap;
use std::path::Path;

use kosha_core::{
    Bm25Params, DocumentId, Field, Manifest, ManifestEntry, NamespaceId, QueryOperator,
    SearchQuery, SegmentId,
};
use kosha_query::two_tier::{self, Config, StatsScope};
use kosha_query::Searcher;
use kosha_segment::SegmentWriter;

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
            assert!(b.set_certified <= b.set_certified_with_ties);
        }
        let (dl, max, loose) = (&f.bounds[0], &f.bounds[1], &f.bounds[2]);
        if f.head_fraction < 1.0 {
            certified_somewhere += (dl.set_certified > 0.0) as usize;
        } else {
            assert_eq!(max.set_certified_with_ties, 1.0);
            assert_eq!(dl.set_certified_with_ties, 1.0);
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
            for x in d {
                if x.rank_certified {
                    assert!(x.head_answer_exact && x.tail_gets == 0);
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
