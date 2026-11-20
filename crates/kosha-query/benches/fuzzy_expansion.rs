//! Fuzzy term matching (issue #201): what expansion costs.
//!
//! Builds a corpus of pseudo-words (syllable strings, Zipfian, so the
//! dictionary has realistic shared prefixes and near neighbours) and
//! reports, per query token:
//!
//! * the expansion walk alone over every segment's term dictionary —
//!   terms in range, terms visited, DP rows, wall time;
//! * warm end-to-end latency of the exact `query_text` term next to the
//!   fuzzy clause at 0 / 1 / 2 / auto edits and with `prefix_length: 2`;
//! * how many of 200 small segments (the only ones that get a fuzzy
//!   bloom) the footer blooms let a one-edit query skip.
//!
//! ```text
//! cargo bench -p kosha-query --bench fuzzy_expansion
//! KOSHA_BENCH_DOCS=1000000 cargo bench -p kosha-query --bench fuzzy_expansion
//! ```

use std::path::Path;
use std::time::Instant;

use kosha_core::fuzzy::{expand, ExpandStats, FuzzyQuery};
use kosha_core::TermBloomMode;
use kosha_core::{
    Bm25Params, DocumentId, Field, Manifest, ManifestEntry, NamespaceId, SearchQuery, SegmentId,
};
use kosha_query::{segment_may_match_query_terms, QueryAnalysis, Searcher};
use kosha_segment::{SegmentReader, SegmentWriter};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// Zipf-ish id in `0..n`.
    fn zipf(&mut self, n: u64) -> u64 {
        let u = (self.next() % 1_000_000) as f64 / 1_000_000.0;
        ((n as f64).powf(u) - 1.0) as u64
    }
}

const ONSETS: [&str; 16] = [
    "b", "c", "d", "f", "g", "l", "m", "n", "p", "r", "s", "t", "v", "st", "pr", "tr",
];
const VOWELS: [&str; 6] = ["a", "e", "i", "o", "u", "ie"];
const CODAS: [&str; 6] = ["", "n", "r", "s", "t", "ve"];

/// The `id`-th pseudo-word: 2–4 syllables, deterministic.
fn word(id: u64) -> String {
    let mut x = id.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x5851_f42d_4c95_7f2d;
    let mut next = |n: usize| {
        x ^= x >> 29;
        x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        (x >> 32) as usize % n
    };
    let syllables = 2 + next(3);
    let mut w = String::new();
    for _ in 0..syllables {
        w.push_str(ONSETS[next(ONSETS.len())]);
        w.push_str(VOWELS[next(VOWELS.len())]);
    }
    w.push_str(CODAS[next(CODAS.len())]);
    w
}

const SEGMENTS: usize = 8;
const VOCAB: u64 = 200_000;
const WORDS_PER_DOC: usize = 30;

fn build(root: &Path, docs: usize) -> Manifest {
    let _ = std::fs::remove_dir_all(root);
    let mut manifest = Manifest {
        version: 1,
        segments: Vec::new(),
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
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let per_segment = docs / SEGMENTS;
    for s in 0..SEGMENTS {
        let id = SegmentId(format!("s{s}"));
        let mut writer = SegmentWriter::new(id.clone(), root.join("ns").join(&id.0));
        for i in 0..per_segment {
            let text: Vec<String> = (0..WORDS_PER_DOC).map(|_| word(rng.zipf(VOCAB))).collect();
            writer.add_document(
                DocumentId(format!("d{s}-{i}")),
                vec![Field::text("content", text.join(" "))],
            );
        }
        let footer = writer.finalize(Bm25Params::default()).unwrap();
        manifest.segments.push(ManifestEntry {
            segment_id: id,
            doc_count: footer.doc_count,
        });
    }
    manifest
}

fn exact(term: &str) -> SearchQuery {
    serde_json::from_value(serde_json::json!({
        "query_text": term, "max_results": 10, "no_cache": true,
    }))
    .unwrap()
}

fn fuzzy(term: &str, fuzziness: serde_json::Value, prefix_length: usize) -> SearchQuery {
    serde_json::from_value(serde_json::json!({
        "query_text": "",
        "max_results": 10,
        "no_cache": true,
        "fuzzy": {"value": term, "fuzziness": fuzziness, "prefix_length": prefix_length},
    }))
    .unwrap()
}

/// Median warm latency in ms over `runs` searches (after one warm-up).
fn time(searcher: &Searcher, manifest: &Manifest, q: &SearchQuery, runs: usize) -> (f64, usize) {
    let ns = NamespaceId("ns".into());
    let hits = searcher.search(&ns, manifest, q, None).unwrap().total_hits;
    let mut samples: Vec<f64> = (0..runs)
        .map(|_| {
            let start = Instant::now();
            std::hint::black_box(searcher.search(&ns, manifest, q, None).unwrap());
            start.elapsed().as_secs_f64() * 1e3
        })
        .collect();
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (samples[samples.len() / 2], hits)
}

/// One character swapped with its neighbour, in the middle of `w`.
fn typo(w: &str) -> String {
    let mut c: Vec<char> = w.chars().collect();
    let i = c.len() / 2;
    c.swap(i - 1, i);
    c.into_iter().collect()
}

fn main() {
    let docs: usize = std::env::var("KOSHA_BENCH_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let root = std::env::temp_dir().join(format!("kosha-fuzzy-bench-{}", std::process::id()));
    let t = Instant::now();
    let manifest = build(&root, docs);
    println!(
        "{docs} docs x {WORDS_PER_DOC} words (vocab {VOCAB}, Zipfian) in {SEGMENTS} segments, built in {:.1}s",
        t.elapsed().as_secs_f64()
    );
    let readers: Vec<SegmentReader> = manifest
        .segments
        .iter()
        .map(|e| SegmentReader::open(root.join("ns").join(&e.segment_id.0)).unwrap())
        .collect();
    let terms: usize = readers.iter().map(|r| r.all_terms().len()).sum();
    let with_fuzzy_bloom = readers
        .iter()
        .filter(|r| r.footer().fuzzy_bloom.is_some())
        .count();
    println!(
        "dictionary: {terms} terms over {SEGMENTS} segments ({} per segment); \
         {with_fuzzy_bloom}/{SEGMENTS} segments small enough for a fuzzy bloom",
        terms / SEGMENTS
    );

    // A frequent, a mid and a rare word, plus a typo of each.
    let tokens: Vec<String> = [3u64, 300, 30_000]
        .iter()
        .map(|&id| word(id))
        .flat_map(|w| [typo(&w), w])
        .collect();

    println!();
    println!("expansion walk only, summed over all segments (no postings read):");
    println!(
        "{:<16} {:>6} {:>7} {:>12} {:>12} {:>10} {:>9} {:>10}",
        "token", "edits", "prefix", "range terms", "visited", "DP rows", "matched", "µs"
    );
    for token in &tokens {
        for (edits, prefix) in [(1u8, 0usize), (2, 0), (2, 2)] {
            let clause: FuzzyQuery = serde_json::from_value(serde_json::json!({
                "value": token, "fuzziness": edits, "prefix_length": prefix,
                "max_expansions": 1024,
            }))
            .unwrap();
            let params = clause.params_for(token);
            let mut stats = ExpandStats::default();
            let runs = 20;
            let start = Instant::now();
            for run in 0..runs {
                let mut s = ExpandStats::default();
                for reader in &readers {
                    std::hint::black_box(expand(
                        &reader.term_dictionary(),
                        "",
                        token,
                        &params,
                        &mut s,
                    ));
                }
                if run == 0 {
                    stats = s;
                }
            }
            let micros = start.elapsed().as_secs_f64() * 1e6 / runs as f64;
            println!(
                "{token:<16} {edits:>6} {prefix:>7} {:>12} {:>12} {:>10} {:>9} {micros:>10.0}",
                stats.range_terms, stats.terms_visited, stats.rows, stats.matched
            );
        }
    }

    let searcher = Searcher::new(root.clone());
    println!();
    println!("warm end-to-end, top 10:");
    println!("{:<44} {:>10} {:>10}", "query", "p50 ms", "hits");
    for token in &tokens {
        let cases: Vec<(String, SearchQuery)> = vec![
            (format!("{token} query_text (exact)"), exact(token)),
            (
                format!("{token} fuzzy 0"),
                fuzzy(token, serde_json::json!(0), 0),
            ),
            (
                format!("{token} fuzzy 1"),
                fuzzy(token, serde_json::json!(1), 0),
            ),
            (
                format!("{token} fuzzy 2"),
                fuzzy(token, serde_json::json!(2), 0),
            ),
            (
                format!("{token} fuzzy auto"),
                fuzzy(token, serde_json::json!("auto"), 0),
            ),
            (
                format!("{token} fuzzy 2 prefix 2"),
                fuzzy(token, serde_json::json!(2), 2),
            ),
        ];
        for (label, q) in cases {
            let (p50, hits) = time(&searcher, &manifest, &q, 15);
            println!("{label:<44} {p50:>10.2} {hits:>10}");
        }
    }
    let _ = std::fs::remove_dir_all(&root);

    // Bloom pruning over many small segments (40 docs each, ~1k terms):
    // a fresh-flush / small-matter namespace, where the fuzzy bloom exists.
    let small_root = root.with_extension("small");
    let mut rng = Rng(0x1234_5678_9abc_def1);
    let mut footers = Vec::new();
    let mut bloom_bytes = 0usize;
    for s in 0..200 {
        let dir = small_root.join(format!("s{s}"));
        let mut writer = SegmentWriter::new(SegmentId(format!("s{s}")), dir);
        for i in 0..40 {
            let text: Vec<String> = (0..WORDS_PER_DOC).map(|_| word(rng.zipf(VOCAB))).collect();
            writer.add_document(
                DocumentId(format!("d{s}-{i}")),
                vec![Field::text("content", text.join(" "))],
            );
        }
        let footer = writer.finalize(Bm25Params::default()).unwrap();
        bloom_bytes += footer.fuzzy_bloom.as_ref().map_or(0, |b| b.bits_b64.len());
        footers.push(footer);
    }
    let with_bloom = footers.iter().filter(|f| f.fuzzy_bloom.is_some()).count();
    println!();
    println!(
        "bloom pruning, 200 segments x 40 docs: {with_bloom}/200 have a fuzzy bloom, \
         avg {:.1} KB (base64) each",
        bloom_bytes as f64 / with_bloom.max(1) as f64 / 1024.0
    );
    println!(
        "{:<16} {:>6} {:>14} {:>14} {:>10}",
        "token", "edits", "may match", "really match", "µs/segment"
    );
    for token in &tokens {
        for edits in [0u8, 1, 2] {
            let q = fuzzy(token, serde_json::json!(edits), 0);
            let analysis = QueryAnalysis::new(&q);
            let start = Instant::now();
            let admitted = footers
                .iter()
                .filter(|f| segment_may_match_query_terms(&analysis, TermBloomMode::And, f))
                .count();
            let micros = start.elapsed().as_secs_f64() * 1e6 / footers.len() as f64;
            let clause = q.fuzzy.as_ref().unwrap();
            let truly = footers
                .iter()
                .filter(|f| {
                    let reader = SegmentReader::open(small_root.join(&f.segment_id.0)).unwrap();
                    !expand(
                        &reader.term_dictionary(),
                        "",
                        token,
                        &clause.params_for(token),
                        &mut ExpandStats::default(),
                    )
                    .is_empty()
                })
                .count();
            println!("{token:<16} {edits:>6} {admitted:>14} {truly:>14} {micros:>10.1}");
        }
    }
    let _ = std::fs::remove_dir_all(&small_root);
}
