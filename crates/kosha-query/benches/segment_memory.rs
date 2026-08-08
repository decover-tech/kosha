//! Fast local iteration harness for segment-format optimizations.
//!
//! Unlike the Criterion benches (which answer "did latency improve, with
//! statistics?"), this is a plain binary that prints one table comparing the
//! legacy v1 (eager-parse) and v2 (lazy) `inverted.idx` formats on the two
//! axes the format work targets:
//!
//!   * **resident memory** while segments are held open — measured as exact
//!     allocated bytes via a counting global allocator (`cap`), i.e. the
//!     same currency the query tier's `MemoryLedger` polices, with none of
//!     the RSS noise of OS-level measurement;
//!   * **cold open / cold + warm query latency** end to end through
//!     `Searcher::search`.
//!
//! Run it locally (seconds, no cluster):
//! ```text
//! cargo bench -p kosha-query --bench segment_memory
//! # bigger corpus:
//! KOSHA_BENCH_SEGS=16 KOSHA_BENCH_DOCS=8000 cargo bench -p kosha-query --bench segment_memory
//! ```
//!
//! The corpus is deterministic (fixed-seed LCG, Zipf-ish vocabulary), so
//! numbers are comparable run to run on the same machine.

use std::alloc::System;
use std::collections::HashMap;
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

use cap::Cap;
use kosha_core::{
    Bm25Params, DocumentId, Field, Manifest, ManifestEntry, MatchPhraseQuery, NamespaceId, Posting,
    SearchQuery, SegmentId, WildcardQuery,
};
use kosha_query::Searcher;
use kosha_segment::{SegmentReader, SegmentWriter};

// Counting wrapper around the system allocator: exact live allocated bytes,
// no unsafe in this crate (the `#[global_allocator]` attribute is safe).
#[global_allocator]
static ALLOC: Cap<System> = Cap::new(System, usize::MAX);

fn live_bytes() -> usize {
    ALLOC.allocated()
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ─── Deterministic synthetic corpus ─────────────────────────────────────────

/// Fixed-seed LCG so corpus content (and therefore numbers) are stable
/// across runs without pulling in a rand dependency.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }

    /// Zipf-ish rank in `[0, n)`: squaring the uniform sample skews mass
    /// toward low ranks, approximating natural-language term frequency —
    /// which is what makes "the" (rank 0) a realistic broad-query term and
    /// gives postings lists realistically uneven lengths.
    fn zipfish(&mut self, n: usize) -> usize {
        let u = self.next() as f64 / u64::MAX as f64;
        ((u * u) * n as f64) as usize
    }
}

fn vocab_word(rank: usize) -> String {
    match rank {
        0 => "the".into(),
        1 => "contract".into(),
        2 => "dispute".into(),
        _ => format!("w{rank}"),
    }
}

const WORDS_PER_DOC: usize = 120;

/// Write `segs` segments × `docs` docs of Zipf-ish prose under `root/<ns>/`.
/// Every ~50th doc ends with the fixed phrase "breach warranty" so phrase
/// queries have real (positional) hits.
fn build_corpus(root: &Path, ns: &NamespaceId, segs: usize, docs: usize, vocab: usize) -> Manifest {
    let _ = std::fs::remove_dir_all(root);
    let mut entries = Vec::with_capacity(segs);
    for s in 0..segs {
        let seg_id = SegmentId(format!("s{s}"));
        let seg_dir = root.join(&ns.0).join(&seg_id.0);
        let mut w = SegmentWriter::new(seg_id.clone(), seg_dir);
        let mut rng = Lcg(0x5EED + s as u64);
        for d in 0..docs {
            let mut words = Vec::with_capacity(WORDS_PER_DOC + 2);
            for _ in 0..WORDS_PER_DOC {
                words.push(vocab_word(rng.zipfish(vocab)));
            }
            if d % 50 == 0 {
                words.push("breach".into());
                words.push("warranty".into());
            }
            w.add_document(
                DocumentId(format!("s{s}-d{d}")),
                vec![Field::text("t", words.join(" "))],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();
        entries.push(ManifestEntry {
            segment_id: seg_id,
            doc_count: docs as u32,
        });
    }
    Manifest {
        version: 1,
        segments: entries,
        segment_footers: Default::default(),
    }
}

/// Serialize an inverted index in the legacy v1 stream layout (the exact
/// bytes the pre-v2 writer produced) — used to build a v1 twin of the
/// corpus so both formats are measured on identical data.
fn serialize_legacy_inverted(index: &HashMap<String, Vec<Posting>>) -> Vec<u8> {
    let mut terms: Vec<&String> = index.keys().collect();
    terms.sort();
    let mut buf = Vec::new();
    buf.extend_from_slice(&(terms.len() as u32).to_le_bytes());
    for term_str in terms {
        let postings = &index[term_str];
        let term_bytes = term_str.as_bytes();
        buf.extend_from_slice(&(term_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(term_bytes);
        buf.extend_from_slice(&(postings.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(postings.len() as u32).to_le_bytes());
        for posting in postings {
            buf.extend_from_slice(&posting.doc_id.to_le_bytes());
            buf.extend_from_slice(&posting.term_frequency.to_le_bytes());
            buf.extend_from_slice(&(posting.positions.len() as u32).to_le_bytes());
            for &pos in &posting.positions {
                buf.extend_from_slice(&pos.to_le_bytes());
            }
        }
    }
    buf
}

/// Copy the corpus into a twin tree whose `inverted.idx` files are v1.
fn build_v1_twin(v2_root: &Path, v1_root: &Path, ns: &NamespaceId, manifest: &Manifest) {
    let _ = std::fs::remove_dir_all(v1_root);
    for entry in &manifest.segments {
        let src = v2_root.join(&ns.0).join(&entry.segment_id.0);
        let dst = v1_root.join(&ns.0).join(&entry.segment_id.0);
        std::fs::create_dir_all(&dst).unwrap();
        for f in [
            "doc_store.bin",
            "doc_store.offsets",
            "filters.bin",
            "footer.json",
        ] {
            std::fs::copy(src.join(f), dst.join(f)).unwrap();
        }
        // Re-express the postings in v1 by reading them back out of v2.
        let reader = SegmentReader::open_with_options(src.clone(), false).unwrap();
        let index: HashMap<String, Vec<Posting>> = reader
            .all_terms()
            .iter()
            .map(|t| (t.to_string(), reader.postings(t).unwrap().into_owned()))
            .collect();
        std::fs::write(dst.join("inverted.idx"), serialize_legacy_inverted(&index)).unwrap();
    }
}

// ─── Measurement ────────────────────────────────────────────────────────────

fn mk_query(text: &str) -> SearchQuery {
    SearchQuery {
        query_text: text.into(),
        max_results: 10,
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

fn median_ms(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    samples[samples.len() / 2]
}

struct FormatReport {
    open_ms: f64,
    open_bytes: usize,
    cold_broad_ms: f64,
    warm: Vec<(&'static str, f64)>,
}

fn measure_format(root: &Path, ns: &NamespaceId, manifest: &Manifest) -> FormatReport {
    // Open cost + exact resident bytes while all segments are held open —
    // the per-pinned-segment currency of the query tier's memory ledger.
    let mut open_samples = Vec::new();
    let mut open_bytes = 0usize;
    for _ in 0..3 {
        let before = live_bytes();
        let t = Instant::now();
        let readers: Vec<SegmentReader> = manifest
            .segments
            .iter()
            .map(|e| {
                SegmentReader::open_with_options(root.join(&ns.0).join(&e.segment_id.0), false)
                    .unwrap()
            })
            .collect();
        open_samples.push(t.elapsed().as_secs_f64() * 1e3);
        open_bytes = live_bytes().saturating_sub(before);
        black_box(&readers);
        drop(readers);
    }

    // Cold end-to-end broad query: fresh Searcher (empty cache), every
    // segment opened inside the search itself.
    let mut cold_samples = Vec::new();
    for _ in 0..3 {
        let searcher = Searcher::new(root.to_path_buf());
        let t = Instant::now();
        let r = searcher
            .search(ns, manifest, &mk_query("the"), None)
            .unwrap();
        cold_samples.push(t.elapsed().as_secs_f64() * 1e3);
        black_box(r.total_hits);
    }

    // Warm queries: one shared Searcher, cache primed by a warmup pass.
    let searcher = Searcher::new(root.to_path_buf());
    let mut warm = Vec::new();
    let mut phrase = mk_query("");
    phrase.match_phrase = Some(MatchPhraseQuery {
        field: "t".into(),
        phrase: "breach warranty".into(),
        slop: 0,
    });
    let mut wildcard = mk_query("");
    wildcard.wildcard = Some(WildcardQuery {
        field: "t".into(),
        pattern: "w1*".into(),
        case_insensitive: true,
    });
    let shapes: Vec<(&'static str, SearchQuery)> = vec![
        ("warm broad (\"the\")", mk_query("the")),
        ("warm 2-term AND", mk_query("contract dispute")),
        ("warm phrase", phrase),
        ("warm wildcard w1*", wildcard),
    ];
    for (label, query) in shapes {
        searcher.search(ns, manifest, &query, None).unwrap(); // warmup
        let mut samples = Vec::new();
        for _ in 0..5 {
            let t = Instant::now();
            let r = searcher.search(ns, manifest, &query, None).unwrap();
            samples.push(t.elapsed().as_secs_f64() * 1e3);
            black_box(r.total_hits);
        }
        warm.push((label, median_ms(samples)));
    }

    FormatReport {
        open_ms: median_ms(open_samples),
        open_bytes,
        cold_broad_ms: median_ms(cold_samples),
        warm,
    }
}

fn dir_file_bytes(root: &Path, ns: &NamespaceId, manifest: &Manifest, file: &str) -> u64 {
    manifest
        .segments
        .iter()
        .map(|e| {
            std::fs::metadata(root.join(&ns.0).join(&e.segment_id.0).join(file))
                .map(|m| m.len())
                .unwrap_or(0)
        })
        .sum()
}

fn mb(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() {
    // `cargo bench` passes harness flags like `--bench`; ignore them.
    let segs = env_usize("KOSHA_BENCH_SEGS", 8);
    let docs = env_usize("KOSHA_BENCH_DOCS", 4000);
    let vocab = env_usize("KOSHA_BENCH_VOCAB", 20_000);

    let base = std::env::temp_dir().join("kosha-bench-segment-memory");
    let v2_root = base.join("v2");
    let v1_root = base.join("v1");
    let ns = NamespaceId("bench".into());

    eprintln!("building corpus: {segs} segs × {docs} docs, vocab ~{vocab} …");
    let manifest = build_corpus(&v2_root, &ns, segs, docs, vocab);
    build_v1_twin(&v2_root, &v1_root, &ns, &manifest);

    let inv_v2 = dir_file_bytes(&v2_root, &ns, &manifest, "inverted.idx");
    let inv_v1 = dir_file_bytes(&v1_root, &ns, &manifest, "inverted.idx");

    eprintln!("measuring v1 (eager) …");
    let v1 = measure_format(&v1_root, &ns, &manifest);
    eprintln!("measuring v2 (lazy) …");
    let v2 = measure_format(&v2_root, &ns, &manifest);

    let ratio = |a: f64, b: f64| if b > 0.0 { a / b } else { f64::NAN };
    println!();
    println!(
        "corpus: {segs} segments × {docs} docs ({} total), vocab ~{vocab}",
        segs * docs
    );
    println!(
        "inverted.idx on disk: v1 {:.1} MiB | v2 {:.1} MiB",
        inv_v1 as f64 / (1024.0 * 1024.0),
        inv_v2 as f64 / (1024.0 * 1024.0)
    );
    println!();
    println!(
        "{:<28} {:>14} {:>14} {:>8}",
        "metric", "v1 (eager)", "v2 (lazy)", "v1/v2"
    );
    println!(
        "{:<28} {:>12.1}ms {:>12.1}ms {:>7.1}x",
        "open all segments",
        v1.open_ms,
        v2.open_ms,
        ratio(v1.open_ms, v2.open_ms)
    );
    println!(
        "{:<28} {:>11.1}MiB {:>11.1}MiB {:>7.1}x",
        "resident while open",
        mb(v1.open_bytes),
        mb(v2.open_bytes),
        ratio(mb(v1.open_bytes), mb(v2.open_bytes))
    );
    println!(
        "{:<28} {:>12.1}ms {:>12.1}ms {:>7.1}x",
        "cold broad (\"the\")",
        v1.cold_broad_ms,
        v2.cold_broad_ms,
        ratio(v1.cold_broad_ms, v2.cold_broad_ms)
    );
    for ((label, v1_ms), (_, v2_ms)) in v1.warm.iter().zip(v2.warm.iter()) {
        println!(
            "{:<28} {:>12.2}ms {:>12.2}ms {:>7.1}x",
            label,
            v1_ms,
            v2_ms,
            ratio(*v1_ms, *v2_ms)
        );
    }
}
