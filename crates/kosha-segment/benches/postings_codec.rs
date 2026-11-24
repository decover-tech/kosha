//! Scoring-block codec microbenchmark for #386: v5 varint `(doc delta,
//! tf)` blocks vs the experimental v6 SIMD-BP128-layout bit-packed blocks.
//!
//! Reports, per posting-list distribution:
//! - scoring-section bytes per posting (v5 vs v6; v6 tails stay varint),
//! - decode throughput in postings/s and integers/s (2 ints per posting)
//!   for whole lists, block by block, exactly as `read_block` decodes,
//! - for the corpus, whole `postings-*.bin` bytes and warm full-postings
//!   fetch/decode throughput from real v5, v6 and v7 segment builds.
//!
//! Distributions: synthetic dense / medium / sparse doc gaps with a
//! skewed tf, and real posting lists from the repository's Markdown files
//! (as in the `tokenizer` bench) split into ~`KOSHA_BENCH_DOC_BYTES`
//! (default 600) byte documents. These are NOT the bench's MSMarco lists:
//! run the bench VM for those numbers.
//!
//!     cargo bench -p kosha-segment --bench postings_codec
//!
//! Set `KOSHA_BENCH_CORPUS=<path>` to use your own text file. A path
//! ending in `.ndjson` is read as bench corpus shards (one
//! `{"id": .., "text": ..}` document per line, as in the MSMarco
//! `shard-*.ndjson` files), one document per line, capped at
//! `KOSHA_BENCH_MAX_DOCS` (default 200000) documents.

use std::collections::BTreeMap;
use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use kosha_core::fuzzy::SortedTerms;
use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};
use kosha_segment::bitpack::{
    decode_bp_block, decode_varint_block, encode_bp_block, encode_varint_block, pack128, unpack128,
    BP_BLOCK,
};
use kosha_segment::{
    postings_section_stats, PostingsSectionStats, ScoringPosting, SegmentReader, SegmentWriter,
    BITPACKED_SKIP_SPLIT_INVERTED_VERSION, PACKED_POSITIONS_INVERTED_VERSION,
    SKIP_SPLIT_INVERTED_VERSION,
};

/// One term's scoring section in both encodings, with per-block byte
/// ranges and decode bases.
struct Encoded {
    n: usize,
    v5: Vec<u8>,
    v5_blocks: Vec<(usize, usize, u32)>,
    v6: Vec<u8>,
    v6_blocks: Vec<(usize, usize, u32, bool)>,
}

fn encode(list: &[ScoringPosting]) -> Encoded {
    let mut v5 = Vec::new();
    let mut v6 = Vec::new();
    let mut v5_blocks = Vec::new();
    let mut v6_blocks = Vec::new();
    let mut prev_last = 0u32;
    for (b, block) in list.chunks(BP_BLOCK).enumerate() {
        let base = if b == 0 { 0 } else { prev_last };
        let s = v5.len();
        encode_varint_block(block, base, &mut v5);
        v5_blocks.push((s, v5.len(), base));
        let s = v6.len();
        if block.len() == BP_BLOCK {
            let docs: [u32; BP_BLOCK] = std::array::from_fn(|i| block[i].doc_id);
            let tfs: [u32; BP_BLOCK] = std::array::from_fn(|i| block[i].term_frequency);
            let next_min = if b == 0 { 0 } else { prev_last.wrapping_add(1) };
            encode_bp_block(&docs, &tfs, next_min, &mut v6);
            v6_blocks.push((s, v6.len(), next_min, true));
        } else {
            encode_varint_block(block, base, &mut v6);
            v6_blocks.push((s, v6.len(), base, false));
        }
        prev_last = block.last().unwrap().doc_id;
    }
    Encoded {
        n: list.len(),
        v5,
        v5_blocks,
        v6,
        v6_blocks,
    }
}

fn decode_v5(e: &Encoded, out: &mut Vec<ScoringPosting>) -> u64 {
    let mut sum = 0u64;
    for (i, &(s, t, base)) in e.v5_blocks.iter().enumerate() {
        out.clear();
        let n = (e.n - i * BP_BLOCK).min(BP_BLOCK);
        let last = decode_varint_block(&e.v5[s..t], n, base, out).unwrap();
        sum += u64::from(last) + u64::from(out[0].term_frequency);
    }
    sum
}

fn decode_v6(e: &Encoded, out: &mut Vec<ScoringPosting>) -> u64 {
    let mut sum = 0u64;
    for (i, &(s, t, base, packed)) in e.v6_blocks.iter().enumerate() {
        out.clear();
        let last = if packed {
            decode_bp_block(&e.v6[s..t], base, out).unwrap()
        } else {
            let n = (e.n - i * BP_BLOCK).min(BP_BLOCK);
            decode_varint_block(&e.v6[s..t], n, base, out).unwrap()
        };
        sum += u64::from(last) + u64::from(out[0].term_frequency);
    }
    sum
}

type Decoder = fn(&Encoded, &mut Vec<ScoringPosting>) -> u64;

/// Per-pass time of decoding every list once: v5 and v6 measured in
/// alternating rounds (best of 15 each, ~40 ms per round) so machine
/// noise on a shared host hits both codecs alike.
fn time_pair(lists: &[Encoded], fs: [Decoder; 2]) -> [Duration; 2] {
    let mut out = Vec::with_capacity(BP_BLOCK);
    let run = |f: Decoder, reps: u32, out: &mut Vec<ScoringPosting>| {
        let t = Instant::now();
        for _ in 0..reps {
            for e in lists {
                black_box(f(black_box(e), out));
            }
        }
        t.elapsed()
    };
    let mut reps = 1u32;
    while run(fs[0], reps, &mut out) < Duration::from_millis(40) && reps < 1 << 20 {
        reps *= 2;
    }
    let mut best = [Duration::MAX; 2];
    for _ in 0..15 {
        for (b, &f) in best.iter_mut().zip(&fs) {
            *b = (*b).min(run(f, reps, &mut out) / reps);
        }
    }
    best
}

fn report(name: &str, lists: &[Vec<ScoringPosting>]) {
    let encoded: Vec<Encoded> = lists.iter().map(|l| encode(l)).collect();
    // Parity: both decoders produce the source lists.
    let mut out = Vec::new();
    for (l, e) in lists.iter().zip(&encoded) {
        let (mut a, mut b) = (Vec::new(), Vec::new());
        for (i, &(s, t, base)) in e.v5_blocks.iter().enumerate() {
            let n = (e.n - i * BP_BLOCK).min(BP_BLOCK);
            decode_varint_block(&e.v5[s..t], n, base, &mut a).unwrap();
        }
        for (i, &(s, t, base, packed)) in e.v6_blocks.iter().enumerate() {
            out.clear();
            if packed {
                decode_bp_block(&e.v6[s..t], base, &mut out).unwrap();
            } else {
                let n = (e.n - i * BP_BLOCK).min(BP_BLOCK);
                decode_varint_block(&e.v6[s..t], n, base, &mut out).unwrap();
            }
            b.extend_from_slice(&out);
        }
        assert_eq!(&a, l);
        assert_eq!(&b, l);
    }
    let postings: usize = lists.iter().map(Vec::len).sum();
    let full: usize = encoded
        .iter()
        .flat_map(|e| &e.v6_blocks)
        .filter(|b| b.3)
        .count()
        * BP_BLOCK;
    let v5_bytes: usize = encoded.iter().map(|e| e.v5.len()).sum();
    let v6_bytes: usize = encoded.iter().map(|e| e.v6.len()).sum();
    let [t5, t6] = time_pair(&encoded, [decode_v5, decode_v6]);
    let mps = |t: Duration| postings as f64 / t.as_secs_f64() / 1e6;
    println!(
        "{name:<28} lists {:>6} postings {:>9} (in full blocks {:>5.1}%)",
        lists.len(),
        postings,
        100.0 * full as f64 / postings.max(1) as f64
    );
    println!(
        "  scoring bytes/posting  v5 {:>6.3}  v6 {:>6.3}  ({:+.1}%)",
        v5_bytes as f64 / postings as f64,
        v6_bytes as f64 / postings as f64,
        100.0 * (v6_bytes as f64 / v5_bytes as f64 - 1.0)
    );
    println!(
        "  decode Mpostings/s     v5 {:>7.1}  v6 {:>7.1}  ({:.2}x)   [M ints/s: v5 {:.0}, v6 {:.0}]",
        mps(t5),
        mps(t6),
        t5.as_secs_f64() / t6.as_secs_f64(),
        2.0 * mps(t5),
        2.0 * mps(t6)
    );
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) as u32
    }
    /// Skewed tf: 1 most of the time, occasionally larger.
    fn tf(&mut self) -> u32 {
        let r = self.next() % 100;
        match r {
            0..=69 => 1,
            70..=89 => 2,
            90..=96 => 3 + self.next() % 3,
            _ => 6 + self.next() % 40,
        }
    }
}

fn synthetic(rng: &mut Rng, lists: usize, len: usize, max_gap: u32) -> Vec<Vec<ScoringPosting>> {
    (0..lists)
        .map(|_| {
            let mut doc = 0u32;
            (0..len)
                .map(|i| {
                    doc += if i == 0 { 0 } else { 1 + rng.next() % max_gap };
                    ScoringPosting {
                        doc_id: doc,
                        term_frequency: rng.tf(),
                    }
                })
                .collect()
        })
        .collect()
}

fn markdown_corpus(root: &Path, out: &mut String) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    let mut paths: Vec<_> = entries.flatten().map(|e| e.path()).collect();
    paths.sort();
    for path in paths {
        let name = path.file_name().unwrap().to_string_lossy();
        if name.starts_with('.') || name == "target" || name == "node_modules" {
            continue;
        }
        if path.is_dir() {
            markdown_corpus(&path, out);
        } else if path.extension().is_some_and(|e| e == "md") {
            if let Ok(text) = std::fs::read_to_string(&path) {
                out.push_str(&text);
                out.push('\n');
            }
        }
    }
}

fn corpus_docs() -> Vec<String> {
    let path = std::env::var("KOSHA_BENCH_CORPUS").ok();
    if let Some(p) = path.as_deref().filter(|p| p.ends_with(".ndjson")) {
        return ndjson_docs(p);
    }
    let text = match path {
        Some(p) => std::fs::read_to_string(p).expect("KOSHA_BENCH_CORPUS readable"),
        None => {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            let mut s = String::new();
            markdown_corpus(&root, &mut s);
            s
        }
    };
    let doc_bytes: usize = std::env::var("KOSHA_BENCH_DOC_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600);
    let mut docs = Vec::new();
    let mut cur = String::new();
    for word in text.split_whitespace() {
        cur.push_str(word);
        cur.push(' ');
        if cur.len() >= doc_bytes {
            docs.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        docs.push(cur);
    }
    docs
}

/// One document per `{"text": ..}` line of a bench corpus shard.
fn ndjson_docs(path: &str) -> Vec<String> {
    use std::io::BufRead;
    let max: usize = std::env::var("KOSHA_BENCH_MAX_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let file = std::fs::File::open(path).expect("KOSHA_BENCH_CORPUS readable");
    std::io::BufReader::new(file)
        .lines()
        .map(|l| l.expect("KOSHA_BENCH_CORPUS line"))
        .filter(|l| !l.trim().is_empty())
        .take(max)
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(&l).expect("ndjson line");
            v.get("text")
                .and_then(|t| t.as_str())
                .expect("ndjson line has a \"text\" string")
                .to_string()
        })
        .collect()
}

/// Simple lowercase-alphanumeric term lists per document (the bench's
/// analyzer is not needed for the codec comparison).
fn corpus_lists(docs: &[String]) -> Vec<Vec<ScoringPosting>> {
    let mut terms: BTreeMap<String, Vec<ScoringPosting>> = BTreeMap::new();
    for (d, text) in docs.iter().enumerate() {
        let mut tf: BTreeMap<String, u32> = BTreeMap::new();
        for tok in text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|t| !t.is_empty())
        {
            *tf.entry(tok.to_lowercase()).or_default() += 1;
        }
        for (t, n) in tf {
            terms.entry(t).or_default().push(ScoringPosting {
                doc_id: d as u32,
                term_frequency: n,
            });
        }
    }
    terms.into_values().collect()
}

fn segment_postings(docs: &[String], version: u32) -> (u64, PostingsSectionStats, Duration) {
    let dir = std::env::temp_dir().join(format!(
        "kosha-bench-postings-codec-v{version}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let mut w =
        SegmentWriter::new(SegmentId("b".into()), dir.clone()).with_postings_write_version(version);
    for (i, d) in docs.iter().enumerate() {
        w.add_document(DocumentId(format!("d{i}")), vec![Field::text("body", d)]);
    }
    w.finalize(Bm25Params::default()).unwrap();
    let bytes = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("postings"))
        .map(|e| e.metadata().unwrap().len())
        .sum();
    let stats = postings_section_stats(&dir).expect("section stats");
    let mut best = Duration::MAX;
    // Section stats warmed the shards. A new reader each pass prevents
    // decoded postings cache hits, and opening it is not timed.
    for _ in 0..7 {
        let r = SegmentReader::open(dir.clone()).unwrap();
        let dictionary = r.term_dictionary();
        let terms: Vec<String> = (0..dictionary.term_count())
            .map(|i| dictionary.term(i).to_owned())
            .collect();
        let start = Instant::now();
        let decoded = black_box(r.postings_for_terms(black_box(&terms)));
        best = best.min(start.elapsed());
        assert_eq!(
            decoded.iter().map(|(_, p)| p.len() as u64).sum::<u64>(),
            stats.postings
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
    (bytes, stats, best)
}

fn main() {
    println!(
        "postings_codec: arch {} ({}), build profile {}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }
    );
    let mut rng = Rng(0x5eed);
    // Raw unpack kernel only (no delta decode, no output structs).
    for bits in [1u32, 2, 4, 7, 10, 13, 17, 24, 32] {
        let blocks: Vec<Vec<u8>> = (0..1024)
            .map(|_| {
                let vals: [u32; BP_BLOCK] = std::array::from_fn(|_| {
                    if bits == 32 {
                        rng.next()
                    } else {
                        rng.next() & ((1 << bits) - 1)
                    }
                });
                let mut b = Vec::new();
                pack128(&vals, bits, &mut b);
                b
            })
            .collect();
        let mut out = [0u32; BP_BLOCK];
        let mut best = Duration::MAX;
        for _ in 0..7 {
            let t = Instant::now();
            for _ in 0..50 {
                for b in &blocks {
                    unpack128(black_box(b), bits, &mut out).unwrap();
                    black_box(&out);
                }
            }
            best = best.min(t.elapsed() / 50);
        }
        println!(
            "unpack128 kernel b={bits:>2}: {:>7.0} M ints/s",
            (blocks.len() * BP_BLOCK) as f64 / best.as_secs_f64() / 1e6
        );
    }
    report(
        "synthetic dense gap<=4",
        &synthetic(&mut rng, 64, 20_000, 4),
    );
    report(
        "synthetic medium gap<=64",
        &synthetic(&mut rng, 64, 20_000, 64),
    );
    report(
        "synthetic sparse gap<=4096",
        &synthetic(&mut rng, 64, 20_000, 4096),
    );
    report(
        "synthetic short len=150",
        &synthetic(&mut rng, 4_000, 150, 512),
    );

    let docs = corpus_docs();
    let lists = corpus_lists(&docs);
    let long: Vec<Vec<ScoringPosting>> = lists
        .iter()
        .filter(|l| l.len() >= BP_BLOCK)
        .cloned()
        .collect();
    let label = match std::env::var("KOSHA_BENCH_CORPUS") {
        Ok(p) if p.ends_with(".ndjson") => "ndjson",
        Ok(_) => "text file",
        Err(_) => "markdown",
    };
    println!("{label} corpus: {} docs, {} terms", docs.len(), lists.len());
    report(&format!("{label} all terms"), &lists);
    report(&format!("{label} terms df>=128"), &long);
    let results = [
        SKIP_SPLIT_INVERTED_VERSION,
        BITPACKED_SKIP_SPLIT_INVERTED_VERSION,
        PACKED_POSITIONS_INVERTED_VERSION,
    ]
    .map(|v| segment_postings(&docs, v));
    let base_bytes = results[0].0;
    println!("{label} segment postings-*.bin (skip+scoring+positions):");
    for (bytes, s, elapsed) in &results {
        let v = s.inverted_version;
        println!(
            "  v{v} {bytes} B ({:+.1}% vs v5), full fetch/decode {:.1} Mpostings/s",
            100.0 * (*bytes as f64 / base_bytes as f64 - 1.0),
            s.postings as f64 / elapsed.as_secs_f64() / 1e6
        );
        println!(
            "  v{v} sections: skip {} B, scoring {} B, positions {} B; postings in full blocks {:.1}%",
            s.skip_bytes,
            s.scoring_bytes,
            s.positions_bytes,
            100.0 * s.postings_in_full_blocks as f64 / s.postings.max(1) as f64
        );
    }
}
