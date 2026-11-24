//! Property tests for the experimental v6 bit-packed scoring blocks
//! (#386): a segment written as v6 must read back exactly like a v5 build
//! of the same random documents (full postings, skip summaries and every
//! scoring block), and damaged v6 posting shards must never panic the
//! reader.
//!
//! 32 cases per property by default; set `PROPTEST_CASES` for more.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};
use kosha_segment::{
    ScoringPostingsRef, SegmentReader, SegmentWriter, BITPACKED_SKIP_SPLIT_INVERTED_VERSION,
    SCORING_BLOCK_LEN, SKIP_SPLIT_INVERTED_VERSION,
};
use proptest::prelude::*;

fn config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32);
    ProptestConfig {
        cases,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-bp-{tag}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Vocabulary skewed so the first words appear in most documents (many
/// full 128-posting blocks) and the tail is rare (partial blocks only).
const VOCAB: [&str; 12] = [
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa",
    "lambda", "mu",
];

fn build(dir: &Path, version: u32, docs: &[Vec<u8>]) {
    let mut w = SegmentWriter::new(SegmentId("bp".into()), dir.to_path_buf())
        .with_postings_write_version(version);
    for (i, words) in docs.iter().enumerate() {
        let text: Vec<&str> = words.iter().map(|&k| VOCAB[k as usize]).collect();
        w.add_document(
            DocumentId(format!("d{i}")),
            vec![Field::text("body", text.join(" "))],
        );
    }
    w.finalize(Bm25Params::default()).unwrap();
}

/// Words per document: index `k` with probability falling in `k`.
fn doc_words() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(
        prop_oneof![
            8 => 0u8..2,
            4 => 2u8..5,
            2 => 5u8..9,
            1 => 9u8..12,
        ],
        0..12,
    )
}

fn assert_same_reads(r5: &SegmentReader, r6: &SegmentReader) -> Result<(), TestCaseError> {
    let terms: Vec<String> = VOCAB.iter().map(|w| w.to_string()).collect();
    for t in &terms {
        let (a, b) = (r5.postings(t), r6.postings(t));
        prop_assert_eq!(a.as_deref(), b.as_deref(), "postings of {}", t);
    }
    let s5 = r5.scoring_postings_for_terms(&terms);
    let s6 = r6.scoring_postings_for_terms(&terms);
    prop_assert_eq!(s5.len(), s6.len());
    let (mut x, mut y) = (Vec::new(), Vec::new());
    for ((t5, p5), (t6, p6)) in s5.iter().zip(&s6) {
        prop_assert_eq!(t5, t6);
        let (ScoringPostingsRef::Blocked(p5), ScoringPostingsRef::Blocked(p6)) = (p5, p6) else {
            return Err(TestCaseError::fail("both must take the blocked path"));
        };
        prop_assert_eq!(p5.doc_count(), p6.doc_count());
        prop_assert_eq!(p5.block_count(), p6.block_count());
        for blk in 0..p5.block_count() {
            let (s, u) = (p5.summary(blk), p6.summary(blk));
            prop_assert_eq!(
                (s.last_doc_id, s.max_tf, s.min_field_length),
                (u.last_doc_id, u.max_tf, u.min_field_length)
            );
            prop_assert!(p5.read_block(blk, &mut x));
            prop_assert!(p6.read_block(blk, &mut y));
            prop_assert_eq!(&x, &y, "{} block {}", t5, blk);
            // find_block (leapfrog gallop) agrees too.
            prop_assert_eq!(
                p5.find_block(s.last_doc_id, 0),
                p6.find_block(s.last_doc_id, 0)
            );
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(config())]

    /// Random documents (0..700, so both partial-only and multi-block
    /// terms) read back identically from v5 and v6 builds.
    #[test]
    fn v6_reads_back_like_v5(docs in proptest::collection::vec(doc_words(), 0..700)) {
        let (d5, d6) = (temp_dir("v5"), temp_dir("v6"));
        build(&d5, SKIP_SPLIT_INVERTED_VERSION, &docs);
        build(&d6, BITPACKED_SKIP_SPLIT_INVERTED_VERSION, &docs);
        let r5 = SegmentReader::open(d5.clone()).unwrap();
        let r6 = SegmentReader::open(d6.clone()).unwrap();
        let res = assert_same_reads(&r5, &r6);
        drop((r5, r6));
        std::fs::remove_dir_all(&d5).ok();
        std::fs::remove_dir_all(&d6).ok();
        res?;
    }

    /// Bit flips anywhere in a v6 posting shard: the reader errors or
    /// returns fewer/other postings, but never panics, and a scoring block
    /// that still decodes ends on its skip entry's `last_doc_id`.
    #[test]
    fn damaged_v6_shards_never_panic(
        flips in proptest::collection::vec((any::<prop::sample::Index>(), 0u8..8), 1..6),
    ) {
        let dir = temp_dir("dmg");
        let docs: Vec<Vec<u8>> = (0..600u32)
            .map(|i| vec![(i % 2) as u8, (i % 5) as u8, (i % 11) as u8])
            .collect();
        build(&dir, BITPACKED_SKIP_SPLIT_INVERTED_VERSION, &docs);
        let mut shards: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("postings"))
            })
            .collect();
        shards.sort();
        prop_assert!(!shards.is_empty());
        for shard in &shards {
            let mut bytes = std::fs::read(shard).unwrap();
            for (idx, bit) in &flips {
                let i = idx.index(bytes.len());
                bytes[i] ^= 1 << bit;
            }
            std::fs::write(shard, &bytes).unwrap();
        }
        if let Ok(reader) = SegmentReader::open(dir.clone()) {
            let terms: Vec<String> = VOCAB.iter().map(|w| w.to_string()).collect();
            for t in &terms {
                let _ = reader.postings(t).map(|p| p.len());
            }
            let mut buf = Vec::new();
            for (_, p) in reader.scoring_postings_for_terms(&terms) {
                if let ScoringPostingsRef::Blocked(b) = p {
                    for blk in 0..b.block_count() {
                        if b.read_block(blk, &mut buf) {
                            prop_assert!(buf.len() <= SCORING_BLOCK_LEN);
                            prop_assert_eq!(buf.last().map(|p| p.doc_id), Some(b.summary(blk).last_doc_id));
                        }
                    }
                }
            }
            let _ = reader.take_blob_read_failure();
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
