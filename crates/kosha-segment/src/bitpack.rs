//! Scoring-block codecs for the skip-split postings layouts (#386).
//!
//! The v5 skip-split span ([`crate::SKIP_SPLIT_INVERTED_VERSION`]) stores
//! each 128-posting scoring block as LEB128 varint `(doc delta, tf)` pairs,
//! decoded byte by byte. The experimental v6 layout
//! ([`crate::BITPACKED_SKIP_SPLIT_INVERTED_VERSION`]) keeps the v5 span and
//! skip table unchanged and swaps only the bytes of each *full* 128-posting
//! block for frame-of-reference bit packing in the SIMD-BP128 layout
//! (Lemire & Boytsov, "Decoding billions of integers per second through
//! vectorization"):
//!
//! ```text
//! full block (exactly 128 postings):
//!   b_doc: u8              bit width of the doc gaps (0..=32)
//!   b_tf:  u8              bit width of tf - 1       (0..=32)
//!   doc gaps: 16 * b_doc bytes   (4 * b_doc little-endian u32 words)
//!   tf - 1:   16 * b_tf bytes    (4 * b_tf  little-endian u32 words)
//! partial (last) block, < 128 postings: v5 varint pairs, byte-identical
//! ```
//!
//! Doc gaps are `doc_id - next_min`, where `next_min` is 0 for the term's
//! first block and `previous doc_id + 1` afterwards (the previous block's
//! skip-table `last_doc_id` + 1 at a block boundary), so strictly
//! increasing doc ids pack with no wasted "delta >= 1" bit. All arithmetic
//! wraps, so the codec is lossless for any `u32` input; only the bit width
//! suffers on unsorted input.
//!
//! Packing layout: 128 values as four interleaved 32-value lanes (value `i`
//! lives in lane `i % 4`), each lane a contiguous little-endian bitstream
//! of `b` words; lane word `w` is stored at word index `w * 4 + lane`. That
//! is the 128-bit-word SIMD-BP128 layout, so the per-width unpack loops
//! below are four identical scalar lanes that LLVM turns into 128-bit
//! vector code with no `unsafe` and no target-specific intrinsics.
//!
//! The partial tail block stays varint because a bit-packed block always
//! costs `16 * (b_doc + b_tf)` bytes: padding a 1-3 posting tail (the
//! common case — most terms are rare) to 128 lanes would grow the index.

use crate::{put_var_u32, take_var_u32, ScoringPosting, SCORING_BLOCK_LEN};

/// Values per bit-packed block — the scoring-block length.
pub const BP_BLOCK: usize = SCORING_BLOCK_LEN;
const LANES: usize = 4;
const PER_LANE: usize = BP_BLOCK / LANES;

/// Bytes of a full bit-packed block's header (`b_doc`, `b_tf`).
pub const BP_HEADER_LEN: usize = 2;

/// Minimum bit width that holds every value in `values`.
#[inline]
pub fn bit_width(values: &[u32]) -> u32 {
    let or = values.iter().fold(0u32, |acc, &v| acc | v);
    32 - or.leading_zeros()
}

/// Pack 128 values at `bits` bits each (every value must fit) and append
/// the `16 * bits` little-endian bytes to `out`.
pub fn pack128(values: &[u32; BP_BLOCK], bits: u32, out: &mut Vec<u8>) {
    debug_assert!(bits <= 32);
    debug_assert!(bits == 32 || values.iter().all(|&v| v >> bits == 0));
    let bits = bits as usize;
    let mut words = [0u32; BP_BLOCK];
    if bits > 0 {
        for lane in 0..LANES {
            let mut bitpos = 0usize;
            for k in 0..PER_LANE {
                let v = values[k * LANES + lane];
                let w = bitpos / 32;
                let off = bitpos % 32;
                words[w * LANES + lane] |= v << off;
                if off + bits > 32 {
                    words[(w + 1) * LANES + lane] |= v >> (32 - off);
                }
                bitpos += bits;
            }
        }
    }
    for w in &words[..bits * LANES] {
        out.extend_from_slice(&w.to_le_bytes());
    }
}

/// Unpack 128 `bits`-wide values from exactly `16 * bits` bytes. Returns
/// `None` on a width above 32 or a length mismatch.
#[inline]
pub fn unpack128(bytes: &[u8], bits: u32, out: &mut [u32; BP_BLOCK]) -> Option<()> {
    if bits > 32 || bytes.len() != bits as usize * 16 {
        return None;
    }
    unpack_bytes(bytes, bits, out);
    Some(())
}

/// One monomorphized unpack per bit width: with `B` a constant the shift
/// amounts and word indices are compile-time, the lane loop is four
/// identical operations and LLVM vectorizes it. Words are read straight
/// from the little-endian bytes (`bytes.len() == 16 * B`, checked by the
/// caller; the reslice lets LLVM drop the per-word bounds checks).
#[inline(always)]
fn unpack_fixed<const B: usize>(bytes: &[u8], out: &mut [u32; BP_BLOCK]) {
    if B == 0 {
        *out = [0; BP_BLOCK];
        return;
    }
    let bytes = &bytes[..B * 16];
    let word = |i: usize| {
        u32::from_le_bytes([
            bytes[i * 4],
            bytes[i * 4 + 1],
            bytes[i * 4 + 2],
            bytes[i * 4 + 3],
        ])
    };
    let mask: u32 = if B == 32 { u32::MAX } else { (1u32 << B) - 1 };
    for k in 0..PER_LANE {
        let bitpos = k * B;
        let w = bitpos / 32;
        let off = bitpos % 32;
        let spill = off + B > 32;
        for lane in 0..LANES {
            let mut v = word(w * LANES + lane) >> off;
            if spill {
                v |= word((w + 1) * LANES + lane) << (32 - off);
            }
            out[k * LANES + lane] = v & mask;
        }
    }
}

fn unpack_bytes(bytes: &[u8], bits: u32, out: &mut [u32; BP_BLOCK]) {
    macro_rules! dispatch {
        ($($b:literal)*) => {
            match bits {
                $($b => unpack_fixed::<$b>(bytes, out),)*
                _ => unreachable!("bit width validated by caller"),
            }
        };
    }
    dispatch!(0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32);
}

/// Encoded length of a full bit-packed block with these widths.
#[inline]
pub fn bp_block_len(b_doc: u32, b_tf: u32) -> usize {
    BP_HEADER_LEN + 16 * (b_doc as usize + b_tf as usize)
}

/// Encode one full 128-posting block in the v6 bit-packed layout.
/// `next_min` is 0 for a term's first block, else the previous block's
/// last doc id + 1 (wrapping).
pub fn encode_bp_block(
    docs: &[u32; BP_BLOCK],
    tfs: &[u32; BP_BLOCK],
    mut next_min: u32,
    out: &mut Vec<u8>,
) {
    let mut gaps = [0u32; BP_BLOCK];
    for (g, &d) in gaps.iter_mut().zip(docs) {
        *g = d.wrapping_sub(next_min);
        next_min = d.wrapping_add(1);
    }
    let mut tf1 = [0u32; BP_BLOCK];
    for (t, &tf) in tf1.iter_mut().zip(tfs) {
        *t = tf.wrapping_sub(1);
    }
    let b_doc = bit_width(&gaps);
    let b_tf = bit_width(&tf1);
    out.push(b_doc as u8);
    out.push(b_tf as u8);
    pack128(&gaps, b_doc, out);
    pack128(&tf1, b_tf, out);
}

/// Decode one full v6 bit-packed block (`bytes` = exactly the block's
/// scoring bytes) into `out` (appended). Returns the last doc id, or
/// `None` on corrupt bytes.
#[inline]
pub fn decode_bp_block(bytes: &[u8], next_min: u32, out: &mut Vec<ScoringPosting>) -> Option<u32> {
    let (&b_doc, rest) = bytes.split_first()?;
    let (&b_tf, rest) = rest.split_first()?;
    let (b_doc, b_tf) = (u32::from(b_doc), u32::from(b_tf));
    if b_doc > 32 || b_tf > 32 || bytes.len() != bp_block_len(b_doc, b_tf) {
        return None;
    }
    let (doc_bytes, tf_bytes) = rest.split_at(b_doc as usize * 16);
    let mut gaps = [0u32; BP_BLOCK];
    let mut tfs = [0u32; BP_BLOCK];
    unpack128(doc_bytes, b_doc, &mut gaps)?;
    unpack128(tf_bytes, b_tf, &mut tfs)?;
    // doc_i = doc_{i-1} + (gap_i + 1), with doc_{-1} = next_min - 1: the
    // `+ 1` is a vectorizable pass, leaving one add on the serial chain.
    for g in gaps.iter_mut() {
        *g = g.wrapping_add(1);
    }
    let mut prev = next_min.wrapping_sub(1);
    for g in gaps.iter_mut() {
        prev = prev.wrapping_add(*g);
        *g = prev;
    }
    out.extend(gaps.iter().zip(&tfs).map(|(&doc_id, &t)| ScoringPosting {
        doc_id,
        term_frequency: t.wrapping_add(1),
    }));
    Some(prev)
}

/// Encode postings as v5 varint `(doc delta, tf)` pairs; `prev_doc` is the
/// delta base (0 for a term's first block, else the previous block's last
/// doc id). Shared by v5 blocks and v6 partial tail blocks.
pub fn encode_varint_block(postings: &[ScoringPosting], mut prev_doc: u32, out: &mut Vec<u8>) {
    for p in postings {
        put_var_u32(out, p.doc_id.wrapping_sub(prev_doc));
        prev_doc = p.doc_id;
        put_var_u32(out, p.term_frequency);
    }
}

/// Decode `n` v5 varint `(doc delta, tf)` pairs from `bytes` into `out`
/// (appended). Returns the last doc id (`prev_doc` when `n == 0`), or
/// `None` on truncated/corrupt varints.
#[inline]
pub fn decode_varint_block(
    mut bytes: &[u8],
    n: usize,
    mut prev_doc: u32,
    out: &mut Vec<ScoringPosting>,
) -> Option<u32> {
    for _ in 0..n {
        let doc_delta = take_var_u32(&mut bytes)?;
        let term_frequency = take_var_u32(&mut bytes)?;
        let doc_id = prev_doc.wrapping_add(doc_delta);
        prev_doc = doc_id;
        out.push(ScoringPosting {
            doc_id,
            term_frequency,
        });
    }
    Some(prev_doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn roundtrip(values: &[u32; BP_BLOCK]) {
        let bits = bit_width(values);
        let mut bytes = Vec::new();
        pack128(values, bits, &mut bytes);
        assert_eq!(bytes.len(), bits as usize * 16);
        let mut out = [0u32; BP_BLOCK];
        unpack128(&bytes, bits, &mut out).unwrap();
        assert_eq!(&out, values);
    }

    #[test]
    fn every_bit_width_roundtrips() {
        for bits in 0..=32u32 {
            let max = if bits == 32 {
                u32::MAX
            } else {
                (1u64 << bits) as u32 - 1
            };
            let mut values = [0u32; BP_BLOCK];
            for (i, v) in values.iter_mut().enumerate() {
                // Mix of extremes and a pseudo-random fill.
                *v = match i % 3 {
                    0 => max,
                    1 => 0,
                    _ => (i as u32).wrapping_mul(2_654_435_761) & max,
                };
            }
            assert_eq!(bit_width(&values), bits);
            roundtrip(&values);
        }
    }

    #[test]
    fn unpack_rejects_bad_width_and_length() {
        let mut out = [0u32; BP_BLOCK];
        assert!(unpack128(&[0u8; 16 * 33], 33, &mut out).is_none());
        assert!(unpack128(&[0u8; 15], 1, &mut out).is_none());
        assert!(unpack128(&[], 0, &mut out).is_some());
    }

    #[test]
    fn bp_block_rejects_corruption() {
        let docs: [u32; BP_BLOCK] = std::array::from_fn(|i| i as u32 * 3);
        let tfs = [1u32; BP_BLOCK];
        let mut bytes = Vec::new();
        encode_bp_block(&docs, &tfs, 0, &mut bytes);
        let mut out = Vec::new();
        assert_eq!(decode_bp_block(&bytes, 0, &mut out), Some(127 * 3));
        // Truncated, width byte corrupted, trailing garbage.
        assert!(decode_bp_block(&bytes[..bytes.len() - 1], 0, &mut Vec::new()).is_none());
        let mut bad = bytes.clone();
        bad[0] = 40;
        assert!(decode_bp_block(&bad, 0, &mut Vec::new()).is_none());
        let mut long = bytes.clone();
        long.push(0);
        assert!(decode_bp_block(&long, 0, &mut Vec::new()).is_none());
    }

    proptest! {
        #[test]
        fn pack_unpack_any_values(values in proptest::collection::vec(any::<u32>(), BP_BLOCK), shift in 0u32..32) {
            let mut arr = [0u32; BP_BLOCK];
            for (a, v) in arr.iter_mut().zip(&values) {
                *a = v >> shift;
            }
            roundtrip(&arr);
        }

        /// v6 full-block decode == v5 varint decode on the same postings,
        /// from any block position (`next_min` / `prev_doc` base).
        #[test]
        fn bp_block_matches_varint_block(
            start in any::<u32>(),
            gaps in proptest::collection::vec(0u32..5_000, BP_BLOCK),
            tfs in proptest::collection::vec(1u32..300, BP_BLOCK),
            first_block in any::<bool>(),
        ) {
            let prev = if first_block { 0 } else { start };
            let mut doc = if first_block { 0 } else { start.wrapping_add(1) };
            let mut postings = Vec::with_capacity(BP_BLOCK);
            for (g, &tf) in gaps.iter().zip(&tfs) {
                doc = doc.wrapping_add(*g);
                postings.push(ScoringPosting { doc_id: doc, term_frequency: tf });
                doc = doc.wrapping_add(1);
            }
            let docs: [u32; BP_BLOCK] = std::array::from_fn(|i| postings[i].doc_id);
            let tf_arr: [u32; BP_BLOCK] = std::array::from_fn(|i| postings[i].term_frequency);
            let next_min = if first_block { 0 } else { prev.wrapping_add(1) };

            let mut v5 = Vec::new();
            encode_varint_block(&postings, prev, &mut v5);
            let mut v6 = Vec::new();
            encode_bp_block(&docs, &tf_arr, next_min, &mut v6);

            let mut a = Vec::new();
            let mut b = Vec::new();
            let last_a = decode_varint_block(&v5, BP_BLOCK, prev, &mut a).unwrap();
            let last_b = decode_bp_block(&v6, next_min, &mut b).unwrap();
            prop_assert_eq!(&a, &postings);
            prop_assert_eq!(&b, &postings);
            prop_assert_eq!(last_a, last_b);
        }

        /// Arbitrary (even unsorted, tf = 0) inputs still round-trip: the
        /// codec wraps instead of assuming sortedness.
        #[test]
        fn bp_block_lossless_on_arbitrary_input(
            docs in proptest::collection::vec(any::<u32>(), BP_BLOCK),
            tfs in proptest::collection::vec(any::<u32>(), BP_BLOCK),
            next_min in any::<u32>(),
        ) {
            let d: [u32; BP_BLOCK] = std::array::from_fn(|i| docs[i]);
            let t: [u32; BP_BLOCK] = std::array::from_fn(|i| tfs[i]);
            let mut bytes = Vec::new();
            encode_bp_block(&d, &t, next_min, &mut bytes);
            let mut out = Vec::new();
            prop_assert_eq!(decode_bp_block(&bytes, next_min, &mut out), Some(d[BP_BLOCK - 1]));
            for i in 0..BP_BLOCK {
                prop_assert_eq!(out[i].doc_id, d[i]);
                prop_assert_eq!(out[i].term_frequency, t[i]);
            }
        }
    }
}
