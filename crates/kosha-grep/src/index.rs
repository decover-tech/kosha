//! The non-positional trigram index.

use std::collections::HashMap;
use std::io::{self, Read, Write};

use roaring::RoaringBitmap;

const MAGIC: &[u8; 4] = b"KGT1";

/// Every distinct trigram of `text`, ASCII-lowercased, never spanning a
/// newline, sorted. Non-ASCII bytes are kept as they are, so a
/// case-insensitive non-ASCII pattern is planned with each case's bytes.
pub fn trigrams(text: &[u8]) -> Vec<[u8; 3]> {
    let mut keys: Vec<u32> = Vec::new();
    for line in text.split(|&b| b == b'\n') {
        keys.extend(line.windows(3).map(|w| key([w[0], w[1], w[2]])));
    }
    keys.sort_unstable();
    keys.dedup();
    keys.into_iter().map(unkey).collect()
}

fn key(t: [u8; 3]) -> u32 {
    u32::from_be_bytes([
        0,
        t[0].to_ascii_lowercase(),
        t[1].to_ascii_lowercase(),
        t[2].to_ascii_lowercase(),
    ])
}

fn unkey(k: u32) -> [u8; 3] {
    let [_, a, b, c] = k.to_be_bytes();
    [a, b, c]
}

/// Sizes of an index, for the overhead budget.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexStats {
    pub docs: u64,
    pub raw_bytes: u64,
    pub distinct_trigrams: u64,
    /// Total (trigram, document) pairs.
    pub postings: u64,
    /// Bytes of [`TrigramIndex::write_to`] output.
    pub serialized_bytes: u64,
}

impl IndexStats {
    /// Index size as a fraction of the raw text.
    pub fn overhead(&self) -> f64 {
        if self.raw_bytes == 0 {
            0.0
        } else {
            self.serialized_bytes as f64 / self.raw_bytes as f64
        }
    }
}

/// Trigram to the documents containing it. Document ids are dense, in
/// insertion order; `names` and `lens` are indexed by them.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TrigramIndex {
    postings: HashMap<u32, RoaringBitmap>,
    names: Vec<String>,
    lens: Vec<u64>,
}

impl TrigramIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a document and returns its id.
    pub fn add(&mut self, name: impl Into<String>, text: &[u8]) -> u32 {
        let id = u32::try_from(self.names.len()).expect("fewer than 2^32 documents per index");
        for t in trigrams(text) {
            self.postings.entry(key(t)).or_default().insert(id);
        }
        self.names.push(name.into());
        self.lens.push(text.len() as u64);
        id
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    pub fn name(&self, doc: u32) -> &str {
        &self.names[doc as usize]
    }

    pub fn doc_len(&self, doc: u32) -> u64 {
        self.lens[doc as usize]
    }

    /// Documents containing trigram `t` (ASCII case-folded).
    pub fn postings(&self, t: [u8; 3]) -> Option<&RoaringBitmap> {
        self.postings.get(&key(t))
    }

    /// Every document id.
    pub fn all(&self) -> RoaringBitmap {
        let mut all = RoaringBitmap::new();
        all.insert_range(0..self.names.len() as u32);
        all
    }

    /// Compaction: merges `parts` into one index, dropping each part's
    /// deleted documents and renumbering the rest densely in order.
    pub fn merge(parts: &[(&TrigramIndex, &RoaringBitmap)]) -> TrigramIndex {
        let mut out = TrigramIndex::new();
        for (part, deleted) in parts {
            let base = out.names.len() as u32;
            // remap[old] = new id, or None when deleted.
            let mut remap = Vec::with_capacity(part.names.len());
            let mut next = base;
            for old in 0..part.names.len() as u32 {
                if deleted.contains(old) {
                    remap.push(None);
                } else {
                    remap.push(Some(next));
                    next += 1;
                    out.names.push(part.names[old as usize].clone());
                    out.lens.push(part.lens[old as usize]);
                }
            }
            for (k, bitmap) in &part.postings {
                let mapped: RoaringBitmap =
                    bitmap.iter().filter_map(|d| remap[d as usize]).collect();
                if !mapped.is_empty() {
                    *out.postings.entry(*k).or_default() |= mapped;
                }
            }
        }
        out
    }

    pub fn stats(&self) -> IndexStats {
        IndexStats {
            docs: self.names.len() as u64,
            raw_bytes: self.lens.iter().sum(),
            distinct_trigrams: self.postings.len() as u64,
            postings: self.postings.values().map(RoaringBitmap::len).sum(),
            serialized_bytes: self.serialized_size() as u64,
        }
    }

    /// Bytes of the posting section alone (what the overhead budget
    /// counts); names and lengths already live in the doc store.
    fn serialized_size(&self) -> usize {
        8 + self
            .postings
            .values()
            .map(|b| 7 + b.serialized_size())
            .sum::<usize>()
    }

    /// Writes the postings, sorted by trigram: magic, count, then per
    /// trigram its 3 bytes, a u32 length and the roaring bitmap. Names and
    /// lengths follow, so [`read_from`](Self::read_from) round-trips.
    pub fn write_to(&self, mut w: impl Write) -> io::Result<()> {
        let mut keys: Vec<&u32> = self.postings.keys().collect();
        keys.sort_unstable();
        w.write_all(MAGIC)?;
        w.write_all(&(keys.len() as u32).to_le_bytes())?;
        let mut buf = Vec::new();
        for k in keys {
            let bitmap = &self.postings[k];
            buf.clear();
            bitmap.serialize_into(&mut buf)?;
            w.write_all(&unkey(*k))?;
            w.write_all(&(buf.len() as u32).to_le_bytes())?;
            w.write_all(&buf)?;
        }
        w.write_all(&(self.names.len() as u32).to_le_bytes())?;
        for (name, len) in self.names.iter().zip(&self.lens) {
            w.write_all(&(name.len() as u32).to_le_bytes())?;
            w.write_all(name.as_bytes())?;
            w.write_all(&len.to_le_bytes())?;
        }
        Ok(())
    }

    pub fn read_from(mut r: impl Read) -> io::Result<TrigramIndex> {
        let mut magic = [0; 4];
        r.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a kosha-grep index",
            ));
        }
        let mut out = TrigramIndex::new();
        for _ in 0..read_u32(&mut r)? {
            let mut t = [0; 3];
            r.read_exact(&mut t)?;
            let len = read_u32(&mut r)? as usize;
            let mut buf = vec![0; len];
            r.read_exact(&mut buf)?;
            out.postings
                .insert(key(t), RoaringBitmap::deserialize_from(&buf[..])?);
        }
        for _ in 0..read_u32(&mut r)? {
            let mut name = vec![0; read_u32(&mut r)? as usize];
            r.read_exact(&mut name)?;
            let name = String::from_utf8(name)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let mut len = [0; 8];
            r.read_exact(&mut len)?;
            out.names.push(name);
            out.lens.push(u64::from_le_bytes(len));
        }
        Ok(out)
    }
}

fn read_u32(r: &mut impl Read) -> io::Result<u32> {
    let mut b = [0; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

/// Estimated extra bytes a positional (Zoekt-style) index would need for
/// `text`: every trigram occurrence's offset, delta-varint encoded per
/// (trigram, document) list. A lower bound, for the design comparison.
pub fn positional_bytes(text: &[u8]) -> u64 {
    let mut occurrences: Vec<(u32, u32)> = Vec::new();
    let mut start = 0usize;
    for line in text.split(|&b| b == b'\n') {
        for (i, w) in line.windows(3).enumerate() {
            occurrences.push((key([w[0], w[1], w[2]]), (start + i) as u32));
        }
        start += line.len() + 1;
    }
    occurrences.sort_unstable();
    let mut bytes = 0u64;
    let mut prev: Option<(u32, u32)> = None;
    for (k, pos) in occurrences {
        let delta = match prev {
            Some((pk, pp)) if pk == k => pos - pp,
            _ => pos,
        };
        bytes += varint_len(delta);
        prev = Some((k, pos));
    }
    bytes
}

fn varint_len(v: u32) -> u64 {
    u64::from((32 - v.leading_zeros()).max(1).div_ceil(7))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigrams_fold_ascii_and_stop_at_newlines() {
        assert_eq!(trigrams(b"AbCd\nef"), vec![*b"abc", *b"bcd"]);
        assert!(trigrams(b"ab\ncd").is_empty());
        // Non-ASCII bytes are not folded.
        assert_eq!(trigrams("É".as_bytes()).len(), 0);
        assert_eq!(trigrams("xÉ".as_bytes()), vec![[b'x', 0xC3, 0x89]]);
    }

    #[test]
    fn postings_and_round_trip() {
        let mut idx = TrigramIndex::new();
        let a = idx.add("a", b"hello world");
        let b = idx.add("b", b"HELLO\nthere");
        assert_eq!(
            idx.postings(*b"hel").unwrap().iter().collect::<Vec<_>>(),
            vec![a, b]
        );
        assert_eq!(
            idx.postings(*b"wor").unwrap().iter().collect::<Vec<_>>(),
            vec![a]
        );
        assert!(idx.postings(*b"o\nt").is_none());
        let mut buf = Vec::new();
        idx.write_to(&mut buf).unwrap();
        assert_eq!(TrigramIndex::read_from(&buf[..]).unwrap(), idx);
        assert!(idx.stats().serialized_bytes <= buf.len() as u64);
    }

    #[test]
    fn merge_drops_deleted_and_renumbers() {
        let mut x = TrigramIndex::new();
        x.add("x0", b"alpha");
        x.add("x1", b"beta");
        let mut y = TrigramIndex::new();
        y.add("y0", b"gamma alpha");
        let deleted: RoaringBitmap = [0].into_iter().collect();
        let m = TrigramIndex::merge(&[(&x, &deleted), (&y, &RoaringBitmap::new())]);
        assert_eq!(m.len(), 2);
        assert_eq!((m.name(0), m.name(1)), ("x1", "y0"));
        assert_eq!(
            m.postings(*b"alp").unwrap().iter().collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(
            m.postings(*b"bet").unwrap().iter().collect::<Vec<_>>(),
            vec![0]
        );
        let mut fresh = TrigramIndex::new();
        fresh.add("x1", b"beta");
        fresh.add("y0", b"gamma alpha");
        assert_eq!(m, fresh);
    }

    #[test]
    fn positional_is_larger() {
        let text = b"the quick brown fox jumps over the lazy dog\n".repeat(10);
        assert!(positional_bytes(&text) > 0);
    }
}
