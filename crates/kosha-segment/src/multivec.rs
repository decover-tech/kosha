//! Late-interaction (ColBERT-style) multi-vector fields (#205).
//!
//! A document's value for such a field is a *bag* of token vectors — the
//! JSON `[[f32; dim]; n]` a `Vector` field carries instead of a flat
//! `[f32; dim]`. A query is a bag too, and a document scores
//!
//! ```text
//! MaxSim(Q, D) = sum over q in Q of max over d in D of cosine(q, d)
//! ```
//!
//! # Files (one pair per field, `i` = position in `Footer::multi_vectors`)
//!
//! `multivec.{i}.bags` — every bag, unit-L2-normalized, in doc_seq order:
//!
//! ```text
//! [magic "KMVB"][version u32][dim u32][doc_count u32][token_count u32]
//! doc_count x [doc_seq u32][token_start u32]     (the offsets table)
//! token_count x dim x f32                         (token vectors)
//! ```
//!
//! A document's bag is tokens `token_start .. next token_start` (or
//! `token_count` for the last one), so it is one ranged read once the
//! small offsets table is resident — the same pattern as
//! `doc_store.offsets` + `doc_store.bin`.
//!
//! `multivec.{i}.ivf` — the candidate generator: every token vector
//! clustered with the same balanced k-means the single-vector index uses
//! (`kosha_vector_spfresh::ClusterIndex`). Postings hold each token as its
//! document's doc_seq plus an int8 code of the unit token vector (one
//! scale per token), a quarter of the f32 size, so candidates can be ranked
//! by near-exact token similarities without reading any bag:
//!
//! ```text
//! [magic "KMVI"][version u32][dim u32][posting_count u32]
//! posting_count x ( dim x f32 centroid (unit) | offset u64 | count u32 )
//! entries: count x ( doc_seq u32 | scale f32 | dim x i8 )   per posting
//! ```
//!
//! The posting table (centroids and ranges) is resident once the field is
//! opened; a probed posting is one ranged read.
//!
//! # Search
//!
//! For each query token, the `nprobe` nearest centroids are probed and
//! every token in them is scored against it from its int8 code. Each
//! document holding a probed token becomes a candidate with an approximate
//! MaxSim: per query token, its best probed token similarity (a query token
//! that reached none of its tokens contributes the lowest similarity that
//! query token saw). The best `num_candidates` candidates are then
//! re-scored exactly from their f32 bags. With `nprobe` covering every
//! posting and `num_candidates` every document, the result is exact MaxSim.
//!
//! The source JSON stays in `doc_store.bin` as sent, so documents read back
//! unchanged, compaction re-derives these files from it, and binaries that
//! predate #205 still read (and compact) such segments — they only lack the
//! late-interaction files, which the next compaction by a newer binary
//! rebuilds.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use kosha_core::{Footer, KoshaError, MultiVectorField, StorageBackend};

/// Magic prefix of `multivec.{i}.bags` ("KMVB").
pub const MULTIVEC_BAGS_MAGIC: u32 = u32::from_le_bytes(*b"KMVB");
/// Magic prefix of `multivec.{i}.ivf` ("KMVI").
pub const MULTIVEC_IVF_MAGIC: u32 = u32::from_le_bytes(*b"KMVI");
/// The only format version written or read so far.
pub const MULTIVEC_FORMAT_VERSION: u32 = 1;
const BAGS_HEADER_LEN: u64 = 20;
const IVF_HEADER_LEN: usize = 16;

/// Name of field `i`'s bag store.
pub fn bags_file(i: usize) -> String {
    format!("multivec.{i}.bags")
}

/// Name of field `i`'s candidate index.
pub fn ivf_file(i: usize) -> String {
    format!("multivec.{i}.ivf")
}

/// Every late-interaction file `footer` says its segment has.
pub fn multivec_files(footer: &Footer) -> Vec<String> {
    (0..footer.multi_vectors.len())
        .flat_map(|i| [bags_file(i), ivf_file(i)])
        .collect()
}

/// Parses a `Vector` field value as a bag of token vectors: a non-empty
/// JSON array of non-empty, equal-length arrays of numbers.
pub fn parse_bag(value: &str) -> Option<Vec<Vec<f32>>> {
    let bag: Vec<Vec<f32>> = serde_json::from_str(value).ok()?;
    let dim = bag.first()?.len();
    (dim > 0 && bag.iter().all(|t| t.len() == dim)).then_some(bag)
}

/// Exact MaxSim between unit-normalized query tokens and a flat buffer of
/// unit-normalized document tokens (`dim` floats each).
fn maxsim_unit(query: &[Vec<f32>], doc: &[f32], dim: usize) -> f64 {
    query
        .iter()
        .map(|q| {
            doc.chunks_exact(dim)
                .map(|d| kosha_vector_spfresh::dot(q, d).clamp(-1.0, 1.0))
                .fold(f32::NEG_INFINITY, f32::max) as f64
        })
        .sum()
}

// ─── Writer ────────────────────────────────────────────────────────────────

struct FieldBags {
    name: String,
    dim: usize,
    /// `(doc_seq, token_start)`, doc_seq strictly increasing.
    docs: Vec<(u32, u32)>,
    /// Unit-normalized token vectors, flat.
    tokens: Vec<f32>,
}

impl FieldBags {
    fn token_count(&self) -> usize {
        self.tokens.len() / self.dim
    }
}

/// Collects bags while a segment is built; see [`Self::write`].
#[derive(Default)]
pub(crate) struct MultiVectorBuilder {
    fields: Vec<FieldBags>,
}

impl MultiVectorBuilder {
    /// Adds `bag` as document `doc_seq`'s value of field `name`. `false`
    /// (and nothing added) when the bag's dimension differs from the
    /// field's earlier bags in this segment, or the field would pass
    /// `u32::MAX` tokens. A second bag for the same document and field is
    /// appended to the first.
    pub(crate) fn add(&mut self, doc_seq: u32, name: &str, bag: &[Vec<f32>]) -> bool {
        let Some(dim) = bag.first().map(Vec::len) else {
            return false;
        };
        let index = match self.fields.iter().position(|f| f.name == name) {
            Some(i) => i,
            None => {
                self.fields.push(FieldBags {
                    name: name.to_string(),
                    dim,
                    docs: Vec::new(),
                    tokens: Vec::new(),
                });
                self.fields.len() - 1
            }
        };
        let field = &mut self.fields[index];
        if field.dim != dim || field.token_count() + bag.len() > u32::MAX as usize {
            return false;
        }
        if field.docs.last().map(|d| d.0) != Some(doc_seq) {
            field.docs.push((doc_seq, field.token_count() as u32));
        }
        for token in bag {
            let at = field.tokens.len();
            field.tokens.extend_from_slice(token);
            kosha_vector_spfresh::normalize_in_place(&mut field.tokens[at..]);
        }
        true
    }

    /// Writes every field's `.bags` and `.ivf` and returns the footer
    /// entries, in file-index order. Writes nothing when no bag was added.
    pub(crate) fn write(
        &self,
        backend: &dyn StorageBackend,
    ) -> Result<Vec<MultiVectorField>, KoshaError> {
        let mut out = Vec::with_capacity(self.fields.len());
        for field in self.fields.iter().filter(|f| !f.docs.is_empty()) {
            let i = out.len();
            backend.write(&bags_file(i), &encode_bags(field))?;
            backend.write(&ivf_file(i), &encode_ivf(field)?)?;
            out.push(MultiVectorField {
                name: field.name.clone(),
                dim: field.dim as u32,
                docs: field.docs.len() as u32,
                tokens: field.token_count() as u64,
            });
        }
        Ok(out)
    }
}

fn encode_bags(field: &FieldBags) -> Vec<u8> {
    let tokens = field.token_count();
    let mut buf = Vec::with_capacity(
        BAGS_HEADER_LEN as usize + field.docs.len() * 8 + field.tokens.len() * 4,
    );
    for v in [
        MULTIVEC_BAGS_MAGIC,
        MULTIVEC_FORMAT_VERSION,
        field.dim as u32,
        field.docs.len() as u32,
        tokens as u32,
    ] {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    for &(doc_seq, start) in &field.docs {
        buf.extend_from_slice(&doc_seq.to_le_bytes());
        buf.extend_from_slice(&start.to_le_bytes());
    }
    for x in &field.tokens {
        buf.extend_from_slice(&x.to_le_bytes());
    }
    buf
}

/// Bytes of one `.ivf` posting entry for `dim`-dimensional tokens.
fn ivf_entry_len(dim: usize) -> usize {
    8 + dim
}

/// int8 code of a unit vector: `x ~= scale * code`.
fn quantize(token: &[f32], out: &mut Vec<u8>) -> f32 {
    let max = token.iter().fold(0.0f32, |m, x| m.max(x.abs()));
    let scale = if max > 0.0 { max / 127.0 } else { 0.0 };
    out.extend(token.iter().map(|&x| {
        let code = if scale > 0.0 {
            (x / scale).round()
        } else {
            0.0
        };
        (code.clamp(-127.0, 127.0) as i8) as u8
    }));
    scale
}

fn encode_ivf(field: &FieldBags) -> Result<Vec<u8>, KoshaError> {
    let dim = field.dim;
    let points: Vec<(u32, Vec<f32>)> = field
        .tokens
        .chunks_exact(dim)
        .enumerate()
        .map(|(ordinal, t)| (ordinal as u32, t.to_vec()))
        .collect();
    let index = kosha_vector_spfresh::ClusterIndex::build(
        &points,
        kosha_vector_spfresh::ClusterIndexConfig::new(dim),
    )
    .map_err(|e| {
        KoshaError::CorruptSegment(format!(
            "failed to build the late-interaction index for field '{}': {e}",
            field.name
        ))
    })?;
    drop(points);
    let postings = index.snapshot();
    drop(index);
    let table_len = IVF_HEADER_LEN + postings.len() * (dim * 4 + 12);
    let mut table = Vec::with_capacity(table_len);
    for v in [
        MULTIVEC_IVF_MAGIC,
        MULTIVEC_FORMAT_VERSION,
        dim as u32,
        postings.len() as u32,
    ] {
        table.extend_from_slice(&v.to_le_bytes());
    }
    let starts: Vec<u32> = field.docs.iter().map(|d| d.1).collect();
    let mut entries: Vec<u8> = Vec::with_capacity(field.token_count() * ivf_entry_len(dim));
    for posting in postings {
        let mut centroid = posting.centroid;
        kosha_vector_spfresh::normalize_in_place(&mut centroid);
        for x in &centroid {
            table.extend_from_slice(&x.to_le_bytes());
        }
        let offset = (table_len + entries.len()) as u64;
        table.extend_from_slice(&offset.to_le_bytes());
        table.extend_from_slice(&(posting.entries.len() as u32).to_le_bytes());
        let mut members: Vec<u32> = posting.entries.iter().map(|(o, _)| *o).collect();
        members.sort_unstable();
        for ordinal in members {
            let doc_seq = field.docs[starts.partition_point(|&s| s <= ordinal) - 1].0;
            let at = ordinal as usize * dim;
            entries.extend_from_slice(&doc_seq.to_le_bytes());
            let scale_at = entries.len();
            entries.extend_from_slice(&[0; 4]);
            let scale = quantize(&field.tokens[at..at + dim], &mut entries);
            entries[scale_at..scale_at + 4].copy_from_slice(&scale.to_le_bytes());
        }
    }
    debug_assert_eq!(table.len(), table_len);
    table.extend_from_slice(&entries);
    Ok(table)
}

// ─── Reader ────────────────────────────────────────────────────────────────

/// Search knobs for [`MultiVectorIndex::search`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LateInteractionParams {
    /// Centroids probed per query token (widened when fewer than `k`
    /// acceptable documents turn up).
    pub nprobe: usize,
    /// Candidates re-scored exactly (at least `k`).
    pub num_candidates: usize,
}

/// What one segment's late-interaction search returned and cost.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LateInteractionHits {
    /// `(doc_seq, MaxSim)`, best first, at most `k`.
    pub hits: Vec<(u32, f64)>,
    /// Postings read, summed over query tokens.
    pub postings_probed: usize,
    /// Bags read and scored exactly.
    pub docs_rescored: usize,
}

/// One field's opened late-interaction files: the bag offsets table and
/// the posting table are resident; postings and bags are ranged reads.
#[derive(Debug)]
pub(crate) struct MultiVectorIndex {
    bags_path: PathBuf,
    ivf_path: PathBuf,
    dim: usize,
    docs: Vec<(u32, u32)>,
    token_count: u32,
    data_offset: u64,
    /// Unit centroids, flat (`dim` floats each).
    centroids: Vec<f32>,
    /// `(offset, entry count)` into `.ivf`, parallel to `centroids`.
    postings: Vec<(u64, u32)>,
}

fn u32_at(buf: &[u8], at: usize) -> Option<u32> {
    buf.get(at..at + 4)
        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn f32s(bytes: &[u8]) -> impl Iterator<Item = f32> + '_ {
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| f32::from_le_bytes(*b))
}

impl MultiVectorIndex {
    pub(crate) fn open(
        segment_dir: &Path,
        i: usize,
        meta: &MultiVectorField,
    ) -> Result<Self, KoshaError> {
        let corrupt = |file: &str, why: &str| {
            KoshaError::CorruptSegment(format!("{}/{file}: {why}", segment_dir.display()))
        };

        let bags_name = bags_file(i);
        let bags_path = segment_dir.join(&bags_name);
        let file_len = fs::metadata(&bags_path)?.len();
        let mut file = fs::File::open(&bags_path)?;
        let mut header = [0u8; BAGS_HEADER_LEN as usize];
        file.read_exact(&mut header)
            .map_err(|_| corrupt(&bags_name, "truncated header"))?;
        let h = |n: usize| u32_at(&header, n * 4).unwrap_or(0);
        if h(0) != MULTIVEC_BAGS_MAGIC || h(1) != MULTIVEC_FORMAT_VERSION {
            return Err(corrupt(&bags_name, "bad magic or unknown version"));
        }
        let (dim, doc_count, token_count) = (h(2) as usize, h(3), h(4));
        if dim == 0 || dim as u32 != meta.dim || doc_count != meta.docs {
            return Err(corrupt(&bags_name, "header disagrees with the footer"));
        }
        let data_offset = BAGS_HEADER_LEN + doc_count as u64 * 8;
        if data_offset + token_count as u64 * dim as u64 * 4 != file_len {
            return Err(corrupt(&bags_name, "file length disagrees with its header"));
        }
        let mut table = vec![0u8; doc_count as usize * 8];
        file.read_exact(&mut table)
            .map_err(|_| corrupt(&bags_name, "truncated offsets table"))?;
        let mut docs: Vec<(u32, u32)> = Vec::with_capacity(doc_count as usize);
        for pair in table.as_chunks::<8>().0 {
            let doc_seq = u32_at(pair, 0).unwrap_or(0);
            let start = u32_at(pair, 4).unwrap_or(0);
            if let Some(&(last_seq, last_start)) = docs.last() {
                if doc_seq <= last_seq || start <= last_start {
                    return Err(corrupt(&bags_name, "offsets table not increasing"));
                }
            } else if start != 0 {
                return Err(corrupt(&bags_name, "first bag does not start at token 0"));
            }
            if start >= token_count {
                return Err(corrupt(&bags_name, "bag starts past the last token"));
            }
            docs.push((doc_seq, start));
        }

        let ivf_name = ivf_file(i);
        let ivf_path = segment_dir.join(&ivf_name);
        let ivf_len = fs::metadata(&ivf_path)?.len();
        let mut file = fs::File::open(&ivf_path)?;
        let mut header = [0u8; IVF_HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|_| corrupt(&ivf_name, "truncated header"))?;
        if u32_at(&header, 0) != Some(MULTIVEC_IVF_MAGIC)
            || u32_at(&header, 4) != Some(MULTIVEC_FORMAT_VERSION)
            || u32_at(&header, 8) != Some(dim as u32)
        {
            return Err(corrupt(&ivf_name, "bad magic, version or dimension"));
        }
        let posting_count = u32_at(&header, 12).unwrap_or(0) as u64;
        let row = dim as u64 * 4 + 12;
        let table_end = IVF_HEADER_LEN as u64 + posting_count * row;
        if table_end > ivf_len {
            return Err(corrupt(&ivf_name, "posting table past end of file"));
        }
        let mut table = vec![0u8; (posting_count * row) as usize];
        file.read_exact(&mut table)
            .map_err(|_| corrupt(&ivf_name, "truncated posting table"))?;
        let entry_len = ivf_entry_len(dim) as u64;
        let mut centroids = Vec::with_capacity(posting_count as usize * dim);
        let mut postings = Vec::with_capacity(posting_count as usize);
        let mut expected = table_end;
        let mut entries_total = 0u64;
        for r in table.chunks_exact(row as usize) {
            centroids.extend(f32s(&r[..dim * 4]));
            let at = dim * 4;
            let offset = u64::from_le_bytes(r[at..at + 8].try_into().unwrap_or_default());
            let count = u32_at(r, at + 8).unwrap_or(0);
            if offset != expected {
                return Err(corrupt(&ivf_name, "postings not contiguous"));
            }
            expected += count as u64 * entry_len;
            entries_total += count as u64;
            postings.push((offset, count));
        }
        if expected != ivf_len || entries_total != token_count as u64 {
            return Err(corrupt(
                &ivf_name,
                "postings disagree with the file or bag count",
            ));
        }
        Ok(Self {
            bags_path,
            ivf_path,
            dim,
            docs,
            token_count,
            data_offset,
            centroids,
            postings,
        })
    }

    pub(crate) fn dim(&self) -> usize {
        self.dim
    }

    /// Token range `[start, end)` of `doc_seq`'s bag.
    fn bag_range(&self, doc_seq: u32) -> Option<(u32, u32)> {
        let i = self.docs.binary_search_by_key(&doc_seq, |d| d.0).ok()?;
        let end = self.docs.get(i + 1).map_or(self.token_count, |d| d.1);
        Some((self.docs[i].1, end))
    }

    /// Exact MaxSim of every document in `doc_seqs` (sorted), via one
    /// ranged read per bag. Documents without a bag are skipped.
    fn rescore(&self, query: &[Vec<f32>], doc_seqs: &[u32]) -> Result<Vec<(u32, f64)>, KoshaError> {
        let mut file = fs::File::open(&self.bags_path)?;
        let mut buf: Vec<u8> = Vec::new();
        let mut tokens: Vec<f32> = Vec::new();
        let mut out = Vec::with_capacity(doc_seqs.len());
        for &doc_seq in doc_seqs {
            let Some((start, end)) = self.bag_range(doc_seq) else {
                continue;
            };
            buf.resize((end - start) as usize * self.dim * 4, 0);
            file.seek(SeekFrom::Start(
                self.data_offset + start as u64 * self.dim as u64 * 4,
            ))?;
            file.read_exact(&mut buf)?;
            tokens.clear();
            tokens.extend(f32s(&buf));
            out.push((doc_seq, maxsim_unit(query, &tokens, self.dim)));
        }
        Ok(out)
    }

    /// Late-interaction top `k` over this field. `query` holds
    /// unit-normalized tokens of this field's dimension.
    pub(crate) fn search(
        &self,
        query: &[Vec<f32>],
        k: usize,
        params: LateInteractionParams,
        accept: Option<&dyn Fn(u32) -> bool>,
    ) -> Result<LateInteractionHits, KoshaError> {
        let posting_count = self.postings.len();
        if k == 0 || query.is_empty() || posting_count == 0 {
            return Ok(LateInteractionHits::default());
        }
        // Postings nearest-first, per query token.
        let ranked: Vec<Vec<usize>> = query
            .iter()
            .map(|q| {
                let mut s: Vec<(usize, f32)> = self
                    .centroids
                    .chunks_exact(self.dim)
                    .map(|c| kosha_vector_spfresh::dot(q, c))
                    .enumerate()
                    .collect();
                s.sort_by(|a, b| b.1.total_cmp(&a.1));
                s.into_iter().map(|(p, _)| p).collect()
            })
            .collect();

        let mut file = fs::File::open(&self.ivf_path)?;
        let entry_len = ivf_entry_len(self.dim);
        let mut buf: Vec<u8> = Vec::new();
        let mut code: Vec<f32> = vec![0.0; self.dim];
        // Per candidate document, the best approximate similarity each
        // query token reached (NEG_INFINITY = none of its tokens probed).
        let mut best: HashMap<u32, Vec<f32>> = HashMap::new();
        // Per query token, the lowest similarity it saw (the imputed
        // contribution where it reached none of a document's tokens).
        let mut floor = vec![f32::INFINITY; query.len()];
        let mut probed = 0usize;
        let mut from = 0usize;
        let mut nprobe = params.nprobe.clamp(1, posting_count);
        loop {
            for (qi, q) in query.iter().enumerate() {
                for &posting in &ranked[qi][from..nprobe] {
                    let (offset, count) = self.postings[posting];
                    buf.resize(count as usize * entry_len, 0);
                    file.seek(SeekFrom::Start(offset))?;
                    file.read_exact(&mut buf)?;
                    probed += 1;
                    for entry in buf.chunks_exact(entry_len) {
                        let doc_seq = u32_at(entry, 0).unwrap_or(0);
                        if accept.is_some_and(|accept| !accept(doc_seq)) {
                            continue;
                        }
                        let scale = f32::from_le_bytes([entry[4], entry[5], entry[6], entry[7]]);
                        for (c, &b) in code.iter_mut().zip(&entry[8..]) {
                            *c = b as i8 as f32;
                        }
                        let sim = scale * kosha_vector_spfresh::dot(q, &code);
                        floor[qi] = floor[qi].min(sim);
                        let slot = &mut best
                            .entry(doc_seq)
                            .or_insert_with(|| vec![f32::NEG_INFINITY; query.len()])[qi];
                        *slot = slot.max(sim);
                    }
                }
            }
            if best.len() >= k || nprobe == posting_count {
                break;
            }
            // Too few acceptable documents (a selective restriction):
            // probe further out rather than return a short page.
            from = nprobe;
            nprobe = (nprobe * 2).min(posting_count);
        }

        // A query token that saw no acceptable token at all adds the same
        // nothing to every candidate.
        for f in &mut floor {
            if *f == f32::INFINITY {
                *f = 0.0;
            }
        }
        let mut candidates: Vec<(u32, f32)> = best
            .into_iter()
            .map(|(doc_seq, per_token)| {
                let score = per_token
                    .iter()
                    .zip(&floor)
                    .map(|(&s, &f)| if s == f32::NEG_INFINITY { f } else { s })
                    .sum();
                (doc_seq, score)
            })
            .collect();
        let keep = params.num_candidates.max(k);
        if candidates.len() > keep {
            candidates.select_nth_unstable_by(keep - 1, |a, b| b.1.total_cmp(&a.1));
            candidates.truncate(keep);
        }
        let mut doc_seqs: Vec<u32> = candidates.into_iter().map(|(d, _)| d).collect();
        doc_seqs.sort_unstable();
        let mut hits = self.rescore(query, &doc_seqs)?;
        let docs_rescored = hits.len();
        hits.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        hits.truncate(k);
        Ok(LateInteractionHits {
            hits,
            postings_probed: probed,
            docs_rescored,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bag_accepts_only_uniform_nested_arrays() {
        assert_eq!(
            parse_bag("[[1,0],[0,1]]"),
            Some(vec![vec![1.0, 0.0], vec![0.0, 1.0]])
        );
        assert_eq!(parse_bag("[1,0]"), None, "a flat vector is not a bag");
        assert_eq!(parse_bag("[]"), None);
        assert_eq!(parse_bag("[[]]"), None);
        assert_eq!(parse_bag("[[1,0],[1]]"), None, "ragged");
        assert_eq!(parse_bag("[[\"a\"]]"), None);
    }

    #[test]
    fn builder_rejects_a_dimension_change_within_a_field() {
        let mut b = MultiVectorBuilder::default();
        assert!(b.add(0, "t", &[vec![1.0, 0.0]]));
        assert!(!b.add(1, "t", &[vec![1.0, 0.0, 0.0]]));
        assert!(
            b.add(1, "u", &[vec![1.0, 0.0, 0.0]]),
            "other fields are independent"
        );
        assert_eq!(b.fields[0].docs, vec![(0, 0)]);
    }

    #[test]
    fn int8_codes_preserve_similarity() {
        let mut t = vec![0.3f32, -0.5, 0.1, 0.8];
        kosha_vector_spfresh::normalize_in_place(&mut t);
        let mut code = Vec::new();
        let scale = quantize(&t, &mut code);
        let restored: Vec<f32> = code.iter().map(|&b| b as i8 as f32 * scale).collect();
        for (a, b) in t.iter().zip(&restored) {
            assert!((a - b).abs() <= scale * 0.5 + 1e-6);
        }
        let mut zero = Vec::new();
        assert_eq!(quantize(&[0.0, 0.0], &mut zero), 0.0);
        assert_eq!(zero, vec![0, 0]);
    }

    #[test]
    fn maxsim_unit_sums_the_best_match_per_query_token() {
        let query = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        let doc = [1.0, 0.0, 0.6, 0.8];
        // q0 best = 1.0 (d0), q1 best = 0.8 (d1).
        assert!((maxsim_unit(&query, &doc, 2) - 1.8).abs() < 1e-6);
    }
}
