//! Offline two-tier (head/tail) impact-index analysis — research #387.
//!
//! The `kosha-two-tier-eval` binary reads real segments and a query
//! set and answers: if every term's postings were split into a small
//! high-impact *head* (kept local) and a *tail* (left in S3), for what
//! fraction of queries does the head alone *provably* produce the exact
//! top-k? See `docs/research/387-two-tier-index.md` for the format and
//! query-path sketch this is the go/no-go measurement for.
//!
//! The simulation, per segment and query term:
//!
//! 1. **Build time.** The head is the `ceil(f · df)` postings with the
//!    highest BM25 contribution under the *segment's own* statistics —
//!    what a segment writer knows. Ties break by `doc_seq`. A term with
//!    `df < min_split_df` is not split (all local, no tail).
//! 2. **Tail bound.** Each tail stores a ceiling that holds under *any*
//!    later statistics. BM25 is increasing in `tf` and non-increasing in
//!    `dl` for every `N`, `df`, `avgdl > 0`, `k1 ≥ 0`, `b ∈ [0, 1]`, so the
//!    tail's Pareto frontier of `(max tf, min dl)` points gives the *exact*
//!    tail maximum under whatever namespace-level stats (#246) or request
//!    `bm25_params` the query brings (`exact` bound). The cheaper
//!    single-point `(max tf, min dl)` ceiling — what the v5 skip table
//!    already stores per block — is the `loose` bound.
//! 3. **Query time.** Score with namespace-level statistics (the engine
//!    default). Every document seen in some head gets a lower bound `LB`
//!    (its head contributions) and an upper bound `UB = LB + Σ tail max`
//!    over the query terms whose head it is not in; a document in no head
//!    is bounded by `Σ tail max` of its segment. The head-only answer is
//!    the top-k by `LB`, `θ` its k-th `LB`.
//!    - **set-certified**: no other document's `UB` reaches `θ` — the
//!      head-only top-k *set* is exactly the true top-k.
//!    - **rank-certified**: set-certified and, down the list, every
//!      `LB(d_i)` beats every later `UB(d_j)` — ids *and* order identical.
//!      The scores a head-only answer could return are still only `LB`s.
//!    - **score-exact**: rank-certified and every top-k document has
//!      `UB = LB` (no unresolved tail term), so ids, order *and* scores
//!      equal the exhaustive answer. This is what "identical top-10"
//!      means for a response that carries scores (the bench and the
//!      server return ids + scores); finishing a rank-certified query's
//!      scores costs the tail reads counted in `tail_gets`.
//! 4. **Oracle.** The exhaustive top-k over full postings. A certified
//!    query whose head answer differs from it is counted as a *violation*;
//!    any non-zero count means the bound logic is wrong.
//!
//! Comparisons against `θ` are strict with a relative slack of `1e-9`, so
//! exact score ties at the cutoff are never certified (the engine breaks
//! them by `_id`, which a head-only pass cannot always see); the
//! `*_with_ties` counters report how many more queries a tie-aware check
//! would certify.
//!
//! Not modelled: tombstones/deletion vectors (every posting is live; the
//! bench script refuses a manifest that has any, and cross-checks the
//! oracle against the live server), filters, phrase/AND/fuzzy/wildcard queries, multi-field (#249)
//! queries, and segments whose footer names more than one analysis chain
//! (counted in `segments_skipped_multi_reading`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use kosha_core::{Bm25Params, KoshaError, QueryOperator, SearchQuery};
use kosha_segment::{ScoringPostingsRef, SegmentReader};
use rayon::prelude::*;
use serde::Serialize;

use crate::{Bm25Scorer, CorpusStats, QueryAnalysis};

/// At most this many head fractions per run (one bit each in a `u8` mask).
pub const MAX_FRACTIONS: usize = 8;
/// Relative slack on every comparison against `θ`.
const REL_EPS: f64 = 1e-9;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StatsScope {
    /// Namespace-level `N`, `avgdl`, `df` (#246) — the engine default.
    Namespace,
    /// Each segment's own statistics (`KOSHA_BM25_STATS=segment`).
    Segment,
}

#[derive(Clone, Debug, Serialize)]
pub struct Config {
    /// Head sizes as fractions of each term's postings, e.g. 0.1/0.2/0.3.
    pub head_fractions: Vec<f64>,
    pub k: usize,
    /// Terms with fewer postings than this in a segment stay whole (no
    /// tail). 128 = one v5 scoring block.
    pub min_split_df: usize,
    pub stats: StatsScope,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            head_fractions: vec![0.1, 0.2, 0.3],
            k: 10,
            min_split_df: 128,
            stats: StatsScope::Namespace,
        }
    }
}

/// Postings in a head of fraction `f`.
pub fn head_len(df: usize, fraction: f64, min_split_df: usize) -> usize {
    if df == 0 {
        return 0;
    }
    if df < min_split_df || fraction >= 1.0 {
        return df;
    }
    ((fraction * df as f64).ceil() as usize).clamp(1, df)
}

/// For each posting, bit `j` set iff it belongs to the head of size
/// `sizes[j]`: the `sizes[j]` largest `keys`, ties to the earlier posting.
pub fn head_masks(keys: &[f64], sizes: &[usize]) -> Vec<u8> {
    debug_assert!(sizes.len() <= MAX_FRACTIONS);
    let n = keys.len();
    let mut masks = vec![0u8; n];
    let mut scratch: Vec<f64> = Vec::new();
    for (j, &h) in sizes.iter().enumerate() {
        let bit = 1u8 << j;
        if h >= n {
            masks.iter_mut().for_each(|m| *m |= bit);
            continue;
        }
        if h == 0 {
            continue;
        }
        scratch.clear();
        scratch.extend_from_slice(keys);
        scratch.select_nth_unstable_by(h - 1, |a, b| b.total_cmp(a));
        let v = scratch[h - 1];
        let greater = keys.iter().filter(|&&x| x > v).count();
        let mut quota = h - greater;
        for (i, &x) in keys.iter().enumerate() {
            if x > v {
                masks[i] |= bit;
            } else if x == v && quota > 0 {
                masks[i] |= bit;
                quota -= 1;
            }
        }
    }
    masks
}

fn varint_len(v: u32) -> u64 {
    match v {
        0..=0x7f => 1,
        0x80..=0x3fff => 2,
        0x4000..=0x1f_ffff => 3,
        0x20_0000..=0x0fff_ffff => 4,
        _ => 5,
    }
}

/// Bytes of the v5 skip-split *scoring* section (count varint, per-block
/// skip entry, per-posting doc delta + tf) for these `(doc, tf, dl)`
/// postings, in doc order — positions excluded. Matches
/// `write_postings_skip_split` byte for byte on the scoring side.
pub fn scoring_section_bytes(postings: impl Iterator<Item = (u32, u32, u32)>) -> u64 {
    const BLOCK: usize = kosha_segment::SCORING_BLOCK_LEN;
    let mut total = 0u64;
    let mut count = 0u32;
    let mut prev_doc = 0u32;
    let mut prev_last = 0u32;
    let (mut in_block, mut block_bytes, mut max_tf, mut min_dl) = (0usize, 0u64, 0u32, u32::MAX);
    let mut flush = |last: u32, block_bytes: u64, max_tf: u32, min_dl: u32, total: &mut u64| {
        *total += varint_len(last.wrapping_sub(prev_last))
            + varint_len(block_bytes as u32)
            + varint_len(max_tf)
            + varint_len(if min_dl == u32::MAX { 0 } else { min_dl })
            + block_bytes;
        prev_last = last;
    };
    for (doc, tf, dl) in postings {
        block_bytes += varint_len(doc.wrapping_sub(prev_doc)) + varint_len(tf);
        prev_doc = doc;
        max_tf = max_tf.max(tf);
        min_dl = min_dl.min(dl);
        in_block += 1;
        count += 1;
        if in_block == BLOCK {
            flush(doc, block_bytes, max_tf, min_dl, &mut total);
            (in_block, block_bytes, max_tf, min_dl) = (0, 0, 0, u32::MAX);
        }
    }
    if in_block > 0 {
        flush(prev_doc, block_bytes, max_tf, min_dl, &mut total);
    }
    total + varint_len(count)
}

pub use kosha_segment::tail_frontier as frontier_steps;

/// `F(dl)` of [`frontier_steps`]: the largest `tf` a tail posting of a
/// document with length `dl` can have, or `None`.
pub fn frontier_tf(steps: &[(u32, u32)], dl: u32) -> Option<u32> {
    let i = steps.partition_point(|&(d, _)| d <= dl);
    (i > 0).then(|| steps[i - 1].1)
}

pub(crate) fn certify_segment(
    reader: &Arc<crate::TrackedSegment>,
    terms: &[String],
    scorer: &Bm25Scorer,
    k: usize,
    count_budget: Option<usize>,
    restricted: bool,
    rejected: &impl Fn(u32) -> bool,
) -> Result<Option<crate::SegmentOutput>, KoshaError> {
    if k == 0
        || !scorer.params.k1.is_finite()
        || scorer.params.k1 < 0.0
        || !(0.0..=1.0).contains(&scorer.params.b)
    {
        return Ok(None);
    }
    // The exact union count is known without tail postings only for one
    // unrestricted posting list. Otherwise the head alone must exceed
    // the reported count cap (at equality the relation might still be eq).
    let heads = terms
        .iter()
        .map(|term| Ok(reader.tier_head(term)?.map(|head| (term, head))))
        .collect::<Result<Vec<_>, KoshaError>>()?;
    let heads: Vec<_> = heads
        .into_iter()
        .flatten()
        .map(|(term, head)| (scorer.doc_frequency(term, head.df), head))
        .collect();
    if heads.iter().all(|(_, head)| head.frontier.is_empty()) {
        return Ok(None);
    }
    let single_list = (!restricted && !heads.is_empty() && terms.iter().all(|t| t == &terms[0]))
        .then(|| heads[0].1.df as usize);
    if single_list.is_none() && count_budget.is_none() {
        return Ok(None);
    }

    let mut scores = HashMap::<u32, f64>::new();
    let mut unseen = 0.0;
    for (df, head) in &heads {
        for posting in &head.postings {
            if rejected(posting.doc_id) {
                continue;
            }
            let dl = reader
                .doc_meta(posting.doc_id)
                .ok_or_else(|| {
                    KoshaError::CorruptSegment("tier head document metadata missing".into())
                })?
                .field_length;
            *scores.entry(posting.doc_id).or_default() +=
                scorer.score_term(posting.term_frequency, *df, dl);
        }
        let mut max = 0.0f64;
        for &(dl, tf) in head.frontier {
            let score = scorer.score_term(tf, *df, dl);
            if !score.is_finite() || score < 0.0 {
                return Ok(None);
            }
            max = max.max(score);
        }
        unseen += max;
    }
    let total_hits = match single_list {
        Some(total) => total,
        None if count_budget.is_some_and(|cap| scores.len() > cap) => scores.len(),
        None => return Ok(None),
    };
    let mut top = crate::BoundedTopK::new(k);
    for (&doc, &score) in &scores {
        if !score.is_finite() || score < 0.0 {
            return Ok(None);
        }
        top.insert(score, doc, reader.doc_meta(doc).unwrap().doc_id);
    }
    if !top.is_full() {
        return Ok(None);
    }
    // Strict comparison (including a rounding margin) keeps score ties
    // from being decided without the potentially unseen document IDs.
    let floor = top.floor();
    let below_floor = |upper: f64| upper + upper.abs().max(floor.abs()) * 1e-9 < floor;
    if !below_floor(unseen) {
        return Ok(None);
    }
    let winning_docs: std::collections::HashSet<_> =
        top.entries.iter().map(|&(_, doc, _)| doc).collect();
    for (&doc, &score) in &scores {
        let dl = reader.doc_meta(doc).unwrap().field_length;
        let mut missing = 0.0;
        for (df, head) in &heads {
            if let Some(tf) = frontier_tf(head.frontier, dl) {
                if head
                    .postings
                    .binary_search_by_key(&doc, |p| p.doc_id)
                    .is_err()
                {
                    missing += scorer.score_term(tf, *df, dl);
                }
            }
        }
        if !missing.is_finite()
            || missing < 0.0
            || if winning_docs.contains(&doc) {
                missing != 0.0
            } else {
                !below_floor(score + missing)
            }
        {
            return Ok(None);
        }
    }
    Ok(Some(crate::SegmentOutput {
        candidates: top.into_candidates(reader),
        vector_candidates: Vec::new(),
        aggs: HashMap::new(),
        total_hits,
        hits_capped: single_list.is_none(),
        knn_segment_degraded: None,
        knn_failed_segment: None,
    }))
}

/// Bytes to store a tail's frontier step function: a count varint, then
/// per point the `dl` and `tf` deltas as varints (both strictly
/// ascending along the frontier). The metadata cost of `frontier_dl`.
pub fn frontier_bytes(steps: &[(u32, u32)]) -> u64 {
    let mut total = varint_len(steps.len() as u32);
    let (mut prev_dl, mut prev_tf) = (0u32, 0u32);
    for &(dl, tf) in steps {
        total += varint_len(dl.wrapping_sub(prev_dl)) + varint_len(tf.wrapping_sub(prev_tf));
        (prev_dl, prev_tf) = (dl, tf);
    }
    total
}

/// Assumed extra TOC bytes per split term for the tail span reference
/// (blob id u32 + offset u64 + length u32, uncompressed): an estimate of
/// what a v6 TOC entry adds, not a measured format.
pub const TAIL_TOC_ENTRY_BYTES_EST: u64 = 16;

/// File kinds that stay local whatever the posting layout: everything but
/// posting shards (counted as span bytes instead), the doc store body and
/// vectors (neither is part of the local text footprint).
pub fn is_fixed_local_kind(kind: &str) -> bool {
    !matches!(kind, "postings_shards" | "doc_store" | "vectors")
}

fn fixed_local_bytes(files: &HashMap<String, u64>) -> u64 {
    files
        .iter()
        .filter(|(k, _)| is_fixed_local_kind(k))
        .map(|(_, v)| *v)
        .sum()
}

/// Bounds compared side by side: 0 = frontier with the document's own
/// length, 1 = frontier max (the exact tail maximum), 2 = single
/// `(max tf, min dl)` ceiling.
pub const BOUNDS: [&str; 3] = ["frontier_dl", "frontier_max", "maxtf_mindl"];
const NB: usize = BOUNDS.len();

/// `(term, [(doc_seq, tf)])` for each query term a segment holds.
type TermPostings = Vec<(String, Vec<(u32, u32)>)>;

// ─── Segments ────────────────────────────────────────────────────────────

pub struct Segment {
    pub id: String,
    reader: SegmentReader,
    /// Field length per `doc_seq` (what the scorer normalizes with).
    lengths: Vec<u32>,
    split: bool,
}

impl Segment {
    pub fn open(dir: PathBuf) -> Result<Self, KoshaError> {
        let id = dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let split = kosha_segment::inverted_uses_posting_blobs(&dir);
        let reader = SegmentReader::open_with_options(dir, false)?;
        if reader.is_tiered() {
            return Err(KoshaError::InvalidQuery(
                "offline head selection needs an untiered segment".into(),
            ));
        }
        let lengths = (0..reader.doc_count())
            .map(|seq| reader.doc_meta(seq).map_or(0, |m| m.field_length))
            .collect();
        Ok(Self {
            id,
            reader,
            lengths,
            split,
        })
    }

    pub fn doc_count(&self) -> u32 {
        self.reader.doc_count()
    }

    fn params(&self) -> Bm25Params {
        self.reader.bm25_params().clone()
    }

    fn local_scorer(&self) -> Bm25Scorer {
        Bm25Scorer::new(
            self.reader.doc_count(),
            self.reader.avg_field_length(),
            self.params(),
        )
    }

    /// The query's distinct terms in this segment's analysis (first
    /// occurrence order) with their multiplicity: the engine's OR pass
    /// scores a repeated query term once per occurrence. `None` when the
    /// footer names several chains.
    fn query_terms(&self, analysis: &QueryAnalysis) -> Option<Vec<(String, u32)>> {
        let readings = analysis.for_segment(self.reader.footer().analysis.as_ref());
        let [reading] = readings.as_slice() else {
            return None;
        };
        let mut terms: Vec<(String, u32)> = Vec::new();
        for t in &reading.terms {
            match terms.iter_mut().find(|(u, _)| u == t) {
                Some((_, n)) => *n += 1,
                None => terms.push((t.clone(), 1)),
            }
        }
        Some(terms)
    }

    /// `(doc, tf)` of each of `terms` the segment holds.
    fn postings(&self, terms: &[String]) -> Result<TermPostings, KoshaError> {
        let mut out = Vec::new();
        let mut buf = Vec::new();
        for (term, p) in self.reader.scoring_postings_for_terms(terms) {
            let mut list = Vec::with_capacity(p.doc_count());
            match p {
                ScoringPostingsRef::Blocked(b) => {
                    for block in 0..b.block_count() {
                        if !b.read_block(block, &mut buf) {
                            return Err(KoshaError::CorruptSegment(format!(
                                "segment {}: term {term:?} block {block} failed to decode",
                                self.id
                            )));
                        }
                        list.extend(buf.iter().map(|sp| (sp.doc_id, sp.term_frequency)));
                    }
                }
                ScoringPostingsRef::Decoded(p) => {
                    list.extend(p.iter().map(|x| (x.doc_id, x.term_frequency)));
                }
            }
            out.push((term.to_string(), list));
        }
        if self.reader.take_blob_read_failure() {
            return Err(KoshaError::CorruptSegment(format!(
                "segment {}: a posting blob failed to read",
                self.id
            )));
        }
        Ok(out)
    }

    /// On-disk span bytes per term (split segments only), looked up in
    /// the TOC the reader already holds: no file read per call.
    fn span_lens(&self, terms: &[String]) -> HashMap<String, u64> {
        if !self.split {
            return HashMap::new();
        }
        terms
            .iter()
            .filter_map(|t| self.reader.posting_span_len(t).map(|len| (t.clone(), len)))
            .collect()
    }

    /// Local file bytes of this segment directory, by [`file_kind`].
    fn file_bytes(&self) -> HashMap<String, u64> {
        let mut files = HashMap::new();
        if let Ok(dir) = std::fs::read_dir(self.reader.segment_dir()) {
            for e in dir.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                let len = e.metadata().map(|m| m.len()).unwrap_or(0);
                *files.entry(file_kind(&name).to_string()).or_default() += len;
            }
        }
        files
    }

    /// The external `_id` of `doc_seq`.
    pub fn doc_id(&self, doc_seq: u32) -> Option<String> {
        self.reader.doc_meta(doc_seq).map(|m| m.doc_id.to_string())
    }

    fn dl(&self, doc: u32) -> u32 {
        self.lengths.get(doc as usize).copied().unwrap_or(0)
    }
}

pub fn open_segments(dirs: &[PathBuf]) -> Result<Vec<Segment>, KoshaError> {
    dirs.par_iter()
        .map(|d| Segment::open(d.clone()))
        .collect::<Result<Vec<_>, _>>()
}

/// Segment directories under `root`: the ids in `list` when given, else
/// every subdirectory holding a `footer.json`, sorted.
pub fn segment_dirs(root: &Path, list: Option<&[String]>) -> std::io::Result<Vec<PathBuf>> {
    if let Some(ids) = list {
        return Ok(ids.iter().map(|id| root.join(id)).collect());
    }
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(root)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.join("footer.json").is_file())
        .collect();
    dirs.sort();
    Ok(dirs)
}

// ─── Per-query analysis ──────────────────────────────────────────────────

/// One query term's data in one segment.
struct TermTiers {
    term: String,
    postings: Vec<(u32, u32)>,
    /// Query-time contribution of each posting.
    contrib: Vec<f64>,
    masks: Vec<u8>,
    /// Query-time `df` and multiplicity of the term.
    df_q: u32,
    times: f64,
    /// Per fraction: tail frontier steps, exact tail max, `(max tf, min
    /// dl)` tail ceiling, tail non-empty, head/tail scoring-section bytes,
    /// head share of position bytes (weighted by `1 + tf`).
    steps: Vec<Vec<(u32, u32)>>,
    tail_exact: Vec<f64>,
    tail_loose: Vec<f64>,
    has_tail: Vec<bool>,
    head_scoring_bytes: Vec<u64>,
    tail_scoring_bytes: Vec<u64>,
    head_pos_share: Vec<f64>,
    full_scoring_bytes: u64,
    span_len: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
struct Cand {
    seg: u32,
    doc: u32,
    lb: f64,
    ub: [f64; NB],
}

fn cmp_desc(a: f64, ad: (u32, u32), b: f64, bd: (u32, u32)) -> std::cmp::Ordering {
    b.total_cmp(&a).then(ad.cmp(&bd))
}

/// One segment's contribution to one (query, fraction).
struct SegFraction {
    /// Index into `segments`.
    seg: u32,
    top: Vec<Cand>,
    /// Max `UB` per bound over this segment's candidates outside `top`.
    rest_ub: [f64; NB],
    /// Bound on any doc in no head, per bound.
    unseen_ub: [f64; NB],
    /// Query terms whose tail is non-empty here (= tail GETs if fetched).
    tail_terms: u32,
    tail_bytes: u64,
}

/// Reusable dense per-segment accumulators.
#[derive(Default)]
struct Scratch {
    exact: Vec<f64>,
    lb: Vec<f64>,
    tm: [Vec<f64>; NB],
    seen: Vec<bool>,
    touched: Vec<u32>,
}

impl Scratch {
    fn ensure(&mut self, n: usize) {
        if self.exact.len() < n {
            self.exact.resize(n, 0.0);
            self.lb.resize(n, 0.0);
            for tm in &mut self.tm {
                tm.resize(n, 0.0);
            }
            self.seen.resize(n, false);
        }
    }
    fn reset(&mut self) {
        for &d in &self.touched {
            let d = d as usize;
            self.exact[d] = 0.0;
            self.lb[d] = 0.0;
            for tm in &mut self.tm {
                tm[d] = 0.0;
            }
            self.seen[d] = false;
        }
        self.touched.clear();
    }
}

/// What a query's head-only pass decides, for one fraction and bound.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Decision {
    pub set_certified: bool,
    pub rank_certified: bool,
    /// Rank-certified and every top-k score pinned (`UB = LB`).
    pub score_exact: bool,
    pub set_certified_with_ties: bool,
    /// Head-only top-k ∩ true top-k, out of `min(k, hits)`.
    pub overlap: usize,
    pub head_answer_exact: bool,
    pub violation: bool,
    /// Segments whose tail must be read to finish the query exactly (ids,
    /// order and scores), and the (segment, term) tail spans that costs if
    /// every query-term tail of those segments is read (an upper bound on
    /// tail GETs). Zero only for score-exact queries.
    pub tail_segments: u32,
    pub tail_gets: u32,
    pub tail_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryOutcome {
    pub query: usize,
    /// The query text as analyzed (for the server cross-check).
    pub text: String,
    pub terms: usize,
    pub hits: usize,
    /// `[fraction][bound]`, bounds in [`BOUNDS`] order.
    pub decisions: Vec<[Decision; NB]>,
    /// The exhaustive top-k: `(segment index, doc_seq, score)`.
    #[serde(skip)]
    pub oracle: Vec<(u32, u32, f64)>,
    /// The same top-k as external `(_id, score)`.
    pub oracle_hits: Vec<(String, f64)>,
}

/// Per distinct (segment, query term): bytes touched now vs. in a head.
#[derive(Clone, Debug, Default)]
struct Touched {
    span_len: Option<u64>,
    full_scoring: u64,
    head_scoring: Vec<u64>,
    head_pos_share: Vec<f64>,
    /// Per fraction: tail frontier bytes (0 when the term has no tail) and
    /// whether the term is split.
    frontier_bytes: Vec<u64>,
    split: Vec<bool>,
}

struct TwoTierAnalyzer<'a> {
    segments: &'a [Segment],
    config: Config,
}

impl TwoTierAnalyzer<'_> {
    fn sizes(&self, df: usize) -> Vec<usize> {
        self.config
            .head_fractions
            .iter()
            .map(|&f| head_len(df, f, self.config.min_split_df))
            .collect()
    }

    /// Analyze one query text. Returns `None` for a query with no terms.
    fn run_query(
        &self,
        qi: usize,
        text: &str,
        scratch: &mut Scratch,
        touched: &mut Vec<((u32, String), Touched)>,
        skipped: &mut usize,
    ) -> Result<Option<QueryOutcome>, KoshaError> {
        let query: SearchQuery = serde_json::from_value(serde_json::json!({
            "query_text": text,
            "max_results": self.config.k,
            "operator": QueryOperator::Or,
        }))
        .map_err(|e| KoshaError::InvalidQuery(e.to_string()))?;
        let analysis = QueryAnalysis::new(&query);
        let nf = self.config.head_fractions.len();
        let k = self.config.k;

        // Pass 1: terms + postings per segment, namespace df.
        type SegPostings = (u32, HashMap<String, u32>, TermPostings);
        let mut per_seg: Vec<SegPostings> = Vec::new();
        let mut df_ns: HashMap<String, u64> = HashMap::new();
        let mut max_terms = 0usize;
        for (si, seg) in self.segments.iter().enumerate() {
            let Some(terms) = seg.query_terms(&analysis) else {
                *skipped += 1;
                continue;
            };
            max_terms = max_terms.max(terms.iter().map(|(_, n)| *n as usize).sum());
            let unique: Vec<String> = terms.iter().map(|(t, _)| t.clone()).collect();
            let postings = seg.postings(&unique)?;
            for (t, p) in &postings {
                *df_ns.entry(t.clone()).or_default() += p.len() as u64;
            }
            per_seg.push((si as u32, terms.into_iter().collect(), postings));
        }
        if max_terms == 0 {
            return Ok(None);
        }
        let corpus = Arc::new(CorpusStats {
            num_docs: self.segments.iter().map(|s| s.doc_count() as u64).sum(),
            total_field_length: self
                .segments
                .iter()
                .map(|s| s.reader.footer().total_field_length)
                .sum(),
            doc_frequencies: df_ns,
            fields: HashMap::new(),
        });

        let mut seg_out: Vec<Vec<SegFraction>> = Vec::with_capacity(per_seg.len());
        let mut exact_tops: Vec<(f64, u32, u32)> = Vec::new();
        let mut hits = 0usize;
        for (si, multiplicity, postings) in per_seg {
            let seg = &self.segments[si as usize];
            let local = seg.local_scorer();
            let query_scorer = match self.config.stats {
                StatsScope::Namespace => Bm25Scorer::with_corpus(Arc::clone(&corpus), seg.params()),
                StatsScope::Segment => seg.local_scorer(),
            };
            let terms: Vec<String> = postings.iter().map(|(t, _)| t.clone()).collect();
            let spans = seg.span_lens(&terms);
            let tiers: Vec<TermTiers> = postings
                .into_iter()
                .map(|(term, list)| {
                    let df_seg = list.len();
                    let seg_df = u32::try_from(df_seg).unwrap_or(u32::MAX);
                    let df_q = query_scorer.doc_frequency(&term, seg_df);
                    let times = multiplicity.get(&term).copied().unwrap_or(1) as f64;
                    let keys: Vec<f64> = list
                        .iter()
                        .map(|&(d, tf)| local.score_term(tf, seg_df, seg.dl(d)))
                        .collect();
                    let contrib: Vec<f64> = list
                        .iter()
                        .map(|&(d, tf)| times * query_scorer.score_term(tf, df_q, seg.dl(d)))
                        .collect();
                    let masks = head_masks(&keys, &self.sizes(df_seg));
                    let mut t = TermTiers {
                        df_q,
                        times,
                        steps: vec![Vec::new(); nf],
                        span_len: spans.get(&term).copied(),
                        term,
                        full_scoring_bytes: scoring_section_bytes(
                            list.iter().map(|&(d, tf)| (d, tf, seg.dl(d))),
                        ),
                        tail_exact: vec![0.0; nf],
                        tail_loose: vec![0.0; nf],
                        has_tail: vec![false; nf],
                        head_scoring_bytes: vec![0; nf],
                        tail_scoring_bytes: vec![0; nf],
                        head_pos_share: vec![0.0; nf],
                        postings: list,
                        contrib,
                        masks,
                    };
                    let weight_total: f64 = t.postings.iter().map(|&(_, tf)| 1.0 + tf as f64).sum();
                    for j in 0..nf {
                        let bit = 1u8 << j;
                        let (mut max_tf, mut min_dl) = (0u32, u32::MAX);
                        let mut head_weight = 0.0;
                        for (i, &(d, tf)) in t.postings.iter().enumerate() {
                            if t.masks[i] & bit == 0 {
                                t.has_tail[j] = true;
                                t.tail_exact[j] = t.tail_exact[j].max(t.contrib[i]);
                                max_tf = max_tf.max(tf);
                                min_dl = min_dl.min(seg.dl(d));
                            } else {
                                head_weight += 1.0 + tf as f64;
                            }
                        }
                        if t.has_tail[j] {
                            t.tail_loose[j] =
                                times * query_scorer.block_max_score(max_tf, df_q, min_dl);
                            let mut tail: Vec<(u32, u32)> = t
                                .postings
                                .iter()
                                .zip(&t.masks)
                                .filter(|(_, m)| **m & bit == 0)
                                .map(|(&(d, tf), _)| (tf, seg.dl(d)))
                                .collect();
                            t.steps[j] = frontier_steps(&mut tail);
                        }
                        t.head_pos_share[j] = if weight_total > 0.0 {
                            head_weight / weight_total
                        } else {
                            0.0
                        };
                        let pick = |head: bool| {
                            t.postings
                                .iter()
                                .zip(&t.masks)
                                .filter(move |(_, m)| (**m & bit != 0) == head)
                                .map(|(&(d, tf), _)| (d, tf, seg.dl(d)))
                        };
                        t.head_scoring_bytes[j] = scoring_section_bytes(pick(true));
                        t.tail_scoring_bytes[j] = if t.has_tail[j] {
                            scoring_section_bytes(pick(false))
                        } else {
                            0
                        };
                    }
                    t
                })
                .collect();

            for t in &tiers {
                touched.push((
                    (si, t.term.clone()),
                    Touched {
                        span_len: t.span_len,
                        full_scoring: t.full_scoring_bytes,
                        head_scoring: t.head_scoring_bytes.clone(),
                        head_pos_share: t.head_pos_share.clone(),
                        frontier_bytes: (0..nf)
                            .map(|j| {
                                if t.has_tail[j] {
                                    frontier_bytes(&t.steps[j])
                                } else {
                                    0
                                }
                            })
                            .collect(),
                        split: t.has_tail.clone(),
                    },
                ));
            }

            // Exact scores (oracle), summed in query-term order.
            scratch.ensure(seg.doc_count() as usize);
            for t in &tiers {
                for (i, &(d, _)) in t.postings.iter().enumerate() {
                    let du = d as usize;
                    if du >= scratch.exact.len() {
                        continue;
                    }
                    if !scratch.seen[du] {
                        scratch.seen[du] = true;
                        scratch.touched.push(d);
                    }
                    scratch.exact[du] += t.contrib[i];
                }
            }
            hits += scratch.touched.len();
            let mut local_exact: Vec<(f64, u32, u32)> = scratch
                .touched
                .iter()
                .map(|&d| (scratch.exact[d as usize], si, d))
                .collect();
            top_k_by(&mut local_exact, k, |x| (x.0, (x.1, x.2)));
            exact_tops.extend(local_exact);

            // Head-only bounds, one fraction at a time.
            let tail_dl = |t: &TermTiers, j: usize, dl: u32| -> f64 {
                frontier_tf(&t.steps[j], dl)
                    .map_or(0.0, |tf| t.times * query_scorer.score_term(tf, t.df_q, dl))
            };
            let mut fractions = Vec::with_capacity(nf);
            for j in 0..nf {
                let bit = 1u8 << j;
                let with_tail: Vec<&TermTiers> = tiers.iter().filter(|t| t.has_tail[j]).collect();
                let mut sum_tail = [0.0f64; NB];
                let mut tail_bytes = 0u64;
                for t in &with_tail {
                    sum_tail[1] += t.tail_exact[j];
                    sum_tail[2] += t.tail_loose[j];
                    tail_bytes += t.tail_scoring_bytes[j];
                }
                // A document in no head: the best length for the bound is
                // a frontier breakpoint (between two, every F is constant
                // and BM25 falls as dl grows).
                let mut breaks: Vec<u32> = with_tail
                    .iter()
                    .flat_map(|t| t.steps[j].iter().map(|&(dl, _)| dl))
                    .collect();
                breaks.sort_unstable();
                breaks.dedup();
                sum_tail[0] = breaks
                    .iter()
                    .map(|&dl| with_tail.iter().map(|t| tail_dl(t, j, dl)).sum::<f64>())
                    .fold(0.0, f64::max);
                // Reuse `seen` as "in some head" for this fraction.
                for &d in &scratch.touched {
                    let d = d as usize;
                    scratch.lb[d] = 0.0;
                    for tm in &mut scratch.tm {
                        tm[d] = 0.0;
                    }
                    scratch.seen[d] = false;
                }
                let mut cands: Vec<u32> = Vec::new();
                for t in &tiers {
                    for (i, &(d, _)) in t.postings.iter().enumerate() {
                        if t.masks[i] & bit == 0 {
                            continue;
                        }
                        let du = d as usize;
                        if du >= scratch.lb.len() {
                            continue;
                        }
                        if !scratch.seen[du] {
                            scratch.seen[du] = true;
                            cands.push(d);
                        }
                        scratch.lb[du] += t.contrib[i];
                        if t.has_tail[j] {
                            scratch.tm[0][du] += tail_dl(t, j, seg.dl(d));
                            scratch.tm[1][du] += t.tail_exact[j];
                            scratch.tm[2][du] += t.tail_loose[j];
                        }
                    }
                }
                let mut all: Vec<Cand> = cands
                    .iter()
                    .map(|&d| {
                        let du = d as usize;
                        let lb = scratch.lb[du];
                        let dl = seg.dl(d);
                        let own: f64 = with_tail.iter().map(|t| tail_dl(t, j, dl)).sum();
                        let missing = [own, sum_tail[1], sum_tail[2]];
                        let mut ub = [lb; NB];
                        for b in 0..NB {
                            ub[b] = lb + (missing[b] - scratch.tm[b][du]).max(0.0);
                        }
                        Cand {
                            seg: si,
                            doc: d,
                            lb,
                            ub,
                        }
                    })
                    .collect();
                let rest = top_k_by(&mut all, k, |c| (c.lb, (c.seg, c.doc)));
                let mut rest_ub = [f64::NEG_INFINITY; NB];
                for c in &rest {
                    for (slot, ub) in rest_ub.iter_mut().zip(c.ub.iter()) {
                        *slot = slot.max(*ub);
                    }
                }
                fractions.push(SegFraction {
                    seg: si,
                    top: all,
                    rest_ub,
                    unseen_ub: sum_tail,
                    tail_terms: with_tail.len() as u32,
                    tail_bytes,
                });
            }
            // Restore `seen` invariant (true for every touched doc) before reset.
            for &d in &scratch.touched {
                scratch.seen[d as usize] = true;
            }
            scratch.reset();
            seg_out.push(fractions);
        }

        // Oracle: exhaustive top-k.
        top_k_by(&mut exact_tops, k, |x| (x.0, (x.1, x.2)));
        let oracle_scored: Vec<(u32, u32, f64)> =
            exact_tops.iter().map(|x| (x.1, x.2, x.0)).collect();
        let oracle_hits = oracle_scored
            .iter()
            .map(|&(s, d, score)| {
                let id = self.segments[s as usize]
                    .doc_id(d)
                    .unwrap_or_else(|| format!("<seg {s} doc {d}>"));
                (id, score)
            })
            .collect();

        let mut decisions = Vec::with_capacity(nf);
        for j in 0..nf {
            let mut pair: [Decision; NB] = Default::default();
            for (b, decision) in pair.iter_mut().enumerate() {
                *decision = decide(&seg_out, j, b, k, &oracle_scored);
            }
            decisions.push(pair);
        }
        Ok(Some(QueryOutcome {
            query: qi,
            text: text.to_string(),
            terms: max_terms,
            hits,
            decisions,
            oracle: oracle_scored,
            oracle_hits,
        }))
    }
}

/// Keep the top `k` of `items` (sorted, descending key) and return the rest.
fn top_k_by<T: Copy>(
    items: &mut Vec<T>,
    k: usize,
    key: impl Fn(&T) -> (f64, (u32, u32)),
) -> Vec<T> {
    let order = |a: &T, b: &T| {
        let (ka, ia) = key(a);
        let (kb, ib) = key(b);
        cmp_desc(ka, ia, kb, ib)
    };
    let rest = if items.len() > k {
        if k > 0 {
            items.select_nth_unstable_by(k - 1, order);
        }
        items.split_off(k)
    } else {
        Vec::new()
    };
    items.sort_unstable_by(order);
    rest
}

fn decide(
    seg_out: &[Vec<SegFraction>],
    j: usize,
    b: usize,
    k: usize,
    oracle_scored: &[(u32, u32, f64)],
) -> Decision {
    let oracle: Vec<(u32, u32)> = oracle_scored.iter().map(|x| (x.0, x.1)).collect();
    let tol = |x: f64| REL_EPS * x.abs().max(1e-12);
    let mut pool: Vec<Cand> = seg_out
        .iter()
        .flat_map(|s| s[j].top.iter().copied())
        .collect();
    let dropped = top_k_by(&mut pool, k, |c| (c.lb, (c.seg, c.doc)));
    let head = pool;
    let theta = if head.len() == k && k > 0 {
        head[k - 1].lb
    } else {
        0.0
    };
    let eps = REL_EPS * theta.abs().max(1e-12);
    // Can a doc with upper bound `ub` reach the cutoff?
    let reaches = |ub: f64| ub > theta - eps;
    let reaches_with_ties = |ub: f64| ub > theta + eps;

    let mut overtake = f64::NEG_INFINITY;
    for s in seg_out {
        let f = &s[j];
        overtake = overtake.max(f.rest_ub[b]).max(f.unseen_ub[b]);
    }
    for c in &dropped {
        overtake = overtake.max(c.ub[b]);
    }
    // Fewer than k candidates: any unseen doc with a positive score enters.
    let (set_cert, set_cert_ties) = if head.len() < k {
        let none = overtake <= 0.0;
        (none, none)
    } else {
        (!reaches(overtake), !reaches_with_ties(overtake))
    };

    // Rank: LB(d_i) must beat every later UB(d_j), j < k.
    let mut rank_cert = set_cert;
    if rank_cert {
        let mut suffix = f64::NEG_INFINITY;
        for c in head.iter().rev() {
            if suffix > c.lb - tol(c.lb) {
                rank_cert = false;
                break;
            }
            suffix = suffix.max(c.ub[b]);
        }
    }

    // Scores: every top-k document's tail is ruled out, so LB is its score.
    let score_exact = rank_cert && head.iter().all(|c| c.ub[b] <= c.lb + tol(c.lb));

    let head_ids: Vec<(u32, u32)> = head.iter().map(|c| (c.seg, c.doc)).collect();
    let overlap = head_ids.iter().filter(|id| oracle.contains(id)).count();
    let set_equal = overlap == oracle.len() && head_ids.len() == oracle.len();
    let scores_equal = head.len() == oracle_scored.len()
        && head
            .iter()
            .zip(oracle_scored)
            .all(|(c, o)| (c.lb - o.2).abs() <= tol(o.2));
    let violation = (set_cert && !set_equal)
        || (rank_cert && head_ids != oracle)
        || (score_exact && !scores_equal);

    // Tail work to finish exactly: segments where something could still
    // reach θ (set), plus segments of head top-k docs whose score is not
    // pinned down yet (rank).
    let mut tail_segments = 0u32;
    let mut tail_gets = 0u32;
    let mut tail_bytes = 0u64;
    if !score_exact {
        let mut needs = vec![false; seg_out.len()];
        let seg_index: HashMap<u32, usize> = seg_out
            .iter()
            .enumerate()
            .map(|(i, s)| (s[j].seg, i))
            .collect();
        for (i, s) in seg_out.iter().enumerate() {
            let f = &s[j];
            if !set_cert && (reaches(f.rest_ub[b]) || reaches(f.unseen_ub[b])) {
                needs[i] = true;
            }
        }
        for c in dropped.iter().filter(|c| !set_cert && reaches(c.ub[b])) {
            if let Some(&i) = seg_index.get(&c.seg) {
                needs[i] = true;
            }
        }
        for c in &head {
            if c.ub[b] > c.lb + tol(c.lb) {
                if let Some(&i) = seg_index.get(&c.seg) {
                    needs[i] = true;
                }
            }
        }
        for (i, s) in seg_out.iter().enumerate() {
            if needs[i] && s[j].tail_terms > 0 {
                tail_segments += 1;
                tail_gets += s[j].tail_terms;
                tail_bytes += s[j].tail_bytes;
            }
        }
    }
    Decision {
        set_certified: set_cert,
        rank_certified: rank_cert,
        score_exact,
        set_certified_with_ties: set_cert_ties,
        overlap,
        head_answer_exact: head_ids == oracle,
        violation,
        tail_segments,
        tail_gets,
        tail_bytes,
    }
}

// ─── Aggregates ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default, Serialize)]
pub struct BoundSummary {
    pub bound: &'static str,
    pub queries: usize,
    pub set_certified: f64,
    pub rank_certified: f64,
    /// Ids, order and scores pinned by the head alone: the #387 bar.
    pub score_exact: f64,
    pub set_certified_with_ties: f64,
    pub head_answer_exact: f64,
    pub mean_overlap_at_k: f64,
    pub violations: usize,
    /// Over queries that are not score-exact (they need tail reads).
    pub uncertified_queries: usize,
    pub tail_gets_mean: f64,
    pub tail_gets_p50: u32,
    pub tail_gets_p99: u32,
    pub tail_gets_max: u32,
    pub tail_segments_mean: f64,
    pub tail_bytes_mean: f64,
    /// Same certification rates bucketed by query term count.
    pub by_terms: Vec<TermBucket>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct TermBucket {
    pub terms: String,
    pub queries: usize,
    pub set_certified: f64,
    pub rank_certified: f64,
    pub score_exact: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct FractionSummary {
    pub head_fraction: f64,
    /// Per bound, in [`BOUNDS`] order.
    pub bounds: Vec<BoundSummary>,
    /// Over distinct (segment, query term) pairs the query set touches —
    /// what the warm gate makes local today vs. what a head tier would.
    pub touched_full_scoring_bytes: u64,
    pub touched_head_scoring_bytes: u64,
    pub touched_span_bytes: u64,
    pub touched_head_span_bytes_est: u64,
    /// Tail metadata a head tier adds for the touched split terms: frontier
    /// step functions (exact) and tail TOC references (estimate).
    pub touched_tail_frontier_bytes: u64,
    pub touched_tail_toc_bytes_est: u64,
    /// Local text bytes with a head tier / today, over the touched spans
    /// plus every file that stays local either way (TOC, doc-store
    /// offsets, filters, footers: [`QueryReport::fixed_local_bytes`]):
    /// `(head span est + frontier + tail TOC + fixed) / (spans + fixed)`.
    /// The #387 "3x smaller" bar is this <= 1/3. `None` without spans.
    pub touched_local_ratio: Option<f64>,
}

const TERM_BUCKETS: [(usize, usize, &str); 5] = [
    (1, 3, "1-3"),
    (4, 6, "4-6"),
    (7, 8, "7-8"),
    (9, 12, "9-12"),
    (13, usize::MAX, "13+"),
];

fn percentile(sorted: &[u32], p: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p * (sorted.len() - 1) as f64).round() as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn summarize(outcomes: &[QueryOutcome], j: usize, b: usize, k: usize) -> BoundSummary {
    let n = outcomes.len();
    let frac = |count: usize| if n == 0 { 0.0 } else { count as f64 / n as f64 };
    let d = |o: &QueryOutcome| o.decisions[j][b].clone();
    let mut gets: Vec<u32> = Vec::new();
    let (mut segs, mut bytes) = (0u64, 0u64);
    let mut s = BoundSummary {
        bound: BOUNDS[b],
        queries: n,
        ..Default::default()
    };
    let (mut set_c, mut rank_c, mut ties_c, mut exact_c, mut overlap) = (0, 0, 0, 0, 0.0);
    let mut score_c = 0;
    for o in outcomes {
        let x = d(o);
        set_c += x.set_certified as usize;
        rank_c += x.rank_certified as usize;
        score_c += x.score_exact as usize;
        ties_c += x.set_certified_with_ties as usize;
        exact_c += x.head_answer_exact as usize;
        s.violations += x.violation as usize;
        let denom = k.min(o.hits).max(1);
        overlap += x.overlap as f64 / denom as f64;
        if !x.score_exact {
            gets.push(x.tail_gets);
            segs += x.tail_segments as u64;
            bytes += x.tail_bytes;
        }
    }
    s.set_certified = frac(set_c);
    s.rank_certified = frac(rank_c);
    s.score_exact = frac(score_c);
    s.set_certified_with_ties = frac(ties_c);
    s.head_answer_exact = frac(exact_c);
    s.mean_overlap_at_k = if n == 0 { 0.0 } else { overlap / n as f64 };
    gets.sort_unstable();
    s.uncertified_queries = gets.len();
    if !gets.is_empty() {
        let m = gets.len() as f64;
        s.tail_gets_mean = gets.iter().map(|&g| g as f64).sum::<f64>() / m;
        s.tail_gets_p50 = percentile(&gets, 0.5);
        s.tail_gets_p99 = percentile(&gets, 0.99);
        s.tail_gets_max = *gets.last().unwrap();
        s.tail_segments_mean = segs as f64 / m;
        s.tail_bytes_mean = bytes as f64 / m;
    }
    for (lo, hi, label) in TERM_BUCKETS {
        let bucket: Vec<&QueryOutcome> = outcomes
            .iter()
            .filter(|o| o.terms >= lo && o.terms <= hi)
            .collect();
        if bucket.is_empty() {
            continue;
        }
        let m = bucket.len() as f64;
        s.by_terms.push(TermBucket {
            terms: label.into(),
            queries: bucket.len(),
            set_certified: bucket.iter().filter(|o| d(o).set_certified).count() as f64 / m,
            rank_certified: bucket.iter().filter(|o| d(o).rank_certified).count() as f64 / m,
            score_exact: bucket.iter().filter(|o| d(o).score_exact).count() as f64 / m,
        });
    }
    s
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryReport {
    pub config: Config,
    pub segments: usize,
    pub documents: u64,
    pub queries_total: usize,
    pub queries_analyzed: usize,
    pub queries_without_terms: usize,
    pub segments_skipped_multi_reading: usize,
    /// Queries not analyzed because the time budget ran out.
    pub queries_skipped_time_budget: usize,
    /// Segments whose postings are split (v3+, span bytes known).
    pub split_segments: usize,
    /// Local file bytes over all segments, by kind, and the part of it
    /// that stays local whatever the posting layout.
    pub local_files: HashMap<String, u64>,
    pub fixed_local_bytes: u64,
    pub fractions: Vec<FractionSummary>,
}

/// Analyze `queries` (in parallel). Returns the report and per-query rows.
pub fn analyze_queries(
    segments: &[Segment],
    config: Config,
    queries: &[String],
) -> Result<(QueryReport, Vec<QueryOutcome>), KoshaError> {
    analyze_queries_until(segments, config, queries, None)
}

/// Queries per parallel batch; the time budget is checked between batches,
/// so a budgeted run analyzes a prefix of the query file.
const QUERY_BATCH: usize = 256;

/// [`analyze_queries`], stopping before the next batch of queries once
/// `deadline` has passed (the rest count as `queries_skipped_time_budget`).
pub fn analyze_queries_until(
    segments: &[Segment],
    config: Config,
    queries: &[String],
    deadline: Option<std::time::Instant>,
) -> Result<(QueryReport, Vec<QueryOutcome>), KoshaError> {
    assert!(
        !config.head_fractions.is_empty() && config.head_fractions.len() <= MAX_FRACTIONS,
        "1..={MAX_FRACTIONS} head fractions"
    );
    let analyzer = TwoTierAnalyzer {
        segments,
        config: config.clone(),
    };
    type Acc = (
        Vec<QueryOutcome>,
        Vec<((u32, String), Touched)>,
        usize,
        usize,
    );
    let mut outcomes = Vec::new();
    let mut touched = Vec::new();
    let (mut empty, mut skipped, mut done) = (0, 0, 0usize);
    for (batch_no, batch) in queries.chunks(QUERY_BATCH).enumerate() {
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            break;
        }
        let base = batch_no * QUERY_BATCH;
        let results: Vec<Result<Acc, KoshaError>> = batch
            .par_iter()
            .enumerate()
            .fold(
                || Ok((Vec::new(), Vec::new(), 0usize, 0usize)),
                |acc: Result<Acc, KoshaError>, (i, text)| {
                    let (mut outs, mut touched, mut empty, mut skipped) = acc?;
                    let mut scratch = Scratch::default();
                    SCRATCH.with(|cell| std::mem::swap(&mut *cell.borrow_mut(), &mut scratch));
                    let r = analyzer.run_query(
                        base + i,
                        text,
                        &mut scratch,
                        &mut touched,
                        &mut skipped,
                    );
                    SCRATCH.with(|cell| std::mem::swap(&mut *cell.borrow_mut(), &mut scratch));
                    match r? {
                        Some(o) => outs.push(o),
                        None => empty += 1,
                    }
                    // Dedup touched pairs as we go to bound memory.
                    if touched.len() > 4096 {
                        dedup_touched(&mut touched);
                    }
                    Ok((outs, touched, empty, skipped))
                },
            )
            .collect();
        for r in results {
            let (o, t, e, s) = r?;
            outcomes.extend(o);
            touched.extend(t);
            empty += e;
            skipped += s;
        }
        dedup_touched(&mut touched);
        done += batch.len();
    }
    outcomes.sort_by_key(|o| o.query);
    dedup_touched(&mut touched);

    let mut local_files: HashMap<String, u64> = HashMap::new();
    for seg in segments {
        for (kind, len) in seg.file_bytes() {
            *local_files.entry(kind).or_default() += len;
        }
    }
    let fixed = fixed_local_bytes(&local_files);

    let fractions = config
        .head_fractions
        .iter()
        .enumerate()
        .map(|(j, &f)| {
            let mut fs = FractionSummary {
                head_fraction: f,
                bounds: (0..NB)
                    .map(|b| summarize(&outcomes, j, b, config.k))
                    .collect(),
                touched_full_scoring_bytes: 0,
                touched_head_scoring_bytes: 0,
                touched_span_bytes: 0,
                touched_head_span_bytes_est: 0,
                touched_tail_frontier_bytes: 0,
                touched_tail_toc_bytes_est: 0,
                touched_local_ratio: None,
            };
            for (_, t) in &touched {
                fs.touched_full_scoring_bytes += t.full_scoring;
                fs.touched_head_scoring_bytes += t.head_scoring[j];
                if let Some(span) = t.span_len {
                    fs.touched_span_bytes += span;
                    let positions = span.saturating_sub(t.full_scoring) as f64;
                    fs.touched_head_span_bytes_est +=
                        t.head_scoring[j] + (positions * t.head_pos_share[j]).round() as u64;
                    fs.touched_tail_frontier_bytes += t.frontier_bytes[j];
                    if t.split[j] {
                        fs.touched_tail_toc_bytes_est += TAIL_TOC_ENTRY_BYTES_EST;
                    }
                }
            }
            if fs.touched_span_bytes > 0 {
                let head = fs.touched_head_span_bytes_est
                    + fs.touched_tail_frontier_bytes
                    + fs.touched_tail_toc_bytes_est
                    + fixed;
                fs.touched_local_ratio = Some(head as f64 / (fs.touched_span_bytes + fixed) as f64);
            }
            fs
        })
        .collect();
    let report = QueryReport {
        segments: segments.len(),
        documents: segments.iter().map(|s| s.doc_count() as u64).sum(),
        queries_total: queries.len(),
        queries_analyzed: outcomes.len(),
        queries_without_terms: empty,
        segments_skipped_multi_reading: skipped,
        queries_skipped_time_budget: queries.len() - done,
        split_segments: segments.iter().filter(|s| s.split).count(),
        local_files,
        fixed_local_bytes: fixed,
        fractions,
        config,
    };
    Ok((report, outcomes))
}

thread_local! {
    static SCRATCH: std::cell::RefCell<Scratch> = std::cell::RefCell::new(Scratch::default());
}

fn dedup_touched(touched: &mut Vec<((u32, String), Touched)>) {
    touched.sort_by(|a, b| a.0.cmp(&b.0));
    touched.dedup_by(|a, b| a.0 == b.0);
}

// ─── Whole-index footprint ───────────────────────────────────────────────

#[derive(Clone, Debug, Default, Serialize)]
pub struct FootprintFraction {
    pub head_fraction: f64,
    pub head_postings: u64,
    pub head_scoring_bytes: u64,
    pub tail_scoring_bytes: u64,
    /// Head scoring bytes + a `(1 + tf)`-weighted share of each term's
    /// position bytes (split segments; an estimate).
    pub head_span_bytes_est: u64,
    /// Tail ceilings: Pareto-frontier points summed / max over terms.
    pub tail_frontier_points: u64,
    pub tail_frontier_points_max: u64,
    /// Bytes of those frontiers ([`frontier_bytes`]) and the estimated
    /// extra TOC bytes for the tail references
    /// ([`TAIL_TOC_ENTRY_BYTES_EST`] per split term).
    pub tail_frontier_bytes: u64,
    pub tail_toc_bytes_est: u64,
    pub split_terms: u64,
    /// Local text bytes with this head tier: head span est + frontier +
    /// tail TOC + [`Footprint::fixed_local_bytes`].
    pub local_text_bytes: u64,
    /// `local_text_bytes / Footprint::local_text_bytes_today`: the #387
    /// "3x smaller local text footprint" bar is this <= 1/3.
    pub local_text_ratio: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Footprint {
    pub segments: usize,
    pub terms: u64,
    pub postings: u64,
    /// v5 scoring section, all terms, re-encoded (exact for v5 segments).
    pub full_scoring_bytes: u64,
    /// On-disk span bytes per the TOC (split segments only).
    pub span_bytes: u64,
    /// Files in the segment directories, by kind.
    pub files: HashMap<String, u64>,
    /// Segments with split postings (span bytes known).
    pub split_segments: usize,
    /// The files that stay local whatever the posting layout (TOC,
    /// doc-store offsets, filters, footers: [`is_fixed_local_kind`]).
    pub fixed_local_bytes: u64,
    /// Local text bytes today: all spans + the fixed files.
    pub local_text_bytes_today: u64,
    pub fractions: Vec<FootprintFraction>,
}

fn file_kind(name: &str) -> &'static str {
    if name.starts_with("postings-") {
        "postings_shards"
    } else if name == "inverted.idx" {
        "inverted_toc"
    } else if name == "doc_store.bin" {
        "doc_store"
    } else if name == "doc_store.offsets" {
        "doc_store_offsets"
    } else if name.starts_with("vector") || name.starts_with("doc_vectors") {
        "vectors"
    } else if name.starts_with("filter") {
        "filters"
    } else {
        "other"
    }
}

/// Head/tail sizes over *every* term of every segment, as a writer would
/// split them at build time (segment-local statistics).
pub fn footprint(segments: &[Segment], config: &Config) -> Result<Footprint, KoshaError> {
    let nf = config.head_fractions.len();
    let per: Vec<Footprint> = segments
        .par_iter()
        .map(|seg| -> Result<Footprint, KoshaError> {
            let mut fp = Footprint {
                segments: 1,
                fractions: config
                    .head_fractions
                    .iter()
                    .map(|&f| FootprintFraction {
                        head_fraction: f,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            fp.files = seg.file_bytes();
            fp.split_segments = seg.split as usize;
            let local = seg.local_scorer();
            let all: Vec<String> = seg
                .reader
                .all_terms()
                .into_iter()
                .map(str::to_string)
                .collect();
            for chunk in all.chunks(1024) {
                let spans = seg.span_lens(chunk);
                for (term, list) in seg.postings(chunk)? {
                    let df = list.len();
                    fp.terms += 1;
                    fp.postings += df as u64;
                    let full =
                        scoring_section_bytes(list.iter().map(|&(d, tf)| (d, tf, seg.dl(d))));
                    fp.full_scoring_bytes += full;
                    let span = spans.get(&term).copied();
                    fp.span_bytes += span.unwrap_or(0);
                    let seg_df = u32::try_from(df).unwrap_or(u32::MAX);
                    let keys: Vec<f64> = list
                        .iter()
                        .map(|&(d, tf)| local.score_term(tf, seg_df, seg.dl(d)))
                        .collect();
                    let sizes: Vec<usize> = config
                        .head_fractions
                        .iter()
                        .map(|&f| head_len(df, f, config.min_split_df))
                        .collect();
                    let masks = head_masks(&keys, &sizes);
                    let weight_total: f64 = list.iter().map(|&(_, tf)| 1.0 + tf as f64).sum();
                    for (j, &size) in sizes.iter().enumerate().take(nf) {
                        let bit = 1u8 << j;
                        let ff = &mut fp.fractions[j];
                        ff.head_postings += size as u64;
                        let pick = |head: bool| {
                            list.iter()
                                .zip(&masks)
                                .filter(move |(_, m)| (**m & bit != 0) == head)
                                .map(|(&(d, tf), _)| (d, tf, seg.dl(d)))
                        };
                        let head_bytes = scoring_section_bytes(pick(true));
                        ff.head_scoring_bytes += head_bytes;
                        if sizes[j] < df {
                            ff.split_terms += 1;
                            ff.tail_scoring_bytes += scoring_section_bytes(pick(false));
                            let mut tail: Vec<(u32, u32)> =
                                pick(false).map(|(_, tf, dl)| (tf, dl)).collect();
                            let steps = frontier_steps(&mut tail);
                            let points = steps.len() as u64;
                            ff.tail_frontier_bytes += frontier_bytes(&steps);
                            ff.tail_toc_bytes_est += TAIL_TOC_ENTRY_BYTES_EST;
                            ff.tail_frontier_points += points;
                            ff.tail_frontier_points_max = ff.tail_frontier_points_max.max(points);
                        }
                        let head_weight: f64 = list
                            .iter()
                            .zip(&masks)
                            .filter(|(_, m)| **m & bit != 0)
                            .map(|(&(_, tf), _)| 1.0 + tf as f64)
                            .sum();
                        let positions = span.map_or(0, |s| s.saturating_sub(full)) as f64;
                        let share = if weight_total > 0.0 {
                            head_weight / weight_total
                        } else {
                            0.0
                        };
                        ff.head_span_bytes_est += head_bytes + (positions * share).round() as u64;
                    }
                }
            }
            Ok(fp)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut total = Footprint {
        fractions: config
            .head_fractions
            .iter()
            .map(|&f| FootprintFraction {
                head_fraction: f,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    for fp in per {
        total.segments += fp.segments;
        total.terms += fp.terms;
        total.postings += fp.postings;
        total.full_scoring_bytes += fp.full_scoring_bytes;
        total.span_bytes += fp.span_bytes;
        total.split_segments += fp.split_segments;
        for (kind, len) in fp.files {
            *total.files.entry(kind).or_default() += len;
        }
        for (t, f) in total.fractions.iter_mut().zip(fp.fractions) {
            t.head_postings += f.head_postings;
            t.head_scoring_bytes += f.head_scoring_bytes;
            t.tail_scoring_bytes += f.tail_scoring_bytes;
            t.head_span_bytes_est += f.head_span_bytes_est;
            t.tail_frontier_points += f.tail_frontier_points;
            t.tail_frontier_points_max = t.tail_frontier_points_max.max(f.tail_frontier_points_max);
            t.tail_frontier_bytes += f.tail_frontier_bytes;
            t.tail_toc_bytes_est += f.tail_toc_bytes_est;
            t.split_terms += f.split_terms;
        }
    }
    total.fixed_local_bytes = fixed_local_bytes(&total.files);
    total.local_text_bytes_today = total.span_bytes + total.fixed_local_bytes;
    for f in &mut total.fractions {
        f.local_text_bytes = f.head_span_bytes_est
            + f.tail_frontier_bytes
            + f.tail_toc_bytes_est
            + total.fixed_local_bytes;
        f.local_text_ratio = (total.span_bytes > 0)
            .then(|| f.local_text_bytes as f64 / total.local_text_bytes_today as f64);
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_len_respects_min_split_and_rounding() {
        assert_eq!(head_len(0, 0.1, 128), 0);
        assert_eq!(head_len(100, 0.1, 128), 100, "below min split stays whole");
        assert_eq!(head_len(1000, 0.1, 128), 100);
        assert_eq!(head_len(1001, 0.1, 128), 101, "ceil");
        assert_eq!(head_len(1000, 1.0, 0), 1000);
        assert_eq!(head_len(5, 0.01, 0), 1, "at least one posting");
    }

    #[test]
    fn head_masks_pick_largest_with_doc_order_ties() {
        let keys = [1.0, 3.0, 2.0, 3.0, 3.0, 0.5];
        let m = head_masks(&keys, &[2, 4, 6, 0]);
        // h=2: the two first 3.0s (indices 1, 3).
        let bit0: Vec<usize> = (0..6).filter(|&i| m[i] & 1 != 0).collect();
        assert_eq!(bit0, vec![1, 3]);
        // h=4: all three 3.0s and the 2.0.
        let bit1: Vec<usize> = (0..6).filter(|&i| m[i] & 2 != 0).collect();
        assert_eq!(bit1, vec![1, 2, 3, 4]);
        assert!(m.iter().all(|x| x & 4 != 0), "h = n takes everything");
        assert!(m.iter().all(|x| x & 8 == 0), "h = 0 takes nothing");
    }

    #[test]
    fn scoring_bytes_count_varints_and_skip_entries() {
        // One posting: count(1) + skip entry (last=5:1, len=2:1, tf=1:1,
        // dl=10:1) + delta 5 (1) + tf 1 (1) = 7.
        assert_eq!(scoring_section_bytes([(5, 1, 10)].into_iter()), 7);
        // 129 postings = two blocks.
        let many: Vec<(u32, u32, u32)> = (0..129).map(|d| (d, 1, 1)).collect();
        let bytes = scoring_section_bytes(many.into_iter());
        // count 129 -> 2 bytes; block 1: last 127 (1) len 256 (2) tf (1)
        // dl (1) + 256; block 2: last delta 1 (1) len 2 (1) tf dl (2) + 2.
        assert_eq!(bytes, 2 + (5 + 256) + (4 + 2));
    }

    #[test]
    fn frontier_keeps_only_undominated_points_as_steps() {
        let mut tail = vec![(3, 50), (3, 40), (2, 10), (1, 5), (2, 30), (1, 20)];
        // (tf 1, dl 5) (2, 10) (3, 40) are undominated.
        let steps = frontier_steps(&mut tail);
        assert_eq!(steps, vec![(5, 1), (10, 2), (40, 3)]);
        assert_eq!(frontier_tf(&steps, 4), None);
        assert_eq!(frontier_tf(&steps, 5), Some(1));
        assert_eq!(frontier_tf(&steps, 39), Some(2));
        assert_eq!(frontier_tf(&steps, 1000), Some(3));
        // Every tail posting is under F(its own dl).
        for &(tf, dl) in tail.iter() {
            assert!(frontier_tf(&steps, dl).unwrap() >= tf);
        }
    }

    #[test]
    fn frontier_bytes_are_delta_varints() {
        // count 3 (1) + (5,1) (1+1) + (10-5, 2-1) (1+1) + (200-10, 3-2) (2+1).
        assert_eq!(frontier_bytes(&[(5, 1), (10, 2), (200, 3)]), 8);
        assert_eq!(frontier_bytes(&[]), 1);
        assert!(is_fixed_local_kind("inverted_toc"));
        assert!(is_fixed_local_kind("doc_store_offsets"));
        assert!(!is_fixed_local_kind("postings_shards"));
        assert!(!is_fixed_local_kind("doc_store"));
    }

    #[test]
    fn top_k_by_splits_and_sorts() {
        let mut v = vec![(1.0, 0, 0), (5.0, 0, 1), (3.0, 1, 0), (5.0, 0, 0)];
        let rest = top_k_by(&mut v, 2, |x| (x.0, (x.1, x.2)));
        assert_eq!(v, vec![(5.0, 0, 0), (5.0, 0, 1)]);
        assert_eq!(rest.len(), 2);
    }
}
