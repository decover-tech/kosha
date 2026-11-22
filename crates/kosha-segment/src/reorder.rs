//! Document reordering by recursive graph bisection ("BP", #385).
//!
//! A segment's `doc_seq`s are assigned in the order documents reach
//! [`crate::SegmentWriter::add_document`]. With related documents scattered,
//! every posting block mixes high and low impacts, so block-max pruning
//! skips less, and doc-id gaps are large, so postings compress worse.
//! Recursive graph bisection (Dhulipala et al., KDD'16; the variant
//! reproduced by Mackenzie et al., 2019, and used by PISA) finds an order
//! that clusters documents sharing terms.
//!
//! This module only computes a permutation. Callers apply it by *adding
//! documents to the writer in that order*, so every per-document structure
//! the writer keys by `doc_seq` (postings, `doc_meta`/field lengths, the doc
//! store, filter columns, vectors and the `doc_vectors.bin` sidecar,
//! multi-vectors) is remapped consistently by construction. A caller that
//! also tracks `doc_seq`-keyed state of its own (compaction's tombstones
//! and vector seeds) remaps it from the same permutation.
//!
//! Ranking does not depend on `doc_seq`: BM25 inputs (tf, field length,
//! df, average length) are permutation invariant and ties break on `_id`,
//! so a reordered segment returns the same top-k as the original order.
//!
//! Off by default ([`DocReorder::from_env`], `KOSHA_DOC_REORDER`). With it
//! off, nothing calls into this module and segments are byte-identical to
//! before. The segment format is unchanged either way: a reordered segment
//! is an ordinary segment whose documents arrived in a different order, so
//! older readers read it.

use std::sync::OnceLock;

/// Whether builders reorder documents before writing a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DocReorder {
    /// Ingest order (the default; what every release before #385 wrote).
    #[default]
    Off,
    /// Recursive graph bisection over the documents' analyzed terms.
    Bp,
}

impl DocReorder {
    /// `KOSHA_DOC_REORDER` (`off` | `bp`), read once. Unset or unknown is
    /// `off`.
    pub fn from_env() -> Self {
        static MODE: OnceLock<DocReorder> = OnceLock::new();
        *MODE.get_or_init(|| Self::parse(std::env::var("KOSHA_DOC_REORDER").ok().as_deref()))
    }

    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") | Some("off") | Some("none") | Some("0") | Some("false") => Self::Off,
            Some("bp") => Self::Bp,
            Some(other) => {
                eprintln!("WARN: KOSHA_DOC_REORDER={other} not off|bp — using off");
                Self::Off
            }
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Bp => "bp",
        }
    }
}

/// Tuning for [`bp_order`]. The defaults follow PISA's `reorder-docids
/// --recursive-graph-bisection` (20 iterations, stop when a partition is
/// small).
#[derive(Debug, Clone)]
pub struct BpConfig {
    /// Swap rounds per bisection (stops early when a round swaps nothing).
    pub iterations: usize,
    /// Partitions at or below this size are not split further.
    pub min_partition: usize,
    /// Maximum recursion depth; `None` = `log2(n) - log2(min_partition)`.
    pub max_depth: Option<usize>,
    /// Terms in fewer documents than this are ignored: a term in one
    /// document has the same cost wherever that document goes.
    pub min_df: usize,
    /// Terms in more than this fraction of the documents are ignored: they
    /// are in nearly every block whatever the order, and dominate the cost
    /// of each round.
    pub max_df_fraction: f64,
    /// Partitions larger than this bisect their two halves on two threads.
    pub parallel_min_docs: usize,
}

impl Default for BpConfig {
    fn default() -> Self {
        Self {
            iterations: 20,
            min_partition: 16,
            max_depth: None,
            min_df: 2,
            max_df_fraction: 0.5,
            parallel_min_docs: 4096,
        }
    }
}

/// Interns terms to dense ids and records each document's distinct terms:
/// the forward index [`bp_order`] runs over.
#[derive(Debug, Default)]
pub struct ForwardIndex {
    ids: std::collections::HashMap<String, u32>,
    docs: Vec<Vec<u32>>,
}

impl ForwardIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends one document (its terms in any order, repeats allowed).
    pub fn push_doc<I, S>(&mut self, terms: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut ids: Vec<u32> = Vec::new();
        for term in terms {
            let term = term.as_ref();
            let next = self.ids.len() as u32;
            let id = match self.ids.get(term) {
                Some(&id) => id,
                None => {
                    self.ids.insert(term.to_string(), next);
                    next
                }
            };
            ids.push(id);
        }
        ids.sort_unstable();
        ids.dedup();
        self.docs.push(ids);
    }

    pub fn len(&self) -> usize {
        self.docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    pub fn num_terms(&self) -> usize {
        self.ids.len()
    }

    /// The BP order of the documents pushed so far: `order[new] = old`,
    /// where `old` is the push index.
    pub fn bp_order(self, config: &BpConfig) -> Vec<u32> {
        let num_terms = self.num_terms();
        bp_order(self.docs, num_terms, config)
    }
}

/// Recursive graph bisection over a forward index (`docs[d]` = distinct
/// term ids of document `d`, each `< num_terms`). Returns the new order as
/// `order[new_position] = old_index`. Deterministic: the same input always
/// gives the same order, whatever the thread count.
pub fn bp_order(mut docs: Vec<Vec<u32>>, num_terms: usize, config: &BpConfig) -> Vec<u32> {
    let n = docs.len();
    let mut order: Vec<u32> = (0..n as u32).collect();
    if n <= config.min_partition.max(1) || num_terms == 0 {
        return order;
    }
    // Drop terms that cannot move the objective (df < min_df) or that are
    // in most documents anyway, then renumber the rest densely.
    let mut df = vec![0u32; num_terms];
    for doc in &docs {
        for &t in doc {
            df[t as usize] += 1;
        }
    }
    let max_df = ((n as f64) * config.max_df_fraction).max(config.min_df as f64) as u32;
    let mut remap = vec![u32::MAX; num_terms];
    let mut kept = 0u32;
    for (t, &f) in df.iter().enumerate() {
        if f as usize >= config.min_df && f <= max_df {
            remap[t] = kept;
            kept += 1;
        }
    }
    if kept == 0 {
        return order;
    }
    for doc in &mut docs {
        doc.retain_mut(|t| {
            let m = remap[*t as usize];
            *t = m;
            m != u32::MAX
        });
    }
    let min_partition = config.min_partition.max(2);
    let max_depth = config.max_depth.unwrap_or_else(|| {
        let log_n = usize::BITS - n.leading_zeros();
        let log_leaf = usize::BITS - min_partition.leading_zeros();
        log_n.saturating_sub(log_leaf) as usize + 1
    });
    // log2 lookup for 0..=n+1; `log2(0)` is never used (degrees are
    // offset by one), so it is left at 0.
    let log2: Vec<f32> = (0..=n + 1)
        .map(|i| if i == 0 { 0.0 } else { (i as f32).log2() })
        .collect();
    let ctx = Ctx {
        docs: &docs,
        num_terms: kept as usize,
        log2: &log2,
        iterations: config.iterations,
        min_partition,
        max_depth,
        parallel_min_docs: config.parallel_min_docs.max(min_partition),
    };
    bisect(&ctx, &mut order, 0);
    order
}

struct Ctx<'a> {
    docs: &'a [Vec<u32>],
    num_terms: usize,
    log2: &'a [f32],
    iterations: usize,
    min_partition: usize,
    max_depth: usize,
    parallel_min_docs: usize,
}

thread_local! {
    /// Per-thread degree scratch (`num_terms` entries each, all zero
    /// between uses), so deep levels do not allocate per partition.
    static SCRATCH: std::cell::RefCell<(Vec<u32>, Vec<u32>)> =
        const { std::cell::RefCell::new((Vec::new(), Vec::new())) };
}

fn bisect(ctx: &Ctx<'_>, part: &mut [u32], depth: usize) {
    if part.len() <= ctx.min_partition || depth >= ctx.max_depth {
        return;
    }
    // The scratch borrow ends before recursing, so a task the recursion
    // runs on this thread never sees it borrowed.
    SCRATCH.with(|s| {
        let (left_deg, right_deg) = &mut *s.borrow_mut();
        if left_deg.len() < ctx.num_terms {
            left_deg.resize(ctx.num_terms, 0);
            right_deg.resize(ctx.num_terms, 0);
        }
        partition(ctx, part, left_deg, right_deg);
    });
    let mid = part.len() / 2;
    let (left, right) = part.split_at_mut(mid);
    if left.len() + right.len() >= ctx.parallel_min_docs {
        rayon::join(
            || bisect(ctx, left, depth + 1),
            || bisect(ctx, right, depth + 1),
        );
    } else {
        bisect(ctx, left, depth + 1);
        bisect(ctx, right, depth + 1);
    }
}

/// One bisection: split `part` into halves (first half left), then swap
/// documents between them while that lowers the log-gap cost estimate.
/// Leaves `left_deg`/`right_deg` all zero.
fn partition(ctx: &Ctx<'_>, part: &mut [u32], left_deg: &mut [u32], right_deg: &mut [u32]) {
    let mid = part.len() / 2;
    let (n1, n2) = (mid, part.len() - mid);
    for &d in &part[..mid] {
        for &t in &ctx.docs[d as usize] {
            left_deg[t as usize] += 1;
        }
    }
    for &d in &part[mid..] {
        for &t in &ctx.docs[d as usize] {
            right_deg[t as usize] += 1;
        }
    }
    let (log_n1, log_n2) = (ctx.log2[n1], ctx.log2[n2]);
    let log2 = ctx.log2;
    // Cost of a term with degrees (a, b) in halves of sizes (n1, n2): the
    // approximate bits of its log-encoded gaps, `a*log2(n1/(a+1)) +
    // b*log2(n2/(b+1))`.
    let cost = |a: u32, b: u32| -> f32 {
        a as f32 * (log_n1 - log2[a as usize + 1]) + b as f32 * (log_n2 - log2[b as usize + 1])
    };
    let mut gains_l: Vec<(f32, u32)> = Vec::with_capacity(n1);
    let mut gains_r: Vec<(f32, u32)> = Vec::with_capacity(n2);
    for _ in 0..ctx.iterations {
        gains_l.clear();
        gains_r.clear();
        for &d in &part[..mid] {
            let mut g = 0.0f32;
            for &t in &ctx.docs[d as usize] {
                let (a, b) = (left_deg[t as usize], right_deg[t as usize]);
                g += cost(a, b) - cost(a - 1, b + 1);
            }
            gains_l.push((g, d));
        }
        for &d in &part[mid..] {
            let mut g = 0.0f32;
            for &t in &ctx.docs[d as usize] {
                let (a, b) = (left_deg[t as usize], right_deg[t as usize]);
                g += cost(a, b) - cost(a + 1, b - 1);
            }
            gains_r.push((g, d));
        }
        // Highest gain first; equal gains by document index, so the result
        // never depends on sort stability or thread timing.
        let by_gain = |x: &(f32, u32), y: &(f32, u32)| y.0.total_cmp(&x.0).then(x.1.cmp(&y.1));
        gains_l.sort_unstable_by(by_gain);
        gains_r.sort_unstable_by(by_gain);
        let mut swaps = 0usize;
        for (&(gl, dl), &(gr, dr)) in gains_l.iter().zip(gains_r.iter()) {
            if gl + gr <= 0.0 {
                break;
            }
            for &t in &ctx.docs[dl as usize] {
                left_deg[t as usize] -= 1;
                right_deg[t as usize] += 1;
            }
            for &t in &ctx.docs[dr as usize] {
                right_deg[t as usize] -= 1;
                left_deg[t as usize] += 1;
            }
            swaps += 1;
        }
        if swaps == 0 {
            break;
        }
        // New halves: the left keeps its unswapped documents and receives
        // the right's top `swaps`, and vice versa. Each half is kept sorted
        // by document index, so the next round and the leaves are
        // deterministic.
        let mut left: Vec<u32> = gains_l[swaps..].iter().map(|&(_, d)| d).collect();
        left.extend(gains_r[..swaps].iter().map(|&(_, d)| d));
        let mut right: Vec<u32> = gains_r[swaps..].iter().map(|&(_, d)| d).collect();
        right.extend(gains_l[..swaps].iter().map(|&(_, d)| d));
        left.sort_unstable();
        right.sort_unstable();
        part[..mid].copy_from_slice(&left);
        part[mid..].copy_from_slice(&right);
    }
    for &d in part.iter() {
        for &t in &ctx.docs[d as usize] {
            left_deg[t as usize] = 0;
            right_deg[t as usize] = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_defaults_off() {
        assert_eq!(DocReorder::parse(None), DocReorder::Off);
        assert_eq!(DocReorder::parse(Some("")), DocReorder::Off);
        assert_eq!(DocReorder::parse(Some("off")), DocReorder::Off);
        assert_eq!(DocReorder::parse(Some("BP")), DocReorder::Bp);
        assert_eq!(DocReorder::parse(Some("graph")), DocReorder::Off);
    }

    fn is_permutation(order: &[u32]) -> bool {
        let mut seen = vec![false; order.len()];
        for &d in order {
            if d as usize >= order.len() || seen[d as usize] {
                return false;
            }
            seen[d as usize] = true;
        }
        true
    }

    /// Documents from 8 topics in random order (each samples words from its
    /// topic's vocabulary plus shared noise): BP must make neighbours share
    /// a topic far more often than the 1-in-8 of random order.
    #[test]
    fn clusters_documents_by_topic() {
        let mut state = 0x0385_0385_0385_0385u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut fwd = ForwardIndex::new();
        let mut topics = Vec::new();
        for _ in 0..2048 {
            let topic = next() % 8;
            topics.push(topic);
            let mut terms: Vec<String> = (0..10)
                .map(|_| format!("t{topic}w{}", next() % 30))
                .collect();
            terms.extend((0..3).map(|_| format!("noise{}", next() % 200)));
            fwd.push_doc(terms);
        }
        let same = |order: &[u32]| {
            order
                .windows(2)
                .filter(|w| topics[w[0] as usize] == topics[w[1] as usize])
                .count() as f64
                / (order.len() - 1) as f64
        };
        let identity: Vec<u32> = (0..2048).collect();
        let order = fwd.bp_order(&BpConfig::default());
        assert!(is_permutation(&order));
        let (before, after) = (same(&identity), same(&order));
        assert!(before < 0.2, "random order: {before}");
        assert!(after > 0.8, "BP order: same-topic neighbours {after}");
    }

    #[test]
    fn deterministic_and_parallel_safe() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let docs: Vec<Vec<u32>> = (0..20_000)
            .map(|_| {
                let topic = (next() % 50) as u32;
                let mut d: Vec<u32> = (0..12).map(|_| topic * 40 + (next() % 40) as u32).collect();
                d.push(2000 + (next() % 500) as u32);
                d.sort_unstable();
                d.dedup();
                d
            })
            .collect();
        let serial = BpConfig {
            parallel_min_docs: usize::MAX,
            ..BpConfig::default()
        };
        let a = bp_order(docs.clone(), 2500, &BpConfig::default());
        let b = bp_order(docs, 2500, &serial);
        assert!(is_permutation(&a));
        assert_eq!(a, b);
    }

    #[test]
    fn tiny_and_empty_inputs_keep_order() {
        assert_eq!(
            bp_order(Vec::new(), 0, &BpConfig::default()),
            Vec::<u32>::new()
        );
        let docs = vec![vec![0], vec![1], vec![0]];
        assert_eq!(bp_order(docs, 2, &BpConfig::default()), vec![0, 1, 2]);
    }
}
