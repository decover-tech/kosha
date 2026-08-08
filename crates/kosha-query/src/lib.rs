use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use rayon::prelude::*;

use kosha_core::{
    segment_may_contain_terms, segment_may_match, AggBucket, AggBucketResult, AggCompositeBucket,
    AggCompositeResult, AggMetricResult, Aggregation, AggregationResults, Bm25Params, DocumentId,
    FieldType, FilterClause, FilterStore, KoshaError, Manifest, ManifestEntry, NamespaceId,
    ScoredDocument, SearchQuery, SearchResult, SortSpec, TermBloomMode,
};
use kosha_segment::{tokenize, SegmentReader};

/// Optional callback the searcher invokes to ensure `doc_store.bin` is local
/// for the segments that hold the materialize page hits — the on-demand half
/// of scoring-set-only hydration. See
/// [`Searcher::search_with_doc_store_hydrator`]. `None` materializes straight
/// from disk (warm / local-dev, where `doc_store.bin` is already present).
pub type DocStoreHydrator<'a> = Option<&'a dyn Fn(&[PathBuf])>;

/// Default number of parsed segments kept resident in memory (see
/// [`SegmentCache`]). Segments are immutable once written (DESIGN.md §6.2),
/// so caching a parsed segment indefinitely is safe — the only reason to
/// bound this is memory, not staleness.
pub const DEFAULT_SEGMENT_CACHE_CAPACITY: usize = 64;

/// Default byte budget for [`SegmentCache`] (see its doc comment for why
/// this exists at all). Deliberately conservative and independent of
/// container memory limits — a query that needs more resident segments than
/// this just re-parses on a cache miss (slower, still correct) rather than
/// growing the cache without bound.
pub const DEFAULT_SEGMENT_CACHE_MAX_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Default live-bytes watermark, as a multiple of the segment cache's byte
/// budget (see [`MemoryLedger`]). Live bytes exceed the cache budget exactly
/// when in-flight requests pin segments beyond what the idle cache would
/// hold, so "2× the cache budget" means: roughly one full extra cache's
/// worth of concurrently-pinned segments before new searches start queueing.
pub const DEFAULT_LIVE_BYTES_FACTOR: u64 = 2;

/// Default time a search waits for live segment memory to free up before
/// being shed with [`KoshaError::Overloaded`] (see [`MemoryLedger::admit`]).
pub const DEFAULT_ADMISSION_TIMEOUT: Duration = Duration::from_secs(15);

// ─── Live-memory ledger & admission control ─────────────────────────────────
//
// The segment cache below bounds *idle* memory: segments kept resident
// between requests. It cannot bound *live* memory — segments pinned by
// `Arc` clones held by in-flight requests. Evicting a pinned entry from the
// cache removes it from the cache's bookkeeping but frees nothing: the
// request's own `Arc` keeps the parsed segment (including `doc_store.bin`'s
// heap copy) alive until the request finishes. Under concurrent broad
// queries (nothing to bloom-prune → every segment in the manifest pinned per
// request), N in-flight requests can pin N × cache-budget worth of real
// memory while the cache's own accounting reads "under budget" — which is
// exactly the staging OOM pattern this ledger exists to fix.
//
// The fix is two-fold:
//   1. Truthful accounting: every opened segment is wrapped in a
//      [`TrackedSegment`] whose `Drop` decrements `live` — so `live` is the
//      real footprint of all referenced segments, cache-held or not, and
//      only drops when memory is actually freed.
//   2. Admission control: [`Searcher::search`] estimates the bytes it will
//      newly load and reserves them via [`MemoryLedger::admit`] before
//      scoring. Over the watermark, it first evicts idle cache entries,
//      then blocks until in-flight searches release memory, then sheds the
//      request with [`KoshaError::Overloaded`] (HTTP 429 at the server —
//      the Python client's existing retry/backoff handles it). Blocking
//      happens only on the request thread at search entry, never inside
//      rayon workers, so the scoring pool can't deadlock on itself.
//
// Lock-order invariant: cache locks (`entries`/`recency`) may be held while
// taking the ledger lock (dropping an evicted `TrackedSegment` does this),
// but never the reverse — `admit` releases the ledger lock before asking the
// cache to evict.

struct LedgerState {
    /// Bytes of all currently-referenced (constructed, not-yet-dropped)
    /// [`TrackedSegment`]s — cache-resident or pinned by in-flight requests.
    live: u64,
    /// Bytes reserved by admitted-but-still-loading searches. Moves to
    /// `live` as segments actually open (see [`AdmissionPermit::consume`]);
    /// any remainder is returned when the permit drops.
    reserved: u64,
    /// Number of admitted searches whose permits are still alive. The
    /// anti-starvation rule keys off this: a search is always admitted when
    /// no other permit is outstanding, however large its estimate — the
    /// watermark bounds concurrent *pile-up*, not single-query size, so one
    /// oversized query degrades exactly as it did before this ledger existed
    /// (LRU thrash) instead of deadlocking or being permanently rejected.
    active: usize,
}

pub struct MemoryLedger {
    state: Mutex<LedgerState>,
    cv: Condvar,
    max_live_bytes: u64,
    admission_timeout: Duration,
}

impl MemoryLedger {
    fn new(max_live_bytes: u64, admission_timeout: Duration) -> Self {
        Self {
            state: Mutex::new(LedgerState {
                live: 0,
                reserved: 0,
                active: 0,
            }),
            cv: Condvar::new(),
            max_live_bytes,
            admission_timeout,
        }
    }

    fn add_live(&self, bytes: u64) {
        let mut st = self.state.lock().unwrap();
        st.live = st.live.saturating_add(bytes);
    }

    fn release_live(&self, bytes: u64) {
        let mut st = self.state.lock().unwrap();
        st.live = st.live.saturating_sub(bytes);
        drop(st);
        self.cv.notify_all();
    }

    /// Reserve `estimate` bytes for a search, blocking (bounded) if that
    /// would push `live + reserved` past the watermark. `evict_idle(needed)`
    /// is invoked (with the ledger lock *released* — see the lock-order
    /// invariant above) to ask the cache to free idle entries first.
    fn admit(
        self: &Arc<Self>,
        estimate: u64,
        evict_idle: impl Fn(u64),
    ) -> Result<AdmissionPermit, KoshaError> {
        let deadline = Instant::now() + self.admission_timeout;
        let mut tried_evict = false;
        let mut st = self.state.lock().unwrap();
        loop {
            let committed = st.live.saturating_add(st.reserved);
            let fits = committed.saturating_add(estimate) <= self.max_live_bytes;
            // Anti-starvation: alone (no other admitted search) after one
            // eviction attempt → admit regardless of size. See
            // `LedgerState::active`.
            if fits || (st.active == 0 && tried_evict) {
                st.reserved = st.reserved.saturating_add(estimate);
                st.active += 1;
                return Ok(AdmissionPermit {
                    ledger: Arc::clone(self),
                    remaining: Mutex::new(estimate),
                });
            }
            if !tried_evict {
                let needed = committed
                    .saturating_add(estimate)
                    .saturating_sub(self.max_live_bytes);
                drop(st);
                evict_idle(needed);
                tried_evict = true;
                st = self.state.lock().unwrap();
                continue;
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(KoshaError::Overloaded(format!(
                    "search needs ~{estimate} more live segment bytes but \
                     {committed} of the {} watermark are already pinned by \
                     {} in-flight search(es); nothing freed within {:?} — \
                     retry with backoff",
                    self.max_live_bytes, st.active, self.admission_timeout,
                )));
            }
            let (guard, _timeout) = self.cv.wait_timeout(st, deadline - now).unwrap();
            st = guard;
        }
    }
}

/// A parsed segment plus its ledger accounting: constructing one records its
/// bytes as live; dropping the last `Arc` to it releases them. This is what
/// makes [`MemoryLedger::state`]'s `live` truthful — the bytes stay counted
/// exactly as long as *anything* (cache or in-flight request) can still
/// reach the parsed data, and are released exactly when the allocation is.
pub struct TrackedSegment {
    reader: SegmentReader,
    bytes: u64,
    ledger: Arc<MemoryLedger>,
}

impl TrackedSegment {
    fn new(reader: SegmentReader, bytes: u64, ledger: Arc<MemoryLedger>) -> Self {
        ledger.add_live(bytes);
        Self {
            reader,
            bytes,
            ledger,
        }
    }
}

impl std::ops::Deref for TrackedSegment {
    type Target = SegmentReader;
    fn deref(&self) -> &SegmentReader {
        &self.reader
    }
}

impl Drop for TrackedSegment {
    fn drop(&mut self) {
        self.ledger.release_live(self.bytes);
    }
}

/// RAII handle for an admitted search's byte reservation. As segments
/// actually open, their bytes move from "reserved" to "live" via
/// [`Self::consume`]; whatever reservation is left (segments that turned
/// out to be bloom-pruned, cache hits raced in by a concurrent search, …)
/// is returned when the permit drops at the end of the search.
pub struct AdmissionPermit {
    ledger: Arc<MemoryLedger>,
    remaining: Mutex<u64>,
}

impl AdmissionPermit {
    fn consume(&self, bytes: u64) {
        let mut remaining = self.remaining.lock().unwrap();
        let take = (*remaining).min(bytes);
        *remaining -= take;
        drop(remaining);
        if take > 0 {
            let mut st = self.ledger.state.lock().unwrap();
            st.reserved = st.reserved.saturating_sub(take);
            // No notify: the matching `live` increase (TrackedSegment::new)
            // keeps committed bytes net-unchanged, so no waiter can newly fit.
        }
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        let remaining = *self.remaining.lock().unwrap();
        let mut st = self.ledger.state.lock().unwrap();
        st.reserved = st.reserved.saturating_sub(remaining);
        st.active = st.active.saturating_sub(1);
        drop(st);
        self.ledger.cv.notify_all();
    }
}

/// In-memory LRU cache of parsed segments, keyed by
/// `(namespace, segment_id, load_vectors)`.
///
/// Without this, every query re-reads and re-parses `doc_store.bin` +
/// `inverted.idx` (+ `vector.idx`/HNSW when applicable) from disk for every
/// segment in the manifest, even when the local NVMe cache is fully warm —
/// so latency scales with total corpus size instead of query cost. Because
/// segments are immutable, a cache hit here never needs invalidation; only
/// eviction to bound memory.
///
/// Bounded by *both* entry count and an approximate byte budget. Count alone
/// is not enough: an unfiltered lexical query has to open every segment in
/// the manifest (nothing to bloom-prune), so a single query against a large
/// namespace can insert dozens of segments in one shot, well under a
/// generous count cap, while still exhausting the container's memory if
/// those segments are individually large — that's a container-level OOM
/// (SIGKILL), not the graceful "re-parse on next miss" this cache is meant
/// to guarantee. The byte size used per entry is approximate (the on-disk
/// footprint of the files that were actually loaded), not an exact
/// `size_of` of the parsed structures, but it's the right order of
/// magnitude and, critically, bounds worst case instead of trusting entry
/// count to correlate with memory.
/// Cache key: `(namespace, segment_id, load_vectors)`.
type SegmentCacheKey = (String, String, bool);

struct SegmentCache {
    capacity: usize,
    max_bytes: u64,
    entries: Mutex<HashMap<SegmentCacheKey, (Arc<TrackedSegment>, u64)>>,
    recency: Mutex<VecDeque<SegmentCacheKey>>,
    total_bytes: AtomicU64,
}

impl SegmentCache {
    fn new(capacity: usize, max_bytes: u64) -> Self {
        Self {
            capacity,
            max_bytes,
            entries: Mutex::new(HashMap::new()),
            recency: Mutex::new(VecDeque::new()),
            total_bytes: AtomicU64::new(0),
        }
    }

    fn touch(&self, key: &SegmentCacheKey) {
        let mut recency = self.recency.lock().unwrap();
        if let Some(pos) = recency.iter().position(|k| k == key) {
            recency.remove(pos);
        }
        recency.push_back(key.clone());
    }

    fn get(&self, key: &SegmentCacheKey) -> Option<Arc<TrackedSegment>> {
        let hit = self
            .entries
            .lock()
            .unwrap()
            .get(key)
            .map(|(r, _)| r.clone());
        if hit.is_some() {
            self.touch(key);
        }
        hit
    }

    fn contains(&self, key: &SegmentCacheKey) -> bool {
        self.entries.lock().unwrap().contains_key(key)
    }

    fn insert(&self, key: SegmentCacheKey, reader: Arc<TrackedSegment>, approx_bytes: u64) {
        {
            let mut entries = self.entries.lock().unwrap();
            entries.insert(key.clone(), (reader, approx_bytes));
        }
        self.total_bytes.fetch_add(approx_bytes, Ordering::Relaxed);
        self.touch(&key);
        self.enforce_budget();
    }

    /// Evict idle LRU entries until the cache is back under both its entry
    /// and byte budgets, or no idle entries remain. Called after every
    /// insert and at the start of every search — the latter catches
    /// segments that were pinned by in-flight requests at insert time
    /// (skipped below) and have since become idle.
    fn enforce_budget(&self) {
        self.evict_idle_until(|len, total, _freed| len <= self.capacity && total <= self.max_bytes);
    }

    /// Evict idle LRU entries until at least `needed` bytes have been
    /// freed, or no idle entries remain. Used by admission (see
    /// [`MemoryLedger::admit`]) to free idle memory before making a search
    /// wait on in-flight releases.
    fn evict_idle(&self, needed: u64) {
        self.evict_idle_until(|_len, _total, freed| freed >= needed);
    }

    /// Shared eviction walk: pop LRU-order entries, evicting the *idle*
    /// ones (strong_count == 1 — the cache's own `Arc` is the only
    /// reference) until `done(len, total_bytes, freed)` says stop.
    ///
    /// Entries pinned by in-flight requests are skipped with their recency
    /// order preserved: removing them from the cache would free no memory
    /// (the request's `Arc` keeps the parsed segment alive regardless)
    /// while forfeiting reuse — the pre-ledger version of this cache did
    /// exactly that, which is why its byte accounting drifted from real
    /// memory under concurrent load. If everything left is pinned, stop:
    /// bounding pinned memory is the admission gate's job, not eviction's.
    fn evict_idle_until(&self, done: impl Fn(usize, u64, u64) -> bool) -> u64 {
        let mut freed = 0u64;
        let mut recency = self.recency.lock().unwrap();
        let mut entries = self.entries.lock().unwrap();
        let mut pinned: Vec<SegmentCacheKey> = Vec::new();
        loop {
            let total = self.total_bytes.load(Ordering::Relaxed);
            if done(entries.len(), total, freed) {
                break;
            }
            let Some(oldest_key) = recency.pop_front() else {
                break;
            };
            match entries
                .get(&oldest_key)
                .map(|(arc, _)| Arc::strong_count(arc) == 1)
            {
                // Stale recency key (entry already gone) — just drop it.
                None => {}
                Some(true) => {
                    if let Some((_, size)) = entries.remove(&oldest_key) {
                        // The Arc drops here → TrackedSegment::drop → ledger
                        // lock. Safe per the lock-order invariant: cache
                        // locks may be held while taking the ledger lock,
                        // never the reverse.
                        self.total_bytes.fetch_sub(size, Ordering::Relaxed);
                        freed = freed.saturating_add(size);
                    }
                }
                Some(false) => pinned.push(oldest_key),
            }
        }
        // Restore skipped (pinned) entries to the front, preserving their
        // original LRU order — they're still the oldest, just untouchable
        // right now.
        for key in pinned.into_iter().rev() {
            recency.push_front(key);
        }
        freed
    }
}

/// Sum the on-disk size of a segment's component files, as a proxy for the
/// in-memory footprint of its parsed representation. Missing files (e.g.
/// `vector.idx` when `load_vectors` is false) simply contribute nothing.
fn approx_segment_bytes(seg_dir: &std::path::Path, load_vectors: bool) -> u64 {
    let mut files = vec![
        "doc_store.bin",
        "inverted.idx",
        "filters.bin",
        "footer.json",
    ];
    if load_vectors {
        files.push("vector.idx");
    }
    files
        .iter()
        .map(|f| {
            std::fs::metadata(seg_dir.join(f))
                .map(|m| m.len())
                .unwrap_or(0)
        })
        .sum()
}

// ─── BM25 scorer ────────────────────────────────────────────────────────────

pub struct Bm25Scorer {
    num_docs: u32,
    avg_field_length: f64,
    params: Bm25Params,
}

impl Bm25Scorer {
    pub fn new(num_docs: u32, avg_field_length: f64, params: Bm25Params) -> Self {
        Self {
            num_docs,
            avg_field_length,
            params,
        }
    }

    pub fn score_term(
        &self,
        term_frequency: u32,
        doc_frequency: u32,
        doc_field_length: u32,
    ) -> f64 {
        let n = self.num_docs as f64;
        let df = doc_frequency as f64;
        let tf = term_frequency as f64;
        let doc_len = doc_field_length as f64;
        let avgdl = self.avg_field_length;
        let k1 = self.params.k1;
        let b = self.params.b;
        if tf == 0.0 || df == 0.0 || n == 0.0 {
            return 0.0;
        }
        let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
        let tf_component = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * doc_len / avgdl));
        idf * tf_component
    }
}

// ─── Wildcard matcher ───────────────────────────────────────────────────────

pub fn wildcard_terms(terms: &[&str], pattern: &str, case_insensitive: bool) -> Vec<String> {
    terms
        .iter()
        .filter(|t| simple_wildcard_match(t, pattern, case_insensitive))
        .map(|t| t.to_string())
        .collect()
}

fn simple_wildcard_match(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    let text = if case_insensitive {
        text.to_lowercase()
    } else {
        text.to_string()
    };
    let pattern = if case_insensitive {
        pattern.to_lowercase()
    } else {
        pattern.to_string()
    };

    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let mut ti = 0;
    let mut pi = 0;
    let mut backtrack_t = 0usize;
    let mut backtrack_p = 0usize;

    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '*' {
            backtrack_t = ti;
            backtrack_p = pi;
            pi += 1;
        } else if backtrack_p < p.len() {
            backtrack_t += 1;
            ti = backtrack_t;
            pi = backtrack_p + 1;
        } else {
            return false;
        }
    }

    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi >= p.len()
}

// ─── Match phrase scorer ────────────────────────────────────────────────────

pub fn match_phrase_score(
    postings_list: &[Vec<u32>], // positions for each query term, per doc
    slop: u32,
) -> f64 {
    if postings_list.is_empty() {
        return 0.0;
    }
    if postings_list.len() == 1 {
        return 1.0;
    }

    let first_positions = &postings_list[0];
    for &start_pos in first_positions {
        let mut matched = true;
        for (i, positions) in postings_list[1..].iter().enumerate() {
            let expected = start_pos + (i as u32) + 1;
            let found = positions.iter().any(|&p| {
                let dist = p.abs_diff(expected);
                dist <= slop
            });
            if !found {
                matched = false;
                break;
            }
        }
        if matched {
            return 1.0;
        }
    }
    0.0
}

// ─── Filter applier ─────────────────────────────────────────────────────────

pub struct FilterApplier;

impl FilterApplier {
    pub fn apply(
        clause: &FilterClause,
        store: &FilterStore,
        candidates: &HashSet<u32>,
    ) -> Result<HashSet<u32>, KoshaError> {
        match clause {
            FilterClause::Term { term } => Self::apply_term(term, store, candidates),
            FilterClause::Terms { terms } => Self::apply_terms(terms, store, candidates),
            FilterClause::Range { range } => Self::apply_range(range, store, candidates),
            FilterClause::Bool { bool: b } => Self::apply_bool(b, store, candidates),
            FilterClause::MatchAll { .. } => Ok(candidates.clone()),
        }
    }

    fn apply_term(
        term: &HashMap<String, String>,
        store: &FilterStore,
        candidates: &HashSet<u32>,
    ) -> Result<HashSet<u32>, KoshaError> {
        let mut result = HashSet::new();
        for (field, value) in term {
            if let Some(entries) = store.string_fields.get(field) {
                for &(doc_seq, ref val) in entries {
                    if candidates.contains(&doc_seq) && val == value {
                        result.insert(doc_seq);
                    }
                }
            }
        }
        Ok(result)
    }

    fn apply_terms(
        terms: &HashMap<String, Vec<String>>,
        store: &FilterStore,
        candidates: &HashSet<u32>,
    ) -> Result<HashSet<u32>, KoshaError> {
        let mut result = HashSet::new();
        for (field, values) in terms {
            let value_set: HashSet<&str> = values.iter().map(|s| s.as_str()).collect();
            if let Some(entries) = store.string_fields.get(field) {
                for &(doc_seq, ref val) in entries {
                    if candidates.contains(&doc_seq) && value_set.contains(val.as_str()) {
                        result.insert(doc_seq);
                    }
                }
            }
        }
        Ok(result)
    }

    fn apply_range(
        range: &HashMap<String, kosha_core::RangeBound>,
        store: &FilterStore,
        candidates: &HashSet<u32>,
    ) -> Result<HashSet<u32>, KoshaError> {
        let mut result = HashSet::new();
        for (field, bound) in range {
            if let Some(entries) = store.integer_fields.get(field) {
                for &(doc_seq, val) in entries {
                    if candidates.contains(&doc_seq) && check_i64(val, bound) {
                        result.insert(doc_seq);
                    }
                }
            } else if let Some(entries) = store.float_fields.get(field) {
                for &(doc_seq, val) in entries {
                    if candidates.contains(&doc_seq) && check_f64(val, bound) {
                        result.insert(doc_seq);
                    }
                }
            } else if let Some(entries) = store.string_fields.get(field) {
                for &(doc_seq, ref val) in entries {
                    if candidates.contains(&doc_seq) && check_str(val, bound) {
                        result.insert(doc_seq);
                    }
                }
            }
        }
        Ok(result)
    }

    fn apply_bool(
        b: &kosha_core::BoolFilter,
        store: &FilterStore,
        candidates: &HashSet<u32>,
    ) -> Result<HashSet<u32>, KoshaError> {
        let mut working: Option<HashSet<u32>> = None;

        if !b.must.is_empty() {
            let mut acc = candidates.clone();
            for clause in &b.must {
                acc = Self::apply(clause, store, &acc)?;
            }
            working = Some(acc);
        }
        if !b.must_not.is_empty() {
            let base = working.take().unwrap_or_else(|| candidates.clone());
            let mut excluded = HashSet::new();
            for clause in &b.must_not {
                excluded.extend(Self::apply(clause, store, &base)?);
            }
            working = Some(base.difference(&excluded).copied().collect());
        }
        if !b.should.is_empty() {
            let base = working.take().unwrap_or_else(|| candidates.clone());
            let mut scores: HashMap<u32, usize> = HashMap::new();
            for clause in &b.should {
                for doc_seq in Self::apply(clause, store, &base)? {
                    *scores.entry(doc_seq).or_default() += 1;
                }
            }
            let passed: HashSet<u32> = scores
                .into_iter()
                .filter(|(_, c)| *c >= b.minimum_should_match)
                .map(|(d, _)| d)
                .collect();
            working = Some(if base.is_empty() {
                passed
            } else {
                base.intersection(&passed).copied().collect()
            });
        }
        Ok(working.unwrap_or_else(|| candidates.clone()))
    }
}

fn check_i64(val: i64, bound: &kosha_core::RangeBound) -> bool {
    if let Some(ref gte) = bound.gte {
        if let Ok(b) = gte.parse::<i64>() {
            if val < b {
                return false;
            }
        }
    }
    if let Some(ref gt) = bound.gt {
        if let Ok(b) = gt.parse::<i64>() {
            if val <= b {
                return false;
            }
        }
    }
    if let Some(ref lte) = bound.lte {
        if let Ok(b) = lte.parse::<i64>() {
            if val > b {
                return false;
            }
        }
    }
    if let Some(ref lt) = bound.lt {
        if let Ok(b) = lt.parse::<i64>() {
            if val >= b {
                return false;
            }
        }
    }
    true
}
fn check_f64(val: f64, bound: &kosha_core::RangeBound) -> bool {
    if let Some(ref gte) = bound.gte {
        if let Ok(b) = gte.parse::<f64>() {
            if val < b {
                return false;
            }
        }
    }
    if let Some(ref gt) = bound.gt {
        if let Ok(b) = gt.parse::<f64>() {
            if val <= b {
                return false;
            }
        }
    }
    if let Some(ref lte) = bound.lte {
        if let Ok(b) = lte.parse::<f64>() {
            if val > b {
                return false;
            }
        }
    }
    if let Some(ref lt) = bound.lt {
        if let Ok(b) = lt.parse::<f64>() {
            if val >= b {
                return false;
            }
        }
    }
    true
}
fn check_str(val: &str, bound: &kosha_core::RangeBound) -> bool {
    if let Some(ref gte) = bound.gte {
        if val < gte.as_str() {
            return false;
        }
    }
    if let Some(ref gt) = bound.gt {
        if val <= gt.as_str() {
            return false;
        }
    }
    if let Some(ref lte) = bound.lte {
        if val > lte.as_str() {
            return false;
        }
    }
    if let Some(ref lt) = bound.lt {
        if val >= lt.as_str() {
            return false;
        }
    }
    true
}

// ─── Highlight applier ──────────────────────────────────────────────────────

pub fn apply_highlight(
    text: &str,
    query_terms: &[String],
    pre_tag: &str,
    post_tag: &str,
) -> String {
    let mut result = text.to_string();
    for term in query_terms {
        let lower = term.to_lowercase();
        if let Some(start) = result.to_lowercase().find(&lower) {
            let (before, rest) = result.split_at(start);
            let (matched, after) = rest.split_at(lower.len());
            result = format!("{}{}{}{}{}", before, pre_tag, matched, post_tag, after);
        }
    }
    result
}

// ─── Searcher ───────────────────────────────────────────────────────────────

// ─── Cosine similarity ─────────────────────────────────────────────────────

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    (dot / (norm_a * norm_b)) as f64
}

/// Flat kNN search: compute cosine similarity against all stored vectors,
/// return top-K (doc_seq, score) pairs.
pub fn flat_knn(query_vector: &[f32], vectors: &[(u32, Vec<f32>)], k: usize) -> Vec<(u32, f64)> {
    let mut scores: Vec<(u32, f64)> = vectors
        .iter()
        .map(|(doc_seq, vec)| (*doc_seq, cosine_similarity(query_vector, vec)))
        .collect();
    scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scores.truncate(k);
    scores
}

/// Per-request phase timings for one `search` call — the query-side half of
/// the cold-read instrumentation (the server adds hydration wall time and
/// bytes on top). Wall-clock phases are disjoint; `open_total_ms` is summed
/// across rayon workers, so it can legitimately exceed `score_wall_ms` under
/// parallelism — read it together with `open_cold` as "average cold-open
/// cost", not as a wall-clock share.
#[derive(Debug, Default, Clone, Copy)]
pub struct SearchPhaseStats {
    /// Admission-gate wait (`MemoryLedger::admit`) plus the byte-estimate
    /// pass. Near-zero unless the pod is shedding load.
    pub admit_wall_ms: f64,
    /// The parallel per-segment scoring pass, cold segment opens included.
    pub score_wall_ms: f64,
    /// Sort / top-k selection / page materialization (the only phase that
    /// touches `doc_store.bin`).
    pub materialize_wall_ms: f64,
    /// Segments opened cold (in-memory segment-cache miss) vs served from
    /// the cache.
    pub open_cold: usize,
    pub open_cached: usize,
    /// Summed wall time of the cold opens above, across rayon workers.
    pub open_total_ms: f64,
}

/// Shared-atomic collector threaded through the parallel scoring pass to
/// build [`SearchPhaseStats`] (open counts/time come from inside
/// `open_segment`, which runs concurrently per segment).
#[derive(Default)]
struct OpenStatsCollector {
    cold: AtomicUsize,
    cached: AtomicUsize,
    open_nanos: AtomicU64,
}

pub struct Searcher {
    data_dir: PathBuf,
    segment_cache: SegmentCache,
    ledger: Arc<MemoryLedger>,
}

impl Searcher {
    pub fn new(data_dir: PathBuf) -> Self {
        Self::with_segment_cache_limits(
            data_dir,
            DEFAULT_SEGMENT_CACHE_CAPACITY,
            DEFAULT_SEGMENT_CACHE_MAX_BYTES,
        )
    }

    /// Like [`Searcher::new`], but with an explicit cap on how many parsed
    /// segments are kept resident in memory at once (see [`SegmentCache`]).
    /// Uses the default byte budget.
    pub fn with_segment_cache_capacity(data_dir: PathBuf, capacity: usize) -> Self {
        Self::with_segment_cache_limits(data_dir, capacity, DEFAULT_SEGMENT_CACHE_MAX_BYTES)
    }

    /// Like [`Searcher::new`], with explicit control over both the entry
    /// count and the approximate byte budget for the in-memory segment
    /// cache (see [`SegmentCache`] — the byte budget is the one that
    /// actually bounds worst-case memory; count alone does not). The live
    /// watermark defaults to [`DEFAULT_LIVE_BYTES_FACTOR`] × `max_bytes`.
    pub fn with_segment_cache_limits(data_dir: PathBuf, capacity: usize, max_bytes: u64) -> Self {
        Self::with_memory_limits(
            data_dir,
            capacity,
            max_bytes,
            max_bytes.saturating_mul(DEFAULT_LIVE_BYTES_FACTOR),
            DEFAULT_ADMISSION_TIMEOUT,
        )
    }

    /// Full control: cache entry cap, cache byte budget, live-bytes
    /// watermark, and admission timeout (see [`MemoryLedger`] for what the
    /// last two govern and why they exist).
    pub fn with_memory_limits(
        data_dir: PathBuf,
        capacity: usize,
        max_bytes: u64,
        max_live_bytes: u64,
        admission_timeout: Duration,
    ) -> Self {
        Self {
            data_dir,
            segment_cache: SegmentCache::new(capacity, max_bytes),
            ledger: Arc::new(MemoryLedger::new(max_live_bytes, admission_timeout)),
        }
    }

    /// Return the cached parsed segment for `(namespace, segment_id,
    /// load_vectors)`, opening and caching it on miss. On a miss, the
    /// opened segment's bytes are recorded as live in the ledger (via
    /// [`TrackedSegment::new`]) and consumed from the search's admission
    /// reservation, keeping committed bytes net-unchanged.
    fn open_segment(
        &self,
        namespace: &str,
        segment_id: &str,
        seg_dir: PathBuf,
        load_vectors: bool,
        permit: Option<&AdmissionPermit>,
        stats: Option<&OpenStatsCollector>,
    ) -> Result<Arc<TrackedSegment>, KoshaError> {
        let key = (namespace.to_string(), segment_id.to_string(), load_vectors);
        if let Some(cached) = self.segment_cache.get(&key) {
            if let Some(stats) = stats {
                stats.cached.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(cached);
        }
        let t_open = Instant::now();
        let approx_bytes = approx_segment_bytes(&seg_dir, load_vectors);
        let reader = SegmentReader::open_with_options(seg_dir, load_vectors)?;
        let tracked = Arc::new(TrackedSegment::new(
            reader,
            approx_bytes,
            Arc::clone(&self.ledger),
        ));
        if let Some(stats) = stats {
            stats.cold.fetch_add(1, Ordering::Relaxed);
            stats
                .open_nanos
                .fetch_add(t_open.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        if let Some(permit) = permit {
            permit.consume(approx_bytes);
        }
        self.segment_cache
            .insert(key, Arc::clone(&tracked), approx_bytes);
        Ok(tracked)
    }

    /// Score one manifest segment against `query`: open it (bloom-pruning
    /// first, to skip the open entirely when possible), run BM25/wildcard/
    /// phrase/kNN scoring, apply the filter, and compute this segment's own
    /// aggregation contribution. Returns `Ok(None)` when the segment is
    /// absent locally or pruned by a bloom check — same skip conditions the
    /// old sequential loop's `continue` handled.
    ///
    /// Segments are otherwise fully independent of each other until the
    /// caller reduces their outputs (candidates flattened, aggregations
    /// merged) — see [`Searcher::search`], which runs this via
    /// `par_iter().map(...)` across the manifest instead of one at a time.
    #[allow(clippy::too_many_arguments)]
    fn score_segment(
        &self,
        namespace: &NamespaceId,
        entry: &ManifestEntry,
        query: &SearchQuery,
        query_terms: &[String],
        term_prune: Option<&(Vec<String>, TermBloomMode)>,
        sort_value_fields: &[String],
        tombstones: Option<
            &std::collections::HashMap<kosha_core::SegmentId, std::collections::HashSet<u32>>,
        >,
        permit: &AdmissionPermit,
        open_stats: &OpenStatsCollector,
    ) -> Result<Option<SegmentOutput>, KoshaError> {
        let seg_dir = self.data_dir.join(&namespace.0).join(&entry.segment_id.0);
        if !seg_dir.exists() {
            return Ok(None);
        }

        // Prune via footer blooms before opening inverted/filters/vectors.
        if query.filter.is_some() || term_prune.is_some() {
            if let Ok(footer) = SegmentReader::read_footer(&seg_dir) {
                if let Some(ref filter) = query.filter {
                    if !segment_may_match(filter, footer.filter_blooms.as_ref()) {
                        return Ok(None);
                    }
                }
                if let Some((terms, mode)) = term_prune {
                    if !segment_may_contain_terms(terms, *mode, footer.term_bloom.as_ref()) {
                        return Ok(None);
                    }
                }
            }
        }

        let is_tombstoned = |doc_seq: u32| -> bool {
            tombstones.is_some_and(|t| {
                t.get(&entry.segment_id)
                    .is_some_and(|seqs| seqs.contains(&doc_seq))
            })
        };

        // Lexical/BM25 queries (no query.knn) never touch vectors or the
        // HNSW graph — skip loading both so keyword search latency stops
        // scaling with embedding volume/segment count.
        let reader = self.open_segment(
            &namespace.0,
            &entry.segment_id.0,
            seg_dir,
            query.knn.is_some(),
            Some(permit),
            Some(open_stats),
        )?;
        let total_docs = reader.doc_count();
        let store = &reader.filter_store;
        let scorer = Bm25Scorer::new(
            total_docs,
            reader.avg_field_length(),
            reader.bm25_params().clone(),
        );

        let has_query =
            !query_terms.is_empty() || query.wildcard.is_some() || query.match_phrase.is_some();
        let has_only_filter = !has_query && query.filter.is_some();

        // Per-segment hits keyed by doc_seq for O(1) kNN hybrid merge.
        let mut seg_hits: HashMap<u32, (f64, DocumentId)> = HashMap::new();

        // ── Wildcard matching ──
        let is_wildcard_mode = query.wildcard.is_some();
        let effective_terms = if let Some(ref wc) = query.wildcard {
            let all_terms: Vec<&str> = reader.all_terms();
            wildcard_terms(&all_terms, &wc.pattern, wc.case_insensitive)
        } else if !query_terms.is_empty() {
            query_terms.to_vec()
        } else {
            Vec::new()
        };

        // ── Match phrase ──
        let phrase_match = if let Some(ref mp) = query.match_phrase {
            let phrase_terms = tokenize(&mp.phrase);
            Some((phrase_terms, mp.slop))
        } else {
            None
        };

        let phrase_tokenized = phrase_match.as_ref().map(|p| &p.0);

        // ── BM25 scoring with positions for phrase ──
        if !effective_terms.is_empty() || phrase_tokenized.is_some() {
            let terms_for_bm25 = if let Some(ref pt) = phrase_tokenized {
                pt
            } else {
                &effective_terms
            };

            // `PostingsRef`: legacy segments lend a borrow into their parsed
            // map; v2 segments hand out a shared Arc of the on-demand decode
            // — see `SegmentReader::postings`. Fetched once per term here
            // and held for the whole scoring pass either way.
            let term_postings: Vec<(&str, kosha_segment::PostingsRef<'_>)> = terms_for_bm25
                .iter()
                .filter_map(|t| reader.postings(t).map(|p| (t.as_str(), p)))
                .collect();

            let mut doc_frequencies: HashMap<&str, u32> = HashMap::new();
            for (t, p) in &term_postings {
                doc_frequencies.insert(t, p.len() as u32);
            }

            // ── Postings AND/OR: AND for multi-term queries, OR for wildcard ──
            let mut scored: HashMap<u32, f64> = HashMap::new();
            let use_and =
                term_postings.len() > 1 && !is_wildcard_mode && phrase_tokenized.is_none();

            if !use_and {
                // OR mode (wildcard, phrase, or single term): score any matching doc.
                for (term, postings) in &term_postings {
                    let df = doc_frequencies.get(term).copied().unwrap_or(0);
                    for posting in postings.iter() {
                        if let Some(meta) = reader.doc_meta(posting.doc_id) {
                            let score =
                                scorer.score_term(posting.term_frequency, df, meta.field_length);
                            *scored.entry(posting.doc_id).or_insert(0.0) += score;
                        }
                    }
                }
            } else {
                // Multi-term AND: build per-term doc→posting maps once,
                // intersect starting from the shortest list, then score
                // via O(1) lookup. The previous path rebuilt a map for
                // intersection but re-scanned each postings list with
                // `.find()` while scoring (~O(hits · postings)) — that
                // dominated warm multi-term latency (see scoring_profile).
                let term_maps: Vec<(&str, HashMap<u32, &kosha_core::Posting>, u32)> = term_postings
                    .iter()
                    .map(|(term, postings)| {
                        let df = doc_frequencies.get(term).copied().unwrap_or(0);
                        let map = postings.iter().map(|p| (p.doc_id, p)).collect();
                        (*term, map, df)
                    })
                    .collect();

                let mut and_candidates: Vec<u32> = {
                    let shortest = term_maps.iter().min_by_key(|(_, m, _)| m.len()).unwrap();
                    shortest.1.keys().copied().collect()
                };
                for (_, map, _) in &term_maps {
                    and_candidates.retain(|doc_id| map.contains_key(doc_id));
                    if and_candidates.is_empty() {
                        break;
                    }
                }

                for doc_id in and_candidates {
                    if let Some(meta) = reader.doc_meta(doc_id) {
                        let mut total_score = 0.0;
                        for (_, map, df) in &term_maps {
                            if let Some(p) = map.get(&doc_id) {
                                total_score +=
                                    scorer.score_term(p.term_frequency, *df, meta.field_length);
                            }
                        }
                        scored.insert(doc_id, total_score);
                    }
                }
            }

            // Apply phrase matching (filter out docs that don't match the phrase).
            if let Some((ref phrase_terms, slop)) = phrase_match {
                // Fetch each phrase term's postings once (not once per
                // candidate doc) and index by doc_id for O(1) lookup.
                // `postings()` is an on-demand decode for v2 segments, so
                // the once-per-term discipline matters for real: the decoded
                // handles are bound first (they must outlive the maps, which
                // borrow position slices out of them).
                let phrase_postings_data: Vec<Option<kosha_segment::PostingsRef<'_>>> =
                    phrase_terms.iter().map(|pt| reader.postings(pt)).collect();
                let phrase_postings: Vec<HashMap<u32, &[u32]>> = phrase_postings_data
                    .iter()
                    .map(|data| {
                        data.as_ref()
                            .map(|postings| {
                                postings
                                    .iter()
                                    .map(|p| (p.doc_id, p.positions.as_slice()))
                                    .collect()
                            })
                            .unwrap_or_default()
                    })
                    .collect();

                let doc_ids: Vec<u32> = scored.keys().copied().collect();
                for doc_id in doc_ids {
                    let mut term_positions: Vec<Vec<u32>> = Vec::new();
                    for term_map in &phrase_postings {
                        if let Some(positions) = term_map.get(&doc_id) {
                            term_positions.push(positions.to_vec());
                        }
                    }
                    if term_positions.len() < phrase_terms.len() {
                        // Not all phrase terms appear in this doc.
                        scored.remove(&doc_id);
                        continue;
                    }
                    let phrase_score = match_phrase_score(&term_positions, slop);
                    if phrase_score == 0.0 {
                        scored.remove(&doc_id);
                    } else {
                        // Boost the score for matching the phrase.
                        *scored.get_mut(&doc_id).unwrap() *= 1.0 + phrase_score * 0.5;
                    }
                }
            }

            // Apply filter to this segment's scored docs *before* merging
            // into candidates. Filtering after the segment loop previously
            // dropped hits from earlier segments.
            let passed_filter: Option<HashSet<u32>> = if let Some(ref clause) = query.filter {
                let filter_candidates: HashSet<u32> = scored.keys().copied().collect();
                Some(FilterApplier::apply(clause, store, &filter_candidates)?)
            } else {
                None
            };

            for (doc_seq, score) in scored {
                if is_tombstoned(doc_seq) {
                    continue;
                }
                if let Some(ref passed) = passed_filter {
                    if !passed.contains(&doc_seq) {
                        continue;
                    }
                }
                if let Some(meta) = reader.doc_meta(doc_seq) {
                    seg_hits.insert(doc_seq, (score, meta.doc_id.clone()));
                }
            }
        } else if has_only_filter {
            let all_candidates: HashSet<u32> = (0..total_docs).collect();
            let passed =
                FilterApplier::apply(query.filter.as_ref().unwrap(), store, &all_candidates)?;
            for doc_seq in passed {
                if is_tombstoned(doc_seq) {
                    continue;
                }
                if let Some(meta) = reader.doc_meta(doc_seq) {
                    let score = scorer.score_term(1, total_docs, meta.field_length);
                    seg_hits.insert(doc_seq, (score, meta.doc_id.clone()));
                }
            }
        }

        // ── kNN search (HNSW when available, flat fallback) ──
        if let Some(ref knn) = query.knn {
            if !reader.vector_store.vectors.is_empty() {
                let knn_results: Vec<(u32, f64)> = if let Some(ref hnsw) = reader.hnsw_map {
                    let query_point = kosha_segment::CosinePoint(knn.vector.clone());
                    let mut search = instant_distance::Search::default();
                    hnsw.search(&query_point, &mut search)
                        .take(knn.k)
                        .map(|item| (*item.value, (1.0 - item.distance as f64).max(0.0)))
                        .collect()
                } else {
                    flat_knn(&knn.vector, &reader.vector_store.vectors, knn.k)
                };
                // Merge with this segment's BM25 hits or use kNN directly.
                // Per-segment HashMap keeps hybrid merge O(k), not O(hits·k).
                if !seg_hits.is_empty() {
                    for (doc_seq, knn_score) in knn_results {
                        if let Some(existing) = seg_hits.get_mut(&doc_seq) {
                            existing.0 = existing.0 * 0.5 + knn_score * 0.5 * 100.0;
                        }
                    }
                } else {
                    // Pure kNN search for this segment.
                    for (doc_seq, score) in knn_results {
                        if is_tombstoned(doc_seq) {
                            continue;
                        }
                        if let Some(meta) = reader.doc_meta(doc_seq) {
                            seg_hits.insert(doc_seq, ((score + 1.0) * 10.0, meta.doc_id.clone()));
                        }
                    }
                }
            }
        }

        let sort_value_maps = if sort_value_fields.is_empty() {
            HashMap::new()
        } else {
            build_sort_value_maps(store, sort_value_fields)
        };
        let mut candidates = Vec::with_capacity(seg_hits.len());
        for (doc_seq, (score, doc_id)) in seg_hits {
            let sort_values = if sort_value_fields.is_empty() {
                Vec::new()
            } else {
                extract_sort_values(doc_seq, sort_value_fields, &sort_value_maps)
            };
            candidates.push(HitCandidate {
                reader: Arc::clone(&reader),
                doc_seq,
                doc_id,
                score,
                sort_values,
            });
        }

        // ── Aggregations (this segment's own contribution only) ──
        let mut aggs = HashMap::new();
        for (agg_name, agg) in &query.aggs {
            match agg {
                Aggregation::Terms { terms } => {
                    let result = compute_single_aggregation(store, &terms.field);
                    aggs.insert(agg_name.clone(), result);
                }
                Aggregation::Cardinality { cardinality } => {
                    let result = compute_cardinality(store, &cardinality.field);
                    aggs.insert(agg_name.clone(), result);
                }
                Aggregation::Composite { composite } => {
                    let result = compute_composite(store, composite);
                    aggs.insert(agg_name.clone(), result);
                }
            }
        }

        Ok(Some(SegmentOutput { candidates, aggs }))
    }

    pub fn search(
        &self,
        namespace: &NamespaceId,
        manifest: &Manifest,
        query: &SearchQuery,
        tombstones: Option<
            &std::collections::HashMap<kosha_core::SegmentId, std::collections::HashSet<u32>>,
        >,
    ) -> Result<SearchResult, KoshaError> {
        self.search_inner(namespace, manifest, query, tombstones, None)
            .map(|(result, _stats)| result)
    }

    /// Like [`Searcher::search`], additionally returning per-phase timings
    /// (see [`SearchPhaseStats`]) so the server can log a per-request
    /// cold-read breakdown without a profiler attached.
    pub fn search_with_stats(
        &self,
        namespace: &NamespaceId,
        manifest: &Manifest,
        query: &SearchQuery,
        tombstones: Option<
            &std::collections::HashMap<kosha_core::SegmentId, std::collections::HashSet<u32>>,
        >,
    ) -> Result<(SearchResult, SearchPhaseStats), KoshaError> {
        self.search_inner(namespace, manifest, query, tombstones, None)
    }

    /// Like [`Searcher::search_with_stats`], but with an optional callback
    /// the searcher invokes to ensure `doc_store.bin` is local for the
    /// segments that hold the materialize page hits — after scoring/ranking
    /// and before field materialization.
    ///
    /// This is the on-demand half of scoring-set-only hydration: a cold
    /// search hydrates the small scoring set (footer + inverted + filters +
    /// `doc_store.offsets`) up front and skips the bulk `doc_store.bin`
    /// entirely; only the segments contributing to the returned page then
    /// get their `doc_store.bin` fetched, via this callback. Pass `None` to
    /// materialize straight from disk (the warm / local-dev path where
    /// `doc_store.bin` is already present).
    pub fn search_with_doc_store_hydrator(
        &self,
        namespace: &NamespaceId,
        manifest: &Manifest,
        query: &SearchQuery,
        tombstones: Option<
            &std::collections::HashMap<kosha_core::SegmentId, std::collections::HashSet<u32>>,
        >,
        hydrator: DocStoreHydrator<'_>,
    ) -> Result<(SearchResult, SearchPhaseStats), KoshaError> {
        self.search_inner(namespace, manifest, query, tombstones, hydrator)
    }

    fn search_inner(
        &self,
        namespace: &NamespaceId,
        manifest: &Manifest,
        query: &SearchQuery,
        tombstones: Option<
            &std::collections::HashMap<kosha_core::SegmentId, std::collections::HashSet<u32>>,
        >,
        doc_store_hydrator: DocStoreHydrator<'_>,
    ) -> Result<(SearchResult, SearchPhaseStats), KoshaError> {
        let mut phase_stats = SearchPhaseStats::default();
        if manifest.segments.is_empty() {
            return Ok((
                SearchResult {
                    results: Vec::new(),
                    total_hits: 0,
                    aggregations: None,
                },
                phase_stats,
            ));
        }
        let t_admit = Instant::now();

        // Lazy budget enforcement: segments that were pinned by in-flight
        // requests at insert time (and so skipped by eviction — see
        // `SegmentCache::evict_idle_until`) get another look now that those
        // requests may have finished. Cheap when already under budget.
        self.segment_cache.enforce_budget();

        // ── Admission (see `MemoryLedger`) ──
        // Estimate the incremental live bytes this search can add: the
        // on-disk footprint of every manifest segment that's present
        // locally but not already in the in-memory cache. Deliberately
        // conservative — bloom pruning may skip some of these without ever
        // opening them; the unconsumed reservation is returned when the
        // permit drops at the end of this search. The broad, unfiltered
        // queries this gate exists for prune nothing, so for exactly the
        // dangerous case the estimate is accurate.
        let load_vectors = query.knn.is_some();
        let estimate: u64 = manifest
            .segments
            .iter()
            .map(|entry| {
                let seg_dir = self.data_dir.join(&namespace.0).join(&entry.segment_id.0);
                let key = (
                    namespace.0.clone(),
                    entry.segment_id.0.clone(),
                    load_vectors,
                );
                if seg_dir.exists() && !self.segment_cache.contains(&key) {
                    approx_segment_bytes(&seg_dir, load_vectors)
                } else {
                    0
                }
            })
            .sum();
        let permit = self
            .ledger
            .admit(estimate, |needed| self.segment_cache.evict_idle(needed))?;
        phase_stats.admit_wall_ms = t_admit.elapsed().as_secs_f64() * 1e3;

        let query_terms = tokenize(&query.query_text);
        let phrase_terms_for_prune = query.match_phrase.as_ref().map(|mp| tokenize(&mp.phrase));
        // Term-bloom prune before open. Wildcard expansion needs the segment
        // vocabulary, so it cannot be pruned here (OR over unknown terms).
        let term_prune: Option<(Vec<String>, TermBloomMode)> = if query.wildcard.is_some() {
            None
        } else if let Some(ref pt) = phrase_terms_for_prune {
            Some((pt.clone(), TermBloomMode::And))
        } else if !query_terms.is_empty() {
            Some((query_terms.clone(), TermBloomMode::And))
        } else {
            None
        };

        // Score-only candidates (#37): defer `fields.clone()` until after
        // top-k / page selection so clone+sort cost scales with page size,
        // not total hit count. Each candidate carries its own `Arc<SegmentReader>`
        // clone so the page can materialize after the segment loop finishes.
        let sort_value_fields = sort_fields_needing_values(&query.sort);

        // Segments are independent until this reduce step, and opening
        // (I/O + parse) plus BM25/kNN scoring is what dominates wall-clock
        // for a broad query that touches most/all segments in the manifest
        // — so score them concurrently instead of one at a time.
        // `par_iter().map(...).collect()` preserves manifest order in the
        // output Vec no matter which thread finishes first, so the reduce
        // below is deterministic and behavior-identical to the old
        // sequential loop, just faster.
        let open_stats = OpenStatsCollector::default();
        let t_score = Instant::now();
        let segment_outputs: Vec<SegmentOutput> = manifest
            .segments
            .par_iter()
            .map(|entry| {
                self.score_segment(
                    namespace,
                    entry,
                    query,
                    &query_terms,
                    term_prune.as_ref(),
                    &sort_value_fields,
                    tombstones,
                    &permit,
                    &open_stats,
                )
            })
            .collect::<Result<Vec<Option<SegmentOutput>>, KoshaError>>()?
            .into_iter()
            .flatten()
            .collect();
        phase_stats.score_wall_ms = t_score.elapsed().as_secs_f64() * 1e3;
        phase_stats.open_cold = open_stats.cold.load(Ordering::Relaxed);
        phase_stats.open_cached = open_stats.cached.load(Ordering::Relaxed);
        phase_stats.open_total_ms = open_stats.open_nanos.load(Ordering::Relaxed) as f64 / 1e6;
        let t_materialize = Instant::now();

        let mut candidates: Vec<HitCandidate> = Vec::new();
        let mut all_aggs: HashMap<String, AggregationResults> = HashMap::new();
        for output in segment_outputs {
            candidates.extend(output.candidates);
            // Last-segment-wins per agg name, same reduction the old
            // sequential loop did via repeated `all_aggs.insert(...)`.
            // `segment_outputs` is in manifest order, so this is
            // behavior-identical to before — not a new source of
            // nondeterminism from parallelizing the scoring above. Note:
            // this was already a pre-existing simplification (each named
            // aggregation reflects only the last segment that computed it,
            // not a true cross-segment merge) — unrelated to this change,
            // called out separately rather than silently fixed here.
            for (agg_name, result) in output.aggs {
                all_aggs.insert(agg_name, result);
            }
        }

        // ── Sort (score-only / sort-key candidates — no full field payloads) ──
        let sort_cmp = |a: &HitCandidate, b: &HitCandidate| -> std::cmp::Ordering {
            if !query.sort.is_empty() {
                compare_candidate_sort_keys(a, b, &query.sort, &sort_value_fields)
            } else {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.doc_id.0.cmp(&b.doc_id.0))
            }
        };

        // ── search_after (cursor pagination) ──
        // search_after needs a fully sorted order to locate the cursor's
        // position — that position is data-dependent, so it can't be
        // bounded to a top-k without knowing where the cursor falls, unlike
        // the plain from/max_results path below. When search_after is set,
        // `from` is ignored (OpenSearch semantics).
        let has_search_after = query
            .search_after
            .as_ref()
            .is_some_and(|after| !after.is_empty());
        let mut page_start = query.from.min(candidates.len());
        if has_search_after {
            candidates.sort_by(sort_cmp);
            if let Some(ref after) = query.search_after {
                page_start = candidates
                    .iter()
                    .position(|r| {
                        candidate_is_strictly_after_cursor(
                            r,
                            after,
                            &query.sort,
                            &sort_value_fields,
                        )
                    })
                    .unwrap_or(candidates.len());
            }
        }

        let total_hits = candidates.len();
        let from = page_start.min(candidates.len());
        let to = (from + query.max_results).min(candidates.len());

        // Bounded top-k (#37): when we haven't already fully sorted for
        // search_after, partition so the top `to` candidates are correctly
        // ranked without sorting the remaining `total_hits - to` — this is
        // what makes cost scale with the page requested (`from +
        // max_results`) instead of total hit count. `to == 0` needs no
        // ordering at all (the page is empty either way).
        if !has_search_after && to > 0 {
            if to < candidates.len() {
                candidates.select_nth_unstable_by(to - 1, sort_cmp);
                candidates[..to].sort_by(sort_cmp);
            } else {
                candidates.sort_by(sort_cmp);
            }
        }

        // On-demand `doc_store.bin` for the materialize page (scoring-set-
        // only hydration): a cold search hydrated just the offsets sidecar
        // for Lazy segments, so `doc_store.bin` is absent until now. Fetch
        // it only for the segments that actually hold the returned page —
        // at most `from + max_results` distinct segments, not the whole
        // manifest — so the bytes read scale with page size, not hit count.
        // Idempotent: the server's `ensure_files_local` skips files already
        // present, so warm segments and legacy (Eager) segments already
        // carrying `doc_store.bin` are no-ops.
        if let Some(ensure_doc_store) = doc_store_hydrator {
            let mut page_seg_paths: Vec<PathBuf> = Vec::new();
            for cand in &candidates[from..to] {
                let p = cand.reader.segment_dir().to_path_buf();
                if !page_seg_paths.contains(&p) {
                    page_seg_paths.push(p);
                }
            }
            if !page_seg_paths.is_empty() {
                ensure_doc_store(&page_seg_paths);
            }
        }

        // Materialize fields / highlights only for the returned page — the
        // only place a `Lazy` segment's full field content is ever read
        // from disk (one seek+read per document, not the whole segment).
        let mut page = Vec::with_capacity(to - from);
        for cand in &candidates[from..to] {
            let Some(doc_rec) = cand.reader.doc_record_full(cand.doc_seq)? else {
                continue;
            };
            let mut doc = ScoredDocument {
                doc_id: cand.doc_id.clone(),
                score: cand.score,
                fields: doc_rec.fields,
                highlights: None,
            };
            if let Some(ref highlight) = query.highlight {
                if !query_terms.is_empty() {
                    let pre = highlight
                        .pre_tags
                        .first()
                        .map(|s| s.as_str())
                        .unwrap_or("<b>");
                    let post = highlight
                        .post_tags
                        .first()
                        .map(|s| s.as_str())
                        .unwrap_or("</b>");
                    let mut highlights = Vec::new();
                    for field in &doc.fields {
                        if field.name == highlight.field && field.field_type == FieldType::Text {
                            highlights.push(apply_highlight(&field.value, &query_terms, pre, post));
                        }
                    }
                    if !highlights.is_empty() {
                        doc.highlights = Some(highlights);
                    }
                }
            }
            page.push(doc);
        }

        // Merge aggregations across segments.
        let merged_aggs = if all_aggs.is_empty() {
            None
        } else {
            // Use the first segment's aggs (all segments have the same data).
            Some(all_aggs.into_values().next().unwrap_or(AggregationResults {
                per_document: None,
                total_documents: None,
                matched_docs: None,
                extra: HashMap::new(),
            }))
        };

        phase_stats.materialize_wall_ms = t_materialize.elapsed().as_secs_f64() * 1e3;

        Ok((
            SearchResult {
                results: page,
                total_hits,
                aggregations: merged_aggs,
            },
            phase_stats,
        ))
    }
}

/// Lightweight hit kept through ranking; full field payloads are cloned only
/// for the final page (see issue #37).
struct HitCandidate {
    /// The segment this hit came from. An `Arc` clone rather than an index
    /// into a shared `Vec` — segments are scored in parallel (see
    /// [`Searcher::score_segment`]), so there's no single ordered `Vec` of
    /// readers to index into by the time candidates are merged. This clone
    /// is also what pins the segment's memory as live in the
    /// [`MemoryLedger`] until the search finishes materializing its page.
    reader: Arc<TrackedSegment>,
    doc_seq: u32,
    doc_id: DocumentId,
    score: f64,
    /// Values for [`sort_fields_needing_values`], parallel to that list.
    sort_values: Vec<String>,
}

/// One segment's independent contribution to a query: its scored candidates
/// and its own aggregation results. Produced by [`Searcher::score_segment`]
/// and merged by [`Searcher::search`] after all segments have been scored.
struct SegmentOutput {
    candidates: Vec<HitCandidate>,
    aggs: HashMap<String, AggregationResults>,
}

fn sort_fields_needing_values(sort: &[SortSpec]) -> Vec<String> {
    let mut fields = Vec::new();
    for spec in sort {
        for field in spec.fields.keys() {
            if field != "_score" && field != "_id" && !fields.iter().any(|f| f == field) {
                fields.push(field.clone());
            }
        }
    }
    fields
}

/// Build `doc_seq -> value` maps for each requested custom-sort field,
/// sourced from the segment's already-eager `filter_store` (populated from
/// `filters.bin`) instead of full document field content. Every filterable
/// field type (`Keyword`/`Boolean`/`Date`/`Text`/`Integer`/`Float`) already
/// lands in one of `filter_store`'s three maps at write time (see
/// `SegmentWriter::add_document`), so this needs zero new disk I/O even
/// once `doc_store.bin`'s full content is only loaded lazily — sorting on a
/// field would otherwise be the one place deferred materialization forced a
/// disk read per candidate instead of just per returned page. Built once
/// per segment per query, not once per hit.
fn build_sort_value_maps(
    store: &FilterStore,
    fields: &[String],
) -> HashMap<String, HashMap<u32, String>> {
    fields
        .iter()
        .map(|field| {
            let map = if let Some(entries) = store.string_fields.get(field) {
                entries.iter().map(|(seq, v)| (*seq, v.clone())).collect()
            } else if let Some(entries) = store.integer_fields.get(field) {
                entries
                    .iter()
                    .map(|(seq, v)| (*seq, v.to_string()))
                    .collect()
            } else if let Some(entries) = store.float_fields.get(field) {
                entries
                    .iter()
                    .map(|(seq, v)| (*seq, v.to_string()))
                    .collect()
            } else {
                HashMap::new()
            };
            (field.clone(), map)
        })
        .collect()
}

fn extract_sort_values(
    doc_seq: u32,
    fields: &[String],
    sort_value_maps: &HashMap<String, HashMap<u32, String>>,
) -> Vec<String> {
    fields
        .iter()
        .map(|field| {
            sort_value_maps
                .get(field)
                .and_then(|m| m.get(&doc_seq))
                .cloned()
                .unwrap_or_default()
        })
        .collect()
}

fn candidate_sort_value<'a>(
    cand: &'a HitCandidate,
    field: &str,
    sort_value_fields: &[String],
) -> std::borrow::Cow<'a, str> {
    match field {
        "_score" => std::borrow::Cow::Owned(format!("{}", cand.score)),
        "_id" => std::borrow::Cow::Borrowed(cand.doc_id.0.as_str()),
        _ => sort_value_fields
            .iter()
            .position(|f| f == field)
            .and_then(|i| cand.sort_values.get(i))
            .map(|s| std::borrow::Cow::Borrowed(s.as_str()))
            .unwrap_or(std::borrow::Cow::Borrowed("")),
    }
}

// ─── Aggregation functions ──────────────────────────────────────────────────

pub fn compute_single_aggregation(store: &FilterStore, field: &str) -> AggregationResults {
    let mut counts: HashMap<String, usize> = HashMap::new();

    if let Some(entries) = store.string_fields.get(field) {
        for (_, val) in entries {
            *counts.entry(val.clone()).or_default() += 1;
        }
    }

    let mut buckets: Vec<AggBucket> = counts
        .into_iter()
        .map(|(k, c)| AggBucket {
            key: k,
            doc_count: c,
        })
        .collect();
    buckets.sort_by_key(|b| std::cmp::Reverse(b.doc_count));

    AggregationResults {
        per_document: Some(AggBucketResult { buckets }),
        total_documents: None,
        matched_docs: None,
        extra: HashMap::new(),
    }
}

pub fn compute_cardinality(store: &FilterStore, field: &str) -> AggregationResults {
    let count = store
        .string_fields
        .get(field)
        .map(|entries| {
            let unique: HashSet<&str> = entries.iter().map(|(_, v)| v.as_str()).collect();
            unique.len()
        })
        .unwrap_or(0);

    AggregationResults {
        per_document: None,
        total_documents: Some(AggMetricResult { value: count }),
        matched_docs: None,
        extra: HashMap::new(),
    }
}

pub fn compute_composite(
    store: &FilterStore,
    composite: &kosha_core::AggComposite,
) -> AggregationResults {
    let mut buckets = Vec::new();
    if let Some(source) = composite.sources.first() {
        for (agg_name, terms_spec) in &source.source {
            let field = &terms_spec.terms.field;
            if let Some(entries) = store.string_fields.get(field) {
                let mut seen: HashMap<&str, usize> = HashMap::new();
                for (_, val) in entries {
                    *seen.entry(val.as_str()).or_default() += 1;
                }
                let _ = agg_name;
                for (key, count) in seen {
                    if buckets.len() >= composite.size {
                        break;
                    }
                    let mut key_map = HashMap::new();
                    key_map.insert(field.clone(), key.to_string());
                    buckets.push(AggCompositeBucket {
                        key: key_map,
                        doc_count: count,
                    });
                }
            }
        }
    }

    let after_key = buckets.last().map(|b| b.key.clone());

    AggregationResults {
        per_document: None,
        total_documents: None,
        matched_docs: Some(AggCompositeResult { buckets, after_key }),
        extra: HashMap::new(),
    }
}

/// True when `cand` sorts strictly after the search_after cursor.
fn candidate_is_strictly_after_cursor(
    cand: &HitCandidate,
    after: &[String],
    sort: &[SortSpec],
    sort_value_fields: &[String],
) -> bool {
    if sort.is_empty() {
        // Default ranking: score desc, then _id asc. Cursor is typically [_id]
        // when callers only paginate by id (Decover embed updater).
        if after.len() == 1 {
            return cand.doc_id.0.as_str() > after[0].as_str();
        }
        return false;
    }

    let mut idx = 0usize;
    for spec in sort {
        for (field, order) in &spec.fields {
            if idx >= after.len() {
                return true;
            }
            let val = candidate_sort_value(cand, field, sort_value_fields);
            let cursor = &after[idx];
            let cmp = if field == "_score" {
                let val_f: f64 = val.parse().unwrap_or(0.0);
                let cur_f: f64 = cursor.parse().unwrap_or(0.0);
                val_f
                    .partial_cmp(&cur_f)
                    .unwrap_or(std::cmp::Ordering::Equal)
            } else {
                val.as_ref().cmp(cursor.as_str())
            };
            // Interpret comparison in the field's sort direction: "greater"
            // means further along the result list.
            let directed = if order.order == "desc" {
                cmp.reverse()
            } else {
                cmp
            };
            match directed {
                std::cmp::Ordering::Greater => return true,
                std::cmp::Ordering::Less => return false,
                std::cmp::Ordering::Equal => idx += 1,
            }
        }
    }
    false
}

fn compare_candidate_sort_keys(
    a: &HitCandidate,
    b: &HitCandidate,
    sort: &[SortSpec],
    sort_value_fields: &[String],
) -> std::cmp::Ordering {
    for spec in sort {
        for (field, order) in &spec.fields {
            let ord = match field.as_str() {
                "_score" => a
                    .score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal),
                "_id" => a.doc_id.0.cmp(&b.doc_id.0),
                _ => {
                    let a_val = candidate_sort_value(a, field, sort_value_fields);
                    let b_val = candidate_sort_value(b, field, sort_value_fields);
                    a_val.as_ref().cmp(b_val.as_ref())
                }
            };
            let ord = if order.order == "desc" {
                ord.reverse()
            } else {
                ord
            };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
    }
    a.doc_id.0.cmp(&b.doc_id.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::{DocumentId, Field, ManifestEntry, SegmentId, SortOrder};
    use kosha_segment::SegmentWriter;

    fn mk_query(text: &str, max: usize) -> SearchQuery {
        SearchQuery {
            query_text: text.into(),
            max_results: max,
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

    /// Write a one-document segment containing `text` and return its dir.
    fn mk_segment(root: &std::path::Path, ns: &str, seg: &str, text: &str) -> PathBuf {
        let seg_dir = root.join(ns).join(seg);
        let mut w = SegmentWriter::new(SegmentId(seg.into()), seg_dir.clone());
        w.add_document(
            DocumentId(format!("{seg}-d1")),
            vec![Field::text("t", text)],
        );
        w.finalize(Bm25Params::default()).unwrap();
        seg_dir
    }

    fn ledger_snapshot(searcher: &Searcher) -> (u64, u64, usize) {
        let st = searcher.ledger.state.lock().unwrap();
        (st.live, st.reserved, st.active)
    }

    #[test]
    fn eviction_skips_segments_pinned_by_inflight_references() {
        // The pre-ledger cache evicted purely by LRU order: an entry whose
        // Arc was still held by an in-flight request would be dropped from
        // the cache's bookkeeping while its memory stayed fully alive —
        // "eviction" that freed nothing. Now eviction must skip pinned
        // entries (freeing them is impossible) and evict idle ones instead.
        let dir = std::env::temp_dir().join("kosha-test-evict-skips-pinned");
        let _ = std::fs::remove_dir_all(&dir);
        let s1_dir = mk_segment(&dir, "test", "s1", "alpha");
        let s2_dir = mk_segment(&dir, "test", "s2", "alpha");

        // Byte budget of zero: every insert immediately wants to evict
        // everything evictable.
        let searcher = Searcher::with_segment_cache_limits(dir.clone(), 10, 0);

        // Open s1 and keep the Arc — simulating an in-flight request.
        let pinned = searcher
            .open_segment("test", "s1", s1_dir, false, None, None)
            .unwrap();
        // Opening s2 triggers insert-time eviction. At that instant *both*
        // entries are pinned (s1 by our Arc, s2 by open_segment's
        // own about-to-be-returned Arc), so nothing can be freed yet —
        // enforcement is lazy by design and re-runs at the next
        // opportunity. Drop s2's returned Arc and re-enforce: now s2 is
        // idle (evictable) while s1 stays pinned.
        drop(
            searcher
                .open_segment("test", "s2", s2_dir, false, None, None)
                .unwrap(),
        );
        searcher.segment_cache.enforce_budget();

        let key1 = ("test".to_string(), "s1".to_string(), false);
        let key2 = ("test".to_string(), "s2".to_string(), false);
        assert!(
            searcher.segment_cache.contains(&key1),
            "pinned segment must not be evicted — removing it frees nothing"
        );
        assert!(
            !searcher.segment_cache.contains(&key2),
            "idle segment should have been evicted to chase the byte budget"
        );

        // Once the in-flight reference drops, enforcement evicts s1 too.
        drop(pinned);
        searcher.segment_cache.enforce_budget();
        assert!(
            !searcher.segment_cache.contains(&key1),
            "segment must become evictable once no request pins it"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn live_bytes_follow_real_references_not_cache_bookkeeping() {
        // `live` must reflect actual referenced memory: counted while the
        // cache or any request can reach the segment, released only when
        // the last Arc drops.
        let dir = std::env::temp_dir().join("kosha-test-live-bytes");
        let _ = std::fs::remove_dir_all(&dir);
        let s1_dir = mk_segment(&dir, "test", "s1", "alpha");
        let s1_bytes = approx_segment_bytes(&s1_dir, false);
        assert!(s1_bytes > 0);

        let searcher = Searcher::with_segment_cache_limits(dir.clone(), 10, u64::MAX);
        let held = searcher
            .open_segment("test", "s1", s1_dir, false, None, None)
            .unwrap();
        assert_eq!(ledger_snapshot(&searcher).0, s1_bytes);

        // Dropping the request's Arc alone frees nothing (cache still
        // holds it) — live must not move.
        drop(held);
        assert_eq!(ledger_snapshot(&searcher).0, s1_bytes);

        // Evicting the now-idle entry drops the last Arc → live returns to
        // zero, i.e. eviction and actual memory release now coincide.
        searcher.segment_cache.evict_idle(u64::MAX);
        assert_eq!(ledger_snapshot(&searcher).0, 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn admission_sheds_search_when_live_memory_stays_pinned() {
        // With another search's reservation filling the watermark and
        // nothing evictable, a new search must come back Overloaded after
        // the admission timeout instead of piling on (the staging OOM
        // pattern: concurrent broad queries each pinning the whole
        // namespace).
        let dir = std::env::temp_dir().join("kosha-test-admission-sheds");
        let _ = std::fs::remove_dir_all(&dir);
        mk_segment(&dir, "test", "s1", "alpha");

        let searcher = Searcher::with_memory_limits(
            dir.clone(),
            10,
            u64::MAX,
            1, // watermark of one byte — any reservation fills it
            Duration::from_millis(50),
        );
        // Simulate an admitted in-flight search holding the watermark.
        let outstanding = searcher.ledger.admit(1, |_| {}).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: 1,
            }],
        };
        let result = searcher.search(
            &NamespaceId("test".into()),
            &manifest,
            &mk_query("alpha", 10),
            None,
        );
        assert!(
            matches!(result, Err(KoshaError::Overloaded(_))),
            "expected Overloaded, got {result:?}"
        );

        // Once the outstanding permit releases, the same search is admitted.
        drop(outstanding);
        let result = searcher
            .search(
                &NamespaceId("test".into()),
                &manifest,
                &mk_query("alpha", 10),
                None,
            )
            .unwrap();
        assert_eq!(result.total_hits, 1);

        // Reservations must be fully returned once searches finish.
        let (_, reserved, active) = ledger_snapshot(&searcher);
        assert_eq!((reserved, active), (0, 0));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn search_with_stats_reports_phase_timings_and_open_counts() {
        let dir = std::env::temp_dir().join("kosha-test-search-stats");
        let _ = std::fs::remove_dir_all(&dir);
        mk_segment(&dir, "test", "s1", "alpha beta");
        mk_segment(&dir, "test", "s2", "alpha beta");
        let manifest = Manifest {
            version: 1,
            segments: vec![
                ManifestEntry {
                    segment_id: SegmentId("s1".into()),
                    doc_count: 1,
                },
                ManifestEntry {
                    segment_id: SegmentId("s2".into()),
                    doc_count: 1,
                },
            ],
        };
        let searcher = Searcher::new(dir.clone());

        // Cold: both segments must be counted as cold opens.
        let (result, stats) = searcher
            .search_with_stats(
                &NamespaceId("test".into()),
                &manifest,
                &mk_query("alpha", 10),
                None,
            )
            .unwrap();
        assert_eq!(result.total_hits, 2);
        assert_eq!((stats.open_cold, stats.open_cached), (2, 0));
        assert!(stats.score_wall_ms > 0.0);
        assert!(stats.open_total_ms > 0.0);

        // Warm: same search must be all cache hits, zero cold opens.
        let (_, stats) = searcher
            .search_with_stats(
                &NamespaceId("test".into()),
                &manifest,
                &mk_query("alpha", 10),
                None,
            )
            .unwrap();
        assert_eq!((stats.open_cold, stats.open_cached), (0, 2));
        assert_eq!(stats.open_total_ms, 0.0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn single_search_larger_than_watermark_still_admitted() {
        // Anti-starvation: the watermark bounds concurrent pile-up, not
        // single-query size. A lone search over a namespace bigger than the
        // watermark must degrade like the pre-ledger code (LRU churn), not
        // be rejected or wait forever.
        let dir = std::env::temp_dir().join("kosha-test-admission-lone");
        let _ = std::fs::remove_dir_all(&dir);
        mk_segment(&dir, "test", "s1", "alpha");
        mk_segment(&dir, "test", "s2", "alpha");

        let searcher = Searcher::with_memory_limits(
            dir.clone(),
            10,
            u64::MAX,
            1, // both segments together vastly exceed this
            Duration::from_millis(50),
        );
        let manifest = Manifest {
            version: 1,
            segments: vec![
                ManifestEntry {
                    segment_id: SegmentId("s1".into()),
                    doc_count: 1,
                },
                ManifestEntry {
                    segment_id: SegmentId("s2".into()),
                    doc_count: 1,
                },
            ],
        };
        let result = searcher
            .search(
                &NamespaceId("test".into()),
                &manifest,
                &mk_query("alpha", 10),
                None,
            )
            .unwrap();
        assert_eq!(result.total_hits, 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wildcard_matching_works() {
        let terms = vec!["hello", "world", "help", "helm", "held"];
        let matched = wildcard_terms(&terms, "hel*", true);
        assert_eq!(matched.len(), 4);
        assert!(matched.contains(&"hello".to_string()));
        assert!(matched.contains(&"help".to_string()));
        assert!(matched.contains(&"helm".to_string()));
        assert!(matched.contains(&"held".to_string()));
    }

    #[test]
    fn match_phrase_no_slop() {
        // Positions: doc has "quick" at 0, "brown" at 1, "fox" at 2.
        let postings = vec![vec![0u32], vec![1u32], vec![2u32]];
        let score = match_phrase_score(&postings, 0);
        assert!((score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn match_phrase_with_slop() {
        // Positions: "quick" at 0, "fox" at 2 (skipping "brown").
        let postings = vec![vec![0u32], vec![2u32]];
        let score = match_phrase_score(&postings, 1);
        assert!((score - 1.0).abs() < 1e-6, "slop=1 should match gap of 2");
    }

    #[test]
    fn match_phrase_no_match() {
        // Positions: "quick" at 0, "fox" at 5 (too far).
        let postings = vec![vec![0u32], vec![5u32]];
        let score = match_phrase_score(&postings, 2);
        assert_eq!(score, 0.0, "slop=2 should not match gap of 5");
    }

    #[test]
    fn aggregate_terms() {
        let mut store = FilterStore::default();
        store.string_fields.insert(
            "documentId".to_string(),
            vec![(0, "d1".into()), (1, "d2".into()), (2, "d1".into())],
        );
        let result = compute_single_aggregation(&store, "documentId");
        let per_doc = result.per_document.unwrap();
        assert_eq!(per_doc.buckets.len(), 2);
        assert_eq!(per_doc.buckets[0].key, "d1");
        assert_eq!(per_doc.buckets[0].doc_count, 2);
    }

    #[test]
    fn cardinality_aggregate() {
        let mut store = FilterStore::default();
        store.string_fields.insert(
            "documentId".to_string(),
            vec![(0, "d1".into()), (1, "d2".into()), (2, "d1".into())],
        );
        let result = compute_cardinality(&store, "documentId");
        let total = result.total_documents.unwrap();
        assert_eq!(total.value, 2);
    }

    #[test]
    fn repeated_search_serves_warm_segment_from_memory_not_disk() {
        // Regression test for the "warm NVMe cache still re-parses every
        // segment on every query" bug: without an in-memory segment cache,
        // deleting `inverted.idx` between two searches would make the second
        // search fail to score, even though nothing about the query or
        // manifest changed. `inverted.idx` stays fully resident once a
        // segment is cached (only `doc_store.bin`'s full field content is
        // lazily loaded, per issue #37's follow-up) — deleting it must not
        // affect a later search against the same cached segment.
        let dir = std::env::temp_dir().join("kosha-test-segment-cache-warm");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir.clone());
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("t", "hello world")],
        );
        w.finalize(Bm25Params::default()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: 1,
            }],
        };
        let searcher = Searcher::new(dir.clone());
        let q = mk_query("hello", 10);

        let first = searcher.search(&ns, &manifest, &q, None).unwrap();
        assert_eq!(first.total_hits, 1, "first search should hit the segment");

        // Simulate the NVMe cache having evicted (or never re-fetched) this
        // segment's inverted index — the parsed segment should already be
        // cached in memory from the first search, so this must not matter.
        // (`doc_store.bin` is deliberately left in place: full field content
        // is now loaded lazily per query, by design — see
        // `full_page_materialization_needs_doc_store_on_disk_but_scoring_does_not`
        // for that half of the contract.)
        std::fs::remove_file(seg_dir.join("inverted.idx")).unwrap();

        let second = searcher.search(&ns, &manifest, &q, None).unwrap();
        assert_eq!(
            second.total_hits, 1,
            "second search must be served from the in-memory segment cache, \
             not re-read the (now-deleted) inverted index from disk"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_cache_evicts_on_byte_budget_not_just_entry_count() {
        // Regression test for a real staging OOM: an unfiltered query opens
        // every segment in the manifest in one shot (nothing to
        // bloom-prune), so a generous *entry count* cap alone doesn't bound
        // memory if individual segments are large — the cache must also
        // evict once an approximate *byte* budget is exceeded, even with
        // plenty of count headroom left.
        let dir = std::env::temp_dir().join("kosha-test-segment-cache-bytes");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());

        // Both segments must contain every query term — multi-term BM25 is
        // AND, and term-bloom prune skips segments missing any required term.
        let seg1_dir = dir.join(&ns.0).join("s1");
        let mut w1 = SegmentWriter::new(SegmentId("s1".into()), seg1_dir.clone());
        w1.add_document(
            DocumentId("d1".into()),
            vec![Field::text("t", "alpha beta")],
        );
        w1.finalize(Bm25Params::default()).unwrap();

        let seg2_dir = dir.join(&ns.0).join("s2");
        let mut w2 = SegmentWriter::new(SegmentId("s2".into()), seg2_dir.clone());
        w2.add_document(
            DocumentId("d2".into()),
            vec![Field::text("t", "alpha beta")],
        );
        w2.finalize(Bm25Params::default()).unwrap();

        let seg_size = |dir: &std::path::Path| -> u64 {
            [
                "doc_store.bin",
                "inverted.idx",
                "filters.bin",
                "footer.json",
            ]
            .iter()
            .map(|f| std::fs::metadata(dir.join(f)).unwrap().len())
            .sum()
        };
        let seg1_bytes = seg_size(&seg1_dir);
        let seg2_bytes = seg_size(&seg2_dir);

        let manifest = Manifest {
            version: 1,
            segments: vec![
                ManifestEntry {
                    segment_id: SegmentId("s1".into()),
                    doc_count: 1,
                },
                ManifestEntry {
                    segment_id: SegmentId("s2".into()),
                    doc_count: 1,
                },
            ],
        };

        // Generous entry-count cap (10) but a byte budget that fits s1 alone,
        // not both — inserting s2 must evict s1 to stay under budget.
        let max_bytes = seg1_bytes + seg2_bytes - 1;
        let searcher = Searcher::with_segment_cache_limits(dir.clone(), 10, max_bytes);
        let q = mk_query("alpha beta", 10);

        let first = searcher.search(&ns, &manifest, &q, None).unwrap();
        assert_eq!(first.total_hits, 2, "both segments should match once");

        // s1 (opened first, so oldest in LRU order) should have been evicted
        // to make room for s2 — delete s1's files and confirm the next
        // search actually needs to re-read them (and fails, since they're
        // gone) rather than serving a stale in-memory hit.
        std::fs::remove_file(seg1_dir.join("doc_store.bin")).unwrap();
        std::fs::remove_file(seg1_dir.join("inverted.idx")).unwrap();

        let second = searcher.search(&ns, &manifest, &q, None);
        assert!(
            second.is_err(),
            "s1 should have been evicted by the byte budget (despite the \
             generous count cap), so re-opening it after deleting its files \
             must fail — a successful search here would mean the cache grew \
             past its byte budget"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn search_with_wildcard() {
        let dir = std::env::temp_dir().join("kosha-test-wildcard");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir);
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("t", "hello world")],
        );
        w.add_document(
            DocumentId("d2".into()),
            vec![Field::text("t", "help others")],
        );
        w.finalize(Bm25Params::default()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: 2,
            }],
        };
        let searcher = Searcher::new(dir.clone());
        let q = SearchQuery {
            query_text: "".into(),
            max_results: 10,
            from: 0,
            bm25_params: Bm25Params::default(),
            filter: None,
            sort: vec![],
            search_after: None,
            highlight: None,
            aggs: HashMap::new(),
            wildcard: Some(kosha_core::WildcardQuery {
                field: "t".into(),
                pattern: "hel*".into(),
                case_insensitive: true,
            }),
            match_phrase: None,
            knn: None,
        };
        let r = searcher.search(&ns, &manifest, &q, None).unwrap();
        assert_eq!(r.total_hits, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn knn_query_still_finds_vectors_after_lazy_loading_change() {
        // SegmentReader::open_with_options skips vectors/HNSW for lexical
        // queries; this proves the KNN path (query.knn.is_some()) still
        // loads and searches them correctly.
        let dir = std::env::temp_dir().join("kosha-test-knn-lazy");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir);
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::vector("contentEmbedding", vec![1.0, 0.0, 0.0])],
        );
        w.add_document(
            DocumentId("d2".into()),
            vec![Field::vector("contentEmbedding", vec![0.0, 1.0, 0.0])],
        );
        w.finalize(Bm25Params::default()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: 2,
            }],
        };
        let searcher = Searcher::new(dir.clone());
        let mut q = mk_query("", 10);
        q.knn = Some(kosha_core::KnnQuery {
            field: "contentEmbedding".into(),
            vector: vec![1.0, 0.0, 0.0],
            k: 1,
            num_candidates: 10,
            filter: None,
        });
        let r = searcher.search(&ns, &manifest, &q, None).unwrap();
        assert_eq!(r.total_hits, 1);
        assert_eq!(r.results[0].doc_id.0, "d1");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn full_page_materialization_needs_doc_store_on_disk_but_scoring_does_not() {
        // Structural proof of the lazy doc_store contract (issue #37
        // follow-up): opening a segment and scoring/counting hits never
        // reads `doc_store.bin`'s full field content — only the small
        // `doc_store.offsets` sidecar (doc_id + field_length per doc,
        // proportional to document count, not content size). Full field
        // content is read from disk only when a query actually materializes
        // a result page, and only for the documents on that page.
        let dir = std::env::temp_dir().join("kosha-test-lazy-doc-store-contract");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir.clone());
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("t", "hello world")],
        );
        w.finalize(Bm25Params::default()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: 1,
            }],
        };
        // Fresh searcher (no warm in-memory segment cache) so opening the
        // segment for these searches goes through the real `try_read_doc_index`
        // -> `DocStoreAccess::Lazy` path, not a cached `Eager` fallback from
        // an earlier call in this test.
        let searcher = Searcher::new(dir.clone());

        // `doc_store.bin` deleted entirely, `doc_store.offsets` untouched.
        std::fs::remove_file(seg_dir.join("doc_store.bin")).unwrap();

        // max_results: 0 never materializes a page, so it must succeed and
        // report the correct count purely from resident metadata.
        let count_only = mk_query("hello", 0);
        let counted = searcher
            .search(&ns, &manifest, &count_only, None)
            .expect("scoring/counting must not require doc_store.bin on disk");
        assert_eq!(counted.total_hits, 1);
        assert!(counted.results.is_empty());

        // A query that actually needs to return the document correctly
        // fails now that its full content is genuinely gone — this is the
        // honest lazy-loading tradeoff, not a bug: the in-memory cache never
        // held full field content in the first place, so there is nothing
        // to silently fall back to.
        let full_page = mk_query("hello", 10);
        let err = searcher
            .search(&ns, &manifest, &full_page, None)
            .expect_err("materializing the page must surface the missing doc_store.bin");
        assert!(matches!(err, KoshaError::Io(_)), "unexpected error: {err}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn search_with_doc_store_hydrator_fetches_only_page_segments_on_demand() {
        // Scoring-set-only hydration contract: a cold search has only the
        // offsets sidecar locally, so `doc_store.bin` is fetched on demand —
        // via the hydrator callback — for *only* the segments holding the
        // returned page, not the whole manifest. Includes a non-matching
        // segment that must never be requested, and a count-only call that
        // must not invoke the hydrator at all.
        let dir = std::env::temp_dir().join("kosha-test-doc-store-on-demand");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());

        let build_seg = |id: &str, doc_id: &str, text: &str| {
            let seg_dir = dir.join(&ns.0).join(id);
            let mut w = SegmentWriter::new(SegmentId(id.into()), seg_dir.clone());
            w.add_document(DocumentId(doc_id.into()), vec![Field::text("t", text)]);
            w.finalize(Bm25Params::default()).unwrap();
            // Back up doc_store.bin, then remove it to model scoring-set-only
            // hydration: offsets sidecar present, doc_store.bin absent.
            let bin = seg_dir.join("doc_store.bin");
            std::fs::copy(&bin, seg_dir.join("doc_store.bin.bak")).unwrap();
            std::fs::remove_file(&bin).unwrap();
            seg_dir
        };

        let _s1 = build_seg("s1", "d1", "hello world");
        let _s2 = build_seg("s2", "d2", "hello moon");
        let s3 = build_seg("s3", "d3", "completely unrelated"); // no hit

        let manifest = Manifest {
            version: 1,
            segments: ["s1", "s2", "s3"]
                .into_iter()
                .map(|id| ManifestEntry {
                    segment_id: SegmentId(id.into()),
                    doc_count: 1,
                })
                .collect(),
        };
        // Fresh searcher so each segment is actually opened (Lazy), not
        // served from a warm cache populated by an earlier call here.
        let searcher = Searcher::new(dir.clone());

        let requested: Arc<Mutex<Vec<PathBuf>>> = Arc::new(Mutex::new(Vec::new()));
        let requested_clone = Arc::clone(&requested);
        let restore = move |segs: &[PathBuf]| {
            for p in segs {
                let bak = p.join("doc_store.bin.bak");
                let bin = p.join("doc_store.bin");
                if !bin.exists() && bak.exists() {
                    std::fs::copy(&bak, &bin).unwrap();
                }
                requested_clone.lock().unwrap().push(p.clone());
            }
        };

        let q = mk_query("hello", 5);
        let r = searcher
            .search_with_doc_store_hydrator(&ns, &manifest, &q, None, Some(&restore))
            .map(|(result, _stats)| result)
            .expect("materialize must succeed after the hydrator restores doc_store.bin");
        assert_eq!(r.total_hits, 2);
        assert_eq!(r.results.len(), 2);
        // Fields were genuinely materialized from the restored doc_store.bin.
        assert!(r.results.iter().any(|d| d.doc_id.0 == "d1"));
        assert!(r.results.iter().any(|d| d.doc_id.0 == "d2"));

        // Exactly the two page-bearing segments were requested; the
        // non-matching s3 segment must never reach the hydrator.
        let req = requested.lock().unwrap();
        assert_eq!(
            req.len(),
            2,
            "expected exactly the 2 page segments, got {req:?}"
        );
        assert!(
            req.iter().all(|p| p != &s3),
            "non-matching segment s3 was hydrated"
        );
        drop(req);

        // A count-only query materializes no page → hydrator untouched.
        let before = requested.lock().unwrap().len();
        let count_only = mk_query("hello", 0);
        let counted = searcher
            .search_with_doc_store_hydrator(&ns, &manifest, &count_only, None, Some(&restore))
            .map(|(result, _stats)| result)
            .unwrap();
        assert_eq!(counted.total_hits, 2);
        assert!(counted.results.is_empty());
        assert_eq!(
            requested.lock().unwrap().len(),
            before,
            "count-only query must not trigger doc_store hydration"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_segment_without_offsets_sidecar_still_searches() {
        // Backward compatibility: a segment written before `doc_store.offsets`
        // existed (or one where the sidecar was otherwise lost) has no offset
        // table to open lazily. `try_read_doc_index` returns `None` for a
        // missing file, so `SegmentReader` falls back to `DocStoreAccess::Eager`
        // — the exact full-parse behavior every segment used before lazy
        // loading, not a degraded or partial mode.
        let dir = std::env::temp_dir().join("kosha-test-legacy-no-offsets-sidecar");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir.clone());
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("t", "hello world")],
        );
        w.add_document(
            DocumentId("d2".into()),
            vec![Field::text("t", "hello moon")],
        );
        w.finalize(Bm25Params::default()).unwrap();

        std::fs::remove_file(seg_dir.join("doc_store.offsets")).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: 2,
            }],
        };
        let searcher = Searcher::new(dir.clone());
        let q = mk_query("hello", 10);
        let r = searcher
            .search(&ns, &manifest, &q, None)
            .expect("missing offsets sidecar must fall back to eager parse, not fail");
        assert_eq!(r.total_hits, 2);
        assert_eq!(r.results.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupted_offsets_sidecar_falls_back_safely() {
        // A `doc_store.offsets` that exists but is corrupt (truncated header,
        // or a declared doc_count that disagrees with footer.json) must not
        // panic or silently return wrong data — `try_read_doc_index` detects
        // the mismatch and returns `None`, degrading to the same full-parse
        // path as a legacy segment: slower, but still correct.
        let dir = std::env::temp_dir().join("kosha-test-corrupted-offsets-sidecar");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir.clone());
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("t", "hello world")],
        );
        w.add_document(
            DocumentId("d2".into()),
            vec![Field::text("t", "hello moon")],
        );
        w.finalize(Bm25Params::default()).unwrap();

        // Corrupt the declared doc_count so it disagrees with footer.json's
        // doc_count (2), tripping the mismatch check rather than the
        // truncation check.
        std::fs::write(seg_dir.join("doc_store.offsets"), 99u32.to_le_bytes()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: 2,
            }],
        };
        let searcher = Searcher::new(dir.clone());
        let q = mk_query("hello", 10);
        let r = searcher
            .search(&ns, &manifest, &q, None)
            .expect("corrupt offsets sidecar must fall back safely, not panic or error");
        assert_eq!(r.total_hits, 2);
        assert_eq!(r.results.len(), 2);

        // Also cover the truncated-header case (fewer than 4 bytes).
        std::fs::write(seg_dir.join("doc_store.offsets"), [0u8, 1u8]).unwrap();
        let searcher2 = Searcher::new(dir.clone());
        let r2 = searcher2
            .search(&ns, &manifest, &q, None)
            .expect("truncated offsets header must fall back safely, not panic or error");
        assert_eq!(r2.total_hits, 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn bounded_topk_selection_matches_full_sort_order() {
        // Regression test for issue #37's second fix: bounded top-k
        // selection (select_nth_unstable_by + a small sort of just the
        // page) instead of a full sort over every candidate. Cross-checks
        // against an unbounded query (max_results covering every hit) to
        // prove the partial-selection path returns exactly the same order
        // — for both the first page and a page deep enough to require
        // `from > 0`, rather than hand-deriving expected BM25 scores.
        let dir = std::env::temp_dir().join("kosha-test-bounded-topk");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir);
        let n = 60usize;
        for i in 0..n {
            // Distinct term frequency per doc so BM25 scores are unique and
            // the ranking is deterministic, not tie-broken by doc_id alone.
            let repeats = i + 1;
            let content = format!("{}filler", "contract ".repeat(repeats));
            w.add_document(
                DocumentId(format!("doc-{i:03}")),
                vec![Field::text("content", content)],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: n as u32,
            }],
        };
        let searcher = Searcher::new(dir.clone());

        let full = searcher
            .search(&ns, &manifest, &mk_query("contract", n), None)
            .unwrap();
        assert_eq!(full.total_hits, n);
        assert_eq!(full.results.len(), n);

        // First page: bounded selection with `to` well under total_hits.
        let page1 = searcher
            .search(&ns, &manifest, &mk_query("contract", 5), None)
            .unwrap();
        assert_eq!(page1.total_hits, n);
        assert_eq!(
            page1
                .results
                .iter()
                .map(|d| d.doc_id.0.clone())
                .collect::<Vec<_>>(),
            full.results[0..5]
                .iter()
                .map(|d| d.doc_id.0.clone())
                .collect::<Vec<_>>(),
            "bounded top-k page must match the equivalent slice of a full sort"
        );

        // Deep page: from > 0, still `to < total_hits` — exercises the
        // select_nth_unstable_by partition at a non-trivial offset.
        let mut deep_query = mk_query("contract", 5);
        deep_query.from = 20;
        let page2 = searcher.search(&ns, &manifest, &deep_query, None).unwrap();
        assert_eq!(
            page2
                .results
                .iter()
                .map(|d| d.doc_id.0.clone())
                .collect::<Vec<_>>(),
            full.results[20..25]
                .iter()
                .map(|d| d.doc_id.0.clone())
                .collect::<Vec<_>>(),
            "deep bounded page (from>0) must match the equivalent slice of a full sort"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn term_bloom_skips_segments_without_query_terms() {
        let dir = std::env::temp_dir().join("kosha-test-term-bloom-prune");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());

        let seg_a = dir.join(&ns.0).join("s-match");
        let mut wa = SegmentWriter::new(SegmentId("s-match".into()), seg_a.clone());
        wa.add_document(
            DocumentId("hit".into()),
            vec![Field::text("content", "contract language here")],
        );
        wa.finalize(Bm25Params::default()).unwrap();

        let seg_b = dir.join(&ns.0).join("s-miss");
        let mut wb = SegmentWriter::new(SegmentId("s-miss".into()), seg_b.clone());
        wb.add_document(
            DocumentId("miss".into()),
            vec![Field::text("content", "completely different vocabulary")],
        );
        wb.finalize(Bm25Params::default()).unwrap();

        let miss_footer = SegmentReader::read_footer(&seg_b).unwrap();
        assert!(
            !kosha_core::segment_may_contain_terms(
                &["contract".into()],
                kosha_core::TermBloomMode::And,
                miss_footer.term_bloom.as_ref(),
            ),
            "segment without 'contract' must be bloom-prunable"
        );

        let manifest = Manifest {
            version: 1,
            segments: vec![
                ManifestEntry {
                    segment_id: SegmentId("s-match".into()),
                    doc_count: 1,
                },
                ManifestEntry {
                    segment_id: SegmentId("s-miss".into()),
                    doc_count: 1,
                },
            ],
        };
        let searcher = Searcher::new(dir.clone());
        let r = searcher
            .search(&ns, &manifest, &mk_query("contract", 10), None)
            .unwrap();
        assert_eq!(r.total_hits, 1);
        assert_eq!(r.results[0].doc_id.0, "hit");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_term_and_intersects_and_scores() {
        // Multi-term queries take the AND path (HashMap posting lookup).
        // Docs must contain *all* terms; partial matches are excluded.
        let dir = std::env::temp_dir().join("kosha-test-multi-term-and");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir);
        w.add_document(
            DocumentId("both".into()),
            vec![Field::text("content", "contract dispute clause")],
        );
        w.add_document(
            DocumentId("only-contract".into()),
            vec![Field::text("content", "contract alone")],
        );
        w.add_document(
            DocumentId("only-dispute".into()),
            vec![Field::text("content", "dispute alone")],
        );
        w.finalize(Bm25Params::default()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: 3,
            }],
        };
        let searcher = Searcher::new(dir.clone());
        let r = searcher
            .search(&ns, &manifest, &mk_query("contract dispute", 10), None)
            .unwrap();
        assert_eq!(r.total_hits, 1);
        assert_eq!(r.results[0].doc_id.0, "both");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn deferred_materialization_pages_large_hit_sets() {
        // Issue #37: scoring must accumulate score-only candidates and clone
        // fields only for the returned page. Correctness check: many hits,
        // small max_results, full total_hits, and page docs still carry fields.
        let dir = std::env::temp_dir().join("kosha-test-deferred-materialize");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir);
        let n = 200usize;
        for i in 0..n {
            w.add_document(
                DocumentId(format!("doc-{i:04}")),
                vec![
                    Field::text(
                        "content",
                        format!(
                            "shared token in document {i} with padding {}",
                            "x".repeat(256)
                        ),
                    ),
                    Field::keyword("custodian", format!("user-{}", i % 7)),
                ],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: n as u32,
            }],
        };
        let searcher = Searcher::new(dir.clone());
        let r = searcher
            .search(&ns, &manifest, &mk_query("shared", 5), None)
            .unwrap();
        assert_eq!(r.total_hits, n);
        assert_eq!(r.results.len(), 5);
        for doc in &r.results {
            assert!(
                doc.fields.iter().any(|f| f.name == "content"),
                "page docs must still materialize fields"
            );
            assert!(
                doc.fields.iter().any(|f| f.name == "custodian"),
                "metadata fields must be present on the page"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn field_sort_still_works_with_deferred_materialization() {
        let dir = std::env::temp_dir().join("kosha-test-deferred-field-sort");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir);
        for (id, custodian) in [("a", "z-user"), ("b", "a-user"), ("c", "m-user")] {
            w.add_document(
                DocumentId(id.into()),
                vec![
                    Field::text("content", "shared token"),
                    Field::keyword("custodian", custodian),
                ],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: 3,
            }],
        };
        let searcher = Searcher::new(dir.clone());
        let mut custodian_sort = HashMap::new();
        custodian_sort.insert(
            "custodian".into(),
            SortOrder {
                order: "asc".into(),
            },
        );
        let r = searcher
            .search(
                &ns,
                &manifest,
                &SearchQuery {
                    query_text: "shared".into(),
                    max_results: 2,
                    from: 0,
                    bm25_params: Bm25Params::default(),
                    filter: None,
                    sort: vec![SortSpec {
                        fields: custodian_sort,
                    }],
                    search_after: None,
                    highlight: None,
                    aggs: HashMap::new(),
                    wildcard: None,
                    match_phrase: None,
                    knn: None,
                },
                None,
            )
            .unwrap();
        assert_eq!(r.total_hits, 3);
        assert_eq!(r.results.len(), 2);
        assert_eq!(r.results[0].doc_id.0, "b"); // a-user
        assert_eq!(r.results[1].doc_id.0, "c"); // m-user
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn search_after_by_id_paginates() {
        let dir = std::env::temp_dir().join("kosha-test-search-after");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir);
        for i in 0..5 {
            w.add_document(
                DocumentId(format!("doc-{i}")),
                vec![
                    Field::text("content", "shared token"),
                    Field::text("documentId", "same-doc"),
                ],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: 5,
            }],
        };
        let searcher = Searcher::new(dir.clone());
        let mut id_sort = std::collections::HashMap::new();
        id_sort.insert(
            "_id".into(),
            SortOrder {
                order: "asc".into(),
            },
        );

        let page1 = searcher
            .search(
                &ns,
                &manifest,
                &SearchQuery {
                    query_text: "shared".into(),
                    max_results: 2,
                    from: 0,
                    bm25_params: Bm25Params::default(),
                    filter: None,
                    sort: vec![SortSpec {
                        fields: id_sort.clone(),
                    }],
                    search_after: None,
                    highlight: None,
                    aggs: HashMap::new(),
                    wildcard: None,
                    match_phrase: None,
                    knn: None,
                },
                None,
            )
            .unwrap();
        assert_eq!(page1.results.len(), 2);
        assert_eq!(page1.results[0].doc_id.0, "doc-0");
        assert_eq!(page1.results[1].doc_id.0, "doc-1");

        let page2 = searcher
            .search(
                &ns,
                &manifest,
                &SearchQuery {
                    query_text: "shared".into(),
                    max_results: 2,
                    from: 0,
                    bm25_params: Bm25Params::default(),
                    filter: None,
                    sort: vec![SortSpec { fields: id_sort }],
                    search_after: Some(vec![page1.results[1].doc_id.0.clone()]),
                    highlight: None,
                    aggs: HashMap::new(),
                    wildcard: None,
                    match_phrase: None,
                    knn: None,
                },
                None,
            )
            .unwrap();
        assert_eq!(page2.results.len(), 2);
        assert_eq!(page2.results[0].doc_id.0, "doc-2");
        assert_eq!(page2.results[1].doc_id.0, "doc-3");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn filter_keeps_hits_across_segments() {
        let dir = std::env::temp_dir().join("kosha-test-filter-multiseg");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());

        for (seg, doc_id, matter) in [("s1", "a", "m1"), ("s2", "b", "m1"), ("s3", "c", "m2")] {
            let seg_dir = dir.join(&ns.0).join(seg);
            let mut w = SegmentWriter::new(SegmentId(seg.into()), seg_dir);
            w.add_document(
                DocumentId(doc_id.into()),
                vec![
                    Field::text("content", "shared token"),
                    Field::text("matterId", matter),
                ],
            );
            w.finalize(Bm25Params::default()).unwrap();
        }

        let manifest = Manifest {
            version: 1,
            segments: vec![
                ManifestEntry {
                    segment_id: SegmentId("s1".into()),
                    doc_count: 1,
                },
                ManifestEntry {
                    segment_id: SegmentId("s2".into()),
                    doc_count: 1,
                },
                ManifestEntry {
                    segment_id: SegmentId("s3".into()),
                    doc_count: 1,
                },
            ],
        };
        let searcher = Searcher::new(dir.clone());
        let q = SearchQuery {
            query_text: "shared".into(),
            max_results: 10,
            from: 0,
            bm25_params: Bm25Params::default(),
            filter: Some(kosha_core::FilterClause::Term {
                term: std::collections::HashMap::from([("matterId".into(), "m1".into())]),
            }),
            sort: vec![],
            search_after: None,
            highlight: None,
            aggs: HashMap::new(),
            wildcard: None,
            match_phrase: None,
            knn: None,
        };
        let r = searcher.search(&ns, &manifest, &q, None).unwrap();
        assert_eq!(
            r.total_hits, 2,
            "BM25+filter must keep hits from every segment"
        );
        let ids: std::collections::HashSet<_> =
            r.results.iter().map(|d| d.doc_id.0.as_str()).collect();
        assert!(ids.contains("a"));
        assert!(ids.contains("b"));

        // Bloom pruning: m2 segment footer must reject matterId=m1.
        let s3_footer = SegmentReader::read_footer(&dir.join(&ns.0).join("s3")).unwrap();
        assert!(!kosha_core::segment_may_match(
            q.filter.as_ref().unwrap(),
            s3_footer.filter_blooms.as_ref()
        ));
        let s1_footer = SegmentReader::read_footer(&dir.join(&ns.0).join("s1")).unwrap();
        assert!(kosha_core::segment_may_match(
            q.filter.as_ref().unwrap(),
            s1_footer.filter_blooms.as_ref()
        ));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Segments are now scored concurrently via `par_iter` (see
    /// `Searcher::score_segment`), so completion order across threads is not
    /// guaranteed run to run. Correctness must not depend on it: build a
    /// manifest wide enough to actually get scheduled across multiple
    /// threads, give every doc the same score (so ranking falls through to
    /// the `doc_id` tie-break), and assert the returned page is byte-for-byte
    /// identical across many repeated queries.
    #[test]
    fn parallel_segment_scoring_is_deterministic_across_many_segments() {
        let dir = std::env::temp_dir().join("kosha-test-parallel-determinism");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());

        let seg_count = 24;
        let mut manifest_segments = Vec::new();
        for i in 0..seg_count {
            let seg_name = format!("s{i}");
            let seg_dir = dir.join(&ns.0).join(&seg_name);
            let mut w = SegmentWriter::new(SegmentId(seg_name.clone()), seg_dir);
            w.add_document(
                DocumentId(format!("doc-{i:03}")),
                vec![Field::text(
                    "content",
                    "contract terms and conditions apply",
                )],
            );
            w.finalize(Bm25Params::default()).unwrap();
            manifest_segments.push(ManifestEntry {
                segment_id: SegmentId(seg_name),
                doc_count: 1,
            });
        }
        let manifest = Manifest {
            version: 1,
            segments: manifest_segments,
        };
        let searcher = Searcher::new(dir.clone());
        let q = SearchQuery {
            query_text: "contract".into(),
            max_results: 5,
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
        };

        let first = searcher.search(&ns, &manifest, &q, None).unwrap();
        assert_eq!(first.total_hits, seg_count, "every segment has one hit");
        let first_ids: Vec<String> = first.results.iter().map(|d| d.doc_id.0.clone()).collect();

        for _ in 0..19 {
            let r = searcher.search(&ns, &manifest, &q, None).unwrap();
            assert_eq!(r.total_hits, first.total_hits);
            let ids: Vec<String> = r.results.iter().map(|d| d.doc_id.0.clone()).collect();
            assert_eq!(
                ids, first_ids,
                "page contents/order must be stable across repeated parallel-scored queries"
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_footer_without_blooms_still_searches() {
        let dir = std::env::temp_dir().join("kosha-test-filter-legacy-footer");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir.clone());
        w.add_document(
            DocumentId("a".into()),
            vec![
                Field::text("content", "shared token"),
                Field::text("matterId", "m1"),
            ],
        );
        w.finalize(Bm25Params::default()).unwrap();

        // Strip blooms to simulate a pre-bloom segment.
        let mut footer = SegmentReader::read_footer(&seg_dir).unwrap();
        footer.filter_blooms = None;
        std::fs::write(
            seg_dir.join("footer.json"),
            serde_json::to_string_pretty(&footer).unwrap(),
        )
        .unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: 1,
            }],
        };
        let searcher = Searcher::new(dir.clone());
        let q = SearchQuery {
            query_text: "shared".into(),
            max_results: 10,
            from: 0,
            bm25_params: Bm25Params::default(),
            filter: Some(kosha_core::FilterClause::Term {
                term: std::collections::HashMap::from([("matterId".into(), "m1".into())]),
            }),
            sort: vec![],
            search_after: None,
            highlight: None,
            aggs: HashMap::new(),
            wildcard: None,
            match_phrase: None,
            knn: None,
        };
        let r = searcher.search(&ns, &manifest, &q, None).unwrap();
        assert_eq!(r.total_hits, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
