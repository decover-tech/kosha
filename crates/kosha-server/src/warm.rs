//! Usage-driven cache warming (#213): the bookkeeping half.
//!
//! Before this, warmup was a hand-maintained list (`KOSHA_WARMUP_NAMESPACES`)
//! warmed once at boot. Nothing knew which namespaces, segments or file
//! classes were actually read, so a namespace that became hot after boot
//! was never warmed, a listed one that went quiet wasted the boot pass, and
//! the segments a flush or compaction published stayed cold on every query
//! replica until a user query paid to fetch them.
//!
//! This module is pure state and policy, with no I/O:
//!
//! - [`AccessTracker`]: exponentially decayed read counters (hits and bytes)
//!   per namespace, per segment and per [`FileClass`], bounded in memory.
//! - [`WarmSet`]: the persisted top-K snapshot of those counters, written
//!   to object storage so a restarted pod warms what was hot, not what an
//!   environment variable says.
//! - [`plan`]: orders candidate items by observed read rate and cuts the
//!   list at a byte budget (`KOSHA_WARMUP_BUDGET_BYTES`), reporting what it
//!   kept and what it skipped.
//! - [`WarmReport`]: the log / `GET /v1/admin/warmup-status` record of one
//!   warming pass.
//!
//! The threads, the fetches and the manifest polling live in `warm_run.rs`.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The groups of segment files warming reasons about. Each is fetched by a
/// different path and has a very different size profile, so read rates and
/// budget decisions are kept per class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileClass {
    /// What every search needs before it can score a segment:
    /// `footer.json`, `inverted.idx`, `doc_store.offsets`, `filters.bin`
    /// (or `filters.toc` + `filters-NNN.col`, #244).
    Scoring,
    /// Posting shards, `postings-XX.bin`: read per query term.
    Postings,
    /// `doc_store.bin` (+ `doc_vectors.bin`): read for the returned page.
    DocStore,
    /// `vector.offsets` + `vector.idx` (and `vector.<n>.*` for further
    /// vector columns, #204): read by kNN probes.
    Vectors,
}

/// Every class, in warm order within a segment.
pub const CLASSES: [FileClass; 4] = [
    FileClass::Scoring,
    FileClass::Postings,
    FileClass::DocStore,
    FileClass::Vectors,
];

impl FileClass {
    pub fn index(self) -> usize {
        match self {
            Self::Scoring => 0,
            Self::Postings => 1,
            Self::DocStore => 2,
            Self::Vectors => 3,
        }
    }

    /// The class a segment file belongs to, by its basename. `None` for
    /// files warming never fetches (`_COMMIT`, span-cache entries).
    pub fn of_file(name: &str) -> Option<Self> {
        match name {
            "footer.json" | "inverted.idx" | "doc_store.offsets" | "filters.bin" => {
                Some(Self::Scoring)
            }
            // Columnar filter index (#244): the TOC and its column files.
            n if kosha_core::is_filter_column_file(n) => Some(Self::Scoring),
            "doc_store.bin" | "doc_vectors.bin" => Some(Self::DocStore),
            // Every vector column's index and sidecar (#204).
            n if kosha_core::is_vector_index_file(n) => Some(Self::Vectors),
            n if n.starts_with("postings-") && n.ends_with(".bin") => Some(Self::Postings),
            _ => None,
        }
    }

    /// Segments per warm batch. A batch holds a hydration permit while it
    /// runs, so classes with large files (doc stores, vector indexes) go two
    /// segments at a time to let live queries interleave — the same sizes
    /// the static boot warmup used.
    pub fn batch_len(self) -> usize {
        match self {
            Self::Scoring | Self::Postings => 8,
            Self::DocStore | Self::Vectors => 2,
        }
    }
}

/// Values per [`FileClass`], indexed by [`FileClass::index`].
type PerClass = [f64; 4];

/// Tuning for [`AccessTracker`].
#[derive(Clone, Debug, PartialEq)]
pub struct AccessConfig {
    /// Half-life of a read in the decayed counters
    /// (`KOSHA_ACCESS_HALF_LIFE_SECS`, default 600). A namespace not read
    /// for one half-life counts half as hot as it did.
    pub half_life: Duration,
    /// Most segments tracked at once (`KOSHA_ACCESS_MAX_SEGMENTS`, default
    /// 50,000). Past it, the coldest tenth is dropped.
    pub max_segments: usize,
}

impl Default for AccessConfig {
    fn default() -> Self {
        Self {
            half_life: Duration::from_secs(600),
            max_segments: 50_000,
        }
    }
}

impl AccessConfig {
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            half_life: env_u64("KOSHA_ACCESS_HALF_LIFE_SECS")
                .filter(|s| *s > 0)
                .map(Duration::from_secs)
                .unwrap_or(d.half_life),
            max_segments: env_u64("KOSHA_ACCESS_MAX_SEGMENTS")
                .filter(|n| *n > 0)
                .map(|n| n as usize)
                .unwrap_or(d.max_segments),
        }
    }
}

fn env_u64(var: &str) -> Option<u64> {
    std::env::var(var).ok().and_then(|v| v.trim().parse().ok())
}

/// One namespace's counters. Values are in *scaled* units (see
/// [`AccessTracker`]); read them through the tracker, which un-scales them.
#[derive(Default)]
struct NsAccess {
    /// Searches touching each class, once per search.
    hits: PerClass,
    /// Bytes fetched from object storage for each class.
    bytes: PerClass,
    /// Per segment: reads of each class.
    segments: HashMap<String, PerClass>,
}

struct TrackerInner {
    /// Tracker-clock time the scaled units are relative to. A read at time
    /// `t` adds `2^((t - origin) / half_life)`; the decayed value at `now`
    /// is the stored sum times `2^(-(now - origin) / half_life)`. That makes
    /// a record O(1) with no per-entry timestamp; `origin` moves forward
    /// (and every value is rescaled) before the weights could overflow.
    origin: f64,
    namespaces: HashMap<String, NsAccess>,
    segments: usize,
}

/// Rescale once the read weight passes `2^RESCALE_AFTER_HALF_LIVES`, far
/// below `f64`'s range.
const RESCALE_AFTER_HALF_LIVES: f64 = 64.0;

/// Exponentially decayed read counters per namespace, segment and file
/// class. Cheap enough for the search path: one mutex acquisition per
/// search and one hash lookup per segment, allocation only the first time a
/// segment is seen. Memory is bounded by [`AccessConfig::max_segments`]
/// and by [`AccessTracker::retain_segments`], which drops segments a
/// compaction removed from the manifest.
pub struct AccessTracker {
    cfg: AccessConfig,
    start: Instant,
    /// Test-only clock skew, in milliseconds.
    skew_ms: AtomicU64,
    inner: Mutex<TrackerInner>,
}

/// One namespace's decayed counters, as of the moment they were read.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct NamespaceRates {
    pub namespace: String,
    /// Decayed searches per class.
    pub hits: ClassValues,
    /// Decayed bytes fetched from object storage per class.
    pub bytes: ClassValues,
    /// Decayed reads per segment and class, hottest (by total) first.
    #[serde(skip)]
    pub segments: Vec<(String, PerClass)>,
}

impl NamespaceRates {
    /// The namespace's heat: decayed searches (every search reads the
    /// scoring class of the segments it scores).
    pub fn score(&self) -> f64 {
        self.hits.to_array().iter().cloned().fold(0.0, f64::max)
    }

    /// Per-segment rate lookup for planning.
    pub fn rate_index(&self) -> RateIndex<'_> {
        let mut hottest = [0.0f64; 4];
        for (_, v) in &self.segments {
            for (h, x) in hottest.iter_mut().zip(v) {
                *h = h.max(*x);
            }
        }
        RateIndex {
            by_segment: self.segments.iter().map(|(s, v)| (s.as_str(), v)).collect(),
            hottest,
        }
    }
}

/// [`NamespaceRates`] indexed by segment id.
pub struct RateIndex<'a> {
    by_segment: HashMap<&'a str, &'a PerClass>,
    hottest: PerClass,
}

impl RateIndex<'_> {
    /// Read rate of `segment` for `class`, falling back to the hottest
    /// segment's rate when the segment has never been read here (a segment a
    /// flush or compaction just published: every later search reads it).
    pub fn rate(&self, segment: &str, class: FileClass) -> f64 {
        let i = class.index();
        self.by_segment
            .get(segment)
            .map_or(self.hottest[i], |v| v[i])
    }
}

impl AccessTracker {
    pub fn new(cfg: AccessConfig) -> Self {
        Self {
            cfg,
            start: Instant::now(),
            skew_ms: AtomicU64::new(0),
            inner: Mutex::new(TrackerInner {
                origin: 0.0,
                namespaces: HashMap::new(),
                segments: 0,
            }),
        }
    }

    pub fn config(&self) -> &AccessConfig {
        &self.cfg
    }

    fn half_life_secs(&self) -> f64 {
        self.cfg.half_life.as_secs_f64().max(1e-3)
    }

    /// Seconds on the tracker's monotonic clock.
    fn now(&self) -> f64 {
        self.start.elapsed().as_secs_f64() + self.skew_ms.load(Ordering::Relaxed) as f64 / 1e3
    }

    /// Move the tracker's clock forward (tests simulate traffic shifts that
    /// take several half-lives).
    #[cfg(test)]
    pub fn advance(&self, by: Duration) {
        self.skew_ms
            .fetch_add(by.as_millis() as u64, Ordering::Relaxed);
    }

    /// The scaled weight of one read at `now`, rescaling every stored value
    /// first when the weight would grow past the rescale threshold.
    fn weight(&self, inner: &mut TrackerInner, now: f64) -> f64 {
        let h = self.half_life_secs();
        let mut exponent = (now - inner.origin) / h;
        if exponent > RESCALE_AFTER_HALF_LIVES {
            let factor = (-exponent).exp2();
            for ns in inner.namespaces.values_mut() {
                scale(&mut ns.hits, factor);
                scale(&mut ns.bytes, factor);
                for seg in ns.segments.values_mut() {
                    scale(seg, factor);
                }
            }
            inner.origin = now;
            exponent = 0.0;
        }
        exponent.exp2()
    }

    /// Multiplier from scaled units to decayed values at `now`.
    fn unscale(&self, inner: &TrackerInner, now: f64) -> f64 {
        (-(now - inner.origin) / self.half_life_secs()).exp2()
    }

    /// Record one search on `namespace`: each `(segment, class)` pair it
    /// read gets one hit, each class it touched one namespace-level hit,
    /// and `bytes_fetched` (per class, from object storage) is added to the
    /// namespace.
    pub fn record(&self, namespace: &str, reads: &[(&str, FileClass)], bytes_fetched: [u64; 4]) {
        if reads.is_empty() && bytes_fetched.iter().all(|b| *b == 0) {
            return;
        }
        let now = self.now();
        let mut inner = self.inner.lock().unwrap();
        let w = self.weight(&mut inner, now);
        let mut added = 0usize;
        {
            if !inner.namespaces.contains_key(namespace) {
                inner
                    .namespaces
                    .insert(namespace.to_string(), NsAccess::default());
            }
            let ns = inner.namespaces.get_mut(namespace).unwrap();
            let mut touched = [false; 4];
            for (segment, class) in reads {
                let i = class.index();
                touched[i] = true;
                match ns.segments.get_mut(*segment) {
                    Some(v) => v[i] += w,
                    None => {
                        let mut v = [0.0; 4];
                        v[i] = w;
                        ns.segments.insert((*segment).to_string(), v);
                        added += 1;
                    }
                }
            }
            for (i, t) in touched.iter().enumerate() {
                if *t {
                    ns.hits[i] += w;
                }
                ns.bytes[i] += bytes_fetched[i] as f64 * w;
            }
        }
        inner.segments += added;
        if inner.segments > self.cfg.max_segments {
            self.shed_coldest(&mut inner);
        }
    }

    /// Drop the coldest tenth of tracked segments (and namespaces left with
    /// none and no recent hits), so memory stays bounded however many
    /// segments a pod ever sees.
    fn shed_coldest(&self, inner: &mut TrackerInner) {
        let mut all: Vec<(f64, String, String)> = inner
            .namespaces
            .iter()
            .flat_map(|(ns, a)| {
                a.segments
                    .iter()
                    .map(move |(seg, v)| (v.iter().sum::<f64>(), ns.clone(), seg.clone()))
            })
            .collect();
        let target = self.cfg.max_segments - self.cfg.max_segments / 10;
        let drop = all.len().saturating_sub(target);
        if drop == 0 {
            return;
        }
        all.select_nth_unstable_by(drop - 1, |a, b| a.0.total_cmp(&b.0));
        for (_, ns, seg) in all.into_iter().take(drop) {
            if let Some(a) = inner.namespaces.get_mut(&ns) {
                if a.segments.remove(&seg).is_some() {
                    inner.segments -= 1;
                }
            }
        }
        inner
            .namespaces
            .retain(|_, a| !a.segments.is_empty() || a.hits.iter().any(|h| *h > 0.0));
    }

    /// Forget segments of `namespace` that are not in `live` (the current
    /// manifest): a compaction's inputs stop counting once its output is
    /// published.
    pub fn retain_segments(&self, namespace: &str, live: &HashSet<String>) {
        let mut inner = self.inner.lock().unwrap();
        let removed = match inner.namespaces.get_mut(namespace) {
            Some(ns) => {
                let before = ns.segments.len();
                ns.segments.retain(|seg, _| live.contains(seg));
                before - ns.segments.len()
            }
            None => 0,
        };
        inner.segments -= removed;
    }

    /// Decayed counters of one namespace, `None` when it was never read.
    pub fn namespace(&self, namespace: &str) -> Option<NamespaceRates> {
        let now = self.now();
        let inner = self.inner.lock().unwrap();
        let k = self.unscale(&inner, now);
        inner
            .namespaces
            .get(namespace)
            .map(|a| rates_of(namespace, a, k))
    }

    /// Every tracked namespace, hottest first.
    pub fn namespaces(&self) -> Vec<NamespaceRates> {
        let now = self.now();
        let inner = self.inner.lock().unwrap();
        let k = self.unscale(&inner, now);
        let mut out: Vec<NamespaceRates> = inner
            .namespaces
            .iter()
            .map(|(name, a)| rates_of(name, a, k))
            .collect();
        out.sort_by(|a, b| {
            b.score()
                .total_cmp(&a.score())
                .then_with(|| a.namespace.cmp(&b.namespace))
        });
        out
    }

    /// Up to `top` namespaces whose score is at least `min_score`, hottest
    /// first.
    pub fn hot_namespaces(&self, top: usize, min_score: f64) -> Vec<NamespaceRates> {
        let mut out = self.namespaces();
        out.retain(|n| n.score() >= min_score && n.score() > 0.0);
        out.truncate(top);
        out
    }

    /// Number of segments currently tracked.
    pub fn tracked_segments(&self) -> usize {
        self.inner.lock().unwrap().segments
    }

    /// Snapshot the hot set for persistence.
    pub fn warm_set(&self, role: &str, limits: &WarmSetLimits) -> WarmSet {
        let namespaces = self
            .hot_namespaces(limits.top_namespaces, limits.min_score)
            .into_iter()
            .map(|mut n| {
                n.segments.truncate(limits.top_segments);
                WarmSetNamespace {
                    namespace: n.namespace,
                    hits: n.hits,
                    bytes: n.bytes,
                    segments: n
                        .segments
                        .into_iter()
                        .map(|(segment_id, v)| WarmSetSegment {
                            segment_id,
                            hits: ClassValues::from_array(v),
                        })
                        .collect(),
                }
            })
            .collect();
        WarmSet {
            format: WARM_SET_FORMAT,
            role: role.to_string(),
            written_at_unix_ms: unix_ms(),
            half_life_secs: self.half_life_secs(),
            namespaces,
        }
    }

    /// Seed the counters from a persisted warm set, decayed for the time
    /// since it was written, so a restarted pod's ranking (and its next
    /// persisted set) starts from what was hot rather than from nothing.
    /// Adds to whatever is already recorded.
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
    pub fn seed(&self, set: &WarmSet) {
        let age_secs = unix_ms().saturating_sub(set.written_at_unix_ms) as f64 / 1e3;
        let decay = (-age_secs / self.half_life_secs()).exp2();
        let now = self.now();
        let mut inner = self.inner.lock().unwrap();
        let w = self.weight(&mut inner, now) * decay;
        let mut added = 0usize;
        for n in &set.namespaces {
            let ns = inner.namespaces.entry(n.namespace.clone()).or_default();
            add_scaled(&mut ns.hits, &n.hits.to_array(), w);
            add_scaled(&mut ns.bytes, &n.bytes.to_array(), w);
            for s in &n.segments {
                let v = ns.segments.entry(s.segment_id.clone()).or_insert_with(|| {
                    added += 1;
                    [0.0; 4]
                });
                add_scaled(v, &s.hits.to_array(), w);
            }
        }
        inner.segments += added;
        if inner.segments > self.cfg.max_segments {
            self.shed_coldest(&mut inner);
        }
    }
}

fn scale(v: &mut PerClass, factor: f64) {
    for x in v.iter_mut() {
        *x *= factor;
    }
}

#[cfg_attr(not(feature = "s3"), allow(dead_code))]
fn add_scaled(into: &mut PerClass, values: &PerClass, w: f64) {
    for (x, v) in into.iter_mut().zip(values) {
        if v.is_finite() && *v > 0.0 {
            *x += v * w;
        }
    }
}

fn rates_of(name: &str, a: &NsAccess, k: f64) -> NamespaceRates {
    let mut segments: Vec<(String, PerClass)> = a
        .segments
        .iter()
        .map(|(s, v)| (s.clone(), v.map(|x| x * k)))
        .collect();
    segments.sort_by(|a, b| {
        b.1.iter()
            .sum::<f64>()
            .total_cmp(&a.1.iter().sum::<f64>())
            .then_with(|| a.0.cmp(&b.0))
    });
    NamespaceRates {
        namespace: name.to_string(),
        hits: ClassValues::from_array(a.hits.map(|x| x * k)),
        bytes: ClassValues::from_array(a.bytes.map(|x| x * k)),
        segments,
    }
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ─── Persisted warm set ────────────────────────────────────────────────────

/// Current [`WarmSet::format`]. Readers accept any version and ignore
/// fields they don't know; every field has a serde default.
pub const WARM_SET_FORMAT: u32 = 1;

/// Per-class numbers in the persisted warm set and the status JSON.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ClassValues {
    #[serde(default)]
    pub scoring: f64,
    #[serde(default)]
    pub postings: f64,
    #[serde(default)]
    pub doc_store: f64,
    #[serde(default)]
    pub vectors: f64,
}

impl ClassValues {
    pub fn to_array(self) -> PerClass {
        [self.scoring, self.postings, self.doc_store, self.vectors]
    }

    pub fn from_array(v: PerClass) -> Self {
        Self {
            scoring: v[0],
            postings: v[1],
            doc_store: v[2],
            vectors: v[3],
        }
    }
}

/// The hot set a pod persists (`_warmset/<role>.json` in object storage)
/// and the next pod of that role warms at boot. Small by construction: top
/// namespaces, top segments each.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WarmSet {
    #[serde(default)]
    pub format: u32,
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub written_at_unix_ms: u64,
    #[serde(default)]
    pub half_life_secs: f64,
    /// Hottest first.
    #[serde(default)]
    pub namespaces: Vec<WarmSetNamespace>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WarmSetNamespace {
    pub namespace: String,
    /// Decayed searches per class when written.
    #[serde(default)]
    pub hits: ClassValues,
    /// Decayed bytes fetched per class when written.
    #[serde(default)]
    pub bytes: ClassValues,
    /// Hottest first.
    #[serde(default)]
    pub segments: Vec<WarmSetSegment>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WarmSetSegment {
    pub segment_id: String,
    #[serde(default)]
    pub hits: ClassValues,
}

/// Object path of a role's warm set, relative to the storage prefix.
pub fn warm_set_path(role: &str) -> String {
    let role: String = role
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("_warmset/{role}.json")
}

/// How much of the tracker goes into a persisted warm set.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WarmSetLimits {
    /// `KOSHA_WARMSET_TOP_NAMESPACES` (default 8).
    pub top_namespaces: usize,
    /// `KOSHA_WARMSET_TOP_SEGMENTS` per namespace (default 512).
    pub top_segments: usize,
    /// `KOSHA_WARMSET_MIN_HITS` (default 1.0): a namespace below this many
    /// decayed searches is not hot, so neither persisted nor re-warmed.
    pub min_score: f64,
}

impl Default for WarmSetLimits {
    fn default() -> Self {
        Self {
            top_namespaces: 8,
            top_segments: 512,
            min_score: 1.0,
        }
    }
}

// ─── Configuration ─────────────────────────────────────────────────────────

/// Warming configuration resolved once at boot.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WarmConfig {
    /// `KOSHA_WARMUP_BUDGET_BYTES`: ceiling on the bytes warming keeps
    /// local (the boot pass, then boot plus every re-warm). `None` (unset)
    /// is unlimited, which is the pre-#213 behavior.
    pub budget_bytes: Option<u64>,
    /// `KOSHA_WARMUP_USAGE` (default on): track reads, persist the warm
    /// set and warm it at boot. Off leaves only `KOSHA_WARMUP_NAMESPACES`.
    pub usage: bool,
    pub limits: WarmSetLimits,
    /// `KOSHA_WARMSET_PERSIST_SECS` (default 60, `0` disables).
    pub persist_interval: Option<Duration>,
    /// `KOSHA_REWARM_INTERVAL_MS` (default 5000, `0` disables): how often a
    /// query pod polls the manifest version of its hot namespaces.
    pub rewarm_interval: Option<Duration>,
    /// `KOSHA_ROLE` (default `query`): names the persisted warm set, so
    /// query and ingest pods keep their own.
    pub role: String,
}

impl Default for WarmConfig {
    fn default() -> Self {
        Self {
            budget_bytes: None,
            usage: true,
            limits: WarmSetLimits::default(),
            persist_interval: Some(Duration::from_secs(60)),
            rewarm_interval: Some(Duration::from_millis(5000)),
            role: "query".into(),
        }
    }
}

impl WarmConfig {
    pub fn from_env() -> Self {
        let d = Self::default();
        let limits = WarmSetLimits {
            top_namespaces: env_u64("KOSHA_WARMSET_TOP_NAMESPACES")
                .map(|n| n as usize)
                .unwrap_or(d.limits.top_namespaces),
            top_segments: env_u64("KOSHA_WARMSET_TOP_SEGMENTS")
                .map(|n| n as usize)
                .unwrap_or(d.limits.top_segments),
            min_score: std::env::var("KOSHA_WARMSET_MIN_HITS")
                .ok()
                .and_then(|v| v.trim().parse::<f64>().ok())
                .filter(|v| v.is_finite() && *v >= 0.0)
                .unwrap_or(d.limits.min_score),
        };
        Self {
            budget_bytes: parse_budget(std::env::var("KOSHA_WARMUP_BUDGET_BYTES").ok().as_deref()),
            usage: crate::env_flag_default_on(std::env::var("KOSHA_WARMUP_USAGE").ok().as_deref()),
            limits,
            persist_interval: interval(env_u64("KOSHA_WARMSET_PERSIST_SECS"), 60_000, 1000),
            rewarm_interval: interval(env_u64("KOSHA_REWARM_INTERVAL_MS"), 5000, 1),
            role: std::env::var("KOSHA_ROLE")
                .ok()
                .filter(|r| !r.trim().is_empty())
                .unwrap_or(d.role),
        }
    }
}

/// `KOSHA_WARMUP_BUDGET_BYTES`: unset, empty or unparsable → unlimited.
/// `0` is a real budget (warm nothing, report everything skipped).
/// Accepts a plain byte count or a `K`/`M`/`G`/`T` (binary) suffix.
pub fn parse_budget(raw: Option<&str>) -> Option<u64> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    let (digits, mult) = match raw.char_indices().last() {
        Some((i, c)) if c.is_ascii_alphabetic() => {
            let mult: u64 = match c.to_ascii_uppercase() {
                'K' => 1 << 10,
                'M' => 1 << 20,
                'G' => 1 << 30,
                'T' => 1 << 40,
                _ => return None,
            };
            (&raw[..i], mult)
        }
        _ => (raw, 1),
    };
    digits
        .trim()
        .parse::<u64>()
        .ok()
        .map(|n| n.saturating_mul(mult))
}

/// An interval variable in `unit_ms` units: unset → `default_ms`, `0` →
/// disabled.
fn interval(raw: Option<u64>, default_ms: u64, unit_ms: u64) -> Option<Duration> {
    match raw {
        None => Some(Duration::from_millis(default_ms)),
        Some(0) => None,
        Some(n) => Some(Duration::from_millis(n.saturating_mul(unit_ms))),
    }
}

// ─── Planning ──────────────────────────────────────────────────────────────

/// Where a candidate namespace came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// Named in `KOSHA_WARMUP_NAMESPACES`: always included, ahead of
    /// usage-derived namespaces.
    Explicit,
    /// From observed reads (the tracker or the persisted warm set).
    Usage,
}

/// One namespace offered to [`plan`].
pub struct Candidate {
    pub namespace: String,
    pub source: Source,
    /// Classes to warm for this namespace.
    pub classes: [bool; 4],
    /// Segments eligible to warm, in manifest order.
    pub segments: Vec<String>,
    /// Bytes per `(segment, class)` in object storage. A pair absent here
    /// (or 0) has nothing to warm: e.g. no vectors in that segment.
    pub sizes: HashMap<(String, FileClass), u64>,
    /// Observed read rates, for ordering. `None` when never read.
    pub rates: Option<NamespaceRates>,
}

/// One `(namespace, segment, class)` unit of warming.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PlanItem {
    pub namespace: String,
    pub segment: String,
    pub class: FileClass,
    pub bytes: u64,
    /// Decayed reads used for ordering.
    pub rate: f64,
    pub source: Source,
}

/// What [`plan`] decided.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WarmPlan {
    /// Items to warm, in warm order.
    pub warm: Vec<PlanItem>,
    /// Items past the budget, in the order they would have been warmed.
    pub skipped: Vec<PlanItem>,
}

/// Order every candidate item and cut the list at `budget` bytes.
///
/// Order:
/// 1. Scoring scaffolding before anything else: no other class is usable
///    without it, and posting-shard warming needs `inverted.idx` local to
///    tell which shards a segment has.
/// 2. Explicit (`KOSHA_WARMUP_NAMESPACES`) namespaces before usage ones.
/// 3. Descending observed read rate.
/// 4. Candidate order, manifest order and class order as tie-breaks, so
///    a pass with no observations warms exactly what the static warmup did.
///
/// The cut is clean: the first item that does not fit and every item after
/// it are skipped, so what was warmed is a prefix of the priority order —
/// no smaller, colder item jumps ahead of a hotter one that missed.
pub fn plan(candidates: &[Candidate], budget: Option<u64>) -> WarmPlan {
    /// An item with its sort key: tier (class, source), rate, then
    /// candidate / segment / class position.
    struct Keyed {
        tier: (u8, u8),
        rate: f64,
        position: (usize, usize, usize),
        item: PlanItem,
    }
    let mut keyed: Vec<Keyed> = Vec::new();
    for (ci, c) in candidates.iter().enumerate() {
        let rates = c.rates.as_ref().map(NamespaceRates::rate_index);
        for (si, seg) in c.segments.iter().enumerate() {
            for class in CLASSES {
                if !c.classes[class.index()] {
                    continue;
                }
                let Some(&bytes) = c.sizes.get(&(seg.clone(), class)) else {
                    continue;
                };
                if bytes == 0 {
                    continue;
                }
                let rate = rates.as_ref().map_or(0.0, |r| r.rate(seg, class));
                keyed.push(Keyed {
                    tier: (
                        u8::from(class != FileClass::Scoring),
                        u8::from(c.source != Source::Explicit),
                    ),
                    rate,
                    position: (ci, si, class.index()),
                    item: PlanItem {
                        namespace: c.namespace.clone(),
                        segment: seg.clone(),
                        class,
                        bytes,
                        rate,
                        source: c.source,
                    },
                });
            }
        }
    }
    keyed.sort_by(|a, b| {
        a.tier
            .cmp(&b.tier)
            .then_with(|| b.rate.total_cmp(&a.rate))
            .then_with(|| a.position.cmp(&b.position))
    });
    let mut out = WarmPlan::default();
    let mut used = 0u64;
    let mut over = false;
    for Keyed { item, .. } in keyed {
        if !over {
            match budget {
                Some(b) if used.saturating_add(item.bytes) > b => over = true,
                _ => {
                    used = used.saturating_add(item.bytes);
                    out.warm.push(item);
                    continue;
                }
            }
        }
        out.skipped.push(item);
    }
    out
}

// ─── Reporting ─────────────────────────────────────────────────────────────

/// Items listed per report section; the counts are always complete.
pub const REPORT_ITEMS: usize = 200;

/// Per-namespace totals of one pass.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct NamespaceSummary {
    pub namespace: String,
    pub source: Option<Source>,
    pub warmed_items: usize,
    pub warmed_bytes: u64,
    pub skipped_items: usize,
    pub skipped_bytes: u64,
    /// Files the pass actually downloaded (the rest were already local).
    pub fetched_files: u64,
    pub fetched_bytes: u64,
    /// Segment files still not local after the pass (fail-open: searches
    /// hydrate them live).
    pub missing: usize,
}

/// One warming pass, for the log and `GET /v1/admin/warmup-status`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct WarmReport {
    /// `boot` (gates `/readyz`) or `rewarm` (manifest change; never
    /// affects readiness).
    pub trigger: String,
    pub started_at_unix_ms: u64,
    pub duration_ms: u64,
    /// `None` is unlimited.
    pub budget_bytes: Option<u64>,
    /// Bytes this pass could still spend: the budget minus what earlier
    /// passes keep warm (re-warm) or the whole budget (boot).
    pub available_bytes: Option<u64>,
    pub warmed_items: usize,
    pub warmed_bytes: u64,
    pub skipped_items: usize,
    pub skipped_bytes: u64,
    pub fetched_files: u64,
    pub fetched_bytes: u64,
    pub namespaces: Vec<NamespaceSummary>,
    /// The first [`REPORT_ITEMS`] warmed items, in warm order.
    pub warmed: Vec<PlanItem>,
    /// The first [`REPORT_ITEMS`] skipped items, in priority order.
    pub skipped: Vec<PlanItem>,
    pub errors: Vec<String>,
}

impl WarmReport {
    pub fn new(trigger: &str, budget: Option<u64>, available: Option<u64>) -> Self {
        Self {
            trigger: trigger.to_string(),
            started_at_unix_ms: unix_ms(),
            budget_bytes: budget,
            available_bytes: available,
            ..Default::default()
        }
    }

    pub fn summary_mut(&mut self, namespace: &str) -> &mut NamespaceSummary {
        if let Some(i) = self
            .namespaces
            .iter()
            .position(|n| n.namespace == namespace)
        {
            return &mut self.namespaces[i];
        }
        self.namespaces.push(NamespaceSummary {
            namespace: namespace.to_string(),
            ..Default::default()
        });
        self.namespaces.last_mut().unwrap()
    }

    /// Account a plan's items (before execution).
    pub fn add_plan(&mut self, plan: &WarmPlan) {
        for item in &plan.warm {
            self.warmed_items += 1;
            self.warmed_bytes += item.bytes;
            let s = self.summary_mut(&item.namespace);
            s.source = Some(item.source);
            s.warmed_items += 1;
            s.warmed_bytes += item.bytes;
        }
        for item in &plan.skipped {
            self.skipped_items += 1;
            self.skipped_bytes += item.bytes;
            let s = self.summary_mut(&item.namespace);
            s.source = Some(item.source);
            s.skipped_items += 1;
            s.skipped_bytes += item.bytes;
        }
        self.warmed
            .extend(plan.warm.iter().take(REPORT_ITEMS).cloned());
        self.skipped
            .extend(plan.skipped.iter().take(REPORT_ITEMS).cloned());
    }

    /// One log line per namespace plus a total.
    pub fn log(&self) {
        let mb = |b: u64| b as f64 / (1024.0 * 1024.0);
        for n in &self.namespaces {
            println!(
                "warmup[{}]: '{}' ({}) warmed {} item(s) {:.1} MB (fetched {} file(s) {:.1} MB), \
                 skipped {} item(s) {:.1} MB over budget, {} file(s) still missing",
                self.trigger,
                n.namespace,
                match n.source {
                    Some(Source::Explicit) => "explicit",
                    Some(Source::Usage) => "usage",
                    None => "-",
                },
                n.warmed_items,
                mb(n.warmed_bytes),
                n.fetched_files,
                mb(n.fetched_bytes),
                n.skipped_items,
                mb(n.skipped_bytes),
                n.missing,
            );
        }
        for e in &self.errors {
            eprintln!("warn: warmup[{}]: {e}", self.trigger);
        }
        println!(
            "warmup[{}]: {} item(s) {:.1} MB warmed, {} item(s) {:.1} MB skipped (budget {}) in {:.1}s",
            self.trigger,
            self.warmed_items,
            mb(self.warmed_bytes),
            self.skipped_items,
            mb(self.skipped_bytes),
            match self.available_bytes {
                Some(b) => format!("{:.1} MB available", mb(b)),
                None => "unlimited".into(),
            },
            self.duration_ms as f64 / 1e3,
        );
    }
}

/// Bytes warming keeps local per `(namespace, segment, class)`, so a
/// re-warm spends only what the budget has left and a compaction's removed
/// inputs give their bytes back.
#[derive(Default)]
pub struct WarmLedger {
    items: Mutex<HashMap<(String, String, FileClass), u64>>,
}

impl WarmLedger {
    pub fn charge(&self, items: &[PlanItem]) {
        let mut map = self.items.lock().unwrap();
        for i in items {
            map.insert((i.namespace.clone(), i.segment.clone(), i.class), i.bytes);
        }
    }

    /// Drop `namespace`'s entries for segments not in `live`.
    pub fn retain_segments(&self, namespace: &str, live: &HashSet<String>) {
        self.items
            .lock()
            .unwrap()
            .retain(|(ns, seg, _), _| ns != namespace || live.contains(seg));
    }

    pub fn total(&self) -> u64 {
        self.items.lock().unwrap().values().sum()
    }

    pub fn len(&self) -> usize {
        self.items.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker() -> AccessTracker {
        AccessTracker::new(AccessConfig {
            half_life: Duration::from_secs(60),
            max_segments: 100,
        })
    }

    #[test]
    fn file_classes_cover_the_segment_files_warming_fetches() {
        assert_eq!(FileClass::of_file("inverted.idx"), Some(FileClass::Scoring));
        assert_eq!(
            FileClass::of_file("doc_store.offsets"),
            Some(FileClass::Scoring)
        );
        assert_eq!(
            FileClass::of_file("postings-0a.bin"),
            Some(FileClass::Postings)
        );
        assert_eq!(
            FileClass::of_file("doc_vectors.bin"),
            Some(FileClass::DocStore)
        );
        assert_eq!(
            FileClass::of_file("vector.offsets"),
            Some(FileClass::Vectors)
        );
        assert_eq!(FileClass::of_file("_COMMIT"), None);
        assert_eq!(FileClass::of_file("doc_store.bin.spans"), None);
    }

    #[test]
    fn reads_decay_with_the_half_life() {
        let t = tracker();
        t.record("a", &[("s1", FileClass::Scoring)], [100, 0, 0, 0]);
        let before = t.namespace("a").unwrap();
        assert!((before.hits.scoring - 1.0).abs() < 1e-3);
        assert!((before.bytes.scoring - 100.0).abs() < 0.1);
        t.advance(Duration::from_secs(60));
        let after = t.namespace("a").unwrap();
        assert!((after.hits.scoring - 0.5).abs() < 1e-3, "{after:?}");
        assert!((after.rate_index().rate("s1", FileClass::Scoring) - 0.5).abs() < 1e-3);
    }

    #[test]
    fn rescaling_keeps_values_across_many_half_lives() {
        let t = tracker();
        t.record("a", &[("s1", FileClass::Scoring)], [0; 4]);
        // Far past the rescale threshold: values must neither overflow nor
        // lose the newer read's weight.
        t.advance(Duration::from_secs(60 * 200));
        t.record("a", &[("s1", FileClass::Scoring)], [0; 4]);
        let a = t.namespace("a").unwrap();
        assert!(a.hits.scoring.is_finite());
        assert!((a.hits.scoring - 1.0).abs() < 1e-3, "{a:?}");
    }

    #[test]
    fn traffic_shift_reorders_hot_namespaces() {
        let t = tracker();
        for _ in 0..50 {
            t.record("a", &[("s", FileClass::Scoring)], [0; 4]);
        }
        t.advance(Duration::from_secs(60 * 10));
        for _ in 0..5 {
            t.record("b", &[("s", FileClass::Scoring)], [0; 4]);
        }
        let hot: Vec<String> = t
            .hot_namespaces(8, 1.0)
            .into_iter()
            .map(|n| n.namespace)
            .collect();
        assert_eq!(hot, vec!["b".to_string()], "a decayed below the floor");
    }

    #[test]
    fn memory_is_bounded_by_shedding_the_coldest_segments() {
        let t = tracker();
        for i in 0..150 {
            let seg = format!("s{i}");
            // Later segments are read more, so they are the hot ones.
            for _ in 0..=(i / 10) {
                t.record("a", &[(seg.as_str(), FileClass::Scoring)], [0; 4]);
            }
        }
        assert!(t.tracked_segments() <= 100, "{}", t.tracked_segments());
        let a = t.namespace("a").unwrap();
        assert!(a.segments.iter().any(|(s, _)| s == "s149"));
        assert!(!a.segments.iter().any(|(s, _)| s == "s0"));
    }

    #[test]
    fn retain_segments_forgets_compacted_inputs() {
        let t = tracker();
        t.record(
            "a",
            &[("old1", FileClass::Scoring), ("old2", FileClass::Scoring)],
            [0; 4],
        );
        let live: HashSet<String> = ["merged".to_string()].into();
        t.retain_segments("a", &live);
        assert_eq!(t.tracked_segments(), 0);
        let a = t.namespace("a").unwrap();
        // The namespace stays hot; a new segment inherits its rate.
        assert!(a.rate_index().rate("merged", FileClass::Scoring) >= 0.0);
        assert!(a.score() > 0.9);
    }

    #[test]
    fn unseen_segments_inherit_the_hottest_segment_rate() {
        let t = tracker();
        t.record("a", &[("s1", FileClass::DocStore)], [0; 4]);
        t.record("a", &[("s1", FileClass::DocStore)], [0; 4]);
        t.record("a", &[("s2", FileClass::DocStore)], [0; 4]);
        let a = t.namespace("a").unwrap();
        assert!((a.rate_index().rate("new", FileClass::DocStore) - 2.0).abs() < 1e-3);
        assert!((a.rate_index().rate("s2", FileClass::DocStore) - 1.0).abs() < 1e-3);
    }

    #[test]
    fn warm_set_round_trips_and_seeds_a_fresh_tracker() {
        let t = tracker();
        for _ in 0..10 {
            t.record(
                "b",
                &[("s1", FileClass::Scoring), ("s1", FileClass::Postings)],
                [10, 20, 0, 0],
            );
        }
        t.record("a", &[("s1", FileClass::Scoring)], [0; 4]);
        let set = t.warm_set("query", &WarmSetLimits::default());
        assert_eq!(set.namespaces[0].namespace, "b");
        let json = serde_json::to_string(&set).unwrap();
        let back: WarmSet = serde_json::from_str(&json).unwrap();
        // Floats may lose the last ulp through JSON; compare the shape.
        assert_eq!(back.role, "query");
        let names = |w: &WarmSet| -> Vec<(String, Vec<String>)> {
            w.namespaces
                .iter()
                .map(|n| {
                    let segs = n.segments.iter().map(|s| s.segment_id.clone()).collect();
                    (n.namespace.clone(), segs)
                })
                .collect()
        };
        assert_eq!(names(&back), names(&set));

        let fresh = tracker();
        fresh.seed(&back);
        let b = fresh.namespace("b").unwrap();
        assert!((b.hits.scoring - 10.0).abs() < 0.05, "{b:?}");
        assert!((b.rate_index().rate("s1", FileClass::Postings) - 10.0).abs() < 0.05);
    }

    #[test]
    fn older_or_newer_warm_set_documents_still_parse() {
        // Missing fields default; unknown fields are ignored.
        let set: WarmSet =
            serde_json::from_str(r#"{"namespaces":[{"namespace":"t/a"}],"extra":1}"#).unwrap();
        assert_eq!(set.namespaces[0].namespace, "t/a");
        assert!(set.namespaces[0].segments.is_empty());
    }

    #[test]
    fn warm_set_path_is_sanitized() {
        assert_eq!(warm_set_path("query"), "_warmset/query.json");
        assert_eq!(warm_set_path("../x"), "_warmset/___x.json");
    }

    #[test]
    fn budget_parsing() {
        assert_eq!(parse_budget(None), None);
        assert_eq!(parse_budget(Some("")), None);
        assert_eq!(parse_budget(Some("0")), Some(0));
        assert_eq!(parse_budget(Some("1048576")), Some(1 << 20));
        assert_eq!(parse_budget(Some("2G")), Some(2 << 30));
        assert_eq!(parse_budget(Some("512m")), Some(512 << 20));
        assert_eq!(parse_budget(Some("lots")), None);
    }

    fn candidate(ns: &str, source: Source, segs: &[(&str, [u64; 4])]) -> Candidate {
        let mut sizes = HashMap::new();
        for (seg, s) in segs {
            for class in CLASSES {
                sizes.insert((seg.to_string(), class), s[class.index()]);
            }
        }
        Candidate {
            namespace: ns.into(),
            source,
            classes: [true; 4],
            segments: segs.iter().map(|(s, _)| s.to_string()).collect(),
            sizes,
            rates: None,
        }
    }

    #[test]
    fn plan_without_budget_warms_everything_in_static_order() {
        let c = candidate(
            "a",
            Source::Explicit,
            &[("s1", [1, 2, 3, 0]), ("s2", [1, 2, 3, 4])],
        );
        let p = plan(&[c], None);
        assert!(p.skipped.is_empty());
        let order: Vec<(String, FileClass)> = p
            .warm
            .iter()
            .map(|i| (i.segment.clone(), i.class))
            .collect();
        assert_eq!(
            order,
            vec![
                ("s1".into(), FileClass::Scoring),
                ("s2".into(), FileClass::Scoring),
                ("s1".into(), FileClass::Postings),
                ("s1".into(), FileClass::DocStore),
                ("s2".into(), FileClass::Postings),
                ("s2".into(), FileClass::DocStore),
                ("s2".into(), FileClass::Vectors),
            ],
            "zero-byte classes (s1 has no vectors) are not items"
        );
    }

    #[test]
    fn plan_orders_by_read_rate_and_stops_cleanly_at_the_budget() {
        let t = tracker();
        for _ in 0..10 {
            t.record(
                "hot",
                &[("h1", FileClass::Scoring), ("h1", FileClass::Postings)],
                [0; 4],
            );
        }
        t.record(
            "cold",
            &[("c1", FileClass::Scoring), ("c1", FileClass::Postings)],
            [0; 4],
        );
        let mut hot = candidate("hot", Source::Usage, &[("h1", [10, 100, 0, 0])]);
        hot.rates = t.namespace("hot");
        let mut cold = candidate("cold", Source::Usage, &[("c1", [10, 100, 0, 0])]);
        cold.rates = t.namespace("cold");
        // Cold first in candidate order: the rate must still win.
        let p = plan(&[cold, hot], Some(130));
        let warmed: Vec<(String, FileClass)> = p
            .warm
            .iter()
            .map(|i| (i.namespace.clone(), i.class))
            .collect();
        assert_eq!(
            warmed,
            vec![
                ("hot".into(), FileClass::Scoring),
                ("cold".into(), FileClass::Scoring),
                ("hot".into(), FileClass::Postings),
            ]
        );
        assert_eq!(p.skipped.len(), 1);
        assert_eq!(p.skipped[0].namespace, "cold");
        assert_eq!(p.skipped[0].class, FileClass::Postings);
        let used: u64 = p.warm.iter().map(|i| i.bytes).sum();
        assert!(used <= 130);
    }

    #[test]
    fn plan_cut_is_a_prefix_even_when_a_smaller_item_would_fit() {
        let c = candidate(
            "a",
            Source::Explicit,
            &[("s1", [10, 1000, 0, 0]), ("s2", [10, 5, 0, 0])],
        );
        let p = plan(&[c], Some(100));
        assert_eq!(p.warm.len(), 2, "both scoring items");
        // s1's postings don't fit; s2's (5 bytes) would, but it comes after.
        assert_eq!(p.skipped.len(), 2);
        assert_eq!(p.skipped[0].segment, "s1");
    }

    #[test]
    fn explicit_namespaces_come_before_usage_ones() {
        let t = tracker();
        for _ in 0..100 {
            t.record("u", &[("s", FileClass::Scoring)], [0; 4]);
        }
        let mut u = candidate("u", Source::Usage, &[("s", [10, 0, 0, 0])]);
        u.rates = t.namespace("u");
        let e = candidate("e", Source::Explicit, &[("s", [10, 0, 0, 0])]);
        let p = plan(&[u, e], Some(10));
        assert_eq!(p.warm[0].namespace, "e");
        assert_eq!(p.skipped[0].namespace, "u");
    }

    #[test]
    fn zero_budget_warms_nothing_and_reports_everything_skipped() {
        let c = candidate("a", Source::Explicit, &[("s1", [1, 1, 1, 1])]);
        let p = plan(&[c], Some(0));
        assert!(p.warm.is_empty());
        assert_eq!(p.skipped.len(), 4);
        let mut r = WarmReport::new("boot", Some(0), Some(0));
        r.add_plan(&p);
        assert_eq!(r.skipped_items, 4);
        assert_eq!(r.namespaces[0].skipped_bytes, 4);
    }

    #[test]
    fn ledger_returns_bytes_of_compacted_segments() {
        let l = WarmLedger::default();
        let item = |seg: &str| PlanItem {
            namespace: "a".into(),
            segment: seg.into(),
            class: FileClass::Scoring,
            bytes: 10,
            rate: 0.0,
            source: Source::Usage,
        };
        l.charge(&[item("s1"), item("s2")]);
        assert_eq!(l.total(), 20);
        l.retain_segments("a", &["s2".to_string()].into());
        assert_eq!(l.total(), 10);
        assert_eq!(l.len(), 1);
    }
}
