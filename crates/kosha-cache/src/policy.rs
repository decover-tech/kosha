//! Scan-resistant eviction policies for the disk cache (#389, research).
//!
//! The default cache (`policy: None`) keeps the original behaviour: a
//! recency list touched on write, so with no read callers it evicts in
//! insertion order. This module adds two opt-in alternatives selected with
//! `KOSHA_CACHE_POLICY`:
//!
//! - **SIEVE** (Zhang et al., NSDI'24): one FIFO queue plus a "hand" that
//!   walks from oldest to newest. An entry read since the hand last passed
//!   survives (its visited bit is cleared, its position kept); an unread one
//!   is evicted.
//! - **S3-FIFO** (Yang et al., SOSP'23): a small FIFO (10% of the bytes)
//!   that one-touch entries leave quickly, a main FIFO with 2-bit
//!   frequency reinsertion, and a ghost queue of recently evicted keys so a
//!   re-read after a short eviction goes straight to main.
//!
//! # The read signal
//!
//! Searches do not read cache files through `Cache::get`; they open segment
//! files directly. So the server reports reads in bulk after a search, at
//! segment granularity, with [`ReadHint`]s:
//!
//! - `Class { dir, class }`: every tracked file of that class under the
//!   segment directory `dir` (e.g. the scoring set of every segment).
//! - `File { dir, file }`: one segment-level file and anything nested under
//!   it (its span-cache directory), e.g. the one posting shard a term
//!   hashes to.
//!
//! A hint is O(1): it bumps a counter on the segment's group, not on each
//! file. Each entry remembers the counter values it has already consumed,
//! so the eviction scan computes "reads since I last looked" lazily for
//! just the entries it visits. Reads are stamped with a logical clock taken
//! when the search *started* (`Cache::begin_read`); a file inserted by that
//! same search (hydrated because it was missing) does not count that search
//! as a re-read. This is approximate under concurrency: a hint whose clock
//! is older than an entry's insertion only counts as zero when *every*
//! read since the entry's last sync is that old (see `Entry::take_reads`).
//!
//! Keys follow the crate convention `{namespace}/{segment_id}/{file}[/...]`:
//! the first two components are the group, the third is the segment-level
//! file. Keys with fewer components group by their parent.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

/// Class id for files the classifier does not know (or no classifier):
/// any read hint on their segment counts as a read of them.
pub const CLASS_ANY: u8 = u8::MAX;

/// Suffix of a segment file's span-cache directory (mirrors
/// `kosha_segment::span_cache::SPANS_SUFFIX`; this crate does not depend on
/// `kosha-segment`).
const SPANS_SUFFIX: &str = ".spans";

/// Maps a segment-level file name (the third key component with any
/// `.spans` suffix removed, e.g. `postings-03.bin`) to a small class id.
pub type Classifier = fn(&str) -> u8;

/// Opt-in eviction policy (`KOSHA_CACHE_POLICY`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachePolicy {
    Sieve,
    S3Fifo,
}

impl CachePolicy {
    /// Parse a `KOSHA_CACHE_POLICY` value. `None` (unset, empty, `legacy`,
    /// `fifo`) keeps today's behaviour.
    pub fn parse(value: Option<&str>) -> Result<Option<Self>, String> {
        match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") | Some("legacy") | Some("fifo") | Some("lru") => Ok(None),
            Some("sieve") => Ok(Some(Self::Sieve)),
            Some("s3fifo") | Some("s3-fifo") => Ok(Some(Self::S3Fifo)),
            Some(other) => Err(format!(
                "unknown KOSHA_CACHE_POLICY={other:?} (expected legacy, sieve or s3fifo)"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sieve => "sieve",
            Self::S3Fifo => "s3fifo",
        }
    }
}

/// One read reported by a search (see the module docs).
#[derive(Debug, Clone, Copy)]
pub enum ReadHint<'a> {
    Class { dir: &'a str, class: u8 },
    File { dir: &'a str, file: &'a str },
}

#[derive(Debug, Default, Clone, Copy)]
struct Counter {
    count: u64,
    /// Largest read clock seen.
    last: u64,
}

impl Counter {
    fn bump(&mut self, at: u64) {
        self.count += 1;
        self.last = self.last.max(at);
    }
}

#[derive(Debug, Default)]
struct Group {
    /// Tracked entries in this group; the group is dropped at zero, so read
    /// hints for segments with nothing cached allocate nothing.
    files: usize,
    classes: Vec<(u8, Counter)>,
    by_file: HashMap<String, Counter>,
}

impl Group {
    fn class(&self, class: u8) -> Counter {
        self.classes
            .iter()
            .find(|(c, _)| *c == class)
            .map(|(_, counter)| *counter)
            .unwrap_or_default()
    }

    fn bump_class(&mut self, class: u8, at: u64) {
        match self.classes.iter_mut().find(|(c, _)| *c == class) {
            Some((_, counter)) => counter.bump(at),
            None => {
                let mut counter = Counter::default();
                counter.bump(at);
                self.classes.push((class, counter));
            }
        }
    }

    /// Combined counter for an entry: its class (or every hint, for
    /// `CLASS_ANY`) plus its own segment-level file.
    fn reads_for(&self, class: u8, file: &str) -> Counter {
        let c = self.class(class);
        let f = self.by_file.get(file).copied().unwrap_or_default();
        Counter {
            count: c.count + f.count,
            last: c.last.max(f.last),
        }
    }
}

#[derive(Debug)]
struct Entry {
    size: u64,
    group: String,
    file: String,
    class: u8,
    /// Read clock at insertion: reads stamped at or before it began before
    /// this entry existed.
    inserted_at: u64,
    /// Group counter value already consumed.
    seen: u64,
    /// Direct reads through `Cache::get`.
    own: u64,
    /// S3-FIFO frequency (0..=3); SIEVE uses `freq > 0` as the visited bit.
    freq: u8,
    /// SIEVE position / S3-FIFO queue token.
    seq: u64,
    in_main: bool,
}

impl Entry {
    /// Reads since the last call, folded into `freq` (capped at 3).
    fn take_reads(&mut self, groups: &HashMap<String, Group>) -> u64 {
        let counter = groups
            .get(&self.group)
            .map(|g| g.reads_for(self.class, &self.file))
            .unwrap_or_default();
        let fresh = counter.count.saturating_sub(self.seen);
        self.seen = counter.count;
        let mut reads = std::mem::take(&mut self.own);
        if fresh > 0 && counter.last > self.inserted_at {
            reads += fresh;
        }
        self.freq = (self.freq as u64 + reads).min(3) as u8;
        reads
    }
}

fn split_key(key: &str) -> (String, String) {
    let mut parts = key.splitn(4, '/');
    let a = parts.next().unwrap_or("");
    match (parts.next(), parts.next()) {
        (Some(b), Some(c)) => (format!("{a}/{b}"), c.to_string()),
        (Some(b), None) => (a.to_string(), b.to_string()),
        _ => (String::new(), a.to_string()),
    }
}

/// Policy bookkeeping. Owned by `Cache` behind one mutex; never touches the
/// filesystem.
#[derive(Debug)]
pub(crate) struct PolicyState {
    kind: CachePolicy,
    classify: Option<Classifier>,
    max_bytes: u64,
    entries: HashMap<String, Entry>,
    groups: HashMap<String, Group>,
    next_seq: u64,
    // SIEVE: insertion order, oldest first; the hand walks toward newer.
    order: BTreeMap<u64, String>,
    hand: Option<u64>,
    // S3-FIFO: (key, seq) tokens, front = oldest. Stale tokens (entry gone
    // or re-queued under a new seq) are skipped when popped.
    small: VecDeque<(String, u64)>,
    main: VecDeque<(String, u64)>,
    small_bytes: u64,
    main_bytes: u64,
    main_count: usize,
    ghost: VecDeque<(String, u64)>,
    ghost_set: HashMap<String, u64>,
}

impl PolicyState {
    pub(crate) fn new(kind: CachePolicy, max_bytes: u64, classify: Option<Classifier>) -> Self {
        Self {
            kind,
            classify,
            max_bytes,
            entries: HashMap::new(),
            groups: HashMap::new(),
            next_seq: 0,
            order: BTreeMap::new(),
            hand: None,
            small: VecDeque::new(),
            main: VecDeque::new(),
            small_bytes: 0,
            main_bytes: 0,
            main_count: 0,
            ghost: VecDeque::new(),
            ghost_set: HashMap::new(),
        }
    }

    pub(crate) fn kind(&self) -> CachePolicy {
        self.kind
    }

    /// Size this policy last recorded for `key`, if tracked.
    pub(crate) fn size_of(&self, key: &str) -> Option<u64> {
        self.entries.get(key).map(|e| e.size)
    }

    /// Track a write. A brand-new key enters SIEVE's queue / S3-FIFO's small
    /// queue (main if it is in the ghost queue, or `warm` for entries found
    /// on disk at startup). An existing key only has its size updated.
    pub(crate) fn upsert(&mut self, key: &str, size: u64, clock: u64, warm: bool) {
        if let Some(entry) = self.entries.get_mut(key) {
            let old = entry.size;
            entry.size = size;
            if self.kind == CachePolicy::S3Fifo {
                let bytes = if entry.in_main {
                    &mut self.main_bytes
                } else {
                    &mut self.small_bytes
                };
                *bytes = bytes.saturating_sub(old) + size;
            }
            return;
        }
        let (group, file) = split_key(key);
        // A span-cache directory (`postings-03.bin.spans/...`) belongs to
        // the segment file it caches ranges of.
        let file = match file.strip_suffix(SPANS_SUFFIX) {
            Some(base) => base.to_string(),
            None => file,
        };
        let class = self.classify.map(|f| f(&file)).unwrap_or(CLASS_ANY);
        let g = self.groups.entry(group.clone()).or_default();
        g.files += 1;
        let seen = g.reads_for(class, &file).count;
        let seq = self.next_seq;
        self.next_seq += 1;
        let mut entry = Entry {
            size,
            group,
            file,
            class,
            inserted_at: clock,
            seen,
            own: 0,
            freq: 0,
            seq,
            in_main: false,
        };
        match self.kind {
            CachePolicy::Sieve => {
                self.order.insert(seq, key.to_string());
            }
            CachePolicy::S3Fifo => {
                let to_main = warm || self.ghost_set.remove(key).is_some();
                entry.in_main = to_main;
                if to_main {
                    self.main.push_back((key.to_string(), seq));
                    self.main_bytes += size;
                    self.main_count += 1;
                } else {
                    self.small.push_back((key.to_string(), seq));
                    self.small_bytes += size;
                }
            }
        }
        self.entries.insert(key.to_string(), entry);
    }

    /// A direct read of one key (`Cache::get`).
    pub(crate) fn note_key_read(&mut self, key: &str) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.own += 1;
        }
    }

    /// Bulk reads from one search, stamped with the clock it started at.
    pub(crate) fn note_reads(&mut self, at: u64, hints: &[ReadHint<'_>]) {
        for hint in hints {
            let dir = match hint {
                ReadHint::Class { dir, .. } | ReadHint::File { dir, .. } => dir,
            };
            let Some(group) = self.groups.get_mut(dir.trim_matches('/')) else {
                continue;
            };
            match hint {
                ReadHint::Class { class, .. } => {
                    group.bump_class(*class, at);
                    if *class != CLASS_ANY {
                        group.bump_class(CLASS_ANY, at);
                    }
                }
                ReadHint::File { file, .. } => {
                    group.by_file.entry(file.to_string()).or_default().bump(at);
                    group.bump_class(CLASS_ANY, at);
                }
            }
        }
    }

    /// Drop every tracked key `remove` selects (external eviction, clear).
    pub(crate) fn forget(&mut self, remove: impl Fn(&str) -> bool) {
        let keys: Vec<String> = self.entries.keys().filter(|k| remove(k)).cloned().collect();
        for key in keys {
            self.remove_entry(&key);
        }
    }

    fn remove_entry(&mut self, key: &str) -> Option<Entry> {
        let entry = self.entries.remove(key)?;
        if let Some(g) = self.groups.get_mut(&entry.group) {
            g.files -= 1;
            if g.files == 0 {
                self.groups.remove(&entry.group);
            }
        }
        match self.kind {
            CachePolicy::Sieve => {
                self.order.remove(&entry.seq);
            }
            CachePolicy::S3Fifo => {
                if entry.in_main {
                    self.main_bytes = self.main_bytes.saturating_sub(entry.size);
                    self.main_count = self.main_count.saturating_sub(1);
                } else {
                    self.small_bytes = self.small_bytes.saturating_sub(entry.size);
                }
            }
        }
        Some(entry)
    }

    /// Choose and untrack the next victim, skipping pinned keys and
    /// `protect` (the key whose write triggered this eviction: both papers
    /// evict *before* inserting, so a new object is never its own victim).
    /// `None` when nothing evictable is left (everything held, or empty).
    pub(crate) fn pick_victim(
        &mut self,
        pinned: &HashMap<String, usize>,
        protect: Option<&str>,
    ) -> Option<String> {
        let held = |key: &str| pinned.contains_key(key) || protect == Some(key);
        match self.kind {
            CachePolicy::Sieve => self.pick_sieve(held),
            CachePolicy::S3Fifo => self.pick_s3fifo(held),
        }
    }

    fn pick_sieve(&mut self, held: impl Fn(&str) -> bool) -> Option<String> {
        let first = |order: &BTreeMap<u64, String>| order.keys().next().copied();
        let mut pos = match self.hand {
            Some(h) => self
                .order
                .range(h..)
                .next()
                .map(|(k, _)| *k)
                .or_else(|| first(&self.order)),
            None => first(&self.order),
        }?;
        // Two full sweeps clear every visited bit, so a third finds a victim
        // unless everything is pinned.
        let mut steps = self.order.len() * 2 + 1;
        loop {
            if steps == 0 {
                self.hand = Some(pos);
                return None;
            }
            steps -= 1;
            let key = self.order.get(&pos)?.clone();
            let evict = !held(&key) && {
                let entry = self.entries.get_mut(&key)?;
                entry.take_reads(&self.groups);
                if entry.freq > 0 {
                    entry.freq = 0;
                    false
                } else {
                    true
                }
            };
            let next = self
                .order
                .range(pos + 1..)
                .next()
                .map(|(k, _)| *k)
                .or_else(|| first(&self.order));
            if evict {
                self.remove_entry(&key);
                self.hand = next.filter(|n| *n != pos);
                return Some(key);
            }
            pos = next?;
        }
    }

    fn pick_s3fifo(&mut self, held: impl Fn(&str) -> bool) -> Option<String> {
        let small_target = self.max_bytes / 10;
        let mut steps = (self.small.len() + self.main.len()) * 4 + 4;
        // Held (pinned / just-written) entries seen per queue: once a whole
        // queue is held, take from the other one instead of cycling.
        let (mut small_held, mut main_held) = (0usize, 0usize);
        loop {
            if steps == 0 {
                return None;
            }
            steps -= 1;
            let small_ok = small_held < self.small.len();
            let main_ok = main_held < self.main.len();
            let from_small = small_ok && (self.small_bytes >= small_target || !main_ok);
            if !from_small && !main_ok {
                return None;
            }
            let queue = if from_small {
                &mut self.small
            } else {
                &mut self.main
            };
            let (key, seq) = queue.pop_front()?;
            let Some(entry) = self.entries.get_mut(&key) else {
                continue;
            };
            if entry.seq != seq || entry.in_main == from_small {
                continue; // stale token
            }
            if held(&key) {
                queue.push_back((key, seq));
                if from_small {
                    small_held += 1;
                } else {
                    main_held += 1;
                }
                continue;
            }
            entry.take_reads(&self.groups);
            if from_small {
                if entry.freq > 1 {
                    // Promote: re-read at least twice while in the small
                    // queue.
                    entry.in_main = true;
                    let size = entry.size;
                    self.small_bytes = self.small_bytes.saturating_sub(size);
                    self.main_bytes += size;
                    self.main_count += 1;
                    self.main.push_back((key, seq));
                    continue;
                }
                self.remove_entry(&key);
                self.push_ghost(&key);
                return Some(key);
            }
            if entry.freq > 0 {
                entry.freq -= 1;
                self.main.push_back((key, seq));
                continue;
            }
            self.remove_entry(&key);
            return Some(key);
        }
    }

    fn push_ghost(&mut self, key: &str) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.ghost_set.insert(key.to_string(), seq);
        self.ghost.push_back((key.to_string(), seq));
        // Same number of ghost entries as main entries (paper §4.1), with a
        // floor so a nearly empty main still remembers some evictions.
        let cap = self.main_count.max(64);
        while self.ghost_set.len() > cap {
            let Some((old, old_seq)) = self.ghost.pop_front() else {
                break;
            };
            if self.ghost_set.get(&old) == Some(&old_seq) {
                self.ghost_set.remove(&old);
            }
        }
        // Bound stale tokens too.
        if self.ghost.len() > cap * 4 {
            let live: HashSet<(String, u64)> = self
                .ghost_set
                .iter()
                .map(|(k, s)| (k.clone(), *s))
                .collect();
            self.ghost.retain(|t| live.contains(t));
        }
    }

    #[cfg(test)]
    pub(crate) fn queue_bytes(&self) -> (u64, u64) {
        (self.small_bytes, self.main_bytes)
    }
}
