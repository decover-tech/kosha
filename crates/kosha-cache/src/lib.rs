//! SSD cache layer (DESIGN.md §9, implementation plan Epic 4): a read-through
//! cache over segment files, backed by local NVMe / pod disk.
//!
//! The cache is never authoritative — losing it is a performance event, not a
//! correctness event. Durable state lives in S3 (segments) + Postgres
//! (manifests).
//!
//! Phase 1: the cache root is typically the same tree as `KOSHA_DATA_DIR`
//! (segment directories written/read by the indexer and searcher). Keys are
//! relative paths like `{namespace}/{segment_id}/doc_store.bin`.

use std::collections::{HashMap, VecDeque};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::SystemTime;

use kosha_core::KoshaError;

/// Handle to a cached file. The file is present on local disk.
#[derive(Debug, Clone)]
pub struct CachedFile {
    /// The path to the cached copy.
    pub local_path: PathBuf,
}

/// Read-through cache for segment files.
///
/// Tracks total cached bytes with an atomic counter (rather than walking the
/// directory tree on demand — that walk used to be the dominant cost of
/// `GET /stats` on a large, long-lived cache) and, when `max_bytes` is set,
/// evicts whole files least-recently touched first once the total exceeds
/// the bound (DESIGN.md §9: "size-bounded per node ... LRU/ARC policy").
pub struct Cache {
    /// Root directory for cached files (usually `KOSHA_DATA_DIR`).
    cache_dir: PathBuf,
    /// Eviction bound in bytes. `None` means unbounded (legacy behavior).
    max_bytes: Option<u64>,
    total_bytes: AtomicU64,
    /// Bumped once per file/dir actually removed by `evict` — see
    /// `eviction_generation()`.
    eviction_generation: AtomicU64,
    /// Cache keys ordered oldest-touched (front) to most-recently-touched
    /// (back). Touched on both read and write.
    recency: Mutex<VecDeque<String>>,
    /// Ref-counted set of keys currently in use by an in-flight operation
    /// (e.g. a hydration batch still writing later files for the same
    /// segment, or a request actively reading one) — see `pin`/`unpin`.
    /// `enforce_limit` never evicts a pinned key, however stale: evicting a
    /// file something still needs doesn't free anything real (the owner
    /// just re-fetches it), and if an operation's own later writes evict
    /// its own earlier ones, the operation can spin without ever
    /// converging — see `pin`'s doc comment for the incident this closes.
    pinned: Mutex<HashMap<String, usize>>,
}

impl Cache {
    /// Unbounded cache (no automatic eviction) — same behavior as before
    /// eviction support was added.
    pub fn new(cache_dir: PathBuf) -> Self {
        Self::with_max_bytes(cache_dir, None)
    }

    /// `max_bytes`: when set, `put`/`put_bytes` evict least-recently-used
    /// whole files after each write so total cached size stays at or below
    /// this bound.
    pub fn with_max_bytes(cache_dir: PathBuf, max_bytes: Option<u64>) -> Self {
        std::fs::create_dir_all(&cache_dir).ok();
        let (total_bytes, recency) = Self::scan_existing(&cache_dir);
        let cache = Self {
            cache_dir,
            max_bytes,
            total_bytes: AtomicU64::new(total_bytes),
            eviction_generation: AtomicU64::new(0),
            recency: Mutex::new(recency),
            pinned: Mutex::new(HashMap::new()),
        };
        cache.enforce_limit();
        cache
    }

    /// Walk pre-existing cache contents once at startup (e.g. a warm NVMe
    /// hostPath surviving a pod restart), oldest-modified first, so LRU order
    /// and the running size total are correct from the very first request.
    fn scan_existing(cache_dir: &Path) -> (u64, VecDeque<String>) {
        fn walk(dir: &Path, root: &Path, out: &mut Vec<(String, SystemTime, u64)>) {
            let Ok(read) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in read.flatten() {
                let path = entry.path();
                let Ok(meta) = entry.metadata() else {
                    continue;
                };
                if meta.is_dir() {
                    walk(&path, root, out);
                } else if let Ok(rel) = path.strip_prefix(root) {
                    let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                    out.push((rel.to_string_lossy().replace('\\', "/"), mtime, meta.len()));
                }
            }
        }
        let mut entries = Vec::new();
        walk(cache_dir, cache_dir, &mut entries);
        entries.sort_by_key(|(_, mtime, _)| *mtime);
        let total = entries.iter().map(|(_, _, len)| len).sum();
        let order = entries.into_iter().map(|(key, _, _)| key).collect();
        (total, order)
    }

    pub fn root(&self) -> &Path {
        &self.cache_dir
    }

    /// Resolve a relative cache key to an absolute path under `cache_dir`.
    ///
    /// Rejects absolute paths and `..` components.
    pub fn path_for(&self, key: &str) -> Result<PathBuf, KoshaError> {
        let key = key.trim_start_matches('/');
        let mut out = self.cache_dir.clone();
        for comp in Path::new(key).components() {
            match comp {
                Component::Normal(part) => out.push(part),
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(KoshaError::NotFound(format!(
                        "invalid cache key (path traversal): {key}"
                    )));
                }
            }
        }
        Ok(out)
    }

    /// Move `key` to the most-recently-used end of the recency list.
    fn touch(&self, key: &str) {
        let mut recency = self.recency.lock().unwrap();
        if let Some(pos) = recency.iter().position(|k| k == key) {
            recency.remove(pos);
        }
        recency.push_back(key.to_string());
    }

    /// Mark `key` as in-use by an in-flight operation — `enforce_limit`
    /// skips pinned entries regardless of age. Ref-counted: nested or
    /// concurrent pins on the same key (e.g. two hydration batches that
    /// both touch a shared file) are safe, and the key only becomes
    /// evictable again once every `pin` has a matching `unpin`.
    ///
    /// Typical use: pin every file in a hydration batch before fetching
    /// any of them, unpin them all once the batch (not just one file) is
    /// done — so a later file's write in the same batch can never evict an
    /// earlier file's write from the same batch. Without this, a working
    /// set close to `max_bytes` could make hydration spin: each new file
    /// written evicts an already-hydrated one, so the segment never
    /// finishes converging (observed in production as an incomplete-
    /// segment count climbing — 37→128→193 — instead of shrinking).
    pub fn pin(&self, key: &str) {
        *self
            .pinned
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_insert(0) += 1;
    }

    /// Release one pin taken by `pin`. A mismatched call (unpinning a key
    /// that isn't pinned) is a silent no-op rather than a panic — cleanup/
    /// error paths must be able to call this unconditionally.
    pub fn unpin(&self, key: &str) {
        let mut pinned = self.pinned.lock().unwrap();
        if let Some(count) = pinned.get_mut(key) {
            *count -= 1;
            if *count == 0 {
                pinned.remove(key);
            }
        }
    }

    /// True if `key` currently has at least one outstanding pin.
    pub fn is_pinned(&self, key: &str) -> bool {
        self.pinned.lock().unwrap().contains_key(key)
    }

    /// Evict least-recently-used, *unpinned* files until `total_bytes` is
    /// within bound. Pinned entries (see `pin`) are never chosen as a
    /// victim, however stale; if every remaining entry is pinned, eviction
    /// stops even while still over budget — the same "nothing safe left to
    /// evict" fallback as a single file bigger than the whole bound.
    fn enforce_limit(&self) {
        let Some(max) = self.max_bytes else {
            return;
        };
        loop {
            if self.total_bytes.load(Ordering::Relaxed) <= max {
                return;
            }
            let victim = {
                let recency = self.recency.lock().unwrap();
                let pinned = self.pinned.lock().unwrap();
                recency
                    .iter()
                    .find(|key| !pinned.contains_key(key.as_str()))
                    .cloned()
            };
            let Some(key) = victim else {
                return;
            };
            let _ = self.evict(&key);
        }
    }

    /// Attempt to retrieve a file from cache.
    ///
    /// Returns `None` if the file is not cached (cache miss).
    pub fn get(&self, key: &str) -> Option<CachedFile> {
        let path = self.path_for(key).ok()?;
        if path.is_file() {
            self.touch(key);
            Some(CachedFile { local_path: path })
        } else {
            None
        }
    }

    /// True when a segment directory (or file) is already present locally.
    pub fn contains(&self, key: &str) -> bool {
        self.path_for(key).map(|p| p.exists()).unwrap_or(false)
    }

    /// Store a file in the cache by copying `source_path` under `key`.
    pub fn put(&self, key: &str, source_path: &Path) -> Result<CachedFile, KoshaError> {
        let dest = self.path_for(key)?;
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let old_len = dest.metadata().map(|m| m.len()).unwrap_or(0);
        std::fs::copy(source_path, &dest)?;
        let new_len = dest.metadata().map(|m| m.len()).unwrap_or(0);
        self.account(key, old_len, new_len);
        Ok(CachedFile { local_path: dest })
    }

    /// Store bytes directly under `key` (used when downloading from S3).
    pub fn put_bytes(&self, key: &str, data: &[u8]) -> Result<CachedFile, KoshaError> {
        let dest = self.path_for(key)?;
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let old_len = dest.metadata().map(|m| m.len()).unwrap_or(0);
        std::fs::write(&dest, data)?;
        self.account(key, old_len, data.len() as u64);
        Ok(CachedFile { local_path: dest })
    }

    /// Update the running size total and LRU order after a write, then
    /// evict if the write pushed the cache over `max_bytes`.
    fn account(&self, key: &str, old_len: u64, new_len: u64) {
        if new_len >= old_len {
            self.total_bytes
                .fetch_add(new_len - old_len, Ordering::Relaxed);
        } else {
            self.total_bytes
                .fetch_sub(old_len - new_len, Ordering::Relaxed);
        }
        self.touch(key);
        self.enforce_limit();
    }

    /// Record that `key` was already written to disk by something else that
    /// shares this cache's root directory (e.g. `S3Storage`'s own download
    /// path) — updates size accounting and LRU order without re-touching the
    /// file. Use this instead of `put`/`put_bytes` when the bytes are
    /// already on disk at the right path, to avoid a redundant copy.
    pub fn note_external_write(&self, key: &str) {
        let Ok(path) = self.path_for(key) else {
            return;
        };
        let new_len = path.metadata().map(|m| m.len()).unwrap_or(0);
        self.account(key, 0, new_len);
    }

    /// Remove a file from the cache.
    pub fn evict(&self, key: &str) -> Result<(), KoshaError> {
        let path = self.path_for(key)?;
        if path.is_file() {
            let len = path.metadata().map(|m| m.len()).unwrap_or(0);
            std::fs::remove_file(&path)?;
            self.total_bytes.fetch_sub(len, Ordering::Relaxed);
            self.eviction_generation.fetch_add(1, Ordering::Relaxed);
        } else if path.is_dir() {
            let len = dir_size(&path);
            std::fs::remove_dir_all(&path)?;
            self.total_bytes.fetch_sub(len, Ordering::Relaxed);
            self.eviction_generation.fetch_add(1, Ordering::Relaxed);
        }
        let mut recency = self.recency.lock().unwrap();
        if let Some(pos) = recency.iter().position(|k| k == key) {
            recency.remove(pos);
        }
        Ok(())
    }

    /// Monotonic counter bumped once per file/dir this cache has actually
    /// removed (LRU `enforce_limit` or an explicit `evict` call) — never on
    /// a no-op `evict` of an already-absent key. Callers that cache their
    /// own "is this file present on disk" verdicts across requests (e.g.
    /// the server's posting-blob presence cache) can snapshot this value
    /// alongside a verdict and treat any change as "something was removed
    /// since — don't trust stale presence verdicts, re-check." Global
    /// rather than per-key by design: the cost of a spurious wide
    /// invalidation (one extra real stat, on the next warm query) is far
    /// cheaper than the plumbing needed for a precise per-key signal, and
    /// eviction is rare relative to query volume, so the blast radius of
    /// "invalidate everything on any eviction" is small in practice.
    pub fn eviction_generation(&self) -> u64 {
        self.eviction_generation.load(Ordering::Relaxed)
    }

    /// Clear the entire cache.
    pub fn clear(&self) -> Result<(), KoshaError> {
        if self.cache_dir.exists() {
            std::fs::remove_dir_all(&self.cache_dir)?;
            std::fs::create_dir_all(&self.cache_dir)?;
        }
        self.total_bytes.store(0, Ordering::Relaxed);
        self.recency.lock().unwrap().clear();
        Ok(())
    }

    /// Return the total size of cached files in bytes. O(1) — tracked
    /// incrementally rather than walked on every call.
    pub fn total_size(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let meta = match entry.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            if meta.is_dir() {
                total += dir_size(&entry.path());
            } else {
                total += meta.len();
            }
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn cache_miss_returns_none() {
        let dir = std::env::temp_dir().join("kosha-test-cache-miss");
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone());
        assert!(cache.get("nonexistent-key").is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn cache_put_and_get_preserves_hierarchy() {
        let dir = std::env::temp_dir().join("kosha-test-cache-put");
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone());

        let src = dir.join("source.txt");
        fs::write(&src, b"hello world").unwrap();

        let cached = cache.put("my-ns/my-seg/doc_store.bin", &src).unwrap();
        assert!(cached.local_path.exists());
        assert!(cached.local_path.ends_with("doc_store.bin"));
        assert!(cached.local_path.to_string_lossy().contains("my-ns"));

        let retrieved = cache.get("my-ns/my-seg/doc_store.bin").unwrap();
        assert_eq!(
            fs::read_to_string(retrieved.local_path).unwrap(),
            "hello world"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_path_traversal() {
        let dir = std::env::temp_dir().join("kosha-test-cache-trav");
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone());
        assert!(cache.path_for("../etc/passwd").is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn cache_evict() {
        let dir = std::env::temp_dir().join("kosha-test-cache-evict");
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone());

        let src = dir.join("src.txt");
        fs::write(&src, b"data").unwrap();

        cache.put("key.bin", &src).unwrap();
        assert!(cache.get("key.bin").is_some());

        cache.evict("key.bin").unwrap();
        assert!(cache.get("key.bin").is_none());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn cache_clear() {
        let dir = std::env::temp_dir().join("kosha-test-cache-clear");
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone());

        let src = dir.join("src.txt");
        fs::write(&src, b"data").unwrap();

        cache.put("k1", &src).unwrap();
        cache.put("k2", &src).unwrap();
        assert!(cache.total_size() > 0);

        cache.clear().unwrap();
        assert_eq!(cache.total_size(), 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn total_size_is_incremental_not_a_directory_walk() {
        let dir = std::env::temp_dir().join("kosha-test-cache-incremental");
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone());

        cache.put_bytes("a/b.bin", &[0u8; 100]).unwrap();
        cache.put_bytes("a/c.bin", &[0u8; 50]).unwrap();
        assert_eq!(cache.total_size(), 150);

        cache.evict("a/b.bin").unwrap();
        assert_eq!(cache.total_size(), 50);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn eviction_bounds_total_size_lru_first() {
        let dir = std::env::temp_dir().join("kosha-test-cache-lru");
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::with_max_bytes(dir.clone(), Some(100));

        cache.put_bytes("first", &[0u8; 60]).unwrap();
        cache.put_bytes("second", &[0u8; 60]).unwrap();
        // Writing "second" pushed total to 120 > 100, so "first" (LRU) must
        // have been evicted to bring it back under the bound.
        assert!(cache.total_size() <= 100);
        assert!(cache.get("first").is_none());
        assert!(cache.get("second").is_some());

        let _ = fs::remove_dir_all(&dir);
    }

    /// `eviction_generation` must bump for an automatic LRU eviction
    /// (`enforce_limit`'s internal `evict` call), not just an explicit
    /// `Cache::evict` from outside — callers that snapshot this counter to
    /// invalidate their own presence caches (see kosha-server's
    /// `posting_blob_presence`) need to catch space-pressure evictions,
    /// which are exactly the LRU path, not the explicit one.
    #[test]
    fn eviction_generation_bumps_on_automatic_lru_eviction() {
        let dir = std::env::temp_dir().join("kosha-test-cache-eviction-gen-lru");
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::with_max_bytes(dir.clone(), Some(100));

        cache.put_bytes("first", &[0u8; 60]).unwrap();
        assert_eq!(cache.eviction_generation(), 0);

        // Pushes total to 120 > 100, forcing "first" out via enforce_limit.
        cache.put_bytes("second", &[0u8; 60]).unwrap();
        assert_eq!(
            cache.eviction_generation(),
            1,
            "the LRU eviction inside enforce_limit must bump the generation"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression for the BM25 benchmark re-run's bug #5
    /// (`scripts/bench/bm25_scale/RESULTS.md`): "disk-cache LRU evicts
    /// files that in-flight hydration still needs." "first" is oldest and
    /// would normally be the LRU victim (as in
    /// `eviction_bounds_total_size_lru_first`), but it's pinned here — the
    /// sweep must skip it and fall through to "second" (next-oldest,
    /// unpinned) instead of either evicting a pinned key or refusing to
    /// evict anything at all.
    #[test]
    fn pinned_entries_are_skipped_in_favor_of_the_next_oldest_unpinned_entry() {
        let dir = std::env::temp_dir().join("kosha-test-cache-pin");
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::with_max_bytes(dir.clone(), Some(200));

        cache.put_bytes("first", &[0u8; 60]).unwrap();
        cache.pin("first");
        cache.put_bytes("second", &[0u8; 60]).unwrap();
        cache.put_bytes("third", &[0u8; 60]).unwrap();
        // Total so far: 180, under 200 — no eviction yet.
        assert!(cache.get("first").is_some());
        assert!(cache.get("second").is_some());
        assert!(cache.get("third").is_some());

        // Pushes total to 240 > 200: "first" is oldest but pinned, so
        // "second" (next-oldest, unpinned) must be the one evicted instead.
        cache.put_bytes("fourth", &[0u8; 60]).unwrap();
        assert!(
            cache.get("first").is_some(),
            "a pinned entry must survive eviction even though it's the oldest"
        );
        assert!(
            cache.get("second").is_none(),
            "eviction must fall through to the next-oldest *unpinned* entry"
        );
        assert!(cache.get("third").is_some());
        assert!(cache.get("fourth").is_some());

        // Once unpinned, "first" becomes evictable again like any other
        // stale entry — pinning isn't a permanent exemption.
        cache.unpin("first");
        cache.put_bytes("fifth", &[0u8; 60]).unwrap();
        assert!(
            cache.get("first").is_none(),
            "unpinning must restore normal LRU eligibility"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// When *everything* remaining is pinned, eviction must give up rather
    /// than evict a pinned key — the same "nothing safe left to evict"
    /// fallback as a single file bigger than the whole bound.
    #[test]
    fn enforce_limit_stops_when_every_remaining_entry_is_pinned() {
        let dir = std::env::temp_dir().join("kosha-test-cache-pin-all");
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::with_max_bytes(dir.clone(), Some(100));

        // Pin both keys *before* writing them — `pin` doesn't require the
        // key to exist yet, and pinning after the write that would trigger
        // eviction is too late (enforce_limit runs synchronously inside
        // that same write, before a subsequent `pin` call could land).
        cache.pin("first");
        cache.pin("second");
        cache.put_bytes("first", &[0u8; 60]).unwrap();
        cache.put_bytes("second", &[0u8; 60]).unwrap();
        // Total is 120 > 100, but both entries are pinned — nothing evictable.
        assert!(cache.total_size() > 100);
        assert!(cache.get("first").is_some());
        assert!(cache.get("second").is_some());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn pin_is_ref_counted_and_unpin_is_safe_on_an_unpinned_key() {
        let dir = std::env::temp_dir().join("kosha-test-cache-pin-refcount");
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::with_max_bytes(dir.clone(), Some(100));

        cache.put_bytes("first", &[0u8; 60]).unwrap();
        cache.pin("first");
        cache.pin("first"); // two concurrent pins on the same key
        cache.unpin("first"); // only one released — still pinned
        assert!(cache.is_pinned("first"));

        cache.put_bytes("second", &[0u8; 60]).unwrap();
        assert!(
            cache.get("first").is_some(),
            "one remaining pin must still block eviction"
        );

        cache.unpin("first"); // second (matching) release
        assert!(!cache.is_pinned("first"));

        // Unpinning an already-unpinned (or never-pinned) key must not panic.
        cache.unpin("first");
        cache.unpin("never-pinned-key");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn scan_existing_accounts_for_pre_populated_cache_dir() {
        let dir = std::env::temp_dir().join("kosha-test-cache-prescan");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("ns/seg")).unwrap();
        fs::write(dir.join("ns/seg/doc_store.bin"), [0u8; 42]).unwrap();

        let cache = Cache::new(dir.clone());
        assert_eq!(cache.total_size(), 42);

        let _ = fs::remove_dir_all(&dir);
    }
}
