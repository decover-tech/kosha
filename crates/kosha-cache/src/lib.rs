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
//!
//! Tenants (#342): a key's tenant is the first component of its namespace
//! (see [`kosha_core::tenancy`]). The cache keeps bytes and hit, miss and
//! eviction counts per tenant, and under pressure evicts from the tenant
//! furthest over its weighted share of `max_bytes` instead of strictly
//! least-recently-used across the whole pod — so one tenant streaming a
//! cold namespace larger than the cache cannot flush another tenant's warm
//! working set below its floor.

use std::collections::{HashMap, VecDeque};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::SystemTime;

use kosha_core::tenancy::{
    tenant_of_file_key, tenant_of_namespace, tenant_of_segment_dir, TenantCacheStats, TenantLedger,
    TenantShares,
};
use kosha_core::KoshaError;

/// Handle to a cached file. The file is present on local disk.
#[derive(Debug, Clone)]
pub struct CachedFile {
    /// The path to the cached copy.
    pub local_path: PathBuf,
}

/// Bytes the cache has accounted for, per key and per tenant.
#[derive(Default)]
struct Tracked {
    /// Size of every tracked key, as last accounted. The running total and
    /// the per-tenant totals move by exactly these values, so re-reporting
    /// a file that is already tracked replaces its size instead of counting
    /// it twice, and a file the cache never saw is never subtracted.
    sizes: HashMap<String, u64>,
    tenants: TenantLedger,
}

impl Tracked {
    fn set(&mut self, key: &str, len: u64) -> u64 {
        let tenant = tenant_of_file_key(key);
        let prev = self.sizes.insert(key.to_string(), len).unwrap_or(0);
        self.tenants.sub_bytes(tenant, prev);
        self.tenants.add_bytes(tenant, len);
        prev
    }

    fn forget(&mut self, key: &str) -> u64 {
        let len = self.sizes.remove(key).unwrap_or(0);
        self.tenants.sub_bytes(tenant_of_file_key(key), len);
        len
    }
}

/// Read-through cache for segment files.
///
/// Tracks total cached bytes with an atomic counter (rather than walking the
/// directory tree on demand — that walk used to be the dominant cost of
/// `GET /stats` on a large, long-lived cache) and, when `max_bytes` is set,
/// evicts whole files once the total exceeds the bound (DESIGN.md §9:
/// "size-bounded per node ... LRU/ARC policy"): least recently touched
/// first within a tenant, from the tenant furthest over its fair share
/// (#342, see [`TenantShares`]).
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
    /// Per-key sizes and per-tenant totals. Lock order: `recency`, then
    /// `pinned`, then `tracked`.
    tracked: Mutex<Tracked>,
    /// How `max_bytes` is split between tenants.
    shares: RwLock<TenantShares>,
}

impl Cache {
    /// Unbounded cache (no automatic eviction) — same behavior as before
    /// eviction support was added.
    pub fn new(cache_dir: PathBuf) -> Self {
        Self::with_max_bytes(cache_dir, None)
    }

    /// `max_bytes`: when set, `put`/`put_bytes` evict whole files after
    /// each write so total cached size stays at or below this bound.
    /// Tenant shares come from `KOSHA_CACHE_TENANT_MIN_BYTES`, with equal
    /// weights until [`Self::set_tenant_shares`] says otherwise.
    pub fn with_max_bytes(cache_dir: PathBuf, max_bytes: Option<u64>) -> Self {
        Self::with_tenant_shares(cache_dir, max_bytes, TenantShares::from_env())
    }

    /// Like [`Self::with_max_bytes`], with an explicit tenant policy.
    pub fn with_tenant_shares(
        cache_dir: PathBuf,
        max_bytes: Option<u64>,
        shares: TenantShares,
    ) -> Self {
        std::fs::create_dir_all(&cache_dir).ok();
        let (recency, sizes) = Self::scan_existing(&cache_dir);
        let mut tracked = Tracked::default();
        let mut total_bytes = 0u64;
        for (key, len) in sizes {
            total_bytes += len;
            tracked.set(&key, len);
        }
        let cache = Self {
            cache_dir,
            max_bytes,
            total_bytes: AtomicU64::new(total_bytes),
            eviction_generation: AtomicU64::new(0),
            recency: Mutex::new(recency),
            pinned: Mutex::new(HashMap::new()),
            tracked: Mutex::new(tracked),
            shares: RwLock::new(shares),
        };
        cache.enforce_limit();
        cache
    }

    /// Replace the tenant policy (weights and floor), e.g. once the
    /// server's tenant limits are loaded.
    pub fn set_tenant_shares(&self, shares: TenantShares) {
        *self.shares.write().unwrap_or_else(|e| e.into_inner()) = shares;
    }

    /// Walk pre-existing cache contents once at startup (e.g. a warm NVMe
    /// hostPath surviving a pod restart), oldest-modified first, so LRU order
    /// and the running size totals are correct from the very first request.
    fn scan_existing(cache_dir: &Path) -> (VecDeque<String>, Vec<(String, u64)>) {
        fn walk(dir: &Path, root: &Path, out: &mut Vec<(String, SystemTime, u64)>) {
            let Ok(read) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in read.flatten() {
                let path = entry.path();
                let Ok(meta) = entry.metadata() else {
                    continue;
                };
                // The WAL and a local WAL kept after moving it to S3
                // (`_wal.migrated-*`) are not cache content.
                if dir == root
                    && entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name == "_wal" || name.starts_with("_wal.migrated-"))
                {
                    continue;
                }
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
        let order = entries.iter().map(|(key, _, _)| key.clone()).collect();
        let sizes = entries
            .into_iter()
            .map(|(key, _, len)| (key, len))
            .collect();
        (order, sizes)
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

    /// The next eviction victim: the least recently used unpinned key of
    /// the tenant [`TenantShares::pick_victim`] chooses, or the least
    /// recently used unpinned key overall when no tenant qualifies.
    fn next_victim(&self, max: u64) -> Option<String> {
        let recency = self.recency.lock().unwrap();
        let pinned = self.pinned.lock().unwrap();
        let tracked = self.tracked.lock().unwrap();
        let usage = tracked.tenants.usage();
        // One pass: each active tenant's least recently used unpinned key.
        let mut oldest: HashMap<&str, &String> = HashMap::new();
        let mut first_unpinned: Option<&String> = None;
        for key in recency.iter() {
            if pinned.contains_key(key.as_str()) {
                continue;
            }
            first_unpinned.get_or_insert(key);
            let tenant = tenant_of_file_key(key);
            if usage.iter().any(|(t, _)| *t == tenant) {
                oldest.entry(tenant).or_insert(key);
                if oldest.len() == usage.len() {
                    break;
                }
            }
        }
        let shares = self.shares.read().unwrap_or_else(|e| e.into_inner());
        shares
            .pick_victim(max, &usage, |tenant| {
                oldest
                    .get(tenant)
                    .map(|key| tracked.sizes.get(key.as_str()).copied().unwrap_or(0))
            })
            .and_then(|tenant| oldest.get(tenant).copied())
            .or(first_unpinned)
            .cloned()
    }

    /// Evict unpinned files until `total_bytes` is within bound, choosing
    /// each victim with [`Self::next_victim`]. Pinned entries (see `pin`)
    /// are never chosen as a victim, however stale; if every remaining
    /// entry is pinned, eviction stops even while still over budget — the
    /// same "nothing safe left to evict" fallback as a single file bigger
    /// than the whole bound.
    fn enforce_limit(&self) {
        let Some(max) = self.max_bytes else {
            return;
        };
        let before = self.total_bytes.load(Ordering::Relaxed);
        let mut files = 0u32;
        loop {
            if self.total_bytes.load(Ordering::Relaxed) <= max {
                break;
            }
            let Some(key) = self.next_victim(max) else {
                break;
            };
            let _ = self.evict(&key);
            files += 1;
        }
        if files > 0 {
            let now = self.total_bytes.load(Ordering::Relaxed);
            println!(
                "disk cache evicted: files={files} freed_mb={:.1} size_mb={:.1} max_mb={:.1}",
                before.saturating_sub(now) as f64 / 1048576.0,
                now as f64 / 1048576.0,
                max as f64 / 1048576.0
            );
        }
    }

    /// Attempt to retrieve a file from cache.
    ///
    /// Returns `None` if the file is not cached (cache miss). Either way
    /// the lookup counts toward the key's tenant's hits or misses.
    pub fn get(&self, key: &str) -> Option<CachedFile> {
        let path = self.path_for(key).ok()?;
        if path.is_file() {
            self.touch(key);
            self.record_lookup(tenant_of_file_key(key), true);
            Some(CachedFile { local_path: path })
        } else {
            self.record_lookup(tenant_of_file_key(key), false);
            None
        }
    }

    /// Count one lookup for `tenant` that a caller answered itself (for
    /// example a search that found its segment files already local, or
    /// had to hydrate them), so per-tenant hit rates cover the paths that
    /// check the disk directly instead of calling [`Self::get`].
    pub fn record_lookup(&self, tenant: &str, hit: bool) {
        let mut tracked = self.tracked.lock().unwrap();
        if hit {
            tracked.tenants.hit(tenant);
        } else {
            tracked.tenants.miss(tenant);
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
        std::fs::copy(source_path, &dest)?;
        let new_len = dest.metadata().map(|m| m.len()).unwrap_or(0);
        self.account(key, new_len);
        Ok(CachedFile { local_path: dest })
    }

    /// Store bytes directly under `key` (used when downloading from S3).
    pub fn put_bytes(&self, key: &str, data: &[u8]) -> Result<CachedFile, KoshaError> {
        let dest = self.path_for(key)?;
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&dest, data)?;
        self.account(key, data.len() as u64);
        Ok(CachedFile { local_path: dest })
    }

    /// Record `key`'s new size in the running and per-tenant totals and the
    /// LRU order after a write, then evict if the write pushed the cache
    /// over `max_bytes`. The previous size is the one the cache tracked
    /// (zero for a key it never saw), so a rewrite or a repeated report of
    /// the same file never counts it twice.
    fn account(&self, key: &str, new_len: u64) {
        let old_len = self.tracked.lock().unwrap().set(key, new_len);
        if new_len >= old_len {
            self.total_bytes
                .fetch_add(new_len - old_len, Ordering::Relaxed);
        } else {
            self.sub_total(old_len - new_len);
        }
        self.touch(key);
        self.enforce_limit();
    }

    fn sub_total(&self, bytes: u64) {
        let mut total = self.total_bytes.load(Ordering::Relaxed);
        while let Err(actual) = self.total_bytes.compare_exchange_weak(
            total,
            total.saturating_sub(bytes),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            total = actual;
        }
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
        self.account(key, new_len);
    }

    /// Remove a file, or a whole directory such as a segment, from the
    /// cache. Returns the bytes removed.
    pub fn evict(&self, key: &str) -> Result<u64, KoshaError> {
        let path = self.path_for(key)?;
        let nested = format!("{}/", key.trim_end_matches('/'));
        let mut removed = 0;
        let mut tenant = None;
        if path.is_file() {
            removed = path.metadata().map(|m| m.len()).unwrap_or(0);
            std::fs::remove_file(&path)?;
            tenant = Some(tenant_of_file_key(key));
        } else if path.is_dir() {
            removed = dir_size(&path);
            std::fs::remove_dir_all(&path)?;
            tenant = Some(tenant_of_segment_dir(key));
        }
        // Only bytes the cache counted leave the totals: a directory can
        // hold files written by something that never reported them (an
        // ingest pod's own flushes), and subtracting those would drive the
        // total below real usage, so `KOSHA_CACHE_MAX_BYTES` stops binding.
        let mut recency = self.recency.lock().unwrap();
        let mut tracked = self.tracked.lock().unwrap();
        let mut freed = 0u64;
        recency.retain(|k| {
            if k == key || k.starts_with(&nested) {
                freed += tracked.forget(k);
                false
            } else {
                true
            }
        });
        if let Some(tenant) = tenant {
            tracked.tenants.evicted(tenant);
            self.eviction_generation.fetch_add(1, Ordering::Relaxed);
        }
        drop(tracked);
        drop(recency);
        self.sub_total(freed);
        Ok(removed)
    }

    /// Remove a whole directory (a superseded segment) and every file under
    /// it, unless any key at or under `key` is pinned. Returns
    /// `Ok(None)` when a pin blocked the eviction (nothing was removed), or
    /// `Ok(Some(bytes))` with the bytes actually freed on disk (`0` when the
    /// directory was already gone).
    ///
    /// Unlike `evict` on a directory key, this keeps the LRU list and the
    /// running total exact: every file key under the directory leaves the
    /// recency list, and the total drops only by the files the cache was
    /// tracking (files written straight to disk by the indexer were never
    /// counted, so subtracting them would wrap the counter).
    ///
    /// The recency and pin locks are held, in `enforce_limit`'s order,
    /// across the check and the removal, so a hydration batch cannot pin a
    /// file in between and then lose it.
    pub fn evict_dir_unless_pinned(&self, key: &str) -> Result<Option<u64>, KoshaError> {
        let path = self.path_for(key)?;
        let key = key
            .trim_start_matches('/')
            .trim_end_matches('/')
            .to_string();
        let prefix = format!("{key}/");
        let mut recency = self.recency.lock().unwrap();
        let pinned = self.pinned.lock().unwrap();
        if pinned.keys().any(|k| {
            k.trim_start_matches('/') == key || k.trim_start_matches('/').starts_with(&prefix)
        }) {
            return Ok(None);
        }
        if !path.is_dir() {
            return Ok(Some(0));
        }
        let mut files = Vec::new();
        collect_files(&path, &mut files);
        let on_disk: u64 = files.iter().map(|(_, len)| len).sum();
        std::fs::remove_dir_all(&path)?;
        let mut tracked = self.tracked.lock().unwrap();
        let mut freed = 0u64;
        for (file, _len) in &files {
            let Ok(rel) = file.strip_prefix(&self.cache_dir) else {
                continue;
            };
            let rel = rel.to_string_lossy().replace('\\', "/");
            if let Some(pos) = recency.iter().position(|k| *k == rel) {
                recency.remove(pos);
                freed += tracked.forget(&rel);
            }
        }
        tracked.tenants.evicted(tenant_of_segment_dir(&key));
        drop(tracked);
        drop(pinned);
        drop(recency);
        self.sub_total(freed);
        self.eviction_generation.fetch_add(1, Ordering::Relaxed);
        Ok(Some(on_disk))
    }

    /// Remove everything under the directory `prefix` (a deleted
    /// namespace's segments), tracked or not, and drop the tracked keys
    /// below it from the size accounting and LRU order. Only the bytes the
    /// cache had accounted for are subtracted, so files written straight to
    /// disk by the indexer cannot underflow the total.
    pub fn evict_prefix(&self, prefix: &str) -> Result<(), KoshaError> {
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty() {
            return Err(KoshaError::NotFound(
                "refusing to evict the cache root".into(),
            ));
        }
        let dir = self.path_for(prefix)?;
        let below = format!("{prefix}/");
        let mut freed = 0u64;
        {
            let mut recency = self.recency.lock().unwrap();
            let mut tracked = self.tracked.lock().unwrap();
            recency.retain(|key| {
                if !key.starts_with(&below) {
                    return true;
                }
                freed += tracked.forget(key);
                false
            });
            tracked.tenants.evicted(tenant_of_namespace(prefix));
        }
        if dir.is_dir() {
            std::fs::remove_dir_all(&dir)?;
        }
        self.sub_total(freed);
        self.eviction_generation.fetch_add(1, Ordering::Relaxed);
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
    ///
    /// Equal to the sum of every tenant's `evictions` in
    /// [`Self::tenant_stats`].
    pub fn eviction_generation(&self) -> u64 {
        self.eviction_generation.load(Ordering::Relaxed)
    }

    /// Clear the entire cache.
    pub fn clear(&self) -> Result<(), KoshaError> {
        if self.cache_dir.exists() {
            std::fs::remove_dir_all(&self.cache_dir)?;
            std::fs::create_dir_all(&self.cache_dir)?;
        }
        let mut recency = self.recency.lock().unwrap();
        let mut tracked = self.tracked.lock().unwrap();
        recency.clear();
        tracked.sizes.clear();
        tracked.tenants.clear_bytes();
        self.total_bytes.store(0, Ordering::Relaxed);
        Ok(())
    }

    /// Return the total size of cached files in bytes. O(1) — tracked
    /// incrementally rather than walked on every call.
    pub fn total_size(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Bytes `tenant` holds in the cache.
    pub fn tenant_bytes(&self, tenant: &str) -> u64 {
        self.tracked.lock().unwrap().tenants.bytes(tenant)
    }

    /// Bytes, hits, misses and evictions for every tenant seen, sorted by
    /// tenant id.
    pub fn tenant_stats(&self) -> Vec<TenantCacheStats> {
        self.tracked.lock().unwrap().tenants.stats()
    }
}

fn collect_files(dir: &Path, out: &mut Vec<(PathBuf, u64)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if meta.is_dir() {
            collect_files(&entry.path(), out);
        } else {
            out.push((entry.path(), meta.len()));
        }
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
    fn evict_prefix_removes_one_namespace_and_keeps_its_neighbours() {
        let dir = std::env::temp_dir().join(format!(
            "kosha-test-cache-evict-prefix-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone());
        cache.put_bytes("t/gone/seg-1/a.bin", &[0; 100]).unwrap();
        cache.put_bytes("t/gone2/seg-1/a.bin", &[0; 10]).unwrap();
        // Written behind the cache's back (the indexer does this).
        fs::create_dir_all(dir.join("t/gone/seg-2")).unwrap();
        fs::write(dir.join("t/gone/seg-2/b.bin"), [0; 1000]).unwrap();
        let generation = cache.eviction_generation();

        cache.evict_prefix("t/gone").unwrap();
        assert!(!dir.join("t/gone").exists());
        assert!(cache.contains("t/gone2/seg-1/a.bin"));
        assert_eq!(cache.total_size(), 10, "only tracked bytes are released");
        assert!(cache.eviction_generation() > generation);
        assert!(cache.evict_prefix("/").is_err());
        let _ = fs::remove_dir_all(&dir);
    }

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

        // A whole segment directory, including bytes the cache never saw.
        cache.put("ns/seg/a.bin", &src).unwrap();
        fs::write(dir.join("ns/seg/unreported.bin"), b"0123456789").unwrap();
        let before = cache.eviction_generation();
        assert_eq!(cache.evict("ns/seg").unwrap(), 14);
        assert!(!dir.join("ns/seg").exists());
        assert_eq!(cache.eviction_generation(), before + 1);
        assert_eq!(cache.total_size(), 0, "never underflows");
        // Untracked bytes in an evicted directory never offset tracked
        // bytes elsewhere: the total stays what the cache holds.
        cache.put_bytes("other/x.bin", &[0u8; 100]).unwrap();
        cache.put("ns/seg2/a.bin", &src).unwrap();
        fs::write(dir.join("ns/seg2/flushed.bin"), [0u8; 1000]).unwrap();
        assert_eq!(cache.total_size(), 104);
        assert_eq!(cache.evict("ns/seg2").unwrap(), 1004);
        assert_eq!(cache.total_size(), 100);
        assert!(cache
            .recency
            .lock()
            .unwrap()
            .iter()
            .all(|k| !k.starts_with("ns/seg")));

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

    /// #242: compaction evicts a superseded segment directory. A pin on any
    /// file under it must block the eviction; otherwise the directory goes,
    /// its file keys leave the LRU list, and the running total drops only
    /// by the bytes the cache was tracking (an untracked file written by the
    /// indexer must not wrap the counter).
    #[test]
    fn evict_dir_unless_pinned_respects_pins_and_keeps_accounting_exact() {
        let dir =
            std::env::temp_dir().join(format!("kosha-test-cache-evict-dir-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone());
        cache
            .put_bytes("ns/seg-a/doc_store.bin", &[0u8; 40])
            .unwrap();
        cache
            .put_bytes("ns/seg-a/inverted.idx", &[0u8; 10])
            .unwrap();
        cache
            .put_bytes("ns/seg-a2/doc_store.bin", &[0u8; 7])
            .unwrap();
        // Written straight to disk, never accounted (an indexer flush).
        fs::write(dir.join("ns/seg-a/filters.bin"), [0u8; 5]).unwrap();
        assert_eq!(cache.total_size(), 57);

        cache.pin("ns/seg-a/inverted.idx");
        assert_eq!(cache.evict_dir_unless_pinned("ns/seg-a").unwrap(), None);
        assert!(dir.join("ns/seg-a/doc_store.bin").exists(), "pinned: kept");
        cache.unpin("ns/seg-a/inverted.idx");

        let gen = cache.eviction_generation();
        assert_eq!(
            cache.evict_dir_unless_pinned("ns/seg-a").unwrap(),
            Some(55),
            "all bytes on disk, tracked or not"
        );
        assert!(!dir.join("ns/seg-a").exists());
        assert_eq!(cache.total_size(), 7, "only tracked bytes subtracted");
        assert!(cache.eviction_generation() > gen);
        assert!(!cache
            .recency
            .lock()
            .unwrap()
            .iter()
            .any(|k| k.starts_with("ns/seg-a/")));
        // A sibling whose name shares the prefix is untouched, and an
        // already-gone directory is a no-op.
        assert!(cache.get("ns/seg-a2/doc_store.bin").is_some());
        assert_eq!(cache.evict_dir_unless_pinned("ns/seg-a").unwrap(), Some(0));
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
    fn cache_eviction_never_removes_recovery_log_or_sequence() {
        let dir = std::env::temp_dir().join("kosha-test-cache-wal-safety");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("_wal")).unwrap();
        fs::write(dir.join("_wal/sequence"), b"100").unwrap();
        fs::write(dir.join("_wal/record.wal"), [1u8; 100]).unwrap();
        fs::create_dir_all(dir.join("_wal.migrated-1")).unwrap();
        fs::write(dir.join("_wal.migrated-1/record.wal"), [2u8; 100]).unwrap();
        let cache = Cache::with_max_bytes(dir.clone(), Some(1));
        cache.put_bytes("ns/seg/large.bin", &[0u8; 10]).unwrap();
        assert!(dir.join("_wal.migrated-1/record.wal").exists());
        assert_eq!(fs::read(dir.join("_wal/sequence")).unwrap(), b"100");
        assert_eq!(
            fs::read(dir.join("_wal/record.wal")).unwrap(),
            vec![1u8; 100]
        );
        fs::remove_dir_all(dir).unwrap();
    }

    /// Tenant B's repeat pass over its warm set: the fraction of lookups
    /// that hit.
    fn repeat_pass_hit_rate(cache: &Cache, keys: &[String]) -> f64 {
        let hits = keys.iter().filter(|k| cache.get(k).is_some()).count();
        hits as f64 / keys.len() as f64
    }

    /// #342: tenant A streaming a working set larger than the whole cache
    /// must not flush tenant B's warm set. Under plain LRU every B file is
    /// older than A's stream and would all be gone.
    #[test]
    fn a_tenant_streaming_past_the_cache_leaves_another_tenants_warm_set() {
        let dir = std::env::temp_dir().join(format!(
            "kosha-test-cache-tenant-iso-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let floor = 2_000;
        let cache = Cache::with_tenant_shares(dir.clone(), Some(10_000), TenantShares::new(floor));
        let warm: Vec<String> = (0..40)
            .map(|i| format!("beta/docs/seg-1/f{i}.bin"))
            .collect();
        for key in &warm {
            cache.put_bytes(key, &[0u8; 100]).unwrap();
        }
        // A cold namespace five times B's set, twice the whole cache.
        for i in 0..200 {
            cache
                .put_bytes(&format!("acme/big/seg-{i}/doc_store.bin"), &[0u8; 100])
                .unwrap();
        }
        assert!(cache.total_size() <= 10_000);
        assert!(
            cache.tenant_bytes("beta") >= floor,
            "B keeps at least its floor ({} bytes)",
            cache.tenant_bytes("beta")
        );
        // B was under its fair share (5,000), so it lost nothing at all.
        let rate = repeat_pass_hit_rate(&cache, &warm);
        assert!(rate >= 0.95, "B's warm hit rate stays within bound: {rate}");
        let stats = cache.tenant_stats();
        let acme = stats.iter().find(|s| s.tenant == "acme").unwrap();
        let beta = stats.iter().find(|s| s.tenant == "beta").unwrap();
        assert!(acme.evictions >= 100, "A paid for its own stream");
        assert_eq!(beta.evictions, 0);
        assert_eq!(beta.hits, 40);
        assert_eq!(
            stats.iter().map(|s| s.evictions).sum::<u64>(),
            cache.eviction_generation(),
            "per-tenant evictions add up to the generation"
        );
        assert_eq!(
            stats.iter().map(|s| s.bytes).sum::<u64>(),
            cache.total_size()
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// A low-weight tenant whose warm set is over its weighted share is
    /// trimmed to its floor, never below, while the other tenant streams.
    #[test]
    fn a_low_weight_tenant_keeps_its_floor() {
        let dir = std::env::temp_dir().join(format!(
            "kosha-test-cache-tenant-floor-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let floor = 2_000;
        let shares = TenantShares::new(floor).with_weights(Some(std::sync::Arc::new(|t: &str| {
            if t == "acme" {
                9
            } else {
                1
            }
        })));
        let cache = Cache::with_tenant_shares(dir.clone(), Some(10_000), shares);
        let warm: Vec<String> = (0..50)
            .map(|i| format!("beta/docs/seg-1/f{i}.bin"))
            .collect();
        for key in &warm {
            cache.put_bytes(key, &[0u8; 100]).unwrap();
        }
        for i in 0..300 {
            cache
                .put_bytes(&format!("acme/big/seg-{i}/doc_store.bin"), &[0u8; 100])
                .unwrap();
            assert!(cache.tenant_bytes("beta") >= floor);
        }
        assert!(cache.total_size() <= 10_000);
        assert_eq!(cache.tenant_bytes("beta"), floor, "trimmed to its floor");
        // The most recently used part of B's set survived.
        let rate = repeat_pass_hit_rate(&cache, &warm[30..]);
        assert_eq!(rate, 1.0);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A key reported twice (a cached file noted again after a hydration
    /// race) is counted once, for the total and for its tenant.
    #[test]
    fn re_reporting_a_tracked_file_does_not_double_count() {
        let dir =
            std::env::temp_dir().join(format!("kosha-test-cache-rereport-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let cache = Cache::new(dir.clone());
        cache.put_bytes("acme/docs/seg/a.bin", &[0u8; 30]).unwrap();
        cache.note_external_write("acme/docs/seg/a.bin");
        cache.note_external_write("acme/docs/seg/a.bin");
        assert_eq!(cache.total_size(), 30);
        assert_eq!(cache.tenant_bytes("acme"), 30);
        cache.put_bytes("acme/docs/seg/a.bin", &[0u8; 10]).unwrap();
        assert_eq!(cache.total_size(), 10);
        assert_eq!(cache.tenant_bytes("acme"), 10);
        cache.record_lookup("acme", false);
        assert_eq!(cache.tenant_stats()[0].misses, 1);
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
