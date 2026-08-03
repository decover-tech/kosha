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

use std::collections::VecDeque;
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
    /// Cache keys ordered oldest-touched (front) to most-recently-touched
    /// (back). Touched on both read and write.
    recency: Mutex<VecDeque<String>>,
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
            recency: Mutex::new(recency),
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

    /// Evict least-recently-used files until `total_bytes` is within bound.
    fn enforce_limit(&self) {
        let Some(max) = self.max_bytes else {
            return;
        };
        loop {
            if self.total_bytes.load(Ordering::Relaxed) <= max {
                return;
            }
            let oldest = {
                let mut recency = self.recency.lock().unwrap();
                recency.pop_front()
            };
            let Some(key) = oldest else {
                // Nothing left to evict (single file bigger than the bound).
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
        } else if path.is_dir() {
            let len = dir_size(&path);
            std::fs::remove_dir_all(&path)?;
            self.total_bytes.fetch_sub(len, Ordering::Relaxed);
        }
        let mut recency = self.recency.lock().unwrap();
        if let Some(pos) = recency.iter().position(|k| k == key) {
            recency.remove(pos);
        }
        Ok(())
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
