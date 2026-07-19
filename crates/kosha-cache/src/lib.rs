//! SSD cache layer (DESIGN.md §9, implementation plan Epic 4): a read-through,
//! write-behind cache over segment files, keyed by `(namespace_id,
//! segment_id, file, byte_range)` and backed by local NVMe storage.
//!
//! The cache is never authoritative — losing it is a performance event, not a
//! correctness event.
//!
//! Phase 1 is a no-op pass-through cache. Real NVMe-backed caching is Epic 4.

use std::path::{Path, PathBuf};

use kosha_core::KoshaError;

/// Handle to a cached file. The file is present on local disk.
#[derive(Debug, Clone)]
pub struct CachedFile {
    /// The path to the cached copy.
    pub local_path: PathBuf,
}

/// Read-through cache for segment files.
///
/// In Phase 1 this is a no-op: it always reports a cache miss, expecting the
/// caller to load data from the source directly.
pub struct Cache {
    /// Root directory for cached files.
    cache_dir: PathBuf,
}

impl Cache {
    pub fn new(cache_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&cache_dir).ok();
        Self { cache_dir }
    }

    /// Attempt to retrieve a file from cache.
    ///
    /// Returns `None` if the file is not cached (cache miss).
    pub fn get(&self, key: &str) -> Option<CachedFile> {
        let path = self.cache_dir.join(sanitize_key(key));
        if path.exists() {
            Some(CachedFile { local_path: path })
        } else {
            None
        }
    }

    /// Store a file in the cache.
    ///
    /// Copies the file at `source_path` into the cache under the given key.
    pub fn put(&self, key: &str, source_path: &Path) -> Result<CachedFile, KoshaError> {
        let dest = self.cache_dir.join(sanitize_key(key));
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(source_path, &dest)?;
        Ok(CachedFile { local_path: dest })
    }

    /// Remove a file from the cache.
    pub fn evict(&self, key: &str) -> Result<(), KoshaError> {
        let path = self.cache_dir.join(sanitize_key(key));
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Clear the entire cache.
    pub fn clear(&self) -> Result<(), KoshaError> {
        if self.cache_dir.exists() {
            std::fs::remove_dir_all(&self.cache_dir)?;
            std::fs::create_dir_all(&self.cache_dir)?;
        }
        Ok(())
    }

    /// Return the total size of cached files in bytes.
    pub fn total_size(&self) -> u64 {
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
        dir_size(&self.cache_dir)
    }
}

/// Replace path separators with underscores to avoid directory traversal.
fn sanitize_key(key: &str) -> String {
    key.replace(['/', '\\'], "_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn cache_miss_returns_none() {
        let dir = std::env::temp_dir().join("kosha-test-cache-miss");
        let cache = Cache::new(dir);
        assert!(cache.get("nonexistent-key").is_none());
    }

    #[test]
    fn cache_put_and_get() {
        let dir = std::env::temp_dir().join("kosha-test-cache-put");
        let cache = Cache::new(dir.clone());

        // Create a source file.
        let src = dir.join("source.txt");
        fs::write(&src, b"hello world").unwrap();

        let cached = cache.put("my-ns/my-seg/doc_store.bin", &src).unwrap();
        assert!(cached.local_path.exists());

        let retrieved = cache.get("my-ns/my-seg/doc_store.bin").unwrap();
        assert!(retrieved.local_path.exists());
        assert_eq!(
            fs::read_to_string(retrieved.local_path).unwrap(),
            "hello world"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn cache_evict() {
        let dir = std::env::temp_dir().join("kosha-test-cache-evict");
        let cache = Cache::new(dir.clone());

        let src = dir.join("src.txt");
        fs::write(&src, b"data").unwrap();

        cache.put("key", &src).unwrap();
        assert!(cache.get("key").is_some());

        cache.evict("key").unwrap();
        assert!(cache.get("key").is_none());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn cache_clear() {
        let dir = std::env::temp_dir().join("kosha-test-cache-clear");
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
}
