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

use std::path::{Component, Path, PathBuf};

use kosha_core::KoshaError;

/// Handle to a cached file. The file is present on local disk.
#[derive(Debug, Clone)]
pub struct CachedFile {
    /// The path to the cached copy.
    pub local_path: PathBuf,
}

/// Read-through cache for segment files.
pub struct Cache {
    /// Root directory for cached files (usually `KOSHA_DATA_DIR`).
    cache_dir: PathBuf,
}

impl Cache {
    pub fn new(cache_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&cache_dir).ok();
        Self { cache_dir }
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

    /// Attempt to retrieve a file from cache.
    ///
    /// Returns `None` if the file is not cached (cache miss).
    pub fn get(&self, key: &str) -> Option<CachedFile> {
        let path = self.path_for(key).ok()?;
        if path.is_file() {
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
        std::fs::copy(source_path, &dest)?;
        Ok(CachedFile { local_path: dest })
    }

    /// Store bytes directly under `key` (used when downloading from S3).
    pub fn put_bytes(&self, key: &str, data: &[u8]) -> Result<CachedFile, KoshaError> {
        let dest = self.path_for(key)?;
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&dest, data)?;
        Ok(CachedFile { local_path: dest })
    }

    /// Remove a file from the cache.
    pub fn evict(&self, key: &str) -> Result<(), KoshaError> {
        let path = self.path_for(key)?;
        if path.is_file() {
            std::fs::remove_file(&path)?;
        } else if path.is_dir() {
            std::fs::remove_dir_all(&path)?;
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
}
