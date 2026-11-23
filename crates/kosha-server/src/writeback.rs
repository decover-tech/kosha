//! Writeback control for the disk cache.
//!
//! Every cache fill is a buffered `write()` and a rename: nothing tells the
//! kernel to write the bytes to disk, so they stay dirty in memory until
//! dirty memory reaches the background threshold (about 3 GB on a query
//! pod). The cache disk is a gp3 volume that writes 125 MiB/s, slower than
//! S3 downloads (~400 MB/s measured), so dirty memory keeps growing toward
//! the dirty limit. Past the halfway point between the two the kernel makes
//! each thread that writes sleep in `balance_dirty_pages`, paced to the
//! disk, whatever that thread was doing for whom. Measured on staging
//! (run9, fresh pods): a boot warm-up left 4.4–5.1 GB dirty when the pod
//! went ready, which took another 41–52 s to drain, and the pod's IO
//! pressure was "full" (every task blocked on IO) a third of the time
//! during the drain.
//!
//! Two tools here keep that from happening:
//! - [`start`]: after a fill lands, start writing it out at once, so dirty
//!   memory drains at disk speed while the fills go on instead of piling up
//!   until the throttle point.
//! - [`sync_filesystem`]: wait until everything written so far is on disk;
//!   the boot warm-up calls it before the pod reports ready.

#![cfg_attr(not(feature = "s3"), allow(dead_code))]

use std::fs::File;
use std::path::Path;

/// Start writing `[offset, offset + len)` of `file` (just written) to disk,
/// without waiting for it. `len == 0` means to the end of the file.
///
/// Linux: `posix_fadvise(POSIX_FADV_DONTNEED)` on the range. The kernel
/// submits the range's dirty pages for writeback without waiting
/// (`WB_SYNC_NONE`), then drops only pages that are already clean, which
/// right after the write is none of them. This gets the effect of
/// `sync_file_range(SYNC_FILE_RANGE_WRITE)` without unsafe code (there is no
/// safe wrapper for that call, and this workspace forbids unsafe code).
/// PostgreSQL's `pg_flush_data` falls back to the same call. Two side
/// effects: the written pages go to the inactive LRU list, so they are the
/// first to go if memory is reclaimed (they are on disk by then), and the
/// kernel drains every CPU's LRU add-cache once per call, which costs a few
/// tens of microseconds.
///
/// Elsewhere a no-op. A failure is logged once per process and otherwise
/// ignored: the kernel's own writeback still writes the bytes, later.
pub fn start(file: &File, offset: u64, len: u64) {
    #[cfg(target_os = "linux")]
    {
        let len = std::num::NonZeroU64::new(len);
        if let Err(e) = rustix::fs::fadvise(file, offset, len, rustix::fs::Advice::DontNeed) {
            warn_once(&format!(
                "cannot start writeback of a cache file ({e}); dirty pages wait for the \
                 kernel's background writeback"
            ));
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (file, offset, len);
}

/// Wait until every dirty page of the filesystem holding `path` is on disk
/// (Linux `syncfs`). Elsewhere a no-op that returns `Ok`.
pub fn sync_filesystem(path: &Path) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let dir = File::open(path)?;
        rustix::fs::syncfs(&dir)?;
    }
    #[cfg(not(target_os = "linux"))]
    let _ = path;
    Ok(())
}

/// Dirty and under-writeback bytes of the whole node (`/proc/meminfo`
/// `Dirty` and `Writeback`). `None` where that file does not exist.
pub fn dirty_bytes() -> Option<(u64, u64)> {
    parse_meminfo(&std::fs::read_to_string("/proc/meminfo").ok()?)
}

fn parse_meminfo(text: &str) -> Option<(u64, u64)> {
    let field = |name: &str| -> Option<u64> {
        let line = text.lines().find(|l| l.starts_with(name))?;
        let kb: u64 = line[name.len()..]
            .trim()
            .trim_end_matches("kB")
            .trim()
            .parse()
            .ok()?;
        Some(kb * 1024)
    };
    Some((field("Dirty:")?, field("Writeback:")?))
}

#[cfg(target_os = "linux")]
fn warn_once(msg: &str) {
    use std::sync::atomic::{AtomicBool, Ordering};
    static WARNED: AtomicBool = AtomicBool::new(false);
    if !WARNED.swap(true, Ordering::Relaxed) {
        eprintln!("WARN: {msg}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meminfo_dirty_and_writeback() {
        let text = "MemTotal:       32000000 kB\nDirty:           5000000 kB\n\
                    Writeback:         10240 kB\nWritebackTmp:          0 kB\n";
        assert_eq!(parse_meminfo(text), Some((5_000_000 * 1024, 10_240 * 1024)));
        assert_eq!(parse_meminfo("MemTotal: 1 kB\n"), None);
    }

    #[test]
    fn start_and_sync_accept_a_fresh_file() {
        let dir = std::env::temp_dir().join(format!("kosha-writeback-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f");
        std::fs::write(&path, vec![7u8; 1 << 20]).unwrap();
        let file = File::open(&path).unwrap();
        start(&file, 0, 0);
        start(&file, 4096, 8192);
        sync_filesystem(&dir).unwrap();
        // The bytes are untouched.
        assert_eq!(std::fs::read(&path).unwrap(), vec![7u8; 1 << 20]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
