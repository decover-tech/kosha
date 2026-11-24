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
//! Tools here:
//! - [`start`]: after a fill lands, start writing it out at once, so the
//!   disk is busy from the first byte instead of idling until dirty memory
//!   reaches the background threshold. It does not stop the kernel from
//!   throttling a writer once dirty memory is at the limit anyway (the disk
//!   is the bound); it removes the idle-disk window that let dirty memory
//!   get there.
//! - [`start_in_background`] / [`ChunkedWriteback`]: the same, handed to
//!   one background thread, for request-path writers: the advice call can
//!   itself sleep (bio submission on a congested queue, the remote LRU
//!   drain), and a request must not.
//! - [`sync_filesystem`]: wait until everything written so far is on disk;
//!   the boot warm-up calls it before the pod reports ready.

#![cfg_attr(not(feature = "s3"), allow(dead_code))]

use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Advice requests waiting for the background thread. Each holds an open
/// file descriptor until it runs; a full queue skips the advice (the
/// kernel's own writeback still writes the bytes, later).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const BACKGROUND_QUEUE: usize = 256;

/// Bytes a [`ChunkedWriteback`] writes between two advice requests: the
/// part size of an S3 download (`s3_storage::DOWNLOAD_PART_BYTES`).
#[cfg_attr(not(all(feature = "s3", feature = "postgres")), allow(dead_code))]
const CHUNK_BYTES: u64 = 16 << 20;

/// Advice requests skipped because the background queue was full.
static SKIPPED: AtomicU64 = AtomicU64::new(0);

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

/// [`start`] on the background writeback thread: never sleeps in the
/// kernel on the caller's thread. Takes ownership of `file` (a handle of
/// its own; the descriptor closes once the advice ran).
pub fn start_in_background(file: File, offset: u64, len: u64) {
    #[cfg(target_os = "linux")]
    {
        use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
        use std::sync::OnceLock;
        static TX: OnceLock<Option<SyncSender<(File, u64, u64)>>> = OnceLock::new();
        let tx = TX.get_or_init(|| {
            let (tx, rx) = sync_channel::<(File, u64, u64)>(BACKGROUND_QUEUE);
            let spawned = std::thread::Builder::new()
                .name("kosha-writeback".into())
                .spawn(move || {
                    while let Ok((file, offset, len)) = rx.recv() {
                        start(&file, offset, len);
                    }
                });
            match spawned {
                Ok(_) => Some(tx),
                Err(e) => {
                    eprintln!(
                        "WARN: cannot start the writeback thread ({e}); dirty cache pages wait \
                         for the kernel's background writeback"
                    );
                    None
                }
            }
        });
        let Some(tx) = tx else { return };
        let sent = tx.try_send((file, offset, len));
        if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) = sent {
            SKIPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (file, offset, len);
}

/// Advice requests skipped because the background queue was full.
pub fn skipped() -> u64 {
    SKIPPED.load(Ordering::Relaxed)
}

/// A writer into `file` that, every [`CHUNK_BYTES`] written, hands the
/// chunk just written to [`start_in_background`], so a long streamed copy
/// drains at disk speed as it goes and never advises pages written (and
/// possibly written back and clean) long before. Call
/// [`Self::finish`] after the last write for the tail.
#[cfg_attr(not(all(feature = "s3", feature = "postgres")), allow(dead_code))]
pub struct ChunkedWriteback<'a> {
    file: &'a File,
    at: u64,
    advised: u64,
}

#[cfg_attr(not(all(feature = "s3", feature = "postgres")), allow(dead_code))]
impl<'a> ChunkedWriteback<'a> {
    /// Writes go to `file` at its current position, which is `offset`.
    pub fn new(file: &'a File, offset: u64) -> Self {
        Self {
            file,
            at: offset,
            advised: offset,
        }
    }

    /// Advise the tail written since the last chunk.
    pub fn finish(mut self) {
        self.advise();
    }

    fn advise(&mut self) {
        if self.at == self.advised {
            return;
        }
        match self.file.try_clone() {
            Ok(file) => start_in_background(file, self.advised, self.at - self.advised),
            Err(e) => {
                #[cfg(target_os = "linux")]
                warn_once(&format!("cannot hand a cache file to writeback ({e})"));
                #[cfg(not(target_os = "linux"))]
                let _ = e;
            }
        }
        self.advised = self.at;
    }
}

impl Write for ChunkedWriteback<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = (&*self.file).write(buf)?;
        self.at += n as u64;
        if self.at - self.advised >= CHUNK_BYTES {
            self.advise();
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        (&*self.file).flush()
    }
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
        start_in_background(file.try_clone().unwrap(), 0, 4096);
        sync_filesystem(&dir).unwrap();
        // The bytes are untouched.
        assert_eq!(std::fs::read(&path).unwrap(), vec![7u8; 1 << 20]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The chunked writer writes every byte, in order, at the file's
    /// position, whatever the write sizes.
    #[test]
    fn chunked_writeback_writes_everything_in_order() {
        let dir = std::env::temp_dir().join(format!("kosha-writeback-c-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f");
        let data: Vec<u8> = (0..(CHUNK_BYTES as usize * 2 + 12_345))
            .map(|i| (i % 251) as u8)
            .collect();
        let file = File::create(&path).unwrap();
        let mut out =
            std::io::BufWriter::with_capacity(256 * 1024, ChunkedWriteback::new(&file, 0));
        std::io::copy(&mut &data[..], &mut out).unwrap();
        out.into_inner()
            .map_err(|e| e.into_error())
            .unwrap()
            .finish();
        drop(file);
        assert_eq!(std::fs::read(&path).unwrap(), data);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
