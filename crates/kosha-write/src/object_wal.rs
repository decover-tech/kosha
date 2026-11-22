//! WAL on an object store (#236): acknowledged writes survive the loss of
//! the ingest node's disk.
//!
//! One writer stream lives under `_wal/{writer_id}/`:
//! - `{key:020}.bin`: one group commit, the same v3 frames the local WAL
//!   writes. `key` is the LSN after the previous group's last record, and
//!   the object is created with a conditional put (`If-None-Match: *`), so
//!   two processes that append to one stream race for the same key and
//!   exactly one wins (see [`WalWriter`](crate::wal::WalWriter)).
//! - `meta/origin`: the stream's origin, created once. Manifests name it in
//!   `wal_origin`, so a replacement node with an empty disk recognizes the
//!   checkpoints its predecessor published.
//! - `meta/sequence`: the LSN high-water mark, written before reclaimed
//!   objects are deleted so their LSNs are never reused.
//!
//! [`ObjectWalStore`] is storage-agnostic: it talks to a
//! [`WalObjectBackend`]. The server's S3 backend lives in `kosha-server`
//! (behind its `s3` feature); [`MemoryObjectBackend`] is an in-memory
//! backend with fault injection for tests and benchmarks.
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kosha_core::KoshaError;

use crate::wal::{LocalWalStore, WalStore};

/// The object operations a WAL stream needs. Keys are relative to the
/// backend's own root (an S3 backend prepends its bucket prefix).
pub trait WalObjectBackend: Send + Sync + std::fmt::Debug {
    /// Create `key` unless it exists. An existing key fails with
    /// [`std::io::ErrorKind::AlreadyExists`] and is left unchanged.
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<(), KoshaError>;
    /// Create or replace `key`.
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), KoshaError>;
    /// `None` when the key does not exist.
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KoshaError>;
    /// Every key starting with `prefix`, in any order.
    fn list(&self, prefix: &str) -> Result<Vec<String>, KoshaError>;
    /// Delete keys; missing keys are not an error.
    fn delete(&self, keys: &[String]) -> Result<(), KoshaError>;
}

/// A WAL stream on an object store. See the module docs for the layout.
#[derive(Debug, Clone)]
pub struct ObjectWalStore {
    backend: Arc<dyn WalObjectBackend>,
    root: String,
}

impl ObjectWalStore {
    /// The stream of `writer_id` (`[A-Za-z0-9._-]`, at most 128 bytes).
    /// Each ingest process that runs at the same time needs its own id;
    /// a replacement for a lost node reuses the lost node's id.
    pub fn new(backend: Arc<dyn WalObjectBackend>, writer_id: &str) -> Result<Self, KoshaError> {
        let valid = !writer_id.is_empty()
            && writer_id.len() <= 128
            && writer_id != "."
            && writer_id != ".."
            && writer_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
        if !valid {
            return Err(wal_error(format!(
                "invalid WAL writer id {writer_id:?}: use 1-128 of [A-Za-z0-9._-]"
            )));
        }
        Ok(Self {
            backend,
            root: format!("_wal/{writer_id}"),
        })
    }

    /// Key prefix of this stream, relative to the backend root.
    pub fn root(&self) -> &str {
        &self.root
    }

    fn object_key(&self, key: u64) -> String {
        format!("{}/{key:020}.bin", self.root)
    }

    fn meta_key(&self, name: &str) -> String {
        format!("{}/meta/{name}", self.root)
    }

    /// Fail unless the backend rejects a second create of one key. A store
    /// that ignores `If-None-Match` would let two writers overwrite each
    /// other's groups, so the server refuses to start on one.
    pub fn verify_conditional_put(&self) -> Result<(), KoshaError> {
        let probe = self.meta_key(&format!("probe-{}", uuid::Uuid::new_v4()));
        self.backend.put_if_absent(&probe, b"1")?;
        let second = self.backend.put_if_absent(&probe, b"2");
        let stored = self.backend.get(&probe);
        self.backend.delete(std::slice::from_ref(&probe)).ok();
        match second {
            Err(KoshaError::Io(e)) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
            Ok(()) => {
                return Err(wal_error(
                    "object store accepted a second If-None-Match: * put of one key; \
                     the S3 WAL needs conditional writes"
                        .into(),
                ))
            }
        }
        match stored? {
            Some(bytes) if bytes == b"1" => Ok(()),
            _ => Err(wal_error(
                "object store replaced an object on a rejected conditional put".into(),
            )),
        }
    }
}

impl WalStore for ObjectWalStore {
    fn append_batch(&self, first_lsn: u64, bytes: &[u8]) -> Result<(), KoshaError> {
        self.backend
            .put_if_absent(&self.object_key(first_lsn), bytes)
    }

    fn conditional(&self) -> bool {
        true
    }

    fn list(&self) -> Result<Vec<u64>, KoshaError> {
        let prefix = format!("{}/", self.root);
        let mut keys: Vec<u64> = self
            .backend
            .list(&prefix)?
            .iter()
            .filter_map(|key| {
                let name = key.strip_prefix(&prefix)?.strip_suffix(".bin")?;
                (name.len() == 20 && name.bytes().all(|b| b.is_ascii_digit()))
                    .then(|| name.parse().ok())
                    .flatten()
            })
            .collect();
        keys.sort_unstable();
        Ok(keys)
    }

    fn read(&self, first_lsn: u64) -> Result<Vec<u8>, KoshaError> {
        let key = self.object_key(first_lsn);
        self.backend
            .get(&key)?
            .ok_or_else(|| KoshaError::NotFound(format!("WAL object {key}")))
    }

    fn delete(&self, first_lsns: &[u64]) -> Result<(), KoshaError> {
        if first_lsns.is_empty() {
            return Ok(());
        }
        let keys: Vec<String> = first_lsns.iter().map(|k| self.object_key(*k)).collect();
        self.backend.delete(&keys)
    }

    fn truncate_torn_tail(&self, first_lsn: u64, complete_len: usize) -> Result<(), KoshaError> {
        // Objects land whole, so this only repairs an object that was
        // copied torn from a local WAL. Keep the original aside first.
        let bytes = self.read(first_lsn)?;
        self.backend
            .put(&format!("{}/torn/{first_lsn:020}.bin", self.root), &bytes)?;
        if complete_len == 0 {
            self.delete(&[first_lsn])
        } else {
            self.backend
                .put(&self.object_key(first_lsn), &bytes[..complete_len])
        }
    }

    fn read_meta(&self, name: &str) -> Result<Option<Vec<u8>>, KoshaError> {
        self.backend.get(&self.meta_key(name))
    }

    fn write_meta(&self, name: &str, bytes: &[u8]) -> Result<(), KoshaError> {
        self.backend.put(&self.meta_key(name), bytes)
    }

    fn create_meta(&self, name: &str, bytes: &[u8]) -> Result<(), KoshaError> {
        match self.backend.put_if_absent(&self.meta_key(name), bytes) {
            Err(KoshaError::Io(e)) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            other => other,
        }
    }
}

/// Copy a local WAL directory into `remote` before a writer opens it, for
/// switching `KOSHA_WAL_MODE` from `local` to `s3`. Files keep their keys
/// and bytes and the stream takes the local origin and sequence, so the
/// published `wal_checkpoint`/`wal_origin` of every manifest keeps its
/// meaning. The directory is then renamed to `_wal.migrated-{unix secs}`
/// (kept, not deleted). Returns the number of files copied.
///
/// Fails, changing nothing locally, when the remote stream already belongs
/// to a different origin: drain one side first (flush every namespace).
/// Safe to rerun after a crash part-way through.
pub fn migrate_local_wal(local_dir: &Path, remote: &dyn WalStore) -> Result<usize, KoshaError> {
    let local = LocalWalStore::new(local_dir.to_path_buf());
    let files = local.list()?;
    if files.is_empty() {
        return Ok(0);
    }
    let local_origin = local.read_meta("origin")?;
    if let Some(origin) = &local_origin {
        remote.create_meta("origin", origin)?;
        let remote_origin = remote.read_meta("origin")?;
        if remote_origin.as_ref() != Some(origin) {
            return Err(wal_error(format!(
                "local WAL {} (origin {}) still holds {} file(s) and the remote WAL stream \
                 belongs to origin {}: flush every namespace in one mode before switching",
                local_dir.display(),
                String::from_utf8_lossy(origin),
                files.len(),
                String::from_utf8_lossy(&remote_origin.unwrap_or_default()),
            )));
        }
    }
    let sequence = |store: &dyn WalStore| -> Result<u64, KoshaError> {
        Ok(store
            .read_meta("sequence")?
            .and_then(|b| String::from_utf8(b).ok())
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0))
    };
    let local_sequence = sequence(&local)?;
    if local_sequence > sequence(remote)? {
        remote.write_meta("sequence", local_sequence.to_string().as_bytes())?;
    }
    // Deleted namespaces whose records the copied files still hold.
    let local_discarded = crate::wal::read_discarded(&local)?;
    if !local_discarded.is_empty() {
        let mut discarded = crate::wal::read_discarded(remote)?;
        for (namespace, through) in local_discarded {
            let entry = discarded.entry(namespace).or_insert(0);
            *entry = (*entry).max(through);
        }
        crate::wal::write_discarded(remote, &discarded)?;
    }
    for file in &files {
        let bytes = local.read(*file)?;
        match remote.append_batch(*file, &bytes) {
            Ok(()) => {}
            Err(KoshaError::Io(e)) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if remote.read(*file)? != bytes {
                    return Err(wal_error(format!(
                        "remote WAL object {file} differs from local WAL file {}",
                        local_dir.display()
                    )));
                }
            }
            Err(e) => return Err(e),
        }
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let parent = local_dir.parent().unwrap_or(Path::new("."));
    let name = local_dir
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "_wal".into());
    std::fs::rename(local_dir, parent.join(format!("{name}.migrated-{stamp}")))?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(files.len())
}

/// In-memory [`WalObjectBackend`] with S3's conditional-put semantics and
/// fault injection, for tests and benchmarks. Clones share the same
/// objects, like two processes pointed at one bucket.
#[derive(Debug, Clone, Default)]
pub struct MemoryObjectBackend {
    inner: Arc<MemoryInner>,
}

#[derive(Debug, Default)]
struct MemoryInner {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
    /// The next N puts fail without storing anything.
    fail_puts: AtomicUsize,
    /// The next N puts store the object and then report a failure, like a
    /// timeout whose request still landed.
    lose_put_replies: AtomicUsize,
    /// The next N conditional puts store the object and then report a
    /// conflict, like an SDK retry of a landed put that gets HTTP 412.
    conflict_put_replies: AtomicUsize,
    /// The next N conditional puts store nothing and report a conflict,
    /// like S3's 409 while an earlier put of the key is still in flight.
    phantom_conflicts: AtomicUsize,
    /// The next N gets fail, like an unreachable store.
    fail_gets: AtomicUsize,
    /// Simulated put latency: fixed part, and nanoseconds per byte.
    latency: Mutex<(Duration, u64)>,
    puts: AtomicU64,
    put_attempts: AtomicU64,
}

impl MemoryObjectBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fail the next `n` puts (nothing is stored).
    pub fn fail_next_puts(&self, n: usize) {
        self.inner.fail_puts.store(n, Ordering::SeqCst);
    }

    /// Store the next `n` puts but report them as failed.
    pub fn lose_next_put_replies(&self, n: usize) {
        self.inner.lose_put_replies.store(n, Ordering::SeqCst);
    }

    /// Store the next `n` conditional puts but report a conflict.
    pub fn conflict_next_put_replies(&self, n: usize) {
        self.inner.conflict_put_replies.store(n, Ordering::SeqCst);
    }

    /// Report a conflict for the next `n` conditional puts without storing.
    pub fn phantom_conflict_next_puts(&self, n: usize) {
        self.inner.phantom_conflicts.store(n, Ordering::SeqCst);
    }

    /// Fail the next `n` gets.
    pub fn fail_next_gets(&self, n: usize) {
        self.inner.fail_gets.store(n, Ordering::SeqCst);
    }

    /// Every put sleeps `fixed + bytes * nanos_per_byte`.
    pub fn set_put_latency(&self, fixed: Duration, nanos_per_byte: u64) {
        *self.inner.latency.lock().unwrap() = (fixed, nanos_per_byte);
    }

    /// Puts sent so far, whatever their outcome.
    pub fn put_attempts(&self) -> u64 {
        self.inner.put_attempts.load(Ordering::SeqCst)
    }

    /// Successful puts so far (conditional and plain).
    pub fn puts(&self) -> u64 {
        self.inner.puts.load(Ordering::SeqCst)
    }

    /// Every stored key, sorted.
    pub fn keys(&self) -> Vec<String> {
        self.inner.objects.lock().unwrap().keys().cloned().collect()
    }

    fn take(counter: &AtomicUsize) -> bool {
        let mut current = counter.load(Ordering::SeqCst);
        while current > 0 {
            match counter.compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
        false
    }

    fn store(&self, key: &str, bytes: &[u8], conditional: bool) -> Result<(), KoshaError> {
        self.inner.put_attempts.fetch_add(1, Ordering::SeqCst);
        let (fixed, per_byte) = *self.inner.latency.lock().unwrap();
        let delay = fixed + Duration::from_nanos(per_byte.saturating_mul(bytes.len() as u64));
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        if Self::take(&self.inner.fail_puts) {
            return Err(std::io::Error::other("injected object store put failure").into());
        }
        if conditional && Self::take(&self.inner.phantom_conflicts) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("conditional request conflict: {key}"),
            )
            .into());
        }
        {
            let mut objects = self.inner.objects.lock().unwrap();
            if conditional && objects.contains_key(key) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("precondition failed: {key} exists"),
                )
                .into());
            }
            objects.insert(key.to_owned(), bytes.to_vec());
        }
        self.inner.puts.fetch_add(1, Ordering::SeqCst);
        if conditional && Self::take(&self.inner.conflict_put_replies) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("precondition failed: {key} exists"),
            )
            .into());
        }
        if Self::take(&self.inner.lose_put_replies) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "injected lost put reply",
            )
            .into());
        }
        Ok(())
    }
}

impl WalObjectBackend for MemoryObjectBackend {
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<(), KoshaError> {
        self.store(key, bytes, true)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), KoshaError> {
        self.store(key, bytes, false)
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KoshaError> {
        if Self::take(&self.inner.fail_gets) {
            return Err(std::io::Error::other("injected object store get failure").into());
        }
        Ok(self.inner.objects.lock().unwrap().get(key).cloned())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, KoshaError> {
        Ok(self
            .inner
            .objects
            .lock()
            .unwrap()
            .range(prefix.to_owned()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, _)| key.clone())
            .collect())
    }

    fn delete(&self, keys: &[String]) -> Result<(), KoshaError> {
        let mut objects = self.inner.objects.lock().unwrap();
        for key in keys {
            objects.remove(key);
        }
        Ok(())
    }
}

fn wal_error(message: String) -> KoshaError {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{GroupCommitConfig, WalWriter};
    use kosha_core::{Document, DocumentId, Field, NamespaceId};
    use std::sync::Barrier;

    fn doc(id: &str) -> Document {
        Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("body", id)],
        }
    }

    fn store(backend: &MemoryObjectBackend, writer: &str) -> Arc<dyn WalStore> {
        Arc::new(ObjectWalStore::new(Arc::new(backend.clone()), writer).unwrap())
    }

    fn open(backend: &MemoryObjectBackend) -> (WalWriter, Vec<String>) {
        let (writer, records) =
            WalWriter::open(store(backend, "ingest-0"), GroupCommitConfig::default()).unwrap();
        let ids = records
            .iter()
            .flat_map(|(_, r)| r.documents.iter().map(|d| d.id.0.clone()))
            .collect();
        (writer, ids)
    }

    fn ns() -> NamespaceId {
        NamespaceId("t/ns".into())
    }

    #[test]
    fn acked_records_survive_losing_the_writer() {
        let backend = MemoryObjectBackend::new();
        let (writer, recovered) = open(&backend);
        assert!(recovered.is_empty());
        let first = writer.append(&ns(), &[doc("a")]).unwrap();
        let second = writer.append(&ns(), &[doc("b")]).unwrap();
        assert!(second > first);
        let origin = writer.origin().to_owned();
        drop(writer);
        // A new process with nothing but the bucket.
        let (restarted, recovered) = open(&backend);
        assert_eq!(recovered, vec!["a", "b"]);
        assert_eq!(restarted.origin(), origin, "origin lives in the stream");
        let third = restarted.append(&ns(), &[doc("c")]).unwrap();
        assert!(third > second, "LSNs never restart");
        assert!(backend
            .keys()
            .iter()
            .all(|k| k.starts_with("_wal/ingest-0/")));
    }

    #[test]
    fn two_writers_racing_for_one_key_exactly_one_wins() {
        let backend = MemoryObjectBackend::new();
        let (a, _) = open(&backend);
        let (b, _) = open(&backend);
        assert_eq!(a.origin(), b.origin(), "one origin per stream");
        let won = a.append(&ns(), &[doc("from-a")]).unwrap();
        let lost = b.append(&ns(), &[doc("from-b")]).unwrap_err();
        assert!(lost.to_string().contains("fenced"), "{lost}");
        // The loser stays fenced: it can no longer see the whole stream.
        assert!(b.append(&ns(), &[doc("from-b-2")]).is_err());
        assert!(a.append(&ns(), &[doc("from-a-2")]).unwrap() > won);
        drop((a, b));
        // The loser's retry after a restart lands behind the winner's.
        let (retry, recovered) = open(&backend);
        assert_eq!(recovered, vec!["from-a", "from-a-2"]);
        retry.append(&ns(), &[doc("from-b")]).unwrap();
        drop(retry);
        assert_eq!(open(&backend).1, vec!["from-a", "from-a-2", "from-b"]);
    }

    #[test]
    fn concurrent_racing_writers_never_lose_an_acked_record() {
        let backend = MemoryObjectBackend::new();
        backend.set_put_latency(Duration::from_micros(200), 0);
        let writers = [open(&backend).0, open(&backend).0];
        let barrier = Barrier::new(writers.len() * 4);
        let acked: Vec<String> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..writers.len() * 4)
                .map(|t| {
                    let writer = &writers[t % writers.len()];
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        (0..25)
                            .filter_map(|n| {
                                let id = format!("w{}-t{t}-{n}", t % 2);
                                let ns = NamespaceId(format!("ns-{t}"));
                                writer.append(&ns, &[doc(&id)]).ok().map(|_| id)
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect()
        });
        assert!(!acked.is_empty());
        let winners: std::collections::HashSet<_> =
            acked.iter().map(|id| id[..2].to_owned()).collect();
        assert_eq!(
            winners.len(),
            1,
            "only one writer keeps acking: {winners:?}"
        );
        drop(writers);
        let (_, recovered) = open(&backend);
        let recovered: std::collections::HashSet<_> = recovered.into_iter().collect();
        for id in &acked {
            assert!(recovered.contains(id), "acked {id} was lost");
        }
    }

    #[test]
    fn failed_put_acks_nothing_and_the_next_group_reuses_the_key() {
        let backend = MemoryObjectBackend::new();
        let (writer, _) = open(&backend);
        writer.append(&ns(), &[doc("a")]).unwrap();
        backend.fail_next_puts(1);
        assert!(writer.append(&ns(), &[doc("lost")]).is_err());
        writer.append(&ns(), &[doc("b")]).unwrap();
        drop(writer);
        assert_eq!(open(&backend).1, vec!["a", "b"]);
    }

    #[test]
    fn a_put_whose_reply_was_lost_is_adopted_not_fenced() {
        let backend = MemoryObjectBackend::new();
        let (writer, _) = open(&backend);
        backend.lose_next_put_replies(1);
        assert!(writer.append(&ns(), &[doc("landed")]).is_err());
        // The next group conflicts with the landed one, recognizes its own
        // bytes and moves past it.
        writer.append(&ns(), &[doc("next")]).unwrap();
        writer.append(&ns(), &[doc("after")]).unwrap();
        drop(writer);
        // Unacknowledged but durable records replay (at-least-once).
        assert_eq!(open(&backend).1, vec!["landed", "next", "after"]);
    }

    /// #368: a put lands although its reply is lost, and the store stays
    /// unreachable for far more groups than the writer remembers attempts.
    /// The landed attempt must still be recognized once the store answers.
    #[test]
    fn a_landed_put_is_still_adopted_after_a_long_outage() {
        let backend = MemoryObjectBackend::new();
        let (writer, _) = open(&backend);
        writer.append(&ns(), &[doc("a")]).unwrap();
        backend.lose_next_put_replies(1);
        assert!(writer.append(&ns(), &[doc("landed")]).is_err());
        // The outage: puts and gets fail.
        backend.fail_next_puts(usize::MAX);
        backend.fail_next_gets(usize::MAX);
        let sent_before = backend.put_attempts();
        for i in 0..40 {
            let err = writer.append(&ns(), &[doc(&format!("down-{i}"))]);
            let err = err.unwrap_err().to_string();
            assert!(!err.contains("fenced"), "{err}");
        }
        // Once as many attempts are remembered as the writer keeps, it
        // sends no put it could not account for later.
        let sent = backend.put_attempts() - sent_before;
        assert!(sent < 10, "{sent} puts sent during the outage");
        backend.fail_next_puts(0);
        backend.fail_next_gets(0);
        writer.append(&ns(), &[doc("b")]).unwrap();
        writer.append(&ns(), &[doc("c")]).unwrap();
        drop(writer);
        // Unacknowledged but durable records replay (at-least-once).
        assert_eq!(open(&backend).1, vec!["a", "landed", "b", "c"]);
    }

    /// The counterpart of the test above: with as many failed attempts
    /// remembered as the writer keeps, an object another writer put at the
    /// key still fences this one, and nothing of the other's is replaced.
    #[test]
    fn a_foreign_object_still_fences_after_many_failed_puts() {
        let backend = MemoryObjectBackend::new();
        let (writer, _) = open(&backend);
        writer.append(&ns(), &[doc("a")]).unwrap();
        // Puts that never landed: the key stays free.
        backend.fail_next_puts(20);
        for i in 0..20 {
            assert!(writer.append(&ns(), &[doc(&format!("down-{i}"))]).is_err());
        }
        // A second process on the same stream takes the key.
        let (other, recovered) = open(&backend);
        assert_eq!(recovered, vec!["a"]);
        other.append(&ns(), &[doc("foreign")]).unwrap();
        let before = backend.keys();
        let err = writer.append(&ns(), &[doc("late")]).unwrap_err();
        assert!(err.to_string().contains("fenced"), "{err}");
        let err = writer.append(&ns(), &[doc("later")]).unwrap_err();
        assert!(err.to_string().contains("fenced"), "{err}");
        assert_eq!(backend.keys(), before);
        other.append(&ns(), &[doc("foreign-2")]).unwrap();
        drop((writer, other));
        assert_eq!(open(&backend).1, vec!["a", "foreign", "foreign-2"]);
    }

    /// A foreign object fences even when this writer has an attempt of its
    /// own with an unknown outcome for the same key.
    #[test]
    fn a_foreign_object_fences_a_writer_with_an_ambiguous_put() {
        let backend = MemoryObjectBackend::new();
        let (writer, _) = open(&backend);
        writer.append(&ns(), &[doc("a")]).unwrap();
        backend.fail_next_puts(1);
        assert!(writer.append(&ns(), &[doc("unknown")]).is_err());
        let (other, _) = open(&backend);
        other.append(&ns(), &[doc("foreign")]).unwrap();
        let err = writer.append(&ns(), &[doc("late")]).unwrap_err();
        assert!(err.to_string().contains("fenced"), "{err}");
        drop((writer, other));
        assert_eq!(open(&backend).1, vec!["a", "foreign"]);
    }

    /// Failed puts that never landed do not wedge the key: the store says
    /// it is still absent, so the writer keeps trying it.
    #[test]
    fn many_failed_puts_that_never_landed_do_not_block_the_key() {
        let backend = MemoryObjectBackend::new();
        let (writer, _) = open(&backend);
        backend.fail_next_puts(20);
        for i in 0..20 {
            assert!(writer.append(&ns(), &[doc(&format!("down-{i}"))]).is_err());
        }
        writer.append(&ns(), &[doc("a")]).unwrap();
        writer.append(&ns(), &[doc("b")]).unwrap();
        drop(writer);
        assert_eq!(open(&backend).1, vec!["a", "b"]);
    }

    #[test]
    fn a_retried_put_that_conflicts_with_its_own_landed_bytes_succeeds() {
        let backend = MemoryObjectBackend::new();
        let (writer, _) = open(&backend);
        writer.append(&ns(), &[doc("a")]).unwrap();
        // The first attempt landed, its reply was lost and the SDK retry
        // got 412: the stored object is exactly what this put wrote.
        backend.conflict_next_put_replies(1);
        writer.append(&ns(), &[doc("b")]).unwrap();
        writer.append(&ns(), &[doc("c")]).unwrap();
        drop(writer);
        assert_eq!(open(&backend).1, vec!["a", "b", "c"]);
    }

    #[test]
    fn a_conflict_with_no_stored_object_is_retried_not_fenced() {
        let backend = MemoryObjectBackend::new();
        let (writer, _) = open(&backend);
        writer.append(&ns(), &[doc("a")]).unwrap();
        // S3 409: an earlier put of the key is still in flight; nothing is
        // readable yet. The outcome is unknown, so the group fails but the
        // writer is not fenced.
        backend.phantom_conflict_next_puts(1);
        let err = writer.append(&ns(), &[doc("unknown")]).unwrap_err();
        assert!(!err.to_string().contains("fenced"), "{err}");
        writer.append(&ns(), &[doc("b")]).unwrap();
        drop(writer);
        assert_eq!(open(&backend).1, vec!["a", "b"]);
    }

    #[test]
    fn a_discarded_namespace_in_a_shared_object_does_not_come_back() {
        let backend = MemoryObjectBackend::new();
        let (writer, _) = open(&backend);
        let other = NamespaceId("t/other".into());
        writer
            .append_records(&[
                kosha_core::WalRecord::new(ns(), vec![doc("deleted")]),
                kosha_core::WalRecord::new(other.clone(), vec![doc("kept")]),
            ])
            .unwrap();
        assert_eq!(writer.discard_namespace(&ns()).unwrap(), 1);
        drop(writer);
        // Node lost before `other` flushed: a new process replays only it.
        let (restarted, recovered) = open(&backend);
        assert_eq!(recovered, vec!["kept"]);
        // Once `other` is checkpointed the object and the discard go.
        let last = restarted.last_sequence();
        restarted.clear_through(&other, last).unwrap();
        assert!(restarted.discarded_namespaces().is_empty());
        assert!(!backend.keys().iter().any(|k| k.ends_with(".bin")));
        assert!(open(&backend).0.discarded_namespaces().is_empty());
    }

    #[test]
    fn reclamation_deletes_checkpointed_objects_only() {
        let backend = MemoryObjectBackend::new();
        let (writer, _) = open(&backend);
        let other = NamespaceId("t/other".into());
        let a = writer.append(&ns(), &[doc("a")]).unwrap();
        writer.append(&other, &[doc("o")]).unwrap();
        writer.clear_through(&ns(), a).unwrap();
        let (_, recovered) = open(&backend);
        assert_eq!(recovered, vec!["o"]);
        assert!(backend
            .keys()
            .contains(&"_wal/ingest-0/meta/sequence".to_owned()));
    }

    #[test]
    fn probe_rejects_stores_without_conditional_put() {
        #[derive(Debug)]
        struct Unconditional(MemoryObjectBackend);
        impl WalObjectBackend for Unconditional {
            fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<(), KoshaError> {
                self.0.put(key, bytes)
            }
            fn put(&self, key: &str, bytes: &[u8]) -> Result<(), KoshaError> {
                self.0.put(key, bytes)
            }
            fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KoshaError> {
                self.0.get(key)
            }
            fn list(&self, prefix: &str) -> Result<Vec<String>, KoshaError> {
                self.0.list(prefix)
            }
            fn delete(&self, keys: &[String]) -> Result<(), KoshaError> {
                self.0.delete(keys)
            }
        }
        let good = ObjectWalStore::new(Arc::new(MemoryObjectBackend::new()), "w").unwrap();
        good.verify_conditional_put().unwrap();
        let backend = MemoryObjectBackend::new();
        let bad = ObjectWalStore::new(Arc::new(Unconditional(backend.clone())), "w").unwrap();
        assert!(bad.verify_conditional_put().is_err());
        assert!(backend.keys().is_empty(), "probe cleans up");
    }

    #[test]
    fn writer_ids_are_validated() {
        let backend: Arc<dyn WalObjectBackend> = Arc::new(MemoryObjectBackend::new());
        for bad in ["", "..", "a/b", "a b", &"x".repeat(129)] {
            assert!(
                ObjectWalStore::new(backend.clone(), bad).is_err(),
                "{bad:?}"
            );
        }
        assert!(ObjectWalStore::new(backend, "kosha-ingest_0.a").is_ok());
    }

    #[test]
    fn local_wal_migrates_with_its_origin_and_lsns() {
        let dir = std::env::temp_dir().join(format!("kosha-wal-migrate-{}", uuid::Uuid::new_v4()));
        let wal_dir = dir.join("_wal");
        let local = WalWriter::new(wal_dir.clone());
        let a = local.append(&ns(), &[doc("a")]).unwrap();
        let b = local.append(&ns(), &[doc("b")]).unwrap();
        let origin = local.origin().to_owned();
        drop(local);
        let expected = WalWriter::recover_sequenced(&wal_dir).unwrap();

        let backend = MemoryObjectBackend::new();
        let remote = store(&backend, "ingest-0");
        assert_eq!(migrate_local_wal(&wal_dir, remote.as_ref()).unwrap(), 2);
        assert!(!wal_dir.exists(), "local WAL moved aside");
        assert!(std::fs::read_dir(&dir).unwrap().any(|e| e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("_wal.migrated-")));
        let (writer, records) = WalWriter::open(remote, GroupCommitConfig::default()).unwrap();
        assert_eq!(writer.origin(), origin, "checkpoints keep their meaning");
        let lsns: Vec<u64> = records.iter().map(|(lsn, _)| *lsn).collect();
        assert_eq!(lsns, vec![a, b]);
        assert_eq!(records.len(), expected.len());
        assert!(writer.append(&ns(), &[doc("c")]).unwrap() > b);
        // Nothing left to migrate.
        assert_eq!(
            migrate_local_wal(&wal_dir, &*store(&backend, "ingest-0")).unwrap(),
            0
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn migration_refuses_a_stream_of_another_origin() {
        let dir = std::env::temp_dir().join(format!("kosha-wal-migrate-{}", uuid::Uuid::new_v4()));
        let local = WalWriter::new(dir.clone());
        local.append(&ns(), &[doc("a")]).unwrap();
        drop(local);
        let backend = MemoryObjectBackend::new();
        let (remote, _) = open(&backend);
        remote.append(&ns(), &[doc("remote")]).unwrap();
        drop(remote);
        let error = migrate_local_wal(&dir, &*store(&backend, "ingest-0")).unwrap_err();
        assert!(
            error.to_string().contains("flush every namespace"),
            "{error}"
        );
        assert_eq!(
            WalWriter::recover(&dir).unwrap().len(),
            1,
            "local WAL untouched"
        );
        std::fs::remove_dir_all(dir).ok();
    }
}
