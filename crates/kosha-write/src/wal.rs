use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use kosha_core::{
    Document, DocumentId, Field, FieldType, KoshaError, NamespaceId, StorageBackend, WalRecord,
};

pub(crate) fn durable_write(path: &std::path::Path, bytes: &[u8]) -> Result<(), KoshaError> {
    use std::io::Write;
    let temporary = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    std::fs::File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}

/// Write-Ahead Log for durability of buffered documents.
///
/// The WAL stores document batches per namespace in sequential files.
/// On crash, the WAL is replayed to recover un-flushed documents.
///
/// New files contain one v2 frame: magic, version, payload length (u64),
/// CRC32C (u32), then a JSON WalRecord. V1 binary records remain readable.
/// An incomplete final file is discarded atomically; malformed/checksum-failed
/// records fail recovery closed rather than silently losing acknowledged data.
pub struct WalWriter {
    #[allow(dead_code)]
    backend: Box<dyn StorageBackend>,
    wal_dir: PathBuf,
    origin: String,
    current_file: String,
    seq: AtomicU64,
    buffer: Vec<u8>,
    records_in_buffer: u32,
    record_namespaces: std::collections::BTreeMap<u64, std::collections::HashSet<NamespaceId>>,
}

impl WalWriter {
    const MAGIC: u32 = 0x4B57414C;
    const VERSION: u32 = 2;

    pub fn new(backend: Box<dyn StorageBackend>, wal_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&wal_dir).expect("cannot create WAL directory");
        let origin_path = wal_dir.join("origin");
        let origin = match std::fs::read_to_string(&origin_path) {
            Ok(origin) => {
                uuid::Uuid::parse_str(&origin).expect("invalid WAL origin");
                origin
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let origin = uuid::Uuid::new_v4().to_string();
                durable_write(&origin_path, origin.as_bytes()).expect("cannot persist WAL origin");
                origin
            }
            Err(e) => panic!("cannot read WAL origin: {e}"),
        };
        let seq = Self::next_seq(&wal_dir);
        let file = format!("wal-{:020x}.wal", seq);
        let mut record_namespaces: std::collections::BTreeMap<
            u64,
            std::collections::HashSet<NamespaceId>,
        > = Default::default();
        let recovered = Self::recover_sequenced(&wal_dir).expect("WAL recovery failed");
        // A discarded tail must not become a corrupt middle record after
        // subsequent writes. Preserve it outside the replay file set.
        let mut paths: Vec<_> = std::fs::read_dir(&wal_dir)
            .expect("cannot read WAL directory")
            .map(|entry| entry.expect("cannot read WAL entry").path())
            .filter(|path| path.extension().is_some_and(|e| e == "wal"))
            .collect();
        paths.sort();
        if let Some(tail) = paths.last() {
            let bytes = std::fs::read(tail).expect("cannot read WAL tail");
            if matches!(decode_file(&bytes), Err(KoshaError::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof)
            {
                std::fs::rename(tail, tail.with_extension("torn"))
                    .expect("cannot quarantine torn WAL tail");
                std::fs::File::open(&wal_dir)
                    .and_then(|dir| dir.sync_all())
                    .expect("cannot sync WAL repair");
            }
        }
        for (sequence, record) in recovered {
            record_namespaces
                .entry(sequence)
                .or_default()
                .insert(record.namespace);
        }
        Self {
            backend,
            wal_dir,
            origin,
            current_file: file,
            seq: AtomicU64::new(seq + 1),
            buffer: Vec::new(),
            records_in_buffer: 0,
            record_namespaces,
        }
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }

    fn next_seq(wal_dir: &PathBuf) -> u64 {
        let mut max = 0u64;
        if let Ok(entries) = std::fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(rest) = name
                    .strip_prefix("wal-")
                    .and_then(|s| s.strip_suffix(".wal"))
                {
                    if let Ok(n) = u64::from_str_radix(rest, 16) {
                        if n > max {
                            max = n;
                        }
                    }
                }
            }
        }
        let persisted = std::fs::read_to_string(wal_dir.join("sequence"))
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        max.max(persisted).saturating_add(1)
    }

    pub fn last_sequence(&self) -> u64 {
        self.seq.load(Ordering::Relaxed).saturating_sub(2)
    }

    /// A fresh ingest disk must not reuse sequence numbers already covered
    /// by the remote manifests restored on startup.
    pub fn advance_past(&mut self, checkpoint: u64) {
        if self.seq.load(Ordering::Relaxed) - 1 <= checkpoint {
            self.current_file = format!("wal-{:020x}.wal", checkpoint + 1);
            self.seq.store(checkpoint + 2, Ordering::Relaxed);
        }
    }

    /// Reclaim only records covered by a successfully published namespace
    /// manifest. Later writes and every other namespace remain recoverable.
    pub fn clear_through(
        &mut self,
        namespace: &NamespaceId,
        checkpoint: u64,
    ) -> Result<(), KoshaError> {
        let eligible: Vec<u64> = self
            .record_namespaces
            .range(..=checkpoint)
            .filter(|(_, namespaces)| namespaces.len() == 1 && namespaces.contains(namespace))
            .map(|(seq, _)| *seq)
            .collect();
        for seq in eligible {
            std::fs::remove_file(self.wal_dir.join(format!("wal-{seq:020x}.wal")))?;
            self.record_namespaces.remove(&seq);
        }
        Ok(())
    }

    /// Append a batch of documents to the WAL.
    pub fn append(
        &mut self,
        namespace: &NamespaceId,
        documents: &[Document],
    ) -> Result<(), KoshaError> {
        let record = WalRecord::new(namespace.clone(), documents.to_vec());
        self.append_record(&record)
    }

    pub fn append_delete(
        &mut self,
        namespace: &NamespaceId,
        ids: Vec<DocumentId>,
    ) -> Result<(), KoshaError> {
        let mut record = WalRecord::new(namespace.clone(), Vec::new());
        record.deleted_ids = ids;
        self.append_record(&record)
    }

    fn append_record(&mut self, record: &WalRecord) -> Result<(), KoshaError> {
        self.buffer = serde_json::to_vec(record)?;
        self.records_in_buffer = 1;
        if let Err(e) = self.flush_buffer() {
            self.buffer.clear();
            self.records_in_buffer = 0;
            return Err(e);
        }
        self.record_namespaces
            .entry(self.last_sequence())
            .or_default()
            .insert(record.namespace.clone());
        Ok(())
    }

    fn flush_buffer(&mut self) -> Result<(), KoshaError> {
        if self.records_in_buffer == 0 {
            return Ok(());
        }

        let mut final_buf = Vec::new();
        final_buf.extend_from_slice(&Self::MAGIC.to_le_bytes());
        final_buf.extend_from_slice(&Self::VERSION.to_le_bytes());
        final_buf.extend_from_slice(&(self.buffer.len() as u64).to_le_bytes());
        final_buf.extend_from_slice(&crc32c::crc32c(&self.buffer).to_le_bytes());
        final_buf.extend_from_slice(&self.buffer);

        let path = self.wal_dir.join(&self.current_file);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        // Reserve the sequence durably before writing the record. A crash
        // may leave a gap, but must never reuse a published checkpoint ID.
        let current = self.seq.load(Ordering::Relaxed) - 1;
        durable_write(
            &self.wal_dir.join("sequence"),
            current.to_string().as_bytes(),
        )?;
        durable_write(&path, &final_buf)?;

        self.buffer.clear();
        self.records_in_buffer = 0;

        // Rotate to next file
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        self.current_file = format!("wal-{:020x}.wal", seq);
        Ok(())
    }

    /// Explicit administrative reset. Normal flushes use `clear_through`
    /// only after durable publication.
    pub fn clear(&mut self) -> Result<(), KoshaError> {
        self.record_namespaces.clear();
        if let Ok(entries) = std::fs::read_dir(&self.wal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "wal") {
                    std::fs::remove_file(&path).ok();
                }
            }
        }
        let seq = Self::next_seq(&self.wal_dir);
        self.seq.store(seq + 1, Ordering::Relaxed);
        self.current_file = format!("wal-{:020x}.wal", seq + 1);
        Ok(())
    }

    /// Read all records from all WAL files for recovery.
    pub fn recover(wal_dir: &PathBuf) -> Result<Vec<WalRecord>, KoshaError> {
        Ok(Self::recover_sequenced(wal_dir)?
            .into_iter()
            .map(|(_, r)| r)
            .collect())
    }

    pub fn recover_sequenced(wal_dir: &PathBuf) -> Result<Vec<(u64, WalRecord)>, KoshaError> {
        let mut records = Vec::new();

        let mut files: Vec<std::path::PathBuf> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "wal") {
                    files.push(path);
                }
            }
        }
        files.sort();

        for path in &files {
            let sequence = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.strip_prefix("wal-"))
                .and_then(|s| u64::from_str_radix(s, 16).ok())
                .ok_or_else(|| KoshaError::NotFound("invalid WAL filename".into()))?;
            let data = std::fs::read(path)?;
            let decoded = decode_file(&data);
            match decoded {
                Ok(batch) => records.extend(batch.into_iter().map(|record| (sequence, record))),
                Err(KoshaError::Io(e))
                    if e.kind() == std::io::ErrorKind::UnexpectedEof
                        && Some(path) == files.last() =>
                {
                    break
                }
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }
}

fn invalid(message: &str) -> KoshaError {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message).into()
}
fn torn() -> KoshaError {
    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "torn WAL tail").into()
}
fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8], KoshaError> {
    if input.len() < n {
        return Err(torn());
    }
    let (value, rest) = input.split_at(n);
    *input = rest;
    Ok(value)
}
fn u32_value(input: &mut &[u8]) -> Result<u32, KoshaError> {
    Ok(u32::from_le_bytes(take(input, 4)?.try_into().unwrap()))
}
fn u64_value(input: &mut &[u8]) -> Result<u64, KoshaError> {
    Ok(u64::from_le_bytes(take(input, 8)?.try_into().unwrap()))
}
fn string(input: &mut &[u8], n: usize) -> Result<String, KoshaError> {
    String::from_utf8(take(input, n)?.to_vec()).map_err(|_| invalid("invalid UTF-8 in WAL"))
}
fn decode_file(data: &[u8]) -> Result<Vec<WalRecord>, KoshaError> {
    let mut input = data;
    if u32_value(&mut input)? != WalWriter::MAGIC {
        return Err(invalid("invalid WAL magic"));
    }
    match u32_value(&mut input)? {
        2 => {
            let len = usize::try_from(u64_value(&mut input)?)
                .map_err(|_| invalid("WAL record too large"))?;
            let crc = u32_value(&mut input)?;
            let payload = take(&mut input, len)?;
            if !input.is_empty() {
                return Err(invalid("trailing WAL bytes"));
            }
            if crc32c::crc32c(payload) != crc {
                return Err(invalid("WAL checksum mismatch"));
            }
            Ok(vec![serde_json::from_slice(payload)?])
        }
        1 => {
            // Legacy files remain readable, but incomplete records are never
            // partially replayed and unknown field types fail closed.
            let count = u32_value(&mut input)?;
            let mut records = Vec::new();
            for _ in 0..count {
                let len = u32_value(&mut input)? as usize;
                let namespace = NamespaceId(string(&mut input, len)?);
                let timestamp = u64_value(&mut input)?;
                let docs = u32_value(&mut input)?;
                let mut documents = Vec::new();
                for _ in 0..docs {
                    let len = u32_value(&mut input)? as usize;
                    let id = DocumentId(string(&mut input, len)?);
                    let count = u32_value(&mut input)?;
                    let mut fields = Vec::new();
                    for _ in 0..count {
                        let len = u32_value(&mut input)? as usize;
                        let name = string(&mut input, len)?;
                        let field_type = match take(&mut input, 1)?[0] {
                            0 => FieldType::Text,
                            1 => FieldType::Keyword,
                            2 => FieldType::Integer,
                            3 => FieldType::Float,
                            4 => FieldType::Date,
                            5 => FieldType::Boolean,
                            6 => FieldType::Vector,
                            7 => FieldType::GeoPoint,
                            _ => return Err(invalid("unknown WAL field type")),
                        };
                        let len = usize::try_from(u64_value(&mut input)?)
                            .map_err(|_| invalid("WAL field too large"))?;
                        let value = string(&mut input, len)?;
                        fields.push(Field {
                            name,
                            field_type,
                            value,
                        });
                    }
                    documents.push(Document { id, fields });
                }
                records.push(WalRecord {
                    namespace,
                    timestamp,
                    documents,
                    deleted_ids: Vec::new(),
                });
            }
            if !input.is_empty() {
                return Err(invalid("trailing legacy WAL bytes"));
            }
            Ok(records)
        }
        _ => Err(invalid("unsupported WAL version")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framed_wal_torn_tail_never_replays_partial_documents_or_deletes() {
        let dir = std::env::temp_dir().join(format!("kosha-wal-tail-{}", uuid::Uuid::new_v4()));
        let mut wal = WalWriter::new(
            Box::new(kosha_core::LocalStorage::new(dir.clone())),
            dir.clone(),
        );
        let ns = NamespaceId("ns".into());
        wal.append(
            &ns,
            &[Document {
                id: DocumentId("a".into()),
                fields: vec![Field::text("body", "hello")],
            }],
        )
        .unwrap();
        wal.append_delete(&ns, vec![DocumentId("a".into())])
            .unwrap();
        let tail = dir.join(format!("wal-{:020x}.wal", wal.last_sequence()));
        let bytes = std::fs::read(&tail).unwrap();
        assert_eq!(
            WalWriter::recover(&dir).unwrap()[1].deleted_ids,
            vec![DocumentId("a".into())]
        );
        for end in 0..bytes.len() {
            std::fs::write(&tail, &bytes[..end]).unwrap();
            let recovered = WalWriter::recover(&dir).unwrap();
            assert_eq!(recovered.len(), 1, "tail length {end}");
            assert_eq!(recovered[0].documents[0].id.0, "a");
        }
        let mut corrupt = bytes;
        *corrupt.last_mut().unwrap() ^= 1;
        std::fs::write(&tail, corrupt).unwrap();
        assert!(WalWriter::recover(&dir).is_err());
        std::fs::write(&tail, [0u8; 3]).unwrap();
        let mut restarted = WalWriter::new(
            Box::new(kosha_core::LocalStorage::new(dir.clone())),
            dir.clone(),
        );
        restarted
            .append(
                &ns,
                &[Document {
                    id: DocumentId("b".into()),
                    fields: vec![],
                }],
            )
            .unwrap();
        drop(restarted);
        let _second_restart = WalWriter::new(
            Box::new(kosha_core::LocalStorage::new(dir.clone())),
            dir.clone(),
        );
        assert_eq!(
            WalWriter::recover(&dir).unwrap().len(),
            2,
            "discarded tail must not block a later restart"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn legacy_wal_rejects_unknown_field_types_and_partial_records() {
        let mut data = Vec::new();
        for n in [WalWriter::MAGIC, 1, 1, 2] {
            data.extend_from_slice(&n.to_le_bytes());
        }
        data.extend_from_slice(b"ns");
        data.extend_from_slice(&1u64.to_le_bytes());
        for n in [1u32, 1] {
            data.extend_from_slice(&n.to_le_bytes());
        }
        data.push(b'a');
        for n in [1u32, 1] {
            data.extend_from_slice(&n.to_le_bytes());
        }
        data.push(b'f');
        let type_offset = data.len();
        data.push(0);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.push(b'v');
        assert_eq!(decode_file(&data).unwrap()[0].documents[0].id.0, "a");
        for end in 0..data.len() {
            assert!(decode_file(&data[..end]).is_err());
        }
        data[type_offset] = 255;
        assert!(decode_file(&data).is_err());
    }

    #[test]
    fn wal_write_and_recover() {
        let dir = std::env::temp_dir().join("kosha-test-wal");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let backend: Box<dyn StorageBackend> = Box::new(kosha_core::LocalStorage::new(dir.clone()));
        let mut wal = WalWriter::new(backend, dir.clone());

        let ns = NamespaceId("test-ns".into());
        let docs = vec![
            Document {
                id: DocumentId("d1".into()),
                fields: vec![Field::text("title", "hello world")],
            },
            Document {
                id: DocumentId("d2".into()),
                fields: vec![Field::keyword("status", "active")],
            },
        ];

        wal.append(&ns, &docs).unwrap();
        wal.flush_buffer().unwrap();

        // Recover
        let recovered = WalWriter::recover(&dir).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].namespace.0, "test-ns");
        assert_eq!(recovered[0].documents.len(), 2);
        assert_eq!(recovered[0].documents[0].id.0, "d1");

        // Clear and verify no more records
        wal.clear().unwrap();
        let recovered = WalWriter::recover(&dir).unwrap();
        assert!(recovered.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_multiple_batches() {
        let dir = std::env::temp_dir().join("kosha-test-wal-multi");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let backend: Box<dyn StorageBackend> = Box::new(kosha_core::LocalStorage::new(dir.clone()));
        let mut wal = WalWriter::new(backend, dir.clone());

        let ns = NamespaceId("ns".into());
        wal.append(
            &ns,
            &[Document {
                id: DocumentId("d1".into()),
                fields: vec![Field::text("t", "batch1")],
            }],
        )
        .unwrap();
        wal.append(
            &ns,
            &[Document {
                id: DocumentId("d2".into()),
                fields: vec![Field::text("t", "batch2")],
            }],
        )
        .unwrap();

        // Each append creates a separate WAL file (sequential rotation)
        wal.flush_buffer().unwrap();

        let recovered = WalWriter::recover(&dir).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].documents[0].id.0, "d1");
        assert_eq!(recovered[1].documents[0].id.0, "d2");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
