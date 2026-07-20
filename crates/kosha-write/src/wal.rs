use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use kosha_core::{
    Document, DocumentId, Field, FieldType, KoshaError, NamespaceId, StorageBackend, WalRecord,
};

/// Write-Ahead Log for durability of buffered documents.
///
/// The WAL stores document batches per namespace in sequential files.
/// On crash, the WAL is replayed to recover un-flushed documents.
///
/// WAL file format (binary LE):
///   [magic: u32 = 0x4B57414C]  ("KWAL")
///   [version: u32 = 1]
///   [record_count: u32]
///   for each record:
///     [ns_len: u32][ns_bytes]
///     [timestamp: u64]
///     [doc_count: u32]
///     for each doc:
///       [id_len: u32][id_bytes]
///       [field_count: u32]
///       for each field:
///         [name_len: u32][name_bytes]
///         [field_type: u8]
///         [val_len: u64][val_bytes]
pub struct WalWriter {
    #[allow(dead_code)]
    backend: Box<dyn StorageBackend>,
    wal_dir: PathBuf,
    current_file: String,
    seq: AtomicU64,
    buffer: Vec<u8>,
    records_in_buffer: u32,
}

impl WalWriter {
    const MAGIC: u32 = 0x4B57414C;
    const VERSION: u32 = 1;

    pub fn new(backend: Box<dyn StorageBackend>, wal_dir: PathBuf) -> Self {
        let seq = Self::next_seq(&wal_dir);
        let file = format!("wal-{:020x}.wal", seq);
        Self {
            backend,
            wal_dir,
            current_file: file,
            seq: AtomicU64::new(seq + 1),
            buffer: Vec::new(),
            records_in_buffer: 0,
        }
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
        max
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

    fn append_record(&mut self, record: &WalRecord) -> Result<(), KoshaError> {
        let ns_bytes = record.namespace.0.as_bytes();
        let doc_count = record.documents.len() as u32;

        // Write to buffer
        self.buffer
            .extend_from_slice(&(ns_bytes.len() as u32).to_le_bytes());
        self.buffer.extend_from_slice(ns_bytes);
        self.buffer
            .extend_from_slice(&record.timestamp.to_le_bytes());
        self.buffer.extend_from_slice(&doc_count.to_le_bytes());

        for doc in &record.documents {
            let id_bytes = doc.id.0.as_bytes();
            self.buffer
                .extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
            self.buffer.extend_from_slice(id_bytes);

            let field_count = doc.fields.len() as u32;
            self.buffer.extend_from_slice(&field_count.to_le_bytes());

            for field in &doc.fields {
                let name_bytes = field.name.as_bytes();
                self.buffer
                    .extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
                self.buffer.extend_from_slice(name_bytes);
                self.buffer.push(field.field_type as u8);
                let val_bytes = field.value.as_bytes();
                self.buffer
                    .extend_from_slice(&(val_bytes.len() as u64).to_le_bytes());
                self.buffer.extend_from_slice(val_bytes);
            }
        }

        self.records_in_buffer += 1;
        self.flush_buffer()?;
        Ok(())
    }

    fn flush_buffer(&mut self) -> Result<(), KoshaError> {
        if self.records_in_buffer == 0 {
            return Ok(());
        }

        let mut final_buf = Vec::new();
        final_buf.extend_from_slice(&Self::MAGIC.to_le_bytes());
        final_buf.extend_from_slice(&Self::VERSION.to_le_bytes());
        final_buf.extend_from_slice(&self.records_in_buffer.to_le_bytes());
        final_buf.extend_from_slice(&self.buffer);

        let path = self.wal_dir.join(&self.current_file);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(&path, &final_buf)?;

        self.buffer.clear();
        self.records_in_buffer = 0;

        // Rotate to next file
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        self.current_file = format!("wal-{:020x}.wal", seq);
        Ok(())
    }

    /// Remove all WAL files (called after successful flush).
    pub fn clear(&mut self) -> Result<(), KoshaError> {
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
            let data = std::fs::read(path)?;
            if data.len() < 12 {
                continue;
            }
            let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
            if magic != Self::MAGIC {
                continue;
            }
            let _version = u32::from_le_bytes(data[4..8].try_into().unwrap());
            let record_count = u32::from_le_bytes(data[8..12].try_into().unwrap());

            let mut cursor = &data[12..];
            for _ in 0..record_count {
                if cursor.len() < 4 {
                    break;
                }
                let ns_len = u32::from_le_bytes(cursor[..4].try_into().unwrap()) as usize;
                cursor = &cursor[4..];
                if cursor.len() < ns_len + 8 + 4 {
                    break;
                }
                let ns_bytes = &cursor[..ns_len];
                let namespace = NamespaceId(String::from_utf8_lossy(ns_bytes).to_string());
                cursor = &cursor[ns_len..];
                let timestamp = u64::from_le_bytes(cursor[..8].try_into().unwrap());
                cursor = &cursor[8..];
                let doc_count = u32::from_le_bytes(cursor[..4].try_into().unwrap()) as usize;
                cursor = &cursor[4..];

                let mut documents = Vec::with_capacity(doc_count);
                for _ in 0..doc_count {
                    if cursor.len() < 4 {
                        break;
                    }
                    let id_len = u32::from_le_bytes(cursor[..4].try_into().unwrap()) as usize;
                    cursor = &cursor[4..];
                    if cursor.len() < id_len + 4 {
                        break;
                    }
                    let id_bytes = &cursor[..id_len];
                    let doc_id = DocumentId(String::from_utf8_lossy(id_bytes).to_string());
                    cursor = &cursor[id_len..];
                    let field_count = u32::from_le_bytes(cursor[..4].try_into().unwrap()) as usize;
                    cursor = &cursor[4..];

                    let mut fields = Vec::with_capacity(field_count);
                    for _ in 0..field_count {
                        if cursor.len() < 4 {
                            break;
                        }
                        let name_len = u32::from_le_bytes(cursor[..4].try_into().unwrap()) as usize;
                        cursor = &cursor[4..];
                        if cursor.len() < name_len + 1 {
                            break;
                        }
                        let name_bytes = &cursor[..name_len];
                        let name = String::from_utf8_lossy(name_bytes).to_string();
                        cursor = &cursor[name_len..];
                        let field_type = match cursor[0] {
                            0 => FieldType::Text,
                            1 => FieldType::Keyword,
                            2 => FieldType::Integer,
                            3 => FieldType::Float,
                            4 => FieldType::Date,
                            5 => FieldType::Boolean,
                            6 => FieldType::Vector,
                            _ => FieldType::Text,
                        };
                        cursor = &cursor[1..];
                        if cursor.len() < 8 {
                            break;
                        }
                        let val_len = u64::from_le_bytes(cursor[..8].try_into().unwrap()) as usize;
                        cursor = &cursor[8..];
                        if cursor.len() < val_len {
                            break;
                        }
                        let val_bytes = &cursor[..val_len];
                        let value = String::from_utf8_lossy(val_bytes).to_string();
                        cursor = &cursor[val_len..];
                        fields.push(Field {
                            name,
                            field_type,
                            value,
                        });
                    }
                    documents.push(Document { id: doc_id, fields });
                }
                records.push(WalRecord {
                    namespace,
                    documents,
                    timestamp,
                });
            }
        }

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
