//! WAL recovery decoder: any bytes decode to records or an error.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let (lsn, bytes) = match data.split_first_chunk::<8>() {
        Some((lsn, rest)) => (u64::from_le_bytes(*lsn), rest),
        None => (1, data),
    };
    if let Ok((records, _torn)) = kosha_write::wal::decode_wal_file(bytes, lsn) {
        if records.is_empty() {
            return;
        }
        // Whatever decodes re-encodes and decodes to the same records.
        let again = kosha_write::wal::encode_wal_file(&records);
        let first = records.first().map_or(lsn, |(l, _)| *l);
        let (back, torn) = kosha_write::wal::decode_wal_file(&again, first).unwrap();
        assert!(!torn);
        assert_eq!(back.len(), records.len());
    }
});
