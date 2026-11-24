//! Property tests for the WAL codec (#257): any record round-trips through
//! the v3 frame encoding, and any mutation or truncation of a valid file
//! decodes to an error or a prefix of the original records (a torn tail),
//! never a panic and never a record that was not written.
//!
//! 64 cases per property by default to keep `cargo test` fast; set
//! `PROPTEST_CASES` for a deeper run.

use kosha_core::{Document, DocumentId, Field, FieldType, NamespaceId, WalRecord};
use kosha_write::wal::{decode_wal_file, encode_wal_file};
use proptest::prelude::*;

fn config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    ProptestConfig {
        cases,
        // Failures print their minimal input; no regression files.
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn field() -> impl Strategy<Value = Field> {
    let name = "[a-z_]{1,8}";
    prop_oneof![
        (name, ".{0,24}").prop_map(|(n, v)| Field::text(n, v)),
        (name, "[ -~]{0,12}").prop_map(|(n, v)| Field::keyword(n, v)),
        (name, any::<i64>()).prop_map(|(n, v)| Field::integer(n, v)),
        (name, any::<bool>()).prop_map(|(n, v)| Field::boolean(n, v)),
        // Finite values: what `Field::vector` stores as binary f32.
        (name, prop::collection::vec(-1e6f32..1e6f32, 0..8)).prop_map(|(n, v)| Field::vector(n, v)),
        // A vector-typed value that is not canonical JSON stays a string.
        (name, "\\[[0-9., ]{0,10}\\]").prop_map(|(n, v)| Field {
            name: n,
            field_type: FieldType::Vector,
            value: v,
        }),
    ]
}

fn record() -> impl Strategy<Value = WalRecord> {
    (
        "[a-z/]{1,12}",
        any::<u64>(),
        prop::collection::vec(
            ("[a-z0-9-]{1,10}", prop::collection::vec(field(), 0..4)),
            0..4,
        ),
        prop::collection::vec("[a-z0-9]{1,10}", 0..3),
    )
        .prop_map(|(ns, timestamp, docs, deleted)| WalRecord {
            namespace: NamespaceId(ns),
            timestamp,
            documents: docs
                .into_iter()
                .map(|(id, fields)| Document {
                    id: DocumentId(id),
                    fields,
                })
                .collect(),
            deleted_ids: deleted.into_iter().map(DocumentId).collect(),
        })
}

/// `(first LSN, records)` with strictly increasing LSNs.
fn file() -> impl Strategy<Value = (u64, Vec<(u64, WalRecord)>)> {
    (
        1u64..1 << 40,
        prop::collection::vec((1u64..1000, record()), 1..5),
    )
        .prop_map(|(first, records)| {
            let mut lsn = first;
            let records = records
                .into_iter()
                .map(|(gap, record)| {
                    let at = lsn;
                    lsn += gap;
                    (at, record)
                })
                .collect();
            (first, records)
        })
}

fn json(records: &[(u64, WalRecord)]) -> Vec<(u64, serde_json::Value)> {
    records
        .iter()
        .map(|(lsn, r)| (*lsn, serde_json::to_value(r).unwrap()))
        .collect()
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn records_round_trip((first, records) in file()) {
        let bytes = encode_wal_file(&records);
        let (decoded, torn) = decode_wal_file(&bytes, first).unwrap();
        prop_assert!(!torn);
        prop_assert_eq!(json(&decoded), json(&records));
    }

    #[test]
    fn mutated_files_error_or_decode_a_prefix(
        (first, records) in file(),
        flips in prop::collection::vec((any::<prop::sample::Index>(), 1u8..=255), 1..4),
    ) {
        let mut bytes = encode_wal_file(&records);
        for (at, mask) in &flips {
            let i = at.index(bytes.len());
            bytes[i] ^= mask;
        }
        if let Ok((decoded, _)) = decode_wal_file(&bytes, first) {
            // A flip inside a length field can make a frame look torn;
            // whatever decodes must be records that were written.
            let decoded = json(&decoded);
            prop_assert!(decoded.len() <= records.len());
            prop_assert_eq!(&decoded[..], &json(&records)[..decoded.len()]);
        }
    }

    #[test]
    fn truncated_files_decode_the_complete_frames((first, records) in file(), cut in any::<prop::sample::Index>()) {
        let bytes = encode_wal_file(&records);
        let len = cut.index(bytes.len());
        let (decoded, torn) = decode_wal_file(&bytes[..len], first).unwrap();
        // A cut on a frame boundary is a shorter, untorn file.
        prop_assert!(torn || decoded.len() < records.len());
        let decoded = json(&decoded);
        prop_assert_eq!(&decoded[..], &json(&records)[..decoded.len()]);
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..256), lsn in any::<u64>()) {
        let _ = decode_wal_file(&bytes, lsn);
        // And behind a valid v3 magic and version, so the frame parser runs.
        let mut framed = 0x4B57414Cu32.to_le_bytes().to_vec();
        framed.extend_from_slice(&3u32.to_le_bytes());
        framed.extend_from_slice(&bytes);
        let _ = decode_wal_file(&framed, lsn);
    }
}
