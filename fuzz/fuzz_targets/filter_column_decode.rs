//! Filter column decoder: the first byte picks the expected kind, the next
//! four the segment's doc count. A column that decodes re-encodes to bytes
//! that decode to the same column.
#![no_main]
use kosha_core::FilterColumnKind;
use kosha_segment::filter_columns::{decode_column, encode_column};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 5 {
        return;
    }
    let kind = match data[0] % 3 {
        0 => FilterColumnKind::String,
        1 => FilterColumnKind::Integer,
        _ => FilterColumnKind::Float,
    };
    let doc_count = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
    if let Ok(column) = decode_column(&data[5..], kind, doc_count, "fuzz") {
        let again = encode_column(&column, doc_count);
        let back = decode_column(&again, kind, doc_count, "fuzz").unwrap();
        assert_eq!(back, column);
    }
});
