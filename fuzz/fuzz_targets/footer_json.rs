//! Footer JSON: any input parses or errors, and a parsed footer
//! serializes to JSON that parses again. (Not byte-exact: serde_json's
//! default float parsing is not correctly rounded for extreme literals.)
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(footer) = serde_json::from_slice::<kosha_core::Footer>(data) {
        let json = serde_json::to_string(&footer).unwrap();
        let back: kosha_core::Footer = serde_json::from_str(&json).unwrap();
        assert_eq!(back.doc_count, footer.doc_count);
        assert_eq!(back.segment_id, footer.segment_id);
    }
});
