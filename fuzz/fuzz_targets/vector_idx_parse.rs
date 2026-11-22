//! Vector index parsers: the input is a `vector.idx` and a
//! `vector.offsets`, split at the length in its first two bytes.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some((len, rest)) = data.split_first_chunk::<2>() else {
        return;
    };
    let at = (u16::from_le_bytes(*len) as usize).min(rest.len());
    let (idx, offsets) = rest.split_at(at);
    let _ = kosha_segment::parse_vector_index_files(idx, offsets);
});
