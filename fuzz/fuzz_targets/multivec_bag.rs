//! Late-interaction decoders: `parse_bag` on the input as text, and the
//! bag/IVF file readers on the input split into the two files (the first
//! two bytes give the split, the next eight the footer's dim and docs).
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        if let Some(bag) = kosha_segment::multivec::parse_bag(text) {
            assert!(!bag.is_empty() && bag.iter().all(|t| t.len() == bag[0].len()));
        }
    }
    if data.len() < 10 {
        return;
    }
    let at = (u16::from_le_bytes([data[0], data[1]]) as usize).min(data.len() - 10);
    let dim = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
    let docs = u32::from_le_bytes([data[6], data[7], data[8], data[9]]);
    let (bags, ivf) = data[10..].split_at(at);
    let dir = kosha_fuzz::work_segment();
    std::fs::write(dir.join(kosha_segment::multivec::bags_file(7)), bags).unwrap();
    std::fs::write(dir.join(kosha_segment::multivec::ivf_file(7)), ivf).unwrap();
    let _ = kosha_segment::multivec::open_multivec_files(dir, 7, dim, docs);
});
