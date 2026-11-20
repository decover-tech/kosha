//! Segment reader: one file of a valid segment replaced by the input
//! (the first byte picks which), then opened and read every way a
//! search, delete or compaction would.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some((&pick, bytes)) = data.split_first() else {
        return;
    };
    let (template, files) = kosha_fuzz::template_segment();
    let dir = kosha_fuzz::work_segment();
    let name = &files[pick as usize % files.len()];
    std::fs::write(dir.join(name), bytes).unwrap();
    kosha_fuzz::exercise(dir);
    std::fs::copy(template.join(name), dir.join(name)).unwrap();
});
