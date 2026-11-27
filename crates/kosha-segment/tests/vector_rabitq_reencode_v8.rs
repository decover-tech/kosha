//! `rewrite_vector_column_rabitq` on the quantile format (idx v6 -> v8),
//! written with `KOSHA_VECTOR_WRITE_VERSION=5`. The write version is read
//! once per process, so this is its own test binary; v3 -> v5 (the
//! default write version) is covered in `vector_rabitq.rs`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use kosha_core::{Bm25Params, DocumentId, Field, SegmentId, VectorQuantization};
use kosha_segment::{
    SegmentReader, SegmentWriter, VectorQuantizationConfig, VectorStoreMode,
    QUANTILE_VECTOR_INDEX_VERSION, RABITQ_QUANTILE_VECTOR_INDEX_VERSION,
};

const DIM: usize = 64;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "kosha-vector-rabitq-v8-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn vector(seed: u64) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    (0..DIM)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
        })
        .collect()
}

fn write(dir: &Path, quantization: VectorQuantizationConfig) {
    let mut w = SegmentWriter::new(SegmentId("v8".into()), dir.to_path_buf())
        .with_vector_store_mode(VectorStoreMode::Sidecar)
        .with_vector_quantization(quantization);
    for i in 0..600u64 {
        w.add_document(
            DocumentId(format!("doc-{i:05}")),
            vec![Field::vector("emb", vector(i))],
        );
    }
    w.finalize(Bm25Params::default()).unwrap();
}

fn files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_file())
        .map(|p| {
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read(&p).unwrap(),
            )
        })
        .collect()
}

#[test]
fn rewrite_vector_column_rabitq_matches_a_rabitq_write_on_the_quantile_format() {
    std::env::set_var("KOSHA_VECTOR_WRITE_VERSION", "5");
    let (f32_dir, rq_dir) = (temp_dir("f32"), temp_dir("rq"));
    write(&f32_dir, VectorQuantizationConfig::default());
    write(
        &rq_dir,
        VectorQuantizationConfig::default().with_field("emb", VectorQuantization::RaBitQ),
    );
    let version = |idx: &[u8]| u32::from_le_bytes(idx[4..8].try_into().unwrap());
    assert_eq!(
        version(&files(&f32_dir)["vector.idx"]),
        QUANTILE_VECTOR_INDEX_VERSION
    );
    SegmentReader::rewrite_vector_column_rabitq(&f32_dir, "emb").unwrap();
    let (after, want) = (files(&f32_dir), files(&rq_dir));
    assert_eq!(
        version(&after["vector.idx"]),
        RABITQ_QUANTILE_VECTOR_INDEX_VERSION
    );
    for name in ["vector.idx", "vector.offsets"] {
        assert_eq!(after[name], want[name], "{name}");
    }
    std::fs::remove_dir_all(&f32_dir).ok();
    std::fs::remove_dir_all(&rq_dir).ok();
}
