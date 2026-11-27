//! RaBitQ 1-bit vector postings (vector index version 5).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use kosha_core::{Bm25Params, DocumentId, Field, SegmentId, VectorQuantization};
use kosha_segment::{
    SegmentReader, SegmentWriter, VectorQuantizationConfig, VectorStoreMode,
    RABITQ_VECTOR_INDEX_VERSION, RADIUS_AWARE_VECTOR_INDEX_VERSION,
};

const DIM: usize = 64;

fn temp_dir(name: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-vector-rabitq-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Deterministic pseudo-random vector in `[-1, 1)^dim`.
fn vector(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    (0..dim)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
        })
        .collect()
}

fn docs(n: usize) -> Vec<(String, Vec<Field>)> {
    (0..n)
        .map(|i| {
            (
                format!("doc-{i:05}"),
                vec![
                    Field::text("body", format!("document {i}")),
                    Field::vector("emb", vector(i as u64, DIM)),
                    Field::vector("title_emb", vector(1_000_000 + i as u64, 8)),
                ],
            )
        })
        .collect()
}

fn write(
    dir: &Path,
    mode: VectorStoreMode,
    quantization: VectorQuantizationConfig,
    docs: Vec<(String, Vec<Field>)>,
) -> kosha_core::Footer {
    let mut w = SegmentWriter::new(SegmentId("quant".into()), dir.to_path_buf())
        .with_vector_store_mode(mode)
        .with_vector_quantization(quantization);
    for (id, fields) in docs {
        w.add_document(DocumentId(id), fields);
    }
    w.finalize(Bm25Params::default()).unwrap()
}

fn rabitq_emb() -> VectorQuantizationConfig {
    VectorQuantizationConfig::default().with_field("emb", VectorQuantization::RaBitQ)
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

fn header_version(idx: &[u8]) -> u32 {
    u32::from_le_bytes(idx[4..8].try_into().unwrap())
}

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let dot: f64 = a.iter().zip(b).map(|(x, y)| *x as f64 * *y as f64).sum();
    let na: f64 = a.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    let nb: f64 = b.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    dot / (na * nb)
}

fn brute_force(n: usize, query: &[f32], k: usize) -> Vec<(u32, f64)> {
    let mut scored: Vec<(u32, f64)> = (0..n)
        .map(|i| (i as u32, cosine(query, &vector(i as u64, DIM))))
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    scored.truncate(k);
    scored
}

/// Exhaustive probe: every posting is read.
fn knn(reader: &SegmentReader, query: &[f32], k: usize) -> Vec<(u32, f64)> {
    reader
        .knn_search_filtered("emb", query, k, 10_000, None)
        .expect("column has a lazy index")
        .unwrap()
}

/// Budgeted probe with no cutoff: every posting is read.
fn knn_within(reader: &SegmentReader, query: &[f32], k: usize) -> Vec<(u32, f64)> {
    reader
        .knn_search_within_filtered("emb", query, k, f32::MAX, usize::MAX, None)
        .expect("column has a lazy index")
        .unwrap()
        .0
}

fn recall(got: &[(u32, f64)], want: &[(u32, f64)]) -> f64 {
    let want: Vec<u32> = want.iter().map(|(id, _)| *id).collect();
    got.iter().filter(|(id, _)| want.contains(id)).count() as f64 / want.len() as f64
}

#[test]
fn rabitq_column_is_one_bit_per_dimension_and_tagged_in_the_footer() {
    let n = 600;
    let f32_dir = temp_dir("f32");
    let rq_dir = temp_dir("rq");
    write(
        &f32_dir,
        VectorStoreMode::Sidecar,
        VectorQuantizationConfig::default(),
        docs(n),
    );
    let footer = write(&rq_dir, VectorStoreMode::Sidecar, rabitq_emb(), docs(n));
    let meta = |name: &str| {
        footer
            .vector_fields
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.name == name)
            .unwrap()
            .quantization
    };
    assert_eq!(meta("emb"), VectorQuantization::RaBitQ);
    assert_eq!(meta("title_emb"), VectorQuantization::F32);
    let footer_json = std::fs::read_to_string(rq_dir.join("footer.json")).unwrap();
    assert!(
        footer_json.contains("\"quantization\": \"rabitq\""),
        "{footer_json}"
    );

    let (f32_files, rq_files) = (files(&f32_dir), files(&rq_dir));
    assert_eq!(
        header_version(&f32_files["vector.idx"]),
        RADIUS_AWARE_VECTOR_INDEX_VERSION
    );
    assert_eq!(
        header_version(&rq_files["vector.idx"]),
        RABITQ_VECTOR_INDEX_VERSION
    );
    // Entry bytes: [id][f32 x 64] = 260 vs [id][norm][ip][64 bits] = 20.
    let entries = |idx: &[u8]| (idx.len() - 16) / n;
    assert_eq!(entries(&f32_files["vector.idx"]), 4 + 4 * DIM);
    assert_eq!(entries(&rq_files["vector.idx"]), 4 + 4 + 4 + DIM / 8);
    for name in [
        "vector.1.idx",
        "vector.1.offsets",
        "doc_vectors.bin",
        "doc_store.bin",
    ] {
        assert_eq!(f32_files[name], rq_files[name], "{name}");
    }
    // Centroids and radii stay f32.
    assert_eq!(
        f32_files["vector.offsets"].len(),
        rq_files["vector.offsets"].len()
    );
}

/// Embedding-like data: `clusters` centres, members scattered around
/// them. 1-bit codes are meant for this regime (hundreds of dimensions,
/// neighbours sharing a posting), not for uniform noise in 64 dimensions,
/// where every score is near zero and no estimate separates them.
struct Clustered {
    dim: usize,
    noise: f32,
    centres: Vec<Vec<f32>>,
    data: Vec<Vec<f32>>,
}

impl Clustered {
    fn new(n: usize, dim: usize, clusters: u64, noise: f32) -> Self {
        let centres: Vec<Vec<f32>> = (0..clusters).map(|c| vector(9_000_000 + c, dim)).collect();
        let data = (0..n)
            .map(|i| {
                Self::around(
                    &centres[i % clusters as usize],
                    &vector(i as u64, dim),
                    noise,
                )
            })
            .collect();
        Self {
            dim,
            noise,
            centres,
            data,
        }
    }

    fn around(centre: &[f32], e: &[f32], noise: f32) -> Vec<f32> {
        centre.iter().zip(e).map(|(c, x)| c + noise * x).collect()
    }

    fn write(&self, dir: &Path) {
        let mut w = SegmentWriter::new(SegmentId("clustered".into()), dir.to_path_buf())
            .with_vector_quantization(rabitq_emb());
        for (i, v) in self.data.iter().enumerate() {
            w.add_document(
                DocumentId(format!("doc-{i:05}")),
                vec![Field::vector("emb", v.clone())],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();
    }

    fn query(&self, q: u64) -> Vec<f32> {
        let centre = &self.centres[(q * 7) as usize % self.centres.len()];
        Self::around(centre, &vector(777_000 + q, self.dim), self.noise)
    }

    fn exact(&self, query: &[f32], id: u32) -> f64 {
        cosine(query, &self.data[id as usize])
    }

    /// Mean recall@10 of an exhaustive probe over `queries` queries;
    /// `check(score, exact)` sees every hit.
    fn recall(&self, reader: &SegmentReader, queries: u64, check: impl Fn(f64, f64)) -> f64 {
        let mut total = 0.0;
        for q in 0..queries {
            let query = self.query(q);
            let mut want: Vec<(u32, f64)> = (0..self.data.len() as u32)
                .map(|id| (id, self.exact(&query, id)))
                .collect();
            want.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            want.truncate(10);
            for probe in [knn, knn_within] {
                let got = probe(reader, &query, 10);
                assert_eq!(got.len(), 10);
                for (id, score) in &got {
                    check(*score, self.exact(&query, *id));
                }
                total += recall(&got, &want) / 2.0;
            }
        }
        total / queries as f64
    }
}

#[test]
fn rescoring_restores_exact_scores_and_recall() {
    let set = Clustered::new(3000, 256, 30, 0.6);
    let dir = temp_dir("recall");
    set.write(&dir);
    let rescored = SegmentReader::open(dir.clone()).unwrap();
    let recall_rescored = set.recall(&rescored, 10, |score, exact| {
        assert!((score - exact).abs() < 1e-5, "{score} vs {exact}");
    });
    assert!(
        recall_rescored >= 0.99,
        "rescored recall@10 {recall_rescored}"
    );

    // Estimates alone land near the exact score but cannot order a
    // cluster's members, which all score alike: rescoring does that.
    let raw = SegmentReader::open(dir.clone())
        .unwrap()
        .with_vector_rescore_factor(0);
    set.recall(&raw, 10, |score, exact| {
        assert!((score - exact).abs() < 0.1, "estimate {score} vs {exact}");
    });
}

/// The error bounds, not the fixed `k * factor` list, decide how much is
/// rescored: at factor 2 the fixed list is 20 candidates, far fewer than
/// the ~100 cluster members the estimates cannot tell apart, yet recall
/// stays exact because the bounds keep all of them in the shortlist.
#[test]
fn error_bounds_widen_a_short_rescoring_list() {
    let set = Clustered::new(3000, 256, 30, 0.6);
    let dir = temp_dir("bounds");
    set.write(&dir);
    let reader = SegmentReader::open(dir.clone())
        .unwrap()
        .with_vector_rescore_factor(2);
    let recall = set.recall(&reader, 10, |score, exact| {
        assert!((score - exact).abs() < 1e-5);
    });
    assert!(recall >= 0.99, "factor-2 recall@10 {recall}");
}

/// Uniform noise is the worst case: the bounds rule almost nothing out,
/// so the shortlist runs into its cap and recall falls short of exact —
/// but it is still a full-precision rescoring of the best candidates, and
/// every returned score is exact.
#[test]
fn uniform_noise_hits_the_shortlist_cap_but_scores_stay_exact() {
    let n = 2000;
    let dir = temp_dir("uniform");
    write(&dir, VectorStoreMode::Sidecar, rabitq_emb(), docs(n));
    let reader = SegmentReader::open(dir.clone()).unwrap();
    let mut total = 0.0;
    for q in 0..10 {
        let query = vector(500_000 + q, DIM);
        let got = knn(&reader, &query, 10);
        for (id, score) in &got {
            let exact = cosine(&query, &vector(*id as u64, DIM));
            assert!((score - exact).abs() < 1e-5, "{score} vs {exact}");
        }
        total += recall(&got, &brute_force(n, &query, 10));
    }
    assert!(total / 10.0 >= 0.8, "uniform recall@10 {}", total / 10.0);
}

#[test]
fn filtered_probe_and_exact_path_work_on_rabitq_columns() {
    let n = 500;
    let dir = temp_dir("filtered");
    write(&dir, VectorStoreMode::Sidecar, rabitq_emb(), docs(n));
    let reader = SegmentReader::open(dir.clone()).unwrap();
    let query = vector(77_777, DIM);
    let even = |id: u32| id.is_multiple_of(2);
    let got = reader
        .knn_search_within_filtered("emb", &query, 5, f32::MAX, usize::MAX, Some(&even))
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(got.len(), 5);
    assert!(got.iter().all(|(id, _)| id.is_multiple_of(2)));

    let candidates: Vec<u32> = (0..n as u32).filter(|id| id.is_multiple_of(2)).collect();
    let exact = reader
        .knn_exact_filtered("emb", &query, 5, &candidates)
        .unwrap()
        .unwrap();
    assert_eq!(
        exact.hits.iter().map(|h| h.0).collect::<Vec<_>>(),
        got.iter().map(|h| h.0).collect::<Vec<_>>()
    );
}

#[test]
fn inline_vector_segments_score_with_rabitq_estimates() {
    // No `doc_vectors.bin` to rescore from: the estimates stand.
    let n = 400;
    let dir = temp_dir("inline");
    write(&dir, VectorStoreMode::Inline, rabitq_emb(), docs(n));
    assert!(!dir.join("doc_vectors.bin").exists());
    let reader = SegmentReader::open(dir.clone()).unwrap();
    let query = vector(31_337, DIM);
    let got = knn(&reader, &query, 10);
    assert_eq!(got.len(), 10);
    assert!(got.windows(2).all(|w| w[0].1 >= w[1].1));
    for (id, score) in &got {
        let exact = cosine(&query, &vector(*id as u64, DIM));
        assert!((score - exact).abs() < 0.35, "{score} vs {exact}");
    }
}

#[test]
fn rabitq_column_without_its_sidecar_is_degraded_not_misread() {
    // Codes are residuals from centroids that live in `vector.offsets`:
    // without it there is nothing to decode against.
    let dir = temp_dir("eager");
    write(&dir, VectorStoreMode::Sidecar, rabitq_emb(), docs(300));
    std::fs::remove_file(dir.join("vector.offsets")).unwrap();
    let reader = SegmentReader::open(dir.clone()).unwrap();
    let index = reader.vector_field("emb").unwrap().unwrap();
    assert!(!index.has_lazy_index());
    assert!(index.flat().vectors.is_empty());
    assert!(index.degraded());
}

#[test]
fn posting_cache_holds_rabitq_entries() {
    let n = 1000;
    let f32_dir = temp_dir("cache-f32");
    let rq_dir = temp_dir("cache-rq");
    write(
        &f32_dir,
        VectorStoreMode::Sidecar,
        VectorQuantizationConfig::default(),
        docs(n),
    );
    write(&rq_dir, VectorStoreMode::Sidecar, rabitq_emb(), docs(n));
    let warm = |dir: &Path| {
        let reader = SegmentReader::open(dir.to_path_buf()).unwrap();
        let query = vector(4242, DIM);
        knn(&reader, &query, 10);
        knn(&reader, &query, 10);
        reader.vector_cache_resident_bytes()
    };
    let (f32_bytes, rq_bytes) = (warm(&f32_dir), warm(&rq_dir));
    assert!(rq_bytes > 0);
    assert!(
        f32_bytes >= 8 * rq_bytes,
        "f32 cache {f32_bytes} B vs rabitq cache {rq_bytes} B"
    );
}

#[test]
fn rabitq_env_values_parse() {
    let config = VectorQuantizationConfig::parse(Some(" RaBitQ "), Some("a:f32,b:rabitq"));
    assert_eq!(config.default, VectorQuantization::RaBitQ);
    assert_eq!(config.for_field("a"), VectorQuantization::F32);
    assert_eq!(config.for_field("b"), VectorQuantization::RaBitQ);
    assert_eq!(VectorQuantization::RaBitQ.as_str(), "rabitq");
}

/// Compaction seeds its output's index from the input's posting layout
/// (#253), which reads ids out of `vector.idx` at the entry size of its
/// version, and re-adds vectors from `doc_vectors.bin` / the doc store —
/// never from the codes. A RaBitQ segment therefore compacts losslessly.
#[test]
fn compacting_a_rabitq_segment_rebuilds_from_full_precision_vectors() {
    for mode in [VectorStoreMode::Sidecar, VectorStoreMode::Inline] {
        let n = 300;
        let in_dir = temp_dir("compact-in");
        write(&in_dir, mode, rabitq_emb(), docs(n));
        let reader = SegmentReader::open(in_dir.clone()).unwrap();
        let layout = reader.vector_posting_layout("emb").unwrap().unwrap();
        assert_eq!(layout.version, RABITQ_VECTOR_INDEX_VERSION);
        let mut seqs: Vec<u32> = layout
            .postings
            .iter()
            .flat_map(|p| p.doc_seqs.iter().copied())
            .collect();
        seqs.sort_unstable();
        assert_eq!(seqs, (0..n as u32).collect::<Vec<_>>());

        let rewritten_dir = temp_dir("compact-out");
        let mut w = SegmentWriter::new(SegmentId("quant".into()), rewritten_dir.clone())
            .with_vector_store_mode(mode)
            .with_vector_quantization(VectorQuantizationConfig::default());
        for record in reader.iter_doc_records() {
            let record = record.unwrap();
            let i: u64 = record.doc_id.0[4..].parse().unwrap();
            let emb = record.fields.iter().find(|f| f.name == "emb").unwrap();
            let v: Vec<f32> = serde_json::from_str(&emb.value).unwrap();
            assert_eq!(
                v,
                vector(i, DIM),
                "{mode:?}: doc {i} keeps its exact vector"
            );
            w.add_document(record.doc_id, record.fields);
        }
        w.finalize(Bm25Params::default()).unwrap();
        let direct_dir = temp_dir("compact-direct");
        write(
            &direct_dir,
            mode,
            VectorQuantizationConfig::default(),
            docs(n),
        );
        let (rewritten, direct) = (files(&rewritten_dir), files(&direct_dir));
        for name in ["vector.idx", "vector.offsets"] {
            assert_eq!(rewritten[name], direct[name], "{mode:?}: {name}");
        }
    }
}

/// `rewrite_vector_column_rabitq` turns an f32 column into exactly the
/// column the writer produces with RaBitQ from the same documents: same
/// postings, centroids, radii and codes, byte for byte, and only the
/// column's two files and the footer's quantization change.
#[test]
fn rewrite_vector_column_rabitq_matches_a_rabitq_write() {
    let n = 600;
    let f32_dir = temp_dir("reencode-f32");
    let rq_dir = temp_dir("reencode-rq");
    write(
        &f32_dir,
        VectorStoreMode::Sidecar,
        VectorQuantizationConfig::default(),
        docs(n),
    );
    write(&rq_dir, VectorStoreMode::Sidecar, rabitq_emb(), docs(n));
    let before = files(&f32_dir);

    let footer = SegmentReader::rewrite_vector_column_rabitq(&f32_dir, "emb").unwrap();
    let (after, want) = (files(&f32_dir), files(&rq_dir));
    for name in ["vector.idx", "vector.offsets"] {
        assert_eq!(after[name], want[name], "{name}");
        assert_ne!(after[name], before[name], "{name}");
    }
    for (name, bytes) in &before {
        if !["vector.idx", "vector.offsets", "footer.json"].contains(&name.as_str()) {
            assert_eq!(&after[name], bytes, "{name} must not change");
        }
    }
    assert_eq!(after.len(), before.len(), "no temp file left behind");
    let quantization = |name: &str| {
        footer
            .vector_fields
            .iter()
            .flatten()
            .find(|m| m.name == name)
            .unwrap()
            .quantization
    };
    assert_eq!(quantization("emb"), VectorQuantization::RaBitQ);
    assert_eq!(quantization("title_emb"), VectorQuantization::F32);
    assert_eq!(
        serde_json::to_value(SegmentReader::read_footer(&f32_dir).unwrap()).unwrap(),
        serde_json::to_value(&footer).unwrap()
    );

    // The rewritten segment searches like the written one.
    let reencoded = SegmentReader::open(f32_dir.clone()).unwrap();
    let written = SegmentReader::open(rq_dir.clone()).unwrap();
    for q in 0..5u64 {
        let query = vector(7_000 + q, DIM);
        assert_eq!(knn(&reencoded, &query, 10), knn(&written, &query, 10));
    }

    // Already RaBitQ, or no exact vectors to rescore from: refused, untouched.
    let again = SegmentReader::rewrite_vector_column_rabitq(&f32_dir, "emb");
    assert!(again.is_err());
    assert_eq!(files(&f32_dir), after);
    let inline_dir = temp_dir("reencode-inline");
    write(
        &inline_dir,
        VectorStoreMode::Inline,
        VectorQuantizationConfig::default(),
        docs(n),
    );
    let inline = files(&inline_dir);
    assert!(SegmentReader::rewrite_vector_column_rabitq(&inline_dir, "emb").is_err());
    assert_eq!(files(&inline_dir), inline);
}
