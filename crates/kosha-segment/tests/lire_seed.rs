//! Seeding a segment's vector index from existing postings (#253): the
//! writer side of LIRE compaction, below kosha-write's merge loop.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use kosha_core::{Bm25Params, DocumentId, Field, SegmentId, VectorQuantization};
use kosha_segment::{
    SegmentReader, SegmentWriter, VectorIndexBuild, VectorQuantizationConfig, VectorSeedPosting,
    I8_VECTOR_INDEX_VERSION, LIRE_ADOPT_MIN_VECTORS, RADIUS_AWARE_VECTOR_INDEX_VERSION,
};

const DIM: usize = 16;

fn temp_dir(name: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-lire-seed-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Deterministic pseudo-random value stream in `[-1, 1)`.
fn noise(seed: u64, n: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    (0..n)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
        })
        .collect()
}

/// Doc `i`'s vector: one of 24 cluster centres plus jitter.
fn vector(i: u64) -> Vec<f32> {
    let centre = noise(1_000_000 + i % 24, DIM);
    centre
        .iter()
        .zip(noise(i, DIM))
        .map(|(c, j)| c + 0.3 * j)
        .collect()
}

fn doc(i: u64) -> (DocumentId, Vec<Field>) {
    (
        DocumentId(format!("doc-{i:05}")),
        vec![
            Field::text("body", format!("document {i}")),
            Field::vector("emb", vector(i)),
        ],
    )
}

fn write(
    dir: &Path,
    ids: impl IntoIterator<Item = u64>,
    quantization: VectorQuantization,
    seeds: Vec<VectorSeedPosting>,
) -> Vec<VectorIndexBuild> {
    write_groups(dir, ids, quantization, seeds, Vec::new())
}

/// [`write`], seeded by several inputs.
fn write_groups(
    dir: &Path,
    ids: impl IntoIterator<Item = u64>,
    quantization: VectorQuantization,
    seeds: Vec<VectorSeedPosting>,
    more_seeds: Vec<Vec<VectorSeedPosting>>,
) -> Vec<VectorIndexBuild> {
    let mut w = SegmentWriter::new(SegmentId("lire".into()), dir.to_path_buf())
        .with_vector_quantization(VectorQuantizationConfig::all(quantization));
    for i in ids {
        let (id, fields) = doc(i);
        w.add_document(id, fields);
    }
    if !seeds.is_empty() {
        w.seed_vector_postings("emb", seeds);
    }
    for group in more_seeds {
        w.seed_vector_postings("emb", group);
    }
    let builds = w.vector_index_builds();
    w.finalize(Bm25Params::default()).unwrap();
    let out = builds.lock().unwrap().clone();
    out
}

/// Every posting of `dir`'s `emb` column as seeds, with doc_seq `d` mapped
/// to `map(d)` (`None`: dropped).
fn seeds_from(dir: &Path, map: impl Fn(u32) -> Option<u32>) -> Vec<VectorSeedPosting> {
    let reader = SegmentReader::open(dir.to_path_buf()).unwrap();
    let layout = reader.vector_posting_layout("emb").unwrap().unwrap();
    layout
        .postings
        .into_iter()
        .map(|p| {
            let before = p.doc_seqs.len();
            let doc_seqs: Vec<u32> = p.doc_seqs.into_iter().filter_map(&map).collect();
            VectorSeedPosting {
                centroid: p.centroid,
                dropped: doc_seqs.len() != before,
                doc_seqs,
            }
        })
        .collect()
}

fn layout_doc_seqs(dir: &Path) -> Vec<u32> {
    let reader = SegmentReader::open(dir.to_path_buf()).unwrap();
    let layout = reader.vector_posting_layout("emb").unwrap().unwrap();
    let mut all: Vec<u32> = layout
        .postings
        .iter()
        .flat_map(|p| p.doc_seqs.iter().copied())
        .collect();
    all.sort_unstable();
    all
}

fn centroids(dir: &Path) -> Vec<Vec<f32>> {
    let reader = SegmentReader::open(dir.to_path_buf()).unwrap();
    let layout = reader.vector_posting_layout("emb").unwrap().unwrap();
    layout.postings.into_iter().map(|p| p.centroid).collect()
}

#[test]
fn layout_lists_every_posting_member_once() {
    let dir = temp_dir("layout");
    let n = 2_000u32;
    write(&dir, 0..n as u64, VectorQuantization::F32, Vec::new());
    assert_eq!(layout_doc_seqs(&dir), (0..n).collect::<Vec<_>>());
    let reader = SegmentReader::open(dir.clone()).unwrap();
    let layout = reader.vector_posting_layout("emb").unwrap().unwrap();
    assert_eq!(layout.dim, DIM);
    assert_eq!(layout.version, RADIUS_AWARE_VECTOR_INDEX_VERSION);
    assert!(reader.vector_posting_layout("missing").unwrap().is_none());

    // Without the sidecar the column is served by exact scan: no layout.
    std::fs::remove_file(dir.join("vector.offsets")).unwrap();
    let reader = SegmentReader::open(dir.clone()).unwrap();
    assert!(reader.vector_posting_layout("emb").unwrap().is_none());
}

#[test]
fn seeded_merge_keeps_untouched_postings_and_covers_every_vector() {
    let input = temp_dir("input");
    let n = 3_000u64;
    write(&input, 0..n, VectorQuantization::F32, Vec::new());
    let input_centroids = centroids(&input);

    // Merge: drop every 500th doc (tombstoned), add 5 new ones after it.
    let kept: Vec<u64> = (0..n).filter(|i| i % 500 != 7).collect();
    let mut new_seq = vec![None; n as usize];
    for (seq, &old) in kept.iter().enumerate() {
        new_seq[old as usize] = Some(seq as u32);
    }
    let seeds = seeds_from(&input, |d| new_seq[d as usize]);
    let dropped_postings = seeds.iter().filter(|s| s.dropped).count();
    let output = temp_dir("output");
    let ids: Vec<u64> = kept.iter().copied().chain(n..n + 5).collect();
    let builds = write(&output, ids.clone(), VectorQuantization::F32, seeds);

    let build = &builds[0];
    assert_eq!(build.field, "emb");
    assert!(build.lire, "{build:?}");
    assert_eq!(build.seeded_postings, input_centroids.len());
    assert_eq!(build.recentred_postings, dropped_postings);
    assert_eq!(build.inserted_vectors, 5);
    assert_eq!(
        layout_doc_seqs(&output),
        (0..ids.len() as u32).collect::<Vec<_>>(),
        "every merged vector in exactly one posting"
    );

    // Postings no member left and no insert reached keep their centroid:
    // the k-means work of the input is reused, not redone. At most the 6
    // recentred postings, the 5 the new vectors landed in, and whatever
    // LIRE then split or merged change.
    let out = centroids(&output);
    let reused = input_centroids
        .iter()
        .filter(|c| {
            out.iter()
                .any(|o| o.iter().zip(c.iter()).all(|(a, b)| (a - b).abs() < 1e-6))
        })
        .count();
    let changed = build.recentred_postings
        + build.inserted_vectors
        + 2 * (build.postings_split + build.postings_merged);
    assert!(
        reused + changed >= input_centroids.len() && reused > 0,
        "only {reused} of {} input centroids survived ({build:?})",
        input_centroids.len()
    );

    // And it serves queries: every doc finds itself.
    let reader = SegmentReader::open(output.clone()).unwrap();
    for (seq, &i) in ids.iter().enumerate().step_by(97) {
        let hits = reader.knn_search(&vector(i), 1, 4).unwrap().unwrap();
        assert_eq!(hits[0].0, seq as u32, "doc {i}");
    }
}

#[test]
fn second_input_is_absorbed_into_the_largest_inputs_postings() {
    let a = temp_dir("abs-a");
    let b = temp_dir("abs-b");
    write(&a, 0..2_500, VectorQuantization::F32, Vec::new());
    write(&b, 2_500..4_000, VectorQuantization::F32, Vec::new());
    let a_postings = centroids(&a).len();
    let b_postings = centroids(&b).len();
    let out = temp_dir("abs-out");
    // b listed first: the larger input is adopted whatever the order.
    let builds = write_groups(
        &out,
        0..4_000,
        VectorQuantization::F32,
        seeds_from(&b, |d| Some(d + 2_500)),
        vec![seeds_from(&a, Some)],
    );
    let build = &builds[0];
    assert!(build.lire);
    assert_eq!(build.seeded_postings, a_postings);
    assert_eq!(build.absorbed_postings, b_postings);
    assert_eq!(build.inserted_vectors, 0);
    assert!(build.postings_split > 0, "{build:?}");
    assert_eq!(layout_doc_seqs(&out), (0..4_000).collect::<Vec<_>>());
    // Not kept side by side: about as many postings as a from-scratch
    // clustering of the same vectors, not the two inputs' sum.
    let rebuilt = temp_dir("abs-rebuilt");
    write(&rebuilt, 0..4_000, VectorQuantization::F32, Vec::new());
    let (merged, scratch) = (centroids(&out).len(), centroids(&rebuilt).len());
    assert!(
        merged * 10 <= scratch * 13,
        "{merged} postings vs {scratch} from scratch"
    );
}

#[test]
fn i8_column_requantizes_from_exact_vectors() {
    let input = temp_dir("i8-in");
    let n = 2_000u64;
    write(&input, 0..n, VectorQuantization::I8, Vec::new());
    let seeds = seeds_from(&input, Some);
    let output = temp_dir("i8-out");
    let builds = write(&output, 0..n, VectorQuantization::I8, seeds);
    assert!(builds[0].lire);
    let reader = SegmentReader::open(output.clone()).unwrap();
    let layout = reader.vector_posting_layout("emb").unwrap().unwrap();
    assert_eq!(layout.version, I8_VECTOR_INDEX_VERSION);
    // Same postings as the input (nothing changed), and requantizing the
    // same exact vectors writes byte-identical entries.
    assert_eq!(
        std::fs::read(input.join("vector.idx")).unwrap(),
        std::fs::read(output.join("vector.idx")).unwrap()
    );
    // i8 scores are approximate (this path does not rescore): probing
    // every posting, a doc is among its own top 5.
    for i in (0..n).step_by(131) {
        let hits = reader.knn_search(&vector(i), 5, 1000).unwrap().unwrap();
        assert!(hits.iter().any(|h| h.0 == i as u32), "doc {i}: {hits:?}");
    }

    // An f32 input seeds an i8 output (and the other way round): the
    // seed is only structure.
    let f32_in = temp_dir("f32-in");
    write(&f32_in, 0..n, VectorQuantization::F32, Vec::new());
    let mixed = temp_dir("mixed-out");
    let builds = write(
        &mixed,
        0..n,
        VectorQuantization::I8,
        seeds_from(&f32_in, Some),
    );
    assert!(builds[0].lire);
    let reader = SegmentReader::open(mixed).unwrap();
    assert_eq!(
        reader
            .vector_posting_layout("emb")
            .unwrap()
            .unwrap()
            .version,
        I8_VECTOR_INDEX_VERSION
    );
}

#[test]
fn falls_back_to_clustering_when_seeds_cannot_be_used() {
    let input = temp_dir("fallback-in");
    write(&input, 0..2_000, VectorQuantization::F32, Vec::new());

    // Seeds cover under half of the column: cluster from scratch.
    let seeds = seeds_from(&input, |d| (d < 900).then_some(d));
    let out = temp_dir("fallback-out");
    let builds = write(&out, 0..2_000, VectorQuantization::F32, seeds);
    assert!(!builds[0].lire, "{:?}", builds[0]);
    assert_eq!(layout_doc_seqs(&out), (0..2_000).collect::<Vec<_>>());

    // A seed of another dimension: cluster from scratch.
    let mut seeds = seeds_from(&input, Some);
    seeds[0].centroid.push(0.0);
    let out = temp_dir("fallback-dim");
    let builds = write(&out, 0..2_000, VectorQuantization::F32, seeds);
    assert!(!builds[0].lire);

    // No seeds: the plain flush path.
    let out = temp_dir("fallback-none");
    let builds = write(&out, 0..2_000, VectorQuantization::F32, Vec::new());
    assert!(!builds[0].lire);
}

#[test]
fn seeds_naming_unknown_or_duplicate_doc_seqs_are_tolerated() {
    let input = temp_dir("dup-in");
    write(&input, 0..1_500, VectorQuantization::F32, Vec::new());
    let mut seeds = seeds_from(&input, Some);
    // Doc 0 claimed twice, plus a doc_seq the output does not have.
    let first = seeds[0].doc_seqs[0];
    seeds[1].doc_seqs.push(first);
    seeds[2].doc_seqs.push(99_999);
    let out = temp_dir("dup-out");
    let builds = write(&out, 0..1_500, VectorQuantization::F32, seeds);
    assert!(builds[0].lire);
    let all = layout_doc_seqs(&out);
    assert_eq!(all, (0..1_500).collect::<Vec<_>>());
    assert_eq!(all.iter().collect::<HashSet<_>>().len(), all.len());
}

#[test]
fn adopt_threshold_is_eight_default_postings() {
    assert_eq!(LIRE_ADOPT_MIN_VECTORS, 1024);
}
