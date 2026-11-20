//! Late-interaction (ColBERT-style) multi-vector fields (#205): storage
//! layout, MaxSim against a brute-force reference, candidate-generation
//! recall, and that segments without bags are unchanged.

use std::path::{Path, PathBuf};

use kosha_core::{Bm25Params, DocumentId, Field, FieldType, Footer, KoshaError};
use kosha_segment::multivec::{bags_file, ivf_file, multivec_files};
use kosha_segment::{LateInteractionParams, SegmentReader, SegmentWriter};

const DIM: usize = 16;

/// Deterministic xorshift, so fixtures are identical on every run.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> f32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        ((self.0 >> 11) as f64 / (1u64 << 53) as f64) as f32 * 2.0 - 1.0
    }
    fn vector(&mut self, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| self.next()).collect()
    }
}

/// `docs` bags of 3..=12 tokens, each token near one of the document's two
/// topics (out of `topics`), so the token space has real cluster structure.
fn fixture(seed: u64, docs: usize, topics: usize) -> (Vec<Vec<Vec<f32>>>, Vec<Vec<f32>>) {
    let mut rng = Rng(seed);
    let centers: Vec<Vec<f32>> = (0..topics).map(|_| rng.vector(DIM)).collect();
    let bags = (0..docs)
        .map(|_| {
            let picks = [
                ((rng.next() + 1.0) * 0.5 * topics as f32) as usize % topics,
                ((rng.next() + 1.0) * 0.5 * topics as f32) as usize % topics,
            ];
            let len = 3 + ((rng.next() + 1.0) * 5.0) as usize;
            (0..len)
                .map(|t| {
                    let c = &centers[picks[t % 2]];
                    c.iter().map(|x| x + 0.35 * rng.next()).collect()
                })
                .collect()
        })
        .collect();
    (bags, centers)
}

fn query_bag(seed: u64, centers: &[Vec<f32>], tokens: usize) -> Vec<Vec<f32>> {
    let mut rng = Rng(seed);
    (0..tokens)
        .map(|t| {
            let c = &centers[(t * 7 + seed as usize) % centers.len()];
            c.iter().map(|x| x + 0.35 * rng.next()).collect()
        })
        .collect()
}

fn bag_field(name: &str, bag: &[Vec<f32>]) -> Field {
    Field {
        name: name.into(),
        field_type: FieldType::Vector,
        value: serde_json::to_string(bag).unwrap(),
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("kosha-li-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn write(dir: &Path, bags: &[Vec<Vec<f32>>]) -> Footer {
    let mut w = SegmentWriter::new(kosha_core::SegmentId("s".into()), dir.to_path_buf());
    for (i, bag) in bags.iter().enumerate() {
        w.add_document(
            DocumentId(format!("d{i}")),
            vec![
                Field::text("body", format!("doc {i}")),
                Field::keyword("parity", if i % 2 == 0 { "even" } else { "odd" }),
                bag_field("tokens", bag),
            ],
        );
    }
    w.finalize(Bm25Params::default()).unwrap()
}

/// Brute-force MaxSim over the raw (un-normalized) bags, best first.
fn brute_force(
    query: &[Vec<f32>],
    bags: &[Vec<Vec<f32>>],
    accept: impl Fn(u32) -> bool,
) -> Vec<(u32, f64)> {
    let mut all: Vec<(u32, f64)> = bags
        .iter()
        .enumerate()
        .filter(|(i, _)| accept(*i as u32))
        .map(|(i, bag)| (i as u32, kosha_vector_spfresh::maxsim(query, bag) as f64))
        .collect();
    all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    all
}

fn exhaustive() -> LateInteractionParams {
    LateInteractionParams {
        nprobe: usize::MAX,
        num_candidates: usize::MAX,
    }
}

fn tokens(q: &[Vec<f32>]) -> Vec<&[f32]> {
    q.iter().map(Vec::as_slice).collect()
}

#[test]
fn maxsim_matches_brute_force_on_a_small_fixture() {
    let dir = temp_dir("exact");
    let (bags, centers) = fixture(7, 80, 6);
    let footer = write(&dir, &bags);
    assert_eq!(footer.multi_vectors.len(), 1);
    assert_eq!(footer.multi_vectors[0].name, "tokens");
    assert_eq!(footer.multi_vectors[0].dim, DIM as u32);
    assert_eq!(footer.multi_vectors[0].docs, 80);
    let reader = SegmentReader::open(dir.clone()).unwrap();
    assert!(reader.has_multi_vector_field("tokens"));

    for seed in 1..=5u64 {
        let query = query_bag(seed, &centers, 4);
        let expected = brute_force(&query, &bags, |_| true);
        let got = reader
            .late_interaction_search("tokens", &tokens(&query), 10, exhaustive(), None)
            .unwrap();
        assert_eq!(got.docs_rescored, 80, "exhaustive params rescore every bag");
        assert_eq!(got.hits.len(), 10);
        for (g, e) in got.hits.iter().zip(&expected) {
            assert_eq!(
                g.0,
                e.0,
                "seed {seed}: ranking differs: {:?} vs {:?}",
                got.hits,
                &expected[..10]
            );
            assert!(
                (g.1 - e.1).abs() < 1e-4,
                "seed {seed}: score {} vs {}",
                g.1,
                e.1
            );
        }
    }

    // A single-token query is the one-vector special case of MaxSim.
    let single = query_bag(9, &centers, 1);
    let got = reader
        .late_interaction_search("tokens", &tokens(&single), 3, exhaustive(), None)
        .unwrap();
    let expected = brute_force(&single, &bags, |_| true);
    assert_eq!(
        got.hits.iter().map(|h| h.0).collect::<Vec<_>>(),
        expected[..3].iter().map(|h| h.0).collect::<Vec<_>>()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn restriction_is_applied_while_candidates_are_gathered() {
    let dir = temp_dir("accept");
    let (bags, centers) = fixture(11, 60, 5);
    write(&dir, &bags);
    let reader = SegmentReader::open(dir.clone()).unwrap();
    let query = query_bag(3, &centers, 3);
    let odd = |d: u32| d % 2 == 1;
    // Default-sized params: even a narrow probe must return k odd docs.
    let params = LateInteractionParams {
        nprobe: 1,
        num_candidates: 5,
    };
    let got = reader
        .late_interaction_search("tokens", &tokens(&query), 5, params, Some(&odd))
        .unwrap();
    assert_eq!(got.hits.len(), 5);
    assert!(got.hits.iter().all(|(d, _)| odd(*d)));
    let got = reader
        .late_interaction_search("tokens", &tokens(&query), 5, exhaustive(), Some(&odd))
        .unwrap();
    let expected = brute_force(&query, &bags, odd);
    assert_eq!(
        got.hits.iter().map(|h| h.0).collect::<Vec<_>>(),
        expected[..5].iter().map(|h| h.0).collect::<Vec<_>>()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn default_candidate_generation_recall_against_brute_force() {
    let dir = temp_dir("recall");
    let (bags, centers) = fixture(23, 1500, 24);
    write(&dir, &bags);
    let reader = SegmentReader::open(dir.clone()).unwrap();
    // The query layer's defaults (`KOSHA_LATE_INTERACTION_NPROBE`, and
    // `knn.num_candidates`'s default).
    let params = LateInteractionParams {
        nprobe: 16,
        num_candidates: 100,
    };
    let (mut found, mut total) = (0usize, 0usize);
    for seed in 1..=20u64 {
        let query = query_bag(seed, &centers, 6);
        let truth: Vec<u32> = brute_force(&query, &bags, |_| true)[..10]
            .iter()
            .map(|h| h.0)
            .collect();
        let got = reader
            .late_interaction_search("tokens", &tokens(&query), 10, params, None)
            .unwrap();
        assert!(got.docs_rescored <= 100);
        found += got.hits.iter().filter(|h| truth.contains(&h.0)).count();
        total += 10;
    }
    let recall = found as f64 / total as f64;
    println!("late-interaction recall@10 (nprobe 16, 100 candidates): {recall:.3}");
    assert!(recall >= 0.9, "recall@10 {recall}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn storage_layout_and_overhead_per_document() {
    let dir = temp_dir("layout");
    let (bags, _) = fixture(5, 200, 8);
    let footer = write(&dir, &bags);
    let tokens: u64 = bags.iter().map(|b| b.len() as u64).sum();
    assert_eq!(footer.multi_vectors[0].tokens, tokens);
    assert_eq!(multivec_files(&footer), vec![bags_file(0), ivf_file(0)]);

    let size = |f: &str| std::fs::metadata(dir.join(f)).unwrap().len();
    let bags_bytes = size(&bags_file(0));
    let ivf_bytes = size(&ivf_file(0));
    // Header + 8-byte offsets entry per doc + 4 bytes per token float.
    assert_eq!(bags_bytes, 20 + 8 * 200 + 4 * DIM as u64 * tokens);
    // Header, one centroid + range per posting, and per token a doc_seq,
    // an int8 scale and a `dim`-byte int8 code (a quarter of the f32 bag).
    let postings = u32::from_le_bytes(
        std::fs::read(dir.join(ivf_file(0))).unwrap()[12..16]
            .try_into()
            .unwrap(),
    ) as u64;
    assert_eq!(
        ivf_bytes,
        16 + postings * (4 * DIM as u64 + 12) + tokens * (8 + DIM as u64)
    );
    let source_json: u64 = bags
        .iter()
        .map(|b| serde_json::to_string(b).unwrap().len() as u64)
        .sum();
    println!(
        "late-interaction storage, {DIM}-dim, {:.1} tokens/doc: bags {:.0} B/doc, ivf {:.0} B/doc, \
         source JSON in doc store {:.0} B/doc",
        tokens as f64 / 200.0,
        bags_bytes as f64 / 200.0,
        ivf_bytes as f64 / 200.0,
        source_json as f64 / 200.0,
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn documents_read_back_with_their_bags_unchanged() {
    let dir = temp_dir("roundtrip");
    let (bags, _) = fixture(3, 4, 2);
    write(&dir, &bags);
    let reader = SegmentReader::open(dir.clone()).unwrap();
    for (i, bag) in bags.iter().enumerate() {
        let record = reader.doc_record_full(i as u32).unwrap().unwrap();
        let field = record.fields.iter().find(|f| f.name == "tokens").unwrap();
        assert_eq!(field.value, serde_json::to_string(bag).unwrap());
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn segments_without_bags_are_unchanged() {
    let dir = temp_dir("nobags");
    let mut w = SegmentWriter::new(kosha_core::SegmentId("s".into()), dir.clone());
    w.add_document(
        DocumentId("d0".into()),
        vec![
            Field::text("body", "hello"),
            Field::vector("emb", vec![1.0, 0.0, 0.0]),
        ],
    );
    let footer = w.finalize(Bm25Params::default()).unwrap();
    assert!(footer.multi_vectors.is_empty());
    let json = std::fs::read_to_string(dir.join("footer.json")).unwrap();
    assert!(!json.contains("multi_vectors"), "no new footer key");
    assert!(std::fs::read_dir(&dir).unwrap().all(|e| !e
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with("multivec")));
    let reader = SegmentReader::open(dir.clone()).unwrap();
    assert!(!reader.has_multi_vector_field("emb"));
    // A late-interaction query over a segment without bags has no hits.
    let got = reader
        .late_interaction_search("emb", &[&[1.0, 0.0, 0.0]], 5, exhaustive(), None)
        .unwrap();
    assert!(got.hits.is_empty());
    // A footer from before #205 (no key at all) still parses.
    let legacy: Footer = serde_json::from_str(&json).unwrap();
    assert!(legacy.multi_vectors.is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn single_and_multi_vector_fields_share_a_segment() {
    let dir = temp_dir("mixed");
    let (bags, centers) = fixture(13, 30, 4);
    let mut w = SegmentWriter::new(kosha_core::SegmentId("s".into()), dir.clone());
    for (i, bag) in bags.iter().enumerate() {
        w.add_document(
            DocumentId(format!("d{i}")),
            vec![
                Field::vector("emb", centers[i % 4].clone()),
                bag_field("tokens", bag),
            ],
        );
    }
    w.finalize(Bm25Params::default()).unwrap();
    let reader = SegmentReader::open(dir.clone()).unwrap();
    // The single-vector index holds one vector per document, no tokens.
    let hits = reader
        .knn_search(&centers[1], 30, 1000)
        .expect("v2 vector index")
        .unwrap();
    assert_eq!(hits.len(), 30);
    let query = query_bag(2, &centers, 3);
    let got = reader
        .late_interaction_search("tokens", &tokens(&query), 5, exhaustive(), None)
        .unwrap();
    assert_eq!(got.hits[0].0, brute_force(&query, &bags, |_| true)[0].0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wrong_query_dimension_is_a_client_error() {
    let dir = temp_dir("dim");
    let (bags, _) = fixture(17, 5, 2);
    write(&dir, &bags);
    let reader = SegmentReader::open(dir.clone()).unwrap();
    let err = reader
        .late_interaction_search("tokens", &[&[1.0, 0.0]], 5, exhaustive(), None)
        .unwrap_err();
    assert!(matches!(err, KoshaError::InvalidQuery(_)), "{err:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_bag_of_a_different_dimension_is_stored_but_not_indexed() {
    let dir = temp_dir("ragged");
    let mut w = SegmentWriter::new(kosha_core::SegmentId("s".into()), dir.clone());
    w.add_document(
        DocumentId("a".into()),
        vec![bag_field("tokens", &[vec![1.0, 0.0], vec![0.0, 1.0]])],
    );
    w.add_document(
        DocumentId("b".into()),
        vec![bag_field("tokens", &[vec![1.0, 0.0, 0.0]])],
    );
    let footer = w.finalize(Bm25Params::default()).unwrap();
    assert_eq!(footer.multi_vectors[0].docs, 1);
    let reader = SegmentReader::open(dir.clone()).unwrap();
    let b = reader.doc_record_full(1).unwrap().unwrap();
    assert_eq!(b.fields[0].value, "[[1.0,0.0,0.0]]");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn corrupt_bag_store_fails_loudly() {
    let dir = temp_dir("corrupt");
    let (bags, centers) = fixture(19, 10, 2);
    write(&dir, &bags);
    let path = dir.join(bags_file(0));
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.truncate(bytes.len() - 4);
    std::fs::write(&path, bytes).unwrap();
    let reader = SegmentReader::open(dir.clone()).unwrap();
    let query = query_bag(1, &centers, 2);
    let err = reader
        .late_interaction_search("tokens", &tokens(&query), 5, exhaustive(), None)
        .unwrap_err();
    assert!(matches!(err, KoshaError::CorruptSegment(_)), "{err:?}");
    let _ = std::fs::remove_dir_all(&dir);
}
