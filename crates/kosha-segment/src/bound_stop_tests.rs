//! The α-scaled bound stop (#384, `KOSHA_KNN_BOUND_STOP_ALPHA`).

use super::*;
use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};

const DIM: usize = 16;
const CLUSTERS: usize = 8;
const PER_CLUSTER: usize = 80;
const FIELD: &str = "emb";

struct Lcg(u64);

impl Lcg {
    fn noise(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 40) as f32 / (1u64 << 23) as f32 * 2.0 - 1.0) * 0.05
    }
}

/// Tight clusters around `CLUSTERS` orthogonal axes.
fn clustered() -> Vec<Vec<f32>> {
    let mut rng = Lcg(0x384);
    (0..CLUSTERS * PER_CLUSTER)
        .map(|i| {
            let mut v: Vec<f32> = (0..DIM).map(|_| rng.noise()).collect();
            v[i % CLUSTERS] += 1.0;
            v
        })
        .collect()
}

struct Fixture {
    dir: PathBuf,
    reader: SegmentReader,
}

impl Fixture {
    fn new(name: &str, vectors: &[Vec<f32>]) -> Self {
        let dir =
            std::env::temp_dir().join(format!("kosha-bound-stop-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut writer = SegmentWriter::new(SegmentId("stop".into()), dir.clone());
        for (i, v) in vectors.iter().enumerate() {
            writer.add_document(
                DocumentId(format!("d{i}")),
                vec![Field::vector(FIELD, v.clone())],
            );
        }
        writer.finalize(Bm25Params::default()).unwrap();
        let reader = SegmentReader::open_with_options(dir.clone(), true).unwrap();
        Self { dir, reader }
    }

    fn postings(&self) -> usize {
        let index = self.reader.vector_field(FIELD).unwrap().unwrap();
        index.lazy.as_ref().unwrap().ranges.len()
    }

    fn search(&self, alpha: Option<f32>, query: &[f32], k: usize) -> (Vec<(u32, f64)>, usize) {
        with_bound_stop_alpha(alpha, || {
            self.reader
                .knn_search_within_filtered(FIELD, query, k, f32::INFINITY, usize::MAX, None)
                .unwrap()
                .unwrap()
        })
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn bound_stop_alpha_parse_accepts_only_positive_finite_values() {
    assert_eq!(parse_bound_stop_alpha(None), None);
    assert_eq!(parse_bound_stop_alpha(Some(" ")), None);
    assert_eq!(parse_bound_stop_alpha(Some("1")), Some(1.0));
    assert_eq!(parse_bound_stop_alpha(Some(" 0.85 ")), Some(0.85));
    for bad in ["0", "-0.5", "NaN", "inf", "abc"] {
        assert_eq!(parse_bound_stop_alpha(Some(bad)), None, "{bad}");
    }
}

#[test]
fn bound_stop_uses_the_angular_ceiling() {
    // Query at 60 degrees from the centroid, members within 30 degrees:
    // no member is closer than 30 degrees, similarity <= cos 30.
    let d60 = 1.0 - 60f32.to_radians().cos();
    let d30 = 1.0 - 30f32.to_radians().cos();
    let cos30 = 30f64.to_radians().cos();
    assert!(bound_stop(cos30 + 1e-4, d60, d30, 1.0));
    assert!(!bound_stop(cos30 - 1e-4, d60, d30, 1.0));
    // Cosine distance alone (1 - (d60 - d30) = 0.634) would be lower than
    // the true ceiling (0.866): the bound must not use it.
    assert!(!bound_stop(0.7, d60, d30, 1.0));
    // A query inside the radius: any similarity up to 1 is possible.
    assert!(!bound_stop(0.999, d30, d60, 1.0));
    // alpha < 1 skips earlier.
    assert!(bound_stop(cos30 * 0.9, d60, d30, 0.85));
}

#[test]
fn sink_kth_lower_is_the_kth_guaranteed_score() {
    let mut exact = CandidateSink::with_running_top_k(2, true);
    exact.push(1, 0.5, 0.0);
    assert_eq!(exact.kth_lower(), None);
    exact.push(2, 0.9, 0.0);
    exact.push(3, 0.7, 0.0);
    assert_eq!(exact.kth_lower(), Some(0.7f32 as f64));

    // Quantized: tracked only when asked, from `estimate - error`.
    let mut quantized = CandidateSink::with_running_top_k(2, false);
    quantized.push(1, 0.9, 0.1);
    quantized.push(2, 0.8, 0.0);
    assert_eq!(quantized.kth_lower(), None);
    let mut quantized = CandidateSink::with_running_top_k(2, false).tracking_kth_lower();
    quantized.push(1, 0.9, 0.5);
    assert_eq!(quantized.kth_lower(), None);
    quantized.push(2, 0.8, 0.0);
    quantized.push(3, 0.7, 0.0);
    let expected = 0.7f32 as f64;
    assert_eq!(quantized.kth_lower(), Some(expected));
    // Every candidate is still kept for the rescoring shortlist.
    assert_eq!(quantized.finish().0.len(), 3);

    assert_eq!(CandidateSink::with_running_top_k(0, true).kth_lower(), None);
}

#[test]
fn bound_stop_skips_far_postings_and_keeps_the_results_on_clustered_data() {
    let fixture = Fixture::new("clustered", &clustered());
    let postings = fixture.postings();
    assert!(
        postings >= CLUSTERS,
        "the fixture needs a posting per cluster"
    );
    for axis in 0..CLUSTERS {
        let mut query = vec![0.0f32; DIM];
        query[axis] = 1.0;
        for k in [1usize, 10] {
            let (off, off_probes) = fixture.search(None, &query, k);
            assert_eq!(off_probes, postings, "off: every posting is in the budget");
            let (on, on_probes) = fixture.search(Some(1.0), &query, k);
            assert_eq!(on, off, "axis {axis} k {k}");
            assert!(
                on_probes < off_probes,
                "axis {axis} k {k}: {on_probes} of {off_probes} postings"
            );
        }
    }
}

#[test]
fn bound_stop_never_fires_before_k_candidates() {
    let fixture = Fixture::new("short", &clustered());
    let postings = fixture.postings();
    // More neighbours than the corpus holds: theta never exists.
    let mut query = vec![0.0f32; DIM];
    query[0] = 1.0;
    let k = CLUSTERS * PER_CLUSTER + 1;
    let (off, off_probes) = fixture.search(None, &query, k);
    let (on, on_probes) = fixture.search(Some(0.01), &query, k);
    assert_eq!((on, on_probes), (off, off_probes));
    assert_eq!(on_probes, postings);
}

#[test]
fn bound_stop_keeps_postings_whose_radius_rounded_to_zero() {
    // A member just off the centroid can beat the centroid's own score;
    // a stored radius of 0 must not let alpha = 1 skip it.
    let distance = 0.1f32;
    let theta = 1.0 - distance as f64;
    assert!(!bound_stop(theta, distance, 0.0, 1.0));
    assert!(bound_stop(1.0, distance, 0.0, 1.0));
}
