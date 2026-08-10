use super::math::{cosine_distance, cosine_similarity};
use super::types::normalize_options;
use super::*;
use std::collections::HashMap;

fn opts() -> SpFreshOptions {
    SpFreshOptions {
        max_posting_len: 4,
        min_posting_len: 1,
        split_neighbor_count: 4,
        boundary_replica_count: 0,
        pq_subvector_count: 0,
        pq_centroids: 16,
    }
}

fn stress_opts() -> SpFreshOptions {
    SpFreshOptions {
        max_posting_len: 6,
        min_posting_len: 2,
        split_neighbor_count: 6,
        boundary_replica_count: 1,
        pq_subvector_count: 0,
        pq_centroids: 16,
    }
}

fn generated_vector(seed: u32, dimensions: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(747_796_405).wrapping_add(2_891_336_453);
    (0..dimensions)
        .map(|dim| {
            state = state
                .wrapping_mul(1_664_525)
                .wrapping_add(1_013_904_223 + dim as u32);
            ((state % 2_000) as f32 - 1_000.0) / 1_000.0
        })
        .collect()
}

fn entry(doc_seq: u32, vector: Vec<f32>) -> SpFreshEntry {
    SpFreshEntry {
        doc_seq,
        version: 0,
        vector,
        is_replica: false,
    }
}

fn exact_knn(model: &HashMap<u32, Vec<f32>>, query: &[f32], k: usize) -> Vec<(u32, f64)> {
    let mut scores: Vec<(u32, f64)> = model
        .iter()
        .map(|(doc_seq, vector)| (*doc_seq, cosine_similarity(query, vector) as f64))
        .collect();
    scores.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    scores.truncate(k);
    scores
}

fn assert_live_vectors_match_model(index: &SpFreshIndex, model: &HashMap<u32, Vec<f32>>) {
    let live = index.live_vectors();
    assert_eq!(live.len(), model.len(), "live vector count mismatch");
    for (doc_seq, vector) in live {
        assert_eq!(
            Some(&vector),
            model.get(&doc_seq),
            "live vector mismatch for doc_seq={doc_seq}"
        );
    }
}

fn assert_single_current_copy_per_live_doc(index: &SpFreshIndex) {
    let mut live_counts: HashMap<u32, usize> = HashMap::new();
    for posting in &index.postings {
        for entry in &posting.entries {
            if !entry.is_replica && index.is_entry_live(entry) {
                *live_counts.entry(entry.doc_seq).or_default() += 1;
            }
        }
    }
    for (doc_seq, state) in &index.version_map {
        let expected = usize::from(!state.deleted);
        assert_eq!(
            live_counts.get(doc_seq).copied().unwrap_or(0),
            expected,
            "unexpected live physical-copy count for doc_seq={doc_seq}"
        );
    }
}

fn assert_nearest_partition_assignment(index: &SpFreshIndex) {
    for posting in &index.postings {
        for entry in &posting.entries {
            if entry.is_replica || !index.is_entry_live(entry) {
                continue;
            }
            let assigned = cosine_distance(&entry.vector, &posting.centroid);
            let best = index
                .postings
                .iter()
                .map(|candidate| cosine_distance(&entry.vector, &candidate.centroid))
                .fold(f32::INFINITY, f32::min);
            assert!(
                assigned <= best + 1e-5,
                "doc_seq={} assigned to posting {} at distance {assigned}, but best distance is {best}",
                entry.doc_seq,
                posting.id
            );
        }
    }
}

fn assert_exhaustive_search_matches_exact(index: &SpFreshIndex, model: &HashMap<u32, Vec<f32>>) {
    if model.is_empty() {
        return;
    }
    let query_count = 24;
    for query_id in 0..query_count {
        let query = generated_vector(10_000 + query_id, index.dimensions());
        let k = model.len().min(5);
        let got = index.search(&query, k, index.postings().len());
        let expected = exact_knn(model, &query, k);
        assert_eq!(
            got.iter().map(|(doc_seq, _)| *doc_seq).collect::<Vec<_>>(),
            expected
                .iter()
                .map(|(doc_seq, _)| *doc_seq)
                .collect::<Vec<_>>(),
            "exact-search doc order mismatch for query_id={query_id}"
        );
        for ((got_doc, got_score), (expected_doc, expected_score)) in got.iter().zip(expected) {
            assert_eq!(*got_doc, expected_doc);
            assert!(
                (got_score - expected_score).abs() < 1e-6,
                "score mismatch for doc_seq={got_doc}: got {got_score}, expected {expected_score}"
            );
        }
    }
}

fn assert_index_invariants(index: &SpFreshIndex, model: &HashMap<u32, Vec<f32>>) {
    assert_live_vectors_match_model(index, model);
    assert_single_current_copy_per_live_doc(index);
    assert_nearest_partition_assignment(index);
    assert_exhaustive_search_matches_exact(index, model);
}

#[test]
fn insert_splits_and_keeps_live_vectors_searchable() {
    let mut index = SpFreshIndex::new(2, opts());
    for i in 0..10 {
        index.insert(i, vec![i as f32, 1.0]).unwrap();
    }
    assert!(index.stats().postings > 1);
    assert_eq!(index.stats().live_vectors, 10);

    let results = index.search(&[9.0, 1.0], 3, 3);
    assert_eq!(results[0].0, 9);
    assert_eq!(results.len(), 3);
}

#[test]
fn delete_and_reinsert_hide_stale_versions() {
    let mut index = SpFreshIndex::new(2, opts());
    index.insert(1, vec![1.0, 0.0]).unwrap();
    index.insert(2, vec![0.0, 1.0]).unwrap();
    assert!(index.delete(1));
    index.insert(1, vec![0.0, 1.0]).unwrap();

    let live = index.live_vectors();
    assert_eq!(live.len(), 2);
    assert_eq!(live.iter().filter(|(doc_seq, _)| *doc_seq == 1).count(), 1);
    assert_eq!(
        live.iter().find(|(doc_seq, _)| *doc_seq == 1).unwrap().1,
        vec![0.0, 1.0]
    );
    assert!(!index
        .search(&[1.0, 0.0], 10, 10)
        .iter()
        .any(|(doc_seq, score)| *doc_seq == 1 && *score > 0.0));
}

#[test]
fn serialized_snapshot_round_trips() {
    let mut index = SpFreshIndex::new(3, opts());
    index.insert(7, vec![1.0, 0.0, 0.0]).unwrap();
    index.insert(8, vec![0.0, 1.0, 0.0]).unwrap();
    index.delete(8);

    let bytes = index.to_bytes();
    assert!(is_spfresh_vector_index(&bytes));
    let decoded = SpFreshIndex::from_bytes(&bytes).unwrap().unwrap();
    assert_eq!(decoded.options(), normalize_options(opts()));
    assert_eq!(decoded.live_vectors(), vec![(7, vec![1.0, 0.0, 0.0])]);
}

#[test]
fn deterministic_update_sequence_preserves_lire_invariants() {
    let mut index = SpFreshIndex::new(4, stress_opts());
    let mut model = HashMap::new();

    for step in 0..160 {
        let doc_seq = (step * 37 + 11) % 41;
        if step % 7 == 0 {
            index.delete(doc_seq);
            model.remove(&doc_seq);
        } else {
            let vector = generated_vector(step + 1_000, 4);
            index.insert(doc_seq, vector.clone()).unwrap();
            model.insert(doc_seq, vector);
        }
        assert_index_invariants(&index, &model);
    }
}

#[test]
fn repeated_updates_past_version_wrap_keep_one_live_copy() {
    let mut index = SpFreshIndex::new(3, stress_opts());
    let mut model = HashMap::new();
    for step in 0..180 {
        let vector = generated_vector(step + 20_000, 3);
        index.insert(9, vector.clone()).unwrap();
        model.insert(9, vector);
        assert_index_invariants(&index, &model);
    }
}

#[test]
fn split_reassigns_neighbor_vectors_to_nearest_new_posting() {
    let mut index = SpFreshIndex::new(
        2,
        SpFreshOptions {
            max_posting_len: 3,
            min_posting_len: 1,
            split_neighbor_count: 4,
            boundary_replica_count: 1,
            pq_subvector_count: 0,
            pq_centroids: 16,
        },
    );
    let mut model = HashMap::new();
    for (doc_seq, vector) in [
        (0, vec![1.0, 0.02]),
        (1, vec![1.0, -0.02]),
        (2, vec![0.92, 0.2]),
        (3, vec![0.92, -0.2]),
        (4, vec![0.65, 0.76]),
        (5, vec![0.62, 0.79]),
        (6, vec![0.64, -0.77]),
        (7, vec![0.61, -0.80]),
    ] {
        index.insert(doc_seq, vector.clone()).unwrap();
        model.insert(doc_seq, vector);
    }

    assert!(
        index.stats().postings > 1,
        "fixture should trigger at least one split"
    );
    assert_index_invariants(&index, &model);
}

#[test]
fn merge_reassigns_survivors_and_preserves_exact_search() {
    let mut index = SpFreshIndex::new(3, stress_opts());
    let mut model = HashMap::new();
    for doc_seq in 0..30 {
        let vector = generated_vector(doc_seq + 30_000, 3);
        index.insert(doc_seq, vector.clone()).unwrap();
        model.insert(doc_seq, vector);
    }
    for doc_seq in (0..30).step_by(3) {
        index.delete(doc_seq);
        model.remove(&doc_seq);
    }
    assert_index_invariants(&index, &model);
}

#[test]
fn boundary_vector_replication_preserves_primary_invariants() {
    let mut index = SpFreshIndex::new(
        3,
        SpFreshOptions {
            max_posting_len: 4,
            min_posting_len: 1,
            split_neighbor_count: 4,
            boundary_replica_count: 2,
            pq_subvector_count: 0,
            pq_centroids: 16,
        },
    );
    let mut model = HashMap::new();
    for doc_seq in 0..18 {
        let vector = generated_vector(doc_seq + 40_000, 3);
        index.insert(doc_seq, vector.clone()).unwrap();
        model.insert(doc_seq, vector);
    }

    assert!(
        index.stats().replica_vectors > 0,
        "boundary replication should materialize replica entries"
    );
    assert_index_invariants(&index, &model);
}

#[test]
fn pq_ivfadc_codes_round_trip_and_score_candidates() {
    let mut index = SpFreshIndex::new(
        4,
        SpFreshOptions {
            max_posting_len: 5,
            min_posting_len: 1,
            split_neighbor_count: 4,
            boundary_replica_count: 0,
            pq_subvector_count: 2,
            pq_centroids: 4,
        },
    );
    for doc_seq in 0..16 {
        index
            .insert(doc_seq, generated_vector(doc_seq + 50_000, 4))
            .unwrap();
    }
    assert_eq!(index.stats().pq_encoded_vectors, index.stats().live_vectors);

    let query = generated_vector(50_123, 4);
    let approx = index.pq_search_adc(&query, 4, index.postings().len());
    assert_eq!(approx.len(), 4);

    let decoded = SpFreshIndex::from_bytes(&index.to_bytes())
        .unwrap()
        .unwrap();
    assert_eq!(
        decoded.stats().pq_encoded_vectors,
        index.stats().pq_encoded_vectors
    );
    assert_eq!(
        decoded.pq_search_adc(&query, 4, decoded.postings().len()),
        approx
    );
}

#[test]
fn centroid_navigation_orders_postings_by_distance() {
    let postings = vec![
        SpFreshPosting {
            id: 10,
            centroid: vec![1.0, 0.0],
            entries: vec![entry(1, vec![1.0, 0.0])],
        },
        SpFreshPosting {
            id: 11,
            centroid: vec![0.0, 1.0],
            entries: vec![entry(2, vec![0.0, 1.0])],
        },
    ];
    let navigator = CentroidNavigator::build(&postings);
    assert_eq!(navigator.nearest_postings(&[0.9, 0.1], 1), vec![0]);
    assert_eq!(navigator.nearest_postings(&[0.1, 0.9], 1), vec![1]);
}

#[test]
fn block_controller_put_append_parallel_get_and_cas() {
    let mut controller = SpFreshBlockController::new(2);
    let initial = controller.put(
        7,
        vec![
            entry(1, vec![1.0, 0.0]),
            entry(2, vec![0.0, 1.0]),
            entry(3, vec![1.0, 1.0]),
        ],
    );
    assert_eq!(initial.entry_count, 3);
    assert_eq!(controller.get(7).unwrap().len(), 3);

    let appended = controller
        .append(7, entry(4, vec![0.5, 0.5]), Some(initial.generation))
        .unwrap();
    assert_eq!(appended.entry_count, 4);
    assert_eq!(
        controller
            .append(7, entry(5, vec![0.2, 0.8]), Some(initial.generation))
            .unwrap_err(),
        PostingCasError {
            expected: initial.generation,
            actual: appended.generation,
        }
    );
    let batch = controller.parallel_get(&[7, 8]);
    assert_eq!(batch.get(&7).unwrap().len(), 4);
    assert!(!batch.contains_key(&8));
}

#[test]
fn async_foreground_updater_background_rebuilder_converges() {
    let async_index = SpFreshAsyncIndex::new(SpFreshIndex::new(3, stress_opts()));
    let mut model = HashMap::new();
    for step in 0..80 {
        let doc_seq = (step * 13 + 5) % 23;
        if step % 11 == 0 {
            async_index.delete(doc_seq);
            model.remove(&doc_seq);
        } else {
            let vector = generated_vector(step + 60_000, 3);
            async_index.insert(doc_seq, vector.clone()).unwrap();
            model.insert(doc_seq, vector);
        }
    }
    async_index.rebuild_now();
    let snapshot = async_index.snapshot();
    assert_index_invariants(&snapshot, &model);
}
