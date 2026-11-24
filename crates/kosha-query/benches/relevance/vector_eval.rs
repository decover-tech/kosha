use kosha_segment::{KnnRefill, KnnSegmentRanking, SegmentReader};
use serde_json::{json, Value};

use crate::{
    metrics::{ExactNeighbors, Mean},
    KNN_K, VECTOR_FIELD,
};

pub(super) fn kmr_curve(
    readers: &[SegmentReader],
    queries: &[&[f32]],
    vectors: &[(String, Vec<f32>)],
) -> Value {
    let mut layouts = Vec::new();
    for reader in readers {
        match reader
            .vector_posting_layout(VECTOR_FIELD)
            .expect("read vector posting layout")
        {
            Some(layout) => layouts.push((reader, layout)),
            None if reader
                .vector_field(VECTOR_FIELD)
                .expect("vector field")
                .is_some() =>
            {
                return json!({"unavailable": "vector column has no clustered posting layout"});
            }
            None => {}
        }
    }
    if layouts.is_empty() {
        return json!({"unavailable": "no clustered vector columns"});
    }

    let truths: Vec<_> = queries
        .iter()
        .map(|query| {
            ExactNeighbors::new(
                query,
                vectors
                    .iter()
                    .map(|(id, vector)| (id.as_str(), vector.as_slice())),
                KNN_K,
            )
        })
        .collect();
    let mut points = Vec::new();
    for budget in [Some(1), Some(2), Some(4), Some(8), Some(16), Some(32), None] {
        let mut recall = Mean::default();
        let mut scanned = Mean::default();
        for (query, truth) in queries.iter().zip(&truths) {
            let mut entries = 0;
            let mut hits = Vec::new();
            for (reader, layout) in &layouts {
                let bounds = reader
                    .knn_centroid_distances(VECTOR_FIELD, query)
                    .expect("get centroid scores");
                let ranking = KnnSegmentRanking::new(
                    bounds,
                    f32::INFINITY,
                    budget.unwrap_or(layout.postings.len()),
                );
                let (segment_hits, postings) = reader
                    .knn_search_planned_fetching(
                        VECTOR_FIELD,
                        query,
                        KNN_K,
                        &ranking,
                        None,
                        KnnRefill::Always,
                        None,
                    )
                    .expect("clustered vector column")
                    .expect("probe clustered vectors");
                assert_eq!(postings, ranking.budgeted().len());
                entries += ranking
                    .budgeted()
                    .iter()
                    .map(|&i| layout.postings[i as usize].doc_seqs.len())
                    .sum::<usize>();
                hits.extend(segment_hits.into_iter().map(|(seq, score)| {
                    (reader.doc_meta(seq).expect("probe document").doc_id, score)
                }));
            }
            hits.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(b.0)));
            let ids: Vec<_> = hits
                .into_iter()
                .take(KNN_K)
                .map(|(id, _)| id.to_string())
                .collect();
            recall.add(truth.recall(&ids));
            scanned.add(Some(entries as f64));
        }
        points.push(json!({
            "max_postings_per_segment": budget,
            "mean_vectors_scanned": scanned.value(),
            "recall_at_10": recall.value(),
            "queries": recall.n,
        }));
    }
    json!({"points": points})
}
