use super::math::cosine_distance;
use super::types::SpFreshPosting;

#[derive(Debug, Clone, PartialEq)]
pub struct CentroidNavigator {
    centroids: Vec<(usize, Vec<f32>)>,
}

impl CentroidNavigator {
    pub fn build(postings: &[SpFreshPosting]) -> Self {
        Self {
            centroids: postings
                .iter()
                .enumerate()
                .map(|(idx, posting)| (idx, posting.centroid.clone()))
                .collect(),
        }
    }

    pub fn nearest_postings(&self, query: &[f32], limit: usize) -> Vec<usize> {
        let mut scored: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .map(|(idx, centroid)| (*idx, cosine_distance(query, centroid)))
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored
            .into_iter()
            .take(limit.max(1))
            .map(|(idx, _)| idx)
            .collect()
    }
}
