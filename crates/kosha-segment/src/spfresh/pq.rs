use super::math::{squared_l2, squared_norm};

#[derive(Debug, Clone, PartialEq)]
pub struct ProductQuantizer {
    pub(crate) dimensions: usize,
    pub(crate) subvector_count: usize,
    pub(crate) centroids_per_subvector: usize,
    pub(crate) codebooks: Vec<Vec<Vec<f32>>>,
}

impl ProductQuantizer {
    pub fn train<'a>(
        vectors: impl Iterator<Item = &'a [f32]>,
        subvector_count: usize,
        centroids_per_subvector: usize,
    ) -> Self {
        let vectors: Vec<&[f32]> = vectors.collect();
        let dimensions = vectors.first().map(|v| v.len()).unwrap_or(0);
        let subvector_count = subvector_count.max(1);
        let subdim = dimensions / subvector_count;
        let centroids_per_subvector = centroids_per_subvector.clamp(1, u8::MAX as usize + 1);
        let mut codebooks = Vec::with_capacity(subvector_count);
        for sub in 0..subvector_count {
            let start = sub * subdim;
            let end = start + subdim;
            let mut samples: Vec<Vec<f32>> = vectors
                .iter()
                .map(|vector| vector[start..end].to_vec())
                .collect();
            samples.sort_by(|a, b| {
                squared_norm(a)
                    .partial_cmp(&squared_norm(b))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let mut codebook = Vec::new();
            for i in 0..centroids_per_subvector.min(samples.len().max(1)) {
                let idx = if samples.len() <= 1 {
                    0
                } else {
                    i * (samples.len() - 1) / centroids_per_subvector.saturating_sub(1).max(1)
                };
                codebook.push(
                    samples
                        .get(idx)
                        .cloned()
                        .unwrap_or_else(|| vec![0.0; subdim]),
                );
            }
            codebooks.push(codebook);
        }
        Self {
            dimensions,
            subvector_count,
            centroids_per_subvector,
            codebooks,
        }
    }

    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        if vector.len() != self.dimensions || self.subvector_count == 0 {
            return Vec::new();
        }
        let subdim = self.dimensions / self.subvector_count;
        let mut code = Vec::with_capacity(self.subvector_count);
        for sub in 0..self.subvector_count {
            let start = sub * subdim;
            let end = start + subdim;
            let query = &vector[start..end];
            let idx = self.codebooks[sub]
                .iter()
                .enumerate()
                .min_by(|(_, a), (_, b)| {
                    squared_l2(query, a)
                        .partial_cmp(&squared_l2(query, b))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|(idx, _)| idx)
                .unwrap_or(0);
            code.push(idx as u8);
        }
        code
    }

    pub fn adc_distance(&self, query: &[f32], code: &[u8]) -> f32 {
        if query.len() != self.dimensions || code.len() != self.subvector_count {
            return f32::INFINITY;
        }
        let subdim = self.dimensions / self.subvector_count;
        let mut distance = 0.0;
        for (sub, &centroid_id) in code.iter().enumerate() {
            let start = sub * subdim;
            let end = start + subdim;
            let Some(centroid) = self.codebooks[sub].get(centroid_id as usize) else {
                return f32::INFINITY;
            };
            distance += squared_l2(&query[start..end], centroid);
        }
        distance
    }
}
