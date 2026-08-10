// Types and option normalization for the SPFresh vector index.

pub(super) const DEFAULT_MAX_POSTING_LEN: usize = 256;
pub(super) const DEFAULT_MIN_POSTING_LEN: usize = 32;
pub(super) const DEFAULT_SPLIT_NEIGHBORS: usize = 8;
pub(super) const DEFAULT_BOUNDARY_REPLICAS: usize = 1;

/// Tuning knobs for the SPFresh/LIRE vector index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpFreshOptions {
    /// Maximum live entries per posting before the local rebuilder splits it.
    pub max_posting_len: usize,
    /// Minimum live entries before a posting becomes eligible for merge.
    pub min_posting_len: usize,
    /// Number of old-centroid neighbors considered for post-split reassignment.
    pub split_neighbor_count: usize,
    /// Number of neighboring postings that receive SPANN-style boundary
    /// replicas for each primary vector.
    pub boundary_replica_count: usize,
    /// Number of PQ subvectors. `0` disables PQ metadata.
    pub pq_subvector_count: usize,
    /// Number of centroids per PQ subquantizer.
    pub pq_centroids: usize,
}

impl Default for SpFreshOptions {
    fn default() -> Self {
        Self {
            max_posting_len: DEFAULT_MAX_POSTING_LEN,
            min_posting_len: DEFAULT_MIN_POSTING_LEN,
            split_neighbor_count: DEFAULT_SPLIT_NEIGHBORS,
            boundary_replica_count: DEFAULT_BOUNDARY_REPLICAS,
            pq_subvector_count: 0,
            pq_centroids: 16,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SpFreshVersion {
    pub(crate) version: u8,
    pub(crate) deleted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpFreshEntry {
    pub doc_seq: u32,
    pub version: u8,
    pub vector: Vec<f32>,
    pub is_replica: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpFreshPosting {
    pub id: u32,
    pub centroid: Vec<f32>,
    pub entries: Vec<SpFreshEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpFreshStats {
    pub dimensions: usize,
    pub postings: usize,
    pub live_vectors: usize,
    pub physical_vectors: usize,
    pub replica_vectors: usize,
    pub deleted_vectors: usize,
    pub pq_encoded_vectors: usize,
}

pub(crate) fn normalize_options(mut options: SpFreshOptions) -> SpFreshOptions {
    options.max_posting_len = options.max_posting_len.max(2);
    options.min_posting_len = options.min_posting_len.min(options.max_posting_len / 2);
    options.split_neighbor_count = options.split_neighbor_count.max(1);
    options.pq_centroids = options.pq_centroids.clamp(1, u8::MAX as usize + 1);
    options
}
