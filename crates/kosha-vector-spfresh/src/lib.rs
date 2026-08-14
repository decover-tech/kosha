//! A standalone, in-memory implementation of the SPFresh/SPANN algorithmic
//! core: a cluster/posting-based ANN index kept balanced by LIRE
//! (**L**ightweight **I**ncremental **RE**-balancing — Split, Merge, and a
//! bounded Reassign, on top of Insert/Delete).
//!
//! This crate is **not** wired into kosha's on-disk segment format or query
//! path — it's a self-contained, benchmarked proof of the mechanism, built
//! to answer the open question `DESIGN.md` already flags about kosha's
//! current per-segment HNSW-rebuilt-from-scratch-on-every-open vector index.
//! See `README.md` for the concept-to-code map and the list of deliberate
//! simplifications versus the full paper (no disk/SPDK backing, no
//! concurrent background rebuilder, no PQ compression).
//!
//! Start at [`ClusterIndex`].

mod build;
mod centroid_probe;
mod error;
mod kmeans;
mod ops;
mod point;
mod posting;
mod rng;
mod search;

use std::collections::HashMap;

pub use centroid_probe::{nearest_centroids, nearest_centroids_dot};
pub use error::VectorIndexError;
pub use point::{cosine_distance, cosine_similarity, dot, normalize_in_place};
pub use rng::DeterministicRng;

use posting::{Posting, PostingId};

/// Tuning knobs for a [`ClusterIndex`]. See each field's doc comment for the
/// paper concept it maps to.
#[derive(Debug, Clone)]
pub struct ClusterIndexConfig {
    /// Vector dimensionality. Every inserted/queried vector must match.
    pub dim: usize,
    /// The size a posting is built toward. Not itself an enforced bound —
    /// `max_posting_size`/`min_posting_size` are the actual triggers.
    pub target_posting_size: usize,
    /// A posting exceeding this live-vector count triggers `Split`.
    pub max_posting_size: usize,
    /// A posting falling below this live-vector count triggers `Merge`
    /// (unless it's the index's only active posting).
    pub min_posting_size: usize,
    /// Acceptance ratio for `balanced_bisect`'s rebalance pass: a split is
    /// accepted once `max(|L|,|R|) / min(|L|,|R|) <= max_cluster_size_ratio`.
    pub max_cluster_size_ratio: f32,
    /// Iteration cap for Lloyd's algorithm within a single bisection.
    pub max_kmeans_iters: usize,
    /// Iteration cap for the post-Lloyd rebalance pass within a bisection.
    pub max_rebalance_iters: usize,
    /// LIRE's `R`: how many neighboring postings a Split/Merge event
    /// rechecks for NPA violations. Clamped to the active posting count.
    pub reassign_radius: usize,
    /// How many posting centroids `search` probes per query.
    pub nprobe: usize,
    /// Seed for the deterministic RNG driving k-means seeding — same input,
    /// same index, every time.
    pub seed: u64,
    /// Independent toggle (not a master switch) so ablation tests/benches
    /// can reproduce the paper's own Figure 10 comparison on one op
    /// sequence.
    pub enable_split: bool,
    pub enable_merge: bool,
    pub enable_reassign: bool,
}

impl ClusterIndexConfig {
    /// Sensible defaults for kosha's segment scale (hundreds to tens of
    /// thousands of vectors per segment) — not the paper's billion-scale
    /// defaults (e.g. `reassign_radius = 64`), scaled down since a kosha
    /// segment has far fewer total postings to begin with.
    pub fn new(dim: usize) -> Self {
        Self {
            dim,
            target_posting_size: 64,
            max_posting_size: 128,
            min_posting_size: 16,
            max_cluster_size_ratio: 2.0,
            max_kmeans_iters: 20,
            max_rebalance_iters: 10,
            reassign_radius: 8,
            nprobe: 16,
            seed: 0x5EED_5EED_5EED_5EED,
            enable_split: true,
            enable_merge: true,
            enable_reassign: true,
        }
    }
}

fn validate_config(cfg: &ClusterIndexConfig) -> Result<(), VectorIndexError> {
    if cfg.dim == 0 {
        return Err(VectorIndexError::InvalidConfig("dim must be > 0"));
    }
    if cfg.min_posting_size == 0 {
        return Err(VectorIndexError::InvalidConfig(
            "min_posting_size must be > 0",
        ));
    }
    if !(cfg.min_posting_size <= cfg.target_posting_size
        && cfg.target_posting_size <= cfg.max_posting_size)
    {
        return Err(VectorIndexError::InvalidConfig(
            "expected min_posting_size <= target_posting_size <= max_posting_size",
        ));
    }
    if cfg.max_cluster_size_ratio < 1.0 {
        return Err(VectorIndexError::InvalidConfig(
            "max_cluster_size_ratio must be >= 1.0",
        ));
    }
    if cfg.max_kmeans_iters == 0 {
        return Err(VectorIndexError::InvalidConfig(
            "max_kmeans_iters must be > 0",
        ));
    }
    if cfg.nprobe == 0 {
        return Err(VectorIndexError::InvalidConfig("nprobe must be > 0"));
    }
    Ok(())
}

/// Point-in-time index shape, for observability/tests — not itself part of
/// the algorithm.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IndexStats {
    pub active_postings: usize,
    pub live_vectors: usize,
    pub tombstoned_entries: usize,
    pub min_posting_size: usize,
    pub max_posting_size: usize,
    pub avg_posting_size: f64,
}

/// Result of an explicit [`ClusterIndex::rebalance`] pass.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RebalanceStats {
    pub postings_split: usize,
    pub postings_merged: usize,
    pub entries_gced: usize,
}

/// A read-only snapshot of one posting's live content — centroid plus
/// `(id, vector)` pairs — for external callers that want to persist the
/// index (e.g. `kosha-segment`'s on-disk writer, via [`ClusterIndex::snapshot`]).
///
/// Tombstoned entries are never included: a persisted snapshot has no
/// notion of "this used to exist but was deleted" — callers that need
/// deletes handle them at their own layer (kosha's segments already do,
/// via document-level tombstones filtered at query time).
#[derive(Debug, Clone, PartialEq)]
pub struct PostingSnapshot {
    pub centroid: Vec<f32>,
    pub entries: Vec<(u32, Vec<f32>)>,
    /// Max cosine distance from `centroid` to any entry in this posting —
    /// a triangle-inequality bound: `distance(query, member) >=
    /// distance(query, centroid) - radius` for every member, so
    /// `(distance(query, centroid) - radius).max(0.0)` is a safe LOWER
    /// bound on how close this posting's closest member could possibly be
    /// to any query. Ranking postings by that bound instead of raw
    /// centroid distance is what lets a "diffuse" posting (small centroid
    /// pull, but a genuinely close member buried inside it) still qualify
    /// for probing — see kosha-query's
    /// `thin_global_budget_can_bury_the_true_neighbour_in_a_diluted_centroid`
    /// for the failure mode this exists to fix at the source, rather than
    /// compensating for it with a wider raw probe budget.
    pub radius: f32,
}

/// A SPANN-style cluster/posting ANN index, kept balanced by LIRE.
///
/// Rebalancing (`Split`/`Merge`/`Reassign`) runs **synchronously inline** at
/// the end of `insert`/`delete`, not on a background thread pool: at
/// kosha's segment scale a rebalance event touches one posting plus at most
/// `reassign_radius` neighbors, so inline is cheap enough that the paper's
/// concurrent Local Rebuilder (built to amortize cost across a
/// billion-vector, multi-threaded index) isn't needed. See `README.md`.
#[derive(Debug)]
pub struct ClusterIndex {
    pub(crate) config: ClusterIndexConfig,
    pub(crate) postings: Vec<Option<Posting>>,
    pub(crate) free_slots: Vec<PostingId>,
    pub(crate) owner_map: HashMap<u32, PostingId>,
    pub(crate) rng: DeterministicRng,
}

impl ClusterIndex {
    /// Builds an index from a full vector set via recursive balanced
    /// bisection (see `build.rs`). `vectors` must have unique ids and
    /// uniform dimensionality matching `config.dim`.
    pub fn build(
        vectors: &[(u32, Vec<f32>)],
        config: ClusterIndexConfig,
    ) -> Result<Self, VectorIndexError> {
        build::build(vectors, config)
    }

    /// LIRE's Insert: append to the nearest posting, splitting it if that
    /// overflows `max_posting_size`.
    pub fn insert(&mut self, id: u32, vector: Vec<f32>) -> Result<(), VectorIndexError> {
        ops::insert::insert(self, id, vector)
    }

    /// LIRE's Delete: tombstone in place. Returns `false` if `id` isn't
    /// currently live in the index (already deleted, or never inserted).
    pub fn delete(&mut self, id: u32) -> bool {
        ops::delete::delete(self, id)
    }

    /// Probes `config.nprobe` nearest posting centroids and returns the
    /// `k` nearest live vectors found, descending by cosine similarity.
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(u32, f32)>, VectorIndexError> {
        search::search(self, query, k)
    }

    /// An explicit, full rebalance pass: physically GCs every posting's
    /// tombstones, then re-checks every active posting against the
    /// split/merge thresholds. `insert`/`delete` already keep the index
    /// balanced incrementally — this is a batch convenience (used by tests,
    /// and documented as the hook a future background rebuilder would call).
    pub fn rebalance(&mut self) -> RebalanceStats {
        let mut stats = RebalanceStats::default();

        let ids: Vec<PostingId> = self.active_posting_ids().collect();
        for pid in &ids {
            if let Some(p) = self.postings[*pid].as_mut() {
                let before = p.entries.len();
                p.gc();
                stats.entries_gced += before - p.entries.len();
            }
        }

        let ids: Vec<PostingId> = self.active_posting_ids().collect();
        for pid in ids {
            // May already have been split/merged away by an earlier
            // iteration's cascade.
            let Some(p) = self.postings[pid].as_ref() else {
                continue;
            };
            let live = p.live_count();
            if self.config.enable_split && live > self.config.max_posting_size {
                ops::split::split_posting(self, pid, 0);
                stats.postings_split += 1;
            } else if self.config.enable_merge
                && live < self.config.min_posting_size
                && self.active_posting_count() > 1
            {
                ops::merge::merge_posting(self, pid);
                stats.postings_merged += 1;
            }
        }

        stats
    }

    /// A read-only snapshot of every active posting's live content, for
    /// external serialization. See [`PostingSnapshot`].
    pub fn snapshot(&self) -> Vec<PostingSnapshot> {
        self.active_posting_ids()
            .map(|pid| {
                let p = self.postings[pid]
                    .as_ref()
                    .expect("active_posting_ids only yields Some slots");
                let entries: Vec<(u32, Vec<f32>)> =
                    p.live_entries().map(|e| (e.id, e.vector.clone())).collect();
                // Free byproduct of the pass that already visits every
                // live entry to clone it — no second scan.
                let radius = entries
                    .iter()
                    .map(|(_, v)| cosine_distance(&p.centroid, v))
                    .fold(0.0f32, f32::max);
                PostingSnapshot {
                    centroid: p.centroid.clone(),
                    entries,
                    radius,
                }
            })
            .collect()
    }

    pub fn stats(&self) -> IndexStats {
        let sizes: Vec<usize> = self
            .active_posting_ids()
            .map(|pid| self.postings[pid].as_ref().unwrap().live_count())
            .collect();
        let tombstoned: usize = self
            .active_posting_ids()
            .map(|pid| {
                let p = self.postings[pid].as_ref().unwrap();
                p.entries.len() - p.live_count()
            })
            .sum();
        let active = sizes.len();
        IndexStats {
            active_postings: active,
            live_vectors: sizes.iter().sum(),
            tombstoned_entries: tombstoned,
            min_posting_size: sizes.iter().copied().min().unwrap_or(0),
            max_posting_size: sizes.iter().copied().max().unwrap_or(0),
            avg_posting_size: if active == 0 {
                0.0
            } else {
                sizes.iter().sum::<usize>() as f64 / active as f64
            },
        }
    }

    pub fn config(&self) -> &ClusterIndexConfig {
        &self.config
    }

    pub fn len(&self) -> usize {
        self.owner_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.owner_map.is_empty()
    }

    pub fn contains(&self, id: u32) -> bool {
        self.owner_map.contains_key(&id)
    }

    /// Live sizes of every active posting — for tests/observability.
    pub fn posting_sizes(&self) -> Vec<usize> {
        self.active_posting_ids()
            .map(|pid| self.postings[pid].as_ref().unwrap().live_count())
            .collect()
    }

    pub fn active_posting_count(&self) -> usize {
        self.postings.iter().filter(|p| p.is_some()).count()
    }

    /// All currently-live ids, sorted ascending.
    pub fn ids(&self) -> Vec<u32> {
        let mut v: Vec<u32> = self.owner_map.keys().copied().collect();
        v.sort_unstable();
        v
    }

    fn empty(config: ClusterIndexConfig) -> Self {
        let rng = DeterministicRng::new(config.seed);
        Self {
            config,
            postings: Vec::new(),
            free_slots: Vec::new(),
            owner_map: HashMap::new(),
            rng,
        }
    }

    pub(crate) fn active_posting_ids(&self) -> impl Iterator<Item = PostingId> + '_ {
        self.postings
            .iter()
            .enumerate()
            .filter_map(|(i, p)| p.is_some().then_some(i))
    }

    pub(crate) fn alloc_slot(&mut self, posting: Posting) -> PostingId {
        if let Some(id) = self.free_slots.pop() {
            self.postings[id] = Some(posting);
            id
        } else {
            self.postings.push(Some(posting));
            self.postings.len() - 1
        }
    }

    pub(crate) fn free_slot(&mut self, id: PostingId) {
        self.postings[id] = None;
        self.free_slots.push(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vec_at(dim: usize, angle_bucket: f32, jitter: f32) -> Vec<f32> {
        // A crude but deterministic way to place points on a circle in the
        // first two dims (rest zero) — enough to build well-separated
        // synthetic clusters for cosine distance.
        let mut v = vec![0.0f32; dim];
        v[0] = angle_bucket.cos() + jitter;
        v[1] = angle_bucket.sin() + jitter;
        v
    }

    #[test]
    fn build_insert_search_roundtrip() {
        let dim = 4;
        let mut vectors = Vec::new();
        for i in 0..40u32 {
            let angle = (i as f32) * std::f32::consts::PI / 20.0;
            vectors.push((i, vec_at(dim, angle, 0.0)));
        }
        let cfg = ClusterIndexConfig::new(dim);
        let mut idx = ClusterIndex::build(&vectors, cfg).unwrap();
        assert_eq!(idx.len(), 40);

        let results = idx.search(&vec_at(dim, 0.0, 0.0), 3).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 0); // exact match should be its own nearest neighbor

        idx.insert(1000, vec_at(dim, 0.01, 0.0)).unwrap();
        assert!(idx.contains(1000));
        assert!(idx.delete(1000));
        assert!(!idx.contains(1000));
        assert!(!idx.delete(1000)); // already gone
    }

    #[test]
    fn snapshot_covers_every_live_vector_exactly_once_and_matches_stats() {
        let dim = 4;
        let mut vectors = Vec::new();
        for i in 0..60u32 {
            let angle = (i as f32) * std::f32::consts::PI / 30.0;
            vectors.push((i, vec_at(dim, angle, 0.0)));
        }
        let mut cfg = ClusterIndexConfig::new(dim);
        cfg.target_posting_size = 8;
        cfg.max_posting_size = 16;
        cfg.min_posting_size = 2;
        let mut idx = ClusterIndex::build(&vectors, cfg).unwrap();
        idx.delete(0);
        idx.delete(1);

        let snap = idx.snapshot();
        assert_eq!(snap.len(), idx.active_posting_count());

        let mut seen: Vec<u32> = snap
            .iter()
            .flat_map(|p| p.entries.iter().map(|(id, _)| *id))
            .collect();
        seen.sort_unstable();
        let mut expected = idx.ids();
        expected.sort_unstable();
        assert_eq!(
            seen, expected,
            "snapshot must cover exactly the live id set, no more, no less"
        );

        // No tombstones should ever surface in a snapshot.
        assert!(!seen.contains(&0));
        assert!(!seen.contains(&1));

        // Each posting's centroid dimension must match the index's.
        for p in &snap {
            assert_eq!(p.centroid.len(), dim);
            for (_, v) in &p.entries {
                assert_eq!(v.len(), dim);
            }
        }
    }

    #[test]
    fn snapshot_radius_is_the_true_max_distance_to_any_live_member() {
        // Direct, independent check: recompute each posting's radius by
        // brute force from its own entries and confirm it matches exactly
        // — not just "some non-negative number got set".
        let dim = 4;
        let mut vectors = Vec::new();
        for i in 0..60u32 {
            let angle = (i as f32) * std::f32::consts::PI / 30.0;
            vectors.push((i, vec_at(dim, angle, 0.0)));
        }
        let mut cfg = ClusterIndexConfig::new(dim);
        cfg.target_posting_size = 8;
        cfg.max_posting_size = 16;
        cfg.min_posting_size = 2;
        let idx = ClusterIndex::build(&vectors, cfg).unwrap();
        let snap = idx.snapshot();

        assert!(!snap.is_empty());
        for p in &snap {
            let brute_force_radius = p
                .entries
                .iter()
                .map(|(_, v)| cosine_distance(&p.centroid, v))
                .fold(0.0f32, f32::max);
            assert!(
                (p.radius - brute_force_radius).abs() < 1e-6,
                "radius {} must equal the brute-force max distance {brute_force_radius}",
                p.radius
            );
            // A posting always contains at least its own centroid's
            // "closest" member — radius can never be negative (cosine
            // distance is >= 0 by construction) and a single-entry
            // posting's radius is that entry's exact distance to centroid.
            assert!(p.radius >= 0.0);
        }
    }

    #[test]
    fn duplicate_insert_is_rejected() {
        let dim = 2;
        let cfg = ClusterIndexConfig::new(dim);
        let mut idx = ClusterIndex::build(&[], cfg).unwrap();
        idx.insert(1, vec![1.0, 0.0]).unwrap();
        let err = idx.insert(1, vec![0.0, 1.0]).unwrap_err();
        assert_eq!(err, VectorIndexError::DuplicateId(1));
    }

    #[test]
    fn dimension_mismatch_is_rejected() {
        let cfg = ClusterIndexConfig::new(3);
        let mut idx = ClusterIndex::build(&[], cfg).unwrap();
        let err = idx.insert(1, vec![1.0, 0.0]).unwrap_err();
        assert_eq!(
            err,
            VectorIndexError::DimensionMismatch {
                expected: 3,
                got: 2
            }
        );
    }

    #[test]
    fn invalid_config_is_rejected() {
        let mut cfg = ClusterIndexConfig::new(4);
        cfg.min_posting_size = 0;
        let err = ClusterIndex::build(&[], cfg).unwrap_err();
        assert_eq!(
            err,
            VectorIndexError::InvalidConfig("min_posting_size must be > 0")
        );
    }

    #[test]
    fn empty_build_then_insert_seeds_first_posting() {
        let cfg = ClusterIndexConfig::new(2);
        let mut idx = ClusterIndex::build(&[], cfg).unwrap();
        assert_eq!(idx.active_posting_count(), 0);
        idx.insert(1, vec![1.0, 0.0]).unwrap();
        assert_eq!(idx.active_posting_count(), 1);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn insert_past_max_posting_size_triggers_split() {
        let dim = 2;
        let mut cfg = ClusterIndexConfig::new(dim);
        cfg.target_posting_size = 4;
        cfg.max_posting_size = 8;
        cfg.min_posting_size = 1;
        let mut idx = ClusterIndex::build(&[], cfg).unwrap();
        for i in 0..20u32 {
            let angle = (i as f32) * std::f32::consts::PI / 10.0;
            idx.insert(i, vec_at(dim, angle, 0.0)).unwrap();
        }
        assert!(
            idx.active_posting_count() > 1,
            "20 inserts at max_posting_size=8 must have split"
        );
        for size in idx.posting_sizes() {
            assert!(
                size <= 8,
                "posting size {size} exceeds max_posting_size after split"
            );
        }
    }
}
