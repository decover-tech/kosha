//! An in-memory implementation of the SPFresh/SPANN algorithmic core: a
//! cluster/posting-based ANN index kept balanced by LIRE
//! (**L**ightweight **I**ncremental **RE**-balancing — Split, Merge, and a
//! bounded Reassign, on top of Insert/Delete).
//!
//! kosha builds every segment's vector index with it: a flush clusters
//! from scratch with [`ClusterIndex::build`], and compaction (#253) seeds
//! the merged index from its inputs' postings with
//! [`ClusterIndex::from_postings`] and [`ClusterIndex::absorb_postings`],
//! then repairs it with [`ClusterIndex::rebalance`]. `kosha-segment`
//! serializes [`ClusterIndex::snapshot`] to `vector.idx` +
//! `vector.offsets`. This crate has no I/O. See `README.md` for what is
//! wired where and the simplifications versus the paper.
//!
//! Start at [`ClusterIndex`].

mod build;
mod centroid_index;
mod centroid_probe;
mod error;
mod kmeans;
mod ops;
mod point;
mod posting;
mod quant;
mod rabitq;
pub mod replication;
mod rng;
mod search;

#[cfg(test)]
extern crate self as kosha_vector_spfresh;
#[cfg(test)]
mod lazy_npa;

use std::collections::{HashMap, HashSet};

pub use centroid_probe::{
    nearest_centroids, nearest_centroids_dot, nearest_centroids_dot_rows, nearest_centroids_rows,
};
pub use error::VectorIndexError;
pub use point::{
    cosine_distance, cosine_similarity, dot, dot_lanes, maxsim, mean_vector, normalize_in_place,
};
pub use quant::{
    dequantize_i8, dot_i16_i8, dot_i8, max_i16_query_code, quantize_i16, quantize_i8, I8_MAX_CODE,
};
pub use rabitq::{
    encode_rabitq, rabitq_code_bytes, rabitq_padded_dim, RaBitQCode, RaBitQEstimator, RaBitQQuery,
    RaBitQRotation, RABITQ_EPSILON0, RABITQ_QUERY_BITS, RABITQ_ROTATION_SEED,
};
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
    /// Rounds of post-build refinement in [`ClusterIndex::build`]: after
    /// balanced bisection, every vector moves to the best centroid
    /// among its posting's `build_refine_candidates` nearest postings,
    /// centroids are recomputed, and [`ClusterIndex::rebalance`] restores
    /// the size bounds. Bisection's balance pass pushes far members into
    /// the other half, which leaves loose postings (large radii); this
    /// tightens them. `0` turns refinement off and builds exactly as
    /// before (kosha: `KOSHA_VECTOR_BUILD_REFINE=off`).
    pub build_refine_rounds: usize,
    /// How many nearby postings (including its own) a vector may move to
    /// during build refinement.
    pub build_refine_candidates: usize,
    /// ScaNN's parallel residual weight (η >= 1) for build refinement.
    /// `None` keeps cosine assignment. Split/Merge/Reassign still use cosine.
    pub build_refine_anisotropic_eta: Option<f32>,
    /// Skip cosine comparisons when Elkan's angular half-distance test
    /// proves the current posting wins. Off by default for cost ablations.
    pub enable_elkan_pruning: bool,
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
            build_refine_rounds: 2,
            build_refine_candidates: 16,
            build_refine_anisotropic_eta: None,
            enable_elkan_pruning: false,
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
    if cfg
        .build_refine_anisotropic_eta
        .is_some_and(|eta| !eta.is_finite() || eta < 1.0)
    {
        return Err(VectorIndexError::InvalidConfig(
            "build_refine_anisotropic_eta must be finite and >= 1",
        ));
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
    pub imbalance_factor: f64,
}

/// Faiss posting-size imbalance: `n * sum(size²) / sum(size)²`. Returns
/// 1 for equally sized non-empty postings and 0 when there are no vectors.
pub fn posting_imbalance_factor(sizes: impl IntoIterator<Item = usize>) -> f64 {
    let (n, sum, squares) = sizes
        .into_iter()
        .fold((0.0, 0.0, 0.0), |(n, sum, squares), size| {
            let size = size as f64;
            (n + 1.0, sum + size, squares + size * size)
        });
    if sum == 0.0 {
        0.0
    } else {
        n * squares / (sum * sum)
    }
}

/// Result of an explicit [`ClusterIndex::rebalance`] pass.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RebalanceStats {
    pub postings_split: usize,
    pub postings_merged: usize,
    pub entries_gced: usize,
    /// Postings left over `max_posting_size` because splitting them made
    /// no progress: `balanced_bisect` could not separate them, or Reassign
    /// moved the halves back together (near-identical members).
    pub postings_left_oversized: usize,
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
    /// Set while [`Self::rebalance`] runs: Reassign then leaves postings it
    /// grew past `max_posting_size` to rebalance's own split loop instead
    /// of splitting them in a nested cascade. With many postings oversized
    /// at once (just after `absorb_postings`), cascades would otherwise
    /// chain from one oversized neighbour to the next.
    pub(crate) defer_cascade_splits: bool,
    #[cfg(test)]
    deferred_reassign: Option<lazy_npa::DeferredReassign>,
}

impl ClusterIndex {
    /// Builds an index from a full vector set via recursive balanced
    /// bisection, then (unless `config.build_refine_rounds` is 0) a
    /// bounded assignment refinement and a [`Self::rebalance`] (see
    /// `build.rs`). `vectors` must have unique ids and uniform
    /// dimensionality matching `config.dim`.
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

    /// Seeds an index from existing postings — each snapshot's centroid
    /// and entries become one posting, as given — without any clustering.
    /// This is how compaction reuses its input segments' postings (#253):
    /// the k-means work already done when those segments were written is
    /// kept, and [`Self::rebalance`] then repairs only the postings the
    /// merge actually disturbed (oversized after inserts, undersized after
    /// deletes) with LIRE's own Split/Merge/Reassign.
    ///
    /// Centroids are taken verbatim (`radius` is ignored; [`Self::snapshot`]
    /// recomputes it): a caller that dropped members from a posting should
    /// pass a fresh [`mean_vector`] of what is left. Postings with no
    /// entries are skipped. Errors on a dimension mismatch (centroid or
    /// entry) or an id present in more than one entry.
    pub fn from_postings(
        postings: Vec<PostingSnapshot>,
        config: ClusterIndexConfig,
    ) -> Result<Self, VectorIndexError> {
        validate_config(&config)?;
        let dim = config.dim;
        let mut index = Self::empty(config);
        for snap in postings {
            if snap.entries.is_empty() {
                continue;
            }
            if snap.centroid.len() != dim {
                return Err(VectorIndexError::DimensionMismatch {
                    expected: dim,
                    got: snap.centroid.len(),
                });
            }
            let pid = index.postings.len();
            let mut entries = Vec::with_capacity(snap.entries.len());
            for (id, vector) in snap.entries {
                if vector.len() != dim {
                    return Err(VectorIndexError::DimensionMismatch {
                        expected: dim,
                        got: vector.len(),
                    });
                }
                if index.owner_map.insert(id, pid).is_some() {
                    return Err(VectorIndexError::DuplicateId(id));
                }
                entries.push(posting::PostingEntry {
                    id,
                    vector,
                    deleted: false,
                    version: 0,
                });
            }
            index.postings.push(Some(Posting {
                centroid: snap.centroid,
                entries,
            }));
        }
        Ok(index)
    }

    /// Folds another partition's postings into this index (#253): each
    /// posting's centroid picks its `candidates` nearest postings here
    /// (one centroid-index lookup per absorbed posting — a coarse index
    /// over this index's centroids once it has 256 or more postings, an
    /// exact scan below that), and each of its entries
    /// is appended to the nearest of those. A loose vector is absorbed as
    /// a one-entry posting centred on itself, which makes the choice
    /// exact. Postings that took entries get their centroid recomputed;
    /// none is split, so call [`Self::rebalance`] afterwards. Into an
    /// empty index a posting comes over whole.
    ///
    /// Returns the number of entries absorbed. Errors, leaving the index
    /// unchanged, on a dimension mismatch or an id already present (here
    /// or twice in `postings`).
    pub fn absorb_postings(
        &mut self,
        postings: Vec<PostingSnapshot>,
        candidates: usize,
    ) -> Result<usize, VectorIndexError> {
        ops::absorb::absorb(self, postings, candidates)
    }

    /// An explicit, full rebalance pass: physically GCs every posting's
    /// tombstones, then splits every posting over `max_posting_size`
    /// (repeatedly — a seeded posting can be many times the cap, and one
    /// bisection only halves it) and merges every posting under
    /// `min_posting_size` into its nearest neighbor, each followed by
    /// LIRE's bounded Reassign. Rounds of split-then-merge repeat until
    /// neither fires (a merge can push its target over the cap), up to a
    /// small fixed bound.
    ///
    /// Cost is proportional to the postings that violate a bound (plus
    /// their `reassign_radius` neighbors), not to the index size — beyond
    /// one O(postings) scan per round to find them. `insert`/`delete`
    /// already keep the index balanced incrementally; this is the batch
    /// form, used after [`Self::from_postings`].
    pub fn rebalance(&mut self) -> RebalanceStats {
        /// Each round either splits or merges something, and posting
        /// count is bounded both ways; this only guards a bookkeeping bug.
        const MAX_ROUNDS: usize = 16;
        let mut stats = RebalanceStats::default();

        let ids: Vec<PostingId> = self.active_posting_ids().collect();
        for pid in &ids {
            if let Some(p) = self.postings[*pid].as_mut() {
                let before = p.entries.len();
                p.gc();
                stats.entries_gced += before - p.entries.len();
            }
        }

        self.defer_cascade_splits = true;
        // A posting `balanced_bisect` cannot separate (every member
        // identical) stays oversized; never retry it. The same goes for a
        // posting that a split did not shrink: with near-identical
        // members Reassign moves the two halves back together (each
        // member is nearest the same centroid by a rounding error), so
        // the split "succeeds", the posting is as large as before, and an
        // empty sibling is left behind. Retrying it never ends.
        let mut unsplittable: HashSet<PostingId> = HashSet::new();
        // Backstop for a cycle the per-posting check does not see (two
        // postings trading members): no rebalance needs more splits than
        // a few times the postings its vectors can fill.
        let live_total: usize = self
            .active_posting_ids()
            .map(|pid| self.postings[pid].as_ref().unwrap().live_count())
            .sum();
        let split_budget = 8 * (live_total / self.config.min_posting_size.max(1) + 1) + 64;
        for _ in 0..MAX_ROUNDS {
            let mut changed = false;
            if self.config.enable_split {
                loop {
                    let max = self.config.max_posting_size;
                    let oversized: Vec<PostingId> = self
                        .active_posting_ids()
                        .filter(|pid| {
                            !unsplittable.contains(pid)
                                && self.postings[*pid].as_ref().unwrap().live_count() > max
                        })
                        .collect();
                    if oversized.is_empty() {
                        break;
                    }
                    if stats.postings_split >= split_budget {
                        eprintln!(
                            "WARN: vector index rebalance stopped after {} splits \
                             (budget {split_budget}) with {} posting(s) still over \
                             max_posting_size; leaving them oversized",
                            stats.postings_split,
                            oversized.len()
                        );
                        unsplittable.extend(oversized);
                        break;
                    }
                    let mut split_any = false;
                    for pid in oversized {
                        // A cascade from an earlier split may already have
                        // dealt with it.
                        let live = self.postings[pid].as_ref().map_or(0, |p| p.live_count());
                        if live <= max {
                            continue;
                        }
                        match ops::split::split_posting_sibling(self, pid, 0) {
                            Some(sibling) => {
                                stats.postings_split += 1;
                                split_any = true;
                                // No progress: Reassign emptied one
                                // half back into the other, which holds
                                // as many as the posting did. (A half
                                // that grew from its neighbours while
                                // its sibling kept members is real
                                // progress and is split again.)
                                let count = |index: &Self, id: PostingId| {
                                    index.postings[id].as_ref().map_or(0, |p| p.live_count())
                                };
                                let (left, right) = (count(self, pid), count(self, sibling));
                                if right == 0 && left >= live {
                                    unsplittable.insert(pid);
                                } else if left == 0 && right >= live {
                                    unsplittable.insert(sibling);
                                }
                            }
                            None => {
                                unsplittable.insert(pid);
                            }
                        }
                    }
                    changed |= split_any;
                    if !split_any {
                        break;
                    }
                }
            }
            if self.config.enable_merge {
                let mut active = self.active_posting_count();
                for pid in self.active_posting_ids().collect::<Vec<_>>() {
                    // May already have been merged away by this loop.
                    let Some(p) = self.postings[pid].as_ref() else {
                        continue;
                    };
                    if active > 1 && p.live_count() < self.config.min_posting_size {
                        ops::merge::merge_posting(self, pid);
                        // The merge target grew and the freed slot may be
                        // reused: retry splits from scratch next round.
                        unsplittable.clear();
                        stats.postings_merged += 1;
                        changed = true;
                        // Reassign can cascade into splits, so recount
                        // rather than assume one fewer.
                        active = self.active_posting_count();
                    }
                }
            }
            if !changed {
                break;
            }
        }
        self.defer_cascade_splits = false;
        // A split Reassign undid leaves its sibling empty, and with merges
        // off nothing else removes it.
        for pid in self.active_posting_ids().collect::<Vec<_>>() {
            if self.active_posting_count() > 1
                && self.postings[pid]
                    .as_ref()
                    .is_some_and(|p| p.live_count() == 0)
            {
                self.free_slot(pid);
            }
        }
        let max = self.config.max_posting_size;
        stats.postings_left_oversized = self
            .active_posting_ids()
            .filter(|pid| self.postings[*pid].as_ref().unwrap().live_count() > max)
            .count();

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
            imbalance_factor: posting_imbalance_factor(sizes.iter().copied()),
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
            defer_cascade_splits: false,
            #[cfg(test)]
            deferred_reassign: None,
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

    #[test]
    fn posting_imbalance_counts_size_squared() {
        assert_eq!(posting_imbalance_factor([]), 0.0);
        assert_eq!(posting_imbalance_factor([0, 0]), 0.0);
        assert_eq!(posting_imbalance_factor([4, 4, 4]), 1.0);
        assert_eq!(posting_imbalance_factor([1, 3]), 1.25);
    }

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
