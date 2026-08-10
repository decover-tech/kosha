use std::collections::{HashMap, HashSet};

use kosha_core::KoshaError;

use super::math::{cosine_distance, cosine_similarity};
use super::navigator::CentroidNavigator;
use super::pq::ProductQuantizer;
use super::types::{
    normalize_options, SpFreshEntry, SpFreshOptions, SpFreshPosting, SpFreshStats, SpFreshVersion,
};

/// Cluster/posting-list vector index with SPFresh's LIRE maintenance protocol.
///
/// The implementation keeps Kosha's segment contract simple: persisted segments
/// are still one `vector.idx` snapshot, while this type provides the mutable
/// in-memory operations needed by the foreground updater/local rebuilder model.
#[derive(Debug, Clone, PartialEq)]
pub struct SpFreshIndex {
    pub(crate) options: SpFreshOptions,
    pub(crate) dimensions: usize,
    pub(crate) postings: Vec<SpFreshPosting>,
    pub(crate) version_map: HashMap<u32, SpFreshVersion>,
    pub(crate) next_posting_id: u32,
    pub(crate) pq: Option<ProductQuantizer>,
    pub(crate) pq_codes: HashMap<u32, Vec<u8>>,
}

impl SpFreshIndex {
    pub fn new(dimensions: usize, options: SpFreshOptions) -> Self {
        let options = normalize_options(options);
        Self {
            options,
            dimensions,
            postings: Vec::new(),
            version_map: HashMap::new(),
            next_posting_id: 0,
            pq: None,
            pq_codes: HashMap::new(),
        }
    }

    pub fn try_build(
        vectors: &[(u32, Vec<f32>)],
        options: SpFreshOptions,
    ) -> Result<Self, KoshaError> {
        let dimensions = vectors.first().map(|(_, v)| v.len()).unwrap_or(0);
        let mut index = Self::new(dimensions, options);
        for (doc_seq, vector) in vectors {
            index.insert(*doc_seq, vector.clone())?;
        }
        Ok(index)
    }

    pub fn build(vectors: &[(u32, Vec<f32>)], options: SpFreshOptions) -> Self {
        Self::try_build(vectors, options).expect("vectors must have consistent dimensions")
    }

    pub fn options(&self) -> SpFreshOptions {
        self.options
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub fn postings(&self) -> &[SpFreshPosting] {
        &self.postings
    }

    pub fn stats(&self) -> SpFreshStats {
        let physical_vectors = self.postings.iter().map(|p| p.entries.len()).sum();
        let replica_vectors = self
            .postings
            .iter()
            .flat_map(|p| &p.entries)
            .filter(|entry| entry.is_replica)
            .count();
        SpFreshStats {
            dimensions: self.dimensions,
            postings: self.postings.len(),
            live_vectors: self.live_vectors().len(),
            physical_vectors,
            replica_vectors,
            deleted_vectors: self
                .version_map
                .values()
                .filter(|state| state.deleted)
                .count(),
            pq_encoded_vectors: self.pq_codes.len(),
        }
    }

    pub fn insert(&mut self, doc_seq: u32, vector: Vec<f32>) -> Result<(), KoshaError> {
        self.foreground_insert(doc_seq, vector)?;
        self.stabilize_assignments();
        Ok(())
    }

    pub fn delete(&mut self, doc_seq: u32) -> bool {
        if !self.foreground_delete(doc_seq) {
            return false;
        }
        self.merge_underfull();
        self.stabilize_assignments();
        true
    }

    pub(crate) fn foreground_insert(
        &mut self,
        doc_seq: u32,
        vector: Vec<f32>,
    ) -> Result<(), KoshaError> {
        self.check_vector(&vector)?;
        let version = self
            .version_map
            .get(&doc_seq)
            .map(|state| state.version.wrapping_add(1) & 0x7f)
            .unwrap_or(0);
        self.version_map.insert(
            doc_seq,
            SpFreshVersion {
                version,
                deleted: false,
            },
        );
        self.garbage_collect_all();
        self.append_live(doc_seq, version, vector)?;
        Ok(())
    }

    pub(crate) fn foreground_delete(&mut self, doc_seq: u32) -> bool {
        let Some(state) = self.version_map.get_mut(&doc_seq) else {
            return false;
        };
        state.deleted = true;
        state.version = state.version.wrapping_add(1) & 0x7f;
        self.pq_codes.remove(&doc_seq);
        true
    }

    pub fn search(&self, query: &[f32], k: usize, candidate_postings: usize) -> Vec<(u32, f64)> {
        if k == 0 || query.len() != self.dimensions || self.postings.is_empty() {
            return Vec::new();
        }
        let posting_order = CentroidNavigator::build(&self.postings)
            .nearest_postings(query, candidate_postings.max(1).min(self.postings.len()));
        let limit = candidate_postings.max(1).min(posting_order.len());
        let mut scores: HashMap<u32, f64> = HashMap::new();
        for posting_idx in posting_order.into_iter().take(limit) {
            for entry in &self.postings[posting_idx].entries {
                if !self.is_entry_live(entry) {
                    continue;
                }
                let score = cosine_similarity(query, &entry.vector) as f64;
                scores
                    .entry(entry.doc_seq)
                    .and_modify(|existing| *existing = existing.max(score))
                    .or_insert(score);
            }
        }
        let mut scores: Vec<(u32, f64)> = scores.into_iter().collect();
        scores.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scores.truncate(k);
        scores
    }

    pub fn pq_search_adc(
        &self,
        query: &[f32],
        k: usize,
        candidate_postings: usize,
    ) -> Vec<(u32, f64)> {
        let Some(pq) = &self.pq else {
            return self.search(query, k, candidate_postings);
        };
        if k == 0 || query.len() != self.dimensions || self.postings.is_empty() {
            return Vec::new();
        }
        let posting_order = CentroidNavigator::build(&self.postings)
            .nearest_postings(query, candidate_postings.max(1).min(self.postings.len()));
        let mut distances: HashMap<u32, f32> = HashMap::new();
        for posting_idx in posting_order {
            for entry in &self.postings[posting_idx].entries {
                if !self.is_entry_live(entry) {
                    continue;
                }
                let Some(code) = self.pq_codes.get(&entry.doc_seq) else {
                    continue;
                };
                let distance = pq.adc_distance(query, code);
                distances
                    .entry(entry.doc_seq)
                    .and_modify(|existing| *existing = existing.min(distance))
                    .or_insert(distance);
            }
        }
        let mut scores: Vec<(u32, f64)> = distances
            .into_iter()
            .map(|(doc_seq, distance)| (doc_seq, 1.0 / (1.0 + distance as f64)))
            .collect();
        scores.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scores.truncate(k);
        scores
    }

    pub fn live_vectors(&self) -> Vec<(u32, Vec<f32>)> {
        let mut seen = HashSet::new();
        let mut vectors = Vec::new();
        for posting in &self.postings {
            for entry in &posting.entries {
                if !entry.is_replica && self.is_entry_live(entry) && seen.insert(entry.doc_seq) {
                    vectors.push((entry.doc_seq, entry.vector.clone()));
                }
            }
        }
        vectors.sort_by_key(|(doc_seq, _)| *doc_seq);
        vectors
    }

    fn check_vector(&self, vector: &[f32]) -> Result<(), KoshaError> {
        if self.dimensions == 0 || vector.len() == self.dimensions {
            Ok(())
        } else {
            Err(KoshaError::InvalidFilter(format!(
                "vector dimension mismatch: expected {}, got {}",
                self.dimensions,
                vector.len()
            )))
        }
    }

    fn append_live(
        &mut self,
        doc_seq: u32,
        version: u8,
        vector: Vec<f32>,
    ) -> Result<(), KoshaError> {
        if self.dimensions == 0 {
            self.dimensions = vector.len();
        }
        self.check_vector(&vector)?;
        let entry = SpFreshEntry {
            doc_seq,
            version,
            vector,
            is_replica: false,
        };
        if self.postings.is_empty() {
            let posting = self.new_posting(vec![entry]);
            self.postings.push(posting);
            return Ok(());
        }
        let idx = self.nearest_posting(&entry.vector).unwrap_or(0);
        self.postings[idx].entries.push(entry);
        refresh_centroid(&mut self.postings[idx]);
        Ok(())
    }

    fn new_posting(&mut self, entries: Vec<SpFreshEntry>) -> SpFreshPosting {
        let id = self.next_posting_id;
        self.next_posting_id = self.next_posting_id.wrapping_add(1);
        let centroid = centroid_for_entries(self.dimensions, &entries);
        SpFreshPosting {
            id,
            centroid,
            entries,
        }
    }

    fn rebuild_until_balanced(&mut self) {
        let mut guard = 0;
        while let Some(idx) = self
            .postings
            .iter()
            .position(|posting| self.live_entry_count(posting) > self.options.max_posting_len)
        {
            self.split_posting(idx);
            guard += 1;
            if guard
                > self
                    .postings
                    .len()
                    .saturating_add(self.stats().live_vectors)
            {
                break;
            }
        }
    }

    pub(crate) fn stabilize_assignments(&mut self) {
        let max_passes = self
            .version_map
            .len()
            .saturating_mul(self.postings.len().max(1))
            .max(1);
        for _ in 0..max_passes {
            self.rebuild_until_balanced();
            if !self.repair_nearest_partition_assignments() {
                break;
            }
        }
        self.rebuild_until_balanced();
        self.refresh_boundary_replicas();
        self.refresh_pq_codes();
    }

    pub(crate) fn split_posting(&mut self, idx: usize) {
        if idx >= self.postings.len() {
            return;
        }
        let old = self.postings.remove(idx);
        let old_centroid = old.centroid.clone();
        let live_entries: Vec<SpFreshEntry> = old
            .entries
            .into_iter()
            .filter(|entry| !entry.is_replica && self.is_entry_live(entry))
            .collect();
        if live_entries.len() <= self.options.max_posting_len {
            if !live_entries.is_empty() {
                let posting = self.new_posting(live_entries);
                self.postings.push(posting);
            }
            return;
        }
        let (left, right) = balanced_split_entries(self.dimensions, live_entries);
        let left_posting = self.new_posting(left);
        let right_posting = self.new_posting(right);
        let left_id = left_posting.id;
        let right_id = right_posting.id;
        let left_centroid = left_posting.centroid.clone();
        let right_centroid = right_posting.centroid.clone();

        self.postings.push(left_posting);
        self.postings.push(right_posting);
        self.reassign_after_split(
            old_centroid,
            [left_centroid, right_centroid],
            [left_id, right_id],
        );
    }

    fn reassign_after_split(
        &mut self,
        old_centroid: Vec<f32>,
        new_centroids: [Vec<f32>; 2],
        new_ids: [u32; 2],
    ) {
        let mut candidate_postings =
            self.neighbor_postings(&old_centroid, self.options.split_neighbor_count);
        for new_id in new_ids {
            if let Some(idx) = self
                .postings
                .iter()
                .position(|posting| posting.id == new_id)
            {
                candidate_postings.push(idx);
            }
        }
        candidate_postings.sort_unstable();
        candidate_postings.dedup();

        let mut moves = Vec::new();
        for posting_idx in candidate_postings {
            if posting_idx >= self.postings.len() {
                continue;
            }
            let is_new_posting = new_ids.contains(&self.postings[posting_idx].id);
            let current_id = self.postings[posting_idx].id;
            for entry in &self.postings[posting_idx].entries {
                if entry.is_replica || !self.is_entry_live(entry) {
                    continue;
                }
                let old_dist = cosine_distance(&entry.vector, &old_centroid);
                let best_new = new_centroids
                    .iter()
                    .map(|centroid| cosine_distance(&entry.vector, centroid))
                    .fold(f32::INFINITY, f32::min);
                let should_check = if is_new_posting {
                    old_dist <= best_new
                } else {
                    best_new <= old_dist
                };
                if !should_check {
                    continue;
                }
                if let Some(nearest_idx) = self.nearest_posting(&entry.vector) {
                    let nearest_id = self.postings[nearest_idx].id;
                    if nearest_id != current_id {
                        moves.push((
                            entry.doc_seq,
                            entry.version,
                            entry.vector.clone(),
                            nearest_id,
                        ));
                    }
                }
            }
        }
        self.apply_reassignments(moves);
    }

    fn apply_reassignments(&mut self, moves: Vec<(u32, u8, Vec<f32>, u32)>) {
        for (doc_seq, seen_version, vector, target_posting_id) in moves {
            let Some(state) = self.version_map.get_mut(&doc_seq) else {
                continue;
            };
            if state.deleted || state.version != seen_version {
                continue;
            }
            state.version = state.version.wrapping_add(1) & 0x7f;
            let version = state.version;
            if let Some(target_idx) = self
                .postings
                .iter()
                .position(|posting| posting.id == target_posting_id)
            {
                self.postings[target_idx].entries.push(SpFreshEntry {
                    doc_seq,
                    version,
                    vector,
                    is_replica: false,
                });
                refresh_centroid(&mut self.postings[target_idx]);
            }
        }
        self.garbage_collect_all();
    }

    pub(crate) fn merge_underfull(&mut self) {
        let mut idx = 0;
        while self.postings.len() > 1 && idx < self.postings.len() {
            self.garbage_collect_posting(idx);
            if self.live_entry_count(&self.postings[idx]) >= self.options.min_posting_len {
                idx += 1;
                continue;
            }
            let nearest = self.nearest_other_posting(idx);
            let Some(target_idx) = nearest else {
                idx += 1;
                continue;
            };
            self.merge_pair(idx, target_idx);
            idx = 0;
        }
    }

    fn merge_pair(&mut self, a: usize, b: usize) {
        if a == b || a >= self.postings.len() || b >= self.postings.len() {
            return;
        }
        let (remove_idx, target_idx) = if self.live_entry_count(&self.postings[a])
            <= self.live_entry_count(&self.postings[b])
        {
            (a, b)
        } else {
            (b, a)
        };
        let removed = self.postings.remove(remove_idx);
        let adjusted_target = if remove_idx < target_idx {
            target_idx - 1
        } else {
            target_idx
        };
        let moved_entries: Vec<SpFreshEntry> = removed
            .entries
            .into_iter()
            .filter(|entry| !entry.is_replica && self.is_entry_live(entry))
            .collect();
        self.postings[adjusted_target].entries.extend(moved_entries);
        refresh_centroid(&mut self.postings[adjusted_target]);
        self.reassign_from_posting(adjusted_target);
        self.rebuild_until_balanced();
    }

    fn reassign_from_posting(&mut self, posting_idx: usize) {
        if posting_idx >= self.postings.len() {
            return;
        }
        let current_id = self.postings[posting_idx].id;
        let moves: Vec<_> = self.postings[posting_idx]
            .entries
            .iter()
            .filter(|entry| !entry.is_replica && self.is_entry_live(entry))
            .filter_map(|entry| {
                let nearest_idx = self.nearest_posting(&entry.vector)?;
                let nearest_id = self.postings[nearest_idx].id;
                (nearest_id != current_id).then(|| {
                    (
                        entry.doc_seq,
                        entry.version,
                        entry.vector.clone(),
                        nearest_id,
                    )
                })
            })
            .collect();
        self.apply_reassignments(moves);
    }

    fn repair_nearest_partition_assignments(&mut self) -> bool {
        let mut moves = Vec::new();
        for posting in &self.postings {
            let current_id = posting.id;
            for entry in &posting.entries {
                if entry.is_replica || !self.is_entry_live(entry) {
                    continue;
                }
                if let Some(nearest_idx) = self.nearest_posting(&entry.vector) {
                    let nearest_id = self.postings[nearest_idx].id;
                    if nearest_id != current_id {
                        moves.push((
                            entry.doc_seq,
                            entry.version,
                            entry.vector.clone(),
                            nearest_id,
                        ));
                    }
                }
            }
        }
        let moved = !moves.is_empty();
        self.apply_reassignments(moves);
        moved
    }

    fn refresh_boundary_replicas(&mut self) {
        for posting in &mut self.postings {
            posting.entries.retain(|entry| !entry.is_replica);
        }
        if self.options.boundary_replica_count == 0 || self.postings.len() <= 1 {
            return;
        }
        let primaries: Vec<(u32, u8, Vec<f32>, u32)> = self
            .postings
            .iter()
            .flat_map(|posting| {
                posting
                    .entries
                    .iter()
                    .filter(|entry| !entry.is_replica && self.is_entry_live(entry))
                    .map(|entry| {
                        (
                            entry.doc_seq,
                            entry.version,
                            entry.vector.clone(),
                            posting.id,
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let replica_count = self.options.boundary_replica_count;
        for (doc_seq, version, vector, primary_posting_id) in primaries {
            let neighbor_ids: Vec<u32> = self
                .neighbor_postings(&vector, self.postings.len())
                .into_iter()
                .filter_map(|idx| {
                    let posting = &self.postings[idx];
                    (posting.id != primary_posting_id).then_some(posting.id)
                })
                .take(replica_count)
                .collect();
            for posting_id in neighbor_ids {
                if let Some(posting) = self.postings.iter_mut().find(|p| p.id == posting_id) {
                    posting.entries.push(SpFreshEntry {
                        doc_seq,
                        version,
                        vector: vector.clone(),
                        is_replica: true,
                    });
                }
            }
        }
    }

    fn refresh_pq_codes(&mut self) {
        self.pq = None;
        self.pq_codes.clear();
        if self.options.pq_subvector_count == 0
            || self.dimensions == 0
            || !self
                .dimensions
                .is_multiple_of(self.options.pq_subvector_count)
        {
            return;
        }
        let vectors = self.live_vectors();
        if vectors.is_empty() {
            return;
        }
        let pq = ProductQuantizer::train(
            vectors.iter().map(|(_, vector)| vector.as_slice()),
            self.options.pq_subvector_count,
            self.options.pq_centroids,
        );
        for (doc_seq, vector) in &vectors {
            self.pq_codes.insert(*doc_seq, pq.encode(vector));
        }
        self.pq = Some(pq);
    }

    fn garbage_collect_all(&mut self) {
        for idx in 0..self.postings.len() {
            self.garbage_collect_posting(idx);
        }
        self.postings.retain(|posting| !posting.entries.is_empty());
    }

    fn garbage_collect_posting(&mut self, idx: usize) {
        if idx >= self.postings.len() {
            return;
        }
        let version_map = &self.version_map;
        self.postings[idx].entries.retain(|entry| {
            version_map
                .get(&entry.doc_seq)
                .map(|state| !state.deleted && state.version == entry.version)
                .unwrap_or(false)
        });
        refresh_centroid(&mut self.postings[idx]);
    }

    pub(crate) fn live_entry_count(&self, posting: &SpFreshPosting) -> usize {
        posting
            .entries
            .iter()
            .filter(|entry| !entry.is_replica && self.is_entry_live(entry))
            .count()
    }

    pub(crate) fn is_entry_live(&self, entry: &SpFreshEntry) -> bool {
        self.version_map
            .get(&entry.doc_seq)
            .map(|state| !state.deleted && state.version == entry.version)
            .unwrap_or(false)
    }

    fn nearest_posting(&self, vector: &[f32]) -> Option<usize> {
        self.postings
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                cosine_distance(vector, &a.centroid)
                    .partial_cmp(&cosine_distance(vector, &b.centroid))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(idx, _)| idx)
    }

    fn nearest_other_posting(&self, idx: usize) -> Option<usize> {
        let centroid = &self.postings.get(idx)?.centroid;
        self.postings
            .iter()
            .enumerate()
            .filter(|(other_idx, _)| *other_idx != idx)
            .min_by(|(_, a), (_, b)| {
                cosine_distance(centroid, &a.centroid)
                    .partial_cmp(&cosine_distance(centroid, &b.centroid))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(idx, _)| idx)
    }

    fn neighbor_postings(&self, centroid: &[f32], count: usize) -> Vec<usize> {
        let mut neighbors: Vec<(usize, f32)> = self
            .postings
            .iter()
            .enumerate()
            .map(|(idx, posting)| (idx, cosine_distance(centroid, &posting.centroid)))
            .collect();
        neighbors.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        neighbors
            .into_iter()
            .take(count)
            .map(|(idx, _)| idx)
            .collect()
    }
}

pub(crate) fn balanced_split_entries(
    dimensions: usize,
    entries: Vec<SpFreshEntry>,
) -> (Vec<SpFreshEntry>, Vec<SpFreshEntry>) {
    if entries.len() <= 1 {
        return (entries, Vec::new());
    }
    let (seed_a, seed_b) = farthest_pair(&entries);
    let a = entries[seed_a].vector.clone();
    let b = entries[seed_b].vector.clone();
    let mut scored: Vec<(f32, SpFreshEntry)> = entries
        .into_iter()
        .map(|entry| {
            let da = cosine_distance(&entry.vector, &a);
            let db = cosine_distance(&entry.vector, &b);
            (da - db, entry)
        })
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let mid = scored.len() / 2;
    let mut left: Vec<SpFreshEntry> = scored[..mid]
        .iter()
        .map(|(_, entry)| entry.clone())
        .collect();
    let mut right: Vec<SpFreshEntry> = scored[mid..]
        .iter()
        .map(|(_, entry)| entry.clone())
        .collect();
    if left.is_empty() {
        left.push(right.remove(0));
    } else if right.is_empty() {
        right.push(left.pop().unwrap());
    }
    debug_assert_eq!(
        centroid_for_entries(dimensions, &left).len(),
        centroid_for_entries(dimensions, &right).len()
    );
    (left, right)
}

fn farthest_pair(entries: &[SpFreshEntry]) -> (usize, usize) {
    let mut best = (0, 1);
    let mut best_distance = f32::NEG_INFINITY;
    for i in 0..entries.len() {
        for j in (i + 1)..entries.len() {
            let dist = cosine_distance(&entries[i].vector, &entries[j].vector);
            if dist > best_distance {
                best = (i, j);
                best_distance = dist;
            }
        }
    }
    best
}

pub(crate) fn refresh_centroid(posting: &mut SpFreshPosting) {
    let dimensions = posting.centroid.len();
    let primaries: Vec<SpFreshEntry> = posting
        .entries
        .iter()
        .filter(|entry| !entry.is_replica)
        .cloned()
        .collect();
    posting.centroid = if primaries.is_empty() {
        centroid_for_entries(dimensions, &posting.entries)
    } else {
        centroid_for_entries(dimensions, &primaries)
    };
}

pub(crate) fn centroid_for_entries(dimensions: usize, entries: &[SpFreshEntry]) -> Vec<f32> {
    if dimensions == 0 {
        return Vec::new();
    }
    let mut centroid = vec![0.0; dimensions];
    if entries.is_empty() {
        return centroid;
    }
    for entry in entries {
        for (acc, value) in centroid.iter_mut().zip(&entry.vector) {
            *acc += *value;
        }
    }
    let denom = entries.len() as f32;
    for value in &mut centroid {
        *value /= denom;
    }
    centroid
}
