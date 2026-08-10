use std::collections::{HashMap, HashSet};

use kosha_core::KoshaError;

const MAGIC: &[u8; 8] = b"KSPFRS1\0";
const DEFAULT_MAX_POSTING_LEN: usize = 256;
const DEFAULT_MIN_POSTING_LEN: usize = 32;
const DEFAULT_SPLIT_NEIGHBORS: usize = 8;

/// Tuning knobs for the SPFresh/LIRE vector index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpFreshOptions {
    /// Maximum live entries per posting before the local rebuilder splits it.
    pub max_posting_len: usize,
    /// Minimum live entries before a posting becomes eligible for merge.
    pub min_posting_len: usize,
    /// Number of old-centroid neighbors considered for post-split reassignment.
    pub split_neighbor_count: usize,
}

impl Default for SpFreshOptions {
    fn default() -> Self {
        Self {
            max_posting_len: DEFAULT_MAX_POSTING_LEN,
            min_posting_len: DEFAULT_MIN_POSTING_LEN,
            split_neighbor_count: DEFAULT_SPLIT_NEIGHBORS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SpFreshVersion {
    version: u8,
    deleted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpFreshEntry {
    pub doc_seq: u32,
    pub version: u8,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpFreshPosting {
    pub id: u32,
    pub centroid: Vec<f32>,
    pub entries: Vec<SpFreshEntry>,
}

/// Cluster/posting-list vector index with SPFresh's LIRE maintenance protocol.
///
/// The implementation keeps Kosha's segment contract simple: persisted segments
/// are still one `vector.idx` snapshot, while this type provides the mutable
/// in-memory operations needed by the foreground updater/local rebuilder model.
#[derive(Debug, Clone, PartialEq)]
pub struct SpFreshIndex {
    options: SpFreshOptions,
    dimensions: usize,
    postings: Vec<SpFreshPosting>,
    version_map: HashMap<u32, SpFreshVersion>,
    next_posting_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpFreshStats {
    pub dimensions: usize,
    pub postings: usize,
    pub live_vectors: usize,
    pub physical_vectors: usize,
    pub deleted_vectors: usize,
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
        SpFreshStats {
            dimensions: self.dimensions,
            postings: self.postings.len(),
            live_vectors: self.live_vectors().len(),
            physical_vectors,
            deleted_vectors: self
                .version_map
                .values()
                .filter(|state| state.deleted)
                .count(),
        }
    }

    pub fn insert(&mut self, doc_seq: u32, vector: Vec<f32>) -> Result<(), KoshaError> {
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
        self.stabilize_assignments();
        Ok(())
    }

    pub fn delete(&mut self, doc_seq: u32) -> bool {
        let Some(state) = self.version_map.get_mut(&doc_seq) else {
            return false;
        };
        state.deleted = true;
        state.version = state.version.wrapping_add(1) & 0x7f;
        self.merge_underfull();
        self.stabilize_assignments();
        true
    }

    pub fn search(&self, query: &[f32], k: usize, candidate_postings: usize) -> Vec<(u32, f64)> {
        if k == 0 || query.len() != self.dimensions || self.postings.is_empty() {
            return Vec::new();
        }
        let mut posting_order: Vec<(usize, f32)> = self
            .postings
            .iter()
            .enumerate()
            .map(|(idx, posting)| (idx, cosine_distance(query, &posting.centroid)))
            .collect();
        posting_order.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let limit = candidate_postings.max(1).min(posting_order.len());
        let mut scores = Vec::new();
        for (posting_idx, _) in posting_order.into_iter().take(limit) {
            for entry in &self.postings[posting_idx].entries {
                if !self.is_entry_live(entry) {
                    continue;
                }
                scores.push((
                    entry.doc_seq,
                    cosine_similarity(query, &entry.vector) as f64,
                ));
            }
        }
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.dedup_by_key(|(doc_seq, _)| *doc_seq);
        scores.truncate(k);
        scores
    }

    pub fn live_vectors(&self) -> Vec<(u32, Vec<f32>)> {
        let mut seen = HashSet::new();
        let mut vectors = Vec::new();
        for posting in &self.postings {
            for entry in &posting.entries {
                if self.is_entry_live(entry) && seen.insert(entry.doc_seq) {
                    vectors.push((entry.doc_seq, entry.vector.clone()));
                }
            }
        }
        vectors.sort_by_key(|(doc_seq, _)| *doc_seq);
        vectors
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        put_u32(&mut buf, self.dimensions as u32);
        put_u32(&mut buf, self.next_posting_id);
        put_u32(&mut buf, self.options.max_posting_len as u32);
        put_u32(&mut buf, self.options.min_posting_len as u32);
        put_u32(&mut buf, self.options.split_neighbor_count as u32);

        let mut versions: Vec<(u32, SpFreshVersion)> = self
            .version_map
            .iter()
            .map(|(doc_seq, state)| (*doc_seq, *state))
            .collect();
        versions.sort_by_key(|(doc_seq, _)| *doc_seq);
        put_u32(&mut buf, versions.len() as u32);
        for (doc_seq, state) in versions {
            put_u32(&mut buf, doc_seq);
            buf.push(state.version & 0x7f);
            buf.push(u8::from(state.deleted));
            buf.extend_from_slice(&[0, 0]);
        }

        put_u32(&mut buf, self.postings.len() as u32);
        for posting in &self.postings {
            put_u32(&mut buf, posting.id);
            for &value in &posting.centroid {
                put_f32(&mut buf, value);
            }
            put_u32(&mut buf, posting.entries.len() as u32);
            for entry in &posting.entries {
                put_u32(&mut buf, entry.doc_seq);
                buf.push(entry.version & 0x7f);
                buf.extend_from_slice(&[0, 0, 0]);
                for &value in &entry.vector {
                    put_f32(&mut buf, value);
                }
            }
        }
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Option<Self>, KoshaError> {
        if !data.starts_with(MAGIC) {
            return Ok(None);
        }
        let mut cursor = &data[MAGIC.len()..];
        let dimensions = get_u32(&mut cursor)? as usize;
        let next_posting_id = get_u32(&mut cursor)?;
        let options = normalize_options(SpFreshOptions {
            max_posting_len: get_u32(&mut cursor)? as usize,
            min_posting_len: get_u32(&mut cursor)? as usize,
            split_neighbor_count: get_u32(&mut cursor)? as usize,
        });

        let version_count = get_u32(&mut cursor)? as usize;
        let mut version_map = HashMap::with_capacity(version_count);
        for _ in 0..version_count {
            let doc_seq = get_u32(&mut cursor)?;
            let version = get_u8(&mut cursor)? & 0x7f;
            let deleted = get_u8(&mut cursor)? != 0;
            skip(&mut cursor, 2)?;
            version_map.insert(doc_seq, SpFreshVersion { version, deleted });
        }

        let posting_count = get_u32(&mut cursor)? as usize;
        let mut postings = Vec::with_capacity(posting_count);
        for _ in 0..posting_count {
            let id = get_u32(&mut cursor)?;
            let centroid = get_f32_vec(&mut cursor, dimensions)?;
            let entry_count = get_u32(&mut cursor)? as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let doc_seq = get_u32(&mut cursor)?;
                let version = get_u8(&mut cursor)? & 0x7f;
                skip(&mut cursor, 3)?;
                let vector = get_f32_vec(&mut cursor, dimensions)?;
                entries.push(SpFreshEntry {
                    doc_seq,
                    version,
                    vector,
                });
            }
            postings.push(SpFreshPosting {
                id,
                centroid,
                entries,
            });
        }

        Ok(Some(Self {
            options,
            dimensions,
            postings,
            version_map,
            next_posting_id,
        }))
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

    fn stabilize_assignments(&mut self) {
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
    }

    fn split_posting(&mut self, idx: usize) {
        if idx >= self.postings.len() {
            return;
        }
        let old = self.postings.remove(idx);
        let old_centroid = old.centroid.clone();
        let live_entries: Vec<SpFreshEntry> = old
            .entries
            .into_iter()
            .filter(|entry| self.is_entry_live(entry))
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
                if !self.is_entry_live(entry) {
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
                });
                refresh_centroid(&mut self.postings[target_idx]);
            }
        }
        self.garbage_collect_all();
    }

    fn merge_underfull(&mut self) {
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
            .filter(|entry| self.is_entry_live(entry))
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
            .filter(|entry| self.is_entry_live(entry))
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
                if !self.is_entry_live(entry) {
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

    fn live_entry_count(&self, posting: &SpFreshPosting) -> usize {
        posting
            .entries
            .iter()
            .filter(|entry| self.is_entry_live(entry))
            .count()
    }

    fn is_entry_live(&self, entry: &SpFreshEntry) -> bool {
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

fn normalize_options(mut options: SpFreshOptions) -> SpFreshOptions {
    options.max_posting_len = options.max_posting_len.max(2);
    options.min_posting_len = options.min_posting_len.min(options.max_posting_len / 2);
    options.split_neighbor_count = options.split_neighbor_count.max(1);
    options
}

fn balanced_split_entries(
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

fn refresh_centroid(posting: &mut SpFreshPosting) {
    let dimensions = posting.centroid.len();
    posting.centroid = centroid_for_entries(dimensions, &posting.entries);
}

fn centroid_for_entries(dimensions: usize, entries: &[SpFreshEntry]) -> Vec<f32> {
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

pub fn is_spfresh_vector_index(data: &[u8]) -> bool {
    data.starts_with(MAGIC)
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    (dot / (norm_a * norm_b)).clamp(-1.0, 1.0)
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    1.0 - cosine_similarity(a, b)
}

fn put_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn put_f32(buf: &mut Vec<u8>, value: f32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn get_u8(cursor: &mut &[u8]) -> Result<u8, KoshaError> {
    if cursor.is_empty() {
        return Err(KoshaError::CorruptSegment(
            "truncated spfresh vector index".into(),
        ));
    }
    let value = cursor[0];
    *cursor = &cursor[1..];
    Ok(value)
}

fn get_u32(cursor: &mut &[u8]) -> Result<u32, KoshaError> {
    if cursor.len() < 4 {
        return Err(KoshaError::CorruptSegment(
            "truncated spfresh vector index".into(),
        ));
    }
    let (bytes, rest) = cursor.split_at(4);
    *cursor = rest;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn get_f32(cursor: &mut &[u8]) -> Result<f32, KoshaError> {
    if cursor.len() < 4 {
        return Err(KoshaError::CorruptSegment(
            "truncated spfresh vector index".into(),
        ));
    }
    let (bytes, rest) = cursor.split_at(4);
    *cursor = rest;
    Ok(f32::from_le_bytes(bytes.try_into().unwrap()))
}

fn get_f32_vec(cursor: &mut &[u8], dimensions: usize) -> Result<Vec<f32>, KoshaError> {
    let mut values = Vec::with_capacity(dimensions);
    for _ in 0..dimensions {
        values.push(get_f32(cursor)?);
    }
    Ok(values)
}

fn skip(cursor: &mut &[u8], len: usize) -> Result<(), KoshaError> {
    if cursor.len() < len {
        return Err(KoshaError::CorruptSegment(
            "truncated spfresh vector index".into(),
        ));
    }
    *cursor = &cursor[len..];
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn opts() -> SpFreshOptions {
        SpFreshOptions {
            max_posting_len: 4,
            min_posting_len: 1,
            split_neighbor_count: 4,
        }
    }

    fn stress_opts() -> SpFreshOptions {
        SpFreshOptions {
            max_posting_len: 6,
            min_posting_len: 2,
            split_neighbor_count: 6,
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
                if index.is_entry_live(entry) {
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
                if !index.is_entry_live(entry) {
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

    fn assert_exhaustive_search_matches_exact(
        index: &SpFreshIndex,
        model: &HashMap<u32, Vec<f32>>,
    ) {
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
        assert_eq!(index.search(&[1.0, 0.0], 10, 10)[0].0, 2);
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
}
