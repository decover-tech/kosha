use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};

use serde::{Deserialize, Serialize};

use crate::{cosine_distance, dot, normalize_in_place};

const ROUTING_AXES: usize = 8;
const FORMAT: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Centroid {
    pub vector: Vec<f32>,
    pub radius: f32,
    pub unit_norm: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Column {
    pub field: String,
    pub dim: usize,
    pub centroids: Vec<Centroid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Entry {
    posting: u32,
    centroid: Centroid,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct Cell {
    segments: BTreeMap<String, Vec<Entry>>,
    radii: BTreeMap<u32, usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct FieldDirectory {
    cells: BTreeMap<u8, Cell>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Segment {
    cells: BTreeMap<String, Vec<u8>>,
    counts: BTreeMap<String, usize>,
}

/// A namespace-wide routing directory. Each field and dimension has a
/// sparse orthant index. Segment commits insert and remove only the rows
/// owned by changed segments; cell boundaries never need retraining.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Directory {
    format: u32,
    segments: BTreeMap<String, Segment>,
    fields: BTreeMap<String, FieldDirectory>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: u64,
    pub directory: Directory,
}

impl Snapshot {
    /// Approximate heap bytes for the routing directory, including spare
    /// B-tree slots and node overhead. The snapshot itself is inline.
    pub fn estimated_heap_bytes(&self) -> usize {
        fn tree<K, V>(map: &BTreeMap<K, V>) -> usize {
            // std's BTreeMap nodes allocate all 11 key/value slots up front.
            // Non-root nodes hold at least five entries. Internal nodes also
            // allocate 12 child pointers and have at least six children below the root.
            const NODE_SLOTS: usize = 11;
            const MIN_NON_ROOT_ENTRIES: usize = 5;
            const NODE_OVERHEAD: usize = 64;
            if map.is_empty() {
                return 0;
            }
            let nodes = 1 + (map.len() - 1) / MIN_NON_ROOT_ENTRIES;
            let internal = (nodes + 3) / (MIN_NON_ROOT_ENTRIES + 1);
            let nodes = if internal == 0 { 1 } else { nodes };
            nodes * (NODE_SLOTS * size_of::<(K, V)>() + NODE_OVERHEAD)
                + internal * (NODE_SLOTS + 1) * size_of::<usize>()
        }

        let directory = &self.directory;
        let mut bytes = tree(&directory.segments) + tree(&directory.fields);
        for (id, segment) in &directory.segments {
            bytes += id.capacity() + tree(&segment.cells) + tree(&segment.counts);
            bytes += segment
                .cells
                .iter()
                .map(|(key, cells)| key.capacity() + cells.capacity())
                .sum::<usize>();
            bytes += segment.counts.keys().map(String::capacity).sum::<usize>();
        }
        for (name, field) in &directory.fields {
            bytes += name.capacity() + tree(&field.cells);
            for cell in field.cells.values() {
                bytes += tree(&cell.segments) + tree(&cell.radii);
                for (id, entries) in &cell.segments {
                    bytes += id.capacity() + entries.capacity() * size_of::<Entry>();
                    bytes += entries
                        .iter()
                        .map(|entry| entry.centroid.vector.capacity() * size_of::<f32>())
                        .sum::<usize>();
                }
            }
        }
        bytes
    }

    pub fn matches<'a>(
        &self,
        version: u64,
        segments: impl ExactSizeIterator<Item = &'a str>,
    ) -> bool {
        self.version == version
            && self.directory.is_supported()
            && self.directory.segments.len() == segments.len()
            && segments
                .into_iter()
                .all(|id| self.directory.contains_segment(id))
    }
}

pub const FILE_NAME: &str = "namespace-centroids.v1.json";

pub fn enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        matches!(
            std::env::var("KOSHA_KNN_NAMESPACE_CENTROIDS")
                .ok()
                .as_deref()
                .map(str::trim),
            Some("1" | "true" | "on")
        )
    })
}

impl Default for Directory {
    fn default() -> Self {
        Self {
            format: FORMAT,
            segments: BTreeMap::new(),
            fields: BTreeMap::new(),
        }
    }
}

fn field_key(field: &str, dim: usize) -> String {
    format!("{dim}:{field}")
}

fn cell_key(v: &[f32]) -> u8 {
    v.iter()
        .take(ROUTING_AXES)
        .enumerate()
        .fold(0, |key, (axis, &value)| {
            key | (u8::from(value < 0.0) << axis)
        })
}

impl Directory {
    pub fn is_supported(&self) -> bool {
        self.format == FORMAT
    }

    pub fn contains_segment(&self, id: &str) -> bool {
        self.segments.contains_key(id)
    }

    pub fn segment_ids(&self) -> impl Iterator<Item = &str> {
        self.segments.keys().map(String::as_str)
    }

    pub fn posting_count(&self, id: &str, field: &str, dim: usize) -> usize {
        self.segments
            .get(id)
            .and_then(|s| s.counts.get(&field_key(field, dim)))
            .copied()
            .unwrap_or(0)
    }

    pub fn insert_segment(&mut self, id: &str, columns: Vec<Column>) {
        self.remove_segment(id);
        let mut segment = Segment {
            cells: BTreeMap::new(),
            counts: BTreeMap::new(),
        };
        for col in columns {
            if col.centroids.is_empty() {
                continue;
            }
            let key = field_key(&col.field, col.dim);
            segment.counts.insert(key.clone(), col.centroids.len());
            let field = self.fields.entry(key.clone()).or_default();
            let mut cells = BTreeMap::<u8, Vec<Entry>>::new();
            for (posting, centroid) in col.centroids.into_iter().enumerate() {
                cells
                    .entry(cell_key(&centroid.vector))
                    .or_default()
                    .push(Entry {
                        posting: posting as u32,
                        centroid,
                    });
            }
            for (cell, entries) in cells {
                let bucket = field.cells.entry(cell).or_default();
                for entry in &entries {
                    *bucket
                        .radii
                        .entry(entry.centroid.radius.max(0.0).to_bits())
                        .or_default() += 1;
                }
                bucket.segments.insert(id.to_owned(), entries);
                segment.cells.entry(key.clone()).or_default().push(cell);
            }
        }
        self.segments.insert(id.to_owned(), segment);
    }

    pub fn remove_segment(&mut self, id: &str) {
        let Some(segment) = self.segments.remove(id) else {
            return;
        };
        for (key, cells) in segment.cells {
            let Some(field) = self.fields.get_mut(&key) else {
                continue;
            };
            for cell in cells {
                if let Some(bucket) = field.cells.get_mut(&cell) {
                    if let Some(entries) = bucket.segments.remove(id) {
                        for entry in entries {
                            let radius = entry.centroid.radius.max(0.0).to_bits();
                            if let Some(count) = bucket.radii.get_mut(&radius) {
                                *count -= 1;
                                if *count == 0 {
                                    bucket.radii.remove(&radius);
                                }
                            }
                        }
                    }
                    if bucket.segments.is_empty() {
                        field.cells.remove(&cell);
                    }
                }
            }
            if field.cells.is_empty() {
                self.fields.remove(&key);
            }
        }
    }

    /// Returns exactly the best `limit` eligible postings in ascending
    /// probe-score order, with ties in segment-id and posting order.
    pub fn lookup<'a>(
        &'a self,
        field: &str,
        query: &[f32],
        alpha: f32,
        limit: usize,
        eligible: impl Fn(&str) -> bool,
    ) -> (Vec<Hit<'a>>, usize) {
        let Some(field) = self.fields.get(&field_key(field, query.len())) else {
            return (Vec::new(), 0);
        };
        if limit == 0 {
            return (Vec::new(), 0);
        }
        let mut unit = query.to_vec();
        normalize_in_place(&mut unit);
        // Coordinates with opposite signs cannot contribute positive dot
        // product. Cauchy-Schwarz on the remaining axes bounds similarity.
        let query_cell = cell_key(&unit);
        let cell_bound = |key: u8, max_radius: f32| {
            let opposite = key ^ query_cell;
            let opposite_norm_sq = unit
                .iter()
                .take(ROUTING_AXES)
                .enumerate()
                .filter(|(axis, _)| opposite & (1 << axis) != 0)
                .map(|(_, x)| x * x)
                .sum::<f32>();
            (1.0 - (1.0 - opposite_norm_sq).max(0.0).sqrt() - alpha * max_radius - 0.001).max(0.0)
        };
        let mut cells: Vec<_> = field
            .cells
            .iter()
            .map(|(&k, c)| {
                let max_radius = c
                    .radii
                    .last_key_value()
                    .map_or(0.0, |(&r, _)| f32::from_bits(r));
                (cell_bound(k, max_radius), c)
            })
            .collect();
        cells.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut hits = BinaryHeap::<Hit<'a>>::new();
        let mut scored = 0;
        for (bound, cell) in cells {
            if hits.len() == limit && hits.peek().is_some_and(|hit| hit.score < bound) {
                break;
            }
            for (id, entries) in &cell.segments {
                if hits.len() == limit
                    && hits
                        .peek()
                        .is_some_and(|worst| worst.score == bound && worst.segment < id.as_str())
                {
                    break;
                }
                if !eligible(id) {
                    continue;
                }
                for entry in entries {
                    let c = &entry.centroid;
                    let distance = if c.unit_norm {
                        1.0 - dot(&unit, &c.vector).clamp(-1.0, 1.0)
                    } else {
                        cosine_distance(query, &c.vector)
                    };
                    scored += 1;
                    let hit = Hit {
                        segment: id,
                        posting: entry.posting,
                        score: (distance - alpha * c.radius).max(0.0),
                    };
                    if hits.len() < limit {
                        hits.push(hit);
                    } else if hits.peek().is_some_and(|worst| hit < *worst) {
                        hits.pop();
                        hits.push(hit);
                    }
                }
            }
        }
        (hits.into_sorted_vec(), scored)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Hit<'a> {
    pub segment: &'a str,
    pub posting: u32,
    pub score: f32,
}

impl Eq for Hit<'_> {}

impl PartialEq for Hit<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl PartialOrd for Hit<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Hit<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.segment.cmp(other.segment))
            .then_with(|| self.posting.cmp(&other.posting))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(x: f32, radius: f32) -> Column {
        Column {
            field: "emb".into(),
            dim: 2,
            centroids: vec![Centroid {
                vector: vec![x, 0.0],
                radius,
                unit_norm: true,
            }],
        }
    }

    #[test]
    fn snapshot_heap_estimate_counts_small_segment_map_allocations() {
        let mut snapshot = Snapshot {
            version: 1,
            directory: Directory::default(),
        };
        let segments = 100;
        for i in 0..segments {
            snapshot.directory.insert_segment(&i.to_string(), Vec::new());
        }
        let without_vectors = snapshot.estimated_heap_bytes();
        for i in 0..segments {
            snapshot
                .directory
                .insert_segment(&i.to_string(), vec![column(1.0, 0.0)]);
        }
        let segment_map_slots =
            11 * (size_of::<(String, Vec<u8>)>() + size_of::<(String, usize)>());
        assert!(snapshot.estimated_heap_bytes() >= without_vectors + segments * segment_map_slots);
    }

    #[test]
    fn eligibility_is_applied_before_the_global_limit() {
        let mut directory = Directory::default();
        directory.insert_segment("a", vec![column(1.0, 0.0)]);
        directory.insert_segment("b", vec![column(-1.0, 0.3)]);
        directory.insert_segment("c", vec![column(-1.0, 0.5)]);
        let (hits, _) = directory.lookup("emb", &[1.0, 0.0], 1.0, 1, |id| id != "a");
        assert_eq!(hits.len(), 1);
        assert_eq!(
            (hits[0].segment, hits[0].posting, hits[0].score),
            ("c", 0, 1.5)
        );
    }
}
