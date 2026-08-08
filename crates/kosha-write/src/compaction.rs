//! Size-tiered compaction policy (DESIGN.md §7.1).
//!
//! Selection only — merge I/O and manifest publish live on `Indexer`.

use kosha_core::{Manifest, ManifestEntry, SegmentId};

/// Knobs for size-tiered (lite) compaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPolicy {
    /// Segments with `doc_count >=` this are never merge inputs.
    pub max_mergeable_docs: u32,
    /// Cap on how many segments one pass may merge.
    pub max_segments_per_merge: usize,
    /// No-op unless at least this many mergeable segments exist.
    pub min_mergeable_segments: usize,
    /// `needs_compaction` flips true when mergeable segment count reaches this.
    pub trigger_mergeable_segments: usize,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            max_mergeable_docs: 50_000,
            max_segments_per_merge: 32,
            min_mergeable_segments: 2,
            trigger_mergeable_segments: 8,
        }
    }
}

/// How aggressively to select merge inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompactMode {
    /// Merge only small segments under [`CompactionPolicy`] caps (scheduler-safe).
    #[default]
    Tiered,
    /// Merge every segment present in the plan's candidate set (admin/emergency).
    Full,
}

/// Options for a single compaction pass.
#[derive(Debug, Clone, Default)]
pub struct CompactOptions {
    pub mode: CompactMode,
    pub policy: CompactionPolicy,
}

impl CompactOptions {
    pub fn tiered(policy: CompactionPolicy) -> Self {
        Self {
            mode: CompactMode::Tiered,
            policy,
        }
    }

    pub fn full() -> Self {
        Self {
            mode: CompactMode::Full,
            policy: CompactionPolicy::default(),
        }
    }
}

/// Outcome of one compaction attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactResult {
    pub merged: bool,
    pub segments_before: usize,
    pub segments_after: usize,
    pub segments_merged: usize,
}

/// Planned merge inputs (segment ids + doc counts at selection time).
#[derive(Debug, Clone)]
pub struct MergePlan {
    pub inputs: Vec<ManifestEntry>,
}

impl MergePlan {
    pub fn input_ids(&self) -> Vec<SegmentId> {
        self.inputs.iter().map(|e| e.segment_id.clone()).collect()
    }
}

/// Count segments that would be eligible as tiered merge inputs.
pub fn mergeable_segment_count(manifest: &Manifest, policy: &CompactionPolicy) -> usize {
    manifest
        .segments
        .iter()
        .filter(|e| e.doc_count < policy.max_mergeable_docs)
        .count()
}

/// Whether a scheduler should consider running a tiered pass.
pub fn needs_compaction(manifest: &Manifest, policy: &CompactionPolicy) -> bool {
    mergeable_segment_count(manifest, policy) >= policy.trigger_mergeable_segments
}

/// Select merge inputs under `opts`.
///
/// `is_local` filters out segments that are not present on disk (same safe
/// partial-merge behavior as the legacy all-to-one compact path).
pub fn select_merge_inputs<F>(
    manifest: &Manifest,
    opts: &CompactOptions,
    mut is_local: F,
) -> Option<MergePlan>
where
    F: FnMut(&SegmentId) -> bool,
{
    let mut candidates: Vec<ManifestEntry> = manifest
        .segments
        .iter()
        .filter(|e| is_local(&e.segment_id))
        .cloned()
        .collect();

    if candidates.len() < 2 {
        return None;
    }

    match opts.mode {
        CompactMode::Full => Some(MergePlan { inputs: candidates }),
        CompactMode::Tiered => {
            candidates.retain(|e| e.doc_count < opts.policy.max_mergeable_docs);
            if candidates.len() < opts.policy.min_mergeable_segments {
                return None;
            }
            candidates.sort_by_key(|e| e.doc_count);
            candidates.truncate(opts.policy.max_segments_per_merge);
            if candidates.len() < opts.policy.min_mergeable_segments {
                return None;
            }
            Some(MergePlan { inputs: candidates })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::SegmentId;

    fn entry(id: &str, docs: u32) -> ManifestEntry {
        ManifestEntry {
            segment_id: SegmentId(id.into()),
            doc_count: docs,
        }
    }

    fn manifest(segments: Vec<ManifestEntry>) -> Manifest {
        Manifest {
            version: 1,
            segments,
            segment_footers: Default::default(),
        }
    }

    #[test]
    fn tiered_skips_large_segments_and_caps_batch() {
        let m = manifest(vec![
            entry("tiny-a", 10),
            entry("tiny-b", 20),
            entry("tiny-c", 30),
            entry("huge", 80_000),
        ]);
        let opts = CompactOptions {
            mode: CompactMode::Tiered,
            policy: CompactionPolicy {
                max_mergeable_docs: 50_000,
                max_segments_per_merge: 2,
                min_mergeable_segments: 2,
                trigger_mergeable_segments: 8,
            },
        };
        let plan = select_merge_inputs(&m, &opts, |_| true).expect("plan");
        assert_eq!(plan.inputs.len(), 2);
        assert_eq!(plan.inputs[0].segment_id.0, "tiny-a");
        assert_eq!(plan.inputs[1].segment_id.0, "tiny-b");
        assert!(!plan.inputs.iter().any(|e| e.segment_id.0 == "huge"));
    }

    #[test]
    fn tiered_noop_when_below_min_mergeable() {
        let m = manifest(vec![entry("a", 10), entry("huge", 80_000)]);
        let opts = CompactOptions::tiered(CompactionPolicy::default());
        assert!(select_merge_inputs(&m, &opts, |_| true).is_none());
    }

    #[test]
    fn full_merges_all_local_segments() {
        let m = manifest(vec![
            entry("a", 10),
            entry("b", 80_000),
            entry("missing", 5),
        ]);
        let opts = CompactOptions::full();
        let plan = select_merge_inputs(&m, &opts, |id| id.0 != "missing").expect("plan");
        assert_eq!(plan.inputs.len(), 2);
        assert!(plan.inputs.iter().any(|e| e.segment_id.0 == "b"));
    }

    #[test]
    fn needs_compaction_respects_trigger() {
        let mut segments = Vec::new();
        for i in 0..8 {
            segments.push(entry(&format!("s{i}"), 1));
        }
        let m = manifest(segments);
        assert!(needs_compaction(&m, &CompactionPolicy::default()));
        let m2 = manifest(vec![entry("a", 1), entry("b", 1)]);
        assert!(!needs_compaction(&m2, &CompactionPolicy::default()));
    }
}
