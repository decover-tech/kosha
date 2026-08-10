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
    /// Byte ceiling on a merge's *combined input size* — no pass may produce
    /// a segment larger than this (`0` disables the cap). Guards memory:
    /// the parsed-segment cache and live-bytes budget evict whole segments,
    /// so an unbounded merge (167 → 1 on the 10M MSMarco bench) produces
    /// one giant, effectively unevictable entry and the box lives in
    /// reclaim stalls. Same idea as Lucene's 5GB `max_merged_segment`;
    /// oversized segments are simply never merge inputs again. `Full` mode
    /// under a cap merges one greedy smallest-first group per pass, so
    /// repeated passes converge to ~`ceil(total_bytes / cap)` segments —
    /// callers loop until `segments_after` stops dropping.
    pub max_merged_segment_bytes: u64,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            max_mergeable_docs: 50_000,
            max_segments_per_merge: 32,
            min_mergeable_segments: 2,
            trigger_mergeable_segments: 8,
            max_merged_segment_bytes: 5 * 1024 * 1024 * 1024,
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
///
/// `segment_bytes` reports a candidate's on-disk size for the
/// `max_merged_segment_bytes` cap. `None` (size unknowable) is treated as
/// "too big to group" — the segment is conservatively left unmerged rather
/// than risking an over-cap output.
pub fn select_merge_inputs<F, G>(
    manifest: &Manifest,
    opts: &CompactOptions,
    mut is_local: F,
    mut segment_bytes: G,
) -> Option<MergePlan>
where
    F: FnMut(&SegmentId) -> bool,
    G: FnMut(&SegmentId) -> Option<u64>,
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

    let cap = opts.policy.max_merged_segment_bytes;
    match opts.mode {
        CompactMode::Full => {
            if cap == 0 {
                return Some(MergePlan { inputs: candidates });
            }
            // Greedy smallest-first group under the cap: one group per
            // pass; repeated passes converge to ~ceil(total/cap) segments.
            // Sort is (size, id) so planning is deterministic; unknown
            // sizes sort last and can never join a capped group.
            let mut sized: Vec<(u64, ManifestEntry)> = candidates
                .into_iter()
                .map(|e| (segment_bytes(&e.segment_id).unwrap_or(u64::MAX), e))
                .collect();
            sized.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then_with(|| a.1.segment_id.0.cmp(&b.1.segment_id.0))
            });
            let mut total = 0u64;
            let mut inputs = Vec::new();
            for (bytes, entry) in sized {
                if total.saturating_add(bytes) > cap {
                    break;
                }
                total += bytes;
                inputs.push(entry);
            }
            if inputs.len() < 2 {
                return None;
            }
            Some(MergePlan { inputs })
        }
        CompactMode::Tiered => {
            candidates.retain(|e| e.doc_count < opts.policy.max_mergeable_docs);
            if candidates.len() < opts.policy.min_mergeable_segments {
                return None;
            }
            candidates.sort_by_key(|e| e.doc_count);
            candidates.truncate(opts.policy.max_segments_per_merge);
            if cap > 0 {
                // Same greedy prefix, in the existing doc-count order.
                let mut total = 0u64;
                let mut kept = Vec::new();
                for entry in candidates {
                    let bytes = segment_bytes(&entry.segment_id).unwrap_or(u64::MAX);
                    if total.saturating_add(bytes) > cap {
                        break;
                    }
                    total += bytes;
                    kept.push(entry);
                }
                candidates = kept;
            }
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
                ..CompactionPolicy::default()
            },
        };
        let plan = select_merge_inputs(&m, &opts, |_| true, |_| Some(1)).expect("plan");
        assert_eq!(plan.inputs.len(), 2);
        assert_eq!(plan.inputs[0].segment_id.0, "tiny-a");
        assert_eq!(plan.inputs[1].segment_id.0, "tiny-b");
        assert!(!plan.inputs.iter().any(|e| e.segment_id.0 == "huge"));
    }

    #[test]
    fn tiered_noop_when_below_min_mergeable() {
        let m = manifest(vec![entry("a", 10), entry("huge", 80_000)]);
        let opts = CompactOptions::tiered(CompactionPolicy::default());
        assert!(select_merge_inputs(&m, &opts, |_| true, |_| Some(1)).is_none());
    }

    #[test]
    fn full_merges_all_local_segments() {
        let m = manifest(vec![
            entry("a", 10),
            entry("b", 80_000),
            entry("missing", 5),
        ]);
        let opts = CompactOptions::full();
        let plan =
            select_merge_inputs(&m, &opts, |id| id.0 != "missing", |_| Some(1)).expect("plan");
        assert_eq!(plan.inputs.len(), 2);
        assert!(plan.inputs.iter().any(|e| e.segment_id.0 == "b"));
    }

    #[test]
    fn full_capped_groups_smallest_first_and_converges() {
        let m = manifest(vec![
            entry("big", 100),
            entry("small-a", 10),
            entry("small-b", 20),
            entry("mid", 50),
        ]);
        let sizes = |id: &SegmentId| -> Option<u64> {
            Some(match id.0.as_str() {
                "small-a" => 10,
                "small-b" => 20,
                "mid" => 50,
                "big" => 4_000,
                _ => unreachable!(),
            })
        };
        let mut opts = CompactOptions::full();
        opts.policy.max_merged_segment_bytes = 100;
        // Greedy smallest-first: small-a(10) + small-b(20) + mid(50) = 80
        // fits; big(4000) would blow the cap and is left alone.
        let plan = select_merge_inputs(&m, &opts, |_| true, sizes).expect("plan");
        let ids: Vec<&str> = plan
            .inputs
            .iter()
            .map(|e| e.segment_id.0.as_str())
            .collect();
        assert_eq!(ids, vec!["small-a", "small-b", "mid"]);

        // Converged state: every remaining pair exceeds the cap → no plan,
        // which is how repeated capped-full passes terminate.
        let m2 = manifest(vec![entry("x", 1), entry("y", 1)]);
        let plan2 = select_merge_inputs(&m2, &opts, |_| true, |_| Some(90));
        assert!(
            plan2.is_none(),
            "two 90-byte segments can't fit a 100-byte cap"
        );
    }

    #[test]
    fn full_cap_zero_disables_the_cap() {
        let m = manifest(vec![entry("a", 1), entry("b", 1), entry("c", 1)]);
        let mut opts = CompactOptions::full();
        opts.policy.max_merged_segment_bytes = 0;
        let plan = select_merge_inputs(&m, &opts, |_| true, |_| Some(u64::MAX)).expect("plan");
        assert_eq!(plan.inputs.len(), 3, "cap 0 must merge everything");
    }

    #[test]
    fn unknown_size_never_joins_a_capped_group() {
        let m = manifest(vec![entry("a", 1), entry("b", 1), entry("c", 1)]);
        let mut opts = CompactOptions::full();
        opts.policy.max_merged_segment_bytes = 100;
        // "c" has no readable size: it must be excluded, the sized pair merges.
        let plan = select_merge_inputs(
            &m,
            &opts,
            |_| true,
            |id| if id.0 == "c" { None } else { Some(10) },
        )
        .expect("plan");
        let ids: Vec<&str> = plan
            .inputs
            .iter()
            .map(|e| e.segment_id.0.as_str())
            .collect();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn tiered_honors_byte_cap() {
        let m = manifest(vec![entry("t1", 10), entry("t2", 20), entry("t3", 30)]);
        let policy = CompactionPolicy {
            max_merged_segment_bytes: 25,
            ..CompactionPolicy::default()
        };
        let opts = CompactOptions::tiered(policy);
        // doc-count order t1,t2,t3 at 10 bytes each: t1+t2=20 fits, t3 would
        // make 30 > 25 → prefix stops at two inputs.
        let plan = select_merge_inputs(&m, &opts, |_| true, |_| Some(10)).expect("plan");
        assert_eq!(plan.inputs.len(), 2);
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
