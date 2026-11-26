//! Size-tiered compaction policy (DESIGN.md §7.1).
//!
//! Selection only — merge I/O and manifest publish live on `Indexer`.

use std::sync::OnceLock;

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
    /// Research #464: a segment whose tombstoned share of documents is at
    /// or above this, in basis points (1/10,000), is a tiered merge input
    /// whatever its size, is listed first, and may be rewritten on its own
    /// when nothing else fits with it (Lucene's `forceMergeDeletes`).
    /// Tiered selection otherwise never looks at deletes, so a large
    /// segment keeps its tombstoned documents (and their vectors, which
    /// still inflate posting radii and probe cost) until a full pass.
    /// `None` (the default: `KOSHA_COMPACT_TOMBSTONE_RATIO` unset) is that
    /// behaviour.
    pub tombstone_ratio_bp: Option<u32>,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            max_mergeable_docs: 50_000,
            max_segments_per_merge: 32,
            min_mergeable_segments: 2,
            trigger_mergeable_segments: 8,
            max_merged_segment_bytes: 5 * 1024 * 1024 * 1024,
            tombstone_ratio_bp: tombstone_ratio_bp_from_env(),
        }
    }
}

/// `KOSHA_COMPACT_TOMBSTONE_RATIO` (#464, experimental), read once: a
/// fraction in `(0, 1]`, e.g. `0.2`. Unset, empty or `off` disables the
/// trigger; anything else is warned about and disabled.
pub fn tombstone_ratio_bp_from_env() -> Option<u32> {
    static BP: OnceLock<Option<u32>> = OnceLock::new();
    *BP.get_or_init(|| {
        let raw = std::env::var("KOSHA_COMPACT_TOMBSTONE_RATIO").ok();
        let bp = parse_tombstone_ratio(raw.as_deref());
        if let Some(bp) = bp {
            eprintln!(
                "INFO: KOSHA_COMPACT_TOMBSTONE_RATIO={}: segments with at least that share of \
                 tombstoned documents are compaction inputs whatever their size (#464, \
                 experimental)",
                bp as f64 / 10_000.0
            );
        }
        bp
    })
}

fn parse_tombstone_ratio(raw: Option<&str>) -> Option<u32> {
    let raw = raw?.trim();
    if raw.is_empty() || raw.eq_ignore_ascii_case("off") {
        return None;
    }
    match raw.parse::<f64>() {
        Ok(r) if r.is_finite() && r > 0.0 && r <= 1.0 => {
            Some(((r * 10_000.0).round() as u32).max(1))
        }
        _ => {
            eprintln!(
                "WARN: KOSHA_COMPACT_TOMBSTONE_RATIO={raw} is not a fraction in (0, 1] — \
                 tombstone-ratio compaction stays off"
            );
            None
        }
    }
}

fn deleted_docs(manifest: &Manifest, entry: &ManifestEntry) -> u64 {
    manifest
        .tombstones
        .get(&entry.segment_id)
        .map_or(0, |t| t.len() as u64)
}

/// Tombstoned share of `entry`'s documents, from the manifest's tombstones.
pub fn tombstone_ratio(manifest: &Manifest, entry: &ManifestEntry) -> f64 {
    if entry.doc_count == 0 {
        return 0.0;
    }
    deleted_docs(manifest, entry) as f64 / entry.doc_count as f64
}

/// Whether `entry` is at or above the policy's tombstone ratio (#464).
fn over_tombstone_ratio(
    manifest: &Manifest,
    policy: &CompactionPolicy,
    entry: &ManifestEntry,
) -> bool {
    let Some(bp) = policy.tombstone_ratio_bp else {
        return false;
    };
    let deleted = deleted_docs(manifest, entry);
    deleted > 0 && deleted * 10_000 >= bp as u64 * entry.doc_count as u64
}

/// Segments at or above the policy's tombstone ratio (#464); empty when
/// the trigger is off.
pub fn tombstone_heavy_segments<'a>(
    manifest: &'a Manifest,
    policy: &CompactionPolicy,
) -> Vec<&'a ManifestEntry> {
    manifest
        .segments
        .iter()
        .filter(|e| over_tombstone_ratio(manifest, policy, e))
        .collect()
}

/// Whether `inputs` is a single segment over the tombstone ratio: a
/// rewrite that expunges its deletes is worth running on its own (#464).
pub fn is_tombstone_rewrite(
    manifest: &Manifest,
    policy: &CompactionPolicy,
    inputs: &[ManifestEntry],
) -> bool {
    inputs.len() == 1 && over_tombstone_ratio(manifest, policy, &inputs[0])
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
    /// How the merged segment's vector indexes are built (#253); `None`
    /// reads `KOSHA_VECTOR_COMPACTION` (see
    /// [`kosha_segment::VectorCompactionMode`]).
    pub vector_index: Option<kosha_segment::VectorCompactionMode>,
}

impl CompactOptions {
    pub fn tiered(policy: CompactionPolicy) -> Self {
        Self {
            mode: CompactMode::Tiered,
            policy,
            vector_index: None,
        }
    }

    /// `self`, building merged vector indexes with `mode` regardless of
    /// `KOSHA_VECTOR_COMPACTION`.
    pub fn with_vector_index(mut self, mode: kosha_segment::VectorCompactionMode) -> Self {
        self.vector_index = Some(mode);
        self
    }

    pub fn full() -> Self {
        Self {
            mode: CompactMode::Full,
            policy: CompactionPolicy::default(),
            vector_index: None,
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
    /// The segments this merge consumed (empty when nothing merged). They
    /// are gone from the manifest once the merge publishes, so the caller
    /// may evict their local copies (#242).
    pub merged_inputs: Vec<SegmentId>,
    /// The segment the merge produced, if it merged.
    pub output: Option<SegmentId>,
}

impl CompactResult {
    /// A pass that merged nothing.
    pub fn unchanged(segments: usize) -> Self {
        Self {
            merged: false,
            segments_before: segments,
            segments_after: segments,
            segments_merged: 0,
            merged_inputs: Vec::new(),
            output: None,
        }
    }
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
        || !tombstone_heavy_segments(manifest, policy).is_empty()
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

    // A lone segment is a plan only as a tiered tombstone rewrite (#464).
    if candidates.len() < 2
        && !(opts.mode == CompactMode::Tiered
            && is_tombstone_rewrite(manifest, &opts.policy, &candidates))
    {
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
            let policy = &opts.policy;
            // #464: tombstone-heavy segments join whatever their size,
            // most-deleted first. With the trigger off this is empty and
            // what follows is the plain doc-count tiering.
            let mut heavy: Vec<ManifestEntry> = candidates
                .iter()
                .filter(|e| over_tombstone_ratio(manifest, policy, e))
                .cloned()
                .collect();
            heavy.sort_by(|a, b| {
                tombstone_ratio(manifest, b)
                    .total_cmp(&tombstone_ratio(manifest, a))
                    .then_with(|| a.segment_id.0.cmp(&b.segment_id.0))
            });
            candidates.retain(|e| {
                e.doc_count < policy.max_mergeable_docs
                    && !over_tombstone_ratio(manifest, policy, e)
            });
            if heavy.is_empty() && candidates.len() < policy.min_mergeable_segments {
                return None;
            }
            candidates.sort_by_key(|e| e.doc_count);
            heavy.extend(candidates);
            let mut candidates = heavy;
            candidates.truncate(policy.max_segments_per_merge);
            if cap > 0 {
                // Same greedy prefix, in the order above. A first input
                // over the cap on its own is kept only when it is
                // tombstone-heavy: rewriting it alone only shrinks it.
                let mut total = 0u64;
                let mut kept = Vec::new();
                for entry in candidates {
                    let bytes = segment_bytes(&entry.segment_id).unwrap_or(u64::MAX);
                    if total.saturating_add(bytes) > cap {
                        if kept.is_empty()
                            && bytes != u64::MAX
                            && over_tombstone_ratio(manifest, policy, &entry)
                        {
                            kept.push(entry);
                        }
                        break;
                    }
                    total += bytes;
                    kept.push(entry);
                }
                candidates = kept;
            }
            if candidates.len() < policy.min_mergeable_segments
                && !is_tombstone_rewrite(manifest, policy, &candidates)
            {
                return None;
            }
            Some(MergePlan { inputs: candidates })
        }
    }
}

/// Plan one merge group *before* anything is hydrated (#242).
///
/// Same selection as [`select_merge_inputs`], but every manifest segment is
/// a candidate whether or not it is on local disk: `segment_bytes` reports
/// its size from object storage (S3 object sizes), so a `full` pass no
/// longer has to download the whole namespace just to learn which segments
/// fit under `max_merged_segment_bytes`. The caller hydrates only the
/// returned group, merges it with [`crate::Indexer::compact_planned`], and
/// evicts the inputs once the merge is published. Peak local disk for one
/// group is then its inputs plus its output, about `2 x cap`.
///
/// An unknown size (`None`) never joins a capped group, exactly as in
/// `select_merge_inputs`.
pub fn plan_merge_group<G>(
    manifest: &Manifest,
    opts: &CompactOptions,
    segment_bytes: G,
) -> Option<MergePlan>
where
    G: FnMut(&SegmentId) -> Option<u64>,
{
    select_merge_inputs(manifest, opts, |_| true, segment_bytes)
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
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            branch: None,
            schema: None,
            retired: Default::default(),
            shards: None,
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
            vector_index: None,
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

    /// #242: planning from remote sizes. Nothing is local; the plan must
    /// still pick the smallest-first group under the cap, never let a
    /// segment of unknown size join it, and terminate once nothing fits.
    #[test]
    fn plan_merge_group_uses_remote_sizes_and_skips_unknown() {
        let m = manifest(vec![
            entry("s1", 1),
            entry("s2", 1),
            entry("unknown", 1),
            entry("s3", 1),
            entry("s4", 1),
            entry("huge", 1),
        ]);
        let remote = |id: &SegmentId| -> Option<u64> {
            match id.0.as_str() {
                "s1" => Some(30),
                "s2" => Some(10),
                "s3" => Some(40),
                "s4" => Some(25),
                "huge" => Some(1_000),
                _ => None, // S3 listed nothing for it
            }
        };
        let mut opts = CompactOptions::full();
        opts.policy.max_merged_segment_bytes = 70;
        let plan = plan_merge_group(&m, &opts, remote).expect("plan");
        let ids: Vec<&str> = plan
            .inputs
            .iter()
            .map(|e| e.segment_id.0.as_str())
            .collect();
        // 10 + 25 + 30 = 65 fits; adding s3 (40) would exceed 70.
        assert_eq!(ids, vec!["s2", "s4", "s1"]);
        assert!(!ids.contains(&"unknown"));

        // Even with an uncapped-looking budget, an unknown size stays out.
        opts.policy.max_merged_segment_bytes = u64::MAX - 1;
        let plan = plan_merge_group(&m, &opts, remote).expect("plan");
        assert!(plan.inputs.iter().all(|e| e.segment_id.0 != "unknown"));

        // Only unknown sizes left: no group, so a looping pass stops.
        let m2 = manifest(vec![entry("u1", 1), entry("u2", 1)]);
        assert!(plan_merge_group(&m2, &opts, |_| None).is_none());

        // Tiered planning works from remote sizes too.
        let tiered = CompactOptions::tiered(CompactionPolicy {
            max_merged_segment_bytes: 70,
            ..CompactionPolicy::default()
        });
        let plan = plan_merge_group(&m, &tiered, remote).expect("plan");
        assert!(plan.inputs.len() >= 2);
        assert!(plan.inputs.iter().all(|e| e.segment_id.0 != "unknown"));
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

    // ── #464: tombstone-ratio trigger ───────────────────────────────────

    /// `m` with `deleted` doc_seqs tombstoned in segment `id`.
    fn delete(mut m: Manifest, id: &str, deleted: u32) -> Manifest {
        m.tombstones
            .insert(SegmentId(id.into()), (0..deleted).collect());
        m
    }

    fn policy(trigger_bp: Option<u32>) -> CompactionPolicy {
        CompactionPolicy {
            max_mergeable_docs: 50_000,
            max_segments_per_merge: 32,
            min_mergeable_segments: 2,
            trigger_mergeable_segments: 8,
            max_merged_segment_bytes: 0,
            tombstone_ratio_bp: trigger_bp,
        }
    }

    fn ids(plan: &MergePlan) -> Vec<&str> {
        plan.inputs
            .iter()
            .map(|e| e.segment_id.0.as_str())
            .collect()
    }

    #[test]
    fn tombstone_ratio_parses_fractions_and_rejects_the_rest() {
        assert_eq!(parse_tombstone_ratio(None), None);
        assert_eq!(parse_tombstone_ratio(Some("")), None);
        assert_eq!(parse_tombstone_ratio(Some("off")), None);
        assert_eq!(parse_tombstone_ratio(Some("0.2")), Some(2_000));
        assert_eq!(parse_tombstone_ratio(Some(" 1 ")), Some(10_000));
        assert_eq!(parse_tombstone_ratio(Some("0.00001")), Some(1));
        for bad in ["0", "-0.1", "1.5", "nan", "inf", "x"] {
            assert_eq!(parse_tombstone_ratio(Some(bad)), None, "{bad}");
        }
    }

    #[test]
    fn trigger_off_leaves_tiered_planning_unchanged() {
        let segments = vec![
            entry("small-a", 10),
            entry("small-b", 20),
            entry("big", 80_000),
        ];
        let clean = manifest(segments.clone());
        let deleted = delete(manifest(segments), "big", 60_000);
        let opts = CompactOptions::tiered(policy(None));
        let a = select_merge_inputs(&clean, &opts, |_| true, |_| Some(1)).unwrap();
        let b = select_merge_inputs(&deleted, &opts, |_| true, |_| Some(1)).unwrap();
        assert_eq!(ids(&a), ids(&b));
        assert_eq!(ids(&b), vec!["small-a", "small-b"]);
        assert!(!needs_compaction(&deleted, &policy(None)));
        assert!(tombstone_heavy_segments(&deleted, &policy(None)).is_empty());
    }

    #[test]
    fn heavy_segment_joins_first_whatever_its_size() {
        let m = delete(
            manifest(vec![
                entry("small-a", 10),
                entry("small-b", 20),
                entry("big", 80_000),
            ]),
            "big",
            40_000,
        );
        let p = policy(Some(2_000));
        assert!(needs_compaction(&m, &p), "a heavy segment alone triggers");
        let plan = select_merge_inputs(&m, &CompactOptions::tiered(p), |_| true, |_| Some(1))
            .expect("plan");
        assert_eq!(ids(&plan), vec!["big", "small-a", "small-b"]);
    }

    #[test]
    fn under_the_ratio_is_ignored() {
        // 10% deleted against a 20% trigger.
        let m = delete(
            manifest(vec![entry("a", 10), entry("big", 80_000)]),
            "big",
            8_000,
        );
        let p = policy(Some(2_000));
        assert!(!needs_compaction(&m, &p));
        assert!(
            select_merge_inputs(&m, &CompactOptions::tiered(p), |_| true, |_| Some(1)).is_none()
        );
    }

    #[test]
    fn a_lone_heavy_segment_is_rewritten_on_its_own() {
        let m = delete(
            manifest(vec![entry("big", 80_000), entry("other-big", 90_000)]),
            "big",
            80_000,
        );
        let p = policy(Some(5_000));
        let plan = select_merge_inputs(
            &m,
            &CompactOptions::tiered(p.clone()),
            |_| true,
            |_| Some(1),
        )
        .expect("singleton rewrite");
        assert_eq!(ids(&plan), vec!["big"]);
        assert!(is_tombstone_rewrite(&m, &p, &plan.inputs));
        // A lone segment that is not heavy is never a plan.
        let clean = manifest(vec![entry("big", 80_000), entry("other-big", 90_000)]);
        assert!(
            select_merge_inputs(&clean, &CompactOptions::tiered(p), |_| true, |_| Some(1))
                .is_none()
        );
    }

    #[test]
    fn heavy_segment_over_the_byte_cap_is_rewritten_alone() {
        let m = delete(
            manifest(vec![entry("small", 10), entry("big", 80_000)]),
            "big",
            50_000,
        );
        let mut p = policy(Some(5_000));
        p.max_merged_segment_bytes = 100;
        let sizes = |id: &SegmentId| Some(if id.0 == "big" { 1_000 } else { 10 });
        let plan = select_merge_inputs(&m, &CompactOptions::tiered(p.clone()), |_| true, sizes)
            .expect("plan");
        assert_eq!(
            ids(&plan),
            vec!["big"],
            "shrinks it; joins nothing over the cap"
        );
        // Unknown size: never planned, heavy or not.
        let plan = select_merge_inputs(
            &m,
            &CompactOptions::tiered(p),
            |_| true,
            |id| (id.0 != "big").then_some(10),
        );
        assert!(plan.is_none());
    }

    #[test]
    fn most_deleted_heavy_segment_goes_first() {
        let m = delete(
            delete(
                manifest(vec![entry("h30", 100), entry("h60", 100)]),
                "h30",
                30,
            ),
            "h60",
            60,
        );
        let plan = select_merge_inputs(
            &m,
            &CompactOptions::tiered(policy(Some(2_500))),
            |_| true,
            |_| Some(1),
        )
        .expect("plan");
        assert_eq!(ids(&plan), vec!["h60", "h30"]);
    }
}
