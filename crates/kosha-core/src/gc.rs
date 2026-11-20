//! Segment and deletion-generation garbage collection (issue #239).
//!
//! Two halves:
//!
//! - **Retirement records.** Every manifest publish that stops referencing a
//!   segment, or replaces a segment's deletion generation, lists what it
//!   dropped in [`Manifest::retired`] with a wall-clock `retired_at_ms` and
//!   the manifest version that dropped it ([`record_retirements`]). The
//!   record rides in the same compare-and-swap as the change itself, so a
//!   crash can never commit one without the other.
//! - **Object-store sweep.** [`collect_namespace`] lists a namespace's
//!   prefix through [`StorageBackend::list_objects`] and deletes what the
//!   current manifest does not reference once it has been retired for
//!   longer than the grace window (or, for objects no manifest ever
//!   recorded, once the newest object under it is older than the orphan
//!   grace). It re-reads the manifest version right before deleting and
//!   gives up on the namespace if it moved.
//!
//! Nothing here knows about S3: the server passes its `S3Storage`, tests pass
//! [`crate::LocalStorage`] or an in-memory backend.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::deletes::DeletionVector;
use crate::{KoshaError, Manifest, NamespaceId, ObjectMeta, SegmentId, StorageBackend};

/// The commit marker a segment upload writes last (#231). GC deletes it
/// first, so a half-deleted segment never looks durable.
pub const COMMIT_FILE: &str = "_COMMIT";

/// One object a manifest stopped referencing: a whole segment prefix
/// (`deletion_generation == None`) or one superseded deletion generation of
/// a segment the manifest still lists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetiredObject {
    pub segment_id: SegmentId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletion_generation: Option<String>,
    /// Unix epoch milliseconds when the publish that dropped it was built.
    pub retired_at_ms: u64,
    /// The manifest version that stopped referencing it.
    pub retired_in_version: u64,
}

impl RetiredObject {
    fn key(&self) -> (SegmentId, Option<String>) {
        (self.segment_id.clone(), self.deletion_generation.clone())
    }
}

/// `deletes-{64 hex}.roaring` → the generation.
pub fn deletion_generation_of(file_name: &str) -> Option<&str> {
    let generation = file_name
        .strip_prefix("deletes-")?
        .strip_suffix(".roaring")?;
    (generation.len() == 64 && generation.bytes().all(|b| b.is_ascii_hexdigit()))
        .then_some(generation)
}

/// What a previously committed manifest referenced, for
/// [`record_retirements`].
#[derive(Debug, Clone, Default)]
pub struct CommittedRefs {
    pub segments: HashSet<SegmentId>,
    pub deletion_vectors: HashMap<SegmentId, DeletionVector>,
    pub retired: Vec<RetiredObject>,
}

impl CommittedRefs {
    pub fn of(manifest: &Manifest) -> Self {
        Self {
            segments: manifest
                .segments
                .iter()
                .map(|e| e.segment_id.clone())
                .collect(),
            deletion_vectors: manifest.deletion_vectors.clone(),
            retired: manifest.retired.clone(),
        }
    }
}

/// How long a retirement record is kept in [`Manifest::retired`] after the
/// newest one (7 days). Only an object-store GC run removes records whose
/// objects it deleted, and GC may never run (the shipped CronJob is
/// suspended), so without a bound every compaction input and superseded
/// deletion generation would stay in every manifest forever. A dropped
/// record's object is then "untracked" to GC, which deletes it once its
/// newest file is older than `KOSHA_GC_ORPHAN_GRACE`; that is already true
/// when the record is dropped, since the object was written before it was
/// retired. Keep `KOSHA_GC_GRACE` below this.
pub const RETIRED_RECORD_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Set `next.retired` to `previous.retired` plus everything `previous`
/// referenced that `next` does not, minus anything `next` references again,
/// minus records more than [`RETIRED_RECORD_TTL_MS`] older than the newest.
///
/// `retired_at_ms(segment, generation)` stamps a newly retired object. A
/// writer that may rebuild the same candidate twice (a retried publish)
/// passes a memo so both builds carry the same timestamp and an identical
/// retry stays byte-identical for the compare-and-swap.
pub fn record_retirements(
    previous: &CommittedRefs,
    next: &mut Manifest,
    mut retired_at_ms: impl FnMut(&SegmentId, Option<&str>) -> u64,
) {
    let listed: HashSet<&SegmentId> = next.segments.iter().map(|e| &e.segment_id).collect();
    let current_generation = |seg: &SegmentId| {
        next.deletion_vectors
            .get(seg)
            .map(|v| v.generation.as_str())
    };
    let referenced = |object: &RetiredObject| match &object.deletion_generation {
        None => listed.contains(&object.segment_id),
        Some(generation) => {
            listed.contains(&object.segment_id)
                && current_generation(&object.segment_id) == Some(generation.as_str())
        }
    };

    let mut out: Vec<RetiredObject> = Vec::new();
    let mut seen: HashSet<(SegmentId, Option<String>)> = HashSet::new();
    for object in &previous.retired {
        if !referenced(object) && seen.insert(object.key()) {
            out.push(object.clone());
        }
    }
    let mut dropped: Vec<&SegmentId> = previous
        .segments
        .iter()
        .filter(|id| !listed.contains(id))
        .collect();
    dropped.sort_by(|a, b| a.0.cmp(&b.0));
    for id in dropped {
        if seen.insert((id.clone(), None)) {
            out.push(RetiredObject {
                segment_id: id.clone(),
                deletion_generation: None,
                retired_at_ms: retired_at_ms(id, None),
                retired_in_version: next.version,
            });
        }
    }
    let mut superseded: Vec<(&SegmentId, &DeletionVector)> = previous
        .deletion_vectors
        .iter()
        .filter(|(seg, vector)| {
            listed.contains(seg) && current_generation(seg) != Some(vector.generation.as_str())
        })
        .collect();
    superseded.sort_by(|a, b| a.0 .0.cmp(&b.0 .0));
    for (seg, vector) in superseded {
        let key = (seg.clone(), Some(vector.generation.clone()));
        if seen.insert(key) {
            out.push(RetiredObject {
                segment_id: seg.clone(),
                deletion_generation: Some(vector.generation.clone()),
                retired_at_ms: retired_at_ms(seg, Some(&vector.generation)),
                retired_in_version: next.version,
            });
        }
    }
    // A whole-segment record covers that segment's deletion generations.
    let whole: HashSet<SegmentId> = out
        .iter()
        .filter(|o| o.deletion_generation.is_none())
        .map(|o| o.segment_id.clone())
        .collect();
    out.retain(|o| o.deletion_generation.is_none() || !whole.contains(&o.segment_id));
    // Relative to the newest stamp, not the clock, so a retried build of the
    // same candidate stays byte-identical.
    if let Some(newest) = out.iter().map(|o| o.retired_at_ms).max() {
        let cutoff = newest.saturating_sub(RETIRED_RECORD_TTL_MS);
        out.retain(|o| o.retired_at_ms >= cutoff);
    }
    next.retired = out;
}

/// Knobs for one GC run.
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Report only; delete nothing and commit nothing.
    pub dry_run: bool,
    /// How long a *retired* object is kept after the publish that dropped
    /// it: must exceed the longest a query replica can keep serving an
    /// older manifest (`KOSHA_GC_GRACE`).
    pub grace_ms: u64,
    /// How old the newest object under an *untracked* prefix (no retirement
    /// record: a crashed flush, a losing CAS writer, a pre-#239 compaction)
    /// must be before it is deleted (`KOSHA_GC_ORPHAN_GRACE`). Longer than
    /// `grace_ms` by default, because an upload that has not been published
    /// yet looks exactly like an orphan.
    pub orphan_grace_ms: u64,
    /// "Now", in Unix epoch milliseconds.
    pub now_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateKind {
    Segment,
    DeletionVector,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateReason {
    /// A manifest recorded dropping it (`Manifest::retired`).
    Retired,
    /// No manifest references it or recorded dropping it.
    Untracked,
}

/// One unit GC would delete: a segment prefix (all of its objects) or one
/// deletion-generation object.
#[derive(Debug, Clone, Serialize)]
pub struct GcCandidate {
    pub kind: CandidateKind,
    pub reason: CandidateReason,
    pub segment_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_generation: Option<String>,
    /// Storage paths, in the order a real run deletes them (`_COMMIT`
    /// first for a segment).
    pub objects: Vec<String>,
    pub bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retired_at_ms: Option<u64>,
    /// Newest object's modification time.
    pub last_modified_ms: u64,
}

/// Something GC saw but kept.
#[derive(Debug, Clone, Serialize)]
pub struct GcSkipped {
    pub path: String,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct NamespaceGcReport {
    pub namespace: String,
    pub dry_run: bool,
    pub manifest_version: u64,
    pub candidates: Vec<GcCandidate>,
    pub reclaimable_bytes: u64,
    pub skipped: Vec<GcSkipped>,
    /// Real run only.
    pub objects_deleted: usize,
    pub segments_deleted: usize,
    pub deletion_vectors_deleted: usize,
    pub bytes_reclaimed: u64,
    /// Retirement records dropped from the manifest (their objects are
    /// gone), and whether that manifest update committed.
    pub records_pruned: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_commit_error: Option<String>,
    /// Set when the namespace was left untouched part-way (manifest moved,
    /// a delete failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aborted: Option<String>,
}

/// Decide what in `objects` (a listing of `{namespace}/`) is garbage under
/// `manifest`. Pure: no I/O.
///
/// `other_namespaces` guards nested namespace names: `org/a` lives under
/// `org/`, so when collecting namespace `org` the child prefix `a` belongs
/// to another namespace and is never a candidate.
pub fn plan_namespace(
    namespace: &NamespaceId,
    manifest: &Manifest,
    objects: &[ObjectMeta],
    other_namespaces: &[NamespaceId],
    config: &GcConfig,
) -> (Vec<GcCandidate>, Vec<GcSkipped>, Vec<RetiredObject>) {
    let root = format!("{}/", namespace.0);
    let listed: HashSet<&SegmentId> = manifest.segments.iter().map(|e| &e.segment_id).collect();
    let retired: HashMap<(SegmentId, Option<String>), &RetiredObject> =
        manifest.retired.iter().map(|o| (o.key(), o)).collect();
    let segment_prefix = format!("{}-", namespace.0.replace('/', "_"));

    // child → (file-or-nested-path, meta)
    let mut by_child: BTreeMap<&str, Vec<(&str, &ObjectMeta)>> = BTreeMap::new();
    for object in objects {
        let Some(rest) = object.path.strip_prefix(&root) else {
            continue;
        };
        let Some((child, inner)) = rest.split_once('/') else {
            continue; // a file directly under the namespace: not ours
        };
        if child.is_empty() || inner.is_empty() {
            continue;
        }
        by_child.entry(child).or_default().push((inner, object));
    }

    let mut candidates = Vec::new();
    let mut skipped = Vec::new();
    let mut present: HashSet<(SegmentId, Option<String>)> = HashSet::new();
    let age = |modified: u64| config.now_ms.saturating_sub(modified);

    for (child, files) in by_child {
        let child_path = format!("{root}{child}");
        if other_namespaces
            .iter()
            .any(|other| other.0 == child_path || other.0.starts_with(&format!("{child_path}/")))
        {
            continue;
        }
        let segment = SegmentId(child.to_string());
        present.insert((segment.clone(), None));
        for (inner, _) in &files {
            if let Some(generation) = deletion_generation_of(inner) {
                present.insert((segment.clone(), Some(generation.to_string())));
            }
        }

        if listed.contains(&segment) {
            // A referenced segment: only superseded deletion generations
            // directly inside it may go.
            let current = manifest
                .deletion_vectors
                .get(&segment)
                .map(|v| v.generation.as_str());
            for (inner, meta) in &files {
                let Some(generation) = deletion_generation_of(inner) else {
                    continue;
                };
                if Some(generation) == current {
                    continue;
                }
                let record = retired.get(&(segment.clone(), Some(generation.to_string())));
                let eligible = match record {
                    Some(r) => age(r.retired_at_ms) >= config.grace_ms,
                    None => age(meta.last_modified_ms) >= config.orphan_grace_ms,
                };
                if !eligible {
                    skipped.push(GcSkipped {
                        path: meta.path.clone(),
                        reason: "superseded deletion generation still inside grace".into(),
                    });
                    continue;
                }
                candidates.push(GcCandidate {
                    kind: CandidateKind::DeletionVector,
                    reason: if record.is_some() {
                        CandidateReason::Retired
                    } else {
                        CandidateReason::Untracked
                    },
                    segment_id: segment.0.clone(),
                    deletion_generation: Some(generation.to_string()),
                    objects: vec![meta.path.clone()],
                    bytes: meta.size,
                    retired_at_ms: record.map(|r| r.retired_at_ms),
                    last_modified_ms: meta.last_modified_ms,
                });
            }
            continue;
        }

        let newest = files
            .iter()
            .map(|(_, m)| m.last_modified_ms)
            .max()
            .unwrap_or(0);
        let has_commit = files.iter().any(|(inner, _)| *inner == COMMIT_FILE);
        let record = retired.get(&(segment.clone(), None));
        let eligible = match record {
            Some(r) => age(r.retired_at_ms) >= config.grace_ms,
            None => {
                if !child.starts_with(&segment_prefix) {
                    skipped.push(GcSkipped {
                        path: child_path.clone(),
                        reason: "unreferenced prefix does not look like a segment of this \
                                 namespace and has no retirement record"
                            .into(),
                    });
                    continue;
                }
                age(newest) >= config.orphan_grace_ms
            }
        };
        if !eligible {
            skipped.push(GcSkipped {
                path: child_path.clone(),
                reason: if record.is_some() {
                    "retired segment still inside grace".into()
                } else if has_commit {
                    "unreferenced segment younger than the orphan grace".into()
                } else {
                    "in-progress upload (no _COMMIT) younger than the orphan grace".into()
                },
            });
            continue;
        }
        let mut paths: Vec<(&str, &ObjectMeta)> = files.clone();
        // `_COMMIT` first, then the rest in a stable order.
        paths.sort_by(|a, b| (a.0 != COMMIT_FILE, a.0).cmp(&(b.0 != COMMIT_FILE, b.0)));
        candidates.push(GcCandidate {
            kind: CandidateKind::Segment,
            reason: if record.is_some() {
                CandidateReason::Retired
            } else {
                CandidateReason::Untracked
            },
            segment_id: segment.0.clone(),
            deletion_generation: None,
            bytes: paths.iter().map(|(_, m)| m.size).sum(),
            objects: paths.iter().map(|(_, m)| m.path.clone()).collect(),
            retired_at_ms: record.map(|r| r.retired_at_ms),
            last_modified_ms: newest,
        });
    }

    // Records whose objects are already gone (deleted by an earlier run that
    // could not commit the pruned manifest, or never uploaded at all).
    let stale = manifest
        .retired
        .iter()
        .filter(|o| !present.contains(&o.key()))
        .cloned()
        .collect();
    (candidates, skipped, stale)
}

/// Run GC over one namespace.
///
/// `load_manifest` reads the namespace's *current* committed manifest; it is
/// called once to plan and again right before deleting. If the version
/// moved, nothing is deleted. `commit` compare-and-swaps the manifest with
/// the deleted objects' retirement records dropped; a failure there is
/// reported, not fatal: the records go stale and the next run prunes them.
pub fn collect_namespace(
    storage: &dyn StorageBackend,
    namespace: &NamespaceId,
    other_namespaces: &[NamespaceId],
    load_manifest: &mut dyn FnMut() -> Result<Option<Manifest>, KoshaError>,
    commit: &mut dyn FnMut(u64, Manifest) -> Result<(), KoshaError>,
    config: &GcConfig,
) -> Result<NamespaceGcReport, KoshaError> {
    let mut report = NamespaceGcReport {
        namespace: namespace.0.clone(),
        dry_run: config.dry_run,
        ..Default::default()
    };
    let Some(manifest) = load_manifest()? else {
        report.aborted = Some("namespace has no committed manifest".into());
        return Ok(report);
    };
    report.manifest_version = manifest.version;
    let objects = storage.list_objects(&format!("{}/", namespace.0))?;
    let (candidates, skipped, stale) =
        plan_namespace(namespace, &manifest, &objects, other_namespaces, config);
    report.reclaimable_bytes = candidates.iter().map(|c| c.bytes).sum();
    report.candidates = candidates;
    report.skipped = skipped;
    if config.dry_run {
        report.records_pruned = 0;
        return Ok(report);
    }

    let now = load_manifest()?.map(|m| m.version);
    if now != Some(manifest.version) {
        report.aborted = Some(format!(
            "manifest version changed from {} to {now:?} during the run; nothing deleted",
            manifest.version
        ));
        return Ok(report);
    }

    let mut gone: HashSet<(SegmentId, Option<String>)> = stale.iter().map(|o| o.key()).collect();
    for candidate in &report.candidates {
        let mut deleted_all = true;
        for path in &candidate.objects {
            match storage.delete(path) {
                Ok(()) => report.objects_deleted += 1,
                Err(e) => {
                    deleted_all = false;
                    report.aborted = Some(format!("delete {path} failed: {e}"));
                    break;
                }
            }
        }
        if !deleted_all {
            break;
        }
        report.bytes_reclaimed += candidate.bytes;
        let segment = SegmentId(candidate.segment_id.clone());
        match candidate.kind {
            CandidateKind::Segment => {
                report.segments_deleted += 1;
                gone.insert((segment, None));
            }
            CandidateKind::DeletionVector => {
                report.deletion_vectors_deleted += 1;
                gone.insert((segment, candidate.deletion_generation.clone()));
            }
        }
    }

    let before = manifest.retired.len();
    let mut pruned = manifest.clone();
    pruned.retired.retain(|o| !gone.contains(&o.key()));
    let removed = before - pruned.retired.len();
    if removed > 0 {
        pruned.version = manifest.version + 1;
        match commit(manifest.version, pruned) {
            Ok(()) => report.records_pruned = removed,
            Err(e) => report.records_commit_error = Some(e.to_string()),
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ManifestEntry;

    fn manifest(version: u64, segments: &[&str]) -> Manifest {
        Manifest {
            version,
            segments: segments
                .iter()
                .map(|s| ManifestEntry {
                    segment_id: SegmentId(s.to_string()),
                    doc_count: 10,
                })
                .collect(),
            segment_footers: Default::default(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            retired: Default::default(),
            schema: Default::default(),
        }
    }

    fn dv(generation_byte: char) -> DeletionVector {
        DeletionVector {
            generation: generation_byte.to_string().repeat(64),
            count: 1,
        }
    }

    #[test]
    fn retirement_records_older_than_the_ttl_are_dropped() {
        // Many compactions with no GC run: the list stays bounded by age.
        let day = 24 * 60 * 60 * 1000;
        let mut current = manifest(1, &["s0"]);
        for n in 1..=30u64 {
            let refs = CommittedRefs::of(&current);
            let mut next = manifest(n + 1, &[&format!("s{n}")]);
            record_retirements(&refs, &mut next, |_, _| n * day);
            current = next;
        }
        let stamps: Vec<u64> = current
            .retired
            .iter()
            .map(|o| o.retired_at_ms / day)
            .collect();
        assert_eq!(
            stamps,
            (23..=30).collect::<Vec<_>>(),
            "seven days back, inclusive"
        );
        // Deterministic for a retried build of the same candidate.
        let refs = CommittedRefs::of(&current);
        let mut a = manifest(40, &["x"]);
        let mut b = manifest(40, &["x"]);
        record_retirements(&refs, &mut a, |_, _| 31 * day);
        record_retirements(&refs, &mut b, |_, _| 31 * day);
        assert_eq!(a.retired, b.retired);
        // Nothing newly retired: nothing is aged out either.
        let refs = CommittedRefs::of(&a);
        let mut same = manifest(41, &["x"]);
        record_retirements(&refs, &mut same, |_, _| 99 * day);
        assert_eq!(same.retired, a.retired);
    }

    #[test]
    fn retirements_record_dropped_segments_and_superseded_generations() {
        let mut previous = manifest(3, &["a", "b", "c"]);
        previous
            .deletion_vectors
            .insert(SegmentId("a".into()), dv('1'));
        previous
            .deletion_vectors
            .insert(SegmentId("b".into()), dv('2'));
        let refs = CommittedRefs::of(&previous);
        let mut next = manifest(4, &["a", "c", "m"]);
        next.deletion_vectors.insert(SegmentId("a".into()), dv('3'));
        record_retirements(&refs, &mut next, |_, _| 77);
        assert_eq!(
            next.retired,
            vec![
                RetiredObject {
                    segment_id: SegmentId("b".into()),
                    deletion_generation: None,
                    retired_at_ms: 77,
                    retired_in_version: 4,
                },
                RetiredObject {
                    segment_id: SegmentId("a".into()),
                    deletion_generation: Some(dv('1').generation),
                    retired_at_ms: 77,
                    retired_in_version: 4,
                },
            ]
        );

        // Carried forward with the original stamp; nothing re-added.
        let refs = CommittedRefs::of(&next);
        let mut later = manifest(5, &["a", "c", "m"]);
        later
            .deletion_vectors
            .insert(SegmentId("a".into()), dv('3'));
        record_retirements(&refs, &mut later, |_, _| 99);
        assert_eq!(later.retired, next.retired);

        // Dropping "a" subsumes its generation record.
        let refs = CommittedRefs::of(&later);
        let mut dropped = manifest(6, &["c", "m"]);
        record_retirements(&refs, &mut dropped, |_, _| 100);
        assert!(dropped
            .retired
            .iter()
            .all(|o| o.deletion_generation.is_none()));
        assert_eq!(dropped.retired.len(), 2);
    }

    #[test]
    fn legacy_manifest_without_retired_field_deserializes_and_omits_empty() {
        let json = r#"{"version":2,"segments":[{"segment_id":"s","doc_count":1}]}"#;
        let m: Manifest = serde_json::from_str(json).unwrap();
        assert!(m.retired.is_empty());
        let out = serde_json::to_string(&m).unwrap();
        assert!(!out.contains("retired"));
    }

    #[test]
    fn generation_file_names_are_strict() {
        let g = "a".repeat(64);
        assert_eq!(
            deletion_generation_of(&format!("deletes-{g}.roaring")),
            Some(g.as_str())
        );
        assert_eq!(deletion_generation_of("deletes-abc.roaring"), None);
        assert_eq!(deletion_generation_of("doc_store.bin"), None);
    }
}
