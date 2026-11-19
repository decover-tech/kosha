//! Namespace branching (#208): copy-on-write manifests.
//!
//! Segments are immutable and S3 is the source of truth, so a branch is a
//! new manifest that lists its parent's segments where they already are:
//! no segment or deletion bitmap is copied. [`BranchInfo::segment_homes`]
//! records, for each segment the branch lists but did not write, the
//! namespace whose prefix holds it, and an inherited deletion bitmap
//! carries its home in [`crate::deletes::DeletionVector::namespace`].
//! Everything the branch writes afterwards (flushes, compaction outputs,
//! new deletion bitmaps) goes to the branch's own prefix, so the parent's
//! prefix is never written to by the branch.
//!
//! The lifecycle rule that keeps a branch from corrupting its parent (or a
//! parent from corrupting a branch): an object may only be deleted when no
//! remaining manifest references it. [`References`] is that set, built from
//! every manifest in the control store; branch deletion reclaims only the
//! deleted namespace's own prefix, and only what [`References`] does not
//! hold. Anything else that deletes segment objects (segment GC,
//! namespace deletion) must consult it the same way.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::{KoshaError, Manifest, NamespaceId, SegmentId};

/// Branch lineage stored in a branch's manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchInfo {
    /// The namespace this one was branched from.
    pub parent: NamespaceId,
    /// The parent's manifest version the branch started from.
    pub parent_version: u64,
    /// For each listed segment that lives in another namespace's prefix:
    /// that namespace. Always the namespace that *wrote* the segment, so a
    /// branch of a branch points at the original owner, and deleting an
    /// intermediate branch never strands its descendants. Pruned to the
    /// listed segments on every publish.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub segment_homes: HashMap<SegmentId, NamespaceId>,
}

impl Manifest {
    /// The manifest of a new branch of this manifest (which belongs to
    /// `parent`), at `version`.
    ///
    /// O(segments + deletion bitmaps) metadata only, independent of the
    /// number of documents: segments and bitmaps are referenced where they
    /// are. The WAL checkpoint is reset because the branch's WAL starts
    /// empty; the schema, deletes and footer checksums carry over.
    pub fn branch_from(&self, parent: &NamespaceId, version: u64) -> Manifest {
        let mut child = self.clone();
        child.version = version;
        child.wal_checkpoint = 0;
        child.wal_origin = None;
        let segment_homes = self
            .segments
            .iter()
            .map(|entry| {
                (
                    entry.segment_id.clone(),
                    self.segment_home(parent, &entry.segment_id).clone(),
                )
            })
            .collect();
        for vector in child.deletion_vectors.values_mut() {
            if vector.namespace.is_none() {
                vector.namespace = Some(parent.clone());
            }
        }
        child.branch = Some(BranchInfo {
            parent: parent.clone(),
            parent_version: self.version,
            segment_homes,
        });
        child
    }

    /// Every object this manifest (belonging to `ns`) references: segment
    /// directories and deletion bitmap files, as logical paths.
    pub fn referenced_objects(
        &self,
        ns: &NamespaceId,
    ) -> Result<(Vec<String>, Vec<String>), KoshaError> {
        let dirs = self
            .segments
            .iter()
            .map(|entry| self.segment_rel_dir(ns, &entry.segment_id))
            .collect();
        let mut files = Vec::with_capacity(self.deletion_vectors.len());
        for (segment, vector) in &self.deletion_vectors {
            files.push(vector.path(ns, segment)?);
        }
        Ok((dirs, files))
    }
}

/// Every object referenced by a set of manifests. An object outside this
/// set is reclaimable; one inside it must never be deleted.
#[derive(Debug, Default, Clone)]
pub struct References {
    segment_dirs: HashSet<String>,
    files: HashSet<String>,
}

impl References {
    /// Collect the references of `manifests` (each with its namespace).
    pub fn collect<'a>(
        manifests: impl IntoIterator<Item = (&'a NamespaceId, &'a Manifest)>,
    ) -> Result<Self, KoshaError> {
        let mut refs = Self::default();
        for (ns, manifest) in manifests {
            refs.add(ns, manifest)?;
        }
        Ok(refs)
    }

    /// Add one manifest's references.
    pub fn add(&mut self, ns: &NamespaceId, manifest: &Manifest) -> Result<(), KoshaError> {
        let (dirs, files) = manifest.referenced_objects(ns)?;
        self.segment_dirs.extend(dirs);
        self.files.extend(files);
        Ok(())
    }

    /// Whether the segment directory `rel_dir` (`{ns}/{segment}`) is
    /// listed by some manifest.
    pub fn holds_segment_dir(&self, rel_dir: &str) -> bool {
        self.segment_dirs.contains(rel_dir.trim_end_matches('/'))
    }

    /// Whether the file `rel_path` is referenced, either as a deletion
    /// bitmap or as part of a referenced segment directory.
    pub fn holds_file(&self, rel_path: &str) -> bool {
        if self.files.contains(rel_path) {
            return true;
        }
        match rel_path.rsplit_once('/') {
            Some((dir, name)) => self.holds_segment_dir(dir) && !is_deletion_bitmap(name),
            None => false,
        }
    }

    /// The files of directory `rel_dir` (named `files`) that may be
    /// deleted, as logical paths. In a referenced segment directory only
    /// unreferenced deletion bitmaps go; in any other directory everything
    /// but a referenced deletion bitmap (a descendant branch can inherit a
    /// bitmap a deleted branch wrote for a segment it did not own).
    pub fn reclaimable_files(&self, rel_dir: &str, files: &[String]) -> Vec<String> {
        let rel_dir = rel_dir.trim_end_matches('/');
        files
            .iter()
            .map(|name| format!("{rel_dir}/{name}"))
            .filter(|path| !self.holds_file(path))
            .collect()
    }

    /// Whether anything under `rel_dir` is referenced.
    pub fn holds_anything_under(&self, rel_dir: &str) -> bool {
        let rel_dir = rel_dir.trim_end_matches('/');
        let prefix = format!("{rel_dir}/");
        self.holds_segment_dir(rel_dir) || self.files.iter().any(|f| f.starts_with(&prefix))
    }
}

/// Deletion bitmaps are content-addressed files inside a segment
/// directory (`deletes-{sha256}.roaring`), referenced one by one.
fn is_deletion_bitmap(name: &str) -> bool {
    name.starts_with("deletes-") && name.ends_with(".roaring")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deletes::DeletionVector;
    use crate::ManifestEntry;

    fn ns(s: &str) -> NamespaceId {
        NamespaceId(s.into())
    }
    fn seg(s: &str) -> SegmentId {
        SegmentId(s.into())
    }

    fn manifest(segments: &[(&str, u32)]) -> Manifest {
        Manifest {
            version: 7,
            segments: segments
                .iter()
                .map(|(id, docs)| ManifestEntry {
                    segment_id: seg(id),
                    doc_count: *docs,
                })
                .collect(),
            segment_footers: Default::default(),
            footer_crc32: Default::default(),
            wal_checkpoint: 42,
            wal_origin: Some("ingest-0".into()),
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            schema: None,
            branch: None,
        }
    }

    #[test]
    fn a_branch_references_every_parent_segment_in_place() {
        let prod = ns("t/prod");
        let mut parent = manifest(&[("p-1", 600_000), ("p-2", 400_000)]);
        let (vector, _) = DeletionVector::encode(&HashSet::from([5])).unwrap();
        parent.deletion_vectors.insert(seg("p-1"), vector);

        let child = parent.branch_from(&prod, 1);
        let test = ns("t/test");
        assert_eq!(child.version, 1);
        assert_eq!(child.wal_checkpoint, 0);
        assert_eq!(child.wal_origin, None);
        assert_eq!(child.segments.len(), 2);
        let info = child.branch.as_ref().unwrap();
        assert_eq!(info.parent, prod);
        assert_eq!(info.parent_version, 7);
        for entry in &child.segments {
            assert_eq!(child.segment_home(&test, &entry.segment_id), &prod);
            assert!(child.is_inherited_segment(&test, &entry.segment_id));
            assert_eq!(
                child.segment_rel_dir(&test, &entry.segment_id),
                format!("t/prod/{}", entry.segment_id.0)
            );
        }
        // Bitmaps resolve to the parent's files, byte for byte the same path.
        let (_, child_files) = child.referenced_objects(&test).unwrap();
        let (_, parent_files) = parent.referenced_objects(&prod).unwrap();
        assert_eq!(child_files, parent_files);
        // The parent manifest is untouched.
        assert!(parent.branch.is_none());
        assert!(parent.deletion_vectors[&seg("p-1")].namespace.is_none());
    }

    #[test]
    fn a_branch_of_a_branch_points_at_the_original_owner() {
        let prod = ns("t/prod");
        let mid = ns("t/mid");
        let mut first = manifest(&[("p-1", 10)]).branch_from(&prod, 1);
        // The middle branch wrote a segment of its own.
        first.segments.push(ManifestEntry {
            segment_id: seg("m-1"),
            doc_count: 3,
        });
        let second = first.branch_from(&mid, 1);
        let leaf = ns("t/leaf");
        assert_eq!(second.segment_home(&leaf, &seg("p-1")), &prod);
        assert_eq!(second.segment_home(&leaf, &seg("m-1")), &mid);
        assert_eq!(second.branch.as_ref().unwrap().parent, mid);
    }

    #[test]
    fn segment_homes_are_pruned_to_listed_segments() {
        let mut child = manifest(&[("p-1", 1), ("p-2", 1)]).branch_from(&ns("t/prod"), 1);
        child.segments.retain(|e| e.segment_id.0 == "p-2");
        child.retain_listed_segment_homes();
        let homes = &child.branch.as_ref().unwrap().segment_homes;
        assert_eq!(homes.len(), 1);
        assert!(homes.contains_key(&seg("p-2")));
    }

    #[test]
    fn manifests_without_branch_info_round_trip_unchanged() {
        let plain = manifest(&[("s", 1)]);
        let json = serde_json::to_value(&plain).unwrap();
        assert!(json.get("branch").is_none(), "{json}");
        let legacy = r#"{"version":3,"segments":[{"segment_id":"s","doc_count":1}]}"#;
        let decoded: Manifest = serde_json::from_str(legacy).unwrap();
        assert!(decoded.branch.is_none());
        let n = ns("t/a");
        assert_eq!(decoded.segment_rel_dir(&n, &seg("s")), "t/a/s");
        assert!(!decoded.is_inherited_segment(&n, &seg("s")));
        let child = plain.branch_from(&n, 1);
        let again: Manifest =
            serde_json::from_value(serde_json::to_value(&child).unwrap()).unwrap();
        assert!(again.same_contents(&child));
    }

    #[test]
    fn references_keep_shared_segments_and_inherited_bitmaps() {
        let prod = ns("t/prod");
        let branch = ns("t/branch");
        let grandchild = ns("t/grand");
        // The branch inherits p-1, writes b-1, and deletes from p-1 (its own
        // bitmap, in its own prefix under p-1's id).
        let mut b = manifest(&[("p-1", 10)]).branch_from(&prod, 1);
        b.segments.push(ManifestEntry {
            segment_id: seg("b-1"),
            doc_count: 4,
        });
        let (own, _) = DeletionVector::encode(&HashSet::from([1])).unwrap();
        b.deletion_vectors.insert(seg("p-1"), own.clone());
        // A grandchild branched from the branch keeps b-1 and the bitmap.
        let g = b.branch_from(&branch, 1);

        let refs =
            References::collect([(&prod, &manifest(&[("p-1", 10)])), (&grandchild, &g)]).unwrap();
        assert!(refs.holds_segment_dir("t/prod/p-1"));
        assert!(refs.holds_segment_dir("t/branch/b-1"));
        let bitmap = own.path(&branch, &seg("p-1")).unwrap();
        assert!(bitmap.starts_with("t/branch/p-1/"));
        assert!(refs.holds_file(&bitmap));
        assert!(refs.holds_anything_under("t/branch/p-1"));

        // Inside a referenced segment dir only stale bitmaps go.
        let files = vec![
            "inverted.idx".to_string(),
            "_COMMIT".to_string(),
            "deletes-0000.roaring".to_string(),
        ];
        assert_eq!(
            refs.reclaimable_files("t/branch/b-1", &files),
            vec!["t/branch/b-1/deletes-0000.roaring".to_string()]
        );
        // An unreferenced dir goes entirely.
        assert_eq!(refs.reclaimable_files("t/branch/b-old", &files).len(), 3);
        assert!(!refs.holds_anything_under("t/branch/b-old"));
        // A dir holding only an inherited bitmap keeps exactly that file.
        let name = bitmap.rsplit_once('/').unwrap().1.to_string();
        let keep = refs.reclaimable_files("t/branch/p-1", &[name, "deletes-ffff.roaring".into()]);
        assert_eq!(keep, vec!["t/branch/p-1/deletes-ffff.roaring".to_string()]);
    }
}
