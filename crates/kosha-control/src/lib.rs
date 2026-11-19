//! Control plane (DESIGN.md §5 and §6.3, implementation plan Epic 6): the
//! namespace/schema registry and the manifest pointer store backing
//! compare-and-swap manifest publishes.
//!
//! In-memory (`Controller`) is the default; Postgres (`PgStore`) is used in
//! production via the `postgres` feature and a `DATABASE_URL` env var.

use std::collections::HashMap;

use kosha_core::{ControlStore, KoshaError, Manifest, NamespaceId};

#[cfg(feature = "postgres")]
mod postgres;

#[cfg(feature = "postgres")]
pub use postgres::PgStore;

#[cfg(feature = "postgres")]
mod hydration_lease;

#[cfg(feature = "postgres")]
pub use hydration_lease::{HydrationLease, HydrationLeaseStore};

/// In-memory namespace registry and manifest store.
///
/// Tracks namespaces and their current segment manifests. Not internally
/// synchronized — the server guards it with a `Mutex`.
pub struct Controller {
    /// Maps namespace ID → current manifest.
    manifests: HashMap<NamespaceId, Manifest>,
    /// Track which namespaces have been created.
    namespaces: Vec<NamespaceId>,
    /// Deleted namespaces → the highest manifest version they reached. A
    /// re-created namespace continues above it (see
    /// [`ControlStore::delete_namespace`]).
    retired: HashMap<NamespaceId, u64>,
}

fn empty_manifest(version: u64) -> Manifest {
    Manifest::empty(version)
}

impl Controller {
    pub fn new() -> Self {
        Self {
            manifests: HashMap::new(),
            namespaces: Vec::new(),
            retired: HashMap::new(),
        }
    }

    /// The version a new incarnation of `id` starts at.
    fn initial_version(&self, id: &NamespaceId) -> u64 {
        kosha_core::initial_manifest_version(self.retired.get(id).copied())
    }

    /// Create a new namespace. `NamespaceExists` if it already exists.
    pub fn create_namespace(&mut self, id: NamespaceId) -> Result<(), KoshaError> {
        if self.namespaces.contains(&id) {
            return Err(KoshaError::NamespaceExists(id));
        }
        self.ensure_namespace(id);
        Ok(())
    }

    /// Ensure a namespace exists, creating it if necessary.
    pub fn ensure_namespace(&mut self, id: NamespaceId) {
        if !self.namespaces.contains(&id) {
            let version = self.initial_version(&id);
            self.namespaces.push(id.clone());
            self.manifests.insert(id, empty_manifest(version));
        }
    }

    /// Delete a namespace, keeping its retired version. See
    /// [`ControlStore::delete_namespace`].
    pub fn delete_namespace(
        &mut self,
        id: &NamespaceId,
        retire_at_least: u64,
    ) -> Result<(Manifest, u64), KoshaError> {
        let Some(at) = self.namespaces.iter().position(|ns| ns == id) else {
            return Err(KoshaError::NamespaceNotFound(id.clone()));
        };
        self.namespaces.remove(at);
        let manifest = self
            .manifests
            .remove(id)
            .unwrap_or_else(|| empty_manifest(0));
        let retired = self
            .retired
            .get(id)
            .copied()
            .unwrap_or(0)
            .max(manifest.version)
            .max(retire_at_least);
        self.retired.insert(id.clone(), retired);
        Ok((manifest, retired))
    }

    /// Check if a namespace exists.
    pub fn has_namespace(&self, id: &NamespaceId) -> bool {
        self.namespaces.contains(id)
    }

    /// Get the current manifest for a namespace.
    pub fn manifest(&self, id: &NamespaceId) -> Option<&Manifest> {
        self.manifests.get(id)
    }

    /// Get a mutable reference to the manifest for a namespace.
    pub fn manifest_mut(&mut self, id: &NamespaceId) -> Option<&mut Manifest> {
        self.manifests.get_mut(id)
    }

    /// Persist a manifest for a namespace (upsert, last-write-wins).
    /// Registers the namespace if it wasn't already known.
    pub fn save_manifest(
        &mut self,
        id: &NamespaceId,
        manifest: &Manifest,
    ) -> Result<(), KoshaError> {
        if !self.namespaces.contains(id) {
            self.namespaces.push(id.clone());
        }
        self.manifests.insert(id.clone(), manifest.clone());
        Ok(())
    }

    /// Atomically update the manifest for a namespace (compare-and-swap style).
    ///
    /// Returns an error if the manifest version doesn't match, indicating a
    /// concurrent modification.
    pub fn compare_and_swap_manifest(
        &mut self,
        id: &NamespaceId,
        expected_version: u64,
        new_manifest: Manifest,
    ) -> Result<(), KoshaError> {
        // A missing namespace is bootstrapped by a writer that knows
        // nothing (expected 0) or one that knows the version a re-created
        // namespace starts at. A bootstrap at a retired namespace's initial
        // version with expected 0 then conflicts, and the writer rebases
        // onto the empty manifest.
        if !self.manifests.contains_key(id)
            && (expected_version == 0 || expected_version == self.initial_version(id))
        {
            self.ensure_namespace(id.clone());
        }
        let current = self
            .manifests
            .get_mut(id)
            .ok_or_else(|| KoshaError::ManifestConflict {
                namespace: id.clone(),
                expected: expected_version,
                actual: None,
            })?;
        if current.same_contents(&new_manifest) {
            return Ok(());
        }
        if current.version != expected_version || new_manifest.version <= expected_version {
            return Err(KoshaError::ManifestConflict {
                namespace: id.clone(),
                expected: expected_version,
                actual: Some(current.version),
            });
        }

        *current = new_manifest;
        Ok(())
    }

    /// List all registered namespaces.
    pub fn list_namespaces(&self) -> &[NamespaceId] {
        &self.namespaces
    }

    /// Return the total number of namespaces.
    pub fn namespace_count(&self) -> usize {
        self.namespaces.len()
    }
}

impl Default for Controller {
    fn default() -> Self {
        Self::new()
    }
}

impl ControlStore for Controller {
    fn create_namespace(&mut self, id: NamespaceId) -> Result<(), KoshaError> {
        self.create_namespace(id)
    }
    fn ensure_namespace(&mut self, id: NamespaceId) {
        self.ensure_namespace(id)
    }
    fn has_namespace(&self, id: &NamespaceId) -> bool {
        self.has_namespace(id)
    }
    fn manifest(&self, id: &NamespaceId) -> Option<&Manifest> {
        self.manifest(id)
    }
    fn manifest_mut(&mut self, id: &NamespaceId) -> Option<&mut Manifest> {
        self.manifest_mut(id)
    }
    fn manifest_version(&self, id: &NamespaceId) -> Option<u64> {
        self.manifest(id).map(|manifest| manifest.version)
    }
    fn save_manifest(&mut self, id: &NamespaceId, manifest: &Manifest) -> Result<(), KoshaError> {
        self.save_manifest(id, manifest)
    }
    fn compare_and_swap_manifest(
        &mut self,
        id: &NamespaceId,
        expected_version: u64,
        new_manifest: Manifest,
    ) -> Result<(), KoshaError> {
        self.compare_and_swap_manifest(id, expected_version, new_manifest)
    }
    fn list_namespaces(&self) -> Vec<NamespaceId> {
        self.list_namespaces().to_vec()
    }
    fn namespace_count(&self) -> usize {
        self.namespace_count()
    }
    fn delete_namespace(
        &mut self,
        id: &NamespaceId,
        retire_at_least: u64,
    ) -> Result<(Manifest, u64), KoshaError> {
        self.delete_namespace(id, retire_at_least)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::ManifestEntry;

    #[test]
    fn cas_bootstrap_retries_and_rejects_nonadvancing_snapshots() {
        let mut ctrl = Controller::new();
        let ns = NamespaceId("cas".into());
        let candidate = Manifest {
            version: 3,
            segments: vec![],
            segment_footers: Default::default(),
            footer_crc32: Default::default(),
            wal_checkpoint: 2,
            wal_origin: Some("writer-a".into()),
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
        };
        ctrl.compare_and_swap_manifest(&ns, 0, candidate.clone())
            .unwrap();
        ctrl.compare_and_swap_manifest(&ns, 0, candidate.clone())
            .unwrap();
        let mut different = candidate.clone();
        different.wal_origin = Some("writer-b".into());
        assert!(matches!(
            ctrl.compare_and_swap_manifest(&ns, 3, different),
            Err(KoshaError::ManifestConflict { .. })
        ));
        assert!(ctrl.manifest(&ns).unwrap().same_contents(&candidate));
        let mut older = candidate.clone();
        older.version = 2;
        assert!(ctrl.compare_and_swap_manifest(&ns, 3, older).is_err());
    }

    #[test]
    fn create_and_list_namespaces() {
        let mut ctrl = Controller::new();
        let ns = NamespaceId("org1/matter42".into());

        ctrl.create_namespace(ns.clone()).unwrap();
        assert!(ctrl.has_namespace(&ns));
        assert_eq!(ctrl.namespace_count(), 1);

        // Duplicate creation should fail with a conflict, not "not found".
        assert!(matches!(
            ctrl.create_namespace(ns),
            Err(KoshaError::NamespaceExists(_))
        ));
    }

    #[test]
    fn delete_namespace_retires_its_version_and_recreate_continues_above_it() {
        let mut ctrl = Controller::new();
        let ns = NamespaceId("t/docs".into());
        assert!(matches!(
            ctrl.delete_namespace(&ns, 0),
            Err(KoshaError::NamespaceNotFound(_))
        ));
        ctrl.create_namespace(ns.clone()).unwrap();
        let mut published = ctrl.manifest(&ns).unwrap().clone();
        published.version = 4;
        ctrl.compare_and_swap_manifest(&ns, 0, published).unwrap();

        let (last, retired) = ctrl.delete_namespace(&ns, 0).unwrap();
        assert_eq!((last.version, retired), (4, 4));
        assert!(!ctrl.has_namespace(&ns));
        assert!(ctrl.manifest(&ns).is_none());
        assert!(ctrl.list_namespaces().is_empty());

        // Explicit re-create starts above every version the old one used.
        ctrl.create_namespace(ns.clone()).unwrap();
        assert_eq!(ctrl.manifest(&ns).unwrap().version, 5);
        assert!(ctrl.manifest(&ns).unwrap().segments.is_empty());

        // A writer that remembers the deleted incarnation (published 4) can
        // never commit over the new one.
        let (_, retired) = ctrl.delete_namespace(&ns, 9).unwrap();
        assert_eq!(retired, 9, "retire_at_least raises the floor");
        let mut stale = empty_manifest(5);
        stale.wal_checkpoint = 1;
        assert!(matches!(
            ctrl.compare_and_swap_manifest(&ns, 4, stale),
            Err(KoshaError::ManifestConflict { actual: None, .. })
        ));
        assert!(
            !ctrl.has_namespace(&ns),
            "a stale writer must not resurrect it"
        );

        // Auto-create by a fresh writer (expected 0) bootstraps at 10 and
        // conflicts, so the writer rebases instead of publishing version 1.
        assert!(matches!(
            ctrl.compare_and_swap_manifest(&ns, 0, empty_manifest(1)),
            Err(KoshaError::ManifestConflict {
                actual: Some(10),
                ..
            })
        ));
        assert_eq!(ctrl.manifest(&ns).unwrap().version, 10);
        // A writer seeded with the initial version commits directly.
        ctrl.delete_namespace(&ns, 0).unwrap();
        ctrl.compare_and_swap_manifest(&ns, 11, empty_manifest(12))
            .unwrap();
        assert_eq!(ctrl.manifest(&ns).unwrap().version, 12);
    }

    #[test]
    fn ensure_namespace_idempotent() {
        let mut ctrl = Controller::new();
        let ns = NamespaceId("test".into());

        ctrl.ensure_namespace(ns.clone());
        assert_eq!(ctrl.namespace_count(), 1);

        // Second ensure should be a no-op.
        ctrl.ensure_namespace(ns);
        assert_eq!(ctrl.namespace_count(), 1);
    }

    #[test]
    fn save_manifest_upserts() {
        let mut ctrl = Controller::new();
        let ns = NamespaceId("tenant/idx".into());

        // Save without a prior create: registers the namespace too.
        ctrl.save_manifest(
            &ns,
            &Manifest {
                version: 3,
                segments: vec![ManifestEntry {
                    segment_id: kosha_core::SegmentId("tenant_idx-000000".into()),
                    doc_count: 7,
                }],
                segment_footers: Default::default(),
                footer_crc32: Default::default(),
                wal_checkpoint: 0,
                wal_origin: None,
                tombstones: Default::default(),
                deletion_vectors: Default::default(),
            },
        )
        .unwrap();
        assert!(ctrl.has_namespace(&ns));
        assert_eq!(ctrl.manifest(&ns).unwrap().version, 3);
        assert_eq!(ctrl.manifest(&ns).unwrap().segments.len(), 1);

        // Overwrite is last-write-wins.
        ctrl.save_manifest(
            &ns,
            &Manifest {
                version: 4,
                segments: vec![],
                segment_footers: Default::default(),
                footer_crc32: Default::default(),
                wal_checkpoint: 0,
                wal_origin: None,
                tombstones: Default::default(),
                deletion_vectors: Default::default(),
            },
        )
        .unwrap();
        assert_eq!(ctrl.manifest(&ns).unwrap().version, 4);
        assert_eq!(ctrl.namespace_count(), 1);

        // The trait's default `manifest_cloned` works off `manifest()`.
        assert_eq!(ctrl.manifest_cloned(&ns).unwrap().version, 4);
    }

    #[test]
    fn manifest_cas() {
        let mut ctrl = Controller::new();
        let ns = NamespaceId("test".into());
        ctrl.create_namespace(ns.clone()).unwrap();

        let manifest_v1 = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: kosha_core::SegmentId("seg-001".into()),
                doc_count: 10,
            }],
            segment_footers: Default::default(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
        };

        // CAS with version 0 → should succeed (initial version is 0).
        ctrl.compare_and_swap_manifest(&ns, 0, manifest_v1.clone())
            .unwrap();

        let stored = ctrl.manifest(&ns).unwrap();
        assert_eq!(stored.version, 1);
        assert_eq!(stored.segments.len(), 1);

        // CAS with wrong version → should fail.
        let manifest_v2 = Manifest {
            version: 2,
            segments: vec![],
            segment_footers: Default::default(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
        };
        assert!(ctrl.compare_and_swap_manifest(&ns, 0, manifest_v2).is_err());
    }
}
