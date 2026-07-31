//! Control plane (DESIGN.md §5 and §6.3, implementation plan Epic 6): the
//! namespace/schema registry and the manifest pointer store backing
//! compare-and-swap manifest publishes.
//!
//! In-memory (`Controller`) is the default; Postgres (`PgStore`) is used in
//! production via the `postgres` feature and a `DATABASE_URL` env var.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use kosha_core::{ControlStore, KoshaError, Manifest, NamespaceId, SegmentGcEntry, SegmentId};

#[cfg(feature = "postgres")]
mod postgres;

#[cfg(feature = "postgres")]
pub use postgres::PgStore;

/// In-memory namespace registry and manifest store.
///
/// Tracks namespaces and their current segment manifests. Not internally
/// synchronized — the server guards it with a `Mutex`.
pub struct Controller {
    /// Maps namespace ID → current manifest.
    manifests: HashMap<NamespaceId, Manifest>,
    /// Track which namespaces have been created.
    namespaces: Vec<NamespaceId>,
    /// Orphaned segments awaiting grace-period GC.
    segment_gc: HashMap<(NamespaceId, SegmentId), SegmentGcEntry>,
}

impl Controller {
    pub fn new() -> Self {
        Self {
            manifests: HashMap::new(),
            namespaces: Vec::new(),
            segment_gc: HashMap::new(),
        }
    }

    fn now_unix() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    /// Create a new namespace. Returns an error if it already exists.
    pub fn create_namespace(&mut self, id: NamespaceId) -> Result<(), KoshaError> {
        if self.namespaces.contains(&id) {
            return Err(KoshaError::NamespaceNotFound(id));
        }
        self.namespaces.push(id.clone());
        self.manifests.insert(
            id,
            Manifest {
                version: 0,
                segments: Vec::new(),
            },
        );
        Ok(())
    }

    /// Ensure a namespace exists, creating it if necessary.
    pub fn ensure_namespace(&mut self, id: NamespaceId) {
        if !self.namespaces.contains(&id) {
            self.namespaces.push(id.clone());
            self.manifests.insert(
                id,
                Manifest {
                    version: 0,
                    segments: Vec::new(),
                },
            );
        }
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
        let current = self
            .manifests
            .get_mut(id)
            .ok_or_else(|| KoshaError::NamespaceNotFound(id.clone()))?;

        if current.version != expected_version {
            return Err(KoshaError::NotFound(format!(
                "manifest version mismatch: expected {expected_version}, got {}",
                current.version
            )));
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

    pub fn mark_segments_for_gc(
        &mut self,
        namespace: &NamespaceId,
        segment_ids: &[SegmentId],
        by_version: u64,
    ) -> Result<(), KoshaError> {
        let now = Self::now_unix();
        for segment_id in segment_ids {
            self.segment_gc.insert(
                (namespace.clone(), segment_id.clone()),
                SegmentGcEntry {
                    namespace_id: namespace.clone(),
                    segment_id: segment_id.clone(),
                    unreferenced_at_unix: now,
                    unreferenced_by_version: by_version,
                },
            );
        }
        Ok(())
    }

    pub fn list_gc_candidates(&self, older_than_unix: i64) -> Result<Vec<SegmentGcEntry>, KoshaError> {
        let mut out: Vec<_> = self
            .segment_gc
            .values()
            .filter(|entry| entry.unreferenced_at_unix <= older_than_unix)
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            a.unreferenced_at_unix
                .cmp(&b.unreferenced_at_unix)
                .then_with(|| a.namespace_id.0.cmp(&b.namespace_id.0))
                .then_with(|| a.segment_id.0.cmp(&b.segment_id.0))
        });
        Ok(out)
    }

    pub fn clear_gc_mark(
        &mut self,
        namespace: &NamespaceId,
        segment_id: &SegmentId,
    ) -> Result<(), KoshaError> {
        self.segment_gc
            .remove(&(namespace.clone(), segment_id.clone()));
        Ok(())
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
    fn mark_segments_for_gc(
        &mut self,
        namespace: &NamespaceId,
        segment_ids: &[SegmentId],
        by_version: u64,
    ) -> Result<(), KoshaError> {
        self.mark_segments_for_gc(namespace, segment_ids, by_version)
    }
    fn list_gc_candidates(&self, older_than_unix: i64) -> Result<Vec<SegmentGcEntry>, KoshaError> {
        self.list_gc_candidates(older_than_unix)
    }
    fn clear_gc_mark(
        &mut self,
        namespace: &NamespaceId,
        segment_id: &SegmentId,
    ) -> Result<(), KoshaError> {
        self.clear_gc_mark(namespace, segment_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::ManifestEntry;

    #[test]
    fn create_and_list_namespaces() {
        let mut ctrl = Controller::new();
        let ns = NamespaceId("org1/matter42".into());

        ctrl.create_namespace(ns.clone()).unwrap();
        assert!(ctrl.has_namespace(&ns));
        assert_eq!(ctrl.namespace_count(), 1);

        // Duplicate creation should fail.
        assert!(ctrl.create_namespace(ns).is_err());
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
        };
        assert!(ctrl.compare_and_swap_manifest(&ns, 0, manifest_v2).is_err());
    }

    #[test]
    fn segment_gc_mark_list_clear() {
        let mut ctrl = Controller::new();
        let ns = NamespaceId("tenant/idx".into());
        let seg = SegmentId("tenant_idx-000001".into());

        ctrl.mark_segments_for_gc(&ns, &[seg.clone()], 3).unwrap();
        let due = ctrl.list_gc_candidates(i64::MAX).unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].segment_id, seg);
        assert_eq!(due[0].unreferenced_by_version, 3);

        // Not yet due if cutoff is before mark time.
        assert!(ctrl.list_gc_candidates(0).unwrap().is_empty());

        ctrl.clear_gc_mark(&ns, &seg).unwrap();
        assert!(ctrl.list_gc_candidates(i64::MAX).unwrap().is_empty());
    }
}
