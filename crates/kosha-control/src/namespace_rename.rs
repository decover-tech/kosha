//! Re-key a namespace in the control plane (#344).
//!
//! Since #262 every client namespace resolves to `<tenant>/<name>`; data
//! written before that lives under bare ids no tenant can address.
//! `kosha-server migrate-namespace` copies such a namespace's segment
//! objects to the scoped prefix and then calls [`NamespaceRename`] here to
//! move its control-plane rows in one step:
//!
//! - the `kosha.namespaces` row and the `kosha.manifests` row move from
//!   `from` to `to` (keeping `created_at`);
//! - the manifest's own namespace references (branch parent, segment homes,
//!   deletion-vector homes) are rewritten from `from` to `to`;
//! - `from` is recorded in `kosha.retired_namespaces` at its last version,
//!   so a namespace later re-created under the bare id starts above it;
//! - the manifest version moves above any version `to` was retired at, so
//!   caches keyed by `(namespace, version)` never mix incarnations;
//! - the writer lease moves to `to` with its holder, expiry and token (the
//!   token never goes backwards for `to`), and the lease left on `from` is
//!   bumped and expired, fencing any writer that still holds it.
//!
//! The rename is refused when `to` already exists or when `from`'s
//! manifest is no longer at `expected_version`.

use std::time::Instant;

use kosha_core::{KoshaError, Manifest, NamespaceId};

use crate::{Controller, WriterLease};

/// What a successful rename left behind.
#[derive(Debug, Clone)]
pub struct RenameOutcome {
    /// The manifest now stored under `to`.
    pub manifest: Manifest,
    /// The writer-lease token now on `to`, when `from` had a lease row.
    /// The lease keeps its holder, so a holder that leased `from` keeps
    /// holding `to` under this token.
    pub lease_token: Option<u64>,
}

/// A control store that can re-key a namespace atomically.
pub trait NamespaceRename {
    /// Move `from` (whose manifest must be at `expected_version`) to `to`.
    /// See the module docs for what moves. Errors:
    /// `NamespaceNotFound(from)`, `NamespaceExists(to)`, or
    /// `ManifestConflict` when the version moved. Nothing changes on error.
    fn rename_namespace(
        &mut self,
        from: &NamespaceId,
        to: &NamespaceId,
        expected_version: u64,
    ) -> Result<RenameOutcome, KoshaError>;
}

/// Rewrite every reference to namespace `from` inside `manifest` to `to`:
/// the branch parent, branch segment homes and deletion-vector homes.
/// Returns how many references changed. Segment ids are opaque directory
/// names and are left alone.
pub fn rekey_manifest_references(
    manifest: &mut Manifest,
    from: &NamespaceId,
    to: &NamespaceId,
) -> usize {
    let mut changed = 0;
    let mut swap = |id: &mut NamespaceId| {
        if id == from {
            *id = to.clone();
            changed += 1;
        }
    };
    if let Some(branch) = manifest.branch.as_mut() {
        swap(&mut branch.parent);
        for home in branch.segment_homes.values_mut() {
            swap(home);
        }
    }
    for vector in manifest.deletion_vectors.values_mut() {
        if let Some(home) = vector.namespace.as_mut() {
            swap(home);
        }
    }
    changed
}

/// The version the manifest takes under `to`: its own, or one past the
/// version a deleted `to` was retired at, whichever is higher.
pub(crate) fn renamed_version(version: u64, to_retired: Option<u64>) -> u64 {
    version.max(kosha_core::initial_manifest_version(to_retired))
}

impl NamespaceRename for Controller {
    fn rename_namespace(
        &mut self,
        from: &NamespaceId,
        to: &NamespaceId,
        expected_version: u64,
    ) -> Result<RenameOutcome, KoshaError> {
        if from == to {
            return Err(KoshaError::NamespaceExists(to.clone()));
        }
        let Some(current) = self.manifests.get(from) else {
            return Err(KoshaError::NamespaceNotFound(from.clone()));
        };
        if !self.namespaces.contains(from) {
            return Err(KoshaError::NamespaceNotFound(from.clone()));
        }
        if self.namespaces.contains(to) || self.manifests.contains_key(to) {
            return Err(KoshaError::NamespaceExists(to.clone()));
        }
        if current.version != expected_version {
            return Err(KoshaError::ManifestConflict {
                namespace: from.clone(),
                expected: expected_version,
                actual: Some(current.version),
            });
        }

        // Every check passed: nothing below can fail.
        let mut manifest = self.manifests.remove(from).expect("checked above");
        let old_version = manifest.version;
        rekey_manifest_references(&mut manifest, from, to);
        manifest.version = renamed_version(old_version, self.retired.get(to).copied());
        let at = self
            .namespaces
            .iter()
            .position(|ns| ns == from)
            .expect("checked above");
        self.namespaces[at] = to.clone();
        self.manifests.insert(to.clone(), manifest.clone());
        let retired = self.retired.entry(from.clone()).or_insert(0);
        *retired = (*retired).max(old_version);

        let lease_token = match self.writer_leases.get_mut(from) {
            Some(old) => {
                let carried = old.clone();
                // Fence whoever still holds `from`.
                old.token += 1;
                old.expires_at = Instant::now();
                let token = self
                    .writer_leases
                    .get(to)
                    .map_or(carried.token, |existing| existing.token.max(carried.token));
                self.writer_leases.insert(
                    to.clone(),
                    WriterLease {
                        holder: carried.holder,
                        token,
                        expires_at: carried.expires_at,
                    },
                );
                Some(token)
            }
            None => None,
        };
        Ok(RenameOutcome {
            manifest,
            lease_token,
        })
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;
    use crate::PgStore;

    fn control_plane_error(e: sqlx::Error) -> KoshaError {
        KoshaError::ControlPlane(e.to_string())
    }

    /// The `segments_json` text stored under `to`: the manifest with its
    /// references re-keyed and its new version. A legacy row (a bare JSON
    /// array of segment entries) names no namespace and is kept as is.
    fn renamed_segments_json(
        segments_json: &str,
        from: &NamespaceId,
        to: &NamespaceId,
        version: u64,
    ) -> Result<String, KoshaError> {
        match serde_json::from_str::<Manifest>(segments_json) {
            Ok(mut manifest) => {
                rekey_manifest_references(&mut manifest, from, to);
                manifest.version = version;
                Ok(serde_json::to_string(&manifest)?)
            }
            Err(_) => Ok(segments_json.to_owned()),
        }
    }

    fn decode(version: u64, segments_json: &str) -> Manifest {
        match serde_json::from_str::<Manifest>(segments_json) {
            Ok(mut manifest) => {
                manifest.version = version;
                manifest
            }
            Err(_) => {
                // Legacy array rows: decode just the segment list.
                #[derive(serde::Deserialize)]
                struct Entry {
                    segment_id: String,
                    doc_count: u32,
                }
                let mut manifest = Manifest::empty(version);
                if let Ok(entries) = serde_json::from_str::<Vec<Entry>>(segments_json) {
                    manifest.segments = entries
                        .into_iter()
                        .map(|e| kosha_core::ManifestEntry {
                            segment_id: kosha_core::SegmentId(e.segment_id),
                            doc_count: e.doc_count,
                        })
                        .collect();
                }
                manifest
            }
        }
    }

    impl NamespaceRename for PgStore {
        /// One transaction: lock `from`'s manifest row, check the version
        /// and that `to` is free, insert `to`'s rows, delete `from`'s,
        /// record `from` as retired and move the writer lease. Any error
        /// drops the transaction, which rolls it back.
        fn rename_namespace(
            &mut self,
            from: &NamespaceId,
            to: &NamespaceId,
            expected_version: u64,
        ) -> Result<RenameOutcome, KoshaError> {
            if from == to {
                return Err(KoshaError::NamespaceExists(to.clone()));
            }
            let (pool, rt) = self.pool_and_runtime();
            rt.block_on(async {
                let mut tx = pool.begin().await.map_err(control_plane_error)?;
                let row: Option<(i64, String)> = sqlx::query_as(
                    "SELECT version, segments_json FROM kosha.manifests \
                     WHERE namespace_id = $1 FOR UPDATE",
                )
                .bind(&from.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(control_plane_error)?;
                let Some((version, segments_json)) = row else {
                    return Err(KoshaError::NamespaceNotFound(from.clone()));
                };
                let version = version as u64;
                if version != expected_version {
                    return Err(KoshaError::ManifestConflict {
                        namespace: from.clone(),
                        expected: expected_version,
                        actual: Some(version),
                    });
                }
                let to_retired: Option<(i64,)> = sqlx::query_as(
                    "SELECT last_version FROM kosha.retired_namespaces WHERE id = $1",
                )
                .bind(&to.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(control_plane_error)?;
                let new_version = renamed_version(version, to_retired.map(|(v,)| v as u64));
                let new_json = renamed_segments_json(&segments_json, from, to, new_version)?;

                let inserted = sqlx::query(
                    "INSERT INTO kosha.namespaces (id, created_at) \
                     SELECT $2, created_at FROM kosha.namespaces WHERE id = $1 \
                     ON CONFLICT (id) DO NOTHING",
                )
                .bind(&from.0)
                .bind(&to.0)
                .execute(&mut *tx)
                .await
                .map_err(control_plane_error)?;
                if inserted.rows_affected() == 0 {
                    // Either `to` exists or `from` has a manifest without a
                    // namespace row (the foreign key rules that out).
                    return Err(KoshaError::NamespaceExists(to.clone()));
                }
                let inserted = sqlx::query(
                    "INSERT INTO kosha.manifests (namespace_id, version, segments_json) \
                     VALUES ($1, $2, $3) ON CONFLICT (namespace_id) DO NOTHING",
                )
                .bind(&to.0)
                .bind(new_version as i64)
                .bind(&new_json)
                .execute(&mut *tx)
                .await
                .map_err(control_plane_error)?;
                if inserted.rows_affected() == 0 {
                    return Err(KoshaError::NamespaceExists(to.clone()));
                }
                sqlx::query("DELETE FROM kosha.manifests WHERE namespace_id = $1")
                    .bind(&from.0)
                    .execute(&mut *tx)
                    .await
                    .map_err(control_plane_error)?;
                sqlx::query("DELETE FROM kosha.namespaces WHERE id = $1")
                    .bind(&from.0)
                    .execute(&mut *tx)
                    .await
                    .map_err(control_plane_error)?;
                sqlx::query(
                    "INSERT INTO kosha.retired_namespaces AS r (id, last_version) VALUES ($1, $2) \
                     ON CONFLICT (id) DO UPDATE SET \
                     last_version = GREATEST(r.last_version, EXCLUDED.last_version), \
                     retired_at = NOW()",
                )
                .bind(&from.0)
                .bind(version as i64)
                .execute(&mut *tx)
                .await
                .map_err(control_plane_error)?;

                let lease: Option<(
                    String,
                    i64,
                    sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>,
                )> = sqlx::query_as(
                    "SELECT holder, token, expires_at FROM kosha.writer_leases \
                         WHERE namespace_id = $1 FOR UPDATE",
                )
                .bind(&from.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(control_plane_error)?;
                let lease_token = match lease {
                    Some((holder, token, expires_at)) => {
                        let (carried,): (i64,) = sqlx::query_as(
                            "INSERT INTO kosha.writer_leases AS l \
                                 (namespace_id, holder, token, expires_at) \
                             VALUES ($1, $2, $3, $4) \
                             ON CONFLICT (namespace_id) DO UPDATE SET \
                                 holder = EXCLUDED.holder, \
                                 token = GREATEST(l.token, EXCLUDED.token), \
                                 expires_at = EXCLUDED.expires_at \
                             RETURNING token",
                        )
                        .bind(&to.0)
                        .bind(&holder)
                        .bind(token)
                        .bind(expires_at)
                        .fetch_one(&mut *tx)
                        .await
                        .map_err(control_plane_error)?;
                        // Fence whoever still holds `from`.
                        sqlx::query(
                            "UPDATE kosha.writer_leases SET token = token + 1, expires_at = now() \
                             WHERE namespace_id = $1",
                        )
                        .bind(&from.0)
                        .execute(&mut *tx)
                        .await
                        .map_err(control_plane_error)?;
                        Some(carried as u64)
                    }
                    None => None,
                };
                tx.commit().await.map_err(control_plane_error)?;
                Ok(RenameOutcome {
                    manifest: decode(new_version, &new_json),
                    lease_token,
                })
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn legacy_rows_are_kept_and_durable_rows_are_rekeyed() {
            let from = NamespaceId("corpus".into());
            let to = NamespaceId("acme/corpus".into());
            let legacy = r#"[{"segment_id":"s","doc_count":2}]"#;
            assert_eq!(
                renamed_segments_json(legacy, &from, &to, 9).unwrap(),
                legacy
            );
            assert_eq!(decode(9, legacy).segments[0].doc_count, 2);

            let mut manifest = Manifest::empty(3);
            manifest.branch = Some(kosha_core::branch::BranchInfo {
                parent: from.clone(),
                parent_version: 1,
                segment_homes: Default::default(),
            });
            let json = serde_json::to_string(&manifest).unwrap();
            let renamed = renamed_segments_json(&json, &from, &to, 9).unwrap();
            let decoded = decode(9, &renamed);
            assert_eq!(decoded.version, 9);
            assert_eq!(decoded.branch.unwrap().parent, to);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::{ManifestEntry, SegmentId};
    use std::time::Duration;

    fn manifest_at(version: u64, docs: &[(&str, u32)]) -> Manifest {
        let mut manifest = Manifest::empty(version);
        for (segment, doc_count) in docs {
            manifest.segments.push(ManifestEntry {
                segment_id: SegmentId((*segment).into()),
                doc_count: *doc_count,
            });
        }
        manifest
    }

    fn ns(id: &str) -> NamespaceId {
        NamespaceId(id.into())
    }

    #[test]
    fn rename_moves_the_manifest_and_retires_the_bare_id() {
        let mut ctrl = Controller::new();
        let (from, to) = (ns("corpus"), ns("acme/corpus"));
        ctrl.compare_and_swap_manifest(&from, 0, manifest_at(4, &[("corpus-000001", 7)]))
            .unwrap();
        let outcome = ctrl.rename_namespace(&from, &to, 4).unwrap();
        assert_eq!(outcome.manifest.version, 4);
        assert_eq!(outcome.lease_token, None);
        assert!(!ctrl.has_namespace(&from));
        assert!(ctrl.manifest(&from).is_none());
        assert!(ctrl.has_namespace(&to));
        let moved = ctrl.manifest(&to).unwrap();
        assert_eq!(moved.segments[0].segment_id.0, "corpus-000001");
        assert_eq!(moved.segments[0].doc_count, 7);
        assert_eq!(ctrl.list_namespaces(), std::slice::from_ref(&to));
        // A bare namespace re-created later starts above the moved one.
        ctrl.create_namespace(from.clone()).unwrap();
        assert_eq!(ctrl.manifest(&from).unwrap().version, 5);
    }

    #[test]
    fn rename_refuses_a_collision_and_changes_nothing() {
        let mut ctrl = Controller::new();
        let (from, to) = (ns("corpus"), ns("acme/corpus"));
        ctrl.compare_and_swap_manifest(&from, 0, manifest_at(2, &[("a", 1)]))
            .unwrap();
        ctrl.create_namespace(to.clone()).unwrap();
        assert!(matches!(
            ctrl.rename_namespace(&from, &to, 2),
            Err(KoshaError::NamespaceExists(id)) if id == to
        ));
        assert_eq!(ctrl.manifest(&from).unwrap().version, 2);
        assert!(ctrl.manifest(&to).unwrap().segments.is_empty());
        assert!(matches!(
            ctrl.rename_namespace(&ns("missing"), &ns("acme/missing"), 0),
            Err(KoshaError::NamespaceNotFound(_))
        ));
        assert!(matches!(
            ctrl.rename_namespace(&from, &from, 2),
            Err(KoshaError::NamespaceExists(_))
        ));
    }

    #[test]
    fn rename_refuses_a_moved_version_and_changes_nothing() {
        let mut ctrl = Controller::new();
        let (from, to) = (ns("corpus"), ns("acme/corpus"));
        ctrl.compare_and_swap_manifest(&from, 0, manifest_at(3, &[("a", 1)]))
            .unwrap();
        assert!(matches!(
            ctrl.rename_namespace(&from, &to, 2),
            Err(KoshaError::ManifestConflict {
                expected: 2,
                actual: Some(3),
                ..
            })
        ));
        assert!(ctrl.has_namespace(&from));
        assert!(!ctrl.has_namespace(&to));
    }

    #[test]
    fn rename_carries_the_lease_token_and_fences_the_old_id() {
        let mut ctrl = Controller::new();
        let (from, to) = (ns("corpus"), ns("acme/corpus"));
        let ttl = Duration::from_secs(60);
        ctrl.compare_and_swap_manifest(&from, 0, manifest_at(1, &[("a", 1)]))
            .unwrap();
        // Bump `from`'s token to 3 through two takeovers.
        let first = ctrl.acquire_writer_lease(&from, "w1", ttl).unwrap();
        ctrl.release_writer_lease(&from, "w1", first);
        let second = ctrl.acquire_writer_lease(&from, "w2", ttl).unwrap();
        ctrl.release_writer_lease(&from, "w2", second);
        let token = ctrl.acquire_writer_lease(&from, "migrator", ttl).unwrap();
        assert_eq!(token, 3);
        // `to` once had a lease row (a deleted namespace) at a lower token.
        assert_eq!(ctrl.acquire_writer_lease(&to, "migrator", ttl), Some(1));

        let outcome = ctrl.rename_namespace(&from, &to, 1).unwrap();
        assert_eq!(outcome.lease_token, Some(3));
        assert_eq!(ctrl.writer_lease_token(&to), Some(3));
        // The holder keeps holding `to` under the carried token.
        assert!(ctrl.renew_writer_lease(&to, "migrator", 3, ttl));
        assert_eq!(ctrl.acquire_writer_lease(&to, "someone-else", ttl), None);
        // The old id's lease is bumped and expired: its holder is fenced.
        assert!(!ctrl.renew_writer_lease(&from, "migrator", 3, ttl));
        assert_eq!(ctrl.writer_lease_token(&from), Some(4));
        assert!(matches!(
            ctrl.compare_and_swap_manifest_fenced(&from, 0, manifest_at(9, &[]), Some(3)),
            Err(KoshaError::Fenced { .. })
        ));

        // `to`'s token never goes backwards.
        let mut ctrl = Controller::new();
        ctrl.compare_and_swap_manifest(&from, 0, manifest_at(1, &[]))
            .unwrap();
        assert_eq!(ctrl.acquire_writer_lease(&from, "m", ttl), Some(1));
        for holder in ["a", "b", "m"] {
            let token = ctrl.acquire_writer_lease(&to, holder, ttl).unwrap();
            ctrl.release_writer_lease(&to, holder, token);
        }
        assert_eq!(ctrl.writer_lease_token(&to), Some(3));
        let outcome = ctrl.rename_namespace(&from, &to, 1).unwrap();
        assert_eq!(outcome.lease_token, Some(3));
    }

    #[test]
    fn rename_moves_above_a_retired_target_and_rekeys_references() {
        let mut ctrl = Controller::new();
        let (from, to) = (ns("corpus"), ns("acme/corpus"));
        // `acme/corpus` existed once and was deleted at version 6.
        ctrl.compare_and_swap_manifest(&to, 0, manifest_at(6, &[]))
            .unwrap();
        ctrl.delete_namespace(&to, 0).unwrap();

        let mut manifest = manifest_at(2, &[("own", 1), ("inherited", 1)]);
        manifest.branch = Some(kosha_core::branch::BranchInfo {
            parent: ns("parent"),
            parent_version: 1,
            segment_homes: [(SegmentId("inherited".into()), ns("parent"))]
                .into_iter()
                .collect(),
        });
        manifest.deletion_vectors.insert(
            SegmentId("own".into()),
            kosha_core::deletes::DeletionVector {
                generation: "0".repeat(64),
                count: 1,
                namespace: Some(from.clone()),
            },
        );
        ctrl.compare_and_swap_manifest(&from, 0, manifest).unwrap();
        let outcome = ctrl.rename_namespace(&from, &to, 2).unwrap();
        assert_eq!(outcome.manifest.version, 7);
        let moved = ctrl.manifest(&to).unwrap();
        assert_eq!(moved.version, 7);
        // Another namespace's references are kept; its own are re-keyed.
        let branch = moved.branch.as_ref().unwrap();
        assert_eq!(branch.parent, ns("parent"));
        assert_eq!(
            moved.segment_home(&to, &SegmentId("inherited".into())),
            &ns("parent")
        );
        assert_eq!(moved.segment_home(&to, &SegmentId("own".into())), &to);
        assert_eq!(
            moved.deletion_vectors[&SegmentId("own".into())].namespace,
            Some(to.clone())
        );
    }
}
