//! Namespace branching endpoints (#208):
//!
//! - `POST   /v1/namespaces/{ns}/branch`   `{"name": "<branch>"}` creates
//!   `<branch>` as a copy-on-write branch of `{ns}`.
//! - `GET    /v1/namespaces/{ns}/branches` lists `{ns}`'s branches.
//! - `DELETE /v1/namespaces/{ns}/branch`   deletes `{ns}`, which must be a
//!   branch, and reclaims only the objects no remaining manifest references.
//!
//! The manifest model is in [`kosha_core::branch`]. Creating a branch writes
//! one manifest row and moves no segment bytes: the branch lists its
//! parent's segments and deletion bitmaps where they are. Branch creation
//! and deletion are serialized on this (single) ingest process by
//! `AppState::branch_lock`, so a deletion's reference set cannot miss a
//! branch being created from the namespace it deletes.

use super::*;
use kosha_core::branch::References;
use kosha_core::{KoshaError, Manifest};
use std::path::Path;

/// What a branch deletion reclaimed.
#[derive(Debug, Default, serde::Serialize)]
pub(crate) struct Reclaimed {
    /// Local files removed under the namespace's own data-dir prefix.
    pub local_files: usize,
    /// S3 objects deleted under the namespace's own prefix.
    pub remote_objects: usize,
    /// Objects kept because another namespace (a branch of this branch)
    /// still references them.
    pub kept: usize,
    /// Objects that should have been reclaimed but could not be. They are
    /// unreferenced, so leaving them is safe; they only cost storage.
    pub errors: Vec<String>,
}

impl Reclaimed {
    fn absorb(&mut self, other: Reclaimed) {
        self.local_files += other.local_files;
        self.remote_objects += other.remote_objects;
        self.kept += other.kept;
        self.errors.extend(other.errors);
    }
}

impl AppState {
    /// Resolve every deletion bitmap `manifest` (of `ns`) references into
    /// its in-memory tombstones, reading each where it lives.
    pub(crate) fn hydrate_deletes(
        &self,
        ns: &NamespaceId,
        manifest: &mut Manifest,
    ) -> Result<(), KoshaError> {
        kosha_core::deletes::hydrate(manifest, |segment, vector| {
            let path = vector.path(ns, segment)?;
            #[cfg(feature = "s3")]
            if let Some(ref s3) = self.s3_storage {
                return s3.read(&path);
            }
            Ok(std::fs::read(self.data_dir.join(path))?)
        })
    }

    /// Create `child` as a branch of `parent`. `Err` is a ready-to-send
    /// JSON error response.
    pub(crate) fn branch_namespace(
        &self,
        parent: &NamespaceId,
        child: &NamespaceId,
    ) -> Result<Manifest, String> {
        let _lineage = self.branch_lock.lock().unwrap();
        // The branch starts from everything the parent has acknowledged.
        flush_and_publish_namespace(self, parent)?;
        let source = self
            .controller
            .lock()
            .unwrap()
            .try_manifest_cloned(parent)
            .map_err(|e| json_error(503, &format!("control store: {e}")))?;
        let Some(source) = source else {
            return Err(json_error(
                404,
                &format!("namespace '{}' not found", short_name(parent)),
            ));
        };
        // A sharded root (#207) lists no segments of its own; branching it
        // would need every shard branched with it, which is not supported.
        if source.shards.is_some_and(|n| n > 1) {
            return Err(json_error(
                400,
                &format!(
                    "namespace '{}' is sharded; sharded namespaces cannot be branched yet",
                    short_name(parent)
                ),
            ));
        }
        let child_exists = self
            .controller
            .lock()
            .unwrap()
            .manifest_version(child)
            .map_err(|e| json_error(503, &format!("control store: {e}")))?
            .is_some()
            || self.indexer.manifest_version(child).is_some()
            || self.indexer.pending_doc_count(child) > 0;
        if child_exists {
            return Err(json_error(
                409,
                &format!("namespace '{}' already exists", short_name(child)),
            ));
        }

        let mut stored = source.branch_from(parent, 1);
        let committed =
            self.controller
                .lock()
                .unwrap()
                .compare_and_swap_manifest(child, 0, stored.clone());
        match committed {
            Ok(()) => {}
            // A name a deleted namespace used starts its next incarnation
            // above the version that one retired at (#305), so the
            // bootstrap from 0 lands on an empty manifest at that version:
            // branch from there instead.
            Err(KoshaError::ManifestConflict {
                actual: Some(at), ..
            }) => {
                let fresh = self
                    .controller
                    .lock()
                    .unwrap()
                    .try_manifest_cloned(child)
                    .map_err(|e| json_error(503, &format!("control store: {e}")))?;
                if !fresh
                    .is_some_and(|m| m.version == at && m.segments.is_empty() && m.branch.is_none())
                {
                    return Err(json_error(
                        409,
                        &format!("namespace '{}' already exists", short_name(child)),
                    ));
                }
                stored = source.branch_from(parent, at + 1);
                self.controller
                    .lock()
                    .unwrap()
                    .compare_and_swap_manifest(child, at, stored.clone())
                    .map_err(|e| publication_error("branch commit failed", &e))?;
            }
            Err(e) => return Err(publication_error("branch commit failed", &e)),
        }

        // The writer needs the deletes in memory. This process normally
        // holds the parent at the version branched from, deletes and parsed
        // footers included, so the bitmaps need not be read back. Each one
        // is still checked against the stored generation (`hydrate`
        // verifies the digest): a delete applied to the parent after the
        // publish above is not part of the branch, and any mismatch falls
        // back to reading the stored bitmaps.
        let local = self
            .indexer
            .manifest_shared(parent)
            .filter(|local| local.version == source.version);
        let from_memory = local.as_ref().and_then(|local| {
            let mut manifest = stored.clone();
            kosha_core::deletes::hydrate(&mut manifest, |segment, _| {
                let ids = local.tombstones.get(segment).cloned().unwrap_or_default();
                Ok(kosha_core::deletes::DeletionVector::encode(&ids)?.1)
            })
            .ok()?;
            manifest.carry_segment_footers_from(local);
            Some(manifest)
        });
        let hydrated = match from_memory {
            Some(manifest) => manifest,
            None => {
                let mut manifest = stored.clone();
                self.hydrate_deletes(child, &mut manifest)
                    .map_err(|e| json_error(500, &format!("branch deletes unreadable: {e}")))?;
                manifest
            }
        };
        self.indexer.restore_manifest(child.clone(), hydrated);
        println!(
            "namespace branched: parent={} version={} branch={} segments={}",
            parent.0,
            source.version,
            child.0,
            stored.segments.len()
        );
        Ok(stored)
    }

    /// Delete the branch `ns` and reclaim its unreferenced objects. `Err`
    /// is a ready-to-send JSON error response.
    #[cfg(test)]
    pub(crate) fn delete_branch(&self, ns: &NamespaceId) -> Result<(Manifest, Reclaimed), String> {
        self.delete_branch_recorded(ns, None)
            .map(|(manifest, reclaimed, _)| (manifest, reclaimed))
    }

    /// [`Self::delete_branch`] with its deletion record (#347): written,
    /// pending, in the control-store step that drops the branch, and
    /// completed once every object of its prefix that no remaining manifest
    /// references is gone (objects a branch of this branch still lists stay
    /// by design, and go with that branch). `parent` is the tenant deletion
    /// this is part of.
    pub(crate) fn delete_branch_recorded(
        &self,
        ns: &NamespaceId,
        parent: Option<&str>,
    ) -> Result<(Manifest, Reclaimed, kosha_core::deletion::DeletionRecord), String> {
        let _lineage = self.branch_lock.lock().unwrap();
        let stored = self
            .controller
            .lock()
            .unwrap()
            .try_manifest_cloned(ns)
            .map_err(|e| json_error(503, &format!("control store: {e}")))?;
        let Some(stored) = stored else {
            return Err(json_error(
                404,
                &format!("namespace '{}' not found", short_name(ns)),
            ));
        };
        if stored.branch.is_none() {
            return Err(json_error(
                409,
                &format!(
                    "namespace '{}' is not a branch; only branches can be deleted here",
                    short_name(ns)
                ),
            ));
        }
        // Publish whatever was written to the branch, so the writer holds
        // nothing a restart could replay into a resurrected namespace.
        flush_and_publish_namespace(self, ns)?;
        let lock = self.publication_lock(ns);
        let _publication = lock.lock().unwrap();
        let request =
            kosha_core::deletion::NewDeletion::branch(&ns.0, namespace_tenant(&ns.0), parent)
                .with_acks(self.deletion_requires_acks());
        let mut outcome = None;
        self.indexer
            .drop_branch_namespace(ns, || {
                let mut controller = self.controller.lock().unwrap();
                // Every other manifest's references, read before the row
                // goes. Fallible on purpose: a reference set missing a
                // manifest would reclaim objects that manifest still lists.
                let mut refs = References::default();
                let mut live = std::collections::HashSet::new();
                for (other, _) in controller.manifest_versions()? {
                    if &other == ns {
                        continue;
                    }
                    if let Some(manifest) = controller.try_manifest_cloned(&other)? {
                        refs.add(&other, &manifest)?;
                    }
                    live.insert(other);
                }
                // `delete_namespace`, not a bare removal: the store keeps the
                // version the branch retired at, so a namespace later
                // created under the same name starts above it and query
                // replicas' version-keyed caches never serve the deleted
                // branch (#305).
                // With its deletion record, in one step (#347).
                let (removed, record) =
                    match controller.delete_namespace_recorded(ns, stored.version, &request) {
                        Ok((removed, _, record)) => (removed, record),
                        Err(KoshaError::NamespaceNotFound(_)) => (
                            stored.clone(),
                            controller.begin_deletion(&request, stored.version)?,
                        ),
                        Err(e) => return Err(e),
                    };
                drop(controller);
                // Prefixes to reclaim: this namespace's own, and that of
                // every already-deleted branch whose objects it was keeping
                // alive (a deleted middle branch's segments that only this
                // descendant still listed). A live namespace's prefix is
                // never swept from here.
                let mut prefixes = vec![ns.clone()];
                let segment_homes = removed
                    .branch
                    .iter()
                    .flat_map(|branch| branch.segment_homes.values());
                let bitmap_homes = removed
                    .deletion_vectors
                    .values()
                    .filter_map(|vector| vector.namespace.as_ref());
                for home in segment_homes.chain(bitmap_homes) {
                    if home != ns && !live.contains(home) && !prefixes.contains(home) {
                        prefixes.push(home.clone());
                    }
                }
                let mut reclaimed = Reclaimed::default();
                for prefix in &prefixes {
                    reclaimed.absorb(self.reclaim_namespace_objects(prefix, &refs));
                }
                outcome = Some((removed, reclaimed, record));
                Ok(())
            })
            .map_err(|e| match e {
                KoshaError::InvalidQuery(message) => json_error(409, &message),
                KoshaError::ControlPlane(message) => {
                    json_error(503, &format!("control store: {message}"))
                }
                other => json_error(500, &format!("branch deletion failed: {other}")),
            })?;
        let (removed, mut reclaimed, record) = outcome.expect("drop_namespace ran the removal");
        // Parsed segments, corpus statistics and ceilings go now on this
        // pod (#347); other pods purge theirs from the record.
        self.forget_namespace_caches(ns);
        self.purge_deleted_from_memory(&ns.0);
        let exports = match self.delete_namespace_exports(ns) {
            Ok(count) => count,
            Err(e) => {
                reclaimed.errors.push(format!("exports: {e}"));
                0
            }
        };
        if exports > 0 {
            let _ = self
                .controller
                .lock()
                .unwrap()
                .note_deletion_exports(&record.id, exports);
        }
        let error = (!reclaimed.errors.is_empty()).then(|| reclaimed.errors.join("; "));
        let record =
            self.note_deletion_attempt(&record, reclaimed.remote_objects as u64, error.as_deref());
        println!(
            "namespace branch deleted: ns={} version={} local_files={} remote_objects={} kept={} errors={} deletion={} state={}",
            ns.0,
            removed.version,
            reclaimed.local_files,
            reclaimed.remote_objects,
            reclaimed.kept,
            reclaimed.errors.len(),
            record.id,
            record.state.as_str()
        );
        Ok((removed, reclaimed, record))
    }

    /// Retry the reclaim of a pending branch deletion (#347): everything
    /// under the branch's own prefix that no manifest references. Refused,
    /// with the reason on the record, while a namespace of that name exists
    /// again.
    pub(crate) fn resume_branch_deletion(
        &self,
        record: &kosha_core::deletion::DeletionRecord,
    ) -> kosha_core::deletion::DeletionRecord {
        let ns = NamespaceId(record.subject.clone());
        let _lineage = self.branch_lock.lock().unwrap();
        let lock = self.publication_lock(&ns);
        let _publication = lock.lock().unwrap();
        let refs = {
            let controller = self.controller.lock().unwrap();
            let collect = || -> Result<Option<References>, KoshaError> {
                let mut refs = References::default();
                for (other, _) in controller.manifest_versions()? {
                    if other == ns {
                        return Ok(None);
                    }
                    if let Some(manifest) = controller.try_manifest_cloned(&other)? {
                        refs.add(&other, &manifest)?;
                    }
                }
                Ok(Some(refs))
            };
            collect()
        };
        let refs = match refs {
            Ok(Some(refs)) => refs,
            Ok(None) => {
                return self.note_deletion_attempt(
                    record,
                    0,
                    Some(
                        "a namespace of this name exists again; its objects share the prefix, \
                         so the deleted branch's leftovers stay until it is deleted too",
                    ),
                )
            }
            Err(e) => {
                return self.note_deletion_attempt(record, 0, Some(&format!("control store: {e}")))
            }
        };
        if self.indexer.manifest_version(&ns).is_some() {
            return self.note_deletion_attempt(
                record,
                0,
                Some("this writer holds a namespace of this name again"),
            );
        }
        let mut reclaimed = self.reclaim_namespace_objects(&ns, &refs);
        if let Err(e) = self.delete_namespace_exports(&ns) {
            reclaimed.errors.push(format!("exports: {e}"));
        }
        let error = (!reclaimed.errors.is_empty()).then(|| reclaimed.errors.join("; "));
        self.note_deletion_attempt(record, reclaimed.remote_objects as u64, error.as_deref())
    }

    /// Delete everything under `ns`'s own prefix (local data dir and S3)
    /// that `refs` does not hold. Never looks outside `{ns}/`, so a
    /// parent's (or any other namespace's) objects cannot be touched, and
    /// within it keeps whatever a remaining branch still references.
    pub(crate) fn reclaim_namespace_objects(
        &self,
        ns: &NamespaceId,
        refs: &References,
    ) -> Reclaimed {
        let mut out = Reclaimed::default();
        let ns_dir = self.data_dir.join(&ns.0);
        if let Ok(entries) = std::fs::read_dir(&ns_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                let rel = format!("{}/{}", ns.0, name);
                let path = entry.path();
                if !path.is_dir() {
                    if refs.holds_file(&rel) {
                        out.kept += 1;
                    } else if let Err(e) = std::fs::remove_file(&path) {
                        out.errors.push(format!("{rel}: {e}"));
                    } else {
                        out.local_files += 1;
                    }
                    continue;
                }
                if !refs.holds_anything_under(&rel) {
                    let files = count_files(&path);
                    match std::fs::remove_dir_all(&path) {
                        Ok(()) => out.local_files += files,
                        Err(e) => out.errors.push(format!("{rel}/: {e}")),
                    }
                    continue;
                }
                let names: Vec<String> = std::fs::read_dir(&path)
                    .map(|it| {
                        it.flatten()
                            .filter(|e| e.path().is_file())
                            .map(|e| e.file_name().to_string_lossy().into_owned())
                            .collect()
                    })
                    .unwrap_or_default();
                let doomed = refs.reclaimable_files(&rel, &names);
                out.kept += names.len() - doomed.len();
                for file in doomed {
                    match std::fs::remove_file(self.data_dir.join(&file)) {
                        Ok(()) => out.local_files += 1,
                        Err(e) => out.errors.push(format!("{file}: {e}")),
                    }
                }
            }
            // Empty now unless a remaining branch still references part of it.
            let _ = std::fs::remove_dir(&ns_dir);
        }
        #[cfg(feature = "s3")]
        if let Some(ref s3) = self.s3_storage {
            match s3.list_child_dirs(&ns.0) {
                Ok(dirs) => {
                    for dir in dirs {
                        let rel = format!("{}/{}", ns.0, dir);
                        let files = match s3.list_remote_with_sizes(&rel) {
                            Ok(files) => files.into_iter().map(|(name, _)| name).collect(),
                            Err(e) => {
                                out.errors.push(format!("list {rel}/: {e}"));
                                continue;
                            }
                        };
                        let files: Vec<String> = files;
                        let doomed = refs.reclaimable_files(&rel, &files);
                        out.kept += files.len() - doomed.len();
                        // Deleting `_COMMIT` first would leave a prefix that
                        // looks uncommitted, never one that looks complete
                        // while files are missing.
                        let mut doomed = doomed;
                        doomed.sort_by_key(|path| !path.ends_with(segment_commit::COMMIT_FILE));
                        for path in doomed {
                            match s3.delete_remote(&path) {
                                Ok(()) => out.remote_objects += 1,
                                Err(e) => out.errors.push(e.to_string()),
                            }
                        }
                    }
                }
                Err(e) => out.errors.push(format!("list {}/: {e}", ns.0)),
            }
        }
        out
    }

    /// Every namespace whose manifest says it was branched from `parent`.
    pub(crate) fn list_branches(
        &self,
        parent: &NamespaceId,
    ) -> Result<Vec<(NamespaceId, Manifest)>, KoshaError> {
        let controller = self.controller.lock().unwrap();
        let tenant_prefix = parent
            .0
            .split_once('/')
            .map(|(tenant, _)| format!("{tenant}/"))
            .unwrap_or_default();
        let mut out = Vec::new();
        for (ns, _) in controller.manifest_versions()? {
            if !ns.0.starts_with(&tenant_prefix) || &ns == parent {
                continue;
            }
            if let Some(manifest) = controller.try_manifest_cloned(&ns)? {
                if manifest
                    .branch
                    .as_ref()
                    .is_some_and(|branch| &branch.parent == parent)
                {
                    out.push((ns, manifest));
                }
            }
        }
        out.sort_by(|a, b| a.0 .0.cmp(&b.0 .0));
        Ok(out)
    }
}

fn count_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| {
                    let path = entry.path();
                    if path.is_dir() {
                        count_files(&path)
                    } else {
                        1
                    }
                })
                .sum()
        })
        .unwrap_or(0)
}

/// The namespace name as the tenant sees it (without the tenant prefix).
fn short_name(ns: &NamespaceId) -> &str {
    ns.0.split_once('/').map_or(ns.0.as_str(), |(_, name)| name)
}

fn branch_summary(ns: &NamespaceId, manifest: &Manifest) -> serde_json::Value {
    let shared = manifest
        .segments
        .iter()
        .filter(|entry| manifest.is_inherited_segment(ns, &entry.segment_id))
        .count();
    let documents: u64 = manifest.segments.iter().map(|e| e.doc_count as u64).sum();
    let branch = manifest.branch.as_ref();
    serde_json::json!({
        "name": short_name(ns),
        "parent": branch.map(|b| short_name(&b.parent).to_string()),
        "parent_version": branch.map(|b| b.parent_version),
        "version": manifest.version,
        "segments": manifest.segments.len(),
        "shared_segments": shared,
        "documents": documents,
    })
}

#[derive(serde::Deserialize)]
struct BranchRequest {
    name: String,
}

/// `POST /v1/namespaces/{ns}/branch`
pub(crate) fn handle_branch_create(
    namespace: &str,
    tenant: &str,
    body: &[u8],
    state: &AppState,
) -> String {
    let request: BranchRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => {
            return json_error(
                400,
                &format!("invalid JSON (expected {{\"name\": ...}}): {e}"),
            )
        }
    };
    if !valid_namespace_component(&request.name) {
        return json_error(400, "invalid branch name");
    }
    if request.name == namespace {
        return json_error(400, "a branch needs a name different from its parent");
    }
    let parent = NamespaceId(tenant_namespace(tenant, namespace));
    let child = NamespaceId(tenant_namespace(tenant, &request.name));
    // A branch is a new top-level namespace of the tenant: it counts
    // against the namespace quota like any other (#341).
    if let Err(rejection) = check_namespace_quota(state, &child) {
        return rejection;
    }
    match state.branch_namespace(&parent, &child) {
        Ok(manifest) => json_ok(&branch_summary(&child, &manifest)),
        Err(response) => response,
    }
}

/// `GET /v1/namespaces/{ns}/branches`
pub(crate) fn handle_branch_list(namespace: &str, tenant: &str, state: &AppState) -> String {
    let parent = NamespaceId(tenant_namespace(tenant, namespace));
    match state.list_branches(&parent) {
        Ok(branches) => json_ok(&serde_json::json!({
            "namespace": namespace,
            "branches": branches
                .iter()
                .map(|(ns, manifest)| branch_summary(ns, manifest))
                .collect::<Vec<_>>(),
        })),
        Err(e) => json_error(503, &format!("control store: {e}")),
    }
}

/// `DELETE /v1/namespaces/{ns}/branch`
pub(crate) fn handle_branch_delete(namespace: &str, tenant: &str, state: &AppState) -> String {
    let ns = NamespaceId(tenant_namespace(tenant, namespace));
    match state.delete_branch_recorded(&ns, None) {
        Ok((manifest, reclaimed, record)) => json_ok(&serde_json::json!({
            "deleted": namespace,
            "parent": manifest.branch.as_ref().map(|b| short_name(&b.parent).to_string()),
            "version": manifest.version,
            "reclaimed": reclaimed,
            "deletion": record,
        })),
        Err(response) => response,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn test_state() -> AppState {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kosha-test-branch-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        AppState::new(dir)
    }

    fn call(state: &AppState, line: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
        let resp = route(
            &format!("{line} HTTP/1.1\r\n"),
            &HashMap::new(),
            &serde_json::to_vec(&body).unwrap(),
            "acme",
            state,
        );
        let status: u16 = resp[9..12].parse().unwrap();
        let body = &resp[resp.find("\r\n\r\n").unwrap() + 4..];
        (status, serde_json::from_str(body).unwrap())
    }

    fn index(state: &AppState, ns: &str, docs: &[(&str, &str)]) {
        let documents: Vec<_> = docs
            .iter()
            .map(|(id, text)| {
                serde_json::json!({"id": id, "fields": [
                    {"name": "body", "field_type": "Text", "value": format!("common {text}")},
                    {"name": "tag", "field_type": "Keyword", "value": id}]})
            })
            .collect();
        let (status, body) = call(
            state,
            &format!("POST /v1/namespaces/{ns}/documents"),
            serde_json::json!({ "documents": documents }),
        );
        assert_eq!(status, 200, "{body}");
        let (status, body) = call(
            state,
            &format!("POST /v1/namespaces/{ns}/flush"),
            serde_json::json!({}),
        );
        assert_eq!(status, 200, "{body}");
    }

    fn delete(state: &AppState, ns: &str, id: &str) {
        let (status, body) = call(
            state,
            &format!("POST /v1/namespaces/{ns}/delete"),
            serde_json::json!({"filter": {"term": {"tag": id}}}),
        );
        assert_eq!(status, 200, "{body}");
        let (status, body) = call(
            state,
            &format!("POST /v1/namespaces/{ns}/flush"),
            serde_json::json!({}),
        );
        assert_eq!(status, 200, "{body}");
    }

    /// The full `results` array, scores included, serialized: what a
    /// client sees.
    fn search(state: &AppState, ns: &str, text: &str) -> String {
        let (status, body) = call(
            state,
            &format!("POST /v1/namespaces/{ns}/search"),
            serde_json::json!({"query_text": text, "top_k": 50, "no_cache": true}),
        );
        assert_eq!(status, 200, "{body}");
        serde_json::to_string(&body["results"]).unwrap()
    }

    fn ids(results: &str) -> Vec<String> {
        let value: serde_json::Value = serde_json::from_str(results).unwrap();
        let mut ids: Vec<String> = value
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["doc_id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        ids
    }

    /// Every file under `root` except the WAL, with its bytes.
    fn tree(root: &Path) -> BTreeMap<String, Vec<u8>> {
        let mut out = BTreeMap::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                let path = entry.path();
                let rel = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                if rel.starts_with("_wal") || rel.contains("/_wal") {
                    continue;
                }
                if path.is_dir() {
                    stack.push(path);
                } else {
                    out.insert(rel, std::fs::read(&path).unwrap());
                }
            }
        }
        out
    }

    fn prefix(tree: &BTreeMap<String, Vec<u8>>, ns: &str) -> BTreeMap<String, Vec<u8>> {
        tree.iter()
            .filter(|(path, _)| path.starts_with(&format!("{ns}/")))
            .map(|(p, b)| (p.clone(), b.clone()))
            .collect()
    }

    /// Every test document contains this word.
    const ALL: &str = "common";

    /// Segment GC (#239) and namespace deletion (#305) both delete objects
    /// under a namespace's prefix; neither may take one a branch still
    /// lists. And a branch name a deleted namespace used starts above the
    /// version that one retired at.
    #[test]
    fn gc_and_namespace_deletion_keep_what_a_branch_references() {
        let state = test_state();
        index(&state, "prod", &[("a", "alpha"), ("b", "beta")]);
        index(&state, "prod", &[("c", "gamma")]);
        let (status, body) = call(
            &state,
            "POST /v1/namespaces/prod/branch",
            serde_json::json!({"name": "test"}),
        );
        assert_eq!(status, 200, "{body}");
        let before = search(&state, "test", ALL);
        assert_eq!(ids(&before), ["a", "b", "c"]);

        // The parent compacts: its two inputs are retired from its own
        // manifest but the branch still lists them.
        let prod = NamespaceId("acme/prod".into());
        compaction_pass(&state, &prod, kosha_write::CompactOptions::full(), "test").unwrap();
        let config = kosha_core::gc::GcConfig {
            dry_run: false,
            grace_ms: 0,
            orphan_grace_ms: 0,
            now_ms: u64::MAX / 2,
            referenced_elsewhere: None,
        };
        let reports = state.run_segment_gc(std::slice::from_ref(&prod), &config);
        assert!(reports[0].aborted.is_none(), "{:?}", reports[0].aborted);
        assert_eq!(
            reports[0].segments_deleted, 0,
            "{:?}",
            reports[0].candidates
        );
        assert!(!reports[0].skipped.is_empty());
        assert_eq!(search(&state, "test", ALL), before);

        let (status, body) = call(&state, "DELETE /v1/namespaces/prod", serde_json::json!({}));
        assert_eq!(status, 409, "{body}");
        assert_eq!(search(&state, "test", ALL), before);
        // A branch is deleted through its own route, which reclaims what
        // only it kept alive.
        let (status, body) = call(&state, "DELETE /v1/namespaces/test", serde_json::json!({}));
        assert_eq!(status, 409, "{body}");
        assert!(body.to_string().contains("/branch"), "{body}");
        assert_eq!(search(&state, "test", ALL), before);

        // Once the branch is gone the parent can go, and the name is free
        // for a new branch, whose versions start above the deleted one's.
        let (status, body) = call(
            &state,
            "DELETE /v1/namespaces/test/branch",
            serde_json::json!({}),
        );
        assert_eq!(status, 200, "{body}");
        let (status, body) = call(&state, "DELETE /v1/namespaces/prod", serde_json::json!({}));
        assert_eq!(status, 200, "{body}");
        index(&state, "prod", &[("d", "delta")]);
        let (status, body) = call(
            &state,
            "POST /v1/namespaces/prod/branch",
            serde_json::json!({"name": "test"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(ids(&search(&state, "test", ALL)), ["d"]);
    }

    /// Local GC (#239) must not remove a segment directory or deletion
    /// generation the parent retired while a branch still lists it: on a
    /// node without an object store the local copy is the only one.
    #[test]
    fn local_gc_keeps_what_a_branch_references() {
        let state = test_state();
        index(&state, "prod", &[("a", "alpha"), ("b", "beta")]);
        index(&state, "prod", &[("c", "gamma")]);
        delete(&state, "prod", "b");
        let (status, body) = call(
            &state,
            "POST /v1/namespaces/prod/branch",
            serde_json::json!({"name": "test"}),
        );
        assert_eq!(status, 200, "{body}");
        let before = search(&state, "test", ALL);
        assert_eq!(ids(&before), ["a", "c"]);

        let prod = NamespaceId("acme/prod".into());
        let inherited: Vec<_> = state
            .indexer
            .manifest(&prod)
            .unwrap()
            .segments
            .iter()
            .map(|e| state.data_dir.join(&prod.0).join(&e.segment_id.0))
            .collect();
        compaction_pass(&state, &prod, kosha_write::CompactOptions::full(), "test").unwrap();
        let report = state.run_local_gc(Duration::ZERO);
        assert_eq!(report.segments_removed, 0, "{report:?}");
        assert_eq!(report.deletion_vectors_removed, 0, "{report:?}");
        for dir in &inherited {
            assert!(dir.exists(), "{} removed under the branch", dir.display());
        }
        assert_eq!(search(&state, "test", ALL), before);
        // The branch can still compact the inputs it inherited.
        let test_ns = NamespaceId("acme/test".into());
        compaction_pass(
            &state,
            &test_ns,
            kosha_write::CompactOptions::full(),
            "test",
        )
        .unwrap();
        assert_eq!(ids(&search(&state, "test", ALL)), ["a", "c"]);

        // With the branch gone, the parent's retired objects are freed.
        let (status, body) = call(
            &state,
            "DELETE /v1/namespaces/test/branch",
            serde_json::json!({}),
        );
        assert_eq!(status, 200, "{body}");
        state.run_local_gc(Duration::ZERO);
        for dir in &inherited {
            assert!(
                !dir.exists(),
                "{} kept after the branch went",
                dir.display()
            );
        }
        assert_eq!(ids(&search(&state, "prod", ALL)), ["a", "c"]);
    }

    /// Object-store GC's reference set must not come from a namespace
    /// listing that can fail silently (`PgStore::list_namespaces` reads a
    /// failed query as no namespaces): it would hold no branch, and the
    /// parent's GC would delete what the branch still lists.
    #[test]
    fn gc_keeps_branch_references_when_the_namespace_listing_fails() {
        let state = test_state();
        let store = crate::tests::outage_store();
        *state.controller.lock().unwrap() = Box::new(store.clone());
        index(&state, "prod", &[("a", "alpha"), ("b", "beta")]);
        index(&state, "prod", &[("c", "gamma")]);
        let (status, body) = call(
            &state,
            "POST /v1/namespaces/prod/branch",
            serde_json::json!({"name": "test"}),
        );
        assert_eq!(status, 200, "{body}");
        let before = search(&state, "test", ALL);
        let prod = NamespaceId("acme/prod".into());
        compaction_pass(&state, &prod, kosha_write::CompactOptions::full(), "test").unwrap();

        store
            .list_fails
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let config = kosha_core::gc::GcConfig {
            dry_run: false,
            grace_ms: 0,
            orphan_grace_ms: 0,
            now_ms: u64::MAX / 2,
            referenced_elsewhere: None,
        };
        let reports = state.run_segment_gc(std::slice::from_ref(&prod), &config);
        assert_eq!(reports[0].segments_deleted, 0, "{:?}", reports[0]);
        store
            .list_fails
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(search(&state, "test", ALL), before);
    }

    #[test]
    fn branching_moves_no_bytes_and_branch_writes_leave_the_parent_identical() {
        let state = test_state();
        index(&state, "prod", &[("a", "alpha beta"), ("b", "beta gamma")]);
        index(
            &state,
            "prod",
            &[("c", "gamma delta"), ("d", "delta alpha")],
        );
        delete(&state, "prod", "b");
        let parent_results = search(&state, "prod", ALL);
        assert_eq!(ids(&parent_results), ["a", "c", "d"]);
        let before = tree(&state.data_dir);

        let (status, body) = call(
            &state,
            "POST /v1/namespaces/prod/branch",
            serde_json::json!({"name": "test"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["name"], "test");
        assert_eq!(body["parent"], "prod");
        assert_eq!(body["shared_segments"], body["segments"]);
        assert_eq!(body["segments"], 2);
        // Constant time: one manifest row; not a byte of segment data moved.
        assert_eq!(tree(&state.data_dir), before, "branching wrote files");
        assert_eq!(search(&state, "test", ALL), parent_results);

        // Duplicate names, missing parents and malformed names are refused.
        let (status, _) = call(
            &state,
            "POST /v1/namespaces/prod/branch",
            serde_json::json!({"name": "test"}),
        );
        assert_eq!(status, 409);
        let (status, _) = call(
            &state,
            "POST /v1/namespaces/nope/branch",
            serde_json::json!({"name": "x"}),
        );
        assert_eq!(status, 404);
        let (status, _) = call(
            &state,
            "POST /v1/namespaces/prod/branch",
            serde_json::json!({"name": "../x"}),
        );
        assert_eq!(status, 400);

        // Write, upsert, delete from an inherited segment, compact.
        index(
            &state,
            "test",
            &[("e", "alpha epsilon"), ("c", "gamma changed")],
        );
        delete(&state, "test", "a");
        let test_ns = NamespaceId("acme/test".into());
        compaction_pass(
            &state,
            &test_ns,
            kosha_write::CompactOptions::full(),
            "test",
        )
        .unwrap();
        assert_eq!(ids(&search(&state, "test", ALL)), ["c", "d", "e"]);
        let stored = state
            .controller
            .lock()
            .unwrap()
            .manifest_cloned(&test_ns)
            .unwrap();
        assert_eq!(stored.segments.len(), 1);
        assert!(!stored.is_inherited_segment(&test_ns, &stored.segments[0].segment_id));

        // The parent: byte-identical results and byte-identical files.
        assert_eq!(search(&state, "prod", ALL), parent_results);
        let after = tree(&state.data_dir);
        assert_eq!(prefix(&after, "acme/prod"), prefix(&before, "acme/prod"));
        assert!(!prefix(&after, "acme/test").is_empty());

        // Listing and stats.
        let (status, body) = call(
            &state,
            "GET /v1/namespaces/prod/branches",
            serde_json::json!({}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["branches"].as_array().unwrap().len(), 1);
        assert_eq!(body["branches"][0]["name"], "test");
        assert_eq!(body["branches"][0]["shared_segments"], 0);
        let (status, body) = call(
            &state,
            "GET /v1/namespaces/test/stats",
            serde_json::json!({}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["branch"]["parent"], "prod");
        let (_, body) = call(
            &state,
            "GET /v1/namespaces/prod/stats",
            serde_json::json!({}),
        );
        assert!(body.get("branch").is_none());

        // Only a branch can be deleted through the branch route.
        let (status, _) = call(
            &state,
            "DELETE /v1/namespaces/prod/branch",
            serde_json::json!({}),
        );
        assert_eq!(status, 409);
        let _ = std::fs::remove_dir_all(&state.data_dir);
    }

    /// Acceptance: branching a 1M-document namespace is metadata-only. The
    /// segments here exist only in the manifest (as if in S3 and not
    /// cached), so any attempt to read, copy or hydrate one would fail or
    /// show up as files; the cost is one manifest row either way.
    #[test]
    fn branching_a_million_document_namespace_moves_no_bytes() {
        let state = test_state();
        let prod = NamespaceId("acme/big".into());
        let mut manifest = Manifest {
            version: 1,
            segments: Vec::new(),
            segment_footers: Default::default(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            schema: None,
            branch: None,
            retired: Default::default(),
            shards: Default::default(),
        };
        for i in 0..40 {
            manifest.segments.push(kosha_core::ManifestEntry {
                segment_id: kosha_core::SegmentId(format!("acme_big-{i:06}")),
                doc_count: 25_000,
            });
        }
        state.import_manifest(&prod, manifest).unwrap();
        let before = tree(&state.data_dir);
        let started = Instant::now();
        let (status, body) = call(
            &state,
            "POST /v1/namespaces/big/branch",
            serde_json::json!({"name": "big-copy"}),
        );
        let elapsed = started.elapsed();
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["documents"], 1_000_000);
        assert_eq!(body["shared_segments"], 40);
        assert_eq!(tree(&state.data_dir), before, "no file was written");
        // Generous for CI; the work is O(segments) map inserts.
        assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
        let copy = NamespaceId("acme/big-copy".into());
        let stored = state
            .controller
            .lock()
            .unwrap()
            .manifest_cloned(&copy)
            .unwrap();
        for entry in &stored.segments {
            assert_eq!(
                stored.segment_rel_dir(&copy, &entry.segment_id),
                format!("acme/big/{}", entry.segment_id.0)
            );
        }
        let _ = std::fs::remove_dir_all(&state.data_dir);
    }

    /// The crux of #208: deleting a branch must not delete a segment its
    /// parent, or a branch of it, still lists.
    #[test]
    fn deleting_a_branch_keeps_every_object_another_manifest_references() {
        let state = test_state();
        index(&state, "prod", &[("a", "alpha beta"), ("b", "beta gamma")]);
        let parent_results = search(&state, "prod", ALL);
        let prod_ns = NamespaceId("acme/prod".into());
        let shared = state.indexer.manifest_cloned(&prod_ns).unwrap().segments[0]
            .segment_id
            .clone();

        let branch = |parent: &str, name: &str| {
            let (status, body) = call(
                &state,
                &format!("POST /v1/namespaces/{parent}/branch"),
                serde_json::json!({ "name": name }),
            );
            assert_eq!(status, 200, "{body}");
        };
        branch("prod", "test");
        // The branch writes its own segment and deletes from the shared one
        // (a bitmap in its own prefix, under the shared segment's id).
        index(&state, "test", &[("t", "alpha tango")]);
        delete(&state, "test", "b");
        branch("test", "grand");
        // Written after `grand` branched: referenced by nobody else.
        index(&state, "test", &[("u", "alpha uniform")]);
        let grand_results = search(&state, "grand", ALL);
        assert_eq!(ids(&grand_results), ["a", "t"]);
        let before = tree(&state.data_dir);
        let shared_dir = format!("acme/prod/{}", shared.0);
        assert!(before.keys().any(|p| p.starts_with(&shared_dir)));
        let test_ns = NamespaceId("acme/test".into());
        let test_manifest = state.indexer.manifest_cloned(&test_ns).unwrap();
        let own_only: Vec<String> = test_manifest
            .segments
            .iter()
            .filter(|e| !test_manifest.is_inherited_segment(&test_ns, &e.segment_id))
            .map(|e| format!("acme/test/{}/", e.segment_id.0))
            .collect();
        assert_eq!(own_only.len(), 2, "t and u");

        let (status, body) = call(
            &state,
            "DELETE /v1/namespaces/test/branch",
            serde_json::json!({}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["parent"], "prod");
        assert!(
            body["reclaimed"]["errors"].as_array().unwrap().is_empty(),
            "{body}"
        );
        assert!(body["reclaimed"]["kept"].as_u64().unwrap() > 0, "{body}");

        let after = tree(&state.data_dir);
        // The segment shared by all three: every byte intact.
        assert_eq!(prefix(&after, "acme/prod"), prefix(&before, "acme/prod"));
        // Still referenced by `grand`: `t` and the deletion bitmap.
        let grand_ns = NamespaceId("acme/grand".into());
        let grand = state
            .controller
            .lock()
            .unwrap()
            .manifest_cloned(&grand_ns)
            .unwrap();
        let (dirs, files) = grand.referenced_objects(&grand_ns).unwrap();
        for dir in dirs.iter().filter(|d| d.starts_with("acme/test/")) {
            assert!(
                after.keys().any(|p| p.starts_with(&format!("{dir}/"))),
                "{dir} reclaimed"
            );
        }
        for file in files.iter().filter(|f| f.starts_with("acme/test/")) {
            assert!(after.contains_key(file), "{file} reclaimed");
        }
        // Referenced by nobody: `u` is gone.
        let u_dir = own_only
            .iter()
            .find(|d| !dirs.iter().any(|g| format!("{g}/") == **d))
            .unwrap();
        assert!(
            !after.keys().any(|p| p.starts_with(u_dir.as_str())),
            "{u_dir} kept"
        );

        // Everyone still answers exactly as before; the branch is gone.
        assert_eq!(search(&state, "prod", ALL), parent_results);
        assert_eq!(search(&state, "grand", ALL), grand_results);
        let (status, _) = call(
            &state,
            "POST /v1/namespaces/test/search",
            serde_json::json!({"query_text": "alpha"}),
        );
        assert_eq!(status, 404);
        let (status, _) = call(
            &state,
            "DELETE /v1/namespaces/test/branch",
            serde_json::json!({}),
        );
        assert_eq!(status, 404);
        let (_, body) = call(
            &state,
            "GET /v1/namespaces/prod/branches",
            serde_json::json!({}),
        );
        assert!(body["branches"].as_array().unwrap().is_empty(), "{body}");

        // Deleting the last branch referencing `acme/test/` empties it.
        let (status, body) = call(
            &state,
            "DELETE /v1/namespaces/grand/branch",
            serde_json::json!({}),
        );
        assert_eq!(status, 200, "{body}");
        let after = tree(&state.data_dir);
        assert!(prefix(&after, "acme/test").is_empty(), "{:?}", after.keys());
        assert!(prefix(&after, "acme/grand").is_empty());
        assert_eq!(prefix(&after, "acme/prod"), prefix(&before, "acme/prod"));
        assert_eq!(search(&state, "prod", ALL), parent_results);

        // The name is free again: a write creates a plain namespace.
        index(&state, "test", &[("z", "alpha zulu")]);
        assert_eq!(ids(&search(&state, "test", ALL)), ["z"]);
        let (_, body) = call(
            &state,
            "GET /v1/namespaces/test/stats",
            serde_json::json!({}),
        );
        assert!(body.get("branch").is_none());
        let _ = std::fs::remove_dir_all(&state.data_dir);
    }
}
