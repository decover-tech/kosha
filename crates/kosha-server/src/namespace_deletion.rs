//! Hard delete with a durable completion record (#347).
//!
//! - `DELETE /v1/namespaces/{ns}` deletes one namespace (tenant route; the
//!   handler is in `main.rs`, the work is [`AppState::delete_namespace_recorded`]).
//! - `DELETE /v1/admin/tenants/{tenant}` with `{"confirm": "<tenant>"}`
//!   deletes a tenant: its API keys, every namespace and branch, and
//!   whatever else sits under its storage prefix.
//! - `GET /v1/admin/deletions[?state=&tenant=&limit=]` and
//!   `GET /v1/admin/deletions/{id}` read the deletion records.
//! - `POST /v1/admin/deletions/resume` (optional `{"id": "..."}`) retries
//!   every pending record.
//!
//! The record model is in [`kosha_core::deletion`]. A record is written,
//! pending, in the step that removes the namespace from the control store,
//! and completes only after the objects under the namespace's prefix were
//! deleted **and a fresh listing of the prefix came back empty**. Anything
//! short of that (an S3 error, a leftover object, a crash) leaves the record
//! pending with the reason, and a resume finishes it later.
//!
//! What a completed record does not cover: the disk caches of *other* pods.
//! Query replicas stop serving a deleted namespace within one manifest
//! refresh and drop their in-memory state for it, but their local copies of
//! its segment files go only when the LRU evicts them.

use super::*;
use kosha_core::deletion::{
    DeletionFilter, DeletionRecord, DeletionScope, DeletionState, NewDeletion,
};

/// Object storage as deletion needs it: remove a prefix, then count what is
/// left under it. S3 in production; tests substitute a store that can fail.
pub(crate) trait RemotePrefixStore: Send + Sync {
    /// Delete every object under the logical directory `prefix`,
    /// recursively. Returns how many went.
    fn delete_prefix(&self, prefix: &str) -> Result<usize, KoshaError>;
    /// How many objects are under the logical directory `prefix` right now.
    fn count_prefix(&self, prefix: &str) -> Result<usize, KoshaError>;
}

#[cfg(feature = "s3")]
impl RemotePrefixStore for s3_storage::S3Storage {
    fn delete_prefix(&self, prefix: &str) -> Result<usize, KoshaError> {
        s3_storage::S3Storage::delete_prefix(self, prefix)
    }

    fn count_prefix(&self, prefix: &str) -> Result<usize, KoshaError> {
        // The trailing slash keeps a sibling (`acme/docs2`) out of the count
        // for `acme/docs`.
        let dir = format!("{}/", prefix.trim_matches('/'));
        Ok(self.list_objects_remote(&dir)?.len())
    }
}

/// Why a pending namespace deletion cannot be finished right now.
const RECREATED: &str = "the namespace was re-created before the storage of its deleted \
     incarnation was reclaimed, and both now share one prefix; deleting the namespace again \
     reclaims everything and lets this record complete";

/// A namespace deletion that went through (the control-plane rows are gone).
pub(crate) struct NamespaceDeleted {
    pub record: DeletionRecord,
    /// False when local files or remote objects could not be removed, or
    /// the prefix could not be verified empty. The record is then pending.
    pub storage_reclaimed: bool,
}

/// What a tenant deletion pass did.
pub(crate) struct TenantDeleted {
    pub record: DeletionRecord,
    pub api_keys_deleted: u64,
    pub static_api_keys: usize,
    /// One entry per namespace or branch handled in this pass.
    pub namespaces: Vec<serde_json::Value>,
    pub errors: Vec<String>,
}

fn short_name(id: &NamespaceId) -> &str {
    id.0.split_once('/').map_or(id.0.as_str(), |(_, name)| name)
}

fn record_json(record: &DeletionRecord) -> serde_json::Value {
    serde_json::to_value(record).unwrap_or(serde_json::Value::Null)
}

/// A JSON response with a status other than 200.
fn json_accepted(value: &serde_json::Value) -> String {
    let body = value.to_string();
    format!(
        "HTTP/1.1 202 Accepted\r\ncontent-length: {}\r\ncontent-type: application/json\r\n\r\n{body}",
        body.len()
    )
}

fn query_param(path: &str, name: &str) -> Option<String> {
    let (_, query) = path.split_once('?')?;
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name && !value.is_empty()).then(|| url_decode(value))
    })
}

impl AppState {
    /// The object store deletions reclaim from, if this node has one.
    fn remote_prefix_store(&self) -> Option<&dyn RemotePrefixStore> {
        #[cfg(test)]
        if let Some(store) = &self.remote_prefix_override {
            return Some(store.as_ref());
        }
        #[cfg(feature = "s3")]
        if let Some(s3) = &self.s3_storage {
            return Some(s3);
        }
        None
    }

    /// Delete every remote object under `prefix`, then list the prefix
    /// again: only an empty listing counts. Returns how many objects were
    /// deleted and, when the prefix is not verified empty, why.
    fn reclaim_remote_prefix(&self, prefix: &str) -> (u64, Option<String>) {
        let Some(store) = self.remote_prefix_store() else {
            return (0, None);
        };
        let deleted = match store.delete_prefix(prefix) {
            Ok(deleted) => deleted as u64,
            Err(e) => {
                return (
                    0,
                    Some(format!("deleting the objects under {prefix}/ failed: {e}")),
                )
            }
        };
        match store.count_prefix(prefix) {
            Ok(0) => (deleted, None),
            Ok(left) => (
                deleted,
                Some(format!(
                    "{left} object(s) remain under {prefix}/ after the delete"
                )),
            ),
            Err(e) => (
                deleted,
                Some(format!("could not verify that {prefix}/ is empty: {e}")),
            ),
        }
    }

    /// Record a reclaim attempt. If the control store cannot be written the
    /// record simply stays pending (a resume repeats the attempt), so the
    /// record as last known is returned.
    fn note_deletion_attempt(
        &self,
        record: &DeletionRecord,
        objects_deleted: u64,
        error: Option<&str>,
    ) -> DeletionRecord {
        let noted = self.controller.lock().unwrap().record_deletion_attempt(
            &record.id,
            objects_deleted,
            error,
        );
        match noted {
            Ok(updated) => updated,
            Err(e) => {
                eprintln!(
                    "WARN: deletion {} ({}): could not record the reclaim attempt: {e}; it stays \
                     pending until POST /v1/admin/deletions/resume",
                    record.id, record.subject
                );
                record.clone()
            }
        }
    }

    /// Drop a deleted root's shard namespaces from the control store and
    /// the writer. Keeps going past a failure and returns the first one.
    fn drop_deleted_shards(&self, root: &NamespaceId, shards: &[NamespaceId]) -> Option<String> {
        let mut failure = None;
        for shard in shards {
            let shard_local = self.indexer.manifest_version(shard);
            let shard_deleted = self
                .controller
                .lock()
                .unwrap()
                .delete_namespace(shard, shard_local.unwrap_or(0));
            let shard_retired = match shard_deleted {
                Ok((_, retired)) => retired,
                Err(KoshaError::NamespaceNotFound(_)) => shard_local.unwrap_or(0),
                Err(e) => {
                    failure
                        .get_or_insert(format!("delete namespace failed: shard {}: {e}", shard.0));
                    shard_local.unwrap_or(0)
                }
            };
            let shard_next = kosha_core::initial_manifest_version(Some(
                shard_retired.max(shard_local.unwrap_or(0)),
            ));
            if let Err(e) = self.indexer.drop_namespace(shard, shard_next, || {}) {
                failure.get_or_insert(format!(
                    "namespace '{}': discarding the write-ahead log of shard {} failed: {e}",
                    short_name(root),
                    shard.0
                ));
            }
            self.forget_namespace_caches(shard);
        }
        failure
    }

    /// Delete namespace `id` and record it. See `handle_delete_namespace`
    /// for the steps. `parent` is the tenant deletion this is part of.
    ///
    /// `Err` is a ready-to-send JSON error response. Before the control
    /// store dropped the namespace nothing has changed; after it, the
    /// deletion record exists and carries the failure.
    pub(crate) fn delete_namespace_recorded(
        &self,
        id: &NamespaceId,
        parent: Option<&str>,
    ) -> Result<NamespaceDeleted, String> {
        let name = short_name(id);
        // No branch is created from (or deleted under) this namespace while it
        // goes (#208).
        let _lineage = self.branch_lock.lock().unwrap();
        // Deleting a namespace deletes everything under its prefix. A branch
        // still listing segments or deletion bitmaps that live there would be
        // left pointing at nothing, so the branches go first.
        match branches_referencing(self, id) {
            Ok(branches) if branches.is_empty() => {}
            Ok(branches) => {
                return Err(api_error(
                    ErrorCode::Conflict,
                    &format!(
                        "namespace '{name}' has branches that share its segments ({}); delete \
                         them first (DELETE /v1/namespaces/{{branch}}/branch)",
                        branches
                            .iter()
                            .map(short_name)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                ))
            }
            Err(e) => {
                return Err(crate::control_store_unavailable(&format!(
                    "control store: {e}"
                )))
            }
        }
        // A branch goes through DELETE .../branch, which also reclaims the
        // prefixes of deleted ancestor branches only it kept alive; deleting
        // it here would leak those for good.
        match self.controller.lock().unwrap().try_manifest_cloned(id) {
            Ok(Some(manifest)) if manifest.branch.is_some() => {
                return Err(api_error(
                    ErrorCode::Conflict,
                    &format!(
                        "namespace '{name}' is a branch; delete it with \
                         DELETE /v1/namespaces/{name}/branch"
                    ),
                ))
            }
            Ok(_) => {}
            Err(e) => {
                return Err(crate::control_store_unavailable(&format!(
                    "control store: {e}"
                )))
            }
        }
        let lock = self.publication_lock(id);
        let _guard = lock.lock().unwrap();
        let local_version = self.indexer.manifest_version(id);
        let request = NewDeletion::namespace(&id.0, namespace_tenant(&id.0), parent);
        let deleted = self.controller.lock().unwrap().delete_namespace_recorded(
            id,
            local_version.unwrap_or(0),
            &request,
        );
        let (manifest, retired, record) = match deleted {
            Ok(deleted) => deleted,
            // Known only to this writer (written to but not yet registered, or
            // WAL-recovered under the in-memory control plane): still delete it.
            Err(KoshaError::NamespaceNotFound(_)) if local_version.is_some() => {
                let last = local_version.unwrap_or(0);
                let record = self
                    .controller
                    .lock()
                    .unwrap()
                    .begin_deletion(&request, last)
                    .map_err(|e| {
                        crate::control_store_unavailable(&format!("control store: {e}"))
                    })?;
                (
                    self.indexer
                        .manifest_cloned(id)
                        .unwrap_or_else(|| kosha_core::Manifest::empty(0)),
                    last,
                    record,
                )
            }
            Err(KoshaError::NamespaceNotFound(_)) => {
                return Err(api_error(
                    ErrorCode::NotFound,
                    &format!("namespace '{name}' not found"),
                ));
            }
            Err(e) => {
                return Err(api_error(
                    ErrorCode::Internal,
                    &format!("delete namespace failed: {e}"),
                ))
            }
        };
        // From here on the namespace is gone from the control store and the
        // record exists. Every remaining step runs even if an earlier one
        // failed, so the writer never keeps serving the deleted namespace;
        // the first failure is reported and kept on the record.
        //
        // A sharded namespace (#207): its shards are namespaces of their own,
        // nested under its prefix (so the local and S3 cleanup below covers
        // their files). Drop them from the control store and the writer too,
        // or their manifests, buffers and WAL records would outlive the root.
        let shard_ids = manifest
            .shards
            .filter(|&n| n > 1)
            .map(|n| kosha_core::sharding::shard_namespaces(id, n))
            .unwrap_or_default();
        let mut failure = self.drop_deleted_shards(id, &shard_ids);
        let next_version =
            kosha_core::initial_manifest_version(Some(retired.max(local_version.unwrap_or(0))));
        // Local segment files go while the writer still holds the namespace's
        // locks, so a write to the new incarnation cannot lose its fresh segment.
        let mut local_cleanup = Ok(());
        let dropped = self.indexer.drop_namespace(id, next_version, || {
            local_cleanup = self.cache.evict_prefix(&id.0);
        });
        match dropped {
            Ok(0) => {}
            Ok(shared) => println!(
                "namespace {} deleted: {shared} WAL file(s) shared with other namespaces are kept \
                 until those flush; recovery skips this namespace's records in them",
                id.0
            ),
            Err(e) => {
                failure.get_or_insert(format!(
                    "namespace '{name}' was removed from the control store, but discarding its \
                     write-ahead log failed: {e}"
                ));
            }
        }
        self.forget_namespace_caches(id);
        let mut storage_error = None;
        if let Err(e) = local_cleanup {
            eprintln!(
                "WARN: namespace {} deleted, local segment cleanup failed: {e}",
                id.0
            );
            storage_error = Some(format!("local segment cleanup failed: {e}"));
        }
        let (objects, remote_error) = self.reclaim_remote_prefix(&id.0);
        if let Some(e) = &remote_error {
            eprintln!("WARN: namespace {} deleted, but {e}", id.0);
        }
        let storage_error = storage_error.or(remote_error);
        let error = failure.clone().or_else(|| storage_error.clone());
        let record = self.note_deletion_attempt(&record, objects, error.as_deref());
        println!(
            "namespace deleted: ns={} version={} segments={} next_version={next_version} \
             s3_objects_deleted={objects} deletion={} state={}",
            id.0,
            manifest.version,
            manifest.segments.len(),
            record.id,
            record.state.as_str()
        );
        if let Some(message) = failure {
            return Err(api_error(ErrorCode::Internal, &message));
        }
        Ok(NamespaceDeleted {
            storage_reclaimed: storage_error.is_none(),
            record,
        })
    }

    /// Retry the storage reclaim of a pending namespace deletion.
    ///
    /// Refused (the record stays pending, with the reason) when the
    /// namespace exists again: a new incarnation writes into the same
    /// prefix, and nothing here can tell its objects from the old ones.
    /// The check and the cleanup hold the namespace's publication lock and
    /// the writer's namespace locks, so no write can slip in between.
    pub(crate) fn resume_namespace_deletion(&self, record: &DeletionRecord) -> DeletionRecord {
        let id = NamespaceId(record.subject.clone());
        let _lineage = self.branch_lock.lock().unwrap();
        let lock = self.publication_lock(&id);
        let _guard = lock.lock().unwrap();
        let (objects, error) = self.retry_namespace_reclaim(&id);
        self.note_deletion_attempt(record, objects, error.as_deref())
    }

    fn retry_namespace_reclaim(&self, id: &NamespaceId) -> (u64, Option<String>) {
        let stored = self.controller.lock().unwrap().manifest_versions();
        let stored = match stored {
            Ok(stored) => stored,
            Err(e) => return (0, Some(format!("control store: {e}"))),
        };
        if stored.iter().any(|(ns, _)| ns == id) {
            return (0, Some(RECREATED.into()));
        }
        let mut local_cleanup = Ok(());
        let discarded = self.indexer.discard_if_untouched(id, || {
            local_cleanup = self.cache.evict_prefix(&id.0);
        });
        match discarded {
            Ok(Some(_)) => {}
            Ok(None) => return (0, Some(RECREATED.into())),
            Err(e) => {
                return (
                    0,
                    Some(format!("discarding its write-ahead log failed: {e}")),
                )
            }
        }
        self.forget_namespace_caches(id);
        // Shard rows the interrupted delete left behind. The root is gone
        // and unwritten, so they belong to the deleted incarnation.
        let below = format!("{}/", id.0);
        let leftover: Vec<NamespaceId> = stored
            .into_iter()
            .map(|(ns, _)| ns)
            .filter(|ns| {
                ns.0.starts_with(&below) && kosha_core::sharding::shard_index_of(ns).is_some()
            })
            .collect();
        let mut error = self.drop_deleted_shards(id, &leftover);
        if let Err(e) = local_cleanup {
            error.get_or_insert(format!("local segment cleanup failed: {e}"));
        }
        let (objects, remote_error) = self.reclaim_remote_prefix(&id.0);
        (objects, error.or(remote_error))
    }

    /// Every namespace id under `tenant/` that the control store or this
    /// writer knows.
    fn tenant_namespaces(&self, tenant: &str) -> Result<Vec<NamespaceId>, KoshaError> {
        let prefix = format!("{tenant}/");
        let mut ids: Vec<NamespaceId> = {
            let controller = self.controller.lock().unwrap();
            // `manifest_versions` fails when the store cannot be read;
            // `list_namespaces` would read as empty and the tenant would
            // look deleted.
            let mut ids: Vec<NamespaceId> = controller
                .manifest_versions()?
                .into_iter()
                .map(|(ns, _)| ns)
                .collect();
            ids.extend(controller.list_namespaces());
            ids
        };
        ids.extend(self.indexer.namespaces());
        ids.retain(|ns| ns.0.starts_with(&prefix) && ns.0.len() > prefix.len());
        ids.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        ids.dedup();
        Ok(ids)
    }

    /// One pass of deleting `tenant` under the pending tenant `record`:
    /// keys, branches, namespaces, then everything left under the tenant's
    /// prefix. Safe to repeat; a resume runs it again.
    pub(crate) fn delete_tenant(
        &self,
        auth: &auth::Auth,
        record: &DeletionRecord,
    ) -> TenantDeleted {
        let tenant = record.subject.as_str();
        let mut errors: Vec<String> = Vec::new();
        let mut namespaces = Vec::new();

        // Keys first, so the tenant stops writing while its data goes.
        let (api_keys_deleted, static_api_keys) = match auth::purge_tenant_keys(auth, tenant) {
            Ok(purge) => (purge.deleted, purge.static_remaining),
            Err(e) => {
                errors.push(format!("deleting API keys failed: {e}"));
                (0, 0)
            }
        };
        if static_api_keys > 0 {
            errors.push(format!(
                "{static_api_keys} API key(s) for this tenant are set in KOSHA_API_KEYS / \
                 KOSHA_API_KEY and still authenticate; remove them from the deployment, restart, \
                 and resume this deletion"
            ));
        }

        match self.tenant_namespaces(tenant) {
            Err(e) => errors.push(format!("control store: {e}")),
            Ok(ids) => {
                let known: std::collections::HashSet<&NamespaceId> = ids.iter().collect();
                // A shard goes with its root.
                let is_shard_of_listed = |ns: &NamespaceId| {
                    kosha_core::sharding::shard_index_of(ns).is_some()
                        && ns
                            .0
                            .rsplit_once('/')
                            .is_some_and(|(root, _)| known.contains(&NamespaceId(root.into())))
                };
                let mut roots = Vec::new();
                // Branches first: a namespace cannot go while a branch
                // still lists its segments.
                for ns in ids.iter().filter(|ns| !is_shard_of_listed(ns)) {
                    let stored = self.controller.lock().unwrap().try_manifest_cloned(ns);
                    match stored {
                        Ok(Some(manifest)) if manifest.branch.is_some() => {
                            let outcome = self.delete_branch(ns);
                            if let Err(response) = &outcome {
                                errors.push(format!(
                                    "branch '{}': {}",
                                    short_name(ns),
                                    response_error(response)
                                ));
                            }
                            namespaces.push(serde_json::json!({
                                "namespace": short_name(ns),
                                "branch": true,
                                "deleted": outcome.is_ok(),
                            }));
                        }
                        Ok(_) => roots.push(ns.clone()),
                        Err(e) => {
                            errors.push(format!("namespace '{}': {e}", short_name(ns)));
                        }
                    }
                }
                for ns in &roots {
                    match self.delete_namespace_recorded(ns, Some(&record.id)) {
                        Ok(deleted) => {
                            if let Some(e) = &deleted.record.last_error {
                                errors.push(format!("namespace '{}': {e}", short_name(ns)));
                            }
                            namespaces.push(serde_json::json!({
                                "namespace": short_name(ns),
                                "deleted": true,
                                "deletion_id": deleted.record.id,
                                "state": deleted.record.state,
                            }));
                        }
                        Err(response) => {
                            errors.push(format!(
                                "namespace '{}': {}",
                                short_name(ns),
                                response_error(&response)
                            ));
                            namespaces.push(serde_json::json!({
                                "namespace": short_name(ns),
                                "deleted": false,
                            }));
                        }
                    }
                }
            }
        }

        // What no namespace deletion covers: objects of namespaces that
        // were never registered, and whatever a branch deletion could not
        // reclaim. Only once every namespace is gone, so a namespace that
        // failed to delete keeps its files.
        let mut objects = 0;
        match self.tenant_namespaces(tenant) {
            Err(e) => errors.push(format!("control store: {e}")),
            Ok(left) if !left.is_empty() => errors.push(format!(
                "{} namespace(s) of the tenant still exist ({}); resume this deletion",
                left.len(),
                left.iter().map(short_name).collect::<Vec<_>>().join(", ")
            )),
            Ok(_) => {
                if let Err(e) = self.cache.evict_prefix(tenant) {
                    errors.push(format!("local cleanup of {tenant}/ failed: {e}"));
                }
                let (deleted, remote_error) = self.reclaim_remote_prefix(tenant);
                objects = deleted;
                errors.extend(remote_error);
                // The sweep holds no namespace lock, so a write with a key
                // another pod has not dropped yet can create a namespace
                // while it runs, and the sweep may have taken that
                // namespace's first objects. Never report the tenant gone
                // then: the next pass deletes the namespace properly.
                match self.tenant_namespaces(tenant) {
                    Err(e) => errors.push(format!("control store: {e}")),
                    Ok(late) if !late.is_empty() => errors.push(format!(
                        "{} namespace(s) of the tenant were created while its prefix was \
                         being swept ({}); resume this deletion",
                        late.len(),
                        late.iter().map(short_name).collect::<Vec<_>>().join(", ")
                    )),
                    Ok(_) => {}
                }
            }
        }

        let error = (!errors.is_empty()).then(|| errors.join("; "));
        let record = self.note_deletion_attempt(record, objects, error.as_deref());
        println!(
            "tenant deletion: tenant={tenant} deletion={} state={} api_keys_deleted={api_keys_deleted} \
             namespaces={} errors={}",
            record.id,
            record.state.as_str(),
            namespaces.len(),
            errors.len()
        );
        TenantDeleted {
            record,
            api_keys_deleted,
            static_api_keys,
            namespaces,
            errors,
        }
    }

    /// Retry one pending record.
    fn resume_deletion(&self, auth: &auth::Auth, record: &DeletionRecord) -> DeletionRecord {
        match record.scope {
            DeletionScope::Namespace => self.resume_namespace_deletion(record),
            DeletionScope::Tenant => self.delete_tenant(auth, record).record,
        }
    }
}

/// The `error` text of a JSON error response, for folding one handler's
/// failure into another's report.
fn response_error(response: &str) -> String {
    response
        .split_once("\r\n\r\n")
        .and_then(|(_, body)| serde_json::from_str::<serde_json::Value>(body).ok())
        .and_then(|body| body.get("error").and_then(|e| e.as_str()).map(String::from))
        .unwrap_or_else(|| "failed".into())
}

/// `DELETE /v1/namespaces/{ns}` response for a deletion that went through.
pub(crate) fn namespace_deleted_response(name: &str, deleted: &NamespaceDeleted) -> String {
    json_ok(&serde_json::json!({
        "namespace": name,
        "acknowledged": true,
        "storage_reclaimed": deleted.storage_reclaimed,
        "deletion": record_json(&deleted.record),
    }))
}

/// Dispatch the admin routes of this module; `None` for any other request.
pub(crate) fn route_admin(request_line: &str, body: &[u8], state: &AppState) -> Option<String> {
    let (method, path) = parse_method_and_path(request_line)?;
    let bare = path.split('?').next().unwrap_or(path);
    match (method, bare) {
        ("DELETE", _) if bare.starts_with("/v1/admin/tenants/") => {
            let tenant = url_decode(&bare["/v1/admin/tenants/".len()..]);
            Some(handle_delete_tenant(&tenant, body, state, auth::global()))
        }
        ("POST", "/v1/admin/deletions/resume") => {
            Some(handle_resume_deletions(body, state, auth::global()))
        }
        ("GET", "/v1/admin/deletions") => Some(handle_list_deletions(path, state)),
        ("GET", _) if bare.starts_with("/v1/admin/deletions/") => {
            let id = url_decode(&bare["/v1/admin/deletions/".len()..]);
            Some(handle_get_deletion(&id, state))
        }
        _ => None,
    }
}

/// `DELETE /v1/admin/tenants/{tenant}` with `{"confirm": "<tenant>"}`.
///
/// 200 when the tenant is gone and its record completed; 202 when the
/// deletion is recorded but something is left (see `errors` and
/// `deletion.last_error`), to be finished by
/// `POST /v1/admin/deletions/resume`.
pub(crate) fn handle_delete_tenant(
    tenant: &str,
    body: &[u8],
    state: &AppState,
    auth: &auth::Auth,
) -> String {
    // Ids starting with `_` or `.` are not tenants of their own (#342), and
    // `_wal` under the storage root is the write-ahead log.
    if !valid_new_tenant_id(tenant) {
        return api_error(
            ErrorCode::InvalidArgument,
            "invalid tenant id (must be non-empty, without '/', '\\' or control characters, \
             and not start with '_' or '.')",
        );
    }
    let confirmed = serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("confirm").and_then(|c| c.as_str()).map(String::from));
    if confirmed.as_deref() != Some(tenant) {
        return api_error(ErrorCode::InvalidArgument,
            &format!(
                "deleting a tenant is permanent: confirm it by sending {{\"confirm\": \"{tenant}\"}}"
            ),
        );
    }
    // A pre-#262 bare namespace named like the tenant keeps its segments
    // directly under `{tenant}/`, the prefix this deletes.
    let bare = NamespaceId(tenant.to_string());
    match state.controller.lock().unwrap().manifest_version(&bare) {
        Ok(None) => {}
        Ok(Some(_)) => {
            return api_error(
                ErrorCode::Conflict,
                &format!(
                    "an unscoped namespace named '{tenant}' stores its segments under the \
                     tenant's prefix; migrate it (kosha-server migrate-namespace) or delete it \
                     first"
                ),
            )
        }
        Err(e) => return crate::control_store_unavailable(&format!("control store: {e}")),
    }
    // A retry of an unfinished deletion continues its record.
    let pending = state.controller.lock().unwrap().deletions(&DeletionFilter {
        state: Some(DeletionState::Pending),
        tenant_id: Some(tenant.to_string()),
        limit: None,
    });
    let existing = match pending {
        Ok(records) => records
            .into_iter()
            .find(|r| r.scope == DeletionScope::Tenant && r.subject == tenant),
        Err(e) => return crate::control_store_unavailable(&format!("control store: {e}")),
    };
    let record = match existing {
        Some(record) => record,
        None => {
            let begun = state
                .controller
                .lock()
                .unwrap()
                .begin_deletion(&NewDeletion::tenant(tenant), 0);
            match begun {
                Ok(record) => record,
                Err(e) => return crate::control_store_unavailable(&format!("control store: {e}")),
            }
        }
    };
    let done = state.delete_tenant(auth, &record);
    let completed = done.record.is_completed();
    let body = serde_json::json!({
        "tenant": tenant,
        "acknowledged": true,
        "completed": completed,
        "deletion": record_json(&done.record),
        "api_keys_deleted": done.api_keys_deleted,
        "static_api_keys_remaining": done.static_api_keys,
        "namespaces": done.namespaces,
        "errors": done.errors,
    });
    if completed {
        json_ok(&body)
    } else {
        json_accepted(&body)
    }
}

/// `POST /v1/admin/deletions/resume` — retry pending deletions: all of them
/// (oldest first, namespaces before tenants), or the one named by
/// `{"id": "..."}`.
pub(crate) fn handle_resume_deletions(body: &[u8], state: &AppState, auth: &auth::Auth) -> String {
    let only = if body.iter().all(u8::is_ascii_whitespace) {
        None
    } else {
        match serde_json::from_slice::<serde_json::Value>(body) {
            Ok(v) => v.get("id").and_then(|id| id.as_str()).map(String::from),
            Err(_) => return api_error(ErrorCode::InvalidArgument, "invalid JSON"),
        }
    };
    let mut pending = match &only {
        Some(id) => match state.controller.lock().unwrap().deletion(id) {
            Ok(Some(record)) => vec![record],
            Ok(None) => {
                return api_error(ErrorCode::NotFound, &format!("deletion '{id}' not found"))
            }
            Err(e) => return crate::control_store_unavailable(&format!("control store: {e}")),
        },
        None => {
            let listed = state
                .controller
                .lock()
                .unwrap()
                .deletions(&DeletionFilter::pending());
            match listed {
                Ok(records) => records,
                Err(e) => return crate::control_store_unavailable(&format!("control store: {e}")),
            }
        }
    };
    // A tenant's pass re-checks its namespaces, so those go first. The
    // sort is stable: oldest first within each scope.
    pending.sort_by_key(|record| record.scope == DeletionScope::Tenant);
    let mut still_pending = 0;
    let resumed: Vec<serde_json::Value> = pending
        .iter()
        .map(|record| {
            let after = if record.is_completed() {
                record.clone()
            } else {
                state.resume_deletion(auth, record)
            };
            if !after.is_completed() {
                still_pending += 1;
            }
            record_json(&after)
        })
        .collect();
    json_ok(&serde_json::json!({
        "resumed": resumed,
        "pending": still_pending,
    }))
}

/// `GET /v1/admin/deletions[?state=pending|completed&tenant=<id>&limit=<n>]`
fn handle_list_deletions(path: &str, state: &AppState) -> String {
    let mut filter = DeletionFilter::default();
    if let Some(raw) = query_param(path, "state") {
        match DeletionState::parse(&raw) {
            Some(parsed) => filter.state = Some(parsed),
            None => {
                return api_error(
                    ErrorCode::InvalidArgument,
                    "invalid 'state' (pending or completed)",
                )
            }
        }
    }
    filter.tenant_id = query_param(path, "tenant");
    if let Some(raw) = query_param(path, "limit") {
        match raw.parse::<u32>() {
            Ok(limit) if limit > 0 => filter.limit = Some(limit),
            _ => {
                return api_error(
                    ErrorCode::InvalidArgument,
                    "invalid 'limit' (a positive integer)",
                )
            }
        }
    }
    let listed = state.controller.lock().unwrap().deletions(&filter);
    match listed {
        Ok(records) => json_ok(&serde_json::json!({ "deletions": records })),
        Err(e) => crate::control_store_unavailable(&format!("control store: {e}")),
    }
}

/// `GET /v1/admin/deletions/{id}`
fn handle_get_deletion(id: &str, state: &AppState) -> String {
    let found = state.controller.lock().unwrap().deletion(id);
    match found {
        Ok(Some(record)) => json_ok(&record),
        Ok(None) => api_error(ErrorCode::NotFound, &format!("deletion '{id}' not found")),
        Err(e) => crate::control_store_unavailable(&format!("control store: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// An in-memory bucket that can refuse deletes, or claim a delete it
    /// did not carry out.
    #[derive(Default)]
    struct FakeRemote {
        objects: Mutex<BTreeSet<String>>,
        fail_deletes: AtomicBool,
        /// Keys a delete reports as gone but leaves in place.
        sticky: Mutex<BTreeSet<String>>,
        /// Runs at the start of every delete, with its prefix: something
        /// happening concurrently with the reclaim.
        on_delete: Mutex<Option<DeleteHook>>,
    }

    type DeleteHook = Box<dyn Fn(&str) + Send + Sync>;

    impl FakeRemote {
        fn put(&self, key: &str) {
            self.objects.lock().unwrap().insert(key.to_string());
        }

        fn under(&self, prefix: &str) -> Vec<String> {
            let dir = format!("{}/", prefix.trim_matches('/'));
            self.objects
                .lock()
                .unwrap()
                .iter()
                .filter(|key| key.starts_with(&dir))
                .cloned()
                .collect()
        }
    }

    impl RemotePrefixStore for FakeRemote {
        fn delete_prefix(&self, prefix: &str) -> Result<usize, KoshaError> {
            if let Some(hook) = self.on_delete.lock().unwrap().as_ref() {
                hook(prefix);
            }
            if self.fail_deletes.load(Ordering::SeqCst) {
                return Err(KoshaError::NotFound("injected S3 outage".into()));
            }
            let doomed = self.under(prefix);
            let sticky = self.sticky.lock().unwrap();
            let mut objects = self.objects.lock().unwrap();
            for key in &doomed {
                if !sticky.contains(key) {
                    objects.remove(key);
                }
            }
            Ok(doomed.len())
        }

        fn count_prefix(&self, prefix: &str) -> Result<usize, KoshaError> {
            Ok(self.under(prefix).len())
        }
    }

    const ADMIN: &str = "admin-secret";

    fn state_with_remote() -> (AppState, Arc<FakeRemote>) {
        let mut state = AppState::new(crate::tests::test_dir());
        state.admin_auth_key = Some(ADMIN.into());
        let remote = Arc::new(FakeRemote::default());
        state.remote_prefix_override = Some(remote.clone());
        (state, remote)
    }

    fn parse(response: &str) -> (u16, serde_json::Value) {
        let status: u16 = response[9..12].parse().unwrap();
        let body = &response[response.find("\r\n\r\n").unwrap() + 4..];
        (status, serde_json::from_str(body).unwrap())
    }

    /// A request through `route`, carrying the admin credential (tenant
    /// routes ignore it).
    fn call(
        state: &AppState,
        tenant: &str,
        line: &str,
        body: serde_json::Value,
    ) -> (u16, serde_json::Value) {
        let headers = HashMap::from([("authorization".to_string(), format!("Bearer {ADMIN}"))]);
        parse(&route(
            &format!("{line} HTTP/1.1\r\n"),
            &headers,
            &serde_json::to_vec(&body).unwrap(),
            tenant,
            state,
        ))
    }

    fn index(state: &AppState, tenant: &str, ns: &str, ids: &[&str]) {
        let documents: Vec<_> = ids
            .iter()
            .map(|id| {
                serde_json::json!({"id": id, "fields": [
                    {"name": "body", "field_type": "Text", "value": format!("common {id}")}]})
            })
            .collect();
        let (status, body) = call(
            state,
            tenant,
            &format!("POST /v1/namespaces/{ns}/documents"),
            serde_json::json!({ "documents": documents }),
        );
        assert_eq!(status, 200, "{body}");
        let (status, body) = call(
            state,
            tenant,
            &format!("POST /v1/namespaces/{ns}/flush"),
            serde_json::json!({}),
        );
        assert_eq!(status, 200, "{body}");
    }

    /// Search status and the ids found.
    fn search(state: &AppState, tenant: &str, ns: &str) -> (u16, Vec<String>) {
        let (status, body) = call(
            state,
            tenant,
            &format!("POST /v1/namespaces/{ns}/search"),
            serde_json::json!({"query_text": "common", "top_k": 50, "no_cache": true}),
        );
        let mut ids: Vec<String> = body["results"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|hit| hit["doc_id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        (status, ids)
    }

    fn delete_ns(state: &AppState, tenant: &str, ns: &str) -> (u16, serde_json::Value) {
        call(
            state,
            tenant,
            &format!("DELETE /v1/namespaces/{ns}"),
            serde_json::json!({}),
        )
    }

    fn resume(state: &AppState) -> serde_json::Value {
        let (status, body) = call(
            state,
            SERVICE_TENANT,
            "POST /v1/admin/deletions/resume",
            serde_json::json!({}),
        );
        assert_eq!(status, 200, "{body}");
        body
    }

    fn listed(state: &AppState, query: &str) -> Vec<serde_json::Value> {
        let (status, body) = call(
            state,
            SERVICE_TENANT,
            &format!("GET /v1/admin/deletions{query}"),
            serde_json::json!({}),
        );
        assert_eq!(status, 200, "{body}");
        body["deletions"].as_array().unwrap().clone()
    }

    fn no_keys() -> auth::Auth {
        auth::Auth::Keys(auth::KeyStore::with_keys(HashMap::new()))
    }

    #[test]
    fn deleting_a_namespace_completes_its_record_once_storage_is_verified_empty() {
        let (state, remote) = state_with_remote();
        index(&state, "acme", "docs", &["a", "b"]);
        index(&state, "acme", "docs2", &["c"]);
        remote.put("acme/docs/seg-1/postings.bin");
        remote.put("acme/docs/seg-1/_COMMIT");
        // Neighbours whose names only share a prefix, and another tenant.
        remote.put("acme/docs2/seg-1/postings.bin");
        remote.put("globex/docs/seg-1/postings.bin");
        assert!(state.data_dir.join("acme/docs").is_dir());

        let (status, body) = delete_ns(&state, "acme", "docs");
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["namespace"], "docs");
        assert_eq!(body["acknowledged"], true);
        assert_eq!(body["storage_reclaimed"], true);
        let record = &body["deletion"];
        assert_eq!(record["scope"], "namespace");
        assert_eq!(record["subject"], "acme/docs");
        assert_eq!(record["tenant_id"], "acme");
        assert_eq!(record["state"], "completed");
        assert_eq!(record["attempts"], 1);
        assert_eq!(record["objects_deleted"], 2);
        assert!(record["last_error"].is_null());
        assert!(
            record["completed_at_ms"].as_u64().unwrap()
                >= record["requested_at_ms"].as_u64().unwrap()
        );

        // Nothing of it is left, locally or remotely; neighbours are whole.
        assert!(!state.data_dir.join("acme/docs").exists());
        assert!(remote.under("acme/docs").is_empty());
        assert_eq!(remote.under("acme/docs2").len(), 1);
        assert_eq!(remote.under("globex").len(), 1);
        assert_eq!(search(&state, "acme", "docs").0, 404);
        assert_eq!(search(&state, "acme", "docs2").1, ["c"]);

        // The record is readable afterwards, by id and in listings.
        let id = record["id"].as_str().unwrap();
        let (status, fetched) = call(
            &state,
            SERVICE_TENANT,
            &format!("GET /v1/admin/deletions/{id}"),
            serde_json::json!({}),
        );
        assert_eq!(status, 200);
        assert_eq!(&fetched, record);
        assert!(listed(&state, "?state=pending").is_empty());
        assert_eq!(listed(&state, "?tenant=acme&state=completed").len(), 1);
        assert!(listed(&state, "?tenant=globex").is_empty());
        let (status, _) = call(
            &state,
            SERVICE_TENANT,
            "GET /v1/admin/deletions?state=bogus",
            serde_json::json!({}),
        );
        assert_eq!(status, 400);
        let (status, _) = call(
            &state,
            SERVICE_TENANT,
            "GET /v1/admin/deletions/del-nope",
            serde_json::json!({}),
        );
        assert_eq!(status, 404);

        // Deleting it again is a 404 and records nothing.
        assert_eq!(delete_ns(&state, "acme", "docs").0, 404);
        assert_eq!(listed(&state, "").len(), 1);
    }

    /// A namespace only the writer knows (buffered, never published) is
    /// recorded too.
    #[test]
    fn deleting_an_unpublished_namespace_is_recorded() {
        let (state, _remote) = state_with_remote();
        let id = NamespaceId("acme/fresh".into());
        state
            .indexer
            .index_documents(
                id.clone(),
                vec![kosha_core::Document {
                    id: kosha_core::DocumentId("a".into()),
                    fields: vec![kosha_core::Field::text("body", "common a")],
                }],
            )
            .unwrap();
        assert!(!state.controller.lock().unwrap().has_namespace(&id));
        let (status, body) = delete_ns(&state, "acme", "fresh");
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["deletion"]["state"], "completed");
        assert_eq!(body["deletion"]["subject"], "acme/fresh");
        assert_eq!(state.indexer.pending_doc_count(&id), 0);
        assert_eq!(listed(&state, "?tenant=acme").len(), 1);
    }

    #[test]
    fn a_failed_storage_reclaim_leaves_the_record_pending_and_resume_finishes_it() {
        let (state, remote) = state_with_remote();
        index(&state, "acme", "docs", &["a"]);
        remote.put("acme/docs/seg-1/postings.bin");
        remote.put("acme/docs/seg-1/doc_store.bin");
        remote.fail_deletes.store(true, Ordering::SeqCst);

        // The delete itself still goes through.
        let (status, body) = delete_ns(&state, "acme", "docs");
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["storage_reclaimed"], false);
        let record = &body["deletion"];
        assert_eq!(record["state"], "pending");
        assert_eq!(record["attempts"], 1);
        assert_eq!(record["objects_deleted"], 0);
        assert!(
            record["last_error"]
                .as_str()
                .unwrap()
                .contains("injected S3 outage"),
            "{record}"
        );
        assert!(record["completed_at_ms"].is_null());
        assert_eq!(search(&state, "acme", "docs").0, 404);
        assert_eq!(remote.under("acme/docs").len(), 2);
        assert_eq!(listed(&state, "?state=pending").len(), 1);

        // Still failing: the attempt is counted, nothing completes.
        let again = resume(&state);
        assert_eq!(again["pending"], 1);
        assert_eq!(again["resumed"][0]["attempts"], 2);
        assert_eq!(again["resumed"][0]["state"], "pending");

        remote.fail_deletes.store(false, Ordering::SeqCst);
        let done = resume(&state);
        assert_eq!(done["pending"], 0);
        let finished = &done["resumed"][0];
        assert_eq!(finished["id"], record["id"]);
        assert_eq!(finished["state"], "completed");
        assert_eq!(finished["attempts"], 3);
        assert_eq!(finished["objects_deleted"], 2);
        assert!(finished["last_error"].is_null());
        assert!(remote.under("acme/docs").is_empty());

        // Nothing is pending any more.
        let idle = resume(&state);
        assert_eq!(idle["resumed"].as_array().unwrap().len(), 0);
        assert!(listed(&state, "?state=pending").is_empty());
    }

    /// The delete call succeeding is not the proof: the listing is.
    #[test]
    fn a_delete_that_leaves_objects_behind_is_not_reported_complete() {
        let (state, remote) = state_with_remote();
        index(&state, "acme", "docs", &["a"]);
        remote.put("acme/docs/seg-1/postings.bin");
        remote.put("acme/docs/seg-1/vectors.bin");
        remote
            .sticky
            .lock()
            .unwrap()
            .insert("acme/docs/seg-1/vectors.bin".into());

        let (status, body) = delete_ns(&state, "acme", "docs");
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["storage_reclaimed"], false);
        let record = &body["deletion"];
        assert_eq!(record["state"], "pending");
        assert!(
            record["last_error"]
                .as_str()
                .unwrap()
                .contains("1 object(s) remain under acme/docs/"),
            "{record}"
        );

        remote.sticky.lock().unwrap().clear();
        let (status, one) = call(
            &state,
            SERVICE_TENANT,
            "POST /v1/admin/deletions/resume",
            serde_json::json!({"id": record["id"]}),
        );
        assert_eq!(status, 200, "{one}");
        assert_eq!(one["pending"], 0);
        assert_eq!(one["resumed"][0]["state"], "completed");
        assert!(remote.under("acme/docs").is_empty());
        let (status, _) = call(
            &state,
            SERVICE_TENANT,
            "POST /v1/admin/deletions/resume",
            serde_json::json!({"id": "del-nope"}),
        );
        assert_eq!(status, 404);
    }

    #[test]
    fn resume_never_touches_a_re_created_namespace() {
        let (state, remote) = state_with_remote();
        index(&state, "acme", "docs", &["old"]);
        remote.put("acme/docs/old-seg/postings.bin");
        remote.fail_deletes.store(true, Ordering::SeqCst);
        let (status, first) = delete_ns(&state, "acme", "docs");
        assert_eq!(status, 200, "{first}");
        assert_eq!(first["deletion"]["state"], "pending");

        // The tenant writes to the same name again before the retry.
        index(&state, "acme", "docs", &["new"]);
        remote.put("acme/docs/new-seg/postings.bin");
        remote.fail_deletes.store(false, Ordering::SeqCst);

        let blocked = resume(&state);
        assert_eq!(blocked["pending"], 1);
        assert_eq!(blocked["resumed"][0]["state"], "pending");
        assert!(
            blocked["resumed"][0]["last_error"]
                .as_str()
                .unwrap()
                .contains("re-created"),
            "{blocked}"
        );
        // The new incarnation is whole: searchable, on disk and remotely.
        assert_eq!(
            search(&state, "acme", "docs"),
            (200, vec!["new".to_string()])
        );
        assert!(state.data_dir.join("acme/docs").is_dir());
        assert_eq!(remote.under("acme/docs").len(), 2);

        // Deleting it again reclaims the whole prefix; the older record
        // can then complete too.
        let (status, second) = delete_ns(&state, "acme", "docs");
        assert_eq!(status, 200, "{second}");
        assert_eq!(second["deletion"]["state"], "completed");
        assert_eq!(second["deletion"]["objects_deleted"], 2);
        assert_ne!(second["deletion"]["id"], first["deletion"]["id"]);
        let done = resume(&state);
        assert_eq!(done["pending"], 0);
        assert_eq!(done["resumed"][0]["id"], first["deletion"]["id"]);
        assert_eq!(done["resumed"][0]["state"], "completed");
        assert!(remote.under("acme/docs").is_empty());
        assert!(!state.data_dir.join("acme/docs").exists());
    }

    /// A write that is only buffered (nothing published, nothing in the
    /// control store) blocks the retry as well.
    #[test]
    fn resume_keeps_a_buffered_write_to_the_new_incarnation() {
        let (state, remote) = state_with_remote();
        index(&state, "acme", "docs", &["old"]);
        remote.put("acme/docs/old-seg/postings.bin");
        remote.fail_deletes.store(true, Ordering::SeqCst);
        assert_eq!(delete_ns(&state, "acme", "docs").0, 200);
        remote.fail_deletes.store(false, Ordering::SeqCst);

        let id = NamespaceId("acme/docs".into());
        state
            .indexer
            .index_documents(
                id.clone(),
                vec![kosha_core::Document {
                    id: kosha_core::DocumentId("buffered".into()),
                    fields: vec![kosha_core::Field::text("body", "common buffered")],
                }],
            )
            .unwrap();
        let blocked = resume(&state);
        assert_eq!(blocked["pending"], 1, "{blocked}");
        assert_eq!(state.indexer.pending_doc_count(&id), 1);
        assert_eq!(remote.under("acme/docs").len(), 1);
    }

    #[test]
    fn deleting_a_tenant_removes_its_keys_namespaces_branches_and_stray_objects() {
        let (state, remote) = state_with_remote();
        index(&state, "acme", "docs", &["a", "b"]);
        index(&state, "acme", "notes", &["n"]);
        let (status, body) = call(
            &state,
            "acme",
            "POST /v1/namespaces/docs/branch",
            serde_json::json!({"name": "exp"}),
        );
        assert_eq!(status, 200, "{body}");
        index(&state, "globex", "docs", &["g"]);
        remote.put("acme/docs/seg-1/postings.bin");
        remote.put("acme/notes/seg-1/postings.bin");
        // Objects no registered namespace accounts for.
        remote.put("acme/abandoned/seg-9/postings.bin");
        remote.put("globex/docs/seg-1/postings.bin");
        remote.put("acme-eu/docs/seg-1/postings.bin");

        let keys = auth::KeyStore::with_keys(HashMap::new());
        let record = |key: &str, tenant: &str| kosha_control::ApiKeyRecord {
            key_hash: kosha_control::hash_api_key(key),
            tenant_id: tenant.to_string(),
            description: String::new(),
            scopes: kosha_control::Scopes::ALL,
            namespaces: None,
            expires_at: None,
            last_used_at: None,
            created_at: 0,
        };
        keys.replace_db_keys(vec![
            record("sk-1", "acme"),
            record("sk-2", "acme"),
            record("sk-3", "globex"),
        ]);
        let auth = auth::Auth::Keys(keys);

        // No confirmation, or the wrong one: refused, nothing changes.
        for body in [&b"{}"[..], br#"{"confirm": "globex"}"#, b""] {
            let (status, _) = parse(&handle_delete_tenant("acme", body, &state, &auth));
            assert_eq!(status, 400);
        }
        // Reserved ids are not tenants (`_wal` is the write-ahead log).
        let (status, _) = parse(&handle_delete_tenant(
            "_wal",
            br#"{"confirm": "_wal"}"#,
            &state,
            &auth,
        ));
        assert_eq!(status, 400);
        assert_eq!(search(&state, "acme", "docs").1, ["a", "b"]);
        assert!(listed(&state, "").is_empty());

        let (status, body) = parse(&handle_delete_tenant(
            "acme",
            br#"{"confirm": "acme"}"#,
            &state,
            &auth,
        ));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["tenant"], "acme");
        assert_eq!(body["completed"], true);
        assert_eq!(body["api_keys_deleted"], 2);
        assert_eq!(body["static_api_keys_remaining"], 0);
        assert_eq!(body["errors"].as_array().unwrap().len(), 0, "{body}");
        assert_eq!(body["deletion"]["scope"], "tenant");
        assert_eq!(body["deletion"]["subject"], "acme");
        assert_eq!(body["deletion"]["state"], "completed");
        // The stray objects; each namespace's own are on its record.
        assert_eq!(body["deletion"]["objects_deleted"], 1);
        let handled = body["namespaces"].as_array().unwrap();
        assert_eq!(handled.len(), 3, "{body}");
        assert!(handled.iter().all(|entry| entry["deleted"] == true));
        assert!(handled
            .iter()
            .any(|entry| entry["namespace"] == "exp" && entry["branch"] == true));

        // Keys: the tenant's are gone, others still resolve.
        let auth::Auth::Keys(keys) = &auth else {
            unreachable!()
        };
        let tenant_of = |key: &str| keys.lookup(key).ok().map(|p| p.tenant);
        assert_eq!(tenant_of("sk-1"), None);
        assert_eq!(tenant_of("sk-2"), None);
        assert_eq!(tenant_of("sk-3").as_deref(), Some("globex"));

        // Control plane, local disk and remote storage hold nothing of it.
        assert!(state
            .controller
            .lock()
            .unwrap()
            .list_namespaces()
            .iter()
            .all(|ns| !ns.0.starts_with("acme/")));
        assert!(!state.data_dir.join("acme").exists());
        assert!(remote.under("acme").is_empty());
        for ns in ["docs", "notes", "exp"] {
            assert_eq!(search(&state, "acme", ns).0, 404, "{ns}");
        }
        // Other tenants, including one whose id starts the same, are whole.
        assert_eq!(search(&state, "globex", "docs").1, ["g"]);
        assert_eq!(remote.under("globex").len(), 1);
        assert_eq!(remote.under("acme-eu").len(), 1);

        // One tenant record, and a child record per namespace.
        let records = listed(&state, "?tenant=acme");
        let tenant_id = body["deletion"]["id"].as_str().unwrap();
        assert_eq!(records.len(), 3, "{records:?}");
        assert!(records.iter().all(|r| r["state"] == "completed"));
        let children: Vec<&serde_json::Value> = records
            .iter()
            .filter(|r| r["scope"] == "namespace")
            .collect();
        assert_eq!(children.len(), 2);
        assert!(children.iter().all(|r| r["parent_id"] == tenant_id));
    }

    #[test]
    fn tenant_deletion_stays_pending_until_keys_and_storage_are_really_gone() {
        let (state, remote) = state_with_remote();

        // A key from the environment cannot be deleted at runtime.
        index(&state, "acme", "docs", &["a"]);
        remote.put("acme/docs/seg-1/postings.bin");
        let with_env_key = auth::Auth::Keys(auth::KeyStore::with_keys(HashMap::from([(
            "sk-env".to_string(),
            "acme".to_string(),
        )])));
        let confirm = br#"{"confirm": "acme"}"#;
        let (status, body) = parse(&handle_delete_tenant(
            "acme",
            confirm,
            &state,
            &with_env_key,
        ));
        assert_eq!(status, 202, "{body}");
        assert_eq!(body["completed"], false);
        assert_eq!(body["static_api_keys_remaining"], 1);
        assert_eq!(body["deletion"]["state"], "pending");
        assert!(body["deletion"]["last_error"]
            .as_str()
            .unwrap()
            .contains("KOSHA_API_KEYS"));
        // The data went all the same.
        assert_eq!(body["namespaces"][0]["state"], "completed");
        assert!(remote.under("acme").is_empty());
        // Once the key is out of the environment, repeating the request
        // continues the same record.
        let (status, retried) = parse(&handle_delete_tenant("acme", confirm, &state, &no_keys()));
        assert_eq!(status, 200, "{retried}");
        assert_eq!(retried["deletion"]["id"], body["deletion"]["id"]);
        assert_eq!(retried["deletion"]["state"], "completed");
        assert_eq!(retried["deletion"]["attempts"], 2);

        // Storage down: namespace and tenant records both stay pending,
        // and one resume finishes both, the namespace first.
        index(&state, "globex", "docs", &["g"]);
        remote.put("globex/docs/seg-1/postings.bin");
        remote.put("globex/stray/seg-1/postings.bin");
        remote.fail_deletes.store(true, Ordering::SeqCst);
        let (status, body) = parse(&handle_delete_tenant(
            "globex",
            br#"{"confirm": "globex"}"#,
            &state,
            &no_keys(),
        ));
        assert_eq!(status, 202, "{body}");
        assert_eq!(body["namespaces"][0]["state"], "pending");
        assert_eq!(listed(&state, "?state=pending&tenant=globex").len(), 2);
        assert_eq!(remote.under("globex").len(), 2);

        remote.fail_deletes.store(false, Ordering::SeqCst);
        let (status, done) = parse(&handle_resume_deletions(b"", &state, &no_keys()));
        assert_eq!(status, 200, "{done}");
        assert_eq!(done["pending"], 0, "{done}");
        let resumed = done["resumed"].as_array().unwrap();
        assert_eq!(resumed.len(), 2);
        assert_eq!(resumed[0]["scope"], "namespace");
        assert_eq!(resumed[1]["scope"], "tenant");
        assert!(resumed.iter().all(|r| r["state"] == "completed"));
        assert!(remote.under("globex").is_empty());
        assert!(listed(&state, "?state=pending").is_empty());
    }

    /// The tenant-wide sweep runs under no namespace lock. A namespace
    /// registered while it runs must keep the tenant record pending, and
    /// the next pass deletes it.
    #[test]
    fn a_namespace_created_during_the_tenant_sweep_keeps_the_record_pending() {
        let (state, remote) = state_with_remote();
        index(&state, "acme", "docs", &["a"]);
        remote.put("acme/docs/seg-1/postings.bin");
        let state = Arc::new(state);
        let racing = Arc::downgrade(&state);
        *remote.on_delete.lock().unwrap() = Some(Box::new(move |prefix| {
            // Only the tenant-wide sweep, not the namespace's own reclaim.
            if prefix != "acme" {
                return;
            }
            let state = racing.upgrade().unwrap();
            let late = NamespaceId("acme/late".into());
            state.controller.lock().unwrap().ensure_namespace(late);
        }));

        let (status, body) = parse(&handle_delete_tenant(
            "acme",
            br#"{"confirm": "acme"}"#,
            &state,
            &no_keys(),
        ));
        assert_eq!(status, 202, "{body}");
        assert_eq!(body["completed"], false);
        assert_eq!(body["deletion"]["state"], "pending");
        assert!(
            body["deletion"]["last_error"]
                .as_str()
                .unwrap()
                .contains("created while its prefix was being swept (late)"),
            "{body}"
        );

        *remote.on_delete.lock().unwrap() = None;
        let (status, done) = parse(&handle_resume_deletions(b"", &state, &no_keys()));
        assert_eq!(status, 200, "{done}");
        assert_eq!(done["pending"], 0, "{done}");
        assert_eq!(done["resumed"][0]["id"], body["deletion"]["id"]);
        assert!(state
            .controller
            .lock()
            .unwrap()
            .list_namespaces()
            .iter()
            .all(|ns| !ns.0.starts_with("acme/")));
        assert_eq!(listed(&state, "?tenant=acme&state=completed").len(), 3);
    }

    /// A pre-#262 namespace named like the tenant keeps its segments right
    /// under `{tenant}/`: the tenant's prefix must not be swept.
    #[test]
    fn tenant_deletion_refuses_while_an_unscoped_namespace_shares_the_prefix() {
        let (state, remote) = state_with_remote();
        state
            .controller
            .lock()
            .unwrap()
            .ensure_namespace(NamespaceId("acme".into()));
        remote.put("acme/seg-1/postings.bin");
        let (status, body) = parse(&handle_delete_tenant(
            "acme",
            br#"{"confirm": "acme"}"#,
            &state,
            &no_keys(),
        ));
        assert_eq!(status, 409, "{body}");
        assert_eq!(remote.under("acme").len(), 1);
        assert!(listed(&state, "").is_empty());
    }

    #[test]
    fn deletion_admin_routes_need_the_admin_credential_and_run_on_the_writer() {
        let (state, _remote) = state_with_remote();
        index(&state, "acme", "docs", &["a"]);
        for line in [
            "DELETE /v1/admin/tenants/acme",
            "GET /v1/admin/deletions",
            "GET /v1/admin/deletions/del-1",
            "POST /v1/admin/deletions/resume",
        ] {
            let response = route(
                &format!("{line} HTTP/1.1\r\n"),
                &HashMap::from([("authorization".to_string(), "Bearer tenant-key".to_string())]),
                br#"{"confirm": "acme"}"#,
                "acme",
                &state,
            );
            assert!(response.starts_with("HTTP/1.1 403"), "{line}: {response}");
        }
        assert_eq!(search(&state, "acme", "docs").1, ["a"]);

        // A query pod forwards the mutating ones to the ingest pod.
        assert!(is_write_path("DELETE /v1/admin/tenants/acme HTTP/1.1"));
        assert!(is_write_path("POST /v1/admin/deletions/resume HTTP/1.1"));
        assert!(!is_write_path("GET /v1/admin/deletions HTTP/1.1"));
        assert!(!role::Role::indexer_serves(
            "DELETE /v1/admin/tenants/acme HTTP/1.1"
        ));

        // With the credential the route reaches the handler.
        let (status, body) = call(
            &state,
            SERVICE_TENANT,
            "DELETE /v1/admin/tenants/acme",
            serde_json::json!({"confirm": "acme"}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["completed"], true);
        assert_eq!(search(&state, "acme", "docs").0, 404);
    }
}
