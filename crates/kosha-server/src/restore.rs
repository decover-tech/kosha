//! Point-in-time namespace restore (#356):
//! `POST /v1/admin/restore-namespace`.
//!
//! A backup of a namespace is a manifest (the `kosha.manifests` row as of
//! the restore point, read from a Postgres instance recovered to that
//! time) plus the segment and deletion-bitmap objects it references (kept
//! by S3 versioning and the GC grace). This endpoint takes that manifest,
//! verifies that every object it references is present and complete in
//! object storage, and commits it as a new namespace, `target`, the same
//! way a branch is created: no segment bytes move, and segment GC keeps
//! every object the restored manifest references.
//!
//! The restore never writes to the namespace it was taken from, so it
//! cannot clobber live data; an operator who wants the old name back
//! renames afterwards. See `docs/operations/backup-restore.md`.

use super::*;
use crate::segment_commit::{assess_listing, Durability, SegmentCommit, COMMIT_FILE};
use kosha_core::Manifest;

/// Problems reported per request before the list is truncated.
const MAX_REPORTED: usize = 50;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreRequest {
    /// The namespace the manifest belongs to (`<tenant>/<name>`). Its
    /// objects are read in place.
    namespace: String,
    /// The namespace to create (`<tenant>/<name>`). Must not exist.
    target: String,
    /// The manifest to restore: the JSON object, or the `segments_json`
    /// text of its `kosha.manifests` row.
    manifest: serde_json::Value,
    /// The row's `version` column, when `manifest` is its `segments_json`
    /// (which may not carry it).
    #[serde(default)]
    version: Option<u64>,
    /// Verify only; commit nothing.
    #[serde(default)]
    dry_run: bool,
}

/// What a restore checked in object storage.
#[derive(Debug, Default, PartialEq, serde::Serialize)]
pub(crate) struct Verification {
    pub segments: usize,
    pub documents: u64,
    pub deletion_vectors: usize,
    /// Objects the manifest references that are missing or incomplete,
    /// at most [`MAX_REPORTED`].
    pub missing: Vec<String>,
    /// How many problems were found in total.
    pub missing_total: usize,
}

impl Verification {
    fn report(&mut self, problem: String) {
        self.missing_total += 1;
        if self.missing.len() < MAX_REPORTED {
            self.missing.push(problem);
        }
    }

    pub(crate) fn restorable(&self) -> bool {
        self.missing_total == 0
    }
}

/// Check that every object `manifest` (of `ns`) references is in `store`:
/// each segment passes the same durability check a cold read applies
/// (`_COMMIT` sizes, or every required file for pre-marker segments), and
/// each deletion bitmap exists.
pub(crate) fn verify_objects(
    store: &dyn StorageBackend,
    ns: &NamespaceId,
    manifest: &Manifest,
) -> Result<Verification, KoshaError> {
    let mut found = Verification {
        segments: manifest.segments.len(),
        documents: manifest.segments.iter().map(|e| e.doc_count as u64).sum(),
        deletion_vectors: manifest.deletion_vectors.len(),
        ..Verification::default()
    };
    for entry in &manifest.segments {
        let dir = manifest.segment_rel_dir(ns, &entry.segment_id);
        let prefix = format!("{dir}/");
        let listing: Vec<(String, u64)> = store
            .list_objects(&prefix)?
            .into_iter()
            .filter_map(|o| {
                let name = o.path.strip_prefix(&prefix)?;
                (!name.contains('/')).then(|| (name.to_string(), o.size))
            })
            .collect();
        let commit = listing.iter().any(|(n, _)| n == COMMIT_FILE).then(|| {
            store
                .read(&format!("{prefix}{COMMIT_FILE}"))
                .map_err(|e| e.to_string())
                .and_then(|bytes| {
                    serde_json::from_slice::<SegmentCommit>(&bytes).map_err(|e| e.to_string())
                })
        });
        let commit = commit.as_ref().map(|r| r.as_ref().map_err(String::as_str));
        if let Durability::Incomplete(reason) = assess_listing(&listing, commit) {
            found.report(format!("{dir}: {reason}"));
        }
    }
    let mut vectors: Vec<_> = manifest.deletion_vectors.iter().collect();
    vectors.sort_by(|a, b| a.0 .0.cmp(&b.0 .0));
    for (segment, vector) in vectors {
        let path = vector.path(ns, segment)?;
        if !store.exists(&path) {
            found.report(format!("{path}: missing"));
        }
    }
    Ok(found)
}

/// `<tenant>/<name>` with both components valid for a new namespace.
fn scoped_id(raw: &str) -> Option<NamespaceId> {
    let (tenant, name) = raw.split_once('/')?;
    (valid_name_component(tenant) && valid_new_namespace_name(name))
        .then(|| NamespaceId(raw.to_string()))
}

fn parse_manifest(value: serde_json::Value) -> Result<Manifest, String> {
    match value {
        serde_json::Value::String(text) => serde_json::from_str(&text),
        other => serde_json::from_value(other),
    }
    .map_err(|e| format!("'manifest' is not a Kosha manifest: {e}"))
}

impl AppState {
    /// Verify `manifest` (of `source`) and, unless `dry_run`, commit it as
    /// the new namespace `target`. Holds `branch_lock` throughout, so
    /// segment GC cannot reclaim an object between the check and the
    /// commit. `Err` is a ready-to-send JSON error response.
    pub(crate) fn restore_namespace(
        &self,
        source: &NamespaceId,
        manifest: &Manifest,
        target: &NamespaceId,
        dry_run: bool,
    ) -> Result<(Verification, Option<Manifest>), String> {
        let _lineage = self.branch_lock.lock().unwrap();
        let verification = self
            .with_gc_store(|store| verify_objects(store, source, manifest))
            .map_err(|e| {
                api_error(
                    ErrorCode::Unavailable,
                    &format!("object store listing failed: {e}"),
                )
            })?;
        if dry_run {
            return Ok((verification, None));
        }
        if !verification.restorable() {
            return Err(api_error(
                ErrorCode::Conflict,
                &format!(
                    "{} object(s) the manifest references are missing or incomplete; \
                     restore them (S3 object versions) and retry: {}",
                    verification.missing_total,
                    verification.missing.join(", ")
                ),
            ));
        }
        let restored = self.commit_branch(source, manifest, target)?;
        Ok((verification, Some(restored)))
    }
}

/// POST /v1/admin/restore-namespace — restore a namespace from a backed-up
/// manifest into a new namespace.
///
/// Request body: `{"namespace": "<tenant>/<name>", "target":
/// "<tenant>/<new name>", "manifest": {...} | "<segments_json>",
/// "version": 42, "dry_run": false}`. `dry_run` only verifies the objects.
pub(crate) fn handle_restore_namespace(body: &[u8], state: &AppState) -> String {
    let req: RestoreRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => return api_error(ErrorCode::InvalidArgument, &format!("invalid request: {e}")),
    };
    let Some(source) = req
        .namespace
        .split_once('/')
        .filter(|(t, n)| valid_name_component(t) && valid_namespace_component(n))
        .map(|_| NamespaceId(req.namespace.clone()))
    else {
        return api_error(
            ErrorCode::InvalidArgument,
            "'namespace' must be a scoped id: <tenant>/<name>",
        );
    };
    let Some(target) = scoped_id(&req.target) else {
        return api_error(
            ErrorCode::InvalidArgument,
            "'target' must be a new scoped id: <tenant>/<name>",
        );
    };
    if target == source {
        return api_error(
            ErrorCode::InvalidArgument,
            "'target' must differ from 'namespace'; a restore never overwrites a namespace",
        );
    }
    let mut manifest = match parse_manifest(req.manifest) {
        Ok(m) => m,
        Err(e) => return api_error(ErrorCode::InvalidArgument, &e),
    };
    if let Some(version) = req.version {
        manifest.version = version;
    }
    if manifest.shards.is_some_and(|n| n > 1) {
        return api_error(
            ErrorCode::InvalidArgument,
            "sharded namespaces cannot be restored yet",
        );
    }
    if !req.dry_run {
        if let Err(e) = check_namespace_quota(state, &target) {
            return e;
        }
    }
    match state.restore_namespace(&source, &manifest, &target, req.dry_run) {
        Ok((verification, restored)) => {
            if let Some(ref m) = restored {
                println!(
                    "[kosha] namespace restored: {} -> {} (manifest v{}, {} segments)",
                    source.0,
                    target.0,
                    manifest.version,
                    m.segments.len()
                );
            }
            json_ok(&serde_json::json!({
                "namespace": source.0,
                "target": target.0,
                "dry_run": req.dry_run,
                "restored": restored.is_some(),
                "restorable": verification.restorable(),
                "manifest_version": manifest.version,
                "verification": verification,
            }))
        }
        Err(response) => response,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::Path;

    fn test_state() -> AppState {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kosha-test-restore-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        AppState::new(dir)
    }

    fn parts(resp: &str) -> (u16, serde_json::Value) {
        let status: u16 = resp[9..12].parse().unwrap();
        let body = &resp[resp.find("\r\n\r\n").unwrap() + 4..];
        (status, serde_json::from_str(body).unwrap())
    }

    fn call(state: &AppState, line: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
        parts(&route(
            &format!("{line} HTTP/1.1\r\n"),
            &HashMap::new(),
            &serde_json::to_vec(&body).unwrap(),
            "acme",
            state,
        ))
    }

    fn restore(state: &AppState, body: serde_json::Value) -> (u16, serde_json::Value) {
        parts(&handle_restore_namespace(
            &serde_json::to_vec(&body).unwrap(),
            state,
        ))
    }

    fn ok(state: &AppState, line: &str, body: serde_json::Value) {
        let (status, body) = call(state, line, body);
        assert_eq!(status, 200, "{body}");
    }

    fn index(state: &AppState, ns: &str, ids: &[&str]) {
        let documents: Vec<_> = ids
            .iter()
            .map(|id| {
                serde_json::json!({"id": id, "fields": [
                    {"name": "body", "field_type": "Text", "value": format!("common {id}")},
                    {"name": "tag", "field_type": "Keyword", "value": id}]})
            })
            .collect();
        let path = format!("POST /v1/namespaces/{ns}");
        ok(
            state,
            &format!("{path}/documents"),
            serde_json::json!({ "documents": documents }),
        );
        ok(state, &format!("{path}/flush"), serde_json::json!({}));
    }

    fn delete(state: &AppState, ns: &str, id: &str) {
        let path = format!("POST /v1/namespaces/{ns}");
        ok(
            state,
            &format!("{path}/delete"),
            serde_json::json!({"filter": {"term": {"tag": id}}}),
        );
        ok(state, &format!("{path}/flush"), serde_json::json!({}));
    }

    fn ids(state: &AppState, ns: &str) -> Vec<String> {
        let (status, body) = call(
            state,
            &format!("POST /v1/namespaces/{ns}/search"),
            serde_json::json!({"query_text": "common", "top_k": 50, "no_cache": true}),
        );
        assert_eq!(status, 200, "{body}");
        let mut ids: Vec<String> = body["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["doc_id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        ids
    }

    /// Every file under `root/prefix`, with its bytes.
    fn tree(root: &Path, prefix: &str) -> BTreeMap<String, Vec<u8>> {
        let mut out = BTreeMap::new();
        let mut stack = vec![root.join(prefix)];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    let rel = path.strip_prefix(root).unwrap().to_string_lossy();
                    out.insert(rel.into_owned(), std::fs::read(&path).unwrap());
                }
            }
        }
        out
    }

    fn collect_garbage(state: &AppState, ns: &NamespaceId) {
        let config = kosha_core::gc::GcConfig {
            dry_run: false,
            grace_ms: 0,
            orphan_grace_ms: 0,
            now_ms: u64::MAX / 2,
            referenced_elsewhere: None,
        };
        let reports = state.run_segment_gc(std::slice::from_ref(ns), &config);
        assert!(reports[0].aborted.is_none(), "{:?}", reports[0].aborted);
    }

    /// The drill the runbook describes, end to end on local storage: take
    /// the manifest as of a point in time, let the namespace move on and
    /// GC reclaim what that manifest referenced, bring the objects back
    /// (what S3 versioning does), restore into a new namespace, and read
    /// exactly the documents of that point in time.
    #[test]
    fn restores_a_namespace_to_a_point_in_time() {
        let state = test_state();
        let prod = NamespaceId("acme/prod".into());
        index(&state, "prod", &["a", "b", "c"]);
        delete(&state, "prod", "c");
        // The restore point: the `kosha.manifests` row and the objects.
        let at = state
            .controller
            .lock()
            .unwrap()
            .manifest_cloned(&prod)
            .unwrap();
        assert!(!at.deletion_vectors.is_empty());
        let objects = tree(&state.data_dir, "acme/prod");
        let row = serde_json::json!({
            "namespace": "acme/prod",
            "target": "acme/prod-restored",
            "manifest": serde_json::to_string(&at).unwrap(),
            "version": at.version,
        });

        // Later writes, a compaction, and GC with no grace remove every
        // object the restore point referenced.
        index(&state, "prod", &["d"]);
        delete(&state, "prod", "a");
        compaction_pass(&state, &prod, kosha_write::CompactOptions::full(), "test").unwrap();
        collect_garbage(&state, &prod);
        assert_eq!(ids(&state, "prod"), ["b", "d"]);

        let mut dry_run = row.clone();
        dry_run["dry_run"] = true.into();
        let (status, body) = restore(&state, dry_run.clone());
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["restorable"], false, "{body}");
        assert_eq!(body["restored"], false);
        let missing = body["verification"]["missing_total"].as_u64().unwrap();
        assert!(missing > 0, "{body}");

        // A restore that would read missing objects is refused, and
        // creates nothing.
        let (status, body) = restore(&state, row.clone());
        assert_eq!(status, 409, "{body}");
        let (status, _) = call(
            &state,
            "POST /v1/namespaces/prod-restored/search",
            serde_json::json!({"query_text": "common"}),
        );
        assert_eq!(status, 404);

        // The bucket's noncurrent versions are restored.
        for (rel, bytes) in &objects {
            let path = state.data_dir.join(rel);
            if !path.exists() {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, bytes).unwrap();
            }
        }
        let (status, body) = restore(&state, dry_run);
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["restorable"], true, "{body}");
        assert_eq!(body["verification"]["segments"], at.segments.len());

        let (status, body) = restore(&state, row.clone());
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["restored"], true, "{body}");
        assert_eq!(body["manifest_version"], at.version);
        assert_eq!(ids(&state, "prod-restored"), ["a", "b"]);
        assert_eq!(ids(&state, "prod"), ["b", "d"], "the source is untouched");

        // The restored namespace keeps its objects alive through the
        // source's GC, and takes writes of its own.
        collect_garbage(&state, &prod);
        assert_eq!(ids(&state, "prod-restored"), ["a", "b"]);
        index(&state, "prod-restored", &["e"]);
        assert_eq!(ids(&state, "prod-restored"), ["a", "b", "e"]);
        assert_eq!(ids(&state, "prod"), ["b", "d"]);

        // Never over an existing namespace.
        let (status, body) = restore(&state, row);
        assert_eq!(status, 409, "{body}");
        assert_eq!(body["code"], "already_exists", "{body}");
    }

    #[test]
    fn refuses_invalid_restore_requests() {
        let state = test_state();
        index(&state, "prod", &["a"]);
        let manifest = state
            .controller
            .lock()
            .unwrap()
            .manifest_cloned(&NamespaceId("acme/prod".into()))
            .unwrap();
        let manifest = serde_json::to_value(&manifest).unwrap();
        let cases = [
            serde_json::json!({"namespace": "acme/prod", "target": "acme/prod", "manifest": manifest}),
            serde_json::json!({"namespace": "prod", "target": "acme/copy", "manifest": manifest}),
            serde_json::json!({"namespace": "acme/prod", "target": "copy", "manifest": manifest}),
            serde_json::json!({"namespace": "acme/prod", "target": "acme/a/b", "manifest": manifest}),
            serde_json::json!({"namespace": "acme/prod", "target": "acme/copy", "manifest": [1]}),
            serde_json::json!({"namespace": "acme/prod", "target": "acme/copy"}),
            serde_json::json!({"namespace": "acme/prod", "target": "acme/copy",
                "manifest": manifest, "overwrite": true}),
        ];
        for case in cases {
            let (status, body) = restore(&state, case.clone());
            assert_eq!(status, 400, "{case}: {body}");
        }
        // The manifest object form restores too.
        let (status, body) = restore(
            &state,
            serde_json::json!({"namespace": "acme/prod", "target": "acme/copy", "manifest": manifest}),
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(ids(&state, "copy"), ["a"]);
    }

    #[test]
    fn restore_is_admin_only() {
        let state = test_state();
        let (status, _) = call(
            &state,
            "POST /v1/admin/restore-namespace",
            serde_json::json!({}),
        );
        assert_eq!(status, 403);
    }
}
