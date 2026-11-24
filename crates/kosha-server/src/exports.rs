//! Namespace exports (#347): give a customer back everything a namespace
//! stores. Opt-in: `KOSHA_EXPORTS=on`.
//!
//! - `POST   /v1/namespaces/{ns}/exports` `{"include_vectors"?: bool,
//!   "upload_url"?: "https://..."}` starts one (202), *read* scope.
//! - `GET    /v1/namespaces/{ns}/exports` lists the namespace's exports.
//! - `GET    /v1/namespaces/{ns}/exports/{id}` one export; once completed
//!   on S3 it carries a time-limited presigned `GET` URL per part.
//! - `GET    /v1/namespaces/{ns}/exports/{id}/download[?part=N]` streams
//!   one part through the API.
//! - `DELETE /v1/namespaces/{ns}/exports/{id}` deletes it now.
//!
//! **Format.** Gzip-compressed JSON Lines, one document per line, in the
//! index API's own JSON form (`{"id", "fields": [{"name", "field_type",
//! "value"}]}`), so a line can be sent back to
//! `POST /v1/namespaces/{ns}/documents` unchanged. Vectors are included as
//! the `Vector` fields they were sent as (`"include_vectors": false` drops
//! them). An export is one or more *parts* of at most
//! `KOSHA_EXPORT_PART_BYTES` of JSON each; every part is a complete gzip
//! member, so the parts concatenated in order are one valid `.jsonl.gz`
//! (that is what the customer upload receives). Parquet is not offered:
//! the `parquet`/`arrow` crates would be the largest dependency tree in
//! the server, for a format a customer can produce from these lines.
//!
//! **Consistency.** Starting an export flushes and publishes the namespace
//! (so every acknowledged write is in it), then pins the manifest it will
//! read (each shard's, for a sharded namespace) on the export record. Until
//! the export finishes, segment GC and the local GC keep every object a
//! pinned manifest references ([`AppState::pin_export_references`]), so a
//! compaction that retires a segment mid-export cannot take it away. The
//! export reads exactly the pinned versions, deletes applied.
//!
//! **Jobs.** The record is durable (`kosha.exports`). A writer pod claims
//! it and runs it, refreshing the claim every 30 s on a timer of its own
//! (writing and delivery alike); if the pod dies, another writer claims it
//! after 10 minutes and runs it again from the start against the same pins
//! (restartable, not resumable). Each attempt writes under its own prefix
//! (`attempt-{n}/`), and a run that lost its claim removes only its own
//! files. A writer pod also expires completed exports after
//! `KOSHA_EXPORT_TTL_SECS` (objects deleted, state `expired`), and removes
//! failed and expired rows after 30 days. With `KOSHA_EXPORTS` off, a
//! writer pod fails the exports still pending or running at boot (their
//! pins go); reading, downloading and deleting exports keep working, only
//! starting one is refused.
//!
//! **Where it goes.** Parts live in object storage under
//! `_exports/{tenant}/{id}/attempt-{n}/` (on a pod without S3, under the
//! data directory), never under a namespace's prefix. An optional
//! customer-supplied presigned PUT URL receives the whole export: it must
//! be `https`, and every address its host resolves to must be public (no
//! loopback, private, link-local, CGNAT, multicast or reserved ranges, IPv4
//! or IPv6); the upload connects to the address that was checked (no
//! second DNS lookup) and follows no redirects. The URL is a credential: it
//! is stored only until the delivery ends and never returned.
//!
//! **Isolation and erasure.** An export is found by id only together with
//! the caller's tenant and the namespace in the path; anything else is a
//! 404. Deleting a namespace deletes its exports (objects and rows), and
//! deleting a tenant deletes all of its exports and the whole
//! `_exports/{tenant}/` prefix; the deletion record counts them
//! (`exports_deleted`).

use super::*;
use kosha_core::export::{
    export_dir, tenant_exports_dir, DeliveryState, ExportFilter, ExportPart, ExportRecord,
    ExportState, ExportUpdate, NewExport,
};
use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::RwLock;

/// How often a writer pod looks for expired and orphaned exports.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// Exports one pod runs at once.
const MAX_RUNNING: usize = 2;
/// How long failed and expired export rows are kept (their objects are
/// already gone) before the sweep removes them.
const RETENTION: Duration = Duration::from_secs(30 * 24 * 3600);
/// Largest page of `GET /v1/namespaces/{ns}/exports`.
const MAX_PAGE: u32 = 100;

/// `KOSHA_EXPORTS`, `KOSHA_EXPORT_TTL_SECS`, `KOSHA_EXPORT_PART_BYTES`,
/// `KOSHA_EXPORT_PRESIGN_SECS`, `KOSHA_EXPORT_MAX_ACTIVE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExportSettings {
    pub enabled: bool,
    pub ttl: Duration,
    pub part_bytes: u64,
    pub presign: Duration,
    /// Pending or running exports one tenant may have at once.
    pub max_active: usize,
    /// Tests only: allow `http://` and loopback upload URLs (a fake
    /// receiver on 127.0.0.1). Never set from the environment.
    pub allow_insecure_upload: bool,
    /// A running export's claim is refreshed this often, on a timer of its
    /// own (writing and delivery alike)...
    pub claim_refresh: Duration,
    /// ...and taken over by another writer once this old. Fixed in
    /// production; shortened in tests.
    pub claim_timeout: Duration,
}

impl Default for ExportSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            ttl: Duration::from_secs(7 * 24 * 3600),
            part_bytes: 256 * 1024 * 1024,
            presign: Duration::from_secs(900),
            max_active: 2,
            allow_insecure_upload: false,
            claim_refresh: Duration::from_secs(30),
            claim_timeout: Duration::from_secs(10 * 60),
        }
    }
}

impl ExportSettings {
    /// `Err` is the message to refuse to start with.
    pub(crate) fn from_env() -> Result<Self, String> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub(crate) fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let mut settings = Self::default();
        match get("KOSHA_EXPORTS").as_deref().map(str::trim) {
            None | Some("") | Some("off") | Some("0") | Some("false") => {}
            Some("on") | Some("1") | Some("true") => settings.enabled = true,
            Some(other) => return Err(format!("KOSHA_EXPORTS={other:?}: expected on or off")),
        }
        let number = |name: &str, min: u64| -> Result<Option<u64>, String> {
            match get(name).as_deref().map(str::trim) {
                None | Some("") => Ok(None),
                Some(raw) => match raw.parse::<u64>() {
                    Ok(n) if n >= min => Ok(Some(n)),
                    _ => Err(format!("{name}={raw:?}: expected a whole number >= {min}")),
                },
            }
        };
        if let Some(secs) = number("KOSHA_EXPORT_TTL_SECS", 60)? {
            settings.ttl = Duration::from_secs(secs);
        }
        if let Some(bytes) = number("KOSHA_EXPORT_PART_BYTES", 1024 * 1024)? {
            settings.part_bytes = bytes;
        }
        if let Some(secs) = number("KOSHA_EXPORT_PRESIGN_SECS", 1)? {
            // S3 caps a SigV4 presigned URL at seven days.
            settings.presign = Duration::from_secs(secs.min(7 * 24 * 3600));
        }
        if let Some(n) = number("KOSHA_EXPORT_MAX_ACTIVE", 1)? {
            settings.max_active = n as usize;
        }
        Ok(settings)
    }
}

/// One pod's export settings and counters.
#[derive(Default)]
pub(crate) struct ExportRuntime {
    settings: RwLock<ExportSettings>,
    running: AtomicUsize,
    started: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    expired: AtomicU64,
    deleted: AtomicU64,
    /// Set when the sweep thread runs (writer pods with exports on).
    sweeping: AtomicBool,
    /// Exports this process is running right now.
    active: Mutex<std::collections::HashSet<String>>,
}

impl ExportRuntime {
    /// Take one of this pod's [`MAX_RUNNING`] export slots.
    fn take_run_slot(&self) -> bool {
        let mut current = self.running.load(Ordering::SeqCst);
        loop {
            if current >= MAX_RUNNING {
                return false;
            }
            match self.running.compare_exchange(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(now) => current = now,
            }
        }
    }

    pub(crate) fn settings(&self) -> ExportSettings {
        self.settings.read().unwrap().clone()
    }

    pub(crate) fn configure(&self, settings: ExportSettings) {
        *self.settings.write().unwrap() = settings;
    }

    /// The `exports` block of `GET /v1/stats`.
    pub(crate) fn to_json(&self) -> serde_json::Value {
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
        serde_json::json!({
            "enabled": self.settings().enabled,
            "running": self.running.load(Ordering::Relaxed),
            "kosha_exports_started_total": load(&self.started),
            "kosha_exports_completed_total": load(&self.completed),
            "kosha_exports_failed_total": load(&self.failed),
            "kosha_exports_expired_total": load(&self.expired),
            "kosha_exports_deleted_total": load(&self.deleted),
        })
    }
}

// ─── Upload URL validation (SSRF) ─────────────────────────────────────────

/// A customer upload URL that passed validation (see [`crate::public_url`]).
pub(crate) type UploadTarget = crate::public_url::PublicUrl;

/// Validate a customer-supplied presigned PUT URL with
/// [`crate::public_url::validate_public_url`]: `https` only, no
/// credentials, every resolved address public. `allow_insecure` (tests
/// only) admits `http` and loopback.
pub(crate) fn validate_upload_url(raw: &str, allow_insecure: bool) -> Result<UploadTarget, String> {
    crate::public_url::validate_public_url(raw, "upload_url", allow_insecure)
}

// ─── Routes ───────────────────────────────────────────────────────────────

/// What an exports request addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExportRoute {
    Create(String),
    List(String),
    Get(String, String),
    Download(String, String),
    Delete(String, String),
}

/// Classify `METHOD /v1/namespaces/{ns}/exports[/{id}[/download]]`.
pub(crate) fn parse_route(method: &str, path: &str) -> Option<ExportRoute> {
    let bare = path.split('?').next().unwrap_or(path);
    let rest = bare.strip_prefix("/v1/namespaces/")?;
    let mut parts = rest.split('/');
    let ns = parts.next().filter(|ns| !ns.is_empty())?;
    if parts.next()? != "exports" {
        return None;
    }
    let ns = url_decode(ns);
    let id = parts.next().filter(|id| !id.is_empty()).map(url_decode);
    let tail = parts.next();
    if parts.next().is_some() {
        return None;
    }
    match (method, id, tail) {
        ("POST", None, None) => Some(ExportRoute::Create(ns)),
        ("GET", None, None) => Some(ExportRoute::List(ns)),
        ("GET", Some(id), None) => Some(ExportRoute::Get(ns, id)),
        ("GET", Some(id), Some("download")) => Some(ExportRoute::Download(ns, id)),
        ("DELETE", Some(id), None) => Some(ExportRoute::Delete(ns, id)),
        _ => None,
    }
}

fn route_of(request_line: &str) -> Option<ExportRoute> {
    let (method, path) = parse_method_and_path(request_line)?;
    parse_route(method, path)
}

/// The export routes that a query pod forwards to the ingest pod
/// (`is_write_path`): starting and deleting one.
pub(crate) fn is_write_route(request_line: &str) -> bool {
    matches!(
        route_of(request_line),
        Some(ExportRoute::Create(_) | ExportRoute::Delete(..))
    )
}

/// Whether this is the streaming download, which `dispatch` answers with a
/// file body instead of a `String`.
pub(crate) fn is_download_route(request_line: &str) -> bool {
    matches!(route_of(request_line), Some(ExportRoute::Download(..)))
}

/// Dispatch the export routes except the download; `None` for any other
/// request.
pub(crate) fn route(
    request_line: &str,
    body: &[u8],
    tenant: &str,
    state: &AppState,
) -> Option<String> {
    let found = route_of(request_line)?;
    // Off, only starting an export is refused: what exists stays readable
    // and, above all, deletable.
    if !state.exports.settings().enabled && matches!(found, ExportRoute::Create(_)) {
        return Some(json_error(
            404,
            "exports are not enabled on this deployment (KOSHA_EXPORTS)",
        ));
    }
    Some(match found {
        // `POST /v1/namespaces/{ns}/exports`
        ExportRoute::Create(ns) => handle_create(&ns, tenant, body, state),
        // `GET /v1/namespaces/{ns}/exports`
        ExportRoute::List(ns) => handle_list(&ns, request_line, tenant, state),
        // `GET /v1/namespaces/{ns}/exports/{id}`
        ExportRoute::Get(ns, id) => handle_get(&ns, &id, tenant, state),
        // `DELETE /v1/namespaces/{ns}/exports/{id}`
        ExportRoute::Delete(ns, id) => handle_delete(&ns, &id, tenant, state),
        // `GET /v1/namespaces/{ns}/exports/{id}/download` streams; see
        // `download_response`.
        ExportRoute::Download(..) => json_error(404, "not found"),
    })
}

fn accepted(value: &serde_json::Value) -> String {
    let body = value.to_string();
    format!(
        "HTTP/1.1 202 Accepted\r\ncontent-length: {}\r\ncontent-type: application/json\r\n\r\n{body}",
        body.len()
    )
}

/// The export `id` if it belongs to `tenant` and namespace `ns` (internal
/// id); a 404 response otherwise, so ids of other tenants read as absent.
fn owned_export(
    state: &AppState,
    tenant: &str,
    ns: &NamespaceId,
    id: &str,
) -> Result<ExportRecord, String> {
    let found = state.controller.lock().unwrap().export(id);
    match found {
        Ok(Some(record)) if record.tenant_id == tenant && record.namespace == ns.0 => Ok(record),
        Ok(_) => Err(json_error(404, &format!("export '{id}' not found"))),
        Err(e) => Err(json_error(503, &format!("control store: {e}"))),
    }
}

impl AppState {
    /// The pod-local path of an export object (staging on an S3 pod, the
    /// store itself without S3).
    fn export_local_path(&self, logical: &str) -> PathBuf {
        self.data_dir.join(logical)
    }

    /// The export as the API shows it: no pins, no upload URL; on S3, a
    /// presigned URL per part once completed.
    fn export_json(&self, record: &ExportRecord, ns_name: &str) -> serde_json::Value {
        let mut value = serde_json::to_value(record).unwrap_or(serde_json::Value::Null);
        if let Some(obj) = value.as_object_mut() {
            obj.insert("namespace".into(), serde_json::json!(ns_name));
            let parts: Vec<serde_json::Value> = record
                .parts
                .iter()
                .enumerate()
                .map(|(index, part)| {
                    let mut entry = serde_json::json!({
                        "part": index,
                        "bytes": part.bytes,
                        "documents": part.documents,
                        "download_path": format!(
                            "/v1/namespaces/{ns_name}/exports/{}/download?part={index}",
                            record.id
                        ),
                    });
                    if record.state == ExportState::Completed {
                        if let Some(url) = self.presign_export_part(&part.path) {
                            entry["download_url"] = serde_json::json!(url);
                        }
                    }
                    entry
                })
                .collect();
            obj.insert("parts".into(), serde_json::json!(parts));
        }
        value
    }

    #[cfg(feature = "s3")]
    fn presign_export_part(&self, path: &str) -> Option<String> {
        let s3 = self.s3_storage.as_ref()?;
        match s3.presign_get(path, self.exports.settings().presign) {
            Ok(url) => Some(url),
            Err(e) => {
                eprintln!("WARN: export: presigning {path} failed: {e}");
                None
            }
        }
    }

    #[cfg(not(feature = "s3"))]
    fn presign_export_part(&self, _path: &str) -> Option<String> {
        None
    }

    /// Pin the manifests of every active export into `refs`, so GC keeps
    /// what they read. Fallible on purpose, like the branch references: a
    /// GC that could not read the pins must not guess.
    pub(crate) fn pin_export_references(
        &self,
        refs: &mut kosha_core::branch::References,
    ) -> Result<(), String> {
        let active = self
            .controller
            .lock()
            .unwrap()
            .exports(&ExportFilter::active())
            .map_err(|e| format!("cannot read the pins of active exports: {e}"))?;
        for record in active {
            for (ns, manifest) in &record.pins {
                refs.add(ns, manifest)
                    .map_err(|e| format!("export {} pins {}: {e}", record.id, ns.0))?;
            }
        }
        Ok(())
    }
}

fn handle_create(name: &str, tenant: &str, body: &[u8], state: &AppState) -> String {
    let ns = NamespaceId(tenant_namespace(tenant, name));
    if state.follows_only() || state.role == role::Role::Indexer {
        return json_error(503, "exports run on the ingest (writer) pod");
    }
    let request: serde_json::Value = if body.iter().all(u8::is_ascii_whitespace) {
        serde_json::json!({})
    } else {
        match serde_json::from_slice(body) {
            Ok(value @ serde_json::Value::Object(_)) => value,
            _ => return json_error(400, "invalid JSON (expected an object)"),
        }
    };
    let include_vectors = match request.get("include_vectors") {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Bool(b)) => *b,
        Some(_) => return json_error(400, "'include_vectors' must be a boolean"),
    };
    let settings = state.exports.settings();
    let upload_url = match request.get("upload_url") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(raw)) => {
            if let Err(e) = validate_upload_url(raw, settings.allow_insecure_upload) {
                return json_error(400, &e);
            }
            Some(raw.clone())
        }
        Some(_) => return json_error(400, "'upload_url' must be a string"),
    };
    let active = state.controller.lock().unwrap().exports(&ExportFilter {
        tenant_id: Some(tenant.to_string()),
        ..ExportFilter::active()
    });
    match active {
        Ok(active) if active.len() >= settings.max_active => {
            return json_error(
                429,
                &format!(
                    "tenant has {} export(s) pending or running (KOSHA_EXPORT_MAX_ACTIVE={}); \
                     wait for one to finish",
                    active.len(),
                    settings.max_active
                ),
            )
        }
        Ok(_) => {}
        Err(e) => return json_error(503, &format!("control store: {e}")),
    }
    // Everything acknowledged so far is in the export.
    let stored = state.controller.lock().unwrap().try_manifest_cloned(&ns);
    match stored {
        Ok(Some(_)) => {}
        Ok(None) if state.indexer.manifest_version(&ns).is_some() => {}
        Ok(None) => return json_error(404, &format!("namespace '{name}' not found")),
        Err(e) => return json_error(503, &format!("control store: {e}")),
    }
    if let Err(response) = flush_and_publish_namespace(state, &ns) {
        return response;
    }
    // Pin under the branch lock, which GC holds while it reads every
    // reference set: a GC run sees either no pin and a manifest whose
    // segments are all live, or the pin.
    let created = {
        let _lineage = state.branch_lock.lock().unwrap();
        let mut pins = Vec::new();
        let root = state.controller.lock().unwrap().try_manifest_cloned(&ns);
        let root = match root {
            Ok(Some(root)) => root,
            Ok(None) => return json_error(404, &format!("namespace '{name}' not found")),
            Err(e) => return json_error(503, &format!("control store: {e}")),
        };
        if let Some(shards) = root.shards.filter(|&n| n > 1) {
            for shard in kosha_core::sharding::shard_namespaces(&ns, shards) {
                let manifest = state.controller.lock().unwrap().try_manifest_cloned(&shard);
                match manifest {
                    Ok(Some(manifest)) => pins.push((shard, manifest)),
                    Ok(None) => {}
                    Err(e) => return json_error(503, &format!("control store: {e}")),
                }
            }
        }
        pins.insert(0, (ns.clone(), root));
        let created = state.controller.lock().unwrap().create_export(&NewExport {
            tenant_id: tenant.to_string(),
            namespace: ns.0.clone(),
            include_vectors,
            pins,
            upload_url,
            ttl_ms: settings.ttl.as_millis() as u64,
        });
        match created {
            Ok(record) => record,
            Err(e) => return json_error(503, &format!("control store: {e}")),
        }
    };
    state.exports.started.fetch_add(1, Ordering::Relaxed);
    println!(
        "export started: id={} ns={} versions={:?}",
        created.id, ns.0, created.versions
    );
    state.spawn_export_run(&created.id);
    accepted(&state.export_json(&created, name))
}

/// `GET /v1/namespaces/{ns}/exports[?limit=N&before=<id>]`: newest first,
/// at most `limit` (default and cap [`MAX_PAGE`]); `next` is the `before`
/// of the following page, `null` on the last.
fn handle_list(name: &str, request_line: &str, tenant: &str, state: &AppState) -> String {
    let ns = NamespaceId(tenant_namespace(tenant, name));
    let path = parse_method_and_path(request_line).map_or("", |(_, path)| path);
    let limit = match namespace_deletion_query_param(path, "limit") {
        None => MAX_PAGE,
        Some(raw) => match raw.parse::<u32>() {
            Ok(n) if n > 0 => n.min(MAX_PAGE),
            _ => return json_error(400, "invalid 'limit' (a positive integer)"),
        },
    };
    let before = match namespace_deletion_query_param(path, "before") {
        None => None,
        Some(id) => match owned_export(state, tenant, &ns, &id) {
            Ok(record) => Some(ExportFilter::position(&record)),
            Err(response) => return response,
        },
    };
    let listed = state.controller.lock().unwrap().exports(&ExportFilter {
        tenant_id: Some(tenant.to_string()),
        namespace: Some(ns.0.clone()),
        limit: Some(limit),
        newest_first: true,
        before,
        ..ExportFilter::default()
    });
    match listed {
        Ok(records) => {
            let next = (records.len() == limit as usize)
                .then(|| records.last().map(|r| r.id.clone()))
                .flatten();
            json_ok(&serde_json::json!({
                "namespace": name,
                "exports": records
                    .iter()
                    .map(|record| state.export_json(record, name))
                    .collect::<Vec<_>>(),
                "next": next,
            }))
        }
        Err(e) => json_error(503, &format!("control store: {e}")),
    }
}

fn handle_get(name: &str, id: &str, tenant: &str, state: &AppState) -> String {
    let ns = NamespaceId(tenant_namespace(tenant, name));
    match owned_export(state, tenant, &ns, id) {
        Ok(record) => json_ok(&state.export_json(&record, name)),
        Err(response) => response,
    }
}

fn handle_delete(name: &str, id: &str, tenant: &str, state: &AppState) -> String {
    let ns = NamespaceId(tenant_namespace(tenant, name));
    let record = match owned_export(state, tenant, &ns, id) {
        Ok(record) => record,
        Err(response) => return response,
    };
    if let Err(e) = state.remove_export_objects(&record) {
        return json_error(503, &format!("deleting the export's objects failed: {e}"));
    }
    let update = ExportUpdate {
        state: Some(ExportState::Expired),
        clear_pins: true,
        clear_upload_url: true,
        parts: Some(Vec::new()),
        ..ExportUpdate::default()
    };
    let updated = state.controller.lock().unwrap().update_export(id, &update);
    match updated {
        Ok(Some(record)) => {
            state.exports.deleted.fetch_add(1, Ordering::Relaxed);
            json_ok(&state.export_json(&record, name))
        }
        Ok(None) => json_error(404, &format!("export '{id}' not found")),
        Err(e) => json_error(503, &format!("control store: {e}")),
    }
}

/// `GET /v1/namespaces/{ns}/exports/{id}/download[?part=N]`: one part as
/// `application/gzip`, streamed from a file.
pub(crate) fn download_response(
    request_line: &str,
    tenant: &str,
    state: &AppState,
) -> http::Response {
    let error = |status: u16, message: &str| http::Response::from_raw(json_error(status, message));
    let Some(ExportRoute::Download(name, id)) = route_of(request_line) else {
        return error(404, "not found");
    };
    let ns = NamespaceId(tenant_namespace(tenant, &name));
    let record = match owned_export(state, tenant, &ns, &id) {
        Ok(record) => record,
        Err(response) => return http::Response::from_raw(response),
    };
    if record.state != ExportState::Completed {
        return error(
            409,
            &format!("export '{id}' is {}, not completed", record.state.as_str()),
        );
    }
    let path = parse_method_and_path(request_line).map_or("", |(_, path)| path);
    let part = match namespace_deletion_query_param(path, "part") {
        None => 0,
        Some(raw) => match raw.parse::<usize>() {
            Ok(n) => n,
            Err(_) => return error(400, "invalid 'part' (a part number)"),
        },
    };
    let Some(part) = record.parts.get(part) else {
        return error(404, &format!("export '{id}' has no part {part}"));
    };
    match state.open_export_part(part) {
        Ok((file, len)) => http::Response::file(file, len, "application/gzip").with_header(
            "content-disposition",
            &format!(
                "attachment; filename=\"{}\"",
                part.path.rsplit('/').next().unwrap_or("export.jsonl.gz")
            ),
        ),
        Err(e) => error(503, &format!("reading the export failed: {e}")),
    }
}

fn namespace_deletion_query_param(path: &str, name: &str) -> Option<String> {
    let (_, query) = path.split_once('?')?;
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name && !value.is_empty()).then(|| url_decode(value))
    })
}

// ─── Running an export ────────────────────────────────────────────────────

/// Writes JSON lines into gzip parts of bounded size.
struct PartWriter {
    dir_logical: String,
    limit: u64,
    index: usize,
    current: Option<(
        String,
        flate2::write::GzEncoder<std::io::BufWriter<std::fs::File>>,
    )>,
    current_json: u64,
    current_docs: u64,
    done: Vec<(ExportPart, PathBuf)>,
    data_dir: PathBuf,
}

impl PartWriter {
    fn new(data_dir: &Path, dir_logical: String, limit: u64) -> std::io::Result<Self> {
        let local = data_dir.join(&dir_logical);
        // A previous attempt's leftovers start over.
        let _ = std::fs::remove_dir_all(&local);
        std::fs::create_dir_all(&local)?;
        Ok(Self {
            dir_logical,
            limit,
            index: 0,
            current: None,
            current_json: 0,
            current_docs: 0,
            done: Vec::new(),
            data_dir: data_dir.to_path_buf(),
        })
    }

    fn write_line(&mut self, line: &[u8]) -> std::io::Result<Option<(ExportPart, PathBuf)>> {
        if self.current.is_none() {
            let logical = format!("{}/part-{:05}.jsonl.gz", self.dir_logical, self.index);
            let file = std::fs::File::create(self.data_dir.join(&logical))?;
            let encoder = flate2::write::GzEncoder::new(
                std::io::BufWriter::new(file),
                flate2::Compression::default(),
            );
            self.current = Some((logical, encoder));
        }
        let (_, encoder) = self.current.as_mut().expect("a part is open");
        encoder.write_all(line)?;
        encoder.write_all(b"\n")?;
        self.current_json += line.len() as u64 + 1;
        self.current_docs += 1;
        if self.current_json >= self.limit {
            return self.finish_part();
        }
        Ok(None)
    }

    /// Close the open part, if any: the finished part and its local file.
    fn finish_part(&mut self) -> std::io::Result<Option<(ExportPart, PathBuf)>> {
        let Some((logical, encoder)) = self.current.take() else {
            return Ok(None);
        };
        let mut file = encoder.finish()?;
        file.flush()?;
        file.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        let local = self.data_dir.join(&logical);
        let bytes = std::fs::metadata(&local)?.len();
        let part = ExportPart {
            path: logical,
            bytes,
            documents: self.current_docs,
        };
        self.index += 1;
        self.current_json = 0;
        self.current_docs = 0;
        self.done.push((part.clone(), local.clone()));
        Ok(Some((part, local)))
    }
}

/// Why a run stopped early.
enum Stop {
    /// The export is gone or claimed elsewhere (deleted with its
    /// namespace, or taken over): clean up what this run wrote and leave
    /// the record alone.
    Lost,
    Failed(String),
}

impl From<std::io::Error> for Stop {
    fn from(e: std::io::Error) -> Self {
        Stop::Failed(format!("writing the export failed: {e}"))
    }
}

impl AppState {
    /// Run export `id` on a thread of its own, if this pod has room.
    pub(crate) fn spawn_export_run(&self, id: &str) {
        if !self.exports.active.lock().unwrap().insert(id.to_string()) {
            return;
        }
        let Some(state) = self.self_ref.upgrade() else {
            // Not shared (unit tests): run inline.
            self.run_export(id);
            self.exports.active.lock().unwrap().remove(id);
            return;
        };
        if !self.exports.take_run_slot() {
            // The sweep picks it up when a slot frees.
            self.exports.active.lock().unwrap().remove(id);
            return;
        }
        let id = id.to_string();
        std::thread::Builder::new()
            .name("kosha-export".into())
            .spawn(move || {
                state.run_export(&id);
                state.exports.running.fetch_sub(1, Ordering::SeqCst);
                state.exports.active.lock().unwrap().remove(&id);
            })
            .expect("failed to spawn export thread");
    }

    /// Claim and run one export to its end. Safe to call for an export
    /// another pod holds (the claim is refused) or that is finished.
    pub(crate) fn run_export(&self, id: &str) -> Option<ExportRecord> {
        let holder = &self.pod_id();
        let settings = self.exports.settings();
        let claimed = self.controller.lock().unwrap().claim_export(
            id,
            holder,
            settings.claim_timeout.as_millis() as u64,
        );
        let record = match claimed {
            Ok(Some(record)) => record,
            Ok(None) => return None,
            Err(e) => {
                eprintln!("WARN: export {id}: claiming failed: {e}");
                return None;
            }
        };
        // Each attempt writes under a prefix of its own, so a run that lost
        // its claim (and a pod that took it over) never share files: what
        // one discards is only ever its own.
        let dir_logical = format!(
            "{}/attempt-{}",
            export_dir(&record.tenant_id, &record.id),
            record.attempts
        );
        // The claim is refreshed on a timer of its own for the whole run,
        // writing and delivery alike, so a slow segment or a slow customer
        // endpoint never lets another writer take a live export over.
        let lost = AtomicBool::new(false);
        let finished_run = AtomicBool::new(false);
        let (outcome, delivery) = std::thread::scope(|scope| {
            scope.spawn(|| {
                self.keep_export_claim(&record.id, holder, &settings, &lost, &finished_run)
            });
            let outcome = self.write_export(&record, &dir_logical, holder, &lost);
            let delivery = match (&outcome, record.upload_url.as_deref()) {
                (Ok(parts), Some(url)) => Some(self.deliver_export(url, parts)),
                _ => None,
            };
            finished_run.store(true, Ordering::SeqCst);
            (outcome, delivery)
        });
        let guarded = |update: ExportUpdate| ExportUpdate {
            holder: Some(holder.to_string()),
            ..update
        };
        match outcome {
            Ok(parts) => {
                let documents = parts.iter().map(|(p, _)| p.documents).sum();
                let bytes = parts.iter().map(|(p, _)| p.bytes).sum();
                let now = unix_millis();
                let update = guarded(ExportUpdate {
                    state: Some(ExportState::Completed),
                    parts: Some(parts.iter().map(|(p, _)| p.clone()).collect()),
                    documents: Some(documents),
                    bytes: Some(bytes),
                    error: Some(None),
                    delivery_state: delivery.as_ref().map(|d| match d {
                        Ok(()) => DeliveryState::Delivered,
                        Err(_) => DeliveryState::Failed,
                    }),
                    delivery_error: delivery.as_ref().map(|d| d.as_ref().err().cloned()),
                    completed_at_ms: Some(now),
                    expires_at_ms: Some(now + record.ttl_ms),
                    clear_pins: true,
                    clear_upload_url: true,
                    ..ExportUpdate::default()
                });
                let finished = self.controller.lock().unwrap().update_export(id, &update);
                // Local staging goes once the parts are in object storage.
                if self.export_has_remote_store() {
                    let _ = std::fs::remove_dir_all(self.export_local_path(&dir_logical));
                }
                match finished {
                    Ok(Some(record)) => {
                        self.exports.completed.fetch_add(1, Ordering::Relaxed);
                        println!(
                            "export completed: id={id} ns={} documents={documents} bytes={bytes} \
                             parts={}",
                            record.namespace,
                            record.parts.len()
                        );
                        Some(record)
                    }
                    // Deleted (its namespace went) or taken over meanwhile.
                    Ok(None) => {
                        self.discard_run_objects(&record, &dir_logical);
                        None
                    }
                    Err(e) => {
                        eprintln!(
                            "WARN: export {id}: recording completion failed: {e}; it is retried \
                             once its claim times out"
                        );
                        None
                    }
                }
            }
            Err(Stop::Lost) => {
                self.discard_run_objects(&record, &dir_logical);
                None
            }
            Err(Stop::Failed(error)) => {
                eprintln!("WARN: export {id} failed: {error}");
                self.discard_run_objects(&record, &dir_logical);
                self.exports.failed.fetch_add(1, Ordering::Relaxed);
                let update = guarded(ExportUpdate {
                    state: Some(ExportState::Failed),
                    error: Some(Some(error)),
                    parts: Some(Vec::new()),
                    clear_pins: true,
                    clear_upload_url: true,
                    delivery_state: record.upload_url.as_ref().map(|_| DeliveryState::Failed),
                    ..ExportUpdate::default()
                });
                self.controller
                    .lock()
                    .unwrap()
                    .update_export(id, &update)
                    .ok()
                    .flatten()
            }
        }
    }

    fn export_has_remote_store(&self) -> bool {
        #[cfg(feature = "s3")]
        let remote = self.s3_storage.is_some();
        #[cfg(not(feature = "s3"))]
        let remote = false;
        remote
    }

    /// Refresh the claim of export `id` every `claim_refresh` until `done`;
    /// set `lost` (and stop) once the export is gone or another pod holds
    /// it.
    fn keep_export_claim(
        &self,
        id: &str,
        holder: &str,
        settings: &ExportSettings,
        lost: &AtomicBool,
        done: &AtomicBool,
    ) {
        let step = settings.claim_refresh.min(Duration::from_millis(50));
        let mut last = Instant::now();
        while !done.load(Ordering::SeqCst) {
            std::thread::sleep(step);
            if last.elapsed() < settings.claim_refresh {
                continue;
            }
            last = Instant::now();
            if self.refresh_export_claim(id, holder).is_err() {
                lost.store(true, Ordering::SeqCst);
                return;
            }
        }
    }

    /// Refresh this pod's claim; `Err(Stop::Lost)` once the export is gone
    /// or held by another pod.
    fn refresh_export_claim(&self, id: &str, holder: &str) -> Result<(), Stop> {
        let timeout = self.exports.settings().claim_timeout;
        let claimed =
            self.controller
                .lock()
                .unwrap()
                .claim_export(id, holder, timeout.as_millis() as u64);
        match claimed {
            Ok(Some(record)) if record.claimed_by.as_deref() == Some(holder) => Ok(()),
            Ok(_) => Err(Stop::Lost),
            // A control-store blip: keep going; completion is guarded.
            Err(_) => Ok(()),
        }
    }

    /// Read every pinned manifest and write the parts. Returns each part
    /// with its local file.
    fn write_export(
        &self,
        record: &ExportRecord,
        dir_logical: &str,
        holder: &str,
        lost: &AtomicBool,
    ) -> Result<Vec<(ExportPart, PathBuf)>, Stop> {
        if record.pins.is_empty() {
            return Err(Stop::Failed("the export lost its pinned manifests".into()));
        }
        let settings = self.exports.settings();
        let mut writer =
            PartWriter::new(&self.data_dir, dir_logical.to_string(), settings.part_bytes)?;
        for (ns, pinned) in &record.pins {
            let mut manifest = pinned.clone();
            self.hydrate_deletes(ns, &mut manifest)
                .map_err(|e| Stop::Failed(format!("reading the deletes of {}: {e}", ns.0)))?;
            for entry in &manifest.segments {
                if lost.load(Ordering::SeqCst) {
                    return Err(Stop::Lost);
                }
                let dir = manifest.segment_dir(&self.data_dir, ns, &entry.segment_id);
                #[cfg(feature = "s3")]
                {
                    let outcome = self
                        .ensure_segments_local(std::slice::from_ref(&dir), record.include_vectors);
                    if !outcome.missing.is_empty() {
                        return Err(Stop::Failed(format!(
                            "segment {} of {} could not be fetched",
                            entry.segment_id.0, ns.0
                        )));
                    }
                }
                let reader = kosha_segment::SegmentReader::open(dir).map_err(|e| {
                    Stop::Failed(format!(
                        "opening segment {} of {}: {e}",
                        entry.segment_id.0, ns.0
                    ))
                })?;
                let deleted = manifest.tombstones.get(&entry.segment_id);
                for doc in reader.iter_doc_records() {
                    let doc = doc.map_err(|e| {
                        Stop::Failed(format!(
                            "reading segment {} of {}: {e}",
                            entry.segment_id.0, ns.0
                        ))
                    })?;
                    if deleted.is_some_and(|d| d.contains(&doc.doc_seq)) {
                        continue;
                    }
                    let fields: Vec<kosha_core::Field> = doc
                        .fields
                        .into_iter()
                        .filter(|f| {
                            record.include_vectors || f.field_type != kosha_core::FieldType::Vector
                        })
                        .collect();
                    let line = serde_json::to_vec(&kosha_core::Document {
                        id: doc.doc_id,
                        fields,
                    })
                    .map_err(|e| Stop::Failed(format!("encoding a document: {e}")))?;
                    if let Some((part, local)) = writer.write_line(&line)? {
                        self.store_export_part(record, &part, &local, holder)?;
                    }
                }
            }
        }
        if let Some((part, local)) = writer.finish_part()? {
            self.store_export_part(record, &part, &local, holder)?;
        }
        if writer.done.is_empty() {
            // An empty namespace still yields one (empty) part, so a
            // download and a delivery always have something to send.
            let logical = format!("{dir_logical}/part-00000.jsonl.gz");
            let local = self.export_local_path(&logical);
            let encoder = flate2::write::GzEncoder::new(
                std::fs::File::create(&local)?,
                flate2::Compression::default(),
            );
            encoder.finish()?.sync_all()?;
            let part = ExportPart {
                bytes: std::fs::metadata(&local)?.len(),
                path: logical,
                documents: 0,
            };
            self.store_export_part(record, &part, &local, holder)?;
            writer.done.push((part, local));
        }
        Ok(writer.done)
    }

    /// Put a finished part into object storage, then check the export is
    /// still this pod's (a namespace deletion may have removed it while
    /// the part was written).
    fn store_export_part(
        &self,
        record: &ExportRecord,
        part: &ExportPart,
        local: &Path,
        holder: &str,
    ) -> Result<(), Stop> {
        #[cfg(feature = "s3")]
        if let Some(s3) = &self.s3_storage {
            s3.put_file_for_tenant(local, &part.path, &record.tenant_id)
                .map_err(|e| Stop::Failed(format!("uploading {}: {e}", part.path)))?;
        }
        let _ = (local, part);
        self.refresh_export_claim(&record.id, holder)
    }

    /// Remove what one attempt wrote (`dir_logical`, its own prefix):
    /// local files and, on S3, its objects. Never another attempt's.
    fn discard_run_objects(&self, record: &ExportRecord, dir_logical: &str) {
        if let Err(e) = self.remove_export_prefix(dir_logical) {
            eprintln!(
                "WARN: export {}: removing the objects of an abandoned run failed: {e}",
                record.id
            );
        }
    }

    /// Delete export `record`'s objects (local and remote) and verify none
    /// is left. Returns how many remote objects went.
    pub(crate) fn remove_export_objects(&self, record: &ExportRecord) -> Result<u64, String> {
        let dir = export_dir(&record.tenant_id, &record.id);
        self.remove_export_prefix(&dir)
    }

    /// Delete everything under the logical export directory `dir`, locally
    /// and in object storage, then list it again: only an empty listing
    /// counts.
    fn remove_export_prefix(&self, dir: &str) -> Result<u64, String> {
        match std::fs::remove_dir_all(self.export_local_path(dir)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!("removing {dir}/ locally: {e}")),
        }
        #[cfg(test)]
        if let Some(store) = &self.remote_prefix_override {
            let deleted = store
                .delete_prefix(dir)
                .map_err(|e| format!("deleting {dir}/: {e}"))?;
            return match store.count_prefix(dir) {
                Ok(0) => Ok(deleted as u64),
                Ok(left) => Err(format!("{left} object(s) remain under {dir}/")),
                Err(e) => Err(format!("could not verify that {dir}/ is empty: {e}")),
            };
        }
        #[cfg(feature = "s3")]
        if let Some(s3) = &self.s3_storage {
            use namespace_deletion::RemotePrefixStore;
            let deleted = RemotePrefixStore::delete_prefix(s3, dir)
                .map_err(|e| format!("deleting {dir}/: {e}"))?;
            return match RemotePrefixStore::count_prefix(s3, dir) {
                Ok(0) => Ok(deleted as u64),
                Ok(left) => Err(format!("{left} object(s) remain under {dir}/")),
                Err(e) => Err(format!("could not verify that {dir}/ is empty: {e}")),
            };
        }
        Ok(0)
    }

    /// Open a finished part for streaming: the local file, or (S3) a copy
    /// fetched into a temporary file that is unlinked once open.
    fn open_export_part(&self, part: &ExportPart) -> Result<(std::fs::File, u64), String> {
        let local = self.export_local_path(&part.path);
        if let Ok(file) = std::fs::File::open(&local) {
            let len = file.metadata().map_err(|e| e.to_string())?.len();
            return Ok((file, len));
        }
        #[cfg(feature = "s3")]
        if let Some(s3) = &self.s3_storage {
            let tmp_dir = self
                .data_dir
                .join(kosha_core::export::EXPORTS_PREFIX)
                .join("_downloads");
            std::fs::create_dir_all(&tmp_dir).map_err(|e| e.to_string())?;
            let tmp = tmp_dir.join(uuid_like());
            let fetched = s3.get_object_to_file(&part.path, &tmp);
            let opened = fetched.map_err(|e| e.to_string()).and_then(|len| {
                std::fs::File::open(&tmp)
                    .map(|file| (file, len))
                    .map_err(|e| e.to_string())
            });
            // The open handle keeps the bytes readable; the name goes now.
            let _ = std::fs::remove_file(&tmp);
            return opened;
        }
        Err(format!("{} is not available on this pod", part.path))
    }

    /// PUT the parts, concatenated (one valid `.jsonl.gz`), to the
    /// customer's presigned URL. The URL is validated again and the
    /// connection goes to the address that was checked.
    fn deliver_export(&self, url: &str, parts: &[(ExportPart, PathBuf)]) -> Result<(), String> {
        let settings = self.exports.settings();
        let target = validate_upload_url(url, settings.allow_insecure_upload)?;
        let len: u64 = parts.iter().map(|(p, _)| p.bytes).sum();
        let mut readers: Vec<std::fs::File> = Vec::with_capacity(parts.len());
        for (part, local) in parts {
            readers.push(
                std::fs::File::open(local).map_err(|e| format!("reopening {}: {e}", part.path))?,
            );
        }
        let body = Concat(readers.into_iter().collect());
        // No proxy, no redirect, the checked address only.
        let client = crate::public_url::pinned_client(&target, Duration::from_secs(3600))?;
        let response = client
            .put(target.url.clone())
            .body(reqwest::blocking::Body::sized(body, len))
            .send()
            .map_err(|e| format!("upload to {} failed: {}", target.host, e.without_url()))?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            Err(format!("upload to {} answered {status}", target.host))
        }
    }

    // ── Hard delete (#347) ───────────────────────────────────────────────

    /// Delete every export of namespace `ns` (see
    /// [`Self::delete_exports_where`]). Returns how many went for good, and
    /// why any is left.
    pub(crate) fn delete_namespace_exports(&self, ns: &NamespaceId) -> (u64, Option<String>) {
        let Some(tenant) = namespace_tenant(&ns.0) else {
            return (0, None);
        };
        self.delete_exports_where(ExportFilter {
            tenant_id: Some(tenant.to_string()),
            namespace: Some(ns.0.clone()),
            limit: Some(u32::MAX),
            ..ExportFilter::default()
        })
    }

    /// Delete every export of `tenant`, then the whole `_exports/{tenant}/`
    /// prefix (whatever an interrupted run left), verified empty.
    pub(crate) fn delete_tenant_exports(&self, tenant: &str) -> (u64, Option<String>) {
        let (deleted, error) = self.delete_exports_where(ExportFilter {
            tenant_id: Some(tenant.to_string()),
            limit: Some(u32::MAX),
            ..ExportFilter::default()
        });
        if error.is_some() {
            return (deleted, error);
        }
        (
            deleted,
            self.remove_export_prefix(&tenant_exports_dir(tenant)).err(),
        )
    }

    /// Delete the exports `filter` selects for good. In this order, so a
    /// failure never leaves files without a row to find them by:
    ///
    /// 1. every row is marked `expired` with its pins and upload URL
    ///    cleared (a run still writing sees its claim gone and removes
    ///    what it wrote itself);
    /// 2. each export's objects are deleted and the prefix listed empty;
    /// 3. only the rows whose objects are confirmed gone are deleted.
    ///
    /// A row whose objects could not be removed stays (expired), so the
    /// next attempt (a deletion retry) finds it again. Returns how many
    /// exports went for good, and the first reason one is left.
    fn delete_exports_where(&self, filter: ExportFilter) -> (u64, Option<String>) {
        let listed = self.controller.lock().unwrap().exports(&filter);
        let records = match listed {
            Ok(records) => records,
            Err(e) => return (0, Some(format!("control store: {e}"))),
        };
        if records.is_empty() {
            return (0, None);
        }
        let stop = ExportUpdate {
            state: Some(ExportState::Expired),
            parts: Some(Vec::new()),
            clear_pins: true,
            clear_upload_url: true,
            ..ExportUpdate::default()
        };
        for record in &records {
            let stopped = self
                .controller
                .lock()
                .unwrap()
                .update_export(&record.id, &stop);
            if let Err(e) = stopped {
                return (0, Some(format!("control store: {e}")));
            }
        }
        let mut first_error = None;
        let mut gone = Vec::new();
        for record in &records {
            match self.remove_export_objects(record) {
                Ok(_) => gone.push(record.id.clone()),
                Err(e) => {
                    first_error.get_or_insert(format!("export {}: {e}", record.id));
                }
            }
        }
        let removed = match self.controller.lock().unwrap().delete_exports(&gone) {
            Ok(removed) => removed,
            Err(e) => return (0, Some(format!("control store: {e}"))),
        };
        self.exports.deleted.fetch_add(removed, Ordering::Relaxed);
        (removed, first_error)
    }

    /// Remove failed and expired export rows not updated for [`RETENTION`].
    /// Their objects were deleted when they failed or expired; deleted
    /// again here in case that did not finish.
    fn prune_export_rows(&self, now_ms: u64) -> usize {
        let listed = self.controller.lock().unwrap().exports(&ExportFilter {
            states: vec![ExportState::Failed, ExportState::Expired],
            limit: Some(10_000),
            ..ExportFilter::default()
        });
        let Ok(records) = listed else {
            return 0;
        };
        let cutoff = now_ms.saturating_sub(RETENTION.as_millis() as u64);
        let old: Vec<String> = records
            .iter()
            .filter(|r| r.updated_at_ms < cutoff)
            .filter(|r| self.remove_export_objects(r).is_ok())
            .map(|r| r.id.clone())
            .collect();
        match self.controller.lock().unwrap().delete_exports(&old) {
            Ok(n) => n as usize,
            Err(e) => {
                eprintln!("WARN: export sweep: pruning old rows: {e}");
                0
            }
        }
    }

    // ── Background sweep ─────────────────────────────────────────────────

    /// Start the export sweep on a writer pod with exports on: expire
    /// completed exports and pick up exports whose pod died.
    pub(crate) fn spawn_exports(self: &Arc<Self>, settings: ExportSettings) {
        let enabled = settings.enabled;
        self.exports.configure(settings);
        let writer = !(self.follows_only() || self.role == role::Role::Indexer);
        if !enabled {
            // Nothing will run what is still pending or running: end it, so
            // its pins stop holding segments against GC for good.
            if writer {
                self.fail_active_exports("exports were turned off (KOSHA_EXPORTS)");
            }
            return;
        }
        if !writer {
            println!("exports: served here, run by the writer pod");
            return;
        }
        println!("exports: enabled; parts expire after the configured TTL");
        self.exports.sweeping.store(true, Ordering::Relaxed);
        let weak = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("kosha-export-sweep".into())
            .spawn(move || loop {
                let Some(state) = weak.upgrade() else {
                    return;
                };
                state.run_export_sweep(unix_millis());
                drop(state);
                std::thread::sleep(SWEEP_INTERVAL);
            })
            .expect("failed to spawn export sweep thread");
    }

    /// Fail every pending or running export with `reason`: its objects are
    /// deleted and its pins dropped. Returns how many it failed.
    pub(crate) fn fail_active_exports(&self, reason: &str) -> usize {
        let active = self
            .controller
            .lock()
            .unwrap()
            .exports(&ExportFilter::active());
        let active = match active {
            Ok(active) => active,
            Err(e) => {
                eprintln!("WARN: exports: reading active exports: {e}");
                return 0;
            }
        };
        let mut failed = 0;
        for record in active {
            let update = ExportUpdate {
                state: Some(ExportState::Failed),
                error: Some(Some(reason.to_string())),
                parts: Some(Vec::new()),
                clear_pins: true,
                clear_upload_url: true,
                delivery_state: record.upload_url.as_ref().map(|_| DeliveryState::Failed),
                ..ExportUpdate::default()
            };
            let updated = self
                .controller
                .lock()
                .unwrap()
                .update_export(&record.id, &update);
            if let Ok(Some(_)) = updated {
                failed += 1;
                if let Err(e) = self.remove_export_objects(&record) {
                    eprintln!(
                        "WARN: export {}: removing its objects failed: {e}",
                        record.id
                    );
                }
            }
        }
        if failed > 0 {
            println!("exports: {failed} pending or running export(s) failed: {reason}");
        }
        failed
    }

    /// One sweep: expire what is past `expires_at_ms`, run active exports
    /// nobody is running, and remove failed and expired rows older than
    /// [`RETENTION`] (their objects are already gone). Returns how many
    /// exports expired.
    pub(crate) fn run_export_sweep(&self, now_ms: u64) -> usize {
        self.prune_export_rows(now_ms);
        let listed = self.controller.lock().unwrap().exports(&ExportFilter {
            states: vec![
                ExportState::Completed,
                ExportState::Pending,
                ExportState::Running,
            ],
            limit: Some(10_000),
            ..ExportFilter::default()
        });
        let records = match listed {
            Ok(records) => records,
            Err(e) => {
                eprintln!("WARN: export sweep: {e}");
                return 0;
            }
        };
        let mut expired = 0;
        for record in records {
            match record.state {
                ExportState::Completed if record.expires_at_ms.is_some_and(|at| at <= now_ms) => {
                    if let Err(e) = self.remove_export_objects(&record) {
                        eprintln!("WARN: export {}: expiring failed: {e}", record.id);
                        continue;
                    }
                    let update = ExportUpdate {
                        state: Some(ExportState::Expired),
                        parts: Some(Vec::new()),
                        ..ExportUpdate::default()
                    };
                    if let Ok(Some(_)) = self
                        .controller
                        .lock()
                        .unwrap()
                        .update_export(&record.id, &update)
                    {
                        expired += 1;
                        self.exports.expired.fetch_add(1, Ordering::Relaxed);
                    }
                }
                // A pending export no pod took (this pod was full), a claim
                // a dead pod stopped refreshing, or this pod's own claim
                // from before a restart: the claim decides whether it is
                // ours to run, and `spawn_export_run` skips what this
                // process is already running.
                ExportState::Pending | ExportState::Running => {
                    self.spawn_export_run(&record.id);
                }
                _ => {}
            }
        }
        expired
    }
}

/// Files read one after the other: the parts of an export as one stream.
struct Concat(std::collections::VecDeque<std::fs::File>);

impl std::io::Read for Concat {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while let Some(file) = self.0.front_mut() {
            let n = file.read(buf)?;
            if n > 0 || buf.is_empty() {
                return Ok(n);
            }
            self.0.pop_front();
        }
        Ok(0)
    }
}

/// A unique-enough temporary file name.
#[cfg(feature = "s3")]
fn uuid_like() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}-{}",
        std::process::id(),
        unix_millis(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
#[path = "exports_tests.rs"]
mod tests;
