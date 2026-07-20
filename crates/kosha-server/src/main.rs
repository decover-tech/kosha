//! Kosha node binary.
//!
//! One binary, three roles (DESIGN.md §5): `ingest`, `query`, `compaction`.
//! Phase 1 HTTP/JSON API:
//!   - `GET  /healthz`           → 200 OK (liveness probe)
//!   - `POST /index              ` → index documents into a namespace
//!   - `GET  /search             ` → BM25 search across a namespace

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Map of valid API keys → tenant id.
/// Loaded once from `KOSHA_API_KEYS` env var (format: `key1=tenant1,key2=tenant2`).
/// In single-tenant/dev mode, `KOSHA_API_KEY` sets a single key with tenant "default".
static API_KEYS: once_cell::sync::Lazy<HashMap<String, String>> =
    once_cell::sync::Lazy::new(load_api_keys);

fn load_api_keys() -> HashMap<String, String> {
    // Multi-tenant: KOSHA_API_KEYS = "sk-kosha-1=acme-corp,sk-kosha-2=other-org"
    if let Ok(keys) = std::env::var("KOSHA_API_KEYS") {
        return keys
            .split(',')
            .filter_map(|pair| {
                pair.split_once('=')
                    .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
            })
            .collect();
    }
    // Single-tenant: KOSHA_API_KEY = "sk-kosha-abc123"
    if let Ok(key) = std::env::var("KOSHA_API_KEY") {
        let mut m = HashMap::new();
        m.insert(key, "default".to_string());
        return m;
    }
    // Dev mode: no auth required
    HashMap::new()
}

/// Extract the tenant prefix from a namespace for isolation.
fn tenant_namespace(tenant: &str, namespace: &str) -> String {
    format!("{tenant}/{namespace}")
}

#[cfg(feature = "s3")]
mod s3_storage;

use kosha_core::{ControlStore, IndexRequest, IndexResponse, KoshaError, NamespaceId, SearchQuery};
#[cfg(feature = "s3")]
use kosha_core::StorageBackend;
use kosha_query::Searcher;
use kosha_write::Indexer;

// ─── Application state ──────────────────────────────────────────────────────

struct AppState {
    controller: Mutex<Box<dyn ControlStore>>,
    indexer: Mutex<Indexer>,
    searcher: Searcher,
    data_dir: PathBuf,
    #[cfg(feature = "s3")]
    s3_storage: Option<s3_storage::S3Storage>,
}

impl AppState {
    fn new(data_dir: PathBuf) -> Self {
        let indexer = Indexer::new(data_dir.clone());
        let searcher = Searcher::new(data_dir.clone());

        #[cfg(feature = "s3")]
        let s3_storage = {
            let bucket = std::env::var("KOSHA_S3_BUCKET").ok();
            let prefix = std::env::var("KOSHA_S3_PREFIX").unwrap_or_default();
            if let Some(ref bucket) = bucket {
                let b = bucket.clone();
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                match rt {
                    Ok(rt) => {
                        let fut = s3_storage::S3Storage::new(data_dir.clone(), b, prefix);
                        match rt.block_on(fut) {
                            Ok(s3) => {
                                println!("S3 storage enabled: bucket={}", bucket);
                                Some(s3)
                            }
                            Err(e) => {
                                eprintln!("Failed to init S3 storage: {e}");
                                None
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to create tokio runtime: {e}");
                        None
                    }
                }
            } else {
                None
            }
        };

        // ── Control plane: in-memory or Postgres ─────────────────────────
        let control_store: Box<dyn ControlStore> =
            if let Ok(db_url) = std::env::var("DATABASE_URL") {
                #[cfg(feature = "postgres")]
                match kosha_control::PgStore::new(&db_url) {
                    Ok(store) => {
                        println!("control plane: postgres ({db_url})");
                        Box::new(store)
                    }
                    Err(e) => {
                        eprintln!("WARN: failed to connect to postgres, falling back to in-memory: {e}");
                        Box::new(kosha_control::Controller::new())
                    }
                }
                #[cfg(not(feature = "postgres"))]
                {
                    println!("control plane: in-memory (DATABASE_URL set but postgres feature disabled)");
                    let _ = db_url; // suppress unused warning
                    Box::new(kosha_control::Controller::new())
                }
            } else {
                println!("control plane: in-memory (no DATABASE_URL)");
                Box::new(kosha_control::Controller::new())
            };

        Self {
            controller: Mutex::new(control_store),
            indexer: Mutex::new(indexer),
            searcher,
            data_dir,
            #[cfg(feature = "s3")]
            s3_storage,
        }
    }

    /// Sync a segment directory to S3 (after flush).
    #[cfg(feature = "s3")]
    fn sync_to_s3(&self, seg_dir: &PathBuf) {
        if let Some(ref s3) = self.s3_storage {
            if let Ok(entries) = std::fs::read_dir(seg_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            let s3_path = format!(
                                "{}/{}",
                                seg_dir
                                    .strip_prefix(&self.data_dir)
                                    .unwrap_or(seg_dir)
                                    .to_string_lossy(),
                                name
                            );
                            if let Ok(data) = std::fs::read(&path) {
                                let _ = s3.write(&s3_path, &data);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Ensure a segment is available locally (download from S3 if needed).
    #[cfg(feature = "s3")]
    fn ensure_segment_local(&self, seg_path: &Path) {
        if seg_path.exists() {
            return;
        }
        if let Some(ref s3) = self.s3_storage {
            if let Ok(rel_path) = seg_path.strip_prefix(&self.data_dir) {
                let s3_prefix = rel_path.to_string_lossy();
                if let Ok(files) = s3.list(&s3_prefix) {
                    for file in &files {
                        let s3_key = format!("{}/{}", s3_prefix, file);
                        let _ = s3.read(&s3_key);
                    }
                }
            }
        }
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────

fn main() {
    let role = std::env::var("KOSHA_ROLE").unwrap_or_else(|_| "query".into());
    let port = std::env::var("KOSHA_HTTP_PORT").unwrap_or_else(|_| "8080".into());
    let data_dir = std::env::var("KOSHA_DATA_DIR").unwrap_or_else(|_| "/var/lib/kosha/data".into());
    let addr = format!("0.0.0.0:{port}");

    let state = AppState::new(PathBuf::from(data_dir.clone()));
    let listener = TcpListener::bind(&addr).expect("failed to bind HTTP listener");
    println!("kosha-server role={role} listening on {addr} data_dir={data_dir}");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(err) = handle(&state, stream) {
                    eprintln!("request error: {err}");
                }
            }
            Err(err) => eprintln!("accept error: {err}"),
        }
    }
}

// ─── Request handling ───────────────────────────────────────────────────────

fn handle(state: &AppState, mut stream: TcpStream) -> Result<(), KoshaError> {
    let mut reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|e| KoshaError::NotFound(format!("failed to clone stream: {e}")))?,
    );

    let mut request_line = String::new();
    reader.read_line(&mut request_line).ok();

    let mut headers = HashMap::new();
    let mut content_length: usize = 0;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).ok() == Some(0) || line == "\r\n" {
            break;
        }
        let line = line.trim_end();
        if let Some((key, value)) = line.split_once(':') {
            let k = key.trim().to_lowercase();
            let v = value.trim();
            headers.insert(k, v.to_string());
            if let Some(len) = headers.get("content-length") {
                content_length = len.parse().unwrap_or(0);
            }
        }
    }

    let mut body = Vec::new();
    if content_length > 0 {
        reader
            .take(content_length as u64)
            .read_to_end(&mut body)
            .ok();
    }

    // ── API key authentication ──────────────────────────────────────────
    // Per proto/kosha/v1/kosha.proto: Authorization: Bearer <key> or X-Api-Key.
    let tenant = match authenticate(&headers) {
        Ok(t) => t,
        Err(resp) => {
            stream.write_all(resp.as_bytes()).ok();
            return Ok(());
        }
    };

    let response = route(&request_line, &headers, &body, &tenant, state);
    stream.write_all(response.as_bytes()).ok();
    Ok(())
}

/// Extract and validate the API key from request headers.
/// Returns the tenant id on success, or a 401 response string on failure.
fn authenticate(headers: &HashMap<String, String>) -> Result<String, String> {
    // Dev mode: if no keys configured, allow all requests.
    if API_KEYS.is_empty() {
        return Ok("dev".to_string());
    }

    let api_key: Option<&str> = headers
        .get("authorization")
        .map(|v| v.as_str())
        .and_then(|v| v.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").map(|v| v.as_str()))
        .or_else(|| {
            // Check case-insensitively for x-api-key (raw headers may preserve case)
            headers.iter().find_map(|(k, v)| {
                if k.eq_ignore_ascii_case("x-api-key") {
                    Some(v.as_str())
                } else {
                    None
                }
            })
        });

    match api_key {
        Some(key) => match API_KEYS.get(key) {
            Some(tenant) => Ok(tenant.clone()),
            None => Err(json_error_body(401, "invalid API key")),
        },
        None => Err(json_error_body(
            401,
            "missing API key — use Authorization: Bearer <key> or X-Api-Key header",
        )),
    }
}

fn route(
    request_line: &str,
    _headers: &HashMap<String, String>,
    body: &[u8],
    tenant: &str,
    state: &AppState,
) -> String {
    // ── v1 proto-defined routes ────────────────────────────────────────────
    // proto/kosha/v1/kosha.proto is the canonical source of truth for paths.
    if request_line.starts_with("GET /v1/healthz") {
        return json_ok(&serde_json::json!({"status": "ok"}));
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/documents") {
        return handle_index_with_ns(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/search") {
        return handle_search_post_with_ns(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/flush") {
        return handle_flush_with_ns(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/delete") {
        return handle_delete_with_ns(&ns, tenant, body, state);
    }

    if request_line.starts_with("GET /v1/stats") {
        return handle_stats(state);
    }

    if let Some(ns) = extract_namespace(request_line, "GET /v1/namespaces/", "/stats") {
        return handle_namespace_stats(&ns, tenant, state);
    }

    // ── Legacy Phase 1 routes (backward compat) ────────────────────────────
    // These will be removed after DecoverAI cuts over to the v1 paths.
    if request_line.starts_with("GET /healthz") {
        return json_ok(&serde_json::json!({"status": "ok"}));
    }

    if request_line.starts_with("POST /index") {
        return handle_index(body, state);
    }

    if request_line.starts_with("GET /search") {
        return handle_search_get(request_line, state);
    }

    if request_line.starts_with("POST /search") {
        return handle_search_post(body, state);
    }

    if request_line.starts_with("POST /flush") {
        return handle_flush(body, state);
    }

    if request_line.starts_with("POST /delete") {
        return handle_delete(body, state);
    }

    if request_line.starts_with("GET /stats") {
        return handle_stats(state);
    }

    json_error(404, "not found")
}

/// Extract a namespace from a v1 path: `METHOD /v1/namespaces/{ns}/suffix ...`
fn extract_namespace(request_line: &str, prefix: &str, suffix: &str) -> Option<String> {
    let after_method = request_line.split(' ').nth(1)?;
    let after_prefix = after_method.strip_prefix(prefix)?;
    let ns = after_prefix.strip_suffix(suffix)?;
    if ns.is_empty() || ns.contains('/') {
        return None;
    }
    Some(url_decode(ns))
}

// ─── POST /index ────────────────────────────────────────────────────────────

fn handle_index(body: &[u8], state: &AppState) -> String {
    let request: IndexRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };

    let count = {
        let mut indexer = state.indexer.lock().unwrap();
        match indexer.index_documents(request.namespace.clone(), request.documents) {
            Ok(c) => c,
            Err(e) => return json_error(500, &format!("indexing error: {e}")),
        }
    };

    // Ensure namespace is registered in the controller.
    {
        let mut ctrl = state.controller.lock().unwrap();
        ctrl.ensure_namespace(request.namespace.clone());
    }

    json_ok(&IndexResponse {
        indexed_count: count,
        namespace: request.namespace,
    })
}

// ─── POST /flush ────────────────────────────────────────────────────────────

fn handle_flush(body: &[u8], state: &AppState) -> String {
    let req: std::collections::HashMap<String, String> =
        serde_json::from_slice(body).unwrap_or_default();
    let ns = req.get("namespace").cloned();

    {
        let mut indexer = state.indexer.lock().unwrap();
        match ns {
            Some(ref name) => {
                let namespace = NamespaceId(name.clone());
                if let Err(e) = indexer.flush_namespace(&namespace) {
                    return json_error(500, &format!("flush error: {e}"));
                }
            }
            None => {
                if let Err(e) = indexer.flush_all() {
                    return json_error(500, &format!("flush error: {e}"));
                }
            }
        }
    }

    // Sync to S3 after flush (if S3 is configured).
    #[cfg(feature = "s3")]
    {
        if let Some(ref ns_name) = ns {
            let ns_id = NamespaceId(ns_name.clone());
            if let Ok(indexer) = state.indexer.lock() {
                if let Some(manifest) = indexer.manifest(&ns_id).cloned() {
                    for entry in &manifest.segments {
                        let seg_path = state.data_dir.join(ns_name).join(&entry.segment_id.0);
                        state.sync_to_s3(&seg_path);
                    }
                }
            }
        }
    }

    json_ok(&serde_json::json!({"status": "flushed"}))
}

// ─── POST /delete (delete by query) ──────────────────────────────────────────

fn handle_delete(body: &[u8], state: &AppState) -> String {
    let body_val: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };

    let ns = match body_val.get("namespace").and_then(|v| v.as_str()) {
        Some(n) => NamespaceId(n.to_string()),
        None => return json_error(400, "missing 'namespace'"),
    };

    // Extract filter from body — supports ES-style "query" field.
    let filter_val = body_val.get("filter").or_else(|| body_val.get("query"));
    let filter: kosha_core::FilterClause = match filter_val {
        Some(v) => match serde_json::from_value(v.clone()) {
            Ok(f) => f,
            Err(e) => return json_error(400, &format!("invalid filter: {e}")),
        },
        None => return json_error(400, "missing 'filter' or 'query'"),
    };

    let (_manifest, count) = {
        let mut indexer = state.indexer.lock().unwrap();
        let manifest = match indexer.manifest_cloned(&ns) {
            Some(m) => m,
            None => return json_error(404, &format!("namespace '{}' not found", ns.0)),
        };
        let old_manifest = manifest.clone();
        match indexer.delete_by_query(&ns, &old_manifest, &filter) {
            Ok(c) => (old_manifest, c),
            Err(e) => return json_error(500, &format!("delete error: {e}")),
        }
    };

    json_ok(&serde_json::json!({
        "deleted": count,
        "namespace": ns.0,
    }))
}

// ─── GET /stats ──────────────────────────────────────────────────────────────

fn handle_stats(state: &AppState) -> String {
    let indexer = state.indexer.lock().unwrap();
    let mut namespaces: Vec<serde_json::Value> = Vec::new();
    let mut total_docs: u64 = 0;
    let mut total_segments: usize = 0;

    for ns in indexer.namespaces() {
        let manifest = match indexer.manifest(ns) {
            Some(m) => m,
            None => continue,
        };
        let ns_docs: u64 = manifest.segments.iter().map(|s| s.doc_count as u64).sum();
        total_docs += ns_docs;
        total_segments += manifest.segments.len();

        namespaces.push(serde_json::json!({
            "namespace": ns.0,
            "documents": ns_docs,
            "segments": manifest.segments.len(),
            "version": manifest.version,
        }));
    }

    json_ok(&serde_json::json!({
        "total_documents": total_docs,
        "total_segments": total_segments,
        "namespaces": namespaces,
    }))
}

// ─── POST /search (JSON body, supports filters) ─────────────────────────────

fn handle_search_post(body: &[u8], state: &AppState) -> String {
    // Parse the full JSON body that includes namespace alongside SearchQuery fields.
    let body_val: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };

    let ns = match body_val.get("namespace").and_then(|v| v.as_str()) {
        Some(n) => NamespaceId(n.to_string()),
        None => return json_error(400, "missing 'namespace' in search body"),
    };

    let query: kosha_core::SearchQuery = match serde_json::from_value(body_val) {
        Ok(q) => q,
        Err(e) => return json_error(400, &format!("invalid search query: {e}")),
    };

    let (manifest, tombstones) = {
        let indexer = state.indexer.lock().unwrap();
        let m = match indexer.manifest_cloned(&ns) {
            Some(m) => m,
            None => return json_error(404, &format!("namespace '{}' not found", ns.0)),
        };
        let t = indexer.get_tombstones(&ns).cloned();
        (m, t)
    };

    // Ensure segment dirs exist locally (download from S3 if needed).
    #[cfg(feature = "s3")]
    for entry in &manifest.segments {
        let seg_path = state.data_dir.join(&ns.0).join(&entry.segment_id.0);
        state.ensure_segment_local(&seg_path);
    }

    match state
        .searcher
        .search(&ns, &manifest, &query, tombstones.as_ref())
    {
        Ok(result) => json_ok(&result),
        Err(e) => json_error(500, &format!("search error: {e}")),
    }
}

// ─── GET /search (query params, simple queries) ─────────────────────────────

fn handle_search_get(request_line: &str, state: &AppState) -> String {
    // Parse query string from the request line.
    // request_line looks like: "GET /search?ns=...&q=...&max_results=... HTTP/1.1"
    let query_string = request_line
        .split(' ')
        .nth(1)
        .and_then(|path| path.split('?').nth(1))
        .unwrap_or("");

    let params: HashMap<String, String> = query_string
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| (k.to_string(), url_decode(v)))
        .collect();

    let ns = match params.get("ns") {
        Some(ns) => NamespaceId(ns.clone()),
        None => return json_error(400, "missing 'ns' query parameter"),
    };

    let query_text = match params.get("q") {
        Some(q) => q.clone(),
        None => return json_error(400, "missing 'q' query parameter"),
    };

    let max_results: usize = params
        .get("max_results")
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    let k1: f64 = params.get("k1").and_then(|v| v.parse().ok()).unwrap_or(1.2);
    let b: f64 = params.get("b").and_then(|v| v.parse().ok()).unwrap_or(0.75);

    let query = SearchQuery {
        query_text,
        max_results,
        bm25_params: kosha_core::Bm25Params { k1, b },
        from: 0,
        filter: None,
        sort: vec![],
        highlight: None,
        aggs: std::collections::HashMap::new(),
        wildcard: None,
        match_phrase: None,
        knn: None,
    };

    let (manifest, tombstones) = {
        let indexer = state.indexer.lock().unwrap();
        let m = match indexer.manifest_cloned(&ns) {
            Some(m) => m,
            None => return json_error(404, &format!("namespace '{}' not found", ns.0)),
        };
        let t = indexer.get_tombstones(&ns).cloned();
        (m, t)
    };

    // Ensure segment dirs exist locally (download from S3 if needed).
    #[cfg(feature = "s3")]
    for entry in &manifest.segments {
        let seg_path = state.data_dir.join(&ns.0).join(&entry.segment_id.0);
        state.ensure_segment_local(&seg_path);
    }

    match state
        .searcher
        .search(&ns, &manifest, &query, tombstones.as_ref())
    {
        Ok(result) => json_ok(&result),
        Err(e) => json_error(500, &format!("search error: {e}")),
    }
}

// ─── v1 route handlers (proto-defined paths, tenant-scoped namespaces) ─────

fn handle_index_with_ns(namespace: &str, tenant: &str, body: &[u8], state: &AppState) -> String {
    let scoped_ns = tenant_namespace(tenant, namespace);
    let mut body_val: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => body_val_fallback(body),
    };
    if let Some(obj) = body_val.as_object_mut() {
        obj.insert("namespace".into(), serde_json::json!(scoped_ns));
    }
    handle_index(body, state)
}

fn handle_search_post_with_ns(
    namespace: &str,
    tenant: &str,
    body: &[u8],
    state: &AppState,
) -> String {
    let scoped_ns = tenant_namespace(tenant, namespace);
    let mut body_val: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return json_error(400, "invalid JSON"),
    };
    if let Some(obj) = body_val.as_object_mut() {
        obj.insert("namespace".into(), serde_json::json!(scoped_ns));
    }
    let modified_body = serde_json::to_vec(&body_val).unwrap_or_else(|_| body.to_vec());
    handle_search_post(&modified_body, state)
}

fn handle_flush_with_ns(namespace: &str, tenant: &str, body: &[u8], state: &AppState) -> String {
    let scoped_ns = tenant_namespace(tenant, namespace);
    let mut body_val: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => serde_json::json!({}),
    };
    if let Some(obj) = body_val.as_object_mut() {
        obj.insert("namespace".into(), serde_json::json!(scoped_ns));
    }
    let modified_body = serde_json::to_vec(&body_val).unwrap_or_else(|_| body.to_vec());
    handle_flush(&modified_body, state)
}

fn handle_delete_with_ns(namespace: &str, tenant: &str, body: &[u8], state: &AppState) -> String {
    let scoped_ns = tenant_namespace(tenant, namespace);
    let mut body_val: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return json_error(400, "invalid JSON"),
    };
    if let Some(obj) = body_val.as_object_mut() {
        obj.insert("namespace".into(), serde_json::json!(scoped_ns));
    }
    let modified_body = serde_json::to_vec(&body_val).unwrap_or_else(|_| body.to_vec());
    handle_delete(&modified_body, state)
}

fn handle_namespace_stats(namespace: &str, tenant: &str, state: &AppState) -> String {
    let scoped_ns = tenant_namespace(tenant, namespace);
    let indexer = state.indexer.lock().unwrap();
    let manifest = match indexer.manifest(&NamespaceId(scoped_ns.clone())) {
        Some(m) => m,
        None => return json_error(404, &format!("namespace '{namespace}' not found")),
    };
    let ns_docs: u64 = manifest.segments.iter().map(|s| s.doc_count as u64).sum();
    json_ok(&serde_json::json!({
        "namespace": namespace,
        "documents": ns_docs,
        "segments": manifest.segments.len(),
        "version": manifest.version,
    }))
}

/// If the body is not valid JSON, try parsing as a raw object with just the documents field.
fn body_val_fallback(body: &[u8]) -> serde_json::Value {
    // Try parsing as a JSON object with a documents array
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) {
        return v;
    }
    serde_json::json!({})
}

// ─── JSON response helpers ──────────────────────────────────────────────────

fn json_ok<T: serde::Serialize>(value: &T) -> String {
    let body = serde_json::to_string(value).unwrap_or_else(|_| "{}".into());
    format!(
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn json_error(status_code: u16, message: &str) -> String {
    let body = serde_json::json!({"error": message}).to_string();
    let status_line = match status_code {
        400 => "400 Bad Request",
        404 => "404 Not Found",
        500 => "500 Internal Server Error",
        _ => "500 Internal Server Error",
    };
    format!(
        "HTTP/1.1 {status_line}\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Returns *just* the JSON body (no HTTP headers) for an error response.
/// Used by `authenticate()` which constructs its own HTTP response.
fn json_error_body(status_code: u16, message: &str) -> String {
    let body = serde_json::json!({"error": message}).to_string();
    let status_line = match status_code {
        400 => "400 Bad Request",
        401 => "401 Unauthorized",
        404 => "404 Not Found",
        500 => "500 Internal Server Error",
        _ => "500 Internal Server Error",
    };
    format!(
        "HTTP/1.1 {status_line}\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Minimal URL decoder: decodes %XX and + → space.
fn url_decode(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(c) = chars.next() {
        match c {
            '+' => result.push(' '),
            '%' => {
                let hi = chars.next().and_then(|c| c.to_digit(16)).unwrap_or(0);
                let lo = chars.next().and_then(|c| c.to_digit(16)).unwrap_or(0);
                result.push((hi as u8 * 16 + lo as u8) as char);
            }
            _ => result.push(c),
        }
    }
    result
}

impl Drop for AppState {
    fn drop(&mut self) {
        if let Ok(mut indexer) = self.indexer.lock() {
            let _ = indexer.flush_all();
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::{Document, DocumentId, Field};
    use std::fs;

    #[test]
    fn healthz_returns_200_ok() {
        let response = route(
            "GET /healthz HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &test_state(),
        );
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("\"status\":\"ok\""));
    }

    #[test]
    fn unknown_path_returns_404() {
        let response = route(
            "GET /nope HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &test_state(),
        );
        assert!(response.starts_with("HTTP/1.1 404 Not Found"));
    }

    #[test]
    fn response_is_well_formed_http11() {
        let response = route(
            "GET /healthz HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &test_state(),
        );
        let (status, headers, body) = parse(&response);
        assert!(status.starts_with("HTTP/1.1 "));
        assert!(headers.contains(&format!("content-length: {}", body.len())));
        assert!(headers.contains("content-type: application/json"));
        assert!(headers.contains("connection: close"));
    }

    #[test]
    fn index_endpoint_works() {
        let state = test_state();
        let req = IndexRequest {
            namespace: NamespaceId("test-ns".into()),
            documents: vec![Document {
                id: DocumentId("doc1".into()),
                fields: vec![Field::text("title", "hello world")],
            }],
        };
        let body = serde_json::to_vec(&req).unwrap();
        let response = route(
            "POST /index HTTP/1.1\r\n",
            &HashMap::new(),
            &body,
            "test",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("\"indexed_count\":1"));
    }

    #[test]
    fn index_then_search() {
        let state = test_state();
        let ns = "test-ns-search";

        // Index a document.
        let req = IndexRequest {
            namespace: NamespaceId(ns.into()),
            documents: vec![
                Document {
                    id: DocumentId("d1".into()),
                    fields: vec![Field::text("title", "quick brown fox")],
                },
                Document {
                    id: DocumentId("d2".into()),
                    fields: vec![Field::text("title", "lazy dog")],
                },
            ],
        };
        let body = serde_json::to_vec(&req).unwrap();
        let index_resp = route(
            "POST /index HTTP/1.1\r\n",
            &HashMap::new(),
            &body,
            "test",
            &state,
        );
        assert!(index_resp.contains("\"indexed_count\":2"));

        // Trigger flush so search can read from disk.
        {
            let mut indexer = state.indexer.lock().unwrap();
            indexer.flush_namespace(&NamespaceId(ns.into())).unwrap();
        }

        // Now search for "quick".
        let search_resp = route(
            &format!("GET /search?ns={ns}&q=quick HTTP/1.1\r\n"),
            &HashMap::new(),
            b"",
            "test",
            &state,
        );
        assert!(search_resp.starts_with("HTTP/1.1 200 OK"));
        assert!(search_resp.contains("\"total_hits\":1"));

        // Search for "dog".
        let search_resp2 = route(
            &format!("GET /search?ns={ns}&q=dog HTTP/1.1\r\n"),
            &HashMap::new(),
            b"",
            "test",
            &state,
        );
        assert!(search_resp2.starts_with("HTTP/1.1 200 OK"));
        assert!(search_resp2.contains("\"total_hits\":1"));
    }

    #[test]
    fn search_missing_namespace_returns_404() {
        let state = test_state();
        let response = route(
            "GET /search?ns=nonexistent&q=test HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 404 Not Found"));
    }

    #[test]
    fn search_missing_params_returns_400() {
        let state = test_state();
        let response = route(
            "GET /search HTTP/1.1\r\n",
            &HashMap::new(),
            b"",
            "test",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 400 Bad Request"));
    }

    fn parse(response: &str) -> (&str, &str, &str) {
        let (head, body) = response
            .split_once("\r\n\r\n")
            .expect("response must have a header/body separator");
        let mut lines = head.splitn(2, "\r\n");
        let status_line = lines.next().expect("missing status line");
        let headers = lines.next().unwrap_or("");
        (status_line, headers, body)
    }

    fn test_state() -> AppState {
        let dir = std::env::temp_dir().join(format!(
            "kosha-test-server-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        AppState::new(dir)
    }
}
