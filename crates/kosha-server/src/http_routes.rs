//! Route authorization and dispatch for the node's HTTP API.

use std::collections::HashMap;

use super::{
    addressable_namespace, admin_actor, api_error, api_key_admin, auth, bare_namespace, billing,
    branching, extract_namespace, handle_backfill_offset_tables, handle_compact_namespace,
    handle_compaction_job, handle_compaction_status, handle_create_namespace, handle_delete,
    handle_delete_namespace, handle_delete_with_ns, handle_exists, handle_exists_with_ns,
    handle_flush, handle_flush_with_ns, handle_get_schema_with_ns, handle_import_namespace,
    handle_index, handle_index_with_ns, handle_list_namespaces, handle_namespace_stats,
    handle_put_schema_with_ns, handle_put_shards_with_ns, handle_rebuild_filter_blooms,
    handle_replace, handle_replace_with_ns, handle_search_get, handle_search_post,
    handle_search_post_with_ns, handle_segment_gc, handle_stats, http, is_create_namespace_route,
    is_list_namespaces_route, json_ok, legacy_route, namespace_deletion, operator_admin, org_admin,
    parse_method_and_path, path_decode, routes, service_authorized, tenant_namespace, unindexed,
    url_decode, usage, usage_reconcile, valid_name_component, with_header, ApiError, AppState,
    ErrorCode,
};

/// Authorize before dispatch, including compatibility and internal routes.
/// A response on a legacy unversioned route carries the deprecation
/// headers (ADR-350 §c): `Deprecation`, `Sunset`, and a `Link` to the
/// migration notes.
pub(super) fn route(
    request_line: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
    tenant: &str,
    state: &AppState,
) -> String {
    let response = route_checked(request_line, headers, body, tenant, state);
    if legacy_route(request_line).is_none() {
        return response;
    }
    let response = with_header(
        response,
        "link",
        &format!("<{}>; rel=\"deprecation\"", auth::LEGACY_DEPRECATION_LINK),
    );
    let response = with_header(response, "sunset", auth::LEGACY_SUNSET);
    with_header(
        response,
        "deprecation",
        &format!("@{}", auth::LEGACY_DEPRECATED_AT),
    )
}

fn route_checked(
    request_line: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
    tenant: &str,
    state: &AppState,
) -> String {
    let Some((method, path)) = parse_method_and_path(request_line) else {
        return api_error(ErrorCode::InvalidArgument, "invalid request line");
    };
    if path.starts_with("/v1/admin/") {
        let Some(operator) = admin_actor(headers, state) else {
            return api_error(ErrorCode::PermissionDenied, "admin credential required");
        };
        if let Some(response) = unrouted_v1(method, path) {
            return response;
        }
        if let Some(response) = state.control_store_write_refusal(request_line) {
            return response;
        }
        // Organizations, users, projects, audit log (#345).
        let key_table = match auth::global() {
            auth::Auth::Keys(keys) => Some(keys),
            auth::Auth::Disabled => None,
        };
        if let Some(response) = org_admin::handle(
            method,
            path,
            headers,
            body,
            &state.orgs,
            key_table,
            &operator,
        ) {
            return response;
        }
        // Every other admin route is the operator's and knows nothing of
        // roles. A request that names a user must not reach one: it would
        // run with the operator's powers (a key for any tenant, gc, ...).
        if org_admin::names_actor(headers) {
            return api_error(
                ErrorCode::PermissionDenied,
                "this route is operator-only: it cannot be called with an 'x-kosha-actor' header",
            );
        }
        // Per-operator admin credentials (#346).
        if let Some(response) = operator_admin::handle(method, path, body, &operator) {
            return response;
        }
    } else if path.starts_with("/internal/") {
        if !service_authorized(headers, state.internal_service_key.as_deref()) {
            return api_error(
                ErrorCode::PermissionDenied,
                "internal service credential required",
            );
        }
    } else {
        if !valid_name_component(tenant) {
            return api_error(ErrorCode::PermissionDenied, "invalid tenant");
        }
        if let Some(rest) = path.strip_prefix("/v1/namespaces/") {
            // `/v1/namespaces/{ns}/{action}`, or the bare
            // `/v1/namespaces/{ns}` that `DELETE` addresses.
            // The query string goes first: a '/' in it must not move the
            // split (`DELETE /v1/namespaces/_shard-0001?x=/` would
            // otherwise validate `_shard-0001?x=` and delete the shard).
            let rest = rest.split('?').next().unwrap_or(rest);
            let ns = match rest.rsplit_once('/') {
                Some((ns, _action)) => ns,
                None => rest,
            };
            if !addressable_namespace(state, tenant, &path_decode(ns)) {
                return api_error(ErrorCode::InvalidArgument, "invalid namespace");
            }
        }
        let bare_path = path.split('?').next().unwrap_or(path);
        let is_legacy = legacy_route(request_line).is_some();
        if is_legacy {
            if !auth::legacy_routes_enabled_for(tenant) {
                return legacy_route_disabled(bare_path);
            }
            auth::record_legacy_use(tenant, bare_path);
            state.metrics.record_legacy_request(bare_path, tenant);
        }
        if let Some(response) = state.control_store_write_refusal(request_line) {
            return response;
        }
        if method == "GET" && (bare_path == "/stats" || bare_path == "/v1/stats") {
            return if admin_actor(headers, state).is_some() {
                handle_stats(state)
            } else {
                api_error(ErrorCode::PermissionDenied, "use namespace-scoped stats")
            };
        }
        if method == "POST"
            && [
                "/index", "/exists", "/replace", "/search", "/flush", "/delete",
            ]
            .contains(&bare_path)
        {
            let mut value: serde_json::Value = match serde_json::from_slice(body) {
                Ok(v) => v,
                Err(_) => return api_error(ErrorCode::InvalidArgument, "invalid JSON"),
            };
            let Some(ns) = value.get("namespace").and_then(|v| v.as_str()) else {
                return api_error(ErrorCode::InvalidArgument, "missing namespace");
            };
            if !addressable_namespace(state, tenant, ns) {
                return api_error(
                    ErrorCode::InvalidArgument,
                    "invalid namespace; use an unqualified namespace name",
                );
            }
            value["namespace"] = serde_json::json!(tenant_namespace(tenant, ns));
            return route_inner(
                request_line,
                headers,
                &serde_json::to_vec(&value).unwrap(),
                tenant,
                state,
            );
        }
        if method == "GET" && bare_path == "/search" {
            let Some((_, query)) = path.split_once('?') else {
                return api_error(ErrorCode::InvalidArgument, "missing query parameters");
            };
            let mut pairs = Vec::new();
            let mut found = false;
            for pair in query.split('&') {
                if let Some(raw) = pair.strip_prefix("ns=") {
                    if found {
                        return api_error(ErrorCode::InvalidArgument, "duplicate namespace");
                    }
                    let ns = url_decode(raw);
                    if !addressable_namespace(state, tenant, &ns) {
                        return api_error(ErrorCode::InvalidArgument, "invalid namespace");
                    }
                    // The original escaped component is preserved; tenant names
                    // are escaped too so they cannot inject query parameters.
                    let encoded_tenant: String =
                        tenant.bytes().map(|b| format!("%{b:02X}")).collect();
                    pairs.push(format!("ns={encoded_tenant}%2F{raw}"));
                    found = true;
                } else {
                    pairs.push(pair.to_string());
                }
            }
            if !found {
                return api_error(ErrorCode::InvalidArgument, "missing ns");
            }
            let scoped = format!("GET /search?{} HTTP/1.1", pairs.join("&"));
            return route_inner(&scoped, headers, body, tenant, state);
        }
        if bare_path.starts_with("/v1/") {
            if let Some(response) = unrouted_v1(method, path) {
                return response;
            }
        } else if bare_path != "/healthz" {
            return api_error(ErrorCode::NotFound, "not found");
        }
    }
    route_inner(request_line, headers, body, tenant, state)
}

/// The 404 (unknown path) or 405 with `Allow` (known path, wrong method)
/// for a `/v1` request [`routes::ROUTES`] does not serve; `None` when it
/// does. Everything `route_inner` answers under `/v1` must have a row.
fn unrouted_v1(method: &str, path: &str) -> Option<String> {
    if routes::lookup(method, path).is_some() {
        return None;
    }
    let allowed = routes::allowed_methods(path);
    if allowed.is_empty() {
        return Some(api_error(ErrorCode::NotFound, "not found"));
    }
    let allow = allowed.join(", ");
    Some(
        ApiError::new(
            ErrorCode::MethodNotAllowed,
            format!("method {method} is not allowed on this path (allowed: {allow})"),
        )
        .detail("allowed", allowed)
        .header("allow", &allow)
        .into_raw(),
    )
}

/// The `/v1` route that replaces a legacy unversioned one.
fn legacy_replacement(path: &str) -> &'static str {
    match path {
        "/index" => "POST /v1/namespaces/{namespace}/documents",
        "/replace" => "POST /v1/namespaces/{namespace}/replace",
        "/exists" => "POST /v1/namespaces/{namespace}/exists",
        "/search" => "POST /v1/namespaces/{namespace}/search",
        "/flush" => "POST /v1/namespaces/{namespace}/flush",
        "/delete" => "POST /v1/namespaces/{namespace}/delete",
        "/stats" => "GET /v1/namespaces/{namespace}/stats",
        _ => "/v1/namespaces/{namespace}/...",
    }
}

/// The 410 for a legacy route that is off for the caller.
pub(super) fn legacy_route_disabled(path: &str) -> String {
    let replacement = legacy_replacement(path);
    ApiError::new(
        ErrorCode::LegacyRouteDisabled,
        format!(
            "unversioned route {path} is disabled (KOSHA_LEGACY_ROUTES=off); use {replacement}"
        ),
    )
    .detail("route", path)
    .detail("replacement", replacement)
    .into_raw()
}

pub(super) fn route_inner(
    request_line: &str,
    _headers: &HashMap<String, String>,
    body: &[u8],
    tenant: &str,
    state: &AppState,
) -> String {
    // ── v1 routes ──────────────────────────────────────────────────────────
    // `route` admitted only paths in `routes::ROUTES` (#350), the table the
    // checked-in OpenAPI documents are tested against.
    if request_line.starts_with("GET /v1/healthz") {
        return json_ok(&serde_json::json!({"status": "ok"}));
    }

    if is_create_namespace_route(request_line) {
        return handle_create_namespace(body, tenant, state);
    }

    if let Some(ns) = bare_namespace(request_line, "DELETE") {
        return handle_delete_namespace(&ns, tenant, state);
    }

    if is_list_namespaces_route(request_line) {
        return handle_list_namespaces(tenant, state);
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/documents") {
        return handle_index_with_ns(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/replace") {
        return handle_replace_with_ns(&ns, tenant, body, state);
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

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/exists") {
        return handle_exists_with_ns(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/schema") {
        return handle_put_schema_with_ns(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "GET /v1/namespaces/", "/schema") {
        return handle_get_schema_with_ns(&ns, tenant, state);
    }

    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/shards") {
        return handle_put_shards_with_ns(&ns, tenant, body, state);
    }

    // ── Namespace branching (#208) ─────────────────────────────────────────
    if let Some(ns) = extract_namespace(request_line, "POST /v1/namespaces/", "/branch") {
        return branching::handle_branch_create(&ns, tenant, body, state);
    }

    if let Some(ns) = extract_namespace(request_line, "GET /v1/namespaces/", "/branches") {
        return branching::handle_branch_list(&ns, tenant, state);
    }

    if let Some(ns) = extract_namespace(request_line, "DELETE /v1/namespaces/", "/branch") {
        return branching::handle_branch_delete(&ns, tenant, state);
    }

    if request_line.starts_with("GET /v1/stats") {
        return handle_stats(state);
    }

    if let Some(ns) = extract_namespace(request_line, "GET /v1/namespaces/", "/stats") {
        return handle_namespace_stats(&ns, tenant, state);
    }

    // ── Usage metering (#348) ──────────────────────────────────────────────
    if request_line.starts_with("GET /v1/usage ") || request_line.starts_with("GET /v1/usage?") {
        return usage::handle_usage(request_line, Some(tenant), false, state);
    }

    if request_line.starts_with("GET /v1/admin/usage ")
        || request_line.starts_with("GET /v1/admin/usage?")
    {
        return usage::handle_admin_usage(request_line, state);
    }

    if request_line.starts_with("GET /v1/admin/usage/reconcile ")
        || request_line.starts_with("GET /v1/admin/usage/reconcile?")
    {
        return usage_reconcile::handle_admin_reconcile(request_line, state);
    }

    // ── Billing (#349) ─────────────────────────────────────────────────────
    if let Some(response) = billing::route(request_line, body, tenant, state) {
        return response;
    }

    // ── Admin routes (Postgres-backed only) ────────────────────────────────
    if let Some(response) = api_key_admin::handle(request_line, body) {
        return response;
    }

    if request_line.starts_with("POST /v1/admin/rebuild-filter-blooms") {
        return handle_rebuild_filter_blooms(body, tenant, state);
    }

    if request_line.starts_with("POST /v1/admin/backfill-offset-tables") {
        return handle_backfill_offset_tables(body, tenant, state);
    }

    if request_line.starts_with("POST /v1/admin/compact-namespace") {
        return handle_compact_namespace(body, tenant, state);
    }

    if request_line.starts_with("GET /v1/admin/warmup-status") {
        return json_ok(&state.warmup_status_json());
    }

    if request_line.starts_with("GET /v1/admin/compaction-status") {
        return handle_compaction_status(state);
    }

    if let Some(rest) = request_line.strip_prefix("GET /v1/admin/compaction-jobs/") {
        let job_id = rest.split([' ', '?']).next().unwrap_or_default();
        return handle_compaction_job(job_id, state);
    }

    if request_line.starts_with("POST /v1/admin/import-namespace") {
        return handle_import_namespace(body, tenant, state);
    }

    if request_line.starts_with("POST /v1/admin/gc ")
        || request_line.starts_with("POST /v1/admin/gc?")
    {
        return handle_segment_gc(body, state);
    }

    // Tenant deletion and deletion records (#347).
    if let Some(response) = namespace_deletion::route_admin(request_line, body, state) {
        return response;
    }

    // `GET /internal/segment/...` (pod-to-pod) never reaches here: it
    // streams a file, so `dispatch` answers it (`internal_segment_response`)
    // once the internal credential checks out. `route` still answers 403
    // to any other credential.

    // A query pod's strong-consistency search reads the ingest pod's
    // unindexed tail (#255). Authorized like the segment route above.
    if let Some(rest) = request_line.strip_prefix("GET /internal/namespaces/") {
        let target = rest.split(' ').next().unwrap_or("");
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        return match path.strip_suffix("/unindexed") {
            Some(ns) if !ns.is_empty() => {
                unindexed::handle_internal_unindexed(&path_decode(ns), query, state)
            }
            _ => api_error(ErrorCode::NotFound, "not found"),
        };
    }

    // ── Legacy Phase 1 routes (backward compat) ────────────────────────────
    // These will be removed after DecoverAI cuts over to the v1 paths.
    if request_line.starts_with("GET /healthz") {
        return json_ok(&serde_json::json!({"status": "ok"}));
    }

    if request_line.starts_with("POST /index") {
        return handle_index(body, state);
    }

    if request_line.starts_with("POST /exists") {
        return handle_exists(body, state);
    }

    if request_line.starts_with("POST /replace") {
        return handle_replace(body, state);
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

    api_error(ErrorCode::NotFound, "not found")
}

// ─── Internal (pod-to-pod) routes ───────────────────────────────────────────

/// `GET /internal/segment/<relative-path>` — stream an already-cached
/// segment file's raw bytes to a peer replica, for `fetch_from_peer`, as
/// `application/octet-stream` in O(chunk) memory (#259). Only called once
/// the internal service credential checked out (see `dispatch`); ordinary
/// tenant credentials cannot retrieve another tenant's cached files.
///
/// `relative_path` is validated the same way any other cache key is
/// (`Cache::path_for` rejects `..`/absolute-path traversal). 404 (not an
/// error — the caller retries against the lease owner) when the path isn't
/// cached on this pod, whether because it's not the owner or hydration is
/// still in flight.
pub(super) fn internal_segment_response(request_line: &str, state: &AppState) -> http::Response {
    #[cfg(all(feature = "s3", feature = "postgres"))]
    {
        let raw = request_line
            .strip_prefix("GET /internal/segment/")
            .and_then(|rest| rest.split(' ').next())
            .unwrap_or("");
        let relative_path = path_decode(raw.split('?').next().unwrap_or(raw));
        let local_path = match state.cache.path_for(&relative_path) {
            Ok(p) => p,
            Err(_) => {
                return http::Response::from_raw(api_error(
                    ErrorCode::InvalidArgument,
                    "invalid path",
                ))
            }
        };
        // Files land in the cache by rename, complete, so an open file is
        // a whole one; its length is fixed from here on.
        let opened = std::fs::File::open(&local_path)
            .and_then(|file| file.metadata().map(|meta| (file, meta)));
        match opened {
            Ok((file, meta)) if meta.is_file() => {
                http::Response::file(file, meta.len(), "application/octet-stream")
            }
            _ => http::Response::from_raw(api_error(ErrorCode::NotFound, "not cached here")),
        }
    }
    #[cfg(not(all(feature = "s3", feature = "postgres")))]
    {
        let _ = (request_line, state);
        http::Response::from_raw(api_error(ErrorCode::NotFound, "not found"))
    }
}
