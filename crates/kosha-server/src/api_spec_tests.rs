//! The route table against the checked-in OpenAPI documents (#350,
//! ADR-350 §d "Route-to-spec sync"): every operation the server serves is
//! in the spec, every spec operation reaches a handler, and the legacy and
//! internal routes are in neither document.

use std::collections::{BTreeSet, HashMap};

use crate::http_routes::legacy_route_disabled;
use crate::routes::{self, Surface};
use crate::*;

const PUBLIC_SPEC: &str = include_str!("../../../api/openapi/v1/kosha.openapi.json");
const ADMIN_SPEC: &str = include_str!("../../../api/openapi/v1/kosha-admin.openapi.json");
const ADMIN_KEY: &str = "spec-test-admin";

/// `(METHOD, path template, operationId)` for every operation in a spec.
fn operations(spec: &str) -> BTreeSet<(String, String, String)> {
    let doc: serde_json::Value = serde_json::from_str(spec).expect("spec is JSON");
    let mut out = BTreeSet::new();
    for (path, item) in doc["paths"].as_object().expect("paths") {
        for (method, op) in item.as_object().expect("path item") {
            if !["get", "post", "put", "patch", "delete"].contains(&method.as_str()) {
                continue;
            }
            let id = op["operationId"].as_str().expect("operationId");
            out.insert((method.to_uppercase(), path.clone(), id.to_string()));
        }
    }
    out
}

fn table(surface: Surface) -> BTreeSet<(String, String, String)> {
    routes::ROUTES
        .iter()
        .filter(|r| r.surface == surface)
        .map(|r| {
            (
                r.method.to_string(),
                r.template.to_string(),
                r.operation_id.to_string(),
            )
        })
        .collect()
}

fn status_of(response: &str) -> u16 {
    response
        .get(9..12)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0)
}

fn header_of<'a>(response: &'a str, name: &str) -> Option<&'a str> {
    let (head, _) = response.split_once("\r\n\r\n")?;
    head.lines().skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim()
            .eq_ignore_ascii_case(name)
            .then_some(value.trim())
    })
}

fn body_of(response: &str) -> serde_json::Value {
    response
        .split_once("\r\n\r\n")
        .and_then(|(_, body)| serde_json::from_str(body).ok())
        .unwrap_or(serde_json::Value::Null)
}

/// A concrete path for a template: every `{param}` becomes a sample value.
fn instantiate(template: &str) -> String {
    template
        .split('/')
        .map(|segment| {
            if segment.starts_with('{') {
                "spec-sample"
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn call(state: &AppState, surface: Surface, method: &str, path: &str) -> String {
    let headers = match surface {
        Surface::Admin => {
            HashMap::from([("authorization".to_string(), format!("Bearer {ADMIN_KEY}"))])
        }
        Surface::Public => HashMap::new(),
    };
    let tenant = match surface {
        Surface::Admin => SERVICE_TENANT,
        Surface::Public => "spec-tenant",
    };
    route(
        &format!("{method} {path} HTTP/1.1\r\n"),
        &headers,
        b"{}",
        tenant,
        state,
    )
}

/// A route-level miss: the router knew no handler for the request.
fn unrouted(response: &str) -> bool {
    let status = status_of(response);
    status == 405 || (status == 404 && body_of(response)["error"] == "not found")
}

#[test]
fn public_routes_match_spec() {
    assert_eq!(
        operations(PUBLIC_SPEC),
        table(Surface::Public),
        "api/openapi/v1/kosha.openapi.json and routes::ROUTES (public) differ: \
         add the row, or the rpc to proto/kosha/v1/kosha.proto, then `make openapi`"
    );
    assert_eq!(
        operations(ADMIN_SPEC),
        table(Surface::Admin),
        "api/openapi/v1/kosha-admin.openapi.json and routes::ROUTES (admin) differ: \
         run `make openapi`"
    );

    let mut state = AppState::new(tests::test_dir());
    state.admin_auth_key = Some(ADMIN_KEY.into());
    for route in routes::ROUTES {
        let path = instantiate(route.template);
        let response = call(&state, route.surface, route.method, &path);
        assert!(
            !unrouted(&response),
            "{} {} ({}) reached no handler: {response}",
            route.method,
            path,
            route.operation_id
        );

        // A method the path does not take: 405, `Allow` naming every method
        // a spec operation serves on that concrete path (a literal path
        // such as `/v1/admin/deletions/resume` also fits `{id}` templates).
        let mut allowed: Vec<&str> = routes::ROUTES
            .iter()
            .filter(|other| routes::lookup(other.method, &path) == Some(*other))
            .map(|other| other.method)
            .collect();
        allowed.sort_unstable();
        allowed.dedup();
        assert!(allowed.contains(&route.method));
        let response = call(&state, route.surface, "PATCH", &path);
        assert_eq!(status_of(&response), 405, "PATCH {path}: {response}");
        assert_eq!(
            header_of(&response, "allow"),
            Some(allowed.join(", ").as_str()),
            "PATCH {path}"
        );
    }
}

#[test]
fn legacy_and_internal_routes_are_not_in_any_spec() {
    for spec in [PUBLIC_SPEC, ADMIN_SPEC] {
        for (_, path, _) in operations(spec) {
            assert!(path.starts_with("/v1/"), "{path}");
            assert!(!path.starts_with("/v1/internal"), "{path}");
            assert!(
                !["/v1/readyz", "/v1/metrics"].contains(&path.as_str()),
                "{path} is operational, not part of the contract"
            );
        }
        for legacy in LEGACY_ROUTES {
            assert!(
                !spec.contains(&format!("\"{legacy}\"")),
                "{legacy} appears in a spec"
            );
        }
    }
    for (_, path, _) in operations(PUBLIC_SPEC) {
        assert!(!path.starts_with("/v1/admin"), "{path} in the public spec");
    }
}

#[test]
fn spec_is_the_wire_contract() {
    let doc: serde_json::Value = serde_json::from_str(PUBLIC_SPEC).unwrap();
    assert_ne!(doc["info"]["version"], "version not set");
    let schemes = &doc["components"]["securitySchemes"];
    assert_eq!(schemes["BearerAuth"]["name"], "Authorization");
    assert_eq!(schemes["ApiKeyAuth"]["name"], "X-Api-Key");
    assert!(!PUBLIC_SPEC.contains("rpcStatus"));
    assert!(!ADMIN_SPEC.contains("rpcStatus"));
    // The bodies the server actually reads (plan §1.2).
    let search = &doc["components"]["schemas"]["KoshaServiceSearchBody"]["properties"];
    for field in [
        "aggs",
        "bm25_params",
        "search_after",
        "consistency",
        "operator",
    ] {
        assert!(search.get(field).is_some(), "SearchRequest lacks {field}");
    }
    assert!(search.get("aggregations").is_none());
    assert_eq!(
        search["consistency"]["enum"],
        serde_json::json!(["strong", "eventual"])
    );
    let counts = &doc["components"]["schemas"]["v1IndexDocumentsResponse"]["properties"];
    assert_eq!(counts["indexed_count"]["type"], "integer");
}

fn request(method: &str, target: &str, headers: &[(&str, &str)]) -> http::Request {
    http::Request {
        method: method.into(),
        target: target.into(),
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        body: Vec::new(),
    }
}

/// Every response, success or error, carries `X-Request-Id` (the caller's,
/// sanitized, or a generated one) and `Kosha-Api-Version`; an error body
/// echoes the same id (#350, ADR-350 §a.2, §b).
#[test]
fn every_response_carries_request_id_and_api_version() {
    let state = AppState::new(tests::test_dir());
    let ok = dispatch(&state, request("GET", "/v1/healthz", &[]));
    assert_eq!(ok.status(), 200);
    assert_eq!(ok.header("kosha-api-version"), Some(api_error::API_VERSION));
    assert_eq!(ok.header("x-request-id").map(str::len), Some(16));

    let mut missing = dispatch(
        &state,
        request(
            "GET",
            "/v1/namespaces/nope/stats",
            &[("x-request-id", "trace-42")],
        ),
    );
    assert_eq!(missing.status(), 404);
    assert_eq!(missing.header("x-request-id"), Some("trace-42"));
    let body: serde_json::Value = serde_json::from_slice(&missing.body_bytes()).unwrap();
    assert_eq!(body["code"], "namespace_not_found");
    assert_eq!(body["request_id"], "trace-42");
    assert_eq!(body["error"], "namespace 'nope' not found");
    assert_eq!(body["retryable"], false);
}

/// A bad filter is the caller's error (400), not a 500.
#[test]
fn an_invalid_filter_is_a_400() {
    let state = AppState::new(tests::test_dir());
    let headers = HashMap::new();
    let docs = serde_json::json!({"documents": [{"id": "1", "fields": [
        {"name": "title", "field_type": "Text", "value": "hello"}]}]});
    let line = "POST /v1/namespaces/f/documents HTTP/1.1\r\n";
    let indexed = route(line, &headers, docs.to_string().as_bytes(), "t", &state);
    assert_eq!(status_of(&indexed), 200, "{indexed}");
    let search = serde_json::json!({"query_text": "hello", "filter": {"no_such_clause": {}}});
    let line = "POST /v1/namespaces/f/search HTTP/1.1\r\n";
    let response = route(line, &headers, search.to_string().as_bytes(), "t", &state);
    assert_eq!(status_of(&response), 400, "{response}");
    let code = body_of(&response)["code"].clone();
    assert!(
        code == "invalid_query" || code == "invalid_argument",
        "{response}"
    );
}

/// Plan acceptance 7, as ADR-350 §c restates it: with the pod default off
/// and tenant B on `KOSHA_LEGACY_ROUTES_TENANTS`, A is refused and B is
/// served; with no allowlist every tenant is refused.
#[test]
fn legacy_routes_follow_the_pod_default_and_the_allowlist() {
    let allow_b = auth::parse_tenant_list(Some(" b , ,c"));
    assert_eq!(allow_b.len(), 2);
    assert!(!auth::legacy_routes_allowed(false, &allow_b, "a"));
    assert!(auth::legacy_routes_allowed(false, &allow_b, "b"));
    let none = auth::parse_tenant_list(None);
    assert!(!auth::legacy_routes_allowed(false, &none, "a"));
    assert!(!auth::legacy_routes_allowed(false, &none, "b"));
    // Pod default on (today's built-in default): everyone.
    assert!(auth::legacy_routes_allowed(true, &none, "a"));
}

/// The same through `route()`: with the pod default off and tenant B
/// allowlisted, A's `POST /index` is a 410 naming the replacement and B's
/// is served; with no allowlist both are refused; `/v1` is never gated.
#[test]
fn legacy_flag_off_and_allowlist_through_the_router() {
    let state = AppState::new(tests::test_dir());
    let headers = HashMap::new();
    let body = serde_json::json!({"namespace": "docs", "documents": [{"id": "1", "fields": [
        {"name": "title", "field_type": "Text", "value": "hello"}]}]})
    .to_string();
    let index = |tenant: &str| {
        route(
            "POST /index HTTP/1.1\r\n",
            &headers,
            body.as_bytes(),
            tenant,
            &state,
        )
    };
    auth::with_legacy_policy(false, "b", || {
        let refused = index("a");
        assert_eq!(status_of(&refused), 410, "{refused}");
        let refused = body_of(&refused);
        assert_eq!(refused["code"], "legacy_route_disabled");
        assert_eq!(
            refused["details"]["replacement"],
            "POST /v1/namespaces/{namespace}/documents"
        );
        let served = index("b");
        assert_eq!(status_of(&served), 200, "{served}");
        assert!(header_of(&served, "deprecation").is_some(), "{served}");
        let v1 = route(
            "POST /v1/namespaces/docs/documents HTTP/1.1\r\n",
            &headers,
            serde_json::json!({"documents": [{"id": "2", "fields": [
                {"name": "title", "field_type": "Text", "value": "hi"}]}]})
            .to_string()
            .as_bytes(),
            "a",
            &state,
        );
        assert_eq!(status_of(&v1), 200, "{v1}");
    });
    auth::with_legacy_policy(false, "", || {
        assert_eq!(status_of(&index("a")), 410);
        assert_eq!(status_of(&index("b")), 410);
    });
    // Back to the process default (on): served again.
    assert_eq!(status_of(&index("a")), 200);
}

/// A missing namespace is `namespace_not_found` on every route, delete
/// included (ADR-350 §b).
#[test]
fn deleting_a_missing_namespace_is_namespace_not_found() {
    let state = AppState::new(tests::test_dir());
    let response = route(
        "DELETE /v1/namespaces/nope HTTP/1.1\r\n",
        &HashMap::new(),
        b"",
        "acme",
        &state,
    );
    assert_eq!(status_of(&response), 404, "{response}");
    let body = body_of(&response);
    assert_eq!(body["code"], "namespace_not_found", "{response}");
    assert_eq!(body["details"]["namespace"], "nope");
    assert_eq!(body["error"], "namespace 'nope' not found");
}

/// Every operation in both documents lists the codes it can return
/// (`x-kosha-error-codes`, for typed SDK errors), and each is a real code.
#[test]
fn every_operation_lists_its_error_codes() {
    let known: BTreeSet<&str> = api_error::ErrorCode::ALL
        .iter()
        .map(|c| c.as_str())
        .collect();
    for spec in [PUBLIC_SPEC, ADMIN_SPEC] {
        let doc: serde_json::Value = serde_json::from_str(spec).unwrap();
        for (path, item) in doc["paths"].as_object().unwrap() {
            for (method, op) in item.as_object().unwrap() {
                if !["get", "post", "put", "patch", "delete"].contains(&method.as_str()) {
                    continue;
                }
                let codes = op["x-kosha-error-codes"]
                    .as_array()
                    .unwrap_or_else(|| panic!("{method} {path} has no x-kosha-error-codes"));
                for code in codes {
                    let code = code.as_str().unwrap();
                    assert!(known.contains(code), "{method} {path}: unknown code {code}");
                }
            }
        }
    }
}

/// While a legacy route is served it says it is going away, and the
/// cutover is counted; a /v1 response carries no deprecation headers.
#[test]
fn legacy_responses_carry_deprecation_headers_and_are_counted() {
    let state = AppState::new(tests::test_dir());
    let headers = HashMap::new();
    let body = serde_json::json!({"namespace": "docs", "documents": [{"id": "1", "fields": [
        {"name": "title", "field_type": "Text", "value": "hello"}]}]});
    let legacy = route(
        "POST /index HTTP/1.1\r\n",
        &headers,
        body.to_string().as_bytes(),
        "acme",
        &state,
    );
    assert_eq!(status_of(&legacy), 200, "{legacy}");
    assert_eq!(header_of(&legacy, "deprecation"), Some("@1797292800"));
    assert_eq!(
        header_of(&legacy, "sunset"),
        Some("Tue, 01 Jun 2027 00:00:00 GMT")
    );
    assert!(header_of(&legacy, "link")
        .unwrap()
        .ends_with("versioning.md#legacy-routes>; rel=\"deprecation\""));
    let exposition = state.metrics.render(&metrics::Scrape::default());
    assert!(
        exposition.contains("kosha_legacy_requests_total{route=\"/index\"} 1"),
        "{exposition}"
    );

    let v1 = route(
        "GET /v1/namespaces HTTP/1.1\r\n",
        &headers,
        b"",
        "acme",
        &state,
    );
    assert_eq!(status_of(&v1), 200);
    assert_eq!(header_of(&v1, "deprecation"), None);
    assert_eq!(header_of(&v1, "sunset"), None);
}

/// A disabled legacy route names its replacement.
#[test]
fn a_disabled_legacy_route_names_its_replacement() {
    let raw = legacy_route_disabled("/index");
    assert_eq!(status_of(&raw), 410);
    let body = body_of(&raw);
    assert_eq!(body["code"], "legacy_route_disabled");
    assert_eq!(body["retryable"], false);
    assert_eq!(body["details"]["route"], "/index");
    assert_eq!(
        body["details"]["replacement"],
        "POST /v1/namespaces/{namespace}/documents"
    );
}
