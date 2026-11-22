//! The route table against the checked-in OpenAPI documents (#350,
//! ADR-350 §d "Route-to-spec sync"): every operation the server serves is
//! in the spec, every spec operation reaches a handler, and the legacy and
//! internal routes are in neither document.

use std::collections::{BTreeSet, HashMap};

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
