//! Route-level tests for tenant scoping (#234) and admin auth / body
//! limits (#235). Kept out of `main.rs`'s own test module so this suite
//! doesn't collide with unrelated edits there.

use crate::auth::{self, Auth, KeyStore, Principal};
use crate::*;
use kosha_core::{Document, DocumentId, Field};

fn state() -> AppState {
    let dir = std::env::temp_dir().join(format!(
        "kosha-auth-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    AppState::new(dir)
}

fn tenant(t: &str) -> Principal {
    Principal {
        tenant: t.into(),
        admin: false,
    }
}

fn admin() -> Principal {
    Principal {
        tenant: "default".into(),
        admin: true,
    }
}

fn call(p: &Principal, line: &str, body: &[u8], s: &AppState) -> String {
    route_as(&format!("{line} HTTP/1.1\r\n"), &HashMap::new(), body, p, s)
}

fn status(resp: &str) -> u16 {
    resp.split(' ')
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0)
}

fn body_json(resp: &str) -> serde_json::Value {
    let body = resp.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
    serde_json::from_str(body).unwrap_or(serde_json::Value::Null)
}

fn index_body(ns: &str, id: &str, text: &str) -> Vec<u8> {
    serde_json::to_vec(&IndexRequest {
        namespace: NamespaceId(ns.into()),
        documents: vec![Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("title", text)],
        }],
    })
    .unwrap()
}

fn total_hits(resp: &str) -> u64 {
    body_json(resp)["total_hits"].as_u64().unwrap_or(0)
}

/// Tenant A indexes `docs` through the legacy routes (the Python client's
/// path) and can read it back through both legacy and v1 routes.
fn seed_tenant_a(s: &AppState) -> Principal {
    let a = tenant("acme");
    let r = call(
        &a,
        "POST /index",
        &index_body("docs", "a1", "secret alpha"),
        s,
    );
    assert_eq!(status(&r), 200, "{r}");
    let r = call(&a, "POST /flush", br#"{"namespace":"docs"}"#, s);
    assert_eq!(status(&r), 200, "{r}");
    assert!(
        s.indexer
            .manifest(&NamespaceId("acme/docs".into()))
            .is_some(),
        "legacy index must land in the tenant-scoped namespace"
    );
    assert!(
        s.indexer.manifest(&NamespaceId("docs".into())).is_none(),
        "legacy index must not create a bare (unscoped) namespace"
    );
    a
}

#[test]
fn legacy_and_v1_routes_resolve_to_the_same_tenant_namespace() {
    let s = state();
    let a = seed_tenant_a(&s);

    let q = br#"{"namespace":"docs","query_text":"secret","max_results":5}"#;
    let r = call(&a, "POST /search", q, &s);
    assert_eq!((status(&r), total_hits(&r)), (200, 1), "{r}");

    let r = call(&a, "GET /search?ns=docs&q=secret", b"", &s);
    assert_eq!((status(&r), total_hits(&r)), (200, 1), "{r}");

    let r = call(
        &a,
        "POST /v1/namespaces/docs/search",
        br#"{"query_text":"secret","max_results":5}"#,
        &s,
    );
    assert_eq!((status(&r), total_hits(&r)), (200, 1), "{r}");

    let r = call(
        &a,
        "POST /exists",
        br#"{"namespace":"docs","ids":["a1"]}"#,
        &s,
    );
    assert_eq!(body_json(&r)["existing"], serde_json::json!(["a1"]), "{r}");
}

#[test]
fn tenant_b_cannot_read_or_probe_tenant_a_namespace() {
    let s = state();
    let _a = seed_tenant_a(&s);
    let b = tenant("beta");

    // Search, legacy POST + GET and v1: B's "docs" is beta/docs → 404.
    let q = br#"{"namespace":"docs","query_text":"secret","max_results":5}"#;
    assert_eq!(status(&call(&b, "POST /search", q, &s)), 404);
    assert_eq!(
        status(&call(&b, "GET /search?ns=docs&q=secret", b"", &s)),
        404
    );
    let v1q = br#"{"query_text":"secret","max_results":5}"#;
    assert_eq!(
        status(&call(&b, "POST /v1/namespaces/docs/search", v1q, &s)),
        404
    );

    // Naming A's namespace explicitly is rejected, not resolved.
    let q = br#"{"namespace":"acme/docs","query_text":"secret","max_results":5}"#;
    assert_eq!(status(&call(&b, "POST /search", q, &s)), 400);
    assert_eq!(
        status(&call(&b, "GET /search?ns=acme%2Fdocs&q=secret", b"", &s)),
        400
    );
    let q = br#"{"namespace":"../acme/docs","query_text":"secret"}"#;
    assert_eq!(status(&call(&b, "POST /search", q, &s)), 400);
    for line in [
        "POST /v1/namespaces/acme%2Fdocs/search",
        "POST /v1/namespaces/..%2Facme%2Fdocs/search",
        "POST /v1/namespaces/%2E%2E/search",
    ] {
        assert_eq!(status(&call(&b, line, v1q, &s)), 404, "{line}");
    }

    // exists-probe, legacy + v1: never reports A's id as present.
    for (line, body) in [
        ("POST /exists", &br#"{"namespace":"docs","ids":["a1"]}"#[..]),
        ("POST /v1/namespaces/docs/exists", &br#"{"ids":["a1"]}"#[..]),
    ] {
        let r = call(&b, line, body, &s);
        let existing = &body_json(&r)["existing"];
        assert!(
            status(&r) == 404 || existing == &serde_json::json!([]),
            "{line} leaked A's ids: {r}"
        );
    }
    let r = call(
        &b,
        "POST /exists",
        br#"{"namespace":"acme/docs","ids":["a1"]}"#,
        &s,
    );
    assert_eq!(status(&r), 400, "{r}");
}

#[test]
fn tenant_b_cannot_write_flush_or_delete_tenant_a_namespace() {
    let s = state();
    let a = seed_tenant_a(&s);
    let b = tenant("beta");
    let a_ns = NamespaceId("acme/docs".into());
    let a_version = s.indexer.manifest(&a_ns).unwrap().version;

    // delete, legacy + v1: B's docs doesn't exist → 404; A's is untouched.
    let del = br#"{"namespace":"docs","filter":{"match_all":{}}}"#;
    assert_eq!(status(&call(&b, "POST /delete", del, &s)), 404);
    let v1del = br#"{"filter":{"match_all":{}}}"#;
    assert_eq!(
        status(&call(&b, "POST /v1/namespaces/docs/delete", v1del, &s)),
        404
    );
    let del_a = br#"{"namespace":"acme/docs","filter":{"match_all":{}}}"#;
    assert_eq!(status(&call(&b, "POST /delete", del_a, &s)), 400);

    // index / replace by B land in beta/docs, never in acme/docs.
    let r = call(
        &b,
        "POST /index",
        &index_body("docs", "a1", "overwritten"),
        &s,
    );
    assert_eq!(status(&r), 200, "{r}");
    let r = call(
        &b,
        "POST /replace",
        &index_body("docs", "a1", "replaced"),
        &s,
    );
    assert_eq!(status(&r), 200, "{r}");
    let r = call(&b, "POST /index", &index_body("acme/docs", "a1", "x"), &s);
    assert_eq!(status(&r), 400, "{r}");
    let r = call(
        &b,
        "POST /v1/namespaces/docs/documents",
        &index_body("ignored", "a2", "v1 write"),
        &s,
    );
    assert_eq!(status(&r), 200, "{r}");

    // flush: named, unnamed (tenant-wide) and v1 — none touch acme/docs.
    for (line, body) in [
        ("POST /flush", &br#"{"namespace":"docs"}"#[..]),
        ("POST /flush", &b"{}"[..]),
        ("POST /v1/namespaces/docs/flush", &b"{}"[..]),
    ] {
        assert_eq!(status(&call(&b, line, body, &s)), 200, "{line}");
    }
    assert_eq!(
        status(&call(
            &b,
            "POST /flush",
            br#"{"namespace":"acme/docs"}"#,
            &s
        )),
        400
    );

    assert_eq!(s.indexer.manifest(&a_ns).unwrap().version, a_version);
    assert!(s
        .indexer
        .manifest(&NamespaceId("beta/docs".into()))
        .is_some());
    // A still sees exactly its own document text.
    let r = call(
        &a,
        "POST /search",
        br#"{"namespace":"docs","query_text":"secret","max_results":5}"#,
        &s,
    );
    assert_eq!(total_hits(&r), 1, "{r}");
    let r = call(
        &a,
        "POST /search",
        br#"{"namespace":"docs","query_text":"overwritten","max_results":5}"#,
        &s,
    );
    assert_eq!(total_hits(&r), 0, "{r}");
}

#[test]
fn stats_are_tenant_filtered_unless_admin() {
    let s = state();
    let a = seed_tenant_a(&s);
    let b = tenant("beta");
    call(
        &b,
        "POST /index",
        &index_body("other", "b1", "beta doc"),
        &s,
    );
    call(&b, "POST /flush", br#"{"namespace":"other"}"#, &s);

    for line in ["GET /stats", "GET /v1/stats"] {
        let names = |p: &Principal| -> Vec<String> {
            let mut v: Vec<String> = body_json(&call(p, line, b"", &s))["namespaces"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| n["namespace"].as_str().unwrap().to_string())
                .collect();
            v.sort();
            v
        };
        assert_eq!(names(&a), vec!["docs"], "{line}");
        assert_eq!(names(&b), vec!["other"], "{line}");
        assert_eq!(names(&admin()), vec!["acme/docs", "beta/other"], "{line}");

        let tenant_view = body_json(&call(&a, line, b"", &s));
        assert_eq!(tenant_view["total_documents"], 1);
        assert!(tenant_view.get("cache_root").is_none());
        assert!(tenant_view.get("s3_bucket").is_none());
        assert!(body_json(&call(&admin(), line, b"", &s))
            .get("cache_root")
            .is_some());
    }
}

#[test]
fn admin_keys_keep_raw_namespace_names_on_legacy_routes() {
    // Pre-tenancy data lives under bare names (e.g. migrated indices); the
    // bootstrap/admin key keeps addressing it exactly as before.
    let s = state();
    let r = call(
        &admin(),
        "POST /index",
        &index_body("legacy-ns", "d1", "hello"),
        &s,
    );
    assert_eq!(status(&r), 200, "{r}");
    let r = call(&admin(), "POST /flush", br#"{"namespace":"legacy-ns"}"#, &s);
    assert_eq!(status(&r), 200, "{r}");
    assert!(s
        .indexer
        .manifest(&NamespaceId("legacy-ns".into()))
        .is_some());
    assert!(s
        .indexer
        .manifest(&NamespaceId("default/legacy-ns".into()))
        .is_none());
}

const ADMIN_ROUTES: &[&str] = &[
    "POST /v1/admin/api-keys",
    "POST /v1/admin/rebuild-filter-blooms",
    "POST /v1/admin/backfill-offset-tables",
    "POST /v1/admin/compact-namespace",
    "POST /v1/admin/import-namespace",
    "GET /v1/admin/anything",
    "GET /internal/segment/acme/docs/seg/doc_store.bin",
];

#[test]
fn non_admin_key_gets_403_on_every_admin_route() {
    let s = state();
    for line in ADMIN_ROUTES {
        let r = call(&tenant("acme"), line, br#"{"namespace":"docs"}"#, &s);
        assert_eq!(status(&r), 403, "{line}: {r}");
    }
}

#[test]
fn admin_key_passes_the_admin_gate() {
    let s = state();
    for line in ADMIN_ROUTES {
        // Bodies are deliberately incomplete: anything but 403 shows the
        // request reached its handler.
        let r = call(&admin(), line, b"{}", &s);
        assert_ne!(status(&r), 403, "{line}: {r}");
    }
}

#[test]
fn legacy_routes_answer_410_when_disabled() {
    let p = tenant("acme");
    for line in [
        "POST /index",
        "POST /exists",
        "POST /replace",
        "GET /search?ns=x&q=y",
        "POST /search",
        "POST /flush",
        "POST /delete",
        "GET /stats",
    ] {
        let r = authorize_route_with(&format!("{line} HTTP/1.1\r\n"), &p, false)
            .unwrap_or_else(|| panic!("{line} must be refused"));
        assert_eq!(status(&r), 410, "{line}");
        assert!(authorize_route_with(&format!("{line} HTTP/1.1\r\n"), &p, true).is_none());
    }
    for line in [
        "POST /v1/namespaces/x/search",
        "GET /v1/stats",
        "GET /healthz",
    ] {
        assert!(authorize_route_with(&format!("{line} HTTP/1.1\r\n"), &p, false).is_none());
    }
}

fn headers_with_key(key: &str) -> HashMap<String, String> {
    HashMap::from([("authorization".to_string(), format!("Bearer {key}"))])
}

#[test]
fn authenticate_fails_closed_with_no_keys() {
    let empty = Auth::Keys(KeyStore::with_keys(HashMap::new()));
    assert!(authenticate(&empty, &HashMap::new()).is_err());
    let err = authenticate(&empty, &headers_with_key("anything")).unwrap_err();
    assert_eq!(status(&err), 401);
    // Only an explicit disable lets an unkeyed request through.
    assert_eq!(
        authenticate(&Auth::Disabled, &HashMap::new()).unwrap(),
        Principal::dev()
    );
}

#[test]
fn created_key_works_after_refresh_and_revoked_key_stops() {
    let store = KeyStore::with_keys(auth::parse_env_keys(None, Some("sk-boot"), None));
    let a = Auth::Keys(store);
    let Auth::Keys(store) = &a else {
        unreachable!()
    };

    assert!(
        authenticate(&a, &headers_with_key("sk-boot"))
            .unwrap()
            .admin
    );
    assert!(authenticate(&a, &headers_with_key("sk-new")).is_err());

    store.replace_db_keys(HashMap::from([("sk-new".into(), "acme".into())]));
    assert_eq!(
        authenticate(&a, &headers_with_key("sk-new")).unwrap(),
        tenant("acme")
    );

    store.replace_db_keys(HashMap::new());
    assert_eq!(
        status(&authenticate(&a, &headers_with_key("sk-new")).unwrap_err()),
        401
    );
}

#[test]
fn oversized_body_gets_413_without_being_read() {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let s = Arc::new(state());
    std::thread::spawn(move || serve(listener, s, Duration::from_secs(5)));

    let mut conn = std::net::TcpStream::connect(addr).unwrap();
    conn.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // Declare 10 GiB but send none of it: a server that tried to read (or
    // allocate) the body would block until its I/O timeout instead.
    conn.write_all(b"POST /index HTTP/1.1\r\nhost: x\r\ncontent-length: 10737418240\r\n\r\n")
        .unwrap();
    let mut resp = String::new();
    conn.read_to_string(&mut resp).unwrap();
    assert!(resp.starts_with("HTTP/1.1 413 Payload Too Large"), "{resp}");
}

#[test]
fn search_get_scoping_encodes_the_scoped_namespace() {
    let line = scope_legacy_search_get(
        "GET /search?q=a+b&ns=my%20ns&max_results=3 HTTP/1.1\r\n",
        &tenant("acme"),
    )
    .unwrap();
    assert_eq!(
        line,
        "GET /search?q=a+b&ns=acme/my%20ns&max_results=3 HTTP/1.1\r\n"
    );
    assert_eq!(
        status(
            &scope_legacy_search_get("GET /search?q=a HTTP/1.1\r\n", &tenant("acme")).unwrap_err()
        ),
        400
    );
}
