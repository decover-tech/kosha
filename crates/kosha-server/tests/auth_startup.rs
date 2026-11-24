//! Process-level auth behavior (#235): fail-closed startup and the
//! explicit `KOSHA_AUTH=disabled` opt-out, against the real binary.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn server(envs: &[(&str, &str)]) -> (Command, u16) {
    let port = free_port();
    let dir =
        std::env::temp_dir().join(format!("kosha-auth-startup-{}-{port}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_kosha-server"));
    cmd.env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("KOSHA_HTTP_PORT", port.to_string())
        .env("KOSHA_DATA_DIR", dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    (cmd, port)
}

fn http(port: u16, raw: &str) -> Option<String> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    s.write_all(raw.as_bytes()).ok()?;
    // The server keeps connections alive (#259): read one response, framed
    // by its content-length, instead of reading to EOF.
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if s.read(&mut byte).ok()? == 0 {
            return None;
        }
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head).into_owned();
    let len: usize = head
        .lines()
        .find_map(|line| {
            let (k, v) = line.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse().ok())?
        })
        .unwrap_or(0);
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).ok()?;
    Some(head + &String::from_utf8_lossy(&body))
}

fn wait_healthy(child: &mut Child, port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Some(r) = http(port, "GET /healthz HTTP/1.1\r\n\r\n") {
            if r.starts_with("HTTP/1.1 200") {
                return;
            }
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("server exited early: {status}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("server never became healthy");
}

fn assert_refuses_to_start(envs: &[(&str, &str)]) {
    let (mut cmd, _) = server(envs);
    let out = cmd.output().expect("spawn kosha-server");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("FATAL: no API keys configured"),
        "stderr: {stderr}"
    );
}

#[test]
fn refuses_to_start_without_keys() {
    assert_refuses_to_start(&[]);
}

#[test]
fn refuses_to_start_when_database_has_no_keys_and_no_opt_out() {
    // Unreachable Postgres stands in for "DATABASE_URL set, no keys found":
    // either way the key set is empty and auth must not silently open.
    assert_refuses_to_start(&[("DATABASE_URL", "postgres://kosha:x@127.0.0.1:1/kosha")]);
}

#[test]
fn explicit_opt_out_starts_with_auth_disabled() {
    let (mut cmd, port) = server(&[("KOSHA_AUTH", "disabled")]);
    let mut child = cmd.spawn().unwrap();
    wait_healthy(&mut child, port);
    let body = r#"{"namespace":"x"}"#;
    let r = http(
        port,
        &format!(
            "POST /flush HTTP/1.1\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .unwrap();
    child.kill().ok();
    child.wait().ok();
    assert!(r.starts_with("HTTP/1.1 200"), "{r}");
}

#[test]
fn admin_key_alone_is_enough_to_start() {
    let (mut cmd, port) = server(&[("KOSHA_ADMIN_API_KEY", "sk-admin")]);
    let mut child = cmd.spawn().unwrap();
    wait_healthy(&mut child, port);
    let unkeyed = http(port, "GET /v1/stats HTTP/1.1\r\n\r\n").unwrap();
    child.kill().ok();
    child.wait().ok();
    assert!(unkeyed.starts_with("HTTP/1.1 401"), "{unkeyed}");
}

#[test]
fn keyed_server_rejects_unkeyed_and_oversized_requests() {
    let (mut cmd, port) = server(&[
        ("KOSHA_API_KEYS", "sk-tenant=acme"),
        ("KOSHA_ADMIN_API_KEY", "sk-admin"),
        ("KOSHA_MAX_BODY_BYTES", "16"),
    ]);
    let mut child = cmd.spawn().unwrap();
    wait_healthy(&mut child, port);

    let unkeyed = http(port, "GET /v1/stats HTTP/1.1\r\n\r\n").unwrap();
    let bad_key = http(port, "GET /v1/stats HTTP/1.1\r\nx-api-key: nope\r\n\r\n").unwrap();
    let tenant_admin = http(
        port,
        "POST /v1/admin/compact-namespace HTTP/1.1\r\nauthorization: Bearer sk-tenant\r\ncontent-length: 2\r\n\r\n{}",
    )
    .unwrap();
    let admin_stats = http(
        port,
        "GET /v1/stats HTTP/1.1\r\nauthorization: Bearer sk-admin\r\n\r\n",
    )
    .unwrap();
    let oversized = http(
        port,
        "POST /index HTTP/1.1\r\nauthorization: Bearer sk-tenant\r\ncontent-length: 17\r\n\r\n",
    )
    .unwrap();
    child.kill().ok();
    child.wait().ok();

    assert!(unkeyed.starts_with("HTTP/1.1 401"), "{unkeyed}");
    assert!(bad_key.starts_with("HTTP/1.1 401"), "{bad_key}");
    assert!(tenant_admin.starts_with("HTTP/1.1 403"), "{tenant_admin}");
    assert!(admin_stats.starts_with("HTTP/1.1 200"), "{admin_stats}");
    assert!(oversized.starts_with("HTTP/1.1 413"), "{oversized}");
}

#[test]
fn legacy_routes_can_be_switched_off() {
    let (mut cmd, port) = server(&[
        ("KOSHA_API_KEYS", "sk-tenant=acme"),
        ("KOSHA_LEGACY_ROUTES", "off"),
    ]);
    let mut child = cmd.spawn().unwrap();
    wait_healthy(&mut child, port);
    let body = r#"{"namespace":"x"}"#;
    let legacy = http(
        port,
        &format!(
            "POST /flush HTTP/1.1\r\nx-api-key: sk-tenant\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .unwrap();
    let v1 = http(
        port,
        "POST /v1/namespaces/x/flush HTTP/1.1\r\nx-api-key: sk-tenant\r\ncontent-length: 2\r\n\r\n{}",
    )
    .unwrap();
    child.kill().ok();
    child.wait().ok();
    assert!(legacy.starts_with("HTTP/1.1 410"), "{legacy}");
    assert!(!v1.starts_with("HTTP/1.1 410"), "{v1}");
}

/// #346: a typo in the switch for the bootstrap admin key must not leave
/// the operator guessing which way it went.
#[test]
fn invalid_bootstrap_key_switch_refuses_to_start() {
    let (mut cmd, _) = server(&[
        ("KOSHA_API_KEYS", "sk-tenant=acme"),
        ("KOSHA_ADMIN_API_KEY", "sk-admin"),
        ("KOSHA_ADMIN_BOOTSTRAP_KEY", "disable"),
    ]);
    let out = cmd.output().expect("spawn kosha-server");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("FATAL: invalid KOSHA_ADMIN_BOOTSTRAP_KEY (expected"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains("disable'"),
        "the value is echoed: {stderr}"
    );
}

/// #346: `KOSHA_ADMIN_BOOTSTRAP_KEY=disabled` turns the shared admin key
/// into a key the server does not know, and `enabled` (the default) leaves
/// it working.
#[test]
fn bootstrap_admin_key_can_be_switched_off() {
    let admin_stats = "GET /v1/stats HTTP/1.1\r\nauthorization: Bearer sk-admin\r\n\r\n";
    let admin_route =
        "GET /v1/admin/compaction-status HTTP/1.1\r\nauthorization: Bearer sk-admin\r\n\r\n";
    let tenant = "GET /v1/namespaces HTTP/1.1\r\nauthorization: Bearer sk-tenant\r\n\r\n";
    for (switch, admin_status) in [("disabled", "401"), ("enabled", "200")] {
        let (mut cmd, port) = server(&[
            ("KOSHA_API_KEYS", "sk-tenant=acme"),
            ("KOSHA_ADMIN_API_KEY", "sk-admin"),
            ("KOSHA_ADMIN_BOOTSTRAP_KEY", switch),
        ]);
        let mut child = cmd.spawn().unwrap();
        wait_healthy(&mut child, port);
        let stats = http(port, admin_stats).unwrap();
        let admin = http(port, admin_route).unwrap();
        let tenant = http(port, tenant).unwrap();
        child.kill().ok();
        child.wait().ok();
        let expected = format!("HTTP/1.1 {admin_status}");
        assert!(stats.starts_with(&expected), "{switch}: {stats}");
        assert!(admin.starts_with(&expected), "{switch}: {admin}");
        // Tenant keys are untouched either way.
        assert!(tenant.starts_with("HTTP/1.1 200"), "{switch}: {tenant}");
    }
}

/// A switched-off bootstrap key is not a configured credential: on its own
/// it must not let the server start with nothing able to authenticate.
#[test]
fn switched_off_bootstrap_key_alone_is_not_enough_to_start() {
    assert_refuses_to_start(&[
        ("KOSHA_ADMIN_API_KEY", "sk-admin"),
        ("KOSHA_ADMIN_BOOTSTRAP_KEY", "disabled"),
    ]);
}

/// Scoped, expiring, rotating keys from the Postgres registry (#346),
/// against the real binary. Opt-in, with a dedicated disposable database
/// that no manifest-writing suite (`postgres_cas`) shares: the server
/// restores every namespace it finds there at startup.
/// KOSHA_TEST_DATABASE_URL=... cargo test -p kosha-server --features postgres --test auth_startup -- --ignored
#[cfg(feature = "postgres")]
mod scoped_keys {
    use super::*;
    use serde_json::{json, Value};

    const ADMIN: &str = "sk-admin-test";

    /// One request; the status and the JSON body (`Null` if it is not JSON).
    fn call(port: u16, method: &str, path: &str, key: &str, body: Option<Value>) -> (u16, Value) {
        let body = body.map(|b| b.to_string()).unwrap_or_default();
        let raw = format!(
            "{method} {path} HTTP/1.1\r\nauthorization: Bearer {key}\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );
        let response = http(port, &raw).expect("response");
        let status = response
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .expect("status line");
        let json = response
            .split_once("\r\n\r\n")
            .and_then(|(_, body)| serde_json::from_str(body).ok())
            .unwrap_or(Value::Null);
        (status, json)
    }

    fn status(port: u16, method: &str, path: &str, key: &str, body: Option<Value>) -> u16 {
        call(port, method, path, key, body).0
    }

    fn create_key(port: u16, request: Value) -> (String, String) {
        let (code, created) = call(port, "POST", "/v1/admin/api-keys", ADMIN, Some(request));
        assert_eq!(code, 200, "{created}");
        (
            created["api_key"].as_str().expect("api_key").to_string(),
            created["key_id"].as_str().expect("key_id").to_string(),
        )
    }

    /// Poll until `probe` is true, for up to ten seconds.
    fn eventually(what: &str, mut probe: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !probe() {
            assert!(Instant::now() < deadline, "timed out waiting for: {what}");
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    struct Server(Child, u16);

    impl Drop for Server {
        fn drop(&mut self) {
            self.0.kill().ok();
            self.0.wait().ok();
        }
    }

    fn start(database_url: &str) -> Server {
        let (mut cmd, port) = server(&[
            ("DATABASE_URL", database_url),
            ("KOSHA_ADMIN_API_KEY", ADMIN),
            ("KOSHA_API_KEY_REFRESH_SECS", "1"),
        ]);
        // Nothing reads the pipes; don't let a chatty server block on them.
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
        let mut child = cmd.spawn().unwrap();
        wait_healthy(&mut child, port);
        Server(child, port)
    }

    #[test]
    #[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
    fn scopes_namespaces_rotation_expiry_and_revocation_are_enforced() {
        let database_url =
            std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
        let server = start(&database_url);
        let port = server.1;
        let tenant = format!("t{}x{port}", std::process::id());
        let docs = "/v1/namespaces/docs";
        let stats = format!("{docs}/stats");
        let flush = format!("{docs}/flush");
        let namespace = |name: &str| Some(json!({ "namespace": name }));

        // The tenant belongs to an organization, so what the admin key
        // routes do below can be read back from that organization's audit
        // log at the end.
        use kosha_control::orgs::{self, Actor};
        let org = {
            let store = kosha_control::PgStore::new(&database_url).unwrap();
            let owner = orgs::create_user(
                &store,
                &Actor::Operator,
                &format!("{tenant}@example.test"),
                "",
            )
            .unwrap();
            let org = orgs::create_org(
                &store,
                &Actor::Operator,
                &format!("org-{tenant}"),
                "Audit",
                Some(&owner.id),
            )
            .unwrap();
            orgs::link_tenant(&store, &Actor::Operator, &org.id, Some(&tenant), None).unwrap();
            org
        };

        // A key created the pre-#346 way (tenant only) can do everything.
        let (full, full_id) = create_key(port, json!({ "tenant_id": tenant }));
        assert_eq!(
            status(port, "POST", "/v1/namespaces", &full, namespace("docs")),
            200
        );
        assert_eq!(
            status(port, "POST", "/v1/namespaces", &full, namespace("other")),
            200
        );
        assert_eq!(status(port, "POST", &flush, &full, Some(json!({}))), 200);
        assert_eq!(status(port, "GET", &stats, &full, None), 200);

        // A read-only key reads, and cannot write or administer.
        let (ro, ro_id) = create_key(port, json!({"tenant_id": tenant, "scopes": ["read"]}));
        assert_eq!(status(port, "GET", &stats, &ro, None), 200);
        assert_eq!(status(port, "GET", "/v1/namespaces", &ro, None), 200);
        let search = status(
            port,
            "POST",
            &format!("{docs}/search"),
            &ro,
            Some(json!({"query_text": "x"})),
        );
        assert!(search != 401 && search != 403, "search answered {search}");
        for (method, path, body) in [
            (
                "POST",
                format!("{docs}/documents"),
                json!({"documents": []}),
            ),
            ("POST", flush.clone(), json!({})),
            ("POST", format!("{docs}/delete"), json!({})),
            (
                "POST",
                "/index".to_string(),
                json!({"namespace": "docs", "documents": []}),
            ),
            (
                "POST",
                "/v1/namespaces".to_string(),
                json!({"namespace": "mine"}),
            ),
            ("DELETE", docs.to_string(), json!({})),
        ] {
            let (code, answer) = call(port, method, &path, &ro, Some(body));
            assert_eq!(code, 403, "{method} {path}: {answer}");
            assert!(
                answer["error"].as_str().unwrap_or("").contains("scope"),
                "{method} {path}: {answer}"
            );
        }
        // Nothing the read-only key tried took effect.
        assert_eq!(status(port, "GET", &stats, &full, None), 200);
        let (code, listed) = call(port, "GET", "/v1/namespaces", &full, None);
        assert_eq!(code, 200, "{listed}");
        let mut names: Vec<&str> = listed["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|n| n["name"].as_str())
            .collect();
        names.sort_unstable();
        assert_eq!(names, ["docs", "other"]);

        // A namespace-restricted key reaches only its namespaces.
        let (docs_only, _) = create_key(
            port,
            json!({"tenant_id": tenant, "scopes": ["read", "write"], "namespaces": ["docs"]}),
        );
        let other_flush = "/v1/namespaces/other/flush";
        assert_eq!(status(port, "GET", &stats, &docs_only, None), 200);
        assert_eq!(
            status(port, "POST", &flush, &docs_only, Some(json!({}))),
            200
        );
        assert_eq!(
            status(port, "GET", "/v1/namespaces/other/stats", &docs_only, None),
            403
        );
        assert_eq!(
            status(port, "POST", other_flush, &docs_only, Some(json!({}))),
            403
        );
        assert_eq!(
            status(port, "POST", "/flush", &docs_only, namespace("other")),
            403
        );
        assert_eq!(status(port, "GET", "/v1/namespaces", &docs_only, None), 403);

        // A tenant's own usage and billing are `read` routes (#426). Both
        // features are off on this pod, so a key that may read is answered
        // 404 by the route itself; a key without `read`, or one limited to
        // some namespaces, is refused before it.
        let (write_only, _) = create_key(port, json!({"tenant_id": tenant, "scopes": ["write"]}));
        for path in ["/v1/usage", "/v1/billing"] {
            assert_eq!(status(port, "GET", path, &full, None), 404, "{path}");
            assert_eq!(status(port, "GET", path, &ro, None), 404, "{path}");
            assert_eq!(status(port, "GET", path, &write_only, None), 403, "{path}");
            assert_eq!(status(port, "GET", path, &docs_only, None), 403, "{path}");
        }

        // No tenant key reaches the key registry, whatever its scopes.
        assert_eq!(status(port, "GET", "/v1/admin/api-keys", &full, None), 403);

        // Rotation: old and new keys both work during the overlap; then the
        // old one expires on its own, with no restart.
        let (code, rotated) = call(
            port,
            "POST",
            "/v1/admin/api-keys/rotate",
            ADMIN,
            Some(json!({"key_id": ro_id, "overlap_secs": 3})),
        );
        assert_eq!(code, 200, "{rotated}");
        let ro_new = rotated["api_key"].as_str().unwrap().to_string();
        let ro_new_id = rotated["key_id"].as_str().unwrap().to_string();
        assert_eq!(rotated["scopes"], json!(["read"]));
        assert_eq!(rotated["replaces"]["key_id"], json!(ro_id));
        assert_eq!(status(port, "GET", &stats, &ro, None), 200);
        assert_eq!(status(port, "GET", &stats, &ro_new, None), 200);
        // The replacement is as read-only as the key it replaces.
        assert_eq!(status(port, "POST", &flush, &ro_new, Some(json!({}))), 403);
        eventually("the rotated-out key to expire", || {
            let (code, answer) = call(port, "GET", &stats, &ro, None);
            code == 401 && answer["error"] == json!("API key expired")
        });
        assert_eq!(status(port, "GET", &stats, &ro_new, None), 200);

        // Revoked through the API: refused by this pod at once.
        let revoke = "/v1/admin/api-keys/revoke";
        let (code, answer) = call(
            port,
            "POST",
            revoke,
            ADMIN,
            Some(json!({"key_id": ro_new_id})),
        );
        assert_eq!(code, 200, "{answer}");
        assert_eq!(status(port, "GET", &stats, &ro_new, None), 401);
        assert_eq!(
            status(
                port,
                "POST",
                revoke,
                ADMIN,
                Some(json!({"key_id": ro_new_id}))
            ),
            404
        );

        // Revoked elsewhere (another pod, or SQL): refused within the
        // refresh interval.
        let registry = kosha_control::PgStore::new(&database_url).unwrap();
        assert!(registry.revoke_api_key(&full_id, "operator").unwrap());
        eventually("a revocation made elsewhere to arrive", || {
            status(port, "GET", &stats, &full, None) == 401
        });

        // The listing shows the surviving keys, their use, and no secret.
        let (code, listed) = call(
            port,
            "GET",
            &format!("/v1/admin/api-keys?tenant_id={tenant}"),
            ADMIN,
            None,
        );
        assert_eq!(code, 200, "{listed}");
        let text = listed.to_string();
        for secret in [&full, &ro, &ro_new, &docs_only] {
            assert!(!text.contains(secret.as_str()), "listing leaks a key");
        }

        // Each key the admin routes created, rotated or revoked is in the
        // audit log (#345), newest first, without a key or a whole key id.
        let audit: Vec<(String, String)> =
            orgs::list_audit(&registry, &Actor::Operator, &org.id, None, None)
                .unwrap()
                .into_iter()
                .filter(|entry| entry.action.starts_with("api_key."))
                .map(|entry| {
                    assert_eq!(entry.actor, "operator");
                    assert_eq!(entry.target_id, tenant);
                    (entry.action, entry.detail.to_string())
                })
                .collect();
        let actions: Vec<&str> = audit.iter().map(|(action, _)| action.as_str()).collect();
        assert_eq!(
            actions,
            [
                "api_key.revoked", // `full`, through the registry
                "api_key.revoked", // the rotated read-only key
                "api_key.rotated",
                "api_key.created", // write-only
                "api_key.created", // docs-only
                "api_key.created", // read-only
                "api_key.created", // full
            ]
        );
        let docs_only_row: Value = serde_json::from_str(&audit[4].1).unwrap();
        assert_eq!(docs_only_row["scopes"], json!(["read", "write"]));
        assert_eq!(docs_only_row["namespaces"], json!(["docs"]));
        for (_, detail) in &audit {
            for secret in [&full, &ro, &ro_new, &docs_only, &full_id, &ro_new_id] {
                assert!(!detail.contains(secret.as_str()), "audit leaks: {detail}");
            }
        }
        let keys = listed["api_keys"].as_array().unwrap();
        let by_id = |id: &str| keys.iter().find(|k| k["key_id"] == json!(id));
        assert!(by_id(&full_id).is_none(), "revoked keys are not listed");
        assert!(by_id(&ro_new_id).is_none(), "revoked keys are not listed");
        let old_ro = by_id(&ro_id).expect("expired keys stay listed");
        assert_eq!(old_ro["expired"], json!(true));
        assert!(old_ro["last_used_at"].as_i64().is_some(), "{old_ro}");
        let restricted = keys
            .iter()
            .find(|k| k["namespaces"] == json!(["docs"]))
            .expect("restricted key listed");
        assert_eq!(restricted["scopes"], json!(["read", "write"]));

        // Scrubbing leaves working keys working.
        let scrub = "/v1/admin/api-keys/scrub-plaintext";
        let (code, scrubbed) = call(port, "POST", scrub, ADMIN, None);
        assert_eq!(code, 200, "{scrubbed}");
        assert!(scrubbed["scrubbed"].as_u64().is_some(), "{scrubbed}");
        assert_eq!(status(port, "GET", &stats, &docs_only, None), 200);

        // A restarted server loads the same keys from their hashes.
        drop(server);
        let server = start(&database_url);
        let port = server.1;
        let reloaded = status(port, "GET", &stats, &docs_only, None);
        assert!(reloaded != 401 && reloaded != 403, "answered {reloaded}");
        assert_eq!(
            status(port, "POST", other_flush, &docs_only, Some(json!({}))),
            403
        );
        assert_eq!(status(port, "GET", &stats, &ro, None), 401);
        assert_eq!(status(port, "GET", &stats, &full, None), 401);
    }

    fn start_without_bootstrap_key(database_url: &str) -> Server {
        let (mut cmd, port) = server(&[
            ("DATABASE_URL", database_url),
            ("KOSHA_ADMIN_API_KEY", ADMIN),
            ("KOSHA_ADMIN_BOOTSTRAP_KEY", "disabled"),
            ("KOSHA_API_KEY_REFRESH_SECS", "1"),
        ]);
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
        let mut child = cmd.spawn().unwrap();
        wait_healthy(&mut child, port);
        Server(child, port)
    }

    /// Per-operator admin credentials (#346), against the real binary:
    /// issued through the bootstrap key, accepted on every admin route,
    /// audited under the operator's name, never usable as (or managed like)
    /// a tenant key, rotated, expired and revoked like one, and enough on
    /// their own once the bootstrap key is switched off.
    #[test]
    #[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
    fn operator_credentials_replace_the_shared_admin_key() {
        let database_url =
            std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
        let server = start(&database_url);
        let port = server.1;
        let run = format!("{}x{port}", std::process::id());
        let operators = "/v1/admin/operators";
        let create = |key: &str, request: Value| call(port, "POST", operators, key, Some(request));

        // The bootstrap key issues the first operator credential.
        let alice = format!("alice-{run}");
        let (code, created) = create(ADMIN, json!({"name": alice, "description": "on call"}));
        assert_eq!(code, 200, "{created}");
        assert_eq!(created["name"], json!(alice));
        let alice_key = created["api_key"].as_str().unwrap().to_string();
        let alice_id = created["key_id"].as_str().unwrap().to_string();
        // One usable credential per name: a second is a rotation.
        let (code, answer) = create(ADMIN, json!({"name": alice}));
        assert_eq!(code, 409, "{answer}");

        // It opens the admin routes the shared key opens, at once.
        assert_eq!(status(port, "GET", operators, &alice_key, None), 200);
        assert_eq!(status(port, "GET", "/v1/stats", &alice_key, None), 200);
        let (code, count) = call(
            port,
            "GET",
            "/v1/admin/api-keys/plaintext-count",
            &alice_key,
            None,
        );
        assert_eq!(code, 200, "{count}");
        let plaintext = count["plaintext_rows"].as_u64().expect("plaintext_rows");
        assert_eq!(count["scrub_due"], json!(plaintext > 0));

        // What its holder does is audited under its name.
        let (code, user) = call(
            port,
            "POST",
            "/v1/admin/users",
            &alice_key,
            Some(json!({"email": format!("ada-{run}@example.com")})),
        );
        assert_eq!(code, 200, "{user}");
        let (code, org) = call(
            port,
            "POST",
            "/v1/admin/orgs",
            &alice_key,
            Some(json!({"slug": format!("org-{run}"), "name": "Org", "owner_user_id": user["id"]})),
        );
        assert_eq!(code, 200, "{org}");
        let org_base = format!("/v1/admin/orgs/{}", org["id"].as_str().unwrap());
        // ... and the shared key's holder stays the anonymous `operator`.
        let project = json!({"slug": "one", "name": "One"});
        let projects = format!("{org_base}/projects");
        assert_eq!(status(port, "POST", &projects, ADMIN, Some(project)), 200);
        let (code, log) = call(
            port,
            "GET",
            &format!("{org_base}/audit-log"),
            &alice_key,
            None,
        );
        assert_eq!(code, 200, "{log}");
        let actors: Vec<(&str, &str)> = log["events"]
            .as_array()
            .unwrap()
            .iter()
            .rev()
            .map(|e| (e["actor"].as_str().unwrap(), e["action"].as_str().unwrap()))
            .collect();
        let by_alice = format!("operator:{alice}");
        assert_eq!(
            actors,
            [
                (by_alice.as_str(), "org.created"),
                ("operator", "project.created")
            ]
        );

        // Tenant keys and operator credentials are separate worlds: a key
        // alice issues for a tenant opens no admin route, alice's
        // credential is not in the tenant-key registry's listing, and that
        // registry's routes cannot revoke or rotate it.
        let tenant = format!("t{run}");
        let (code, tenant_key) = call(
            port,
            "POST",
            "/v1/admin/api-keys",
            &alice_key,
            Some(json!({"tenant_id": tenant})),
        );
        assert_eq!(code, 200, "{tenant_key}");
        let tenant_key = tenant_key["api_key"].as_str().unwrap().to_string();
        assert_eq!(
            status(port, "GET", "/v1/namespaces", &tenant_key, None),
            200
        );
        assert_eq!(status(port, "GET", operators, &tenant_key, None), 403);
        let (code, listed) = call(port, "GET", "/v1/admin/api-keys", ADMIN, None);
        assert_eq!(code, 200, "{listed}");
        assert!(!listed.to_string().contains(&alice_id), "{listed}");
        assert!(!listed.to_string().contains("_operator/"), "{listed}");
        for route in ["/v1/admin/api-keys/revoke", "/v1/admin/api-keys/rotate"] {
            let by_id = Some(json!({"key_id": alice_id}));
            assert_eq!(status(port, "POST", route, ADMIN, by_id), 404, "{route}");
        }
        // And the operator routes do not touch a tenant key.
        for route in ["/v1/admin/operators/revoke", "/v1/admin/operators/rotate"] {
            let by_key = Some(json!({"api_key": tenant_key}));
            assert_eq!(status(port, "POST", route, ADMIN, by_key), 404, "{route}");
        }
        assert_eq!(
            status(port, "GET", "/v1/namespaces", &tenant_key, None),
            200
        );
        assert_eq!(status(port, "GET", operators, &alice_key, None), 200);

        // The listing names the operator and shows no secret.
        let (code, listed) = call(port, "GET", operators, &alice_key, None);
        assert_eq!(code, 200, "{listed}");
        assert!(!listed.to_string().contains(&alice_key), "{listed}");
        let shown = listed["operators"]
            .as_array()
            .unwrap()
            .iter()
            .find(|o| o["key_id"] == json!(alice_id))
            .expect("alice listed");
        assert_eq!(shown["name"], json!(alice));
        assert_eq!(shown["description"], json!("on call"));

        // Rotation: both credentials work during the overlap, then the old
        // one expires on its own. alice rotates her own.
        let (code, rotated) = call(
            port,
            "POST",
            "/v1/admin/operators/rotate",
            &alice_key,
            Some(json!({"key_id": alice_id, "overlap_secs": 3})),
        );
        assert_eq!(code, 200, "{rotated}");
        assert_eq!(rotated["name"], json!(alice));
        assert_eq!(rotated["replaces"]["key_id"], json!(alice_id));
        let alice_new = rotated["api_key"].as_str().unwrap().to_string();
        assert_eq!(status(port, "GET", operators, &alice_key, None), 200);
        assert_eq!(status(port, "GET", operators, &alice_new, None), 200);
        eventually("the rotated-out credential to expire", || {
            let (code, answer) = call(port, "GET", operators, &alice_key, None);
            code == 401 && answer["error"] == json!("API key expired")
        });
        assert_eq!(status(port, "GET", operators, &alice_new, None), 200);

        // Expiry needs no refresh and no restart.
        let bob = format!("bob-{run}");
        let (code, created) = create(&alice_new, json!({"name": bob, "expires_in_secs": 2}));
        assert_eq!(code, 200, "{created}");
        let bob_key = created["api_key"].as_str().unwrap().to_string();
        assert_eq!(status(port, "GET", operators, &bob_key, None), 200);
        eventually("bob's credential to expire", || {
            status(port, "GET", operators, &bob_key, None) == 401
        });
        // An expired credential no longer holds the name.
        let (code, created) = create(&alice_new, json!({"name": bob}));
        assert_eq!(code, 200, "{created}");
        let bob_key = created["api_key"].as_str().unwrap().to_string();

        // Revoked through the API (by name: every credential of that
        // operator): refused by this pod at once.
        let (code, answer) = call(
            port,
            "POST",
            "/v1/admin/operators/revoke",
            &alice_new,
            Some(json!({"name": bob})),
        );
        assert_eq!(code, 200, "{answer}");
        // Both of bob's: the usable one and the expired one.
        assert_eq!(answer["revoked"].as_array().unwrap().len(), 2, "{answer}");
        assert_eq!(status(port, "GET", operators, &bob_key, None), 401);

        // Revoked elsewhere (another pod): refused within the refresh
        // interval.
        let carol = format!("carol-{run}");
        let (code, created) = create(&alice_new, json!({"name": carol}));
        assert_eq!(code, 200, "{created}");
        let carol_key = created["api_key"].as_str().unwrap().to_string();
        assert_eq!(status(port, "GET", operators, &carol_key, None), 200);
        let registry = kosha_control::PgStore::new(&database_url).unwrap();
        let revoked = registry
            .revoke_operator_credentials(
                kosha_control::OperatorCredentials::Operator(&carol),
                "operator",
            )
            .unwrap();
        assert_eq!(revoked.len(), 1);
        eventually("a revocation made elsewhere to arrive", || {
            status(port, "GET", operators, &carol_key, None) == 401
        });

        // With the bootstrap key switched off, the shared key is just an
        // unknown key and alice's credential (loaded from its hash) still
        // administers. Switching it back on is the break-glass path.
        drop(server);
        let server = start_without_bootstrap_key(&database_url);
        let port = server.1;
        assert_eq!(status(port, "GET", operators, ADMIN, None), 401);
        assert_eq!(status(port, "GET", "/v1/stats", ADMIN, None), 401);
        assert_eq!(status(port, "GET", operators, &alice_new, None), 200);
        assert_eq!(status(port, "GET", operators, &carol_key, None), 401);
        let dave = format!("dave-{run}");
        let (code, created) = call(
            port,
            "POST",
            operators,
            &alice_new,
            Some(json!({"name": dave})),
        );
        assert_eq!(code, 200, "{created}");
        // Clean up after this run: alice revokes dave's and then her own.
        for name in [&dave, &alice] {
            let by_name = Some(json!({"name": name}));
            let revoke = "/v1/admin/operators/revoke";
            assert_eq!(status(port, "POST", revoke, &alice_new, by_name), 200);
        }
        assert_eq!(status(port, "GET", operators, &alice_new, None), 401);
        drop(server);
        let server = start(&database_url);
        assert_eq!(status(server.1, "GET", operators, ADMIN, None), 200);
    }
}
