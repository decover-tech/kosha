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

/// `POST` `body` as `key`, through the real server.
fn post(port: u16, path: &str, key: &str, body: &str) -> String {
    http(
        port,
        &format!(
            "POST {path} HTTP/1.1\r\nauthorization: Bearer {key}\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .unwrap()
}

fn json_of(response: &str) -> serde_json::Value {
    serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap()
}

#[test]
fn scoped_keys_from_the_admin_api_end_to_end() {
    // #346, env-only: the admin API keeps keys in memory on this server.
    let (mut cmd, port) = server(&[("KOSHA_ADMIN_API_KEY", "sk-admin")]);
    let mut child = cmd.spawn().unwrap();
    wait_healthy(&mut child, port);

    let created = post(
        port,
        "/v1/admin/api-keys",
        "sk-admin",
        r#"{"tenant_id":"acme","scopes":["read"],"namespaces":["docs"]}"#,
    );
    let reader = json_of(&created)["api_key"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let id = json_of(&created)["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let search_body = r#"{"query_text":"x","max_results":1}"#;
    let write = post(
        port,
        "/v1/namespaces/docs/documents",
        &reader,
        r#"{"documents":[]}"#,
    );
    let search = post(port, "/v1/namespaces/docs/search", &reader, search_body);
    let other_ns = post(port, "/v1/namespaces/other/search", &reader, search_body);
    let listed = http(
        port,
        "GET /v1/admin/api-keys?tenant_id=acme HTTP/1.1\r\nauthorization: Bearer sk-admin\r\n\r\n",
    )
    .unwrap();
    let revoked = post(
        port,
        &format!("/v1/admin/api-keys/{id}/revoke"),
        "sk-admin",
        "",
    );
    let after_revoke = post(port, "/v1/namespaces/docs/search", &reader, search_body);
    child.kill().ok();
    child.wait().ok();

    assert!(created.starts_with("HTTP/1.1 200"), "{created}");
    assert!(reader.starts_with("sk-"), "{created}");
    assert!(write.starts_with("HTTP/1.1 403"), "{write}");
    assert!(write.contains("insufficient_scope"), "{write}");
    assert!(!search.starts_with("HTTP/1.1 401"), "{search}");
    assert!(!search.starts_with("HTTP/1.1 403"), "{search}");
    assert!(other_ns.starts_with("HTTP/1.1 403"), "{other_ns}");
    assert!(listed.starts_with("HTTP/1.1 200"), "{listed}");
    assert!(listed.contains(&id), "{listed}");
    assert!(!listed.contains(&reader), "{listed}");
    assert!(revoked.starts_with("HTTP/1.1 200"), "{revoked}");
    assert!(after_revoke.starts_with("HTTP/1.1 401"), "{after_revoke}");
}
