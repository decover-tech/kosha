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
    let mut out = String::new();
    s.read_to_string(&mut out).ok()?;
    Some(out)
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
