//! Process-level graceful shutdown (#259): on SIGTERM the real binary stops
//! accepting, drains, releases its writer leases and exits 0.
#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// One request on a fresh connection; the status code, or `None` while the
/// server is not up.
fn status(port: u16, raw: &str) -> Option<u16> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    s.write_all(raw.as_bytes()).ok()?;
    let mut head = [0u8; 12];
    s.read_exact(&mut head).ok()?;
    std::str::from_utf8(&head[9..12]).ok()?.parse().ok()
}

#[test]
fn sigterm_drains_and_exits_cleanly() {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("kosha-shutdown-{}-{port}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let mut child = Command::new(env!("CARGO_BIN_EXE_kosha-server"))
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("KOSHA_HTTP_PORT", port.to_string())
        .env("KOSHA_DATA_DIR", &dir)
        .env("KOSHA_AUTH", "disabled")
        .env("KOSHA_HTTP_SHUTDOWN_GRACE_SECS", "5")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn kosha-server");

    let deadline = Instant::now() + Duration::from_secs(30);
    while status(port, "GET /healthz HTTP/1.1\r\nhost: x\r\n\r\n") != Some(200) {
        assert!(Instant::now() < deadline, "server never became healthy");
        std::thread::sleep(Duration::from_millis(100));
    }
    // A write takes the namespace's writer lease, which shutdown releases.
    let body = r#"{"documents":[{"id":"a","fields":{"text":"hello"}}]}"#;
    let written = status(
        port,
        &format!(
            "POST /v1/namespaces/n/documents HTTP/1.1\r\nhost: x\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        ),
    );
    assert!(written.is_some(), "write got no response");

    let killed = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    let started = Instant::now();
    let out = loop {
        if child.try_wait().unwrap().is_some() {
            break child.wait_with_output().unwrap();
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "server did not exit after SIGTERM"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "stdout: {stdout}");
    assert!(stdout.contains("http: shutting down"), "stdout: {stdout}");
    assert!(stdout.contains("kosha-server stopped"), "stdout: {stdout}");
    assert!(
        TcpStream::connect(("127.0.0.1", port)).is_err(),
        "listener must be closed"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
