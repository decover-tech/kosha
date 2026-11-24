//! Process-level check of TLS to S3 (#343): with
//! `KOSHA_INTERNAL_TLS=required`, the real binary refuses a plain-HTTP
//! custom S3 endpoint before it talks to S3, and never prints the endpoint.
#![cfg(feature = "s3")]

use std::process::{Command, Stdio};

const SECRET: &str = "s3cr3t-pw";

#[test]
fn required_refuses_a_plain_http_s3_endpoint() {
    let dir = std::env::temp_dir().join(format!("kosha-s3-tls-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    // Port 1 is closed: a server that got as far as calling S3 would fail
    // with a connection error, not the endpoint message.
    let endpoint = format!("http://kosha:{SECRET}@127.0.0.1:1");
    let out = Command::new(env!("CARGO_BIN_EXE_kosha-server"))
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("KOSHA_HTTP_PORT", "0")
        .env("KOSHA_DATA_DIR", dir.join("data"))
        .env("KOSHA_API_KEY", "test-key")
        .env("KOSHA_INTERNAL_TLS", "required")
        .env("KOSHA_S3_BUCKET", "kosha-test")
        .env("KOSHA_S3_ENDPOINT", &endpoint)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn kosha-server");
    let _ = std::fs::remove_dir_all(&dir);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("KOSHA_S3_ENDPOINT must use https://")
            && stderr.contains("KOSHA_INTERNAL_TLS=required"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains(SECRET) && !stdout.contains(SECRET),
        "endpoint leaked\nstdout: {stdout}\nstderr: {stderr}"
    );
}
