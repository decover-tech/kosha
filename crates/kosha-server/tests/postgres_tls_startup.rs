//! Process-level check of TLS to Postgres (#343): with
//! TLS required by default, the real binary refuses a `DATABASE_URL`
//! that does not force TLS before it opens any connection, and never prints
//! the URL.

use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

const PASSWORD: &str = "s3cr3t-pw";

/// A self-signed CA in a fresh directory, for `KOSHA_INTERNAL_TLS_CA_PATH`.
fn ca_file(tag: &str) -> PathBuf {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
    let dir = std::env::temp_dir().join(format!("kosha-pg-tls-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let path = dir.join("ca.pem");
    std::fs::write(&path, params.self_signed(&key).unwrap().pem()).unwrap();
    path
}

fn command(tag: &str, args: &[&str], mode: &str, database_url: &str) -> (Command, PathBuf) {
    let ca = ca_file(tag);
    let dir = ca.parent().unwrap().to_path_buf();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_kosha-server"));
    cmd.args(args)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("KOSHA_HTTP_PORT", "0")
        .env("KOSHA_DATA_DIR", dir.join("data"))
        .env("KOSHA_API_KEY", "test-key")
        .env("KOSHA_INTERNAL_TLS", mode)
        .env("KOSHA_INTERNAL_TLS_CA_PATH", &ca)
        // `required` only checks that a server certificate is configured.
        .env("KOSHA_TLS_CERT_PATH", &ca)
        .env("KOSHA_TLS_KEY_PATH", &ca)
        .env("DATABASE_URL", database_url)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    (cmd, dir)
}

/// Run to exit: every caller expects a refusal before the listener binds.
fn run(tag: &str, args: &[&str], mode: &str, database_url: &str) -> Output {
    let (mut cmd, dir) = command(tag, args, mode, database_url);
    let out = cmd.output().expect("spawn kosha-server");
    let _ = std::fs::remove_dir_all(dir);
    out
}

fn assert_refused(out: &Output) {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("KOSHA_INTERNAL_TLS=required") && stderr.contains("sslmode"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains(PASSWORD) && !stdout.contains(PASSWORD),
        "password leaked\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
fn default_refuses_a_database_url_without_sslmode() {
    // Port 1 is closed: a server that got as far as connecting would fail
    // with a connection error instead of the sslmode message.
    let url = format!("postgres://kosha:{PASSWORD}@127.0.0.1:1/kosha");
    let (mut cmd, dir) = command("unset", &[], "required", &url);
    let out = cmd.env_remove("KOSHA_INTERNAL_TLS").output().unwrap();
    let _ = std::fs::remove_dir_all(dir);
    assert_refused(&out);
    assert_refused(&run(
        "prefer",
        &[],
        "required",
        &format!("{url}?sslmode=prefer"),
    ));
}

#[cfg(feature = "postgres")]
#[test]
fn required_refuses_migrate_namespace_over_plaintext_postgres() {
    let url = format!("postgres://kosha:{PASSWORD}@127.0.0.1:1/kosha?sslmode=disable");
    let out = run(
        "migrate-namespace",
        &["migrate-namespace", "--list-unreachable"],
        "required",
        &url,
    );
    assert_refused(&out);
}

#[test]
fn on_alias_also_refuses_a_plaintext_database_url() {
    let url = format!("postgres://kosha:{PASSWORD}@127.0.0.1:1/kosha?sslmode=disable");
    assert_refused(&run("on", &[], "on", &url));
}

#[test]
fn default_refuses_to_serve_without_an_internal_ca() {
    let (mut cmd, dir) = command("missing-ca", &[], "required", "");
    let out = cmd
        .env_remove("KOSHA_INTERNAL_TLS")
        .env_remove("KOSHA_INTERNAL_TLS_CA_PATH")
        .env_remove("DATABASE_URL")
        .output()
        .unwrap();
    let _ = std::fs::remove_dir_all(dir);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("KOSHA_INTERNAL_TLS=required needs KOSHA_INTERNAL_TLS_CA_PATH"),
        "stderr: {stderr}"
    );
}
