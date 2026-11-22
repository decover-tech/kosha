//! TLS to the Postgres control plane (#343), with no database needed.
//!
//! sqlx only honors `sslmode=require|verify-ca|verify-full` when it is built
//! with a TLS feature; without one it fails before touching the network with
//! "SQLx was built without TLS support enabled", and the default `prefer`
//! silently stays plaintext. These tests stand in for the server with a local
//! listener and check that the client actually opens with the Postgres
//! SSLRequest packet and then refuses to carry on unencrypted.
#![cfg(feature = "postgres")]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

use kosha_control::PgStore;

/// Int32(8) + Int32(80877103): the Postgres SSLRequest message.
const SSL_REQUEST: [u8; 8] = [0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f];

/// Accept one connection, record its first 8 bytes, answer the SSLRequest
/// with `answer`, then send `after` and hang up. Returns the listener's
/// port and a handle yielding the first packet.
fn fake_postgres(
    answer: u8,
    after: &'static [u8],
) -> (u16, std::thread::JoinHandle<std::io::Result<[u8; 8]>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept()?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        let mut first = [0u8; 8];
        stream.read_exact(&mut first)?;
        stream.write_all(&[answer])?;
        stream.write_all(after)?;
        stream.flush()?;
        // Drain whatever the client sends next (a ClientHello), so the close
        // is a clean EOF on its side rather than a reset.
        let mut sink = [0u8; 4096];
        let _ = stream.read(&mut sink);
        Ok(first)
    });
    (port, handle)
}

fn connect_error(port: u16, sslmode: &str) -> String {
    // The password must never show up in an error either.
    let url = format!("postgres://kosha:s3cr3t-pw@127.0.0.1:{port}/kosha?sslmode={sslmode}");
    let error = match PgStore::new(&url) {
        Ok(_) => panic!("connected to a listener that is not Postgres"),
        Err(error) => error,
    };
    assert!(!error.contains("s3cr3t-pw"), "password leaked: {error}");
    error
}

#[test]
fn sslmode_require_refuses_a_server_without_tls() {
    let (port, server) = fake_postgres(b'N', b"");
    let error = connect_error(port, "require");
    assert!(
        !error.contains("built without TLS support"),
        "sqlx has no TLS feature: {error}"
    );
    assert!(error.contains("does not support TLS"), "{error}");
    assert_eq!(server.join().unwrap().unwrap(), SSL_REQUEST);
}

#[test]
fn sslmode_verify_full_fails_the_handshake_with_a_server_that_is_not_tls() {
    // The server agrees to TLS ('S') and then answers the ClientHello with
    // bytes that are not a TLS record.
    let (port, server) = fake_postgres(b'S', b"this is not a TLS ServerHello\n");
    let error = connect_error(port, "verify-full");
    assert!(
        !error.contains("built without TLS support"),
        "sqlx has no TLS feature: {error}"
    );
    assert!(
        error.starts_with("failed to connect to postgres"),
        "{error}"
    );
    assert_eq!(server.join().unwrap().unwrap(), SSL_REQUEST);
}
