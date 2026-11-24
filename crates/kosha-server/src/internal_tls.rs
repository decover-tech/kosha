//! TLS between pods (#343): the client side of pod-to-pod traffic.
//!
//! Two kinds of internal request leave a pod:
//!
//! - **Peer hydration.** A query pod that lost a cross-replica fetch race
//!   reads the segment file from the winner's `GET /internal/segment/...`.
//! - **Ingest traffic.** A query pod forwards every write to
//!   `KOSHA_INGEST_HOST` and reads the ingest pod's unindexed tail from it.
//!
//! Every client for that traffic is built from [`InternalTls`]:
//!
//! - `KOSHA_INTERNAL_TLS`: `required` (default; verified HTTPS to peers and
//!   the ingest host, with a server certificate of this pod's own), or
//!   `off` (plain HTTP to peers for local development).
//! - `KOSHA_INTERNAL_TLS_CA_PATH`: PEM CA bundle that peer certificates must
//!   chain to. Required unless mode is `off`. Only this bundle is trusted for
//!   internal traffic, not the platform roots.
//! - `KOSHA_INTERNAL_TLS_CLIENT_CERT_PATH` / `KOSHA_INTERNAL_TLS_CLIENT_KEY_PATH`:
//!   an optional client identity, presented when the peer asks for one
//!   (mTLS; the server side is `KOSHA_TLS_CLIENT_CA_PATH` in `http.rs`).
//! - `KOSHA_INTERNAL_TLS_SERVER_NAME`: the DNS name peer certificates are
//!   checked against instead of the host in the URL. Peers are addressed by
//!   pod IP (`POD_IP`), which a cert-manager certificate cannot list in
//!   advance; with this set, one certificate with a fixed DNS name serves
//!   every pod. Unset, the certificate must match the URL host (an IP SAN
//!   for peer fetches).
//!
//! The Postgres control plane is the other internal hop. Its TLS is set in
//! `DATABASE_URL` itself (`sslmode=`, `sslrootcert=`), not here, but
//! [`check_database_url_for`] holds it to the same mode: `required`
//! refuses a URL that does not force TLS.
//!
//! Configuration fails closed: an unknown mode, a missing or unreadable
//! file, half a client identity, or a TLS setting with the mode `off` stops
//! the pod at startup instead of silently falling back to plain HTTP.

use std::path::Path;
use std::sync::Arc;
#[cfg(not(test))]
use std::sync::OnceLock;

use tokio_rustls::rustls;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio_rustls::rustls::{DigitallySignedStruct, SignatureScheme};

pub(crate) const MODE_ENV: &str = "KOSHA_INTERNAL_TLS";
pub(crate) const CA_PATH_ENV: &str = "KOSHA_INTERNAL_TLS_CA_PATH";
pub(crate) const CLIENT_CERT_ENV: &str = "KOSHA_INTERNAL_TLS_CLIENT_CERT_PATH";
pub(crate) const CLIENT_KEY_ENV: &str = "KOSHA_INTERNAL_TLS_CLIENT_KEY_PATH";
pub(crate) const SERVER_NAME_ENV: &str = "KOSHA_INTERNAL_TLS_SERVER_NAME";
/// The pod's own server certificate (`http.rs`); `required` insists on it.
const SERVER_CERT_ENV: &str = "KOSHA_TLS_CERT_PATH";
const SERVER_KEY_ENV: &str = "KOSHA_TLS_KEY_PATH";

/// `KOSHA_INTERNAL_TLS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Off,
    Required,
}

impl Mode {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" | "false" | "0" | "no" | "none" | "disabled" => Ok(Self::Off),
            "" | "required" | "require" | "strict" | "on" | "true" | "1" | "yes" | "enabled" => {
                Ok(Self::Required)
            }
            _ => Err(format!("{MODE_ENV}={raw:?} is not one of off, required")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Required => "required",
        }
    }
}

/// The validated internal-TLS settings, with the rustls client config
/// built once at startup.
#[derive(Clone)]
pub(crate) struct InternalTls {
    mode: Mode,
    /// `Some` exactly when `mode` is not [`Mode::Off`].
    client: Option<Arc<rustls::ClientConfig>>,
    client_identity: bool,
    server_name: Option<String>,
}

impl std::fmt::Debug for InternalTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.describe())
    }
}

impl InternalTls {
    /// Plain HTTP between pods.
    pub fn off() -> Self {
        Self {
            mode: Mode::Off,
            client: None,
            client_identity: false,
            server_name: None,
        }
    }

    /// Read and validate the `KOSHA_INTERNAL_TLS*` environment.
    #[cfg_attr(test, allow(dead_code))]
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// [`InternalTls::from_env`] over any variable source (tests).
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let get = |name: &str| {
            get(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let mode = Mode::parse(&get(MODE_ENV).unwrap_or_default())?;
        let ca = get(CA_PATH_ENV);
        let cert = get(CLIENT_CERT_ENV);
        let key = get(CLIENT_KEY_ENV);
        let server_name = get(SERVER_NAME_ENV);

        if mode == Mode::Off {
            let set = [
                (CA_PATH_ENV, ca.is_some()),
                (CLIENT_CERT_ENV, cert.is_some()),
                (CLIENT_KEY_ENV, key.is_some()),
                (SERVER_NAME_ENV, server_name.is_some()),
            ];
            if let Some((name, _)) = set.iter().find(|(_, set)| *set) {
                return Err(format!(
                    "{name} is set but {MODE_ENV} is off; set {MODE_ENV}=required \
                     (or unset {name})"
                ));
            }
            return Ok(Self::off());
        }

        let Some(ca) = ca else {
            return Err(format!(
                "{MODE_ENV}={} needs {CA_PATH_ENV} (the CA bundle peers are verified against)",
                mode.as_str()
            ));
        };
        if get(SERVER_CERT_ENV).is_none() || get(SERVER_KEY_ENV).is_none() {
            return Err(format!(
                "{MODE_ENV}=required needs {SERVER_CERT_ENV}/{SERVER_KEY_ENV}: peers reach \
                 this pod's /internal/* routes over HTTPS only"
            ));
        }
        let roots = load_roots(Path::new(&ca))?;
        let identity = match (cert, key) {
            (Some(cert), Some(key)) => {
                Some((load_certs(Path::new(&cert))?, load_key(Path::new(&key))?))
            }
            (None, None) => None,
            _ => {
                return Err(format!(
                    "set both {CLIENT_CERT_ENV} and {CLIENT_KEY_ENV}, or neither"
                ))
            }
        };
        let pinned_name = match &server_name {
            Some(name) => Some(
                ServerName::try_from(name.clone())
                    .map_err(|e| format!("{SERVER_NAME_ENV}={name:?}: {e}"))?,
            ),
            None => None,
        };

        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let webpki = rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::clone(&provider),
        )
        .build()
        .map_err(|e| format!("{CA_PATH_ENV}: {e}"))?;
        let verifier: Arc<dyn ServerCertVerifier> = match pinned_name {
            Some(name) => Arc::new(PinnedNameVerifier {
                inner: webpki,
                name,
            }),
            None => webpki,
        };
        let builder = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("TLS protocol versions: {e}"))?
            .dangerous()
            .with_custom_certificate_verifier(verifier);
        let client_identity = identity.is_some();
        let mut config = match identity {
            Some((certs, key)) => builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| format!("{CLIENT_CERT_ENV}/{CLIENT_KEY_ENV}: {e}"))?,
            None => builder.with_no_client_auth(),
        };
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self {
            mode,
            client: Some(Arc::new(config)),
            client_identity,
            server_name,
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// The scheme for `GET /internal/segment/...` on a peer pod.
    #[cfg_attr(
        not(any(test, all(feature = "s3", feature = "postgres"))),
        allow(dead_code)
    )]
    pub fn peer_scheme(&self) -> &'static str {
        match self.mode {
            Mode::Off => "http",
            Mode::Required => "https",
        }
    }

    /// Validate `KOSHA_INGEST_HOST` against the mode.
    pub fn check_ingest_host(&self, host: &str) -> Result<(), String> {
        let https = host
            .trim()
            .get(..8)
            .is_some_and(|s| s.eq_ignore_ascii_case("https://"));
        match self.mode {
            _ if https => Ok(()),
            Mode::Off => Ok(()),
            Mode::Required => Err(format!(
                "KOSHA_INGEST_HOST={host} must use https:// with {MODE_ENV}=required"
            )),
        }
    }

    /// A blocking reqwest client builder for internal traffic: the shared
    /// idle-pool setting and, unless `off`, the internal CA (only), the
    /// optional client identity and the server-name pin.
    pub fn client_builder(&self) -> reqwest::blocking::ClientBuilder {
        let builder = reqwest::blocking::Client::builder()
            .pool_idle_timeout(crate::http::INTERNAL_CLIENT_POOL_IDLE);
        match &self.client {
            Some(config) => builder
                .https_only(true)
                .tls_backend_preconfigured((**config).clone()),
            None => builder,
        }
    }

    /// One line for the startup log. Never prints file contents.
    pub fn describe(&self) -> String {
        match self.mode {
            Mode::Off => "off (plain HTTP between pods)".into(),
            mode => format!(
                "{} (internal CA, client_cert={}, server_name={})",
                mode.as_str(),
                if self.client_identity { "yes" } else { "no" },
                self.server_name.as_deref().unwrap_or("<url host>")
            ),
        }
    }
}

/// The process-wide settings, read from the environment on first use.
/// Exits the process on an invalid configuration (fail closed).
#[cfg(not(test))]
pub(crate) fn global() -> &'static InternalTls {
    static GLOBAL: OnceLock<InternalTls> = OnceLock::new();
    GLOBAL.get_or_init(|| match InternalTls::from_env() {
        Ok(tls) => tls,
        Err(e) => {
            eprintln!("FATAL: internal TLS: {e}");
            std::process::exit(1);
        }
    })
}

/// Validate the Postgres `DATABASE_URL` against the mode, like
/// [`InternalTls::check_ingest_host`]: `required` refuses a URL whose
/// `sslmode` is not `require`, `verify-ca` or `verify-full`; `off` accepts anything.
///
/// `pg_ssl_mode` is the `PGSSLMODE` environment variable, which the
/// Postgres client uses when the URL names no `sslmode`. The URL holds
/// credentials, so no returned message quotes any part of it except a
/// recognized `sslmode` value.
///
/// It takes the bare mode and nothing else, so `migrate` and
/// `migrate-namespace` can run it without the internal CA and certificates a
/// serving pod loads.
fn check_database_url_for(mode: Mode, url: &str, pg_ssl_mode: Option<&str>) -> Result<(), String> {
    if mode == Mode::Off {
        return Ok(());
    }
    let (ssl_mode, source) = match database_url_ssl_mode(url) {
        Some(mode) => (Some(mode), "DATABASE_URL sslmode"),
        // Not trimmed: the client parses `PGSSLMODE` as it stands and
        // silently falls back to `prefer` when that fails, so `"require\n"`
        // (a Secret made with `echo`) or `"require "` is plaintext-capable.
        // Such a value must not pass as `require` here.
        None => (
            pg_ssl_mode
                .map(str::to_ascii_lowercase)
                .filter(|v| !v.is_empty()),
            "PGSSLMODE",
        ),
    };
    let problem = match ssl_mode.as_deref() {
        Some("require" | "verify-ca" | "verify-full") => return Ok(()),
        // The client's default is `prefer`, which falls back to
        // plaintext when the server (or anything in between) declines.
        None => "DATABASE_URL has no sslmode, so the Postgres connection may fall back to \
                 plaintext"
            .to_string(),
        Some(mode @ ("disable" | "allow" | "prefer")) => {
            format!("{source}={mode} does not force TLS to Postgres")
        }
        // Never echo an unrecognized value: a malformed URL could put
        // anything there.
        Some(_) if source == "PGSSLMODE" => {
            "PGSSLMODE is not a known sslmode (check for surrounding whitespace or a trailing \
             newline), so the Postgres client ignores it and may fall back to plaintext"
                .to_string()
        }
        Some(_) => format!("{source} is not a known sslmode"),
    };
    let fix = "set sslmode=verify-full (with sslrootcert=), verify-ca or require in DATABASE_URL";
    Err(format!("{problem}; {MODE_ENV}=required: {fix}"))
}

/// [`check_database_url_for`] for the process environment: a no-op
/// without `DATABASE_URL`, `Err` when the pod
/// (or the `migrate` / `migrate-namespace` command) must not connect. Call it
/// before the first Postgres connection.
///
/// Reads only the mode (`KOSHA_INTERNAL_TLS`): the migrate
/// Jobs share the pods' ConfigMap but do not mount the internal-TLS Secret,
/// and they open no pod-to-pod connection, so a missing CA file must not
/// stop them. A serving pod validates the rest of the settings in `global`.
pub(crate) fn check_database_url_env() -> Result<(), String> {
    check_database_url_lookup(|name| std::env::var(name).ok())
}

/// [`check_database_url_env`] over any variable source (tests).
fn check_database_url_lookup(get: impl Fn(&str) -> Option<String>) -> Result<(), String> {
    let Some(url) = get("DATABASE_URL").filter(|u| !u.is_empty()) else {
        return Ok(());
    };
    let mode = Mode::parse(&get(MODE_ENV).unwrap_or_default())?;
    check_database_url_for(mode, &url, get("PGSSLMODE").as_deref())
}

/// The `sslmode` (or `ssl-mode`) query parameter of a Postgres URL,
/// lowercased; the last one wins, as in the client's own parser.
fn database_url_ssl_mode(url: &str) -> Option<String> {
    let (_, query) = url.split_once('?')?;
    let query = query.split('#').next().unwrap_or(query);
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .filter(|(key, _)| matches!(*key, "sslmode" | "ssl-mode"))
        .map(|(_, value)| value.trim().to_ascii_lowercase())
        .next_back()
}

/// Checks the chain against the internal CA as usual, but matches the
/// certificate against a fixed name instead of the host that was dialed.
#[derive(Debug)]
struct PinnedNameVerifier {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
    name: ServerName<'static>,
}

impl ServerCertVerifier for PinnedNameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.inner
            .verify_server_cert(end_entity, intermediates, &self.name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

// ─── PEM loading (shared with http.rs) ──────────────────────────────────────

/// Every certificate in a PEM file; an empty file is an error.
pub(crate) fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let pem = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let certs = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("parse {}: {e}", path.display()))?;
    if certs.is_empty() {
        return Err(format!("{} holds no certificate", path.display()));
    }
    Ok(certs)
}

/// The private key in a PEM file (PKCS#8, PKCS#1 or SEC1).
pub(crate) fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let pem = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    PrivateKeyDer::from_pem_slice(&pem).map_err(|e| format!("parse {}: {e}", path.display()))
}

/// A root store holding every certificate of a PEM CA bundle.
pub(crate) fn load_roots(path: &Path) -> Result<rustls::RootCertStore, String> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in load_certs(path)? {
        roots
            .add(cert)
            .map_err(|e| format!("{}: not a usable CA certificate: {e}", path.display()))?;
    }
    Ok(roots)
}

/// A throwaway PKI for tests: a CA, a server certificate for `localhost`,
/// `kosha-internal` and `127.0.0.1`, and a client certificate, all as PEM
/// files in a fresh directory.
#[cfg(test)]
pub(crate) mod test_pki {
    use std::path::PathBuf;

    pub struct Pki {
        pub dir: PathBuf,
        pub ca: PathBuf,
        pub server_cert: PathBuf,
        pub server_key: PathBuf,
        pub client_cert: PathBuf,
        pub client_key: PathBuf,
    }

    impl Drop for Pki {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    pub fn generate(tag: &str) -> Pki {
        use rcgen::{
            BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
            SanType,
        };
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kosha-pki-{tag}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca_params, ca_key);

        let leaf = |names: Vec<String>, usage: ExtendedKeyUsagePurpose| {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(names).unwrap();
            params
                .subject_alt_names
                .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
            params.extended_key_usages = vec![usage];
            let cert = params.signed_by(&key, &issuer).unwrap();
            (cert.pem(), key.serialize_pem())
        };
        let (server_cert, server_key) = leaf(
            vec!["localhost".into(), "kosha-internal".into()],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let (client_cert, client_key) = leaf(
            vec!["kosha-client".into()],
            ExtendedKeyUsagePurpose::ClientAuth,
        );

        let write = |name: &str, contents: &str| {
            let path = dir.join(name);
            std::fs::write(&path, contents).unwrap();
            path
        };
        Pki {
            ca: write("ca.pem", &ca_cert.pem()),
            server_cert: write("server.pem", &server_cert),
            server_key: write("server-key.pem", &server_key),
            client_cert: write("client.pem", &client_cert),
            client_key: write("client-key.pem", &client_key),
            dir,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn parse(vars: &[(&str, &str)]) -> Result<InternalTls, String> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        InternalTls::from_lookup(|name| vars.get(name).cloned())
    }

    #[test]
    fn only_an_explicit_opt_out_keeps_plain_http() {
        assert!(parse(&[]).unwrap_err().contains(CA_PATH_ENV));
        assert!(parse(&[(MODE_ENV, " ")]).unwrap_err().contains(CA_PATH_ENV));
        let tls = parse(&[(MODE_ENV, "off")]).unwrap();
        assert_eq!(tls.mode(), Mode::Off);
        assert_eq!(tls.peer_scheme(), "http");
        assert_eq!(tls.check_ingest_host("http://ingest:8080"), Ok(()));
        assert_eq!(parse(&[(MODE_ENV, "OFF")]).unwrap().mode(), Mode::Off);
    }

    #[test]
    fn bad_configurations_fail_closed() {
        let pki = test_pki::generate("bad");
        let ca = pki.ca.to_str().unwrap();
        let cert = pki.client_cert.to_str().unwrap();
        let server = pki.server_cert.to_str().unwrap();
        let server_key = pki.server_key.to_str().unwrap();
        let cases: Vec<(Vec<(&str, &str)>, &str)> = vec![
            (vec![(MODE_ENV, "maybe")], "not one of"),
            (vec![(MODE_ENV, "on")], CA_PATH_ENV),
            (vec![(MODE_ENV, "off"), (CA_PATH_ENV, ca)], "is off"),
            (
                vec![(MODE_ENV, "off"), (SERVER_NAME_ENV, "kosha-internal")],
                "is off",
            ),
            (
                vec![
                    (MODE_ENV, "on"),
                    (CA_PATH_ENV, "/nonexistent/ca.pem"),
                    (SERVER_CERT_ENV, server),
                    (SERVER_KEY_ENV, server_key),
                ],
                "read",
            ),
            (
                vec![
                    (MODE_ENV, "on"),
                    (CA_PATH_ENV, ca),
                    (CLIENT_CERT_ENV, cert),
                    (SERVER_CERT_ENV, server),
                    (SERVER_KEY_ENV, server_key),
                ],
                "both",
            ),
            (
                vec![(MODE_ENV, "required"), (CA_PATH_ENV, ca)],
                SERVER_CERT_ENV,
            ),
            (
                vec![
                    (MODE_ENV, "on"),
                    (CA_PATH_ENV, ca),
                    (SERVER_NAME_ENV, "not a name!"),
                    (SERVER_CERT_ENV, server),
                    (SERVER_KEY_ENV, server_key),
                ],
                SERVER_NAME_ENV,
            ),
        ];
        for (vars, expected) in cases {
            let err = parse(&vars).unwrap_err();
            assert!(err.contains(expected), "{vars:?}: {err}");
        }
        // A key file that is not a key.
        let err = parse(&[
            (MODE_ENV, "on"),
            (CA_PATH_ENV, ca),
            (CLIENT_CERT_ENV, cert),
            (CLIENT_KEY_ENV, cert),
            (SERVER_CERT_ENV, server),
            (SERVER_KEY_ENV, server_key),
        ])
        .unwrap_err();
        assert!(err.contains("parse"), "{err}");
        // A complete configuration loads in required mode by default.
        let tls = parse(&[
            (CA_PATH_ENV, ca),
            (SERVER_CERT_ENV, server),
            (SERVER_KEY_ENV, server_key),
            (CLIENT_CERT_ENV, cert),
            (CLIENT_KEY_ENV, pki.client_key.to_str().unwrap()),
        ])
        .unwrap();
        assert_eq!(tls.mode(), Mode::Required);
        assert_eq!(tls.peer_scheme(), "https");
        assert!(tls.describe().contains("client_cert=yes"), "{tls:?}");
    }

    #[test]
    fn required_mode_refuses_a_plain_http_ingest_host() {
        let pki = test_pki::generate("ingest-host");
        let ca = pki.ca.to_str().unwrap();
        let server = pki.server_cert.to_str().unwrap();
        let server_key = pki.server_key.to_str().unwrap();
        let required = parse(&[
            (CA_PATH_ENV, ca),
            (SERVER_CERT_ENV, server),
            (SERVER_KEY_ENV, server_key),
        ])
        .unwrap();
        let err = required
            .check_ingest_host("http://kosha-ingest-service:8080")
            .unwrap_err();
        assert!(err.contains("https://"), "{err}");
        assert_eq!(
            required.check_ingest_host("HTTPS://kosha-ingest-service:8443"),
            Ok(())
        );

        let on = parse(&[
            (MODE_ENV, "on"),
            (CA_PATH_ENV, ca),
            (SERVER_CERT_ENV, server),
            (SERVER_KEY_ENV, server_key),
        ])
        .unwrap();
        assert!(on
            .check_ingest_host("http://kosha-ingest-service:8080")
            .is_err());
        assert_eq!(on.check_ingest_host("https://ingest"), Ok(()));
    }

    #[test]
    fn required_mode_refuses_a_database_url_that_does_not_force_tls() {
        let required = Mode::parse("").unwrap();
        let on = Mode::parse("on").unwrap();
        let off = Mode::Off;

        const BASE: &str = "postgres://kosha:s3cr3t-pw@pg.internal:5432/kosha";
        let url = |query: &str| format!("{BASE}{query}");
        let plaintext = [
            "",
            "?sslmode=prefer",
            "?sslmode=disable",
            "?sslmode=allow",
            "?application_name=kosha",
            // The last sslmode wins.
            "?sslmode=verify-full&sslmode=disable",
            "?sslmode=s3cr3t-pw",
        ];
        let encrypted = [
            "?sslmode=require",
            "?sslmode=verify-ca",
            "?sslmode=verify-full",
            "?sslmode=VERIFY-FULL&sslrootcert=/etc/kosha/pg/ca.crt",
            "?application_name=kosha&ssl-mode=verify-full",
            "?sslmode=disable&sslmode=require",
        ];
        for query in plaintext {
            let url = url(query);
            let err = check_database_url_for(required, &url, None).unwrap_err();
            assert!(err.contains("sslmode"), "{query}: {err}");
            let alias_err = check_database_url_for(on, &url, None).unwrap_err();
            assert_eq!(check_database_url_for(off, &url, None), Ok(()));
            // The URL holds credentials: nothing of it may reach a log.
            for message in [err, alias_err] {
                for secret in ["s3cr3t-pw", "kosha:", "pg.internal"] {
                    assert!(!message.contains(secret), "{query}: {message}");
                }
            }
        }
        for query in encrypted {
            let url = url(query);
            for mode in [required, on, off] {
                assert_eq!(check_database_url_for(mode, &url, None), Ok(()), "{query}");
            }
        }

        // PGSSLMODE applies only when the URL names no sslmode.
        assert_eq!(
            check_database_url_for(required, BASE, Some("verify-full")),
            Ok(())
        );
        assert!(check_database_url_for(required, BASE, Some("prefer")).is_err());
        // The client does not trim PGSSLMODE: a value it cannot parse is
        // dropped for the default `prefer`, so it must not pass as TLS.
        for raw in [
            "require\n",
            "require ",
            " verify-full",
            "verify-full\r\n",
            "",
        ] {
            let err = check_database_url_for(required, BASE, Some(raw)).unwrap_err();
            assert!(err.contains("required"), "{raw:?}: {err}");
            assert!(check_database_url_for(on, BASE, Some(raw)).is_err());
        }
        // Case is the one thing the client's parser forgives.
        assert_eq!(
            check_database_url_for(required, BASE, Some("REQUIRE")),
            Ok(())
        );
        assert!(
            check_database_url_for(required, &url("?sslmode=prefer"), Some("verify-full")).is_err()
        );
        assert_eq!(
            check_database_url_for(required, &url("?sslmode=require"), Some("disable")),
            Ok(())
        );
    }

    /// The migrate Jobs take the pods' ConfigMap without the TLS Secret:
    /// the Postgres check must not need the CA file to exist.
    #[test]
    fn database_url_check_reads_only_the_mode() {
        let env = |mode: &'static str, url: &'static str| {
            move |name: &str| match name {
                "DATABASE_URL" => Some(url.to_string()),
                MODE_ENV => Some(mode.to_string()),
                CA_PATH_ENV => Some("/nonexistent/kosha/tls/ca.crt".to_string()),
                CLIENT_CERT_ENV => Some("/nonexistent/kosha/tls/tls.crt".to_string()),
                CLIENT_KEY_ENV => Some("/nonexistent/kosha/tls/tls.key".to_string()),
                _ => None,
            }
        };
        const PLAIN: &str = "postgres://kosha:pw@pg.internal:5432/kosha";
        const TLS: &str = "postgres://kosha:pw@pg.internal:5432/kosha?sslmode=require";
        // The full settings cannot load here...
        assert!(InternalTls::from_lookup(env("on", PLAIN)).is_err());
        // ...but the check still runs, and still enforces the mode.
        assert!(check_database_url_lookup(env("on", PLAIN)).is_err());
        assert_eq!(check_database_url_lookup(env("off", PLAIN)), Ok(()));
        assert_eq!(check_database_url_lookup(env("on", TLS)), Ok(()));
        assert_eq!(check_database_url_lookup(env("required", TLS)), Ok(()));
        assert!(check_database_url_lookup(env("required", PLAIN)).is_err());
        assert!(check_database_url_lookup(env("bogus", TLS)).is_err());
        assert_eq!(check_database_url_lookup(|_| None), Ok(()));
    }
}
