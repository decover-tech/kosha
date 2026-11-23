//! HTTP/1.1 server (#259): hyper on a dedicated tokio runtime.
//!
//! This replaces the old accept loop that spawned one OS thread per
//! connection and closed every connection after one response. What it adds:
//!
//! - **Keep-alive.** A client reuses one connection for many requests.
//! - **Bounded concurrency.** At most `KOSHA_HTTP_MAX_CONNECTIONS` open
//!   connections are served normally. A connection over that cap gets one
//!   `503` with `Retry-After` and is closed. At most `KOSHA_HTTP_MAX_INFLIGHT`
//!   requests run a handler at once; a request over that cap gets the same
//!   `503`. No path spawns an OS thread per connection or per request.
//! - **Timeouts.** A connection that has not delivered a complete request
//!   head within `KOSHA_HTTP_IDLE_TIMEOUT_SECS` of the server starting to
//!   wait for one is closed: that covers both a slow-header client and a
//!   kept-alive connection left idle. A request body must arrive within
//!   `KOSHA_HTTP_IO_TIMEOUT_SECS`, and no single response write may stall
//!   longer than that (a client that stops reading). None of these bound
//!   how long a handler may take to produce a response.
//! - **Streamed bodies.** A [`Response`] body is either bytes in memory or a
//!   file streamed in [`FILE_CHUNK_BYTES`] chunks, so serving a segment file
//!   to a peer uses O(chunk) memory whatever the file size.
//! - **Optional TLS.** With `KOSHA_TLS_CERT_PATH` and `KOSHA_TLS_KEY_PATH`
//!   set, the listener serves HTTPS (rustls).
//! - **Optional mTLS on `/internal/*` (#343).** With `KOSHA_TLS_CLIENT_CA_PATH`
//!   also set, a client certificate is verified against that CA during the
//!   handshake (a bad one fails it), and a request for `/internal/*` on a
//!   connection that presented none gets `403`. Other routes keep working
//!   for clients without a certificate.
//! - **Graceful shutdown.** When the shutdown future resolves (SIGTERM or
//!   SIGINT in production, see [`os_shutdown_signal`]), the listener stops
//!   accepting, idle connections close, and in-flight requests get up to
//!   `KOSHA_HTTP_SHUTDOWN_GRACE_SECS` to finish.
//!
//! Handlers stay synchronous: each request runs its [`Handler`] on tokio's
//! blocking pool (capped at the in-flight limit), so the request-handling
//! code in `main.rs` and its rayon scoring pool are unchanged.

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use http_body_util::{BodyExt, LengthLimitError, Limited};
use hyper::body::{Bytes, Frame, Incoming, SizeHint};
use hyper::header::{HeaderName, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{watch, Semaphore};
use tokio_rustls::rustls;

/// Chunk size for streamed file bodies: the most memory one file response
/// holds at a time.
pub(crate) const FILE_CHUNK_BYTES: usize = 256 * 1024;

/// How long the pod's own HTTP clients (ingest forwarding, peer hydration,
/// the unindexed-tail fetch) keep an idle pooled connection. Well under the
/// server's default idle timeout (30 s), so a client does not reuse a
/// connection just as the other pod closes it.
pub(crate) const INTERNAL_CLIENT_POOL_IDLE: Duration = Duration::from_secs(10);

// ─── Request / response ─────────────────────────────────────────────────────

/// The pseudo-header carrying the TCP peer's IP address to the handler.
/// Set by the server on every request; a client cannot supply it.
pub(crate) const PEER_IP_HEADER: &str = "x-kosha-peer-ip";

/// One parsed request, handed to the synchronous [`Handler`].
#[derive(Debug, Clone)]
pub(crate) struct Request {
    pub method: String,
    /// Path plus query string, e.g. `/v1/namespaces/docs/search?x=1`.
    pub target: String,
    /// Lowercased header names. A repeated header keeps its last value.
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl Request {
    /// The request line the routing code in `main.rs` matches on:
    /// `"METHOD /target HTTP/1.1\r\n"`, exactly as the old hand-rolled
    /// parser read it off the socket.
    pub fn request_line(&self) -> String {
        format!("{} {} HTTP/1.1\r\n", self.method, self.target)
    }
}

/// A response: status, headers, and a body that is either bytes or a file
/// to stream. `content-length` is derived from the body.
#[derive(Debug)]
pub(crate) struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: ResponseBody,
}

#[derive(Debug)]
enum ResponseBody {
    Bytes(Bytes),
    // Only `/internal/segment` (s3 + postgres builds) streams a file.
    #[cfg_attr(not(all(feature = "s3", feature = "postgres")), allow(dead_code))]
    File {
        file: std::fs::File,
        len: u64,
    },
}

/// Headers the server computes itself; a handler's value is dropped.
fn is_hop_header(name: &str) -> bool {
    matches!(
        name,
        "content-length" | "connection" | "transfer-encoding" | "keep-alive"
    )
}

impl Response {
    /// A JSON body.
    pub fn json(status: u16, body: impl Into<Bytes>) -> Self {
        Self {
            status,
            headers: vec![("content-type".into(), "application/json".into())],
            body: ResponseBody::Bytes(body.into()),
        }
    }

    /// `{"error": message}` with the given status.
    pub fn json_error(status: u16, message: &str) -> Self {
        Self::json(status, serde_json::json!({ "error": message }).to_string())
    }

    /// Stream `len` bytes of `file`, from its current position.
    #[cfg_attr(not(all(feature = "s3", feature = "postgres")), allow(dead_code))]
    pub fn file(file: std::fs::File, len: u64, content_type: &str) -> Self {
        Self {
            status: 200,
            headers: vec![("content-type".into(), content_type.into())],
            body: ResponseBody::File { file, len },
        }
    }

    /// Convert a complete raw HTTP/1.1 response (status line, headers,
    /// blank line, body), as the `String`-producing helpers in `main.rs`
    /// build it. `content-length` and `connection` are dropped: the server
    /// sets framing itself and keeps the connection alive.
    pub fn from_raw(raw: String) -> Self {
        let Some(head_end) = raw.find("\r\n\r\n") else {
            return Self::json_error(500, "malformed response");
        };
        let mut lines = raw[..head_end].split("\r\n");
        let status = lines
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .unwrap_or(500);
        let headers = lines
            .filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                let name = name.trim().to_ascii_lowercase();
                (!is_hop_header(&name)).then(|| (name, value.trim().to_string()))
            })
            .collect();
        let body = Bytes::from(raw.into_bytes()).slice(head_end + 4..);
        Self {
            status,
            headers,
            body: ResponseBody::Bytes(body),
        }
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        let name = name.to_ascii_lowercase();
        self.headers.retain(|(k, _)| *k != name);
        self.headers.push((name, value.to_string()));
        self
    }

    pub fn status(&self) -> u16 {
        self.status
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// ` error="…" reason=… limit=… retry_after_ms=… plan=…` from a JSON
    /// error body (`{"error": …}`, the 429 and 402 fields), empty when the body is
    /// not one. For the request log line of a failed request only: it
    /// parses the body.
    pub fn error_fields(&self) -> String {
        let ResponseBody::Bytes(bytes) = &self.body else {
            return String::new();
        };
        let Ok(serde_json::Value::Object(body)) = serde_json::from_slice(bytes) else {
            return String::new();
        };
        let mut out = String::new();
        for field in ["error", "reason", "limit", "retry_after_ms", "plan"] {
            match body.get(field) {
                Some(serde_json::Value::String(s)) if field == "error" => {
                    out.push_str(&format!(" error={s:?}"))
                }
                Some(serde_json::Value::String(s)) => out.push_str(&format!(" {field}={s}")),
                Some(v @ serde_json::Value::Number(_)) => out.push_str(&format!(" {field}={v}")),
                _ => {}
            }
        }
        out
    }

    pub fn body_len(&self) -> u64 {
        match &self.body {
            ResponseBody::Bytes(bytes) => bytes.len() as u64,
            ResponseBody::File { len, .. } => *len,
        }
    }

    /// The whole body, reading a file body into memory. Tests only.
    #[cfg(test)]
    pub fn body_bytes(&mut self) -> Vec<u8> {
        use std::io::Read;
        match &mut self.body {
            ResponseBody::Bytes(bytes) => bytes.to_vec(),
            ResponseBody::File { file, len } => {
                let mut out = Vec::new();
                file.take(*len).read_to_end(&mut out).unwrap();
                out
            }
        }
    }

    /// The response as the server would write it (minus `date`), as one
    /// string. Tests only.
    #[cfg(test)]
    pub fn into_raw_string(mut self) -> String {
        let body = self.body_bytes();
        let reason = hyper::StatusCode::from_u16(self.status)
            .ok()
            .and_then(|s| s.canonical_reason())
            .unwrap_or("Response");
        let mut out = format!("HTTP/1.1 {} {reason}\r\n", self.status);
        for (name, value) in &self.headers {
            out.push_str(&format!("{name}: {value}\r\n"));
        }
        out.push_str(&format!("content-length: {}\r\n\r\n", body.len()));
        out.push_str(&String::from_utf8_lossy(&body));
        out
    }

    fn into_hyper(self) -> hyper::Response<Body> {
        let body = match self.body {
            ResponseBody::Bytes(bytes) => Body::Full(Some(bytes)),
            ResponseBody::File { file, len } => Body::File(FileBody {
                file: tokio::fs::File::from_std(file),
                remaining: len,
                buf: vec![0; FILE_CHUNK_BYTES].into_boxed_slice(),
            }),
        };
        let mut response = hyper::Response::new(body);
        *response.status_mut() = hyper::StatusCode::from_u16(self.status)
            .unwrap_or(hyper::StatusCode::INTERNAL_SERVER_ERROR);
        let headers = response.headers_mut();
        for (name, value) in self.headers {
            if is_hop_header(&name) {
                continue;
            }
            if let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(&value),
            ) {
                headers.append(name, value);
            }
        }
        response
    }
}

/// Response body as hyper sees it.
pub(crate) enum Body {
    Full(Option<Bytes>),
    File(FileBody),
}

/// A file streamed in [`FILE_CHUNK_BYTES`] chunks.
pub(crate) struct FileBody {
    file: tokio::fs::File,
    remaining: u64,
    buf: Box<[u8]>,
}

impl hyper::body::Body for Body {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        match self.get_mut() {
            Body::Full(bytes) => Poll::Ready(
                bytes
                    .take()
                    .filter(|b| !b.is_empty())
                    .map(|b| Ok(Frame::data(b))),
            ),
            Body::File(body) => {
                if body.remaining == 0 {
                    return Poll::Ready(None);
                }
                let want = body.buf.len().min(body.remaining as usize);
                let mut read = ReadBuf::new(&mut body.buf[..want]);
                ready!(Pin::new(&mut body.file).poll_read(cx, &mut read))?;
                let n = read.filled().len();
                if n == 0 {
                    return Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("file ended {} bytes early", body.remaining),
                    ))));
                }
                body.remaining -= n as u64;
                Poll::Ready(Some(Ok(Frame::data(Bytes::copy_from_slice(read.filled())))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Body::Full(bytes) => bytes.as_ref().is_none_or(Bytes::is_empty),
            Body::File(body) => body.remaining == 0,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Body::Full(bytes) => SizeHint::with_exact(bytes.as_ref().map_or(0, |b| b.len() as u64)),
            Body::File(body) => SizeHint::with_exact(body.remaining),
        }
    }
}

/// The synchronous request handler. Runs on tokio's blocking pool.
pub(crate) type Handler = Arc<dyn Fn(Request) -> Response + Send + Sync + 'static>;

// ─── Configuration ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct Config {
    /// Connections served normally at once (`KOSHA_HTTP_MAX_CONNECTIONS`).
    pub max_connections: usize,
    /// Requests running a handler at once (`KOSHA_HTTP_MAX_INFLIGHT`).
    pub max_inflight: usize,
    /// Time to receive a request's headers, counted from when the server
    /// starts waiting for them. On a kept-alive connection that includes
    /// the idle time since the previous response, so this is also the
    /// idle timeout (`KOSHA_HTTP_IDLE_TIMEOUT_SECS`).
    pub header_timeout: Duration,
    /// Time to receive a request body, and the longest a single response
    /// write may stall (`KOSHA_HTTP_IO_TIMEOUT_SECS`).
    pub io_timeout: Duration,
    /// Drain deadline on shutdown (`KOSHA_HTTP_SHUTDOWN_GRACE_SECS`).
    pub shutdown_grace: Duration,
    /// Largest accepted request body (`KOSHA_MAX_BODY_BYTES`).
    pub max_body_bytes: usize,
    /// Certificate chain and private key, PEM (`KOSHA_TLS_CERT_PATH`,
    /// `KOSHA_TLS_KEY_PATH`).
    pub tls: Option<(PathBuf, PathBuf)>,
    /// CA bundle client certificates are verified against
    /// (`KOSHA_TLS_CLIENT_CA_PATH`). Requires `tls`. Set, `/internal/*`
    /// needs a verified client certificate.
    pub client_ca: Option<PathBuf>,
    /// Async worker threads; they only do socket I/O.
    pub worker_threads: usize,
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok())
}

impl Config {
    /// Defaults for everything, given the request-body / write-stall
    /// timeout and the body cap.
    pub fn new(io_timeout: Duration, max_body_bytes: usize) -> Self {
        let cpus = std::thread::available_parallelism().map_or(2, |n| n.get());
        Self {
            max_connections: 1024,
            max_inflight: 256,
            header_timeout: io_timeout.min(Duration::from_secs(30)),
            io_timeout,
            shutdown_grace: Duration::from_secs(25),
            max_body_bytes,
            tls: None,
            client_ca: None,
            worker_threads: cpus.clamp(2, 4),
        }
    }

    /// [`Config::new`] overridden by the `KOSHA_HTTP_*` and `KOSHA_TLS_*`
    /// environment. Exits the process if only one of the TLS paths is set.
    pub fn from_env(io_timeout: Duration, max_body_bytes: usize) -> Self {
        let mut config = Self::new(io_timeout, max_body_bytes);
        if let Some(n) = env_u64("KOSHA_HTTP_MAX_CONNECTIONS").filter(|n| *n > 0) {
            config.max_connections = n as usize;
        }
        if let Some(n) = env_u64("KOSHA_HTTP_MAX_INFLIGHT").filter(|n| *n > 0) {
            config.max_inflight = n as usize;
        }
        if let Some(secs) = env_u64("KOSHA_HTTP_IDLE_TIMEOUT_SECS").filter(|n| *n > 0) {
            config.header_timeout = Duration::from_secs(secs);
        }
        if let Some(secs) = env_u64("KOSHA_HTTP_SHUTDOWN_GRACE_SECS") {
            config.shutdown_grace = Duration::from_secs(secs);
        }
        let cert = std::env::var("KOSHA_TLS_CERT_PATH")
            .ok()
            .filter(|v| !v.is_empty());
        let key = std::env::var("KOSHA_TLS_KEY_PATH")
            .ok()
            .filter(|v| !v.is_empty());
        config.tls = match (cert, key) {
            (Some(cert), Some(key)) => Some((cert.into(), key.into())),
            (None, None) => None,
            _ => {
                eprintln!("FATAL: set both KOSHA_TLS_CERT_PATH and KOSHA_TLS_KEY_PATH, or neither");
                std::process::exit(1);
            }
        };
        config.client_ca = std::env::var("KOSHA_TLS_CLIENT_CA_PATH")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(PathBuf::from);
        if config.client_ca.is_some() && config.tls.is_none() {
            eprintln!(
                "FATAL: KOSHA_TLS_CLIENT_CA_PATH needs KOSHA_TLS_CERT_PATH and KOSHA_TLS_KEY_PATH \
                 (client certificates are only checked over TLS)"
            );
            std::process::exit(1);
        }
        config
    }
}

/// Build the rustls acceptor from PEM files. With `client_ca`, a client
/// certificate is optional but, when presented, must chain to that CA.
fn tls_acceptor(
    cert_path: &Path,
    key_path: &Path,
    client_ca: Option<&Path>,
) -> io::Result<tokio_rustls::TlsAcceptor> {
    use crate::internal_tls::{load_certs, load_key, load_roots};

    let invalid = |what: String| io::Error::new(io::ErrorKind::InvalidInput, what);
    let certs = load_certs(cert_path).map_err(invalid)?;
    let key = load_key(key_path).map_err(invalid)?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| invalid(format!("TLS protocol versions: {e}")))?;
    let builder = match client_ca {
        Some(ca) => {
            let roots = load_roots(ca).map_err(invalid)?;
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            // Public routes stay open to clients without a
            // certificate; `/internal/*` checks for one per request.
            .allow_unauthenticated()
            .build()
            .map_err(|e| invalid(format!("KOSHA_TLS_CLIENT_CA_PATH: {e}")))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let mut config = builder
        .with_single_cert(certs, key)
        .map_err(|e| invalid(format!("TLS certificate/key: {e}")))?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

// ─── Server ─────────────────────────────────────────────────────────────────

/// Resolves on SIGTERM or SIGINT. Must be polled inside the runtime, which
/// [`run`] does.
pub(crate) async fn os_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = term.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            }
            Err(e) => {
                eprintln!(
                    "WARN: cannot listen for SIGTERM ({e}); only SIGINT shuts down gracefully"
                );
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

struct Shared {
    config: Config,
    handler: Handler,
    inflight: Arc<Semaphore>,
    tls: Option<tokio_rustls::TlsAcceptor>,
    /// `/internal/*` needs a verified client certificate
    /// (`KOSHA_TLS_CLIENT_CA_PATH`).
    client_auth: bool,
}

/// Serve `listener` until `shutdown` resolves, then drain. Blocks the
/// calling thread; builds and owns its runtime.
pub(crate) fn run(
    listener: std::net::TcpListener,
    config: Config,
    handler: Handler,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> io::Result<()> {
    let tls = match &config.tls {
        Some((cert, key)) => Some(tls_acceptor(cert, key, config.client_ca.as_deref())?),
        None => None,
    };
    let client_auth = tls.is_some() && config.client_ca.is_some();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.worker_threads)
        // Handlers take at most `max_inflight` of these; the rest serve
        // tokio's own file I/O (streamed file bodies).
        .max_blocking_threads(config.max_inflight + 16)
        .thread_name("kosha-http")
        .enable_all()
        .build()?;
    let shared = Arc::new(Shared {
        inflight: Arc::new(Semaphore::new(config.max_inflight)),
        config,
        handler,
        tls,
        client_auth,
    });
    let result = runtime.block_on(accept_loop(listener, shared, shutdown));
    // Handlers still running past the grace period are abandoned, not
    // waited for: the process is on its way out.
    runtime.shutdown_background();
    result
}

async fn accept_loop(
    listener: std::net::TcpListener,
    shared: Arc<Shared>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> io::Result<()> {
    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let max_connections = shared
        .config
        .max_connections
        .min(Semaphore::MAX_PERMITS)
        .min(u32::MAX as usize);
    let connections = Arc::new(Semaphore::new(max_connections));
    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((tcp, peer)) => {
                    tcp.set_nodelay(true).ok();
                    let shared = Arc::clone(&shared);
                    match Arc::clone(&connections).try_acquire_owned() {
                        Ok(permit) => {
                            let stop = stop_rx.clone();
                            tokio::spawn(async move {
                                accept_connection(tcp, peer.ip(), shared, Some(stop)).await;
                                drop(permit);
                            });
                        }
                        // Over the connection cap: one 503, then close.
                        // A task, not a thread, and short-lived.
                        Err(_) => {
                            tokio::spawn(accept_connection(tcp, peer.ip(), shared, None));
                        }
                    }
                }
                Err(e) => {
                    // Typically EMFILE under a connection flood; back off
                    // instead of spinning on the error.
                    eprintln!("accept error: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
        }
    }

    drop(listener);
    let grace = shared.config.shutdown_grace;
    println!(
        "http: shutting down; draining connections for up to {}s",
        grace.as_secs()
    );
    let _ = stop_tx.send(true);
    match tokio::time::timeout(grace, connections.acquire_many(max_connections as u32)).await {
        Ok(_) => println!("http: drained"),
        Err(_) => eprintln!(
            "WARN: http: {} connection(s) still open after the {}s shutdown grace period",
            max_connections - connections.available_permits(),
            grace.as_secs()
        ),
    }
    Ok(())
}

/// TLS handshake (if configured), then serve the connection. `stop` is
/// `None` for an over-capacity connection, which only gets a 503.
async fn accept_connection(
    tcp: tokio::net::TcpStream,
    peer: std::net::IpAddr,
    shared: Arc<Shared>,
    stop: Option<watch::Receiver<bool>>,
) {
    match shared.tls.clone() {
        Some(acceptor) => {
            // A failed or stalled handshake just drops the socket.
            let handshake =
                tokio::time::timeout(shared.config.header_timeout, acceptor.accept(tcp)).await;
            if let Ok(Ok(tls)) = handshake {
                // rustls has already verified a presented certificate
                // against KOSHA_TLS_CLIENT_CA_PATH.
                let client_cert = tls
                    .get_ref()
                    .1
                    .peer_certificates()
                    .is_some_and(|certs| !certs.is_empty());
                serve_connection(tls, shared, stop, client_cert, peer).await;
            }
        }
        None => serve_connection(tcp, shared, stop, false, peer).await,
    }
}

async fn serve_connection<I>(
    io: I,
    shared: Arc<Shared>,
    stop: Option<watch::Receiver<bool>>,
    client_cert: bool,
    peer: std::net::IpAddr,
) where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let over_capacity = stop.is_none();
    let service = {
        let shared = Arc::clone(&shared);
        service_fn(move |request| {
            let shared = Arc::clone(&shared);
            async move {
                let response = if over_capacity {
                    busy("server at connection capacity (KOSHA_HTTP_MAX_CONNECTIONS)")
                        .with_header("connection", "close")
                } else {
                    handle_request(request, &shared, client_cert, peer).await
                };
                Ok::<_, Infallible>(response.into_hyper())
            }
        })
    };
    let mut builder = http1::Builder::new();
    // hyper starts this timer whenever it waits for a request head,
    // including between requests on a kept-alive connection, so it is
    // both the slow-header (slowloris) bound and the keep-alive idle
    // timeout.
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(shared.config.header_timeout)
        .keep_alive(!over_capacity);
    let io = TokioIo::new(WriteTimeout::new(io, shared.config.io_timeout));
    let connection = builder.serve_connection(io, service);
    tokio::pin!(connection);

    let Some(mut stop) = stop else {
        let _ = connection.await;
        return;
    };
    tokio::select! {
        _ = connection.as_mut() => return,
        _ = stop.wait_for(|stopping| *stopping) => {}
    }
    // Shutting down: finish a response in progress, then close; an idle
    // connection closes now.
    connection.as_mut().graceful_shutdown();
    let _ = connection.await;
}

/// `503` with `Retry-After: 1`.
fn busy(message: &str) -> Response {
    Response::json_error(503, message).with_header("retry-after", "1")
}

async fn handle_request(
    request: hyper::Request<Incoming>,
    shared: &Shared,
    client_cert: bool,
    peer: std::net::IpAddr,
) -> Response {
    let config = &shared.config;
    let path = request.uri().path();
    // Liveness answers on the async side, without an in-flight slot, so a
    // pod whose handlers are all busy still reports alive.
    if request.method() == hyper::Method::GET && (path == "/healthz" || path == "/v1/healthz") {
        return Response::json(200, r#"{"status":"ok"}"#);
    }
    if shared.client_auth && !client_cert && path.starts_with("/internal/") {
        return Response::json_error(
            403,
            "/internal/* requires a client certificate (KOSHA_TLS_CLIENT_CA_PATH)",
        );
    }

    // Reject an oversized body from its declared length, before reading
    // (or allocating) any of it.
    let declared = request
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if let Some(len) = declared.filter(|len| *len > config.max_body_bytes as u64) {
        return too_large(len, config.max_body_bytes);
    }

    let Ok(permit) = Arc::clone(&shared.inflight).try_acquire_owned() else {
        return busy("server at request capacity (KOSHA_HTTP_MAX_INFLIGHT)");
    };

    let (parts, body) = request.into_parts();
    let body = match tokio::time::timeout(
        config.io_timeout,
        Limited::new(body, config.max_body_bytes).collect(),
    )
    .await
    {
        Ok(Ok(collected)) => collected.to_bytes(),
        Ok(Err(e)) if e.downcast_ref::<LengthLimitError>().is_some() => {
            return too_large(declared.unwrap_or(0), config.max_body_bytes);
        }
        Ok(Err(e)) => return Response::json_error(400, &format!("reading request body: {e}")),
        Err(_) => {
            return Response::json_error(
                408,
                "request body not received in time (KOSHA_HTTP_IO_TIMEOUT_SECS)",
            )
        }
    };

    let mut headers = HashMap::with_capacity(parts.headers.len());
    for (name, value) in &parts.headers {
        headers.insert(
            name.as_str().to_string(),
            String::from_utf8_lossy(value.as_bytes()).trim().to_string(),
        );
    }
    // The connection's peer address, for the console's per-IP attempt
    // limits (#345). Always the server's own value: one a client sent
    // under this name is replaced.
    headers.insert(PEER_IP_HEADER.to_string(), peer.to_string());
    let request = Request {
        method: parts.method.as_str().to_string(),
        target: parts.uri.path_and_query().map_or_else(
            || parts.uri.path().to_string(),
            |pq| pq.as_str().to_string(),
        ),
        headers,
        body: body.to_vec(),
    };
    let handler = Arc::clone(&shared.handler);
    let job = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        handler(request)
    });
    match job.await {
        Ok(response) => response,
        Err(e) => {
            eprintln!("ERROR: request handler failed: {e}");
            Response::json_error(500, "internal error")
        }
    }
}

fn too_large(len: u64, max: usize) -> Response {
    let message = if len > 0 {
        format!("request body of {len} bytes exceeds the {max}-byte limit (KOSHA_MAX_BODY_BYTES)")
    } else {
        format!("request body exceeds the {max}-byte limit (KOSHA_MAX_BODY_BYTES)")
    };
    eprintln!("WARN: request failed: status=413 {message}");
    Response::json_error(413, &message)
}

// ─── Write-stall timeout ────────────────────────────────────────────────────

/// Fails a write that makes no progress for `timeout`: a client that stops
/// reading its response cannot hold the connection forever.
struct WriteTimeout<I> {
    inner: I,
    timeout: Duration,
    stalled: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<I> WriteTimeout<I> {
    fn new(inner: I, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            stalled: None,
        }
    }

    /// Called when a write returned `Pending`: `Ready` once it has been
    /// pending for longer than the timeout.
    fn poll_stall(&mut self, cx: &mut Context<'_>) -> Poll<io::Error> {
        let timeout = self.timeout;
        let sleep = self
            .stalled
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
        ready!(sleep.as_mut().poll(cx));
        Poll::Ready(io::Error::new(
            io::ErrorKind::TimedOut,
            "client stopped reading the response (KOSHA_HTTP_IO_TIMEOUT_SECS)",
        ))
    }

    fn progressed<T>(
        &mut self,
        poll: Poll<io::Result<T>>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<T>> {
        match poll {
            Poll::Ready(result) => {
                self.stalled = None;
                Poll::Ready(result)
            }
            Poll::Pending => self.poll_stall(cx).map(Err),
        }
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for WriteTimeout<I> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for WriteTimeout<I> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.progressed(poll, cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        this.progressed(poll, cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_flush(cx);
        this.progressed(poll, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

// ─── Test support ───────────────────────────────────────────────────────────

/// Read one HTTP/1.1 response off a blocking stream, framed by its
/// `content-length` (the server keeps the connection open afterwards).
/// Returns the head (status line and headers, `\r\n`-separated) and body.
#[cfg(test)]
pub(crate) fn read_response(stream: &mut impl std::io::Read) -> io::Result<(String, Vec<u8>)> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte)? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "connection closed mid-head: {:?}",
                    String::from_utf8_lossy(&head)
                ),
            ));
        }
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head[..head.len() - 4]).into_owned();
    let len = head
        .split("\r\n")
        .skip(1)
        .find_map(|line| {
            let (k, v) = line.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body)?;
    Ok((head, body))
}

/// [`read_response`] reassembled into one string, the shape the older
/// socket tests in `main.rs` parse.
#[cfg(test)]
pub(crate) fn read_response_string(stream: &mut impl std::io::Read) -> io::Result<String> {
    let (head, body) = read_response(stream)?;
    Ok(format!("{head}\r\n\r\n{}", String::from_utf8_lossy(&body)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    fn test_config() -> Config {
        let mut config = Config::new(Duration::from_secs(10), 1 << 20);
        config.shutdown_grace = Duration::from_secs(5);
        config
    }

    struct TestServer {
        addr: SocketAddr,
        stop: Option<tokio::sync::oneshot::Sender<()>>,
        thread: Option<std::thread::JoinHandle<io::Result<()>>>,
    }

    impl TestServer {
        fn start(config: Config, handler: Handler) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let thread = std::thread::spawn(move || {
                run(listener, config, handler, async move {
                    let _ = rx.await;
                })
            });
            Self {
                addr,
                stop: Some(tx),
                thread: Some(thread),
            }
        }

        /// Trigger shutdown and wait for `run` to return.
        fn shutdown(&mut self) -> io::Result<()> {
            if let Some(stop) = self.stop.take() {
                let _ = stop.send(());
            }
            self.thread.take().unwrap().join().unwrap()
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            if let Some(stop) = self.stop.take() {
                let _ = stop.send(());
            }
        }
    }

    fn echo_handler() -> Handler {
        Arc::new(|req: Request| {
            Response::json(
                200,
                serde_json::json!({
                    "method": req.method,
                    "target": req.target,
                    "body": String::from_utf8_lossy(&req.body),
                })
                .to_string(),
            )
        })
    }

    fn connect(addr: SocketAddr) -> TcpStream {
        let s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s
    }

    fn status(head: &str) -> u16 {
        head.get(9..12).and_then(|c| c.parse().ok()).unwrap_or(0)
    }

    #[test]
    fn from_raw_drops_framing_headers_and_keeps_the_rest() {
        let mut r = Response::from_raw(
            "HTTP/1.1 503 Service Unavailable\r\ncontent-length: 2\r\ncontent-type: application/json\r\n\
             retry-after: 7\r\nconnection: close\r\n\r\n{}"
                .to_string(),
        );
        assert_eq!(r.status(), 503);
        assert_eq!(r.header("retry-after"), Some("7"));
        assert_eq!(r.header("content-type"), Some("application/json"));
        assert_eq!(r.header("connection"), None);
        assert_eq!(r.header("content-length"), None);
        assert_eq!(r.body_bytes(), b"{}");
    }

    #[test]
    fn several_requests_share_one_keep_alive_connection() {
        let _server = TestServer::start(test_config(), echo_handler());
        let mut s = connect(_server.addr);
        for i in 0..5 {
            let body = format!("payload-{i}");
            write!(
                s,
                "POST /echo/{i}?q=1 HTTP/1.1\r\nhost: x\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
            let (head, resp) = read_response(&mut s).unwrap();
            assert_eq!(status(&head), 200, "{head}");
            assert!(
                !head.to_ascii_lowercase().contains("connection: close"),
                "{head}"
            );
            let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
            assert_eq!(v["target"], format!("/echo/{i}?q=1"));
            assert_eq!(v["body"], body);
        }
    }

    #[test]
    fn chunked_request_bodies_are_decoded() {
        let _server = TestServer::start(test_config(), echo_handler());
        let mut s = connect(_server.addr);
        s.write_all(
            b"POST /x HTTP/1.1\r\nhost: x\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n",
        )
        .unwrap();
        let (head, resp) = read_response(&mut s).unwrap();
        assert_eq!(status(&head), 200, "{head}");
        let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        assert_eq!(v["body"], "abcde");
    }

    #[test]
    fn oversized_bodies_get_413_declared_or_chunked() {
        let mut config = test_config();
        config.max_body_bytes = 8;
        let _server = TestServer::start(config, echo_handler());

        let mut s = connect(_server.addr);
        s.write_all(b"POST /x HTTP/1.1\r\nhost: x\r\ncontent-length: 100\r\n\r\n")
            .unwrap();
        let (head, body) = read_response(&mut s).unwrap();
        assert_eq!(status(&head), 413, "{head}");
        assert!(String::from_utf8_lossy(&body).contains("KOSHA_MAX_BODY_BYTES"));

        let mut s = connect(_server.addr);
        s.write_all(
            b"POST /x HTTP/1.1\r\nhost: x\r\ntransfer-encoding: chunked\r\n\r\n10\r\n0123456789abcdef\r\n0\r\n\r\n",
        )
        .unwrap();
        let (head, _) = read_response(&mut s).unwrap();
        assert_eq!(status(&head), 413, "{head}");
    }

    #[test]
    fn stalled_clients_do_not_block_other_requests() {
        let _server = TestServer::start(test_config(), echo_handler());

        // One client connects and sends nothing; another sends half a
        // request head and stalls.
        let _silent = TcpStream::connect(_server.addr).unwrap();
        let mut partial = TcpStream::connect(_server.addr).unwrap();
        partial.write_all(b"GET /slow HTTP/1.1\r\nhost:").unwrap();
        std::thread::sleep(Duration::from_millis(100));

        for path in ["/healthz", "/v1/namespaces"] {
            let mut s = connect(_server.addr);
            write!(s, "GET {path} HTTP/1.1\r\nhost: x\r\n\r\n").unwrap();
            let (head, _) = read_response(&mut s).unwrap();
            assert_eq!(status(&head), 200, "{path}: {head}");
        }
    }

    #[test]
    fn header_timeout_closes_a_connection_that_never_finishes_its_head() {
        let mut config = test_config();
        config.header_timeout = Duration::from_millis(300);
        let _server = TestServer::start(config, echo_handler());
        let mut s = connect(_server.addr);
        s.write_all(b"GET /x HTTP/1.1\r\nhost:").unwrap();
        let started = Instant::now();
        let mut buf = Vec::new();
        // hyper answers 408 or just closes; either way the read ends.
        let _ = s.read_to_end(&mut buf);
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn idle_keep_alive_connections_are_closed() {
        let mut config = test_config();
        config.header_timeout = Duration::from_millis(300);
        let _server = TestServer::start(config, echo_handler());
        let mut s = connect(_server.addr);
        s.write_all(b"GET /x HTTP/1.1\r\nhost: x\r\n\r\n").unwrap();
        let (head, _) = read_response(&mut s).unwrap();
        assert_eq!(status(&head), 200);
        let started = Instant::now();
        let mut rest = Vec::new();
        assert_eq!(s.read_to_end(&mut rest).unwrap(), 0);
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    /// None of the timeouts bound handler time: a cold search that takes
    /// many times `KOSHA_HTTP_IO_TIMEOUT_SECS` and the idle timeout still
    /// delivers its response (the BM25 benchmark's bug #3).
    #[test]
    fn slow_handler_outlives_every_timeout() {
        let mut config = test_config();
        config.io_timeout = Duration::from_millis(200);
        config.header_timeout = Duration::from_millis(200);
        let handler: Handler = Arc::new(|_req: Request| {
            std::thread::sleep(Duration::from_millis(1000));
            Response::json(200, "{}")
        });
        let _server = TestServer::start(config, handler);
        let mut s = connect(_server.addr);
        s.write_all(b"GET /cold-search HTTP/1.1\r\nhost: x\r\n\r\n")
            .unwrap();
        let (head, _) = read_response(&mut s).unwrap();
        assert_eq!(status(&head), 200, "{head}");
    }

    /// Thread count of this process, where the OS exposes it cheaply.
    fn process_threads() -> Option<usize> {
        std::fs::read_dir("/proc/self/task")
            .ok()
            .map(|entries| entries.count())
    }

    #[test]
    fn connection_flood_gets_503s_not_threads() {
        // Linux CI allows plenty of descriptors; a developer's macOS shell
        // often only 256.
        let clients = if cfg!(target_os = "linux") { 1000 } else { 120 };
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let handler: Handler = {
            let (running, peak) = (Arc::clone(&running), Arc::clone(&peak));
            Arc::new(move |_req: Request| {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(300));
                running.fetch_sub(1, Ordering::SeqCst);
                Response::json(200, "{}")
            })
        };
        let mut config = test_config();
        config.max_connections = 32;
        config.max_inflight = 8;
        let _server = TestServer::start(config, handler);
        std::thread::sleep(Duration::from_millis(50));
        let before = process_threads();

        let mut streams: Vec<TcpStream> = (0..clients)
            .map(|_| {
                let mut s = connect(_server.addr);
                s.write_all(b"GET /work HTTP/1.1\r\nhost: x\r\n\r\n")
                    .unwrap();
                s
            })
            .collect();
        std::thread::sleep(Duration::from_millis(100));
        let during = process_threads();

        let (mut ok, mut busy) = (0, 0);
        for s in &mut streams {
            let (head, _) = read_response(s).unwrap();
            match status(&head) {
                200 => ok += 1,
                503 => {
                    assert!(head.contains("retry-after: 1"), "{head}");
                    busy += 1;
                }
                other => panic!("unexpected status {other}: {head}"),
            }
        }
        assert_eq!(ok + busy, clients);
        assert!(ok >= 1, "some requests must be served");
        assert!(busy >= clients - 64, "ok={ok} busy={busy}");
        assert!(peak.load(Ordering::SeqCst) <= 8, "in-flight cap exceeded");
        if let (Some(before), Some(during)) = (before, during) {
            // The old server held one thread per connection here. Other
            // tests run in parallel in this process, hence the slack.
            assert!(
                during < before + 200,
                "threads grew from {before} to {during} for {clients} connections"
            );
        }
    }

    #[test]
    fn streams_a_large_file_body_in_chunks() {
        let dir = std::env::temp_dir().join(format!(
            "kosha-http-file-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("blob.bin");
        let data: Vec<u8> = (0..(3 * FILE_CHUNK_BYTES + 123))
            .map(|i| (i * 31 % 251) as u8)
            .collect();
        std::fs::write(&path, &data).unwrap();
        let handler: Handler = {
            let path = path.clone();
            Arc::new(move |_req: Request| {
                let file = std::fs::File::open(&path).unwrap();
                let len = file.metadata().unwrap().len();
                Response::file(file, len, "application/octet-stream")
            })
        };
        let _server = TestServer::start(test_config(), handler);
        let mut s = connect(_server.addr);
        for _ in 0..2 {
            s.write_all(b"GET /blob HTTP/1.1\r\nhost: x\r\n\r\n")
                .unwrap();
            let (head, body) = read_response(&mut s).unwrap();
            assert_eq!(status(&head), 200, "{head}");
            assert!(head.contains("application/octet-stream"), "{head}");
            assert_eq!(body, data);
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn shutdown_drains_in_flight_requests_and_stops_accepting() {
        let handler: Handler = Arc::new(|_req: Request| {
            std::thread::sleep(Duration::from_millis(500));
            Response::json(200, r#"{"done":true}"#)
        });
        let mut server = TestServer::start(test_config(), handler);
        let addr = server.addr;
        let client = std::thread::spawn(move || {
            let mut s = connect(addr);
            s.write_all(b"GET /slow HTTP/1.1\r\nhost: x\r\n\r\n")
                .unwrap();
            read_response(&mut s).unwrap()
        });
        std::thread::sleep(Duration::from_millis(150));
        let started = Instant::now();
        server.shutdown().unwrap();
        assert!(started.elapsed() < Duration::from_secs(4));
        let (head, body) = client.join().unwrap();
        assert_eq!(status(&head), 200, "{head}");
        assert_eq!(body, br#"{"done":true}"#);
        assert!(TcpStream::connect(addr).is_err(), "listener must be closed");
    }

    #[test]
    fn serves_https_with_a_configured_certificate() {
        let dir = std::env::temp_dir().join(format!(
            "kosha-http-tls-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_pem = generated.cert.pem();
        std::fs::write(dir.join("cert.pem"), &cert_pem).unwrap();
        std::fs::write(dir.join("key.pem"), generated.signing_key.serialize_pem()).unwrap();

        let mut config = test_config();
        config.tls = Some((dir.join("cert.pem"), dir.join("key.pem")));
        let _server = TestServer::start(config, echo_handler());

        let mut roots = rustls::RootCertStore::empty();
        roots.add(generated.cert.der().clone()).unwrap();
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let client_config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connection =
            rustls::ClientConnection::new(Arc::new(client_config), "localhost".try_into().unwrap())
                .unwrap();
        let mut tls = rustls::StreamOwned::new(connection, connect(_server.addr));
        for path in ["/healthz", "/v1/thing"] {
            write!(tls, "GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n").unwrap();
            let (head, body) = read_response(&mut tls).unwrap();
            assert_eq!(status(&head), 200, "{head}");
            if path != "/healthz" {
                let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(v["target"], path);
            }
        }

        // Plain HTTP against the TLS port gets no HTTP response.
        let mut plain = connect(_server.addr);
        plain
            .write_all(b"GET /healthz HTTP/1.1\r\nhost: x\r\n\r\n")
            .unwrap();
        let mut out = Vec::new();
        let _ = plain.read_to_end(&mut out);
        assert!(!out.starts_with(b"HTTP/1.1 200"));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// #343: with `KOSHA_TLS_CLIENT_CA_PATH`, `/internal/*` needs a client
    /// certificate from that CA; public routes do not; a certificate from
    /// another CA fails the handshake.
    #[test]
    fn mtls_server_rejects_internal_requests_without_a_client_certificate() {
        use crate::internal_tls::{test_pki, InternalTls};

        let pki = test_pki::generate("mtls-server");
        let mut config = test_config();
        config.tls = Some((pki.server_cert.clone(), pki.server_key.clone()));
        config.client_ca = Some(pki.ca.clone());
        let server = TestServer::start(config, echo_handler());

        let client = |cert: Option<(&std::path::Path, &std::path::Path)>, ca: &std::path::Path| {
            let ca = ca.to_str().unwrap().to_string();
            let (cert, key) = cert
                .map(|(c, k)| {
                    (
                        Some(c.to_str().unwrap().to_string()),
                        Some(k.to_str().unwrap().to_string()),
                    )
                })
                .unwrap_or((None, None));
            InternalTls::from_lookup(|name| match name {
                "KOSHA_INTERNAL_TLS" => Some("on".into()),
                "KOSHA_INTERNAL_TLS_CA_PATH" => Some(ca.clone()),
                "KOSHA_INTERNAL_TLS_CLIENT_CERT_PATH" => cert.clone(),
                "KOSHA_INTERNAL_TLS_CLIENT_KEY_PATH" => key.clone(),
                _ => None,
            })
            .unwrap()
            .client_builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap()
        };
        let url = |path: &str| format!("https://localhost:{}{path}", server.addr.port());
        let status = |client: &reqwest::blocking::Client, path: &str| {
            client.get(url(path)).send().map(|r| r.status().as_u16())
        };

        let anonymous = client(None, &pki.ca);
        assert_eq!(status(&anonymous, "/internal/segment/a/b").unwrap(), 403);
        assert_eq!(status(&anonymous, "/v1/thing").unwrap(), 200);
        assert_eq!(status(&anonymous, "/healthz").unwrap(), 200);

        let peer = client(Some((&pki.client_cert, &pki.client_key)), &pki.ca);
        assert_eq!(status(&peer, "/internal/segment/a/b").unwrap(), 200);

        // A client certificate from an unrelated CA is refused outright.
        let other = test_pki::generate("mtls-other");
        let stranger = client(Some((&other.client_cert, &other.client_key)), &pki.ca);
        assert!(status(&stranger, "/internal/segment/a/b").is_err());
        // And a client that does not trust the server's CA never connects.
        let untrusting = client(None, &other.ca);
        assert!(status(&untrusting, "/v1/thing").is_err());
    }
}
