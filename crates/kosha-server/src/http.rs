//! HTTP/1.1 transport (#259): hyper 1.x on tokio.
//!
//! Replaces the hand-rolled thread-per-connection server. This module owns
//! everything about the socket — accept, keep-alive, the connection limit,
//! header and body timeouts, the body size limit, graceful shutdown — and
//! nothing about routing: each request becomes the `(request_line, headers,
//! body)` triple main.rs's [`crate::handle_request`] has always consumed,
//! runs on tokio's blocking pool (handlers are synchronous and may block on
//! disk, S3 or rayon scoring), and its HTTP/1.1 response string is parsed
//! back into a hyper response. That string stays the internal contract, so
//! every route and every route-level test is untouched; on the wire only the
//! `connection: close` header is gone (keep-alive).
//!
//! The one route that bypasses the string contract is a raw peer segment
//! transfer (`GET /internal/segment/*` with `x-kosha-accept-encoding: raw`),
//! which streams the cached file in [`SEGMENT_CHUNK_BYTES`] chunks instead of
//! base64-encoding it whole into a `String`.

use crate::AppState;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::body::Incoming;
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Request header a peer sends to ask for raw segment bytes.
#[cfg(all(feature = "s3", feature = "postgres"))]
pub(crate) const ACCEPT_ENCODING_HEADER: &str = "x-kosha-accept-encoding";
/// Response header naming the body encoding of a segment transfer: `raw` or
/// `base64` (the pre-#259 format, still served to peers that do not ask for
/// raw).
#[cfg(all(feature = "s3", feature = "postgres"))]
pub(crate) const ENCODING_HEADER: &str = "x-kosha-encoding";
/// Read/write unit for streamed segment transfers, on both ends.
#[cfg(all(feature = "s3", feature = "postgres"))]
pub(crate) const SEGMENT_CHUNK_BYTES: usize = 64 * 1024;

const DEFAULT_MAX_CONNECTIONS: usize = 1024;
const DEFAULT_HEADER_READ_TIMEOUT_SECS: u64 = 30;
/// tokio's own default for its blocking pool.
const DEFAULT_MAX_BLOCKING_THREADS: usize = 512;

/// Transport limits. Read from the environment by [`HttpConfig::from_env`].
#[derive(Clone, Debug)]
pub(crate) struct HttpConfig {
    /// Whole-body read budget for one request (`KOSHA_HTTP_IO_TIMEOUT_SECS`).
    pub io_timeout: Duration,
    /// How long a client may take to send a complete request head, and how
    /// long an idle keep-alive connection is held open waiting for the next
    /// one (`KOSHA_HTTP_HEADER_READ_TIMEOUT_SECS`). This is what makes a
    /// slowloris on headers time out.
    pub header_read_timeout: Duration,
    /// Connections served at once (`KOSHA_HTTP_MAX_CONNECTIONS`). The accept
    /// loop stops accepting at the limit, so further clients wait in the
    /// kernel backlog rather than costing a task and a file descriptor each.
    pub max_connections: usize,
    /// Upper bound on threads running request handlers
    /// (`KOSHA_HTTP_MAX_BLOCKING_THREADS`): tokio's blocking pool.
    pub max_blocking_threads: usize,
}

impl HttpConfig {
    pub(crate) fn from_env(io_timeout: Duration) -> Self {
        fn positive<T: std::str::FromStr + PartialOrd + Default>(var: &str, default: T) -> T {
            std::env::var(var)
                .ok()
                .and_then(|v| v.trim().parse::<T>().ok())
                .filter(|v| *v > T::default())
                .unwrap_or(default)
        }
        Self {
            io_timeout,
            header_read_timeout: Duration::from_secs(positive(
                "KOSHA_HTTP_HEADER_READ_TIMEOUT_SECS",
                DEFAULT_HEADER_READ_TIMEOUT_SECS,
            )),
            max_connections: positive("KOSHA_HTTP_MAX_CONNECTIONS", DEFAULT_MAX_CONNECTIONS),
            max_blocking_threads: positive(
                "KOSHA_HTTP_MAX_BLOCKING_THREADS",
                DEFAULT_MAX_BLOCKING_THREADS,
            ),
        }
    }
}

/// Live and peak counters, for tests and the shutdown log line.
#[derive(Default, Debug)]
pub(crate) struct HttpStats {
    connections: AtomicUsize,
    peak_connections: AtomicUsize,
    handlers: AtomicUsize,
    peak_handlers: AtomicUsize,
    accepted: AtomicU64,
    requests: AtomicU64,
}

impl HttpStats {
    #[cfg(test)]
    pub(crate) fn peak_connections(&self) -> usize {
        self.peak_connections.load(Ordering::SeqCst)
    }
    #[cfg(test)]
    pub(crate) fn peak_handlers(&self) -> usize {
        self.peak_handlers.load(Ordering::SeqCst)
    }
    #[cfg(test)]
    pub(crate) fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::SeqCst)
    }
    #[cfg(test)]
    pub(crate) fn requests(&self) -> u64 {
        self.requests.load(Ordering::SeqCst)
    }
    pub(crate) fn open_connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

/// Increments `live` (raising `peak`) for as long as it lives.
struct Gauge<'a> {
    live: &'a AtomicUsize,
}

impl<'a> Gauge<'a> {
    fn enter(live: &'a AtomicUsize, peak: &AtomicUsize) -> Self {
        let now = live.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(now, Ordering::SeqCst);
        Self { live }
    }
}

impl Drop for Gauge<'_> {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Resolves on SIGTERM (or Ctrl-C), whichever comes first. Lazy: the
/// signal handlers are installed when the server's runtime first polls it.
pub(crate) async fn terminate_signal() {
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
                eprintln!("WARN: cannot install SIGTERM handler ({e}); Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Serve `listener` until `shutdown` resolves, then stop accepting and drain:
/// idle keep-alive connections close at once, in-flight requests finish and
/// are answered. Blocks the calling thread on its own tokio runtime.
pub(crate) fn serve_until<F>(
    listener: TcpListener,
    state: Arc<AppState>,
    config: HttpConfig,
    stats: Arc<HttpStats>,
    shutdown: F,
) where
    F: Future<Output = ()>,
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("kosha-http")
        .max_blocking_threads(config.max_blocking_threads)
        .build()
        .expect("failed to build the HTTP server runtime");
    runtime.block_on(run(listener, state, config, stats, shutdown));
    // Every connection has drained, so nothing is waiting on a blocking
    // handler any more; don't hang exit on an idle pool thread.
    runtime.shutdown_timeout(Duration::from_secs(1));
}

struct Ctx {
    state: Arc<AppState>,
    config: HttpConfig,
    stats: Arc<HttpStats>,
}

async fn run<F>(
    listener: TcpListener,
    state: Arc<AppState>,
    config: HttpConfig,
    stats: Arc<HttpStats>,
    shutdown: F,
) where
    F: Future<Output = ()>,
{
    listener
        .set_nonblocking(true)
        .expect("failed to make the HTTP listener non-blocking");
    let listener =
        tokio::net::TcpListener::from_std(listener).expect("failed to register the HTTP listener");
    let permits = Arc::new(tokio::sync::Semaphore::new(config.max_connections));
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(config.header_read_timeout)
        .keep_alive(true)
        // Responses stay byte-for-byte what the hand-rolled server sent,
        // apart from `connection: close` (#259).
        .auto_date_header(false);
    let ctx = Arc::new(Ctx {
        state,
        config,
        stats: stats.clone(),
    });
    let shutdown = std::pin::pin!(shutdown);
    let mut shutdown = shutdown;

    loop {
        // Take a connection slot before accepting, so a server at its limit
        // leaves new connections in the kernel backlog.
        let permit = tokio::select! {
            permit = permits.clone().acquire_owned() => {
                permit.expect("the connection semaphore is never closed")
            }
            () = &mut shutdown => break,
        };
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(e) => {
                    // EMFILE and friends: back off instead of spinning.
                    eprintln!("accept error: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            () = &mut shutdown => break,
        };
        stream.set_nodelay(true).ok();
        stats.accepted.fetch_add(1, Ordering::SeqCst);
        let service_ctx = ctx.clone();
        let service = hyper::service::service_fn(move |req| {
            let ctx = service_ctx.clone();
            async move { Ok::<_, Infallible>(dispatch(ctx, req).await) }
        });
        let conn = graceful.watch(builder.serve_connection(TokioIo::new(stream), service));
        let stats = stats.clone();
        tokio::spawn(async move {
            let _gauge = Gauge::enter(&stats.connections, &stats.peak_connections);
            // Errors here are client resets and header-read timeouts (which
            // is also how an idle keep-alive connection ends): routine.
            let _ = conn.await;
            drop(permit);
        });
    }

    drop(listener);
    let open = stats.open_connections();
    if open > 0 {
        println!("http: shutting down, draining {open} open connection(s)");
    }
    graceful.shutdown().await;
}

type ResponseBody = http_body_util::combinators::UnsyncBoxBody<Bytes, std::io::Error>;

fn full(bytes: Bytes) -> ResponseBody {
    Full::new(bytes)
        .map_err(|never: Infallible| match never {})
        .boxed_unsync()
}

/// One request: read it within its limits, then hand it to main.rs.
async fn dispatch(ctx: Arc<Ctx>, req: Request<Incoming>) -> Response<ResponseBody> {
    ctx.stats.requests.fetch_add(1, Ordering::SeqCst);
    let version = match req.version() {
        hyper::Version::HTTP_10 => "HTTP/1.0",
        _ => "HTTP/1.1",
    };
    let target = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| req.uri().to_string());
    let request_line = format!("{} {target} {version}\r\n", req.method());
    let mut headers: HashMap<String, String> = HashMap::with_capacity(req.headers().len());
    for (name, value) in req.headers() {
        headers.insert(
            name.as_str().to_string(),
            String::from_utf8_lossy(value.as_bytes()).trim().to_string(),
        );
    }

    // Reject an oversized body from its declared length, before reading
    // (or allocating) any of it; the connection is closed rather than
    // drained.
    let max_body = crate::auth::max_body_bytes();
    let declared = headers
        .get("content-length")
        .and_then(|v| v.parse::<usize>().ok());
    if let Some(len) = declared.filter(|len| *len > max_body) {
        return to_response(crate::body_too_large(Some(len), max_body), true);
    }

    // The whole body within one budget; a chunked body is held to the same
    // size limit as it streams.
    let collected = tokio::time::timeout(
        ctx.config.io_timeout,
        Limited::new(req.into_body(), max_body).collect(),
    )
    .await;
    let body = match collected {
        Ok(Ok(collected)) => collected.to_bytes(),
        Ok(Err(e)) if e.downcast_ref::<LengthLimitError>().is_some() => {
            return to_response(crate::body_too_large(None, max_body), true);
        }
        Ok(Err(e)) => {
            return to_response(
                crate::json_error(400, &format!("failed to read request body: {e}")),
                true,
            );
        }
        Err(_) => return to_response(request_timeout(ctx.config.io_timeout), true),
    };

    // Probes never queue behind handlers in the blocking pool: they are
    // cheap and non-blocking, and liveness must answer under load.
    if crate::is_probe(&request_line) {
        return to_response(
            crate::handle_request(&ctx.state, &request_line, &headers, &body),
            false,
        );
    }

    #[cfg(all(feature = "s3", feature = "postgres"))]
    if raw_segment::accepts_raw(&headers) {
        if let Some(path) = crate::internal_segment_raw_path(&ctx.state, &request_line, &headers) {
            if let Some(response) = raw_segment::stream_file(&path).await {
                return response;
            }
        }
    }

    let state = ctx.state.clone();
    let stats = ctx.stats.clone();
    let handled = tokio::task::spawn_blocking(move || {
        let _gauge = Gauge::enter(&stats.handlers, &stats.peak_handlers);
        crate::handle_request(&state, &request_line, &headers, &body)
    })
    .await;
    let raw =
        handled.unwrap_or_else(|e| crate::json_error(500, &format!("request handler failed: {e}")));
    to_response(raw, false)
}

/// Raw peer segment streaming (only built where peer hydration exists).
#[cfg(all(feature = "s3", feature = "postgres"))]
mod raw_segment {
    use super::*;
    use http_body::{Body, Frame, SizeHint};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Instant;

    /// Whether a request's `x-kosha-accept-encoding` lists `raw`.
    pub(super) fn accepts_raw(headers: &HashMap<String, String>) -> bool {
        headers.get(ACCEPT_ENCODING_HEADER).is_some_and(|v| {
            v.split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("raw"))
        })
    }

    /// A raw `200` streaming `path`, or `None` if it cannot be opened (evicted
    /// between the check and the open), leaving the caller to answer the
    /// ordinary way — a 404 the peer retries.
    pub(super) async fn stream_file(path: &std::path::Path) -> Option<Response<ResponseBody>> {
        let started = Instant::now();
        let file = tokio::fs::File::open(path).await.ok()?;
        let len = file.metadata().await.ok()?.len();
        let body = FileBody {
            file,
            remaining: len,
            buf: vec![0; SEGMENT_CHUNK_BYTES],
        };
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_LENGTH, len)
            .header(hyper::header::CONTENT_TYPE, "application/octet-stream")
            .header(ENCODING_HEADER, "raw")
            .body(body.boxed_unsync())
            .ok()?;
        println!(
            "request: GET /internal/segment status=200 tenant=service ms={:.1} in_bytes=0 out_bytes={len} encoding=raw",
            started.elapsed().as_secs_f64() * 1e3
        );
        Some(response)
    }

    /// A file as a response body, read and sent [`SEGMENT_CHUNK_BYTES`] at a
    /// time: hyper polls for the next chunk only once the previous one is
    /// written, so memory per transfer is one chunk whatever the file size.
    struct FileBody {
        file: tokio::fs::File,
        remaining: u64,
        buf: Vec<u8>,
    }

    impl Body for FileBody {
        type Data = Bytes;
        type Error = std::io::Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
            use tokio::io::AsyncRead;
            let this = self.get_mut();
            if this.remaining == 0 {
                return Poll::Ready(None);
            }
            let want = this.buf.len().min(this.remaining as usize);
            let mut read_buf = tokio::io::ReadBuf::new(&mut this.buf[..want]);
            match Pin::new(&mut this.file).poll_read(cx, &mut read_buf) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Ready(Ok(())) => {
                    let n = read_buf.filled().len();
                    if n == 0 {
                        // Truncated under us: fail the transfer, so the peer
                        // sees a short body rather than installing it.
                        return Poll::Ready(Some(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "segment file shrank while streaming",
                        ))));
                    }
                    this.remaining -= n as u64;
                    Poll::Ready(Some(Ok(Frame::data(Bytes::copy_from_slice(
                        read_buf.filled(),
                    )))))
                }
            }
        }

        fn is_end_stream(&self) -> bool {
            self.remaining == 0
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(self.remaining)
        }
    }
}

fn request_timeout(budget: Duration) -> String {
    eprintln!("WARN: request failed: status=408 body not received within {budget:?}");
    let body = serde_json::json!({
        "error": format!(
            "request body not received within {}s (KOSHA_HTTP_IO_TIMEOUT_SECS)",
            budget.as_secs()
        )
    })
    .to_string();
    format!(
        "HTTP/1.1 408 Request Timeout\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Parse a response string from main.rs (status line, headers, blank line,
/// body) into a hyper response. Status code, reason phrase, header order and
/// body are kept as they are; `connection: close` is dropped unless `close`
/// is set, so the connection stays open for the next request. The
/// `content-length` sent is always the real body length.
pub(crate) fn to_response(raw: String, close: bool) -> Response<ResponseBody> {
    let split = raw.find("\r\n\r\n");
    let head_end = split.unwrap_or(raw.len());
    let body_start = split.map_or(raw.len(), |i| i + 4);
    let body_len = raw.len() - body_start;

    let mut builder = Response::builder();
    {
        let head = &raw[..head_end];
        let mut lines = head.split("\r\n");
        let status_line = lines.next().unwrap_or_default();
        let mut parts = status_line.splitn(3, ' ');
        let _version = parts.next();
        let status = parts
            .next()
            .and_then(|code| code.trim().parse::<u16>().ok())
            .and_then(|code| StatusCode::from_u16(code).ok())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let reason = parts.next().unwrap_or_default().trim();
        builder = builder.status(status);
        if !reason.is_empty() && Some(reason) != status.canonical_reason() {
            if let Ok(phrase) = hyper::ext::ReasonPhrase::try_from(reason.as_bytes().to_vec()) {
                builder = builder.extension(phrase);
            }
        }
        let mut saw_length = false;
        for line in lines {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            let name = name.trim();
            if name.eq_ignore_ascii_case("connection") {
                if close {
                    builder = builder.header(hyper::header::CONNECTION, "close");
                }
                continue;
            }
            if name.eq_ignore_ascii_case("content-length") {
                saw_length = true;
                builder = builder.header(hyper::header::CONTENT_LENGTH, body_len);
                continue;
            }
            let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(value.trim()),
            ) else {
                continue;
            };
            builder = builder.header(name, value);
        }
        if !saw_length {
            builder = builder.header(hyper::header::CONTENT_LENGTH, body_len);
        }
    }
    let body = Bytes::from(raw.into_bytes()).slice(body_start..);
    builder.body(full(body)).unwrap_or_else(|e| {
        let mut fallback = Response::new(full(Bytes::from(format!(
            "{{\"error\":\"invalid response: {e}\"}}"
        ))));
        *fallback.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        fallback
    })
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
