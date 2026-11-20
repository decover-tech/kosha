//! Transport tests for `http.rs` (#259): keep-alive, limits, timeouts,
//! bounded concurrency, graceful shutdown and raw peer segment transfer.
//! Route behavior is covered by main.rs's own tests through
//! `handle_request`/`route`; these drive real sockets.

use super::*;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

/// Process-wide heap counter for the peer-transfer memory test; a
/// pass-through to the system allocator everywhere else.
#[global_allocator]
static ALLOC: cap::Cap<std::alloc::System> = cap::Cap::new(std::alloc::System, usize::MAX);

fn test_dir() -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-http-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config() -> HttpConfig {
    HttpConfig {
        io_timeout: Duration::from_secs(30),
        header_read_timeout: Duration::from_secs(30),
        max_connections: DEFAULT_MAX_CONNECTIONS,
        max_blocking_threads: DEFAULT_MAX_BLOCKING_THREADS,
    }
}

struct Server {
    addr: SocketAddr,
    stats: Arc<HttpStats>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Server {
    fn start(state: AppState, config: HttpConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stats = Arc::new(HttpStats::default());
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let server_stats = stats.clone();
        let thread = std::thread::spawn(move || {
            serve_until(listener, Arc::new(state), config, server_stats, async {
                let _ = stopped.await;
            })
        });
        Self {
            addr,
            stats,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    /// Signal shutdown and wait for the server to drain and return.
    fn stop(&mut self) -> Duration {
        let started = Instant::now();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
        started.elapsed()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

/// A runtime for test clients: one thread, so client connections add no
/// threads of their own to the process.
fn client_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

struct Reply {
    status: u16,
    head: String,
    body: Vec<u8>,
}

impl Reply {
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }
    fn header(&self, name: &str) -> Option<String> {
        self.head.split("\r\n").skip(1).find_map(|line| {
            let (k, v) = line.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case(name)
                .then(|| v.trim().to_string())
        })
    }
}

/// Write one raw request on an open connection and read one response
/// (content-length framed), leaving the connection open.
async fn exchange(conn: &mut BufReader<tokio::net::TcpStream>, request: &str) -> Reply {
    conn.get_mut().write_all(request.as_bytes()).await.unwrap();
    read_reply(conn)
        .await
        .expect("connection closed before a response")
}

async fn read_reply(conn: &mut BufReader<tokio::net::TcpStream>) -> Option<Reply> {
    let mut head = String::new();
    loop {
        let mut line = String::new();
        if conn.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        if line == "\r\n" {
            break;
        }
        head.push_str(&line);
    }
    let status = head.get(9..12)?.parse().ok()?;
    let mut reply = Reply {
        status,
        head: head.trim_end().to_string(),
        body: Vec::new(),
    };
    let len: usize = reply
        .header("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    reply.body = vec![0; len];
    conn.read_exact(&mut reply.body).await.ok()?;
    Some(reply)
}

async fn connect(addr: SocketAddr) -> BufReader<tokio::net::TcpStream> {
    BufReader::new(tokio::net::TcpStream::connect(addr).await.unwrap())
}

fn get(path: &str) -> String {
    format!("GET {path} HTTP/1.1\r\nhost: k\r\n\r\n")
}

fn post(path: &str, body: &serde_json::Value) -> String {
    let body = body.to_string();
    format!(
        "POST {path} HTTP/1.1\r\nhost: k\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    )
}

/// A state with a searchable, flushed namespace `docs`.
fn search_state() -> AppState {
    let state = AppState::new(test_dir());
    let call = |line: &str, body: serde_json::Value| {
        let response = crate::route(
            &format!("{line} HTTP/1.1\r\n"),
            &HashMap::new(),
            body.to_string().as_bytes(),
            "dev",
            &state,
        );
        assert!(response.starts_with("HTTP/1.1 200"), "{line}: {response}");
    };
    call(
        "POST /v1/namespaces",
        serde_json::json!({"namespace": "docs"}),
    );
    call(
        "POST /v1/namespaces/docs/documents",
        serde_json::json!({"documents": [
            {"id": "d1", "fields": [{"name": "title", "field_type": "Text", "value": "quick brown fox"}]},
            {"id": "d2", "fields": [{"name": "title", "field_type": "Text", "value": "lazy dog"}]}
        ]}),
    );
    call("POST /v1/namespaces/docs/flush", serde_json::json!({}));
    state
}

fn search_request() -> String {
    post(
        "/v1/namespaces/docs/search",
        &serde_json::json!({"query_text": "quick"}),
    )
}

// ─── Response conversion ────────────────────────────────────────────────

#[test]
fn to_response_keeps_status_reason_headers_and_body_and_drops_connection_close() {
    let rt = client_runtime();
    let raw = "HTTP/1.1 202 Response\r\ncontent-length: 7\r\ncontent-type: application/json\r\nx-extra: 1\r\nconnection: close\r\n\r\n{\"a\":1}".to_string();
    let response = to_response(raw, false);
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        response
            .extensions()
            .get::<hyper::ext::ReasonPhrase>()
            .map(|r| r.as_bytes().to_vec()),
        Some(b"Response".to_vec()),
        "a non-canonical reason phrase is kept"
    );
    let names: Vec<&str> = response.headers().keys().map(|k| k.as_str()).collect();
    assert_eq!(names, ["content-length", "content-type", "x-extra"]);
    assert_eq!(response.headers()["content-length"], "7");
    let body = rt
        .block_on(response.into_body().collect())
        .unwrap()
        .to_bytes();
    assert_eq!(&body[..], b"{\"a\":1}");

    let closing = to_response(
        "HTTP/1.1 413 Payload Too Large\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".into(),
        true,
    );
    assert_eq!(closing.headers()["connection"], "close");
    // Canonical reasons need no extension.
    assert!(closing
        .extensions()
        .get::<hyper::ext::ReasonPhrase>()
        .is_none());
}

#[test]
fn wire_response_matches_the_old_server_apart_from_connection_close() {
    let server = Server::start(AppState::new(test_dir()), config());
    let rt = client_runtime();
    let mut raw = Vec::new();
    rt.block_on(async {
        let mut conn = connect(server.addr).await;
        conn.get_mut()
            .write_all(b"GET /healthz HTTP/1.1\r\nhost: k\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
        conn.read_to_end(&mut raw).await.unwrap();
    });
    let old = crate::json_ok(&serde_json::json!({"status": "ok"}));
    assert_eq!(
        String::from_utf8(raw).unwrap(),
        old,
        "byte-for-byte the response the hand-rolled server wrote"
    );

    // Keep-alive: the same response without `connection: close`.
    let reply = rt.block_on(async {
        let mut conn = connect(server.addr).await;
        exchange(&mut conn, &get("/healthz")).await
    });
    assert_eq!(
        format!(
            "{}\r\n\r\n{}",
            reply.head,
            String::from_utf8_lossy(&reply.body)
        ),
        old.replace("connection: close\r\n", "")
    );
}

// ─── Keep-alive ─────────────────────────────────────────────────────────

#[test]
fn keep_alive_serves_many_requests_on_one_connection() {
    let server = Server::start(search_state(), config());
    let rt = client_runtime();
    rt.block_on(async {
        let mut conn = connect(server.addr).await;
        for _ in 0..3 {
            let reply = exchange(&mut conn, &get("/healthz")).await;
            assert_eq!(reply.status, 200);
            assert_eq!(reply.header("connection"), None);
            let reply = exchange(&mut conn, &search_request()).await;
            assert_eq!(reply.status, 200, "{}", reply.json());
            assert_eq!(reply.json()["total_hits"], 1, "{}", reply.json());
        }
    });
    assert_eq!(server.stats.accepted(), 1, "one TCP connection for all six");
    assert_eq!(server.stats.requests(), 6);
}

// ─── Body limits and timeouts ───────────────────────────────────────────

#[test]
fn declared_oversized_body_gets_413_without_being_read() {
    let server = Server::start(AppState::new(test_dir()), config());
    let rt = client_runtime();
    let reply = rt.block_on(async {
        let mut conn = connect(server.addr).await;
        // Declares 1 TB and sends none of it: the answer can only come
        // from the header.
        let request = format!(
            "POST /v1/namespaces/docs/documents HTTP/1.1\r\nhost: k\r\ncontent-length: {}\r\n\r\n",
            1u64 << 40
        );
        let reply = tokio::time::timeout(Duration::from_secs(5), exchange(&mut conn, &request))
            .await
            .expect("413 must not wait for the body");
        // ...and the connection is closed rather than drained.
        let mut rest = Vec::new();
        let closed = tokio::time::timeout(Duration::from_secs(5), conn.read_to_end(&mut rest))
            .await
            .is_ok();
        (reply, closed)
    });
    let (reply, closed) = reply;
    assert_eq!(reply.status, 413);
    assert!(
        reply.json()["error"]
            .as_str()
            .unwrap()
            .contains("KOSHA_MAX_BODY_BYTES"),
        "{}",
        reply.json()
    );
    assert!(closed, "an unread oversized body must end the connection");
}

#[test]
fn chunked_body_over_the_limit_gets_413_while_streaming() {
    let server = Server::start(AppState::new(test_dir()), config());
    let max = crate::auth::max_body_bytes();
    let rt = client_runtime();
    let reply = rt.block_on(async {
        let mut conn = connect(server.addr).await;
        conn.get_mut()
            .write_all(
                b"POST /v1/namespaces/docs/documents HTTP/1.1\r\nhost: k\r\ntransfer-encoding: chunked\r\n\r\n",
            )
            .await
            .unwrap();
        // Stream 1 MiB chunks until the server answers.
        let chunk = vec![b'x'; 1 << 20];
        let mut sent = 0usize;
        let writer = async {
            while sent <= max + (1 << 20) {
                let mut frame = format!("{:x}\r\n", chunk.len()).into_bytes();
                frame.extend_from_slice(&chunk);
                frame.extend_from_slice(b"\r\n");
                if conn.get_mut().write_all(&frame).await.is_err() {
                    break;
                }
                sent += chunk.len();
            }
        };
        writer.await;
        read_reply(&mut conn).await
    });
    let reply = reply.expect("a 413, not a dropped connection");
    assert_eq!(reply.status, 413, "{}", reply.json());
}

#[test]
fn slow_body_gets_408_after_the_io_timeout() {
    let mut cfg = config();
    cfg.io_timeout = Duration::from_millis(300);
    let server = Server::start(AppState::new(test_dir()), cfg);
    let rt = client_runtime();
    let reply = rt.block_on(async {
        let mut conn = connect(server.addr).await;
        conn.get_mut()
            .write_all(b"POST /v1/namespaces/docs/search HTTP/1.1\r\nhost: k\r\ncontent-length: 100\r\n\r\n{")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), read_reply(&mut conn))
            .await
            .expect("the body budget must end the request")
    });
    assert_eq!(reply.expect("a 408").status, 408);
}

#[test]
fn slowloris_headers_time_out_without_blocking_healthz() {
    let mut cfg = config();
    cfg.header_read_timeout = Duration::from_millis(500);
    let server = Server::start(AppState::new(test_dir()), cfg);
    let rt = client_runtime();
    rt.block_on(async {
        // Many clients that each send half a request head and stall.
        let mut stalled = Vec::new();
        for _ in 0..50 {
            let mut conn = connect(server.addr).await;
            conn.get_mut()
                .write_all(b"GET /healthz HTTP/1.1\r\nhost: k\r\nx-slow: ")
                .await
                .unwrap();
            stalled.push(conn);
        }
        // Meanwhile probes answer at once.
        let started = Instant::now();
        let mut conn = connect(server.addr).await;
        let reply = exchange(&mut conn, &get("/healthz")).await;
        assert_eq!(reply.status, 200);
        assert!(started.elapsed() < Duration::from_millis(400));
        // And every stalled connection is cut off by the header timeout.
        for mut conn in stalled {
            let mut rest = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), conn.read_to_end(&mut rest))
                .await
                .expect("a stalled header read must be closed by the server")
                .ok();
        }
    });
}

#[test]
fn idle_keep_alive_connection_is_closed_after_the_header_timeout() {
    let mut cfg = config();
    cfg.header_read_timeout = Duration::from_millis(300);
    let server = Server::start(AppState::new(test_dir()), cfg);
    let rt = client_runtime();
    rt.block_on(async {
        let mut conn = connect(server.addr).await;
        assert_eq!(exchange(&mut conn, &get("/healthz")).await.status, 200);
        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), conn.read_to_end(&mut rest))
            .await
            .expect("an idle keep-alive connection must not hold its slot forever")
            .ok();
    });
}

// ─── Bounded concurrency ────────────────────────────────────────────────

/// `clients` keep-alive connections at once, each issuing `rounds` ×
/// (2 × /healthz + 1 × search), all from one client thread.
fn run_concurrent_clients(clients: usize, rounds: usize, cfg: HttpConfig) -> Arc<HttpStats> {
    let max_connections = cfg.max_connections;
    let server = Server::start(search_state(), cfg);
    let rt = client_runtime();
    let ok = rt.block_on(async {
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..clients {
            let addr = server.addr;
            tasks.spawn(async move {
                let mut conn = connect(addr).await;
                let mut ok = 0usize;
                for _ in 0..rounds {
                    ok += usize::from(exchange(&mut conn, &get("/healthz")).await.status == 200);
                    let search = exchange(&mut conn, &search_request()).await;
                    ok += usize::from(search.status == 200 && search.json()["total_hits"] == 1);
                    ok += usize::from(exchange(&mut conn, &get("/v1/healthz")).await.status == 200);
                }
                ok
            });
        }
        let mut ok = 0usize;
        while let Some(done) = tasks.join_next().await {
            ok += done.unwrap();
        }
        ok
    });
    assert_eq!(
        ok,
        clients * rounds * 3,
        "every request on every connection"
    );
    assert_eq!(server.stats.accepted(), clients as u64);
    assert!(
        server.stats.peak_connections() <= max_connections,
        "peak {} connections over the limit of {max_connections}",
        server.stats.peak_connections()
    );
    #[cfg(target_os = "linux")]
    {
        // Workers (one per core) + the blocking pool's cap + the test
        // harness — never one thread per connection.
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let threads: usize = status
            .lines()
            .find_map(|l| l.strip_prefix("Threads:"))
            .and_then(|v| v.trim().parse().ok())
            .unwrap();
        let cores = std::thread::available_parallelism().map_or(8, |n| n.get());
        assert!(
            threads < clients.min(cores + 256),
            "{threads} threads for {clients} connections"
        );
    }
    server.stats.clone()
}

#[test]
fn concurrent_keep_alive_connections_complete_with_bounded_handlers() {
    let mut cfg = config();
    cfg.max_blocking_threads = 8;
    let stats = run_concurrent_clients(200, 3, cfg);
    // The macOS-portable form of "no thread per connection": handlers ran
    // on at most the blocking pool's threads, never one per connection.
    assert!(
        stats.peak_handlers() <= 8,
        "peak {} concurrent handlers",
        stats.peak_handlers()
    );
    assert!(stats.peak_connections() > 8, "connections were concurrent");
}

#[test]
fn connection_limit_queues_excess_clients_instead_of_refusing_them() {
    let mut cfg = config();
    cfg.max_connections = 16;
    let stats = run_concurrent_clients(64, 2, cfg);
    assert!(stats.peak_connections() <= 16);
}

/// The acceptance load: 1,000 concurrent keep-alive connections, each
/// issuing several /healthz and search requests.
#[test]
#[ignore = "heavy: 1,000 connections; run with --ignored"]
fn thousand_concurrent_keep_alive_connections_complete() {
    let mut cfg = config();
    cfg.max_blocking_threads = 64;
    let started = Instant::now();
    let stats = run_concurrent_clients(1000, 5, cfg);
    eprintln!(
        "1000 connections x 15 requests in {:?}: peak connections {}, peak handlers {}",
        started.elapsed(),
        stats.peak_connections(),
        stats.peak_handlers()
    );
    assert!(stats.peak_handlers() <= 64);
}

// ─── Graceful shutdown ──────────────────────────────────────────────────

#[test]
fn shutdown_drains_in_flight_requests_and_closes_idle_connections() {
    let mut server = Server::start(search_state(), config());
    let rt = client_runtime();
    let (in_flight_reply, idle_closed) = rt.block_on(async {
        let mut idle = connect(server.addr).await;
        assert_eq!(exchange(&mut idle, &get("/healthz")).await.status, 200);

        // A request whose body is still arriving when shutdown starts.
        let body = serde_json::json!({"query_text": "quick"}).to_string();
        let (first, second) = body.split_at(5);
        let mut in_flight = connect(server.addr).await;
        in_flight
            .get_mut()
            .write_all(
                format!(
                    "POST /v1/namespaces/docs/search HTTP/1.1\r\nhost: k\r\ncontent-length: {}\r\n\r\n{first}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let stop = server.stop.take().unwrap();
        let _ = stop.send(());
        tokio::time::sleep(Duration::from_millis(200)).await;

        // The idle connection is closed...
        let mut rest = Vec::new();
        let idle_closed = tokio::time::timeout(Duration::from_secs(5), idle.read_to_end(&mut rest))
            .await
            .is_ok();
        // ...new ones are refused...
        assert!(
            tokio::net::TcpStream::connect(server.addr).await.is_err()
                || read_reply(&mut connect(server.addr).await).await.is_none()
        );
        // ...and the in-flight request still gets its answer.
        in_flight
            .get_mut()
            .write_all(second.as_bytes())
            .await
            .unwrap();
        (read_reply(&mut in_flight).await, idle_closed)
    });
    let reply = in_flight_reply.expect("an in-flight request is answered during drain");
    assert_eq!(reply.status, 200, "{}", reply.json());
    assert!(idle_closed);
    assert!(
        server.stop() < Duration::from_secs(5),
        "the server returned"
    );
}

// ─── Raw peer segment transfer ──────────────────────────────────────────

#[cfg(all(feature = "s3", feature = "postgres"))]
mod segment {
    use super::*;

    const SECRET: &str = "peer-test-secret";

    fn peer_with(file: &str, bytes: &[u8]) -> AppState {
        let mut peer = AppState::new(test_dir());
        peer.internal_service_key = Some(SECRET.into());
        peer.cache.put_bytes(file, bytes).unwrap();
        peer
    }

    fn requester(timeout: Duration) -> AppState {
        let mut requester = AppState::new(test_dir());
        requester.internal_auth_key = Some(SECRET.into());
        requester.peer_http_client = Some(
            reqwest::blocking::Client::builder()
                .timeout(timeout)
                .build()
                .unwrap(),
        );
        requester
    }

    fn segment_get(path: &str, raw: bool) -> String {
        format!(
            "GET /internal/segment/{path} HTTP/1.1\r\nhost: k\r\nauthorization: Bearer {SECRET}\r\n{}\r\n",
            if raw { "x-kosha-accept-encoding: raw\r\n" } else { "" }
        )
    }

    #[test]
    fn raw_is_streamed_only_to_peers_that_ask_for_it() {
        let server = Server::start(
            peer_with("ns/seg/doc_store.bin", b"\0\x01raw bytes\xff"),
            config(),
        );
        let rt = client_runtime();
        let (raw, legacy) = rt.block_on(async {
            let mut conn = connect(server.addr).await;
            let raw = exchange(&mut conn, &segment_get("ns/seg/doc_store.bin", true)).await;
            let legacy = exchange(&mut conn, &segment_get("ns/seg/doc_store.bin", false)).await;
            (raw, legacy)
        });
        assert_eq!(raw.status, 200);
        assert_eq!(raw.header("x-kosha-encoding").as_deref(), Some("raw"));
        assert_eq!(
            raw.header("content-type").as_deref(),
            Some("application/octet-stream")
        );
        assert_eq!(raw.body, b"\0\x01raw bytes\xff");

        // A pre-#259 peer sends no header and gets base64, as before.
        assert_eq!(legacy.status, 200);
        assert_eq!(legacy.header("x-kosha-encoding").as_deref(), Some("base64"));
        use base64::Engine;
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(&legacy.body)
                .unwrap(),
            b"\0\x01raw bytes\xff"
        );
    }

    #[test]
    fn raw_requests_are_refused_exactly_as_before() {
        let server = Server::start(peer_with("ns/seg/doc_store.bin", b"x"), config());
        let rt = client_runtime();
        rt.block_on(async {
            let mut conn = connect(server.addr).await;
            let missing = exchange(&mut conn, &segment_get("ns/seg/missing.bin", true)).await;
            assert_eq!(missing.status, 404);
            let traversal = exchange(&mut conn, &segment_get("../../etc/passwd", true)).await;
            assert_eq!(traversal.status, 400);
            let unauthorized = exchange(
                &mut conn,
                "GET /internal/segment/ns/seg/doc_store.bin HTTP/1.1\r\nhost: k\r\nx-kosha-accept-encoding: raw\r\n\r\n",
            )
            .await;
            assert_eq!(unauthorized.status, 403);
        });
    }

    #[test]
    fn fetch_from_peer_installs_raw_bytes_in_the_cache() {
        let peer = Server::start(
            peer_with("ns/seg/inverted.idx", b"raw peer bytes"),
            config(),
        );
        let requester = requester(Duration::from_secs(5));
        let bytes = requester
            .fetch_from_peer(&peer.addr.to_string(), "ns/seg/inverted.idx")
            .unwrap();
        assert_eq!(bytes, 14);
        let local = requester.cache.path_for("ns/seg/inverted.idx").unwrap();
        assert_eq!(std::fs::read(&local).unwrap(), b"raw peer bytes");
        let leftovers: Vec<_> = std::fs::read_dir(local.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(leftovers.len(), 1, "no temp files left: {leftovers:?}");
    }

    /// A pre-#259 peer: ignores `x-kosha-accept-encoding` and answers base64.
    #[test]
    fn fetch_from_peer_still_decodes_a_legacy_base64_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            use std::io::{BufRead, Write};
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                while reader.read_line(&mut line).unwrap_or(0) > 0 && line != "\r\n" {
                    line.clear();
                }
                let _ = stream.write_all(crate::segment_response(b"legacy bytes").as_bytes());
            }
        });
        let requester = requester(Duration::from_secs(5));
        let bytes = requester
            .fetch_from_peer(&addr.to_string(), "ns/seg/doc_store.bin")
            .unwrap();
        assert_eq!(bytes, 12);
        let local = requester.cache.path_for("ns/seg/doc_store.bin").unwrap();
        assert_eq!(std::fs::read(local).unwrap(), b"legacy bytes");
    }

    /// Acceptance: a 500 MB peer transfer streams in O(chunk) memory. The
    /// file is sparse on the peer (no disk cost to create) and real on the
    /// requester. Both ends run in this process, so the heap peak covers
    /// the serving and the receiving side.
    #[test]
    #[ignore = "writes 500 MB to the temp dir; run with --ignored"]
    fn peer_transfer_of_500_mb_streams_in_bounded_memory() {
        const SIZE: u64 = 500 * 1024 * 1024;
        const MIB: usize = 1024 * 1024;
        let mut peer = AppState::new(test_dir());
        peer.internal_service_key = Some(SECRET.into());
        let path = peer.cache.path_for("ns/seg/doc_store.bin").unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(SIZE).unwrap();
        drop(file);
        let peer_dir = peer.data_dir.clone();
        let server = Server::start(peer, config());
        let requester = requester(Duration::from_secs(600));

        let base = ALLOC.allocated();
        let started = Instant::now();
        let bytes = requester
            .fetch_from_peer(&server.addr.to_string(), "ns/seg/doc_store.bin")
            .unwrap();
        let elapsed = started.elapsed();
        let peak = ALLOC.max_allocated().saturating_sub(base);
        eprintln!(
            "500 MB peer transfer: {:.1}s ({:.0} MB/s), peak heap growth {:.2} MiB",
            elapsed.as_secs_f64(),
            SIZE as f64 / 1e6 / elapsed.as_secs_f64(),
            peak as f64 / MIB as f64
        );
        assert_eq!(bytes, SIZE);
        let local = requester.cache.path_for("ns/seg/doc_store.bin").unwrap();
        assert_eq!(std::fs::metadata(&local).unwrap().len(), SIZE);
        assert!(
            peak < 32 * MIB,
            "peer transfer must stream: heap grew {peak} bytes for a {SIZE}-byte file"
        );
        let _ = std::fs::remove_dir_all(requester.data_dir.clone());
        let _ = std::fs::remove_dir_all(peer_dir);
    }
}
