"""The transport keeps connections alive and reuses them.

Opening a connection per request costs a DNS lookup (in Kubernetes, ndots:5
makes that ~10 UDP queries, and a lost reply stalls glibc for 5 s) and a TCP
handshake. These tests run the real transport against a local HTTP/1.1
server that counts the connections it accepts.
"""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from kosha_client import KoshaRequestError
from kosha_client import transport as transport_mod
from kosha_client.transport import Transport


class CountingServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, handler):
        super().__init__(("127.0.0.1", 0), handler)
        self.accepts = 0
        self.requests = 0
        self.script = []  # (status, headers, body) answered in order, last repeats
        self.delay = 0.0
        self.close_after_response = False
        self.lock = threading.Lock()

    def get_request(self):
        conn = super().get_request()
        with self.lock:
            self.accepts += 1
        return conn

    def next_answer(self):
        with self.lock:
            self.requests += 1
            if len(self.script) > 1:
                return self.script.pop(0)
            return self.script[0] if self.script else (200, {}, None)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive, unlike the HTTP/1.0 default

    def _answer(self):
        length = int(self.headers.get("content-length") or 0)
        sent = self.rfile.read(length)
        status, headers, body = self.server.next_answer()
        if self.server.delay:
            time.sleep(self.server.delay)
        if body is None:  # echo, so threads can check they got their own answer
            body = {"path": self.path, "body": json.loads(sent) if sent else None}
        data = json.dumps(body).encode()
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        if self.server.close_after_response:
            # Close without a "connection: close" header: the client learns
            # only when it next uses the connection, like an idle timeout.
            self.close_connection = True

    do_GET = do_POST = do_DELETE = _answer

    def log_message(self, *args):  # quiet
        pass


@pytest.fixture
def server():
    httpd = CountingServer(Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield httpd
    httpd.shutdown()
    httpd.server_close()


@pytest.fixture(autouse=True)
def fresh_pools(monkeypatch):
    monkeypatch.setattr(transport_mod, "_POOLS", {}, raising=False)
    monkeypatch.setattr(Transport, "backoff_base", 0.01)
    monkeypatch.setattr(Transport, "backoff_cap", 0.02)


def transport(server, **kwargs):
    kwargs.setdefault("max_retries", 3)
    return Transport(f"http://127.0.0.1:{server.server_port}", **kwargs)


SEARCH = "v1/namespaces/ns/search"


def test_sequential_requests_share_one_connection(server):
    t = transport(server)
    for i in range(50):
        assert t.request("POST", SEARCH, {"i": i})["body"] == {"i": i}
    assert server.requests == 50
    assert server.accepts == 1


def test_clients_for_the_same_host_share_connections(server):
    # The backend builds a new KoshaClient (so a new Transport) per call.
    for i in range(20):
        transport(server).request("GET", f"v1/namespaces/ns/x{i}")
    assert server.accepts == 1


def test_a_connection_the_server_closed_while_idle_is_replaced(server):
    server.close_after_response = True
    t = transport(server, max_retries=1)
    for i in range(5):
        assert t.request("POST", SEARCH, {"i": i})["body"] == {"i": i}
    assert server.requests == 5
    assert server.accepts == 5


def test_a_read_on_a_connection_closed_after_the_liveness_check_is_resent(
    server, monkeypatch
):
    # The race the liveness check cannot see: the server closes the
    # connection between the check and the request.
    t = transport(server, max_retries=1)
    t.request("POST", SEARCH, {"i": 0})
    server.close_after_response = True
    t.request("POST", SEARCH, {"i": 1})  # server closes after answering
    server.close_after_response = False
    monkeypatch.setattr(transport_mod, "_alive", lambda sock: True)
    # One attempt only: the resend on a new connection is not an attempt.
    assert t.request("POST", SEARCH, {"i": 2})["body"] == {"i": 2}
    assert server.requests == 3
    assert server.accepts == 2


def test_a_write_on_a_stale_connection_takes_the_connection_error_retry(
    server, monkeypatch
):
    t = transport(server, max_retries=2)
    # Several idle connections, all closed by the server (a restart).
    server.close_after_response = True
    threads = [
        threading.Thread(target=t.request, args=("POST", SEARCH, {"i": i}))
        for i in range(4)
    ]
    server.delay = 0.05
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    server.close_after_response = False
    server.delay = 0.0
    monkeypatch.setattr(transport_mod, "_alive", lambda sock: True)
    # The first failure drops every idle connection, so the second attempt
    # opens a new one instead of trying the next stale one.
    body = {"doc": 1}
    assert t.request("POST", "v1/namespaces/ns/documents", body)["body"] == body


def test_threads_share_the_pool_safely(server):
    server.delay = 0.002
    t = transport(server)
    errors = []

    def worker(n):
        try:
            for i in range(40):
                got = t.request("POST", SEARCH, {"n": n, "i": i})
                assert got["body"] == {"n": n, "i": i}
        except (AssertionError, KoshaRequestError, OSError) as e:  # surfaced below
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(16)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert errors == []
    assert server.requests == 16 * 40
    # At most one connection per concurrent request, not one per request.
    assert server.accepts <= 16
    pool = next(iter(transport_mod._POOLS.values()))
    assert len(pool._idle) <= transport_mod._POOL_MAXSIZE


def test_idle_connections_expire(server, monkeypatch):
    t = transport(server)
    t.request("GET", "v1/x")
    monkeypatch.setattr(transport_mod, "_POOL_IDLE_SECS", 0.0)
    time.sleep(0.01)
    t.request("GET", "v1/x")
    assert server.accepts == 2


def test_timeout_applies_to_a_reused_connection(server):
    transport(server, timeout=60).request("GET", "v1/x")  # pools the connection
    server.delay = 1.0
    t = transport(server, timeout=0.2)
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        t.request("POST", SEARCH, {})
    assert time.monotonic() - started < 0.9
    assert server.accepts == 1


def test_a_response_timeout_raises_as_with_urlopen(server):
    # urlopen raised the socket timeout itself when the response did not
    # come in time, and nothing retried it; a slow search is not resent.
    server.delay = 0.5
    t = transport(server, timeout=0.1, max_retries=3)
    with pytest.raises(TimeoutError):
        t.request("POST", SEARCH, {})
    time.sleep(0.5)
    assert server.requests == 1


def test_a_refused_connection_is_retried_as_before(monkeypatch):
    t = Transport("http://127.0.0.1:9", max_retries=2)  # discard port: refused
    with pytest.raises(KoshaRequestError) as info:
        t.request("POST", SEARCH, {})
    assert info.value.status_code == 0
    assert "after 2 retries" in str(info.value)


def test_old_connections_are_retired(server, monkeypatch):
    t = transport(server)
    t.request("GET", "v1/x")
    monkeypatch.setattr(transport_mod, "_POOL_MAX_AGE_SECS", 0.0)
    t.request("GET", "v1/x")  # reuses the pooled one, then retires it
    t.request("GET", "v1/x")  # opens a new one
    assert server.accepts == 2
    assert len(next(iter(transport_mod._POOLS.values()))._idle) == 0


def test_shed_is_retried_on_the_same_connection(server):
    server.script = [
        (429, {"retry-after": "0"}, {"error": "search overloaded"}),
        (503, {"retry-after": "0"}, {"error": "unavailable"}),
        (200, {}, {"ok": True}),
    ]
    assert transport(server).request("POST", SEARCH, {}) == {"ok": True}
    assert server.requests == 3
    assert server.accepts == 1


def test_error_status_keeps_the_connection(server):
    server.script = [(400, {}, {"error": "bad query"}), (200, {}, {"ok": True})]
    t = transport(server)
    with pytest.raises(KoshaRequestError) as info:
        t.request("POST", SEARCH, {})
    assert info.value.status_code == 400
    assert info.value.error == "bad query"
    assert t.request("POST", SEARCH, {}) == {"ok": True}
    assert server.accepts == 1


def test_a_proxied_base_url_goes_through_urlopen(server, monkeypatch):
    proxy = f"http://127.0.0.1:{server.server_port}"
    monkeypatch.setenv("http_proxy", proxy)
    monkeypatch.delenv("no_proxy", raising=False)
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.setattr("urllib.request._opener", None)  # urlopen caches proxies
    t = Transport("http://kosha.example:8080")
    # The "proxy" (our server) receives the absolute URL, as a proxy does.
    assert t.request("GET", "v1/x")["path"] == "http://kosha.example:8080/v1/x"
    assert transport_mod._POOLS == {}
    monkeypatch.setattr("urllib.request._opener", None)
