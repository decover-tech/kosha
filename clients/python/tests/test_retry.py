"""The shared retry policy (clients/conformance/RETRY.md) against a fake server."""

from __future__ import annotations

import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from kosha_client.transport import (
    KoshaRequestError,
    Transport,
    is_idempotent,
    parse_retry_after,
    retryable_status,
)


class FakeServer:
    """Answers each request with ``responses[n]`` (status, body, headers[, delay])."""

    def __init__(self, responses):
        self.responses = responses
        self.requests = []
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def _answer(self):
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                fake.requests.append((self.command, self.path, dict(self.headers), raw))
                n = min(len(fake.requests), len(fake.responses)) - 1
                status, body, headers, *rest = fake.responses[n]
                if rest:
                    time.sleep(rest[0])
                payload = body.encode() if isinstance(body, str) else json.dumps(body).encode()
                try:
                    self.send_response(status)
                    for k, v in headers.items():
                        self.send_header(k, v)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            do_GET = do_POST = do_DELETE = _answer

            def log_message(self, *args):  # noqa: D401 - silence
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def serve():
    servers = []

    def make(*responses):
        s = FakeServer(list(responses))
        servers.append(s)
        return s

    yield make
    for s in servers:
        s.close()


def transport(url, **kw):
    kw.setdefault("base_delay_ms", 5)
    kw.setdefault("max_delay_ms", 20)
    return Transport(url, api_key="sk-test", **kw)


def err(code, retryable, **extra):
    return {"error": f"{code} happened", "code": code, "retryable": retryable, "request_id": "req-1", **extra}


def test_outcome_unknown_is_replayed_only_when_safe():
    unknown = {"retryable": True, "details": {"outcome": "unknown"}}
    assert not retryable_status(503, unknown, idempotent=False)
    assert retryable_status(503, unknown, idempotent=True)
    assert retryable_status(503, {**unknown, "retryable": False}, idempotent=True)
    assert retryable_status(503, unknown, idempotent=False, replay_safe=True)
    assert retryable_status(503, {"retryable": True}, idempotent=False)


def test_retry_after_is_honoured(serve):
    s = serve((429, {"error": "slow down"}, {"Retry-After": "1"}), (200, {"results": []}, {}))
    started = time.monotonic()
    assert transport(s.url).request("POST", "v1/namespaces/n/search", {"query_text": ""}) == {"results": []}
    assert len(s.requests) == 2
    assert time.monotonic() - started >= 0.95


def test_a_quota_429_is_not_retried(serve):
    s = serve((429, err("namespace_quota_exceeded", False), {}))
    with pytest.raises(KoshaRequestError) as e:
        transport(s.url).request("POST", "v1/namespaces/n/search", {})
    assert len(s.requests) == 1
    assert e.value.attempts == 1
    assert e.value.code == "namespace_quota_exceeded"
    assert e.value.retryable is False


def test_bare_503s_back_off_then_succeed(serve):
    s = serve((503, {"error": "x"}, {}), (503, {"error": "x"}, {}), (200, {"status": "flushed"}, {}))
    assert transport(s.url).request("POST", "v1/namespaces/n/flush", {}) == {"status": "flushed"}
    assert len(s.requests) == 3


def test_max_retries_counts_attempts(serve):
    s = serve((503, err("unavailable", True, retry_after_ms=10), {}))
    with pytest.raises(KoshaRequestError) as e:
        transport(s.url).request("GET", "v1/namespaces")
    assert len(s.requests) == 3
    assert e.value.attempts == 3
    s2 = serve((503, err("unavailable", True, retry_after_ms=10), {}))
    with pytest.raises(KoshaRequestError):
        transport(s2.url, max_retries=1).request("GET", "v1/namespaces")
    assert len(s2.requests) == 1


def test_a_long_retry_after_gives_up_at_once(serve):
    s = serve((429, {"error": "later"}, {"Retry-After": "120"}))
    started = time.monotonic()
    with pytest.raises(KoshaRequestError) as e:
        transport(s.url).request("GET", "v1/namespaces")
    assert len(s.requests) == 1
    assert e.value.retry_after_ms == 120000
    assert time.monotonic() - started < 1


def test_the_body_delay_wins_over_the_header(serve):
    s = serve(
        (429, err("tenant_qps_exceeded", True, retry_after_ms=50), {"Retry-After": "120"}),
        (200, {"namespaces": []}, {}),
    )
    started = time.monotonic()
    transport(s.url).request("GET", "v1/namespaces")
    assert len(s.requests) == 2
    assert time.monotonic() - started < 1


def test_the_total_budget_stops_retries(serve):
    s = serve((503, err("unavailable", True, retry_after_ms=500), {}))
    with pytest.raises(KoshaRequestError) as e:
        transport(s.url, total_budget_ms=200).request("GET", "v1/namespaces")
    assert len(s.requests) == 1 and e.value.attempts == 1


def test_502_is_retried_only_when_idempotent(serve):
    s = serve((502, {"error": "bad gateway"}, {}))
    with pytest.raises(KoshaRequestError):
        transport(s.url).request("POST", "v1/namespaces", {"namespace": "a"})
    assert len(s.requests) == 1
    with pytest.raises(KoshaRequestError):
        transport(s.url).request("GET", "v1/namespaces")
    assert len(s.requests) == 4


def test_a_timeout_is_retried_only_when_idempotent(serve):
    s = serve((200, {}, {}, 0.5))
    t = transport(s.url, timeout=0.1)
    with pytest.raises(KoshaRequestError) as create:
        t.request("POST", "v1/namespaces", {"namespace": "a"})
    assert create.value.attempts == 1 and create.value.status_code == 0
    t2 = transport(s.url, timeout=0.1, max_retries=2)
    with pytest.raises(KoshaRequestError) as search:
        t2.request("POST", "v1/namespaces/a/search", {})
    assert search.value.attempts == 2
    t3 = transport(s.url, timeout=0.1, max_retries=2, retry_non_idempotent=True)
    with pytest.raises(KoshaRequestError) as opted:
        t3.request("POST", "v1/namespaces", {"namespace": "a"})
    assert opted.value.attempts == 2
    t4 = transport(s.url, timeout=0.1, max_retries=2, retry_on_timeout=False)
    with pytest.raises(KoshaRequestError) as off:
        t4.request("POST", "v1/namespaces/a/search", {})
    assert off.value.attempts == 1


def test_a_refused_connection_is_retried_for_any_operation():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    with pytest.raises(KoshaRequestError) as e:
        transport(f"http://127.0.0.1:{port}").request("POST", "v1/namespaces", {"namespace": "a"})
    assert e.value.attempts == 3
    assert e.value.status_code == 0
    # retry_on_timeout=False is 0.2.x's fail-fast on every transport error.
    with pytest.raises(KoshaRequestError) as off:
        transport(f"http://127.0.0.1:{port}", retry_on_timeout=False).request("GET", "v1/namespaces")
    assert off.value.attempts == 1


def test_a_legacy_merge_timeout_is_retried(serve):
    s = serve((200, {}, {}, 0.5))
    with pytest.raises(KoshaRequestError) as e:
        transport(s.url, timeout=0.1, max_retries=2).request("POST", "replace", {"namespace": "a", "documents": []})
    assert e.value.attempts == 2


def test_error_fallbacks(serve):
    s = serve(
        (403, {"error": "nope", "reason": "body_too_large"}, {"X-Request-Id": "hdr-1"}),
        (500, "plain failure", {"Content-Type": "text/plain"}),
    )
    t = transport(s.url, max_retries=1)
    with pytest.raises(KoshaRequestError) as a:
        t.request("GET", "v1/namespaces")
    assert a.value.code == "body_too_large"
    assert a.value.request_id == "hdr-1"
    with pytest.raises(KoshaRequestError) as b:
        t.request("GET", "v1/namespaces")
    assert b.value.code is None
    assert b.value.error == "plain failure"
    assert b.value.status_code == 500


def test_idempotency_table():
    assert is_idempotent("GET", "/v1/namespaces/a/stats")
    for action in ("search", "documents", "replace", "delete", "flush", "exists"):
        assert is_idempotent("POST", f"/v1/namespaces/a/{action}")
    assert is_idempotent("DELETE", "/v1/namespaces/a")
    assert not is_idempotent("POST", "/v1/namespaces")
    assert not is_idempotent("POST", "/v1/namespaces/a/schema")
    assert not is_idempotent("POST", "/v1/namespaces/a/branch")
    assert not is_idempotent("DELETE", "/v1/namespaces/a/branch")
    assert not is_idempotent("POST", "/v1/namespaces/a/shards")
    # The deprecated merge route the client falls back to on pre-0.9 servers.
    assert is_idempotent("POST", "/replace")
    assert not is_idempotent("POST", "/index")
    assert not is_idempotent("POST", "/v1/replace")


def test_parse_retry_after():
    assert parse_retry_after("2") == 2000
    now = 1792567680.0  # Wed, 21 Oct 2026 07:28:00 GMT
    assert parse_retry_after("Wed, 21 Oct 2026 07:28:03 GMT", now) == 3000
    assert parse_retry_after("soon") is None
    assert parse_retry_after(None) is None


def test_client_constructor_accepts_every_documented_retry_setting():
    """RETRY.md's Defaults table names these as the Python settings."""
    from kosha_client import KoshaClient

    c = KoshaClient(
        "http://kosha.invalid",
        api_key="k",
        max_retries=5,
        base_delay_ms=1,
        max_delay_ms=2,
        max_retry_after_ms=5,
        total_budget_ms=10,
    )
    t = c._transport
    assert (t.max_retries, t.base_delay_ms, t.max_delay_ms, t.max_retry_after_ms, t.total_budget_ms) == (5, 1, 2, 5, 10)
