"""The transport retries what the server asks it to retry, and nothing else.

The server answers 429 for work it refused before starting (tenant limits,
write backpressure, a search shed under load) and 503 for transient
failures, both on the premise that the caller retries. These tests run the
real urllib transport against a local HTTP server.
"""

import json
import threading
import time
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from kosha_client import KoshaClient, KoshaRequestError
from kosha_client.transport import Transport, _retry_after_secs


class Script:
    """Answers each request with the next scripted (status, headers, body)."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls = []

    def next(self, method, path):
        self.calls.append((method, path, time.monotonic()))
        if len(self.answers) > 1:
            return self.answers.pop(0)
        return self.answers[0]


@pytest.fixture(scope="module")
def server():
    holder = {}

    class Handler(BaseHTTPRequestHandler):
        def _answer(self):
            length = int(self.headers.get("content-length") or 0)
            self.rfile.read(length)
            status, headers, body = holder["script"].next(self.command, self.path)
            data = json.dumps(body).encode()
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST = do_DELETE = _answer

        def log_message(self, *args):  # quiet
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    def run(script, **kwargs):
        holder["script"] = script
        kwargs.setdefault("max_retries", 3)
        return Transport(f"http://127.0.0.1:{httpd.server_port}", **kwargs)

    yield run
    httpd.shutdown()


OK = (200, {}, {"results": [], "total_hits": 0})
SHED = (429, {"retry-after": "0"}, {"error": "search overloaded", "reason": "x"})


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch):
    monkeypatch.setattr(Transport, "backoff_base", 0.01)
    monkeypatch.setattr(Transport, "backoff_cap", 0.02)


def test_429_then_success(server):
    script = Script(SHED, OK)
    assert server(script).request("POST", "search", {"q": 1}) == OK[2]
    assert len(script.calls) == 2


def test_429_on_a_write_is_retried(server):
    # A 429 is a refusal before any work: retrying a write cannot apply it twice.
    script = Script(SHED, (200, {}, {"indexed_count": 1}))
    assert server(script).request("POST", "index", {"documents": []}) == {
        "indexed_count": 1
    }
    assert len(script.calls) == 2


def test_honors_retry_after_seconds(server):
    script = Script((429, {"retry-after": "1"}, {"error": "shed"}), OK)
    server(script).request("POST", "search", {})
    (_, _, first), (_, _, second) = script.calls
    assert second - first >= 1.0


def test_honors_retry_after_http_date(server):
    when = formatdate(time.time() + 2, usegmt=True)
    script = Script((503, {"retry-after": when}, {"error": "warming"}), OK)
    server(script).request("GET", "v1/namespaces/ns/stats")
    (_, _, first), (_, _, second) = script.calls
    # The date has one-second resolution: at least ~1 s, never the full budget.
    assert 0.9 <= second - first < 3.0


def test_gives_up_after_max_attempts_with_last_error(server):
    script = Script(
        (429, {"retry-after": "0"}, {"error": "first"}),
        (429, {"retry-after": "0"}, {"error": "second"}),
        (503, {}, {"error": "last"}),
    )
    with pytest.raises(KoshaRequestError) as raised:
        server(script, max_retries=3).request("POST", "search", {})
    assert raised.value.status_code == 503
    assert raised.value.error == "last"
    assert len(script.calls) == 3


@pytest.mark.parametrize("status", [400, 401, 402, 403, 404, 409, 413, 500])
def test_does_not_retry_other_statuses(server, status):
    script = Script((status, {"retry-after": "0"}, {"error": "no"}), OK)
    with pytest.raises(KoshaRequestError) as raised:
        server(script).request("POST", "search", {})
    assert raised.value.status_code == status
    assert len(script.calls) == 1


@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", "index"),
        ("POST", "replace"),
        ("POST", "delete"),
        ("POST", "flush"),
        ("POST", "v1/namespaces"),
        ("DELETE", "v1/namespaces/ns"),
    ],
)
@pytest.mark.parametrize("status", [502, 503, 504])
def test_does_not_retry_5xx_writes(server, method, path, status):
    # The write may have reached the ingest pod before the 503: no retry.
    script = Script((status, {"retry-after": "0"}, {"error": "x"}), OK)
    with pytest.raises(KoshaRequestError) as raised:
        server(script).request(method, path, {})
    assert raised.value.status_code == status
    assert len(script.calls) == 1


@pytest.mark.parametrize(
    "method,path",
    [("POST", "search"), ("POST", "v1/namespaces/ns/search"), ("GET", "healthz")],
)
@pytest.mark.parametrize("status", [502, 503, 504])
def test_retries_5xx_reads(server, method, path, status):
    script = Script((status, {}, {"error": "x"}), OK)
    server(script).request(method, path, {})
    assert len(script.calls) == 2


def test_retry_after_beyond_budget_raises_at_once(server):
    # A quota refusal says 60 s: not worth holding the caller for.
    script = Script((429, {"retry-after": "60"}, {"error": "quota"}), OK)
    start = time.monotonic()
    with pytest.raises(KoshaRequestError) as raised:
        server(script, retry_budget=20).request("POST", "search", {})
    assert raised.value.status_code == 429
    assert len(script.calls) == 1
    assert time.monotonic() - start < 1.0


def test_total_time_bounded_by_budget(server):
    # Each retry waits 1 s; a 1.5 s budget admits one retry, not nine.
    script = Script((429, {"retry-after": "1"}, {"error": "shed"}))
    start = time.monotonic()
    with pytest.raises(KoshaRequestError):
        server(script, max_retries=10, retry_budget=1.5).request("POST", "search", {})
    assert len(script.calls) == 2
    assert time.monotonic() - start < 2.5


def test_budget_zero_disables_status_retries(server):
    script = Script(SHED, OK)
    with pytest.raises(KoshaRequestError):
        server(script, retry_budget=0).request("POST", "search", {})
    assert len(script.calls) == 1


def test_client_search_retries_a_shed(server, monkeypatch):
    script = Script(SHED, OK)
    transport = server(script)
    client = KoshaClient(hosts=transport.base_url)
    assert client._transport.retry_budget == 20.0
    assert client.search(index="ns", body={"query": {"match_all": {}}})["hits"][
        "hits"
    ] == []
    assert [c[:2] for c in script.calls] == [("POST", "/search")] * 2


@pytest.mark.parametrize(
    "value,expected",
    [(None, None), ("", None), ("3", 3.0), ("-1", 0.0), ("junk", None)],
)
def test_retry_after_parsing(value, expected):
    assert _retry_after_secs(value) == expected


def test_retry_after_date_in_the_past_is_zero():
    assert _retry_after_secs("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0
