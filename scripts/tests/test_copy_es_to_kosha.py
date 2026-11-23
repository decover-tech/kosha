"""Smoke test for scripts/copy_es_to_kosha.py against a stub Elasticsearch.

A stdlib HTTP server plays Elasticsearch (_cat/indices, _count, _mapping,
sliced scroll). The copy goes into a fake Kosha that serves only the /v1
routes the script should use, or, when ``KOSHA_TEST_HOST`` (plus
``KOSHA_TEST_API_KEY``) names a running server, into that server too.

Every ``python3 scripts/copy_es_to_kosha.py ...`` command in
docs/migration/from-elasticsearch.md is run as written, so the guide cannot
drift from the script.

    pip install pytest
    python -m pytest scripts/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import shlex
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT = os.path.join(ROOT, "scripts", "copy_es_to_kosha.py")
GUIDE = os.path.join(ROOT, "docs", "migration", "from-elasticsearch.md")


def _load_script():
    spec = importlib.util.spec_from_file_location("copy_es_to_kosha", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


copy = _load_script()


# ─── stub Elasticsearch ─────────────────────────────────────────────────────

PRODUCTS_MAPPING = {
    "properties": {
        "name": {"type": "text", "analyzer": "whitespace"},
        "notes": {"type": "text", "analyzer": "english"},
        "sku": {"type": "keyword"},
        "price": {"type": "double"},
        "stock": {"type": "long"},
        "released": {"type": "date"},
        "active": {"type": "boolean"},
        "tags": {"type": "keyword"},
        "emb": {"type": "dense_vector", "dims": 8},
        "dimensions": {"properties": {"w": {"type": "long"}}},
        "location": {"type": "geo_point"},
    }
}
LOGS_MAPPING = {"properties": {"message": {"type": "text"}, "level": {"type": "keyword"}}}


def _product(i: int) -> dict:
    return {
        "name": "desk lamp model %d" % i,
        "sku": "LMP-%03d" % i,
        "price": 10.5 + i,
        "stock": i * 3,
        "released": 1767225600000 + i * 86_400_000 if i % 2 else "2026-01-%02d" % (i % 28 + 1),
        "active": i % 3 != 0,
        "tags": ["lighting", "desk"] if i % 2 else ["lighting"],
        "emb": [round((i * 7 + d) % 10 / 10, 1) for d in range(8)],
        "dimensions": {"w": i},
        "location": {"lat": 40.0, "lon": -70.0},
    }


def stub_indices() -> dict:
    return {
        "products": {"mapping": PRODUCTS_MAPPING, "docs": {"p%02d" % i: _product(i) for i in range(25)}},
        "logs-2026.01": {"mapping": LOGS_MAPPING, "docs": {"a%d" % i: {"message": "boot %d" % i, "level": "info"} for i in range(7)}},
        "logs-2026.02": {"mapping": LOGS_MAPPING, "docs": {"b%d" % i: {"message": "halt %d" % i, "level": "warn"} for i in range(5)}},
        ".kibana": {"mapping": {}, "docs": {}},
    }


class StubES(BaseHTTPRequestHandler):
    indices: dict = {}
    scrolls: dict = {}
    lock = threading.Lock()

    def log_message(self, *args):  # quiet
        pass

    def _send(self, status: int, body) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}") if n else {}

    def _index(self, name: str):
        name = urllib.parse.unquote(name)
        if name not in self.indices:
            self._send(404, {"error": {"type": "index_not_found_exception"}, "status": 404})
            return None, None
        return name, self.indices[name]

    def do_GET(self):  # noqa: N802
        path = urllib.parse.urlsplit(self.path).path
        if path.startswith("/_cat/indices/"):
            pattern = urllib.parse.unquote(path[len("/_cat/indices/"):])
            regex = re.compile("^" + re.escape(pattern).replace("\\*", ".*") + "$")
            self._send(200, [{"index": n} for n in self.indices if regex.match(n)])
            return
        m = re.fullmatch(r"/([^/]+)/_mapping", path)
        if m:
            name, idx = self._index(m.group(1))
            if idx is not None:
                self._send(200, {name: {"mappings": idx["mapping"]}})
            return
        self._send(404, {"error": "no route GET %s" % path})

    def do_POST(self):  # noqa: N802
        parsed = urllib.parse.urlsplit(self.path)
        path, body = parsed.path, self._body()
        m = re.fullmatch(r"/([^/]+)/_count", path)
        if m:
            _, idx = self._index(m.group(1))
            if idx is None:
                return
            docs = idx["docs"].values()
            field = ((body.get("query") or {}).get("exists") or {}).get("field")
            self._send(200, {"count": sum(1 for d in docs if field is None or field in d)})
            return
        m = re.fullmatch(r"/([^/]+)/_search", path)
        if m:
            name, idx = self._index(m.group(1))
            if idx is None:
                return
            ids = sorted(idx["docs"])
            sl = body.get("slice")
            if sl:
                ids = [i for i in ids if zlib.crc32(i.encode()) % sl["max"] == sl["id"]]
            hits = [{"_index": name, "_id": i, "_source": idx["docs"][i]} for i in ids]
            with self.lock:
                sid = "scroll-%d" % (len(self.scrolls) + 1)
                size = int(body.get("size", 10))
                self.scrolls[sid] = (hits[size:], size)
            self._send(200, {"_scroll_id": sid, "hits": {"total": {"value": len(hits), "relation": "eq"}, "hits": hits[:size]}})
            return
        if path == "/_search/scroll":
            sid = body["scroll_id"]
            with self.lock:
                rest, size = self.scrolls.get(sid, ([], 10))
                self.scrolls[sid] = (rest[size:], size)
            self._send(200, {"_scroll_id": sid, "hits": {"hits": rest[:size]}})
            return
        self._send(404, {"error": "no route POST %s" % path})

    def do_DELETE(self):  # noqa: N802
        if urllib.parse.urlsplit(self.path).path == "/_search/scroll":
            self._send(200, {"succeeded": True})
            return
        self._send(404, {"error": "no route"})


# ─── fake Kosha: the /v1 routes only ────────────────────────────────────────


class FakeKosha(BaseHTTPRequestHandler):
    namespaces: dict = {}
    paths: list = []
    drop_one_from: set = set()
    lock = threading.Lock()
    api_key = "sk-test"

    def log_message(self, *args):
        pass

    def _send(self, status: int, body) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):  # noqa: N802
        path = urllib.parse.urlsplit(self.path).path
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        with self.lock:
            self.paths.append(path)
        if self.headers.get("Authorization") != "Bearer " + self.api_key:
            self._send(401, {"error": "bad key"})
            return
        if path == "/v1/namespaces":
            with self.lock:
                if body["namespace"] in self.namespaces:
                    self._send(409, {"error": "namespace '%s' already exists" % body["namespace"]})
                    return
                self.namespaces[body["namespace"]] = {"schema": body.get("schema"), "docs": {}, "flushes": 0}
            self._send(200, {"namespace": body["namespace"], "created": True})
            return
        m = re.fullmatch(r"/v1/namespaces/([^/]+)/(documents|flush|search)", path)
        if not m:
            self._send(410, {"error": "legacy or unknown route %s" % path})
            return
        ns, action = urllib.parse.unquote(m.group(1)), m.group(2)
        with self.lock:
            entry = self.namespaces.setdefault(ns, {"schema": None, "docs": {}, "flushes": 0})
            if action == "documents":
                for doc in body["documents"]:
                    entry["docs"][doc["id"]] = doc["fields"]
                if ns in self.drop_one_from and entry["docs"]:
                    entry["docs"].pop(sorted(entry["docs"])[0])
                self._send(200, {"indexed_count": len(body["documents"])})
            elif action == "flush":
                entry["flushes"] += 1
                self._send(200, {"status": "flushed"})
            else:
                assert body.get("filter") == {"match_all": {}} and body.get("max_results") == 0
                self._send(200, {"results": [], "total_hits": len(entry["docs"])})


def _serve(handler):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()
    return server, "http://127.0.0.1:%d" % server.server_address[1]


@pytest.fixture
def es():
    StubES.indices = stub_indices()
    StubES.scrolls = {}
    server, url = _serve(StubES)
    yield url
    server.shutdown()


@pytest.fixture
def kosha():
    FakeKosha.namespaces = {}
    FakeKosha.paths = []
    FakeKosha.drop_one_from = set()
    server, url = _serve(FakeKosha)
    yield url
    server.shutdown()


@pytest.fixture
def env(es, kosha, monkeypatch):
    monkeypatch.setenv("ELASTICSEARCH_HOST", es)
    monkeypatch.setenv("KOSHA_HOST", kosha)
    monkeypatch.setenv("KOSHA_API_KEY", FakeKosha.api_key)
    for name in ("ELASTICSEARCH_USERNAME", "ELASTICSEARCH_PASSWORD", "ELASTICSEARCH_AUTH_MODE"):
        monkeypatch.delenv(name, raising=False)
    return {"es": es, "kosha": kosha}


def by_name(fields: list) -> dict:
    out: dict = {}
    for f in fields:
        out.setdefault(f["name"], []).append((f["field_type"], f["value"]))
    return out


# ─── the guide's commands ───────────────────────────────────────────────────


def guide_commands() -> list:
    """Every `python3 scripts/copy_es_to_kosha.py ...` line in the guide."""
    with open(GUIDE, encoding="utf-8") as f:
        text = f.read()
    commands = []
    for block in re.findall(r"```bash\n(.*?)```", text, flags=re.S):
        for line in block.replace("\\\n", " ").splitlines():
            line = line.strip()
            if line.startswith("python3 scripts/copy_es_to_kosha.py"):
                commands.append(line)
    return commands


def test_the_guide_has_copy_commands():
    commands = guide_commands()
    assert any("--dry-run" in c for c in commands)
    assert any("--create-schema-from-mapping" in c for c in commands)
    assert any("--pattern" in c for c in commands)


@pytest.mark.parametrize("command", guide_commands())
def test_guide_command_runs(env, command):
    argv = shlex.split(command)
    assert argv[:2] == ["python3", "scripts/copy_es_to_kosha.py"]
    proc = subprocess.run(
        [sys.executable, *argv[1:]], cwd=ROOT, env=dict(os.environ), capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == 0, proc.stderr + proc.stdout
    if "--dry-run" in argv:
        assert FakeKosha.paths == []
        return
    namespace = argv[argv.index("--namespace") + 1] if "--namespace" in argv else None
    for index, data in StubES.indices.items():
        if index.startswith("."):
            assert index not in FakeKosha.namespaces  # system indices skipped
            continue
        selected = ("--index" in argv and index == argv[argv.index("--index") + 1]) or (
            "--pattern" in argv and index.startswith(argv[argv.index("--pattern") + 1].rstrip("*"))
        )
        if not selected:
            continue
        stored = FakeKosha.namespaces[namespace or index]
        want = len(data["docs"])
        if "--limit" in argv:
            want = min(want, int(argv[argv.index("--limit") + 1]))
        assert len(stored["docs"]) == want
        assert stored["flushes"] >= 1
        if "--create-schema-from-mapping" in argv:
            assert stored["schema"] is not None
    assert all(p.startswith("/v1/") for p in FakeKosha.paths), FakeKosha.paths


# ─── behaviour ──────────────────────────────────────────────────────────────


def test_schema_and_types_from_the_mapping(env):
    assert copy.main(["--index", "products", "--create-schema-from-mapping"]) == 0
    stored = FakeKosha.namespaces["products"]
    fields = stored["schema"]["fields"]
    assert fields["sku"] == {"type": "FIELD_TYPE_KEYWORD", "indexed": True, "stored": True}
    assert fields["stock"]["type"] == "FIELD_TYPE_INTEGER"
    assert fields["released"]["type"] == "FIELD_TYPE_DATE"
    assert fields["emb"]["type"] == "FIELD_TYPE_VECTOR"
    assert fields["name"]["analyzer"] == ["whitespace"]
    assert "analyzer" not in fields["notes"]  # no Kosha "english": the default
    assert "dimensions" not in fields and "location" not in fields
    assert len(stored["docs"]) == 25

    doc = by_name(stored["docs"]["p03"])
    assert doc["sku"] == [("Keyword", "LMP-003")]
    assert doc["stock"] == [("Integer", "9")]
    assert doc["price"] == [("Float", "13.5")]
    assert doc["active"] == [("Boolean", "false")]
    assert doc["name"] == [("Text", "desk lamp model 3")]
    # epoch_millis -> ISO-8601 UTC
    assert doc["released"] == [("Date", "2026-01-04T00:00:00Z")]
    # a keyword list is multi-valued
    assert doc["tags"] == [("Keyword", "lighting"), ("Keyword", "desk")]
    assert doc["emb"][0][0] == "Vector"
    # unmapped in the schema: typed from the JSON shape
    assert doc["dimensions"] == [("Keyword", json.dumps({"w": 3}))]
    assert by_name(stored["docs"]["p02"])["released"] == [("Date", "2026-01-03")]


def test_without_a_mapping_values_are_typed_like_the_client(env):
    sys.path.insert(0, os.path.join(ROOT, "clients", "python"))
    try:
        from kosha_client import KoshaClient
    except ImportError:  # the client needs Python 3.10+
        pytest.skip("kosha_client not importable")
    finally:
        sys.path.pop(0)
    source = {k: v for k, v in _product(3).items()}
    assert copy.source_to_fields(source) == KoshaClient._source_to_fields(source)

    assert copy.main(["--index", "products"]) == 0
    stored = FakeKosha.namespaces["products"]
    assert stored["schema"] is None
    assert by_name(stored["docs"]["p03"])["sku"] == [("Text", "LMP-003")]


def test_field_type_table_matches_the_client():
    sys.path.insert(0, os.path.join(ROOT, "clients", "python"))
    try:
        from kosha_client.client import IndexOps
    except ImportError:
        pytest.skip("kosha_client not importable")
    finally:
        sys.path.pop(0)
    assert copy.FIELD_TYPES_FROM_ES == IndexOps._FIELD_TYPES_FROM_ES


def test_existing_namespace_keeps_its_schema(env):
    FakeKosha.namespaces["products"] = {"schema": {"fields": {}}, "docs": {}, "flushes": 0}
    assert copy.main(["--index", "products", "--create-schema-from-mapping"]) == 0
    stored = FakeKosha.namespaces["products"]
    assert stored["schema"] == {"fields": {}}
    assert by_name(stored["docs"]["p01"])["sku"] == [("Keyword", "LMP-001")]


@pytest.mark.parametrize("workers", [1, 3])
def test_workers_and_limit(env, workers):
    assert copy.main(["--index", "products", "--workers", str(workers), "--batch-size", "4"]) == 0
    assert len(FakeKosha.namespaces["products"]["docs"]) == 25
    FakeKosha.namespaces.clear()
    assert copy.main(["--index", "products", "--workers", str(workers), "--limit", "10", "--batch-size", "3"]) == 0
    assert len(FakeKosha.namespaces["products"]["docs"]) == 10


def test_a_count_mismatch_fails(env):
    FakeKosha.drop_one_from = {"products"}
    assert copy.main(["--index", "products"]) == 1
    assert copy.main(["--index", "products", "--no-verify"]) == 0


def test_a_rerun_after_deletes_in_es_needs_allow_extra(env):
    assert copy.main(["--index", "products"]) == 0
    StubES.indices["products"]["docs"].pop("p00")
    # The re-run upserts and never deletes, so Kosha holds one more document.
    assert copy.main(["--index", "products"]) == 1
    assert copy.main(["--index", "products", "--allow-extra"]) == 0
    # Fewer documents in Kosha than in Elasticsearch still fails.
    FakeKosha.namespaces.clear()
    FakeKosha.drop_one_from = {"products"}
    assert copy.main(["--index", "products", "--allow-extra"]) == 1


def test_an_index_or_pattern_is_required(env):
    with pytest.raises(SystemExit) as e:
        copy.main([])
    assert e.value.code == 2


def test_namespace_needs_a_single_index(env):
    with pytest.raises(SystemExit):
        copy.main(["--pattern", "logs-*", "--namespace", "x"])


def test_kosha_retries_load_shedding(monkeypatch):
    calls = []

    class Flaky(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):  # noqa: N802
            self.rfile.read(int(self.headers.get("Content-Length") or 0))
            calls.append(self.path)
            status, body = (429, b'{"error":"busy"}') if len(calls) < 3 else (200, b'{"indexed_count":1}')
            self.send_response(status)
            if status == 429:
                self.send_header("Retry-After", "0")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server, url = _serve(Flaky)
    try:
        writer = copy.KoshaWriter(url, None)
        assert writer.index_batch("ns", [{"id": "1", "fields": []}]) == 1
        assert len(calls) == 3
    finally:
        server.shutdown()


# ─── against a real server ──────────────────────────────────────────────────


@pytest.mark.skipif(not os.environ.get("KOSHA_TEST_HOST"), reason="set KOSHA_TEST_HOST to copy into a running Kosha")
def test_copy_into_a_live_server(es, monkeypatch):
    host = os.environ["KOSHA_TEST_HOST"].rstrip("/")
    key = os.environ.get("KOSHA_TEST_API_KEY", "")
    namespace = "copy-smoke-%d" % int(time.time() * 1000)
    monkeypatch.setenv("KOSHA_API_KEY", key)
    argv = ["--es-host", es, "--kosha-host", host, "--index", "products", "--namespace", namespace,
            "--create-schema-from-mapping", "--workers", "2"]
    assert copy.main(argv) == 0

    def call(method, path, body=None):
        req = urllib.request.Request(host + path, data=json.dumps(body).encode() if body is not None else None, method=method)
        req.add_header("Content-Type", "application/json")
        if key:
            req.add_header("Authorization", "Bearer " + key)
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read() or b"null")

    try:
        schema = call("GET", "/v1/namespaces/%s/schema" % namespace)
        schema = schema.get("schema", schema)
        assert schema["fields"]["sku"]["type"] == "FIELD_TYPE_KEYWORD"
        # A keyword stays whole: an exact term filter finds the one document.
        out = call("POST", "/v1/namespaces/%s/search" % namespace,
                   {"query_text": "", "filter": {"term": {"sku": "LMP-007"}}, "exact_total_hits": True})
        assert [h["doc_id"] for h in out["results"]] == ["p07"]
        multi = call("POST", "/v1/namespaces/%s/search" % namespace,
                     {"query_text": "", "filter": {"term": {"tags": "desk"}}, "exact_total_hits": True})
        assert multi["total_hits"] == 12
        assert copy.KoshaWriter(host, key or None).count(namespace) == 25
    finally:
        try:
            call("DELETE", "/v1/namespaces/%s" % namespace)
        except urllib.error.HTTPError:
            pass
