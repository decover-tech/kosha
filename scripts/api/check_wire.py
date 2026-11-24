#!/usr/bin/env python3
"""Wire check (#350): real requests and responses against the checked-in spec.

The proto (and so the OpenAPI document generated from it) is only useful if
it describes the JSON the server actually reads and writes. This script
checks that mechanically, in both directions:

  requests   every conformance search body (scripts/conformance/queries.py),
             the corpus documents and one request per public operation must
             validate against that operation's request schema;
  responses  the server's answer to each of those requests must validate
             against the response schema for its status code.

Validation is strict: an object schema with `properties` is treated as
closed (`additionalProperties: false`) here, at validation time only, so a
field the spec does not name is a failure. The published spec stays open
for additive evolution. Free-form objects (google.protobuf.Struct) stay
free-form; `null` is accepted for an optional property, as proto3 JSON
does.

Stdlib only.

  cargo build -p kosha-server
  python3 scripts/api/check_wire.py --spawn target/debug/kosha-server
  python3 scripts/api/check_wire.py --host http://localhost:8080 --api-key ...

Exit status 0 when every check passes, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
SPEC = os.path.join(ROOT, "api", "openapi", "v1", "kosha.openapi.json")
sys.path.insert(0, os.path.join(ROOT, "scripts", "conformance"))

import corpus  # noqa: E402
import queries  # noqa: E402
from oracle import Oracle  # noqa: E402


# ─── schema validation ──────────────────────────────────────────────────────


class Validator:
    def __init__(self, spec: Dict):
        self.spec = spec

    def resolve(self, schema: Dict) -> Dict:
        while "$ref" in schema:
            ref = schema["$ref"]
            if not ref.startswith("#/"):
                raise ValueError("external $ref %s" % ref)
            node: Any = self.spec
            for part in ref[2:].split("/"):
                node = node[part]
            schema = node
        return schema

    def errors(self, value: Any, schema: Dict, path: str = "$") -> List[str]:
        schema = self.resolve(schema)
        out: List[str] = []
        if "allOf" in schema:
            for sub in schema["allOf"]:
                out += self.errors(value, sub, path)
        kind = schema.get("type")
        if "enum" in schema and value not in schema["enum"]:
            out.append("%s: %r is not one of %s" % (path, value, schema["enum"]))
        if kind is None:
            if "properties" in schema and isinstance(value, dict):
                kind = "object"
            else:
                return out  # any JSON value
        if kind == "object":
            if not isinstance(value, dict):
                return out + ["%s: expected an object, got %s" % (path, type(value).__name__)]
            props = schema.get("properties", {})
            for name in schema.get("required", []):
                if name not in value:
                    out.append("%s: missing required %r" % (path, name))
            extra = schema.get("additionalProperties")
            for name, item in value.items():
                where = "%s.%s" % (path, name)
                if name in props:
                    if item is None and name not in schema.get("required", []):
                        continue
                    out += self.errors(item, props[name], where)
                elif isinstance(extra, dict):
                    out += self.errors(item, extra, where)
                elif extra is True or (extra is None and not props):
                    continue  # free-form object
                else:
                    out.append("%s: field not in the spec" % where)
        elif kind == "array":
            if not isinstance(value, list):
                return out + ["%s: expected an array, got %s" % (path, type(value).__name__)]
            for i, item in enumerate(value):
                out += self.errors(item, schema.get("items", {}), "%s[%d]" % (path, i))
        elif kind == "string":
            if not isinstance(value, str):
                out.append("%s: expected a string, got %r" % (path, value))
        elif kind == "integer":
            if isinstance(value, bool) or not isinstance(value, int):
                out.append("%s: expected an integer, got %r" % (path, value))
        elif kind == "number":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                out.append("%s: expected a number, got %r" % (path, value))
        elif kind == "boolean":
            if not isinstance(value, bool):
                out.append("%s: expected a boolean, got %r" % (path, value))
        return out


def operation(spec: Dict, method: str, template: str) -> Dict:
    try:
        return spec["paths"][template][method.lower()]
    except KeyError:
        raise SystemExit("spec has no %s %s" % (method, template))


def request_schema(spec: Dict, method: str, template: str) -> Optional[Dict]:
    body = operation(spec, method, template).get("requestBody")
    if not body:
        return None
    if "$ref" in body:
        body = Validator(spec).resolve(body)
    return body["content"]["application/json"]["schema"]


def response_schema(spec: Dict, method: str, template: str, status: int) -> Optional[Dict]:
    responses = operation(spec, method, template)["responses"]
    resp = responses.get(str(status)) or responses.get("default")
    if resp is None:
        return None
    resp = Validator(spec).resolve(resp)
    return resp.get("content", {}).get("application/json", {}).get("schema")


# ─── server ─────────────────────────────────────────────────────────────────


class Server:
    def __init__(self, host: str, api_key: str, admin_key: str):
        self.host = host.rstrip("/")
        self.api_key = api_key
        self.admin_key = admin_key

    def call(self, method: str, path: str, body: Any = None) -> Tuple[int, Any]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.host + path, data=data, method=method)
        req.add_header("Authorization", "Bearer " + self.api_key)
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw, status = resp.read(), resp.status
        except urllib.error.HTTPError as e:
            raw, status = e.read(), e.code
        try:
            return status, json.loads(raw or b"null")
        except ValueError:
            return status, raw.decode(errors="replace")

    def wait_ready(self, timeout: float = 120) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(self.host + "/v1/readyz", timeout=2) as r:
                    if r.status == 200:
                        return
            except Exception:
                pass
            time.sleep(0.2)
        raise SystemExit("server at %s not ready after %ss" % (self.host, timeout))


def spawn(binary: str, port: int, api_key: str, admin_key: str) -> Tuple[subprocess.Popen, str]:
    workdir = tempfile.mkdtemp(prefix="kosha-wire-")
    env = dict(os.environ)
    env.update(
        {
            "KOSHA_API_KEY": api_key,
            "KOSHA_ADMIN_API_KEY": admin_key,
            "KOSHA_HTTP_PORT": str(port),
            "KOSHA_DATA_DIR": os.path.join(workdir, "data"),
            "KOSHA_FLUSH_THRESHOLD": "1000000",
            "KOSHA_FLUSH_MAX_AGE_MS": "0",
            "KOSHA_COMPACTION_INTERVAL_MS": "0",
        }
    )
    log = open(os.path.join(workdir, "server.log"), "ab")
    proc = subprocess.Popen([os.path.abspath(binary)], env=env, cwd=workdir, stdout=log, stderr=subprocess.STDOUT)
    return proc, workdir


# ─── checks ─────────────────────────────────────────────────────────────────


class Report:
    def __init__(self) -> None:
        self.failures: List[str] = []
        self.passed = 0

    def check(self, label: str, errors: List[str]) -> None:
        if errors:
            self.failures.append("%s\n    %s" % (label, "\n    ".join(errors[:8])))
        else:
            self.passed += 1


def exchange(
    spec: Dict,
    v: Validator,
    server: Server,
    report: Report,
    label: str,
    method: str,
    template: str,
    path: str,
    body: Any = None,
    want: Optional[int] = 200,
    valid_request: bool = True,
) -> Tuple[int, Any]:
    if body is not None and valid_request:
        schema = request_schema(spec, method, template)
        if schema is None:
            report.check("%s request" % label, ["the spec takes no body for %s %s" % (method, template)])
        else:
            report.check("%s request" % label, v.errors(body, schema))
    status, out = server.call(method, path, body)
    if want is not None and status != want:
        report.check("%s status" % label, ["HTTP %s, want %s: %s" % (status, want, str(out)[:300])])
        return status, out
    schema = response_schema(spec, method, template, status)
    if schema is None:
        report.check("%s response %s" % (label, status), ["the spec declares no %s response" % status])
    else:
        report.check("%s response %s" % (label, status), v.errors(out, schema))
    return status, out


def run(spec: Dict, server: Server, seed: int, docs: int) -> Report:
    v = Validator(spec)
    report = Report()
    ns = "wire"
    base = "/v1/namespaces/%s" % ns
    nst = "/v1/namespaces/{namespace}"

    def x(label, method, template, path, body=None, want=200, valid_request=True):
        return exchange(spec, v, server, report, label, method, template, path, body, want, valid_request)

    x("HealthCheck", "GET", "/v1/healthz", "/v1/healthz")
    x(
        "CreateNamespace",
        "POST",
        "/v1/namespaces",
        "/v1/namespaces",
        {
            "namespace": ns,
            "schema": {
                "fields": {
                    "title": {"type": "text", "analyzer": ["standard"]},
                    "status": {"type": "FIELD_TYPE_KEYWORD"},
                }
            },
        },
    )
    corpus_docs = corpus.batch(seed, 0, docs)
    for i in range(0, len(corpus_docs), 500):
        x("IndexDocuments", "POST", nst + "/documents", base + "/documents", {"documents": corpus_docs[i : i + 500]})
    x("Flush", "POST", nst + "/flush", base + "/flush", {})
    first = corpus_docs[0]
    x(
        "ReplaceDocuments",
        "POST",
        nst + "/replace",
        base + "/replace",
        {"documents": [{"id": first["id"], "fields": first["fields"][:1]}]},
    )
    x("Exists", "POST", nst + "/exists", base + "/exists", {"ids": [first["id"], "no-such-doc"]})
    x("ListNamespaces", "GET", "/v1/namespaces", "/v1/namespaces")
    x("GetStats", "GET", nst + "/stats", base + "/stats")
    x("GetNamespaceSchema", "GET", nst + "/schema", base + "/schema")
    title = {"type": "text", "analyzer": ["standard"]}
    x(
        "PutNamespaceSchema",
        "POST",
        nst + "/schema",
        base + "/schema",
        {
            "fields": {
                "title": title,
                "status": {"type": "FIELD_TYPE_KEYWORD"},
                "category": {"type": "FIELD_TYPE_KEYWORD"},
            },
        },
    )
    x(
        "PutNamespaceSchema[reindex required]",
        "POST",
        nst + "/schema",
        base + "/schema",
        {"fields": {"title": title}},
        want=409,
    )

    # Every conformance search, with the keys the conformance runner adds.
    scratch = Oracle()
    scratch.index(corpus_docs)
    scratch.flush()
    bodies = [q["body"] for q in queries.build(scratch, seed)] + [w["body"] for w in queries.walks(scratch, seed)]
    for i, body in enumerate(bodies):
        b = dict(body)
        b.setdefault("query_text", "")
        b.setdefault("exact_total_hits", True)
        b.setdefault("consistency", "eventual")
        b["no_cache"] = True
        x("Search[%d]" % i, "POST", nst + "/search", base + "/search", b)
    # The response fields only some requests produce.
    x(
        "Search[aggs+highlight]",
        "POST",
        nst + "/search",
        base + "/search",
        {
            "query_text": corpus_docs[0]["fields"][0]["value"].split()[0],
            "aggs": {"by_status": {"terms": {"field": "status"}}},
            "highlight": {"fields": {"title": {}}},
            "bm25_params": {"k1": 1.2, "b": 0.75},
            "max_results": 3,
        },
    )

    x("BranchNamespace", "POST", nst + "/branch", base + "/branch", {"name": "wire-branch"})
    x("ListBranches", "GET", nst + "/branches", base + "/branches")
    x("DeleteBranch", "DELETE", nst + "/branch", "/v1/namespaces/wire-branch/branch")
    x("DeleteByQuery", "POST", nst + "/delete", base + "/delete", {"filter": {"term": {"status": "withdrawn"}}})
    x("CreateNamespace[sharded]", "POST", "/v1/namespaces", "/v1/namespaces", {"namespace": "wire-sharded"})
    x("PutShards", "POST", nst + "/shards", "/v1/namespaces/wire-sharded/shards", {"shards": 2})
    x("GetStats[sharded]", "GET", nst + "/stats", "/v1/namespaces/wire-sharded/stats")
    # Off on a default pod: the 404 must still be an ErrorResponse.
    x("GetUsage", "GET", "/v1/usage", "/v1/usage", want=None)
    x("GetBilling", "GET", "/v1/billing", "/v1/billing", want=None)

    # Error responses.
    x("Search[invalid]", "POST", nst + "/search", base + "/search", {"query_text": "x", "max_results": "ten"}, want=400, valid_request=False)
    x("GetStats[missing]", "GET", nst + "/stats", "/v1/namespaces/no-such-ns/stats", want=404)
    x("DeleteNamespace", "DELETE", nst, base)
    return report


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument("--host", help="a running server, e.g. http://localhost:8080")
    target.add_argument("--spawn", metavar="BINARY", help="start this kosha-server binary with a throwaway data dir")
    p.add_argument("--api-key", default=os.environ.get("KOSHA_API_KEY", "sk-wire"))
    p.add_argument("--admin-key", default=os.environ.get("KOSHA_ADMIN_API_KEY", "sk-wire-admin"))
    p.add_argument("--port", type=int, default=18093)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--docs", type=int, default=600)
    p.add_argument("--spec", default=SPEC)
    args = p.parse_args()

    with open(args.spec) as f:
        spec = json.load(f)
    proc, workdir = None, None
    if args.spawn:
        proc, workdir = spawn(args.spawn, args.port, args.api_key, args.admin_key)
        host = "http://127.0.0.1:%d" % args.port
    else:
        host = args.host
    server = Server(host, args.api_key, args.admin_key)
    try:
        server.wait_ready()
        report = run(spec, server, args.seed, args.docs)
    finally:
        if proc:
            proc.terminate()
            proc.wait(timeout=20)
            shutil.rmtree(workdir, ignore_errors=True)
    for failure in report.failures:
        print("FAIL " + failure)
    print("wire check: %d passed, %d failed" % (report.passed, len(report.failures)))
    return 1 if report.failures else 0


if __name__ == "__main__":
    sys.exit(main())
