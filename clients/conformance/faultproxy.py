#!/usr/bin/env python3
"""A fault-injecting reverse proxy for the SDK conformance suite (stdlib only).

    python3 clients/conformance/faultproxy.py \
        --upstream http://localhost:8081 --listen 18081 --control 18091

SDKs under test talk to :18081, which forwards to the Kosha query pod.
Each request carries ``X-Conformance-Case: <case key>#<step>`` (the proxy
strips it before forwarding); ``<step>`` is the step index, ``setup`` or
``cleanup``. Faults apply only to numbered steps.

Control API (on --control):

    POST   /faults        {"case": "<case key>", "rules": [<fault>, ...]}
    DELETE /faults?case=<case key>
    GET    /log?case=<case key>   {"requests": [{"step", "method", "path", "status"}]}
    GET    /healthz

A fault (case.schema.json) is ``{"match": {"method", "path_suffix", "step"},
"times": n, "respond" | "delay_ms" | "inject_field": ...}``. A canned error
body is checked against the spec's ErrorResponse when installed, so a case
cannot rely on a field the server never sends.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

TAG = "X-Conformance-Case"
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "content-length", "host",
}
ERROR_TYPES = {
    "error": str, "code": str, "reason": str, "request_id": str,
    "retryable": bool, "retry_after_ms": int, "details": dict, "limit": (int, float),
}


class State:
    def __init__(self, error_fields: set[str]):
        self.lock = threading.Lock()
        self.rules: dict[str, list[dict]] = {}
        self.log: list[dict] = []
        self.error_fields = error_fields

    def install(self, case: str, rules: list[dict]) -> None:
        for rule in rules:
            self.validate(rule)
        with self.lock:
            self.rules[case] = [dict(r, _left=r.get("times")) for r in rules]

    def validate(self, rule: dict) -> None:
        actions = [k for k in ("respond", "delay_ms", "inject_field") if k in rule]
        if len(actions) != 1:
            raise ValueError("a fault needs exactly one of respond, delay_ms, inject_field")
        respond = rule.get("respond")
        if respond and respond["status"] >= 400 and isinstance(respond.get("body"), dict):
            for key, value in respond["body"].items():
                if key not in self.error_fields:
                    raise ValueError(f"canned error field {key!r} is not in the spec's ErrorResponse")
                want = ERROR_TYPES.get(key)
                if want and (not isinstance(value, want) or (want is int and isinstance(value, bool))):
                    raise ValueError(f"canned error field {key!r} has the wrong type")

    def take(self, case: str, step: str, method: str, path: str) -> dict | None:
        if not step.isdigit():
            return None
        with self.lock:
            for rule in self.rules.get(case, []):
                m = rule.get("match", {})
                if m.get("method") and m["method"].upper() != method:
                    continue
                if m.get("path_suffix") and not path.split("?", 1)[0].endswith(m["path_suffix"]):
                    continue
                if "step" in m and int(step) != m["step"]:
                    continue
                if rule["_left"] is not None:
                    if rule["_left"] <= 0:
                        continue
                    rule["_left"] -= 1
                return rule
        return None

    def record(self, case: str, step: str, method: str, path: str) -> dict:
        """Logs a request on arrival (status 0 until it is answered), so a
        request the client abandoned (a timeout) still counts as an attempt."""
        row = {"case": case, "step": step, "method": method, "path": path, "status": 0, "at": time.time()}
        with self.lock:
            self.log.append(row)
        return row


def make_proxy(state: State, upstream: urllib.parse.SplitResult):
    class Proxy(BaseHTTPRequestHandler):
        def _handle(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else None
            tag = self.headers.get(TAG, "")
            case, _, step = tag.partition("#")
            rule = state.take(case, step, self.command, self.path) if case else None
            row = state.record(case, step, self.command, self.path) if case else {}
            status = 0
            try:
                if rule and "respond" in rule:
                    r = rule["respond"]
                    payload = b"" if r.get("body") is None else json.dumps(r["body"]).encode()
                    status = r["status"]
                    self._send(status, {"Content-Type": "application/json", **r.get("headers", {})}, payload)
                    return
                if rule and "delay_ms" in rule:
                    time.sleep(rule["delay_ms"] / 1000.0)
                status, headers, payload = self._forward(body)
                if rule and "inject_field" in rule:
                    try:
                        doc = json.loads(payload)
                        if isinstance(doc, dict):
                            doc.update(rule["inject_field"])
                            payload = json.dumps(doc).encode()
                    except ValueError:
                        pass
                self._send(status, headers, payload)
            except (BrokenPipeError, ConnectionResetError):
                pass  # the client gave up (a timeout case)
            finally:
                with state.lock:
                    row["status"] = status

        def _forward(self, body: bytes | None) -> tuple[int, dict, bytes]:
            conn_cls = http.client.HTTPSConnection if upstream.scheme == "https" else http.client.HTTPConnection
            conn = conn_cls(upstream.hostname, upstream.port, timeout=120)
            headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP and k != TAG}
            if body is not None:
                headers["Content-Length"] = str(len(body))
            try:
                conn.request(self.command, self.path, body=body, headers=headers)
                resp = conn.getresponse()
                payload = resp.read()
                out = {k: v for k, v in resp.getheaders() if k.lower() not in HOP_BY_HOP}
                return resp.status, out, payload
            finally:
                conn.close()

        def _send(self, status: int, headers: dict, payload: bytes) -> None:
            self.send_response(status)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = _handle

        def log_message(self, fmt: str, *args: Any) -> None:
            if os.environ.get("FAULTPROXY_VERBOSE"):
                sys.stderr.write("proxy: " + fmt % args + "\n")

    return Proxy


def make_control(state: State):
    class Control(BaseHTTPRequestHandler):
        def _json(self, status: int, body: Any) -> None:
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _query(self) -> tuple[str, dict]:
            url = urllib.parse.urlsplit(self.path)
            return url.path, {k: v[0] for k, v in urllib.parse.parse_qs(url.query).items()}

        def do_GET(self) -> None:
            path, q = self._query()
            if path == "/healthz":
                return self._json(200, {"status": "ok"})
            if path == "/log":
                case = q.get("case", "")
                with state.lock:
                    rows = [
                        {k: r[k] for k in ("step", "method", "path", "status")}
                        for r in state.log
                        if r["case"] == case
                    ]
                return self._json(200, {"requests": rows})
            self._json(404, {"error": "not found"})

        def do_POST(self) -> None:
            path, _ = self._query()
            if path != "/faults":
                return self._json(404, {"error": "not found"})
            try:
                req = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
                state.install(req["case"], req.get("rules", []))
            except (ValueError, KeyError, TypeError) as e:
                return self._json(400, {"error": str(e)})
            self._json(200, {"installed": len(req.get("rules", []))})

        def do_DELETE(self) -> None:
            path, q = self._query()
            if path != "/faults":
                return self._json(404, {"error": "not found"})
            with state.lock:
                state.rules.pop(q.get("case", ""), None)
            self._json(200, {"deleted": True})

        def log_message(self, fmt: str, *args: Any) -> None:
            pass

    return Control


def error_fields(spec_path: str) -> set[str]:
    with open(spec_path, encoding="utf-8") as f:
        spec = json.load(f)
    schemas = spec["components"]["schemas"]
    name = next(n for n in schemas if n.endswith("ErrorResponse"))
    return set(schemas[name]["properties"])


def main() -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--upstream", default="http://localhost:8081")
    p.add_argument("--listen", type=int, default=18081)
    p.add_argument("--control", type=int, default=18091)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--spec", default=os.path.join(here, "..", "..", "api", "openapi", "v1", "kosha.openapi.json"))
    args = p.parse_args()

    state = State(error_fields(args.spec))
    upstream = urllib.parse.urlsplit(args.upstream)
    proxy = ThreadingHTTPServer((args.host, args.listen), make_proxy(state, upstream))
    control = ThreadingHTTPServer((args.host, args.control), make_control(state))
    proxy.daemon_threads = control.daemon_threads = True
    threading.Thread(target=control.serve_forever, daemon=True).start()
    print(f"faultproxy: :{args.listen} -> {args.upstream}, control on :{args.control}", flush=True)
    try:
        proxy.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
