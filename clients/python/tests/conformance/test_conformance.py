"""The shared SDK conformance suite (clients/conformance), Python runner.

Skipped unless KOSHA_CONFORMANCE_URL is set; see clients/conformance/README.md.
The TypeScript and Go runners implement the same ops and matchers.
"""

from __future__ import annotations

import glob
import json
import os
import re
import time
import urllib.parse
import urllib.request
from typing import Any, Callable

import pytest

from kosha_client import BulkError, KoshaClient, KoshaConfigError, KoshaRequestError
from kosha_client.native import ns_path

ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "..", "conformance")
LANG = "py"
URL = os.environ.get("KOSHA_CONFORMANCE_URL")
CONTROL = os.environ.get("KOSHA_CONFORMANCE_CONTROL")
RUN_ID = os.environ.get("KOSHA_CONFORMANCE_RUN_ID") or format(int(time.time() * 1000) % 36**6, "x")
KEYS = {
    "default": os.environ.get("KOSHA_CONFORMANCE_API_KEY", "sk-kosha-dev"),
    "small": os.environ.get("KOSHA_CONFORMANCE_SMALL_KEY", "sk-conformance-small"),
    "limited": os.environ.get("KOSHA_CONFORMANCE_LIMITED_KEY", "sk-conformance-limited"),
    "invalid": "sk-conformance-invalid",
}
BIG = "x" * 5000


def load_cases() -> list[dict]:
    cases = []
    for path in sorted(glob.glob(os.path.join(ROOT, "cases", "*.json"))):
        with open(path, encoding="utf-8") as f:
            cases.extend(json.load(f))
    return cases


def load_schema() -> dict:
    with open(os.path.join(ROOT, "case.schema.json"), encoding="utf-8") as f:
        return json.load(f)["$defs"]


# ── Generators and placeholders ───────────────────────────────────────────

_TEMPLATE = re.compile(r"\{i(?:%(\d+))?(?::0(\d+))?\}")


def render(template: str, i: int) -> str:
    def one(m: re.Match) -> str:
        n = i % int(m.group(1)) if m.group(1) else i
        return str(n).zfill(int(m.group(2))) if m.group(2) else str(n)

    return _TEMPLATE.sub(one, template)


def generate(spec: dict) -> list[dict]:
    return [
        {
            "id": render(spec["id"], i),
            "fields": [
                {"name": name, "field_type": f["type"], "value": render(f["value"], i)}
                for name, f in (spec.get("fields") or {}).items()
            ],
        }
        for i in range(spec["count"])
    ]


def substitute(value: Any, ns: str) -> Any:
    if isinstance(value, str):
        return value.replace("$NS", ns).replace("$BIG", BIG)
    if isinstance(value, list):
        return [substitute(v, ns) for v in value]
    if isinstance(value, dict):
        return {k: substitute(v, ns) for k, v in value.items()}
    return value


def documents(value: Any) -> list[dict]:
    return generate(value["generate"]) if isinstance(value, dict) and "generate" in value else value


# ── Ops: each returns {ids, total_hits, count, indexed, batches, failed_ids} ─


def _ids(result: dict) -> dict:
    ids = result.get("ids")
    if ids is not None:
        result.setdefault("count", len(ids))
    return result


def op_create_namespace(c: KoshaClient, ns: str, a: dict) -> dict:
    body = {"namespace": ns}
    if "schema" in a:
        body["schema"] = a["schema"]
    c._transport.request("POST", "v1/namespaces", body)
    return {}


def op_list_namespaces(c: KoshaClient, ns: str, a: dict) -> dict:
    out = c._transport.request("GET", "v1/namespaces")
    return _ids({"ids": [n.get("name") for n in out.get("namespaces") or []]})


def op_delete_namespace(c: KoshaClient, ns: str, a: dict) -> dict:
    c._transport.request("DELETE", ns_path(ns))
    return {}


def op_put_schema(c: KoshaClient, ns: str, a: dict) -> dict:
    c._transport.request("POST", ns_path(ns, "schema"), a["schema"])
    return {}


def op_get_schema(c: KoshaClient, ns: str, a: dict) -> dict:
    out = c._transport.request("GET", ns_path(ns, "schema"))
    fields = ((out.get("schema") or {}).get("fields") or {}).keys()
    return _ids({"ids": sorted(fields)})


def op_upsert(c: KoshaClient, ns: str, a: dict) -> dict:
    out = c._transport.request("POST", ns_path(ns, "documents"), {"documents": documents(a["documents"])})
    return {"indexed": out.get("indexed_count")}


def op_merge(c: KoshaClient, ns: str, a: dict) -> dict:
    out = c._transport.request("POST", ns_path(ns, "replace"), {"documents": documents(a["documents"])})
    return {"indexed": out.get("indexed_count")}


def op_exists(c: KoshaClient, ns: str, a: dict) -> dict:
    out = c._transport.request("POST", ns_path(ns, "exists"), {"ids": a["ids"]})
    return _ids({"ids": list(out.get("existing") or [])})


def op_search(c: KoshaClient, ns: str, a: dict) -> dict:
    out = c._transport.request("POST", ns_path(ns, "search"), a["body"])
    return _ids({"ids": [h.get("doc_id") for h in out.get("results") or []], "total_hits": out.get("total_hits")})


def op_search_all(c: KoshaClient, ns: str, a: dict) -> dict:
    return _ids({"ids": [h.get("doc_id") for h in c.search_all(ns, a["body"], page_size=a.get("page_size"))]})


def op_bulk_index(c: KoshaClient, ns: str, a: dict) -> dict:
    opts = {k: a[k] for k in ("max_docs_per_batch", "max_bytes_per_batch", "concurrency", "on_error") if k in a}
    out = c.bulk_index(ns, documents(a["documents"]), **opts)
    return bulk_result(out)


def bulk_result(out: dict) -> dict:
    return {
        "indexed": out["indexed"],
        "batches": out["batches"],
        "failed_ids": [i for f in out["failures"] for i in f["ids"]],
    }


def op_delete_by_query(c: KoshaClient, ns: str, a: dict) -> dict:
    out = c._transport.request("POST", ns_path(ns, "delete"), a["body"])
    return {"count": out.get("deleted")}


def op_flush(c: KoshaClient, ns: str, a: dict) -> dict:
    c._transport.request("POST", ns_path(ns, "flush"), {})
    return {}


def op_stats(c: KoshaClient, ns: str, a: dict) -> dict:
    out = c._transport.request("GET", ns_path(ns, "stats"))
    return {"count": out.get("documents")}


def op_raw_request(c: KoshaClient, ns: str, a: dict) -> dict:
    c._transport.request(a["method"], a["path"], a.get("body"))
    return {}


OPS: dict[str, Callable[[KoshaClient, str, dict], dict]] = {
    name[3:]: fn for name, fn in globals().items() if name.startswith("op_")
}


# ── Matchers ──────────────────────────────────────────────────────────────


def error_kind(err: Exception) -> str:
    if isinstance(err, KoshaConfigError):
        return "config"
    if isinstance(err, KoshaRequestError):
        return "http" if err.status_code else "transport"
    return "unknown"


def check_error(want: dict, err: Exception | None) -> None:
    assert err is not None, f"expected an error {want}, the call succeeded"
    if "kind" in want:
        assert error_kind(err) == want["kind"], f"error kind {error_kind(err)} ({err!r})"
    if "status" in want:
        assert getattr(err, "status_code", None) == want["status"], repr(err)
    if "code" in want:
        assert getattr(err, "code", None) == want["code"], repr(err)
    if "retryable" in want:
        assert getattr(err, "retryable", None) == want["retryable"], repr(err)
    if "has_request_id" in want:
        assert bool(getattr(err, "request_id", None)) == want["has_request_id"], repr(err)
    if "retry_after_ms" in want:
        assert getattr(err, "retry_after_ms", None) == want["retry_after_ms"], repr(err)


MATCHERS: dict[str, Callable[[Any, dict, Exception | None, dict], None]] = {
    "ok": lambda want, r, e, m: None,  # success is the default; checked below
    "ids": lambda want, r, e, m: _eq(
        r.get("ids"), [d["id"] for d in generate(want["generate"])] if isinstance(want, dict) else want
    ),
    "ids_unordered": lambda want, r, e, m: _eq(sorted(r.get("ids") or []), sorted(want)),
    "ids_include": lambda want, r, e, m: _true(set(want) <= set(r.get("ids") or []), f"{want} not all in {r.get('ids')}"),
    "total_hits": lambda want, r, e, m: _eq(r.get("total_hits"), want),
    "count": lambda want, r, e, m: _eq(r.get("count"), want),
    "unique": lambda want, r, e, m: _true(len(set(r.get("ids") or [])) == len(r.get("ids") or []), "duplicate ids"),
    "indexed": lambda want, r, e, m: _eq(r.get("indexed"), want),
    "batches": lambda want, r, e, m: _eq(r.get("batches"), want),
    "failed_ids": lambda want, r, e, m: _eq(sorted(r.get("failed_ids") or []), sorted(want)),
    "error": lambda want, r, e, m: check_error(want, e),
    "attempts": lambda want, r, e, m: _eq(m["attempts"](), want),
    "attempts_min": lambda want, r, e, m: _true(m["attempts"]() >= want, f"attempts {m['attempts']()} < {want}"),
    "min_elapsed_ms": lambda want, r, e, m: _true(m["elapsed_ms"] >= want, f"elapsed {m['elapsed_ms']:.0f} ms < {want}"),
    "max_elapsed_ms": lambda want, r, e, m: _true(m["elapsed_ms"] <= want, f"elapsed {m['elapsed_ms']:.0f} ms > {want}"),
}


def _eq(got: Any, want: Any) -> None:
    assert got == want, f"got {got!r}, want {want!r}"


def _true(ok: bool, msg: str) -> None:
    assert ok, msg


# ── Runner ────────────────────────────────────────────────────────────────


def control(method: str, path: str, body: Any = None) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(CONTROL.rstrip("/") + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read() or b"{}")


def needs_proxy(case: dict) -> bool:
    return bool(case.get("faults")) or any(
        "attempts" in s.get("expect", {}) or "attempts_min" in s.get("expect", {}) for s in case["steps"]
    )


def make_client(api_key: str, tag: str, settings: dict | None = None) -> KoshaClient:
    settings = settings or {}
    kw: dict[str, Any] = {"api_key": api_key}
    if "timeout_ms" in settings:
        kw["timeout"] = settings["timeout_ms"] / 1000.0
    if "max_attempts" in settings:
        kw["max_retries"] = settings["max_attempts"]
    if "retry_non_idempotent" in settings:
        kw["retry_non_idempotent"] = settings["retry_non_idempotent"]
    if "total_budget_ms" in settings:
        kw["total_budget_ms"] = settings["total_budget_ms"]
    c = KoshaClient(URL, **kw)
    c._transport.headers["X-Conformance-Case"] = tag
    return c


def run_op(client: KoshaClient, op: str, ns: str, args: dict) -> tuple[dict, Exception | None]:
    args = substitute(args, ns)
    target = args.pop("namespace", ns)
    try:
        return OPS[op](client, target, args), None
    except BulkError as e:
        return bulk_result(e.result), e
    except (KoshaRequestError, KoshaConfigError) as e:
        return {}, e


def test_runner_implements_the_schema():
    schema = load_schema()
    assert set(OPS) == set(schema["op"]["enum"])
    assert set(MATCHERS) == set(schema["expect"]["properties"])


def test_template_rendering():
    assert render("d{i:04}", 7) == "d0007"
    assert render("t{i%7}", 23) == "t2"
    assert render("x{i%7:02}-{i}", 23) == "x02-23"


@pytest.mark.parametrize("case", load_cases(), ids=lambda c: c["id"])
def test_case(case: dict) -> None:
    if not URL:
        pytest.skip("KOSHA_CONFORMANCE_URL is not set")
    if needs_proxy(case) and not CONTROL:
        pytest.skip("needs faultproxy (KOSHA_CONFORMANCE_CONTROL)")
    key = f"{LANG}-{RUN_ID}-{case['id']}"
    ns = f"conf-{LANG}-{RUN_ID}-{re.sub(r'[^a-z0-9]+', '-', case['id'])}"
    api_key = KEYS[case.get("tenant", "default")]
    if case.get("faults"):
        control("POST", "/faults", {"case": key, "rules": case["faults"]})
    try:
        setup = make_client(api_key, f"{key}#setup")
        for step in case.get("setup", []):
            _, err = run_op(setup, step["op"], ns, dict(step.get("args", {})))
            assert err is None, f"setup {step['op']} failed: {err!r}"
        for i, step in enumerate(case["steps"]):
            tag = f"{key}#{i}"
            client = make_client(api_key, tag, step.get("client"))
            expect = substitute(step.get("expect", {}), ns)
            started = time.monotonic()
            result, err = run_op(client, step["op"], ns, dict(step.get("args", {})))
            elapsed_ms = (time.monotonic() - started) * 1000
            if "error" not in expect:
                assert err is None, f"step {i} ({step['op']}) failed: {err!r}"

            def attempts(tag: str = tag) -> int:
                log = control("GET", "/log?case=" + urllib.parse.quote(key))["requests"]
                return sum(1 for r in log if f"{key}#{r['step']}" == tag)

            meta = {"elapsed_ms": elapsed_ms, "attempts": attempts}
            for name, want in expect.items():
                assert name in MATCHERS, f"unknown matcher {name!r}"
                try:
                    MATCHERS[name](want, result, err, meta)
                except AssertionError as e:
                    raise AssertionError(f"step {i} ({step['op']}) {name}: {e}") from None
    finally:
        if case.get("faults"):
            control("DELETE", "/faults?case=" + urllib.parse.quote(key))
        cleanup = make_client(api_key, f"{key}#cleanup", {"max_attempts": 1})
        run_op(cleanup, "delete_namespace", ns, {})
