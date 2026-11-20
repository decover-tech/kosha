#!/usr/bin/env python3
"""Kosha integration suite: an ingest pod and a query pod over MinIO + Postgres.

Drives the read/write split over HTTP, the way k8s/base deploys it: writes go
to the ingest pod (or to the query pod, which forwards them), the ingest pod
uploads segments to S3 and publishes manifests by CAS, and the query pod
follows the published manifests and serves searches from its own cache.

Scenarios, in order (each check id starts with the scenario name):

  a  ingest-publish   index + flush on ingest -> exact answers on the query
                      pod (S3 upload, _COMMIT marker, CAS publish: #230,
                      #231); further flushes reach the already-loaded query
                      pod through the manifest refresher alone (#232)
  b  forwarded-writes index, upsert, delete-by-query and flush sent to the
                      query pod are forwarded to ingest; a strong search on
                      the query pod reads the unflushed tail (#255)
  f  deletes          delete by id and by query, visible on both pods (#237);
                      re-checked after e, c and h
  d  bulk-loaders     concurrent loader threads (half through each pod),
                      interleaved flushes and upserts -> exact final contents
  g  tenants          key A cannot read, write or delete tenant B's
                      namespace on either pod, v1 and legacy routes (#234)
  e  compaction       /v1/admin/compact-namespace, then exact answers on both
  c  ingest-restart   unflushed acknowledged writes survive an ingest restart,
                      then one with its local volume wiped (S3 WAL #236, boot
                      reconciliation)
  h  query-restart    query pod restarted with an empty cache serves
                      everything from S3

a, b, f, e, c and h share one namespace and check every answer exactly
against the conformance reference model (scripts/conformance/oracle.py):
ids, order, total hits, BM25 scores and stored fields, on both pods.

Usage:

  # The compose stack (what CI runs):
  docker compose -f docker-compose.yml \\
      -f scripts/integration/docker-compose.integration.yml up --build -d
  python3 scripts/integration/run.py \\
      --ingest http://localhost:8080 --query http://localhost:8081 \\
      --restart-cmd scripts/integration/compose_restart.sh --out report.json

  # One local process playing both pods (no forwarding, no restarts):
  cargo build --release -p kosha-server
  python3 scripts/integration/run.py --spawn target/release/kosha-server

  # Helpers only, against a fake HTTP server:
  python3 scripts/integration/run.py --self-test

Exit status: 0 when every check passed or failed only as listed in
expected_failures.json; 1 otherwise (with --strict, also when a listed
failure passes).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable, Dict, List, Optional, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
CONFORMANCE = os.path.join(os.path.dirname(HERE), "conformance")


def _load_conformance():
    """The conformance runner, loaded by path: this file is also `run.py`."""
    spec = importlib.util.spec_from_file_location("conformance_run", os.path.join(CONFORMANCE, "run.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # puts scripts/conformance on sys.path
    return module


conf = _load_conformance()
import corpus  # noqa: E402  (scripts/conformance)
import queries  # noqa: E402
from oracle import Oracle, matches  # noqa: E402

TENANT_KEYS = {"tenant-a": "sk-tenant-a", "tenant-b": "sk-tenant-b"}
CONVERGE_TIMEOUT = 30.0  # seconds a published version may take to reach the query pod


# ─── HTTP ───────────────────────────────────────────────────────────────────


class Pod(conf.Kosha):
    """One server and namespace. Adds to the conformance client: a name for
    check ids, and retries across a restarting server (connection errors
    and 503, e.g. a writer lease not yet taken over) for writes."""

    def __init__(self, name: str, host: str, api_key: str, namespace: str, admin_key: str, tenant: str):
        super().__init__(host, api_key, namespace, admin_key, tenant)
        self.name = name

    def scoped(self, namespace: str, api_key: Optional[str] = None, tenant: Optional[str] = None) -> "Pod":
        return Pod(self.name, self.host, api_key or self.api_key, namespace, self.admin_key, tenant or self.tenant)

    def call(self, method: str, path: str, body: Optional[Dict] = None, timeout: float = 120, key: Optional[str] = None, retry_for: float = 60):
        deadline = time.time() + retry_for
        delay = 0.2
        while True:
            try:
                status, out = super().call(method, path, body, timeout=timeout, key=key)
            except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
                if time.time() >= deadline:
                    raise
                status, out = None, {"error": str(e)}
            if status not in (None, 503) or time.time() >= deadline:
                return status, out
            time.sleep(delay)
            delay = min(delay * 2, 2.0)

    def raw(self, method: str, path: str, body=None, key: Optional[str] = None) -> Tuple[int, object]:
        """One request, no retries: for checks that expect a refusal."""
        data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
        req = urllib.request.Request(self.host + path, data=data, method=method)
        req.add_header("Authorization", "Bearer " + (key or self.api_key))
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                status = resp.status
        except urllib.error.HTTPError as e:
            raw, status = e.read(), e.code
        try:
            return status, json.loads(raw or b"null")
        except ValueError:
            return status, {"error": raw.decode(errors="replace")}

    def total(self, flt: Dict, consistency: str = "eventual") -> Tuple[int, Optional[int]]:
        status, out = self.search({"filter": flt, "max_results": 1, "consistency": consistency})
        return status, (out.get("total_hits") if status == 200 else None)

    def fetch_hits(self, flt: Dict, page: int = 500, consistency: str = "eventual") -> List[Dict]:
        """Every hit of a filter, in _id order, paged with `from`."""
        hits: List[Dict] = []
        while True:
            status, out = self.search(
                {"filter": flt, "max_results": page, "from": len(hits), "sort": [{"_id": {"order": "asc"}}], "consistency": consistency}
            )
            if status != 200:
                raise RuntimeError("%s search -> %s %s" % (self.name, status, out))
            batch = out.get("results", [])
            hits.extend(batch)
            if len(batch) < page:
                return hits


def field(hit: Dict, name: str) -> Optional[str]:
    for f in hit.get("fields", []):
        if f["name"] == name:
            return f["value"]
    return None


def poll_until(fn: Callable[[], bool], timeout: float, interval: float = 0.1) -> Optional[float]:
    """Seconds until fn() was true, or None on timeout."""
    start = time.time()
    while True:
        try:
            if fn():
                return time.time() - start
        except Exception:
            pass
        if time.time() - start > timeout:
            return None
        time.sleep(interval)


def denied(status: Optional[int]) -> bool:
    """A refused request: any 4xx. A 5xx is not a refusal, it is a bug."""
    return status is not None and 400 <= status < 500


# ─── fixtures ───────────────────────────────────────────────────────────────


def with_slot(doc: Dict) -> Dict:
    """A conformance corpus doc plus a `slot` keyword equal to its id, so
    scenarios can address (and delete) exact documents by filter. Keyword
    fields do not change BM25 lengths, so the reference model still holds."""
    return {"id": doc["id"], "fields": doc["fields"] + [{"name": "slot", "field_type": "Keyword", "value": doc["id"]}]}


def docs(seed: int, start: int, count: int, revision: int = 0) -> List[Dict]:
    return [with_slot(d) for d in corpus.batch(seed, start, count, revision)]


# Filter totals that move with almost any write; the query pod has caught up
# once they all match the reference model.
PROBES = (
    [{"match_all": {}}]
    + [{"term": {"status": s}} for s in corpus.STATUSES]
    + [{"term": {"tags": t}} for t in corpus.TAGS]
    + [{"term": {"owner": o}} for o in corpus.OWNERS[:4]]
)


def expected_totals(oracle: Oracle) -> List[int]:
    live = list(oracle.live_docs().values())
    return [sum(1 for d in live if matches(p, d)) for p in PROBES]


def observed_totals(pod: Pod) -> List[Optional[int]]:
    return [pod.total(p)[1] for p in PROBES]


# ─── the suite ──────────────────────────────────────────────────────────────


class Suite:
    def __init__(self, args, res, ingest: Pod, query: Pod, restart: Optional[Callable[[str, bool], None]], split: bool):
        self.args = args
        self.res = res
        self.ingest = ingest
        self.query = query
        self.restart = restart  # (service, wipe) -> None
        self.split = split  # two processes (compose) vs one (--spawn)
        self.oracle = Oracle()
        self.seed = args.seed
        self.next_doc = 0
        self.deleted_ids: List[str] = []
        self.timings: List[Dict] = []
        scratch = Oracle()
        scratch.index(corpus.batch(self.seed, 0, args.docs))
        scratch.flush()
        self.qs = queries.build(scratch, self.seed)
        self.walks = queries.walks(scratch, self.seed)

    # helpers -----------------------------------------------------------

    def record(self, check: str, ok: bool, detail: str = "") -> None:
        self.res.record(check, bool(ok), detail)

    def pods(self) -> List[Pod]:
        return [self.ingest, self.query] if self.split else [self.ingest]

    def take(self, count: int) -> List[Dict]:
        out = docs(self.seed, self.next_doc, count)
        self.next_doc += count
        return out

    def index(self, pod: Pod, batch: List[Dict]) -> None:
        pod.index(batch)
        self.oracle.index(batch)

    def flush(self, pod: Pod) -> None:
        pod.flush()
        self.oracle.flush()

    def converge(self, name: str, pod: Pod) -> None:
        """Wait until the pod answers the probe filters like the model."""
        want = expected_totals(self.oracle)
        last: List = []

        def done() -> bool:
            last[:] = observed_totals(pod)
            return last == want

        took = poll_until(done, CONVERGE_TIMEOUT)
        self.record(
            "%s@%s:converged" % (name, pod.name),
            took is not None,
            "caught up in %.2fs" % took if took is not None else "after %ss: probe totals %s, want %s" % (CONVERGE_TIMEOUT, last, want),
        )

    def checkpoint(self, name: str, pods: Optional[List[Pod]] = None) -> None:
        """Exact BM25 + filter answers (the conformance query set) on each
        pod, after it has caught up; then the deleted ids stay gone."""
        for pod in pods or self.pods():
            self.converge(name, pod)
            conf.checkpoint("%s@%s" % (name, pod.name), self.res, pod, self.oracle, self.qs, self.walks)
            if self.deleted_ids:
                status, total = pod.total({"terms": {"slot": self.deleted_ids}})
                self.record(
                    "%s@%s:deleted-stay-deleted" % (name, pod.name),
                    status == 200 and total == 0,
                    "HTTP %s, %s of %d deleted ids visible" % (status, total, len(self.deleted_ids)),
                )

    def wait_ready(self, pod: Pod) -> None:
        pod.wait_healthy(timeout=180)

    # scenarios -----------------------------------------------------------

    def a_ingest_publish(self) -> None:
        n = self.args.docs
        self.index(self.ingest, self.take(n // 3))
        self.flush(self.ingest)
        self.checkpoint("a.first-flush")
        # The namespace is now loaded on the query pod: from here on only
        # the background refresher brings it new versions.
        per = n // 6
        for _ in range(3):
            self.index(self.ingest, self.take(per))
            self.flush(self.ingest)
        self.checkpoint("a.refreshed")

    def b_forwarded_writes(self) -> None:
        q = self.query
        batch = self.take(self.args.docs // 10)
        self.index(q, batch)
        ids = [d["id"] for d in batch[:20]]
        # Read-your-writes before any flush: the query pod reads the ingest
        # pod's unindexed tail (#255).
        status, out = q.search({"filter": {"terms": {"slot": ids}}, "max_results": 50, "consistency": "strong"})
        got = sorted(h["doc_id"] for h in out.get("results", [])) if status == 200 else None
        self.record("b.strong-read-unflushed@%s" % q.name, got == sorted(ids), "HTTP %s, got %s" % (status, got))
        # Upsert flushed ids through the query pod (mark-deleted + append).
        upserts = [with_slot(corpus.make_doc(self.seed, i, 1)) for i in range(0, self.next_doc // 2, 41)]
        self.index(q, upserts)
        self.flush(q)
        flt = {"bool": {"must": [{"term": {"owner": "owner03"}}, {"range": {"priority": {"gte": "9"}}}]}}
        got_n = q.delete(flt)
        want_n = self.oracle.delete(flt)
        self.record("b.forwarded-delete-count@%s" % q.name, got_n == want_n, "deleted %s, want %d" % (got_n, want_n))
        self.checkpoint("b.forwarded")

    def f_deletes(self) -> None:
        live = sorted(self.oracle.live_docs())
        victims = live[5::97][:25]
        for pod, flt, label in (
            (self.ingest, {"terms": {"slot": victims}}, "by-id"),
            (self.query, {"term": {"status": "withdrawn"}}, "by-query"),
        ):
            before = set(self.oracle.live_docs())
            got = pod.delete(flt)
            want = self.oracle.delete(flt)
            self.deleted_ids.extend(sorted(before - set(self.oracle.live_docs())))
            self.record("f.delete-%s-count@%s" % (label, pod.name), got == want, "deleted %s, want %d" % (got, want))
        self.checkpoint("f.deleted")

    def d_bulk_loaders(self) -> None:
        loaders, per, batch, flush_every = self.args.loaders, self.args.loader_docs, 50, 4
        ns = "bulk-%d" % int(time.time())
        targets = [p.scoped(ns) for p in self.pods()]
        errors: List[str] = []
        acked = [0] * loaders

        def doc(loader: int, i: int, rev: int) -> Dict:
            return {
                "id": "L%02d-%05d" % (loader, i),
                "fields": [
                    {"name": "loader", "field_type": "Keyword", "value": "L%02d" % loader},
                    {"name": "rev", "field_type": "Keyword", "value": str(rev)},
                    {"name": "body", "field_type": "Text", "value": "bulk loader %d item %d rev %d" % (loader, i, rev)},
                ],
            }

        def run(loader: int) -> None:
            pod = targets[loader % len(targets)]  # half the loaders write through the query pod
            try:
                for b, start in enumerate(range(0, per, batch)):
                    pod.index([doc(loader, i, 0) for i in range(start, min(per, start + batch))])
                    acked[loader] += min(batch, per - start)
                    if b % flush_every == flush_every - 1:
                        pod.flush()
                pod.index([doc(loader, i, 1) for i in range(0, per, 5)])
                pod.flush()
            except Exception as e:  # recorded below
                errors.append("loader %d: %s" % (loader, e))

        threads = [threading.Thread(target=run, args=(i,)) for i in range(loaders)]
        start = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.record("d.loaders-acked", not errors and sum(acked) == loaders * per, "; ".join(errors[:3]) or "%.1fs" % (time.time() - start))
        targets[0].flush()

        want = {"L%02d-%05d" % (l, i): str(1 if i % 5 == 0 else 0) for l in range(loaders) for i in range(per)}
        for pod in targets:
            took = poll_until(lambda: pod.total({"match_all": {}})[1] == len(want) and pod.total({"term": {"rev": "1"}})[1] == len(want) // 5, CONVERGE_TIMEOUT)
            self.record("d.converged@%s" % pod.name, took is not None, "%.2fs" % took if took is not None else "timed out")
            hits = pod.fetch_hits({"match_all": {}})
            got = {h["doc_id"]: field(h, "rev") for h in hits}
            self.record("d.count@%s" % pod.name, len(hits) == len(want) and len(got) == len(want), "%d hits (%d distinct), want %d" % (len(hits), len(got), len(want)))
            missing = sorted(set(want) - set(got))
            extra = sorted(set(got) - set(want))
            stale = sorted(i for i in want if i in got and got[i] != want[i])
            self.record(
                "d.contents@%s" % pod.name,
                not (missing or extra or stale),
                "missing %s, unexpected %s, stale revision %s" % (missing[:5], extra[:5], stale[:5]),
            )

    def g_tenants(self) -> None:
        ns = "shared-%d" % int(time.time())
        foreign = urllib.parse.quote("tenant-b/" + ns, safe="")
        a = self.ingest.scoped(ns, TENANT_KEYS["tenant-a"], "tenant-a")
        b = self.ingest.scoped(ns, TENANT_KEYS["tenant-b"], "tenant-b")

        def tdoc(prefix: str, i: int) -> Dict:
            return {"id": "%s-%03d" % (prefix, i), "fields": [{"name": "title", "field_type": "Text", "value": "tenant doc %s" % prefix}, {"name": "owner", "field_type": "Keyword", "value": prefix}]}

        b_ids = sorted("b-%03d" % i for i in range(30))
        a_ids = sorted("a-%03d" % i for i in range(10))
        b.index([tdoc("b", i) for i in range(30)])
        b.flush()
        a.index([tdoc("a", i) for i in range(10)])
        a.flush()

        def ids_of(pod: Pod) -> Optional[List[str]]:
            try:
                return sorted(h["doc_id"] for h in pod.fetch_hits({"match_all": {}}))
            except RuntimeError:
                return None

        for pod in self.pods():
            pa = pod.scoped(ns, TENANT_KEYS["tenant-a"], "tenant-a")
            pb = pod.scoped(ns, TENANT_KEYS["tenant-b"], "tenant-b")
            p = pod.name
            poll_until(lambda: ids_of(pa) == a_ids and ids_of(pb) == b_ids, CONVERGE_TIMEOUT)
            self.record("g.own-namespace@%s" % p, ids_of(pa) == a_ids and ids_of(pb) == b_ids, "A sees %s, B sees %s" % (ids_of(pa), ids_of(pb)))

            match_all = {"query_text": "", "filter": {"match_all": {}}, "max_results": 100, "consistency": "eventual"}
            refusals = {
                "v1-search": ("POST", "/v1/namespaces/%s/search" % foreign, match_all),
                "v1-stats": ("GET", "/v1/namespaces/%s/stats" % foreign, None),
                "v1-exists": ("POST", "/v1/namespaces/%s/exists" % foreign, {"ids": b_ids[:3]}),
                "v1-index": ("POST", "/v1/namespaces/%s/documents" % foreign, {"documents": [tdoc("x", 0)]}),
                "v1-delete": ("POST", "/v1/namespaces/%s/delete" % foreign, {"filter": {"match_all": {}}}),
                "v1-flush": ("POST", "/v1/namespaces/%s/flush" % foreign, {}),
                "v1-drop-namespace": ("DELETE", "/v1/namespaces/%s" % foreign, None),
                "legacy-search": ("POST", "/search", dict(match_all, namespace="tenant-b/" + ns)),
                "legacy-get-search": ("GET", "/search?ns=%s&q=tenant" % foreign, None),
                "legacy-index": ("POST", "/index", {"namespace": "tenant-b/" + ns, "documents": [tdoc("x", 0)]}),
                "legacy-replace": ("POST", "/replace", {"namespace": "tenant-b/" + ns, "documents": [tdoc("x", 0)]}),
                "legacy-delete": ("POST", "/delete", {"namespace": "tenant-b/" + ns, "filter": {"match_all": {}}}),
                "legacy-flush": ("POST", "/flush", {"namespace": "tenant-b/" + ns}),
                "legacy-exists": ("POST", "/exists", {"namespace": "tenant-b/" + ns, "ids": b_ids[:3]}),
                "admin-compact": ("POST", "/v1/admin/compact-namespace", {"namespace": "tenant-b/" + ns}),
                "global-stats": ("GET", "/v1/stats", None),
            }
            for label, (method, path, body) in refusals.items():
                status, out = pa.raw(method, path, body)
                leaked = [i for i in b_ids if i in json.dumps(out)]
                self.record("g.cross-%s@%s" % (label, p), denied(status) and not leaked, "HTTP %s %s" % (status, json.dumps(out)[:200]))
            # The legacy routes scope an unqualified name to the caller.
            status, out = pa.raw("POST", "/search", dict(match_all, namespace=ns))
            got = sorted(h["doc_id"] for h in out.get("results", [])) if status == 200 else None
            self.record("g.legacy-search-own@%s" % p, got == a_ids, "HTTP %s, got %s" % (status, got))

        # A wipes its own namespace through the legacy route on the query
        # pod (forwarded); B's same-named namespace is untouched.
        last = self.pods()[-1].scoped(ns, TENANT_KEYS["tenant-a"], "tenant-a")
        status, out = last.raw("POST", "/delete", {"namespace": ns, "filter": {"match_all": {}}})
        self.record("g.legacy-delete-own@%s" % last.name, status == 200 and out.get("deleted") == len(a_ids), "HTTP %s %s" % (status, out))
        for pod in self.pods():
            pa = pod.scoped(ns, TENANT_KEYS["tenant-a"], "tenant-a")
            pb = pod.scoped(ns, TENANT_KEYS["tenant-b"], "tenant-b")
            poll_until(lambda: ids_of(pa) == [], CONVERGE_TIMEOUT)
            self.record("g.after-delete@%s" % pod.name, ids_of(pa) == [] and ids_of(pb) == b_ids, "A sees %s, B sees %s" % (ids_of(pa), ids_of(pb)))

    def e_compaction(self) -> None:
        out = self.ingest.compact()
        before = len(self.oracle.segments)
        self.oracle.compact()
        summary = {k: out.get(k) for k in ("segments_before", "segments_after", "segments_merged", "not_hydrated")}
        self.record("e.compacted", out.get("segments_after") == len(self.oracle.segments), "%s; model %d -> %d segments" % (summary, before, len(self.oracle.segments)))
        self.checkpoint("e.compacted")

    def c_ingest_restart(self) -> None:
        if not self.restart:
            print("  c  skipped (needs --restart-cmd)")
            return
        for wipe, name in ((False, "c.ingest-restart"), (True, "c.ingest-restart-wiped")):
            # Acknowledged but never flushed: only the WAL has these.
            self.index(self.ingest, self.take(self.args.docs // 20))
            self.restart("kosha-ingest", wipe)
            self.wait_ready(self.ingest)
            self.flush(self.ingest)
            self.checkpoint(name)

    def h_query_restart(self) -> None:
        if not (self.restart and self.split):
            print("  h  skipped (needs --restart-cmd and a query pod)")
            return
        self.restart("kosha-query", True)
        self.wait_ready(self.query)
        self.checkpoint("h.query-restart-cold", [self.query])

    def run(self) -> None:
        print("namespace %s, seed %d, %d queries + %d walks per checkpoint, pods: %s" % (self.ingest.ns, self.seed, len(self.qs), len(self.walks), ", ".join("%s=%s" % (p.name, p.host) for p in self.pods())))
        steps = [
            ("a", self.a_ingest_publish),
            ("b", self.b_forwarded_writes),
            ("f", self.f_deletes),
            ("d", self.d_bulk_loaders),
            ("g", self.g_tenants),
            ("e", self.e_compaction),
            ("c", self.c_ingest_restart),
            ("h", self.h_query_restart),
        ]
        only = set(self.args.only.split(",")) if self.args.only else None
        for key, fn in steps:
            if only and key not in only:
                continue
            if key == "b" and not self.split:
                print("  b  skipped (one process: nothing to forward)")
                continue
            t = time.time()
            print("scenario %s (%s)" % (key, fn.__name__))
            error = None
            try:
                fn()
            except Exception as e:
                error = "%s: %s" % (type(e).__name__, e)
                traceback.print_exc()
                self.record("%s.error" % key, False, error)
            self.timings.append({"scenario": fn.__name__, "seconds": round(time.time() - t, 1), "error": error})


# ─── self-test ──────────────────────────────────────────────────────────────


def self_test() -> int:
    """Exercise the helpers against a fake server: retries across 429/503
    and refused connections, readiness polling, paging, refusals, the
    expected-failures file and the report."""
    import http.server
    import tempfile

    hits_db = ["d%03d" % i for i in range(23)]
    counters: Dict[str, int] = {}

    class Fake(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code: int, body: Dict) -> None:
            raw = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("content-length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            counters[self.path] = counters.get(self.path, 0) + 1
            if self.path == "/readyz":
                return self._send(200 if counters[self.path] > 2 else 503, {})
            self._send(404, {"error": "not found"})

        def do_POST(self):
            n = counters[self.path] = counters.get(self.path, 0) + 1
            body = json.loads(self.rfile.read(int(self.headers["content-length"])) or b"{}")
            if self.path == "/flaky":
                return self._send({1: 429, 2: 503}.get(n, 200), {"n": n})
            if self.path == "/forbidden":
                return self._send(403, {"error": "nope"})
            if self.path.endswith("/search"):
                assert self.headers["Authorization"] == "Bearer k"
                start, size = body.get("from", 0), body["max_results"]
                page = hits_db[start : start + size]
                return self._send(200, {"total_hits": len(hits_db), "results": [{"doc_id": i, "fields": [{"name": "rev", "value": "0"}]} for i in page]})
            self._send(404, {})

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Fake)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    pod = Pod("fake", "http://127.0.0.1:%d" % server.server_port, "k", "ns", "admin", "default")
    failures: List[str] = []

    def check(name: str, ok: bool, detail="") -> None:
        print("  %-34s %s %s" % (name, "ok" if ok else "FAIL", detail))
        if not ok:
            failures.append(name)

    status, out = pod.call("POST", "/flaky", {})
    check("retries 429 then 503", status == 200 and out == {"n": 3}, (status, out))
    status, _ = pod.call("POST", "/forbidden", {})
    check("4xx returned, not retried", status == 403 and counters["/forbidden"] == 1)
    check("raw() does not retry", pod.raw("POST", "/flaky", {})[0] == 200 and counters["/flaky"] == 4)
    pod.wait_healthy(timeout=10)
    check("wait_healthy polls /readyz", counters["/readyz"] == 3)
    hits = pod.fetch_hits({"match_all": {}}, page=5)
    check("fetch_hits pages to the end", [h["doc_id"] for h in hits] == hits_db, len(hits))
    check("total()", pod.total({"match_all": {}}) == (200, 23))
    check("field()", field(hits[0], "rev") == "0" and field(hits[0], "x") is None)
    check("denied()", denied(400) and denied(403) and not denied(200) and not denied(500) and not denied(None))
    ticks = iter(range(100))
    check("poll_until succeeds", poll_until(lambda: next(ticks) >= 3, 5, 0.01) is not None)
    check("poll_until times out", poll_until(lambda: False, 0.05, 0.01) is None)
    check("poll_until swallows errors", poll_until(lambda: 1 / 0, 0.05, 0.01) is None)

    dead = Pod("dead", "http://127.0.0.1:9", "k", "ns", "a", "t")
    t = time.time()
    try:
        dead.call("GET", "/x", retry_for=0.5)
        check("connection refused raises after retry window", False)
    except urllib.error.URLError:
        check("connection refused raises after retry window", time.time() - t >= 0.4)

    d = docs(1, 0, 3)
    check("with_slot adds slot = id", all(field({"fields": x["fields"]}, "slot") == x["id"] for x in d))
    o = Oracle()
    o.index(d)
    o.flush()
    check("expected_totals counts the model", expected_totals(o)[0] == 3 and len(expected_totals(o)) == len(PROBES))

    known = load_expected()
    res = conf.Results(known["checks"], known["bugs"])
    res.record("x:1", True)
    res.record("x:2", False, "boom")
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "r.json")
        code = conf.report(res, path, False, False)
        add_timings(path, [{"scenario": "x", "seconds": 1.0, "error": None}])
        with open(path) as f:
            rep = json.load(f)
    check("report fails on a failure", code == 1 and rep["summary"]["fail"] == 1 and rep["scenarios"][0]["scenario"] == "x")
    server.shutdown()
    print("self-test: %s" % ("FAILED: " + ", ".join(failures) if failures else "all ok"))
    return 1 if failures else 0


# ─── main ───────────────────────────────────────────────────────────────────


def load_expected() -> Dict:
    with open(os.path.join(HERE, "expected_failures.json")) as f:
        known = json.load(f)
    for glob, bug in known["checks"].items():
        if bug not in known["bugs"]:
            raise SystemExit("expected_failures.json: check %r names unknown bug %r" % (glob, bug))
    return known


def add_timings(path: Optional[str], timings: List[Dict]) -> None:
    if not path:
        return
    with open(path) as f:
        rep = json.load(f)
    rep["scenarios"] = timings
    with open(path, "w") as f:
        json.dump(rep, f, indent=1)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument("--ingest", help="URL of the ingest pod (with --query)")
    target.add_argument("--spawn", metavar="BINARY", help="start one kosha-server that plays both pods")
    target.add_argument("--self-test", action="store_true", help="test the helpers against a fake server and exit")
    p.add_argument("--query", help="URL of the query pod (KOSHA_INGEST_HOST set)")
    p.add_argument("--api-key", default="sk-kosha-dev", help="key of tenant `default`")
    p.add_argument("--admin-key", default="sk-kosha-admin-dev")
    p.add_argument("--restart-cmd", help="`CMD <service> [--wipe]` restarts kosha-ingest / kosha-query (compose_restart.sh)")
    p.add_argument("--port", type=int, default=18090, help="port for --spawn")
    p.add_argument("--namespace", help="default: integration-<seed>-<unix time>")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--docs", type=int, default=2000, help="documents in the main lifecycle")
    p.add_argument("--loaders", type=int, default=8, help="concurrent bulk loader threads")
    p.add_argument("--loader-docs", type=int, default=400, help="documents per loader")
    p.add_argument("--only", help="comma-separated scenario letters to run, e.g. a,g")
    p.add_argument("--out", help="write a JSON report here")
    p.add_argument("--keep", action="store_true", help="keep the --spawn data dir and log")
    p.add_argument("--strict", action="store_true", help="also fail when an expected failure passes")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()
    if args.self_test:
        return self_test()
    if args.ingest and not args.query:
        p.error("--ingest needs --query")

    known = load_expected()
    res = conf.Results(known["checks"], known["bugs"])
    ns = args.namespace or "integration-%d-%d" % (args.seed, int(time.time()))
    spawned = None
    restart = None
    if args.spawn:
        os.environ["KOSHA_API_KEYS"] = ",".join("%s=%s" % (k, t) for t, k in TENANT_KEYS.items())
        spawned = conf.Spawned(args.spawn, args.port, args.api_key, args.admin_key, None)
        spawned.start()
        host = "http://127.0.0.1:%d" % args.port
        ingest = Pod("ingest", host, args.api_key, ns, args.admin_key, "default")
        query = ingest
    else:
        ingest = Pod("ingest", args.ingest, args.api_key, ns, args.admin_key, "default")
        query = Pod("query", args.query, args.api_key, ns, args.admin_key, "default")
        if args.restart_cmd:
            def restart(service: str, wipe: bool) -> None:
                cmd = "%s %s%s" % (args.restart_cmd, service, " --wipe" if wipe else "")
                print("  $ " + cmd)
                subprocess.run(cmd, shell=True, check=True)
    suite = None
    started = time.time()
    try:
        ingest.wait_healthy(timeout=180)
        query.wait_healthy(timeout=180)
        suite = Suite(args, res, ingest, query, restart, split=spawned is None)
        suite.run()
    finally:
        if spawned:
            spawned.cleanup(args.keep)
    print("\nscenarios: " + ", ".join("%s %.0fs%s" % (t["scenario"], t["seconds"], " (error)" if t["error"] else "") for t in suite.timings) + "; total %.0fs" % (time.time() - started))
    code = conf.report(res, args.out, args.strict, args.verbose)
    add_timings(args.out, suite.timings)
    return code


if __name__ == "__main__":
    sys.exit(main())
