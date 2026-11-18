#!/usr/bin/env python3
"""Kosha conformance suite: BM25 + filters, checked against a reference model.

Drives one namespace through a write lifecycle and, at each checkpoint, runs
the same query set against Kosha and against an in-memory reference model
(``oracle.py``), comparing every answer exactly: matching ids, page order,
exact total hit count, BM25 score per hit, stored fields, and the
namespace's segment/document counts.

Checkpoints, in order:

  buffered       docs indexed but not flushed  -> nothing may be visible
  one-segment    first flush
  many-segments  several more flushes          -> per-segment BM25 statistics
  upserts        re-index flushed and buffered ids (mark-deleted + append path)
  deletes        delete-by-query, delete vs. unflushed docs, re-index of a
                 deleted id
  compacted      tiered compaction via the admin endpoint
  restarted      server restart (only with --spawn or --restart-cmd); with
                 an emptied local cache this exercises cold hydration

Usage:

  # Let the suite start (and restart) a locally built server itself:
  cargo build --release -p kosha-server
  python3 scripts/conformance/run.py --spawn target/release/kosha-server

  # Against an already-running server (e.g. the docker compose stack):
  python3 scripts/conformance/run.py --host http://localhost:8080 \\
      --api-key sk-kosha-dev \\
      --restart-cmd "scripts/conformance/compose_restart_cold.sh"

The server must run with KOSHA_MAX_CONCURRENT_FLUSH_WRITERS=1, a large
KOSHA_FLUSH_THRESHOLD and KOSHA_FLUSH_MAX_AGE_MS=0 (no idle-buffer flush
timer) so segments come only from explicit flushes, one per flush; otherwise
the per-segment score checks cannot be predicted (``--spawn`` sets all three).

Exit status: 0 when every check passed or failed only as listed in
expected_failures.json; 1 on any unexpected failure. With --strict, an
expected failure that passes is also an error, so the list stays honest.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from typing import Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import corpus  # noqa: E402
import queries  # noqa: E402
from oracle import Oracle, cursor_for  # noqa: E402

SCORE_REL_TOL = 1e-6


# ─── HTTP ───────────────────────────────────────────────────────────────────


class Kosha:
    def __init__(self, host: str, api_key: str, namespace: str, admin_key: str, tenant: str):
        self.host = host.rstrip("/")
        self.api_key = api_key
        self.ns = namespace
        # Admin routes need the dedicated admin credential and address the
        # namespace by its stored, tenant-qualified name.
        self.admin_key = admin_key
        self.tenant = tenant

    def call(self, method: str, path: str, body: Optional[Dict] = None, timeout: float = 120, key: Optional[str] = None):
        data = json.dumps(body).encode() if body is not None else None
        for attempt in range(6):
            req = urllib.request.Request(self.host + path, data=data, method=method)
            req.add_header("Authorization", "Bearer " + (key or self.api_key))
            req.add_header("Content-Type", "application/json")
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return resp.status, json.loads(resp.read() or b"null")
            except urllib.error.HTTPError as e:
                raw = e.read()
                if e.code == 429 and attempt < 5:  # load shedding: transient
                    time.sleep(0.2 * (2**attempt))
                    continue
                try:
                    return e.code, json.loads(raw)
                except ValueError:
                    return e.code, {"error": raw.decode(errors="replace")}
        raise RuntimeError("unreachable")

    def ns_path(self, suffix: str) -> str:
        return "/v1/namespaces/%s/%s" % (self.ns, suffix)

    def must(self, method: str, path: str, body: Optional[Dict] = None, key: Optional[str] = None):
        status, out = self.call(method, path, body, key=key)
        if status != 200:
            raise RuntimeError("%s %s -> %s %s" % (method, path, status, out))
        return out

    def index(self, docs: List[Dict]) -> None:
        for i in range(0, len(docs), 500):
            self.must("POST", self.ns_path("documents"), {"documents": docs[i : i + 500]})

    def flush(self) -> None:
        self.must("POST", self.ns_path("flush"), {})

    def delete(self, flt: Dict) -> int:
        return self.must("POST", self.ns_path("delete"), {"filter": flt}).get("deleted")

    def compact(self) -> Dict:
        return self.must(
            "POST",
            "/v1/admin/compact-namespace",
            {"namespace": "%s/%s" % (self.tenant, self.ns)},
            key=self.admin_key,
        )

    def stats(self):
        return self.call("GET", self.ns_path("stats"))

    def search(self, body: Dict):
        b = dict(body)
        b.setdefault("query_text", "")  # required by the API, even for filter-only
        b.setdefault("exact_total_hits", True)
        b["no_cache"] = True
        return self.call("POST", self.ns_path("search"), b)

    def wait_healthy(self, timeout: float = 120) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(self.host + "/readyz", timeout=2) as r:
                    if r.status == 200:
                        return
            except Exception:
                pass
            time.sleep(0.3)
        raise RuntimeError("server at %s not ready after %ss" % (self.host, timeout))


# ─── spawned server ─────────────────────────────────────────────────────────


class Spawned:
    """A kosha-server child process with a throwaway data dir."""

    def __init__(self, binary: str, port: int, api_key: str, admin_key: str, database_url: Optional[str], cold_restart: bool = False):
        self.cold_restart = cold_restart
        self.binary = os.path.abspath(binary)
        self.database_url = database_url
        self.port = port
        self.api_key = api_key
        self.admin_key = admin_key
        self.dir = tempfile.mkdtemp(prefix="kosha-conformance-")
        self.proc: Optional[subprocess.Popen] = None

    def start(self) -> None:
        env = dict(os.environ)
        env.update(
            {
                "KOSHA_API_KEY": self.api_key,
                "KOSHA_ADMIN_API_KEY": self.admin_key,
                "KOSHA_HTTP_PORT": str(self.port),
                "KOSHA_DATA_DIR": os.path.join(self.dir, "data"),
                "KOSHA_FLUSH_THRESHOLD": "1000000",
                "KOSHA_MAX_CONCURRENT_FLUSH_WRITERS": "1",
                "KOSHA_FLUSH_MAX_AGE_MS": "0",
            }
        )
        if self.database_url:
            env["DATABASE_URL"] = self.database_url
        log = open(os.path.join(self.dir, "server.log"), "ab")
        self.proc = subprocess.Popen([self.binary], env=env, cwd=self.dir, stdout=log, stderr=subprocess.STDOUT)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def restart(self) -> None:
        self.stop()
        if self.cold_restart:
            shutil.rmtree(os.path.join(self.dir, "data"), ignore_errors=True)
        self.start()

    def cleanup(self, keep: bool) -> None:
        self.stop()
        if keep:
            print("server data and log kept in %s" % self.dir)
        else:
            shutil.rmtree(self.dir, ignore_errors=True)


# ─── checking ───────────────────────────────────────────────────────────────


class Results:
    def __init__(self, expected: Dict[str, str], bugs: Dict[str, str]):
        self.expected = expected  # check glob -> bug key
        self.bugs = bugs  # bug key -> description
        self.rows: List[Dict] = []

    def _expected_issue(self, check_id: str) -> Optional[str]:
        for pattern, issue in self.expected.items():
            if fnmatch.fnmatchcase(check_id, pattern):
                return issue
        return None

    def record(self, check_id: str, ok: bool, detail: str = "") -> None:
        issue = self._expected_issue(check_id)
        if ok:
            outcome = "xpass" if issue else "pass"
        else:
            outcome = "xfail" if issue else "fail"
        self.rows.append({"check": check_id, "outcome": outcome, "issue": issue, "detail": detail})

    def unexpected(self, strict: bool) -> List[Dict]:
        bad = ("fail", "xpass") if strict else ("fail",)
        return [r for r in self.rows if r["outcome"] in bad]


def _close(a: float, b: float) -> bool:
    return abs(a - b) <= SCORE_REL_TOL * max(1.0, abs(a), abs(b))


def _fieldset(fields: List[Dict]):
    return sorted((f["name"], f["field_type"], f["value"]) for f in fields)


def _first_diff(got: List[str], want: List[str]) -> str:
    for i, (g, w) in enumerate(zip(got, want)):
        if g != w:
            return "first difference at #%d: got %s, want %s (got %d ids, want %d)" % (i, g, w, len(got), len(want))
    return "got %d ids, want %d" % (len(got), len(want))


def check_query(res: Results, prefix: str, kosha: Kosha, oracle: Oracle, qid: str, body: Dict, empty_ok_404: bool) -> None:
    want_all = oracle.hits(body)
    start = body.get("from", 0)
    size = body.get("max_results", 10)
    want = want_all[start : start + size]
    cid = "%s:%s" % (prefix, qid)

    status, out = kosha.search(body)
    if status == 404 and empty_ok_404 and not want_all:
        res.record(cid + ":status", True)
        return
    res.record(cid + ":status", status == 200, "" if status == 200 else "HTTP %s %s" % (status, out))
    if status != 200:
        return

    got = out.get("results", [])
    got_ids = [h["doc_id"] for h in got]
    want_ids = [h[0] for h in want]

    total, rel = out.get("total_hits"), out.get("total_hits_relation", "eq")
    res.record(
        cid + ":total",
        total == len(want_all) and rel == "eq",
        "" if total == len(want_all) and rel == "eq" else "total_hits %s (%s), want %d (eq)" % (total, rel, len(want_all)),
    )
    res.record(cid + ":ids", got_ids == want_ids, "" if got_ids == want_ids else _first_diff(got_ids, want_ids))

    by_id = {h[0]: h for h in want_all}
    bad_scores, bad_fields = [], []
    for h in got:
        ref = by_id.get(h["doc_id"])
        if ref is None:
            continue
        if not _close(h["score"], ref[1]):
            bad_scores.append("%s got %.9g want %.9g" % (h["doc_id"], h["score"], ref[1]))
        if _fieldset(h.get("fields", [])) != _fieldset(ref[2].fields):
            bad_fields.append(h["doc_id"])
    res.record(cid + ":scores", not bad_scores, "; ".join(bad_scores[:3]) + (" (+%d more)" % (len(bad_scores) - 3) if len(bad_scores) > 3 else ""))
    res.record(cid + ":fields", not bad_fields, "stale/wrong fields for " + ", ".join(bad_fields[:5]) if bad_fields else "")


def check_walk(res: Results, prefix: str, kosha: Kosha, oracle: Oracle, walk: Dict) -> None:
    body = walk["body"]
    page = walk["page"]
    want_ids = [h[0] for h in oracle.hits(body)]
    cid = "%s:%s" % (prefix, walk["id"])
    got_ids: List[str] = []
    cursor = None
    for _ in range(len(want_ids) // page + 3):
        b = dict(body, max_results=page)
        if walk["mode"] == "from":
            b["from"] = len(got_ids)
        elif cursor is not None:
            b["search_after"] = cursor
        status, out = kosha.search(b)
        if status != 200:
            res.record(cid + ":walk", False, "HTTP %s %s after %d ids" % (status, out, len(got_ids)))
            return
        hits = out.get("results", [])
        if not hits:
            break
        got_ids.extend(h["doc_id"] for h in hits)
        if walk["mode"] == "search_after":
            last = hits[-1]
            ref = oracle.live_docs().get(last["doc_id"])
            if ref is None:
                res.record(cid + ":walk", False, "page returned unknown doc %s" % last["doc_id"])
                return
            cursor = cursor_for((last["doc_id"], last["score"], ref), body["sort"])
    dupes = len(got_ids) - len(set(got_ids))
    ok = got_ids == want_ids
    detail = ""
    if not ok:
        detail = _first_diff(got_ids, want_ids) + ("; %d duplicate ids across pages" % dupes if dupes else "")
    res.record(cid + ":walk", ok, detail)


def check_stats(res: Results, prefix: str, kosha: Kosha, oracle: Oracle, empty_ok_404: bool) -> None:
    status, out = kosha.stats()
    want_docs = sum(s.n for s in oracle.segments)
    want_segs = len(oracle.segments)
    if status == 404 and empty_ok_404 and want_segs == 0:
        res.record(prefix + ":stats", True)
        return
    ok = status == 200 and out.get("documents") == want_docs and out.get("segments") == want_segs
    res.record(
        prefix + ":stats",
        ok,
        "" if ok else "HTTP %s: documents=%s segments=%s, want documents=%d segments=%d (incl. tombstoned)"
        % (status, out.get("documents"), out.get("segments"), want_docs, want_segs),
    )


def checkpoint(name: str, res: Results, kosha: Kosha, oracle: Oracle, qs: List[Dict], walks: List[Dict], empty_ok_404=False) -> None:
    t = time.time()
    check_stats(res, name, kosha, oracle, empty_ok_404)
    for q in qs:
        check_query(res, name, kosha, oracle, q["id"], q["body"], empty_ok_404)
    if not empty_ok_404:
        for w in walks:
            check_walk(res, name, kosha, oracle, w)
    mine = [r for r in res.rows if r["check"].startswith(name + ":")]
    counts = {k: sum(1 for r in mine if r["outcome"] == k) for k in ("pass", "fail", "xfail", "xpass")}
    print(
        "  %-14s %4d pass  %3d fail  %3d xfail  %3d xpass   (%d live docs, %d segments, %.1fs)"
        % (name, counts["pass"], counts["fail"], counts["xfail"], counts["xpass"], len(oracle.live_docs()), len(oracle.segments), time.time() - t)
    )


# ─── scenario ───────────────────────────────────────────────────────────────


def run(args, kosha: Kosha, restart) -> Results:
    with open(os.path.join(HERE, "expected_failures.json")) as f:
        known = json.load(f)
    for glob, bug in known["checks"].items():
        if bug not in known["bugs"]:
            raise SystemExit("expected_failures.json: check %r names unknown bug %r" % (glob, bug))
    res = Results(known["checks"], known["bugs"])
    oracle = Oracle()
    seed, n = args.seed, args.docs

    # Query set is derived from the full original corpus up front, so every
    # checkpoint runs the identical set.
    scratch = Oracle()
    scratch.index(corpus.batch(seed, 0, n))
    scratch.flush()
    qs, walks = queries.build(scratch, seed), queries.walks(scratch, seed)

    def both(fn_k, fn_o):
        out = fn_k()
        fn_o()
        return out

    first = n // 3
    rest = n - first - n // 6
    per = max(1, rest // 5)
    tail_start = first + 5 * per

    print("namespace %s, %d docs, seed %d, %d queries + %d paginated walks per checkpoint" % (kosha.ns, n, seed, len(qs), len(walks)))

    # buffered
    docs = corpus.batch(seed, 0, first)
    both(lambda: kosha.index(docs), lambda: oracle.index(docs))
    checkpoint("buffered", res, kosha, oracle, qs, walks, empty_ok_404=True)

    # one-segment
    both(kosha.flush, oracle.flush)
    checkpoint("one-segment", res, kosha, oracle, qs, walks)

    # many-segments
    for i in range(5):
        docs = corpus.batch(seed, first + i * per, per)
        both(lambda: kosha.index(docs), lambda: oracle.index(docs))
        both(kosha.flush, oracle.flush)
    checkpoint("many-segments", res, kosha, oracle, qs, walks)

    # upserts: flushed ids (mark-deleted + append), then buffered ids replaced
    # before their first flush.
    flushed = [corpus.make_doc(seed, i, 1) for i in range(0, tail_start, 37)]
    both(lambda: kosha.index(flushed[: len(flushed) // 2]), lambda: oracle.index(flushed[: len(flushed) // 2]))
    both(lambda: kosha.index(flushed[len(flushed) // 2 :]), lambda: oracle.index(flushed[len(flushed) // 2 :]))
    mid = tail_start + (n - tail_start) // 2
    docs = corpus.batch(seed, tail_start, mid - tail_start)
    both(lambda: kosha.index(docs), lambda: oracle.index(docs))
    again = [corpus.make_doc(seed, i, 1) for i in range(tail_start, mid, 5)]
    both(lambda: kosha.index(again), lambda: oracle.index(again))
    both(kosha.flush, oracle.flush)
    checkpoint("upserts", res, kosha, oracle, qs, walks)

    # deletes
    for flt in (
        {"term": {"status": "withdrawn"}},
        {"bool": {"must": [{"term": {"owner": "owner05"}}, {"range": {"priority": {"gte": "8"}}}]}},
    ):
        got = kosha.delete(flt)
        want = oracle.delete(flt)
        res.record("deletes:delete-count:%s" % json.dumps(flt, sort_keys=True), got == want, "" if got == want else "deleted %s, want %d" % (got, want))
    # Delete-by-query flushes first, so buffered matches are deleted too and
    # can't reappear after the next flush (#238).
    docs = corpus.batch(seed, mid, n - mid)
    both(lambda: kosha.index(docs), lambda: oracle.index(docs))
    flt = {"term": {"tags": "sealed"}}
    got, want = kosha.delete(flt), oracle.delete(flt)
    res.record("deletes:delete-count:buffered-included", got == want, "" if got == want else "deleted %s, want %d" % (got, want))
    both(kosha.flush, oracle.flush)
    # Re-index a few deleted ids: they must come back with the new content.
    dead = sorted(set(scratch.live_docs()) - set(oracle.live_docs()))[:10]
    revived = [corpus.make_doc(seed, int(d[1:]), 2) for d in dead]
    both(lambda: kosha.index(revived), lambda: oracle.index(revived))
    both(kosha.flush, oracle.flush)
    checkpoint("deletes", res, kosha, oracle, qs, walks)

    if args.cold_restart and restart is not None:
        restart()
        kosha.wait_healthy()
        checkpoint("deletes-restarted", res, kosha, oracle, qs, walks)

    # compacted
    out = kosha.compact()
    oracle.compact()
    print("  compaction: %s" % json.dumps({k: out.get(k) for k in ("segments_before", "segments_after", "segments_merged", "not_hydrated")}))
    checkpoint("compacted", res, kosha, oracle, qs, walks)

    # restarted
    if restart is not None:
        restart()
        kosha.wait_healthy()
        checkpoint("restarted", res, kosha, oracle, qs, walks)
    else:
        print("  restarted      skipped (needs --restart-cmd, or --spawn with --database-url)")
    return res


def report(res: Results, out_path: Optional[str], strict: bool, verbose: bool) -> int:
    sections = [("fail", "UNEXPECTED FAILURES")]
    if verbose:
        sections += [("xpass", "Expected failures that passed"), ("xfail", "Expected failures")]
    else:
        for kind, title in (("xfail", "Known bugs tripped"), ("xpass", "Known bugs NOT tripped this run (fixed, or layout-dependent; --strict fails on these)")):
            per: Dict[str, int] = {}
            for r in res.rows:
                if r["outcome"] == kind:
                    per[r["issue"]] = per.get(r["issue"], 0) + 1
            if per:
                print("\n%s (checks per bug; descriptions in expected_failures.json):" % title)
                for bug, c in sorted(per.items()):
                    print("  %4d  %s" % (c, bug))
    for kind, title in sections:
        rows = [r for r in res.rows if r["outcome"] == kind]
        if not rows:
            continue
        print("\n%s (%d)" % (title, len(rows)))
        for r in rows[:60]:
            tag = " [%s]" % r["issue"] if r["issue"] else ""
            print("  %s%s  %s" % (r["check"], tag, r["detail"]))
        if len(rows) > 60:
            print("  ... %d more in the JSON report" % (len(rows) - 60))
    total = {k: sum(1 for r in res.rows if r["outcome"] == k) for k in ("pass", "fail", "xfail", "xpass")}
    print("\nTOTAL %(pass)d pass, %(fail)d fail, %(xfail)d xfail, %(xpass)d xpass" % total)
    if out_path:
        with open(out_path, "w") as f:
            json.dump({"summary": total, "checks": res.rows}, f, indent=1)
        print("report written to %s" % out_path)
    return 1 if res.unexpected(strict) else 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument("--spawn", metavar="BINARY", help="start this kosha-server binary with a throwaway data dir")
    target.add_argument("--host", help="URL of an already-running server")
    p.add_argument("--api-key", default=os.environ.get("KOSHA_API_KEY", "sk-conformance"))
    p.add_argument(
        "--admin-key",
        default=os.environ.get("KOSHA_ADMIN_API_KEY", "sk-conformance-admin"),
        help="KOSHA_ADMIN_API_KEY of the server (compaction is an admin route)",
    )
    p.add_argument(
        "--tenant",
        default="default",
        help="tenant that --api-key belongs to; namespaces are stored as <tenant>/<ns>",
    )
    p.add_argument("--port", type=int, default=18080, help="port for --spawn")
    p.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres control plane for --spawn. Without it the server keeps manifests in memory, "
        "so a restart forgets the namespace and the 'restarted' checkpoint is skipped",
    )
    p.add_argument("--cold-restart", action="store_true", help="clear the spawned server cache on restart; requires Postgres and S3")
    p.add_argument("--restart-cmd", help="shell command that restarts the --host server (enables the 'restarted' checkpoint)")
    p.add_argument("--namespace", help="default: conformance-<seed>-<unix time>")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--docs", type=int, default=6000)
    p.add_argument("--out", help="write a JSON report here")
    p.add_argument("--keep", action="store_true", help="keep the --spawn data dir and log")
    p.add_argument("--strict", action="store_true", help="also fail when an expected failure passes")
    p.add_argument("--verbose", action="store_true", help="list every expected failure, not just the bugs")
    args = p.parse_args()
    if args.cold_restart and not (args.spawn and args.database_url and os.environ.get("KOSHA_S3_BUCKET")):
        p.error("--cold-restart requires --spawn, --database-url and KOSHA_S3_BUCKET")

    ns = args.namespace or "conformance-%d-%d" % (args.seed, int(time.time()))
    spawned = None
    if args.spawn:
        spawned = Spawned(args.spawn, args.port, args.api_key, args.admin_key, args.database_url, args.cold_restart)
        spawned.start()
        kosha = Kosha("http://127.0.0.1:%d" % args.port, args.api_key, ns, args.admin_key, args.tenant)
        restart = spawned.restart if args.database_url else None
    else:
        kosha = Kosha(args.host, args.api_key, ns, args.admin_key, args.tenant)
        restart = (lambda: subprocess.run(args.restart_cmd, shell=True, check=True)) if args.restart_cmd else None
    try:
        kosha.wait_healthy()
        res = run(args, kosha, restart)
    finally:
        if spawned:
            spawned.cleanup(args.keep)
    return report(res, args.out, args.strict, args.verbose)


if __name__ == "__main__":
    sys.exit(main())
