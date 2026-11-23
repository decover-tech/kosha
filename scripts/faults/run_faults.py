#!/usr/bin/env python3
"""Kosha fault-injection suite (#357): process-level faults against the
ingest + query compose stack, with toxiproxy in front of MinIO and Postgres.

#257 covers crash points inside one process (failpoints, cargo test). This
suite kills and starves the real server processes: a SIGKILLed ingest pod,
S3 that is slow, times out or resets connections, a Postgres that goes away,
and three pods booting against a database nobody has migrated yet.

Every scenario runs a continuous acknowledged-write workload against the
ingest pod in its own namespace and records each acknowledged document id.
A write that got no 200 is "unacked": it may or may not have landed. After
the fault is over and the stack has recovered, the runner re-sends every
unacked batch (upserts are idempotent) until it is acknowledged, flushes,
and checks:

  :acked-present        every acknowledged id exists (exists API, ingest
                        pod): zero acknowledged-write loss
  :doc-count@<pod>      match_all total == every id ever written, exactly,
                        on the ingest pod and (after it catches up) the
                        query pod
  :manifest-committed   every segment of every manifest in kosha.manifests
                        (read from Postgres directly) has its _COMMIT object
                        in MinIO (read directly, past the proxy); a watcher
                        thread also samples this every few seconds while the
                        fault is on (:manifest-during-fault)
  :recovered            the ingest pod acknowledged a write and a flush again
                        within --max-recovery seconds of the fault ending;
                        the times are in the report's "recovery" section

Scenarios (each check id starts with the scenario name):

  kill-mid-flush       buffer --flush-docs documents, start a flush, SIGKILL
                       the ingest pod while the flush is in flight (S3 is
                       slowed with a latency toxic so the upload is), start it
  kill-mid-compaction  --segments flushed segments, start an async full
                       compaction, SIGKILL the ingest pod while it runs
  s3-toxics            latency, then timeouts, then reset_peer on the MinIO
                       proxy, a flush attempted under each; then a SIGKILL
                       restart so the acked writes are checked after one
  postgres-cut         the Postgres proxy disabled for --cut-seconds, then a
                       real Postgres restart (failover approximation), a
                       flush attempted during each; then a SIGKILL restart
  first-boot           a fresh database, three pods (ingest + 2 query)
                       started at the same moment: none may exit, all must be
                       ready, and a write through one is readable on the
                       others (the #330 concurrent-migration race)

Usage (from the repo root; what .github/workflows/faults.yml runs):

  docker compose -f docker-compose.yml \\
      -f scripts/integration/docker-compose.integration.yml \\
      -f scripts/faults/docker-compose.faults.yml up --build -d
  python3 scripts/faults/run_faults.py --out faults-report.json

  # The pure logic (SigV4, manifest parsing, invariants, toxiproxy client,
  # the workload's bookkeeping) against fakes, no Docker:
  python3 scripts/faults/run_faults.py --self-test

Exit status: 0 when every check passed or failed only as listed in
expected_failures.json; 1 otherwise (with --strict, also when a listed
failure passes).

Python 3.9+ standard library only.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import hmac
import importlib.util
import json
import os
import shlex
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
INTEGRATION = os.path.join(os.path.dirname(HERE), "integration")


def _load_integration():
    """scripts/integration/run.py, loaded by path (it is also `run.py`). It
    brings the conformance client, Results and report() with it."""
    spec = importlib.util.spec_from_file_location("integration_run", os.path.join(INTEGRATION, "run.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


integ = _load_integration()
conf = integ.conf
Pod = integ.Pod
poll_until = integ.poll_until

COMPOSE = (
    "docker compose -f docker-compose.yml"
    " -f scripts/integration/docker-compose.integration.yml"
    " -f scripts/faults/docker-compose.faults.yml"
)


# ─── HTTP ───────────────────────────────────────────────────────────────────


def http_json(method: str, url: str, body=None, key: Optional[str] = None, timeout: float = 30, headers: Optional[Dict[str, str]] = None) -> Tuple[Optional[int], object]:
    """One request, no retries. Status None on a connection error/timeout."""
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    if key:
        req.add_header("Authorization", "Bearer " + key)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw, status = resp.read(), resp.status
    except urllib.error.HTTPError as e:
        raw, status = e.read(), e.code
    except Exception as e:  # refused, reset, timed out
        return None, {"error": "%s: %s" % (type(e).__name__, e)}
    try:
        return status, json.loads(raw or b"null")
    except ValueError:
        return status, {"raw": raw.decode(errors="replace")[:500]}


# ─── S3 (SigV4, enough for HEAD) ────────────────────────────────────────────


def sigv4_headers(method: str, url: str, region: str, access_key: str, secret_key: str, now: datetime.datetime, extra: Optional[Dict[str, str]] = None, payload_hash: Optional[str] = None, service: str = "s3") -> Dict[str, str]:
    """AWS Signature Version 4 headers for a request with an empty body (or
    `payload_hash`). `url` must already be URI-encoded."""
    parts = urllib.parse.urlsplit(url)
    payload_hash = payload_hash or hashlib.sha256(b"").hexdigest()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    headers = {"host": parts.netloc, "x-amz-content-sha256": payload_hash, "x-amz-date": amz_date}
    for k, v in (extra or {}).items():
        headers[k.lower()] = v
    names = sorted(headers)
    query = "&".join(
        "%s=%s" % (urllib.parse.quote(k, safe="-_.~"), urllib.parse.quote(v, safe="-_.~"))
        for k, v in sorted(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
    )
    canonical = "\n".join(
        [method, parts.path or "/", query]
        + ["%s:%s" % (n, " ".join(headers[n].strip().split())) for n in names]
        + ["", ";".join(names), payload_hash]
    )
    scope = "%s/%s/%s/aws4_request" % (day, region, service)
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])

    def h(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode(), hashlib.sha256).digest()

    k = h(h(h(h(("AWS4" + secret_key).encode(), day), region), service), "aws4_request")
    signature = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
    out = {n: headers[n] for n in names if n != "host"}
    out["Authorization"] = "AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s" % (access_key, scope, ";".join(names), signature)
    return out


class S3:
    """HEAD objects in a path-style bucket (MinIO, read past the proxy)."""

    def __init__(self, endpoint: str, bucket: str, access_key: str, secret_key: str, region: str = "us-east-1"):
        self.endpoint = endpoint.rstrip("/")
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region

    def head(self, key: str) -> Optional[int]:
        """HTTP status of HEAD <key> (200 present, 404 absent), None when S3
        could not be reached."""
        url = "%s/%s/%s" % (self.endpoint, self.bucket, urllib.parse.quote(key, safe="/-_.~"))
        hdrs = sigv4_headers("HEAD", url, self.region, self.access_key, self.secret_key, datetime.datetime.now(datetime.timezone.utc))
        req = urllib.request.Request(url, method="HEAD")
        for k, v in hdrs.items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status
        except urllib.error.HTTPError as e:
            return e.code
        except Exception:
            return None


# ─── manifests ──────────────────────────────────────────────────────────────


def join_key(prefix: str, path: str) -> str:
    """kosha-server's join_s3_key: `{prefix}/{path}` without doubled slashes."""
    path = path.lstrip("/")
    return path if not prefix else "%s/%s" % (prefix.rstrip("/"), path)


def commit_keys(namespace: str, segments_json: str, prefix: str) -> List[str]:
    """The S3 key of the `_COMMIT` of every segment a manifest row lists.
    A row is either a Manifest object or (pre-Manifest) a bare list of
    entries. A branch's inherited segments live under their home namespace."""
    doc = json.loads(segments_json)
    if isinstance(doc, list):
        entries, homes = doc, {}
    else:
        entries = doc.get("segments") or []
        homes = ((doc.get("branch") or {}).get("segment_homes")) or {}
    keys = []
    for e in entries:
        seg = e["segment_id"]
        home = homes.get(seg, namespace)
        keys.append(join_key(prefix, "%s/%s/_COMMIT" % (home, seg)))
    return keys


def parse_psql_rows(out: str) -> List[Tuple[str, int, str]]:
    """`psql -At -F '|'` rows of namespace_id|version|segments_json."""
    rows = []
    for line in out.splitlines():
        if not line.strip():
            continue
        ns, version, segments = line.split("|", 2)
        rows.append((ns, int(version), segments))
    return rows


def manifest_problems(rows: Iterable[Tuple[str, int, str]], prefix: str, head: Callable[[str], Optional[int]], known_good: Optional[Set[str]] = None) -> Tuple[int, List[str]]:
    """(segments checked, problems) for manifest rows: each listed segment
    must have a `_COMMIT`. `known_good` caches keys already seen present
    (segments are immutable; the final check passes None to re-check all)."""
    checked, problems = 0, []
    for ns, version, segments_json in rows:
        try:
            keys = commit_keys(ns, segments_json, prefix)
        except (ValueError, KeyError, TypeError) as e:
            problems.append("%s v%d: unparseable manifest (%s)" % (ns, version, e))
            continue
        for key in keys:
            checked += 1
            if known_good is not None and key in known_good:
                continue
            status = head(key)
            if status == 200:
                if known_good is not None:
                    known_good.add(key)
            elif status is None:
                problems.append("%s v%d: S3 unreachable for %s" % (ns, version, key))
            else:
                problems.append("%s v%d: %s -> HTTP %s" % (ns, version, key, status))
    return checked, problems


# ─── toxiproxy ──────────────────────────────────────────────────────────────


class Toxiproxy:
    def __init__(self, api: str):
        self.api = api.rstrip("/")

    def _call(self, method: str, path: str, body=None) -> object:
        status, out = http_json(method, self.api + path, body, timeout=10)
        if status is None or status >= 300:
            raise RuntimeError("toxiproxy %s %s -> %s %s" % (method, path, status, out))
        return out

    def reset(self) -> None:
        """Re-enable every proxy and remove every toxic."""
        self._call("POST", "/reset", {})

    def proxies(self) -> Dict:
        return self._call("GET", "/proxies")

    def add_toxic(self, proxy: str, name: str, kind: str, attributes: Dict, stream: str = "downstream", toxicity: float = 1.0) -> None:
        self._call("POST", "/proxies/%s/toxics" % proxy, {"name": name, "type": kind, "stream": stream, "toxicity": toxicity, "attributes": attributes})

    def remove_toxic(self, proxy: str, name: str) -> None:
        self._call("DELETE", "/proxies/%s/toxics/%s" % (proxy, name))

    def set_enabled(self, proxy: str, enabled: bool) -> None:
        self._call("POST", "/proxies/%s" % proxy, {"enabled": enabled})


# ─── workload ───────────────────────────────────────────────────────────────


def make_doc(doc_id: str, filler: int = 0) -> Dict:
    words = ("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel")
    n = int(doc_id.lstrip("abcdefghijklmnopqrstuvwxyz-") or 0)
    text = " ".join(words[(n * 7 + i) % len(words)] for i in range(12))
    if filler:
        text += " " + ("lorem%d " % (n % 97)) * filler
    return {
        "id": doc_id,
        "fields": [
            {"name": "body", "field_type": "Text", "value": text},
            {"name": "slot", "field_type": "Keyword", "value": doc_id},
            {"name": "bucket", "field_type": "Keyword", "value": "b%d" % (n % 16)},
        ],
    }


class Workload:
    """Acknowledged-write bookkeeping for one namespace, plus a background
    thread that writes a batch every `interval` seconds.

    `acked` holds every id whose write returned 200. `unacked` holds batches
    that did not (refused, reset, timed out, 5xx): they may or may not have
    landed, and `redrive` re-sends them until they are acknowledged."""

    def __init__(self, url: str, key: str, namespace: str, batch: int = 50, interval: float = 0.05, timeout: float = 30, prefix: str = "w", filler: int = 0):
        self.url = url.rstrip("/")
        self.key = key
        self.ns = namespace
        self.batch = batch
        self.interval = interval
        self.timeout = timeout
        self.prefix = prefix
        self.filler = filler
        self.next_id = 0
        self.acked: Set[str] = set()
        self.unacked: List[List[str]] = []
        self.ack_times: List[float] = []
        self.failures: Dict[str, int] = {}
        self.lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def endpoint(self, action: str) -> str:
        return "%s/v1/namespaces/%s/%s" % (self.url, urllib.parse.quote(self.ns, safe=""), action)

    def new_ids(self, count: int) -> List[str]:
        with self.lock:
            ids = ["%s%07d" % (self.prefix, self.next_id + i) for i in range(count)]
            self.next_id += count
        return ids

    def send(self, ids: List[str]) -> bool:
        status, out = http_json("POST", self.endpoint("documents"), {"documents": [make_doc(i, self.filler) for i in ids]}, self.key, self.timeout)
        with self.lock:
            if status == 200:
                self.acked.update(ids)
                self.ack_times.append(time.time())
                return True
            label = "conn" if status is None else str(status)
            self.failures[label] = self.failures.get(label, 0) + 1
            self.unacked.append(ids)
            return False

    def write(self, count: int, chunk: int = 500) -> int:
        """Write `count` new docs in requests of `chunk`; returns how many
        were acknowledged."""
        ok = 0
        for start in range(0, count, chunk):
            ids = self.new_ids(min(chunk, count - start))
            if self.send(ids):
                ok += len(ids)
        return ok

    def _run(self) -> None:
        while not self._stop.is_set():
            if not self.send(self.new_ids(self.batch)):
                self._stop.wait(0.2)  # the pod is down or refusing: back off
            self._stop.wait(self.interval)

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join()

    def first_ack_after(self, t: float) -> Optional[float]:
        with self.lock:
            later = [a for a in self.ack_times if a > t]
        return min(later) - t if later else None

    def all_ids(self) -> Set[str]:
        with self.lock:
            return set(self.acked) | {i for b in self.unacked for i in b}

    def redrive(self, deadline: float) -> Tuple[int, int]:
        """Re-send every unacked batch until acknowledged or `deadline`.
        Returns (batches re-sent, batches still unacknowledged)."""
        with self.lock:
            pending, self.unacked = self.unacked, []
        sent = len(pending)
        while pending and time.time() < deadline:
            ids = pending.pop(0)
            if not self.send(ids):
                with self.lock:
                    pending.append(self.unacked.pop())
                time.sleep(0.5)
        with self.lock:
            self.unacked.extend(pending)
            left = len(self.unacked)
        return sent, left


def missing_ids(exists: Callable[[List[str]], Optional[Set[str]]], ids: Iterable[str], chunk: int = 500) -> Tuple[List[str], int]:
    """(ids the exists API does not report, chunks that errored)."""
    ids = sorted(ids)
    missing: List[str] = []
    errors = 0
    for i in range(0, len(ids), chunk):
        part = ids[i : i + chunk]
        found = exists(part)
        if found is None:
            errors += 1
            missing.extend(part)
        else:
            missing.extend(x for x in part if x not in found)
    return missing, errors


# ─── the suite ──────────────────────────────────────────────────────────────


class ManifestWatcher:
    """Samples the manifest invariant every `every` seconds while a fault is
    on. Postgres/S3 read errors are counted, not failures: the Postgres
    scenario takes the database away on purpose."""

    def __init__(self, check: Callable[[Optional[Set[str]]], Tuple[int, List[str]]], every: float):
        self.check = check
        self.every = every
        self.samples = 0
        self.read_errors = 0
        self.problems: List[str] = []
        self.known: Set[str] = set()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                _, problems = self.check(self.known)
                self.samples += 1
                self.problems.extend("t+%.1fs %s" % (time.time() - self.started, p) for p in problems if "unreachable" not in p)
            except Exception:
                self.read_errors += 1
            self._stop.wait(self.every)

    def __enter__(self) -> "ManifestWatcher":
        self.started = time.time()
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join()


class Suite:
    def __init__(self, args, res):
        self.args = args
        self.res = res
        self.compose = shlex.split(args.compose)
        self.run_id = args.run_id or str(int(time.time()))
        self.ingest = Pod("ingest", args.ingest, args.api_key, "unused", args.admin_key, "default")
        self.query = Pod("query", args.query, args.api_key, "unused", args.admin_key, "default")
        self.tox = Toxiproxy(args.toxiproxy)
        self.s3 = S3(args.s3_endpoint, args.s3_bucket, args.s3_access_key, args.s3_secret_key)
        self.recovery: List[Dict] = []
        self.timings: List[Dict] = []

    # plumbing -----------------------------------------------------------

    def record(self, check: str, ok: bool, detail: str = "") -> None:
        print("  %-48s %s %s" % (check, "ok" if ok else "FAIL", detail[:300]))
        self.res.record(check, bool(ok), detail)

    def sh(self, *argv: str, check: bool = True, env: Optional[Dict[str, str]] = None, capture: bool = False, quiet: bool = False) -> subprocess.CompletedProcess:
        """`<compose> argv...` from the repo root."""
        if not quiet:
            print("  $ compose " + " ".join(argv))
        full_env = dict(os.environ)
        full_env.update(env or {})
        pipe = subprocess.PIPE if capture else None
        return subprocess.run(self.compose + list(argv), cwd=ROOT, check=check, env=full_env, stdout=pipe, stderr=pipe, universal_newlines=True)

    def psql(self, sql: str, db: str = "kosha") -> str:
        out = self.sh("exec", "-T", "postgres", "psql", "-U", "kosha", "-d", db, "-At", "-F", "|", "-c", sql, capture=True, quiet=True)
        return out.stdout

    def manifest_check(self, known: Optional[Set[str]] = None) -> Tuple[int, List[str]]:
        rows = parse_psql_rows(self.psql("SELECT namespace_id, version, segments_json FROM kosha.manifests"))
        return manifest_problems(rows, self.args.s3_prefix, self.s3.head, known)

    def kill(self, service: str) -> float:
        t = time.time()
        self.sh("kill", "-s", "SIGKILL", service)
        return t

    def start(self, service: str) -> None:
        self.sh("start", service)

    def workload(self, scenario: str, **kw) -> Workload:
        ns = "faults-%s-%s" % (scenario, self.run_id)
        return Workload(self.args.ingest, self.args.api_key, ns, batch=self.args.write_batch, interval=self.args.write_interval, **kw)

    def flush(self, w: Workload, timeout: float = 120) -> Tuple[Optional[int], object]:
        return http_json("POST", w.endpoint("flush"), {}, w.key, timeout)

    def flush_until_ok(self, w: Workload, since: float, budget: float) -> Optional[float]:
        """Flush until one succeeds; seconds from `since` to that success."""
        deadline = time.time() + budget
        while time.time() < deadline:
            status, _ = self.flush(w, timeout=max(5.0, deadline - time.time()))
            if status == 200:
                return time.time() - since
            time.sleep(1)
        return None

    def wait_ready(self, since: float) -> Optional[float]:
        took = poll_until(lambda: http_json("GET", self.args.ingest.rstrip("/") + "/v1/readyz", timeout=2)[0] == 200, self.args.max_recovery, 0.2)
        return None if took is None else time.time() - since

    def exists(self, w: Workload) -> Callable[[List[str]], Optional[Set[str]]]:
        def call(ids: List[str]) -> Optional[Set[str]]:
            status, out = http_json("POST", w.endpoint("exists"), {"ids": ids}, w.key, 60)
            return set(out.get("existing", [])) if status == 200 else None

        return call

    def total(self, url: str, w: Workload, consistency: str) -> Optional[int]:
        body = {"query_text": "", "filter": {"match_all": {}}, "max_results": 1, "exact_total_hits": True, "no_cache": True, "consistency": consistency}
        status, out = http_json("POST", "%s/v1/namespaces/%s/search" % (url.rstrip("/"), urllib.parse.quote(w.ns, safe="")), body, w.key, 60)
        return out.get("total_hits") if status == 200 else None

    # invariants ---------------------------------------------------------

    def verify(self, name: str, w: Workload, fault_end: float, rec: Dict) -> None:
        """Recovery, then the invariants: no acked write lost, exact count,
        every manifest segment committed."""
        a = self.args
        rec["first_ack_s"] = w.first_ack_after(fault_end)
        if rec["first_ack_s"] is None:
            # The workload may have stopped before the pod came back.
            took = poll_until(lambda: w.send(w.new_ids(1)), a.max_recovery, 0.5)
            rec["first_ack_s"] = None if took is None else time.time() - fault_end
        rec["first_flush_s"] = self.flush_until_ok(w, fault_end, a.max_recovery)
        ok = rec["first_ack_s"] is not None and rec["first_flush_s"] is not None and max(rec["first_ack_s"], rec["first_flush_s"]) <= a.max_recovery
        self.record("%s:recovered" % name, ok, json.dumps({k: (round(v, 2) if isinstance(v, float) else v) for k, v in rec.items()}))
        w.stop()
        acked = set(w.acked)
        # A pod that never acked again is not going to start now: do not
        # spend another --max-recovery on it.
        sent, left = w.redrive(time.time() + (a.max_recovery if rec["first_ack_s"] is not None else 30))
        rec["unacked_batches"] = sent
        rec["write_failures"] = dict(w.failures)
        self.record("%s:redrive" % name, left == 0, "%d unacked batches re-sent, %d still unacknowledged" % (sent, left))
        status, out = self.flush(w)
        self.record("%s:final-flush" % name, status == 200, "HTTP %s %s" % (status, json.dumps(out)[:200]))

        missing, errors = missing_ids(self.exists(w), acked)
        self.record("%s:acked-present" % name, not missing and not errors, "%d acked ids, %d missing %s, %d exists calls failed" % (len(acked), len(missing), missing[:10], errors))
        everything = w.all_ids()
        rec["acked"] = len(acked)
        rec["written"] = len(everything)
        got = self.total(a.ingest, w, "strong")
        self.record("%s:doc-count@ingest" % name, got == len(everything), "total_hits %s, want %d" % (got, len(everything)))
        seen: List[Optional[int]] = [None]

        def caught_up() -> bool:
            seen[0] = self.total(a.query, w, "eventual")
            return seen[0] == len(everything)

        took = poll_until(caught_up, a.converge_timeout, 0.25)
        self.record("%s:doc-count@query" % name, took is not None, "total_hits %s, want %d" % (seen[0], len(everything)))
        try:
            checked, problems = self.manifest_check()
            self.record("%s:manifest-committed" % name, not problems, "%d segments checked; %s" % (checked, problems[:5]))
        except Exception as e:
            self.record("%s:manifest-committed" % name, False, "could not read: %s" % e)

    def watched(self) -> ManifestWatcher:
        return ManifestWatcher(self.manifest_check, self.args.watch_every)

    def watch_result(self, name: str, watcher: ManifestWatcher) -> None:
        self.record(
            "%s:manifest-during-fault" % name,
            not watcher.problems and watcher.samples > 0,
            "%d samples (%d unreadable): %s" % (watcher.samples, watcher.read_errors, watcher.problems[:5]),
        )

    def restart_check(self, name: str, w: Workload, rec: Dict) -> None:
        """SIGKILL + start the ingest pod, so the acked writes are checked
        after a restart too; records the restart's ready time."""
        t = self.kill("kosha-ingest")
        self.start("kosha-ingest")
        rec["restart_ready_s"] = self.wait_ready(t)
        self.record("%s:restart-ready" % name, rec["restart_ready_s"] is not None, "ready in %s s" % rec["restart_ready_s"])

    # scenarios ----------------------------------------------------------

    def kill_mid_flush(self) -> None:
        name = "kill-mid-flush"
        a = self.args
        w = self.workload(name, filler=a.filler)
        w.write(a.flush_docs)
        rec: Dict = {"scenario": name, "buffered_docs": len(w.acked)}
        w.start()
        flushed: Dict = {}

        def do_flush() -> None:
            flushed["result"] = self.flush(w, timeout=300)
            flushed["at"] = time.time()

        with self.watched() as watcher:
            # Slow S3 so the segment upload is surely in flight at the kill.
            self.tox.add_toxic("minio", "stretch", "latency", {"latency": a.stretch_ms, "jitter": 0})
            flusher = threading.Thread(target=do_flush, daemon=True)
            started = time.time()
            flusher.start()
            time.sleep(a.kill_delay)
            killed = self.kill("kosha-ingest")
            rec["flush_in_flight_at_kill"] = "at" not in flushed or flushed["at"] > killed
            rec["kill_after_flush_start_s"] = round(killed - started, 2)
            self.tox.remove_toxic("minio", "stretch")
            self.start("kosha-ingest")
            rec["ready_s"] = self.wait_ready(killed)
            flusher.join(timeout=10)
            rec["interrupted_flush"] = str(flushed.get("result", ("pending",))[0])
            self.verify(name, w, killed, rec)
        self.watch_result(name, watcher)
        self.recovery.append(rec)

    def kill_mid_compaction(self) -> None:
        name = "kill-mid-compaction"
        a = self.args
        w = self.workload(name)
        for _ in range(a.segments):
            w.write(a.segment_docs)
            status, out = self.flush(w)
            if status != 200:
                raise RuntimeError("setup flush -> %s %s" % (status, out))
        rec: Dict = {"scenario": name, "segments_before": a.segments}
        w.start()
        with self.watched() as watcher:
            self.tox.add_toxic("minio", "stretch", "latency", {"latency": a.stretch_ms, "jitter": 0})
            status, out = http_json(
                "POST", a.ingest.rstrip("/") + "/v1/admin/compact-namespace", {"namespace": "default/" + w.ns, "mode": "full", "async": True}, a.admin_key, 60
            )
            rec["compact_request"] = status
            job = (out or {}).get("job_id") if isinstance(out, dict) else None
            time.sleep(a.kill_delay)
            if job:
                st, js = http_json("GET", "%s/v1/admin/compaction-jobs/%s" % (a.ingest.rstrip("/"), job), key=a.admin_key, timeout=5)
                rec["job_state_at_kill"] = js.get("state") or js.get("status") if isinstance(js, dict) and st == 200 else st
            killed = self.kill("kosha-ingest")
            self.tox.remove_toxic("minio", "stretch")
            self.start("kosha-ingest")
            rec["ready_s"] = self.wait_ready(killed)
            self.verify(name, w, killed, rec)
            # Compaction works again on the recovered pod, and the answers
            # are still exact after it.
            status, out = http_json("POST", a.ingest.rstrip("/") + "/v1/admin/compact-namespace", {"namespace": "default/" + w.ns, "mode": "full"}, a.admin_key, 300)
            self.record("%s:compaction-after-recovery" % name, status == 200, "HTTP %s %s" % (status, json.dumps(out)[:200]))
            got = self.total(a.ingest, w, "strong")
            self.record("%s:doc-count-after-compaction" % name, got == len(w.all_ids()), "total_hits %s, want %d" % (got, len(w.all_ids())))
        self.watch_result(name, watcher)
        self.recovery.append(rec)

    def s3_toxics(self) -> None:
        name = "s3-toxics"
        a = self.args
        w = self.workload(name)
        w.write(a.segment_docs)
        rec: Dict = {"scenario": name, "phases": []}
        w.start()
        phases = [
            ("latency", {"latency": 1000, "jitter": 250}, 1.0),
            ("timeout", {"timeout": 2000}, 1.0),
            ("reset_peer", {"timeout": 0}, 0.5),
        ]
        with self.watched() as watcher:
            for kind, attrs, toxicity in phases:
                self.tox.add_toxic("minio", kind, kind, attrs, toxicity=toxicity)
                t = time.time()
                w.write(a.segment_docs // 4)
                status, _ = self.flush(w, timeout=a.toxic_seconds + 60)
                left = a.toxic_seconds - (time.time() - t)
                if left > 0:
                    time.sleep(left)
                self.tox.remove_toxic("minio", kind)
                acks = len([x for x in w.ack_times if x >= t])
                rec["phases"].append({"toxic": kind, "flush_status": status, "acks_during": acks, "seconds": round(time.time() - t, 1)})
            fault_end = time.time()
            self.verify(name, w, fault_end, rec)
        self.watch_result(name, watcher)
        self.restart_check(name, w, rec)
        self.after_restart(name, w)
        self.recovery.append(rec)

    def postgres_cut(self) -> None:
        name = "postgres-cut"
        a = self.args
        w = self.workload(name)
        w.write(a.segment_docs)
        rec: Dict = {"scenario": name}
        w.start()
        with self.watched() as watcher:
            # 1. Connection cut: the proxy refuses and drops connections.
            self.tox.set_enabled("postgres", False)
            t = time.time()
            status, _ = self.flush(w, timeout=a.cut_seconds + 30)
            rec["flush_during_cut"] = status
            left = a.cut_seconds - (time.time() - t)
            if left > 0:
                time.sleep(left)
            self.tox.set_enabled("postgres", True)
            cut_end = time.time()
            rec["cut_first_flush_s"] = self.flush_until_ok(w, cut_end, a.max_recovery)
            rec["cut_first_ack_s"] = w.first_ack_after(cut_end)
            self.record("%s:cut-recovered" % name, rec["cut_first_flush_s"] is not None, "flush ok %s s after reconnect" % rec["cut_first_flush_s"])
            # 2. A real restart of Postgres: every pooled connection dies and
            # the server is briefly not accepting (failover approximation).
            w.write(a.segment_docs // 4)
            self.sh("restart", "-t", "5", "postgres")
            restarted = time.time()
            self.verify(name, w, restarted, rec)
        self.watch_result(name, watcher)
        self.restart_check(name, w, rec)
        self.after_restart(name, w)
        self.recovery.append(rec)

    def after_restart(self, name: str, w: Workload) -> None:
        # Writes still unacknowledged before the restart (a pod that stopped
        # accepting them) get another chance on the fresh process.
        # The flush retries: a fresh pod refuses writes (503) until it holds
        # the namespace's writer lease.
        t = time.time()
        _, left = w.redrive(t + self.args.max_recovery)
        took = self.flush_until_ok(w, t, self.args.max_recovery)
        self.record("%s:redrive-after-restart" % name, left == 0 and took is not None, "%d batches still unacknowledged, flushed after %s s" % (left, None if took is None else round(took, 2)))
        missing, errors = missing_ids(self.exists(w), w.acked)
        self.record("%s:acked-present-after-restart" % name, not missing and not errors, "%d missing %s" % (len(missing), missing[:10]))
        want = len(w.all_ids())
        seen: List[Optional[int]] = [None]

        def ok() -> bool:
            seen[0] = self.total(self.args.ingest, w, "strong")
            return seen[0] == want

        poll_until(ok, 30, 0.5)
        self.record("%s:doc-count-after-restart" % name, seen[0] == want, "total_hits %s, want %d" % (seen[0], want))

    def first_boot(self) -> None:
        name = "first-boot"
        a = self.args
        db = "kosha_firstboot_%s" % self.run_id
        env = {"KOSHA_FIRSTBOOT_DB": db}
        pods = ["kosha-boot-1", "kosha-boot-2", "kosha-boot-3"]
        urls = {"kosha-boot-1": "http://localhost:8091", "kosha-boot-2": "http://localhost:8092", "kosha-boot-3": "http://localhost:8093"}
        self.sh("--profile", "first-boot", "rm", "--stop", "--force", *pods, env=env, check=False)
        self.sh("exec", "-T", "postgres", "createdb", "-U", "kosha", db)
        rec: Dict = {"scenario": name, "database": db}
        rows_before = len(self.res.rows)
        t = time.time()
        # One `up` starts the three containers concurrently.
        self.sh("--profile", "first-boot", "up", "-d", "--no-deps", *pods, env=env)
        ready: Dict[str, Optional[float]] = {}

        def wait(p: str) -> None:
            took = poll_until(lambda: http_json("GET", urls[p] + "/v1/readyz", timeout=2)[0] == 200, a.boot_timeout, 0.25)
            ready[p] = None if took is None else round(time.time() - t, 2)

        threads = [threading.Thread(target=wait, args=(p,)) for p in pods]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        rec["ready_s"] = ready
        for p in pods:
            out = subprocess.run(["docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}} {{.RestartCount}}"] + self.container(p, env), stdout=subprocess.PIPE, universal_newlines=True)
            state = out.stdout.strip()
            self.record("%s:running@%s" % (name, p), state.startswith("running"), "state/exit/restarts: %s" % state)
            self.record("%s:ready@%s" % (name, p), ready.get(p) is not None, "ready after %s s" % ready.get(p))
        if all(ready.get(p) is not None for p in pods):
            w = Workload(urls["kosha-boot-1"], a.api_key, "firstboot-%s" % self.run_id)
            w.write(200)
            status, _ = self.flush(w)
            for p in pods[1:]:
                seen: List[Optional[int]] = [None]

                def ok(url=urls[p]) -> bool:
                    seen[0] = self.total(url, w, "eventual")
                    return seen[0] == len(w.acked)

                poll_until(ok, a.converge_timeout, 0.25)
                self.record("%s:read-after-write@%s" % (name, p), status == 200 and seen[0] == len(w.acked) == 200, "flush %s, total_hits %s of %d acked" % (status, seen[0], len(w.acked)))
        else:
            for p in pods[1:]:
                self.record("%s:read-after-write@%s" % (name, p), False, "not every pod became ready")
        self.recovery.append(rec)
        if any(r["outcome"] not in ("pass", "xpass") for r in self.res.rows[rows_before:]):
            # The pods are removed below; their logs go to the job log.
            self.sh("--profile", "first-boot", "logs", "--no-color", "--tail", "40", *pods, env=env, check=False)
        if not a.keep_boot_pods:
            self.sh("--profile", "first-boot", "rm", "--stop", "--force", *pods, env=env, check=False)

    def container(self, service: str, env: Dict[str, str]) -> List[str]:
        out = self.sh("--profile", "first-boot", "ps", "-a", "-q", service, env=env, check=False, capture=True)
        return out.stdout.split() or [service]

    def run(self) -> None:
        steps = [
            ("kill-mid-flush", self.kill_mid_flush),
            ("kill-mid-compaction", self.kill_mid_compaction),
            ("s3-toxics", self.s3_toxics),
            ("postgres-cut", self.postgres_cut),
            ("first-boot", self.first_boot),
        ]
        only = set(self.args.only.split(",")) if self.args.only else None
        self.tox.reset()
        for key, fn in steps:
            if only and key not in only:
                continue
            print("\nscenario %s" % key)
            t = time.time()
            error = None
            try:
                fn()
            except Exception as e:
                error = "%s: %s" % (type(e).__name__, e)
                traceback.print_exc()
                self.record("%s:error" % key, False, error)
            finally:
                try:
                    self.tox.reset()
                except Exception as e:
                    print("  toxiproxy reset failed: %s" % e)
                # Leave the stack up for the next scenario.
                for svc in ("kosha-ingest", "kosha-query"):
                    if http_json("GET", (self.args.ingest if svc == "kosha-ingest" else self.args.query).rstrip("/") + "/v1/readyz", timeout=2)[0] != 200:
                        self.sh("up", "-d", "--no-deps", svc, check=False)
                Pod("x", self.args.ingest, "", "", "", "").wait_healthy(timeout=180)
            self.timings.append({"scenario": key, "seconds": round(time.time() - t, 1), "error": error})


# ─── self-test ──────────────────────────────────────────────────────────────


def self_test() -> int:
    """The pure logic, against fakes: SigV4 (AWS's published example),
    manifest rows -> _COMMIT keys -> problems, the toxiproxy client, the
    workload's acked/unacked bookkeeping and redrive, the exists checker,
    expected failures and the report."""
    import http.server
    import tempfile

    failures: List[str] = []

    def check(name: str, ok: bool, detail="") -> None:
        print("  %-44s %s %s" % (name, "ok" if ok else "FAIL", detail if not ok else ""))
        if not ok:
            failures.append(name)

    # SigV4: the "GET Object" example of the S3 SigV4 documentation.
    hdrs = sigv4_headers(
        "GET",
        "https://examplebucket.s3.amazonaws.com/test.txt",
        "us-east-1",
        "AKIAIOSFODNN7EXAMPLE",
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        datetime.datetime(2013, 5, 24),
        extra={"Range": "bytes=0-9"},
    )
    check(
        "sigv4 matches the AWS example",
        hdrs["Authorization"].endswith("Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
        and "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date" in hdrs["Authorization"],
        hdrs["Authorization"],
    )

    check("join_key", join_key("kosha/", "/a/b") == "kosha/a/b" and join_key("", "a") == "a" and join_key("p", "a") == "p/a")
    manifest = json.dumps({"version": 3, "segments": [{"segment_id": "s1", "doc_count": 3}, {"segment_id": "s2", "doc_count": 1}], "branch": {"parent": "default/p", "parent_version": 1, "segment_homes": {"s1": "default/p"}}})
    check("commit_keys (branch homes)", commit_keys("default/b", manifest, "kosha/") == ["kosha/default/p/s1/_COMMIT", "kosha/default/b/s2/_COMMIT"])
    check("commit_keys (legacy list)", commit_keys("t/n", '[{"segment_id":"x","doc_count":1}]', "") == ["t/n/x/_COMMIT"])
    rows = parse_psql_rows("t/a|4|%s\n\nt/b|1|[]\n" % manifest.replace("default/b", "t/a"))
    check("parse_psql_rows keeps | in json", len(rows) == 2 and rows[0][1] == 4 and json.loads(rows[0][2])["version"] == 3)
    present = {"kosha/default/p/s1/_COMMIT"}
    heads: List[str] = []

    def head(key: str) -> Optional[int]:
        heads.append(key)
        return 200 if key in present else 404

    checked, problems = manifest_problems([("default/b", 3, manifest)], "kosha/", head)
    check("manifest_problems flags a missing _COMMIT", checked == 2 and len(problems) == 1 and "s2/_COMMIT -> HTTP 404" in problems[0], problems)
    known: Set[str] = set()
    manifest_problems([("default/b", 3, manifest)], "kosha/", head, known)
    n = len(heads)
    manifest_problems([("default/b", 3, manifest)], "kosha/", head, known)
    check("known_good skips re-HEADs", len(heads) == n + 1 and known == present)
    _, problems = manifest_problems([("x", 1, "{not json")], "", head)
    check("unparseable manifest is a problem", len(problems) == 1 and "unparseable" in problems[0])

    # A fake kosha + toxiproxy in one server.
    state = {"docs": set(), "fail_next": 0, "toxics": [], "enabled": None, "requests": 0}

    class Fake(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code: int, body) -> None:
            raw = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("content-length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_DELETE(self):
            state["toxics"] = [t for t in state["toxics"] if not self.path.endswith("/" + t)]
            self._send(200, {})

        def do_GET(self):
            self._send(200, {"minio": {}, "postgres": {}})

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers.get("content-length") or 0)) or b"{}")
            state["requests"] += 1
            if self.path == "/reset":
                state["toxics"], state["enabled"] = [], None
                return self._send(200, {})
            if self.path.endswith("/toxics"):
                state["toxics"].append(body["name"])
                return self._send(200, body)
            if self.path == "/proxies/postgres":
                state["enabled"] = body["enabled"]
                return self._send(200, body)
            if self.path.endswith("/documents"):
                if state["fail_next"] > 0:
                    state["fail_next"] -= 1
                    return self._send(503, {"error": "lease"})
                state["docs"].update(d["id"] for d in body["documents"])
                return self._send(200, {"indexed": len(body["documents"])})
            if self.path.endswith("/exists"):
                ids = body["ids"]
                return self._send(200, {"existing": [i for i in ids if i in state["docs"] and i != "w0000003"], "missing": []})
            self._send(404, {})

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Fake)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = "http://127.0.0.1:%d" % server.server_port

    tox = Toxiproxy(url)
    tox.add_toxic("minio", "lat", "latency", {"latency": 10})
    tox.set_enabled("postgres", False)
    check("toxiproxy add + disable", state["toxics"] == ["lat"] and state["enabled"] is False)
    tox.remove_toxic("minio", "lat")
    tox.reset()
    check("toxiproxy remove + reset", state["toxics"] == [] and state["enabled"] is None)

    w = Workload(url, "k", "ns/with slash", batch=5, interval=0)
    check("namespace is URL-encoded", w.endpoint("flush").endswith("/v1/namespaces/ns%2Fwith%20slash/flush"))
    w.write(10, chunk=5)
    state["fail_next"] = 2
    t0 = time.time()
    w.write(10, chunk=5)
    check("acked vs unacked", len(w.acked) == 10 and len(w.unacked) == 2 and w.failures == {"503": 2} and len(w.all_ids()) == 20, (len(w.acked), w.unacked, w.failures))
    check("first_ack_after", w.first_ack_after(t0) is None and w.first_ack_after(0) is not None)
    state["fail_next"] = 1
    sent, left = w.redrive(time.time() + 10)
    check("redrive until acked", sent == 2 and left == 0 and len(w.acked) == 20 and state["docs"] >= w.all_ids())
    state["fail_next"] = 10**6
    sent, left = w.redrive(time.time() + 10)
    check("redrive with nothing pending", sent == 0 and left == 0)
    w.write(5, chunk=5)
    sent, left = w.redrive(time.time() + 0.6)
    check("redrive gives up at the deadline", sent == 1 and left == 1 and len(w.unacked) == 1)
    state["fail_next"] = 0
    w.start()
    time.sleep(0.2)
    w.stop()
    check("background writer acks", len(w.acked) > 20)

    exists = lambda ids: set(http_json("POST", w.endpoint("exists"), {"ids": ids}, "k")[1]["existing"])  # noqa: E731
    missing, errors = missing_ids(exists, w.acked, chunk=7)
    check("missing_ids finds the lost id", missing == ["w0000003"] and errors == 0, missing)
    missing, errors = missing_ids(lambda ids: None, ["a", "b", "c"], chunk=2)
    check("missing_ids counts failed calls", missing == ["a", "b", "c"] and errors == 2)
    check("http_json on a dead port", http_json("GET", "http://127.0.0.1:9/x", timeout=1)[0] is None)
    server.shutdown()

    watcher_calls: List[int] = []

    def fake_check(known):
        watcher_calls.append(1)
        if len(watcher_calls) == 2:
            raise RuntimeError("postgres down")
        return 1, (["x v1: k -> HTTP 404"] if len(watcher_calls) == 3 else [])

    with ManifestWatcher(fake_check, 0.01) as watcher:
        poll_until(lambda: len(watcher_calls) >= 4, 5, 0.01)
    check("watcher samples, tolerates read errors", watcher.read_errors == 1 and len(watcher.problems) == 1 and watcher.samples >= 3)

    load_expected()
    # A list of its own: expected_failures.json is empty when no bug is open.
    res = conf.Results({"s3-toxics:recovered": "#0"}, {"#0": "an example of a listed bug"})
    res.record("s3-toxics:recovered", False, "writes fenced")
    res.record("kill-mid-flush:acked-present", True)
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "r.json")
        code = conf.report(res, path, False, False)
        extend_report(path, [{"scenario": "x", "seconds": 1.0, "error": None}], [{"scenario": "x", "ready_s": 1.5}], {"run_id": "1"})
        with open(path) as f:
            rep = json.load(f)
        res.record("kill-mid-flush:doc-count@ingest", False, "lost")
        code2 = conf.report(res, None, False, False)
    check("a listed failure is expected", code == 0 and rep["summary"]["xfail"] == 1, rep["summary"])
    check("report carries recovery + timings", rep["recovery"][0]["ready_s"] == 1.5 and rep["scenarios"][0]["scenario"] == "x" and rep["config"]["run_id"] == "1")
    check("an unlisted failure fails the run", code2 == 1)
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


def extend_report(path: Optional[str], timings: List[Dict], recovery: List[Dict], config: Dict) -> None:
    if not path:
        return
    with open(path) as f:
        rep = json.load(f)
    rep["scenarios"] = timings
    rep["recovery"] = recovery
    rep["config"] = config
    with open(path, "w") as f:
        json.dump(rep, f, indent=1, default=str)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--self-test", action="store_true", help="test the pure logic against fakes and exit")
    p.add_argument("--ingest", default="http://localhost:8080")
    p.add_argument("--query", default="http://localhost:8081")
    p.add_argument("--toxiproxy", default="http://localhost:8474", help="toxiproxy API")
    p.add_argument("--compose", default=COMPOSE, help="compose command prefix, run from the repo root")
    p.add_argument("--api-key", default="sk-kosha-dev")
    p.add_argument("--admin-key", default="sk-kosha-admin-dev")
    p.add_argument("--s3-endpoint", default="http://localhost:9000", help="MinIO, direct (not through the proxy)")
    p.add_argument("--s3-bucket", default="dsearch-dev")
    p.add_argument("--s3-prefix", default="kosha/")
    p.add_argument("--s3-access-key", default="kosha")
    p.add_argument("--s3-secret-key", default="kosha-dev-secret")
    p.add_argument("--only", help="comma-separated scenarios, e.g. kill-mid-flush,first-boot")
    p.add_argument("--run-id", help="suffix of namespace and database names (default: unix time)")
    p.add_argument("--write-batch", type=int, default=50, help="docs per background write")
    p.add_argument("--write-interval", type=float, default=0.05, help="seconds between background writes")
    p.add_argument("--flush-docs", type=int, default=40000, help="kill-mid-flush: buffered docs before the flush")
    p.add_argument("--filler", type=int, default=20, help="kill-mid-flush: extra words per doc (bigger segment)")
    p.add_argument("--segments", type=int, default=8, help="kill-mid-compaction: segments to compact")
    p.add_argument("--segment-docs", type=int, default=2000, help="docs per setup segment")
    p.add_argument("--kill-delay", type=float, default=0.75, help="seconds between starting the flush/compaction and the SIGKILL")
    p.add_argument("--stretch-ms", type=int, default=150, help="S3 latency while a flush/compaction is being killed")
    p.add_argument("--toxic-seconds", type=float, default=8, help="s3-toxics: seconds per toxic")
    p.add_argument("--cut-seconds", type=float, default=10, help="postgres-cut: seconds the proxy is disabled")
    p.add_argument("--max-recovery", type=float, default=120, help="seconds the ingest pod may take to ack and flush again")
    p.add_argument("--converge-timeout", type=float, default=60, help="seconds the query pod may take to show the final count")
    p.add_argument("--boot-timeout", type=float, default=90, help="first-boot: seconds for each pod to become ready")
    p.add_argument("--watch-every", type=float, default=2, help="seconds between manifest samples during a fault")
    p.add_argument("--keep-boot-pods", action="store_true")
    p.add_argument("--out", help="write the JSON report here")
    p.add_argument("--strict", action="store_true", help="also fail when an expected failure passes")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()
    if args.self_test:
        return self_test()

    known = load_expected()
    res = conf.Results(known["checks"], known["bugs"])
    suite = Suite(args, res)
    started = time.time()
    Pod("ingest", args.ingest, "", "", "", "").wait_healthy(timeout=180)
    Pod("query", args.query, "", "", "", "").wait_healthy(timeout=180)
    print("run %s: ingest %s, query %s, toxiproxy %s" % (suite.run_id, args.ingest, args.query, args.toxiproxy))
    suite.run()
    print("\nscenarios: " + ", ".join("%s %.0fs%s" % (t["scenario"], t["seconds"], " (error)" if t["error"] else "") for t in suite.timings) + "; total %.0fs" % (time.time() - started))
    print("recovery:")
    for rec in suite.recovery:
        print("  " + json.dumps(rec, default=str))
    code = conf.report(res, args.out, args.strict, args.verbose)
    config = {k: v for k, v in vars(args).items() if k not in ("s3_secret_key",)}
    config["run_id"] = suite.run_id
    extend_report(args.out, suite.timings, suite.recovery, config)
    return code


if __name__ == "__main__":
    sys.exit(main())
