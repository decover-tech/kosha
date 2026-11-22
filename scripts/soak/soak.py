#!/usr/bin/env python3
"""Kosha soak driver (#357): mixed multi-tenant ingest + query for a fixed
duration, with pass/fail thresholds.

Layout: one large namespace plus N namespaces whose sizes follow a Zipf
distribution, spread round-robin over the tenants given by --tenant-keys.
The driver preloads them, then for --duration seconds runs, concurrently:

  writers     upserts of new ids, re-upserts of live ids and deletes by id,
              at --docs-per-s in total, the namespace picked by size. Each
              writer thread owns the ids with id % writers == its index, so
              the driver knows every namespace's exact live set without
              ordering problems. A write that is not acknowledged is retried
              (upserts and deletes by id are idempotent) for --write-retry-s;
              a namespace whose write never got an ack is "uncertain" and
              fails the run.
  queries     BM25, kNN and filtered BM25 searches, open loop at a rate that
              ramps from --qps-start to --qps-end, split by --mix, spread
              over the --query-url pods, the namespace picked by size
  visibility  one fresh insert every --visible-every seconds is polled for
              on the query pods until an eventual-consistency search finds
              it: write-to-visible latency
  sampler     every --sample-every seconds: /metrics of every pod (queue
              depth, cache bytes, RSS when exported), the large namespace's
              stats and every tenant's namespace listing (segment counts)
  flusher     with --flush-every, flushes the namespaces written since the
              last pass (leave 0 when the server flushes on its own)
  compactor   with --compact-every, asks for a tiered compaction of the
              large namespace and a few written ones (leave 0 when the server
              compacts in the background)
  replacer    with --replace-cmd and --replace-every, runs a command (e.g.
              deleting a query pod) on that period, as the 24h run's hourly
              pod replacement
  flooder     with --flood-tenant-key, a noisy neighbour (#341): extra
              searches as that tenant at --flood-multiplier times its
              configured rate (--flood-qps). Its requests and 429s are
              counted apart from everything else, the other ("victim")
              tenants' query latency is reported on its own, and
              --victim-p99-ms bounds it

At the end it flushes everything and checks that every namespace's
match_all count is exactly its live set, on the write pod (strong) and on
every query pod (eventual, after it catches up).

Reported: p50/p99 latency per query class (overall and per --window),
write-to-visible p50/p99, 5xx and connection-error rates, 429s, segment
counts over time, and every threshold's verdict.

Usage:

  # 5-minute smoke against the compose stack (what faults.yml runs):
  python3 scripts/soak/soak.py --smoke \\
      --write-url http://localhost:8080 --query-url http://localhost:8081
  # The 24h run against the stage cluster: see scripts/soak/README.md.
  # Pure logic plus a short run against an in-process fake server:
  python3 scripts/soak/soak.py --self-test

Exit status: 0 when every threshold held, 1 otherwise. Python 3.9+
standard library only.
"""

from __future__ import annotations

import argparse
import array
import json
import math
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional, Tuple

VOCAB = (
    "contract breach notice damages delivery supplier warranty claim court appeal merger review patent license "
    "invoice payment dispute arbitration clause termination employee policy privacy data security audit filing "
    "motion order ruling evidence witness deposition exhibit counsel settlement statute regulation"
).split()
CATEGORIES = 10
SMOKE = {
    "duration": 300,
    "namespaces": 20,
    "large_docs": 20000,
    "zipf_docs": 10000,
    "docs_per_s": 300,
    "qps_start": 4,
    "qps_end": 12,
    "flush_every": 2,
    "compact_every": 60,
    "sample_every": 10,
    "window": 60,
}
FULL = {
    "duration": 86400,
    "namespaces": 1000,
    "large_docs": 10_000_000,
    "zipf_docs": 2_000_000,
    "docs_per_s": 1500,
    "qps_start": 8,
    "qps_end": 32,
    "flush_every": 0,
    "compact_every": 0,
    "sample_every": 60,
    "window": 600,
}


# ─── HTTP ───────────────────────────────────────────────────────────────────


def http_json(method: str, url: str, body=None, key: Optional[str] = None, timeout: float = 30) -> Tuple[Optional[int], object]:
    """One request. Status None on a connection error or timeout."""
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    if key:
        req.add_header("Authorization", "Bearer " + key)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw, status = resp.read(), resp.status
    except urllib.error.HTTPError as e:
        raw, status = e.read(), e.code
    except Exception as e:
        return None, {"error": "%s: %s" % (type(e).__name__, e)}
    try:
        return status, json.loads(raw or b"null")
    except ValueError:
        return status, raw.decode(errors="replace")


def http_text(url: str, timeout: float = 10, key: Optional[str] = None) -> Optional[str]:
    req = urllib.request.Request(url)
    if key:
        req.add_header("Authorization", "Bearer " + key)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode(errors="replace")
    except Exception:
        return None


# ─── pure helpers ───────────────────────────────────────────────────────────


def parse_tenant_keys(spec: str) -> List[Tuple[str, str]]:
    """`key=tenant,key2=tenant2` -> [(key, tenant), ...]."""
    out = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        key, _, tenant = part.partition("=")
        if not key or not tenant:
            raise ValueError("bad --tenant-keys entry %r (want key=tenant)" % part)
        out.append((key, tenant))
    if not out:
        raise ValueError("--tenant-keys is empty")
    return out


def zipf_sizes(n: int, total: int, s: float) -> List[int]:
    """`n` sizes, rank r proportional to 1/r^s, summing to about `total`,
    each at least 1."""
    if n <= 0:
        return []
    weights = [1.0 / (r ** s) for r in range(1, n + 1)]
    z = sum(weights)
    return [max(1, int(round(total * w / z))) for w in weights]


def percentile(values, p: float) -> Optional[float]:
    if not len(values):
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(math.ceil(p / 100.0 * len(s))) - 1))
    return s[k]


def ramp(elapsed: float, duration: float, start: float, end: float) -> float:
    """Linear ramp from `start` to `end` over `duration`."""
    if duration <= 0:
        return end
    f = min(1.0, max(0.0, elapsed / duration))
    return start + (end - start) * f


def parse_prometheus(text: str) -> Dict[str, float]:
    """Prometheus text exposition -> {"name{labels}": value}. Comments and
    unparseable lines are skipped."""
    out: Dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "}" in line:
            name, _, rest = line.rpartition("}")
            name += "}"
        else:
            name, _, rest = line.partition(" ")
        fields = rest.split()
        if not fields:
            continue
        try:
            out[name.strip()] = float(fields[0])
        except ValueError:
            continue
    return out


SAMPLED_METRICS = (
    "kosha_search_queue_depth",
    'kosha_cache_bytes{cache="segment"}',
    'kosha_cache_bytes{cache="nvme"}',
    "process_resident_memory_bytes",
    "kosha_index_documents_total",
    "kosha_flush_total",
    "kosha_writer_fenced_total",
)


def pick_metrics(m: Dict[str, float]) -> Dict[str, float]:
    return {k: m[k] for k in SAMPLED_METRICS if k in m}


def vector_for(n: int, dim: int) -> List[float]:
    rng = random.Random(n * 7919 + 17)
    v = [rng.uniform(-1, 1) for _ in range(dim)]
    norm = math.sqrt(sum(x * x for x in v)) or 1.0
    return [round(x / norm, 5) for x in v]


def make_doc(n: int, rev: int, dim: int) -> Dict:
    rng = random.Random(n * 31 + rev)
    doc_id = "d%d" % n
    return {
        "id": doc_id,
        "fields": [
            {"name": "body", "field_type": "Text", "value": " ".join(rng.choice(VOCAB) for _ in range(16))},
            {"name": "cat", "field_type": "Keyword", "value": "c%d" % (n % CATEGORIES)},
            {"name": "slot", "field_type": "Keyword", "value": doc_id},
            {"name": "rev", "field_type": "Keyword", "value": str(rev)},
            {"name": "emb", "field_type": "Vector", "value": json.dumps(vector_for(n, dim))},
        ],
    }


class Bitmap:
    """Live flags of integer ids, growable."""

    def __init__(self) -> None:
        self.bits = bytearray()
        self.count = 0

    def _grow(self, i: int) -> None:
        need = i // 8 + 1
        if need > len(self.bits):
            self.bits.extend(bytes(max(need - len(self.bits), len(self.bits))))

    def get(self, i: int) -> bool:
        return i // 8 < len(self.bits) and bool(self.bits[i // 8] & (1 << (i % 8)))

    def set(self, i: int) -> None:
        self._grow(i)
        if not self.get(i):
            self.bits[i // 8] |= 1 << (i % 8)
            self.count += 1

    def clear(self, i: int) -> None:
        if self.get(i):
            self.bits[i // 8] &= ~(1 << (i % 8)) & 0xFF
            self.count -= 1


class RateLimiter:
    """Token bucket: `rate` tokens/s, bursts up to one second's worth."""

    def __init__(self, rate: float, clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep):
        self.rate = float(rate)
        self.clock = clock
        self.sleep = sleep
        self.tokens = 0.0
        self.last = clock()
        self.lock = threading.Lock()

    def acquire(self, n: int, stop: Optional[threading.Event] = None) -> bool:
        while True:
            with self.lock:
                now = self.clock()
                self.tokens = min(max(self.rate, n), self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= n:
                    self.tokens -= n
                    return True
                wait = (n - self.tokens) / self.rate
            if stop is not None and stop.is_set():
                return False
            self.sleep(min(wait, 0.5))


class Latencies:
    """Per-class latency samples (seconds), overall and per time window."""

    def __init__(self, window: float, t0: float) -> None:
        self.window = window
        self.t0 = t0
        self.all: Dict[str, array.array] = {}
        self.windows: Dict[Tuple[int, str], array.array] = {}
        self.lock = threading.Lock()

    def add(self, cls: str, seconds: float, at: Optional[float] = None) -> None:
        w = int(((time.time() if at is None else at) - self.t0) // self.window) if self.window > 0 else 0
        with self.lock:
            self.all.setdefault(cls, array.array("d")).append(seconds)
            self.windows.setdefault((w, cls), array.array("d")).append(seconds)

    def summary(self) -> Dict[str, Dict]:
        with self.lock:
            return {c: summarize(v) for c, v in sorted(self.all.items())}

    def per_window(self) -> List[Dict]:
        with self.lock:
            rows = [{"window": w, "start_s": w * self.window, "class": c, **summarize(v)} for (w, c), v in self.windows.items()]
        return sorted(rows, key=lambda r: (r["window"], r["class"]))


def summarize(values) -> Dict:
    return {
        "count": len(values),
        "p50_ms": None if not len(values) else round(percentile(values, 50) * 1000, 2),
        "p99_ms": None if not len(values) else round(percentile(values, 99) * 1000, 2),
        "max_ms": None if not len(values) else round(max(values) * 1000, 2),
    }


def status_label(status: Optional[int]) -> str:
    if status is None:
        return "conn"
    if status >= 500:
        return "5xx"
    if status == 429:
        return "429"
    if status >= 400:
        return "4xx"
    return "ok"


def evaluate(report: Dict, t: Dict) -> List[Dict]:
    """Threshold verdicts. `t` holds the threshold flags (None/<0 = off)."""
    out = []

    def gate(name: str, ok: bool, detail: str) -> None:
        out.append({"check": name, "ok": bool(ok), "detail": detail})

    for cls, s in report["latency"].items():
        if t.get("max_p99_ms") is not None and t["max_p99_ms"] >= 0:
            gate("p99:%s" % cls, s["p99_ms"] is not None and s["p99_ms"] <= t["max_p99_ms"], "p99 %s ms, bound %s ms over %d queries" % (s["p99_ms"], t["max_p99_ms"], s["count"]))
    for cls in t.get("required_classes", []):
        s = report["latency"].get(cls)
        gate("ran:%s" % cls, bool(s and s["count"]), "%s queries" % (s["count"] if s else 0))
    vis = report["visibility"]
    if t.get("max_visible_p99_s") is not None and t["max_visible_p99_s"] >= 0:
        p99 = vis["p99_s"]
        gate("visible-p99", vis["samples"] > 0 and vis["timeouts"] == 0 and p99 is not None and p99 <= t["max_visible_p99_s"], "p99 %s s over %d samples, %d never visible, bound %s s" % (p99, vis["samples"], vis["timeouts"], t["max_visible_p99_s"]))
    req = report["requests"]
    total = max(1, req["total"])
    if t.get("max_5xx_rate") is not None and t["max_5xx_rate"] >= 0:
        gate("5xx-rate", req["5xx"] / total <= t["max_5xx_rate"], "%d of %d requests (%.5f), bound %s" % (req["5xx"], req["total"], req["5xx"] / total, t["max_5xx_rate"]))
    if t.get("max_conn_error_rate") is not None and t["max_conn_error_rate"] >= 0:
        gate("conn-error-rate", req["conn"] / total <= t["max_conn_error_rate"], "%d of %d requests (%.5f), bound %s" % (req["conn"], req["total"], req["conn"] / total, t["max_conn_error_rate"]))
    if t.get("max_segments") is not None and t["max_segments"] > 0:
        seg = report["final"].get("large_segments")
        gate("large-segments", seg is not None and seg <= t["max_segments"], "%s segments at the end, bound %s" % (seg, t["max_segments"]))
    flood = report.get("flood")
    if flood:
        fr = flood["requests"]
        gate("flood:limited", fr["429"] > 0, "%d of %d flood requests got 429 (none: the flood tenant has no query limit on the server)" % (fr["429"], fr["total"]))
        if t.get("victim_p99_ms") is not None and t["victim_p99_ms"] >= 0:
            vl = flood["victim"]["latency"]
            gate("flood:victim-p99", vl["p99_ms"] is not None and vl["p99_ms"] <= t["victim_p99_ms"], "p99 %s ms, bound %s ms over %d queries of the other tenants" % (vl["p99_ms"], t["victim_p99_ms"], vl["count"]))
    final = report["final"]
    gate("exact-count", final["checked"] > 0 and not final["mismatches"], "%d namespace/pod counts checked, mismatches %s" % (final["checked"], final["mismatches"][:5]))
    gate("no-uncertain-writes", not report["writes"]["uncertain_namespaces"], "namespaces with an unacknowledged write: %s" % report["writes"]["uncertain_namespaces"][:5])
    return out


# ─── the driver ─────────────────────────────────────────────────────────────


class Namespace:
    def __init__(self, name: str, key: str, tenant: str, size: int, writers: int):
        self.name = name
        self.key = key
        self.tenant = tenant
        self.size = size
        self.live = Bitmap()
        # New ids start at a multiple of `writers`, so id % writers is the
        # owning writer for preloaded and new ids alike.
        self.base = -(-size // writers) * writers
        self.next_m = [0] * writers
        self.pinned: set = set()
        self.dirty = False
        self.uncertain = False
        self.lock = threading.Lock()

    def path(self, action: str) -> str:
        return "/v1/namespaces/%s/%s" % (urllib.parse.quote(self.name, safe=""), action)


class Driver:
    def __init__(self, args, clock: Callable[[], float] = time.time):
        self.a = args
        self.clock = clock
        self.rng = random.Random(args.seed)
        self.write_url = args.write_url.rstrip("/")
        self.query_urls = [u.rstrip("/") for u in (args.query_url or [args.write_url])]
        tenants = parse_tenant_keys(args.tenant_keys)
        prefix = "%s-%s" % (args.prefix, args.run_id)
        sizes = [args.large_docs] + zipf_sizes(args.namespaces, args.zipf_docs, args.zipf_s)
        self.namespaces: List[Namespace] = []
        for i, size in enumerate(sizes):
            key, tenant = tenants[i % len(tenants)]
            name = "%s-large" % prefix if i == 0 else "%s-z%04d" % (prefix, i)
            self.namespaces.append(Namespace(name, key, tenant, size, args.writers))
        self.large = self.namespaces[0]
        self.weights = [max(1, n.size) for n in self.namespaces]
        self.cum = []
        acc = 0
        for w in self.weights:
            acc += w
            self.cum.append(acc)
        self.stop = threading.Event()
        self.t0 = clock()
        self.lat = Latencies(args.window, self.t0)
        self.counts = {"total": 0, "5xx": 0, "conn": 0, "429": 0, "4xx": 0}
        self.by_kind: Dict[str, Dict[str, int]] = {}
        self.counts_lock = threading.Lock()
        self.writes = {"inserted": 0, "updated": 0, "deleted": 0, "retries": 0, "delete_count_mismatches": 0}
        self.visible: List[float] = []
        self.visible_timeouts = 0
        self.want_sample = threading.Event()
        self.samples: List[Tuple[Namespace, int, float]] = []
        self.timeline: List[Dict] = []
        self.events: List[Dict] = []
        self.dropped_queries = 0
        self.inflight = 0
        self.inflight_lock = threading.Lock()
        # Flood mode (#341): one tenant's extra searches, counted apart.
        self.flood_key = getattr(args, "flood_tenant_key", None) or None
        self.flood_targets = [n for n in self.namespaces if n.key == self.flood_key]
        if self.flood_key is not None:
            if not self.flood_targets:
                raise ValueError("--flood-tenant-key is not a key of --tenant-keys that holds a namespace")
            if len(self.flood_targets) == len(self.namespaces):
                raise ValueError("--flood-tenant-key needs at least one other tenant in --tenant-keys to be the victim")
            if args.flood_multiplier <= 0:
                raise ValueError("--flood-multiplier must be positive")
        self.flood_counts = {"total": 0, "ok": 0, "429": 0, "4xx": 0, "5xx": 0, "conn": 0}
        self.flood_dropped = 0
        self.flood_inflight = 0
        self.victim_counts = {"total": 0, "ok": 0, "429": 0, "4xx": 0, "5xx": 0, "conn": 0}
        self.victim_lat = Latencies(args.window, self.t0)

    # bookkeeping --------------------------------------------------------

    def note(self, kind: str, status: Optional[int]) -> None:
        label = status_label(status)
        with self.counts_lock:
            self.counts["total"] += 1
            if label != "ok":
                self.counts[label] += 1
            k = self.by_kind.setdefault(kind, {})
            k[label] = k.get(label, 0) + 1

    def pick_ns(self, rng: random.Random) -> Namespace:
        x = rng.random() * self.cum[-1]
        lo, hi = 0, len(self.cum) - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if self.cum[mid] > x:
                hi = mid
            else:
                lo = mid + 1
        return self.namespaces[lo]

    def elapsed(self) -> float:
        return self.clock() - self.t0

    # writes -------------------------------------------------------------

    def send_until_acked(self, kind: str, ns: Namespace, action: str, body: Dict) -> Optional[Dict]:
        """POST until 200 or --write-retry-s; None (and the namespace marked
        uncertain) when it never got an ack."""
        deadline = time.time() + self.a.write_retry_s
        delay = 0.2
        first = True
        while True:
            status, out = http_json("POST", self.write_url + ns.path(action), body, ns.key, self.a.request_timeout)
            self.note(kind, status)
            if status == 200:
                if not first:
                    with self.counts_lock:
                        self.writes["retries"] += 1
                return out if isinstance(out, dict) else {}
            if status is not None and 400 <= status < 500 and status != 429:
                ns.uncertain = True  # refused outright: a driver or server bug
                self.events.append({"t": round(self.elapsed(), 1), "event": "write refused", "namespace": ns.name, "status": status, "body": str(out)[:200]})
                return None
            if time.time() >= deadline:
                ns.uncertain = True
                return None
            first = False
            time.sleep(delay)
            delay = min(delay * 2, 5.0)

    def owned_live(self, ns: Namespace, k: int, n: int, rng: random.Random, for_delete: bool) -> List[int]:
        W = self.a.writers
        upper = ns.base + k + W * ns.next_m[k]
        slots = max(1, (upper - k) // W + 1)
        out: List[int] = []
        seen = set()
        for _ in range(n * 4):
            i = k + W * rng.randrange(slots)
            if i in seen or not ns.live.get(i) or (for_delete and i in ns.pinned):
                continue
            seen.add(i)
            out.append(i)
            if len(out) == n:
                break
        return out

    def writer(self, k: int, limiter: RateLimiter) -> None:
        rng = random.Random(self.a.seed * 1000 + k)
        W = self.a.writers
        while not self.stop.is_set():
            n = self.a.write_batch
            if not limiter.acquire(n, self.stop):
                return
            ns = self.pick_ns(rng)
            r = rng.random()
            if r < self.a.delete_frac:
                with ns.lock:
                    ids = self.owned_live(ns, k, n, rng, True)
                if not ids:
                    continue
                out = self.send_until_acked("delete", ns, "delete", {"filter": {"terms": {"slot": ["d%d" % i for i in ids]}}})
                if out is None:
                    continue
                with ns.lock:
                    for i in ids:
                        ns.live.clear(i)
                    ns.dirty = True
                with self.counts_lock:
                    self.writes["deleted"] += len(ids)
                    if out.get("deleted") not in (None, len(ids)):
                        self.writes["delete_count_mismatches"] += 1
            elif r < self.a.delete_frac + self.a.update_frac:
                with ns.lock:
                    ids = self.owned_live(ns, k, n, rng, False)
                if not ids:
                    continue
                rev = int(self.elapsed())
                if self.send_until_acked("upsert", ns, "documents", {"documents": [make_doc(i, rev, self.a.dim) for i in ids]}) is None:
                    continue
                with ns.lock:
                    ns.dirty = True
                with self.counts_lock:
                    self.writes["updated"] += len(ids)
            else:
                with ns.lock:
                    m = ns.next_m[k]
                    ns.next_m[k] += n
                ids = [ns.base + k + W * (m + j) for j in range(n)]
                if self.send_until_acked("insert", ns, "documents", {"documents": [make_doc(i, 0, self.a.dim) for i in ids]}) is None:
                    continue
                acked_at = time.time()
                with ns.lock:
                    for i in ids:
                        ns.live.set(i)
                    ns.dirty = True
                    # Not from the flooded tenant: the probe's searches would
                    # be rate limited with the flood's.
                    if self.want_sample.is_set() and ns.key != self.flood_key:
                        self.want_sample.clear()
                        ns.pinned.add(ids[0])
                        self.samples.append((ns, ids[0], acked_at))
                with self.counts_lock:
                    self.writes["inserted"] += len(ids)

    # reads --------------------------------------------------------------

    def query_body(self, cls: str, rng: random.Random) -> Dict:
        body: Dict = {"max_results": 10, "consistency": "eventual", "no_cache": True}
        if cls == "knn":
            body["query_text"] = ""
            body["knn"] = {"field": "emb", "vector": vector_for(rng.randrange(10**9), self.a.dim), "k": 10}
        else:
            body["query_text"] = " ".join(rng.choice(VOCAB) for _ in range(rng.randint(1, 3)))
            if cls == "filtered":
                body["filter"] = {"term": {"cat": "c%d" % rng.randrange(CATEGORIES)}}
        return body

    def one_query(self, cls: str, ns: Namespace, url: str, body: Dict) -> None:
        try:
            t = time.time()
            status, _ = http_json("POST", url + ns.path("search"), body, ns.key, self.a.request_timeout)
            took = time.time() - t
            self.note("query:" + cls, status)
            if status == 200:
                self.lat.add(cls, took, t)
            if self.flood_key is not None and ns.key != self.flood_key:
                with self.counts_lock:
                    self.victim_counts["total"] += 1
                    self.victim_counts[status_label(status)] += 1
                if status == 200:
                    self.victim_lat.add("victim", took, t)
        finally:
            with self.inflight_lock:
                self.inflight -= 1

    def flood_rate(self) -> float:
        """The flooder's request rate now: --flood-multiplier times the
        flood tenant's configured qps (--flood-qps), or times the current
        query rate when that is not given."""
        base = self.a.flood_qps if self.a.flood_qps > 0 else ramp(self.elapsed(), self.a.duration, self.a.qps_start, self.a.qps_end)
        return base * self.a.flood_multiplier

    def one_flood_query(self, ns: Namespace, url: str, body: Dict) -> None:
        try:
            status, _ = http_json("POST", url + ns.path("search"), body, ns.key, self.a.request_timeout)
            with self.counts_lock:
                self.flood_counts["total"] += 1
                self.flood_counts[status_label(status)] += 1
        finally:
            with self.inflight_lock:
                self.flood_inflight -= 1

    def flooder(self, pool: ThreadPoolExecutor) -> None:
        """Open loop at flood_rate(), on its own pool and its own counters:
        nothing it sends counts towards the run's request totals or gates."""
        rng = random.Random(self.a.seed + 11)
        i = 0
        nxt = time.time()
        while not self.stop.is_set():
            nxt += 1.0 / max(self.flood_rate(), 0.01)
            delay = nxt - time.time()
            if delay > 0:
                self.stop.wait(delay)
            elif delay < -1.0:
                nxt = time.time()  # stalled: do not burst to catch up
            ns = self.flood_targets[rng.randrange(len(self.flood_targets))]
            with self.inflight_lock:
                if self.flood_inflight >= self.a.flood_concurrency:
                    self.flood_dropped += 1
                    continue
                self.flood_inflight += 1
            pool.submit(self.one_flood_query, ns, self.query_urls[i % len(self.query_urls)], self.query_body("bm25", rng))
            i += 1

    def flood_report(self) -> Dict:
        with self.counts_lock:
            flood, victim = dict(self.flood_counts), dict(self.victim_counts)
        tenant = self.flood_targets[0].tenant
        return {
            "tenant": tenant,
            "multiplier": self.a.flood_multiplier,
            "requests": flood,
            "rate_429": round(flood["429"] / max(1, flood["total"]), 4),
            "dropped": self.flood_dropped,
            "victim": {
                "tenants": sorted({n.tenant for n in self.namespaces if n.key != self.flood_key}),
                "requests": victim,
                "rate_429": round(victim["429"] / max(1, victim["total"]), 4),
                "latency": self.victim_lat.summary().get("victim", summarize([])),
                "latency_windows": self.victim_lat.per_window(),
            },
        }

    def dispatcher(self, pool: ThreadPoolExecutor) -> None:
        rng = random.Random(self.a.seed + 7)
        classes, cum = [], []
        acc = 0.0
        for cls, w in self.a.mix:
            acc += w
            classes.append(cls)
            cum.append(acc)
        i = 0
        nxt = time.time()
        while not self.stop.is_set():
            qps = ramp(self.elapsed(), self.a.duration, self.a.qps_start, self.a.qps_end)
            nxt += 1.0 / max(qps, 0.01)
            delay = nxt - time.time()
            if delay > 0:
                self.stop.wait(delay)
            x = rng.random() * acc
            cls = classes[next(j for j, c in enumerate(cum) if c > x)]
            ns = self.pick_ns(rng)
            with self.inflight_lock:
                if self.inflight >= self.a.query_concurrency:
                    self.dropped_queries += 1
                    continue
                self.inflight += 1
            pool.submit(self.one_query, cls, ns, self.query_urls[i % len(self.query_urls)], self.query_body(cls, rng))
            i += 1

    def prober(self) -> None:
        i = 0
        while not self.stop.is_set():
            # Ask the writers for the next acknowledged insert, then follow
            # it right away; one sample at a time, every --visible-every.
            self.want_sample.set()
            while not self.samples and not self.stop.is_set():
                self.stop.wait(0.02)
            started = time.time()
            while self.samples:
                ns, n, acked_at = self.samples.pop(0)
                url = self.query_urls[i % len(self.query_urls)]
                i += 1
                body = {"query_text": "", "filter": {"term": {"slot": "d%d" % n}}, "max_results": 1, "consistency": "eventual", "no_cache": True}
                found = None
                abandoned = False
                while time.time() - acked_at < self.a.visible_timeout:
                    status, out = http_json("POST", url + ns.path("search"), body, ns.key, self.a.request_timeout)
                    if status == 200 and isinstance(out, dict) and out.get("total_hits", len(out.get("results", []))) >= 1:
                        found = time.time()
                        break
                    if self.stop.is_set():
                        # The load (and with it --flush-every) has stopped:
                        # this sample says nothing about visibility.
                        abandoned = True
                        break
                    time.sleep(0.1)
                if abandoned:
                    pass
                elif found is None:
                    self.visible_timeouts += 1
                    self.events.append({"t": round(self.elapsed(), 1), "event": "never visible", "namespace": ns.name, "id": "d%d" % n})
                else:
                    self.visible.append(found - acked_at)
                with ns.lock:
                    ns.pinned.discard(n)
            self.stop.wait(max(0.0, self.a.visible_every - (time.time() - started)))

    # background ---------------------------------------------------------

    def sample(self) -> Dict:
        row: Dict = {"t": round(self.elapsed(), 1), "pods": {}}
        for url in sorted(set([self.write_url] + self.query_urls)):
            text = http_text(url + "/metrics", key=self.a.metrics_key)
            row["pods"][url] = pick_metrics(parse_prometheus(text)) if text is not None else None
        status, out = http_json("GET", self.query_urls[0] + self.large.path("stats"), key=self.large.key, timeout=30)
        if status == 200 and isinstance(out, dict):
            row["large"] = {k: out.get(k) for k in ("documents", "segments", "manifest_version")}
        ours = {n.name for n in self.namespaces}
        segs, listed = 0, 0
        for key, _tenant in parse_tenant_keys(self.a.tenant_keys):
            status, out = http_json("GET", self.query_urls[0] + "/v1/namespaces", key=key, timeout=30)
            if status == 200 and isinstance(out, dict):
                for n in out.get("namespaces", []):
                    if n.get("name") in ours and n.get("segments") is not None:
                        segs += n["segments"]
                        listed += 1
        row["segments_total"] = segs
        row["namespaces_listed"] = listed
        with self.counts_lock:
            row["requests"] = dict(self.counts)
        return row

    def sampler(self) -> None:
        while not self.stop.is_set():
            self.timeline.append(self.sample())
            self.stop.wait(self.a.sample_every)

    def periodic(self, every: float, fn: Callable[[], None]) -> None:
        while not self.stop.wait(every):
            try:
                fn()
            except Exception as e:  # keep the soak running
                self.events.append({"t": round(self.elapsed(), 1), "event": "periodic task failed", "error": str(e)})

    def flush_dirty(self) -> None:
        for ns in self.namespaces:
            if ns.dirty and not self.stop.is_set():
                ns.dirty = False
                status, _ = http_json("POST", self.write_url + ns.path("flush"), {}, ns.key, self.a.request_timeout)
                self.note("flush", status)
                if status != 200:
                    ns.dirty = True

    def compact_some(self) -> None:
        targets = [self.large] + self.rng.sample(self.namespaces[1:], min(3, len(self.namespaces) - 1))
        for ns in targets:
            status, _ = http_json("POST", self.write_url + "/v1/admin/compact-namespace", {"namespace": "%s/%s" % (ns.tenant, ns.name), "mode": "tiered"}, self.a.admin_key, 600)
            # 409: a pass is already running on it.
            self.note("compact", 200 if status == 409 else status)

    def replace(self) -> None:
        t = time.time()
        p = subprocess.run(self.a.replace_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        self.events.append({"t": round(self.elapsed(), 1), "event": "replace", "exit": p.returncode, "seconds": round(time.time() - t, 1), "output": p.stdout[-300:]})

    # phases -------------------------------------------------------------

    def preload(self) -> None:
        a = self.a
        chunks = []
        for ns in self.namespaces:
            for start in range(0, ns.size, a.preload_batch):
                chunks.append((ns, start, min(ns.size, start + a.preload_batch)))
        t = time.time()
        done = [0]
        lock = threading.Lock()

        def load(chunk) -> None:
            ns, start, end = chunk
            if self.send_until_acked("preload", ns, "documents", {"documents": [make_doc(i, 0, a.dim) for i in range(start, end)]}) is None:
                return
            with ns.lock:
                for i in range(start, end):
                    ns.live.set(i)
            with lock:
                done[0] += end - start
                if done[0] // 100000 != (done[0] - (end - start)) // 100000:
                    print("  preloaded %d docs (%.0f/s)" % (done[0], done[0] / max(0.001, time.time() - t)), flush=True)

        with ThreadPoolExecutor(a.preload_threads) as pool:
            list(pool.map(load, chunks))
        for ns in self.namespaces:
            self.send_until_acked("flush", ns, "flush", {})
        print("preload: %d namespaces, %d docs in %.1fs" % (len(self.namespaces), sum(n.live.count for n in self.namespaces), time.time() - t), flush=True)

    def final_check(self) -> Dict:
        a = self.a
        for ns in self.namespaces:
            self.send_until_acked("flush", ns, "flush", {})
        mismatches: List[str] = []
        checked = 0
        deadline = time.time() + a.final_timeout
        targets = [(self.write_url, "strong")] + [(u, "eventual") for u in self.query_urls]
        for ns in self.namespaces:
            want = ns.live.count
            for url, consistency in targets:
                body = {"query_text": "", "filter": {"match_all": {}}, "max_results": 1, "exact_total_hits": True, "consistency": consistency, "no_cache": True}
                got = None
                while True:
                    status, out = http_json("POST", url + ns.path("search"), body, ns.key, a.request_timeout)
                    got = out.get("total_hits") if status == 200 and isinstance(out, dict) else None
                    if got == want or time.time() >= deadline:
                        break
                    time.sleep(0.5)
                checked += 1
                if got != want:
                    mismatches.append("%s@%s: %s, want %d" % (ns.name, url, got, want))
        large_segments = None
        status, out = http_json("GET", self.write_url + self.large.path("stats"), key=self.large.key)
        if status == 200 and isinstance(out, dict):
            large_segments = out.get("segments")
        return {"checked": checked, "mismatches": mismatches, "large_segments": large_segments, "live_docs": sum(n.live.count for n in self.namespaces)}

    def run(self) -> Dict:
        a = self.a
        if not a.no_preload:
            self.preload()
        else:
            # The namespaces hold exactly an earlier preload of this run id
            # (a `--duration 0` run): ids 0..size-1.
            for ns in self.namespaces:
                with ns.lock:
                    for i in range(ns.size):
                        ns.live.set(i)
        self.t0 = self.clock()
        self.lat.t0 = self.t0
        self.victim_lat.t0 = self.t0
        limiter = RateLimiter(a.docs_per_s)
        threads = [threading.Thread(target=self.writer, args=(k, limiter), daemon=True) for k in range(a.writers)]
        threads.append(threading.Thread(target=self.prober, daemon=True))
        threads.append(threading.Thread(target=self.sampler, daemon=True))
        if a.flush_every > 0:
            threads.append(threading.Thread(target=self.periodic, args=(a.flush_every, self.flush_dirty), daemon=True))
        if a.compact_every > 0:
            threads.append(threading.Thread(target=self.periodic, args=(a.compact_every, self.compact_some), daemon=True))
        if a.replace_cmd and a.replace_every > 0:
            threads.append(threading.Thread(target=self.periodic, args=(a.replace_every, self.replace), daemon=True))
        pool = ThreadPoolExecutor(a.query_concurrency)
        dispatcher = threading.Thread(target=self.dispatcher, args=(pool,), daemon=True)
        flood_pool = flooder = None
        if self.flood_key is not None and a.duration > 0:
            flood_pool = ThreadPoolExecutor(a.flood_concurrency)
            flooder = threading.Thread(target=self.flooder, args=(flood_pool,), daemon=True)
            flooder.start()
        for t in threads + [dispatcher]:
            t.start()
        last = time.time()
        while self.elapsed() < a.duration:
            time.sleep(min(1.0, max(0.0, a.duration - self.elapsed())))
            if time.time() - last >= a.progress_every:
                last = time.time()
                with self.counts_lock:
                    c = dict(self.counts)
                lat = self.lat.summary()
                vis = percentile(self.visible, 99)
                print("  t=%5.0fs  requests %d (5xx %d, conn %d, 429 %d)  %s  visible p99 %s" % (
                    self.elapsed(), c["total"], c["5xx"], c["conn"], c["429"],
                    " ".join("%s p99 %sms" % (k, v["p99_ms"]) for k, v in lat.items()),
                    "-" if vis is None else "%.2fs" % vis), flush=True)
                if flooder is not None:
                    f = self.flood_report()
                    print("           flood %s: %d requests, %.0f%% 429; victims p99 %sms, %d 429" % (
                        f["tenant"], f["requests"]["total"], 100 * f["rate_429"], f["victim"]["latency"]["p99_ms"], f["victim"]["requests"]["429"]), flush=True)
        self.stop.set()
        dispatcher.join()
        pool.shutdown(wait=True)
        if flooder is not None:
            flooder.join()
            flood_pool.shutdown(wait=True)
        for t in threads:
            t.join(timeout=a.write_retry_s + a.visible_timeout + 30)
        self.timeline.append(self.sample())
        final = self.final_check()
        report = {
            "config": {k: v for k, v in vars(a).items() if k not in ("admin_key",)},
            "duration_s": round(self.elapsed(), 1),
            "namespaces": len(self.namespaces),
            "latency": self.lat.summary(),
            "latency_windows": self.lat.per_window(),
            "visibility": {
                "samples": len(self.visible),
                "timeouts": self.visible_timeouts,
                "p50_s": None if not self.visible else round(percentile(self.visible, 50), 3),
                "p99_s": None if not self.visible else round(percentile(self.visible, 99), 3),
                "max_s": None if not self.visible else round(max(self.visible), 3),
            },
            "requests": dict(self.counts),
            "requests_by_kind": self.by_kind,
            "dropped_queries": self.dropped_queries,
            "writes": dict(self.writes, uncertain_namespaces=[n.name for n in self.namespaces if n.uncertain]),
            "timeline": self.timeline,
            "events": self.events,
            "final": final,
        }
        if flooder is not None:
            report["flood"] = self.flood_report()
        thresholds = {
            "victim_p99_ms": a.victim_p99_ms,
            "max_p99_ms": a.max_p99_ms,
            # A `--duration 0` run only preloads: no queries or samples to gate.
            "max_visible_p99_s": a.max_visible_p99_s if a.duration > 0 else None,
            "max_5xx_rate": a.max_5xx_rate,
            "max_conn_error_rate": a.max_conn_error_rate,
            "max_segments": a.max_segments,
            "required_classes": [c for c, w in a.mix if w > 0] if a.duration > 0 else [],
        }
        report["verdicts"] = evaluate(report, thresholds)
        report["pass"] = all(v["ok"] for v in report["verdicts"])
        return report


# ─── self-test ──────────────────────────────────────────────────────────────


class FakeKosha:
    """Just enough of the HTTP API for the driver: tenants by key, index,
    delete by `terms` on slot, flush (makes writes visible to eventual
    reads), search totals, stats, listing, /metrics. `lose` drops that many
    acknowledged inserts (and every later write of those ids), to prove the
    final check catches it. `qps` limits a tenant's searches per second
    like the server's tenant limits: over it, 429 with Retry-After."""

    def __init__(self, keys: Dict[str, str], lose: int = 0, qps: Optional[Dict[str, float]] = None):
        import http.server

        self.keys = keys
        self.lose = lose
        self.qps = qps or {}
        # tenant -> [tokens, last refill]
        self.buckets: Dict[str, List[float]] = {t: [r, time.monotonic()] for t, r in self.qps.items()}
        self.lost: set = set()
        self.buffer: Dict[str, Dict[str, Dict]] = {}
        self.visible: Dict[str, Dict[str, Dict]] = {}
        self.lock = threading.Lock()
        fake = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code: int, body, text: bool = False) -> None:
                raw = body.encode() if text else json.dumps(body).encode()
                self.send_response(code)
                if code == 429:
                    self.send_header("retry-after", "1")
                self.send_header("content-length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def _ns(self) -> Tuple[Optional[str], str]:
                tenant = fake.keys.get(self.headers.get("Authorization", "")[7:])
                parts = self.path.split("/")
                return tenant, ("%s/%s" % (tenant, urllib.parse.unquote(parts[3])) if len(parts) > 3 else "")

            def do_GET(self):
                if self.path == "/metrics":
                    return self._send(200, "# HELP x\nkosha_search_queue_depth 0\nkosha_cache_bytes{cache=\"segment\"} 42\n", True)
                tenant, ns = self._ns()
                if tenant is None:
                    return self._send(401, {})
                with fake.lock:
                    if self.path == "/v1/namespaces":
                        names = [k.split("/", 1)[1] for k in fake.visible if k.startswith(tenant + "/")]
                        return self._send(200, {"namespaces": [{"name": n, "segments": 1} for n in names]})
                    if self.path.endswith("/stats"):
                        return self._send(200, {"documents": len(fake.visible.get(ns, {})), "segments": 1, "manifest_version": 1})
                self._send(404, {})

            def do_POST(self):
                tenant, ns = self._ns()
                if tenant is None:
                    return self._send(401, {})
                body = json.loads(self.rfile.read(int(self.headers.get("content-length") or 0)) or b"{}")
                action = self.path.rsplit("/", 1)[1]
                with fake.lock:
                    buf = fake.buffer.setdefault(ns, dict(fake.visible.get(ns, {})))
                    if action == "documents":
                        for d in body["documents"]:
                            if fake.lose and d["id"] not in buf:
                                fake.lose -= 1
                                fake.lost.add((ns, d["id"]))
                            if (ns, d["id"]) in fake.lost:
                                continue
                            buf[d["id"]] = d
                        return self._send(200, {"indexed": len(body["documents"])})
                    if action == "delete":
                        ids = body["filter"]["terms"]["slot"]
                        n = sum(1 for i in ids if buf.pop(i, None) is not None)
                        return self._send(200, {"deleted": n})
                    if action == "flush":
                        fake.visible[ns] = dict(buf)
                        return self._send(200, {})
                    if action == "search":
                        bucket = fake.buckets.get(tenant)
                        if bucket is not None:
                            now = time.monotonic()
                            rate = fake.qps[tenant]
                            bucket[0] = min(rate, bucket[0] + (now - bucket[1]) * rate)
                            bucket[1] = now
                            if bucket[0] < 1:
                                return self._send(429, {"error": "tenant query rate limit exceeded", "reason": "tenant_qps_exceeded", "limit": rate, "retry_after_ms": 1000})
                            bucket[0] -= 1
                        docs = buf if body.get("consistency") == "strong" else fake.visible.get(ns, {})
                        flt = body.get("filter") or {}
                        if "term" in flt and "slot" in flt["term"]:
                            n = 1 if flt["term"]["slot"] in docs else 0
                        else:
                            n = len(docs)
                        return self._send(200, {"total_hits": n, "results": []})
                self._send(404, {})

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = "http://127.0.0.1:%d" % self.server.server_port
        threading.Thread(target=self.server.serve_forever, daemon=True).start()


def self_test() -> int:
    failures: List[str] = []

    def check(name: str, ok: bool, detail="") -> None:
        print("  %-44s %s %s" % (name, "ok" if ok else "FAIL", "" if ok else detail))
        if not ok:
            failures.append(name)

    check("parse_tenant_keys", parse_tenant_keys("a=x, b=y,") == [("a", "x"), ("b", "y")])
    try:
        parse_tenant_keys("nokey")
        check("parse_tenant_keys rejects junk", False)
    except ValueError:
        check("parse_tenant_keys rejects junk", True)
    sizes = zipf_sizes(100, 100000, 1.1)
    check("zipf_sizes: decreasing, sum ~ total, >= 1", sizes == sorted(sizes, reverse=True) and abs(sum(sizes) - 100000) < 200 and min(sizes) >= 1 and sizes[0] > 10 * sizes[50], (sizes[:3], sum(sizes)))
    check("zipf_sizes(0)", zipf_sizes(0, 10, 1) == [])
    check("percentile (nearest rank)", percentile([5, 1, 3, 2, 4], 50) == 3 and percentile(list(range(1, 101)), 99) == 99 and percentile([], 50) is None and percentile([7], 99) == 7)
    check("ramp", ramp(0, 100, 8, 32) == 8 and ramp(50, 100, 8, 32) == 20 and ramp(500, 100, 8, 32) == 32 and ramp(5, 0, 1, 2) == 2)
    m = parse_prometheus('# HELP a b\n# TYPE a gauge\nkosha_search_queue_depth 3\nkosha_cache_bytes{cache="segment",x="a b"} 1.5e3\nbad line here\nprocess_resident_memory_bytes 7 1700000000\n')
    check("parse_prometheus", m == {"kosha_search_queue_depth": 3.0, 'kosha_cache_bytes{cache="segment",x="a b"}': 1500.0, "process_resident_memory_bytes": 7.0}, m)
    check("pick_metrics", pick_metrics({"kosha_search_queue_depth": 1.0, "other": 2.0}) == {"kosha_search_queue_depth": 1.0})
    b = Bitmap()
    for i in (0, 7, 8, 1000, 7):
        b.set(i)
    b.clear(8)
    b.clear(9)
    check("Bitmap", b.count == 3 and b.get(0) and b.get(1000) and not b.get(8) and not b.get(10**6))
    d = make_doc(12, 3, 4)
    vec = json.loads(next(f["value"] for f in d["fields"] if f["name"] == "emb"))
    check("make_doc deterministic", d == make_doc(12, 3, 4) and d["id"] == "d12" and len(vec) == 4 and abs(sum(x * x for x in vec) - 1) < 1e-3)
    now = [0.0]
    slept: List[float] = []

    def fake_sleep(s: float) -> None:
        slept.append(s)
        now[0] += s

    rl = RateLimiter(100, clock=lambda: now[0], sleep=fake_sleep)
    for _ in range(10):
        rl.acquire(50)
    check("RateLimiter holds the rate", 4.4 <= now[0] <= 5.1, now[0])
    lat = Latencies(10, 0)
    for i in range(100):
        lat.add("bm25", i / 1000.0, at=i * 0.2)
    s = lat.summary()["bm25"]
    check("Latencies summary + windows", s["count"] == 100 and s["p99_ms"] == 98.0 and len(lat.per_window()) == 2, (s, lat.per_window()))

    rep = {
        "latency": {"bm25": {"count": 5, "p99_ms": 50.0}, "knn": {"count": 5, "p99_ms": 500.0}},
        "visibility": {"samples": 3, "timeouts": 0, "p99_s": 2.0},
        "requests": {"total": 1000, "5xx": 2, "conn": 0},
        "final": {"checked": 4, "mismatches": [], "large_segments": 12},
        "writes": {"uncertain_namespaces": []},
    }
    v = {x["check"]: x["ok"] for x in evaluate(rep, {"max_p99_ms": 100, "max_visible_p99_s": 5, "max_5xx_rate": 0.001, "max_conn_error_rate": 0.01, "max_segments": 10, "required_classes": ["bm25", "filtered"]})}
    check("evaluate", v == {"p99:bm25": True, "p99:knn": False, "ran:bm25": True, "ran:filtered": False, "visible-p99": True, "5xx-rate": False, "conn-error-rate": True, "large-segments": False, "exact-count": True, "no-uncertain-writes": True}, v)

    # A short run against the fake: everything holds; then one that loses
    # an acknowledged insert, which the exact count must catch.
    keys = {"k1": "t1", "k2": "t2"}
    for lose, want_pass in ((0, True), (1, False)):
        fake = FakeKosha(keys, lose=0)
        args = parse_args([
            "--write-url", fake.url, "--query-url", fake.url, "--tenant-keys", "k1=t1,k2=t2",
            "--duration", "3", "--namespaces", "5", "--large-docs", "300", "--zipf-docs", "200",
            "--docs-per-s", "400", "--writers", "3", "--write-batch", "10", "--qps-start", "20", "--qps-end", "40",
            "--flush-every", "0.3", "--sample-every", "1", "--visible-every", "0.5", "--visible-timeout", "5",
            "--progress-every", "100", "--final-timeout", "3", "--run-id", "selftest", "--max-p99-ms", "1000",
            # No deletes in the lossy run: deleting the lost id would hide it.
            "--delete-frac", "0.2" if want_pass else "0",
        ])
        driver = Driver(args)
        driver.preload()
        fake.lose = lose
        args.no_preload = True
        report = driver.run()
        fake.server.shutdown()
        verdicts = {x["check"]: x for x in report["verdicts"]}
        if want_pass:
            w = report["writes"]
            check("fake run passes", report["pass"], [x for x in report["verdicts"] if not x["ok"]])
            check("fake run did inserts, updates, deletes", w["inserted"] > 0 and w["updated"] > 0 and w["deleted"] > 0, w)
            check("fake run: all classes queried", all(report["latency"].get(c, {}).get("count") for c in ("bm25", "knn", "filtered")), report["latency"])
            check("fake run: visibility sampled", report["visibility"]["samples"] > 0 and report["visibility"]["timeouts"] == 0, report["visibility"])
            check("fake run: timeline has metrics + segments", report["timeline"] and report["timeline"][0]["pods"][fake.url]["kosha_search_queue_depth"] == 0 and report["timeline"][-1]["namespaces_listed"] == 6, report["timeline"][-1])
            check("fake run: two tenants", {n.tenant for n in driver.namespaces} == {"t1", "t2"})
        else:
            check("lost insert fails the exact count", not report["pass"] and not verdicts["exact-count"]["ok"], verdicts["exact-count"])

    # Flood mode (#341): gates on the victims only.
    flood_rep = dict(rep, flood={"requests": {"total": 100, "429": 90}, "victim": {"latency": {"count": 9, "p99_ms": 80.0}}})
    v = {x["check"]: x["ok"] for x in evaluate(flood_rep, {"victim_p99_ms": 100})}
    check("evaluate: flood gates hold", v.get("flood:limited") is True and v.get("flood:victim-p99") is True, v)
    v = {x["check"]: x["ok"] for x in evaluate(flood_rep, {"victim_p99_ms": 50})}
    check("evaluate: victim p99 over the bound fails", v.get("flood:victim-p99") is False, v)
    flood_rep["flood"]["requests"]["429"] = 0
    v = {x["check"]: x["ok"] for x in evaluate(flood_rep, {})}
    check("evaluate: an unlimited flooder fails", v.get("flood:limited") is False and "flood:victim-p99" not in v, v)
    check("evaluate: no flood, no flood gates", not any(x["check"].startswith("flood:") for x in evaluate(rep, {"victim_p99_ms": 1})))
    try:
        Driver(parse_args(["--tenant-keys", "k1=t1,k2=t2", "--namespaces", "3", "--large-docs", "1", "--zipf-docs", "3", "--duration", "1", "--flood-tenant-key", "nope"]))
        check("flood key must be a tenant key", False)
    except ValueError:
        check("flood key must be a tenant key", True)

    # A flood run against the fake: tenant t1 is limited to 20 searches/s
    # and floods at 10x that. Its 429s stay out of the run's totals, and the
    # other tenant is reported (and gated) on its own.
    fake = FakeKosha(keys, qps={"t1": 20})
    args = parse_args([
        "--write-url", fake.url, "--query-url", fake.url, "--tenant-keys", "k1=t1,k2=t2",
        "--duration", "3", "--namespaces", "5", "--large-docs", "300", "--zipf-docs", "200",
        "--docs-per-s", "400", "--writers", "3", "--write-batch", "10", "--qps-start", "20", "--qps-end", "40",
        "--flush-every", "0.3", "--sample-every", "1", "--visible-every", "0.5", "--visible-timeout", "5",
        "--progress-every", "100", "--final-timeout", "10", "--run-id", "floodtest", "--max-p99-ms", "1000",
        "--flood-tenant-key", "k1", "--flood-qps", "20", "--flood-multiplier", "10", "--victim-p99-ms", "1000",
    ])
    driver = Driver(args)
    report = driver.run()
    fake.server.shutdown()
    flood = report.get("flood") or {}
    fr, victim = flood.get("requests", {}), flood.get("victim", {})
    verdicts = {x["check"]: x for x in report["verdicts"]}
    check("flood run passes", report["pass"], [x for x in report["verdicts"] if not x["ok"]])
    check("flood run: the flooder ran at about 10x and got 429s", flood.get("tenant") == "t1" and fr.get("total", 0) >= 300 and flood.get("rate_429", 0) >= 0.5 and fr.get("ok", 0) > 0, flood)
    check("flood run: flood requests are not in the run's totals", not any(k.startswith("flood") for k in report["requests_by_kind"]) and sum(sum(k.values()) for k in report["requests_by_kind"].values()) == report["requests"]["total"], report["requests"])
    check("flood run: victims are never 429 and never the flood tenant", victim.get("tenants") == ["t2"] and victim.get("requests", {}).get("429") == 0 and victim.get("requests", {}).get("ok", 0) > 0, victim)
    check("flood run: victim p99 reported and gated separately", victim.get("latency", {}).get("count", 0) > 0 and victim["latency"]["p99_ms"] is not None and verdicts.get("flood:victim-p99", {}).get("ok") is True and verdicts.get("flood:limited", {}).get("ok") is True, (victim.get("latency"), verdicts))
    args.victim_p99_ms = 0.0001
    check("flood run: a victim p99 over the bound fails the run", not all(x["ok"] for x in evaluate(report, {"victim_p99_ms": args.victim_p99_ms})))
    print("self-test: %s" % ("FAILED: " + ", ".join(failures) if failures else "all ok"))
    return 1 if failures else 0


# ─── main ───────────────────────────────────────────────────────────────────


def parse_mix(spec: str) -> List[Tuple[str, float]]:
    out = []
    for part in spec.split(","):
        cls, _, w = part.partition("=")
        if cls not in ("bm25", "knn", "filtered"):
            raise argparse.ArgumentTypeError("unknown query class %r" % cls)
        out.append((cls, float(w)))
    return out


def parse_args(argv: Optional[List[str]] = None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--self-test", action="store_true", help="test the pure logic and a short run against a fake server, then exit")
    p.add_argument("--smoke", action="store_true", help="5-minute run with small sizes (defaults below are the 24h run's)")
    p.add_argument("--write-url", default="http://localhost:8080", help="where writes go (the ingest pod, or any pod: query pods forward)")
    p.add_argument("--query-url", action="append", help="a query pod; repeat for several (default: --write-url)")
    p.add_argument("--tenant-keys", default="sk-kosha-dev=default,sk-tenant-a=tenant-a,sk-tenant-b=tenant-b", help="key=tenant,... namespaces are spread over these")
    p.add_argument("--admin-key", default="sk-kosha-admin-dev", help="for --compact-every")
    p.add_argument("--metrics-key", help="bearer key for /metrics when KOSHA_METRICS_AUTH=admin")
    p.add_argument("--prefix", default="soak", help="namespace name prefix")
    p.add_argument("--run-id", default=str(int(time.time())), help="namespace name suffix; reuse with --no-preload to continue on existing data")
    p.add_argument("--seed", type=int, default=357)
    p.add_argument("--duration", type=float, help="seconds of mixed load after the preload (24h: 86400; smoke: 300)")
    p.add_argument("--namespaces", type=int, help="Zipf-sized namespaces besides the large one (1000; smoke 20)")
    p.add_argument("--large-docs", type=int, help="documents preloaded into the large namespace (10M; smoke 20k)")
    p.add_argument("--zipf-docs", type=int, help="documents preloaded over the Zipf namespaces (2M; smoke 10k)")
    p.add_argument("--zipf-s", type=float, default=1.1, help="Zipf exponent")
    p.add_argument("--no-preload", action="store_true", help="skip the preload (the namespaces must already hold exactly the preload of the same --run-id)")
    p.add_argument("--preload-batch", type=int, default=500)
    p.add_argument("--preload-threads", type=int, default=8)
    p.add_argument("--docs-per-s", type=float, help="target write rate, docs/s (1500; smoke 300)")
    p.add_argument("--writers", type=int, default=4, help="writer threads")
    p.add_argument("--write-batch", type=int, default=50, help="docs per write request")
    p.add_argument("--update-frac", type=float, default=0.3, help="share of write requests that re-upsert live ids")
    p.add_argument("--delete-frac", type=float, default=0.2, help="share of write requests that delete live ids")
    p.add_argument("--write-retry-s", type=float, default=300, help="seconds to retry an unacknowledged write before the namespace is uncertain")
    p.add_argument("--qps-start", type=float, help="query rate at the start (8; smoke 4)")
    p.add_argument("--qps-end", type=float, help="query rate at the end (32; smoke 12)")
    p.add_argument("--mix", type=parse_mix, default=parse_mix("bm25=0.4,knn=0.3,filtered=0.3"), help="query class weights")
    p.add_argument("--query-concurrency", type=int, default=64, help="queries in flight at most; beyond it they are dropped and counted")
    p.add_argument("--dim", type=int, default=8, help="vector dimension")
    p.add_argument("--request-timeout", type=float, default=60)
    p.add_argument("--visible-every", type=float, default=5, help="seconds between write-to-visible samples")
    p.add_argument("--visible-timeout", type=float, default=120, help="a sample not visible after this counts as never visible")
    p.add_argument("--flush-every", type=float, help="flush written namespaces this often; 0 = the server's own flushing (24h: 0; smoke: 2)")
    p.add_argument("--compact-every", type=float, help="request tiered compactions this often; 0 = background compaction (24h: 0; smoke: 60)")
    p.add_argument("--sample-every", type=float, help="seconds between /metrics + stats samples (60; smoke 10)")
    p.add_argument("--window", type=float, help="latency window, seconds (600; smoke 60)")
    p.add_argument("--replace-cmd", help="shell command run every --replace-every seconds, e.g. delete a query pod")
    p.add_argument("--replace-every", type=float, default=3600)
    p.add_argument("--final-timeout", type=float, default=300, help="seconds the final counts may take to converge, over all namespaces")
    p.add_argument("--progress-every", type=float, default=30)
    p.add_argument("--max-p99-ms", type=float, default=1000, help="p99 bound per query class; -1 = off")
    p.add_argument("--max-visible-p99-s", type=float, default=30, help="write-to-visible p99 bound; -1 = off")
    p.add_argument("--max-5xx-rate", type=float, default=0.001, help="5xx share of all requests; -1 = off")
    p.add_argument("--max-conn-error-rate", type=float, default=0.01, help="connection-error share of all requests; -1 = off")
    p.add_argument("--max-segments", type=int, default=0, help="segments of the large namespace at the end; 0 = off")
    p.add_argument("--flood-tenant-key", help="noisy-neighbour mode (#341): a key of --tenant-keys whose tenant also sends extra searches at --flood-multiplier times its rate; off when unset")
    p.add_argument("--flood-qps", type=float, default=0, help="the flood tenant's configured query limit on the server (its plan's qps); 0 = use the run's current query rate")
    p.add_argument("--flood-multiplier", type=float, default=10, help="the flooder sends at this multiple of --flood-qps")
    p.add_argument("--flood-concurrency", type=int, default=64, help="flood searches in flight at most; beyond it they are dropped and counted")
    p.add_argument("--victim-p99-ms", type=float, default=-1, help="with --flood-tenant-key: p99 bound over the other tenants' queries during the flood; -1 = off")
    p.add_argument("--out", help="write the JSON report here")
    args = p.parse_args(argv)
    defaults = SMOKE if args.smoke else FULL
    for k, v in defaults.items():
        if getattr(args, k) is None:
            setattr(args, k, v)
    return args


def main() -> int:
    args = parse_args()
    if args.self_test:
        return self_test()
    driver = Driver(args)
    print("soak %s: %d namespaces (large %d docs, Zipf %d docs), %.0f docs/s, %.0f->%.0f QPS, %.0fs; write %s, query %s" % (
        args.run_id, len(driver.namespaces), args.large_docs, args.zipf_docs, args.docs_per_s, args.qps_start, args.qps_end,
        args.duration, driver.write_url, ", ".join(driver.query_urls)), flush=True)
    report = driver.run()
    for v in report["verdicts"]:
        print("  %-22s %s  %s" % (v["check"], "ok" if v["ok"] else "FAIL", v["detail"]))
    print("latency: " + json.dumps(report["latency"]))
    print("visibility: " + json.dumps(report["visibility"]))
    print("requests: " + json.dumps(report["requests"]) + ", dropped queries %d" % report["dropped_queries"])
    if report.get("flood"):
        f = report["flood"]
        print("flood (tenant %s, x%s): %s, 429 rate %s, dropped %d" % (f["tenant"], f["multiplier"], json.dumps(f["requests"]), f["rate_429"], f["dropped"]))
        print("victims %s: latency %s, requests %s" % (",".join(f["victim"]["tenants"]), json.dumps(f["victim"]["latency"]), json.dumps(f["victim"]["requests"])))
    print("SOAK %s" % ("PASSED" if report["pass"] else "FAILED"))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(report, f, indent=1, default=str)
        print("report written to %s" % args.out)
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
