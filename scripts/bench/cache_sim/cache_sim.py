#!/usr/bin/env python3
"""Offline cache / tiering simulator for research issue #389.

Replays a per-search read trace against byte-bounded caches (FIFO = Kosha's
disk cache today, LRU, SIEVE, S3-FIFO) and against static cost-driven
placement (the five-minute rule), and prices each outcome: S3 GETs, S3
bytes, local GB-hours, and $ per 1k queries. Standard library only.

Subcommands
-----------
replay   Trace + file sizes -> hit ratio, GETs, bytes, $ per policy and size.
logcost  Price what archived server logs say searches actually fetched
         (``search timing:`` lines): today's measured cost, no simulation.
arm      Summarize latency, S3 work, and local GiB-hours for one archived
         timed benchmark run, excluding its unmeasured warm fill.
p99ci    Bootstrap confidence intervals for p99 (and p50) latency of two bench
         arms from query_bench.py ``--raw-out`` files, and for their
         difference: decides whether a bounded arm is "within noise".
synth    Write a synthetic trace + sizes file (Zipf segments plus one-touch
         scans). For testing the tool only; numbers from it are not Kosha
         measurements.

Inputs
------
Trace (``--trace``, JSONL, optionally .gz), written by the server when
``KOSHA_CACHE_TRACE_PATH`` is set (crates/kosha-server/src/cache_reads.rs),
one line per successful search, format v1::

    {"v": 1, "t_ms": 1763700000123, "ns": "default/msmarco",
     "segs": ["default/msmarco/seg-a", ...],  # every segment dir searched
     "classes": ["scoring", "postings"],       # + "vectors" for kNN
     "postings": ["postings-3f.bin", ...],     # exact shards; null = all
     "pages": ["default/msmarco/seg-a"]}       # doc stores read for the page

Segment directories are data-dir relative and, on the server, tenant
scoped: ``{tenant}/{ns}/{segment}`` (#262). Older traces/tests use
``{ns}/{segment}``; any depth works.

Lines that do not parse as JSON (e.g. ``docker logs --timestamps`` output
around them) are skipped, as is a leading timestamp before the ``{``.

Sizes (``--sizes``, TSV, optionally .gz): ``<path>\\t<bytes>`` for every
segment file, ``<path>`` being ``<segment dir>/<file>`` relative to the
data dir. Use the S3 segment listing, which holds every file whole::

    aws s3 ls --recursive s3://<segment bucket>/segments/ \\
      | awk '{print $4"\\t"$3}' | gzip > s3-sizes.tsv.gz
    cache_sim.py replay --sizes s3-sizes.tsv.gz --strip-prefix segments/ ...

(benchmark.yml writes this as ``server/s3-sizes.tsv.gz`` when
``cache_trace`` is on). A listing of the *local* data dir is not enough:
under ``warm_gate=queries``/``timed`` span promotion is off, so doc stores,
posting shards and ``vector.idx`` reads are local only as
``<file>.spans/<range>`` files, and a local listing would model the
scoring files only. Span entries are therefore ignored by default
(``--span-files skip``); ``--span-files sum`` instead sizes a file that
has no whole-file entry by the sum of its cached spans (a lower bound,
for when no S3 listing exists). ``.tmp.`` files are always ignored.

A trace line expands to the files of each named segment in the named
classes (posting shards narrowed to the listed ones). Segments missing
from the sizes listing, and (segment, class) pairs that resolve to no
file, are counted and reported; ``replay`` exits 2 when more than
``--max-missing-frac`` (default 0.05) of the segment references are
missing, so a path-layout mismatch cannot print all-zero rows silently.

Server log (``logcost --log``): ``docker logs`` output, optionally .gz;
only ``search timing:`` lines are read.

Cost model (all overridable; defaults are list-price assumptions, not
measurements): S3 GET $0.0004 per 1k requests; same-region transfer free;
local NVMe $0.16/GB-month (Durner et al. VLDB'23: c5d vs c5 instance-store
premium, $158.4/TB-month); memory $2.50/GB-month (rough r- vs c-family
premium); 730 h per month. One GET per whole-file miss unless
``--get-part-mb`` splits large files.
"""

from __future__ import annotations

import argparse
import collections
import csv
import gzip
import json
import math
import random
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

CLASSES = ("scoring", "postings", "docstore", "vectors")


# ---------------------------------------------------------------------------
# File classes (mirror kosha-server warm.rs FileClass::of_file)


def file_class(name: str) -> str | None:
    if name in ("footer.json", "inverted.idx", "doc_store.offsets", "filters.bin"):
        return "scoring"
    if name == "filters.toc" or re.fullmatch(r"filters-\d+\.col", name):
        return "scoring"
    if name in ("doc_store.bin", "doc_vectors.bin"):
        return "docstore"
    if re.fullmatch(r"vector\.(\d+\.)?(idx|offsets)", name):
        return "vectors"
    if name.startswith("postings-") and name.endswith(".bin"):
        return "postings"
    return None


def _open(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, encoding="utf-8", errors="replace")


@dataclass
class SegmentFiles:
    by_class: dict = field(default_factory=lambda: collections.defaultdict(list))


SPANS_SUFFIX = ".spans"


def split_segment_path(rel: str):
    """``<segment dir>/<file>[.spans/<range>]`` -> (segment dir, file, is_span).

    The segment dir is everything before the segment-level file, at any
    depth (``{tenant}/{ns}/{segment}`` on the server). None when the path
    has no directory part.
    """
    comps = [c for c in rel.strip("/").split("/") if c]
    for i, c in enumerate(comps[:-1]):
        if c.endswith(SPANS_SUFFIX) and len(c) > len(SPANS_SUFFIX):
            if i == 0:
                return None
            return "/".join(comps[:i]), c[: -len(SPANS_SUFFIX)], True
    if len(comps) < 2:
        return None
    return "/".join(comps[:-1]), comps[-1], False


def load_sizes(path: str, strip_prefix: str = "", span_files: str = "skip",
               stats: collections.Counter | None = None) -> dict:
    """{segment dir: SegmentFiles} with (key, file name, size) per class."""
    if span_files not in ("skip", "sum"):
        raise ValueError(f"span_files must be skip or sum, got {span_files!r}")
    stats = stats if stats is not None else collections.Counter()
    segs: dict = collections.defaultdict(SegmentFiles)
    whole: set = set()
    spans: dict = collections.Counter()
    prefix = strip_prefix.strip("/")
    with _open(path) as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 2:
                continue
            rel, size = parts[0].strip().strip("/"), parts[1].strip()
            if prefix:
                if rel != prefix and not rel.startswith(prefix + "/"):
                    stats["sizes_outside_prefix"] += 1
                    continue
                rel = rel[len(prefix):].strip("/")
            if ".tmp." in rel or not size.isdigit():
                continue
            split = split_segment_path(rel)
            if split is None:
                continue
            seg, name, is_span = split
            if is_span:
                spans[(seg, name)] += int(size)
                stats["span_entries"] += 1
                continue
            cls = file_class(name)
            if cls is None:
                continue
            whole.add((seg, name))
            segs[seg].by_class[cls].append((f"{seg}/{name}", name, int(size)))
    for (seg, name), total in spans.items():
        if (seg, name) in whole:
            continue
        stats["span_only_files"] += 1
        cls = file_class(name)
        if span_files == "sum" and cls is not None:
            segs[seg].by_class[cls].append((f"{seg}/{name}", name, total))
    return segs


@dataclass
class Query:
    t_ms: int
    objects: list  # [(key, size)]


def load_trace(path: str, sizes: dict) -> tuple[list, dict]:
    queries, stats = [], collections.Counter()
    with _open(path) as f:
        for line in f:
            brace = line.find("{")
            if brace < 0:
                continue
            try:
                rec = json.loads(line[brace:])
            except json.JSONDecodeError:
                stats["bad_lines"] += 1
                continue
            if rec.get("v") != 1:
                stats["bad_lines"] += 1
                continue
            queries.append(expand(rec, sizes, stats))
    queries.sort(key=lambda q: q.t_ms)
    return queries, stats


def expand(rec: dict, sizes: dict, stats: collections.Counter) -> Query:
    classes = set(rec.get("classes") or ["scoring"])
    shards = rec.get("postings")
    shards = set(shards) if shards is not None else None
    seen, objs = set(), []

    def add(key, size):
        if key not in seen:
            seen.add(key)
            objs.append((key, size))

    for seg in rec.get("segs") or []:
        stats["segment_refs"] += 1
        files = sizes.get(seg)
        if files is None:
            stats["segments_missing_sizes"] += 1
            continue
        for cls in classes:
            found = 0
            for key, name, size in files.by_class.get(cls, ()):
                if cls == "postings" and shards is not None and name not in shards:
                    continue
                add(key, size)
                found += 1
            stats[f"{cls}_resolved" if found else f"{cls}_unresolved"] += 1
    for seg in rec.get("pages") or []:
        stats["segment_refs"] += 1
        files = sizes.get(seg)
        if files is None:
            stats["segments_missing_sizes"] += 1
            continue
        found = files.by_class.get("docstore", ())
        stats["docstore_resolved" if found else "docstore_unresolved"] += 1
        for key, _name, size in found:
            add(key, size)
    return Query(int(rec.get("t_ms") or 0), objs)


# ---------------------------------------------------------------------------
# Byte-bounded caches. access(key, size) -> hit?  A miss inserts the object
# (evicting first, so a new object is never its own victim) unless it is
# larger than the whole cache. The inserting access is not a re-read.


class FifoCache:
    """Insertion order, no read signal: Kosha's disk cache today."""

    name = "fifo"

    def __init__(self, capacity: int):
        self.cap, self.used = capacity, 0
        self.q: collections.OrderedDict = collections.OrderedDict()

    def access(self, key, size) -> bool:
        if key in self.q:
            self.on_hit(key)
            return True
        if size > self.cap:
            return False
        while self.used + size > self.cap and self.q:
            _, s = self.q.popitem(last=False)
            self.used -= s
        self.q[key] = size
        self.used += size
        return False

    def on_hit(self, key):
        pass


class LruCache(FifoCache):
    name = "lru"

    def on_hit(self, key):
        self.q.move_to_end(key)


class SieveCache:
    """SIEVE (NSDI'24): FIFO + visited bit + hand moving oldest -> newest."""

    name = "sieve"

    def __init__(self, capacity: int):
        self.cap, self.used = capacity, 0
        # Doubly linked list, tail = oldest, head = newest.
        self.newer: dict = {}
        self.older: dict = {}
        self.size: dict = {}
        self.visited: dict = {}
        self.head = self.tail = self.hand = None

    def access(self, key, size) -> bool:
        if key in self.size:
            self.visited[key] = True
            return True
        if size > self.cap:
            return False
        while self.used + size > self.cap and self.size:
            self._evict()
        self.size[key], self.visited[key] = size, False
        self.older[key], self.newer[key] = self.head, None
        if self.head is not None:
            self.newer[self.head] = key
        self.head = key
        if self.tail is None:
            self.tail = key
        self.used += size
        return False

    def _evict(self):
        obj = self.hand if self.hand in self.size else self.tail
        while self.visited[obj]:
            self.visited[obj] = False
            obj = self.newer[obj] if self.newer[obj] is not None else self.tail
        self.hand = self.newer[obj]
        o, n = self.older.pop(obj), self.newer.pop(obj)
        if o is not None:
            self.newer[o] = n
        else:
            self.tail = n
        if n is not None:
            self.older[n] = o
        else:
            self.head = o
        self.used -= self.size.pop(obj)
        del self.visited[obj]


class S3FifoCache:
    """S3-FIFO (SOSP'23), byte-weighted: small 10%, main, ghost, freq<=3."""

    name = "s3fifo"

    def __init__(self, capacity: int, small_frac: float = 0.1):
        self.cap = capacity
        self.small_target = capacity * small_frac
        self.small: collections.OrderedDict = collections.OrderedDict()  # key -> size
        self.main: collections.OrderedDict = collections.OrderedDict()
        self.freq: dict = {}
        self.small_bytes = self.main_bytes = 0
        self.ghost: collections.OrderedDict = collections.OrderedDict()

    def access(self, key, size) -> bool:
        if key in self.small or key in self.main:
            self.freq[key] = min(self.freq[key] + 1, 3)
            return True
        if size > self.cap:
            return False
        while self.small_bytes + self.main_bytes + size > self.cap and (self.small or self.main):
            self._evict()
        self.freq[key] = 0
        if key in self.ghost:
            del self.ghost[key]
            self.main[key] = size
            self.main_bytes += size
        else:
            self.small[key] = size
            self.small_bytes += size
        return False

    def _evict(self):
        while True:
            if self.small and (self.small_bytes >= self.small_target or not self.main):
                key, size = self.small.popitem(last=False)
                self.small_bytes -= size
                if self.freq[key] > 1:
                    self.main[key] = size
                    self.main_bytes += size
                    continue
                del self.freq[key]
                self.ghost[key] = None
                while len(self.ghost) > max(len(self.main), 64):
                    self.ghost.popitem(last=False)
                return
            key, size = self.main.popitem(last=False)
            if self.freq[key] > 0:
                self.freq[key] -= 1
                self.main[key] = size
                continue
            self.main_bytes -= size
            del self.freq[key]
            return


POLICIES = {c.name: c for c in (FifoCache, LruCache, SieveCache, S3FifoCache)}


# ---------------------------------------------------------------------------
# Cost model


@dataclass
class Costs:
    get_per_1k: float = 0.0004
    transfer_per_gb: float = 0.0
    nvme_gb_month: float = 0.16
    mem_gb_month: float = 2.50
    nvme_read_cost: float = 0.0  # $ per local read, for the memory tier
    get_part_mb: float = 0.0  # 0 = one GET per whole-file miss
    hours_per_month: float = 730.0

    def gets_for(self, size: int) -> int:
        if self.get_part_mb <= 0:
            return 1
        return max(1, math.ceil(size / (self.get_part_mb * 1048576)))

    def per_get(self) -> float:
        return self.get_per_1k / 1000.0

    def nvme_per_byte_s(self) -> float:
        return self.nvme_gb_month / (1 << 30) / (self.hours_per_month * 3600)

    def mem_per_byte_s(self) -> float:
        return self.mem_gb_month / (1 << 30) / (self.hours_per_month * 3600)

    def break_even_s(self, size: int) -> float:
        """Five-minute rule: keep locally if re-read within this interval."""
        return self.gets_for(size) * self.per_get() / max(size * self.nvme_per_byte_s(), 1e-30)


@dataclass
class Result:
    policy: str
    capacity: int
    queries: int = 0
    accesses: int = 0
    hits: int = 0
    bytes_read: int = 0
    bytes_hit: int = 0
    gets: int = 0
    s3_bytes: int = 0
    footprint: int = 0  # bytes held locally (provisioned for caches)
    mem_bytes: int = 0
    duration_s: float = 0.0
    per_query_misses: list = field(default_factory=list)

    def dollars(self, c: Costs) -> dict:
        get_cost = self.gets * c.per_get() + self.s3_bytes / (1 << 30) * c.transfer_per_gb
        local = (self.footprint - self.mem_bytes) * c.nvme_per_byte_s() * self.duration_s
        mem = self.mem_bytes * c.mem_per_byte_s() * self.duration_s
        total = get_cost + local + mem
        return {
            "s3_dollars": get_cost,
            "local_dollars": local + mem,
            "total_dollars": total,
            "dollars_per_1k_queries": total / self.queries * 1000 if self.queries else 0.0,
        }

    def row(self, c: Costs) -> dict:
        misses = sorted(self.per_query_misses)

        def pct(p):
            return misses[min(len(misses) - 1, int(p * len(misses)))] if misses else 0

        out = {
            "policy": self.policy,
            "capacity_gb": round(self.capacity / (1 << 30), 3),
            "footprint_gb": round(self.footprint / (1 << 30), 3),
            "queries": self.queries,
            "hit_ratio": self.hits / self.accesses if self.accesses else 0.0,
            "byte_hit_ratio": self.bytes_hit / self.bytes_read if self.bytes_read else 0.0,
            "gets": self.gets,
            "gets_per_query": self.gets / self.queries if self.queries else 0.0,
            "s3_gb": self.s3_bytes / (1 << 30),
            "miss_files_p50": pct(0.50),
            "miss_files_p99": pct(0.99),
            "queries_with_miss": sum(1 for m in misses if m) / len(misses) if misses else 0.0,
        }
        out.update(self.dollars(c))
        return out


def split_train(queries: list, train_frac: float) -> int:
    return int(len(queries) * train_frac)


def duration_s(queries: list) -> float:
    if len(queries) < 2:
        return 1.0
    return max((queries[-1].t_ms - queries[0].t_ms) / 1000.0, 1.0)


def simulate_cache(policy: str, capacity: int, queries: list, start: int, c: Costs) -> Result:
    cache = POLICIES[policy](capacity)
    res = Result(policy, capacity, footprint=capacity, duration_s=duration_s(queries[start:]))
    for i, q in enumerate(queries):
        measure = i >= start
        misses = 0
        for key, size in q.objects:
            hit = cache.access(key, size)
            if not measure:
                continue
            res.accesses += 1
            res.bytes_read += size
            if hit:
                res.hits += 1
                res.bytes_hit += size
            else:
                misses += 1
                res.gets += c.gets_for(size)
                res.s3_bytes += size
        if measure:
            res.queries += 1
            res.per_query_misses.append(misses)
    return res


def object_rates(queries: list) -> tuple[dict, dict, float]:
    reads, sizes = collections.Counter(), {}
    for q in queries:
        for key, size in q.objects:
            reads[key] += 1
            sizes[key] = size
    return reads, sizes, duration_s(queries)


def static_placement(queries: list, start: int, c: Costs, capacity: int | None) -> Result:
    """Place objects using rates from queries[:start]; evaluate on the rest.

    capacity None: the five-minute rule, unconstrained (keep an object iff its
    expected GET cost exceeds its local storage cost; memory iff local reads
    cost more than the memory premium). With a capacity: greedy by expected
    GET $ saved per byte, the best static set that fits.
    """
    train = queries[:start] or queries
    reads, sizes, dur = object_rates(train)
    local, mem = set(), set()
    ranked = []
    for key, n in reads.items():
        size = sizes[key]
        rate = n / dur
        saved = rate * c.gets_for(size) * c.per_get()
        ranked.append((saved / max(size, 1), key, size, rate, saved))
    if capacity is None:
        for _, key, size, rate, saved in ranked:
            if saved >= size * c.nvme_per_byte_s():
                local.add(key)
                premium = size * (c.mem_per_byte_s() - c.nvme_per_byte_s())
                if c.nvme_read_cost > 0 and rate * c.nvme_read_cost >= premium:
                    mem.add(key)
        name = "fivemin"
    else:
        used = 0
        for _, key, size, _rate, _saved in sorted(ranked, reverse=True):
            if used + size <= capacity:
                local.add(key)
                used += size
        name = "static"
    footprint = sum(sizes[k] for k in local)
    res = Result(
        name,
        capacity if capacity is not None else footprint,
        footprint=footprint,
        mem_bytes=sum(sizes[k] for k in mem),
        duration_s=duration_s(queries[start:]),
    )
    for q in queries[start:]:
        misses = 0
        res.queries += 1
        for key, size in q.objects:
            res.accesses += 1
            res.bytes_read += size
            if key in local:
                res.hits += 1
                res.bytes_hit += size
            else:
                misses += 1
                res.gets += c.gets_for(size)
                res.s3_bytes += size
        res.per_query_misses.append(misses)
    return res


def working_set_bytes(queries: list) -> int:
    sizes = {}
    for q in queries:
        for key, size in q.objects:
            sizes[key] = size
    return sum(sizes.values())


def run_replay(queries: list, capacities: list, policies: list, c: Costs, train_frac: float) -> list:
    start = split_train(queries, train_frac)
    rows = []
    for cap in capacities:
        for p in policies:
            if p in POLICIES:
                rows.append(simulate_cache(p, cap, queries, start, c).row(c))
            elif p == "static":
                rows.append(static_placement(queries, start, c, cap).row(c))
    if "fivemin" in policies:
        rows.append(static_placement(queries, start, c, None).row(c))
    return rows


# ---------------------------------------------------------------------------
# logcost: what searches actually fetched, from `search timing:` lines

TIMING_RE = re.compile(r"search timing: (.*)$")
KV_RE = re.compile(r"(\w+)=(\S+)")
GET_COUNTERS = ("hydrate_files", "page_gets", "page_whole_files", "postings_gets",
                "vector_gets", "vector_whole_files")


def parse_timing_lines(path: str) -> list:
    rows = []
    with _open(path) as f:
        for line in f:
            m = TIMING_RE.search(line)
            if not m:
                continue
            kv = dict(KV_RE.findall(m.group(1)))
            rows.append(kv)
    return rows


def _num(kv: dict, key: str) -> float:
    try:
        return float(kv.get(key, 0))
    except ValueError:
        return 0.0


def run_logcost(rows: list, c: Costs, duration: float | None) -> dict:
    """GETs per search, approximated from the logged counters: whole-file
    hydration (scaffolding + whole posting shards: `hydrate_files`), ranged
    posting / page / vector GETs, and whole page / vector files."""
    gets = bytes_ = 0.0
    with_s3 = 0
    totals = []
    for kv in rows:
        g = sum(_num(kv, k) for k in GET_COUNTERS)
        gets += g
        bytes_ += _num(kv, "s3_bytes")
        with_s3 += g > 0
        totals.append(_num(kv, "total_ms"))
    n = len(rows)
    totals.sort()
    out = {
        "searches": n,
        "gets": int(gets),
        "gets_per_query": gets / n if n else 0.0,
        "s3_gb": bytes_ / (1 << 30),
        "queries_with_s3": with_s3 / n if n else 0.0,
        "s3_dollars": gets * c.per_get() + bytes_ / (1 << 30) * c.transfer_per_gb,
        "total_ms_p50": totals[int(0.5 * n)] if n else 0.0,
        "total_ms_p99": totals[min(n - 1, int(0.99 * n))] if n else 0.0,
    }
    out["s3_dollars_per_1k_queries"] = out["s3_dollars"] / n * 1000 if n else 0.0
    if duration:
        out["note"] = f"local storage $ needs the footprint; at {duration:.0f}s see replay"
    return out


# ---------------------------------------------------------------------------
# p99ci: bootstrap CIs for bench arms (query_bench.py --raw-out files)


def load_latencies(paths: list) -> tuple[list, int]:
    """latency_ms of successful requests across raw-out files, plus errors."""
    out, errors = [], 0
    for path in paths:
        with _open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("error") or rec.get("latency_ms") is None:
                    errors += 1
                    continue
                out.append(float(rec["latency_ms"]))
    return out, errors


def pctl(sorted_vals: list, p: float) -> float:
    """Linear-interpolated percentile (same rule as query_bench.py)."""
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * p
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def bootstrap_ci(a: list, b: list | None, p: float, iters: int, seed: int, alpha: float = 0.05) -> dict:
    """Percentile-bootstrap CI of pctl(a) (and of pctl(b) - pctl(a))."""
    rng = random.Random(seed)
    est_a = pctl(sorted(a), p)
    samples_a, diffs = [], []
    for _ in range(iters):
        ra = sorted(rng.choices(a, k=len(a)))
        va = pctl(ra, p)
        samples_a.append(va)
        if b is not None:
            rb = sorted(rng.choices(b, k=len(b)))
            diffs.append(pctl(rb, p) - va)
    samples_a.sort()
    diffs.sort()
    lo, hi = alpha / 2, 1 - alpha / 2
    out = {"a": est_a, "a_ci": [pctl(samples_a, lo), pctl(samples_a, hi)]}
    if b is not None:
        out["b"] = pctl(sorted(b), p)
        out["diff_b_minus_a"] = out["b"] - est_a
        out["diff_ci"] = [pctl(diffs, lo), pctl(diffs, hi)]
    return out


def run_p99ci(a_paths: list, b_paths: list, iters: int, seed: int) -> dict:
    a, a_err = load_latencies(a_paths)
    b, b_err = load_latencies(b_paths) if b_paths else ([], 0)
    if not a or (b_paths and not b):
        raise SystemExit("p99ci: an arm has no successful requests")
    out = {"a_files": a_paths, "b_files": b_paths, "a_requests": len(a), "a_errors": a_err,
           "b_requests": len(b), "b_errors": b_err, "iters": iters,
           "a_tail_samples_p99": sum(1 for v in a if v >= pctl(sorted(a), 0.99))}
    for name, p in (("p50", 0.50), ("p99", 0.99)):
        out[name] = bootstrap_ci(a, b if b_paths else None, p, iters, seed)
    return out


# ---------------------------------------------------------------------------
# arm: measured storage costs and latency from a timed benchmark artifact


def footprint_byte_seconds(samples: list, start: tuple, end: tuple) -> float:
    """Integrate with linear interpolation, using samples inside this phase only."""
    points = [start, *(p for p in samples if start[0] < p[0] < end[0]), end]
    return sum((t1 - t0) * (b0 + b1) / 2
               for (t0, b0), (t1, b1) in zip(points, points[1:]))


def run_arm(run_dir: Path, c: Costs) -> dict:
    meta = json.loads((run_dir / "meta.json").read_text())
    suite = meta["suite"]
    if meta["warm_gate"] != "timed" or suite not in ("bm25", "vector"):
        raise ValueError("expected a timed benchmark with suite=bm25 or vector")
    repeats = int(meta["phase_repeats"])
    if not 1 <= repeats <= 5:
        raise ValueError("phase_repeats must be in [1, 5]")
    prefix = "warm_or_nocache" if suite == "bm25" else "knn_warm_nocache"
    labels = [f"{prefix}_r{i}" for i in range(1, repeats + 1)]
    suite_dir = run_dir / suite
    with (suite_dir / "footprint.tsv").open() as f:
        samples = [(float(t), int(b)) for t, b in csv.reader(f, delimiter="\t")]
    if (any(not math.isfinite(t) or b < 0 for t, b in samples)
            or any(a[0] >= b[0] for a, b in zip(samples, samples[1:]))):
        raise ValueError("invalid or out-of-order footprint samples")
    with (suite_dir / "phase_footprint.tsv").open() as f:
        windows = list(csv.DictReader(f, delimiter="\t"))
    if [w.get("phase") for w in windows] != labels:
        raise ValueError("phase_footprint.tsv does not match the expected measured repeats")

    phases, pooled, prev_end = [], [], -math.inf
    for label, w in zip(labels, windows):
        try:
            start = float(w["start_s"]), int(w["start_bytes"])
            end = float(w["end_s"]), int(w["end_bytes"])
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError(f"{label}: missing or invalid phase footprint boundary") from e
        if not (math.isfinite(start[0]) and math.isfinite(end[0])
                and prev_end <= start[0] < end[0] and min(start[1], end[1]) >= 0):
            raise ValueError(f"{label}: invalid phase footprint window")
        prev_end = end[0]
        occupancy = footprint_byte_seconds(samples, start, end)
        summary = json.loads((suite_dir / f"{label}.json").read_text())
        latencies, errors = load_latencies([str(suite_dir / f"{label}.requests.jsonl.gz")])
        requests = summary["requests"]
        if (not latencies or not all(math.isfinite(v) and v >= 0 for v in latencies)
                or (len(latencies), errors, len(latencies) + errors)
                != (summary["successes"], summary["errors"], requests)):
            raise ValueError(f"{label}: missing or invalid client request records")
        rows = parse_timing_lines(str(suite_dir / "server" / f"{label}.log.gz"))
        if not len(latencies) <= len(rows) <= requests:
            raise ValueError(f"{label}: expected {len(latencies)} to {requests} server search timings, got {len(rows)}")
        counters = (row.get(k, "") for row in rows for k in (*GET_COUNTERS, "s3_bytes"))
        if any(not (v.isascii() and v.isdecimal()) for v in counters):
            raise ValueError(f"{label}: missing or invalid S3 counters in server search timings")
        s3 = run_logcost(rows, c, None)
        pooled.extend(latencies)
        phases.append({
            "phase": label,
            "duration_s": end[0] - start[0],
            "requests": requests,
            "errors": errors,
            "knn_degraded_requests": summary["knn_degraded_requests"],
            "logged_searches": s3["searches"],
            "s3_gets": s3["gets"],
            "s3_gib": s3["s3_gb"],
            "s3_dollars": s3["s3_dollars"],
            "local_gib_hours": occupancy / (1 << 30) / 3600,
            "local_dollars": occupancy * c.nvme_per_byte_s(),
            "end_local_bytes": end[1],
            "p99_ms": pctl(sorted(latencies), 0.99),
        })

    totals = {k: sum(p[k] for p in phases) for k in phases[0]
              if k not in ("phase", "end_local_bytes", "p99_ms")}
    n, seconds = totals["requests"], totals["duration_s"]
    total_dollars = totals["s3_dollars"] + totals["local_dollars"]
    pooled.sort()
    return {
        "run": meta,
        "phases": phases,
        "totals": totals | {
            "p50_ms": pctl(pooled, 0.50),
            "p90_ms": pctl(pooled, 0.90),
            "p99_ms": pctl(pooled, 0.99),
            "gets_per_query": totals["s3_gets"] / n,
            "s3_bytes_per_query": totals["s3_gib"] * (1 << 30) / n,
            "mean_local_bytes": totals["local_gib_hours"] * (1 << 30) * 3600 / seconds,
            "end_local_bytes": phases[-1]["end_local_bytes"],
            "total_dollars": total_dollars,
            "dollars_per_1k_queries": total_dollars * 1000 / n,
        },
        "prices": {"get_per_1k": c.get_per_1k, "transfer_per_gib": c.transfer_per_gb,
                   "local_gib_month": c.nvme_gb_month, "hours_per_month": c.hours_per_month},
    }


# ---------------------------------------------------------------------------
# synth: a synthetic trace for exercising the tool (NOT Kosha data)


def synth(n_queries: int, n_segments: int, qps: float, scan_every: int, seed: int,
          tenant: str = ""):
    rng = random.Random(seed)
    sizes, trace = [], []
    t_prefix = f"{tenant}/" if tenant else ""
    hot_ns, cold_ns = f"{t_prefix}hot", f"{t_prefix}cold"
    for ns in (hot_ns, cold_ns):
        for s in range(n_segments):
            seg = f"{ns}/seg-{s:04d}"
            sizes.append((f"{seg}/footer.json", 4096))
            sizes.append((f"{seg}/inverted.idx", 2 << 20))
            sizes.append((f"{seg}/doc_store.offsets", 1 << 20))
            sizes.append((f"{seg}/doc_store.bin", 64 << 20))
            for sh in range(16):
                sizes.append((f"{seg}/postings-{sh:02x}.bin", 4 << 20))
    weights = [1.0 / (i + 1) for i in range(16)]
    t = 1_700_000_000_000
    scan_seg = 0
    for i in range(n_queries):
        t += int(1000 / qps)
        if scan_every and i % scan_every == scan_every - 1:
            # one-touch scan: a cold segment, read once
            segs = [f"{cold_ns}/seg-{scan_seg % n_segments:04d}"]
            scan_seg += 1
            shards = None
        else:
            segs = [f"{hot_ns}/seg-{s:04d}" for s in range(n_segments)]
            shards = sorted({f"postings-{rng.choices(range(16), weights)[0]:02x}.bin" for _ in range(3)})
        page = [rng.choice(segs)]
        trace.append(
            {"v": 1, "t_ms": t, "ns": segs[0].rsplit("/", 1)[0], "segs": segs,
             "classes": ["scoring", "postings"], "postings": shards, "pages": page}
        )
    return sizes, trace


# ---------------------------------------------------------------------------
# CLI


def parse_capacities(args, ws: int) -> list:
    caps = []
    for g in filter(None, (args.capacity_gb or "").split(",")):
        caps.append(int(float(g) * (1 << 30)))
    for fr in filter(None, (args.capacity_frac or "").split(",")):
        caps.append(int(float(fr) * ws))
    return caps or [int(ws * f) for f in (0.1, 0.25, 0.5, 1.0)]


def print_table(rows: list):
    cols = [
        "policy", "capacity_gb", "footprint_gb", "hit_ratio", "byte_hit_ratio",
        "gets_per_query", "s3_gb", "miss_files_p99", "queries_with_miss",
        "dollars_per_1k_queries",
    ]
    print("\t".join(cols))
    for r in rows:
        print("\t".join(f"{r[c]:.4g}" if isinstance(r[c], float) else str(r[c]) for c in cols))


def add_cost_args(p):
    p.add_argument("--get-per-1k", type=float, default=Costs.get_per_1k)
    p.add_argument("--transfer-per-gb", type=float, default=Costs.transfer_per_gb)
    p.add_argument("--nvme-gb-month", type=float, default=Costs.nvme_gb_month,
                   help="local tier $/GB-month (instance NVMe ~0.16, gp3 ~0.08)")
    p.add_argument("--mem-gb-month", type=float, default=Costs.mem_gb_month)
    p.add_argument("--nvme-read-cost", type=float, default=Costs.nvme_read_cost,
                   help="$ per local read; > 0 enables the memory tier in fivemin")
    p.add_argument("--get-part-mb", type=float, default=Costs.get_part_mb)


def costs_from(args) -> Costs:
    return Costs(
        get_per_1k=args.get_per_1k,
        transfer_per_gb=args.transfer_per_gb,
        nvme_gb_month=args.nvme_gb_month,
        mem_gb_month=args.mem_gb_month,
        nvme_read_cost=args.nvme_read_cost,
        get_part_mb=args.get_part_mb,
    )


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("replay", help="simulate policies over a trace")
    r.add_argument("--trace", required=True)
    r.add_argument("--sizes", required=True)
    r.add_argument("--capacity-gb", help="comma list of cache sizes in GiB")
    r.add_argument("--capacity-frac", help="comma list of fractions of the trace working set")
    r.add_argument("--policies", default="fifo,lru,sieve,s3fifo,static,fivemin")
    r.add_argument("--train-frac", type=float, default=0.5,
                   help="leading share of the trace used to warm caches / estimate rates; "
                        "metrics cover the rest")
    r.add_argument("--json", help="also write rows as JSON here")
    r.add_argument("--strip-prefix", default="",
                   help="drop this leading path from every sizes entry (e.g. segments/ "
                        "for an S3 listing)")
    r.add_argument("--span-files", choices=["skip", "sum"], default="skip",
                   help="span-cache entries: skip (default; use an S3 listing) or size a "
                        "file with no whole-file entry by the sum of its spans")
    r.add_argument("--max-missing-frac", type=float, default=0.05,
                   help="exit 2 when more than this share of trace segment references "
                        "has no sizes (layout or prefix mismatch)")
    add_cost_args(r)

    lc = sub.add_parser("logcost", help="price searches from search timing lines")
    lc.add_argument("--log", required=True)
    lc.add_argument("--json")
    add_cost_args(lc)

    arm = sub.add_parser("arm", help="report measured cost, footprint and latency for a timed run")
    arm.add_argument("--run-dir", type=Path, required=True,
                     help="unpacked bench-results directory (contains meta.json)")
    arm.add_argument("--get-per-1k", type=float, default=Costs.get_per_1k)
    arm.add_argument("--transfer-per-gb", type=float, default=Costs.transfer_per_gb)
    arm.add_argument("--nvme-gb-month", type=float, default=Costs.nvme_gb_month)

    s = sub.add_parser("synth", help="write a synthetic trace + sizes (testing only)")
    s.add_argument("--trace-out", required=True)
    s.add_argument("--sizes-out", required=True)
    s.add_argument("--queries", type=int, default=4000)
    s.add_argument("--segments", type=int, default=20)
    s.add_argument("--qps", type=float, default=8.0)
    s.add_argument("--scan-every", type=int, default=2)
    s.add_argument("--seed", type=int, default=389)
    s.add_argument("--tenant", default="",
                   help="prefix segment dirs with this tenant (the server layout)")

    ci = sub.add_parser("p99ci", help="bootstrap CIs for p50/p99 of bench arms")
    ci.add_argument("--a", required=True,
                    help="comma list of query_bench --raw-out files (control arm, pooled)")
    ci.add_argument("--b", default="", help="comma list for the compared arm (pooled)")
    ci.add_argument("--iters", type=int, default=2000)
    ci.add_argument("--seed", type=int, default=389)
    ci.add_argument("--json")

    args = ap.parse_args(argv)
    if args.cmd == "synth":
        sizes, trace = synth(args.queries, args.segments, args.qps, args.scan_every, args.seed,
                             args.tenant)
        with open(args.sizes_out, "w") as f:
            for k, v in sizes:
                f.write(f"{k}\t{v}\n")
        with open(args.trace_out, "w") as f:
            for rec in trace:
                f.write(json.dumps(rec) + "\n")
        return 0

    if args.cmd == "p99ci":
        out = run_p99ci([p for p in args.a.split(",") if p], [p for p in args.b.split(",") if p],
                        args.iters, args.seed)
        print(json.dumps(out, indent=2))
        if args.json:
            with open(args.json, "w") as f:
                json.dump(out, f, indent=2)
        return 0

    if args.cmd == "arm":
        prices = {k: getattr(args, k) for k in ("get_per_1k", "transfer_per_gb", "nvme_gb_month")}
        if not all(math.isfinite(p) and p >= 0 for p in prices.values()):
            arm.error("prices must be finite and nonnegative")
        try:
            out = run_arm(args.run_dir, Costs(**prices))
        except (OSError, KeyError, TypeError, ValueError) as e:
            arm.error(str(e))
        print(json.dumps(out, indent=2))
        return 0

    c = costs_from(args)
    if args.cmd == "logcost":
        out = run_logcost(parse_timing_lines(args.log), c, None)
        print(json.dumps(out, indent=2))
        if args.json:
            with open(args.json, "w") as f:
                json.dump(out, f, indent=2)
        return 0

    size_stats: collections.Counter = collections.Counter()
    sizes = load_sizes(args.sizes, args.strip_prefix, args.span_files, size_stats)
    queries, stats = load_trace(args.trace, sizes)
    stats.update(size_stats)
    if not queries:
        print("no trace lines parsed", file=sys.stderr)
        return 1
    refs = stats.get("segment_refs", 0)
    missing = stats.get("segments_missing_sizes", 0)
    if refs and missing / refs > args.max_missing_frac:
        print(f"error: {missing}/{refs} trace segment references have no sizes "
              f"(sizes listing covers {len(sizes)} segment dirs, e.g. "
              f"{next(iter(sizes), None)!r}); check --strip-prefix and the path layout. "
              f"{dict(stats)}", file=sys.stderr)
        return 2
    if stats.get("span_only_files") and args.span_files == "skip":
        print(f"# warning: {stats['span_only_files']} files are present only as span-cache "
              f"entries and are not modelled; use an S3 listing for whole-file sizes",
              file=sys.stderr)
    ws = working_set_bytes(queries)
    caps = parse_capacities(args, ws)
    policies = [p.strip() for p in args.policies.split(",") if p.strip()]
    rows = run_replay(queries, caps, policies, c, args.train_frac)
    print(f"# queries={len(queries)} working_set_gb={ws / (1 << 30):.3f} "
          f"duration_s={duration_s(queries):.0f} {dict(stats)}")
    print(f"# break-even re-read interval for a 4 MiB file: "
          f"{c.break_even_s(4 << 20):.0f}s; 64 MiB: {c.break_even_s(64 << 20):.0f}s")
    print_table(rows)
    if args.json:
        with open(args.json, "w") as f:
            json.dump({"working_set_bytes": ws, "stats": dict(stats), "rows": rows}, f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
