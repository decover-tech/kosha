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
synth    Write a synthetic trace + sizes file (Zipf segments plus one-touch
         scans). For testing the tool only; numbers from it are not Kosha
         measurements.

Inputs
------
Trace (``--trace``, JSONL, optionally .gz), written by the server when
``KOSHA_CACHE_TRACE_PATH`` is set (crates/kosha-server/src/cache_reads.rs),
one line per successful search, format v1::

    {"v": 1, "t_ms": 1763700000123, "ns": "msmarco",
     "segs": ["msmarco/seg-a", ...],          # every segment searched
     "classes": ["scoring", "postings"],       # + "vectors" for kNN
     "postings": ["postings-3f.bin", ...],     # exact shards; null = all
     "pages": ["msmarco/seg-a"]}               # doc stores read for the page

Lines that do not parse as JSON (e.g. ``docker logs --timestamps`` output
around them) are skipped, as is a leading timestamp before the ``{``.

Sizes (``--sizes``, TSV, optionally .gz): ``<relative path>\\t<bytes>`` for
every segment file, relative to the data dir, e.g. from the bench VM::

    sudo find /data/kosha/data -type f -printf '%P\\t%s\\n' | gzip > sizes.tsv.gz

or from S3: ``aws s3 ls --recursive s3://bucket/prefix/ | awk '{print $4"\\t"$3}'``
(strip the prefix). Span-cache entries (``*.spans/...``) and temp files are
ignored: the simulator models whole files. A trace line expands to the
files of each named segment in the named classes (posting shards narrowed
to the listed ones); files missing from the sizes listing are skipped and
counted.

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
import gzip
import json
import math
import random
import re
import sys
from dataclasses import dataclass, field

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


def load_sizes(path: str) -> dict:
    """{segment dir: SegmentFiles} with (key, file name, size) per class."""
    segs: dict = collections.defaultdict(SegmentFiles)
    with _open(path) as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 2:
                continue
            rel, size = parts[0].strip().lstrip("/"), parts[1].strip()
            if ".spans/" in rel or ".tmp." in rel or not size.isdigit():
                continue
            comps = rel.split("/")
            if len(comps) != 3:
                continue
            cls = file_class(comps[2])
            if cls is None:
                continue
            segs[f"{comps[0]}/{comps[1]}"].by_class[cls].append((rel, comps[2], int(size)))
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
        files = sizes.get(seg)
        if files is None:
            stats["segments_missing_sizes"] += 1
            continue
        for cls in classes:
            for key, name, size in files.by_class.get(cls, ()):
                if cls == "postings" and shards is not None and name not in shards:
                    continue
                add(key, size)
    for seg in rec.get("pages") or []:
        files = sizes.get(seg)
        if files is None:
            stats["segments_missing_sizes"] += 1
            continue
        for key, _name, size in files.by_class.get("docstore", ()):
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
        g = sum(
            _num(kv, k)
            for k in (
                "hydrate_files",
                "page_gets",
                "page_whole_files",
                "postings_gets",
                "vector_gets",
                "vector_whole_files",
            )
        )
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
# synth: a synthetic trace for exercising the tool (NOT Kosha data)


def synth(n_queries: int, n_segments: int, qps: float, scan_every: int, seed: int):
    rng = random.Random(seed)
    sizes, trace = [], []
    hot_ns, cold_ns = "hot", "cold"
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
            {"v": 1, "t_ms": t, "ns": segs[0].split("/")[0], "segs": segs,
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
    add_cost_args(r)

    lc = sub.add_parser("logcost", help="price searches from search timing lines")
    lc.add_argument("--log", required=True)
    lc.add_argument("--json")
    add_cost_args(lc)

    s = sub.add_parser("synth", help="write a synthetic trace + sizes (testing only)")
    s.add_argument("--trace-out", required=True)
    s.add_argument("--sizes-out", required=True)
    s.add_argument("--queries", type=int, default=4000)
    s.add_argument("--segments", type=int, default=20)
    s.add_argument("--qps", type=float, default=8.0)
    s.add_argument("--scan-every", type=int, default=2)
    s.add_argument("--seed", type=int, default=389)

    args = ap.parse_args(argv)
    if args.cmd == "synth":
        sizes, trace = synth(args.queries, args.segments, args.qps, args.scan_every, args.seed)
        with open(args.sizes_out, "w") as f:
            for k, v in sizes:
                f.write(f"{k}\t{v}\n")
        with open(args.trace_out, "w") as f:
            for rec in trace:
                f.write(json.dumps(rec) + "\n")
        return 0

    c = costs_from(args)
    if args.cmd == "logcost":
        out = run_logcost(parse_timing_lines(args.log), c, None)
        print(json.dumps(out, indent=2))
        if args.json:
            with open(args.json, "w") as f:
                json.dump(out, f, indent=2)
        return 0

    sizes = load_sizes(args.sizes)
    queries, stats = load_trace(args.trace, sizes)
    if not queries:
        print("no trace lines parsed", file=sys.stderr)
        return 1
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
