"""Engine internals for a benchmark phase: where the time went, how much
work each query did, and what the server process used while it ran.

query_bench.py reports end-to-end p50/p90/p99. Those numbers say how fast,
not why. This module turns what the run already collects into a breakdown
by part:

- Stages: the server's own per-request phase timings (`timings: true` on
  each search, needs KOSHA_DASHBOARD on the server): queue, admit, hydrate,
  bm25_stats, score and materialize. For each, p50/p90/p99/mean and its
  share of mean server time, plus whatever the named stages don't cover.
- Tail vs median: mean stage times for the slowest 1% of requests next to
  the middle band (p45-p55), so a p99 can be read as "more hydrate" or
  "more score" instead of guessed at.
- Overheads outside the engine: client latency minus server total (HTTP,
  serialization, the client itself) and the load generator's send lag.
- I/O and cache: S3 round trips and bytes per request, the share of
  requests served fully from local data, and result-cache hits.
- Work counters: deltas of /v1/stats counters across the phase (scoring
  blocks decoded, posting and doc-store reads), per request and per hit.
- Resources: CPU cores and RSS of the kosha-server process, and host CPU,
  sampled once a second from /proc while the phase runs (Linux only).

Everything here is reporting: a missing field or an unreadable /proc
leaves that part out and never fails a run. Stdlib only, so the bench VM
needs nothing new.
"""
from __future__ import annotations

import os
import statistics
import threading
import time
from pathlib import Path

STAGES = ["queue_ms", "admit_ms", "hydrate_ms", "bm25_stats_ms", "score_ms", "materialize_ms"]

# /v1/stats subtrees whose numeric leaves are monotonic counters, so a
# before/after difference is the work the phase did.
STATS_COUNTERS = ["scoring_blocks_decoded", "posting_reads", "doc_store_reads"]


def pct(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    k = (len(s) - 1) * (p / 100)
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def dist(xs: list[float], ps=(50, 90, 99)) -> dict:
    out = {f"p{p}": round(pct(xs, p), 3) for p in ps}
    out["mean"] = round(statistics.fmean(xs), 3) if xs else float("nan")
    out["max"] = round(max(xs), 3) if xs else float("nan")
    return out


def _mean(xs):
    return statistics.fmean(xs) if xs else float("nan")


def stage_breakdown(rows: list[dict]) -> dict | None:
    """rows: one per successful request, {"latency_ms", "send_lag_ms",
    "hits", "server": <timings object or None>}. None when no request
    carried server timings (an older server, or KOSHA_DASHBOARD off)."""
    timed = [r for r in rows if isinstance(r.get("server"), dict)]
    if not timed:
        return None
    # A result-cache hit runs no stage, so stage shares come from the
    # requests that ran the engine; the cache rate is reported on its own.
    ran = [r for r in timed if not r["server"].get("cached")] or timed
    totals = [float(r["server"].get("total_ms", 0.0)) for r in ran]
    mean_total = _mean(totals)

    stages = {}
    covered = 0.0
    for st in STAGES:
        vals = [float(r["server"][st]) for r in ran if st in r["server"]]
        if not vals:
            continue
        d = dist(vals)
        m = _mean(vals)
        covered += m
        d["share"] = round(m / mean_total, 4) if mean_total > 0 else float("nan")
        stages[st] = d
    other = mean_total - covered
    if mean_total > 0:
        stages["other_ms"] = {"mean": round(other, 3), "share": round(other / mean_total, 4)}

    # Tail vs median band, by client latency (what the reader of p99 saw).
    by_lat = sorted(ran, key=lambda r: r["latency_ms"])
    n = len(by_lat)
    tail = by_lat[int(n * 0.99):] or by_lat[-1:]
    mid = by_lat[int(n * 0.45):max(int(n * 0.55), int(n * 0.45) + 1)]

    def band(rs):
        b = {"requests": len(rs), "client_ms": round(_mean([r["latency_ms"] for r in rs]), 3),
             "total_ms": round(_mean([float(r["server"].get("total_ms", 0.0)) for r in rs]), 3)}
        for st in STAGES:
            vals = [float(r["server"][st]) for r in rs if st in r["server"]]
            if vals:
                b[st] = round(_mean(vals), 3)
        rounds = [r["server"].get("s3_rounds") for r in rs if r["server"].get("s3_rounds") is not None]
        if rounds:
            b["s3_rounds"] = round(_mean(rounds), 3)
        return b

    overhead = [r["latency_ms"] - float(r["server"].get("total_ms", 0.0)) for r in timed]
    lags = [r["send_lag_ms"] for r in rows if r.get("send_lag_ms") is not None]
    rounds = [float(r["server"].get("s3_rounds", 0)) for r in timed]
    s3_bytes = [float(r["server"].get("s3_bytes", 0)) for r in timed]
    out = {
        "requests_with_timings": len(timed),
        "server_total_ms": dist(totals),
        "client_overhead_ms": dist(overhead),
        "send_lag_ms": dist(lags) if lags else None,
        "stages": stages,
        "tail_band": band(tail),
        "median_band": band(mid),
        "result_cache_hit_rate": round(sum(1 for r in timed if r["server"].get("cached")) / len(timed), 4),
        "fully_cached_rate": round(sum(1 for r in timed if r["server"].get("fully_cached")) / len(timed), 4),
        "s3_rounds_per_request": dist(rounds),
        "s3_mb_per_request": round(_mean(s3_bytes) / 1e6, 4),
    }
    knn = [float(r["server"]["knn_postings"]) for r in timed if r["server"].get("knn_postings") is not None]
    if knn:
        out["knn_postings"] = dist(knn)
    return out


def _flatten(prefix: str, v, out: dict):
    if isinstance(v, bool):
        return
    if isinstance(v, (int, float)):
        out[prefix] = v
    elif isinstance(v, dict):
        for k, sub in v.items():
            _flatten(f"{prefix}.{k}", sub, out)


def counter_deltas(before: dict | None, after: dict | None, requests: int, hits: int) -> dict | None:
    """Per-phase work from two /v1/stats bodies: total delta, per request and
    per returned hit, for each numeric leaf under STATS_COUNTERS."""
    if not before or not after:
        return None
    fb, fa = {}, {}
    for key in STATS_COUNTERS:
        _flatten(key, before.get(key), fb)
        _flatten(key, after.get(key), fa)
    out = {}
    for k, a in fa.items():
        if k not in fb:
            continue
        d = a - fb[k]
        out[k] = {
            "delta": d,
            "per_request": round(d / requests, 3) if requests else float("nan"),
            "per_hit": round(d / hits, 3) if hits else float("nan"),
        }
    return out or None


class ResourceSampler:
    """Samples the server process's CPU and RSS and host CPU once a second
    from /proc. The process is found by name (`kosha-server`, the bench
    container's entrypoint; containers share the host's /proc) or given
    by pid. A no-op off Linux or when the process isn't found."""

    def __init__(self, process_name: str = "kosha-server", pid: int | None = None, interval: float = 1.0):
        self.interval = interval
        self.pid = pid or self._find(process_name)
        self.tick = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
        self.ncpu = os.cpu_count() or 1
        self.samples: list[dict] = []
        self._stop = threading.Event()
        self._thread = None
        self._first = None

    @staticmethod
    def _find(name: str) -> int | None:
        proc = Path("/proc")
        if not proc.is_dir():
            return None
        for p in proc.iterdir():
            if not p.name.isdigit():
                continue
            try:
                if (p / "comm").read_text().strip() == name[:15]:
                    return int(p.name)
            except OSError:
                continue
        return None

    def _read(self) -> dict | None:
        try:
            stat = Path(f"/proc/{self.pid}/stat").read_text()
            # comm can contain spaces; fields after the closing paren.
            fields = stat[stat.rindex(")") + 2:].split()
            cpu_ticks = int(fields[11]) + int(fields[12])  # utime + stime
            threads = int(fields[17])
            rss_kb = 0
            for line in Path(f"/proc/{self.pid}/status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    rss_kb = int(line.split()[1])
                    break
            host = Path("/proc/stat").read_text().splitlines()[0].split()[1:]
            host_vals = [int(x) for x in host]
            host_idle = host_vals[3] + (host_vals[4] if len(host_vals) > 4 else 0)
            return {"t": time.time(), "cpu_s": cpu_ticks / self.tick, "rss_mb": rss_kb / 1024,
                    "threads": threads, "host_total": sum(host_vals), "host_idle": host_idle}
        except (OSError, ValueError, IndexError):
            return None

    def _run(self):
        while not self._stop.wait(self.interval):
            s = self._read()
            if s:
                self.samples.append(s)

    def start(self):
        if self.pid is None:
            return self
        self._first = self._read()
        if self._first is None:
            return self
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def stop(self, requests: int) -> dict | None:
        if self._thread is None:
            return None
        self._stop.set()
        self._thread.join(timeout=self.interval + 1)
        last = self._read()
        if last:
            self.samples.append(last)
        return summarize_resources(self._first, self.samples, requests, self.ncpu)


def summarize_resources(first: dict, samples: list[dict], requests: int, ncpu: int) -> dict | None:
    if not first or not samples:
        return None
    cores, host_busy = [], []
    prev = first
    for s in samples:
        dt = s["t"] - prev["t"]
        if dt > 0:
            cores.append((s["cpu_s"] - prev["cpu_s"]) / dt)
        dtot = s["host_total"] - prev["host_total"]
        if dtot > 0:
            host_busy.append(1 - (s["host_idle"] - prev["host_idle"]) / dtot)
        prev = s
    last = samples[-1]
    cpu_s = last["cpu_s"] - first["cpu_s"]
    rss = [s["rss_mb"] for s in samples]
    return {
        "host_cpus": ncpu,
        "server_cpu_cores": dist(cores, ps=(50, 90)) if cores else None,
        "server_cpu_ms_per_request": round(cpu_s * 1000 / requests, 3) if requests else float("nan"),
        "host_cpu_busy": dist(host_busy, ps=(50, 90)) if host_busy else None,
        "server_rss_mb": {"start": round(first["rss_mb"], 1), "max": round(max(rss), 1),
                          "end": round(last["rss_mb"], 1)},
        "server_threads_max": max(s["threads"] for s in samples),
        "samples": len(samples),
    }
