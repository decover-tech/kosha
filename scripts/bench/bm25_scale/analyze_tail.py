#!/usr/bin/env python3
"""Decompose a benchmark phase's latency tail from query_bench.py --raw-out.

Answers "where does p99 come from?" with four views:

1. Client vs server: client latency against the server's own total
   (`timings.total_ms`). The gap is time outside the engine's measured
   phases: HTTP accept/parse/serialize, the connection setup, and the
   load generator itself (`send_lag_ms`: thread start vs schedule).
2. Phase breakdown of the slowest requests: mean and share of queue,
   admit, hydrate, bm25_stats, score and materialize for the p99+ set,
   next to the same for the median band.
3. Query-intrinsic vs contention: if the slow requests are the same
   queries every time they repeat, the tail is query cost (long queries,
   common terms); if a query is slow on one repetition and fast on the
   others, it's interference (concurrency, GC-like pauses, I/O).
4. Time and load: tail rate per minute, and latency by the number of
   requests in flight when each one was sent.

Usage: analyze_tail.py <phase>.requests.jsonl.gz [--queries queries.txt] [--top 15]
"""
from __future__ import annotations

import argparse
import gzip
import json
import statistics
from collections import defaultdict
from pathlib import Path

PHASES = ["queue_ms", "admit_ms", "hydrate_ms", "bm25_stats_ms", "score_ms", "materialize_ms"]


def pct(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def load(path: Path) -> list[dict]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as f:
        return [json.loads(line) for line in f if line.strip()]


def fmt_row(label, vals):
    return f"  {label:<22}" + "".join(f"{v:>10.1f}" for v in vals)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("raw", type=Path)
    ap.add_argument("--queries", type=Path, default=None, help="queries.txt, to print the slow queries' text")
    ap.add_argument("--top", type=int, default=15)
    args = ap.parse_args()

    recs = load(args.raw)
    ok = [r for r in recs if "error" not in r]
    errs = len(recs) - len(ok)
    lat = [r["latency_ms"] for r in ok]
    p50, p90, p99 = pct(lat, 50), pct(lat, 90), pct(lat, 99)
    print(f"{args.raw.name}: {len(recs)} requests, {errs} errors")
    print(f"client latency p50/p90/p99/max: {p50:.1f} / {p90:.1f} / {p99:.1f} / {max(lat):.1f} ms\n")

    with_srv = [r for r in ok if "server" in r]
    cached = [r for r in with_srv if r["server"].get("cached")]
    uncached = [r for r in with_srv if not r["server"].get("cached")]
    if not with_srv:
        print("no server timings in this file (run with --server-timings and KOSHA_DASHBOARD=on)")
    else:
        print(f"server timings on {len(with_srv)} requests ({len(cached)} served from result cache)")

    # 1. client vs server
    print("\n1. Client vs server (ms)            p50       p90       p99       max")
    send_lag = [r["send_lag_ms"] for r in ok]
    print(fmt_row("client latency", [p50, p90, p99, max(lat)]))
    if with_srv:
        srv = [r["server"]["total_ms"] for r in with_srv]
        gap = [r["latency_ms"] - r["server"]["total_ms"] for r in with_srv]
        print(fmt_row("server total", [pct(srv, 50), pct(srv, 90), pct(srv, 99), max(srv)]))
        print(fmt_row("outside server phases", [pct(gap, 50), pct(gap, 90), pct(gap, 99), max(gap)]))
    print(fmt_row("client send lag", [pct(send_lag, 50), pct(send_lag, 90), pct(send_lag, 99), max(send_lag)]))

    # 2. phase breakdown, tail vs median band
    if uncached:
        thr = pct(lat, 99)
        tail = [r for r in uncached if r["latency_ms"] >= thr]
        mid = [r for r in uncached if pct(lat, 45) <= r["latency_ms"] <= pct(lat, 55)]
        print(f"\n2. Server phase means, uncached requests (ms)   median band (n={len(mid)})   p99+ (n={len(tail)})")
        for ph in PHASES + ["total_ms"]:
            m = statistics.fmean(r["server"].get(ph, 0.0) for r in mid) if mid else float("nan")
            t = statistics.fmean(r["server"].get(ph, 0.0) for r in tail) if tail else float("nan")
            t_share = (t / statistics.fmean(r["server"]["total_ms"] for r in tail) * 100) if tail else float("nan")
            print(f"  {ph:<16}{m:>14.1f}{t:>16.1f}   ({t_share:4.0f}% of tail server time)")
        outside = statistics.fmean(r["latency_ms"] - r["server"]["total_ms"] for r in tail) if tail else 0
        print(f"  {'outside server':<16}{'':>14}{outside:>16.1f}")

    # 3. query-intrinsic vs contention
    by_q = defaultdict(list)
    for r in ok:
        by_q[r["qidx"]].append(r["latency_ms"])
    thr = pct(lat, 99)
    tail_q = defaultdict(int)
    for r in ok:
        if r["latency_ms"] >= thr:
            tail_q[r["qidx"]] += 1
    reps = [len(v) for v in by_q.values()]
    print(f"\n3. Query-intrinsic vs contention ({len(by_q)} distinct queries, ~{statistics.fmean(reps):.1f} repeats each)")
    if tail_q:
        # For each slow request: is its query slow on its *other* repeats too?
        consistent = 0
        tail_n = 0
        for q, n in tail_q.items():
            others = sorted(by_q[q])
            med = statistics.median(others)
            tail_n += n
            if med >= pct(lat, 90):
                consistent += n
        print(f"  {consistent}/{tail_n} p99+ requests are queries whose MEDIAN latency is already >= p90")
        print("  (high share -> tail is query cost; low share -> interference/contention)")
        corr_terms = defaultdict(list)
        for r in ok:
            corr_terms[min(r["terms"], 12)].append(r["latency_ms"])
        print("  latency by query term count:   terms     n     p50     p99")
        for t in sorted(corr_terms):
            xs = corr_terms[t]
            print(f"  {'':<31}{t:>5}{len(xs):>6}{pct(xs, 50):>8.1f}{pct(xs, 99):>8.1f}")
        qtext = args.queries.read_text().splitlines() if args.queries else None
        print(f"  slowest queries by median (top {args.top}):")
        worst = sorted(by_q.items(), key=lambda kv: -statistics.median(kv[1]))[: args.top]
        for q, xs in worst:
            text = (qtext[q][:70] if qtext and q < len(qtext) else "")
            print(f"    q{q:<6} med {statistics.median(xs):7.1f}  min {min(xs):7.1f}  max {max(xs):7.1f}  {text}")

    # 4. time and load
    print("\n4. Tail over time (per minute: requests, p50, p99)")
    by_min = defaultdict(list)
    for r in ok:
        by_min[int(r["t_send_s"] // 60)].append(r["latency_ms"])
    for m in sorted(by_min):
        xs = by_min[m]
        print(f"  min {m:>3}: n={len(xs):>4}  p50={pct(xs, 50):7.1f}  p99={pct(xs, 99):8.1f}")
    # in-flight at send time
    events = sorted(ok, key=lambda r: r["t_send_s"])
    sends = [e["t_send_s"] for e in events]
    ends = sorted(r["t_send_s"] + r["latency_ms"] / 1000 for r in ok)
    import bisect

    inflight = defaultdict(list)
    for r in events:
        started = bisect.bisect_right(sends, r["t_send_s"]) - 1
        finished = bisect.bisect_right(ends, r["t_send_s"])
        inflight[min(max(started - finished, 0), 16)].append(r["latency_ms"])
    print("\n   latency by requests already in flight at send:  in-flight     n     p50     p99")
    for k in sorted(inflight):
        xs = inflight[k]
        print(f"  {'':<48}{k:>9}{len(xs):>6}{pct(xs, 50):>8.1f}{pct(xs, 99):>8.1f}")


if __name__ == "__main__":
    main()
