#!/usr/bin/env python3
"""OpenSearch counterpart to query_bench.py — same QPS-controlled replay,
same queries.txt (from generate_corpus.py), same topk, for a side-by-side
number against Kosha.

Cold/warm caveat (read before trusting a "cold" OpenSearch number): unlike
Kosha's reset_cache.sh (which clears the on-disk cache AND restarts the
process, so a cold run genuinely re-fetches from durable storage), this
script's --phase cold only calls POST /<index>/_cache/clear, which drops
OpenSearch's request/query/fielddata caches. It does NOT evict the Lucene
segment files from the OS page cache or the JVM's own filesystem-level
segment buffers on a managed AWS OpenSearch domain — there is no node-shell
access to do that on a managed domain. So this "cold" number is a weaker
guarantee than Kosha's; treat it as "caches OpenSearch itself manages are
cleared," not "genuinely never-touched storage," and say so in the writeup.

Usage:
    python3 query_bench_opensearch.py --host https://<domain-endpoint> \\
        --index bm25-bench-10m --queries-file /data/bm25-10m/queries.txt \\
        --qps 8 --topk 10 --duration 120 --phase cold --out cold_os.json
"""

import argparse
import json
import statistics
import threading
import time
from pathlib import Path

import requests


def percentile(data: list[float], p: float) -> float:
    if not data:
        return float("nan")
    s = sorted(data)
    k = (len(s) - 1) * (p / 100)
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def run_query(session, host, index, query_text, topk, timeout, results, lock, errors):
    t0 = time.time()
    try:
        resp = session.post(
            f"{host}/{index}/_search",
            json={"query": {"match": {"text": query_text}}, "size": topk},
            timeout=timeout,
        )
        resp.raise_for_status()
        resp.json()
        latency = time.time() - t0
        with lock:
            results.append(latency)
    except Exception as e:  # noqa: BLE001 - benchmark tool, record and move on
        with lock:
            errors.append(str(e))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", required=True)
    ap.add_argument("--index", required=True)
    ap.add_argument("--queries-file", required=True, type=Path)
    ap.add_argument("--qps", type=float, default=8.0)
    ap.add_argument("--topk", type=int, default=10)
    ap.add_argument("--duration", type=float, default=120.0)
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--phase", choices=["cold", "warm"], required=True)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    queries = [l.strip() for l in args.queries_file.read_text().splitlines() if l.strip()]
    if not queries:
        raise SystemExit(f"no queries in {args.queries_file}")

    session = requests.Session()

    if args.phase == "cold":
        print("clearing OpenSearch request/query/fielddata caches (see script docstring "
              "for what this does and does not guarantee)...")
        session.post(f"{args.host}/{args.index}/_cache/clear", timeout=30).raise_for_status()

    results: list[float] = []
    errors: list[str] = []
    lock = threading.Lock()
    threads: list[threading.Thread] = []

    interval = 1.0 / args.qps
    print(
        f"phase={args.phase} qps={args.qps} topk={args.topk} "
        f"duration={args.duration}s (~{int(args.qps * args.duration)} requests)"
    )

    start = time.time()
    next_send = start
    i = 0
    while time.time() - start < args.duration:
        now = time.time()
        if now < next_send:
            time.sleep(next_send - now)
        query_text = queries[i % len(queries)]
        i += 1
        th = threading.Thread(
            target=run_query,
            args=(session, args.host, args.index, query_text, args.topk, args.timeout, results, lock, errors),
            daemon=True,
        )
        th.start()
        threads.append(th)
        next_send += interval

    print("draining in-flight requests...")
    for th in threads:
        th.join(timeout=args.timeout + 5)

    elapsed = time.time() - start
    achieved_qps = len(results) / elapsed if elapsed > 0 else 0

    summary = {
        "engine": "opensearch",
        "phase": args.phase,
        "index": args.index,
        "target_qps": args.qps,
        "achieved_qps": achieved_qps,
        "topk": args.topk,
        "duration_s": elapsed,
        "requests": i,
        "successes": len(results),
        "errors": len(errors),
        "p50_ms": percentile(results, 50) * 1000,
        "p90_ms": percentile(results, 90) * 1000,
        "p99_ms": percentile(results, 99) * 1000,
        "mean_ms": (statistics.fmean(results) * 1000) if results else float("nan"),
        "max_ms": (max(results) * 1000) if results else float("nan"),
    }

    print(json.dumps(summary, indent=2))
    if errors:
        print(f"\n{len(errors)} errors, first 5:")
        for e in errors[:5]:
            print(f"  {e}")
    if args.out:
        args.out.write_text(json.dumps(summary, indent=2))
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
