#!/usr/bin/env python3
"""QPS-controlled BM25 query benchmark against a Kosha namespace, replicating
turbopuffer's tpuf-benchmark methodology: fixed 8 QPS, topk=10, p50/p90/p99
latency, reported separately for "cold" and "warm" namespace state.

This script does NOT itself manage the cold/warm transition — that requires
clearing Kosha's on-disk NVMe cache and restarting the process so its
in-memory parsed-segment cache is empty too (see reset_cache.sh in this
directory). The intended run sequence is:

    ./reset_cache.sh                         # clear on-disk + in-memory cache
    python3 query_bench.py ... --phase cold  # first touch: segments fetched
                                              #   from S3/disk, HNSW/postings
                                              #   parsed fresh
    python3 query_bench.py ... --phase warm  # same queries again: segments
                                              #   now resident in the
                                              #   in-process segment cache

Load generation is open-loop at a fixed target QPS (sleeps to the next
scheduled send time regardless of how long prior requests took), which is
what actually exercises tail latency under load — a closed-loop generator
(wait for each response before sending the next) would just measure
single-connection round-trip time and hide queueing effects.

Usage:
    python3 query_bench.py --host http://localhost:8080 \\
        --namespace bm25-bench-10m --queries-file /data/bm25-10m/queries.txt \\
        --api-key "$KOSHA_API_KEY" --qps 8 --topk 10 \\
        --duration 120 --phase cold --out cold_results.json
"""

import argparse
import json
import statistics
import threading
import time
from pathlib import Path

import requests


def auth_headers(api_key: str | None) -> dict:
    return {"Authorization": f"Bearer {api_key}"} if api_key else {}


def percentile(data: list[float], p: float) -> float:
    if not data:
        return float("nan")
    s = sorted(data)
    k = (len(s) - 1) * (p / 100)
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def run_query(session, host, namespace, headers, query_text, topk, timeout, results, lock, errors):
    t0 = time.time()
    try:
        resp = session.post(
            f"{host}/search",
            json={"namespace": namespace, "query_text": query_text, "max_results": topk},
            headers=headers,
            timeout=timeout,
        )
        resp.raise_for_status()
        resp.json()  # force full body read so latency includes deserialization
        latency = time.time() - t0
        with lock:
            results.append(latency)
    except Exception as e:  # noqa: BLE001 - benchmark tool, record and move on
        with lock:
            errors.append(str(e))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", required=True)
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--queries-file", required=True, type=Path)
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--qps", type=float, default=8.0)
    ap.add_argument("--topk", type=int, default=10)
    ap.add_argument("--duration", type=float, default=120.0, help="seconds")
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument(
        "--phase",
        choices=["cold", "warm"],
        required=True,
        help="label only — this script doesn't manage cache state itself",
    )
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    queries = [l.strip() for l in args.queries_file.read_text().splitlines() if l.strip()]
    if not queries:
        raise SystemExit(f"no queries in {args.queries_file}")

    headers = auth_headers(args.api_key)
    session = requests.Session()
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
            args=(
                session,
                args.host,
                args.namespace,
                headers,
                query_text,
                args.topk,
                args.timeout,
                results,
                lock,
                errors,
            ),
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
        "phase": args.phase,
        "namespace": args.namespace,
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
