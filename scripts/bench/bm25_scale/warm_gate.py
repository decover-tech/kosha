#!/usr/bin/env python3
"""Warm gate: bring a namespace to turbopuffer's "hot" state with the
phase's own workload queries, then return. Warmup time is reported but is
NOT part of any measured phase.

turbopuffer's tpuf-benchmark (pkg/bench/bench.go) does this for hot runs:
it warms the namespace, then polls with the workload's own queries until
`performance.cache_hit_ratio >= 1.0` on 3 consecutive queries (backing off
on a miss), and only then starts the measured phase. Kosha's counterpart
of that ratio is `timings.fully_cached` on a search response (sent when the
request has `"timings": true` and the server runs with KOSHA_DASHBOARD=on):
true when the search made no S3 request of any kind and nothing it needed
was missing or shed.

Steps:
  0. probe: one query, to fail fast when the server does not report
     `timings.fully_cached` (server too old, or KOSHA_DASHBOARD off).
  1. hydrate: every query of the workload once, --concurrency at a time,
     so whatever the workload reads (scoring files, vector.idx, the ranged
     doc-store spans of its result pages, which the span cache persists)
     becomes local.
  2. poll: cycle through the queries until --consecutive responses in a
     row are fully cached; a miss (or an error) resets the streak and
     backs off.

Payloads are exactly the ones query_bench.py sends (same payload_for),
always with no_cache=true (so the engine path runs, not the whole-response
result cache, which would report fully_cached=true without reading
anything) and timings=true.

Greppable markers: WARM-GATE-READY in <s>s (...) / WARM-GATE-TIMEOUT /
WARM-GATE-UNSUPPORTED. Exit status: 0 ready, 1 timeout, 2 unsupported.

Usage:
    python3 warm_gate.py --host http://127.0.0.1:8080 --namespace msmarco-1m \\
        --queries-file /data/queries.txt --api-key sk-bench --operator or
    python3 warm_gate.py ... --knn-embeddings /data/queries_emb.f32
"""

import argparse
import concurrent.futures
import os
import sys
import threading
import time
from pathlib import Path

import requests

import query_bench as qb

EXIT_READY, EXIT_TIMEOUT, EXIT_UNSUPPORTED = 0, 1, 2

_local = threading.local()


def _session() -> requests.Session:
    s = getattr(_local, "session", None)
    if s is None:
        s = _local.session = requests.Session()
    return s


class Probe:
    """Outcome of one search: fully_cached is None when the field is absent."""

    __slots__ = ("ok", "fully_cached", "s3_bytes", "error")

    def __init__(self, ok, fully_cached=None, s3_bytes=0, error=None):
        self.ok = ok
        self.fully_cached = fully_cached
        self.s3_bytes = s3_bytes
        self.error = error


def search(host, namespace, headers, queries, topk, qidx, timeout) -> Probe:
    try:
        resp = _session().post(
            f"{host}/search",
            json=qb.payload_for(namespace, queries[qidx], topk, qidx),
            headers=headers,
            timeout=timeout,
        )
        resp.raise_for_status()
        timings = resp.json().get("timings") or {}
    except Exception as e:  # noqa: BLE001 - a failed warm query is a miss
        return Probe(False, error=str(e)[:200])
    fc = timings.get("fully_cached")
    return Probe(True, fully_cached=fc if isinstance(fc, bool) else None,
                 s3_bytes=int(timings.get("s3_bytes") or 0))


def gib(n: int) -> str:
    return f"{n / 2**30:.2f} GiB"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", required=True)
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--queries-file", required=True, type=Path)
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--topk", type=int, default=10)
    ap.add_argument("--operator", choices=["and", "or"], default=None)
    ap.add_argument("--knn-embeddings", type=Path, default=None,
                    help="as query_bench.py: run the kNN workload instead of BM25")
    ap.add_argument("--knn-field", default="text_emb")
    ap.add_argument("--knn-num-candidates", type=int, default=100)
    ap.add_argument("--concurrency", type=int, default=16,
                    help="in-flight queries during the hydrate pass")
    ap.add_argument("--consecutive", type=int, default=3,
                    help="fully cached responses in a row that declare the namespace warm")
    ap.add_argument("--timeout", type=float, default=10800.0,
                    help="overall budget in seconds for probe + hydrate + poll")
    ap.add_argument("--request-timeout", type=float, default=600.0)
    ap.add_argument("--backoff-initial", type=float, default=1.0)
    ap.add_argument("--backoff-max", type=float, default=30.0)
    ap.add_argument("--progress-every", type=float, default=60.0, help="seconds between progress lines")
    args = ap.parse_args(argv)

    qb.OPERATOR = args.operator
    qb.NO_CACHE = True
    qb.SERVER_TIMINGS = True
    queries = qb.load_queries(args.queries_file)
    if args.knn_embeddings:
        qb.KNN_VECS = qb.load_knn_vectors(args.knn_embeddings, len(queries))
        qb.KNN_FIELD = args.knn_field
        qb.KNN_NUM_CANDIDATES = args.knn_num_candidates
    workload = "knn" if args.knn_embeddings else f"bm25 operator={args.operator or 'default'}"
    headers = qb.auth_headers(args.api_key)
    n = len(queries)

    t0 = time.monotonic()
    deadline = t0 + args.timeout
    sent = 0
    s3_bytes = 0
    errors = 0

    def elapsed() -> float:
        return time.monotonic() - t0

    def remaining() -> float:
        return deadline - time.monotonic()

    def one(qidx: int) -> Probe:
        return search(args.host, args.namespace, headers, queries, args.topk, qidx,
                      max(1.0, min(args.request_timeout, remaining())))

    def timeout_exit(stage: str) -> int:
        print(f"WARM-GATE-TIMEOUT after {elapsed():.0f}s in {stage} "
              f"(queries sent: {sent}, errors: {errors}, s3 bytes observed: {s3_bytes} = {gib(s3_bytes)})",
              flush=True)
        return EXIT_TIMEOUT

    print(f"warm gate: namespace={args.namespace} workload={workload} queries={n} "
          f"concurrency={args.concurrency} consecutive={args.consecutive} timeout={args.timeout:.0f}s",
          flush=True)

    # ── 0. probe ────────────────────────────────────────────────────────
    probe = one(0)
    sent += 1
    if not probe.ok:
        errors += 1
        print(f"probe query failed: {probe.error}", flush=True)
    elif probe.fully_cached is None:
        print("WARM-GATE-UNSUPPORTED: search responses carry no timings.fully_cached "
              "(server older than the cache-hit gate, or KOSHA_DASHBOARD is off). "
              "Use warm_gate=preload, or deploy a server that reports it.", flush=True)
        return EXIT_UNSUPPORTED
    s3_bytes += probe.s3_bytes
    print(f"probe: ok={probe.ok} fully_cached={probe.fully_cached} s3_bytes={probe.s3_bytes} "
          f"({elapsed():.1f}s)", flush=True)

    # ── 1. hydrate: every workload query once ───────────────────────────
    done = cached = 0
    missing_field = 0
    last_report = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        it = iter(range(1, n))
        inflight = set()
        timed_out = False
        while True:
            while not timed_out and len(inflight) < args.concurrency:
                qidx = next(it, None)
                if qidx is None:
                    break
                inflight.add(pool.submit(one, qidx))
                sent += 1
            if not inflight:
                break
            finished, inflight = concurrent.futures.wait(
                inflight, timeout=max(0.1, min(5.0, remaining())),
                return_when=concurrent.futures.FIRST_COMPLETED)
            for f in finished:
                p = f.result()
                done += 1
                s3_bytes += p.s3_bytes
                if not p.ok:
                    errors += 1
                elif p.fully_cached is None:
                    missing_field += 1
                elif p.fully_cached:
                    cached += 1
            if missing_field:
                print("WARM-GATE-UNSUPPORTED: a hydrate response carried no timings.fully_cached",
                      flush=True)
                # Do not wait for the in-flight hydrations (up to
                # --request-timeout each): the answer is already known.
                sys.stdout.flush()
                os._exit(EXIT_UNSUPPORTED)
            if remaining() <= 0:
                timed_out = True  # stop submitting; drain what is in flight
            if time.monotonic() - last_report >= args.progress_every:
                print(f"WARM-GATE-HYDRATE {done + 1}/{n} done, {cached} already cached, "
                      f"{errors} errors, {gib(s3_bytes)} from S3, {elapsed():.0f}s", flush=True)
                last_report = time.monotonic()
    if timed_out:
        return timeout_exit("hydrate")
    print(f"hydrate pass: {n} queries in {elapsed():.0f}s, {cached + bool(probe.fully_cached)} already "
          f"fully cached, {errors} errors, {gib(s3_bytes)} from S3", flush=True)

    # ── 2. poll until K consecutive fully cached responses ──────────────
    streak = 0
    backoff = args.backoff_initial
    qidx = 0
    misses = 0
    while streak < args.consecutive:
        if remaining() <= 0:
            return timeout_exit("poll")
        p = one(qidx % n)
        qidx += 1
        sent += 1
        s3_bytes += p.s3_bytes
        if p.ok and p.fully_cached is None:
            print("WARM-GATE-UNSUPPORTED: a poll response carried no timings.fully_cached", flush=True)
            return EXIT_UNSUPPORTED
        if p.ok and p.fully_cached:
            streak += 1
            backoff = args.backoff_initial
            continue
        streak = 0
        misses += 1
        if not p.ok:
            errors += 1
        why = p.error if not p.ok else f"not fully cached ({p.s3_bytes} bytes from S3)"
        print(f"poll miss on query {(qidx - 1) % n}: {why}; backing off {backoff:.1f}s", flush=True)
        time.sleep(max(0.0, min(backoff, remaining())))
        backoff = min(backoff * 2, args.backoff_max)

    print(f"WARM-GATE-READY in {elapsed():.0f}s (queries sent: {sent}, poll misses: {misses}, "
          f"errors: {errors}, s3 bytes observed: {s3_bytes} = {gib(s3_bytes)})", flush=True)
    return EXIT_READY


if __name__ == "__main__":
    sys.exit(main())
