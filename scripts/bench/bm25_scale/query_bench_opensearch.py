#!/usr/bin/env python3
"""Open-loop BM25 latency against the index load_corpus_opensearch.py built.

The send loop, the percentile, and the query order are query_bench.py's.
Latency is client time from just before the POST until the body has been
read and parsed, which is what query_bench.py records for Kosha. A 200
whose shards failed or that set ``timed_out`` counts as an error and is
left out of the percentile.

Two cache modes, both on a warm page cache:

* ``off`` — request cache and query cache disabled, and every search sends
  ``request_cache=false``. This is the configuration to set beside Kosha's
  result-cache-off phase.
* ``defaults`` — both caches set back to the engine default (enabled), and
  the search does not pass ``request_cache``. A default request cache does
  not store a ``size > 0`` hit list. The phase JSON includes the request
  cache and query cache hit/miss deltas so the run shows what engaged.

``--warmup`` sends every query once, 16 at a time, and does not record
those latencies. The workflow warms only the first phase. The second phase
starts right after it, the way Kosha's cache-on phase follows cache-off.

This script does not drop the OS page cache. Both phases are warm on local
disk. There is no cold-from-S3 number here; OpenSearch and Elasticsearch
are holding the index, not a cache of it.
"""

from __future__ import annotations

import argparse
import json
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

import lucene_compare
from query_bench import percentile, query_order


def cache_counter(session: requests.Session, host: str, index: str) -> dict:
    resp = session.get(f"{host}/{index}/_stats", timeout=60)
    resp.raise_for_status()
    prim = resp.json()["indices"][index]["primaries"]

    def count(group: str, field: str) -> int:
        return int(prim.get(group, {}).get(field, 0) or 0)

    return {
        "request_cache_hits": count("request_cache", "hit_count"),
        "request_cache_misses": count("request_cache", "miss_count"),
        "query_cache_hits": count("query_cache", "hit_count"),
        "query_cache_misses": count("query_cache", "miss_count"),
    }


def apply_cache_mode(session: requests.Session, host: str, index: str, mode: str) -> None:
    resp = session.put(
        f"{host}/{index}/_settings",
        json=lucene_compare.cache_index_settings(mode),
        timeout=60,
    )
    resp.raise_for_status()
    cleared = session.post(f"{host}/{index}/_cache/clear", timeout=60)
    cleared.raise_for_status()
    print(f"cache mode {mode}: {lucene_compare.cache_index_settings(mode)}")


def one_search(session, host, index, text, topk, timeout, mode, return_fields):
    """Return (latency_seconds, hits) or raise. Shared by warmup and the
    measured loop so both send the same body."""
    t0 = time.time()
    resp = session.post(
        f"{host}/{index}/_search",
        params=lucene_compare.search_params(mode),
        json=lucene_compare.search_body(text, topk, return_fields),
        timeout=timeout,
    )
    resp.raise_for_status()
    body = resp.json()
    latency = time.time() - t0
    shards = body.get("_shards") or {}
    failed = shards.get("failed") or 0
    if failed or body.get("timed_out"):
        raise RuntimeError(
            f"partial search failed_shards={failed} timed_out={body.get('timed_out')}"
        )
    hits = body.get("hits", {}).get("hits", [])
    return latency, len(hits)


def warmup(session, host, index, queries, topk, timeout, mode, return_fields, workers: int) -> float:
    started = time.time()
    errors: list[str] = []
    lock = threading.Lock()

    def run(text: str) -> None:
        try:
            one_search(session, host, index, text, topk, timeout, mode, return_fields)
        except Exception as exc:  # noqa: BLE001
            with lock:
                errors.append(str(exc))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(run, queries))
    elapsed = time.time() - started
    if errors:
        raise SystemExit(
            f"warmup had {len(errors)} errors, first: {errors[0]}"
        )
    print(f"WARMUP-DONE in {elapsed:.1f}s ({len(queries)} queries, not measured)")
    return elapsed


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", required=True)
    ap.add_argument("--engine", required=True, choices=sorted(lucene_compare.ENGINES))
    ap.add_argument("--index", default="msmarco-10m")
    ap.add_argument("--queries-file", required=True, type=Path)
    ap.add_argument("--cache-mode", required=True, choices=["off", "defaults"])
    ap.add_argument("--qps", type=float, default=8.0)
    ap.add_argument("--topk", type=int, default=10)
    ap.add_argument("--duration", type=float, default=120.0)
    ap.add_argument("--timeout", type=float, default=600.0)
    ap.add_argument("--phase", default="warm")
    ap.add_argument("--order", choices=["cycle", "shuffle", "zipf"], default="cycle")
    ap.add_argument("--order-seed", type=int, default=389)
    ap.add_argument("--return", dest="return_fields", choices=["ids", "source"], default="ids")
    ap.add_argument(
        "--warmup",
        action="store_true",
        help="send every query once before measuring; those latencies are not recorded",
    )
    ap.add_argument("--warmup-workers", type=int, default=16)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--raw-out", type=Path, default=None)
    args = ap.parse_args()

    queries = [line.strip() for line in args.queries_file.read_text().splitlines() if line.strip()]
    if not queries:
        raise SystemExit(f"no queries in {args.queries_file}")

    spec = lucene_compare.ENGINES[args.engine]
    session = requests.Session()
    apply_cache_mode(session, args.host, args.index, args.cache_mode)
    warmup_s = None
    if args.warmup:
        warmup_s = warmup(
            session, args.host, args.index, queries, args.topk, args.timeout,
            args.cache_mode, args.return_fields, args.warmup_workers,
        )

    before = cache_counter(session, args.host, args.index)
    results: list[float] = []
    errors: list[str] = []
    hit_counts: list[int] = []
    raw: list[dict] | None = [] if args.raw_out else None
    lock = threading.Lock()
    threads: list[threading.Thread] = []
    interval = 1.0 / args.qps
    next_qidx = query_order(len(queries), args.order, args.order_seed, 1.0, 389)
    print(
        f"engine={args.engine} cache_mode={args.cache_mode} phase={args.phase} "
        f"qps={args.qps} topk={args.topk} duration={args.duration}s "
        f"return={args.return_fields} order={args.order}"
    )

    def run_query(text: str, qidx: int, seq: int, scheduled: float, start: float) -> None:
        t0 = time.time()
        rec = None
        if raw is not None:
            rec = {
                "seq": seq,
                "qidx": qidx,
                "terms": len(text.split()),
                "t_send_s": round(t0 - start, 4),
                "send_lag_ms": round((t0 - scheduled) * 1000, 2),
            }
        try:
            latency, hits = one_search(
                session, args.host, args.index, text, args.topk, args.timeout,
                args.cache_mode, args.return_fields,
            )
            with lock:
                results.append(latency)
                hit_counts.append(hits)
                if rec is not None:
                    rec.update(latency_ms=round(latency * 1000, 3), hits=hits)
                    raw.append(rec)
        except Exception as exc:  # noqa: BLE001
            with lock:
                errors.append(str(exc))
                if rec is not None:
                    rec.update(latency_ms=round((time.time() - t0) * 1000, 3), error=str(exc)[:200])
                    raw.append(rec)

    start = time.time()
    next_send = start
    sent = 0
    while time.time() - start < args.duration:
        now = time.time()
        if now < next_send:
            time.sleep(next_send - now)
        qidx = next_qidx(sent)
        text = queries[qidx]
        sent += 1
        thread = threading.Thread(
            target=run_query,
            args=(text, qidx, sent - 1, next_send, start),
            daemon=True,
        )
        thread.start()
        threads.append(thread)
        next_send += interval

    print("draining in-flight requests...")
    for thread in threads:
        thread.join(timeout=args.timeout + 5)

    elapsed = time.time() - start
    after = cache_counter(session, args.host, args.index)
    deltas = {key: after[key] - before[key] for key in before}
    n = len(hit_counts)
    zero_hit = sum(1 for hits in hit_counts if hits == 0)
    summary = {
        "engine": args.engine,
        "image": spec["image"],
        "version_pinned": spec["version"],
        "phase": args.phase,
        "cache_mode": args.cache_mode,
        "index": args.index,
        "heap": lucene_compare.HEAP,
        "target_qps": args.qps,
        "achieved_qps": (len(results) / elapsed) if elapsed else 0,
        "topk": args.topk,
        "order": args.order,
        "return_fields": args.return_fields,
        "duration_s": elapsed,
        "requests": sent,
        "successes": len(results),
        "errors": len(errors),
        "p50_ms": percentile(results, 50) * 1000,
        "p90_ms": percentile(results, 90) * 1000,
        "p99_ms": percentile(results, 99) * 1000,
        "mean_ms": (statistics.fmean(results) * 1000) if results else float("nan"),
        "max_ms": (max(results) * 1000) if results else float("nan"),
        "zero_hit_requests": zero_hit,
        "zero_hit_rate": (zero_hit / n) if n else float("nan"),
        "warmup_s": warmup_s,
        "cache_deltas": deltas,
        "request_cache_query_param": lucene_compare.search_params(args.cache_mode).get("request_cache"),
        "protocol": {
            "corpus": lucene_compare.CORPUS_S3,
            "operator": "or",
            "client": "query_bench.py open loop and percentile",
            "track_total_hits": lucene_compare.TRACK_TOTAL_HITS,
        },
    }
    print(json.dumps(summary, indent=2))
    if errors:
        print(f"\n{len(errors)} errors, first 5:")
        for err in errors[:5]:
            print(f"  {err}")
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(summary, indent=2) + "\n")
        print(f"wrote {args.out}")
    if raw is not None:
        import gzip

        raw.sort(key=lambda rec: rec["seq"])
        opener = gzip.open if str(args.raw_out).endswith(".gz") else open
        args.raw_out.parent.mkdir(parents=True, exist_ok=True)
        with opener(args.raw_out, "wt") as handle:
            for rec in raw:
                handle.write(json.dumps(rec) + "\n")
        print(f"wrote {len(raw)} request records to {args.raw_out}")


if __name__ == "__main__":
    main()
