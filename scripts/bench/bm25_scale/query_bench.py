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


OPERATOR = None
NO_CACHE = False
# kNN mode (Vector Perf): when KNN_VECS is set, requests carry a knn clause
# with the query's embedding instead of BM25 query text.
KNN_VECS = None
KNN_FIELD = "text_emb"
KNN_NUM_CANDIDATES = 100


def payload_for(namespace: str, query_text: str, topk: int, qidx: int = 0) -> dict:
    if KNN_VECS is not None:
        payload = {
            "namespace": namespace,
            "query_text": "",
            "max_results": topk,
            "knn": {
                "field": KNN_FIELD,
                "vector": KNN_VECS[qidx % len(KNN_VECS)],
                "k": topk,
                "num_candidates": KNN_NUM_CANDIDATES,
            },
        }
    else:
        payload = {"namespace": namespace, "query_text": query_text, "max_results": topk}
        if OPERATOR:
            payload["operator"] = OPERATOR
    if NO_CACHE:
        payload["no_cache"] = True
    return payload


def run_query(
    session, host, namespace, headers, query_text, topk, timeout,
    results, lock, errors, hit_counts, degraded_counts, qidx=0,
):
    t0 = time.time()
    try:
        resp = session.post(
            f"{host}/search",
            json=payload_for(namespace, query_text, topk, qidx),
            headers=headers,
            timeout=timeout,
        )
        resp.raise_for_status()
        # A 200 with an empty/short results array, or a kNN response with
        # segments that silently dropped their vector candidates, looks
        # identical to a healthy one if you only check status + latency —
        # that's the gap this is closing. See the correctness-signal
        # discussion this script was extended for.
        body = resp.json()
        latency = time.time() - t0
        hits = len(body.get("results", []))
        # None for a non-kNN query (the field isn't sent at all); 0 means
        # "kNN query, every segment searched cleanly" — see
        # `SearchResult::knn_degraded_segments` on the server.
        degraded = body.get("knn_degraded_segments")
        with lock:
            results.append(latency)
            hit_counts.append(hits)
            degraded_counts.append(degraded if degraded is not None else 0)
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
    ap.add_argument(
        "--operator",
        choices=["and", "or"],
        default=None,
        help="query operator sent with each request (engine default when omitted)",
    )
    ap.add_argument(
        "--no-cache",
        action="store_true",
        help="send no_cache=true on every request (bypass the result cache)",
    )
    ap.add_argument(
        "--knn-embeddings",
        type=Path,
        default=None,
        help="queries_emb.f32 from fetch_msmarco_embeddings.py (1024-dim "
        "float32 records aligned with --queries-file). When set, requests "
        "run vector search (knn clause) instead of BM25",
    )
    ap.add_argument("--knn-field", default="text_emb")
    ap.add_argument(
        "--knn-num-candidates",
        type=int,
        default=100,
        help="ANN candidate pool per query (tpuf-comparable default)",
    )
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()
    global OPERATOR, NO_CACHE, KNN_VECS, KNN_FIELD, KNN_NUM_CANDIDATES
    OPERATOR = args.operator
    NO_CACHE = args.no_cache

    queries = [l.strip() for l in args.queries_file.read_text().splitlines() if l.strip()]
    if not queries:
        raise SystemExit(f"no queries in {args.queries_file}")

    if args.knn_embeddings:
        import array as _array

        DIM = 1024
        raw = args.knn_embeddings.read_bytes()
        if len(raw) % (DIM * 4) != 0:
            raise SystemExit(f"{args.knn_embeddings} is not a whole number of {DIM}-float records")
        vecs = []
        for off in range(0, len(raw), DIM * 4):
            a = _array.array("f")
            a.frombytes(raw[off : off + DIM * 4])
            vecs.append(list(a))
        if len(vecs) != len(queries):
            raise SystemExit(
                f"{len(vecs)} query embeddings vs {len(queries)} queries — "
                "misaligned inputs, refusing to run"
            )
        KNN_VECS = vecs
        KNN_FIELD = args.knn_field
        KNN_NUM_CANDIDATES = args.knn_num_candidates
        print(f"kNN mode: {len(vecs)} query vectors, field={KNN_FIELD!r}, num_candidates={KNN_NUM_CANDIDATES}")

    headers = auth_headers(args.api_key)
    session = requests.Session()
    results: list[float] = []
    errors: list[str] = []
    hit_counts: list[int] = []
    degraded_counts: list[int] = []
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
        qidx = i % len(queries)
        query_text = queries[qidx]
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
                hit_counts,
                degraded_counts,
                qidx,
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

    # Correctness signal, not just latency: a 200 with an empty/short page,
    # or a kNN response missing whatever a failed segment would have
    # contributed, previously looked identical to a healthy response here.
    # `hit_counts`/`degraded_counts` are index-aligned with `results` (both
    # only ever appended together, under the same lock, in `run_query`).
    n = len(hit_counts)
    zero_hit = sum(1 for h in hit_counts if h == 0)
    short_page = sum(1 for h in hit_counts if 0 < h < args.topk)
    knn_degraded_requests = sum(1 for d in degraded_counts if d > 0)
    knn_degraded_segments_total = sum(degraded_counts)

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
        # Zero rate on a healthy run is normal for BM25 (a narrow query can
        # genuinely match nothing); on kNN it should be ~0 — any nonzero
        # rate there means "successful" responses are hiding empty pages.
        "zero_hit_requests": zero_hit,
        "zero_hit_rate": (zero_hit / n) if n else float("nan"),
        "short_page_requests": short_page,
        "short_page_rate": (short_page / n) if n else float("nan"),
        # Always 0 for BM25 (the field is never sent on a non-kNN response —
        # see SearchResult::knn_degraded_segments). Nonzero on kNN means at
        # least one segment's vector index search failed and silently
        # contributed zero candidates for that request.
        "knn_degraded_requests": knn_degraded_requests,
        "knn_degraded_rate": (knn_degraded_requests / n) if n else float("nan"),
        "knn_degraded_segments_total": knn_degraded_segments_total,
    }

    print(json.dumps(summary, indent=2))
    if errors:
        print(f"\n{len(errors)} errors, first 5:")
        for e in errors[:5]:
            print(f"  {e}")
    if zero_hit:
        print(f"\n{zero_hit}/{n} requests returned zero hits ({summary['zero_hit_rate']:.1%})")
    if knn_degraded_requests:
        print(
            f"\n{knn_degraded_requests}/{n} requests had a degraded kNN segment "
            f"({knn_degraded_segments_total} segment-failures total) — results may be "
            "missing true nearest neighbors from the affected segment(s)"
        )

    if args.out:
        args.out.write_text(json.dumps(summary, indent=2))
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
