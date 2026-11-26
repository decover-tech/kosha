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

import bench_internals


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
SERVER_TIMINGS = False
# What each hit carries back. "ids" (default) asks for no stored fields
# (source_includes: []), so a hit is just doc_id + score: what
# turbopuffer's benchmark queries return (no include_attributes). "all"
# returns every stored field, including the 1024-dim embedding as JSON
# (~100-200KB per top-10 kNN response), which also overflows the 64MB
# result cache on the 1,677-query cycle.
RETURN_FIELDS = "ids"
# --raw-ids: also record each response's ranked doc_ids and scores in the
# --raw-out records (#385 ranking parity, topk_parity.py).
RAW_IDS = False


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
    if SERVER_TIMINGS:
        # Server phase breakdown in the response (needs KOSHA_DASHBOARD on
        # the server; silently absent otherwise).
        payload["timings"] = True
    if RETURN_FIELDS == "ids":
        payload["source_includes"] = []
    return payload


def read_stats(session, host: str, admin_key: str | None) -> dict | None:
    """GET /v1/stats (admin), for the counters diffed around a phase
    (scoring_blocks_decoded #385, posting and doc-store reads); None when
    not asked for or unreachable."""
    if not admin_key:
        return None
    try:
        resp = session.get(f"{host}/v1/stats", headers=auth_headers(admin_key), timeout=60)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:  # noqa: BLE001 - reporting only, never fails a run
        print(f"WARN: could not read /v1/stats: {e}")
        return None


def blocks_decoded(stats: dict | None) -> int | None:
    value = (stats or {}).get("scoring_blocks_decoded")
    return int(value) if value is not None else None


def load_queries(path: Path) -> list[str]:
    """Non-empty lines of a queries file, in order."""
    queries = [l.strip() for l in path.read_text().splitlines() if l.strip()]
    if not queries:
        raise SystemExit(f"no queries in {path}")
    return queries


def query_order(n: int, order: str, seed: int, zipf_s: float, rank_seed: int = 389):
    """qidx for the i-th request. cycle (default): i % n, the fixed loop
    every earlier run used. shuffle: a seeded random permutation, re-drawn
    each pass, so no query is a fixed distance from its repeat. zipf:
    i.i.d. draws with P(rank r) ~ 1/r^s over a random ranking of the
    queries, a skewed popularity mix (#389 cache-policy runs: a pure loop
    is adversarial to FIFO/LRU and flattering to SIEVE/S3-FIFO). The
    ranking comes from rank_seed, so the popular queries stay the same
    across stages run with different draw seeds."""
    import random as _random

    if order == "cycle":
        return lambda i: i % n
    rng = _random.Random(seed)
    if order == "shuffle":
        state = {"pass": -1, "perm": []}

        def shuffled(i: int) -> int:
            p = i // n
            if p != state["pass"]:
                state["pass"] = p
                state["perm"] = list(range(n))
                rng.shuffle(state["perm"])
            return state["perm"][i % n]

        return shuffled
    if order == "zipf":
        ranking = list(range(n))
        _random.Random(rank_seed).shuffle(ranking)
        weights = [1.0 / (r + 1) ** zipf_s for r in range(n)]
        cum, total = [], 0.0
        for w in weights:
            total += w
            cum.append(total)
        import bisect

        return lambda _i: ranking[min(bisect.bisect_left(cum, rng.random() * total), n - 1)]
    raise SystemExit(f"unknown --order {order}")


def load_knn_vectors(path: Path, n_queries: int, dim: int = 1024) -> list[list[float]]:
    """queries_emb.f32 (dim-float32 records aligned with the queries file)."""
    import array as _array

    raw = path.read_bytes()
    if len(raw) % (dim * 4) != 0:
        raise SystemExit(f"{path} is not a whole number of {dim}-float records")
    vecs = []
    for off in range(0, len(raw), dim * 4):
        a = _array.array("f")
        a.frombytes(raw[off : off + dim * 4])
        vecs.append(list(a))
    if len(vecs) != n_queries:
        raise SystemExit(
            f"{len(vecs)} query embeddings vs {n_queries} queries — "
            "misaligned inputs, refusing to run"
        )
    return vecs


def run_query(
    session, host, namespace, headers, query_text, topk, timeout,
    results, lock, errors, hit_counts, degraded_counts, qidx=0,
    raw=None, seq=0, scheduled=0.0, start=0.0, rows=None,
):
    t0 = time.time()
    rec = None
    if raw is not None:
        rec = {
            "seq": seq,
            "qidx": qidx,
            "terms": len(query_text.split()),
            "t_send_s": round(t0 - start, 4),
            # Client-side lateness: thread start vs the open-loop schedule.
            # Large values mean the load generator, not the server, added delay.
            "send_lag_ms": round((t0 - scheduled) * 1000, 2),
        }
    try:
        resp = session.post(
            f"{host}/v1/namespaces/{namespace}/search",
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
            if rows is not None:
                rows.append({
                    "latency_ms": latency * 1000,
                    "send_lag_ms": (t0 - scheduled) * 1000 if scheduled else None,
                    "hits": hits,
                    "server": body.get("timings"),
                })
            if rec is not None:
                rec.update(latency_ms=round(latency * 1000, 3), hits=hits)
                if RAW_IDS:
                    # Ranked ids and scores, so two runs over different
                    # snapshots can be checked for identical top-k per
                    # qidx (topk_parity.py, #385).
                    res = body.get("results", [])
                    rec["ids"] = [r.get("doc_id") for r in res]
                    rec["scores"] = [r.get("score") for r in res]
                if "timings" in body:
                    rec["server"] = body["timings"]
                raw.append(rec)
    except Exception as e:  # noqa: BLE001 - benchmark tool, record and move on
        with lock:
            errors.append(str(e))
            if rec is not None:
                rec.update(latency_ms=round((time.time() - t0) * 1000, 3), error=str(e)[:200])
                raw.append(rec)


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
        "--knn-dim",
        type=int,
        default=1024,
        help="dimension of the --knn-embeddings records (msmarco embeddings are 1024)",
    )
    ap.add_argument(
        "--knn-num-candidates",
        type=int,
        default=100,
        help="ANN candidate pool per query (tpuf-comparable default)",
    )
    ap.add_argument(
        "--order",
        choices=["cycle", "shuffle", "zipf"],
        default="cycle",
        help="request order over the queries file: cycle (default, the fixed loop), "
        "shuffle (seeded permutation per pass) or zipf (seeded skewed popularity)",
    )
    ap.add_argument(
        "--order-seed",
        type=int,
        default=389,
        help="seed for the shuffle / zipf draws; use a different one per stage",
    )
    ap.add_argument(
        "--zipf-rank-seed",
        type=int,
        default=389,
        help="seed for which queries are popular under --order zipf (keep fixed across stages)",
    )
    ap.add_argument("--zipf-s", type=float, default=1.0, help="zipf exponent for --order zipf")
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument(
        "--raw-out",
        type=Path,
        default=None,
        help="write one JSON line per request (qidx, terms, send lag, latency, "
        "hits, server timings) for tail analysis; gzipped if the name ends .gz",
    )
    ap.add_argument(
        "--server-timings",
        action="store_true",
        help="send timings=true so each response carries the server phase "
        "breakdown (requires KOSHA_DASHBOARD on the server)",
    )
    ap.add_argument(
        "--return",
        dest="return_fields",
        choices=["ids", "all"],
        default="ids",
        help="ids (default): hits carry doc_id + score only (source_includes: []), "
        "as turbopuffer's benchmark queries do; all: every stored field, "
        "including embeddings",
    )
    ap.add_argument(
        "--raw-ids",
        action="store_true",
        help="with --raw-out, also record each response's ranked doc_ids and "
        "scores (ids, scores), for an off-vs-on ranking parity check with "
        "topk_parity.py (#385)",
    )
    ap.add_argument(
        "--stats-api-key",
        default=None,
        help="admin key for GET /v1/stats: if set, read the server's "
        "scoring_blocks_decoded counter before and after the phase and report "
        "the delta and blocks decoded per successful request (#385). The "
        "counter is process wide, so it is only per-query when nothing else "
        "queries the server during the phase",
    )
    ap.add_argument(
        "--no-internals",
        action="store_true",
        help="leave the engine-internals block out of the summary (stage "
        "breakdown from --server-timings, /v1/stats counter deltas from "
        "--stats-api-key, server CPU/RSS sampled from /proc)",
    )
    ap.add_argument(
        "--server-process",
        default="kosha-server",
        help="process name whose CPU and RSS are sampled during the phase "
        "(Linux /proc; containers share the host's). Ignored when not found",
    )
    ap.add_argument("--server-pid", type=int, default=None, help="sample this pid instead")
    args = ap.parse_args()
    global OPERATOR, NO_CACHE, KNN_VECS, KNN_FIELD, KNN_NUM_CANDIDATES, SERVER_TIMINGS, RETURN_FIELDS, RAW_IDS
    OPERATOR = args.operator
    NO_CACHE = args.no_cache
    SERVER_TIMINGS = args.server_timings
    RETURN_FIELDS = args.return_fields
    RAW_IDS = args.raw_ids

    queries = load_queries(args.queries_file)

    if args.knn_embeddings:
        vecs = load_knn_vectors(args.knn_embeddings, len(queries), args.knn_dim)
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
    raw: list[dict] | None = [] if args.raw_out else None
    rows: list[dict] | None = None if args.no_internals else []
    lock = threading.Lock()
    threads: list[threading.Thread] = []

    interval = 1.0 / args.qps
    next_qidx = query_order(
        len(queries), args.order, args.order_seed, args.zipf_s, args.zipf_rank_seed
    )
    print(
        f"phase={args.phase} qps={args.qps} topk={args.topk} "
        f"duration={args.duration}s (~{int(args.qps * args.duration)} requests) order={args.order}"
    )

    stats_before = read_stats(session, args.host, args.stats_api_key)
    blocks_before = blocks_decoded(stats_before)
    sampler = None
    if not args.no_internals:
        sampler = bench_internals.ResourceSampler(args.server_process, args.server_pid).start()
    start = time.time()
    next_send = start
    i = 0
    while time.time() - start < args.duration:
        now = time.time()
        if now < next_send:
            time.sleep(next_send - now)
        qidx = next_qidx(i)
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
                raw,
                i - 1,
                next_send,
                start,
                rows,
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
    resources = sampler.stop(len(results)) if sampler is not None else None
    stats_after = read_stats(session, args.host, args.stats_api_key)
    blocks_after = blocks_decoded(stats_after)
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
        "order": args.order,
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
    if blocks_before is not None and blocks_after is not None:
        delta = blocks_after - blocks_before
        summary["scoring_blocks_decoded"] = delta
        summary["scoring_blocks_decoded_per_request"] = (delta / len(results)) if results else float("nan")
    if rows is not None:
        # The engine taken apart: where each request's time went, the work
        # it did, and what the server used (bench_internals.py).
        internals = bench_internals.stage_breakdown(rows) or {}
        counters = bench_internals.counter_deltas(
            stats_before, stats_after, len(results), sum(hit_counts)
        )
        if counters:
            internals["counters"] = counters
        if resources:
            internals["resources"] = resources
        if internals:
            summary["internals"] = internals

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
    if raw is not None:
        import gzip

        raw.sort(key=lambda r: r["seq"])
        opener = gzip.open if str(args.raw_out).endswith(".gz") else open
        with opener(args.raw_out, "wt") as f:
            for r in raw:
                f.write(json.dumps(r) + "\n")
        print(f"wrote {len(raw)} request records to {args.raw_out}")


if __name__ == "__main__":
    main()
