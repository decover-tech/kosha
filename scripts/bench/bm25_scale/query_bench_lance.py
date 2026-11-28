#!/usr/bin/env python3
"""Open-loop BM25 latency against the Lance table load_corpus_lance.py built.

The send loop, the percentile, and the query order are query_bench.py's,
the same ones query_bench_opensearch.py uses. The table is opened once per
process through the Iceberg REST catalog (``lance.dataset(namespace_client=,
table_id=)``); that one catalog lookup is reported as ``catalog_open_ms``
and is not part of any query latency. Each query is an OR ``MatchQuery`` on
the pre-trimmed text column, top 10, projecting ``id`` + ``_score``
(``ids``) or also the stored passage (``source``).

Latency is in-process time from just before the search call until the
Arrow result is in hand. There is no HTTP hop, unlike the Kosha and Lucene
clients, so this slightly favors Lance; it is recorded in ``protocol``.

Phases (vm/run_lance.sh):

* ``cold_first_pass`` — fresh process and empty Lance caches, no warmup,
  one pass over the query set (capped by --duration). Every index read is
  a first touch on S3, the analogue of Kosha's first-touch phase.
* ``warm_nocache`` — fresh process, ``prewarm_index`` plus one unmeasured
  pass, then the measured window. Lance has no result cache, so this is
  the same configuration as Kosha's result-cache-off phase.

Each phase records Lance's object-store IO stats (read requests and bytes)
for the measured window. A phase where every request returned zero hits
exits non-zero, so a broken index can not pass as a fast one.
"""

from __future__ import annotations

import argparse
import json
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import lance
import lance_namespace
import lance_namespace_impls  # noqa: F401  registers the "iceberg" impl
from lance.query import FullTextOperator, MatchQuery

import lance_compare
from query_bench import percentile, query_order


def open_table(root: str, region: str, index_cache: int, metadata_cache: int):
    session = lance.Session(
        index_cache_size_bytes=index_cache, metadata_cache_size_bytes=metadata_cache
    )
    t0 = time.time()
    ns = lance_namespace.connect("iceberg", lance_compare.namespace_properties(root))
    ds = lance.dataset(
        namespace_client=ns,
        table_id=lance_compare.TABLE_ID,
        storage_options={"region": region},
        session=session,
    )
    return ds, (time.time() - t0) * 1000


def one_search(ds, text: str, topk: int, columns: list[str]) -> tuple[float, int]:
    t0 = time.time()
    query = MatchQuery(
        lance_compare.normalize(text), lance_compare.INDEX_COLUMN, operator=FullTextOperator.OR
    )
    table = ds.to_table(full_text_query=query, columns=columns, limit=topk)
    return time.time() - t0, table.num_rows


def io_stats(ds) -> dict:
    try:
        s = ds.io_stats_incremental()
    except Exception as exc:  # noqa: BLE001  reporting only
        return {"error": str(exc)[:200]}
    out = {}
    for key in ("read_iops", "read_bytes", "write_iops", "written_bytes", "num_hops"):
        if hasattr(s, key):
            out[key] = getattr(s, key)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", required=True, help="same --root the loader used")
    ap.add_argument("--region", required=True)
    ap.add_argument("--queries-file", required=True, type=Path)
    ap.add_argument("--phase", required=True, choices=["cold_first_pass", "warm_nocache"])
    ap.add_argument("--qps", type=float, default=8.0)
    ap.add_argument("--topk", type=int, default=10)
    ap.add_argument("--duration", type=float, default=120.0)
    ap.add_argument("--timeout", type=float, default=600.0)
    ap.add_argument("--order", choices=["cycle", "shuffle", "zipf"], default="cycle")
    ap.add_argument("--order-seed", type=int, default=389)
    ap.add_argument("--return", dest="return_fields", choices=["ids", "source"], default="ids")
    ap.add_argument("--index-cache-bytes", type=int, default=lance_compare.DEFAULT_INDEX_CACHE_BYTES)
    ap.add_argument("--metadata-cache-bytes", type=int, default=lance_compare.DEFAULT_METADATA_CACHE_BYTES)
    ap.add_argument("--warmup-workers", type=int, default=16)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--raw-out", type=Path, default=None)
    args = ap.parse_args()

    queries = [line.strip() for line in args.queries_file.read_text().splitlines() if line.strip()]
    if not queries:
        raise SystemExit(f"no queries in {args.queries_file}")
    columns = lance_compare.projected_columns(args.return_fields)

    ds, catalog_open_ms = open_table(
        args.root, args.region, args.index_cache_bytes, args.metadata_cache_bytes
    )
    print(f"opened {ds.uri} via Iceberg REST catalog in {catalog_open_ms:.1f} ms")

    duration = args.duration
    warmup_s = None
    prewarm_s = None
    if args.phase == "cold_first_pass":
        duration = min(args.duration, len(queries) / args.qps)
    else:
        t0 = time.time()
        ds.prewarm_index(lance_compare.INDEX_COLUMN + "_idx")
        prewarm_s = time.time() - t0
        print(f"PREWARM-DONE in {prewarm_s:.1f}s")
        t0 = time.time()
        errors: list[str] = []

        def warm(text: str) -> None:
            try:
                one_search(ds, text, args.topk, columns)
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))

        with ThreadPoolExecutor(max_workers=args.warmup_workers) as pool:
            list(pool.map(warm, queries))
        warmup_s = time.time() - t0
        if errors:
            raise SystemExit(f"warmup had {len(errors)} errors, first: {errors[0]}")
        print(f"WARMUP-DONE in {warmup_s:.1f}s ({len(queries)} queries, not measured)")

    io_stats(ds)  # reset the incremental counters to the measured window
    results: list[float] = []
    errors = []
    hit_counts: list[int] = []
    raw: list[dict] | None = [] if args.raw_out else None
    lock = threading.Lock()
    threads: list[threading.Thread] = []
    interval = 1.0 / args.qps
    next_qidx = query_order(len(queries), args.order, args.order_seed, 1.0, 389)
    print(
        f"engine=lance phase={args.phase} qps={args.qps} topk={args.topk} "
        f"duration={duration:.0f}s return={args.return_fields} order={args.order}"
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
            latency, hits = one_search(ds, text, args.topk, columns)
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
    while time.time() - start < duration:
        now = time.time()
        if now < next_send:
            time.sleep(next_send - now)
        qidx = next_qidx(sent)
        sent += 1
        thread = threading.Thread(
            target=run_query, args=(queries[qidx], qidx, sent - 1, next_send, start), daemon=True
        )
        thread.start()
        threads.append(thread)
        next_send += interval

    print("draining in-flight requests...")
    for thread in threads:
        thread.join(timeout=args.timeout + 5)
    elapsed = time.time() - start
    window_io = io_stats(ds)

    n = len(hit_counts)
    zero_hit = sum(1 for hits in hit_counts if hits == 0)
    summary = {
        "engine": "lance",
        "pylance_version": lance.__version__,
        "catalog": "iceberg-rest",
        "catalog_image": lance_compare.CATALOG_IMAGE,
        "table_id": lance_compare.TABLE_ID,
        "location": ds.uri,
        "phase": args.phase,
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
        "catalog_open_ms": catalog_open_ms,
        "prewarm_s": prewarm_s,
        "warmup_s": warmup_s,
        "index_cache_bytes": args.index_cache_bytes,
        "metadata_cache_bytes": args.metadata_cache_bytes,
        "io_stats": window_io,
        "protocol": {
            "corpus": lance_compare.CORPUS_NAME,
            "operator": "or",
            "client": "query_bench.py open loop and percentile, in-process (no HTTP hop)",
            "analyzer": "Kosha standard@1 edge trim before indexing and on the query; "
            "whitespace tokenizer, lowercase, no stemming/stop words/folding",
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
    if n and zero_hit == n:
        raise SystemExit("every request returned zero hits: index or analyzer is broken")


if __name__ == "__main__":
    main()
