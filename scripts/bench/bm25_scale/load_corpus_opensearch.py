#!/usr/bin/env python3
"""Bulk-load the MS MARCO shard directory into a single-node OpenSearch or
Elasticsearch index.

The index is the one ``lucene_compare.index_body`` builds: BM25 k1=1.2
b=0.75, the Kosha ``standard`` analyzer (whitespace, ASCII edge
punctuation, lowercase), replicas 0, refresh_interval -1. After the bulk
load finishes, one ``_refresh`` makes the docs searchable and
``_forcemerge?max_num_segments=N`` runs before any query. Translog
durability and the codec stay at the engine defaults, so the ingest time
includes the default fsync.

The August synthetic-corpus loader (5 shards, 1 replica) is this file's
previous shape. A replica cannot be assigned on one node; this script
refuses replicas other than 0.

Usage:
    python3 load_corpus_opensearch.py --host http://127.0.0.1:9200 \\
        --engine opensearch --index msmarco-10m \\
        --corpus-dir /data/msmarco-10m --recreate \\
        --out /data/results-ci/opensearch/load.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

import lucene_compare


def create_index(session: requests.Session, host: str, index: str, shards: int, recreate: bool) -> None:
    if recreate:
        resp = session.delete(f"{host}/{index}", timeout=60)
        if resp.status_code not in (200, 404):
            resp.raise_for_status()
    else:
        existing = session.head(f"{host}/{index}", timeout=30)
        if existing.status_code == 200:
            raise SystemExit(
                f"index {index} already exists; pass --recreate so ingest time "
                "is a full load and not an append"
            )
    resp = session.put(
        f"{host}/{index}",
        json=lucene_compare.index_body(shards),
        timeout=60,
    )
    resp.raise_for_status()
    print(f"created index {index}")


def post_bulk(host: str, body: str, timeout: float) -> None:
    # A session per call: requests.Session is not a thread-safe connection pool.
    with requests.Session() as session:
        resp = session.post(
            f"{host}/_bulk",
            data=body.encode(),
            headers={"content-type": "application/x-ndjson"},
            timeout=timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
    if payload.get("errors"):
        failed = [item for item in payload["items"] if item["index"].get("status", 200) >= 300]
        raise RuntimeError(f"bulk had {len(failed)} failures, e.g. {failed[:2]}")


def iter_batches(corpus_dir: Path, batch_size: int):
    shard_paths = sorted(corpus_dir.glob("shard-*.ndjson"))
    if not shard_paths:
        raise SystemExit(f"no shard-*.ndjson files found under {corpus_dir}")
    buf: list[str] = []
    for shard_path in shard_paths:
        with shard_path.open() as handle:
            for line in handle:
                if not line.strip():
                    continue
                buf.append(line)
                if len(buf) >= batch_size:
                    yield buf
                    buf = []
    if buf:
        yield buf


def du_bytes(path: Path) -> int | None:
    try:
        out = subprocess.check_output(["sudo", "du", "-sb", str(path)], text=True)
        return int(out.split()[0])
    except (OSError, subprocess.CalledProcessError, IndexError, ValueError) as exc:
        print(f"WARN: could not du {path}: {exc}")
        return None


def cluster_facts(session: requests.Session, host: str, index: str) -> dict:
    root = session.get(host, timeout=30)
    root.raise_for_status()
    version = root.json().get("version", {})
    health = session.get(f"{host}/_cluster/health", timeout=30)
    health.raise_for_status()
    health_body = health.json()
    nodes = session.get(f"{host}/_nodes", timeout=30)
    nodes.raise_for_status()
    node_ids = list(nodes.json().get("nodes", {}))
    stats = session.get(f"{host}/{index}/_stats", timeout=120)
    stats.raise_for_status()
    prim = stats.json()["indices"][index]["primaries"]
    return {
        "node_version": version.get("number"),
        "node_build": version.get("build_flavor") or version.get("distribution"),
        "lucene_version": version.get("lucene_version"),
        "node_count": health_body.get("number_of_nodes"),
        "node_ids": len(node_ids),
        "cluster_status": health_body.get("status"),
        "docs_count": prim.get("docs", {}).get("count"),
        "store_size_bytes": prim.get("store", {}).get("size_in_bytes"),
        "segments_count": prim.get("segments", {}).get("count"),
    }


def apply_concurrent_search(session: requests.Session, host: str, engine: str) -> dict:
    body = lucene_compare.ENGINES[engine]["concurrent_segment_search"]
    resp = session.put(f"{host}/_cluster/settings", json=body, timeout=30)
    recorded = {"request": body, "status": resp.status_code}
    try:
        recorded["body"] = resp.json()
    except ValueError:
        recorded["body"] = resp.text[:500]
    if resp.status_code >= 300:
        # One segment per shard has nothing for this setting to slice.
        # Shard fan-out is the parallelism either way. Keep the refusal in
        # the report and go on, so an engine that renamed the knob still
        # produces a latency number with the failure written down.
        print(f"WARN: concurrent segment search setting refused: {recorded['body']}")
    else:
        print(f"concurrent segment search: {body}")
    return recorded


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", required=True)
    ap.add_argument("--engine", required=True, choices=sorted(lucene_compare.ENGINES))
    ap.add_argument("--index", default="msmarco-10m")
    ap.add_argument("--corpus-dir", required=True, type=Path)
    ap.add_argument("--data-dir", type=Path, default=None, help="container data dir, for du -sb")
    ap.add_argument("--shards", type=int, default=lucene_compare.DEFAULT_SHARDS)
    ap.add_argument("--batch-size", type=int, default=5_000)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--timeout", type=float, default=120.0)
    ap.add_argument(
        "--max-num-segments",
        type=int,
        default=lucene_compare.DEFAULT_MAX_SEGMENTS,
        help="force-merge target per shard (1 = one segment on each shard)",
    )
    ap.add_argument("--forcemerge-timeout", type=float, default=7200.0)
    ap.add_argument("--recreate", action="store_true")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    spec = lucene_compare.ENGINES[args.engine]
    session = requests.Session()
    create_index(session, args.host, args.index, args.shards, args.recreate)
    concurrent = apply_concurrent_search(session, args.host, args.engine)

    print(
        f"loading {args.corpus_dir} -> {args.host}/{args.index} "
        f"engine={args.engine} shards={args.shards} "
        f"batch_size={args.batch_size} concurrency={args.concurrency}"
    )
    bulk_started = time.time()
    docs_sent = 0
    lock = threading.Lock()
    next_report = 200_000

    def consume(body: str, n: int) -> None:
        nonlocal docs_sent, next_report
        post_bulk(args.host, body, args.timeout)
        with lock:
            docs_sent += n
            if docs_sent >= next_report:
                elapsed = time.time() - bulk_started
                rate = docs_sent / elapsed if elapsed else 0
                print(f"  {docs_sent:,} docs sent, {rate:,.0f} docs/sec")
                next_report += 200_000

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = []
        for lines in iter_batches(args.corpus_dir, args.batch_size):
            body, n = lucene_compare.to_bulk_ndjson(lines, args.index)
            futures.append(pool.submit(consume, body, n))
            if len(futures) >= args.concurrency * 4:
                futures[0].result()
                futures = futures[1:]
        for fut in futures:
            fut.result()
    bulk_s = time.time() - bulk_started
    print(f"bulk done: {docs_sent:,} docs in {bulk_s:.1f}s")

    print("refresh (one explicit refresh; refresh_interval stays -1)...")
    t0 = time.time()
    session.post(f"{args.host}/{args.index}/_refresh", timeout=3600).raise_for_status()
    refresh_s = time.time() - t0
    print(f"refresh done in {refresh_s:.1f}s")

    print(f"forcemerge max_num_segments={args.max_num_segments} per shard...")
    t0 = time.time()
    session.post(
        f"{args.host}/{args.index}/_forcemerge",
        params={"max_num_segments": args.max_num_segments},
        timeout=args.forcemerge_timeout,
    ).raise_for_status()
    forcemerge_s = time.time() - t0
    print(f"forcemerge done in {forcemerge_s:.1f}s")

    facts = cluster_facts(session, args.host, args.index)
    if facts["node_count"] != 1:
        raise SystemExit(f"expected a single node, health says {facts['node_count']}")
    report = {
        "engine": args.engine,
        "image": spec["image"],
        "version_pinned": spec["version"],
        "released": spec["released"],
        "license": spec["license"],
        "heap": lucene_compare.HEAP,
        "index": args.index,
        "shards": args.shards,
        "replicas": 0,
        "max_num_segments": args.max_num_segments,
        "refresh_interval": "-1",
        "translog_durability": "engine default (request); not overridden",
        "codec": "engine default; not overridden",
        "analyzer": lucene_compare.ANALYZER_NAME,
        "bm25": {"k1": lucene_compare.BM25_K1, "b": lucene_compare.BM25_B},
        "batch_size": args.batch_size,
        "concurrency": args.concurrency,
        "docs_sent": docs_sent,
        "bulk_s": round(bulk_s, 3),
        "refresh_s": round(refresh_s, 3),
        "forcemerge_s": round(forcemerge_s, 3),
        "docs_per_sec_bulk": (docs_sent / bulk_s) if bulk_s else None,
        "data_dir_bytes": du_bytes(args.data_dir) if args.data_dir else None,
        "concurrent_segment_search": concurrent,
        "cost_assumptions": lucene_compare.COST_ASSUMPTIONS,
        **facts,
    }
    print(json.dumps(report, indent=2))
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2) + "\n")
        print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
