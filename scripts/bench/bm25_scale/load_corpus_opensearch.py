#!/usr/bin/env python3
"""Bulk-load a generate_corpus.py NDJSON corpus into an OpenSearch index, for
a side-by-side comparison against load_corpus.py's Kosha load of the exact
same corpus.

Index settings mirror this repo's existing OpenSearch-vs-Kosha correctness
bench (scripts/bench/run_benchmark.py's LEXICAL_ANALYZER): BM25 with Kosha's
default k1=1.2/b=0.75 so scoring is comparable, whitespace tokenizer +
lowercase (no stemming) to match Kosha's own tokenization, 5 shards / 1
replica to match this domain's existing paragraph_index_hnsw convention.

Usage:
    python3 load_corpus_opensearch.py --host https://<domain-endpoint> \\
        --index bm25-bench-10m --corpus-dir /data/bm25-10m \\
        --batch-size 5000 --concurrency 8
"""

import argparse
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

INDEX_SETTINGS = {
    "settings": {
        "number_of_shards": 5,
        "number_of_replicas": 1,
        "analysis": {
            "filter": {
                "strip_edge_punct": {
                    "type": "pattern_replace",
                    "pattern": "^\\p{Punct}+|\\p{Punct}+$",
                    "replacement": "",
                },
                "drop_empty": {"type": "length", "min": 1},
            },
            "analyzer": {
                "bm25_match": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "filter": ["lowercase", "strip_edge_punct", "drop_empty"],
                }
            },
        },
        "similarity": {"default": {"type": "BM25", "k1": 1.2, "b": 0.75}},
    },
    "mappings": {"properties": {"text": {"type": "text", "analyzer": "bm25_match"}}},
}


def create_index(host: str, index: str) -> None:
    resp = requests.head(f"{host}/{index}", timeout=15)
    if resp.status_code == 200:
        print(f"index {index} already exists, reusing (not recreating)")
        return
    resp = requests.put(f"{host}/{index}", json=INDEX_SETTINGS, timeout=30)
    resp.raise_for_status()
    print(f"created index {index}: {resp.json()}")


def to_bulk_ndjson(lines: list[str], index: str) -> str:
    parts = []
    for line in lines:
        rec = json.loads(line)
        parts.append(json.dumps({"index": {"_index": index, "_id": rec["id"]}}))
        parts.append(json.dumps({"text": rec["text"]}))
    return "\n".join(parts) + "\n"


def post_bulk(session, host, index, ndjson_body, timeout) -> float:
    t0 = time.time()
    resp = session.post(
        f"{host}/_bulk",
        data=ndjson_body,
        headers={"content-type": "application/x-ndjson"},
        timeout=timeout,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("errors"):
        failed = [i for i in body["items"] if i["index"].get("status", 200) >= 300]
        raise RuntimeError(f"bulk had {len(failed)} failures, e.g. {failed[:2]}")
    return time.time() - t0


def iter_batches(corpus_dir: Path, batch_size: int):
    shard_paths = sorted(corpus_dir.glob("shard-*.ndjson"))
    if not shard_paths:
        raise SystemExit(f"no shard-*.ndjson files found under {corpus_dir}")
    buf: list[str] = []
    for shard_path in shard_paths:
        with shard_path.open() as f:
            for line in f:
                buf.append(line)
                if len(buf) >= batch_size:
                    yield buf
                    buf = []
    if buf:
        yield buf


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", required=True)
    ap.add_argument("--index", required=True)
    ap.add_argument("--corpus-dir", required=True, type=Path)
    ap.add_argument("--batch-size", type=int, default=5_000)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--timeout", type=float, default=120.0)
    ap.add_argument("--recreate", action="store_true", help="delete the index first if it exists")
    args = ap.parse_args()

    session = requests.Session()

    if args.recreate:
        requests.delete(f"{args.host}/{args.index}")  # ignore 404
    create_index(args.host, args.index)

    print(f"loading {args.corpus_dir} -> {args.host}/{args.index}")
    print(f"batch_size={args.batch_size} concurrency={args.concurrency}")

    t0 = time.time()
    docs_sent = 0
    batch_latencies: list[float] = []

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = {}
        for lines in iter_batches(args.corpus_dir, args.batch_size):
            body = to_bulk_ndjson(lines, args.index)
            fut = pool.submit(post_bulk, session, args.host, args.index, body, args.timeout)
            futures[fut] = len(lines)
            if len(futures) >= args.concurrency * 2:
                done_fut = next(iter(futures))
                n = futures.pop(done_fut)
                batch_latencies.append(done_fut.result())
                docs_sent += n
                _progress(docs_sent, t0)

        for fut in as_completed(futures):
            n = futures[fut]
            batch_latencies.append(fut.result())
            docs_sent += n
            _progress(docs_sent, t0)

    print("refreshing index (make docs searchable)...")
    session.post(f"{args.host}/{args.index}/_refresh").raise_for_status()

    print("forcemerge to a small, realistic segment count (comparable to Kosha's compact-after)...")
    tm0 = time.time()
    session.post(
        f"{args.host}/{args.index}/_forcemerge", params={"max_num_segments": 5}, timeout=3600
    ).raise_for_status()
    print(f"forcemerge done in {time.time() - tm0:.1f}s")

    elapsed = time.time() - t0
    print(f"\nloaded {docs_sent:,} docs in {elapsed:.1f}s ({docs_sent / elapsed:,.0f} docs/sec)")
    if batch_latencies:
        print(
            f"batch latency (n={len(batch_latencies)}): "
            f"p50={statistics.median(batch_latencies):.3f}s max={max(batch_latencies):.3f}s"
        )


def _progress(docs_sent: int, t0: float) -> None:
    if docs_sent % 200_000 < 5_000:
        elapsed = time.time() - t0
        rate = docs_sent / elapsed if elapsed > 0 else 0
        print(f"  {docs_sent:,} docs sent, {rate:,.0f} docs/sec")


if __name__ == "__main__":
    main()
