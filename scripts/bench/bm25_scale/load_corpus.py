#!/usr/bin/env python3
"""Bulk-load a generate_corpus.py NDJSON corpus into a Kosha namespace.

Deliberately does NOT use kosha_client.KoshaClient.bulk() — that helper calls
POST /flush after every batch (client.py), which forces one immutable segment
per batch regardless of server-side flush threshold. At helpers.bulk's usual
chunk size (500) that's ~20,000 segments for a 10M-doc corpus before any
compaction — the exact segment-sprawl pathology a bulk loader needs to avoid.

Instead this script POSTs directly to /index in large batches and never calls
/flush mid-load: auto-flush is left entirely to the server's
KOSHA_FLUSH_THRESHOLD (set that env var on the kosha-server deployment before
running this — see README.md in this directory). One explicit /flush happens
once at the very end to drain any partial buffer, followed by an optional
/v1/admin/compact-namespace call to merge whatever segment count resulted.

Usage:
    python3 load_corpus.py --host http://localhost:8080 \\
        --namespace bm25-bench-10m --corpus-dir /data/bm25-10m \\
        --api-key "$KOSHA_API_KEY" --batch-size 20000 --concurrency 4 \\
        --compact-after
"""

import argparse
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests


def auth_headers(api_key: str | None) -> dict:
    return {"Authorization": f"Bearer {api_key}"} if api_key else {}


def to_kosha_documents(lines: list[str]) -> list[dict]:
    docs = []
    for line in lines:
        rec = json.loads(line)
        docs.append(
            {
                "id": rec["id"],
                "fields": [{"name": "text", "field_type": "Text", "value": rec["text"]}],
            }
        )
    return docs


def post_batch(session, host, namespace, headers, docs, timeout) -> float:
    t0 = time.time()
    resp = session.post(
        f"{host}/index",
        json={"namespace": namespace, "documents": docs},
        headers=headers,
        timeout=timeout,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("indexed_count") != len(docs):
        raise RuntimeError(
            f"indexed_count mismatch: sent {len(docs)}, server reports {body.get('indexed_count')}"
        )
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
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--corpus-dir", required=True, type=Path)
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--batch-size", type=int, default=20_000)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--timeout", type=float, default=120.0)
    ap.add_argument(
        "--compact-after",
        action="store_true",
        help="run /v1/admin/compact-namespace after the final flush (see --compact-mode)",
    )
    ap.add_argument(
        "--compact-mode",
        choices=["tiered", "full"],
        default="tiered",
        help="compaction mode: tiered merges small segments per pass; "
        "full is an all-to-one merge (single pass)",
    )
    ap.add_argument(
        "--compact-passes",
        type=int,
        default=1,
        help="max tiered passes; loops until the segment count stops "
        "dropping or this many passes ran (ignored for --compact-mode full)",
    )
    args = ap.parse_args()

    headers = auth_headers(args.api_key)
    session = requests.Session()

    print(f"loading {args.corpus_dir} -> {args.host} namespace={args.namespace}")
    print(f"batch_size={args.batch_size} concurrency={args.concurrency}")

    t0 = time.time()
    docs_sent = 0
    batch_latencies: list[float] = []

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = {}
        for lines in iter_batches(args.corpus_dir, args.batch_size):
            docs = to_kosha_documents(lines)
            fut = pool.submit(
                post_batch, session, args.host, args.namespace, headers, docs, args.timeout
            )
            futures[fut] = len(docs)
            # Bound in-flight requests so we don't buffer the whole corpus
            # in memory as submitted-but-not-yet-awaited futures.
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

    # Drain any partial buffer below flush_threshold.
    print("final flush...")
    session.post(
        f"{args.host}/flush", json={"namespace": args.namespace}, headers=headers
    ).raise_for_status()

    if args.compact_after:
        docs_before = namespace_stats(session, args.host, headers, args.namespace)[
            "documents"
        ]
        passes = 1 if args.compact_mode == "full" else max(1, args.compact_passes)
        for pass_no in range(1, passes + 1):
            print(
                f"compaction pass {pass_no}/{passes} "
                f"(mode={args.compact_mode}, synchronous, may take a while)..."
            )
            tc0 = time.time()
            resp = session.post(
                f"{args.host}/v1/admin/compact-namespace",
                json={"namespace": args.namespace, "mode": args.compact_mode},
                headers=headers,
                timeout=7200,
            )
            resp.raise_for_status()
            body = resp.json()
            print(f"compaction: {body} ({time.time() - tc0:.1f}s)")
            if body.get("not_hydrated"):
                raise SystemExit(
                    f"compaction pass was partial: {len(body['not_hydrated'])} "
                    "segment(s) could not be hydrated — refusing to continue "
                    "(bench numbers over a partially-compacted namespace are "
                    "not comparable)"
                )
            if body.get("segments_after", 0) >= body.get("segments_before", 0):
                print("compaction converged (segment count stopped dropping)")
                break
        # Doc-loss guard (the tiered doc-loss class fixed in #113): compaction
        # rewrites every document, so any delta here is silent corruption the
        # latency numbers would happily hide.
        stats = namespace_stats(session, args.host, headers, args.namespace)
        if stats["documents"] != docs_before:
            raise SystemExit(
                f"DOC-COUNT MISMATCH after compaction: {docs_before:,} before, "
                f"{stats['documents']:,} after — do not trust this namespace"
            )
        print(
            f"post-compaction: {stats['segments']} segment(s), "
            f"{stats['documents']:,} docs verified unchanged"
        )

    elapsed = time.time() - t0
    print(
        f"\nloaded {docs_sent:,} docs in {elapsed:.1f}s "
        f"({docs_sent / elapsed:,.0f} docs/sec)"
    )
    if batch_latencies:
        print(
            f"batch latency (n={len(batch_latencies)}): "
            f"p50={statistics.median(batch_latencies):.3f}s "
            f"max={max(batch_latencies):.3f}s"
        )


def namespace_stats(session, host: str, headers: dict, namespace: str) -> dict:
    """Fetch this namespace's entry from GET /stats (documents, segments)."""
    resp = session.get(f"{host}/stats", headers=headers, timeout=60)
    resp.raise_for_status()
    for entry in resp.json().get("namespaces", []):
        if entry.get("namespace") == namespace:
            return entry
    raise SystemExit(f"namespace {namespace!r} missing from /stats response")


def _progress(docs_sent: int, t0: float) -> None:
    if docs_sent % 200_000 < 20_000:  # coarse, batch-size-independent-ish
        elapsed = time.time() - t0
        rate = docs_sent / elapsed if elapsed > 0 else 0
        print(f"  {docs_sent:,} docs sent, {rate:,.0f} docs/sec")


if __name__ == "__main__":
    main()
