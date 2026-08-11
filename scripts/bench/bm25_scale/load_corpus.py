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
import array
import itertools
import json
import os
import statistics
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests


def auth_headers(api_key: str | None) -> dict:
    return {"Authorization": f"Bearer {api_key}"} if api_key else {}


EMB_DIM = 1024
EMB_REC_BYTES = EMB_DIM * 4


def to_kosha_documents(lines: list[str], embs: list | None = None) -> list[dict]:
    docs = []
    for i, line in enumerate(lines):
        rec = json.loads(line)
        fields = [{"name": "text", "field_type": "Text", "value": rec["text"]}]
        if embs is not None:
            emb_id, vec = embs[i]
            if emb_id != rec["id"]:
                raise SystemExit(
                    f"embedding/text shard misalignment: text doc {rec['id']!r} "
                    f"paired with embedding for {emb_id!r} — refusing to load"
                )
            # Field::vector wire format: the value is a JSON array string.
            fields.append(
                {
                    "name": "text_emb",
                    "field_type": "Vector",
                    "value": json.dumps(vec),
                }
            )
        docs.append({"id": rec["id"], "fields": fields})
    return docs


def iter_emb_records(emb_dir: str):
    """Yield (docid, [f32; 1024]) across emb-*.f32/.ids shard pairs, local
    dir or s3://bucket/prefix, in shard order."""
    if emb_dir.startswith("s3://"):
        base = emb_dir.rstrip("/")
        listing = subprocess.run(
            ["aws", "s3", "ls", base + "/"],
            check=True, capture_output=True, text=True,
        ).stdout
        names = sorted(
            f[-1] for line in listing.splitlines()
            if (f := line.split()) and f[-1].startswith("emb-") and f[-1].endswith(".f32")
        )
        if not names:
            raise SystemExit(f"no emb-*.f32 objects under {base}/")
        tmp_dir = bench_tmp_dir()
        for name in names:
            ids_name = name[: -len(".f32")] + ".ids"
            ids_tmp = tmp_dir / f"stage-{os.getpid()}-{ids_name}"
            f32_tmp = tmp_dir / f"stage-{os.getpid()}-{name}"
            s3_fetch_with_retries(f"{base}/{ids_name}", ids_tmp)
            s3_fetch_with_retries(f"{base}/{name}", f32_tmp)
            try:
                ids = ids_tmp.read_text().splitlines()
                expected = len(ids) * EMB_REC_BYTES
                actual = f32_tmp.stat().st_size
                if actual != expected:
                    raise SystemExit(
                        f"embedding shard {name}: {actual} bytes on disk, "
                        f"{expected} expected for {len(ids)} ids"
                    )
                with f32_tmp.open("rb") as f:
                    for docid in ids:
                        buf = f.read(EMB_REC_BYTES)
                        if len(buf) != EMB_REC_BYTES:
                            raise SystemExit(f"truncated embedding shard {name}")
                        vec = array.array("f")
                        vec.frombytes(buf)
                        yield docid, list(vec)
            finally:
                ids_tmp.unlink(missing_ok=True)
                f32_tmp.unlink(missing_ok=True)
    else:
        shard_paths = sorted(Path(emb_dir).glob("emb-*.f32"))
        if not shard_paths:
            raise SystemExit(f"no emb-*.f32 files found under {emb_dir}")
        for shard_path in shard_paths:
            ids = shard_path.with_suffix(".ids").read_text().splitlines()
            with shard_path.open("rb") as f:
                for docid in ids:
                    buf = f.read(EMB_REC_BYTES)
                    if len(buf) != EMB_REC_BYTES:
                        raise SystemExit(f"truncated embedding shard {shard_path}")
                    vec = array.array("f")
                    vec.frombytes(buf)
                    yield docid, list(vec)


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


def s3_fetch_with_retries(url: str, dest: Path) -> None:
    """Download one S3 object to a local file, retrying transient failures.

    Streaming objects through a live `aws s3 cp <key> -` pipe proved
    fragile: while the loader blocks on a flush-triggering /index request
    (minutes at large flush thresholds), the pipe backs up and the idle S3
    connection gets killed — surfacing as a truncated shard mid-load
    (round 9). A to-file download gets the CLI's own retry/resume
    machinery, and per-shard staging is only one shard of disk at a time.
    """
    for attempt in range(4):
        r = subprocess.run(
            ["aws", "s3", "cp", "--quiet", url, str(dest)],
            capture_output=True,
        )
        if r.returncode == 0:
            return
        time.sleep(2 ** attempt)
    raise SystemExit(f"aws s3 cp failed after 4 attempts: {url}")


def bench_tmp_dir() -> Path:
    """Shard staging dir — /data/tmp when the bench VM's big volume exists,
    else the system tempdir."""
    data_tmp = Path("/data/tmp")
    if data_tmp.parent.is_dir():
        data_tmp.mkdir(exist_ok=True)
        return data_tmp
    return Path(tempfile.gettempdir())


def iter_shard_lines(corpus_dir: str):
    """Yield NDJSON lines across all shards, local dir or s3://bucket/prefix.

    S3 shards are streamed through `aws s3 cp <key> -` (instance-role auth,
    no local staging) one shard at a time, so a 10M-doc corpus needs no
    corpus-sized disk on the bench VM at all.
    """
    if corpus_dir.startswith("s3://"):
        base = corpus_dir.rstrip("/")
        listing = subprocess.run(
            ["aws", "s3", "ls", base + "/"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        shards = []
        for line in listing.splitlines():
            fields = line.split()
            if fields and fields[-1].startswith("shard-") and fields[-1].endswith(".ndjson"):
                shards.append(fields[-1])
        shards.sort()
        if not shards:
            raise SystemExit(f"no shard-*.ndjson objects under {base}/")
        tmp_dir = bench_tmp_dir()
        for name in shards:
            tmp = tmp_dir / f"stage-{os.getpid()}-{name}"
            s3_fetch_with_retries(f"{base}/{name}", tmp)
            try:
                with tmp.open() as f:
                    yield from f
            finally:
                tmp.unlink(missing_ok=True)
    else:
        shard_paths = sorted(Path(corpus_dir).glob("shard-*.ndjson"))
        if not shard_paths:
            raise SystemExit(f"no shard-*.ndjson files found under {corpus_dir}")
        for shard_path in shard_paths:
            with shard_path.open() as f:
                yield from f


def iter_batches(corpus_dir: str, batch_size: int):
    buf: list[str] = []
    for line in iter_shard_lines(corpus_dir):
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
    # A local shard directory or s3://bucket/prefix (kept as str: Path()
    # would collapse the double slash in the URI).
    ap.add_argument("--corpus-dir", required=True)
    ap.add_argument("--api-key", default=None)
    ap.add_argument(
        "--embeddings-dir",
        default=None,
        help="emb-*.f32/.ids shards from fetch_msmarco_embeddings.py (local "
        "dir or s3://bucket/prefix); each doc gains a 1024-dim Vector field "
        "'text_emb'. Alignment with the text shards is verified per doc. "
        "Use a much smaller --batch-size (e.g. 1000): vectors are ~12KB of "
        "JSON per doc",
    )
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
        help="max compaction passes; loops until the segment count stops "
        "dropping or this many passes ran. Applies to both modes: full is "
        "size-capped server-side (5GiB default), so it too converges over "
        "passes rather than producing one unevictable monolith",
    )
    args = ap.parse_args()

    headers = auth_headers(args.api_key)
    session = requests.Session()

    print(f"loading {args.corpus_dir} -> {args.host} namespace={args.namespace}")
    print(f"batch_size={args.batch_size} concurrency={args.concurrency}")

    t0 = time.time()
    docs_sent = 0
    batch_latencies: list[float] = []

    emb_iter = iter_emb_records(args.embeddings_dir) if args.embeddings_dir else None

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = {}
        for lines in iter_batches(args.corpus_dir, args.batch_size):
            embs = list(itertools.islice(emb_iter, len(lines))) if emb_iter else None
            if embs is not None and len(embs) != len(lines):
                raise SystemExit(
                    f"embeddings exhausted: {len(embs)} records for a "
                    f"{len(lines)}-doc batch"
                )
            docs = to_kosha_documents(lines, embs)
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
        passes = max(1, args.compact_passes)
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
