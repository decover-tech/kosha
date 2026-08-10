#!/usr/bin/env python3
"""Fetch the Cohere MSMarco v2.1 embeddings (1024-dim float32) as binary
shards aligned 1:1 with fetch_msmarco.py's text shards, plus the query
embeddings — the vector half of the tpuf-benchmark workload ("Vector
Perf": same corpus, embed-english-v3 vectors, real queries).

Output layout (alongside the text shards' out-dir or its own):
    emb-00000.f32    docs-per-shard × 1024 float32 records, dataset order
    emb-00000.ids    one docid per line, same order — lets the loader
                     verify alignment against shard-00000.ndjson cheaply
    queries_emb.f32  one 1024-float32 record per queries.txt line
    emb_manifest.json

Doc order is identical to the text fetcher's by construction: both
iterate the same passage parquet files in order and take the first
--docs rows. The .ids sidecars exist so the loader can *prove* that
instead of trusting it.

JSON was rejected for this data: 10M × 1024 floats ≈ 150GB of text vs
41GB of raw float32.

Usage (needs: pip install pyarrow huggingface_hub numpy):
    python3 fetch_msmarco_embeddings.py --out-dir /data/corpus \\
        --docs 10000000 --s3-cache s3://bucket/msmarco-10m-emb
"""

import argparse
import json
import subprocess
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
from huggingface_hub import HfFileSystem

BASE = "datasets/CohereLabs/msmarco-v2.1-embed-english-v3"
PASSAGE_FILES = [
    f"{BASE}/passages_parquet/msmarco_v2.1_doc_segmented_{i:02d}.parquet"
    for i in range(60)
]
QUERIES_URL = f"{BASE}/queries_parquet/queries.parquet"
BLOCK_SIZE = 32 * 1024 * 1024
DIM = 1024


def pick_emb_column(names):
    for cand in ("emb", "embedding", "embeddings", "vector"):
        if cand in names:
            return cand
    raise SystemExit(f"no embedding column found in schema: {names}")


def s3_sync(src: str, dst: str) -> None:
    """Mirror ONLY embedding artifacts — the out-dir may be shared with the
    text corpus, which must not leak into the embeddings cache prefix."""
    print(f"s3 sync: {src} -> {dst}")
    subprocess.run(
        [
            "aws", "s3", "sync", src, dst, "--no-progress",
            "--exclude", "*",
            "--include", "emb-*.f32", "--include", "emb-*.ids",
            "--include", "queries_emb.f32", "--include", "emb_manifest.json",
        ],
        check=True,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--docs", type=int, default=10_000_000)
    ap.add_argument("--docs-per-shard", type=int, default=100_000)
    ap.add_argument("--id-column", default="docid")
    ap.add_argument("--skip-queries", action="store_true")
    ap.add_argument(
        "--s3-cache",
        default=None,
        help="s3://bucket/prefix for the embedding shards; synced down "
        "before any HuggingFace traffic, uploaded after a fresh fetch. "
        "Use a DIFFERENT prefix from the text cache (e.g. .../msmarco-10m-emb)",
    )
    args = ap.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.s3_cache:
        s3_sync(args.s3_cache, str(args.out_dir))
        mpath = args.out_dir / "emb_manifest.json"
        if mpath.exists():
            cached = json.loads(mpath.read_text())
            queries_ok = args.skip_queries or (
                (args.out_dir / "queries_emb.f32").exists()
            )
            if cached.get("docs", 0) >= args.docs and queries_ok:
                print(
                    f"embedding cache hit: {cached['docs']:,} docs from "
                    f"{args.s3_cache}; skipping HuggingFace entirely"
                )
                return
            print("partial embeddings in S3 cache; resuming from HuggingFace")

    fs = HfFileSystem()

    # ── query embeddings, one record per queries.txt line ────────────────
    if not args.skip_queries:
        qpath = args.out_dir / "queries_emb.f32"
        if qpath.exists() and qpath.stat().st_size > 0:
            print(f"queries_emb.f32 already present, keeping ({qpath})")
        else:
            print("fetching query embeddings ...")
            with fs.open(QUERIES_URL, block_size=BLOCK_SIZE) as f:
                pf = pq.ParquetFile(f)
                emb_col = pick_emb_column(pf.schema_arrow.names)
                n = 0
                with qpath.open("wb") as out:
                    for batch in pf.iter_batches(columns=[emb_col]):
                        for v in batch.column(0).to_pylist():
                            arr = np.asarray(v, dtype=np.float32)
                            if arr.shape != (DIM,):
                                raise SystemExit(
                                    f"query embedding {n} has shape {arr.shape}"
                                )
                            out.write(arr.tobytes())
                            n += 1
                print(f"queries_emb.f32: {n:,} embeddings")

    # ── doc embedding shards — resumable per shard file ──────────────────
    docs_written = 0
    shard_idx = 0
    shard_docs = 0
    shard_f = None
    ids_f = None
    t0 = time.time()

    def open_shard(idx: int):
        return (
            (args.out_dir / f"emb-{idx:05d}.f32.tmp").open("wb"),
            (args.out_dir / f"emb-{idx:05d}.ids.tmp").open("w"),
        )

    def seal_shard(idx: int):
        for ext in ("f32", "ids"):
            tmp = args.out_dir / f"emb-{idx:05d}.{ext}.tmp"
            tmp.rename(args.out_dir / f"emb-{idx:05d}.{ext}")

    sealed = sorted(args.out_dir.glob("emb-*.f32"))
    if sealed:
        docs_written = len(sealed) * args.docs_per_shard
        shard_idx = len(sealed)
        print(f"resuming after {len(sealed)} sealed shards ({docs_written:,} docs)")

    done = docs_written >= args.docs
    skip = docs_written
    for path in PASSAGE_FILES:
        if done:
            break
        with fs.open(path, block_size=BLOCK_SIZE) as f:
            pf = pq.ParquetFile(f)
            names = pf.schema_arrow.names
            emb_col = pick_emb_column(names)
            id_col = args.id_column if args.id_column in names else names[0]
            for batch in pf.iter_batches(columns=[id_col, emb_col]):
                nrows = batch.num_rows
                if skip >= nrows:
                    skip -= nrows
                    continue
                ids = batch.column(0).to_pylist()
                embs = batch.column(1).to_pylist()
                start = skip
                skip = 0
                for row in range(start, nrows):
                    if shard_f is None:
                        shard_f, ids_f = open_shard(shard_idx)
                        shard_docs = 0
                    arr = np.asarray(embs[row], dtype=np.float32)
                    if arr.shape != (DIM,):
                        raise SystemExit(
                            f"doc {ids[row]!r} embedding has shape {arr.shape}"
                        )
                    shard_f.write(arr.tobytes())
                    ids_f.write(str(ids[row]) + "\n")
                    docs_written += 1
                    shard_docs += 1
                    if shard_docs >= args.docs_per_shard:
                        shard_f.close()
                        ids_f.close()
                        seal_shard(shard_idx)
                        shard_idx += 1
                        shard_f = None
                        ids_f = None
                        rate = docs_written / max(time.time() - t0, 1e-9)
                        print(
                            f"  emb shard {shard_idx} sealed — "
                            f"{docs_written:,} docs, {rate:,.0f} docs/sec"
                        )
                    if docs_written >= args.docs:
                        done = True
                        break
                if done:
                    break
    if shard_f is not None:
        shard_f.close()
        ids_f.close()
        seal_shard(shard_idx)
        shard_idx += 1

    manifest = {
        "dataset": "Cohere/msmarco-v2.1-embed-english-v3 (embeddings)",
        "docs": docs_written,
        "dim": DIM,
        "shards": shard_idx,
        "docs_per_shard": args.docs_per_shard,
        "bytes_per_doc": DIM * 4,
    }
    (args.out_dir / "emb_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"done: {docs_written:,} embeddings, {shard_idx} shards")

    if args.s3_cache:
        s3_sync(str(args.out_dir), args.s3_cache)
        print(f"embeddings uploaded to cache: {args.s3_cache}")


if __name__ == "__main__":
    main()
