#!/usr/bin/env python3
"""Fetch the Cohere MSMarco v2.1 corpus (text only) into load_corpus.py's
NDJSON shard format, plus the real MSMarco queries as queries.txt — the
exact dataset tpuf-benchmark's `CohereMSMarco` datasource uses, so a Kosha
run over this corpus is apples-to-apples with turbopuffer's published
"Full-Text Perf" chart (BM25, 10M docs, ~9GB, real natural-language
queries cycled in dataset order).

Downloads via HTTP range reads with column projection (pyarrow + fsspec):
only the id/text column chunks are fetched, never the 1024-dim embedding
columns that dominate the parquet files (~9GB transferred for 10M docs
instead of ~40GB+).

Usage (needs: pip install pyarrow huggingface_hub):
    python3 fetch_msmarco.py --out-dir /data/msmarco-10m --docs 10000000
"""

import argparse
import json
import subprocess
import time
from pathlib import Path

import pyarrow.parquet as pq
from huggingface_hub import HfFileSystem

# tpuf-benchmark points at the `Cohere/` org, which now redirects to
# `CohereLabs/` — same dataset, canonical location. Accessed through
# HfFileSystem (hf://) rather than raw HTTPS: HF serves files via signed
# CDN redirects that break plain fsspec-HTTP HEAD probes, while
# HfFileSystem handles redirects and range reads natively.
BASE = "datasets/CohereLabs/msmarco-v2.1-embed-english-v3"
PASSAGE_FILES = [
    f"{BASE}/passages_parquet/msmarco_v2.1_doc_segmented_{i:02d}.parquet"
    for i in range(60)
]
QUERIES_URL = f"{BASE}/queries_parquet/queries.parquet"
# Large read blocks: the default HfFileSystem block size makes the fetch
# range-request-latency-bound (tens of docs/sec); 32MB blocks make it
# throughput-bound.
BLOCK_SIZE = 32 * 1024 * 1024


def pick_columns(schema_names, id_col, text_col):
    """Resolve the id/text column names against the file's actual schema.
    tpuf-benchmark reads by positional index (text=1); we match by name
    with positional fallback so a schema tweak upstream fails loudly."""
    text = text_col if text_col in schema_names else None
    if text is None:
        for cand in ("text", "passage", "body", "segment"):
            if cand in schema_names:
                text = cand
                break
    if text is None and len(schema_names) > 1:
        text = schema_names[1]
    ident = id_col if id_col in schema_names else None
    if ident is None:
        for cand in ("id", "_id", "docid", "doc_id"):
            if cand in schema_names:
                ident = cand
                break
    if text is None:
        raise SystemExit(f"cannot find a text column in schema {schema_names}")
    return ident, text


def s3_sync(src: str, dst: str) -> None:
    """Mirror sealed artifacts between the out-dir and the S3 cache via the
    aws CLI (present on the bench AMI; instance-role auth — no extra deps).
    In-progress .tmp shards never enter the cache."""
    print(f"s3 sync: {src} -> {dst}")
    subprocess.run(
        ["aws", "s3", "sync", src, dst, "--exclude", "*.tmp", "--no-progress"],
        check=True,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--docs", type=int, default=10_000_000)
    ap.add_argument("--docs-per-shard", type=int, default=100_000)
    # Passages schema: docid/url/title/headings/segment/start_char/end_char/
    # emb — tpuf-benchmark indexes exactly the `segment` column (its parser
    # comment: 'parses the "segment" column (index 4)'), so we do too.
    ap.add_argument("--id-column", default="docid")
    ap.add_argument("--text-column", default="segment")
    ap.add_argument(
        "--skip-queries", action="store_true", help="corpus shards only"
    )
    ap.add_argument(
        "--s3-cache",
        default=None,
        help="s3://bucket/prefix holding previously-fetched shards; synced "
        "down before any HuggingFace traffic and re-uploaded after a fresh "
        "fetch, so the HF download happens once ever. Use one prefix per "
        "corpus size (e.g. .../msmarco-10m) — the cache is keyed by nothing "
        "else",
    )
    args = ap.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.s3_cache:
        s3_sync(args.s3_cache, str(args.out_dir))
        mpath = args.out_dir / "manifest.json"
        if mpath.exists():
            cached = json.loads(mpath.read_text())
            queries_ok = args.skip_queries or (
                (args.out_dir / "queries.txt").exists()
            )
            if cached.get("docs", 0) >= args.docs and queries_ok:
                print(
                    f"corpus cache hit: {cached['docs']:,} docs from "
                    f"{args.s3_cache}; skipping HuggingFace entirely"
                )
                return
            print("partial corpus in S3 cache; resuming from HuggingFace")

    fs = HfFileSystem()

    # ── queries.txt — the real MSMarco queries, dataset order ─────────────
    if not args.skip_queries:
        qpath = args.out_dir / "queries.txt"
        if qpath.exists() and qpath.stat().st_size > 0:
            print(f"queries.txt already present, keeping ({qpath})")
        else:
            print("fetching queries.parquet ...")
            with fs.open(QUERIES_URL, block_size=BLOCK_SIZE) as f:
                pf = pq.ParquetFile(f)
                names = pf.schema_arrow.names
                _, qtext = pick_columns(names, args.id_column, args.text_column)
                n = 0
                with qpath.open("w") as out:
                    for batch in pf.iter_batches(columns=[qtext]):
                        for v in batch.column(0).to_pylist():
                            q = " ".join(str(v).split())
                            if q:
                                out.write(q + "\n")
                                n += 1
                print(f"queries.txt: {n:,} queries (column {qtext!r})")

    # ── corpus shards — resumable per shard file ──────────────────────────
    docs_written = 0
    bytes_written = 0
    shard_idx = 0
    shard_docs = 0
    shard_f = None
    t0 = time.time()

    def open_shard(idx: int):
        return (args.out_dir / f"shard-{idx:05d}.ndjson.tmp").open("w")

    def seal_shard(idx: int):
        tmp = args.out_dir / f"shard-{idx:05d}.ndjson.tmp"
        tmp.rename(args.out_dir / f"shard-{idx:05d}.ndjson")

    # Resume: count docs already sealed (docs-per-shard per full shard).
    sealed = sorted(args.out_dir.glob("shard-*.ndjson"))
    if sealed:
        docs_written = len(sealed) * args.docs_per_shard
        shard_idx = len(sealed)
        print(f"resuming after {len(sealed)} sealed shards ({docs_written:,} docs)")
    if docs_written >= args.docs:
        print("corpus already complete")
    else:
        done = False
        for url in PASSAGE_FILES:
            if done:
                break
            name = url.rsplit("/", 1)[-1].split("?")[0]
            print(f"[{docs_written:,}/{args.docs:,}] reading {name} ...")
            with fs.open(url, block_size=BLOCK_SIZE) as f:
                pf = pq.ParquetFile(f)
                names = pf.schema_arrow.names
                ident, text = pick_columns(names, args.id_column, args.text_column)
                cols = [c for c in (ident, text) if c]
                for batch in pf.iter_batches(batch_size=32768, columns=cols):
                    d = batch.to_pydict()
                    texts = d[text]
                    ids = d[ident] if ident else [None] * len(texts)
                    for i, t in zip(ids, texts):
                        # tpuf-benchmark loads passages in file order until
                        # document_count is reached; skipping empties keeps
                        # ordering identical for every non-empty doc.
                        t = str(t or "").strip()
                        if not t:
                            continue
                        if shard_f is None:
                            shard_f = open_shard(shard_idx)
                            shard_docs = 0
                        doc_id = str(i) if i is not None else f"d{docs_written}"
                        line = json.dumps(
                            {"id": doc_id, "text": t}, ensure_ascii=False
                        )
                        shard_f.write(line + "\n")
                        bytes_written += len(t.encode())
                        docs_written += 1
                        shard_docs += 1
                        if shard_docs >= args.docs_per_shard:
                            shard_f.close()
                            seal_shard(shard_idx)
                            shard_idx += 1
                            shard_f = None
                            rate = docs_written / max(time.time() - t0, 1e-9)
                            print(
                                f"  shard {shard_idx} sealed — "
                                f"{docs_written:,} docs, "
                                f"{bytes_written/1e9:.2f}GB text, "
                                f"{rate:,.0f} docs/sec"
                            )
                        if docs_written >= args.docs:
                            done = True
                            break
                    if done:
                        break
        if shard_f is not None:
            shard_f.close()
            seal_shard(shard_idx)
            shard_idx += 1

    manifest = {
        "dataset": "Cohere/msmarco-v2.1-embed-english-v3 (text only)",
        "docs": docs_written,
        "bytes_text": bytes_written,
        "shards": shard_idx,
        "docs_per_shard": args.docs_per_shard,
        "queries": "queries.txt (real MSMarco queries, dataset order)",
    }
    (args.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(
        f"done: {docs_written:,} docs, {bytes_written/1e9:.2f}GB text, "
        f"{shard_idx} shards"
    )
    print(f"manifest: {args.out_dir / 'manifest.json'}")

    if args.s3_cache:
        s3_sync(str(args.out_dir), args.s3_cache)
        print(f"corpus uploaded to cache: {args.s3_cache}")


if __name__ == "__main__":
    main()
