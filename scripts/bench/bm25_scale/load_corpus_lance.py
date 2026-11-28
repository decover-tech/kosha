#!/usr/bin/env python3
"""Load the MS MARCO shard directory into a Lance table on S3, cataloged
in an Iceberg REST catalog, and build its BM25 full-text index.

1. Create the namespace in the Iceberg REST catalog (lance-namespace's
   ``iceberg`` impl).
2. ``lance.write_dataset(..., namespace_client=, table_id=, mode="create")``:
   the impl declares the table in the catalog (``table_type=lance``) and
   returns its S3 location; Lance writes the data there.
3. Re-open the table through the catalog and build one INVERTED index on
   the pre-trimmed text column (see lance_compare.INDEX_COLUMN).

The caller (vm/run_lance.sh) empties the S3 prefix first, so the timings
are a full load and not an append.

Usage:
    python3 load_corpus_lance.py --corpus-dir /data/msmarco-10m \\
        --root s3://<bench-bucket>/lance --region us-east-1 \\
        --out /data/results-ci/lance/load.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path

import lance
import lance_namespace
import lance_namespace_impls  # noqa: F401  registers the "iceberg" impl
import pyarrow as pa

import lance_compare

SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field(lance_compare.TEXT_COLUMN, pa.large_string(), nullable=False),
        pa.field(lance_compare.INDEX_COLUMN, pa.large_string(), nullable=False),
    ]
)


def iter_record_batches(corpus_dir: Path, batch_size: int, counter: list[int]):
    shard_paths = sorted(corpus_dir.glob("shard-*.ndjson"))
    if not shard_paths:
        raise SystemExit(f"no shard-*.ndjson files found under {corpus_dir}")
    ids: list[str] = []
    texts: list[str] = []

    def flush():
        batch = pa.record_batch(
            [
                pa.array(ids, pa.string()),
                pa.array(texts, pa.large_string()),
                pa.array([lance_compare.normalize(t) for t in texts], pa.large_string()),
            ],
            schema=SCHEMA,
        )
        counter[0] += len(ids)
        ids.clear()
        texts.clear()
        return batch

    for shard_path in shard_paths:
        with shard_path.open() as handle:
            for line in handle:
                if not line.strip():
                    continue
                rec = json.loads(line)
                ids.append(str(rec["id"]))
                texts.append(rec["text"])
                if len(ids) >= batch_size:
                    yield flush()
        print(f"read {shard_path.name} (rows so far {counter[0] + len(ids):,})", flush=True)
    if ids:
        yield flush()


def s3_bytes(location: str) -> int | None:
    try:
        out = subprocess.check_output(
            ["aws", "s3", "ls", "--recursive", "--summarize", location.rstrip("/") + "/"],
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"WARN: could not size {location}: {exc}")
        return None
    for line in out.splitlines():
        if "Total Size:" in line:
            return int(line.split(":")[1].strip())
    return None


def connect(root: str):
    return lance_namespace.connect("iceberg", lance_compare.namespace_properties(root))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--corpus-dir", required=True, type=Path)
    ap.add_argument("--root", required=True, help="s3://bucket/prefix for the Lance table")
    ap.add_argument("--region", required=True)
    ap.add_argument("--batch-size", type=int, default=50_000)
    ap.add_argument("--limit-docs", type=int, default=0, help="smoke tests only; 0 = all")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    storage = {"region": args.region}
    ns = connect(args.root)
    ns_id = lance_compare.TABLE_ID[:-1]
    try:
        ns.create_namespace(lance_namespace.CreateNamespaceRequest(id=ns_id))
        print(f"created catalog namespace {'.'.join(ns_id)}")
    except Exception as exc:  # noqa: BLE001  already exists on a re-run
        print(f"catalog namespace {'.'.join(ns_id)}: {exc}")

    counter = [0]
    batches = iter_record_batches(args.corpus_dir, args.batch_size, counter)
    if args.limit_docs:
        def limited(src, n):
            seen = 0
            for b in src:
                if seen >= n:
                    break
                b = b.slice(0, n - seen)
                seen += b.num_rows
                yield b
        batches = limited(batches, args.limit_docs)
    reader = pa.RecordBatchReader.from_batches(SCHEMA, batches)

    t0 = time.time()
    lance.write_dataset(
        reader,
        namespace_client=ns,
        table_id=lance_compare.TABLE_ID,
        mode="create",
        storage_options=storage,
    )
    write_s = time.time() - t0
    rows_written = min(counter[0], args.limit_docs) if args.limit_docs else counter[0]
    print(f"WRITE-COMPLETE {rows_written:,} rows in {write_s:.1f}s", flush=True)

    # Re-open through the catalog, the way the query client will.
    ds = lance.dataset(
        namespace_client=connect(args.root),
        table_id=lance_compare.TABLE_ID,
        storage_options=storage,
    )
    location = ds.uri
    t1 = time.time()
    ds.create_scalar_index(lance_compare.INDEX_COLUMN, "INVERTED", **lance_compare.INDEX_PARAMS)
    index_s = time.time() - t1
    print(f"INDEX-COMPLETE in {index_s:.1f}s", flush=True)

    ds = lance.dataset(
        namespace_client=connect(args.root),
        table_id=lance_compare.TABLE_ID,
        storage_options=storage,
    )
    summary = {
        "engine": "lance",
        "pylance_version": lance.__version__,
        "catalog": "iceberg-rest",
        "catalog_image": lance_compare.CATALOG_IMAGE,
        "table_id": lance_compare.TABLE_ID,
        "location": location,
        "rows_written": rows_written,
        "rows_count": ds.count_rows(),
        "fragments": len(ds.get_fragments()),
        "dataset_version": ds.version,
        "write_s": round(write_s, 1),
        "index_s": round(index_s, 1),
        "index_column": lance_compare.INDEX_COLUMN,
        "index_params": lance_compare.INDEX_PARAMS,
        "indices": [
            {k: str(v) for k, v in idx.items() if k in ("name", "type", "fields")}
            for idx in ds.list_indices()
        ],
        "s3_bytes": s3_bytes(location),
    }
    print(json.dumps(summary, indent=2))
    if summary["rows_count"] != rows_written:
        raise SystemExit(f"row count mismatch: wrote {rows_written}, dataset has {summary['rows_count']}")
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(summary, indent=2) + "\n")
        print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
