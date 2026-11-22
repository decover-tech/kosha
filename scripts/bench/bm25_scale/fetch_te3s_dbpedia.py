#!/usr/bin/env python3
"""Fetch the text-embedding-3-small sample used by #383's simulation.

Writes two .npy files for repr_recall_sim.py from the public HF dataset
Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K:

    <out>/te3s_queries.npy   the first --holdout rows of a seeded permutation
    <out>/te3s_docs.npy      the remaining rows (the corpus)

The queries are held-out *documents*, so every result from this sample is
doc-to-doc nearest-neighbour recall on DBpedia entities, not short queries
over passages. Say so next to any number taken from it.

With the defaults (seed 383, 1000 held out) this reproduces the split in
docs/research/383-matryoshka-binary.md section 3a.

Needs numpy, pyarrow and huggingface_hub (no HF token: the dataset is
public). About 600 MB of f32 output.

Usage:
    python3 fetch_te3s_dbpedia.py --out /data/te3s
    python3 repr_recall_sim.py --docs /data/te3s/te3s_docs.npy \\
        --queries /data/te3s/te3s_queries.npy --truncate 768,512,384,256,128 \\
        --shortlist 10,20,40,100,200 --segments 1,2,10
"""

import argparse
import os

import numpy as np
import pyarrow.parquet as pq
from huggingface_hub import HfFileSystem

DATASET = "datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K/data"
COLUMN = "text-embedding-3-small-1536-embedding"
SHARDS = 4


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True, help="output directory")
    ap.add_argument("--holdout", type=int, default=1000, help="documents held out as queries")
    ap.add_argument("--seed", type=int, default=383)
    args = ap.parse_args(argv)

    os.makedirs(args.out, exist_ok=True)
    fs = HfFileSystem()
    parts = []
    for i in range(SHARDS):
        path = f"{DATASET}/train-{i:05d}-of-{SHARDS:05d}.parquet"
        table = pq.read_table(fs.open(path, block_size=32 << 20), columns=[COLUMN])
        arr = np.array(table.column(COLUMN).to_pylist(), dtype=np.float32)
        print(path, arr.shape)
        parts.append(arr)
    x = np.concatenate(parts)
    perm = np.random.default_rng(args.seed).permutation(x.shape[0])
    queries, docs = x[perm[: args.holdout]], x[perm[args.holdout :]]
    np.save(os.path.join(args.out, "te3s_docs.npy"), docs)
    np.save(os.path.join(args.out, "te3s_queries.npy"), queries)
    print("docs", docs.shape, "queries", queries.shape)


if __name__ == "__main__":
    main()
