#!/usr/bin/env python3
"""Offline recall simulation for reduced search representations (#383).

Question this answers, without a Kosha server: if a vector field searched a
smaller representation of each embedding (the first d dims, int8 codes, or
one sign bit per dim) to pick a shortlist of R candidates, and then rescored
that shortlist with the full-precision vector, how much of the exact top-k
would survive?

It is a brute-force simulation over one in-memory sample of the corpus, so
it isolates the *representation* loss from everything Kosha's ANN index adds
(IVF probing, per-segment budgets, quantized postings). Kosha's own recall on
the same representation can only be lower or equal at equal shortlist size;
measure that on the bench VM (recall_check.py) once a representation is
worth building.

Ground truth is exact cosine top-k over the same sample with the full
vectors (dot product of unit vectors, matching Kosha post-#143). A smaller
sample is an easier corpus: recall here is an upper bound on what the same
representation gets over 10M docs. Report the sample size with every number.

Representations (bytes/vector at dim D in parentheses):
  full                  exact baseline                         (4*D)
  trunc<d>              first d dims, re-normalized             (4*d)
  int8                  per-vector symmetric scale, as v4 i8    (D + 4)
  bin                   sign bit per dim, Hamming (sym)         (D / 8)
  bin-asym              sign bits for docs, f32 query (asym)    (D / 8)
  trunc<d>-bin          sign bits of the first d dims           (d / 8)

Truncation is only meaningful for a Matryoshka-trained model (e.g. OpenAI
text-embedding-3-*). On a model that was not trained that way (Cohere
embed-english-v3, the bench corpus) it is a negative control. "bin" here is
post-hoc sign quantization, not a vendor's native binary output; a native
binary embedding is trained for it and may do better.

Inputs: raw little-endian f32 files (emb-*.f32 / queries_emb.f32 from
fetch_msmarco_embeddings.py, need --dim) or .npy (any float dtype).

Usage (needs numpy only):
    python3 repr_recall_sim.py --docs /data/corpus/emb-00000.f32 \\
        --docs /data/corpus/emb-00001.f32 --docs-limit 200000 \\
        --queries /data/queries_emb.f32 --dim 1024 --k 10 \\
        --truncate 512,256,128 --shortlist 10,40,100,200 --out sim.json
"""

import argparse
import json
import sys
import time

import numpy as np


def load_matrix(path, dim, limit):
    """Rows of `path` as float32, at most `limit` (None = all)."""
    if path.endswith(".npy"):
        arr = np.load(path, mmap_mode="r")
        if arr.ndim != 2:
            raise SystemExit(f"{path}: expected a 2-D array, got shape {arr.shape}")
    else:
        if not dim:
            raise SystemExit(f"{path}: raw f32 input needs --dim")
        arr = np.memmap(path, dtype="<f4", mode="r")
        if arr.size % dim:
            raise SystemExit(f"{path}: size {arr.size} floats is not a multiple of dim {dim}")
        arr = arr.reshape(-1, dim)
    if limit is not None:
        arr = arr[:limit]
    return np.ascontiguousarray(arr, dtype=np.float32)


def unit(x):
    n = np.linalg.norm(x, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return x / n


def topk_idx(scores, k):
    """Indices of the k largest scores per row, best first."""
    k = min(k, scores.shape[1])
    part = np.argpartition(-scores, k - 1, axis=1)[:, :k]
    order = np.take_along_axis(scores, part, axis=1)
    return np.take_along_axis(part, np.argsort(-order, axis=1), axis=1)


def int8_codes(docs):
    """Kosha v4 style: unit vector scaled by max|x|/127 and rounded."""
    scale = np.abs(docs).max(axis=1, keepdims=True) / 127.0
    scale[scale == 0] = 1.0
    codes = np.clip(np.rint(docs / scale), -127, 127).astype(np.int8)
    return codes, scale.astype(np.float32)


def sign_pm1(x):
    """Sign bits as +-1 float32: x >= 0 -> +1. Dot products of these rank
    exactly as negative Hamming distance (dot = d - 2 * hamming)."""
    return np.where(x >= 0, 1.0, -1.0).astype(np.float32)


def parse_ints(s):
    return [int(v) for v in s.split(",") if v.strip()]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--docs", action="append", required=True, help="doc embedding file (repeatable, concatenated in order)")
    ap.add_argument("--docs-limit", type=int, default=200_000, help="max docs to load in total")
    ap.add_argument("--queries", required=True, help="query embedding file")
    ap.add_argument("--queries-limit", type=int, default=1000)
    ap.add_argument("--dim", type=int, default=0, help="dimension, required for raw .f32 inputs")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--truncate", default="512,256,128", help="prefix lengths to test (comma list, empty = none)")
    ap.add_argument("--shortlist", default="10,40,100,200", help="candidates R rescored with full vectors (comma list)")
    ap.add_argument("--batch", type=int, default=128, help="queries per matmul batch")
    ap.add_argument("--out", help="write results as JSON here")
    args = ap.parse_args(argv)

    t0 = time.time()
    parts, left = [], args.docs_limit
    for p in args.docs:
        if left <= 0:
            break
        m = load_matrix(p, args.dim, left)
        parts.append(m)
        left -= m.shape[0]
    docs = unit(np.concatenate(parts) if len(parts) > 1 else parts[0])
    queries = unit(load_matrix(args.queries, args.dim or docs.shape[1], args.queries_limit))
    n, dim = docs.shape
    if queries.shape[1] != dim:
        raise SystemExit(f"query dim {queries.shape[1]} != doc dim {dim}")
    truncs = [d for d in parse_ints(args.truncate) if 0 < d < dim]
    shortlists = sorted(set(parse_ints(args.shortlist)) | {args.k})
    rmax = max(shortlists)
    k = args.k
    print(f"loaded {n} docs x {dim} dims, {queries.shape[0]} queries in {time.time() - t0:.1f}s", file=sys.stderr)

    # Each representation: (name, bytes/vector, doc matrix, query transform, per-doc scale or None).
    reps = []
    codes, scale = int8_codes(docs)
    reps.append(("int8", dim + 4, codes.astype(np.float32), lambda q: q, scale.ravel()))
    del codes
    bits = sign_pm1(docs)
    reps.append(("bin", dim // 8, bits, sign_pm1, None))
    reps.append(("bin-asym", dim // 8, bits, lambda q: q, None))
    for d in truncs:
        td = unit(docs[:, :d])
        reps.append((f"trunc{d}", 4 * d, td, lambda q, d=d: unit(q[:, :d]), None))
        reps.append((f"trunc{d}-bin", d // 8, sign_pm1(td), lambda q, d=d: sign_pm1(q[:, :d]), None))

    hits = {name: {r: 0 for r in shortlists} for name, *_ in reps}
    total = 0
    for s in range(0, queries.shape[0], args.batch):
        q = queries[s : s + args.batch]
        exact = q @ docs.T
        truth = topk_idx(exact, k)
        for name, _, mat, qf, sc in reps:
            approx = qf(q) @ mat.T
            if sc is not None:
                approx *= sc[None, :]
            short = topk_idx(approx, rmax)  # best-first, so a prefix is the top-R
            for r in shortlists:
                cand = short[:, :r]
                # Rescore the shortlist with the exact full-dim score.
                resc = np.take_along_axis(exact, cand, axis=1)
                kk = min(k, r)
                best = np.take_along_axis(cand, np.argsort(-resc, axis=1)[:, :kk], axis=1)
                for row in range(q.shape[0]):
                    hits[name][r] += len(set(best[row].tolist()) & set(truth[row].tolist()))
        total += q.shape[0] * k
        print(f"  {s + q.shape[0]}/{queries.shape[0]} queries ({time.time() - t0:.0f}s)", file=sys.stderr)

    result = {
        "docs": n,
        "dim": dim,
        "queries": int(queries.shape[0]),
        "k": k,
        "note": "brute-force over an in-memory sample; recall is an upper bound for an ANN index over a larger corpus",
        "representations": [
            {
                "name": name,
                "bytes_per_vector": b,
                "size_vs_f32": round(4 * dim / b, 2),
                "recall_at_k_after_rescore": {str(r): round(hits[name][r] / total, 4) for r in shortlists},
            }
            for name, b, *_ in reps
        ],
    }
    hdr = f"{'repr':<14}{'B/vec':>7}{'x smaller':>10}" + "".join(f"{'R=' + str(r):>9}" for r in shortlists)
    print(f"recall@{k} after full-dim rescore of top-R; {n} docs, {queries.shape[0]} queries, dim {dim}")
    print(hdr)
    for rep in result["representations"]:
        cells = "".join(f"{rep['recall_at_k_after_rescore'][str(r)]:>9.4f}" for r in shortlists)
        print(f"{rep['name']:<14}{rep['bytes_per_vector']:>7}{rep['size_vs_f32']:>10}{cells}")
    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
