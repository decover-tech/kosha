#!/usr/bin/env python3
"""Offline recall simulation for reduced search representations (#383).

Question this answers, without a Kosha server: if a vector field searched a
smaller representation of each embedding (the first d dims, int8 codes, or
one sign bit per dim) to pick a shortlist of R candidates, and then rescored
that shortlist with the full-precision vector, how much of the exact top-k
would survive?

It is a brute-force simulation over one in-memory sample of the corpus, so
it isolates the *representation* loss from IVF probing. It is NOT a bound on
Kosha's recall in either direction:

  * Kosha does not rescore one global top-R list. Each segment rescores its
    own best k * KOSHA_VECTOR_RESCORE_FACTOR probe candidates, and the
    segments' results are merged. A true neighbour only has to rank in its
    own segment's top R. --segments N models that: the sample is split into
    N contiguous shards, each shard keeps its own top R by the approximate
    score, and the union (N * R candidates) is rescored. With --segments 1
    (the default) R is one global shortlist and does not map to
    KOSHA_VECTOR_RESCORE_FACTOR.
  * IVF probing loses neighbours the simulator never misses; a 10M corpus
    has denser neighbourhoods than a 100k sample; RaBitQ columns widen
    their shortlist by an error bound. None of that is modelled.

Measure the engine on the bench VM (recall_check.py) once a representation
is worth building. Report the sample size and --segments with every number.

Ground truth is exact cosine top-k over the same sample with the full
vectors (dot product of unit vectors, matching Kosha post-#143).

Bytes: "B/vec" is the search representation only (what vector.idx / the
probe would hold per entry, ignoring the 4-byte doc id and any posting
replication). Kosha also keeps the full f32 vector in doc_vectors.bin for
rescoring, so "total x" compares search repr + 4*D rescore bytes with
today's f32 layout (4*D in vector.idx + 4*D in doc_vectors.bin).

Representations (bytes/vector at dim D in parentheses):
  full                  exact baseline                         (4*D)
  trunc<d>              first d dims, re-normalized             (4*d)
  int8                  per-vector symmetric scale, as v4 i8    (D + 4)
  trunc<d>-int8         int8 codes of the first d dims          (d + 4)
  bin                   sign bit per dim, Hamming (sym)         (D / 8)
  bin-asym              sign bits for docs, f32 query (asym)    (D / 8)
  trunc<d>-bin          sign bits of the first d dims           (d / 8)

Two-stage mode (--rescore): by default the shortlist is rescored with the
exact full vector ("full"), so the rescore store is 4*D bytes and total x
cannot exceed 2. --rescore full,trunc768,int8 also rescores each shortlist
with a *reduced rescore store* (any representation above except bin, bin-asym
and trunc<d>-bin; f32 query against it), takes that top-k, and compares it
with the exact full-dim top-k. Total bytes are then search repr + rescore
repr. This is the layout question of docs/research/383-matryoshka-binary.md
section 3c: whether some search + rescore pair is >= 4x smaller in total at
recall within 0.01. --search restricts which search representations run.

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
        --truncate 512,256,128 --shortlist 10,40,100,200 \\
        --segments 1,2,10 --out sim.json

    # Two-stage: sign-bit search, then a 768-dim prefix or int8 rescore store.
    python3 repr_recall_sim.py --docs te3s_docs.npy --queries te3s_queries.npy \
        --search bin-asym,trunc384 --truncate 768,384 \
        --rescore full,trunc768,int8,trunc768-int8 --segments 1,2
"""

import argparse
import json
import re
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


def parse_names(s):
    return [v.strip() for v in s.split(",") if v.strip()]


def make_rep(name, docs):
    """(bytes/vector, doc matrix, query transform, per-doc scale or None) for
    representation `name`, or None if the name is unknown or its prefix is
    not shorter than the dimension."""
    dim = docs.shape[1]
    if name == "full":
        return 4 * dim, docs, lambda q: q, None
    m = re.fullmatch(r"trunc(\d+)(?:-(int8|bin))?", name)
    if m:
        d, enc = int(m.group(1)), m.group(2)
        if not 0 < d < dim:
            return None
        src = unit(docs[:, :d])

        def qd(q, d=d):
            return unit(q[:, :d])

    elif name in ("int8", "bin", "bin-asym"):
        d, enc, src = dim, name, docs

        def qd(q):
            return q

    else:
        return None
    if enc is None:
        return 4 * d, src, qd, None
    if enc == "int8":
        codes, scale = int8_codes(src)
        return d + 4, codes.astype(np.float32), qd, scale.ravel()
    if enc == "bin":
        return d // 8, sign_pm1(src), lambda q: sign_pm1(qd(q)), None
    return d // 8, sign_pm1(src), qd, None  # bin-asym: sign bits for docs, f32 query


def scores(rep, q):
    _, mat, qf, sc = rep
    out = qf(q) @ mat.T
    if sc is not None:
        out *= sc[None, :]
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--docs", action="append", required=True, help="doc embedding file (repeatable, concatenated in order)")
    ap.add_argument("--docs-limit", type=int, default=200_000, help="max docs to load in total")
    ap.add_argument("--queries", required=True, help="query embedding file")
    ap.add_argument("--queries-limit", type=int, default=1000)
    ap.add_argument("--dim", type=int, default=0, help="dimension, required for raw .f32 inputs")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--truncate", default="512,256,128", help="prefix lengths to test (comma list, empty = none)")
    ap.add_argument(
        "--search",
        default="",
        help="search representations to run (comma list); default int8, bin, bin-asym "
        "and trunc<d>, trunc<d>-bin for each --truncate d",
    )
    ap.add_argument(
        "--rescore",
        default="full",
        help="rescore representations (comma list): full (exact, today's doc_vectors.bin), "
        "trunc<d>, int8, trunc<d>-int8",
    )
    ap.add_argument("--shortlist", default="10,40,100,200", help="candidates R rescored (comma list)")
    ap.add_argument(
        "--segments",
        default="1",
        help="comma list of shard counts N; R is then the shortlist per shard, "
        "as in Kosha's per-segment rescore (1 = one global shortlist)",
    )
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
    seg_counts = sorted({c for c in parse_ints(args.segments) if 0 < c <= n})
    if not seg_counts:
        raise SystemExit("--segments needs at least one N with 0 < N <= docs")
    # Contiguous shards, like segments in ingest order.
    bounds = {c: np.linspace(0, n, c + 1).astype(int) for c in seg_counts}
    print(f"loaded {n} docs x {dim} dims, {queries.shape[0]} queries in {time.time() - t0:.1f}s", file=sys.stderr)

    search_names = parse_names(args.search) or (
        ["int8", "bin", "bin-asym"] + [f"trunc{d}{e}" for d in truncs for e in ("", "-bin")]
    )
    rescore_names = parse_names(args.rescore)
    for name in rescore_names:
        if name.endswith("bin") or name.endswith("bin-asym"):
            raise SystemExit(f"--rescore {name}: sign bits are a search representation, not a rescore store")
    reps = {}
    for name in dict.fromkeys(search_names + rescore_names):
        if name == "full":
            continue
        rep = make_rep(name, docs)
        if rep is None:
            raise SystemExit(f"unknown representation or prefix not < {dim}: {name}")
        reps[name] = rep
    reps["full"] = make_rep("full", docs)

    def hits_table():
        return {s: {c: {r: 0 for r in shortlists} for c in seg_counts} for s in search_names}

    hits = {r: hits_table() for r in rescore_names}
    total = 0
    for s in range(0, queries.shape[0], args.batch):
        q = queries[s : s + args.batch]
        exact = scores(reps["full"], q)
        truth = topk_idx(exact, k)
        resc_scores = {r: exact if r == "full" else scores(reps[r], q) for r in rescore_names}
        for name in search_names:
            approx = exact if name == "full" else scores(reps[name], q)
            for segs in seg_counts:
                b = bounds[segs]
                # Per shard, best-first, so a prefix is that shard's top-R.
                shard_short = [topk_idx(approx[:, b[i] : b[i + 1]], rmax) + b[i] for i in range(segs)]
                for r in shortlists:
                    cand = np.concatenate([sh[:, :r] for sh in shard_short], axis=1)
                    kk = min(k, cand.shape[1])
                    for rname, rs in resc_scores.items():
                        # Rescore the union of the shortlists with the rescore store's score.
                        resc = np.take_along_axis(rs, cand, axis=1)
                        best = np.take_along_axis(cand, np.argsort(-resc, axis=1)[:, :kk], axis=1)
                        found = (best[:, :, None] == truth[:, None, :]).any(axis=2)
                        hits[rname][name][segs][r] += int(found.sum())
        total += q.shape[0] * k
        print(f"  {s + q.shape[0]}/{queries.shape[0]} queries ({time.time() - t0:.0f}s)", file=sys.stderr)

    def entry(name, rname):
        b = reps[name][0]
        rb = reps[rname][0]
        e = {
            "name": name,
            "bytes_per_vector": b,
            "size_vs_f32": round(4 * dim / b, 2),
            "total_bytes_per_doc": b + rb,
            "total_vs_today": round(8 * dim / (b + rb), 2),
            # Keyed by shard count, then by R (per shard).
            "recall_at_k_after_rescore": {
                str(c): {str(r): round(hits[rname][name][c][r] / total, 4) for r in shortlists} for c in seg_counts
            },
        }
        if rname != "full":
            e["rescore"] = rname
            e["rescore_bytes_per_vector"] = rb
        return e

    result = {
        "docs": n,
        "dim": dim,
        "queries": int(queries.shape[0]),
        "k": k,
        "segments": seg_counts,
        "note": (
            "brute-force over an in-memory sample; not a bound on Kosha recall. "
            "R is per shard when segments > 1 (Kosha rescores k*rescore_factor per segment); "
            "bytes_per_vector is the search representation only, "
            "total_bytes_per_doc adds the rescore store (4*dim f32 unless two_stage)"
        ),
        "representations": [entry(name, "full") for name in search_names] if "full" in rescore_names else [],
        "two_stage": [entry(name, r) for r in rescore_names if r != "full" for name in search_names],
    }
    print(f"recall@{k} vs exact full-dim top-{k}; {n} docs, {queries.shape[0]} queries, dim {dim}")
    print(f"B/vec = search repr only; total x = vs today's {4 * dim} B vector.idx + {4 * dim} B doc_vectors.bin")
    groups = [("full", result["representations"])] if result["representations"] else []
    groups += [(r, [e for e in result["two_stage"] if e["rescore"] == r]) for r in rescore_names if r != "full"]
    for rname, rows in groups:
        print(f"rescore store: {rname} ({reps[rname][0]} B/vec)")
        for c in seg_counts:
            scope = "one global top-R" if c == 1 else f"{c} shards of ~{n // c} docs, top-R per shard"
            print(f"segments={c} ({scope})")
            print(
                f"{'repr':<14}{'B/vec':>7}{'x smaller':>10}{'total x':>9}"
                + "".join(f"{'R=' + str(r):>9}" for r in shortlists)
            )
            for rep in rows:
                cells = "".join(f"{rep['recall_at_k_after_rescore'][str(c)][str(r)]:>9.4f}" for r in shortlists)
                print(
                    f"{rep['name']:<14}{rep['bytes_per_vector']:>7}{rep['size_vs_f32']:>10}"
                    f"{rep['total_vs_today']:>9}{cells}"
                )
    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
