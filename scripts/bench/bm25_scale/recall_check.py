#!/usr/bin/env python3
"""kNN recall@k against exact brute-force ground truth over the REAL corpus.

`query_bench.py` (and the server's own `knn_degraded_segments` signal) can
tell you a request succeeded and came back full — neither can tell you the
neighbors it returned are actually close to correct. A broken or badly
under-probed ANN index returns a full, fast, "successful" page of the WRONG
docs. This script is the check that catches that: for a sample of real
queries, it asks the live server for its top-k (exactly the request shape
`query_bench.py` sends) and separately computes the TRUE top-k by brute-force
cosine similarity against every document embedding in the corpus, then
reports what fraction of the server's answer is actually in the true top-k.

Ground truth is streamed shard-by-shard (emb-*.f32/.ids from
fetch_msmarco_embeddings.py / load_corpus.py's --embeddings-dir) rather than
loaded into memory at once — a 10M-doc x 1024-dim corpus is ~41GB, more than
this needs to hold resident to get an exact answer. Peak memory is one shard
(~400MB at the default 100k docs/shard) plus the query sample.

Cosine, not raw dot product: matches how Kosha scores kNN post-#143 (vectors
unit-normed at write time, dot-only at read — dot product of unit vectors IS
cosine similarity). Ground truth normalizes both sides the same way so the
comparison is apples to apples with what the server is actually optimizing.

Usage:
    python3 recall_check.py --host http://127.0.0.1:8080 \\
        --namespace msmarco-10m-vec --api-key "$KOSHA_API_KEY" \\
        --embeddings-dir /data/corpus --queries-file /data/queries.txt \\
        --queries-emb /data/queries_emb.f32 \\
        --sample 100 --k 10 --num-candidates 100 --out recall.json
"""

import argparse
import array
import json
import statistics
from pathlib import Path

import numpy as np
import requests

DIM = 1024


def auth_headers(api_key: str | None) -> dict:
    return {"Authorization": f"Bearer {api_key}"} if api_key else {}


def read_f32_records(path: Path, dim: int) -> list[list[float]]:
    raw = path.read_bytes()
    if len(raw) % (dim * 4) != 0:
        raise SystemExit(f"{path} is not a whole number of {dim}-float records")
    out = []
    for off in range(0, len(raw), dim * 4):
        a = array.array("f")
        a.frombytes(raw[off : off + dim * 4])
        out.append(list(a))
    return out


def normalize_rows(mat: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0  # a zero vector stays zero rather than NaN-ing out
    return mat / norms


def server_topk(
    session, host, namespace, headers, field, vector, k, num_candidates, timeout
) -> list[str]:
    payload = {
        "namespace": namespace,
        "query_text": "",
        "max_results": k,
        "knn": {"field": field, "vector": vector, "k": k, "num_candidates": num_candidates},
    }
    resp = session.post(f"{host}/search", json=payload, headers=headers, timeout=timeout)
    resp.raise_for_status()
    body = resp.json()
    return [r["doc_id"] for r in body.get("results", [])], body.get("knn_degraded_segments")


def brute_force_topk_streaming(
    embeddings_dir: Path, query_vecs: np.ndarray, k: int
) -> list[list[tuple[str, float]]]:
    """True top-k (docid, cosine) per query, computed by scanning every
    emb-*.f32/.ids shard once. `query_vecs` is (n_queries, DIM), already
    unit-normalized. Returns one list of (docid, score) top-k pairs per
    query, sorted best-first.
    """
    shard_paths = sorted(embeddings_dir.glob("emb-*.f32"))
    if not shard_paths:
        raise SystemExit(
            f"no emb-*.f32 shards found under {embeddings_dir} — this needs "
            "the corpus embeddings on local disk (fetch_msmarco_embeddings.py "
            "/ load_corpus.py --embeddings-dir), not just the query embeddings"
        )

    n_queries = query_vecs.shape[0]
    # Running best-k per query: parallel lists of (score, docid), kept
    # sorted ascending by score so `[0]` is the current cut point — the one
    # a new candidate must beat to earn a place.
    running: list[list[tuple[float, str]]] = [[] for _ in range(n_queries)]

    total_docs = 0
    for shard_idx, shard_path in enumerate(shard_paths):
        ids_path = shard_path.with_suffix(".ids")
        if not ids_path.exists():
            raise SystemExit(f"{shard_path} has no matching {ids_path.name} sidecar")
        ids = ids_path.read_text().splitlines()
        raw = shard_path.read_bytes()
        if len(raw) % (DIM * 4) != 0:
            raise SystemExit(f"{shard_path} is not a whole number of {DIM}-float records")
        n_docs = len(raw) // (DIM * 4)
        if n_docs != len(ids):
            raise SystemExit(
                f"{shard_path.name}: {n_docs} vectors vs {len(ids)} ids — misaligned shard"
            )
        mat = np.frombuffer(raw, dtype=np.float32).reshape(n_docs, DIM)
        mat = normalize_rows(mat.astype(np.float32, copy=False))

        # (n_queries, n_docs) cosine similarity block, one BLAS matmul.
        sims = query_vecs @ mat.T

        # Only worth taking the top `k` *candidates* from this shard per
        # query — nothing outside a shard's own top-k can possibly be in
        # the global top-k either, so this is exact, not an approximation.
        local_k = min(k, n_docs)
        top_idx = np.argpartition(-sims, local_k - 1, axis=1)[:, :local_k]
        for qi in range(n_queries):
            for di in top_idx[qi]:
                score = float(sims[qi, di])
                docid = ids[di]
                bucket = running[qi]
                if len(bucket) < k:
                    bucket.append((score, docid))
                    bucket.sort()
                elif score > bucket[0][0]:
                    bucket[0] = (score, docid)
                    bucket.sort()

        total_docs += n_docs
        print(
            f"  shard {shard_idx + 1}/{len(shard_paths)} ({shard_path.name}, "
            f"{n_docs:,} docs) — {total_docs:,} docs scanned so far"
        )

    return [
        sorted(bucket, key=lambda sd: sd[0], reverse=True) for bucket in running
    ]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", required=True)
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--api-key", default=None)
    ap.add_argument(
        "--embeddings-dir", type=Path, required=True,
        help="dir with emb-*.f32/.ids corpus shards (fetch_msmarco_embeddings.py output)",
    )
    ap.add_argument("--queries-file", type=Path, required=True)
    ap.add_argument("--queries-emb", type=Path, required=True, help="queries_emb.f32")
    ap.add_argument("--field", default="text_emb")
    ap.add_argument("--sample", type=int, default=100, help="number of queries to check (first N, deterministic)")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--num-candidates", type=int, default=100)
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    queries = [l.strip() for l in args.queries_file.read_text().splitlines() if l.strip()]
    query_vecs_raw = read_f32_records(args.queries_emb, DIM)
    if len(query_vecs_raw) != len(queries):
        raise SystemExit(
            f"{len(query_vecs_raw)} query embeddings vs {len(queries)} queries — misaligned inputs"
        )

    n = min(args.sample, len(queries))
    if n < len(queries):
        print(f"sampling first {n} of {len(queries)} queries (deterministic, not random)")
    query_vecs = normalize_rows(np.asarray(query_vecs_raw[:n], dtype=np.float32))

    headers = auth_headers(args.api_key)
    session = requests.Session()

    print(f"querying live server for top-{args.k} ({n} queries, num_candidates={args.num_candidates}) ...")
    server_results: list[list[str]] = []
    degraded_flags: list[int] = []
    for i in range(n):
        ids, degraded = server_topk(
            session, args.host, args.namespace, headers,
            args.field, query_vecs[i].tolist(), args.k, args.num_candidates, args.timeout,
        )
        server_results.append(ids)
        degraded_flags.append(degraded if degraded else 0)

    print(f"computing exact brute-force top-{args.k} over {args.embeddings_dir} ...")
    truth = brute_force_topk_streaming(args.embeddings_dir, query_vecs, args.k)

    per_query = []
    recalls = []
    top1_matches = 0
    for i in range(n):
        true_ids = {docid for _, docid in truth[i]}
        got_ids = server_results[i]
        overlap = len(true_ids & set(got_ids))
        recall = overlap / args.k
        recalls.append(recall)
        top1_true = truth[i][0][1] if truth[i] else None
        top1_match = bool(got_ids) and got_ids[0] == top1_true
        top1_matches += top1_match
        per_query.append(
            {
                "query": queries[i],
                "recall_at_k": recall,
                "overlap": overlap,
                "top1_match": top1_match,
                "knn_degraded_segments": degraded_flags[i],
                "server_top1": got_ids[0] if got_ids else None,
                "true_top1": top1_true,
            }
        )

    summary = {
        "namespace": args.namespace,
        "sample_size": n,
        "k": args.k,
        "num_candidates": args.num_candidates,
        "mean_recall_at_k": statistics.fmean(recalls) if recalls else float("nan"),
        "min_recall_at_k": min(recalls) if recalls else float("nan"),
        "median_recall_at_k": statistics.median(recalls) if recalls else float("nan"),
        "top1_match_rate": top1_matches / n if n else float("nan"),
        "queries_with_degraded_segments": sum(1 for d in degraded_flags if d > 0),
        "per_query": per_query,
    }

    print(json.dumps({k: v for k, v in summary.items() if k != "per_query"}, indent=2))
    if summary["mean_recall_at_k"] < 0.9:
        print(
            f"\n⚠ mean recall@{args.k} = {summary['mean_recall_at_k']:.2f} — below the "
            "0.9 bar kosha-vector-spfresh's own unit tests require of the ANN index in "
            "isolation. This measures the whole serving path (index + num_candidates "
            "truncation + kNN merge), so a low number here doesn't localize the cause "
            "on its own — but it does mean the search results are measurably not what "
            "a client asked for."
        )

    if args.out:
        args.out.write_text(json.dumps(summary, indent=2))
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
