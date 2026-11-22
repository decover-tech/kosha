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
(~400MB at the default 100k docs/shard) plus one block of query x shard
similarities (--query-block queries at a time).

Cosine, not raw dot product: matches how Kosha scores kNN post-#143 (vectors
unit-normed at write time, dot-only at read — dot product of unit vectors IS
cosine similarity). Ground truth normalizes both sides the same way so the
comparison is apples to apples with what the server is actually optimizing.

Recall definitions (#384), all per query, then summarized (mean and the
low percentiles: p1/p5/p10, since a probe budget that saves work on easy
queries must not sink the hard ones):

  recall_at_k        exact-id recall: |server top-k ∩ true top-k ids| / k.
                     Penalizes a returned doc that ties the true k-th
                     neighbour but has a different id.
  recall_dist_at_k   ANN-Benchmarks "knn" recall: a returned doc counts if
                     its exact distance (1 - cosine, computed here from the
                     corpus embeddings, never trusted from the server) is
                     <= the true k-th distance + --dist-epsilon (1e-3).
                     Tie-aware.
  recall_eps_at_k    ANN-Benchmarks "epsilon" recall: counts a returned doc
                     whose distance is <= (1 + --recall-eps) x the true k-th
                     distance (default eps 0.01).

Probe depth: with --server-timings the request asks for `timings` (needs
KOSHA_DASHBOARD=on on the server) and each query's `timings.knn_postings`
(server builds with #384) is recorded, so recall can be read against the
number of postings actually probed. With the experimental relative
cutoff on (KOSHA_KNN_RELATIVE_CUTOFF_EPS), `timings.knn_cutoff_rule` says
which rule set each query's cutoff (budget / eps / floor / zero_best) and
the summary counts them in `knn_cutoff_rules`. Older servers omit the
fields; the
summary then says so (and `knn_sweep_report.py` falls back to the
`search timing:` log line's `knn_postings=`).

Sampling: --sampling first (default, unchanged behaviour: the first N
queries) or --sampling random --seed S (N distinct queries drawn uniformly
with a fixed seed; the per-query output lists the query indices so runs are
comparable).

Ground truth for a (sample, k) is the same for every server configuration,
so --truth-cache FILE stores it, together with the exact similarity of
every returned doc seen so far. A later sweep point then skips the
brute-force top-k; it rescans the corpus only to score returned docs that
are neither in the true top-k nor already cached (an exact pass, cheaper
than the top-k one, and skipped entirely when there are none).

Usage:
    python3 recall_check.py --host http://127.0.0.1:8080 \\
        --namespace msmarco-10m-vec --api-key "$KOSHA_API_KEY" \\
        --embeddings-dir /data/corpus --queries-file /data/queries.txt \\
        --queries-emb /data/queries_emb.f32 \\
        --sample 1000 --sampling random --seed 384 --k 10 --num-candidates 100 \\
        --server-timings --no-cache --out recall.json
"""

import argparse
import array
import hashlib
import json
import random
import statistics
import time
from collections import Counter
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


def pct(xs: list[float], p: float) -> float:
    """Linear-interpolated percentile (same as query_bench.py)."""
    if not xs:
        return float("nan")
    s = sorted(xs)
    k = (len(s) - 1) * (p / 100)
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def select_queries(n_total: int, sample: int, sampling: str, seed: int) -> list[int]:
    """Query indices to check: the first `sample`, or `sample` distinct
    indices drawn uniformly with `seed` (sorted, for stable output)."""
    n = min(sample, n_total)
    if sampling == "first" or n == n_total:
        return list(range(n))
    return sorted(random.Random(seed).sample(range(n_total), n))


def server_topk(
    session, host, namespace, headers, field, vector, k, num_candidates, timeout,
    server_timings=False, no_cache=False,
):
    """(doc ids, knn_degraded_segments, timings or None, client latency ms)."""
    payload = {
        "namespace": namespace,
        "query_text": "",
        "max_results": k,
        "knn": {"field": field, "vector": vector, "k": k, "num_candidates": num_candidates},
        # Only doc ids are compared; skip returning stored fields (the
        # embedding alone is ~10-20KB of JSON per hit).
        "source_includes": [],
    }
    if server_timings:
        payload["timings"] = True
    if no_cache:
        payload["no_cache"] = True
    t0 = time.time()
    resp = session.post(f"{host}/search", json=payload, headers=headers, timeout=timeout)
    latency_ms = (time.time() - t0) * 1000
    resp.raise_for_status()
    body = resp.json()
    return (
        [r["doc_id"] for r in body.get("results", [])],
        body.get("knn_degraded_segments"),
        body.get("timings"),
        latency_ms,
    )


def brute_force_topk_streaming(
    embeddings_dir: Path,
    query_vecs: np.ndarray,
    k: int,
    wanted: dict[str, list[int]] | None = None,
    query_block: int = 256,
    need_topk: bool = True,
) -> tuple[list[list[tuple[str, float]]], dict[tuple[int, str], float]]:
    """True top-k (docid, cosine) per query, computed by scanning every
    emb-*.f32/.ids shard once. `query_vecs` is (n_queries, DIM), already
    unit-normalized. Returns one list of (docid, score) top-k pairs per
    query, sorted best-first, plus the exact cosine of every
    (query, docid) pair in `wanted` (docid -> query positions: the docs a
    server returned), for tie-aware recall. With `need_topk=False` only
    the `wanted` similarities are computed (no matmul over the shard) and
    the top-k lists come back empty.
    """
    shard_paths = sorted(embeddings_dir.glob("emb-*.f32"))
    if not shard_paths:
        raise SystemExit(
            f"no emb-*.f32 shards found under {embeddings_dir} — this needs "
            "the corpus embeddings on local disk (fetch_msmarco_embeddings.py "
            "/ load_corpus.py --embeddings-dir), not just the query embeddings"
        )
    wanted = wanted or {}

    n_queries = query_vecs.shape[0]
    # Running best-k per query: parallel lists of (score, docid), kept
    # sorted ascending by score so `[0]` is the current cut point — the one
    # a new candidate must beat to earn a place.
    running: list[list[tuple[float, str]]] = [[] for _ in range(n_queries)]
    returned_sims: dict[tuple[int, str], float] = {}

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

        # Only worth taking the top `k` *candidates* from this shard per
        # query — nothing outside a shard's own top-k can possibly be in
        # the global top-k either, so this is exact, not an approximation.
        local_k = min(k, n_docs)
        for q0 in range(0, n_queries if need_topk else 0, query_block):
            block = query_vecs[q0 : q0 + query_block]
            # (block, n_docs) cosine similarity, one BLAS matmul.
            sims = block @ mat.T
            top_idx = np.argpartition(-sims, local_k - 1, axis=1)[:, :local_k]
            for bi in range(block.shape[0]):
                bucket = running[q0 + bi]
                for di in top_idx[bi]:
                    score = float(sims[bi, di])
                    if len(bucket) < k:
                        bucket.append((score, ids[di]))
                        bucket.sort()
                    elif score > bucket[0][0]:
                        bucket[0] = (score, ids[di])
                        bucket.sort()
            del sims

        # Exact similarity of every doc a server returned, for the
        # distance-based recalls (never the server's own score).
        if wanted:
            for pos, docid in enumerate(ids):
                qis = wanted.get(docid)
                if qis:
                    v = mat[pos]
                    for qi in qis:
                        returned_sims[(qi, docid)] = float(query_vecs[qi] @ v)

        total_docs += n_docs
        print(
            f"  shard {shard_idx + 1}/{len(shard_paths)} ({shard_path.name}, "
            f"{n_docs:,} docs) — {total_docs:,} docs scanned so far"
        )

    truth = [sorted(bucket, key=lambda sd: sd[0], reverse=True) for bucket in running]
    return [[(docid, score) for score, docid in t] for t in truth], returned_sims


def truth_cache_key(embeddings_dir: Path, qidx: list[int], k: int, queries_emb: Path) -> str:
    shards = sorted(p.name for p in embeddings_dir.glob("emb-*.f32"))
    h = hashlib.sha256()
    h.update(json.dumps({"k": k, "qidx": qidx, "shards": shards}).encode())
    h.update(hashlib.sha256(queries_emb.read_bytes()).digest())
    return h.hexdigest()


def recall_metrics(
    got_ids: list[str],
    truth: list[tuple[str, float]],
    sims: dict[str, float],
    k: int,
    dist_epsilon: float,
    recall_eps: float,
) -> dict:
    """Per-query recalls; `truth` is (docid, cosine) best-first, `sims`
    the exact cosine of each returned doc (missing = unknown doc, counts as
    a miss)."""
    true_ids = {docid for docid, _ in truth}
    got = got_ids[:k]
    overlap = len(true_ids & set(got))
    if truth:
        kth_dist = 1.0 - truth[min(k, len(truth)) - 1][1]
    else:
        kth_dist = float("inf")
    dists = [1.0 - sims[d] for d in got if d in sims]
    within_add = sum(1 for d in dists if d <= kth_dist + dist_epsilon)
    within_rel = sum(1 for d in dists if d <= (1.0 + recall_eps) * kth_dist)
    return {
        "recall_at_k": overlap / k,
        "recall_dist_at_k": min(within_add, k) / k,
        "recall_eps_at_k": min(within_rel, k) / k,
        "overlap": overlap,
        "true_kth_distance": kth_dist,
    }


def summarize(xs: list[float]) -> dict:
    if not xs:
        return {"n": 0}
    return {
        "n": len(xs),
        "mean": statistics.fmean(xs),
        "min": min(xs),
        "p1": pct(xs, 1),
        "p5": pct(xs, 5),
        "p10": pct(xs, 10),
        "p50": pct(xs, 50),
        "p90": pct(xs, 90),
        "p99": pct(xs, 99),
        "max": max(xs),
    }


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
    ap.add_argument("--sample", type=int, default=100, help="number of queries to check")
    ap.add_argument(
        "--sampling", choices=["first", "random"], default="first",
        help="first: the first --sample queries (default, the pre-#384 behaviour); "
        "random: --sample distinct queries drawn with --seed",
    )
    ap.add_argument("--seed", type=int, default=384, help="seed for --sampling random")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--num-candidates", type=int, default=100)
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument(
        "--dist-epsilon", type=float, default=1e-3,
        help="additive slack on the true k-th distance for recall_dist_at_k "
        "(ANN-Benchmarks knn recall uses 1e-3)",
    )
    ap.add_argument(
        "--recall-eps", type=float, default=0.01,
        help="relative slack for recall_eps_at_k: (1+eps) x the true k-th distance",
    )
    ap.add_argument(
        "--server-timings", action="store_true",
        help="send timings=true and record timings.knn_postings per query "
        "(needs KOSHA_DASHBOARD=on; knn_postings needs a #384 server)",
    )
    ap.add_argument(
        "--no-cache", action="store_true",
        help="send no_cache=true so every query runs the engine, not the result cache",
    )
    ap.add_argument("--query-block", type=int, default=256, help="queries per brute-force matmul block")
    ap.add_argument(
        "--truth-cache", type=Path, default=None,
        help="JSON file to reuse the true top-k from (written when missing or "
        "keyed to another sample/k/corpus). Returned docs' exact similarities "
        "still need a corpus scan unless every returned doc is in the true "
        "top-k or the cache already holds it",
    )
    ap.add_argument("--label", default=None, help="free-form label stored in the output (sweep point)")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    queries = [l.strip() for l in args.queries_file.read_text().splitlines() if l.strip()]
    query_vecs_raw = read_f32_records(args.queries_emb, DIM)
    if len(query_vecs_raw) != len(queries):
        raise SystemExit(
            f"{len(query_vecs_raw)} query embeddings vs {len(queries)} queries — misaligned inputs"
        )

    qidx = select_queries(len(queries), args.sample, args.sampling, args.seed)
    n = len(qidx)
    if args.sampling == "first" and n < len(queries):
        print(f"sampling first {n} of {len(queries)} queries (deterministic, not random)")
    elif n < len(queries):
        print(f"sampling {n} of {len(queries)} queries at random (seed {args.seed})")
    query_vecs = normalize_rows(np.asarray([query_vecs_raw[i] for i in qidx], dtype=np.float32))

    headers = auth_headers(args.api_key)
    session = requests.Session()

    print(f"querying live server for top-{args.k} ({n} queries, num_candidates={args.num_candidates}) ...")
    server_results: list[list[str]] = []
    degraded_flags: list[int] = []
    postings: list[int | None] = []
    cutoff_rules: list[str | None] = []
    latencies: list[float] = []
    for i in range(n):
        ids, degraded, timings, latency_ms = server_topk(
            session, args.host, args.namespace, headers,
            args.field, query_vecs[i].tolist(), args.k, args.num_candidates, args.timeout,
            server_timings=args.server_timings, no_cache=args.no_cache,
        )
        server_results.append(ids)
        degraded_flags.append(degraded if degraded else 0)
        postings.append((timings or {}).get("knn_postings"))
        cutoff_rules.append((timings or {}).get("knn_cutoff_rule"))
        latencies.append(latency_ms)

    # Ground truth: from the cache when it matches this sample, else a scan.
    truth = None
    cached_sims: dict[str, float] = {}
    key = truth_cache_key(args.embeddings_dir, qidx, args.k, args.queries_emb)
    if args.truth_cache and args.truth_cache.exists():
        try:
            cache = json.loads(args.truth_cache.read_text())
            if cache.get("key") == key:
                truth = [[(d, s) for d, s in t] for t in cache["truth"]]
                cached_sims = cache.get("sims", {})
                print(f"ground truth from {args.truth_cache}")
        except (OSError, ValueError, KeyError) as e:
            print(f"ignoring unreadable truth cache {args.truth_cache}: {e}")

    def sim_key(i: int, docid: str) -> str:
        return f"{qidx[i]}\t{docid}"

    # Exact similarity of every returned doc: known for docs in the true
    # top-k and for those cached from an earlier point; scan for the rest.
    sims: dict[tuple[int, str], float] = {}
    missing: dict[str, list[int]] = {}
    for i in range(n):
        in_truth = dict(truth[i]) if truth is not None else {}
        for docid in server_results[i][: args.k]:
            if docid in in_truth:
                sims[(i, docid)] = in_truth[docid]
            elif sim_key(i, docid) in cached_sims:
                sims[(i, docid)] = cached_sims[sim_key(i, docid)]
            else:
                missing.setdefault(docid, []).append(i)

    if truth is None or missing:
        what = "exact brute-force top-k" if truth is None else f"exact similarity of {len(missing)} returned docs"
        print(f"computing {what} over {args.embeddings_dir} ...")
        scanned_truth, returned_sims = brute_force_topk_streaming(
            args.embeddings_dir, query_vecs, args.k, wanted=missing, query_block=args.query_block,
            need_topk=truth is None,
        )
        if truth is None:
            truth = scanned_truth
            for i in range(n):
                in_truth = dict(truth[i])
                for docid in server_results[i][: args.k]:
                    if docid in in_truth:
                        sims[(i, docid)] = in_truth[docid]
        sims.update(returned_sims)
        if args.truth_cache:
            cached_sims.update({sim_key(i, d): s for (i, d), s in sims.items()})
            args.truth_cache.write_text(
                json.dumps({"key": key, "k": args.k, "qidx": qidx, "truth": truth, "sims": cached_sims})
            )
            print(f"wrote ground truth to {args.truth_cache}")

    per_query = []
    rows = {"recall_at_k": [], "recall_dist_at_k": [], "recall_eps_at_k": []}
    top1_matches = 0
    for i in range(n):
        got_ids = server_results[i]
        returned = {d: sims[(i, d)] for d in got_ids[: args.k] if (i, d) in sims}
        m = recall_metrics(got_ids, truth[i], returned, args.k, args.dist_epsilon, args.recall_eps)
        for key_ in rows:
            rows[key_].append(m[key_])
        top1_true = truth[i][0][0] if truth[i] else None
        top1_match = bool(got_ids) and got_ids[0] == top1_true
        top1_matches += top1_match
        per_query.append(
            {
                "qidx": qidx[i],
                "query": queries[qidx[i]],
                **m,
                "top1_match": top1_match,
                "knn_degraded_segments": degraded_flags[i],
                "knn_postings": postings[i],
                "knn_cutoff_rule": cutoff_rules[i],
                "latency_ms": round(latencies[i], 3),
                "server_top1": got_ids[0] if got_ids else None,
                "true_top1": top1_true,
            }
        )

    recalls = rows["recall_at_k"]
    probe_counts = [p for p in postings if p is not None]
    summary = {
        "label": args.label,
        "namespace": args.namespace,
        "sample_size": n,
        "sampling": args.sampling,
        "seed": args.seed if args.sampling == "random" else None,
        "k": args.k,
        "num_candidates": args.num_candidates,
        # Unchanged keys (benchmark.yml's summary reads these).
        "mean_recall_at_k": statistics.fmean(recalls) if recalls else float("nan"),
        "min_recall_at_k": min(recalls) if recalls else float("nan"),
        "median_recall_at_k": statistics.median(recalls) if recalls else float("nan"),
        "top1_match_rate": top1_matches / n if n else float("nan"),
        "queries_with_degraded_segments": sum(1 for d in degraded_flags if d > 0),
        # #384 additions.
        "p10_recall_at_k": pct(recalls, 10),
        "recall_at_k_dist": summarize(recalls),
        "recall_dist_at_k": summarize(rows["recall_dist_at_k"]),
        "recall_eps_at_k": summarize(rows["recall_eps_at_k"]),
        "dist_epsilon": args.dist_epsilon,
        "recall_eps": args.recall_eps,
        "knn_postings": summarize([float(p) for p in probe_counts]) if probe_counts else None,
        "knn_postings_reported": len(probe_counts),
        # Which rule set each query's probe cutoff (#384 relative cutoff on
        # only; empty otherwise): budget / eps / floor / zero_best counts.
        "knn_cutoff_rules": dict(sorted(Counter(r for r in cutoff_rules if r).items())),
        # Sequential (closed-loop) client latency of the recall queries:
        # informational only; the 8-QPS phase is the latency measurement.
        "sequential_latency_ms": summarize(latencies),
        "per_query": per_query,
    }

    printable = {k_: v for k_, v in summary.items() if k_ != "per_query"}
    print(json.dumps(printable, indent=2))
    if args.server_timings and not probe_counts:
        print(
            "\nno timings.knn_postings in any response: server older than #384, "
            "KOSHA_DASHBOARD off, or every query served from the result cache "
            "(use --no-cache). Read knn_postings= from the server log instead."
        )
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
