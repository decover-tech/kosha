#!/usr/bin/env python3
"""Microbenchmark: BM25 + kNN search on OpenSearch vs Kosha.

Indexes an identical corpus into both engines and runs the same query
set against both, reporting:

  - Performance: single-client query latency (p50/p95/mean) over N
    repetitions per query (first rep discarded as warmup).
  - Correctness: top-K result-set agreement (Jaccard, top-1 match,
    Kendall tau) between the two engines.
  - Indexing time for the full corpus into each engine.

Modes
  lexical  — BM25 keyword search  (GET  /search for Kosha)
  semantic — kNN vector search    (POST /search for Kosha, knn field)
  both     — runs both modes on the same corpus

Semantic mode expects a ``vector`` field (array of floats) on each
corpus document and each query.

Usage:
    # Lexical only
    python3 scripts/bench/run_benchmark.py \\
        --corpus corpus.jsonl --queries queries.json \\
        --os-host http://localhost:9250 \\
        --kosha-host http://localhost:8099 \\
        --mode lexical --out results.json

    # Semantic (kNN) only
    python3 scripts/bench/run_benchmark.py \\
        --corpus corpus.jsonl --queries queries.json \\
        --os-host http://localhost:9250 \\
        --kosha-host http://localhost:8099 \\
        --mode semantic --out results.json
"""

import argparse
import json
import statistics
import time
from pathlib import Path

import requests

# ══════════════════════════════════════════════════════════════════════
# Index settings
# ══════════════════════════════════════════════════════════════════════

LEXICAL_ANALYZER = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "filter": {
                "strip_edge_punct": {
                    "type": "pattern_replace",
                    "pattern": "^\\p{Punct}+|\\p{Punct}+$",
                    "replacement": "",
                },
                "drop_empty": {"type": "length", "min": 1},
            },
            "analyzer": {
                "kosha_match": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "filter": ["lowercase", "strip_edge_punct", "drop_empty"],
                }
            },
        },
        "similarity": {"default": {"type": "BM25", "k1": 1.2, "b": 0.75}},
    },
    "mappings": {
        "properties": {
            "text": {"type": "text", "analyzer": "kosha_match"},
            "source_file": {"type": "keyword"},
            "doc_type": {"type": "keyword"},
            "page": {"type": "integer"},
        }
    },
}

KNN_SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "index": {"knn": True},
    },
    "mappings": {
        "properties": {
            "text": {"type": "text", "analyzer": "standard"},
            "vector": {
                "type": "knn_vector",
                "dimension": 768,
                "method": {
                    "name": "hnsw",
                    "space_type": "cosinesimil",
                    "engine": "lucene",
                },
            },
            "source_file": {"type": "keyword"},
            "doc_type": {"type": "keyword"},
            "page": {"type": "integer"},
        }
    },
}

# ══════════════════════════════════════════════════════════════════════
# Data loading helpers
# ══════════════════════════════════════════════════════════════════════


def load_corpus(path: Path) -> list[dict]:
    docs = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                docs.append(json.loads(line))
    return docs


def load_queries(path: Path) -> list[dict]:
    return json.loads(path.read_text())


# ══════════════════════════════════════════════════════════════════════
# OpenSearch helpers
# ══════════════════════════════════════════════════════════════════════


def _delete_index(host: str, index: str) -> None:
    requests.delete(f"{host}/{index}")


def _bulk_index(host: str, index: str, docs: list[dict]) -> None:
    lines = []
    for doc in docs:
        lines.append(json.dumps({"index": {"_index": index, "_id": doc["id"]}}))
        lines.append(json.dumps({k: v for k, v in doc.items() if k != "id"}))
    resp = requests.post(
        f"{host}/_bulk",
        data="\n".join(lines) + "\n",
        headers={"content-type": "application/x-ndjson"},
        timeout=120,
    )
    resp.raise_for_status()
    if resp.json().get("errors"):
        errored = [
            i for i in resp.json()["items"]
            if i.get("index", {}).get("status", 200) >= 300
        ]
        raise RuntimeError(
            f"opensearch bulk index reported {len(errored)} errors: {errored[:3]}"
        )
    requests.post(f"{host}/{index}/_refresh").raise_for_status()


def _os_vector_dim(docs: list[dict]) -> int:
    for d in docs:
        v = d.get("vector")
        if v:
            return len(v)
    return 768


# ══════════════════════════════════════════════════════════════════════
# Lexical (BM25) setup + search
# ══════════════════════════════════════════════════════════════════════


def setup_opensearch_lexical(
    host: str, index: str, docs: list[dict]
) -> float:
    _delete_index(host, index)
    resp = requests.put(f"{host}/{index}", json=LEXICAL_ANALYZER)
    resp.raise_for_status()
    start = time.monotonic()
    _bulk_index(host, index, docs)
    return time.monotonic() - start


def search_opensearch_lexical(
    host: str, index: str, query: str, size: int
) -> tuple[list[tuple[str, float]], float]:
    body = {"query": {"match": {"text": query}}, "size": size}
    start = time.monotonic()
    resp = requests.post(f"{host}/{index}/_search", json=body)
    elapsed_ms = (time.monotonic() - start) * 1000
    resp.raise_for_status()
    hits = resp.json()["hits"]["hits"]
    return [(h["_id"], h["_score"]) for h in hits], elapsed_ms


def search_kosha_lexical(
    host: str, namespace: str, query: str, size: int
) -> tuple[list[tuple[str, float]], float]:
    start = time.monotonic()
    resp = requests.get(
        f"{host}/search",
        params={"ns": namespace, "q": query, "max_results": size},
    )
    elapsed_ms = (time.monotonic() - start) * 1000
    resp.raise_for_status()
    results = resp.json()["results"]
    return [(r["doc_id"], r["score"]) for r in results], elapsed_ms


# ══════════════════════════════════════════════════════════════════════
# Semantic (kNN) setup + search
# ══════════════════════════════════════════════════════════════════════


def setup_opensearch_semantic(
    host: str, index: str, docs: list[dict]
) -> float:
    _delete_index(host, index)
    settings = dict(KNN_SETTINGS)
    dim = _os_vector_dim(docs)
    settings["mappings"]["properties"]["vector"]["dimension"] = dim
    resp = requests.put(f"{host}/{index}", json=settings)
    resp.raise_for_status()
    start = time.monotonic()
    _bulk_index(host, index, docs)
    return time.monotonic() - start


def search_opensearch_semantic(
    host: str, index: str, vector: list[float], size: int
) -> tuple[list[tuple[str, float]], float]:
    body = {
        "query": {"knn": {"vector": {"vector": vector, "k": size}}},
        "size": size,
    }
    start = time.monotonic()
    resp = requests.post(f"{host}/{index}/_search", json=body)
    elapsed_ms = (time.monotonic() - start) * 1000
    resp.raise_for_status()
    hits = resp.json()["hits"]["hits"]
    return [(h["_id"], h["_score"]) for h in hits], elapsed_ms


def search_kosha_semantic(
    host: str, namespace: str, vector: list[float], size: int
) -> tuple[list[tuple[str, float]], float]:
    body = {
        "namespace": namespace,
        "query_text": "",
        "max_results": size,
        "knn": {"vector": {"vector": vector, "k": size}},
    }
    start = time.monotonic()
    resp = requests.post(f"{host}/search", json=body)
    elapsed_ms = (time.monotonic() - start) * 1000
    resp.raise_for_status()
    results = resp.json()["results"]
    return [(r["doc_id"], r["score"]) for r in results], elapsed_ms


# ══════════════════════════════════════════════════════════════════════
# Setup dispatcher
# ══════════════════════════════════════════════════════════════════════


def setup_kosha(host: str, namespace: str, docs: list[dict]) -> float:
    """Index documents into Kosha.

    Handles both lexical (text-only) and semantic (text + vector) docs.
    """
    start = time.monotonic()
    has_vectors = any("vector" in d for d in docs)

    documents = []
    for doc in docs:
        fields = [
            {"name": "text", "field_type": "Text", "value": doc.get("text", "")},
            {"name": "source_file", "field_type": "Keyword", "value": doc.get("source_file", "")},
            {"name": "doc_type", "field_type": "Keyword", "value": doc.get("doc_type", "")},
        ]
        if has_vectors and "vector" in doc:
            fields.append(
                {
                    "name": "vector",
                    "field_type": "Vector",
                    "value": json.dumps(doc["vector"]),
                }
            )
        documents.append({"id": doc["id"], "fields": fields})

    resp = requests.post(
        f"{host}/index",
        json={"namespace": namespace, "documents": documents},
        timeout=120,
    )
    resp.raise_for_status()

    requests.post(f"{host}/flush", json={"namespace": namespace}).raise_for_status()
    return time.monotonic() - start


# ══════════════════════════════════════════════════════════════════════
# Latency stats
# ══════════════════════════════════════════════════════════════════════


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * (pct / 100)
    f, c = int(k), min(int(k) + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] + (values[c] - values[f]) * (k - f)


def latency_stats(samples: list[float]) -> dict:
    return {
        "p50_ms": round(percentile(samples, 50), 3),
        "p95_ms": round(percentile(samples, 95), 3),
        "mean_ms": round(statistics.mean(samples), 3),
        "min_ms": round(min(samples), 3),
        "max_ms": round(max(samples), 3),
        "n": len(samples),
    }


# ══════════════════════════════════════════════════════════════════════
# Correctness metrics
# ══════════════════════════════════════════════════════════════════════


def jaccard(a: list[str], b: list[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    return len(sa & sb) / len(sa | sb)


def rank_correlation(a: list[str], b: list[str]) -> float | None:
    """Kendall tau over the subset of ids present in both rankings."""
    common = [d for d in a if d in b]
    if len(common) < 2:
        return None
    rank_a = {d: i for i, d in enumerate(common)}
    rank_b = {d: b.index(d) for d in common}
    concordant = discordant = 0
    for i in range(len(common)):
        for j in range(i + 1, len(common)):
            di = rank_a[common[i]] - rank_a[common[j]]
            dj = rank_b[common[i]] - rank_b[common[j]]
            if di * dj > 0:
                concordant += 1
            elif di * dj < 0:
                discordant += 1
    total = concordant + discordant
    if total == 0:
        return 1.0
    return (concordant - discordant) / total


# ══════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════


def run_mode(
    mode: str,
    docs: list[dict],
    queries: list[dict],
    os_host: str,
    kosha_host: str,
    index: str,
    size: int,
    reps: int,
) -> list[dict]:
    """Run one mode (lexical or semantic) and return per-query results."""

    is_semantic = mode == "semantic"

    # ── Index ────────────────────────────────────────────────────────
    if is_semantic:
        print(f"  indexing {len(docs)} docs into opensearch (semantic)...")
        os_index_s = setup_opensearch_semantic(os_host, index, docs)
        print(f"    opensearch index time: {os_index_s:.3f}s")
    else:
        print(f"  indexing {len(docs)} docs into opensearch (lexical)...")
        os_index_s = setup_opensearch_lexical(os_host, index, docs)
        print(f"    opensearch index time: {os_index_s:.3f}s")

    print(f"  indexing {len(docs)} docs into kosha...")
    kosha_index_s = setup_kosha(kosha_host, index, docs)
    print(f"    kosha index time: {kosha_index_s:.3f}s")

    # ── Search ───────────────────────────────────────────────────────
    per_query_results = []
    for q in queries:
        qid = q["id"]
        qtext = q.get("text", "")

        if is_semantic:
            qvec = q.get("vector")
            if not qvec:
                print(f"  WARNING: query {qid} has no vector, skipping")
                continue
            # Warmup
            os_hits, _ = search_opensearch_semantic(os_host, index, qvec, size)
            kosha_hits, _ = search_kosha_semantic(kosha_host, index, qvec, size)

            os_latencies, kosha_latencies = [], []
            for _ in range(reps):
                _, t = search_opensearch_semantic(os_host, index, qvec, size)
                os_latencies.append(t)
            for _ in range(reps):
                _, t = search_kosha_semantic(kosha_host, index, qvec, size)
                kosha_latencies.append(t)
        else:
            if not qtext:
                print(f"  WARNING: query {qid} has no text, skipping")
                continue
            os_hits, _ = search_opensearch_lexical(os_host, index, qtext, size)
            kosha_hits, _ = search_kosha_lexical(kosha_host, index, qtext, size)

            os_latencies, kosha_latencies = [], []
            for _ in range(reps):
                _, t = search_opensearch_lexical(os_host, index, qtext, size)
                os_latencies.append(t)
            for _ in range(reps):
                _, t = search_kosha_lexical(kosha_host, index, qtext, size)
                kosha_latencies.append(t)

        os_ids = [h[0] for h in os_hits]
        kosha_ids = [h[0] for h in kosha_hits]

        result = {
            "query_id": qid,
            "query_text": qtext,
            "category": q.get("category"),
            "opensearch": {
                "latency": latency_stats(os_latencies),
                "top_hits": [{"id": i, "score": round(s, 4)} for i, s in os_hits],
                "total_hits": len(os_hits),
            },
            "kosha": {
                "latency": latency_stats(kosha_latencies),
                "top_hits": [{"id": i, "score": round(s, 4)} for i, s in kosha_hits],
                "total_hits": len(kosha_hits),
            },
            "correctness": {
                "top1_match": (
                    (os_ids[0] if os_ids else None)
                    == (kosha_ids[0] if kosha_ids else None)
                ),
                "jaccard_topk": round(jaccard(os_ids, kosha_ids), 4),
                "kendall_tau_overlap": rank_correlation(os_ids, kosha_ids),
                "os_result_count": len(os_ids),
                "kosha_result_count": len(kosha_ids),
            },
        }
        per_query_results.append(result)
        print(
            f"  {qid:>4} {qtext or '(vector)':28} "
            f"os p50={result['opensearch']['latency']['p50_ms']:.2f}ms "
            f"kosha p50={result['kosha']['latency']['p50_ms']:.2f}ms "
            f"jaccard={result['correctness']['jaccard_topk']:.2f} "
            f"top1={result['correctness']['top1_match']}"
        )

    return per_query_results


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--corpus", required=True, type=Path)
    parser.add_argument("--queries", required=True, type=Path)
    parser.add_argument("--os-host", default="http://localhost:9250")
    parser.add_argument("--kosha-host", default="http://localhost:8099")
    parser.add_argument("--index", default="benchmark")
    parser.add_argument("--size", type=int, default=10, help="top-K results per query")
    parser.add_argument(
        "--reps", type=int, default=20, help="timed repetitions per query"
    )
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument(
        "--mode",
        choices=["lexical", "semantic", "both"],
        default="lexical",
        help="search mode to benchmark",
    )
    parser.add_argument(
        "--skip-setup", action="store_true", help="reuse already-indexed corpus"
    )
    args = parser.parse_args()

    docs = load_corpus(args.corpus)
    queries = load_queries(args.queries)

    modes = ["lexical", "semantic"] if args.mode == "both" else [args.mode]

    output = {
        "corpus_size": len(docs),
        "num_queries": len(queries),
        "reps_per_query": args.reps,
        "top_k": args.size,
        "modes": {},
    }

    for mode in modes:
        print(f"\n── {mode.upper()} ──")
        print(f"  corpus: {len(docs)} documents, {len(queries)} queries, {args.reps} reps/query")

        per_query = run_mode(
            mode=mode,
            docs=docs,
            queries=queries,
            os_host=args.os_host,
            kosha_host=args.kosha_host,
            index=args.index,
            size=args.size,
            reps=args.reps,
        )

        os_lats = sum((q["opensearch"]["latency"]["mean_ms"] for q in per_query), 0)
        kosha_lats = sum((q["kosha"]["latency"]["mean_ms"] for q in per_query), 0)
        avg_jaccard = (
            sum(q["correctness"]["jaccard_topk"] for q in per_query) / len(per_query)
            if per_query
            else 0
        )
        top1_matches = sum(
            1 for q in per_query if q["correctness"]["top1_match"]
        )

        results_summary = {
            "queries": per_query,
            "summary": {
                "avg_opensearch_mean_ms": round(os_lats / len(per_query), 3) if per_query else 0,
                "avg_kosha_mean_ms": round(kosha_lats / len(per_query), 3) if per_query else 0,
                "avg_jaccard_topk": round(avg_jaccard, 4),
                "top1_match_rate": round(top1_matches / len(per_query), 4) if per_query else 0,
            },
        }
        output["modes"][mode] = results_summary

    args.out.write_text(json.dumps(output, indent=2))
    print(f"\nwrote results to {args.out}")


if __name__ == "__main__":
    main()
