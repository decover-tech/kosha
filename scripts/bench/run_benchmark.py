#!/usr/bin/env python3
"""Microbenchmark: BM25 search on OpenSearch vs Kosha over the same corpus.

Indexes an identical page-level document corpus into both engines with a
matched analyzer (whitespace tokenize + lowercase, no stemming/stopwords —
this mirrors Kosha's Phase-1 tokenizer exactly in crates/kosha-segment, see
`tokenize()`), runs the same query set against both, and reports:

  - Performance: sequential single-client query latency (p50/p95/mean),
    measured client-side over N repetitions per query (first rep discarded
    as warmup).
  - Correctness: top-K result-set agreement (Jaccard overlap, top-1 match
    rate, rank correlation on the overlapping subset) between the two
    engines' rankings for each query.
  - Indexing time for the full corpus into each engine.

Writes a single JSON results file for downstream reporting.

Usage:
    python3 scripts/bench/run_benchmark.py \\
        --corpus /path/to/corpus.jsonl \\
        --queries scripts/bench/queries.json \\
        --os-host http://localhost:9250 \\
        --kosha-host http://localhost:8099 \\
        --index ruffino-archer \\
        --reps 20 \\
        --out results.json
"""

import argparse
import json
import statistics
import time
from pathlib import Path

import requests

ANALYZER_SETTINGS = {
    # Kosha's tokenizer (crates/kosha-segment/src/lib.rs `tokenize()`) is NOT a
    # plain whitespace split: it splits on whitespace, lowercases, and then
    # trims leading/trailing ASCII punctuation from each token (so
    # "negligence:" indexes as "negligence", matching a bare "negligence"
    # query). A stock ES "whitespace" analyzer does NOT strip that
    # punctuation, which silently desyncs tokens between the two engines
    # (e.g. "negligence:" never matches "negligence"). The pattern_replace
    # filter below reproduces the edge-trim exactly so both engines analyze
    # text identically and the correctness comparison isolates BM25/ranking
    # behavior rather than tokenizer drift.
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
                "drop_empty": {
                    "type": "length",
                    "min": 1,
                },
            },
            "analyzer": {
                "kosha_match": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "filter": ["lowercase", "strip_edge_punct", "drop_empty"],
                }
            }
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


# ── OpenSearch ───────────────────────────────────────────────────────────────

def setup_opensearch(host: str, index: str, docs: list[dict]) -> float:
    requests.delete(f"{host}/{index}")
    resp = requests.put(f"{host}/{index}", json=ANALYZER_SETTINGS)
    resp.raise_for_status()

    start = time.monotonic()
    lines = []
    for doc in docs:
        lines.append(json.dumps({"index": {"_index": index, "_id": doc["id"]}}))
        lines.append(json.dumps({k: v for k, v in doc.items() if k != "id"}))
    bulk_body = "\n".join(lines) + "\n"
    resp = requests.post(
        f"{host}/_bulk",
        data=bulk_body,
        headers={"content-type": "application/x-ndjson"},
        timeout=120,
    )
    resp.raise_for_status()
    if resp.json().get("errors"):
        errored = [i for i in resp.json()["items"] if i["index"].get("status", 200) >= 300]
        raise RuntimeError(f"opensearch bulk index reported {len(errored)} errors: {errored[:3]}")

    requests.post(f"{host}/{index}/_refresh").raise_for_status()
    return time.monotonic() - start


def search_opensearch(host: str, index: str, query: str, size: int) -> tuple[list[tuple[str, float]], float]:
    body = {"query": {"match": {"text": query}}, "size": size}
    start = time.monotonic()
    resp = requests.post(f"{host}/{index}/_search", json=body)
    elapsed_ms = (time.monotonic() - start) * 1000
    resp.raise_for_status()
    hits = resp.json()["hits"]["hits"]
    return [(h["_id"], h["_score"]) for h in hits], elapsed_ms


# ── Kosha ────────────────────────────────────────────────────────────────────

def setup_kosha(host: str, namespace: str, docs: list[dict]) -> float:
    start = time.monotonic()
    body = {
        "namespace": namespace,
        "documents": [
            {
                "id": doc["id"],
                "fields": [
                    {"name": "text", "field_type": "Text", "value": doc["text"]},
                    {"name": "source_file", "field_type": "Keyword", "value": doc["source_file"]},
                    {"name": "doc_type", "field_type": "Keyword", "value": doc["doc_type"]},
                ],
            }
            for doc in docs
        ],
    }
    resp = requests.post(f"{host}/index", json=body, timeout=120)
    resp.raise_for_status()

    resp = requests.post(f"{host}/flush", json={"namespace": namespace})
    resp.raise_for_status()
    return time.monotonic() - start


def search_kosha(host: str, namespace: str, query: str, size: int) -> tuple[list[tuple[str, float]], float]:
    start = time.monotonic()
    resp = requests.get(
        f"{host}/search",
        params={"ns": namespace, "q": query, "max_results": size},
    )
    elapsed_ms = (time.monotonic() - start) * 1000
    resp.raise_for_status()
    results = resp.json()["results"]
    return [(r["doc_id"], r["score"]) for r in results], elapsed_ms


# ── Latency stats ────────────────────────────────────────────────────────────

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


# ── Correctness ──────────────────────────────────────────────────────────────

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


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--corpus", required=True, type=Path)
    parser.add_argument("--queries", required=True, type=Path)
    parser.add_argument("--os-host", default="http://localhost:9250")
    parser.add_argument("--kosha-host", default="http://localhost:8099")
    parser.add_argument("--index", default="ruffino-archer")
    parser.add_argument("--size", type=int, default=10, help="top-K results per query")
    parser.add_argument("--reps", type=int, default=20, help="timed repetitions per query (after 1 warmup)")
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--skip-setup", action="store_true", help="reuse already-indexed corpus")
    args = parser.parse_args()

    docs = load_corpus(args.corpus)
    queries = load_queries(args.queries)
    print(f"corpus: {len(docs)} documents, {len(queries)} queries, {args.reps} reps/query")

    os_index_s = kosha_index_s = 0.0
    if not args.skip_setup:
        print("indexing into opensearch...")
        os_index_s = setup_opensearch(args.os_host, args.index, docs)
        print(f"  opensearch index time: {os_index_s:.3f}s")
        print("indexing into kosha...")
        kosha_index_s = setup_kosha(args.kosha_host, args.index, docs)
        print(f"  kosha index time: {kosha_index_s:.3f}s")

    per_query_results = []
    for q in queries:
        qid, qtext = q["id"], q["text"]

        # Warmup (untimed).
        os_hits, _ = search_opensearch(args.os_host, args.index, qtext, args.size)
        kosha_hits, _ = search_kosha(args.kosha_host, args.index, qtext, args.size)

        os_latencies, kosha_latencies = [], []
        for _ in range(args.reps):
            _, t = search_opensearch(args.os_host, args.index, qtext, args.size)
            os_latencies.append(t)
        for _ in range(args.reps):
            _, t = search_kosha(args.kosha_host, args.index, qtext, args.size)
            kosha_latencies.append(t)

        os_ids = [h[0] for h in os_hits]
        kosha_ids = [h[0] for h in kosha_hits]

        result = {
            "query_id": qid,
            "query_text": qtext,
            "category": q.get("category"),
            "opensearch": {
                "latency": latency_stats(os_latencies),
                "top_hits": [{"id": i, "score": s} for i, s in os_hits],
                "total_hits": len(os_hits),
            },
            "kosha": {
                "latency": latency_stats(kosha_latencies),
                "top_hits": [{"id": i, "score": s} for i, s in kosha_hits],
                "total_hits": len(kosha_hits),
            },
            "correctness": {
                "top1_match": (os_ids[0] if os_ids else None) == (kosha_ids[0] if kosha_ids else None),
                "jaccard_topk": round(jaccard(os_ids, kosha_ids), 4),
                "kendall_tau_overlap": rank_correlation(os_ids, kosha_ids),
                "os_result_count": len(os_ids),
                "kosha_result_count": len(kosha_ids),
            },
        }
        per_query_results.append(result)
        print(
            f"  {qid:>4} {qtext!r:30} "
            f"os p50={result['opensearch']['latency']['p50_ms']:.2f}ms "
            f"kosha p50={result['kosha']['latency']['p50_ms']:.2f}ms "
            f"jaccard={result['correctness']['jaccard_topk']:.2f} "
            f"top1_match={result['correctness']['top1_match']}"
        )

    output = {
        "corpus_size": len(docs),
        "num_queries": len(queries),
        "reps_per_query": args.reps,
        "top_k": args.size,
        "indexing": {
            "opensearch_seconds": round(os_index_s, 3),
            "kosha_seconds": round(kosha_index_s, 3),
        },
        "queries": per_query_results,
    }
    args.out.write_text(json.dumps(output, indent=2))
    print(f"\nwrote results to {args.out}")


if __name__ == "__main__":
    main()
