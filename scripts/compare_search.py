#!/usr/bin/env python3
"""Compare BM25 search results between OpenSearch/Elasticsearch and Kosha.

Indexes the same small corpus into both engines with an equivalent minimal
schema (single "title" text field), runs the same query against each, and
prints results side by side (doc id + score) for a quick sanity comparison.

This is a standalone dev tool, not the real Sage integration (Epic 11) — it
talks to each engine's native API directly, not through Sage.

Usage:
    python3 scripts/compare_search.py --setup --query "quick fox"
    python3 scripts/compare_search.py --query "dog"          # reuse existing index
    python3 scripts/compare_search.py --setup                # just (re)index

Defaults assume the local dev stack from docs/local-development.md:
    OpenSearch: http://localhost:9200  (Tilt port-forward)
    Kosha:      http://localhost:8081  (Tilt port-forward, host 8080 taken)
"""

import argparse
import json
import sys

import requests

SAMPLE_DOCS = [
    ("d1", "quick brown fox"),
    ("d2", "lazy dog"),
    ("d3", "quick rabbit"),
    ("d4", "the dog barked at the fox"),
]


def setup_opensearch(host: str, index: str, docs: list[tuple[str, str]]) -> None:
    requests.delete(f"{host}/{index}")
    mapping = {"mappings": {"properties": {"title": {"type": "text"}}}}
    resp = requests.put(f"{host}/{index}", json=mapping)
    resp.raise_for_status()

    lines = []
    for doc_id, text in docs:
        lines.append(json.dumps({"index": {"_index": index, "_id": doc_id}}))
        lines.append(json.dumps({"title": text}))
    bulk_body = "\n".join(lines) + "\n"
    resp = requests.post(
        f"{host}/_bulk",
        data=bulk_body,
        headers={"content-type": "application/x-ndjson"},
    )
    resp.raise_for_status()
    if resp.json().get("errors"):
        print(f"warning: opensearch bulk index reported errors: {resp.json()}", file=sys.stderr)

    requests.post(f"{host}/{index}/_refresh").raise_for_status()


def setup_kosha(host: str, namespace: str, docs: list[tuple[str, str]]) -> None:
    body = {
        "namespace": namespace,
        "documents": [
            {"id": doc_id, "fields": [{"name": "title", "text": text}]}
            for doc_id, text in docs
        ],
    }
    resp = requests.post(f"{host}/index", json=body)
    resp.raise_for_status()

    resp = requests.post(f"{host}/flush", json={"namespace": namespace})
    resp.raise_for_status()


def search_opensearch(host: str, index: str, query: str, size: int) -> list[tuple[str, float]]:
    body = {"query": {"match": {"title": query}}, "size": size}
    resp = requests.post(f"{host}/{index}/_search", json=body)
    resp.raise_for_status()
    hits = resp.json()["hits"]["hits"]
    return [(h["_id"], h["_score"]) for h in hits]


def search_kosha(host: str, namespace: str, query: str, size: int) -> list[tuple[str, float]]:
    resp = requests.get(
        f"{host}/search",
        params={"ns": namespace, "q": query, "max_results": size},
    )
    resp.raise_for_status()
    results = resp.json()["results"]
    return [(r["doc_id"], r["score"]) for r in results]


def print_comparison(query: str, os_hits: list[tuple[str, float]], kosha_hits: list[tuple[str, float]]) -> None:
    rows = max(len(os_hits), len(kosha_hits))
    print(f"\nQuery: {query!r}")
    print(f"{'rank':<5}{'opensearch (id, score)':<32}{'kosha (id, score)':<32}")
    for i in range(rows):
        os_cell = f"{os_hits[i][0]} ({os_hits[i][1]:.4f})" if i < len(os_hits) else "-"
        kosha_cell = f"{kosha_hits[i][0]} ({kosha_hits[i][1]:.4f})" if i < len(kosha_hits) else "-"
        print(f"{i + 1:<5}{os_cell:<32}{kosha_cell:<32}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--os-host", default="http://localhost:9200", help="OpenSearch/Elasticsearch base URL")
    parser.add_argument("--kosha-host", default="http://localhost:8081", help="Kosha base URL")
    parser.add_argument("--index", default="compare-test", help="index name / Kosha namespace to use")
    parser.add_argument("--query", help="query text to search for")
    parser.add_argument("--size", type=int, default=10, help="max results per engine")
    parser.add_argument("--setup", action="store_true", help="(re)index the sample corpus into both engines first")
    args = parser.parse_args()

    if not args.setup and not args.query:
        parser.error("nothing to do: pass --setup, --query, or both")

    if args.setup:
        print(f"Indexing {len(SAMPLE_DOCS)} sample docs into opensearch:{args.index} ...")
        setup_opensearch(args.os_host, args.index, SAMPLE_DOCS)
        print(f"Indexing {len(SAMPLE_DOCS)} sample docs into kosha namespace {args.index!r} ...")
        setup_kosha(args.kosha_host, args.index, SAMPLE_DOCS)
        print("Done.")

    if args.query:
        os_hits = search_opensearch(args.os_host, args.index, args.query, args.size)
        kosha_hits = search_kosha(args.kosha_host, args.index, args.query, args.size)
        print_comparison(args.query, os_hits, kosha_hits)


if __name__ == "__main__":
    main()
