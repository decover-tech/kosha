#!/usr/bin/env python3
"""Turn OpenSearch search responses into pseudo-qrels for the relevance harness.

    python3 scripts/relevance/opensearch_to_qrels.py responses.jsonl \
        [--top-k 10] [--id-field FIELD] [--out qrels.txt]

When no human judgments exist, the incumbent engine's ranking is the
reference: each query's top-k OpenSearch hits are written as grade 1 in TREC
format (`qid 0 docid 1`), so nDCG@10 / recall@100 measure agreement with
OpenSearch rather than true relevance.

Input is JSONL, one `{"qid": ..., "response": <OpenSearch _search body>}`
per line, or a single JSON object `{qid: <response>, ...}`. A response's
hits are read from `hits.hits[]` in order; the document id is `_id`, or
`_source.<FIELD>` / `fields.<FIELD>[0]` with `--id-field` when the corpus
exported to corpus.jsonl uses another key. Duplicate ids keep their first
(best) rank. Queries with no hits produce no lines (the harness skips
unjudged queries).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Tuple


def load_responses(text: str) -> List[Tuple[str, dict]]:
    """[(qid, response)] from JSONL or a single {qid: response} object."""
    stripped = text.strip()
    if not stripped:
        return []
    try:
        whole = json.loads(stripped)
    except ValueError:
        whole = None
    if isinstance(whole, dict) and "qid" not in whole:
        return [(str(q), r) for q, r in whole.items()]
    out = []
    for n, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError as e:
            raise ValueError(f"line {n}: {e}") from e
        if not isinstance(row, dict) or "qid" not in row or "response" not in row:
            raise ValueError(f'line {n}: want {{"qid": ..., "response": {{...}}}}')
        out.append((str(row["qid"]), row["response"]))
    return out


def hit_id(hit: dict, id_field: Optional[str]) -> Optional[str]:
    if not id_field:
        v = hit.get("_id")
    else:
        v = (hit.get("_source") or {}).get(id_field)
        if v is None:
            vals = (hit.get("fields") or {}).get(id_field)
            v = vals[0] if isinstance(vals, list) and vals else vals
    return None if v is None else str(v)


def qrels_lines(
    responses: Iterable[Tuple[str, dict]], top_k: int, id_field: Optional[str] = None
) -> Iterator[str]:
    for qid, response in responses:
        if " " in qid:
            raise ValueError(f"qid {qid!r} contains a space; TREC qrels are whitespace separated")
        hits = ((response or {}).get("hits") or {}).get("hits") or []
        seen = set()
        for hit in hits:
            if len(seen) >= top_k:
                break
            doc = hit_id(hit, id_field)
            if doc is None or doc in seen:
                continue
            seen.add(doc)
            yield f"{qid} 0 {doc} 1"


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("responses", type=Path, help="JSONL of {qid, response}, or {qid: response}")
    ap.add_argument("--top-k", type=int, default=10, help="hits per query judged relevant")
    ap.add_argument("--id-field", help="take the doc id from _source.FIELD instead of _id")
    ap.add_argument("--out", type=Path, help="write here instead of stdout")
    args = ap.parse_args(argv)
    if args.top_k < 1:
        ap.error("--top-k must be >= 1")
    lines = list(qrels_lines(load_responses(args.responses.read_text()), args.top_k, args.id_field))
    text = "".join(line + "\n" for line in lines)
    if args.out:
        args.out.write_text(text)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
