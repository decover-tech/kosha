#!/usr/bin/env python3
"""Import a frozen eval set (scripts/eval/build_evalset.py) as harness queries + qrels.

    python3 scripts/relevance/evalset_to_fixture.py cases.jsonl.gz OUT_DIR [--top-k 10]

The eval sets built from a Sage matter export (branch feat/eval) carry
exact expectations computed from the records. Two kinds map onto ranked
relevance:

  * kNN cases (`request.query.knn.<field>.vector`, `expect.neighbours`):
    the first --top-k exact neighbours become grade-1 judgments for a
    vector query — the qrels-side view of filtered/unfiltered recall;
  * word / phrase cases (a `match` or `match_phrase` clause anywhere in the
    query, `expect.ids`): every matching chunk becomes grade 1 for a text
    query. Boolean match sets carry no ranking, so recall@100 is the
    meaningful number for these; nDCG@10 only says whether the top ten
    match.

Every other case (aggregations, ranges, collapse...) is skipped. Filters in
the requests (matter scoping, format terms) are OpenSearch DSL and are
dropped unless --keep-filters; export a single matter's corpus so that
dropping the matter filter changes nothing.

Writes OUT_DIR/queries.jsonl and OUT_DIR/qrels.txt; put the matter's
corpus.jsonl next to them (format: scripts/relevance/README.md).
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Tuple


def read_cases(path: Path) -> List[dict]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def find_clause(node, names: Tuple[str, ...]) -> Optional[Tuple[str, dict]]:
    """Depth-first search for the first {name: body} with name in `names`."""
    if isinstance(node, dict):
        for k, v in node.items():
            if k in names and isinstance(v, dict):
                return k, v
        for v in node.values():
            found = find_clause(v, names)
            if found:
                return found
    elif isinstance(node, list):
        for v in node:
            found = find_clause(v, names)
            if found:
                return found
    return None


def match_text(query: dict) -> Optional[str]:
    found = find_clause(query, ("match", "match_phrase"))
    if not found:
        return None
    _, body = found
    for value in body.values():
        if isinstance(value, str):
            return value
        if isinstance(value, dict) and isinstance(value.get("query"), str):
            return value["query"]
    return None


def convert(
    cases: Iterable[dict], top_k: int = 10, keep_filters: bool = False
) -> Tuple[List[dict], List[str], int]:
    """(queries, qrels lines, skipped case count)."""
    queries, qrels, skipped = [], [], 0
    for case in cases:
        qid = str(case.get("id"))
        request = case.get("request") or {}
        query = request.get("query") or {}
        expect = case.get("expect") or {}
        knn = query.get("knn") if isinstance(query, dict) else None
        if isinstance(knn, dict) and expect.get("neighbours"):
            body = next(iter(knn.values()))
            q = {"qid": qid, "vector": body["vector"]}
            if keep_filters and body.get("filter"):
                q["filter"] = body["filter"]
            docs = [str(n[0]) for n in expect["neighbours"][:top_k]]
        elif expect.get("ids") and match_text(query):
            q = {"qid": qid, "text": match_text(query)}
            if keep_filters:
                found = find_clause(query, ("bool",))
                flt = found[1].get("filter") if found else None
                if flt:
                    q["filter"] = flt[0] if isinstance(flt, list) and len(flt) == 1 else flt
            docs = [str(d) for d in expect["ids"]]
        else:
            skipped += 1
            continue
        queries.append(q)
        qrels += [f"{qid} 0 {d} 1" for d in dict.fromkeys(docs)]
    return queries, qrels, skipped


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("cases", type=Path, help="cases.jsonl or cases.jsonl.gz")
    ap.add_argument("out_dir", type=Path)
    ap.add_argument("--top-k", type=int, default=10, help="kNN neighbours judged relevant")
    ap.add_argument("--keep-filters", action="store_true")
    args = ap.parse_args(argv)
    queries, qrels, skipped = convert(read_cases(args.cases), args.top_k, args.keep_filters)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "queries.jsonl").write_text(
        "".join(json.dumps(q, separators=(",", ":")) + "\n" for q in queries)
    )
    (args.out_dir / "qrels.txt").write_text("".join(line + "\n" for line in qrels))
    print(f"{len(queries)} queries, {len(qrels)} judgments, {skipped} cases skipped", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
