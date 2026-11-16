"""Query set for the conformance suite: BM25 and structured filters.

Queries are derived from the corpus actually loaded (via the oracle), so term
choices land in the frequency band they claim to test no matter the seed or
scale. Each query has a stable id; ids, not positions, are what
``expected_failures.json`` refers to.
"""

from __future__ import annotations

import random
from typing import Dict, List

from oracle import Oracle

OWNER_HEAVY = "owner00"


def _df_table(oracle: Oracle) -> List[tuple]:
    df: Dict[str, int] = {}
    for d in oracle.live_docs().values():
        for t in d.terms:
            df[t] = df.get(t, 0) + 1
    return sorted(df.items(), key=lambda kv: (-kv[1], kv[0]))


def build(oracle: Oracle, seed: int) -> List[Dict]:
    rng = random.Random("queries/%d" % seed)
    table = [(t, n) for t, n in _df_table(oracle) if t.startswith("w")]
    n = len(table)
    common = [t for t, _ in table[: max(5, n // 50)]]
    mid = [t for t, _ in table[n // 5 : n // 2]]
    rare = [t for t, c in table if c <= 3]
    pick = lambda pool, k: rng.sample(pool, min(k, len(pool)))  # noqa: E731

    q: List[Dict] = []

    def add(qid: str, body: Dict) -> None:
        q.append({"id": qid, "body": body})

    # ── BM25: single terms by document-frequency band ──
    for i, t in enumerate(pick(common, 3)):
        add("bm25/common/%d" % i, {"query_text": t})
    for i, t in enumerate(pick(mid, 4)):
        add("bm25/mid/%d" % i, {"query_text": t})
    for i, t in enumerate(pick(rare, 4)):
        add("bm25/rare/%d" % i, {"query_text": t})

    # ── BM25: multi-term AND / OR ──
    for i in range(4):
        a, b = pick(mid, 2) if len(mid) >= 2 else (common[0], common[1])
        add("bm25/and2/%d" % i, {"query_text": "%s %s" % (common[i % len(common)], a)})
        add("bm25/or2/%d" % i, {"query_text": "%s %s" % (a, b), "operator": "or"})
    add("bm25/and3", {"query_text": " ".join(common[:3])})
    add("bm25/or3-rare", {"query_text": " ".join(pick(rare, 3)), "operator": "or"})
    # A term no document contains must make an AND query match nothing.
    add("bm25/and-missing-term", {"query_text": "%s zzznotaword" % common[0]})
    add("bm25/or-missing-term", {"query_text": "%s zzznotaword" % mid[0], "operator": "or"})
    add("bm25/no-such-term", {"query_text": "zzznotaword"})

    # ── Tokenizer: punctuation, case, non-ASCII ──
    add("tok/punct-query", {"query_text": "BREACH!!"})
    add("tok/wrapped", {"query_text": "(contract)"})
    add("tok/hyphen-interior", {"query_text": "co-op"})
    add("tok/apostrophe", {"query_text": "Plaintiff's"})
    add("tok/dots", {"query_text": "u.s."})
    add("tok/non-ascii", {"query_text": "CAFÉ"})
    add("tok/unicode-dash", {"query_text": "—"})
    add("tok/only-punct", {"query_text": "..."})

    # ── BM25 parameters ──
    add("bm25/params-k1-2-b0", {"query_text": mid[1], "bm25_params": {"k1": 2.0, "b": 0.0}})
    add("bm25/params-b1", {"query_text": "%s %s" % (common[0], common[1]), "bm25_params": {"k1": 1.2, "b": 1.0}})

    # ── Filters alone (filter-only scoring path) ──
    add("filter/term-keyword", {"filter": {"term": {"status": "open"}}})
    add("filter/term-bool", {"filter": {"term": {"archived": "true"}}})
    add("filter/term-multivalued", {"filter": {"term": {"tags": "privileged"}}})
    add("filter/terms", {"filter": {"terms": {"owner": ["owner01", "owner02", "owner07"]}}})
    add("filter/range-int", {"filter": {"range": {"priority": {"gte": "3", "lt": "10"}}}})
    add("filter/range-int-gt-lte", {"filter": {"range": {"priority": {"gt": "9", "lte": "12"}}}})
    add("filter/range-float", {"filter": {"range": {"amount": {"gte": "100.5", "lte": "999.99"}}}})
    add("filter/range-date", {"filter": {"range": {"filed": {"gte": "2021-06-01", "lt": "2023-01-01"}}}})
    add("filter/match-all", {"filter": {"match_all": {}}})
    add("filter/term-no-match", {"filter": {"term": {"status": "nonexistent"}}})
    # ES-correct: a term filter on a numeric field matches numerically.
    add("filter/term-on-integer", {"filter": {"term": {"priority": "7"}}})
    add("filter/bool-must", {"filter": {"bool": {"must": [{"term": {"status": "open"}}, {"range": {"priority": {"gte": "6"}}}]}}})
    add("filter/bool-must-not", {"filter": {"bool": {"must_not": [{"term": {"owner": OWNER_HEAVY}}]}}})
    add(
        "filter/bool-should-msm2",
        {
            "filter": {
                "bool": {
                    "should": [
                        {"term": {"tags": "urgent"}},
                        {"term": {"tags": "sealed"}},
                        {"term": {"archived": "true"}},
                    ],
                    "minimum_should_match": 2,
                }
            }
        },
    )
    add(
        "filter/bool-nested",
        {
            "filter": {
                "bool": {
                    "must": [{"range": {"filed": {"gte": "2020-01-01"}}}],
                    "must_not": [{"terms": {"status": ["closed", "withdrawn"]}}],
                    "should": [{"term": {"owner": "owner03"}}, {"bool": {"must": [{"term": {"tags": "draft"}}]}}],
                }
            }
        },
    )

    # ── BM25 + filter ──
    add("mixed/common+term", {"query_text": common[0], "filter": {"term": {"status": "review"}}})
    add("mixed/and+range", {"query_text": "%s %s" % (common[0], common[2]), "filter": {"range": {"amount": {"lt": "5000"}}}})
    add("mixed/rare+must-not", {"query_text": rare[0] if rare else mid[0], "filter": {"bool": {"must_not": [{"term": {"archived": "true"}}]}}})

    # ── Sorting ──
    add("sort/keyword-asc", {"query_text": common[0], "sort": [{"owner": {"order": "asc"}}]})
    add("sort/id-desc", {"filter": {"term": {"status": "pending"}}, "sort": [{"_id": {"order": "desc"}}]})
    add("sort/date-desc", {"query_text": common[1], "sort": [{"filed": {"order": "desc"}}]})
    # ES-correct: numeric fields sort numerically (Kosha compares the strings).
    add("sort/integer-asc", {"filter": {"term": {"status": "open"}}, "sort": [{"priority": {"order": "asc"}}]})
    add("sort/float-desc", {"query_text": common[0], "sort": [{"amount": {"order": "desc"}}]})
    add("sort/score-asc", {"query_text": mid[2], "sort": [{"_score": {"order": "asc"}}]})

    # ── Pagination (deep pages; walks are generated by the runner) ──
    add("page/from-20", {"query_text": common[0], "from": 20, "max_results": 15})
    add("page/past-end", {"query_text": mid[0], "from": 100000, "max_results": 10})
    add("page/big", {"filter": {"term": {"owner": OWNER_HEAVY}}, "max_results": 500})

    return q


# Queries whose full result list is walked page by page; the concatenation of
# every page must equal the oracle's full ordering, with no gaps or repeats.
def walks(oracle: Oracle, seed: int) -> List[Dict]:
    table = [(t, n) for t, n in _df_table(oracle) if t.startswith("w")]
    common = table[0][0]
    return [
        {"id": "walk/from-score", "mode": "from", "page": 7, "body": {"query_text": common}},
        {"id": "walk/after-id", "mode": "search_after", "page": 9, "body": {"query_text": common, "sort": [{"_id": {"order": "asc"}}]}},
        {
            "id": "walk/after-keyword-id",
            "mode": "search_after",
            "page": 11,
            "body": {"filter": {"terms": {"status": ["open", "review"]}}, "sort": [{"owner": {"order": "asc"}}, {"_id": {"order": "asc"}}]},
        },
    ]
