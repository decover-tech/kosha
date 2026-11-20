#!/usr/bin/env python3
"""Tests for opensearch_to_qrels.py, evalset_to_fixture.py and the synthetic
fixture generator.

    python3 scripts/relevance/test_converters.py
    python3 -m pytest scripts/relevance
"""

import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE / "fixtures" / "synthetic"))

import evalset_to_fixture  # noqa: E402
import generate  # noqa: E402
import opensearch_to_qrels  # noqa: E402

# A trimmed real-shaped OpenSearch `_search` response.
CANNED_RESPONSE = {
    "took": 7,
    "timed_out": False,
    "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
    "hits": {
        "total": {"value": 1234, "relation": "eq"},
        "max_score": 12.5,
        "hits": [
            {"_index": "paragraphs", "_id": "p-17", "_score": 12.5, "_source": {"chunkId": "c17"}},
            {"_index": "paragraphs", "_id": "p-03", "_score": 11.0, "_source": {"chunkId": "c03"}},
            {"_index": "paragraphs", "_id": "p-17", "_score": 10.0, "_source": {"chunkId": "c17"}},
            {"_index": "paragraphs", "_id": "p-42", "_score": 9.1, "fields": {"chunkId": ["c42"]}},
            {"_index": "paragraphs", "_id": "p-08", "_score": 8.0, "_source": {"chunkId": "c08"}},
        ],
    },
}


class OpenSearchToQrelsTest(unittest.TestCase):
    def test_top_k_hits_become_grade_one_in_rank_order(self):
        rows = [("q1", CANNED_RESPONSE), ("q2", {"hits": {"hits": []}})]
        got = list(opensearch_to_qrels.qrels_lines(rows, top_k=3))
        self.assertEqual(got, ["q1 0 p-17 1", "q1 0 p-03 1", "q1 0 p-42 1"])

    def test_id_field_reads_source_then_fields(self):
        got = list(opensearch_to_qrels.qrels_lines([("q", CANNED_RESPONSE)], 10, "chunkId"))
        self.assertEqual(got, ["q 0 c17 1", "q 0 c03 1", "q 0 c42 1", "q 0 c08 1"])

    def test_jsonl_and_object_inputs_and_cli(self):
        jsonl = "\n".join(
            json.dumps({"qid": q, "response": CANNED_RESPONSE}) for q in ("a", "b")
        )
        self.assertEqual([q for q, _ in opensearch_to_qrels.load_responses(jsonl)], ["a", "b"])
        obj = json.dumps({"7": CANNED_RESPONSE})
        self.assertEqual([q for q, _ in opensearch_to_qrels.load_responses(obj)], ["7"])
        with self.assertRaises(ValueError):
            opensearch_to_qrels.load_responses('{"qid": "x"}\n{"nope": 1}')
        with self.assertRaises(ValueError):
            list(opensearch_to_qrels.qrels_lines([("has space", CANNED_RESPONSE)], 1))
        with tempfile.TemporaryDirectory() as tmp:
            src, out = Path(tmp) / "r.jsonl", Path(tmp) / "qrels.txt"
            src.write_text(jsonl + "\n")
            self.assertEqual(
                opensearch_to_qrels.main([str(src), "--top-k", "2", "--out", str(out)]), 0
            )
            self.assertEqual(
                out.read_text(), "a 0 p-17 1\na 0 p-03 1\nb 0 p-17 1\nb 0 p-03 1\n"
            )


# Shapes as scripts/eval/build_evalset.py (branch feat/eval) writes them.
CASES = [
    {
        "id": "knn-001",
        "area": "knn",
        "tier": "quick",
        "request": {
            "size": 10,
            "_source": False,
            "query": {
                "knn": {
                    "contentEmbedding": {
                        "vector": [0.1, 0.2],
                        "k": 30,
                        "filter": {"term": {"matterId": "m1"}},
                    }
                }
            },
        },
        "expect": {"neighbours": [["c1", 0.99], ["c2", 0.98], ["c3", 0.5]], "query_chunk": "c1"},
    },
    {
        "id": "word-002",
        "area": "word",
        "tier": "full",
        "request": {
            "query": {
                "bool": {
                    "must": [{"match": {"content": {"query": "indemnity", "operator": "and"}}}],
                    "filter": [{"term": {"matterId": "m1"}}],
                }
            }
        },
        "expect": {"total": 2, "ids": ["c9", "c4", "c9"]},
    },
    {"id": "agg-003", "area": "tag", "request": {"aggs": {}}, "expect": {"buckets": {}}},
]


class EvalsetToFixtureTest(unittest.TestCase):
    def test_converts_knn_and_match_cases(self):
        queries, qrels, skipped = evalset_to_fixture.convert(CASES, top_k=2)
        self.assertEqual(skipped, 1)
        self.assertEqual(
            queries,
            [{"qid": "knn-001", "vector": [0.1, 0.2]}, {"qid": "word-002", "text": "indemnity"}],
        )
        self.assertEqual(
            qrels, ["knn-001 0 c1 1", "knn-001 0 c2 1", "word-002 0 c9 1", "word-002 0 c4 1"]
        )

    def test_keep_filters_and_cli_on_gzip(self):
        queries, _, _ = evalset_to_fixture.convert(CASES, keep_filters=True)
        self.assertEqual(queries[0]["filter"], {"term": {"matterId": "m1"}})
        self.assertEqual(queries[1]["filter"], {"term": {"matterId": "m1"}})
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "cases.jsonl.gz"
            with gzip.open(src, "wt") as fh:
                fh.writelines(json.dumps(c) + "\n" for c in CASES)
            out = Path(tmp) / "fx"
            self.assertEqual(evalset_to_fixture.main([str(src), str(out)]), 0)
            self.assertEqual(len((out / "queries.jsonl").read_text().splitlines()), 2)
            self.assertIn("knn-001 0 c3 1", (out / "qrels.txt").read_text())


class SyntheticFixtureTest(unittest.TestCase):
    def test_committed_fixture_matches_the_generator(self):
        fixture = HERE / "fixtures" / "synthetic"
        with tempfile.TemporaryDirectory() as tmp:
            generate.write(Path(tmp))
            for name in ("corpus.jsonl", "queries.jsonl", "qrels.txt", "fixture.json"):
                self.assertEqual(
                    (Path(tmp) / name).read_bytes(),
                    (fixture / name).read_bytes(),
                    f"{name} is stale: rerun fixtures/synthetic/generate.py",
                )

    def test_fixture_is_small_and_graded(self):
        fixture = HERE / "fixtures" / "synthetic"
        size = sum(p.stat().st_size for p in fixture.iterdir() if p.is_file())
        self.assertLess(size, 2 * 1024 * 1024)
        grades = {line.split()[3] for line in (fixture / "qrels.txt").read_text().splitlines()}
        self.assertEqual(grades, {"1", "2"})


if __name__ == "__main__":
    unittest.main()
