#!/usr/bin/env python3
"""Unit tests for the single-node Lucene comparison settings.

    python3 scripts/bench/bm25_scale/test_lucene_compare.py
"""

from __future__ import annotations

import ast
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import lucene_compare  # noqa: E402


ROOT = Path(__file__).resolve().parent


def function_source(path: Path, name: str) -> str:
    tree = ast.parse(path.read_text())
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(path.read_text(), node) or ""
    raise AssertionError(f"{name} not found in {path}")


class IndexAndQuery(unittest.TestCase):
    def test_index_is_single_node_and_matches_kosha_bm25(self):
        body = lucene_compare.index_body(8)
        settings = body["settings"]
        self.assertEqual(settings["number_of_shards"], 8)
        self.assertEqual(settings["number_of_replicas"], 0)
        self.assertEqual(settings["refresh_interval"], "-1")
        self.assertEqual(settings["similarity"]["default"], {"type": "BM25", "k1": 1.2, "b": 0.75})
        analyzer = settings["analysis"]["analyzer"]["kosha_standard"]
        self.assertEqual(analyzer["tokenizer"], "whitespace")
        self.assertEqual(
            analyzer["filter"],
            ["lowercase", "strip_edge_punct", "drop_empty"],
        )
        pattern = settings["analysis"]["filter"]["strip_edge_punct"]["pattern"]
        self.assertTrue(pattern.startswith("^"))
        self.assertIn(r"\u0021-\u002F", pattern)
        self.assertNotIn("codec", json.dumps(body))
        self.assertNotIn("translog", json.dumps(body))
        self.assertEqual(body["mappings"]["properties"]["text"]["analyzer"], "kosha_standard")

    def test_replicas_rejected(self):
        with self.assertRaises(ValueError):
            lucene_compare.index_body(1, replicas=1)

    def test_cache_modes(self):
        self.assertEqual(
            lucene_compare.cache_index_settings("off"),
            {"index.requests.cache.enable": False, "index.queries.cache.enabled": False},
        )
        self.assertEqual(
            lucene_compare.cache_index_settings("defaults"),
            {"index.requests.cache.enable": True, "index.queries.cache.enabled": True},
        )
        self.assertEqual(lucene_compare.search_params("off"), {"request_cache": "false"})
        self.assertEqual(lucene_compare.search_params("defaults"), {})

    def test_search_body_is_an_or_match(self):
        ids = lucene_compare.search_body("red shoes", 10, "ids")
        self.assertFalse(ids["_source"])
        self.assertEqual(ids["size"], 10)
        self.assertEqual(ids["track_total_hits"], 10000)
        self.assertEqual(ids["query"]["match"]["text"]["operator"], "or")
        self.assertEqual(ids["query"]["match"]["text"]["query"], "red shoes")
        source = lucene_compare.search_body("red shoes", 10, "source")
        self.assertTrue(source["_source"])

    def test_bulk_uses_corpus_id(self):
        body, n = lucene_compare.to_bulk_ndjson(
            [json.dumps({"id": "abc", "text": "hello world"}) + "\n"],
            "msmarco-10m",
        )
        self.assertEqual(n, 1)
        action, doc = body.strip().split("\n")
        self.assertEqual(json.loads(action)["index"]["_id"], "abc")
        self.assertEqual(json.loads(doc), {"text": "hello world"})


class Container(unittest.TestCase):
    def test_opensearch_image_and_heap(self):
        argv = lucene_compare.docker_argv("opensearch", "/data/lucene/opensearch")
        text = " ".join(argv)
        self.assertIn("opensearchproject/opensearch:3.10.1", argv)
        self.assertIn("OPENSEARCH_JAVA_OPTS=-Xms31g -Xmx31g", argv)
        self.assertIn("DISABLE_SECURITY_PLUGIN=true", argv)
        self.assertIn("discovery.type=single-node", argv)
        self.assertNotIn("xpack.security.enabled=false", text)
        self.assertIn("/usr/share/opensearch/data", text)

    def test_elasticsearch_image_and_security_off(self):
        argv = lucene_compare.docker_argv("elasticsearch", "/data/lucene/elasticsearch")
        text = " ".join(argv)
        self.assertIn("docker.elastic.co/elasticsearch/elasticsearch:9.5.7", argv)
        self.assertIn("ES_JAVA_OPTS=-Xms31g -Xmx31g", argv)
        self.assertIn("xpack.security.enabled=false", argv)
        self.assertNotIn("DISABLE_SECURITY_PLUGIN=true", text)
        self.assertIn("/usr/share/elasticsearch/data", text)

    def test_docker_cmd_is_a_bash_array(self):
        from io import StringIO
        from contextlib import redirect_stdout

        buf = StringIO()
        with redirect_stdout(buf):
            rc = lucene_compare.main(
                ["lucene_compare.py", "docker-cmd", "opensearch", "/data/lucene/opensearch"]
            )
        self.assertEqual(rc, 0)
        text = buf.getvalue()
        self.assertTrue(text.startswith("docker_cmd=("))
        self.assertIn("opensearchproject/opensearch:3.10.1", text)


class MeasurementWiring(unittest.TestCase):
    def test_query_client_uses_kosha_percentile_and_order(self):
        text = (ROOT / "query_bench_opensearch.py").read_text()
        self.assertIn("from query_bench import percentile, query_order", text)
        # The Kosha client's percentile is the one both reports call.
        src = function_source(ROOT / "query_bench.py", "percentile")
        self.assertIn("sorted(data)", src)
        self.assertIn("(len(s) - 1) * (p / 100)", src)

    def test_shell_runs_both_cache_modes_on_the_same_corpus(self):
        text = (ROOT / "vm" / "run_lucene.sh").read_text()
        self.assertIn("s3://decoverai-bench-corpus-cache/msmarco-10m", text)
        self.assertIn("--cache-mode off", text)
        self.assertIn("--warmup", text)
        self.assertIn("--cache-mode defaults", text)
        self.assertIn("docker rm -f kosha", text)
        self.assertLess(text.index("--cache-mode off"), text.index("--cache-mode defaults"))


class Report(unittest.TestCase):
    def test_load_report_labels_list_prices(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "opensearch" / "load.json"
            path.parent.mkdir()
            path.write_text(json.dumps({
                "engine": "opensearch",
                "version_pinned": "3.10.1",
                "image": "opensearchproject/opensearch:3.10.1",
                "heap": "31g",
                "shards": 8,
                "replicas": 0,
                "max_num_segments": 1,
                "refresh_interval": "-1",
                "docs_sent": 10,
                "docs_count": 10,
                "bulk_s": 1.5,
                "refresh_s": 0.2,
                "forcemerge_s": 3,
                "store_size_bytes": 1000,
                "data_dir_bytes": 1200,
                "segments_count": 8,
                "node_count": 1,
                "node_version": "3.10.1",
                "concurrent_segment_search": {"status": 200},
            }))
            text = lucene_compare.format_load_reports(Path(tmp))
        self.assertIn("opensearch 3.10.1", text)
        self.assertIn("1,000 bytes", text)
        self.assertIn("force-merge", text)
        self.assertNotIn("p50", text)

    def test_cost_assumptions_are_list_prices(self):
        cost = lucene_compare.COST_ASSUMPTIONS
        self.assertEqual(cost["instance_type"], "m7i.8xlarge")
        self.assertEqual(cost["hourly_usd"], 1.6128)
        self.assertEqual(cost["root_volume_gb"], 1000)
        self.assertEqual(cost["gp3_usd_per_gb_month"], 0.08)
        self.assertIn("Not an invoice", cost["hourly_note"])


if __name__ == "__main__":
    unittest.main()
