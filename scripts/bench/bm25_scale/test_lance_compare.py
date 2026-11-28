#!/usr/bin/env python3
"""Unit tests for the Lance-over-Iceberg comparison settings. No pylance
needed; the end-to-end path runs on the bench VM.

    python3 scripts/bench/bm25_scale/test_lance_compare.py
"""

from __future__ import annotations

import json
import re
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import lance_compare  # noqa: E402
import lucene_compare  # noqa: E402


def lucene_trim(word: str) -> str:
    """What lucene_compare's pattern_replace filter does to one token."""
    return re.sub(lucene_compare.EDGE_PUNCT_PATTERN, "", word)


class Analyzer(unittest.TestCase):
    CASES = [
        ("Hello, world!", "Hello world"),
        ("hello,world", "hello,world"),
        ('"quoted" (paren) [x]', "quoted paren x"),
        ("--- ... !!!", ""),
        ("Café's  über-cool\tthing.", "Café's über-cool thing"),
        ("a b", "a b"),
        ("", ""),
    ]

    def test_matches_kosha_standard_v1_edge_trim(self):
        for raw, want in self.CASES:
            self.assertEqual(lance_compare.normalize(raw), want, raw)

    def test_matches_lucene_parity_analyzer_tokens(self):
        for raw, _ in self.CASES:
            lucene = [t for t in (lucene_trim(w) for w in raw.split()) if t]
            self.assertEqual(lance_compare.normalize(raw).split(), lucene, raw)

    def test_index_params_do_not_stem_or_fold(self):
        p = lance_compare.INDEX_PARAMS
        self.assertEqual(p["base_tokenizer"], "whitespace")
        self.assertTrue(p["lower_case"])
        self.assertFalse(p["stem"])
        self.assertFalse(p["remove_stop_words"])
        self.assertFalse(p["ascii_folding"])
        self.assertIsNone(p["max_token_length"])


class Wiring(unittest.TestCase):
    def test_projection(self):
        self.assertEqual(lance_compare.projected_columns("ids"), ["id", "_score"])
        self.assertIn("text", lance_compare.projected_columns("source"))
        with self.assertRaises(ValueError):
            lance_compare.projected_columns("all")

    def test_table_location_is_under_root(self):
        self.assertEqual(
            lance_compare.table_location("s3://b/lance/"),
            "s3://b/lance/bench/kosha_bench/msmarco_10m",
        )

    def test_catalog_container_is_pinned_and_uses_s3_fileio(self):
        argv = lance_compare.catalog_docker_argv("us-east-1", "s3://b/lance")
        self.assertEqual(argv[-1], "apache/iceberg-rest-fixture:1.10.1")
        self.assertIn("CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO", argv)
        self.assertIn("AWS_REGION=us-east-1", argv)
        self.assertIn("host", argv)

    def test_pip_requirements_are_pinned(self):
        for pkg in ("pylance==", "lance-namespace==", "lance-namespace-impls=="):
            self.assertTrue(any(r.startswith(pkg) for r in lance_compare.PIP_REQUIREMENTS), pkg)

    def test_load_reports_are_split_by_engine(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "lance").mkdir()
            (root / "lance" / "load.json").write_text(json.dumps({
                "engine": "lance", "pylance_version": "14.0.3",
                "catalog_image": lance_compare.CATALOG_IMAGE,
                "table_id": lance_compare.TABLE_ID, "location": "s3://b/x",
                "rows_written": 10, "rows_count": 10, "fragments": 1,
                "write_s": 1.0, "index_s": 2.0, "index_params": {}, "s3_bytes": 2048,
            }))
            (root / "opensearch").mkdir()
            (root / "opensearch" / "load.json").write_text(json.dumps({"engine": "opensearch"}))
            lance_md = lance_compare.format_load_reports(root)
            lucene_md = lucene_compare.format_load_reports(root)
        self.assertIn("2,048 bytes", lance_md)
        self.assertNotIn("opensearch", lance_md)
        self.assertNotIn("lance/load.json", lucene_md)
        self.assertIn("opensearch/load.json", lucene_md)


if __name__ == "__main__":
    unittest.main()
