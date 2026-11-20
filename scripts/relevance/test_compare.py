#!/usr/bin/env python3
"""Unit tests for compare.py.

    python3 scripts/relevance/test_compare.py
    python3 scripts/relevance/compare.py --self-test
    python3 -m pytest scripts/relevance
"""

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import compare  # noqa: E402
from compare import MARKER, compare as compare_metrics, failures, render_comment  # noqa: E402


def report(**metrics):
    return {
        "schema": 1,
        "kind": "kosha-relevance",
        "fixture": {"name": "synthetic", "docs": 3000, "segments": 3, "judged_queries": 120},
        "config": {"num_candidates": 1},
        "metrics": {k.replace("__", "."): {"value": v, "queries": 120} for k, v in metrics.items()},
    }


BASE = {
    "lexical.ndcg@10": 0.9304,
    "vector.recall@100": 0.6150,
    "filtered_knn.probe@0.1%.recall@10": 1.0,
    "filtered_knn.probe@1%.recall@10": 0.98,
}


def write(tmp, name, metrics):
    p = Path(tmp) / name
    body = {
        "schema": 1,
        "kind": "kosha-relevance",
        "fixture": {"name": "synthetic"},
        "config": {},
        "metrics": {k: {"value": v} for k, v in metrics.items()},
    }
    p.write_text(json.dumps(body))
    return p


def run(argv):
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = compare.main(argv)
    return code, out.getvalue(), err.getvalue()


class CompareTest(unittest.TestCase):
    def test_identical_reports_pass_with_zero_deltas(self):
        rows = compare_metrics(BASE, dict(BASE))
        self.assertEqual(failures(rows), [])
        self.assertTrue(all(r["status"] == "ok" and r["delta"] == 0.0 for r in rows))

    def test_drop_past_one_point_fails_but_within_passes(self):
        head = dict(BASE, **{"lexical.ndcg@10": 0.9304 - 0.0101})
        self.assertEqual([r["name"] for r in failures(compare_metrics(BASE, head))], ["lexical.ndcg@10"])
        head = dict(BASE, **{"lexical.ndcg@10": 0.9304 - 0.0099})
        self.assertEqual(failures(compare_metrics(BASE, head)), [])
        # Exactly the limit is allowed.
        head = dict(BASE, **{"lexical.ndcg@10": 0.9204})
        self.assertEqual(failures(compare_metrics(BASE, head)), [])

    def test_lowest_selectivity_gets_two_points(self):
        head = dict(BASE, **{"filtered_knn.probe@0.1%.recall@10": 0.985})
        self.assertEqual(failures(compare_metrics(BASE, head)), [])
        head = dict(BASE, **{"filtered_knn.probe@0.1%.recall@10": 0.975})
        self.assertEqual(len(failures(compare_metrics(BASE, head))), 1)
        # 1% selectivity keeps the 1-point default.
        head = dict(BASE, **{"filtered_knn.probe@1%.recall@10": 0.965})
        self.assertEqual(len(failures(compare_metrics(BASE, head))), 1)

    def test_improvements_new_and_missing_metrics(self):
        head = dict(BASE, **{"vector.recall@100": 0.7, "hybrid.ndcg@10": 0.9})
        del head["lexical.ndcg@10"]
        rows = {r["name"]: r for r in compare_metrics(BASE, head)}
        self.assertEqual(rows["vector.recall@100"]["status"], "improved")
        self.assertAlmostEqual(rows["vector.recall@100"]["delta"], 8.5)
        self.assertEqual(rows["hybrid.ndcg@10"]["status"], "new")
        self.assertEqual(rows["lexical.ndcg@10"]["status"], "missing")
        self.assertEqual([r["name"] for r in failures(rows.values())], ["lexical.ndcg@10"])
        # A metric that became unscorable (n/a) is missing too.
        head = dict(BASE, **{"vector.recall@100": None})
        self.assertEqual(failures(compare_metrics(BASE, head))[0]["status"], "missing")

    def test_rows_follow_mode_then_selectivity_order(self):
        names = [
            "filtered_knn.probe@0.1%.recall@10",
            "hybrid.ndcg@10",
            "filtered_knn.planned@100%.recall@10",
            "filtered_knn.probe@100%.recall@10",
            "lexical.recall@100",
            "filtered_knn.probe@10%.recall@10",
            "vector.ndcg@10",
        ]
        m = {n: 0.5 for n in names}
        got = [r["name"] for r in compare_metrics(m, m)]
        self.assertEqual(
            got,
            [
                "lexical.recall@100",
                "vector.ndcg@10",
                "hybrid.ndcg@10",
                "filtered_knn.probe@100%.recall@10",
                "filtered_knn.probe@10%.recall@10",
                "filtered_knn.probe@0.1%.recall@10",
                "filtered_knn.planned@100%.recall@10",
            ],
        )

    def test_threshold_overrides(self):
        head = dict(BASE, **{"lexical.ndcg@10": 0.9104})  # -2.0 points
        self.assertEqual(len(failures(compare_metrics(BASE, head, default=1.0))), 1)
        self.assertEqual(failures(compare_metrics(BASE, head, default=2.5)), [])
        over = compare.DEFAULT_OVERRIDES + [("lexical.*", 3.0)]
        self.assertEqual(failures(compare_metrics(BASE, head, 1.0, over)), [])
        self.assertEqual(compare.parse_override("a.*@0.1%.*=2.5"), ("a.*@0.1%.*", 2.5))
        with self.assertRaises(Exception):
            compare.parse_override("nope")

    def test_comment_has_marker_table_and_verdict(self):
        head = dict(BASE, **{"lexical.ndcg@10": 0.90})
        md = render_comment(
            compare_metrics(BASE, head),
            base_label="abc",
            head_label="def",
            head_report=report(),
        )
        self.assertTrue(md.startswith(MARKER))
        self.assertIn("❌ 1 metric(s) regressed", md)
        self.assertIn("| `lexical.ndcg@10` | 93.04 | 90.00 | -3.04 | −1 | ❌ |", md)
        self.assertIn("| `filtered_knn.probe@0.1%.recall@10` | 100.00 | 100.00 | 0.00 | −2 |  |", md)
        self.assertIn("3000 docs in 3 segments", md)
        ok = render_comment(compare_metrics(BASE, BASE), base_label="a", head_label="b")
        self.assertIn("✅ No metric regressed", ok)

    def test_cli_exit_status_and_baseline_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = write(tmp, "base.json", BASE)
            good = write(tmp, "good.json", BASE)
            bad = write(tmp, "bad.json", dict(BASE, **{"vector.recall@100": 0.5}))
            out = Path(tmp) / "c.md"
            code, _, _ = run(["--base", str(base), "--head", str(good), "--out", str(out)])
            self.assertEqual(code, 0)
            self.assertTrue(out.read_text().startswith(MARKER))
            code, _, err = run(["--base", str(base), "--head", str(bad)])
            self.assertEqual(code, 1)
            self.assertIn("::error::relevance regression: vector.recall@100", err)
            self.assertEqual(run(["--base", str(base), "--head", str(bad), "--no-fail"])[0], 0)
            # No base report: fall back to the committed baseline.
            missing = Path(tmp) / "nope.json"
            code, md, _ = run(["--base", str(missing), "--baseline", str(base), "--head", str(bad)])
            self.assertEqual(code, 1)
            self.assertIn("committed", md)
            self.assertEqual(run(["--base", str(missing), "--head", str(good)])[0], 2)
            # A stale committed baseline is reported, not failed.
            stale = write(tmp, "stale.json", dict(BASE, **{"lexical.ndcg@10": 0.5}))
            code, md, _ = run(["--base", str(base), "--baseline", str(stale), "--head", str(good)])
            self.assertEqual(code, 0)
            self.assertIn("differs from the base run (`lexical.ndcg@10`)", md)

    def test_rejects_non_relevance_reports(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "x.json"
            p.write_text(json.dumps({"schema": 1, "formats": {}}))
            with self.assertRaises(compare.ReportError):
                compare.load_report(p)

    def test_committed_baseline_is_a_complete_report(self):
        metrics, rep = compare.load_report(HERE / "baseline.json")
        for mode in ("lexical", "vector", "hybrid"):
            self.assertIsNotNone(metrics[f"{mode}.ndcg@10"])
            self.assertIsNotNone(metrics[f"{mode}.recall@100"])
        for sel in rep["config"]["selectivities"]:
            for policy in ("probe", "planned"):
                self.assertIsNotNone(metrics[f"filtered_knn.{policy}@{sel}.recall@10"])
        self.assertEqual(rep["config"]["selectivities"], ["100%", "50%", "10%", "1%", "0.1%"])


if __name__ == "__main__":
    unittest.main()
