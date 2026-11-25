#!/usr/bin/env python3
"""Unit tests for compare_knn.py.  python3 scripts/bench/test_compare_knn.py"""

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import compare_knn  # noqa: E402


def dist(p50, p90=None, p99=None):
    return {"n": 200, "p50": p50, "p90": p90 or p50 * 1.3, "p99": p99 or p50 * 2}


def report(warm=1.0, hybrid=1.2, cold=6.0, open_ms=2.5, postings=100.0, digest="aa"):
    return {
        "schema": 1,
        "corpus": {"segs": 8, "docs": 4000, "dim": 256, "k": 10, "clusters": 64},
        "open_ms": dist(open_ms),
        "cold_knn_ms": dist(cold),
        "warm_ms": {"knn": dist(warm), "hybrid_broad": dist(hybrid)},
        "warm_knn_scan": {"postings_per_query": postings},
        "warm_knn_phases": {"topk_digest": digest},
    }


class CompareKnnTest(unittest.TestCase):
    def run_main(self, base, head, *extra):
        with tempfile.TemporaryDirectory() as tmp:
            paths = []
            for side, reports in (("base", base), ("head", head)):
                for i, r in enumerate(reports):
                    p = Path(tmp) / f"{side}{i}.json"
                    p.write_text(json.dumps(r))
                    paths += [f"--{side}", str(p)]
            out = Path(tmp) / "out.md"
            code = compare_knn.main(paths + ["--out", str(out), *extra])
            return code, out.read_text() if out.exists() else ""

    def test_noise_passes(self):
        code, text = self.run_main([report()], [report(warm=1.15, cold=7.5)])
        self.assertEqual(code, 0)
        self.assertIn("No kNN regression", text)

    def test_large_warm_regression_fails(self):
        code, text = self.run_main([report()], [report(warm=1.5)])
        self.assertEqual(code, 1)
        self.assertIn("`warm kNN p50`", text)

    def test_absolute_floor_prevents_tiny_failures(self):
        # +50% but only 0.05 ms.
        code, _ = self.run_main([report(warm=0.1)], [report(warm=0.15)])
        self.assertEqual(code, 0)

    def test_best_of_interleaved_runs(self):
        # One noisy head run is ignored: the lower p50 is used.
        code, _ = self.run_main([report(), report(warm=1.05)], [report(warm=2.0), report(warm=1.1)])
        self.assertEqual(code, 0)

    def test_postings_growth_fails_even_when_latency_is_flat(self):
        code, text = self.run_main([report()], [report(postings=130.0)])
        self.assertEqual(code, 1)
        self.assertIn("postings probed", text)

    def test_accept_label_reports_but_passes(self):
        code, text = self.run_main([report()], [report(warm=3.0)], "--accept")
        self.assertEqual(code, 0)
        self.assertIn("accepted by the `perf-regression-accepted` label", text)

    def test_missing_base_is_not_gated(self):
        code, text = self.run_main([], [report(warm=9.0)])
        self.assertEqual(code, 0)
        self.assertIn("No baseline report", text)

    def test_corpus_mismatch_is_not_gated(self):
        other = copy.deepcopy(report())
        other["corpus"]["docs"] = 2000
        code, _ = self.run_main([other], [report(warm=9.0)])
        self.assertEqual(code, 0)

    def test_digest_change_is_reported(self):
        code, text = self.run_main([report()], [report(digest="bb")])
        self.assertEqual(code, 0)
        self.assertIn("top-k changed", text)

    def test_bad_head_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "h.json"
            p.write_text("{}")
            self.assertEqual(compare_knn.main(["--head", str(p)]), 2)


if __name__ == "__main__":
    unittest.main()
