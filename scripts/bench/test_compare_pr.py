#!/usr/bin/env python3
"""Unit tests for compare_pr.py (run: python3 scripts/bench/test_compare_pr.py)."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from compare_pr import (  # noqa: E402
    MARKER,
    fmt_delta,
    fmt_ms,
    load_report,
    render_comment,
)


def dist(p50, p90=None, p99=None, n=200):
    return {"n": n, "p50": p50, "p90": p90 or p50 * 1.5, "p99": p99 or p50 * 2.0}


def report(scale=1.0, segs=8, docs=4000):
    """Synthetic bench report; `scale` multiplies every latency."""
    return {
        "schema": 1,
        "corpus": {"segs": segs, "docs": docs, "vocab": 20000},
        "formats": {
            "v1": {},  # ignored by the comparator
            "v2": {
                "open_ms": dist(1.0 * scale, n=3),
                "open_bytes": int(1_200_000 * scale),
                "cold_broad_ms": dist(40.0 * scale, n=25),
                "warm_ms": {
                    "broad": dist(0.9 * scale),
                    "two_term_and": dist(0.5 * scale),
                    "phrase": dist(0.4 * scale),
                    "wildcard_w1": dist(15.0 * scale),
                },
            },
        },
    }


class TestFormatting(unittest.TestCase):
    def test_fmt_ms_adaptive_precision(self):
        self.assertEqual(fmt_ms(123.4), "123 ms")
        self.assertEqual(fmt_ms(12.34), "12.3 ms")
        self.assertEqual(fmt_ms(1.234), "1.23 ms")
        self.assertEqual(fmt_ms(0.1234), "0.123 ms")
        self.assertEqual(fmt_ms(None), "–")

    def test_fmt_delta_signs_and_flags(self):
        self.assertEqual(fmt_delta(100.0, 100.0), "+0.0%")
        self.assertEqual(fmt_delta(100.0, 105.0), "+5.0%")  # inside noise band
        self.assertEqual(fmt_delta(100.0, 125.0), "+25.0% ⚠️")
        self.assertEqual(fmt_delta(100.0, 50.0), "-50.0% 🟢")
        self.assertEqual(fmt_delta(None, 50.0), "–")
        self.assertEqual(fmt_delta(0.0, 50.0), "–")  # guard divide-by-zero


class TestRender(unittest.TestCase):
    def test_happy_path_regression_flagged(self):
        md = render_comment(
            report(1.0), report(1.3), base_sha="a" * 40, head_sha="b" * 40
        )
        self.assertTrue(md.startswith(MARKER))
        self.assertIn("aaaaaaaaa", md)
        self.assertIn("bbbbbbbbb", md)
        # every latency row regressed 30% → flagged
        self.assertIn("+30.0% ⚠️", md)
        # all five metrics and the memory row are present
        for label in (
            'cold broad ("the")',
            'warm broad ("the")',
            "warm 2-term AND",
            "warm phrase",
            "warm wildcard w1*",
            "resident while open",
        ):
            self.assertIn(label, md)
        # 3 percentile rows per latency metric + memory row
        self.assertEqual(md.count("| p50 |"), 5)
        self.assertEqual(md.count("| p99 |"), 5)
        self.assertIn("cold n=25, warm n=200", md)
        self.assertNotIn("No baseline", md)
        self.assertNotIn("Corpus mismatch", md)

    def test_improvement_flagged_green(self):
        md = render_comment(
            report(1.0), report(0.5), base_sha="a" * 40, head_sha="b" * 40
        )
        self.assertIn("-50.0% 🟢", md)
        self.assertNotIn("⚠️", md)

    def test_missing_base_degrades_to_pr_only(self):
        md = render_comment(None, report(), base_sha="a" * 40, head_sha="b" * 40)
        self.assertIn("No baseline report", md)
        self.assertIn("| p50 | – |", md)  # before column empty, table still renders
        self.assertIn("40.0 ms", md)  # head numbers present

    def test_corpus_mismatch_warns(self):
        md = render_comment(
            report(segs=4), report(segs=8), base_sha="a" * 40, head_sha="b" * 40
        )
        self.assertIn("Corpus mismatch", md)

    def test_missing_head_v2_is_hard_error(self):
        with self.assertRaises(ValueError):
            render_comment(report(), {"formats": {}}, base_sha="a", head_sha="b")


class TestLoadReport(unittest.TestCase):
    def test_absent_and_malformed_files_return_none(self):
        self.assertIsNone(load_report("/nonexistent/kosha-bench.json"))
        with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
            f.write("not json{")
            f.flush()
            self.assertIsNone(load_report(f.name))
        with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
            f.write('{"no_formats": true}')
            f.flush()
            self.assertIsNone(load_report(f.name))


class TestCli(unittest.TestCase):
    def run_cli(self, base, head):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            if base is not None:
                (d / "base.json").write_text(json.dumps(base))
            (d / "head.json").write_text(json.dumps(head))
            proc = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).parent / "compare_pr.py"),
                    "--base", str(d / "base.json"),
                    "--head", str(d / "head.json"),
                    "--base-sha", "c" * 40,
                    "--head-sha", "d" * 40,
                    "--out", str(d / "comment.md"),
                ],
                capture_output=True,
                text=True,
            )
            out = d / "comment.md"
            return proc, out.read_text() if out.exists() else None

    def test_cli_writes_comment(self):
        proc, md = self.run_cli(report(1.0), report(1.1))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(md.startswith(MARKER))

    def test_cli_missing_base_still_succeeds(self):
        proc, md = self.run_cli(None, report())
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("No baseline report", md)

    def test_cli_missing_head_fails(self):
        with tempfile.TemporaryDirectory() as d:
            proc = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).parent / "compare_pr.py"),
                    "--base", f"{d}/nope.json",
                    "--head", f"{d}/nope.json",
                    "--base-sha", "c",
                    "--head-sha", "d",
                    "--out", f"{d}/comment.md",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 1)
            self.assertIn("head report", proc.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
