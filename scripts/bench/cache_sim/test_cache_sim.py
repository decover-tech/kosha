"""Unit tests for cache_sim.py (#389). Run: python3 -m unittest discover -s scripts/bench/cache_sim"""

import gzip
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cache_sim as cs  # noqa: E402


def run(cache, seq):
    return [cache.access(k, s) for k, s in seq]


class PolicyTests(unittest.TestCase):
    def scan_pattern(self):
        # Two hot objects re-read between every one-touch scan object.
        seq = [("h1", 10), ("h2", 10)]
        for i in range(50):
            seq += [("h1", 10), ("h2", 10), (f"s{i}", 10)]
        return seq

    def hot_hits_at_end(self, cls):
        cache = cls(50)
        hits = run(cache, self.scan_pattern())
        return sum(hits[-30:])

    def test_fifo_loses_hot_objects_to_a_scan(self):
        # Without a read signal the hot pair is evicted by the scan and
        # re-fetched over and over.
        self.assertLess(self.hot_hits_at_end(cs.FifoCache), 20)

    def test_lru_sieve_s3fifo_keep_hot_objects(self):
        for cls in (cs.LruCache, cs.SieveCache, cs.S3FifoCache):
            cache = cls(50)
            hits = run(cache, self.scan_pattern())
            # Every hot access after warmup hits: 2 of each 3 accesses.
            self.assertEqual(sum(hits[-30:]), 20, cls.name)

    def test_byte_bound_holds(self):
        for cls in cs.POLICIES.values():
            cache = cls(100)
            for i in range(200):
                cache.access(f"k{i % 37}", 7 + i % 13)
            used = getattr(cache, "used", None)
            if used is None:
                used = cache.small_bytes + cache.main_bytes
            self.assertLessEqual(used, 100, cls.name)

    def test_object_larger_than_cache_is_never_admitted(self):
        for cls in cs.POLICIES.values():
            cache = cls(10)
            self.assertFalse(cache.access("big", 11))
            self.assertFalse(cache.access("big", 11))

    def test_sieve_evicts_unvisited_first(self):
        c = cs.SieveCache(30)
        run(c, [("a", 10), ("b", 10), ("c", 10), ("a", 10)])
        c.access("d", 10)  # a visited -> spared; b evicted
        self.assertIn("a", c.size)
        self.assertNotIn("b", c.size)


class CostTests(unittest.TestCase):
    def test_break_even_interval_scales_inversely_with_size(self):
        c = cs.Costs()
        self.assertAlmostEqual(c.break_even_s(1 << 20) / c.break_even_s(4 << 20), 4.0, places=6)
        # $0.0004/1k GETs vs $0.16/GB-month: a 1 MiB file breaks even at
        # roughly 0.0000004 / (2^20 * 0.16 / 2^30 / 2628000) s.
        expected = 4e-7 / ((1 << 20) * 0.16 / (1 << 30) / (730 * 3600))
        self.assertAlmostEqual(c.break_even_s(1 << 20), expected, delta=expected * 1e-9)

    def test_part_size_splits_gets(self):
        c = cs.Costs(get_part_mb=8)
        self.assertEqual(c.gets_for(1), 1)
        self.assertEqual(c.gets_for(17 << 20), 3)


class TraceTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.sizes = os.path.join(self.dir, "sizes.tsv")
        with open(self.sizes, "w") as f:
            f.write("ns/a/footer.json\t100\n")
            f.write("ns/a/inverted.idx\t1000\n")
            f.write("ns/a/postings-01.bin\t5000\n")
            f.write("ns/a/postings-02.bin\t7000\n")
            f.write("ns/a/doc_store.bin\t9000\n")
            f.write("ns/a/doc_store.bin.spans/0-10\t10\n")  # ignored
            f.write("ns/a/_COMMIT\t10\n")  # unclassified: ignored
        self.trace = os.path.join(self.dir, "trace.jsonl.gz")
        lines = [
            {"v": 1, "t_ms": 1000, "ns": "ns", "segs": ["ns/a"], "classes": ["scoring", "postings"],
             "postings": ["postings-01.bin"], "pages": []},
            {"v": 1, "t_ms": 2000, "ns": "ns", "segs": ["ns/a", "ns/missing"], "classes": ["scoring"],
             "postings": None, "pages": ["ns/a"]},
        ]
        with gzip.open(self.trace, "wt") as f:
            f.write("2026-11-20T00:00:00Z junk line\n")
            for rec in lines:
                f.write("2026-11-20T00:00:00Z " + json.dumps(rec) + "\n")

    def test_expand_names_exact_shards_and_page_doc_store(self):
        sizes = cs.load_sizes(self.sizes)
        queries, stats = cs.load_trace(self.trace, sizes)
        self.assertEqual(len(queries), 2)
        k0 = {k for k, _ in queries[0].objects}
        self.assertEqual(k0, {"ns/a/footer.json", "ns/a/inverted.idx", "ns/a/postings-01.bin"})
        k1 = {k for k, _ in queries[1].objects}
        self.assertEqual(k1, {"ns/a/footer.json", "ns/a/inverted.idx", "ns/a/doc_store.bin"})
        self.assertEqual(stats["segments_missing_sizes"], 1)

    def test_replay_rows_cover_every_policy(self):
        sizes = cs.load_sizes(self.sizes)
        queries, _ = cs.load_trace(self.trace, sizes)
        rows = cs.run_replay(queries, [20000], ["fifo", "lru", "sieve", "s3fifo", "static", "fivemin"],
                             cs.Costs(), 0.5)
        self.assertEqual([r["policy"] for r in rows], ["fifo", "lru", "sieve", "s3fifo", "static", "fivemin"])
        for r in rows:
            self.assertGreaterEqual(r["total_dollars"], 0.0)

    def test_synth_and_cli_round_trip(self):
        t = os.path.join(self.dir, "s.jsonl")
        s = os.path.join(self.dir, "s.tsv")
        out = os.path.join(self.dir, "o.json")
        self.assertEqual(cs.main(["synth", "--trace-out", t, "--sizes-out", s, "--queries", "200",
                                  "--segments", "4"]), 0)
        self.assertEqual(cs.main(["replay", "--trace", t, "--sizes", s, "--capacity-frac", "0.3",
                                  "--json", out]), 0)
        with open(out) as f:
            rows = json.load(f)["rows"]
        by = {r["policy"]: r for r in rows}
        # The synthetic trace interleaves one-touch scans: FIFO must do no
        # better than the scan-resistant policies.
        self.assertLessEqual(by["fifo"]["hit_ratio"], by["s3fifo"]["hit_ratio"])
        self.assertLessEqual(by["fifo"]["hit_ratio"], by["sieve"]["hit_ratio"])


class LogCostTests(unittest.TestCase):
    def test_parses_search_timing_lines(self):
        d = tempfile.mkdtemp()
        p = os.path.join(d, "server.log")
        with open(p, "w") as f:
            f.write("2026-11-20T00:00:00.1Z search timing: ns=x total_ms=12.5 hydrate_ms=0.1 "
                    "hydrate_files=2 hydrate_mb=1.0 page_gets=3 page_whole_files=0 postings_gets=1 "
                    "vector_gets=0 vector_whole_files=0 s3_bytes=1048576 request_id=abc\n")
            f.write("2026-11-20T00:00:00.2Z search timing: ns=x total_ms=30.0 hydrate_files=0 "
                    "page_gets=0 postings_gets=0 vector_gets=0 s3_bytes=0\n")
            f.write("unrelated line\n")
        out = cs.run_logcost(cs.parse_timing_lines(p), cs.Costs(), None)
        self.assertEqual(out["searches"], 2)
        self.assertEqual(out["gets"], 6)
        self.assertAlmostEqual(out["queries_with_s3"], 0.5)
        self.assertAlmostEqual(out["s3_dollars"], 6 * 4e-7)


class TenantLayoutTests(unittest.TestCase):
    """The server keeps segments under {tenant}/{ns}/{segment}/ (#262); the
    first version only accepted {ns}/{segment}/{file} and printed all-zero
    rows for real listings."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.trace = os.path.join(self.dir, "trace.jsonl")
        with open(self.trace, "w") as f:
            for i in range(20):
                f.write(json.dumps({
                    "v": 1, "t_ms": 1000 + i, "ns": "default/msmarco-10m",
                    "segs": ["default/msmarco-10m/seg-a"], "classes": ["scoring", "postings"],
                    "postings": ["postings-01.bin"], "pages": ["default/msmarco-10m/seg-a"],
                }) + "\n")

    def write_sizes(self, lines):
        path = os.path.join(self.dir, "sizes.tsv")
        with open(path, "w") as f:
            f.write("".join(f"{k}\t{v}\n" for k, v in lines))
        return path

    def test_split_segment_path_any_depth(self):
        self.assertEqual(cs.split_segment_path("default/ns/seg/footer.json"),
                         ("default/ns/seg", "footer.json", False))
        self.assertEqual(cs.split_segment_path("ns/seg/doc_store.bin.spans/00-10"),
                         ("ns/seg", "doc_store.bin", True))
        self.assertEqual(cs.split_segment_path("default/ns/seg/doc_store.bin.spans/00-10"),
                         ("default/ns/seg", "doc_store.bin", True))
        self.assertIsNone(cs.split_segment_path("footer.json"))

    def test_s3_listing_with_prefix_resolves_every_class(self):
        sizes_path = self.write_sizes([
            ("segments/default/msmarco-10m/seg-a/footer.json", 100),
            ("segments/default/msmarco-10m/seg-a/inverted.idx", 1000),
            ("segments/default/msmarco-10m/seg-a/postings-01.bin", 5000),
            ("segments/default/msmarco-10m/seg-a/postings-02.bin", 7000),
            ("segments/default/msmarco-10m/seg-a/doc_store.bin", 9000),
            ("other/x/y/z", 1),
        ])
        stats = cs.collections.Counter()
        sizes = cs.load_sizes(sizes_path, strip_prefix="segments/", stats=stats)
        self.assertEqual(stats["sizes_outside_prefix"], 1)
        queries, qstats = cs.load_trace(self.trace, sizes)
        keys = {k for k, _ in queries[0].objects}
        self.assertEqual(keys, {
            "default/msmarco-10m/seg-a/footer.json", "default/msmarco-10m/seg-a/inverted.idx",
            "default/msmarco-10m/seg-a/postings-01.bin", "default/msmarco-10m/seg-a/doc_store.bin",
        })
        self.assertEqual(qstats.get("segments_missing_sizes", 0), 0)
        self.assertEqual(qstats["docstore_resolved"], 20)
        out = os.path.join(self.dir, "o.json")
        self.assertEqual(cs.main(["replay", "--trace", self.trace, "--sizes", sizes_path,
                                  "--strip-prefix", "segments/", "--capacity-gb", "1",
                                  "--json", out]), 0)
        with open(out) as f:
            rows = json.load(f)["rows"]
        self.assertTrue(all(r["queries"] > 0 for r in rows))
        self.assertGreater(rows[0]["hit_ratio"], 0)

    def test_layout_mismatch_exits_nonzero_instead_of_zero_rows(self):
        # A listing whose paths do not match the trace's (here: the S3
        # prefix left on) must fail loudly.
        sizes_path = self.write_sizes([
            ("segments/default/msmarco-10m/seg-a/footer.json", 100),
        ])
        self.assertEqual(cs.main(["replay", "--trace", self.trace, "--sizes", sizes_path,
                                  "--capacity-gb", "1"]), 2)

    def test_span_only_files_are_counted_and_optionally_summed(self):
        # Local disk with span promotion off: doc store and shards exist
        # only as spans.
        lines = [
            ("default/msmarco-10m/seg-a/footer.json", 100),
            ("default/msmarco-10m/seg-a/doc_store.bin.spans/0000-0010", 16),
            ("default/msmarco-10m/seg-a/doc_store.bin.spans/0020-0030", 16),
            ("default/msmarco-10m/seg-a/postings-01.bin.spans/0000-0040", 64),
        ]
        sizes_path = self.write_sizes(lines)
        stats = cs.collections.Counter()
        skip = cs.load_sizes(sizes_path, stats=stats)
        self.assertEqual(stats["span_only_files"], 2)
        _, qstats = cs.load_trace(self.trace, skip)
        self.assertEqual(qstats["docstore_unresolved"], 20)
        self.assertEqual(qstats["postings_unresolved"], 20)
        summed = cs.load_sizes(sizes_path, span_files="sum")
        queries, qstats = cs.load_trace(self.trace, summed)
        sz = dict(queries[0].objects)
        self.assertEqual(sz["default/msmarco-10m/seg-a/doc_store.bin"], 32)
        self.assertEqual(sz["default/msmarco-10m/seg-a/postings-01.bin"], 64)
        self.assertEqual(qstats.get("docstore_unresolved", 0), 0)

    def test_synth_tenant_round_trip(self):
        t = os.path.join(self.dir, "s.jsonl")
        s = os.path.join(self.dir, "s.tsv")
        self.assertEqual(cs.main(["synth", "--trace-out", t, "--sizes-out", s, "--queries", "100",
                                  "--segments", "3", "--tenant", "default"]), 0)
        self.assertEqual(cs.main(["replay", "--trace", t, "--sizes", s,
                                  "--capacity-frac", "0.3"]), 0)


class P99CiTests(unittest.TestCase):
    def write(self, name, lat, errors=0):
        path = os.path.join(self.dir, name)
        with gzip.open(path, "wt") as f:
            for i, v in enumerate(lat):
                f.write(json.dumps({"i": i, "latency_ms": v}) + "\n")
            for _ in range(errors):
                f.write(json.dumps({"latency_ms": 1.0, "error": "timeout"}) + "\n")
        return path

    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_identical_arms_ci_covers_zero_and_shifted_arm_does_not(self):
        import random as _r
        rng = _r.Random(1)
        a = [rng.lognormvariate(3.4, 0.6) for _ in range(3000)]
        b_same = [rng.lognormvariate(3.4, 0.6) for _ in range(3000)]
        b_slow = [v * 2 for v in b_same]
        pa = self.write("a.jsonl.gz", a, errors=3)
        out = cs.run_p99ci([pa], [self.write("b.jsonl.gz", b_same)], 300, 7)
        self.assertEqual(out["a_errors"], 3)
        lo, hi = out["p99"]["diff_ci"]
        self.assertLessEqual(lo, 0)
        self.assertGreaterEqual(hi, 0)
        out = cs.run_p99ci([pa], [self.write("c.jsonl.gz", b_slow)], 300, 7)
        self.assertGreater(out["p99"]["diff_ci"][0], 0)
        lo, hi = out["p99"]["a_ci"]
        self.assertLessEqual(lo, out["p99"]["a"])
        self.assertGreaterEqual(hi, out["p99"]["a"])


class ArmTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)

    def make_arm(self, suite):
        (self.root / "meta.json").write_text(json.dumps({
            "suite": suite, "warm_gate": "timed", "phase_repeats": "2", "cache_policy": "sieve",
        }))
        out = self.root / suite
        (out / "server").mkdir(parents=True)
        gib = 1 << 30
        prefix = "warm_or_nocache" if suite == "bm25" else "knn_warm_nocache"
        labels = [f"{prefix}_r{i}" for i in (1, 2)]
        (out / "phase_footprint.tsv").write_text(
            "phase\tstart_s\tstart_bytes\tend_s\tend_bytes\n"
            f"{labels[0]}\t100\t{gib}\t200\t{gib}\n"
            f"{labels[1]}\t300\t{2 * gib}\t400\t{4 * gib}\n"
        )
        # Warm fill, exact boundary duplicates and the gap are all excluded.
        (out / "footprint.tsv").write_text(
            f"50\t{100 * gib}\n100\t{100 * gib}\n150\t{3 * gib}\n250\t{100 * gib}\n"
        )
        for i, label in enumerate(labels):
            (out / f"{label}.json").write_text(json.dumps({
                "requests": 3, "successes": 2, "errors": 1,
                "knn_degraded_requests": i if suite == "vector" else 0,
            }))
            with gzip.open(out / f"{label}.requests.jsonl.gz", "wt") as f:
                for rec in ({"latency_ms": 1 + 2 * i}, {"latency_ms": 2 + 2 * i}, {"error": "timeout"}):
                    f.write(json.dumps(rec) + "\n")
            with gzip.open(out / "server" / f"{label}.log.gz", "wt") as f:
                f.write(("search timing: hydrate_files=1 page_gets=1 page_whole_files=0 "
                         "postings_gets=0 vector_gets=0 vector_whole_files=0 s3_bytes=4096\n") * 2)
        return out, labels

    def test_measured_cost_and_pooled_latency(self):
        self.make_arm("bm25")
        c = cs.Costs(get_per_1k=1, nvme_gb_month=0.73)
        totals = cs.run_arm(self.root, c)["totals"]
        self.assertEqual(totals["requests"], 6)
        self.assertEqual(totals["errors"], 2)
        self.assertEqual(totals["s3_gets"], 8)
        self.assertAlmostEqual(totals["s3_bytes_per_query"], 4 * 4096 / 6)
        self.assertEqual(totals["duration_s"], 200)
        self.assertAlmostEqual(totals["local_gib_hours"], 500 / 3600)
        self.assertAlmostEqual(totals["mean_local_bytes"], 2.5 * (1 << 30))
        self.assertEqual(totals["end_local_bytes"], 4 << 30)
        self.assertAlmostEqual(totals["p99_ms"], 3.97)
        self.assertAlmostEqual(totals["dollars_per_1k_queries"], (0.008 + 0.001 * 500 / 3600) * 1000 / 6)

    def test_vector_arm_surfaces_degradation_and_rejects_missing_logs(self):
        out, labels = self.make_arm("vector")
        self.assertEqual(cs.run_arm(self.root, cs.Costs())["totals"]["knn_degraded_requests"], 1)
        with gzip.open(out / "server" / f"{labels[0]}.log.gz", "wt") as f:
            f.write("no search timings\n")
        with self.assertRaisesRegex(ValueError, "expected 2 to 3 server search timings, got 0"):
            cs.run_arm(self.root, cs.Costs())

    def test_rejects_incomplete_repeats(self):
        out, _ = self.make_arm("bm25")
        windows = out / "phase_footprint.tsv"
        windows.write_text("\n".join(windows.read_text().splitlines()[:-1]) + "\n")
        with self.assertRaisesRegex(ValueError, "expected measured repeats"):
            cs.run_arm(self.root, cs.Costs())


if __name__ == "__main__":
    unittest.main()
