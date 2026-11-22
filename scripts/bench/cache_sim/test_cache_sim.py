"""Unit tests for cache_sim.py (#389). Run: python3 -m unittest discover -s scripts/bench/cache_sim"""

import gzip
import json
import os
import sys
import tempfile
import unittest

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


if __name__ == "__main__":
    unittest.main()
