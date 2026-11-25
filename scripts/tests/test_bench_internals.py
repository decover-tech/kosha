import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

BENCH_DIR = Path(__file__).resolve().parents[1] / "bench/bm25_scale"
sys.path.insert(0, str(BENCH_DIR))

import bench_internals as bi  # noqa: E402
import internals_report  # noqa: E402


def timings(total, hydrate, score, cached=False, fully_cached=True, s3_rounds=0, s3_bytes=0):
    return {"total_ms": total, "queue_ms": 0.1, "admit_ms": 0.1, "hydrate_ms": hydrate,
            "bm25_stats_ms": 0.2, "score_ms": score, "materialize_ms": 0.5,
            "cached": cached, "fully_cached": fully_cached,
            "s3_rounds": s3_rounds, "s3_bytes": s3_bytes}


def test_stage_breakdown_shares_tail_and_cache():
    rows = [{"latency_ms": 5.0 + i * 0.01, "send_lag_ms": 0.1, "hits": 10,
             "server": timings(4.0, 1.0, 2.0)} for i in range(99)]
    # One slow request whose time is all hydrate (an S3 fetch).
    rows.append({"latency_ms": 100.0, "send_lag_ms": 0.2, "hits": 10,
                 "server": timings(98.0, 95.0, 2.0, fully_cached=False, s3_rounds=3, s3_bytes=2_000_000)})
    # A result-cache hit ran no stage: counted in the cache rate only.
    rows.append({"latency_ms": 0.5, "send_lag_ms": 0.1, "hits": 10,
                 "server": timings(0.05, 0, 0, cached=True)})

    out = bi.stage_breakdown(rows)
    assert out["requests_with_timings"] == 101
    assert out["result_cache_hit_rate"] == pytest.approx(1 / 101, abs=1e-4)
    assert out["fully_cached_rate"] == pytest.approx(100 / 101, abs=1e-4)
    st = out["stages"]
    assert st["score_ms"]["p50"] == pytest.approx(2.0)
    shares = sum(v["share"] for v in st.values())
    assert shares == pytest.approx(1.0, abs=1e-3)
    # The tail band is the slow hydrate request; the median band is not.
    assert out["tail_band"]["hydrate_ms"] == pytest.approx(95.0)
    assert out["tail_band"]["s3_rounds"] == pytest.approx(3.0)
    assert out["median_band"]["hydrate_ms"] == pytest.approx(1.0)
    assert out["client_overhead_ms"]["p50"] == pytest.approx(1.5, abs=0.05)


def test_stage_breakdown_none_without_timings():
    assert bi.stage_breakdown([{"latency_ms": 3.0, "hits": 10, "server": None}]) is None


def test_counter_deltas_per_request_and_hit():
    before = {"scoring_blocks_decoded": 1000, "posting_reads": {"mode": "ranged", "ranged_gets": 5},
              "doc_store_reads": {"whole_files": 2}, "total_documents": 9}
    after = {"scoring_blocks_decoded": 21000, "posting_reads": {"mode": "ranged", "ranged_gets": 9},
             "doc_store_reads": {"whole_files": 2}, "total_documents": 9}
    d = bi.counter_deltas(before, after, requests=10, hits=100)
    assert d["scoring_blocks_decoded"] == {"delta": 20000, "per_request": 2000.0, "per_hit": 200.0}
    assert d["posting_reads.ranged_gets"]["delta"] == 4
    assert "total_documents" not in d and "posting_reads.mode" not in d
    assert bi.counter_deltas(None, after, 10, 100) is None


def test_summarize_resources():
    first = {"t": 0.0, "cpu_s": 0.0, "rss_mb": 100.0, "threads": 10, "host_total": 0, "host_idle": 0}
    samples = [
        {"t": 1.0, "cpu_s": 2.0, "rss_mb": 120.0, "threads": 12, "host_total": 100, "host_idle": 75},
        {"t": 2.0, "cpu_s": 4.0, "rss_mb": 110.0, "threads": 12, "host_total": 200, "host_idle": 150},
    ]
    r = bi.summarize_resources(first, samples, requests=16, ncpu=8)
    assert r["server_cpu_cores"]["p50"] == pytest.approx(2.0)
    assert r["server_cpu_ms_per_request"] == pytest.approx(250.0)
    assert r["host_cpu_busy"]["p50"] == pytest.approx(0.25)
    assert r["server_rss_mb"] == {"start": 100.0, "max": 120.0, "end": 110.0}


def test_sampler_is_a_noop_when_process_missing():
    s = bi.ResourceSampler("no-such-process-xyz").start()
    assert s.stop(10) is None


def test_report_renders_and_skips_old_phases(tmp_path):
    rows = [{"latency_ms": 5.0, "send_lag_ms": 0.1, "hits": 10, "server": timings(4.0, 1.0, 2.0)}
            for _ in range(20)]
    internals = bi.stage_breakdown(rows)
    internals["counters"] = {"scoring_blocks_decoded": {"delta": 200, "per_request": 10.0, "per_hit": 1.0}}
    (tmp_path / "bm25").mkdir()
    (tmp_path / "bm25" / "warm.json").write_text(json.dumps({"p50_ms": 5.0, "internals": internals}))
    (tmp_path / "bm25" / "old.json").write_text(json.dumps({"p50_ms": 5.0}))
    md = internals_report.render(tmp_path)
    assert "## Engine internals" in md and "| bm25/warm |" in md
    assert "bm25/old" not in md
    assert "2.00 (50%)" in md  # score p50 and share
    assert internals_report.render(tmp_path / "bm25" / "nothing") == ""


class FakeKosha(BaseHTTPRequestHandler):
    stats_calls = 0

    def log_message(self, *a):
        pass

    def _json(self, body):
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        FakeKosha.stats_calls += 1
        self._json({"scoring_blocks_decoded": 1000 * FakeKosha.stats_calls,
                    "posting_reads": {"ranged_gets": FakeKosha.stats_calls}})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        resp = {"results": [{"doc_id": str(i), "score": 1.0} for i in range(10)]}
        if body.get("timings"):
            resp["timings"] = timings(2.0, 0.5, 1.0)
        self._json(resp)


def test_query_bench_writes_internals_end_to_end(tmp_path):
    pytest.importorskip("requests")
    srv = ThreadingHTTPServer(("127.0.0.1", 0), FakeKosha)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        q = tmp_path / "q.txt"
        q.write_text("alpha\nbeta gamma\n")
        out = tmp_path / "phase.json"
        r = subprocess.run(
            [sys.executable, str(BENCH_DIR / "query_bench.py"),
             "--host", f"http://127.0.0.1:{srv.server_address[1]}", "--namespace", "ns",
             "--queries-file", str(q), "--qps", "20", "--duration", "1", "--phase", "warm",
             "--server-timings", "--stats-api-key", "k", "--out", str(out)],
            capture_output=True, text=True, timeout=60)
        assert r.returncode == 0, r.stdout + r.stderr
        d = json.loads(out.read_text())
        i = d["internals"]
        assert i["requests_with_timings"] == d["successes"] > 0
        assert i["stages"]["score_ms"]["p50"] == pytest.approx(1.0)
        assert i["counters"]["scoring_blocks_decoded"]["delta"] == 1000
        assert i["counters"]["posting_reads.ranged_gets"]["delta"] == 1
    finally:
        srv.shutdown()
