import json
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "bench/bm25_scale/bench_gate.py"


def phase(tmp, suite, name, **over):
    d = {"p50_ms": 20.0, "p90_ms": 30.0, "p99_ms": 40.0, "requests": 2400, "errors": 0,
         "zero_hit_requests": 0}
    d.update(over)
    (tmp / suite).mkdir(parents=True, exist_ok=True)
    (tmp / suite / f"{name}.json").write_text(json.dumps(d))


def recall(tmp, tie=0.95, exact=0.94, degraded=0):
    (tmp / "vector").mkdir(parents=True, exist_ok=True)
    (tmp / "vector" / "recall.json").write_text(json.dumps({
        "k": 10, "sample_size": 500, "mean_recall_at_k": exact,
        "recall_dist_at_k": {"n": 500, "mean": tie},
        "queries_with_degraded_segments": degraded,
    }))


def run(tmp, *args):
    r = subprocess.run([sys.executable, str(SCRIPT), str(tmp), *args],
                       capture_output=True, text=True)
    return r.returncode, r.stdout + r.stderr


def test_clean_run_passes(tmp_path):
    phase(tmp_path, "vector", "warm")
    phase(tmp_path, "bm25", "warm_or")
    recall(tmp_path)
    code, out = run(tmp_path, "--expect-recall")
    assert code == 0, out
    assert "All checks passed" in out


def test_zero_hits_fail(tmp_path):
    phase(tmp_path, "bm25", "warm_or", zero_hit_requests=3)
    code, out = run(tmp_path)
    assert code == 1 and "zero-hit" in out


def test_errors_fail_unless_allowed(tmp_path):
    phase(tmp_path, "vector", "cold", errors=34)
    assert run(tmp_path)[0] == 1
    assert run(tmp_path, "--spec", "max_error_rate=0.05")[0] == 0


def test_low_recall_fails_on_tie_tolerant_value(tmp_path):
    phase(tmp_path, "vector", "warm")
    recall(tmp_path, tie=0.81, exact=0.99)
    code, out = run(tmp_path)
    assert code == 1 and "tie-tolerant mean recall@k 0.810" in out


def test_missing_recall_fails_only_when_expected(tmp_path):
    phase(tmp_path, "vector", "warm")
    assert run(tmp_path)[0] == 0
    code, out = run(tmp_path, "--expect-recall")
    assert code == 1 and "recall.json missing" in out


def test_sweep_and_meta_are_not_gated(tmp_path):
    phase(tmp_path, "vector", "warm")
    phase(tmp_path / "knn_sweep", "f0.01", "warm", zero_hit_requests=5)
    (tmp_path / "meta.json").write_text("{}")
    assert run(tmp_path)[0] == 0


def test_off_reports_but_passes(tmp_path):
    phase(tmp_path, "vector", "warm", errors=10)
    code, out = run(tmp_path, "--spec", "off")
    assert code == 0 and "Gate **off**" in out


def test_bad_spec_is_rejected(tmp_path):
    phase(tmp_path, "vector", "warm")
    assert run(tmp_path, "--spec", "recall=0.9")[0] == 2


def test_no_phases_fails(tmp_path):
    assert run(tmp_path)[0] == 1
