import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "bench/bm25_scale/boundary_replication_report.py"


def test_rejects_overlapping_panels_from_different_runs(tmp_path):
    header = "dataset,layout,encoding,assignment,max_copies,spill_fraction,queries\n"
    baseline = tmp_path / "baseline.csv"
    baseline.write_text(header + "synthetic,input,f32,baseline,1,0,100\n")
    variant = tmp_path / "variant.csv"
    variant.write_text(header + "synthetic,input,f32,soar,2,0.2,20\n")
    out = tmp_path / "curves.png"

    result = subprocess.run(
        [sys.executable, str(SCRIPT), str(baseline), str(variant), "--out", str(out)],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "panels already supplied by another CSV: synthetic / input / f32" in result.stderr
    assert not out.exists()
