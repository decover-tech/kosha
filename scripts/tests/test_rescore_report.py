import gzip
import json
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "bench" / "bm25_scale" / "rescore_report.py"


@pytest.fixture
def archive(tmp_path):
    def make(label, *, global_rescore=False, counts=(0, 0), whole_bytes=0):
        root = tmp_path / label
        vector = root / "vector"
        server = vector / "server"
        server.mkdir(parents=True)
        env = "-e KOSHA_VECTOR_QUANTIZATION=rabitq -e KOSHA_KNN_RESCORE_STATS=1"
        if global_rescore:
            env += " -e KOSHA_KNN_GLOBAL_RESCORE=1"
        (root / "meta.json").write_text(json.dumps({
            "sha": "test", "vector_snapshot": "rabitq-snapshot", "server_env_args": env,
        }))
        (vector / "recall.json").write_text(json.dumps({
            "k": 10, "mean_recall_at_k": 0.95, "sample_size": 1000,
            "sampling": "random", "seed": 384, "num_candidates": 100,
        }))
        (vector / "knn_warm_nocache.json").write_text(json.dumps({
            "requests": 2, "successes": 2, "errors": 0, "p50_ms": 35, "p99_ms": 70,
        }))
        (vector / "warm_gate.log").write_text("WARM-GATE-READY in 65s (all queries cached)\n")
        sizes = {"vector.idx": 400, "vector.offsets": 50, "doc_vectors.bin.spans": 100, "other": 80}
        if whole_bytes:
            sizes["doc_vectors.bin"] = whole_bytes
        sizes["total"] = sum(sizes.values())
        (vector / "local_bytes.txt").write_text("".join(
            f"LOCAL-BYTES label={phase} ns=bench class={klass} files=1 bytes={size}\n"
            for phase in ("warm", "after_phases")
            for klass, size in sizes.items()
        ))
        with gzip.open(server / "knn_warm_nocache.log.gz", "wt") as log:
            for count in counts:
                log.write("search timing: ns=bench knn_postings=100 s3_bytes=2048 "
                          "vector_gets=1 postings_gets=0 page_gets=1 knn_degraded=0")
                if count is not None:
                    log.write(f" knn_rescored={count}")
                if global_rescore:
                    log.write(" knn_rescore_selected=4 knn_rescore_unavailable=0")
                else:
                    log.write(" knn_seg_quantized=2 knn_seg_unrescored=2")
                log.write("\n")
        return root

    return make


def report(*args):
    return subprocess.check_output([sys.executable, str(SCRIPT), *map(str, args)], text=True)


def test_compares_global_and_per_segment_counts(archive):
    on = archive("on", global_rescore=True, counts=(3, 4))
    off = archive("off")
    on_row, off_row = json.loads(report("--json", on, off))
    assert on_row["global_rescore"] is True
    assert off_row["global_rescore"] is False
    assert on_row["counters"]["knn_rescored"]["mean"] == 3.5
    assert off_row["counters"]["knn_rescored"]["mean"] == 0
    assert off_row["counters"]["knn_rescore_selected"] is None
    assert on_row["counters"]["ranged_gets"]["mean"] == 2
    assert on_row["recall_at_10"] == 0.95
    assert on_row["warm_gate_s"] == 65
    assert on_row["local_bytes"]["warm"]["vector_bytes"] == 550
    assert on_row["issues"] == off_row["issues"] == []
    assert "0 / 0 (2/2)" in report(on, off)


def test_missing_and_partial_counts_are_not_zero(archive):
    missing = archive("missing", counts=(None, None))
    (missing / "vector" / "recall.json").unlink()
    (missing / "vector" / "local_bytes.txt").unlink()
    partial = archive("partial", counts=(8, None))
    missing_row, partial_row = json.loads(report("--json", missing, partial))
    assert missing_row["counters"]["knn_rescored"] is None
    assert missing_row["recall_at_10"] is None
    assert missing_row["local_bytes"] == {}
    assert "missing exact rescore counts" in missing_row["issues"]
    assert partial_row["counters"]["knn_rescored"]["n"] == 1
    assert partial_row["counters"]["knn_rescored"]["mean"] == 8
    assert partial_row["issues"] == ["partial exact rescore coverage"]
    assert "| missing | 2048.0 / 2.0 |" in report(missing)


def test_whole_sidecar_is_not_codes_only(archive):
    root = archive("whole", counts=(8, 8), whole_bytes=200)
    row, = json.loads(report("--json", root))
    assert row["local_bytes"]["warm"]["vector_bytes"] == 750
    assert row["issues"] == ["whole doc_vectors.bin present in codes-only run"]
    whole_row, = json.loads(report("--json", "--allow-whole-sidecar", root))
    assert whole_row["issues"] == []
