#!/usr/bin/env python3
"""Compare #381 codes-only runs from local copies of the benchmark archives."""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path

from knn_log_stats import read_rows, summarize

PHASE = "knn_warm_nocache"
VECTOR_CLASSES = {
    "vector.idx", "vector.idx.spans", "vector.offsets",
    "doc_vectors.bin", "doc_vectors.bin.spans",
}
COUNTERS = (
    "knn_rescored", "knn_rescore_selected", "knn_rescore_unavailable",
    "knn_degraded", "knn_seg_quantized", "knn_seg_unrescored", "knn_postings",
    "s3_bytes", "vector_gets", "vector_whole_files", "postings_gets", "page_gets",
)


def load_json(path):
    return json.loads(path.read_text()) if path.is_file() else {}


def number(value):
    return value if isinstance(value, (int, float)) and math.isfinite(value) else None


def local_bytes(path):
    snapshots = {}
    if path.is_file():
        for label, klass, size in re.findall(
            r"^LOCAL-BYTES label=(\S+) ns=\S+ class=(\S+) files=\d+ bytes=(\d+)$",
            path.read_text(), re.MULTILINE,
        ):
            snapshots.setdefault(label, {})[klass] = int(size)
    return {
        label: {
            "vector_bytes": sum(classes.get(klass, 0) for klass in VECTOR_CLASSES),
            "whole_sidecar_bytes": classes.get("doc_vectors.bin", 0),
            "classes": classes,
        }
        for label, classes in snapshots.items()
        if classes.get("total", 0) > 0
        and classes["total"] == sum(size for klass, size in classes.items() if klass != "total")
    }


def run_row(root, allow_whole_sidecar=False):
    vector = root / "vector"
    meta = load_json(root / "meta.json")
    recall = load_json(vector / "recall.json")
    phase = load_json(vector / f"{PHASE}.json")
    env_args = meta.get("server_env_args")
    env = dict(word.split("=", 1) for word in (env_args or "").split() if "=" in word)
    global_rescore = (
        env.get("KOSHA_KNN_GLOBAL_RESCORE", "").lower() in {"1", "true", "on"}
        if env_args is not None else None
    )
    log = vector / "server" / f"{PHASE}.log.gz"
    rows = read_rows(str(log), 0)[0] if log.is_file() else []
    # Only count logged ranged GET categories; whole-file counters are counts
    # of files, not an exact tally of S3 requests.
    for row in rows:
        if all(key in row for key in ("vector_gets", "postings_gets", "page_gets")):
            row["ranged_gets"] = row["vector_gets"] + row["postings_gets"] + row["page_gets"]
    stats = summarize(rows, (*COUNTERS, "ranged_gets"))
    gate = vector / "warm_gate.log"
    terminal = re.findall(
        r"WARM-GATE-(READY|TIMEOUT|UNSUPPORTED)(?: (?:in|after) (\d+)s)?",
        gate.read_text() if gate.is_file() else "",
    )
    gate_status, seconds = terminal[-1] if terminal else (None, "")
    footprint = local_bytes(vector / "local_bytes.txt")
    recall_at_10 = number(recall.get("mean_recall_at_k")) if recall.get("k") == 10 else None
    p50 = number(phase.get("p50_ms")) if phase.get("successes", 0) > 0 else None
    p99 = number(phase.get("p99_ms")) if phase.get("successes", 0) > 0 else None
    issues = []
    if env_args is None:
        issues.append("missing run configuration")
    if recall_at_10 is None:
        issues.append("missing recall@10")
    if not (recall.get("sample_size", 0) >= 1000 and recall.get("sampling") == "random"):
        issues.append("requires at least 1,000 random recall queries")
    if p50 is None or p99 is None:
        issues.append("missing warm latency")
    if phase.get("errors", 0) > 0:
        issues.append("warm phase had failed requests")
    if gate_status != "READY":
        issues.append(f"warm gate {gate_status or 'missing'}")
    if not {"warm", "after_phases"} <= footprint.keys():
        issues.append("missing local-byte snapshot")
    if not allow_whole_sidecar and any(s["whole_sidecar_bytes"] for s in footprint.values()):
        issues.append("whole doc_vectors.bin present in codes-only run")
    if phase.get("knn_degraded_requests", 0) > 0 or recall.get("queries_with_degraded_segments", 0) > 0:
        issues.append("queries reported degraded segments")
    rescores = stats.get("knn_rescored")
    if rescores is None:
        issues.append("missing exact rescore counts")
    elif rescores["n"] != phase.get("requests"):
        issues.append("partial exact rescore coverage")
    return {
        "label": root.name,
        "sha": meta.get("sha"),
        "snapshot": meta.get("vector_snapshot"),
        "quantization": env.get("KOSHA_VECTOR_QUANTIZATION", "f32") if env_args is not None else None,
        "global_rescore": global_rescore,
        "server_env_args": env_args,
        "recall_at_10": recall_at_10,
        "recall_sample": recall.get("sample_size"),
        "recall_seed": recall.get("seed"),
        "recall_num_candidates": recall.get("num_candidates"),
        "p50_ms": p50,
        "p99_ms": p99,
        "target_qps": phase.get("target_qps"),
        "requests": phase.get("requests"),
        "warm_gate_status": gate_status,
        "warm_gate_s": int(seconds) if seconds else None,
        "local_bytes": footprint,
        "logged_queries": len(rows),
        "counters": {key: stats.get(key) for key in (*COUNTERS, "ranged_gets")},
        "issues": issues,
    }


def markdown(runs):
    def fmt(value, spec=""):
        return "missing" if value is None else format(value, spec)

    header = (
        "Run", "Format", "Global", "Recall@10", "Recall probes", "Warm p50 / p99 ms",
        "Rescored p50 / p99 (coverage)", "Mean S3 bytes / ranged GETs",
        "Warm gate s", "Local vector bytes (warm / after)", "Run notes",
    )
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for run in runs:
        counters = run["counters"]
        rescores = counters["knn_rescored"]
        coverage = f" ({rescores['n']}/{fmt(run['requests'])})" if rescores is not None else ""
        values = [
            run["label"].replace("|", "\\|"),
            fmt(run["quantization"]),
            fmt({True: "on", False: "off", None: None}[run["global_rescore"]]),
            fmt(run["recall_at_10"], ".4f"),
            fmt(run["recall_num_candidates"]),
            f"{fmt(run['p50_ms'], '.1f')} / {fmt(run['p99_ms'], '.1f')}",
            " / ".join(fmt(rescores[p], ".0f") for p in ("p50", "p99")) + coverage
            if rescores is not None else "missing",
            " / ".join(fmt((counters[key] or {}).get("mean"), ".1f")
                       for key in ("s3_bytes", "ranged_gets")),
            fmt(run["warm_gate_s"]),
            " / ".join(fmt(run["local_bytes"].get(label, {}).get("vector_bytes"))
                       for label in ("warm", "after_phases")),
            "; ".join(run["issues"]) or "–",
        ]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="+", type=Path, help="archive roots containing meta.json and vector/")
    parser.add_argument("--json", action="store_true", help="print metrics and sample counts as JSON")
    parser.add_argument("--allow-whole-sidecar", action="store_true", help="report a separate whole-sidecar cohort")
    args = parser.parse_args()
    runs = [run_row(root, args.allow_whole_sidecar) for root in args.runs]
    print(json.dumps(runs, indent=2, allow_nan=False) if args.json else markdown(runs))


if __name__ == "__main__":
    main()
