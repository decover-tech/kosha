#!/usr/bin/env python3
"""Pass/fail bar for a benchmark.yml run, over its collected results dir.

    python3 scripts/bench/bm25_scale/bench_gate.py RESULTS_DIR \
        [--spec "recall_min=0.93 max_error_rate=0"] [--expect-recall] [--out gate.md]

Checks, each one a reason the numbers are not publishable:

  * every latency phase (<suite>/<phase>.json from query_bench.py) ran
    requests, has a real p50, an error rate at most `max_error_rate`, and
    no zero-hit requests. A broken read path returns zero hits *faster*
    (the KIZC regression, PR #83), so a zero-hit phase is never a pass.
  * vector/recall.json: tie-tolerant mean recall@k (`recall_dist_at_k`,
    falling back to exact `mean_recall_at_k` on older reports) is at least
    `recall_min`, and no query hit a degraded kNN segment. A latency number
    without a paired recall number is how the #160 regression shipped, so
    with --expect-recall a missing recall.json fails too.

The kNN sweep (knn_sweep/) is reported by its own table, never gated: its
points are meant to trade recall for latency.

`--spec off` skips the gate (exit 0) but still writes the report.
Exit status 1 when any check fails.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import List, Optional, Tuple

DEFAULTS = {"recall_min": 0.93, "max_error_rate": 0.0}
SKIP_NAMES = {"recall.json", "meta.json", "snapshot-build-info.json", "load.json"}
SKIP_DIRS = {"knn_sweep", "two_tier"}


def parse_spec(text: str) -> Optional[dict]:
    """`key=value ...` over DEFAULTS; None for `off`."""
    spec = dict(DEFAULTS)
    text = (text or "").strip()
    if text == "off":
        return None
    for item in text.split():
        key, sep, value = item.partition("=")
        if not sep or key not in DEFAULTS:
            raise ValueError(f"bad gate item {item!r}; keys: {', '.join(DEFAULTS)} (or 'off')")
        spec[key] = float(value)
    return spec


def phase_files(root: Path) -> List[Path]:
    out = []
    for p in sorted(root.rglob("*.json")):
        rel = p.relative_to(root)
        if p.name in SKIP_NAMES or any(part in SKIP_DIRS for part in rel.parts):
            continue
        out.append(p)
    return out


def check_phase(path: Path, max_error_rate: float) -> Tuple[str, List[str]]:
    try:
        d = json.loads(path.read_text())
    except (OSError, ValueError) as e:
        return path.stem, [f"unreadable ({e})"]
    if not isinstance(d, dict) or "p50_ms" not in d:
        return path.stem, []  # not a latency phase report
    problems = []
    requests = d.get("requests") or 0
    errors = d.get("errors") or 0
    p50 = d.get("p50_ms")
    if requests <= 0:
        problems.append("no requests")
    if not isinstance(p50, (int, float)) or math.isnan(p50):
        problems.append("p50 is NaN")
    if requests and errors / requests > max_error_rate:
        problems.append(f"{errors}/{requests} errors")
    zero = d.get("zero_hit_requests") or 0
    if zero:
        problems.append(f"{zero} zero-hit requests")
    return f"{path.parent.name}/{path.stem}", problems


def recall_value(d: dict) -> Tuple[Optional[float], str]:
    tie = d.get("recall_dist_at_k")
    if isinstance(tie, dict) and isinstance(tie.get("mean"), (int, float)):
        return tie["mean"], "tie-tolerant"
    v = d.get("mean_recall_at_k")
    return (v if isinstance(v, (int, float)) else None), "exact"


def check_recall(root: Path, recall_min: float, expect: bool) -> Tuple[Optional[str], List[str]]:
    path = root / "vector" / "recall.json"
    if not path.exists():
        return None, (["vector/recall.json missing (recall check expected)"] if expect else [])
    try:
        d = json.loads(path.read_text())
    except (OSError, ValueError) as e:
        return None, [f"vector/recall.json unreadable ({e})"]
    value, kind = recall_value(d)
    problems = []
    if value is None or math.isnan(value):
        problems.append("recall is missing or NaN")
    elif value < recall_min:
        problems.append(f"{kind} mean recall@k {value:.3f} < {recall_min:.3f}")
    degraded = d.get("queries_with_degraded_segments") or 0
    if degraded:
        problems.append(f"{degraded} queries hit a degraded kNN segment")
    shown = "n/a" if value is None else f"{value:.3f}"
    return f"{kind} recall@{d.get('k')} {shown} over {d.get('sample_size')} queries", problems


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("results", type=Path)
    ap.add_argument("--spec", default="")
    ap.add_argument("--expect-recall", action="store_true")
    ap.add_argument("--out", type=Path)
    args = ap.parse_args(argv)
    try:
        spec = parse_spec(args.spec)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    effective = spec or DEFAULTS
    failures: List[str] = []
    lines = ["## Bench gate", ""]
    phases = [check_phase(p, effective["max_error_rate"]) for p in phase_files(args.results)]
    if not phases:
        failures.append("no latency phase results")
    for name, problems in phases:
        failures += [f"`{name}`: {p}" for p in problems]
    recall_line, problems = check_recall(args.results, effective["recall_min"], args.expect_recall)
    failures += problems

    if spec is None:
        lines.append("Gate **off** (`bench_gate=off`); checks shown for information.")
    elif failures:
        lines.append(f"**❌ {len(failures)} check(s) failed** — these numbers are not publishable.")
    else:
        lines.append("**✅ All checks passed.**")
    lines += [
        "",
        f"- bar: recall ≥ {effective['recall_min']:.3f} (tie-tolerant), error rate ≤ "
        f"{effective['max_error_rate']:g}, zero-hit requests = 0",
        f"- phases checked: {len(phases)}",
    ]
    if recall_line:
        lines.append(f"- {recall_line}")
    lines += [f"- ❌ {f}" for f in failures]
    lines.append("")
    text = "\n".join(lines)
    if args.out:
        args.out.write_text(text)
    print(text)
    if spec is not None and failures:
        for f in failures:
            print(f"::error::bench gate: {f}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
