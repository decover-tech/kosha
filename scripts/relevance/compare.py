#!/usr/bin/env python3
"""Compare two relevance-harness reports and gate on regressions (#258).

    python3 scripts/relevance/compare.py --base base.json --head head.json \
        [--baseline scripts/relevance/baseline.json] [--out comment.md] \
        [--threshold 1.0] [--threshold-for 'GLOB=POINTS' ...] [--no-fail]
    python3 scripts/relevance/compare.py --self-test

Reports are what `cargo bench -p kosha-query --bench relevance` writes to
KOSHA_RELEVANCE_JSON: `{"kind": "kosha-relevance", "metrics": {name:
{"value": 0.93, ...}}}`, every value a fraction in [0, 1]. Deltas and
thresholds are in points (hundredths): a drop of more than the threshold
from base to head is a regression and the exit status is 1.

Default thresholds: 1.0 point for every metric, 2.0 for filtered recall at
0.1% selectivity (a handful of passing documents per query, so one miss
moves it a lot). `--threshold` replaces the default; `--threshold-for` adds
an fnmatch-pattern override, the last matching pattern winning.

When `--base` is missing or unreadable (the base tree predates the harness,
or its build failed), the head is compared with `--baseline`, the baseline
committed for main, and the comment says so. When both a base report and the
committed baseline are available, the comment also notes whether the
committed baseline has gone stale (it should equal the base report, since
the harness is deterministic).

The rendered markdown starts with MARKER so CI can find and update one
sticky PR comment.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

MARKER = "<!-- kosha-relevance-compare -->"
DEFAULT_THRESHOLD = 1.0
DEFAULT_OVERRIDES: List[Tuple[str, float]] = [("*filtered_knn.*@0.1%.*", 2.0)]
# Deltas within this many points are float noise, not a change.
EPS = 1e-9

Metrics = Dict[str, Optional[float]]


class ReportError(Exception):
    pass


def load_report(path: Path) -> Tuple[Metrics, dict]:
    """(metric name -> value or None, whole report)."""
    try:
        report = json.loads(Path(path).read_text())
    except (OSError, ValueError) as e:
        raise ReportError(f"{path}: {e}") from e
    if not isinstance(report, dict) or report.get("kind") != "kosha-relevance":
        raise ReportError(f"{path}: not a kosha-relevance report")
    metrics = report.get("metrics")
    if not isinstance(metrics, dict):
        raise ReportError(f"{path}: no metrics")
    out: Metrics = {}
    for name, m in metrics.items():
        v = m.get("value") if isinstance(m, dict) else m
        out[name] = float(v) if isinstance(v, (int, float)) else None
    return out, report


def threshold_for(name: str, default: float, overrides: List[Tuple[str, float]]) -> float:
    limit = default
    for pattern, points in overrides:
        if fnmatch.fnmatchcase(name, pattern):
            limit = points
    return limit


def parse_override(text: str) -> Tuple[str, float]:
    pattern, sep, points = text.rpartition("=")
    if not sep or not pattern:
        raise argparse.ArgumentTypeError(f"want GLOB=POINTS, got {text!r}")
    try:
        return pattern, float(points)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{points!r} is not a number") from None


MODE_ORDER = {"lexical": 0, "vector": 1, "hybrid": 2}
POLICY_ORDER = {"probe": 0, "planned": 1}
_FILTERED = re.compile(r"^(ood\.)?filtered_knn\.(\w+)@([\d.]+)%\.(.*)$")


def sort_key(name: str) -> tuple:
    """Modes first (lexical, vector, hybrid), then filtered kNN by policy and
    descending selectivity; anything else last, by name. The reports store
    metrics in key order, which would put 0.1% before 100%."""
    mode = name.split(".", 1)[0]
    if mode in MODE_ORDER:
        return (0, MODE_ORDER[mode], name)
    m = _FILTERED.match(name)
    if m:
        ood, policy, selectivity, metric = m.groups()
        return (1, bool(ood), POLICY_ORDER.get(policy, 9), policy, -float(selectivity), metric)
    return (2, name)


def compare(
    base: Metrics,
    head: Metrics,
    default: float = DEFAULT_THRESHOLD,
    overrides: Optional[List[Tuple[str, float]]] = None,
) -> List[dict]:
    """One row per metric in either report, ordered by `sort_key`.

    status: ok | improved | regressed | missing (in base, gone or n/a in
    head; counts as a regression) | new (only in head).
    """
    overrides = DEFAULT_OVERRIDES if overrides is None else overrides
    names = sorted(set(head) | set(base), key=sort_key)
    rows = []
    for name in names:
        b, h = base.get(name), head.get(name)
        limit = threshold_for(name, default, overrides)
        delta = None
        if name not in base or b is None:
            status = "new" if h is not None or name not in base else "ok"
        elif h is None:
            status = "missing"
        else:
            delta = (h - b) * 100.0
            if delta < -limit - EPS:
                status = "regressed"
            elif delta > EPS:
                status = "improved"
            else:
                status = "ok"
        rows.append(
            {"name": name, "base": b, "head": h, "delta": delta, "limit": limit, "status": status}
        )
    return rows


def failures(rows: List[dict]) -> List[dict]:
    return [r for r in rows if r["status"] in ("regressed", "missing")]


def fmt_value(v: Optional[float]) -> str:
    return "n/a" if v is None else f"{v * 100:.2f}"


def fmt_delta(d: Optional[float]) -> str:
    if d is None:
        return ""
    if abs(d) <= EPS:
        return "0.00"
    return f"{d:+.2f}"


ICON = {"ok": "", "improved": "⬆️", "regressed": "❌", "missing": "❌ missing", "new": "🆕"}


def render_comment(
    rows: List[dict],
    *,
    base_label: str,
    head_label: str,
    against_baseline: bool = False,
    stale_baseline: Optional[List[str]] = None,
    head_report: Optional[dict] = None,
) -> str:
    bad = failures(rows)
    lines = [MARKER, "### Relevance & recall (`kosha-query` bench `relevance`)", ""]
    if bad:
        names = ", ".join(f"`{r['name']}`" for r in bad)
        lines.append(f"**❌ {len(bad)} metric(s) regressed past the threshold:** {names}")
    else:
        lines.append("**✅ No metric regressed past its threshold.**")
    lines.append("")
    if against_baseline:
        lines.append(
            f"> No base-tree report; head is compared with the committed "
            f"`scripts/relevance/baseline.json` ({base_label})."
        )
        lines.append("")
    lines += [
        f"Values and deltas in points (×100). Base `{base_label}` → head `{head_label}`. "
        "The harness is deterministic, so any non-zero delta is a real change.",
        "",
        "| metric | base | head | Δ | limit | |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for r in rows:
        lines.append(
            f"| `{r['name']}` | {fmt_value(r['base'])} | {fmt_value(r['head'])} "
            f"| {fmt_delta(r['delta'])} | −{r['limit']:g} | {ICON[r['status']]} |"
        )
    if head_report:
        fx = head_report.get("fixture", {})
        cfg = head_report.get("config", {})
        lines += [
            "",
            f"Fixture `{fx.get('name')}`: {fx.get('docs')} docs in {fx.get('segments')} "
            f"segments, {fx.get('judged_queries')} judged queries; kNN num_candidates "
            f"{cfg.get('num_candidates')}.",
        ]
        exact = head_report.get("vector_vs_exact")
        if exact:
            lines += [
                "",
                "**ANN vs exact on the head** (same embeddings and qrels; points ×100):",
                "",
                "| metric | ANN | exact | ANN − exact | queries |",
                "|---|---:|---:|---:|---:|",
            ]
            for name, scores in sorted(exact.items()):
                delta = scores["delta"]
                lines.append(
                    f"| `{name}` | {fmt_value(scores['ann'])} | {fmt_value(scores['exact'])} "
                    f"| {fmt_delta(None if delta is None else delta * 100)} | {scores['queries']} |"
                )
    if stale_baseline:
        shown = ", ".join(f"`{n}`" for n in stale_baseline[:6])
        more = "" if len(stale_baseline) <= 6 else f" and {len(stale_baseline) - 6} more"
        lines += [
            "",
            f"ℹ️ The committed `scripts/relevance/baseline.json` differs from the base run "
            f"({shown}{more}). Regenerate it when this PR intends to move these numbers "
            "(see scripts/relevance/README.md).",
        ]
    lines += [
        "",
        "<sub>Fails when a metric drops by more than its limit; thresholds are set in "
        "`.github/workflows/relevance-compare.yml`.</sub>",
        "",
    ]
    return "\n".join(lines)


def differing(a: Metrics, b: Metrics) -> List[str]:
    names = list(a) + [n for n in b if n not in a]
    out = []
    for n in names:
        x, y = a.get(n), b.get(n)
        if (x is None) != (y is None) or (x is not None and abs(x - y) > EPS):
            out.append(n)
    return out


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--base", type=Path, help="report from the base tree")
    ap.add_argument("--head", type=Path, help="report from the PR head")
    ap.add_argument(
        "--baseline",
        type=Path,
        help="committed baseline, used when --base is missing and checked for staleness",
    )
    ap.add_argument("--base-sha", default="base")
    ap.add_argument("--head-sha", default="head")
    ap.add_argument("--out", type=Path, help="write the markdown comment here")
    ap.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    ap.add_argument("--threshold-for", type=parse_override, action="append", default=None)
    ap.add_argument("--no-fail", action="store_true", help="always exit 0")
    ap.add_argument("--self-test", action="store_true", help="run the unit tests")
    args = ap.parse_args(argv)

    if args.self_test:
        import unittest

        sys.path.insert(0, str(Path(__file__).resolve().parent))
        suite = unittest.defaultTestLoader.loadTestsFromName("test_compare")
        ok = unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful()
        return 0 if ok else 1
    if args.head is None:
        ap.error("--head is required")

    head, head_report = load_report(args.head)
    base: Optional[Metrics] = None
    if args.base is not None:
        try:
            base, _ = load_report(args.base)
        except ReportError as e:
            print(f"warning: base report unusable ({e})", file=sys.stderr)
    baseline: Optional[Metrics] = None
    if args.baseline is not None:
        try:
            baseline, _ = load_report(args.baseline)
        except ReportError as e:
            print(f"warning: committed baseline unusable ({e})", file=sys.stderr)

    against_baseline = base is None
    base_label = args.base_sha
    if against_baseline:
        if baseline is None:
            print("error: neither a base report nor a committed baseline", file=sys.stderr)
            return 2
        base, base_label = baseline, "committed baseline"
    overrides = list(DEFAULT_OVERRIDES)
    if args.threshold_for:
        overrides += args.threshold_for
    rows = compare(base, head, args.threshold, overrides)
    stale = None
    if not against_baseline and baseline is not None:
        stale = differing(baseline, base) or None
    comment = render_comment(
        rows,
        base_label=base_label,
        head_label=args.head_sha,
        against_baseline=against_baseline,
        stale_baseline=stale,
        head_report=head_report,
    )
    if args.out:
        args.out.write_text(comment)
    print(comment)
    bad = failures(rows)
    if bad and not args.no_fail:
        for r in bad:
            print(
                f"::error::relevance regression: {r['name']} {fmt_value(r['base'])} -> "
                f"{fmt_value(r['head'])} ({fmt_delta(r['delta']) or r['status']}; limit "
                f"-{r['limit']:g} points)",
                file=sys.stderr,
            )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
