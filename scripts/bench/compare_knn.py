#!/usr/bin/env python3
"""Compare kNN microbench reports (main vs PR) and gate on large regressions.

    python3 scripts/bench/compare_knn.py \
        --base base1.json --base base2.json --head head1.json --head head2.json \
        --base-sha SHA --head-sha SHA --out section.md [--accept]

Inputs are what `cargo bench -p kosha-query --bench knn_open_search` writes
to KOSHA_BENCH_JSON. bench-compare.yml runs base and head alternately
(base, head, base, head) on one runner; for each latency the gate uses the
lowest p50 of each side's runs, since runner interference only ever adds
time. p90/p99 are shown but never gated: on a shared runner they are noise.

A PR fails when, against main:

  * a gated p50 grows by more than its limit AND by more than its absolute
    floor (so a 0.1 ms bench can't fail on a 30% wobble);
  * the postings probed per query grow by more than POSTINGS_LIMIT. The
    corpus and queries are fixed, so this count is deterministic: it is how
    a probe-budget change (the #160 floor that cost 4x p50 at 10M) shows up
    even when the small corpus hides the latency.

A change in the warm kNN top-k digest is reported, never gated: a ranking
change may be intended, and relevance-compare judges quality.

`--accept` (the `perf-regression-accepted` PR label) keeps the report but
exits 0. Without a usable base report there is nothing to gate: exit 0.
Exit status 1 = regression, 2 = bad head report.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

MARKER = "<!-- kosha-knn-bench-compare -->"

# (label, key path to a {n,p50,p90,p99} dist, max p50 growth, abs floor ms)
# Floors sized for the CI runner, where warm kNN p50 is ~0.25 ms: 0.05 ms
# there is 20%, so the % limit is what bites, not the floor.
LATENCIES = [
    ("warm kNN", ("warm_ms", "knn"), 0.25, 0.05),
    ("warm hybrid", ("warm_ms", "hybrid_broad"), 0.25, 0.05),
    ("cold kNN", ("cold_knn_ms",), 0.40, 0.3),
    ("open all segments", ("open_ms",), 0.40, 0.3),
]
POSTINGS_LIMIT = 0.10
POSTINGS_PATH = ("warm_knn_scan", "postings_per_query")
DIGEST_PATH = ("warm_knn_phases", "topk_digest")


def load(path: Path) -> Optional[dict]:
    try:
        report = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(report, dict) or "warm_ms" not in report:
        return None
    return report


def dig(node, path):
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return None
        node = node[key]
    return node


def best(reports: List[dict], path, pct: str) -> Optional[float]:
    values = [dig(r, path + (pct,)) for r in reports]
    values = [v for v in values if isinstance(v, (int, float))]
    return min(values) if values else None


def first(reports: List[dict], path):
    for r in reports:
        v = dig(r, path)
        if v is not None:
            return v
    return None


def ms(v: Optional[float]) -> str:
    if v is None:
        return "–"
    return f"{v:.2f} ms" if v < 10 else f"{v:.1f} ms"


def pct(before: Optional[float], after: Optional[float]) -> Optional[float]:
    if before is None or after is None or before <= 0:
        return None
    return (after - before) / before


def compare(base: List[dict], head: List[dict]) -> List[dict]:
    """One row per gated number: name, base, head, frac, limit, failed."""
    rows = []
    for label, path, limit, floor in LATENCIES:
        b, h = best(base, path, "p50"), best(head, path, "p50")
        frac = pct(b, h)
        failed = frac is not None and frac > limit and (h - b) > floor
        rows.append(
            {
                "name": f"{label} p50",
                "base": b,
                "head": h,
                "frac": frac,
                "limit": limit,
                "failed": failed,
                "fmt": ms,
                "tails": [(p, best(base, path, p), best(head, path, p)) for p in ("p90", "p99")],
            }
        )
    b, h = first(base, POSTINGS_PATH), first(head, POSTINGS_PATH)
    frac = pct(b, h)
    rows.append(
        {
            "name": "postings probed / query",
            "base": b,
            "head": h,
            "frac": frac,
            "limit": POSTINGS_LIMIT,
            "failed": frac is not None and frac > POSTINGS_LIMIT,
            "fmt": lambda v: "–" if v is None else f"{v:.1f}",
            "tails": [],
        }
    )
    return rows


def render(
    rows: List[dict],
    base: List[dict],
    head: List[dict],
    *,
    base_sha: str,
    head_sha: str,
    accepted: bool,
) -> str:
    bad = [r for r in rows if r["failed"]]
    lines = [MARKER, "### kNN microbench — main vs this PR (`knn_open_search`)", ""]
    if not base:
        lines.append("> ⚠️ No baseline report from main: PR-only numbers, nothing gated.")
    elif bad:
        verdict = "accepted by the `perf-regression-accepted` label" if accepted else "fails"
        names = ", ".join(f"`{r['name']}`" for r in bad)
        lines.append(f"**❌ Regression past the limit ({verdict}):** {names}")
    else:
        lines.append("**✅ No kNN regression past its limit.**")
    lines += [
        "",
        f"before = main `{base_sha[:9]}` · after = PR `{head_sha[:9]}` · same runner, "
        f"runs interleaved ({len(base)}× / {len(head)}×), best p50 of each side",
        "",
        "| metric | before | after | Δ | limit | |",
        "|:--|--:|--:|--:|--:|:--|",
    ]
    for r in rows:
        fmt = r["fmt"]
        delta = "–" if r["frac"] is None else f"{r['frac'] * 100:+.1f}%"
        mark = "❌" if r["failed"] else ""
        lines.append(
            f"| **{r['name']}** | {fmt(r['base'])} | {fmt(r['head'])} | {delta} "
            f"| +{r['limit'] * 100:.0f}% | {mark} |"
        )
        for p, b, h in r["tails"]:
            d = pct(b, h)
            lines.append(
                f"| {p} (not gated) | {ms(b)} | {ms(h)} | "
                f"{'–' if d is None else f'{d * 100:+.1f}%'} | | |"
            )
    bd, hd = first(base, DIGEST_PATH), first(head, DIGEST_PATH)
    corpus = first(head, ("corpus",)) or {}
    lines += [""]
    if bd is not None and hd is not None and bd != hd:
        lines.append(
            f"ℹ️ Warm kNN top-k changed (digest `{bd}` → `{hd}`). Intended? "
            "relevance-compare shows whether recall moved."
        )
        lines.append("")
    lines += [
        f"<sub>corpus {corpus.get('segs')} segs × {corpus.get('docs')} docs, dim "
        f"{corpus.get('dim')}, {corpus.get('clusters')} clusters · latency fails only past "
        "both the % limit and an absolute floor · postings/query is deterministic · "
        "label `perf-regression-accepted` to accept an intended cost</sub>",
        "",
    ]
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--base", type=Path, action="append", default=[])
    ap.add_argument("--head", type=Path, action="append", default=[], required=True)
    ap.add_argument("--base-sha", default="base")
    ap.add_argument("--head-sha", default="head")
    ap.add_argument("--out", type=Path)
    ap.add_argument("--accept", action="store_true", help="report, but exit 0")
    args = ap.parse_args(argv)

    head = [r for r in map(load, args.head) if r is not None]
    if not head:
        print("error: no usable head report", file=sys.stderr)
        return 2
    base = [r for r in map(load, args.base) if r is not None]
    if base and first(base, ("corpus",)) != first(head, ("corpus",)):
        print("warning: corpus differs between base and head; not gating", file=sys.stderr)
        base = []
    rows = compare(base, head) if base else compare([], head)
    text = render(
        rows, base, head, base_sha=args.base_sha, head_sha=args.head_sha, accepted=args.accept
    )
    if args.out:
        args.out.write_text(text)
    print(text)
    bad = [r for r in rows if r["failed"]]
    if bad and not args.accept:
        for r in bad:
            print(
                f"::error::kNN bench regression: {r['name']} {r['fmt'](r['base'])} -> "
                f"{r['fmt'](r['head'])} (limit +{r['limit'] * 100:.0f}%)",
                file=sys.stderr,
            )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
