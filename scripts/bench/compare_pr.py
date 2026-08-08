#!/usr/bin/env python3
"""Render the PR bench-compare comment from two segment_memory JSON reports.

Used by .github/workflows/bench-compare.yml: the workflow runs
`cargo bench -p kosha-query --bench segment_memory` twice on the same runner
— once at the PR's merge-base with main ("before") and once at the PR head
("after"), each with KOSHA_BENCH_JSON=<path> — then calls this script to turn
the two reports into one markdown comment with before/after p50/p90/p99 for
the cold and warm query shapes, posted (sticky, marker-keyed) on the PR.

Only the v2 (lazy, current) format's numbers are compared; v1 exists in the
report solely for the format-migration table printed by the bench itself.

Exit code is always 0 when the comment renders — a missing/unparseable
*base* report degrades to a PR-only table (the merge-base may predate JSON
support in the bench); a bad *head* report is a hard error.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

MARKER = "<!-- kosha-bench-compare -->"

# (json key path under formats.v2, display label). Order = table order.
METRICS = [
    (("cold_broad_ms",), 'cold broad ("the")'),
    (("warm_ms", "broad"), 'warm broad ("the")'),
    (("warm_ms", "two_term_and"), "warm 2-term AND"),
    (("warm_ms", "phrase"), "warm phrase"),
    (("warm_ms", "wildcard_w1"), "warm wildcard w1*"),
]

PERCENTILES = ["p50", "p90", "p99"]

# |Δ| above this fraction gets a flag emoji; below it, deltas on a shared CI
# runner are mostly noise.
FLAG_THRESHOLD = 0.10


def load_report(path: str | Path) -> dict | None:
    """Parse a bench JSON report; None if the file is absent or malformed."""
    try:
        report = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(report, dict) or "formats" not in report:
        return None
    return report


def dig(report: dict | None, *keys: str):
    node = report
    for key in keys:
        if not isinstance(node, dict) or key not in node:
            return None
        node = node[key]
    return node


def fmt_ms(value) -> str:
    if value is None:
        return "–"
    if value >= 100:
        return f"{value:.0f} ms"
    if value >= 10:
        return f"{value:.1f} ms"
    if value >= 1:
        return f"{value:.2f} ms"
    return f"{value:.3f} ms"


def fmt_delta(before, after) -> str:
    """Signed percent change after→before, flagged outside the noise band."""
    if before is None or after is None or before <= 0:
        return "–"
    frac = (after - before) / before
    cell = f"{frac * 100:+.1f}%"
    if frac > FLAG_THRESHOLD:
        cell += " ⚠️"
    elif frac < -FLAG_THRESHOLD:
        cell += " 🟢"
    return cell


def corpus_line(report: dict | None) -> str:
    corpus = dig(report, "corpus") or {}
    segs, docs = corpus.get("segs", "?"), corpus.get("docs", "?")
    return f"{segs} segs × {docs} docs"


def render_comment(
    base: dict | None,
    head: dict,
    *,
    base_sha: str,
    head_sha: str,
) -> str:
    base_v2 = dig(base, "formats", "v2")
    head_v2 = dig(head, "formats", "v2")
    if head_v2 is None:
        raise ValueError("head report has no formats.v2 section")

    lines = [
        MARKER,
        "### ⏱ Microbench — main vs this PR (`segment_memory`, v2 format)",
        "",
        f"**before** = merge-base `{base_sha[:9]}` · **after** = PR head"
        f" `{head_sha[:9]}` · same runner, back to back",
        "",
    ]

    if base_v2 is None:
        lines += [
            "> ⚠️ No baseline report from the merge-base (it predates JSON"
            " support in the bench, or its run failed) — PR-only numbers"
            " below; the next PR after this lands will have a baseline.",
            "",
        ]
    elif dig(base, "corpus") != dig(head, "corpus"):
        lines += [
            f"> ⚠️ Corpus mismatch — before: {corpus_line(base)},"
            f" after: {corpus_line(head)}. Deltas are not comparable.",
            "",
        ]

    lines += [
        "| metric | pctl | before (main) | after (PR) | Δ |",
        "|:--|:--|--:|--:|--:|",
    ]

    for key_path, label in METRICS:
        base_dist = dig(base_v2, *key_path) or {}
        head_dist = dig(head_v2, *key_path) or {}
        for i, pct in enumerate(PERCENTILES):
            before, after = base_dist.get(pct), head_dist.get(pct)
            name = f"**{label}**" if i == 0 else ""
            lines.append(
                f"| {name} | {pct} | {fmt_ms(before)} | {fmt_ms(after)}"
                f" | {fmt_delta(before, after)} |"
            )

    base_bytes = dig(base_v2, "open_bytes")
    head_bytes = dig(head_v2, "open_bytes")
    mib = lambda b: "–" if b is None else f"{b / 1048576:.1f} MiB"  # noqa: E731
    lines.append(
        f"| **resident while open** | — | {mib(base_bytes)} | {mib(head_bytes)}"
        f" | {fmt_delta(base_bytes, head_bytes)} |"
    )

    cold_n = dig(head_v2, "cold_broad_ms", "n") or "?"
    warm_n = dig(head_v2, "warm_ms", "broad", "n") or "?"
    lines += [
        "",
        f"<sub>corpus {corpus_line(head)} · cold n={cold_n}, warm n={warm_n}"
        " · nearest-rank percentiles · deltas within ±10% are usually runner"
        " noise</sub>",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="baseline (merge-base) JSON report")
    parser.add_argument("--head", required=True, help="PR-head JSON report")
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--out", required=True, help="markdown comment output path")
    args = parser.parse_args()

    head = load_report(args.head)
    if head is None:
        print(f"error: head report {args.head} missing or malformed", file=sys.stderr)
        return 1

    comment = render_comment(
        load_report(args.base),
        head,
        base_sha=args.base_sha,
        head_sha=args.head_sha,
    )
    Path(args.out).write_text(comment)
    print(f"wrote {args.out} ({len(comment)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
