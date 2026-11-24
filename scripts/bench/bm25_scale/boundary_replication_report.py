#!/usr/bin/env python3
"""Plot #457's recall/scan curves and report the smallest measured target-recall point."""

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def scans(row):
    return float(row["mean_scanned"])


def at_target(rows, target):
    return min(
        (row for row in rows if float(row["recall_at_10"]) >= target),
        key=scans,
        default=None,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path, nargs="+")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--target", type=float, default=0.95)
    args = parser.parse_args()
    curves = defaultdict(lambda: defaultdict(list))
    for path in args.csv:
        with path.open(newline="") as f:
            for row in csv.DictReader(f):
                panel = (row["dataset"], row["layout"], row["encoding"])
                variant = (row["assignment"], row["max_copies"], row["spill_fraction"])
                curves[panel][variant].append(row)
    if not curves:
        parser.error("no curves in the supplied CSVs")

    height = (len(curves) + 1) // 2
    fig, axes = plt.subplots(height, 2, figsize=(13, 3.6 * height), squeeze=False)
    for ax, (panel, variants) in zip(axes.flat, sorted(curves.items())):
        ax.set(
            title=" / ".join(panel),
            xlabel="Mean posting entries scanned per query",
            ylabel="Mean recall@10",
            xscale="log",
        )
        ax.axhline(args.target, color="0.7", linestyle=":")
        baseline = variants[("baseline", "1", "0")]
        baseline_point = at_target(baseline, args.target)
        base_bytes = int(baseline[0]["index_bytes"])
        print(" / ".join(panel))
        for (method, copies, fraction), rows in sorted(variants.items()):
            rows.sort(key=scans)
            label = "baseline" if method == "baseline" else f"{method} {copies} copies (cap {fraction})"
            ax.plot([scans(r) for r in rows], [float(r["recall_at_10"]) for r in rows], ".-", label=label)
            point = at_target(rows, args.target)
            growth = int(rows[0]["index_bytes"]) / base_bytes - 1
            if point is None:
                best = max(float(r["recall_at_10"]) for r in rows)
                print(f"  {label}: target not reached (best {best:.4f}); index {growth:+.1%}")
                continue
            ratio = (
                f"{scans(baseline_point) / scans(point):.2f}x baseline/variant"
                if baseline_point else "baseline did not reach target"
            )
            print(
                f"  {label}: {scans(point):,.0f} scanned ({ratio}); "
                f"p10 recall {point['p10_recall']}; index {growth:+.1%}"
            )
        ax.legend(fontsize="small")
    for ax in list(axes.flat)[len(curves):]:
        ax.set_visible(False)
    fig.tight_layout()
    fig.savefig(args.out)


if __name__ == "__main__":
    main()
