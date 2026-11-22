#!/usr/bin/env python3
"""Tabulate a kNN probe-budget sweep (#384): recall@10 vs latency vs
postings probed, one row per sweep point.

Reads the directory knn_budget_sweep.sh writes:

    <sweep-dir>/<label>/knobs.env                         server knobs of the point
    <sweep-dir>/<label>/knn_warm_nocache.json             query_bench.py summary
    <sweep-dir>/<label>/knn_warm_nocache.requests.jsonl.gz  per request (server timings)
    <sweep-dir>/<label>/recall.json                       recall_check.py summary
    <sweep-dir>/<label>/server/knn_warm_nocache.log.gz    server log of the phase

Postings probed per query come from the phase's per-request server timings
(`timings.knn_postings`, #384 servers); when absent (older server), from
the `knn_postings=` field of the phase's `search timing:` log lines.

Writes <prefix>.json, <prefix>.md (the table, plus the #384 success-bar
check against the baseline point) and <prefix>.csv, and <prefix>.png with
recall@10 against p50/p99 when matplotlib is importable (optional).

Success bar (issue #384): >= 2x fewer postings probed than the baseline
at equal mean recall@10 (within --recall-tolerance), with p10 recall not
worse.

Usage: knn_sweep_report.py <sweep-dir> [--baseline f0.05] [--out-prefix DIR/sweep]
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import statistics
from pathlib import Path

LOG_POSTINGS = re.compile(r"search timing: .*?\bknn_postings=(\d+)")


def pct(xs, p):
    if not xs:
        return float("nan")
    s = sorted(xs)
    k = (len(s) - 1) * (p / 100)
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def read_jsonl_gz(path: Path) -> list[dict]:
    if not path.exists():
        return []
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as f:
        return [json.loads(line) for line in f if line.strip()]


def log_postings(path: Path) -> list[int]:
    if not path.exists():
        return []
    opener = gzip.open if path.suffix == ".gz" else open
    out = []
    with opener(path, "rt", errors="replace") as f:
        for line in f:
            m = LOG_POSTINGS.search(line)
            if m:
                out.append(int(m.group(1)))
    return out


def point_row(d: Path) -> dict | None:
    phase = d / "knn_warm_nocache.json"
    recall = d / "recall.json"
    if not phase.exists() and not recall.exists():
        return None
    row: dict = {"label": d.name}
    knobs = d / "knobs.env"
    row["knobs"] = " ".join(knobs.read_text().split()) if knobs.exists() else ""
    if phase.exists():
        p = json.loads(phase.read_text())
        row.update(
            p50_ms=p.get("p50_ms"), p90_ms=p.get("p90_ms"), p99_ms=p.get("p99_ms"),
            requests=p.get("requests"), errors=p.get("errors"),
        )
    recs = read_jsonl_gz(d / "knn_warm_nocache.requests.jsonl.gz")
    postings = [
        r["server"]["knn_postings"]
        for r in recs
        if isinstance(r.get("server"), dict) and r["server"].get("knn_postings") is not None
    ]
    source = "timings"
    if not postings:
        postings = log_postings(d / "server" / "knn_warm_nocache.log.gz")
        source = "log" if postings else "none"
    row["postings_source"] = source
    row["postings_mean"] = statistics.fmean(postings) if postings else float("nan")
    row["postings_p50"] = pct(postings, 50)
    row["postings_p99"] = pct(postings, 99)
    if recall.exists():
        r = json.loads(recall.read_text())
        row["recall_mean"] = r.get("mean_recall_at_k")
        row["recall_p10"] = r.get("p10_recall_at_k")
        row["recall_dist_mean"] = (r.get("recall_dist_at_k") or {}).get("mean")
        row["recall_eps_mean"] = (r.get("recall_eps_at_k") or {}).get("mean")
        row["recall_sample"] = r.get("sample_size")
        kp = r.get("knn_postings") or {}
        row["recall_postings_mean"] = kp.get("mean")
    return row


def fmt(v, nd=1):
    if v is None:
        return "-"
    if isinstance(v, float):
        return "NaN" if v != v else f"{v:.{nd}f}"
    return str(v)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sweep_dir", type=Path)
    ap.add_argument("--baseline", default="f0.05", help="label of the baseline point (default f0.05)")
    ap.add_argument("--recall-tolerance", type=float, default=0.005, help="'equal recall' slack on mean recall@10")
    ap.add_argument("--out-prefix", type=Path, default=None)
    args = ap.parse_args()

    rows = [r for d in sorted(p for p in args.sweep_dir.iterdir() if p.is_dir()) if (r := point_row(d))]
    if not rows:
        raise SystemExit(f"no sweep points under {args.sweep_dir}")
    base = next((r for r in rows if r["label"] == args.baseline), None)
    for r in rows:
        if base and base.get("postings_mean") and r.get("postings_mean") == r.get("postings_mean"):
            r["postings_vs_baseline"] = r["postings_mean"] / base["postings_mean"]
        if base and r.get("recall_mean") is not None and base.get("recall_mean") is not None:
            r["meets_bar"] = bool(
                r.get("postings_vs_baseline", 1.0) <= 0.5
                and r["recall_mean"] >= base["recall_mean"] - args.recall_tolerance
                and (r.get("recall_p10") or 0.0) >= (base.get("recall_p10") or 0.0)
            )

    cols = [
        ("label", 0), ("p50_ms", 1), ("p90_ms", 1), ("p99_ms", 1), ("recall_mean", 4),
        ("recall_p10", 2), ("recall_dist_mean", 4), ("recall_eps_mean", 4),
        ("postings_mean", 0), ("postings_p99", 0), ("postings_vs_baseline", 2),
        ("meets_bar", 0), ("postings_source", 0), ("knobs", 0),
    ]
    lines = [
        "## kNN probe-budget sweep (#384)",
        "",
        f"Baseline point: `{args.baseline}`. Bar: postings <= 0.5x baseline, mean recall@10 "
        f">= baseline - {args.recall_tolerance}, p10 recall >= baseline.",
        "",
        "| " + " | ".join(c for c, _ in cols) + " |",
        "|" + "---|" * len(cols),
    ]
    for r in rows:
        lines.append("| " + " | ".join(fmt(r.get(c), nd) for c, nd in cols) + " |")
    md = "\n".join(lines) + "\n"
    print(md)

    if args.out_prefix:
        prefix = args.out_prefix
        prefix.with_suffix(".json").write_text(json.dumps(rows, indent=2))
        prefix.with_suffix(".md").write_text(md)
        with prefix.with_suffix(".csv").open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=[c for c, _ in cols], extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        try:
            import matplotlib

            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            fig, ax = plt.subplots(figsize=(6, 4))
            pts = [r for r in rows if r.get("recall_mean") is not None and r.get("p50_ms") is not None]
            for key, marker in (("p50_ms", "o"), ("p99_ms", "s")):
                ax.plot([r[key] for r in pts], [r["recall_mean"] for r in pts], marker=marker, linestyle="", label=key)
                for r in pts:
                    ax.annotate(r["label"], (r[key], r["recall_mean"]), fontsize=7)
            ax.set_xlabel("latency (ms)")
            ax.set_ylabel("mean recall@10")
            ax.legend()
            fig.tight_layout()
            fig.savefig(prefix.with_suffix(".png"), dpi=120)
        except ImportError:
            print("matplotlib not installed: no plot (the table and CSV carry the curve)")
        print(f"wrote {prefix}.json/.md/.csv")


if __name__ == "__main__":
    main()
