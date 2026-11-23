#!/usr/bin/env python3
"""Summarize the per-query `search timing:` lines of a bench server log
(research #380): mean and percentiles of the kNN cost counters, so two
segment layouts can be compared at matched recall.

Input is a per-phase server log as run_phases.sh archives it
(`<suite>/server/<phase>.log.gz`, from `docker logs --timestamps`), gzip
or plain text. Only `search timing:` lines are read; every `key=value`
token on them is a candidate column.

Usage:
    python3 knn_log_stats.py vector/server/knn_warm_nocache.log.gz
    python3 knn_log_stats.py vector/server/knn_cold_nocache.log.gz --skip-first-s 180
    python3 knn_log_stats.py LOG --keys knn_postings,knn_segments --json

--skip-first-s drops the lines of the first N seconds after the first
`search timing:` line (needs the docker timestamp prefix), to read a cold
phase's start-up convoy apart from the rest.
"""

import argparse
import datetime as dt
import gzip
import json
import sys

DEFAULT_KEYS = [
    "total_ms",
    "score_ms",
    "knn_plan_ms",
    "knn_read_ms",
    "knn_decode_ms",
    "knn_dot_ms",
    "knn_rank_ms",
    "knn_postings",
    "knn_postings_cached",
    "knn_segments",
    "knn_centroids",
    "knn_plan_exact_centroids",
    "opens_cold",
    "opens_cached",
    "open_total_ms",
    "hydrate_files",
    "hydrate_mb",
    "s3_bytes",
    "vector_gets",
    "postings_gets",
]
MARKER = "search timing:"


def parse_ts(token):
    # docker --timestamps: 2026-11-21T06:23:26.123456789Z
    if not token or not token[0].isdigit() or "T" not in token:
        return None
    base = token.rstrip("Z")
    frac = ""
    if "." in base:
        base, frac = base.split(".", 1)
    try:
        t = dt.datetime.strptime(base, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None
    seconds = t.replace(tzinfo=dt.timezone.utc).timestamp()
    if frac:
        seconds += float("0." + frac[:9])
    return seconds


def parse_line(line):
    """(timestamp or None, {key: float}) for a `search timing:` line."""
    head, _, tail = line.partition(MARKER)
    fields = {}
    for token in tail.split():
        key, sep, value = token.partition("=")
        if not sep:
            continue
        try:
            fields[key] = float(value)
        except ValueError:
            pass
    first = head.split(maxsplit=1)
    return (parse_ts(first[0]) if first else None), fields


def percentile(sorted_values, q):
    if not sorted_values:
        return float("nan")
    idx = min(len(sorted_values) - 1, max(0, int(round(q * (len(sorted_values) - 1)))))
    return sorted_values[idx]


def summarize(rows, keys):
    out = {}
    for key in keys:
        values = sorted(r[key] for r in rows if key in r)
        if not values:
            continue
        out[key] = {
            "n": len(values),
            "mean": sum(values) / len(values),
            "p50": percentile(values, 0.50),
            "p90": percentile(values, 0.90),
            "p99": percentile(values, 0.99),
            "max": values[-1],
        }
    return out


def read_rows(path, skip_first_s):
    opener = gzip.open if path.endswith(".gz") else open
    rows, t0, skipped, untimed = [], None, 0, 0
    with opener(path, "rt", errors="replace") as fh:
        for line in fh:
            if MARKER not in line:
                continue
            ts, fields = parse_line(line)
            if skip_first_s:
                if ts is None:
                    untimed += 1
                    continue
                if t0 is None:
                    t0 = ts
                if ts - t0 < skip_first_s:
                    skipped += 1
                    continue
            rows.append(fields)
    return rows, skipped, untimed


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("log", help="server log (.log or .log.gz)")
    ap.add_argument("--keys", default=",".join(DEFAULT_KEYS),
                    help="comma-separated key=value fields to summarize")
    ap.add_argument("--skip-first-s", type=float, default=0.0,
                    help="drop lines in the first N seconds of the phase")
    ap.add_argument("--json", action="store_true", help="print JSON instead of a table")
    args = ap.parse_args(argv)
    keys = [k for k in args.keys.split(",") if k]
    rows, skipped, untimed = read_rows(args.log, args.skip_first_s)
    stats = summarize(rows, keys)
    if args.json:
        print(json.dumps({"log": args.log, "queries": len(rows), "skipped": skipped,
                          "untimed": untimed, "stats": stats}, indent=1))
        return 0
    print(f"{args.log}: {len(rows)} search timing lines"
          + (f" ({skipped} in the first {args.skip_first_s:g}s skipped)" if args.skip_first_s else "")
          + (f", {untimed} without a timestamp dropped" if untimed else ""))
    print("| key | n | mean | p50 | p90 | p99 | max |")
    print("|---|---|---|---|---|---|---|")
    for key in keys:
        s = stats.get(key)
        if s is None:
            print(f"| {key} | 0 | - | - | - | - | - |")
            continue
        print(f"| {key} | {s['n']} | {s['mean']:.2f} | {s['p50']:.2f} | {s['p90']:.2f} "
              f"| {s['p99']:.2f} | {s['max']:.2f} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
