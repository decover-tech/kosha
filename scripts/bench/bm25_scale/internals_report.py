#!/usr/bin/env python3
"""Markdown tables of engine internals for every phase summary under a
results directory: the `internals` block query_bench.py writes
(bench_internals.py). Printed under the latency table in the benchmark
workflow's job summary, so every run publishes where its time went next
to its p50/p90/p99.

Three tables:
1. Where the time goes: server p50 and the time outside the engine, then
   each stage's p50 with its share of mean server time.
2. Tail vs median: mean stage times for the slowest 1% of requests next to
   the middle band, which says what a p99 is made of.
3. Work and resources: scoring blocks decoded per request and per hit, S3
   rounds and MB per request, the fully-cached and result-cache rates,
   server CPU (cores, ms per request), peak RSS and the client's send lag.

Phases without an `internals` block (older runs, --no-internals) are
skipped; prints nothing when no phase has one.

Usage: internals_report.py <results-dir>
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

STAGES = ["queue_ms", "admit_ms", "hydrate_ms", "bm25_stats_ms", "score_ms", "materialize_ms"]
SHORT = {"queue_ms": "queue", "admit_ms": "admit", "hydrate_ms": "hydrate",
         "bm25_stats_ms": "bm25 stats", "score_ms": "score", "materialize_ms": "materialize"}


def num(v, digits=1):
    if v is None or v != v:
        return "–"
    return f"{v:,.{digits}f}"


def pct(v):
    return "–" if v is None or v != v else f"{v * 100:.0f}%"


def phases(root: Path):
    for f in sorted(root.rglob("*.json")):
        try:
            d = json.loads(f.read_text())
        except (OSError, ValueError):
            continue
        if isinstance(d, dict) and isinstance(d.get("internals"), dict):
            yield f.relative_to(root).with_suffix("").as_posix(), d["internals"]


def render(root: Path) -> str:
    rows = list(phases(root))
    if not rows:
        return ""
    out = ["## Engine internals", "",
           "Server-side stage timings per request (`timings: true`), /v1/stats counter "
           "deltas across each phase, and the kosha-server process sampled from /proc. "
           "Stage cells are p50 ms (share of mean server time).", ""]

    timed = [(n, i) for n, i in rows if i.get("stages")]
    if timed:
        out += ["### Where the time goes", "",
                "| phase | server p50 | outside engine p50 | "
                + " | ".join(SHORT[s] for s in STAGES) + " | other |",
                "|---|---|---|" + "---|" * (len(STAGES) + 1)]
        for n, i in timed:
            st = i["stages"]
            cells = []
            for s in STAGES:
                d = st.get(s)
                cells.append(f"{num(d['p50'], 2)} ({pct(d.get('share'))})" if d else "–")
            other = st.get("other_ms")
            cells.append(f"{num(other['mean'], 2)} ({pct(other['share'])})" if other else "–")
            out.append(f"| {n} | {num(i['server_total_ms']['p50'], 2)} | "
                       f"{num(i['client_overhead_ms']['p50'], 2)} | " + " | ".join(cells) + " |")

        out += ["", "### Tail (slowest 1%) vs median band, mean ms", "",
                "| phase | band | requests | client | server | "
                + " | ".join(SHORT[s] for s in STAGES) + " | S3 rounds |",
                "|---|---|---|---|---|" + "---|" * (len(STAGES) + 1)]
        for n, i in timed:
            for label, key in (("median", "median_band"), ("p99+", "tail_band")):
                b = i.get(key) or {}
                out.append(f"| {n} | {label} | {b.get('requests', '–')} | {num(b.get('client_ms'), 2)} | "
                           f"{num(b.get('total_ms'), 2)} | "
                           + " | ".join(num(b.get(s), 2) for s in STAGES)
                           + f" | {num(b.get('s3_rounds'), 2)} |")

    out += ["", "### Work and resources", "",
            "| phase | blocks/req | blocks/hit | S3 rounds/req | S3 MB/req | fully cached | "
            "result cache | CPU cores p50/p90 | CPU ms/req | RSS max MB | send lag p99 ms |",
            "|---|---|---|---|---|---|---|---|---|---|---|"]
    for n, i in rows:
        blocks = (i.get("counters") or {}).get("scoring_blocks_decoded") or {}
        res = i.get("resources") or {}
        cores = res.get("server_cpu_cores") or {}
        rounds = i.get("s3_rounds_per_request") or {}
        lag = i.get("send_lag_ms") or {}
        out.append(
            f"| {n} | {num(blocks.get('per_request'), 0)} | {num(blocks.get('per_hit'), 0)} | "
            f"{num(rounds.get('mean'), 2)} | {num(i.get('s3_mb_per_request'), 2)} | "
            f"{pct(i.get('fully_cached_rate'))} | {pct(i.get('result_cache_hit_rate'))} | "
            f"{num(cores.get('p50'), 2)} / {num(cores.get('p90'), 2)} | "
            f"{num(res.get('server_cpu_ms_per_request'), 2)} | "
            f"{num((res.get('server_rss_mb') or {}).get('max'), 0)} | {num(lag.get('p99'), 2)} |")
    return "\n".join(out) + "\n"


def main():
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    sys.stdout.write(render(Path(sys.argv[1])))


if __name__ == "__main__":
    main()
