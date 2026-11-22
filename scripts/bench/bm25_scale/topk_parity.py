#!/usr/bin/env python3
"""Ranking parity between two benchmark phases (#385).

Compares the ranked results that two `query_bench.py --raw-out ... --raw-ids`
logs recorded per query (`qidx`), for example the same phase run against an
ingest-order snapshot and a docid-reordered one. Reordering must not change
results, so every query answered in both logs must return the same doc_ids
in the same order, with scores equal within --score-rtol.

A query repeats within a phase (the queries file is cycled). If the
repetitions of one query disagree within a single log, that is reported as
unstable: the log is then not a clean baseline.

Exits 0 only when at least one query was compared and none differ. A log
recorded without --raw-ids is an error, never a pass.

Usage: topk_parity.py <a>.requests.jsonl.gz <b>.requests.jsonl.gz
           [--score-rtol 1e-6] [--show 10] [--out parity.json]
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path


def load(path: Path) -> tuple[dict[int, tuple[list, list]], int, list[int]]:
    """qidx -> (ids, scores) of its first successful response, the number
    of successful records, and the qidxs whose repetitions disagreed."""
    opener = gzip.open if str(path).endswith(".gz") else open
    first: dict[int, tuple[list, list]] = {}
    unstable: set[int] = set()
    ok = 0
    with opener(path, "rt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if "error" in rec:
                continue
            if "ids" not in rec:
                raise SystemExit(
                    f"{path}: a record has no ids; re-run query_bench.py with --raw-ids"
                )
            ok += 1
            q = rec["qidx"]
            got = (rec["ids"], rec.get("scores") or [])
            if q not in first:
                first[q] = got
            elif first[q][0] != got[0]:
                unstable.add(q)
    return first, ok, sorted(unstable)


def scores_close(a: list, b: list, rtol: float) -> bool:
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        if x is None or y is None:
            if x != y:
                return False
            continue
        if abs(x - y) > rtol * max(abs(x), abs(y), 1e-12):
            return False
    return True


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("a", type=Path)
    ap.add_argument("b", type=Path)
    ap.add_argument("--score-rtol", type=float, default=1e-6)
    ap.add_argument("--show", type=int, default=10, help="mismatches to print")
    ap.add_argument("--out", type=Path, default=None, help="write the summary JSON here")
    args = ap.parse_args()

    a, a_ok, a_unstable = load(args.a)
    b, b_ok, b_unstable = load(args.b)
    common = sorted(set(a) & set(b))
    id_mismatch, score_mismatch = [], []
    for q in common:
        if a[q][0] != b[q][0]:
            id_mismatch.append(q)
        elif not scores_close(a[q][1], b[q][1], args.score_rtol):
            score_mismatch.append(q)

    summary = {
        "a": str(args.a),
        "b": str(args.b),
        "a_successes": a_ok,
        "b_successes": b_ok,
        "a_queries": len(a),
        "b_queries": len(b),
        "compared_queries": len(common),
        "id_mismatches": len(id_mismatch),
        "score_mismatches": len(score_mismatch),
        "score_rtol": args.score_rtol,
        "a_unstable_queries": len(a_unstable),
        "b_unstable_queries": len(b_unstable),
    }
    summary["parity"] = bool(common) and not id_mismatch and not score_mismatch
    print(json.dumps(summary, indent=2))
    for q in id_mismatch[: args.show]:
        print(f"ids differ qidx={q}\n  a={a[q][0]}\n  b={b[q][0]}")
    for q in score_mismatch[: args.show]:
        print(f"scores differ qidx={q}\n  a={a[q][1]}\n  b={b[q][1]}")
    if a_unstable or b_unstable:
        print(
            "WARN: repetitions of the same query disagreed within a log "
            f"(a: {a_unstable[: args.show]}, b: {b_unstable[: args.show]})"
        )
    if args.out:
        args.out.write_text(json.dumps(summary, indent=2) + "\n")
    if not common:
        print("FAIL: no query was answered in both logs")
    sys.exit(0 if summary["parity"] else 1)


if __name__ == "__main__":
    main()
