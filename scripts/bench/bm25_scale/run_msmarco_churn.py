#!/usr/bin/env python3
"""Run the LIRE stability gate on local MSMarco embeddings; no server needed."""

import argparse
import os
import subprocess
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("embeddings", type=Path, help="directory from fetch_msmarco_embeddings.py")
    parser.add_argument("output", type=Path, help="output CSV (also written if the gate fails)")
    parser.add_argument("--size", type=int, default=1_000_000, help="live vectors; needs twice as many passage records")
    parser.add_argument("--cycles", type=int, default=50, help="cycles per rate and scenario")
    parser.add_argument("--queries", type=int, default=100, help="fixed held-out query embeddings")
    args = parser.parse_args()
    if not args.embeddings.is_dir():
        parser.error(f"not an embedding directory: {args.embeddings}")
    if not 20 <= args.size <= (2**32 - 1) // 2 or min(args.cycles, args.queries) < 1:
        parser.error("size must be 20–2147483647; cycles and queries must be positive")

    env = os.environ | {
        "KOSHA_BENCH_CHURN_EMBEDDINGS": str(args.embeddings.resolve()),
        "KOSHA_BENCH_CHURN_SIZE": str(args.size),
        "KOSHA_BENCH_CHURN_CYCLES": str(args.cycles),
        "KOSHA_BENCH_CHURN_QUERIES": str(args.queries),
    }
    command = [
        "cargo", "test", "--release", "--locked", "-p", "kosha-vector-spfresh",
        "--test", "churn_stability", "churn_cycle_recall_stability",
        "--", "--ignored", "--exact", "--nocapture",
    ]
    with args.output.open("w") as csv, subprocess.Popen(
        command, cwd=Path(__file__).resolve().parents[3], env=env,
        stdout=subprocess.PIPE, text=True,
    ) as run:
        for line in run.stdout:
            print(line, end="", flush=True)
            if line.startswith(("scenario,", "initial,", "stationary,", "drifting,")):
                csv.write(line)
                csv.flush()
        return run.wait()


if __name__ == "__main__":
    raise SystemExit(main())
