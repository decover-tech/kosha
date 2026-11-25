#!/usr/bin/env python3
"""Build a cargo bench without running it and print its executable path.

    python3 scripts/bench/bench_exe.py <package> <bench-name>

Used by bench-compare.yml to build the base and head bench binaries up front,
so their runs can be interleaved on one runner.
"""

import json
import subprocess
import sys


def main() -> int:
    package, bench = sys.argv[1], sys.argv[2]
    proc = subprocess.run(
        ["cargo", "bench", "-p", package, "--bench", bench, "--no-run",
         "--message-format=json-render-diagnostics"],
        stdout=subprocess.PIPE,
        text=True,
    )
    exe = None
    for line in proc.stdout.splitlines():
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if msg.get("reason") == "compiler-artifact" and msg.get("executable") \
                and msg["target"]["name"] == bench:
            exe = msg["executable"]
    if proc.returncode != 0 or exe is None:
        print(f"error: building bench {bench} failed", file=sys.stderr)
        return 1
    print(exe)
    return 0


if __name__ == "__main__":
    sys.exit(main())
