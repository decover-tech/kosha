#!/usr/bin/env python3
"""Local cold-read iteration loop — measure hydration impact in seconds, not
staging round-trips.

The staging baseline showed cold read is dominated by (a) bytes/files
hydrated from S3 and (b) whether hydration *converges* under
KOSHA_CACHE_MAX_BYTES at all. Both signals are network-independent, so they
measure identically against a local MinIO — which makes this the fast loop
for cold-read optimizations (scoring-set-only hydration, compression, …),
with staging reserved for validating real latency at the end.

What it does:
  1. Starts the repo's own compose stack (postgres control plane + MinIO)
  2. Builds kosha-server with --features s3,postgres
  3. Ingests a deterministic Zipf-ish corpus once (reused across runs;
     re-ingests only if the namespace's doc count doesn't match)
  4. Cold-read loop: fresh empty data dir + fresh query-role process per
     run → search (retrying 503 like the real client) → parse the server's
     own `search timing:` line — then one warm repeat in the same process
  5. Prints a cold/warm table: client wall, attempts, hydrate ms/files/MB,
     score/materialize, cold-vs-cached opens

Usage:
  python3 scripts/bench/cold_read_local.py                  # defaults
  python3 scripts/bench/cold_read_local.py --segs 16 --docs 4000
  # Reproduce staging's non-convergence (budget < working set):
  python3 scripts/bench/cold_read_local.py --budget 2000000
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
WORK = Path(os.environ.get("KOSHA_COLD_LOOP_DIR", "/tmp/kosha-cold-read-local"))
NAMESPACE = "cold-read-local"
INGEST_PORT = 18200
QUERY_PORT = 18201

S3_ENV = {
    "KOSHA_S3_BUCKET": "dsearch-dev",
    "KOSHA_S3_PREFIX": "cold-read-local/",
    "KOSHA_S3_ENDPOINT": "http://127.0.0.1:9000",
    "KOSHA_S3_ACCESS_KEY": "kosha",
    "KOSHA_S3_SECRET_KEY": "kosha-dev-secret",
    "AWS_DEFAULT_REGION": "us-east-1",
    "DATABASE_URL": "postgresql://kosha:kosha-dev@127.0.0.1:5432/kosha",
}

TIMING_RE = re.compile(r"search timing: .*")


def sh(cmd, **kw):
    print(f"  $ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, **kw)


def http(method, port, path, body=None, timeout=600):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/{path.lstrip('/')}",
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def wait_healthy(port, proc, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server on :{port} exited early (rc={proc.returncode})")
        try:
            http("GET", port, "healthz", timeout=2)
            return
        except (urllib.error.URLError, OSError):
            time.sleep(0.3)
    raise RuntimeError(f"server on :{port} never became healthy")


def spawn_server(role, data_dir, port, extra_env=None, log_path=None):
    env = {**os.environ, **S3_ENV, **(extra_env or {})}
    env.update(
        KOSHA_ROLE=role,
        KOSHA_HTTP_PORT=str(port),
        KOSHA_DATA_DIR=str(data_dir),
        KOSHA_HTTP_IO_TIMEOUT_SECS="600",
    )
    log = open(log_path, "w") if log_path else subprocess.DEVNULL
    proc = subprocess.Popen(
        [str(REPO / "target/release/kosha-server")],
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        cwd=REPO,
    )
    wait_healthy(port, proc)
    return proc


# ─── Corpus (same Zipf-ish shape as the segment_memory rust bench) ───────────


def make_doc(rng_state, vocab, words_per_doc, add_phrase):
    words = []
    s = rng_state[0]
    for _ in range(words_per_doc):
        s = (s * 6364136223846793005 + 1442695040888963407) % (1 << 64)
        u = s / float(1 << 64)
        rank = int(u * u * vocab)
        words.append(
            "the" if rank == 0 else "contract" if rank == 1 else f"w{rank}"
        )
    rng_state[0] = s
    if add_phrase:
        words += ["breach", "warranty"]
    return " ".join(words)


def ensure_corpus(args):
    """Ingest once; reuse if the namespace already holds the expected docs."""
    expected = args.segs * args.docs
    ingest_dir = WORK / "ingest-data"
    ingest_dir.mkdir(parents=True, exist_ok=True)
    proc = spawn_server("ingest", ingest_dir, INGEST_PORT, log_path=WORK / "ingest.log")
    try:
        stats = http("GET", INGEST_PORT, "stats")
        for ns in stats.get("namespaces", []):
            if ns["namespace"] == NAMESPACE and ns["documents"] == expected:
                print(f"  corpus reused: {expected} docs / {ns['segments']} segments")
                return
        print(f"  ingesting {args.segs} segs × {args.docs} docs …")
        rng = [0x5EED]
        for s in range(args.segs):
            docs = [
                {
                    "id": f"s{s}-d{d}",
                    "fields": [
                        {
                            "name": "content",
                            "field_type": "Text",
                            "value": make_doc(rng, args.vocab, args.words, d % 50 == 0),
                        }
                    ],
                }
                for d in range(args.docs)
            ]
            http("POST", INGEST_PORT, "index", {"namespace": NAMESPACE, "documents": docs})
            # One flush per batch = one segment, synced to MinIO on publish.
            http("POST", INGEST_PORT, "flush", {"namespace": NAMESPACE})
            print(f"    segment {s + 1}/{args.segs} flushed")
    finally:
        proc.terminate()
        proc.wait(timeout=10)


# ─── Cold/warm measurement ──────────────────────────────────────────────────


def search_until_ok(port, max_attempts):
    """Retry 503s like the real kosha_client. Returns (wall_s, attempts)."""
    t0 = time.time()
    for attempt in range(1, max_attempts + 1):
        try:
            http(
                "POST",
                port,
                "search",
                {"namespace": NAMESPACE, "query_text": "the", "max_results": 10},
            )
            return time.time() - t0, attempt
        except urllib.error.HTTPError as e:
            if e.code != 503:
                raise RuntimeError(f"search failed: HTTP {e.code}: {e.read()[:200]}")
            time.sleep(0.5)
    return None, max_attempts  # non-convergence


def timing_lines(log_path):
    return TIMING_RE.findall(Path(log_path).read_text())


def cold_run(run_idx, args):
    data_dir = WORK / "query-data"
    if data_dir.exists():
        shutil.rmtree(data_dir)  # ← this is what makes it cold
    data_dir.mkdir(parents=True)
    log_path = WORK / f"query-run{run_idx}.log"
    extra = {}
    if args.budget is not None:
        extra["KOSHA_CACHE_MAX_BYTES"] = str(args.budget)
    proc = spawn_server("query", data_dir, QUERY_PORT, extra, log_path)
    try:
        cold_wall, cold_attempts = search_until_ok(QUERY_PORT, args.max_attempts)
        warm_wall = warm_attempts = None
        if cold_wall is not None:
            warm_wall, warm_attempts = search_until_ok(QUERY_PORT, 3)
    finally:
        proc.terminate()
        proc.wait(timeout=10)
    lines = timing_lines(log_path)
    return {
        "cold_wall": cold_wall,
        "cold_attempts": cold_attempts,
        "warm_wall": warm_wall,
        "timing": lines,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--segs", type=int, default=8)
    ap.add_argument("--docs", type=int, default=2000)
    ap.add_argument("--vocab", type=int, default=20000)
    ap.add_argument("--words", type=int, default=120)
    ap.add_argument("--runs", type=int, default=3, help="cold iterations")
    ap.add_argument("--max-attempts", type=int, default=20)
    ap.add_argument(
        "--budget",
        type=int,
        default=None,
        help="KOSHA_CACHE_MAX_BYTES for the query server (bytes). Set below "
        "the corpus size to reproduce staging's non-convergence locally.",
    )
    ap.add_argument("--skip-build", action="store_true")
    args = ap.parse_args()

    WORK.mkdir(parents=True, exist_ok=True)

    print("── stack (postgres + minio) ──")
    sh(["docker", "compose", "up", "-d", "postgres", "minio", "createbuckets"], cwd=REPO)

    if not args.skip_build:
        print("── build ──")
        sh(
            ["cargo", "build", "--release", "-p", "kosha-server", "--features", "s3,postgres"],
            cwd=REPO,
        )

    print("── corpus ──")
    ensure_corpus(args)

    print(f"── cold-read loop ({args.runs} runs, budget={args.budget or 'default'}) ──")
    results = []
    for i in range(args.runs):
        r = cold_run(i, args)
        results.append(r)
        if r["cold_wall"] is None:
            print(
                f"  run {i}: DID NOT CONVERGE after {r['cold_attempts']} attempts "
                "(staging bug #5 reproduced — budget below working set)"
            )
        else:
            print(
                f"  run {i}: cold {r['cold_wall'] * 1e3:7.0f} ms "
                f"({r['cold_attempts']} attempt(s)), "
                f"warm {r['warm_wall'] * 1e3:6.0f} ms"
            )
        for line in r["timing"]:
            print(f"      {line}")

    ok = [r for r in results if r["cold_wall"] is not None]
    if ok:
        med = sorted(r["cold_wall"] for r in ok)[len(ok) // 2]
        print(f"\nmedian cold: {med * 1e3:.0f} ms across {len(ok)} converged run(s)")
    else:
        print("\nno run converged — cold read impossible under this budget")
        sys.exit(2)


if __name__ == "__main__":
    main()
