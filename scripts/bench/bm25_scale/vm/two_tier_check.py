#!/usr/bin/env python3
"""Validity checks for a research #387 two-tier analysis run.

Subcommands:

  manifest <segments_json file>
      Exit 1 when the restored manifest has tombstones, deletion vectors or
      shards: the analysis counts every posting as live and reads one
      unsharded namespace, so its oracle would not be the engine's answer.

  run <report.json> <per_query.jsonl> <host> <namespace> <api-key> <n> <out.json>
      (1) report checks: no segment skipped for multiple analysis chains,
      no query skipped by the time budget, every segment split (span
      bytes known), violations == 0 everywhere;
      (2) server cross-check: send the first <n> analyzed queries (OR,
      k = the report's k, no cache, ids + scores only) to the live server
      and compare ids and scores with the oracle in per_query.jsonl. Ids may
      differ only among equal scores (the engine breaks ties by _id, the
      oracle by segment and doc_seq).
      Writes <out.json> and exits 1 if anything fails: the run is then
      invalid and its certification numbers must not be used.
"""

import json
import sys
import time
import urllib.request

REL_TOL = 1e-6


def check_manifest(path):
    raw = open(path).read().strip()
    doc = json.loads(raw)
    problems = []
    if isinstance(doc, dict):
        tomb = doc.get("tombstones") or {}
        n_tomb = sum(len(v) for v in tomb.values()) if isinstance(tomb, dict) else len(tomb)
        if n_tomb:
            problems.append(f"{n_tomb} tombstoned docs in {len(tomb)} segments")
        dv = doc.get("deletion_vectors") or {}
        if dv:
            n_del = sum(int(v.get("count", 0)) for v in dv.values() if isinstance(v, dict))
            problems.append(f"deletion vectors on {len(dv)} segments ({n_del} deleted docs)")
        shards = doc.get("shards")
        if shards and int(shards) > 1:
            problems.append(f"sharded namespace ({shards} shards)")
    if problems:
        print("::error::two-tier analysis needs a manifest with no deletes: " + "; ".join(problems))
        return 1
    print("manifest check: no tombstones, deletion vectors or shards")
    return 0


def close(a, b):
    return abs(a - b) <= REL_TOL * max(abs(a), abs(b), 1.0)


def compare(engine, oracle):
    """None if equal up to tie order at equal scores, else a reason."""
    if len(engine) != len(oracle):
        return f"{len(engine)} engine hits vs {len(oracle)} oracle hits"
    for i, (e, o) in enumerate(zip(engine, oracle)):
        if not close(e[1], o[1]):
            return f"rank {i}: engine score {e[1]!r} vs oracle {o[1]!r}"
    # Ids, rank by rank: each run of equal scores must hold the same ids
    # (in any order), except the run at the cutoff, which may be cut
    # differently.
    kth = oracle[-1][1] if oracle else 0.0
    i = 0
    while i < len(oracle):
        j = i
        while j + 1 < len(oracle) and close(oracle[j + 1][1], oracle[i][1]):
            j += 1
        if not close(oracle[i][1], kth):
            ids_e = sorted(x[0] for x in engine[i : j + 1])
            ids_o = sorted(x[0] for x in oracle[i : j + 1])
            if ids_e != ids_o:
                return f"ranks {i}-{j}: engine ids {ids_e} vs oracle {ids_o}"
        i = j + 1
    return None


def search(host, ns, key, text, k):
    body = json.dumps(
        {
            "namespace": ns,
            "query_text": text,
            "max_results": k,
            "operator": "or",
            "no_cache": True,
            "source_includes": [],
        }
    ).encode()
    req = urllib.request.Request(
        f"{host}/search",
        data=body,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"},
    )
    with urllib.request.urlopen(req, timeout=300) as r:
        res = json.load(r)
    return [(str(h["doc_id"]), float(h["score"])) for h in res.get("results", [])]


def check_run(report_path, rows_path, host, ns, key, n, out_path):
    rep = json.load(open(report_path))
    report = rep["report"]
    k = int(report["config"]["k"])
    problems = []
    if report.get("segments_skipped_multi_reading", 0):
        problems.append(
            f"segments_skipped_multi_reading = {report['segments_skipped_multi_reading']}"
        )
    if report.get("queries_skipped_time_budget", 0):
        problems.append(f"queries_skipped_time_budget = {report['queries_skipped_time_budget']}")
    if report.get("split_segments") != report.get("segments"):
        problems.append(
            f"only {report.get('split_segments')}/{report.get('segments')} segments are split; "
            "span-byte ratios cover the split ones only"
        )
    for f in report["fractions"]:
        for b in f["bounds"]:
            if b["violations"]:
                problems.append(
                    f"{b['violations']} violations at head {f['head_fraction']} bound {b['bound']}"
                )

    rows = [json.loads(line) for line in open(rows_path) if line.strip()]
    checked, mismatches, errors = 0, [], []
    for row in rows[:n]:
        try:
            engine = search(host, ns, key, row["text"], k)
        except Exception as e:  # noqa: BLE001 - record and count as a failure
            errors.append({"query": row["query"], "error": str(e)[:200]})
            continue
        oracle = [(str(i), float(s)) for i, s in row["oracle_hits"]]
        why = compare(engine, oracle)
        checked += 1
        if why:
            mismatches.append(
                {"query": row["query"], "text": row["text"], "why": why, "engine": engine, "oracle": oracle}
            )
    if n <= 0:
        problems.append("server cross-check skipped (TWO_TIER_CROSSCHECK=0)")
    elif checked == 0:
        problems.append("server cross-check: no query answered")
    if mismatches:
        problems.append(f"server cross-check: {len(mismatches)}/{checked} queries differ from the oracle")
    if errors:
        problems.append(f"server cross-check: {len(errors)} request errors")
    out = {
        "valid": not problems,
        "problems": problems,
        "crosscheck_queries": checked,
        "crosscheck_mismatches": mismatches[:20],
        "crosscheck_errors": errors[:20],
        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    json.dump(out, open(out_path, "w"), indent=2)
    print(f"server cross-check: {checked} queries, {len(mismatches)} mismatches, {len(errors)} errors")
    if problems:
        print("TWO-TIER-RUN-INVALID: " + "; ".join(problems))
        return 1
    print("TWO-TIER-RUN-VALID")
    return 0


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "manifest":
        sys.exit(check_manifest(sys.argv[2]))
    if len(sys.argv) == 9 and sys.argv[1] == "run":
        _, _, report, rows, host, ns, key, n, out = sys.argv
        sys.exit(check_run(report, rows, host, ns, key, int(n), out))
    print(__doc__)
    sys.exit(2)


if __name__ == "__main__":
    main()
