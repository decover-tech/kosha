"""Collect what explains an eval run: the Kosha pods' logs and the cluster's
state, joined to the run's own requests by request id.

Needs kubectl access to the cluster; run it beside run_eval.py.
  bundle.py follow <dir>                  start before the run; Ctrl-C or `finish` stops it
  bundle.py finish <dir> <samples.jsonl.gz>   after the run
`follow` tails every Kosha pod, including pods that appear or die mid-run.
`finish` adds events, pod and data state, each pod's local files, and writes
analysis.md: errors and latency per pod, pod age when it failed, and what
the server did for the slow and semantic requests.
"""
import collections
import gzip
import json
import os
import re
import subprocess
import sys
import time

NS = "kosha"
FIELD = re.compile(r"(\w+)=(\S+)")


def kubectl(*args, namespace=NS):
    return subprocess.run(["kubectl", "--request-timeout=60s", "-n", namespace, *args],
                          capture_output=True, text=True).stdout


def pods():
    return [p for p in kubectl("get", "pods", "-o", "name").split() if "loader" not in p]


def follow(out):
    os.makedirs(f"{out}/logs", exist_ok=True)
    tails = {}
    while not os.path.exists(f"{out}/STOP"):
        for pod in pods():
            if pod not in tails:
                log = open(f"{out}/logs/{pod.split('/')[-1]}.log", "a")
                tails[pod] = subprocess.Popen(["kubectl", "-n", NS, "logs", "-f", "--timestamps", "--since=1s", pod],
                                              stdout=log, stderr=subprocess.DEVNULL)
        time.sleep(15)
    for tail in tails.values():
        tail.terminate()


def pct(values, p):
    values = sorted(values)
    return round(values[min(len(values) - 1, int(round((len(values) - 1) * p)))], 1) if values else None


def finish(out, samples_path):
    open(f"{out}/STOP", "w").close()
    time.sleep(16)
    open(f"{out}/events.txt", "w").write(kubectl("get", "events", "--sort-by=.lastTimestamp"))
    open(f"{out}/pods.txt", "w").write(kubectl("get", "pods", "-o", "wide") + kubectl("top", "pods"))
    open(f"{out}/manifests.txt", "w").write(kubectl(
        "exec", "postgres-0", "--", "psql", "-U", "postgres", "-d", "kosha", "-Atc",
        "select namespace_id, version, updated_at from kosha.manifests order by 1", namespace="decoverai-services"))
    with open(f"{out}/local_files.txt", "w") as fh:
        for pod in pods():
            fh.write(f"## {pod}\n" + kubectl("exec", pod.split("/")[-1], "--", "sh", "-c",
                     "cd /var/cache/kosha/data && find . -type f -size +1M -printf '%s %p\\n' | sort -k2"))
    started = {p["metadata"]["name"]: p["status"].get("startTime", "")
               for p in json.loads(kubectl("get", "pods", "-o", "json") or '{"items": []}')["items"]}

    # request id -> the pod that served it and what it logged
    served = {}
    for name in sorted(os.listdir(f"{out}/logs")):
        pod = name[:-4]
        for line in open(f"{out}/logs/{name}", errors="replace"):
            m = re.search(r" rid=(\S+)", line)
            if m:
                entry = served.setdefault(m.group(1), {"pod": pod})
                if "search timing:" in line:
                    entry.update(FIELD.findall(line))
                elif "request failed:" in line:
                    entry["failed"] = line.split("request failed:")[1].split(" rid=")[0].strip()[:110]
    by_pod = collections.defaultdict(lambda: {"ms": collections.defaultdict(list), "errors": collections.Counter(), "n": 0})
    knn = collections.defaultdict(lambda: collections.Counter())
    unmatched = total = 0
    slow = []
    with gzip.open(samples_path, "rt") as fh:
        for line in fh:
            s = json.loads(line)
            total += 1
            log = served.get(s.get("rid"))
            if not log:
                unmatched += 1
                continue
            pod = by_pod[log["pod"]]
            pod["n"] += 1
            if s["error"]:
                pod["errors"][log.get("failed") or s["error"]] += 1
            else:
                pod["ms"][s["area"]].append(s["ms"])
                slow.append((s["ms"], s["area"], s["phase"], log))
            if s["area"].startswith("knn") and "knn_mode" in log:
                knn[log["pod"]][f"mode {log['knn_mode']}"] += 1
                knn[log["pod"]]["groups examined"] += int(log.get("knn_postings", 0))
                knn[log["pod"]]["vectors scanned exactly"] += int(log.get("knn_exact_vectors", 0))
                knn[log["pod"]]["queries"] += 1
    md = [f"# What the servers did: {total} requests, {total - unmatched} matched to a pod log line", ""]
    md += ["| Pod | Started | Requests | Errors | Word p50/p95 | Phrase p50/p95 | Semantic p50/p95 |", "|---|---|---|---|---|---|---|"]
    for name, pod in sorted(by_pod.items()):
        cell = lambda a: f"{pct(pod['ms'][a], .5)} / {pct(pod['ms'][a], .95)}"
        md.append(f"| {name[-5:]} | {started.get(name, 'gone')[11:19]} | {pod['n']} | {sum(pod['errors'].values())} | "
                  f"{cell('word')} | {cell('phrase')} | {cell('knn')} |")
    md += ["", "Errors by pod and reason", ""]
    md += [f"- {name[-5:]}: {n} × {reason}" for name, pod in sorted(by_pod.items()) for reason, n in pod["errors"].most_common(4)] or ["- none"]
    md += ["", "Semantic queries by pod", ""]
    for name, c in sorted(knn.items()):
        q = max(1, c["queries"])
        modes = ", ".join(f"{k[5:]} ×{v}" for k, v in c.items() if k.startswith("mode "))
        md.append(f"- {name[-5:]}: {c['queries']} queries; modes {modes}; {c['groups examined'] / q:.0f} groups and "
                  f"{c['vectors scanned exactly'] / q:.0f} exactly-scanned vectors per query")
    md += ["", "Ten slowest answered requests", "", "| ms | Area | Phase | Pod | Fetched MB | Queue ms | Scoring ms | Page ms |", "|---|---|---|---|---|---|---|---|"]
    for ms, area, phase, log in sorted(slow, key=lambda r: -r[0])[:10]:
        md.append(f"| {ms:.0f} | {area} | {phase} | {log['pod'][-5:]} | {log.get('hydrate_mb', '-')} | {log.get('queue_ms', '-')} | "
                  f"{log.get('score_ms', '-')} | {log.get('materialize_ms', '-')} |")
    open(f"{out}/analysis.md", "w").write("\n".join(md) + "\n")
    print("\n".join(md))


if __name__ == "__main__":
    follow(sys.argv[2]) if sys.argv[1] == "follow" else finish(sys.argv[2], sys.argv[3])
