"""Run a frozen eval set (see build_evalset.py) against a Kosha deployment.

Measures Kosha from the outside, the way the backend uses it: every request
goes through the Python client as an OpenSearch-style body. Reports
correctness against the set's expected answers, latency (p50/p90/p95),
behaviour as clients are added, and every error by reason. Read-only.

Usage: run_eval.py <evalset_dir> --mode quick|standard|full [--out DIR]
                   [--host URL] [--namespace NAME] [--baseline report.json]
                   [--areas knn,phrase]
  quick     ~10 min  the quick tier; warm latency; 1 and 8 clients
  standard  ~30 min  every case; warm and cached latency; 1-16 clients
  full      ~60 min  standard, 1-32 clients, then a sustained mixed run
  diagnose  ~10 min  experiments that test a cause, not a regression check:
                     semantic recall as more vector groups are examined, and
                     what a matter-wide count costs by shape
Writes report.json, report.md, failures.jsonl and samples.jsonl.gz to --out.
The report's start/end times bound the server logs that explain its numbers.
"""
import argparse
import collections
import concurrent.futures as cf
import datetime
import gzip
import json
import os
import random
import threading
import time

from kosha_client import KoshaClient

MODES = {  # warm rounds, cached rounds, (clients, seconds each), sustained minutes
    "quick": dict(warm=5, cached=0, ramp=((1, 8), 60), sustained=0),
    "standard": dict(warm=8, cached=1, ramp=((1, 4, 8, 16), 180), sustained=0),
    "full": dict(warm=8, cached=1, ramp=((1, 4, 8, 16, 32), 180), sustained=20),
    "diagnose": {},
}
GROUP_BUDGETS = (100, 800, 1600, 3200, 6400, 20000)  # knn num_candidates; the server raises small ones to its floor
LOAD_AREAS = ("word", "phrase", "word_grouped", "word_per_document", "document", "knn")
WALK_PAGE = 1000

ap = argparse.ArgumentParser()
ap.add_argument("evalset")
ap.add_argument("--mode", choices=MODES, default="quick")
ap.add_argument("--out", default="eval-out")
ap.add_argument("--host", default=os.environ.get("KOSHA_HOST"))
ap.add_argument("--namespace", default="paragraph_index_hnsw")
ap.add_argument("--baseline")
ap.add_argument("--areas", help="comma-separated areas to run, e.g. knn,phrase (default: all)")
args = ap.parse_args()
mode = MODES[args.mode]
os.makedirs(args.out, exist_ok=True)
manifest = json.load(open(os.path.join(args.evalset, "manifest.json")))
with gzip.open(os.path.join(args.evalset, "cases.jsonl.gz"), "rt") as fh:
    every_case = [json.loads(line) for line in fh]
by_id = {c["id"]: c for c in every_case}
areas = set(args.areas.split(",")) if args.areas else None
cases = [c for c in every_case if (args.mode != "quick" or c["tier"] == "quick") and (not areas or c["area"] in areas)]
client = KoshaClient(hosts=[args.host], namespace=args.namespace, timeout=120)
samples = gzip.open(os.path.join(args.out, "samples.jsonl.gz"), "wt")
samples_lock = threading.Lock()
random.seed(3)
# Every request carries an id the server prints on its log lines, so a slow
# or failed request here can be found in the pod that served it (bundle.py).
RUN_ID = f"ev{int(time.time()) % 10**8}"
request_numbers = iter(range(1, 10**9))


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def pct(values, p):
    values = sorted(values)
    return round(values[min(len(values) - 1, int(round((len(values) - 1) * p)))], 1) if values else None


def summary(values):
    return {"n": len(values), "p50": pct(values, 0.5), "p90": pct(values, 0.9), "p95": pct(values, 0.95)}


def send(case, phase, cached=False, body=None):
    """One search. Returns (response or None, ms, error or None); the error
    is "<status> <reason>". Every request is recorded in samples."""
    body = dict(body or case["request"])
    if not cached:
        body["request_cache"] = False
    with samples_lock:
        rid = f"{RUN_ID}-{next(request_numbers)}"
    started = time.perf_counter()
    response = error = None
    try:
        response = client.search(body=body, headers={"X-Request-Id": rid})
    except Exception as e:  # recorded, never hidden
        error = f"{getattr(e, 'status_code', type(e).__name__)} {str(getattr(e, 'error', e))[:90]}"
    ms = (time.perf_counter() - started) * 1000
    with samples_lock:
        samples.write(json.dumps({"t": round(time.time(), 3), "rid": rid, "phase": phase, "case": case["id"],
                                  "area": case["area"], "ms": round(ms, 2), "error": error}) + "\n")
    return response, ms, error


def hit_ids(response):
    return [h["_id"] for h in response["hits"]["hits"]]


def walk_ids(case):
    """Every matching id, paged in `_id` order."""
    ids, after = [], None
    while True:
        body = {"size": WALK_PAGE, "_source": False, "sort": [{"_id": "asc"}], "query": case["request"]["query"]}
        if after is not None:
            body["search_after"] = [after]
        response, _, error = send(case, "walk", body=body)
        if error:
            raise RuntimeError(error)
        page = hit_ids(response)
        ids += page
        if len(page) < WALK_PAGE:
            return ids
        after = page[-1]


def check(case, response):
    """Problems with one response; empty when it is correct."""
    expect, problems = case["expect"], []
    hits = response["hits"]
    got_ids = hit_ids(response)
    if "total" in expect:
        total = hits["total"]
        if (total["value"], total.get("relation", "eq")) != (expect["total"], "eq"):
            problems.append(f"total: expected {expect['total']}, got {total['value']} ({total.get('relation')})")
    allowed = expect.get("ids")
    if allowed is None and expect.get("ids_ref"):
        allowed = by_id[expect["ids_ref"]]["expect"].get("ids")
    if allowed is not None:
        wrong = set(got_ids) - set(allowed)
        if wrong:
            problems.append(f"{len(wrong)} hit(s) are not real matches, e.g. {sorted(wrong)[:2]}")
    if "ids" in expect and (args.mode != "quick" or len(expect["ids"]) <= 5000):
        walked = walk_ids(case)
        missing, extra = set(expect["ids"]) - set(walked), set(walked) - set(expect["ids"])
        if missing or extra or len(walked) != len(set(walked)):
            problems.append(f"id set: {len(missing)} missing, {len(extra)} extra, "
                            f"{len(walked) - len(set(walked))} duplicate(s)")
    if "ordered_ids" in expect and got_ids != expect["ordered_ids"]:
        first = next((i for i, (a, b) in enumerate(zip(got_ids, expect["ordered_ids"])) if a != b), min(len(got_ids), len(expect["ordered_ids"])))
        problems.append(f"order: first difference at position {first}")
    if "groups" in expect:
        docs = [h.get("_source", {}).get("documentId") for h in hits["hits"]]
        if len(docs) != expect["groups"] or len(set(docs)) != len(docs):
            problems.append(f"groups: expected {expect['groups']} distinct, got {len(docs)} ({len(set(docs))} distinct)")
    if "buckets" in expect:
        want = expect["buckets"]
        got = {b["key"]: b["doc_count"] for b in response.get("aggregations", {}).get(want["agg"], {}).get("buckets", [])}
        wrong = [k for k, n in got.items() if k in want["counts"] and want["counts"][k] != n]
        unknown = [k for k in got if k not in want["counts"]] if not want.get("truncated") else []
        if wrong or unknown or len(got) != len(want["counts"]):
            problems.append(f"buckets: expected {len(want['counts'])}, got {len(got)}; {len(wrong)} wrong count(s), {len(unknown)} unexpected")
    if "value" in expect:
        got = response.get("aggregations", {}).get(expect["value"]["agg"], {}).get("value")
        if got != expect["value"]["value"]:
            problems.append(f"{expect['value']['agg']}: expected {expect['value']['value']}, got {got}")
    return problems


def recall(case, response):
    """(recall@10, top hit right). A returned chunk counts when it is as
    close as the 10th true neighbour, and the top hit when it is as close as
    the best one: duplicate documents have identical vectors, so several
    different answers are equally right."""
    exact = case["expect"]["neighbours"]
    closeness = dict(map(tuple, exact))
    k = min(10, len(exact))
    got = hit_ids(response)[:10]
    as_close = [i for i in got if closeness.get(i, -2.0) >= exact[k - 1][1] - 1e-5]
    top_right = bool(got) and closeness.get(got[0], -2.0) >= exact[0][1] - 1e-5
    return min(len(as_close), k) / max(1, k), top_right


def load(clients, seconds, phase, semantic=True):
    """`clients` threads sending a random mix for `seconds`."""
    pool = [c for c in cases if c["area"] in LOAD_AREAS and (semantic or c["area"] != "knn")]
    lat, errors, stop, lock = [], collections.Counter(), time.time() + seconds, threading.Lock()

    def worker(seed):
        rnd = random.Random(seed)
        while time.time() < stop:
            _, ms, error = send(rnd.choice(pool), phase)
            with lock:
                errors[error] += 1 if error else 0
                if not error:
                    lat.append(ms)

    with cf.ThreadPoolExecutor(clients) as ex:
        list(ex.map(worker, range(clients)))
    errors = {k: v for k, v in errors.items() if k}
    return {"clients": clients, "seconds": seconds, "requests_per_second": round((len(lat) + sum(errors.values())) / seconds, 1),
            **summary(lat), "errors": sum(errors.values()), "errors_by_reason": errors}


report = {"matter": manifest["matter"], "evalset_version": manifest["version"], "mode": args.mode, "run_id": RUN_ID,
          "host": args.host, "namespace": args.namespace, "started": utc(), "cases": len(cases)}


def diagnose():
    """Each experiment changes one thing and reports what moved."""
    out = [f"# Kosha diagnose: matter {manifest['matter'][:8]}, set {manifest['version']}", "", f"Started {report['started']} UTC, run id {RUN_ID}", ""]
    for area in ("knn", "knn_filtered"):
        semantic = [c for c in cases if c["area"] == area and not c["expect"].get("ambiguous")]
        out += [f"Semantic recall as more vector groups are examined ({area}, {len(semantic)} cases)", "",
                "| Groups asked for | Mean recall@10 | Cases at 1.0 | Top hit right | p50 ms | p95 ms | Errors |", "|---|---|---|---|---|---|---|"]
        report[f"recall_by_groups_{area}"] = []
        for budget in GROUP_BUDGETS:
            recalls, tops, lat, errors = [], 0, [], 0
            for case in semantic:
                body = json.loads(json.dumps(case["request"]))
                body["query"]["knn"]["contentEmbedding"]["num_candidates"] = budget
                response, ms, error = send(case, f"groups-{budget}", body=body)
                if error:
                    errors += 1
                    continue
                r, top = recall(case, response)
                recalls.append(r)
                tops += top
                lat.append(ms)
            row = {"groups": budget, "mean_recall": round(sum(recalls) / max(1, len(recalls)), 3),
                   "perfect": sum(r == 1.0 for r in recalls), "top_right": tops, **summary(lat), "errors": errors}
            report[f"recall_by_groups_{area}"].append(row)
            out.append(f"| {budget} | {row['mean_recall']} | {row['perfect']} of {len(recalls)} | {tops} | {row['p50']} | {row['p95']} | {errors} |")
        out.append("")
    scope = next(c for c in cases if c["id"] == "matter-001")["request"]["query"]
    shapes = {
        "count only": {"size": 0, "track_total_hits": True, "query": scope},
        "distinct documents": {"size": 0, "query": scope, "aggs": {"a": {"cardinality": {"field": "documentId"}}}},
        "file types, top 10": {"size": 0, "query": scope, "aggs": {"a": {"terms": {"field": "fileFormats", "size": 10}}}},
        "tags, top 10": {"size": 0, "query": scope, "aggs": {"a": {"terms": {"field": "system_tags", "size": 10}}}},
        "tags, top 1000": {"size": 0, "query": scope, "aggs": {"a": {"terms": {"field": "system_tags", "size": 1000}}}},
        "documents, top 10": {"size": 0, "query": scope, "aggs": {"a": {"terms": {"field": "documentId", "size": 10}}}},
        "documents, top 1000": {"size": 0, "query": scope, "aggs": {"a": {"terms": {"field": "documentId", "size": 1000}}}},
    }
    out += ["What a matter-wide count costs, by shape (10 warm requests each)", "", "| Shape | p50 ms | p95 ms | Errors |", "|---|---|---|---|"]
    report["matter_wide"] = {}
    probe = {"id": "matter-shape", "area": "matter"}
    for name, body in shapes.items():
        results = [send(probe, f"shape-{name}", body=body) for _ in range(11)][1:]  # the first warms it
        lat = [ms for _, ms, error in results if not error]
        report["matter_wide"][name] = {**summary(lat), "errors": len(results) - len(lat)}
        out.append(f"| {name} | {pct(lat, 0.5)} | {pct(lat, 0.95)} | {len(results) - len(lat)} |")
    report["ended"] = utc()
    samples.close()
    json.dump(report, open(os.path.join(args.out, "report.json"), "w"), indent=1)
    open(os.path.join(args.out, "report.md"), "w").write("\n".join(out) + "\n")
    print("\n".join(out))
    print("EVAL_DONE", flush=True)
    raise SystemExit(0)


if args.mode == "diagnose":
    diagnose()
failures = open(os.path.join(args.out, "failures.jsonl"), "w")

# ── 1. Correctness, on the first send of each case ─────────────────────────
correct = collections.defaultdict(lambda: collections.Counter())
first, recalls, top1 = collections.defaultdict(list), [], []
for case in cases:
    response, ms, error = send(case, "first")
    area = correct[case["area"]]
    if error:
        problems = [f"request failed: {error}"]
        area["errors"] += 1
    else:
        first[case["area"]].append(ms)
        try:
            problems = check(case, response)
        except RuntimeError as e:
            problems = [f"request failed while reading all ids: {e}"]
            area["errors"] += 1
        if "neighbours" in case["expect"] and not case["expect"].get("ambiguous"):
            r, same_top = recall(case, response)
            recalls.append(r)
            top1.append(same_top)
            if r < 0.9 or not same_top:
                problems.append(f"recall@10 {r:.1f}, top hit {'right' if same_top else 'wrong'}")
    area["passed" if not problems else "failed"] += 1
    if problems:
        failures.write(json.dumps({"case": case["id"], "area": case["area"], "problems": problems}) + "\n")
failures.close()
report["correctness"] = {a: dict(c) for a, c in correct.items()}
report["passed"] = sum(c["passed"] for c in correct.values())
report["unanswered"] = sum(c["errors"] for c in correct.values())  # the request itself failed
report["failed"] = sum(c["failed"] for c in correct.values()) - report["unanswered"]  # answered wrongly
if recalls:
    report["semantic"] = {"cases": len(recalls), "mean_recall_at_10": round(sum(recalls) / len(recalls), 3),
                          "top_hit_right": sum(top1)}
report["latency_first_ms"] = {a: summary(v) for a, v in first.items()}
print(f"correctness done: {report['passed']} passed, {report['failed']} wrong, {report['unanswered']} unanswered", flush=True)

# ── 2. Latency: warm (answered from the index), then cached ────────────────
for label, rounds, cached in (("warm", mode["warm"], False), ("cached", mode["cached"], True)):
    lat, errors = collections.defaultdict(list), collections.Counter()
    for _ in range(rounds):
        for case in cases:
            _, ms, error = send(case, label, cached=cached)
            if error:
                errors[error] += 1
            else:
                lat[case["area"]].append(ms)
    if rounds:
        report[f"latency_{label}_ms"] = {a: summary(v) for a, v in lat.items()}
        report[f"latency_{label}_errors"] = dict(errors)
print("latency done", flush=True)

# ── 3. Adding clients, then a sustained run ────────────────────────────────
levels, seconds = mode["ramp"]
report["concurrency"] = [load(n, seconds, f"clients-{n}") for n in levels]
# The same top level without semantic queries, to tell their cost apart.
report["concurrency_no_semantic"] = [load(levels[-1], seconds, f"clients-{levels[-1]}-no-semantic", semantic=False)]
if mode["sustained"]:
    minutes = [load(8, 60, f"sustained-{m:02d}") for m in range(mode["sustained"])]
    report["sustained"] = {"clients": 8, "minutes": len(minutes), "p95_first_5_min": pct([m["p95"] or 0 for m in minutes[:5]], 0.5),
                           "p95_last_5_min": pct([m["p95"] or 0 for m in minutes[-5:]], 0.5),
                           "errors": sum(m["errors"] for m in minutes), "per_minute": minutes}
report["ended"] = utc()
samples.close()
json.dump(report, open(os.path.join(args.out, "report.json"), "w"), indent=1)

# ── Report ─────────────────────────────────────────────────────────────────
base = json.load(open(args.baseline)) if args.baseline else {}


def cell(now, before):
    if now is None:
        return "-"
    return f"{now:g}" if before in (None, now) else f"{now:g} (was {before:g})"


out = [f"# Kosha eval: {args.mode}, matter {manifest['matter'][:8]}, set {manifest['version']}", "",
       f"{report['started']} to {report['ended']} UTC, {args.host}", "",
       f"**Correctness: {report['passed']} of {len(cases)} cases pass; {report['failed']} answered wrongly, "
       f"{report['unanswered']} got no answer (request failed).**"]
if recalls:
    s = report["semantic"]
    out.append(f"Semantic: mean recall@10 {s['mean_recall_at_10']}, top hit right in {s['top_hit_right']} of {s['cases']}.")
out += ["", "| Area | Passed | Wrong | No answer |", "|---|---|---|---|"]
out += [f"| {a} | {c.get('passed', 0)} | {c.get('failed', 0) - c.get('errors', 0)} | {c.get('errors', 0)} |" for a, c in report["correctness"].items()]
for key, title in (("latency_first_ms", "first request"), ("latency_warm_ms", "warm"), ("latency_cached_ms", "result cache allowed")):
    if key in report:
        out += ["", f"Latency, {title} (ms)", "", "| Area | n | p50 | p90 | p95 |", "|---|---|---|---|---|"]
        for a, v in report[key].items():
            b = base.get(key, {}).get(a, {})
            out.append(f"| {a} | {v['n']} | {cell(v['p50'], b.get('p50'))} | {cell(v['p90'], b.get('p90'))} | {cell(v['p95'], b.get('p95'))} |")
out += ["", "Adding clients (mixed requests)", "", "| Clients | Requests/s | p50 | p90 | p95 | Errors |", "|---|---|---|---|---|---|"]
out += [f"| {r['clients']} | {r['requests_per_second']} | {r['p50']} | {r['p90']} | {r['p95']} | {r['errors']} {r['errors_by_reason'] or ''} |"
        for r in report["concurrency"]]
r = report["concurrency_no_semantic"][0]
out += ["", f"Without semantic queries at {r['clients']} clients: {r['requests_per_second']} requests/s, "
        f"p50 {r['p50']}, p95 {r['p95']} ms, {r['errors']} error(s)."]
if "sustained" in report:
    s = report["sustained"]
    out += ["", f"Sustained {s['minutes']} min at {s['clients']} clients: p95 {s['p95_first_5_min']} ms in the first 5 minutes, "
            f"{s['p95_last_5_min']} ms in the last 5; {s['errors']} error(s)."]
open(os.path.join(args.out, "report.md"), "w").write("\n".join(out) + "\n")
print("\n".join(out))
print("EVAL_DONE", flush=True)
