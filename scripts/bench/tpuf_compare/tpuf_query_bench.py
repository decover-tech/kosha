#!/usr/bin/env python3
"""turbopuffer twin of query_bench.py: same open-loop fixed-QPS protocol, same
summary + per-request jsonl schema, against a turbopuffer namespace (read-only).

BM25:  rank_by [field, "BM25", text]        (turbopuffer BM25 is OR over terms)
kNN:   rank_by ["vector", "ANN", vec]       with --knn-embeddings (f32, --dim)
--consistency strong|eventual  (the app runs eventual; strong is the default)
"""
import argparse, gzip, json, threading, time
from pathlib import Path

import numpy as np
import requests


def pct(a, p):
    if not a:
        return float("nan")
    s = sorted(a); k = (len(s) - 1) * p / 100; f = int(k); c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--queries-file", type=Path, required=True)
    ap.add_argument("--api-key", required=True)
    ap.add_argument("--region", default="aws-us-east-1")
    ap.add_argument("--field", default="content")
    ap.add_argument("--knn-embeddings", type=Path)
    ap.add_argument("--dim", type=int, default=1536)
    ap.add_argument("--qps", type=float, default=8.0)
    ap.add_argument("--topk", type=int, default=10)
    ap.add_argument("--duration", type=float, default=300.0)
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--consistency", choices=["strong", "eventual"], default="strong")
    ap.add_argument("--phase", default="warm")
    ap.add_argument("--out", type=Path)
    ap.add_argument("--raw-out", type=Path)
    a = ap.parse_args()

    queries = [l.strip() for l in a.queries_file.read_text().splitlines() if l.strip()]
    vecs = None
    if a.knn_embeddings:
        vecs = np.fromfile(a.knn_embeddings, dtype="<f4").reshape(-1, a.dim)
        assert len(vecs) == len(queries), (len(vecs), len(queries))
        vecs = [v.tolist() for v in vecs]
    url = f"https://{a.region}.turbopuffer.com/v2/namespaces/{a.namespace}/query"
    sess = requests.Session()
    sess.headers.update({"Authorization": f"Bearer {a.api_key}", "Content-Type": "application/json"})
    adapter = requests.adapters.HTTPAdapter(pool_connections=64, pool_maxsize=64)
    sess.mount("https://", adapter)

    def body(q):
        rank = ["vector", "ANN", vecs[q]] if vecs is not None else [a.field, "BM25", queries[q]]
        b = {"rank_by": rank, "top_k": a.topk, "include_attributes": False}
        if a.consistency == "eventual":
            b["consistency"] = {"level": "eventual"}
        return b

    lat, errs, hits, raw, lock = [], [], [], [], threading.Lock()

    def one(seq, q, sched, start):
        t0 = time.time()
        rec = {"seq": seq, "qidx": q, "terms": len(queries[q].split()),
               "t_send_s": round(t0 - start, 4), "send_lag_ms": round((t0 - sched) * 1000, 2)}
        try:
            r = sess.post(url, data=json.dumps(body(q)), timeout=a.timeout)
            r.raise_for_status()
            d = r.json(); dt = time.time() - t0
            rows = d.get("rows", [])
            perf = d.get("performance", {})
            rec.update(latency_ms=round(dt * 1000, 3), hits=len(rows),
                       ids=[x["id"] for x in rows], scores=[x.get("$dist", x.get("$score")) for x in rows],
                       server={"total_ms": perf.get("server_total_ms"),
                               "query_execution_ms": perf.get("query_execution_ms"),
                               "cache_temperature": perf.get("cache_temperature"),
                               "cache_hit_ratio": perf.get("cache_hit_ratio")})
            with lock:
                lat.append(dt); hits.append(len(rows)); raw.append(rec)
        except Exception as e:  # noqa: BLE001
            rec.update(latency_ms=round((time.time() - t0) * 1000, 3), error=str(e)[:200])
            with lock:
                errs.append(str(e)); raw.append(rec)

    interval, start, i, ths = 1.0 / a.qps, time.time(), 0, []
    nxt = start
    while time.time() - start < a.duration:
        now = time.time()
        if now < nxt:
            time.sleep(nxt - now)
        th = threading.Thread(target=one, args=(i, i % len(queries), nxt, start), daemon=True)
        th.start(); ths.append(th); i += 1; nxt += interval
    for th in ths:
        th.join(a.timeout + 5)
    el = time.time() - start
    ms = [x * 1000 for x in lat]
    srv = [r["server"]["total_ms"] for r in raw if r.get("server") and r["server"]["total_ms"] is not None]
    summ = {"engine": "turbopuffer", "phase": a.phase, "namespace": a.namespace, "consistency": a.consistency,
            "target_qps": a.qps, "achieved_qps": i / el, "topk": a.topk, "requests": i,
            "successes": len(lat), "errors": len(errs),
            "p50_ms": pct(ms, 50), "p90_ms": pct(ms, 90), "p99_ms": pct(ms, 99),
            "mean_ms": sum(ms) / len(ms) if ms else None, "max_ms": max(ms) if ms else None,
            "server_p50_ms": pct(srv, 50), "server_p90_ms": pct(srv, 90), "server_p99_ms": pct(srv, 99),
            "zero_hit_requests": sum(1 for h in hits if h == 0),
            "short_page_requests": sum(1 for h in hits if h < a.topk)}
    print(json.dumps(summ, indent=1))
    if a.out:
        a.out.write_text(json.dumps(summ, indent=1))
    if a.raw_out:
        op = gzip.open if str(a.raw_out).endswith(".gz") else open
        with op(a.raw_out, "wt") as fh:
            for r in sorted(raw, key=lambda r: r["seq"]):
                fh.write(json.dumps(r) + "\n")


if __name__ == "__main__":
    main()
