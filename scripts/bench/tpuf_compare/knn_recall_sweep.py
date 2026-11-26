#!/usr/bin/env python3
"""kNN probe-selection sweep against the local kosha-server (prints numbers only).
For N queries (sequential, warm after one untimed pass): tie-tolerant recall@10
vs exact cosine, server total/score ms and postings probed."""
import json, os, statistics, sys
import numpy as np, requests

N = int(sys.argv[1]) if len(sys.argv) > 1 else 300
LABEL = sys.argv[2] if len(sys.argv) > 2 else "?"
NC = int(os.environ.get("NC", "100"))
HEX = "0123456789abcdef"; DIM = 1536
ids, mats = [], []
for p in HEX:
    mats.append(np.fromfile(f"/data/corpus/emb/emb-{p}.f32", dtype="<f4").reshape(-1, DIM))
    ids += open(f"/data/corpus/emb/emb-{p}.ids").read().split("\n")[:-1]
X = np.concatenate(mats); del mats
X /= np.linalg.norm(X, axis=1, keepdims=True)
pos = {d: i for i, d in enumerate(ids)}
qv = np.fromfile("/data/queries/knn_emb.f32", dtype="<f4").reshape(-1, DIM)[:N]
sims = qv @ X.T
kth = -np.sort(np.partition(-sims, 10, axis=1)[:, :10], axis=1)[:, -1]
s = requests.Session(); s.headers.update({"Authorization": "Bearer sk-bench"})


def run(i):
    r = s.post("http://127.0.0.1:8080/v1/namespaces/stage-vec/search", timeout=120, json={
        "query_text": "", "max_results": 10, "source_includes": [], "timings": True, "no_cache": True,
        "knn": {"field": "text_emb", "vector": qv[i].tolist(), "k": 10, "num_candidates": NC}})
    r.raise_for_status(); return r.json()


for i in range(N):  # warm pass
    run(i)
rec, tot, sc, posts = [], [], [], []
for i in range(N):
    d = run(i)
    g = [pos[h["doc_id"]] for h in d.get("results", []) if h["doc_id"] in pos]
    rec.append(min(10, sum(1 for j in g if sims[i, j] >= kth[i] - 1e-4)) / 10)
    t = d.get("timings", {})
    tot.append(t.get("total_ms", float("nan"))); sc.append(t.get("score_ms", float("nan")))
    posts.append(t.get("knn_postings", 0))
r = np.array(rec)
q = lambda a, p: float(np.percentile(a, p))
print(f"{LABEL:42s} recall {r.mean():.3f} (p10 {q(r,10):.1f}, <0.9 {np.mean(r<0.9):.2f}) | "
      f"server p50 {q(tot,50):5.1f} p90 {q(tot,90):5.1f} ms | score p50 {q(sc,50):5.1f} | postings p50 {int(q(posts,50))}", flush=True)
