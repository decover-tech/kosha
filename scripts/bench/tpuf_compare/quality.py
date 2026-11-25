#!/usr/bin/env python3
"""Result-quality check on the VM (prints numbers only).
kNN: exact and tie-tolerant recall@10 (hit counts if its exact cosine >= the
     10th-best exact cosine - 1e-4), for both engines, at several Kosha
     num_candidates.
BM25: Kosha's top-10 rescored by turbopuffer's own BM25 (id In filter): a Kosha
     hit is 'equivalent' if its tpuf score >= tpuf's 10th score * (1 - 1e-4)."""
import json, os, sys
import numpy as np, requests

N = int(sys.argv[1]) if len(sys.argv) > 1 else 200
KEY = os.environ["TURBOPUFFER_API_KEY"]
TP = f"https://{os.environ.get('TURBOPUFFER_REGION', 'aws-us-east-1')}.turbopuffer.com/v2/namespaces/{os.environ['TPUF_PREFIX']}-{{}}/query"
KO = "http://127.0.0.1:8080/v1/namespaces/{}/search"
ts = requests.Session(); ts.headers.update({"Authorization": f"Bearer {KEY}"})
ks = requests.Session(); ks.headers.update({"Authorization": "Bearer sk-bench"})
HEX = "0123456789abcdef"; DIM = 1536


def tp(kind, body):
    r = ts.post(TP.format(kind), json=body, timeout=60); r.raise_for_status(); return r.json()["rows"]


def ko(ns, payload):
    r = ks.post(KO.format(ns), json=payload, timeout=120); r.raise_for_status(); return r.json().get("results", [])


# ---- kNN
ids, mats = [], []
for p in HEX:
    mats.append(np.fromfile(f"/data/corpus/emb/emb-{p}.f32", dtype="<f4").reshape(-1, DIM))
    ids += open(f"/data/corpus/emb/emb-{p}.ids").read().split("\n")[:-1]
X = np.concatenate(mats); del mats
X /= np.linalg.norm(X, axis=1, keepdims=True)
pos = {d: i for i, d in enumerate(ids)}
qv = np.fromfile("/data/queries/knn_emb.f32", dtype="<f4").reshape(-1, DIM)[:N]
sims = qv @ X.T
kth = np.sort(np.partition(-sims, 10, axis=1)[:, :10], axis=1)[:, -1] * -1  # 10th best
true10 = [set(np.argpartition(-sims[i], 10)[:10]) for i in range(N)]
ties = sum(int((sims[i] >= kth[i] - 1e-4).sum() > 10) for i in range(N))


def score(i, got):
    g = [pos[d] for d in got if d in pos]
    exact = len(true10[i] & set(g)) / 10
    tol = min(10, sum(1 for j in g if sims[i, j] >= kth[i] - 1e-4)) / 10
    return exact, tol


res = {"tpuf": []}
for i in (range(0) if os.environ.get("SKIP_KNN") else range(N)):
    rows = tp("chunkvec", {"rank_by": ["vector", "ANN", qv[i].tolist()], "top_k": 10, "include_attributes": False})
    res["tpuf"].append(score(i, [r["id"] for r in rows]))
for nc in ([] if os.environ.get("SKIP_KNN") else [100, 300, 1000]):
    k = f"kosha nc={nc}"; res[k] = []
    for i in range(N):
        hits = ko("stage-vec", {"query_text": "", "max_results": 10, "source_includes": [],
                                "knn": {"field": "text_emb", "vector": qv[i].tolist(), "k": 10, "num_candidates": nc}})
        res[k].append(score(i, [h["doc_id"] for h in hits]))
print(f"kNN n={N}: queries with >10 docs tied at the cutoff: {ties}")
for k, v in res.items():
    a = np.array(v)
    if a.ndim < 2: continue
    print(f"  {k:14s} recall@10 exact {a[:,0].mean():.3f} | tie-tolerant {a[:,1].mean():.3f} (min {a[:,1].min():.1f}, share<0.9 {np.mean(a[:,1]<0.9):.2f})")

# ---- BM25
bm = [l.strip() for l in open("/data/queries/bm25.txt") if l.strip()][:N]
eq, exact_ov, worse = [], [], 0
for q in bm:
    t = tp("chunks", {"rank_by": ["content", "BM25", q], "top_k": 10, "include_attributes": False})
    k = ko("stage-bm25", {"query_text": q, "max_results": 10, "operator": "or", "source_includes": []})
    tids = [r["id"] for r in t]; kids = [h["doc_id"] for h in k]
    if not tids or not kids:
        continue
    exact_ov.append(len(set(tids) & set(kids)) / 10)
    t10 = t[-1]["$dist"]
    rs = tp("chunks", {"rank_by": ["content", "BM25", q], "top_k": 10, "include_attributes": False,
                       "filters": ["id", "In", kids]})
    sc = {r["id"]: r["$dist"] for r in rs}
    ok = sum(1 for d in kids if sc.get(d, 0) >= t10 * (1 - 1e-4))
    eq.append(ok / len(kids)); worse += ok < len(kids)
print(f"BM25 n={len(eq)}: exact top-10 overlap {np.mean(exact_ov):.3f} | Kosha hits tpuf-score-equivalent {np.mean(eq):.3f} | queries with a Kosha hit below tpuf's 10th: {worse}")
