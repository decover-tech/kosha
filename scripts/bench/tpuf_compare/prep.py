#!/usr/bin/env python3
"""On the VM: aligned vector text shards, query sets, exact kNN ground truth.
Everything stays under /data; prints only counts and shapes."""
import json, os, random, re
import numpy as np

C = "/data/corpus"; Q = "/data/queries"; DIM = 1536; N_Q = 1000
os.makedirs(f"{C}/vtxt", exist_ok=True); os.makedirs(Q, exist_ok=True)
HEX = "0123456789abcdef"
rng = random.Random(20261124)

# 1. vector-aligned text shards (same id order as emb-*.ids)
texts_sample = []
nv = 0
for p in HEX:
    ids = open(f"{C}/emb/emb-{p}.ids").read().split("\n")[:-1]
    want = set(ids)
    text = {}
    with open(f"{C}/txt/shard-{p}.ndjson") as fh:
        for line in fh:
            d = json.loads(line)
            if rng.random() < 0.01 and d["text"]:
                texts_sample.append(d["text"])
            if d["id"] in want:
                text[d["id"]] = d["text"]
    with open(f"{C}/vtxt/shard-{p}.ndjson", "w") as out:
        for i in ids:
            out.write(json.dumps({"id": i, "text": text.get(i, "")}) + "\n")
    nv += len(ids)
print("vector-aligned docs", nv, "missing text", 0)

# 2. BM25 queries: 1..8-word windows from random chunks
WORD = re.compile(r"[A-Za-z][A-Za-z0-9']+")
qs = []
while len(qs) < N_Q:
    words = WORD.findall(rng.choice(texts_sample))
    if len(words) < 8:
        continue
    L = rng.randint(1, 8)
    s = rng.randint(0, len(words) - L)
    qs.append(" ".join(w.lower() for w in words[s:s + L]))
open(f"{Q}/bm25.txt", "w").write("\n".join(qs) + "\n")
print("bm25 queries", len(qs), "terms histogram", {k: sum(1 for q in qs if len(q.split()) == k) for k in range(1, 9)})

# 3. kNN queries + exact ground truth (cosine)
mats, allids = [], []
for p in HEX:
    m = np.fromfile(f"{C}/emb/emb-{p}.f32", dtype="<f4").reshape(-1, DIM)
    mats.append(m); allids += open(f"{C}/emb/emb-{p}.ids").read().split("\n")[:-1]
X = np.concatenate(mats); del mats
norms = np.linalg.norm(X, axis=1)
print("corpus", X.shape, "norm min/median/max", float(norms.min()), float(np.median(norms)), float(norms.max()))
Xn = X / np.maximum(norms, 1e-12)[:, None]
nprng = np.random.default_rng(20261124)
a = nprng.integers(0, len(Xn), N_Q); b = nprng.integers(0, len(Xn), N_Q)
qv = Xn[a] + Xn[b]; qv /= np.linalg.norm(qv, axis=1, keepdims=True)
qv = qv.astype("<f4")
qv.tofile(f"{Q}/knn_emb.f32")
open(f"{Q}/knn.txt", "w").write("\n".join(f"q{i}" for i in range(N_Q)) + "\n")
gt = []
for s in range(0, N_Q, 100):
    sims = qv[s:s + 100] @ Xn.T
    top = np.argpartition(-sims, 10, axis=1)[:, :10]
    for r in range(top.shape[0]):
        t = top[r][np.argsort(-sims[r, top[r]])]
        gt.append([allids[j] for j in t])
json.dump(gt, open(f"{Q}/knn_gt.json", "w"))
print("knn queries", qv.shape, "ground truth rows", len(gt))
