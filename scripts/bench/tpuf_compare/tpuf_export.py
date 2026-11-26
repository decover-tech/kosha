#!/usr/bin/env python3
"""Export a turbopuffer namespace pair (text + vectors) into kosha-build-segments input.

Writes, under OUT:
  txt/shard-PP.ndjson   {"id", "text"}            one per hex id prefix PP (all chunks)
  emb/emb-PP.f32 + .ids little-endian f32 x DIM    vectors (chunkvec), same prefixes
Read-only against turbopuffer. Pages with rank_by id asc + id Gt cursor (the
documented export method), 16 prefixes in parallel.
"""
import base64, json, os, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
import numpy as np

KEY = os.environ["TURBOPUFFER_API_KEY"]
BASE = f"https://{os.environ.get('TURBOPUFFER_REGION', 'aws-us-east-1')}.turbopuffer.com"
PREFIX, OUT = sys.argv[1], sys.argv[2]  # e.g. <prefix>-chunks / <prefix>-chunkvec
DIM = 1536
PAGE = 10_000
HEX = "0123456789abcdef"


def query(ns, body):
    for attempt in range(6):
        try:
            req = urllib.request.Request(
                f"{BASE}/v2/namespaces/{ns}/query",
                data=json.dumps(body).encode(),
                headers={"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"},
            )
            return json.load(urllib.request.urlopen(req, timeout=300))
        except Exception as e:  # noqa: BLE001
            print(f"retry {ns} {attempt}: {e}", file=sys.stderr, flush=True)
            time.sleep(2 ** attempt)
    raise RuntimeError("query failed")


def bounds(p):
    lo = p
    hi = HEX[HEX.index(p) + 1] if p != "f" else None
    return lo, hi


def scan(ns, prefix, attrs, extra, sink):
    lo, hi = bounds(prefix)
    last, n = None, 0
    while True:
        f = [["id", "Gte", lo]] if last is None else [["id", "Gt", last]]
        if hi:
            f.append(["id", "Lt", hi])
        body = {"rank_by": ["id", "asc"], "top_k": PAGE, "include_attributes": attrs,
                "filters": ["And", f], **extra}
        rows = query(ns, body)["rows"]
        if not rows:
            return n
        sink(rows)
        n += len(rows)
        last = rows[-1]["id"]
        if len(rows) < PAGE:
            return n


def do_text(prefix):
    path = f"{OUT}/txt/shard-{prefix}.ndjson"
    with open(path + ".tmp", "w") as fh:
        def sink(rows):
            for r in rows:
                fh.write(json.dumps({"id": r["id"], "text": r.get("content") or ""}) + "\n")
        n = scan(f"{PREFIX}-chunks", prefix, ["content"], {}, sink)
    os.replace(path + ".tmp", path)
    return prefix, "txt", n


def do_vec(prefix):
    base = f"{OUT}/emb/emb-{prefix}"
    with open(base + ".f32.tmp", "wb") as fv, open(base + ".ids.tmp", "w") as fi:
        def sink(rows):
            for r in rows:
                v = r.get("vector")
                if not v:
                    continue
                a = np.frombuffer(base64.b64decode(v), dtype="<f2")
                assert a.shape == (DIM,), (r["id"], a.shape)
                fv.write(a.astype("<f4").tobytes())
                fi.write(r["id"] + "\n")
        n = scan(f"{PREFIX}-chunkvec", prefix, ["vector"], {"vector_encoding": "base64"}, sink)
    os.replace(base + ".f32.tmp", base + ".f32")
    os.replace(base + ".ids.tmp", base + ".ids")
    return prefix, "vec", n


os.makedirs(f"{OUT}/txt", exist_ok=True)
os.makedirs(f"{OUT}/emb", exist_ok=True)
t = time.time()
jobs = [(do_text, p) for p in HEX if not os.path.exists(f"{OUT}/txt/shard-{p}.ndjson")]
jobs += [(do_vec, p) for p in HEX if not os.path.exists(f"{OUT}/emb/emb-{p}.f32")]
with ThreadPoolExecutor(16) as ex:
    for prefix, kind, n in ex.map(lambda j: j[0](j[1]), jobs):
        print(f"{kind} {prefix} {n} rows  t={time.time()-t:.0f}s", flush=True)
print("EXPORT-COMPLETE", flush=True)
