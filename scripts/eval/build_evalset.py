"""Build a frozen eval set for one matter from its exported records.

Every expected answer is computed here, directly from the records, with no
search engine involved, so the set stays valid across Kosha versions: it
describes what a correct engine must return, not how Kosha works inside.

Usage: build_evalset.py <export_dir> <out_dir> [--version v1]
  <export_dir>  chunks-*.ndjson.gz, one {"_id", "_source"} per line
Writes <out_dir>/cases.jsonl.gz (one case per line) and manifest.json.

A case is {"id", "area", "tier", "request", "expect"}. `request` is an
OpenSearch-style body, exactly what the backend sends. `expect` holds any of:
  total        exact hit count
  ids          every matching chunk id (omitted above ID_CAP)
  ids_ref      id of another case whose `ids` the hits must come from
  buckets      {"agg": name, "counts": {key: n}} for a terms aggregation
  value        {"agg": name, "value": n} for a metric aggregation
  groups       number of distinct documents on a collapsed page
  ordered_ids  the exact first hits, in order
  neighbours   exact nearest neighbours [[id, cosine], ...], best first: the
               top 30 plus every chunk tied with the 10th (duplicate and
               padded chunks share a vector). "ambiguous": true marks a case
               with more ties than are stored; it is timed but not scored
Tier "quick" marks the small subset run after every change.

Text rules (the contract being tested): split on whitespace, trim ASCII
punctuation from both ends, lowercase; a word or phrase matches a chunk when
it is in the field the query names (`content`), as in OpenSearch. Sets up to
v2 used "in any string field", which is what Kosha did before per-field
indexes; `in_any_field` keeps that count for reference.
"""
import argparse
import collections
import datetime
import glob
import gzip
import hashlib
import json
import os
import random
import re
import string
import time

import numpy as np

ID_CAP = 20000  # largest id set stored per case
DIM = 1536
EMB = re.compile(r'"contentEmbedding":\s*\[([^\]]*)\],?\s*')
PUNCT = string.punctuation
QUICK = {"matter": 9, "word": 10, "word_per_document": 5, "word_grouped": 5, "and": 3, "or": 3, "phrase": 6, "document": 3, "tag": 3,
         "range": 2, "word_filtered": 3, "knn": 10, "knn_filtered": 3}

ap = argparse.ArgumentParser()
ap.add_argument("export_dir")
ap.add_argument("out_dir")
ap.add_argument("--version", default="v3")
args = ap.parse_args()
os.makedirs(args.out_dir, exist_ok=True)
random.seed(11)
files = sorted(glob.glob(os.path.join(args.export_dir, "chunks-*.ndjson.gz")))


def tokens(text):
    return [w.lower() for w in (t.strip(PUNCT) for t in text.split()) if w]


def records(path):
    """(id, source without the embedding, embedding text or None) per line."""
    with gzip.open(path, "rt") as fh:
        for line in fh:
            m = EMB.search(line)
            emb = m.group(1) if m else None
            if m:
                line = re.sub(r",\s*}", "}", line[: m.start()] + line[m.end():])
            d = json.loads(line)
            yield d["_id"], d["_source"], emb


def has_phrase(toks, want):
    n = len(want)
    return any(toks[j] == want[0] and toks[j:j + n] == want for j in range(len(toks) - n + 1))


def when(value):
    try:
        return datetime.datetime.fromisoformat(value) if value else None
    except ValueError:
        return None


# ── Pick the cases from a sample spread over the export ────────────────────
df, doc_chunks, phrases, knn_ids, formats, pages, bates, sent = {}, {}, [], [], collections.Counter(), [], [], []
matter = None
for path in files[:: max(1, len(files) // 4)][:4]:
    for i, (cid, src, emb) in enumerate(records(path)):
        matter = matter or src["matterId"]
        toks = tokens(src.get("content") or "")
        for t in set(toks):
            if t.isalpha() and 4 <= len(t) <= 14:
                df[t] = df.get(t, 0) + 1
        doc_chunks[src["documentId"]] = doc_chunks.get(src["documentId"], 0) + 1
        formats.update(src.get("fileFormats") or [])
        pages.append(src.get("pageNumber") or 0)
        if src.get("batesStart") is not None:
            bates.append(src["batesStart"])
        if when(src.get("sentAt")):
            sent.append(when(src["sentAt"]))
        if i % 101 == 0 and len(phrases) < 200:
            for j in range(len(toks) - 2):
                if all(t.isalpha() and len(t) >= 4 for t in toks[j:j + 3]):
                    phrases.append(" ".join(toks[j:j + random.choice((2, 3))]))
                    break
        if emb and i % 53 == 0 and len(knn_ids) < 400:
            knn_ids.append(cid)
        if i >= 12000:
            break
ranked = sorted(df.items(), key=lambda kv: -kv[1])
COMMON = random.sample([w for w, _ in ranked[:80]], 20)
MEDIUM = random.sample([w for w, n in ranked if 20 <= n <= 400], 50)
WORDS = COMMON + MEDIUM + random.sample([w for w, n in ranked if 2 <= n <= 5], 30)
random.shuffle(WORDS)  # so the quick tier mixes common, medium and rare
# A common word with a medium one, so most AND cases match something.
PAIRS = [(random.choice(COMMON), m) for m in random.sample(MEDIUM, 20)]
PHRASES = random.sample(list(dict.fromkeys(phrases)), min(50, len(set(phrases))))
by_size = sorted(doc_chunks.items(), key=lambda kv: -kv[1])
DOCS = [d for d, _ in by_size[:5]] + random.sample([d for d, n in by_size if 2 <= n <= 40], 15)
random.shuffle(DOCS)
KNN = set(random.sample(knn_ids, min(100, len(knn_ids))))
FORMATS = [f for f, _ in formats.most_common(3)]


def quantiles(values, cuts):
    values = sorted(values)
    return [values[int(len(values) * c)] for c in cuts] if values else []


PAGE_RANGES = [(0, 0)] + [(lo, hi) for lo, hi in zip(quantiles(pages, (0.5, 0.9, 0.2)), quantiles(pages, (0.9, 0.999, 0.6))) if lo <= hi]
q = quantiles(bates, (0.1, 0.4, 0.5, 0.9))
BATES_RANGES = [(q[0], q[1]), (q[2], q[3])] if q else []
q = quantiles(sent, (0.1, 0.5, 0.5, 0.95))
SENT_RANGES = [(q[0], q[1]), (q[2], q[3])] if q else []
print(f"matter {matter}: {len(WORDS)} words, {len(PHRASES)} phrases, {len(DOCS)} docs, {len(KNN)} knn", flush=True)

# ── One pass over every record ─────────────────────────────────────────────
wset, ptoks = set(WORDS), {p: p.split() for p in PHRASES}
word_ids = {w: [] for w in WORDS}
word_any_field = collections.Counter()  # for reference: chunks with the word in any string field
word_docs = {w: collections.Counter() for w in WORDS}
word_format = {(w, f): [] for w in WORDS[:10] for f in FORMATS[:1]}
phrase_ids = {p: [] for p in PHRASES}
doc_pages = {d: [] for d in DOCS}
counts = collections.defaultdict(collections.Counter)  # field -> value -> chunks
range_counts = collections.Counter()
all_docs, total, n_vec = set(), 0, 0
vec_ids, vec_format = [], []
capacity = 600000
vecs = np.lib.format.open_memmap(os.path.join(args.out_dir, "vectors.tmp.npy"), mode="w+", dtype=np.float32, shape=(capacity, DIM))
knn_query = {}
sha = hashlib.sha256()
t0 = time.time()
for path in files:
    sha.update(os.path.basename(path).encode() + str(os.path.getsize(path)).encode())
    for cid, src, emb in records(path):
        total += 1
        doc = src["documentId"]
        all_docs.add(doc)
        content_tokens = tokens(src.get("content") or "")
        content = set(content_tokens)
        elsewhere = set().union(*(tokens(v) for k, v in src.items() if k != "content" and isinstance(v, str)))
        fmts = src.get("fileFormats") or []
        for w in wset & (content | elsewhere):
            word_any_field[w] += 1
            if w not in content:
                continue
            word_ids[w].append(cid)
            word_docs[w][doc] += 1
            for f in fmts:
                if (w, f) in word_format:
                    word_format[(w, f)].append(cid)
        for p, want in ptoks.items():
            if want[0] in content and has_phrase(content_tokens, want):
                phrase_ids[p].append(cid)
        if doc in doc_pages:
            doc_pages[doc].append((src.get("pageNumber") or 0, cid))
        for field in ("system_tags", "user_defined_tags", "fileFormats"):
            for v in set(src.get(field) or []):
                counts[field][v] += 1
        counts["isHot"][bool(src.get("isHot"))] += 1
        page = src.get("pageNumber") or 0
        for lo, hi in PAGE_RANGES:
            range_counts[("pageNumber", lo, hi)] += lo <= page <= hi
        if src.get("batesStart") is not None:
            for lo, hi in BATES_RANGES:
                range_counts[("batesStart", lo, hi)] += lo <= src["batesStart"] <= hi
        sent_at = when(src.get("sentAt"))
        if sent_at:
            for lo, hi in SENT_RANGES:
                range_counts[("sentAt", lo, hi)] += lo <= sent_at <= hi
        if emb:
            v = np.fromstring(emb, dtype=np.float32, sep=",")
            if v.shape[0] == DIM and n_vec < capacity:
                vecs[n_vec] = v
                vec_ids.append(cid)
                vec_format.append(FORMATS[0] in fmts if FORMATS else False)
                if cid in KNN:
                    knn_query[cid] = v.copy()
                n_vec += 1
    print(f"scanned {os.path.basename(path)} total={total} {time.time() - t0:.0f}s", flush=True)

# ── Exact nearest neighbours by brute force ────────────────────────────────
# One pass over the vectors for all queries at once: the matrix is larger
# than memory, so reading it once per query would take hours.
knn_cases = list(knn_query.items())
Q = np.stack([v / (np.linalg.norm(v) or 1.0) for _, v in knn_cases])
in_format = np.array(vec_format, dtype=bool)


TIE_SCAN = 500  # how deep to look for chunks tied with the 10th neighbour


def all_neighbours(queries, mask=None, k=TIE_SCAN):
    best_sim = np.full((len(queries), k), -2.0, dtype=np.float32)
    best_row = np.zeros((len(queries), k), dtype=np.int64)
    for a in range(0, n_vec, 50000):
        block = np.asarray(vecs[a:a + 50000][: n_vec - a])
        sims = (block @ queries.T) / np.maximum(np.linalg.norm(block, axis=1), 1e-12)[:, None]
        if mask is not None:
            sims[~mask[a:a + len(block)]] = -2.0
        sims = np.concatenate([best_sim, sims.T], axis=1)
        rows = np.concatenate([best_row, np.broadcast_to(np.arange(a, a + len(block)), (len(queries), len(block)))], axis=1)
        keep = np.argpartition(-sims, k, axis=1)[:, :k]
        best_sim, best_row = np.take_along_axis(sims, keep, 1), np.take_along_axis(rows, keep, 1)
    order = np.argsort(-best_sim, axis=1)
    best_sim, best_row = np.take_along_axis(best_sim, order, 1), np.take_along_axis(best_row, order, 1)
    found = []
    for rows, sims in zip(best_row, best_sim):
        ranked = [[vec_ids[r], round(float(x), 6)] for r, x in zip(rows, sims) if x > -2.0]
        tied = sum(1 for _, x in ranked if x >= ranked[min(9, len(ranked) - 1)][1] - 1e-5)
        found.append((ranked[: max(30, tied)], tied >= len(ranked) and len(ranked) == k))
    return found


exact = all_neighbours(Q)
exact_in_format = all_neighbours(Q[:10], in_format)
print(f"exact neighbours for {len(knn_cases)} queries {time.time() - t0:.0f}s", flush=True)

# ── Cases ──────────────────────────────────────────────────────────────────
M = {"bool": {"should": [{"term": {"matterId": matter}}, {"term": {"productionId": matter}}]}}
cases, seen = [], collections.Counter()


def case(area, request, **expect):
    seen[area] += 1
    cid = f"{area}-{seen[area]:03d}"
    tier = "quick" if seen[area] <= QUICK.get(area, 0) else "standard"
    cases.append({"id": cid, "area": area, "tier": tier, "request": request, "expect": expect})
    return cid


def scoped(*filters, must=None):
    body = {"bool": {"filter": [M, *filters]}}
    if must:
        body["bool"]["must"] = [must]
    return body


def match(text, operator=None):
    return {"match": {"content": {"query": text, "operator": operator} if operator else text}}


def ids_or_none(ids):
    return {"ids": ids} if len(ids) <= ID_CAP else {}


def top_buckets(counter, size=1000):
    """What a terms aggregation of `size` must return: the largest buckets."""
    top = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))[:size]
    return {"counts": dict(top), "truncated": len(counter) > size}


def counted(area, query, total):
    return case(area, {"size": 0, "track_total_hits": True, "query": query}, total=total)


counted("matter", scoped(), total)
case("matter", {"size": 0, "query": scoped(), "aggs": {"documents": {"cardinality": {"field": "documentId"}}}},
     value={"agg": "documents", "value": len(all_docs)})
for field in ("fileFormats", "system_tags"):
    case("matter", {"size": 0, "query": scoped(), "aggs": {"by": {"terms": {"field": field, "size": 1000}}}},
         buckets={"agg": "by", **top_buckets(counts[field])})
for hot in (True, False):
    counted("matter", scoped({"term": {"isHot": hot}}), counts["isHot"][hot])

for w in WORDS:
    ref = case("word", {"size": 20, "track_total_hits": True, "_source": ["documentId"], "query": scoped(must=match(w))},
               total=len(word_ids[w]), in_any_field=word_any_field[w], **ids_or_none(word_ids[w]))
    top = top_buckets(word_docs[w])
    case("word_per_document", {"size": 0, "query": scoped(must=match(w)),
                               "aggs": {"per_document": {"terms": {"field": "documentId", "size": 1000}}}},
         buckets={"agg": "per_document", **top})
    case("word_grouped", {"size": 20, "_source": ["documentId"], "collapse": {"field": "documentId"},
                          "query": scoped(must=match(w))},
         groups=min(20, len(word_docs[w])), **({"ids_ref": ref} if len(word_ids[w]) <= ID_CAP else {}))
for (w, f), ids in word_format.items():
    case("word_filtered", {"size": 20, "track_total_hits": True, "_source": False,
                           "query": scoped({"term": {"fileFormats": f}}, must=match(w))}, total=len(ids), **ids_or_none(ids))
for a, b in PAIRS:
    both = sorted(set(word_ids[a]) & set(word_ids[b]))
    either = sorted(set(word_ids[a]) | set(word_ids[b]))
    for area, op, ids in (("and", "and", both), ("or", "or", either)):
        case(area, {"size": 20, "track_total_hits": True, "_source": False, "query": scoped(must=match(f"{a} {b}", op))},
             total=len(ids), **ids_or_none(ids))
for p in PHRASES:
    case("phrase", {"size": 20, "track_total_hits": True, "_source": False,
                    "query": scoped(must={"match_phrase": {"content": p}})}, total=len(phrase_ids[p]), **ids_or_none(phrase_ids[p]))
for d in DOCS:
    ordered = [c for _, c in sorted(doc_pages[d])]
    case("document", {"size": 200, "track_total_hits": True, "_source": ["pageNumber"],
                      "sort": [{"pageNumber": {"order": "asc"}}, {"_id": {"order": "asc"}}],
                      "query": scoped({"term": {"documentId": d}})}, total=len(ordered), ordered_ids=ordered[:200])
for field in ("system_tags", "user_defined_tags", "fileFormats"):
    for value, n in counts[field].most_common(6):
        counted("tag", scoped({"term": {field: value}}), n)
for (field, lo, hi), n in range_counts.items():
    bounds = {"gte": lo.isoformat(), "lte": hi.isoformat()} if field == "sentAt" else {"gte": lo, "lte": hi}
    counted("range", scoped({"range": {field: bounds}}), n)
def knn_body(vector, knn_filter):
    return {"size": 10, "_source": False, "query": {"knn": {"contentEmbedding": {
        "vector": [float(x) for x in vector], "k": 30, "filter": knn_filter}}}}


def knn_case(area, cid, vector, knn_filter, found):
    neighbours, ambiguous = found
    case(area, knn_body(vector, knn_filter), neighbours=neighbours, query_chunk=cid, **({"ambiguous": True} if ambiguous else {}))


for (cid, vector), found in zip(knn_cases, exact):
    knn_case("knn", cid, vector, M, found)
for (cid, vector), found in zip(knn_cases, exact_in_format):
    knn_case("knn_filtered", cid, vector, scoped({"term": {"fileFormats": FORMATS[0]}}), found)

with gzip.open(os.path.join(args.out_dir, "cases.jsonl.gz"), "wt") as fh:
    for c in cases:
        fh.write(json.dumps(c) + "\n")
manifest = {
    "matter": matter, "version": args.version, "built_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "source": {"files": len(files), "fingerprint": sha.hexdigest()},
    "chunks": total, "documents": len(all_docs), "chunks_with_vector": n_vec,
    "text_rules": "whitespace split; trim ASCII punctuation; lowercase; a word or phrase matches in the queried field (content)",
    "cases": dict(seen), "quick_cases": sum(c["tier"] == "quick" for c in cases), "id_cap": ID_CAP,
}
json.dump(manifest, open(os.path.join(args.out_dir, "manifest.json"), "w"), indent=1)
del vecs
os.remove(os.path.join(args.out_dir, "vectors.tmp.npy"))
print("EVALSET_DONE", json.dumps(manifest), flush=True)
