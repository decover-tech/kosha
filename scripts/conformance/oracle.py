"""In-memory reference model of Kosha's write path and BM25/filter read path.

The model is deliberately brute force: every search scans every live
document. That makes it slow but obviously correct, which is the point — it
is the thing Kosha's answers are checked against.

What it models, and where each rule comes from in the engine:

Visibility (kosha-write ``Indexer``)
  * Indexed documents sit in a per-namespace buffer and are NOT searchable
    until a flush turns the buffer into a segment.
  * Re-indexing an id that is still buffered replaces the buffered copy.
  * Re-indexing an id that is live in a segment rewrites every segment
    holding any id in the batch: the buffer is flushed first, the affected
    segments are dropped, and their other live docs plus the new versions
    are written out as one new segment (``rewrite_documents``). Re-indexing
    an id whose only copy is tombstoned is an ordinary buffered append.
  * Delete-by-query tombstones matching docs in *flushed* segments only.
    Buffered docs are not visible, so delete-by-query does not see them —
    the same as Elasticsearch's delete_by_query against unrefreshed docs.
  * Compaction merges every segment under the size cap into one segment of
    live docs, dropping tombstoned docs.

Scoring (kosha-query ``Bm25Scorer`` / ``score_segment``)
  * BM25 statistics are PER SEGMENT: N = docs in the segment (tombstoned
    docs included — segments are immutable), avgdl = mean field length over
    those docs, df = postings length for the term in that segment. So a
    document's score legitimately changes after compaction; the model tracks
    the segment layout so it can predict exact scores, not just rankings.
  * idf = ln(1 + (N - df + 0.5) / (df + 0.5)); k1=1.2, b=0.75 by default.
  * All Text fields share one inverted index; field_length is the total
    token count across a document's Text fields.
  * Multi-term queries are AND by default (every term required); a single
    term, or ``operator: or``, is a union scored over the terms present.
  * A filter-only query (no query text) scores every passing doc with
    score_term(tf=1, df=N, field_length) — a small, length-normalised
    constant rather than 1.0.

Filters (kosha-query ``FilterApplier``)
  * term / terms compare against string-typed values (Keyword, Boolean,
    Date, Text); a doc with a multi-valued field matches if any value does.
  * range is numeric on Integer / Float fields and lexicographic otherwise.
  * bool: must narrows, must_not subtracts, and a non-empty should list is
    always required (at least minimum_should_match clauses), even alongside
    must.

Where Kosha's current behavior looks like a bug rather than a design choice,
the model encodes the CORRECT (Elasticsearch-compatible) behavior and the
check is listed in ``expected_failures.json`` with its issue, so it flips to
a visible "unexpectedly passing" the day the bug is fixed. Those spots are
marked ``# ES-correct`` below.
"""

from __future__ import annotations

import math
from collections import OrderedDict
from typing import Dict, Iterable, List, Optional, Tuple

ASCII_PUNCT = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
NUMERIC_TYPES = ("Integer", "Float")


def tokenize(text: str) -> List[str]:
    """Mirror of ``kosha_segment::tokenize``: whitespace split, trim ASCII
    punctuation from both ends, lowercase, drop empties."""
    out = []
    for word in text.split():
        w = word.strip(ASCII_PUNCT)
        if w:
            out.append(w.lower())
    return out


class Doc:
    __slots__ = ("id", "fields", "terms", "length")

    def __init__(self, raw: Dict):
        self.id = raw["id"]
        self.fields = raw["fields"]
        self.terms: Dict[str, int] = {}
        length = 0
        for f in self.fields:
            if f["field_type"] == "Text":
                toks = tokenize(f["value"])
                length += len(toks)
                for t in toks:
                    self.terms[t] = self.terms.get(t, 0) + 1
        self.length = length

    def values(self, name: str) -> List[Tuple[str, str]]:
        return [(f["field_type"], f["value"]) for f in self.fields if f["name"] == name]


class Segment:
    _next = 0

    def __init__(self, docs: List[Doc]):
        Segment._next += 1
        self.seq = Segment._next
        self.docs = docs
        self.tombstones = set()  # doc ids
        self.df: Dict[str, int] = {}
        total = 0
        for d in docs:
            total += d.length
            for t in d.terms:
                self.df[t] = self.df.get(t, 0) + 1
        self.n = len(docs)
        self.avgdl = (total / self.n) if self.n else 0.0

    def live(self) -> Iterable[Doc]:
        return (d for d in self.docs if d.id not in self.tombstones)


def score_term(tf: int, df: int, n: int, dl: int, avgdl: float, k1: float, b: float) -> float:
    if tf == 0 or df == 0 or n == 0:
        return 0.0
    idf = math.log(1.0 + (n - df + 0.5) / (df + 0.5))
    return idf * (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * dl / avgdl))


# ─── filters ────────────────────────────────────────────────────────────────


def _parse_num(s: str, kind: str):
    try:
        return int(s) if kind == "Integer" else float(s)
    except (TypeError, ValueError):
        return None


def _in_range(value: str, kind: str, bound: Dict[str, str]) -> bool:
    if kind in NUMERIC_TYPES:
        v = _parse_num(value, kind)
        if v is None:
            return False
        conv = lambda s: _parse_num(s, kind)  # noqa: E731
    else:
        v = value
        conv = lambda s: s  # noqa: E731
    for op, raw in bound.items():
        b = conv(raw)
        if b is None:  # unparseable bound is ignored, as in check_i64/check_f64
            continue
        if op == "gte" and not v >= b:
            return False
        if op == "gt" and not v > b:
            return False
        if op == "lte" and not v <= b:
            return False
        if op == "lt" and not v < b:
            return False
    return True


def matches(clause: Dict, doc: Doc) -> bool:
    (key, body), = clause.items()
    if key == "match_all":
        return True
    if key == "term":
        (field, want), = body.items()
        # ES-correct: a term on a numeric field compares numerically.
        for kind, val in doc.values(field):
            if kind in NUMERIC_TYPES:
                if _parse_num(val, kind) == _parse_num(want, kind):
                    return True
            elif val == want:
                return True
        return False
    if key == "terms":
        (field, wants), = body.items()
        return any(matches({"term": {field: w}}, doc) for w in wants)
    if key == "range":
        (field, bound), = body.items()
        return any(_in_range(val, kind, bound) for kind, val in doc.values(field))
    if key == "bool":
        if not all(matches(c, doc) for c in body.get("must", [])):
            return False
        if any(matches(c, doc) for c in body.get("must_not", [])):
            return False
        should = body.get("should", [])
        if should:
            need = max(1, body.get("minimum_should_match", 1))
            if sum(1 for c in should if matches(c, doc)) < need:
                return False
        return True
    raise ValueError("unknown filter clause %r" % key)


# ─── the model ──────────────────────────────────────────────────────────────


class Oracle:
    def __init__(self, compact_max_docs: int = 50_000):
        self.buffer: "OrderedDict[str, Doc]" = OrderedDict()
        self.segments: List[Segment] = []
        self.compact_max_docs = compact_max_docs

    # writes ---------------------------------------------------------------

    def _segment_ids(self) -> Dict[str, Segment]:
        """Live (non-tombstoned) flushed ids -> their segment. A deleted id
        that is indexed again is a plain append, not a rewrite
        (``has_flushed_ids_in`` skips tombstoned locations)."""
        out = {}
        for seg in self.segments:
            for d in seg.live():
                out[d.id] = seg
        return out

    def index(self, raw_docs: List[Dict]) -> None:
        latest: "OrderedDict[str, Doc]" = OrderedDict()
        for raw in raw_docs:  # last copy of an id within one request wins
            latest.pop(raw["id"], None)
            latest[raw["id"]] = Doc(raw)
        for doc_id in latest:
            self.buffer.pop(doc_id, None)
        flushed = self._segment_ids()
        if not any(i in flushed for i in latest):
            self.buffer.update(latest)
            return
        # rewrite_documents: flush, drop every segment holding a replaced id,
        # carry their other live docs, append the new versions, flush again.
        self.flush()
        flushed = self._segment_ids()
        hit = {id(flushed[i]) for i in latest if i in flushed}
        carried: List[Doc] = []
        keep = []
        for seg in self.segments:
            if id(seg) in hit:
                carried.extend(d for d in seg.live() if d.id not in latest)
            else:
                keep.append(seg)
        self.segments = keep
        new_docs = carried + list(latest.values())
        if new_docs:
            self.segments.append(Segment(new_docs))

    def flush(self) -> None:
        if self.buffer:
            self.segments.append(Segment(list(self.buffer.values())))
            self.buffer = OrderedDict()

    def delete(self, flt: Dict) -> int:
        n = 0
        for seg in self.segments:
            for d in seg.live():
                if matches(flt, d):
                    seg.tombstones.add(d.id)
                    n += 1
        return n

    def compact(self) -> None:
        small = [s for s in self.segments if s.n < self.compact_max_docs]
        if len(small) < 2:
            return
        merged = [d for s in small for d in s.live()]
        self.segments = [s for s in self.segments if s.n >= self.compact_max_docs]
        if merged:
            self.segments.append(Segment(merged))

    # reads ----------------------------------------------------------------

    def live_docs(self) -> Dict[str, Doc]:
        return {d.id: d for seg in self.segments for d in seg.live()}

    def hits(self, q: Dict) -> List[Tuple[str, float, Doc]]:
        """Every matching live doc as (id, score, doc), in final sort order."""
        terms = tokenize(q.get("query_text", ""))
        flt = q.get("filter")
        params = q.get("bm25_params") or {}
        k1, b = params.get("k1", 1.2), params.get("b", 0.75)
        use_and = len(terms) > 1 and q.get("operator", "and") == "and"
        out = []
        for seg in self.segments:
            for d in seg.live():
                if terms:
                    present = [t for t in terms if t in d.terms]
                    if not present or (use_and and len(present) != len(terms)):
                        continue
                    score = sum(
                        score_term(d.terms[t], seg.df[t], seg.n, d.length, seg.avgdl, k1, b) for t in present
                    )
                    if flt is not None and not matches(flt, d):
                        continue
                elif flt is not None:
                    if not matches(flt, d):
                        continue
                    score = score_term(1, seg.n, seg.n, d.length, seg.avgdl, k1, b)
                else:
                    continue
                out.append((d.id, score, d))
        out.sort(key=lambda h: sort_key(h, q.get("sort") or []))
        return out


def _sort_value(doc: Doc, field: str):
    vals = doc.values(field)
    if not vals:
        return (0, "")  # missing sorts first ascending, as Kosha does today
    kind, val = vals[0]
    if kind in NUMERIC_TYPES:  # ES-correct: numeric fields sort numerically
        return (1, _parse_num(val, kind))
    return (1, val)


class _Desc:
    __slots__ = ("v",)

    def __init__(self, v):
        self.v = v

    def __lt__(self, other):
        return other.v < self.v

    def __eq__(self, other):
        return self.v == other.v


def sort_key(hit, sort: List[Dict]):
    doc_id, score, doc = hit
    if not sort:
        return (-score, doc_id)
    key = []
    for spec in sort:
        (field, order), = spec.items()
        if field == "_score":
            v = score
        elif field == "_id":
            v = doc_id
        else:
            v = _sort_value(doc, field)
        key.append(_Desc(v) if order.get("order") == "desc" else v)
    key.append(doc_id)
    return tuple(key)


def cursor_for(hit, sort: List[Dict]) -> Optional[List[str]]:
    """The ``search_after`` value a client would send after ``hit``."""
    doc_id, score, doc = hit
    out = []
    for spec in sort:
        (field,) = spec.keys()
        if field == "_id":
            out.append(doc_id)
        elif field == "_score":
            out.append(repr(score))
        else:
            vals = doc.values(field)
            out.append(vals[0][1] if vals else "")
    return out
