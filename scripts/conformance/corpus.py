"""Deterministic synthetic corpus for the conformance suite.

Every document is a pure function of (seed, doc number, revision), so the
same seed always produces byte-identical batches — a failure reproduces
exactly by rerunning with the same `--seed`.

The text is built to exercise the tokenizer and BM25 rather than to look like
prose:

* A Zipfian vocabulary (``w0000`` most common ... ``w1499`` rarest), so the
  query generator can pick terms by document frequency band: very common
  terms stress the scoring loop, rare terms stress segment pruning and
  single-posting lists.
* A small set of "planted" tokens wrapped in ASCII punctuation, mixed case,
  interior hyphens/apostrophes and a few non-ASCII words — the cases where
  Kosha's tokenizer (whitespace split, trim ASCII punctuation, lowercase) is
  easiest to get subtly wrong.
* Two text fields (``title``, ``body``) because Kosha indexes every Text
  field into one inverted index and sums their lengths into one
  ``field_length`` — the reference model has to agree on that.

Filterable fields cover every filter-store type: Keyword (single and
multi-valued), Integer, Float, Date (ISO strings, so lexicographic order is
chronological) and Boolean.
"""

from __future__ import annotations

import random
from typing import Dict, List

VOCAB_SIZE = 1500
STATUSES = ["open", "closed", "pending", "review", "withdrawn"]
OWNERS = ["owner%02d" % i for i in range(20)]
TAGS = ["urgent", "privileged", "draft", "final", "exhibit", "sealed", "redacted"]

# Planted surface forms -> the token Kosha's tokenizer should produce.
# Documented here so a reader can see what each one is probing.
PLANTED = [
    "Breach,",  # trailing punctuation trimmed, lowercased -> "breach"
    "(Contract)",  # wrapping punctuation -> "contract"
    "\"Indemnity\"",  # quotes -> "indemnity"
    "co-op",  # interior hyphen kept -> "co-op"
    "plaintiff's",  # interior apostrophe kept -> "plaintiff's"
    "U.S.",  # trailing dot trimmed, interior kept -> "u.s"
    "ARBITRATION",  # all caps -> "arbitration"
    "café",  # non-ASCII letter -> "café"
    "Straße",  # non-ASCII, lowercase-stable -> "straße"
    "—",  # Unicode dash: not ASCII punctuation, so it survives as a token
    "...",  # pure punctuation -> dropped entirely
]


def _zipf_cdf(n: int, s: float = 1.07) -> List[float]:
    weights = [1.0 / ((i + 1) ** s) for i in range(n)]
    total = sum(weights)
    cdf, acc = [], 0.0
    for w in weights:
        acc += w / total
        cdf.append(acc)
    return cdf


_CDF = _zipf_cdf(VOCAB_SIZE)


def _word(rng: random.Random) -> str:
    x = rng.random()
    lo, hi = 0, VOCAB_SIZE - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if _CDF[mid] < x:
            lo = mid + 1
        else:
            hi = mid
    return "w%04d" % lo


def _words(rng: random.Random, lo: int, hi: int) -> str:
    out = [_word(rng) for _ in range(rng.randint(lo, hi))]
    if rng.random() < 0.15:
        out.insert(rng.randrange(len(out) + 1), rng.choice(PLANTED))
    return " ".join(out)


def doc_id(n: int) -> str:
    return "d%06d" % n


def make_doc(seed: int, n: int, revision: int = 0) -> Dict:
    """Document number ``n`` at ``revision`` (0 = original, >0 = an upsert)."""
    rng = random.Random("%d/%d/%d" % (seed, n, revision))
    fields = [
        {"name": "title", "field_type": "Text", "value": _words(rng, 2, 8)},
        {"name": "body", "field_type": "Text", "value": _words(rng, 10, 80)},
        {"name": "status", "field_type": "Keyword", "value": rng.choice(STATUSES)},
        {"name": "owner", "field_type": "Keyword", "value": OWNERS[min(int(rng.paretovariate(1.2)) - 1, 19)]},
        {"name": "priority", "field_type": "Integer", "value": str(rng.randint(1, 12))},
        {"name": "amount", "field_type": "Float", "value": repr(round(rng.uniform(0.5, 25000.0), 2))},
        {
            "name": "filed",
            "field_type": "Date",
            "value": "20%02d-%02d-%02d" % (rng.randint(18, 26), rng.randint(1, 12), rng.randint(1, 28)),
        },
        {"name": "archived", "field_type": "Boolean", "value": "true" if rng.random() < 0.2 else "false"},
    ]
    # Multi-valued keyword: the same field name repeated, 0-3 times.
    for tag in rng.sample(TAGS, rng.randint(0, 3)):
        fields.append({"name": "tags", "field_type": "Keyword", "value": tag})
    return {"id": doc_id(n), "fields": fields}


def batch(seed: int, start: int, count: int, revision: int = 0) -> List[Dict]:
    return [make_doc(seed, n, revision) for n in range(start, start + count)]
