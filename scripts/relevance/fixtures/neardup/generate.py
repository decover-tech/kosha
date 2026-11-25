#!/usr/bin/env python3
"""Deterministic near-duplicate relevance fixture (legal-matter shaped).

    python3 scripts/relevance/fixtures/neardup/generate.py [--out DIR]

Writes corpus.jsonl, queries.jsonl, qrels.txt and fixture.json next to this
script (or into --out). Re-running it reproduces the committed files byte for
byte: everything is drawn from one seeded `random.Random`.

Why this fixture exists: on a real legal matter (2026-11-25, 1.17M chunks)
kNN recall@10 was 0.81 against a competitor's 0.99, and the cause was probe
selection on a corpus full of near-duplicates — boilerplate clauses, repeated
exhibits, re-produced documents. The topic-mixture `synthetic` fixture has
none of that shape, so a change that hurts probe ranking on duplicate-heavy
data passes its gate. This one is built to fail it.

Shape:

  * DOCS documents in FAMILIES families (a template clause, an exhibit that
    is produced many times). Family sizes are Zipf-like: a few families have
    dozens of members, most have a handful.
  * Families live in REGIONS regions of vector space, so neighbouring
    families are close and the 10th neighbour of a query is often a tie
    between members of different families.
  * A member's vector is its family's vector plus tiny noise; EXACT_DUP of
    members copy another member's vector exactly (a re-produced document).
    Its text is the family template with a couple of words changed.
  * Queries: half are "family" queries (a family's vector plus noise, text
    from its template), half follow the 2026-11-25 protocol — the
    normalised sum of two corpus vectors — which lands between families.
    Every QUERY_FILTER_EVERY-th query filters on `lang`.

Judgments: a document is relevant (grade 1) to a query when it is in the
query's family (family queries) or in either source document's family
(sum queries). Vector recall in the harness is tie-tolerant, which is the
only meaningful recall on this data.
"""

from __future__ import annotations

import argparse
import json
import math
import random
from pathlib import Path

SEED = 1125
DOCS = 1900
FAMILIES = 150
REGIONS = 12
DIM = 16
REGION_SPREAD = 0.45  # family vector = region centre + N(0, spread)
MEMBER_NOISE = 0.015  # member vector = family vector + N(0, noise)
EXACT_DUP = 0.25  # share of members that copy another member's vector
TEMPLATE_WORDS = 12
MUTATIONS = 2
BACKGROUND_WORDS = 200
FAMILY_QUERIES = 60
SUM_QUERIES = 60
QUERY_NOISE = 0.05
QUERY_FILTER_EVERY = 6

CONSONANTS = "bcdfghjklmnprstvz"
VOWELS = "aeiou"


def make_word(rng: random.Random, syllables: int) -> str:
    return "".join(rng.choice(CONSONANTS) + rng.choice(VOWELS) for _ in range(syllables))


def unique_words(rng: random.Random, n: int, syllables: int, taken: set[str]) -> list[str]:
    out = []
    while len(out) < n:
        w = make_word(rng, syllables)
        if w not in taken:
            taken.add(w)
            out.append(w)
    return out


def r4(x: float) -> float:
    return round(x, 3)


def normalise(v: list[float]) -> list[float]:
    n = math.sqrt(sum(a * a for a in v)) or 1.0
    return [a / n for a in v]


def family_sizes(rng: random.Random) -> list[int]:
    """Zipf-like sizes summing to DOCS, every family at least 2."""
    weights = [1.0 / (i + 1) ** 0.9 for i in range(FAMILIES)]
    total = sum(weights)
    spare = DOCS - 2 * FAMILIES
    sizes = [2 + int(spare * w / total) for w in weights]
    i = 0
    while sum(sizes) < DOCS:
        sizes[i % FAMILIES] += 1
        i += 1
    rng.shuffle(sizes)
    return sizes


def generate() -> tuple[list[dict], list[dict], list[tuple[str, str, int]]]:
    rng = random.Random(SEED)
    taken: set[str] = set()
    background = unique_words(rng, BACKGROUND_WORDS, 2, taken)
    region_words = [unique_words(rng, 10, 3, taken) for _ in range(REGIONS)]
    # One rare "matter term" per family: what a lawyer would type to find it.
    family_terms = unique_words(rng, FAMILIES, 4, taken)
    centres = [[rng.uniform(-1.0, 1.0) for _ in range(DIM)] for _ in range(REGIONS)]
    bg_weights = [1.0 / (i + 1) for i in range(BACKGROUND_WORDS)]

    fam_region = [rng.randrange(REGIONS) for _ in range(FAMILIES)]
    fam_vec = [
        [c + rng.gauss(0.0, REGION_SPREAD) for c in centres[fam_region[f]]] for f in range(FAMILIES)
    ]
    templates = []
    for f in range(FAMILIES):
        words = []
        for _ in range(TEMPLATE_WORDS):
            r = rng.random()
            if r < 0.35:
                words.append(rng.choice(region_words[fam_region[f]]))
            else:
                words.append(rng.choices(background, bg_weights)[0])
        words.insert(rng.randrange(len(words) + 1), family_terms[f])
        templates.append(words)

    # Interleave families so every segment holds members of most of them,
    # the way re-produced documents arrive over a matter's life.
    slots = [f for f, n in enumerate(family_sizes(rng)) for _ in range(n)]
    rng.shuffle(slots)

    docs, doc_family, doc_lang, doc_vecs = [], [], [], []
    members: dict[int, list[int]] = {}
    for i, f in enumerate(slots):
        prior = members.setdefault(f, [])
        if prior and rng.random() < EXACT_DUP:
            vec = list(doc_vecs[rng.choice(prior)])
        else:
            vec = [r4(a + rng.gauss(0.0, MEMBER_NOISE)) for a in fam_vec[f]]
        body = list(templates[f])
        for _ in range(rng.randrange(MUTATIONS + 1)):
            body[rng.randrange(len(body))] = rng.choices(background, bg_weights)[0]
        lang = "en" if rng.random() < 0.75 else "fr"
        docs.append(
            {
                "id": f"nd{i:05d}",
                "fields": {
                    "body": " ".join(body),
                    "lang": lang,
                    "family": f"f{f:03d}",
                },
                "vector": vec,
            }
        )
        prior.append(i)
        doc_family.append(f)
        doc_lang.append(lang)
        doc_vecs.append(vec)

    queries, qrels = [], []

    def add(qid: str, text: str, vector: list[float], fams: set[int]) -> None:
        query = {"qid": qid, "text": text, "vector": [r4(a) for a in vector]}
        flt = None
        if (len(queries) + 1) % QUERY_FILTER_EVERY == 0:
            flt = {"term": {"lang": "en"}}
            query["filter"] = flt
        queries.append(query)
        for i, f in enumerate(doc_family):
            if f in fams and (flt is None or doc_lang[i] == "en"):
                qrels.append((qid, f"nd{i:05d}", 1))

    # Family queries favour big families: that is where the ties are.
    big = sorted(members, key=lambda f: -len(members[f]))
    for j in range(FAMILY_QUERIES):
        f = big[j % 40] if j % 2 == 0 else rng.randrange(FAMILIES)
        words = [family_terms[f]] + rng.sample(templates[f], 2)
        vec = [a + rng.gauss(0.0, QUERY_NOISE) for a in fam_vec[f]]
        add(f"fq{j:03d}", " ".join(words), vec, {f})
    for j in range(SUM_QUERIES):
        a, b = rng.sample(range(len(docs)), 2)
        vec = normalise([x + y for x, y in zip(normalise(doc_vecs[a]), normalise(doc_vecs[b]))])
        fa, fb = doc_family[a], doc_family[b]
        words = [family_terms[fa], family_terms[fb], rng.choice(templates[fa])]
        add(f"sq{j:03d}", " ".join(words), vec, {fa, fb})
    return docs, queries, qrels


def write(out: Path) -> None:
    docs, queries, qrels = generate()
    out.mkdir(parents=True, exist_ok=True)
    dump = lambda o: json.dumps(o, separators=(",", ":"), sort_keys=True)  # noqa: E731
    (out / "corpus.jsonl").write_text("".join(dump(d) + "\n" for d in docs))
    (out / "queries.jsonl").write_text("".join(dump(q) + "\n" for q in queries))
    (out / "qrels.txt").write_text("".join(f"{q} 0 {d} {g}\n" for q, d, g in qrels))
    meta = {
        "description": "Near-duplicate, legal-matter-shaped fixture; see generate.py",
        "seed": SEED,
        "keyword_fields": ["family", "lang"],
    }
    (out / "fixture.json").write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", type=Path, default=Path(__file__).resolve().parent)
    write(ap.parse_args().out)


if __name__ == "__main__":
    main()
