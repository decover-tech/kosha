#!/usr/bin/env python3
"""Deterministic synthetic relevance fixture for the Kosha relevance harness.

    python3 scripts/relevance/fixtures/synthetic/generate.py [--out DIR]

Writes corpus.jsonl, queries.jsonl, qrels.txt and fixture.json next to this
script (or into --out). Re-running it reproduces the committed files byte for
byte: everything is drawn from one seeded `random.Random`.

The corpus is a mixture of TOPICS topics. Each topic has

  * its own vocabulary (TOPIC_WORDS words), a few of which it shares with the
    next topic, so lexical matching alone confuses neighbouring topics;
  * KEY_TERMS key terms: rarer words that pick out a sub-theme. A document
    of the topic carries one of them half the time, and documents of other
    topics occasionally carry one too (noise);
  * a centre in vector space. A document's vector is its topic centre plus
    an offset for its key term (if any) plus Gaussian noise wide enough that
    neighbouring clusters overlap.

Every query belongs to one (topic, key term) pair. Its text is the key term
plus two topic words; its vector is drawn the way a document with that key
term would be. Graded judgments (TREC qrels) come straight from the labels:

    2  same topic and carries the query's key term
    1  same topic

A few queries carry a filter on the `lang` keyword; their judgments only
cover documents that pass it (a document the filter excludes cannot be
retrieved, so judging it relevant would only cap recall).
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

SEED = 258
DOCS = 3000
TOPICS = 30
DIM = 24
TOPIC_WORDS = 12
SHARED_WITH_NEXT = 4
KEY_TERMS = 3
BACKGROUND_WORDS = 250
QUERIES_PER_TOPIC = 4
BODY_WORDS = 24
TITLE_WORDS = 4
FILTERED_QUERY_EVERY = 5  # every 5th query filters on lang = "en"
OOD_QUERIES = 30

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
    return round(x, 4)


def generate() -> tuple[list[dict], list[dict], list[tuple[str, str, int]]]:
    rng = random.Random(SEED)
    taken: set[str] = set()
    background = unique_words(rng, BACKGROUND_WORDS, 2, taken)
    topic_words = [unique_words(rng, TOPIC_WORDS, 3, taken) for _ in range(TOPICS)]
    key_terms = [unique_words(rng, KEY_TERMS, 4, taken) for _ in range(TOPICS)]
    all_keys = [k for ks in key_terms for k in ks]
    centres = [[rng.uniform(-1.0, 1.0) for _ in range(DIM)] for _ in range(TOPICS)]
    key_offsets = [
        [[rng.gauss(0.0, 0.35) for _ in range(DIM)] for _ in range(KEY_TERMS)]
        for _ in range(TOPICS)
    ]
    # Zipf-ish background: earlier words are much more common.
    bg_weights = [1.0 / (i + 1) for i in range(BACKGROUND_WORDS)]

    def topic_vocab(t: int) -> list[str]:
        nxt = topic_words[(t + 1) % TOPICS][:SHARED_WITH_NEXT]
        return topic_words[t] + nxt

    def vector(t: int, key: int | None, noise: float) -> list[float]:
        v = list(centres[t])
        if key is not None:
            v = [a + b for a, b in zip(v, key_offsets[t][key])]
        return [r4(a + rng.gauss(0.0, noise)) for a in v]

    docs, labels = [], []
    for i in range(DOCS):
        t = rng.randrange(TOPICS)
        key = rng.randrange(KEY_TERMS) if rng.random() < 0.5 else None
        vocab = topic_vocab(t)
        title = [rng.choice(topic_words[t]) for _ in range(TITLE_WORDS)]
        body = []
        for _ in range(BODY_WORDS):
            r = rng.random()
            if r < 0.40:
                body.append(rng.choice(vocab))
            elif r < 0.50:
                body.append(rng.choice(topic_words[rng.randrange(TOPICS)]))
            else:
                body.append(rng.choices(background, bg_weights)[0])
        if key is not None:
            body.insert(rng.randrange(len(body) + 1), key_terms[t][key])
            if rng.random() < 0.3:
                title.append(key_terms[t][key])
        if rng.random() < 0.06:  # off-topic key-term noise
            body.insert(rng.randrange(len(body) + 1), rng.choice(all_keys))
        lang = "en" if rng.random() < 0.7 else "fr"
        doc_id = f"doc{i:05d}"
        docs.append(
            {
                "id": doc_id,
                "fields": {
                    "title": " ".join(title),
                    "body": " ".join(body),
                    "lang": lang,
                    "topic": f"t{t:02d}",
                },
                "vector": vector(t, key, 0.55),
            }
        )
        labels.append((doc_id, t, key, lang))

    queries, qrels = [], []
    for t in range(TOPICS):
        for j in range(QUERIES_PER_TOPIC):
            key = j % KEY_TERMS
            qid = f"q{t:02d}{j}"
            words = rng.sample(topic_words[t], 2)
            query = {
                "qid": qid,
                "text": " ".join([key_terms[t][key]] + words),
                "vector": vector(t, key, 0.3),
            }
            flt = None
            if (len(queries) + 1) % FILTERED_QUERY_EVERY == 0:
                flt = {"term": {"lang": "en"}}
                query["filter"] = flt
            queries.append(query)
            for doc_id, dt, dkey, lang in labels:
                if dt != t or (flt is not None and lang != "en"):
                    continue
                qrels.append((qid, doc_id, 2 if dkey == key else 1))
    # Independent of the corpus topic centers, key offsets and RNG stream.
    ood_rng = random.Random(SEED + 1)
    for i in range(OOD_QUERIES):
        queries.append(
            {
                "qid": f"ood{i:02d}",
                "distribution": "ood",
                "vector": [r4(ood_rng.gauss(0, 1)) for _ in range(DIM)],
            }
        )
    return docs, queries, qrels


def write(out: Path) -> None:
    docs, queries, qrels = generate()
    out.mkdir(parents=True, exist_ok=True)
    dump = lambda o: json.dumps(o, separators=(",", ":"), sort_keys=True)  # noqa: E731
    (out / "corpus.jsonl").write_text("".join(dump(d) + "\n" for d in docs))
    (out / "queries.jsonl").write_text("".join(dump(q) + "\n" for q in queries))
    (out / "qrels.txt").write_text("".join(f"{q} 0 {d} {g}\n" for q, d, g in qrels))
    meta = {
        "description": "Synthetic topic/key-term fixture; see generate.py",
        "seed": SEED,
        "keyword_fields": ["lang", "topic"],
    }
    (out / "fixture.json").write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", type=Path, default=Path(__file__).resolve().parent)
    write(ap.parse_args().out)


if __name__ == "__main__":
    main()
