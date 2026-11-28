#!/usr/bin/env python3
"""Small deterministic corpus for the local OpenSearch comparison.

Not the 10M MS MARCO set and not generate_corpus.py. This writer uses only
the standard library so the harness does not need NumPy. Same --seed, same
bytes.

Output (under --out-dir):
  corpus.jsonl   one {"id","text"} per line, the shape run_benchmark.py reads
  queries.json   [{"id","text"}, ...]
  manifest.json  docs, bytes, seed
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path


def zipf_weights(n: int, s: float) -> list[float]:
    raw = [1.0 / (i + 1) ** s for i in range(n)]
    total = sum(raw)
    return [w / total for w in raw]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--docs", type=int, default=2000)
    parser.add_argument("--avg-bytes", type=int, default=400)
    parser.add_argument("--queries", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--vocab", type=int, default=5000)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    vocab = [f"t{i}" for i in range(args.vocab)]
    weights = zipf_weights(args.vocab, 1.07)
    # random.choices is fine; build a small alias-free sampler by cumulative
    # weights so a given seed does not depend on CPython's choices algorithm
    # changing between versions.
    cumulative = []
    acc = 0.0
    for w in weights:
        acc += w
        cumulative.append(acc)

    def draw_term() -> str:
        x = rng.random()
        lo, hi = 0, len(cumulative) - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if cumulative[mid] < x:
                lo = mid + 1
            else:
                hi = mid
        return vocab[lo]

    args.out_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = args.out_dir / "corpus.jsonl"
    written = 0
    n = 0
    with corpus_path.open("w") as fh:
        for i in range(args.docs):
            words = [draw_term()]
            while len(" ".join(words)) < args.avg_bytes:
                words.append(draw_term())
            text = " ".join(words)
            line = json.dumps({"id": f"d{i}", "text": text}, separators=(",", ":"))
            fh.write(line + "\n")
            written += len(text)
            n += 1

    queries = []
    for q in range(args.queries):
        n_terms = 2 + (q % 5)
        text = " ".join(draw_term() for _ in range(n_terms))
        queries.append({"id": f"q{q:02d}", "text": text})
    (args.out_dir / "queries.json").write_text(json.dumps(queries, indent=2) + "\n")

    manifest = {
        "docs": n,
        "bytes": written,
        "avg_bytes_per_doc": written / n if n else 0,
        "vocab": args.vocab,
        "queries": args.queries,
        "seed": args.seed,
        "target_bytes_per_doc": args.avg_bytes,
        "generator": "scripts/bench/compare/prepare_corpus.py",
    }
    (args.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(
        f"wrote {n} docs ({written} text bytes, ~{written / n:.0f} B/doc), "
        f"{args.queries} queries, seed {args.seed} -> {args.out_dir}"
    )


if __name__ == "__main__":
    main()
