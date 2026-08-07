#!/usr/bin/env python3
"""Generate a synthetic BM25 corpus sized to match turbopuffer's published
tpuf-benchmark "Full-Text Perf" workload: 10M docs, ~9GB (i.e. ~900 bytes of
text per doc). In this codebase's terms, "doc" here means one chunk-sized
record — the same granularity as Kosha's own paragraph-level indices
(paragraph_index_hnsw, findings_index, white_river_paragraph), not a whole
legal document.

Corpus is synthetic (not real legal text) so it's deterministic and
reproducible run-to-run: same --seed always produces the same corpus, which
is what makes before/after comparisons (e.g. across Kosha versions) valid.

Term-frequency distribution is Zipfian over a fixed vocabulary, which is what
makes BM25 scoring costs realistic — a handful of very common terms most
queries will match broadly, and a long tail of rare/selective terms. Purely
random (uniform) word choice would understate BM25's IDF-driven cost profile.

Output: NDJSON shards under --out-dir (one doc per line: {"id", "text"}),
plus a queries.txt file (one query per line) sampled from the same
vocabulary/frequency distribution, for query_bench.py to replay.

Usage:
    python3 generate_corpus.py --out-dir /data/bm25-10m --docs 10_000_000 \\
        --avg-bytes 900 --seed 42
"""

import argparse
import json
import math
import time
from pathlib import Path

import numpy as np

VOCAB_SIZE = 50_000
ZIPF_S = 1.07  # exponent; ~1.0-1.1 matches natural-language term frequency
NUM_QUERIES = 2_000
QUERY_TERMS_MIN = 1
QUERY_TERMS_MAX = 4


def build_vocab(vocab_size: int, rng: np.random.Generator) -> list[str]:
    """Deterministic pseudo-word vocabulary: word length correlates loosely
    with rank so common ("rank 0") terms read like short function words and
    rare terms read like longer content words — cosmetic only, doesn't affect
    the benchmark's actual selectivity distribution (that's set by ZIPF_S)."""
    alphabet = "abcdefghijklmnopqrstuvwxyz"
    words = []
    seen = set()
    while len(words) < vocab_size:
        length = int(rng.integers(3, 10))
        w = "".join(alphabet[i] for i in rng.integers(0, len(alphabet), size=length))
        if w not in seen:
            seen.add(w)
            words.append(w)
    return words


def zipf_weights(vocab_size: int, s: float) -> np.ndarray:
    ranks = np.arange(1, vocab_size + 1, dtype=np.float64)
    weights = 1.0 / np.power(ranks, s)
    return weights / weights.sum()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--docs", type=int, default=10_000_000)
    ap.add_argument("--avg-bytes", type=int, default=900)
    ap.add_argument(
        "--docs-per-shard",
        type=int,
        default=100_000,
        help="NDJSON lines per output file, for parallel/resumable loading",
    )
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(args.seed)

    print(f"building vocabulary ({VOCAB_SIZE} terms)...")
    vocab = build_vocab(VOCAB_SIZE, rng)
    weights = zipf_weights(VOCAB_SIZE, ZIPF_S)

    # avg word length ~6.5 chars incl. trailing space -> derive target word
    # count per doc from --avg-bytes, then vary +/-40% per doc (real text
    # isn't fixed-length) via a lognormal jitter clipped to a sane floor.
    avg_word_len = sum(len(w) for w in vocab) / len(vocab) + 1  # +1 for space
    target_words = max(1, round(args.avg_bytes / avg_word_len))
    print(
        f"target ~{target_words} words/doc (avg_bytes={args.avg_bytes}, "
        f"avg_word_len={avg_word_len:.2f})"
    )

    n_shards = math.ceil(args.docs / args.docs_per_shard)
    t0 = time.time()
    docs_written = 0
    bytes_written = 0

    for shard_idx in range(n_shards):
        shard_docs = min(args.docs_per_shard, args.docs - docs_written)
        if shard_docs <= 0:
            break

        # Per-doc word counts, lognormal jitter around target_words. mean is
        # set to -sigma^2/2 so E[jitter] == 1 (a plain mean=0.0 lognormal has
        # E[X] = exp(sigma^2/2) > 1, which biases the realized avg-bytes/doc
        # above --avg-bytes).
        jitter_sigma = 0.35
        jitter = rng.lognormal(mean=-(jitter_sigma**2) / 2, sigma=jitter_sigma, size=shard_docs)
        word_counts = np.clip((target_words * jitter).astype(int), 5, None)
        total_words = int(word_counts.sum())

        # One big vectorized draw for the whole shard instead of one draw per
        # doc — the latter is the dominant cost if done naively at 10M docs.
        word_indices = rng.choice(VOCAB_SIZE, size=total_words, p=weights)

        shard_path = args.out_dir / f"shard-{shard_idx:05d}.ndjson"
        with shard_path.open("w") as f:
            cursor = 0
            for i in range(shard_docs):
                n = int(word_counts[i])
                doc_id = f"chunk-{docs_written + i:010d}"
                words = [vocab[idx] for idx in word_indices[cursor : cursor + n]]
                cursor += n
                text = " ".join(words)
                line = json.dumps({"id": doc_id, "text": text}, separators=(",", ":"))
                f.write(line + "\n")
                bytes_written += len(text)

        docs_written += shard_docs
        elapsed = time.time() - t0
        rate = docs_written / elapsed if elapsed > 0 else 0
        print(
            f"shard {shard_idx + 1}/{n_shards}: {docs_written:,}/{args.docs:,} docs, "
            f"{bytes_written / 1e9:.2f}GB text, {rate:,.0f} docs/sec"
        )

    # Query set: sampled from the same vocab/frequency distribution so query
    # selectivity is representative of the corpus (a query for a rank-1 term
    # hits nearly every doc; a query for a rank-40000 term hits almost none).
    print(f"generating {NUM_QUERIES} queries...")
    queries = []
    for _ in range(NUM_QUERIES):
        n_terms = int(rng.integers(QUERY_TERMS_MIN, QUERY_TERMS_MAX + 1))
        idxs = rng.choice(VOCAB_SIZE, size=n_terms, p=weights)
        queries.append(" ".join(vocab[i] for i in idxs))
    (args.out_dir / "queries.txt").write_text("\n".join(queries) + "\n")

    manifest = {
        "docs": docs_written,
        "bytes": bytes_written,
        "avg_bytes_per_doc": bytes_written / docs_written if docs_written else 0,
        "vocab_size": VOCAB_SIZE,
        "zipf_s": ZIPF_S,
        "seed": args.seed,
        "shards": n_shards,
        "num_queries": NUM_QUERIES,
    }
    (args.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"done: {docs_written:,} docs, {bytes_written / 1e9:.2f}GB, {n_shards} shards")
    print(f"manifest: {args.out_dir / 'manifest.json'}")


if __name__ == "__main__":
    main()
