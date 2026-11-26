# Hybrid-aware vector probing (research #467)

Status: experiment design. No engine changes or benchmark results. See
[#467](https://github.com/decover-tech/kosha/issues/467) and the
[#384 measurement baseline](https://github.com/decover-tech/kosha/issues/384).

## Baseline and reference

Kosha's RRF ranks BM25 and vector hits independently. For a fusion window
`W = rank_window_size`, the two lists are cut at `W` and `min(knn.k, W)`
before fusion. At the default RRF constant of 60, rank 60 in both lists
scores `2 / 120`, higher than rank 1 in just one list (`1 / 61`). Vector
recall@10 alone can therefore miss candidates that matter to the final page.

- Start with the BEIR SciFact test split, using its corpus and judged qrels.
  Freeze the dataset revision, query IDs, embedding model and revision, and
  document/query embeddings. Use the
  [relevance fixture format](../../scripts/relevance/README.md#fixture-format);
  the existing harness scores qrels but does not run this probe experiment.
  Repeat on a second BEIR dataset before generalizing; a small corpus with too
  few postings to separate budgets is a smoke test, not a cost result.
- Keep the snapshot, BM25 ranks, embeddings, scoring and tie-breaks fixed.
  For each query and window, retain the BM25 top `W`, brute-force all eligible
  vectors on those same embeddings, and fuse with the exact vector top
  `min(knn.k, W)` using Kosha's RRF formula and score-descending, ID-ascending
  tie-break. Treat this as the retrieval reference, not as relevance labels:
  even exact RRF can rank a judged document poorly.
- Start with plain BM25 + single-vector cosine queries, with no reranker,
  group limit or score modifiers. Use `max_results=10`, `W ∈ {10, 50, 100}`
  and `knn.k=W`. If measuring reranker input later, hold the reranker depth
  fixed between arms (`top_n ≤ W`) and also report judged candidate recall at
  `top_n`.

## Probe-budget sweep

1. Run current independent probing at the #384 fractions
   `{0.01, 0.02, 0.03, 0.05}`, with `num_candidates=1` so the explicit
   request does not mask the floor. Use a separate process per fraction
   (`KOSHA_KNN_BUDGET_FRACTION` is cached), leave the relative cutoff off,
   and include 5% as the unchanged baseline. Verify requests actually use ANN
   and record realized posting counts; if two settings differ only on paper,
   do not treat them as different cost points.
2. At each actual probe budget, run a seeded arm. Take the namespace-wide
   BM25 top hits, map each live document carrying the requested vector to its
   posting memberships, and deduplicate by (segment, posting) before probing.
   Count those postings against the same budget: take unique seeds in lexical
   rank order (stable posting order within a document), truncate to the cap,
   then fill from the existing vector ranking. Compare at the same realized
   cost; a seed posting already in the vector plan counts once. A BM25 hit
   without a vector stays eligible on the lexical side. Record seed lookup
   time and how many lexical hits received a vector score.
3. Record per query: vector recall@10 and at the effective vector window
   against exact neighbors, fused nDCG@10 against the BEIR qrels, and the
   paired nDCG@10 delta against exact-vector RRF. Archive the two ranked
   lists and fused top 10 to distinguish agreement with exact RRF from
   judged quality. Report warm p50/p99 latency, actual postings probed
   (including cached postings), and local/S3 bytes and S3 GETs, using
   [the bench protocol](../../scripts/bench/bm25_scale/README.md) for runs at
   scale. Use `timings.knn_postings` where available and keep the timings
   paired with the quality queries.

Plot judged nDCG@10, its delta to exact RRF, and window recall against mean
postings per query, with posting-count and latency tails alongside. Compare the
lowest-cost point that matches the fixed-5% baseline's judged nDCG@10 against
that baseline: the proposed bar is at least 2× fewer postings at equal quality.
Report paired uncertainty and the actual delta; an inconclusive result is not
evidence of parity. Separately mark the budget that reaches 0.95 recall@10 if
one exists; whether fused quality saturates sooner is a hypothesis.

## Fused stopping rule: separate follow-up

Run the first two arms with unchanged RRF. A BM25 score ceiling and a vector
similarity ceiling do **not** directly bound RRF ranks, which can change when
unseen hits enter either list. Instead, evaluate a score-based rule separately
against exhaustive **convex** fusion over the same eligible documents, with a
fixed weight `a ∈ [0, 1]` and monotone transforms `L` and `V` chosen before
the run. Do not normalize by a maximum observed only in the probed set.

For each unprobed posting `p`, a conservative upper bound is
`a × L(Bmax) + (1 − a) × V(Smax(p))`. `Bmax` must bound the actual BM25
query, including its IDF, boosts and term contributions. `Smax(p)` must bound
the final similarity of every eligible vector in `p`, accounting for any
quantization error; a centroid ordering heuristic alone is not such a bound.
Use a fixed missing-signal convention and ensure the ceilings bound it too.

Stop only when the heap holds the full required depth and the score of its
last hit is strictly higher than every remaining candidate's bound, including
lexical candidates with unresolved vector scores. Do not use partial scores as
final heap scores. If a bound is unknown or equality could change the ID
tie-break, continue probing. Validate the stopped ranking against exhaustive
convex fusion before comparing cost. Keep this separate from the RRF budget
sweep so that a change in fusion formula cannot explain the probe savings.
