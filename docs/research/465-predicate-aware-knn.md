# Predicate-aware posting ranking and a cost-model exact threshold (#465)

Research for [#465](https://github.com/decover-tech/kosha/issues/465), track
#260. Two parts: a measurement of predicate-aware posting ranking, and an
experimental cost model for the exact-vs-probed choice behind a flag (off by
default; nothing changes unless `KOSHA_KNN_EXACT_COST_MODEL=1`).

## 1. Predicate-aware posting ranking

Today a filtered kNN query widens the global probe budget by `1 / selectivity`
(capped at `KOSHA_KNN_FILTER_BUDGET_MAX_SCALE`, 8) and probes the postings
nearest the query, whether or not any of their documents pass. The
`kosha-filter-knn-eval` tool compares:

- `baseline`: that policy.
- `pass-aware`: postings with no passing document are ranked last, the cutoff
  is taken over the rest, and the budget is widened by the selectivity within
  them.

It reports recall@10 against brute force over the passing documents, postings
fetched per query, the share of fetched postings with no passing document
(`empty%`), and the filter's query correlation (ACORN-style: mean similarity of
the filtered top-10 minus that of a random filter's top-10 at the same
selectivity). Filters: `random` (uncorrelated), `clustered` (the nearest `s·N`
documents to one fixed corpus vector, like a tenant or category that follows
the embedding), `near` / `far` (per query, the nearest / farthest `s·N`).

### Results

Synthetic corpus: 200,000 × 384-dim vectors, 1,000 Gaussian clusters, 10
segments of 20,000, 2,407 postings; 200 queries; base budgets 10–200 in steps
of 10. Postings fetched per query at the first budget reaching recall@10 ≥ 0.9:

| filter | selectivity | baseline | pass-aware | ratio |
|---|---|---|---|---|
| random | 10% | 400 | 400 | 1.00× |
| random | 1% | 1,440 | 800 | 1.80× |
| random | 0.1% | never (0.897 at 1,635) | 160 | — |
| clustered | 10% | 1,120 | 1,040 | 1.08× |
| clustered | 1% | 1,600 | 640 | **2.50×** |
| clustered | 0.1% | 1,392 | 39 | 35.7× |
| near | 10% | 240 | 240 | 1.00× |
| near | 1% | 240 | 240 | 1.00× |
| near | 0.1% | 262 | 60 | 4.40× |
| far | 10% | never | never | — |
| far | 1% | never | 959 | — |
| far | 0.1% | never | 167 | — |

Share of `baseline`'s fetched postings with no passing document, at a base
budget of 200: random 0.1% / 45% / 92% at 10% / 1% / 0.1%; clustered
3% / 61% / 98%; near 0.3% / 49% / 97%. `pass-aware` fetches no empty posting
until the passing ones run out.

Findings:

- At 10% selectivity nearly every posting holds a passing document, so
  pass-aware ranking changes nothing: the win is entirely below ~5%.
- The #465 bar (≥ 2× fewer postings at equal recall@10 at 1%) is met for
  the correlated `clustered` filter (2.5×) and nearly for `random` (1.8×); a
  positively correlated (`near`) filter needs no help, since the nearest
  postings already hold the passing documents.
- At 0.1%, the baseline's capped widening cannot reach 0.9 for a random filter
  at all, while pass-aware ranking reaches 1.0 at under 200 postings: the
  postings with any passing document are few, and it probes exactly those.
- Negatively correlated (`far`) filters are where the even-spread assumption
  fails worst: only pass-aware ranking reaches the bar at 1% and 0.1%.

### What it costs in the engine

Pass counts need the filter's `DocSet` and each posting's members before
planning. The tool reads members from `vector.idx` (`vector_posting_layout`),
which is fine offline but is a full column read per query. In the engine this
needs a resident posting → doc-seq map; with posting-ordered doc seqs (#456)
each posting is a contiguous doc-seq range and its pass count is a popcount
over one range of the filter bitmap, `O(postings)` per segment per query.
Until #456 lands this stays offline. A cheaper approximation (a per-posting
"any pass" bit from a resident signature) is enough for the `empty%` savings;
exact counts additionally size the budget.

## 2. Cost-model exact threshold

`KOSHA_KNN_EXACT_THRESHOLD` (50,000) and `KOSHA_KNN_EXACT_FRACTION` (1%) are
fixed: they do not know a column's entry width (f32 vs i8 / RaBitQ), posting
size, or whether `doc_vectors.bin` exists. With `KOSHA_KNN_EXACT_COST_MODEL=1`
the planner instead picks per segment by estimated bytes read
(`knn_exact_is_cheaper`, from the resident `vector.offsets` via
`SegmentReader::knn_column_cost`):

- probe: `n × (avg_posting_entries × entry_bytes + overhead)` where `n` is the
  segment's share of the global budget widened as `filtered_knn_budget` widens
  it (without global budgeting, `num_candidates` unwidened, as the per-segment
  probe reads), at least `k / (selectivity × avg_posting_entries)` (to see `k` passing
  entries), at most every posting; plus `k × KOSHA_VECTOR_RESCORE_FACTOR`
  exact vectors for a quantized column.
- exact: `passing × 4·dim` from `doc_vectors.bin`, or every posting without it.
- `overhead` is `KOSHA_KNN_COST_POSTING_OVERHEAD_BYTES` (4096: one page per
  posting fetch).

Effects: an i8 column (4× smaller entries) moves the crossover to a lower
selectivity than an f32 one; a segment without `doc_vectors.bin` goes exact
only when a probe would read the whole column anyway; small segments, where
the default budget already reads most postings, go exact at any selectivity.
The namespace-wide 50,000 rule is not applied under the cost model; a threshold
of `0` still turns exact off.

Recall is unchanged by construction (exact is exact; probed segments take the
existing path). `filtered_knn_cost_model_recall_at_every_selectivity` keeps
#252's bar (recall@10 ≥ 0.95, `min(k, |filtered|)` results) at 50%–0.025%.
Next: calibrate `overhead` against cold-read latency on the bench corpus
(the byte model ignores that exact reads are random 4·dim reads while probes
are sequential posting reads) before considering it as a default.

## Reproduce

```sh
B=target/release
cargo build --release -p kosha-build-segments
$B/kosha-vector-quant-eval gen --out /tmp/c --docs 200000 --dim 384 --clusters 1000
$B/kosha-build-segments --shards-dir /tmp/c --embeddings-dir /tmp/c --emb-dim 384 \
  --namespace ns --out-dir /tmp/s --docs-per-segment 20000
$B/kosha-filter-knn-eval --embeddings-dir /tmp/c --dim 384 --segments /tmp/s/ns \
  --queries 200 --budgets "$(seq 10 10 200 | paste -sd, -)"
```
