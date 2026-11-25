# Relevance and recall regression suite

A continuous check of ranking quality and vector recall (issue #258). Latency has
`bench-compare`; this suite covers the other half: a change that makes search faster by
returning worse results shows up here.

The harness is the `relevance` bench in `kosha-query`
(`crates/kosha-query/benches/relevance/`). It needs no server. It writes the fixture
through the real `kosha_write::Indexer` into a temp directory and queries it with the real
`Searcher`:

| Metric | What it measures |
| --- | --- |
| `lexical.ndcg@10`, `lexical.recall@100` | `query_text` (operator OR, BM25) against the qrels |
| `vector.ndcg@10`, `vector.recall@100` | `knn` alone against the qrels |
| `hybrid.ndcg@10`, `hybrid.recall@100` | `query_text` + `knn`, fused by RRF (`rank.rrf` defaults) |
| `filtered_knn.probe@S.recall@10` | Filter-aware centroid probe (exact planning disabled) vs exact brute force over the documents passing a filter of selectivity S |
| `filtered_knn.planned@S.recall@10` | The same with the default planner, which answers selective filters exactly (#252) |
| `ood.filtered_knn.…` | The same selectivity sweep for out-of-distribution (OOD) queries, reported separately |
| `vector_ann.ndcg@10`, `vector_ann.mrr@10` | Probe-only vector search with k=10, against the qrels |

S is 100%, 50%, 10%, 1% and 0.1%. The harness adds an integer attribute
`kosha_relevance_sel` to every document: the document's rank in a fixed pseudo-random
permutation. `kosha_relevance_sel < ceil(n × S)` therefore passes exactly a fraction S of
the corpus, spread evenly over it, whatever the corpus.

Filtered recall is tie-tolerant: a unique hit counts if its exact cosine is
at least the true k-th cosine (within f64 rounding tolerance). If fewer than
k vectors pass, the denominator is the number that pass.

The JSON report adds two diagnostics:

- `vector_vs_exact`: nDCG@10 and MRR@10 for ANN and brute-force cosine on the
  same original embeddings, with the same query filters and qrels. The delta
  is ANN minus exact. MRR@10 credits the first positive judgment in the top ten.
- `kmr`: tie-tolerant recall@10 versus mean vector entries scanned per query,
  sweeping the number of postings probed per segment from 1 to all. This is an
  unfiltered sweep of the segment index, not the query planner's budget. The
  scan count uses actual membership counts of visited postings (including
  repeated entries, excluding centroid scores and rescoring). The curve is
  unavailable when a vector column has no clustered posting layout.

kNN queries use `num_candidates: 1` by default, so the probe budget is the engine's own
floor: `KOSHA_KNN_BUDGET_FRACTION` (5%) of the namespace's centroids. That is the budget a
large namespace actually gets, and the one that a change to the vector index, the
clustering or the planner moves. Set `KOSHA_RELEVANCE_NUM_CANDIDATES` to measure another
budget.

Every number is deterministic, so a base-to-head delta is a real change and not noise.
Running the harness twice gives byte-identical reports.

## Running it

```sh
# synthetic fixture, report to stdout (~20 s cold build + ~2 s run in release)
cargo bench -p kosha-query --bench relevance

# any fixture directory, plus the JSON report
KOSHA_RELEVANCE_JSON=/tmp/rel.json \
  cargo bench -p kosha-query --bench relevance -- path/to/fixture

# compare two reports; exit status 1 on a regression
python3 scripts/relevance/compare.py --base scripts/relevance/baseline.json --head /tmp/rel.json

# unit tests (compare.py, the converters, the fixture generator)
python3 scripts/relevance/compare.py --self-test
python3 scripts/relevance/test_converters.py      # or: python3 -m pytest scripts/relevance
cargo test -p kosha-query --test relevance_metrics
```

Relative paths in the argument and in `KOSHA_RELEVANCE_JSON` resolve against the
workspace root. `cargo bench` runs the binary from the crate directory.

## Fixture format

A fixture is a directory with these files:

**`corpus.jsonl`**: one document per line.

```json
{"id": "doc00001", "fields": {"title": "...", "body": "...", "lang": "en", "year": 2021}, "vector": [0.1, -0.3]}
```

- A string field is `Text` (BM25-indexed). It is `Keyword` (filterable, not scored) when
  `fixture.json` lists it in `keyword_fields`.
- An integer is `Integer`, any other number is `Float`, and a boolean is `Boolean`.
- `vector` is optional and is indexed as the field `vector`. Every vector must have the same
  dimension.

**`queries.jsonl`**: one query per line, as `{"qid", "text"?, "vector"?, "filter"?, "distribution"?}`.

```json
{"qid": "q001", "text": "breach of warranty", "vector": [0.2, 0.1], "filter": {"term": {"lang": "en"}}}
```

- Lexical mode runs the queries that have `text`. Vector mode runs those that have `vector`.
  Hybrid runs those that have both.
- `filter` is a Kosha filter clause (`term`, `terms`, `range`, `bool`, …). It applies to the
  query and to its `knn` clause.
- Filtered-kNN recall runs every query that has a vector. It uses only the selectivity filter
  and ignores the query's own filter.
- `distribution` defaults to `"id"`; set `"ood"` on out-of-distribution vector queries
  to report their filtered recall and KMR curve separately. No qrels are needed for those
  diagnostics.

**`qrels.txt`**: TREC qrels, one judgment per line: `qid 0 docid grade`.

- Grades above 0 are relevant. nDCG uses gain `2^grade − 1` and a `log2(rank + 1)`
  discount (trec_eval / Elasticsearch `dcg`).
- Recall@100 counts every grade above 0.
- Queries without a relevant judgment are skipped.

**`fixture.json`** (optional): `{"keyword_fields": ["lang", ...]}`.

## Bringing real judgments

The harness accepts any directory in the format above. It does not care where the qrels
came from:

- **Human judgments**: write `qrels.txt` directly.
- **OpenSearch as reference** (pseudo-qrels): run each query against OpenSearch and save
  `{"qid": ..., "response": <_search body>}` per line. Then
  `python3 scripts/relevance/opensearch_to_qrels.py responses.jsonl --top-k 10 > qrels.txt`
  writes the top-k hits as grade 1. With pseudo-qrels, nDCG measures agreement with
  OpenSearch, not true relevance. Use `--id-field` when the corpus ids are a `_source`
  field rather than `_id`.
- **Frozen eval sets** (`scripts/eval/build_evalset.py`, branch `feat/eval`):
  `python3 scripts/relevance/evalset_to_fixture.py cases.jsonl.gz OUT_DIR` turns the kNN
  cases into vector queries judged by their exact neighbours. It turns the word and phrase
  cases into text queries judged by their match sets.

Not done yet (still open in #258): judged queries, or OpenSearch pseudo-qrels, for the Sage
paragraph and case indices. Someone with data access needs to export the corpus, queries
and qrels in the format above. Then point the harness at that directory. Do not commit
client data. Keep such fixtures outside the repository and run them locally or from a
private job.

## The synthetic fixture

`fixtures/synthetic/` is generated by `fixtures/synthetic/generate.py`, which uses a fixed
seed. A unit test checks that the committed files match the generator byte for byte.

- **Size**: 3,000 documents in 30 topics, 24-dimensional vectors, 120 judged queries
  and 30 unjudged OOD queries, about
  1.5 MB.
- **Vocabulary**: each topic has its own vocabulary and shares part of it with the next
  topic. Each topic has three rarer "key terms" that mark a sub-theme. Documents from other
  topics occasionally contain a key term as noise.
- **Vectors**: a document's vector is its topic centre plus an offset for its key term, plus
  noise wide enough that neighbouring topics overlap.
- **Queries**: each query is a key term plus two topic words, with a matching vector.
  The OOD cohort is drawn independently from a spherical Gaussian, without using the
  corpus topic centers or key-term offsets.
- **Qrels**: grade 2 means the document is on the same topic and has the query's key term.
  Grade 1 means it is on the same topic. Every fifth query filters on `lang = en` and is
  judged only over documents that pass the filter.

The fixture is a regression signal, not a quality claim. Its absolute numbers mean little,
but their movement does.

## Fixtures

CI runs every fixture in `RELEVANCE_FIXTURES` (`relevance-compare.yml`) and gates each one
separately:

| Fixture | Shape | Why it is gated |
| --- | --- | --- |
| `synthetic` | 30 topics with overlapping vocabularies and vector clusters, key terms, OOD queries | General ranking and filtered-recall regressions |
| `neardup` | 1,900 documents (16-d vectors) in 150 Zipf-sized families: near-identical text and vectors, 25% byte-identical vectors, families packed into 12 regions | Probe selection on duplicate-heavy data, the legal-matter shape where kNN recall fell to 0.81 (2026-11-25). At the default 5% floor its recall@10 is about 0.79 and reaches 1.0 when every posting is probed, so a worse probe ranking shows up immediately |

Each fixture has a `generate.py` that reproduces its files byte for byte
(`test_converters.py` checks this). The committed baselines are `baseline.json`
(synthetic) and `baseline-<fixture>.json` for the others.

## CI gate and baseline

`bench-compare.yml` calls `relevance-compare.yml` on PRs that touch `kosha-query`,
`kosha-segment`, `kosha-vector-spfresh`, `kosha-core`, `kosha-write` or this directory. It:

1. Runs the harness on the base tree (with the head's harness and fixture overlaid) and on
   the PR head, on the same runner.
2. Posts a sticky PR comment rendered by `compare.py` on same-repo PRs, and uploads both
   reports and the comparison (including deltas) as the `relevance-report` artifact.
3. Fails when a metric drops by more than its threshold. The default is 1.0 point
   (absolute, ×100) for every metric. Filtered recall at 0.1% selectivity gets 2.0 points,
   because only a few documents pass there and one miss moves the number a lot. The
   workflow's `env` sets the thresholds.

If the base tree cannot run the harness, the head is compared with the committed
`baseline.json` instead. The comment also notes when `baseline.json` no longer matches the
base run.

**When a PR intends to change the numbers**, for example new clustering or a planner
change, a regression is expected. Explain it in the PR, regenerate the baseline, and get
the drop reviewed:

```sh
KOSHA_RELEVANCE_JSON=scripts/relevance/baseline.json cargo bench -p kosha-query --bench relevance
KOSHA_RELEVANCE_JSON=scripts/relevance/baseline-neardup.json \
  cargo bench -p kosha-query --bench relevance -- scripts/relevance/fixtures/neardup
```

Do this without any `KOSHA_*` engine overrides set; the report records the ones that are
set under `config.env`.

To see the gate trip, starve the probe:

```sh
KOSHA_KNN_BUDGET_FRACTION=0.001 KOSHA_RELEVANCE_JSON=/tmp/degraded.json \
  cargo bench -p kosha-query --bench relevance
python3 scripts/relevance/compare.py --base scripts/relevance/baseline.json --head /tmp/degraded.json
# exit 1: vector, hybrid and filtered-kNN metrics fall by 6–32 points
```
