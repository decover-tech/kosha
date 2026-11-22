# Two-tier impact index: high-impact head local, tail in S3 (research #387)

Status: **analysis stage.** This document sketches the format and query path,
and defines the offline measurement (`kosha-two-tier-eval`) that decides
whether the idea is worth building. No engine, format or query-path code
exists yet. Everything below the "Analysis tool" section is design, not
shipped behaviour.

## Problem

Local NVMe and warmup time grow with the whole index. Kosha's span cache
(#245) already keeps only the posting spans that queries touch. Still, every
posting of every touched term has to be local before a warm query avoids S3.
Most top-k answers are decided by each term's highest-impact postings. If a
small per-term **head** could be kept local and the **tail** left in S3, the
local text footprint would shrink by roughly the head fraction. The cost is
an S3 round trip for the queries whose top-k the head cannot prove.

Success bar (from #387): at least **3x smaller local text footprint**, with
**>= 95% of queries served from the head alone** and **identical top-10**.

## Prior work

- Ntoulas & Cho, *Pruning policies for two-tiered inverted index with
  correctness guarantee* (SIGIR'07). They define a first tier of pruned
  postings and a check that decides, per query, whether the first tier's
  top-k is provably the full index's top-k. The certification below is that
  check, adapted to BM25 with namespace-level statistics and segments.
- Mackenzie, Petri & Moffat, *Anytime ranking on document-ordered indexes*
  (TOIS 2022): impact-ordered ranges processed under a time budget. This is
  the optional "anytime" mode below.
- Kosha today: no tiering or static pruning. Block-max WAND (#248) already
  stores a `(max tf, min dl)` ceiling per 128-posting block in the v5 skip
  table (`write_postings_skip_split`), and namespace-level BM25 statistics
  (#246) are the engine default (`KOSHA_BM25_STATS`, `namespace` unless set
  to `segment`).

Paper gains are on the papers' corpora. Treat them as order of magnitude
until measured here.

## The correctness question, made precise

For one query (OR of terms `t`, top-k), one segment `s` and head fraction `f`:

- **Head** `H(s,t)`: the `ceil(f * df_s(t))` postings with the highest BM25
  contribution under the segment's own statistics (all a writer knows at
  build time), ties broken by `doc_seq`. A term with `df_s(t) <
  min_split_df` (default 128, one scoring block) is not split.
- **Tail bound.** BM25's per-term score `idf * tf (k1+1) / (tf + k1 (1 - b +
  b dl/avgdl))` increases with `tf` and does not increase with `dl`, for
  every `N`, `df`, `avgdl > 0`, `k1 >= 0` and `b` in `[0, 1]`. So the tail
  `(tf, dl)` **Pareto frontier** bounds the tail under *any* statistics the
  query later brings. That covers namespace stats that drift as segments are
  added (#246) and per-request `bm25_params`. Three bounds are compared:
  1. `frontier_dl`: for a document of known length `dl`, the term's tail
     contribution is at most `score(F(dl), dl)`, where `F(dl)` is the
     largest `tf` among frontier points with `dl_i <= dl`. If no such point
     exists, the term cannot be in that document's tail. This is the
     tightest bound.
  2. `frontier_max`: the exact tail maximum, `max over the frontier`.
  3. `maxtf_mindl`: the single `(max tf, min dl)` point. This is what one
     skip-table entry already stores, and it is the cheapest bound.
- **Certification.** Each document that appears in some head gets
  `LB = sum of its head contributions` and `UB = LB + sum of tail bounds`
  over the terms whose head it is not in. A document in no head of segment
  `s` is bounded by the sum of that segment's tail bounds (for
  `frontier_dl`, maximised over frontier breakpoints). The head-only answer
  is the global top-k by `LB`, and `theta` is its k-th `LB`.
  - *set-certified*: every other document's `UB < theta`, and every
    segment's no-head bound is `< theta`. The top-k **set** is then exact.
  - *rank-certified*: set-certified, and `LB(d_i) > UB(d_j)` for every
    `i < j` in the top-k. **Ids and order** are then exact (the issue's
    "identical top-10").
  - Comparisons are strict, with a relative slack of 1e-9, so exact score
    ties at the cutoff never certify. The engine breaks those ties by `_id`.
    The `with_ties` counters report the extra share a tie-aware check could
    claim.
- Certification is **namespace-global**. `theta` comes from all segments,
  so a segment whose bounds fall below `theta` needs no tail read even when
  the query as a whole is uncertified. This matters because the 10M
  namespace has had hundreds of segments (376 in an earlier round, see
  `scripts/bench/bm25_scale/RESULTS.md`).

## Analysis tool (this PR)

`crates/kosha-query/src/two_tier.rs` with the binary `kosha-two-tier-eval`.
It runs offline over segment directories and a query file:

```text
kosha-two-tier-eval --segments-root DIR [--segment-list ids.txt] --queries queries.txt \
    [--max-queries N] [--k 10] [--head-fractions 0.1,0.2,0.3] [--min-split-df 128] \
    [--stats namespace|segment] [--footprint] [--threads N] [--out report.json] \
    [--per-query rows.jsonl]
```

- It analyzes queries with each segment's own analysis chain
  (`QueryAnalysis::for_segment`) and scores with `Bm25Scorer`, the engine's
  own code. Namespace statistics are summed exactly as `CorpusStats` does. A
  repeated query term counts once per occurrence, as the engine's OR pass
  does.
- **Oracle self-check.** An exhaustive top-k is computed for every query.
  A certified query whose head answer differs from it counts as a
  `violation`, and the expected number is 0. The integration test
  (`crates/kosha-query/tests/two_tier_analysis.rs`) also checks the oracle
  against the real `Searcher` (OR, namespace statistics) on a 3-segment
  synthetic namespace with skewed segment sizes and lengths.
- Per fraction and bound it reports: set/rank-certified share,
  head-only-exact share, mean overlap@k of the head-only answer (what an
  approximate or anytime mode would return), and violations. It reports the
  same shares bucketed by query length (1-3, 4-6, 7-8, 9-12, 13+ terms).
  For uncertified queries it reports the segments and (segment, term) tail
  spans that would have to be read, plus their bytes. The GET count is an
  upper bound: every query-term tail of each segment that cannot be ruled
  out.
- Footprint, covering the bytes the query set touches (what the warm gate
  makes local today) and, with `--footprint`, every term of every segment:
  - head and full **scoring-section** bytes, re-encoded in the v5 layout
    (exact for v5 segments);
  - head **span** bytes. Positions are attributed by `1 + tf` weight, so
    this figure is an estimate;
  - tail frontier size in points, which is the metadata cost of the exact
    bounds;
  - file sizes by kind.
- Not modelled: tombstones and deletion vectors (all postings count as
  live), filters, phrase/AND/fuzzy/wildcard queries, per-field (#249)
  queries, and segments with more than one analysis chain (these are counted
  and skipped). The footprint pass covers the combined index only.
  Per-field keys would stay whole.

### On the bench VM

`scripts/bench/bm25_scale/vm/two_tier_eval.sh <namespace> <out-dir>` reads
the live segment ids from the restored manifest. It copies those segments
from the bench bucket, skipping `doc_store.bin` when `doc_store.offsets`
exists and skipping vector files. It builds the tool in the server image's
`builder` stage and writes `report.json`, `per_query.jsonl`, `summary.txt`
and `s3_listing.txt`, which holds every object's size, including files that
were not copied.

`benchmark.yml` gains `two_tier_eval` (boolean, default **false**) and
`two_tier_fractions` (default `0.1,0.2,0.3`). With `suite=none` the step
restores the BM25 namespace itself. Nothing on the server changes: the
analysis runs in a throwaway builder container.

### Local results (not the success-bar measurement)

These numbers come from Apple M5, 10 cores, 16 GB, 3 threads, on a
**synthetic** corpus: 400k docs with Zipf(1.07) words over a 50k vocabulary,
log-normal lengths, 8 segments of 50k, and 1,000 random 1-12-term queries.
They only check the tool and its cost. Synthetic text has nearly all
`tf = 1`, which makes head and tail scores close, so they say little about
MSMarco.

| head | rank-cert (frontier_dl / frontier_max / maxtf_mindl) | set-cert, frontier_dl | head-only exact | overlap@10 | violations |
|---|---|---|---|---|---|
| 10% | 2.3% / 2.3% / 0.3% | 7.0% | 11.8% | 0.50 | 0 |
| 20% | 2.6% / 2.4% / 0.3% | 7.4% | 13.2% | 0.63 | 0 |
| 30% | 2.8% / 2.4% / 0.3% | 8.1% | 15.0% | 0.72 | 0 |

- Set certification by length, `frontier_dl` at 10%: 1-3 terms 26%, 4-6
  terms 0.4%, 7 or more terms 0%.
- Uncertified queries needed tail reads in 7.1-7.3 of 8 segments, and a
  mean of 36 (segment, term) tail GETs (an upper bound).
- Footprint: at 10% the heads hold 36% of postings and an estimated 41% of
  combined-index span bytes. Most terms fall below `min_split_df` and stay
  whole.
- Runtime: 76 s for 1,000 queries plus 3 s for the footprint pass, at 274 MB
  peak RSS.

On this corpus, per-term heads certify almost nothing beyond short
queries. If MSMarco looks similar, the two-tier index fails the issue's
95% bar, and the cheaper outcome is an approximate or anytime head-only
mode, not exact tiering. The real numbers come from the bench run below.

## Format sketch (not built; gated)

- A new inverted version, `TIERED_SPLIT_INVERTED_VERSION = 6`, written only
  when a writer flag is set (`KOSHA_TWO_TIER_HEAD_FRACTION`, unset = off and
  v5 unchanged). This follows the precedent of `KOSHA_POSTINGS_WRITE_VERSION`
  for v5: a reader cannot read versions newer than its own build, so v6 must
  never be written by default, and the flag stays off until every replica
  reads v6. Before the prototype, a test must confirm that a pre-v6 reader
  *errors* on a v6 segment rather than misparsing it. Mixed namespaces are
  fine, because v5 segments simply have no tail.
- Each TOC entry gains:
  - a **head span**: a v5-layout span over the head postings in doc order,
    with positions, in the existing `postings-XX.bin` shards. This is what
    hydration and warmup fetch;
  - a **tail span**: the same layout over the tail, in separate
    `tail-XX.bin` shards that warmup never hydrates, so they stay S3-only
    unless span promotion pulls them;
  - **tail bound** metadata: the frontier step function `(dl, tf)*` (median
    size to be read from the footprint report; the synthetic run had 34-40
    points per term at most), or one `(max tf, min dl)` pair if the bench
    shows the cheaper bound is enough.
- `df` stays the full posting count (head + tail), so `doc_frequencies`
  (#246) and namespace statistics are unchanged. Head selection uses build
  time statistics only. Correctness never depends on them, because the tail
  bound holds under any statistics.
- Compaction re-splits from merged postings. Head plus tail is lossless, so
  a merge reads both, which costs tail GETs at compaction time only.

## Query-path sketch (not built; gated)

`KOSHA_TWO_TIER_QUERY=off|exact|anytime` (default off: v6 heads and tails
are read like v5 by concatenating, so results are identical).

1. **Head pass.** WAND over head spans only, carrying the per-document
   `LB`/`UB` above. The tail bounds play the role of the "non-essential"
   list ceilings in MaxScore.
2. **Certify** against the namespace-wide `theta`. The shared WAND floor
   (#248) already makes this a global quantity.
3. **Tail pass**, in `exact` mode and only for segments that fail. Issue one
   ranged GET per (segment, term) tail, in parallel and largest bound
   first, then re-certify. Stop as soon as certified. Tail fetches go
   through the existing span cache, so a frequently needed tail becomes
   local, and the footprint adapts to the workload.
4. **Anytime** mode (Mackenzie et al.): with a budget
   (`KOSHA_TWO_TIER_BUDGET_MS`), return the head-only answer when the budget
   runs out, and flag the response `approximate`. The tool's `overlap@k` and
   `head_answer_exact` columns are exactly this mode's quality.

Interactions:
- MaxScore and WAND: the head is the essential list. Block-max ceilings
  inside the tail still prune within a tail read.
- R6 (docid ordering): reordering makes heads denser in docid space, which
  shrinks head scoring bytes (the deltas get smaller).
- Segment count: tail GETs scale with segments times terms. Fewer, larger
  segments help both.

## S3 tail-fetch cost model

Per uncertified query, the tool reports `tail_segments`, `tail_gets` (an
upper bound) and `tail_bytes` (tail scoring sections).

- **Latency.** One parallel round of small ranged GETs. Kosha measured
  ~29/71/180 ms for 10 parallel doc-store ranged GETs, but against
  loopback MinIO (`RESULTS.md`, 2026-08-07), so this is not an S3 number.
  Real-S3 tail latency for this access pattern is **not measured**. A
  `p99 <= 170 ms` BM25 target leaves no room for even one S3 round trip on
  more than about 1% of queries. That makes the 95% certification bar a
  latency bar too: the uncertified share has to sit below the p99 quantile
  being protected (< 1% for p99).
- **Request cost.** S3 Standard GETs are priced per 1,000 requests (about
  $0.0004 per 1,000 in us-east-1 at the time of writing; check current
  pricing). At 8 QPS with an uncertified share `u` and `G` GETs each, the
  cost is `8 * 86,400 * u * G / 1000 * $0.0004` per day. For example,
  `u = 5%` and `G = 100` gives about $1.4/day. This is small next to NVMe,
  so latency, not dollars, is the binding cost.
- **Bytes.** Tail scoring sections, from the `tail_bytes` column. Positions
  are needed only for phrase queries.

## Decision rule for the bench run

Go to a format prototype only if, on 10M MSMarco with namespace statistics
and the bench query set, some head fraction meets all three:
1. rank-certified >= 95% (`frontier_dl`, or `maxtf_mindl` if possible);
2. touched-set head span bytes <= 1/3 of today's touched spans, and
   whole-index head span bytes <= 1/3 of the combined-index span bytes;
3. violations = 0.

If (1) fails but `overlap@k` is high, propose the anytime and approximate
mode as a separate experiment instead. If (1) fails for long queries only,
report the query-length cutoff.
