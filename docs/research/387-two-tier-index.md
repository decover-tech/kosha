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
    `i < j` in the top-k. **Ids and order** are then exact, but a
    head-only answer could only return each document's `LB` as its score.
  - *score-exact*: rank-certified, and every top-k document has
    `UB = LB` (no tail term left unresolved). **Ids, order and scores**
    are then exact. This is the issue's "identical top-10": the bench
    (`query_bench.py`) and the server return ids and scores. A
    rank-certified query that is not score-exact still needs tail reads
    to finish its scores, and the tool counts those reads.
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
- Per fraction and bound it reports: set/rank-certified/score-exact share,
  head-only-exact share, mean overlap@k of the head-only answer (what an
  approximate or anytime mode would return), and violations. It reports the
  same shares bucketed by query length (1-3, 4-6, 7-8, 9-12, 13+ terms).
  For queries that are not score-exact it reports the segments and
  (segment, term) tail spans that would have to be read, plus their bytes. The GET count is an
  upper bound: every query-term tail of each segment that cannot be ruled
  out.
- Footprint, covering the bytes the query set touches (what the warm gate
  makes local today) and, with `--footprint`, every term of every segment:
  - head and full **scoring-section** bytes, re-encoded in the v5 layout
    (exact for v5 segments);
  - head **span** bytes. Positions are attributed by `1 + tf` weight, so
    this figure is an estimate;
  - tail frontier size in points and in bytes (delta varints), which is
    the metadata cost of the exact bounds;
  - file sizes by kind;
  - the **local text ratio**, the quantity the "3x smaller" bar is about:
    `(head span est + tail frontier bytes + tail TOC refs + fixed files) /
    (all spans + fixed files)`. Fixed files are everything that stays local
    whatever the posting layout: the TOC (`inverted.idx`),
    `doc_store.offsets` (ids and lengths, which `frontier_dl` needs),
    filters, footers. Doc-store bodies and vectors are left out of both
    sides. Tail TOC refs are an estimate of 16 bytes per split term (blob
    id, offset, length, uncompressed). The ratio is reported for the
    touched set and, with `--footprint`, for the whole index.
- Not modelled: tombstones and deletion vectors (all postings count as
  live; the VM script refuses a manifest that has any), filters, phrase/AND/fuzzy/wildcard queries, per-field (#249)
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

**Run validity.** The analysis only means something if its oracle is the
engine's answer on this namespace. `two_tier_check.py` gates the run:
- before the copy, the restored manifest must have no tombstones, deletion
  vectors or shards;
- afterwards, `segments_skipped_multi_reading` and
  `queries_skipped_time_budget` must be 0, every segment must be split
  (span bytes known) and violations must be 0;
- the oracle top-10 of the first `TWO_TIER_CROSSCHECK` (default 200)
  analyzed queries must equal what the restored live server returns for
  the same query (OR, k=10, no cache, ids and scores). Ids may differ only
  among equal scores.

Any failure writes `validity.json` and exits 1. That run's certification
numbers must not be used.

**Runtime guard.** Span lengths come from the TOC the segment reader
already holds (`SegmentReader::posting_span_len`), not from re-reading and
decompressing `inverted.idx` per (query, segment). The query pass stops
starting new 256-query batches after `TWO_TIER_MAX_SECONDS` (default
3600 s). A capped run reports how many queries it skipped and fails the
validity gate. Runtime on 10M MSMarco is **not measured**: the default
analyzer keeps stop words, so terms like `the` bring millions of postings
into every query that has them.

`benchmark.yml` gains `two_tier_eval` (boolean, default **false**) and
`two_tier_fractions` (default `0.1,0.2,0.3`). With `suite=none` the step
restores the BM25 namespace itself, from an **existing snapshot only**:
`restore_namespace.sh` exits 1 when there is none. The build-if-absent
path lives only in the BM25 suite step, so a corpus without a snapshot
needs `suite=bm25`. Nothing on the server changes: the
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
mode, not exact tiering. The MSMarco measurement is still pending.

## MSMarco benchmark status

No valid MSMarco evaluation was found in the GitHub Actions runs available
after [#411](https://github.com/decover-tech/kosha/pull/411) merged the
analysis harness. The two-tier step was skipped wherever it appeared, including
main runs [45078634960](https://github.com/decover-tech/kosha/actions/runs/45078634960)
and [45104084241](https://github.com/decover-tech/kosha/actions/runs/45104084241).
All 13 available `bench-results` artifacts had `two_tier_eval=false` in
`meta.json` and none contained a two-tier result. The other three runs were
cancelled without artifacts. The S3 archives were not checked because the
investigation environment had no AWS credentials.

Score-exact share, local text ratios and run validity are therefore
unmeasured on MSMarco. This is not a no-go result. The requested next
step without a valid run is the default-off exact prototype below,
without claiming the success bar has been met. The go/no-go run is still
pending: dispatch `benchmark.yml` with `suite=none`, `mode=restore`,
`bm25_docs=10m`, and `two_tier_eval=true` against the existing untiered
MSMarco snapshot.

## Experimental format (default off)

- Version 7 is allocated to tiering because #411 already assigned v6
  to bit-packed scoring blocks (#386). Write v7 only with
  `KOSHA_TWO_TIER_HEAD_FRACTION` set to a fraction in `(0, 1)`. Keep
  this flag off until every replica can read v7. With the flag off,
  both v5 and v6 read and write as before. Mixed namespaces are supported.
- Each TOC entry gains:
  - a **head span**: a v5-layout span over the head postings in doc order,
    with positions, in the existing `postings-XX.bin` shards. This is what
    hydration and warmup fetch;
  - a **tail span**: the same layout in one raw `tail-00.bin` object per
    segment, excluded from warmup; the object is absent if no term splits;
  - **tail bound** metadata: the frontier step function `(dl, tf)*`.
    The raw v7 TOC uses the old entry's reserved word to point into these
    appended records.
- `df` stays the full posting count (head + tail), so `doc_frequencies`
  (#246) and namespace statistics are unchanged. Head selection uses build
  time statistics only. Correctness never depends on them, because the tail
  bound holds under any statistics.
- Terms shorter than one scoring block and per-field keys stay whole.
  The opt-in writer buffers one full term at a time to partition its
  postings. Measure peak writer memory before using it with large segments.
- Compaction rebuilds postings from the original documents and splits
  the output only when the writer flag is enabled.

## Exact query prototype (default off)

`KOSHA_TWO_TIER_QUERY=exact` enables the head-only check for plain ranked
BM25 OR and single-term queries on v7 segments. With it unset, v7 always
reads full postings. Scores and document frequency use the same BM25
statistics and parameters as the full path.

1. Score the term heads and certify **each segment's** top window using
   Pareto bounds with the active BM25 statistics. Every winner's score
   must be final and every outsider must be strictly below the cutoff.
   The exact top windows can then be merged without a global certificate.
2. Preserve hit counts: an unrestricted single term knows its exact
   count from `df`; an OR query can skip tails only if its head hits also
   prove the reported count is capped. Other counts use full postings.
3. In segments that cannot certify, fetch the missing query-term tails
   in one range batch through the existing span cache. Merge the sorted
   head and tail postings and use the normal scorer. Phrase, multi-term
   AND, hybrid and other unsupported query shapes use this full path.
   A missing required tail fails the query rather than dropping matches.

Namespace-wide certification, incremental fetch-and-recertify, and an
anytime mode with an explicit approximate response are follow-up ideas,
not part of the prototype.

Interactions:
- The head plays the role of the essential list in MaxScore. The
  prototype scores v7 posts exhaustively after a tail fetch; it does not
  prune within a fetched tail.
- R6 (docid ordering): reordering makes heads denser in docid space, which
  shrinks head scoring bytes (the deltas get smaller).
- Segment count: tail GETs scale with segments times terms. Fewer, larger
  segments help both. Measuring that effect needs the same corpus with a
  different segment layout (for example a compacted copy of 10M). A
  different corpus size such as 1M also changes `N`, `df`, list lengths and
  the build path, so it does not isolate segment count.

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

The run must first pass the validity gate above (`validity.json` has
`valid: true`: no deletes in the manifest, no skipped segments or queries,
0 violations, and the oracle equals the live server on the cross-checked
queries).

Go to a format prototype only if, on 10M MSMarco with namespace statistics
and the bench query set, some head fraction meets all three:
1. score-exact >= 95% (`frontier_dl`, or `maxtf_mindl` if possible). If
   rank-certified passes but score-exact does not, a head-only answer
   would return lower-bound scores. Accepting ids plus order as the bar
   needs explicit agreement on #387; it is not the default;
2. local text ratio <= 1/3, both for the touched set
   (`touched_local_ratio`) and for the whole index
   (`footprint.fractions[].local_text_ratio`). Span-only ratios are
   reported, but they leave out the TOC, doc-store offsets, filters and
   tail metadata, so they overstate the reduction;
3. violations = 0.

If (1) fails but `overlap@k` is high, propose the anytime and approximate
mode as a separate experiment instead. If (1) fails for long queries only,
report the query-length cutoff.
