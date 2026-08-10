# BM25 scale benchmark — results (2026-08-06)

Methodology: see [README.md](./README.md). Replicates turbopuffer's
tpuf-benchmark "Full-Text Perf" workload (BM25, 10M docs, ~9GB, 8 QPS,
topk=10, p50/p90/p99, warm vs cold) for a comparable number. "Docs" here
means chunk-sized records (~900 bytes each), matching Kosha's own
paragraph-level index granularity — not whole documents.

## Corpus

Generated via `generate_corpus.py --docs 10000000 --avg-bytes 900 --seed 42`:

| | |
|---|---|
| Docs | 10,000,000 |
| Text size | 8,559,725,432 bytes (8.56 GB) |
| Avg bytes/doc | 855.97 |
| Vocabulary | 50,000 terms, Zipfian (s=1.07) |
| Queries | 2,000, sampled from the same term distribution |
| S3 handle | `s3://decoverai-nonprod-kosha-bench/bm25-scale/10M-docs-900b-seed42/` |

Regenerating with the same `--seed 42` reproduces this corpus exactly.

## OpenSearch — result

Domain: `decoverai-nonprod-search` (staging, 2× `r8g.large.search`), index
`bm25-bench-10m`, 5 shards / 1 replica, BM25 k1=1.2/b=0.75, forcemerged to 5
segments/shard after load. Both runs: 961/961 successes, 0 errors, achieved
~8.0 QPS.

| | p50 | p90 | p99 |
|---|---|---|---|
| tpuf-benchmark reference — **warm** | 13ms | 18ms | 29ms |
| **OpenSearch — warm** | **12.3ms** | **24.8ms** | **62.1ms** |
| tpuf-benchmark reference — **cold** | 316ms | 381ms | 559ms |
| **OpenSearch — cold** | **18.5ms** | **38.2ms** | **98.1ms** |

**Cold-cache caveat:** `query_bench_opensearch.py --phase cold` only calls
`POST /_cache/clear` (request/query/fielddata caches) — there's no
node-shell access on a managed AWS OpenSearch domain to evict the Lucene
segment files from the OS page cache the way a full process restart does.
So this "cold" number is a **weaker guarantee** than the tpuf-benchmark
chart's own cold number: the page cache was very likely still warm from the
load's own forcemerge and the run itself touching most of the corpus. The
small cold/warm gap here (18.5ms vs 12.3ms p50) versus the reference
chart's large gap (316ms vs 13ms) is best explained by that, not by this
domain being dramatically better at genuinely-cold reads.

**Fair takeaway:** the **warm** numbers are directly usable — a real,
comparable measurement for this domain/instance size against a 10M-doc/~9GB
BM25 workload at 8 QPS/topk=10, in the same ballpark as tpuf-benchmark's own
warm numbers (higher at p90/p99, plausibly instance-size and corpus
differences). The cold numbers are not apples-to-apples with the reference
chart.

Raw JSON: `cold_opensearch.json` / `warm_opensearch.json` (re-run with
`--out` to regenerate; not committed here since they're just the JSON shown
above).

## Kosha — not completed

Attempted against the shared staging `kosha`/`kosha-query` deployments and
aborted partway through; **no clean Kosha number exists yet**. What
happened, and why it matters more than just "the run failed":

1. Loaded the identical 10M-doc corpus into a dedicated namespace
   (`bm25-bench-10m`) via `load_corpus.py` — this part worked (10M docs
   sent, compacted 500→376 segments over several `tiered` rounds).
2. A `mode: "full"` compaction attempt (trying to consolidate the 376
   segments further) **OOMKilled the ingest pod** (exceeded its 56Gi
   memory limit) — full-mode compaction has no memory-bounded/streaming
   merge, unlike tiered mode's self-limiting `max_segments_per_merge=32`.
   This is also what caused ~24,310 docs (0.24%) to go missing from the
   namespace.
3. Running the actual 8 QPS cold query benchmark against this cold,
   376-segment namespace triggered a cascading incident on the **query**
   tier: HPA scaled kosha-query 2→6 replicas reacting to the load, but each
   new replica independently re-hydrated the same ~22GB segment set from S3
   (no cache coordination across replicas — see architectural notes below),
   blew past the 20Gi per-pod ephemeral-storage limit, and got
   Kubernetes-**Evicted** repeatedly. This starved capacity for the *real*
   pre-existing namespace `paragraph_index_hnsw` too (queries against it
   timed out during the incident, despite having nothing to do with the
   benchmark's data).
4. Remediated by deleting `bm25-bench-10m`'s S3 segments + postgres
   control-plane rows and restarting both deployments. Fleet returned to
   its normal namespace list and HPA settled back to 2 replicas. Did not
   re-attempt the Kosha benchmark — see architectural recommendations below
   before trying again.

No durable data loss beyond the 24,310-doc full-compaction incident above;
the eviction/crash-loop was a service-availability issue, not corruption.

### Architectural recommendations before re-attempting

In priority order (full rationale in chat/PR discussion):

1. **Cap `KOSHA_CACHE_MAX_BYTES` explicitly** (currently unbounded by
   default) so app-level LRU eviction happens before the kubelet forcibly
   evicts the pod for exceeding ephemeral-storage.
2. **Fix the HPA scaling signal.** CPU was ~idle throughout the incident —
   the bottleneck was S3 I/O wait. Scaling out on CPU during a cold-hydration
   event adds redundant fetches, not real capacity.
3. **Gate readiness on actual warmth**, not just process-up `/healthz`, so
   HPA-added replicas don't take traffic while cold and thrashing.
4. **Coordinate hydration across replicas** (consistent-hash partitioning
   of segment ownership, or single-flight dedup of concurrent fetches for
   the same segment) — right now N replicas = N× redundant cold-start cost,
   not N× cache capacity.
5. **Make `full` compaction memory-bounded** (tiered-until-convergence or a
   streaming/spilling merge) instead of one unbounded in-memory merge.
6. **Fewer, larger steady-state segments and fewer S3 objects per segment**
   — 376 segments/1,642 files for 10M docs means a single broad query needs
   ~1,642 S3 GetObjects cold.
7. **Backpressure with a fast, honest response** ("still warming up") for
   queries that would require hydrating an outsized fraction of segments,
   instead of hanging until client timeout.
8. **Pre-warm after bulk load/compaction** so first real traffic doesn't
   pay the full synchronous cold-hydration cost.
9. **Metrics on hydration volume / cache headroom / eviction rate** — this
   failure mode was invisible until pods started dying.

---

# Addendum: post-fix re-run attempt (2026-08-07, PR #59 deployed)

Re-ran the pipeline against staging with #59's fixes live (vector.idx skip,
streamed downloads, KOSHA_HYDRATE_BYTE_BUDGET=1GiB default). Outcome: **#59's
OOM fixes are validated — query pods never OOMed all night at 16Gi under
loads that previously killed them within seconds** (byte-budgeted chunked
hydration visible in logs). But no clean warm/cold percentile table exists
yet, for two reasons: five newly-found platform bugs (below), and — decisive
for the final attempt — a concurrent indexing+search workload against
`paragraph_index_hnsw` saturated both the ingest pod (6.7/7 cores, segment
count 1,100+) and the single benchmark query pod's disk cache (LRU war
between the 26.9GB bench namespace and 15.7GB of paragraph_index_hnsw),
making any measurement a measurement of contention. The benchmark corpus,
namespace (`bm25-bench-10m-v2`), and harness all remain in place for a
re-run on quiet infra.

Measured before contention took over:
- **Cold hydration throughput ~9.9GB/min (~165MB/s)** from S3, memory-stable
  (~1.4-2.4GB RSS during hydration vs OOM-at-16Gi before #59).
- **Cold-first-query cost is architectural**: a broad query hydrates the
  whole ~27GB namespace before answering (~3-5 min best case) — vs
  turbopuffer's per-query object-storage reads (their cold p50 = 316ms).
  Kosha has no comparable "cold percentile" today; its cold story is
  "namespace warm-up," and should be benchmarked as such (time-to-first-OK +
  post-warm-up percentiles).

## New bugs found during the re-run (in severity order)

1. **S3 segment durability gap (data-loss risk).** Only the latest flushed
   segment is synced to S3 per publish (`sync_latest_segment_to_s3`); under
   the loader's 8-way concurrent batches, 174 of 376 segments were never
   uploaded — they existed only on the ingest node's NVMe. An ingest node
   loss = permanent loss of those segments. Worked around via a one-off
   hostPath→S3 sync from a helper pod. Fix: sync every flushed segment
   (queue per flush), plus a reconciliation sweep comparing manifest vs S3.
2. **Tiered compaction silently loses documents.** 10,000,000 →
   9,975,781 (−24,219, 0.24%) across 4 clean tiered rounds, no crash, no
   error — reproducible (previous run lost a near-identical 24,310, then
   attributed to an OOM; this run had no OOM). ~6k docs vanish per
   32-segment merge round. Needs a unit test asserting merge output
   doc-count equals input.
3. **30s HTTP io-timeout makes cold namespaces unqueryable.** Any query
   outliving `KOSHA_HTTP_IO_TIMEOUT_SECS` (default 30) gets its socket
   dropped with no error response (client sees RemoteDisconnected), while
   the server keeps hydrating. Cold queries against 10M-doc namespaces need
   minutes. Fix: 503 + Retry-After (or progress heartbeat) instead of a
   silent drop; reconsider the default.
4. **Hydration S3 GETs have no retry.** Throttled GETs during fan-out
   bursts WARN and leave the segment incomplete until a future query
   re-triggers hydration; convergence to a fully-hydrated namespace is
   flaky. Fix: bounded retries with backoff in `fetch_one`.
5. **Disk-cache LRU evicts files that in-flight hydration still needs.**
   When the working set ≈ `KOSHA_CACHE_MAX_BYTES`, eviction deletes
   just-hydrated files of the same namespace while its tail hydrates —
   observed incomplete-segment counts climbing 37→128→193 instead of
   converging. Same class of fix as #58's pinned-aware in-memory eviction,
   applied to the disk cache: never evict files pinned by an in-flight
   hydration/request, and fail admission instead when the budget can't hold
   the request's working set.

Also observed (env, not engine): staging spot-instance reclaims + Karpenter
consolidation repeatedly killed pods mid-hydration (each replacement
restarts hydration from zero — no cross-pod cache reuse; the
`karpenter.sh/do-not-disrupt` annotation stops consolidation but not spot
reclaims), and a single shared staging deployment cannot isolate a
benchmark from tenant traffic (CPU, disk cache, and S3 bandwidth are
pod-global).

## Standing comparison (unchanged)

OpenSearch (2× r8g.large.search, same corpus/queries/protocol): warm
**12.3/24.8/62.1ms**, "cold" (cache-clear only) **18.5/38.2/98.1ms** —
tpuf-benchmark reference: warm 13/18/29ms, cold 316/381/559ms. Kosha warm
percentiles: pending a quiet-infra re-run.

---

# Addendum: cold-read baseline (2026-08-08, per-phase instrumentation deployed)

Per-request phase timing (`search timing:` log line — hydrate ms/files/MB,
queue, admit, score, materialize, cold-vs-cached opens) shipped via PR #63
and captured with `scripts/capture_cold_baseline.sh` (staging) and
`scripts/bench/cold_read_local.py` (laptop loop against the repo compose
stack: MinIO + Postgres). Query text `"the"` (broad, minimal bloom pruning),
`max_results=10`, fully-cold query tier per run (rollout restart wipes
emptyDir + in-memory caches).

## Staging results

| namespace | docs / segs | cold | warm |
|---|---|---|---|
| `paragraph_index_hnsw_v2` | 99k / 99 | **14.4 s** (converged, 1 attempt) | 33 ms |
| `paragraph_index_hnsw` | 1.05M / 1103 | **does not converge** | n/a |
| `white_river_paragraph` | 1.17M / 59 | **does not converge + evicts pods** | n/a |
| `bm25-bench-10m-v2` | 9.98M / 376 | not attempted (~27 GB working set — strictly worse by the same math) | n/a |

**Converging case breakdown** (`paragraph_index_hnsw_v2`, server-side):

```
cold: total=13800ms  hydrate=12436ms (90%, 495 files, 2755MB ≈ 222MB/s)
      score=1360ms (99 cold opens, Σopen=5361ms ≈ 54ms/segment)
      materialize=2.9ms
warm: total=33ms     hydrate=0 files  score=11ms  (all 99 opens cached)
```

**Non-convergence detail.** `paragraph_index_hnsw`: the lexical scoring
working set (doc stores up to 265 MB/segment, all present in S3) exceeds
`KOSHA_CACHE_MAX_BYTES` (12 GiB) — the disk LRU reaches a stable
equilibrium where the same 18 segments are evicted every hydration round;
every attempt 503s forever. `white_river_paragraph` is worse: its working
set exceeds the pod's **20 Gi ephemeral-storage limit outright**, so the
kubelet evicted the pod mid-hydration twice
(`Evicted: ephemeral local storage usage exceeds the total limit of
containers 20Gi`) — an eviction→rehydrate→eviction loop that also wipes
warm caches for unrelated tenant traffic. The app-level LRU cannot help in
either case: every file on disk is needed by the one in-flight search, so
nothing is legally evictable.

**Headline: cold read on every production-scale staging namespace is
currently impossible at any latency.** Scoring-set-only hydration (skip
`doc_store.bin` — ~70–85% of non-vector segment bytes; fetch it only for
the top-k page's segments at materialize time) is therefore a correctness
fix for cold reads, not just a latency optimization.

Also surfaced by the instrumentation:
- **Footers are fetched twice** on a cold namespace: once by the
  bloom-prune prefetch, then again inside the full-segment fetch (495
  files = 99 footers + 99×4 — the segment fetch doesn't exclude the
  already-local footer). ~300 redundant GETs on a 1103-segment namespace.
- **Cold opens cost ~54 ms/segment** even with the v2 lazy inverted index
  (Σopen 5.4 s across 99 segments) — remaining eager work is the filters
  parse and the per-doc `doc_store.offsets` metas (heap `String` per doc);
  the arena treatment is the known follow-up.
- Warm client wall through `kubectl port-forward` measured ~300 ms vs
  33 ms server-side — port-forward overhead; don't quote client walls from
  this harness as service latency.

## Local loop (laptop, MinIO — for fast iteration)

8 segs × 2000 docs (Zipf-ish ~900 B docs): cold ≈ **700 ms**, hydrate =
99% of it (57.8 MB / 48 files); warm ≈ 3 ms. The bytes/files and
convergence signals reproduce the staging structure exactly and iterate in
~30 s, so scoring-set-only hydration (and later compression) get validated
locally first: expected post-fix numbers are ~15–30% of today's hydrate MB
and 4 fewer files per segment, plus `--budget` below working-set size
flipping from non-convergent to convergent.

---

# Addendum: cold-read optimization results (2026-08-08, full stack deployed)

Scoring-set-only hydration + page-scoped doc-store fetch (#67), the
resumable offsets-backfill job + `KOSHA_SCORING_HYDRATE_CONCURRENCY=64`
(#68), and the offsets migration run for every legacy namespace.

| namespace | baseline | scoring-set @ fan-out 16 | + offsets + fan-out 64 | warm |
|---|---|---|---|---|
| `white_river_paragraph` (1.17M docs / 59 segs) | impossible — kubelet evicted pods mid-hydration | impossible (legacy fallback: no offsets sidecars in S3) | **60.4 s — first-ever converging cold read** | 0.19 s |
| `paragraph_index_hnsw` (1.05M docs / 1103 segs) | impossible — 12 GiB budget LRU war | 27.0 s | **18.9 s** | 0.28 s |
| `paragraph_index_hnsw_v2` (99k docs / 99 segs) | 14.4 s | 11.8 s (legacy fallback) | **4.85 s** — hydrate bytes 2753 → 277 MB (10×) | 0.03 s |

Key phase data for the next optimization round:

- **Cold opens dominate white_river**: Σopen 31.2 s (529 ms/segment — its
  `filters.bin` is 50 MB/segment, eagerly parsed at open). Next lever:
  arena doc-id metas + lazy/dictionary-encoded filters.
- **Page materialization fetches whole doc stores**: white_river
  materialize = 15.7 s (up to 10 × 295 MB files for a 10-hit page). The
  offsets sidecar already knows each hit's byte span → ranged GETs.
- `paragraph_index_hnsw` hydrate 19.2 → 11.2 s from fan-out 64 alone;
  1103 of its 2024 cold GETs are footers → namespace-level meta object.
- Legacy-namespace caveat: the scoring-set path only applies where
  `doc_store.offsets` exists in S3. The backfill route
  (`POST /v1/admin/backfill-offset-tables`, async/resumable since #68) is
  the migration tool; run it for any namespace still showing
  baseline-like cold bytes.

---

# Addendum: ranged-GET page materialization micro-benchmark (2026-08-07)

Backlog item #2 after scoring-set-only hydration shipped: the materialize
phase still fetched the **whole** `doc_store.bin` for every segment holding
a page hit (white_river_paragraph: up to 10 × 295 MB for a 10-hit page →
materialize = 15.7 s, and those bytes also had to fit under the 20 Gi
ephemeral-storage limit). The offsets sidecar already knows each hit's
exact byte span, so materialize now asks for just those spans as S3 ranged
GETs (`Range: bytes=…`), and `doc_store.bin` never lands on disk at all.

Measured with the local loop, fattened so doc stores dominate
(`--segs 8 --docs 5000 --words 700` → 40k docs ≈ 4.9 KB each, ~24 MB
doc store/segment, same corpus reused for both builds; query `"the"`,
`max_results=10`, 3 cold runs each):

| build | cold materialize (3 runs) | bytes fetched at materialize | cold total (median) |
|---|---|---|---|
| main @ `2084816` (whole-file) | 561 / 1503 / 2448 ms | ~190 MB (8 doc stores) | 20,987 ms |
| ranged-GET branch | 29 / 71 / 180 ms | **41.3 KB** (10 ranged GETs) | 16,296 ms |

**Cold materialize: ~21× faster at the median, ~4,600× fewer bytes** — and
this is against loopback MinIO, which flatters the whole-file baseline;
against real S3 with 295 MB objects the gap is what separates 15.7 s from
one parallel round of KB-sized GETs (tens of ms). Just as important for
white_river_paragraph: the materialize working set no longer includes doc
stores, so page materialization can't contribute to the
ephemeral-storage-eviction loop no matter how large the doc stores grow.

**Trade-off (measured):** warm-path materialize was ~1.5–3 ms when the
cold query left whole doc stores on local disk; it is now 15–115 ms,
because every page re-fetches its spans remotely (nothing is persisted —
deliberately, since a partial `doc_store.bin` on disk would read as
complete to every existence check). Server-side warm total: 5–10 ms →
18–121 ms locally. If that ever matters for hot namespaces, the follow-up
is a small in-memory LRU of materialized doc records keyed by
`(segment, doc_seq)` — not persisting partial files.
---

# Addendum: cold-read round 3 (2026-08-08 — filters skip, ranged-GET materialize, postings blobs)

Deployed: #70+#72 (lazy filters + skip `filters.bin` hydration for broad
queries + cache-poisoning fix), #74 (ranged-GET page materialization via
the offsets sidecar), #75 (inverted postings split into blobs). All rounds
single-attempt, fully-cold query tier.

| namespace | round 2 | round 3 | phase deltas |
|---|---|---|---|
| `white_river_paragraph` | 60.4 s | **6.8 s (9×)** | hydrate 35.5→2.8 s (6075→1241 MB — filters out of the fetch), materialize 15.7→**0.23 s** (ranged-GET, 69×), score 8.5→3.1 s |
| `paragraph_index_hnsw` | 18.9 s | **13.7 s** | hydrate 11.2→8.7 s (2816→1170 MB), score 7.0→4.4 s, Σopen 11.9→7.1 s |
| `paragraph_index_hnsw_v2` | 4.85 s | **3.5 s** | bytes 277→118 MB, materialize 1.6→0.16 s |

Cumulative from the original baseline: white_river **impossible → 6.8 s**
(cold) / 0.2 s (warm); paragraph_index_hnsw **impossible → 13.7 s** /
0.6 s; v2 **14.4 s → 3.5 s** / 0.12 s.

What the remaining phase data points at, in order:

1. **Per-segment footer GETs are now the dominant file count** — 1103 of
   `paragraph_index_hnsw`'s 1717 cold GETs (64%) are footers, and even its
   *warm* hydrate check costs ~400 ms of per-segment stats. A
   namespace-level `segments.meta` object (blooms + doc counts + format
   versions, one GET) is the next hydrate lever.
2. **Compaction** — 1103 segments for 1M docs multiplies every remaining
   per-segment cost (blocked on the tiered doc-loss bug).
3. **Postings compression** — remaining scoring-set bytes (1170 MB on
   `paragraph_index_hnsw`) are mostly postings.
4. **Residual open cost** — white_river still Σ12 s across 59 opens
   (~204 ms/seg) with filters lazy; the remaining eager work (inverted
   blob read + offsets arena parse) is the tail.

---

# Addendum: 10M MSMarco apples-to-apples vs turbopuffer (2026-08-09)

True like-for-like against tpuf-benchmark's published "Full-Text Perf"
chart: **their exact corpus and queries** (CohereLabs msmarco-v2.1 — the
`segment` text column, real MSMarco queries cycled in dataset order, via
`fetch_msmarco.py`), 10M docs / 9.91GB text, topk=10, on **their hardware
class** (m7i.8xlarge, 32 vCPU / 128GB ≈ their c2-standard-30; newer CPU
generation, noted), single-node Kosha (main @ #89) + same-region S3
segment bucket, mirroring their VM↔GCS architecture. 167 segments, 23GB
segment working set, exactly 10,000,000 docs loaded (4,187 docs/sec
ingest, CPU-idle — flush/upload-serialized).

## Headline 1: tpuf's 8 QPS workload does not fit — CPU-saturated at ~6.4 QPS

At their published load spec (8 QPS open-loop) the box pins all 32 cores
(95%+ user CPU) completing ~6.4 QPS; the open-loop queue grows without
bound (queue_ms reached 230+ seconds). Multi-term full scoring costs
~0.4–4s of CPU per real MSMarco query (5–15 terms, common terms with df
in the millions). Evidence: `kosha_8qps_saturation.log`.

## Headline 2: service latency at a sustainable 4 QPS

30 min per phase, 7,201/7,201 requests each, zero errors:

| | p50 | p90 | p99 | max |
|---|---|---|---|---|
| tpuf-benchmark published — warm | 13ms | 18ms | 29ms | — |
| **Kosha warm** | **304ms** | 1,695ms | 3,343ms | 6.5s |
| tpuf-benchmark published — cold | 316ms | 381ms | 559ms | — |
| **Kosha cold (30m window incl. transient)** | **287ms** | 1,600ms | 4,951ms | 8.4s |

Cold detail: cache wiped + process restarted, then 30m at 4 QPS. First
minute: p50 3.5s / p99 7.8s; converged to warm-equivalent (p50 280ms) by
~4 minutes. **Total hydration over the whole cold phase: 3.8GB of the
23GB namespace across 131 of 7,201 queries** — per-query lazy hydration
holds at 10M scale, and Kosha's cold *median* sits at tpuf's published
cold median (287ms vs 316ms) because most queries touch already-hydrated
shards. (Their cold is steady-state `disable_cache: true` — a stricter
definition; Kosha has no per-query cache bypass, so the transient window
is the nearest honest equivalent. Both definitions stated, pick your
reading.)

## Semantics caveat — Kosha's multi-term BM25 is AND

32–33% of requests returned zero hits (vs ~10% legitimate on the
synthetic corpus): Kosha requires **all** terms to match (documented in
kosha-query: "multi-term BM25 is AND"), so long natural-language queries
often match nothing, while tpuf/Lucene-style engines rank the union.
Two consequences:
1. The engines answer different questions on the same query stream —
   disclosed here, doesn't invalidate latency comparison but affects
   relevance comparability.
2. **Kosha currently pays OR-cost for AND-results**: it fully traverses
   and scores every term's postings, then intersects. A conjunctive
   query can instead be driven by the rarest term's postings (skip-list
   intersection), touching ~df(rarest) postings instead of Σdf — that
   plus positions-free scoring decode is likely a 10–50× scoring win on
   this workload *without* changing semantics.

## What closes the gap, in leverage order

1. **Intersection-driven / block-max multi-term scoring** (#85 extends
   from single-term): drive by rarest term, skip blocks below threshold.
   This is the entire warm gap — tpuf runs the same queries at 13ms via
   exactly this class of pruning; Lucene has shipped BMW since 8.0.
2. **tf-only postings decode on the scoring path** — positions decode is
   pure overhead for non-phrase queries.
3. **Ingest throughput** (secondary): 4.2k docs/sec with 32 idle cores —
   flush/upload serialization, relevant for time-to-benchmark, not
   queries.

Setup/teardown: `terraform/dev-machines/aws/bench-kosha` in the infra
repo (m7i.8xlarge ~$2/hr — destroy after each round). Corpus fetch:
`fetch_msmarco.py --out-dir /data/corpus --docs 10000000` (~40 min).
Raw artifacts: results JSONs + per-phase server logs archived with the
benchmark session.

---

# Addendum: 10M MSMarco round 2 — multi-term WAND + v5 skip-split (2026-08-09)

Same corpus, queries, protocol, and hardware as the round-1 addendum
above; engine now main @ #94 (`2dd7ae1`: #93 multi-term block-max WAND
leapfrog join + #94 v5 skip-split postings — positions out of the scoring
decode, write-time per-block upper bounds). Segments rewritten in v5
(10,000,000 docs, 167 segments, 23GB working set, all in S3). Parity/
hardening fixes #95/#96 landed after this image was built; they are
semantics fixes with negligible latency impact.

## Headline: tpuf's 8 QPS spec is now sustainable

Round 1 could not run 8 QPS at all (32 cores saturated at ~6.4 QPS,
queues diverged to 230+ seconds). Round 2 holds 8.00 QPS for the full 30
minutes of each phase — 14,401/14,401 requests, zero errors, both phases:

| 8 QPS, topk=10, 30min | p50 | p90 | p99 | max |
|---|---|---|---|---|
| tpuf published — warm | 13ms | 18ms | 29ms | — |
| **Kosha warm** | **449ms** | 1,374ms | 2,295ms | 4.5s |
| tpuf published — cold | 316ms | 381ms | 559ms | — |
| **Kosha cold (wipe + 30m window)** | **431ms** | 1,218ms | 4,641ms | 8.8s |

Round-over-round at the same rate (4 QPS, warm, 10-minute run):
**304ms → 224ms p50** (1.36× service-time improvement); the 8 QPS p50 of
449ms is that service time plus near-saturation queueing (load ~31/32). Correctness invariant held: zero-hit rate 33.2%
in both phases, identical to round 1's AND-semantics rate — the
optimizations changed latency, not results. Warm phase hydrated **0
bytes**; cold hydrated 4.1GB of 23GB lazily, first-minute p50 3.1s
converging to warm within minutes.

## Where the remaining 34× lives (server-side, all 14,401 warm requests)

| phase | p50 | p90 | p99 |
|---|---|---|---|
| score (leapfrog intersection walk) | 323ms | 956ms | 1,655ms |
| hydrate (per-query blob presence checks) | 86ms | 92ms | 101ms |
| queue | 0ms | 248ms | 893ms |
| admit | 2ms | 68ms | 765ms |
| materialize | 0.1ms | 0.1ms | 0.2ms |

1. **`score_ms` is no longer BM25 math — it's the intersection walk.**
   v5 already stripped positions and made block UBs free; the cost is
   advancing cursors through stopword-scale lists to *enumerate* the AND
   intersection, which exact `total_hits` makes mandatory (a 3-hit
   intersection can cost ~900ms of walking). The unlock is **capped
   counts** (ES `track_total_hits`-style: exact to 10k, `gte` beyond) —
   with the cap, the join can early-terminate once the page is stable,
   and MaxScore-class term partitioning becomes applicable. Plausibly
   5–15× on broad queries.
2. **`hydrate_ms` is a flat ~86–92ms/query tax** — per-query
   posting-blob existence stats scaling with segments × terms (167 × ~5
   ≈ 800+ metadata stats). A per-`(namespace, manifest-version, shard)`
   presence cache removes ~20% of p50 outright.
3. **Queue/admit only appear at p90+** — saturation artifacts that
   shrink as service time drops.
4. **167 segments multiply fixed per-query costs** — compaction to ~16
   large segments (blocked on the tiered doc-loss bug) trims cursor
   setup, TOC lookups, and fan-out overhead.

Trajectory: unrunnable → 100× off (round 1 @4QPS) → 34× off at full
spec, in two days, with every remaining contributor named and owned.

---

# Addendum: 10M MSMarco round 5 — OR-mode + result cache (2026-08-09)

Engine: main @ #110 (adds #104 rarest-first probe, #106 hot-path
allocs, #108 OR-mode union WAND, #110 whole-response result cache).
Four 30-minute 8 QPS phases on one build, 57,604 requests total, zero
errors everywhere.

| phase | p50 | p90 | p99 | zero-hit |
|---|---|---|---|---|
| warm, AND, cache-off | 379ms | 1,059ms | 1,857ms | 33% (AND invariant) |
| warm, OR, cache-off | **174ms** | **309ms** | **481ms** | **0.06%** |
| warm, OR, cache-on | **6.0ms** | 107ms | 329ms | 0.06% |
| cold, OR, cache-on | 6.0ms | 104ms | 6,483ms | 0.06% |
| tpuf published — warm | 13ms | 18ms | 29ms | — |
| tpuf published — cold | 316ms | 381ms | 559ms | — |

## Findings

1. **OR-mode (#108) is the structural win the roadmap predicted**: −54%
   p50 and −74% p99 vs AND on the same build, with the p99/p50 ratio
   tightening from 4.9× to 2.8× (tpuf's shape is 2.2×) — the signature
   of pruning-friendly union execution. It also ends the 33%-empty-
   results UX: union matching leaves 0.06% zero-hit.
2. **The result cache (#110) collapses the protocol's steady state**:
   warm p50 6.0ms — under tpuf's published 13ms — with 32 cores near
   idle (load 0.08 vs 25–29 in every uncached phase). The honest
   framing: the tpuf-benchmark protocol repeats each of 1,677 queries
   ~8.6× per phase, so cache-on p50 measures the hit path; p90 (107ms)
   is the miss path, ≈ the cache-off distribution as expected. Engine-
   vs-engine comparisons should quote the cache-off row unless the
   competitor's caching posture is known.
3. **Cold with cache**: p50 6ms immediately (the cache needs no segment
   warmth), p99 6.5s = the S3 rehydration transient on misses, same
   cold story as previous rounds underneath a warm hit path.

## Where this leaves the gap

Cache-off OR — the honest engine number — stands at 174/309/481 vs
13/18/29: ~13× at p50, with the remaining decomposition (probe-measured
in round 4) split between per-query fixed costs at 167 segments,
concurrency contention, and union-WAND execution constants. The next
levers per the roadmap: impact-ordered threshold bootstrapping,
compaction (blocked on tiered doc-loss), cross-segment floor sharing.

Trajectory across three days: 8 QPS unrunnable → 34× → 13× cache-off,
and past tpuf's published warm median with the cache the protocol
rewards.

---

# Addendum — 10M MSMarco round 6: cold, cache-off (2026-08-09)

The one cell rounds 1–5 never measured: a cold namespace with the result
cache bypassed (`no_cache: true`), i.e. honest engine speed on first
contact with the data. Fresh m7i.8xlarge, same corpus/queries/protocol,
engine main @ `4f505dc` (round-5 build + #101 presence-cache hardening);
bench script from #111. Local segment store wiped (`rm -rf` + container
restart), then one 30-minute 8 QPS OR-mode phase: 14,401/14,401
requests, zero errors, zero-hit 0.062% (union corridor — valid run).

| 8 QPS, topk=10, 30min | p50 | p90 | p99 | max |
|---|---|---|---|---|
| **Kosha cold, OR, cache-off** | **184ms** | **366ms** | 7,858ms | 14.2s |
| tpuf published — cold | 316ms | 381ms | 559ms | — |

Cold p50 beats tpuf's published cold median by 1.7×; p90 is at parity.
The p99 gap is entirely a **startup convoy**, not steady-state behavior:

| window (server-side totals) | p50 | p90 | p99 |
|---|---|---|---|
| first 3 minutes (~1,440 reqs) | 196ms | 7,847ms | 12,353ms |
| **steady state (minutes 3–30)** | **170ms** | **333ms** | **529ms** |

Every one of the 150 slowest requests landed in the first tenth of the
run. Their median breakdown: **7.4s queue + 2.0s hydrate, score only
~130ms** — at t=0 every query needs S3 blob hydration across 167
segments, the hydrator saturates, and arrivals behind the burst wait in
line. Once first-touch hydration clears (~3 minutes), cold-steady-state
p99 of 529ms sits at tpuf's published 559ms.

Implications:
1. **Steady-state cold is already at parity with tpuf** across the
   distribution; the whole headline p99 is a transient measured once per
   cold start.
2. **The convoy is addressable**: pre-hydration warmup on namespace
   attach (bulk-fetch posting blobs ahead of query traffic) would
   collapse the transient; compaction 167→16 segments cuts the
   first-touch fan-out ~10× independently.
3. The presentation pair for engine comparisons: warm cache-on
   (6.0/107/329) + cold cache-off (184/366/7,858), with the windowed
   decomposition above as the honest footnote on the cold tail.
