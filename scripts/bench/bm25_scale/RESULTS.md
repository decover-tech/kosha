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
