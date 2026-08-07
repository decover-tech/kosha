# BM25 scale benchmark

Replicates turbopuffer's tpuf-benchmark "Full-Text Perf" methodology
(BM25, 10M docs, ~9GB, 8 QPS, topk=10, p50/p90/p99, warm vs cold namespace)
against Kosha, so the two are comparable. "Docs" here means chunk-sized
records (~900 bytes each) — the same granularity as Kosha's own indices
(`paragraph_index_hnsw`, `findings_index`, `white_river_paragraph`), not
whole legal documents.

Scope: **full-text (BM25) only.** The vector/kNN half of tpuf-benchmark
(1024-dim, 10M docs, ~40GB) is intentionally out of scope for this first
pass — Kosha's vector path has zero production traffic and has never been
run at that scale; see the assessment in the accompanying chat/PR
description before attempting it. Running it would be a genuine
scale-feasibility test of unproven code, not a rerun of this harness.

## Prerequisites

1. **Server-side flush threshold.** Kosha's live default (`flush_threshold =
   1000`) is tuned for steady-state writes, not bulk loads — at 10M docs that
   produces ~10,000 segments before any compaction. Set
   `KOSHA_FLUSH_THRESHOLD` (added in this same change, `main.rs`) on the
   `kosha-server` deployment before loading — e.g. `50000`. This requires
   rebuilding/redeploying kosha-server with this branch's patch, then a
   config change to set the env var (k8s deployment env, not a ConfigMap key
   today — there's no existing ConfigMap wiring for it, add a literal env
   entry to `k8s/base/deployment.yaml` or an overlay patch).
2. **Segment cache sized for the corpus.** Set `KOSHA_SEGMENT_CACHE_MAX_BYTES`
   comfortably above ~9GB (e.g. `12884901888` for 12GiB) so the "warm" run
   can actually hold the whole corpus resident. Staging's kosha pod already
   has a 56Gi memory limit, so this is headroom, not a resize.
3. A dedicated namespace (e.g. `bm25-bench-10m`) — namespaces are cheap and
   isolated, so this doesn't touch any other tenant's data. It **does**
   share the single staging pod's CPU/disk/memory and the on-disk cache is
   process-wide, so other namespaces' warm cache gets evicted by
   `reset_cache.sh` and query latency for other tenants will be affected
   while this runs.

## Run sequence

```bash
# 1. Generate the corpus once (reproducible via --seed; ~9GB on disk, budget
#    real wall-clock time and disk space for this step).
python3 generate_corpus.py --out-dir /data/bm25-10m --docs 10000000 \
    --avg-bytes 900 --seed 42

# 2. Bulk-load it. --compact-after merges whatever segment count resulted
#    down via the existing (synchronous) admin endpoint.
python3 load_corpus.py --host http://localhost:8080 \
    --namespace bm25-bench-10m --corpus-dir /data/bm25-10m \
    --api-key "$KOSHA_API_KEY" --batch-size 20000 --concurrency 4 \
    --compact-after

# 3. Cold run: reset caches, then measure immediately.
./reset_cache.sh kosha kosha
python3 query_bench.py --host http://localhost:8080 \
    --namespace bm25-bench-10m --queries-file /data/bm25-10m/queries.txt \
    --api-key "$KOSHA_API_KEY" --qps 8 --topk 10 --duration 120 \
    --phase cold --out cold_results.json

# 4. Warm run: same queries again, right after — cache is now populated
#    from step 3's fetches.
python3 query_bench.py --host http://localhost:8080 \
    --namespace bm25-bench-10m --queries-file /data/bm25-10m/queries.txt \
    --api-key "$KOSHA_API_KEY" --qps 8 --topk 10 --duration 120 \
    --phase warm --out warm_results.json
```

## Notes on fidelity to the tpuf-benchmark chart

- **QPS control is open-loop** (`query_bench.py` sleeps to a fixed send
  schedule regardless of response time) — this is what actually exercises
  queueing/tail latency under load; a closed-loop generator would just
  measure single-connection round-trip time.
- **Query set is sampled from the corpus's own term distribution**
  (Zipfian, same vocabulary as the corpus), so query selectivity — the mix
  of "matches almost everything" vs "matches almost nothing" queries that
  drives BM25's p50-vs-p99 spread — is representative rather than arbitrary.
- **Cold vs warm is a real cache-state distinction**, not a label: cold
  clears both the on-disk NVMe cache and the in-process parsed-segment cache
  (via a pod restart) before the first touch; warm reuses whatever the cold
  run just populated.
- Not controlled for (differences from tpuf-benchmark's actual setup, which
  is not public): exact hardware/instance type, exact corpus text
  distribution (turbopuffer's is undocumented; this uses a Zipfian synthetic
  corpus), and whether tpuf-benchmark's "8 QPS" is per-namespace or
  aggregate across concurrent namespaces.
