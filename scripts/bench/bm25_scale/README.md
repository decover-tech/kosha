# BM25 scale benchmark

Replicates turbopuffer's tpuf-benchmark methodology (8 QPS, topk=10,
p50/p90/p99, warm vs cold namespace) against Kosha, so the two are
comparable.

> **Corpus size (corrected 2026-11-21):** turbopuffer's published
> full-text numbers (warm 13/18/29 ms, cold 316/381/559 ms) are for
> **1M docs, ~300MB** ([docs](https://turbopuffer.com/docs-md/performance.md)),
> not 10M. Its vector numbers (warm 14/17/27 ms, cold 874/1214/1686 ms) are
> for 10M docs. Rounds before 2026-11-21 compared Kosha at 10M BM25 docs
> against that 1M figure. Use `bm25_docs: 1m` in the `benchmark` workflow
> for the like-for-like BM25 comparison; 10m remains the scale test. "Docs" here means chunk-sized
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
   entry to `k8s/base/deployment.yaml` or an overlay patch). Also set
   `KOSHA_FLUSH_MAX_AGE_MS=0`: the idle-buffer flush timer (#241, default
   1 s) would otherwise cut a segment from whatever arrived in the last
   second instead of waiting for the threshold. And set
   `KOSHA_COMPACTION_INTERVAL_MS=0`: the background compaction scheduler
   (#242) would otherwise merge segments mid-load and collide (`409`) with
   `load_corpus.py`'s own `--compact-mode` passes.
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

## CI benchmark: warm gate, cache reuse, sidecar snapshot, quick runs

The `benchmark` workflow (`.github/workflows/benchmark.yml`) runs these
scripts on the bench VM. Its inputs choose how the namespace gets warm,
which vector snapshot to restore, and whether to keep the VM.

### Warm gate (`warm_gate`: `queries` (default), `preload`, `timed` or `cold`)

`queries` follows turbopuffer's tpuf-benchmark (`pkg/bench/bench.go`).
For hot runs, tpuf warms the namespace, then polls with the workload's own
queries until `performance.cache_hit_ratio >= 1.0` on 3 consecutive
queries, backing off on a miss. Only after that does the measured phase
start, and warmup time is never measured. In this mode:

- The kosha container starts **without** `KOSHA_WARMUP_NAMESPACES`, so no
  boot preload runs and `/readyz` is green right away.
- Span promotion is **off** on the container
  (`KOSHA_DOC_SPAN_PROMOTE_GETS/BYTES=0`,
  `KOSHA_POSTING_SPAN_PROMOTE_GETS/BYTES=0`). By default, after 64 ranged
  GETs or 16 MiB on one segment's doc store (32 or 8 MiB on a postings
  shard), the server downloads the whole file in the background. On the
  original vector snapshot that is ~1 GB of JSON-vector `doc_store.bin`
  per segment, and a large enough hydrate pass would queue it for most
  segments. `fully_cached` reports only what the search itself fetched,
  not promotions still running, so the gate could pass while hundreds of
  GB were still being written during the measured phases. With promotion
  off, warm queries read the spans the gate made local and nothing else
  downloads. This differs from the server defaults: promotion would turn
  many small span files into one whole file, which this mode does not
  measure.
- `run_phases.sh` does not wipe or restart. It runs `warm_gate.py` with the
  phase's workload (BM25 `--operator or`, or kNN `--knn-embeddings`) and
  builds each payload with `query_bench.py`'s own `payload_for`, always
  with `no_cache: true` and `timings: true`. The gate works in four steps:
  1. A probe query. If the response has no `timings.fully_cached`, the gate
     exits 2 with `WARM-GATE-UNSUPPORTED` (the server is older than the
     cache-hit gate, or `KOSHA_DASHBOARD` is off).
  2. A hydrate pass that sends every query in `queries.txt` once, 16 at a
     time. Each query fetches what it reads: scoring files, `vector.idx`,
     and the ranged doc-store spans of its result pages, which the span
     cache keeps.
  3. A verify pass. Every query whose last response was not fully cached
     (an error, a timeout, a shed or degraded search, or just the query
     that did the fetching) is sent again, 16 at a time, round after
     round with backoff (1 s doubling to at most 30 s), until **every**
     query in `queries.txt` has come back fully cached at least once. The
     measured phases cycle through all of them, so one left cold would
     read S3 during a warm phase.
  4. tpuf's check: queries drawn at random from the whole set until 3
     consecutive responses have `timings.fully_cached == true`. A miss
     resets the streak, backs off, and sends that query back through the
     verify step.
  It prints `WARM-GATE-READY in <s>s (queries sent, s3 bytes observed)`,
  or `WARM-GATE-TIMEOUT` and exits 1 (budget: `WARM_GATE_TIMEOUT_S`,
  default 3h). The output goes to `<suite>/warm_gate.log` and the server
  log to `<suite>/server/warm_gate.log.gz`. Both are archived, and the
  job summary prints the final marker line.
- Only warm phases are measured: BM25 runs `warm_or_nocache` and
  `warm_or_cache`, and vector runs `knn_warm_nocache` and
  `knn_warm_cache`. The old warmup-gated "cold" phase is **dropped** in
  this mode because it is not turbopuffer's cold. tpuf cold sends
  `disable_cache: true` on every query, so every query reads object
  storage. Kosha has no equivalent switch, and a phase that runs after a
  full preload measures a warm engine.

`fully_cached` is true when a search made no S3 request of any kind and
nothing it needed was missing or shed. A result-cache hit also counts, and
that is why the gate always sends `no_cache`. The flag is new in the
cache-hit-gate server PR (`feat/bench-cache-hit-gate`). Until that server
is what the workflow builds, `queries` fails fast with
`WARM-GATE-UNSUPPORTED`; use `preload`.

`preload` keeps the earlier protocol unchanged: wipe the local store,
restart with the boot preload, wait for `/readyz`, then run the cold and
warm phases. On the original vector snapshot the boot preload takes about
115 minutes, because the server preloads the whole ~249 GB namespace.
Its "cold" phase runs after that preload, so it measures a warm engine.

`cold` (research, #380) measures first touch with no preload at all. The
container starts without `KOSHA_WARMUP_NAMESPACES`, with
`KOSHA_WARMUP_USAGE=off` (else the persisted usage warm set is warmed at
boot) and with span promotion off. `run_phases.sh` checks the first two,
wipes the local store, restarts, and runs one phase only
(`knn_cold_nocache` or `cold_or_nocache`). Every byte a query needs is
read from S3 during that phase, so the `search timing:` fields
`hydrate_files`, `hydrate_mb`, `s3_bytes`, `vector_gets` and
`postings_gets` in `server/<phase>.log.gz` are the per-query cold cost.
A first-touch kNN query still fetches the small files (footer,
`inverted.idx`, `doc_store.offsets`, `vector.offsets`) of every segment,
because `knn_plan` opens every segment, so read the phase's first minutes
apart from the rest (`knn_log_stats.py --skip-first-s`).

`timed` (research #389) is for runs with a disk cache smaller than the
working set, where `queries` cannot converge: a later query evicts what an
earlier one needs, so "every query fully cached once" never happens.
`run_phases.sh` then empties the local store (every arm starts cold, and
the bound covers only the suite namespace), runs the workload unmeasured
for `fill` seconds, and measures only the no-result-cache phase,
`repeats` times (`warm_or_nocache_rN` / `knn_warm_nocache_rN`).
S3 misses are part of the measured latency by design. It writes
`footprint.tsv` (local bytes every 60 s), `phase_footprint.tsv` (start
and end samples for each measured repeat), and `evictions.tsv` (files and MB
the server evicted per stage).

### Disk-cache experiment input (research #389): `cache_experiment`

One input, a space-separated `key=value` list, so the workflow stays under
GitHub's limit of 25 dispatch inputs. Empty (the default) is what every
earlier run used. The "Validate disk-cache experiment inputs" step
rejects unknown keys and bad values before anything is provisioned.

| key | default | effect |
| --- | --- | --- |
| `max` | `644245094400` | `KOSHA_CACHE_MAX_BYTES`. A smaller value needs `warm_gate=timed` and one suite per dispatch (the validate step refuses otherwise). |
| `policy` | `legacy` | `sieve` / `s3fifo` set `KOSHA_CACHE_POLICY`. The policy runs per tenant: #342 still picks which tenant gives up bytes, and that tenant's own policy state picks the file. |
| `trace` | `false` | `true` sets `KOSHA_CACHE_TRACE_PATH` on the container; the trace and an S3 size listing are archived as `server/cache-trace.jsonl.gz` and `server/s3-sizes.tsv.gz` for `scripts/bench/cache_sim/cache_sim.py replay --strip-prefix segments/`. |
| `order` | `cycle` | `query_bench.py --order`: `shuffle` or `zipf` (seeded, the same sequence for every arm). |
| `fill`, `repeats` | `900`, `1` | `timed` mode only: seconds of unmeasured workload, then measured repeats (1-5). |

Example: `cache_experiment="max=107374182400 policy=s3fifo order=zipf fill=600 repeats=3"` with `warm_gate=timed suite=bm25`.

There is no `cache_accounting` key any more: #342 made the cache count
every file once on every path, which is what `KOSHA_CACHE_ACCOUNTING=exact`
used to provide. A legacy control and a policy arm get the same effective
capacity without it.

For each timed run, download the `bench-results` artifact and report the arm:

```sh
python3 scripts/bench/cache_sim/cache_sim.py arm --run-dir results > arm.json
```

The JSON includes per-repeat results and pooled latency percentiles, S3 work
per submitted query, and estimated local GiB-hours and cost. Local occupancy
is integrated between samples during measured repeats only; warm fill and
the gaps between repeats are excluded. This prices occupied bytes, not the
entire provisioned volume. Use the same deployment prices for all arms;
`arm --help` lists the overrides. Check HTTP errors and degraded kNN
requests before treating an arm as comparable. GETs are estimates from logged
searches; failed requests may do work that the timing lines do not capture.

Compare tails with `cache_sim.py p99ci --a <control raw-out files> --b <arm
raw-out files>` (bootstrap CIs for p50/p99 and their difference).

### What each hit returns (`return_fields`: `ids` (default) or `all`)

turbopuffer's benchmark queries return each hit's id and distance only (no
`include_attributes`). `return_fields=ids` matches that: `query_bench.py`,
`warm_gate.py` and `recall_check.py` send `source_includes: []`, so a hit is
`doc_id` + `score`. `all` returns every stored field, including the 1024-dim
embedding as JSON. That made top-10 kNN responses ~100-200KB (estimated),
so the 1,677-query cycle (~165-330MB) overflowed the 64MB result cache
(`KOSHA_RESULT_CACHE_MAX_BYTES`): run 44967150572 served 0 of 14,401
cache-on kNN requests from the cache. Runs before this change used `all`.

The server still reads each returned document's record before dropping its
fields (materialize was ~1 ms per query in that run); skipping the read for
an ids-only request is a possible server follow-up.

### Reusing the local cache on a reused VM

A VM survives a run when the run fails (standing rule) or when `keep_vm`
is on. Before this change, every restore wiped `/data/kosha/data` and
downloaded everything again. Now `restore_namespace.sh` records which
snapshot populated the local cache and the bench bucket. It uses
`vm/snapshot_cache.sh` and keeps markers under
`/data/bench-snapshot-markers/`. A snapshot's identity is its name plus
the S3 ETag and LastModified of its `pg-kosha.sql.gz`, which is
re-uploaded on every (re-)snapshot.

- Restoring the **same** identity keeps the local cache and skips
  re-copying bucket objects. Pass `KEEP_LOCAL_CACHE=0` to force a wipe.
- Wipes in `restore_namespace.sh` and `build_and_import.sh` are **per
  namespace**: they remove `/data/kosha/data/<tenant>/<ns>` (and a
  pre-#262 bare `<ns>` dir), the local WAL, and only the `local.<ns>`
  marker. So a `suite=both` run keeps both caches on a kept VM: the vector
  restore no longer wipes the BM25 cache or its marker, and the other way
  round. Both caches then sit on the 1,000 GB root volume together (the
  vector one is ~249 GB on the original snapshot), under the server
  `KOSHA_CACHE_MAX_BYTES` of 600 GiB.
- **Any mismatch** wipes. That covers a different snapshot, the same name
  re-created, and a missing marker. Segment ids repeat across rebuilds of
  a namespace name (`<ns>-offline-NNNNNN`), so stale local files or bucket
  objects would be served silently wrong. On a mismatch, the bucket prefix
  is also emptied before `aws s3 sync`, because sync neither deletes
  extra objects nor reliably replaces same-size ones.
- Markers are removed before the state they describe is touched, and
  written only after a successful restore. A failed restore leaves no
  marker, so the next restore wipes.
- `run_phases.sh` never wipes in `queries` mode, where warmup is not
  measured. In `preload` mode it always wipes the whole local store,
  because that mode measures warmup, and the wipe also drops every
  `local.*` marker.
- Caveat: a kept cache was written by the previous run's server build.
  Use `KEEP_LOCAL_CACHE=0` (or a fresh VM) when a change alters the local
  file layout.

### Vector snapshot with the binary sidecar (`vector_snapshot`, `build_vector_snapshot`)

The original `msmarco-10m-vec` snapshot stores every vector as JSON
inside `doc_store.bin`, about 1 GB per segment. `kosha-build-segments`
writes through `SegmentWriter`, whose vector store follows
`KOSHA_VECTOR_STORE`. That variable is unset in the builder container,
which means **sidecar**, so a rebuild puts vectors in `doc_vectors.bin`
instead. Estimate, not measured: about 200 segments × (~205 MB
`vector.idx` + ~205 MB `doc_vectors.bin` + a small text doc store) is
roughly 80–90 GB, against 249 GB.

- `vector_snapshot` names the snapshot that the vector suite restores into
  namespace `msmarco-10m-vec`. The default is `msmarco-10m-vec` (today's).
- `build_vector_snapshot: true` with a **new** name (for example
  `msmarco-10m-vec-sidecar`) builds that snapshot once, offline, with
  `SNAPSHOT_NAME=<name> build_and_import.sh msmarco-10m-vec
  --with-embeddings`. If the snapshot already exists, the step skips the
  build and only restores. The workflow refuses to build under
  `msmarco-10m-vec`, and `build_and_import.sh` refuses to overwrite any
  existing snapshot unless `SNAPSHOT_OVERWRITE=1` is set. The build path
  is off by default. It syncs ~40 GB+ of embeddings, builds 10M docs and
  uploads the segments. That is untimed so far, so run it with
  `suite=vector` and watch the 350-minute limit.
- `build_segments_args` (research, #380; empty by default) passes extra
  `kosha-build-segments` flags to that build, for example
  `--cluster-layout namespace`.
- Provenance: `build_and_import.sh` writes
  `snapshots/<name>/build-info.json` (builder commit from `github.sha`, a
  digest of the builder sources, build args, max docs). The vector suite
  copies the restored snapshot's file to `vector/snapshot-build-info.json`
  in the results, or logs that it has none. Snapshots built before this
  change have none, so their builder commit is unknown. To compare two
  layouts, build both under new names from one commit rather than reusing
  an older snapshot.

### Compressed doc store snapshots (research #388, experimental)

`build_and_import.sh` passes `KOSHA_DOC_STORE_COMPRESSION` and the
`KOSHA_DOC_STORE_ZSTD_*` tuning variables into the builder, and a
`SNAPSHOT_NAME` containing `-zstd` defaults the first to `zstd`. That
name rule only exists on refs that carry #388, and the workflow rsyncs
the **dispatching ref** to the VM, so dispatch these builds from the #388
branch (`gh workflow run benchmark.yml --ref <branch> ...`). A ref without
#388 would build raw segments under the `-zstd` name, and the workflow
never rebuilds an existing snapshot; on refs with #388 the build now
refuses to upload when `zstd` was asked for and no footer is compressed.

Every build also writes `snapshots/<name>/build_info.txt` with the
builder env and a fingerprint (`source_sha256`) of the sources it
compiled: the VM copy has no `.git`, so two snapshots come from the same
commit exactly when their fingerprints match. Build the raw baseline for
a comparison from the same ref under a name without `-zstd`.

`vm/doc_store_bytes.sh <snapshot> [zstd|raw]` is the deterministic size
check: it sums the snapshot's `doc_store.bin`, `doc_store.offsets` and
`doc_vectors.bin` object sizes, reads every segment footer, splits
`doc_store.bin` into stored records and the dictionary tail copy, and
prints one JSON object (`doc_store_bytes.py`). With a format argument it
exits 3 when the footers disagree. It only lists and downloads.

### Fast iteration recipe

Dispatch with `suite=vector` (or `bm25`, or `both`), `warm_gate=queries`,
`duration=300` and `keep_vm=true`, then dispatch again for each change.
The last run turns `keep_vm` off, or a `suite=none` run tears down.
Approximate times per stage:

| stage | first run (fresh VM) | rerun on the kept VM |
|---|---|---|
| terraform apply | VM create, a few min | no-op, ~1 min |
| wait for SSH, rsync, pip | cloud-init, a few min | <1 min |
| docker build | full release build | cached layers; a Rust change rebuilds |
| restore (same snapshot) | S3 copy + pg load, minutes | sync no-op + pg load + restart, ~2–3 min |
| warm gate | hydrates what the workload reads from S3 (not measured yet) | local files only: ~1 min (warmup from local files took ~10 s in August) |
| phases | 2 × 5 min | 2 × 5 min |
| archive + summary | ~1 min | ~1 min |

For comparison, `warm_gate=preload` on the original vector snapshot
spends about 115 minutes in warmup alone, before its phases.

### Codes-only vector tier comparison (#381)

Use one commit, the same 10M MSMarco corpus and query workload, and equivalent
starting caches (a fresh VM, or a wiped cache, for each arm). Build one
sidecar snapshot per format using a fresh `vector_snapshot` name and
`build_vector_snapshot=true`; pass `KOSHA_VECTOR_QUANTIZATION=f32`, `i8`,
or `rabitq` in `server_env`. Reuse each snapshot for its flag-on/off pair.
For every arm, use `suite=vector`, `warm_gate=queries`, `qps=8`,
`duration=300`, `run_recall_check=true`, and `recall_sample=1000`.
Include `KOSHA_KNN_RESCORE_STATS=1` in every arm. The bench enables search
timing logs for all requests.

| Arm | Format | Additional `server_env` |
|---|---|---|
| Control | f32 | none (repeat with global on to measure control overhead) |
| i8 off | i8 | none |
| i8 on | i8 | `KOSHA_KNN_GLOBAL_RESCORE=1` (strict bound by default; optional separate probabilistic-bound arm) |
| RaBitQ off | rabitq | none |
| RaBitQ on | rabitq | `KOSHA_KNN_GLOBAL_RESCORE=1` |

The regular workflow recall step samples the *first* queries. For the #381
success bar, use `keep_vm=true` and, before switching to the next arm, rerun
`recall_check.py` against that arm with the same arguments as the workflow
recall step plus `--sampling random --seed 384`. Save that JSON as
`vector/recall.json` in the local copy of the archive. Use the same random
sample and recall probe settings for every arm at a given comparison point.
The standard warm latency phase uses the default query probe count (100);
vary the probe settings consistently across arms for a curve, checking logged
`knn_postings` for actual probe counts.

From the repository root, point the report at downloaded run directories
(each containing `meta.json` and `vector/`):

```sh
python3 scripts/bench/bm25_scale/rescore_report.py runs/f32-off runs/i8-off \
    runs/i8-on runs/rabitq-off runs/rabitq-on
```

Add `--json` for per-counter distributions and sample counts. Format comes
from vector column footers recorded in `vector/snapshot-build-info.json`;
older archives without this metadata show an unknown format. Compare recall
against warm p50/p99 at matched probe settings; compare each arm to the f32
sidecar control for the 0.01 recall tolerance, 2× warm p50 reduction, and 10×
local vector byte reduction. Warm-gate time means time for the workload to
become cached, not time to preload a namespace. The local byte total includes
vector indexes, offsets, and exact-vector files or cached spans. S3 bytes are
the server's `s3_bytes`; ranged GETs sum the reported vector, postings, and page
range counts, not whole-file requests. Missing counters are not zero, and
incomplete log coverage is marked separately.

The default report checks for whole `doc_vectors.bin` files in a codes-only
run. Flag-off quantized arms can return unrescored estimates in this cohort;
that is not a comparison to local per-segment rescoring. To compare the two
rescoring algorithms with whole vectors local, run a separate matched cohort
with `KOSHA_DOC_STORE_READ_MODE=whole` and report with
`--allow-whole-sidecar`. Verify actual locality and the
`knn_seg_unrescored` / `knn_seg_quantized` counters; enabling whole page reads
does not guarantee the sidecar is present on every probed segment.

### Defaults and the pass/fail bar (`bench_gate`)

Every dispatch now runs the recall check (`run_recall_check=true`,
`recall_sample=500`) and the `fractions` kNN sweep by default, so no kNN
latency number is published without a paired recall number. The sweep adds
about 55 minutes; clear `knn_sweep` to skip it.

The last step, after teardown, runs `bench_gate.py` over the collected results
and fails the run when a phase has errors or zero-hit requests, or when
tie-tolerant recall@10 is below `recall_min` (0.93 by default), or when the
recall check was expected and produced nothing. The sweep is never gated.
Override the bar with `bench_gate="recall_min=0.9 max_error_rate=0.001"`, or
report only with `bench_gate=off` (for example for a server_env experiment
that trades recall on purpose). Running the gate after teardown means a
failed bar never keeps the VM.

### kNN probe-budget sweep (`knn_sweep`, #384)

Recall@10 against p50/p99 and postings probed, one point per server
configuration, without a new VM or a new snapshot per point. Dispatch with
`suite=vector`, `warm_gate=queries`, `run_recall_check=true`,
`duration=300` and `knn_sweep=fractions` (or a point list). After the
normal vector phases, `vm/knn_budget_sweep.sh`, for each point:

1. recreates the kosha container with its original environment plus the
   point's env vars (the local NVMe cache is kept, so only the in-process
   caches restart cold), and re-runs `warm_gate.py`;
2. runs a warm, result-cache-off kNN phase (`query_bench.py
   --server-timings --raw-out`) at `qps` for `duration` seconds;
3. runs `recall_check.py` on 1,000 random queries (seed 384) with
   `--server-timings --no-cache`, sharing one ground-truth cache file
   across points (the first point pays the brute-force scan).

The container is put back to its original configuration at the end, and
`knn_sweep_report.py` writes `knn_sweep/sweep.{md,json,csv}` (the `.md`
table is appended to the job summary). Points:

- `fractions`: `KOSHA_KNN_BUDGET_FRACTION` = 0.05, 0.03, 0.02, 0.01
  (labels `f0.05` ... `f0.01`; `f0.05` is the baseline the report's
  success-bar column compares against).
- `relative`: the #384 step 2 grid, 8 points: `f0.05` (baseline), `a0`
  and `a0.5` (`KOSHA_KNN_PROBE_RADIUS_ALPHA` alone), `a0rel0.5`, `a0rel1`,
  `a0rel2` (alpha 0 plus `KOSHA_KNN_RELATIVE_CUTOFF_EPS` 0.5 / 1 / 2), and
  `a0rel1m500`, `a0rel0.5m1000` (`KOSHA_KNN_RELATIVE_MIN_PROBES` 500 /
  1000). The relative points all use alpha 0: at alpha 1 the best score
  is usually 0 and eps has no effect.
- `bound`: the alpha-scaled bound stop (`KOSHA_KNN_BOUND_STOP_ALPHA`), 5
  points: `f0.05` (baseline), `b1`, `b0.95`,
  `b0.9`, `b0.85`. `b1` only skips postings whose similarity bound cannot
  beat the segment's k-th score; the report's `knn_postings` column against
  `f0.05` is the #384 success bar.
- `label:ENV=V,ENV=V;label2:...`, over these knobs only:
  `KOSHA_KNN_BUDGET_FRACTION`, `KOSHA_KNN_BUDGET_MAX_CAP`,
  `KOSHA_KNN_RELATIVE_CUTOFF_EPS`, `KOSHA_KNN_RELATIVE_MIN_PROBES`,
  `KOSHA_KNN_PROBE_RADIUS_ALPHA`, `KOSHA_KNN_BOUND_STOP_ALPHA`. Include an
  `f0.05` point (or pass the
  report `--baseline`) for the comparison column.
- `radius`: the #458 probe-order radius (`KOSHA_KNN_PROBE_RADIUS`), 5
  points: `f0.05` (baseline, max radius), `rp90`, `rp50`, `rp90a0.5`
  (p90 at alpha 0.5) and `rp90rel1m500` (p90, alpha 0, eps 1, min probes
  500). Only meaningful on a `vector_snapshot` built with
  `KOSHA_VECTOR_WRITE_VERSION=5` (p50/p90 stored); older segments fall
  back to the max radius.

`recall_check.py` reports, per query and summarized (mean, p1/p5/p10/p50):
exact-id recall@k, ANN-Benchmarks distance-threshold recall (a returned
doc counts if its exact distance is within the true k-th distance +
1e-3, so exact ties count) and epsilon recall (within (1 + 0.01) x the
k-th distance), plus each query's `timings.knn_postings`. Distances are
computed from the corpus embeddings, never taken from the server's scores.
Servers before #384 do not report `timings.knn_postings`; the report then
reads `knn_postings=` from the `search timing:` log lines instead.

The success bar (`meets_bar`) is judged on one query set: postings,
mean recall and p10 recall all come from the 1,000 recall queries
(`recall_postings_mean`). The latency phase's postings
(`phase_postings_mean`, the workload queries in order) are shown next to
them with their own ratio, and `pair_check` reads `DISAGREE` when the two
ratios fall on different sides of 0.5 or differ by more than 15%.

With the relative cutoff on, each kNN search also reports
`timings.knn_cutoff_rule` (and `knn_cutoff_rule=` on its `search timing:`
line): `budget` (the fixed budget capped it), `eps` (`(1 + eps) x best`
decided), `floor` (`KOSHA_KNN_RELATIVE_MIN_PROBES` decided) or
`zero_best` (the best score was 0, so eps had no effect). The report's
`cutoff_rules` column gives each rule's share of the recall queries.
Neither field appears with the flag off.

The #384 engine flags are off by default:

| env var | default | effect |
|---|---|---|
| `KOSHA_KNN_RELATIVE_CUTOFF_EPS` | unset (off) | unfiltered kNN probes a posting only if its rank score <= (1 + eps) x the best posting's score (SPANN's relative cutoff); never fewer than `KOSHA_KNN_RELATIVE_MIN_PROBES` postings, never more than the fixed budget |
| `KOSHA_KNN_RELATIVE_MIN_PROBES` | the query's `num_candidates` | floor for the relative cutoff |
| `KOSHA_KNN_PROBE_RADIUS_ALPHA` | 1.0 (`d - radius`, unchanged) | posting rank score `max(0, d(q,c) - alpha x radius)`; 0 = raw centroid distance |
| `KOSHA_KNN_BOUND_STOP_ALPHA` | unset (off) | a segment's probe skips a budgeted posting once its k-th guaranteed similarity >= alpha x the posting's similarity ceiling (angle triangle inequality on centroid distance and radius); never more than the fixed budget |

With the default alpha the best posting's score is often clamped to 0, so
the relative cutoff then falls back to `min_probes`; SPANN's own rule is
`KOSHA_KNN_PROBE_RADIUS_ALPHA=0` plus the cutoff.

Cost: about `duration` + 8 minutes per point once the ground truth is
cached (estimate, not measured).

### Experiment inputs (`server_env`, `rustflags`, `bm25_snapshot`, `knn_budget_fraction`)

These inputs serve the research track (#260): one dispatch measures one
flag or build setting against the baseline. All four are optional. Left
empty (the default), the run behaves exactly as before: the same
`docker build` command, the same container env, the same snapshots.
A first step, "Validate experiment inputs", checks them before anything
is provisioned, so a rejected value costs nothing and fails the run
with an `::error::` line.

| input | default | effect | accepted values |
|---|---|---|---|
| `server_env` | empty | extra `-e KEY=VALUE` flags on the kosha container, after the standard bench env (so a repeated key, e.g. `KOSHA_CACHE_MAX_BYTES`, overrides it) | space-separated pairs, each `^[A-Z0-9_]+=[A-Za-z0-9._:/,-]*$` |
| `rustflags` | empty | `--build-arg KOSHA_RUSTFLAGS=...` on the server image build; the Dockerfile exports it as `RUSTFLAGS` for `cargo chef cook` and `cargo build` only when non-empty | letters, digits, space and `= + , . _ : -`, e.g. `-C target-cpu=x86-64-v3` |
| `bm25_snapshot` | `msmarco-<bm25_docs>` | snapshot the BM25 suite restores into `msmarco-<bm25_docs>`; one that does not exist yet is built offline first (see below) | `^[a-z0-9.-]+$`, restore mode only |
| `knn_budget_fraction` | empty (server default 0.05) | `-e KOSHA_KNN_BUDGET_FRACTION=<x>` | a number with 0 < x <= 1; do not also set the key in `server_env` |

No value may contain quotes, `$`, backticks, `;`, `|`, `&`, redirections
or globs. The validated values are spliced into the remote scripts, so
these rules are what keeps them safe.

Examples:

```
# kNN probe budget sweep point
knn_budget_fraction=0.02 suite=vector

# a new code path behind an env flag, plus a cache size override
server_env="KOSHA_SOME_FLAG=1 KOSHA_SEGMENT_CACHE_MAX_BYTES=34359738368"

# x86-64-v3 build of the server (AVX2/FMA/BMI2)
rustflags="-C target-cpu=x86-64-v3"

# BM25 on a snapshot built with a new segment format flag
bm25_snapshot=msmarco-10m-fmtv2 server_env="KOSHA_SOME_FORMAT_FLAG=2" suite=bm25
```

Details and caveats:

- **New snapshots.** When `bm25_snapshot` names a snapshot that does not
  exist, the BM25 step runs `SNAPSHOT_NAME=<name> build_and_import.sh
  msmarco-<bm25_docs>` (first 1M docs when `bm25_docs=1m`) and then
  restores it. The same rule as the vector side applies: an existing
  snapshot is never overwritten, and the default name keeps its old
  behaviour (the 1M snapshot is still built on its first run, without
  `server_env`). `server_env` reaches the offline builder container
  through `build_and_import.sh`'s `BUILD_DOCKER_ENV` (validated again
  there), for both a new `bm25_snapshot` and a `build_vector_snapshot`
  build, so a format flag applies when the segments are written as
  well as when they are read. `rustflags` applies only to the server
  image. The offline builder is built without it.
- **Kept VM.** The local cache is reused when the snapshot identity
  matches (see above), whatever `server_env` says. That is right for
  flags that change only how segments are read or queried. A flag that
  changes what the server writes to its local cache needs a VM that
  has not cached that snapshot yet. Run the previous dispatch with
  `keep_vm` off, or use a different snapshot name.
- **Rebuilds.** A new `rustflags` value invalidates the dependency cook
  layer, so that run does a full release build.
- **Records.** `meta.json` in the S3 run archive also records
  `bm25_snapshot`, `server_env_args` (the normalized `-e` flags,
  including `knn_budget_fraction`) and `rustflags`. When any of them is
  set, the job summary prints an `experiment:` line under the heading.
- **Input count.** `workflow_dispatch` now has 17 inputs. GitHub's limit
  is 25 (it used to be 10; this workflow already had 13 before these
  four and dispatches fine).
  Further experiment knobs go in `server_env` rather than new inputs.

### Distance kernels and x86-64-v3 (#382, experimental)

For a kernel-only check on a v3-capable x86 host, run the same benchmark
with the default target and with v3 codegen:

```bash
cargo bench --locked -p kosha-vector-spfresh --bench distance_kernels
RUSTFLAGS="-C target-cpu=x86-64-v3" cargo bench --locked -p kosha-vector-spfresh --bench distance_kernels
```

Compare the scalar reference, the current 8-lane dot, and the wider
dots on the same synthetic f32 posting. Compare LUT and popcount on the
same RaBitQ posting; query preparation is timed separately. These
numbers do not include the cost of the real posting scan.

Use one revision that contains the experiment and the zero-copy posting
scan (#435). The old `research/382-bench-integration` ref predates the
scan. Keep the host type, 10M snapshots, cache policy, query order, QPS,
and duration fixed within each comparison. Use `mode=restore`,
`warm_gate=queries`, `return_fields=ids`, and `run_recall_check=true`;
compare the warm result-cache-off phases after the warm gate reports ready.

| Comparison | `suite` | Vector snapshot | `rustflags` | `server_env` |
|---|---|---|---|---|
| A: baseline | both | `msmarco-10m-vec` (f32) | empty | `KOSHA_VECTOR_DOT_LANES=8` |
| B: codegen only | both | same f32 | `-C target-cpu=x86-64-v3` | `KOSHA_VECTOR_DOT_LANES=8` |
| C: wider dot | both | same f32 | same v3 | `KOSHA_VECTOR_DOT_LANES=16` (or `32`/`64`) |
| D: RaBitQ LUT | vector | dedicated RaBitQ | same v3 | `KOSHA_VECTOR_RABITQ_ESTIMATOR=lut` |
| E: RaBitQ popcount | vector | same RaBitQ | same v3 | `KOSHA_VECTOR_RABITQ_ESTIMATOR=popcount` |

Compare B to A for BM25 and kNN codegen, C to B for widening, and E to D
for the estimator. Only run C if the kernel bench on the target host
shows a wider dot beating 8 lanes. For D and E, use the same snapshot
built with `KOSHA_VECTOR_QUANTIZATION=rabitq`; changing the estimator on
an f32 snapshot does not exercise either RaBitQ kernel.

Before comparing, check `meta.json` for the inputs and
`zgrep 'vector kernels (#382' server/full.log.gz` for the effective
lanes and estimator and for `x86_features=baseline` (A) or
`x86_features=popcnt,avx2,fma,bmi2` (B–E). A sets the default lane count
explicitly so it also emits this line.

Use `knn_log_stats.py` on each `vector/server/knn_warm_nocache.log.gz`
for `score_ms` and the read/decode/dot split. Judge the 2× `score_ms`
target using the best f32 candidate vs A, not the isolated dot benchmark.
Use `topk_parity.py` on the matching f32 request logs to check ranked IDs
and scores within float tolerance. For E vs D use recall@10 against the same ground truth:
4-bit query rounding changes approximate scores, not the on-disk format.
Record recall@10 against warm p50/p99 latency, local bytes, S3 bytes,
and GETs/query; report the warm BM25 p50/p90/p99 side effect separately.

### Docid reordering and blocks decoded (#385, experimental)

`KOSHA_DOC_REORDER=bp` makes `kosha-build-segments` (and compaction)
write each segment's documents in recursive-graph-bisection order. The
segment format is unchanged and results are identical (ties break on
`_id`); postings get smaller and block-max pruning should skip more.
Default `off`. `kosha-build-segments --doc-reorder off|bp` overrides the
env var. To compare, build both orders offline under new snapshot names
(the harness's `bm25_snapshot` + `server_env`, which reaches the builder
through `BUILD_DOCKER_ENV`): one with no `server_env`, one with
`server_env=KOSHA_DOC_REORDER=bp`. The existing `msmarco-10m` snapshot was
loaded through the server, so it is not the like-for-like "off" baseline.

`GET /stats` (admin) reports `scoring_blocks_decoded`, a process-wide
count of posting blocks varint-decoded. `run_phases.sh` passes the admin
key to `query_bench.py --stats-api-key`, which reads it before and after
each phase and writes `scoring_blocks_decoded` and
`scoring_blocks_decoded_per_request` to the phase JSON (absent against an
older server). It is only per-query while nothing else queries the server.

`run_phases.sh` also passes `--raw-ids`, so every `<phase>.requests.jsonl.gz`
record carries the response's ranked `ids` and `scores`. Reordering must not
change results; check it per query across the two snapshots' runs of the
same phase:

    topk_parity.py off/warm_or_nocache.requests.jsonl.gz bp/warm_or_nocache.requests.jsonl.gz

It exits 0 only when at least one query was compared and every query
answered in both logs returned the same ids in the same order (scores within
`--score-rtol`, default 1e-6); a log recorded without `--raw-ids` is an
error, not a pass.

Both snapshots must be built by a ref that has the #385 builder *and* the
harness's `bm25_snapshot` / `server_env` inputs. A builder without #385
ignores `KOSHA_DOC_REORDER` and would write an ingest-order snapshot under
the `bp` name, which later runs then reuse. The builder prints
`doc reorder: bp` and a non-zero `(reorder Xs)` per segment when it really
reorders; check the build log for both before trusting a `bp` snapshot.

Compaction reorders only when `KOSHA_DOC_REORDER_COMPACTION_MAX_DOCS` is
also set (default 0: never), because it buffers the merged output in
memory; the bench runs with compaction off.

## Engine internals in every result

p50/p90/p99 say how fast a phase was, not why. Each phase summary
(`<phase>.json`) also carries an `internals` block, built by
`bench_internals.py` from data the run already collects, and the
benchmark workflow prints it under the latency table
(`internals_report.py <results-dir>`):

- **Where the time goes:** the server's per-request stage timings
  (`--server-timings`, needs `KOSHA_DASHBOARD=on`): queue, admit, hydrate,
  bm25_stats, score and materialize, each with p50/p90/p99/mean and its
  share of mean server time, plus time no stage covers (`other_ms`) and
  time outside the engine (client latency minus server total).
- **Tail vs median:** mean stage times for the slowest 1% of requests next
  to the p45-p55 band, so a p99 reads as "more hydrate" or "more score".
- **Work counters:** `/v1/stats` deltas across the phase
  (`--stats-api-key`): scoring blocks decoded and posting / doc-store reads,
  per request and per returned hit. Process-wide, so only per-query when
  nothing else queries the server during the phase.
- **I/O and cache:** S3 rounds and MB per request, fully-cached and
  result-cache rates.
- **Resources:** CPU cores and RSS of the `kosha-server` process, and host
  CPU, sampled once a second from `/proc` (`--server-process`,
  `--server-pid`; a no-op off Linux).

All of it is reporting only: anything missing is left out, never failing a
run. `--no-internals` drops the block. For per-request detail use
`--raw-out` and `analyze_tail.py` (below).

## Logs and tail analysis

Every CI benchmark run (`.github/workflows/benchmark.yml`) archives its
server logs and per-request records to
`s3://decoverai-bench-corpus-cache/runs/<run_id>/`, including failed runs:

- `<suite>/<phase>.json`: summary percentiles (also in the job artifact)
- `<suite>/<phase>.requests.jsonl.gz`: one record per request (query
  index, term count, client send lag, latency, hits, and the server's own
  phase timings, from `query_bench.py --raw-out --server-timings` with
  `KOSHA_DASHBOARD=on`)
- `<suite>/server/warmup.log.gz`, `<suite>/server/<phase>.log.gz`: the
  server log for each stage, including the `search timing:` line per request
- `server/full.log.gz`, `meta.json` (commit, inputs)

A VM kept after a failed run has its container log rescued to
`runs/rescued-<timestamp>/` before the next run recreates the container.

For kNN cost counters per query (`knn_postings`, `knn_segments`,
`knn_centroids`, `knn_plan_ms`, opens, hydration and S3 bytes) and the
posting-scan split of `score_ms` (`knn_read_ms`, `knn_decode_ms`,
`knn_dot_ms`, each summed across scoring threads, and
`knn_postings_cached`, #435; `knn_rank_ms`, the time segments spent
ranking their own centroids, which is zero when `knn_plan` handed them
their ranking, #436), summarize a phase's server log:

```bash
python3 knn_log_stats.py vector/server/knn_warm_nocache.log.gz
python3 knn_log_stats.py vector/server/knn_cold_nocache.log.gz --skip-first-s 180
```

To see where a phase's tail comes from:

```bash
aws s3 cp s3://decoverai-bench-corpus-cache/runs/<run_id>/bm25/warm_or_nocache.requests.jsonl.gz .
python3 analyze_tail.py warm_or_nocache.requests.jsonl.gz --queries queries.txt
```

It splits client latency into server time vs time outside the engine,
breaks the p99+ requests down by server phase, tells query-intrinsic cost
(same queries slow every time) from contention (slow once, fast on
repeats), and shows the tail by minute and by requests in flight.

## Offline representation recall (`repr_recall_sim.py`, #383)

`repr_recall_sim.py` asks, with no server and no ANN index, how much
recall@k survives when the shortlist is picked with a smaller
representation (a d-dim prefix, int8 codes, or sign bits) and then
rescored with the full vector. It is brute force over an in-memory sample
and is **not** a bound on Kosha's recall in either direction: it has no
IVF probing, and by default R is one global shortlist, while Kosha
rescores `k * KOSHA_VECTOR_RESCORE_FACTOR` candidates per segment.
`--segments N` splits the sample into N shards and takes R per shard,
which is closer to the engine; the mapping to the factor is still not
measured. It prints bytes per vector for the search representation and
"total x", which adds the f32 rescore vector in `doc_vectors.bin`
(capped at 2x while that store is f32). It reads the same `emb-*.f32` /
`queries_emb.f32` files as `recall_check.py`, or `.npy`:

```bash
python3 repr_recall_sim.py --docs /data/corpus/emb-00000.f32 \
    --docs /data/corpus/emb-00001.f32 --docs-limit 200000 \
    --queries /data/queries_emb.f32 --dim 1024 \
    --truncate 512,256,128 --shortlist 10,40,100,200 \
    --segments 1,4 --out sim.json
```

`fetch_te3s_dbpedia.py` (needs `pyarrow` and `huggingface_hub`) writes
the `text-embedding-3-small` sample used in the doc: 1000 held-out
DBpedia documents as queries (seed 383) and the other 99,000 as the
corpus. Those are doc-to-doc neighbours, not queries over passages.

Memory is roughly 5x the f32 sample (a copy per representation), so
about 4 GB at 100k x 1024. Results and caveats are in
`docs/research/383-matryoshka-binary.md`.
