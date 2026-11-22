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

### Warm gate (`warm_gate`: `queries` (default) or `preload`)

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
- `label:ENV=V,ENV=V;label2:...`, over these knobs only:
  `KOSHA_KNN_BUDGET_FRACTION`, `KOSHA_KNN_BUDGET_MAX_CAP`,
  `KOSHA_KNN_RELATIVE_CUTOFF_EPS`, `KOSHA_KNN_RELATIVE_MIN_PROBES`,
  `KOSHA_KNN_PROBE_RADIUS_ALPHA`. Include an `f0.05` point (or pass the
  report `--baseline`) for the comparison column.

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
