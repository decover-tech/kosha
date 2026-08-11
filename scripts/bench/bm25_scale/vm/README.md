# Bench VM scripts

Two independent tool sets that run on the terraform bench VM
(`infra: terraform/dev-machines/aws/bench-kosha`, m7i.8xlarge): the
corpus snapshot/restore pair, and the round 7-9 end-to-end pipelines.

## Namespace snapshot / restore

A loaded bench namespace is exactly two things: its segment objects in
the per-round (terraform-destroyed) bench bucket, and the control-plane
rows in postgres pointing at them. These scripts snapshot that pair into
the persistent corpus-cache bucket and restore it onto any future bench
stack — so each corpus is loaded once, ever.

- `snapshot_namespace.sh <ns> [name]` — segment-prefix sync into
  `s3://decoverai-bench-corpus-cache/snapshots/<name>/` + gzipped
  `pg_dump`. Run after a load completes (phases don't mutate the
  namespace, so post-round is fine too).
- `restore_namespace.sh <ns> [name]` — inverse: sync segments into the
  current round's bench bucket, load the dump, restart the server; the
  boot-restore picks the namespace up and warmup (if configured) makes
  the pod query-ready.

Economics: the 10M vector load runs ~6h at the measured ~485 docs/sec
sustained (flush-build bound, see #126); a restore is minutes. Engine-fix
validation rounds become restore + warmup + phases (~90 min end to end).

Caveat: the pg dump is full-database — restore on a stack whose bench
bucket holds only this snapshot's segments, or expect other namespaces'
manifests to dangle (their queries 503 on hydration; the restored
namespace is unaffected).

## Pipeline scripts

End-to-end pipeline scripts for the tpuf-comparable benchmark rounds
(RESULTS.md). Each is launched detached on the VM (`nohup ./<script> >
<log> 2>&1 & disown`) and emits grep-able stage markers
(`LOAD-AND-COMPACT-COMPLETE`, `WARMUP-READY in Ns`,
`COLD-PHASE-COMPLETE`, `PIPELINE-COMPLETE`, `FLAGS-MISSING*`, …) that a
driver polls for.

Shared assumptions:
- Corpus caches (fetched once ever, see `../fetch_msmarco.py
  --s3-cache` / `../fetch_msmarco_embeddings.py`):
  - text: `s3://decoverai-bench-corpus-cache/msmarco-10m/`
  - embeddings: `s3://decoverai-bench-corpus-cache/msmarco-10m-emb/`
- Instance-role S3 auth; docker; `~/venv311` (python3.11) with
  `requests` (+ `pyarrow numpy huggingface_hub` for fetch scripts);
  repo rsynced to `~/kosha`.
- Every script preflights `--help | grep` for each flag it relies on
  before doing anything expensive (round-5/-7 argparse lesson: a
  missing flag must fail in seconds, not after a 40-minute load).

| script | round(s) | what it measures |
|---|---|---|
| `pipeline_bm25.sh` | 7/8 | BM25 OR-mode: S3-cache load → full compaction (#117/#122, doc-loss guard) → warmup-gated cold reset (#116/#123) → cold + warm cache-off phases |
| `pipeline_vector_smoke.sh` | vec smoke | 1M-doc kNN shakedown: embeddings-cache seed → aligned vector load → segment-layout check → short kNN phase. Caught issue #126 (query-path HNSW builds) before the 10M round could waste hours on it |
| `pipeline_vector.sh` | 9 | 10M Vector Perf: 500k-doc flush threshold (~20 large segments — deliberately NO compaction: the vector corpus is ~260GB on disk and capped merging would rewrite it for hours) → warmup incl. vector files (#129) → cold kNN cache-off + warm kNN cache-on (#128) |

Gotchas encoded in these scripts, learned the expensive way:
- Cold reset = stop container + `rm -rf` the data dir + restart, then
  gate the phase on `/readyz` (the production-honest protocol).
- `pkill -f <script>` from an ssh command self-matches the remote
  shell; kill by pid or use bracketed patterns (`"round[9]"`).
- Loader/bench stdout is block-buffered under nohup — poll `/stats`
  for live progress, not the log.
- Vector loads need `--batch-size ~1000` (vectors are ~12KB JSON per
  doc) and `--timeout 900` (the batch that trips a 500k flush waits for
  that flush's SPFresh build inside the request).
