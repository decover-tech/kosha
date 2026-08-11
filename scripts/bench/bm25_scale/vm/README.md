# Bench VM: namespace snapshot / restore

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
