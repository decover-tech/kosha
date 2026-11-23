# Short local soak with no deletes (2026-11-22)

This run repeats [`2026-11-22-local.md`](2026-11-22-local.md) with
`--delete-frac 0`. Delete-by-query flushes the namespace's buffer and
publishes it (`Indexer::delete_by_query` in `crates/kosha-write/src/lib.rs`,
`delete_in` in `crates/kosha-server/src/main.rs`). In the first run, deletes
therefore made some inserts visible to `eventual` reads before the idle flush
did. Without deletes, only the server's idle flush and size flush publish
documents.

- **What:** `scripts/soak/soak.py --smoke --duration 180 --flush-every 0
  --compact-every 0 --visible-every 2 --delete-frac 0
  --run-id local-20261122-nodelete`. The driver neither flushes nor compacts,
  and sends no deletes. Inserts are 70% of write requests and re-upserts 30%.
- **Commit:** `e174058` server code (main, the PR #426 merge). The run was
  built from a #352 docs branch that changed no file under `crates/` since
  `e174058`, so the server is the same code as the first run. `cargo build --release -p kosha-server --no-default-features`.
- **Date:** 2026-11-22.
- **Setup:** one standalone server (no S3, local WAL, in-memory control
  plane) on an Apple M5 laptop, with the driver on the same machine. Every
  server setting was left at its default: idle flush
  `KOSHA_FLUSH_MAX_AGE_MS` 1000 ms, checked every 250 ms, and size flush
  `KOSHA_FLUSH_THRESHOLD` 1000 documents. Writes and queries went to the
  same pod, so this does not cover the cross-pod path (manifest poll).
- **Full report:** [`2026-11-22-local-no-delete.json`](2026-11-22-local-no-delete.json).

## Write-to-visible (an acknowledged insert, polled with an `eventual` search)

| samples | never visible | p50 | p99 | max |
| --- | --- | --- | --- | --- |
| 70 | 0 | 0.46 s | 4.642 s | 4.642 s |

With 70 samples, p99 is the slowest sample. The p50 is under the 1 s idle
flush interval for two reasons. First, the timer measures a namespace's age
from its oldest pending document. An insert that arrives while older
documents are already pending is published with them, before the insert is
1 s old. Second, the Zipf-weighted namespaces that get the most writes can
reach the 1000-document size flush first. The flush timings were not
recorded, so this run does not show how the samples split between the two.

## Other results

- Queries (4 to 12 QPS, `no_cache`): BM25 p50 6.43 ms / p99 11.58 ms
  (561 queries), filtered p50 6.25 / p99 10.26 ms (439), kNN (8 dims)
  p50 6.17 / p99 10.46 ms (440).
- 2,013 requests: 0 5xx, 0 connection errors, 0 429s.
- Writes: 16,850 inserted, 6,143 re-upserted, 0 deleted.
- Exact document count matched in all 21 namespaces, with both strong and
  eventual reads. Every gate passed (`SOAK PASSED`).

This is a 3-minute developer-machine run. It is not the 24-hour stage soak,
and its numbers are not production guarantees.
