# Conformance suite (BM25 + filters)

A small end-to-end correctness check for Kosha. It loads a deterministic
synthetic corpus into a real server, takes the namespace through its write
lifecycle, and at every step checks each query's answer **exactly** against
an in-memory reference model. It runs in about 15 seconds locally and on every PR in CI
(`.github/workflows/conformance.yml`).

This complements the existing benchmarks rather than replacing them:
`scripts/bench/` measures speed and recall at scale and compares against
OpenSearch, and `cargo test` checks crates in isolation. Neither one runs the
server through flush, compaction and restart while checking every answer
against ground truth.

## Run it

```bash
# Local: the suite starts (and stops) a server binary itself.
cargo build --release -p kosha-server
python3 scripts/conformance/run.py --spawn target/release/kosha-server

# Full stack (MinIO + Postgres), including a cold restart — what CI runs:
docker compose -f docker-compose.yml \
  -f scripts/conformance/docker-compose.conformance.yml up --build -d
python3 scripts/conformance/run.py --host http://localhost:8080 \
  --api-key sk-kosha-dev \
  --restart-cmd scripts/conformance/compose_restart_cold.sh
```

Python 3.9+ standard library only; no dependencies.

Useful flags: `--seed N` (a different corpus and query set), `--docs N`
(corpus size, default 6000), `--out report.json`, `--verbose` (list every
known-bug check), `--strict` (also fail when a known bug *didn't* trip).

> `--spawn` without Postgres keeps manifests in memory, so a restart would
> forget the namespace; the `restarted` checkpoint is skipped unless you pass
> `--database-url` (or set `DATABASE_URL`).

## What it checks

**Checkpoints** (the same queries run at each one):

| Checkpoint | What happened before it |
| --- | --- |
| `buffered` | Docs indexed, not flushed. Nothing may be visible yet. |
| `one-segment` | First flush. |
| `many-segments` | Five more flushes. BM25 statistics are namespace-level (summed over every segment), so scores must not depend on which segment a doc landed in; `bm25_params.stats: "segment"` queries check the per-segment debug switch. |
| `upserts` | Already-flushed ids re-indexed (segment rewrite path). Buffered ids replaced before their first flush. |
| `deletes` | Delete-by-query, a delete issued while matching docs are still buffered (they are flushed and deleted too), and deleted ids re-indexed. |
| `compacted` | Tiered compaction via `POST /v1/admin/compact-namespace`. |
| `gc` | Segment GC (`POST /v1/admin/gc`, zero grace): the dry run must list exactly the segments compaction merged away, the real run must delete them, and every answer must stay exact. The `restarted` checkpoint after it then hydrates from what GC left in S3. |
| `restarted` | Server restarted. With the compose script, the local cache is wiped first, so every segment hydrates from S3. |

**Per query**, each a separate check `<checkpoint>:<query id>:<aspect>`:

- `status`: HTTP 200.
- `total`: the exact `total_hits` (queries are sent with `exact_total_hits: true`).
- `ids`: the page, in order. Score ties break by `_id`.
- `scores`: each hit's BM25 score within 1e-6 of the model's.
- `fields`: the stored fields, which catches stale versions after upserts.

Each checkpoint also checks the namespace's segment and document counts, and
walks three result sets page by page (`from` paging, and `search_after` on
`_id` and on `keyword, _id`). Every walk must return each hit exactly once,
in order.

**Query set** (`queries.py`, about 57 queries):

- BM25 single terms in the common, mid-frequency and rare bands.
- AND/OR combinations, including a term that matches nothing.
- Tokenizer edge cases: punctuation, case, interior hyphens and apostrophes, non-ASCII.
- Custom `bm25_params`.
- Every filter clause, alone and combined with text.
- Sorting on keyword, date, numeric, `_id` and `_score` fields.
- Deep pages.

## The reference model

`oracle.py` is brute force and models Kosha's documented semantics:

- namespace-level BM25 statistics, `N` / `df` / `avgdl` summed over every segment (tombstoned docs included), and per-segment statistics for `bm25_params.stats: "segment"`
- one inverted index shared by all Text fields
- AND by default
- the write path's buffer → segment → rewrite → tombstone → compact behavior

The module docstring lists each rule and the engine code it mirrors.

The suite only works if the server's segment layout is predictable.
`KOSHA_MAX_CONCURRENT_FLUSH_WRITERS=1` and a large `KOSHA_FLUSH_THRESHOLD`
make one flush produce exactly one segment, and `KOSHA_FLUSH_MAX_AGE_MS=0`
turns off the idle-buffer flush timer so buffered docs stay invisible until an
explicit flush. `KOSHA_COMPACTION_INTERVAL_MS=0` turns off the background
compaction scheduler (#242), which would otherwise merge the suite's segments
mid-run. `--spawn` and the compose overlay set all four. The `stats` check fails loudly if the layout ever diverges from the
model.

Where Kosha's current behavior looks like a bug rather than a design choice,
the model encodes the correct (Elasticsearch-compatible) answer. Those spots
are marked `# ES-correct`.

## Known bugs: `expected_failures.json`

Checks that fail because of a known engine bug are listed there, mapped to a
short description of the bug and where it lives. They are reported as
`xfail` and don't fail the run.

When you fix one of these bugs, delete its entry. Then run with `--strict` to
confirm that none of its checks still fail.

Some bugs only trip for certain segment layouts. For example, OR pruning only
loses docs when a segment holds one query term but not the other. So a listed
check that passes (`xpass`) only fails the run under `--strict`.

## Extending

- **New query:** add it to `queries.build()` with a stable id. If Kosha gets
  it wrong because of a known bug, add a glob to `expected_failures.json`.
- **New write-path step:** add it to `run()` in `run.py` and mirror it in
  `Oracle`.
- **Next pass:** kNN/hybrid (recall@k against brute force), aggregations
  (#215), and fault injection (deleting a `doc_store.offsets` sidecar from
  MinIO to reproduce #179/#184).

The suite uses `--admin-key` (default `sk-conformance-admin`) for compaction.
Spawned-server mode configures the supplied credential; the compose overlay uses
`sk-kosha-admin-dev` (pass it with `--admin-key` as above);
when targeting another server, supply its configured admin key explicitly.
The oracle models mark-deleted + append upserts and flush-before-delete semantics.

For a spawned S3-backed server, `--cold-restart --database-url ...` clears only
its temporary local cache before checking both deleted and compacted snapshots.
This verifies that referenced deletion bitmaps and segment data survive remotely.

## OpenSearch / Elasticsearch DSL matrix: `dsl/`

`dsl/run.py` reuses this suite's corpus, reference model and server helpers to
check the OpenSearch / Elasticsearch DSL cases in `dsl/cases.yaml` through
kosha-client (Python), and writes the compatibility matrix in
[`docs/compat/opensearch.md`](../../docs/compat/opensearch.md). CI runs it on the
same stack after this suite and fails when the matrix is stale. See
[`dsl/README.md`](dsl/README.md).
