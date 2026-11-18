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
| `many-segments` | Five more flushes. BM25 statistics are per segment, so scores now depend on layout. |
| `upserts` | Already-flushed ids re-indexed (segment rewrite path). Buffered ids replaced before their first flush. |
| `deletes` | Delete-by-query, a delete issued while matching docs are still buffered (they are flushed and deleted too), and deleted ids re-indexed. |
| `compacted` | Tiered compaction via `POST /v1/admin/compact-namespace`. |
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

- per-segment BM25 statistics (tombstoned docs included)
- one inverted index shared by all Text fields
- AND by default
- the write path's buffer → segment → rewrite → tombstone → compact behavior

The module docstring lists each rule and the engine code it mirrors.

The suite only works if the server's segment layout is predictable.
`KOSHA_MAX_CONCURRENT_FLUSH_WRITERS=1` and a large `KOSHA_FLUSH_THRESHOLD`
make one flush produce exactly one segment. `--spawn` and the compose overlay
set both. The `stats` check fails loudly if the layout ever diverges from the
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
