# #389: cost-driven tiering and a scan-resistant disk cache

Status: **prototype, experimental, default off.** Nothing here changes behaviour
or on-disk formats unless `KOSHA_CACHE_POLICY` or `KOSHA_CACHE_TRACE_PATH` is set.
No bench numbers yet; the planned runs are listed at the end.

## Experiment spec

- **Hypothesis.** The NVMe cache evicts in insertion order because it never sees a
  read. Giving it a cheap read signal and a scan-resistant policy (SIEVE or S3-FIFO)
  keeps the re-read working set local when the cache is smaller than the namespace.
  Then the same p99 holds at a smaller local footprint, and fewer S3 GETs are made
  per query.
- **Change.** (a) Per-search read hints from the server to `kosha_cache::Cache`,
  plus SIEVE and S3-FIFO eviction in `crates/kosha-cache/src/policy.rs`. (b) An
  offline trace-replay simulator (`scripts/bench/cache_sim/cache_sim.py`) that prices
  FIFO, LRU, SIEVE, S3-FIFO and static five-minute-rule placement, using a per-search
  trace the server can write.
- **Flags.** `KOSHA_CACHE_POLICY=legacy|sieve|s3fifo`: unset means legacy, which is
  today's behaviour. It only has an effect when `KOSHA_CACHE_MAX_BYTES` is set.
  `KOSHA_CACHE_TRACE_PATH=<file>`: unset means no trace.
- **Metrics.** Warm p50/p90/p99, S3 GETs per query and S3 bytes per query (from
  `search timing:` lines, via `cache_sim.py logcost`), the local footprint, and
  simulated hit ratio and $ per 1k queries at each cache size.
- **Success bar (from the issue).** Same p99 at a local footprint at least 2x
  smaller, or lower total $ per query at equal p99. The baselines are 10M MSMarco at
  8 QPS: BM25 p99 170 ms, kNN p99 227 ms.

## What Kosha does today

- `Cache` (`crates/kosha-cache/src/lib.rs`) keeps a recency `VecDeque`, touched on
  `put`, `put_bytes` and `note_external_write`, and on `get`. `Cache::get` has no
  non-test callers: searches open segment files directly from the data dir. So
  under `KOSHA_CACHE_MAX_BYTES` the victim is always the file written longest ago.
  That is FIFO by write time, whatever is being read.
- **Also observed, not fixed here (default path unchanged).** `hydrate_files` passes
  every path in a batch to `note_external_write`, including the ones that
  `fetch_one` found already local (`cached: true`). This happens in
  `hydrate_from_s3_budgeted` (`main.rs`), and the full-segment path hands it the
  whole listing. `note_external_write` calls `account(key, 0, len)`, which adds
  `len` to `total_bytes` again for a file that is already counted. A partly present
  segment therefore inflates the running total, and the cache evicts more than
  `KOSHA_CACHE_MAX_BYTES` requires. In policy mode the cache keeps its own size for
  each key, so a re-noted file is counted once (test:
  `policy_accounting_does_not_double_count_a_re_noted_file`). The legacy fix should
  be its own PR, because it changes default eviction.
- Access rates already exist. `warm.rs::AccessTracker` keeps decayed hits and bytes
  per (namespace, segment, `FileClass`). Placement is still all or nothing for each
  namespace and class.

## Design (a): read signal + policies

**Read hints.** After each successful search, `AppState::record_cache_reads`
(`crates/kosha-server/src/cache_reads.rs`) builds the following hints. It sits next
to `record_search_access`, on both search handlers.

| what the search read | hint |
| --- | --- |
| scoring set of every manifest segment | `Class { dir: "{ns}/{seg}", class: Scoring }` |
| posting shards the query terms hash to | `File { dir, file: "postings-XX.bin" }` per segment, the same set `ensure_query_posting_blobs` makes local; wildcard or fuzzy falls back to `Class(Postings)` |
| vector index (kNN) | `Class(Vectors)` |
| doc stores that served the page | `Class(DocStore)` on `phases.page_segments` |

A hint costs O(1). It bumps a counter on the segment's group, not on every file.
Each cache entry remembers how much of its group's counters it has already
consumed. The eviction scan works out "reads since I last looked" lazily, and only
for the entries it visits. Span-cache entries (`postings-03.bin.spans/...`) belong
to the shard they cache. Posting shards are named exactly. With class granularity,
every BM25 query would mark all 256 shards of every segment as read.

**Not counting the inserting search.** `begin_cache_read()` stamps a logical clock
before hydration. A file inserted by a search, because it was missing, records the
clock at insertion. That search's own hint, stamped earlier, does not count as a
re-read. This matters for scan resistance: without it, every one-touch file would
look re-read once. The rule is approximate under concurrency, because counters are
aggregated per group (see `Entry::take_reads`).

**Policies** (`policy.rs`, byte-weighted, pins respected):

- **SIEVE.** One insertion-ordered queue and a hand that moves from oldest to newest.
  A visited entry has its bit cleared and keeps its place. An unvisited entry is
  evicted.
- **S3-FIFO.** The small queue holds 10% of bytes, and an entry moves to main after
  more than one re-read. Main uses FIFO reinsertion with a 2-bit frequency. The ghost
  queue holds as many keys as main, with a floor of 64, and a ghost hit inserts
  straight into main.
- Both evict *before* admitting, as the papers do: the key whose write triggered an
  eviction is never its own victim. Pinned keys (hydration batches) are skipped. When
  everything is held, eviction stops instead of spinning, the same fallback as
  legacy.
- Files found on disk at startup are adopted in mtime order, unread. S3-FIFO puts
  them in main.
- External removals (`evict`, `evict_dir_unless_pinned`, `evict_prefix`, `clear`)
  also drop the keys from the policy. This is O(tracked keys), on paths that are
  already rare.

**Default path.** With the policy unset, `Cache::with_options(.., policy: None)`
builds no policy state. `begin_read` returns 0, `note_reads` is a no-op,
`enforce_limit` scans `recency` exactly as before, and `record_cache_reads` returns
before computing anything unless the trace is on.
`legacy_policy_evicts_re_read_entries_under_a_one_touch_scan` pins this behaviour.

**Costs of the policy path (not measured on the bench).** There is one mutex
acquisition per search, plus O(segments x (2 + shards)) hash lookups. The
posting-shard computation repeats `posting_blob_files_for_query`, which reads
footers from the manifest or disk, as the warm path already does. `recency` is
still maintained in policy mode, for the accounting helpers. Its O(n) `touch` on
write is unchanged from today.

## Design (b): trace + simulator

`KOSHA_CACHE_TRACE_PATH` writes one JSON line per search. The format is v1, and its
full spec is in the `cache_sim.py` docstring:

```json
{"v":1,"t_ms":1763700000123,"ns":"msmarco","segs":["msmarco/seg-a"],
 "classes":["scoring","postings"],"postings":["postings-3f.bin"],"pages":["msmarco/seg-a"]}
```

The existing logs cannot drive a file-level replay. `search timing:` lines carry
counts (`hydrate_files`, `page_gets`, `postings_gets`, `vector_gets`, `s3_bytes`)
but not which segment or file was read. They are still useful for pricing what
happened: `cache_sim.py logcost --log server/*.log.gz` turns the archived runs into
GETs, S3 GB and S3 $ per 1k queries for today's system.

`cache_sim.py replay --trace T --sizes S` expands each line into files, using a
sizes listing (`find <data_dir> -type f -printf '%P\t%s\n'`). It then:

- Simulates byte-bounded FIFO (today), LRU, SIEVE and S3-FIFO at each capacity. The
  default capacities are 10, 25, 50 and 100% of the trace's working set.
- Builds static placement from the first `--train-frac` of the trace, and evaluates
  it on the rest:
  - `fivemin`: keep a file locally iff `rate x GET$ >= size x local$/s`. The memory
    tier is enabled only when `--nvme-read-cost > 0`.
  - `static`: the greedy best static set by GET $ saved per byte, at each capacity.
- Reports the hit ratio, byte hit ratio, GETs per query, S3 GB, misses per query
  (p50 and p99, a proxy for critical-path S3 requests), and S3 $, local $ and total
  $ per 1k queries.

The simulator models whole files. Ranged span reads are priced as whole-file
misses, so GETs and bytes are an upper bound for `ranged` read modes.

**Five-minute-rule numbers** under the default prices, which are list-price
assumptions: GET $0.0004/1k, instance NVMe $0.16/GB-month. The break-even re-read
interval scales with 1/size:

| file size | keep locally if re-read at least every |
| --- | --- |
| 1 MiB | ~6,700 s (1.9 h) |
| 4 MiB | ~1,700 s (28 min) |
| 64 MiB | ~105 s |
| 1 GiB | ~7 s |

On gp3 ($0.08/GB-month), every interval doubles. The implication is that small
scaffolding and posting files are worth keeping even if read rarely. A multi-GB doc
store or vector index is worth keeping only when read every few seconds, which a
steady 8 QPS kNN load does. Cold namespaces' big files are where S3-only reads (R2
and R8 in #260) pay off.

**Synthetic smoke run, for the tool only, not Kosha data.** `synth` with 2,400
queries, 200 segments per namespace, a one-touch scan every 4th query and a 51 GB
working set, at 25% capacity. Run on Apple arm64 (shared build machine), 43 s wall:

| policy | hit ratio | GETs/query |
| --- | --- | --- |
| fifo | 0.89 | 92 |
| lru | 0.95 | 43 |
| sieve | 0.98 | 16 |
| s3fifo | 0.97 | 29 |

This shows the simulator separates the policies on a scan-heavy pattern. It says
nothing about MSMarco until a real trace is replayed.

## Cost-driven placement in the server: next step, not in this PR

The decayed rates in `warm.rs` are enough to apply the same rule online. For each
(segment, class), the expected GET $/s (`hits/s x files x GET$`) can be compared
with `bytes x local$/s`. The result could rank or prune the warm planner's
candidates (`warm::plan`), and decide which classes are admitted to the disk cache
at all. That second use would pair with S3-direct reads for classes that do not
pay their rent (#260 R2/R8). It waits on the replay showing that placement beats
the best policy at equal footprint by enough to matter.

## Risks and limits

- Read hints are coarse for the scoring set and the vector index: the whole class
  of each segment is marked read. kNN reads only some posting ranges of
  `vector.idx` (span entries), and the cache cannot tell which.
- A segment-level hint covers every manifest segment, including segments that a
  bloom filter or term check pruned. Those segments' scoring files look read.
- The clock rule is approximate when searches overlap with hydration of the same
  files.
- The bench warm gate (`warm_gate.py`, `WARM_GATE=queries`) waits until every query
  has been fully cached once, and then 3 in a row. With a cache smaller than the
  working set and a poor policy, the gate may not pass before
  `WARM_GATE_TIMEOUT_S`. That outcome is itself a result, but it costs VM time.
- Prices differ per deployment. Every price in the simulator is a CLI flag.
