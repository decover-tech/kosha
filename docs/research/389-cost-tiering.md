# #389: cost-driven tiering and a scan-resistant disk cache

Status: **prototype, experimental, default off.** Nothing here changes behaviour
or on-disk formats unless `KOSHA_CACHE_POLICY`, `KOSHA_CACHE_ACCOUNTING` or
`KOSHA_CACHE_TRACE_PATH` is set. No bench numbers yet; the bench plan is at the
end ("Bench plan").

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
  `KOSHA_CACHE_ACCOUNTING=legacy|exact`: unset means legacy; `exact` fixes the
  legacy double count below, for a fair legacy control.
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
- **Also observed; fixed only behind a flag (default path unchanged).** `hydrate_files` passes
  every path in a batch to `note_external_write`, including the ones that
  `fetch_one` found already local (`cached: true`). This happens in
  `hydrate_from_s3_budgeted` (`main.rs`), and the full-segment path hands it the
  whole listing. `note_external_write` calls `account(key, 0, len)`, which adds
  `len` to `total_bytes` again for a file that is already counted. A partly present
  segment therefore inflates the running total, and the cache evicts more than
  `KOSHA_CACHE_MAX_BYTES` requires. In policy mode the cache keeps its own size for
  each key, so a re-noted file is counted once (test:
  `policy_accounting_does_not_double_count_a_re_noted_file`).
  `KOSHA_CACHE_ACCOUNTING=exact` applies the same rule to the legacy path
  (test: `exact_accounting_stops_the_legacy_double_count`). This matters for the
  experiment: at one `KOSHA_CACHE_MAX_BYTES`, a default legacy arm has *less*
  effective capacity than a policy arm, so a policy "win" could come from the
  accounting alone. The legacy control arms therefore run with `exact`. Making
  `exact` the default changes default eviction and is its own PR.
- Access rates already exist. `warm.rs::AccessTracker` keeps decayed hits and bytes
  per (namespace, segment, `FileClass`). Placement is still all or nothing for each
  namespace and class.

## Design (a): read signal + policies

**Read hints.** After each successful search, `AppState::record_cache_reads`
(`crates/kosha-server/src/cache_reads.rs`) builds the following hints. It sits next
to `record_search_access`, on both search handlers.

| what the search read | hint |
| --- | --- |
| scoring set of every manifest segment | `Class { dir: "{tenant}/{ns}/{seg}", class: Scoring }` |
| posting shards the query terms hash to | `File { dir, file: "postings-XX.bin" }` per segment, the same set `ensure_query_posting_blobs` makes local; wildcard or fuzzy falls back to `Class(Postings)` |
| vector index (kNN) | `Class(Vectors)` |
| doc stores that served the page | `Class(DocStore)` on `phases.page_segments` |

`dir` is the segment directory relative to the data dir, from
`Manifest::segment_dir`: `{tenant}/{ns}/{segment}` on the server (#262). The cache
groups each key by the same directory: everything before the segment-level file
(`policy.rs::split_key`; the `.spans` component for span entries, else the last
component). The first version of this prototype grouped by the first two key
components, which matches only the pre-#262 `{ns}/{segment}` layout; on the real
layout every hint was dropped and both policies degraded to FIFO. Regression tests:
`policies_keep_re_read_entries_under_the_tenant_scoped_layout`,
`span_entries_under_the_tenant_layout_take_file_hints`, and the server-level
`cache_reads::tests::tenant_scoped_search_hints_reach_cached_entries`, which runs a
tenant-scoped search through `AppState` with SIEVE on and checks the hint reaches
the cached footer. All three fail on the old grouping.

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
{"v":1,"t_ms":1763700000123,"ns":"default/msmarco-10m","segs":["default/msmarco-10m/seg-a"],
 "classes":["scoring","postings"],"postings":["postings-3f.bin"],"pages":["default/msmarco-10m/seg-a"]}
```

The existing logs cannot drive a file-level replay. `search timing:` lines carry
counts (`hydrate_files`, `page_gets`, `postings_gets`, `vector_gets`, `s3_bytes`)
but not which segment or file was read. They are still useful for pricing what
happened: `cache_sim.py logcost --log server/*.log.gz` turns the archived runs into
GETs, S3 GB and S3 $ per 1k queries for today's system.

`cache_sim.py replay --trace T --sizes S` expands each line into files, using a
sizes listing of `<segment dir>/<file>` paths at any depth. Use the **S3 segment
listing** (`aws s3 ls --recursive s3://<bucket>/segments/`, then
`--strip-prefix segments/`; benchmark.yml archives it as `server/s3-sizes.tsv.gz`
when `cache_trace` is on). A listing of the local data dir is not enough: the bench
runs with span promotion off, so doc stores, posting shards and `vector.idx` reads
are local only as `<file>.spans/<range>` files, and a local listing would model the
scoring files alone. Span entries are ignored by default; `--span-files sum` sizes a
span-only file by its cached spans (a lower bound). The replay reports, per class,
how many (segment, class) references resolved to a file, and exits 2 when more than
5% of segment references have no sizes, so a layout or prefix mismatch fails loudly
instead of printing zero rows (the first version silently dropped every
tenant-layout path). It then:

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
  has been fully cached once. With a cache smaller than the working set that never
  happens, so bounded runs use `warm_gate=timed` instead (see the bench plan); the
  workflow refuses a non-default bound with any other gate.
- `KOSHA_CACHE_MAX_BYTES` bounds the whole data dir, not one namespace. Bounded runs
  therefore start from an empty local store holding only the suite namespace
  (`timed` mode wipes it), one suite per dispatch.
- The default query order is a fixed loop over 1,677 queries, a pattern adversarial
  to FIFO/LRU and favourable to SIEVE/S3-FIFO. The plan uses `query_order=zipf`
  (seeded skewed popularity) as the primary workload, with `shuffle` as a check.
- Prices differ per deployment. Every price in the simulator is a CLI flag.

## Bench plan (WF-2)

Nothing below has run. Every input exists in `benchmark.yml` on this branch
(`cache_max_bytes`, `cache_policy`, `cache_accounting`, `cache_trace`,
`warm_gate=timed`, `warm_fill_s`, `phase_repeats`, `query_order`); defaults reproduce
today's runs exactly.

**Protocol for every arm** (control and bounded alike, so they are comparable):
`suite=bm25`, `mode=restore`, `bm25_docs=10m`, `qps=8`, `warm_gate=timed`,
`query_order=zipf`, `warm_fill_s=900`, `duration=600`, `phase_repeats=2`,
`keep_vm=true` (last run of the session `false`). `timed` empties the local store,
so each arm starts cold with only the suite namespace local; the 900 s fill is
unmeasured; then 2 x 600 s measured no-result-cache phases (about 9,600 requests,
about 96 samples above p99). Misses count in the measured latency. Every arm reports
p50/p99 (with bootstrap CIs from `cache_sim.py p99ci`), S3 GETs and bytes per query
(`cache_sim.py logcost` on the phase logs), `footprint.tsv` and `evictions.tsv`.
All legacy arms use `cache_accounting=exact`.

| # | arm | cache_max_bytes | policy | trace | purpose |
| --- | --- | --- | --- | --- | --- |
| 1 | control A | default (600 GiB) | legacy | on | unbounded reference, trace + S3 sizes for the replay, `W` = final `footprint.tsv` value |
| 2 | bounded | 0.5 W | legacy (exact) | off | FIFO under the bound |
| 3 | bounded | 0.5 W | sieve | off | |
| 4 | bounded | 0.5 W | s3fifo | off | |
| 5 | control B | default | legacy | off | same-session repeat of 1: noise band, and trace overhead (1 vs 5) |
| 6 | bounded | 0.25 W | legacy (exact) | off | only if 3 or 4 beats 2 |
| 7 | bounded | 0.25 W | best of 3/4 | off | only if 3 or 4 beats 2 |

Between 1 and 2, replay run 1's trace offline (`cache_sim.py replay --sizes
server/s3-sizes.tsv.gz --strip-prefix segments/ --capacity-frac 0.25,0.5`). If no
policy beats FIFO by at least 5 points of byte hit ratio at 0.5 W in the replay, stop
after run 2 and report that instead of spending runs 3-7. Run order 2/3/4 can be
shuffled; each starts cold, so order does not carry state.

**Noise band, defined before running.** For each arm, pool its two measured repeats.
Noise band = the larger of (a) |p99(control A) - p99(control B)| and (b) the half
width of the 95% bootstrap CI of the control p99 difference. An arm is *within
noise* of the control when the 95% CI of its p99 difference to pooled controls A+B
lies below +noise band. A policy *beats* legacy when the CI of (policy - legacy) p99
excludes 0 on the negative side, or GETs/query drop by more than the spread between
its own two repeats.

**Success (from #389).** A policy arm at <= 0.5 W within noise of the controls
(same p99 at half the footprint, by the real `footprint.tsv`, not just the bound),
or lower total $/query (S3 GETs from `logcost` plus local GB-h from `footprint.tsv`)
at equal p99. **Failure** is any of: no policy arm within noise at 0.5 W; the policy
arms not distinguishable from legacy (exact); or the replay predicting a win the
bench does not show (then the hint granularity is the suspect).

**Vector (conditional, separate session, fresh VM).** Only if BM25 shows a win:
`suite=vector`, same protocol but `warm_fill_s=3600` (the vector working set is far
larger; check `footprint.tsv` has flattened before the measured phases), runs 1, 2
and the BM25 winner at 0.5 W, plus control B.

**Budget (estimated, not measured).** A BM25 arm is about 15 min fill + 20 min
measured + 15-20 min provision/restore, so about 50-55 VM minutes; runs 1-5 about
4.5 h, 6-7 another 2 h. Vector arms about 2 h each.
