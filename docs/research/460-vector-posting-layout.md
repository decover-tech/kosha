# #460 Vector posting layout and sizing for object storage

Status: offline feasibility study, research track (#260). This PR adds a range
replay benchmark; it does not change the writer, query planner, or S3 client.

## TL;DR

- **Layout first, not wider gaps first.** In the synthetic localized workload,
  reordering cuts modeled posting GETs from 4,235 to 200 (21×) and bytes from 108
  to 67 MiB/query. With uniform probes even the 2 MiB bridge is only a 6.3×
  request reduction, and transfers about 1 GiB/query. Neither is a measured Kosha
  recall or latency result.
- **Test 128/256/512/1024 with RaBitQ separately.** At 1,024 dimensions these
  maxima correspond to 17.5/35/70/140 KiB of entries; even 1,024 entries leave
  room to coalesce. Larger postings can reduce centroid work but may also widen
  radii and increase bytes scanned at matched recall.
- **Include a bulk-read control.** Ten million 1,024-dim RaBitQ entries occupy
  1.40 GB before sidecars. This is at least 84 16 MiB GETs if packed into one
  object, not necessarily 130 in Kosha: each segment/column is its own object.

## What Kosha writes and reads today

[`SegmentWriter::encode_vector_column`](../../crates/kosha-segment/src/lib.rs)
uses the same [`ClusterIndexConfig::new`](../../crates/kosha-vector-spfresh/src/lib.rs)
for every entry format (min/target/max 16/64/128). The
[scratch build](../../crates/kosha-vector-spfresh/src/build.rs) bisects until
it reaches the max; it can leave a degenerate, unsplittable leaf larger.
`snapshot` walks active posting slots. `encode_vector_index_v2` emits their
entries in that order, with no explicit proximity ordering at serialization.
Do not assume this is random on real data: the scratch builder traverses its
bisection tree, and slot order may already have locality.

| On-disk entries | Bytes/vector at dimension `d` | Bytes at 1,024 | KiB at 128 entries |
|---|---:|---:|---:|
| f32 (radius-aware version 3 by default) | `4 + 4d` | 4,100 | 512.5 |
| i8 (version 4) | `4 + 4 + d` | 1,032 | 129 |
| RaBitQ (version 5) | `12 + next_power_of_two(max(d, 8)) / 8` | 140 | 17.5 |

`vector.idx` has a 16-byte header and contiguous entries, with no per-posting
payload header. RaBitQ's 12 bytes are the document ID, residual norm, and inner
product scalar; the packed code is relative to the posting centroid.
`vector.offsets` has a 16-byte header and, for versions 3–5, a `4d + 16` byte
record for each posting (f32 centroid, u64 offset, u32 length, f32 radius).
At the roughly 113k centroids in the [#384 protocol](https://github.com/decover-tech/kosha/issues/384)
this alone is about 443 MiB at 1,024 dims. Quantization does not shrink those
records or the full-precision vectors in `doc_vectors.bin`.

In [`hydrate_vector_postings`](../../crates/kosha-server/src/main.rs), a
segment's `vector.idx` misses go through
[`fetch_file_spans`](../../crates/kosha-server/src/posting_spans.rs):
check span coverage, coalesce missing intervals from the same object with
`KOSHA_POSTING_SPAN_COALESCE_GAP_BYTES` (64 KiB by default), and batch-fetch
under a 16 MiB cap even for contiguous runs. A single posting is never split,
including one above the cap. The
[`S3Storage::read_ranges`](../../crates/kosha-server/src/s3_storage.rs)
batch is already concurrent, subject to its semaphore and hydration budgets.
That is not a single query-wide batch: metadata must arrive before planning,
segments fetch in their own scoring tasks, filtered/refill probes can do
additional reads, and quantized rescoring needs exact vectors. Non-shed posting
miss failures can fall back to a whole-file fetch. The experiment should isolate
posting GETs from these other costs, then report the whole query too.

## Offline range replay (run locally)

```sh
python3 scripts/bench/bm25_scale/vector_range_replay.py
python3 scripts/bench/bm25_scale/vector_range_replay.py --trace trace.json
```

The replay holds selected posting identities and bytes fixed across policies.
Current order uses actual sidecar offsets in trace mode. The candidate
sorts centroids on the axis from the first centroid to its farthest centroid,
then repacks whole postings (stable ties). This is a cheap, deterministic
proximity heuristic, not a guarantee in 1,024 dimensions. The simulator applies
the same greedy merge and no-split cap as the current engine, per object. At
30 ms fixed plus 20 ms/MiB, bridging under 1.5 MiB costs less than another
request in a **serial** comparison (the simulation permits equality). This is
not a latency model for concurrent requests: use the caps as controls and
measure queueing, byte pressure, and tail latency on storage.

Synthetic cases: 20 queries each, seed 460, 200 segment/column objects, 565
postings of 88 RaBitQ entries per object (9.944M vectors), 5,700 wanted
postings/query (67.0 MiB). Writer order is shuffled. The positive control probes
a contiguous window in one-dimensional centroid rank per object; the negative
control samples uniformly. Results summarize mean **posting-only** GETs and
transferred MiB per query (cold spans, metadata already available):

| Policy | Localized GETs | Localized MiB | Uniform GETs | Uniform MiB |
|---|---:|---:|---:|---:|
| Current (64 KiB / 16 MiB) | 4,235.4 | 107.6 | 4,224.9 | 107.3 |
| Proximity, current gap/cap | 200.0 | 67.0 | 4,223.6 | 107.8 |
| Current + bridge / 0.5 MiB | 1,921.7 | 552.4 | 1,918.6 | 549.7 |
| Current + bridge / 1 MiB | 1,163.2 | 840.3 | 1,160.8 | 837.8 |
| Current + bridge / 2 MiB | 665.0 | 1,047.9 | 665.5 | 1,047.7 |
| Proximity + bridge / 0.5 MiB | 200.0 | 67.0 | 1,919.2 | 549.5 |
| Proximity + bridge / 1 MiB | 200.0 | 67.0 | 1,160.6 | 834.6 |
| Proximity + bridge / 2 MiB | 200.0 | 67.0 | 666.5 | 1,046.1 |

The target 0.5–2 MiB is a cap sweep, not a minimum read or prefetch alignment:
in the localized case useful ranges are already contiguous and only ≈0.33 MiB
per object. Reading more would not save a GET. With scattered probes the wide
bridge nearly transfers the entire payload; low GET count alone is misleading.

For a real replay, record the posting ordinals that reach
`SegmentReader::prefetch_vector_postings` for fixed query IDs, preserving
segment generation and vector column. Use matching sidecars. The manifest has
the shape `{"segments":{"segA/col0":"snapshot/segA/vector.offsets"},"queries":[{"segA/col0":[1,7,8]}]}`
(paths relative to the manifest, ordinals zero-based in the original sidecar).
One query entry represents one known selection per object; run the initial
budget batch separately from later refills if they occur. The archived bench
stat fields give totals, not these identities. No real posting trace or
benchmark corpus was available in this checkout.

## Follow-up experiment and rollout gate

1. **Replay real recorded probes before changing the writer.** Compare current
   order with proximity at the same gap/cap, then sweep gap/cap at both orders
   as above. Report GET and transferred-byte distributions, amplification
   (transferred/useful), and per-touched-object ranges, checking the issue's
   1–3-range goal separately from the per-query target. If one projection has
   poor locality, try ordering by recursive centroid bisection with deterministic
   ties. Freeze the budget, ranking (including radii), and selection first: the
   layout-only experiment is not a recall experiment.
2. **Rebuild four RaBitQ snapshots with max 128/256/512/1024.** Provide a
   benchmark-only override to `encode_vector_column` before both build and
   LIRE seed/repair; keep min/target/refinement fixed to isolate the max, and
   record actual posting size distribution and radius quantiles. There is no
   existing max-posting-size knob; don't use the writer-version or quantization
   flag as a proxy. The table gives ideal counts for one 10M-vector namespace
   with every posting full (ignoring per-segment rounding and oversized leaves):

   | Max entries | RaBitQ posting at max (KiB) | Ideal posting count |
   |---:|---:|---:|
   | 128 | 17.5 | 78,125 |
   | 256 | 35 | 39,063 |
   | 512 | 70 | 19,532 |
   | 1,024 | 140 | 9,766 |

   At 5% probed and equal posting sizes, fewer centroids still translate to
   roughly the same 500k vectors scanned. Larger radii can make ranking by
   `max(0, distance − alpha × radius)` less selective. Follow [#384](https://github.com/decover-tech/kosha/issues/384):
   10M MSMarco, 8 QPS, top 10, at least 1,000 fixed queries and exact ground
   truth. Sweep probe budgets to produce recall@10 vs scan bytes, centroid count,
   and `knn_plan_ms` curves; hold quantization, rescoring, and segment boundaries
   constant. [`cluster_eval`](../../crates/kosha-build-segments/src/bin/cluster_eval.rs)
   can compare offline posting budgets across built snapshots;
   [`recall_check.py`](../../scripts/bench/bm25_scale/recall_check.py) and
   [`knn_log_stats.py`](../../scripts/bench/bm25_scale/knn_log_stats.py) cover
   the server-side recall and timing check.
3. **Compare the best arm with bulk and cold controls.** Version 5 uses 140 B
   per entry at 1,024 dimensions, but 268 B at 1,536 (RaBitQ pads to 2,048).
   For 10M vectors those are 1.40 GB and 2.68 GB: at minimum 84 or 160
   16 MiB GETs in single packed objects. In the synthetic 200-object
   layout each object is smaller than 16 MiB, so bulk takes 200 posting GETs
   and transfers ~1.30 GiB at 1,024 dims, excluding sidecars and rerank.
   For real snapshots sum `ceil(object_bytes / 16 MiB)` over objects. Test
   whole-code hydration followed by the **same** fixed probe and rerank, so
   bulk fetch is a transport control. Separately test scoring all codes
   (using their centroids) plus exact rerank; this changes compute and recall.
4. **Run [`warm_gate=cold`](../../scripts/bench/bm25_scale/README.md) on the
   best layout vs the RaBitQ/current-layout control**, and pair it with the
   `queries` warm gate. Cold is a first-touch phase after a local wipe, not
   forced cache bypass on every query. Repeat from a wipe and separate first
   metadata opens from the subsequent phase. Report total GETs and bytes
   (including metadata, rerank, fallback, and hedge attempts), posting GETs
   and bytes separately, and cold and warm p50/p99. Keep the proposed bar:
   **≥10× fewer cold kNN GETs at equal recall@10, with warm p50 no worse**.
   Don't claim it from the synthetic posting-only speedup.

If replay supports a prototype, order the finalized snapshots immediately
before serialization, after refinement/repair and centroid normalization, and
regenerate both files together. [Compaction](../../crates/kosha-write/src/lib.rs)
passes seed postings into the same segment writer; apply the order both there
and on flush/rebuild, not only at initial ingest. Any bijective reordering of
complete posting/sidecar pairs appears representable in v5 as-is: validate
reader compatibility, cache generation isolation, and equal-score tie behavior
(ordinals change) with byte-level and query-parity checks. Mixed old/new layout
objects should remain readable. Changing membership, code encoding, or the
coupling between centroids and entries is not a layout-only experiment.

For the I/O prototype, keep a vector-specific range policy (the existing gap is
shared with other reads). Submit the known posting misses in one logical wave
with request/byte limits; keep a later refill or exact rerank as a separately
accounted dependency. Add first-byte hedging only as a separate disabled-by-default
arm: if the primary has no first byte by the measured tail threshold, start at
most one extra GET under the same budgets, cancel the loser, cache only the
winning complete range, and count both attempts. Compare against no hedge at
equal concurrency; neither a hedge nor a retry is free under per-request billing.

Prior work motivates the sweep, not its result: Durner et al. (VLDB'23, §2)
model S3 startup cost, bulk reads and hedging; Lance (2025) emphasizes locality
for coalescing small reads; Airphant (2021) batches independent reads; SPANN
(NeurIPS'21) sizes postings for SSDs rather than object storage. The 30 ms and
20 ms/MiB assumptions are from #460, not fitted Kosha measurements.
