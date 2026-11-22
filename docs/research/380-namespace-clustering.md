# #380 Namespace-level clustering and cluster-aligned segments

Status: research prototype (offline builder only). Not merged. Part of the
research track in #260.

## Experiment spec

- **Hypothesis.** Kosha clusters vectors per segment, and the offline builder
  cuts segments in input order. Every segment is therefore a random sample of
  the space, and every segment has postings near every query. If segments are
  built from whole namespace-level clusters instead, a query's nearest
  postings sit in a few segments. Postings and segments touched at equal
  recall should then drop, and a cold query should need only those few
  segments.
- **Change.** `kosha-build-segments --cluster-layout namespace` trains a
  namespace head (two-level spherical k-means, 16 clusters per segment by
  default) on a sample of the corpus. It routes every document to a head
  cluster and packs whole head clusters into segments in nearest-neighbour
  order. Segment format, ids and import path are unchanged. Additions that
  measure the effect: `kosha-cluster-eval` (offline), and `knn_segments=`,
  `knn_centroids=` and `knn_plan_ms=` on the server's `search timing:` log
  line.
- **Flags.** Builder: `--cluster-layout input|namespace` (default `input`,
  which keeps today's behaviour; a local diff against `origin/main`'s
  builder on a 50k-doc build differed only in footer `created_at`),
  `--cluster-method head|tree`,
  `--cluster-head-per-segment`, `--cluster-sample`, `--cluster-iters`,
  `--cluster-seed`, `--spill-dir`. Bench: `BUILD_ARGS` in
  `build_and_import.sh` and the `build_segments_args` input in
  `benchmark.yml` (both empty by default), and `warm_gate=cold` (a
  no-preload cold phase; the default stays `queries`). No server flag: the
  query path only gains log counters.
- **Metric.** At matched recall@10 on the 10M MSMarco vector corpus, per
  query from the `search timing:` lines: `knn_postings`, `knn_segments`,
  and, for the cost the layout does not remove, `opens_cold` /
  `opens_cached`, `open_total_ms`, `knn_centroids` and `knn_plan_ms`. Warm
  p50/p99. In a `warm_gate=cold` run, `hydrate_files`, `hydrate_mb`,
  `s3_bytes`, `vector_gets` and `postings_gets` per query. The baseline is a
  snapshot built from the same commit without the flag (see the bench plan),
  not an older snapshot.
- **Success bar (from #380).** At least 5x fewer segments and postings
  touched at equal recall, and a cold path that serves without a full
  preload.
- **What phase 0 can and cannot show.** `knn_segments` counts segments that
  probed at least one posting. `knn_plan` still opens every segment and
  ranks every centroid, so opens, the centroid scan and cold
  `vector.offsets` / `inverted.idx` / `doc_store.offsets` hydration still
  scale with the segment count. A drop in `knn_segments` is therefore an
  upper bound on what phase 1 (segment pruning before open) could save. It
  does not meet the "segments touched" bar of #380 on its own. For the same
  reason a cold query in phase 0 serves without a full preload of
  `vector.idx`, but it still hydrates the small per-segment files of every
  segment; "serves without touching most segments" needs phase 1.

## What Kosha does today

- **Build.** `kosha-build-segments` and the server's flush cut documents into
  segments in arrival order. `SegmentWriter` then clusters each segment's
  vectors on its own (`crates/kosha-vector-spfresh`, balanced 2-means into
  postings of 16-128 vectors). On the 10M bench that gives about 200 segments
  and about 114k posting centroids.
- **Query.** `knn_plan` (`crates/kosha-query/src/lib.rs`) opens every
  segment. It computes the radius-aware lower bound to every posting centroid
  (`knn_centroid_distances`, `crates/kosha-segment/src/lib.rs`) and pools all
  of them. The cutoff is the `budget`-th smallest bound, where
  `budget = max(num_candidates, 5% of centroids)`, about 5,700 on the bench.
  Each segment then probes only the postings inside the cutoff
  (`knn_search_within_filtered_fetching`).
- **Why that is expensive.** Neighbouring documents arrive at random times,
  so they are spread over all 200 segments. The postings a query needs are
  spread the same way. Each segment's ~570 postings tile the whole space
  coarsely. The query therefore needs a few coarse postings in every segment,
  not a few fine postings in one place.

## Design

### Head

- The namespace keeps a centroid head of `K` clusters. In this prototype
  `K = 16 x segments`, about 3,200 at 10M. The head is used to place
  documents. It is not used to answer queries yet.
- Two levels: `T ≈ √K` top centroids, then sub-centroids inside each top in
  proportion to its size. Routing a document costs `T + K/T` dot products,
  about 113 at `K = 3,200`.
- The query-side head is the one the issue sizes at `K ≈ 15-20·√N`, about
  50-64k at 10M (Faiss Eq. 18). In this prototype it comes for free: it is
  the union of the per-segment SPFresh posting centroids (about 114k). Those
  postings are now fine-grained namespace clusters, because each segment
  covers one region. `knn_plan` already ranks them namespace-wide, which is
  what a SPANN head does (SPANN §3.2). The only difference is that the head
  is scanned flat rather than through a graph. This is why the existing
  format and query path can serve cluster-aligned segments unchanged.

### Build (this PR)

1. **Sample.** Read `--cluster-sample` embeddings (default 131,072), evenly
   strided over the corpus, by seeking into the `emb-*.f32` shards. Each one
   is normalized to unit length.
2. **Train.** Fit the two-level spherical k-means head.
3. **Order and pack.** Chain the tops by nearest neighbour. Within each top,
   chain its subs, starting from the sub nearest the previous top's last
   sub. Walk this sequence and assign each head cluster, whole, to the
   segment its midpoint (by estimated size) falls in. Sizes vary by about one
   head cluster, roughly ±1/16 of a segment. The local runs below saw 4.4k to
   5.4k documents per 5k-document segment.
4. **Route and spill.** Stream the corpus once, with the same text and
   embedding lockstep and id check as today. Route each document and append
   it to that segment's spill file (`--spill-dir`, default
   `<out>/<ns>.cluster-spill`).
5. **Build segments.** Hand each spill file to the existing worker pool as
   one segment, using the same `SegmentWriter`, ids `<ns>-offline-NNNNNN` and
   output layout. Each spill file is deleted once it has been read.
6. **Write the head.** Save it to `<out>/<ns>.cluster-head.json`. The file
   sits outside the namespace directory, so it is not uploaded or imported.

An alternative `--cluster-method tree` builds a balanced hyperplane tree
with exactly equal leaves. It is kept for comparison. It loses locally
(below), because forcing equal halves cuts dense regions in two.

Costs and requirements:

- **Disk.** Spill needs about text plus f32 vector bytes, about 45 GB at 10M
  and 1024 dimensions.
- **Memory.** Peak memory is the sample (512 MB at 1024 dimensions) plus the
  usual in-flight segments.
- **Time.** Training takes seconds locally. At 10M, routing is about 1.2
  TFLOP single-threaded inside the streaming pass. That is not measured
  here, but expect minutes.

### Write path, compaction and LIRE (#253): not in this PR

- **Flush.** A server flush still produces an input-order segment. To keep
  alignment, the writer would route each document through the stored head.
  Then either (a) buffer per region and flush region-pure segments, or (b)
  flush as today and let compaction re-align.
- **Compaction.** (b) is simpler and matches the immutable-segment model. A
  compaction pass picks segments by head region instead of by size tier. It
  rewrites them as region-pure outputs, bounded by
  `max_merged_segment_bytes`. Unaligned "delta" segments are probed like
  today's segments. Their number is bounded by the flush rate and compaction
  lag.
- **LIRE (SPFresh, SOSP'23).** LIRE is the per-posting split and reassign
  that #253 wires in. At namespace scope it becomes a split of a head
  cluster, and the matching segment split when a segment grows past its byte
  cap. Reassign moves boundary vectors to the neighbouring cluster's segment
  at the next compaction, never in place.
- **Head drift.** The head itself is re-trained rarely, for example when
  compaction finds that a large share of a region's documents route
  elsewhere. Re-training means a full re-layout, which is a rebuild.
- **Hot clusters.** Writes skew toward hot clusters. A hot region's segment
  hits the byte cap first and splits. The size cap keeps segments evictable.

### Deletes

- Tombstones stay per segment (manifest tombstone sets) and are unaffected.
  A delete is applied to whichever segment holds the document. The new
  version of an updated document may route to a different region's segment.
  That is the same as today, where the new version lands in a newer segment.
- Region-pure compaction drops tombstoned rows as it rewrites, as today's
  compaction does.

### Query path

- **Phase 0 (this PR).** Nothing changes. Pooled global ranking already
  skips every posting outside the cutoff. A segment with none inside the
  cutoff probes nothing. The new `knn_segments=` log field counts segments
  that probed at least one posting. `knn_centroids=` (centroid bounds
  ranked) and `knn_plan_ms=` (wall time of `knn_plan`: opens plus centroid
  scan) show the per-segment cost that remains. `knn_plan_ms` runs outside
  the other timed phases, so the log's `total_ms` does not include it.
- **Phase 1 (follow-up, flag-gated).** Per-segment region summaries, either
  the head clusters it owns or its centroid plus radius, in the manifest or
  a small namespace head file. `knn_plan` could then skip segments before
  opening them. Today `knn_plan` opens every segment and scans every
  centroid, so CPU and cold `vector.offsets` hydration still scale with
  segment count. Phase 1 is what turns "fewer segments probed" into "fewer
  segments opened". A cold query would then hydrate the head, a few
  segments' offsets, and their probed postings (ranged reads, #245). It
  would not need a full preload.
- **Phase 2.** Use the head to pick `nprobe` regions directly. The flat scan
  of 114k centroids per query (about 117M MACs at 1024 dimensions) becomes
  `T + K/T` plus the centroids of the selected segments.

### BM25 side effects

Segments become topic-coherent. BM25 scoring is unchanged, because scoring
already uses namespace-level statistics (#246). Per-segment block-max
ceilings (#248) change, which can help or hurt WAND pruning. This should be
measured, not assumed. A snapshot built with `--cluster-layout namespace`
also carries text, so the BM25 suite can run on it.

## Local results (synthetic, offline)

These numbers are from an Apple M5 (10 cores, 16 GB), a release build, and
`kosha-cluster-eval`. The corpus is synthetic: 200,000 vectors of 384
dimensions from `kosha-vector-quant-eval gen`, as Gaussian blobs with
members at about cosine 0.7 from their centre. Each layout has 40 segments
of 5,000 documents. Truth is brute-force recall@10.

Queries are in-distribution: a random corpus vector plus noise. That favours
a clustered layout compared with real MSMarco query-to-passage embeddings,
where a query need not sit inside a passage cluster. Only the MSMarco bench
run can confirm a postings reduction.

The search is the server's global-budget algorithm, re-run offline. The
"budget" is the pooled cutoff rank. Under that cutoff postings/query
equals the budget, so a postings ratio at matched recall is a ratio of
budgets and is only as precise as the budget grid.

### Fine-grid sweep (500 queries)

Budgets swept in steps of 20 (1,000 blobs, 200-1,000) and 5 (100 blobs,
5-80). For recall@10 >= 0.95 the table gives the last budget below the
target and the first at or above it. The true matched budget lies in
between.

| corpus | layout | budget below (recall) | budget reaching (recall) | segments/query at reaching |
|---|---|---|---|---|
| 100 blobs (2,000 each) | input (today) | 45 (0.931) | 50 (0.962) | 38.8 |
| 100 blobs | **head** | 20 (0.902) | 25 (0.998) | **2.4** |
| 1,000 blobs (200 each) | input (today) | 740 (0.948) | 760 (0.951) | 40.0 |
| 1,000 blobs | **head** | 240 (0.945) | 260 (0.951) | 39.5 |

Postings ratio at matched recall (input / head), bounded by the grid:

- **100 blobs:** 1.8x to 2.5x fewer postings. Segments that probed a
  posting drop from about 39 to 1.3-2.4.
- **1,000 blobs:** 2.8x to 3.2x fewer postings at recall 0.95. Read off the
  same sweep at other targets: about 2.9x at 0.96 (860 vs 300) and about
  2.6x at 0.97 (980 vs 380). Segments do not drop (40 vs 39.5).

At the server's default 5% floor (`--fractions 0.05`, same 500 queries):

| corpus | layout | recall@10 | postings | segments |
|---|---|---|---|---|
| 100 blobs | input | 1.000 | 119 | 40.0 |
| 100 blobs | head | 1.000 | 117 | 14.4 |
| 1,000 blobs | input | 0.785 | 143 | 39.5 |
| 1,000 blobs | head | 0.879 | 127 | 35.9 |

An earlier pass of this table used the coarse grid 25, 50, 100, ..., 1,600
and 200 queries. It reported the 1,000-blob result as "2.7x" (800 vs 300).
On that grid the ratio could have been anywhere from about 1.3x to 4x. The
fine grid narrows it to the range above.

How to read this:

- **Tight, large clusters (100 blobs).** The head layout cuts segments that
  probed a posting by more than 15x and roughly halves postings. These are
  not segments opened (see the phase-0 note in the spec).
- **Small, diffuse clusters (1,000 blobs).** The head layout needs about 3x
  fewer postings at matched recall, short of the 5x bar, and still probes
  almost every segment. Two things cause this:
  - **Lower-bound ties.** The radius-aware bound is `max(0, d - r)`. With
    today's layout, about 137 postings in 39 segments tie at 0 for every
    query. With the head layout, about 27 postings in 16 segments tie.
    Those tied postings are always probed.
  - **Global ranking reaches far.** Posting radii are wide, so the ranking
    still pulls in far postings with wide radii from every segment. The
    `segments p99` column in the eval output shows this.
- **Phase 1 is needed for the segment bar.** Ranking by raw centroid
  distance inside a pre-selected set of segments may matter as much as the
  layout. Phase 1 segment pruning, or a tighter bound, is likely needed to
  meet the segment bar on diffuse data.
- **The tree method loses** to `head` on both corpora and is not proposed
  for the bench.

Not measured locally: latency on x86 (AVX2/AVX-512 effects), cold S3 GETs and
bytes, the 1024-dimension MSMarco corpus, and BM25 on the re-laid-out
corpus.

Reproduce:

```sh
B=target/release
$B/kosha-vector-quant-eval gen --out /tmp/c --docs 200000 --dim 384 --clusters 100
$B/kosha-build-segments --shards-dir /tmp/c --embeddings-dir /tmp/c --emb-dim 384 \
  --namespace input --out-dir /tmp/s --docs-per-segment 5000
$B/kosha-build-segments --shards-dir /tmp/c --embeddings-dir /tmp/c --emb-dim 384 \
  --namespace head --out-dir /tmp/s --docs-per-segment 5000 --cluster-layout namespace
$B/kosha-cluster-eval --embeddings-dir /tmp/c --dim 384 --queries 500 \
  --layout input=/tmp/s/input --layout head=/tmp/s/head \
  --budgets "$(seq 5 5 80 | paste -sd, -)"
# 1,000 blobs: gen --clusters 1000, then --budgets "$(seq 200 20 1000 | paste -sd, -)".
# The server's default form: --fractions 0.05
```

## Bench plan (WF-2)

Prerequisites:

- **Combined branch.** Every run sweeps `knn_budget_fraction`, an input that
  exists only in the open harness PR #390. This branch has 14 workflow
  inputs (13 on `main`, plus `build_segments_args`) and a new `warm_gate`
  option, `cold`; it has no `knn_budget_fraction`. #390 has neither of this
  branch's additions. Dispatching this branch with `knn_budget_fraction`
  fails as an unexpected input; dispatching #390's branch loses
  `build_segments_args` and `warm_gate=cold`. Dispatch from a temporary
  branch that merges #390 into this one. A local trial merge of #390's
  head into this branch conflicts in two places only, and both resolve as
  a union of the two sides: the `build_and_import.sh` call of the vector
  suite (keep `BUILD_ARGS`, `BUILDER_SHA` and #390's `BUILD_DOCKER_ENV` on
  one line) and the variable block of `build_and_import.sh` (keep both
  validation blocks). The input list and the container args of the
  single-quoted build step merge cleanly (18 inputs; `$WARMUP_ARGS
  $EXPERIMENT_ARGS`). After the merge, bash -n of every step and the quote
  parity of the single-quoted build step were clean locally; redo both
  checks if either PR changes.
- **Same-commit baseline.** The builder commit of the existing
  `msmarco-10m-vec-sidecar` snapshot is not recorded. The per-segment index
  build changed recently: LIRE build refinement and centroid indexing
  (#253, `4fa1906`, `b64817a`) and vector index version 5 (`b5331c1`). So
  that snapshot cannot be assumed to be a same-builder baseline, and the
  workflow skips the build of an existing snapshot name. Build the baseline
  from the dispatch commit under a new name, for example
  `msmarco-10m-vec-input-<sha7>`, with empty `build_segments_args`. Build
  the clustered snapshot from the same commit, for example
  `msmarco-10m-vec-nsclust-<sha7>`. `build_and_import.sh` now writes
  `build-info.json` (builder commit, source digest, build args, max docs)
  next to the snapshot. The vector suite copies it into the results as
  `vector/snapshot-build-info.json`. Both snapshots' `builder_sha` must
  match before the runs are compared.

Runs:

1. **Build both snapshots** at one commit. The names must be new, because
   an existing name is not rebuilt. `suite=vector`,
   `build_vector_snapshot=true`, `warm_gate=queries`, `duration=300`; once
   with `build_segments_args=--cluster-layout namespace` and once without.
2. **Warm matched-recall sweep** on each snapshot: `warm_gate=queries`,
   `duration=300`, `run_recall_check=true`, `recall_sample=1000` (100 is
   too few to separate layouts near 0.95), and `knn_budget_fraction` in
   0.01, 0.02, 0.05 and 0.1. Use one dispatch per fraction, so each run's
   per-phase server log (`vector/server/knn_warm_nocache.log.gz`) belongs
   to exactly one configuration. Add fractions between the two that
   bracket recall 0.95 on each layout.
3. **Compare** at matched recall@10, interpolated between the bracketing
   fractions. Per query, mean and p99 of `knn_postings`, `knn_segments`,
   `knn_centroids`, `knn_plan_ms`, `opens_cold` and `open_total_ms`, plus
   warm p50/p99. `scripts/bench/bm25_scale/knn_log_stats.py` summarizes
   them from a phase log.
4. **Cold, no preload**, one run per snapshot at the server's default
   fraction: `warm_gate=cold`, `duration=300`. The server starts after a
   local wipe with no `KOSHA_WARMUP_NAMESPACES`, no usage warming and span
   promotion off. Read `hydrate_files`, `hydrate_mb`, `s3_bytes`,
   `vector_gets` and `postings_gets` per query from
   `vector/server/knn_cold_nocache.log.gz`. Read the first minutes (every
   segment's small files are fetched once) separately from the rest
   (`knn_log_stats.py --skip-first-s`). By construction phase 0 still
   hydrates `vector.offsets` and the other small files of every segment.
   This run measures how much the layout saves in posting reads. It does
   not show that a query can skip segments.
5. **Confirm** the winning fraction with one `duration=1800` warm run.

How to read the result:

- The layout helps if, at matched recall, `knn_postings` drops by 5x or
  more and `knn_segments` falls well below the segment count. A drop in
  `knn_segments` is a phase-0 upper bound on what phase 1 would save. The
  #380 segment bar is met only when opens and `knn_centroids` fall too,
  and that needs phase 1.
- It fails if `knn_postings` drops by less than about 2x at matched recall
  (no better than the diffuse synthetic case), or if recall at a fixed
  fraction falls.

## Risks and open questions

- **The 5x segment bar probably needs phase 1.** The layout alone may not
  meet it on real embeddings, for the tie and radius reasons above.
- **Segment sizes vary by about one head cluster.** A very large head
  cluster would push a segment past the byte cap. At 16 clusters per segment
  this was not seen locally.
- **Build time and disk.** The spill doubles transient disk use (about 45 GB
  at 10M), and routing is single-threaded.
- **Head staleness under writes and compaction** is designed above but not
  built.
