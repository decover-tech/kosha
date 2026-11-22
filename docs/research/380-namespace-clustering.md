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
  order. Segment format, ids and import path are unchanged. Two additions
  measure the effect: `kosha-cluster-eval` (offline) and a
  `knn_segments=` field on the server's `search timing:` log line.
- **Flags.** Builder: `--cluster-layout input|namespace` (default `input`,
  which keeps today's behaviour; a local diff against `origin/main`'s
  builder on a 50k-doc build differed only in footer `created_at`),
  `--cluster-method head|tree`,
  `--cluster-head-per-segment`, `--cluster-sample`, `--cluster-iters`,
  `--cluster-seed`, `--spill-dir`. Bench: `BUILD_ARGS` in
  `build_and_import.sh` and the `build_segments_args` input in
  `benchmark.yml`. Both are empty by default. No server flag: the query path
  is not changed.
- **Metric.** At matched recall@10 on the 10M MSMarco vector corpus:
  `knn_postings` and `knn_segments` per query, warm p50/p99, and in a cold run
  GETs and bytes per query. The baseline snapshot is today's layout,
  `msmarco-10m-vec`, or the sidecar rebuild with the same builder.
- **Success bar (from #380).** At least 5x fewer segments and postings
  touched at equal recall, and a cold path that serves without a full
  preload.

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
  that probed at least one posting.
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
of 5,000 documents. Queries are 200 corpus vectors plus noise. Truth is
brute-force recall@10.

The search is the server's global-budget algorithm, re-run offline. The
"budget" is the pooled cutoff rank. Real MSMarco embeddings are not blobs,
so treat these numbers as a check of the mechanism, not a prediction.

Matched recall: the cheapest swept budget with recall@10 ≥ 0.95.

| corpus | layout | budget | recall@10 | postings/query | segments/query |
|---|---|---|---|---|---|
| 100 blobs (2,000 each) | input (today) | 50 | 0.962 | 50.0 | 38.9 |
| 100 blobs | **head** | 25 | 1.000 | 25.0 | **2.5** |
| 100 blobs | tree | 200 | 0.964 | 200.0 | 28.4 |
| 1,000 blobs (200 each) | input (today) | 800 | 0.961 | 800.0 | 40.0 |
| 1,000 blobs | **head** | 300 | 0.962 | **300.0** | 39.7 |
| 1,000 blobs | tree | 500 | 0.954 | 500.0 | 40.0 |

At the server's default 5% floor:

| corpus | layout | recall@10 | postings | segments |
|---|---|---|---|---|
| 100 blobs | input | 1.000 | 119 | 40 |
| 100 blobs | head | 1.000 | 117 | 14.5 |
| 1,000 blobs | input | 0.790 | 143 | 39.5 |
| 1,000 blobs | head | 0.877 | 127 | 36.0 |

How to read this:

- **Tight, large clusters (100 blobs).** The head layout meets the segment
  bar by a wide margin (15x fewer segments) and halves postings.
- **Small, diffuse clusters (1,000 blobs).** The head layout needs 2.7x fewer
  postings at matched recall, but still touches almost every segment. Two
  things cause this:
  - **Lower-bound ties.** The radius-aware bound is `max(0, d - r)`. With
    today's layout, 137 postings in 39 segments tie at 0 for every query.
    With the head layout, 26.5 postings in 16 segments tie. Those tied
    postings are always probed.
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
$B/kosha-cluster-eval --embeddings-dir /tmp/c --dim 384 \
  --layout input=/tmp/s/input --layout head=/tmp/s/head --fractions 0.05
```

## Bench plan (WF-2)

1. **Build the clustered snapshot.** Run `build_vector_snapshot=true`,
   `vector_snapshot=msmarco-10m-vec-nsclust-head16`, and
   `build_segments_args=--cluster-layout namespace`. This needs the
   `build_segments_args` input from this PR. Measure the vector suite on it
   with a `knn_budget_fraction` sweep, and run `run_recall_check=true`.
2. **Baseline.** Run the same sweep on a snapshot from the same builder
   without the flag (`msmarco-10m-vec-sidecar`), so the only difference is
   the layout.
3. **Compare.** At matched recall@10, compare `knn_postings` and
   `knn_segments` from the `search timing:` log lines, plus warm p50/p99.
4. **Cold run.** Do a `warm_gate=preload`-style cold phase on both, or a
   `reset_cache.sh` cold pass, for GETs and bytes per query.

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
