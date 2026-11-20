# kosha-vector-spfresh

An in-memory implementation of the algorithmic core of
[SPFresh](https://arxiv.org/abs/2410.14452) (SOSP '23): a cluster/posting-based
ANN index kept balanced by **LIRE** (**L**ightweight **I**ncremental
**RE**-balancing: Split, Merge and a bounded Reassign, on top of Insert/Delete).

kosha uses it to build every segment's vector index. The crate itself has no
I/O and no dependency on the rest of the workspace; `kosha-segment` turns its
postings into files and `kosha-query` probes them.

## What is wired into kosha

### Segment files (`kosha-segment`)

Each `Vector` field of a segment gets one index (#204): `vector.idx` +
`vector.offsets` for the first column, `vector.<n>.idx` + `vector.<n>.offsets`
for column `n`.

- `vector.idx`: a header (`KVE2` magic, version, dim, count), then every
  posting's entries back to back: `[doc_seq:u32][f32 x dim]`, or
  `[doc_seq:u32][scale:f32][i8 x dim]` for an i8 column (version 4, #210).
- `vector.offsets`: per posting, its centroid (f32), the byte range of its
  entries in `vector.idx` and, from version 3, its radius: the largest cosine
  distance from the centroid to any member.

Vectors and centroids are unit-normalized at write time (version 2 and up), so
scoring is a dot product. A reader keeps `vector.offsets` resident and reads a
posting's entries from `vector.idx` only when a query probes it (with an LRU
cache of decoded postings). Legacy flat `vector.idx` files (no magic) and
indexes whose sidecar is missing are served by exact scan. Nothing builds an
ANN structure when a segment is opened.

### Building an index

- **Flush** clusters the segment's vectors from scratch with
  [`ClusterIndex::build`]: recursive balanced 2-means until every posting holds
  at most `max_posting_size` (128) vectors. Bisection's balance pass leaves far
  members in postings (loose radii), so the build then refines (#253):
  `build_refine_rounds` (2) rounds in which every vector moves to the nearest
  centroid among its posting's `build_refine_candidates` (16) nearest postings
  and centroids are recomputed, followed by [`ClusterIndex::rebalance`] (LIRE
  Split / Merge / Reassign) to restore the 16..128 size bounds. A stray that
  bisection pushed into an unrelated region can be farther from all of its
  posting's neighbours than from some other posting; in the first round a
  vector the candidates cannot be shown to cover (an angular
  triangle-inequality test against the farthest candidate) looks up its own
  nearest posting. On 10k unit-norm 128-d vectors this lifts radius-aware
  recall@10 at a 5% probe budget from 0.44 to 1.00 (median radius 0.87 to
  0.45) for about 23% more build time; at 200k, p90 radius drops from 0.83 to
  0.19 for about 11%.
  `build_refine_rounds = 0` (kosha: `KOSHA_VECTOR_BUILD_REFINE=off`) is the
  plain bisection build, with undersized postings merged away, exactly as
  before. The same build serves `KOSHA_VECTOR_COMPACTION=rebuild` and the
  compaction fallback. Late-interaction `.ivf` clustering keeps the plain
  build.
- **Compaction** (#253) does not re-cluster. Per vector column:
  1. The largest input whose column has at least 1,024 vectors and a clustered
     index has its postings adopted with [`ClusterIndex::from_postings`]:
     centroid and members as stored, members remapped to the merged segment's
     doc_seqs, tombstoned or deleted members dropped, and the centroid
     recomputed for a posting that lost members.
  2. Every other input's postings are folded in with
     [`ClusterIndex::absorb_postings`]: an absorbed posting's centroid picks its
     8 nearest adopted postings, and each member moves to the nearest of them.
     The lookup goes through a coarse index over the adopted centroids
     (`centroid_index.rs`: about `sqrt(P)` cells, the nearest quarter of them
     probed, an exact scan under 256 postings), so absorbing `c` loose vectors
     costs about `c * P / 4` centroid comparisons instead of `c * P`.
     The inputs each partitioned the same space on their own, so keeping their
     postings side by side would split a query's neighbours across overlapping
     postings. Vectors of inputs too small to adopt are absorbed one by one.
  3. [`ClusterIndex::rebalance`] runs LIRE: Split every posting over
     `max_posting_size` (repeatedly, until none is), Merge every posting under
     `min_posting_size` (16) into its nearest neighbour, and after each event a
     Reassign over the touched postings and their `reassign_radius` (8)
     neighbours.

  Postings that nothing touched keep their centroid and members, so the work
  follows the number of changed postings rather than the column size. Entries
  are re-encoded from the exact f32 vectors in `doc_vectors.bin` (or the doc
  store), so an i8 column is requantized from full precision, and radii are
  computed from exact vectors as on flush. The output is the same on-disk
  format. A column is clustered from scratch instead when no input qualifies,
  when fewer than half of its vectors come from seeded postings, or on a
  dimension mismatch. `KOSHA_VECTOR_COMPACTION=rebuild` switches the whole path
  off (default `lire`).

### Querying (`kosha-query`)

- **Global probe budget.** A kNN does not probe a fixed `nprobe` per segment.
  It ranks the centroids of every segment it searches together and hydrates
  the best `num_candidates` postings across all of them, with the budget
  floored at `KOSHA_KNN_BUDGET_FRACTION` (5%) of the namespace's centroids and
  capped by `KOSHA_KNN_BUDGET_MAX_CAP`. Filtered kNN widens it by the filter's
  selectivity, or scores passing documents exactly when few pass (#252).
- **Radius-aware ranking.** Postings are ranked by the triangle-inequality
  lower bound `max(0, distance(query, centroid) - radius)` instead of raw
  centroid distance, so a posting with an unremarkable centroid but a close
  member still gets probed.
- **i8 rescoring.** An i8 column scores with an i8 dot product, then rescores
  its best `k * KOSHA_VECTOR_RESCORE_FACTOR` candidates with the f32 vectors in
  `doc_vectors.bin`.

### Not wired yet

- A namespace- or shard-level centroid index whose postings are shared across
  segments and updated from the WAL (#253 step 2, after the indexer role in
  #254). Today every segment carries its own postings, so the number of
  centroids a query ranks grows with segment count; compaction only reduces it
  by producing fewer, larger segments.
- LIRE on flush: a flush has no seed (every vector is new), so it still
  clusters from scratch; the post-build refinement above is what gives its
  postings LIRE-like radii.
- `insert` / `delete` on a live index. Segments are immutable; deletes are
  document tombstones filtered at query time, and LIRE runs only inside
  compaction.

## Concept to code

| Paper concept | This crate |
|---|---|
| A posting (partition): centroid + assigned vectors | `posting::Posting` |
| NPA (nearest partition assignment), the invariant LIRE maintains | doc comment on `posting::Posting` |
| SPTAG (centroid index for probing) | `centroid_probe.rs`, a flat linear scan (see below); `centroid_index.rs`, a coarse two-level index for batch lookups (absorb, build refinement) |
| Multi-constraint balanced clustering (build + Split's clusterer) | `kmeans::balanced_bisect` |
| LIRE: Insert | `ops::insert::insert` |
| LIRE: Delete (tombstone) | `ops::delete::delete` |
| LIRE: Split | `ops::split::split_posting` |
| LIRE: Merge | `ops::merge::merge_posting` |
| LIRE: Reassign, Eq. 1 / Eq. 2 necessary conditions | `ops::reassign::run_reassign` |
| Bulk Insert of another partition's postings (not in the paper) | `ops::absorb::absorb` |
| Version byte (tombstone + version) | `posting::PostingEntry::{deleted, version}` |
| Split/Reassign convergence (posting count bounded by vector count) | the `postings: Vec<Option<Posting>>` slab's monotonic growth (`ClusterIndex::alloc_slot`), the `MAX_CASCADE_DEPTH` guard in `ops/split.rs`, and `tests/invariants.rs` |
| Local Rebuilder (background job queue) | not implemented (see below) |
| Block Controller (SPDK, append-only disk blocks) | not implemented: postings are persisted by `kosha-segment` |

## Simplifications versus the paper

The paper targets a billion-vector, disk-backed, concurrently updated index.
kosha's postings live in immutable segments of up to a few hundred thousand
vectors, rewritten by compaction, which shapes these choices:

- **In memory, persisted by the caller.** The index exists only while a
  segment is being written; `kosha-segment` serializes `snapshot()` and reads
  postings back with `vector_posting_layout()`. No SPDK, raw block I/O or
  crash recovery here.
- **Synchronous rebalancing, no background job queue.** A Split or Merge
  touches one posting plus at most `reassign_radius` neighbours and runs
  inline. The paper's Local Rebuilder thread pool amortizes cost across an
  index that never stops taking writes; compaction is already a batch job.
- **Flat centroid probing, not SPTAG.** A segment has tens to a few thousand
  postings, so a linear scan plus a partial sort is cheap per lookup. Batches
  of lookups against centroids that do not move (absorbing thousands of loose
  vectors, a build refinement round) use `centroid_index.rs`, a static
  two-level index built per batch; LIRE's per-event lookups still scan,
  because every event moves centroids.
- **`balanced_bisect` stands in for multi-constraint balanced clustering.**
  Farthest-pair seeding, Lloyd iterations, then a pass that moves the larger
  side's farthest members to the smaller side until the size ratio is within
  `max_cluster_size_ratio`. That last pass can leave a far member in a
  posting, which inflates its radius. `build` therefore refines the leaves by
  bounded nearest-centroid reassignment and rebalances them (see "Building an
  index"); with `build_refine_rounds = 0` absorb plus Reassign produces tighter
  postings than a from-scratch build.
- **`deleted: bool` plus an informational `version: u8`, no CAS.** There is
  one writer thread, so nothing races. The version is bumped on every
  tombstone so a concurrent design has somewhere to put real conflict
  detection.
- **i8 scalar quantization, not PQ.** Quantization (#210) happens in
  `kosha-segment` at encode time with `quantize_i8`; the index itself holds
  f32 vectors.

## Configuration defaults

`ClusterIndexConfig::new(dim)`: `target_posting_size: 64`,
`max_posting_size: 128`, `min_posting_size: 16`, `reassign_radius: 8`,
`nprobe: 16`, `build_refine_rounds: 2`, `build_refine_candidates: 16`. These are scaled to kosha segments, not to the paper's
billion-scale defaults (it uses `reassign_radius = 64`). `nprobe` applies only
to this crate's own `search`; kosha-query uses the global budget above.

## Tests and benchmarks

- `tests/compaction_merge.rs`: compaction-shaped merges (one big input plus
  flush-sized updates, four equal inputs, six churn cycles) compared against a
  from-scratch build at the same relative probe budget. Recall@10 must not be
  more than 0.5 points below the build's. Build refinement must tighten the
  median radius and lift radius-aware recall@10 at 5% to 0.95 or more, and
  absorption through the centroid index must place 97%+ of loose vectors in
  their exact nearest posting. The ignored `compaction_cost_report` prints
  build time, median radius and recall with refinement off and on, and
  wall-clock rebuild vs lire times
  (`cargo test --release -p kosha-vector-spfresh --test compaction_merge -- --ignored --nocapture`).
- `tests/churn_ablation.rs`: the paper's Figure 10 ablation on one
  insert/delete/drift sequence.
- `tests/invariants.rs`, `tests/recall.rs`: structural invariants and recall
  floors.
- `benches/build_query_cost.rs`: build, incremental update and query cost,
  (`build_refine`) build time with refinement off and on, and
  (`compaction_vector_index`) compaction's index step, `rebuild` against
  `lire`. Run with
  `cargo bench -p kosha-vector-spfresh --bench build_query_cost -- compaction_vector_index`.
