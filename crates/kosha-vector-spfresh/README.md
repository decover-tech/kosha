# kosha-vector-spfresh

A standalone, in-memory implementation of the algorithmic core of
[SPFresh](https://arxiv.org/abs/2410.14452) (SOSP '23) — a cluster/posting-based
ANN index kept balanced by **LIRE** (**L**ightweight **I**ncremental
**RE**-balancing: Split, Merge, and a bounded Reassign, on top of Insert/Delete).

**This crate is not wired into kosha's on-disk segment format or query path.**
It exists to answer a question `DESIGN.md` already flags as open: kosha's
current per-segment vector index (`crates/kosha-segment/src/lib.rs`) reads a
flat, uncompressed `vector.idx` and rebuilds an `instant-distance` HNSW graph
**from scratch, in memory, on every segment open** — nothing about the graph
is ever persisted, and periodic compaction triggers the same rebuild again.
`DESIGN.md` reserves a "vector.idx v2 — HNSW graph OR IVF-PQ codebook +
posting lists" slot for exactly this tradeoff and calls it unresolved. This
crate is a benchmarked, tested answer to that question — not yet a decision
to ship it.

See `benches/build_query_cost.rs` for a direct comparison against kosha's real
`build_hnsw`/`flat_knn`, and `tests/churn_ablation.rs` for a re-run of the
paper's own Figure 10 ablation using kosha's own numbers.

## Concept → code map

| Paper concept | This crate |
|---|---|
| A posting (partition): centroid + assigned vectors | `posting::Posting` |
| NPA (nearest partition assignment) — the invariant LIRE maintains | doc comment on `posting::Posting` |
| SPTAG (centroid index for probing) | `centroid_probe.rs` — flat linear scan, see below |
| "Multi-constraint balanced clustering" (build + Split's clusterer) | `kmeans::balanced_bisect` |
| LIRE: Insert | `ops::insert::insert` |
| LIRE: Delete (tombstone) | `ops::delete::delete` |
| LIRE: Split | `ops::split::split_posting` |
| LIRE: Merge | `ops::merge::merge_posting` |
| LIRE: Reassign, Eq. 1 / Eq. 2 necessary conditions | `ops::reassign::run_reassign` |
| Version byte (tombstone + version) | `posting::PostingEntry::{deleted, version}` |
| Split→Reassign convergence proof (posting count monotonically bounded above by vector count) | the `postings: Vec<Option<Posting>>` slab's monotonic-growth property (`ClusterIndex::alloc_slot`) + `MAX_CASCADE_DEPTH` guard in `ops/split.rs` + `tests/invariants.rs` (empirical check) |
| Local Rebuilder (background job queue) | not implemented — see below |
| Block Controller (SPDK, append-only disk blocks) | not implemented — see below |

## Simplifications versus the full paper

This is a single-node, single-threaded, in-memory prototype sized for
kosha's actual segment scale (hundreds to ~50,000 vectors per segment), not
the paper's billion-scale, disk-backed target. Every deviation below is
deliberate:

- **In-memory only, no disk backing.** No SPDK, no raw block I/O, no
  crash recovery/WAL/snapshotting. Kosha's segments are small enough that an
  in-memory structure is the right scale; a future "wire into production"
  pass would need to design a persisted format for this (see `DESIGN.md`'s
  reserved `vector.idx` v2 slot).
- **Synchronous, inline rebalancing — no background job queue.** The paper's
  Local Rebuilder is a concurrent thread pool because it amortizes cost
  across a billion-vector index. Here, a rebalance event touches one posting
  plus at most `reassign_radius` (default 8) neighbors, so running it
  synchronously at the end of `insert`/`delete` is cheap enough that the
  thread-pool machinery isn't justified. See `ClusterIndex`'s doc comment.
- **Flat linear-scan centroid probing, not SPTAG.** SPANN uses a graph index
  over centroids because it has thousands-to-millions of postings. A kosha
  segment has, realistically, tens to a few hundred postings — a linear scan
  + partial sort (`centroid_probe.rs`) is simpler, easier to reason about,
  and not the bottleneck at this scale.
- **`balanced_bisect` stands in for "multi-constraint balanced clustering."**
  The SPANN/SPFresh papers cite this algorithm by reference without
  detailing it in the sections consulted for this implementation. This
  crate's version — farthest-pair seeding, Lloyd iterations, then a
  rebalance pass that moves the larger cluster's furthest-from-centroid
  members into the smaller one — is a concrete, from-scratch substitute, not
  a port of SPANN's actual algorithm. `tests/churn_ablation.rs` found one
  real consequence of this: a from-scratch rebuild doesn't reliably
  outperform incrementally-maintained full LIRE in this implementation
  (unlike the paper's Figure 10), most likely because re-clustering the
  *entire* live set every cycle re-applies this clusterer's own
  approximation noise everywhere, whereas incremental LIRE only touches the
  small region a Split/Merge actually changed. See the comment above that
  test's assertions for detail — this is reported, not hidden behind a
  tuned threshold.
- **Plain `deleted: bool` + informational `version: u8`, no CAS.** The
  paper's version byte exists to detect a vector being concurrently
  reassigned by two racing threads. There's exactly one thread here, so
  there's nothing to race with — the version field is kept (and bumped on
  every tombstone) mainly so a future concurrent version has a natural place
  to add real CAS-based conflict detection.
- **No product quantization / vector compression.** Vectors are stored raw
  (`Vec<f32>`), matching kosha's current `vector.idx` format. PQ (see the
  other paper this crate's design drew on, Jégou et al. 2011) would reduce
  memory/disk footprint at some recall cost — worth revisiting only if
  segment vector storage size (not update cost) becomes the bottleneck.

## Configuration defaults

`ClusterIndexConfig::new(dim)` defaults (`target_posting_size: 64`,
`max_posting_size: 128`, `min_posting_size: 16`, `reassign_radius: 8`,
`nprobe: 16`) are scaled for kosha's segment sizes, not the paper's
billion-scale defaults (e.g. the paper found `reassign_radius = 64` — its
"R nearest postings" — sufficient; this crate defaults to 8 since a whole
kosha segment has far fewer total postings to begin with).
