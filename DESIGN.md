# Kosha — A Storage-Disaggregated Search Engine on S3 + SSD Cache

Status: Draft v1.1
Author: Ravi Tandon
Date: 2026-08-05
Implementation language: Rust

## 1. Summary

Kosha is a purpose-built search microservice intended to fully replace Elasticsearch/OpenSearch
across Decover. It separates storage from compute: durable index data lives as immutable segment
files in S3, and every query/index node keeps a local NVMe SSD cache in front of S3 to get
disk-like latency without owning the durable copy. The service is exposed behind a single gRPC
(+ optional REST) API and is schema/embedding-model agnostic, so any tenant or product surface
(Sage's paragraph/case/page indices today, anything else tomorrow) can onboard by describing its
schema in config rather than by forking the search engine.

The goal is not "a faster Elasticsearch." It's an index format and runtime that assumes object
storage as the only source of truth, treats compute nodes as disposable/stateless, and makes
cost, elasticity, and multi-tenant isolation first-class instead of retrofitted.

## 2. Motivation

Sage currently runs hybrid lexical+vector search against OpenSearch (`backend/sage/app/`). That
gives us the feature set we need — `script_score` cosine similarity over `contentEmbedding`,
per-org/matter index resolution via `IndexRegistry`, `search_after` cursor pagination, structured
filters (user/document/matter/persons/orgs/dates/senders/recipients/tags/drive_paths), RRF fusion
of lexical and semantic candidate sets, and a Cohere rerank pass on top. But it comes with
OpenSearch's operational model baked in:

- **Compute and storage are fused.** Every data node holds a shard's data on local disk. Scaling
  query capacity means moving index data, not just adding stateless workers.
- **Cluster sizing is provisioned for peak, not billed for use.** Idle tenants (most matters,
  most of the time, in a legal-document product with bursty case activity) still hold a full
  shard's worth of hot storage and heap.
- **Index sprawl per tenant/matter is expensive.** `IndexRegistry` already works around this by
  resolving per-org/matter indices and custom connections — a sign that one-cluster-many-indices
  doesn't scale cleanly to the multi-tenant-per-matter isolation model Decover needs.
- **Recovery and rebalancing are cluster operations.** Losing a node means shard relocation
  across the cluster; there's no clean notion of "just re-fetch this segment from durable
  storage."

Kosha fixes this by making S3 the only place data must survive, and by making every node a
cache in front of it. Adding a query node is "start a process near S3"; losing one is "start
another and let the SSD cache warm."

## 3. Goals / Non-Goals

### Goals

1. **Full functional parity with Sage's current OpenSearch usage**: hybrid lexical (BM25-style)
   + vector (ANN) retrieval, structured filters, cursor-based pagination, highlighting, and a
   pluggable rerank hook.
2. **S3 as the sole durable store.** No node holds data that isn't recoverable by re-reading S3.
3. **SSD as a transparent cache layer**, not a second source of truth. Cache loss is a performance
   event, never a correctness event.
4. **Generalized, plug-and-play**: onboarding a new document type / tenant / embedding model is a
   config change (schema definition + index profile), not an engine change.
5. **Multi-tenant isolation** at least as strong as today's per-org/matter `IndexRegistry` model,
   with dramatically lower fixed cost per idle tenant.
6. **Operable as a normal microservice**: gRPC API, health checks, metrics, horizontal autoscaling
   of stateless compute, no dependency on a stateful cluster coordination layer beyond what's
   described here.

### Non-Goals (v1)

- Full Lucene/OpenSearch query DSL compatibility. We support the specific retrieval primitives
  Decover's products need (see §8), not an open-ended query language.
- General-purpose analytics / aggregations (`terms`, `date_histogram`, etc.) beyond simple
  faceting needed for filters. If a real analytics need emerges, it's a separate consumer of the
  same S3 data, not a feature of the query path. (Since then a bounded set computed from the
  filter columns has been added — metrics, `histogram` and fixed-interval `date_histogram`; see
  the README's Aggregations section.)
- Real-time (sub-second) durability of every write. Writes are durable to S3 within a bounded
  flush interval (see §7); we accept a small, tunable visibility lag in exchange for not fusing
  compute and storage.

### 3.1 Phasing

This document describes the target architecture, which was built in phases.

> **Where the implementation stands (keep in sync with the README's supported-features table).**
> Phase 1 (BM25 lexical search on the S3 + SSD-cache storage model) is complete, and most of
> what was deferred to Phase 2 has shipped too: segments now carry a posting-based
> `vector.idx` (cluster-and-posting ANN with centroid probing and triangle-inequality bounds),
> requests take a `knn` clause with `knn.filter`, and a lexical query plus `knn` is fused with
> RRF (§8 step 5). Still not implemented: the rerank hook (§8 step 6) and per-retriever RRF
> weights. The phase plan below is kept as the historical record of how the work was ordered.

Phase 1 was BM25 lexical search only — no vector index, no ANN, no fusion, no rerank hook.
Concretely, Phase 1 included:

- The S3 segment format and manifest (§6), with each segment containing `doc_store.bin`,
  `inverted.idx`, and `filters.bin` only.
- The full write path (§7): WAL, buffering, flush-to-segment, compaction.
- The full SSD caching layer (§9): read-through cache over segment files, exactly as described.
- The read path (§8), restricted to step 4's lexical branch: postings intersection/union +
  BM25 scoring.
- Structured filters and cursor pagination.
- Multi-tenancy (§10), migration approach (§14), and operational model (§15) — none of them
  are vector-specific.

**Deferred to Phase 2** (since implemented, except the rerank hook): `vector.idx`, ANN
retrieval, RRF fusion, the rerank hook, and embedding-dependent parts of the namespace schema.
The segment footer reserved a slot for the vector index so Phase 2 could add it without a
segment format migration.

Rationale: BM25 alone is a complete, independently useful retrieval mode (Sage already runs a pure
lexical path via `search_filter.lexical_search=True`), and it lets us prove out the harder,
genuinely novel part of this design — S3-as-source-of-truth with an SSD read-through cache and
disposable compute — without also debugging ANN index quality at the same time. Hybrid retrieval
is additive on top of a working BM25 engine, not a prerequisite for it.

## 4. Requirements

### Functional

- Hybrid retrieval: lexical term match + dense vector similarity, fused (RRF or configurable),
  optional rerank pass.
- Filters: exact-match (user/org/matter/document ids), set membership, date ranges, and
  extracted-entity filters (persons/orgs/dates from NER), matching what `SearchFilter` /
  `QueryIntent` produce today.
- Cursor pagination with independent lexical/semantic cursor state (mirrors `SearchCursor`).
- Per-namespace (org/matter/collection) index isolation, resolved at query time.
- Highlighting of matched terms in returned chunks.
- Bulk and incremental ingestion (documents arrive continuously via Pandora's pipeline).

### Non-Functional

- p50 query latency ≤ 80ms, p99 ≤ 300ms for a warm-cache namespace at typical result-set sizes.
- Cold namespace (no SSD cache entries) first-query latency bounded by S3 GET latency for the
  relevant segment footprint — target ≤ 1.5s p99 for a typical matter-sized index.
- Storage durability equal to S3's (11 nines), independent of compute node lifecycle.
- Horizontal scale-out of query capacity without data rebalancing.
- Cost scales with actual query/index volume and data size, not with tenant count.

## 5. High-Level Architecture

```
                         ┌─────────────────────────────────────────┐
                         │              Control Plane                │
                         │  - Namespace/schema registry (Postgres)   │
                         │  - Segment manifest store                 │
                         │  - Placement / cache-hint service          │
                         └───────────────┬─────────────────────────┘
                                         │
   ┌─────────────────────────────────────┼─────────────────────────────────────┐
   │                                     │                                     │
┌──▼──────────────┐               ┌──────▼──────────┐               ┌──────────▼───────┐
│  Ingest Nodes    │               │   Query Nodes     │               │  Compaction /     │
│  (stateless)     │               │   (stateless)     │               │  Merge Workers     │
│                  │               │                    │               │  (stateless)       │
│  WAL buffer→SSD  │               │  gRPC API          │               │  Read segments     │
│  Build segment    │               │  SSD block cache   │               │  from S3, merge,   │
│  Flush to S3      │               │  Query planner      │               │  write new segment │
│  Update manifest  │               │  Retrieval + fusion │               │  to S3, publish     │
└──────────────────┘               │  + rerank hook      │               │  new manifest       │
                                    └──────────┬──────────┘               └────────────────────┘
                                               │  read-through on miss
                                               ▼
                                    ┌──────────────────────┐
                                    │   S3 (source of truth) │
                                    │  segments/ manifests/  │
                                    │  WAL checkpoints       │
                                    └──────────────────────┘
```

All three node types (ingest, query, compaction) are stateless w.r.t. durability: local disk is
only ever a cache or a not-yet-flushed buffer that's also mirrored to a WAL segment in S3 on a
short interval. Any node can be killed and replaced without data loss.

### 5.1 Microservice boundary

Kosha is a single deployable service (à la Sage) with a gRPC surface, run as N replicas of each
node role behind the existing service mesh. It replaces Sage's OpenSearch client calls
(`index_searcher.py`, `query_builder.py`, `connection_factory.py`) with calls to Kosha's gRPC
API; the rest of Sage's ranking/fusion/rerank logic (`ranking/rrf.py`, `ChunkGenerator.__re_rank`)
either moves into Kosha as a pluggable stage or stays in Sage and consumes Kosha as a plain
retrieval backend. §12 covers this boundary choice.

## 6. Data Model

### 6.1 Namespace

A **namespace** is the unit of isolation and physical layout — equivalent to today's resolved
per-org/matter index in `IndexRegistry`. Each namespace has:

- A **schema**: field definitions (text, keyword, date, vector\<dim\>, geo, etc.), which fields are
  lexically indexed, which carry vector indices, which are filter-only.
- An **index profile**: tokenizer/analyzer choice (implemented: the schema's per-field `analyzer`,
  stored on the manifest and applied by the segment writers — see §6.2 and README "Analyzers"),
  ANN index type + params (e.g. HNSW `M`/`efConstruction`,
  or IVF-PQ for very large namespaces), compaction policy, cache priority tier.
- A **manifest**: an ordered, versioned list of segment references that make up the current
  readable state of the namespace (see 6.3).

Namespaces are cheap to create — creating one is a control-plane metadata write, not a cluster
operation. This directly addresses the `IndexRegistry` per-org/matter proliferation problem: a
namespace with zero documents costs nothing beyond a metadata row.

### 6.2 Segment

A **segment** is the immutable, self-contained unit of durable storage, analogous to a Lucene
segment. Once written, a segment is never mutated — only superseded by compaction. Each segment is
a directory of objects in S3:

```
s3://kosha-{env}/{namespace_id}/segments/{segment_id}/
    doc_store.bin       # document payloads (content, metadata), compressed, block-addressed
    inverted.idx         # term → postings list (docId, freq, positions) for lexical search
    vector.idx           # ANN index of the first vector column (cluster centroids + posting lists)
    vector.offsets       # its centroids and posting byte ranges, read at open
    vector.<n>.idx       # ANN index of vector column n (#204), and vector.<n>.offsets beside it
    doc_vectors.bin      # stored vector values, binary f32 (#243)
    filters.bin          # filter fields (dates, ids, tags), row-oriented — or, with the columnar format:
    filters.toc          #   column directory + per-column stats (#244)
    filters-NNN.col      #   one file per (field, type) column, numbered by id
    footer.json          # counts, blooms, analysis chains, vector columns, format version
```

**Filter index formats (#244).** A segment stores its filter values in one of two formats, and
`footer.json` says which (`filter_columns` present = columnar); readers never infer it from the
files that happen to be on local disk.

- `filters.bin` (v1): every field's `(doc_seq, value)` pairs in one row-oriented blob. Any filter,
  sort or aggregation on one field needs the whole file.
- Columnar (v2): `filters.toc` plus one `filters-NNN.col` per `(field, type)` column. Files are
  numbered, not named after fields, because field names are arbitrary strings. String columns
  (keyword, bool, date, capped text) hold a sorted dictionary, a roaring bitmap of documents per
  term, and a per-entry dictionary ordinal for sorts and aggregations. Integer and float columns
  hold values in doc order with min/max zone maps per 4096 entries. Every column has a presence
  bitmap and a CRC over its header and body; a damaged or truncated column fails the query
  rather than matching nothing. The footer also carries the column directory and per-column
  stats (value count, document count, distinct count, min, max), so a query node can plan
  which columns to fetch without an extra round trip, and later work can skip segments by
  range.

A query node fetches only `filters.toc` and the columns the query names (filter, kNN filter,
field sorts, collapse and aggregations; `x.keyword` also fetches `x`). On a columnar segment
the query evaluates straight from the columns (`kosha_segment::filter_eval`, entry point
`SegmentReader::evaluate_filter`):

- `term`/`terms` on a string column: a dictionary binary search, then that term's bitmap. A
  list value (JSON text such as `["a","b"]`) also matches per element through a per-column
  element index built once from the dictionary.
- `term`/`terms`/`range` on a numeric column: zone maps skip blocks that cannot match; integer
  blocks entirely inside a range are taken without comparing values; only the remaining blocks
  are scanned. Zone maps are verified against the values at decode.
- `range` on a string column: the dictionary's ordinal range, then those terms' bitmaps (or
  one pass over the ordinals when the range spans many terms).
- `exists`: the presence bitmap, less documents whose only values are empty lists.
- `bool`: set algebra; `must_not` subtracts from the all-documents set.

The result is the segment's dense `DocSet`, cached per segment and clause as before. Each
request removes its deleted documents from a copy, which gives the scoring loops a single
pre-filter: one bit test per candidate, and when it is sparse (under 1 in 64 documents) the
single-term WAND walk skips postings blocks whose doc range holds no passing document.
`DocSet::to_bitmap` hands the same set to a later filtered vector probe (#252). Aggregations
(`terms`, `cardinality`, `composite`) and field sorts/collapse read the ordinal and value
arrays for the matching documents instead of building row maps. `filters.bin` segments keep
the row-scan evaluator and the `FilterStore` paths. Both formats give identical results; a
namespace may mix them. On a 1M-document segment, columnar `term`/`terms`/`range` evaluation
is 40x to several thousand times faster than the row scan (`filter_eval_1m` bench).

`KOSHA_FILTERS_WRITE_VERSION` picks the format writers (flush and compaction) emit: `1`
(default) writes `filters.bin`, `2` writes columns. Every reader in this release reads both.
Rollout: keep the gate at `1` until every query and write node runs a release that reads v2,
then set it to `2`; compaction converts segments as it merges them. Rollback: set it back to `1`
and compaction rewrites columnar segments as `filters.bin`, but a binary older than #244 cannot
read columnar segments (their filters match nothing), so do not downgrade past this release
while any remain.
**Vector columns are per segment (#204).** Every `Vector` field gets its own ANN index, with its
own dimension. The footer's `vector_fields` lists each column's name, dimension, vector count and
file slot: slot 0 is `vector.idx` + `vector.offsets`, so a segment with one vector field is
byte-identical to one written before columns existed, and slot `n` is `vector.<n>.idx` +
`vector.<n>.offsets` (field names never appear in object keys). A kNN loads, hydrates and probes
only the column `knn.field` names; its centroids alone take part in the global probe budget. A
footer without `vector_fields` is a legacy segment: its single unnamed index answers a kNN on any
field, unless its `doc_vectors.bin` names exactly one field, which then binds it. A `knn.field`
that no segment indexes and no legacy segment could answer, or a query vector whose dimension
differs from the field's, is a 400. The writer fixes each field's dimension per namespace at the
first vector written to it (from segment footers and the write buffer) and rejects a document that
disagrees with a 400; a segment writer that still meets one stores it without indexing it rather
than failing the flush. There is no namespace schema for vector fields: everything above is read
from segment footers.

**Analysis is versioned per segment (#220, #221).** `inverted.idx` holds whatever terms the
analyzer produced at write time, so a segment's `footer.json` records the exact analysis chains its
`Text` fields went through (`analysis`: tokenizer + version + filters, with stopword lists copied
in). A footer without `analysis` means the legacy `standard@1` chain — every segment written before
analyzers existed, and every segment written since with default settings, which therefore stay
byte-identical. Queries are analyzed **per segment** with that segment's chains; a segment whose
fields used several chains reads the query once per chain and a document matches if it matches one
reading in full (scored with its best). An analyzer change (schema edit, tokenizer upgrade) thus
never makes older segments go dark: they keep answering with the analysis they were written with
until compaction re-analyzes their stored documents under the current schema.

**Per-field indexes (#249).** `inverted.idx` keeps the **combined** index (every `Text` field of a
document in one postings list per term, one length per document) that every query without
`fields` reads, and segments written since #249 add each field's own postings under the key
`\0{field}\0{term}`, a per-field norms list (posting tf = the document's length in that field),
and `field_index` in the footer (per-field `doc_count` / `total_length`, position gap). Field keys
use the same posting blobs, caches and hydration as any term; the term bloom holds only combined
terms (a field term is always also one), so bloom checks strip the field prefix. Consecutive `Text`
values of a document are a 65,536-position gap apart in both indexes and the phrase slop is
clamped below it, so a phrase never matches across fields. A segment without `field_index`
(written before #249, or with `KOSHA_FIELD_INDEX=off`) answers field-restricted queries from its
combined index rather than with zero hits, until compaction rewrites it.

Segments are sized to a target (e.g. 128–512MB) so that a full segment fetch/cache-fill is a
bounded, predictable operation. Small/continuous writes are buffered (§7) and flushed as a segment
once they cross the size or time threshold — never written as one-object-per-document (that's
what makes today's ES per-doc indexing overhead go away).

### 6.3 Manifest

The manifest for a namespace is a small, versioned JSON/protobuf object listing the current set of
live segment IDs plus tombstones for logically-deleted documents. Manifests are written with
compare-and-swap semantics (S3 conditional writes / a lightweight control-plane row backing the
pointer) so that:

- Query nodes always read a **consistent, point-in-time** view of a namespace (fetch manifest once
  per query, then only touch the segments it lists).
- Writers publish new state atomically by writing a new manifest version and flipping the pointer
  — old segments remain readable by anyone still holding the prior manifest until they're
  garbage-collected.

This gives snapshot-isolation reads without a distributed transaction: it's the same trick
Iceberg/Delta Lake use for tables, applied to a search index.

## 7. Write Path

1. **Ingest** (from Pandora, replacing the current `Elasticsearch bulk index` calls): a document's
   extracted text, chunk boundaries, embeddings, and metadata arrive at an ingest node's gRPC
   `IndexDocuments` call.
2. **Buffer**: the ingest node appends to an in-memory + local-SSD write buffer for that namespace,
   and synchronously appends to the WAL before acknowledging. *Implemented:* the WAL sits behind a
   `WalStore` trait with two modes. `KOSHA_WAL_MODE=local` (default) writes fsynced files under
   `data_dir/_wal`, which survive pod restarts but not node loss. `KOSHA_WAL_MODE=s3` (#236)
   writes each group commit as one object `_wal/{writer_id}/{lsn:020}.bin` in the segment bucket
   with a conditional PUT (`If-None-Match: *`) and acknowledges only after the PUT, so the write
   survives node loss before it is part of a segment. Concurrent appends are group-committed: one
   WAL file or object holds the records of every namespace that arrived during the previous
   commit. LSNs are allocated per writer stream (the manifest's `wal_origin`), not per namespace;
   each record carries its own LSN, and a file is deleted only once every namespace in it has a
   published manifest checkpoint at or past its records there. In s3 mode group `n + 1` is keyed
   by the LSN after group `n`, so two writers on one stream race for the same key: one wins, the
   other fails the write and stops accepting writes until restarted. A node that starts on an empty
   disk lists its stream, replays every record above each namespace's checkpoint into its
   buffers and continues after the highest LSN.
3. **Flush**: when the buffer crosses a size/time threshold (default: 512MB or 60s, whichever
   first), the ingest node builds inverted + vector + filter indices for the buffered documents,
   writes a new segment to S3, and publishes an updated manifest that adds the new segment.
4. **Compaction**: a background worker merges *small / recent* segments (and drops
   tombstoned/deleted docs) into fewer, larger ones under a size-tiered policy (§7.1), publishing
   a new manifest and marking old segments for GC after a grace period (bounded by the longest
   manifest a query node might still be holding). Compaction never holds a process-wide lock
   across merge I/O, and never re-merges already-large segments on every pass.

Deletes and updates are tombstone-based (mark-and-compact), same as Lucene/ES — there is no
in-place mutation of a segment.

### 7.1 Compaction policy & isolation

Compaction is the write-path operation most likely to become an operational burden if done
naively. The Phase-1 `compact_namespace` stopgap (admin route) merged *every* live segment into
one while holding the process-wide indexer mutex for the full synchronous disk read+write. That
is unacceptable as a scheduled behavior: it freezes every tenant/namespace for the duration of
the largest merge, and it defeats lazy per-segment hydration by repeatedly rebuilding one
ever-growing survivor.

**Target properties**

1. **Per-namespace isolation.** Compacting namespace A must not block reads or writes to
   namespace B. Prefer: do not block reads/writes to A for the duration of A's merge I/O either
   (snapshot → merge off the hot path → compare-and-swap publish).
2. **Size-tiered / incremental merges.** Only segments below a size threshold participate as
   merge inputs; already-large segments are left alone. Each pass merges a bounded batch (e.g.
   at most N smallest mergeable segments), not the entire namespace.
3. **Trigger on need, not on a blind timer.** Fire when live mergeable segment count (or total
   mergeable bytes) crosses a threshold — e.g. `mergeable_segments >= 8` — so compaction runs
   when it actually reduces query fan-out, not on a fixed clock regardless of load.
4. **Out-of-process from the query/ingest path (target).** DESIGN §5's dedicated compaction
   node role remains the end state: merge workers read segments from S3, write a new segment,
   and publish a new manifest via CAS. In-process tiered compaction with per-namespace locking
   is the Phase-1 bridge that makes the admin/emergency path and a future in-process scheduler
   safe; it is not a substitute for the compaction role at scale.

**In-process locking model (Phase 1)**

```
Indexer
  └─ namespaces: map<NamespaceId, NamespaceHandle>
                    ├─ state: Mutex<NamespaceState>   # buffer, manifest, tombstones, id_index
                    └─ compact: Mutex<()>             # at most one compact per namespace
```

- Looking up a `NamespaceHandle` takes only a short registry lock.
- Ingest / flush / exists / replace / delete take that namespace's `state` lock only.
- Compaction takes `compact` for the whole operation (no concurrent compact on the same ns), but
  holds `state` only for (a) selecting merge inputs + cloning tombstones, and (b) CAS-publishing
  the new manifest. Merge I/O (open + `iter_doc_records_raw` + `SegmentWriter::finalize`) runs with
  `state` released so searches and writes against other work on that namespace can proceed against
  the pre-compact snapshot; concurrent mutations that remove a merge input cause publish to abort
  (or retry) rather than corrupt the manifest.
- There is **no** process-wide `Mutex<Indexer>` on the request path.
- Across processes, the manifest CAS is the only coordination. A writer whose CAS loses
  (`ManifestConflict`) rebases its unpublished delta onto the committed manifest
  (`Indexer::rebase_onto`) and retries a bounded number of times. A compaction rebases only if
  its inputs are still listed remotely with no new remote deletes. A remote compaction is accepted
  only if it removed no segment with unpublished local deletes. Otherwise the publish fails with
  409, and nothing is published that references a segment the remote manifest removed.

**Merge memory (#242)**

A merge feeds every live input document through one `SegmentWriter`, so its heap is the
writer's heap:

- Stored records stream to `.build/doc_store.bin`; sidecar vectors arrive as f32 from
  `SegmentReader::iter_doc_records_raw` and go to `doc_vectors.bin` without a JSON round trip.
- In-memory postings are capped by `KOSHA_SEGMENT_BUILD_MEMORY_BYTES`; past it they spill to
  term-sorted runs.
- `finalize` k-way merges the runs by term. Only each run's head term and posting count are
  resident; a term's postings stream from the runs into a per-term encoder that writes the v5
  skip-split (or v4) bytes incrementally and spills its sections to scratch files past
  1/16 of the budget (4 KiB to 8 MiB). A term in every document costs no more memory than a rare
  one. The output is byte-identical to the whole-list encoders.

What remains O(input): the term table and string pool (O(vocabulary)), about 30 B per document
for the doc-store offsets and 4 B for its field length, filter columns (short values only: `Text`
is capped at 256 chars), and vectors plus k-means (#253). `crates/kosha-segment/tests/merge_memory.rs`
asserts peak heap ≤ budget + 64 B/doc + 6 MiB for a merge where one term occurs 32 times in every
document.

**Merge selection (size-tiered lite)**

Given a namespace manifest and a `CompactionPolicy`:

| Knob | Default | Meaning |
|------|-------------------|---------|
| `max_mergeable_docs` | 50_000 | Segment with `doc_count >=` this is never a merge input |
| `max_segments_per_merge` | 32 | Cap inputs per pass |
| `min_mergeable_segments` | 2 | No-op unless at least this many mergeable segments exist |
| `trigger_mergeable_segments` | 8 | `needs_compaction` becomes true at this count |

Algorithm: consider segments with `doc_count < max_mergeable_docs` (and present on local disk),
sort ascending by `doc_count`, take up to `max_segments_per_merge`. Merge those into one new
segment; leave larger segments and non-selected small segments untouched. A separate `Full` mode
(admin/emergency only) may still merge every local segment — it must never be what a scheduler
calls.

**Triggers & scheduling**

- **Do not** schedule blind periodic full compaction against live ingest/query nodes.
- The background scheduler on the write pod (`crates/kosha-server/src/compaction_sched.rs`,
  #242) calls `needs_compaction(ns)` every `KOSHA_COMPACTION_INTERVAL_MS` (default 30 s, `0`
  disables) and runs one tiered pass per due namespace, at most
  `KOSHA_COMPACTION_MAX_CONCURRENT` at a time, oldest-compacted namespace first. A failed pass
  backs off exponentially; a pass that merged nothing waits for the manifest to change. A
  separate compaction node role is still future work.
- Merged segments are capped at `max_merged_segment_bytes` (default 5 GiB,
  `KOSHA_COMPACT_MAX_SEGMENT_BYTES`); larger segments are never merge inputs again.
- The `POST /v1/admin/compact-namespace` route remains the emergency lever; default body mode is
  `tiered`. Pass `"mode": "full"` only for deliberate one-shot remediation.
- **Bounded local disk (#242).** A pass plans each merge group from object sizes in S3 (one
  paginated ListObjectsV2 over the namespace prefix) before hydrating anything, hydrates only that
  group's inputs (vectors included), merges and publishes it, then evicts the superseded inputs'
  local directories (never one the manifest still references or with a file pinned by an in-flight
  hydration or warmup; local-only mode never evicts, since there the directory is the only copy).
  Peak local disk for a group is its inputs plus its output, about `2 x max_merged_segment_bytes`.
  `full` loops capped groups inside one pass until none fits; a segment whose size S3 cannot report
  never joins a capped group. `max_segment_bytes: 0` (uncapped, all-to-one) still hydrates
  everything.

**What this deliberately leaves to the compaction node role (§5)**

- Running merges on separate compute so ingest/query pods never pay merge CPU/IO.
- Fair multi-tenant scheduling and cost controls across namespaces.
- Off-pod segment GC. Today GC runs on the write pod (`POST /v1/admin/gc`, §11).

**Durability/visibility tradeoff**: a document is durable (survives any single node failure) as
soon as its WAL append to S3 acknowledges. It becomes *queryable* once its segment is flushed and
the manifest updated — bounded by the flush interval above. This is a deliberate, tunable relaxation
versus ES's near-real-time (~1s) visibility; for Decover's ingestion pattern (batch document
processing via Pandora, not live chat-style writes) this is an acceptable and, in fact, currently
true in practice since ES refresh intervals are already tuned up for indexing throughput.

## 8. Read Path

1. Query node receives a `Search` RPC: namespace id, query text and/or query embedding, filters,
   cursor, max_results, retrieval mode (lexical / semantic / hybrid — mirroring
   `search_filter.lexical_search` today).
2. **Manifest fetch**: read the namespace's current manifest (cached in-process with a short TTL;
   invalidated on push notification from the control plane when a new manifest publishes).
3. **Segment selection**: filter predicates (date ranges, ids) prune segments using each segment's
   footer statistics (min/max, bloom filters) before touching their content — same trick as
   Parquet/Lucene segment skipping.
4. **Per-segment retrieval** (fanned out across the query node's worker pool, segments read
   through the SSD cache, see §9):
   - Lexical: postings-list intersection/union + BM25 scoring over `inverted.idx`.
     BM25 `k1` / `b` come from the request (`SearchQuery.bm25_params`), each falling back to the
     segment footer's build-time value when omitted, and are validated (`k1` ≥ 0, 0 ≤ `b` ≤ 1;
     otherwise HTTP 400). Nothing on disk bakes them in: postings keep raw term frequencies and
     block summaries keep `max_tf` / `min_field_length`, so block-max WAND ceilings are computed
     at query time with the same scorer as the hits and pruning stays exact for any valid values.
     BM25's collection statistics are **namespace-level** (issue #246): `N = Σ doc_count` and
     `avgdl = Σ total_field_length / N` over every manifest segment's footer, and
     `df(term) = Σ` per-segment posting counts, computed lazily for query terms only (a segment
     whose term bloom rules the term out contributes 0 without being opened) and cached per
     `(namespace, manifest version, segment-list fingerprint, term)`. Every segment scores with
     the same values, so a document's score does not depend on which segment it landed in and
     the cross-segment merge compares like with like; the block-max ceilings take their `df`
     from the same scorer. Deleted documents still count until compaction drops them, as they do
     in Lucene. If any segment's footer or a term's postings can't be read locally, the search
     falls back to per-segment statistics as a whole (reported as `segment_fallback`, and not result-cached) rather than
     score with a partial `df`; the server therefore also hydrates the inverted index and posting
     blob of segments the query's filter prunes but whose term bloom admits a query term.
     `bm25_params.stats: "segment"` (or `KOSHA_BM25_STATS=segment` engine-wide) restores
     per-segment statistics for A/B debugging. Wildcard queries keep per-segment statistics,
     since their expansion is per segment.
     **Fuzzy** (`fuzzy: {field, value, fuzziness, prefix_length, max_expansions,
     transpositions}`, issue #201) analyzes the value with each segment's chains and expands each
     token against that segment's term table. Every inverted-index format keeps the table sorted,
     so it already is the leaf order of a trie: the expansion is a Levenshtein automaton in DP
     form (optimal string alignment, so an adjacent swap is one edit) whose rows are shared across
     consecutive terms' common prefix, and a prefix whose row minimum exceeds the budget skips its
     whole range by binary search; `prefix_length` narrows the range up front. No sidecar is
     needed, so segments already on S3 expand the same way. The best `max_expansions` terms per
     token (fewest edits, then term order; the exact term first) are read like any term and
     scored in edit-distance tiers, `(2 − edits) + tf_norm` with `tf_norm ∈ (0, 1]` the BM25 tf
     saturation under the namespace `avgdl`, so an exact match outranks every one-edit match in
     the namespace, whatever their tf or length. IDF is left out because the expansions (and so
     any `df`) differ per segment; a per-segment IDF would let a rare typo in one segment outrank
     an exact hit in another. Pruning: `footer.json` carries `fuzzy_bloom`, a bloom over the
     vocabulary's one-deletion neighbourhood (SymSpell's observation: two strings are within one
     edit only if their deletion neighbourhoods meet), so a one-edit token costs `len + 1` bloom
     probes; zero-edit tokens use the term bloom; two-edit tokens, segments written before #201
     and segments too large for a useful neighbourhood bloom are always opened. Every posting
     blob shard is hydrated for a fuzzy query, as for a wildcard.
     A `fields` query (`["title^2", "content"]`, issue #249) scores
     `Σ_fields boost × BM25_field`, each field with its own postings, per-document lengths and
     statistics (`N` = documents with the field, `avgdl` = its mean length, `df` from its own
     postings — namespace-level, summed from the footers' `field_index` and the field keys'
     posting counts). Each field reads the query with the analysis chain its segment indexed it
     with; with AND, each query position must match in some listed field. It runs on an
     exhaustive per-field path (per-field block summaries carry no lengths yet, so no WAND).
     `match_phrase` with a `field` (or `fields`) is matched in that field's postings.
     Block-max WAND prunes against a **namespace-wide top-k floor** (issue #248): segments of
     one plain ranked lexical query (no kNN/hybrid, phrase, wildcard, `fields`, fuzzy, aggs,
     collapse, `limit_per`, custom sort or `search_after`) share an `AtomicU64` holding the highest
     k-th-best score any segment has reached (`k = from + max_results`, or the `rank_by` rescore
     window when that is larger), and each prunes blocks and candidates strictly
     below `max(local k-th-best, shared floor)`. A published value is the k-th score of `k` real,
     accepted documents scored exactly as the merge ranks them, so the final page's k-th score
     is at least that high and nothing that could reach the page is ever pruned; ties are still
     scored. Before scoring, each segment's ceiling (Σ over query terms of its best block's
     `block_max_score`) is computed and segments are handed out highest ceiling first from a
     shared queue, so the strongest segments raise the floor early (the ceiling uses the terms
     that segment's own analysis chains read the query as, #220; a mixed-analyzer segment gets an
     infinite ceiling); a single-term segment whose
     ceiling is already below the floor is skipped outright (its exact count is the term's
     `df`). The ceiling inputs are cached per `(namespace, manifest version, segment-list
     fingerprint, term)` like the `df` values: each segment's footer shape and the Pareto
     frontier of the term's block `(max_tf, min_field_length)` skip summaries. BM25 rises with
     tf and falls with length for every valid k1/b, so the best block under any request's
     scorer lies on that frontier, and a warm query computes all ceilings from memory (~10 µs
     for 200 segments); a new term costs one pass reading skip summaries, decoding no posting. Counts are unaffected:
     exact counting still visits every match, and capped counting stops a segment only once its
     own count reaches the cap. A legacy segment without stored summaries has an infinite
     ceiling (visited first, never skipped). A segment that publishes and then abandons its WAND
     walk for the general path (a corrupt block found mid-walk) poisons the floor and the search
     is rescored with per-segment floors. Hybrid (RRF) requests are untouched: their lexical
     list is a rank window, scored by the general path. `SearchPhaseStats` reports
     `wand_docs_scored`, `wand_segments_skipped`, `wand_global_threshold` and `wand_bounds_ms`.
   - Semantic: ANN search over the `knn.field` column's index (`vector.idx`, or
     `vector.<n>.idx` for a second column, #204) — probe the nearest cluster centroids (one probe
     budget for the whole query, bounded with triangle-inequality radii) and score the
     postings' unit-norm vectors by dot product (cosine); older segments fall back to flat or
     HNSW search. This is the direct replacement for OpenSearch's `script_score` cosineSimilarity
     query in `query_builder.py`.
5. **Fusion**: candidate sets from lexical and semantic retrieval are merged via Reciprocal Rank
   Fusion, `score(d) = Σ_r 1/(rank_constant + rank_r(d))`, matching `ranking/rrf.py`'s existing
   behavior — ported in, not reinvented. Request shape is Elasticsearch's
   `"rank": {"rrf": {"rank_window_size", "rank_constant"}}` (defaults: `max_results`, 60); every
   request with both a lexical query and `knn` is fused this way (implemented, issue #228). Ranks
   are **global**: each segment returns its best `rank_window_size` BM25 hits and its nearest `k`
   vectors, and only after every segment has answered are both lists ordered across the whole
   namespace, cut to their windows, ranked and fused. Fusing per-segment ranks would make each
   segment's best match rank 1 however weak it is namespace-wide; it is also why namespace-level
   BM25 statistics (above) matter here — lexical scores have to be comparable across segments
   before they can be ranked together. Wildcard queries keep per-segment statistics, so their
   global lexical order is approximate. The top-level filter applies to both retrievers,
   `knn.filter` to the vector one only. `total_hits` and aggregations cover the lexical matches
   plus the vector window; `from`/`sort`/`collapse`/`search_after` apply within the fused list,
   which is at most `rank_window_size` long. Each hit carries its per-retriever ranks.
   **Diversification** (`limit_per: {field, max}`, issue #211) caps hits per distinct value of a
   keyword field. Whether a hit survives depends only on the result order — fewer than `max`
   hits of its group rank above it — so it is decided while collecting, not by post-filtering a
   page, and a `search_after` cursor needs no cap state of its own: every page recomputes the
   same capped list and resumes it after the cursor. Each segment caps its own ordered hits
   first (a hit in the namespace's capped list always survives its segment's cap, since no more
   of its group can rank above it inside one segment than across all of them), cuts to
   `from + max_results` when there is no cursor, and the reduce step caps the merged list again.
   Under a cursor a segment does not drop hits before it: those are what use up a group's cap
   namespace-wide. Hybrid requests diversify each retriever's list before its window cut, so the
   fused list holds at least `rank_window_size` capped hits; kNN segments probe up to
   `num_candidates` instead of `k` so the cap has neighbours to choose from. `total_hits` counts
   matches before the cap, as with `collapse` (which is the `max: 1` case and the same code).
   **Score modifiers** (`rank_by`, issue #212) blend attributes into the score: field-value
   boosts and linear / exp / gauss decays over numeric, date and geo fields, read from the
   filter columns (no doc-store reads). An unbounded modifier would break the block-max WAND
   ceilings, so modifiers are applied in a **rescore window** rather than folded into block
   maxima: retrieval, WAND pruning and fusion run exactly as without `rank_by`, each segment
   hands back its best `max(window_size, from + max_results)` hits by that score (WAND's top-k
   is simply widened), the reduce step keeps the namespace's best `window_size`, rescores them
   and re-sorts. The window is therefore the exact top-N by the unmodified score, and nothing
   outside it is returned (`total_hits` still counts every match). Group caps and `search_after`
   apply to the rescored order, so segments do not cap or cursor-filter under `rank_by`.
   `origin: "now"` is resolved once per request, and such responses bypass the result cache.
   **Geo**: a `GeoPoint` value is stored as given and copied into the string filter column as
   canonical `"lat,lon"` text — an existing column type, so segments stay readable by older
   binaries and no footer or manifest field changes. `geo_distance` filters parse that column
   (haversine, mean Earth radius); geo decays read it the same way.
6. **Rerank hook** (implemented, issue #251): an optional, pluggable cross-encoder rerank stage
   runs on the fused top-N before results return — same role as `ChunkGenerator.__re_rank`.
   Request: `"rerank": {"provider", "model", "field", "top_n", "timeout_ms"}`. The searcher runs
   a first-stage query (top `top_n` from offset 0, the rerank field always returned), the server
   releases its scoring slot, sends the query text and each hit's `field` text to the named
   provider (`kosha_query::rerank::RerankProvider`; the server's HTTP implementation speaks the
   Cohere `POST /rerank` shape, keys from the environment), orders by the provider score and
   then applies `from` / `max_results` and the source filter. Hits carry `fused_score` and
   `rerank_score`. A timeout or provider error falls back to the fused order with
   `rerank_degraded: true` (cf. `knn_degraded_segments`) and is not result-cached; totals and
   aggregations are the first stage's. Rerank params are part of the result-cache key.
   Per-provider latency / outcome / cost counters are in admin `/stats`.
7. **Cursor**: response includes per-mode (lexical/semantic) cursor state (last score + doc id) so
   pagination works identically to `SearchCursor` today.
8. **Highlighting**: computed from the matched postings' term offsets stored in `inverted.idx`,
   returned alongside each hit.

The retrieval + fusion + cursor logic in steps 4–7 can live inside Kosha (so Sage becomes a thin
client) or stay in Sage calling a lower-level Kosha retrieval RPC per mode — see §12 for the
recommended split.

## 9. Caching Strategy (SSD Layer)

The SSD layer is a **read-through, write-behind cache**, never authoritative:

- **Unit of caching**: individual segment files (or fixed-size blocks within them for very large
  segments), addressed by `(namespace_id, segment_id, file, byte_range)`.
- **Population**: on a query node cache miss, fetch from S3, serve the request, and asynchronously
  persist the fetched bytes to local NVMe. Ingest nodes populate the cache directly from the
  buffer they just flushed (avoids an immediate read-back round-trip to S3 for freshly written
  data).
- **Eviction**: size-bounded per node (e.g. 80% of local NVMe capacity), LRU/ARC policy, with cache
  priority weighted by the namespace's index profile (e.g. active-matter namespaces pinned warmer
  than archived ones).
- **Placement hinting**: the control plane tracks approximate namespace→node affinity (consistent
  hashing over namespace id) so repeated queries for the same namespace land on query nodes likely
  to already have it cached, without requiring hard node ownership — a node with a cold cache for
  a namespace simply falls back to S3 and warms up, no rebalancing operation required.
- **Page documents by byte range** (#245): a query reads the scoring set (footers, TOC,
  filters, `doc_store.offsets`) whole, but materializes its result page from ranged GETs of just
  those documents' spans in `doc_store.bin` / `doc_vectors.bin`, coalescing spans closer than
  64 KiB into one GET. Fetched spans persist in a sparse span cache,
  `{segment}/{file}.spans/{start:016x}-{end:016x}`, one file per fetched range, counted and
  LRU-evicted like any other cached file. A partial file is never written at the real file path,
  because existence there means complete. A segment that keeps taking ranged reads is promoted:
  its whole files are hydrated in the background and its spans dropped. A whole local file always
  wins over spans. `KOSHA_DOC_STORE_READ_MODE=whole` reverts to whole-file page hydration.
- **Postings by byte range** (#245): posting shards are written raw (no whole-file zstd wrapper),
  flagged `posting_blobs_raw` in the footer, so the `(postings_off, postings_len)` the
  `inverted.idx` TOC holds per term is a byte range of the stored shard. A cold query fetches its
  terms' spans (coalesced per shard) into the same span cache, in one round trip after the
  scaffolding round, and `SplitInvertedIndex` reads a term from the spans when the whole shard is
  absent. A term whose span is missing still fails the query (never scores as absent). kNN
  queries hold only `vector.offsets` locally and the reader asks the query node to fetch the
  `vector.idx` ranges of the postings it is about to probe. A cold BM25 query is therefore
  scaffolding, postings, page documents: 3 sequential round trips (4 when the footers must be
  fetched first for pruning). No reader-format change: older binaries read raw shards, so a
  rollback still reads new segments; segments without the flag keep whole-shard hydration.
- **Usage-driven warming** (#213): every search records decayed read counts (hits, and bytes
  fetched from S3) per namespace, per segment and per file class (scoring scaffolding, posting
  shards, doc store, vector index), with a half-life of `KOSHA_ACCESS_HALF_LIFE_SECS` (default
  600) and at most `KOSHA_ACCESS_MAX_SEGMENTS` segments tracked (default 50,000; the coldest tenth
  is dropped past it). Every `KOSHA_WARMSET_PERSIST_SECS` (default 60) the pod writes its top
  namespaces and segments to `_warmset/<KOSHA_ROLE>.json` under the S3 prefix (pods of one role
  share it; the last writer wins). The readiness-gated boot pass warms `KOSHA_WARMUP_NAMESPACES`
  (always included, first) plus the namespaces of that warm set, so a restarted pod warms what was
  actually hot. A usage namespace gets the scoring scaffolding plus only the classes it was seen
  reading. Items are warmed scaffolding first, then in descending read rate, and the pass stops at
  `KOSHA_WARMUP_BUDGET_BYTES` (unset is unlimited): the first item that does not fit and
  everything after it are skipped, and the log and `GET /v1/admin/warmup-status` list what was
  warmed and what was skipped. On query pods a re-warm thread polls `manifest_version` for hot
  namespaces every `KOSHA_REWARM_INTERVAL_MS` (default 5000) and, when a flush or compaction
  publishes, installs the new manifest and fetches the added segments before a user query does,
  within what is left of the budget (a compaction's removed inputs give their bytes back). It
  batches like the boot pass (a hydration permit per 8 segments, 2 for doc stores and vector
  indexes), never touches readiness, and fails open. Warming a segment whole also drops its
  sparse spans, the same as a promotion. `KOSHA_WARMUP_USAGE=0` restores the static behavior.
- **Failure mode**: losing a query node's local disk (or the whole node) loses nothing but cache
  warmth. Traffic reroutes (consistent hashing + health checks) and the replacement node rebuilds
  its cache from S3 traffic as queries land.

This is the core mechanism that makes storage/compute separation viable at ES-competitive latency:
cold data pays an S3 GET; hot data (the overwhelming majority of real query traffic in a
matter-centric product where a handful of active matters dominate query volume) pays a local NVMe
read.

### 9.1 NVMe on AWS

Query and ingest nodes run on **instance-store-backed EC2 types** (`i4i`, `i3en`, or `r6id`
family) — physically attached NVMe on the host, not network-attached EBS. This is a deliberate
choice, not a default:

- Instance-store NVMe gives the lowest latency/highest IOPS path available on AWS, with no
  network hop to a separate storage service (unlike EBS, even io2).
- It is **ephemeral by design** — data is wiped on stop/terminate or lost on host failure. That's
  acceptable, and actually desirable, because the SSD only ever holds a cache (§9): nothing durable
  lives there, so there's no data-loss risk to reason about, only a cold-start rewarm cost.
- EBS (gp3/io2) is deliberately *not* used for the cache tier: it costs more per GB, adds
  network-attached latency, and its one advantage — surviving an instance stop/restart — buys
  nothing here since a fresh cache just rewarms from S3 on the next queries anyway.
- On EKS/ECS, this means query-node pools run on `i4i`/`i3en` worker nodes with the instance's
  local NVMe mounted as a hostPath/local volume for the cache directory, sized against §4's cache
  hit-rate targets and evicted per the LRU/ARC policy above.

## 10. Multi-Tenancy

Namespace = tenant isolation boundary, same granularity as today's `IndexRegistry.resolve(org_id,
matter_id, index_type)`. Because a namespace with no data costs only a metadata row (no cluster
shard, no reserved heap), we can create one per org **and** per matter without the current
incentive to consolidate tenants into shared indices for cost reasons. This directly simplifies
`IndexRegistry`: instead of resolving between "default shared index" vs. "custom connection to a
dedicated cluster," every namespace is just... a namespace. Access control (which namespaces a
request is allowed to touch) is enforced at the control plane / gRPC interceptor layer using the
existing org/user auth context Bach already establishes.

## 11. Consistency & Durability

- **Durability**: with `KOSHA_WAL_MODE=s3` the WAL PUT to S3 completes before `/index`,
  `/replace` or `/delete` returns, so an acknowledged write survives node loss immediately; a failed
  or timed-out PUT (`KOSHA_WAL_S3_TIMEOUT_MS`) fails the request and nothing is acknowledged. The
  cost is ack latency: one S3 PUT instead of an fsync (in a simulated 25 ms + 100 MB/s PUT model, a
  500 kB write acks at p50 ≈ 38 ms alone and ≈ 144 ms with 16 concurrent namespaces sharing PUTs;
  turbopuffer reports p50 ≈ 165 ms). With the default `KOSHA_WAL_MODE=local` the WAL is an fsynced
  local file: acknowledged writes survive process and pod restarts, but not the loss of the node's
  disk until a flush publishes them. Segment + manifest writes are the durable "committed" state
  for query visibility; a flush publishes `wal_checkpoint` by CAS and only then reclaims the WAL
  records it covers.
- **Read consistency**: each query snapshots one manifest version for its whole execution —
  read-your-writes is bounded by the flush interval (§7), not immediate, which matches the
  approach we already accept operationally with ES refresh tuning.
- **Manifest updates**: conditional (compare-and-swap) writes prevent lost updates from concurrent
  flush/compaction; a losing writer retries against the new base.
- **Garbage collection** (#239): superseded segments are deleted only after the max
  manifest-staleness window elapses (i.e., after no query node could plausibly still be holding a
  reference to them).
  - *Retirement records.* A publish that drops a segment (compaction, rebase, admin import) or
    replaces a segment's deletion generation lists it in the manifest's optional `retired` field
    with `retired_at_ms` and `retired_in_version`. The record rides in the same compare-and-swap as
    the change, so neither can commit without the other. The control plane keeps only the current
    manifest (no history, no branches yet), so "referenced" means referenced by the current
    manifest of some namespace.
  - *Local disk.* Each pod queues the segments its manifest dropped and removes their directories
    once all of these hold: `KOSHA_LOCAL_GC_GRACE` (default 5 min) has passed; the dropping
    version is committed (without S3, local disk is the only copy until then); every search
    snapshot alive at retirement has been dropped; and the searcher holds no open reader of the
    segment (readers load the doc store and postings lazily, by path). Removal goes through the
    disk cache, so its `eviction_generation` bumps and cached presence verdicts are re-checked.
  - *Object store.* `POST /v1/admin/gc` lists each namespace's prefix. A candidate is a segment
    prefix or `deletes-*.roaring` object the current manifest does not reference that was either
    retired more than `KOSHA_GC_GRACE` ago (default 1 h, above any replica's manifest lag) or has
    no record and whose newest object is older than `KOSHA_GC_ORPHAN_GRACE` (default 24 h: a
    crashed flush, a losing CAS writer, pre-#239 compactions; long because an upload whose publish
    has not committed yet looks the same). Untracked prefixes must carry this namespace's
    segment-id prefix, and prefixes that belong to a nested namespace are skipped. The manifest is
    re-read right before deleting; if its version moved, the namespace is skipped. `_COMMIT` goes
    first, so a half-deleted segment never looks durable. The deleted objects' records are then
    dropped in one more CAS; if that loses a race, the next run prunes records whose objects are
    gone. Deletion generations are not part of a segment's `_COMMIT` (they are verified by their
    checksum in the manifest), so dropping a superseded one never makes its segment look
    incomplete. Dry run is the default.

## 12. API & Integration with Sage

Recommended split: **Kosha owns retrieval + fusion + cursor state; Sage keeps rerank, query
intent extraction (NER), and answer generation.** Rationale: fusion and cursoring are tightly
coupled to segment-level ranking internals Kosha controls; rerank (Cohere) and intent (NER) are
product-specific concerns that don't belong in a generalized search engine.

### 12.1 Spec-first design (Elastic-style)

The Kosha API follows a strict **spec → server + client** pattern, modeled on Elasticsearch's
approach:

1. **Source of truth**: `proto/kosha/v1/kosha.proto` — a single protobuf service definition
   with `google.api.http` annotations that jointly define the gRPC and HTTP/JSON surfaces.
2. **Spec compilation**: the proto is compiled into a standalone OpenAPI spec via buf +
   protoc-gen-openapiv2, which becomes the public contract for non-gRPC clients.
3. **Code generation**: all language clients (Python, Go, TypeScript, etc.) are auto-generated
   from either the proto (gRPC stubs) or the OpenAPI spec (REST clients). No hand-written
   request/response serialization.
4. **Thin client layer**: a minimal hand-written wrapper around the generated stub that adds
   auth, retry, logging, and (for Python) the opensearch compatibility shim — no business logic.

This is the same pattern Elastic uses ([elasticsearch-specification](https://github.com/elastic/elasticsearch-specification)):
a formal spec drives codegen in every language; the spec IS the contract, clients are derived
artifacts.

### 12.2 Transport: HTTP/JSON primary, gRPC secondary

Given the intent to open-source Kosha as a serverless-friendly service (§15), **HTTP/JSON is
the primary, canonical API surface; gRPC is an optional secondary transport for internal,
latency-sensitive callers** (Sage today).

- gRPC's persistent HTTP/2 streams don't play cleanly with a lot of serverless/edge
  infrastructure (API Gateway, Lambda, Cloudflare Workers), and browsers can't speak raw gRPC
  without a proxy layer (grpc-web, envoy). For an open-source project where third parties will
  self-host or embed Kosha behind arbitrary infra, that's friction most adopters shouldn't have
  to pay.
- Plain HTTP is universally supported, curl-able, and needs no protobuf toolchain to get a first
  request working — this matters far more for open-source adoption than shaving off gRPC's
  marginal per-call latency advantage. This mirrors what Turbopuffer (§18, prior art) actually
  ships.
- Internal callers that already speak gRPC throughout Decover's stack (Bach → Callosum/Sage/
  Pandora/Valora) can keep using gRPC against the same logical API — it's a second binding of the
  same service definition (see below), not a fork of the API.

The service is defined once in `proto/kosha/v1/kosha.proto` and exposed two ways:

| Transport | Mechanism | Use case |
|-----------|-----------|----------|
| **HTTP/JSON** | gRPC transcoding via `google.api.http` annotations | External/open-source callers, curl, serverless |
| **gRPC** | Direct HTTP/2 | Internal Decover services (Sage, Bach) |

Equivalent HTTP calls from the proto definition:

```
# Index documents
POST /v1/namespaces/{namespace}/documents
{"documents": [{"id": "doc1", "fields": [{"name": "title", "value": "hello"}]}]}

# Search
POST /v1/namespaces/{namespace}/search
{"query_text": "breach of contract", "max_results": 20}

# Health
GET /v1/healthz

# Stats
GET /v1/stats
GET /v1/namespaces/{namespace}/stats

# Flush
POST /v1/namespaces/{namespace}/flush

# Delete by query
POST /v1/namespaces/{namespace}/delete
{"filter": {"term": {"documentId": "doc1"}}}
```

`common/index/paragraph_repository.py`'s `search_similar_paragraphs(...)` becomes a thin
translation layer from Sage's existing call signature into a `SearchRequest`, dispatched over
whichever transport Sage's client is configured for (gRPC, to stay consistent with its other
internal service calls) — so `index_searcher.py` and `chunk_generator.py` need minimal changes
beyond the repository/connection layer, regardless of which transport the wider open-source
project standardizes on for external users.

## 13. Generalization ("Plug-and-Play")

To be a true ES/OpenSearch replacement rather than a Sage-specific engine, three things must be
config, not code:

1. **Schema-driven namespaces**: field types, analyzers, and vector dimensions/metric are declared
   per namespace at creation time (`CreateNamespace`), not hardcoded to Sage's paragraph/case/page
   shapes. A new product surface (e.g. a future non-legal document type) onboards by defining a
   schema, not by touching Kosha internals.
2. **Pluggable embedding models**: vector dimension and distance metric are namespace properties;
   Kosha never generates embeddings itself, it only indexes/searches whatever vector the caller
   supplies — same contract as today (`query_embedding` computed by Sage/Pandora, not by
   OpenSearch).
3. **Pluggable rerank/fusion**: fusion strategy (RRF vs. simple concat-dedupe, matching the
   existing `ENABLE_RRF_FUSION` flag) and whether a rerank hook is invoked. Rerank is per request
   (`rerank`, §8 step 6) against server-configured providers; a caller can still rerank itself
   (per §12 recommendation, Sage may keep owning rerank).

## 14. Migration from OpenSearch

Phased, reversible cutover:

1. **Dual-write**: Pandora's ingestion writes to both OpenSearch and Kosha; no read traffic to
   Kosha yet. Validates ingest path and lets segments/caches warm.
2. **Shadow read**: Sage issues the same query to both backends, logs/compares results (recall,
   latency, top-k overlap) without serving Kosha's results to users.
3. **Canary cutover**: route a percentage of read traffic (by org, to bound blast radius) to
   Kosha, with automatic fallback to OpenSearch on error/timeout.
4. **Full cutover**: all reads from Kosha; OpenSearch becomes write-only (kept warm) for a
   rollback window.
5. **Decommission**: stop dual-write, tear down OpenSearch clusters once the rollback window
   passes and Kosha metrics have held steady.

Backfill of existing documents (rather than relying on dual-write for pre-existing data) is a
one-time batch job reading from the existing Postgres/S3 document store and replaying through
`IndexDocuments`, sized/throttled independently of live ingestion.

## 15. Operational Model

- **Scaling**: query and ingest node pools scale independently and horizontally (stateless →
  standard autoscaling on CPU/queue depth). No shard rebalancing operation exists in this design.
  **Doc/reality gap, not yet resolved**: "ingest scales horizontally" is not actually true as
  implemented — `kosha-write::Indexer` allocates each namespace's next segment ID from an
  in-memory-only counter with no cross-pod coordination, so two ingest pods writing the same
  namespace concurrently can pick the same ID and silently clobber each other's data. The k8s
  split (`k8s/README.md`'s "Query/ingest split" section) keeps ingest pinned to exactly one
  replica for this reason. Query does scale horizontally as described here; ingest doesn't yet —
  making it safe to do so would need a coordinated segment-ID allocator (e.g. backed by Postgres),
  which is out of scope for now.
- **Cost**: steady-state cost ≈ S3 storage + GET/PUT volume + compute node-hours actually running,
  which tracks real usage instead of provisioned-for-peak cluster capacity. Idle namespaces cost
  only their S3 bytes.
- **Monitoring**: cache hit rate (per namespace and aggregate), manifest staleness, segment count
  per namespace (compaction health), p50/p99 query latency split by cache-hit vs. cache-miss path,
  WAL flush lag.
- **Failure domains**: S3 durability failure is out of scope (treated as infrastructure-level).
  Any compute node failure is a non-event beyond transient cache-miss latency for its share of
  traffic.

## 16. Comparison to Current State

| | OpenSearch (today) | Kosha |
|---|---|---|
| Source of truth | Local disk per shard, replicated within cluster | S3 segments, single source of truth |
| Hot-path storage | Same disk as source of truth | SSD cache, independently sized from durable data |
| Scaling query capacity | Requires shard placement/rebalancing | Add stateless query nodes |
| Idle-tenant cost | Full shard footprint regardless of activity | Metadata row + S3 bytes only |
| Multi-tenant isolation | Per-cluster/per-index, driving `IndexRegistry` complexity | Per-namespace, cheap to create |
| Node loss | Shard relocation, potential temporary reduced replication | Cache rewarm only, no data risk |
| Query surface | Full ES DSL (mostly unused by Sage) | Purpose-built hybrid retrieval + filters |
| **Cost of data** | ~$0.16–0.24/GB-month (SSD-backed EBS volumes, 2–3× replication for HA), and sized for peak so effective utilization of provisioned storage is often only 30–50% | ~$0.023/GB-month (S3 Standard, single durable copy — no manual replication). SSD cache cost is decoupled from total corpus size: it scales with *active* data, not total stored data |
| **Read / Write Latency** | Read: p50 ~50–100ms, p99 ~200–500ms (shard-size dependent). Write: near-real-time, ~1s refresh-to-visibility | Read: p50 ≤80ms / p99 ≤300ms warm-cache, p99 ≤1.5s cold-cache (§4). Write: WAL-ack durability <100ms, but visibility is bounded by the flush interval (default 60s, §7) — a deliberate regression on write-visibility latency, traded for storage/compute separation |
| **Read / Write Throughput** | Bounded by shard count — max query/index parallelism is fixed at shard-creation time; adding capacity requires rebalancing | Read: scales ~linearly by adding stateless query nodes, no data movement required. Write: batched segment builds generally exceed ES's per-document bulk-index overhead, bounded mainly by S3 PUT throughput per ingest node |

*Cost and throughput figures above are directional planning targets, not measured benchmarks — see §17 for the validation this design still needs before these numbers can be treated as commitments.*

## 17. Open Questions / Risks

- **ANN index choice at scale**: *partly resolved* — segments now write a cluster-and-posting
  index (`KOSHA_VECTOR_WRITE_VERSION=4`) built at flush/merge time, rather than an HNSW graph
  rebuilt on every open. Still open: incremental updates (the SPFresh/LIRE prototype in
  `kosha-vector-spfresh`), quantization, and recall tuning at large segment counts. Original
  note: HNSW is simple and fast to query but expensive to build/merge
  incrementally at compaction time; IVF-PQ compacts cheaper but costs recall. Needs a benchmark
  against Sage's actual embedding dimensionality and namespace size distribution before locking in
  a default per index profile.
- **Cold-start latency for rarely-accessed matters**: legal matters can go dormant for
  months and then need an urgent search. First-query-after-dormancy latency (full segment fetch
  from S3) needs to be measured against the p99 latency SLO in §4 and may need a "matter reopened"
  cache-prewarm hook from Bach.
- **Manifest store dependency**: the control-plane metadata store (namespace registry, manifest
  pointers) is a new stateful dependency (likely Postgres, reusing Callosum's existing database)
  and becomes a new availability-critical path for the *write* side; read-path availability should
  degrade gracefully (serve last-known-good manifest) if it's briefly unavailable. *Implemented
  for query pods (#232):* a background refresher polls manifest versions every
  `KOSHA_MANIFEST_REFRESH_MS`; on control-store errors it keeps the last-known-good manifests,
  counts the errors and lets `manifest_lag_seconds` (in `/v1/stats`) grow. Searches fail only for
  a namespace the pod has never loaded. The write side still requires the store.
- **Compaction cost/scheduling**: ~~needs a concrete policy (size-tiered vs. leveled)~~
  **resolved for Phase 1 in §7.1** (size-tiered lite, per-namespace locks, need-based triggers,
  admin `full` mode as emergency-only). Still open: cost model / fair multi-tenant scheduling for
  the dedicated compaction node role, and whether to graduate from size-tiered-lite to a true
  leveled LSM policy once namespace size distributions are measured.
- **Feature gaps vs. ES DSL**: confirm no other Decover consumer beyond Sage depends on
  OpenSearch aggregation/analytics features not covered by §3's non-goals before committing to
  full replacement.
- **Unvalidated cost/latency/throughput targets**: the §16 figures for cost of data, read/write
  latency, and read/write throughput are estimates from public S3/EBS pricing and this design's
  intended behavior, not measurements. Phase 1 (BM25-only) should include a benchmark harness that
  replays real Sage query/ingest traffic against a Kosha prototype to confirm these numbers,
  particularly the write-visibility latency regression versus ES's near-real-time refresh, before
  any cutover decision in §14 relies on them.

## 18. Prior Art

This design follows the same storage-disaggregation pattern used by Quickwit (search on object
storage), Turbopuffer (vector/hybrid search on S3 + NVMe cache), and table formats like Iceberg/
Delta Lake (manifest-based snapshot isolation over immutable object-storage files) — applied here
specifically to Decover's hybrid lexical+vector legal-document retrieval workload.
