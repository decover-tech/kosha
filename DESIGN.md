# Kosha — A Storage-Disaggregated Search Engine on S3 + SSD Cache

Status: Draft v1
Author: Ravi Tandon
Date: 2026-07-18
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
  same S3 data, not a feature of the query path.
- Real-time (sub-second) durability of every write. Writes are durable to S3 within a bounded
  flush interval (see §7); we accept a small, tunable visibility lag in exchange for not fusing
  compute and storage.

### 3.1 Phasing

This document describes the target architecture, but it is built in phases. **Phase 1 (this
implementation effort) is BM25 lexical search only** — no vector index, no ANN, no fusion, no
rerank hook. Concretely, Phase 1 includes:

- The S3 segment format and manifest (§6), but each segment contains `doc_store.bin`,
  `inverted.idx`, and `filters.bin` only — `vector.idx` is not built or read.
- The full write path (§7): WAL, buffering, flush-to-segment, compaction.
- The full SSD caching layer (§9): read-through cache over segment files, exactly as described.
- The read path (§8), restricted to step 4's lexical branch only: postings intersection/union +
  BM25 scoring. Steps 4's semantic branch, step 5 (fusion), and step 6 (rerank hook) are stubbed
  as no-ops or simply omitted from the Phase 1 API surface.
- Structured filters and cursor pagination (single cursor, not the lexical/semantic pair) — a
  namespace running Phase 1 only ever has one retrieval mode, so `SearchCursor`'s dual-cursor
  shape collapses to one field.
- Multi-tenancy (§10), migration approach (§14), and operational model (§15) apply unchanged —
  none of them are vector-specific.

**Explicitly deferred to Phase 2**: `vector.idx`, ANN retrieval, RRF fusion, the rerank hook, and
embedding-dependent parts of the namespace schema (vector dimension/metric). The segment
`footer.json` and manifest format should still *reserve* the field for a vector index reference so
Phase 2 can add it without a segment format migration, but Phase 1 never writes or reads it.

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
- An **index profile**: tokenizer/analyzer choice, ANN index type + params (e.g. HNSW `M`/`efConstruction`,
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
    vector.idx           # ANN index (HNSW graph or IVF-PQ codebook + posting lists) — Phase 2, not written in Phase 1
    filters.bin          # columnar filter fields (dates, ids, tags) for fast predicate eval
    footer.json          # offsets, counts, schema version, checksum (reserves a vector.idx slot even in Phase 1)
```

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
   and synchronously appends to a WAL object in S3 (small, cheap PUTs) so the write survives node
   loss before it's part of a segment.
3. **Flush**: when the buffer crosses a size/time threshold (default: 512MB or 60s, whichever
   first), the ingest node builds inverted + vector + filter indices for the buffered documents,
   writes a new segment to S3, and publishes an updated manifest that adds the new segment.
4. **Compaction**: a background worker periodically merges small segments (and drops
   tombstoned/deleted docs) into fewer, larger ones, publishing a new manifest and marking old
   segments for GC after a grace period (bounded by the longest manifest a query node might still
   be holding).

Deletes and updates are tombstone-based (mark-and-compact), same as Lucene/ES — there is no
in-place mutation of a segment.

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
   - Semantic: ANN search over `vector.idx` (HNSW graph traversal or IVF-PQ probe), scored by
     cosine/dot-product — the direct replacement for OpenSearch's `script_score` cosineSimilarity
     query in `query_builder.py`.
5. **Fusion**: candidate sets from lexical and semantic retrieval are merged via Reciprocal Rank
   Fusion (configurable k), matching `ranking/rrf.py`'s existing behavior — ported in, not
   reinvented.
6. **Rerank hook**: an optional, pluggable cross-encoder rerank stage (Cohere today) runs on the
   fused top-N before results return — same role as `ChunkGenerator.__re_rank`.
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

- **Durability**: WAL append to S3 acks before the ingest RPC returns → data survives node loss
  immediately. Segment + manifest writes are the durable "committed" state for query visibility.
- **Read consistency**: each query snapshots one manifest version for its whole execution —
  read-your-writes is bounded by the flush interval (§7), not immediate, which matches the
  approach we already accept operationally with ES refresh tuning.
- **Manifest updates**: conditional (compare-and-swap) writes prevent lost updates from concurrent
  flush/compaction; a losing writer retries against the new base.
- **Garbage collection**: superseded segments are deleted only after the max manifest-staleness
  window elapses (i.e., after no query node could plausibly still be holding a reference to them).

## 12. API & Integration with Sage

Recommended split: **Kosha owns retrieval + fusion + cursor state; Sage keeps rerank, query
intent extraction (NER), and answer generation.** Rationale: fusion and cursoring are tightly
coupled to segment-level ranking internals Kosha controls; rerank (Cohere) and intent (NER) are
product-specific concerns that don't belong in a generalized search engine.

### 12.1 Transport: HTTP/JSON primary, gRPC secondary

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

The service is defined once (as a protobuf service, since protobuf's schema discipline is worth
keeping even for the HTTP surface) and exposed two ways: gRPC directly, and HTTP/JSON via
`google.api.http` annotations (the same transcoding approach used by grpc-gateway/Envoy), so both
transports share one source of truth for the request/response shapes below.

```protobuf
service Kosha {
  rpc IndexDocuments(IndexRequest) returns (IndexResponse) {
    option (google.api.http) = { post: "/v1/namespaces/{namespace_id}/documents" body: "*" };
  }
  rpc Search(SearchRequest) returns (SearchResponse) {
    option (google.api.http) = { post: "/v1/namespaces/{namespace_id}/search" body: "*" };
  }
  rpc CreateNamespace(NamespaceSpec) returns (Namespace) {
    option (google.api.http) = { post: "/v1/namespaces" body: "*" };
  }
  rpc GetNamespaceStats(NamespaceId) returns (NamespaceStats) {
    option (google.api.http) = { get: "/v1/namespaces/{namespace_id}/stats" };
  }
}

message SearchRequest {
  string namespace_id = 1;
  string query_text = 2;
  repeated float query_embedding = 3;
  RetrievalMode mode = 4;          // LEXICAL | SEMANTIC | HYBRID
  repeated Filter filters = 5;      // term/range/set predicates
  int32 max_results = 6;
  Cursor cursor = 7;
  bool include_highlights = 8;
}

message SearchResponse {
  repeated Hit hits = 1;
  Cursor next_cursor = 2;
}
```

Equivalent HTTP call for the sketch above:

```
POST /v1/namespaces/{namespace_id}/search
{
  "query_text": "breach of contract",
  "mode": "LEXICAL",
  "filters": [...],
  "max_results": 20,
  "cursor": null,
  "include_highlights": true
}
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
   existing `ENABLE_RRF_FUSION` flag) and whether a rerank hook is invoked are namespace/profile
   config, exposed as a webhook-style callback or left to the caller (per §12 recommendation, Sage
   owns rerank).

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

- **ANN index choice at scale**: HNSW is simple and fast to query but expensive to build/merge
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
  degrade gracefully (serve last-known-good manifest) if it's briefly unavailable.
- **Compaction cost/scheduling**: needs a concrete policy (size-tiered vs. leveled, à la LSM
  trees) and a cost model before it's clear compaction doesn't become the new "cluster rebalance"
  operational burden in disguise.
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
