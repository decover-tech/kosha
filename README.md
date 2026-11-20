# Kosha

[![CI](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml/badge.svg)](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml)
[![rustfmt](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml/badge.svg?job=fmt)](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml)
[![test](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml/badge.svg?job=test)](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml)

A storage-disaggregated search engine: **S3 is the source of truth, local NVMe
SSD is a transparent cache, and compute nodes are disposable.** Kosha is being
built to replace Elasticsearch/OpenSearch and is intended to be reusable as a general-purpose, schema-driven search service.

See [DESIGN.md](DESIGN.md) for the full architecture.

> **Status: Phase 1 complete — BM25 lexical + kNN/ANN search implemented.**
> All seven crates have functional BM25 indexing, query, filtering,
> aggregation, HNSW vector search, and RRF hybrid fusion. Rerank is Phase 2
> (DESIGN.md §3.1).

## Install

| Artifact | Where | Get it |
| --- | --- | --- |
| Server image | [Docker Hub — `ravidecoverai/kosha`](https://hub.docker.com/r/ravidecoverai/kosha) | `docker pull ravidecoverai/kosha:latest` |
| Python client | [PyPI — `kosha-client`](https://pypi.org/project/kosha-client/) | `pip install kosha-client` |
| CLI | not published to crates.io yet | `cargo install --path crates/kosha-cli` |

## Supported features

### Query

| Feature | Notes |
| --- | --- |
| BM25 lexical search | `k1` / `b` tunable per request: `bm25_params: {"k1": 2.0, "b": 0.3}` on `POST /search`, `k1=` / `b=` on `GET /search`. Either may be omitted and falls back to the segment's build-time value (1.2 / 0.75 unless built otherwise). `k1` must be ≥ 0 and `b` in [0, 1]; anything else (or non-numeric) is a 400. WAND pruning bounds are recomputed for the request's values, so results match exhaustive scoring. `k3` is not supported. IDF and `avgdl` are namespace-level (summed over all segments), so a document scores the same whichever segment holds it; `bm25_params: {"stats": "segment"}` / `bm25_stats=segment` (or `KOSHA_BM25_STATS=segment` server-wide) switches back to per-segment statistics for debugging. Wildcard queries use per-segment statistics |
| `operator: and` / `or` | AND (default) requires every query term; OR scores the union |
| Multi-field BM25 (`fields`) | `"fields": ["title^2", "content"]` restricts `query_text` to those `Text` fields (`GET /search?...&fields=title^2,content`). Each field has its own postings, document lengths and statistics (`N` = documents with the field, `avgdl` = its mean length, namespace-level like the combined index), and a hit scores `Σ boost × BM25_field` — Elasticsearch `multi_match` `most_fields`, turbopuffer `Sum` of per-attribute BM25. With AND every query term must match in at least one of the listed fields. `_all` names the combined index of every `Text` field and may be mixed in (`["title^3", "_all"]`). Each field reads the query with the analyzer it was indexed with. Boosts must be > 0; field patterns (`title*`) are not supported. Field queries take an exhaustive per-field path, not block-max WAND (~2 ms vs ~0.3 ms warm for a 10k-hit term on 100k docs, `cargo bench -p kosha-query --bench field_bm25`); rare terms cost the same. See [Per-field indexes](#per-field-indexes) for segments written before this existed |
| Block-max WAND | Skip-list postings, rarest-first probe ordering, leapfrog AND join; one top-k floor shared across segments, visited highest score ceiling first, with segments that cannot reach the page skipped |
| `match_phrase` | Positional postings, configurable `slop`. Matches only inside one field: `field` (or a `fields` list, any one of them) restricts it, and `_all` / `*` / no field means any single `Text` field. A phrase never spans two fields or two values of a multi-valued field, at any slop. A word a `stop` filter removed still counts as a position, so `"state of the art"` matches across the removed stopwords and `"state art"` does not |
| Configurable analyzers | Per-field `analyzer` in the namespace schema: `standard`, `standard@2` (Unicode), `keyword`, `lowercase`, `whitespace`, plus `lowercase` / `stop` / `stem` filters. See [Analyzers](#analyzers) |
| `wildcard` | Per-field pattern match, case-insensitive by default |
| `fuzzy` | Typo-tolerant term matching: `"fuzzy": {"field": "content", "value": "recieve", "fuzziness": 1}` finds `receive`. `fuzziness` is `0`, `1`, `2` or `"auto"` (default; Elasticsearch `AUTO:3,6`: exact below 3 characters, one edit up to 5, two from 6; `"auto:low,high"` moves the thresholds). An edit is an insertion, deletion, substitution or — with `transpositions` (default `true`) — a swap of adjacent characters. `prefix_length` (default 0) leading characters must match exactly and make expansion cheaper; `max_expansions` (default 50, at most 1024) caps the terms each token expands to per segment, nearest first, the exact term always kept. A non-exact match must leave a character of the shorter string untouched (`ox` never matches `o`). The value is analyzed with each segment's own analyzer and every token is expanded against that segment's sorted term dictionary (a Levenshtein-automaton walk that skips whole prefix ranges); several tokens combine with `operator` (AND by default). Scoring is tiered so **an exact match always outranks a one-edit match, which always outranks a two-edit one**: per token, `score = (2 − edits) + tf_norm` with `tf_norm ∈ (0, 1]` BM25's length-normalized tf saturation (namespace `avgdl`); no IDF, since expansions differ per segment. `field` (`""` / `_all` / `*` = every `Text` field) restricts it to one field's postings; segments written before per-field indexes answer from their combined index. Its own lexical query: not combinable with `query_text` / `fields` / `wildcard` / `match_phrase`, but can be the lexical leg of a hybrid request and takes `rank_by`. Segments are bloom-pruned for 0- and 1-edit tokens (see [Fuzzy bloom](#fuzzy-bloom)). Runs on an exhaustive path (no block-max WAND). On 1M docs x 30 words in 8 segments (1.08M distinct terms), warm: ~0.5–1 ms at 1 edit and ~3.5–4.5 ms at 2 edits for mid-frequency and rare words, ~10–14 ms when an expansion is a very frequent word (every posting is scored; the exact `query_text` WAND query takes 0.5 ms). The dictionary walk itself is ~0.2 ms per segment at 1 edit and ~2 ms at 2 edits (~0.3 ms with `prefix_length: 2`). Non-fuzzy queries are unchanged (`cargo bench -p kosha-query --bench fuzzy_expansion`) |
| kNN / ANN vector search | Cluster-and-posting on-disk vector index with centroid probing and triangle-inequality lower bounds; global probe budget; flat/HNSW fallback for older segments |
| Hybrid retrieval (RRF) | A lexical query (`query_text` / `wildcard` / `match_phrase` / `fuzzy`) plus a `knn` clause is fused with Reciprocal Rank Fusion, `score = Σ 1/(rank_constant + rank)`. Tune with `"rank": {"rrf": {"rank_window_size": 100, "rank_constant": 60}}` (Elasticsearch names and defaults: window = `max_results`, constant = 60; window must be ≥ `max_results`). Each retriever is ranked across the whole namespace before fusing: the lexical list is the best `rank_window_size` BM25 hits, the vector list the nearest `min(knn.k, rank_window_size)`. Vector-only hits are kept. Each hit returns the fused `score` plus `rank_details` (`lexical_rank` / `lexical_score` / `knn_rank` / `knn_score`). The fused list is at most `rank_window_size` long; `from` / `max_results`, `sort`, `collapse` and `search_after` (e.g. on `[_score desc, _id asc]`) apply within it. `total_hits` and aggregations count the lexical matches plus the vector window. Optional `"mode": "lexical" \| "semantic" \| "hybrid"` is checked against the clauses present |
| Filters | `term`, `terms`, `range` (`gte`/`gt`/`lte`/`lt`), `bool` (`must` / `must_not` / `should` / `minimum_should_match`), `match_all` |
| kNN-scoped filters | `knn.filter` restricts only the vector list; the top-level `filter` restricts both the lexical and the vector list |
| Sorting | Multi-field `sort` over stored fields, plus `_id` |
| Pagination | `from` / `max_results`, and an OpenSearch-style `search_after` cursor |
| Result diversification | `"limit_per": {"field": "author", "max": 2}` returns at most `max` hits per distinct value of a keyword field. The cap is applied while the result list is collected, so a page is filled from the next-best groups instead of coming back short; documents without the field are not capped. `collapse: {"field": ...}` is the one-per-group case (and groups missing values together); the two cannot be combined. `search_after` and `from` resume the same capped list, so no document repeats and a group spent on an earlier page is never refilled. `total_hits` and aggregations count matching documents before the cap (a pure kNN request reports its neighbour set, which is the diversified one). Works for BM25, kNN (diversity is found within the probed `num_candidates`) and hybrid, where each retriever's list is diversified before fusion and `rank_details` ranks are positions in those lists |
| Score modifiers | `rank_by` blends attributes into the score: `field_value` (`modifier(factor × value)`, Elasticsearch `field_value_factor` modifiers) and `decay` (`linear` / `exp` / `gauss` around an `origin`, with `scale`, `offset`, `decay`) over `Integer`, `Float`, `Date` and `GeoPoint` fields, each with a `weight`. Functions combine by `score_mode` (`sum` default, `multiply`, `avg`, `max`, `min`) and apply to the text score by `boost_mode` (`multiply` default: `text_weight × score × F`; `sum`: `text_weight × score + F`; `replace`: `F`). A decay's `origin` picks its axis: a number (numeric field), a date or `"now"` (scale like `"30d"`), or `{"lat", "lon"}` (scale like `"5km"`). Values come from the filter columns, never the doc store. Applied in a rescore window: the best `window_size` hits by the query's own score (BM25, fused RRF or vector; default `max(100, max_results)`, at most 10,000; a hybrid request rescores within its fused `rank_window_size` list, a kNN request within its `k` neighbours) are rescored and re-sorted, so block-max WAND keeps pruning on BM25 alone; hits below the window are not returned, `total_hits` still counts every match. Each hit reports `rank_details.base_score`. Missing values use the function's `missing` (default 0, which under `multiply` zeroes the hit). `collapse` / `limit_per` apply to the rescored order; `sort` must be by `_score`; `origin: "now"` responses are not cached. Example — halve the score every 30 days: `"rank_by": {"functions": [{"decay": {"field": "published", "function": "exp", "origin": "now", "scale": "30d", "decay": 0.5}}]}` |
| Geo | `GeoPoint` fields (`"lat,lon"`, `{"lat", "lon"}` or GeoJSON `[lon, lat]`; invalid points are a 400 at index time). `{"geo_distance": {"distance": "10km", "<field>": {"lat": .., "lon": ..}}}` filters by great-circle distance; a `decay` with a geo `origin` ranks by distance |
| Highlighting | Any number of text fields per request, Elasticsearch-shaped: `"highlight": {"pre_tags": ["<em>"], "post_tags": ["</em>"], "fields": {"title": {}, "body": {"pre_tags": ["<mark>"], "post_tags": ["</mark>"]}}}`. Each field's tags fall back to the top-level `pre_tags` / `post_tags`, then to `<b>` / `</b>`. Hits return `"highlight": {"title": ["..."], "body": ["..."]}` (one fragment per field value, the whole value with the query's terms marked up); requested fields that a hit lacks or that are not `Text` are left out silently. The legacy single-field form `"highlight": {"field": "body"}` still works and still returns the flat `"highlights": [...]` list; that form and `highlights` are deprecated. Giving both `field` and `fields` is a 400. Fragment size/count options and other highlighter types are not supported (unknown options are ignored) |
| Aggregations | Buckets: `terms`, `composite`. Metrics: `cardinality`, `min`, `max`, `sum`, `avg`, `value_count`, top-level or nested under a `terms` / `composite` bucket via `aggs`. Computed over every document the query and filter match (not just the page), from the filter columns only — see [Aggregations](#aggregations) |
| Total-hit accounting | Capped counting with an `eq` / `gte` relation (`track_total_hits`-style), overridable per query via `exact_total_hits` / `total_hits_cap` |
| Degradation signal | `knn_degraded_segments` reports segments whose vector search failed, so a 200 with silently missing neighbors is visible to the caller |

### Aggregations

Aggregations go in `aggs`, keyed by a name you choose, in Elasticsearch's
request and response shapes:

```json
{"query_text": "invoice", "max_results": 0,
 "aggs": {
   "avg_amount": {"avg": {"field": "amount"}},
   "latest":     {"max": {"field": "sentAt"}},
   "by_category": {"terms": {"field": "category", "size": 10},
                   "aggs": {"total": {"sum": {"field": "amount"}}}}}}
```

```json
{"aggregations": {
   "avg_amount": {"value": 41.5},
   "latest": {"value": 1718409600000.0, "value_as_string": "2024-06-15T00:00:00.000Z"},
   "by_category": {"buckets": [{"key": "travel", "doc_count": 12, "total": {"value": 498.0}}]}}}
```

- Each aggregation object has exactly one type key. An unknown or misspelled
  type (`stats`, `averag`) is a 400 that lists the supported types.
- `min`, `max`, `sum` and `avg` read `Integer` and `Float` fields, and `Date`
  fields as epoch milliseconds (`value_as_string` is added for `min` / `max` /
  `avg` over dates). Date text is parsed as `YYYY-MM-DD[THH:MM[:SS[.fff]]][Z|±HH:MM]`;
  a string value that is neither a date nor a number is skipped. A document
  without the field is skipped, never counted as 0.
- `value_count` counts values in a field of any type. A list value counts once
  per element, as a multi-valued field does in Elasticsearch, so it can exceed
  the number of documents. For a plain count of matching documents use
  `total_hits` (with `exact_total_hits: true` past the cap) or a bucket's
  `doc_count`.
- With no matching values, `min`, `max` and `avg` return `{"value": null}`;
  `sum` and `value_count` return `0`.
- Sub-aggregations (`aggs` or `aggregations` beside `terms` / `composite`) may
  only be metrics (`min`, `max`, `sum`, `avg`, `value_count`); their results
  appear inside each bucket under the sub-aggregation's name. Nesting a bucket
  aggregation, or putting `aggs` under a metric, is a 400.
- Partial results — bucket counts, and `(count, sum, min, max)` for metrics —
  are merged across segments before anything is finished, so `avg` is the
  merged sum over the merged count, not an average of per-segment averages.
- An aggregation-only request (`max_results: 0`) never reads the document
  store.
- Not supported yet: `stats`, `percentiles`, `histogram`, `date_histogram`,
  `range` buckets, `missing` / `script` / `format` options.

### Indexing and writes

- Per-field inverted indexes (#249): besides the combined index of every `Text` field, each segment indexes each `Text` field on its own — postings, per-document lengths and statistics — for `fields` queries and field-restricted phrases. See [Per-field indexes](#per-field-indexes).

- Bulk document indexing, with field types `Text`, `Keyword`, `Integer`, `Float`, `Date`, `Boolean`, `Vector`, `GeoPoint`.
- `Text` values longer than 256 chars (`KOSHA_TEXT_FILTER_IGNORE_ABOVE`; `off` disables the cap) are full-text searchable and returned in hits, but are not copied into the filter columns, so `term`/`terms`/`range` filters, `sort`, and aggregations do not see them. This matches Elasticsearch's dynamic-mapping `keyword` `ignore_above: 256`. Use `Keyword` for a long value that must be exact-matched.
- Vector field values are stored once, as binary f32, in each segment's `doc_vectors.bin` instead of as JSON text in `doc_store.bin` (`KOSHA_VECTOR_STORE=sidecar`, the default). Hits and compaction still see the full vector, which comes back as the f32 values that were indexed, re-serialized as JSON. Its text can differ from what was sent: digits beyond f32 precision are lost, and so is number formatting. Set `KOSHA_VECTOR_STORE=inline` to keep the old layout. **Before rolling back to a release older than this change, set `inline` and compact:** older binaries can't read the sidecar, and an older binary compacting a sidecar segment drops its vectors.
- Upsert by document id — segments holding a prior version of an id are rewritten.
- Delete by query (tombstone-based) and a document `exists` check.
- Write-ahead log for buffered documents, replayed on restart.
- Auto-flush at a configurable document threshold (`KOSHA_FLUSH_THRESHOLD`), plus an explicit `flush` that makes writes visible immediately. A buffer that stays below the threshold is flushed and published once it has been pending for `KOSHA_FLUSH_MAX_AGE_MS` (default 1000; `0` disables), similar to Elasticsearch's `refresh_interval`. The Python client flushes only on `refresh=True` / `"wait_for"` or `indices.refresh()`, not after every bulk batch.
- Size-tiered compaction with a cap on merged-segment size (5 GiB default). A background scheduler on the write pod runs a tiered pass on any namespace past the segment trigger every `KOSHA_COMPACTION_INTERVAL_MS` (default 30000; `0` disables), at most `KOSHA_COMPACTION_MAX_CONCURRENT` passes at a time (default 1). Every pass plans its merge group from S3 object sizes before downloading anything, hydrates only that group, and evicts the superseded inputs from local disk once the merge is published, so a pass needs about 2 x the cap of local disk rather than the whole namespace. A `full` pass loops capped groups inside one job until none fits; job results report `groups`, `bytes_hydrated`, `bytes_evicted` and `peak_local_bytes`. `POST /v1/admin/compact-namespace` also accepts `"async": true` and returns `202` with a job id; poll `GET /v1/admin/compaction-jobs/{id}`, or see scheduler state in `GET /v1/admin/compaction-status`. Building a segment (flush or merge) keeps at most `KOSHA_SEGMENT_BUILD_MEMORY_BYTES` (default 256 MiB) of postings in memory: stored documents stream to disk as they're added, and postings over the budget spill to sorted runs that are merged on disk when the segment is finished. That final merge streams each term's postings through the encoder, so a term that appears in every document doesn't need its posting list in memory, and compaction passes vectors through as f32. A merge's peak heap is about the budget plus ~35 B per merged document (doc-store offsets and field lengths) plus the vocabulary, not counting short filter values and vectors (#253).

### Storage and caching

- S3 as the source of truth; any S3-compatible endpoint works (MinIO locally, path-style supported).
- Local NVMe read-through cache, size-bounded with LRU eviction.
- Lazy segment loading — the doc store, filter columns, and postings are read on demand. A cold query fetches only its result page's documents from S3: ranged GETs for those documents' byte spans in `doc_store.bin` (and `doc_vectors.bin`), with nearby spans coalesced into one GET (`KOSHA_DOC_SPAN_COALESCE_GAP_BYTES`, default 64 KiB). Fetched spans are kept in a per-segment sparse span cache that counts toward the cache budget, so a warm repeat makes no S3 request. A segment that has served `KOSHA_DOC_SPAN_PROMOTE_GETS` ranged GETs (default 64) or `KOSHA_DOC_SPAN_PROMOTE_BYTES` (default 16 MiB) is hydrated whole in the background and its spans are dropped. `KOSHA_DOC_STORE_READ_MODE=whole` restores whole-file fetches per page segment. The `search timing:` log line reports `page_gets`, `page_ranged_kb`, `page_whole_files` and `page_whole_mb`; `GET /stats` reports the totals under `doc_store_reads`. Postings are read the same way (#245): segments are written with raw (uncompressed) posting shards (`KOSHA_POSTINGS_RAW_BLOBS`, default `1`; the footer records `posting_blobs_raw`), so a cold query fetches only its terms' postings spans, positions included, in one batch of ranged GETs. A shard that has served `KOSHA_POSTING_SPAN_PROMOTE_GETS` ranged GETs (default 32) or `KOSHA_POSTING_SPAN_PROMOTE_BYTES` (default 8 MiB) is hydrated whole. kNN queries fetch only `vector.offsets` up front and then the probed postings of `vector.idx` by range. Older or compressed-shard segments, wildcard queries, and `KOSHA_POSTINGS_READ_MODE=whole` keep whole-file fetches. A cold BM25 query is 3 sequential S3 round trips (scaffolding: footers, `inverted.idx`, `doc_store.offsets`; postings; page documents), or 4 when the node must first fetch footers it has not cached (persisted manifests no longer carry them, #241). The `search timing:` line adds `s3_rounds`, `s3_bytes`, `postings_gets`, `postings_ranged_kb`, `postings_whole_files`, `postings_whole_mb`, `vector_gets`, `vector_ranged_kb` and `vector_whole_files`; `GET /stats` reports `posting_reads`. Raw shards cost some disk and S3 space: about 25% more for the shard files on a synthetic corpus, since postings are already varint/delta coded.
- In-memory parsed-segment cache governed by a live-bytes ledger and an admission gate, with a per-request hydration byte budget and bounded hydration concurrency.
- Posting-blob presence cache, postings cache, and vector-postings cache.
- Whole-response result cache for `POST /search` (bypassable per query with `no_cache`).
- Cross-replica hydration leases in Postgres: one pod fetches a cold segment from S3, its peers stream the bytes from it over `GET /internal/segment/...` rather than stampeding S3.
- Bloom filters over terms and filter fields, so a segment that cannot match is skipped without being read.
- Columnar filter index (#244, `KOSHA_FILTERS_WRITE_VERSION=2`; default `1` keeps the row-oriented `filters.bin`): one file per filter column plus a small `filters.toc`, with dictionary + roaring-bitmap string columns, zone-mapped numeric columns, and per-column stats in the footer. A query node fetches only the TOC and the columns the query's filter, sort, collapse and aggregations name — about 3.6% of the filter bytes for a one-field filter in the `filtered_broad_query` bench. Columnar segments evaluate filters from term bitmaps, zone maps and presence bitmaps, and aggregate and sort from the column arrays: `term`/`terms`/`range` filters are 40x+ faster on a 1M-document segment (`filter_eval_1m` bench). Readers handle both formats; see DESIGN.md §6.2 for the rollout and rollback rules.
- Namespace warmup on boot, gated behind `/readyz` so traffic is not routed to a cold pod. Warming is usage-driven (#213): pods count reads per namespace, segment and file class, persist the hot set to `_warmset/<role>.json` in S3, and warm it at the next boot alongside the explicit `KOSHA_WARMUP_NAMESPACES`. Query pods poll the manifest version of hot namespaces (`KOSHA_REWARM_INTERVAL_MS`, default 5000) and fetch the segments a flush or compaction adds before the next query. `KOSHA_WARMUP_BUDGET_BYTES` caps what warming keeps local (hottest first); `GET /v1/admin/warmup-status` reports what was warmed and skipped. `KOSHA_WARMUP_USAGE=0` turns the usage-driven part off.

### Control plane and operations

- Namespace registry and manifest store, in-memory by default or Postgres-backed (`postgres` feature + `DATABASE_URL`), with compare-and-swap manifest publishes.
- Multi-tenancy: every API key maps to a tenant prefix that scopes all namespace access.
- Read/write split: query pods forward every mutating route to `KOSHA_INGEST_HOST` and serve reads locally. A background refresher on each query pod polls every namespace's manifest version in one control-store query every `KOSHA_MANIFEST_REFRESH_MS` (default 500; `0` disables it and the pod checks before every search instead) and swaps in manifests that advanced; searches already running finish on the snapshot they started with, and namespaces in `KOSHA_WARMUP_NAMESPACES` have the new version's scoring set fetched before the swap. Searches don't touch the control store unless they pass `max_staleness_seconds` (body field or query parameter) and the namespace was last confirmed current longer ago than that. If the control store is unreachable, query pods keep serving their last-known-good manifests and only a namespace the pod never loaded fails (503). `GET /v1/stats` on a query pod reports per-namespace `manifest_version`, `stored_manifest_version`, `last_refresh_at` (Unix seconds) and `manifest_lag_seconds`, plus a `manifest_refresh` block with error counters and the maximum lag.
- Backpressure controls: max concurrent searches, search queue depth and timeout, hydration concurrency, admission timeout.
- `GET /healthz` (liveness), `GET /readyz` (readiness), `GET /v1/stats` and per-namespace stats.
- Admin endpoints: create API key, rebuild filter blooms, backfill offset tables, compact a namespace (sync or async), compaction job and scheduler status, warmup status, import a namespace.
- Schema migrations via `kosha-server migrate`.

### API and clients

- HTTP/JSON `/v1` API — `documents`, `search`, `flush`, `delete`, `exists`, `stats` — with the Phase 1 unversioned routes still served for backward compatibility.
- `proto/kosha/v1/kosha.proto` is the canonical API contract and the source for generated stubs and the OpenAPI spec.
- `kosha` CLI (built from `crates/kosha-cli`, not yet on crates.io): health, index, search, flush, delete, stats, admin commands, named profiles, `--json` output, and a `kosha curl` escape hatch.
- OpenSearch-compatible Python client, [`kosha-client` on PyPI](https://pypi.org/project/kosha-client/): `search`, `index`, `bulk`, `count`, `update`, `delete_by_query`, `update_by_query`, `scroll`, plus `indices` and `tasks` namespaces, translating the ES query DSL to Kosha's native shape.

### Not supported yet

- Reranking (Phase 2, DESIGN.md §3.1). Per-retriever weights for RRF.
- Multi-field scoring other than a boosted sum (`dis_max` / `best_fields` tie-breakers, turbopuffer `Product` / `Max` of fields), field-name patterns in `fields`, and block-max WAND for `fields` queries.
- Analyzer features beyond the list in [Analyzers](#analyzers): per-language stemmers other than English, synonyms, n-grams, CJK dictionary segmentation (`standard@2` splits Han ideographs one per token), full Unicode case folding (`ß` and `SS` stay distinct). Other `FieldSchema` members (`type`, `indexed`, `stored`) are stored but not enforced — field types still come with each document.
- Fuzzy: block-max WAND over expansions (fuzzy queries score every posting of every expansion), highlighting of fuzzy matches, fuzzy inside `fields` / `match_phrase`, bloom pruning for two-edit tokens.
- gRPC — the service is defined in the proto, but HTTP/JSON is the only implemented transport.
- `kosha-vector-spfresh` (SPFresh/SPANN with LIRE rebalancing) is a standalone, benchmarked prototype; it is not wired into the segment format or the query path.

## Per-field indexes

Every segment has always held one **combined** inverted index: all `Text` fields of a document
tokenized together, with one length per document. It is unchanged and still answers every query
without `fields`, so default ranking is exactly what it was. Segments written since #249 add, in
the same `inverted.idx` and posting blobs:

- each field's own postings (key `\0{field}\0{term}`; no analyzer emits a NUL, so field keys
  never collide with combined terms) and a norms list holding each document's length in the field;
- `field_index` in `footer.json`: per-field `doc_count` / `total_length`, and the position gap.

Positions of consecutive `Text` values in a document are now 65,536 apart in both indexes, and
the phrase slop is clamped below the gap, so a phrase cannot match across fields.

**Existing namespaces keep working without reindexing.** A segment written before this change has
no `field_index`. A `fields` query or a field-restricted phrase is answered there from its combined
index (it matches the terms in any field, boosts ignored) — never with zero hits — and phrases
there can still span fields as before. Compaction rewrites such segments with per-field indexes.
Until then a namespace with both kinds scores them with different statistics.

**Cost.** On a two-field corpus (`title` ~6 words, `content` ~60) per-field indexes make the
inverted index 2.3x larger on disk and segment builds 1.5x slower (`cargo bench -p kosha-query
--bench field_bm25`). An open segment's term dictionary holds the field keys too: on the
one-field `segment_memory` corpus resident memory while open goes from 6.9 to 13.1 MiB and a cold
broad query from ~2 to ~5.6 ms (`cargo bench -p kosha-query --bench segment_memory`, with and
without `KOSHA_FIELD_INDEX=off`); warm queries are unchanged. `KOSHA_FIELD_INDEX=off` writes segments exactly as before (no field
postings, no position gap). **Before rolling back** to a binary older than this change, set it and
compact: an older binary ignores `field_index` but lists field keys among a segment's terms, so a
leading-`*` wildcard could match them.

## Fuzzy bloom

A fuzzy token can't be looked up in a segment's term bloom: its expansions are only known from the
dictionary. Testing every string within one edit would take hundreds of probes, and a 1%
false-positive bloom says "maybe" to at least one of them nearly every time. Segments written
since #201 therefore add `fuzzy_bloom` to `footer.json`: a bloom over every combined-index term and
every string one character deletion away from it. Two strings are within one edit only if these
neighbourhoods meet, so a one-edit token needs `len + 1` probes, and a segment that rejects all of
them for a required token is skipped without being fetched or opened. Zero-edit tokens are tested
against the ordinary term bloom; two-edit tokens are never pruned.

The neighbourhood is ~8x the vocabulary, so the bloom is only written for segments with at most
32,768 neighbourhood strings (about 4,000 terms, ≤ ~40 KB of footer); larger segments, and every
segment written before #201, have none and are always searched. `POST /v1/admin/rebuild-filter-blooms`
also writes it for existing small segments. Older binaries ignore the key.

## Analyzers

A namespace schema chooses how each `Text` field is analyzed (issues #220, #221). Set it with
`POST /v1/namespaces/{ns}/schema` (read it back with `GET`); the body is a proto `NamespaceSchema`:

```json
{"fields": {
   "title": {"analyzer": ["standard@2", "stop", "stem"]},
   "sku":   {"analyzer": ["keyword"]},
   "body":  {"analyzer": ["standard", "stop"], "stopwords": ["the", "a"]}},
 "default_analyzer": ["standard@2"]}
```

The first name picks the analyzer, every later name appends a filter:

| Analyzer | Tokens |
| --- | --- |
| `standard` (default) | Legacy tokenizer: split on whitespace, trim ASCII punctuation from each word's edges, lowercase. `hello,world` is one token |
| `standard@2` | Unicode: UAX #29 word boundaries (`unicode-segmentation`), also split at `.`/`:` between letters, lowercase. `hello,world` → `hello` `world`; `naïve.café` → `naïve` `café`; `3.14` stays whole; Han ideographs are one token each |
| `keyword` | The whole value, untouched — one token, case-sensitive |
| `lowercase` | The whole value, lowercased — one token |
| `whitespace` | Split on whitespace; case and punctuation kept |

| Filter | Effect |
| --- | --- |
| `lowercase` | Unicode lowercase |
| `stop` | Drop stopwords: Lucene's 33-word English list, or the field's `stopwords` (`default_stopwords` for `default_analyzer`). Applied to the query too; a query of only stopwords matches nothing |
| `stem` | English Snowball stemmer (`rust-stemmers`): `running`, `runs` and `run` all match each other |

`default_analyzer` covers every `Text` field the schema does not list (clients send every JSON
string as `Text`). The combined index (a query without `fields`) holds every text field's terms,
so a query matches a document if it matches one analysis of the query across its fields — e.g. a
stemmed `title` term and an unstemmed `body` term together. A `fields` query reads each listed
field with that field's own chain.

**Compatibility.** Each segment records the chains it was written with in its footer, and every
query is analyzed per segment with those chains, so changing the schema (or the tokenizer version)
never makes existing segments return zero hits: they answer as they were written until compaction
rewrites them, re-analyzing their stored documents under the current schema. An invalid schema is
rejected with a 400 and changes nothing.

**Why `standard` still means the legacy tokenizer.** The Unicode tokenizer is opt-in
(`standard@2`), not the new default, for three reasons: segments written with no schema stay
byte-identical to every earlier release; old segments' terms (`hello,world`) are only reachable
by the legacy reading anyway; and rollback stays safe. A binary older than this change ignores the
footer `analysis` field and analyzes every query the legacy way, so after **rolling back** it,
segments written with any non-default analyzer return wrong or missing hits for affected terms
until they are rewritten. Before rolling back past this change, remove the schema (`POST` one with
no analyzers) and run a full compaction (`POST /v1/admin/compact-namespace`) so every segment is
legacy again. Flipping the default to `standard@2` later only needs a new default here: old
segments keep their recorded version.

Throughput on the repository's Markdown (16 MB, ~2 KB documents; `cargo bench -p kosha-segment
--bench tokenizer`, Apple M5): `standard` 249 MB/s, `standard@2` 147 MB/s (0.59x),
`standard@2` + `stop` + `stem` 41 MB/s; whole segment build 61 / 50 / 28 MB/s (`standard@2`
costs 0.82x of a legacy build; the Snowball stemmer is most of the rest). All-ASCII words take a
hand-written UAX #29 fast path checked against `unicode-segmentation` by a differential test.

## Authorization and durability

All client namespace routes, including legacy `/index`, `/search`, `/flush`,
`/replace`, `/exists`, and `/delete`, resolve an **unqualified namespace name**
inside the authenticated tenant. Client-supplied paths and qualified names are
rejected. Legacy callers that previously accessed bare stored namespaces need
an explicit migration to tenant-scoped namespaces; this change does not rename
existing data automatically.

- Set `KOSHA_ADMIN_API_KEY` for `/v1/admin/*` and global `/stats` access.
- Set a separate `KOSHA_INTERNAL_API_KEY` on every peer participating in segment
  hydration. Tenant API keys do not authorize admin or peer-file access.
- Both service capabilities are disabled when their respective secret is unset.
  Ordinary API-key configuration remains `KOSHA_API_KEY` / `KOSHA_API_KEYS` or
  the Postgres key registry. Namespace-scoped stats remain available to tenants.
- The server refuses to start when no credential is configured at all, unless
  `KOSHA_AUTH=disabled` is set explicitly (local development only). Postgres keys
  are re-read every `KOSHA_API_KEY_REFRESH_SECS` (default 30), request bodies are
  capped at `KOSHA_MAX_BODY_BYTES` (default 64 MiB, 413 above it), and
  `KOSHA_LEGACY_ROUTES=off` turns the unversioned routes into 410s.

A successful flush uploads its segment snapshot before publishing its manifest.
Upload and manifest-store failures return errors and remain retryable. Buffered
writes use a local, fsynced WAL; they are not S3-durable until a successful flush.
Concurrent appends are group-committed: one WAL file and one fsync cover every
record queued while the previous commit ran, and each caller is acknowledged
only after the fsync covering its own record. Once the previous group held more
than one record, the committer waits up to `KOSHA_WAL_GROUP_COMMIT_MS` (default
2; `0` disables the wait) for more, and cuts a group at
`KOSHA_WAL_GROUP_COMMIT_BYTES` (default 4 MiB). A lone writer never waits.
Every record has its own log sequence number. WAL records are reclaimed only
through the published namespace checkpoint: a file shared by several namespaces
is deleted once each of them has a published checkpoint past its records there,
so a flush never removes later writes or another namespace's records. Partial
parallel flush failures restore the entire snapshot for retry.

Publication uses compare-and-swap against the writer's last committed manifest
version. Concurrent writers cannot replace each other's snapshots. A stale
writer reads the newer manifest back (resolving its deletion sidecars), rebases
its unpublished delta onto it and retries the CAS, up to 3 times with jittered
backoff. The delta is the segments it added, the segments it compacted away and
its new deletes; deletes on a segment both sides list are merged, and the
published WAL checkpoint stays the rebasing writer's own. A writer with nothing
unpublished just adopts the newer manifest. The rebase aborts, leaving local
state and the WAL untouched, when merging would lose data: both writers
compacted the same segment, the other writer deleted documents in a segment this
writer compacted away, or the other writer compacted away a segment that carries
this writer's unpublished deletes. An unresolved conflict returns HTTP 409.
Restarting the writer with its existing data directory loads the published
manifest and replays its pending writes from the WAL. Retrying an identical
committed snapshot is safe after a lost commit response, and a rebase
recognizes such a commit as its own. Segment object keys include a random
writer identifier to avoid collisions before publication. Migration
(`kosha-server migrate`) and `POST /v1/admin/import-namespace` also publish by
CAS against the version they read. They never rebase, and they fail instead of
overwriting a manifest another writer committed (409 for import).

Each WAL directory has a persistent origin identifier. New manifest checkpoints
apply only to that origin, so another writer's checkpoint cannot discard local
pending writes during recovery. Preserve the entire WAL directory, including
its `origin` file, across restarts. Legacy checkpoints without an origin retain
their previous recovery behavior; upgrade all writers before concurrent use.

Deletes include buffered documents and WAL-log resolved document IDs before
applying them. New WAL files hold one or more checksummed v3 frames (the CRC
covers the record's sequence number and payload); vectors are stored as binary
f32 rather than JSON text. Recovery keeps every complete frame of a torn final
file, discards only the incomplete tail, and rejects corrupt records. v1 and v2
files remain readable.
Deletes and writes replay in order, so reinserting a deleted ID remains valid.

Deletion sets are immutable `deletes-{sha256}.roaring` sidecars referenced by the
manifest. Publication uploads these sidecars before CAS, including new generations
for segments uploaded earlier. Query pods load and validate the referenced
bitmaps before installing refreshed manifests; a missing or corrupt bitmap fails
the request. Result caching includes deletion generations, and WAND filters
removed documents before counting or scoring them.

Upserts retain old segments, mark prior copies deleted, and write only incoming
documents into a new segment. Partial replacements read the affected document
spans and merge fields. Segment I/O runs outside the namespace state lock; the
new segment and deletion markers become visible together. Compaction reclaims
these dead copies later. Superseded files remain available to older snapshots;
S3 garbage collection is still deferred.

**Upgrade note:** Postgres `segments_json` now stores the complete manifest
object, including tombstones and checkpoints. New servers can read the old entry
arrays. Old servers cannot read the new object format: upgrade ingest and query
roles together before resuming writes, and do not roll back readers without a
manifest-format migration. Servers predating deletion-vector support ignore
those references, and older WAL readers cannot replay v2 or v3 frames: upgrade
all roles together before accepting writes and preserve the WAL directory. Rolling
back requires converting both formats. The SQL column needs no schema change.

## Repository layout

    crates/
      kosha-core      shared types, data model, filter/query DSL  — Epic 2
      kosha-segment   segment format: inverted idx, doc store,    — Epic 2
                     filter columns, vector store, HNSW graph
      kosha-write     document buffer + flush-to-segment          — Epic 3
      kosha-cache     NVMe SSD read-through cache (§9)            — Epic 4
      kosha-query     BM25 scorer, kNN/ANN search, aggregations,  — Epic 5
                     wildcard, match phrase, filtering
      kosha-control   in-memory namespace + manifest store        — Epic 6
      kosha-server    HTTP API (healthz, index, search, stats,    — Epic 8
                     delete, flush)
      kosha-cli       Remote `kosha` CLI (profiles, search,       — Epic 12
                     index, curl escape hatch)
    clients/
      python/kosha_client  OpenSearch-compatible Python client    — Epic 11
                           (spec → codegen → thin client)
      python/README.md     how the client is structured
    proto/
      buf.yaml             Buf module config
      kosha/v1/kosha.proto  Canonical API contract (source of truth) — Epic 1
    gen/                   Generated stubs + OpenAPI spec (git-ignored)
    tools/codegen/         Code generation scripts and docs
    docs/                  Development and integration guides
    DESIGN.md              Architecture document (v1 draft)

## Quickstart (test as a customer would)

```bash
# 1. Start Kosha locally
docker compose up --build

# 2. Install the CLI (or use cargo run -p kosha-cli -- …)
cargo install --path crates/kosha-cli

# 3. In another terminal, explore the index
export KOSHA_HOST=http://localhost:8080
export KOSHA_API_KEY=sk-kosha-dev

kosha health
kosha index -n quickstart-demo --file crates/kosha-cli/examples/docs.jsonl
kosha flush -n quickstart-demo
kosha search -n quickstart-demo "breach"
kosha stats -n quickstart-demo
```

See [crates/kosha-cli/README.md](crates/kosha-cli/README.md) for profiles
(`~/.kosha/config.toml`), `--json` output, and the `kosha curl` escape hatch.

The Python client ([`kosha-client` on PyPI](https://pypi.org/project/kosha-client/))
still works the same way for application code:

```bash
pip install kosha-client
```

```python
from kosha_client import KoshaClient

client = KoshaClient(
    hosts="https://app.kosha.io",
    api_key="sk-acme-corp-xxx",
)
client.ping()  # True
```

Both the CLI and the Python client respect `KOSHA_HOST` and `KOSHA_API_KEY`.
`scripts/quickstart.py` remains as a scripted alternative to the CLI flow.

## Development

Prerequisites: Rust stable (via [rustup](https://rustup.rs) — the repo pins the
channel via `rust-toolchain.toml`), Docker, and optionally `pre-commit`.

    cargo build                                # build the workspace
    cargo test                                 # run unit tests
    cargo fmt --all -- --check                 # formatting (CI gate)
    cargo clippy --all-targets -- -D warnings  # linting (CI gate)
    pre-commit install                         # optional: run gates on commit

The Postgres CAS integration test races independent writers and checks retries.
Run it against a dedicated disposable database:

    KOSHA_TEST_DATABASE_URL=postgres://... cargo test -p kosha-control --features postgres --test postgres_cas -- --ignored

To measure single-document upsert write amplification against a real 500 MiB
segment (including its stored filter payload):

    cargo bench -p kosha-write --bench upsert_write_amplification

### Local S3 (MinIO)

    docker compose up -d minio createbuckets

MinIO serves an S3-compatible API on `localhost:9000` (web console on `:9001`,
credentials `kosha` / `kosha-dev-secret`) with the `dsearch-dev` bucket
auto-created.

### Run with Docker

Pre-built images are published to Docker Hub at
[`ravidecoverai/kosha`](https://hub.docker.com/r/ravidecoverai/kosha) — `:main`
on every merge to `main`, and `:latest` plus the semver tags (`:0.1.0`, `:0.1`)
for every `v*` tag. Tagged releases are multi-arch (amd64 + arm64); `:main` is
amd64 only.

    docker pull ravidecoverai/kosha:latest
    docker run --rm -p 8080:8080 ravidecoverai/kosha:latest
    curl localhost:8080/healthz   # -> ok

Or build locally:

    docker build -t kosha:latest .
    docker run --rm -p 8080:8080 kosha:latest

Or bring up the whole local stack (MinIO + server): `docker compose up --build`.

### Run alongside the Decover backend (Tilt)

The backend repo's Tilt setup builds and deploys `kosha` into the local k8s
cluster as `kosha-service` (`:8080` HTTP, `:50051` gRPC). See
[docs/local-development.md](docs/local-development.md) — including how the
OpenSearch → Kosha swap will work once the read/write path lands.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache-2.0 (see [LICENSE](LICENSE)). The final open-source license choice is
confirmed before the v0.1 release (implementation plan step 126).
