# Kosha

[![CI](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml/badge.svg)](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml)
[![rustfmt](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml/badge.svg?job=fmt)](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml)
[![test](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml/badge.svg?job=test)](https://github.com/decover-tech/Kosha/actions/workflows/ci.yml)

A storage-disaggregated search engine: **S3 is the source of truth, local NVMe
SSD is a transparent cache, and compute nodes are disposable.** Kosha is being
built to replace Elasticsearch/OpenSearch and is intended to be reusable as a general-purpose, schema-driven search service.

See [DESIGN.md](DESIGN.md) for the full architecture.

> **Status: BM25 lexical, kNN/ANN vector and RRF hybrid search are implemented**,
> with filtering, aggregations, compaction, a Postgres control plane and a
> pluggable rerank hook. See [Supported features](#supported-features) and
> [Not supported yet](#not-supported-yet).

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
| kNN / ANN vector search | Cluster-and-posting on-disk vector index with centroid probing and triangle-inequality lower bounds; global probe budget; flat/HNSW fallback for older segments. Compaction merges the inputs' postings and repairs them with SPFresh's LIRE split / merge / reassign instead of re-clustering every vector (`KOSHA_VECTOR_COMPACTION=lire`, the default; `rebuild` re-clusters as before). Same on-disk format either way (#253) |
| Hybrid retrieval (RRF) | A lexical query (`query_text` / `wildcard` / `match_phrase` / `fuzzy`) plus a `knn` clause is fused with Reciprocal Rank Fusion, `score = Σ 1/(rank_constant + rank)`. Tune with `"rank": {"rrf": {"rank_window_size": 100, "rank_constant": 60}}` (Elasticsearch names and defaults: window = `max_results`, constant = 60; window must be ≥ `max_results`). Each retriever is ranked across the whole namespace before fusing: the lexical list is the best `rank_window_size` BM25 hits, the vector list the nearest `min(knn.k, rank_window_size)`. Vector-only hits are kept. Each hit returns the fused `score` plus `rank_details` (`lexical_rank` / `lexical_score` / `knn_rank` / `knn_score`). The fused list is at most `rank_window_size` long; `from` / `max_results`, `sort`, `collapse` and `search_after` (e.g. on `[_score desc, _id asc]`) apply within it. `total_hits` and aggregations count the lexical matches plus the vector window. Optional `"mode": "lexical" \| "semantic" \| "hybrid"` is checked against the clauses present |
| Filters | `term`, `terms`, `range` (`gte`/`gt`/`lte`/`lt`), `exists`, `prefix`, `bool` (`must` / `must_not` / `should` / `minimum_should_match`; OpenSearch's `filter` is accepted as `must`), `match_all`. Every clause composes inside `bool` and works in `filter`, `knn.filter` and `/delete`. An unknown or malformed clause is a 400, never a match-all. On a multi-valued field (a list, or a [nested path](#nested-attributes) with several values) a clause matches if ANY value does |
| `prefix` filter | `{"prefix": {"path": "/matters/2026/"}}`, or the long form `{"prefix": {"path": {"value": "/matters/2026/", "case_insensitive": true}}}`. Case-sensitive by default, like `term` (and unlike the `wildcard` query): `"ab"` matches `abc` and `ab`, not `a`, `xab` or `ABC`. A list value matches when any element does. The empty prefix matches exactly the documents `exists` matches. String fields only: a `prefix` on an `Integer`/`Float` field is a 400. Only `value` and `case_insensitive` are accepted in the long form. Evaluated by a scan of the segment's filter column (each distinct value once on a columnar segment), cached per segment; bloom filters cannot skip segments for it |
| Multiple vector columns | A namespace can hold several `Vector` fields (e.g. a 384-dim `title_emb` and a 1536-dim `body_emb`); each segment builds one index per field and `knn.field` picks the one searched. Each field has one dimension per namespace, fixed by the first vector written to it: a document whose vector disagrees is rejected with a 400, and so is a `knn` naming a field no segment indexes or a query vector of the wrong dimension. Segments written before this change keep serving any `knn.field` from their single index, unless their `doc_vectors.bin` names exactly one field (#204) |
| i8 vector quantization | `KOSHA_VECTOR_QUANTIZATION=i8` (or per field, `KOSHA_VECTOR_QUANTIZATION_FIELDS=body_emb:i8,title_emb:f32`) stores each segment's ANN posting entries as one f32 scale plus `dim` signed bytes instead of `dim` f32s: `vector.idx`, the bytes a cold kNN hydrates and the warm posting cache all shrink ~4x (measured 3.96x at 768 dims). The probe scores with an i8 dot product, then rescores its best `k * KOSHA_VECTOR_RESCORE_FACTOR` candidates (default 4, `0` disables) with the full-precision vectors in `doc_vectors.bin`, so final scores are exact cosine. f32 and i8 segments mix freely in a namespace. Default `f32`; see the rollout note below (#210) |
| Late-interaction (ColBERT-style) vectors | A `Vector` field whose value is a bag of token vectors (`[[f32; dim]; n]`) is indexed for MaxSim: `"knn": {"field": "tokens", "vectors": [[...], [...]], "k": 10}` scores `Σ over query tokens of max cosine over the document's tokens`. See [Late interaction](#late-interaction) |
| kNN-scoped filters | `knn.filter` restricts only the vector list; the top-level `filter` restricts both the lexical and the vector list |
| Filtered kNN | Filters are applied inside the probe, and a filtered kNN always returns `min(k, documents passing)` hits. When at most 50,000 documents pass namespace-wide (`KOSHA_KNN_EXACT_THRESHOLD`; `0` disables), every passing document's vector is scored exactly instead of probing centroids, so recall is 1.0. A segment where at most 1% of documents pass (`KOSHA_KNN_EXACT_FRACTION`) is searched exactly too. Otherwise the global probe budget is widened by the inverse of the filter's selectivity (at most 8x, `KOSHA_KNN_FILTER_BUDGET_MAX_SCALE`). The response reports `knn_mode` (`ann` / `exact` / `mixed`) and `knn_filtered_candidates` (documents passing the filter). Unfiltered kNN is unchanged and reports `ann` |
| Sorting | Multi-field `sort` over stored fields and [nested dotted paths](#nested-attributes), plus `_id`. A multi-valued field sorts by its smallest value for `asc` and its largest for `desc` (Elasticsearch's default `mode`) |
| Pagination | `from` / `max_results`, and an OpenSearch-style `search_after` cursor |
| Result diversification | `"limit_per": {"field": "author", "max": 2}` returns at most `max` hits per distinct value of a keyword field. The cap is applied while the result list is collected, so a page is filled from the next-best groups instead of coming back short; documents without the field are not capped. `collapse: {"field": ...}` is the one-per-group case (and groups missing values together); the two cannot be combined. `search_after` and `from` resume the same capped list, so no document repeats and a group spent on an earlier page is never refilled. `total_hits` and aggregations count matching documents before the cap (a pure kNN request reports its neighbour set, which is the diversified one). Works for BM25, kNN (diversity is found within the probed `num_candidates`) and hybrid, where each retriever's list is diversified before fusion and `rank_details` ranks are positions in those lists |
| Score modifiers | `rank_by` blends attributes into the score: `field_value` (`modifier(factor × value)`, Elasticsearch `field_value_factor` modifiers) and `decay` (`linear` / `exp` / `gauss` around an `origin`, with `scale`, `offset`, `decay`) over `Integer`, `Float`, `Date` and `GeoPoint` fields, each with a `weight`. Functions combine by `score_mode` (`sum` default, `multiply`, `avg`, `max`, `min`) and apply to the text score by `boost_mode` (`multiply` default: `text_weight × score × F`; `sum`: `text_weight × score + F`; `replace`: `F`). A decay's `origin` picks its axis: a number (numeric field), a date or `"now"` (scale like `"30d"`), or `{"lat", "lon"}` (scale like `"5km"`). Values come from the filter columns, never the doc store. Applied in a rescore window: the best `window_size` hits by the query's own score (BM25, fused RRF or vector; default `max(100, max_results)`, at most 10,000; a hybrid request rescores within its fused `rank_window_size` list, a kNN request within its `k` neighbours) are rescored and re-sorted, so block-max WAND keeps pruning on BM25 alone; hits below the window are not returned, `total_hits` still counts every match. Each hit reports `rank_details.base_score`. Missing values use the function's `missing` (default 0, which under `multiply` zeroes the hit). `collapse` / `limit_per` apply to the rescored order; `sort` must be by `_score`; `origin: "now"` responses are not cached. Example — halve the score every 30 days: `"rank_by": {"functions": [{"decay": {"field": "published", "function": "exp", "origin": "now", "scale": "30d", "decay": 0.5}}]}` |
| Geo | `GeoPoint` fields (`"lat,lon"`, `{"lat", "lon"}` or GeoJSON `[lon, lat]`; invalid points are a 400 at index time). `{"geo_distance": {"distance": "10km", "<field>": {"lat": .., "lon": ..}}}` filters by great-circle distance; a `decay` with a geo `origin` ranks by distance |
| Highlighting | Any number of text fields per request (including a [nested](#nested-attributes) dotted path such as `author.bio`), Elasticsearch-shaped: `"highlight": {"pre_tags": ["<em>"], "post_tags": ["</em>"], "fields": {"title": {}, "body": {"pre_tags": ["<mark>"], "post_tags": ["</mark>"]}}}`. Each field's tags fall back to the top-level `pre_tags` / `post_tags`, then to `<b>` / `</b>`. Hits return `"highlight": {"title": ["..."], "body": ["..."]}` (one fragment per field value, the whole value with the query's terms marked up); requested fields that a hit lacks or that are not `Text` are left out silently. The legacy single-field form `"highlight": {"field": "body"}` still works and still returns the flat `"highlights": [...]` list; that form and `highlights` are deprecated. Giving both `field` and `fields` is a 400. Fragment size/count options and other highlighter types are not supported (unknown options are ignored) |
| Aggregations | Buckets: `terms`, `composite`, `histogram`, `date_histogram`. Metrics: `cardinality`, `min`, `max`, `sum`, `avg`, `value_count`, top-level or nested under a bucket aggregation via `aggs`. Computed over every document the query and filter match (not just the page), from the filter columns only — see [Aggregations](#aggregations) |
| Total-hit accounting | Capped counting with an `eq` / `gte` relation (`track_total_hits`-style), overridable per query via `exact_total_hits` / `total_hits_cap` |
| Rerank | `"rerank": {"provider": "cohere", "model": "rerank-v3.5", "field": "content", "top_n": 50, "timeout_ms": 2000}` sends the top `top_n` hits (the fused list for a hybrid request) to an external reranker and pages the reordered list. `top_n` defaults to `from + max_results` and must be ≥ it (≤ 1000; ≤ `rank_window_size` when that is set); `timeout_ms` defaults to 2000. Each hit then has `score` = `rerank_score` (the provider's) and `fused_score` (the search-stage score). On timeout or provider error the hits keep search order and the response has `rerank_degraded: true` and `rerank_degraded_reason`; such responses are not cached. Not combinable with `sort` / `search_after`. Providers are configured on the server from the environment (`KOSHA_RERANK_PROVIDERS`, `KOSHA_RERANK_<NAME>_URL` / `_API_KEY` / `_MODEL`; `cohere` reads `COHERE_API_KEY`); any Cohere-style `POST /rerank` API works. Per-provider calls, outcomes, documents, billed units and a latency histogram are under `rerank` in admin `/stats` |
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
- Sub-aggregations (`aggs` or `aggregations` beside `terms`, `composite`,
  `histogram` or `date_histogram`) may only be metrics (`min`, `max`, `sum`, `avg`, `value_count`); their results
  appear inside each bucket under the sub-aggregation's name. Nesting a bucket
  aggregation, or putting `aggs` under a metric, is a 400.
- Partial results — bucket counts, and `(count, sum, min, max)` for metrics —
  are merged across segments before anything is finished, so `avg` is the
  merged sum over the merged count, not an average of per-segment averages.
- An aggregation-only request (`max_results: 0`) never reads the document
  store.
- Not supported yet: `stats`, `percentiles`, `range` buckets, `missing` /
  `script` / `format` options.

#### Histograms

```json
{"query_text": "", "max_results": 0, "filter": {"match_all": {}},
 "aggs": {
   "by_size":   {"histogram": {"field": "bytes", "interval": 1024}},
   "over_time": {"date_histogram": {"field": "sentAt", "fixed_interval": "1h"},
                 "aggs": {"total": {"sum": {"field": "amount"}}}}}}
```

```json
{"aggregations": {
   "by_size": {"buckets": [{"key": 0.0, "doc_count": 3}, {"key": 1024.0, "doc_count": 0},
                           {"key": 2048.0, "doc_count": 1}]},
   "over_time": {"buckets": [{"key": 1718409600000, "key_as_string": "2024-06-15T00:00:00.000Z",
                              "doc_count": 2, "total": {"value": 41.5}}]}}}
```

- A value `v` lands in the bucket keyed
  `floor((v - offset) / interval) * interval + offset`, so a value exactly on
  a boundary starts the higher bucket and negative values round down
  (`-0.5` with `interval: 10` is in bucket `-10`). `histogram` computes this
  in floating point (as Elasticsearch does); `date_histogram` in integer
  epoch milliseconds, all in UTC.
- `histogram` takes `field`, `interval` (a number > 0), optional `offset`
  (a number) and `min_doc_count`. It reads `Integer` and `Float` fields, and
  string values that are numbers or dates (as epoch milliseconds).
- `date_histogram` takes `field` and either `fixed_interval` — a positive
  whole number and one of `ms`, `s`, `m`, `h`, `d` (`"30s"`, `"5m"`, `"1h"`,
  `"7d"`) — or `calendar_interval`, limited to the units whose length is
  fixed in UTC: `1m`, `1h`, `1d` and `1w` (`minute`, `hour`, `day`, `week`;
  weeks start on Monday, as in Elasticsearch). `1M`, `1q` and `1y` are a 400
  that points at `fixed_interval` (#300). `offset` is a signed duration
  (`"+6h"`, `"-30m"`) or milliseconds. `Date` fields are read as epoch
  milliseconds; `Integer` fields are taken to already be epoch milliseconds.
- Anything else is a 400 with the reason: a malformed interval
  (`"1 banana"`, `"1.5h"`, `"1w"` as a fixed interval), a zero or negative
  interval, both or neither of `fixed_interval` / `calendar_interval`, the
  deprecated `interval` on `date_histogram`, and the not-yet-supported
  `time_zone` (#301), `extended_bounds` / `hard_bounds` (#302), `keyed`,
  `order`, `missing` and `format`.
- Buckets are sorted by key ascending. `key` is a number; `date_histogram`
  also returns `key_as_string` (RFC 3339, UTC, millisecond precision).
- **Empty buckets.** `min_doc_count` defaults to 0, as in Elasticsearch:
  every bucket between the lowest and the highest non-empty bucket is
  returned, empty ones with `doc_count: 0` (and their metric
  sub-aggregations `null`, or `0` for `sum` / `value_count`), so a time
  series has no missing days. No buckets are added before the first or after
  the last value (`extended_bounds`, #302). With `min_doc_count: n ≥ 1` only
  buckets holding at least `n` documents are returned. A histogram that
  would return more than 10,000 buckets, filled gaps included, is a 400 —
  use a larger interval, a narrower query, or `min_doc_count: 1`.
- A document counts once per bucket: a multi-valued field with two values in
  one bucket adds 1 to it, with values in two buckets adds 1 to each.
- Each segment buckets its own matches with the request's interval and
  offset, and the coordinator sums counts bucket by bucket, so results are
  the same however the documents are split into segments.

### Nested attributes

A `Keyword` field whose value is the JSON text of an object, or of an array
of objects, is a nested container. Filters, `sort`, aggregations and
`highlight` address its leaves by dotted path (`author.name`), and a hit
returns the field exactly as it was sent. The Python client sends a `dict`
this way automatically.

```json
{"id": "1", "fields": [
  {"name": "author",  "field_type": "Keyword", "value": "{\"name\":\"Ada\",\"org\":\"acme\",\"age\":36}"},
  {"name": "authors", "field_type": "Keyword", "value": "[{\"name\":\"Ada\",\"org\":\"acme\"},{\"name\":\"Bob\",\"org\":\"beta\"}]"},
  {"name": "tags",    "field_type": "Keyword", "value": "[\"a\",\"b\"]"}]}
```

```json
{"query_text": "", "filter": {"bool": {"must": [
   {"term": {"author.org": "acme"}}, {"range": {"author.age": {"gte": "30"}}}]}},
 "sort": [{"author.age": {"order": "desc"}}],
 "aggs": {"by_author": {"terms": {"field": "authors.name"}}}}
```

**Semantics are flattened, like Elasticsearch's `object` type, not
`nested`.** Each dotted path collects the leaves of every element of an
array, and the link between leaves of the same element is lost. With the
`authors` value above:

| Filter | Matches? | Why |
| --- | --- | --- |
| `authors.name = Ada` | yes | `authors.name` is `[Ada, Bob]` |
| `authors.name = Ada AND authors.org = acme` | yes | Ada is at acme |
| `authors.name = Ada AND authors.org = beta` | **yes** | `authors.org` is `[acme, beta]`; no author is both Ada and at beta, but the paths are matched independently |
| `authors.name = Cy` | no | no element has that name |

True nested queries, where each array element is matched as a unit, are not
supported. If you need "same element" matching, index each element as its
own document, or combine the two values into one leaf (`"name_org": "Ada|beta"`).

- Each leaf is indexed under its full path. Objects inside arrays, and arrays
  inside arrays, are flattened into the same path. `null`, `{}` and `[]` do
  not add a value.
- A path with several values is multi-valued. A filter matches if any value
  matches. A `terms` bucket counts a document once per distinct value, so a
  value repeated in one document still counts once. Metric aggregations
  (`sum`, `avg`, `value_count`...) see every value.
- Numbers go to the numeric (float) column, with one entry per value, so
  `range`, numeric `sort` and metrics work on them. Strings and booleans go to
  the keyword column. If several values are stored at one path, they are kept
  as their JSON-list text, the same convention `tags` above uses. If a path
  has both strings and numbers in one document, all of its values are stored
  as strings, so keep each path to one JSON type.
- Leaves are filter and sort values. They are not tokenized for full-text
  `query_text` search.
- Dotted paths are worked out each time a segment is built. A container
  indexed before this release can be addressed by dotted path once its
  segment is rebuilt by compaction or re-indexing. Flat documents are
  unaffected.
- The field names a query uses can be listed with
  `SearchQuery::field_paths()`. `validate_field_paths()` rejects a path that
  a namespace schema does not know. Until namespace schemas are enforced on
  search, unknown paths are allowed and match nothing.

### Indexing and writes

- Per-field inverted indexes (#249): besides the combined index of every `Text` field, each segment indexes each `Text` field on its own — postings, per-document lengths and statistics — for `fields` queries and field-restricted phrases. See [Per-field indexes](#per-field-indexes).

- Bulk document indexing, with field types `Text`, `Keyword`, `Integer`, `Float`, `Date`, `Boolean`, `Vector`, `GeoPoint`.
- `Text` values longer than 256 chars (`KOSHA_TEXT_FILTER_IGNORE_ABOVE`; `off` disables the cap) are full-text searchable and returned in hits, but are not copied into the filter columns, so `term`/`terms`/`range` filters, `sort`, and aggregations do not see them. This matches Elasticsearch's dynamic-mapping `keyword` `ignore_above: 256`. Use `Keyword` for a long value that must be exact-matched.
- Vector field values are stored once, as binary f32, in each segment's `doc_vectors.bin` instead of as JSON text in `doc_store.bin` (`KOSHA_VECTOR_STORE=sidecar`, the default). Hits and compaction still see the full vector, which comes back as the f32 values that were indexed, re-serialized as JSON. Its text can differ from what was sent: digits beyond f32 precision are lost, and so is number formatting. Set `KOSHA_VECTOR_STORE=inline` to keep the old layout. **Before rolling back to a release older than this change, set `inline` and compact:** older binaries can't read the sidecar, and an older binary compacting a sidecar segment drops its vectors.
- Each `Vector` field gets its own ANN index per segment (#204). A segment with one vector field is byte-identical to before. **A release older than this change reads only a segment's first vector column, for any `knn.field`, and fails to compact a segment whose columns differ in dimension:** don't roll back past it once documents with several vector fields have been flushed.
- i8 vector quantization (#210) is a writer setting: flushes and compactions written while it is on produce i8 indexes, and existing segments keep theirs until compaction rewrites them (from the f32 vectors in `doc_vectors.bin` or the doc store, never from the i8 copy). **Turn it on only after every query pod runs a release that reads i8 indexes**, since an older reader cannot read them and fails the kNN, and **turn it off and compact before rolling back** past that release. With it off, segments are byte-identical to before. Rescoring reads `doc_vectors.bin` where it is local; a segment stored with `KOSHA_VECTOR_STORE=inline`, or whose sidecar is not yet hydrated from S3, returns the i8 estimates (within ~0.01 of the exact cosine).
- Upsert by document id — segments holding a prior version of an id are rewritten.
- Delete by query (tombstone-based) and a document `exists` check.
- Write-ahead log for buffered documents, replayed on restart.
- Auto-flush at a configurable document threshold (`KOSHA_FLUSH_THRESHOLD`), plus an explicit `flush` that makes writes visible immediately. A buffer that stays below the threshold is flushed and published once it has been pending for `KOSHA_FLUSH_MAX_AGE_MS` (default 1000; `0` disables), similar to Elasticsearch's `refresh_interval`. The Python client flushes only on `refresh=True` / `"wait_for"` or `indices.refresh()`, not after every bulk batch.
- Size-tiered compaction with a cap on merged-segment size (5 GiB default). A background scheduler on the write pod runs a tiered pass on any namespace past the segment trigger every `KOSHA_COMPACTION_INTERVAL_MS` (default 30000; `0` disables), at most `KOSHA_COMPACTION_MAX_CONCURRENT` passes at a time (default 1). Every pass plans its merge group from S3 object sizes before downloading anything, hydrates only that group, and evicts the superseded inputs from local disk once the merge is published, so a pass needs about 2 x the cap of local disk rather than the whole namespace. A `full` pass loops capped groups inside one job until none fits; job results report `groups`, `bytes_hydrated`, `bytes_evicted` and `peak_local_bytes`. `POST /v1/admin/compact-namespace` also accepts `"async": true` and returns `202` with a job id; poll `GET /v1/admin/compaction-jobs/{id}`, or see scheduler state in `GET /v1/admin/compaction-status`. Building a segment (flush or merge) keeps at most `KOSHA_SEGMENT_BUILD_MEMORY_BYTES` (default 256 MiB) of postings in memory: stored documents stream to disk as they're added, and postings over the budget spill to sorted runs that are merged on disk when the segment is finished. That final merge streams each term's postings through the encoder, so a term that appears in every document doesn't need its posting list in memory, and compaction passes vectors through as f32. A merge's peak heap is about the budget plus ~35 B per merged document (doc-store offsets and field lengths) plus the vocabulary, not counting short filter values and vectors (#253).
- Segment garbage collection. Every publish records the segments and deletion generations it stops referencing in the manifest (`retired`). Each pod removes its local copies once `KOSHA_LOCAL_GC_GRACE` (default 5m) has passed and no in-flight search can still read them, checked every `KOSHA_LOCAL_GC_INTERVAL_MS` (default 60000; `0` disables). `POST /v1/admin/gc` `{"namespace"?, "dry_run": true}` lists (dry run, the default) or deletes the S3 objects no current manifest references: retired ones after `KOSHA_GC_GRACE` (default 1h), never-recorded orphans after `KOSHA_GC_ORPHAN_GRACE` (default 24h). A segment's `_COMMIT` is deleted before its data. Counters (`kosha_gc_bytes_reclaimed_total`, `kosha_gc_segments_deleted_total`, `kosha_retired_segments`, local equivalents) are under `gc` in `GET /v1/stats`. `k8s/base/cronjob-segment-gc.yaml` runs it daily, shipped suspended and in dry-run mode.

### Storage and caching

- S3 as the source of truth; any S3-compatible endpoint works (MinIO locally, path-style supported).
- Local NVMe read-through cache, size-bounded with LRU eviction.
- Lazy segment loading — the doc store, filter columns, and postings are read on demand. A cold query fetches only its result page's documents from S3: ranged GETs for those documents' byte spans in `doc_store.bin` (and `doc_vectors.bin`), with nearby spans coalesced into one GET (`KOSHA_DOC_SPAN_COALESCE_GAP_BYTES`, default 64 KiB). Fetched spans are kept in a per-segment sparse span cache that counts toward the cache budget, so a warm repeat makes no S3 request. A segment that has served `KOSHA_DOC_SPAN_PROMOTE_GETS` ranged GETs (default 64) or `KOSHA_DOC_SPAN_PROMOTE_BYTES` (default 16 MiB) is hydrated whole in the background and its spans are dropped. `KOSHA_DOC_STORE_READ_MODE=whole` restores whole-file fetches per page segment. The `search timing:` log line reports `page_gets`, `page_ranged_kb`, `page_whole_files` and `page_whole_mb`; `GET /stats` reports the totals under `doc_store_reads`. Postings are read the same way (#245): segments are written with raw (uncompressed) posting shards (`KOSHA_POSTINGS_RAW_BLOBS`, default `1`; the footer records `posting_blobs_raw`), so a cold query fetches only its terms' postings spans, positions included, in one batch of ranged GETs. A shard that has served `KOSHA_POSTING_SPAN_PROMOTE_GETS` ranged GETs (default 32) or `KOSHA_POSTING_SPAN_PROMOTE_BYTES` (default 8 MiB) is hydrated whole. kNN queries fetch only `vector.offsets` up front and then the probed postings of `vector.idx` by range. Older or compressed-shard segments, wildcard queries, and `KOSHA_POSTINGS_READ_MODE=whole` keep whole-file fetches. A cold BM25 query is 3 sequential S3 round trips (scaffolding: footers, `inverted.idx`, `doc_store.offsets`; postings; page documents), or 4 when the node must first fetch footers it has not cached (persisted manifests no longer carry them, #241). The `search timing:` line adds `s3_rounds`, `s3_bytes`, `postings_gets`, `postings_ranged_kb`, `postings_whole_files`, `postings_whole_mb`, `vector_gets`, `vector_ranged_kb` and `vector_whole_files`; `GET /stats` reports `posting_reads`. Raw shards cost some disk and S3 space: about 25% more for the shard files on a synthetic corpus, since postings are already varint/delta coded.
- A segment whose `doc_store.offsets` sidecar is missing from S3, or fails the reader's sanity check, is still searchable: the query pod fetches that segment's `doc_store.bin` instead, logs a WARN naming the segment, and counts it in `doc_store_offsets_fallbacks` on `GET /v1/stats`. Repair such segments with `POST /v1/admin/backfill-offset-tables` so cold queries stop paying for the full doc store.
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
- Namespace sharding with a fixed shard count set at creation (`POST /v1/namespaces/{ns}/shards`): documents route by id to N shard namespaces, each with its own manifest, segments, flushes and compaction; searches read all shards as one namespace. See [Namespace sharding](#namespace-sharding).
- Backpressure controls: max concurrent searches, search queue depth and timeout, hydration concurrency, admission timeout.
- `GET /healthz` (liveness), `GET /readyz` (readiness), `GET /v1/stats` and per-namespace stats.
- Admin endpoints: create API key, rebuild filter blooms, backfill offset tables, compact a namespace (sync or async), compaction job and scheduler status, warmup status, import a namespace.
- Schema migrations via `kosha-server migrate`.
- Namespace branching (copy-on-write). `POST /v1/namespaces/{ns}/branch` with `{"name": "<branch>"}` creates a new namespace whose manifest lists the parent's segments and deletion bitmaps where they already are, so it takes the same time for 1M documents as for 10 and copies nothing. Writes, deletes, upserts and compactions on the branch go to the branch's own prefix; the parent's files and query results do not change. `GET /v1/namespaces/{ns}/branches` lists a namespace's branches (`GET .../stats` on a branch shows its parent and how many segments it still shares). `DELETE /v1/namespaces/{ns}/branch` deletes a branch and removes only the objects in its own prefix that no remaining manifest lists: segments a branch of the branch still uses are kept. Only branches can be deleted this way. Admin rewrites (`rebuild-filter-blooms`, `backfill-offset-tables`) on a branch skip the segments it shares with its parent. Segment GC never deletes an object a branch still lists, and `DELETE /v1/namespaces/{ns}` refuses (409) a namespace whose segments a branch still shares: delete the branches first. A sharded namespace cannot be branched yet.

### API and clients

- HTTP/JSON `/v1` API (see [HTTP routes](#http-routes)) — namespace create and delete, `documents`, `search`, `flush`, `delete`, `exists`, `stats` — with the Phase 1 unversioned routes still served for backward compatibility.
- `proto/kosha/v1/kosha.proto` is the canonical API contract and the source for generated stubs and the OpenAPI spec.
- `kosha` CLI (built from `crates/kosha-cli`, not yet on crates.io): health, index, search, flush, delete, stats, admin commands, named profiles, `--json` output, and a `kosha curl` escape hatch.
- OpenSearch-compatible Python client, [`kosha-client` on PyPI](https://pypi.org/project/kosha-client/): `search`, `index`, `bulk`, `count`, `update`, `delete_by_query`, `update_by_query`, `scroll`, plus `indices` and `tasks` namespaces, translating the ES query DSL to Kosha's native shape.

### Not supported yet

- Per-retriever weights for RRF. Reranking over more than one field, and ES `text_similarity_reranker` retrievers in the Python client.
- Multi-field scoring other than a boosted sum (`dis_max` / `best_fields` tie-breakers, turbopuffer `Product` / `Max` of fields), field-name patterns in `fields`, and block-max WAND for `fields` queries.
- Analyzer features beyond the list in [Analyzers](#analyzers): per-language stemmers other than English, synonyms, n-grams, CJK dictionary segmentation (`standard@2` splits Han ideographs one per token), full Unicode case folding (`ß` and `SS` stay distinct). Other `FieldSchema` members (`type`, `indexed`, `stored`) are stored but not enforced — field types still come with each document.
- Fuzzy: block-max WAND over expansions (fuzzy queries score every posting of every expansion), highlighting of fuzzy matches, fuzzy inside `fields` / `match_phrase`, bloom pruning for two-edit tokens.
- gRPC — the service is defined in the proto, but HTTP/JSON is the only implemented transport.
- A namespace-level centroid index shared across segments and updated from the WAL (#253 step 2): each segment still carries its own postings, and a kNN probes every segment's centroids under the global budget.

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

## HTTP routes

Every `/v1/namespaces` route is scoped to the caller's tenant: `{ns}` is an
unqualified name, stored as `{tenant}/{ns}`, and another tenant's namespace of
the same name is invisible (404). Query pods (`KOSHA_INGEST_HOST` set) forward
the routes marked *write* to the ingest pod.

| Method and path | Body → response | Notes |
|---|---|---|
| `POST /v1/namespaces` | `CreateNamespaceRequest` → `CreateNamespaceResponse` | *write*. 409 if it exists, 400 for an invalid name or schema. Optional: namespaces still auto-create on first write. A `schema` is validated, echoed back and stored as the namespace schema, as `POST /v1/namespaces/{ns}/schema` would (see [Analyzers](#analyzers)). |
| `DELETE /v1/namespaces/{ns}` | → `DeleteNamespaceResponse` | *write*. 404 if it does not exist, 409 while a branch still shares its segments. Permanent; see below. |
| `POST /v1/namespaces/{ns}/documents` | `IndexDocumentsRequest` → `IndexDocumentsResponse` | *write*. Upsert by id. |
| `POST /v1/namespaces/{ns}/flush` | → `FlushResponse` | *write*. Make buffered writes searchable. |
| `POST /v1/namespaces/{ns}/delete` | `DeleteByQueryRequest` → `DeleteByQueryResponse` | *write*. Delete by query. |
| `POST /v1/namespaces/{ns}/branch`, `GET /v1/namespaces/{ns}/branches`, `DELETE /v1/namespaces/{ns}/branch` | `{"name": "<branch>"}` → the branch; → its branches; → what was reclaimed | Create (*write*), list and delete (*write*) copy-on-write branches (#208). 409 if the name is taken, or for deleting a namespace that is not a branch. |
| `POST /v1/namespaces/{ns}/shards` | `{"shards": N}` → `{"namespace", "shards", "changed"}` | *write*. Create the namespace sharded (#207); see [Namespace sharding](#namespace-sharding). |
| `POST /v1/namespaces/{ns}/schema`, `GET /v1/namespaces/{ns}/schema` | `NamespaceSchema` | Set (*write*) or read the namespace schema; see [Analyzers](#analyzers). |
| `POST /v1/namespaces/{ns}/search` | `SearchRequest` → `SearchResponse` | 404 for a missing namespace. |
| `POST /v1/namespaces/{ns}/exists` | ids → per-id existence | |
| `GET /v1/namespaces/{ns}/stats` | → `NamespaceStats` | 404 for a missing namespace. |
| `GET /v1/healthz`, `GET /healthz`, `GET /readyz` | | Unauthenticated probes. |
| `GET /v1/stats` | | Admin key only. |
| `/v1/admin/*` | | Admin key only; see the operations list above. |

**Deleting a namespace** (`DELETE /v1/namespaces/{ns}`) runs on the ingest pod
under the namespace's publication lock:

1. The control store drops the namespace and its manifest. It keeps the highest
   manifest version the namespace reached (`kosha.retired_namespaces` in
   Postgres), and a re-created namespace of the same name starts above it, so a
   query pod's caches, which are keyed by manifest version, can never serve the
   deleted data.
2. The writer discards the namespace's manifest, write buffer, pending deletes
   and every WAL record for it, so neither a later flush nor a restart brings the
   documents back. A group-commit WAL file that also holds other namespaces'
   records is kept until those flush, and the deletion is recorded durably in
   the WAL stream (`discarded`), so recovery skips the deleted namespace's
   records in it.
3. Its local segment directory and per-namespace caches are removed.
4. With S3 configured, every object under the namespace's prefix
   (`{KOSHA_S3_PREFIX}/{tenant}/{ns}/`) is deleted before the response returns.
   If that fails, the namespace is still deleted and the response says
   `"storage_reclaimed": false`; the leftover objects are unreferenced, and a
   re-created namespace never reuses their keys.

Query pods answer 404 for the namespace once they see the control store has no
manifest for it: within one manifest refresh (`KOSHA_MANIFEST_REFRESH_MS`,
default 500 ms), or at once for a search with `max_staleness_seconds: 0`. Their local disk copies of its segments are not removed
eagerly; the LRU cache (`KOSHA_CACHE_MAX_BYTES`) evicts them. Re-creating the
namespace, explicitly or by writing to it, starts it empty.

## Late interaction

ColBERT-style multi-vector retrieval (issue #205). Send a document's token embeddings as a
`Vector` field whose value is a JSON array of equal-length arrays, and query them with a bag of
query token embeddings:

```json
{"id": "p1", "fields": [{"name": "tokens", "field_type": "Vector",
                         "value": "[[0.12, -0.03, ...], [0.08, 0.41, ...], ...]"}]}

{"query_text": "", "max_results": 10,
 "knn": {"field": "tokens", "vectors": [[0.2, ...], [0.1, ...]], "k": 10, "num_candidates": 100}}
```

The score is `MaxSim(Q, D) = Σ_q max_d cosine(q, d)` (so it ranges up to the number of query
tokens). `vectors` implies `"scoring": "late_interaction"`; setting that with a single `vector`
searches with a one-token bag. `knn.filter`, the top-level `filter`, deletes and hybrid RRF work
as for single-vector kNN. A query whose tokens differ in dimension from the field's is a 400.

**How it runs.** Each segment keeps, per late-interaction field, `multivec.{i}.bags` (every
token, unit-normalized f32, with a per-document offsets table so one bag is one ranged read) and
`multivec.{i}.ivf` (the tokens clustered with the same balanced k-means as the single-vector
index; each posting holds its tokens' doc ids and int8 codes). For every query token the
`KOSHA_LATE_INTERACTION_NPROBE` (default 16) nearest postings are scored from the int8 codes,
the best `num_candidates` documents by that approximate MaxSim are re-scored exactly from their
bags, and the top `k` are kept. With enough probes and candidates the result is exact MaxSim
(tested against a brute-force reference). The source JSON stays in the doc store, so documents
read back as sent and compaction rebuilds the files from it; a binary older than this change
reads these segments normally, just without late-interaction hits, and its compactions drop the
`multivec` files until a newer binary compacts again.

**Cost** (`cargo bench -p kosha-query --bench late_interaction`, synthetic clustered corpus,
Apple M5). Per document, relative to its raw f32 tokens: `.bags` 1.00x, `.ivf` 0.28x, and the
source JSON in the doc store 2.55x, so about 4.8x the raw token bytes in all. With 128-dim
tokens, 64 per document, that is 32 KB raw, ~124 KB stored, against ~0.5 KB for one 128-dim
vector; the index build is k-means over every token (7.6 s for 8,000 such documents, 512k
tokens). Queries: 8,000 documents x 64 tokens, 32 query tokens: warm p50 15.3 ms, recall@10
against brute-force MaxSim 1.000 (single-vector kNN over mean-pooled tokens: 1.1 ms, but only
0.40 of the MaxSim top 10). 4,000 x 32 tokens at 64 dim, 16 query tokens: 1.9 ms. Single-vector
kNN is unchanged when the feature is unused (`knn_open_search` p50s identical to main).

## Namespace sharding

A namespace can be split into a fixed number of shards when it is created (issue #207). Before
writing any document, set the count:

```sh
curl -X POST "$KOSHA/v1/namespaces/contracts/shards" -H "x-api-key: $KEY" -d '{"shards": 8}'
# {"namespace":"contracts","shards":8,"changed":true}
```

- `2 <= shards <= 256`. `1` means an ordinary namespace and changes nothing. Repeating the call
  with the same count is a no-op; any other count, or any count on a namespace that already holds
  documents, is a 400 — resharding is not supported yet.
- Every write route (`documents`, `flush`, `delete`, `exists`, the legacy `/index`, `/replace`)
  keeps its shape. Each document goes to shard `jump_hash(fnv1a64(id), N)`, a function of the id
  bytes and N only, so an upsert or `replace` always finds the previous version in the same shard.
  A batch spanning shards is applied one shard after another; if one fails, retry the batch (writes
  are keyed by id, so the retry is idempotent).
- Each shard is a namespace of its own, `{tenant}/{ns}/_shard-0000` … `_shard-{N-1}`, with its own
  manifest row, segments in S3 under that prefix, WAL checkpoint, flushes and compaction. The root
  `{tenant}/{ns}` manifest records `"shards": N` and lists no segments. Shard namespaces are created
  in the control store when the count is set, inherit the namespace schema, and follow later
  `schema` changes.
- Searches read the shards as one namespace: one merged manifest over every shard's segments,
  scored with namespace-wide BM25 statistics, one top-K, one `search_after` order and one capped
  total-hit count, so results — scores, order, totals, aggregations, pagination — equal the same
  corpus in one unsharded namespace. Query pods follow every shard's manifest like any other.
- `GET /v1/namespaces/{ns}/stats` reports totals plus `shards` and per-shard `shard_stats`.
  `POST /v1/admin/compact-namespace` on the root runs one pass per shard; the other admin routes
  take a shard by its full name (`{tenant}/{ns}/_shard-0003`).
- `DELETE /v1/namespaces/{ns}` on the root deletes every shard with it (control store rows,
  write buffers, WAL records and files).

**Today's limits.** Scatter-gather runs inside one pod: every query pod still serves all shards of
a namespace, so sharding bounds per-manifest size, publish contention, flush and compaction units,
but not one pod's working set. Fanning shards out across pods, manual resharding and automatic
resharding are follow-ups. Namespaces created before this change carry no shard count and are
unchanged. A binary older than this change ignores the count: it sees the root as an empty
namespace, so don't roll back past it while sharded namespaces are in use.

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
Upload and manifest-store failures return errors and remain retryable. By
default (`KOSHA_WAL_MODE=local`) buffered writes use a local, fsynced WAL under
`data_dir/_wal`; they are not S3-durable until a successful flush, so losing the
node (not just the pod) loses acknowledged, unflushed writes.
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

### S3 WAL (`KOSHA_WAL_MODE=s3`)

With `KOSHA_WAL_MODE=s3` (needs the `s3` feature and `KOSHA_S3_BUCKET`), an
ingest node acknowledges `/index`, `/replace` and `/delete` only after the
group commit holding the write is stored in the segment bucket, so an
acknowledged write survives losing the node and its disk. Each group commit is
one object, `{KOSHA_S3_PREFIX}/_wal/{KOSHA_WAL_WRITER_ID}/{lsn:020}.bin`, with
the same checksummed v3 frames as the local WAL, created with a conditional
`PutObject` (`If-None-Match: *`). The stream's `meta/origin` and
`meta/sequence` objects sit next to it.

- **Writer id.** `KOSHA_WAL_WRITER_ID` (default `default`) names the stream.
  Every ingest process running at the same time needs its own id; a
  replacement for a lost node must reuse the lost node's id. Set the mode
  only where writes land: a node with `KOSHA_INGEST_HOST` forwards writes and
  keeps a local WAL. The stock
  single-replica, `Recreate` ingest Deployment can keep the default.
- **LSNs** are allocated per writer stream (per `wal_origin`), not per
  namespace: one object holds every namespace's records that arrived during
  the previous PUT. A manifest's `wal_checkpoint` is still the highest LSN of
  its `wal_origin` that its segments contain. The origin lives in the bucket,
  so a replacement node recognizes its predecessor's checkpoints.
- **One writer per stream.** Group `n + 1` is keyed by the LSN after group
  `n`'s last record. Two processes appending to one stream race for the same
  key; exactly one PUT succeeds. The loser fails that write (HTTP 500),
  rejects every later write, and must be restarted, which re-reads the stream.
  A PUT that timed out but landed is recognized by its bytes on the next PUT
  and kept (its records replay; they were never acknowledged). At startup the
  server checks that the store rejects a second conditional PUT of one key and
  refuses to start otherwise. MinIO and AWS S3 both support `If-None-Match`
  on `PutObject`. Conditional PUTs do not fence a writer that stays paused
  until the object it would collide with has been reclaimed; run one ingest
  process per writer id (the default Deployment's `Recreate` strategy does).
- **Timeouts.** Every WAL S3 call has a deadline of `KOSHA_WAL_S3_TIMEOUT_MS`
  (default 10000). A failed or timed-out PUT fails the write with a 500 and
  nothing is buffered.
- **Recovery.** On startup the node lists the stream, replays every record
  above each namespace's published checkpoint into its write buffers (the
  same path as a local restart) and continues after the highest LSN. A flush
  then builds segments as usual and publishes the manifest by CAS with the new
  `wal_checkpoint`; objects at or below it are deleted after the publication
  is acknowledged, as local files are.
- **Latency trade-off.** An acknowledgement now waits for an S3 PUT instead of
  an fsync. Writes to one namespace are serialized, so a single namespace
  writer gets one PUT round trip per write; concurrent writes to different
  namespaces share a PUT. Groups are written one at a time, so throughput is
  bounded by one PUT stream. `cargo bench -p kosha-write --bench s3_wal_ack`
  (500 kB writes, simulated PUT of 25 ms + 100 MB/s; not real S3):

  | writers | window ms | writes/s | MB/s | p50 ms | p99 ms | PUTs/write |
  |--------:|----------:|---------:|-----:|-------:|-------:|-----------:|
  | 1  | 0 | 26.4  | 13.6 | 38.1  | 38.5  | 1.00 |
  | 4  | 0 | 50.6  | 26.1 | 79.3  | 82.4  | 0.51 |
  | 4  | 2 | 55.6  | 28.6 | 78.9  | 95.2  | 0.41 |
  | 16 | 0 | 108.7 | 55.9 | 143.7 | 177.4 | 0.14 |

  turbopuffer reports p50 ≈ 165 ms for 500 kB writes. Measure on real S3
  before relying on these numbers.
- **Switching local → s3.** On the first start in s3 mode, a non-empty local
  `data_dir/_wal` is copied into the stream with its origin, sequence and
  file keys, so existing manifest checkpoints keep their meaning, and is then
  renamed to `_wal.migrated-{unix seconds}`. If the stream already belongs to
  another origin the node refuses to start: flush every namespace in one mode
  first.
- **Switching s3 → local.** Local mode does not read the S3 stream. Stop
  writes, flush every namespace (`POST /flush`) so the checkpoint covers every
  record, and check that `_wal/{writer id}/` holds no `.bin` objects before
  restarting with `KOSHA_WAL_MODE=local`. The local WAL then starts a new
  origin.

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
      kosha-core            shared types, data model, filter/query DSL, analyzers
      kosha-segment         segment format: inverted index, doc store, filter
                            columns, blooms, posting-based vector index
      kosha-vector-spfresh  vector clustering used to build vector.idx, plus an
                            incremental SPFresh/LIRE prototype
      kosha-write           WAL, per-namespace buffer, flush-to-segment, compaction
      kosha-cache           NVMe SSD read-through cache (DESIGN.md §9)
      kosha-query           BM25 + WAND, kNN/ANN, RRF fusion, filters, sorting,
                            highlighting, aggregations, segment cache
      kosha-control         namespace registry + manifest store (in-memory or
                            Postgres), hydration leases, SQL migrations
      kosha-server          HTTP /v1 API, auth, S3 upload/hydration, warmup,
                            compaction scheduler, `migrate`
      kosha-cli             remote `kosha` CLI (profiles, search, index, curl)
      kosha-build-segments  offline parallel segment builder
    clients/
      python/kosha_client   OpenSearch-compatible Python client
                            (spec → codegen → thin client)
      python/README.md      how the client is structured
    proto/
      buf.yaml              Buf module config
      kosha/v1/kosha.proto  canonical API contract (source of truth)
    gen/                    generated stubs + OpenAPI spec (git-ignored)
    tools/codegen/          code generation scripts and docs
    scripts/conformance/    end-to-end BM25 + filter correctness suite
    scripts/bench/          large-scale benchmark harness
    docs/                   configuration reference, development guides
    DESIGN.md               architecture document

[CONTRIBUTING.md](CONTRIBUTING.md#where-does-my-change-go) maps each crate to the kind of
change that belongs in it.

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

# Vector search (the example docs carry a 4-dimensional `embedding`)
kosha index -n quickstart-demo --file crates/kosha-cli/examples/vectors.jsonl
kosha flush -n quickstart-demo
kosha search -n quickstart-demo --knn-field embedding --knn-vector '[0.85,0.15,0,0.05]'
```

See [crates/kosha-cli/README.md](crates/kosha-cli/README.md) for profiles
(`~/.kosha/config.toml`), `--json` output, hybrid search / sort / cursor
flags, admin commands, shell completions, and the `kosha curl` escape hatch.

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

    cargo build                                                # build the workspace
    cargo test --all-features                                  # tests, incl. s3/postgres code (CI gate)
    cargo fmt --all -- --check                                 # formatting (CI gate)
    cargo clippy --all-targets --all-features -- -D warnings   # linting (CI gate)
    pre-commit install                                         # optional: run gates on commit

Every `KOSHA_*` environment variable the server reads, with its default and units, is listed
in [docs/configuration.md](docs/configuration.md). [CONTRIBUTING.md](CONTRIBUTING.md) covers
the full contributor workflow: crate map, feature flags, benchmarks and PR conventions.

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

Apache-2.0 (see [LICENSE](LICENSE)).
