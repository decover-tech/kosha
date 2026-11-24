# OpenSearch / Elasticsearch compatibility

<!-- GENERATED FILE: do not edit by hand. Source: scripts/conformance/dsl/cases.yaml.
     Regenerate with `make compat-matrix`; CI fails when this file is stale. -->

**What this describes:** kosha-client (Python) and the native `/v1` API. Kosha has no
server-side OpenSearch endpoint (`_search`, `_bulk`). `KoshaClient` (`clients/python`)
translates OpenSearch and Elasticsearch request bodies into native `/v1` requests, and this
matrix says what that translation does with each construct. The TypeScript and Go SDKs send
native `/v1` bodies only; they do not translate the DSL, so this matrix does not apply to them.

Every row is a case in [`scripts/conformance/dsl/cases.yaml`](../../scripts/conformance/dsl/cases.yaml).
The conformance workflow translates each case with the client, sends it to a live server
built from the same commit, checks the outcome below (and, where the reference model covers
it, the exact ids, totals and BM25 scores), and fails if this page is out of date.
Machine-readable copy: [`opensearch.json`](opensearch.json).

| Status | Meaning |
| --- | --- |
| supported | Translated, and answered as Elasticsearch answers it: the same matching documents, with Kosha's BM25 ranking. |
| partial | Runs, with the divergence stated in the row. |
| rejected | Refused with HTTP 400 `unsupported by Kosha: ...` before anything is sent (*client*), or refused by the server (*server*). A clause is never dropped silently except where a partial row says so. |

| Checked | How CI checks the row |
| --- | --- |
| exact | Run against a live server; ids, total hits and BM25 scores equal the reference model (`scripts/conformance/oracle.py`) exactly. |
| live | Run against a live server, with the row's own checks (the translated body, hit counts, response fields, the divergence where it can be observed). |
| client | Checked in the client only: refusals happen before anything is sent; other rows need something CI does not run, such as an embedding model. |

Migrating from Elasticsearch or OpenSearch: see [the migration guide](../migration/from-elasticsearch.md).

## Summary

| Area | Supported | Partial | Rejected |
| --- | ---: | ---: | ---: |
| [Query DSL](#query-dsl) | 10 | 6 | 13 |
| [Filters (bool filter context)](#filters-bool-filter-context) | 10 | 1 | 3 |
| [Aggregations](#aggregations) | 7 | 1 | 3 |
| [Sort](#sort) | 2 | 2 | 0 |
| [Paging and totals](#paging-and-totals) | 3 | 0 | 0 |
| [Top-level search body keys](#top-level-search-body-keys) | 7 | 4 | 0 |
| [Index and document APIs](#index-and-document-apis) | 9 | 9 | 2 |

## Query DSL

| Construct | Status | Notes | Checked | Case |
| --- | --- | --- | --- | --- |
| match_all alone, or no query | partial | Inside a bool next to other clauses it adds nothing, as in Elasticsearch. **Divergence:** On its own (and likewise a body with no `query`) it matches no documents, so aggregations over it are empty too: the translator drops it, and a Kosha search with neither query text nor a filter matches nothing. Write it as a filter, `{"bool": {"filter": [{"match_all": {}}]}}`, to match every document. | live | `query.match_all` |
| match_all as a bool filter (match every document) | supported | Matches every document; the way to list or aggregate over a whole index. | exact | `query.match_all.as_filter` |
| match | supported | The field restricts the search (Kosha `fields`). `operator` defaults to `or`, as in Elasticsearch. | live | `query.match` |
| match with operator and | supported | Every term must match. | live | `query.match.operator_and` |
| match with boost | supported | The same text in several fields is one Kosha query over those fields, each keeping its boost (`title^3`). | live | `query.match.boost` |
| match with fuzziness, minimum_should_match, analyzer, ... | rejected (client) | Only `query`, `operator` and `boost` are understood. Any other option is refused rather than ignored. | client | `query.match.unsupported_options` |
| multi_match (best_fields, most_fields, cross_fields) | partial | Field boosts are kept. **Divergence:** Every type is scored as `most_fields`: the sum of each field's boosted BM25. The matching documents are the same; the ranking differs from `best_fields` (the default) and `cross_fields`. | live | `query.multi_match` |
| multi_match over every field (`*`, `_all`) | supported | Searches every Text field. | exact | `query.multi_match.all_fields` |
| multi_match type phrase, phrase_prefix, bool_prefix | rejected (client) | Use `match_phrase` per field instead. | client | `query.multi_match.phrase_type` |
| match_phrase (with slop) | supported | The same phrase in several fields (a bool should) searches any of them. | live | `query.match_phrase` |
| two different phrases in one query | rejected (client) | A Kosha query holds one phrase. | client | `query.match_phrase.two_phrases` |
| wildcard | partial | One wildcard per query, with `*` and `?`. **Divergence:** The field is not applied: the pattern matches terms of every Text field, and keyword fields are never matched (this case names the keyword `status` and matches body text). Case-insensitive unless the clause sets `case_insensitive: false`; Elasticsearch is case-sensitive by default. Known engine bug: the part of a pattern before its first `*` is not anchored, so `op*` also matches `co-op`. | live | `query.wildcard` |
| knn (vector, k, num_candidates, filter) | supported | The filter runs inside the vector search, so a filtered search still returns up to k hits. | live | `query.knn` |
| knn with query_text (server-side embedding) | supported | The server embeds the text with the model the namespace schema declares for the field (native embedding). *Not run live: needs an embedding model configured on the server.* | client | `query.knn.query_text` |
| function_score | partial | The inner query is translated as usual. **Divergence:** `functions`, `boost_mode` and `score_mode` are ignored: hits are the inner query's, ranked by plain BM25. | exact | `query.function_score` |
| bool with must and filter | supported | Required text plus a filter tree. | exact | `query.bool` |
| bool should next to must (score-only clauses) | partial | Optional clauses never change which documents match, in Elasticsearch or in Kosha. **Divergence:** The optional clauses are dropped, so they do not raise the score of the documents that also match them. | exact | `query.bool.optional_should` |
| bool should over text (at least one) | supported | One Kosha query with operator `or`. | exact | `query.bool.should_text` |
| bool must_not over text | rejected (client) | Only a filter can be negated; Kosha has no "text must not match". | client | `query.bool.must_not_text` |
| bool should mixing text and filters | rejected (client) | Text OR a filter has no Kosha equivalent. | client | `query.bool.text_or_filter` |
| bool minimum_should_match > 1 over text | rejected (client) | Use `operator: and` when every term is required. | client | `query.bool.msm_over_text` |
| term, terms, range, exists, prefix in query context | partial | The clause runs as a Kosha filter and matches the same documents. **Divergence:** Hits get a small length-normalised constant score instead of Elasticsearch's term score (or constant 1.0), so ties are ordered differently. | exact | `query.term_level` |
| nested, has_child, has_parent | rejected (client) | Kosha has no nested documents; an object field's leaves are indexed under dotted paths instead. | client | `query.nested` |
| query_string, simple_query_string | rejected (client) | Rewrite as match / bool clauses. | client | `query.query_string` |
| fuzzy, regexp, match_phrase_prefix, match_bool_prefix | rejected (client) | No fuzzy or regular-expression matching. | client | `query.fuzzy` |
| ids, and term / terms / prefix on _id | rejected (client) | Use `client.get` / `client.mget` to fetch by id. | client | `query.ids` |
| constant_score, dis_max, boosting | rejected (client) | Put the clause in a bool `filter` instead. | client | `query.constant_score` |
| script_score, script, more_like_this, span_*, intervals | rejected (client) | No scripting, similarity or positional span queries. | client | `query.script_score` |
| geo_distance, geo_bounding_box, geo_shape | rejected (client) | Not translated by the client, although the native /v1 filter has geo_distance; send a native body for geo search. | client | `query.geo_distance` |

## Filters (bool filter context)

| Construct | Status | Notes | Checked | Case |
| --- | --- | --- | --- | --- |
| term | supported | Values compare as strings; booleans become "true"/"false". | exact | `filter.term` |
| term on a boolean | supported | `true` is sent as "true", the way the client stores booleans. | exact | `filter.term.boolean` |
| term / terms / prefix on _id | rejected (client) | Document ids are not filterable; use `client.get` / `client.mget`. | client | `filter.term.id` |
| terms | supported | Matches any value; a multi-valued field matches if any of its values does. | exact | `filter.terms` |
| terms on a multi-valued keyword | supported | A document matches if any of its values is listed. | exact | `filter.terms.multivalued` |
| range on a number (gt, gte, lt, lte) | supported | Numeric on Integer and Float fields. | exact | `filter.range.numeric` |
| range on a date | supported | ISO-8601 dates compare chronologically. | exact | `filter.range.date` |
| range with format, time_zone, relation | partial | The bounds are applied. **Divergence:** `format`, `time_zone` and `relation` are dropped: bounds are compared as given, with no date math or time-zone shift. Any other option is refused. | exact | `filter.range.options` |
| exists | supported | Matches documents that have the field. | live | `filter.exists` |
| prefix (short and long form, case_insensitive) | supported | Case-sensitive by default, as in Elasticsearch. `boost` and `rewrite` are ignored; other options are refused. | live | `filter.prefix` |
| match_all as a filter | supported | A no-op next to other clauses. | exact | `filter.match_all` |
| nested bool (must, filter, must_not, should) | supported | Any depth. `should` follows Elasticsearch: required (at least one) when the bool has no must/filter, optional otherwise. | exact | `filter.bool` |
| minimum_should_match as a percentage or combination | rejected (client) | Only an integer count. | client | `filter.bool.msm_percent` |
| nested, regexp, wildcard, script, geo_* as filters | rejected (client) | Refused, never dropped; dropping a filter would return documents the caller excluded. | client | `filter.unsupported` |

## Aggregations

| Construct | Status | Notes | Checked | Case |
| --- | --- | --- | --- | --- |
| terms | supported | Buckets over keyword values. | live | `aggregation.terms` |
| metric sub-aggregations under a bucket aggregation | supported | min, max, sum, avg and value_count can be nested under terms, composite, histogram and date_histogram. | live | `aggregation.terms.metric_sub` |
| bucket sub-aggregations (terms under terms, ...) | rejected (client) | Refused rather than ignored; the caller would read buckets that are not there. | client | `aggregation.bucket_sub` |
| composite | partial | Paginate with `after`. **Divergence:** `size` is required; Elasticsearch defaults it to 10. | live | `aggregation.composite` |
| composite without size | rejected (server) | The server answers 400 (missing field `size`). | live | `aggregation.composite.no_size` |
| histogram | supported | Numeric buckets. | live | `aggregation.histogram` |
| date_histogram (calendar_interval, fixed_interval, time_zone, bounds) | supported | Options are passed through to the server. | live | `aggregation.date_histogram` |
| cardinality | supported | Top level only; it takes no sub-aggregations. | live | `aggregation.cardinality` |
| min, max, sum, avg, value_count | supported | Single-value metrics. | live | `aggregation.metrics` |
| percentiles (percents, keyed) | supported | Top level or under a bucket aggregation. Exact up to 4,096 values per aggregation or bucket, then a DDSketch within 1% relative error. | live | `aggregation.percentiles` |
| stats, top_hits, filter(s), range, significant_terms, nested, ... | rejected (client) | Any other aggregation type is refused by name. | client | `aggregation.unsupported` |

## Sort

| Construct | Status | Notes | Checked | Case |
| --- | --- | --- | --- | --- |
| sort by a field (asc / desc, string or object form) | supported | Numeric fields sort numerically, others as strings; ties break by `_id`. | exact | `sort.field` |
| sort by _score and _id | supported | `_score` sorts by BM25; `_id` sorts by document id. | exact | `sort.score_and_id` |
| sort by _doc | partial | Stable, so it still walks a result set exactly once. **Divergence:** `_doc` (index order) becomes `_id` ascending. | exact | `sort.doc` |
| sort with missing, mode, unmapped_type, nested | partial | Field and order are applied. **Divergence:** The other options are dropped: a document without the field sorts first, and a multi-valued field sorts by its first value. | exact | `sort.options` |

## Paging and totals

| Construct | Status | Notes | Checked | Case |
| --- | --- | --- | --- | --- |
| from / size | supported | Offset paging. | exact | `paging.from_size` |
| search_after | supported | Cursor paging; values are sent as strings and `from` is reset to 0. | exact | `paging.search_after` |
| track_total_hits (true or a number) | supported | `true` counts exactly; a number caps the count, and `total.relation` reports whether it was reached. Without it the server's default cap applies. | live | `paging.track_total_hits` |

## Top-level search body keys

| Construct | Status | Notes | Checked | Case |
| --- | --- | --- | --- | --- |
| query | supported | See the query DSL and filter rows. | exact | `body_key.query` |
| top-level knn | supported | The older top-level form; the same as `query.knn`. | live | `body_key.knn` |
| post_filter | partial | The hits are the same as in Elasticsearch. **Divergence:** It is merged into the main filter, so aggregations are computed over the post-filtered hits too; Elasticsearch computes them before post_filter. | live | `body_key.post_filter` |
| aggs / aggregations | supported | Both spellings; sent as Kosha `aggs`. See the aggregation rows. | live | `body_key.aggs` |
| _source (false, a field list, includes / excludes) | supported | Filters the stored fields returned with each hit. | live | `body_key.source` |
| highlight | partial | Every listed field is highlighted with its own or the top-level tags. **Divergence:** Other highlighter options (`number_of_fragments`, `fragment_size`, `type`, ...) are dropped; they change only the markup, never which documents match. | live | `body_key.highlight` |
| collapse | supported | One hit per value of the field. `inner_hits` and `max_concurrent_group_searches` are refused. | live | `body_key.collapse` |
| request_cache | supported | `false` bypasses Kosha's response cache (`no_cache`). | exact | `body_key.request_cache` |
| rerank (Kosha extension) | supported | Not Elasticsearch DSL. Asks the server to rerank the top hits with an external model; see `KoshaClient.rerank_spec`. *Not run live: needs a rerank provider configured on the server.* | client | `body_key.rerank` |
| min_score | partial | Not sent to Kosha. **Divergence:** Ignored without an error; hits below the threshold are returned. | exact | `body_key.min_score` |
| suggest, rescore, script_fields, terminate_after, timeout, explain, profile, ... | partial | Not sent to Kosha. **Divergence:** Every top-level key not listed above is ignored without an error: no suggestions, no rescoring, no early termination. | exact | `body_key.ignored` |

## Index and document APIs

| Construct | Status | Notes | Checked | Case |
| --- | --- | --- | --- | --- |
| indices.create (with mappings) | partial | Creates the namespace (`POST /v1/namespaces`); plain mapping types become the namespace schema. **Divergence:** `settings` are ignored (no shards, replicas or refresh interval), and object, nested and multi-field mappings are left out of the schema. A text field's `analyzer` is passed through, and the server refuses a name it does not have (language analyzers, custom analyzers) with a 400. | live | `api.indices.create` |
| indices.exists | supported | True when every named namespace exists. | live | `api.indices.exists` |
| indices.get_mapping, indices.get | supported | The schema's fields with ES types; a namespace without a schema has empty properties. Fields are never guessed. | live | `api.indices.get_mapping` |
| indices.get_settings | partial | Returns the index name. **Divergence:** Nothing else is reported; Kosha has no shard, replica or refresh settings. | live | `api.indices.get_settings` |
| indices.put_mapping, indices.put_settings | partial | Acknowledged. **Divergence:** A no-op; the schema is unchanged. Set the schema when creating the namespace. | live | `api.indices.put_mapping` |
| indices.refresh | supported | Makes every write searchable now (a flush). | live | `api.indices.refresh` |
| indices.flush, indices.close | partial | Acknowledged. **Divergence:** Both are no-ops; use `indices.refresh` to make writes searchable. | live | `api.indices.flush` |
| indices.delete | supported | Deletes the namespace and its data. `ignore_unavailable` and `ignore=404` are honoured. | live | `api.indices.delete` |
| index (one document) | supported | Upserts by id. `refresh=true` / `wait_for` makes it searchable on return. | live | `api.index` |
| bulk / helpers.bulk | partial | index, create and update (partial `doc` merge) actions. **Divergence:** `delete` actions are not applied: each comes back as an item error with status 501. | live | `api.bulk` |
| get, mget | partial | A one-hit search per id, so it sees what search sees. **Divergence:** A missing document returns `found: false` instead of raising NotFoundError. | live | `api.get` |
| count | partial | Counts the documents matching the query, exactly. **Divergence:** Without a query (or with `match_all`) it counts 0, for the reason in the `match_all` row; pass `{"query": {"bool": {"filter": [{"match_all": {}}]}}}` to count every document. | live | `api.count` |
| msearch | supported | Runs as one search per body, in order; a failed search becomes an error item and the others still run. | live | `api.msearch` |
| search | supported | The translator described by every row above; the response has the OpenSearch shape. | live | `api.search` |
| update (doc) | partial | Merges the partial `doc` into the stored document. **Divergence:** `script`, `upsert` and `doc_as_upsert` are not interpreted. | live | `api.update` |
| delete_by_query | supported | The query must translate as a filter; a text query is refused rather than over-deleting. | live | `api.delete_by_query` |
| update_by_query, tasks.get | partial | Runs synchronously, or as a task polled with `tasks.get`. **Divergence:** Only simple assignments (`ctx._source.f = 'v'` or another field) and `params.<map>[_did]` lookups are understood; other Painless is refused. Tasks live in the client process. | live | `api.update_by_query` |
| scroll, clear_scroll | rejected (client) | `scroll` raises NotImplementedError; walk results with `search_after` (or from / size). `clear_scroll` is a no-op. | live | `api.scroll` |
| ping | supported | True when the server answers. | live | `api.ping` |
| delete (by id), exists, create, reindex, aliases, point in time, ... | rejected (client) | Not on KoshaClient (AttributeError). There is no delete by id (`delete_by_query` takes a filter on fields, not `_id`); copy indices with scripts/copy_es_to_kosha.py. | client | `api.not_implemented` |
