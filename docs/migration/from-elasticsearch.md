# Migrating from Elasticsearch or OpenSearch

This guide moves an Elasticsearch or OpenSearch index into Kosha and switches
an application over to it. It uses two tools from this repository:

- [`scripts/copy_es_to_kosha.py`](../../scripts/copy_es_to_kosha.py) copies
  documents through Kosha's public `/v1` API. It needs network access to both
  clusters and a Kosha API key, nothing else (Python 3.9+, standard library
  only).
- [kosha-client](../../clients/python), the Python client, which accepts
  OpenSearch and Elasticsearch request bodies and translates them into Kosha's
  native API, so most `opensearch-py` code runs unchanged.

Kosha has no server-side OpenSearch endpoint (`_search`, `_bulk`). The
[compatibility matrix](../compat/opensearch.md) lists which query clauses,
filters, aggregations, body keys and index APIs the Python client supports,
partially supports or rejects. Read it before you start: a query that uses a
rejected construct fails with HTTP 400 `unsupported by Kosha: ...` instead of
returning different results. The TypeScript and Go SDKs take native `/v1`
bodies only.

## 1. Check your queries against the matrix

List the query DSL your application sends (log a sample of request bodies,
or search the code for `query`, `aggs` and `sort`) and look each construct up
in the [matrix](../compat/opensearch.md). Pay attention to the partial rows,
which run but answer differently. Among the most common:

- `match_all` alone, or a body with no `query`, matches nothing; write it as
  `{"bool": {"filter": [{"match_all": {}}]}}`. `count()` without a query is
  affected in the same way.
- `multi_match` scores every type as `most_fields`.
- `post_filter` also narrows the aggregations.
- `wildcard` does not restrict the search to the named field.

## 2. Point the script at both clusters

```bash
export ELASTICSEARCH_HOST=https://user:password@es.example.com:9200
export KOSHA_HOST=https://kosha.example.com
export KOSHA_API_KEY=sk-...
```

For Amazon OpenSearch Service with IAM, set `ELASTICSEARCH_AUTH_MODE=sigv4`
and `AWS_REGION`; the script then signs requests with the credentials boto3
finds (it needs `boto3` installed only in this mode). Both hosts can also be
given as flags (`--es-host`, `--kosha-host`).

## 3. Dry run

Count what would be copied, without writing anything:

```bash
python3 scripts/copy_es_to_kosha.py --index products --dry-run
```

`--index` takes an exact index or alias name and can be repeated. `--pattern`
expands a pattern through `_cat/indices` (system indices are skipped). Each
index is copied into a Kosha namespace of the same name; an alias keeps its
alias name rather than the name of its backing index.

## 4. Copy a sample into a scratch namespace

```bash
python3 scripts/copy_es_to_kosha.py --index products --namespace products-smoke --limit 10 --create-schema-from-mapping
```

`--create-schema-from-mapping` creates the namespace with a schema built from
the index mapping, and sends each value with its mapped type:

| Elasticsearch type | Kosha field type |
| --- | --- |
| `text`, `match_only_text` | Text (`standard`, `whitespace` and `keyword` analyzers are kept; others, such as language analyzers, fall back to Kosha's default analysis) |
| `keyword`, `constant_keyword` | Keyword |
| `long`, `integer`, `short`, `byte` | Integer |
| `double`, `float`, `half_float`, `scaled_float` | Float |
| `date`, `date_nanos` | Date (epoch milliseconds become ISO-8601 UTC) |
| `boolean` | Boolean |
| `dense_vector`, `knn_vector` | Vector |
| `object`, `nested`, `geo_point`, anything else | not in the schema; the value is typed from its JSON shape |

A list of values for a mapped field becomes a multi-valued field, so a `term`
filter matches a document when any of its values matches, as in
Elasticsearch. Without the flag every value is typed from its JSON shape, the
way kosha-client's `index()` types it: strings become Text, numbers Float, and
lists and objects JSON-encoded Keyword. Prefer the flag: a `keyword` field
copied as Text is tokenized, and a `term` filter on it stops matching whole
values.

If the namespace already exists, the script leaves its schema as it is and
still uses the mapping to type values.

Search the scratch namespace with the queries from step 1 before going on.
Delete it afterwards with `client.indices.delete("products-smoke")`.

## 5. Copy everything

```bash
python3 scripts/copy_es_to_kosha.py --index products --create-schema-from-mapping --workers 4
python3 scripts/copy_es_to_kosha.py --pattern 'logs-*' --create-schema-from-mapping
```

| Flag | Default | Effect |
| --- | --- | --- |
| `--workers N` | 1 | N parallel sliced scrolls per index. |
| `--batch-size N` | 200 | Documents per Kosha write. Lower it if writes are refused as too large. |
| `--scroll-size N` | 1000 | ES scroll page size. |
| `--sleep S` | 0 | Pause between batches, to throttle the copy. |
| `--limit N` | none | Copy at most N documents per index. |
| `--embedding-field F` | none | Also report how many documents carry field F. |
| `--no-verify` | off | Skip the count check described below. |
| `--allow-extra` | off | In the count check, accept a namespace holding more documents than Elasticsearch (a re-run after deletes there). |

Writes are upserts by document id, so a copy that stopped part-way can simply
be run again. The script retries Kosha's 429 and 503 answers (honouring
`Retry-After`). It flushes each namespace once its documents are written.

The copy is a snapshot of each index as its scroll reads it. Documents written
to Elasticsearch after that are not copied, and deletions are not propagated:
keep writing to both systems until you cut over, or run the copy again just
before switching (it upserts changed documents, but does not remove documents
deleted in Elasticsearch). Pass `--allow-extra` to that re-run, because any
document deleted in Elasticsearch since the first copy is still in Kosha and
the count check would otherwise fail:

```bash
python3 scripts/copy_es_to_kosha.py --index products --allow-extra
```

To drop those stale documents instead, delete the namespace
(`DELETE /v1/namespaces/products`) and run the copy into it again.

## 6. Verify

After each index the script counts the documents in the namespace and
compares the number with Elasticsearch's `_count` (or with `--limit`). A
mismatch is logged as `count mismatch` and the script exits with status 1.
With `--allow-extra`, more documents in Kosha than in Elasticsearch is a
warning instead; fewer is still a mismatch.

`_count` is read when an index's copy finishes, not when its scroll started.
On an index that is still taking writes or deletes, the two numbers can
differ even though the copy did what it should. Run the verifying copy
after writes to Elasticsearch have stopped for the cutover, or treat a
mismatch on a live index as expected and re-run once writes stop.

Then compare search results for a handful of real queries, sending the same
body through both clients:

```python
from opensearchpy import OpenSearch
from kosha_client import KoshaClient

es = OpenSearch("https://es.example.com:9200")
kosha = KoshaClient(hosts="https://kosha.example.com", api_key="sk-...")

body = {"query": {"bool": {"must": [{"match": {"name": "lamp"}}],
                           "filter": [{"term": {"sku": "LMP-1"}}]}}}
es_ids = [h["_id"] for h in es.search(index="products", body=body)["hits"]["hits"]]
kosha_ids = [h["_id"] for h in kosha.search(index="products", body=body)["hits"]["hits"]]
print(es_ids, kosha_ids)
```

Expect the same documents for supported constructs. Rankings are close but
not identical: both engines use BM25 with `k1=1.2`, `b=0.75`, but Kosha
computes its statistics over the whole namespace and tokenizes text its own
way (see [Analyzers](../../README.md#analyzers) in the README).

## 7. Switch the application

```bash
pip install kosha-client
```

```python
# Before
from opensearchpy import OpenSearch
client = OpenSearch(hosts=["https://es.example.com:9200"], http_auth=("user", "password"))

# After
from kosha_client import KoshaClient as OpenSearch
client = OpenSearch(hosts="https://kosha.example.com", api_key="sk-...")
```

`opensearch_dsl` (`Search`, `Q`, `Document.save`) and `opensearchpy.helpers.bulk`
work on top of it. A few behaviours to know:

- **Visibility.** With the server's default `strong` consistency a write is
  searchable as soon as the call returns. `refresh=True` or
  `refresh="wait_for"` also flushes. `KoshaClient(consistency="eventual")`
  searches only flushed data, like Elasticsearch between refreshes.
- **Index settings** (shards, replicas, refresh interval) have no Kosha
  equivalent; `indices.put_settings` and `indices.put_mapping` are
  acknowledged no-ops. Set the schema when the namespace is created.
- **Fetch by id** (`get`, `mget`) is a search, and a missing document returns
  `found: false` instead of raising.
- **`scroll`** is not supported; page with `search_after` or `from`/`size`.

The [matrix](../compat/opensearch.md) has the full list.

## Self-hosted bulk backfills

Operators who run Kosha themselves can backfill very large indices without
going through the HTTP API: `kosha-server migrate` reads OpenSearch directly,
builds segments in-process and publishes them to S3 and Postgres. It needs
the server's storage credentials and replaces the namespace manifest, so it
is an operator tool rather than part of this guide; see "Migrating data from
OpenSearch → Kosha" in [k8s/README.md](../../k8s/README.md).
