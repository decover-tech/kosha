# Local comparison results

**Status: not measured.** This file is a placeholder. The commands that fill
it are in [README.md](README.md). Do not invent the cells below.

The README numbers for Kosha at 10M MS MARCO, and the separate OpenSearch
numbers on a synthetic 10M corpus, live in
[docs/benchmarks.md](../../../docs/benchmarks.md). They are not this harness.

## Machine

| | |
| --- | --- |
| Date | not measured |
| CPU / RAM / disk | not measured |
| Kosha image | `decoverai/kosha:0.1.0` (digest not measured) |
| OpenSearch image | `opensearchproject/opensearch:2.18.0` (digest not measured) |

## Corpus

`prepare_corpus.py` defaults: 2,000 documents, about 400 bytes each, seed 42,
20 queries. The run's `out/manifest.json` is the record of what was indexed.

| | |
| --- | --- |
| Documents | not measured |
| Seed | not measured |

## Lexical summary

From `out/results.json` → `modes.lexical.summary`, after a real run:

| | |
| --- | --- |
| Average OpenSearch mean latency | not measured |
| Average Kosha mean latency | not measured |
| Average top-k Jaccard | not measured |
| Top-1 match rate | not measured |

## Elasticsearch

Not measured. This harness does not start Elasticsearch.
