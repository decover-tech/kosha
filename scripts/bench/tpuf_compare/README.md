# Same-data comparison against turbopuffer

Runs Kosha and turbopuffer on **the same corpus and the same queries**, from one
machine in the turbopuffer region, so latency and result quality compare like
for like (DEC-3203). The published tpuf-benchmark numbers use different corpora
(BM25 on 1M docs, vectors at 768-d f16), so they can only bound a comparison.

**Never run this through the `benchmark` workflow.** This repository is public, so
workflow logs and artifacts are public. The corpus is read out of a real
turbopuffer namespace. Use a private VM, keep the data on its disk and in a
private bucket, and delete both afterwards.

## Inputs

A turbopuffer namespace pair `<prefix>-chunks` (BM25 on a `content` string
attribute) and `<prefix>-chunkvec` (a `vector` attribute, cosine), plus
`TURBOPUFFER_API_KEY`. Reads only: each query is billed at the namespace's
logical size (~$1 per PB queried), so a full run costs cents.

## Steps (on an m7i.8xlarge in the turbopuffer region)

1. `python tpuf_export.py <prefix> /data/corpus`: pages both namespaces by id
   (16 id-prefix ranges in parallel) into `kosha-build-segments` input:
   `txt/shard-*.ndjson` and `emb/emb-*.f32` + `.ids` (f16 widened to f32).
   About a minute for 1M rows in-region.
2. `python prep.py`: vector-aligned text shards, 1,000 BM25 queries (1-8-word
   windows from random chunks), 1,000 kNN queries (normalized sum of two corpus
   vectors) and their exact cosine top-10. Seeded, so reruns use identical queries.
3. Build segments with `kosha-build-segments` (`--emb-dim` to match), upload
   them under `segments/default/<ns>/` in the server's bucket and
   `POST /v1/admin/import-namespace`. Run the server with the `benchmark.yml`
   flags.
4. Latency, 8 QPS, topk 10, 300 s per run after one warm pass:
   - Kosha: `../bm25_scale/query_bench.py --no-cache --server-timings` (ids-only
     responses by default; `--operator or` for BM25, since turbopuffer's BM25 is
     OR; `--knn-embeddings ... --knn-dim 1536` for kNN).
   - turbopuffer: `tpuf_query_bench.py`, once with `--consistency strong` and once
     with `eventual` (what an application with eventual reads sees). It reports
     turbopuffer's own `server_total_ms` too: compare server-side times, since the
     Kosha client is on localhost and turbopuffer's crosses HTTPS (~3-5 ms).
5. Quality, `TPUF_PREFIX=<prefix> python quality.py 200`:
   - kNN recall@10 for both engines, **tie-tolerant** (a hit counts if its exact
     similarity is at least the 10th-best). Legal corpora are full of near-duplicate
     chunks, and most queries have more than 10 documents tied at the cutoff, so
     exact-id recall understates both engines.
   - BM25: Kosha's top 10 rescored by turbopuffer's own BM25 (`id In` filter);
     a hit is equivalent if it scores at least turbopuffer's 10th.
6. `python knn_recall_sweep.py 300 <label>` against a server restarted with
   different `KOSHA_KNN_*` settings: tie-tolerant recall, server p50/p90 and
   postings probed per configuration.
