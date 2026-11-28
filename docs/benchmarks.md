# Benchmarks

How the numbers in the README were produced, which comparisons do not exist yet, and how to rerun both harnesses. Every figure on this page is copied from [scripts/bench/bm25_scale/RESULTS.md](../scripts/bench/bm25_scale/RESULTS.md) or from [DESIGN.md](../DESIGN.md). Nothing here was filled in by guesswork.

## What to quote

Kosha, 8 QPS open loop, top 10, 30 minutes per phase, recorded 2026-11-20/21 on `main` after pulls #362 and #373.

| Workload | Phase | p50 | p90 | p99 | Errors | Run |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| BM25, 10M MS MARCO passages | fresh pod after warmup, result cache off | 32.7 ms | 74.5 ms | 201 ms | 0 | [44843615336](https://github.com/decover-tech/Kosha/actions/runs/44843615336) |
| BM25, same | fresh pod after warmup, result cache off | 29.0 ms | 66.4 ms | 175 ms | 0 | [44892212058](https://github.com/decover-tech/Kosha/actions/runs/44892212058) |
| BM25, same | warm, result cache off | 29.0 ms | 63.5 ms | 170 ms | 0 | 44892212058 |
| BM25, same | warm, result cache on | 1.7 ms | 12.1 ms | 66 ms | 0 | 44892212058 |
| kNN, 10M × 1024-d f32, `k=10`, `num_candidates=100` | fresh pod after warmup, cache off | 88.5 ms | 113 ms | 607 ms | 263 (`429`) | [44876437446](https://github.com/decover-tech/Kosha/actions/runs/44876437446) |
| kNN, same | warm, cache on | 88.0 ms | 111 ms | 222 ms | 0 | 44876437446 |

Recall@10 of that kNN run against exact brute force, 100 queries: mean 0.90, median 1.0, minimum 0.5, top-1 100%.

"Fresh pod" waits until `/readyz`, which is after the namespace has been preloaded. It is not an on-demand cold read. The BM25 preload of the 23 GB working set took 7.3 minutes on that run. The vector namespace (~249 GB) took 1h55m and the following run hit the 2h warmup timeout. Warm cache-on and cache-off kNN medians match, so those kNN responses do not look like result-cache hits.

Per-request logs for runs from 44892212058 on are in `s3://decoverai-bench-corpus-cache/runs/<run_id>/`. That bucket is private.

### Hardware and build

The benchmark workflow (`.github/workflows/benchmark.yml`) provisions the VM described in [scripts/bench/bm25_scale/vm/README.md](../scripts/bench/bm25_scale/vm/README.md): an `m7i.8xlarge` (32 vCPU, 128 GB) with the segment bucket in the same region. The November result rows cite the Actions run id and do not repeat the instance type, so treat "m7i.8xlarge" as the workflow's machine, not a line item restated on those rows.

The server image is the repo's Dockerfile. The workflow can pass `KOSHA_RUSTFLAGS`. The November note says the runs are `main` plus #362 and #373. Kosha's crate version at the time of this write-up is `0.1.0` (`Cargo.toml`). The Docker image that CI publishes for a `v*` tag is `decoverai/kosha` (see [development.md](development.md#which-docker-image)).

### Dataset

BM25 and the vector runs use Cohere's MS MARCO v2.1 passage corpus (`fetch_msmarco.py`, `fetch_msmarco_embeddings.py` under [scripts/bench/bm25_scale](../scripts/bench/bm25_scale)). The 10M BM25 load is the `segment` text column, real MS MARCO queries cycled in dataset order, 10,000,000 docs, about 9.91 GB of text, 167 segments in the August write-up and 200 segments in the November tail analysis. The vector corpus is 1024-dimensional f32. turbopuffer's own vector page disagrees with itself on shape (1024-d vs 768-d f16); Kosha's corpus is 1024-d f32.

An earlier synthetic corpus (`generate_corpus.py --docs 10000000 --avg-bytes 900 --seed 42`, Zipf vocabulary of 50,000 terms) is what the OpenSearch row below was loaded with. It is reproducible from that command. It is not MS MARCO.

### OpenSearch, and why it is not in the same table

Recorded 2026-08-06, [RESULTS.md](../scripts/bench/bm25_scale/RESULTS.md):

| | p50 | p90 | p99 |
| --- | ---: | ---: | ---: |
| OpenSearch warm | 12.3 ms | 24.8 ms | 62.1 ms |
| OpenSearch "cold" | 18.5 ms | 38.2 ms | 98.1 ms |

Domain: staging OpenSearch, 2× `r8g.large.search`, index `bm25-bench-10m`, 5 shards / 1 replica, BM25 `k1=1.2` / `b=0.75`, forcemerged to 5 segments per shard. 961/961 successes, about 8.0 QPS. The OpenSearch version of that domain is not written down in the results file.

The "cold" phase only calls `POST /_cache/clear`. There was no node-shell access to drop the OS page cache, so the cold number is a weaker condition than a process restart. The results file says the warm numbers are the ones to use, and that Kosha's attempt on this same synthetic namespace was aborted. No clean Kosha percentile exists for that corpus.

Do not subtract these from the MS MARCO rows.

### turbopuffer's published figures

turbopuffer's full-text figures (warm 13/18/29 ms, cold 316/381/559 ms) are for **1M docs, ~300 MB**, not 10M. Their vector figures (warm 14/17/27 ms) are for 10M docs. Older sections of RESULTS.md compared Kosha at 10M BM25 against the 1M full-text figures. The results file's 2026-11-21 correction says to treat the 10M BM25 rows as a scale result, not a like-for-like comparison. A 1M slice is a workflow input (`bm25_docs: 1m`) and had not been published as a result row when this page was written. All turbopuffer numbers are vendor-reported.

### Elasticsearch

Not measured. No Elasticsearch version, cluster, or latency number is checked in as a result.

### Cost

This repo does not contain a measured production cost ratio against OpenSearch or Elasticsearch.

[DESIGN.md](../DESIGN.md) §16 prints planning figures of about $0.16–0.24/GB-month for SSD-backed OpenSearch volumes and about $0.023/GB-month for S3 Standard, plus latency targets (warm p50 ≤ 80 ms, p99 ≤ 300 ms). The same section says those figures are directional planning targets, not measured benchmarks. Dividing the storage prices is not a result the document stands behind, and it is not a bill from DecoverAI's cluster.

[scripts/bench/render_report.py](../scripts/bench/render_report.py) builds an HTML report with an illustrative AWS price model (S3 Standard vs gp3 per GB, and a duty-cycle compute table). The script's own note says the cost figures are estimates from public list prices and DESIGN.md's target architecture, not measurements of a running deployment, and that the microbench corpus is small. The checked-in tree does not include a rendered report with filled-in dollar totals.

## Rerun the publication-scale harness

Full protocol, warm-gate modes, and the CI workflow inputs are in [scripts/bench/bm25_scale/README.md](../scripts/bench/bm25_scale/README.md). Short version:

1. Fetch MS MARCO with `scripts/bench/bm25_scale/fetch_msmarco.py` (and `fetch_msmarco_embeddings.py` for the vector suite). The synthetic stand-in is `generate_corpus.py --docs 10000000 --avg-bytes 900 --seed 42`.
2. Build segments with the offline builder, or load through `load_corpus.py`. On the server set `KOSHA_FLUSH_THRESHOLD` high (the README suggests `50000`), `KOSHA_FLUSH_MAX_AGE_MS=0`, and `KOSHA_COMPACTION_INTERVAL_MS=0` during the load so the idle flush and the compaction scheduler do not cut the run apart.
3. Dispatch `.github/workflows/benchmark.yml` (`workflow_dispatch`). Default suite is both BM25 and vector, `mode=restore` from a snapshot, `qps=8`. `duration=1800` is the publication-length phase; the workflow default of 300 is a short check. The job needs the bench AWS credentials and the infra repo token named in the workflow file. It tears the VM down on success.
4. OpenSearch on the same corpus, if you have a cluster: `load_corpus_opensearch.py` then `query_bench_opensearch.py`. Write the JSON next to a Kosha run that used the same `queries.txt`. That pair does not exist in git.

Query the engine the way the harness does: open-loop 8 QPS, top 10, BM25 `operator: or` on the MS MARCO workload (the August runs were AND, and the results file records that AND returned zero hits on about a third of natural-language queries).

## Rerun a small comparison on one machine

[scripts/bench/compare](../scripts/bench/compare/README.md) starts OpenSearch 2.18.0 and `decoverai/kosha:0.1.0`, generates a few thousand synthetic documents, and runs [scripts/bench/run_benchmark.py](../scripts/bench/run_benchmark.py) against both. That measures one laptop-sized corpus. It does not replace the table above. Elasticsearch is not started by that compose file. The result file in that directory is a placeholder until someone runs the script and commits the machine description with the JSON.

`run_benchmark.py` is also the older single-client microbench (lexical and kNN). It expects you to pass `--corpus` and `--queries` and to already have both servers listening. It does not send an API key, so the Kosha process has to be started with `KOSHA_AUTH=disabled` or the compare compose file.
