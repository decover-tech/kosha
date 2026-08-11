#!/bin/bash
# Vector campaign stage 1: seed the embeddings cache (one-time HF fetch),
# then a 1M-doc SPFresh smoke — first at-scale exercise of vector ingest +
# kNN query path. Produces /data/results-vec-smoke/.
set -euo pipefail

EMB_CACHE=s3://decoverai-bench-corpus-cache/msmarco-10m-emb
TXT_CACHE=s3://decoverai-bench-corpus-cache/msmarco-10m
sudo mkdir -p /data/corpus-emb /data/smoke /data/results-vec-smoke
sudo chown -R ec2-user:ec2-user /data/corpus-emb /data/smoke /data/results-vec-smoke

~/venv311/bin/pip install -q numpy pyarrow huggingface_hub requests
# Preflight (argparse lesson)
~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/fetch_msmarco_embeddings.py --help | grep -q -- "--s3-cache" || { echo FLAGS-MISSING-fetch; exit 1; }
~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/load_corpus.py --help | grep -q -- "--embeddings-dir" || { echo FLAGS-MISSING-load; exit 1; }
~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/query_bench.py --help | grep -q -- "--knn-embeddings" || { echo FLAGS-MISSING-bench; exit 1; }
echo "preflight ok"

# ── seed the embeddings cache (~41GB from HF, once ever) ─────────────────
~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/fetch_msmarco_embeddings.py \
  --out-dir /data/corpus-emb --docs 10000000 --s3-cache "$EMB_CACHE"
echo "EMB-SEED-COMPLETE"

# ── 1M smoke inputs: first 10 text shards + queries, local ───────────────
for i in 0 1 2 3 4 5 6 7 8 9; do
  aws s3 cp --quiet "$TXT_CACHE/shard-0000${i}.ndjson" /data/smoke/ || { echo STAGE-CP-FAILED; exit 1; }
done
aws s3 cp --quiet "$TXT_CACHE/queries.txt" /data/queries.txt
mkdir -p /data/smoke-emb
for i in 0 1 2 3 4 5 6 7 8 9; do
  cp "/data/corpus-emb/emb-0000${i}.f32" "/data/corpus-emb/emb-0000${i}.ids" /data/smoke-emb/
done
cp /data/corpus-emb/queries_emb.f32 /data/
echo "smoke inputs staged"

# ── containers ───────────────────────────────────────────────────────────
sudo docker start pg >/dev/null 2>&1 || sudo docker run -d --name pg --network host \
  -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=kosha \
  -v /data/pg:/var/lib/postgresql/data postgres:16 >/dev/null
until sudo docker exec pg pg_isready -U postgres >/dev/null 2>&1; do sleep 2; done
DBURL="postgresql://postgres:bench@127.0.0.1:5432/kosha"
sudo docker rm -f kosha >/dev/null 2>&1 || true
sudo docker run -d --name kosha --network host \
  -e DATABASE_URL=$DBURL -e KOSHA_API_KEY=sk-bench \
  -e KOSHA_S3_BUCKET=decoverai-bench-kosha-segments -e KOSHA_S3_PREFIX=segments/ \
  -e AWS_DEFAULT_REGION=us-east-1 -e KOSHA_FLUSH_THRESHOLD=50000 \
  -e KOSHA_HTTP_IO_TIMEOUT_SECS=1800 \
  -e KOSHA_CACHE_DIR=/var/cache/kosha -e KOSHA_DATA_DIR=/var/cache/kosha/data \
  -e KOSHA_CACHE_MAX_BYTES=644245094400 -e KOSHA_SEGMENT_CACHE_MAX_BYTES=51539607552 \
  -e KOSHA_SEGMENT_LIVE_MAX_BYTES=68719476736 -e KOSHA_SCORING_HYDRATE_CONCURRENCY=64 \
  -e KOSHA_SEGMENT_CACHE_CAPACITY=16384 \
  -v /data/kosha:/var/cache/kosha kosha-server:bench8 >/dev/null
until curl -sf -H "Authorization: Bearer sk-bench" http://127.0.0.1:8080/stats >/dev/null; do sleep 2; done
echo "kosha up"

# ── 1M vector load ───────────────────────────────────────────────────────
~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/load_corpus.py \
  --host http://127.0.0.1:8080 --namespace msmarco-1m-vec \
  --corpus-dir /data/smoke --embeddings-dir /data/smoke-emb \
  --api-key sk-bench --batch-size 1000 --concurrency 8
echo "VECTOR-LOAD-COMPLETE"

# Segment file layout — what does a vector segment actually contain?
SEG=$(ls /data/kosha/data/msmarco-1m-vec | head -1)
ls -la "/data/kosha/data/msmarco-1m-vec/$SEG" > /data/results-vec-smoke/segment_layout.txt
cat /data/results-vec-smoke/segment_layout.txt

# ── short kNN phase: 5 min @ 8 QPS ───────────────────────────────────────
~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/query_bench.py \
  --host http://127.0.0.1:8080 --namespace msmarco-1m-vec \
  --queries-file /data/queries.txt --api-key sk-bench \
  --knn-embeddings /data/queries_emb.f32 \
  --qps 8 --topk 10 --duration 300 --timeout 120 \
  --phase warm --out /data/results-vec-smoke/knn_1m_warm.json
sudo docker logs kosha 2>&1 | tail -3000 > /data/results-vec-smoke/kosha_smoke.log
echo "SMOKE-COMPLETE"
