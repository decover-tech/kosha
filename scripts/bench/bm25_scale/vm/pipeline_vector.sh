#!/bin/bash
# Round 9 — 10M Vector Perf: main + #128 (knn result cache) + #129 (warmup
# vector prefetch). 500k-doc flush threshold -> ~20 large segments, no
# compaction (vector corpus is ~260GB on disk; capped merging would rewrite
# it for hours). Phases: cold kNN cache-OFF, warm kNN cache-ON.
set -euo pipefail

TXT_CACHE=s3://decoverai-bench-corpus-cache/msmarco-10m
EMB_CACHE=s3://decoverai-bench-corpus-cache/msmarco-10m-emb
sudo mkdir -p /data/results-r9 && sudo chown -R ec2-user:ec2-user /data/results-r9

# Disk hygiene: drop smoke namespaces' local segments (S3 copies remain).
sudo rm -rf /data/kosha/data/msmarco-1m-vec /data/kosha/data/msmarco-1m-vec2 || true

cd ~/kosha
sudo docker build -q -t kosha-server:bench10 . > /tmp/build10.log 2>&1 || { tail -20 /tmp/build10.log; exit 1; }
echo "image built"

~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/load_corpus.py --help | grep -q -- "--embeddings-dir" || { echo FLAGS-MISSING; exit 1; }
aws s3 cp --quiet "$TXT_CACHE/queries.txt" /data/queries.txt
aws s3 cp --quiet "$EMB_CACHE/queries_emb.f32" /data/queries_emb.f32

sudo docker start pg >/dev/null 2>&1 || true
until sudo docker exec pg pg_isready -U postgres >/dev/null 2>&1; do sleep 2; done
DBURL="postgresql://postgres:bench@127.0.0.1:5432/kosha"
start_kosha() {
  sudo docker rm -f kosha >/dev/null 2>&1 || true
  sudo docker run -d --name kosha --network host \
    -e DATABASE_URL=$DBURL -e KOSHA_API_KEY=sk-bench \
    -e KOSHA_S3_BUCKET=decoverai-bench-kosha-segments -e KOSHA_S3_PREFIX=segments/ \
    -e AWS_DEFAULT_REGION=us-east-1 -e KOSHA_FLUSH_THRESHOLD=500000 \
    -e KOSHA_HTTP_IO_TIMEOUT_SECS=1800 \
    -e KOSHA_CACHE_DIR=/var/cache/kosha -e KOSHA_DATA_DIR=/var/cache/kosha/data \
    -e KOSHA_CACHE_MAX_BYTES=644245094400 -e KOSHA_SEGMENT_CACHE_MAX_BYTES=51539607552 \
    -e KOSHA_SEGMENT_LIVE_MAX_BYTES=68719476736 -e KOSHA_SCORING_HYDRATE_CONCURRENCY=64 \
    -e KOSHA_SEGMENT_CACHE_CAPACITY=16384 \
    -e KOSHA_WARMUP_NAMESPACES="msmarco-10m-vec,default/msmarco-10m-vec" \
    -v /data/kosha:/var/cache/kosha kosha-server:bench10 >/dev/null
  until curl -sf -H "Authorization: Bearer sk-bench" http://127.0.0.1:8080/stats >/dev/null; do sleep 3; done
}
start_kosha
echo "kosha up"

# ── 10M vector load, streamed from both S3 caches ────────────────────────
# --timeout 900: the batch that trips a 500k-doc flush waits for that
# flush's SPFresh build inside the request.
~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/load_corpus.py \
  --host http://127.0.0.1:8080 --namespace msmarco-10m-vec \
  --corpus-dir "$TXT_CACHE" --embeddings-dir "$EMB_CACHE" \
  --api-key sk-bench --batch-size 1000 --concurrency 8 --timeout 900
echo "VECTOR-LOAD-COMPLETE"

SEG=$(ls /data/kosha/data/msmarco-10m-vec | head -1)
head -c 4 "/data/kosha/data/msmarco-10m-vec/$SEG/vector.idx" | od -c | head -1
ls /data/kosha/data/msmarco-10m-vec | wc -l

# ── cold reset + warmup (now includes vector files) ──────────────────────
sudo docker stop kosha >/dev/null
sudo rm -rf /data/kosha/data/*
T0=$(date +%s)
start_kosha
until curl -sf http://127.0.0.1:8080/readyz >/dev/null 2>&1; do
  sleep 5
  [ $(( $(date +%s) - T0 )) -gt 3600 ] && { echo "WARMUP-TIMEOUT"; exit 1; }
done
echo "WARMUP-READY in $(( $(date +%s) - T0 ))s"
sudo docker logs kosha 2>&1 | grep -ia "warmup" > /data/results-r9/warmup.log || true

# ── cold kNN, result cache OFF ───────────────────────────────────────────
~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/query_bench.py \
  --host http://127.0.0.1:8080 --namespace msmarco-10m-vec \
  --queries-file /data/queries.txt --api-key sk-bench \
  --knn-embeddings /data/queries_emb.f32 --no-cache \
  --qps 8 --topk 10 --duration 1800 --timeout 600 \
  --phase cold --out /data/results-r9/knn_cold_nocache.json
echo "COLD-PHASE-COMPLETE"

# ── warm kNN, result cache ON (#128) ─────────────────────────────────────
~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/query_bench.py \
  --host http://127.0.0.1:8080 --namespace msmarco-10m-vec \
  --queries-file /data/queries.txt --api-key sk-bench \
  --knn-embeddings /data/queries_emb.f32 \
  --qps 8 --topk 10 --duration 1800 --timeout 600 \
  --phase warm --out /data/results-r9/knn_warm_cache.json
echo "WARM-PHASE-COMPLETE"

sudo docker logs --tail 100000 kosha 2>&1 > /data/results-r9/kosha_r9_tail.log || true
echo "PIPELINE-COMPLETE"
