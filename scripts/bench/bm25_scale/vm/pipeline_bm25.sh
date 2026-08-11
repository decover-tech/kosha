#!/bin/bash
# Round 7: S3-cached corpus -> load -> FULL compaction -> warmup-enabled
# cold reset -> cold OR cache-off phase -> warm OR cache-off phase.
# Levers under test vs round 6 (184/366/7,858ms cold cache-off @ 167 segs):
#   - full compaction (#117): 167 -> ~1 segment
#   - warmup posting-blob prefetch (#116): kills the startup convoy
set -euo pipefail

CACHE=s3://decoverai-bench-corpus-cache/msmarco-10m
mkdir_all() { sudo mkdir -p /data/kosha /data/results-r7 && sudo chown -R ec2-user:ec2-user /data && sudo chown -R 10001:10001 /data/kosha; }
mkdir_all

cd ~/kosha
sudo docker build -q -t kosha-server:bench . > /tmp/build.log 2>&1 &
BUILD_PID=$!

sudo dnf install -q -y python3.11 python3.11-pip >/dev/null 2>&1
python3.11 -m venv ~/venv311 2>/dev/null || true
~/venv311/bin/pip install -q requests
# Preflight (argparse lesson): every flag this run relies on must be registered.
~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/load_corpus.py --help | grep -q -- "--compact-mode" || { echo FLAGS-MISSING-load; exit 1; }
~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/query_bench.py --help | grep -q -- "--no-cache" || { echo FLAGS-MISSING-bench; exit 1; }
aws s3 cp --quiet "$CACHE/queries.txt" /data/queries.txt

sudo docker rm -f pg 2>/dev/null || true
sudo docker run -d --name pg --network host \
  -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=kosha \
  -v /data/pg:/var/lib/postgresql/data postgres:16 >/dev/null
until sudo docker exec pg pg_isready -U postgres >/dev/null 2>&1; do sleep 2; done

wait $BUILD_PID || { tail -20 /tmp/build.log; exit 1; }
echo "image built"

DBURL="postgresql://postgres:bench@127.0.0.1:5432/kosha"
start_kosha() {
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
    -e KOSHA_WARMUP_NAMESPACES="msmarco-10m,default/msmarco-10m" \
    -v /data/kosha:/var/cache/kosha kosha-server:bench >/dev/null
  until curl -sf -H "Authorization: Bearer sk-bench" http://127.0.0.1:8080/stats >/dev/null; do sleep 2; done
}
start_kosha
echo "kosha up"

# ── load from S3 cache + FULL compaction (doc-loss guard inside) ──────────
~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/load_corpus.py \
  --host http://127.0.0.1:8080 --namespace msmarco-10m \
  --corpus-dir "$CACHE" --api-key sk-bench \
  --batch-size 20000 --concurrency 8 \
  --compact-after --compact-mode full
echo "LOAD-AND-COMPACT-COMPLETE"

# ── cold reset with warmup: wipe local store, restart, wait for /readyz ──
sudo docker stop kosha >/dev/null
sudo rm -rf /data/kosha/data/*
T_WARM0=$(date +%s)
start_kosha
until curl -sf http://127.0.0.1:8080/readyz >/dev/null 2>&1; do
  sleep 5
  [ $(( $(date +%s) - T_WARM0 )) -gt 2400 ] && { echo "WARMUP-TIMEOUT"; exit 1; }
done
echo "WARMUP-READY in $(( $(date +%s) - T_WARM0 ))s"
sudo docker logs kosha 2>&1 | grep -i "warmup" > /data/results-r7/warmup.log || true

~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/query_bench.py \
  --host http://127.0.0.1:8080 --namespace msmarco-10m \
  --queries-file /data/queries.txt --api-key sk-bench \
  --qps 8 --topk 10 --duration 1800 --timeout 600 \
  --operator or --no-cache \
  --phase cold --out /data/results-r7/cold_or_nocache.json
echo "COLD-PHASE-COMPLETE"

~/venv311/bin/python3 ~/kosha/scripts/bench/bm25_scale/query_bench.py \
  --host http://127.0.0.1:8080 --namespace msmarco-10m \
  --queries-file /data/queries.txt --api-key sk-bench \
  --qps 8 --topk 10 --duration 1800 --timeout 600 \
  --operator or --no-cache \
  --phase warm --out /data/results-r7/warm_or_nocache.json
echo "WARM-PHASE-COMPLETE"

sudo docker logs kosha > /data/results-r7/kosha_r7.log 2>&1
echo "PIPELINE-COMPLETE"
