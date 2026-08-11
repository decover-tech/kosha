#!/bin/bash
# Run benchmark phases against an already-loaded (or restored) namespace.
# The phases-only complement to the pipeline_* scripts — what a
# restore-based run (snapshot_namespace.sh / restore_namespace.sh) or the
# benchmark GitHub Action executes.
#
# Usage:
#   ./run_phases.sh bm25   <namespace> <out-dir> [qps] [duration]
#   ./run_phases.sh vector <namespace> <out-dir> [qps] [duration]
#
# bm25:   cold OR cache-off phase, then warm OR cache-off + warm cache-on.
# vector: cold kNN cache-off phase, then warm kNN cache-on.
# "cold" here means: this script wipes the local store and restarts the
# kosha container first, gating on /readyz (warmup does the prefetch —
# configure KOSHA_WARMUP_NAMESPACES on the container for the namespace).
set -euo pipefail

SUITE="${1:?usage: run_phases.sh bm25|vector <namespace> <out-dir> [qps] [duration]}"
NS="${2:?namespace required}"
OUT="${3:?out-dir required}"
QPS="${4:-8}"
DURATION="${5:-1800}"
API_KEY="${KOSHA_API_KEY:-sk-bench}"
BENCH=~/kosha/scripts/bench/bm25_scale/query_bench.py
PY=~/venv311/bin/python3

$PY $BENCH --help | grep -q -- "--no-cache" || { echo FLAGS-MISSING; exit 1; }
[ "$SUITE" = "vector" ] && { $PY $BENCH --help | grep -q -- "--knn-embeddings" || { echo FLAGS-MISSING; exit 1; }; }
sudo mkdir -p "$OUT" && sudo chown -R "$(id -u):$(id -g)" "$OUT"

# ── cold reset, warmup-gated ─────────────────────────────────────────────
sudo docker stop kosha >/dev/null
sudo rm -rf /data/kosha/data/*
T0=$(date +%s)
sudo docker start kosha >/dev/null
until curl -sf http://127.0.0.1:8080/readyz >/dev/null 2>&1; do
  sleep 5
  [ $(( $(date +%s) - T0 )) -gt 3600 ] && { echo "WARMUP-TIMEOUT"; exit 1; }
done
echo "WARMUP-READY in $(( $(date +%s) - T0 ))s"

phase() { # phase <label> <extra-args...>
  local label="$1"; shift
  $PY $BENCH \
    --host http://127.0.0.1:8080 --namespace "$NS" \
    --queries-file /data/queries.txt --api-key "$API_KEY" \
    --qps "$QPS" --topk 10 --duration "$DURATION" --timeout 600 \
    "$@" --out "$OUT/$label.json"
  echo "PHASE-COMPLETE $label"
}

if [ "$SUITE" = "bm25" ]; then
  phase cold_or_nocache  --phase cold --operator or --no-cache
  phase warm_or_nocache  --phase warm --operator or --no-cache
  phase warm_or_cache    --phase warm --operator or
else
  phase knn_cold_nocache --phase cold --knn-embeddings /data/queries_emb.f32 --no-cache
  phase knn_warm_cache   --phase warm --knn-embeddings /data/queries_emb.f32
fi
echo "PHASES-COMPLETE"
