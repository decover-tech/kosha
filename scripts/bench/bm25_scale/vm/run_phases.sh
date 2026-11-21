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
# Two phases each, cold cache-off then warm cache-on — the pair that
# actually distinguishes "first touch" from "steady state", which is what
# most runs are checking. (bm25 used to also run a warm-cache-off phase in
# between; dropped — vector never had it, and it wasn't pulling its weight
# for the usual "did this change help" check.)
# "cold" here means: this script wipes the local store and restarts the
# kosha container first, gating on /readyz (warmup does the prefetch —
# configure KOSHA_WARMUP_NAMESPACES on the container for the namespace).
#
# [duration] defaults to 300s (quick, ~5 min/phase) — fine for warm-phase
# numbers and for cold p50/p90 sanity checks, i.e. most runs ("did this
# change help"). NOT enough to trust a *cold* p99: round 6 (RESULTS.md)
# measured the startup convoy — every query needing S3 hydration across
# every segment at t=0 — taking ~3 min to clear on its own, so a 5-min cold
# phase leaves only ~2 min (≈960 reqs @ 8 QPS) of genuinely post-convoy
# data, thin for a tail percentile. Pass 1800 explicitly (the full
# tpuf-protocol duration) whenever a cold p99 is going into a report.
set -euo pipefail

SUITE="${1:?usage: run_phases.sh bm25|vector <namespace> <out-dir> [qps] [duration]}"
NS="${2:?namespace required}"
OUT="${3:?out-dir required}"
QPS="${4:-8}"
DURATION="${5:-300}"
API_KEY="${KOSHA_API_KEY:-sk-bench}"
BENCH=~/kosha/scripts/bench/bm25_scale/query_bench.py
PY=~/venv311/bin/python3

for f in --no-cache --raw-out --server-timings; do $PY $BENCH --help | grep -q -- "$f" || { echo "FLAGS-MISSING $f"; exit 1; }; done
[ "$SUITE" = "vector" ] && { $PY $BENCH --help | grep -q -- "--knn-embeddings" || { echo FLAGS-MISSING; exit 1; }; }
sudo mkdir -p "$OUT/server" && sudo chown -R "$(id -u):$(id -g)" "$OUT"

# Server logs per stage (warmup, each phase) land in $OUT/server/ so they
# ship with the results artifact and get archived to S3 by the workflow.
# `docker logs --since` takes an RFC 3339 timestamp; ISO_NOW marks a stage.
iso_now() { date -u +%Y-%m-%dT%H:%M:%S.%NZ; }
save_server_log() { # save_server_log <name> <since> [until]
  sudo docker logs --timestamps --since "$2" ${3:+--until "$3"} kosha 2>&1 \
    | gzip > "$OUT/server/$1.log.gz" || true
}

# ── cold reset, warmup-gated ─────────────────────────────────────────────
sudo docker stop kosha >/dev/null
sudo rm -rf /data/kosha/data/*
T0=$(date +%s)
WARMUP_SINCE=$(iso_now)
sudo docker start kosha >/dev/null
# The 10M vector working set (~257GB from S3) can outlast an hour of
# warmup; WARMUP_TIMEOUT_S lets the caller budget for it. Progress every
# 5 minutes (bytes on local disk) so a slow warmup is distinguishable
# from a stuck one, and the server log on timeout.
WARMUP_TIMEOUT_S="${WARMUP_TIMEOUT_S:-3600}"
LAST_REPORT=$T0
until curl -sf http://127.0.0.1:8080/readyz >/dev/null 2>&1; do
  sleep 5
  NOW=$(date +%s)
  if [ $(( NOW - LAST_REPORT )) -ge 300 ]; then
    echo "WARMUP-PROGRESS $(( NOW - T0 ))s local=$(sudo du -sb /data/kosha 2>/dev/null | cut -f1) bytes"
    LAST_REPORT=$NOW
  fi
  if [ $(( NOW - T0 )) -gt "$WARMUP_TIMEOUT_S" ]; then
    echo "WARMUP-TIMEOUT after ${WARMUP_TIMEOUT_S}s"
    curl -s http://127.0.0.1:8080/readyz || true; echo
    sudo docker logs --tail 200 kosha 2>&1 || true
    save_server_log warmup "$WARMUP_SINCE"
    exit 1
  fi
done
echo "WARMUP-READY in $(( $(date +%s) - T0 ))s"
save_server_log warmup "$WARMUP_SINCE"

# Every phase also writes one record per request (--raw-out: query,
# term count, client send lag, latency, hits, and the server's own phase
# timings via --server-timings) and its server log, so a tail can be
# decomposed after the fact: analyze_tail.py <label>.requests.jsonl.gz.
phase() { # phase <label> <extra-args...>
  local label="$1"; shift
  local since; since=$(iso_now)
  $PY $BENCH \
    --host http://127.0.0.1:8080 --namespace "$NS" \
    --queries-file /data/queries.txt --api-key "$API_KEY" \
    --qps "$QPS" --topk 10 --duration "$DURATION" --timeout 600 \
    --server-timings --raw-out "$OUT/$label.requests.jsonl.gz" \
    "$@" --out "$OUT/$label.json"
  save_server_log "$label" "$since"
  echo "PHASE-COMPLETE $label"
}

if [ "$SUITE" = "bm25" ]; then
  phase cold_or_nocache  --phase cold --operator or --no-cache
  # Same process, caches now warm, result cache still bypassed: the
  # engine's steady-state number, comparable to tpuf's published warm.
  phase warm_or_nocache  --phase warm --operator or --no-cache
  phase warm_or_cache    --phase warm --operator or
else
  phase knn_cold_nocache --phase cold --knn-embeddings /data/queries_emb.f32 --no-cache
  phase knn_warm_cache   --phase warm --knn-embeddings /data/queries_emb.f32
fi
echo "PHASES-COMPLETE"
