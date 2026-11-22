#!/bin/bash
# Run benchmark phases against an already-loaded (or restored) namespace.
# The phases-only complement to the pipeline_* scripts — what a
# restore-based run (snapshot_namespace.sh / restore_namespace.sh) or the
# benchmark GitHub Action executes.
#
# Usage:
#   [WARM_GATE=queries|preload] ./run_phases.sh bm25   <namespace> <out-dir> [qps] [duration]
#   [WARM_GATE=queries|preload] ./run_phases.sh vector <namespace> <out-dir> [qps] [duration]
#
# WARM_GATE selects how the namespace gets warm before measuring:
#
#   queries: the tpuf-benchmark protocol. The server runs WITHOUT
#     KOSHA_WARMUP_NAMESPACES (no boot preload) and with span promotion
#     off (benchmark.yml), the local cache is NOT wiped
#     (restore_namespace.sh already dropped this namespace unless it holds
#     this exact snapshot), and warm_gate.py sends the phase's own
#     workload queries until every query has come back fully cached
#     (timings.fully_cached) at least once and then 3 random ones in a
#     row do. Warmup is reported (warm_gate.log) but, as in tpuf, never
#     measured. Phases are warm only:
#       bm25:   warm_or_nocache, warm_or_cache
#       vector: knn_warm_nocache, knn_warm_cache
#     The warmup-gated "cold" phase below is dropped in this mode: it is
#     not turbopuffer cold either (tpuf cold sends disable_cache on every
#     query so every query reads object storage; Kosha has no equivalent
#     switch, and a phase after a full preload measures a warm engine).
#
#   preload (default here, for callers that predate the switch): the
#     protocol below, unchanged: wipe, restart, boot preload gated on
#     /readyz, then cold cache-off and warm phases.
#
# Preload mode: two phases each, cold cache-off then warm cache-on — the pair that
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
. "$(dirname "$0")/snapshot_cache.sh"

SUITE="${1:?usage: run_phases.sh bm25|vector <namespace> <out-dir> [qps] [duration]}"
NS="${2:?namespace required}"
OUT="${3:?out-dir required}"
QPS="${4:-8}"
DURATION="${5:-300}"
API_KEY="${KOSHA_API_KEY:-sk-bench}"
BENCH=~/kosha/scripts/bench/bm25_scale/query_bench.py
GATE=~/kosha/scripts/bench/bm25_scale/warm_gate.py
PY=~/venv311/bin/python3
WARM_GATE="${WARM_GATE:-preload}"
# What each hit returns: ids (doc_id + score, as tpuf queries do) or all.
RETURN_FIELDS="${RETURN_FIELDS:-ids}"
case "$RETURN_FIELDS" in ids|all) ;; *) echo "RETURN_FIELDS must be ids or all, got $RETURN_FIELDS"; exit 1 ;; esac
case "$WARM_GATE" in queries|preload) ;; *) echo "WARM_GATE must be queries or preload, got $WARM_GATE"; exit 1 ;; esac
echo "WARM_GATE=$WARM_GATE RETURN_FIELDS=$RETURN_FIELDS"

for f in --no-cache --raw-out --server-timings --return; do $PY $BENCH --help | grep -q -- "$f" || { echo "FLAGS-MISSING $f"; exit 1; }; done
[ "$SUITE" = "vector" ] && { $PY $BENCH --help | grep -q -- "--knn-embeddings" || { echo FLAGS-MISSING; exit 1; }; }
if [ "$WARM_GATE" = queries ]; then
  for f in --consecutive --knn-embeddings --operator --return; do $PY $GATE --help | grep -q -- "$f" || { echo "FLAGS-MISSING warm_gate $f"; exit 1; }; done
fi
sudo mkdir -p "$OUT/server" && sudo chown -R "$(id -u):$(id -g)" "$OUT"

# Server logs per stage (warmup, each phase) land in $OUT/server/ so they
# ship with the results artifact and get archived to S3 by the workflow.
# `docker logs --since` takes an RFC 3339 timestamp; ISO_NOW marks a stage.
iso_now() { date -u +%Y-%m-%dT%H:%M:%S.%NZ; }
save_server_log() { # save_server_log <name> <since> [until]
  sudo docker logs --timestamps --since "$2" ${3:+--until "$3"} kosha 2>&1 \
    | gzip > "$OUT/server/$1.log.gz" || true
}

if [ "$SUITE" = "bm25" ]; then
  WORKLOAD=(--operator or)
else
  WORKLOAD=(--knn-embeddings /data/queries_emb.f32)
fi

if [ "$WARM_GATE" = queries ]; then
# ── tpuf protocol: warm with the workload queries, measure warm only ─────
# No wipe and no restart: restore_namespace.sh restarted the server and
# kept the local cache only if it holds this exact snapshot.
sudo docker start kosha >/dev/null 2>&1 || true
T0=$(date +%s)
# No boot warmup is configured in this mode, so /readyz is green within
# seconds of the server answering at all.
for i in $(seq 120); do curl -sf http://127.0.0.1:8080/readyz >/dev/null 2>&1 && break; sleep 5
  [ "$i" -eq 120 ] && { echo "::error::/readyz not green after 600s (KOSHA_WARMUP_NAMESPACES set on the container?)"; curl -s http://127.0.0.1:8080/readyz || true; echo; exit 1; }; done
GATE_SINCE=$(iso_now)
set +e
$PY $GATE --host http://127.0.0.1:8080 --namespace "$NS" \
  --queries-file /data/queries.txt --api-key "$API_KEY" --topk 10 \
  "${WORKLOAD[@]}" --return "$RETURN_FIELDS" --timeout "${WARM_GATE_TIMEOUT_S:-10800}" 2>&1 | tee "$OUT/warm_gate.log"
GATE_RC=${PIPESTATUS[0]}
set -e
save_server_log warm_gate "$GATE_SINCE"
if [ "$GATE_RC" -ne 0 ]; then
  echo "WARM-GATE-FAILED rc=$GATE_RC (see $OUT/warm_gate.log)"
  sudo docker logs --tail 200 kosha 2>&1 || true
  exit 1
fi
echo "WARMUP-READY in $(( $(date +%s) - T0 ))s (warm gate, not measured)"
else
# ── cold reset, warmup-gated ─────────────────────────────────────────────
sudo docker stop kosha >/dev/null
# This mode measures warmup, so it always starts from an empty local
# store; wipe_local_cache also drops the reuse markers.
wipe_local_cache
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
fi

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
    --server-timings --raw-out "$OUT/$label.requests.jsonl.gz" --return "$RETURN_FIELDS" \
    "$@" --out "$OUT/$label.json"
  save_server_log "$label" "$since"
  echo "PHASE-COMPLETE $label"
}

if [ "$WARM_GATE" = queries ]; then
  # Warm only (see the header). Same queries and payloads as the gate.
  if [ "$SUITE" = "bm25" ]; then
    phase warm_or_nocache  --phase warm --operator or --no-cache
    phase warm_or_cache    --phase warm --operator or
  else
    phase knn_warm_nocache --phase warm --knn-embeddings /data/queries_emb.f32 --no-cache
    phase knn_warm_cache   --phase warm --knn-embeddings /data/queries_emb.f32
  fi
elif [ "$SUITE" = "bm25" ]; then
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
