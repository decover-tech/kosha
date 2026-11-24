#!/bin/bash
# Run benchmark phases against an already-loaded (or restored) namespace.
# The phases-only complement to the pipeline_* scripts — what a
# restore-based run (snapshot_namespace.sh / restore_namespace.sh) or the
# benchmark GitHub Action executes.
#
# Usage:
#   [WARM_GATE=queries|preload|timed|cold] ./run_phases.sh bm25   <namespace> <out-dir> [qps] [duration]
#   [WARM_GATE=queries|preload|timed|cold] ./run_phases.sh vector <namespace> <out-dir> [qps] [duration]
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
#   timed (research #389: bounded-cache runs): with KOSHA_CACHE_MAX_BYTES
#     below the working set, "every query fully cached" never happens (later
#     queries evict what earlier ones need), so the queries gate would spin
#     to its timeout. This mode instead:
#       1. empties the local store (wipe_local_cache, as preload mode does)
#          and restarts the server. The bound covers the whole data dir, so
#          another namespace left there would share it or be evicted by
#          it; and every arm, bounded or not, starts from the same empty
#          state instead of whatever the previous run left. On a kept VM
#          this also drops the other suite cache (the ~2 h vector warmup);
#       2. runs the workload unmeasured for WARM_FILL_S seconds (default
#          900) to reach steady state (warm_fill.log, not a result);
#       3. measures the no-result-cache phase PHASE_REPEATS times
#          (default 1): warm_or_nocache_rN / knn_warm_nocache_rN. The
#          result-cache phase is skipped: it hides the disk cache.
#     It also samples the local footprint (du of the data dir, every 60 s,
#     footprint.tsv) and sums the server's "disk cache evicted" lines per
#     stage (evictions.tsv), so arms can be compared at equal real
#     footprint and their miss work is on record. Misses are part of the
#     measured latency by design.
#
#   QUERY_ORDER (any mode; default cycle = the fixed loop over the
#   queries file, as before): shuffle or zipf, passed to query_bench.py
#   --order (seeded, so arms see the same sequence).
#   cold (research, #380): a cold phase with NO preload at all. Wipe the
#     local store, restart, and measure first-touch queries straight away:
#     every byte a query needs comes from S3 during the measured phase, so
#     the per-query hydrate_files / hydrate_mb / s3_bytes / vector_gets /
#     postings_gets fields of the `search timing:` lines in
#     server/<phase>.log.gz are the cold cost of the layout. The container
#     must run WITHOUT KOSHA_WARMUP_NAMESPACES and with
#     KOSHA_WARMUP_USAGE=off (else the persisted usage warm set is warmed
#     at boot), and with span promotion off (benchmark.yml does all three
#     for warm_gate=cold; this script refuses to run otherwise). One phase
#     only: knn_cold_nocache (vector) or cold_or_nocache (bm25). Note that
#     a first-touch kNN query still hydrates footer, inverted.idx,
#     doc_store.offsets and vector.offsets of EVERY segment (knn_plan opens
#     every segment to rank its centroids), so the first queries pay a
#     per-segment convoy whatever the layout; read the steady part of the
#     phase separately from its first minutes.
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
case "$WARM_GATE" in queries|preload|timed|cold) ;; *) echo "WARM_GATE must be queries, preload, timed or cold, got $WARM_GATE"; exit 1 ;; esac
QUERY_ORDER="${QUERY_ORDER:-cycle}"
case "$QUERY_ORDER" in cycle|shuffle|zipf) ;; *) echo "QUERY_ORDER must be cycle, shuffle or zipf, got $QUERY_ORDER"; exit 1 ;; esac
WARM_FILL_S="${WARM_FILL_S:-900}"
PHASE_REPEATS="${PHASE_REPEATS:-1}"
case "$WARM_FILL_S$PHASE_REPEATS" in *[!0-9]*|'') echo "WARM_FILL_S and PHASE_REPEATS must be integers"; exit 1 ;; esac
# The default (cycle) passes no flag, so default runs send exactly the
# commands they did before. Otherwise each stage (warm fill, each phase)
# draws with the next seed, the same sequence of seeds for every arm, so
# a measured phase never replays the fill sequence. The zipf popularity
# ranking stays fixed (query_bench.py --zipf-rank-seed).
ORDER_SEED=389
ORDER_ARGS=()
next_order_args() {
  ORDER_ARGS=()
  if [ "$QUERY_ORDER" != cycle ]; then
    ORDER_ARGS=(--order "$QUERY_ORDER" --order-seed "$ORDER_SEED")
    ORDER_SEED=$((ORDER_SEED + 1))
  fi
}
echo "WARM_GATE=$WARM_GATE RETURN_FIELDS=$RETURN_FIELDS QUERY_ORDER=$QUERY_ORDER"

for f in --no-cache --raw-out --server-timings --return; do $PY $BENCH --help | grep -q -- "$f" || { echo "FLAGS-MISSING $f"; exit 1; }; done
[ "$QUERY_ORDER" = cycle ] || { $PY $BENCH --help | grep -q -- "--order" || { echo "FLAGS-MISSING --order"; exit 1; }; }
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

# Research #389 bookkeeping for timed mode (see the header).
local_bytes() { sudo du -sb "${KOSHA_LOCAL_DATA:?}" 2>/dev/null | cut -f1; }
count_evictions() { # count_evictions <stage>: files + MB the server evicted in that stage log
  zcat "$OUT/server/$1.log.gz" 2>/dev/null \
    | awk '/disk cache evicted:/ { for (i = 1; i <= NF; i++) { split($i, kv, "="); if (kv[1] == "files") f += kv[2]; if (kv[1] == "freed_mb") m += kv[2] } } END { printf "%d\t%.1f\n", f, m }'
}
FOOTPRINT_PID=""
stop_footprint() { if [ -n "$FOOTPRINT_PID" ]; then kill "$FOOTPRINT_PID" 2>/dev/null || true; fi; FOOTPRINT_PID=""; }
trap stop_footprint EXIT

if [ "$WARM_GATE" = timed ]; then
# ── bounded-cache protocol: single namespace, timed fill, steady state ───
sudo docker stop kosha >/dev/null
# Empty local store (also drops every local.* reuse marker): see step 1.
wipe_local_cache
sudo docker start kosha >/dev/null
for i in $(seq 120); do curl -sf http://127.0.0.1:8080/v1/readyz >/dev/null 2>&1 && break; sleep 5
  [ "$i" -eq 120 ] && { echo "::error::/v1/readyz not green after 600s"; exit 1; }; done
echo "LOCAL-BYTES-START $(local_bytes)"
( while true; do printf '%s\t%s\n' "$(date +%s)" "$(local_bytes)"; sleep 60; done ) > "$OUT/footprint.tsv" &
FOOTPRINT_PID=$!
printf 'stage\tevicted_files\tevicted_mb\n' > "$OUT/evictions.tsv"
FILL_SINCE=$(iso_now)
T0=$(date +%s)
NOCACHE=(--no-cache)
next_order_args
$PY $BENCH --host http://127.0.0.1:8080 --namespace "$NS" \
  --queries-file /data/queries.txt --api-key "$API_KEY" \
  --qps "$QPS" --topk 10 --duration "$WARM_FILL_S" --timeout 600 --return "$RETURN_FIELDS" \
  --phase warm "${WORKLOAD[@]}" "${NOCACHE[@]}" ${ORDER_ARGS[@]+"${ORDER_ARGS[@]}"} 2>&1 | tee "$OUT/warm_fill.log"
save_server_log warm_fill "$FILL_SINCE"
printf 'warm_fill\t%s\n' "$(count_evictions warm_fill)" >> "$OUT/evictions.tsv"
echo "WARM-FILL-DONE in $(( $(date +%s) - T0 ))s local=$(local_bytes) bytes (not measured)"
elif [ "$WARM_GATE" = queries ]; then
# ── tpuf protocol: warm with the workload queries, measure warm only ─────
# No wipe and no restart: restore_namespace.sh restarted the server and
# kept the local cache only if it holds this exact snapshot.
sudo docker start kosha >/dev/null 2>&1 || true
T0=$(date +%s)
# No boot warmup is configured in this mode, so /readyz is green within
# seconds of the server answering at all.
for i in $(seq 120); do curl -sf http://127.0.0.1:8080/v1/readyz >/dev/null 2>&1 && break; sleep 5
  [ "$i" -eq 120 ] && { echo "::error::/v1/readyz not green after 600s (KOSHA_WARMUP_NAMESPACES set on the container?)"; curl -s http://127.0.0.1:8080/v1/readyz || true; echo; exit 1; }; done
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
elif [ "$WARM_GATE" = cold ]; then
# ── cold, no preload: wipe, restart, measure first touch ─────────────────
# Refuse to call anything cold that a boot warmup could have prefetched.
ENV_DUMP="$(sudo docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' kosha)"
if printf '%s\n' "$ENV_DUMP" | grep -q '^KOSHA_WARMUP_NAMESPACES='; then
  echo "::error::WARM_GATE=cold but the kosha container sets KOSHA_WARMUP_NAMESPACES (boot preload)"; exit 1
fi
if ! printf '%s\n' "$ENV_DUMP" | grep -qE '^KOSHA_WARMUP_USAGE=(0|false|off)$'; then
  echo "::error::WARM_GATE=cold needs KOSHA_WARMUP_USAGE=off on the kosha container (usage warm set is warmed at boot otherwise)"; exit 1
fi
sudo docker stop kosha >/dev/null
wipe_local_cache
T0=$(date +%s)
BOOT_SINCE=$(iso_now)
sudo docker start kosha >/dev/null
for i in $(seq 120); do curl -sf http://127.0.0.1:8080/v1/readyz >/dev/null 2>&1 && break; sleep 5
  [ "$i" -eq 120 ] && { echo "::error::/v1/readyz not green after 600s with no warmup configured"; curl -s http://127.0.0.1:8080/v1/readyz || true; echo; exit 1; }; done
save_server_log boot "$BOOT_SINCE"
echo "COLD-READY in $(( $(date +%s) - T0 ))s local=$(sudo du -sb /data/kosha 2>/dev/null | cut -f1) bytes (no preload)"
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
until curl -sf http://127.0.0.1:8080/v1/readyz >/dev/null 2>&1; do
  sleep 5
  NOW=$(date +%s)
  if [ $(( NOW - LAST_REPORT )) -ge 300 ]; then
    echo "WARMUP-PROGRESS $(( NOW - T0 ))s local=$(sudo du -sb /data/kosha 2>/dev/null | cut -f1) bytes"
    LAST_REPORT=$NOW
  fi
  if [ $(( NOW - T0 )) -gt "$WARMUP_TIMEOUT_S" ]; then
    echo "WARMUP-TIMEOUT after ${WARMUP_TIMEOUT_S}s"
    curl -s http://127.0.0.1:8080/v1/readyz || true; echo
    sudo docker logs --tail 200 kosha 2>&1 || true
    save_server_log warmup "$WARMUP_SINCE"
    exit 1
  fi
done
echo "WARMUP-READY in $(( $(date +%s) - T0 ))s"
save_server_log warmup "$WARMUP_SINCE"
fi

# Local cache bytes per segment file class (#381), once warm and again
# after the phases: whole files and span-cache entries separately, so a run
# shows what the measured phases actually kept local. Never fails the run.
local_bytes() { # local_bytes <label>
  "$(dirname "$0")/local_bytes.sh" "$NS" default "$1" 2>&1 | tee -a "$OUT/local_bytes.txt" || true
}
local_bytes warm

# Every phase also writes one record per request (--raw-out: query,
# term count, client send lag, latency, hits, and the server's own phase
# timings via --server-timings) and its server log, so a tail can be
# decomposed after the fact: analyze_tail.py <label>.requests.jsonl.gz.
# --raw-ids adds each response's ranked doc_ids and scores, so two runs over
# different snapshots can be checked for identical results per query:
# topk_parity.py <a>.requests.jsonl.gz <b>.requests.jsonl.gz (#385).
phase() { # phase <label> <extra-args...>
  local label="$1"; shift
  local since; since=$(iso_now)
  next_order_args
  $PY $BENCH \
    --host http://127.0.0.1:8080 --namespace "$NS" \
    --queries-file /data/queries.txt --api-key "$API_KEY" \
    --qps "$QPS" --topk 10 --duration "$DURATION" --timeout 600 \
    --server-timings --raw-out "$OUT/$label.requests.jsonl.gz" --raw-ids --return "$RETURN_FIELDS" \
    --stats-api-key "${KOSHA_ADMIN_API_KEY:-sk-bench-admin}" \
    "$@" ${ORDER_ARGS[@]+"${ORDER_ARGS[@]}"} --out "$OUT/$label.json"
  save_server_log "$label" "$since"
  if [ "$WARM_GATE" = timed ]; then
    printf '%s\t%s\n' "$label" "$(count_evictions "$label")" >> "$OUT/evictions.tsv"
  fi
  echo "PHASE-COMPLETE $label"
}

if [ "$WARM_GATE" = timed ]; then
  # Steady state under the bound, no result cache, repeated (see header).
  for r in $(seq "$PHASE_REPEATS"); do
    if [ "$SUITE" = "bm25" ]; then
      phase "warm_or_nocache_r$r"  --phase warm --operator or --no-cache
    else
      phase "knn_warm_nocache_r$r" --phase warm --knn-embeddings /data/queries_emb.f32 --no-cache
    fi
  done
  stop_footprint
  echo "LOCAL-BYTES-END $(local_bytes)"
elif [ "$WARM_GATE" = queries ]; then
  # Warm only (see the header). Same queries and payloads as the gate.
  if [ "$SUITE" = "bm25" ]; then
    phase warm_or_nocache  --phase warm --operator or --no-cache
    phase warm_or_cache    --phase warm --operator or
  else
    phase knn_warm_nocache --phase warm --knn-embeddings /data/queries_emb.f32 --no-cache
    phase knn_warm_cache   --phase warm --knn-embeddings /data/queries_emb.f32
  fi
elif [ "$WARM_GATE" = cold ]; then
  # First touch only (see the header); a warm phase here would just repeat
  # warm_gate=queries without its gate.
  if [ "$SUITE" = "bm25" ]; then
    phase cold_or_nocache  --phase cold --operator or --no-cache
  else
    phase knn_cold_nocache --phase cold --knn-embeddings /data/queries_emb.f32 --no-cache
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
local_bytes after_phases
echo "PHASES-COMPLETE"
