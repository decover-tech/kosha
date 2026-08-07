#!/usr/bin/env bash
# Capture cold-read baselines for the cold-read optimization plan (Day 1).
#
# For each target namespace: force a fully-cold query tier (rollout restart
# wipes the emptyDir NVMe cache AND the in-memory segment cache), fire one
# cold search then two warm repeats, and print the server's per-phase
# `search timing:` log lines — hydrate_ms/files/mb, queue/admit/score/
# materialize, cold-vs-cached opens.
#
# Prereqs:
#   - kubectl context pointed at the staging cluster (decoverai-nonprod)
#   - KOSHA_API_KEY exported (any valid staging key)
#
# Usage:
#   KOSHA_API_KEY=... ./scripts/capture_cold_baseline.sh [ns1 ns2 ...]
#
# With no args: prints /stats (all namespaces, doc/segment counts) and picks
# paragraph_index_hnsw plus the largest-by-docs namespace automatically.
set -euo pipefail

NS_K8S=kosha
LOCAL_PORT=18080
QUERY_TEXT=${QUERY_TEXT:-the}
: "${KOSHA_API_KEY:?export KOSHA_API_KEY (any valid staging API key)}"

say() { printf '\n=== %s ===\n' "$*"; }

port_forward() {
  kubectl -n "$NS_K8S" port-forward svc/kosha-service "$LOCAL_PORT:8080" >/dev/null 2>&1 &
  PF_PID=$!
  trap 'kill "$PF_PID" 2>/dev/null || true' EXIT
  # Wait for the tunnel.
  for _ in $(seq 1 30); do
    curl -sf -o /dev/null "http://127.0.0.1:$LOCAL_PORT/healthz" && return 0
    sleep 1
  done
  echo "port-forward to kosha-service never became ready" >&2
  exit 1
}

kcurl() { curl -sf -H "Authorization: Bearer $KOSHA_API_KEY" "$@"; }

search_once() {
  local ns=$1 label=$2
  local t0 t1
  t0=$(python3 -c 'import time; print(time.time())')
  kcurl -X POST "http://127.0.0.1:$LOCAL_PORT/search" \
    -H 'Content-Type: application/json' \
    -d "{\"namespace\": \"$ns\", \"query_text\": \"$QUERY_TEXT\", \"max_results\": 10}" \
    -o /dev/null
  t1=$(python3 -c 'import time; print(time.time())')
  printf '  %-12s client wall: %6.0f ms\n' "$label" \
    "$(python3 -c "print(($t1 - $t0) * 1000)")"
}

say "resolving target namespaces"
port_forward
STATS_JSON=$(kcurl "http://127.0.0.1:$LOCAL_PORT/stats")
echo "$STATS_JSON" | python3 -c '
import json, sys
stats = json.load(sys.stdin)
rows = sorted(stats.get("namespaces", []), key=lambda r: -r.get("documents", 0))
print(f"{'namespace':<50} {'documents':>12} {'segments':>9}")
for r in rows[:15]:
    print(f"{r['namespace']:<50} {r['documents']:>12} {r['segments']:>9}")
'

if [ "$#" -gt 0 ]; then
  TARGETS=("$@")
else
  # Default: paragraph_index_hnsw + the largest namespace by document count.
  BIGGEST=$(echo "$STATS_JSON" | python3 -c '
import json, sys
rows = json.load(sys.stdin).get("namespaces", [])
rows = [r for r in rows if r.get("documents", 0) > 0]
print(max(rows, key=lambda r: r["documents"])["namespace"] if rows else "")
')
  TARGETS=(paragraph_index_hnsw)
  [ -n "$BIGGEST" ] && [ "$BIGGEST" != "paragraph_index_hnsw" ] && TARGETS+=("$BIGGEST")
fi
echo
echo "targets: ${TARGETS[*]}"

for ns in "${TARGETS[@]}"; do
  say "forcing cold query tier (rollout restart wipes emptyDir + segment cache)"
  MEASURE_START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  kubectl -n "$NS_K8S" rollout restart deployment/kosha-query
  kubectl -n "$NS_K8S" rollout status deployment/kosha-query --timeout=5m
  # Rollout restart kills the port-forward's backing pod; re-establish.
  kill "$PF_PID" 2>/dev/null || true
  sleep 2
  port_forward

  say "namespace: $ns — 1 cold + 2 warm searches (query_text=\"$QUERY_TEXT\")"
  search_once "$ns" cold
  search_once "$ns" warm-1
  search_once "$ns" warm-2

  say "server-side phase breakdown ($ns)"
  # --since-time scopes to this round; all query pods, cold line first.
  kubectl -n "$NS_K8S" logs deployment/kosha-query --since-time="$MEASURE_START" --all-containers 2>/dev/null \
    | grep "search timing:" | grep "ns=$ns" || echo "  (no timing lines found — check pod logs manually)"
done

say "done — paste the 'search timing:' lines into the baseline table"
