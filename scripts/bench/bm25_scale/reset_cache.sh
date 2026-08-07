#!/usr/bin/env bash
# Force a genuinely "cold" Kosha for the next query_bench.py run:
#   1. exec into the running pod and empty its on-disk NVMe cache
#      (/var/cache/kosha, a hostPath mount — survives pod restarts on its own,
#      so it must be cleared explicitly).
#   2. delete the pod so the replacement starts with an empty in-memory
#      parsed-segment cache too (kosha-query::SegmentCache is process memory).
#
# Namespace segments themselves are NOT touched — this only evicts caches,
# never data. Safe to run against a shared staging Kosha: other namespaces'
# data is unaffected, but they DO lose their warm cache too (single shared
# on-disk cache + single shared in-process segment cache per pod), so don't
# run this against a Kosha instance serving other latency-sensitive traffic
# without warning whoever owns that traffic.
#
# Usage: ./reset_cache.sh [k8s-namespace] [deployment-name]
set -euo pipefail

K8S_NAMESPACE="${1:-kosha}"
DEPLOYMENT="${2:-kosha}"

echo "resolving current pod for deployment/$DEPLOYMENT in ns/$K8S_NAMESPACE..."
POD=$(kubectl -n "$K8S_NAMESPACE" get pod -l app="$DEPLOYMENT" -o jsonpath='{.items[0].metadata.name}')
echo "pod: $POD"

echo "clearing on-disk cache at /var/cache/kosha..."
kubectl -n "$K8S_NAMESPACE" exec "$POD" -c kosha -- sh -c 'rm -rf /var/cache/kosha/* 2>/dev/null || true'

echo "deleting pod to reset in-memory segment cache (deployment will recreate it)..."
kubectl -n "$K8S_NAMESPACE" delete pod "$POD"

echo "waiting for replacement pod to become ready..."
kubectl -n "$K8S_NAMESPACE" rollout status "deployment/$DEPLOYMENT" --timeout=180s

echo "cold cache ready."
