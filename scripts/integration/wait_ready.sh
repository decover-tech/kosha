#!/usr/bin/env bash
# Waits until a Kosha pod answers /v1/readyz on each given local port
# (default 8080 8081), up to ~3 minutes each. Used by integration.yml and
# clients.yml after `docker compose up`.
#
#   scripts/integration/wait_ready.sh 8080 8081
set -euo pipefail

ports=("$@")
[ ${#ports[@]} -gt 0 ] || ports=(8080 8081)
for port in "${ports[@]}"; do
  for i in $(seq 1 90); do
    curl -fsS "http://localhost:$port/v1/readyz" >/dev/null 2>&1 && break
    [ "$i" = 90 ] && { echo "pod on :$port not ready"; exit 1; }
    sleep 2
  done
done
