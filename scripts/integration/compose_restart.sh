#!/usr/bin/env bash
# Restart one kosha service of the integration stack.
#
#   compose_restart.sh kosha-ingest          # stop + start, local volume kept
#   compose_restart.sh kosha-ingest --wipe   # also delete its data volume
#
# --wipe is the "node lost its disk" case: the replacement must rebuild
# everything from MinIO (segments, S3 WAL) and Postgres (manifests).
# Run from the repo root, where run.py's CI step runs it.
set -euo pipefail
service=${1:?usage: compose_restart.sh <kosha-ingest|kosha-query> [--wipe]}
wipe=${2:-}
compose=(docker compose -f docker-compose.yml -f scripts/integration/docker-compose.integration.yml)
case "$service" in
  kosha-ingest) volume=kosha_ingest-data ;;
  kosha-query) volume=kosha_query-data ;;
  *) echo "unknown service $service" >&2; exit 2 ;;
esac
"${compose[@]}" rm --stop --force "$service"
if [ "$wipe" = "--wipe" ]; then
  docker volume rm --force "$volume"
fi
"${compose[@]}" up -d --no-deps "$service"
