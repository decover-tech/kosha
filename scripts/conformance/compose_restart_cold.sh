#!/usr/bin/env bash
# Restart kosha-server in the compose stack with an EMPTY local cache, so the
# next queries must hydrate every segment from MinIO and the manifest from
# Postgres — the cold-start path (see issues #179 / #184).
#
# Run from the repo root (the suite is normally invoked from there).
set -euo pipefail
compose=(docker compose -f docker-compose.yml -f scripts/conformance/docker-compose.conformance.yml)
"${compose[@]}" rm --stop --force kosha-server
docker volume rm --force kosha_kosha-data
"${compose[@]}" up -d kosha-server
