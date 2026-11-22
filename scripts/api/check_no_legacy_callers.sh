#!/usr/bin/env bash
# Fail if Kosha's own clients, bench and tooling call a legacy unversioned
# route (#350, plan acceptance 9). Every caller uses /v1; the legacy routes
# exist only for external clients that have not migrated yet
# (docs/api/versioning.md#legacy-routes).
#
# scripts/copy_es_to_kosha.py and scripts/gen_es_migration_job.py move to
# /v1 under #353 and are not checked here yet.
set -euo pipefail
cd "$(dirname "$0")/../.."

# A legacy path right after a quote, a paren, a host (f"{host}/search"), a port
# (localhost:8080/search) or a shell variable ($PORT/search).
pattern='(["'"'"'(]|[Hh][Oo][Ss][Tt]}|:[0-9]+|PORT)(/index|/search|/flush|/delete|/replace|/exists|/stats)(["'"'"'?) ]|$)'
paths=(
  clients/python/kosha_client
  crates/kosha-cli/src
  scripts/bench
  scripts/soak
  scripts/conformance
  scripts/integration
  scripts/eval
  scripts/relevance
  scripts/quickstart.py
  scripts/compare_search.py
  scripts/capture_cold_baseline.sh
)
existing=()
for p in "${paths[@]}"; do
  [ -e "$p" ] && existing+=("$p")
done

# Lines that intentionally name a legacy route (the integration suite's
# legacy-route checks) carry `legacy-route-ok`; a fake server's
# `path.endswith("/search")` matches /v1 paths too and is not a call.
if hits=$(grep -rnE "$pattern" "${existing[@]}" | grep -v 'legacy-route-ok' | grep -v 'endswith('); then
  echo "legacy unversioned routes are still called (use /v1/namespaces/{ns}/...):"
  echo "$hits"
  exit 1
fi
echo "no legacy route callers"
