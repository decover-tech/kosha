#!/usr/bin/env bash
# Fail if Kosha's own clients, bench and tooling call a legacy unversioned
# route (#350, plan acceptance 9). Every caller uses /v1; the legacy routes
# exist only for external clients that have not migrated yet
# (docs/api/versioning.md#legacy-routes).
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
  scripts/faults
  scripts/conformance
  scripts/integration
  scripts/eval
  scripts/relevance
  scripts/quickstart.py
  scripts/compare_search.py
  scripts/capture_cold_baseline.sh
  scripts/copy_es_to_kosha.py
  scripts/gen_es_migration_job.py
  scripts/tests
)
existing=()
for p in "${paths[@]}"; do
  [ -e "$p" ] && existing+=("$p")
done

# Lines that intentionally name a legacy route (the integration suite's
# legacy-route checks) carry `legacy-route-ok`; a fake server's
# `path.endswith("/search")` matches /v1 paths too and is not a call.
hits=$(grep -rnE "$pattern" "${existing[@]}" | grep -v 'legacy-route-ok' | grep -v 'endswith(' || true)

# A legacy route given as a bare relative path to an HTTP helper that joins
# it onto the base URL: http("POST", PORT, "index", ...) or
# transport.request("GET", "stats"). kosha_client's own `_request(method,
# "index", ...)` names an operation that it maps to /v1/namespaces/{ns}/...
# (client.py _V1_ACTIONS, pinned by the client tests), so that file is not
# checked by this pattern.
bare='"(GET|POST|PUT|DELETE)",[[:space:]]*([A-Za-z_][A-Za-z_0-9.]*,[[:space:]]*)?f?["'"'"'](index|search|flush|delete|replace|exists|stats|healthz)["'"'"']'
bare_hits=$(grep -rnE "$bare" "${existing[@]}" | grep -v '^clients/python/kosha_client/client.py:' | grep -v 'legacy-route-ok' || true)

if [ -n "$hits$bare_hits" ]; then
  echo "legacy unversioned routes are still called (use /v1/namespaces/{ns}/...):"
  [ -n "$hits" ] && echo "$hits"
  [ -n "$bare_hits" ] && echo "$bare_hits"
  exit 1
fi
echo "no legacy route callers"
