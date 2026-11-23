#!/usr/bin/env bash
# Runs the Python, TypeScript and Go conformance runners against a running
# Kosha (default: the integration stack's query pod on :8081) through
# faultproxy.py. Exits non-zero if any runner fails; JUnit / logs land in
# build/conformance/.
#
#   clients/conformance/run.sh [upstream-url]
#
# Needs python3 with kosha-client and pytest installed (PYTHON=...), npm
# dependencies in clients/typescript (npm ci), and Go.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"
UPSTREAM=${1:-${KOSHA_CONFORMANCE_UPSTREAM:-http://localhost:8081}}
PROXY_PORT=${FAULTPROXY_PORT:-18081}
CONTROL_PORT=${FAULTPROXY_CONTROL_PORT:-18091}
PYTHON=${PYTHON:-python3}
GO=${GO:-go}
OUT="$ROOT/build/conformance"
mkdir -p "$OUT"

"$PYTHON" clients/conformance/faultproxy.py --upstream "$UPSTREAM" \
  --listen "$PROXY_PORT" --control "$CONTROL_PORT" >"$OUT/faultproxy.log" 2>&1 &
PROXY_PID=$!
trap 'kill $PROXY_PID 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  curl -fsS "http://127.0.0.1:$CONTROL_PORT/healthz" >/dev/null 2>&1 && break
  sleep 0.2
done
curl -fsS "http://127.0.0.1:$CONTROL_PORT/healthz" >/dev/null || { cat "$OUT/faultproxy.log"; exit 1; }

export KOSHA_CONFORMANCE_URL="http://127.0.0.1:$PROXY_PORT"
export KOSHA_CONFORMANCE_CONTROL="http://127.0.0.1:$CONTROL_PORT"
export KOSHA_CONFORMANCE_RUN_ID=${KOSHA_CONFORMANCE_RUN_ID:-$(date +%s)}

status=0
echo "== python"
"$PYTHON" -m pytest clients/python/tests/conformance -q --junitxml="$OUT/python.xml" || status=1
echo "== typescript"
(cd clients/typescript && npx vitest run test/conformance.test.ts \
  --reporter=default --reporter=junit --outputFile.junit="$OUT/typescript.xml") || status=1
echo "== go"
(cd clients/go && "$GO" test -tags conformance -run Conformance -count=1 -v ./... 2>&1 | tee "$OUT/go.log") || status=1

[ "$status" = 0 ] && echo "conformance: all three SDKs passed" || echo "conformance: FAILED (see $OUT)"
exit "$status"
