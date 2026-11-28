#!/usr/bin/env bash
# Start OpenSearch 2.18.0 and a Kosha image, index one small synthetic
# corpus into both, and write scripts/bench/compare/out/results.json.
#
# This is a single-client microbench. It is not the 10M MS MARCO run.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
OUT="$HERE/out"
BUILD_KOSHA=0

for arg in "$@"; do
  case "$arg" in
    --build-kosha) BUILD_KOSHA=1 ;;
    -h|--help)
      echo "usage: $0 [--build-kosha]"
      echo "KOSHA_IMAGE overrides the Kosha image (default decoverai/kosha:0.1.0)."
      exit 0
      ;;
    *)
      echo "unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

if [[ "$BUILD_KOSHA" -eq 1 ]]; then
  export KOSHA_IMAGE="${KOSHA_IMAGE:-kosha:local}"
  docker build -t "$KOSHA_IMAGE" "$ROOT"
fi

mkdir -p "$OUT"
python3 "$HERE/prepare_corpus.py" --out-dir "$OUT"

docker compose -f "$HERE/docker-compose.yml" up -d

cleanup() {
  docker compose -f "$HERE/docker-compose.yml" down
}
trap cleanup EXIT

echo "waiting for OpenSearch and Kosha..."
for _ in $(seq 1 90); do
  if curl -sf "http://localhost:9200" >/dev/null && curl -sf "http://localhost:8080/healthz" >/dev/null; then
    break
  fi
  sleep 2
done
curl -sf "http://localhost:9200" >/dev/null
curl -sf "http://localhost:8080/healthz" >/dev/null

python3 "$ROOT/scripts/bench/run_benchmark.py" \
  --corpus "$OUT/corpus.jsonl" \
  --queries "$OUT/queries.json" \
  --os-host "http://localhost:9200" \
  --kosha-host "http://localhost:8080" \
  --index compare \
  --mode lexical \
  --reps 20 \
  --out "$OUT/results.json"

python3 - << PY
import json
p = "$OUT/results.json"
data = json.load(open(p))
summary = data["modes"]["lexical"]["summary"]
print(json.dumps(summary, indent=2))
print("wrote", p)
print("These numbers are a local microbench. They do not replace docs/benchmarks.md.")
PY
