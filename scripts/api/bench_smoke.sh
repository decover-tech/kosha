#!/usr/bin/env bash
# Run the BM25 bench scripts (scripts/bench/bm25_scale/load_corpus.py and
# query_bench.py) briefly against a server with KOSHA_LEGACY_ROUTES=off
# (#350, plan acceptance 8): a bench that regressed to a legacy route gets
# 410s here and fails.
#
#   scripts/api/bench_smoke.sh <host> <api-key> <admin-key>
#
# Needs python3 with `requests`.
set -euo pipefail
cd "$(dirname "$0")/../.."
host=${1:?host}
api_key=${2:?api key}
admin_key=${3:?admin key}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/corpus"
python3 - "$work" <<'PY'
import json, sys
work = sys.argv[1]
words = "contract breach warranty negligence consent liability damages court".split()
with open(f"{work}/corpus/shard-00000.ndjson", "w") as f:
    for i in range(2000):
        text = " ".join(words[(i * 7 + j * 3) % len(words)] for j in range(16))
        f.write(json.dumps({"id": f"d{i:05d}", "text": text}) + "\n")
with open(f"{work}/queries.txt", "w") as f:
    f.write("contract breach\nwarranty\nnegligence consent\ncourt damages\n")
PY

namespace="bench-smoke-$$"
python3 scripts/bench/bm25_scale/load_corpus.py --host "$host" --api-key "$api_key" \
  --namespace "$namespace" --corpus-dir "$work/corpus" --batch-size 500 --concurrency 2
python3 scripts/bench/bm25_scale/query_bench.py --host "$host" --api-key "$api_key" \
  --stats-api-key "$admin_key" --namespace "$namespace" --queries-file "$work/queries.txt" \
  --phase warm --qps 20 --duration 3 --out "$work/bench.json"
python3 - "$work/bench.json" <<'PY'
import json, sys
report = json.load(open(sys.argv[1]))
def find(node, key):
    if isinstance(node, dict):
        if key in node:
            yield node[key]
        for value in node.values():
            yield from find(value, key)
    elif isinstance(node, list):
        for value in node:
            yield from find(value, key)
errors = sum(v for v in find(report, "errors") if isinstance(v, int))
if errors:
    sys.exit(f"query_bench saw {errors} failed request(s)")
print("bench smoke: load_corpus and query_bench ran on /v1 only")
PY
