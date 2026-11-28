#!/bin/bash
# Single-node OpenSearch or Elasticsearch on the bench VM, same MS MARCO
# corpus and the same open-loop client as run_phases.sh.
#
# The Kosha container is stopped first. A 31g heap does not fit beside it.
# The index is wiped so ingest time starts from an empty data directory.
#
# Usage: run_lucene.sh <opensearch|elasticsearch> <out-dir> [qps] [duration] [ids|source]
set -euo pipefail

ENGINE="${1:?usage: run_lucene.sh opensearch|elasticsearch <out-dir> [qps] [duration] [ids|source]}"
OUT="${2:?out-dir required}"
QPS="${3:-8}"
DURATION="${4:-300}"
RETURN_FIELDS="${5:-ids}"
HERE="$(cd "$(dirname "$0")" && pwd)"
PY="${PY:-$HOME/venv311/bin/python3}"
CORPUS="${CORPUS:-/data/msmarco-10m}"
CORPUS_S3="${CORPUS_S3:-s3://decoverai-bench-corpus-cache/msmarco-10m}"
DATA="/data/lucene/$ENGINE"
SHARDS="${LUCENE_SHARDS:-8}"
MAX_SEGMENTS="${LUCENE_MAX_SEGMENTS:-1}"
ORDER="${QUERY_ORDER:-cycle}"

case "$ENGINE" in
  opensearch|elasticsearch) ;;
  *) echo "engine must be opensearch or elasticsearch, got $ENGINE"; exit 1 ;;
esac
case "$QPS" in
  ''|*[!0-9.]*) echo "qps must be a number, got $QPS"; exit 1 ;;
esac
case "$DURATION" in
  ''|*[!0-9]*) echo "duration must be whole seconds, got $DURATION"; exit 1 ;;
esac
case "$RETURN_FIELDS" in
  ids|source) ;;
  *) echo "return fields must be ids or source, got $RETURN_FIELDS"; exit 1 ;;
esac
case "$ORDER" in
  cycle|shuffle|zipf) ;;
  *) echo "QUERY_ORDER must be cycle, shuffle or zipf, got $ORDER"; exit 1 ;;
esac
case "$SHARDS" in
  ''|*[!0-9]*) echo "LUCENE_SHARDS must be an integer, got $SHARDS"; exit 1 ;;
esac
case "$MAX_SEGMENTS" in
  ''|*[!0-9]*) echo "LUCENE_MAX_SEGMENTS must be an integer, got $MAX_SEGMENTS"; exit 1 ;;
esac

$PY "$HERE/../load_corpus_opensearch.py" --help | grep -q -- "--engine" || { echo "FLAGS-MISSING load --engine"; exit 1; }
$PY "$HERE/../query_bench_opensearch.py" --help | grep -q -- "--cache-mode" || { echo "FLAGS-MISSING bench --cache-mode"; exit 1; }
$PY "$HERE/../query_bench.py" --help | grep -q -- "--no-cache" || { echo "FLAGS-MISSING kosha --no-cache"; exit 1; }

echo "syncing corpus $CORPUS_S3 -> $CORPUS"
sudo mkdir -p "$CORPUS" "$OUT/server" "$DATA"
sudo chown -R "$(id -u):$(id -g)" "$CORPUS" "$OUT"
aws s3 sync --only-show-errors "$CORPUS_S3" "$CORPUS" --exclude "*" --include "shard-*.ndjson" --include "queries.txt" --include "manifest.json"
[ -s "$CORPUS/queries.txt" ] || { echo "queries.txt missing after sync"; exit 1; }

# Lucene refuses to start when this is below 262144.
sudo sysctl -w vm.max_map_count=262144 >/dev/null

# Free the box. Kosha's segment cache and a 31g heap do not share 128 GiB.
sudo docker rm -f kosha pg opensearch elasticsearch >/dev/null 2>&1 || true
sudo rm -rf "$DATA"
sudo mkdir -p "$DATA"
sudo chown -R 1000:1000 "$DATA"

eval "$($PY "$HERE/../lucene_compare.py" docker-cmd "$ENGINE" "$DATA")"
echo "starting $ENGINE"
"${docker_cmd[@]}" >/dev/null

for i in $(seq 60); do
  curl -sf "http://127.0.0.1:9200/" >/dev/null && break
  sleep 3
  if [ "$i" -eq 60 ]; then
    echo "engine never answered on :9200"
    sudo docker logs --tail 200 "$ENGINE" || true
    exit 1
  fi
done
echo "ENGINE-UP $ENGINE"

$PY "$HERE/../load_corpus_opensearch.py" \
  --host http://127.0.0.1:9200 \
  --engine "$ENGINE" \
  --index msmarco-10m \
  --corpus-dir "$CORPUS" \
  --data-dir "$DATA" \
  --shards "$SHARDS" \
  --max-num-segments "$MAX_SEGMENTS" \
  --recreate \
  --out "$OUT/load.json"
echo "LOAD-COMPLETE $ENGINE"

# Page cache warm, request cache off, then the defaults phase with no
# second warmup. Warmup latencies stay out of the percentile.
$PY "$HERE/../query_bench_opensearch.py" \
  --host http://127.0.0.1:9200 \
  --engine "$ENGINE" \
  --index msmarco-10m \
  --queries-file "$CORPUS/queries.txt" \
  --cache-mode off \
  --warmup \
  --qps "$QPS" --topk 10 --duration "$DURATION" --timeout 600 \
  --order "$ORDER" --return "$RETURN_FIELDS" --phase warm \
  --raw-out "$OUT/warm_nocache.requests.jsonl.gz" \
  --out "$OUT/warm_nocache.json"
echo "PHASE-COMPLETE warm_nocache"

$PY "$HERE/../query_bench_opensearch.py" \
  --host http://127.0.0.1:9200 \
  --engine "$ENGINE" \
  --index msmarco-10m \
  --queries-file "$CORPUS/queries.txt" \
  --cache-mode defaults \
  --qps "$QPS" --topk 10 --duration "$DURATION" --timeout 600 \
  --order "$ORDER" --return "$RETURN_FIELDS" --phase warm \
  --raw-out "$OUT/warm_defaults.requests.jsonl.gz" \
  --out "$OUT/warm_defaults.json"
echo "PHASE-COMPLETE warm_defaults"

sudo docker logs --timestamps "$ENGINE" 2>&1 | gzip > "$OUT/server/full.log.gz" || true
echo "LUCENE-COMPLETE $ENGINE"
