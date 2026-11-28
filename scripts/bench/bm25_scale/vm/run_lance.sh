#!/bin/bash
# Lance over an Iceberg REST catalog on the bench VM, data on S3, same
# MS MARCO corpus and the same open-loop client as run_lucene.sh.
#
# The Kosha and Lucene containers are stopped first so the box is Lance's.
# The S3 prefix is emptied so the load time is a full load, not an append.
# Settings and pinned versions live in ../lance_compare.py.
#
# Env (set by benchmark.yml): LANCE_ROOT=s3://bucket/prefix for the table,
# CORPUS_S3=s3://bucket/prefix of the MS MARCO shards.
#
# Usage: run_lance.sh <out-dir> [qps] [duration] [ids|source]
set -euo pipefail

OUT="${1:?usage: run_lance.sh <out-dir> [qps] [duration] [ids|source]}"
QPS="${2:-8}"
DURATION="${3:-300}"
RETURN_FIELDS="${4:-ids}"
HERE="$(cd "$(dirname "$0")" && pwd)"
BENCH="$HERE/.."
CORPUS="${CORPUS:-/data/msmarco-10m}"
CORPUS_S3="${CORPUS_S3:?CORPUS_S3 (s3://bucket/prefix of the corpus) must be set}"
ORDER="${QUERY_ORDER:-cycle}"
VENV="${LANCE_VENV:-$HOME/venv-lance}"

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

ROOT="${LANCE_ROOT:?LANCE_ROOT (s3://bucket/prefix for the Lance table) must be set}"
ROOT="${ROOT%/}"
case "$ROOT" in
  s3://*/*) ;;
  *) echo "LANCE_ROOT must be an s3://bucket/prefix, got $ROOT"; exit 1 ;;
esac

TOKEN=$(curl -sf -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
REGION="${AWS_REGION:-$(curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/region)}"
[ -n "$REGION" ] || { echo "could not determine the AWS region"; exit 1; }
echo "lance: root=$ROOT region=$REGION qps=$QPS duration=${DURATION}s return=$RETURN_FIELDS order=$ORDER"

if [ ! -x "$VENV/bin/python3" ]; then
  python3.11 -m venv "$VENV"
fi
# shellcheck disable=SC2046
"$VENV/bin/pip" install -q $(python3 "$BENCH/lance_compare.py" pip-requirements)
PY="$VENV/bin/python3"
"$PY" -c 'import lance, lance_namespace_impls; print("pylance", lance.__version__)'

echo "syncing corpus $CORPUS_S3 -> $CORPUS"
sudo mkdir -p "$CORPUS" "$OUT/server"
sudo chown -R "$(id -u):$(id -g)" "$CORPUS" "$OUT"
aws s3 sync --only-show-errors "$CORPUS_S3" "$CORPUS" --exclude "*" --include "shard-*.ndjson" --include "queries.txt" --include "manifest.json"
[ -s "$CORPUS/queries.txt" ] || { echo "queries.txt missing after sync"; exit 1; }

# Free the box, then start the catalog.
CATALOG=$(python3 -c 'import sys; sys.path.insert(0, sys.argv[1]); import lance_compare; print(lance_compare.CATALOG_CONTAINER)' "$BENCH")
sudo docker rm -f kosha pg opensearch elasticsearch "$CATALOG" >/dev/null 2>&1 || true
echo "emptying $ROOT/"
aws s3 rm --only-show-errors --recursive "$ROOT/" || true

eval "$(python3 "$BENCH/lance_compare.py" catalog-cmd "$REGION" "$ROOT")"
echo "starting Iceberg REST catalog"
# shellcheck disable=SC2154  # set by the eval above
"${catalog_cmd[@]}" >/dev/null
for i in $(seq 60); do
  curl -sf "http://127.0.0.1:8181/v1/config" >/dev/null && break
  sleep 3
  if [ "$i" -eq 60 ]; then
    echo "Iceberg REST catalog never answered on :8181"
    sudo docker logs --tail 200 "$CATALOG" || true
    exit 1
  fi
done
echo "CATALOG-UP"

cd "$BENCH"
"$PY" load_corpus_lance.py \
  --corpus-dir "$CORPUS" \
  --root "$ROOT" \
  --region "$REGION" \
  --out "$OUT/load.json"
echo "LOAD-COMPLETE lance"

# Each phase is its own process, so the cold phase really starts with
# empty Lance caches and the warm phase does its own prewarm + warmup.
for PHASE in cold_first_pass warm_nocache; do
  "$PY" query_bench_lance.py \
    --root "$ROOT" \
    --region "$REGION" \
    --queries-file "$CORPUS/queries.txt" \
    --phase "$PHASE" \
    --qps "$QPS" --topk 10 --duration "$DURATION" --timeout 600 \
    --order "$ORDER" --return "$RETURN_FIELDS" \
    --raw-out "$OUT/$PHASE.requests.jsonl.gz" \
    --out "$OUT/$PHASE.json"
  echo "PHASE-COMPLETE $PHASE"
done

sudo docker logs --timestamps "$CATALOG" 2>&1 | gzip > "$OUT/server/catalog.log.gz" || true
echo "LANCE-COMPLETE"
