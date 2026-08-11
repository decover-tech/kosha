#!/bin/bash
# Offline bulk ingest: build a namespace's segments in parallel with
# kosha-build-segments (no server involved), upload them, attach via
# /v1/admin/import-namespace (#133), snapshot (#132). Replaces the
# serialized server-side load — the 10M vector corpus went from ~6h of
# /index requests to roughly total_build_cpu / cores.
#
# Usage: ./build_and_import.sh <namespace> [--with-embeddings]
#   ./build_and_import.sh msmarco-10m-vec --with-embeddings
set -euo pipefail

NS="${1:?usage: build_and_import.sh <namespace> [--with-embeddings]}"
WITH_EMB="${2:-}"
TXT_CACHE=s3://decoverai-bench-corpus-cache/msmarco-10m
EMB_CACHE=s3://decoverai-bench-corpus-cache/msmarco-10m-emb
SEG_BUCKET="${KOSHA_S3_BUCKET:-decoverai-bench-kosha-segments}"
API_KEY="${KOSHA_API_KEY:-sk-bench}"

sudo mkdir -p /data/corpus-txt /data/corpus-emb /data/segments-out
sudo chown -R "$(id -u):$(id -g)" /data/corpus-txt /data/corpus-emb /data/segments-out

echo "staging corpus caches..."
aws s3 sync "$TXT_CACHE" /data/corpus-txt --no-progress --exclude "*" --include "shard-*.ndjson"
EMB_ARGS=()
if [ "$WITH_EMB" = "--with-embeddings" ]; then
  aws s3 sync "$EMB_CACHE" /data/corpus-emb --no-progress \
    --exclude "*" --include "emb-*.f32" --include "emb-*.ids"
  EMB_ARGS=(--embeddings-dir /data/corpus-emb)
fi

cd ~/kosha
cargo build --release -p kosha-build-segments
rm -rf "/data/segments-out/$NS"
./target/release/kosha-build-segments \
  --shards-dir /data/corpus-txt "${EMB_ARGS[@]}" \
  --namespace "$NS" --out-dir /data/segments-out
echo "BUILD-COMPLETE"

echo "uploading segments..."
aws s3 sync "/data/segments-out/$NS" "s3://$SEG_BUCKET/segments/$NS" --no-progress
echo "UPLOAD-COMPLETE"

curl -sf -X POST http://127.0.0.1:8080/v1/admin/import-namespace \
  -H "Authorization: Bearer $API_KEY" -H "Content-Type: application/json" \
  -d "{\"namespace\": \"$NS\"}" | tee /tmp/import_result.json
echo
grep -q '"segments"' /tmp/import_result.json || { echo IMPORT-FAILED; exit 1; }
echo "IMPORT-COMPLETE"

"$(dirname "$0")/snapshot_namespace.sh" "$NS"
echo "BUILD-AND-IMPORT-COMPLETE"
