#!/bin/bash
# Offline bulk ingest: build a namespace's segments in parallel with
# kosha-build-segments (no server involved), upload them, attach via
# /v1/admin/import-namespace (#133), snapshot (#132). Replaces the
# serialized server-side load — the 10M vector corpus went from ~6h of
# /index requests to roughly total_build_cpu / cores.
#
# Usage: [MAX_DOCS=N] ./build_and_import.sh <namespace> [--with-embeddings]
#   ./build_and_import.sh msmarco-10m-vec --with-embeddings
#   MAX_DOCS=1000000 ./build_and_import.sh msmarco-1m   # first 1M docs
#
# Tenant scoping (#262): clients address <namespace>, the server stores it
# as <tenant>/<namespace> (sk-bench is tenant "default") and S3 keys embed
# that id, so segments are uploaded and imported under the scoped id. They
# are built under the bare name: the builder derives segment ids from it
# (<ns>-offline-NNNNNN), and a "/" there would nest directories. Import is
# an admin route: it needs KOSHA_ADMIN_API_KEY, not the tenant key. The snapshot keeps the bare name so restore_namespace.sh
# finds it by <namespace>.
#
# The builder runs inside the server image's `builder` stage (pinned Rust
# toolchain + protoc; shares its cached layers), so the VM needs no Rust
# install and the binary never runs against the host glibc.
set -euo pipefail

NS="${1:?usage: [MAX_DOCS=N] build_and_import.sh <namespace> [--with-embeddings]}"
WITH_EMB="${2:-}"
MAX_DOCS="${MAX_DOCS:-0}"
TENANT="${KOSHA_TENANT:-default}"
SCOPED="$TENANT/$NS"
TXT_CACHE=s3://decoverai-bench-corpus-cache/msmarco-10m
EMB_CACHE=s3://decoverai-bench-corpus-cache/msmarco-10m-emb
SEG_BUCKET="${KOSHA_S3_BUCKET:-decoverai-bench-kosha-segments}"
ADMIN_KEY="${KOSHA_ADMIN_API_KEY:-sk-bench-admin}"

sudo mkdir -p /data/corpus-txt /data/corpus-emb /data/segments-out
sudo chown -R "$(id -u):$(id -g)" /data/corpus-txt /data/corpus-emb /data/segments-out

echo "staging corpus caches..."
aws s3 sync "$TXT_CACHE" /data/corpus-txt --no-progress --exclude "*" --include "shard-*.ndjson"
EMB_ARGS=""
if [ "$WITH_EMB" = "--with-embeddings" ]; then
  aws s3 sync "$EMB_CACHE" /data/corpus-emb --no-progress \
    --exclude "*" --include "emb-*.f32" --include "emb-*.ids"
  EMB_ARGS="--embeddings-dir /data/corpus-emb"
fi

cd ~/kosha
sudo docker build -q --target builder -t kosha-builder:bench . >/dev/null
sudo rm -rf "/data/segments-out/$NS"
sudo docker run --rm -v /data:/data kosha-builder:bench sh -c "
  cargo build --release --locked -p kosha-build-segments &&
  ./target/release/kosha-build-segments \
    --shards-dir /data/corpus-txt $EMB_ARGS \
    --namespace '$NS' --out-dir /data/segments-out --max-docs $MAX_DOCS"
echo "BUILD-COMPLETE $NS (max_docs=$MAX_DOCS)"

echo "uploading segments..."
aws s3 sync "/data/segments-out/$NS" "s3://$SEG_BUCKET/segments/$SCOPED" --no-progress
echo "UPLOAD-COMPLETE"

curl -sf -X POST http://127.0.0.1:8080/v1/admin/import-namespace \
  -H "Authorization: Bearer $ADMIN_KEY" -H "Content-Type: application/json" \
  -d "{\"namespace\": \"$SCOPED\", \"overwrite\": true}" | tee /tmp/import_result.json
echo
grep -q '"segments"' /tmp/import_result.json || { echo IMPORT-FAILED; exit 1; }
echo "IMPORT-COMPLETE"

"$(dirname "$0")/snapshot_namespace.sh" "$SCOPED" "$NS"
echo "BUILD-AND-IMPORT-COMPLETE"
