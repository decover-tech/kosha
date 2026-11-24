#!/bin/bash
# Doc-store byte accounting of one snapshot (research #388): sums the
# snapshot's doc_store.bin / doc_store.offsets / doc_vectors.bin object
# sizes, reads every segment's footer.json, and prints one JSON object
# (see ../doc_store_bytes.py). Read-only: it lists and downloads from the
# snapshot prefix, never writes to S3.
#
# Usage: ./doc_store_bytes.sh <snapshot-name> [zstd|raw]
#   ./doc_store_bytes.sh msmarco-10m-vec-zstd zstd
#   ./doc_store_bytes.sh msmarco-10m-vec-raw388 raw
#
# With an expected format, exits 3 when the footers disagree (for example
# a *-zstd snapshot built from a ref without #388, which is raw). Run it
# before trusting any number from a snapshot.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/snapshot_cache.sh"

NAME="${1:?usage: doc_store_bytes.sh <snapshot-name> [zstd|raw]}"
EXPECT="${2:-}"
case "$EXPECT" in
  ''|zstd|raw) ;;
  *) echo "expected format must be zstd or raw, got: $EXPECT" >&2; exit 2 ;;
esac
SNAP="s3://$SNAPSHOT_CACHE_BUCKET/snapshots/$NAME"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

aws s3 ls "$SNAP/segments/" --recursive > "$WORK/listing.txt"
aws s3 cp "$SNAP/segments/" "$WORK/footers/" --recursive --only-show-errors \
  --exclude "*" --include "*/footer.json"
aws s3 cp "$SNAP/build_info.txt" "$WORK/build_info.txt" --only-show-errors 2>/dev/null || true

ARGS=(--listing "$WORK/listing.txt" --footers "$WORK/footers" --label "$NAME" \
      --build-info "$WORK/build_info.txt")
if [ -n "$EXPECT" ]; then ARGS+=(--expect "$EXPECT"); fi
python3 -I "$SCRIPT_DIR/../doc_store_bytes.py" "${ARGS[@]}"
