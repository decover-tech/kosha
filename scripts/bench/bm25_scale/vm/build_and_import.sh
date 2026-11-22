#!/bin/bash
# Offline bulk ingest: build a namespace's segments in parallel with
# kosha-build-segments (no server involved), upload them, attach via
# /v1/admin/import-namespace (#133), snapshot (#132). Replaces the
# serialized server-side load — the 10M vector corpus went from ~6h of
# /index requests to roughly total_build_cpu / cores.
#
# Usage: [MAX_DOCS=N] [SNAPSHOT_NAME=name] ./build_and_import.sh <namespace> [--with-embeddings]
#   ./build_and_import.sh msmarco-10m-vec --with-embeddings
#   MAX_DOCS=1000000 ./build_and_import.sh msmarco-1m   # first 1M docs
#   SNAPSHOT_NAME=msmarco-10m-vec-sidecar ./build_and_import.sh msmarco-10m-vec --with-embeddings
#
# BUILD_ARGS (default: empty) are extra kosha-build-segments flags, e.g.
#   BUILD_ARGS="--cluster-layout namespace" for cluster-aligned segments
#   (#380, research). Restricted to [A-Za-z0-9 =._-]. Pair it with a new
#   SNAPSHOT_NAME: a snapshot built with other flags is a different corpus.
#
# Provenance (#380): next to the snapshot this writes
# snapshots/<SNAPSHOT_NAME>/build-info.json with BUILDER_SHA (the commit
# the builder was built from; the workflow passes github.sha, the VM copy
# has no .git), a digest of the builder sources (crates/, Cargo.toml,
# Cargo.lock), BUILD_ARGS and MAX_DOCS, so two snapshots can be shown to
# come from the same builder. Snapshots built before this have none.
#
# SNAPSHOT_NAME (default: the namespace) names the snapshot this writes;
# restore it with restore_namespace.sh <namespace> <snapshot-name>. An
# existing snapshot is never overwritten unless SNAPSHOT_OVERWRITE=1:
# the build would otherwise silently replace a corpus other runs restore.
#
# Vectors: kosha-build-segments writes through SegmentWriter, whose vector
# store mode follows KOSHA_VECTOR_STORE (unset = sidecar), so vectors land
# in the binary doc_vectors.bin sidecar, not inlined as JSON in
# doc_store.bin. The builder container does not set the variable.
#
# The namespace's previous objects in the bench bucket and the server
# local cache are dropped before the new segments go in: offline segment
# ids are <ns>-offline-NNNNNN, the same on every rebuild, so leftovers of
# an earlier build or restore of this namespace would be read as the new
# ones (see snapshot_cache.sh).
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
# Absolute script dir: the build below cds to ~/kosha, after which a
# relative "$(dirname "$0")" (./) points at the wrong directory.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/snapshot_cache.sh"

NS="${1:?usage: [MAX_DOCS=N] [SNAPSHOT_NAME=name] build_and_import.sh <namespace> [--with-embeddings]}"
SNAPSHOT_NAME="${SNAPSHOT_NAME:-$NS}"
WITH_EMB="${2:-}"
MAX_DOCS="${MAX_DOCS:-0}"
BUILD_ARGS="${BUILD_ARGS:-}"
BUILDER_SHA="${BUILDER_SHA:-unknown}"
case "$BUILDER_SHA" in
  *[!0-9a-z]*) echo "::error::BUILDER_SHA must be a hex commit id or unknown, got: $BUILDER_SHA"; exit 1 ;;
esac
case "$BUILD_ARGS" in
  *[!A-Za-z0-9\ =._-]*) echo "::error::BUILD_ARGS may only contain [A-Za-z0-9 =._-], got: $BUILD_ARGS"; exit 1 ;;
esac
TENANT="${KOSHA_TENANT:-default}"
SCOPED="$TENANT/$NS"
TXT_CACHE=s3://decoverai-bench-corpus-cache/msmarco-10m
EMB_CACHE=s3://decoverai-bench-corpus-cache/msmarco-10m-emb
SEG_BUCKET="${KOSHA_S3_BUCKET:-decoverai-bench-kosha-segments}"
ADMIN_KEY="${KOSHA_ADMIN_API_KEY:-sk-bench-admin}"

if [ "${SNAPSHOT_OVERWRITE:-0}" != 1 ] && \
   aws s3 ls "s3://$SNAPSHOT_CACHE_BUCKET/snapshots/$SNAPSHOT_NAME/pg-kosha.sql.gz" >/dev/null 2>&1; then
  echo "::error::snapshot $SNAPSHOT_NAME already exists; refusing to overwrite it (SNAPSHOT_OVERWRITE=1 to force)"
  exit 1
fi

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
# Digest of what the builder compiles: every file under crates/ plus the
# workspace manifest and lockfile, by path and content.
SOURCE_DIGEST="$( { find crates -type f -print0 | sort -z | xargs -0 sha256sum; sha256sum Cargo.toml Cargo.lock; } | sha256sum | cut -d' ' -f1)"
echo "BUILDER builder_sha=$BUILDER_SHA source_digest=$SOURCE_DIGEST build_args=[$BUILD_ARGS] max_docs=$MAX_DOCS"
sudo docker build -q --target builder -t kosha-builder:bench . >/dev/null
sudo rm -rf "/data/segments-out/$NS"
sudo docker run --rm -v /data:/data kosha-builder:bench sh -c "
  cargo build --release --locked -p kosha-build-segments &&
  ./target/release/kosha-build-segments \
    --shards-dir /data/corpus-txt $EMB_ARGS \
    --namespace '$NS' --out-dir /data/segments-out --max-docs $MAX_DOCS $BUILD_ARGS"
echo "BUILD-COMPLETE $NS (max_docs=$MAX_DOCS build_args=$BUILD_ARGS)"

echo "uploading segments..."
marker_clear bucket "$NS"
aws s3 rm "s3://$SEG_BUCKET/segments/$SCOPED/" --recursive --only-show-errors
aws s3 sync "/data/segments-out/$NS" "s3://$SEG_BUCKET/segments/$SCOPED" --no-progress
echo "UPLOAD-COMPLETE"
# The bucket (and, below, the snapshot) holds the build now; the local
# copy is only disk pressure for the restore that follows.
sudo rm -rf "/data/segments-out/$NS"

# Drop local files of any earlier version of this namespace before the
# server sees the new manifest. Other namespaces keep their caches.
sudo docker stop kosha >/dev/null
wipe_namespace_cache "$NS" "$TENANT"
sudo docker start kosha >/dev/null
for i in $(seq 60); do curl -sf http://127.0.0.1:8080/healthz >/dev/null && break; sleep 3
  [ "$i" -eq 60 ] && { echo "::error::kosha /healthz never responded after restart"; exit 1; }; done

curl -sf -X POST http://127.0.0.1:8080/v1/admin/import-namespace \
  -H "Authorization: Bearer $ADMIN_KEY" -H "Content-Type: application/json" \
  -d "{\"namespace\": \"$SCOPED\", \"overwrite\": true}" | tee /tmp/import_result.json
echo
grep -q '"segments"' /tmp/import_result.json || { echo IMPORT-FAILED; exit 1; }
echo "IMPORT-COMPLETE"

"$SCRIPT_DIR/snapshot_namespace.sh" "$SCOPED" "$SNAPSHOT_NAME"
# Bucket and local cache now derive from exactly this snapshot: a restore
# of it right after keeps both instead of re-copying them.
printf '{"snapshot": "%s", "namespace": "%s", "builder_sha": "%s", "source_digest": "%s", "build_args": "%s", "max_docs": %s, "with_embeddings": "%s", "built_at": "%s"}\n' \
  "$SNAPSHOT_NAME" "$NS" "$BUILDER_SHA" "$SOURCE_DIGEST" "$BUILD_ARGS" "$MAX_DOCS" "$WITH_EMB" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  > /tmp/build-info.json
aws s3 cp --only-show-errors /tmp/build-info.json "s3://$SNAPSHOT_CACHE_BUCKET/snapshots/$SNAPSHOT_NAME/build-info.json"
echo "BUILD-INFO $(cat /tmp/build-info.json)"
rm -f /tmp/build-info.json
IDENTITY="$(snapshot_identity "$SNAPSHOT_NAME")"
marker_write bucket "$NS" "$IDENTITY"
marker_write local "$NS" "$IDENTITY"
echo "BUILD-AND-IMPORT-COMPLETE $NS -> snapshot $SNAPSHOT_NAME"
