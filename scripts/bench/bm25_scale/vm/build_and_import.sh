#!/bin/bash
# Offline bulk ingest: build a namespace's segments in parallel with
# kosha-build-segments (no server involved), upload them, attach via
# /v1/admin/import-namespace (#133), snapshot (#132). Replaces the
# serialized server-side load — the 10M vector corpus went from ~6h of
# /index requests to roughly total_build_cpu / cores.
#
# Usage: [MAX_DOCS=N] [SNAPSHOT_NAME=name] [BUILD_DOCKER_ENV='-e K=V ...'] ./build_and_import.sh <namespace> [--with-embeddings]
#   ./build_and_import.sh msmarco-10m-vec --with-embeddings
#   MAX_DOCS=1000000 ./build_and_import.sh msmarco-1m   # first 1M docs
#   SNAPSHOT_NAME=msmarco-10m-vec-sidecar ./build_and_import.sh msmarco-10m-vec --with-embeddings
#
# SNAPSHOT_NAME (default: the namespace) names the snapshot this writes;
# restore it with restore_namespace.sh <namespace> <snapshot-name>. An
# existing snapshot is never overwritten unless SNAPSHOT_OVERWRITE=1:
# the build would otherwise silently replace a corpus other runs restore.
#
# BUILD_DOCKER_ENV (default: empty) adds environment to the segment
# builder container, as docker run flags: '-e KEY=VALUE' repeated (the
# benchmark workflow passes its server_env input here so an experimental
# segment format flag reaches the builder too). Each KEY=VALUE must match
# ^[A-Z0-9_]+=[A-Za-z0-9._:/,-]*$; anything else is refused. Empty = the
# builder runs exactly as before.
#
# Vectors: kosha-build-segments writes through SegmentWriter, whose vector
# store mode follows KOSHA_VECTOR_STORE (unset = sidecar), so vectors land
# in the binary doc_vectors.bin sidecar, not inlined as JSON in
# doc_store.bin. The builder container does not set the variable.
#
# Doc-store compression (research #388, experimental): the builder gets
# KOSHA_DOC_STORE_COMPRESSION and the KOSHA_DOC_STORE_ZSTD_* tuning vars
# when they are set here. A SNAPSHOT_NAME containing "-zstd" (for example
# msmarco-10m-vec-zstd) defaults KOSHA_DOC_STORE_COMPRESSION to zstd, so
# the workflow's build_vector_snapshot path needs no new input. Without
# either, segments are built exactly as before (raw doc store). The server
# that restores such a snapshot must be a build that reads the format.
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
case "$SNAPSHOT_NAME" in
  *-zstd*) export KOSHA_DOC_STORE_COMPRESSION="${KOSHA_DOC_STORE_COMPRESSION:-zstd}" ;;
esac
BUILD_ENV=""
for v in KOSHA_DOC_STORE_COMPRESSION KOSHA_DOC_STORE_ZSTD_LEVEL KOSHA_DOC_STORE_ZSTD_DICT_BYTES \
         KOSHA_DOC_STORE_ZSTD_SAMPLE_BYTES; do
  if [ -n "${!v:-}" ]; then BUILD_ENV="$BUILD_ENV -e $v=${!v}"; fi
done
if [ -n "$BUILD_ENV" ]; then echo "builder env:$BUILD_ENV"; fi
MAX_DOCS="${MAX_DOCS:-0}"
BUILD_DOCKER_ENV="${BUILD_DOCKER_ENV:-}"
PAIR_RE='^[A-Z0-9_]+=[A-Za-z0-9._:/,-]*$'
OLD_LC_ALL="${LC_ALL-}"
LC_ALL=C   # ASCII ranges in PAIR_RE whatever the locale
set -f
EXPECT_PAIR=0
for w in $BUILD_DOCKER_ENV; do
  if [ "$EXPECT_PAIR" = 0 ]; then
    [ "$w" = "-e" ] || { echo "::error::BUILD_DOCKER_ENV must be '-e KEY=VALUE' repeated, got: $w"; exit 1; }
    EXPECT_PAIR=1
  else
    [[ "$w" =~ $PAIR_RE ]] || { echo "::error::BUILD_DOCKER_ENV pair rejected: $w"; exit 1; }
    EXPECT_PAIR=0
  fi
done
set +f
if [ -n "$OLD_LC_ALL" ]; then LC_ALL="$OLD_LC_ALL"; else unset LC_ALL; fi
[ "$EXPECT_PAIR" = 0 ] || { echo "::error::BUILD_DOCKER_ENV ends with a bare -e"; exit 1; }
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
sudo docker build -q --target builder -t kosha-builder:bench . >/dev/null
sudo rm -rf "/data/segments-out/$NS"
if [ -n "$BUILD_DOCKER_ENV" ]; then echo "builder env: $BUILD_DOCKER_ENV"; fi
# shellcheck disable=SC2086  # BUILD_ENV (#388 doc-store vars) and the validated
# BUILD_DOCKER_ENV (harness server_env) are lists of -e flags, split on purpose
sudo docker run --rm $BUILD_ENV $BUILD_DOCKER_ENV -v /data:/data kosha-builder:bench sh -c "
  cargo build --release --locked -p kosha-build-segments &&
  ./target/release/kosha-build-segments \
    --shards-dir /data/corpus-txt $EMB_ARGS \
    --namespace '$NS' --out-dir /data/segments-out --max-docs $MAX_DOCS"
echo "BUILD-COMPLETE $NS (max_docs=$MAX_DOCS)"

# Research #388: the footers say which doc store format was written. A
# builder that ignored KOSHA_DOC_STORE_COMPRESSION=zstd (a ref without
# #388) must not get its raw segments snapshotted under a -zstd name.
FOOTERS=$(sudo find "/data/segments-out/$NS" -name footer.json | wc -l | tr -d ' ')
ZSTD_FOOTERS=$( (sudo find "/data/segments-out/$NS" -name footer.json \
  -exec grep -l '"doc_store_compression"' {} + 2>/dev/null || true) | wc -l | tr -d ' ')
echo "doc store format: $ZSTD_FOOTERS of $FOOTERS segment footers name a compressed doc store"
if [ "${KOSHA_DOC_STORE_COMPRESSION:-}" = zstd ] && [ "$ZSTD_FOOTERS" = 0 ]; then
  echo "::error::KOSHA_DOC_STORE_COMPRESSION=zstd but no segment is compressed; this ref predates #388. Not uploading."
  exit 1
fi
if [ "${KOSHA_DOC_STORE_COMPRESSION:-}" != zstd ] && [ "$ZSTD_FOOTERS" != 0 ]; then
  echo "::error::$ZSTD_FOOTERS segment(s) are compressed but KOSHA_DOC_STORE_COMPRESSION is not zstd. Not uploading."
  exit 1
fi
# Provenance stored next to the snapshot (build_info.txt): the builder env
# and a fingerprint of the sources the builder compiled. The VM copy has
# no .git, so two snapshots come from the same commit when their
# fingerprints match.
SRC_FINGERPRINT=$(find crates proto Cargo.toml Cargo.lock Dockerfile -type f 2>/dev/null \
  | LC_ALL=C sort | xargs sha256sum | sha256sum | cut -c1-16)
BUILD_INFO="snapshot=$SNAPSHOT_NAME namespace=$NS max_docs=$MAX_DOCS source_sha256=$SRC_FINGERPRINT builder_env=[${BUILD_ENV# }] compressed_footers=$ZSTD_FOOTERS/$FOOTERS built_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "build info: $BUILD_INFO"

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
echo "$BUILD_INFO" | aws s3 cp - "s3://$SNAPSHOT_CACHE_BUCKET/snapshots/$SNAPSHOT_NAME/build_info.txt" --only-show-errors
# Bucket and local cache now derive from exactly this snapshot: a restore
# of it right after keeps both instead of re-copying them.
IDENTITY="$(snapshot_identity "$SNAPSHOT_NAME")"
marker_write bucket "$NS" "$IDENTITY"
marker_write local "$NS" "$IDENTITY"
echo "BUILD-AND-IMPORT-COMPLETE $NS -> snapshot $SNAPSHOT_NAME"
