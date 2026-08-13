#!/bin/bash
# Mount the persistent embeddings-cache EBS volume (terraform stack
# dev-machines/aws/bench-embeddings-cache in the infra repo — its own
# state, never destroyed by a bench round) and populate it from the
# permanent S3 corpus-embeddings cache on first-ever use. Idempotent:
# every run after the first just mounts an already-populated disk
# (seconds), instead of re-syncing ~41GB from S3 (~4 min at this instance
# class's measured S3 throughput — see RESULTS.md's ~165MB/s figure) for
# a corpus that hasn't changed since the last round.
#
# Prints the mount path to stdout on success; everything else goes to
# stderr, so callers can safely do
#   EMB_DIR=$(./mount_embeddings_cache.sh vol-0123456789abcdef0)
#
# Usage: ./mount_embeddings_cache.sh <volume-id>
set -euo pipefail

VOLUME_ID="${1:?usage: mount_embeddings_cache.sh <volume-id>}"
EMB_CACHE=s3://decoverai-bench-corpus-cache/msmarco-10m-emb
MOUNT=/data/embeddings-cache

log() { echo "$@" >&2; }

# Nitro instances (this whole VM family is Nitro-based) only guarantee an
# attached EBS volume's *actual* kernel device path via this by-id symlink
# — the device name requested in terraform (e.g. /dev/sdf) is a hint the
# kernel isn't required to honor as the real path.
LINK="/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_${VOLUME_ID//-/}"
for i in $(seq 30); do
  [ -e "$LINK" ] && break
  sleep 2
  if [ "$i" -eq 30 ]; then
    log "::error::embeddings cache volume $VOLUME_ID never attached (no $LINK after 60s)"
    exit 1
  fi
done
DEV="$(readlink -f "$LINK")"
log "embeddings cache volume $VOLUME_ID -> $DEV"

sudo mkdir -p "$MOUNT"

# A blank volume has no recognizable filesystem — `file -s` reports plain
# "data" for it, and "Linux rev 1.0 ext4 filesystem data" once formatted.
# Only format on first-ever use: this volume's whole point is that its
# filesystem (and the embeddings on it) survive across bench rounds.
if ! sudo file -s "$DEV" | grep -q ext4; then
  log "embeddings cache volume is blank — formatting (first-ever use only)"
  sudo mkfs -t ext4 -q "$DEV"
fi

sudo mount "$DEV" "$MOUNT"
sudo chown -R "$(id -u):$(id -g)" "$MOUNT"

if [ -f "$MOUNT/emb_manifest.json" ]; then
  log "embeddings cache already populated: $(cat "$MOUNT/emb_manifest.json")"
else
  log "embeddings cache empty — syncing from $EMB_CACHE (first-ever use only, ~4 min)"
  aws s3 sync "$EMB_CACHE" "$MOUNT" --no-progress \
    --exclude "*" --include "emb-*.f32" --include "emb-*.ids" --include "emb_manifest.json"
  log "sync complete: $(cat "$MOUNT/emb_manifest.json")"
fi

echo "$MOUNT"
