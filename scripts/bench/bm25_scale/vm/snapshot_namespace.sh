#!/bin/bash
# Snapshot a loaded bench namespace so future rounds restore in minutes
# instead of re-running a multi-hour load.
#
# A loaded namespace is exactly two things: its segment objects in the
# (per-round, terraform-destroyed) bench bucket, and the control-plane
# rows in postgres pointing at them. This copies the segments into the
# PERSISTENT corpus-cache bucket and stores a full pg dump beside them.
#
# Usage: ./snapshot_namespace.sh <namespace> [snapshot-name]
#   ./snapshot_namespace.sh msmarco-10m-vec
set -euo pipefail

NS="${1:?usage: snapshot_namespace.sh <namespace> [snapshot-name]}"
NAME="${2:-$NS}"
SEG_BUCKET="${KOSHA_S3_BUCKET:-decoverai-bench-kosha-segments}"
SNAP="s3://decoverai-bench-corpus-cache/snapshots/$NAME"

echo "snapshotting namespace '$NS' -> $SNAP"
aws s3 sync "s3://$SEG_BUCKET/segments/$NS" "$SNAP/segments/$NS" --no-progress --copy-props none

# Full control-plane dump. NOTE: this includes every namespace's rows —
# restore on a fresh stack whose bench bucket only holds this snapshot's
# segments, or expect other namespaces' manifests to dangle (queries to
# them 503 on hydration; the snapshotted namespace is unaffected).
sudo docker exec pg pg_dump -U postgres kosha | gzip > /tmp/pg-kosha.sql.gz
aws s3 cp --quiet /tmp/pg-kosha.sql.gz "$SNAP/pg-kosha.sql.gz"
rm -f /tmp/pg-kosha.sql.gz

COUNT=$(aws s3 ls "$SNAP/segments/$NS/" --recursive | wc -l | tr -d ' ')
echo "SNAPSHOT-COMPLETE $NAME: $COUNT objects + control-plane dump"
