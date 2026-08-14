#!/bin/bash
# Restore a snapshot_namespace.sh snapshot onto the current bench stack:
# sync the segments back into this round's bench bucket, load the
# control-plane dump, and restart the server so its boot-restore picks
# the namespace up. Query-ready after warmup — minutes, not hours.
#
# Assumes the pg container exists (running or stopped) and the kosha
# container has been created by the round's pipeline (it is restarted
# here; set KOSHA_WARMUP_NAMESPACES on it to prewarm the restored ns).
#
# Usage: ./restore_namespace.sh <namespace> [snapshot-name]
set -euo pipefail

NS="${1:?usage: restore_namespace.sh <namespace> [snapshot-name]}"
NAME="${2:-$NS}"
SEG_BUCKET="${KOSHA_S3_BUCKET:-decoverai-bench-kosha-segments}"
SNAP="s3://decoverai-bench-corpus-cache/snapshots/$NAME"

aws s3 ls "$SNAP/pg-kosha.sql.gz" >/dev/null || { echo "no snapshot at $SNAP"; exit 1; }

echo "restoring '$NS' from $SNAP"
aws s3 sync "$SNAP/segments/$NS" "s3://$SEG_BUCKET/segments/$NS" --no-progress --copy-props none

sudo docker start pg >/dev/null 2>&1 || true
until sudo docker exec pg pg_isready -U postgres >/dev/null 2>&1; do sleep 2; done
aws s3 cp --quiet "$SNAP/pg-kosha.sql.gz" /tmp/pg-kosha.sql.gz
sudo docker exec pg psql -U postgres -q -c "DROP DATABASE IF EXISTS kosha WITH (FORCE);" -c "CREATE DATABASE kosha;"
gunzip -c /tmp/pg-kosha.sql.gz | sudo docker exec -i pg psql -U postgres -q kosha >/dev/null
rm -f /tmp/pg-kosha.sql.gz

# Fresh local store + server restart: boot-restore reads the manifests,
# warmup (if configured for $NS) hydrates before /readyz goes green.
sudo docker stop kosha >/dev/null 2>&1 || true
sudo rm -rf /var/cache/kosha-placeholder 2>/dev/null || true
sudo rm -rf /data/kosha/data/* 2>/dev/null || true
sudo docker start kosha >/dev/null
until curl -sf -H "Authorization: Bearer ${KOSHA_API_KEY:-sk-bench}" http://127.0.0.1:8080/stats >/dev/null; do sleep 3; done

# Segment count matters for anything that reasons about a per-query probe
# budget spread across segments (e.g. knn.num_candidates is a whole-query
# budget, not per-segment) — surface it here rather than leave it buried,
# it's cheap and this is the one point every run already waits on /stats.
STATS_SEGMENTS="$(curl -sf -H "Authorization: Bearer ${KOSHA_API_KEY:-sk-bench}" http://127.0.0.1:8080/stats \
  | python3 -c "import json,sys; d=json.load(sys.stdin); ns=[n for n in d.get('namespaces',[]) if n.get('namespace')=='$NS']; print(f\"{ns[0]['segments']} segments, {ns[0]['documents']} docs\" if ns else 'namespace not found in /stats')" \
  2>/dev/null || echo "stats parse failed")"
echo "RESTORE-STATS $NS: $STATS_SEGMENTS"

echo "RESTORE-COMPLETE $NAME — server up; wait on /readyz for warmup before benching"
