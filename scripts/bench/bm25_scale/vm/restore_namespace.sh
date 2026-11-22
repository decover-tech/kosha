#!/bin/bash
# Restore a snapshot_namespace.sh snapshot onto the current bench stack:
# sync the segments back into this round's bench bucket, load the
# control-plane dump, and restart the server so its boot-restore picks
# the namespace up. Query-ready after warmup — minutes, not hours.
#
# Assumes the pg container exists (running or stopped) and the kosha
# container has been created by the round's pipeline (it is restarted
# here; set KOSHA_WARMUP_NAMESPACES on it to prewarm the restored ns —
# using the tenant-scoped id, e.g. default/msmarco-10m).
#
# Tenant scoping (#262): the server resolves every client namespace as
# `<tenant>/<name>`, and the S3 segment key embeds that id
# (`segments/<tenant>/<name>/<segment>/...`). Snapshots taken before #262
# hold the bare name in both places. This script lands the namespace
# under `$KOSHA_TENANT/<name>` (tenant of the bench API key, `default`
# for KOSHA_API_KEY) whichever form the snapshot has, so clients keep
# addressing it by its bare name.
#
# Reused VM (see snapshot_cache.sh): the local cache and the bench bucket
# objects are kept when they were restored from this exact snapshot
# identity (name + ETag/LastModified of its pg dump), so a rerun does not
# re-download up to ~249GB. Any mismatch (other snapshot, same name
# re-created, no marker) wipes them first. KEEP_LOCAL_CACHE=0 forces the
# local wipe even on a match.
#
# Usage: ./restore_namespace.sh <namespace> [snapshot-name]
set -euo pipefail
. "$(dirname "$0")/snapshot_cache.sh"

NS="${1:?usage: restore_namespace.sh <namespace> [snapshot-name]}"
NAME="${2:-$NS}"
TENANT="${KOSHA_TENANT:-default}"
SCOPED="$TENANT/$NS"
SEG_BUCKET="${KOSHA_S3_BUCKET:-decoverai-bench-kosha-segments}"
SNAP="s3://decoverai-bench-corpus-cache/snapshots/$NAME"
API_KEY="${KOSHA_API_KEY:-sk-bench}"

aws s3 ls "$SNAP/pg-kosha.sql.gz" >/dev/null || { echo "no snapshot at $SNAP"; exit 1; }
IDENTITY="$(snapshot_identity "$NAME")" || { echo "::error::cannot read the identity of $SNAP"; exit 1; }
KEEP_LOCAL=0
if [ "${KEEP_LOCAL_CACHE:-1}" = 1 ] && marker_matches local "$NS" "$IDENTITY"; then KEEP_LOCAL=1; fi
KEEP_BUCKET=0
marker_matches bucket "$NS" "$IDENTITY" && KEEP_BUCKET=1
echo "SNAPSHOT-IDENTITY $IDENTITY (keep local cache: $KEEP_LOCAL, keep bucket objects: $KEEP_BUCKET)"
# Invalidate before touching anything; rewritten only on success below.
marker_clear local "$NS"
marker_clear bucket "$NS"

# Pre-#262 snapshots store segments under the bare name; newer ones under
# the scoped id. Either way they land at the scoped id in the bench bucket.
if aws s3 ls "$SNAP/segments/$SCOPED/" >/dev/null 2>&1; then
  SRC="$SNAP/segments/$SCOPED"
else
  SRC="$SNAP/segments/$NS"
fi
echo "restoring '$NS' as '$SCOPED' from $SRC"
if [ "$KEEP_BUCKET" = 0 ]; then
  # A different snapshot of this namespace may be in the bucket (reused
  # stack). sync neither deletes extra objects nor reliably replaces
  # same-size ones, and segment ids repeat across rebuilds: start empty.
  aws s3 rm "s3://$SEG_BUCKET/segments/$SCOPED/" --recursive --only-show-errors
fi
aws s3 sync "$SRC" "s3://$SEG_BUCKET/segments/$SCOPED" --no-progress --copy-props none
marker_write bucket "$NS" "$IDENTITY"

sudo docker start pg >/dev/null 2>&1 || true
until sudo docker exec pg pg_isready -U postgres >/dev/null 2>&1; do sleep 2; done
aws s3 cp --quiet "$SNAP/pg-kosha.sql.gz" /tmp/pg-kosha.sql.gz
sudo docker exec pg psql -U postgres -q -c "DROP DATABASE IF EXISTS kosha WITH (FORCE);" -c "CREATE DATABASE kosha;"
gunzip -c /tmp/pg-kosha.sql.gz | sudo docker exec -i pg psql -U postgres -q kosha >/dev/null
rm -f /tmp/pg-kosha.sql.gz

# Re-key a bare-named namespace to its scoped id (no-op when the dump is
# already scoped). manifests.namespace_id references namespaces.id, so:
# insert the new id, repoint the manifest, drop the old id — one txn.
sudo docker exec -i pg psql -U postgres -q -v ON_ERROR_STOP=1 \
  -v bare="$NS" -v scoped="$SCOPED" kosha <<'SQL'
BEGIN;
INSERT INTO kosha.namespaces (id, created_at)
  SELECT :'scoped', created_at FROM kosha.namespaces WHERE id = :'bare'
  ON CONFLICT (id) DO NOTHING;
UPDATE kosha.manifests SET namespace_id = :'scoped' WHERE namespace_id = :'bare';
DELETE FROM kosha.namespaces WHERE id = :'bare';
COMMIT;
SQL
ROWS="$(sudo docker exec pg psql -U postgres -tA kosha \
  -c "SELECT count(*) FROM kosha.manifests WHERE namespace_id = '$SCOPED'")"
[ "$ROWS" = "1" ] || { echo "::error::no manifest row for '$SCOPED' after restore"; exit 1; }

# Server restart: boot-restore reads the manifests, warmup (if configured
# for $SCOPED) hydrates before /readyz goes green. The local store is
# wiped unless it already holds exactly this snapshot.
sudo docker stop kosha >/dev/null 2>&1 || true
sudo rm -rf /var/cache/kosha-placeholder 2>/dev/null || true
if [ "$KEEP_LOCAL" = 1 ]; then
  echo "LOCAL-CACHE-KEPT $IDENTITY ($(sudo du -sh "$KOSHA_LOCAL_DATA" 2>/dev/null | cut -f1) local)"
else
  wipe_local_cache
  echo "LOCAL-CACHE-WIPED"
fi
sudo docker start kosha >/dev/null
until curl -sf http://127.0.0.1:8080/healthz >/dev/null; do sleep 3; done

# Segment count matters for anything that reasons about a per-query probe
# budget spread across segments (e.g. knn.num_candidates is a whole-query
# budget, not per-segment) — surface it here rather than leave it buried.
# Namespace-scoped stats: global /stats is admin-only since #262.
STATS_SEGMENTS="$(curl -sf -H "Authorization: Bearer $API_KEY" \
  "http://127.0.0.1:8080/v1/namespaces/$NS/stats" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(f\"{d['segments']} segments, {d['documents']} docs\")" \
  2>/dev/null || echo "stats unavailable")"
echo "RESTORE-STATS $SCOPED: $STATS_SEGMENTS"
case "$STATS_SEGMENTS" in
  "0 segments"*|"stats unavailable") echo "::error::restored namespace '$NS' is not visible to tenant '$TENANT'"; exit 1 ;;
esac

marker_write local "$NS" "$IDENTITY"
echo "RESTORE-COMPLETE $NAME — server up; wait on /readyz for warmup before benching"
