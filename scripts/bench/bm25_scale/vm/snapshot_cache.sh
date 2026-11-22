#!/bin/bash
# Snapshot-identity markers for reusing state on a reused bench VM.
# Sourced (not run) by restore_namespace.sh, build_and_import.sh and
# run_phases.sh.
#
# Two things outlive a run on a VM kept after a failed run: the server
# local cache (/data/kosha/data, up to ~249GB for the vector namespace)
# and the namespace objects in the stack bench bucket. Re-populating
# either costs real time, but reusing them is only safe when they came
# from the very snapshot being restored: segment ids are derived from the
# namespace name (<ns>-offline-NNNNNN for offline builds), so a rebuilt
# snapshot of the same namespace reuses ids, and stale files under those
# ids would be served silently wrong.
#
# A snapshot identity is its name plus the S3 ETag and LastModified of
# its pg-kosha.sql.gz: snapshot_namespace.sh re-uploads that dump on every
# (re-)snapshot, so re-creating a snapshot under the same name changes
# its identity.
#
# Markers live on the VM under $SNAPSHOT_MARKER_DIR, outside the server
# data dir:
#   local.<ns>   the local cache holds <ns> as restored from <identity>
#   bucket.<ns>  the bench bucket holds <ns> as restored from <identity>
# The bucket and the VM belong to the same terraform stack, so a VM-side
# marker for the bucket shares its lifetime. Protocol: a marker is
# removed BEFORE the state it describes is touched and written only AFTER
# that state is complete, so a failure in between leaves no marker and
# the next restore starts clean.

SNAPSHOT_CACHE_BUCKET="${SNAPSHOT_CACHE_BUCKET:-decoverai-bench-corpus-cache}"
SNAPSHOT_MARKER_DIR="${SNAPSHOT_MARKER_DIR:-/data/bench-snapshot-markers}"
KOSHA_LOCAL_DATA="${KOSHA_LOCAL_DATA:-/data/kosha/data}"

# snapshot_identity <snapshot-name>: one line, or nonzero if absent.
snapshot_identity() {
  local name="$1" meta
  meta="$(aws s3api head-object --bucket "$SNAPSHOT_CACHE_BUCKET" \
    --key "snapshots/$name/pg-kosha.sql.gz" \
    --query '[ETag,LastModified]' --output text 2>/dev/null)" || return 1
  [ -n "$meta" ] || return 1
  echo "$name $meta" | tr -s ' \t' '  '
}

_marker_dir() {
  [ -d "$SNAPSHOT_MARKER_DIR" ] && [ -w "$SNAPSHOT_MARKER_DIR" ] && return 0
  sudo mkdir -p "$SNAPSHOT_MARKER_DIR"
  sudo chown "$(id -u):$(id -g)" "$SNAPSHOT_MARKER_DIR"
}

# marker_matches local|bucket <ns> <identity>
marker_matches() {
  local f="$SNAPSHOT_MARKER_DIR/$1.$2"
  [ -f "$f" ] && [ "$(cat "$f")" = "$3" ]
}

# marker_write local|bucket <ns> <identity>
marker_write() {
  _marker_dir
  printf '%s\n' "$3" > "$SNAPSHOT_MARKER_DIR/$1.$2.tmp"
  mv "$SNAPSHOT_MARKER_DIR/$1.$2.tmp" "$SNAPSHOT_MARKER_DIR/$1.$2"
}

# marker_clear local|bucket <ns>
marker_clear() {
  rm -f "$SNAPSHOT_MARKER_DIR/$1.$2" 2>/dev/null || sudo rm -f "$SNAPSHOT_MARKER_DIR/$1.$2"
}

# wipe_local_cache: empty the server data dir. Callers stop the kosha
# container first. The wipe is global (every namespace lives under the one
# data dir), so every local.* marker goes, and it goes first.
wipe_local_cache() {
  sudo rm -f "$SNAPSHOT_MARKER_DIR"/local.* 2>/dev/null || true
  # find, not a glob: the glob would expand as this (unprivileged) user.
  sudo find "${KOSHA_LOCAL_DATA:?}" -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true
}
