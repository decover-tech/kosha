#!/bin/bash
# Local cache bytes of one namespace, per segment file class (#381).
#
# Usage: ./local_bytes.sh <namespace> [tenant] [label]
#
# Walks $KOSHA_LOCAL_DATA/<tenant>/<ns>/ (default /data/kosha/data/default/<ns>,
# the server data dir on the bench VM) and prints one line per class:
#
#   LOCAL-BYTES label=<label> ns=<ns> class=<class> files=<n> bytes=<n>
#
# Whole files and span-cache entries ({file}.spans/<start>-<end>) are
# separate classes, so a run can tell "doc_vectors.bin downloaded whole"
# from "doc_vectors.bin read by range":
#
#   vector.idx  vector.idx.spans  vector.offsets
#   doc_vectors.bin  doc_vectors.bin.spans
#   doc_store.bin  doc_store.bin.spans
#   postings  postings.spans   (postings-XX.bin)
#   other  (everything else: footer, inverted.idx, filters, ...)
#
# vector.<n>.idx / vector.<n>.offsets (further vector columns) count with
# vector.idx / vector.offsets. A final class=total line sums them. Read-only;
# missing directory prints a total of 0.
set -euo pipefail
NS="${1:?usage: local_bytes.sh <namespace> [tenant] [label]}"
TENANT="${2:-default}"
LABEL="${3:-now}"
KOSHA_LOCAL_DATA="${KOSHA_LOCAL_DATA:-/data/kosha/data}"
case "$NS/$TENANT" in /*|*/|*..*) echo "local_bytes.sh: bad namespace $TENANT/$NS" >&2; exit 1 ;; esac
ROOT="$KOSHA_LOCAL_DATA/$TENANT/$NS"
# The server writes the cache as its own uid (10001 on the bench VM), so
# read it through sudo where that works without a password.
SUDO=""
sudo -n true 2>/dev/null && SUDO="sudo"
{ $SUDO find "$ROOT" -type f -printf '%s %P\n' 2>/dev/null || true; } | awk -v label="$LABEL" -v ns="$NS" '
function cls(path,   n, parts, base, parent) {
  n = split(path, parts, "/")
  base = parts[n]
  parent = (n > 1) ? parts[n - 1] : ""
  if (parent ~ /\.spans$/) {
    sub(/\.spans$/, "", parent)
    return norm(parent) ".spans"
  }
  return norm(base)
}
function norm(f) {
  if (f ~ /^vector(\.[0-9]+)?\.idx$/) return "vector.idx"
  if (f ~ /^vector(\.[0-9]+)?\.offsets$/) return "vector.offsets"
  if (f == "doc_vectors.bin" || f == "doc_store.bin") return f
  if (f ~ /^postings-.*\.bin$/) return "postings"
  return "other"
}
{
  size = $1
  sub(/^[0-9]+ /, "")
  c = cls($0)
  files[c]++; bytes[c] += size
  tf++; tb += size
}
END {
  for (c in files) printf "LOCAL-BYTES label=%s ns=%s class=%s files=%d bytes=%.0f\n", label, ns, c, files[c], bytes[c]
  printf "LOCAL-BYTES label=%s ns=%s class=total files=%d bytes=%.0f\n", label, ns, tf + 0, tb + 0
}' | sort
