#!/bin/bash
# Research #387: offline two-tier (head/tail) impact-index analysis over a
# restored namespace. Measurement only; it changes nothing on the server.
#
# Run after restore_namespace.sh (it needs the restored manifest in pg
# and the segments in the bench bucket). It copies the manifest live
# segments to local disk (skipping doc_store.bin when doc_store.offsets is
# present, and all vector files), builds kosha-two-tier-eval in the server
# image builder stage, and runs it over the query file. For each head
# fraction it reports the share of queries whose exact top-k the head
# alone certifies, plus footprint and tail-GET estimates.
#
# Validity (two_tier_check.py): the run refuses a manifest with tombstones,
# deletion vectors or shards (the analysis counts every posting as live),
# and afterwards fails unless no segment was skipped, no query was cut by
# the time budget, violations are 0, and the oracle top-k of the first
# TWO_TIER_CROSSCHECK queries equals what the live server returns (ids and
# scores, OR, k=10). A failed check writes validity.json and exits 1: the
# certification numbers of that run must not be used.
#
# Usage: ./two_tier_eval.sh <namespace> <out-dir>
# Env:
#   TWO_TIER_FRACTIONS   head fractions (default 0.1,0.2,0.3)
#   TWO_TIER_MAX_QUERIES cap on queries (default 0 = the whole file)
#   TWO_TIER_MAX_SECONDS stop starting new 256-query batches after this
#                        long in the query pass (default 3600; 0 = no cap).
#                        A capped run reports queries_skipped_time_budget
#                        and fails the validity check, but its numbers for
#                        the analyzed prefix are kept.
#   TWO_TIER_CROSSCHECK  queries to compare against the live server
#                        (default 200; 0 skips the check and fails validity)
#   TWO_TIER_THREADS     analysis threads (default 8; each holds dense
#                        per-segment arrays, ~41 bytes per doc of the
#                        largest segment)
#   TWO_TIER_MIN_SPLIT   terms with fewer postings stay whole (default 128)
#   TWO_TIER_STATS       namespace (default, the engine default) or segment
#   QUERIES_FILE         default /data/queries.txt
set -euo pipefail

NS="${1:?usage: two_tier_eval.sh <namespace> <out-dir>}"
OUT="${2:?out-dir required}"
TENANT="${KOSHA_TENANT:-default}"
SCOPED="$TENANT/$NS"
SEG_BUCKET="${KOSHA_S3_BUCKET:-decoverai-bench-kosha-segments}"
SRC="s3://$SEG_BUCKET/segments/$SCOPED"
WORK="/data/two-tier/$NS"
FRACTIONS="${TWO_TIER_FRACTIONS:-0.1,0.2,0.3}"
MAX_QUERIES="${TWO_TIER_MAX_QUERIES:-0}"
THREADS="${TWO_TIER_THREADS:-8}"
MIN_SPLIT="${TWO_TIER_MIN_SPLIT:-128}"
STATS="${TWO_TIER_STATS:-namespace}"
QUERIES_FILE="${QUERIES_FILE:-/data/queries.txt}"
MAX_SECONDS="${TWO_TIER_MAX_SECONDS:-3600}"
CROSSCHECK="${TWO_TIER_CROSSCHECK:-200}"
API_KEY="${KOSHA_API_KEY:-sk-bench}"
HOST="http://127.0.0.1:8080"
HERE="$(cd "$(dirname "$0")" && pwd)"

sudo mkdir -p "$OUT" "$WORK"
sudo chown -R "$(id -u):$(id -g)" "$OUT" /data/two-tier

# Live segment ids from the restored manifest: the snapshot prefix can
# also hold segments a compaction retired.
sudo docker exec pg psql -U postgres -tA kosha \
  -c "SELECT segments_json FROM kosha.manifests WHERE namespace_id = '$SCOPED'" > "$WORK/manifest.json"
python3 - "$WORK/manifest.json" "$WORK/segment_ids.txt" <<'PY'
import json, sys
raw = open(sys.argv[1]).read().strip()
doc = json.loads(raw)
entries = doc["segments"] if isinstance(doc, dict) else doc
ids = []
for e in entries:
    sid = e.get("segment_id") if isinstance(e, dict) else e
    if isinstance(sid, dict):
        sid = next(iter(sid.values()))
    ids.append(str(sid))
open(sys.argv[2], "w").write("\n".join(ids) + "\n")
print(f"{len(ids)} live segments in the manifest")
PY
cp "$WORK/manifest.json" "$OUT/manifest.json"
# Deletes would make the oracle differ from the engine: refuse early.
python3 "$HERE/two_tier_check.py" manifest "$WORK/manifest.json"
N_SEGS=$(wc -l < "$WORK/segment_ids.txt" | tr -d ' ')
[ "$N_SEGS" -gt 0 ] || { echo "::error::no live segments for $SCOPED"; exit 1; }

FIRST=$(head -1 "$WORK/segment_ids.txt")
DOCSTORE_EXCLUDE=""
if aws s3 ls "$SRC/$FIRST/doc_store.offsets" >/dev/null 2>&1; then
  DOCSTORE_EXCLUDE="--exclude doc_store.bin"
fi
echo "copying $N_SEGS segments from $SRC to $WORK (doc store excluded: ${DOCSTORE_EXCLUDE:-no})"
T0=$(date +%s)
# shellcheck disable=SC2086 # DOCSTORE_EXCLUDE is two words or empty
xargs -P 16 -I{} aws s3 sync "$SRC/{}" "$WORK/{}" --no-progress --only-show-errors \
  --exclude "doc_vectors.bin" --exclude "vector*" $DOCSTORE_EXCLUDE < "$WORK/segment_ids.txt"
echo "COPY-COMPLETE in $(( $(date +%s) - T0 ))s, $(du -sh "$WORK" | cut -f1) local"
# Object sizes of every segment file, including those not copied
# (doc_store.bin, vectors), for the footprint table.
aws s3 ls "$SRC/" --recursive --summarize > "$OUT/s3_listing.txt"
tail -2 "$OUT/s3_listing.txt"

cd ~/kosha
sudo docker build -q --target builder -t kosha-builder:bench . >/dev/null
QARGS=""
[ "$MAX_QUERIES" != 0 ] && QARGS="--max-queries $MAX_QUERIES"
[ "$MAX_SECONDS" != 0 ] && QARGS="$QARGS --max-seconds $MAX_SECONDS"
sudo docker run --rm -v /data:/data kosha-builder:bench sh -c "
  cargo build --release --locked -p kosha-query --bin kosha-two-tier-eval &&
  ./target/release/kosha-two-tier-eval \
    --segments-root $WORK --segment-list $WORK/segment_ids.txt \
    --queries $QUERIES_FILE $QARGS --k 10 --head-fractions $FRACTIONS \
    --min-split-df $MIN_SPLIT --stats $STATS --threads $THREADS --footprint \
    --out $OUT/report.json --per-query $OUT/per_query.jsonl" 2>&1 | tee "$OUT/summary.txt"
sudo chown -R "$(id -u):$(id -g)" "$OUT"
echo "TWO-TIER-EVAL-COMPLETE $NS -> $OUT"

# Cross-check the oracle against the live server, then gate the run.
for i in $(seq 120); do
  curl -sf "$HOST/readyz" >/dev/null 2>&1 && break
  [ "$i" -eq 120 ] && echo "server not ready after 600s; the cross-check will fail"
  sleep 5
done
python3 "$HERE/two_tier_check.py" run "$OUT/report.json" "$OUT/per_query.jsonl" \
  "$HOST" "$NS" "$API_KEY" "$CROSSCHECK" "$OUT/validity.json" 2>&1 | tee -a "$OUT/summary.txt"
