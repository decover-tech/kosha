#!/bin/bash
# kNN probe-budget sweep (#384): recall@10 vs p50/p99 and postings probed,
# one point per server configuration, on an already restored and warmed
# vector namespace (run after run_phases.sh vector).
#
# Usage:
#   ./knn_budget_sweep.sh <namespace> <out-dir> <embeddings-dir> [qps] [duration] [points]
#
# <points> is a semicolon-separated list of sweep points, each
#   label:ENV=VALUE,ENV=VALUE
# where every ENV is one of the kNN probe knobs below (anything else is
# refused). The shorthand "fractions" (the default) is the issue #384 step
# 1 sweep:
#   f0.05:KOSHA_KNN_BUDGET_FRACTION=0.05;f0.03:...=0.03;f0.02:...;f0.01:...
# The shorthand "relative" is the step 2 sweep over the #384 flags (default
# OFF on the server), all relative points at raw centroid distance
# (alpha=0, SPANN's own rule): at the default alpha=1 the best posting's
# score is usually clamped to 0, so eps has no effect there and only the
# min_probes floor would be measured.
#   f0.05            baseline
#   a0, a0.5         ranking alone (KOSHA_KNN_PROBE_RADIUS_ALPHA), fixed budget
#   a0rel0.5/1/2     alpha=0 + KOSHA_KNN_RELATIVE_CUTOFF_EPS 0.5, 1.0, 2.0
#                    (the toy harness found eps <= 0.3 far too tight)
#   a0rel1m500, a0rel0.5m1000
#                    KOSHA_KNN_RELATIVE_MIN_PROBES 500 / 1000 (default floor is
#                    num_candidates = 100, ~0.56 recall at 10M)
# The shorthand "bound" is the alpha-scaled bound stop
# (KOSHA_KNN_BOUND_STOP_ALPHA, default OFF):
#   f0.05            baseline
#   b1, b0.95, b0.9, b0.85
#                    skip a posting once the segment's k-th score reaches
#                    alpha x the posting's similarity ceiling (Block-Max
#                    Pruning reports alpha=0.85 near-lossless on its index)
# The shorthand "radius" is the #458 probe-order radius
# (KOSHA_KNN_PROBE_RADIUS, default max). It only changes anything on a
# snapshot written with KOSHA_VECTOR_WRITE_VERSION=5 (p50/p90 stored);
# older segments fall back to the max radius:
#   f0.05            baseline (max radius, fixed budget)
#   rp90, rp50       rank by the p90 / p50 member distance, fixed budget
#   rp90a0.5         p90 at half radius weight
#   rp90rel1m500     p90 + alpha=0 relative cutoff (eps 1, min 500)
# Each recall query reports which rule set its cutoff
# (timings.knn_cutoff_rule: budget / eps / floor / zero_best), and the
# report tabulates the shares, so a point that misses the bar can be read.
#
# Per point: the kosha container is recreated from its original
# configuration (environment, image) plus the point's overrides, the local
# NVMe cache is kept (only the in-process caches start cold), warm_gate.py
# re-warms with the workload queries (fast when everything is local), then
#   1. a warm, result-cache-off kNN phase at <qps> for <duration> seconds
#      (query_bench.py --server-timings --raw-out), and
#   2. recall_check.py on SWEEP_SAMPLE (default 1000) random queries with a
#      fixed seed, tie-aware recall and per-query knn_postings, sharing one
#      ground-truth cache across points.
# The container is put back to its original configuration at the end (also
# on failure), and knn_sweep_report.py writes <out-dir>/sweep.{json,md,csv}.
#
# Cost: ~(duration + ~8 min) per point after the first ground-truth scan.
set -euo pipefail

NS="${1:?usage: knn_budget_sweep.sh <namespace> <out-dir> <embeddings-dir> [qps] [duration] [points]}"
OUT="${2:?out-dir required}"
EMB_DIR="${3:?embeddings dir required}"
QPS="${4:-8}"
DURATION="${5:-300}"
POINTS="${6:-fractions}"
SAMPLE="${SWEEP_SAMPLE:-1000}"
SEED="${SWEEP_SEED:-384}"
NUM_CANDIDATES="${SWEEP_NUM_CANDIDATES:-100}"
API_KEY="${KOSHA_API_KEY:-sk-bench}"
RETURN_FIELDS="${RETURN_FIELDS:-ids}"
HERE="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$HERE/.." && pwd)"
PY=~/venv311/bin/python3
# The knobs a point may set. All are read once at server start, hence the
# container recreate per point.
KNOBS="KOSHA_KNN_BUDGET_FRACTION KOSHA_KNN_BUDGET_MAX_CAP KOSHA_KNN_RELATIVE_CUTOFF_EPS KOSHA_KNN_RELATIVE_MIN_PROBES KOSHA_KNN_PROBE_RADIUS_ALPHA KOSHA_KNN_PROBE_RADIUS KOSHA_KNN_BOUND_STOP_ALPHA"

if [ "$POINTS" = fractions ]; then
  POINTS="f0.05:KOSHA_KNN_BUDGET_FRACTION=0.05;f0.03:KOSHA_KNN_BUDGET_FRACTION=0.03;f0.02:KOSHA_KNN_BUDGET_FRACTION=0.02;f0.01:KOSHA_KNN_BUDGET_FRACTION=0.01"
fi
if [ "$POINTS" = relative ]; then
  A0=KOSHA_KNN_PROBE_RADIUS_ALPHA=0
  EPS=KOSHA_KNN_RELATIVE_CUTOFF_EPS
  MIN=KOSHA_KNN_RELATIVE_MIN_PROBES
  POINTS="f0.05:KOSHA_KNN_BUDGET_FRACTION=0.05;a0:$A0;a0.5:KOSHA_KNN_PROBE_RADIUS_ALPHA=0.5"
  POINTS="$POINTS;a0rel0.5:$A0,$EPS=0.5;a0rel1:$A0,$EPS=1.0;a0rel2:$A0,$EPS=2.0"
  POINTS="$POINTS;a0rel1m500:$A0,$EPS=1.0,$MIN=500;a0rel0.5m1000:$A0,$EPS=0.5,$MIN=1000"
fi
if [ "$POINTS" = bound ]; then
  B=KOSHA_KNN_BOUND_STOP_ALPHA
  POINTS="f0.05:KOSHA_KNN_BUDGET_FRACTION=0.05;b1:$B=1;b0.95:$B=0.95;b0.9:$B=0.9;b0.85:$B=0.85"
fi
if [ "$POINTS" = radius ]; then
  R=KOSHA_KNN_PROBE_RADIUS
  POINTS="f0.05:KOSHA_KNN_BUDGET_FRACTION=0.05;rp90:$R=p90;rp50:$R=p50;rp90a0.5:$R=p90,KOSHA_KNN_PROBE_RADIUS_ALPHA=0.5"
  POINTS="$POINTS;rp90rel1m500:$R=p90,KOSHA_KNN_PROBE_RADIUS_ALPHA=0,KOSHA_KNN_RELATIVE_CUTOFF_EPS=1.0,KOSHA_KNN_RELATIVE_MIN_PROBES=500"
fi
case "$POINTS" in *[!A-Za-z0-9_.=:\;,-]*) echo "points may only contain [A-Za-z0-9_.=:;,-]: $POINTS"; exit 1 ;; esac

for f in --sampling --truth-cache --server-timings --no-cache; do
  $PY "$BENCH_DIR/recall_check.py" --help | grep -q -- "$f" || { echo "FLAGS-MISSING recall_check $f"; exit 1; }
done
sudo docker inspect kosha >/dev/null || { echo "no kosha container to sweep"; exit 1; }

sudo mkdir -p "$OUT" && sudo chown -R "$(id -u):$(id -g)" "$OUT"
# The original container configuration, captured once, minus any knob a
# point may set (so overrides never accumulate across points).
IMAGE=$(sudo docker inspect kosha --format '{{.Config.Image}}')
sudo docker inspect kosha --format '{{range .Config.Env}}{{println .}}{{end}}' | sed '/^$/d' > "$OUT/base.env"
KNOB_RE="^($(echo $KNOBS | tr ' ' '|'))="
grep -Ev "$KNOB_RE" "$OUT/base.env" > "$OUT/base-sans-knobs.env" || true
echo "sweep: image=$IMAGE points=$POINTS sample=$SAMPLE seed=$SEED qps=$QPS duration=${DURATION}s"
echo "base kNN knobs on the container: $(grep -E "$KNOB_RE" "$OUT/base.env" | tr '\n' ' ' || true)"

iso_now() { date -u +%Y-%m-%dT%H:%M:%S.%NZ; }
recreate() { # recreate <env-file>
  sudo docker rm -f kosha >/dev/null
  # Same shape as benchmark.yml: host network, cache volume, env.
  sudo docker run -d --name kosha --network host --env-file "$1" \
    -v /data/kosha:/var/cache/kosha "$IMAGE" >/dev/null
  for i in $(seq 120); do curl -sf http://127.0.0.1:8080/v1/readyz >/dev/null 2>&1 && return 0; sleep 5; done
  echo "::error::kosha /readyz not green 600s after recreate"; sudo docker logs --tail 200 kosha 2>&1 || true
  return 1
}
restore_base() { echo "restoring the original kosha container"; recreate "$OUT/base.env" || true; }
trap restore_base EXIT

TRUTH="$OUT/truth-s${SAMPLE}-seed${SEED}.json"
IFS=';' read -ra PTS <<< "$POINTS"
for pt in "${PTS[@]}"; do
  [ -z "$pt" ] && continue
  label="${pt%%:*}"; assigns="${pt#*:}"
  [ "$label" = "$pt" ] && { echo "point without label: $pt"; exit 1; }
  dir="$OUT/$label"; mkdir -p "$dir/server"
  cp "$OUT/base-sans-knobs.env" "$dir/server.env"
  IFS=',' read -ra AS <<< "$assigns"
  for a in "${AS[@]}"; do
    key="${a%%=*}"
    case " $KNOBS " in *" $key "*) ;; *) echo "point $label: $key is not a sweep knob ($KNOBS)"; exit 1 ;; esac
    echo "$a" >> "$dir/server.env"
  done
  grep -E "$KNOB_RE" "$dir/server.env" > "$dir/knobs.env" || true
  echo "=== SWEEP-POINT $label: $(tr '\n' ' ' < "$dir/knobs.env")"
  recreate "$dir/server.env"
  since=$(iso_now)
  # Re-warm in-process caches with the workload (local NVMe is kept).
  $PY "$BENCH_DIR/warm_gate.py" --host http://127.0.0.1:8080 --namespace "$NS" \
    --queries-file /data/queries.txt --api-key "$API_KEY" --topk 10 \
    --knn-embeddings /data/queries_emb.f32 --return "$RETURN_FIELDS" \
    --timeout "${WARM_GATE_TIMEOUT_S:-10800}" > "$dir/warm_gate.log" 2>&1 \
    || { echo "warm gate failed for $label"; tail -20 "$dir/warm_gate.log"; exit 1; }
  grep -E 'WARM-GATE-READY' "$dir/warm_gate.log" | tail -1 || true
  sudo docker logs --timestamps --since "$since" kosha 2>&1 | gzip > "$dir/server/warm_gate.log.gz" || true
  since=$(iso_now)
  $PY "$BENCH_DIR/query_bench.py" --host http://127.0.0.1:8080 --namespace "$NS" \
    --queries-file /data/queries.txt --api-key "$API_KEY" \
    --qps "$QPS" --topk 10 --duration "$DURATION" --timeout 600 --phase warm \
    --knn-embeddings /data/queries_emb.f32 --knn-num-candidates "$NUM_CANDIDATES" --no-cache \
    --server-timings --raw-out "$dir/knn_warm_nocache.requests.jsonl.gz" --return "$RETURN_FIELDS" \
    --out "$dir/knn_warm_nocache.json"
  sudo docker logs --timestamps --since "$since" kosha 2>&1 | gzip > "$dir/server/knn_warm_nocache.log.gz" || true
  since=$(iso_now)
  $PY "$BENCH_DIR/recall_check.py" --host http://127.0.0.1:8080 --namespace "$NS" --api-key "$API_KEY" \
    --embeddings-dir "$EMB_DIR" --queries-file /data/queries.txt --queries-emb /data/queries_emb.f32 \
    --sample "$SAMPLE" --sampling random --seed "$SEED" --k 10 --num-candidates "$NUM_CANDIDATES" \
    --server-timings --no-cache --truth-cache "$TRUTH" --label "$label" --out "$dir/recall.json"
  sudo docker logs --timestamps --since "$since" kosha 2>&1 | gzip > "$dir/server/recall.log.gz" || true
  echo "SWEEP-POINT-COMPLETE $label"
done

$PY "$BENCH_DIR/knn_sweep_report.py" "$OUT" --out-prefix "$OUT/sweep"
echo "SWEEP-COMPLETE"
