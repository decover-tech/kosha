#!/usr/bin/env bash
# Append an idempotent "Latency" section — the current cold + warm latency
# from the `segment_memory` microbench — to a git commit message.
#
# Two entry points share this one driver (so the workflow re-runs verbatim
# what the local hook already ran):
#
#   * `scripts/commit_bench_section.sh hook [<msg-file>]`
#       wired into `.pre-commit-config.yaml` as a `prepare-commit-msg` hook;
#       appends the section to the message file git hands the hook BEFORE
#       the commit is sealed. Never blocks the commit on bench failure.
#
#   * `scripts/commit_bench_section.sh amend`
#       used by `.github/workflows/pre-commit.yml`: runs the same bench on
#       the PR head, amends the head commit's message with the section, and
#       force-pushes back to the PR branch.
#
# The bench: `cargo bench -p kosha-query --bench segment_memory` — a plain
# `harness = false` binary that builds a deterministic LCG corpus then prints
# a table of cold open / cold + warm query latency for the v1 (eager) and v2
# (lazy) `inverted.idx` formats. We take the v2 (lazy) column as "current".
# See `crates/kosha-query/benches/segment_memory.rs`.
#
# Idempotency: the section is wrapped in fenced markers (`---kosha-bench---`
# .. `---end kosha-bench---`). If the markers are already present in the
# target message, the script exits WITHOUT rewriting — so the workflow's own
# synchronize re-trigger never loops, and re-running a commit never
# duplicates the section.
#
# Skipping: in `hook` mode, missing cargo / a failed build / an unparseable
# table logs a warning to stderr and exits 0 (the commit proceeds, just
# without a section). In `amend` mode the same failures exit non-zero so the
# workflow surfaces them in the run's red box.
#
# Prepare-commit-msg install (one-time):
#   pre-commit install --hook-type prepare-commit-msg
set -euo pipefail

readonly BEGIN_MARKER='---kosha-bench---'
readonly END_MARKER='---end kosha-bench---'

say()  { printf '\n=== %s ===\n' "$*"; }
warn() { printf '\n!!! %s\n' "$*" >&2; }
die()  { warn "$*"; exit 1; }

# Tempfile tracking. We accumulate paths into a global array and install ONE
# EXIT trap at script load — that trap iterates live names instead of baking
# out-of-scope `local` vars into the trap string (the SC2064 trap-with-locals
# pitfall). Each `mktmp` push_back is removed unconditionally on exit.
#
# `set -e` is live during the EXIT trap, so a `[ -n "$f" ] && rm` whose LHS
# returns false (an empty entry from `${arr[@]:-}` for an empty array) would
# otherwise abort the trap and clobber a deliberately-set exit code (e.g.
# the `exit 5` no-op signal) by overriding it with 1. The explicit
# `return 0` and `|| continue` make this function unconditionally clean.
declare -a TMPFILES=()

cleanup_tmpfiles() {
  local f
  for f in "${TMPFILES[@]:-}"; do
    [ -n "$f" ] || continue
    rm -f -- "$f"
  done
  return 0
}
trap cleanup_tmpfiles EXIT

mktmp() {
  local t
  t="$(mktemp "${TMPDIR:-/tmp}/kosha-bench.XXXXXX")" || return $?
  TMPFILES+=("$t")
  printf '%s\n' "$t"
}

# ─── idempotency: any existing fenced section ───────────────────────────────
# `-Fqe` lets the marker (which starts with `---`) be treated as a fixed
# pattern instead of an option chain by BSD/GNU grep; `--` guards the path.
has_marker() { grep -Fqe "$BEGIN_MARKER" -- "$1"; }

# Strip any `---kosha-bench---`..`---end kosha-bench---` block (inclusive)
# from $1 in place and trim trailing blank lines.
strip_block_inplace() {
  local f="$1"
  awk -v b="$BEGIN_MARKER" -v e="$END_MARKER" '
    $0 == b { skip = 1; next }
    $0 == e { skip = 0; next }
    !skip   { print }' "$f" \
  | awk '
      { lines[++n] = $0 }
      END {
        last = n
        while (last > 0 && lines[last] == "") last--
        for (j = 1; j <= last; j++) print lines[j]
      }' > "$f.tmp"
  mv -- "$f.tmp" "$f"
}

# ─── bench + section builder ────────────────────────────────────────────────
# run cargo bench (segment_memory), capturing stdout+stderr into $1.
# returns 5 on a soft-failure (no cargo, failed build) so callers can decide.
run_bench_into() {
  local out="$1"
  command -v cargo >/dev/null 2>&1 || { warn "cargo not on PATH — skipping kosha bench section."; return 5; }
  if ! cargo bench -p kosha-query --bench segment_memory > "$out" 2>&1; then
    warn "cargo bench (segment_memory) failed — see $out"
    return 5
  fi
  return 0
}

# Build the section body (BEGIN..END fenced block) into $1 by parsing the
# bench log $2. We take the v2 (lazy) column = "current" format. Returns 5
# if the parse looks incomplete.
build_section_into() {
  local out="$1" log="$2"
  local corpus rust_ver ts
  corpus="$(grep -E -m1 -o '^corpus: .*' "$log" || true)"
  rust_ver="$(rustc --version 2>/dev/null || echo 'rustc=n/a')"
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [ -z "$corpus" ]; then
    warn "no 'corpus:' line in bench output — skipping."
    return 5
  fi
  {
    printf '\n%s\n' "$BEGIN_MARKER"
    printf 'microbenchmark: segment_memory (cargo bench -p kosha-query --bench segment_memory)\n'
    printf '%s\n' "$corpus"
    printf 'runner: %s | %s | %s\n' "$(uname -srm)" "$rust_ver" "$ts"
    printf '\n'
    printf 'latency (v2 = current lazy format, median of inner samples):\n'
    awk '
      /^cold broad \("the"\) / || /^warm broad \("the"\) / || \
      /^warm 2-term AND /       || /^warm phrase /           || \
      /^warm wildcard w1\* / {
        label = ""
        for (i = 1; i <= NF - 3; i++) {
          if (i == 1) label = $i
          else        label = label " " $i
        }
        v2 = $(NF - 1)
        sub(/ms$/, "", v2)
        printf "  %-34s %s ms\n", label, v2
      }' "$log"
    printf '%s\n' "$END_MARKER"
  } > "$out"
  if ! grep -Fq 'cold broad' "$out" || ! grep -Fq 'warm broad' "$out"; then
    warn "bench output parsed no cold/warm rows — see $out"
    return 5
  fi
  return 0
}

# Splice fresh section $1 into message file $2: strip any prior block, then
# the section's own leading blank line is the separator (no extra newlines).
attach_to_msg_file() {
  local section="$1" msg="$2"
  strip_block_inplace "$msg"
  cat -- "$section" >> "$msg"
}

# ─── entry: hook (prepare-commit-msg) ───────────────────────────────────────
# Args: $1 = message file path. Source (`message`/`merge`/`squash`/`commit`/...)
# arrives via the PRE_COMMIT_COMMIT_MSG_SOURCE env var (pre-commit framework
# convention) — fall back to the bare git native positional if unset.
cmd_hook() {
  local msg="${1:-}"
  # A misconfigured pre-commit entry (or any invocation that doesn't supply
  # the message-file argument) must not block the commit — same "never
  # blocks on failure" contract as every other soft-fail path below. This
  # previously `die`d (exit 1), which pre-commit treats as a failed hook and
  # aborts the commit entirely; a config regression here once blocked every
  # commit in the repo.
  if [ -z "$msg" ]; then
    warn "hook: no message file path given — skipping (commit proceeds)."
    exit 0
  fi
  if [ ! -f "$msg" ]; then
    warn "hook: message file not found: $msg — skipping (commit proceeds)."
    exit 0
  fi

  local source="${PRE_COMMIT_COMMIT_MSG_SOURCE:-}"
  case "$source" in
    merge|squash|commit) exit 0 ;;   # don't touch merge/squash/amend messages
  esac
  if has_marker "$msg"; then
    say "kosha bench section already present — skipping."
    exit 0
  fi

  local log section
  log="$(mktmp)" || exit 0
  section="$(mktmp)" || exit 0

  run_bench_into "$log"                || exit 0
  build_section_into "$section" "$log" || exit 0
  attach_to_msg_file "$section" "$msg"
  say "appended kosha bench latency section to commit message."
}

# ─── entry: amend (workflow) ─────────────────────────────────────────────────
# Exit codes: 0 = amended; 5 = no-op (marker already present); non-zero = fail.
cmd_amend() {
  local msg log section
  msg="$(mktmp)"     || die "mktemp failed"
  log="$(mktmp)"     || die "mktemp failed"
  section="$(mktmp)" || die "mktemp failed"

  git log -1 --pretty='%B' > "$msg"
  if has_marker "$msg"; then
    say "kosha bench section already present on HEAD — nothing to amend."
    exit 5
  fi
  run_bench_into "$log"                || exit 1
  build_section_into "$section" "$log" || exit 1
  attach_to_msg_file "$section" "$msg"
  git commit --amend -F "$msg" --no-verify
  say "amended HEAD with kosha bench section ($(git rev-parse --short HEAD))."
}

# ─── dispatch ────────────────────────────────────────────────────────────────
case "${1:-}" in
  hook)  shift; cmd_hook "$@" ;;
  amend) cmd_amend ;;
  *)    die "usage: $0 hook <msg-file> | $0 amend" ;;
esac
