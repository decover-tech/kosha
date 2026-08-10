#!/usr/bin/env bash
# Regression tests for scripts/commit_bench_section.sh.
#
# Runs ~20 tests covering both entry points (hook + amend), the awk parser,
# idempotency, source-skip, soft-fail vs. loud-fail exit codes, and YAML
# validity of the config/workflow. Uses a cargo shim (canned bench output)
# so tests are deterministic and fast — no real cargo bench is invoked.
#
# Run:  bash scripts/test_commit_bench_section.sh
#   or: make bench-hook-test
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$SCRIPT_DIR/commit_bench_section.sh"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PASS=0
FAIL=0

ok()   { printf '  PASS %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  FAIL %s\n' "$1"; FAIL=$((FAIL + 1)); }

# assert_contains <file> <pattern> <label>
# Uses -Fqe so patterns starting with `---` work on BSD + GNU grep.
assert_contains() {
  local file="$1" pat="$2" label="$3"
  if grep -Fqe "$pat" -- "$file" 2>/dev/null; then ok "$label"; else bad "$label ($file missing '$pat')"; fi
}

assert_not_contains() {
  local file="$1" pat="$2" label="$3"
  if grep -Fqe "$pat" -- "$file" 2>/dev/null; then bad "$label ($file has '$pat')"; else ok "$label"; fi
}

# assert_exit <expected_rc> <label> <cmd...>
# The label must be the 2nd arg (right after expected_rc); remaining args
# are the command + its arguments.  `set +e` around the command prevents
# `set -e` from aborting the test runner when the command exits non-zero
# (which is exactly what we want to capture for negative-exit-code tests).
assert_exit() {
  local expect="$1"; shift
  local label="$1"; shift
  set +e
  "$@" >/dev/null 2>&1
  local rc=$?
  set -e
  if [ "$rc" -eq "$expect" ]; then ok "$label (exit=$rc)"; else bad "$label (exit=$rc, expected $expect)"; fi
}

assert_eq() {
  local a="$1" b="$2" label="$3"
  if [ "$a" = "$b" ]; then ok "$label"; else bad "$label ('$a' != '$b')"; fi
}

# ─── Canned bench output ────────────────────────────────────────────────────
# Matches the real segment_memory table format the awk parser expects.
# v1 (eager) column has distinct values so we can assert the parser picks v2.
BENCH_OUT='building corpus: 8 segs x 4000 docs, vocab ~20000
measuring v1 (eager)
measuring v2 (lazy)

corpus: 8 segments x 4000 docs (32000 total), vocab ~20000
inverted artifacts on disk: v1 22.4 MiB | v2 7.9 MiB

metric                        v1 (eager)    v2 (lazy)   v1/v2
open all segments             184.3ms        42.1ms      4.4x
resident while open         248.1MiB       81.3MiB      3.1x
cold broad ("the")           120.5ms        42.3ms      2.8x
warm broad ("the")             1.40ms         0.42ms      3.3x
warm 2-term AND                0.95ms         0.18ms      5.3x
warm phrase                    2.10ms         0.55ms      3.8x
warm wildcard w1*              3.30ms         1.10ms      3.0x'

BENCH_NO_CORPUS='measuring v1 (eager)
measuring v2 (lazy)

metric                        v1 (eager)    v2 (lazy)   v1/v2
cold broad ("the")           120.5ms        42.3ms      2.8x
warm broad ("the")             1.40ms         0.42ms      3.3x'

BENCH_NO_ROWS='corpus: 8 segments x 4000 docs (32000 total), vocab ~20000
inverted artifacts on disk: v1 22.4 MiB | v2 7.9 MiB

metric                        v1 (eager)    v2 (lazy)   v1/v2
open all segments             184.3ms        42.1ms      4.4x'

# ─── Test infrastructure ────────────────────────────────────────────────────
WORK=""
NO_CARGO_PATH="/usr/bin:/bin"

setup() {
  WORK="$(mktemp -d)"
  mkdir -p "$WORK/bin"
}

# make_cargo_shim <bench-output-string>
# Installs a fake `cargo` in $WORK/bin that prints the given string.
make_cargo_shim() {
  local out_file="$WORK/bench_output.txt"
  printf '%s\n' "$1" > "$out_file"
  printf '#!/usr/bin/env bash\ncat "%s"\nexit 0\n' "$out_file" > "$WORK/bin/cargo"
  chmod +x "$WORK/bin/cargo"
}

# hook_run <msg-file> <source>  — invokes cmd_hook with the cargo shim.
hook_run() {
  local msg="$1" source="${2:-}"
  PATH="$WORK/bin:$NO_CARGO_PATH" PRE_COMMIT_COMMIT_MSG_SOURCE="$source" \
    "$SCRIPT" hook "$msg"
}

amend_run() {
  GIT_DIR="$WORK/repo/.git" GIT_WORK_TREE="$WORK/repo" \
    PATH="$WORK/bin:$NO_CARGO_PATH" "$SCRIPT" amend
}

setup_git_repo() {
  git init -q "$WORK/repo"
  git -C "$WORK/repo" config user.email test@test.com
  git -C "$WORK/repo" config user.name test
  printf 'hello\n' > "$WORK/repo/file"
  git -C "$WORK/repo" add file
  git -C "$WORK/repo" commit -q -m "test subject" -m "test body line"
}

pristine_msg() {
  printf 'feat(query): some perf change\n\nA real commit body, ending here.\n' > "$1"
}

teardown() { [ -n "$WORK" ] && rm -rf "$WORK"; }
trap teardown EXIT

# ═══ TESTS ══════════════════════════════════════════════════════════════════

test_hook_appends_section() {
  setup; make_cargo_shim "$BENCH_OUT"
  local msg="$WORK/msg"; pristine_msg "$msg"
  hook_run "$msg" message || true
  assert_contains "$msg" '---kosha-bench---'    'hook: BEGIN marker appended'
  assert_contains "$msg" '---end kosha-bench---' 'hook: END marker appended'
  assert_contains "$msg" 'cold broad'            'hook: cold broad row present'
  assert_contains "$msg" 'warm broad'            'hook: warm broad row present'
  assert_contains "$msg" 'warm 2-term AND'       'hook: warm 2-term AND row present'
  assert_contains "$msg" 'warm phrase'           'hook: warm phrase row present'
  assert_contains "$msg" 'warm wildcard w1*'     'hook: warm wildcard row present'
}

test_hook_parses_v2_not_v1() {
  setup; make_cargo_shim "$BENCH_OUT"
  local msg="$WORK/msg"; pristine_msg "$msg"
  hook_run "$msg" message || true
  # v2 cold = 42.3ms, v1 cold = 120.5ms. Parser must pick v2.
  assert_contains "$msg" '42.3 ms'  'hook: parsed v2 cold (42.3, not v1 120.5)'
  assert_contains "$msg" '0.42 ms'  'hook: parsed v2 warm-broad (0.42, not v1 1.40)'
  assert_not_contains "$msg" '120.5 ms' 'hook: v1 cold value absent'
  assert_not_contains "$msg" '1.40 ms'  'hook: v1 warm value absent'
}

test_hook_has_metadata_lines() {
  setup; make_cargo_shim "$BENCH_OUT"
  local msg="$WORK/msg"; pristine_msg "$msg"
  hook_run "$msg" message || true
  assert_contains "$msg" 'microbenchmark: segment_memory' 'hook: microbenchmark line'
  assert_contains "$msg" 'corpus: 8 segments'              'hook: corpus line'
  assert_contains "$msg" 'runner:'                         'hook: runner line'
  assert_contains "$msg" 'latency (v2'                     'hook: latency header line'
}

test_hook_preserves_body() {
  setup; make_cargo_shim "$BENCH_OUT"
  local msg="$WORK/msg"; pristine_msg "$msg"
  hook_run "$msg" message || true
  assert_contains "$msg" 'feat(query): some perf change' 'hook: original subject preserved'
  assert_contains "$msg" 'A real commit body'              'hook: original body preserved'
}

test_hook_idempotent_double_run() {
  setup; make_cargo_shim "$BENCH_OUT"
  local msg="$WORK/msg"; pristine_msg "$msg"
  hook_run "$msg" message || true
  local before; before="$(cksum "$msg")"
  hook_run "$msg" message || true
  local after; after="$(cksum "$msg")"
  assert_eq "$before" "$after" 'hook: idempotent re-run (msg unchanged)'
  local count; count="$(grep -Fc 'kosha-bench' "$msg" || true)"
  assert_eq "2" "$count" 'hook: exactly one block (2 marker lines)'
}

test_hook_source_merge_skips() {
  setup; make_cargo_shim "$BENCH_OUT"
  local msg="$WORK/msg"; pristine_msg "$msg"
  local before; before="$(cksum "$msg")"
  assert_exit 0 'hook: source=merge exits 0' hook_run "$msg" merge
  assert_eq "$before" "$(cksum "$msg")" 'hook: source=merge msg unchanged'
}

test_hook_source_squash_skips() {
  setup; make_cargo_shim "$BENCH_OUT"
  local msg="$WORK/msg"; pristine_msg "$msg"
  local before; before="$(cksum "$msg")"
  assert_exit 0 'hook: source=squash exits 0' hook_run "$msg" squash
  assert_eq "$before" "$(cksum "$msg")" 'hook: source=squash msg unchanged'
}

test_hook_source_commit_skips() {
  setup; make_cargo_shim "$BENCH_OUT"
  local msg="$WORK/msg"; pristine_msg "$msg"
  local before; before="$(cksum "$msg")"
  assert_exit 0 'hook: source=commit exits 0' hook_run "$msg" commit
  assert_eq "$before" "$(cksum "$msg")" 'hook: source=commit msg unchanged'
}

test_hook_source_message_proceeds() {
  setup; make_cargo_shim "$BENCH_OUT"
  local msg="$WORK/msg"; pristine_msg "$msg"
  hook_run "$msg" message || true
  assert_contains "$msg" '---kosha-bench---' 'hook: source=message proceeds and appends'
}

test_hook_no_cargo_exits_zero() {
  setup  # no cargo shim installed
  local msg="$WORK/msg"; pristine_msg "$msg"
  local before; before="$(cksum "$msg")"
  PATH="$NO_CARGO_PATH" PRE_COMMIT_COMMIT_MSG_SOURCE=message \
    "$SCRIPT" hook "$msg" >/dev/null 2>&1
  local rc=$?
  assert_eq 0 "$rc" 'hook: no-cargo exits 0 (does not block commit)'
  assert_eq "$before" "$(cksum "$msg")" 'hook: no-cargo msg unchanged'
}

test_hook_no_corpus_exits_zero() {
  setup; make_cargo_shim "$BENCH_NO_CORPUS"
  local msg="$WORK/msg"; pristine_msg "$msg"
  local before; before="$(cksum "$msg")"
  assert_exit 0 'hook: no-corpus-line exits 0 (soft fail)' hook_run "$msg" message
  assert_eq "$before" "$(cksum "$msg")" 'hook: no-corpus-line msg unchanged'
}

test_hook_no_latency_rows_exits_zero() {
  setup; make_cargo_shim "$BENCH_NO_ROWS"
  local msg="$WORK/msg"; pristine_msg "$msg"
  local before; before="$(cksum "$msg")"
  assert_exit 0 'hook: no-latency-rows exits 0 (soft fail)' hook_run "$msg" message
  assert_eq "$before" "$(cksum "$msg")" 'hook: no-latency-rows msg unchanged'
}

test_hook_missing_arg_exits_zero() {
  # A misconfigured pre-commit entry (or any caller that omits the
  # message-file arg) must never block the commit — same soft-fail
  # contract as the no-cargo/no-corpus/no-rows cases below. This used to
  # `die` (exit 1); a `pass_filenames: false` regression on the
  # prepare-commit-msg hook entry once made every commit in the repo fail
  # this way.
  setup
  assert_exit 0 'hook: missing msg arg exits 0 (does not block commit)' \
    env PATH="$NO_CARGO_PATH" "$SCRIPT" hook
}

test_hook_missing_file_exits_zero() {
  setup
  assert_exit 0 'hook: nonexistent msg file exits 0 (does not block commit)' \
    env PATH="$NO_CARGO_PATH" "$SCRIPT" hook "$WORK/does-not-exist"
}

test_dispatch_bad_subcommand_exits_one() {
  setup
  assert_exit 1 'dispatch: bad subcommand exits 1' env PATH="$NO_CARGO_PATH" "$SCRIPT" bogus
}

test_amend_adds_markers() {
  setup; make_cargo_shim "$BENCH_OUT"; setup_git_repo
  amend_run || true
  git -C "$WORK/repo" log -1 --pretty='%B' > "$WORK/amended_msg"
  assert_contains "$WORK/amended_msg" '---kosha-bench---'    'amend: BEGIN marker on HEAD'
  assert_contains "$WORK/amended_msg" '---end kosha-bench---' 'amend: END marker on HEAD'
  assert_contains "$WORK/amended_msg" 'cold broad'            'amend: cold broad row on HEAD'
  assert_contains "$WORK/amended_msg" 'warm broad'            'amend: warm broad row on HEAD'
}

test_amend_preserves_subject_body() {
  setup; make_cargo_shim "$BENCH_OUT"; setup_git_repo
  amend_run || true
  git -C "$WORK/repo" log -1 --pretty='%B' > "$WORK/amended_msg"
  assert_contains "$WORK/amended_msg" 'test subject'    'amend: original subject preserved'
  assert_contains "$WORK/amended_msg" 'test body line'  'amend: original body preserved'
}

test_amend_idempotent_exit_5() {
  setup; make_cargo_shim "$BENCH_OUT"; setup_git_repo
  amend_run || true
  local sha; sha="$(git -C "$WORK/repo" rev-parse HEAD)"
  assert_exit 5 'amend: re-amend exits 5 (no-op)' amend_run
  local sha2; sha2="$(git -C "$WORK/repo" rev-parse HEAD)"
  assert_eq "$sha" "$sha2" 'amend: re-amend HEAD unchanged'
}

test_amend_no_cargo_exits_one() {
  setup; setup_git_repo  # no cargo shim
  assert_exit 1 'amend: no-cargo exits 1 (loud fail)' amend_run
}

test_amend_no_corpus_exits_one() {
  setup; make_cargo_shim "$BENCH_NO_CORPUS"; setup_git_repo
  assert_exit 1 'amend: no-corpus-line exits 1 (loud fail)' amend_run
}

test_yaml_pre_commit_config_valid() {
  if python3 -c "import yaml; yaml.safe_load(open('$REPO_ROOT/.pre-commit-config.yaml'))" 2>/dev/null; then
    ok 'yaml: .pre-commit-config.yaml parses'
  else
    bad 'yaml: .pre-commit-config.yaml parses'
  fi
}

test_yaml_kosha_bench_hook_does_not_set_pass_filenames_false() {
  # For a `prepare-commit-msg`-stage hook, the "filename" pre-commit
  # forwards *is* the commit-message file path (git's native hook
  # argument) — not a list of changed source files. `pass_filenames:
  # false` on this hook entry starves `commit_bench_section.sh hook` of
  # the one argument it requires, and every commit failed as a result
  # until this was caught. Assert it stays absent.
  # 'ABSENT' sentinel distinguishes "key not set" (correct — defaults to
  # pre-commit's own default, which forwards the msg-file arg) from an
  # explicit `pass_filenames: false` (the regression).
  local value
  value="$(python3 -c "
import yaml
cfg = yaml.safe_load(open('$REPO_ROOT/.pre-commit-config.yaml'))
for repo in cfg.get('repos', []):
    for hook in repo.get('hooks', []):
        if hook.get('id') == 'kosha-bench-commit-msg':
            print(hook.get('pass_filenames', 'ABSENT'))
" 2>/dev/null)"
  if [ "$value" = "False" ]; then
    bad 'yaml: kosha-bench-commit-msg hook does not set pass_filenames: false'
  else
    ok 'yaml: kosha-bench-commit-msg hook does not set pass_filenames: false'
  fi
}

test_yaml_workflow_valid() {
  if python3 -c "import yaml; yaml.safe_load(open('$REPO_ROOT/.github/workflows/pre-commit.yml'))" 2>/dev/null; then
    ok 'yaml: pre-commit.yml parses'
  else
    bad 'yaml: pre-commit.yml parses'
  fi
}

# ─── Runner ─────────────────────────────────────────────────────────────────

main() {
  printf '\n=== commit_bench_section.sh regression tests ===\n\n'
  test_hook_appends_section
  test_hook_parses_v2_not_v1
  test_hook_has_metadata_lines
  test_hook_preserves_body
  test_hook_idempotent_double_run
  test_hook_source_merge_skips
  test_hook_source_squash_skips
  test_hook_source_commit_skips
  test_hook_source_message_proceeds
  test_hook_no_cargo_exits_zero
  test_hook_no_corpus_exits_zero
  test_hook_no_latency_rows_exits_zero
  test_hook_missing_arg_exits_zero
  test_hook_missing_file_exits_zero
  test_dispatch_bad_subcommand_exits_one
  test_amend_adds_markers
  test_amend_preserves_subject_body
  test_amend_idempotent_exit_5
  test_amend_no_cargo_exits_one
  test_amend_no_corpus_exits_one
  test_yaml_pre_commit_config_valid
  test_yaml_kosha_bench_hook_does_not_set_pass_filenames_false
  test_yaml_workflow_valid
  printf '\n=== Results: %d passed, %d failed ===\n' "$PASS" "$FAIL"
  [ "$FAIL" -eq 0 ] || exit 1
}

main
