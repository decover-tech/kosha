#!/usr/bin/env python3
"""Keep docs/guarantees.md honest (#352).

Every number on the page comes from one of two places, and this script checks
both:

1. **Code constants.** Each table row whose Source cell names a
   ``crates/...rs`` file must have an entry in ``CHECKS`` below, keyed by the
   row's first cell. The script reads the constant from that file, converts
   both sides to one base unit (bytes, milliseconds or a plain count) and fails
   if the page's Value differs. It also fails if the row's Source cell does
   not name the file or the ``KOSHA_*`` override the check expects, or if a
   ``CHECKS`` entry has no row on the page.

2. **Measurements.** Each row of a table with a ``Result file`` column must
   cite a committed results file that exists. Every run id (11 digits) and
   date (YYYY-MM-DD) in the row must appear in that file. The commit in the
   row must be reachable from HEAD (skipped, with a note, in a shallow clone).

3. **No unchecked restatements.** Cells in the columns named in
   ``POINTER_COLUMNS`` (refusal and at-a-glance columns) may not state a
   number other than an HTTP status code or an issue reference: they must
   point at the checked row that holds the value instead. That keeps a
   restated default from drifting while its checked row is updated.

4. **Error codes.** A refusal cell that names a stable error ``code`` (in
   backticks) must give the HTTP status ``ErrorCode::status`` returns for it
   in ``crates/kosha-server/src/api_error.rs``, and must say "not retryable"
   exactly when ``ErrorCode::retryable`` is false for it.

It also fails on ``TBD``/``TODO`` on the page, if README.md does not link
to the page, or if #350's error model has landed (``ERRORS_PROTO`` exists)
while a table still has a pre-#350 status column (``PRE_350_COLUMNS``).

    python3 scripts/docs/check_guarantees.py            # check
    python3 scripts/docs/check_guarantees.py --self-test

Standard library only. Exit status 0 when every check holds.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[2]
PAGE = "docs/guarantees.md"
# #350 added this file. While it exists, a table with a pre-#350 status
# column ("Refusal today" / "After #350") is stale and fails.
ERRORS_PROTO = "proto/kosha/v1/errors.proto"
PRE_350_COLUMNS = ("Refusal today", "After #350")
API_ERROR = "crates/kosha-server/src/api_error.rs"

KIB = 1024
UNITS = {
    # bytes
    "b": ("bytes", 1),
    "bytes": ("bytes", 1),
    "kib": ("bytes", KIB),
    "mib": ("bytes", KIB**2),
    "gib": ("bytes", KIB**3),
    # time
    "ms": ("ms", 1),
    "s": ("ms", 1000),
    "seconds": ("ms", 1000),
}


@dataclass(frozen=True)
class Check:
    """One page row backed by one code constant."""

    label: str  # the row's first cell, verbatim
    file: str  # repo-relative source file
    pattern: str  # regex; group 1 is the value expression in the code
    unit: str  # "count", "bytes", "ms" or "s": what the code expression means
    env: str | None = None  # the KOSHA_* override, when one exists
    # Turns the matched integer into the page's value, for a number the code
    # derives from others (gets the match and the whole source file).
    derive: Callable[[int, str], int] | None = None
    # (file, regex): a second file that must match, for a value that reaches
    # the response only through code elsewhere (e.g. a default a caller uses).
    requires: tuple[str, str] | None = None


def _const(name: str, ty: str = r"\w+") -> str:
    return rf"\bconst {name}: {ty} = ([^;]+);"


def _env_default(var: str) -> str:
    # `std::env::var("VAR") ... .unwrap_or(EXPR)`, within a few lines.
    return rf'"{var}"\)(?:(?!\bfn\b)[\s\S]){{0,300}}?\.unwrap_or\(([^()]+(?:\([^()]*\))?)\)'


_IDLE_TICK = r"fn idle_flush_tick\(max_age: Duration\) -> Duration \{\s*\(max_age / (\d+)\)"


def _idle_flush_tick_ms(divisor: int, source: str) -> int:
    # `(max_age / 4).max(Duration::from_millis(50))` with the default max_age.
    max_age = eval_int_expr(re.search(_const("DEFAULT_FLUSH_MAX_AGE_MS"), source).group(1))
    floor = int(re.search(_IDLE_TICK + r"\.max\(Duration::from_millis\((\d+)\)\)", source).group(2))
    return max(max_age // divisor, floor)


_BUSY = (
    r"fn busy\(message: &str, reason: &str, request_id: &str\) -> Response \{\s*"
    r"Response::error_with_id\(\s*"
    r"ApiError::new\(ErrorCode::Unavailable, message\)\.detail\(\"reason\", reason\),\s*"
    r"request_id,\s*\)\s*\}"
)

# Only `impl Default for GroupCommitConfig`, not the tests' configs.
_WAL_DEFAULT = r"impl Default for GroupCommitConfig \{\s*fn default\(\) -> Self \{\s*Self \{\s*"

CHECKS: list[Check] = [
    # Hard limits.
    Check("Request body", "crates/kosha-server/src/auth.rs",
          _env_default("KOSHA_MAX_BODY_BYTES"), "bytes", "KOSHA_MAX_BODY_BYTES"),
    Check("Histogram buckets per aggregation", "crates/kosha-core/src/histogram.rs",
          _const("MAX_HISTOGRAM_BUCKETS"), "count"),
    Check("Server-side embedding dimensions", "crates/kosha-core/src/analysis.rs",
          _const("MAX_EMBEDDING_DIMS"), "count"),
    Check("Shards per namespace", "crates/kosha-core/src/sharding.rs",
          _const("MAX_SHARDS"), "count"),
    Check("Rerank `top_n`", "crates/kosha-core/src/lib.rs",
          _const("MAX_RERANK_TOP_N"), "count"),
    Check("Rerank `timeout_ms`", "crates/kosha-core/src/lib.rs",
          _const("MAX_RERANK_TIMEOUT_MS"), "ms"),
    Check("`rank_by` window", "crates/kosha-core/src/rank.rs",
          _const("MAX_RANK_BY_WINDOW"), "count"),
    Check("Fuzzy `max_expansions`", "crates/kosha-core/src/fuzzy.rs",
          _const("MAX_MAX_EXPANSIONS"), "count"),
    Check("Fuzzy edit distance", "crates/kosha-core/src/fuzzy.rs",
          _const("MAX_FUZZY_EDITS"), "count"),
    # Defaults that bound a response.
    Check("`max_results` (top-k) default", "crates/kosha-core/src/lib.rs",
          r"fn default_max_results\(\) -> usize \{\s*(\d+)\s*\}", "count"),
    Check("`knn.k` default", "crates/kosha-core/src/lib.rs",
          r"fn default_knn_k\(\) -> usize \{\s*(\d+)\s*\}", "count"),
    Check("`knn.num_candidates` default", "crates/kosha-core/src/lib.rs",
          r"fn default_knn_num_candidates\(\) -> usize \{\s*(\d+)\s*\}", "count"),
    Check("Total hits counted", "crates/kosha-query/src/lib.rs",
          _env_default("KOSHA_TOTAL_HITS_CAP"), "count", "KOSHA_TOTAL_HITS_CAP"),
    Check("Text value copied into filter columns", "crates/kosha-segment/src/lib.rs",
          _const("DEFAULT_TEXT_FILTER_IGNORE_ABOVE"), "count", "KOSHA_TEXT_FILTER_IGNORE_ABOVE"),
    # Per-pod capacity defaults.
    Check("Open HTTP connections", "crates/kosha-server/src/http.rs",
          r"max_connections: (\d+),", "count", "KOSHA_HTTP_MAX_CONNECTIONS"),
    Check("Requests in flight", "crates/kosha-server/src/http.rs",
          r"max_inflight: (\d+),", "count", "KOSHA_HTTP_MAX_INFLIGHT"),
    Check("Concurrent searches", "crates/kosha-server/src/main.rs",
          _env_default("KOSHA_MAX_CONCURRENT_SEARCHES"), "count", "KOSHA_MAX_CONCURRENT_SEARCHES"),
    Check("Search queue wait", "crates/kosha-server/src/main.rs",
          _env_default("KOSHA_SEARCH_QUEUE_TIMEOUT_MS"), "ms", "KOSHA_SEARCH_QUEUE_TIMEOUT_MS"),
    Check("Request body read timeout", "crates/kosha-server/src/main.rs",
          _env_default("KOSHA_HTTP_IO_TIMEOUT_SECS"), "s", "KOSHA_HTTP_IO_TIMEOUT_SECS"),
    # The page states the multiplier: "4 × permits".
    Check("Search queue depth", "crates/kosha-server/src/main.rs",
          r'"KOSHA_SEARCH_QUEUE_DEPTH"\)[\s\S]{0,300}?\.unwrap_or\(permits \* (\d+)\)',
          "count", "KOSHA_SEARCH_QUEUE_DEPTH"),
    # `busy` builds a retryable `unavailable` error and sets no delay of its
    # own, so the header carries `DEFAULT_RETRY_AFTER`. The `requires` regex
    # spans the whole of `busy`, so a `.retry_after(..)` added there fails it.
    Check("Connection and in-flight shed `Retry-After`", API_ERROR,
          r"\bconst DEFAULT_RETRY_AFTER: Duration = Duration::from_secs\((\d+)\);", "s",
          requires=("crates/kosha-server/src/http.rs", _BUSY)),
    Check("Unindexed backlog `Retry-After`", "crates/kosha-server/src/unindexed.rs",
          _const("BACKPRESSURE_RETRY_AFTER_SECS"), "s"),
    Check("Unindexed backlog per namespace", "crates/kosha-server/src/unindexed.rs",
          _const("DEFAULT_MAX_UNINDEXED_BYTES"), "bytes", "KOSHA_MAX_UNINDEXED_BYTES"),
    # Timings behind the guarantees.
    Check("Group-commit window", "crates/kosha-write/src/wal.rs",
          _WAL_DEFAULT + r"max_delay: Duration::from_millis\((\d+)\),", "ms", "KOSHA_WAL_GROUP_COMMIT_MS"),
    Check("Group-commit size cut", "crates/kosha-write/src/wal.rs",
          _WAL_DEFAULT + r"[\s\S]{0,80}?max_bytes: ([\d_ *]+),", "bytes", "KOSHA_WAL_GROUP_COMMIT_BYTES"),
    Check("S3 WAL PUT timeout", "crates/kosha-server/src/s3_wal.rs",
          r'"KOSHA_WAL_S3_TIMEOUT_MS"\)[\s\S]{0,300}?\.unwrap_or\(Duration::from_secs\((\d+)\)\)',
          "s", "KOSHA_WAL_S3_TIMEOUT_MS"),
    Check("Manifest poll interval", "crates/kosha-server/src/manifest_follow.rs",
          _const("DEFAULT_REFRESH_MS"), "ms", "KOSHA_MANIFEST_REFRESH_MS"),
    Check("Idle flush", "crates/kosha-server/src/main.rs",
          _const("DEFAULT_FLUSH_MAX_AGE_MS"), "ms", "KOSHA_FLUSH_MAX_AGE_MS"),
    Check("Size flush", "crates/kosha-server/src/main.rs",
          _env_default("KOSHA_FLUSH_THRESHOLD"), "count", "KOSHA_FLUSH_THRESHOLD"),
    Check("Idle flush check", "crates/kosha-server/src/main.rs",
          _IDLE_TICK, "ms", "KOSHA_FLUSH_MAX_AGE_MS", derive=_idle_flush_tick_ms),
]


# Columns whose cells describe a refusal or summarise a limit. A number in one
# of these would be an unchecked copy of a checked row, so only HTTP status
# codes (4xx/5xx) and issue references (#NNN) are allowed.
POINTER_COLUMNS = ("When exceeded", "Refusal", "Limit today", "Visible after")
# Of those, the columns whose cells may name an error code to check.
REFUSAL_COLUMNS = ("When exceeded", "Refusal")


def stray_numbers(cell: str) -> list[str]:
    cell = re.sub(r"#\d+", "", cell)
    cell = re.sub(r"\b[45]\d\d\b", "", cell)
    return re.findall(r"\d[\d,._]*", cell)


# ─── parsing ────────────────────────────────────────────────────────────────


def eval_int_expr(expr: str) -> int:
    """`64 * 1024 * 1024`, `10_000`, `4096`, `Duration::from_secs(10)`'s 10."""
    expr = expr.strip()
    if not re.fullmatch(r"[\d_ *]+", expr):
        raise ValueError(f"not a constant integer expression: {expr!r}")
    value = 1
    for factor in expr.split("*"):
        value *= int(factor.strip().replace("_", ""))
    return value


def code_value(check: Check, source: str) -> tuple[str, int]:
    """The constant as (base unit, value)."""
    matches = re.findall(check.pattern, source)
    if not matches:
        raise ValueError(f"pattern not found in {check.file}: {check.pattern}")
    values = {eval_int_expr(m) for m in matches}
    if len(values) != 1:
        raise ValueError(f"pattern is ambiguous in {check.file}: {sorted(values)}")
    value = values.pop()
    if check.derive is not None:
        try:
            value = check.derive(value, source)
        except (AttributeError, ValueError) as e:
            raise ValueError(f"cannot derive {check.label!r} from {check.file}: {e}") from e
    if check.unit == "s":
        return "ms", value * 1000
    if check.unit in ("bytes", "ms"):
        return check.unit, value
    return "count", value


NUMBER = re.compile(r"(\d[\d,_]*(?:\.\d+)?)\s*([A-Za-z]+)?")


def page_value(cell: str) -> tuple[str, int]:
    """The first number in a Value cell, with its unit, as (base unit, value)."""
    m = NUMBER.search(cell.replace("**", ""))
    if not m:
        raise ValueError(f"no number in value cell {cell!r}")
    number = float(m.group(1).replace(",", "").replace("_", ""))
    unit = (m.group(2) or "").lower()
    if unit in UNITS:
        base, scale = UNITS[unit]
        return base, round(number * scale)
    return "count", round(number)


def split_row(line: str) -> list[str]:
    cells = line.strip().strip("|").split("|")
    return [c.strip() for c in cells]


def tables(text: str) -> list[tuple[list[str], list[list[str]]]]:
    """Every GitHub-style table as (header, rows)."""
    out = []
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        if (
            lines[i].lstrip().startswith("|")
            and i + 1 < len(lines)
            and re.fullmatch(r"\s*\|[\s:|-]+\|\s*", lines[i + 1])
        ):
            header = split_row(lines[i])
            rows = []
            i += 2
            while i < len(lines) and lines[i].lstrip().startswith("|"):
                rows.append(split_row(lines[i]))
                i += 1
            out.append((header, rows))
        else:
            i += 1
    return out


def column(header: list[str], name: str) -> int | None:
    for idx, h in enumerate(header):
        if h.lower() == name.lower():
            return idx
    return None


def error_codes(source: str) -> dict[str, tuple[int, bool]]:
    """``{"payload_too_large": (413, False), ...}`` from api_error.rs."""

    def fn_body(name: str) -> str:
        m = re.search(rf"fn {name}\(self\)[^{{]*\{{([\s\S]*?)\n    \}}", source)
        if not m:
            raise ValueError(f"{API_ERROR}: fn {name}(self) not found")
        return m.group(1)

    names = dict(re.findall(r'Self::(\w+) => "([a-z_]+)"', fn_body("as_str")))
    statuses: dict[str, int] = {}
    for variants, status in re.findall(r"((?:\|?\s*Self::\w+\s*)+)=> (\d{3})", fn_body("status")):
        for v in re.findall(r"Self::(\w+)", variants):
            statuses[v] = int(status)
    retryable = set(re.findall(r"Self::(\w+)", fn_body("retryable")))
    if not names or set(names) != set(statuses):
        raise ValueError(f"{API_ERROR}: as_str and status do not cover the same codes")
    return {names[v]: (statuses[v], v in retryable) for v in names}


def check_refusal(label: str, column_name: str, cell: str, codes: dict[str, tuple[int, bool]]) -> list[str]:
    errors = []
    for code in re.findall(r"`([a-z]+(?:_[a-z]+)+|[a-z]+)`", cell):
        if code not in codes:
            continue
        status, retryable = codes[code]
        stated = re.findall(r"\b[45]\d\d\b", cell)
        if str(status) not in stated:
            errors.append(
                f"row {label!r}, column {column_name!r}: `{code}` is HTTP {status} "
                f"in {API_ERROR}, the cell says {stated or 'no status'}"
            )
        says_not = "not retryable" in cell
        if retryable == says_not or (retryable and "retryable" not in cell):
            want = "retryable" if retryable else "not retryable"
            errors.append(
                f"row {label!r}, column {column_name!r}: `{code}` is {want} in {API_ERROR}; say so"
            )
    return errors


# ─── checks ─────────────────────────────────────────────────────────────────


def shallow_clone(root: Path) -> bool:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--is-shallow-repository"],
            cwd=root, capture_output=True, text=True, check=True,
        ).stdout.strip()
        return out == "true"
    except (OSError, subprocess.CalledProcessError):
        return True


def commit_exists(root: Path, sha: str) -> bool:
    """True when ``sha`` is a commit reachable from HEAD.

    Object existence alone is not enough: a commit from a rebased or deleted
    local branch still exists in the object store but is absent from any
    clone CI makes.
    """
    return subprocess.run(
        ["git", "merge-base", "--is-ancestor", sha, "HEAD"],
        cwd=root, capture_output=True,
    ).returncode == 0


def check_page(root: Path, text: str, readme: str, verify_commits: bool | None = None) -> list[str]:
    errors: list[str] = []
    notes: list[str] = []
    by_label = {c.label: c for c in CHECKS}
    seen: set[str] = set()
    measured_rows = 0

    if re.search(r"\b(TBD|TODO)\b", text):
        errors.append(f"{PAGE}: contains TBD/TODO")
    if "docs/guarantees.md" not in readme:
        errors.append("README.md: no link to docs/guarantees.md")
    if (root / ERRORS_PROTO).is_file():
        for header, _ in tables(text):
            stale = [h for h in header if h in PRE_350_COLUMNS]
            if stale:
                errors.append(
                    f"{PAGE}: {ERRORS_PROTO} exists, so #350's error model has landed, but a "
                    f"table still has the pre-#350 status column(s) {stale}: rewrite them"
                )

    try:
        codes = error_codes((root / API_ERROR).read_text())
    except (OSError, ValueError) as e:
        errors.append(f"cannot read error codes: {e}")
        codes = {}

    if verify_commits is None:
        verify_commits = not shallow_clone(root)
        if not verify_commits:
            notes.append("shallow clone: commit SHAs not verified")

    for header, rows in tables(text):
        src_col = column(header, "Source")
        val_col = column(header, "Value")
        res_col = column(header, "Result file")
        pointer_cols = [i for i in (column(header, n) for n in POINTER_COLUMNS) if i is not None]
        refusal_cols = [i for i in (column(header, n) for n in REFUSAL_COLUMNS) if i is not None]
        for row in rows:
            label = row[0]
            for i in refusal_cols:
                if i < len(row):
                    errors.extend(check_refusal(label, header[i], row[i], codes))
            for i in pointer_cols:
                if i < len(row) and stray_numbers(row[i]):
                    errors.append(
                        f"row {label!r}, column {header[i]!r}: states {stray_numbers(row[i])}; "
                        "point at the checked row instead of restating a number"
                    )
            if src_col is not None and src_col < len(row) and "crates/" in row[src_col]:
                if val_col is None:
                    errors.append(f"table row {label!r}: Source cites code but the table has no Value column")
                    continue
                check = by_label.get(label)
                if check is None:
                    errors.append(
                        f"row {label!r} cites code but has no entry in CHECKS "
                        "(scripts/docs/check_guarantees.py): every code number must be checked"
                    )
                    continue
                seen.add(label)
                src = row[src_col]
                if check.file not in src:
                    errors.append(f"row {label!r}: Source must name {check.file}")
                if check.env and check.env not in src:
                    errors.append(f"row {label!r}: Source must name the override {check.env}")
                try:
                    if check.requires is not None:
                        req_file, req_pattern = check.requires
                        if not re.search(req_pattern, (root / req_file).read_text()):
                            raise ValueError(
                                f"{req_file} no longer matches what this value relies on: {req_pattern}"
                            )
                    code = code_value(check, (root / check.file).read_text())
                    page = page_value(row[val_col])
                except (OSError, ValueError) as e:
                    errors.append(f"row {label!r}: {e}")
                    continue
                if code != page:
                    errors.append(
                        f"row {label!r}: page says {page[1]} {page[0]}, "
                        f"{check.file} says {code[1]} {code[0]}"
                    )
            if res_col is not None and res_col < len(row):
                measured_rows += 1
                errors.extend(check_measurement(root, label, row, row[res_col], verify_commits))

    for label in by_label:
        if label not in seen:
            errors.append(f"CHECKS entry {label!r} has no row on {PAGE}")
    if measured_rows == 0:
        errors.append(f"{PAGE}: no table with a 'Result file' column")
    for n in notes:
        print(f"note: {n}")
    return errors


def check_measurement(root: Path, label: str, row: list[str], cell: str, verify_commits: bool) -> list[str]:
    errors = []
    whole = " ".join(row)
    paths = re.findall(r"`([\w./-]+\.(?:md|json))`", cell)
    if not paths:
        return [f"measured row {label!r}: Result file cell names no .md/.json file"]
    contents = ""
    for p in paths:
        f = root / p
        if not f.is_file():
            errors.append(f"measured row {label!r}: {p} does not exist")
        else:
            contents += f.read_text()
    if errors:
        return errors
    for run_id in re.findall(r"\b\d{11}\b", whole):
        if run_id not in contents:
            errors.append(f"measured row {label!r}: run {run_id} is not in {', '.join(paths)}")
    dates = re.findall(r"\b20\d\d-\d\d-\d\d\b", whole)
    if not dates:
        errors.append(f"measured row {label!r}: no date (YYYY-MM-DD)")
    for d in dates:
        if d not in contents:
            errors.append(f"measured row {label!r}: date {d} is not in {', '.join(paths)}")
    shas = re.findall(r"commit `([0-9a-f]{7,40})`", whole)
    if not shas:
        errors.append(f"measured row {label!r}: no commit (write it as commit `<sha>`)")
    if verify_commits:
        for sha in shas:
            if not commit_exists(root, sha):
                errors.append(f"measured row {label!r}: commit {sha} is not in git history")
    return errors


# ─── self-test ──────────────────────────────────────────────────────────────


def self_test() -> None:
    assert eval_int_expr("64 * 1024 * 1024") == 64 * 1024 * 1024
    assert eval_int_expr("10_000") == 10000
    assert page_value("**64 MiB** (67,108,864 bytes)") == ("bytes", 64 * 1024 * 1024)
    assert page_value("500 ms") == ("ms", 500)
    assert page_value("10 s") == ("ms", 10000)
    assert page_value("10,000 buckets") == ("count", 10000)
    assert page_value("2 GiB") == ("bytes", 2 * KIB**3)
    src = 'let x = std::env::var("KOSHA_FOO")\n    .ok()\n    .unwrap_or(64 * 1024);\n'
    assert code_value(Check("x", "f", _env_default("KOSHA_FOO"), "bytes"), src) == ("bytes", 65536)
    assert code_value(Check("x", "f", _const("MAX_X"), "count"), "pub const MAX_X: u32 = 4_096;") == ("count", 4096)
    t = tables("| a | Value |\n| --- | --- |\n| r | 1 |\n\ntext\n")
    assert t == [(["a", "Value"], [["r", "1"]])], t

    # The real page passes, and a changed number or a missing results file fails.
    text = (ROOT / PAGE).read_text()
    readme = (ROOT / "README.md").read_text()
    assert check_page(ROOT, text, readme, verify_commits=False) == []
    broken = text.replace("| Request body | 64 MiB", "| Request body | 32 MiB", 1)
    assert broken != text, "self-test anchor missing: the Request body row"
    errs = check_page(ROOT, broken, readme, verify_commits=False)
    assert any("Request body" in e for e in errs), errs
    broken = text.replace("2026-11-22-wal-group-commit.md", "2026-11-22-missing.md")
    errs = check_page(ROOT, broken, readme, verify_commits=False)
    assert any("does not exist" in e for e in errs), errs
    for old, new in [
        ("| Connection and in-flight shed `Retry-After` | 1 s", "| Connection and in-flight shed `Retry-After` | 2 s"),
        ("| Search queue depth | 4 ×", "| Search queue depth | 8 ×"),
        ("| Unindexed backlog `Retry-After` | 1 s", "| Unindexed backlog `Retry-After` | 2 s"),
    ]:
        broken = text.replace(old, new, 1)
        assert broken != text, f"self-test anchor missing: {old}"
        assert check_page(ROOT, broken, readme, verify_commits=False), old
    broken = text.replace("| Request size | See", "| Request size | 64 MiB. See", 1)
    assert broken != text, "self-test anchor missing: the Request size row"
    errs = check_page(ROOT, broken, readme, verify_commits=False)
    assert any("Request size" in e and "restating" in e for e in errs), errs
    broken = text.replace("| 503 `unavailable`, retryable, with `Retry-After` (below) |",
                          "| 503 `unavailable`, retryable, `Retry-After: 1` |", 1)
    assert broken != text, "self-test anchor missing: Requests in flight"
    assert any("Retry-After" in e or "Requests in flight" in e
               for e in check_page(ROOT, broken, readme, verify_commits=False))
    # Refusal cells are checked against api_error.rs.
    for old, new in [
        ("| 403 `doc_quota_exceeded`, not retryable", "| 429 `doc_quota_exceeded`, not retryable"),
        ("| 403 `doc_quota_exceeded`, not retryable", "| 403 `doc_quota_exceeded`, retryable"),
        ("| 429 `tenant_qps_exceeded`, retryable", "| 429 `tenant_qps_exceeded`, not retryable"),
        ("| 413 `payload_too_large`, not retryable, no", "| 429 `payload_too_large`, not retryable, no"),
        ("| 408 `request_timeout`, retryable", "| 408 `request_timeout`"),
    ]:
        broken = text.replace(old, new, 1)
        assert broken != text, f"self-test anchor missing: {old}"
        errs = check_page(ROOT, broken, readme, verify_commits=False)
        assert any("api_error.rs" in e for e in errs), (old, new, errs)
    codes = error_codes((ROOT / API_ERROR).read_text())
    assert codes["payload_too_large"] == (413, False), codes
    assert codes["unavailable"] == (503, True), codes
    assert codes["doc_quota_exceeded"] == (403, False), codes
    # `busy` setting its own delay would bypass DEFAULT_RETRY_AFTER.
    busy_src = (ROOT / "crates/kosha-server/src/http.rs").read_text()
    assert re.search(_BUSY, busy_src), "self-test: _BUSY does not match http.rs"
    assert not re.search(_BUSY, busy_src.replace(
        '.detail("reason", reason),', '.detail("reason", reason).retry_after(d),', 1))
    assert stray_numbers("429 `doc_quota_exceeded`, 403 after #350") == []
    assert stray_numbers("retry 60 s") == ["60"]
    errs = check_page(ROOT, text, readme.replace("docs/guarantees.md", "x"), verify_commits=False)
    assert any("README" in e for e in errs), errs
    tick = Check("t", "f", _IDLE_TICK, "ms", derive=_idle_flush_tick_ms)
    tick_src = (
        "const DEFAULT_FLUSH_MAX_AGE_MS: u64 = {};\n"
        "fn idle_flush_tick(max_age: Duration) -> Duration {{\n"
        "    (max_age / 4).max(Duration::from_millis(50))\n}}"
    )
    assert code_value(tick, tick_src.format("1000")) == ("ms", 250)
    assert code_value(tick, tick_src.format("100")) == ("ms", 50)

    # Once #350's errors.proto exists, the pre-#350 status column fails.
    with tempfile.TemporaryDirectory() as tmp:
        fake = Path(tmp)
        (fake / ERRORS_PROTO).parent.mkdir(parents=True)
        (fake / ERRORS_PROTO).write_text("")
        errs = check_page(fake, "| Refusal today |\n| --- |\n| x |\n", readme, verify_commits=False)
        assert any("#350" in e for e in errs), errs
        errs = check_page(fake, "| Refusal |\n| --- |\n| x |\n", readme, verify_commits=False)
        assert not any("#350" in e for e in errs), errs
    print("self-test ok")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        self_test()
        return 0
    errors = check_page(ROOT, (ROOT / PAGE).read_text(), (ROOT / "README.md").read_text())
    for e in errors:
        print(f"error: {e}", file=sys.stderr)
    if errors:
        print(f"{PAGE}: {len(errors)} problem(s)", file=sys.stderr)
        return 1
    print(f"{PAGE}: {len(CHECKS)} code constants and every cited result checked")
    return 0


if __name__ == "__main__":
    sys.exit(main())
