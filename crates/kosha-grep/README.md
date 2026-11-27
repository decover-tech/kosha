# kosha-grep: indexed grep research prototype (#588)

Exact, ripgrep-compatible regex search over raw text, accelerated by a
non-positional trigram index ([Russ Cox, 2012](https://swtch.com/~rsc/regexp/regexp4.html)).
This is a standalone research crate: nothing in the server, segment format or
`/v1` API uses it yet. Its job is to answer, with numbers, whether `grep()`
belongs in Kosha and how to build it.

## Design

- **Index** (`TrigramIndex`): every trigram of each document's raw bytes,
  ASCII-lowercased and never spanning `\n`, mapped to a roaring bitmap of
  document ids. Non-ASCII bytes are kept as they are.
- **Planner** (`plan`): walks the regex HIR (`regex-syntax`, the parser
  ripgrep uses) and derives an AND/OR trigram query every matching line must
  satisfy. Small exact sets (literals, small classes, `x?`, alternations) are
  expanded and crossed so trigrams spanning node boundaries are kept;
  anything unbounded (`.`, `\w`, `*`, large classes) cuts the run. Unicode
  case folding comes from the HIR (`(?i)k` also plans the Kelvin sign).
- **Verification** (`Grep::run`): candidates (minus tombstones) are fetched
  in id order and every line is checked with `regex::bytes`, the engine
  ripgrep uses by default. The index only decides what is read, never what
  matches, so results are exact.
- **Fallback**: a pattern with no required trigram plans to
  `TrigramQuery::All` and scans everything, bounded by `GrepOptions`
  `max_bytes`, `max_time` and `max_matches`; hitting one sets
  `GrepOutcome::truncated`. Because documents are read in id order, a
  truncated result is a deterministic prefix of the full one.
- **Compaction** (`TrigramIndex::merge`): merges segment indexes, drops
  deleted documents and renumbers densely; identical to rebuilding.
- **Blocks**: the corpus can be indexed as blocks of whole lines of about N
  KiB, so a candidate is one ranged read of a block instead of a whole
  object. This is the main lever on bytes read (below).

Semantics match `rg --text --hidden --no-ignore --encoding none`: line
numbering, `^`/`$` per line, CRLF (`\r` stays part of the line), invalid
UTF-8, NUL bytes, BOMs, empty files and files without a final newline. A
pattern with a literal `\n` is rejected as ripgrep rejects it without `-U`.
Backreferences and lookaround are rejected, as by ripgrep's default engine;
supporting them (`rg -P`) would need a PCRE2 verifier and plans that ignore
those nodes.

## Results

`cargo run --release -p kosha-grep --example bench -- DIR --drop-caches --block-kib N`
on the local cargo registry sources (27,436 files, 477.5 MiB of Rust, C, docs
and data), 8 vCPU VM, ripgrep 15.2.0, 10 warm and 3 cold runs per pattern
(p99 of 10 samples is close to the max). Warm: index and documents in memory.
Cold: page cache dropped, index file loaded, candidates read from local disk.
kosha-grep verifies on one thread; rg uses all 8.

| Target (#588) | Whole files | 64 KiB blocks | 4 KiB blocks |
|---|---|---|---|
| 100% agreement with `rg` | 15/15 | 15/15 | 15/15 |
| Non-positional index ≤ 30% of raw | 16.4% | 18.7% | 30.6% |
| Positional index (estimated, for comparison) | 158.6% | 160.2% | 168.3% |
| ≥ 100× fewer bytes for selective patterns | 1 of 12 | 3 of 12 | 6 of 12 |
| Warm p50 < 50 ms at 100 GB | not measured | not measured | not measured |

Per pattern, 4 KiB blocks:

| Pattern | Trigrams | Candidates | Bytes read (MiB) | Fewer bytes | Matches | = rg | Warm p50 / p99 ms | Cold p50 / p99 ms | rg warm p50 ms | rg cold p50 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| Pattern | Trigrams | Candidates | Bytes read (MiB) | Fewer bytes | Matches | = rg | Warm p50 / p99 ms | Cold p50 / p99 ms | rg warm p50 ms | rg cold p50 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| `unsafe impl Send for` | 18 | 186 (0.14%) | 0.7 | 732× | 146 | yes | 0.7 / 0.9 | 467.1 / 645.1 | 73.6 | 244.5 |
| `#\[derive\(Debug, Clone, Copy, PartialEq, Eq, Hash\)\]` | 45 | 105 (0.08%) | 0.4 | 1215× | 30 | yes | 0.6 / 0.7 | 454.3 / 632.5 | 69.5 | 232.9 |
| `serde_json::from_str` | 18 | 238 (0.18%) | 1.1 | 440× | 436 | yes | 0.9 / 1.1 | 465.0 / 483.6 | 70.3 | 216.9 |
| `AtomicUsize::new\(0\)` | 17 | 258 (0.20%) | 0.9 | 512× | 412 | yes | 1.0 / 1.2 | 684.0 / 700.2 | 77.4 | 227.2 |
| `\bunreachable_unchecked\b` | 19 | 102 (0.08%) | 21.5 | 22× | 102 | yes | 7.0 / 8.3 | 486.1 / 516.9 | 99.2 | 246.0 |
| `MAX_[A-Z_]+_SIZE` | 5 | 3130 (2.38%) | 33.0 | 14× | 1445 | yes | 20.3 / 32.4 | 679.8 / 729.8 | 103.1 | 293.4 |
| `CVE-\d{4}-\d+` | 2 | 41 (0.03%) | 17.2 | 28× | 45 | yes | 6.8 / 7.3 | 527.6 / 574.9 | 111.6 | 322.1 |
| `(?i)copyright \(c\) 20\d\d` | 14 | 909 (0.69%) | 2.2 | 213× | 1012 | yes | 2.8 / 3.5 | 508.4 / 519.4 | 130.3 | 334.6 |
| `fn (main\|run)\(\)` | 13 | 2248 (1.71%) | 6.6 | 73× | 3212 | yes | 8.6 / 9.2 | 542.7 / 568.3 | 91.9 | 279.7 |
| `TODO\|FIXME\|XXX` | 6 | 3200 (2.43%) | 32.7 | 15× | 3409 | yes | 19.1 / 21.5 | 593.6 / 603.2 | 76.5 | 267.9 |
| `colou?r` | 7 | 2651 (2.02%) | 31.1 | 15× | 2680 | yes | 17.7 / 18.9 | 668.4 / 1369.5 | 87.5 | 278.5 |
| `this string appears nowhere zqx` | 29 | 0 (0.00%) | 0.0 | all | 0 | yes | 0.0 / 0.2 | 465.9 / 494.6 | 85.1 | 239.4 |
| `\d{4}-\d{2}-\d{2}` | 0 | 131555 (100.00%) | 477.5 | 1× | 9814 | yes | 807.1 / 832.9 | 2848.6 / 2952.4 | 98.2 | 290.9 |
| `\w+\s*=\s*\d{6,}` | 0 | 131555 (100.00%) | 477.5 | 1× | 67267 | yes | 772.4 / 1669.1 | 3477.7 / 4189.9 | 150.1 | 348.7 |
| `[A-Z]{2}\d{3}` | 0 | 131555 (100.00%) | 477.5 | 1× | 55026 | yes | 1939.1 / 2079.3 | 4723.7 / 4859.9 | 211.2 | 466.4 |

Fallback budget (`max_bytes` = 64 MiB): `\w+`, `^\s*$` and `.` stop after
64 MiB with `truncated = Bytes` and return a subset of ripgrep's matches.

`tests/ripgrep.rs` also checks 35 edge-case patterns against `rg` at whole
file, 1, 8 and 64 byte blocks, through both in-memory and ranged disk reads.

## Findings

1. **Correctness holds.** Every pattern agreed with ripgrep exactly in every
   configuration; the index only prunes.
2. **Granularity decides bytes read, not the candidate rate.** With whole
   files, selective patterns already cut candidates to 0.1–7% of documents,
   but those are the largest files, so bytes fell only 2–100×. At 4 KiB
   blocks literal-heavy patterns read 73–1215× fewer bytes. Patterns whose
   only trigrams are common (`CVE-\d{4}`: `cve`, `ve-`;
   `\bunreachable_unchecked\b` hits a few huge single-line files) stay at
   14–28×.
3. **Overhead vs granularity is the real trade-off.** The non-positional
   index costs 16% of raw text per file and 31% at 4 KiB blocks, right at the
   30% target; 16 KiB blocks are the likely sweet spot. A positional
   (Zoekt-style) index costs about 160% here and is not needed for exact
   results, since verification reads the candidate anyway.
4. **Unselective patterns are a scan.** 3 of 15 patterns had no required
   trigram and read all 477.5 MiB, single-threaded 0.8–1.9 s against
   rg's 0.1–0.2 s with 8 threads. In Kosha this must be bounded (budgets work
   as designed) and, when allowed, parallelised across segments.
5. **Cold latency is index loading.** Cold p50 is ~340–690 ms, and ~350–460
   ms of that is reading and deserialising the whole index file (the
   zero-candidate pattern costs the same). A segment format must store
   posting lists sorted by trigram with an offset table so a query reads
   only its trigrams' lists with ranged reads.
6. **The 100 GB warm p50 target is not measured.** At 0.5 GB, warm p50 for
   selective patterns is 0.6–20 ms with 4 KiB blocks; posting-list work
   grows with corpus size, so this needs a run on a real 100 GB corpus.

## Recommendation for productising

Store a per-segment, non-positional trigram index over blocks of ~16 KiB of
raw stored text, with posting lists addressable by ranged read; verify
candidates in parallel across segments with explicit byte/time budgets
surfaced in the API response; merge in compaction with `merge`'s renumbering.
Run the 100 GB benchmark before committing to the `/v1` `grep` contract.
