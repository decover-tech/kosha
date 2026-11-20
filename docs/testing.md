# Write-path durability testing

How the write path (WAL, flush, publish, compaction, deletes) is tested for crashes,
races and corrupt input (#257). Everything here except fuzzing runs in
`cargo test --all-features`, which CI runs on every PR; a nightly workflow
(`.github/workflows/fuzz.yml`) runs the same suites much longer.

## Failpoints

`kosha-write` and `kosha-server` have a `failpoints` cargo feature that compiles in named
[`fail`](https://docs.rs/fail) points. Without the feature the `fail_point!` macros expand to
nothing; with it, a point does nothing until a test configures it. Names are internal and
may change.

| Point | Where |
| --- | --- |
| `wal.append.before_fsync` | WAL group commit, before the group's durable write (no caller acked) |
| `wal.append.after_fsync` | WAL group commit, after the durable write, before any caller is acked |
| `flush.segment_written` | Flush: segment files on disk, not yet in the manifest |
| `flush.before_upload` | Publish: before the segment upload |
| `flush.after_upload_before_commit_marker` | Segment upload: data files uploaded, `_COMMIT` not yet written |
| `publish.before_cas` | Publish: upload done, before the manifest compare-and-swap |
| `publish.after_cas_before_ack` | Publish: manifest committed, writer not yet acknowledged |
| `wal.truncate.before` | After a committed publish, before the WAL records it covers are reclaimed |
| `compaction.output_written` | Compaction: merged segment on disk, inputs still in the manifest |
| `compaction.before_publish` | Server compaction pass: merged locally, before its publish |
| `delete.persist.before` | Delete by query: matches computed, nothing durable or applied |
| `upsert.segment_written` | Upsert: replacement segment on disk, old copies not yet deleted |

Scheduling-only points used by the interleaving tests: `flush.leader.pass`,
`flush.piggyback.registered`, `flush.snapshot_taken`, `compaction.planned`,
`upsert.before_io`.

## Crash-point matrix

`crates/kosha-server/src/crash_points_tests.rs` (in the binary crate, because `AppState`
and the handlers are private to it) has one test per durability failpoint. Each runs a
workload (index, flush, delete, upsert, a delete of buffered documents, compaction) through
the real handlers against a temp data dir, an in-memory control plane and an in-memory
object bucket (segment files first, `_COMMIT` last, the same order as S3). A dry run proves
the point is reached; then the workload crashes (a panic) at the point's first, middle and
last hit. The node is dropped without destructors, as in a process death, then reopened on
the same dir, control plane and bucket, and after WAL replay and a flush the test asserts:

- every acknowledged write is present with its latest acknowledged value, and every
  acknowledged delete is absent (the interrupted operation may or may not have landed);
- no document is live twice;
- every segment of the published manifest has a `_COMMIT` in the bucket, and every file it
  lists is there at its size; deletion vectors are uploaded.

Points are armed for the test's own thread only, so the rest of the server's tests, which
share the process, never see them.

    cargo test -p kosha-server --all-features crash_points_tests

## Interleavings

`crates/kosha-write/tests/interleavings.rs` (needs `--features failpoints`) runs flush
coalescing (leader and piggybacks), compaction vs delete, compaction vs upsert, and all
three at once under randomized schedules: every worker parks at each scheduling point and a
seeded scheduler picks who runs next. `shuttle`/`loom` were not used because `Indexer` uses
`std::sync` directly around real file I/O; the failpoint seam needs no production change.
1,000 schedules run by default; `KOSHA_INTERLEAVING_SCHEDULES` sets the count per scenario.
A failure prints the seed and the decision trace.

    KOSHA_INTERLEAVING_SCHEDULES=2500 cargo test -p kosha-write --features failpoints --test interleavings

## Property tests

`proptest` suites, 64 cases per property by default (`PROPTEST_CASES` raises it):

- `crates/kosha-write/tests/wal_proptest.rs`: WAL records round-trip; a mutated or
  truncated WAL file decodes to an error or a prefix of what was written, never a panic.
- `crates/kosha-segment/tests/decoder_proptest.rs`: random documents round-trip through a
  segment; footer JSON, filter columns and late-interaction bags round-trip; random
  mutation or truncation of any file of a valid segment, of the vector index, or of the
  bag/IVF files never panics the reader. Set `KOSHA_FILTERS_WRITE_VERSION=2` to cover the
  columnar filter format too.

Pinned regressions for #233 (WAL isolation across namespaces, checksum coverage) and #238
(delete by query and buffered documents) are in `crates/kosha-write/tests/regressions.rs`.

## Fuzzing

`fuzz/` is a `cargo-fuzz` crate with its own workspace. Targets: `wal_decode`,
`segment_open` (input written over one file of a valid segment), `footer_json`,
`filter_column_decode`, `vector_idx_parse` and `multivec_bag`.

    rustup toolchain install nightly && cargo install cargo-fuzz
    cd fuzz && cargo +nightly fuzz run wal_decode -- -max_total_time=60

Without nightly, `cargo check --manifest-path fuzz/Cargo.toml` still type-checks the
targets. The nightly workflow fuzzes each target for two minutes, keeps the corpus in the
Actions cache and uploads any crash as an artifact. Reproduce one with
`cargo +nightly fuzz run <target> <artifact>`; a decoder panic is a bug: it must return
`KoshaError::CorruptSegment` (or the WAL's `InvalidData`) instead, with a regression test.
