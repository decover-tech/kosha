# WAL group-commit ack latency, local mode (2026-11-22)

- **What:** `cargo bench -p kosha-write --bench wal_group_commit -- --list`.
  The `--list` flag skips the criterion samples. The table below is the
  bench's own `report()` pass, with 800 appends per configuration.
- **Commit:** `e174058` (main, the PR #426 merge), with no local changes.
- **Date:** 2026-11-22.
- **Machine:** Apple M5 laptop, macOS (Darwin 25.6.0), internal SSD, APFS,
  `rustc 1.100.0`. The WAL calls `File::sync_all` on the file and its
  directory. On macOS that issues `F_FULLFSYNC`, which flushes the drive
  cache and is slower than `fsync` on a Linux EBS volume. **This is a
  developer-machine number, not a production measurement.** It shows how
  group commit behaves (one fsync per ack, shared by concurrent writers). It
  does not show the latency a production pod sees.
- **Document:** about 0.9 kB of JSON (a 352-character text field, a keyword
  and a 128-dim vector). Each append writes one document.

```
writers delay_ms appends appends/s p50_ms p99_ms files/append
      1        0     800       129   7.98   8.81        1.000
      1        2     800       128   7.99   8.22        1.000
     16        0     800      1044  15.83  21.38        0.125
     16        2     800      1508  10.93  14.95        0.064
```

`delay_ms` is `KOSHA_WAL_GROUP_COMMIT_MS` (default 2). `files/append` is the
number of fsync'd WAL files per acknowledged append. A value of 1.0 means
every append paid for its own fsync. A value of 0.064 means about 16 appends
shared one.
