## What and why

<!-- What does this PR change, and why? Link the issue: "Closes #123". -->

## How it was tested

<!-- New or changed tests, and anything you ran by hand. For performance changes, the bench
command, the machine and before/after numbers (see CONTRIBUTING.md#benchmarks). -->

## Compatibility

Does this change any of the following? If yes, explain how existing data and clients keep
working (version gates, readers for the old format, migration steps, rollback).

- [ ] The on-disk segment format (files under a segment, footer, posting/vector/filter layout)
- [ ] The manifest, WAL, or Postgres schema
- [ ] The `/v1` wire contract (`proto/kosha/v1/kosha.proto`, request/response JSON)
- [ ] None of the above

## Checklist

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --all-targets --all-features -- -D warnings`
- [ ] `cargo test --all-features`
- [ ] `make client-python-test` (if `clients/python` changed)
- [ ] `docker build -t kosha:ci .` (if the Dockerfile or build configuration changed)
- [ ] New `KOSHA_*` variables have a row in `docs/configuration.md`
- [ ] README / docs updated for user-visible changes
