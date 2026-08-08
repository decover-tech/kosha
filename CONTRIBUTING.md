# Contributing to Kosha

Thanks for your interest! Kosha is in early development — the crate layout is
a skeleton and most functionality lands with the implementation plan epics
(see the README status note and DESIGN.md).

## Development setup

1. Install Rust stable via [rustup](https://rustup.rs). The repo pins the
   toolchain channel and components in `rust-toolchain.toml`; rustup picks it
   up automatically.
2. Install Docker (for the local MinIO S3 stand-in and image builds).
3. (Recommended) `pip install pre-commit && pre-commit install` to run the
   formatting/lint gates on every commit. The config enables both the
   `pre-commit` and `prepare-commit-msg` hook types (see
   `default_install_hook_types` in `.pre-commit-config.yaml`), so the
   `kosha-bench-commit-msg` hook also wires up — it appends an idempotent
   `---kosha-bench---` section with the current cold + warm latency from the
   `segment_memory` microbench to each commit message before it is sealed
   (see `scripts/commit_bench_section.sh`). The same section is appended in
   CI by `.github/workflows/pre-commit.yml` (see below). A one-time
   `pre-commit install --hook-type prepare-commit-msg` is enough on clones
   that pre-date this option; failures never block the commit.

## Before opening a PR

All of these are CI gates — run them locally first:

    cargo fmt --all -- --check
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --all-features
    make client-python-test      # if you touched clients/python
    docker build -t kosha:ci .   # if you touched the Dockerfile or build config

The `pre-commit` workflow (`.github/workflows/pre-commit.yml`, fires on
`pull_request`) re-runs the `segment_memory` microbench in CI, amends the PR
head commit with the same `---kosha-bench---` latency section the local
hook produces, and force-pushes back to the PR branch. The append is
idempotent (the workflow no-ops if the markers are already on HEAD), so the
synchronize re-trigger from its own push terminates after one round.

## Conventions

- Unsafe code is forbidden workspace-wide (`[workspace.lints]`).
- Keep crates aligned with the architecture boundaries in DESIGN.md — shared
  types go in `kosha-core`; no cross-crate reach-arounds.
- Phase 1 scope is BM25 lexical search only. Do not add vector/ANN code yet;
  the segment format reserves the slot (DESIGN.md §3.1).

## License

Contributions are licensed under the terms of the LICENSE file in this
repository. The final open-source license will be confirmed before the first
public release (implementation plan step 126).
