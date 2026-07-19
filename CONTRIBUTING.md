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
   formatting/lint gates on every commit.

## Before opening a PR

All of these are CI gates — run them locally first:

    cargo fmt --all -- --check
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --all-features
    docker build -t kosha:ci .   # if you touched the Dockerfile or build config

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
