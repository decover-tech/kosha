# syntax=docker/dockerfile:1

# cargo-chef splits dependency compilation from app compilation so GHA/buildx
# layer cache stays hot across source-only changes.

# ---------- Planner (recipe) ----------
FROM lukemathwalker/cargo-chef:0.1.77-rust-1.97.1-slim-bookworm AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---------- Build ----------
FROM chef AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Optional RUSTFLAGS for experiments (e.g. --build-arg
# KOSHA_RUSTFLAGS="-C target-cpu=x86-64-v3"); the benchmark workflow sets it
# from its rustflags input. Empty (the default) leaves RUSTFLAGS unset, so
# the build is the same as without the arg. Applied to the dependency cook
# too, so the cooked layer matches the final build flags.
ARG KOSHA_RUSTFLAGS=""

COPY --from=planner /app/recipe.json recipe.json
RUN if [ -n "$KOSHA_RUSTFLAGS" ]; then export RUSTFLAGS="$KOSHA_RUSTFLAGS"; fi \
    && cargo chef cook --release --locked --recipe-path recipe.json \
    --package kosha-server --features migrate

COPY . .
RUN if [ -n "$KOSHA_RUSTFLAGS" ]; then export RUSTFLAGS="$KOSHA_RUSTFLAGS"; fi \
    && cargo build --release --locked --package kosha-server --features migrate

# ---------- Runtime ----------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 kosha \
    # Pre-create the default KOSHA_DATA_DIR owned by the runtime user: a
    # named volume mounted here (docker-compose.yml) copies this ownership,
    # whereas a missing dir makes Docker create the volume root-owned and
    # the first flush fails with EACCES.
    && mkdir -p /var/lib/kosha/data \
    && chown -R kosha /var/lib/kosha

COPY --from=builder /app/target/release/kosha-server /usr/local/bin/kosha-server

USER kosha
# 8080 = HTTP/JSON, 50051 = gRPC.
EXPOSE 8080 50051
ENTRYPOINT ["kosha-server"]
