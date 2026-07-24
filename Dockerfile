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

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json \
    --package kosha-server --features postgres,s3

COPY . .
RUN cargo build --release --locked --package kosha-server --features postgres,s3

# ---------- Runtime ----------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 kosha

COPY --from=builder /app/target/release/kosha-server /usr/local/bin/kosha-server

USER kosha
# 8080 = HTTP/JSON, 50051 = gRPC.
EXPOSE 8080 50051
ENTRYPOINT ["kosha-server"]
