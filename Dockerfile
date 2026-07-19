# syntax=docker/dockerfile:1

# ---------- Build stage ----------
FROM rust:1.90-slim-bookworm AS builder
WORKDIR /app

# protoc will be needed by tonic-build once proto/ lands (Epic 1). Installing
# it now keeps that later change additive.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release --locked --package kosha-server

# ---------- Runtime stage ----------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 kosha

COPY --from=builder /app/target/release/kosha-server /usr/local/bin/kosha-server

USER kosha
# 8080 = HTTP/JSON (health-only until Epic 8), 50051 = gRPC (Epic 8).
EXPOSE 8080 50051
ENTRYPOINT ["kosha-server"]
