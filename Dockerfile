FROM rust:1.88-slim AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release -p app

FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates wget \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/app /usr/local/bin/aether-indexer

EXPOSE 8090

ENTRYPOINT ["/usr/local/bin/aether-indexer"]
