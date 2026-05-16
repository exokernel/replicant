# Multi-stage build for the `replica` binary.
#
# Builder stage: Debian-slim Rust image with protoc (tonic-prost-build invokes
# it during `common`'s build.rs to compile proto/replicant.proto).
# Runtime stage: distroless/cc — glibc + ca-certs only, no shell. Final image
# is ~25 MB; debug via `docker logs <container>`.

# ── Builder ────────────────────────────────────────────────────────────────
FROM rust:1-slim-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the whole workspace (Cargo.lock pins all transitive deps). Could be
# split into deps-prefetch + sources for better layer caching once cold-build
# pain shows up; not preemptively wired in.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY proto ./proto

RUN cargo build --release --bin replica

# ── Runtime ────────────────────────────────────────────────────────────────
FROM gcr.io/distroless/cc-debian12

COPY --from=builder /build/target/release/replica /replica

EXPOSE 50051

ENTRYPOINT ["/replica"]
