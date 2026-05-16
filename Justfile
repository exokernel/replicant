# Default: show available recipes
default:
    @just --list

# Format all crates in place
fmt:
    cargo fmt --all

# Quick compile check without producing binaries (faster than lint)
check:
    cargo check --all-targets

# Run clippy across all targets with warnings as errors
lint:
    cargo clippy --all-targets -- -D warnings

# Run unit tests
test:
    cargo test --all

# Run the orchestrator end-to-end smoke test
smoke:
    cargo run --bin orchestrator

# Build the replica image, run 5 replicas via docker-compose, run the
# full-mesh-n5 scenario against them, then tear the stack down. Verifies
# the Dockerfile + compose wiring + orchestrator --replicas path end-to-end.
# Not part of `just ci` because it requires a docker daemon and a network pull.
smoke-docker:
    docker compose up -d --build
    cargo run --release --bin orchestrator -- \
        --replicas localhost:50051=replica-0:50051,localhost:50052=replica-1:50051,localhost:50053=replica-2:50051,localhost:50054=replica-3:50051,localhost:50055=replica-4:50051 \
        scenarios/full-mesh-n5.toml \
        ; docker compose down

# Build rustdoc for all crates and open in browser
docs:
    cargo doc --workspace --no-deps --open

# Run a scenario file through the orchestrator
bench scenario:
    cargo run --bin orchestrator -- {{scenario}}

# Full CI gate: format check → lint → test → smoke
ci:
    cargo fmt --all --check
    just lint
    just test
    just smoke
