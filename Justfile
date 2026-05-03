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

# Build rustdoc for all crates and open in browser
docs:
    cargo doc --workspace --no-deps --open

# Full CI gate: format check → lint → test → smoke
ci:
    cargo fmt --all --check
    just lint
    just test
    just smoke
