# Default: show available recipes
default:
    @just --list

# Format all crates in place
fmt:
    cargo fmt --all

# Strip outputs from all notebooks in place (jj does not honor the
# .gitattributes nbstripout clean filter on snapshot, so we run it
# explicitly). Run before `jj describe` / `jj git push` on any change
# that re-executed a notebook. `jj fix` also runs nbstripout — this
# recipe is the manual escape hatch.
clean-notebooks:
    find analysis -name '*.ipynb' -not -path '*/.ipynb_checkpoints/*' -print0 \
        | xargs -0 nbstripout

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

# End-to-end docker check: build image, run 5 replicas + otel-collector + prometheus, run full-mesh-n5, tear down. Not in `just ci` because it needs a docker daemon and image pulls. Shebang recipe + trap so the stack is always torn down and the recipe's exit code reflects the orchestrator, not the teardown.
smoke-docker:
    #!/usr/bin/env bash
    set -euo pipefail
    compose='docker compose -f deploy/docker/compose.yaml'
    $compose up -d --build
    trap "$compose down" EXIT
    cargo run --release --bin orchestrator -- \
        --replicas localhost:50051=replica-0:50051,localhost:50052=replica-1:50051,localhost:50053=replica-2:50051,localhost:50054=replica-3:50051,localhost:50055=replica-4:50051 \
        scenarios/full-mesh-n5.toml

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
