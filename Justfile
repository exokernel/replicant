# Run `just` to list the recipes.
#
# The container-stack recipes are thin wrappers. The shell lives in scripts/,
# where `just lint-sh` checks it.

python := if path_exists(justfile_directory() / ".venv/bin/python3") == "true" { ".venv/bin/python3" } else { "python3" }
default_scenario := "scenarios/full-mesh-n5.toml"

# List the recipes
default:
    @just --list --unsorted

# ── Develop ─────────────────────────────────────────────────────────────────

# Format all crates
[group('develop')]
fmt:
    cargo fmt --all

# Fail if any file is not formatted
[group('develop')]
fmt-check:
    cargo fmt --all --check

# Type-check all targets without building binaries
[group('develop')]
check:
    cargo check --workspace --all-targets

# Run clippy on all targets; warnings are errors
[group('develop')]
lint:
    cargo clippy --workspace --all-targets -- -D warnings

# Shellcheck scripts/; skipped if shellcheck is not installed
[group('develop')]
lint-sh:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v shellcheck >/dev/null; then
        echo "shellcheck is not installed; skipping" >&2
        exit 0
    fi
    # -x follows each `. scripts/lib/...` so the shared functions are checked
    # against their call sites.
    shellcheck -x scripts/*.sh scripts/lib/*.sh

# Run all tests
[group('develop')]
test:
    cargo test --workspace

# Build rustdoc with private items; warnings, such as broken links, are errors
[group('develop')]
doc-check:
    RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --document-private-items

# Build rustdoc and open it in a browser
[group('develop')]
docs:
    cargo doc --workspace --no-deps --open

# Run the built-in regression scenarios (2, 3, and 4 nodes) in-process
[group('develop')]
smoke:
    cargo run --bin orchestrator

# Full gate, as run by CI
[group('develop')]
ci: fmt-check lint lint-sh test doc-check smoke check-scenarios

# ── Run in-process ──────────────────────────────────────────────────────────

# Run the orchestrator (release build), e.g. `just run --trials 10 scenarios/ring-n5.toml`
[group('run')]
run *args:
    cargo run --release --bin orchestrator -- {{ args }}

# ── Container stacks ────────────────────────────────────────────────────────
#
# `crdt` is one of automerge, yrs, or loro. It is a deployment argument, not a
# scenario field, so one scenario file runs against every library. See
# docs/deployment.md.

# Start, run one trial of the scenario, tear down
[group('docker')]
smoke-docker scenario=default_scenario crdt='automerge':
    scripts/docker.sh smoke '{{ scenario }}' '{{ crdt }}'

# Multi-trial sweep; writes results/results-docker.csv
[group('docker')]
bench-docker scenarios=default_scenario trials='10' crdt='automerge':
    scripts/bench-docker.sh '{{ scenarios }}' '{{ trials }}' '{{ crdt }}'

# Start or resize a stack for the scenario, and keep it running
[group('docker')]
docker-up scenario=default_scenario crdt='automerge':
    scripts/docker.sh up '{{ scenario }}' '{{ crdt }}'

# Wipe replica and Prometheus state; keep Grafana edits
[group('docker')]
docker-reset:
    scripts/docker.sh reset

# Tear down
[group('docker')]
docker-down:
    scripts/docker.sh down

# Start, run one trial of the scenario, tear down (KEEP_KIND=1 keeps the cluster)
[group('k8s')]
smoke-k8s scenario=default_scenario crdt='automerge':
    scripts/k8s.sh smoke '{{ scenario }}' '{{ crdt }}'

# Multi-trial sweep; writes results/results-k8s.csv
[group('k8s')]
bench-k8s scenarios=default_scenario trials='10' crdt='automerge':
    scripts/bench-k8s.sh '{{ scenarios }}' '{{ trials }}' '{{ crdt }}'

# Start or rescale a cluster for the scenario, and keep it running
[group('k8s')]
k8s-up scenario=default_scenario crdt='automerge':
    scripts/k8s.sh up '{{ scenario }}' '{{ crdt }}'

# Port-forward Grafana (:3000) and Prometheus (:9090) until Ctrl-C
[group('k8s')]
k8s-ui:
    scripts/k8s.sh ui

# Wipe replica and Prometheus state; keep Grafana edits
[group('k8s')]
k8s-reset:
    scripts/k8s.sh reset

# Delete the cluster and its port-forwards
[group('k8s')]
k8s-down:
    scripts/k8s.sh down

# ── Scenarios and notebooks ─────────────────────────────────────────────────

# Regenerate the divergence scenario grid in scenarios/
[group('data')]
gen-scenarios:
    {{ python }} scenarios/gen-divergence-grid.py

# Fail if the divergence grid on disk differs from its generator
[group('data')]
check-scenarios:
    {{ python }} scenarios/gen-divergence-grid.py --check

# Strip notebook outputs; run before describing a change that re-ran a notebook
[group('data')]
clean-notebooks:
    #!/usr/bin/env bash
    set -euo pipefail
    # jj does not apply the .gitattributes nbstripout filter, so strip
    # explicitly. `jj fix` runs nbstripout too; this is the manual route.
    if [ -x .venv/bin/nbstripout ]; then
        nbstripout=.venv/bin/nbstripout
    elif command -v nbstripout >/dev/null 2>&1; then
        nbstripout=nbstripout
    else
        echo "error: nbstripout not found; pip install -r analysis/requirements.txt" >&2
        exit 1
    fi
    find analysis -name '*.ipynb' -not -path '*/.ipynb_checkpoints/*' -print0 \
        | xargs -0 "$nbstripout"
