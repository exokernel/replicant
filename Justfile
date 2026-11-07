# Run `just` to list the recipes.
#
# The container-stack recipes are thin wrappers. The shell lives in scripts/,
# where `just lint-sh` checks it.

python := if path_exists(justfile_directory() / ".venv/bin/python3") == "true" { ".venv/bin/python3" } else { "python3" }
default_scenario := "scenarios/full-mesh-n5.toml"

# Every CRDT library. Must match `Crdt::ALL` in crates/replica/src/adapter.rs.
crdts := "automerge yrs loro"

# List the recipes
default:
    @just --list --unsorted

# ── Develop ─────────────────────────────────────────────────────────────────

# Create .venv if it is missing, and install the locked packages into it
[group('develop')]
setup:
    uv sync --quiet

# Also install the packages for the archived Jupyter notebooks
[group('develop')]
setup-archive:
    uv sync --quiet --group archive

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
    shellcheck -x scripts/*.sh scripts/lib/*.sh tools/crdt-benchmarks/*.sh

# Run all tests
[group('develop')]
test:
    cargo test --workspace

# Run the unit tests of the analysis helpers (no build, no Docker)
[group('develop')]
test-py:
    {{ python }} -m pytest -m "not integration" -q

# Run every test of the analysis helpers, also those that run the release binaries
[group('develop')]
test-py-all:
    cargo build --release --bin orchestrator --bin replay
    {{ python }} -m pytest -q

# Measure test coverage of the crates and the analysis helpers; reports go to target/coverage/
[group('develop')]
coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! cargo llvm-cov --version >/dev/null 2>&1; then
        echo "cargo-llvm-cov is not installed. Run: cargo install cargo-llvm-cov --locked" >&2
        exit 1
    fi
    # One profile holds the tests and the smoke runs of `just ci`, so the code
    # that only a binary runs counts too. A replica in a container does not
    # count, because its profile stays in the container.
    cargo llvm-cov clean --workspace
    cargo llvm-cov --workspace --no-report
    for crdt in {{ crdts }}; do
        cargo llvm-cov run --no-report --bin orchestrator -- --crdt "$crdt" > /dev/null
    done
    cargo llvm-cov run --no-report --bin replay -- --spec workloads/smoke.toml \
        --data-dir workloads/fixtures --trials 1 > /dev/null
    cargo llvm-cov report --html --output-dir target/coverage/rust
    cargo llvm-cov report --summary-only
    # The Python integration tests run the release binaries.
    cargo build --release --bin orchestrator --bin replay
    {{ python }} -m pytest -q --cov=tour_helpers --cov=divergence_helpers \
        --cov-report=term-missing --cov-report=html:target/coverage/python
    echo "Reports: target/coverage/rust/html/index.html and target/coverage/python/index.html"

# Build rustdoc with private items; warnings, such as broken links, are errors
[group('develop')]
doc-check:
    RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --document-private-items

# Build rustdoc and open it in a browser
[group('develop')]
docs:
    cargo doc --workspace --no-deps --open

# Run the built-in regression scenarios (2, 3, and 4 nodes) in-process, with each CRDT library
[group('develop')]
smoke:
    for crdt in {{ crdts }}; do cargo run --bin orchestrator -- --crdt "$crdt" || exit 1; done

# Full gate, as run by CI
[group('develop')]
ci: fmt-check lint lint-sh test test-py doc-check smoke check-scenarios

# ── Run in-process ──────────────────────────────────────────────────────────

# Run the orchestrator (release build), e.g. `just run --trials 10 scenarios/ring-n5.toml`
[group('run')]
run *args:
    cargo run --release --bin orchestrator -- {{ args }}

# Replay workloads in-process (release build), e.g. `just replay --only b4 --trials 10`
[group('run')]
replay *args:
    cargo run --release --bin replay -- {{ args }}

# Replay workloads on containerized replicas, e.g. `just replay-docker 1 yrs --only b4`
[group('docker')]
replay-docker replicas crdt *args:
    scripts/replay-docker.sh '{{ replicas }}' '{{ crdt }}' {{ args }}

# Make the trace files in traces/ and the specs in workloads/ from a crdt-benchmarks clone
[group('data')]
gen-traces clone:
    tools/crdt-benchmarks/gen-traces.sh '{{ clone }}'

# Download the archived traces to traces/, with no crdt-benchmarks clone
[group('data')]
fetch-traces:
    scripts/fetch-traces.sh

# Run all workloads once and check their content. The times have no use. Needs traces/.
[group('data')]
replay-check:
    cargo run --release --bin replay -- --trials 1 --no-memory --out /dev/null
    cargo run --release --bin replay -- --spec workloads/editing-traces.toml --trials 1 --no-memory --out /dev/null

# Compare replay results with crdt-benchmarks results, on the versions the crates use
[group('data')]
calibrate clone trials='6' repeats='5':
    tools/crdt-benchmarks/run-current.sh '{{ clone }}' results/calibration/js-work results/calibration/js '{{ repeats }}'
    cargo run --release --bin replay -- --trials '{{ trials }}' --out results/calibration/replay.jsonl
    {{ python }} tools/crdt-benchmarks/compare.py results/calibration/js results/calibration/replay.jsonl

# Run every library-findings probe; writes results/findings/<date>/ (see tools/findings/README.md)
[group('data')]
findings out='' trace='':
    tools/findings/run.sh '{{ out }}' '{{ trace }}'

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

# Open the guided notebook in a browser: `edit` shows the code, `run` hides it
[group('data')]
tour mode='edit':
    {{ python }} -m marimo {{ mode }} analysis/tour.py

# Open the divergence study notebook (archived data only): `edit` shows the code, `run` hides it
[group('data')]
divergence-notebook mode='edit':
    {{ python }} -m marimo {{ mode }} analysis/divergence.py

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
        echo "error: nbstripout not found; run just setup" >&2
        exit 1
    fi
    find analysis -name '*.ipynb' -not -path '*/.ipynb_checkpoints/*' -print0 \
        | xargs -0 "$nbstripout"
