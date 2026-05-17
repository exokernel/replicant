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

# End-to-end kind check: build image, spin up a local kind cluster (named `replicant`), apply the deploy/k8s/overlays/kind manifests, port-forward 5 pods, run full-mesh-n5, tear the cluster down. Not in `just ci` (needs docker + kind). Set KEEP_KIND=1 to preserve the cluster after the run for debugging. If the cluster already exists (e.g. from `just k8s-up`), this recipe reuses it and does NOT delete it on exit — only clusters it created itself are torn down.
smoke-k8s:
    #!/usr/bin/env bash
    set -euo pipefail
    cluster=replicant
    img=replicant-replica:dev

    # 1. Build the replica image locally (kind nodes only see images we load).
    docker build -t "$img" .

    # 2. Spin up the kind cluster if not already present; remember whether we
    # created it so the trap below only tears down clusters we own.
    created_by_me=0
    if ! kind get clusters 2>/dev/null | grep -qx "$cluster"; then
        kind create cluster --name "$cluster"
        created_by_me=1
    fi
    cleanup() {
        if [ "${pids+x}" = x ]; then kill "${pids[@]}" 2>/dev/null || true; fi
        if [ "$created_by_me" = "1" ] && [ "${KEEP_KIND:-0}" != "1" ]; then
            kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
        fi
    }
    trap cleanup EXIT

    # 3. Load the freshly-built image into the kind nodes.
    kind load docker-image "$img" --name "$cluster"

    # 4. Apply manifests and wait for everything to become Ready.
    kubectl apply -k deploy/k8s/overlays/kind
    kubectl -n replicant rollout status statefulset/node --timeout=180s
    kubectl -n replicant rollout status deployment/otel-collector --timeout=60s

    # 5. Port-forward each replica pod to a distinct host port.
    pids=()
    for i in 0 1 2 3 4; do
        kubectl -n replicant port-forward "pod/node-$i" "$((50051+i)):50051" \
            >/dev/null 2>&1 &
        pids+=($!)
    done
    # Wait until each forwarder is actually accepting connections.
    for i in 0 1 2 3 4; do
        until (exec 3<>"/dev/tcp/localhost/$((50051+i))") 2>/dev/null; do
            sleep 0.2
        done
    done

    # 6. Run the scenario. peer_addr is in-cluster DNS (resolvable from pods),
    # client_addr is the port-forwarded host endpoint.
    replicas=""
    for i in 0 1 2 3 4; do
        [ -n "$replicas" ] && replicas="${replicas},"
        replicas="${replicas}localhost:$((50051+i))=node-${i}.node:50051"
    done
    cargo run --release --bin orchestrator -- \
        --replicas "$replicas" \
        scenarios/full-mesh-n5.toml

# Bring up a persistent kind cluster (named `replicant`) with the full replica stack for manual development. Idempotent: re-runs build → load → apply on top of an existing cluster, so it's safe to use after editing manifests or rebuilding the image. Pair with `just k8s-down` to tear down, `just k8s-reset` to clear state between scenarios.
k8s-up:
    #!/usr/bin/env bash
    set -euo pipefail
    cluster=replicant
    img=replicant-replica:dev

    docker build -t "$img" .

    if ! kind get clusters 2>/dev/null | grep -qx "$cluster"; then
        kind create cluster --name "$cluster"
    fi
    kind load docker-image "$img" --name "$cluster"

    kubectl apply -k deploy/k8s/overlays/kind
    kubectl -n replicant rollout status statefulset/node --timeout=180s
    kubectl -n replicant rollout status deployment/otel-collector --timeout=60s
    echo "kind cluster '$cluster' is up. \`just k8s-reset\` to clear state, \`just k8s-down\` to tear down."

# Delete the kind cluster created by `just k8s-up`. No-op if not present.
k8s-down:
    #!/usr/bin/env bash
    set -euo pipefail
    kind delete cluster --name replicant 2>/dev/null || true

# Clear all Automerge state by restarting the replica StatefulSet. Cluster must already be up (run `just k8s-up` first). Waits for the rollout to complete before returning.
k8s-reset:
    kubectl -n replicant rollout restart statefulset/node
    kubectl -n replicant rollout status statefulset/node --timeout=180s

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
