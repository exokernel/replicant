# shellcheck shell=bash
#
# kind (local k8s) helpers. Sourced after scripts/lib/stack.sh.
#
# Globals:
#   kind_created  1 if kind_create_and_load created the cluster
#   forward_pids  PIDs of attached port-forwards, from kind_forward_replicas

KIND_CLUSTER=replicant
KIND_IMAGE=replicant-replica:dev
KIND_NS=replicant

kind_created=0
forward_pids=()

# Runs kubectl in the replicant namespace.
k() {
    kubectl -n "$KIND_NS" "$@"
}

# Builds the replica image and loads it into the cluster. Creates the cluster
# first if it does not exist, and then sets kind_created=1.
#
# kind nodes cannot pull a local image, so the image must be loaded.
kind_create_and_load() {
    docker build -t "$KIND_IMAGE" .
    if ! kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
        kind create cluster --name "$KIND_CLUSTER"
        kind_created=1
    fi
    kind load docker-image "$KIND_IMAGE" --name "$KIND_CLUSTER"
}

# Applies the manifests and waits for the observability deployments.
kind_apply() {
    # `kubectl apply -k` has no --load-restrictor. Render with
    # `kubectl kustomize` first, so that the configMapGenerators in base/ can
    # read deploy/shared/, which is outside the kustomization root.
    kubectl kustomize --load-restrictor=LoadRestrictionsNone deploy/k8s/overlays/kind \
        | kubectl apply -f -
    local d
    for d in otel-collector prometheus grafana; do
        k rollout status "deployment/$d" --timeout=60s
    done
}

# Points the replica StatefulSet at CRDT library $1, scales it to $2 pods, and
# waits for the rollout.
kind_scale_replicas() {
    # Set the library before scaling. Both change the StatefulSet, so in this
    # order `rollout status` waits for one rollout, not two.
    k set env statefulset/node CRDT="$1"
    k scale statefulset/node --replicas="$2"
    k rollout status statefulset/node --timeout=180s
}

# Port-forwards localhost:REPLICA_PORT_BASE+i to pod node-i, for each of the
# $1 replicas, and waits until each forward accepts connections.
#
# By default the forwards are attached: their PIDs go into forward_pids, and
# kind_kill_forwards stops them. With $2 = detach, they outlive the script, and
# kind_stop_all_forwards stops them.
kind_forward_replicas() {
    local n="$1" mode="${2:-attach}" i
    for ((i = 0; i < n; i++)); do
        if [ "$mode" = detach ]; then
            nohup kubectl -n "$KIND_NS" port-forward "pod/node-$i" "$((REPLICA_PORT_BASE + i)):50051" \
                >/dev/null 2>&1 &
            disown
        else
            k port-forward "pod/node-$i" "$((REPLICA_PORT_BASE + i)):50051" >/dev/null 2>&1 &
            forward_pids+=($!)
        fi
    done
    for ((i = 0; i < n; i++)); do
        wait_for_port "$((REPLICA_PORT_BASE + i))"
    done
}

# Stops the attached port-forwards in forward_pids.
kind_kill_forwards() {
    if [ "${#forward_pids[@]}" -gt 0 ]; then
        kill "${forward_pids[@]}" 2>/dev/null || true
        wait "${forward_pids[@]}" 2>/dev/null || true
        forward_pids=()
    fi
}

# Stops every replica port-forward, including detached ones from an earlier
# `just k8s-up`.
kind_stop_all_forwards() {
    pkill -f "kubectl -n $KIND_NS port-forward pod/node-" 2>/dev/null || true
}

# EXIT trap for scripts that own the cluster: stops the attached forwards, and
# deletes the cluster if this script created it. Set KEEP_KIND=1 to keep it.
kind_cleanup() {
    kind_kill_forwards
    if [ "$kind_created" = 1 ] && [ "${KEEP_KIND:-0}" != 1 ]; then
        kind delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
    fi
}
