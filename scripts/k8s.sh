#!/usr/bin/env bash
#
# The kind (local k8s) stack. Run via the `just smoke-k8s`, `k8s-up`,
# `k8s-ui`, `k8s-reset`, and `k8s-down` recipes.
#
#   scripts/k8s.sh smoke [scenario] [crdt]   Start, run one trial, tear down.
#   scripts/k8s.sh up    [scenario] [crdt]   Start or rescale, and keep running.
#   scripts/k8s.sh ui                        Port-forward Grafana and Prometheus.
#   scripts/k8s.sh reset                     Wipe replica and Prometheus state.
#   scripts/k8s.sh down                      Delete the cluster.
#
# The cluster is named `replicant`. `smoke` reuses a cluster that already
# exists, and deletes the cluster on exit only if it created it. Set
# KEEP_KIND=1 to keep it anyway. `smoke` checks that the deployment works. Its
# output is not for analysis; use `just bench-k8s` for that.
set -euo pipefail

cd "$(dirname "$0")/.."
. scripts/lib/stack.sh
. scripts/lib/kind.sh

cmd_smoke() {
    local scenario="${1:-$DEFAULT_SCENARIO}" crdt="${2:-$DEFAULT_CRDT}" n
    require_crdt "$crdt"
    n=$(scenario_node_count "$scenario")
    trap kind_cleanup EXIT
    kind_create_and_load
    kind_apply
    kind_scale_replicas "$crdt" "$n"
    kind_forward_replicas "$n"
    # The orchestrator dials the port-forwards. The replicas dial each other
    # by in-cluster DNS.
    replica_wiring "$n" 'node-{i}.node:50051'
    cargo run --release --bin orchestrator -- --replicas "$replicas" "$scenario"
}

cmd_up() {
    local scenario="${1:-$DEFAULT_SCENARIO}" crdt="${2:-$DEFAULT_CRDT}" n
    require_crdt "$crdt"
    n=$(scenario_node_count "$scenario")
    # Running `up` again rebuilds, reloads, and rescales in place.
    kind_create_and_load
    kind_apply
    kind_scale_replicas "$crdt" "$n"
    # Stop the forwards from an earlier `up` first. Otherwise a rescale from
    # 10 to 5 leaves orphan forwards on the upper ports.
    kind_stop_all_forwards
    kind_forward_replicas "$n" detach
    echo "kind cluster '$KIND_CLUSTER' is up with $n replicas backed by $crdt (from $scenario)."
    echo "  replica ports localhost:$REPLICA_PORT_BASE..$((REPLICA_PORT_BASE + n - 1)) forward to node-0..node-$((n - 1)) in the background."
    echo "  \`just k8s-ui\` to port-forward Grafana (:3000) and Prometheus (:9090)."
    echo "  \`just k8s-reset\` to clear state, \`just k8s-down\` to tear down."
}

cmd_ui() {
    trap kind_kill_forwards EXIT INT TERM
    k port-forward svc/grafana 3000:3000 >/dev/null 2>&1 &
    forward_pids+=($!)
    k port-forward svc/prometheus 9090:9090 >/dev/null 2>&1 &
    forward_pids+=($!)
    echo "port-forwards up:"
    echo "  Grafana:    http://localhost:3000  (admin/admin)"
    echo "  Prometheus: http://localhost:9090"
    echo "Ctrl-C to stop."
    wait
}

cmd_reset() {
    # Replica state is in memory only, and Prometheus keeps its TSDB in an
    # emptyDir, so new pods clear both. Grafana is not touched, so dashboard
    # edits stay.
    k rollout restart statefulset/node deployment/prometheus
    k rollout status statefulset/node --timeout=180s
    k rollout status deployment/prometheus --timeout=60s
    # The old port-forwards point at pods that no longer exist. Replace them.
    local n
    n=$(k get statefulset/node -o jsonpath='{.spec.replicas}')
    kind_stop_all_forwards
    kind_forward_replicas "$n" detach
    echo "reset: replicas restarted, Prometheus wiped, Grafana edits kept, port-forwards replaced."
}

cmd_down() {
    kind_stop_all_forwards
    kind delete cluster --name "$KIND_CLUSTER" 2>/dev/null || true
}

case "${1:-}" in
    smoke | up | ui | reset | down)
        cmd="$1"
        shift
        "cmd_$cmd" "$@"
        ;;
    *)
        sed -n '3,10p' "$0" >&2
        exit 2
        ;;
esac
