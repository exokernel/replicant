#!/usr/bin/env bash
#
# The docker compose stack. Run via the `just smoke-docker`, `docker-up`,
# `docker-reset`, and `docker-down` recipes.
#
#   scripts/docker.sh smoke [scenario] [crdt]   Start, run one trial, tear down.
#   scripts/docker.sh up    [scenario] [crdt]   Start or resize, and keep running.
#   scripts/docker.sh reset                     Wipe replica and Prometheus state.
#   scripts/docker.sh down                      Tear down.
#
# The stack has one replica per node in the scenario, plus otel-collector,
# Prometheus, and Grafana. `smoke` checks that the deployment works. Its output
# is not for analysis; use `just bench-docker` for that.
set -euo pipefail

cd "$(dirname "$0")/.."
. scripts/lib/stack.sh
. scripts/lib/compose.sh

cmd_smoke() {
    local scenario="${1:-$DEFAULT_SCENARIO}" crdt="${2:-$DEFAULT_CRDT}" n
    require_crdt "$crdt"
    n=$(scenario_node_count "$scenario")
    compose_generate "$n" "$crdt"
    # Tear down on every exit. The script's exit code is the orchestrator's,
    # not the teardown's.
    trap 'compose down' EXIT
    compose up -d --build
    replica_wiring "$n" 'replica-{i}:50051'
    cargo run --release --bin orchestrator -- --replicas "$replicas" "$scenario"
}

cmd_up() {
    local scenario="${1:-$DEFAULT_SCENARIO}" crdt="${2:-$DEFAULT_CRDT}" n
    require_crdt "$crdt"
    n=$(scenario_node_count "$scenario")
    # Running `up` again with another scenario regenerates the file, and
    # `compose up` reconciles the running stack to it.
    compose_generate "$n" "$crdt"
    compose up -d --build
    echo "stack up with $n replicas backed by $crdt (from $scenario)."
    echo "  Grafana:    http://localhost:3000  (admin/admin)"
    echo "  Prometheus: http://localhost:9090"
    echo "  \`just docker-down\` to tear down."
}

cmd_reset() {
    if [ ! -f "$COMPOSE_FILE" ]; then
        echo "error: no stack; start one with \`just docker-up\`" >&2
        exit 1
    fi
    local names
    mapfile -t names < <(compose ps --services --filter status=running 2>/dev/null | grep '^replica-' || true)
    if [ "${#names[@]}" -eq 0 ]; then
        echo "error: no replica containers are running; start them with \`just docker-up\`" >&2
        exit 1
    fi
    # Replica state is in memory only, so a restart clears it.
    compose restart "${names[@]}"
    # The Prometheus TSDB is in the container's writable layer, and a restart
    # keeps it. Recreate the container instead. Grafana is not touched, so
    # dashboard edits stay.
    compose rm -fsv prometheus
    compose up -d prometheus
    echo "reset: replicas restarted, Prometheus wiped, Grafana edits kept."
}

cmd_down() {
    if [ -f "$COMPOSE_FILE" ]; then
        compose down
    fi
}

case "${1:-}" in
    smoke | up | reset | down)
        cmd="$1"
        shift
        "cmd_$cmd" "$@"
        ;;
    *)
        sed -n '3,9p' "$0" >&2
        exit 2
        ;;
esac
