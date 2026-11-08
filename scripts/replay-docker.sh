#!/usr/bin/env bash
#
# Runs the replay binary against containerized replicas. Run via the
# `just replay-docker` recipe.
#
#   scripts/replay-docker.sh <replicas> <crdt> [replay arguments]
#
# The script starts a compose stack with <replicas> replicas of library <crdt>,
# runs the replay on the host, and tears the stack down. A workload that needs
# more replicas than the stack has gives a `skipped` row. One replica is enough
# for a workload with one writer and no receivers, such as B4.
set -euo pipefail

cd "$(dirname "$0")/.."
. scripts/lib/stack.sh
. scripts/lib/compose.sh

n="${1:?usage: replay-docker.sh <replicas> <crdt> [replay arguments]}"
crdt="${2:?usage: replay-docker.sh <replicas> <crdt> [replay arguments]}"
shift 2
require_crdt "$crdt"

compose_generate "$n" "$crdt"
# Tear down on every exit. The exit code is the replay's, not the teardown's.
trap 'compose down' EXIT
compose up -d --build

addrs=""
for ((i = 0; i < n; i++)); do
    port=$((REPLICA_PORT_BASE + i))
    wait_for_port "$port" 60
    addrs="${addrs:+$addrs,}localhost:$port"
done

cargo run --release --bin replay -- --replicas "$addrs" --crdt "$crdt" "$@"
