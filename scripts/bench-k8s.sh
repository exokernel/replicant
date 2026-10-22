#!/usr/bin/env bash
#
# Multi-trial kind benchmark. Run via `just bench-k8s`, which documents the
# arguments; that recipe is a thin wrapper around this script.
#
#   scripts/bench-k8s.sh [scenarios] [trials] [crdt]
#
# Groups scenarios by node_count, points the replica StatefulSet at `crdt` and
# rescales it to each group's N, and concatenates the per-group CSVs into
# results/results-k8s.csv. The cluster, image, and manifests are shared across
# all groups; only the replica count and the port-forwards change between
# them. Owns the cluster lifecycle, so it is exclusive of `just k8s-up` /
# `just k8s-down`. Set KEEP_KIND=1 to preserve the cluster after the run; a
# cluster that was already up is preserved automatically.
set -euo pipefail

cd "$(dirname "$0")/.."
. scripts/lib/stack.sh
. scripts/lib/kind.sh
. scripts/lib/bench-common.sh

bench_parse_args "$@"
bench_bucket_scenarios
bench_init_output 'results/results-k8s.csv'

trap kind_cleanup EXIT
kind_create_and_load
# Apply the manifests once. The loop below rescales the StatefulSet per group.
kind_apply

for n in "${ns[@]}"; do
    echo ">>> bench-k8s: N=$n, crdt=$crdt, scenarios:${by_n[$n]}" >&2

    # Stop the previous group's forwards before rescaling. Some of the pods
    # they target are about to go away.
    kind_kill_forwards
    kind_scale_replicas "$crdt" "$n"
    kind_forward_replicas "$n"

    # peer_addr is in-cluster DNS (resolvable from pods); the client side is
    # the port-forwarded host endpoint.
    replica_wiring "$n" 'node-{i}.node:50051'
    bench_run_group "$n"
done

bench_finalize
