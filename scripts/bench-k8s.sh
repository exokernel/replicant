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
. scripts/lib/bench-common.sh

bench_parse_args "$@"
bench_bucket_scenarios
bench_init_output 'results/results-k8s.csv'

cluster=replicant
img=replicant-replica:dev

docker build -t "$img" .

created_by_me=0
if ! kind get clusters 2>/dev/null | grep -qx "$cluster"; then
    kind create cluster --name "$cluster"
    created_by_me=1
fi
pids=()
cleanup() {
    if [ "${#pids[@]}" -gt 0 ]; then kill "${pids[@]}" 2>/dev/null || true; fi
    if [ "$created_by_me" = "1" ] && [ "${KEEP_KIND:-0}" != "1" ]; then
        kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

kind load docker-image "$img" --name "$cluster"

# One-time manifest apply — the StatefulSet is rescaled per group below.
kubectl kustomize --load-restrictor=LoadRestrictionsNone deploy/k8s/overlays/kind | kubectl apply -f -
kubectl -n replicant rollout status deployment/otel-collector --timeout=60s
kubectl -n replicant rollout status deployment/prometheus --timeout=60s
kubectl -n replicant rollout status deployment/grafana --timeout=60s

for n in "${ns[@]}"; do
    echo ">>> bench-k8s: N=$n, crdt=$crdt, scenarios:${by_n[$n]}" >&2

    # Tear down stale port-forwards from the previous group before rescaling
    # (some of the pods they target may be about to be terminated).
    if [ "${#pids[@]}" -gt 0 ]; then
        kill "${pids[@]}" 2>/dev/null || true
        wait "${pids[@]}" 2>/dev/null || true
        pids=()
    fi

    # Set the CRDT before scaling: both mutate the StatefulSet, and doing it
    # in this order means the subsequent `rollout status` waits once for pods
    # that already carry the right library, instead of waiting for an
    # automerge rollout and then a second one. Re-setting it per group is
    # harmless (kubectl is a no-op when the value is unchanged) and keeps the
    # loop body independent of what ran before it.
    kubectl -n replicant set env statefulset/node CRDT="$crdt"
    kubectl -n replicant scale statefulset/node --replicas="$n"
    kubectl -n replicant rollout status statefulset/node --timeout=180s

    for i in $(seq 0 $((n - 1))); do
        kubectl -n replicant port-forward "pod/node-$i" "$((50051 + i)):50051" \
            >/dev/null 2>&1 &
        pids+=($!)
    done
    # Wait until each forwarder is actually accepting connections.
    for i in $(seq 0 $((n - 1))); do
        until (exec 3<>"/dev/tcp/localhost/$((50051 + i))") 2>/dev/null; do
            sleep 0.2
        done
    done

    # peer_addr is in-cluster DNS (resolvable from pods); the client side is
    # the port-forwarded host endpoint.
    bench_build_replica_wiring "$n" 'node-{i}.node:50051'
    bench_run_group "$n"
done

bench_finalize
