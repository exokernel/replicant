#!/usr/bin/env bash
#
# Multi-trial docker benchmark. Run via `just bench-docker`, which documents
# the arguments; that recipe is a thin wrapper around this script.
#
#   scripts/bench-docker.sh [scenarios] [trials] [crdt]
#
# Groups scenarios by node_count, brings up a compose stack sized to each
# group's N and backed by `crdt`, and concatenates the per-group CSVs into
# results/results-docker.csv. Owns the stack lifecycle, so it is exclusive of
# `just docker-up` / `just docker-down`.
set -euo pipefail

cd "$(dirname "$0")/.."
. scripts/lib/bench-common.sh

bench_parse_args "$@"
bench_bucket_scenarios
bench_init_output 'results/results-docker.csv'

# Prefer the analysis .venv interpreter so the pyyaml from requirements.txt is
# used; fall back to system python3 (also expected to have pyyaml).
py=$([ -x .venv/bin/python3 ] && echo .venv/bin/python3 || echo python3)
compose='docker compose -f deploy/docker/compose.generated.yaml'

# `--remove-orphans` makes this self-healing against stragglers from a
# previously-interrupted run (same project name) — without it, a leaked
# replica-N container from a prior bench would hold port 50051+N and the next
# `up` would fail with "address already in use". The trap uses the same flag
# for the same reason.
trap '$compose down --remove-orphans 2>/dev/null || true' EXIT

# Defensive pre-loop sweep: if a previous invocation crashed mid-run (e.g.
# orchestrator failure before the loop's own `down`), this clears any leftover
# containers tied to the same compose file before the first `up`.
if [ -f deploy/docker/compose.generated.yaml ]; then
    $compose down --remove-orphans 2>/dev/null || true
fi

for n in "${ns[@]}"; do
    echo ">>> bench-docker: N=$n, crdt=$crdt, scenarios:${by_n[$n]}" >&2
    "$py" deploy/docker/gen-compose.py "$n" --crdt "$crdt" \
        > deploy/docker/compose.generated.yaml
    $compose up -d --build --remove-orphans

    bench_build_replica_wiring "$n" 'replica-{i}:50051'

    # Range-query bound for the deferred snapshot below. Taken before the
    # orchestrator starts, so it covers every trial in this group.
    t0=$(date +%s)
    bench_run_group "$n"

    # ---- Deferred-sync snapshot (validity signal) ----
    # `replicant.sync.messages.deferred` counts flushes skipped because a
    # peer's outbound channel was full. A non-zero count means this group's
    # convergence timings include those stalls (see docs/metrics.md). The
    # counter is replica-side OTel, so it reaches the host only through
    # otel-collector -> Prometheus. This recipe used to tear that stack down
    # unread, which made the signal unreadable after the fact.
    #
    # An OTel counter emits no data points until its first increment, so a
    # clean run has NO deferred series at all. Absence is the good case. On
    # its own it is also indistinguishable from a dead metrics pipeline, so
    # the query collects the tx counter too: tx_total > 0 proves the pipeline
    # delivered. Read `deferred_total: 0, tx_total: >0` as "verified clean".
    #
    # Range, not instant: if the count is non-zero, the per-2s samples show
    # WHEN the deferrals happened, so attributing a burst does not cost
    # another sweep. Counters do not reset inside a group (trials use the
    # Reset RPC, not a container restart), so the last sample is the total.
    # Sample timestamps are scrape times, not op times — good to ~seconds,
    # which is enough to place a burst within a multi-minute sweep.
    #
    # Wait for the pipeline rather than sleeping a fixed interval. There are
    # two distinct races, and a single fixed sleep cannot win both:
    #
    #  - COLD START. Measured on forge, the first replica data does not
    #    reach Prometheus until ~10s after the group starts: export interval
    #    (2s) + collector batch (1s) + Prometheus container startup and
    #    first scrape. A 5s sleep lost this race and recorded an empty
    #    window that read as a clean run.
    #  - TAIL LOSS. The final trials' increments need one more
    #    export+batch+scrape (~5s) AFTER the orchestrator exits. In a long
    #    run, data from earlier trials is already present, so a
    #    liveness-only check passes immediately and the snapshot silently
    #    truncates the end of the run — exactly where a late deferral would
    #    hide.
    #
    # Waiting for the totals to stop changing covers both: quiescence
    # implies delivery has caught up, whatever the pipeline latency is.
    # Liveness is checked on doc_size_bytes rather than on tx, because
    # doc_size is emitted on every local op and so exists even for a
    # single-replica scenario that never syncs. The `or vector(...)`
    # fallbacks turn "series absent" into a stable sentinel, so an absent
    # counter compares equal to itself instead of alternating with an
    # empty string.
    promq() {
        curl -sf --max-time 5 -G http://localhost:9090/api/v1/query \
            --data-urlencode "query=$1" | jq -r '.data.result[0].value[1] // "none"'
    }
    deadline=$(( $(date +%s) + 120 ))
    prev="init"
    while :; do
        sleep 3
        live=$(promq 'count(replicant_doc_size_bytes) or vector(0)') || live=0
        cur=$(promq 'sum(replicant_sync_messages_tx_total) or vector(-1)') || cur="none"
        if [ "${live:-0}" != "0" ] && [ "$cur" = "$prev" ]; then
            break  # delivered, and unchanged across two reads 3s apart
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            echo "warning: metrics pipeline did not settle in 120s (N=$n) — snapshot may be partial" >&2
            break
        fi
        prev="$cur"
    done
    t1=$(date +%s)
    promjson=$(mktemp)
    if curl -sf --max-time 15 -G http://localhost:9090/api/v1/query_range \
            --data-urlencode 'query={__name__=~"replicant_sync_messages_(deferred|tx)_total"}' \
            --data-urlencode "start=$t0" --data-urlencode "end=$t1" \
            --data-urlencode 'step=2' -o "$promjson"; then
        snap=$(jq -c --argjson t0 "$t0" --argjson t1 "$t1" '
            def series($name): [.data.result[] | select(.metric.__name__ == $name)];
            def total($name): [series($name)[] | .values[-1][1] | tonumber] | add // 0;
            {
                window: {start: $t0, end: $t1},
                tx_total: total("replicant_sync_messages_tx_total"),
                deferred_total: total("replicant_sync_messages_deferred_total"),
                deferred_series: [series("replicant_sync_messages_deferred_total")[]
                                  | {actor: .metric.actor, peer: .metric.peer, values: .values}]
            }' "$promjson")
    else
        # A failed snapshot must not discard a sweep that already ran. Record
        # the failure in the provenance instead, and say so loudly.
        snap='{"error":"prometheus query_range failed or unreachable at localhost:9090"}'
        echo "warning: deferred-counter snapshot FAILED for N=$n — validity signal unavailable" >&2
    fi
    rm -f "$promjson"
    # `crdt` itself is recorded by bench_run_group; this only adds the
    # lane-specific deferred snapshot on top.
    jq --argjson d "$snap" '. + {deferred: $d}' "$tmpmeta" > "${tmpmeta}.new" \
        && mv "${tmpmeta}.new" "$tmpmeta"
    echo ">>> deferred snapshot N=$n: $(jq -c '{tx_total, deferred_total, error}' <<<"$snap")" >&2


    $compose down --remove-orphans
done

bench_finalize
