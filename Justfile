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
#
# Resolves nbstripout from .venv/bin first (forge: pip-into-venv install),
# then $PATH (mac: pipx/brew install). Works on either machine without
# requiring the same install method.
clean-notebooks:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -x .venv/bin/nbstripout ]; then
        nbstripout=.venv/bin/nbstripout
    elif command -v nbstripout >/dev/null 2>&1; then
        nbstripout=nbstripout
    else
        echo "error: nbstripout not found. Install via 'pip install -r analysis/requirements.txt' (into .venv) or system-wide (brew/pipx)." >&2
        exit 1
    fi
    find analysis -name '*.ipynb' -not -path '*/.ipynb_checkpoints/*' -print0 \
        | xargs -0 "$nbstripout"

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

# `crdt` (accepted by smoke-docker, bench-docker, docker-up, smoke-k8s,
# bench-k8s, k8s-up; one of automerge|yrs|loro, default automerge) selects the
# library backing every replica in the stack. It is a DEPLOYMENT parameter, not
# a scenario field: RQ-1 compares libraries on the identical workload, so one
# scenario file runs against all three rather than needing three near-duplicate
# copies. The bench recipes record it in the run-provenance JSON, because the
# orchestrator never sees it and the CSV alone cannot tell three libraries'
# rows apart.

# End-to-end docker verification: generate an N-replica compose file from the scenario's node_count and the given CRDT library, build the image, run the stack, run one trial of the scenario, tear down. Output goes to the terminal — this proves the deployment plumbing works, it is NOT for analysis. For multi-trial sweeps that produce a notebook-readable CSV, use `just bench-docker`. Not in `just ci` because it needs a docker daemon and image pulls. Shebang recipe + trap so the stack is always torn down and the recipe's exit code reflects the orchestrator, not the teardown.
smoke-docker scenario='scenarios/full-mesh-n5.toml' crdt='automerge':
    #!/usr/bin/env bash
    set -euo pipefail
    scenario='{{scenario}}'
    crdt='{{crdt}}'
    n=$(grep -oP 'node_count\s*=\s*\K\d+' "$scenario" | head -1)
    if [ -z "$n" ]; then echo "error: no node_count in $scenario" >&2; exit 1; fi

    # Prefer the analysis .venv interpreter so the pyyaml from requirements.txt
    # is used; fall back to system python3 (also expected to have pyyaml).
    py=$([ -x .venv/bin/python3 ] && echo .venv/bin/python3 || echo python3)
    "$py" deploy/docker/gen-compose.py "$n" --crdt "$crdt" > deploy/docker/compose.generated.yaml

    compose='docker compose -f deploy/docker/compose.generated.yaml'
    $compose up -d --build
    trap "$compose down" EXIT

    replicas=""
    for i in $(seq 0 $((n-1))); do
        [ -n "$replicas" ] && replicas="${replicas},"
        replicas="${replicas}localhost:$((50051+i))=replica-${i}:50051"
    done
    cargo run --release --bin orchestrator -- --replicas "$replicas" "$scenario"

# Multi-trial docker benchmark: groups scenarios by node_count, brings up a docker stack sized to each group's N and backed by `crdt` (Reset between trials within a group), and concatenates per-group CSVs into results/results-docker.csv for notebook analysis. Single-N callers (e.g. `scenarios/*-n5*.toml`) get one stack up/down; mixed-N callers (e.g. `scenarios/*.toml`) get one stack per distinct N. Pass scenarios as a space-separated string. Exclusive of `just docker-up`/`just docker-down` (owns the stack lifecycle). Before each group's teardown it snapshots the `deferred` sync counter out of Prometheus into the provenance file — see the comment on that block for why the run is only trustworthy when `deferred_total` is 0 and `tx_total` is not.
bench-docker scenarios='scenarios/full-mesh-n5.toml' trials='10' crdt='automerge':
    #!/usr/bin/env bash
    set -euo pipefail
    scenarios='{{scenarios}}'
    trials='{{trials}}'
    crdt='{{crdt}}'
    out='results/results-docker.csv'
    # Run-provenance file for $out — commit, host, build profile, seeds, cell params.
    # `results/` is gitignored, so without this a stored CSV cannot be traced
    # back to the code that produced it.
    meta="${out%.csv}.meta.json"

    # Validate scenarios exist and bucket them by node_count.
    declare -A by_n
    for s in $scenarios; do
        [ -f "$s" ] || { echo "error: scenario file not found: $s" >&2; exit 1; }
        sn=$(grep -oP 'node_count\s*=\s*\K\d+' "$s" | head -1)
        if [ -z "$sn" ]; then echo "error: no node_count in $s" >&2; exit 1; fi
        by_n[$sn]+="$s "
    done
    # Sort distinct N's numerically so the output CSV row order is deterministic.
    ns=($(printf '%s\n' "${!by_n[@]}" | sort -n))

    py=$([ -x .venv/bin/python3 ] && echo .venv/bin/python3 || echo python3)
    compose='docker compose -f deploy/docker/compose.generated.yaml'
    # `--remove-orphans` makes the recipe self-healing against stragglers
    # from a previously-interrupted run (same project name) — without it, a
    # leaked replica-N container from a prior bench would hold port 50051+N
    # and the next `up` would fail with "address already in use". The trap
    # uses the same flag for the same reason.
    trap '$compose down --remove-orphans 2>/dev/null || true' EXIT

    # Defensive pre-loop sweep: if a previous bench-docker invocation
    # crashed mid-run (e.g. orchestrator failure before the loop's own
    # `down`), this clears any leftover containers tied to the same
    # compose file before the first `up`.
    if [ -f deploy/docker/compose.generated.yaml ]; then
        $compose down --remove-orphans 2>/dev/null || true
    fi

    mkdir -p results
    : > "$out"  # start clean — bench runs are not additive across invocations
    header_written=0
    metas=()  # per-group provenance temp files, merged into "$meta" after the loop

    for n in "${ns[@]}"; do
        echo ">>> bench-docker: N=$n, crdt=$crdt, scenarios:${by_n[$n]}" >&2
        "$py" deploy/docker/gen-compose.py "$n" --crdt "$crdt" > deploy/docker/compose.generated.yaml
        $compose up -d --build --remove-orphans

        replicas=""
        for i in $(seq 0 $((n-1))); do
            [ -n "$replicas" ] && replicas="${replicas},"
            replicas="${replicas}localhost:$((50051+i))=replica-${i}:50051"
        done

        tmpcsv=$(mktemp)
        tmpmeta=$(mktemp)
        metas+=("$tmpmeta")
        # Range-query bound for the deferred snapshot below. Taken before the
        # orchestrator starts, so it covers every trial in this group.
        t0=$(date +%s)
        # ${by_n[$n]} intentionally unquoted so the shell splits on whitespace.
        cargo run --release --bin orchestrator -- --trials "$trials" --replicas "$replicas" --provenance "$tmpmeta" ${by_n[$n]} > "$tmpcsv"
        if [ "$header_written" = "0" ]; then
            cat "$tmpcsv" >> "$out"
            header_written=1
        else
            tail -n +2 "$tmpcsv" >> "$out"
        fi
        rm "$tmpcsv"

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
        # `crdt` is a deployment parameter, so the orchestrator never sees it
        # and cannot record it. Without this the CSV is unanalyzable for RQ-1:
        # rows from three libraries are indistinguishable.
        jq --argjson d "$snap" --arg c "$crdt" '. + {deferred: $d, crdt: $c}' "$tmpmeta" > "${tmpmeta}.new" && mv "${tmpmeta}.new" "$tmpmeta"
        echo ">>> deferred snapshot N=$n: $(jq -c '{tx_total, deferred_total, error}' <<<"$snap")" >&2

        $compose down --remove-orphans
    done

    # One provenance record per orchestrator invocation (i.e. per node-count group). Kept
    # as a list rather than merged into one object: each invocation has its own
    # replica wiring and scenario set, and flattening them would invent a single
    # run that never happened.
    jq -s '{invocations: .}' "${metas[@]}" > "$meta"
    rm -f "${metas[@]}"

    echo "wrote $out and $meta (${#ns[@]} node-count groups)"

# End-to-end kind verification: parse N from the scenario's node_count, build the image, point the StatefulSet at the given CRDT library, spin up the kind cluster (named `replicant`), apply manifests and scale the StatefulSet to N, port-forward N pods, run one trial of the scenario, tear the cluster down. Output goes to the terminal — this proves the deployment plumbing works, it is NOT for analysis. For multi-trial sweeps that produce a notebook-readable CSV, use `just bench-k8s`. Not in `just ci` (needs docker + kind). Set KEEP_KIND=1 to preserve the cluster after the run for debugging. If the cluster already exists (e.g. from `just k8s-up`), this recipe reuses it and does NOT delete it on exit — only clusters it created itself are torn down.
smoke-k8s scenario='scenarios/full-mesh-n5.toml' crdt='automerge':
    #!/usr/bin/env bash
    set -euo pipefail
    scenario='{{scenario}}'
    crdt='{{crdt}}'
    n=$(grep -oP 'node_count\s*=\s*\K\d+' "$scenario" | head -1)
    if [ -z "$n" ]; then echo "error: no node_count in $scenario" >&2; exit 1; fi
    case "$crdt" in
        automerge|yrs|loro) ;;
        *) echo "error: unknown crdt '$crdt' (expected automerge, yrs, or loro)" >&2; exit 1 ;;
    esac
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

    # 4. Apply manifests, scale to N replicas (the base sets a default but
    # scenarios with N != base get rescaled here), wait for everything Ready.
    # `kubectl apply -k` doesn't expose --load-restrictor; pipe through
    # `kubectl kustomize` first so the configMapGenerators in base/ can
    # read from deploy/shared/ (outside the kustomization root).
    kubectl kustomize --load-restrictor=LoadRestrictionsNone deploy/k8s/overlays/kind | kubectl apply -f -
    # Set the CRDT before scaling: both mutate the StatefulSet, and doing it
    # in this order means the subsequent `rollout status` waits once for pods
    # that already carry the right library, instead of waiting for an
    # automerge rollout and then a second one.
    kubectl -n replicant set env statefulset/node CRDT="$crdt"
    kubectl -n replicant scale statefulset/node --replicas="$n"
    kubectl -n replicant rollout status statefulset/node --timeout=180s
    kubectl -n replicant rollout status deployment/otel-collector --timeout=60s
    kubectl -n replicant rollout status deployment/prometheus --timeout=60s
    kubectl -n replicant rollout status deployment/grafana --timeout=60s

    # 5. Port-forward each replica pod to a distinct host port.
    pids=()
    for i in $(seq 0 $((n-1))); do
        kubectl -n replicant port-forward "pod/node-$i" "$((50051+i)):50051" \
            >/dev/null 2>&1 &
        pids+=($!)
    done
    # Wait until each forwarder is actually accepting connections.
    for i in $(seq 0 $((n-1))); do
        until (exec 3<>"/dev/tcp/localhost/$((50051+i))") 2>/dev/null; do
            sleep 0.2
        done
    done

    # 6. Run the scenario. peer_addr is in-cluster DNS (resolvable from pods),
    # client_addr is the port-forwarded host endpoint.
    replicas=""
    for i in $(seq 0 $((n-1))); do
        [ -n "$replicas" ] && replicas="${replicas},"
        replicas="${replicas}localhost:$((50051+i))=node-${i}.node:50051"
    done
    cargo run --release --bin orchestrator -- --replicas "$replicas" "$scenario"

# Multi-trial kind benchmark: groups scenarios by node_count, points the StatefulSet at `crdt` and rescales it to each group's N (Reset between trials within a group), and concatenates per-group CSVs into results/results-k8s.csv for notebook analysis. The kind cluster, image, and manifests are shared across all groups — only the StatefulSet replica count and port-forwards change between groups. Set KEEP_KIND=1 to preserve the cluster after the run; if the cluster was already up it is preserved automatically. Exclusive of `just k8s-up`/`just k8s-down` (owns the cluster lifecycle).
bench-k8s scenarios='scenarios/full-mesh-n5.toml' trials='10' crdt='automerge':
    #!/usr/bin/env bash
    set -euo pipefail
    scenarios='{{scenarios}}'
    trials='{{trials}}'
    crdt='{{crdt}}'
    case "$crdt" in
        automerge|yrs|loro) ;;
        *) echo "error: unknown crdt '$crdt' (expected automerge, yrs, or loro)" >&2; exit 1 ;;
    esac
    out='results/results-k8s.csv'
    # Run provenance for $out — see the note in `bench-docker`.
    meta="${out%.csv}.meta.json"
    cluster=replicant
    img=replicant-replica:dev

    # Validate scenarios exist and bucket them by node_count.
    declare -A by_n
    for s in $scenarios; do
        [ -f "$s" ] || { echo "error: scenario file not found: $s" >&2; exit 1; }
        sn=$(grep -oP 'node_count\s*=\s*\K\d+' "$s" | head -1)
        if [ -z "$sn" ]; then echo "error: no node_count in $s" >&2; exit 1; fi
        by_n[$sn]+="$s "
    done
    ns=($(printf '%s\n' "${!by_n[@]}" | sort -n))

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

    mkdir -p results
    : > "$out"  # start clean — bench runs are not additive across invocations
    header_written=0
    metas=()  # per-group provenance temp files, merged into "$meta" after the loop

    for n in "${ns[@]}"; do
        echo ">>> bench-k8s: N=$n, scenarios:${by_n[$n]}" >&2

        # Tear down stale port-forwards from the previous group before rescaling
        # (some of the pods they target may be about to be terminated).
        if [ "${#pids[@]}" -gt 0 ]; then
            kill "${pids[@]}" 2>/dev/null || true
            wait "${pids[@]}" 2>/dev/null || true
            pids=()
        fi

        # Set the CRDT before scaling: both mutate the StatefulSet, and doing
        # it in this order means the subsequent `rollout status` waits once
        # for pods that already carry the right library, instead of waiting
        # for an automerge rollout and then a second one. Re-setting it per
        # group is harmless (kubectl is a no-op when the value is unchanged)
        # and keeps the loop body independent of what ran before it.
        kubectl -n replicant set env statefulset/node CRDT="$crdt"
        kubectl -n replicant scale statefulset/node --replicas="$n"
        kubectl -n replicant rollout status statefulset/node --timeout=180s

        for i in $(seq 0 $((n-1))); do
            kubectl -n replicant port-forward "pod/node-$i" "$((50051+i)):50051" \
                >/dev/null 2>&1 &
            pids+=($!)
        done
        for i in $(seq 0 $((n-1))); do
            until (exec 3<>"/dev/tcp/localhost/$((50051+i))") 2>/dev/null; do
                sleep 0.2
            done
        done

        replicas=""
        for i in $(seq 0 $((n-1))); do
            [ -n "$replicas" ] && replicas="${replicas},"
            replicas="${replicas}localhost:$((50051+i))=node-${i}.node:50051"
        done

        tmpcsv=$(mktemp)
        tmpmeta=$(mktemp)
        metas+=("$tmpmeta")
        # ${by_n[$n]} intentionally unquoted so the shell splits on whitespace.
        cargo run --release --bin orchestrator -- --trials "$trials" --replicas "$replicas" --provenance "$tmpmeta" ${by_n[$n]} > "$tmpcsv"
        if [ "$header_written" = "0" ]; then
            cat "$tmpcsv" >> "$out"
            header_written=1
        else
            tail -n +2 "$tmpcsv" >> "$out"
        fi
        rm "$tmpcsv"
        # `crdt` is a deployment parameter the orchestrator never sees — see
        # the note in `bench-docker`.
        jq --arg c "$crdt" '. + {crdt: $c}' "$tmpmeta" > "${tmpmeta}.new" && mv "${tmpmeta}.new" "$tmpmeta"
    done

    # One provenance record per orchestrator invocation — see the note in `bench-docker`.
    jq -s '{invocations: .}' "${metas[@]}" > "$meta"
    rm -f "${metas[@]}"

    echo "wrote $out and $meta (${#ns[@]} node-count groups)"

# Bring up the docker compose stack sized for the given scenario, backed by the given CRDT library, and leave it running. Use when you want to inspect Prometheus (http://localhost:9090) while iterating, or run many scenarios against the same stack. Pair with `just docker-down` to tear down. Idempotent: re-running regenerates the compose file from the new scenario and `docker compose up -d` reconciles.
docker-up scenario='scenarios/full-mesh-n5.toml' crdt='automerge':
    #!/usr/bin/env bash
    set -euo pipefail
    scenario='{{scenario}}'
    crdt='{{crdt}}'
    n=$(grep -oP 'node_count\s*=\s*\K\d+' "$scenario" | head -1)
    if [ -z "$n" ]; then echo "error: no node_count in $scenario" >&2; exit 1; fi

    py=$([ -x .venv/bin/python3 ] && echo .venv/bin/python3 || echo python3)
    "$py" deploy/docker/gen-compose.py "$n" --crdt "$crdt" > deploy/docker/compose.generated.yaml

    docker compose -f deploy/docker/compose.generated.yaml up -d --build
    echo "stack up with $n replicas backed by $crdt (from $scenario)."
    echo "  Grafana:    http://localhost:3000  (admin/admin)"
    echo "  Prometheus: http://localhost:9090"
    echo "  \`just docker-down\` to tear down."

# Tear down the docker compose stack brought up by `just docker-up`. No-op if not present.
docker-down:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -f deploy/docker/compose.generated.yaml ]; then
        docker compose -f deploy/docker/compose.generated.yaml down
    fi

# Wipe scenario data without rebuilding the stack: restart replica containers (clears in-memory Automerge state) and recreate the prometheus container (drops its tsdb). Grafana edits are preserved because Grafana's container is untouched. Stack must already be up (`just docker-up` first).
docker-reset:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -f deploy/docker/compose.generated.yaml ]; then
        echo "error: no compose.generated.yaml — bring stack up first with \`just docker-up\`" >&2
        exit 1
    fi
    compose='docker compose -f deploy/docker/compose.generated.yaml'
    replicas=$($compose ps --services --filter status=running 2>/dev/null | grep '^replica-' || true)
    if [ -z "$replicas" ]; then
        echo "error: no replica containers running — bring stack up first with \`just docker-up\`" >&2
        exit 1
    fi
    # Replica state is purely in-memory, so `restart` (re-exec the
    # process in the same container) is enough — no need to recreate.
    $compose restart $replicas
    # Prometheus's tsdb lives in the container's writable layer, so
    # `restart` would preserve it. Recreate the container instead.
    $compose rm -fsv prometheus
    $compose up -d prometheus
    echo "reset: replicas restarted, Prometheus wiped, Grafana edits preserved."

# Bring up a persistent kind cluster (named `replicant`) with the replica stack sized for the given scenario and backed by the given CRDT library, plus background per-pod port-forwards on localhost:50051..50050+N so the host-side orchestrator can dial each replica directly. Idempotent: re-runs build → load → apply → scale on top of an existing cluster, so re-invoking with a different scenario rescales the StatefulSet and the forwards. Pair with `just k8s-down` to tear down, `just k8s-reset` to clear state between scenarios.
k8s-up scenario='scenarios/full-mesh-n5.toml' crdt='automerge':
    #!/usr/bin/env bash
    set -euo pipefail
    scenario='{{scenario}}'
    crdt='{{crdt}}'
    n=$(grep -oP 'node_count\s*=\s*\K\d+' "$scenario" | head -1)
    if [ -z "$n" ]; then echo "error: no node_count in $scenario" >&2; exit 1; fi
    case "$crdt" in
        automerge|yrs|loro) ;;
        *) echo "error: unknown crdt '$crdt' (expected automerge, yrs, or loro)" >&2; exit 1 ;;
    esac
    cluster=replicant
    img=replicant-replica:dev

    docker build -t "$img" .

    if ! kind get clusters 2>/dev/null | grep -qx "$cluster"; then
        kind create cluster --name "$cluster"
    fi
    kind load docker-image "$img" --name "$cluster"

    # `kubectl apply -k` doesn't expose --load-restrictor; pipe through
    # `kubectl kustomize` first so the configMapGenerators in base/ can
    # read from deploy/shared/ (outside the kustomization root).
    kubectl kustomize --load-restrictor=LoadRestrictionsNone deploy/k8s/overlays/kind | kubectl apply -f -
    # Set the CRDT before scaling: both mutate the StatefulSet, and doing it
    # in this order means the subsequent `rollout status` waits once for pods
    # that already carry the right library, instead of waiting for an
    # automerge rollout and then a second one.
    kubectl -n replicant set env statefulset/node CRDT="$crdt"
    kubectl -n replicant scale statefulset/node --replicas="$n"
    kubectl -n replicant rollout status statefulset/node --timeout=180s
    kubectl -n replicant rollout status deployment/otel-collector --timeout=60s
    kubectl -n replicant rollout status deployment/prometheus --timeout=60s
    kubectl -n replicant rollout status deployment/grafana --timeout=60s

    # Background per-pod port-forwards so the host-side orchestrator can dial
    # each replica without a manual loop in another shell. Clears any stale
    # forwards from a previous k8s-up first (so a rescale-from-10-to-5
    # doesn't leave 5 orphans on :50056-:50060). nohup + disown so they
    # outlive this recipe.
    pkill -f 'kubectl -n replicant port-forward pod/node-' 2>/dev/null || true
    for i in $(seq 0 $((n-1))); do
        nohup kubectl -n replicant port-forward "pod/node-$i" "$((50051+i)):50051" >/dev/null 2>&1 &
        disown
    done
    # Wait until each forwarder is actually accepting connections (5s max).
    for i in $(seq 0 $((n-1))); do
        for _ in $(seq 1 50); do
            (exec 3<>"/dev/tcp/localhost/$((50051+i))") 2>/dev/null && break
            sleep 0.1
        done
    done

    echo "kind cluster '$cluster' is up with $n replicas (from $scenario)."
    echo "  replica ports localhost:50051..$((50050+n)) → node-0..node-$((n-1)) forwarded in background"
    echo "  \`just k8s-ui\` to port-forward Grafana (:3000) and Prometheus (:9090)."
    echo "  \`just k8s-reset\` to clear state, \`just k8s-down\` to tear down."

# Delete the kind cluster created by `just k8s-up`. No-op if not present. Also kills the background per-pod port-forwards launched by k8s-up.
k8s-down:
    #!/usr/bin/env bash
    set -euo pipefail
    pkill -f 'kubectl -n replicant port-forward pod/node-' 2>/dev/null || true
    kind delete cluster --name replicant 2>/dev/null || true

# Foreground port-forwards for the cluster's observability UIs. Run this in a separate shell after `just k8s-up`; Ctrl-C tears both forwards down. Grafana on :3000 (admin/admin), Prometheus on :9090.
k8s-ui:
    #!/usr/bin/env bash
    set -euo pipefail
    pids=()
    cleanup() {
        if [ "${pids+x}" = x ]; then kill "${pids[@]}" 2>/dev/null || true; fi
    }
    trap cleanup EXIT INT TERM
    kubectl -n replicant port-forward svc/grafana 3000:3000 >/dev/null 2>&1 &
    pids+=($!)
    kubectl -n replicant port-forward svc/prometheus 9090:9090 >/dev/null 2>&1 &
    pids+=($!)
    echo "port-forwards up:"
    echo "  Grafana:    http://localhost:3000  (admin/admin)"
    echo "  Prometheus: http://localhost:9090"
    echo "Ctrl-C to stop."
    wait

# Wipe scenario data without re-applying manifests: rollout-restart the replica StatefulSet (clears in-memory Automerge state) and the prometheus Deployment (drops its emptyDir tsdb). Grafana edits are preserved because grafana's pod is untouched. Restoring the per-pod port-forwards is part of the reset since the pods themselves are replaced. Cluster must already be up (`just k8s-up` first).
k8s-reset:
    #!/usr/bin/env bash
    set -euo pipefail
    kubectl -n replicant rollout restart statefulset/node deployment/prometheus
    kubectl -n replicant rollout status statefulset/node --timeout=180s
    kubectl -n replicant rollout status deployment/prometheus --timeout=60s

    # Replicas got new pods, so the existing port-forwards point at gone IPs.
    # Replace them.
    n=$(kubectl -n replicant get statefulset/node -o jsonpath='{.spec.replicas}')
    pkill -f 'kubectl -n replicant port-forward pod/node-' 2>/dev/null || true
    for i in $(seq 0 $((n-1))); do
        nohup kubectl -n replicant port-forward "pod/node-$i" "$((50051+i)):50051" >/dev/null 2>&1 &
        disown
    done
    for i in $(seq 0 $((n-1))); do
        for _ in $(seq 1 50); do
            (exec 3<>"/dev/tcp/localhost/$((50051+i))") 2>/dev/null && break
            sleep 0.1
        done
    done
    echo "reset: replicas restarted, Prometheus wiped, Grafana edits preserved, port-forwards re-established."

# Build rustdoc for all crates and open in browser
docs:
    cargo doc --workspace --no-deps --open

# Run a scenario file through the orchestrator
bench scenario:
    cargo run --bin orchestrator -- {{scenario}}

# Regenerate the divergence-n2 scenario grid (ops-per-side x edit-locality) in
# scenarios/. Idempotent — re-running over an unchanged grid rewrites identical
# bytes. Stdlib-only, so the .venv is preferred but not required.
gen-scenarios:
    #!/usr/bin/env bash
    set -euo pipefail
    py=$([ -x .venv/bin/python3 ] && echo .venv/bin/python3 || echo python3)
    "$py" scenarios/gen-divergence-grid.py

# Verify the on-disk divergence grid matches its generator. Guards against a
# hand-edited cell whose filename no longer matches its ops_per_group — which
# would look like a real sweep result rather than a typo. Part of `just ci`.
check-scenarios:
    #!/usr/bin/env bash
    set -euo pipefail
    py=$([ -x .venv/bin/python3 ] && echo .venv/bin/python3 || echo python3)
    "$py" scenarios/gen-divergence-grid.py --check

# Full CI gate: format check → lint → test → smoke → scenario-grid check
ci:
    cargo fmt --all --check
    just lint
    just test
    just smoke
    just check-scenarios
