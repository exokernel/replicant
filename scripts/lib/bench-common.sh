# shellcheck shell=bash
#
# Shared plumbing for the multi-trial bench runners.
#
# Sourced, not executed: `. scripts/lib/bench-common.sh`. Every function
# communicates through globals rather than stdout, because two of them return
# a bash associative array and a bash array, neither of which survives a
# command substitution. Globals set here, in order of appearance:
#
#   scenarios trials crdt      — from bench_parse_args
#   by_n ns                    — from bench_bucket_scenarios
#   out meta header_written metas
#                              — from bench_init_output
#   replicas                   — from bench_build_replica_wiring
#   tmpmeta                    — from bench_run_group (the caller may append
#                                to this group's provenance before teardown)
#
# Callers own the stack lifecycle (compose vs kind); this file owns everything
# that is identical between them.

# Must match the `Crdt` value_enum in crates/replica/src/main.rs and
# CRDT_CHOICES in deploy/docker/gen-compose.py.
BENCH_CRDT_CHOICES="automerge yrs loro"

# Parse the three positional arguments every bench runner takes, applying the
# same defaults the Justfile recipes advertise.
bench_parse_args() {
    scenarios="${1:-scenarios/full-mesh-n5.toml}"
    trials="${2:-10}"
    crdt="${3:-automerge}"

    local choice found=0
    for choice in $BENCH_CRDT_CHOICES; do
        [ "$crdt" = "$choice" ] && found=1
    done
    if [ "$found" = "0" ]; then
        echo "error: unknown crdt '$crdt' (expected: $BENCH_CRDT_CHOICES)" >&2
        return 1
    fi
}

# Validate that every scenario file exists and bucket them by node_count.
#
# Sets `by_n` (node_count -> space-separated scenario paths) and `ns` (the
# distinct node counts, sorted numerically so the output CSV's row order is
# deterministic across runs).
bench_bucket_scenarios() {
    declare -gA by_n=()
    local s sn
    for s in $scenarios; do
        [ -f "$s" ] || { echo "error: scenario file not found: $s" >&2; return 1; }
        sn=$(grep -oP 'node_count\s*=\s*\K\d+' "$s" | head -1)
        if [ -z "$sn" ]; then echo "error: no node_count in $s" >&2; return 1; fi
        by_n[$sn]+="$s "
    done
    # shellcheck disable=SC2207  # word-splitting is the intent; counts are integers
    ns=($(printf '%s\n' "${!by_n[@]}" | sort -n))
}

# Prepare the output CSV and provenance paths.
#
# The CSV is truncated: bench runs are not additive across invocations. A
# previous run worth keeping must be copied out first.
bench_init_output() {
    out="$1"
    # Run-provenance file for $out — commit, host, build profile, seeds, cell
    # params. `results/` is gitignored, so without this a stored CSV cannot be
    # traced back to the code that produced it.
    meta="${out%.csv}.meta.json"
    mkdir -p results
    : > "$out"
    header_written=0
    metas=()  # per-group provenance temp files, merged into "$meta" at the end
}

# Build the orchestrator's `--replicas` argument for a group of size $1.
#
# $2 is the in-stack peer address with `{i}` standing in for the node index —
# 'replica-{i}:50051' under compose, 'node-{i}.node:50051' in the cluster,
# where the name is in-cluster DNS. The client side is always the host-side
# port-forward, which is why the two halves differ at all.
#
# A `{i}` placeholder rather than a printf format: a caller-supplied format
# string is a live hazard if it ever grows a stray `%`, and plain substitution
# has no such edge.
bench_build_replica_wiring() {
    local n="$1" peer_tmpl="$2" i peer
    replicas=""
    for i in $(seq 0 $((n - 1))); do
        [ -n "$replicas" ] && replicas="${replicas},"
        peer="${peer_tmpl//\{i\}/$i}"
        replicas="${replicas}localhost:$((50051 + i))=${peer}"
    done
}

# Run the orchestrator over one node-count group and append its rows to $out.
#
# Leaves `tmpmeta` set to this group's provenance file so the caller can add
# lane-specific fields (the docker runner appends a deferred-counter snapshot)
# before the stack comes down.
bench_run_group() {
    local n="$1" tmpcsv
    tmpcsv=$(mktemp)
    tmpmeta=$(mktemp)
    metas+=("$tmpmeta")
    # ${by_n[$n]} intentionally unquoted so the shell splits on whitespace.
    # shellcheck disable=SC2086
    cargo run --release --bin orchestrator -- \
        --trials "$trials" --replicas "$replicas" --provenance "$tmpmeta" \
        ${by_n[$n]} > "$tmpcsv"
    if [ "$header_written" = "0" ]; then
        cat "$tmpcsv" >> "$out"
        header_written=1
    else
        tail -n +2 "$tmpcsv" >> "$out"
    fi
    rm "$tmpcsv"

    # `crdt` is a deployment parameter, so the orchestrator never sees it and
    # cannot record it. Without this the CSV is unanalyzable for RQ-1: rows
    # from three libraries are indistinguishable.
    jq --arg c "$crdt" '. + {crdt: $c}' "$tmpmeta" > "${tmpmeta}.new" \
        && mv "${tmpmeta}.new" "$tmpmeta"
}

# Merge the per-group provenance files into $meta and report.
#
# Kept as a list rather than merged into one object: each orchestrator
# invocation has its own replica wiring and scenario set, and flattening them
# would invent a single run that never happened.
bench_finalize() {
    jq -s '{invocations: .}' "${metas[@]}" > "$meta"
    rm -f "${metas[@]}"
    echo "wrote $out and $meta (${#ns[@]} node-count groups)"
}
