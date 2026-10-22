# shellcheck shell=bash
#
# Shared plumbing for the multi-trial bench runners.
#
# Sourced, not executed, after scripts/lib/stack.sh:
# `. scripts/lib/bench-common.sh`. Every function
# communicates through globals rather than stdout, because two of them return
# a bash associative array and a bash array, neither of which survives a
# command substitution. Globals set here, in order of appearance:
#
#   scenarios trials crdt      — from bench_parse_args
#   by_n ns                    — from bench_bucket_scenarios
#   out meta header_written metas
#                              — from bench_init_output
#   replicas                   — from replica_wiring (scripts/lib/stack.sh)
#   tmpmeta                    — from bench_run_group (the caller may append
#                                to this group's provenance before teardown)
#
# Callers own the stack lifecycle (compose vs kind); this file owns everything
# that is identical between them.

# Parse the three positional arguments every bench runner takes, applying the
# same defaults the Justfile recipes advertise.
bench_parse_args() {
    scenarios="${1:-$DEFAULT_SCENARIO}"
    trials="${2:-10}"
    crdt="${3:-$DEFAULT_CRDT}"
    require_crdt "$crdt"
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
        sn=$(scenario_node_count "$s")
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
    # ${by_n[$n]} is unquoted on purpose, so the shell splits it on
    # whitespace. `replicas` comes from replica_wiring in scripts/lib/stack.sh.
    # shellcheck disable=SC2086,SC2154
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

    # For an external stack the orchestrator cannot know which library it is
    # talking to -- the replicas were launched with their own --crdt long
    # before it connected -- so it writes `run.crdt: null` and we fill it in.
    # Same field the in-process lane populates itself, so `.run.crdt` is the
    # one path to read regardless of lane. Without it the CSV is unanalyzable
    # for RQ-1: rows from three libraries are indistinguishable.
    jq --arg c "$crdt" '.run.crdt = $c' "$tmpmeta" > "${tmpmeta}.new" \
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
