# shellcheck shell=bash
#
# Helpers shared by every container-stack script. Sourced, not executed, from
# the repository root: `. scripts/lib/stack.sh`.

if ((BASH_VERSINFO[0] < 4)); then
    echo "error: the stack scripts need bash 4 or later (macOS: brew install bash)" >&2
    exit 1
fi

# Must match `Crdt::ALL` in crates/replica/src/adapter.rs, `crdts` in the
# Justfile, and CRDT_CHOICES in deploy/docker/gen-compose.py.
CRDT_CHOICES="automerge yrs loro"

# Used by the scripts that source this file.
# shellcheck disable=SC2034
DEFAULT_SCENARIO=scenarios/full-mesh-n5.toml
# shellcheck disable=SC2034
DEFAULT_CRDT=automerge

# Host port of replica 0. Replica i listens on REPLICA_PORT_BASE + i.
REPLICA_PORT_BASE=50051

# Prints the Python interpreter to use: the analysis .venv if it exists,
# otherwise python3 from PATH.
python_bin() {
    if [ -x .venv/bin/python3 ]; then
        echo .venv/bin/python3
    else
        echo python3
    fi
}

# Fails unless $1 is a known CRDT library name.
require_crdt() {
    local choice
    for choice in $CRDT_CHOICES; do
        [ "$1" = "$choice" ] && return 0
    done
    echo "error: unknown crdt '$1' (expected one of: $CRDT_CHOICES)" >&2
    return 1
}

# Prints the node_count of scenario file $1. Fails if the file has none.
#
# Uses sed, not `grep -P`: the grep that ships with macOS has no -P.
scenario_node_count() {
    local n
    [ -f "$1" ] || { echo "error: scenario file not found: $1" >&2; return 1; }
    n=$(sed -nE 's/^[[:space:]]*node_count[[:space:]]*=[[:space:]]*([0-9]+).*/\1/p' "$1" | head -n 1)
    if [ -z "$n" ]; then
        echo "error: no node_count in $1" >&2
        return 1
    fi
    echo "$n"
}

# Sets `replicas` to the orchestrator's --replicas argument for $1 replicas.
#
# The orchestrator dials each replica on its host port. $2 is the address the
# replicas use to dial each other, with `{i}` for the node index:
# 'replica-{i}:50051' under compose, 'node-{i}.node:50051' in kind.
#
# `{i}` is a plain substitution, not a printf format, so a stray `%` in the
# template cannot break it.
replica_wiring() {
    local n="$1" peer_tmpl="$2" i
    replicas=""
    for ((i = 0; i < n; i++)); do
        [ -n "$replicas" ] && replicas="${replicas},"
        replicas="${replicas}localhost:$((REPLICA_PORT_BASE + i))=${peer_tmpl//\{i\}/$i}"
    done
}

# Waits until localhost:$1 accepts TCP connections. Fails after $2 seconds
# (default 30).
wait_for_port() {
    local port="$1" deadline=$((SECONDS + ${2:-30}))
    until (exec 3<>"/dev/tcp/localhost/$port") 2>/dev/null; do
        if ((SECONDS >= deadline)); then
            echo "error: nothing is listening on localhost:$port" >&2
            return 1
        fi
        sleep 0.2
    done
}
