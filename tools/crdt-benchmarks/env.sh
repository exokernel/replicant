# shellcheck shell=bash
#
# Puts the pinned Node and npm first on PATH, and pins the crdt-benchmarks
# commit. Sourced by the scripts in this directory. It sets `here` to this
# directory.
#
# mise installs the pinned version if it is missing. The trust setting applies to
# this one command, and it names this directory only.

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# The crdt-benchmarks commit that made the traces. The spec hashes depend on it,
# and the calibration must run the JS suite on the same commit.
BENCHMARKS_COMMIT=796f702

# Exits with an error if the clone $1 is not at BENCHMARKS_COMMIT.
check_benchmarks_commit() {
    local head
    head=$(git -C "$1" rev-parse HEAD)
    if [[ $head != "$BENCHMARKS_COMMIT"* ]]; then
        echo "error: $1 is at ${head:0:7}, expected $BENCHMARKS_COMMIT" >&2
        exit 1
    fi
}

if ! command -v mise >/dev/null; then
    echo "error: these tools need mise, to pin the Node version" >&2
    exit 1
fi
node_path=$(MISE_TRUSTED_CONFIG_PATHS="$here" mise exec --cd "$here" -- which node)
PATH="$(dirname "$node_path"):$PATH"
export PATH
