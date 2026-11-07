#!/usr/bin/env bash
#
# Makes the trace files and the spec files for the replay binary.
#
# Usage: gen-traces.sh <crdt-benchmarks clone> [out dir]
#
# The clone must be at the pinned commit. This script does not change the clone.
# It copies that commit to the out dir and installs its packages there, with
# --ignore-scripts. It also clones editing-traces there, at its pinned commit.

set -euo pipefail

# The commits and the lib0 version that the spec hashes were made with.
BENCHMARKS_COMMIT=796f702
EDITING_TRACES_COMMIT=762fa6c
LIB0_VERSION=0.2.88

# shellcheck source=tools/crdt-benchmarks/env.sh
. "$(dirname "$0")/env.sh"

repo=$(cd "$here/../.." && pwd)
clone=$(cd "${1:?usage: gen-traces.sh <crdt-benchmarks clone> [out dir]}" && pwd)
out=${2:-$repo/traces}
mkdir -p "$out"
out=$(cd "$out" && pwd)

head=$(git -C "$clone" rev-parse --short=7 HEAD)
if [ "$head" != "$BENCHMARKS_COMMIT" ]; then
    echo "error: $clone is at $head, expected $BENCHMARKS_COMMIT" >&2
    exit 1
fi

suite=$out/.src/crdt-benchmarks
rm -rf "$suite"
mkdir -p "$suite"
git -C "$clone" archive HEAD | tar -x -C "$suite"
(cd "$suite" && npm ci --ignore-scripts --no-audit --no-fund --silent)

lib0=$(node -p "require('$suite/node_modules/lib0/package.json').version")
if [ "$lib0" != "$LIB0_VERSION" ]; then
    echo "error: lib0 is $lib0, expected $LIB0_VERSION" >&2
    exit 1
fi

src=$out/.src/editing-traces
if [ ! -d "$src/.git" ]; then
    mkdir -p "$out/.src"
    git clone --quiet https://github.com/josephg/editing-traces.git "$src"
fi
git -C "$src" checkout --quiet "$EDITING_TRACES_COMMIT"

node "$here/dump.mjs" "$suite" "$src" "$out" "$repo"
