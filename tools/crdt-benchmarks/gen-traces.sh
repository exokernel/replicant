#!/usr/bin/env bash
#
# Makes the trace files and the spec files for the replay binary.
#
# Usage: gen-traces.sh <crdt-benchmarks clone> [out dir]
#
# The clone must be at the pinned commit, with `npm ci` already run. This script
# clones editing-traces into the out dir at its pinned commit.

set -euo pipefail

# The commits and the lib0 version that the spec hashes were made with.
BENCHMARKS_COMMIT=796f702
EDITING_TRACES_COMMIT=762fa6c
LIB0_VERSION=0.2.88

repo=$(cd "$(dirname "$0")/../.." && pwd)
clone=$(cd "${1:?usage: gen-traces.sh <crdt-benchmarks clone> [out dir]}" && pwd)
out=${2:-$repo/traces}
mkdir -p "$out"
out=$(cd "$out" && pwd)

head=$(git -C "$clone" rev-parse --short=7 HEAD)
if [ "$head" != "$BENCHMARKS_COMMIT" ]; then
    echo "error: $clone is at $head, expected $BENCHMARKS_COMMIT" >&2
    exit 1
fi
lib0=$(node -p "require('$clone/node_modules/lib0/package.json').version")
if [ "$lib0" != "$LIB0_VERSION" ]; then
    echo "error: lib0 is $lib0, expected $LIB0_VERSION. Run npm ci in the clone." >&2
    exit 1
fi

src=$out/.src/editing-traces
if [ ! -d "$src/.git" ]; then
    mkdir -p "$out/.src"
    git clone --quiet https://github.com/josephg/editing-traces.git "$src"
fi
git -C "$src" checkout --quiet "$EDITING_TRACES_COMMIT"

node "$repo/tools/crdt-benchmarks/dump.mjs" "$clone" "$src" "$out" "$repo"
