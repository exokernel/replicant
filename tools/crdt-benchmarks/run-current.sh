#!/usr/bin/env bash
#
# Runs the crdt-benchmarks suite on library versions that match the Rust crates,
# and saves one results file per repeat and library.
#
# Usage: run-current.sh <crdt-benchmarks clone> <work dir> <out dir> [repeats]
#
# The clone must be at the commit that made the traces (BENCHMARKS_COMMIT in
# env.sh). This script does not change the clone. It copies that commit to the
# work dir, applies current-versions.patch, and installs with --ignore-scripts. The
# patch keeps lib0 at the version that made the trace files, and it updates each
# factory to the current library API. It leaves out B4x100.

set -euo pipefail

# shellcheck source=tools/crdt-benchmarks/env.sh
. "$(dirname "$0")/env.sh"

clone=$(cd "${1:?usage: run-current.sh <clone> <work dir> <out dir> [repeats]}" && pwd)
work=${2:?missing work dir}
out=${3:?missing out dir}
repeats=${4:-5}
libs=(yjs ywasm automerge-wasm automerge loro)
check_benchmarks_commit "$clone"

mkdir -p "$out"
out=$(cd "$out" && pwd)
# compare.py reads every r*-*.json file in the out dir. Delete the samples of an
# earlier run, so that a run with fewer repeats does not keep old ones.
rm -f "$out"/r*-*.json "$out"/r*-*.log "$out/versions.txt"
rm -rf "$work"
mkdir -p "$work"
git -C "$clone" archive HEAD | tar -x -C "$work"
cd "$work"
rm -f package-lock.json
patch --quiet -p1 < "$here/current-versions.patch"
npm install --ignore-scripts --no-audit --no-fund --silent

{
    node --version
    for p in lib0 yjs ywasm @automerge/automerge loro-crdt; do
        echo "$p $(node -p "require('./node_modules/$p/package.json').version")"
    done
} > "$out/versions.txt"

for ((repeat = 1; repeat <= repeats; repeat++)); do
    for lib in "${libs[@]}"; do
        echo "repeat $repeat: $lib"
        rm -f benchmarks/results.json
        (cd "benchmarks/$lib" && node --expose-gc run.js > "$out/r$repeat-$lib.log" 2>&1)
        mv benchmarks/results.json "$out/r$repeat-$lib.json"
    done
done
echo "done: $out"
