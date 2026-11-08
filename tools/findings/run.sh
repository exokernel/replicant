#!/usr/bin/env bash
#
# Runs every findings probe and writes the results to an output directory:
# one CSV for each probe, and meta.json with the provenance of the run. Run it
# via `just findings`, which documents the arguments.
#
#   tools/findings/run.sh [out-dir] [trace]
#
# The probes measure time. Run nothing else on the host while they run, and do
# not compile.
set -euo pipefail

cd "$(dirname "$0")/../.."
out="${1:-results/findings/$(date +%F)}"
trace="${2:-traces/editing-traces/automerge-paper.json.gz}"
[ -f "$trace" ] || { echo "error: trace not found: $trace (see docs/replay.md)" >&2; exit 1; }
mkdir -p "$out"

# Build first, so that no compile runs while a probe measures time.
cargo build --release --quiet --manifest-path tools/findings/Cargo.toml
cargo build --release --quiet --manifest-path tools/loro-peer-probe/Cargo.toml

for probe in automerge-sync-state automerge-apply-cost yrs-random-insert; do
    echo ">>> findings: $probe" >&2
    "tools/findings/target/release/$probe" "$trace" > "$out/$probe.csv"
done
echo ">>> findings: loro-peer-probe" >&2
tools/loro-peer-probe/target/release/loro-peer-probe 10000 > "$out/loro-peer-probe.csv"

# The library versions are the exact pins in each probe's Cargo.toml.
pin() { sed -n "s/^$1 *= *\"=\\([^\"]*\\)\".*/\\1/p" "$2"; }
jq -n \
    --arg date "$(date -u +%FT%TZ)" \
    --arg host "$(hostname -s)" \
    --arg os "$(uname -sm)" \
    --arg commit "$(git rev-parse HEAD)" \
    --argjson dirty "$([ -z "$(git status --porcelain)" ] && echo false || echo true)" \
    --arg automerge "$(pin automerge tools/findings/Cargo.toml)" \
    --arg yrs "$(pin yrs tools/findings/Cargo.toml)" \
    --arg loro "$(pin loro tools/loro-peer-probe/Cargo.toml)" \
    --arg trace "$trace" \
    --arg trace_sha256 "$(shasum -a 256 "$trace" | cut -d' ' -f1)" \
    '{date: $date, host: $host, os: $os, git: {commit: $commit, dirty: $dirty},
      versions: {automerge: $automerge, yrs: $yrs, loro: $loro},
      trace: {path: $trace, sha256: $trace_sha256}}' > "$out/meta.json"
echo "wrote $out" >&2
