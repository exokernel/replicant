#!/usr/bin/env bash
#
# Downloads the archived traces to traces/, so that a replay needs no
# crdt-benchmarks clone. Run via the `just fetch-traces` recipe.
#
# The archive holds what `just gen-traces` makes. This script checks the hash of
# the archive. The replay checks the hash of each trace when it loads the trace.
set -euo pipefail

cd "$(dirname "$0")/.."

name=replicant-traces-v1
url="https://github.com/exokernel/replicant/releases/download/traces-v1/$name.tar.gz"
sha256=55dee9b5d6852b0d4f4c68c23273f27db527aea12fdf74dc7b1ca29b9667740f

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

curl --fail --location --silent --show-error --retry 3 --output "$tmp/traces.tar.gz" "$url"
echo "$sha256  $tmp/traces.tar.gz" | shasum -a 256 --check --quiet
tar -xzf "$tmp/traces.tar.gz" -C "$tmp"

mkdir -p traces
cp -R "$tmp/$name/crdt-benchmarks" "$tmp/$name/editing-traces" traces/
echo "traces are in traces/"
