# orchestrator

Spawns in-process replica nodes, wires topology scenarios, applies writes, and
measures CRDT convergence time. Results are emitted as structured CSV or JSON
Lines to stdout; tracing logs go to stderr.

## Usage

```sh
# Run all thesis evaluation scenarios, 10 trials each → results/results.csv
mkdir -p results
cargo run --bin orchestrator -- --trials 10 --output csv \
  scenarios/full-mesh-n{2,3,5,10}.toml \
  scenarios/partition-heal-n{4,6,8}.toml \
  2>/dev/null > results/results.csv

# Single scenario, JSON Lines output
cargo run --bin orchestrator -- --output json scenarios/full-mesh-n5.toml

# No args: runs the built-in regression suite (same as `just smoke`)
cargo run --bin orchestrator
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--trials N` | 1 | Times to run each scenario |
| `--output csv\|json` | csv | Output format |
| `--metrics-file PATH` | *(none)* | Write OTel metrics snapshot (sync counts, op latencies, doc sizes) as JSON Lines to this file. Counters are cumulative across all scenarios in a single run — invoke once per scenario file for per-scenario data |

## Output format

One record per trial followed by one summary record per scenario.

**CSV** — single header row, then data rows distinguished by `row_type`:

```
row_type,scenario,trial,node_count,op_count,convergence_ms,mean_ms,p50_ms,p95_ms
trial,full-mesh-n5,1,5,10,3,,,
trial,full-mesh-n5,2,5,10,2,,,
summary,full-mesh-n5,2,5,10,,2.500,2.000,3.000
```

`trial` rows leave summary columns blank; `summary` rows leave `convergence_ms`
blank. In pandas: `df[df.row_type == 'trial']` for raw measurements,
`df[df.row_type == 'summary']` for aggregates.

**JSON Lines** — one object per record; trial and summary objects carry only
their relevant fields (pandas fills absent columns with NaN automatically):

```json
{"row_type":"trial","scenario":"full-mesh-n5","trial":1,"node_count":5,"op_count":10,"convergence_ms":3}
{"row_type":"summary","scenario":"full-mesh-n5","trials":2,"node_count":5,"op_count":10,"mean_ms":2.5,"p50_ms":2.0,"p95_ms":3.0}
```

## Scenario files

TOML files in `scenarios/`. Two variants:

**Full-mesh topology** — all nodes connected before writes:

```toml
name = "full-mesh-n5"

[topology]
node_count    = 5
connections   = "full_mesh"
write_pattern = "round_robin"   # or "concentrated"
op_count      = 10
```

**Partition-heal** — groups connect internally, write independently, then
cross-group edges are added; `convergence_ms` is measured from heal trigger:

```toml
name = "partition-heal-n6"

[partition_heal]
node_count    = 6
write_pattern = "round_robin"
ops_per_group = 6

[[partition_heal.groups]]
nodes = [0, 1, 2]

[[partition_heal.groups]]
nodes = [3, 4, 5]
```

Exactly one of `[topology]` or `[partition_heal]` must be present.

## Bundled scenarios

| File | Type | Nodes |
|---|---|---|
| `full-mesh-n2.toml` | full-mesh | 2 |
| `full-mesh-n3.toml` | full-mesh | 3 |
| `full-mesh-n5.toml` | full-mesh | 5 |
| `full-mesh-n10.toml` | full-mesh | 10 |
| `partition-heal-n4.toml` | partition-heal | 4 (2+2) |
| `partition-heal-n6.toml` | partition-heal | 6 (3+3) |
| `partition-heal-n8.toml` | partition-heal | 8 (4+4) |

## Architecture note

Replicas run in-process as Tokio tasks (no subprocess spawning). The
`replica` crate is a library as well as a binary for this reason. Partition
simulation is connection-level: partition = wire only within groups, heal =
add remaining full-mesh edges. This isolates CRDT convergence cost from
TCP reconnect overhead.
