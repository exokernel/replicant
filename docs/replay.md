# Replaying Workloads

The `replay` binary runs a recorded workload through a CRDT library, times it,
and checks the final content. It runs the crdt-benchmarks suite (B1 to B4) and
editing traces through every adapter. The same format can hold real traces and
parameterized conflict sweeps.

## Quick start

```sh
# Make the trace files. They stay out of the repository, in traces/.
just gen-traces ../crdt-benchmarks.git

# Run all workloads on all libraries, in this process.
just replay --trials 6 --out results/replay/suite.jsonl

# Run some workloads. A name that ends in a dot selects a group.
just replay --only b4,b2. --crdt yrs,loro

# Run one workload on one containerized replica.
just replay-docker 1 yrs --only b4 --out results/replay/b4-yrs-docker.jsonl

# Run the real concurrent traces.
just replay --spec workloads/editing-traces.toml
```

The clone must be at the commit that `tools/crdt-benchmarks/gen-traces.sh` names.
The tools do not change the clone. They copy its commit to a gitignored directory
and install the packages there, with `--ignore-scripts`. Node comes from
`tools/crdt-benchmarks/mise.toml`, so these tools need [mise](https://mise.jdx.dev).
Each spec pins the SHA-256 of its trace, and a replay stops if a hash differs.

## The two parts of a workload

A **trace** says what each replica does and what it has seen. A **spec** says how
to run a trace. The library is not in either one, so one workload runs on every
library.

### Trace

A trace file is JSON, and it can be gzipped. It is a superset of the
[editing-traces](https://github.com/josephg/editing-traces) format, so an upstream
sequential or concurrent file loads unchanged.

```json
{
  "kind": "concurrent",
  "numAgents": 2,
  "endContent": "BBAAbase",
  "txns": [
    {"agent": 0, "parents": [],  "patches": [[0, 0, "base"]]},
    {"agent": 0, "parents": [0], "patches": [[0, 0, "A"], [1, 0, "A"]]},
    {"agent": 1, "parents": [1], "patches": [[0, 0, "B"], [1, 0, "B"]]}
  ]
}
```

| Field | Meaning |
|---|---|
| `txns[].agent` | The replica that applies the transaction. |
| `txns[].parents` | Earlier transactions. The agent must have these and their ancestors, and no others. |
| `txns[].patches` | Text ops: `[position, deleted chars, inserted text]`. Positions are Unicode code points. |
| `txns[].listOps` | List ops: `[position, deleted values, inserted values]`. An addition to the upstream format. |
| `txns[].mapOps` | Map ops: `[key, value]`, on the root map. An addition to the upstream format. |
| `endContent` | The exact final text. |
| `oracle` | The check, if `endContent` does not apply. An addition to the upstream format. |

All ops in one transaction go in one library transaction. Each op sees the effect
of the ops before it. A trace edits one kind of object.

The `parents` graph makes concurrency visible in the data. In the example above,
the second batch follows the first, so nothing is concurrent. If its parent were
`[0]`, the two batches would fork, and they would be concurrent.

### Spec

A spec file is TOML, with one `[[workload]]` table per workload.

| Field | Meaning | Default |
|---|---|---|
| `name` | The name in result rows and in `--only`. | required |
| `description` | What the workload does. | empty |
| `trace` | The trace file, relative to `--data-dir`. | required |
| `sha256` | The expected hash of the trace file. | required |
| `repeat` | Replays one chain this many times on one document. | 1 |
| `receivers` | Extra replicas that only receive updates. | 0 |
| `setup_txns` | Leading transactions outside the timer. Every replica gets them first. | 0 |
| `timing` | `all`, or `merge_only` (see below). | `all` |
| `merge_batch` | Gives all updates to a replica in one adapter call. | false |
| `check_replicas` | How many replicas to read and check, from replica 0. | all |
| `heavy` | Runs only with `--include-heavy`. | false |

With `timing = "all"`, the timer covers each transaction after the setup, the
delivery to the checked replicas, and the final reads. With `merge_only`, the
writers apply their transactions and replica 0 merges them, all before the timer.
The timer then covers the merge on the other checked replicas and the final reads.

## Final-content checks

Equal replicas prove only that the replicas agree. A merge can lose one side's
work and still give equal replicas. So every workload must pass a check, and a
trial that fails it gives no timing. A trace without a check does not load.

| Check | Holds when | Asserts |
|---|---|---|
| `endContent` | The final text has one correct value. | The text is exact. |
| `exact_list` | The final list has one correct value. | The list is exact. |
| `map_winner` | Concurrent writes to map keys. | Each key holds a value that some agent wrote to it. No other key exists. |
| `tree_invariants` | Concurrent inserts at one position. | The surviving elements match, and each branch keeps its local order. |

Every check also asserts that all checked replicas agree, and that a document
loaded from the encoded bytes equals replica 0.

`crdt-benchmarks` checks less than this. It checks exact content in B1 and B4 only.
B2.1 and B2.2 check the length. B2.3, B2.4, and B3 check only that replicas agree.
The check in B3.5 cannot fail, and B4x100 has no check.

## The rule about positions

A position names an element in the document that its agent sees. Libraries order
concurrent inserts at one position differently. After a merge of such inserts,
one position can name different elements in different libraries. The libraries
then run different workloads, with no error.

So a trace with `tree_invariants` must have no merge. It can fork, and its
branches merge only at the end of the replay. The validator rejects any other
shape. A trace with a merge needs an exact check, which fails if the libraries
disagree. A cell with more than one round of divergence and merge needs a new
decision before it can use this format.

## Fixed library ids

A library breaks ties between concurrent ops with its replica ids. These ids are
random by default. A replay sets a fixed id for each replica, so each run is the
same, and document sizes do not change with the width of a random id. `--id-order`
reverses the order.

This setting can change a result. The upstream rules say that no trace has
concurrent inserts at one position, but `friendsforever` depends on the id order
under YATA. With `--id-order descending`, Yrs gives the wrong text from char 3798.
Real Yjs does the same. Automerge and Loro reach `endContent` in both orders.

## Lanes

| Lane | How to select it | What is in the timed path |
|---|---|---|
| In process | The default | The library only |
| Remote | `--replicas host:port,...`, or `just replay-docker` | The library, gRPC, and transport |

A remote row also gives `adapter_ms`, the time inside the adapter. The difference
from `time_ms` is transport. Each op is one RPC, so the remote lane is much slower
than the library alone.

The driver moves updates between replicas itself, in both lanes. No sync protocol
runs, because a sync protocol sends all that a peer has and cannot give one exact
version. To measure a sync protocol, use the orchestrator's scenarios.

A workload that needs more replicas than a lane has gives a `skipped` row. B3 needs
1540 replicas, so it runs only in process.

## Output

The output is JSON Lines. The first row has `row_type = "run"`. It records the
commit, the host, the build profile, and the library versions.

| `row_type` | Fields |
|---|---|
| `workload` | `agents`, `txns`, `roots`, `merges`, and `forks` of the causal graph. One row per workload. |
| `trial` | `time_ms` (includes `read_ms`), `read_ms`, `encode_ms`, `doc_size`, `parse_ms`, `update_bytes`, `update_count`, `adapter_ms` |
| `memory` | `peak_bytes` during the replay, and `retained_bytes` that the loaded document holds. One extra trial, in process only. Its times are not used. |
| `skipped` | `reason` |
| `failed` | `trial`, which is null for the memory trial, and `error`. The binary then exits with an error. |

Discard the first trial of each workload as a warm-up.

## Calibration against crdt-benchmarks

`just calibrate <clone>` runs their suite on the npm releases that match the Rust
crates, runs the replay, and prints a comparison. `current-versions.patch` makes the
version changes, and it updates each factory to the current library API. Run both
sides on one host.

These differences make the two sides not directly comparable:

| Difference | Effect |
|---|---|
| Rust against JS and WASM | Compare with the WASM builds: `automerge-wasm` and `ywasm`. Compare ratios, not absolute times. |
| One JS sample | Their harness times once per process and rounds to whole ms. Values below about 5 ms have no rank. |
| B2 update size | The Automerge and Loro factories repeat the first batch in the second update. The replay does not. Also, the harness reports total bytes divided by 2. `compare.py` removes this division. |
| B2 on `automerge-wasm` | Its factory ignores `transact` and makes one update for each op. It does 12000 update round trips, and the replay does 2. It takes minutes, and it is a different workload. `compare.py` compares B2 with the `automerge` factory, which uses transactions. |
| Fixed ids | The replay uses fixed, ascending ids. The JS libraries use random ids. See below. |
| `memUsed` | It measures the JS heap, so it cannot see WASM memory. Do not compare it with `retained_bytes`. |
| Document format | Automerge's JS factory creates three objects first. The replay creates one. |
| B3.2 | Not run, because the op model has no object value. In the JS factories, Automerge makes a nested CRDT object, and Yjs and Loro store a plain value. |

### Effect of the fixed ids

On B3, the ids change the result. These values are from one test on one host.

| Measurement | Ascending (default) | Descending | Random |
|---|---|---|---|
| Yrs merge time, B3.4 | 44 ms | 0.7 ms | 22 ms |
| Yrs merge time, B3.1 | 45 ms | 4.8 ms | 25 ms |
| Loro snapshot size, B3.4 | 53729 bytes | not measured | 102083 bytes |

- Yrs: ascending ids are the slowest order that was measured. All B3 clients edit one position. The likely cause is that each merge scans the items that conflict, and the id order sets how many it must pass. This cause was not profiled.
- Loro: the fixed peer ids share their high bits, and the snapshot is smaller. With random 64-bit ids, the size is within 4% of JS.
- Automerge: the size difference is about 3% at most. With random actor ids, it is about 0.1% at most. Random ids were also 20% to 40% slower in that one test.

So do not compare Yrs B3 time or Loro B3 size with crdt-benchmarks. The ids had almost no effect on the two B1 workloads in the same test, B1.10 and B1.11. Other workloads were not tested.

## Generating the crdt-benchmarks traces

`tools/crdt-benchmarks/dump.mjs` runs the unmodified js-lib against a factory that
records each call, so the traces hold what each library received. It must run the
whole suite in order. One PRNG serves all benchmarks, and `b2.js` draws from it at
import time, so a filter changes the inputs of later benchmarks.

Each B2 benchmark gives two workloads:

- `b2.x` runs as crdt-benchmarks runs it. The factories sync each batch at once, so
  the second batch follows the first. Nothing is concurrent, and all libraries
  reach the same text.
- `b2.xc` holds the same ops, with both batches forked from the setup. This is the
  concurrent form that crdt-benchmarks describes but does not run.
