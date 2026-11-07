# Replaying Workloads

The `replay` binary runs a recorded workload through a CRDT library, times it,
and checks the final content. It runs the crdt-benchmarks suite (B1 to B4) and
editing traces through every adapter. The same format can hold real traces and
parameterized conflict sweeps.

This file is the reference. For an explanation of the replay and its results, read
[the guide](crdt-benchmarks-guide.md) first.

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

If you have no clone, `just fetch-traces` downloads the same files from the
`traces-v1` release. `just replay-check` runs every workload once and checks its
content. CI runs these two recipes, so a change that breaks a real workload fails CI.

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
| `tree_invariants` | Concurrent inserts at one position. | The surviving elements match, and the merged sequence is an interleaving of the branches that keeps the local order of each. |

Every check also asserts that all checked replicas agree, and that a document
loaded from the encoded bytes equals replica 0.

`tree_invariants` checks the branches jointly when at most two branches have two
or more surviving elements, as in the B2 fork traces. Each element then has one
place, and an element that two branches share is used once. A check of each
branch on its own is not enough: with branches `aab` and `aac`, each branch is a
subsequence of `aabcaa`, but no interleaving gives it. With more branches, the
check tests each branch on its own. To find if a sequence is an interleaving of
many sequences is NP-complete, and B3.5 has 1,530 such branches over ten digits.

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
disagree. The exact check is necessary, but it is not sufficient. Two merge
orders can give the same final text, for example when later edits delete both
concurrent inserts. The libraries then run different workloads, and the check
passes. The traces have no intermediate states, so the replay cannot find this
case. On the real traces, a different merge order almost always changes the
final text. See [Fixed library ids](#fixed-library-ids). A cell with more than one round of divergence and merge needs a new
decision before it can use this format.

## Library ids

A library breaks ties between concurrent ops with its replica ids. These ids are
random by default. A replay sets the id of each replica with `--id-order`:

| Order | Ids | Use |
|---|---|---|
| `seeded` (default) | They look random. They come from `--id-seed`, so each run with one seed is the same. | Comparison with crdt-benchmarks, whose libraries use random ids. |
| `ascending` | Agent 0 has the lowest id. | A trace that needs this order, and tests of the effect of the order. |
| `descending` | Agent 0 has the highest id. | Tests of the effect of the order. |

Each adapter keeps as many bits as its library can hold, and keeps the encoded width
of a random id. The `id_order` field of a `workload` row records the order that the
workload used, and the `run` row records the seed.

A workload can name the order that it needs with `id_order` in the spec. `--id-order`
overrides this field. The upstream rules say that no trace has concurrent inserts at
one position, but `friendsforever` depends on the id order under YATA. With
descending ids, and with two of three seeds, Yrs gives the wrong text from char 3798.
Real Yjs does the same with descending ids. So this workload names `ascending`. Automerge and Loro reach
`endContent` in both fixed orders and with three seeds.

## Lanes

| Lane | How to select it | What is in the timed path |
|---|---|---|
| In process | The default | The library only |
| Remote | `--replicas host:port,...`, or `just replay-docker` | The library, gRPC, and transport |

A remote row also gives `adapter_ms`, the time inside the adapter. The difference
from `time_ms` is transport. Each op is one RPC, so the remote lane is much slower
than the library alone.

**Remote `adapter_ms` before 2026-11-07 is too high for Automerge and Loro.** The
replica's log subscriber enabled the libraries' `tracing` spans, and an enabled span
formats its arguments. Automerge's `load_changes` span formats the whole change graph
on each update. The replica now keeps these crates at WARN (`replica::logging`).
Automerge B1.1 on two containers: `adapter_ms` was about 8.3 s, and is now 1.89-1.91 s,
against about 1.7 s in process. Loro was not measured again. The in-process lane
installs no subscriber, so its times are not affected.

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
| `workload` | `agents`, `id_order`, `txns`, `roots`, `merges`, and `forks` of the causal graph. One row per workload. |
| `trial` | `time_ms` (includes `read_ms`), `read_ms`, `encode_ms`, `doc_size`, `parse_ms`, `update_bytes`, `update_count`, `adapter_ms` |
| `memory` | `peak_bytes`, the highest live bytes during the replay above the live bytes of the trial before its first transaction, and `retained_bytes` that the loaded document holds. One extra trial, in process only. Its times are not used. |
| `skipped` | `reason` |
| `failed` | `trial`, which is null for the memory trial, and `error`. The binary then exits with an error. |

Discard the first trial of each workload as a warm-up.

## Calibration against crdt-benchmarks

`just calibrate <clone>` runs their suite on the npm releases that match the Rust
crates, runs the replay, and prints a comparison. `current-versions.patch` makes the
version changes, and it updates each factory to the current library API. Run both
sides on one host. The clone must be at the commit that made the traces, and the
scripts refuse any other commit. `compare.py` refuses a trial on the remote lane,
because its `time_ms` includes gRPC.

These differences make the two sides not directly comparable:

| Difference | Effect |
|---|---|
| Rust against JS and WASM | Compare with the WASM builds: `automerge-wasm` and `ywasm`. Compare ratios, not absolute times. |
| One JS sample | Their harness times once per process and rounds to whole ms. Values below about 5 ms have no rank. |
| B2 update size | The Automerge and Loro factories repeat the first batch in the second update. The replay does not. Also, the harness reports total bytes divided by 2. `compare.py` removes this division. |
| B2 on `automerge-wasm` | Its factory ignores `transact` and makes one update for each op. It does 12000 update round trips, and the replay does 2. It takes minutes, and it is a different workload. `compare.py` compares B2 with the `automerge` factory, which uses transactions. |
| Seeded ids | The replay uses one seed by default, so one run is one sample of the id order. The JS libraries draw new random ids in each process. See below. |
| `memUsed` | It measures the JS heap, so it cannot see WASM memory. Do not compare it with `retained_bytes`. |
| Document format | Automerge's JS factory creates three objects first. The replay creates one. |
| B3.2 | Not run, because the op model has no object value. In the JS factories, Automerge makes a nested CRDT object, and Yjs and Loro store a plain value. |

### Effect of the ids

On B3, the ids change the result. These values are from one host, with 6 trials for
each setting. The seeded column gives the range of the medians across seeds 1 to 5.

| Measurement | Ascending | Descending | Seeded |
|---|---|---|---|
| Yrs merge time, B3.1 | 45 ms | 4.8 ms | 24-25 ms |
| Yrs merge time, B3.4 | 44 ms | 0.7 ms | 21-22 ms |
| Yrs document size, B3.3 | 50837 bytes | 46217 bytes | 47757-50837 bytes |
| Loro snapshot size, B3.4 | 53158 bytes | not measured | 103041-103251 bytes |
| Automerge merge time, B3.4 | 109 ms | not measured | 145-149 ms |
| Automerge merge time, B3.5 | 391 ms | not measured | 492-503 ms |

- Yrs: ascending ids are the slowest order that was measured. All B3 clients edit one position. The likely cause is that each merge scans the items that conflict, and the id order sets how many it must pass. This cause was not profiled. The seed has almost no effect on the time.
- Loro: the fixed peer ids share their high bits, and the snapshot is smaller. With seeded ids, the B3 size is within 4% of JS.
- Automerge: the seeded size is within 0.1% of JS. The time depends on how the actors sort. See below.

### Automerge actor order

Automerge keeps its actors in a sorted table. If a new actor does not go at the end of
the table, `insert_actor` renumbers the actor references of the existing ops
(`op_set.rs` in automerge 0.12). B3 adds 1540 actors in one merge, so this cost can show.
The adapter also adds one actor: the `replicant-bootstrap` actor that makes the shared
object. Its first byte is `0x72`.

A test changed only the actor bytes, on B3 with 5 usable trials for each setting. Times
are medians, in ms. The same control run at the start and the end gave 108.2 and 107.7 ms
on B3.4.

| Actors | Each new actor goes | B3.1 | B3.4 | B3.5 |
|---|---|---|---|---|
| Ascending, above the bootstrap actor (`0x73`, `0x80`, `0xa5`, `0xf0`) | At the end | 85-86 | 108 | 387-392 |
| Ascending, below it (`0x00`, `0x10`, `0x71`) | Just before it | 85-89 | 134-138 | 481-487 |
| Ascending `0x00`, bootstrap actor moved to the lowest position | At the end | 86 | 108 | 392 |
| Ascending `0xa5`, bootstrap actor moved to the highest position | Just before it | 86 | 135 | 485 |
| Descending (`0xa5` or `0x00`) | Near the start | 106-107 | 142 | 501-503 |
| Seeded, with the bootstrap actor at any of three positions | At random positions | 109-110 | 147-149 | 499-504 |

What this shows:

- **The seeded times are not an effect of the bootstrap actor.** They stay the same when that actor moves. The JS libraries also have random actors, so seeded ids are the setting to compare with JS.
- **The fixed ascending times are the low limit.** No renumbering happens in them, so they understate the cost that random actors have. Seeded ids cost 37% more on B3.4 and 28% more on B3.5.
- **The effect is per actor.** On B3.4 it is about 26 µs for each actor (40 ms over 1540 actors), and on B3.5 about 71 µs. A document adds each actor once, so a run with few replicas has few of these events. The effect on orchestrator runs was not measured.

B3.1 does not change when only the bootstrap actor is renumbered, but B3.4 and B3.5 do. The
likely reason is that each list or text op refers to the object that the bootstrap actor
made, and a map op at the root does not. This reason was inferred from the results, and
it was not measured directly.

Results with seeded ids at commit `45f699cc`:

- **Document size:** Automerge is within 2% of JS on all 20 workloads. Yrs and Loro are within 2% on 17 and 15. Yrs is 7.8% larger on B3.1, 3.5% larger on B3.3, and 4.8% smaller on B3.4. Loro is 2.0% to 3.9% larger on B3, and 7.4% and 5.8% larger on B1.10 and B1.11. These differences are not explained.
- **Rank:** 9 of 16 rankings agree with JS. B3.4 and B3.5 now agree. B3.1 and B3.3 do not agree, because Loro is about 4 to 5 times slower in JS than in Rust.

`friendsforever` runs with ascending ids, so do not compare its Yrs time with a seeded workload.

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
