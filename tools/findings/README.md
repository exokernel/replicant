# Library findings

Each finding is a behaviour of a CRDT library that a simple benchmark does not show,
but that changes a result of the divergence sweep. Each finding has a probe that
anyone can run again. A probe calls the library directly and uses no Replicant code,
so the claim is about the library and not about Replicant. Each probe pins the exact
version of its library.

Run all probes:

    just findings

This builds the probes first, and then runs them one at a time. It writes one CSV
for each probe, and `meta.json` with the date, the host, the Replicant commit, the
library versions, and the SHA-256 of the trace, to `results/findings/<date>/`. The
probes measure time, so run nothing else on the host, and do not compile, while
they run. To run one probe:

    cargo run --release --manifest-path tools/findings/Cargo.toml --bin <probe>

Most probes use the B4 paper of crdt-benchmarks (`traces/editing-traces/automerge-paper.json.gz`,
104,852 characters and 259,778 transactions). See `docs/replay.md` for how to get the
trace. Give another path as the first argument.

## Findings with a probe

The expected results are from Artemis (macOS, aarch64) on 2026-11-07. On another host
the times change, but the ratios must stay about the same. If a ratio changes much,
the finding no longer holds for that version.

### `automerge-sync-state`: the sync state at a reconnect sets the cost of the heal

Automerge 0.12.0. Two documents load the paper and sync. Each inserts one character,
and the two sync again. The sync state before the second sync is one of:

| Mode | What it keeps | Heal | Of which generate |
|---|---|---|---|
| `Kept` | All state in memory | 187-195 ms | 133-143 ms |
| `Persisted` | `sync::State::decode(&state.encode())`: the shared heads only | 52-55 ms | 0.1 ms |
| `Fresh` | Nothing: a first contact | 622-645 ms | 568-592 ms |

Why: when both sides start a sync at the same time, the last `have` that each side
receives has an empty `last_sync`. A kept state holds that `have`, so the first
generate of the heal tests all 259,778 changes against its bloom filter. A persisted
state does not hold it.

Effect on Replicant: the replica now treats an unblock as a reconnect, and keeps only
the persisted state (`CrdtAdapter::sync_reconnect`).

### `automerge-apply-cost`: one change costs more to apply on a long history

Automerge 0.12.0. A target applies 20 changes of one character each, one at a time,
with `load_incremental`, at each of five positions (the start, 25%, 50%, 75%, and
the end of the text):

| Document | Changes in history | Mean time of one apply |
|---|---|---|
| `paper` | 259,779 | 15.3-16.2 ms at every position |
| `small` (100 characters) | 1 | 0.017-0.019 ms at every position |

The position of the change does not change the time.

Why: the apply walks the operations of the document (`BatchApply::apply`, and
`ChangeGraph::remove_ancestors` in a profile). This is the same mechanism as the
regression in Automerge JS 3.x, which applies updates one at a time.

Effect on Replicant: most of the 54 ms Automerge heal of one op per side is two such
applies. Every Automerge heal on the paper has this floor.

### `yrs-random-insert`: inserts at random positions get slower as they add up

Yrs 0.28.1. 10,000 inserts of one character at random positions, in blocks of 2,000:

| Document | Mean time of one insert, by block (ms) |
|---|---|
| `paper` (built from its history) | 0.029, 0.048, 0.065, 0.089, 0.103 |
| `one-insert` (the same text, inserted at once) | 0.005, 0.014, 0.030, 0.046, 0.064 |

Why: the time grows in both documents, so the cause is the random inserts, not the
history. The history adds a base cost. A random insert probably splits a block, and
Yrs walks the blocks to find an index. That cause is not confirmed.

Effect on Replicant: the divergence phase of a Yrs `random_position` cell at 10⁵ ops
takes about 6 minutes on the docker lane. This time is outside the heal window.
For comparison, the same inserts cost about 0.011 ms in Automerge and 0.001 ms in
Loro, and the cost did not grow (measured with Replicant's adapters).

### `loro-peer-probe`: some pairs of peer ids make a merge about 400 times slower

Loro 1.17.1. A separate crate: [`tools/loro-peer-probe`](../loro-peer-probe/). Two
documents each insert 10,000 characters at position 0 with no sync, and then each
imports the other's update. Each import takes about 10 ms or about 4,300 ms, and the
result is fixed for each pair of peer ids. In 12 random pairs, 10 of 24 imports were
slow. The rule is not known. It is not which id is lower.

Effect on Replicant: `same_region` cells on Loro give two or three clusters of times.
With seeded ids, the same repetitions are slow in every contested cell, so the slow
path belongs to the id pair. One repetition was slow only in the cells that start
empty, so the document matters too. See `data/divergence-grid-2026-11-07/`.

## Findings with no probe yet, seen in the grid

- **Yrs: the ids can change a merge by about 2.5 times.** `random_position-1e5` took
  about 2,200 ms with the ids of repetition 1, and about 870 ms with the ids of
  repetitions 2 and 3, in two runs. A probe needs the cell's 10⁵ seeded positions and
  the two Yrs client ids.
- **Automerge and Loro: an enabled `tracing` span formats its arguments.**
  Automerge's `load_changes` records the whole change graph with `Debug`. With a log
  subscriber at INFO, one received change on the paper took about 125 ms instead of
  about 21 ms. Replicant now keeps these crates at WARN (`replica::logging`). A probe
  is the `automerge-apply-cost` probe with a subscriber installed.

## Findings with a test in Replicant

These are checked by `just test`, not by `just findings`:

| Finding | Test |
|---|---|
| Converged Loro replicas can give snapshots of different sizes | `loro_snapshot_sizes_of_converged_replicas_differ_by_under_one_percent` (replica) |
| Converged Yrs replicas with different histories have the same state vector and delete set | `yrs_fingerprint_ignores_history`, `yrs_fingerprint_ignores_history_on_random_edits` (replica) |
| gRPC's default limit of 4 MiB stopped large sync messages | `a_sync_message_above_4_mib_arrives` (orchestrator) |
| The log filter keeps the CRDT libraries at WARN | `libraries_are_at_warn_by_default` and two more (replica, `logging`) |

## Findings with no probe yet

- **Automerge JS 3.x applies updates one at a time slowly.** From 3.0.0, one
  `loadIncremental` costs more as the document grows. One batch is fast on all
  versions. On B1.1, the apply phase took 75 ms on 2.1.10 and 5,165 ms on 3.0.0.
  `tools/crdt-benchmarks/` runs the harness, but no probe separates the per-update
  cost. See `docs/crdt-benchmarks-guide.md`.
- **Automerge merge time depends on the sort position of new actors.** See "Automerge
  actor order" in `docs/replay.md`. The measurement used a change that was not merged,
  so it cannot be run again from a tracked commit.
