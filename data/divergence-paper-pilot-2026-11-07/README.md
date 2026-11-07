# Divergence pilot on the B4 paper, 2026-11-07 (Artemis, docker lane)

The first runs of `random_position` cells that start from the LaTeX paper of
crdt-benchmarks B4: 104,852 characters, with a history of 259,778 transactions. Two
replicas, one in each group. The host is the one in
[`topology-baseline-2026-11-07`](../topology-baseline-2026-11-07/).

These runs found two faults in the heal, and the second set of files is after the
fix. Use them as a pilot. They are not the sweep.

## Files

10 trials for each cell, and 3 for `1e5`.

| Files | Heal | Code |
|---|---|---|
| `<lib>-first-contact-docker`, `<lib>-1e5-first-contact-docker` | First contact: the replicas had never synced, and the unblock reset the sync state | `52b99a30`; Loro `059c207b`, which differs only in a 10 s heal timeout |
| `<lib>-reconnect-docker`, `<lib>-1e5-reconnect-docker` | Reconnect: the links synced before the partition, kept their sync state, and Yrs and Loro used the new sync protocol | `c6b51578` |

`scenarios/divergence-n2-random_position-1e0.toml` is a control cell with one op per
side. It is not in the grid. It measures the cost of a heal with almost nothing to
merge.

A `1e5` file with no trial rows is a cell that did not converge within 600 s.

## Results

Median `convergence_ms`. The contested share is from the contention model.

| Ops per side | Contested ops | Automerge, first / reconnect | Yrs, first / reconnect | Loro, first / reconnect |
|---|---|---|---|---|
| 1 | about 0 | 688 / 254 | 9.5 / 8.3 | 10.0 / 1.4 |
| 10² | 0.3% | 705 / 269 | 9.5 / 8.6 | 9.9 / 4.4 |
| 10³ | 1.2% | 737 / 295 | 12.5 / 10.5 | 11.7 / 5.8 |
| 10⁴ | 8.3% | 1028 / 572 | 92 / 84 | 48 / 40 |
| 10⁵ | 34% | over 600 s / over 600 s | over 600 s / 915 | 497 / 459 |

What this shows:

- **The first-contact heal measured history, not the merge.** Each replica had loaded
  the paper and never synced, so Automerge reconciled all 259,778 changes at each heal,
  and the Yrs and Loro adapters sent the whole document. With a reconnect, Automerge
  is about 440 ms faster at every size.
- **The old Yrs sync layer could not heal 10⁵ within 600 s. The new one heals it in
  about 0.9 s.** The old layer sent the whole document, and sent each peer's own
  changes back.
- **Automerge still has a floor of about 254 ms with one op per side, and it cannot
  heal 10⁵ within 600 s.** The likely cause is its cost to apply one update, which
  grows with the size of the document. This was measured for Automerge JS 3.x. It was
  not measured for this cell.
- **Yrs has a floor of about 8 ms that is probably harness cost.** The Yrs fingerprint
  encodes the full 260 KB document at each poll, and the Yrs sync layer encodes the
  full delete set at each send to see whether it changed. Neither cost is part of Yjs.
- **Loro's merge time did not depend on its peer ids here.** All ten trials at 10⁴ were
  within 46 to 51 ms with random ids. On `same_region` with an empty start, some peer
  id pairs make a Loro import about 400 times slower.

## Reproducing

    just bench-docker "data/divergence-paper-pilot-2026-11-07/scenarios/divergence-n2-random_position-1e0.toml $(echo scenarios/divergence-n2-random_position-1e{2,3,4}.toml)" 10 <lib>

The trace of the paper must be in `traces/`. See `docs/replay.md`.
