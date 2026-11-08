# Divergence grid, 2026-11-07 (Artemis, docker lane)

The full grid of the divergence sweep: 18 cells on Automerge, Yrs, and Loro. Two
replicas, one in each group. Each main cell starts from the B4 paper (104,852
characters, 259,778 transactions). Six control cells (`-empty-`) start from an empty
document. See [`docs/divergence-sweep.md`](../../docs/divergence-sweep.md) for the
cells and the heal.

Host: Artemis, macOS aarch64. Docker VM with 6 CPUs and 7.7 GiB, as in
[`topology-baseline-2026-11-07`](../topology-baseline-2026-11-07/).

## Files

| Folder | Code | What differs |
|---|---|---|
| `seeded/` | `f2e3ec0b` | **The result.** Seeded library ids, and the CRDT libraries at WARN in the log filter |
| `random-ids/` | `99968d1d` | A comparison. Random ids in each trial, and the log filter cost on Automerge |
| `seeded-rerun/` | `f2e3ec0b` | Yrs `random_position-1e5` again, to test the slow trial 1 |

Each folder has 12 runs: for each library, one run of the 15 cells up to 10⁴ (10
trials each), and one run of each 10⁵ cell (3 trials each). Each `.csv` has a
`.meta.json` with its provenance. Every file has `dirty: false`, its own library in
`run.crdt`, and no deferred sync flushes.

In both grids, a heal is a reconnect: each replica keeps only the sync state that its
library keeps across a disconnect. The Yrs fingerprint is the state vector and the
delete set.

## Limits of this archive

These runs are older than two later changes, so some differences between trials have
more than one possible cause:

- **The baseline author.** At `f2e3ec0b`, replica 0 of the first trial built the paper
  under its own trial id, and a replica that loads a document keeps its id. So in trial
  1 of every paper cell, one writer continued the author of the paper's history, and in
  trials 2-10 no writer did. A later commit builds the paper under `BASELINE_AUTHOR`, an
  id that no trial uses.
- **The positions.** The seed of a trial picks its ids and, for `random_position`, its
  insert positions. So two trials of a `random_position` cell differ in both.

So a difference between trial 1 and the other trials of a paper cell can come from the
ids or from the author, and a difference between trials of a `random_position` cell can
come from the ids or from the positions. The least affected result is Loro
`same_region` in trials 2-10: its positions do not depend on the seed, and those trials
have no author effect. A rerun with the fix, and with the same positions for each id
pair, can separate the causes.

## Results: `seeded/`

Median `convergence_ms`, with the lowest and highest trial. A value with `s` is in
seconds.

| Cell | Automerge | Yrs | Loro |
|---|---|---|---|
| append-1e2 | 44.7 (43.7-50.6) | 1.7 (1.6-5.0) | 1.3 (1.2-1.5) |
| append-1e3 | 60.6 (52.4-65.0) | 1.7 (1.6-1.9) | 1.3 (1.2-1.8) |
| append-1e4 | 147 (133-150) | 1.8 (1.8-1.9) | 2.6 (2.5-3.1) |
| append-1e5 | 1309 (1288-1313) | 4.6 (3.7-4.6) | 115 (114-118) |
| random_position-1e2 | 51.4 (46.5-54.3) | 1.9 (1.9-4.2) | 1.5 (1.4-1.6) |
| random_position-1e3 | 75.8 (67.3-86.4) | 6.1 (5.9-6.5) | 4.5 (4.4-4.9) |
| random_position-1e4 | 280 (271-288) | 76.4 (75.8-122) | 41.6 (40.0-44.3) |
| random_position-1e5 | 2208 (2206-2243) | 880 (876-2207) | 658 (639-664) |
| same_region-1e2 | 49.1 (44.0-50.3) | 1.9 (1.7-1.9) | 2.3 (1.5-2.5) |
| same_region-1e3 | 63.7 (58.9-68.4) | 43.8 (43.4-44.8) | 100 (3.2-104) |
| same_region-1e4 | 230 (216-234) | 3780 (3768-3790) | 11143 (25.3-11458) |
| same_region-1e5 | 2170 (2153-2195) | 353 s (351-356 s) | 1179 s (0.27-1195 s) |
| append-empty-1e3 | 8.3 (8.2-8.6) | 1.0 (1.0-1.1) | 1.1 (1.0-1.2) |
| append-empty-1e4 | 75.6 (74.9-80.5) | 1.1 (1.0-1.5) | 2.3 (2.2-2.4) |
| random_position-empty-1e3 | 9.0 (8.9-9.3) | 2.0 (1.7-2.6) | 7.4 (3.4-10.6) |
| random_position-empty-1e4 | 82.2 (80.5-92.5) | 17.1 (14.0-21.7) | 871 (30.9-1599) |
| same_region-empty-1e3 | 13.7 (13.6-13.8) | 42.9 (42.6-43.4) | 98.4 (2.7-103) |
| same_region-empty-1e4 | 135 (133-139) | 3785 (3763-3802) | 11108 (20.6-11233) |

**Do not use the median of a Loro cell with slow trials.** It counts how many trials
have a slow id pair. Use the fast and slow groups:

| Loro cell | Slow trials | Fast (ms) | Slow (ms) |
|---|---|---|---|
| same_region-1e3 | 2, 3, 5, 6, 8, 9 | 3.2-3.3 | 99-104 |
| same_region-1e4 | 2, 3, 5, 6, 8, 9 | 25.3-25.7 | 11,122-11,458 |
| same_region-1e5 | 2, 3 | 269 | 1,179,237-1,195,235 |
| same_region-empty-1e3 | 1, 2, 3, 5, 6, 8, 9 | 2.7-3.1 | 98-103 |
| same_region-empty-1e4 | 1, 2, 3, 5, 6, 8, 9 | 20.6-21.8 | 11,073-11,233 |
| random_position-empty-1e4 | 1, 2, 3, 5, 6, 8, 9 | 30.9-121 | 803-1,599 |

A slow trial is more than 10 times the fastest trial of its cell.

## What this shows

- **Seeded ids make the Loro slow path repeatable.** Trial *r* has the same ids in
  every cell. Trials 2, 3, 5, 6, 8, and 9 are slow in every contested Loro cell, and
  trials 4, 7, and 10 are fast. So the slow path depends on the id pair. Trial 1 is
  slow only in the cells that start empty. In the paper cells, trial 1 also has the
  baseline author effect (see [Limits](#limits-of-this-archive)), so this archive cannot
  tell whether the document or the author makes trial 1 fast there. The rule is not
  known.
  At 10⁵ the spread of one cell is about 4,400 times: 269 ms against about 20 minutes.
- **Yrs `same_region` grows with the square of the ops.** 44 ms at 10³, 3.8 s at 10⁴,
  and 353 s at 10⁵: about 86 and 93 times for each 10 times the ops. It is the same
  from the paper and from an empty start. Every insert contests one anchor.
- **Automerge's heal on the paper is now its merge.** In `random-ids/`, a subscriber
  at INFO formatted Automerge's change graph on each received change. Without that
  cost, the paper cells are 44-280 ms up to 10⁴, against 250-561 ms before:

  | Automerge cell | `random-ids/` | `seeded/` |
  |---|---|---|
  | random_position-1e2 | 255 | 51.4 |
  | random_position-1e4 | 561 | 280 |
  | random_position-1e5 | 3105 | 2208 |
  | random_position-empty-1e4 | 89.6 | 82.2 |

  The empty controls change little, because their history is short.
- **Automerge has a floor of about 45 ms on the paper.** Most of it is the cost to
  apply one change on a long history (about 16 ms for each apply, two applies). See
  `automerge-apply-cost` in [`tools/findings/`](../../tools/findings/README.md).
- **Yrs and Loro have a floor of about 1.3-1.9 ms.**
- **The two grids mostly agree on Yrs.** The log filter does not change Yrs, so this
  is a check of the noise between runs. Of the 15 cells up to 10⁴, the medians agree
  within 2% in 10 and within 4% in 13. Two cells differ more: `random_position-empty-1e4`
  (17.1 ms seeded, 15.7 ms random ids, 8%) and `same_region-1e2` (1.87 ms and 1.74 ms,
  7%). The second is near the floor, where 0.1 ms is a large share. The first also
  changes its positions with its seeds.
- **Yrs also has a merge cost that depends on the ids**, at `random_position-1e5`.
  Trial 1 took 2,207 ms, and 2,249 ms in `seeded-rerun/`. Trials 2 and 3 took
  865-880 ms in both runs. Trial 1 has other ids, other positions, and the baseline
  author effect (see [Limits](#limits-of-this-archive)), so any of the three can make
  it about 2.5 times slower.
  No other Yrs cell shows it.

## Not explained

- **Why trial 1 is slow on Yrs `random_position-1e5`**, and no other Yrs cell: its
  ids, its positions, or the baseline author.
- **Loro `random_position-1e5`: 639-664 ms**, against 460-471 ms in `random-ids/`.
  Possibly an effect of the ids of trials 1-3. Not tested.
- **The heal timeout does not stop a long import.** The timeout is 600 s, but two Loro
  trials converged after about 1,180 s and passed. The orchestrator checks the
  timeout only after a fingerprint poll returns, and a poll waits for the adapter
  lock while an import runs.

## Contested share

The share of ops that the contention model counts as contested, from `achieved_contention`
in the provenance. It is the same on every library.

| Cell | Contested ops | Most concurrent siblings on one anchor |
|---|---|---|
| append-1e2 | 1.00% | 2 |
| append-1e3 | 0.10% | 2 |
| append-1e4 | 0.01% | 2 |
| append-1e5 | 0.00% | 2 |
| random_position-1e2 | 0.20% | 0 |
| random_position-1e3 | 0.92% | 2 |
| random_position-1e4 | 8.28% | 4 |
| random_position-1e5 | 34.30% | 9 |
| same_region-1e2 | 100.00% | 200 |
| same_region-1e3 | 100.00% | 2000 |
| same_region-1e4 | 100.00% | 20000 |
| same_region-1e5 | 100.00% | 200000 |
| append-empty-1e3 | 0.10% | 2 |
| append-empty-1e4 | 0.01% | 2 |
| random_position-empty-1e3 | 0.76% | 15 |
| random_position-empty-1e4 | 0.09% | 19 |
| same_region-empty-1e3 | 100.00% | 2000 |
| same_region-empty-1e4 | 100.00% | 20000 |

## Reproducing

    just bench-docker "$(ls scenarios/divergence-n2-*.toml | grep -v -- '-1e5' | tr '\n' ' ')" 10 <lib>
    just bench-docker scenarios/divergence-n2-<locality>-1e5.toml 3 <lib>

The trace of the paper must be in `traces/`. See `docs/replay.md`.
