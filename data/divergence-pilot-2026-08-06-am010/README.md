# Divergence pilot re-run, 2026-08-06 (automerge 0.10, app-layer partitions)

Re-run of the 9-cell divergence sweep in `../divergence-pilot-2026-08-06/`
after three harness changes that touch its numbers: heal wiring moved out of
the timed window (app-layer partitions, `eab73112`), automerge 0.9 → 0.10
(`d77d3f66`), and the sync-message-loss fix (`04c2ab64`). Same cells
({1e2,1e3,1e4} ops-per-side × {append, random_position, same_region}), same
20 trials, same docker lane, same seeds (bit-identical op streams). Provenance
is clean: `dirty: false` at commit `6a458ed8` — unlike the first archive,
these numbers are traceable to committed code with no caveat.

**This pair of directories is a controlled before/after on the harness
itself.** Analysis convention unchanged: discard trials 1–8, stats over 9–20.

## Headline (trials 9–20)

| size | append ms | same_region/append | random/append |
|---|---|---|---|
| 1e2 | 4.68 | **1.94×** (t = 5.0) | 1.04× (n.s.) |
| 1e3 | 30.60 | **1.94×** (t = 13.3) | 1.11× (t = 2.3) |
| 1e4 | 252.83 | **2.05×** (t = 18.4) | 1.21× (t = 6.6) |

Compared with the old archive:

- **The wiring-bias prediction confirmed, and the story simplified.** The old
  run's contention ratio graded with size (1.43× / 1.93× / 1.92×); with the
  shared ~0.8 ms wiring offset removed, the ratio is flat ≈ 2× across all
  three decades. The apparent size-dependence at 1e2 was the offset, not the
  CRDT.
- **`wiring_ms` behaves as designed**: 0.32–0.91 ms over all 180 trials
  (mean 0.47), reported in its own column, outside `convergence_ms`. Its
  magnitude matches the offset removed from the old numbers (old append-1e2
  5.55 − ~0.8 ≈ new 4.68).
- **automerge 0.10 ≈ 0.9 on absolute time** — every cell within ~5% after
  accounting for the offset, inside the ~10% between-run noise floor. Doubles
  as the reproducibility check.
- **random_position strengthens with size** (1.04× → 1.21×), consistent with
  positional-access cost growing with document length.

## Known anomaly — treat same_region-1e2 with care

`same_region-1e2` has CV 33% in the kept window: trials 14–17 spiked to
12–14 ms against a ~7 ms baseline. A mid-run burst, not a warm-up transient
(discard-8 cannot remove it). On medians the 1e2 contention ratio is nearer
1.6×. The spike signature matches the deferred-sync-message path
(`replicant.sync.messages.deferred`), but that counter is replica-side OTel
and `bench-docker` tears the stack down without snapshotting Prometheus, so
this run cannot confirm or rule it out. Before the next sweep, wire a
Prometheus snapshot (or per-replica metrics dump) into the recipe so the
deferred counter is checkable post-hoc.

## Reproduction

```
just bench-docker "$(echo scenarios/divergence-n2-*-1e{2,3,4}.toml)" 20
```
