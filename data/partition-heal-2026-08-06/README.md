# Partition-heal topology comparison, 2026-08-06 (forge, docker lane)

Bridge-heal versus full-mesh-heal across n = {4, 6, 8} and both write patterns
(round_robin, concentrated) — 12 scenarios, 10 trials each, Automerge adapter.

These runs exist to **replace a retracted result**. The comparison was
previously reported at 7-21× in favour of bridge heal; that figure was two
stacked measurement artifacts and the real effect is 1.1-1.9×. Both halves of
the correction are archived here, because the pair is the evidence.

## Files

- `heal-topology-applayer-docker.{csv,meta.json}` — **the valid run.** Taken at
  commit `eab73112` (clean tree, release profile, docker lane), the first
  partition-heal measurement in which the heal window contains merging rather
  than connection setup. Cite this one.

- `heal-topology-wiring-biased-docker.csv` — **the before-half, kept
  deliberately.** Same 12 scenarios, same lane, same day, taken at commit
  `d77d3f66` — identical except that the heal still opened its cross-group
  streams inside the timed window. No `.meta.json`: the bench recipe writes
  provenance to a fixed path and the second run overwrote it. The commit is
  recorded here instead; treat that as weaker provenance than a generated file.

## The result

| n | cross-group edges (bridge vs full-mesh) | bridge | full-mesh | speedup |
|---|---|---|---|---|
| 4 | 1 vs 4  | 2.51 ms | 2.71 ms | 1.08× |
| 6 | 1 vs 9  | 4.59 ms | 5.74 ms | 1.25× |
| 8 | 1 vs 16 | 4.81 ms | 9.24 ms | 1.92× |

(Means over both write patterns. Per-trial CV is 4-35%, so n=4 is within noise,
n=6 is marginal, n=8 is solid — means separated by roughly 4-5 standard errors.
No formal confidence intervals; see the statistical-rigor item in `TODO.md`.)

The bridge advantage **grows monotonically with cross-group edge count** while
diameter moves the opposite way (bridge post-heal diameter 3, full-mesh 1). A
higher-diameter, fewer-edge graph heals faster — the same relay-amplification
mechanism as the steady-state topology results, which are unaffected by this
correction because they always wired before the timer started.

## What was wrong before, and why it took two passes

**Artifact 1 — the lane.** The original 7-21× came from in-process runs, whose
timings are dominated by ~40 ms TCP delayed-ACK stalls on loopback. The
original write-up even noted the bridge cells were bimodal (mean 6.5 ms, p50
2.3 ms, p95 43.7 ms) and guessed "TCP/handshake jitter" — a correct diagnosis
that should have invalidated the headline at the time.

**Artifact 2 — wiring inside the window.** The runner opened the cross-group
sync streams *as* the heal, so TCP connect plus HTTP/2 handshake sat inside
`convergence_ms`. That cost scales with the number of edges opened, so full-mesh
paid it on every cross-group pair (70% of the n=8 window) and bridge paid it
once (23%). The comparison was substantially comparing connection setup, along
the exact axis under study.

**The trap in between.** After artifact 2 was identified but before it was
fixed, an intermediate analysis tried to control for it by subtracting the
reported `wiring_ms` from `convergence_ms`. That pointed the *wrong way* —
suggesting full-mesh merged faster and the finding had reversed. It is invalid:
merging overlaps wiring (streams that come up early begin merging while later
ones are still connecting), so `convergence − wiring` is a loose lower bound,
and far looser for full-mesh's 16 overlapping streams than for bridge's one.
**Subtracting an overlapping cost is not a control.** Only removing wiring from
the window entirely produced a comparable measurement.

## Reproducing

    just bench-docker "$(echo scenarios/partition-heal-*.toml)" 10

`wiring_ms` / `mean_wiring_ms` in the CSV report the residual setup still inside
the window — one concurrent round of unblock RPCs, 0.36-0.84 ms here, and
**flat in edge count**. If a future run shows it scaling with the number of
healed edges, the app-layer partition has regressed and the timings are
contaminated again.
