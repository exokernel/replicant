# Divergence pilot, 2026-08-06 (forge, docker lane)

Archived measurement runs for the RQ-1 offline-divergence pilot: divergence-n2
cells ({1e2,1e3,1e4} ops-per-side × {append, random_position, same_region}
edit locality), 20 trials per cell, Automerge adapter. Each `.csv` has a
matching `.meta.json` provenance sidecar (commit, host, build profile, node
source, per-cell parameters, per-(repetition,node) PRNG seeds, achieved
contention) written by the orchestrator's `--sidecar` flag.

Unlike the gitignored `results/` scratch directory, this folder is tracked:
these are the runs the analysis and writeup refer back to.

## Files

- `divergence-1e2-1e4-shared-docker.{csv,meta.json}` — **the valid run.**
  Produced with the shared-text-object bootstrap (`EnsureText`) and the
  per-trial text-length gate; all 540 heals verified to contain both sides'
  inserts. Headline: same_region ≈ 1.9× append at 1e3/1e4 (Welch t = 14–30),
  random_position +8–14%. Note the sidecar records `dirty: true` against
  commit `48921029` — the fix was authored but not yet committed when the
  sweep ran; the code state is the commit that introduced this directory's
  parent tree (see repo history: "divergence sweep: shared-text-object fix,
  …").

- `divergence-1e2-1e4-INVALID-noshared-docker.{csv,meta.json}` and
  `divergence-1e2-1e3-INVALID-noshared-docker.{csv,meta.json}` — **known-bad
  runs, kept deliberately.** Recorded before the fix: partitioned replicas
  each lazily created their own `ROOT["text"]`, so every heal resolved a
  map-key conflict and silently discarded one side's text; no sequence
  interleaving occurred and the locality axis measured flat
  (same_region ≈ append). They are retained as the before side of the
  bug → detection → fix arc: the same cells, the same seeds, the same
  hardware, differing only in whether the text object was shared. Do not
  use them for performance claims.

## Reproduction

```
just gen-scenarios
just bench-docker "$(ls scenarios/divergence-n2-*-1e{2,3,4}.toml | tr '\n' ' ')" 20
```

Analysis convention: discard trials 1–8 as warm-up (see the
delayed-ACK/CV-calibration notes), compute stats over trials 9–20.
Seeded generation makes the op streams bit-identical across adapters and
runs; between-run cell means still wobble ~10% (scheduler/network noise),
so cross-run comparisons below that are not meaningful.
