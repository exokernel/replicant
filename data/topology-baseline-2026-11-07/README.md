# Topology baseline, 2026-11-07 (Artemis, docker lane)

The convergence time of every topology and partition-heal scenario, on all three
libraries, on the code after the per-op encode was removed. It replaces
[`partition-heal-2026-08-06`](../partition-heal-2026-08-06/) as the reference for
later runs on this host. That run was on another host (`forge`, x86_64 Linux) and on
Automerge only, so do not compare its values with these.

## Host

`Artemis`: macOS on aarch64, 18 CPUs. Docker runs in a Linux VM with 6 CPUs and 7.7
GiB. The replicas are containers on that one host, so there is no real network
between them.

## Files

Each `.csv` has a `.meta.json` provenance file. 10 trials for each scenario.

| Files | Scenarios | Code | Heal |
|---|---|---|---|
| `<lib>-topology-docker` | All 34: full mesh, line, ring, star, and partition-heal | `52b99a30` | First contact: the unblock reset the sync state |
| `<lib>-heal-reconnect-docker` | The 12 partition-heal scenarios | `c6b51578` | Reconnect: the links synced before the partition, and kept their sync state |
| `<lib>-heal-oldcode-docker` | The 12 partition-heal scenarios | `a3ee7c3b` (`main`) | First contact, with the per-op encode |

The `oldcode` provenance files say `dirty: true`. The only change was a 15 s pause
between stacks in that copy of `scripts/bench-docker.sh`. Without it, the VM had not
released a forwarded port when the next stack started, and the 6-node stack failed.
The code is `main` with no change. The first Yrs run on the old code had one trial
of `partition-heal-n4-bridge-concentrated` that did not converge within 10 s. A rerun
of all 120 trials passed, and its files are the ones here.

No run had a deferred sync message.

## Steady-state topologies

Median `convergence_ms`.

| Scenario | Automerge | Yrs | Loro |
|---|---|---|---|
| `full-mesh-n2` | 1.30 | 1.08 | 1.17 |
| `full-mesh-n2-concentrated` | 1.91 | 1.12 | 1.21 |
| `full-mesh-n2-text` | 1.42 | 1.10 | 1.16 |
| `full-mesh-n3` | 2.63 | 2.47 | 2.87 |
| `full-mesh-n3-concentrated` | 2.73 | 2.49 | 2.93 |
| `full-mesh-n5` | 4.77 | 4.28 | 5.63 |
| `full-mesh-n5-concentrated` | 5.09 | 4.34 | 6.24 |
| `full-mesh-n5-paced` | 25.98 | 27.72 | 29.03 |
| `full-mesh-n10` | 29.14 | 11.71 | 32.45 |
| `full-mesh-n10-concentrated` | 35.23 | 13.48 | 30.62 |
| `line-n5` | 7.87 | 4.02 | 8.80 |
| `line-n5-concentrated` | 4.24 | 4.02 | 4.71 |
| `line-n10` | 15.64 | 14.38 | 17.02 |
| `line-n10-concentrated` | 9.50 | 8.41 | 11.33 |
| `ring-n5` | 4.43 | 4.27 | 5.42 |
| `ring-n5-concentrated` | 4.33 | 4.09 | 5.18 |
| `ring-n10` | 9.94 | 9.14 | 11.46 |
| `ring-n10-concentrated` | 10.12 | 9.38 | 12.55 |
| `star-n5` | 4.22 | 4.14 | 5.33 |
| `star-n5-concentrated` | 4.63 | 4.19 | 5.69 |
| `star-n10` | 15.50 | 9.90 | 12.65 |
| `star-n10-concentrated` | 19115.65 | 19106.48 | 19116.24 |

`star-n10-concentrated` and `full-mesh-n5-paced` send their ops at a fixed interval,
inside the clock. `star-n10-concentrated` sends 20 ops 1 s apart, so 19 s of its
value is the pacing.

## Partition-heal

Median `convergence_ms`, as old code / first contact / reconnect.

| Scenario | Automerge | Yrs | Loro |
|---|---|---|---|
| `n4` | 1.58 / 1.65 / 1.93 | 1.51 / 1.55 / 1.76 | 2.31 / 2.11 / 2.13 |
| `n4-bridge` | 1.42 / 1.43 / 1.77 | 1.42 / 1.44 / 1.56 | 1.90 / 2.04 / 1.75 |
| `n4-bridge-concentrated` | 1.66 / 1.61 / 1.74 | 1.61 / 1.60 / 1.74 | 1.80 / 1.78 / 1.74 |
| `n4-concentrated` | 1.62 / 1.62 / 1.97 | 1.58 / 1.60 / 1.76 | 2.12 / 2.19 / 1.95 |
| `n6` | 2.82 / 3.13 / 2.93 | 2.44 / 2.48 / 2.56 | 7.57 / 3.34 / 3.44 |
| `n6-bridge` | 2.01 / 1.96 / 2.26 | 1.91 / 1.93 / 2.14 | 6.42 / 2.57 / 2.38 |
| `n6-bridge-concentrated` | 2.36 / 2.21 / 2.41 | 2.04 / 1.98 / 2.00 | 6.19 / 2.42 / 2.30 |
| `n6-concentrated` | 2.61 / 2.56 / 2.85 | 2.47 / 2.42 / 2.36 | 3.15 / 2.75 / 2.77 |
| `n8` | 4.55 / 4.20 / 4.01 | 3.08 / 3.21 / 3.27 | 6.32 / 5.59 / 5.30 |
| `n8-bridge` | 3.04 / 2.75 / 3.59 | 2.54 / 2.59 / 2.72 | 8.24 / 8.12 / 7.52 |
| `n8-bridge-concentrated` | 3.43 / 3.05 / 3.01 | 2.92 / 3.04 / 2.98 | 3.35 / 3.39 / 3.08 |
| `n8-concentrated` | 4.27 / 4.17 / 3.80 | 3.09 / 3.08 / 3.26 | 4.76 / 3.99 / 4.04 |

What this shows:

- These scenarios start empty and write few map keys, so the kind of heal changes
  little. Most values agree within about 15%.
- **Loro on 6 nodes is the exception.** The old code took 6.2 to 7.6 ms where the new
  code takes 2.3 to 3.4 ms. The old code encoded the document at each op and each sync
  message, and a Loro snapshot export is costly. An earlier measurement found that
  the change moved heal times by at most about 6%. That was on Automerge, and it does
  not hold for Loro.
- The values are a few milliseconds, and the spread between trials is large. Ten
  trials can show a large effect, but not a small one.

## Reproducing

    just bench-docker "$(echo scenarios/{full-mesh,line,ring,star,partition-heal}-*.toml)" 10 <lib>
