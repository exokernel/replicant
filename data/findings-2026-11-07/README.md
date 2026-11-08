# Library findings, 2026-11-07 (Artemis)

One run of `just findings` at commit `f2e3ec0b` (`dirty: false`), on Artemis (macOS,
aarch64). The libraries are Automerge 0.12.0, Yrs 0.28.1, and Loro 1.17.1. The trace is
the B4 paper. `meta.json` has the provenance.

[`tools/findings/README.md`](../../tools/findings/README.md) states the claim of each
probe and the result to expect. One CSV for each probe:

| File | Probe |
|---|---|
| `automerge-sync-state.csv` | The heal of one op per side with the sync state kept, persisted, or fresh |
| `automerge-apply-cost.csv` | The time to apply one change on the paper and on a short document, at five positions |
| `yrs-random-insert.csv` | The time of one random insert in Yrs, by block of 2,000 inserts |
| `loro-peer-probe.csv` | The import time of two Loro documents, for 21 pairs of peer ids |

`analysis/divergence.py` draws these files.
