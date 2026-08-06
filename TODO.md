# TODO — Framework gaps for thesis-grade contribution

Identified 2026-05-20 during an early-stage architecture review. The framework's design choices look sound (3-layer architecture, `CrdtAdapter` trait, scaffolding-owned OTel, multi-deployment target). These are the gaps to close before the contribution can claim what the abstractions promise.

Ordered roughly by impact on the thesis (highest first).

> **2026-07-13 update:** thesis reframed around an RQ (offline-divergence
> sweep across Automerge/Yrs/Loro — see NEXT_SESSION.md "Thesis direction
> pivot"). This puts **Second CRDT backend** and **Workload diversity**
> on the critical path (both are discharged by the pilot); **Multi-host
> testbed** (Phase G) is parked behind the pilot.

## High-impact — validates the contribution claim

### Second CRDT backend
The `CrdtAdapter` trait is the thesis contribution; it has exactly one implementation (Automerge). Until a second backend (Yjs, Loro, or Diamond Types) lands, the abstraction is unproven — we can't claim it generalizes. The trait may need revision to fit a second CRDT's sync model.

### Statistical rigor in the analysis
The notebook reports CV and p50/p95 but does no confidence intervals, hypothesis tests, or warm-up handling. The "edges drive convergence, not diameter" observation is the most prominent pattern in the data so far — if we ever do want to elevate it from a description to a defensible claim, that would need a methodology paragraph and CI bands on the convergence-vs-N plots.

### ~~Reproducibility metadata in the CSV~~ — DONE 2026-08-06
Landed as the proposed sidecar: `--sidecar PATH` on the orchestrator (plus `--dry-run`) writes commit hash + dirty flag, host, build profile, node source, per-cell parameters, per-(trial, node) PRNG seeds, and achieved contention; `bench-docker`/`bench-k8s` emit `results-{source}.meta.json` automatically. Citable sweeps are archived with their sidecars under tracked `data/`. Remaining sliver: host OS/kernel + container-runtime versions aren't captured yet — folded into the statistical-rigor item's methodology work if needed.

## Medium-impact — broadens the empirical surface

### Workload diversity beyond `MapPut`
**Text: DONE 2026-08-05/06** — `workload = "text_splice"` with the seeded `locality` axis (append / random_position / same_region) and the 12-cell `divergence-n2-*` grid; backed by the `EnsureText` shared-object bootstrap and the post-convergence text-length gate. **Remaining:** no scenario exercises list insert/splice yet — one list-splice scenario would complete the `Op` enum's coverage.

### In-process vs docker/k8s metrics consistency gap
Per-actor OTel metrics (`replicant.sync.messages.tx/rx`, `replicant.doc.size_bytes`, `replicant.op.duration`) only flow into the analysis pipeline for in-process runs. But [[finding_inprocess_artifact_roundrobin]] shows in-process round_robin numbers are artifactually inflated. So trusted convergence numbers (docker/k8s) and trusted per-actor metrics live in different runs. Options:

- Add a `--metrics-file` flag to the replica binary (each pod writes its own file, orchestrator-side concatenation).
- Add a Prometheus-to-JSON snapshot step in the `bench-docker` / `bench-k8s` recipes (queries PromQL just before tearing down the stack).

### Multi-host testbed
Docker compose and kind both run on one host. For the deployment-scenario realism the advisor's framing emphasizes, single-host loopback isn't going to defend well. Phase G2 (cross-AZ on EKS/GKE) on the existing roadmap addresses this — keep it on the critical path.

## Lower-impact — paper cuts

### Notebook robustness
- Cells silently produce stale figure files when renamed (orphaned `full_mesh_scaling.pdf`, the legacy combined `boxplot.pdf`). Document explicit figure-filename invariants or have the cells clean up legacy paths on first run.
- Plots don't auto-adapt to large data shapes — the boxplot needed manual restructuring after the sweep grew to 30+ scenarios. A small-data fast-path + large-data layout would prevent the next churn.

### Notebook split (planned for next session)
The current `convergence.ipynb` is doing four jobs (CSV analysis, OTel JSON ingestion, Prometheus live queries, would-be cross-source comparison). Split into `convergence.ipynb` / `protocol_metrics.ipynb` / `live_metrics.ipynb` / `comparison.ipynb`. Full plan in [`NEXT_SESSION.md`](NEXT_SESSION.md).

---

**Verdict:** the framework's bones can absorb everything in this list as additive work — no item requires redesigning what's there. Order matters though: deferring the second-backend or statistical-rigor items narrows what the framework can credibly claim to contribute, while deferring multi-host or workload-diversity items only narrows the empirical breadth.
