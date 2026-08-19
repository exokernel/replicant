# TODO — Framework gaps for thesis-grade contribution

Identified 2026-05-20 during an early-stage architecture review. The framework's design choices look sound (3-layer architecture, `CrdtAdapter` trait, scaffolding-owned OTel, multi-deployment target). These are the gaps to close before the contribution can claim what the abstractions promise.

Ordered roughly by impact on the thesis (highest first).

> **2026-07-13 update:** thesis reframed around an RQ (offline-divergence
> sweep across Automerge/Yrs/Loro — see NEXT_SESSION.md "Thesis direction
> pivot"). This puts **Second CRDT backend** and **Workload diversity**
> on the critical path (both are discharged by the pilot); **Multi-host
> testbed** (Phase G) is parked behind the pilot.

> **2026-08-18 update:** RQ-1 sharpened during a proposal drill (notes repo,
> `trace-replay-notes.md` — "RQ-1 drill (2026-08-18)"). It now asks which
> workload parameters significantly alter the *pairwise performance ratios*
> between the three libraries, and commits to **memory** as a dependent
> variable and to **baseline history depth** as a third workload axis. An audit
> against `54c0810` found neither exists: nothing measures memory at all, and
> the pre-divergence document is always empty. Both are added below.

## High-impact — validates the contribution claim

### Second CRDT backend
The `CrdtAdapter` trait is the thesis contribution; it has exactly one implementation (Automerge). Until a second backend (Yjs, Loro, or Diamond Types) lands, the abstraction is unproven — we can't claim it generalizes. The trait may need revision to fit a second CRDT's sync model.

### Memory instrumentation
RQ-1 names merge time, memory, and storage. Time is solid (`convergence_ms`) and storage is partial (`doc_size_bytes` is an OTel gauge, absent from the results CSV, and per-actor OTel only reaches the analysis pipeline on the in-process lane). **Memory has no instrumentation of any kind** — no RSS, no retained heap, no allocator hook. It is the only committed dependent variable with nothing behind it, which makes it the gap most likely to silently drop out of the thesis.

Design question to settle first: RSS versus retained heap. Eg-walker's protocol — the gold standard the proposal already adopts — reports retained heap, so parity argues for that, but retained heap in Rust needs an allocator shim or explicit accounting rather than a process-level read. Whatever is chosen must be emitted from the scaffolding, not from inside `CrdtAdapter`, like every other comparable metric.

Memory is also the measure that most needs the baseline-history axis below: CRDT memory is dominated by per-element metadata and tombstones, both functions of history rather than of visible text, so an empty-baseline grid measures the least discriminating case.

### Statistical rigor in the analysis
The notebook reports CV and p50/p95 but does no confidence intervals, hypothesis tests, or warm-up handling. The "edges drive convergence, not diameter" observation is the most prominent pattern in the data so far — if we ever do want to elevate it from a description to a defensible claim, that would need a methodology paragraph and CI bands on the convergence-vs-N plots.

**2026-08-06 — this now has a concrete motivating case.** The bridge-vs-full-mesh heal result was published internally at 7-21× and stood for months. It was two stacked artifacts: the in-process lane's delayed-ACK stalls, and connection setup sitting inside the heal window while scaling with the number of edges opened. The real figure is 1.1-1.9×. Neither artifact would have been caught by more trials or tighter CIs — both were *bias*, not variance — which is the argument for the methodology paragraph covering measurement construction, not just error bars. It also produced a reusable trap: an intermediate analysis "controlled" for wiring by subtracting the reported `wiring_ms` from `convergence_ms`, which pointed the wrong way, because merging overlaps wiring and the subtraction is a loose lower bound. Subtracting an overlapping cost is not a control.

**2026-08-18 — the plan is now specified by RQ-1's "significantly".** Four pieces, in dependency order:

1. **Equivalence needs a positive test.** Half of RQ-1 asks where the libraries *are* equivalent, and failing to reject "no difference" does not establish sameness — it is equally consistent with a noisy experiment. Use TOST against a pre-specified equivalence margin. This gives the ~3x pilot-gate threshold a principled home: it becomes the margin, declared before the data rather than applied after.
2. **Effect sizes with intervals, not p-values.** Per cell, the pairwise ratio of medians with a CI. Excludes 1 → ranking resolved; inside the margin → equivalent; straddling and wide → underpowered, and reported as such. That three-state grid is the answer to RQ-1.
3. **Nonparametric.** Merge timings are skewed and heavy-tailed; bootstrap CIs on the ratio of medians rather than normal-theory intervals.
4. **Multiple-comparison correction.** 12 cells x 3 pairwise comparisons is 36 tests; Holm or Benjamini-Hochberg.

The bias-versus-variance caveat above outranks all four: the statistics sit on top of the validity gates (deferred counter, text-length gate, partition-held assertion, `wiring_ms`), not in place of them.

### ~~Heal measurements include connection setup~~ — DONE 2026-08-06
Partition-heal scenarios wired their cross-group edges *as* the heal, so TCP connect + HTTP/2 handshake sat inside `convergence_ms` and scaled with the number of edges opened — up to 70% of the measured window for `full_mesh` heal at n=8 versus 23% for `bridge`. Fixed by simulating partitions at the application layer (`Replica.SetPeerLinks` / `KickSync`): the runner wires the entire post-heal topology during setup, blocks the cross-group links, and heals by unblocking. Wiring is now one sub-ms RPC round, flat in edge count, still reported as `wiring_ms` so a regression is visible. An in-runner gate asserts the groups actually diverged before healing, so a block-enforcement bug fails loudly instead of reporting a fast heal.

Follow-on: the archived divergence pilot in `data/` predates this and predates automerge 0.10 — re-run before citing it (see NEXT_SESSION.md).

### ~~Reproducibility metadata in the CSV~~ — DONE 2026-08-06
Landed as the proposed provenance file: `--provenance PATH` on the orchestrator (plus `--dry-run`) writes commit hash + dirty flag, host, build profile, node source, per-cell parameters, per-(trial, node) PRNG seeds, and achieved contention; `bench-docker`/`bench-k8s` emit `results-{source}.meta.json` automatically. Citable sweeps are archived with their provenance files under tracked `data/`. Remaining sliver: host OS/kernel + container-runtime versions aren't captured yet — folded into the statistical-rigor item's methodology work if needed.

## Medium-impact — broadens the empirical surface

### Workload diversity beyond `MapPut`
**Text: DONE 2026-08-05/06** — `workload = "text_splice"` with the seeded `locality` axis (append / random_position / same_region) and the 12-cell `divergence-n2-*` grid; backed by the `EnsureText` shared-object bootstrap and the post-convergence text-length gate. **Remaining:** no scenario exercises list insert/splice yet — one list-splice scenario would complete the `Op` enum's coverage.

### Baseline history depth — the third RQ-1 workload axis
The divergence grid is two-dimensional today: `ops_per_group` x `locality`. RQ-1's third axis was "document size/lifetime", which turned out to be five different things. Only two are settable knobs — **baseline length** (visible characters in the converged document when the partition opens) and **baseline history depth** (ops in its history at that instant). Both are pinned at zero: `run_partition_heal` resets, calls `ensure_text_all` on an empty document, blocks the links, and enters the divergence phase, with no prepopulate step. And since `text_splice` hardcodes `del_count: 0` and every op inserts one character, visible length, history depth, and ops-per-side are *the same number* in all 12 cells. That is why size could not be swept: it was not correlated with the divergence axis, it was that axis renamed.

**Decision (2026-08-18): add baseline history depth. Baseline length deferred.** Depth is the axis that discriminates for memory, and it is the honest name for what "lifetime" was gesturing at — a document is not old in seconds, it is old in accumulated operations.

Work:

- A pre-divergence phase in `run_partition_heal`: wire the topology, apply K ops, poll fingerprints to convergence (the machinery exists), *then* block the cross-group links and run the divergence phase as today.
- Enable `del_count > 0`. Without deletes, history depth cannot be decoupled from visible length, so the axis collapses back into baseline length.
- **Reformulate the text-length gate.** It currently asserts `final length == total ops`, which holds only for insert-only workloads. Enabling deletes breaks it, and it is a load-bearing validity check — a silently-discarded side must still fail loudly.
- Deletes also unblock **tombstone diagnostics**, which the proposal names as an RQ-2 mechanism. With `del_count: 0` there are no tombstones, so that diagnostic currently has nothing to measure.

### In-process vs docker/k8s metrics consistency gap
Per-actor OTel metrics (`replicant.sync.messages.tx/rx`, `replicant.doc.size_bytes`, `replicant.op.duration`) only flow into the analysis pipeline for in-process runs. But [[finding_inprocess_artifact_roundrobin]] shows in-process round_robin numbers are artifactually inflated. So trusted convergence numbers (docker/k8s) and trusted per-actor metrics live in different runs. Options:

- Add a `--metrics-file` flag to the replica binary (each pod writes its own file, orchestrator-side concatenation).
- Add a Prometheus-to-JSON snapshot step in the `bench-docker` / `bench-k8s` recipes (queries PromQL just before tearing down the stack).

### Multi-host testbed
Docker compose and kind both run on one host. For the deployment-scenario realism the advisor's framing emphasizes, single-host loopback isn't going to defend well. Phase G2 (cross-AZ on EKS/GKE) on the existing roadmap addresses this — keep it on the critical path.

## Lower-impact — paper cuts

### Sweep parameters are not result columns
`locality` and `ops_per_group` do not appear in the results CSV — they live in the scenario name (`divergence-n2-same_region-1e4`) and in the provenance file, so analysis parses them back out of a string. Promoting them to real columns would remove that parsing step and make cell identity explicit in the data rather than in a naming convention. Cheap; do it when the CSV next changes shape (adding memory will be that moment).

### Notebook robustness
- Cells silently produce stale figure files when renamed (orphaned `full_mesh_scaling.pdf`, the legacy combined `boxplot.pdf`). Document explicit figure-filename invariants or have the cells clean up legacy paths on first run.
- Plots don't auto-adapt to large data shapes — the boxplot needed manual restructuring after the sweep grew to 30+ scenarios. A small-data fast-path + large-data layout would prevent the next churn.

### Notebook split (planned for next session)
The current `convergence.ipynb` is doing four jobs (CSV analysis, OTel JSON ingestion, Prometheus live queries, would-be cross-source comparison). Split into `convergence.ipynb` / `protocol_metrics.ipynb` / `live_metrics.ipynb` / `comparison.ipynb`. Full plan in [`NEXT_SESSION.md`](NEXT_SESSION.md).

---

**Verdict:** the framework's bones can absorb everything in this list as additive work — no item requires redesigning what's there. Order matters though: deferring the second-backend or statistical-rigor items narrows what the framework can credibly claim to contribute, while deferring multi-host or workload-diversity items only narrows the empirical breadth.
