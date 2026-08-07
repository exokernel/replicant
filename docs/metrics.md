# Replicant Metric Schema

This document is a contract. Metric names, types, units, and attribute keys
defined here must be used identically by every `CrdtAdapter` implementation
and by the orchestrator. Changing a name here is a breaking change to
collected data.

The metrics inventory is intentionally small. Each instrument earns its
place by answering a specific thesis question. Add a new metric only when
an existing one cannot.

## Conventions

All metric names use the `replicant.*` namespace. Attribute values are
strings. Units are recorded on the instrument via OTel's `with_unit` builder
so consumers don't need to infer them from names.

| Attribute | Example values                                              | Description                                                       |
|-----------|-------------------------------------------------------------|-------------------------------------------------------------------|
| `actor`   | `node-0`, `node-1`                                          | Stable replica ID, set explicitly at startup — never random.      |
| `peer`    | `node-1`                                                    | The peer involved in a sync exchange.                             |
| `op`      | `map_put` (currently the only op type emitted)              | Operation type, matching `OpRequest` oneof field names.           |

---

## Emitted metrics

All five are emitted by the **replica** binary; the orchestrator emits no
OTel metrics of its own (it consumes them via the file exporter or
Prometheus). Defined in [`crates/replica/src/metrics.rs`](../crates/replica/src/metrics.rs);
recorded at call sites in [`crates/replica/src/server.rs`](../crates/replica/src/server.rs).

### Op metrics

| Name                       | Type              | Unit | Attributes      | Description                                                          |
|----------------------------|-------------------|------|-----------------|----------------------------------------------------------------------|
| `replicant.op.duration`    | Histogram<f64>    | ms   | `actor`, `op`   | Wall time for one `apply_op` call. CRDT library + commit only; excludes gRPC overhead. |

### Sync metrics

`tx` and `rx` are split into two separate counters rather than a single
counter with a `direction` attribute. This keeps Prometheus queries simple
(`sum(replicant_sync_messages_tx_total) by (actor)` without needing a
filter) and matches what the analysis notebook expects.

| Name                              | Type            | Unit | Attributes        | Description                                  |
|-----------------------------------|-----------------|------|-------------------|----------------------------------------------|
| `replicant.sync.messages.tx`      | Counter<u64>    | —    | `actor`, `peer`   | Outbound sync messages sent to a peer.       |
| `replicant.sync.messages.rx`      | Counter<u64>    | —    | `actor`, `peer`   | Inbound sync messages received from a peer.  |
| `replicant.sync.messages.deferred`| Counter<u64>    | —    | `actor`, `peer`   | Flushes skipped because the peer's outbound channel was full. |

`deferred` should normally be zero. A flush cannot apply backpressure —
`flush_to_peers` runs inside `recv_loop`, so blocking on a full channel would
deadlock two replicas that are each waiting on the other — so it reserves
channel capacity before generating and skips the flush when there is none. The
change stays pending for the next flush rather than being lost, but a non-zero
count means a peer was not draining fast enough and that run's convergence
timings include the resulting stalls. Treat it as a validity check on a sweep,
alongside the text-length gate.

**Reading the check on a docker sweep.** `just bench-docker` queries Prometheus
for this counter before it tears each node-count group down, and writes the
result to the group's record in `results-docker.meta.json`:

```json
"deferred": {
  "window": {"start": 1754538000, "end": 1754539500},
  "tx_total": 4820,
  "deferred_total": 0,
  "deferred_series": []
}
```

An OTel counter emits no data points until its first increment. A clean run
therefore has **no deferred series at all**, and `deferred_total: 0` is derived
from an empty result — not from a series that reads zero. Absence alone cannot
be told apart from a metrics pipeline that never delivered, so `tx_total` rides
along as the liveness proof. Read the pair together:

| `tx_total` | `deferred_total` | Meaning                                            |
|------------|------------------|----------------------------------------------------|
| `> 0`      | `0`              | Verified clean. Timings contain no deferral stalls. |
| `> 0`      | `> 0`            | Timings include stalls. `deferred_series` holds the per-2s samples, so the burst can be placed in time. |
| `0`        | `0`              | Nothing was collected. The check did not run — do not read it as clean. |
| absent, or `error` key | — | Prometheus was unreachable. The sweep still completed; the validity signal did not. |

The counter is scoped to a whole node-count group, not to one trial, because
the group is one orchestrator invocation against one stack. Use
`deferred_series` timestamps, not the total, to attribute a spike to a cell.
`just bench-k8s` does not capture this yet.

### Document state metrics

Sampled on every local op application AND on every received sync message,
so post-convergence the gauge reflects each replica's final save() byte
length. See [[finding_automerge_save_not_canonical]] in the analysis notebook
for why these values can differ across logically-converged replicas.

| Name                       | Type           | Unit  | Attributes | Description                                          |
|----------------------------|----------------|-------|------------|------------------------------------------------------|
| `replicant.doc.size_bytes` | Gauge<u64>     | By    | `actor`    | Serialized document size (`AutoCommit::save()` byte length). |

---

## Convergence is not an OTel metric

Convergence latency is the thesis's headline measurement, but it is **not**
emitted as an OTel instrument. The orchestrator measures convergence
externally and writes it to its CSV / JSON Lines output as a `convergence_ms`
column. Reasons:

- Convergence is a *whole-cluster* property, not per-replica. OTel
  instruments naturally attach to per-actor attributes; convergence has no
  natural `actor` tag.
- The orchestrator already produces structured per-trial output to stdout.
  Reporting convergence as a column there keeps the analysis pipeline
  uniform — every measurement lives in `results/results.csv`, queryable by
  pandas without joining against the OTel snapshot.

See [`crates/orchestrator/src/runner.rs`](../crates/orchestrator/src/runner.rs)
for the measurement protocol — fingerprint-poll loop after the workload
burst until all replicas report identical `GetStateFingerprint`.

### `wiring_ms`: the residual setup inside the window

Partition-heal runs simulate the partition at the application layer rather than
by withholding connections. The orchestrator wires the *entire* post-heal
topology during setup, blocks every cross-group link via `SetPeerLinks`, runs
the divergence phase, and heals by unblocking. So no connection is established
inside the timed window; `wiring_ms` is the one concurrent round of unblock RPCs
that starts the heal, and it does not scale with the number of healed edges.
`mean_wiring_ms` is the per-scenario mean. Topology runs report `wiring_ms = 0`.

**This was not always true, and the difference mattered.** When the heal *was*
the wiring, opening streams was up to 70% of the measured window on the docker
lane — and it scaled with edge count, so a `FullMesh` heal paid it on every
cross-group pair while a `Bridge` heal paid it once. Comparing the two compared
connection setup as much as merge cost. Any partition-heal measurement taken
before commit `d77d3f6` carries that bias; see the retraction in the analysis
notes.

A blocked link is enforced on both endpoints: outbound flushes skip the peer
(never consuming sync protocol state toward it) and inbound messages are dropped
unprocessed. Unblocking discards the per-peer sync protocol state, so the heal
starts from a fresh handshake rather than trusting beliefs formed while the link
was down. The runner asserts the partition actually held — no two groups may
share a fingerprint at the end of the divergence phase — for the same reason the
text-length gate exists: a silently-leaking partition reports a fast heal that
looks like a result rather than a broken experiment.

---

## Collection topology

The replicas push OTel data via OTLP gRPC to an `otelcol` endpoint, which
fans out to two consumption paths used by the analysis notebook:

```
                                       ┌──▶ File exporter ──▶ results/metrics-*.json ──▶ pandas (offline)
Replicas ──OTLP push──▶ otelcol ───────┤
                                       └──▶ Prometheus exporter (/metrics on :8889) ──▶ Prometheus scrape ──▶ PromQL (live)
```

- **Offline path** (used for all thesis-table data): orchestrator runs with
  `--metrics-file results/metrics-<scenario>.json` per scenario. The file
  exporter writes one JSON Lines record per PeriodicReader flush. The
  notebook's "OTel Protocol Metrics" section loads these.
- **Live path** (used for ad-hoc Prometheus inspection): `just docker-up
  <scenario>` brings up Prometheus alongside the replicas; it scrapes
  the collector's `:8889` endpoint every 2s. The notebook's
  "Prometheus-backed metrics (live stack)" section queries PromQL.

Both paths are derived from the same OTLP stream, so the metric names and
attribute semantics are identical across them. The k8s deployment uses the
same collector pipeline (`deploy/k8s/base/otel-collector-{configmap,deployment,svc}.yaml`).

---

## Diagnostic metrics (per-adapter, future work)

Comparable metrics above use identical names and semantics across all
`CrdtAdapter` implementations. Library-specific diagnostic metrics
(`replicant.<library>.<signal>`) are not currently emitted but would live
in a separate `emit_internal_metrics(&meter)` hook on the adapter trait
when we add a second adapter.

Candidates that would earn their place:

- `replicant.automerge.save.duration_us` — cost of `AutoCommit::save()`.
  Worth adding if op latency histograms start showing `save()` as the
  dominant term (currently it's not, but the call is now on the
  sync_receive path too — see the doc_size_bytes overhead note in
  [NEXT_SESSION.md](../NEXT_SESSION.md)).
- `replicant.sync.bytes` — replication bandwidth in addition to message
  counts. Useful for distinguishing "many small messages" from "few large
  messages" at the topology level.

Add entries here only after the code emits them. Don't pre-document
aspirational metrics — this file was previously full of them and went stale.

---

## Adding a new metric

1. Declare the instrument in `crates/replica/src/metrics.rs` (`Metrics`
   struct) with a `replicant.*` name, OTel type, unit, and the attribute
   keys it uses.
2. Build it in `Metrics::new`.
3. Record it at the relevant call site in `server.rs` (or wherever the
   measurement boundary lives — keep recordings **outside** the
   `CrdtAdapter` trait so adapters stay purely functional).
4. Add a row to the appropriate table above.
5. If the metric is consumed by the notebook, update
   [analysis/convergence.ipynb](../analysis/convergence.ipynb) to load and
   plot it.

Keep this doc and the code synchronized — every metric name in the tables
above must `grep` to a real `replicant.<...>` string in `crates/replica/`.
