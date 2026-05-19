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

All four are emitted by the **replica** binary; the orchestrator emits no
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

| Name                            | Type            | Unit | Attributes        | Description                                  |
|---------------------------------|-----------------|------|-------------------|----------------------------------------------|
| `replicant.sync.messages.tx`    | Counter<u64>    | —    | `actor`, `peer`   | Outbound sync messages sent to a peer.       |
| `replicant.sync.messages.rx`    | Counter<u64>    | —    | `actor`, `peer`   | Inbound sync messages received from a peer.  |

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
