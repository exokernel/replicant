# Replicant Metric Schema

This document is a contract. Metric names, types, units, and attribute keys
defined here must be used identically by every `CrdtAdapter` implementation
and by the orchestrator. Changing a name here is a breaking change to
collected data.

## Common attributes

All attributes are strings. These appear on most metrics.

| Attribute   | Example values                                                         | Description                                                       |
|-------------|------------------------------------------------------------------------|-------------------------------------------------------------------|
| `actor`     | `replica-a`, `replica-b`                                               | Stable replica ID, set explicitly at startup — never a random UUID. |
| `op`        | `map_put`, `map_delete`, `list_insert`, `list_delete`, `list_splice`, `text_splice` | Operation type, matching `OpRequest` oneof field names.  |
| `result`    | `ok`, `error`                                                          | Outcome of an operation.                                          |
| `direction` | `send`, `receive`                                                      | Direction of a sync message.                                      |
| `peer`      | `replica-b`                                                            | The peer involved in a sync exchange.                             |

---

## Comparable metrics

These metrics use identical names and semantics across all `CrdtAdapter`
implementations. Timing is recorded in the scaffolding layer, *outside* the
adapter, so the measured boundary is the same for every library. None of
these metric names contain a library name — that is intentional.

### Op metrics

| Name                        | Type      | Unit | Attributes           | Description                                                                      |
|-----------------------------|-----------|------|----------------------|----------------------------------------------------------------------------------|
| `replicant.op.duration_us`  | Histogram | µs   | `actor`, `op`        | Wall time for one `apply_op` call. Measures CRDT library time only; excludes gRPC overhead. |
| `replicant.op.count`        | Counter   | ops  | `actor`, `op`, `result` | Total operations applied.                                                     |

### Sync metrics

| Name                                  | Type      | Unit     | Attributes                      | Description                                                              |
|---------------------------------------|-----------|----------|---------------------------------|--------------------------------------------------------------------------|
| `replicant.sync.message.duration_us`  | Histogram | µs       | `actor`, `direction`            | Wall time to generate or receive one sync message.                       |
| `replicant.sync.bytes`                | Counter   | bytes    | `actor`, `direction`, `peer`    | Bytes sent or received in sync messages. Measures replication bandwidth. |
| `replicant.sync.messages`             | Counter   | messages | `actor`, `direction`, `peer`    | Number of sync messages exchanged.                                       |

### Document state metrics

Sampled by each replica approximately every 1 second. Not emitted per-op.

| Name                          | Type  | Unit    | Attributes | Description                                                              |
|-------------------------------|-------|---------|------------|--------------------------------------------------------------------------|
| `replicant.doc.size_bytes`    | Gauge | bytes   | `actor`    | Serialized document size (`save()` byte length).                         |
| `replicant.doc.heads_count`   | Gauge | heads   | `actor`    | Number of current document heads (width of the DAG frontier).            |
| `replicant.doc.changes_total` | Gauge | changes | `actor`    | Total changes in the document DAG.                                       |

### Convergence metrics

Emitted by the **orchestrator**, not by replicas.

| Name                                | Type      | Unit   | Attributes | Description                                                                                   |
|-------------------------------------|-----------|--------|------------|-----------------------------------------------------------------------------------------------|
| `replicant.convergence.latency_us`  | Histogram | µs     | —          | Time from end of workload burst to all replicas reporting equal `GetStateFingerprint` values. |
| `replicant.convergence.rounds`      | Histogram | rounds | —          | Sync round-trips until quiescence after a workload burst.                                     |

---

## Diagnostic metrics

These metrics diagnose *why* a library performs the way it does. They are
**not** comparable across libraries and live in a library-specific namespace.
Each adapter emits its own via `emit_internal_metrics(&meter)`.

Naming pattern: `replicant.<library>.<signal>`

| Name                                    | Type      | Unit | Attributes | Description                                      |
|-----------------------------------------|-----------|------|------------|--------------------------------------------------|
| `replicant.automerge.save.duration_us`  | Histogram | µs   | `actor`    | Time for `AutoCommit::save()` (full serialization). |

Add entries here as profiling reveals interesting internal signals.

---

## Recommended histogram boundaries

Default OTel bucket boundaries are poorly suited to µs-scale measurements.
Configure these explicitly in the SDK or collector pipeline.

| Metric group           | Boundaries (µs)                                              |
|------------------------|--------------------------------------------------------------|
| Op duration            | 1, 5, 10, 25, 50, 100, 250, 500, 1 000, 5 000, 10 000       |
| Sync message duration  | 10, 50, 100, 250, 500, 1 000, 5 000, 10 000, 50 000         |
| Convergence latency    | 1 000, 5 000, 10 000, 50 000, 100 000, 500 000, 1 000 000   |

---

## Collection topology

```
Replicas      ──OTLP push──▶ ┐
                              ├──▶ otelcol ──▶ JSON / Parquet files ──▶ pandas
Orchestrator  ──OTLP push──▶ ┘
```

Both replicas and the orchestrator push to the same `otelcol` endpoint.
The collector is a single process on localhost (prototype) or a container
sidecar (docker-compose / k8s). After the benchmark run, output files are
pulled for offline analysis.

No Prometheus or Grafana required for the prototype.

---

## Convergence measurement protocol

Convergence latency is measured from the orchestrator's clock throughout, so
no cross-replica clock synchronization is required.

1. Run the workload burst (orchestrator sends `ApplyOp` calls to replicas).
2. Stop sending new ops. Record wall time **T₀**.
3. Poll `GetStateFingerprint` on all replicas at a fixed interval (e.g. 10 ms).
4. When all replicas return identical fingerprint bytes, record wall time **T₁**.
5. Emit `replicant.convergence.latency_us = T₁ − T₀`.

The fingerprint is opaque to the orchestrator — it just compares byte equality.
For Automerge, the `AutomergeAdapter` returns sorted, concatenated `get_heads()`
bytes. Two replicas with equal fingerprints have the same DAG frontier and have
therefore converged.
