# Replicant

A CRDT benchmarking framework built on [Automerge](https://automerge.org/) and gRPC. Replicant spins up replica nodes, drives sync workloads between them, and collects latency/throughput metrics via OpenTelemetry.

## Crates

| Crate | Role |
|---|---|
| `common` | Protobuf-generated types and shared gRPC stubs |
| `replica` | Automerge replica with a gRPC server and OTel metrics export |
| `orchestrator` | Launches replicas, drives workloads, and runs the smoke test |

## Quick start

```sh
just          # list all available recipes
just smoke    # run the three built-in regression scenarios (2, 3, and 4 nodes)
just ci       # full gate: fmt check → lint → test → smoke
just docs     # build and open rustdoc
```

## Requirements

- Rust (stable, edition 2024)
- [`just`](https://github.com/casey/just)
- `protoc` (Protocol Buffers compiler)

## Scenarios

### Orchestration flow

The orchestrator runs all replicas as in-process Tokio tasks (no subprocesses).
Each node exposes two gRPC services: `Replica` for control-plane RPCs and `Sync`
for peer-to-peer Automerge sync streams.

```mermaid
sequenceDiagram
    participant O as Orchestrator
    participant A as Node 0
    participant B as Node 1
    participant C as Node 2

    O->>A: spawn (in-process)
    O->>B: spawn (in-process)
    O->>C: spawn (in-process)

    O->>A: ConnectPeer(B)
    A-->>B: open bidi sync stream
    O->>A: ConnectPeer(C)
    A-->>C: open bidi sync stream
    O->>B: ConnectPeer(C)
    B-->>C: open bidi sync stream

    loop round-robin writes
        O->>A: ApplyOp
        A-->>B: sync message (flush_to_peers)
        A-->>C: sync message (flush_to_peers)
        B-->>A: sync reply
        C-->>A: sync reply
    end

    loop poll until converged
        O->>A: GetFingerprint
        O->>B: GetFingerprint
        O->>C: GetFingerprint
    end
    Note over O: all fingerprints match → record convergence_ms
```

### Full-mesh topology

All nodes are connected to every other node before writes begin.
`flush_to_peers` after each local op delivers changes in one hop.

```mermaid
graph LR
    N0((Node 0)) --- N1((Node 1))
    N0 --- N2((Node 2))
    N0 --- N3((Node 3))
    N0 --- N4((Node 4))
    N1 --- N2
    N1 --- N3
    N1 --- N4
    N2 --- N3
    N2 --- N4
    N3 --- N4
```

### Partition-heal topology

Nodes are split into groups that are fully connected internally. Each group
writes independently, accumulating divergent history. The heal step adds
cross-group edges; `convergence_ms` is measured from that point.

**Partitioned (writes in progress):**

```mermaid
graph LR
    subgraph "Group A (nodes 0–2)"
        A0((Node 0)) --- A1((Node 1))
        A0 --- A2((Node 2))
        A1 --- A2
    end
    subgraph "Group B (nodes 3–5)"
        B3((Node 3)) --- B4((Node 4))
        B3 --- B5((Node 5))
        B4 --- B5
    end
```

**After heal (cross-group edges added):**

```mermaid
graph LR
    subgraph "Group A"
        A0((Node 0)) --- A1((Node 1))
        A0 --- A2((Node 2))
        A1 --- A2
    end
    subgraph "Group B"
        B3((Node 3)) --- B4((Node 4))
        B3 --- B5((Node 5))
        B4 --- B5
    end
    A0 --- B3
    A1 --- B4
    A2 --- B5
```
