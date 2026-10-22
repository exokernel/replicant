# Scenarios

A scenario is one TOML file under [`scenarios/`](../scenarios/). It sets the
node count, the topology, the write pattern, the workload, and the number of
ops. The orchestrator runs each file as one scenario and measures
`convergence_ms`.

This document describes the scenario fields and how the orchestrator runs them.
The offline-divergence sweep has its own document:
[`divergence-sweep.md`](divergence-sweep.md).

## Orchestration flow

By default, the orchestrator runs every replica as an in-process Tokio task.
With `--replicas`, it connects to replicas that already run elsewhere, for
example the docker compose stack.

Each replica exposes two gRPC services:

- `Replica` for control-plane RPCs.
- `Sync` for peer-to-peer CRDT sync streams.

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

## Topologies

The `connections` field in `[topology]` sets the graph:

- `connections = "full_mesh" | "ring" | "line" | "star"` selects a named
  topology.
- `connections = { edges = [[0,1], ...] }` sets a custom graph.

After each merge, `recv_loop` relays the inbound state to all other connected
peers. Thus non-mesh topologies converge through intermediate hops.

```mermaid
graph LR
    subgraph "full_mesh (diameter 1, n·(n-1)/2 edges)"
        F0((0)) --- F1((1))
        F0 --- F2((2))
        F0 --- F3((3))
        F1 --- F2
        F1 --- F3
        F2 --- F3
    end
    subgraph "ring (diameter ⌊n/2⌋, n edges)"
        R0((0)) --- R1((1))
        R1 --- R2((2))
        R2 --- R3((3))
        R3 --- R0
    end
    subgraph "line (diameter n-1, n-1 edges)"
        L0((0)) --- L1((1))
        L1 --- L2((2))
        L2 --- L3((3))
    end
    subgraph "star (diameter 2, n-1 edges)"
        S0((0))
        S0 --- S1((1))
        S0 --- S2((2))
        S0 --- S3((3))
    end
```

## Partition-heal

A `[partition_heal]` scenario splits the nodes into groups. Each group is fully
connected inside. Each group writes on its own, so the groups diverge. The heal
step then connects the groups. `convergence_ms` starts at the heal.

The partition is simulated at the application layer:

1. The orchestrator wires the **full post-heal topology** before the writes.
2. It blocks the cross-group links. A blocked link drops traffic in both
   directions, but its stream stays open.
3. The heal unblocks the links and starts the sync handshake.

Thus connection setup stays out of the measured window. For the reason, see
[`metrics.md`](metrics.md#wiring_ms-the-residual-setup-inside-the-window).

The `heal_topology` field sets which edges the heal adds:

- **`"full_mesh"`** (default, can be omitted): one edge for each cross-group
  pair. The post-heal graph is `K_n`.
- **`"bridge"`**: one edge only, between `groups[0].nodes[0]` and
  `groups[1].nodes[0]`. This requires exactly 2 groups.

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

**After `heal_topology = "full_mesh"`:** all cross-group edges are added.

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
    A0 --- B4
    A0 --- B5
    A1 --- B3
    A1 --- B4
    A1 --- B5
    A2 --- B3
    A2 --- B4
    A2 --- B5
```

**After `heal_topology = "bridge"`:** one edge, between `groups[0].nodes[0]`
and `groups[1].nodes[0]`.

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
```

The bridge sends all cross-partition state through one edge. It has far fewer
cross edges to flood (1, against N²/4 for full mesh). In the n=4 to n=8
scenarios, bridge heal is **7-21× faster** than full-mesh heal. A line
converges faster than a full mesh in steady state for the same reason. See the
"Partition-heal" section of
[`analysis/convergence.ipynb`](../analysis/convergence.ipynb).

## Write patterns

The `write_pattern` field sets which nodes receive the writes. The bundled
scenarios cover both patterns on every topology. Files for `concentrated` have
the `-concentrated` suffix.

**`round_robin`**: ops cycle through all nodes in scope.

```mermaid
graph LR
    O[Orchestrator]
    O -->|"op 0, 3, 6…"| N0((Node 0))
    O -->|"op 1, 4, 7…"| N1((Node 1))
    O -->|"op 2, 5, 8…"| N2((Node 2))
    N0 --- N1
    N0 --- N2
    N1 --- N2
    linkStyle 0,1,2 stroke:#e67e00,stroke-width:2px
    linkStyle 3,4,5 stroke:#4a9eff,stroke-width:2px
```

**`concentrated`**: all ops go to node 0.

```mermaid
graph LR
    O[Orchestrator]
    O -->|"all ops"| N0((Node 0))
    N0 --- N1((Node 1))
    N0 --- N2((Node 2))
    N1 --- N2
    linkStyle 0 stroke:#e67e00,stroke-width:2px
    linkStyle 1,2,3 stroke:#4a9eff,stroke-width:2px
```

> **Orange** = write op (orchestrator → node) &nbsp; **Blue** = CRDT sync connection (node ↔ node)

## Text workloads

The `workload` field sets what each write does:

- `map_put` (default): a map write. All older scenarios use it.
- `text_splice`: a text insert. It exercises the sequence-CRDT path.

In a partition-heal text scenario, the `locality` field sets where each insert
goes. The position is drawn for each op, against the text length on the
replica that issues it:

- **`append`**: at the end (`pos = len`). This is the low-contention floor.
- **`random_position`**: uniform in `[0, len]`. This exercises positional
  access.
- **`same_region`**: always `pos = 0`. Every op contests the same anchor. This
  is maximal contention.

A seeded PRNG for each replica (SplitMix64) draws the positions. The seed is
`f(cell, replica, trial)`. Thus the op stream is bit-identical across runs and
across CRDT adapters. The run-provenance file records the seeds.

Two mechanisms keep text workloads correct:

- **Shared-object bootstrap** (`Replica.EnsureText`): before the orchestrator
  wires any edge, every replica authors the same bootstrap change (fixed actor,
  time 0). This change creates the text object. Thus all replicas share one
  object identity without a sync.
- **Validity gate** (`Replica.GetTextLength`): after each text trial, the
  runner checks that the final text length equals the total op count on every
  node. This check runs outside the measured window. Equal fingerprints show
  that the replicas agree. Only this check shows that the agreed document
  contains all the work.

## The divergence grid

The `divergence-n2-*` scenarios are a 12-cell grid: {1e2..1e5} ops per side ×
the three localities. Each cell models two replicas that edit offline and then
merge. It is a partition-heal scenario with one node in each group, so
`convergence_ms` is the heal-to-merge time.

A script generates the cells. Do not edit them by hand:

- `just gen-scenarios` writes them.
- `just check-scenarios` fails if a file differs from its generator output.
  `just ci` runs this check.

For what the grid measures and why, see
[`divergence-sweep.md`](divergence-sweep.md).

## Run provenance

`--provenance PATH` writes a JSON provenance record next to the results. It
contains:

- The git commit and a dirty flag.
- The host and the build profile.
- The node source (in-process or external).
- The parameters of each cell.
- The PRNG seed for each (trial, node) pair.
- The achieved contention of each cell (concurrent siblings per anchor). This
  is simulated from the same seeded draws that the runner uses.

`--dry-run` writes the provenance file and runs no trials. The `bench-docker`
and `bench-k8s` recipes write it automatically, as
`results/results-{docker,k8s}.meta.json`.

`run.crdt` names the CRDT library. Read this field on every lane (in-process,
docker, k8s):

- The in-process lane sets it from `--crdt`.
- On an external stack, the orchestrator cannot know the library, because the
  replicas started before it connected. It writes `null`, and the bench
  scripts fill in the value after the run.

Sweeps worth citing are archived, with their provenance files, under
[`data/`](../data/), for example
[`data/divergence-pilot-2026-08-06/`](../data/divergence-pilot-2026-08-06/).
The `results/` directory is gitignored scratch space.
