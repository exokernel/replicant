# Replicant

```
 ____  _____ ____  _     ___ ____    _    _   _ _____
|  _ \| ____|  _ \| |   |_ _/ ___|  / \  | \ | |_   _|
| |_) |  _| | |_) | |    | | |     / _ \ |  \| | | |
|  _ <| |___|  __/| |___ | | |___ / ___ \| |\  | | |
|_| \_\_____|_|   |_____|___\____/_/   \_\_| \_| |_|
```

> *"All those moments will be lost in time, like tears in rain..."*
> — Roy Batty, *Blade Runner* (1982)
>
> ...unless you measure them.

A CRDT benchmarking framework built on [Automerge](https://automerge.org/) and gRPC. Replicant spins up replica nodes, drives sync workloads between them, and collects latency/throughput metrics via OpenTelemetry.

> [!IMPORTANT]
> **Work in progress.** Replicant is an early-stage research framework. Any specific numbers or patterns surfaced by the notebooks are descriptions of what one or two sweeps produced on a single host — not claims about CRDT performance in general. Treat figures as illustrative; see [TODO.md](TODO.md) for the framework gaps (second CRDT backend, statistical rigor, reproducibility metadata, multi-host) that need to land before any of it would be defensible as more than that.

## Crates

| Crate | Role |
|---|---|
| `common` | Protobuf-generated types and shared gRPC stubs |
| `replica` | Automerge replica with a gRPC server and OTel metrics export |
| `orchestrator` | Launches replicas, drives workloads, and runs the smoke test |

## Quick start

In-process (no Docker needed; the fastest path for local development):

```sh
just          # list all available recipes
just smoke    # run the three built-in regression scenarios (2, 3, and 4 nodes)
just ci       # full gate: fmt check → lint → test → smoke
just docs     # build and open rustdoc
```

## Containerized run

The orchestrator can drive a stack of containerized replicas wired to a real OTel collector and Prometheus. Two runtimes are supported: docker compose (single host) and a local kind cluster (k8s, single host). Both run the same replica image; only the recipe and wiring differ.

`--replicas` accepts comma-separated `client_addr[=peer_addr]` entries — the first is what the orchestrator dials, the second (defaults to the first when omitted) is what each replica passes to its peers in `ConnectPeer`. The two diverge whenever replicas live in a different network namespace from the orchestrator.

Replica state is cleared between trials and between scenarios via a `Replica.Reset` RPC, so a single orchestrator invocation against an externally-managed stack can sweep multiple scenarios with `--trials N` — no need to bounce containers/pods between runs. The `just bench-docker` and `just bench-k8s` recipes wrap this: one stack per distinct `node_count` bucket, Reset between trials within a bucket, output to `results/results-{docker,k8s}.csv` for the analysis notebooks.

### docker compose

`just smoke-docker` is the one-liner end-to-end check:

```sh
just smoke-docker    # builds the replica image, brings up 5 replicas +
                     # otel-collector + prometheus + grafana, runs
                     # full-mesh-n5 against them, tears the stack down
```

For longer interactive sessions, leave the stack running:

```sh
just docker-up scenarios/full-mesh-n5.toml      # build → up; stays running
# Grafana:    http://localhost:3000  (admin/admin)
# Prometheus: http://localhost:9090

# Run one or many scenarios (Replica.Reset clears state between trials and
# between scenarios — no container bounce needed):
cargo run --release --bin orchestrator -- --trials 10 \
  --replicas localhost:50051=replica-0:50051,localhost:50052=replica-1:50051,localhost:50053=replica-2:50051,localhost:50054=replica-3:50051,localhost:50055=replica-4:50051 \
  scenarios/full-mesh-n5.toml scenarios/star-n5.toml

# Or use the sweep recipe (wraps stack lifecycle + Reset + CSV emit):
just bench-docker "scenarios/*-n5*.toml" 10      # writes results/results-docker.csv

just docker-reset                               # wipe Prometheus + replica
                                                # state, keep Grafana edits
just docker-down                                # tear down
```

Wiring: the orchestrator on the host reaches replicas via published ports (`localhost:5005N`); containers resolve each other via service DNS (`replica-N:50051`). The compose YAML is regenerated per scenario from [`deploy/docker/gen-compose.py`](deploy/docker/gen-compose.py); configs for otel-collector and prometheus live in [`deploy/shared/`](deploy/shared/) and are bind-mounted by compose / loaded as ConfigMaps in k8s, so both stacks read the same source.

### kind cluster (local k8s)

`just smoke-k8s` brings up a single-host kind cluster, applies the manifests in `deploy/k8s/overlays/kind`, port-forwards the 5 replica pods to host ports `50051..50055`, runs full-mesh-n5, and tears the cluster down:

```sh
just smoke-k8s                    # build → kind create → load → apply →
                                  # port-forwards → orchestrator → teardown
KEEP_KIND=1 just smoke-k8s        # preserve the cluster for debugging
```

For longer interactive sessions:

```sh
just k8s-up scenarios/full-mesh-n5.toml         # idempotent: rescales in place;
                                                # also sets up the per-pod
                                                # `localhost:5005N` port-forwards
                                                # in the background so the
                                                # orchestrator can dial each pod
just k8s-ui                                     # foreground port-forwards:
                                                # Grafana :3000, Prometheus :9090
just k8s-reset                                  # wipe Prometheus + replica
                                                # state, keep Grafana edits
just k8s-down                                   # tear down

# Multi-trial sweep (uses Replica.Reset, no pod bounces):
just bench-k8s "scenarios/*-n5*.toml" 10        # writes results/results-k8s.csv
```

Manifests are organised as Kustomize bases under `deploy/k8s/base/` with overlay-specific patches under `deploy/k8s/overlays/`. The same image runs in both runtimes (no separate "k8s build"). Replica pods are a `StatefulSet` for stable identity (`node-0`…`node-4` matching the actor scheme) — they are not stateful for storage.

### Dashboards

Both stacks ship with a provisioned Grafana dashboard at [`deploy/shared/grafana/dashboards/replicant.json`](deploy/shared/grafana/dashboards/replicant.json) — four panels covering document size convergence, op latency p50/p95, sync messages tx/rx per actor, and sync edge inventory. Reachable at `http://localhost:3000` (admin/admin) when the stack is up via `docker-up` or `k8s-ui`. The dashboard is editable in-browser; edits persist across container restarts and across `*-reset` (which only wipes Prometheus tsdb + replica state), and are wiped by `*-down` if you want a fully clean slate. Useful for live debugging and demos; the analysis notebooks (especially [`analysis/convergence.ipynb`](analysis/convergence.ipynb)) remain the source of truth for the numbers that go into the write-up.

## Analysis

Four Jupyter notebooks under `analysis/`, one per question — see [`analysis/README.md`](analysis/README.md) for the pointer table:

- [`convergence.ipynb`](analysis/convergence.ipynb) — CSV-driven plots. Pick `SOURCE = "in_process" | "docker" | "k8s"`.
- [`protocol_metrics.ipynb`](analysis/protocol_metrics.ipynb) — OTel JSON files (in-process only — see notebook header for why).
- [`live_metrics.ipynb`](analysis/live_metrics.ipynb) — live PromQL on a running stack.
- [`comparison.ipynb`](analysis/comparison.ipynb) — cross-source view; default `INCLUDE = ["docker", "k8s"]`.

**Offline path** (CSV from a finished sweep, no live stack required): three CSVs feed the notebooks depending on `SOURCE`:

```sh
mkdir -p results

# in_process — same orchestrator process as the replicas. Cheapest, but
# round_robin on relay-heavy topologies is artifactually slow here; see
# analysis/comparison.ipynb.
cargo run --release --bin orchestrator -- --trials 10 --output csv \
  scenarios/*.toml > results/results.csv

# docker / k8s — externally-managed stacks, multi-scenario + multi-trial via
# the Replica.Reset RPC. The recipes own the stack lifecycle (one stack per
# distinct node_count bucket) and write the per-source CSV the notebook reads.
just bench-docker "scenarios/*.toml" 10                  # results/results-docker.csv
just bench-k8s    "scenarios/*.toml" 10                  # results/results-k8s.csv

# Per-scenario OTel JSON for protocol_metrics.ipynb (in_process only — one
# invocation per scenario so counters don't accumulate across scenarios):
for s in $(ls scenarios/*.toml | xargs -n1 basename -s .toml); do
  cargo run --release --bin orchestrator -- --trials 10 \
    --metrics-file "results/metrics-${s}.json" --output csv \
    "scenarios/${s}.toml" > /dev/null 2>&1
done

cd analysis && jupyter lab
```

Each CSV is cached alongside it as `<stem>.parquet` and refreshed when the CSV is newer. The `results/` directory is gitignored.

**Live path** (Prometheus-backed): bring up a containerized stack (`just docker-up` or `just k8s-up` + `just k8s-ui`), run a scenario against it (typically a paced one — see [`scenarios/full-mesh-n5-paced.toml`](scenarios/full-mesh-n5-paced.toml)), then open `analysis/live_metrics.ipynb` and Run All. It queries Prometheus directly via PromQL — useful for live demos and for asserting the structural invariant that all replicas converge to the same `doc_size_bytes`.

## Requirements

- Rust (toolchain pinned via `rust-toolchain.toml` to 1.95.0; rustup auto-installs)
- [`just`](https://github.com/casey/just)
- `protoc` (Protocol Buffers compiler)
- Docker + Compose v2 (optional, for `smoke-docker`)
- [`kind`](https://kind.sigs.k8s.io/) + `kubectl` (optional, for `smoke-k8s`)

## Scenarios

### Orchestration flow

By default the orchestrator runs all replicas as in-process Tokio tasks (no
subprocesses); with `--replicas` it instead connects to externally-managed
replicas (e.g. the docker-compose stack above). Either way each node exposes
two gRPC services: `Replica` for control-plane RPCs and `Sync` for
peer-to-peer Automerge sync streams.

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

### Topology variants

Scenario TOML files set `connections = "full_mesh" | "ring" | "line" | "star"` for the named topologies, or `connections = { edges = [[0,1], ...] }` for arbitrary custom graphs. `recv_loop` relays inbound state to all other connected peers after each merge, so non-mesh topologies converge through intermediate hops.

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

### Partition-heal topology

Nodes are split into groups that are fully connected internally. Each group
writes independently, accumulating divergent history. The heal step adds
cross-group edges; `convergence_ms` is measured from that point.

The `heal_topology` field in `[partition_heal]` selects what gets reconnected:

- **`heal_topology = "full_mesh"`** (default, omittable) — every cross-group pair gets an edge; post-heal graph is `K_n`.
- **`heal_topology = "bridge"`** — only one edge is added, between `groups[0].nodes[0]` and `groups[1].nodes[0]`. Requires exactly 2 groups.

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

**After `heal_topology = "full_mesh"` — every cross-group edge added:**

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

**After `heal_topology = "bridge"` — single edge between `groups[0].nodes[0]` and `groups[1].nodes[0]`:**

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

The bridge variant forces all cross-partition state through one edge. With far fewer cross-edges flooding (1 vs N²/4), bridge heal is **7-21× faster** than full-mesh heal in the n=4-8 scenarios — same edges-vs-diameter mechanism as the line-vs-full-mesh steady-state result above. See the notebook's "Partition-heal" section.

### Write patterns

Two write distributions are supported via the `write_pattern` field in scenario
TOML files. Bundled scenarios cover both patterns across every topology — see
the `-concentrated` suffix on filenames in [scenarios/](scenarios/).

**`round_robin`** — ops cycle through all nodes in scope:

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

**`concentrated`** — all ops go to node 0:

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

> **Orange** = write op (orchestrator → node) &nbsp; **Blue** = Automerge sync connection (node ↔ node)
