# Container stacks

The orchestrator can run scenarios against containerized replicas. There are
two stacks, both on a single host:

- docker compose
- a local kind cluster (k8s)

Both stacks run the same replica image, an OTel collector, Prometheus, and
Grafana. Only the recipes and the wiring differ.

## Recipes

| docker compose | kind | What it does |
|---|---|---|
| `just smoke-docker` | `just smoke-k8s` | Build, start, run one trial of one scenario, tear down. Checks the plumbing; not for analysis. |
| `just bench-docker` | `just bench-k8s` | Multi-trial sweep. Writes `results/results-docker.csv` or `results/results-k8s.csv`. |
| `just docker-up` | `just k8s-up` | Start a stack sized for one scenario, and keep it running. On kind, also port-forward each replica pod in the background. |
| | `just k8s-ui` | Port-forward Grafana (`:3000`) and Prometheus (`:9090`) in the foreground. |
| `just docker-reset` | `just k8s-reset` | Wipe Prometheus and replica state. Keep Grafana edits. |
| `just docker-down` | `just k8s-down` | Tear down. |

The `bench-*` recipes manage the stack themselves. Do not run them while a
`*-up` stack is running.

The recipes call `scripts/docker.sh`, `scripts/k8s.sh`, and the `bench-*`
scripts. The shared shell code is in `scripts/lib/`.

Each recipe that starts a stack takes an optional CRDT library as its last
argument. The default is `automerge`.

```sh
just smoke-docker scenarios/full-mesh-n5.toml loro
just bench-docker 'scenarios/divergence-*.toml' 20 yrs
just k8s-up       scenarios/full-mesh-n5.toml automerge
```

The orchestrator cannot see which library the replicas use. Thus the `bench-*`
recipes record the library in the run-provenance file. See
[Run provenance](scenarios.md#run-provenance).

## Run the orchestrator against a stack

The `bench-*` recipes do this for you. To do it by hand, start a stack and pass
its addresses to `--replicas`:

```sh
just docker-up scenarios/full-mesh-n5.toml
cargo run --release --bin orchestrator -- --trials 10 \
  --replicas localhost:50051=replica-0:50051,localhost:50052=replica-1:50051,localhost:50053=replica-2:50051,localhost:50054=replica-3:50051,localhost:50055=replica-4:50051 \
  scenarios/full-mesh-n5.toml scenarios/star-n5.toml
```

`--replicas` takes a comma-separated list of `client_addr[=peer_addr]` entries:

- `client_addr` is the address that the orchestrator dials.
- `peer_addr` is the address that other replicas dial, through the
  `ConnectPeer` call. It defaults to `client_addr`.

The two addresses differ when the replicas are in a different network
namespace from the orchestrator.

A scenario uses the first `node_count` entries and resets the rest. A stack
larger than the scenario therefore works, and one stack can serve scenarios of
several sizes.

The `Replica.Reset` RPC clears replica state between trials and between
scenarios. Thus one orchestrator run can sweep many scenarios, with
`--trials N` trials each. You do not need to restart containers or pods.

## docker compose wiring

- [`deploy/docker/gen-compose.py`](../deploy/docker/gen-compose.py) generates
  the compose YAML from the scenario's `node_count`.
- The orchestrator on the host reaches the replicas on published ports
  (`localhost:5005N`).
- The containers reach each other by service DNS (`replica-N:50051`).

## kind wiring

`just smoke-k8s` does these steps:

1. Create a kind cluster named `replicant`, or reuse one that exists.
2. Apply the manifests in `deploy/k8s/overlays/kind`.
3. Port-forward the replica pods to host ports `50051` and up.
4. Run the scenario.
5. Delete the cluster, but only if this recipe created it.

Set `KEEP_KIND=1` to keep the cluster after the run.

- The manifests are Kustomize bases under `deploy/k8s/base/`. Overlay patches
  are under `deploy/k8s/overlays/`. There is no separate k8s build.
- The replica pods are a `StatefulSet`, so their names are stable: `node-0`,
  `node-1`, and so on. These names match the actor IDs. The pods do not store
  data.
- The CRDT argument sets the `CRDT` env var on the StatefulSet.

## Shared configuration

The otel-collector and Prometheus configs are in
[`deploy/shared/`](../deploy/shared/). Compose bind-mounts them, and k8s loads
them as ConfigMaps. Thus both stacks read the same files.

## Dashboard

Both stacks provision one Grafana dashboard,
[`replicant.json`](../deploy/shared/grafana/dashboards/replicant.json). It has
four panels:

- Document size convergence
- Op latency p50/p95
- Sync messages tx/rx per actor
- Sync edge inventory

Open it at `http://localhost:3000` (admin/admin). On kind, run `just k8s-ui`
first.

You can edit the dashboard in the browser. Edits persist across container
restarts and across `*-reset`. `*-down` deletes them.

Use the dashboard for live debugging and demos. The analysis notebooks are the
source of truth for reported numbers.
