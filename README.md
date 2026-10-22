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

[![CI](https://github.com/exokernel/replicant/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/exokernel/replicant/actions/workflows/ci.yml)

Replicant compares CRDT libraries on the same synthetic workloads. It starts
replica nodes, drives writes and sync between them over gRPC, and measures how
long they take to converge. One scenario file runs against each library, so
the workload stays fixed.

Each library sits behind one `CrdtAdapter` trait. The `--crdt` flag selects
the library:

| `--crdt` | Library | List/text algorithm |
|---|---|---|
| `automerge` (default) | [Automerge](https://automerge.org/) | RGA-descended |
| `yrs` | [Yrs](https://github.com/y-crdt/y-crdt) (Rust port of [Yjs](https://yjs.dev/)) | YATA |
| `loro` | [Loro](https://loro.dev/) | Fugue-descended |

> [!IMPORTANT]
> **Work in progress.** Replicant is an early-stage research framework.

## Requirements

- Rust. `rust-toolchain.toml` pins 1.99.0, and rustup installs it automatically.
- [`just`](https://github.com/casey/just)
- `protoc` (Protocol Buffers compiler)
- Optional, for the container stacks:
  - Docker with Compose v2, or [`kind`](https://kind.sigs.k8s.io/) with
    `kubectl`.
  - bash 4 or later. macOS ships bash 3.2; install a newer one with
    `brew install bash`.
  - `jq` and `curl`, for the bench recipes.
- Python 3, for the notebooks and the deploy tooling. `just setup` creates
  `.venv` and installs `requirements.txt` into it. With
  [mise](https://mise.jdx.dev), entering the repository activates `.venv`.

## Quick start

These recipes run the replicas in-process. They do not need Docker.

```sh
just          # list all recipes
just smoke    # run the three built-in regression scenarios (2, 3, and 4 nodes)
just ci       # full gate: fmt check, lint, test, smoke, scenario-grid check
just docs     # build and open rustdoc
```

To run against containerized replicas, with Prometheus and Grafana:

```sh
just smoke-docker scenarios/full-mesh-n5.toml loro   # or: just smoke-k8s
```

See [`docs/deployment.md`](docs/deployment.md) for the docker compose and kind
stacks.

## Crates

| Crate | Role |
|---|---|
| `common` | Protobuf-generated types, the `Op` model, and the `CrdtAdapter` trait |
| `replica` | CRDT replica (Automerge / Yrs / Loro) with a gRPC server and OTel metrics export |
| `orchestrator` | Launches replicas, drives workloads, and measures convergence. See [its README](crates/orchestrator/README.md) for flags and output format. |

## Scenarios

A scenario is a TOML file under [`scenarios/`](scenarios/). For example,
[`scenarios/full-mesh-n3.toml`](scenarios/full-mesh-n3.toml):

```toml
name = "full-mesh-n3"

[topology]
node_count = 3
connections = "full_mesh"
write_pattern = "round_robin"
op_count = 6
```

## Documentation

| Document | Contents |
|---|---|
| [`docs/scenarios.md`](docs/scenarios.md) | Scenario fields, topologies, partition-heal, write patterns, text workloads, run provenance |
| [`docs/deployment.md`](docs/deployment.md) | The docker compose and kind stacks, and the Grafana dashboard |
| [`docs/metrics.md`](docs/metrics.md) | The metric schema |
| [`docs/divergence-sweep.md`](docs/divergence-sweep.md) | The offline-divergence sweep: what it measures, its grid, its validity gates |
| [`analysis/README.md`](analysis/README.md) | The guided notebook, which runs scenarios live, and the archived analysis notebooks |

## Adding a CRDT backend

A backend has these parts:

- One implementation of the `CrdtAdapter` trait (`crates/common/src/lib.rs`).
- One module under `crates/replica/src/adapter/`.
- One variant in `replica::adapter::Crdt`. Its doc comment lists the rest of
  the Rust changes.
- The library name in the accepted-value lists in the `Justfile`,
  `deploy/docker/gen-compose.py`, and `scripts/lib/stack.sh`.

The rest of the code is generic over the trait.

**An adapter is done when it passes the conformance suite**,
`crates/replica/src/adapter/conformance.rs`. The suite holds test functions
that are generic over `A: CrdtAdapter + Default`. Each adapter module wraps the
functions that apply to it in a `#[test]`. There are two kinds:

- **Universal**: every adapter must pass it.
- **Characterization** (marked `ACCOMMODATION`): only some libraries pass it.
  An adapter that fails it does not skip it. The adapter adds a mirror test
  that asserts what it does instead.

A function is universal until an adapter shows otherwise. When a second
adapter needs the same mirror test, move the mirror into the suite. It is
universal if every adapter passes it; otherwise mark it `ACCOMMODATION` too.

Check these two points early:

- **Can the library's version identify converged state?** Automerge heads and
  Loro frontiers can. A Yrs state vector cannot, because a Yrs delete does not
  advance a clock. Read how the library represents deletes before you trust a
  value derived from its version.
- **Does the library have a peer sync protocol?** Only Automerge does. The Yrs
  and Loro adapters keep their own per-peer cache to reach the `None` fixed
  point that `sync_generate` requires. A comparison of sync-message counts
  across backends must state this.

## AI assistance

This project is developed with substantial AI assistance (Anthropic's Claude)
for code, experiment tooling, and analysis. The author treats AI output like
any other untrusted input:

- Design decisions are questioned, not accepted.
- Measurements are cross-checked. See the
  [validity gates](docs/divergence-sweep.md#validity-gates) and
  [run provenance](docs/scenarios.md#run-provenance).
- Results that look plausible are still verified against the underlying
  systems.

The author reviews every line with the goal of being able to defend it, and is
responsible for the code, the methodology, and all claims made from the data.
