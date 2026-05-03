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
just smoke    # end-to-end smoke test (orchestrator → two replicas)
just ci       # full gate: fmt check → lint → test → smoke
just docs     # build and open rustdoc
```

## Requirements

- Rust (stable, edition 2024)
- [`just`](https://github.com/casey/just)
- `protoc` (Protocol Buffers compiler)
