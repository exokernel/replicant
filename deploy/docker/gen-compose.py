#!/usr/bin/env python3
"""Emit a docker compose YAML for N replicant replicas + observability stack.

Run via `just smoke-docker <scenario>` / `just docker-up <scenario>`; those
recipes parse node_count from the scenario TOML and pipe the output to
`deploy/docker/compose.generated.yaml`. Can also be invoked directly:

    python3 deploy/docker/gen-compose.py 5 > deploy/docker/compose.generated.yaml
    docker compose -f deploy/docker/compose.generated.yaml up -d --build

`--crdt` selects which CRDT library backs every replica in the generated
stack (default `automerge`). It is a *deployment* parameter rather than a
scenario field on purpose: RQ-1 compares libraries on the identical
workload, so one scenario file must be runnable against all three rather
than needing three near-duplicate copies of every scenario.

Output structure: replica services (N of them), then the observability
stack (otel-collector → prometheus → grafana). Section comments and
blank lines are inserted between groups so the generated file is
human-scannable when debugging. Build contexts and volume bind-mounts
resolve relative to deploy/docker/, so the output file must live there.
"""

import argparse
import sys
from typing import TextIO

import yaml


# PyYAML's default SafeDumper emits anchor/alias refs for repeated objects,
# turning the per-replica common config into `*id001` lines. Disable aliases
# so each service is fully self-describing — the whole point of generating
# inspectable YAML is that readers don't need to chase references.
class NoAliasDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):  # noqa: D401, ARG002
        return True


REPLICA_INTERNAL_PORT = 50051
REPLICA_HOST_PORT_BASE = 50051

# Must match `Crdt::ALL` in crates/replica/src/adapter.rs. Duplicated
# rather than derived because the generator must not need a cargo build to
# emit a compose file; the replica binary re-validates on startup, so a drift
# here fails fast at container start rather than producing wrong measurements.
CRDT_CHOICES = ("automerge", "yrs", "loro")
DEFAULT_CRDT = "automerge"


def replica_service(i: int, crdt: str = DEFAULT_CRDT) -> dict:
    """Service definition for replica-i. Each call returns a fresh dict."""
    return {
        "build": {"context": "../..", "dockerfile": "Dockerfile"},
        "container_name": f"replica-{i}",
        "hostname": f"replica-{i}",
        "command": [
            "--actor",
            f"node-{i}",
            "--port",
            str(REPLICA_INTERNAL_PORT),
            "--crdt",
            crdt,
        ],
        "ports": [f"{REPLICA_HOST_PORT_BASE + i}:{REPLICA_INTERNAL_PORT}"],
        "networks": ["replicant"],
        "restart": "no",
        "environment": {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector:4317",
            # OTel SDK default flush is 60s — too coarse for the dashboard's
            # rate panels to show anything other than one big counter jump
            # per scenario. 2s matches Prometheus's scrape interval, so any
            # multi-second workload produces a visible curve.
            "OTEL_METRIC_EXPORT_INTERVAL": "2000",
        },
        "depends_on": {"otel-collector": {"condition": "service_started"}},
    }


def otel_collector_service() -> dict:
    return {
        "image": "otel/opentelemetry-collector-contrib:0.152.0",
        "container_name": "otel-collector",
        "hostname": "otel-collector",
        "networks": ["replicant"],
        "command": ["--config=/etc/otel-collector-config.yaml"],
        "volumes": [
            "../shared/otel-collector-config.yaml:/etc/otel-collector-config.yaml:ro"
        ],
        "ports": ["4317:4317", "8889:8889"],
    }


def prometheus_service() -> dict:
    return {
        "image": "prom/prometheus:v3.11.3",
        "container_name": "prometheus",
        "hostname": "prometheus",
        "networks": ["replicant"],
        "volumes": ["../shared/prometheus.yml:/etc/prometheus/prometheus.yml:ro"],
        "ports": ["9090:9090"],
        "depends_on": {"otel-collector": {"condition": "service_started"}},
    }


def grafana_service() -> dict:
    return {
        "image": "grafana/grafana:11.3.1",
        "container_name": "grafana",
        "hostname": "grafana",
        "networks": ["replicant"],
        "environment": {
            "GF_SECURITY_ADMIN_USER": "admin",
            "GF_SECURITY_ADMIN_PASSWORD": "admin",
            "GF_USERS_ALLOW_SIGN_UP": "false",
            # Anonymous viewer would let you skip login entirely; we keep
            # admin/admin so in-UI dashboard edits attribute correctly.
        },
        "volumes": [
            "../shared/grafana/datasource.yaml:/etc/grafana/provisioning/datasources/datasource.yaml:ro",
            "../shared/grafana/dashboards-provider.yaml:/etc/grafana/provisioning/dashboards/dashboards.yaml:ro",
            "../shared/grafana/dashboards:/var/lib/grafana/dashboards:ro",
        ],
        "ports": ["3000:3000"],
        "depends_on": {"prometheus": {"condition": "service_started"}},
    }


def _dump_indented(data: dict, indent: int) -> str:
    """yaml.dump `data` and prefix every non-blank line with `indent` spaces."""
    body = yaml.dump(
        data, Dumper=NoAliasDumper, sort_keys=False, default_flow_style=False
    )
    prefix = " " * indent
    return "".join(
        prefix + line if line.strip() else line for line in body.splitlines(keepends=True)
    )


def write_compose(n: int, out: TextIO, crdt: str = DEFAULT_CRDT) -> None:
    if n < 1:
        raise ValueError(f"node_count must be >= 1, got {n}")
    if crdt not in CRDT_CHOICES:
        raise ValueError(f"crdt must be one of {CRDT_CHOICES}, got {crdt!r}")

    out.write(
        f"# Generated by deploy/docker/gen-compose.py for n={n}, crdt={crdt}.\n"
        f"# Do not edit by hand — re-run the generator to refresh. See the script\n"
        f"# docstring for usage.\n"
        f"\n"
        f"name: replicant\n"
        f"\n"
        f"networks:\n"
        f"  replicant:\n"
        f"    driver: bridge\n"
        f"\n"
        f"services:\n"
        f"\n"
        f"  # ---- Replica nodes ----\n"
        f"  # One service per CRDT replica, all backed by {crdt}. Each binds host\n"
        f"  # port 5005X to internal gRPC port 50051 and pushes OTel metrics to\n"
        f"  # otel-collector:4317.\n"
        f"\n"
    )
    for i in range(n):
        if i > 0:
            out.write("\n")
        out.write(_dump_indented({f"replica-{i}": replica_service(i, crdt)}, 2))

    out.write(
        f"\n"
        f"  # ---- Observability stack ----\n"
        f"  # otel-collector receives OTLP from replicas and exposes a Prometheus\n"
        f"  # scrape endpoint at :8889. Prometheus scrapes it every 2s. Grafana\n"
        f"  # serves the dashboard UI at :3000 (admin/admin).\n"
        f"\n"
    )
    out.write(_dump_indented({"otel-collector": otel_collector_service()}, 2))
    out.write("\n")
    out.write(_dump_indented({"prometheus": prometheus_service()}, 2))
    out.write("\n")
    out.write(_dump_indented({"grafana": grafana_service()}, 2))


def main() -> int:
    p = argparse.ArgumentParser(
        description="Emit a docker compose YAML for N replicant replicas + observability stack."
    )
    p.add_argument("n", type=int, help="number of replica services to generate")
    p.add_argument(
        "--crdt",
        choices=CRDT_CHOICES,
        default=DEFAULT_CRDT,
        help=f"CRDT library backing every replica (default: {DEFAULT_CRDT})",
    )
    args = p.parse_args()
    write_compose(args.n, sys.stdout, args.crdt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
