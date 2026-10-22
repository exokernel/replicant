"""Helpers for tour.py: run the orchestrator, and draw what it reports.

The notebook builds scenarios as dicts, runs them with `run`, and draws the
result with `graph_chart` and `timeline_chart`.

A run uses one of two lanes:

- `docker`: the replicas are containers in a 10-replica docker compose stack.
  `STACK` starts the stack the first time, and recreates it when the CRDT
  library changes. A scenario uses the first `node_count` replicas.
- `in-process`: the replicas are Tokio tasks inside the orchestrator process.

Every run is traced. The trace's `stats` events give the per-link sync counts
in both lanes.
"""

from __future__ import annotations

import json
import math
import socket
import subprocess
import tempfile
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from pathlib import Path

import altair as alt
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
BINARY = REPO / "target" / "release" / "orchestrator"

# Must match `crdts` in the Justfile.
CRDTS = ("automerge", "yrs", "loro")

# A trace of a large run has tens of thousands of points.
alt.data_transformers.disable_max_rows()


def build() -> Path:
    """Builds the release orchestrator, and returns the path to the binary.

    Debug builds are much slower, and their timings mean nothing.
    """
    proc = subprocess.run(
        ["cargo", "build", "--release", "--bin", "orchestrator"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"cargo build failed:\n{proc.stderr[-3000:]}")
    return BINARY


# ── The docker stack ────────────────────────────────────────────────────────


def docker_available() -> bool:
    """True if the docker daemon answers."""
    try:
        return subprocess.run(["docker", "info"], capture_output=True).returncode == 0
    except FileNotFoundError:
        return False


class Stack:
    """The docker compose stack that the docker lane runs against.

    It has `size` replicas, so it serves every scenario of up to `size`
    replicas without a restart. It runs one CRDT library at a time.
    """

    PORT_BASE = 50051
    GRAFANA = "http://localhost:3000"
    DASHBOARD_UID = "replicant-scenarios"

    def __init__(self, size: int = 10):
        self.size = size
        #: The library the stack runs, or None before the first `ensure`.
        self.crdt: str | None = None

    def ensure(self, crdt: str) -> None:
        """Starts the stack with `crdt`, unless it already runs it.

        The first start builds the replica image, which takes minutes. A
        library change recreates the replica containers, which takes seconds.
        """
        if self.crdt == crdt:
            return
        with tempfile.TemporaryDirectory() as tmp:
            sizing = Path(tmp) / "stack.toml"
            sizing.write_text(to_toml(topology("stack", self.size, "full_mesh", 1)))
            proc = subprocess.run(
                [str(REPO / "scripts" / "docker.sh"), "up", str(sizing), crdt],
                cwd=REPO,
                capture_output=True,
                text=True,
            )
        if proc.returncode != 0:
            raise RuntimeError(f"starting the docker stack failed:\n{proc.stderr[-3000:]}")
        for i in range(self.size):
            _wait_for_port(self.PORT_BASE + i)
        self.crdt = crdt

    def replicas_arg(self) -> str:
        """The orchestrator's --replicas argument for the whole stack."""
        return ",".join(
            f"localhost:{self.PORT_BASE + i}=replica-{i}:50051" for i in range(self.size)
        )

    def panel_url(self, panel_id: int, minutes: int = 5) -> str:
        """URL of one dashboard panel, for an iframe."""
        return (
            f"{self.GRAFANA}/d-solo/{self.DASHBOARD_UID}/replicant"
            f"?orgId=1&panelId={panel_id}&from=now-{minutes}m&to=now&refresh=5s&theme=light"
        )

    def dashboard_url(self) -> str:
        return f"{self.GRAFANA}/d/{self.DASHBOARD_UID}/replicant?orgId=1&refresh=5s"


def _wait_for_port(port: int, timeout_s: float = 60) -> None:
    """Waits until localhost:`port` accepts TCP connections."""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                return
        except OSError:
            if time.monotonic() > deadline:
                raise RuntimeError(f"nothing is listening on localhost:{port}") from None
            time.sleep(0.2)


#: The one stack this Python process uses.
STACK = Stack()


# ── Scenarios ───────────────────────────────────────────────────────────────


def topology(
    name: str,
    node_count: int,
    connections: str,
    op_count: int,
    write_pattern: str = "round_robin",
    workload: str = "map_put",
    op_interval_ms: int = 0,
) -> dict:
    """A steady-state scenario: wire the graph, write, and wait for agreement."""
    return {
        "name": name,
        "topology": {
            "node_count": node_count,
            "connections": connections,
            "write_pattern": write_pattern,
            "workload": workload,
            "op_count": op_count,
            "op_interval_ms": op_interval_ms,
        },
    }


def partition(
    name: str,
    groups: list[list[int]],
    ops_per_group: int,
    heal_topology: str = "full_mesh",
    write_pattern: str = "round_robin",
    workload: str = "map_put",
    locality: str = "append",
) -> dict:
    """A partition-heal scenario: the groups write apart, then the links heal."""
    return {
        "name": name,
        "partition_heal": {
            "node_count": sum(len(g) for g in groups),
            "write_pattern": write_pattern,
            "heal_topology": heal_topology,
            "workload": workload,
            "locality": locality,
            "ops_per_group": ops_per_group,
            "groups": groups,
        },
    }


def to_toml(scenario: dict) -> str:
    """Writes a scenario dict as TOML. Supports only the shapes above."""

    def value(v) -> str:
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, str):
            return json.dumps(v)
        if isinstance(v, list):
            return "[" + ", ".join(value(x) for x in v) + "]"
        raise TypeError(f"cannot write {v!r} as TOML")

    lines = [f"name = {value(scenario['name'])}"]
    for section, body in scenario.items():
        if section == "name":
            continue
        lines += ["", f"[{section}]"]
        lines += [f"{k} = {value(v)}" for k, v in body.items() if k != "groups"]
        for group in body.get("groups", []):
            lines += ["", f"[[{section}.groups]]", f"nodes = {value(group)}"]
    return "\n".join(lines) + "\n"


# ── Running ─────────────────────────────────────────────────────────────────

#: One --trace event, as decoded from JSON.
Event = dict[str, Any]


@dataclass
class Run:
    """What one orchestrator run reports."""

    scenario: dict
    crdt: str
    lane: str
    #: One row per trial: the orchestrator's CSV columns.
    trials: pd.DataFrame
    #: The summary row.
    summary: dict[str, Any]
    #: The --trace events of every trial, in order.
    events: list[Event]

    @property
    def trace(self) -> pd.DataFrame:
        """The events as a table, for display."""
        return pd.DataFrame(self.events)

    def trial_events(self, trial: int = 1) -> list[Event]:
        return [e for e in self.events if e["trial"] == trial]

    def edges(self) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
        """The links, and the links that start blocked, from the trace."""
        wired = next(e for e in self.events if e["event"] == "wired")
        return [(a, b) for a, b in wired["edges"]], [(a, b) for a, b in wired["blocked"]]

    def link_stats(self) -> list[dict[str, int]]:
        """One entry per trial and directed link: messages sent, received, and
        deferred, from the trace's `stats` events."""
        return [
            {"trial": e["trial"], "node": e["node"], **peer}
            for e in self.events
            if e["event"] == "stats"
            for peer in e["peers"]
        ]

    def sync_per_edge(self) -> dict[tuple[int, int], int]:
        """Sync messages sent on each link, both directions and all trials."""
        totals: dict[tuple[int, int], int] = {}
        for link in self.link_stats():
            a, b = link["node"], link["peer"]
            key = (min(a, b), max(a, b))
            totals[key] = totals.get(key, 0) + link["tx"]
        return totals

    def sync_messages(self) -> int:
        """Sync messages sent over all links and all trials."""
        return sum(link["tx"] for link in self.link_stats())

    def deferred(self) -> int:
        """Flushes deferred because a peer's channel was full, all trials."""
        return sum(link["deferred"] for link in self.link_stats())

    def doc_size(self) -> int:
        """The largest document size any replica reported, in bytes."""
        return max((e["doc_size_bytes"] for e in self.events if e["event"] == "stats"), default=0)


def run(
    scenario: dict, crdt: str, binary: Path, trials: int = 1, lane: str = "docker"
) -> Run:
    """Runs `scenario` with `crdt` in `lane`, traced, and returns the result.

    `binary` comes from `build()`. Passing it makes each caller wait for the
    build.
    """
    if lane == "docker":
        STACK.ensure(crdt)
        # The stack fixes the library; the orchestrator rejects --crdt here.
        lane_args = ["--replicas", STACK.replicas_arg()]
    else:
        lane_args = ["--crdt", crdt]
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        (tmp / "scenario.toml").write_text(to_toml(scenario))
        proc = subprocess.run(
            [
                str(binary),
                "--output", "json",
                "--trials", str(trials),
                "--trace", str(tmp / "trace.jsonl"),
                *lane_args,
                str(tmp / "scenario.toml"),
            ],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"orchestrator failed:\n{proc.stderr[-3000:]}")
        rows = [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]
        events = [json.loads(line) for line in (tmp / "trace.jsonl").read_text().splitlines()]
    return Run(
        scenario=scenario,
        crdt=crdt,
        lane=lane,
        trials=pd.DataFrame([r for r in rows if r["row_type"] == "trial"]),
        summary=next(r for r in rows if r["row_type"] == "summary"),
        events=events,
    )


def phase_times(events: list[Event]) -> dict[str, float]:
    """The time of each one-off event in one trial, in ms.

    `converged` is the last convergence, which is the measured one.
    """
    times: dict[str, float] = {}
    for e in events:
        if e["event"] in ("wired", "clock_start", "unblocked", "kicked", "converged"):
            times[e["event"]] = e["t_ms"]
    writes = [e["t_ms"] for e in events if e["event"] == "write"]
    if writes:
        times["first_write"] = writes[0]
        times["last_write"] = writes[-1]
    return times


def poll_count(events: list[Event]) -> int:
    return sum(e["event"] == "poll" for e in events)


# ── Charts ──────────────────────────────────────────────────────────────────


def graph_chart(
    n: int,
    edges: Sequence[tuple[int, int]],
    blocked: Sequence[tuple[int, int]] = (),
    weights: dict[tuple[int, int], int] | None = None,
    title: str = "",
) -> alt.TopLevelMixin:
    """Draws the replicas on a circle and the links between them.

    A dashed link starts blocked. With `weights`, a link's width shows the
    number of sync messages it carried.
    """
    # Node 0 at the top, then clockwise.
    pos = {
        i: (math.sin(2 * math.pi * i / n), math.cos(2 * math.pi * i / n)) for i in range(n)
    }
    blocked_set = {(min(a, b), max(a, b)) for a, b in blocked}
    weights = weights or {}
    edge_rows = []
    for a, b in edges:
        key = (min(a, b), max(a, b))
        edge_rows.append(
            {
                "x": pos[a][0], "y": pos[a][1], "x2": pos[b][0], "y2": pos[b][1],
                "link": f"{a}–{b}",
                "state": "blocked until heal" if key in blocked_set else "open",
                "sync messages": weights.get(key, 0),
            }
        )
    node_rows = [{"x": x, "y": y, "node": str(i)} for i, (x, y) in pos.items()]
    axis = alt.Axis(labels=False, ticks=False, grid=False, domain=False, title=None)
    scale = alt.Scale(domain=[-1.3, 1.3])
    links = (
        alt.Chart(pd.DataFrame(edge_rows))
        .mark_rule(color="#888")
        .encode(
            x=alt.X("x:Q", axis=axis, scale=scale),
            y=alt.Y("y:Q", axis=axis, scale=scale),
            x2="x2:Q",
            y2="y2:Q",
            strokeDash=alt.StrokeDash(
                "state:N",
                scale=alt.Scale(domain=["open", "blocked until heal"], range=[[1, 0], [4, 4]]),
                legend=alt.Legend(title=None, orient="bottom") if blocked_set else None,
            ),
            strokeWidth=(
                alt.StrokeWidth("sync messages:Q", scale=alt.Scale(range=[1, 8]))
                if weights
                else alt.value(1.5)
            ),
            tooltip=["link", "state", "sync messages"],
        )
    )
    nodes = (
        alt.Chart(pd.DataFrame(node_rows))
        .mark_circle(size=500, color="#4c78a8", opacity=1)
        .encode(x="x:Q", y="y:Q", tooltip=["node"])
    )
    labels = (
        alt.Chart(pd.DataFrame(node_rows))
        .mark_text(color="white", fontWeight="bold")
        .encode(x="x:Q", y="y:Q", text="node:N")
    )
    return (links + nodes + labels).properties(width=260, height=260, title=title)


def timeline_chart(events: list[Event], n: int, title: str = "") -> alt.TopLevelMixin:
    """Draws one trial: writes as ticks, polls as dots, phases as rules.

    A dot's colour is the replica's fingerprint class at that poll. Replicas
    with the same colour have the same state. Grey means no operations yet.
    """
    writes = [{"t_ms": e["t_ms"], "node": e["node"]} for e in events if e["event"] == "write"]
    polls = [
        {
            "t_ms": e["t_ms"],
            "node": node,
            "class": "no ops" if cls is None else f"state {cls}",
        }
        for e in events
        if e["event"] == "poll"
        for node, cls in zip(e["nodes"], e["classes"])
    ]
    phases = [
        {"t_ms": t, "phase": name}
        for name, t in phase_times(events).items()
        if name in ("wired", "clock_start", "unblocked", "kicked", "converged")
    ]
    y = alt.Y("node:O", title="replica", scale=alt.Scale(domain=list(range(n))))
    x = alt.X("t_ms:Q", title="ms since the trial started")
    layers = []
    if writes:
        layers.append(
            alt.Chart(pd.DataFrame(writes))
            .mark_tick(color="#999", thickness=1, size=14)
            .encode(x=x, y=y, tooltip=["t_ms", "node"])
        )
    if polls:
        layers.append(
            alt.Chart(pd.DataFrame(polls))
            .mark_circle(size=90, opacity=0.9)
            .encode(
                x=x,
                y=y,
                color=alt.Color("class:N", title="fingerprint at poll"),
                tooltip=["t_ms", "node", "class"],
            )
        )
    if phases:
        rules = alt.Chart(pd.DataFrame(phases)).encode(x="t_ms:Q")
        layers.append(rules.mark_rule(strokeDash=[3, 3], color="black"))
        layers.append(
            rules.mark_text(angle=270, align="left", dx=4, dy=-6, fontSize=10).encode(
                text="phase:N", y=alt.value(0)
            )
        )
    return alt.layer(*layers).properties(width=620, height=40 + 28 * n, title=title)
