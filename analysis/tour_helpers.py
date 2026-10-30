"""Helpers for tour.py: run the orchestrator, and draw what it reports.

The notebook builds scenarios as dicts, runs them with `run`, and draws the
result with `graph_chart` and `timeline_chart`.

A run uses one of three lanes:

- `docker`: the replicas are containers in a 10-replica docker compose stack.
- `k8s`: the replicas are pods in a 10-replica StatefulSet on a local kind
  cluster. The orchestrator reaches them through `kubectl port-forward`.
- `in-process`: the replicas are Tokio tasks inside the orchestrator process.

`STACKS` holds the stack of each container lane. A stack starts the first
time a run needs it, and restarts its replicas when the CRDT library changes.
A scenario uses the first `node_count` replicas.

Every run is traced. The trace's `stats` events give the per-link sync counts
in every lane.
"""

from __future__ import annotations

import atexit
import json
import math
import os
import shutil
import socket
import subprocess
import tempfile
import time
from collections import deque
from collections.abc import Callable, Sequence
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


# ── The container stacks ────────────────────────────────────────────────────


def docker_available() -> bool:
    """True if the docker daemon answers within 10 seconds."""
    try:
        return (
            subprocess.run(["docker", "info"], capture_output=True, timeout=10).returncode == 0
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def kind_installed() -> bool:
    """True if the kind and kubectl commands exist. kind also needs docker."""
    return bool(shutil.which("kind") and shutil.which("kubectl"))


class Stack:
    """A container stack that a lane runs against.

    It has `size` replicas, so it serves every scenario of up to `size`
    replicas without a restart. It runs one CRDT library at a time.
    """

    #: The lane name.
    LANE = ""
    #: The script that starts the stack, in scripts/.
    SCRIPT = ""
    #: Host port of replica 0. Replica i is on PORT_BASE + i.
    PORT_BASE = 0
    #: The address that the replicas use to dial each other, with {i}.
    PEER = ""
    GRAFANA_PORT = 0
    DASHBOARD_UID = "replicant-scenarios"

    def __init__(self, size: int = 10):
        self.size = size
        #: The library the stack runs, or None before the first `ensure`.
        self.crdt: str | None = None

    def ensure(self, crdt: str, on_line: Callable[[str], None] | None = None) -> None:
        """Starts the stack with `crdt`, unless it already runs it.

        The first start builds the replica image, which takes minutes. A
        library change recreates the replica containers, which takes seconds.
        `on_line` gets each line of the build output as it arrives.

        If a replica port does not answer, for example after
        `just docker-down`, it starts the stack again. Before a start, it
        stops the other stacks, so that only one stack runs at a time.
        """
        if self.crdt == crdt and self._replicas_answer():
            return
        for other in STACKS.values():
            if other is not self:
                other.stop()
        with tempfile.TemporaryDirectory() as tmp:
            sizing = Path(tmp) / "stack.toml"
            sizing.write_text(to_toml(topology("stack", self.size, "full_mesh", 1)))
            proc = subprocess.Popen(
                [str(REPO / "scripts" / self.SCRIPT), "up", str(sizing), crdt],
                cwd=REPO,
                env=self._env(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            tail: deque[str] = deque(maxlen=40)
            assert proc.stdout is not None
            for line in proc.stdout:
                line = line.rstrip()
                if not line:
                    continue
                tail.append(line)
                if on_line:
                    on_line(line)
            returncode = proc.wait()
        if returncode != 0:
            raise RuntimeError(
                f"starting the {self.LANE} stack failed:\n" + "\n".join(tail)
            )
        for i in range(self.size):
            _wait_for_port(self.PORT_BASE + i)
        self._after_start()
        self.crdt = crdt

    def stop(self) -> None:
        """Stops the stack's containers. Does nothing if they do not run."""
        self.crdt = None
        subprocess.run(
            [str(REPO / "scripts" / self.SCRIPT), "down"], cwd=REPO, capture_output=True
        )

    def _replicas_answer(self) -> bool:
        """True if every replica port accepts a TCP connection."""
        for i in range(self.size):
            try:
                with socket.create_connection(("localhost", self.PORT_BASE + i), timeout=1):
                    pass
            except OSError:
                return False
        return True

    def _env(self) -> dict[str, str] | None:
        """The environment of the start script. None keeps the notebook's."""
        return None

    def _after_start(self) -> None:
        """Runs after the replicas accept connections."""

    def replicas_arg(self) -> str:
        """The orchestrator's --replicas argument for the whole stack."""
        return ",".join(
            f"localhost:{self.PORT_BASE + i}={self.PEER.replace('{i}', str(i))}"
            for i in range(self.size)
        )

    def grafana_url(self) -> str:
        return f"http://localhost:{self.GRAFANA_PORT}"

    def panel_url(self, panel_id: int, minutes: int = 5) -> str:
        """URL of one dashboard panel, for an iframe."""
        return (
            f"{self.grafana_url()}/d-solo/{self.DASHBOARD_UID}/replicant"
            f"?orgId=1&panelId={panel_id}&from=now-{minutes}m&to=now&refresh=5s&theme=light"
        )

    def dashboard_url(self) -> str:
        return f"{self.grafana_url()}/d/{self.DASHBOARD_UID}/replicant?orgId=1&refresh=5s"


class DockerStack(Stack):
    """The docker compose stack. Grafana is a published port."""

    LANE = "docker"
    SCRIPT = "docker.sh"
    PORT_BASE = 50051
    PEER = "replica-{i}:50051"
    GRAFANA_PORT = 3000


class KindStack(Stack):
    """The StatefulSet on the kind cluster `replicant`.

    The first start also creates the cluster, which takes about a minute
    more. `stop` keeps the cluster, and `just k8s-down` deletes it. The ports
    are not the docker stack's, so a docker stack that another tool started
    does not block them.
    """

    LANE = "k8s"
    SCRIPT = "k8s.sh"
    PORT_BASE = 51051
    PEER = "node-{i}.node:50051"
    GRAFANA_PORT = 3001

    def __init__(self, size: int = 10):
        super().__init__(size)
        self._grafana: subprocess.Popen | None = None

    def _env(self) -> dict[str, str]:
        return {**os.environ, "KIND_PORT_BASE": str(self.PORT_BASE)}

    def stop(self) -> None:
        """Scales every pod to zero, and stops the port-forwards.

        It keeps the cluster, so the next start takes seconds, not minutes.
        `k8s.sh up` applies the manifests again, which restores the pods.
        """
        self.crdt = None
        if self._grafana is not None:
            self._grafana.terminate()
            self._grafana = None
        subprocess.run(
            ["pkill", "-f", "kubectl -n replicant port-forward pod/node-"], capture_output=True
        )
        # Name the context, so that this never scales pods on another cluster.
        for kind in ("statefulset", "deployment"):
            subprocess.run(
                [
                    "kubectl", "--context", "kind-replicant", "-n", "replicant",
                    "scale", kind, "--all", "--replicas=0",
                ],
                capture_output=True,
            )

    def _after_start(self) -> None:
        """Port-forwards Grafana, unless the forward already runs.

        The forward stops when the notebook stops.
        """
        if self._grafana is not None and self._grafana.poll() is None:
            return
        self._grafana = subprocess.Popen(
            [
                "kubectl", "-n", "replicant", "port-forward",
                "svc/grafana", f"{self.GRAFANA_PORT}:3000",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        atexit.register(self._grafana.terminate)
        _wait_for_port(self.GRAFANA_PORT)


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
STACKS: dict[str, Stack] = {"docker": DockerStack(), "k8s": KindStack()}


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

    def trace_table(self, trial: int | None = None) -> pd.DataFrame:
        """The events as a table with one `detail` column, for display.

        Each kind of event has its own fields. One column per field would be
        mostly empty, so the fields of an event go in its `detail` text.
        """
        rows = [
            {
                "t_ms": round(e["t_ms"], 3),
                "event": e["event"],
                "detail": ", ".join(
                    f"{k}={v}"
                    for k, v in e.items()
                    if k not in ("t_ms", "event", "scenario", "trial")
                ),
            }
            for e in self.events
            if trial is None or e["trial"] == trial
        ]
        return pd.DataFrame(rows, columns=["t_ms", "event", "detail"])

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
    if lane in STACKS:
        STACKS[lane].ensure(crdt)
        # The stack fixes the library; the orchestrator rejects --crdt here.
        lane_args = ["--replicas", STACKS[lane].replicas_arg()]
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
