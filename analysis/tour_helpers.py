"""Helpers for tour.py: run the orchestrator, and draw what it reports.

The notebook builds scenarios as dicts, runs them with `run`, and draws the
result with `graph_chart` and `timeline_chart`. Each run uses in-process
replicas and a fresh temporary directory, so runs do not share state.
"""

from __future__ import annotations

import json
import math
import subprocess
import tempfile
from dataclasses import dataclass
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


@dataclass
class Run:
    """What one orchestrator run reports."""

    scenario: dict
    crdt: str
    #: One row per trial: the orchestrator's CSV columns.
    trials: pd.DataFrame
    #: The summary row.
    summary: dict
    #: The --trace events of every trial.
    trace: pd.DataFrame
    #: The final --metrics-file snapshot: metric name to data points. The
    #: counters add up over every trial of the run.
    metrics: dict[str, list[dict]]

    @property
    def body(self) -> dict:
        return next(v for k, v in self.scenario.items() if k != "name")

    def trial_trace(self, trial: int = 1) -> pd.DataFrame:
        return self.trace[self.trace["trial"] == trial].reset_index(drop=True)

    def edges(self) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
        """The links, and the links that start blocked, from the trace."""
        wired = self.trace[self.trace["event"] == "wired"].iloc[0]
        return [tuple(e) for e in wired["edges"]], [tuple(e) for e in wired["blocked"]]

    def counter(self, name: str) -> pd.DataFrame:
        """The data points of counter `name` as a frame of actor, peer, value."""
        points = self.metrics.get(name, [])
        return pd.DataFrame(points, columns=["actor", "peer", "value"])

    def sync_per_edge(self) -> dict[tuple[int, int], int]:
        """Sync messages sent on each link, both directions added together."""
        tx = self.counter("replicant.sync.messages.tx")
        totals: dict[tuple[int, int], int] = {}
        for row in tx.itertuples():
            a, b = node_index(row.actor), node_index(row.peer)
            key = (min(a, b), max(a, b))
            totals[key] = totals.get(key, 0) + int(row.value)
        return totals

    def total(self, name: str) -> int:
        return int(self.counter(name)["value"].sum())

    def doc_size(self) -> int:
        """The largest document size any replica reported, in bytes."""
        points = self.metrics.get("replicant.doc.size_bytes", [])
        return max((p["value"] for p in points), default=0)


def node_index(actor: str) -> int:
    """`node-3` becomes 3."""
    return int(actor.rsplit("-", 1)[1])


def run(scenario: dict, crdt: str, binary: Path, trials: int = 1) -> Run:
    """Runs `scenario` in-process with `crdt`, traced, and returns the result.

    `binary` comes from `build()`. Passing it makes each caller wait for the
    build.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        (tmp / "scenario.toml").write_text(to_toml(scenario))
        proc = subprocess.run(
            [
                str(binary),
                "--output", "json",
                "--crdt", crdt,
                "--trials", str(trials),
                "--trace", str(tmp / "trace.jsonl"),
                "--metrics-file", str(tmp / "metrics.jsonl"),
                str(tmp / "scenario.toml"),
            ],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"orchestrator failed:\n{proc.stderr[-3000:]}")
        rows = [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]
        trace = [json.loads(line) for line in (tmp / "trace.jsonl").read_text().splitlines()]
        # The exporter writes one snapshot per export. The last one is final.
        snapshots = (tmp / "metrics.jsonl").read_text().splitlines()
        metrics = {m["name"]: m["data_points"] for m in json.loads(snapshots[-1])["metrics"]}
    return Run(
        scenario=scenario,
        crdt=crdt,
        trials=pd.DataFrame([r for r in rows if r["row_type"] == "trial"]),
        summary=next(r for r in rows if r["row_type"] == "summary"),
        trace=pd.DataFrame(trace),
        metrics=metrics,
    )


def phase_times(trace: pd.DataFrame) -> dict[str, float]:
    """The time of each one-off event in one trial's trace, in ms.

    `converged` is the last convergence, which is the measured one.
    """
    times = {}
    for event in ("wired", "clock_start", "unblocked", "kicked", "converged"):
        rows = trace[trace["event"] == event]
        if not rows.empty:
            times[event] = float(rows["t_ms"].iloc[-1])
    writes = trace[trace["event"] == "write"]
    if not writes.empty:
        times["first_write"] = float(writes["t_ms"].iloc[0])
        times["last_write"] = float(writes["t_ms"].iloc[-1])
    return times


# ── Charts ──────────────────────────────────────────────────────────────────


def graph_chart(
    n: int,
    edges: list[tuple[int, int]],
    blocked: list[tuple[int, int]] = (),
    weights: dict[tuple[int, int], int] | None = None,
    title: str = "",
) -> alt.Chart:
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
    node_rows = [{"x": x, "y": y, "node": f"node-{i}"} for i, (x, y) in pos.items()]
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
        .encode(x="x:Q", y="y:Q", text=alt.Text("node:N"))
        .transform_calculate(node="replace(datum.node, 'node-', '')")
    )
    return (links + nodes + labels).properties(width=260, height=260, title=title)


def timeline_chart(trace: pd.DataFrame, n: int, title: str = "") -> alt.Chart:
    """Draws one trial: writes as ticks, polls as dots, phases as rules.

    A dot's colour is the replica's fingerprint class at that poll. Replicas
    with the same colour have the same state. Grey means no operations yet.
    """
    writes = trace[trace["event"] == "write"][["t_ms", "node"]]
    poll_rows = []
    for row in trace[trace["event"] == "poll"].itertuples():
        for node, cls in zip(row.nodes, row.classes):
            poll_rows.append(
                {
                    "t_ms": row.t_ms,
                    "node": node,
                    "class": "no ops" if cls is None or cls != cls else f"state {int(cls)}",
                }
            )
    polls = pd.DataFrame(poll_rows, columns=["t_ms", "node", "class"])
    phases = pd.DataFrame(
        [
            {"t_ms": t, "phase": name}
            for name, t in phase_times(trace).items()
            if name in ("wired", "clock_start", "unblocked", "kicked", "converged")
        ]
    )
    y = alt.Y("node:O", title="replica", scale=alt.Scale(domain=list(range(n))))
    x = alt.X("t_ms:Q", title="ms since the trial started")
    layers = []
    if not writes.empty:
        layers.append(
            alt.Chart(writes)
            .mark_tick(color="#999", thickness=1, size=14)
            .encode(x=x, y=y, tooltip=["t_ms", "node"])
        )
    if not polls.empty:
        layers.append(
            alt.Chart(polls)
            .mark_circle(size=90, opacity=0.9)
            .encode(
                x=x,
                y=y,
                color=alt.Color("class:N", title="fingerprint at poll"),
                tooltip=["t_ms", "node", "class"],
            )
        )
    if not phases.empty:
        rules = alt.Chart(phases).encode(x="t_ms:Q")
        layers.append(rules.mark_rule(strokeDash=[3, 3], color="black"))
        layers.append(
            rules.mark_text(angle=270, align="left", dx=4, dy=-6, fontSize=10).encode(
                text="phase:N", y=alt.value(0)
            )
        )
    return alt.layer(*layers).properties(width=620, height=40 + 28 * n, title=title)
