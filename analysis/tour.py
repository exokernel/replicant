import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="Replicant tour")


@app.cell
def _():
    import marimo as mo
    import altair as alt
    import pandas as pd

    import tour_helpers as th

    return alt, mo, pd, th


@app.cell
def _(mo):
    mo.md(r"""
    # How Replicant works, and how to read what it measures

    Replicant compares CRDT libraries. It starts replicas, wires them into a
    graph, applies writes, and measures how long the replicas take to agree.
    This notebook runs small scenarios live, so that you can see each step and
    change it.

    It assumes that you know what a CRDT is. It does not assume that you know
    Replicant.

    Every run here uses **in-process replicas**: each replica is a gRPC server
    on a Tokio task inside the orchestrator process. The container stacks run
    the same replica code in separate processes. Section 6 says what that
    difference means for the numbers.
    """)
    return


@app.cell
def _(mo, th):
    with mo.status.spinner(title="Building the release orchestrator…"):
        binary = th.build()
    mo.md(f"Orchestrator ready: `{binary.relative_to(th.REPO)}`")
    return (binary,)


@app.cell
def _(mo, th):
    crdt = mo.ui.dropdown(options=list(th.CRDTS), value="automerge", label="CRDT library")
    mo.callout(
        mo.hstack(
            [crdt, mo.md("Sections 1 to 4 and 6 use this library. Change it at any time.")],
            justify="start",
        ),
        kind="info",
    )
    return (crdt,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. What is under test?

    A **replica** holds one CRDT document behind the `CrdtAdapter` trait. Each
    library (Automerge, Yrs, or Loro) has one adapter. A replica serves two
    gRPC services:

    - **Replica**: control calls from the orchestrator. Apply an op, read the
      state fingerprint, connect to a peer, block a link, reset.
    - **Sync**: a stream per link to each peer. The adapter's sync messages go
      over it.

    The **orchestrator** is the client. It never carries sync traffic. It
    wires the replicas, sends the writes, and polls each replica until all of
    them agree.
    """)
    return


@app.cell
def _(mo):
    mo.mermaid(
        """
        flowchart LR
            O[orchestrator]
            O -- "apply op, poll fingerprint" --> R0[replica 0]
            O -- "apply op, poll fingerprint" --> R1[replica 1]
            R0 <-- "sync stream" --> R1
        """
    )
    return


@app.cell
def _(binary, crdt, mo, th):
    first = th.run(
        th.topology("two-replicas", 2, "full_mesh", op_count=3, write_pattern="concentrated"),
        crdt.value,
        binary,
    )
    mo.vstack(
        [
            mo.md(
                f"""
                The smallest run: two replicas, one link, and three map writes,
                all to replica 0, with **{crdt.value}**. This is the row that
                the orchestrator reports for the trial:
                """
            ),
            mo.ui.table(first.trials, selection=None, show_column_summaries=False),
            mo.md(
                """
                And this is the trace of the same trial, from `--trace`. Each
                line is one event. A `poll` gives each replica's fingerprint
                **class**: replicas in the same class have the same state.
                """
            ),
            mo.ui.table(
                first.trace.drop(columns=["scenario", "trial"]),
                selection=None,
                show_column_summaries=False,
            ),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### What "converged" means

    Each adapter returns a **state fingerprint**: opaque bytes that are equal
    if and only if two replicas hold the same state. The orchestrator compares
    the bytes and does not interpret them. A trial has converged when every
    replica returns the same non-empty fingerprint. An empty fingerprint means
    that the replica has no operations yet, so an untouched run cannot
    "converge" at once.

    Each library needs a different fingerprint:

    | Library | Fingerprint | Why |
    |---|---|---|
    | Automerge | the sorted change-hash heads | Equal heads mean equal history, so equal state. |
    | Loro | the sorted causal frontier | The same argument as Automerge. The snapshot bytes are not used, because two converged replicas can produce different snapshots. |
    | Yrs | the full update encoding | A Yrs delete does not advance the state vector. Heads alone would miss a delete. |

    So "the same fingerprint" is exact for each library, but the fingerprints
    of two libraries do not compare with each other.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. What does one trial measure?

    A steady-state trial does these steps, in order:

    1. **Get the replicas** and reset them, so each trial starts empty.
    2. **Wire every link.** This is outside the clock.
    3. **Start the clock** (`clock_start`).
    4. **Apply the writes**, one RPC each. Sync starts as soon as a write lands.
    5. **Poll** every replica's fingerprint, about once a millisecond.
    6. **Stop the clock** at the first poll where every replica agrees.

    `convergence_ms` is the time from step 3 to step 6. It **includes the time
    to send the writes**. The clock must start before the first write: over
    loopback, sync often finishes before the first poll returns, so a clock
    that started after the writes would often read zero.

    Change the run below and watch the timeline.
    """)
    return


@app.cell
def _(mo):
    s2_nodes = mo.ui.slider(2, 8, value=4, label="replicas", show_value=True)
    s2_ops = mo.ui.slider(1, 200, value=20, label="writes", show_value=True)
    s2_interval = mo.ui.slider(0, 5, value=0, label="pause between writes (ms)", show_value=True)
    mo.hstack([s2_nodes, s2_ops, s2_interval], justify="start")
    return s2_interval, s2_nodes, s2_ops


@app.cell
def _(binary, crdt, mo, s2_interval, s2_nodes, s2_ops, th):
    one = th.run(
        th.topology(
            "one-trial",
            s2_nodes.value,
            "full_mesh",
            s2_ops.value,
            op_interval_ms=s2_interval.value,
        ),
        crdt.value,
        binary,
    )
    _trace = one.trial_trace()
    _p = th.phase_times(_trace)
    _measured = _p["converged"] - _p["clock_start"]
    _writing = _p["last_write"] - _p["clock_start"]
    _polls = int((_trace["event"] == "poll").sum())
    mo.vstack(
        [
            th.timeline_chart(_trace, s2_nodes.value, title="One trial, full mesh"),
            mo.md(
                f"""
                - Reported `convergence_ms`: **{one.trials["convergence_ms"].iloc[0]:.3f} ms**.
                  From the trace, `converged` minus `clock_start` gives
                  {_measured:.3f} ms. The two differ only by when each clock
                  was read.
                - Sending the writes took **{_writing:.3f} ms**, which is
                  **{100 * _writing / _measured:.0f}%** of the measured time.
                - The orchestrator polled **{_polls}** time(s). One poll means
                  that the replicas already agreed when the first poll
                  returned.
                """
            ),
        ]
    )
    return (one,)


@app.cell
def _(mo, one, pd):
    _meaning = {
        "row_type": "`trial` for one trial, `summary` for the scenario.",
        "scenario": "The scenario name.",
        "trial": "The 1-based trial number.",
        "node_count": "Number of replicas.",
        "op_count": "Total writes, over all replicas.",
        "topology_kind": "The graph shape: `full_mesh`, `ring`, `line`, `star`, `custom`, or `partition_heal`.",
        "edge_count": "Links in the final graph.",
        "diameter": "Most hops between two replicas in the final graph.",
        "convergence_ms": "From `clock_start` to the poll that saw agreement.",
        "wiring_ms": "Link setup inside the window. Always 0 here; see section 4.",
    }
    _row = one.trials.iloc[0]
    mo.vstack(
        [
            mo.md("### Read the row, column by column"),
            mo.ui.table(
                pd.DataFrame(
                    [{"column": k, "value": _row[k], "meaning": v} for k, v in _meaning.items()]
                ),
                selection=None,
                show_column_summaries=False,
            ),
            mo.md(
                """
                The summary row adds `mean_ms`, `p50_ms`, `p95_ms`, and
                `mean_wiring_ms` over all trials of the scenario.
                """
            ),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Does the topology change convergence?

    Each replica syncs only with the replicas it has a link to. A write must
    therefore travel up to **diameter** hops before every replica has it.
    Replicas relay: when a replica receives new changes, it sends sync
    messages to its other peers.

    The graph below shows the links. A link's width is the number of sync
    messages it carried, in both directions, over all trials.
    """)
    return


@app.cell
def _(mo):
    s3_conn = mo.ui.dropdown(
        options=["full_mesh", "ring", "line", "star"], value="ring", label="shape"
    )
    s3_nodes = mo.ui.slider(3, 10, value=6, label="replicas", show_value=True)
    s3_pattern = mo.ui.dropdown(
        options=["round_robin", "concentrated"], value="round_robin", label="writes go to"
    )
    s3_ops = mo.ui.slider(5, 200, value=30, label="writes", show_value=True)
    s3_trials = mo.ui.slider(1, 30, value=10, label="trials", show_value=True)
    mo.vstack(
        [
            mo.hstack([s3_conn, s3_nodes, s3_pattern], justify="start"),
            mo.hstack([s3_ops, s3_trials], justify="start"),
            mo.md(
                "`round_robin` spreads the writes over every replica. "
                "`concentrated` sends every write to replica 0."
            ),
        ]
    )
    return s3_conn, s3_nodes, s3_ops, s3_pattern, s3_trials


@app.cell
def _(alt, binary, crdt, mo, s3_conn, s3_nodes, s3_ops, s3_pattern, s3_trials, th):
    topo = th.run(
        th.topology("topology", s3_nodes.value, s3_conn.value, s3_ops.value, s3_pattern.value),
        crdt.value,
        binary,
        trials=s3_trials.value,
    )
    _edges, _ = topo.edges()
    _tx = topo.total("replicant.sync.messages.tx")
    _strip = (
        alt.Chart(topo.trials)
        .mark_circle(size=60)
        .encode(
            x=alt.X("convergence_ms:Q", title="convergence_ms", scale=alt.Scale(zero=True)),
            y=alt.value(20),
            tooltip=["trial", "convergence_ms"],
        )
        .properties(width=320, height=40, title="One dot per trial")
    )
    mo.vstack(
        [
            mo.hstack(
                [
                    th.graph_chart(
                        s3_nodes.value,
                        _edges,
                        weights=topo.sync_per_edge(),
                        title=f"{s3_conn.value}, {s3_nodes.value} replicas",
                    ),
                    mo.vstack(
                        [
                            _strip,
                            mo.md(
                                f"""
                                | | |
                                |---|---|
                                | links | {topo.summary["edge_count"]} |
                                | diameter | {topo.summary["diameter"]} |
                                | p50 / p95 | {topo.summary["p50_ms"]:.2f} / {topo.summary["p95_ms"]:.2f} ms |
                                | sync messages per write | {_tx / (s3_ops.value * s3_trials.value):.1f} |
                                """
                            ),
                        ]
                    ),
                ],
                justify="start",
            ),
        ]
    )
    return


@app.cell
def _(mo):
    s3_sweep = mo.ui.run_button(label="Compare the four shapes")
    mo.hstack(
        [s3_sweep, mo.md("Runs each shape with the replica, write, and trial settings above.")],
        justify="start",
    )
    return (s3_sweep,)


@app.cell
def _(alt, binary, crdt, mo, pd, s3_nodes, s3_ops, s3_pattern, s3_sweep, s3_trials, th):
    mo.stop(not s3_sweep.value)
    _rows = []
    for _shape in ["full_mesh", "star", "ring", "line"]:
        _r = th.run(
            th.topology(_shape, s3_nodes.value, _shape, s3_ops.value, s3_pattern.value),
            crdt.value,
            binary,
            trials=s3_trials.value,
        )
        _rows.append(
            {
                "shape": _shape,
                "diameter": _r.summary["diameter"],
                "links": _r.summary["edge_count"],
                "p50_ms": _r.summary["p50_ms"],
                "p95_ms": _r.summary["p95_ms"],
                "sync messages per write": _r.total("replicant.sync.messages.tx")
                / (s3_ops.value * s3_trials.value),
            }
        )
    _df = pd.DataFrame(_rows)
    _base = alt.Chart(_df).encode(y=alt.Y("shape:N", sort=None, title=None))
    _time = _base.mark_bar().encode(x=alt.X("p50_ms:Q", title="p50 convergence (ms)")) + _base.mark_rule(
        color="black"
    ).encode(x="p50_ms:Q", x2="p95_ms:Q")
    _msgs = _base.mark_bar(color="#f58518").encode(x=alt.X("sync messages per write:Q"))
    mo.vstack(
        [
            alt.hconcat(
                _time.properties(width=280, title="Time (bar p50, line to p95)"),
                _msgs.properties(width=280, title="Sync traffic"),
            ),
            mo.ui.table(_df, selection=None, show_column_summaries=False),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### How to read it

    Two effects pull against each other:

    - **Diameter.** A change must cross up to `diameter` links, one sync
      round trip each. A line of 6 has diameter 5; a full mesh has 1.
    - **Traffic.** A replica that receives new changes relays them to its
      other peers, so more links mean more messages per write. A full mesh of
      6 sends about 28 messages per write; a line sends about 5.

    In this lane, traffic usually wins: **the full mesh is the slowest shape
    for all three libraries**, although its diameter is the smallest. All
    replicas share one process and one CPU pool, so every message costs time
    that the other replicas wait for. Over a real network a hop costs more,
    so diameter can matter more there. The archived `comparison` notebook
    checks whether a ranking holds in the container lanes.

    Treat small differences between shapes as noise until the trial spread
    (section 6) says otherwise.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. What does a partition cost?

    A **partition-heal** scenario splits the replicas into groups. Each group
    writes on its own, so the groups' states diverge. Then the links between
    the groups **heal**, and the clock measures how long the merge takes.

    The order of the steps matters:

    1. **Block** every cross-group link, before it exists.
    2. **Wire** the whole final graph, blocked links included. Opening a sync
       stream costs time that grows with the number of links. Doing it here
       keeps it out of the measurement, so a dense heal does not look slow for
       a reason that has nothing to do with merging.
    3. **Write**: each group applies its writes. Blocked links drop all sync
       traffic in both directions.
    4. **Gate**: wait until each group agrees internally, then check that the
       groups really differ. If state leaked across the partition, the heal
       would measure nothing, and the run fails.
    5. **Heal**: start the clock, unblock every healed link, then send one sync
       handshake on each (`kicked`). Every unblock must finish before any
       handshake, or a handshake could reach a still-blocked peer and be
       dropped.
    6. **Poll** until every replica agrees.

    `wiring_ms` is the time of the unblock step (5). It is inside the window,
    because it is part of the heal. Report it next to `convergence_ms`; do not
    subtract it.
    """)
    return


@app.cell
def _(mo):
    s4_size = mo.ui.slider(1, 4, value=2, label="replicas per group", show_value=True)
    s4_ops = mo.ui.dropdown(
        options={"10": 10, "100": 100, "1,000": 1000},
        value="100",
        label="writes per group",
    )
    s4_workload = mo.ui.dropdown(
        options=["map_put", "text_splice"], value="text_splice", label="workload"
    )
    s4_locality = mo.ui.dropdown(
        options=["append", "random_position", "same_region"],
        value="random_position",
        label="text position",
    )
    s4_heal = mo.ui.dropdown(options=["full_mesh", "bridge"], value="full_mesh", label="heal with")
    mo.vstack(
        [
            mo.hstack([s4_size, s4_ops, s4_heal], justify="start"),
            mo.hstack([s4_workload, s4_locality], justify="start"),
            mo.md(
                """
                `text_splice` inserts one character per write into a shared
                text. The text position sets the contention: `append` writes at
                the end, `random_position` anywhere, `same_region` always at
                the start. `map_put` writes distinct map keys and ignores the
                position. `bridge` heals with one link instead of every
                cross-group pair.

                The divergence phase is not timed, but it can be slow. With
                Automerge, groups of 2 or more, and 1,000 writes per group, one
                trial takes several seconds.
                """
            ),
        ]
    )
    return s4_heal, s4_locality, s4_ops, s4_size, s4_workload


@app.cell
def _(binary, crdt, mo, s4_heal, s4_locality, s4_ops, s4_size, s4_workload, th):
    _g = s4_size.value
    heal = th.run(
        th.partition(
            "partition-heal",
            [list(range(_g)), list(range(_g, 2 * _g))],
            s4_ops.value,
            heal_topology=s4_heal.value,
            workload=s4_workload.value,
            locality=s4_locality.value,
        ),
        crdt.value,
        binary,
    )
    _edges, _blocked = heal.edges()
    _trace = heal.trial_trace()
    _p = th.phase_times(_trace)
    _row = heal.trials.iloc[0]
    mo.vstack(
        [
            mo.hstack(
                [
                    th.graph_chart(2 * _g, _edges, _blocked, title="Groups and heal links"),
                    mo.md(
                        f"""
                        | | |
                        |---|---|
                        | `convergence_ms` | **{_row["convergence_ms"]:.3f} ms** |
                        | `wiring_ms` (unblock) | {_row["wiring_ms"]:.3f} ms |
                        | divergence phase | {_p["clock_start"] - _p["wired"]:.3f} ms, not measured |
                        """
                    ),
                ],
                justify="start",
            ),
            th.timeline_chart(_trace, 2 * _g, title="One partition-heal trial"),
            mo.md(
                """
                Before `clock_start`, each group's poll shows its own colour:
                the groups agree internally but differ from each other. After
                the heal, the classes merge into one. A replica can also jump
                to a colour that no replica had before: the merged state is
                new.
                """
            ),
        ]
    )
    return


@app.cell
def _(mo):
    s4_sweep = mo.ui.run_button(label="Measure the cost of divergence")
    mo.hstack(
        [
            s4_sweep,
            mo.md(
                "Runs the scenario above with 10, 100, and 1,000 writes per "
                "group, 2 trials each."
            ),
        ],
        justify="start",
    )
    return (s4_sweep,)


@app.cell
def _(alt, binary, crdt, mo, pd, s4_heal, s4_locality, s4_size, s4_sweep, s4_workload, th):
    mo.stop(not s4_sweep.value)
    _g = s4_size.value
    _rows = []
    with mo.status.progress_bar(total=3, title="Running") as _bar:
        for _ops in [10, 100, 1000]:
            _r = th.run(
                th.partition(
                    "divergence",
                    [list(range(_g)), list(range(_g, 2 * _g))],
                    _ops,
                    heal_topology=s4_heal.value,
                    workload=s4_workload.value,
                    locality=s4_locality.value,
                ),
                crdt.value,
                binary,
                trials=2,
            )
            for _t in _r.trials.itertuples():
                _rows.append({"writes per group": _ops, "convergence_ms": _t.convergence_ms})
            _bar.update()
    _df = pd.DataFrame(_rows)
    mo.vstack(
        [
            alt.Chart(_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("writes per group:Q", scale=alt.Scale(type="log")),
                y=alt.Y("mean(convergence_ms):Q", scale=alt.Scale(type="log"), title="convergence_ms"),
            )
            .properties(width=420, height=260, title=f"Heal time against divergence, {crdt.value}"),
            mo.md(
                """
                Both axes are logarithmic. A slope of 1 means the heal time
                grows linearly with the divergence. Compare the slope between
                libraries (change the library at the top) and between text
                positions. A steeper slope means the merge costs more per
                diverged operation.
                """
            ),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 5. Do the libraries differ?

    The same scenario file runs against every library, so the workload stays
    fixed and only the library changes. Choose a scenario, then run it once
    per library.
    """)
    return


@app.cell
def _(mo, th):
    s5_preset = mo.ui.dropdown(
        options={
            "map writes, full mesh of 5": th.topology("map-mesh-5", 5, "full_mesh", 50),
            "map writes, line of 6": th.topology("map-line-6", 6, "line", 50),
            "text, 2 replicas, 1,000 random inserts each": th.partition(
                "text-random-1e3", [[0], [1]], 1000, workload="text_splice", locality="random_position"
            ),
            "text, 2 replicas, 1,000 inserts at the start each": th.partition(
                "text-same-1e3", [[0], [1]], 1000, workload="text_splice", locality="same_region"
            ),
        },
        value="map writes, full mesh of 5",
        label="scenario",
    )
    s5_trials = mo.ui.slider(1, 20, value=5, label="trials", show_value=True)
    s5_go = mo.ui.run_button(label="Run with each library")
    mo.hstack([s5_preset, s5_trials, s5_go], justify="start")
    return s5_go, s5_preset, s5_trials


@app.cell
def _(alt, binary, mo, pd, s5_go, s5_preset, s5_trials, th):
    mo.stop(not s5_go.value)
    _rows = []
    for _c in th.CRDTS:
        _r = th.run(s5_preset.value, _c, binary, trials=s5_trials.value)
        _rows.append(
            {
                "library": _c,
                "p50_ms": _r.summary["p50_ms"],
                "p95_ms": _r.summary["p95_ms"],
                "sync messages per trial": _r.total("replicant.sync.messages.tx") / s5_trials.value,
                "document bytes": _r.doc_size(),
            }
        )
    _df = pd.DataFrame(_rows)
    _base = alt.Chart(_df).encode(y=alt.Y("library:N", title=None))
    mo.vstack(
        [
            alt.hconcat(
                (
                    _base.mark_bar().encode(x=alt.X("p50_ms:Q", title="p50 convergence (ms)"))
                    + _base.mark_rule(color="black").encode(x="p50_ms:Q", x2="p95_ms:Q")
                ).properties(width=200, title="Time"),
                _base.mark_bar(color="#f58518")
                .encode(x="sync messages per trial:Q")
                .properties(width=200, title="Sync traffic"),
                _base.mark_bar(color="#54a24b")
                .encode(x="document bytes:Q")
                .properties(width=200, title="Document size"),
            ),
            mo.ui.table(_df, selection=None, show_column_summaries=False),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### What does not compare directly

    - **Who keeps the sync state.** Automerge has a sync protocol with
      per-peer state. Yrs and Loro have none, so their adapters keep a
      per-peer cache of what each peer has been sent. That cache is part of
      this harness, not of the library. A comparison of message counts or
      bytes must say so.
    - **Handshakes.** Automerge's protocol always starts with a handshake, so
      it sends messages even when there is little to say. That shows in the
      traffic of small runs.
    - **Document size** is each library's own encoding of the same content.
      It is a fair measure of storage cost, not of how much content there is.
    - **Fingerprints** differ in kind (section 1). That does not change when
      the clock stops, because each one is exact for its library.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 6. When should you distrust a number?

    One trial is one sample. The run below repeats one scenario many times so
    you can see the spread.
    """)
    return


@app.cell
def _(mo):
    s6_trials = mo.ui.slider(10, 100, value=40, step=10, label="trials", show_value=True)
    s6_go = mo.ui.run_button(label="Run")
    mo.hstack([s6_trials, s6_go], justify="start")
    return s6_go, s6_trials


@app.cell
def _(alt, binary, crdt, mo, s6_go, s6_trials, th):
    mo.stop(not s6_go.value)
    spread = th.run(th.topology("spread", 6, "ring", 30), crdt.value, binary, trials=s6_trials.value)
    _s = spread.summary
    _deferred = spread.total("replicant.sync.messages.deferred")
    _dots = (
        alt.Chart(spread.trials)
        .mark_circle(size=40)
        .encode(x=alt.X("trial:Q"), y=alt.Y("convergence_ms:Q", scale=alt.Scale(zero=True)))
    )
    _hist = (
        alt.Chart(spread.trials)
        .mark_bar()
        .encode(x=alt.X("convergence_ms:Q", bin=alt.Bin(maxbins=30)), y="count()")
    )
    mo.vstack(
        [
            alt.hconcat(
                _dots.properties(width=320, title="Each trial, in order"),
                _hist.properties(width=320, title="Distribution"),
            ),
            mo.md(
                f"""
                Ring of 6, 30 writes, **{crdt.value}**: p50 **{_s["p50_ms"]:.2f} ms**,
                p95 **{_s["p95_ms"]:.2f} ms**, mean {_s["mean_ms"]:.2f} ms.
                Deferred flushes: **{_deferred}**.
                """
            ),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### A checklist for reading a sweep

    - **Look at p95, not only p50.** A large gap means a few slow trials. The
      mean hides which.
    - **Look at the trials in order.** Early trials can be slower while caches
      and the allocator warm up. A trend over trial number is drift on the
      host, not a property of the library.
    - **Check the deferred counter.** Each sync link has a buffer of 64
      messages. When it is full, the replica defers the flush instead of
      waiting. Any deferral means that the timings include those stalls. In
      the container stacks the bench recipes record this counter; see
      `docs/metrics.md`.
    - **Know the lane.** In-process replicas share one process and one CPU
      pool, and sync goes over loopback. The container stacks add real
      process and network boundaries. The archived `comparison` notebook
      checks whether a finding survives the change of lane.
    - **Keep the provenance.** Run sweeps with `--provenance`. It records the
      commit, host, build profile, seeds, and cell parameters. `results/` is
      not in version control, so without it a CSV cannot be traced to the
      code that produced it.
    - **Do not measure with `--trace`.** Tracing reads the clock and compares
      fingerprints inside the timed window.
    """)
    return


if __name__ == "__main__":
    app.run()
