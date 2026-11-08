import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="Replicant tour")


with app.setup:
    import marimo as mo
    import altair as alt
    import pandas as pd

    import tour_helpers as th


@app.cell
def _():
    mo.md(r"""
    # A guided tour of Replicant

    Replicant compares CRDT libraries. It starts replicas, wires them into a
    graph, applies writes, and measures how long the replicas take to agree. It
    also replays recorded editing traces through each library. This notebook
    explains how Replicant does this, and it runs small examples live, so that
    you can see each step and change it.

    It assumes that you know what a CRDT is. It does not assume that you know
    Replicant. The sections:

    1. **Principles**: what Replicant measures, and the rules that it keeps.
    2. **Architecture**: the crates, the adapter trait, the gRPC services, the
       sync protocols, and the lanes.
    3. **Steady-state scenarios**: one trial, step by step, and topologies.
    4. **Partition and heal**: the divergence experiment, and the contention
       model.
    5. **Convergence and the poll**: fingerprints, and the resolution of a
       measurement.
    6. **Libraries**: one scenario on each library.
    7. **Traces and replay**: from crdt-benchmarks to a replay.
    8. **Measurement faults**: four faults that we found, each as a worked
       example.
    9. **Lanes**: how the lane changes the answer.
    10. **What is open**.

    For the results of the divergence study, open the second notebook,
    [`divergence.py`](divergence.py) (`just divergence-notebook`). This tour
    does not repeat them.

    By default every run uses the **docker lane**: each replica is its own
    container in a docker compose stack, and sync goes over real TCP between
    containers. If Docker is not running, the notebook uses the **in-process
    lane**, where every replica is a task inside the orchestrator process. If
    `kind` and `kubectl` are installed, you can also pick the **k8s lane**: each
    replica is a pod on a local kind cluster.

    **Every live cell is timed work.** Do not compile or run other benchmarks
    while a cell runs, because they change the numbers.
    """)
    return


@app.cell
def _():
    with mo.status.spinner(title="Building the release orchestrator and replay binaries…"):
        binary = th.build()
    docker_up = th.docker_available()
    kind_up = docker_up and th.kind_installed()
    traces_ok = th.traces_available()
    mo.md(
        f"Orchestrator ready: `{binary.relative_to(th.REPO)}`. Replay ready: "
        f"`{th.REPLAY.relative_to(th.REPO)}`. Trace files in `traces/`: "
        + ("**present**." if traces_ok else "**missing**. Run `just gen-traces <clone>` to make them (section 7).")
    )
    return binary, docker_up, kind_up, traces_ok


@app.cell
def _(docker_up, kind_up):
    crdt = mo.ui.dropdown(options=list(th.CRDTS), value="automerge", label="CRDT library")
    lane = mo.ui.dropdown(
        options=["docker", "k8s", "in-process"] if kind_up else ["docker", "in-process"],
        value="docker" if docker_up else "in-process",
        label="lane",
    )
    if not docker_up:
        _note = (
            "**Docker is not running**, so the lane is in-process. Start Docker "
            "(for example `colima start`) and re-run this cell to use the docker lane."
        )
    elif not kind_up:
        _note = (
            "Every section uses these settings. Change them at any time. "
            "Install `kind` and `kubectl` to also use the k8s lane."
        )
    else:
        _note = "Every section uses these settings. Change them at any time."
    mo.callout(
        mo.hstack([crdt, lane, mo.md(_note)], justify="start"),
        kind="info" if docker_up else "warn",
    )
    return crdt, lane


@app.cell
def _(binary, crdt, lane):
    stack = th.STACKS.get(lane.value)
    if stack is not None:
        with mo.status.spinner(
            title=f"Starting a {stack.size}-replica {stack.LANE} stack with {crdt.value}…",
            subtitle=(
                "The first start creates a kind cluster and builds the replica "
                "image, which takes a few minutes."
                if stack.LANE == "k8s"
                else "The first start builds the replica image, which takes a few minutes."
            ),
        ) as _spinner:
            stack.ensure(crdt.value, on_line=lambda line: _spinner.update(subtitle=line[-120:]))

    def go(scenario, trials=1, library=None, env=None):
        """Runs `scenario` with the library and lane chosen above.

        `library` overrides the library for one run. `env` adds environment
        variables to the orchestrator.
        """
        return th.run(
            scenario, library or crdt.value, binary, trials=trials, lane=lane.value, env=env
        )

    _k8s_note = (
        " The orchestrator reaches each pod through `kubectl port-forward`, so "
        "every control call and every poll takes longer than in docker. Sync "
        "between the pods stays inside the cluster."
    )
    mo.md(
        f"Lane: **{lane.value}**"
        + (
            f", a stack of {stack.size} replicas running **{stack.crdt}**. "
            "A scenario uses the first replicas it needs."
            + (_k8s_note if stack.LANE == "k8s" else "")
            if stack is not None
            else "."
        )
    )
    return go, stack


@app.cell
def _(stack):
    _panels = mo.hstack(
        [
            mo.Html(
                f'<iframe src="{stack.panel_url(_id)}" width="480" height="260" '
                'frameborder="0"></iframe>'
            )
            for _id in (3, 4)
        ]
        if stack is not None
        else [],
        justify="start",
    )
    mo.accordion(
        {
            "Live dashboard (docker and k8s lanes)": mo.vstack(
                [
                    mo.md(
                        f"""
                        The stack also runs Prometheus and Grafana. These two
                        panels show the replicas' OTel sync counters, live.
                        Replicas export every 2 s, so a run shows up a few
                        seconds after it ends. The counters add up over every
                        run since the stack started.
                        [Open the full dashboard]({stack.dashboard_url()}).
                        """
                    ),
                    _panels,
                ]
            )
            if stack is not None
            else mo.md("The dashboard needs the docker or the k8s lane.")
        }
    )
    return


@app.cell
def _():
    mo.md(r"""
    ## 1. Principles

    ### What Replicant measures, and why

    A CRDT library promises that replicas converge. A benchmark of one library
    in one process measures how fast it applies operations. It does not measure
    what a user of a replicated system waits for. Replicant measures three
    things:

    - **Convergence time.** Replicas in a graph receive writes, sync with their
      neighbours, and reach the same state. The clock stops when every replica
      reports the same state. This includes the sync protocol, the network, and
      the relay through the graph. Sections 3 and 4.
    - **Merge cost after divergence.** Groups of replicas edit apart, then
      reconnect. The clock measures the heal. This is the question of the
      divergence study: how does the merge cost grow with the divergence and
      with the conflict between the two sides? Section 4.
    - **The cost of a recorded history.** The replay applies a trace, such as a
      crdt-benchmarks benchmark, through each library, with no sync protocol.
      This connects Replicant's numbers to published ones. Section 7.

    Every comparison is **library-agnostic**: each library sits behind one
    trait, `CrdtAdapter`, and the scaffolding around it is the same. So the
    same scenario file runs on every library, and only the library changes.
    """)
    return


@app.cell
def _():
    mo.md(r"""
    ### The rules that Replicant keeps

    Each rule exists because a result without it described the harness, not
    the library. Section 8 shows four faults that broke one of these rules.

    | Rule | Why | Where |
    |---|---|---|
    | **Seed every workload.** Each insert position comes from a seeded PRNG: `seed = f(cell, replica, repetition)`. | Each library gets the same workload, so a difference between libraries is not sampling noise. | [`workload.rs`](../crates/orchestrator/src/workload.rs) (`seed_for`, `SplitMix64`) |
    | **Seed the library ids.** Repetition *r* of every cell, on every library, uses the same ids. | A library breaks ties between concurrent ops with the replica ids, and some id pairs change the cost of a merge. | `id_seed_for`, [`instance_ids.rs`](../crates/common/src/instance_ids.rs) |
    | **Keep setup out of the clock.** Wire every link before the clock starts. Read the stats after it stops. | Opening a sync stream costs a TCP and HTTP/2 handshake that grows with the number of links. | [`runner.rs`](../crates/orchestrator/src/runner.rs), [`partition_heal.rs`](../crates/orchestrator/src/partition_heal.rs) |
    | **Check the result, not only the agreement.** A partition must hold. The merged text must hold every insert. A replay must pass its oracle. A failed check is an error, never a number. | Equal fingerprints prove only that the replicas agree. A merge that loses one side's work also agrees. | `verify_groups_diverged`, `verify_text_length`, [`oracle.rs`](../crates/replay/src/oracle.rs) |
    | **Record the provenance.** `--provenance` writes the commit, host, build profile, seeds, ids, and cell parameters before the first trial. | `results/` is not in version control, so a CSV alone cannot say which code made it. | [`provenance.rs`](../crates/orchestrator/src/provenance.rs) |
    | **Use a release build, and do not trace a measurement.** | A debug build is much slower. `--trace` reads the clock and makes extra calls in the timed window. | `th.build()`, `--trace` |
    | **Run one timed job at a time.** Do not compile, and do not run a second benchmark, while a run goes. | Every replica of the docker lane shares one host. Another job takes the same CPUs. | This notebook, and the bench scripts |

    The cell below asks the orchestrator what it *would* run for a small
    partition-heal scenario, with `--dry-run --provenance`. No replica starts.
    This is the record that a real run writes beside its CSV.
    """)
    return


@app.cell
def _(binary, crdt):
    _cell = th.partition(
        "provenance-example", [[0], [1]], 100, workload="text_splice", locality="random_position"
    )
    provenance_example = th.dry_run([_cell], crdt.value, binary)[0]
    mo.vstack(
        [
            mo.json(provenance_example, label="scenarios[0] of the provenance file"),
            mo.md(
                """
                Look at `seeds`: one seed for each replica of each repetition.
                `instance_ids` gives the id seed of each repetition, and the
                rule that makes it. `achieved_contention` is the contention
                model of section 4, computed from the same seeds with no
                library.
                """
            ),
        ]
    )
    return


@app.cell
def _():
    mo.md(r"""
    ## 2. Architecture

    ### The crates

    Replicant is one Cargo workspace with four crates. An arrow means "depends
    on".
    """)
    return


@app.cell
def _():
    mo.mermaid(
        """
        flowchart TB
            common["<b>common</b><br/>proto (gRPC types), CrdtAdapter, Op,<br/>NodeId, SplitMix64, seeded ids, provenance"]
            replica["<b>replica</b><br/>the three adapters, the gRPC server<br/>binary: replica"]
            orch["<b>orchestrator</b><br/>scenarios, the two runners, the poll,<br/>contention, provenance, --trace<br/>binary: orchestrator"]
            replay["<b>replay</b><br/>traces, specs, driver, oracle<br/>binary: replay"]
            replica --> common
            orch --> replica
            orch --> common
            replay --> replica
            replay --> common
        """
    )
    return


@app.cell
def _():
    _v = th.library_versions()
    mo.md(rf"""
    | Crate | What it holds | Start reading at |
    |---|---|---|
    | `common` | The gRPC API ([`replicant.proto`](../proto/replicant.proto)), the op model (`Op`, `ScalarVal`), the `CrdtAdapter` trait, the seeded PRNG, and the seeded library ids. | [`adapter.rs`](../crates/common/src/adapter.rs) |
    | `replica` | One adapter per library, and the server that wraps an adapter in two gRPC services. The `replica` binary is one container of the docker lane. | [`server/state.rs`](../crates/replica/src/server/state.rs) |
    | `orchestrator` | Reads scenario files, wires replicas, applies writes, polls until agreement, and writes the results. It can also start replicas in its own process. | [`main.rs`](../crates/orchestrator/src/main.rs) |
    | `replay` | Reads a trace and a spec, moves updates between replicas itself, times the replay, and checks the final content. | [`driver.rs`](../crates/replay/src/driver.rs) |

    ### The `CrdtAdapter` trait

    The trait is the boundary between Replicant and a library. The scaffolding
    times each call and records the metrics, so an adapter does not. All
    methods take `&mut self`, because Automerge commits a pending transaction
    before a read.

    | Group | Methods | Who calls them, and when |
    |---|---|---|
    | Document | `apply_op`, `ensure_text`, `ensure_list` | The orchestrator, through `ApplyOp` and `EnsureText`. `ensure_text` runs on every replica before the partition, so that both sides edit one shared text object. |
    | State | `state_fingerprint`, `get_heads`, `doc_size_bytes`, `text_length` | The orchestrator's poll, and the checks after the clock stops. |
    | Sync | `sync_generate`, `sync_receive`, `sync_pending`, `sync_reset`, `sync_reconnect` | The replica's own sync loop. `sync_generate` must return `None` at some point, or the replicas send messages without end. |
    | Lifecycle | `reset`, `set_instance_id` | The start of each trial. |
    | Replay | `apply_txn`, `apply_updates`, `encode`, `load`, `read_text`, `read_list`, `read_map` | The replay driver, in process or through gRPC. |

    ### The three adapters

    | | Automerge {_v.get("automerge", "?")} | Yrs {_v.get("yrs", "?")} | Loro {_v.get("loro", "?")} |
    |---|---|---|---|
    | Sequence CRDT | RGA | YATA | descends from Fugue |
    | Sync protocol | Automerge's own (`sync::State` per peer) | Yjs-style `Frame`, kept by the adapter | Yjs-style `Frame`, kept by the adapter |
    | An unblock keeps | the heads that both peers have | nothing: a new version exchange | nothing: a new version exchange |
    | Fingerprint | the sorted change-hash heads | the sorted state vector, then the delete set | the sorted causal frontier |
    | Root objects | created by an op, so the adapter needs a fixed bootstrap actor | named lookups, no op | `(name, type)` containers, always present |
    | Id bits kept | all 128 | 52 (a client id has 53 bits, and the adapter sets the top one) | 64 |

    `Crdt::build` in [`adapter.rs`](../crates/replica/src/adapter.rs) picks the
    adapter. A conformance suite
    ([`conformance.rs`](../crates/replica/src/adapter/conformance.rs)) runs the
    same tests on each adapter.
    """)
    return


@app.cell
def _():
    mo.md(r"""
    ### The gRPC services

    A replica serves two services from
    [`replicant.proto`](../proto/replicant.proto):

    - **`Replica`**: control calls from the orchestrator or the replay driver.
      - Writes and reads: `ApplyOp`, `GetStateFingerprint`, `GetTextLength`,
        `GetStats`.
      - Wiring: `ConnectPeer` opens a sync stream to a peer. `SetPeerLinks`
        blocks or unblocks the link to a peer and keeps the stream open.
        `KickSync` sends a new sync message to each named peer. `SyncPending`
        names the peers that have a message waiting.
      - Lifecycle: `Reset`, `EnsureText`, `SetInstanceId`.
      - The replay surface: `ApplyTxn`, `ApplyUpdates`, `Encode`, `Load`,
        `ReadText`, `ReadList`, `ReadMap`. Each reply gives `adapter_ns`, the
        time inside the adapter.
    - **`Sync`**: one bidirectional `Stream` per link. It carries the adapter's
      sync messages as opaque bytes. Only the same adapter on the other side
      decodes them.

    **The orchestrator never carries sync traffic.** It wires the replicas,
    sends the writes, and polls.

    When a replica applies a write or receives a sync message, it calls
    `sync_generate` for each peer and puts the message on that peer's outbound
    channel. The channel holds 64 messages. If it is full, the replica
    **defers** the flush and counts it, and it does not wait. Each received
    message causes more calls, so a change relays through the graph until every
    call returns `None`.

    ### The sync protocols

    Automerge has a sync protocol with per-peer state: each side sends its heads
    and a Bloom filter of its changes, and the other side sends the changes
    that the filter does not hold. Yrs and Loro have no sync protocol, so their
    adapters add one that follows the Yjs sync protocol. One message is a
    `Frame` ([`frame.rs`](../crates/replica/src/adapter/frame.rs)): a flag byte
    (bit 1: has a version, bit 2: has an update, bit 4: asks for the version),
    the sender's version vector, and an update. The first contact between two
    replicas takes three messages:
    """)
    return


@app.cell
def _():
    mo.mermaid(
        """
        sequenceDiagram
            participant A as replica A (Yrs or Loro)
            participant B as replica B
            A->>B: Frame { version: SV(A), asks_version }
            Note right of B: B now knows SV(A)
            B->>A: Frame { version: SV(B), update: what SV(A) lacks }
            Note left of A: A now knows SV(B)
            A->>B: Frame { update: what SV(B) lacks }
            Note over A,B: After this, each side sends only what it adds.<br/>The adapter records what it sent, so it does not send it again.
        """
    )
    return


@app.cell
def _():
    mo.md(r"""
    So for Yrs and Loro, the per-peer record of what each peer has is part of
    this harness, not of the library. A comparison of message counts or bytes
    between Automerge and the other two must say so.

    ### One partition-heal trial, call by call

    This diagram shows the calls of one trial of section 4, with two replicas.
    The steps above the clock are outside the measurement.
    """)
    return


@app.cell
def _():
    mo.mermaid(
        """
        sequenceDiagram
            participant O as orchestrator
            participant A as replica 0
            participant B as replica 1
            Note over O,B: Setup (not timed)
            O->>A: Reset, SetInstanceId, Load (start document), EnsureText
            O->>B: Reset, SetInstanceId, Load (start document), EnsureText
            O->>A: ConnectPeer(replica 1)
            A-->>B: sync stream opens, versions exchange
            loop until idle on two polls in a row
                O->>A: SyncPending, GetStats
                O->>B: SyncPending, GetStats
            end
            O->>A: SetPeerLinks(block replica 1)
            O->>B: SetPeerLinks(block replica 0)
            Note over O,B: Divergence (not timed)
            O->>A: ApplyOp x n
            O->>B: ApplyOp x n
            O->>A: GetStateFingerprint
            O->>B: GetStateFingerprint
            Note over O: gate: the groups must differ
            Note over O,B: Heal (timed)
            O->>A: SetPeerLinks(unblock) = reconnect
            O->>B: SetPeerLinks(unblock) = reconnect
            O->>A: KickSync(replica 1)
            O->>B: KickSync(replica 0)
            A-->>B: sync messages
            B-->>A: sync messages
            loop each poll round, then wait 1 ms
                O->>A: GetStateFingerprint
                O->>B: GetStateFingerprint
            end
            Note over O: clock stops: all fingerprints equal
            O->>A: GetStats, GetTextLength
            O->>B: GetStats, GetTextLength
            Note over O: gate: text length = start + all inserts
        """
    )
    return


@app.cell
def _():
    mo.md(r"""
    ### The lanes

    A **lane** is where the replicas run. The scenario and the library stay the
    same, and the lane changes what is in the timed path.

    | Tool | Lane | Replicas | In the timed path |
    |---|---|---|---|
    | orchestrator | in-process | Tokio tasks in the orchestrator process (`--crdt`) | the library, gRPC over loopback, one shared CPU pool |
    | orchestrator | docker | one container per replica ([`scripts/docker.sh`](../scripts/docker.sh), `--replicas`) | the library, gRPC across a container network, one host |
    | orchestrator | k8s | one pod per replica on a kind cluster ([`scripts/k8s.sh`](../scripts/k8s.sh)) | as docker, plus `kubectl port-forward` for each control call |
    | replay | in process | adapters in the replay process | the library only |
    | replay | remote | replicas over gRPC (`--replicas`) | the library, gRPC, and transport. `adapter_ms` gives the library part. |

    In a container lane, the library is a deployment argument, not a scenario
    field, so one stack runs one library. This notebook restarts the replicas
    when the library changes. See [`docs/deployment.md`](../docs/deployment.md).

    ### The smallest run
    """)
    return


@app.cell
def _(crdt, go):
    first = go(th.topology("two-replicas", 2, "full_mesh", op_count=3, write_pattern="concentrated"))
    _links = pd.DataFrame(first.link_stats())
    mo.vstack(
        [
            mo.md(
                f"""
                Two replicas, one link, and three map writes, all to replica 0,
                with **{crdt.value}**. This is the row that the orchestrator
                reports for the trial:
                """
            ),
            mo.ui.table(first.trials, selection=None, show_column_summaries=False),
            mo.md(
                """
                And this is the trace of the same trial, from `--trace`. Each
                line is one event. A `poll` gives each replica's fingerprint
                **class**: replicas in the same class have the same state. The
                `stats` events at the end are each replica's sync counts, read
                after the clock stopped.
                """
            ),
            mo.ui.table(first.trace_table(), selection=None, show_column_summaries=False),
            mo.md(
                """
                The `stats` events as a table. `tx` and `rx` count the sync
                messages on the link to each peer. Change the library at the
                top and compare: Automerge's handshake sends more messages
                than the three-message `Frame` exchange.
                """
            ),
            mo.ui.table(_links, selection=None, show_column_summaries=False),
        ]
    )
    return


@app.cell
def _():
    mo.md(r"""
    ## 3. Steady-state scenarios

    ### What does one trial measure?

    A steady-state trial
    ([`runner.rs`](../crates/orchestrator/src/runner.rs)) does these steps, in
    order:

    1. **Get the replicas** and reset them, so each trial starts empty.
    2. **Wire every link.** This is outside the clock.
    3. **Start the clock** (`clock_start`).
    4. **Apply the writes**, one RPC each. Sync starts as soon as a write lands.
    5. **Poll**: ask each replica for its fingerprint in turn, wait 1 ms, and
       do it again. Section 5 explains what this does to the resolution.
    6. **Stop the clock** at the first poll where every replica agrees.

    `convergence_ms` is the time from step 3 to step 6. It **includes the time
    to send the writes**. The clock must start before the first write: sync is
    fast enough that it often finishes before the first poll returns, so a
    clock that started after the writes would often read zero.

    Change the run below and watch the timeline.
    """)
    return


@app.cell
def _():
    s2_nodes = mo.ui.slider(2, 8, value=4, label="replicas", show_value=True)
    s2_ops = mo.ui.slider(1, 200, value=20, label="writes", show_value=True)
    s2_interval = mo.ui.slider(0, 5, value=0, label="pause between writes (ms)", show_value=True)
    mo.hstack([s2_nodes, s2_ops, s2_interval], justify="start")
    return s2_interval, s2_nodes, s2_ops


@app.cell
def _(go, s2_interval, s2_nodes, s2_ops):
    one = go(
        th.topology(
            "one-trial",
            int(s2_nodes.value),
            "full_mesh",
            int(s2_ops.value),
            op_interval_ms=int(s2_interval.value),
        )
    )
    _trace = one.trial_events()
    _p = th.phase_times(_trace)
    _measured = _p["converged"] - _p["clock_start"]
    _writing = _p["last_write"] - _p["clock_start"]
    _polls = th.poll_count(_trace)
    mo.vstack(
        [
            th.timeline_chart(_trace, int(s2_nodes.value), title="One trial, full mesh"),
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
def _(one):
    _meaning = {
        "row_type": "`trial` for one trial, `summary` for the scenario.",
        "scenario": "The scenario name.",
        "trial": "The 1-based trial number. It is also the repetition that seeds the ids.",
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
def _():
    mo.md(r"""
    ### Does the topology change convergence?

    Each replica syncs only with the replicas it has a link to. A write must
    therefore travel up to **diameter** hops before every replica has it.
    Replicas relay: when a replica receives new changes, it sends sync
    messages to its other peers.

    The graph below shows the links. A link's width is the number of sync
    messages it carried, in both directions, over all trials.
    """)
    return


@app.cell
def _():
    s3_conn = mo.ui.dropdown(
        options=["full_mesh", "ring", "line", "star"], value="ring", label="shape"
    )
    s3_nodes = mo.ui.slider(3, 10, value=5, label="replicas", show_value=True)
    s3_pattern = mo.ui.dropdown(
        options=["round_robin", "concentrated"], value="round_robin", label="writes go to"
    )
    s3_ops = mo.ui.slider(5, 200, value=10, label="writes", show_value=True)
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
def _(go, s3_conn, s3_nodes, s3_ops, s3_pattern, s3_trials):
    topo = go(
        th.topology("topology", int(s3_nodes.value), s3_conn.value, int(s3_ops.value), s3_pattern.value),
        trials=s3_trials.value,
    )
    _edges, _ = topo.edges()
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
    mo.hstack(
        [
            th.graph_chart(
                int(s3_nodes.value),
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
                        | sync messages per write | {topo.sync_messages() / (s3_ops.value * s3_trials.value):.1f} |
                        """
                    ),
                ]
            ),
        ],
        justify="start",
    )
    return


@app.cell
def _():
    s3_sweep = mo.ui.run_button(label="Compare the four shapes")
    mo.hstack(
        [s3_sweep, mo.md("Runs each shape with the replica, write, and trial settings above.")],
        justify="start",
    )
    return (s3_sweep,)


@app.cell
def _(go, s3_nodes, s3_ops, s3_pattern, s3_sweep, s3_trials):
    mo.stop(not s3_sweep.value)
    _rows = []
    for _shape in ["full_mesh", "star", "ring", "line"]:
        _r = go(
            th.topology(_shape, int(s3_nodes.value), _shape, int(s3_ops.value), s3_pattern.value),
            trials=s3_trials.value,
        )
        _rows.append(
            {
                "shape": _shape,
                "diameter": _r.summary["diameter"],
                "links": _r.summary["edge_count"],
                "p50_ms": _r.summary["p50_ms"],
                "p95_ms": _r.summary["p95_ms"],
                "sync messages per write": _r.sync_messages() / (s3_ops.value * s3_trials.value),
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
def _():
    mo.md(r"""
    ### How to read it

    Two effects pull against each other:

    - **Diameter.** A change must cross up to `diameter` links, one sync
      round trip each. A line of 5 has diameter 4; a full mesh has 1.
    - **Traffic.** A replica that receives new changes relays them to its
      other peers, so more links mean more messages per write. A full mesh
      sends several times as many messages per write as a line.

    Which effect wins depends on the load. With a light load (the default 10
    writes), diameter wins in the docker lane: **the line is the slowest
    shape**. Each hop crosses a real network stack, so hops are expensive.
    Raise the writes to 50 and compare again: the traffic grows, and the four
    shapes come close together.

    Treat small differences between shapes as noise until the trial spread
    (section 8) says otherwise. Section 9 shows that the in-process lane gives
    a different answer.
    """)
    return


@app.cell
def _():
    mo.md(r"""
    ## 4. Partition and heal

    A **partition-heal** scenario splits the replicas into groups. Each group
    writes on its own, so the groups' states diverge. Then the links between
    the groups **heal**, and the clock measures how long the merge takes. This
    is one **cell** of the divergence study. The runner is
    [`partition_heal.rs`](../crates/orchestrator/src/partition_heal.rs). The
    order of the steps matters:

    1. **Reset** every replica, and **seed its library id** from the
       repetition.
    2. **Load the starting document**, if the cell has one, and create the
       shared text object on every replica.
    3. **Wire the whole final graph**, with every link open, and wait until the
       sync on every link is **idle**: no replica has a message to send, and
       every sent message has arrived, on two polls in a row. Opening a stream
       costs time that grows with the number of links, so this keeps it out of
       the measurement.
    4. **Block** every cross-group link. A blocked link stays open but carries
       no data in either direction. Because sync was idle, both sides know what
       the other has.
    5. **Diverge**: each group applies its writes.
    6. **Gate**: wait until each group agrees internally, then check that the
       groups differ. If state leaked across the partition, the heal would
       measure nothing, and the run fails.
    7. **Heal**: start the clock, unblock every healed link, then send one sync
       message on each (`kicked`). Every unblock must finish before any kick,
       or a kick could reach a still-blocked peer and be dropped.
    8. **Poll** until every replica agrees, then check that the text holds the
       starting document and every insert.

    `wiring_ms` is the time of the unblock round in step 7. It is inside the
    window, because it is part of the heal. Report it next to
    `convergence_ms`; do not subtract it.

    ### The reconnect heal

    An unblock is a **reconnect**, not a first contact
    (`CrdtAdapter::sync_reconnect`). Each replica keeps only the sync state
    that its library keeps across a disconnect:

    | Library | Keeps at an unblock | So the heal sends |
    |---|---|---|
    | Automerge | the heads that both peers have (`sync::State::decode(&state.encode())`) | only the changes after the shared heads |
    | Yrs, Loro | nothing | a new version exchange, then what each side lacks |

    Two other choices are wrong. A first contact makes Automerge send its whole
    history again. Keeping *all* the state keeps the peer's last request, and
    Automerge then tests the whole history against it. On the B4 paper, an
    Automerge heal of one op per side takes about 54 ms as a reconnect, about
    630 ms as a first contact, and about 190 ms with all state kept. The probe
    is
    `tools/findings` (`automerge-sync-state`), and
    [`divergence.py`](divergence.py) section 4 has the data.
    """)
    return


@app.cell
def _(traces_ok):
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
    s4_start = mo.ui.dropdown(
        options=["empty", "B4 paper"] if traces_ok else ["empty"],
        value="empty",
        label="start from",
    )
    mo.vstack(
        [
            mo.hstack([s4_size, s4_ops, s4_heal], justify="start"),
            mo.hstack([s4_workload, s4_locality, s4_start], justify="start"),
            mo.md(
                """
                `text_splice` inserts one character per write into a shared
                text. The text position sets the contention: `append` writes at
                the end, `random_position` anywhere, `same_region` always at
                the start. `map_put` writes distinct map keys and ignores the
                position. `bridge` heals with one link instead of every
                cross-group pair. `B4 paper` starts every replica from the
                LaTeX paper of crdt-benchmarks (104,852 characters, 259,778
                transactions of history). It needs `text_splice` and the
                trace files. Each run loads the paper again: in the docker
                lane this takes more than a minute per run, before the
                trial starts.

                The divergence phase is not timed, but it can be slow. With
                Automerge, groups of 2 or more, and 1,000 writes per group, one
                trial takes several seconds.
                """
            ),
        ]
    )
    return s4_heal, s4_locality, s4_ops, s4_size, s4_start, s4_workload


@app.cell
def _(go, s4_heal, s4_locality, s4_ops, s4_size, s4_start, s4_workload):
    _g = int(s4_size.value)
    _paper = s4_start.value == "B4 paper"
    mo.stop(
        _paper and s4_workload.value != "text_splice",
        mo.callout(mo.md("A starting document needs the `text_splice` workload."), kind="warn"),
    )
    heal = go(
        th.partition(
            "partition-heal",
            [list(range(_g)), list(range(_g, 2 * _g))],
            s4_ops.value,
            heal_topology=s4_heal.value,
            workload=s4_workload.value,
            locality=s4_locality.value,
            baseline=th.PAPER if _paper else None,
        )
    )
    _edges, _blocked = heal.edges()
    _trace = heal.trial_events()
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
    return (heal,)


@app.cell
def _():
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
def _(crdt, go, s4_heal, s4_locality, s4_size, s4_start, s4_sweep, s4_workload):
    mo.stop(not s4_sweep.value)
    _g = int(s4_size.value)
    _rows = []
    with mo.status.progress_bar(total=3, title="Running") as _bar:
        for _ops in [10, 100, 1000]:
            _r = go(
                th.partition(
                    "divergence",
                    [list(range(_g)), list(range(_g, 2 * _g))],
                    _ops,
                    heal_topology=s4_heal.value,
                    workload=s4_workload.value,
                    locality=s4_locality.value,
                    baseline=th.PAPER if s4_start.value == "B4 paper" else None,
                ),
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
                diverged operation. The full grid, from 10² to 10⁵ ops per
                side on all three libraries, is in
                [`divergence.py`](divergence.py).
                """
            ),
        ]
    )
    return


@app.cell
def _():
    mo.md(r"""
    ### The contention model

    An insert in a sequence CRDT attaches to an existing element, its
    **anchor**. Two inserts from different sides on the same anchor are
    concurrent siblings, and the library must order them: that is the merge
    work that conflict adds. The text position is meant to control how many
    inserts contest an anchor. The **contention model**
    ([`contention.rs`](../crates/orchestrator/src/contention.rs)) checks that
    claim. It reads the same seeded positions that the run sends, and counts
    the contested inserts. No library runs, so the numbers describe the
    workload only.

    With an empty start and one replica per side, HEAD is the only anchor that
    both sides can see, so the model counts the inserts at position 0. With a
    starting document, every character of it can be an anchor, and the model
    tracks which element each position names. A test checks both shortcuts
    against a full simulation.

    The chart below comes from `--dry-run` on two singleton groups, so it takes
    no timed work.
    """)
    return


@app.cell
def _(traces_ok):
    s4c_start = mo.ui.radio(
        options=["empty", "B4 paper"], value="empty", label="start from", inline=True
    )
    mo.hstack(
        [
            s4c_start,
            mo.md(
                "The model needs only the length of the paper, not the trace file."
                + ("" if traces_ok else " The trace files are missing, but this still works.")
            ),
        ],
        justify="start",
    )
    return (s4c_start,)


@app.cell
def _(binary, crdt, s4c_start):
    _cells = [
        th.partition(
            f"{_loc}-{_ops}",
            [[0], [1]],
            _ops,
            workload="text_splice",
            locality=_loc,
            baseline=th.PAPER if s4c_start.value == "B4 paper" else None,
        )
        for _loc in ("append", "random_position", "same_region")
        for _ops in (100, 1000, 10_000, 100_000)
    ]
    _rows = []
    for _cell, _prov in zip(_cells, th.dry_run(_cells, crdt.value, binary)):
        _body = _cell["partition_heal"]
        _c = _prov["achieved_contention"]
        _rows.append(
            {
                "locality": _body["locality"],
                "ops per side": _body["ops_per_group"],
                "contested ops": _c["contested_ops"]["mean"],
                "contested share": _c["contested_ops"]["mean"] / (2 * _body["ops_per_group"]),
                "contested anchors": _c["contested_anchors"]["mean"],
                "most siblings on one anchor": _c["max_concurrent_siblings"]["mean"],
            }
        )
    _df = pd.DataFrame(_rows)
    mo.vstack(
        [
            alt.Chart(_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("ops per side:Q", scale=alt.Scale(type="log")),
                y=alt.Y("contested share:Q", axis=alt.Axis(format="%")),
                color=alt.Color("locality:N", sort=["append", "random_position", "same_region"]),
                tooltip=list(_df.columns),
            )
            .properties(width=460, height=240, title=f"Contested share, start: {s4c_start.value}"),
            mo.ui.table(_df, selection=None, show_column_summaries=False),
            mo.md(
                """
                Compare `random_position` with each start. From an empty
                document, only HEAD can be contested, and a random position is
                0 less often as the text grows, so the share **falls** as the
                ops grow. From the paper, every character is an anchor that
                both sides can see. With more inserts per side, the two sides
                hit more of the same characters, so the share **rises**, to
                about a third at 10⁵. So in the divergence grid, which starts
                from the paper, more ops also mean more conflict. To measure
                conflict alone, a later grid needs cells with a fixed op count
                and a varied contested share.
                """
            ),
        ]
    )
    return


@app.cell
def _():
    mo.md(r"""
    ## 5. Convergence and the poll

    ### What "converged" means

    Each adapter returns a **state fingerprint**: opaque bytes that are equal
    if and only if two replicas hold the same state. The orchestrator compares
    the bytes and does not interpret them. A trial has converged when every
    replica returns the same non-empty fingerprint. An empty fingerprint means
    that the replica has no operations yet, so an untouched run cannot
    "converge" at once.

    | Library | Fingerprint | Why |
    |---|---|---|
    | Automerge | the sorted change-hash heads | Equal heads mean equal history, so equal state. |
    | Loro | the sorted causal frontier | The same argument as Automerge. The snapshot bytes are not used, because two converged replicas can produce different snapshots. |
    | Yrs | the sorted state vector, then the encoded delete set | A Yrs delete does not advance the state vector, so the state vector alone would miss a delete. The delete set encoding is canonical. |

    So "the same fingerprint" is exact for each library, but the fingerprints
    of two libraries do not compare with each other.

    ### The resolution of `convergence_ms`

    The poll ([`convergence.rs`](../crates/orchestrator/src/replicas/convergence.rs),
    `wait_for_nodes`) asks each replica for its fingerprint **in turn**, and
    then waits 1 ms. So the clock can stop only at the end of a poll round, and
    `convergence_ms` changes in steps of one round. Two more facts make the
    fingerprint cost important:

    - A round is short only if each fingerprint is cheap. The fingerprint must
      come from the version, not from a full encode of the document.
    - The fingerprint call holds the adapter's lock. While a replica computes
      its fingerprint, it cannot apply a sync message. A slow fingerprint thus
      also delays the sync that it measures.

    The cell below reads the timed steps of the heal trial of section 4 from
    its trace. Change the settings in section 4 and watch them.
    """)
    return


@app.cell
def _(heal, lane):
    _events = heal.trial_events()
    _p = th.phase_times(_events)
    _polls = th.timed_polls(_events)
    _steps = pd.DataFrame(
        [
            {"step": "unblock every healed link", "ms after clock_start": _p["unblocked"] - _p["clock_start"]},
            {"step": "kick the sync", "ms after clock_start": _p["kicked"] - _p["clock_start"]},
            {"step": "first poll returns (the floor)", "ms after clock_start": _polls[0]},
            {"step": "agreement seen", "ms after clock_start": _p["converged"] - _p["clock_start"]},
        ]
    )
    _rounds = th.poll_rounds(_events)
    _verdict = (
        "This heal converged **at the first poll**. So its `convergence_ms` "
        "is the floor: the merge took at most this long, and the number "
        "measures the poll as much as the merge. If a merge holds the "
        "adapter's lock, the first poll waits for it, so a long merge can "
        "also end at the first poll."
        if len(_polls) == 1
        else f"This heal took **{len(_polls)}** polls. The rounds after the first "
        f"took a median of **{pd.Series(_rounds).median():.3f} ms**: that is the "
        "step of `convergence_ms` here."
    )
    mo.vstack(
        [
            mo.md(f"The heal trial of section 4, in the **{lane.value}** lane:"),
            mo.ui.table(_steps.round(3), selection=None, show_column_summaries=False),
            mo.md(_verdict + " Section 8.1 compares the floor of each lane."),
        ]
    )
    return


@app.cell
def _():
    mo.md(r"""
    ## 6. Do the libraries differ?

    The same scenario file runs against every library, so the workload stays
    fixed and only the library changes. Choose a scenario, then run it once
    per library. In the docker lane, each library change recreates the
    replica containers, which takes a few seconds.
    """)
    return


@app.cell
def _():
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
def _(go, s5_go, s5_preset, s5_trials):
    mo.stop(not s5_go.value)
    _rows = []
    with mo.status.progress_bar(total=len(th.CRDTS), title="Running") as _bar:
        for _c in th.CRDTS:
            _r = go(s5_preset.value, trials=s5_trials.value, library=_c)
            _rows.append(
                {
                    "library": _c,
                    "p50_ms": _r.summary["p50_ms"],
                    "p95_ms": _r.summary["p95_ms"],
                    "sync messages per trial": _r.sync_messages() / s5_trials.value,
                    "document bytes": _r.doc_size(),
                }
            )
            _bar.update()
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
def _():
    mo.md(r"""
    ### What does not compare directly

    - **Who keeps the sync state.** Automerge has a sync protocol with
      per-peer state. Yrs and Loro have none, so their adapters keep a
      per-peer record of what each peer has (section 2). That record is part
      of this harness, not of the library. A comparison of message counts or
      bytes must say so.
    - **Handshakes.** Automerge's protocol always starts with a handshake, so
      it sends messages even when there is little to say. That shows in the
      traffic of small runs.
    - **Document size** is each library's own encoding of the same content.
      It is a fair measure of storage cost, not of how much content there is.
    - **Fingerprints** differ in kind (section 5). That does not change when
      the clock stops, because each one is exact for its library. But a slow
      fingerprint makes a coarse clock.
    """)
    return


@app.cell
def _():
    mo.md(r"""
    ## 7. Traces and replay

    The orchestrator measures sync and merge in a graph of replicas. The
    **replay** ([`crates/replay`](../crates/replay/src/lib.rs),
    [`docs/replay.md`](../docs/replay.md)) asks a different question: what does
    one library do with one recorded history? It runs the crdt-benchmarks suite
    (B1 to B4) and real editing traces through each adapter, so that
    Replicant's numbers connect to published ones. The guide
    [`docs/crdt-benchmarks-guide.md`](../docs/crdt-benchmarks-guide.md)
    explains the calibration against crdt-benchmarks.

    ### From crdt-benchmarks to trace files

    crdt-benchmarks is JavaScript. It calls each library through a factory. To
    replay its inputs in Rust, Replicant records what every library received:
    """)
    return


@app.cell
def _():
    mo.mermaid(
        """
        flowchart LR
            cb["crdt-benchmarks clone<br/>at commit 796f702"] -->|git archive, npm ci --ignore-scripts| suite[".src/crdt-benchmarks<br/>lib0 0.2.88 checked"]
            et["editing-traces<br/>at commit 762fa6c"] --> dump
            suite --> dump["dump.mjs<br/>runs the whole suite against a<br/>factory that records each call"]
            dump --> traces["traces/crdt-benchmarks/*.trace.json<br/>traces/editing-traces/*.json.gz"]
            dump --> specs["workloads/crdt-benchmarks.toml<br/>workloads/editing-traces.toml<br/>(sha256 of each trace)"]
            traces --> replay["replay binary"]
            specs --> replay
        """
    )
    return


@app.cell
def _():
    mo.md(r"""
    [`gen-traces.sh`](../tools/crdt-benchmarks/gen-traces.sh) (`just gen-traces
    <clone>`) does this. It does not change the clone. It checks the pinned
    commits and the `lib0` version, because one PRNG serves the whole suite and
    `b2.js` draws from it at import time: a different version or run order gives
    different inputs. Each spec pins the SHA-256 of its trace, and a replay
    stops if a hash differs. The trace files stay out of the repository, in
    `traces/`.

    The table lists every workload, and whether its trace file is here.
    """)
    return


@app.cell
def _():
    mo.ui.table(th.workloads(), selection=None, show_column_summaries=False)
    return


@app.cell
def _():
    fork_trace = {
        "kind": "concurrent",
        "numAgents": 2,
        "oracle": {"kind": "tree_invariants"},
        "txns": [
            {"agent": 0, "parents": [], "patches": [[0, 0, "base"]]},
            {"agent": 0, "parents": [0], "patches": [[0, 0, "AA"]]},
            {"agent": 1, "parents": [0], "patches": [[0, 0, "BB"]]},
        ],
    }
    mo.vstack(
        [
            mo.md(r"""
            ### The trace format

            A trace is a causal graph of transactions. It is a superset of the
            [editing-traces](https://github.com/josephg/editing-traces) format.
            Each transaction names its **agent** (the replica that applies it),
            its **parents** (the transactions that the agent must have, and no
            others), and its ops. A text op is `[position, deleted chars,
            inserted text]`. The trace also says how to check the result:
            `endContent` for an exact text, or an `oracle`.

            This trace forks: agent 0 writes `base`, and then both agents
            insert at position 0, each without the other's insert.
            """),
            mo.hstack(
                [mo.json(fork_trace, label="fork trace"), mo.mermaid(th.trace_graph(fork_trace))],
                justify="start",
            ),
        ]
    )
    return (fork_trace,)


@app.cell
def _():
    mo.md(r"""
    ### How the driver replays a trace

    The driver ([`driver.rs`](../crates/replay/src/driver.rs)) **moves the
    updates between replicas itself**. Before an agent applies a transaction,
    the driver gives it the updates of that transaction's ancestors, and no
    others. So each replica has the exact state that the trace names. No sync
    protocol runs: a sync protocol sends all that a peer has, so it cannot give
    one exact version of a concurrent trace. To measure a sync protocol, use
    the orchestrator.

    One trial: create the replicas and set their ids, run the setup
    transactions (not timed), run the timed part, encode replica 0 and load
    the bytes (each with its own timer), and check the content (not timed). A
    spec's `timing = "merge_only"` times only the merge on the receivers.

    **The oracle.** Equal replicas prove only that they agree. So every trial
    checks the content, and a failed check gives no timing:

    | Check | Holds when | Asserts |
    |---|---|---|
    | `endContent` | the final text has one correct value | the text is exact |
    | `exact_list` | the final list has one correct value | the list is exact |
    | `map_winner` | concurrent writes to map keys | each key holds a value that some agent wrote to it |
    | `tree_invariants` | concurrent inserts at one position | the surviving elements match, and each branch keeps its local order |

    Concurrent inserts at one position have no single correct order: each
    library has its own rule, and the **library ids** break ties. The cell below
    replays the fork trace with an exact `endContent` for each of the two
    possible texts, with ascending and with descending ids, and reports which
    text each library made.
    """)
    return


@app.cell
def _(fork_trace):
    _rows = []
    for _order in ("ascending", "descending"):
        _made = {}
        for _text in ("AABBbase", "BBAAbase"):
            _exact = {k: v for k, v in fork_trace.items() if k != "oracle"} | {"endContent": _text}
            _r = th.replay_trace(_exact, "--id-order", _order)
            for _c in _r.trials.get("crdt", []):
                _made[_c] = _text
        _rows += [{"id order": _order, "library": _c, "final text": _made.get(_c, "?")} for _c in th.CRDTS]
    _df = pd.DataFrame(_rows).pivot(index="library", columns="id order", values="final text")
    _tree = th.replay_trace(fork_trace)
    mo.vstack(
        [
            mo.ui.table(_df.reset_index(), selection=None, show_column_summaries=False),
            mo.md(
                f"""
                The libraries disagree, and each one changes its answer with
                the id order. With `tree_invariants`, all three pass
                ({len(_tree.trials)} of 3 trials passed, {len(_tree.failed)}
                failed): the check asserts only what every library must keep.

                This is **the rule about positions** in
                [`docs/replay.md`](../docs/replay.md): after a merge of such
                inserts, one position names different elements in different
                libraries. So a trace with `tree_invariants` must not merge
                and then edit again, or the libraries would run different
                workloads with no error.
                """
            ),
        ]
    )
    return


@app.cell
def _():
    mo.md(r"""
    ### A live replay

    The smoke spec (`workloads/smoke.toml`) needs no generated traces. CI runs
    it on a containerized replica. Each row is one trial: `time_ms` is the timed
    part, `doc_size` the encoded document, and `update_bytes` the updates that
    the driver moved.
    """)
    return


@app.cell
def _():
    _smoke = th.replay(
        "--spec", "workloads/smoke.toml", data_dir=th.REPO / "workloads" / "fixtures", trials=3
    )
    mo.vstack(
        [
            mo.ui.table(
                _smoke.trials[["workload", "crdt", "lane", "trial", "time_ms", "doc_size", "update_bytes"]],
                selection=None,
                show_column_summaries=False,
            ),
            mo.md(
                f"""
                The `run` row records the commit (`{_smoke.run["git_commit"][:8]}`,
                dirty: {_smoke.run["git_dirty"]}), the build profile
                (`{_smoke.run["build_profile"]}`), and the library versions
                ({", ".join(f"{k} {v}" for k, v in _smoke.run["versions"].items())}).
                Discard the first trial of each workload as a warm-up.
                """
            ),
        ]
    )
    return


@app.cell
def _(traces_ok):
    s7_workload = mo.ui.dropdown(
        options={
            "B1.4: 6,000 random single-character inserts": "b1.4",
            "B2.1c: two users, concurrent inserts": "b2.1c",
            "B4: the LaTeX paper, 259,778 transactions": "b4",
        },
        value="B1.4: 6,000 random single-character inserts",
        label="workload",
    )
    s7_local = mo.ui.run_button(label="Replay in process", disabled=not traces_ok)
    s7_remote = mo.ui.run_button(label="Replay on the stack", disabled=not traces_ok)
    mo.vstack(
        [
            mo.hstack([s7_workload, s7_local, s7_remote], justify="start"),
            mo.md(
                "**In process** runs all three libraries, 3 trials each. B4 takes "
                "about 10 s. **On the stack** runs the stack's library on the "
                "replicas of the docker or k8s lane, 2 trials. Each op is one RPC, "
                "so it is much slower: start with B1.4."
                if traces_ok
                else "These need the trace files. Run `just gen-traces <clone>`."
            ),
        ]
    )
    return s7_local, s7_remote, s7_workload


@app.cell
def _(s7_local, s7_workload):
    mo.stop(not s7_local.value)
    with mo.status.spinner(title=f"Replaying {s7_workload.value} in process…"):
        _r = th.replay("--only", s7_workload.value, trials=3)
    _t = _r.trials[_r.trials["trial"] > 1]
    mo.vstack(
        [
            alt.Chart(_t)
            .mark_circle(size=70)
            .encode(
                x=alt.X("time_ms:Q", scale=alt.Scale(type="log"), title="time_ms (log)"),
                y=alt.Y("crdt:N", title=None),
                color=alt.Color("crdt:N", legend=None),
                tooltip=["crdt", "trial", "time_ms", "doc_size"],
            )
            .properties(width=460, height=120, title=f"{s7_workload.value}, in process, warm-up dropped"),
            mo.ui.table(
                _r.trials[["crdt", "trial", "time_ms", "encode_ms", "doc_size", "update_bytes"]],
                selection=None,
                show_column_summaries=False,
            ),
        ]
    )
    return


@app.cell
def _(s7_remote, s7_workload, stack):
    mo.stop(not s7_remote.value)
    mo.stop(stack is None, mo.callout(mo.md("This needs the docker or the k8s lane."), kind="warn"))
    with mo.status.spinner(title=f"Replaying {s7_workload.value} on the {stack.LANE} stack…"):
        _r = th.replay(
            "--only", s7_workload.value,
            crdts=[stack.crdt],
            trials=2,
            replicas=",".join(f"localhost:{stack.PORT_BASE + _i}" for _i in range(stack.size)),
        )
    mo.stop(
        _r.trials.empty,
        mo.ui.table(pd.concat([_r.skipped, _r.failed]), selection=None, show_column_summaries=False),
    )
    _t = _r.trials.assign(transport_ms=_r.trials["time_ms"] - _r.trials["adapter_ms"])
    mo.vstack(
        [
            mo.ui.table(
                _t[["crdt", "lane", "trial", "time_ms", "adapter_ms", "transport_ms"]],
                selection=None,
                show_column_summaries=False,
            ),
            mo.md(
                """
                `adapter_ms` is the time inside the adapter, which each reply
                reports. The rest is gRPC and transport. Compare `adapter_ms`
                with the in-process `time_ms` of the same library: they should
                be close. Section 8.2 explains a case where they were not.
                """
            ),
        ]
    )
    return


@app.cell
def _():
    mo.md(r"""
    ## 8. Measurement faults

    A benchmark measures the harness too. Each fault below made a number
    describe Replicant instead of the library. Each was found because a number
    did not agree with an independent measurement. The table of all faults, with
    numbers before and after each fix, is in [`divergence.py`](divergence.py)
    section 3. Here, each fault is a worked example that you can run.

    ### 8.1 The poll sets the resolution

    Before 2026-11-07, the Yrs fingerprint was the full document encoding: 377
    KB and about 0.4 ms on the B4 paper. One docker poll round then took about 3
    ms, and a heal of one op per side read as one round (about 3.7 ms) or two
    (about 8.6 ms), never a value between. The fix made the fingerprint the
    state vector and the delete set (about 0.02 ms). This notebook cannot run
    the old fingerprint without a code change, so the run below shows the same
    effect from the other side: **the lane changes the floor**. It runs a heal
    of one map write per side, 5 trials, in each lane. For each trial it reads
    the floor (the time from `clock_start` to the end of the first poll) and
    the number of polls.
    """)
    return


@app.cell
def _(docker_up, kind_up):
    s81_lanes = ["in-process"] + (["docker"] if docker_up else []) + (["k8s"] if kind_up else [])
    s81_go = mo.ui.run_button(label="Compare the poll rounds of the lanes")
    mo.hstack([s81_go, mo.md(f"Lanes: {', '.join(s81_lanes)}.")], justify="start")
    return s81_go, s81_lanes


@app.cell
def _(binary, crdt, s81_go, s81_lanes):
    mo.stop(not s81_go.value)
    _rows = []
    with mo.status.progress_bar(total=len(s81_lanes), title="Running") as _bar:
        for _lane in s81_lanes:
            _r = th.run(th.partition("one-op-heal", [[0], [1]], 1), crdt.value, binary, trials=5, lane=_lane)
            for _t in _r.trials.itertuples():
                _polls = th.timed_polls(_r.trial_events(_t.trial))
                _rows.append(
                    {
                        "lane": _lane,
                        "trial": _t.trial,
                        "convergence_ms": _t.convergence_ms,
                        "floor_ms": _polls[0],
                        "polls": len(_polls),
                    }
                )
            _bar.update()
    _df = pd.DataFrame(_rows)
    _y = alt.Y("lane:N", sort=s81_lanes, title=None)
    mo.vstack(
        [
            (
                alt.Chart(_df).mark_tick(thickness=2, size=22, color="black").encode(
                    x=alt.X("floor_ms:Q", scale=alt.Scale(type="log"), title="ms (log)"), y=_y, tooltip=list(_df.columns)
                )
                + alt.Chart(_df).mark_circle(size=70, opacity=0.7).encode(
                    x="convergence_ms:Q", y=_y, tooltip=list(_df.columns)
                )
            ).properties(
                width=460,
                height=45 * len(s81_lanes),
                title=f"One op per side, {crdt.value}: dot = heal, tick = floor",
            ),
            mo.ui.table(
                _df.groupby("lane", sort=False)
                .agg(
                    heal_p50=("convergence_ms", "median"),
                    floor_p50=("floor_ms", "median"),
                    first_poll_share=("polls", lambda p: (p == 1).mean()),
                )
                .reset_index()
                .round(3),
                selection=None,
                show_column_summaries=False,
            ),
            mo.md(
                """
                The merge of one op is the same in each lane, but the heal
                reads differently. When a dot sits on its tick, the trial
                converged at the first poll: it measured the floor of the
                lane, not the merge. `first_poll_share` is the share of such
                trials. A result near the floor says "faster than the poll",
                and nothing more precise.
                """
            ),
        ]
    )
    return


@app.cell
def _(traces_ok):
    s82_go = mo.ui.run_button(label="Run with and without the library spans", disabled=not traces_ok)
    mo.vstack(
        [
            mo.md(r"""
            ### 8.2 The log filter

            Automerge and Loro put `tracing` spans on their hot paths, at the
            INFO level. When a span is enabled, the log subscriber formats each
            argument. Automerge's `load_changes` span records the whole change
            graph: on the B4 paper, that is 259,779 hashes, formatted again for
            each received change. That made one received change cost about 125
            ms instead of about 21 ms, and it set the Automerge heal floor of
            the docker and in-process lanes. The replay installs no
            subscriber, so it did not pay this cost. That is how the fault was
            found: the replay and the orchestrator disagreed.

            The fix ([`logging.rs`](../crates/replica/src/logging.rs)) keeps the
            CRDT crates at WARN, unless `RUST_LOG` names them. So
            `RUST_LOG=info,automerge=info` brings the cost back. The run below
            uses the in-process lane, where the orchestrator's own filter is
            the replicas' filter: Automerge, the B4 paper, one insert per
            side, 3 trials, with each filter.
            """),
            s82_go if traces_ok else mo.md("This needs the trace files (the B4 paper)."),
        ]
    )
    return (s82_go,)


@app.cell
def _(binary, s82_go):
    mo.stop(not s82_go.value)
    _cell = th.partition(
        "paper-one-op", [[0], [1]], 1, workload="text_splice", locality="random_position", baseline=th.PAPER
    )
    _rows = []
    for _label, _env in (("libraries at WARN (default)", None), ("RUST_LOG=info,automerge=info", {"RUST_LOG": "info,automerge=info"})):
        with mo.status.spinner(title=f"Running with {_label}…"):
            _r = th.run(_cell, "automerge", binary, trials=3, lane="in-process", env=_env)
        _rows += [{"log filter": _label, "trial": _t.trial, "convergence_ms": _t.convergence_ms} for _t in _r.trials.itertuples()]
    _df = pd.DataFrame(_rows)
    mo.vstack(
        [
            alt.Chart(_df)
            .mark_circle(size=80)
            .encode(
                x=alt.X("convergence_ms:Q", scale=alt.Scale(zero=True), title="heal (ms)"),
                y=alt.Y("log filter:N", title=None),
                tooltip=list(_df.columns),
            )
            .properties(width=460, height=100, title="Automerge heal on the B4 paper, in process"),
            mo.ui.table(_df, selection=None, show_column_summaries=False),
            mo.md(
                """
                When this was written, the default filter gave about 45 ms and
                the enabled span about 245 ms. The lesson: a number that
                disagrees with an independent measurement is a bug report on
                the harness until it is explained.
                """
            ),
        ]
    )
    return


@app.cell
def _(traces_ok):
    mo.md(r"""
    ### 8.3 Random library ids

    A library breaks ties between concurrent ops with the replica ids (section
    7 showed the effect on a text). The ids can also change the **cost** of a
    merge: for some pairs of Loro peer ids, an import takes about 4,300 ms
    instead of about 10 ms. Before 2026-11-07, every trial drew random ids, so
    a cell's spread was partly luck in the ids, and a rerun did not repeat a
    trial. Now repetition *r* of every cell, on every library, has the same
    ids.

    Two runs below. The first replays `friendsforever`, two users typing with 1
    s of latency, on Yrs with each id order. Its upstream `endContent` is
    correct only with ascending ids.
    """ + ("" if traces_ok else "\n\n**The trace files are missing**, so the first run is skipped."))
    return


@app.cell
def _(traces_ok):
    mo.stop(not traces_ok)
    _rows = []
    for _order in ("ascending", "descending"):
        _r = th.replay(
            "--spec", "workloads/editing-traces.toml", "--only", "et.friendsforever", "--id-order", _order,
            crdts=["yrs"], trials=1,
        )
        _rows.append(
            {
                "id order": _order,
                "result": "passed" if not _r.trials.empty else "FAILED",
                "time_ms": _r.trials["time_ms"].iloc[0] if not _r.trials.empty else None,
                "error": _r.failed["error"].iloc[0] if not _r.failed.empty else "",
            }
        )
    mo.ui.table(pd.DataFrame(_rows), selection=None, show_column_summaries=False)
    return


@app.cell
def _():
    s83_go = mo.ui.run_button(label="Run a Loro cell twice")
    mo.vstack(
        [
            mo.md(r"""
            The second run is a Loro cell with no starting document: two
            replicas, 1,000 inserts per side, all at position 0, 6 trials. It
            runs the cell twice in the lane chosen at the top. Each trial has
            its own ids. Look for two groups of trials, fast and slow, and
            check that the second run repeats each trial's group.
            """),
            s83_go,
        ]
    )
    return (s83_go,)


@app.cell
def _(go, s83_go):
    mo.stop(not s83_go.value)
    _cell = th.partition("loro-same-1e3", [[0], [1]], 1000, workload="text_splice", locality="same_region")
    _rows = []
    for _run in ("first run", "second run"):
        with mo.status.spinner(title=f"Loro, {_run}…"):
            _r = go(_cell, trials=6, library="loro")
        _rows += [{"run": _run, "trial": _t.trial, "convergence_ms": _t.convergence_ms} for _t in _r.trials.itertuples()]
    _df = pd.DataFrame(_rows)
    mo.vstack(
        [
            alt.Chart(_df)
            .mark_circle(size=90, opacity=0.8)
            .encode(
                x=alt.X("trial:O", title="trial (= repetition, which seeds the ids)"),
                y=alt.Y("convergence_ms:Q", scale=alt.Scale(type="log"), title="heal (ms, log)"),
                color="run:N",
                xOffset="run:N",
                tooltip=list(_df.columns),
            )
            .properties(width=460, height=240, title="Loro, same_region, 1,000 per side, empty start"),
            mo.md(
                """
                In the archived grid (docker lane), this cell had trials near
                3 ms and trials near 100 ms. The rule that makes an id pair
                slow is not known: see section 10. So report the spread of such
                a cell, or its fast and slow groups, not only its median.
                """
            ),
        ]
    )
    return


@app.cell
def _():
    _check = th.check_archive()
    _bad = _check[_check["verdict"] != "ok"]
    mo.vstack(
        [
            mo.md(r"""
            ### 8.4 Stale provenance

            The bench scripts write a CSV and a provenance file
            (`.meta.json`) for each run. Before 2026-11-07, a run that failed
            before its first trial left an empty CSV, and the provenance file
            of an **earlier** run with the same name stayed beside it. The
            result: a file named for one library, with the provenance of
            another. The scripts now remove the old provenance file when a run
            starts, and `divergence_helpers.load_grid` checks `run.crdt`
            against the file name.

            The cell below checks every archive under `data/` in the same way:
            """),
            mo.ui.table(_bad if not _bad.empty else _check, selection=None, show_column_summaries=False),
            mo.md(
                f"""
                {len(_check)} CSV files have a provenance file. **{len(_bad)}**
                have the wrong label. Each one is an empty CSV: a failed run,
                with the provenance of another library's run. These files are
                in the pushed pilot archive, and they are not fixed yet.

                The lesson: read the CSV and its provenance together, and check
                that they describe the same run.
                """
            ),
        ]
    )
    return


@app.cell
def _():
    mo.md(r"""
    ### 8.5 When to distrust a number

    One trial is one sample. The run below repeats one scenario many times so
    you can see the spread.
    """)
    return


@app.cell
def _():
    s6_trials = mo.ui.slider(10, 100, value=40, step=10, label="trials", show_value=True)
    s6_go = mo.ui.run_button(label="Run")
    mo.hstack([s6_trials, s6_go], justify="start")
    return s6_go, s6_trials


@app.cell
def _(crdt, go, lane, s6_go, s6_trials):
    mo.stop(not s6_go.value)
    spread = go(th.topology("spread", 6, "ring", 30), trials=s6_trials.value)
    _s = spread.summary
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
                Ring of 6, 30 writes, **{crdt.value}**, {lane.value} lane:
                p50 **{_s["p50_ms"]:.2f} ms**, p95 **{_s["p95_ms"]:.2f} ms**,
                mean {_s["mean_ms"]:.2f} ms. Deferred flushes:
                **{spread.deferred()}**.
                """
            ),
        ]
    )
    return


@app.cell
def _():
    mo.md(r"""
    ### A checklist for reading a result

    - **Look at p95, not only p50.** A large gap means a few slow trials. The
      mean hides which.
    - **Look at the trials in order.** Early trials can be slower while caches
      and the allocator warm up. A trend over trial number is drift on the
      host, not a property of the library.
    - **Look for groups.** A cell with fast and slow trials can be an id effect
      (8.3). Report the groups.
    - **Know the floor.** A trial that converged at the first poll measured
      the poll, not the merge (8.1).
    - **Check the deferred counter.** Each sync link has a buffer of 64
      messages. Any deferral means that the timings include those stalls.
    - **Keep the libraries' logs at WARN.** An enabled span can cost more than
      the work (8.2).
    - **Know the lane.** Section 9 shows that the lane can change which shape
      or library looks fastest. Say which lane a number comes from.
    - **Remember the host.** Every container of the docker lane runs on one
      machine. It has real process and TCP boundaries, but no real network
      latency or loss.
    - **Keep the provenance, and check it.** Run sweeps with `--provenance`,
      and check that each CSV and its provenance describe the same run (8.4).
    - **Do not measure with `--trace`.** Tracing reads the clock, compares
      fingerprints, and makes extra RPCs inside or near the timed window.
    """)
    return


@app.cell
def _():
    mo.md(r"""
    ## 9. Does the lane change the answer?

    The same scenarios, run in each lane. In-process, every replica is a task
    in one process: replicas compete for the same CPU, and a sync message
    never leaves the process's network stack for long. In docker, each
    replica is its own process with its own runtime, and every message crosses
    a container network. In k8s, the replicas are pods, and sync crosses the
    cluster network. The orchestrator reaches the pods through
    `kubectl port-forward`, so its polls are slower than in docker.
    """)
    return


@app.cell
def _(docker_up, kind_up):
    s7_lanes = ["in-process", "docker"] + (["k8s"] if kind_up else [])
    s7_go = mo.ui.run_button(label="Compare the lanes", disabled=not docker_up)
    mo.hstack(
        [
            s7_go,
            mo.md(
                "Runs the four shapes with 5 replicas, at 10 and 50 writes, 10 "
                f"trials each, in these lanes: {', '.join(s7_lanes)}."
                if docker_up
                else "This section needs Docker."
            ),
        ],
        justify="start",
    )
    return s7_go, s7_lanes


@app.cell
def _(binary, crdt, s7_go, s7_lanes):
    mo.stop(not s7_go.value)
    _rows = []
    with mo.status.progress_bar(total=8 * len(s7_lanes), title="Running") as _bar:
        for _lane in s7_lanes:
            for _ops in [10, 50]:
                for _shape in ["full_mesh", "star", "ring", "line"]:
                    _r = th.run(
                        th.topology(_shape, 5, _shape, _ops),
                        crdt.value,
                        binary,
                        trials=10,
                        lane=_lane,
                    )
                    _rows.append(
                        {
                            "lane": _lane,
                            "load": f"{_ops} writes",
                            "shape": _shape,
                            "p50_ms": _r.summary["p50_ms"],
                        }
                    )
                    _bar.update()
    _df = pd.DataFrame(_rows)
    mo.vstack(
        [
            alt.Chart(_df)
            .mark_bar()
            .encode(
                x=alt.X("p50_ms:Q", title="p50 convergence (ms)"),
                y=alt.Y("shape:N", sort=["full_mesh", "star", "ring", "line"], title=None),
                color=alt.Color("lane:N"),
                yOffset="lane:N",
                column=alt.Column("load:N", title=None),
            )
            .properties(width=260, height=200),
            mo.md(
                f"""
                With **{crdt.value}**. When this tour was written, with
                Automerge, the two lanes ranked the shapes differently. At 50
                writes the in-process full mesh was by far the slowest shape,
                because its traffic competed for one CPU pool. In docker the
                four shapes were close. At 10 writes, docker made the line the
                slowest shape, because each hop crossed a network stack.

                The k8s lane was not in that run, so the text does not say
                how it ranks the shapes. Its p50 includes the slower polls
                through the port-forward.

                The lesson: a ranking measured in one lane is a claim about
                that lane. Before you report one, check it in another.
                """
            ),
        ]
    )
    return


@app.cell
def _():
    mo.md(r"""
    ## 10. What is open

    These are known gaps. Each one is a limit on what a result can claim today.

    - **The heal timeout is not enforced during a blocked poll.** The poll
      checks its timeout only between rounds. A fingerprint call waits for the
      adapter's lock, so a merge that holds the lock for minutes holds the
      round for minutes too. In the archived grid, two Loro `same_region`
      trials at 10⁵ ops per side took about 1,180 s and 1,195 s. The heal
      timeout is 600 s, and neither trial failed.
    - **The Loro peer-id rule.** Which id pairs are slow, and why, is not known
      (8.3). A probe is in [`tools/findings`](../tools/findings/README.md).
    - **A Yrs id effect** at `random_position` 10⁵: one repetition took about
      2.5 times as long as the others. It is not explained.
    - **A conflict knob.** In the divergence grid, more conflict always comes
      with more ops (section 4). Cells with a fixed op count and a varied
      contested share would measure conflict alone.
    - **More replicas and a real network.** The divergence study uses two
      replicas on one host.

    Read next:

    - [`docs/scenarios.md`](../docs/scenarios.md): every scenario field.
    - [`docs/divergence-sweep.md`](../docs/divergence-sweep.md): the
      divergence study, its terms, and its dated records.
    - [`docs/metrics.md`](../docs/metrics.md): the OTel metrics, and why
      convergence is not one of them.
    - [`docs/replay.md`](../docs/replay.md) and
      [`docs/crdt-benchmarks-guide.md`](../docs/crdt-benchmarks-guide.md): the
      replay, and its calibration against crdt-benchmarks.
    - [`tools/findings/README.md`](../tools/findings/README.md): the library
      findings, each as a repeatable probe.
    """)
    return


if __name__ == "__main__":
    app.run()
