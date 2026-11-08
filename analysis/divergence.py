import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="Divergence study")


with app.setup:
    import marimo as mo
    import altair as alt
    import pandas as pd

    import divergence_helpers as dh


@app.cell
def _():
    mo.md(r"""
    # What does a merge cost after replicas diverge?

    This notebook explains the divergence study of Replicant: what it measures,
    how it measures it, which faults in the measurement we found and fixed, and
    what the results show so far. Each chart reads archived data under `data/`,
    so the notebook needs no Docker and no build. You can change the controls and
    see the data from another side.

    It assumes that you know what a CRDT is. Links go to the code and the docs
    that hold the details.
    """)
    return


@app.cell
def _():
    mo.md(r"""
    ## 1. The question

    Two replicas start from the same document. A network partition cuts them
    off from each other, and each side keeps editing. Later the partition heals,
    and the replicas merge. The study asks: **how long does the merge take, and
    how does that time change as the two sides' edits conflict more?**

    An insert in a sequence CRDT attaches to an existing element, its *anchor*.
    Two inserts that attach to the same anchor from different sides are
    *concurrent siblings*. The library must order them, and that is the merge
    work that conflict adds. An anchor with inserts from both sides is a
    *contested anchor*.

    A **cell** is one experiment. Two settings define it:

    - **ops per side**: 10², 10³, 10⁴, or 10⁵ inserts on each side.
    - **locality**: where each insert goes.
      - `append`: at the end of the text. Each side extends its own tail.
      - `random_position`: at a random position.
      - `same_region`: at position 0. Every insert contests one anchor.

    Each main cell starts from the LaTeX paper of crdt-benchmarks B4 (104,852
    characters, 259,778 transactions of history). Six control cells start from
    an empty document, so you can see what the starting document costs. See
    [`docs/divergence-sweep.md`](../docs/divergence-sweep.md).
    """)
    return


@app.cell
def _():
    grid_folder = dh.latest("divergence-grid-")
    seeded = dh.load_grid(grid_folder / "seeded")
    contention = dh.load_contention(grid_folder / "seeded")
    mo.md(f"Data: `{grid_folder.relative_to(dh.REPO)}`, {len(seeded)} trials, commit `{seeded['commit'].iloc[0]}`.")
    return contention, grid_folder, seeded


@app.cell
def _(contention):
    _chart = (
        alt.Chart(contention)
        .mark_line(point=True)
        .encode(
            x=alt.X("ops:Q", scale=alt.Scale(type="log"), title="ops per side"),
            y=alt.Y("contested_share:Q", axis=alt.Axis(format="%"), title="contested ops"),
            color=alt.Color("locality:N", sort=list(dh.LOCALITIES)),
            strokeDash=alt.StrokeDash("start:N"),
            tooltip=["scenario", alt.Tooltip("contested_share:Q", format=".2%"), "max_siblings"],
        )
        .properties(width=520, height=260, title="How much of the workload is contested")
    )
    mo.vstack([
        mo.md(r"""
        The contention model counts the contested ops of each cell from the same
        seeded positions that the run uses. No library runs in this count, so it
        describes the workload only.

        Look at `random_position`: its contested share rises from 0.3% to 34% only
        because the op count rises. **In this grid, more conflict always comes
        with more ops.** To measure conflict alone, a later grid needs cells with
        a fixed op count and a varied contested share.
        """),
        _chart,
    ])
    return


@app.cell
def _():
    mo.md(r"""
    ## 2. How a cell runs

    [`run_partition_heal`](../crates/orchestrator/src/partition_heal.rs) runs one
    trial of a cell:

    1. **Reset** every replica, and **seed its library id**. The id seed depends
       on the repetition only, so repetition *r* has the same ids in every cell
       and on every library. See section 5.
    2. **Load** the B4 paper on every replica, if the cell has a starting
       document.
    3. **Wire** every link, let sync run until it is idle, and then **block** the
       links between the two sides. A blocked link stays open but carries no data.
    4. **Diverge**: each side applies its ops. The positions come from a seeded
       generator, so every library gets the same workload.
    5. **Heal**: unblock the links and start the sync. The clock starts at the
       first unblock and stops when every replica reports the same fingerprint.
       An unblock is a *reconnect*: each replica keeps only the sync state that
       its library keeps across a disconnect
       ([`CrdtAdapter::sync_reconnect`](../crates/common/src/adapter.rs)).
    6. **Gate**: the merged text must hold the starting document and both sides'
       inserts.

    The replicas run as containers on the **docker lane**. Each one is the
    [`replica`](../crates/replica/src/main.rs) binary with one library behind the
    [`CrdtAdapter`](../crates/common/src/adapter.rs) trait.
    """)
    return


@app.cell
def _():
    _fixes = dh.FIXES.copy()
    _chart = (
        alt.Chart(_fixes.dropna(subset=["before_ms", "after_ms"]).melt(
            id_vars=["fault"], value_vars=["before_ms", "after_ms"], var_name="when", value_name="ms"))
        .mark_bar()
        .encode(
            y=alt.Y("fault:N", title=None, sort=None),
            # A colored bar stacks by default, and a stack starts at 0, which a
            # log scale cannot show. Then no bar draws.
            x=alt.X("ms:Q", scale=alt.Scale(type="log"), stack=None, title="heal time (ms), log scale"),
            color=alt.Color("when:N", scale=alt.Scale(domain=["before_ms", "after_ms"], range=["#bab0ac", "#4c78a8"])),
            yOffset="when:N",
            tooltip=["fault", "when", "ms"],
        )
        .properties(width=480, height=260)
    )
    mo.vstack([
        mo.md(r"""
        ## 3. Faults in the measurement, and their fixes

        A benchmark measures the harness too. Each row below is a fault that made
        a result describe the harness instead of the library. Each was found
        because a number did not agree with an independent measurement. A probe
        outside Replicant gave one value, and Replicant gave another.

        Two of them teach the general lesson:

        - **A slow fingerprint sets the resolution.** The orchestrator polls each
          replica's fingerprint until all agree. With a full encode of 377 KB per
          poll, a Yrs heal could take one poll round (3.7 ms) or two (8.6 ms), but
          no value between. See [`docs/metrics.md`](../docs/metrics.md).
        - **Logging can cost more than the work.** Automerge puts a `tracing` span
          on `load_changes`, and the span records the whole change graph. With a
          log subscriber at INFO, each received change formatted 259,779 hashes:
          about 125 ms instead of 21 ms. See
          [`crates/replica/src/logging.rs`](../crates/replica/src/logging.rs).
        """),
        mo.ui.table(_fixes, selection=None),
        _chart,
    ])
    return


@app.cell
def _():
    findings_folder = dh.latest("findings-")
    sync_state = dh.load_findings(findings_folder, "automerge-sync-state")
    apply_cost = dh.load_findings(findings_folder, "automerge-apply-cost")
    yrs_insert = dh.load_findings(findings_folder, "yrs-random-insert")
    loro_peers = dh.load_findings(findings_folder, "loro-peer-probe")
    return apply_cost, findings_folder, loro_peers, sync_state, yrs_insert


@app.cell
def _(apply_cost, findings_folder, loro_peers, sync_state, yrs_insert):
    _sync = (
        alt.Chart(sync_state).mark_boxplot()
        .encode(x=alt.X("mode:N", sort=["Persisted", "Kept", "Fresh"]), y=alt.Y("heal_ms:Q", title="heal (ms)"))
        .properties(width=220, height=220, title="Automerge: sync state at a reconnect")
    )
    _apply = (
        alt.Chart(apply_cost).mark_bar()
        .encode(x=alt.X("position:O", title="position in the text"), y=alt.Y("mean_apply_ms:Q", scale=alt.Scale(type="log"), stack=None, title="apply one change (ms)"),
                color="document:N", xOffset="document:N")
        .properties(width=260, height=220, title="Automerge: apply cost and history")
    )
    _yrs = (
        alt.Chart(yrs_insert).mark_line(point=True)
        .encode(x=alt.X("inserts_before_block:Q", title="random inserts made before"), y=alt.Y("mean_insert_ms:Q", title="one insert (ms)"), color="document:N")
        .properties(width=260, height=220, title="Yrs: random inserts get slower")
    )
    _loro = (
        alt.Chart(loro_peers).mark_circle(size=60)
        .encode(x=alt.X("a_imports_b_ms:Q", scale=alt.Scale(type="log"), title="a imports b (ms)"),
                y=alt.Y("b_imports_a_ms:Q", scale=alt.Scale(type="log"), title="b imports a (ms)"),
                tooltip=["label", "peer_a", "peer_b"])
        .properties(width=260, height=220, title="Loro: import time by peer-id pair")
    )
    mo.vstack([
        mo.md(rf"""
        ## 4. Library findings

        These are effects of the libraries, not of Replicant. Each has a probe in
        [`tools/findings/`](../tools/findings/README.md) that calls the library
        directly and pins its version. `just findings` runs them all. Data:
        `{findings_folder.relative_to(dh.REPO)}`.

        - **Automerge, sync state.** A heal after a reconnect costs about 54 ms
          with the persisted state, 190 ms if the old request is kept, and 630 ms
          as a first contact.
        - **Automerge, apply cost.** One small change costs about 16 ms to apply
          on the paper and 0.02 ms on a short document, wherever it goes in the
          text. The cost grows with the history.
        - **Yrs, random inserts.** Each random insert splits a block, and later
          inserts get slower, with or without the paper's history.
        - **Loro, peer ids.** For some pairs of peer ids, an import takes about
          4,300 ms instead of about 10 ms. The rule is not known.
        """),
        mo.hstack([_sync, _apply]),
        mo.hstack([_yrs, _loro]),
    ])
    return


@app.cell
def _(seeded):
    libraries = mo.ui.multiselect(options=list(dh.LIBRARIES), value=list(dh.LIBRARIES), label="libraries")
    locality = mo.ui.dropdown(options=list(dh.LOCALITIES), value="random_position", label="locality")
    start = mo.ui.radio(options=["paper", "empty"], value="paper", label="start", inline=True)
    mo.vstack([mo.md("## 5. The grid"), mo.hstack([libraries, locality, start])])
    return libraries, locality, start


@app.cell
def _(libraries, locality, seeded, start):
    _trials = seeded[
        seeded["library"].isin(libraries.value)
        & (seeded["locality"] == locality.value)
        & (seeded["start"] == start.value)
    ]
    _med = dh.medians(_trials)
    mo.vstack([
        mo.md(rf"""
        Each small point is one trial, and each large marker is a median. The
        line joins the medians of cells with one group. A Loro cell whose trials
        fall into two groups, at least ten times apart, shows a fast and a slow
        median instead, because one median of both would describe no trial.
        Both axes are logarithmic, so a straight line is a power law: a slope of 1 means the
        time grows in proportion to the ops, and a slope of 2 means it grows with
        their square.

        Things to look for:

        - `same_region` on Yrs: about 45 ms at 10³, 3.8 s at 10⁴, and 6 minutes at
          10⁵. The slope is about 2. Every insert contests one anchor, and Yrs
          orders the siblings in quadratic time.
        - Loro in contested cells: some trials are hundreds of times slower than
          others in the same cell. That is the peer-id effect of section 4, and
          each such trial is repeatable, because its ids are seeded.
        - `start = empty`: the control cells. Compare them with the paper to see
          what the starting document costs.
        """),
        dh.heal_chart(_med, _trials),
        mo.ui.table(_med.round(2), selection=None),
    ])
    return


@app.cell
def _(contention, seeded):
    _med = dh.medians(seeded).merge(contention, on=["locality", "start", "ops"])
    _chart = (
        alt.Chart(_med)
        .mark_point(filled=True, size=70)
        .encode(
            x=alt.X("contested_share:Q", scale=alt.Scale(type="symlog", constant=0.001), axis=alt.Axis(format="%"), title="contested ops"),
            y=alt.Y("median:Q", scale=alt.Scale(type="log"), title="median heal (ms)"),
            color=alt.Color("library:N", scale=dh.LIBRARY_COLORS),
            shape=alt.Shape("locality:N", sort=list(dh.LOCALITIES)),
            opacity=alt.condition(alt.datum.group == "slow", alt.value(0.5), alt.value(1.0)),
            tooltip=["library", "locality", "start", "ops", "group", alt.Tooltip("contested_share:Q", format=".2%"), "median", "trials"],
        )
        .properties(width=520, height=320)
    )
    mo.vstack([
        mo.md(r"""
        ### Heal time against the contested share

        Each point is one cell on one library. A cell with a fast and a slow
        group of trials has a point for each; the slow one is paler. The x axis
        is the share of ops that the contention model counts as contested. Remember the limit of
        section 1: in this grid, a cell with more contested ops also has more ops.
        """),
        _chart,
    ])
    return


@app.cell
def _(grid_folder, seeded):
    random_ids = dh.load_grid(grid_folder / "random-ids")
    _both = pd.concat([
        seeded.assign(ids="seeded"),
        random_ids.assign(ids="random"),
    ])
    _loro = _both[(_both["library"] == "loro") & (_both["locality"] == "same_region") & (_both["ops"] >= 1000) & (_both["ops"] <= 10_000)]
    _chart = (
        alt.Chart(_loro)
        .mark_tick(thickness=2, size=18)
        .encode(
            x=alt.X("convergence_ms:Q", scale=alt.Scale(type="log"), title="heal time (ms)"),
            y=alt.Y("scenario:N", title=None),
            color="ids:N",
            yOffset="ids:N",
            tooltip=["scenario", "ids", "trial", "convergence_ms"],
        )
        .properties(width=520, height=220)
    )
    mo.vstack([
        mo.md(r"""
        ## 6. Why the library ids are seeded

        A library breaks ties between concurrent inserts with the ids of the
        replicas, and for Loro the id pair can change the cost of a merge by
        hundreds of times. The first grid drew random ids in every trial. Each
        tick below is one trial of a Loro `same_region` cell. The color shows the
        grid: `random` for the first grid, and `seeded` for the seeded grid.

        With seeded ids, the spread of a cell is still there, because the
        repetitions use different ids. But each trial is now repeatable, and
        repetition *r* has the same ids in every cell and on every library. So a
        difference between two cells is not luck in the ids. Report the spread
        of such a cell, not only its median.
        """),
        _chart,
    ])
    return


@app.cell
def _():
    mo.md(r"""
    ## 7. What is open

    - **A conflict knob.** Cells with a fixed op count and a varied contested
      share, so that conflict is measured apart from size. A shared window of
      width *w* is one design: both sides insert at random positions inside it.
    - **The Loro peer-id rule.** Which pairs are slow, and why. A possible
      upstream report.
    - **Upstream reports.** Automerge's `load_changes` span formats the whole
      change graph when a subscriber enables it.
    - **More replicas.** All of this is two replicas on one host.
    - **A rerun with the baseline-author fix.** The archive is older than the
      fix, so its first repetition differs from the others in more than its
      ids, and the seed also picks the `random_position` positions. See
      "Limits of this archive" in the
      [grid README](../data/divergence-grid-2026-11-07/README.md).

    The dated records of each result are in
    [`docs/divergence-sweep.md`](../docs/divergence-sweep.md) under "Recorded
    observations".
    """)
    return


if __name__ == "__main__":
    app.run()
