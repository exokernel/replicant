"""Unit tests of tour_helpers: the scenario TOML, the trace readers, and the
archive check. None of them runs a binary."""

import json
import tomllib

import pandas as pd
import pytest

import tour_helpers as th


# ── Scenarios ───────────────────────────────────────────────────────────────


def test_a_topology_scenario_round_trips_through_toml():
    scenario = th.topology("ring", 5, "ring", 20, write_pattern="concentrated", op_interval_ms=2)
    assert tomllib.loads(th.to_toml(scenario)) == scenario


def test_a_partition_with_a_baseline_round_trips_through_toml():
    scenario = th.partition(
        "p", [[0, 1], [2]], 100, workload="text_splice", locality="same_region", baseline=th.PAPER
    )
    parsed = tomllib.loads(th.to_toml(scenario))
    body = parsed["partition_heal"]
    assert body["baseline"] == th.PAPER
    assert body["groups"] == [{"nodes": [0, 1]}, {"nodes": [2]}]
    assert body["node_count"] == 3
    assert body["locality"] == "same_region"


def test_a_partition_without_a_baseline_has_no_baseline_table():
    assert "baseline" not in th.to_toml(th.partition("p", [[0], [1]], 1))


def test_toml_quotes_strings_and_writes_booleans():
    text = th.to_toml({"name": 'a "b"', "t": {"flag": True, "items": ["x", 1]}})
    assert tomllib.loads(text) == {"name": 'a "b"', "t": {"flag": True, "items": ["x", 1]}}


def test_toml_refuses_a_value_it_cannot_write():
    with pytest.raises(TypeError):
        th.to_toml({"name": "n", "t": {"bad": None}})


# ── Trace readers ───────────────────────────────────────────────────────────


def heal_trace() -> list[dict]:
    """One partition-heal trial: gate polls, the heal, three timed polls."""
    return [
        {"trial": 1, "t_ms": 0.0, "event": "wired", "edges": [[0, 1]], "blocked": [[0, 1]]},
        {"trial": 1, "t_ms": 1.0, "event": "write", "node": 0, "op": 0},
        {"trial": 1, "t_ms": 2.0, "event": "write", "node": 1, "op": 1},
        {"trial": 1, "t_ms": 3.0, "event": "poll", "nodes": [0], "classes": [0]},
        {"trial": 1, "t_ms": 10.0, "event": "clock_start"},
        {"trial": 1, "t_ms": 10.5, "event": "unblocked"},
        {"trial": 1, "t_ms": 11.0, "event": "kicked"},
        {"trial": 1, "t_ms": 12.0, "event": "poll", "nodes": [0, 1], "classes": [0, 1]},
        {"trial": 1, "t_ms": 14.5, "event": "poll", "nodes": [0, 1], "classes": [0, 1]},
        {"trial": 1, "t_ms": 17.0, "event": "poll", "nodes": [0, 1], "classes": [2, 2]},
        {"trial": 1, "t_ms": 17.0, "event": "converged", "nodes": [0, 1]},
        {
            "trial": 1, "t_ms": 18.0, "event": "stats", "node": 0, "doc_size_bytes": 50,
            "peers": [{"peer": 1, "tx": 3, "rx": 2, "deferred": 0}],
        },
        {
            "trial": 1, "t_ms": 18.5, "event": "stats", "node": 1, "doc_size_bytes": 70,
            "peers": [{"peer": 0, "tx": 2, "rx": 3, "deferred": 1}],
        },
    ]


def test_phase_times_reads_each_step_and_the_writes():
    p = th.phase_times(heal_trace())
    assert p == {
        "wired": 0.0, "clock_start": 10.0, "unblocked": 10.5, "kicked": 11.0,
        "converged": 17.0, "first_write": 1.0, "last_write": 2.0,
    }


def test_timed_polls_skip_the_gate_and_start_at_the_clock():
    assert th.timed_polls(heal_trace()) == [2.0, 4.5, 7.0]
    assert th.poll_rounds(heal_trace()) == [2.5, 2.5]
    assert th.poll_count(heal_trace()) == 4


def test_a_heal_of_one_poll_has_no_round():
    events = [e for e in heal_trace() if e["t_ms"] not in (12.0, 14.5)]
    assert th.timed_polls(events) == [7.0]
    assert th.poll_rounds(events) == []


def run_of(events: list[dict]) -> th.Run:
    """A Run with `events` and no rows."""
    return th.Run(
        scenario={}, crdt="yrs", lane="in-process", trials=pd.DataFrame(), summary={}, events=events
    )


def test_run_reads_links_and_sync_counts_from_the_trace():
    run = run_of(heal_trace())
    assert run.edges() == ([(0, 1)], [(0, 1)])
    assert run.sync_per_edge() == {(0, 1): 5}
    assert run.sync_messages() == 5
    assert run.deferred() == 1
    assert run.doc_size() == 70
    assert len(run.trial_events(1)) == len(heal_trace())
    assert run.trial_events(2) == []


def test_the_trace_table_puts_the_fields_of_an_event_in_detail():
    table = run_of(heal_trace()).trace_table()
    assert list(table.columns) == ["t_ms", "event", "detail"]
    write = table[table["event"] == "write"].iloc[0]
    assert write["detail"] == "node=0, op=0"


def test_trace_graph_draws_a_chain_and_a_fork():
    chain = th.trace_graph({"txns": [{"patches": [[0, 0, "a"]]}, {"patches": [[1, 0, "b"]]}]})
    assert "t0 --> t1" in chain
    fork = th.trace_graph(
        {"txns": [
            {"agent": 0, "parents": [], "patches": [[0, 0, "x"]]},
            {"agent": 0, "parents": [0], "patches": []},
            {"agent": 1, "parents": [0], "patches": []},
        ]}
    )
    assert "t0 --> t1" in fork and "t0 --> t2" in fork and "t1 --> t2" not in fork


def test_charts_draw_a_graph_and_a_timeline():
    graph = th.graph_chart(3, [(0, 1), (1, 2)], blocked=[(1, 2)], weights={(0, 1): 4})
    assert len(graph.to_dict()["layer"]) == 3
    timeline = th.timeline_chart(heal_trace(), 2)
    assert len(timeline.to_dict()["layer"]) == 4


# ── Archive check and readers ───────────────────────────────────────────────


def write_result(folder, name: str, rows: str, crdt: str | None) -> None:
    """Writes `name.csv` with `rows`, and its provenance with `crdt`."""
    (folder / f"{name}.csv").write_text(rows)
    run = {"crdt": crdt} if crdt else {}
    (folder / f"{name}.meta.json").write_text(json.dumps({"invocations": [{"run": run}]}))


def test_check_archive_finds_a_label_that_disagrees(tmp_path):
    grid = tmp_path / "grid"
    grid.mkdir()
    write_result(grid, "yrs-good", "a\n1\n", "yrs")
    write_result(grid, "loro-stale", "", "automerge")
    write_result(grid, "pilot", "a\n1\n", None)
    (grid / "no-meta.csv").write_text("a\n")
    table = th.check_archive(tmp_path).set_index("file")
    assert len(table) == 3
    assert table.loc["grid/loro-stale.csv", "verdict"] == "WRONG LABEL"
    assert table.loc["grid/loro-stale.csv", "csv rows"] == "none"
    assert table.loc["grid/yrs-good.csv", "verdict"] == "ok"
    assert table.loc["grid/pilot.csv", "name says"] == "-"


def test_trace_folders_alone_do_not_count_as_traces(tmp_path, monkeypatch):
    monkeypatch.setattr(th, "TRACES", tmp_path)
    for path in th.TOUR_TRACES:
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
    assert not th.traces_available()
    for path in th.TOUR_TRACES:
        (tmp_path / path).write_text("{}")
    assert th.traces_available()
    assert not th.traces_available("crdt-benchmarks/absent.trace.json")


def test_check_archive_shows_lost_provenance(tmp_path):
    (tmp_path / "yrs-timeout.csv").write_text("")
    (tmp_path / "yrs-timeout.meta.json").write_text(
        json.dumps({"provenance_unavailable": "the run timed out", "invocations": []})
    )
    row = th.check_archive(tmp_path).iloc[0]
    assert row["verdict"] == "no provenance"
    assert row["provenance says"] == "unavailable"


def test_library_versions_and_workloads_read_the_repository(tmp_path, monkeypatch):
    (tmp_path / "Cargo.lock").write_text(
        '[[package]]\nname = "yrs"\nversion = "1.2.3"\n\n[[package]]\nname = "tokio"\nversion = "1.0.0"\n'
    )
    specs = tmp_path / "workloads"
    (specs / "fixtures").mkdir(parents=True)
    (specs / "fixtures" / "s.json").write_text("{}")
    (specs / "smoke.toml").write_text('[[workload]]\nname = "s"\ntrace = "s.json"\nsha256 = ""\n')
    (specs / "suite.toml").write_text(
        '[[workload]]\nname = "b"\ntrace = "absent.json"\nsha256 = ""\nheavy = true\n'
    )
    monkeypatch.setattr(th, "REPO", tmp_path)
    monkeypatch.setattr(th, "TRACES", tmp_path / "traces")
    assert th.library_versions() == {"yrs": "1.2.3"}
    table = th.workloads().set_index("name")
    assert bool(table.loc["s", "trace present"]) is True
    assert bool(table.loc["b", "trace present"]) is False
    assert bool(table.loc["b", "heavy"]) is True
    assert th.traces_available() is False


def test_a_stack_names_each_replica_for_the_orchestrator():
    stack = th.DockerStack(size=2)
    assert stack.replicas_arg() == "localhost:50051=replica-0:50051,localhost:50052=replica-1:50051"
    assert th.KindStack(size=1).replicas_arg() == "localhost:51051=node-0.node:50051"
    assert "panelId=3" in stack.panel_url(3)
