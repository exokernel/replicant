"""Integration tests of the analysis helpers against the real binaries.

They need the release orchestrator and replay binaries (`just test-py-all`
builds them), but no Docker and no trace files. Each test takes about a second.
"""

import subprocess
import sys

import pytest

import tour_helpers as th

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def binaries():
    """Fails at once, with the command to run, if a binary is missing."""
    for binary in (th.BINARY, th.REPLAY):
        if not binary.exists():
            pytest.fail(f"{binary} is missing: run `cargo build --release --bin orchestrator --bin replay`")


def test_the_orchestrator_accepts_the_scenario_toml_and_models_contention():
    cells = [
        th.partition("paper", [[0], [1]], 1000, workload="text_splice", locality="same_region", baseline=th.PAPER),
        th.topology("ring", 4, "ring", 5),
    ]
    paper, ring = th.dry_run(cells, "yrs", th.BINARY)
    assert paper["name"] == "paper"
    assert paper["achieved_contention"]["contested_ops"]["mean"] == 2000
    assert ring["name"] == "ring"
    assert "achieved_contention" not in ring or ring["achieved_contention"] is None


def test_an_in_process_run_reports_rows_and_a_trace():
    run = th.run(th.topology("two", 2, "full_mesh", 3), "loro", th.BINARY, trials=2, lane="in-process")
    assert list(run.trials["trial"]) == [1, 2]
    assert run.summary["row_type"] == "summary"
    assert run.sync_messages() > 0
    assert th.timed_polls(run.trial_events(2))


def test_the_fork_trace_passes_its_tree_check_on_every_library():
    fork = {
        "kind": "concurrent",
        "numAgents": 2,
        "oracle": {"kind": "tree_invariants"},
        "txns": [
            {"agent": 0, "parents": [], "patches": [[0, 0, "base"]]},
            {"agent": 0, "parents": [0], "patches": [[0, 0, "AA"]]},
            {"agent": 1, "parents": [0], "patches": [[0, 0, "BB"]]},
        ],
    }
    result = th.replay_trace(fork)
    assert sorted(result.trials["crdt"]) == sorted(th.CRDTS)
    assert result.failed.empty


def test_a_failed_check_is_a_row_not_an_exception():
    exact = {"endContent": "nope", "txns": [{"patches": [[0, 0, "hello"]]}]}
    result = th.replay_trace(exact, crdts=["yrs"])
    assert result.trials.empty
    assert "final content is wrong" in result.failed["error"].iloc[0]


def test_the_smoke_spec_replays_from_the_fixtures():
    result = th.replay(
        "--spec", "workloads/smoke.toml", data_dir=th.REPO / "workloads" / "fixtures", crdts=["automerge"], trials=1
    )
    assert sorted(result.trials["workload"]) == ["smoke.receiver", "smoke.single"]
    assert result.run["build_profile"] == "release"


def test_the_divergence_notebook_runs_headless(tmp_path):
    out = tmp_path / "divergence.html"
    proc = subprocess.run(
        [sys.executable, "-m", "marimo", "export", "html", str(th.REPO / "analysis" / "divergence.py"), "-o", str(out)],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert "application/vnd.marimo+error" not in out.read_text()
