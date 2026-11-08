"""Unit tests of divergence_helpers, on small archives that each test writes."""

import json

import pytest

import divergence_helpers as dh


def test_parse_cell_reads_the_locality_the_start_and_the_ops():
    assert dh.parse_cell("divergence-n2-same_region-empty-1e4") == {
        "locality": "same_region", "start": "empty", "ops": 10_000,
    }
    assert dh.parse_cell("divergence-n2-random_position-1e2") == {
        "locality": "random_position", "start": "paper", "ops": 100,
    }
    with pytest.raises(ValueError):
        dh.parse_cell("partition-heal-n4")


def write_run(folder, name: str, crdt: str, rows: list[tuple[str, int, float | None]], cells=()):
    """Writes `name.csv` with trial rows and a summary row, and its provenance."""
    lines = ["row_type,scenario,trial,convergence_ms"]
    lines += [f"trial,{s},{t},{'' if ms is None else ms}" for s, t, ms in rows]
    lines += [f"summary,{rows[0][0]},,"]
    (folder / f"{name}.csv").write_text("\n".join(lines) + "\n")
    meta = {"invocations": [{"run": {"crdt": crdt}, "git": {"commit": "abcdef0123"}, "scenarios": list(cells)}]}
    (folder / f"{name}.meta.json").write_text(json.dumps(meta))


def test_load_grid_keeps_the_trials_and_tags_each_with_its_library(tmp_path):
    cell = "divergence-n2-append-1e2"
    write_run(tmp_path, "yrs-small", "yrs", [(cell, 1, 2.0), (cell, 2, 4.0), (cell, 3, None)])
    write_run(tmp_path, "loro-small", "loro", [(cell, 1, 1.0)])
    grid = dh.load_grid(tmp_path)
    assert sorted(grid["library"]) == ["loro", "yrs", "yrs"]
    assert set(grid["commit"]) == {"abcdef01"}
    assert set(grid["ops"]) == {100}
    med = dh.medians(grid).set_index("library")
    assert med.loc["yrs", "median"] == 3.0
    assert med.loc["yrs", "trials"] == 2


def test_load_grid_refuses_a_provenance_of_another_library(tmp_path):
    write_run(tmp_path, "automerge-small", "loro", [("divergence-n2-append-1e2", 1, 1.0)])
    with pytest.raises(ValueError, match="meta says loro"):
        dh.load_grid(tmp_path)


def test_load_contention_reads_each_cell_once(tmp_path):
    cell = {
        "name": "divergence-n2-same_region-1e2",
        "params": {"ops_per_group": 100},
        "achieved_contention": {
            "contested_ops": {"mean": 200.0},
            "max_concurrent_siblings": {"mean": 200.0},
        },
    }
    row = [(cell["name"], 1, 1.0)]
    write_run(tmp_path, "yrs-small", "yrs", row, cells=[cell])
    write_run(tmp_path, "loro-small", "loro", row, cells=[cell])
    table = dh.load_contention(tmp_path)
    assert len(table) == 1
    assert table["contested_share"].iloc[0] == 1.0


def test_the_loro_probe_csv_loads_with_and_without_quoted_labels(tmp_path):
    header = "label,peer_a,peer_b,a_imports_b_ms,b_imports_a_ms,a_lt_b"
    (tmp_path / "loro-peer-probe.csv").write_text(
        f'{header}\n1,2,0x1,0x2,8.2,10.4,true\n"a5 base",0xa,0xb,9.1,4279.0,true\n'
    )
    table = dh.load_findings(tmp_path, "loro-peer-probe")
    assert list(table["label"]) == ["1,2", "a5 base"]
    assert table["b_imports_a_ms"].tolist() == [10.4, 4279.0]


def test_latest_picks_the_newest_folder_of_a_prefix(tmp_path, monkeypatch):
    for name in ("grid-2026-01-01", "grid-2026-11-07", "other-2027-01-01"):
        (tmp_path / name).mkdir()
    monkeypatch.setattr(dh, "DATA", tmp_path)
    assert dh.latest("grid-").name == "grid-2026-11-07"
    with pytest.raises(FileNotFoundError):
        dh.latest("absent-")
