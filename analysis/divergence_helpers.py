"""Helpers for divergence.py: read the archived divergence data, and draw it.

The notebook reads only files under `data/`, so it runs from a clone with no
Docker and no build. Each loader returns a pandas DataFrame:

- `load_grid`: one row for each trial of each cell of a grid archive.
- `load_contention`: one row for each cell, with the contested share that the
  contention model computed for the cell.
- `load_findings`: the CSV of one probe of `tools/findings`.

`FIXES` is the table of measurement faults that the study found and fixed.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import altair as alt
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"

#: The libraries, in the order that every chart uses.
LIBRARIES = ("automerge", "yrs", "loro")

#: One color for each library, the same in every chart.
LIBRARY_COLORS = alt.Scale(domain=list(LIBRARIES), range=["#4c78a8", "#f58518", "#54a24b"])

#: The localities, from the least to the most contested.
LOCALITIES = ("append", "random_position", "same_region")

_CELL = re.compile(r"divergence-n2-(?P<locality>[a-z_]+?)(?P<empty>-empty)?-1e(?P<exp>\d)$")


def latest(prefix: str) -> Path:
    """Returns the newest folder under `data/` whose name starts with `prefix`."""
    folders = sorted(p for p in DATA.glob(f"{prefix}*") if p.is_dir())
    if not folders:
        raise FileNotFoundError(f"no folder data/{prefix}*")
    return folders[-1]


def parse_cell(name: str) -> dict:
    """Splits a cell name such as `divergence-n2-same_region-empty-1e4`."""
    m = _CELL.match(name)
    if m is None:
        raise ValueError(f"not a divergence-n2 cell: {name}")
    return {
        "locality": m["locality"],
        "start": "empty" if m["empty"] else "paper",
        "ops": 10 ** int(m["exp"]),
    }


def load_grid(folder: Path) -> pd.DataFrame:
    """Reads every `<library>-*.csv` of a grid folder.

    The library comes from the file name, and the function checks it against
    `run.crdt` in the matching `.meta.json`. A file with no trial rows is a run
    that failed; its cells are missing from the result.
    """
    frames = []
    for csv in sorted(folder.glob("*.csv")):
        library = csv.name.split("-")[0]
        meta = json.loads(csv.with_suffix(".meta.json").read_text())
        for invocation in meta["invocations"]:
            if invocation["run"]["crdt"] != library:
                raise ValueError(f"{csv.name}: meta says {invocation['run']['crdt']}")
        df = pd.read_csv(csv)
        df = df[df["row_type"] == "trial"].dropna(subset=["convergence_ms"])
        df = df.assign(library=library, commit=meta["invocations"][0]["git"]["commit"][:8])
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    cells = pd.DataFrame([parse_cell(s) for s in df["scenario"]], index=df.index)
    df = pd.concat([df, cells], axis=1)
    return df[["library", "scenario", "locality", "start", "ops", "trial", "convergence_ms", "commit"]]


def load_contention(folder: Path) -> pd.DataFrame:
    """Returns the contested share of each cell, from the provenance files.

    The share is the mean count of contested ops over the repetitions, divided
    by all ops of both sides. It describes the workload only: it is the same for
    every library.
    """
    rows = {}
    for meta_path in sorted(folder.glob("*.meta.json")):
        meta = json.loads(meta_path.read_text())
        for invocation in meta["invocations"]:
            for cell in invocation["scenarios"]:
                contention = cell.get("achieved_contention")
                if contention is None or cell["name"] in rows:
                    continue
                ops = cell["params"]["ops_per_group"]
                rows[cell["name"]] = {
                    "scenario": cell["name"],
                    **parse_cell(cell["name"]),
                    "contested_ops": contention["contested_ops"]["mean"],
                    "contested_share": contention["contested_ops"]["mean"] / (2 * ops),
                    "max_siblings": contention["max_concurrent_siblings"]["mean"],
                }
    return pd.DataFrame(rows.values())


def load_findings(folder: Path, probe: str) -> pd.DataFrame:
    """Reads the CSV of one probe from a `just findings` output folder."""
    if probe == "loro-peer-probe":
        return _load_loro_peers(folder / f"{probe}.csv")
    return pd.read_csv(folder / f"{probe}.csv")


def _load_loro_peers(path: Path) -> pd.DataFrame:
    """Reads the Loro peer-id CSV.

    Before 2026-11-08 the probe did not quote its labels, and some labels have a
    comma, such as `1,2`. So this reads the last five fields of each line as data,
    and the fields before them as the label.
    """
    lines = path.read_text().splitlines()
    header = lines[0].split(",")
    rows = []
    for line in lines[1:]:
        fields = line.split(",")
        label = ",".join(fields[: len(fields) - 5]).strip('"')
        rows.append([label, *fields[-5:]])
    df = pd.DataFrame(rows, columns=header)
    for column in ("a_imports_b_ms", "b_imports_a_ms"):
        df[column] = df[column].astype(float)
    return df


#: Two trials of one cell this many times apart, with no trial between them,
#: split the cell into a fast and a slow group. The Loro peer-id effect makes
#: such cells: on `same_region` at 10^4, 25 ms against 11,000 ms.
BIMODAL_RATIO = 10.0


def split_groups(values: pd.Series) -> list[tuple[str, pd.Series]]:
    """Splits the trial times of one cell at their largest gap, if the next
    value is at least `BIMODAL_RATIO` times the one before it. Returns
    `[("all", values)]` or `[("fast", ...), ("slow", ...)]`."""
    ordered = values.sort_values()
    if len(ordered) < 2:
        return [("all", ordered)]
    ratios = ordered.to_numpy()[1:] / ordered.to_numpy()[:-1]
    cut = int(ratios.argmax())
    if ratios[cut] < BIMODAL_RATIO:
        return [("all", ordered)]
    return [("fast", ordered.iloc[: cut + 1]), ("slow", ordered.iloc[cut + 1 :])]


def medians(grid: pd.DataFrame) -> pd.DataFrame:
    """Returns the median, minimum, and maximum of each cell and library.

    A cell whose trials fall into two groups (see `split_groups`) gives two
    rows, `group = "fast"` and `"slow"`. One median of both groups would
    describe no trial. Other cells give one row with `group = "all"`.
    """
    keys = ["library", "locality", "start", "ops"]
    rows = []
    for key, cell in grid.groupby(keys):
        for group, values in split_groups(cell["convergence_ms"]):
            rows.append(
                {
                    **dict(zip(keys, key)),
                    "group": group,
                    "median": values.median(),
                    "low": values.min(),
                    "high": values.max(),
                    "trials": len(values),
                }
            )
    return pd.DataFrame(rows, columns=[*keys, "group", "median", "low", "high", "trials"])


#: Measurement faults that the study found and fixed. Each row is one fault: the
#: cell or probe that showed it, the value before and after the fix, and where
#: the fix is. `fixed_in` gives the pull request and the title of the commit.
#: The stack merges each pull request as one squashed commit, so a commit hash
#: from a branch is not in the history of main, but the pull request number is.
#: The values are from the dated records in `docs/divergence-sweep.md` and the
#: archived data.
FIXES = pd.DataFrame(
    [
        ("A heal measured history, not the merge", "Automerge, 1 op/side, paper", 688, 254,
         "#5: feat(orchestrator): sync every link before the partition",
         "Links sync before the partition; the heal is a reconnect."),
        ("Sync state kept the peer's last request", "Automerge probe, 1 op/side", 190, 54,
         "#6: feat(replica): model an unblock as a reconnect",
         "An unblock keeps only the state that the library persists."),
        ("gRPC's 4 MiB message limit", "Automerge, random_position 10^5", 600_000, 3_200,
         "#5: fix(replica): raise the size limit on sync streams",
         "Raise the size limit on sync streams. Was a timeout."),
        ("A fingerprint poll encoded the whole document", "Yrs, 1 op/side, paper", 8.6, 1.5,
         "#6: perf(replica): fingerprint Yrs by its state vector and delete set",
         "Fingerprint = state vector + delete set."),
        ("The log subscriber formatted Automerge's change graph", "Automerge, 1 op/side, docker", 260, 44,
         "#6: fix(replica): keep the CRDT libraries at WARN in the log filter",
         "Keep the CRDT libraries at WARN in the log filter."),
        ("Every trial drew random library ids", "Loro, same_region 10^4: spread of one cell", 11_600, None,
         "#6: feat(orchestrator): seed the library ids of each divergence trial",
         "Seed the ids for each repetition."),
        ("A failed run kept the old provenance file", "3 files of the pilot archive", None, None,
         "#6: fix(scripts): remove the old provenance file when a bench run starts",
         "Remove the old meta.json when a run starts."),
    ],
    columns=["fault", "shown_by", "before_ms", "after_ms", "fixed_in", "fix"],
)


def heal_chart(med: pd.DataFrame, trials: pd.DataFrame) -> alt.Chart:
    """Heal time against ops per side, on log axes: a point for each trial, one
    color for each library, and a marker for each median. The line joins the
    medians of cells with one group. A cell with a fast and a slow group (see
    `medians`) shows both medians, with their own shapes, and no line."""
    x = alt.X("ops:Q", scale=alt.Scale(type="log"), title="ops per side")
    y = alt.Y("convergence_ms:Q", scale=alt.Scale(type="log"), title="heal time (ms)")
    color = alt.Color("library:N", scale=LIBRARY_COLORS)
    points = (
        alt.Chart(trials)
        .mark_circle(size=30, opacity=0.45)
        .encode(x=x, y=y, color=color, tooltip=["library", "scenario", "trial", "convergence_ms"])
    )
    data = med.rename(columns={"median": "convergence_ms"})
    tooltip = ["library", "ops", "group", "convergence_ms", "low", "high", "trials"]
    lines = (
        alt.Chart(data[data["group"] == "all"])
        .mark_line()
        .encode(x=x, y=y, color=color)
    )
    marks = (
        alt.Chart(data)
        .mark_point(filled=True, size=70)
        .encode(
            x=x,
            y=y,
            color=color,
            shape=alt.Shape("group:N", sort=["all", "fast", "slow"], title="median of"),
            tooltip=tooltip,
        )
    )
    return (points + lines + marks).properties(width=560, height=340)
