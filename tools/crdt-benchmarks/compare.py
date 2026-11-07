#!/usr/bin/env python3
"""Compare replay results with crdt-benchmarks results, and print a Markdown report.

Each Rust adapter is compared with its nearest JS build: automerge with the raw
automerge-wasm handle, yrs with ywasm, and loro with loro-crdt. B2 is the exception
for Automerge. See `reference`. The time table also shows Automerge JS and Yjs,
because they show how much a JS factory itself costs.

This script uses the Python standard library only.
"""

import argparse
import json
import re
import statistics
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

# Rust library -> the JS results that use the same core.
REFERENCE = {"automerge": "automerge-wasm", "yrs": "ywasm", "loro": "loro"}
# The automerge-wasm factory ignores `transact`, and it makes one update for each op.
# On B2 it thus does 12000 round trips, and the replay and the other factories do 2.
# The automerge factory uses transactions, so B2 compares with that factory.
B2_AUTOMERGE_REFERENCE = "automerge"
LIBS = tuple(REFERENCE)
# A time below this is mostly rounding, because the JS harness reports whole ms.
MIN_MS = 5.0
# Two JS times closer than this are a tie, because the JS harness reports whole ms.
JS_RESOLUTION_MS = 1.0
# The largest size difference that still counts as the same document size.
SIZE_TOLERANCE = 0.02
# B1 divides total update bytes by its input count, which is 1 for these.
ONE_INPUT = {"b1.2", "b1.6", "b1.9"}
# B2 reports total update bytes divided by this number of documents.
B2_DOCS = 2
N = 6000
FIXED_ID_NOTE = (
    "B3 is not comparable for Yrs time and Loro size. This run used fixed ids, "
    "and the JS libraries use random ids. See docs/replay.md. The tables show "
    "these cells, and the totals leave them out."
)
SEEDED_ID_NOTE = (
    "This run used seeded ids that look random, as the JS ids do. One seed is one "
    "sample of the id order. See docs/replay.md."
)
# The (metric, library) pairs that the fixed ids make not comparable on B3.
FIXED_ID_SKEW = {("time", "yrs"), ("docSize", "loro")}

Row = dict[str, Any]
JsValues = dict[tuple[str, str, str], list[float]]
Trials = dict[tuple[str, str], list[Row]]


def reference(bench: str, lib: str) -> str:
    """Return the JS library to compare with `lib` on `bench`."""
    if lib == "automerge" and bench.startswith("b2."):
        return B2_AUTOMERGE_REFERENCE
    return REFERENCE[lib]


def skewed(bench: str, metric: str, lib: str, fixed: bool) -> bool:
    """Return True if fixed ids make `metric` of `lib` not comparable on `bench`."""
    return fixed and bench.startswith("b3.") and (metric, lib) in FIXED_ID_SKEW


def load_js(directory: Path) -> JsValues:
    """Return {(benchmark, metric, JS library): [one value per repeat]}."""
    values: JsValues = {}
    for path in sorted(directory.glob("r*-*.json")):
        results = json.loads(path.read_text(encoding="utf-8"))["6000"]
        for name, by_lib in results.items():
            if not (match := re.match(r"\[(.*?)\].*\((\w+)\)$", name)):
                continue
            for lib, text in by_lib.items():
                if number := re.match(r"[0-9.]+", text):
                    key = (match[1].lower(), match[2], lib)
                    values.setdefault(key, []).append(float(number[0]))
    return values


def load_replay(path: Path) -> tuple[Row, Trials, list[Row]]:
    """Return the run row, {(workload, library): [trial rows]}, and problem rows."""
    run: Row = {}
    trials: Trials = {}
    problems: list[Row] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        match row["row_type"]:
            case "run":
                run = row
            case "trial":
                if row.get("lane", "in_process") != "in_process":
                    raise SystemExit(
                        f"error: {path} has a trial on the {row['lane']} lane. Its "
                        "time_ms includes gRPC, so the JS times cannot compare with "
                        "it. Calibrate with in-process trials."
                    )
                trials.setdefault((row["workload"], row["crdt"]), []).append(row)
            case "skipped" | "failed":
                problems.append(row)
    return run, trials, problems


def replay_median(rows: Sequence[Row], field: str) -> float:
    """Return the median of a field, without the first trial, which is a warm-up."""
    return statistics.median(row[field] for row in rows[1:] or rows)


def js_median(js: JsValues, bench: str, metric: str, lib: str) -> float | None:
    """Return the median across repeats, or None if the value is absent."""
    values = js.get((bench, metric, lib))
    return statistics.median(values) if values else None


def js_update_total(js: JsValues, bench: str, lib: str) -> float | None:
    """Return total update bytes in JS, on the same scale as the replay."""
    if bench.startswith("b1."):
        average = js_median(js, bench, "avgUpdateSize", lib)
        factor = 1 if bench in ONE_INPUT else N
    else:
        average = js_median(js, bench, "updateSize", lib)
        factor = B2_DOCS if bench.startswith("b2.") else 1
    return None if average is None else average * factor


def ranking(values: dict[str, float]) -> str:
    """Return the first letter of each library, fastest first."""
    return "<".join(lib[0] for lib in sorted(values, key=values.__getitem__))


def tied(values: dict[str, float], resolution: float) -> bool:
    """Return True if two values are closer than `resolution`, or equal."""
    ordered = sorted(values.values())
    return any(b - a < resolution or a == b for a, b in zip(ordered, ordered[1:]))


def bench_order(bench: str) -> list[int | str]:
    """Return a sort key that puts b1.2 before b1.10."""
    return [int(p) if p.isdigit() else p for p in re.findall(r"\d+|[a-z]+", bench)]


def show_ms(value: float | None) -> str:
    """Format a time for a table cell."""
    if value is None:
        return "-"
    return f"{value:.0f}" if value >= 10 else f"{value:.1f}"


def print_table(header: Iterable[str], rows: Iterable[Iterable[str]]) -> None:
    """Print a Markdown table."""
    header = list(header)
    print("| " + " | ".join(header) + " |")
    print("|" + "---|" * len(header))
    for row in rows:
        print("| " + " | ".join(row) + " |")
    print()


def fixed_ids(run: Row) -> bool:
    """Return True if the run used fixed ids, not seeded ones."""
    return run.get("args", {}).get("id_order") in ("ascending", "descending")


def id_note(fixed: bool) -> str:
    """Return the note that says how the run's ids affect the comparison."""
    return FIXED_ID_NOTE if fixed else SEEDED_ID_NOTE


def report_run(run: Row) -> None:
    """Print where the replay ran."""
    print("## Run\n")
    print(f"- Rust versions: {run.get('versions')}")
    print(f"- Commit: {run.get('git_commit')} (dirty: {run.get('git_dirty')})")
    print(
        f"- Host: {run.get('host')}, {run.get('os')}/{run.get('arch')}, "
        f"{run.get('build_profile')}\n"
    )


def report_sizes(
    js: JsValues, trials: Trials, benches: Sequence[str], fixed: bool
) -> None:
    """Print the document size ratios, and how many are within the tolerance."""
    print("## Document size: replay / JS\n")
    print(id_note(fixed) + "\n")
    ratios: dict[str, list[float]] = {lib: [] for lib in LIBS}
    rows = []
    for bench in benches:
        cells = [bench]
        for lib in LIBS:
            ours = trials.get((bench, lib))
            theirs = js_median(js, bench, "docSize", reference(bench, lib))
            if not ours or not theirs:
                cells.append("-")
                continue
            ratio = replay_median(ours, "doc_size") / theirs
            if not skewed(bench, "docSize", lib, fixed):
                ratios[lib].append(ratio)
            cells.append(f"{ratio:.3f}")
        rows.append(cells)
    print_table(["benchmark", *LIBS], rows)
    for lib, values in ratios.items():
        if values:
            near = sum(abs(ratio - 1) <= SIZE_TOLERANCE for ratio in values)
            print(
                f"- {lib}: {near} of {len(values)} within {SIZE_TOLERANCE:.0%}; "
                f"median ratio {statistics.median(values):.3f}"
            )
    print()


def report_times(
    js: JsValues, trials: Trials, benches: Sequence[str], fixed: bool
) -> None:
    """Print the times, the rankings on each side, and the ratio shifts."""
    print("## Time in ms\n")
    print(
        "Automerge ranks and ratios use am-js on B2, and am-wasm on all others. "
        + id_note(fixed)
        + "\n"
    )
    print(
        f"Rank letters run fastest first. A row with a value below {MIN_MS:.0f} ms has "
        "no rank, because the JS timer rounds to whole ms. A row where two JS times "
        f"are closer than {JS_RESOLUTION_MS:.0f} ms, or two replay times are equal, "
        "shows tie and is not in the count.\n"
    )
    compared = agreed = 0
    shifts: dict[tuple[str, str], list[float]] = {}
    rows = []
    for bench in benches:
        ours = {
            lib: replay_median(trials[bench, lib], "time_ms")
            for lib in LIBS
            if (bench, lib) in trials
        }
        theirs = {
            lib: value
            for lib in LIBS
            if (value := js_median(js, bench, "time", reference(bench, lib)))
            is not None
        }
        rank_ours = rank_js = agree = "-"
        if (
            len(ours) == len(theirs) == len(LIBS)
            and min(*ours.values(), *theirs.values()) >= MIN_MS
            and not any(skewed(bench, "time", lib, fixed) for lib in LIBS)
        ):
            if tied(theirs, JS_RESOLUTION_MS) or tied(ours, 0.0):
                rank_ours = rank_js = agree = "tie"
            else:
                rank_ours, rank_js = ranking(ours), ranking(theirs)
                compared += 1
                agreed += rank_ours == rank_js
                agree = "yes" if rank_ours == rank_js else "NO"
                for i, a in enumerate(LIBS):
                    for b in LIBS[i + 1 :]:
                        shift = (ours[a] / ours[b]) / (theirs[a] / theirs[b])
                        shifts.setdefault((a, b), []).append(shift)
        rows.append(
            [
                bench,
                show_ms(ours.get("automerge")),
                show_ms(js_median(js, bench, "time", "automerge-wasm")),
                show_ms(js_median(js, bench, "time", "automerge")),
                show_ms(ours.get("yrs")),
                show_ms(theirs.get("yrs")),
                show_ms(js_median(js, bench, "time", "yjs")),
                show_ms(ours.get("loro")),
                show_ms(theirs.get("loro")),
                rank_ours,
                rank_js,
                agree,
            ]
        )
    print_table(
        [
            "benchmark",
            "automerge",
            "am-wasm",
            "am-js",
            "yrs",
            "ywasm",
            "yjs",
            "loro",
            "loro-js",
            "rank replay",
            "rank JS",
            "agree",
        ],
        rows,
    )
    print(f"Ranking agrees on {agreed} of {compared} comparable benchmarks.\n")
    print(
        "Pairwise ratio shift is (a/b in replay) / (a/b in JS). A value of 1 means the "
        "same ratio.\n"
    )
    print_table(
        ["pair", "median", "min", "max"],
        (
            [
                f"{a}/{b}",
                f"{statistics.median(v):.2f}",
                f"{min(v):.2f}",
                f"{max(v):.2f}",
            ]
            for (a, b), v in shifts.items()
        ),
    )


def report_updates(js: JsValues, trials: Trials, benches: Sequence[str]) -> None:
    """Print the total update byte ratios."""
    print("## Total update bytes: replay / JS\n")
    print(
        "B2 is not comparable for Automerge and Loro. Their JS factories repeat the first "
        "batch in the second update, and the replay does not. B3 is not comparable for "
        "Automerge. Its JS updates are larger by a fixed amount for each client.\n"
    )
    rows = []
    for bench in benches:
        cells = []
        for lib in LIBS:
            ours = trials.get((bench, lib))
            theirs = js_update_total(js, bench, reference(bench, lib))
            cells.append(
                f"{replay_median(ours, 'update_bytes') / theirs:.3f}"
                if ours and theirs
                else "-"
            )
        if any(cell != "-" for cell in cells):
            rows.append([bench, *cells])
    print_table(["benchmark", *LIBS], rows)


def report_problems(problems: Iterable[Row]) -> None:
    """Print the rows that gave no timing."""
    lines = [
        f"- {row['row_type']}: {row['workload']} {row['crdt']}: "
        f"{row.get('reason') or row.get('error')}"
        for row in problems
    ]
    if lines:
        print("## Rows with no timing\n")
        print("\n".join(lines) + "\n")


def main() -> None:
    """Read both result sets and print the report."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "js_dir", type=Path, help="the directory that run-current.sh wrote"
    )
    parser.add_argument(
        "replay", type=Path, help="the JSON Lines file that replay wrote"
    )
    args = parser.parse_args()

    js = load_js(args.js_dir)
    run, trials, problems = load_replay(args.replay)
    in_js = {bench for bench, _, _ in js}
    benches = sorted({workload for workload, _ in trials} & in_js, key=bench_order)

    report_run(run)
    fixed = fixed_ids(run)
    report_sizes(js, trials, benches, fixed)
    report_times(js, trials, benches, fixed)
    report_updates(js, trials, benches)
    report_problems(problems)


if __name__ == "__main__":
    main()
