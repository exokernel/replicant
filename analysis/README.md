# Analysis

[`tour.py`](tour.py) is a guided [marimo](https://marimo.io) notebook. It
explains how Replicant works and how to read what it measures, and it runs
small scenarios live. It is for a reader who knows CRDTs but not Replicant.

```sh
just setup   # installs requirements.txt into .venv
just tour    # opens the notebook in a browser
```

The notebook builds the release orchestrator, then runs each scenario with
in-process replicas and `--trace`. [`tour_helpers.py`](tour_helpers.py) holds
the code that runs the orchestrator and draws the charts.

[`archive/`](archive/) holds the first set of Jupyter notebooks, and the
commands that generate their input data. They are kept so their analysis and
plots can be reused.
