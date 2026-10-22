# Analysis

[`tour.py`](tour.py) is a guided [marimo](https://marimo.io) notebook. It
explains how Replicant works and how to read what it measures, and it runs
small scenarios live. It is for a reader who knows CRDTs but not Replicant.

```sh
just setup      # installs requirements.txt into .venv
just tour       # opens the notebook in a browser, with its code
just tour run   # opens it as an app, with the code hidden
```

marimo serves the notebook from a local web server. The marimo extension for
VS Code shows the same notebook inside the editor.

The notebook builds the release orchestrator. By default it then runs each
scenario on a 10-replica docker compose stack, which it starts itself, and
embeds live panels from the stack's Grafana dashboard. If Docker is not
running, it uses in-process replicas instead. Every run is traced.
[`tour_helpers.py`](tour_helpers.py) holds the code that runs the
orchestrator and draws the charts.

[`archive/`](archive/) holds the first set of Jupyter notebooks, and the
commands that generate their input data. They are kept so their analysis and
plots can be reused.
