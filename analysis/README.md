# Analysis notebooks

There is one notebook for each question. Each notebook reads one fixed data
source, so no section silently does nothing for a different source.

| Notebook | Question | Data source |
| --- | --- | --- |
| [`convergence.ipynb`](convergence.ipynb) | How does each topology converge as N grows? Boxplots, summary table, measurement stability. | `results/results{,-docker,-k8s}.csv` (pick via `SOURCE`) |
| [`protocol_metrics.ipynb`](protocol_metrics.ipynb) | Sync-message amplification, op latency, post-convergence doc size. | `results/metrics-<scenario>.json` (in-process only — see header) |
| [`live_metrics.ipynb`](live_metrics.ipynb) | Live sync traffic and doc size from a running docker / k8s stack. | PromQL on `localhost:9090` |
| [`comparison.ipynb`](comparison.ipynb) | Does the deployment target change the convergence finding? The methodology robustness check. | All three CSVs concatenated |

Set `SOURCE = "in_process" | "docker" | "k8s"` in `convergence.ipynb`. Set
`INCLUDE` in `comparison.ipynb`; the default is `["docker", "k8s"]`.

## Generate inputs

These steps read CSVs from a finished sweep. They do not need a live stack.

CSV, for `convergence` and `comparison`:

```sh
mkdir -p results

# in_process: the replicas run in the orchestrator process. This is the
# cheapest lane. round_robin on relay-heavy topologies is artifactually slow
# here; see comparison.ipynb.
cargo run --release --bin orchestrator -- --trials 10 --output csv \
  scenarios/*.toml > results/results.csv

# docker / k8s: the recipes start one stack for each node_count and write
# results/results-docker.csv or results/results-k8s.csv.
just bench-docker "scenarios/*.toml" 10
just bench-k8s    "scenarios/*.toml" 10
```

OTel JSON, for `protocol_metrics`. Run one scenario per invocation, so that
counters do not accumulate across scenarios:

```sh
for s in $(ls scenarios/*.toml | xargs -n1 basename -s .toml); do
  cargo run --release --bin orchestrator -- --trials 10 \
    --metrics-file "results/metrics-${s}.json" \
    --output csv "scenarios/${s}.toml" > /dev/null 2>&1
done
```

Then run `cd analysis && jupyter lab`.

The notebooks cache each CSV as `<stem>.parquet`. They refresh the cache when
the CSV is newer. `results/` is gitignored.

## Live metrics

`live_metrics.ipynb` queries Prometheus on a running stack. It also checks that
all replicas converge to the same `doc_size_bytes`.

1. Start a stack: `just docker-up`, or `just k8s-up` and then `just k8s-ui`.
   See [`docs/deployment.md`](../docs/deployment.md).
2. Run a scenario against it. A paced scenario works best, for example
   [`full-mesh-n5-paced.toml`](../scenarios/full-mesh-n5-paced.toml).
3. Open `live_metrics.ipynb` and Run All.
