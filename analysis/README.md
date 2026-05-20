# Analysis notebooks

One notebook per question. Each has a fixed data source so there is no
"this section silently no-ops for source X" hidden in the middle.

| Notebook | Question | Data source |
| --- | --- | --- |
| [`convergence.ipynb`](convergence.ipynb) | How does each topology converge as N grows? Boxplots, summary table, measurement stability. | `results/results{,-docker,-k8s}.csv` (pick via `SOURCE`) |
| [`protocol_metrics.ipynb`](protocol_metrics.ipynb) | Sync-message amplification, op latency, post-convergence doc size. | `results/metrics-<scenario>.json` (in-process only — see header) |
| [`live_metrics.ipynb`](live_metrics.ipynb) | Live sync traffic and doc size from a running docker / k8s stack. | PromQL on `localhost:9090` |
| [`comparison.ipynb`](comparison.ipynb) | Does the deployment target change the convergence finding? The methodology robustness check. | All three CSVs concatenated |

### Generating inputs

CSV (convergence + comparison):

```sh
# in_process
cargo run --release --bin orchestrator -- --trials 10 --output csv \
  scenarios/*.toml > results/results.csv

# docker / k8s — recipe owns the stack lifecycle
just bench-docker "scenarios/*.toml" 10
just bench-k8s "scenarios/*.toml" 10
```

OTel JSON (protocol_metrics) — run one scenario at a time so counters
don't accumulate across scenarios:

```sh
for s in $(ls scenarios/*.toml | xargs -n1 basename -s .toml); do
  cargo run --release --bin orchestrator -- --trials 10 \
    --metrics-file "results/metrics-${s}.json" \
    --output csv "scenarios/${s}.toml" > /dev/null 2>&1
done
```

Live (live_metrics) — stack must be running:

```sh
just docker-up scenarios/full-mesh-n5.toml   # or just k8s-up
# …run a scenario against it…
# then open Prometheus at http://localhost:9090
```
