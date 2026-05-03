//! Orchestrator — runs topology scenarios and reports convergence metrics.
//!
//! With no file arguments, runs three built-in regression scenarios.
//! With file arguments, runs each TOML scenario file `--trials` times and
//! emits structured output to stdout:
//!
//! ```text
//! cargo run --bin orchestrator -- --trials 5 --output csv scenarios/full-mesh-n5.toml
//! ```
//!
//! Tracing logs are written to stderr so stdout remains pipe-clean.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Parser;
use opentelemetry_sdk::metrics::SdkMeterProvider;

mod runner;
mod topology;

use topology::{
    Connections, Group, PartitionConfig, RunResult, ScenarioFile, TopologyConfig, WritePattern,
};

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(about = "CRDT convergence benchmark orchestrator")]
struct Args {
    /// Number of times to run each scenario (must be ≥ 1).
    #[arg(long, default_value_t = 1)]
    trials: usize,

    /// Output format for benchmark results written to stdout.
    #[arg(long, value_enum, default_value_t = OutputFormat::Csv)]
    output: OutputFormat,

    /// TOML scenario files to run. Omit to run the built-in regression scenarios.
    scenarios: Vec<PathBuf>,
}

/// Output format for per-trial and summary records.
#[derive(clap::ValueEnum, Debug, Clone, Copy)]
enum OutputFormat {
    /// CSV rows; header is emitted once before the first record.
    Csv,
    /// Newline-delimited JSON (JSON Lines); one object per record.
    Json,
}

// ── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // Tracing always goes to stderr so structured stdout output stays clean.
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(std::io::stderr)
        .init();

    let provider = init_metrics();
    opentelemetry::global::set_meter_provider(provider.clone());

    let args = Args::parse();
    if args.trials == 0 {
        bail!("--trials must be at least 1");
    }

    let scenarios: Vec<ScenarioFile> = if args.scenarios.is_empty() {
        builtin_scenarios()
    } else {
        args.scenarios
            .iter()
            .map(|path| {
                let raw = std::fs::read_to_string(path)
                    .with_context(|| format!("reading {}", path.display()))?;
                toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
            })
            .collect::<Result<_>>()?
    };

    if matches!(args.output, OutputFormat::Csv) {
        println!(
            "row_type,scenario,trial,node_count,op_count,convergence_ms,mean_ms,p50_ms,p95_ms"
        );
    }

    for scenario in &scenarios {
        let node_count = scenario_node_count(scenario);
        let mut trial_ms: Vec<u128> = Vec::with_capacity(args.trials);
        let mut last_op_count = 0usize;

        for t in 1..=args.trials {
            let result = run_scenario_once(scenario).await?;
            last_op_count = result.total_ops;
            trial_ms.push(result.convergence_ms);
            emit_trial(
                args.output,
                &scenario.name,
                t,
                node_count,
                result.total_ops,
                result.convergence_ms,
            );
        }

        let s = stats(&trial_ms);
        emit_summary(
            args.output,
            &scenario.name,
            args.trials,
            node_count,
            last_op_count,
            &s,
        );
    }

    // provider.shutdown() is intentionally skipped here: the stdout MetricExporter
    // would write OTel JSON blobs to stdout and corrupt the structured output.
    // Orchestrator OTel metrics are secondary to the captured convergence_ms data.
    let _ = provider;
    Ok(())
}

// ── Scenario helpers ─────────────────────────────────────────────────────────

/// Run a single scenario once and return its result.
async fn run_scenario_once(scenario: &ScenarioFile) -> Result<RunResult> {
    match (&scenario.topology, &scenario.partition_heal) {
        (Some(config), None) => {
            tracing::info!(scenario = %scenario.name, "starting");
            let r = runner::run(config).await?;
            tracing::info!(scenario = %scenario.name, convergence_ms = r.convergence_ms, "PASSED");
            Ok(r)
        }
        (None, Some(config)) => {
            tracing::info!(scenario = %scenario.name, "starting");
            let r = runner::run_partition_heal(config).await?;
            tracing::info!(scenario = %scenario.name, heal_convergence_ms = r.convergence_ms, "PASSED");
            Ok(r)
        }
        _ => bail!(
            "scenario '{}': exactly one of [topology] or [partition_heal] must be present",
            scenario.name
        ),
    }
}

/// Return the node count for any scenario variant.
fn scenario_node_count(s: &ScenarioFile) -> usize {
    s.topology
        .as_ref()
        .map(|t| t.node_count)
        .or_else(|| s.partition_heal.as_ref().map(|p| p.node_count))
        .unwrap_or(0)
}

/// Built-in scenarios run when no file arguments are given (regression suite).
fn builtin_scenarios() -> Vec<ScenarioFile> {
    vec![
        ScenarioFile {
            name: "full-mesh-n2".to_owned(),
            topology: Some(TopologyConfig {
                node_count: 2,
                connections: Connections::FullMesh,
                write_pattern: WritePattern::RoundRobin,
                op_count: 2,
            }),
            partition_heal: None,
        },
        ScenarioFile {
            name: "full-mesh-n3".to_owned(),
            topology: Some(TopologyConfig {
                node_count: 3,
                connections: Connections::FullMesh,
                write_pattern: WritePattern::RoundRobin,
                op_count: 6,
            }),
            partition_heal: None,
        },
        ScenarioFile {
            name: "partition-heal-n4".to_owned(),
            topology: None,
            partition_heal: Some(PartitionConfig {
                node_count: 4,
                groups: vec![Group { nodes: vec![0, 1] }, Group { nodes: vec![2, 3] }],
                ops_per_group: 4,
                write_pattern: WritePattern::RoundRobin,
            }),
        },
    ]
}

// ── Output emission ──────────────────────────────────────────────────────────

fn emit_trial(
    fmt: OutputFormat,
    scenario: &str,
    trial: usize,
    node_count: usize,
    op_count: usize,
    ms: u128,
) {
    match fmt {
        OutputFormat::Csv => {
            // Empty trailing fields are the summary-only columns (mean_ms, p50_ms, p95_ms).
            println!("trial,{scenario},{trial},{node_count},{op_count},{ms},,,");
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::json!({
                    "row_type": "trial",
                    "scenario": scenario,
                    "trial": trial,
                    "node_count": node_count,
                    "op_count": op_count,
                    "convergence_ms": ms,
                })
            );
        }
    }
}

fn emit_summary(
    fmt: OutputFormat,
    scenario: &str,
    n_trials: usize,
    node_count: usize,
    op_count: usize,
    s: &TrialStats,
) {
    match fmt {
        OutputFormat::Csv => {
            // `trial` column holds the trial count for summary rows; `convergence_ms` is empty.
            println!(
                "summary,{scenario},{n_trials},{node_count},{op_count},,{:.3},{:.3},{:.3}",
                s.mean, s.p50, s.p95
            );
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::json!({
                    "row_type": "summary",
                    "scenario": scenario,
                    "trials": n_trials,
                    "node_count": node_count,
                    "op_count": op_count,
                    "mean_ms": s.mean,
                    "p50_ms": s.p50,
                    "p95_ms": s.p95,
                })
            );
        }
    }
}

// ── Statistics ───────────────────────────────────────────────────────────────

/// Aggregated convergence statistics for a scenario's trial set.
struct TrialStats {
    mean: f64,
    p50: f64,
    p95: f64,
}

/// Compute mean, p50, and p95 over a non-empty slice of millisecond durations.
fn stats(values: &[u128]) -> TrialStats {
    debug_assert!(!values.is_empty());
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    TrialStats {
        mean: sorted.iter().sum::<u128>() as f64 / sorted.len() as f64,
        p50: percentile(&sorted, 50.0),
        p95: percentile(&sorted, 95.0),
    }
}

/// Nearest-rank percentile on a pre-sorted slice.
fn percentile(sorted: &[u128], p: f64) -> f64 {
    let idx = ((p / 100.0 * sorted.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[idx] as f64
}

// ── Metrics setup ─────────────────────────────────────────────────────────────

/// Build a stdout metrics provider for the in-process replica servers.
fn init_metrics() -> SdkMeterProvider {
    use opentelemetry_sdk::metrics::PeriodicReader;
    use opentelemetry_stdout::MetricExporter;

    let reader = PeriodicReader::builder(MetricExporter::default()).build();
    SdkMeterProvider::builder().with_reader(reader).build()
}
