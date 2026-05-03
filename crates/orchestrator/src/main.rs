//! Orchestrator — runs topology scenarios and reports convergence metrics.
//!
//! With no arguments, runs three built-in scenarios that serve as a
//! regression suite (`just smoke` / `just ci`).
//!
//! With file arguments, each TOML scenario file is loaded and run in order:
//!
//! ```text
//! cargo run --bin orchestrator -- scenarios/partition-heal-n4.toml
//! ```

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use opentelemetry_sdk::metrics::SdkMeterProvider;

mod runner;
mod topology;

use topology::{Connections, Group, PartitionConfig, ScenarioFile, TopologyConfig, WritePattern};

// ── Entry point ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let provider = init_metrics();
    opentelemetry::global::set_meter_provider(provider.clone());

    let args: Vec<PathBuf> = std::env::args().skip(1).map(PathBuf::from).collect();

    if args.is_empty() {
        run_builtin_scenarios().await?;
    } else {
        for path in &args {
            let raw = std::fs::read_to_string(path)
                .with_context(|| format!("reading {}", path.display()))?;
            let scenario: ScenarioFile =
                toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
            run_scenario(scenario).await?;
        }
    }

    provider.shutdown()?;
    Ok(())
}

// ── Scenario dispatch ──────────────────────────────────────────────────────

/// Load and run a single scenario, logging name and result.
async fn run_scenario(scenario: ScenarioFile) -> Result<()> {
    match (&scenario.topology, &scenario.partition_heal) {
        (Some(config), None) => {
            tracing::info!(scenario = %scenario.name, "starting");
            let r = runner::run(config).await?;
            tracing::info!(
                scenario = %scenario.name,
                ops = r.total_ops,
                convergence_ms = r.convergence_ms,
                "PASSED"
            );
        }
        (None, Some(config)) => {
            tracing::info!(scenario = %scenario.name, "starting");
            let r = runner::run_partition_heal(config).await?;
            tracing::info!(
                scenario = %scenario.name,
                ops = r.total_ops,
                heal_convergence_ms = r.convergence_ms,
                "PASSED"
            );
        }
        _ => bail!(
            "scenario '{}': exactly one of [topology] or [partition_heal] must be present",
            scenario.name
        ),
    }
    Ok(())
}

/// Hard-coded scenarios run when no file arguments are given.
///
/// Covers 2-node full mesh (backward-compat smoke), 3-node full mesh,
/// and a 4-node 2+2 partition-heal.
async fn run_builtin_scenarios() -> Result<()> {
    let scenarios = vec![
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
    ];

    for scenario in scenarios {
        run_scenario(scenario).await?;
    }

    tracing::info!("all scenarios PASSED");
    Ok(())
}

// ── Metrics setup ──────────────────────────────────────────────────────────

/// Build a stdout metrics provider.
///
/// In production, replace with an OTLP exporter pointed at a collector.
fn init_metrics() -> SdkMeterProvider {
    use opentelemetry_sdk::metrics::PeriodicReader;
    use opentelemetry_stdout::MetricExporter;

    let reader = PeriodicReader::builder(MetricExporter::default()).build();
    SdkMeterProvider::builder().with_reader(reader).build()
}
