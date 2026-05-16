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

use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{SdkMeterProvider, Temporality};

mod runner;
mod topology;

use topology::{RunResult, ScenarioFile, builtin_scenarios};

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

    /// Write OTel metrics (sync message counts, op latencies, doc sizes) as JSON
    /// to this file after all scenarios complete. If omitted, metrics are discarded.
    #[arg(long)]
    metrics_file: Option<PathBuf>,

    /// TOML scenario files to run. Omit to run the built-in regression scenarios.
    scenarios: Vec<PathBuf>,
}

/// Output format for per-trial and summary records.
///
/// Both formats interleave per-trial rows with the per-scenario summary row,
/// distinguished by a `row_type` column/field (`trial` or `summary`).
#[derive(clap::ValueEnum, Debug, Clone, Copy)]
enum OutputFormat {
    /// CSV rows; header is emitted once before the first record.
    Csv,
    /// JSON Lines (one self-describing object per line, no surrounding array).
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

    let args = Args::parse();
    if args.trials == 0 {
        bail!("--trials must be at least 1");
    }
    let (provider, file_exporter) = init_metrics(args.metrics_file.as_deref())?;
    opentelemetry::global::set_meter_provider(provider.clone());

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
            "row_type,scenario,trial,node_count,op_count,topology_kind,edge_count,diameter,convergence_ms,mean_ms,p50_ms,p95_ms"
        );
    }

    for scenario in &scenarios {
        let node_count = scenario.node_count();
        let op_count = scenario.op_count();
        let mut trial_ms: Vec<f64> = Vec::with_capacity(args.trials);
        // Captured from the first trial — topology_kind/edge_count/diameter
        // are deterministic per scenario, so the summary row reuses them.
        let mut shape: Option<(&'static str, usize, usize)> = None;

        for t in 1..=args.trials {
            let result = run_scenario_once(scenario).await?;
            debug_assert_eq!(
                result.total_ops, op_count,
                "scenario '{}' trial {t}: runner reported {} ops, expected {}",
                scenario.name, result.total_ops, op_count,
            );
            shape = Some((result.topology_kind, result.edge_count, result.diameter));
            trial_ms.push(result.convergence_ms);
            emit_trial(
                args.output,
                &scenario.name,
                t,
                node_count,
                op_count,
                result.topology_kind,
                result.edge_count,
                result.diameter,
                result.convergence_ms,
            );
        }

        let (kind, edges, diameter) = shape.expect("trials >= 1 enforced above");
        let s = stats(&trial_ms);
        emit_summary(
            args.output,
            &scenario.name,
            args.trials,
            node_count,
            op_count,
            kind,
            edges,
            diameter,
            &s,
        );
    }

    // If a metrics file was requested, trigger a final collection and write to disk.
    if file_exporter.is_some() {
        provider
            .force_flush()
            .map_err(|e| anyhow::anyhow!("metrics flush failed: {e:?}"))?;
        tracing::info!(path = ?args.metrics_file, "OTel metrics written");
    }
    // Drop without calling provider.shutdown(): the stdout MetricExporter
    // (used when no metrics file is specified) writes a JSON blob on shutdown,
    // which would pollute the structured stdout output.
    drop(provider);
    Ok(())
}

// ── Scenario helpers ─────────────────────────────────────────────────────────

/// Run a single scenario once and return its result.
async fn run_scenario_once(scenario: &ScenarioFile) -> Result<RunResult> {
    tracing::info!(scenario = %scenario.name, "starting");
    let result = match (&scenario.topology, &scenario.partition_heal) {
        (Some(config), None) => runner::run(config).await?,
        (None, Some(config)) => runner::run_partition_heal(config).await?,
        _ => bail!(
            "scenario '{}': exactly one of [topology] or [partition_heal] must be present",
            scenario.name
        ),
    };
    tracing::info!(
        scenario = %scenario.name,
        convergence_ms = result.convergence_ms,
        "PASSED",
    );
    Ok(result)
}

// ── Output emission ──────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn emit_trial(
    fmt: OutputFormat,
    scenario: &str,
    trial: usize,
    node_count: usize,
    op_count: usize,
    topology_kind: &str,
    edge_count: usize,
    diameter: usize,
    ms: f64,
) {
    match fmt {
        OutputFormat::Csv => {
            // Empty trailing fields are the summary-only columns (mean_ms, p50_ms, p95_ms).
            println!(
                "trial,{scenario},{trial},{node_count},{op_count},{topology_kind},{edge_count},{diameter},{ms:.3},,,"
            );
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
                    "topology_kind": topology_kind,
                    "edge_count": edge_count,
                    "diameter": diameter,
                    "convergence_ms": ms,
                })
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_summary(
    fmt: OutputFormat,
    scenario: &str,
    n_trials: usize,
    node_count: usize,
    op_count: usize,
    topology_kind: &str,
    edge_count: usize,
    diameter: usize,
    s: &TrialStats,
) {
    match fmt {
        OutputFormat::Csv => {
            // `trial` column holds the trial count for summary rows; `convergence_ms` is empty.
            println!(
                "summary,{scenario},{n_trials},{node_count},{op_count},{topology_kind},{edge_count},{diameter},,{:.3},{:.3},{:.3}",
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
                    "topology_kind": topology_kind,
                    "edge_count": edge_count,
                    "diameter": diameter,
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
/// All values are in milliseconds, matching the input to [`stats`].
struct TrialStats {
    mean: f64,
    p50: f64,
    p95: f64,
}

/// Compute mean, p50, and p95 over a non-empty slice of millisecond durations.
fn stats(values: &[f64]) -> TrialStats {
    debug_assert!(!values.is_empty());
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    TrialStats {
        mean: sorted.iter().sum::<f64>() / sorted.len() as f64,
        p50: percentile(&sorted, 50.0),
        p95: percentile(&sorted, 95.0),
    }
}

/// Nearest-rank percentile on a pre-sorted slice.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    let idx = ((p / 100.0 * sorted.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[idx]
}

// ── Metrics setup ─────────────────────────────────────────────────────────────

/// Build the metrics provider.
///
/// If `metrics_file` is `Some`, attaches a [`FileMetricExporter`] that writes a
/// JSON snapshot to the given path on `force_flush`.  Otherwise falls back to
/// the stdout exporter (whose output is suppressed by never calling `shutdown`).
fn init_metrics(
    metrics_file: Option<&std::path::Path>,
) -> Result<(SdkMeterProvider, Option<FileMetricExporter>)> {
    use opentelemetry_sdk::metrics::PeriodicReader;

    if let Some(path) = metrics_file {
        let file = std::fs::File::create(path)
            .with_context(|| format!("cannot create metrics file {}", path.display()))?;
        let exporter = FileMetricExporter::new(file);
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        Ok((provider, Some(exporter)))
    } else {
        use opentelemetry_stdout::MetricExporter;
        let reader = PeriodicReader::builder(MetricExporter::default()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        Ok((provider, None))
    }
}

// ── FileMetricExporter ────────────────────────────────────────────────────────

/// A [`PushMetricExporter`] that serialises each export call to a JSON line in a file.
///
/// The struct is cheaply `Clone`able; all clones share the same underlying file
/// handle via an `Arc<Mutex<…>>` so that the instance given to `PeriodicReader`
/// and the one kept by the caller both write to the same file.
#[derive(Clone)]
struct FileMetricExporter {
    sink: Arc<Mutex<std::fs::File>>,
}

impl FileMetricExporter {
    fn new(file: std::fs::File) -> Self {
        Self {
            sink: Arc::new(Mutex::new(file)),
        }
    }
}

impl fmt::Debug for FileMetricExporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FileMetricExporter")
    }
}

impl PushMetricExporter for FileMetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> opentelemetry_sdk::error::OTelSdkResult {
        use std::io::Write;
        let line = serialize_metrics(metrics);
        let mut file = self.sink.lock().expect("metrics file mutex poisoned");
        // Ignore individual write errors; metrics are best-effort.
        let _ = writeln!(file, "{line}");
        Ok(())
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        // File is unbuffered, so its `Drop` closes the handle and the OS
        // commits any pending writes — no explicit flush needed here.
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

// ── Metric serialisation ──────────────────────────────────────────────────────

/// Serialise a [`ResourceMetrics`] snapshot to a compact JSON string.
///
/// Output shape:
/// ```json
/// {"metrics":[{"name":"replicant.sync.messages.tx","kind":"sum",
///   "data_points":[{"actor":"node-0","peer":"node-1","value":14}]}]}
/// ```
fn serialize_metrics(rm: &ResourceMetrics) -> String {
    let metrics_arr: Vec<serde_json::Value> = rm
        .scope_metrics()
        .flat_map(|sm| sm.metrics())
        .map(|m| {
            let (kind, data_points) = match m.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => (
                    "sum",
                    sum.data_points()
                        .map(|dp| {
                            data_point(dp.attributes(), |o| {
                                o.insert("value".into(), dp.value().into());
                            })
                        })
                        .collect(),
                ),
                AggregatedMetrics::U64(MetricData::Gauge(gauge)) => (
                    "gauge",
                    gauge
                        .data_points()
                        .map(|dp| {
                            data_point(dp.attributes(), |o| {
                                o.insert("value".into(), dp.value().into());
                            })
                        })
                        .collect(),
                ),
                AggregatedMetrics::F64(MetricData::Histogram(hist)) => (
                    "histogram",
                    hist.data_points()
                        .map(|dp| {
                            data_point(dp.attributes(), |o| {
                                o.insert("count".into(), dp.count().into());
                                o.insert("sum".into(), dp.sum().into());
                                if let Some(v) = dp.min() {
                                    o.insert("min".into(), v.into());
                                }
                                if let Some(v) = dp.max() {
                                    o.insert("max".into(), v.into());
                                }
                            })
                        })
                        .collect(),
                ),
                _ => {
                    tracing::warn!(metric = m.name(), "unhandled aggregation kind");
                    ("unknown", Vec::new())
                }
            };
            serde_json::json!({
                "name": m.name(),
                "description": m.description(),
                "unit": m.unit(),
                "kind": kind,
                "data_points": data_points,
            })
        })
        .collect();

    // serde_json::Value serialization is infallible (no non-string map keys).
    serde_json::to_string(&serde_json::json!({ "metrics": metrics_arr }))
        .expect("serde_json::Value serialization is infallible")
}

/// Build one JSON data-point object: attributes as top-level keys, plus
/// whatever numeric fields `fill` inserts (e.g. `value`, or `count`/`sum`).
fn data_point<'a, F>(
    attrs: impl Iterator<Item = &'a opentelemetry::KeyValue>,
    fill: F,
) -> serde_json::Value
where
    F: FnOnce(&mut serde_json::Map<String, serde_json::Value>),
{
    let mut obj = kv_to_map(attrs);
    fill(&mut obj);
    serde_json::Value::Object(obj)
}

/// Convert an attribute iterator to a `serde_json::Map` keyed by attribute name.
fn kv_to_map<'a>(
    attrs: impl Iterator<Item = &'a opentelemetry::KeyValue>,
) -> serde_json::Map<String, serde_json::Value> {
    attrs
        .map(|kv| {
            (
                kv.key.to_string(),
                serde_json::Value::String(kv.value.to_string()),
            )
        })
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── stats / percentile ─────────────────────────────────────────────────

    #[test]
    fn stats_single_value() {
        let s = stats(&[42.0]);
        assert_eq!(s.mean, 42.0);
        assert_eq!(s.p50, 42.0);
        assert_eq!(s.p95, 42.0);
    }

    #[test]
    fn stats_mean_and_percentiles() {
        // [10, 20, 30, 40, 50] — mean=30, p50=30, p95=50
        let s = stats(&[50.0, 10.0, 40.0, 30.0, 20.0]);
        assert_eq!(s.mean, 30.0);
        assert_eq!(s.p50, 30.0);
        assert_eq!(s.p95, 50.0);
    }

    #[test]
    fn percentile_p100_is_max() {
        let sorted = vec![1.0f64, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(percentile(&sorted, 100.0), 5.0);
    }

    #[test]
    fn percentile_p0_is_min() {
        let sorted = vec![1.0f64, 2.0, 3.0, 4.0, 5.0];
        // ceil(0/100 * 5) = 0, saturating_sub(1) = 0 → first element
        assert_eq!(percentile(&sorted, 0.0), 1.0);
    }
}
