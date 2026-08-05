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

use runner::{NodeSource, ReplicaEndpoint};
use topology::{RunResult, ScenarioBody, ScenarioFile, builtin_scenarios};

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

    /// Connect to externally-managed replicas instead of spawning in-process.
    ///
    /// Comma-separated `client_addr[=peer_addr]` entries. `client_addr` is what
    /// the orchestrator dials; `peer_addr` (defaults to `client_addr` if
    /// omitted) is what each replica passes to its peers in `ConnectPeer`. The
    /// two diverge when replicas live in a different network namespace from
    /// the orchestrator — e.g. docker-compose, where the orchestrator reaches
    /// replicas via published ports but containers reach each other via
    /// service DNS:
    ///
    /// ```text
    /// --replicas localhost:50051=replica-0:50051,localhost:50052=replica-1:50051
    /// ```
    ///
    /// Each external replica must be launched with actor ID `node-N` (matching
    /// the orchestrator's wiring scheme) and the scenario's `node_count` must
    /// equal the number of entries. The runner calls the `Reset` RPC on every
    /// replica at the start of each trial, so multiple scenarios and trials can
    /// share a single long-lived stack without the prior run's state leaking in.
    #[arg(long, value_delimiter = ',', value_parser = parse_replica_endpoint)]
    replicas: Vec<ReplicaEndpoint>,

    /// TOML scenario files to run. Omit to run the built-in regression scenarios.
    scenarios: Vec<PathBuf>,
}

/// Parse a single `client_addr[=peer_addr]` entry from the `--replicas` flag.
fn parse_replica_endpoint(s: &str) -> Result<ReplicaEndpoint, String> {
    let (client, peer) = s.split_once('=').unwrap_or((s, s));
    let (client, peer) = (client.trim(), peer.trim());
    if client.is_empty() || peer.is_empty() {
        return Err(format!("empty address in --replicas entry '{s}'"));
    }
    Ok(ReplicaEndpoint {
        client_addr: client.to_owned(),
        peer_addr: peer.to_owned(),
    })
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
        let mut last_result: Option<RunResult> = None;

        for t in 1..=args.trials {
            let source = if args.replicas.is_empty() {
                NodeSource::InProcess
            } else {
                NodeSource::External(args.replicas.clone())
            };
            let result = run_scenario_once(scenario, source, t).await?;
            debug_assert_eq!(
                result.total_ops, op_count,
                "scenario '{}' trial {t}: runner reported {} ops, expected {}",
                scenario.name, result.total_ops, op_count,
            );
            trial_ms.push(result.convergence_ms);
            emit_trial(
                args.output,
                &scenario.name,
                t,
                node_count,
                op_count,
                &result,
            );
            last_result = Some(result);
        }

        let shape = last_result.expect("trials >= 1 enforced above");
        let s = stats(&trial_ms);
        emit_summary(
            args.output,
            &scenario.name,
            args.trials,
            node_count,
            op_count,
            &shape,
            &s,
        );
    }

    // Flush any pending metrics before exit. Shutting down the provider gives
    // the OTLP exporter a chance to send its final batch (PeriodicReader's
    // interval is longer than a typical scenario run); for the file exporter
    // it triggers the final JSON snapshot.
    provider
        .shutdown()
        .map_err(|e| anyhow::anyhow!("metrics provider shutdown failed: {e:?}"))?;
    if file_exporter.is_some() {
        tracing::info!(path = ?args.metrics_file, "OTel metrics written");
    }
    Ok(())
}

// ── Scenario helpers ─────────────────────────────────────────────────────────

/// Run a single scenario once and return its result.
///
/// `repetition` is the 1-based trial index; it seeds the divergence generator
/// so each repetition of a text cell draws an independent op stream (the
/// source of run-to-run CV), while map-put scenarios ignore it.
async fn run_scenario_once(
    scenario: &ScenarioFile,
    source: NodeSource,
    repetition: usize,
) -> Result<RunResult> {
    tracing::info!(scenario = %scenario.name, "starting");
    let result = match &scenario.body {
        ScenarioBody::Topology(config) => runner::run(config, source).await?,
        ScenarioBody::PartitionHeal(config) => {
            runner::run_partition_heal(config, source, repetition).await?
        }
    };
    tracing::info!(
        scenario = %scenario.name,
        convergence_ms = result.convergence_ms,
        "PASSED",
    );
    Ok(result)
}

// ── Output emission ──────────────────────────────────────────────────────────

fn emit_trial(
    fmt: OutputFormat,
    scenario: &str,
    trial: usize,
    node_count: usize,
    op_count: usize,
    result: &RunResult,
) {
    let RunResult {
        convergence_ms: ms,
        topology_kind,
        edge_count,
        diameter,
        ..
    } = *result;
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

fn emit_summary(
    fmt: OutputFormat,
    scenario: &str,
    n_trials: usize,
    node_count: usize,
    op_count: usize,
    shape: &RunResult,
    s: &TrialStats,
) {
    let RunResult {
        topology_kind,
        edge_count,
        diameter,
        ..
    } = *shape;
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
/// Exporter precedence:
/// 1. `--metrics-file PATH` → [`FileMetricExporter`] (offline JSON snapshot).
/// 2. `OTEL_EXPORTER_OTLP_ENDPOINT` (or the metrics-specific variant) set →
///    OTLP gRPC exporter; endpoint is read from the env var by the builder.
/// 3. Neither → no reader; instruments still record but nothing is exported,
///    keeping `just smoke` and unit-test runs free of exporter noise.
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
        return Ok((provider, Some(exporter)));
    }

    let mut builder = SdkMeterProvider::builder();
    let otlp_endpoint_set = std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
        || std::env::var_os("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").is_some();
    if otlp_endpoint_set {
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .build()
            .context("building OTLP metric exporter")?;
        builder = builder.with_reader(PeriodicReader::builder(exporter).build());
    }
    Ok((builder.build(), None))
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
        .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
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

    // ── parse_replica_endpoint ────────────────────────────────────────────

    #[test]
    fn replica_endpoint_single_addr_uses_same_for_client_and_peer() {
        let ep = parse_replica_endpoint("localhost:50051").unwrap();
        assert_eq!(ep.client_addr, "localhost:50051");
        assert_eq!(ep.peer_addr, "localhost:50051");
    }

    #[test]
    fn replica_endpoint_distinct_client_and_peer() {
        let ep = parse_replica_endpoint("localhost:50051=replica-0:50051").unwrap();
        assert_eq!(ep.client_addr, "localhost:50051");
        assert_eq!(ep.peer_addr, "replica-0:50051");
    }

    #[test]
    fn replica_endpoint_trims_whitespace_around_separator() {
        let ep = parse_replica_endpoint(" localhost:50051 = replica-0:50051 ").unwrap();
        assert_eq!(ep.client_addr, "localhost:50051");
        assert_eq!(ep.peer_addr, "replica-0:50051");
    }

    #[test]
    fn replica_endpoint_rejects_empty_side() {
        assert!(parse_replica_endpoint("=replica-0:50051").is_err());
        assert!(parse_replica_endpoint("localhost:50051=").is_err());
        assert!(parse_replica_endpoint("").is_err());
    }
}
