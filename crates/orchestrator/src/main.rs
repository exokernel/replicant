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

mod contention;
mod provenance;
mod runner;
mod topology;

use replica::adapter::Crdt;
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

    /// Write a run-provenance file (commit, host, build profile, seeds, cell
    /// parameters) as JSON to this path. Pair it with the result CSV, which
    /// records none of that; `results/` is gitignored, so without a provenance
    /// file a stored CSV cannot be traced back to the code that produced it.
    ///
    /// Written before the first trial, so an interrupted sweep still leaves a
    /// record of what it was configured to run.
    #[arg(long)]
    provenance: Option<PathBuf>,

    /// Parse the scenarios and write the provenance file, then exit without
    /// running any trials. Everything in it is derived from the config and the
    /// seeds, so cell metadata — including achieved contention — is available
    /// without spending the CPU a sweep would cost.
    #[arg(long)]
    dry_run: bool,

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

    /// Which CRDT library backs the in-process replicas.
    ///
    /// Only meaningful without `--replicas`: externally-managed replicas were
    /// launched with their own `--crdt` and the orchestrator cannot change
    /// them, so passing both is rejected rather than silently ignored. In the
    /// containerized lanes the choice is made by the deploy generators — see
    /// the `crdt` argument on `just bench-docker` / `just bench-k8s`.
    #[arg(long, value_parser = crdt_parser())]
    crdt: Option<Crdt>,

    /// TOML scenario files to run. Omit to run the built-in regression scenarios.
    scenarios: Vec<PathBuf>,
}

/// clap parser for `--crdt`.
///
/// `PossibleValuesParser` gives `--help` and the error message the accepted
/// list, and `.map` turns the validated string back into a [`Crdt`] so the
/// args struct stays typed. Both are derived from [`Crdt::ALL`], so a new
/// backend cannot be accepted while going unmentioned in `--help`.
///
/// This lives in the binary rather than on `Crdt` itself: a `ValueEnum`
/// derive would put a CLI concern in a library crate for its binaries'
/// benefit. The cost is these few lines in each binary that takes the flag,
/// which is the right side of that trade — each binary owns its own CLI.
fn crdt_parser() -> impl clap::builder::TypedValueParser<Value = Crdt> {
    use clap::builder::TypedValueParser as _;
    clap::builder::PossibleValuesParser::new(Crdt::ALL.map(|c| c.as_str())).map(|s| {
        s.parse::<Crdt>()
            .expect("PossibleValuesParser restricts the input to Crdt::ALL")
    })
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
    // Reject rather than ignore: a sweep that silently ran Automerge because
    // --crdt was overridden by the stack's own flag is exactly the kind of
    // wrong-library result that reads as a real finding.
    if !args.replicas.is_empty() && args.crdt.is_some() {
        bail!(
            "--crdt applies only to in-process replicas; with --replicas the \
             library is fixed by how each replica was launched (see the `crdt` \
             argument on `just bench-docker` / `just bench-k8s`)"
        );
    }
    let in_process_crdt = args.crdt.unwrap_or(Crdt::Automerge);
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

    if let Some(path) = &args.provenance {
        provenance::write(
            path,
            &provenance::RunMeta {
                scenarios: &scenarios,
                paths: &args.scenarios,
                trials: args.trials,
                replicas: &args.replicas,
                // Only the in-process lane's library is the orchestrator's to
                // know; for an external stack the bench scripts fill this in.
                crdt: args.replicas.is_empty().then(|| in_process_crdt.as_str()),
            },
        )?;
        tracing::info!(path = %path.display(), "run provenance written");
    }

    if args.dry_run {
        tracing::info!(
            scenarios = scenarios.len(),
            "dry run — scenarios parsed, no trials executed"
        );
        return Ok(());
    }

    if matches!(args.output, OutputFormat::Csv) {
        println!("{}", header());
    }

    for scenario in &scenarios {
        let node_count = scenario.node_count();
        let op_count = scenario.op_count();
        let mut trial_ms: Vec<f64> = Vec::with_capacity(args.trials);
        let mut wiring_ms: Vec<f64> = Vec::with_capacity(args.trials);
        // Captured from the first trial — topology_kind/edge_count/diameter
        // are deterministic per scenario, so the summary row reuses them.
        let mut last_result: Option<RunResult> = None;

        for t in 1..=args.trials {
            let source = if args.replicas.is_empty() {
                NodeSource::InProcess(in_process_crdt)
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
            wiring_ms.push(result.wiring_ms);
            emit(
                args.output,
                &trial_row(&scenario.name, t, node_count, op_count, &result),
            );
            last_result = Some(result);
        }

        let shape = last_result.expect("trials >= 1 enforced above");
        let s = stats(&trial_ms);
        emit(
            args.output,
            &summary_row(
                &scenario.name,
                args.trials,
                node_count,
                op_count,
                &shape,
                &s,
                stats(&wiring_ms).mean,
            ),
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

/// The output schema, in column order — the single definition of it.
///
/// Both row kinds carry every column; the ones a kind does not populate render
/// as an empty CSV field and are omitted from the JSON object. [`emit`] asserts
/// that each row it renders matches this list name-for-name and in order, so a
/// column cannot be added to one row kind and forgotten in the other or in the
/// header.
///
/// Columns are append-only: `wiring_ms` and `mean_wiring_ms` are at the end
/// rather than beside the timings they belong with, so the original twelve keep
/// the positions any archived CSV in `data/` was written with.
const COLUMNS: &[&str] = &[
    "row_type",
    "scenario",
    "trial",
    "node_count",
    "op_count",
    "topology_kind",
    "edge_count",
    "diameter",
    "convergence_ms",
    "mean_ms",
    "p50_ms",
    "p95_ms",
    "wiring_ms",
    "mean_wiring_ms",
];

/// One output row as `(column, value)` pairs in [`COLUMNS`] order. `None` marks
/// a column that does not apply to this row kind.
type Row = Vec<(&'static str, Option<serde_json::Value>)>;

/// The CSV header line.
fn header() -> String {
    COLUMNS.join(",")
}

/// Build the per-trial row.
fn trial_row(
    scenario: &str,
    trial: usize,
    node_count: usize,
    op_count: usize,
    result: &RunResult,
) -> Row {
    vec![
        ("row_type", Some("trial".into())),
        ("scenario", Some(scenario.into())),
        ("trial", Some(trial.into())),
        ("node_count", Some(node_count.into())),
        ("op_count", Some(op_count.into())),
        ("topology_kind", Some(result.topology_kind.into())),
        ("edge_count", Some(result.edge_count.into())),
        ("diameter", Some(result.diameter.into())),
        ("convergence_ms", Some(result.convergence_ms.into())),
        ("mean_ms", None),
        ("p50_ms", None),
        ("p95_ms", None),
        ("wiring_ms", Some(result.wiring_ms.into())),
        ("mean_wiring_ms", None),
    ]
}

/// Build the per-scenario summary row.
///
/// `shape` supplies the structural fields (topology kind, edge count,
/// diameter), which are deterministic per scenario and so identical in every
/// trial. The `trial` column holds the trial *count* on this row kind.
fn summary_row(
    scenario: &str,
    n_trials: usize,
    node_count: usize,
    op_count: usize,
    shape: &RunResult,
    s: &TrialStats,
    mean_wiring_ms: f64,
) -> Row {
    vec![
        ("row_type", Some("summary".into())),
        ("scenario", Some(scenario.into())),
        ("trial", Some(n_trials.into())),
        ("node_count", Some(node_count.into())),
        ("op_count", Some(op_count.into())),
        ("topology_kind", Some(shape.topology_kind.into())),
        ("edge_count", Some(shape.edge_count.into())),
        ("diameter", Some(shape.diameter.into())),
        ("convergence_ms", None),
        ("mean_ms", Some(s.mean.into())),
        ("p50_ms", Some(s.p50.into())),
        ("p95_ms", Some(s.p95.into())),
        ("wiring_ms", None),
        ("mean_wiring_ms", Some(mean_wiring_ms.into())),
    ]
}

/// Print `row` in the requested format.
fn emit(fmt: OutputFormat, row: &Row) {
    debug_assert!(
        row.iter()
            .map(|(name, _)| *name)
            .eq(COLUMNS.iter().copied()),
        "row columns do not match COLUMNS in name or order"
    );
    match fmt {
        OutputFormat::Csv => println!("{}", csv_line(row)),
        OutputFormat::Json => println!("{}", json_line(row)),
    }
}

/// Render a row as one CSV line.
fn csv_line(row: &Row) -> String {
    row.iter()
        .map(|(_, value)| csv_cell(value.as_ref()))
        .collect::<Vec<_>>()
        .join(",")
}

/// One CSV cell: absent values are empty, floats carry three decimal places
/// (the historical precision of every millisecond column), strings are bare
/// (no field in this schema can contain a comma), and integers print plainly.
fn csv_cell(value: Option<&serde_json::Value>) -> String {
    match value {
        None => String::new(),
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Number(n)) if n.is_f64() => {
            format!("{:.3}", n.as_f64().expect("checked is_f64"))
        }
        Some(other) => other.to_string(),
    }
}

/// Render a row as one self-describing JSON object; absent columns are omitted
/// rather than emitted as `null`.
fn json_line(row: &Row) -> String {
    let obj: serde_json::Map<String, serde_json::Value> = row
        .iter()
        .filter_map(|(name, value)| value.clone().map(|v| ((*name).to_owned(), v)))
        .collect();
    serde_json::Value::Object(obj).to_string()
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

    // ── Output schema ──────────────────────────────────────────────────────

    fn sample_result() -> RunResult {
        RunResult {
            convergence_ms: 12.3456,
            wiring_ms: 1.5,
            total_ops: 10,
            topology_kind: "full_mesh",
            edge_count: 6,
            diameter: 1,
        }
    }

    fn sample_stats() -> TrialStats {
        TrialStats {
            mean: 1.0,
            p50: 2.0,
            p95: 3.0,
        }
    }

    /// The header and both row kinds are generated from one column list, so
    /// they cannot drift apart. This is the property that broke when the schema
    /// lived in three hand-maintained format strings.
    #[test]
    fn header_and_both_row_kinds_share_one_column_list() {
        let trial = trial_row("s", 1, 3, 10, &sample_result());
        let summary = summary_row("s", 5, 3, 10, &sample_result(), &sample_stats(), 1.5);

        for row in [&trial, &summary] {
            let names: Vec<&str> = row.iter().map(|(name, _)| *name).collect();
            assert_eq!(names, COLUMNS, "row columns diverged from COLUMNS");
        }
        assert_eq!(header().split(',').count(), COLUMNS.len());
        assert_eq!(csv_line(&trial).split(',').count(), COLUMNS.len());
        assert_eq!(csv_line(&summary).split(',').count(), COLUMNS.len());
    }

    /// Each row kind fills its own columns and leaves the other kind's empty.
    #[test]
    fn row_kinds_populate_complementary_columns() {
        let trial = trial_row("s", 1, 3, 10, &sample_result());
        let summary = summary_row("s", 5, 3, 10, &sample_result(), &sample_stats(), 1.5);
        let filled = |row: &Row, name: &str| {
            row.iter()
                .find(|(n, _)| *n == name)
                .expect("column exists")
                .1
                .is_some()
        };

        for name in ["convergence_ms", "wiring_ms"] {
            assert!(filled(&trial, name), "trial row missing {name}");
            assert!(!filled(&summary, name), "summary row should omit {name}");
        }
        for name in ["mean_ms", "p50_ms", "p95_ms", "mean_wiring_ms"] {
            assert!(filled(&summary, name), "summary row missing {name}");
            assert!(!filled(&trial, name), "trial row should omit {name}");
        }
    }

    /// Millisecond columns keep three decimal places, absent columns are empty
    /// fields, and strings are unquoted — the CSV shape archived sweeps in
    /// `data/` were written with.
    #[test]
    fn csv_cells_preserve_the_historical_formatting() {
        let line = csv_line(&trial_row("full-mesh-n3", 2, 3, 10, &sample_result()));
        let cells: Vec<&str> = line.split(',').collect();
        assert_eq!(cells[0], "trial");
        assert_eq!(cells[1], "full-mesh-n3");
        assert_eq!(cells[3], "3");
        assert_eq!(cells[8], "12.346", "convergence_ms rounds to 3 places");
        assert_eq!(cells[9], "", "mean_ms is summary-only");
        assert_eq!(cells[12], "1.500", "wiring_ms rounds to 3 places");
    }

    /// JSON Lines rows stay self-describing: every populated column becomes a
    /// key, and columns that do not apply are omitted rather than `null`.
    #[test]
    fn json_rows_omit_inapplicable_columns() {
        let json: serde_json::Value =
            serde_json::from_str(&json_line(&trial_row("s", 1, 3, 10, &sample_result()))).unwrap();
        assert_eq!(json["row_type"], "trial");
        assert_eq!(json["convergence_ms"], 12.3456);
        assert_eq!(json["wiring_ms"], 1.5);
        assert!(json.get("mean_ms").is_none());

        let json: serde_json::Value = serde_json::from_str(&json_line(&summary_row(
            "s",
            5,
            3,
            10,
            &sample_result(),
            &sample_stats(),
            1.5,
        )))
        .unwrap();
        assert_eq!(json["trial"], 5, "summary `trial` holds the trial count");
        assert_eq!(json["mean_wiring_ms"], 1.5);
        assert!(json.get("convergence_ms").is_none());
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
