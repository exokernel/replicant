//! Sets up the orchestrator's OTel metrics export, and the file exporter
//! behind `--metrics-file`.
//!
//! The in-process replicas record their metrics through the global meter
//! provider that [`init_metrics`] builds, so this is where those metrics go.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{SdkMeterProvider, Temporality};

/// Builds the metrics provider, choosing an exporter in this order:
///
/// 1. `--metrics-file PATH` gives a [`FileMetricExporter`], which writes a JSON
///    snapshot.
/// 2. `OTEL_EXPORTER_OTLP_ENDPOINT`, or its metrics-specific variant, gives an
///    OTLP gRPC exporter. The builder reads the endpoint from the variable.
/// 3. Neither gives no reader at all. Instruments still record, but nothing is
///    exported, which keeps `just smoke` and the tests quiet.
pub fn init_metrics(
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

/// A [`PushMetricExporter`] that writes each export as one JSON line to a file.
///
/// Cloning is cheap. Every clone shares one file handle through an
/// `Arc<Mutex<_>>`, so the copy held by the reader and the copy held by the
/// caller write to the same file.
#[derive(Clone)]
pub struct FileMetricExporter {
    sink: Arc<Mutex<std::fs::File>>,
}

impl FileMetricExporter {
    /// Wraps `file`. Each export appends one line to it.
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
        // Metrics are best-effort, so a failed write does not stop the run. It
        // is logged, because the file is an analysis input and a lost line
        // leaves it incomplete.
        if let Err(e) = writeln!(file, "{line}") {
            tracing::warn!(error = %e, "failed to write metrics snapshot");
        }
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

/// Serialises a [`ResourceMetrics`] snapshot to a compact JSON string.
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

    // Serialising a serde_json::Value cannot fail here: every map key is a
    // string.
    serde_json::to_string(&serde_json::json!({ "metrics": metrics_arr }))
        .expect("serde_json::Value serialization is infallible")
}

/// Builds one JSON data point. Attributes become top-level keys, and `fill`
/// adds the numeric fields, such as `value`, or `count` and `sum`.
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

/// Converts an attribute iterator to a `serde_json::Map` keyed by name.
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
