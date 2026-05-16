//! `replicant-replica` — one gRPC replica process.
//!
//! Spins up an [`AutomergeAdapter`] behind [`ReplicaService`] (control RPCs)
//! and [`SyncService`] (peer sync streams) on the configured port, with a
//! stdout OTel metrics exporter for development visibility.

use clap::Parser as _;
use common::proto::{replica_server::ReplicaServer, sync_server::SyncServer};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use replica::adapter::AutomergeAdapter;
use replica::server::{ReplicaService, ReplicaState, SyncService};
use tonic::transport::Server;

#[derive(clap::Parser)]
#[command(about = "Replicant replica process")]
struct Args {
    /// Stable actor ID for this replica (used in metric labels and peer routing)
    #[arg(long)]
    actor: String,

    /// gRPC listen port
    #[arg(long, default_value = "50051")]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let provider = init_metrics()?;
    opentelemetry::global::set_meter_provider(provider.clone());

    let args = Args::parse();
    let addr = format!("0.0.0.0:{}", args.port).parse()?;
    let state = ReplicaState::new(args.actor.clone(), AutomergeAdapter::new());

    tracing::info!(actor = %args.actor, %addr, "replica starting");

    Server::builder()
        .add_service(ReplicaServer::new(ReplicaService::new(state.clone())))
        .add_service(SyncServer::new(SyncService::new(state)))
        .serve(addr)
        .await?;

    // Flush any buffered metrics before exit.
    provider.shutdown()?;

    Ok(())
}

/// Build a metrics provider.
///
/// Attaches an OTLP gRPC exporter when `OTEL_EXPORTER_OTLP_ENDPOINT` (or the
/// metrics-specific `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`) is set; the OTLP
/// builder reads the env var itself. Without it, no reader is attached:
/// instruments still record but nothing is exported, which keeps standalone
/// `cargo run --bin replica` quiet for local development.
fn init_metrics() -> anyhow::Result<SdkMeterProvider> {
    let env_set = std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
        || std::env::var_os("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").is_some();

    let mut builder = SdkMeterProvider::builder();
    if env_set {
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .build()?;
        builder = builder.with_reader(PeriodicReader::builder(exporter).build());
    }
    Ok(builder.build())
}
