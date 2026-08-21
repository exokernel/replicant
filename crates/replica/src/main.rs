//! `replicant-replica` — one gRPC replica process.
//!
//! Spins up a [`common::CrdtAdapter`] (selected by `--crdt`) behind
//! [`ReplicaService`] (control RPCs) and [`SyncService`] (peer sync streams)
//! on the configured port, with a stdout OTel metrics exporter for
//! development visibility.

use clap::Parser as _;
use common::proto::{replica_server::ReplicaServer, sync_server::SyncServer};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use replica::adapter::{AutomergeAdapter, LoroAdapter, YrsAdapter};
use replica::server::{ReplicaService, ReplicaState, SyncService};
use tonic::transport::Server;

/// Which [`common::CrdtAdapter`] backs this replica process. A new variant
/// only needs a matching arm in `main`'s `match args.crdt` below — the
/// `ReplicaState`/gRPC scaffolding is already generic over `CrdtAdapter`
/// (`ReplicaState::new` takes `impl CrdtAdapter` and boxes it internally).
#[derive(Copy, Clone, Debug, clap::ValueEnum)]
enum Crdt {
    Automerge,
    Yrs,
    Loro,
}

#[derive(clap::Parser)]
#[command(about = "Replicant replica process")]
struct Args {
    /// Stable actor ID for this replica (used in metric labels and peer routing)
    #[arg(long)]
    actor: String,

    /// gRPC listen port
    #[arg(long, default_value = "50051")]
    port: u16,

    /// Which CRDT library backs this replica's document
    #[arg(long, value_enum, default_value = "automerge")]
    crdt: Crdt,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse first: `--help`, `--version`, and bad arguments should exit before
    // any exporter or subscriber is built.
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let provider = init_metrics()?;
    opentelemetry::global::set_meter_provider(provider.clone());

    let addr = format!("0.0.0.0:{}", args.port).parse()?;

    tracing::info!(actor = %args.actor, %addr, crdt = ?args.crdt, "replica starting");

    // `ReplicaState::new` takes `impl CrdtAdapter` and boxes it internally
    // (`Mutex<Box<dyn CrdtAdapter>>`), so each arm just needs to construct
    // its concrete adapter — no further generic plumbing per variant.
    match args.crdt {
        Crdt::Automerge => {
            let state = ReplicaState::new(args.actor.clone(), AutomergeAdapter::new());
            Server::builder()
                .add_service(ReplicaServer::new(ReplicaService::new(state.clone())))
                .add_service(SyncServer::new(SyncService::new(state)))
                .serve(addr)
                .await?;
        }
        Crdt::Yrs => {
            let state = ReplicaState::new(args.actor.clone(), YrsAdapter::new());
            Server::builder()
                .add_service(ReplicaServer::new(ReplicaService::new(state.clone())))
                .add_service(SyncServer::new(SyncService::new(state)))
                .serve(addr)
                .await?;
        }
        Crdt::Loro => {
            let state = ReplicaState::new(args.actor.clone(), LoroAdapter::new());
            Server::builder()
                .add_service(ReplicaServer::new(ReplicaService::new(state.clone())))
                .add_service(SyncServer::new(SyncService::new(state)))
                .serve(addr)
                .await?;
        }
    }

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
