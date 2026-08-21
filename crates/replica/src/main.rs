//! `replicant-replica` — one gRPC replica process.
//!
//! Spins up a [`common::CrdtAdapter`] (selected by `--crdt`) behind
//! [`ReplicaService`] (control RPCs) and [`SyncService`] (peer sync streams)
//! on the configured port, with a stdout OTel metrics exporter for
//! development visibility.

use clap::Parser as _;
use common::NodeId;
use common::proto::{replica_server::ReplicaServer, sync_server::SyncServer};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use replica::adapter::Crdt;
use replica::server::{ReplicaService, ReplicaState, SyncService};
use tonic::transport::Server;

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

/// clap adapter for [`NodeId`]'s checked constructor.
///
/// `value_parser` needs an owned error type; `anyhow::Error` is not `Clone`,
/// which clap requires, so the message is flattened to a `String`.
fn parse_node_id(s: &str) -> Result<NodeId, String> {
    NodeId::new(s).map_err(|e| e.to_string())
}

#[derive(clap::Parser)]
#[command(about = "Replicant replica process")]
struct Args {
    /// Stable actor ID for this replica (used in metric labels and peer routing)
    ///
    /// Parsed into a [`common::NodeId`], so an id that could not be sent as
    /// the `x-peer-id` gRPC header is rejected here rather than on first peer
    /// connect.
    #[arg(long, value_parser = parse_node_id)]
    actor: NodeId,

    /// gRPC listen port
    #[arg(long, default_value = "50051")]
    port: u16,

    /// Which CRDT library backs this replica's document
    #[arg(long, value_parser = crdt_parser(), default_value = "automerge")]
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

    tracing::info!(actor = %args.actor, %addr, crdt = %args.crdt, "replica starting");

    let state = ReplicaState::from_boxed_adapter(args.actor.clone(), args.crdt.build());
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
