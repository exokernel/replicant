use common::proto::{replica_server::ReplicaServer, sync_server::SyncServer};
use opentelemetry_sdk::metrics::SdkMeterProvider;
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
    use clap::Parser as _;

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let provider = init_metrics();
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

/// Build a stdout metrics provider that exports every 30 seconds.
///
/// Uses the stdout exporter for development visibility. In production this
/// would be replaced with an OTLP exporter pointed at a collector.
fn init_metrics() -> SdkMeterProvider {
    use opentelemetry_sdk::metrics::PeriodicReader;
    use opentelemetry_stdout::MetricExporter;

    let reader = PeriodicReader::builder(MetricExporter::default()).build();
    SdkMeterProvider::builder().with_reader(reader).build()
}
