//! `replicant-replica`: one gRPC replica process.
//!
//! Builds the [`common::CrdtAdapter`] that `--crdt` selects. Serves it on
//! `--port` through [`server::router`], which has the control and peer sync
//! services. Exports metrics over OTLP when the OTLP endpoint is
//! set (see `init_metrics`).

use clap::Parser as _;
use common::NodeId;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use replica::adapter::Crdt;
use replica::server::{self, ReplicaState};

/// Returns the clap parser for `--crdt`.
///
/// `PossibleValuesParser` puts the accepted names in `--help` and in the error
/// message. `.map` turns the checked string into a [`Crdt`]. Both come from
/// [`Crdt::ALL`], so `--help` always lists every backend.
fn crdt_parser() -> impl clap::builder::TypedValueParser<Value = Crdt> {
    use clap::builder::TypedValueParser as _;
    clap::builder::PossibleValuesParser::new(Crdt::ALL.map(|c| c.as_str())).map(|s| {
        s.parse::<Crdt>()
            .expect("PossibleValuesParser restricts the input to Crdt::ALL")
    })
}

/// Parses `--actor` with [`NodeId::new`].
///
/// clap requires a `Clone` error type. `anyhow::Error` is not `Clone`, so the
/// error becomes a `String`.
fn parse_node_id(s: &str) -> Result<NodeId, String> {
    NodeId::new(s).map_err(|e| e.to_string())
}

#[derive(clap::Parser)]
#[command(about = "Replicant replica process")]
struct Args {
    /// Stable actor ID for this replica (used in metric labels and peer routing)
    ///
    /// Parsed into a [`common::NodeId`]. An ID that is not a legal `x-peer-id`
    /// header value fails here, at startup.
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
    // Parse first. `--help`, `--version`, and bad arguments must exit before
    // the exporter and the subscriber are built.
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let provider = init_metrics()?;
    opentelemetry::global::set_meter_provider(provider.clone());

    let addr = format!("0.0.0.0:{}", args.port).parse()?;

    tracing::info!(actor = %args.actor, %addr, crdt = %args.crdt, "replica starting");

    let state = ReplicaState::new(args.actor.clone(), args.crdt.build());
    let signal = shutdown_signal()?;
    // A graceful shutdown waits for every open stream to end. Close the peer
    // sync streams, or the shutdown waits for the peers to close them.
    let shutdown = {
        let state = state.clone();
        async move {
            signal.await;
            state.close_peer_streams().await;
        }
    };
    server::router(state)
        .serve_with_shutdown(addr, shutdown)
        .await?;

    // Flush any buffered metrics before exit.
    provider.shutdown()?;

    Ok(())
}

/// Returns a future that resolves on Ctrl-C or SIGTERM. The server then shuts
/// down, and `main` flushes the metrics.
///
/// The replica is PID 1 in its container. The kernel does not deliver a
/// signal to PID 1 unless the process installs a handler for it. Without this
/// handler, `docker compose down` and pod deletion wait for their timeout and
/// then send SIGKILL, and the last metrics interval is lost.
fn shutdown_signal() -> anyhow::Result<impl Future<Output = ()>> {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    Ok(async move {
        #[cfg(unix)]
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutdown signal received");
    })
}

/// Builds the metrics provider.
///
/// If `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` is
/// set, attaches an OTLP gRPC exporter. The OTLP builder reads the variable
/// itself. If neither is set, attaches no reader. The instruments still
/// record, but nothing is exported, so a local `cargo run --bin replica` stays
/// quiet.
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
