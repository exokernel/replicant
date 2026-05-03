//! Orchestrator smoke test.
//!
//! Starts two in-process Automerge replicas, connects them via the gRPC
//! control plane, writes a key to each, and verifies both converge to the
//! same state fingerprint — exercising the full `ApplyOp` → `flush_to_peers`
//! → bidi sync stream path end-to-end.
//!
//! Exit code 0 = PASSED.  Any error prints to stderr and exits non-zero.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::transport::{Channel, Server};

use common::proto::{
    Empty, MapPut, OpRequest, PeerRef, ScalarValue, op_request, replica_client::ReplicaClient,
    replica_server::ReplicaServer, scalar_value, sync_server::SyncServer,
};
use replica::adapter::AutomergeAdapter;
use replica::server::{ReplicaService, ReplicaState, SyncService};

// ── Entry point ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let provider = init_metrics();
    opentelemetry::global::set_meter_provider(provider.clone());

    let (addr_a, addr_b) = spawn_replicas().await?;
    tracing::info!(%addr_a, %addr_b, "replicas started");

    let mut client_a = ReplicaClient::connect(format!("http://{addr_a}")).await?;
    let mut client_b = ReplicaClient::connect(format!("http://{addr_b}")).await?;

    // Open a bidi sync stream from A to B.  Both replicas register each other
    // in their peer_txs tables so subsequent flushes are bidirectional.
    client_a
        .connect_peer(Request::new(PeerRef {
            peer_id: "b".to_owned(),
            addr: addr_b.to_string(),
        }))
        .await?;

    // Wait for the TCP + Automerge sync handshake to complete.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ── Write to A, verify propagation to B ───────────────────────────────

    tracing::info!("writing 'greeting' to replica A");
    apply_map_put(&mut client_a, "greeting", "hello").await?;

    wait_for_convergence(&mut client_a, &mut client_b, Duration::from_secs(5)).await?;

    // ── Write to B, verify propagation back to A ──────────────────────────

    tracing::info!("writing 'farewell' to replica B");
    apply_map_put(&mut client_b, "farewell", "goodbye").await?;

    wait_for_convergence(&mut client_a, &mut client_b, Duration::from_secs(5)).await?;

    tracing::info!("smoke test PASSED");

    // Force-flush metrics before exit so all observations appear in output.
    provider.shutdown()?;

    Ok(())
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Build a stdout metrics provider.
///
/// Exports on shutdown (and periodically while running). In production,
/// replace with an OTLP exporter pointed at a collector.
fn init_metrics() -> SdkMeterProvider {
    use opentelemetry_sdk::metrics::PeriodicReader;
    use opentelemetry_stdout::MetricExporter;

    let reader = PeriodicReader::builder(MetricExporter::default()).build();
    SdkMeterProvider::builder().with_reader(reader).build()
}

/// Bind two OS-assigned ports, start an in-process replica on each, and
/// return their addresses.
///
/// Using `TcpListener::bind("127.0.0.1:0")` before spawning guarantees the
/// port is known and reserved before any client tries to connect.
async fn spawn_replicas() -> Result<(SocketAddr, SocketAddr)> {
    let listener_a = TcpListener::bind("127.0.0.1:0").await?;
    let listener_b = TcpListener::bind("127.0.0.1:0").await?;
    let addr_a = listener_a.local_addr()?;
    let addr_b = listener_b.local_addr()?;

    start_replica("a", listener_a);
    start_replica("b", listener_b);

    Ok((addr_a, addr_b))
}

/// Start a named replica on the given listener in a background task.
///
/// Both the `Replica` (control-plane) and `Sync` (data-plane) services are
/// multiplexed on the same port via tonic's service builder.
fn start_replica(actor_id: &'static str, listener: TcpListener) {
    let state = ReplicaState::new(actor_id.to_owned(), AutomergeAdapter::new());
    tokio::spawn(
        Server::builder()
            .add_service(ReplicaServer::new(ReplicaService::new(state.clone())))
            .add_service(SyncServer::new(SyncService::new(state)))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
}

/// Apply a `MapPut` on ROOT with a string value to the given replica.
async fn apply_map_put(client: &mut ReplicaClient<Channel>, key: &str, value: &str) -> Result<()> {
    client
        .apply_op(Request::new(OpRequest {
            op: Some(op_request::Op::MapPut(MapPut {
                obj: String::new(),
                key: key.to_owned(),
                value: Some(ScalarValue {
                    value: Some(scalar_value::Value::StrVal(value.to_owned())),
                }),
            })),
        }))
        .await?;
    Ok(())
}

/// Poll both replicas' fingerprints until they match, or `timeout` elapses.
///
/// Convergence is declared when both fingerprints are non-empty and equal —
/// identical byte sequences mean both replicas have the same Automerge DAG
/// frontier.
async fn wait_for_convergence(
    client_a: &mut ReplicaClient<Channel>,
    client_b: &mut ReplicaClient<Channel>,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();

    loop {
        let fp_a = client_a
            .get_state_fingerprint(Request::new(Empty {}))
            .await?
            .into_inner()
            .fingerprint;
        let fp_b = client_b
            .get_state_fingerprint(Request::new(Empty {}))
            .await?
            .into_inner()
            .fingerprint;

        if !fp_a.is_empty() && fp_a == fp_b {
            tracing::info!(
                elapsed_ms = start.elapsed().as_millis(),
                "replicas converged"
            );
            return Ok(());
        }

        if start.elapsed() >= timeout {
            bail!(
                "replicas did not converge within {}s (fp_a={fp_a:?}, fp_b={fp_b:?})",
                timeout.as_secs()
            );
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
