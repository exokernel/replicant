use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Context as _;
use opentelemetry::KeyValue;
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use common::proto::{self, replica_server::Replica, sync_server::Sync};
use common::{CrdtAdapter, Op};

use crate::metrics::Metrics;

// ── Shared state ───────────────────────────────────────────────────────────

/// Shared document state, peer connection registry, and metric instruments
/// for one replica.
///
/// Wrapped in `Arc` so the two gRPC service structs ([`ReplicaService`] and
/// [`SyncService`]) and all background tasks share the same instance without
/// copying the document or lock.
pub struct ReplicaState {
    /// Stable actor identifier used in metric labels and as the `x-peer-id`
    /// gRPC metadata value when opening outbound sync streams.
    actor_id: String,
    /// `std::sync::Mutex` is intentional: the lock is never held across an
    /// `.await` point, so there is no risk of blocking the async executor.
    adapter: Mutex<Box<dyn CrdtAdapter>>,
    /// Outbound raw-bytes sender per connected peer.
    ///
    /// Uses `tokio::sync::Mutex` because `flush_to_peers` holds this lock
    /// across `.await` points while sending on each channel.
    peer_txs: tokio::sync::Mutex<HashMap<String, mpsc::Sender<Vec<u8>>>>,
    metrics: Metrics,
}

impl ReplicaState {
    /// Create shared state from a stable `actor_id` and a concrete adapter.
    ///
    /// Instruments are obtained from the global OTel meter provider, so the
    /// provider must be initialized before calling this.
    ///
    /// Panics if `actor_id` is not a valid HTTP-header value — it is sent as
    /// the `x-peer-id` metadata header on every outbound sync stream, and
    /// failing here makes the misconfiguration obvious at startup rather than
    /// on first peer connect.
    pub fn new(actor_id: String, adapter: impl CrdtAdapter) -> Arc<Self> {
        assert!(
            actor_id
                .parse::<tonic::metadata::AsciiMetadataValue>()
                .is_ok(),
            "actor_id must be a valid HTTP-header value: {actor_id:?}",
        );
        let meter = opentelemetry::global::meter("replicant");
        Arc::new(Self {
            actor_id,
            adapter: Mutex::new(Box::new(adapter)),
            peer_txs: tokio::sync::Mutex::new(HashMap::new()),
            metrics: Metrics::new(&meter),
        })
    }

    // Each method below takes the adapter lock just long enough to call one
    // trait method; the lock is never held across an `.await`. Panics if the
    // mutex is poisoned (another thread panicked while holding it — the
    // document state is no longer trustworthy).

    fn apply_op(&self, op: &Op) -> anyhow::Result<()> {
        self.adapter
            .lock()
            .expect("adapter mutex poisoned")
            .apply_op(op)
    }

    fn get_heads(&self) -> Vec<Vec<u8>> {
        self.adapter
            .lock()
            .expect("adapter mutex poisoned")
            .get_heads()
    }

    fn state_fingerprint(&self) -> Vec<u8> {
        self.adapter
            .lock()
            .expect("adapter mutex poisoned")
            .state_fingerprint()
    }

    fn doc_size_bytes(&self) -> usize {
        self.adapter
            .lock()
            .expect("adapter mutex poisoned")
            .doc_size_bytes()
    }

    fn sync_generate(&self, peer: &str) -> Option<Vec<u8>> {
        self.adapter
            .lock()
            .expect("adapter mutex poisoned")
            .sync_generate(peer)
    }

    fn sync_receive(&self, peer: &str, msg: Vec<u8>) -> anyhow::Result<()> {
        self.adapter
            .lock()
            .expect("adapter mutex poisoned")
            .sync_receive(peer, msg)
    }

    pub fn actor_id(&self) -> &str {
        &self.actor_id
    }

    // ── Metric recorders ───────────────────────────────────────────────────
    //
    // These wrap the attribute construction so callers don't repeat the
    // `KeyValue::new("actor", ...)` boilerplate. Keeping them on `ReplicaState`
    // means there's exactly one place that knows which attributes each
    // instrument expects.

    fn record_op_duration(&self, op_name: &'static str, elapsed_ms: f64) {
        self.metrics.op_duration_ms.record(
            elapsed_ms,
            &[
                KeyValue::new("actor", self.actor_id.clone()),
                KeyValue::new("op", op_name),
            ],
        );
    }

    /// Sample the document size and record it on the gauge.
    fn record_doc_size(&self) {
        self.metrics.doc_size_bytes.record(
            self.doc_size_bytes() as u64,
            &[KeyValue::new("actor", self.actor_id.clone())],
        );
    }

    fn record_sync_tx(&self, peer_id: &str) {
        self.metrics.sync_tx.add(1, &self.peer_attrs(peer_id));
    }

    fn record_sync_rx(&self, peer_id: &str) {
        self.metrics.sync_rx.add(1, &self.peer_attrs(peer_id));
    }

    fn peer_attrs(&self, peer_id: &str) -> [KeyValue; 2] {
        [
            KeyValue::new("actor", self.actor_id.clone()),
            KeyValue::new("peer", peer_id.to_owned()),
        ]
    }

    async fn register_peer(&self, peer_id: String, tx: mpsc::Sender<Vec<u8>>) {
        self.peer_txs.lock().await.insert(peer_id, tx);
    }

    async fn deregister_peer(&self, peer_id: &str) {
        self.peer_txs.lock().await.remove(peer_id);
    }

    /// Generate and push any pending sync messages to all connected peers.
    ///
    /// Called immediately after every local op so peers hear about changes
    /// without waiting for the next inbound message.
    async fn flush_to_peers(&self) {
        // Collect (peer_id, sender) in one lock acquisition; no lock is held
        // across the subsequent async sends.
        let peers: Vec<(String, mpsc::Sender<Vec<u8>>)> = self
            .peer_txs
            .lock()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (peer_id, tx) in &peers {
            if let Some(msg) = self.sync_generate(peer_id)
                && tx.try_send(msg).is_ok()
            {
                self.record_sync_tx(peer_id);
            }
        }
    }
}

// ── Service structs ────────────────────────────────────────────────────────

/// gRPC [`Replica`] service — handles control-plane RPCs from the orchestrator
/// (apply ops, inspect state, connect peers, shutdown).
#[derive(Clone)]
pub struct ReplicaService {
    state: Arc<ReplicaState>,
}

/// gRPC [`Sync`] service — accepts inbound bidi sync streams from peer replicas.
#[derive(Clone)]
pub struct SyncService {
    state: Arc<ReplicaState>,
}

impl ReplicaService {
    pub fn new(state: Arc<ReplicaState>) -> Self {
        Self { state }
    }
}

impl SyncService {
    pub fn new(state: Arc<ReplicaState>) -> Self {
        Self { state }
    }
}

// ── Replica service ────────────────────────────────────────────────────────

#[tonic::async_trait]
impl Replica for ReplicaService {
    async fn apply_op(
        &self,
        request: Request<proto::OpRequest>,
    ) -> Result<Response<proto::OpResponse>, Status> {
        let op = Op::try_from(request.into_inner())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let t0 = Instant::now();
        self.state
            .apply_op(&op)
            .map_err(|e| Status::internal(e.to_string()))?;
        let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;

        self.state.record_op_duration(op.name(), elapsed_ms);
        self.state.record_doc_size();

        self.state.flush_to_peers().await;

        Ok(Response::new(proto::OpResponse {}))
    }

    async fn get_heads(
        &self,
        _: Request<proto::Empty>,
    ) -> Result<Response<proto::HeadsResponse>, Status> {
        Ok(Response::new(proto::HeadsResponse {
            heads: self.state.get_heads(),
        }))
    }

    async fn get_state_fingerprint(
        &self,
        _: Request<proto::Empty>,
    ) -> Result<Response<proto::FingerprintResponse>, Status> {
        Ok(Response::new(proto::FingerprintResponse {
            fingerprint: self.state.state_fingerprint(),
        }))
    }

    async fn connect_peer(
        &self,
        request: Request<proto::PeerRef>,
    ) -> Result<Response<proto::Empty>, Status> {
        let proto::PeerRef { peer_id, addr } = request.into_inner();
        let state = self.state.clone();
        // `ready_rx` resolves once the TCP connection and gRPC stream are open
        // and the peer is registered in `peer_txs`. Awaiting it here means the
        // orchestrator's `ConnectPeer` call only returns after the stream is
        // actually usable, removing the need for any post-connect sleep.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(connect_to_peer(state, peer_id.clone(), addr, ready_tx));
        ready_rx.await.map_err(|_| {
            Status::internal("connect_to_peer task dropped before signalling ready")
        })?;
        // Await the handle in a watcher task so errors and panics that occur
        // after the stream signals ready are logged rather than silently lost
        // when the handle would otherwise be dropped here.
        tokio::spawn(async move {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::error!(peer = %peer_id, "sync stream error: {e:#}"),
                Err(e) => tracing::error!(peer = %peer_id, "sync stream task panicked: {e}"),
            }
        });
        Ok(Response::new(proto::Empty {}))
    }

    async fn shutdown(&self, _: Request<proto::Empty>) -> Result<Response<proto::Empty>, Status> {
        // Graceful per-replica shutdown is not yet implemented. In the current
        // in-process model the orchestrator tears everything down by dropping
        // the process, which triggers provider.shutdown() and flushes OTel.
        // Warn so a future caller relying on this RPC sees that it is a no-op.
        tracing::warn!(actor = %self.state.actor_id, "shutdown RPC is a no-op");
        Ok(Response::new(proto::Empty {}))
    }
}

// ── Sync service ───────────────────────────────────────────────────────────

#[tonic::async_trait]
impl Sync for SyncService {
    type StreamStream = ReceiverStream<Result<proto::SyncMessage, Status>>;

    /// Accept a bidi sync stream from a peer.
    ///
    /// The caller must set the `x-peer-id` metadata header to its stable actor
    /// ID. Each received [`proto::SyncMessage`] is decoded by the adapter;
    /// outbound messages are sent back over the same stream.
    async fn stream(
        &self,
        request: Request<tonic::Streaming<proto::SyncMessage>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let peer_id = request
            .metadata()
            .get("x-peer-id")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::invalid_argument("missing x-peer-id metadata"))?
            .to_owned();

        // Two-stage outbound pipeline: internal code writes raw Vec<u8> onto
        // raw_tx; the adapter task below re-wraps each payload as a typed gRPC
        // message and forwards it to grpc_tx, whose receiver end is returned to
        // tonic as the wire stream back to the caller.
        let (raw_tx, raw_rx) = mpsc::channel::<Vec<u8>>(64);
        let (grpc_tx, grpc_rx) = mpsc::channel::<Result<proto::SyncMessage, Status>>(64);
        spawn_payload_forwarder(raw_rx, grpc_tx, Ok);

        // Register raw_tx so both recv_loop (protocol replies) and
        // flush_to_peers (proactive post-op push) can write to this peer.
        self.state
            .register_peer(peer_id.clone(), raw_tx.clone())
            .await;

        let state = self.state.clone();
        let inbound = request.into_inner();
        tokio::spawn(recv_loop(state, peer_id, inbound, raw_tx));

        // grpc_rx is the outbound wire stream; tonic drains it and sends each
        // message back to the calling peer over the open HTTP/2 connection.
        Ok(Response::new(ReceiverStream::new(grpc_rx)))
    }
}

// ── Sync tasks ─────────────────────────────────────────────────────────────

/// Drive an inbound sync stream: receive messages, update state, send replies.
///
/// Runs for the lifetime of the connection. On exit, deregisters the peer.
async fn recv_loop(
    state: Arc<ReplicaState>,
    peer_id: String,
    mut inbound: tonic::Streaming<proto::SyncMessage>,
    tx: mpsc::Sender<Vec<u8>>,
) {
    while let Some(result) = inbound.next().await {
        match result {
            Ok(msg) => {
                if let Err(e) = state.sync_receive(&peer_id, msg.payload) {
                    tracing::error!(peer = %peer_id, "sync_receive failed: {e:#}");
                    break;
                }
                state.record_sync_rx(&peer_id);
                // Also re-sample doc size here: the apply_op path only updates
                // the gauge when a *local* write lands, so without this a
                // replica that only receives sync messages would report the
                // pre-merge size forever. Recording on receive lets
                // post-convergence equality checks (max() by (actor)) hold.
                state.record_doc_size();
                // Immediately reply if the protocol has something to send back.
                if let Some(response) = state.sync_generate(&peer_id)
                    && tx.send(response).await.is_err()
                {
                    break;
                }
                // Relay newly received state to all other connected peers.
                // Required for non-mesh topologies (ring/line/star) to
                // converge; safe for full-mesh because Automerge's per-peer
                // sync::State quiesces once peers are caught up.
                state.flush_to_peers().await;
            }
            Err(status) => {
                tracing::warn!(peer = %peer_id, "stream error: {status}");
                break;
            }
        }
    }

    state.deregister_peer(&peer_id).await;
    tracing::info!(peer = %peer_id, "sync stream closed");
}

/// Open an outbound bidi sync stream to `peer_id` at `addr` and drive it.
///
/// Spawned by `ConnectPeer`. Signals `ready_tx` once the stream is open and
/// the peer is registered in `peer_txs`, then enters the long-running
/// [`recv_loop`]. This allows the `ConnectPeer` RPC to block until the
/// connection is genuinely usable rather than relying on a fixed sleep.
async fn connect_to_peer(
    state: Arc<ReplicaState>,
    peer_id: String,
    addr: String,
    ready_tx: tokio::sync::oneshot::Sender<()>,
) -> anyhow::Result<()> {
    let endpoint = Channel::from_shared(format!("http://{addr}"))
        .with_context(|| format!("invalid peer address '{addr}'"))?;
    let channel = endpoint.connect().await?;
    let mut client = proto::sync_client::SyncClient::new(channel);

    let (raw_tx, raw_rx) = mpsc::channel::<Vec<u8>>(64);
    let (grpc_tx, grpc_rx) = mpsc::channel::<proto::SyncMessage>(64);
    spawn_payload_forwarder(raw_rx, grpc_tx, std::convert::identity);

    let mut request = Request::new(ReceiverStream::new(grpc_rx));
    request.metadata_mut().insert(
        "x-peer-id",
        state
            .actor_id
            .parse()
            .expect("actor_id is validated as a header value in ReplicaState::new"),
    );

    let response = client.stream(request).await?;
    let inbound = response.into_inner();

    state.register_peer(peer_id.clone(), raw_tx.clone()).await;
    tracing::info!(peer = %peer_id, %addr, "sync stream established");

    // Kick off the protocol from our side before signalling ready, so the
    // Automerge handshake is already in flight when the caller proceeds.
    if let Some(msg) = state.sync_generate(&peer_id) {
        raw_tx.send(msg).await?;
    }

    // Unblock the ConnectPeer RPC — stream is open and initial message sent.
    let _ = ready_tx.send(());

    recv_loop(state, peer_id, inbound, raw_tx).await;
    Ok(())
}

/// Spawn a task that wraps each raw payload into a typed message and forwards it.
///
/// The two sync streams (server-side and client-side) use slightly different
/// outbound wire types — `Result<SyncMessage, Status>` versus `SyncMessage` —
/// so `wrap` lifts a built `SyncMessage` into whatever the caller's channel
/// expects (e.g. `Ok` for the fallible variant, `identity` for the bare one).
/// The task exits once the receiver closes or the outbound sender is dropped.
fn spawn_payload_forwarder<T, F>(
    mut raw_rx: mpsc::Receiver<Vec<u8>>,
    grpc_tx: mpsc::Sender<T>,
    wrap: F,
) where
    T: Send + 'static,
    F: Fn(proto::SyncMessage) -> T + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(payload) = raw_rx.recv().await {
            if grpc_tx
                .send(wrap(proto::SyncMessage { payload }))
                .await
                .is_err()
            {
                break;
            }
        }
    });
}
