use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

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
    pub actor_id: String,
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
    pub fn new(actor_id: String, adapter: impl CrdtAdapter) -> Arc<Self> {
        let meter = opentelemetry::global::meter("replicant");
        Arc::new(Self {
            actor_id,
            adapter: Mutex::new(Box::new(adapter)),
            peer_txs: tokio::sync::Mutex::new(HashMap::new()),
            metrics: Metrics::new(&meter),
        })
    }

    fn apply_op(&self, op: &Op) -> anyhow::Result<()> {
        self.adapter.lock().unwrap().apply_op(op)
    }

    fn get_heads(&self) -> Vec<Vec<u8>> {
        self.adapter.lock().unwrap().get_heads()
    }

    fn state_fingerprint(&self) -> Vec<u8> {
        self.adapter.lock().unwrap().state_fingerprint()
    }

    fn doc_size_bytes(&self) -> usize {
        self.adapter.lock().unwrap().doc_size_bytes()
    }

    fn sync_generate(&self, peer: &str) -> Option<Vec<u8>> {
        self.adapter.lock().unwrap().sync_generate(peer)
    }

    fn sync_receive(&self, peer: &str, msg: Vec<u8>) -> anyhow::Result<()> {
        self.adapter.lock().unwrap().sync_receive(peer, msg)
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
        let peer_ids: Vec<String> = self.peer_txs.lock().await.keys().cloned().collect();
        for peer_id in &peer_ids {
            if let Some(msg) = self.sync_generate(peer_id) {
                // Clone sender before awaiting so we don't hold the lock.
                let tx = self.peer_txs.lock().await.get(peer_id).cloned();
                if let Some(tx) = tx
                    && tx.send(msg).await.is_ok()
                {
                    self.metrics.sync_tx.add(
                        1,
                        &[
                            KeyValue::new("actor", self.actor_id.clone()),
                            KeyValue::new("peer", peer_id.clone()),
                        ],
                    );
                }
            }
        }
    }
}

// ── Service structs ────────────────────────────────────────────────────────

/// gRPC [`Replica`] service — handles control-plane RPCs from the orchestrator
/// (apply ops, inspect state, connect peers, shutdown).
#[derive(Clone)]
pub struct ReplicaService(Arc<ReplicaState>);

/// gRPC [`Sync`] service — accepts inbound bidi sync streams from peer replicas.
#[derive(Clone)]
pub struct SyncService(Arc<ReplicaState>);

impl ReplicaService {
    pub fn new(state: Arc<ReplicaState>) -> Self {
        Self(state)
    }
}

impl SyncService {
    pub fn new(state: Arc<ReplicaState>) -> Self {
        Self(state)
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
        self.0
            .apply_op(&op)
            .map_err(|e| Status::internal(e.to_string()))?;
        let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;

        let actor = self.0.actor_id.clone();
        self.0.metrics.op_duration_ms.record(
            elapsed_ms,
            &[
                KeyValue::new("actor", actor.clone()),
                KeyValue::new("op", op.name()),
            ],
        );
        self.0.metrics.doc_size_bytes.record(
            self.0.doc_size_bytes() as u64,
            &[KeyValue::new("actor", actor)],
        );

        self.0.flush_to_peers().await;

        Ok(Response::new(proto::OpResponse {}))
    }

    async fn get_heads(
        &self,
        _: Request<proto::Empty>,
    ) -> Result<Response<proto::HeadsResponse>, Status> {
        Ok(Response::new(proto::HeadsResponse {
            heads: self.0.get_heads(),
        }))
    }

    async fn get_state_fingerprint(
        &self,
        _: Request<proto::Empty>,
    ) -> Result<Response<proto::FingerprintResponse>, Status> {
        Ok(Response::new(proto::FingerprintResponse {
            fingerprint: self.0.state_fingerprint(),
        }))
    }

    async fn connect_peer(
        &self,
        request: Request<proto::PeerRef>,
    ) -> Result<Response<proto::Empty>, Status> {
        let proto::PeerRef { peer_id, addr } = request.into_inner();
        let state = self.0.clone();
        // `ready_rx` resolves once the TCP connection and gRPC stream are open
        // and the peer is registered in `peer_txs`. Awaiting it here means the
        // orchestrator's `ConnectPeer` call only returns after the stream is
        // actually usable, removing the need for any post-connect sleep.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            if let Err(e) = connect_to_peer(state, peer_id, addr, ready_tx).await {
                tracing::error!("connect_to_peer failed: {e:#}");
            }
        });
        ready_rx.await.map_err(|_| {
            Status::internal("connect_to_peer task dropped before signalling ready")
        })?;
        Ok(Response::new(proto::Empty {}))
    }

    async fn shutdown(&self, _: Request<proto::Empty>) -> Result<Response<proto::Empty>, Status> {
        // Graceful per-replica shutdown is not yet implemented. In the current
        // in-process model the orchestrator tears everything down by dropping
        // the process, which triggers provider.shutdown() and flushes OTel.
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

        let (raw_tx, mut raw_rx) = mpsc::channel::<Vec<u8>>(64);
        let (grpc_tx, grpc_rx) = mpsc::channel::<Result<proto::SyncMessage, Status>>(64);

        // Adaptor: raw bytes → typed outbound stream items.
        tokio::spawn(async move {
            while let Some(payload) = raw_rx.recv().await {
                if grpc_tx
                    .send(Ok(proto::SyncMessage { payload }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        self.0.register_peer(peer_id.clone(), raw_tx.clone()).await;

        let state = self.0.clone();
        let inbound = request.into_inner();
        tokio::spawn(recv_loop(state, peer_id, inbound, raw_tx));

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
                state.metrics.sync_rx.add(
                    1,
                    &[
                        KeyValue::new("actor", state.actor_id.clone()),
                        KeyValue::new("peer", peer_id.clone()),
                    ],
                );
                // Immediately reply if the protocol has something to send back.
                if let Some(response) = state.sync_generate(&peer_id)
                    && tx.send(response).await.is_err()
                {
                    break;
                }
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
        .map_err(|e| anyhow::anyhow!("invalid peer address '{addr}': {e}"))?;
    let channel = endpoint.connect().await?;
    let mut client = proto::sync_client::SyncClient::new(channel);

    let (raw_tx, mut raw_rx) = mpsc::channel::<Vec<u8>>(64);
    let (grpc_tx, grpc_rx) = mpsc::channel::<proto::SyncMessage>(64);

    // Adaptor: raw bytes → client outbound stream items.
    tokio::spawn(async move {
        while let Some(payload) = raw_rx.recv().await {
            if grpc_tx.send(proto::SyncMessage { payload }).await.is_err() {
                break;
            }
        }
    });

    let mut request = Request::new(ReceiverStream::new(grpc_rx));
    request.metadata_mut().insert(
        "x-peer-id",
        state
            .actor_id
            .parse()
            .expect("actor_id must be a valid ASCII header value"),
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
