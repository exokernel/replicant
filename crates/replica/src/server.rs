use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use anyhow::Context as _;
use opentelemetry::KeyValue;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
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
    /// Join handles for the inbound and outbound sync-stream driver tasks.
    ///
    /// Tracked so [`Self::reset`] can `abort` + `await` each task before
    /// wiping the adapter, ruling out a race where an in-flight `sync_receive`
    /// from an old peer connection pollutes the freshly-reset document.
    peer_tasks: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
    /// Peers whose sync links are administratively blocked — the app-layer
    /// partition primitive behind the `SetPeerLinks` RPC.
    ///
    /// While a peer is here, no sync message is generated toward it (which
    /// would consume protocol state for a message that cannot be delivered)
    /// and inbound messages from it are dropped unprocessed (which would leak
    /// document state across the simulated partition). Keyed by stable peer
    /// ID rather than stream so a peer can be blocked before its stream even
    /// exists.
    ///
    /// `std::sync::Mutex`: held only for set membership operations, never
    /// across an `.await`.
    blocked_peers: Mutex<HashSet<String>>,
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
            peer_tasks: tokio::sync::Mutex::new(Vec::new()),
            blocked_peers: Mutex::new(HashSet::new()),
            metrics: Metrics::new(&meter),
        })
    }

    /// Lock the adapter for the duration of one trait call.
    ///
    /// The guard is never held across an `.await` point — that is what makes a
    /// `std::sync::Mutex` sound here. Panics if the mutex is poisoned (another
    /// thread panicked while holding it, so the document is no longer
    /// trustworthy).
    fn adapter(&self) -> MutexGuard<'_, Box<dyn CrdtAdapter>> {
        self.adapter.lock().expect("adapter mutex poisoned")
    }

    /// Apply `op` and return how long the adapter took, in fractional ms.
    ///
    /// The timer starts *after* the lock is acquired: `replicant.op.duration`
    /// is meant to measure CRDT work, and including lock acquisition would fold
    /// in waiting on a concurrent `sync_receive` — contention in the harness,
    /// not cost in the CRDT under test.
    fn apply_op_timed(&self, op: &Op) -> anyhow::Result<f64> {
        let mut adapter = self.adapter();
        let t0 = Instant::now();
        adapter.apply_op(op)?;
        Ok(t0.elapsed().as_secs_f64() * 1000.0)
    }

    fn get_heads(&self) -> Vec<Vec<u8>> {
        self.adapter().get_heads()
    }

    fn state_fingerprint(&self) -> Vec<u8> {
        self.adapter().state_fingerprint()
    }

    fn doc_size_bytes(&self) -> usize {
        self.adapter().doc_size_bytes()
    }

    fn sync_generate(&self, peer: &str) -> Option<Vec<u8>> {
        self.adapter().sync_generate(peer)
    }

    fn sync_receive(&self, peer: &str, msg: Vec<u8>) -> anyhow::Result<()> {
        self.adapter().sync_receive(peer, msg)
    }

    fn ensure_text(&self, obj: &str) -> anyhow::Result<()> {
        self.adapter().ensure_text(obj)
    }

    fn text_length(&self, obj: &str) -> anyhow::Result<usize> {
        self.adapter().text_length(obj)
    }

    fn is_blocked(&self, peer: &str) -> bool {
        self.blocked_peers
            .lock()
            .expect("blocked_peers mutex poisoned")
            .contains(peer)
    }

    /// Block or unblock sync traffic with `peers` (see [`Self::blocked_peers`]).
    ///
    /// Unblocking also discards the per-peer sync protocol state: a message
    /// generated just before the block engaged may have been dropped by the
    /// receiver, leaving this side's `sync::State` believing data was
    /// delivered that never was — the exact stall mode the flush-permit fix
    /// guards against elsewhere. Restarting from a fresh handshake makes the
    /// heal correct regardless of what was in flight when the partition began.
    ///
    /// Deliberately generates no sync traffic. The orchestrator kicks the
    /// heal handshake separately (`KickSync`) once BOTH endpoints of every
    /// healed link are unblocked; kicking from here would race the peer's own
    /// unblock and the kick would be dropped as still-blocked inbound.
    fn set_peer_links(&self, peers: &[String], blocked: bool) {
        {
            let mut set = self
                .blocked_peers
                .lock()
                .expect("blocked_peers mutex poisoned");
            for peer in peers {
                if blocked {
                    set.insert(peer.clone());
                } else {
                    set.remove(peer);
                }
            }
            // Scope ends: never hold this lock while taking the adapter lock,
            // so there is no ordering to get wrong elsewhere.
        }
        if !blocked {
            let mut adapter = self.adapter();
            for peer in peers {
                adapter.sync_reset(peer);
            }
        }
        tracing::info!(
            actor = %self.actor_id,
            ?peers,
            blocked,
            "peer links updated"
        );
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

    fn record_sync_deferred(&self, peer_id: &str) {
        self.metrics.sync_deferred.add(1, &self.peer_attrs(peer_id));
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

    /// Store the join handle for a peer driver task so [`Self::reset`] can
    /// shut it down deterministically.
    async fn register_peer_task(&self, handle: JoinHandle<()>) {
        self.peer_tasks.lock().await.push(handle);
    }

    /// Drop all peer connections and wipe the document back to its initial
    /// empty state.
    ///
    /// Steps, in order:
    /// 1. Abort every registered peer driver task and await its completion.
    ///    Awaiting matters: it guarantees that any `sync_receive` already in
    ///    flight has returned the adapter mutex before we touch it.
    /// 2. Clear `peer_txs`. The abort'd tasks' cleanup tails already call
    ///    `deregister_peer`, so this is idempotent; it covers the case where
    ///    a task was cancelled before reaching that tail.
    /// 3. Reset the adapter. Doing this last means any stale `sync_receive`
    ///    that did land between abort and adapter-lock acquisition is wiped
    ///    by the subsequent `reset` call.
    ///
    /// Callers (the orchestrator) must serialize `reset` with `connect_peer`
    /// on a given replica — concurrent `connect_peer` during reset would race
    /// against the peer-task-list drain.
    pub async fn reset(&self) {
        let handles: Vec<JoinHandle<()>> = self.peer_tasks.lock().await.drain(..).collect();
        for h in &handles {
            h.abort();
        }
        for h in handles {
            // Aborted tasks resolve with a `Cancelled` JoinError; treat that
            // as a normal exit. Real panics still land in the warn branch.
            if let Err(e) = h.await
                && !e.is_cancelled()
            {
                tracing::warn!(actor = %self.actor_id, "peer task panicked during reset: {e}");
            }
        }
        self.peer_txs.lock().await.clear();
        self.blocked_peers
            .lock()
            .expect("blocked_peers mutex poisoned")
            .clear();
        self.adapter.lock().expect("adapter mutex poisoned").reset();
    }

    /// Generate and push any pending sync messages to all connected peers.
    ///
    /// Called immediately after every local op so peers hear about changes
    /// without waiting for the next inbound message.
    ///
    /// Never blocks on a slow peer: a full outbound channel defers the flush
    /// instead of applying backpressure. Backpressure here would deadlock —
    /// this is called from `recv_loop`, so two replicas whose channels are both
    /// full would each stop draining the other's inbound stream.
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
            self.try_flush_peer(peer_id, tx);
        }
    }

    /// Generate and enqueue one pending sync message for `peer_id`, if the
    /// link permits it.
    ///
    /// Two guards, both about never consuming protocol state for a message
    /// that will not arrive:
    /// - A blocked peer is skipped entirely — generating toward it would both
    ///   leak state across a simulated partition and strand the protocol.
    /// - Capacity is reserved BEFORE generating. `sync_generate` advances the
    ///   per-peer `sync::State` to "these heads have been sent", so a message
    ///   generated and then dropped on a full channel is lost from the
    ///   protocol's point of view: the next generate returns `None` and the
    ///   peer never learns about the change. Holding a permit first leaves
    ///   the change pending for a later flush instead.
    fn try_flush_peer(&self, peer_id: &str, tx: &mpsc::Sender<Vec<u8>>) {
        if self.is_blocked(peer_id) {
            tracing::trace!(peer = %peer_id, "link blocked; skipping sync flush");
            return;
        }
        let Ok(permit) = tx.try_reserve() else {
            self.record_sync_deferred(peer_id);
            tracing::debug!(peer = %peer_id, "outbound channel full; deferring sync flush");
            return;
        };
        if let Some(msg) = self.sync_generate(peer_id) {
            permit.send(msg);
            self.record_sync_tx(peer_id);
        }
        // Nothing to send: dropping the permit returns the slot.
    }

    /// Generate and send a fresh sync message to each of `peers` — the
    /// heal-phase handshake starter behind the `KickSync` RPC.
    ///
    /// Peers without an open stream are skipped silently: on non-mesh heal
    /// graphs only the healed edges have streams, and a kick names peers, not
    /// edges.
    async fn kick_sync(&self, peers: &[String]) {
        let txs = self.peer_txs.lock().await;
        let targets: Vec<(String, mpsc::Sender<Vec<u8>>)> = peers
            .iter()
            .filter_map(|p| txs.get(p).map(|tx| (p.clone(), tx.clone())))
            .collect();
        drop(txs);
        for (peer_id, tx) in &targets {
            self.try_flush_peer(peer_id, tx);
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

        let elapsed_ms = self
            .state
            .apply_op_timed(&op)
            .map_err(|e| Status::internal(e.to_string()))?;

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
        let task_peer_id = peer_id.clone();
        let task_state = state.clone();
        // Wrap the driver so its `Result` is logged and the spawned task's
        // output type is `()` — that lets `reset()` store every peer-task
        // handle in a single homogeneous Vec and abort/await them uniformly.
        let handle = tokio::spawn(async move {
            if let Err(e) = connect_to_peer(task_state, task_peer_id.clone(), addr, ready_tx).await
            {
                tracing::error!(peer = %task_peer_id, "sync stream error: {e:#}");
            }
        });
        // Register the handle BEFORE awaiting `ready_rx`. If a concurrent
        // `reset` aborts the task during setup, `ready_rx` resolves with an
        // error, which we convert to a clean RPC failure.
        state.register_peer_task(handle).await;
        ready_rx.await.map_err(|_| {
            Status::internal("connect_to_peer task dropped before signalling ready")
        })?;
        Ok(Response::new(proto::Empty {}))
    }

    async fn reset(&self, _: Request<proto::Empty>) -> Result<Response<proto::Empty>, Status> {
        self.state.reset().await;
        Ok(Response::new(proto::Empty {}))
    }

    async fn ensure_text(
        &self,
        request: Request<proto::ObjRef>,
    ) -> Result<Response<proto::Empty>, Status> {
        let obj = request.into_inner().obj;
        self.state
            .ensure_text(&obj)
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        // No flush_to_peers: the bootstrap change is bit-identical on every
        // replica, so peers either already have it or will author it
        // themselves; regular post-op flushes reconcile the DAG regardless.
        Ok(Response::new(proto::Empty {}))
    }

    async fn get_text_length(
        &self,
        request: Request<proto::ObjRef>,
    ) -> Result<Response<proto::TextLengthResponse>, Status> {
        let obj = request.into_inner().obj;
        let length = self
            .state
            .text_length(&obj)
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Response::new(proto::TextLengthResponse {
            length: length as u64,
        }))
    }

    async fn set_peer_links(
        &self,
        request: Request<proto::PeerLinkUpdate>,
    ) -> Result<Response<proto::Empty>, Status> {
        let proto::PeerLinkUpdate { peer_ids, blocked } = request.into_inner();
        self.state.set_peer_links(&peer_ids, blocked);
        Ok(Response::new(proto::Empty {}))
    }

    async fn kick_sync(
        &self,
        request: Request<proto::PeerIds>,
    ) -> Result<Response<proto::Empty>, Status> {
        let peer_ids = request.into_inner().peer_ids;
        self.state.kick_sync(&peer_ids).await;
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
        let handle = tokio::spawn(recv_loop(state.clone(), peer_id, inbound, raw_tx));
        // Track the handle so `reset()` can shut this driver down with the
        // outbound peers' drivers uniformly.
        state.register_peer_task(handle).await;

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
                // A blocked link drops inbound traffic unprocessed. Applying
                // it would carry document changes across the simulated
                // partition; even generating a reply would consume sync
                // protocol state toward a peer that will drop it. The sender's
                // own stale state (it believes this message arrived) is
                // discarded when the link is unblocked — see
                // `ReplicaState::set_peer_links`.
                if state.is_blocked(&peer_id) {
                    tracing::trace!(peer = %peer_id, "link blocked; dropping inbound sync message");
                    continue;
                }
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
    // `try_flush_peer` skips the kick when the link is blocked — a stream
    // wired during a simulated partition opens silently, and the heal-phase
    // `KickSync` starts the handshake later.
    state.try_flush_peer(&peer_id, &raw_tx);

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

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::AutomergeAdapter;
    use common::ScalarVal;

    fn map_put(key: &str) -> Op {
        Op::MapPut {
            obj: String::new(),
            key: key.to_owned(),
            value: ScalarVal::Str("v".to_owned()),
        }
    }

    /// A momentarily-full outbound channel must not consume the change.
    ///
    /// `sync_generate` advances the per-peer `sync::State`, so generating a
    /// message and then failing to enqueue it used to lose it outright: the
    /// next generate returned `None` because the state already believed those
    /// heads had been sent, and the peer never heard about the write until some
    /// unrelated traffic perturbed the state. Here the peer's channel is
    /// saturated across one flush; once drained, a later flush must still
    /// deliver the pending change.
    #[tokio::test]
    async fn full_peer_channel_defers_rather_than_dropping_a_change() {
        let state = ReplicaState::new("node-0".to_owned(), AutomergeAdapter::new());
        // Capacity 1, so a single un-drained message saturates the channel.
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1);
        state.register_peer("node-1".to_owned(), tx).await;

        // Fills the only slot with the sync handshake — left undrained, so the
        // channel is full for the next flush.
        state.flush_to_peers().await;

        // The write's flush finds no room. The change must stay pending.
        state.apply_op_timed(&map_put("k")).unwrap();
        state.flush_to_peers().await;

        // Drain everything queued, then flush once more. The write is still
        // owed to the peer, so this must produce a message.
        while rx.try_recv().is_ok() {}
        state.flush_to_peers().await;
        assert!(
            rx.try_recv().is_ok(),
            "change was lost while the peer channel was momentarily full"
        );
    }

    /// The happy path still sends on every flush that has something to say.
    #[tokio::test]
    async fn flush_delivers_when_the_peer_channel_has_room() {
        let state = ReplicaState::new("node-0".to_owned(), AutomergeAdapter::new());
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
        state.register_peer("node-1".to_owned(), tx).await;

        state.flush_to_peers().await;
        state.apply_op_timed(&map_put("k")).unwrap();
        state.flush_to_peers().await;

        // Handshake plus the post-write message.
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
    }

    /// A flush with nothing to send must release the permit it reserved,
    /// otherwise repeated no-op flushes would drain the channel's capacity.
    #[tokio::test]
    async fn idle_flush_does_not_consume_channel_capacity() {
        let state = ReplicaState::new("node-0".to_owned(), AutomergeAdapter::new());
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(2);
        state.register_peer("node-1".to_owned(), tx.clone()).await;

        // First flush queues the handshake; subsequent ones have nothing new.
        state.flush_to_peers().await;
        for _ in 0..10 {
            state.flush_to_peers().await;
        }
        assert!(rx.try_recv().is_ok(), "handshake");
        assert!(
            rx.try_recv().is_err(),
            "idle flushes must not enqueue anything"
        );
        // Capacity is intact: both slots are still usable.
        assert!(tx.try_reserve().is_ok());
    }

    // ── Peer-link blocking (partition primitive) ───────────────────────────

    /// A blocked peer must receive nothing from a flush — not even a
    /// handshake — while an unblocked peer on the same replica still does.
    /// This is the outbound half of the partition primitive.
    #[tokio::test]
    async fn flush_skips_blocked_peers_and_serves_the_rest() {
        let state = ReplicaState::new("node-0".to_owned(), AutomergeAdapter::new());
        let (tx_b, mut rx_blocked) = mpsc::channel::<Vec<u8>>(8);
        let (tx_o, mut rx_open) = mpsc::channel::<Vec<u8>>(8);
        state.register_peer("node-1".to_owned(), tx_b).await;
        state.register_peer("node-2".to_owned(), tx_o).await;
        state.set_peer_links(&["node-1".to_owned()], true);

        state.apply_op_timed(&map_put("k")).unwrap();
        state.flush_to_peers().await;

        assert!(
            rx_blocked.try_recv().is_err(),
            "blocked peer must receive nothing"
        );
        assert!(
            rx_open.try_recv().is_ok(),
            "open peer must still be flushed"
        );
    }

    /// Blocking must not consume sync protocol state: after unblocking, a
    /// flush must deliver everything the peer missed. The unblock resets the
    /// per-peer state, so this holds even if the block raced an in-flight
    /// generate.
    #[tokio::test]
    async fn unblock_delivers_changes_made_during_the_block() {
        let state = ReplicaState::new("node-0".to_owned(), AutomergeAdapter::new());
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
        state.register_peer("node-1".to_owned(), tx).await;
        state.set_peer_links(&["node-1".to_owned()], true);

        state.apply_op_timed(&map_put("during-block")).unwrap();
        state.flush_to_peers().await;
        assert!(rx.try_recv().is_err(), "nothing crosses a blocked link");

        state.set_peer_links(&["node-1".to_owned()], false);
        state.kick_sync(&["node-1".to_owned()]).await;
        assert!(
            rx.try_recv().is_ok(),
            "post-unblock kick must start the handshake"
        );
    }

    /// Kicking a peer with no open stream must be a silent no-op — kicks name
    /// peers, and on sparse heal graphs not every peer has an edge.
    #[tokio::test]
    async fn kick_sync_ignores_unknown_peers() {
        let state = ReplicaState::new("node-0".to_owned(), AutomergeAdapter::new());
        state.kick_sync(&["node-9".to_owned()]).await;
    }

    /// Trial reset must clear the blocked set: a fresh trial's links start
    /// open, whatever the previous scenario left behind.
    #[tokio::test]
    async fn reset_clears_blocked_peers() {
        let state = ReplicaState::new("node-0".to_owned(), AutomergeAdapter::new());
        state.set_peer_links(&["node-1".to_owned()], true);
        assert!(state.is_blocked("node-1"));

        state.reset().await;
        assert!(!state.is_blocked("node-1"), "reset must unblock all links");
    }
}
