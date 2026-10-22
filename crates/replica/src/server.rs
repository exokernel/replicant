//! The gRPC server for one replica.
//!
//! [`ReplicaState`] holds the document, the peer streams, and the metric
//! instruments. Two services share it. [`ReplicaService`] takes control calls
//! from the orchestrator. [`SyncService`] accepts sync streams from peer
//! replicas.

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

use common::NodeId;
use common::proto::{self, replica_server::Replica, sync_server::Sync};
use common::{CrdtAdapter, Op};

use crate::metrics::Metrics;

// ── Shared state ───────────────────────────────────────────────────────────

/// The document, peer streams, and metric instruments of one replica.
///
/// It is shared through an `Arc` by both gRPC services and by every
/// background task.
pub struct ReplicaState {
    /// This replica's ID. It labels metrics, and it is sent as the
    /// `x-peer-id` header on outbound sync streams.
    actor_id: NodeId,
    /// The CRDT document.
    ///
    /// A `std::sync::Mutex` is correct here because no code holds this lock
    /// across an `.await`. Keep it that way.
    adapter: Mutex<Box<dyn CrdtAdapter>>,
    /// The outbound channel for each connected peer.
    ///
    /// This is a `tokio::sync::Mutex`. No code holds it across an `.await`
    /// at present: [`Self::flush_to_peers`] and [`Self::kick_sync`] copy the
    /// senders out and release the lock before they send.
    peer_txs: tokio::sync::Mutex<HashMap<NodeId, mpsc::Sender<Vec<u8>>>>,
    /// Handles of the tasks that drive the inbound and outbound sync streams.
    ///
    /// [`Self::reset`] aborts and awaits each task before it clears the
    /// document. Without this, a `sync_receive` from an old connection could
    /// write into the new, empty document.
    peer_tasks: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
    /// Peers whose sync links are blocked. This is the partition mechanism
    /// behind the `SetPeerLinks` RPC.
    ///
    /// For a blocked peer, the replica does not generate sync messages, and
    /// it drops inbound messages without applying them. Generating a message
    /// would use up sync protocol state for a message that cannot arrive.
    /// Applying an inbound message would carry document changes across the
    /// partition.
    ///
    /// The set is keyed by peer ID, not by stream, so a peer can be blocked
    /// before its stream exists.
    ///
    /// A `std::sync::Mutex` is correct here because no code holds this lock
    /// across an `.await`.
    blocked_peers: Mutex<HashSet<NodeId>>,
    /// The OTel instruments this replica records to.
    metrics: Metrics,
}

impl ReplicaState {
    /// Creates the shared state for a replica with ID `actor_id`.
    ///
    /// The instruments come from the global OTel meter provider. Initialize
    /// the provider before you call this.
    pub fn new(actor_id: NodeId, adapter: impl CrdtAdapter) -> Arc<Self> {
        Self::from_boxed_adapter(actor_id, Box::new(adapter))
    }

    /// Same as [`Self::new`], but takes an adapter that is already boxed.
    ///
    /// The replica binary uses this when it selects the adapter at run time
    /// from the `--crdt` flag.
    pub fn from_boxed_adapter(actor_id: NodeId, adapter: Box<dyn CrdtAdapter>) -> Arc<Self> {
        let meter = opentelemetry::global::meter("replicant");
        Arc::new(Self {
            actor_id,
            adapter: Mutex::new(adapter),
            peer_txs: tokio::sync::Mutex::new(HashMap::new()),
            peer_tasks: tokio::sync::Mutex::new(Vec::new()),
            blocked_peers: Mutex::new(HashSet::new()),
            metrics: Metrics::new(&meter),
        })
    }

    /// Locks the adapter for one trait call.
    ///
    /// Do not hold the guard across an `.await`. That rule is what makes a
    /// `std::sync::Mutex` safe here.
    ///
    /// Panics if the mutex is poisoned. A poisoned mutex means a thread
    /// panicked during a write, so the document can be in a bad state.
    fn adapter(&self) -> MutexGuard<'_, Box<dyn CrdtAdapter>> {
        self.adapter.lock().expect("adapter mutex poisoned")
    }

    /// Applies `op` and returns the adapter time in milliseconds.
    ///
    /// The timer starts after the lock is acquired. `replicant.op.duration`
    /// measures CRDT work only. Time spent waiting for a concurrent
    /// `sync_receive` is harness contention, so it is not included.
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

    /// Blocks or unblocks sync traffic with `peers`. See
    /// [`Self::blocked_peers`].
    ///
    /// An unblock also resets the sync protocol state for each peer. The
    /// reason: the peer can drop a message that this replica generated just
    /// before the block. This replica's state then records data as sent that
    /// never arrived, and sync stalls. A fresh handshake removes that risk,
    /// whatever was in flight when the partition started.
    ///
    /// This method sends no sync traffic. The orchestrator calls `KickSync`
    /// after both ends of every healed link are unblocked. A kick from here
    /// could reach a peer that is still blocked, and that peer would drop it.
    fn set_peer_links(&self, peers: &[NodeId], blocked: bool) {
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
            // Release this lock before taking the adapter lock. No code holds
            // both, so there is no lock order to keep.
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

    /// Returns this replica's ID.
    pub fn actor_id(&self) -> &NodeId {
        &self.actor_id
    }

    // ── Metric recorders ───────────────────────────────────────────────────
    //
    // These methods attach the attributes that each instrument expects. Add
    // new attributes here, not at the call sites.

    fn record_op_duration(&self, op_name: &'static str, elapsed_ms: f64) {
        self.metrics.op_duration_ms.record(
            elapsed_ms,
            &[
                KeyValue::new("actor", self.actor_id.to_string()),
                KeyValue::new("op", op_name),
            ],
        );
    }

    /// Sample the document size and record it on the gauge.
    fn record_doc_size(&self) {
        self.metrics.doc_size_bytes.record(
            self.doc_size_bytes() as u64,
            &[KeyValue::new("actor", self.actor_id.to_string())],
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
            KeyValue::new("actor", self.actor_id.to_string()),
            KeyValue::new("peer", peer_id.to_owned()),
        ]
    }

    async fn register_peer(&self, peer_id: NodeId, tx: mpsc::Sender<Vec<u8>>) {
        self.peer_txs.lock().await.insert(peer_id, tx);
    }

    async fn deregister_peer(&self, peer_id: &str) {
        self.peer_txs.lock().await.remove(peer_id);
    }

    /// Stores the handle of a peer stream task, so that [`Self::reset`] can
    /// stop it.
    async fn register_peer_task(&self, handle: JoinHandle<()>) {
        self.peer_tasks.lock().await.push(handle);
    }

    /// Closes all peer connections and returns the document to its empty
    /// state.
    ///
    /// The steps must run in this order:
    /// 1. Abort every peer stream task and await it. The await makes sure
    ///    that a `sync_receive` in flight has released the adapter lock.
    /// 2. Clear `peer_txs` and the blocked set. A task that ends normally
    ///    deregisters its peer, but an aborted task does not.
    /// 3. Reset the adapter. This step is last, so it also clears any stale
    ///    `sync_receive` that landed after step 1.
    ///
    /// The caller must not call `connect_peer` on this replica while `reset`
    /// runs. A new task could be registered while step 1 drains the list.
    pub async fn reset(&self) {
        let handles: Vec<JoinHandle<()>> = self.peer_tasks.lock().await.drain(..).collect();
        for h in &handles {
            h.abort();
        }
        for h in handles {
            // An aborted task returns a `Cancelled` error, which is a normal
            // exit. Log only real panics.
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

    /// Sends any pending sync message to each connected peer.
    ///
    /// The replica calls this after every local op and after every inbound
    /// sync message, so peers get changes without delay.
    ///
    /// This method never waits for a slow peer. If a peer's channel is full,
    /// the flush to that peer is deferred. Do not add backpressure here.
    /// `recv_loop` calls this method, so two replicas with full channels
    /// would each stop reading from the other, and both would deadlock.
    async fn flush_to_peers(&self) {
        // Copy the senders in one lock acquisition. No lock is held during
        // the sends.
        let peers: Vec<(NodeId, mpsc::Sender<Vec<u8>>)> = self
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

    /// Queues one pending sync message for `peer_id`, if the link allows it.
    ///
    /// Both checks make sure that no protocol state is used up for a message
    /// that will not arrive:
    /// - If the peer is blocked, the method does nothing.
    /// - The method reserves channel space before it generates the message.
    ///   `sync_generate` records the message as sent. If the channel were
    ///   full after that, the message would be lost, the next generate would
    ///   return `None`, and the peer would never get the change. With the
    ///   space reserved first, a full channel leaves the change pending for
    ///   a later flush.
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
        // If there was nothing to send, dropping the permit frees the slot.
    }

    /// Sends a sync message to each of `peers`. This starts the handshake
    /// after a heal, and is the `KickSync` RPC.
    ///
    /// A peer with no open stream is skipped. A kick names peers, not edges,
    /// and in a sparse heal graph only the healed edges have streams.
    async fn kick_sync(&self, peers: &[NodeId]) {
        let txs = self.peer_txs.lock().await;
        let targets: Vec<(NodeId, mpsc::Sender<Vec<u8>>)> = peers
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

/// The gRPC [`Replica`] service. It handles control calls from the
/// orchestrator: apply ops, read state, connect peers, block links, and reset.
#[derive(Clone)]
pub struct ReplicaService {
    /// The replica this service controls.
    state: Arc<ReplicaState>,
}

/// The gRPC [`Sync`] service. It accepts sync streams from peer replicas.
#[derive(Clone)]
pub struct SyncService {
    /// The replica that receives the sync messages.
    state: Arc<ReplicaState>,
}

impl ReplicaService {
    /// Creates the service for the replica `state`.
    pub fn new(state: Arc<ReplicaState>) -> Self {
        Self { state }
    }
}

impl SyncService {
    /// Creates the service for the replica `state`.
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
        // Validate the ID here. A bad ID would otherwise fail later, as a bad
        // metadata header on the outbound stream.
        let peer_id = NodeId::new(peer_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let state = self.state.clone();
        // `ready_rx` resolves when the stream is open and the peer is in
        // `peer_txs`. `ConnectPeer` returns only after that, so the caller
        // can use the stream at once, with no sleep.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let task_peer_id = peer_id.clone();
        let task_state = state.clone();
        // The wrapper logs the error and makes the task return `()`. All
        // peer-task handles then have one type, and `reset()` can keep them in
        // one Vec.
        let handle = tokio::spawn(async move {
            if let Err(e) = connect_to_peer(task_state, task_peer_id.clone(), addr, ready_tx).await
            {
                tracing::error!(peer = %task_peer_id, "sync stream error: {e:#}");
            }
        });
        // Register the handle before awaiting `ready_rx`, so that a
        // concurrent `reset` can abort the task during setup. `ready_rx` then
        // returns an error, and the RPC fails cleanly.
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
        // No flush_to_peers here. Every replica makes the same bootstrap
        // change, byte for byte, so each peer has it or will make it. The
        // normal flushes after each op merge the histories anyway.
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
        let peer_ids = to_node_ids(peer_ids)?;
        self.state.set_peer_links(&peer_ids, blocked);
        Ok(Response::new(proto::Empty {}))
    }

    async fn kick_sync(
        &self,
        request: Request<proto::PeerIds>,
    ) -> Result<Response<proto::Empty>, Status> {
        let peer_ids = to_node_ids(request.into_inner().peer_ids)?;
        self.state.kick_sync(&peer_ids).await;
        Ok(Response::new(proto::Empty {}))
    }
}

/// Converts a list of peer IDs from the wire. If any ID is bad, the whole
/// request fails.
///
/// `SetPeerLinks` and `KickSync` each describe one topology change. Applying
/// only the valid part would give a state that no scenario asked for.
fn to_node_ids(ids: Vec<String>) -> Result<Vec<NodeId>, Status> {
    ids.into_iter()
        .map(|id| NodeId::new(id).map_err(|e| Status::invalid_argument(e.to_string())))
        .collect()
}

// ── Sync service ───────────────────────────────────────────────────────────

#[tonic::async_trait]
impl Sync for SyncService {
    type StreamStream = ReceiverStream<Result<proto::SyncMessage, Status>>;

    /// Accepts a bidirectional sync stream from a peer.
    ///
    /// The caller must set the `x-peer-id` metadata header to its actor ID.
    /// The adapter applies each received [`proto::SyncMessage`]. Replies go
    /// back on the same stream.
    async fn stream(
        &self,
        request: Request<tonic::Streaming<proto::SyncMessage>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let peer_id = request
            .metadata()
            .get("x-peer-id")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::invalid_argument("missing x-peer-id metadata"))?;
        // A value that arrived as a header is a legal header value, so in
        // practice this rejects only the empty string.
        let peer_id = NodeId::new(peer_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Outbound messages go through two channels. Replica code writes raw
        // bytes to raw_tx. The forwarder task wraps each payload as a gRPC
        // message and sends it to grpc_tx. Tonic reads grpc_rx as the stream
        // back to the caller.
        let (raw_tx, raw_rx) = mpsc::channel::<Vec<u8>>(64);
        let (grpc_tx, grpc_rx) = mpsc::channel::<Result<proto::SyncMessage, Status>>(64);
        spawn_payload_forwarder(raw_rx, grpc_tx, Ok);

        // Register raw_tx, so that recv_loop (replies) and flush_to_peers
        // (changes after each op) can both write to this peer.
        self.state
            .register_peer(peer_id.clone(), raw_tx.clone())
            .await;

        let state = self.state.clone();
        let inbound = request.into_inner();
        let handle = tokio::spawn(recv_loop(state.clone(), peer_id, inbound, raw_tx));
        // Register the handle, so that `reset()` stops this task too.
        state.register_peer_task(handle).await;

        // Tonic reads grpc_rx and sends each message to the calling peer.
        Ok(Response::new(ReceiverStream::new(grpc_rx)))
    }
}

// ── Sync tasks ─────────────────────────────────────────────────────────────

/// Reads a sync stream from a peer. For each message, it updates the
/// document, sends a reply, and relays the change to the other peers.
///
/// Runs until the stream closes, then deregisters the peer.
async fn recv_loop(
    state: Arc<ReplicaState>,
    peer_id: NodeId,
    mut inbound: tonic::Streaming<proto::SyncMessage>,
    tx: mpsc::Sender<Vec<u8>>,
) {
    while let Some(result) = inbound.next().await {
        match result {
            Ok(msg) => {
                // Drop the message if the link is blocked. Applying it would
                // carry changes across the partition. A reply would use up
                // sync state toward a peer that drops it. The sender now
                // believes this message arrived. The unblock resets that
                // state; see `ReplicaState::set_peer_links`.
                if state.is_blocked(&peer_id) {
                    tracing::trace!(peer = %peer_id, "link blocked; dropping inbound sync message");
                    continue;
                }
                if let Err(e) = state.sync_receive(&peer_id, msg.payload) {
                    tracing::error!(peer = %peer_id, "sync_receive failed: {e:#}");
                    break;
                }
                state.record_sync_rx(&peer_id);
                // Record the doc size here too. apply_op records it only for
                // local writes. Without this, a replica that only receives
                // changes would report its size from before the merge, and
                // the size checks after convergence would fail.
                state.record_doc_size();
                // Reply at once if the protocol has something to send back.
                if let Some(response) = state.sync_generate(&peer_id)
                    && tx.send(response).await.is_err()
                {
                    break;
                }
                // Relay the new state to all other peers. Ring, line, and
                // star topologies need this to converge. It is also safe on a
                // full mesh: once a peer is up to date, sync_generate returns
                // None and the relay stops.
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

/// Opens a sync stream to `peer_id` at `addr`, then reads it with
/// [`recv_loop`].
///
/// `ConnectPeer` spawns this task. The task signals `ready_tx` when the stream
/// is open and the peer is in `peer_txs`. `ConnectPeer` waits for that signal,
/// so it returns only when the stream is ready to use.
async fn connect_to_peer(
    state: Arc<ReplicaState>,
    peer_id: NodeId,
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
    request
        .metadata_mut()
        .insert("x-peer-id", state.actor_id.header_value());

    let response = client.stream(request).await?;
    let inbound = response.into_inner();

    state.register_peer(peer_id.clone(), raw_tx.clone()).await;
    tracing::info!(peer = %peer_id, %addr, "sync stream established");

    // Send the first sync message before signalling ready, so the handshake
    // has started when the caller continues. If the link is blocked,
    // `try_flush_peer` sends nothing. The stream stays quiet, and `KickSync`
    // starts the handshake at heal time.
    state.try_flush_peer(&peer_id, &raw_tx);

    // The stream is open, so let the ConnectPeer RPC return.
    let _ = ready_tx.send(());

    recv_loop(state, peer_id, inbound, raw_tx).await;
    Ok(())
}

/// Spawns a task that wraps each raw payload as a [`proto::SyncMessage`] and
/// forwards it to `grpc_tx`.
///
/// The server side of a stream sends `Result<SyncMessage, Status>`. The client
/// side sends `SyncMessage`. `wrap` converts to the type the channel needs:
/// `Ok` for the server side, `identity` for the client side.
///
/// The task stops when either channel closes.
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

    /// Makes a `NodeId` from a test literal. Test IDs are valid, so this
    /// cannot fail.
    fn id(s: &str) -> NodeId {
        NodeId::new(s).expect("test node ids are valid")
    }
    use crate::adapter::AutomergeAdapter;
    use common::ScalarVal;

    fn map_put(key: &str) -> Op {
        Op::MapPut {
            obj: String::new(),
            key: key.to_owned(),
            value: ScalarVal::Str("v".to_owned()),
        }
    }

    /// A full outbound channel must not lose a change.
    ///
    /// `sync_generate` records a message as sent. If the channel is full after
    /// that, the change is lost. This test fills the channel for one flush,
    /// drains it, and checks that a later flush still sends the change.
    #[tokio::test]
    async fn full_peer_channel_defers_rather_than_dropping_a_change() {
        let state = ReplicaState::new(id("node-0"), AutomergeAdapter::new());
        // Capacity 1, so a single un-drained message saturates the channel.
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1);
        state.register_peer(id("node-1"), tx).await;

        // The handshake fills the only slot. It is not drained, so the
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

    /// With room in the channel, each flush sends its pending message.
    #[tokio::test]
    async fn flush_delivers_when_the_peer_channel_has_room() {
        let state = ReplicaState::new(id("node-0"), AutomergeAdapter::new());
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
        state.register_peer(id("node-1"), tx).await;

        state.flush_to_peers().await;
        state.apply_op_timed(&map_put("k")).unwrap();
        state.flush_to_peers().await;

        // Handshake plus the post-write message.
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
    }

    /// A flush with nothing to send must release the space it reserved.
    /// Otherwise, repeated idle flushes would use up the channel.
    #[tokio::test]
    async fn idle_flush_does_not_consume_channel_capacity() {
        let state = ReplicaState::new(id("node-0"), AutomergeAdapter::new());
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(2);
        state.register_peer(id("node-1"), tx.clone()).await;

        // The first flush queues the handshake. The others have nothing new.
        state.flush_to_peers().await;
        for _ in 0..10 {
            state.flush_to_peers().await;
        }
        assert!(rx.try_recv().is_ok(), "handshake");
        assert!(
            rx.try_recv().is_err(),
            "idle flushes must not enqueue anything"
        );
        // Both slots are still free.
        assert!(tx.try_reserve().is_ok());
    }

    // ── Peer-link blocking (partition primitive) ───────────────────────────

    /// A flush sends nothing to a blocked peer, not even a handshake. An
    /// open peer on the same replica still gets its message. This tests the
    /// outbound side of a partition.
    #[tokio::test]
    async fn flush_skips_blocked_peers_and_serves_the_rest() {
        let state = ReplicaState::new(id("node-0"), AutomergeAdapter::new());
        let (tx_b, mut rx_blocked) = mpsc::channel::<Vec<u8>>(8);
        let (tx_o, mut rx_open) = mpsc::channel::<Vec<u8>>(8);
        state.register_peer(id("node-1"), tx_b).await;
        state.register_peer(id("node-2"), tx_o).await;
        state.set_peer_links(&[id("node-1")], true);

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

    /// A block must not use up sync state. After the unblock, a kick must
    /// start sending the changes the peer missed.
    #[tokio::test]
    async fn unblock_delivers_changes_made_during_the_block() {
        let state = ReplicaState::new(id("node-0"), AutomergeAdapter::new());
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
        state.register_peer(id("node-1"), tx).await;
        state.set_peer_links(&[id("node-1")], true);

        state.apply_op_timed(&map_put("during-block")).unwrap();
        state.flush_to_peers().await;
        assert!(rx.try_recv().is_err(), "nothing crosses a blocked link");

        state.set_peer_links(&[id("node-1")], false);
        state.kick_sync(&[id("node-1")]).await;
        assert!(
            rx.try_recv().is_ok(),
            "post-unblock kick must start the handshake"
        );
    }

    /// A kick to a peer with no open stream does nothing. In a sparse heal
    /// graph, not every peer has an edge.
    #[tokio::test]
    async fn kick_sync_ignores_unknown_peers() {
        let state = ReplicaState::new(id("node-0"), AutomergeAdapter::new());
        state.kick_sync(&[id("node-9")]).await;
    }

    /// A reset clears the blocked set. Each trial starts with all links open.
    #[tokio::test]
    async fn reset_clears_blocked_peers() {
        let state = ReplicaState::new(id("node-0"), AutomergeAdapter::new());
        state.set_peer_links(&[id("node-1")], true);
        assert!(state.is_blocked("node-1"));

        state.reset().await;
        assert!(!state.is_blocked("node-1"), "reset must unblock all links");
    }
}
