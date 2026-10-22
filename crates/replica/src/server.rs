//! The gRPC server for one replica.
//!
//! [`ReplicaState`] holds the document, the peer streams, and the metric
//! instruments. [`router`] serves it over gRPC with two services: `Replica`
//! takes control calls from the orchestrator, and `Sync` accepts sync streams
//! from peer replicas. The `peer` submodule holds the sync stream code for
//! both directions.

mod peer;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use opentelemetry::KeyValue;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::transport::server::Router;
use tonic::{Request, Response, Status};

use common::NodeId;
use common::proto::{
    self,
    replica_server::{Replica, ReplicaServer},
    sync_server::SyncServer,
};
use common::{CrdtAdapter, Op};

use crate::metrics::Metrics;

// ── Shared state ───────────────────────────────────────────────────────────

/// The document, peer streams, and metric instruments of one replica.
///
/// It is shared through an `Arc` by both gRPC services and by every
/// background task.
///
/// All locks are `std::sync::Mutex`. No code holds a guard across an
/// `.await`. Keep it that way. The guards are not `Send`, so the compiler
/// rejects a guard held across an `.await` in a tonic handler or a spawned
/// task.
pub struct ReplicaState {
    /// This replica's ID. It is sent as the `x-peer-id` header on outbound
    /// sync streams.
    actor_id: NodeId,
    /// The `actor` metric attribute. It is built once, so that recording a
    /// metric does not allocate.
    actor_attr: KeyValue,
    /// The CRDT document.
    adapter: Mutex<Box<dyn CrdtAdapter>>,
    /// The outbound channel for each connected peer.
    peer_txs: Mutex<HashMap<NodeId, mpsc::Sender<Vec<u8>>>>,
    /// Handles of the tasks that drive the inbound and outbound sync streams.
    ///
    /// [`Self::reset`] aborts and awaits each task before it clears the
    /// document. Without this, a `sync_receive` from an old connection could
    /// write into the new, empty document.
    peer_tasks: Mutex<Vec<JoinHandle<()>>>,
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
    blocked_peers: Mutex<HashSet<NodeId>>,
    /// The OTel instruments this replica records to.
    metrics: Metrics,
}

impl ReplicaState {
    /// Creates the shared state for a replica with ID `actor_id`.
    ///
    /// The instruments come from the global OTel meter provider. Initialize
    /// the provider before you call this.
    ///
    /// [`Crdt::build`](crate::adapter::Crdt::build) returns the boxed adapter.
    pub fn new(actor_id: NodeId, adapter: Box<dyn CrdtAdapter>) -> Arc<Self> {
        let meter = opentelemetry::global::meter("replicant");
        Arc::new(Self {
            actor_attr: KeyValue::new("actor", Arc::<str>::from(&actor_id)),
            actor_id,
            adapter: Mutex::new(adapter),
            peer_txs: Mutex::new(HashMap::new()),
            peer_tasks: Mutex::new(Vec::new()),
            blocked_peers: Mutex::new(HashSet::new()),
            metrics: Metrics::new(&meter),
        })
    }

    // ── Lock helpers ───────────────────────────────────────────────────────
    //
    // Each helper panics if its mutex is poisoned. A poisoned mutex means a
    // thread panicked while it held the lock, so the data can be in a bad
    // state.

    /// Locks the adapter for one trait call.
    fn adapter(&self) -> MutexGuard<'_, Box<dyn CrdtAdapter>> {
        self.adapter.lock().expect("adapter mutex poisoned")
    }

    fn peer_txs(&self) -> MutexGuard<'_, HashMap<NodeId, mpsc::Sender<Vec<u8>>>> {
        self.peer_txs.lock().expect("peer_txs mutex poisoned")
    }

    fn peer_tasks(&self) -> MutexGuard<'_, Vec<JoinHandle<()>>> {
        self.peer_tasks.lock().expect("peer_tasks mutex poisoned")
    }

    fn blocked(&self) -> MutexGuard<'_, HashSet<NodeId>> {
        self.blocked_peers
            .lock()
            .expect("blocked_peers mutex poisoned")
    }

    // ── Adapter calls ──────────────────────────────────────────────────────

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

    // ── Peer links ─────────────────────────────────────────────────────────

    fn is_blocked(&self, peer: &str) -> bool {
        self.blocked().contains(peer)
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
            let mut set = self.blocked();
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

    // ── Metric recorders ───────────────────────────────────────────────────
    //
    // These methods attach the attributes that each instrument expects. Add
    // new attributes here, not at the call sites.

    fn record_op_duration(&self, op_name: &'static str, elapsed_ms: f64) {
        self.metrics.op_duration_ms.record(
            elapsed_ms,
            &[self.actor_attr.clone(), KeyValue::new("op", op_name)],
        );
    }

    /// Samples the document size and records it on the gauge.
    fn record_doc_size(&self) {
        self.metrics.doc_size_bytes.record(
            self.doc_size_bytes() as u64,
            std::slice::from_ref(&self.actor_attr),
        );
    }

    fn record_sync_tx(&self, peer_id: &NodeId) {
        self.metrics.sync_tx.add(1, &self.peer_attrs(peer_id));
    }

    fn record_sync_rx(&self, peer_id: &NodeId) {
        self.metrics.sync_rx.add(1, &self.peer_attrs(peer_id));
    }

    fn record_sync_deferred(&self, peer_id: &NodeId) {
        self.metrics.sync_deferred.add(1, &self.peer_attrs(peer_id));
    }

    /// Returns the `actor` and `peer` attributes. Both share their strings
    /// with existing values, so this does not allocate.
    fn peer_attrs(&self, peer_id: &NodeId) -> [KeyValue; 2] {
        [
            self.actor_attr.clone(),
            KeyValue::new("peer", Arc::<str>::from(peer_id)),
        ]
    }

    // ── Peer registry ──────────────────────────────────────────────────────

    fn register_peer(&self, peer_id: NodeId, tx: mpsc::Sender<Vec<u8>>) {
        self.peer_txs().insert(peer_id, tx);
    }

    fn deregister_peer(&self, peer_id: &str) {
        self.peer_txs().remove(peer_id);
    }

    /// Stores the handle of a peer stream task, so that [`Self::reset`] can
    /// stop it.
    fn register_peer_task(&self, handle: JoinHandle<()>) {
        self.peer_tasks().push(handle);
    }

    /// Closes all peer connections and returns the document to its empty
    /// state.
    ///
    /// The steps must run in this order:
    /// 1. Close the peer streams with [`Self::close_peer_streams`]. Its
    ///    await makes sure that a `sync_receive` in flight has released the
    ///    adapter lock.
    /// 2. Clear the blocked set.
    /// 3. Reset the adapter. This step is last, so it also clears any stale
    ///    `sync_receive` that landed after step 1.
    ///
    /// The caller must not call `connect_peer` on this replica while `reset`
    /// runs. A new task could be registered while step 1 drains the list.
    pub(crate) async fn reset(&self) {
        self.close_peer_streams().await;
        self.blocked().clear();
        self.adapter().reset();
    }

    /// Stops every peer stream task, then forgets all peers. Does not change
    /// the document.
    ///
    /// Call this before a graceful server shutdown. The shutdown waits until
    /// every open stream ends, and an inbound sync stream otherwise ends only
    /// when the peer closes it.
    ///
    /// The method aborts each task and awaits it. Then it clears `peer_txs`.
    /// A task that ends normally deregisters its peer, but an aborted task
    /// does not.
    pub async fn close_peer_streams(&self) {
        let handles: Vec<JoinHandle<()>> = self.peer_tasks().drain(..).collect();
        for h in &handles {
            h.abort();
        }
        for h in handles {
            // An aborted task returns a `Cancelled` error, which is a normal
            // exit. Log only real panics.
            if let Err(e) = h.await
                && !e.is_cancelled()
            {
                tracing::warn!(actor = %self.actor_id, "peer task panicked during shutdown: {e}");
            }
        }
        self.peer_txs().clear();
    }

    // ── Outbound sync ──────────────────────────────────────────────────────

    /// Sends any pending sync message to each connected peer.
    ///
    /// The replica calls this after every local op and after every inbound
    /// sync message, so peers get changes without delay.
    ///
    /// This method never waits for a slow peer. If a peer's channel is full,
    /// the flush to that peer is deferred. Do not add backpressure here.
    /// `recv_loop` calls this method, so two replicas with full channels
    /// would each stop reading from the other, and both would deadlock.
    fn flush_to_peers(&self) {
        // Copy the senders under the lock, then send with no lock held.
        let targets: Vec<(NodeId, mpsc::Sender<Vec<u8>>)> = self
            .peer_txs()
            .iter()
            .map(|(p, tx)| (p.clone(), tx.clone()))
            .collect();
        for (peer_id, tx) in &targets {
            self.try_flush_peer(peer_id, tx);
        }
    }

    /// Sends a sync message to each of `peers`. This starts the handshake
    /// after a heal, and is the `KickSync` RPC.
    ///
    /// A peer with no open stream is skipped. A kick names peers, not edges,
    /// and in a sparse heal graph only the healed edges have streams.
    fn kick_sync(&self, peers: &[NodeId]) {
        // Copy the senders under the lock, then send with no lock held.
        let targets: Vec<(NodeId, mpsc::Sender<Vec<u8>>)> = {
            let txs = self.peer_txs();
            peers
                .iter()
                .filter_map(|p| Some((p.clone(), txs.get(p)?.clone())))
                .collect()
        };
        for (peer_id, tx) in &targets {
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
    fn try_flush_peer(&self, peer_id: &NodeId, tx: &mpsc::Sender<Vec<u8>>) {
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
}

// ── Service structs ────────────────────────────────────────────────────────

/// Builds a gRPC server that serves the replica `state`.
///
/// The server has two services. [`Replica`] handles control calls from the
/// orchestrator: apply ops, read state, connect peers, block links, and reset.
/// [`Sync`](proto::sync_server::Sync) accepts sync streams from peer replicas.
/// The caller picks the transport, for example with `serve_with_shutdown`.
pub fn router(state: Arc<ReplicaState>) -> Router {
    let service = ReplicaService { state };
    Server::builder()
        .add_service(ReplicaServer::new(service.clone()))
        .add_service(SyncServer::new(service))
}

/// Implements both gRPC services for one replica. This file implements
/// [`Replica`]; the `peer` submodule implements `Sync`.
#[derive(Clone)]
struct ReplicaService {
    /// The replica that this service serves.
    state: Arc<ReplicaState>,
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

        self.state.flush_to_peers();

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

    /// Opens a sync stream to a peer, and returns when the stream is ready.
    ///
    /// If the connection fails, the call returns `UNAVAILABLE` with the cause.
    async fn connect_peer(
        &self,
        request: Request<proto::PeerRef>,
    ) -> Result<Response<proto::Empty>, Status> {
        let proto::PeerRef { peer_id, addr } = request.into_inner();
        // Validate the ID here. A bad ID would otherwise fail later, as a bad
        // metadata header on the outbound stream.
        let peer_id = NodeId::new(peer_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        // `ready_rx` gets the setup result. On success, the stream is open and
        // the peer is in `peer_txs`, so the caller can use the stream at once,
        // with no sleep.
        let (ready_tx, ready_rx) = oneshot::channel();
        let handle = tokio::spawn(peer::connect_and_run(
            self.state.clone(),
            peer_id,
            addr,
            ready_tx,
        ));
        // Register the handle before awaiting `ready_rx`, so that a
        // concurrent `reset` can abort the task during setup. `ready_rx` then
        // returns an error, and the RPC fails cleanly.
        self.state.register_peer_task(handle);
        match ready_rx.await {
            Ok(Ok(())) => Ok(Response::new(proto::Empty {})),
            Ok(Err(cause)) => Err(Status::unavailable(cause)),
            Err(_) => Err(Status::internal(
                "peer connection task ended before it was ready",
            )),
        }
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
        self.state.kick_sync(&peer_ids);
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

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::AutomergeAdapter;
    use common::ScalarVal;

    /// Makes a `NodeId` from a test literal. Test IDs are valid, so this
    /// cannot fail.
    /// Returns the state of a replica `node-0` that uses Automerge.
    fn new_state() -> Arc<ReplicaState> {
        ReplicaState::new(id("node-0"), Box::new(AutomergeAdapter::new()))
    }

    fn id(s: &str) -> NodeId {
        NodeId::new(s).expect("test node ids are valid")
    }

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
    #[test]
    fn full_peer_channel_defers_rather_than_dropping_a_change() {
        let state = new_state();
        // Capacity 1, so a single un-drained message saturates the channel.
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1);
        state.register_peer(id("node-1"), tx);

        // The handshake fills the only slot. It is not drained, so the
        // channel is full for the next flush.
        state.flush_to_peers();

        // The write's flush finds no room. The change must stay pending.
        state.apply_op_timed(&map_put("k")).unwrap();
        state.flush_to_peers();

        // Drain everything queued, then flush once more. The write is still
        // owed to the peer, so this must produce a message.
        while rx.try_recv().is_ok() {}
        state.flush_to_peers();
        assert!(
            rx.try_recv().is_ok(),
            "change was lost while the peer channel was momentarily full"
        );
    }

    /// With room in the channel, each flush sends its pending message.
    #[test]
    fn flush_delivers_when_the_peer_channel_has_room() {
        let state = new_state();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
        state.register_peer(id("node-1"), tx);

        state.flush_to_peers();
        state.apply_op_timed(&map_put("k")).unwrap();
        state.flush_to_peers();

        // Handshake plus the post-write message.
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
    }

    /// A flush with nothing to send must release the space it reserved.
    /// Otherwise, repeated idle flushes would use up the channel.
    #[test]
    fn idle_flush_does_not_consume_channel_capacity() {
        let state = new_state();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(2);
        state.register_peer(id("node-1"), tx.clone());

        // The first flush queues the handshake. The others have nothing new.
        state.flush_to_peers();
        for _ in 0..10 {
            state.flush_to_peers();
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
    #[test]
    fn flush_skips_blocked_peers_and_serves_the_rest() {
        let state = new_state();
        let (tx_b, mut rx_blocked) = mpsc::channel::<Vec<u8>>(8);
        let (tx_o, mut rx_open) = mpsc::channel::<Vec<u8>>(8);
        state.register_peer(id("node-1"), tx_b);
        state.register_peer(id("node-2"), tx_o);
        state.set_peer_links(&[id("node-1")], true);

        state.apply_op_timed(&map_put("k")).unwrap();
        state.flush_to_peers();

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
    #[test]
    fn unblock_delivers_changes_made_during_the_block() {
        let state = new_state();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
        state.register_peer(id("node-1"), tx);
        state.set_peer_links(&[id("node-1")], true);

        state.apply_op_timed(&map_put("during-block")).unwrap();
        state.flush_to_peers();
        assert!(rx.try_recv().is_err(), "nothing crosses a blocked link");

        state.set_peer_links(&[id("node-1")], false);
        state.kick_sync(&[id("node-1")]);
        assert!(
            rx.try_recv().is_ok(),
            "post-unblock kick must start the handshake"
        );
    }

    /// A kick to a peer with no open stream does nothing. In a sparse heal
    /// graph, not every peer has an edge.
    #[test]
    fn kick_sync_ignores_unknown_peers() {
        let state = new_state();
        state.kick_sync(&[id("node-9")]);
    }

    /// A reset clears the blocked set. Each trial starts with all links open.
    #[tokio::test]
    async fn reset_clears_blocked_peers() {
        let state = new_state();
        state.set_peer_links(&[id("node-1")], true);
        assert!(state.is_blocked("node-1"));

        state.reset().await;
        assert!(!state.is_blocked("node-1"), "reset must unblock all links");
    }
}
