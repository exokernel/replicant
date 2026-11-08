//! The shared state of one replica: the document, the peer streams, and the
//! metric instruments.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use opentelemetry::KeyValue;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use common::NodeId;
use common::proto;
use common::{CrdtAdapter, Op};

use crate::metrics::Metrics;

/// Sync message counts on the link to one peer.
#[derive(Clone, Copy, Default)]
struct PeerCounts {
    /// Messages sent to the peer.
    tx: u64,
    /// Messages received from the peer.
    rx: u64,
    /// Flushes skipped because the channel to the peer was full.
    deferred: u64,
}

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
    pub(super) actor_id: NodeId,
    /// The `actor` metric attribute. It is built once, so that recording a
    /// metric does not allocate.
    actor_attr: KeyValue,
    /// The CRDT document.
    adapter: Mutex<Box<dyn CrdtAdapter>>,
    /// The outbound channel for each connected peer.
    peer_txs: Mutex<HashMap<NodeId, mpsc::Sender<Vec<u8>>>>,
    /// Sync message counts for each peer since the last reset. The `GetStats`
    /// RPC returns them. Unlike the OTel counters, they reset with the
    /// document, so they describe one trial.
    peer_stats: Mutex<HashMap<NodeId, PeerCounts>>,
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
            peer_stats: Mutex::new(HashMap::new()),
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

    /// Locks and returns the outbound channel of each peer.
    fn peer_txs(&self) -> MutexGuard<'_, HashMap<NodeId, mpsc::Sender<Vec<u8>>>> {
        self.peer_txs.lock().expect("peer_txs mutex poisoned")
    }

    /// Locks and returns the task of each peer stream.
    fn peer_tasks(&self) -> MutexGuard<'_, Vec<JoinHandle<()>>> {
        self.peer_tasks.lock().expect("peer_tasks mutex poisoned")
    }

    /// Locks and returns the peers whose links are blocked.
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
    pub(super) fn apply_op_timed(&self, op: &Op) -> anyhow::Result<f64> {
        let mut adapter = self.adapter();
        let t0 = Instant::now();
        adapter.apply_op(op)?;
        Ok(t0.elapsed().as_secs_f64() * 1000.0)
    }

    /// Runs `f` on the adapter. Returns its result and the time in the adapter
    /// in nanoseconds. The timer starts after the lock is acquired, as in
    /// [`Self::apply_op_timed`].
    pub(super) fn timed<T>(&self, f: impl FnOnce(&mut dyn CrdtAdapter) -> T) -> (T, u64) {
        let mut adapter = self.adapter();
        let t0 = Instant::now();
        let out = f(adapter.as_mut());
        (out, t0.elapsed().as_nanos() as u64)
    }

    /// Returns the document heads. See [`CrdtAdapter::get_heads`].
    pub(super) fn get_heads(&self) -> Vec<Vec<u8>> {
        self.adapter().get_heads()
    }

    /// Returns the fingerprint. See [`CrdtAdapter::state_fingerprint`].
    pub(super) fn state_fingerprint(&self) -> Vec<u8> {
        self.adapter().state_fingerprint()
    }

    /// Returns the encoded size. This does a full encode of the document.
    fn doc_size_bytes(&self) -> usize {
        self.adapter().doc_size_bytes()
    }

    /// Returns the next sync message for `peer`. See
    /// [`CrdtAdapter::sync_generate`].
    pub(super) fn sync_generate(&self, peer: &str) -> Option<Vec<u8>> {
        self.adapter().sync_generate(peer)
    }

    /// Applies a sync message from `peer`. See [`CrdtAdapter::sync_receive`].
    pub(super) fn sync_receive(&self, peer: &str, msg: Vec<u8>) -> anyhow::Result<()> {
        self.adapter().sync_receive(peer, msg)
    }

    /// Creates the text object. See [`CrdtAdapter::ensure_text`].
    pub(super) fn ensure_text(&self, obj: &str) -> anyhow::Result<()> {
        self.adapter().ensure_text(obj)
    }

    /// Returns the text length. See [`CrdtAdapter::text_length`].
    pub(super) fn text_length(&self, obj: &str) -> anyhow::Result<usize> {
        self.adapter().text_length(obj)
    }

    // ── Peer links ─────────────────────────────────────────────────────────

    /// Returns true if the link to `peer` is blocked.
    pub(super) fn is_blocked(&self, peer: &str) -> bool {
        self.blocked().contains(peer)
    }

    /// Blocks or unblocks sync traffic with `peers`. See
    /// [`Self::blocked_peers`].
    ///
    /// An unblock is a reconnect to the same peer. For each link that was
    /// blocked, it calls [`CrdtAdapter::sync_reconnect`]. The adapter keeps
    /// only the sync state that its library keeps across a disconnect. For
    /// Automerge, that is the heads that both peers have, so the heal sends
    /// only what changed. Yrs and Loro start a new version exchange. A link
    /// that was not blocked keeps all of its state.
    ///
    /// This method sends no sync traffic. The orchestrator calls `KickSync`
    /// after both ends of every healed link are unblocked. A kick from here
    /// could reach a peer that is still blocked, and that peer would drop it.
    ///
    /// [`CrdtAdapter::sync_reconnect`]: common::CrdtAdapter::sync_reconnect
    pub(super) fn set_peer_links(&self, peers: &[NodeId], blocked: bool) {
        // Release the lock on the blocked set before the adapter lock is
        // taken. This method never holds both locks.
        let reconnected: Vec<&NodeId> = {
            let mut set = self.blocked();
            if blocked {
                set.extend(peers.iter().cloned());
                Vec::new()
            } else {
                peers.iter().filter(|peer| set.remove(*peer)).collect()
            }
        };
        if !reconnected.is_empty() {
            let mut adapter = self.adapter();
            for peer in &reconnected {
                adapter.sync_reconnect(peer);
            }
        }
        tracing::info!(
            actor = %self.actor_id,
            ?peers,
            blocked,
            ?reconnected,
            "peer links updated"
        );
    }

    // ── Metric recorders ───────────────────────────────────────────────────
    //
    // These methods attach the attributes that each instrument expects. Add
    // new attributes here, not at the call sites.

    /// Records the time of one op in the op duration histogram.
    pub(super) fn record_op_duration(&self, op_name: &'static str, elapsed_ms: f64) {
        self.metrics.op_duration_ms.record(
            elapsed_ms,
            &[self.actor_attr.clone(), KeyValue::new("op", op_name)],
        );
    }

    /// Samples the document size and records it on the gauge.
    /// Records the document size gauge.
    fn record_doc_size(&self, size_bytes: u64) {
        self.metrics
            .doc_size_bytes
            .record(size_bytes, std::slice::from_ref(&self.actor_attr));
    }

    /// Counts one sync message sent to `peer_id`.
    pub(super) fn record_sync_tx(&self, peer_id: &NodeId) {
        self.metrics.sync_tx.add(1, &self.peer_attrs(peer_id));
        self.count(peer_id, |c| c.tx += 1);
    }

    /// Counts one sync message received from `peer_id`.
    pub(super) fn record_sync_rx(&self, peer_id: &NodeId) {
        self.metrics.sync_rx.add(1, &self.peer_attrs(peer_id));
        self.count(peer_id, |c| c.rx += 1);
    }

    /// Counts one flush to `peer_id` that was skipped, because the channel was full.
    fn record_sync_deferred(&self, peer_id: &NodeId) {
        self.metrics.sync_deferred.add(1, &self.peer_attrs(peer_id));
        self.count(peer_id, |c| c.deferred += 1);
    }

    /// Applies `bump` to the counts of `peer_id`.
    fn count(&self, peer_id: &NodeId, bump: impl FnOnce(&mut PeerCounts)) {
        let mut stats = self.peer_stats.lock().expect("peer_stats mutex poisoned");
        match stats.get_mut(peer_id) {
            Some(counts) => bump(counts),
            None => bump(stats.entry(peer_id.clone()).or_default()),
        }
    }

    /// Returns the document size and the per-peer sync counts since the last
    /// reset, sorted by peer ID. Also records the size gauge.
    ///
    /// This is the only place that records the gauge. The size comes from a
    /// full encode of the document, and its cost grows with the document. Do
    /// not call this on the op path or the sync path. There it adds a full
    /// encode to each op and to each sync message.
    pub(super) fn stats(&self) -> proto::ReplicaStats {
        let mut peers: Vec<proto::PeerStats> = self
            .peer_stats
            .lock()
            .expect("peer_stats mutex poisoned")
            .iter()
            .map(|(peer, c)| proto::PeerStats {
                peer_id: peer.as_str().to_owned(),
                tx: c.tx,
                rx: c.rx,
                deferred: c.deferred,
            })
            .collect();
        peers.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        let doc_size_bytes = self.doc_size_bytes() as u64;
        self.record_doc_size(doc_size_bytes);
        proto::ReplicaStats {
            doc_size_bytes,
            peers,
        }
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

    /// Adds the outbound channel of `peer_id`.
    ///
    /// A new stream starts sync with `peer_id` from nothing. Any state from an
    /// older stream can record messages that it did not deliver.
    pub(super) fn register_peer(&self, peer_id: NodeId, tx: mpsc::Sender<Vec<u8>>) {
        self.adapter().sync_reset(&peer_id);
        self.peer_txs().insert(peer_id, tx);
    }

    /// Returns the connected peers to which the adapter has a sync message to
    /// send now. Blocked peers are left out, because nothing is sent to them.
    pub(super) fn sync_pending_peers(&self) -> Vec<NodeId> {
        let peers: Vec<NodeId> = self.peer_txs().keys().cloned().collect();
        let blocked = self.blocked().clone();
        let mut adapter = self.adapter();
        peers
            .into_iter()
            .filter(|p| !blocked.contains(p.as_str()) && adapter.sync_pending(p))
            .collect()
    }

    /// Removes the outbound channel of `peer_id`.
    pub(super) fn deregister_peer(&self, peer_id: &str) {
        self.peer_txs().remove(peer_id);
    }

    /// Stores the handle of a peer stream task, so that [`Self::reset`] can
    /// stop it.
    pub(super) fn register_peer_task(&self, handle: JoinHandle<()>) {
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
        self.peer_stats
            .lock()
            .expect("peer_stats mutex poisoned")
            .clear();
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
    pub(super) fn flush_to_peers(&self) {
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
    pub(super) fn kick_sync(&self, peers: &[NodeId]) {
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
    pub(super) fn try_flush_peer(&self, peer_id: &NodeId, tx: &mpsc::Sender<Vec<u8>>) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::AutomergeAdapter;
    use common::ScalarVal;

    /// Returns the state of a replica `node-0` that uses Automerge.
    fn new_state() -> Arc<ReplicaState> {
        ReplicaState::new(id("node-0"), Box::new(AutomergeAdapter::new()))
    }

    /// Makes a `NodeId` from a test literal. Test IDs are valid, so this
    /// cannot fail.
    fn id(s: &str) -> NodeId {
        NodeId::new(s).expect("test node ids are valid")
    }

    /// Returns one `MapPut` op on the root map.
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

    /// An unblock reconnects only the links that were blocked. Automerge then
    /// forgets a message that it sent but the peer did not confirm, and sends
    /// again. A link that was not blocked keeps that record, and sends nothing.
    #[test]
    fn unblock_reconnects_only_blocked_links() {
        let state = new_state();
        let (tx_blocked, mut rx_blocked) = mpsc::channel::<Vec<u8>>(8);
        let (tx_open, mut rx_open) = mpsc::channel::<Vec<u8>>(8);
        state.register_peer(id("node-1"), tx_blocked);
        state.register_peer(id("node-2"), tx_open);
        state.apply_op_timed(&map_put("before-block")).unwrap();
        state.flush_to_peers();
        assert!(rx_blocked.try_recv().is_ok() && rx_open.try_recv().is_ok());

        // No replies come, so both peers have an unconfirmed message.
        state.set_peer_links(&[id("node-1")], true);
        state.set_peer_links(&[id("node-1"), id("node-2")], false);
        state.kick_sync(&[id("node-1"), id("node-2")]);
        assert!(
            rx_blocked.try_recv().is_ok(),
            "a reconnected link must start sync again"
        );
        assert!(
            rx_open.try_recv().is_err(),
            "a link that was not blocked must keep its sync state"
        );
    }

    /// A kick to a peer with no open stream does nothing. In a sparse heal
    /// graph, not every peer has an edge.
    #[test]
    fn kick_sync_ignores_unknown_peers() {
        let state = new_state();
        state.kick_sync(&[id("node-9")]);
    }

    /// The per-peer counts describe one trial: a reset clears them.
    #[tokio::test]
    async fn stats_count_per_peer_and_reset_with_the_document() {
        let state = new_state();
        state.record_sync_tx(&id("node-1"));
        state.record_sync_tx(&id("node-1"));
        state.record_sync_rx(&id("node-2"));
        state.record_sync_deferred(&id("node-1"));

        let stats = state.stats();
        let counts: Vec<_> = stats
            .peers
            .iter()
            .map(|p| (p.peer_id.as_str(), p.tx, p.rx, p.deferred))
            .collect();
        assert_eq!(counts, [("node-1", 2, 0, 1), ("node-2", 0, 1, 0)]);

        state.reset().await;
        assert!(
            state.stats().peers.is_empty(),
            "reset must clear the counts"
        );
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
