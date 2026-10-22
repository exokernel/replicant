//! Sync streams between replicas, in both directions.
//!
//! - Inbound: a peer opens a stream to this replica's `Sync` service.
//! - Outbound: `ConnectPeer` makes this replica open a stream to a peer, with
//!   [`connect_and_run`].
//!
//! Both directions then run the same [`recv_loop`]. Each direction has an
//! outbound channel of raw payloads, which `ReplicaState::peer_txs` holds.

use std::sync::Arc;

use anyhow::Context as _;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use common::NodeId;
use common::proto::{self, sync_client::SyncClient, sync_server::Sync};

use super::{ReplicaService, ReplicaState};

/// The capacity of each outbound channel, in messages.
///
/// When a peer's channel is full, `try_flush_peer` defers the flush. It does
/// not wait. See `ReplicaState::flush_to_peers`.
const PEER_CHANNEL_CAPACITY: usize = 64;

#[tonic::async_trait]
impl Sync for ReplicaService {
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

        let (raw_tx, grpc_rx) = outbound_channel(Ok);
        // Register raw_tx, so that recv_loop (replies) and flush_to_peers
        // (changes after each op) can both write to this peer.
        self.state.register_peer(peer_id.clone(), raw_tx.clone());

        let handle = tokio::spawn(recv_loop(
            self.state.clone(),
            peer_id,
            request.into_inner(),
            raw_tx,
        ));
        // Register the handle, so that `reset()` stops this task too.
        self.state.register_peer_task(handle);

        // Tonic reads grpc_rx and sends each message to the calling peer.
        Ok(Response::new(ReceiverStream::new(grpc_rx)))
    }
}

/// Opens a sync stream to `peer_id` at `addr`, then reads it with
/// [`recv_loop`].
///
/// `ConnectPeer` spawns this task and waits on `ready_tx`. The task sends the
/// setup result there:
/// - `Ok(())` when the stream is open and the peer is in `peer_txs`.
/// - `Err` with the cause when the connection fails.
///
/// On success, the steps run in this order: register the peer, send the first
/// sync message, signal ready, run `recv_loop`.
pub(super) async fn connect_and_run(
    state: Arc<ReplicaState>,
    peer_id: NodeId,
    addr: String,
    ready_tx: oneshot::Sender<Result<(), String>>,
) {
    let (inbound, raw_tx) = match open_stream(&state, &peer_id, &addr).await {
        Ok(stream) => stream,
        Err(e) => {
            let cause = format!("{e:#}");
            tracing::error!(peer = %peer_id, "sync stream error: {cause}");
            let _ = ready_tx.send(Err(cause));
            return;
        }
    };

    state.register_peer(peer_id.clone(), raw_tx.clone());
    tracing::info!(peer = %peer_id, %addr, "sync stream established");

    // Send the first sync message before signalling ready, so the handshake
    // has started when the caller continues. If the link is blocked,
    // `try_flush_peer` sends nothing. The stream stays quiet, and `KickSync`
    // starts the handshake at heal time.
    state.try_flush_peer(&peer_id, &raw_tx);

    // The stream is open, so let the ConnectPeer RPC return.
    let _ = ready_tx.send(Ok(()));

    recv_loop(state, peer_id, inbound, raw_tx).await;
}

/// Connects to `addr` and opens a sync stream to `peer_id`. Returns the
/// inbound half and the sender for the outbound half.
async fn open_stream(
    state: &ReplicaState,
    peer_id: &NodeId,
    addr: &str,
) -> anyhow::Result<(tonic::Streaming<proto::SyncMessage>, mpsc::Sender<Vec<u8>>)> {
    let endpoint = Channel::from_shared(format!("http://{addr}"))
        .with_context(|| format!("invalid peer address '{addr}'"))?;
    let channel = endpoint
        .connect()
        .await
        .with_context(|| format!("cannot connect to peer {peer_id} at {addr}"))?;
    let mut client = SyncClient::new(channel);

    let (raw_tx, grpc_rx) = outbound_channel(std::convert::identity);
    let mut request = Request::new(ReceiverStream::new(grpc_rx));
    request
        .metadata_mut()
        .insert("x-peer-id", state.actor_id.header_value());

    let inbound = client
        .stream(request)
        .await
        .with_context(|| format!("cannot open a sync stream to peer {peer_id} at {addr}"))?
        .into_inner();
    Ok((inbound, raw_tx))
}

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
                state.flush_to_peers();
            }
            Err(status) => {
                tracing::warn!(peer = %peer_id, "stream error: {status}");
                break;
            }
        }
    }

    state.deregister_peer(&peer_id);
    tracing::info!(peer = %peer_id, "sync stream closed");
}

/// Creates the outbound channels for one sync stream. Returns the raw sender
/// for replica code and the receiver that tonic reads.
///
/// Outbound messages go through two channels. Replica code writes raw bytes
/// to the raw sender. A forwarder task wraps each payload as a
/// [`proto::SyncMessage`] and sends it on the second channel. The server side
/// of a stream sends `Result<SyncMessage, Status>`, and the client side sends
/// `SyncMessage`. `wrap` converts to the type the stream needs: `Ok` for the
/// server side, `identity` for the client side.
///
/// The forwarder stops when either channel closes.
fn outbound_channel<T: Send + 'static>(
    wrap: impl Fn(proto::SyncMessage) -> T + Send + 'static,
) -> (mpsc::Sender<Vec<u8>>, mpsc::Receiver<T>) {
    let (raw_tx, mut raw_rx) = mpsc::channel::<Vec<u8>>(PEER_CHANNEL_CAPACITY);
    let (grpc_tx, grpc_rx) = mpsc::channel::<T>(PEER_CHANNEL_CAPACITY);
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
    (raw_tx, grpc_rx)
}
