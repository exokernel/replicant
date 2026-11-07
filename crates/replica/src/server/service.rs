//! The `Replica` gRPC service: the control calls from the orchestrator.

use std::sync::Arc;

use tokio::sync::oneshot;
use tonic::transport::Server;
use tonic::transport::server::Router;
use tonic::{Request, Response, Status};

use common::NodeId;
use common::proto::{
    self,
    replica_server::{Replica, ReplicaServer},
    sync_server::SyncServer,
};
use common::{MAX_RPC_BYTES, Op};

use super::peer;
use super::state::ReplicaState;

/// Builds a gRPC server that serves the replica `state`.
///
/// The server has two services. [`Replica`] handles control calls from the
/// orchestrator: apply ops, read state, connect peers, block links, and reset.
/// [`Sync`](proto::sync_server::Sync) accepts sync streams from peer replicas.
/// The caller picks the transport, for example with `serve_with_shutdown`.
pub fn router(state: Arc<ReplicaState>) -> Router {
    let service = ReplicaService { state };
    Server::builder()
        .add_service(
            ReplicaServer::new(service.clone())
                .max_decoding_message_size(MAX_RPC_BYTES)
                .max_encoding_message_size(MAX_RPC_BYTES),
        )
        .add_service(SyncServer::new(service))
}

/// Implements both gRPC services for one replica. This module implements
/// [`Replica`]. The `peer` module implements `Sync`.
#[derive(Clone)]
pub(super) struct ReplicaService {
    /// The replica that this service serves.
    pub(super) state: Arc<ReplicaState>,
}

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

    async fn get_stats(
        &self,
        _: Request<proto::Empty>,
    ) -> Result<Response<proto::ReplicaStats>, Status> {
        Ok(Response::new(self.state.stats()))
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

    async fn set_instance_id(
        &self,
        request: Request<proto::InstanceId>,
    ) -> Result<Response<proto::Empty>, Status> {
        let proto::InstanceId { high, low } = request.into_inner();
        let id = (u128::from(high) << 64) | u128::from(low);
        let (result, _) = self.state.timed(|a| a.set_instance_id(id));
        result.map_err(internal)?;
        Ok(Response::new(proto::Empty {}))
    }

    async fn ensure_list(
        &self,
        request: Request<proto::ObjRef>,
    ) -> Result<Response<proto::Empty>, Status> {
        let obj = request.into_inner().obj;
        let (result, _) = self.state.timed(|a| a.ensure_list(&obj));
        result.map_err(internal)?;
        Ok(Response::new(proto::Empty {}))
    }

    async fn read_text(
        &self,
        request: Request<proto::ObjRef>,
    ) -> Result<Response<proto::ReadTextResponse>, Status> {
        let obj = request.into_inner().obj;
        let (text, adapter_ns) = self.state.timed(|a| a.read_text(&obj));
        Ok(Response::new(proto::ReadTextResponse {
            text: text.map_err(internal)?,
            adapter_ns,
        }))
    }

    async fn read_list(
        &self,
        request: Request<proto::ObjRef>,
    ) -> Result<Response<proto::ReadListResponse>, Status> {
        let obj = request.into_inner().obj;
        let (values, adapter_ns) = self.state.timed(|a| a.read_list(&obj));
        let values = values.map_err(internal)?;
        Ok(Response::new(proto::ReadListResponse {
            values: values.iter().map(Into::into).collect(),
            adapter_ns,
        }))
    }

    async fn read_map(
        &self,
        request: Request<proto::ObjRef>,
    ) -> Result<Response<proto::ReadMapResponse>, Status> {
        let obj = request.into_inner().obj;
        let (values, adapter_ns) = self.state.timed(|a| a.read_map(&obj));
        let values = values.map_err(internal)?;
        Ok(Response::new(proto::ReadMapResponse {
            values: values.iter().map(|(k, v)| (k.clone(), v.into())).collect(),
            adapter_ns,
        }))
    }

    async fn apply_txn(
        &self,
        request: Request<proto::TxnRequest>,
    ) -> Result<Response<proto::TxnResponse>, Status> {
        let ops = request
            .into_inner()
            .ops
            .into_iter()
            .map(Op::try_from)
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let (update, adapter_ns) = self.state.timed(|a| a.apply_txn(&ops));
        let update = update.map_err(internal)?;
        self.state
            .record_op_duration("apply_txn", adapter_ns as f64 / 1e6);
        self.state.flush_to_peers();
        Ok(Response::new(proto::TxnResponse { update, adapter_ns }))
    }

    async fn apply_updates(
        &self,
        request: Request<proto::UpdatesRequest>,
    ) -> Result<Response<proto::TimedResponse>, Status> {
        let updates = request.into_inner().updates;
        let updates: Vec<&[u8]> = updates.iter().map(Vec::as_slice).collect();
        let (result, adapter_ns) = self.state.timed(|a| a.apply_updates(&updates));
        result.map_err(internal)?;
        Ok(Response::new(proto::TimedResponse { adapter_ns }))
    }

    async fn encode(
        &self,
        _: Request<proto::Empty>,
    ) -> Result<Response<proto::EncodeResponse>, Status> {
        let (state, adapter_ns) = self.state.timed(|a| a.encode());
        Ok(Response::new(proto::EncodeResponse { state, adapter_ns }))
    }

    async fn load(
        &self,
        request: Request<proto::LoadRequest>,
    ) -> Result<Response<proto::TimedResponse>, Status> {
        let state = request.into_inner().state;
        let (result, adapter_ns) = self.state.timed(|a| a.load(&state));
        result.map_err(internal)?;
        Ok(Response::new(proto::TimedResponse { adapter_ns }))
    }
}

/// Converts an adapter error to a gRPC status.
fn internal(e: anyhow::Error) -> Status {
    Status::internal(format!("{e:#}"))
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
