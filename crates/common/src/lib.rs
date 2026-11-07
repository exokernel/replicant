//! Shared types for the replicant project.
//!
//! - [`proto`]: generated tonic/prost code for the `replicant.v1` gRPC API.
//! - [`NodeId`]: the identity of a replica.
//! - [`ScalarVal`] and [`Op`]: library-agnostic values and operations. Each
//!   has a `TryFrom` impl from its proto type.
//! - [`CrdtAdapter`]: the trait that each CRDT backend (Automerge, Yrs, Loro)
//!   implements. The replica scaffolding calls it.
//! - [`SplitMix64`]: a deterministic random number generator.

mod adapter;
mod node_id;
mod op;
pub mod provenance;
mod split_mix64;

pub use adapter::{CrdtAdapter, INSTANCE_ID_BITS};
pub use node_id::NodeId;
pub use op::{Op, ScalarVal};
pub use split_mix64::SplitMix64;
use tonic::transport::Channel;

/// The text object that text workloads edit.
pub const TEXT_OBJ: &str = "text";

/// The largest gRPC message that a replica and its clients accept, in bytes.
///
/// The tonic default is 4 MiB. A replay reads and encodes whole documents, and
/// these can be much larger. B4x100 has more than 10 MB of text.
pub const MAX_RPC_BYTES: usize = 1 << 30;

/// Generated protobuf types for the `replicant.v1` package.
#[allow(missing_docs, clippy::missing_docs_in_private_items)]
pub mod proto {
    tonic::include_proto!("replicant.v1");
}

/// Connects to the replica at `addr`, in `host:port` form.
pub async fn connect_replica(
    addr: &str,
) -> Result<proto::replica_client::ReplicaClient<Channel>, tonic::transport::Error> {
    let client = proto::replica_client::ReplicaClient::connect(format!("http://{addr}")).await?;
    Ok(client
        .max_decoding_message_size(MAX_RPC_BYTES)
        .max_encoding_message_size(MAX_RPC_BYTES))
}
