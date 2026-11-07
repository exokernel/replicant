//! Shared types for the replicant project.
//!
//! - [`proto`]: generated tonic/prost code for the `replicant.v1` gRPC API.
//! - [`NodeId`]: the identity of a replica.
//! - [`ScalarVal`] and [`Op`]: library-agnostic values and operations. Each
//!   has a `TryFrom` impl from its proto type.
//! - [`CrdtAdapter`]: the trait that each CRDT backend (Automerge, Yrs, Loro)
//!   implements. The replica scaffolding calls it.

mod adapter;
mod node_id;
mod op;

pub use adapter::CrdtAdapter;
pub use node_id::NodeId;
pub use op::{Op, ScalarVal};

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
