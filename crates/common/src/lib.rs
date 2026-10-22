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

/// Generated protobuf types for the `replicant.v1` package.
pub mod proto {
    tonic::include_proto!("replicant.v1");
}
