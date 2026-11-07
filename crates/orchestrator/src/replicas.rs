//! Talks to replicas over gRPC: gets them, wires them, resets them, drives
//! writes, and polls them for convergence.
//!
//! The two scenario runners, [`crate::runner`] and [`crate::partition_heal`],
//! both build on these helpers.
//!
//! A replica is either an in-process Tokio task or an externally managed
//! process reached over gRPC. [`NodeSource`] selects between them, and the rest
//! of the module is written against the gRPC client either way.

mod convergence;
mod nodes;
mod wiring;
mod writes;

use tonic::transport::Channel;

use common::proto::replica_client::ReplicaClient;

pub use convergence::{fingerprint, record_stats, verify_text_length, wait_for_nodes};
pub use nodes::{NodeSource, ReplicaEndpoint, acquire_nodes, check_tasks};
#[cfg(test)]
pub use nodes::{node_index, node_name, spawn_nodes};
pub use wiring::{connect_edges, kick_sync_edges, reset_all, set_links_blocked};
#[cfg(test)]
pub use writes::map_put;
pub use writes::{TEXT_OBJ, apply_write, ensure_text_all, text_splice};

/// The gRPC client the orchestrator uses for one replica.
pub type Client = ReplicaClient<Channel>;

#[cfg(test)]
mod tests;
