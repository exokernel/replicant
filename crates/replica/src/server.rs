//! The gRPC server for one replica.
//!
//! [`ReplicaState`] holds the document, the peer streams, and the metric
//! instruments. [`router`] serves it over gRPC with two services: `Replica`
//! takes control calls from the orchestrator, and `Sync` accepts sync streams
//! from peer replicas. The `service` module holds the `Replica` service. The
//! `peer` module holds the sync stream code for both directions.

mod peer;
mod service;
mod state;

use service::ReplicaService;
pub use service::router;
pub use state::ReplicaState;
