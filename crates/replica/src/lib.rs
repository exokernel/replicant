//! The gRPC replica and its CRDT backends.
//!
//! The modules are public so that the orchestrator can run replicas in its own
//! process:
//!
//! - [`server`]: the tonic services and the shared replica state.
//! - [`adapter`]: one [`common::CrdtAdapter`] per CRDT library, and the
//!   [`adapter::Crdt`] selector that both binaries take as `--crdt`.
//!
//! The private `metrics` module holds the OTel instruments.

pub mod adapter;
mod metrics;
pub mod server;
