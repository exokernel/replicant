//! Replica crate — the gRPC replica process and its CRDT backends.
//!
//! Exposes [`server`] (tonic service impls + shared state), [`adapter`] (one
//! [`common::CrdtAdapter`] implementation per backing library, plus the
//! [`adapter::Crdt`] selector both binaries take as `--crdt`), and
//! [`metrics`] (OTel instrument definitions) as public modules so the
//! orchestrator can spin up in-process replicas for integration testing.

pub mod adapter;
pub mod metrics;
pub mod server;
