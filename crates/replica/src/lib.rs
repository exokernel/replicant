//! Replica crate — Automerge-backed gRPC replica process.
//!
//! Exposes [`server`] (tonic service impls + shared state), [`adapter`]
//! (the [`common::CrdtAdapter`] implementation for Automerge), and [`metrics`]
//! (OTel instrument definitions) as public modules so the orchestrator can
//! spin up in-process replicas for integration testing.

pub mod adapter;
pub mod metrics;
pub mod server;
