//! Gives every replica of a divergence cell the same starting document.
//!
//! A cell with a [`Baseline`] does not start empty. It starts from the document
//! that a recorded trace makes. The replicas share the elements of that document,
//! so an insert on each side can attach to the same element. That is the conflict
//! that a cell with an empty start cannot have. See `docs/divergence-sweep.md`.
//!
//! # How a replica gets the document
//!
//! The store replays the trace once on replica 0, with one `ApplyTxn` call for each
//! transaction, so the document has the full history of the trace. It keeps the
//! encoded bytes. Each trial then loads these bytes on every replica. This does not
//! use the sync protocol, and it happens before the clock starts.
//!
//! A replica that loads a document takes a new random id, so the replicas do not
//! share an author id.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use common::proto;
use replay::trace::{ContentKind, Trace, read_verified};
use tonic::Request;

use crate::replicas::{Client, ensure_text_all, fingerprint, verify_text_length};
use crate::scenario::Baseline;

/// The traces and the encoded documents of the baselines of one run.
///
/// An encoded document belongs to one CRDT library. One orchestrator process
/// drives one library, so one store must serve only one process.
#[derive(Default)]
pub struct BaselineStore {
    /// The directory that holds the trace files.
    data_dir: PathBuf,
    /// The parsed traces, by the path in the scenario.
    traces: HashMap<PathBuf, Trace>,
    /// The encoded documents, by the path in the scenario.
    encoded: HashMap<PathBuf, Vec<u8>>,
}

impl BaselineStore {
    /// Creates a store that reads trace files from `data_dir`.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            data_dir: data_dir.to_owned(),
            ..Self::default()
        }
    }

    /// Reads and checks the trace of `baseline`, if the store does not have it.
    ///
    /// Call this before the first trial, so that a missing file or a wrong hash
    /// fails before a sweep runs.
    pub fn check(&mut self, baseline: &Baseline) -> Result<()> {
        if self.traces.contains_key(&baseline.trace) {
            return Ok(());
        }
        let path = self.data_dir.join(&baseline.trace);
        let bytes = read_verified(&path, Some(&baseline.sha256)).with_context(|| {
            format!(
                "reading baseline {}. Make the traces with `just gen-traces`",
                path.display()
            )
        })?;
        let trace =
            Trace::parse(&bytes).with_context(|| format!("parsing baseline {}", path.display()))?;
        ensure!(
            trace.kind == ContentKind::Text && trace.is_single_chain(),
            "baseline {} must be text, with one chain of edits",
            path.display()
        );
        // The Yrs adapter indexes text by bytes.
        ensure!(
            !trace.non_ascii,
            "baseline {} must be ASCII",
            path.display()
        );
        self.traces.insert(baseline.trace.clone(), trace);
        Ok(())
    }

    /// Replaces the document of each replica with the baseline, and checks that
    /// all replicas hold the same document with the expected length.
    ///
    /// Call this after a reset, while no links exist.
    pub async fn install(&mut self, baseline: &Baseline, clients: &mut [Client]) -> Result<()> {
        self.check(baseline)?;
        if !self.encoded.contains_key(&baseline.trace) {
            let trace = &self.traces[&baseline.trace];
            let state = build(trace, clients).await?;
            self.encoded.insert(baseline.trace.clone(), state);
        }
        let state = &self.encoded[&baseline.trace];
        for (i, client) in clients.iter_mut().enumerate() {
            client
                .load(Request::new(proto::LoadRequest {
                    state: state.clone(),
                }))
                .await
                .with_context(|| format!("loading the baseline on node {i}"))?;
        }
        // Yrs gives a root object its type only when a caller asks for it, so a
        // loaded document must be asked for the text before the checks read it.
        let n = clients.len();
        ensure_text_all(clients, 0..n).await?;
        verify_same_document(clients, baseline.length).await
    }
}

/// Replays `trace` on replica 0 and returns the encoded document.
async fn build(trace: &Trace, clients: &mut [Client]) -> Result<Vec<u8>> {
    ensure_text_all(clients, 0..1).await?;
    let builder = &mut clients[0];
    for (i, txn) in trace.txns.iter().enumerate() {
        let ops = txn
            .ops
            .iter()
            .map(proto::OpRequest::try_from)
            .collect::<Result<Vec<_>>>()
            .with_context(|| format!("converting baseline txn {i}"))?;
        builder
            .apply_txn(Request::new(proto::TxnRequest { ops }))
            .await
            .with_context(|| format!("applying baseline txn {i}"))?;
    }
    let reply = builder
        .encode(Request::new(proto::Empty {}))
        .await
        .context("encoding the baseline")?;
    Ok(reply.into_inner().state)
}

/// Fails unless all replicas report one fingerprint and the expected text length.
async fn verify_same_document(clients: &mut [Client], length: usize) -> Result<()> {
    let mut first = None;
    for (i, client) in clients.iter_mut().enumerate() {
        let fp = fingerprint(client).await?;
        match &first {
            None => first = Some(fp),
            Some(expected) if *expected != fp => {
                bail!("node {i} does not hold the same baseline as node 0");
            }
            Some(_) => {}
        }
    }
    let all: Vec<usize> = (0..clients.len()).collect();
    verify_text_length(clients, &all, length).await
}
