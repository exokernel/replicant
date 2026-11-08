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
//! The store builds the document under [`BASELINE_AUTHOR`], an id that no trial
//! uses. A replica that loads the document keeps its own id. So every trial
//! starts from the same author history, and no trial writer continues it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use common::{ensure_distinct_instance_ids, proto};
use replay::oracle::{CheckPlan, Content};
use replay::trace::{ContentKind, Trace, read_verified};
use tonic::Request;

use crate::replicas::{
    Client, ensure_text_all, fingerprint, reset_all, set_instance_ids, verify_text_length,
};
use crate::scenario::Baseline;

/// The traces and the encoded documents of the baselines of one run.
///
/// An encoded document belongs to one CRDT library. One orchestrator process
/// drives one library, so one store must serve only one process.
#[derive(Default)]
pub struct BaselineStore {
    /// The directory that holds the trace files.
    data_dir: PathBuf,
    /// The parsed traces and their checked SHA-256 hashes, by the path in the
    /// scenario.
    traces: HashMap<PathBuf, Checked>,
    /// The encoded documents, by the path in the scenario.
    encoded: HashMap<PathBuf, Vec<u8>>,
}

/// The library id that builds every baseline document. It is fixed, so the
/// author of the baseline history is the same in every trial. The seeded trial
/// ids look random, and [`BaselineStore::install`] checks that none of them is
/// equal to this id in the bits that the adapters keep.
pub const BASELINE_AUTHOR: u128 = 0xba5e_11ae_0000_0000_0000_0000_0000_0001;

/// A trace whose file had the expected hash.
struct Checked {
    /// The hash that the scenario asked for, and the file had.
    sha256: String,
    /// The parsed trace.
    trace: Trace,
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
    /// fails before a sweep runs. A trace that the store has already checked
    /// must have the same hash in `baseline`. Two scenarios that give one path
    /// two hashes cannot both be right.
    pub fn check(&mut self, baseline: &Baseline) -> Result<()> {
        if let Some(checked) = self.traces.get(&baseline.trace) {
            ensure!(
                checked.sha256.eq_ignore_ascii_case(&baseline.sha256),
                "baseline {} has SHA-256 {} in one scenario and {} in another",
                baseline.trace.display(),
                checked.sha256,
                baseline.sha256
            );
            return Ok(());
        }
        let path = self.data_dir.join(&baseline.trace);
        // A hash is hex, so compare it in one case. The cached branch above
        // ignores case too, so the order of the scenarios does not matter.
        let sha256 = baseline.sha256.to_ascii_lowercase();
        let bytes = read_verified(&path, Some(&sha256)).with_context(|| {
            format!(
                "reading baseline {}. Make the traces with `just gen-traces <crdt-benchmarks clone>`",
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
        self.traces.insert(
            baseline.trace.clone(),
            Checked {
                sha256: baseline.sha256.clone(),
                trace,
            },
        );
        Ok(())
    }

    /// Replaces the document of each replica with the baseline, and checks that
    /// all replicas hold the same document with the expected length.
    ///
    /// Call this after a reset, while no links exist.
    /// `ids` are the trial ids that the replicas have now. To build the document,
    /// replica 0 takes [`BASELINE_AUTHOR`] for a moment, and then gets its trial
    /// id back on an empty document.
    pub async fn install(
        &mut self,
        baseline: &Baseline,
        clients: &mut [Client],
        ids: &[u128],
    ) -> Result<()> {
        self.check(baseline)?;
        let with_author: Vec<u128> = ids.iter().copied().chain([BASELINE_AUTHOR]).collect();
        ensure_distinct_instance_ids(&with_author)
            .context("a trial id is equal to the baseline author id")?;
        if !self.encoded.contains_key(&baseline.trace) {
            let trace = &self.traces[&baseline.trace].trace;
            set_instance_ids(&clients[..1], &[BASELINE_AUTHOR]).await?;
            let state = build(trace, clients).await?;
            reset_all(&clients[..1]).await?;
            set_instance_ids(&clients[..1], &ids[..1]).await?;
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

/// Replays `trace` on replica 0, checks the text against the trace's oracle,
/// and returns the encoded document.
///
/// The check runs before the store caches the bytes. Every trial loads them, and
/// equal fingerprints after the load show only that every replica has the same
/// document, not that it is the trace's text.
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
    let text = builder
        .read_text(Request::new(proto::ObjRef {
            obj: common::TEXT_OBJ.to_owned(),
        }))
        .await
        .context("reading the baseline text")?
        .into_inner()
        .text;
    CheckPlan::new(trace, 1)?
        .check(&Content::Text(text))
        .context("the baseline document is not the trace's final text")?;
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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use sha2::{Digest, Sha256};

    use super::BaselineStore;
    use crate::scenario::Baseline;

    /// Returns an empty directory for one test.
    fn scratch(test: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("baseline-{}-{test}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Writes `json` as a trace file in `dir`, and returns a baseline that
    /// names it with its true hash.
    fn baseline(dir: &Path, name: &str, json: &str) -> Baseline {
        std::fs::write(dir.join(name), json).unwrap();
        Baseline {
            trace: name.into(),
            sha256: replay::trace::hex(&Sha256::digest(json)),
            length: 1,
        }
    }

    /// Returns the error of `check`, as text.
    fn refusal(dir: &Path, baseline: &Baseline) -> String {
        format!("{:#}", BaselineStore::new(dir).check(baseline).unwrap_err())
    }

    #[test]
    fn a_single_chain_of_ascii_text_is_accepted() {
        let dir = scratch("ok");
        let b = baseline(
            &dir,
            "ok.json",
            r#"{"endContent":"ab","txns":[{"patches":[[0,0,"ab"]]}]}"#,
        );
        BaselineStore::new(&dir).check(&b).unwrap();
    }

    #[test]
    fn a_baseline_with_the_wrong_hash_is_refused() {
        let dir = scratch("hash");
        let mut b = baseline(
            &dir,
            "t.json",
            r#"{"endContent":"a","txns":[{"patches":[[0,0,"a"]]}]}"#,
        );
        b.sha256 = "0".repeat(64);
        let err = refusal(&dir, &b);
        assert!(
            err.contains("SHA-256") && err.contains("gen-traces"),
            "{err}"
        );
    }

    /// The divergence cells start every replica from one document, so a
    /// baseline must be one chain of text edits. A fork has no single text.
    #[test]
    fn a_baseline_that_forks_is_refused() {
        let dir = scratch("fork");
        let b = baseline(
            &dir,
            "fork.json",
            r#"{"kind":"concurrent","numAgents":2,"oracle":{"kind":"tree_invariants"},"txns":[
                {"agent":0,"parents":[],"patches":[[0,0,"a"]]},
                {"agent":0,"parents":[0],"patches":[[0,0,"b"]]},
                {"agent":1,"parents":[0],"patches":[[0,0,"c"]]}]}"#,
        );
        assert!(refusal(&dir, &b).contains("one chain"));
    }

    #[test]
    fn a_list_baseline_is_refused() {
        let dir = scratch("list");
        let b = baseline(
            &dir,
            "list.json",
            r#"{"oracle":{"kind":"exact_list","values":["a"]},"txns":[{"listOps":[[0,0,["a"]]]}]}"#,
        );
        assert!(refusal(&dir, &b).contains("must be text"));
    }

    /// The Yrs adapter indexes text by bytes, so the positions of a text that
    /// is not ASCII would differ between libraries.
    #[test]
    fn a_baseline_that_is_not_ascii_is_refused() {
        let dir = scratch("ascii");
        let b = baseline(
            &dir,
            "e.json",
            r#"{"endContent":"é","txns":[{"patches":[[0,0,"é"]]}]}"#,
        );
        assert!(refusal(&dir, &b).contains("must be ASCII"));
    }

    /// A second scenario that gives a checked trace another hash is refused,
    /// and the same hash again, in any case, is accepted.
    #[test]
    fn a_checked_trace_with_another_hash_is_refused() {
        let dir = scratch("reuse");
        let json = r#"{"endContent":"ab","txns":[{"patches":[[0,0,"ab"]]}]}"#;
        let mut store = BaselineStore::new(&dir);
        let mut b = baseline(&dir, "t.json", json);
        // An uppercase pin works when it is checked first, and after a
        // lowercase one.
        let mut upper = baseline(&dir, "t.json", json);
        upper.sha256 = upper.sha256.to_uppercase();
        BaselineStore::new(&dir).check(&upper).unwrap();
        store.check(&b).unwrap();
        b.sha256 = b.sha256.to_uppercase();
        store.check(&b).unwrap();
        b.sha256 = "0".repeat(64);
        let err = format!("{:#}", store.check(&b).unwrap_err());
        assert!(err.contains("in another"), "{err}");
    }

    /// A trace whose patches do not make its `endContent` is refused when the
    /// store builds the document, before any trial can load it.
    #[tokio::test]
    async fn a_baseline_that_does_not_make_its_end_content_is_refused() {
        let dir = std::env::temp_dir().join(format!("baseline-oracle-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let json = br#"{"endContent":"ba","txns":[{"patches":[[0,0,"ab"]]}]}"#;
        std::fs::write(dir.join("t.json"), json).unwrap();
        let baseline = Baseline {
            trace: PathBuf::from("t.json"),
            sha256: replay::trace::hex(&Sha256::digest(json)),
            length: 2,
        };
        let mut tasks = tokio::task::JoinSet::new();
        let (_, mut clients) =
            crate::replicas::spawn_nodes(2, replica::adapter::Crdt::Yrs, &mut tasks)
                .await
                .unwrap();
        let ids = common::seeded_instance_ids(1, 2);
        let err = BaselineStore::new(&dir)
            .install(&baseline, &mut clients, &ids)
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("not the trace's final text"),
            "{err:#}"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The store builds the paper under `BASELINE_AUTHOR`. After the install,
    /// on a cache miss and on a cache hit, replica 0 writes under its own trial
    /// id, and every change of the baseline has the baseline author.
    #[tokio::test]
    async fn the_baseline_has_its_own_author_and_replica_0_keeps_its_trial_id() {
        use automerge::{ActorId, AutoCommit};
        use common::proto::Empty;
        use tonic::Request;

        use crate::replicas::{map_put, reset_all, set_instance_ids, spawn_nodes};

        let dir = scratch("author");
        let mut b = baseline(
            &dir,
            "t.json",
            r#"{"endContent":"ab","txns":[{"patches":[[0,0,"a"]]},{"patches":[[1,0,"b"]]}]}"#,
        );
        b.length = 2;
        let mut tasks = tokio::task::JoinSet::new();
        let (_, mut clients) = spawn_nodes(2, replica::adapter::Crdt::Automerge, &mut tasks)
            .await
            .unwrap();
        let ids = common::seeded_instance_ids(7, 2);
        let actor = |id: u128| ActorId::from(id.to_be_bytes().as_slice());
        let mut store = BaselineStore::new(&dir);
        for pass in ["cache miss", "cache hit"] {
            // Each trial resets the replicas and sets their ids first.
            reset_all(&clients).await.unwrap();
            set_instance_ids(&clients, &ids).await.unwrap();
            store.install(&b, &mut clients, &ids).await.unwrap();
            map_put(&mut clients[0], "k", "v").await.unwrap();
            let state = clients[0]
                .encode(Request::new(Empty {}))
                .await
                .unwrap()
                .into_inner()
                .state;
            let mut doc = AutoCommit::load(&state).unwrap();
            let authors: Vec<ActorId> = doc
                .get_changes(&[])
                .iter()
                .map(|change| change.actor_id().clone())
                .collect();
            // The first change creates the text object under the fixed actor of
            // EnsureText (`replicant-bootstrap` in the Automerge adapter), so
            // that every replica has the same object.
            let bootstrap = ActorId::from(b"replicant-bootstrap".as_slice());
            let (write, history) = authors.split_last().unwrap();
            assert_eq!(*write, actor(ids[0]), "{pass}: the write has the trial id");
            assert_eq!(history.first(), Some(&bootstrap), "{pass}: the text object");
            assert!(history.len() > 1, "{pass}: the baseline has edits");
            assert!(
                history[1..]
                    .iter()
                    .all(|a| *a == actor(super::BASELINE_AUTHOR)),
                "{pass}: every baseline edit has the baseline author"
            );
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
