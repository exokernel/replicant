//! [`common::CrdtAdapter`] implementations, one per backing CRDT library.
//!
//! Each submodule owns its own adapter struct and the `#[test]` wrappers
//! that claim generic conformance-suite functions from [`conformance`]. A
//! new adapter is done when every generic function it wraps passes — see
//! that module's doc comment for what "generic" and "wraps" mean here.

pub mod automerge;
pub mod loro;
pub mod yrs;

pub use automerge::AutomergeAdapter;
pub use loro::LoroAdapter;
pub use yrs::YrsAdapter;

/// Which [`common::CrdtAdapter`] implementation backs a replica.
///
/// Shared by both binaries rather than defined per-binary: the replica
/// process takes it as `--crdt`, and the orchestrator takes the same flag for
/// its in-process lane. Two copies of this enum would be two lists to keep in
/// step, and the one that drifted would silently benchmark the wrong library.
///
/// A new variant needs one arm in [`Crdt::build`] and nothing else — the
/// `ReplicaState`/gRPC scaffolding is already generic over `CrdtAdapter`.
///
/// The value-enum names clap derives (`automerge`, `yrs`, `loro`) are also
/// what `deploy/docker/gen-compose.py` and the k8s StatefulSet's `CRDT`
/// environment variable emit, so renaming a variant is a deploy-side change
/// too.
#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Crdt {
    Automerge,
    Yrs,
    Loro,
}

impl Crdt {
    /// Construct the selected adapter.
    ///
    /// Boxing here is what lets callers keep a single setup path: the three
    /// adapters are different concrete types, so a `match` that also built the
    /// server would have to repeat that setup once per variant.
    /// `ReplicaState` stores a `Box<dyn CrdtAdapter>` regardless, so this
    /// costs nothing it was not already paying.
    pub fn build(self) -> Box<dyn common::CrdtAdapter> {
        match self {
            Crdt::Automerge => Box::new(AutomergeAdapter::new()),
            Crdt::Yrs => Box::new(YrsAdapter::new()),
            Crdt::Loro => Box::new(LoroAdapter::new()),
        }
    }

    /// The library's name as it appears on the command line, in the deploy
    /// generators, and in the run-provenance file.
    ///
    /// One spelling everywhere: `Debug` would print `Loro`, which does not
    /// match the `--crdt loro` a reader would type to reproduce the run.
    pub fn as_str(self) -> &'static str {
        match self {
            Crdt::Automerge => "automerge",
            Crdt::Yrs => "yrs",
            Crdt::Loro => "loro",
        }
    }
}

impl std::fmt::Display for Crdt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
pub(crate) mod conformance;
