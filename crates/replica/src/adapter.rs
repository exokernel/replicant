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
/// A new variant needs an arm in [`Crdt::build`] and an entry in
/// [`Crdt::ALL`]; [`Crdt::as_str`] and [`FromStr`] are exhaustive matches, so
/// the compiler catches the rest.
///
/// The names in [`Crdt::as_str`] are also what `deploy/docker/gen-compose.py`
/// and the k8s StatefulSet's `CRDT` environment variable emit, so renaming a
/// variant is a deploy-side change too.
///
/// Deliberately *not* deriving `clap::ValueEnum`: that would put a CLI
/// concern in a library crate for its binaries' benefit. [`FromStr`] plus
/// [`Crdt::ALL`] give each binary everything it needs to build its own
/// `value_parser` without this module knowing that a command line exists.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Crdt {
    Automerge,
    Yrs,
    Loro,
}

impl Crdt {
    /// Every variant, for callers that need to enumerate them — a CLI's
    /// accepted-values list, or a sweep over all backends.
    pub const ALL: [Crdt; 3] = [Crdt::Automerge, Crdt::Yrs, Crdt::Loro];

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

impl std::str::FromStr for Crdt {
    type Err = String;

    /// The inverse of [`Crdt::as_str`]. The error lists the accepted values,
    /// since the only callers are argument parsers reporting to a human.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Crdt::ALL
            .into_iter()
            .find(|c| c.as_str() == s)
            .ok_or_else(|| {
                let names: Vec<&str> = Crdt::ALL.iter().map(|c| c.as_str()).collect();
                format!("unknown CRDT '{s}' (expected one of: {})", names.join(", "))
            })
    }
}

#[cfg(test)]
pub(crate) mod conformance;
