//! [`common::CrdtAdapter`] implementations, one per CRDT library.
//!
//! Each submodule holds one adapter struct. It also holds the `#[test]`
//! wrappers that run the generic functions of the `conformance` test module
//! against that adapter. A new adapter is complete when every generic function
//! it wraps passes. The `conformance` module doc defines "generic" and "wraps".

mod automerge;
mod loro;
mod yrs;

pub use automerge::AutomergeAdapter;
pub use loro::LoroAdapter;
pub use yrs::YrsAdapter;

/// Name of the root map that stands in for ROOT (the empty object name).
///
/// In the [`common::Op`] model, an empty `obj` names ROOT: one unnamed
/// top-level map. Automerge has this map. Yrs and Loro do not, so their
/// adapters map the empty name to this name with [`map_name`]. Every replica
/// resolves the name the same way, and no op creates the map, so two replicas
/// cannot create it concurrently.
///
/// Do not change the value. It is encoded in every Yrs and Loro document that
/// writes to ROOT, so a change alters doc sizes and sync bytes.
///
/// Do not use this name for a workload object. It would alias ROOT.
pub(crate) const ROOT_MAP_NAME: &str = "_root";

/// Returns the root map name for `obj`. The empty name becomes
/// [`ROOT_MAP_NAME`].
pub(crate) fn map_name(obj: &str) -> &str {
    if obj.is_empty() { ROOT_MAP_NAME } else { obj }
}

/// Rejects an empty object name for a list or text op.
///
/// ROOT is a map, so an empty name has no meaning for a list or text op. An
/// error is better than a silently invented root sequence.
pub(crate) fn require_name(obj: &str) -> anyhow::Result<()> {
    if obj.is_empty() {
        anyhow::bail!("object name must not be empty: ROOT is a map, not a list or text");
    }
    Ok(())
}

/// Selects the [`common::CrdtAdapter`] implementation that backs a replica.
///
/// Both binaries use this one enum. The replica takes it as `--crdt`. The
/// orchestrator takes the same flag for its in-process lane.
///
/// To add a variant, add an arm to [`Crdt::build`] and an entry to
/// [`Crdt::ALL`]. [`Crdt::as_str`] is an exhaustive match, so the compiler
/// finds the remaining change.
///
/// The names from [`Crdt::as_str`] also appear in `deploy/docker/gen-compose.py`
/// and in the `CRDT` environment variable of the k8s StatefulSet. If you rename
/// a variant, change the deploy files too.
///
/// This enum does not derive `clap::ValueEnum`, because this crate has no CLI
/// code. Each binary builds its own parser from [`Crdt::ALL`] and
/// [`std::str::FromStr`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Crdt {
    Automerge,
    Yrs,
    Loro,
}

impl Crdt {
    /// Every variant. Use it to list the accepted CLI values or to sweep all
    /// backends.
    pub const ALL: [Crdt; 3] = [Crdt::Automerge, Crdt::Yrs, Crdt::Loro];

    /// Builds the selected adapter.
    ///
    /// The adapters are different concrete types. Boxing them lets the caller
    /// use one setup path for all of them. `ReplicaState` stores a
    /// `Box<dyn CrdtAdapter>`, so the box adds no cost.
    pub fn build(self) -> Box<dyn common::CrdtAdapter> {
        match self {
            Crdt::Automerge => Box::new(AutomergeAdapter::new()),
            Crdt::Yrs => Box::new(YrsAdapter::new()),
            Crdt::Loro => Box::new(LoroAdapter::new()),
        }
    }

    /// Returns the library name used on the command line, in the deploy
    /// generators, and in the run-provenance file.
    ///
    /// Use this name, not `Debug`. `Debug` prints `Loro`, but the flag is
    /// `--crdt loro`.
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

    /// Parses a name from [`Crdt::as_str`]. The error message lists the
    /// accepted values, because a person reads it.
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
