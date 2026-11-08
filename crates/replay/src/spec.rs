//! The workload spec: how to run a trace.
//!
//! A spec file is TOML, with one `[[workload]]` table per workload. The library
//! is not a spec field, so one spec runs against every library.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Selects the part of a replay that the timer covers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Timing {
    /// The timer covers every transaction after the setup, the delivery to the
    /// checked replicas, and the final reads.
    #[default]
    All,
    /// All writers apply their transactions first. Replica 0 then merges all
    /// updates, as a reference. The timer covers only the merge on the other
    /// checked replicas, and the final reads.
    MergeOnly,
}

/// The order of the library ids across agents.
///
/// A library breaks ties between concurrent ops with these ids. So this order
/// can change the final text of a trace that has concurrent inserts at one
/// position, and the time of a merge. Under Yjs and Yrs, the upstream
/// `friendsforever` trace reaches its `endContent` only with ascending ids.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum IdOrder {
    /// Ids that look random, as the libraries make by default. They come from a
    /// seed, so each run with that seed is the same.
    #[default]
    Seeded,
    /// Agent 0 has the lowest id.
    Ascending,
    /// Agent 0 has the highest id.
    Descending,
}

/// One workload in a spec file.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Workload {
    /// The name in the result rows and in `--only`.
    pub name: String,
    /// What the workload does, for a person reading the file.
    #[serde(default)]
    pub description: String,
    /// The trace file, relative to the data directory.
    pub trace: PathBuf,
    /// The expected SHA-256 hash of the trace file.
    pub sha256: String,
    /// How many times one agent replays the trace on one document.
    #[serde(default = "one")]
    pub repeat: usize,
    /// Extra replicas that only receive updates.
    #[serde(default)]
    pub receivers: usize,
    /// How many leading transactions run before the timer starts. Every replica
    /// receives them before the timer starts.
    #[serde(default)]
    pub setup_txns: usize,
    /// The part of the replay that the timer covers.
    #[serde(default)]
    pub timing: Timing,
    /// Delivers all updates to a replica in one adapter call.
    #[serde(default)]
    pub merge_batch: bool,
    /// How many replicas to read and check, starting at replica 0. Omit this to
    /// check all replicas.
    pub check_replicas: Option<usize>,
    /// Runs only with `--include-heavy`, because it can take many minutes.
    #[serde(default)]
    pub heavy: bool,
    /// The id order that this trace needs to reach its expected text. Omit this
    /// to use the default. `--id-order` overrides it.
    pub id_order: Option<IdOrder>,
}

/// The default of [`Workload::repeat`].
fn one() -> usize {
    1
}

impl Workload {
    /// Returns the settings for a trace with no spec: all agents, all timed, and
    /// all replicas checked.
    #[must_use]
    pub fn ad_hoc(name: String, trace: PathBuf) -> Self {
        Self {
            name,
            trace,
            repeat: 1,
            ..Self::default()
        }
    }
}

/// The top level of a spec file.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SpecFile {
    /// The `[[workload]]` tables.
    workload: Vec<Workload>,
}

/// Reads the workloads in the spec file at `path`.
pub fn load(path: &Path) -> Result<Vec<Workload>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let spec: SpecFile =
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    Ok(spec.workload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_workload_has_useful_defaults() {
        let spec: SpecFile = toml::from_str(
            r#"
            [[workload]]
            name = "b4"
            trace = "b4.json"
            sha256 = "00"
            "#,
        )
        .unwrap();
        let w = &spec.workload[0];
        assert_eq!(w.repeat, 1);
        assert_eq!(w.timing, Timing::All);
        assert_eq!(w.check_replicas, None);
        assert!(!w.heavy);
    }

    /// Returns the path of a file relative to the repository root.
    fn repo_path(relative: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(relative)
    }

    #[test]
    fn the_committed_specs_are_valid() {
        let suite = load(&repo_path("workloads/crdt-benchmarks.toml")).unwrap();
        assert_eq!(suite.len(), 25);
        let traces = load(&repo_path("workloads/editing-traces.toml")).unwrap();
        assert_eq!(traces.len(), 2);
        for w in suite.iter().chain(&traces) {
            assert_eq!(w.sha256.len(), 64, "{}", w.name);
        }
        let mut names: Vec<&str> = suite.iter().map(|w| w.name.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), suite.len(), "workload names must be unique");
    }

    /// CI runs this workload on a container. This test runs it in process, so a
    /// bad fixture or a stale hash fails before CI builds an image.
    #[test]
    fn the_smoke_workloads_pass_on_every_library() {
        use crate::driver::{Ids, Plan, run_trial};
        use crate::target::Lane;
        use crate::trace::{Trace, read_verified};
        use replica::adapter::Crdt;

        for w in load(&repo_path("workloads/smoke.toml")).unwrap() {
            let path = repo_path("workloads/fixtures").join(&w.trace);
            let bytes = read_verified(&path, Some(&w.sha256)).unwrap();
            for crdt in Crdt::ALL {
                let plan = Plan::new(w.clone(), Trace::parse(&bytes).unwrap()).unwrap();
                run_trial(&plan, &Lane::InProcess(crdt), Ids::SEEDED)
                    .unwrap_or_else(|e| panic!("{} {crdt}: {e:#}", w.name));
            }
        }
    }

    /// Yjs and Yrs reach the expected text of this trace only with ascending ids.
    #[test]
    fn friendsforever_pins_its_id_order() {
        let workloads = load(&repo_path("workloads/editing-traces.toml")).unwrap();
        let pinned = |name: &str| {
            workloads
                .iter()
                .find(|w| w.name == name)
                .unwrap_or_else(|| panic!("no workload {name}"))
                .id_order
        };
        assert_eq!(pinned("et.friendsforever"), Some(IdOrder::Ascending));
    }

    #[test]
    fn an_unknown_field_is_an_error() {
        let bad = r#"
            [[workload]]
            name = "b4"
            trace = "b4.json"
            sha256 = "00"
            recievers = 1
            "#;
        assert!(toml::from_str::<SpecFile>(bad).is_err());
    }
}
